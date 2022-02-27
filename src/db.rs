use tiberius::{Client, Config, AuthMethod};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use crate::init::{State};
use super::init::{LogSource,Comp};
//use std::hash::Hash;
//Experimental
use std::{collections::HashMap}; 
//Arc mutex for thread communication
use std::sync::Arc;
use parking_lot::Mutex;
use serde_json;
use tokio::{ time::{self, Duration}};


pub async fn sync_db_change(db_track_change: Arc<Mutex<HashMap<String,String>>>,peer_addr:String)
{
    let mut data = "".to_owned();
    loop {
        let new_data = serde_json::to_string(&*db_track_change.lock()).unwrap();
        if data!=new_data {
            println!("Config sync started!");
            data = new_data;
            std::fs::write("./db_track_change.json", &data).expect("Unable to write to config.toml");
            super::com::send_data(&peer_addr, &data,"***CHT***","***END***").await;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub async fn call_db(state:Arc<Mutex<State>>,log_source_config:LogSource, db_track_change: Arc<Mutex<HashMap<String,String>>>
                        ,log_server:String)  {
    println!("Call DB");
    let two_seconds = std::time::Duration::from_millis(1000);
    let mut counter;
    let mut connection_wait = true;
    if *db_track_change.lock().get(&log_source_config.name).unwrap()=="" {
        counter = log_source_config.counter_default_value;
    }      
    
    loop {
        if *state.lock()!=State::Slave {
            connection_wait = true;
            let mut config = Config::new();
            config.host(&log_source_config.addr);
            config.port(log_source_config.port);
            config.authentication(AuthMethod::sql_server(&log_source_config.username, &log_source_config.pass));
            config.trust_cert(); // on production, it is not a good idea to do this  
            use std::io::{Error,ErrorKind};
            match time::timeout(Duration::from_secs(1),
            TcpStream::connect(config.get_addr())).await.unwrap_or(Err(Error::new(ErrorKind::TimedOut, "Timed out"))) {                        
                Ok(tcp) => {
                    match tcp.set_nodelay(true) {
                        Ok(_) =>(),
                        Err(e) => println!("Error in set_nodelay function, Original: {}",e)
                    }                    
                    match Client::connect(config, tcp.compat_write()).await {
                        Ok(mut client) => {
                            println!("Authenticate successfully");
                            match super::com::connect_on_udp(&log_server).await {
                                Ok(sock) => {
                                    while *state.lock()!=State::Slave {                            
                                        counter = db_track_change.lock().get(&log_source_config.name).unwrap().parse::<String>().unwrap();
                                        let query = log_source_config.query.replace("??", &counter.to_string());
                                        let stream = client.query(&query, &[]).await.unwrap();
                                        match stream.into_first_result().await {                                    
                                            Ok(rows) => {
                                                for item in rows {
                                                    let x = format!("{}\n",item.get_data());
                                                    match sock.send(x.as_bytes()).await {
                                                        Ok(_) => {
                                                            counter= item.get_filed_data_as_string(&*log_source_config.counter_field);
                                                            println!("Counter: {}", counter);
                                                            *db_track_change.lock().get_mut(&log_source_config.name).unwrap()=counter;
                                                            println!("{}", x);
                                                        },
                                                        Err(e) => println!("Error in sending log, Original error: {}",e)
                                                    }                                
                                                }
                                            },
                                            Err(e) => {
                                                println!("Error on calling inot_first_result, Original: {}", e);
                                                break;
                                            }
                                        }
                                        std::thread::sleep(two_seconds);
                                        connection_wait = false;
                                    }
                                },
                                Err(e) => println!("Error in connecting to log server, Original Error: {}", e)
                            }
                            
                        },
                        Err(e) => println!("An error happened!, Original: {}",e)
                    }
                },
                Err(e) => println!("Error on connecting to databse, Original: {}", e)
            }
        }
        if connection_wait { //Prevent from two times waiting in one cycle
            std::thread::sleep(two_seconds);
        }
    }       
}

//-----------------------------------------*********************************************************************>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
use tokio_util::compat::Compat;
pub async fn create_sql_con(username:&str,pass:&str,addr:&str,port:u16) -> Result<Client<Compat<TcpStream>>,tiberius::error::Error>  {
    println!("Call DB");
    let mut config = Config::new();
    config.host(addr);
    config.port(port);
    config.authentication(AuthMethod::sql_server(username, pass));
    config.trust_cert(); // on production, it is not a good idea to do this  
    use std::io::{Error,ErrorKind};
    let tcp = time::timeout(Duration::from_secs(1),
        TcpStream::connect(config.get_addr())).await.unwrap_or(Err(Error::new(ErrorKind::TimedOut, "Timed out")))?; 
    tcp.set_nodelay(true)?;
    return Ok(Client::connect(config, tcp.compat_write()).await?);
}

pub async fn call_query(client:&mut Client<Compat<TcpStream>>, query:&str,counter:&str) -> Result<Vec<tiberius::Row>,tiberius::error::Error> {
    let query = query.replace("??", &counter.to_string());
    let stream = client.query(&query, &[]).await?;
    return  Ok(stream.into_first_result().await?);        
}

pub async fn call_comp(state:Arc<Mutex<State>>,comp:Comp, db_track_change: Arc<Mutex<HashMap<String,String>>>,log_server:String)  {
    let two_seconds = std::time::Duration::from_millis(1000);
    let mut counter;
    let mut connection_wait = true;
    
        
    loop {
        if *state.lock()!=State::Slave {
            let track_change: HashMap<String,String> = HashMap::new();
            let mut sql_conn_list: HashMap<String,Client<Compat<TcpStream>>> = HashMap::new();
            for log_source in &comp.log_sources {
                match create_sql_con(&log_source.username.clone(), &log_source.pass.clone(), &log_source.addr.clone(), log_source.port).await {
                    Ok(client) => {
                        sql_conn_list.insert(log_source.name.clone(), client);  
                        //******************************************************** */
                    },
                    Err(e) => println!("Error in connecting to sql server, Original: {}", e)
                }
            }
            //************Start fetching data
            let udp_result = super::com::connect_on_udp(&log_server).await;
            if let Ok(sock)= udp_result && sql_conn_list.len()==comp.log_sources.len()  {
                while *state.lock()!=State::Slave {
                    let mut log = comp.result.clone();
                    for log_source in comp.log_sources {
                        let client = sql_conn_list.get(&log_source.name).unwrap();
                        counter = db_track_change.lock().get(&log_source.name).unwrap().parse::<String>().unwrap();
                        match call_query(&mut client, &log_source.query , &counter).await {
                            Ok(rows) => {
                                log.replace(&log_source.name, &rows[0].get_data() );
                                track_change.insert(log_source.name, rows[0].get_filed_data_as_string(&*log_source.counter_field));
                            },
                            Err(e) => println!("Error in querying data, Original: {}", e)
                        }
                    }
                    ///////////////////////////////////////////////
                    // match sock.send(log.as_bytes()).await {
                    //     Ok(_) => {
                    //         counter= item.get_filed_data_as_string(&*log_source.counter_field);
                    //         println!("Counter: {}", counter);
                    //         *db_track_change.lock().get_mut(&log_source.name).unwrap()=counter;
                    //         println!("{}", x);
                    //     },
                    //     Err(e) => println!("Error in sending log, Original error: {}",e)
                    // }
                    std::thread::sleep(two_seconds);
                    connection_wait = false;
                }
            }
            else if let Err(e) = udp_result {
                println!("Error in connecting to log server, Original Error: {}", e)
            }
            
        }
        if connection_wait { //Prevent from two times waiting in one cycle
            std::thread::sleep(two_seconds);
        }
    }       
}