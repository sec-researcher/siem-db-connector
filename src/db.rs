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
use parking_lot::{Mutex};
use serde_json;
use tokio::{ time::{self, Duration}};
use crate::init::{ResultExt, OptionExt };


pub async fn sync_db_change(db_track_change: Arc<Mutex<HashMap<String,String>>>,peer_addr:String)
{
    let mut data = "".to_owned();
    loop {
        let new_data = serde_json::to_string(&*db_track_change.lock()).log_or("Can not convert db_track_change object to string", "".to_string());
        if data!="" && data!=new_data {
            log::info!("db_track_change config sync started!");
            data = new_data;
            std::fs::write("./db_track_change.json", &data).log_or("Unable to write to config.toml in sync_db_change", ());
            super::com::send_data(&peer_addr, &data,"***CHT***","***END***").await.log_or("Error in sending new config to partner.", true);
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub async fn call_db(state:Arc<Mutex<State>>,log_source_config:LogSource, db_track_change: Arc<Mutex<HashMap<String,String>>>
                        ,log_server:String)  {
    log::info!("Call DB started for {}", log_source_config.name);
    let two_seconds = std::time::Duration::from_millis(1000);
    let mut counter;
    let mut connection_wait = true;
          
    
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
                        Err(e) => log::error!("Error in set_nodelay function, OE: {}",e)
                    }                    
                    match Client::connect(config, tcp.compat_write()).await {
                        Ok(mut client) => {
                            log::warn!("Authenticate successfully for {}", log_source_config.name);
                            match super::com::connect_on_udp(&log_server).await {
                                Ok(sock) => {
                                    while *state.lock()!=State::Slave {                            
                                        let query;
                                        if let Some(counter_field) = log_source_config.counter_field.clone() { //Makeing sure of setting counter field.
                                            let counter = db_track_change.lock().get(&log_source_config.name).log_or("Can not retrieve field from db_track_change.", &"".to_string()).clone();
                                            query = log_source_config.query.replace("??", &counter.to_string());
                                        }
                                        else {
                                            query = log_source_config.query.clone();
                                        }
                                        let stream = client.query(&query, &[]).await.unwrap();
                                        match stream.into_first_result().await {                                    
                                            Ok(rows) => {
                                                for item in rows {
                                                    let x;
                                                    if let Some(counter_field) = log_source_config.counter_field.clone() {
                                                        if log_source_config.hide_counter.unwrap() {
                                                            x = format!("{}\n",item.get_data(&counter_field));
                                                        }
                                                        else {
                                                            x = format!("{}\n",item.get_data(""));
                                                        }                                                        
                                                    }
                                                    else {
                                                        x = format!("{}\n",item.get_data(""));
                                                    }

                                                    match sock.send(x.as_bytes()).await {
                                                        Ok(_) => {
                                                            if let Some(counter_field) = log_source_config.counter_field.clone() {
                                                                counter= item.get_filed_data_as_string(&*counter_field);
                                                                println!("Counter: {}", counter);
                                                                *db_track_change.lock().get_mut(&log_source_config.name).unwrap()=counter;
                                                            }
                                                            // counter= item.get_filed_data_as_string(&*log_source_config.counter_field);
                                                            // println!("Counter: {}", counter);
                                                            // *db_track_change.lock().get_mut(&log_source_config.name).unwrap()=counter;
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

pub async fn call_query(mut client: Client<Compat<TcpStream>>, query:&str,counter:String, i:usize) -> Result<(Vec<tiberius::Row>,usize,Client<Compat<TcpStream>>),tiberius::error::Error> {
    let query = query.replace("??", &counter);
    println!("{}", query);
    //let mut client = client.lock();    
    let stream = client.query(&query, &[]).await?;
    return  Ok((stream.into_first_result().await?, i, client));
}

pub async fn call_comp(state:Arc<Mutex<State>>,comp:Comp, db_track_change: Arc<Mutex<HashMap<String,String>>>,log_server:String)  {
    let two_seconds = std::time::Duration::from_millis(1000);
    //let mut counter;
    let mut connection_wait = true;
    loop {
        if *state.lock()!=State::Slave {            
            let mut sql_conn_list = vec!();
            let mut connections = vec!();
            let mut futures = vec!();
            for i in 0..comp.log_sources.len() {
                let log_source = &comp.log_sources[i];                
                let future = create_sql_con(&log_source.username, &log_source.pass, &log_source.addr, log_source.port);
                futures.push(future);
            }
            connections = futures::future::join_all(futures).await;
            let mut i = 0;
            for connection in connections { //Usage is for error handling and making sure are connection made successfully
                match connection {
                    Ok(client) => {
                        sql_conn_list.push(client);
                        //******************************************************** */
                    },
                    Err(e) => println!("Error in connecting to sql server, Original: {}", e)
                }
                i+=1;
            }
            //************Start fetching data
            match super::com::connect_on_udp(&log_server).await {
                Ok(sock)=>  {
                    if sql_conn_list.len()==comp.log_sources.len() { //Make sure that connection to all server was success                                                
                        while *state.lock()!=State::Slave {
                            let mut track_change: HashMap<String,String> = HashMap::new();
                            let mut log = comp.result.clone();
                            let mut query_handles = vec!();
                            let mut futures = vec!();

                            sql_conn_list.reverse();
                            let mut i =0;
                            while !sql_conn_list.is_empty() {
                                let log_source = &comp.log_sources[i];
                                let client = sql_conn_list.pop().unwrap();
                                let counter = db_track_change.lock().get(&log_source.name).unwrap_or(&"".to_owned()).to_string();
                                let future = call_query(  client, &log_source.query , counter, i);
                                //let future = call_query( &sql_conn_list, &log_source.query , counter);
                                futures.push(future);
                                i+=1;
                            }                            
                            query_handles = futures::future::join_all(futures).await;
                            i = 0;
                            for query_handle in query_handles {
                                let log_source = &comp.log_sources[i];
                                match query_handle {
                                    Ok(rows) => {
                                        sql_conn_list.push(rows.2);
                                        //println!("num: {}",rows.1); //Used for makeing sure the sequence is correct
                                        let rows = rows.0;
                                        if let Some(counter_field) = log_source.counter_field.clone() {
                                            track_change.insert(log_source.name.clone(), rows[0].get_filed_data_as_string(&*counter_field));
                                            if log_source.hide_counter.unwrap() {
                                                log = log.replace(&log_source.name, &rows[0].get_data(&counter_field) );
                                            }
                                            else {
                                                log = log.replace(&log_source.name, &rows[0].get_data("") );
                                            }                                            
                                        }
                                        else {
                                            log = log.replace(&log_source.name, &rows[0].get_data("") );
                                        }
                                    },
                                    Err(e) => println!("Error in querying data, Original: {}", e) //Error in this section may lead to log with no integrity
                                }
                                i+=1;
                            }
                            ///////////////////////////////////////////////
                            log = format!("{}\n",log);
                            match sock.send(log.as_bytes()).await {
                                Ok(_) => {
                                    for item in track_change {
                                        *db_track_change.lock().get_mut(&item.0).unwrap() = item.1;
                                    }                                    
                                },
                                Err(e) => println!("Error in sending log, Original error: {}",e)
                            }
                            std::thread::sleep(two_seconds);
                            connection_wait = false;
                        }
                    }
                },
                Err(e) => println!("Error in connecting to log server, Original Error: {}", e)
            }           
        }
        if connection_wait { //Prevent from two times waiting in one cycle
            std::thread::sleep(two_seconds);
        }
    }       
}