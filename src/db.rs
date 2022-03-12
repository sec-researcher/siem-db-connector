use tiberius::{Client, Config, AuthMethod};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use crate::init::{State};
use super::init::{LogSource,Comp};
use std::{collections::HashMap}; 
use std::sync::Arc;
use parking_lot::{Mutex};
use serde_json;
use tokio::{ time::{self, Duration}};
use crate::init::ResultExt;


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

pub async fn call_db(state:Arc<Mutex<State>>,log_source_config:LogSource, db_track_change: Arc<Mutex<HashMap<String,String>>>
                        ,mut log_server:String)  {
    log_server = log_source_config.log_server.unwrap_or( log_server );    
    log::warn!("Call DB started for {}", log_source_config.name);
    let two_seconds = std::time::Duration::from_millis(1000);
    let mut counter;
    let mut connection_wait = true;
    loop {
        if *state.lock()!=State::Slave {
            connection_wait = true;
            match create_sql_con(&log_source_config.username, &log_source_config.pass, &log_source_config.addr, log_source_config.port).await {
                Ok(mut client)=> {
                    log::warn!("successfully connect and Auth to {}", log_source_config.name);
                    match super::com::connect_on_udp(&log_server).await {
                        Ok(sock) => {
                            log::warn!("successfully connected to log server at {}", log_server);
                            while *state.lock()!=State::Slave {
                                counter = db_track_change.lock().get(&log_source_config.name).unwrap_or(&"".to_owned()).to_string();
                                match call_query(  client, &log_source_config.query , counter, 0).await {
                                    Ok((rows,_i,returned_client)) => {
                                        client=returned_client;
                                        for item in rows {
                                            let log;
                                            if let Some(counter_field) = log_source_config.counter_field.clone() {
                                                if log_source_config.hide_counter!=Option::None {
                                                    log = format!("{}\n",item.get_data(&counter_field));
                                                }
                                                else {
                                                    log = format!("{}\n",item.get_data(""));
                                                }                                                        
                                            }
                                            else {
                                                log = format!("{}\n",item.get_data(""));
                                            }
                                            match sock.send(log.as_bytes()).await {
                                                Ok(_) => {
                                                    if let Some(counter_field) = log_source_config.counter_field.clone() {
                                                        counter= item.get_filed_data_as_string(&*counter_field);                                                                                                                
                                                        match db_track_change.lock().get_mut(&log_source_config.name) {
                                                            Some(track_change) => *track_change = counter,
                                                            None => log::error!("Can not find {} key in db_track_change.", log_source_config.name)
                                                        }
                                                    }
                                                    log::debug!("{}", log)
                                                },
                                                Err(e) => log::error!("Error in sending log, OE: {}",e)
                                            }                                
                                        }
                                    },
                                    Err(e) => {
                                        log::error!("Error on calling query&fetching result, OE: {}", e);
                                        break;
                                    }                                    
                                }                                
                                std::thread::sleep(two_seconds);
                                connection_wait = false;
                            }
                        },
                        Err(e) => log::error!("Error in connecting to log server: {}, OE: {}", log_server, e)
                    }                    
                },
                Err(e) => log::error!("Error in connectimg to {}({}), OE: {}", log_source_config.name, log_source_config.addr, e )
            }
        }
        if connection_wait { //Prevent from two times waiting in one cycle
            std::thread::sleep(two_seconds);
        }
    }       
}

//-----------------------------------------*********************************************************************>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

pub async fn call_comp(state:Arc<Mutex<State>>,comp:Comp, db_track_change: Arc<Mutex<HashMap<String,String>>>,mut log_server:String)  {    
    log_server = comp.log_server.unwrap_or( log_server );
    let two_seconds = std::time::Duration::from_millis(1000);
    //let mut counter;
    let mut connection_wait = true;
    loop {
        if *state.lock()!=State::Slave {
            let mut sql_conn_list = vec!();
            let mut connections = vec!();
            let mut futures = vec!();
            log::warn!("Trying to connect to all sql server in comp {}", comp.name);
            for i in 0..comp.log_sources.len() {
                let log_source = &comp.log_sources[i];                
                let future = create_sql_con(&log_source.username, &log_source.pass, &log_source.addr, log_source.port);
                futures.push(future);
            }
            connections = futures::future::join_all(futures).await;
            let mut i = 0;
            log::warn!("Checking result of connection to all sql server in comp {}", comp.name);
            for connection in connections { //Usage is for error handling and making sure are connection made successfully
                match connection {
                    Ok(client) => {
                        sql_conn_list.push(client);
                        //******************************************************** */
                    },
                    Err(e) => log::error!("Error in connecting to sql server, OE: {}", e)
                }
                i+=1;
            }
            //************Start fetching data
            match super::com::connect_on_udp(&log_server).await {                
                Ok(sock)=>  {
                    log::warn!("Connected to log server {}", log_server);
                    if sql_conn_list.len()==comp.log_sources.len() { //Make sure that connection to all server was success                                                
                        while *state.lock()!=State::Slave {
                            let mut track_change: HashMap<String,String> = HashMap::new();
                            let mut log = comp.result.clone();
                            let mut query_handles = vec!();
                            let mut futures = vec!();

                            sql_conn_list.reverse();
                            let mut i =0;
                            log::info!("Sending query to all mssql server on {}", comp.name);
                            while !sql_conn_list.is_empty() {
                                let log_source = &comp.log_sources[i];
                                let client = sql_conn_list.pop().unwrap(); //Using unwrap is safe here because at the start of the loop we check that list is not empty
                                let counter = db_track_change.lock().get(&log_source.name).unwrap_or(&"".to_owned()).to_string();
                                let future = call_query(  client, &log_source.query , counter, i);
                                //let future = call_query( &sql_conn_list, &log_source.query , counter);
                                futures.push(future);
                                i+=1;
                            }                            
                            query_handles = futures::future::join_all(futures).await;
                            i = 0;
                            log::info!("Checking for result of all query created in comp: {}", comp.name);
                            for query_handle in query_handles {
                                let log_source = &comp.log_sources[i];
                                match query_handle {
                                    Ok(rows) => {
                                        sql_conn_list.push(rows.2); //Add returned connection from call_query to the connections list                                        
                                        let rows = rows.0;
                                        if let Some(counter_field) = log_source.counter_field.clone() {
                                            track_change.insert(log_source.name.clone(), rows[0].get_filed_data_as_string(&*counter_field));
                                            if log_source.hide_counter!=Option::None {
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
                                    Err(e) => log::error!("Log integrity fails, Error in querying data in comp: {}, OE: {}", comp.name, e) //Error in this section may lead to log with no integrity
                                }
                                i+=1;
                            }
                            ///////////////////////////////////////////////                            
                            match sock.send(format!("{}\n",log).as_bytes()).await {
                                Ok(_) => {
                                    log::debug!("Log sent to log server for comp: {}", comp.name);
                                    for item in track_change {
                                        match db_track_change.lock().get_mut(&item.0) {
                                            Some(track_change) => *track_change = item.1,
                                            None => log::error!("Can not find {} key in db_track_change.", item.0)
                                        }                                        
                                    }                                    
                                },
                                Err(e) => log::error!("Error in sending log for comp: {}, OE: {}", comp.name, e)
                            }
                            std::thread::sleep(two_seconds);
                            connection_wait = false;
                        }
                    }
                    else {
                        log::error!("Connection to all sql server in comp {} was not successful", comp.name);
                    }
                },
                Err(e) => log::error!("Error in connecting to log server: {}, OE: {}", log_server, e)
            }           
        }
        if connection_wait { //Prevent from two times waiting in one cycle
            std::thread::sleep(two_seconds);
        }
    }       
}