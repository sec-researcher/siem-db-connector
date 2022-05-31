use tiberius::{Client, Config, AuthMethod};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use crate::init::{State, LogSources};
use super::init::{LogSource,Comp};
use std::{collections::HashMap}; 
use std::sync::Arc;
use parking_lot::{Mutex};
use serde_json;
use tokio::{ time::{self, Duration}};
use crate::prelude::{ResultExt };
use crate::prelude::RowExt;
use std::time::{ Instant};



pub async fn sync_db_change(db_track_change: Arc<Mutex<HashMap<String,String>>>,peer_addr:String)
{
    log::debug!("db_track_change run");
    let mut data = "".to_owned();
    loop {
        let new_data = serde_json::to_string(&*db_track_change.lock()).log_or("Can not convert db_track_change object to string", "".to_string());
        if data!=new_data {
            log::info!("db_track_change config sync started!");
            data = new_data;
            std::fs::write("/var/siem-db-connector/db_track_change.json", &data).log_or("Unable to write to /var/siem-db-connector/db_track_change.json in sync_db_change", ());            
            super::com::send_data(&peer_addr, &data,"***CHT***","***END***").await.log_or("Error in sending new config to partner.", true);
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}


use tokio_util::compat::Compat;
pub async fn create_sql_con(username:&str,pass:&str,addr:&str,port:u16) -> Result<Client<Compat<TcpStream>>,tiberius::error::Error>  {    
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
    let log_servers = log_server.split(",").collect::<Vec<&str>>();
    log::warn!("Call DB started for {}", log_source_config.name);
    let pause_duration = std::time::Duration::from_millis(log_source_config.pause_duration.unwrap_or(2000));
    let mut counter;
    let mut connection_wait;
    loop {
        connection_wait = true; //Prevent from infinite loop
        let before_connection = Instant::now();
        if *state.lock()!=State::Slave {
            connection_wait = true;
            match create_sql_con(&log_source_config.username, &log_source_config.pass, &log_source_config.addr, log_source_config.port).await {
                Ok(mut client)=> {
                    log::warn!("successfully connect and Auth to {}", log_source_config.name);                    
                    let sockets = super::com::create_udp_sockets_concurrently(&log_servers).await;
                    if sockets.len()>0  {
                        log::warn!("successfully connected to log server at {}", log_server);
                        while *state.lock()!=State::Slave {
                            let before_query = Instant::now();
                            counter = db_track_change.lock().get(&log_source_config.name).unwrap_or(&"".to_owned()).to_string();
                            match call_query(  client, &log_source_config.query , counter, 0).await {
                                Ok((rows,_i,returned_client)) => {
                                    client=returned_client;
                                    for item in rows {
                                        let log;
                                        if log_source_config.counter_field!=Option::None && log_source_config.hide_counter!=Option::None {                                                
                                            log = format!("{}\n",item.get_row_str(&log_source_config.counter_field.clone().unwrap()));
                                        }
                                        else {
                                            log = format!("{}\n",item.get_row_str(""));
                                        }
                                        for sock in &sockets {
                                            match sock.send(log.as_bytes()).await {
                                                Ok(_) => {
                                                    let counter = item.get_field_str(log_source_config.counter_field.clone());
                                                    if counter!=""  {                                                                                                                                
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
                                    }
                                },
                                Err(e) => {
                                    log::error!("Error on calling query&fetching result, OE: {}", e);
                                    break;
                                }                                    
                            }                                
                            pause(pause_duration, before_query.elapsed()).await;
                            connection_wait = false;
                        }                        
                    }
                    else {
                        //Err(e) => log::error!("Error in connecting to log server: {}, OE: {}", log_server, e)                        
                    }
                    
                                        
                },
                Err(e) => log::error!("Error in connectimg to {}({}), OE: {}", log_source_config.name, log_source_config.addr, e )
            }
        }
        if connection_wait { //Prevent from two times waiting in one cycle
            pause(pause_duration, before_connection.elapsed()).await;
        }
    }       
}


pub async fn call_comp(state:Arc<Mutex<State>>,comp:Comp, db_track_change: Arc<Mutex<HashMap<String,String>>>,mut log_server:String)  {    
    log_server = comp.log_server.unwrap_or( log_server );
    let log_servers = log_server.split(",").collect::<Vec<&str>>();
    let pause_duration = std::time::Duration::from_millis(comp.pause_duration.unwrap_or(2000));
    //let mut counter;
    let mut connection_wait;
    loop {
        connection_wait = true;
        let before_connection = Instant::now();
        if *state.lock()!=State::Slave {
            connection_wait = true;
            let mut sql_conn_list = create_sql_connection_concurrently(&comp.log_sources, &comp.name).await;
            //************Start fetching data
            let sockets = super::com::create_udp_sockets_concurrently(&log_servers).await;
            if sockets.len()>0  {
                log::warn!("Connected to log server {}", log_server);
                if sql_conn_list.len()==comp.log_sources.len() { //Make sure that connection to all server was success                                                
                    while *state.lock()!=State::Slave {
                        let before_query = Instant::now();
                        let mut track_change: HashMap<String,String> = HashMap::new();                                                
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
                        let query_handles = futures::future::join_all(futures).await;
                        i = 0;
                        log::info!("Checking for result of all query created in comp: {}", comp.name);
                        let log = create_comp_log(query_handles, &comp.log_sources, &mut track_change, &mut sql_conn_list, &comp.result);                        
                        ///////////////////////////////////////////////
                        send_comp_log(db_track_change.clone(), track_change, &sockets, &log, &log_servers, &comp.name).await;
                        pause(pause_duration, before_query.elapsed()).await;
                        connection_wait = false;
                    }
                }
                else {
                    log::error!("Connection to all sql server in comp {} was not successful", comp.name);
                }
            }           
        }
        if connection_wait { //Prevent from two times waiting in one cycle
            pause(pause_duration, before_connection.elapsed()).await;                        
        }
    }
}


//DB module utility functions
async fn pause(pause_duration: Duration, elapsed: Duration) {
    if elapsed<pause_duration {
        tokio::time::sleep(pause_duration - elapsed).await;
    }
}
//-----------------------------------------*********************************************************************>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

async fn create_sql_connection_concurrently(log_sources: &Vec<LogSource>, name:&str) -> Vec<Client<Compat<TcpStream>>>{
    let mut sql_conn_list = vec!();
        let mut connections = vec!();
        let mut futures = vec!();
        log::warn!("Trying to connect to all sql server in comp {}", name);
        for i in 0..log_sources.len() {
            let log_source = &log_sources[i];                
            let future = create_sql_con(&log_source.username, &log_source.pass, &log_source.addr, log_source.port);
            futures.push(future);
        }
        connections = futures::future::join_all(futures).await;
        let mut i = 0;
        log::warn!("Checking result of connection to all sql server in comp {}", name);
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
        sql_conn_list
}

fn create_comp_log(query_handles: Vec<Result<(Vec<tiberius::Row>, usize, Client<Compat<TcpStream>>), tiberius::error::Error>>, log_sources: &Vec<LogSource>,
                    track_change: &mut HashMap<String,String>, sql_conn_list: &mut Vec<Client<Compat<TcpStream>>>, log:&str) -> String {
    let mut i=0;
    let mut log = log.to_string();
    for query_handle in query_handles {
        let log_source = &log_sources[i];
        match query_handle {
            Ok(rows) => {
                sql_conn_list.push(rows.2); //Add returned connection from call_query to the connections list                                        
                let rows = rows.0;
                if let Some(counter_field) = log_source.counter_field.clone() {
                    track_change.insert(log_source.name.clone(), rows[0].get_field_str(Some(counter_field.clone())) );
                    if log_source.hide_counter!=Option::None {
                        log = log.replace(&log_source.name, &rows[0].get_row_str(&counter_field) );
                    }
                    else {
                        log = log.replace(&log_source.name, &rows[0].get_row_str("") );
                    }                                            
                }
                else {
                    log = log.replace(&log_source.name, &rows[0].get_row_str("") );
                }
            },
            Err(e) => log::error!("Log integrity fails, Error in querying data in comp: {}, OE: {}", log_source.name, e) //Error in this section may lead to log with no integrity
        }
        i+=1;
    }
    log
}



async fn send_comp_log(db_track_change: Arc<Mutex<HashMap<String,String>>>, track_change: HashMap<String,String>, 
        sockets: &Vec<tokio::net::UdpSocket>, log:&str, log_servers: &Vec<&str>, comp_name:&str) {
    let mut send = false;
    let mut i=0;
    for sock in sockets {                            
        match sock.send(format!("{}\n",log).as_bytes()).await {
            Ok(_) => {
                log::debug!("Log sent to server {} for comp: {}", log_servers[i], comp_name);
                send = true;
            },
            Err(e) => log::error!("Error in sending log to server {} for comp: {}, OE: {}", log_servers[i], comp_name, e)
        }
        i+=1;
    }
    if send { //If log sent to at least one server that make db_track_change persistent.
        for item in track_change {
            match db_track_change.lock().get_mut(&item.0) {
                Some(track_change) => *track_change = item.1,
                None => log::error!("Can not find {} key in db_track_change.", item.0)
            }                                        
        }
    }
}