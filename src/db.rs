use tiberius::{Client, Config, AuthMethod};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use crate::init::{State};

use super::init::{LogSource};

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

pub async fn call_db(state:Arc<Mutex<State>>,log_source_config:LogSource, db_track_change: Arc<Mutex<HashMap<String,String>>>)  {
    println!("Call DB");
    let two_seconds = std::time::Duration::from_millis(1000);
    let mut counter;
    let mut connection_wait = true;
    if *db_track_change.lock().get(&log_source_config.name).unwrap()=="" {
        counter = log_source_config.counter_default_value;
    }
   
    
    //let mut current_state = *state.lock();
    //let mut last_state = current_state;
    loop {
        // current_state = *state.lock();
        // if last_state!=current_state && current_state==State::Master {
        //     super::init::update_db_track_change_from_disk(Arc::clone(&db_track_change));   
        //     println!("-------update track chnage")         
        // }
        // last_state = current_state;

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
            //match TcpStream::connect(config.get_addr()).await {
                Ok(tcp) => {
                    match tcp.set_nodelay(true) {
                        Ok(_) =>(),
                        Err(e) => println!("Error in set_nodelay function, Original: {}",e)
                    }
                    // To be able to use Tokio's tcp, we're using the `compat_write` from
                    // the `TokioAsyncWriteCompatExt` to get a stream compatible with the
                    // traits from the `futures` crate.
                    match Client::connect(config, tcp.compat_write()).await {
                        Ok(mut client) => {
                            println!("Authenticate successfully");
                            while *state.lock()!=State::Slave {                                
                                counter = db_track_change.lock().get(&log_source_config.name).unwrap().parse::<String>().unwrap();
                                let query = log_source_config.query.replace("??", &counter.to_string());
                                let stream = client.query(&query, &[]).await.unwrap();
                                match stream.into_first_result().await {
                                    Ok(rows) => {
                                        for item in rows {
                                            let x = item.get_data();
                                            counter= item.get_filed_data_as_string(&*log_source_config.counter_field);
                                            println!("Counter: {}", counter);
                                            *db_track_change.lock().get_mut(&log_source_config.name).unwrap()=counter;
                                            println!("{}", x);
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