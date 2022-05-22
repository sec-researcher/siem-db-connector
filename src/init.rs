//Arc mutex for thread communication
use std::{sync::Arc, collections::HashMap};
use parking_lot::Mutex;
//Logging dependencies
use log::{LevelFilter};
use log4rs::{
    config::Config,
    append::console::{ConsoleAppender, Target},
    append::file::FileAppender,
    config::{Appender, Root},
    encode::json::JsonEncoder,
    filter::threshold::ThresholdFilter,
};
use tokio::io::AsyncWriteExt;
use crate::com::ComError;

use super::prelude::{ ResultExt, fatal};


#[derive(PartialEq,Clone,Copy,Debug)]
pub enum State {
    MasterWaiting,
    Master,
    Slave
}
use serde_derive::{Deserialize, Serialize };

use validator::{Validate};
use regex::Regex;
lazy_static! {
    static ref IP_REG: Regex = Regex::new(r"^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$").unwrap();
    static ref IP_PORT_REG: Regex = Regex::new(r"^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:[0-9]{1,5}$").unwrap();
    static ref ROLE_REG: Regex = Regex::new(r"^(master|slave)$").unwrap();
}


#[derive(Deserialize,Serialize,Validate)]
pub struct ConfigData {
    #[validate(regex="IP_PORT_REG")]
    pub listening_addr: String,
    #[validate(regex="IP_PORT_REG")]
    pub peer_addr: String,
    #[validate(length(min = 1))]
    pub app_socket: String,
    #[validate(regex="ROLE_REG")]
    pub default_role: String,
    pub ping_duration: u64, //In mili second
    #[validate(regex="IP_PORT_REG")]
    pub log_server: String,
    #[validate]
    pub log_sources: Vec<LogSource>,
    #[validate]
    pub comp: Option<Vec<Comp>>
}
impl ConfigData {
    pub fn get_all_logsource_name(&self) -> Vec<(String, String)> {
        let mut v:Vec<(String,String)> = vec!();
        for i in 0..self.log_sources.len() {
            let item = &self.log_sources[i];
            if let Some(counter_default_value) = item.counter_default_value.clone() {
                v.push((item.name.clone(), counter_default_value.clone()));
            }            
        }        
        if let Some(complicated) = &self.comp {
            for comp in complicated {
                for item in &comp.log_sources {
                    if let Some(counter_default_value) = item.counter_default_value.clone() {
                        v.push((item.name.clone(), counter_default_value.clone()));
                    }
                }
            }
        }
        
        v
    }
    
    
}

#[derive(Clone,Deserialize,Serialize, Validate)]
pub struct Comp {
    #[validate(length(min = 1))]
    pub result: String,
    #[validate(length(min = 1))]
    pub name:String,
    #[validate]
    pub log_sources: Vec<LogSource>,
    #[validate(regex="IP_PORT_REG")]
    pub log_server: Option<String>,
    pub pause_duration: Option<u64>,
}


#[derive(Deserialize,Serialize,Validate)]
pub struct LogSources {
    pub log_sources: Vec<LogSource>
}

#[derive(Clone,Deserialize,Serialize, Validate)]
pub struct LogSource {
    #[validate(length(min = 1))]
    pub name: String,
    #[validate(regex = "IP_REG")]
    pub addr: String,
    #[validate(range(min = 0, max = 65535))]
    pub port: u16,
    #[validate(length(min = 1))]
    pub username: String,    
    pub pass: String,
    #[validate(length(min = 1))]
    pub query: String,
    #[validate(length(min = 1))]
    pub counter_field: Option<String>,
    #[validate(length(min = 1))]
    pub counter_default_value: Option<String>,
    pub hide_counter: Option<bool>,
    #[validate(regex="IP_PORT_REG")]
    pub log_server: Option<String>,
    pub pause_duration: Option<u64>, //In mili second
}

pub fn init(app_socket: String,state:Arc<Mutex<State>>) 
{
    use std::os::unix::net::UnixStream;
    //Check if another instance is running
    match UnixStream::connect(&app_socket) {
        Ok(mut stream) => {
            let mut response = String::new();
            use std::io::prelude::*; //Allow us to read and write from Unix sockets.
            //if let Some(response);
            if let Ok(i) = stream.read_to_string(&mut response) {
                println!("{}", response);
            }
            
            log::error!("Another instance is running, so this process will ends.");
            panic!("Another instance is running.");
        },
        //If error happenes means there is no other instance running
        Err(e) => {
            log::info!("It seems there's no other instance running, Original error: {}", e);
            tokio::spawn(unix_socket_listener(app_socket, state));
        }
    }
}




pub fn enable_logging()  {
    let level = log::LevelFilter::Info;
    let file_path = "/var/log/mslog/log";
    // Build a stderr logger.
    let stderr = ConsoleAppender::builder().encoder(Box::new(JsonEncoder::new())).target(Target::Stderr).build();
    // Logging to log file.
    let logfile = FileAppender::builder()
        // Pattern: https://docs.rs/log4rs/*/log4rs/encode/pattern/index.html
        .encoder(Box::new(JsonEncoder::new()))
        .build(file_path)
        .expect("Error in createing log4rs FileAppender");

    // Log Trace level output to file where trace is the default level
    // and the programmatically specified level to stderr.
    let config = Config::builder()
        .appender(Appender::builder()
            .filter(Box::new(ThresholdFilter::new(level)))
            .build("logfile", Box::new(logfile)))
        .appender(
            Appender::builder()
                .filter(Box::new(ThresholdFilter::new(level)))
                .build("stderr", Box::new(stderr)),
        )
        .build(
            Root::builder()
                .appender("logfile")
                .appender("stderr")
                .build(LevelFilter::Trace),
        )
        .expect("Error in building log4rs config");

    // Use this to change log levels at runtime.
    // This means you can change the default log level to trace
    // if you are trying to debug an issue and need more logs on then turn it off
    // once you are done.
    let _handle = log4rs::init_config(config).expect("Error in initializing log4rs config");
}

pub async fn load_db_track_change(partner_address:&str,all_log_sources_name:Vec<(String,String)>) -> Arc<Mutex<HashMap<String,String>>> {    
    use tokio::{ time::{self, Duration}};
    let mut db_track_change:HashMap<String,String> = HashMap::new();

    match time::timeout(Duration::from_secs(2), 
        super::com::send_data_get_response(partner_address, "init_db_track_change", "", "")).await.unwrap_or(
            Err(ComError::CustomError("Timeout"))
        ) {
    //match super::com::send_data_get_response(partner_address, "init_db_track_change", "", "").await {
        Ok(data) => db_track_change = serde_json::from_str(&data).log("Can not deserialize db_track_change data received from partner"),        
        Err(e) => {
            log::warn!("Could not connect to '{}' as partner to load db_track_change, OE: {:?}", partner_address, e);
            match std::fs::read_to_string("/var/log/mslog/db_track_change.json") {
                Ok(mut data) => {
                    if data=="" {
                        data="{}".to_owned();
                        log::info!("db_track_change.json is empty");
                    }
                    db_track_change = serde_json::from_str(&data).log("Can not deserialize db_track_change data readed from disk");
                    for item in all_log_sources_name {                        
                        match db_track_change.get_mut(&item.0) {
                            Some(value) => { 
                                if value=="" {
                                    *value = item.1;
                                    log::warn!("db_track_change key: {} was empty, so it sets by default value", item.0)                                    
                                }
                            },
                            None => { 
                                db_track_change.insert(item.0.to_string(),item.1.to_string() );                                                                
                            }
                        }
                    }
                }
                Err(e) => {
                    fatal!("Error in reading file content /var/log/mslog/db_track_change.json, OE:{}",e);                    
                }
            }
        }
    }
    Arc::new(Mutex::new(db_track_change))
}

async fn unix_socket_listener(app_socket: String,state:Arc<Mutex<State>>) {
    //use std::os::unix::net::UnixListener;
    use tokio::net::UnixListener;
    use std::io::prelude::*; //Allow us to read and write from Unix sockets.    
    if std::path::Path::new(&app_socket).exists() {
        std::fs::remove_file(&app_socket).log("There is a file in path specified by config.app_socket, this process can not delete it");        
    }
    let listener = UnixListener::bind(&app_socket).log("Can not bind to unix socket");
    //Set read only permision on file to protect if from accidental deletion.
    //let mut perms = std::fs::metadata(&config.app_socket).expect("Can not get socket permission.").permissions();
    //perms.set_readonly(true);
    //std::fs::set_permissions(&config.app_socket,perms).expect("Can not set readonly permission.");
     
    // accept connections and process them, spawning a new thread for each one
    println!("Unix socket listener is waiting!!");
    loop  {
        match listener.accept().await {
            Ok((mut stream, _addr)) => {
                let msg = format!("Agent is in {:?} mode", *state.lock());
                stream.write( msg.as_bytes() ).await.log("Error in writing to unix socket");
            }
            Err(err) => {
                log::warn!("Creating incoming connection failed, OE: {}", err);
                break;
            }
        }
    }
}


