#[macro_export]
macro_rules! fatal {
    () => {
        log::error!("config panic");
        panic!()
    };
    ($msg:expr) => {
        log::error!($msg);
        panic!()
    };
    ($msg:expr) => {
        log::error!($msg);
        panic!()
    };
    ($fmt:expr, $($arg:tt)+) => {
        let msg = format!($fmt, $($arg)+);
        log::error!("{}", &msg);
        panic!()
    };

}

//https://github.com/abreis/tracing-unwrap/
use std::fmt;

pub trait OptionExt<T> {
    fn log_or(self, msg: &str, value:T) -> T;
}
impl<T> OptionExt<T> for Option<T> {
    fn log_or(self, msg: &str, value:T) -> T {
        match self {
            Some(val) => val,
            None => {
                log::error!( "{}", msg);
                value
            }
        }
    }
}

pub trait ResultExt<T, E> {
    fn log(self, msg: &str) -> T
    where
        E: fmt::Debug;
    
    fn log_or(self, msg: &str, return_value:T) -> T
    where
        E: fmt::Debug;
}
impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn log(self, msg: &str) -> T
    where
        E: fmt::Debug,
    {
        match self {
            Ok(t) => t,
            Err(e) => {                
                log::error!( "{}, OE:{:?}",msg,e );
                std::process::exit(0)
            }
        }
    }

    fn log_or(self, msg: &str, return_value: T) -> T
    where
        E: fmt::Debug,
    {
        match self {
            Ok(t) => t,
            Err(e) => {
                log::error!( "{}, OE:{:?}",msg,e );
                return_value
            }
        }
    }
}



//Arc mutex for thread communication
use std::{sync::Arc, collections::HashMap};
use parking_lot::Mutex;
//Logging dependencies
use log::{LevelFilter, SetLoggerError};
use log4rs::{
    config::Config,
    append::console::{ConsoleAppender, Target},
    append::file::FileAppender,
    config::{Appender, Root},
    encode::json::JsonEncoder,
    filter::threshold::ThresholdFilter,
};


#[derive(PartialEq,Clone,Copy,Debug)]
pub enum State {
    MasterWaiting,
    Master,
    Slave
}
use serde::de::value::Error;
use serde_derive::{Deserialize, Serialize };
#[derive(Deserialize,Serialize)]
pub struct ConfigData {
    pub listening_addr: String,
    pub peer_addr: String,   
    pub app_socket: String,
    pub default_role: String,
    pub pause_duration: u16, //In mili second
    pub log_server: String,
    pub log_sources: Vec<LogSource>,
    pub comp: Vec<Comp>    
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
        for comp in &self.comp {
            for item in &comp.log_sources {
                if let Some(counter_default_value) = item.counter_default_value.clone() {
                    v.push((item.name.clone(), counter_default_value.clone()));
                }
            }
        }
        v
    }
    
    
}

#[derive(Clone,Deserialize,Serialize)]
pub struct Comp {
    pub result: String,
    pub log_sources: Vec<LogSource>
}


#[derive(Deserialize,Serialize)]
pub struct LogSources {
    pub log_sources: Vec<LogSource>
}

#[derive(Clone,Deserialize,Serialize)]
pub struct LogSource {
    pub name: String,
    pub addr: String,
    pub port: u16,
    pub username: String,
    pub pass: String,
    pub query: String,
    pub counter_field: Option<String>,
    pub counter_default_value: Option<String>,
    pub hide_counter: Option<bool>
}

pub fn init(app_socket: String,state:Arc<Mutex<State>>) 
{
    use std::os::unix::net::UnixStream;
    //Check if another instance is running
    match UnixStream::connect(&app_socket) {
        Ok(mut _stream) => {
            // let mut response = String::new();
            //use std::io::prelude::*; //Allow us to read and write from Unix sockets.
            // //if let Some(response);
            // if let Ok(i) = stream.read_to_string(&mut response) {
            //     log::info!("Can not read response of unix socket, returned value is: {}",i);
            // }
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
    let file_path = "./log";
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
    let mut db_track_change: HashMap<String,String> = HashMap::new();
    match super::com::send_data_get_response(partner_address, "init_db_track_change", "", "").await {
        Ok(data) => db_track_change = serde_json::from_str(&data).log("Can not deserialize db_track_change data received from partner"),        
        Err(e) => { 
            match std::fs::read_to_string("./db_track_change.json") {
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
                    fatal!("Error in reading file content, OE:{}",e);                    
                }
            }
        }
    }
    Arc::new(Mutex::new(db_track_change))
}

async fn unix_socket_listener(app_socket: String,state:Arc<Mutex<State>>) {
    use std::os::unix::net::UnixListener;
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
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                /* connection succeeded */
                let state = Arc::clone(&state);
                std::thread::spawn(move || {
                    let msg;
                    if *state.lock()==State::Master {
                        msg = format!("Agent is Master.");
                    }
                    else {
                        msg = format!("Agent is Slave.");
                    }
                    stream.write_all( msg.as_bytes() ).log("Error in writing to unix socket")
                });
            }
            Err(err) => {
                log::warn!("Creating incoming connection failed, OE: {}", err);
                break;
            }
        }
    }
}