extern crate static_vcruntime;
use std::{error::Error};
use tokio::net::TcpListener;
//Arc mutex for thread communication
use std::sync::Arc;
use parking_lot::Mutex;
//use std::env;
use init::{State, LogSources, ConfigData};
mod init;
//---------------------------------------------------------
mod com;
mod db;


#[tokio::main(flavor = "multi_thread", worker_threads = 100)]
async fn main() -> Result<(), Box<dyn Error>> {     
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    
    let config_text= std::fs::read_to_string("config.toml").expect("Can not read file").parse::<String>().expect("Error in parsing");
    let config: ConfigData = toml::from_str(&config_text).unwrap();
    let log_sources =  LogSources { log_sources: config.log_sources};
    let log_sources_text = toml::to_string(&log_sources).unwrap();
    hasher.update(log_sources_text.as_bytes());
    let config_hash = format!("{:x}",hasher.finalize());
    let state;
    if config.default_role=="master" {
        state = Arc::new(Mutex::new(State::MasterWaiting));
    }
    else {
        state = Arc::new(Mutex::new(State::Slave));
    }

    use std::collections::HashMap;
    let db_track_change=Arc::new(Mutex::new(HashMap::new()));
    //*db_track_change.lock() = serde_json::from_str(&init::init(config.app_socket,Arc::clone(&state))).unwrap();
    println!("Initializing.....");
    init::update_db_track_change(Arc::clone(&db_track_change), &config.peer_addr).await;
    println!("init ended");
    
    let listener = TcpListener::bind(&config.listening_addr).await?;
    println!("Listening on network started!");
    tokio::spawn(com::check_partner_status(Arc::clone(&state), config.peer_addr.clone(),config_hash.to_owned(), toml::from_str(&config_text).unwrap()));
    
    
    for log_source in log_sources.log_sources {
        if db_track_change.lock().contains_key(&log_source.name)==false {
            db_track_change.lock().insert(log_source.name.to_owned(), "".to_owned());
        }        
        tokio::spawn(db::call_db(Arc::clone(&state),log_source, Arc::clone(&db_track_change)));
    }
    tokio::spawn(db::sync_db_change(Arc::clone(&db_track_change),config.peer_addr.clone()));
    loop {
        let (socket, _) = listener.accept().await?;        
        tokio::spawn(com::process_incominng(socket, Arc::clone(&state),
        config_hash.to_owned(), log_sources_text.clone(), Arc::clone(&db_track_change)));
    }
}



//hasher.update(b"hello world");
    //let result = hasher.finalize();
    //println!("{:x}", result);
 
    //Reading configuration and parse it


//println!("call ./app listening_ip:listening_port remote_ip:remote_port");
    //let args: Vec<String> = env::args().collect();    
    //let partner = args[2].to_owned();