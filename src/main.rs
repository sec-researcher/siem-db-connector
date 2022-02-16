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
    //hasher.update(b"hello world");
    //let result = hasher.finalize();
    //println!("{:x}", result);
 
    //Reading configuration and parse it
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
    init::init(config.app_socket,Arc::clone(&state));
    println!("init ended");
    //println!("call ./app listening_ip:listening_port remote_ip:remote_port");
    //let args: Vec<String> = env::args().collect();    
    //let partner = args[2].to_owned();
    let listener = TcpListener::bind(&config.listening_addr).await?;
    println!("Listening on network started!");
    tokio::spawn(com::check_partner_status(Arc::clone(&state), config.peer_addr,config_hash.to_owned(), toml::from_str(&config_text).unwrap()));
    
    for log_source in log_sources.log_sources {
        tokio::spawn(db::call_db(log_source));
    }
    loop {
        let (socket, _) = listener.accept().await?;
        
        tokio::spawn(com::process_incominng(socket, Arc::clone(&state),config_hash.to_owned(), log_sources_text.clone()));
    }
}





