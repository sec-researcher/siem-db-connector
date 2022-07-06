extern crate static_vcruntime;
use std::error::Error;
use tokio::net::TcpListener;
//Arc mutex for thread communication
use parking_lot::Mutex;
use std::sync::Arc;
//use std::env;
use init::{ConfigData, LogSources, State};
mod init;
//---------------------------------------------------------
mod com;
mod db;
mod prelude;
#[macro_use]
extern crate lazy_static;

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    use prelude::ResultExt;
    init::enable_logging();
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    let config_text = std::fs::read_to_string("/var/siem-db-connector/config.toml")
        .log_and_exit("Can not read file")
        .parse::<String>()
        .log_and_exit("Error in parsing");
    let config: ConfigData = init::load_config(&config_text); //toml::from_str(&config_text).log("/var/siem-db-connector/config.toml syntax error.");

    use validator::Validate;
    match config.validate() {
        Ok(_) => (),
        Err(e) => {
            prelude::fatal!("Error: {}", e);
            ()
        }
    }
    let all_log_sources_name = config.get_all_logsource_name();
    let log_sources = LogSources {
        log_sources: config.log_sources,
    };
    let log_sources_text =
        toml::to_string(&log_sources).log_and_exit("/var/siem-db-connector/config.toml syntax error.");
    hasher.update(log_sources_text.as_bytes());
    let config_hash = format!("{:x}", hasher.finalize());
    let state;
    if config.default_role == "master" {
        state = Arc::new(Mutex::new(State::MasterWaiting));
    } else {
        state = Arc::new(Mutex::new(State::Slave));
    }
    init::init(config.app_socket, Arc::clone(&state));

    let db_track_change = init::load_db_track_change(&config.peer_addr, all_log_sources_name).await;
    println!("init ended");

    let listener = TcpListener::bind(&config.listening_addr).await?;
    println!("Listening on network started!");
    tokio::spawn(com::check_partner_status(
        Arc::clone(&state),
        config.peer_addr.clone(),
        config_hash.to_owned(),
        toml::from_str(&config_text).unwrap(),
    ));

    if let Some(complicated) = config.comp {
        for comp in complicated {
            tokio::spawn(db::call_comp(
                Arc::clone(&state),
                comp,
                Arc::clone(&db_track_change),
                config.log_server.clone(),
            ));
        }
    }

    for log_source in log_sources.log_sources {
        tokio::spawn(db::call_db(
            Arc::clone(&state),
            log_source,
            Arc::clone(&db_track_change),
            config.log_server.clone(),
        ));
    }
    tokio::spawn(db::sync_db_change(
        Arc::clone(&db_track_change),
        config.peer_addr.clone(),
    ));
    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(com::process_incoming(
            socket,
            Arc::clone(&state),
            config_hash.to_owned(),
            log_sources_text.clone(),
            Arc::clone(&db_track_change),
        ));
    }
}
