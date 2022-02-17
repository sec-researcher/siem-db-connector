//Arc mutex for thread communication
use std::{sync::Arc, collections::HashMap};
use parking_lot::Mutex;

#[derive(PartialEq,Clone,Copy,Debug)]
pub enum State {
    MasterWaiting,
    Master,
    Slave
}


use serde_derive::{Deserialize, Serialize };
#[derive(Deserialize,Serialize)]
pub struct ConfigData {
    pub listening_addr: String,
    pub peer_addr: String,   
    pub app_socket: String,
    pub default_role: String,
    pub pause_duration: u16, //In mili second
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
    pub query: String
}

pub fn init(app_socket: String,state:Arc<Mutex<State>>) 
{
    use std::os::unix::net::UnixStream;
    use std::io::prelude::*; //Allow us to read and write from Unix sockets.
    //Check if another instance is running
    
    match UnixStream::connect(&app_socket) {
        Ok(mut stream) => {
            println!("Another instance is running..");
            let mut response = String::new();
            stream.read_to_string(&mut response).expect("Error happend in reading response from unix socket");
            println!("{}", response);
            std::process::exit(0);
        },
        //If error happenes means there is no other instance running
        Err(e) => {
            println!("Go to listening mode, Error: {}",e);
            tokio::spawn(unix_socket_listener(app_socket, state));
        }
    }


}

pub fn update_db_track_change_from_disk(db_track_change:Arc<Mutex<HashMap<String,String>>>) {
    match std::fs::read_to_string("./db_track_change.json") {
        Ok(mut data) => {
            if data=="" {
                data="{}".to_owned();
            }
            *db_track_change.lock() = serde_json::from_str(&data).unwrap();
        }
        Err(e) => {
            println!("Error in reading file content");
            std::process::exit(0)
        }
    }
}

async fn unix_socket_listener(app_socket: String,state:Arc<Mutex<State>>) {
    use std::os::unix::net::UnixListener;
    use std::io::prelude::*; //Allow us to read and write from Unix sockets.    
    if std::path::Path::new(&app_socket).exists() {
        std::fs::remove_file(&app_socket).expect("Can not delete file")
    }
    let listener = UnixListener::bind(&app_socket).expect("Can not bind to unix socket");
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
                    stream.write_all( msg.as_bytes() ).expect("Error in writing to unix socket")
                });
            }
            Err(err) => {
                /* connection failed */
                break;
            }
        }
    }
}