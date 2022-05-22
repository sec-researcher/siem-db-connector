//Arc mutex for thread communication
use std::{sync::Arc};
use parking_lot::Mutex;
use tokio::{ time::{self, Duration}};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use super::init::{State, LogSources, ConfigData };
use std::collections::HashMap;

use tokio::net::UdpSocket;
use std::io;
pub async fn connect_on_udp(log_server: &str) -> io::Result<UdpSocket> {
    let sock = UdpSocket::bind("0.0.0.0:0").await?;
    sock.connect(log_server).await?;  
    io::Result::Ok(sock)    
}
use crate::prelude::ResultExt;

pub async fn send_data(partner_address:&str, change_track:&str,start_sign:&str,end_sign:&str) -> Result<bool, std::io::Error>   {    
    match TcpStream::connect(partner_address).await {
        Ok(mut socket) => {
                match socket.write_all(&format!("{}{}{}", start_sign,change_track, end_sign).as_bytes()).await {                        
                    Ok(_) => {
                        Ok(true)
                    },
                    Err(e) => {
                        Err(e)
                    }                        
                }               
        },
        Err(e) => {
            Err(e)
        }
    }    
}

pub async fn send_data_get_response(partner_address:&str, data:&str,start_sign:&str,end_sign:&str) -> Result<String, ComError>   {    
    match TcpStream::connect(partner_address).await {
        Ok(mut socket) => {
                match socket.write_all(&format!("{}{}{}", start_sign,data, end_sign).as_bytes()).await {                        
                    Ok(_) => {
                        match receive_data(&mut socket, "", "***END***","".to_string()).await {
                            Ok(data)=> {                                
                                Ok(data)
                            },
                            Err(e) => Err(e)
                        }
                    },
                    Err(e) => {
                        Err(ComError::IOError(e))
                    }                        
                }               
        },
        Err(e) => {
            Err(ComError::IOError(e))
        }
    }    
}

#[derive(Debug)]
pub enum ComError {
    IOError(std::io::Error),
    CustomError(&'static str)
}
pub async fn receive_data(socket: &mut tokio::net::TcpStream, start_sign:&str, end_sign: &str, mut msg: String) -> Result<String, ComError>  {
    let mut buf = [0; 256];
    let mut check_start = true;
    let end_sign_len = end_sign.len();

    while msg.len()<end_sign_len || &msg[msg.len()-end_sign_len..]!=end_sign {
        match time::timeout(Duration::from_secs(2), socket.read(&mut buf)).await.unwrap_or(Ok(0)) {   
            Ok(n) => {                                                                        
                let data= String::from_utf8(buf[..n].to_vec()).unwrap();
                msg = format!("{}{}", msg,data);
                if start_sign!="" && check_start && msg.len()>=start_sign.len(){
                    if &msg[..start_sign.len()-1]!=start_sign {
                        return Err(ComError::CustomError("Start sign is not matched"))
                    }
                    else {
                        check_start=false;
                    }
                }
            },
            Err(e) => {
                return Err(ComError::IOError(e));
            }
        }
    }
    Ok(msg.replace("***CHT***", "").replace("***END***", ""))
}


pub async fn check_partner_status(state:Arc<Mutex<State>>,partner_address:String, config_hash:String, mut config:ConfigData)   {            
    while *state.lock()==State::MasterWaiting {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        log::info!("Waiting for an slave to connect!!");
    }
    let mut buf = [0; 256];
    let ping_duration = std::time::Duration::from_millis(config.ping_duration);
    
    // In a loop, read data from the socket and write the data back.
    '_connect: loop  {
        log::warn!("trying to create a connection to partner for checking partner status.");
        match TcpStream::connect(&partner_address).await {            
            Ok(mut socket) => {
                'write: loop {       
                    match socket.write_all(&format!("What's up?\n").as_bytes()).await {                        
                        Ok(_) => {
                            log::info!("What's up message sent to check partner status.");
                            match time::timeout(Duration::from_secs(2), socket.read(&mut buf)).await.unwrap_or(Ok(0)) {  
                                //unwrap_or pass Ok(0) instead of writing error handling for timeout for simplicity we pass Ok(0) means putting 
                                //agent to master mode, but it's not appropriate way and made false positive logs.         
                                // socket closed                    
                                Ok(n) if n==0 => { //0 means socket closed or timeout happend in above line
                                    log::warn!("This agent go to master mode because answer with 0 length received or connection timed out");
                                    *state.lock() = State::Master;
                                },
                                Ok(n) => {
                                    let message= String::from_utf8(buf[..n].to_vec()).unwrap_or("".to_string());
                                    if message=="" {
                                        log::error!("Converting bytes to string fails in check_partner_status, so app will close current socket and open a new one");
                                        break 'write;
                                    }
                                    else if message=="I'm slave\n" {
                                        *state.lock() = State::Master;
                                        log::warn!("This agent go to master mode because incorrect message received from partner")
                                    }
                                    else { //If message!="I'm slave\n", It's I'm master+query config hash splitted by \n
                                        let message = message.split("\n").collect::<Vec<&str>>();
                                        if message.len()==2 {
                                            if message[1]!=config_hash {
                                                match socket.write_all(&format!("New config").as_bytes()).await {
                                                    Ok(_n) => {
                                                        let mut msg="".to_owned();                                                            
                                                        while msg.len()<9 || &msg[msg.len()-9..]!="***END***" {
                                                            match time::timeout(Duration::from_secs(2), socket.read(&mut buf)).await.unwrap_or(Ok(0)) {   
                                                                Ok(n) => {                                                                        
                                                                    let data= String::from_utf8(buf[..n].to_vec()).unwrap_or("".to_string());
                                                                    if data=="" {
                                                                        log::error!("Error in converting bytes to string in received new config");
                                                                        break 'write;
                                                                    }
                                                                    msg = format!("{}{}", msg,data);
                                                                },
                                                                Err(e) => log::error!("Error in receiving new config, OE: {}", e)
                                                            }
                                                        }
                                                        if msg.len()>=9 {
                                                            match toml::from_str::<LogSources>(&msg[..msg.len()-9]) {
                                                                Ok(log_sources) => {
                                                                    config.log_sources = log_sources.log_sources;
                                                                    msg = toml::to_string(&config).log_or("Can not convert toml object to string, empty string will be return", "".to_string());
                                                                    std::fs::write("/etc/mslog.toml", msg).log_or("Unable to write to /etc/mslog.toml",());
                                                                    use std::os::unix::process::CommandExt;
                                                                    log::warn!("App will restart for setting new config, result: {}", std::process::Command::new("/proc/self/exe").exec());
                                                                    std::process::exit(0);
                                                                },
                                                                Err(e) => log::error!("Received data for new config is not a valid toml format, OE: {}",e)
                                                            }
                                                        }
                                                    },
                                                    Err(e) => log::error!("Error in requesting new config, OE: {}", e)
                                                }
                                            }
                                        }
                                        log::warn!("This agent go to slave mode, Because other side is in master mode");
                                        *state.lock() = State::Slave;
                                    }
                                },
                                Err(e) => {                
                                    *state.lock() = State::Master;
                                    log::warn!("This agent go to master mode because failed to read from socket, OE: {}", e);
                                    break 'write;//Break the loop to make a new connection
                                }
                            }
                            //------------------------
                        },
                        Err(e) => {
                            log::error!("An error hapeened in sending what's up request,OE: {}", e);
                            break 'write; //Break the loop to make a new connection
                        }                        
                    }
                    tokio::time::sleep(ping_duration).await;
                }
            },
            Err(e) => {
                log::error!("Can not connect to partner so this agent go to master mode,OE: {}", e);
                *state.lock() = State::Master;
            }
        }
        tokio::time::sleep(ping_duration).await;
    };   
}

pub async fn process_incoming(mut socket:tokio::net::TcpStream, state:Arc<Mutex<State>>,
    config_hash:String,log_sources_text:String,db_track_change: Arc<Mutex<HashMap<String,String>>>) {
    {        
        let mut buf = [0; 1024]; 
        // In a loop, read data from the socket and write the data back.
        loop {            
            match socket.read(&mut buf).await {                    
                // socket closed                    
                Ok(n) if n ==0 => {
                    log::warn!("Size of received data is 0");
                    return
                },
                Ok(n) => {
                    log::info!("Process_incoming, data len:{}",n);
                    let message= String::from_utf8(buf[..n].to_vec()).log_or("Can not convert process_incoming data to string", "".to_string());                    
                    if message=="What's up?\n" {
                        log::info!("whats up message received!!!!");
                        if *state.lock()!=State::Slave {
                            match socket.write_all(&format!("I'm master\n{}",config_hash).as_bytes()).await {
                                Ok(_) => {
                                    log::info!("'I'm master' message sent successfully");
                                    if *state.lock()==State::MasterWaiting {
                                        *state.lock() = State::Master;
                                        log::warn!("Agent from MasterWaiting go to Master mode")
                                    }                                    
                                },
                                Err(e) => log::error!("Error in sending 'I'm master' message to client, OE: {}", e)
                            }
                        }
                        else {
                            match socket.write_all(&format!("I'm slave\n").as_bytes()).await {
                                Ok(_) => log::info!("'I'm slave' message sent to client successfully"),
                                Err(e) => log::error!("Error in sending 'I'm slave' message to client, OE: {}", e)
                            }                        
                        }                    
                    }
                    else if message=="New config" {                    
                        match socket.write_all(format!("{}***END***",log_sources_text).as_bytes()).await {
                            Ok(_) => log::info!("New config sent to client successfully"),
                            Err(e) => log::error!("Error in sending new config to client, OE: {}",e)
                        }
                    }
                    else if message=="init_db_track_change" {
                        let data  = serde_json::to_string(&*db_track_change.lock()).log_or("Can not convert db_track_change object to string", "".to_string());
                        match socket.write_all(&format!("{}***END***",data).as_bytes()).await {
                            Ok(_) => log::info!("db_track_change for init sent successfully"),
                            Err(e) => log::error!("Error on sending init db_track_change, OE:{}", e)
                        }
                    }
                    else if &message[..9]=="***CHT***" {
                        match receive_data(&mut socket, "", "***END***",message).await {
                            Ok(data)=> {
                                match serde_json::from_str(&data) {
                                    Ok(json) => {
                                        *db_track_change.lock() = json;
                                        std::fs::write("/var/log/mslog/db_track_change.json", data).log_or("Unable to write to db_track_change.json", ());
                                    },
                                    Err(e) => log::error!("Can not convert db_track_change data received over net to json, OE: {}",e)
                                }
                            },
                            Err(e) => log::error!("Error in receiving db_track_change data, OE: {:?}", e)
                        }
                    }
                },
                Err(e) => {
                    log::error!("Failed to read from socket, OE: {}", e);
                    return;
                }
            };
        }
    }
}