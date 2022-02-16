//Arc mutex for thread communication
use std::sync::Arc;
use parking_lot::Mutex;
use tokio::{ time::{self, Duration}};
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use super::init::{State, LogSources, ConfigData };


pub async fn check_partner_status(state:Arc<Mutex<State>>,partner_address:String, config_hash:String, mut config:ConfigData)   {        
    let pause_duration = 2000;
    println!("partner check started");
    while *state.lock()==State::MasterWaiting {
        std::thread::sleep(std::time::Duration::from_secs(1));
        println!("Waiting for an slave to connect!!");
    }
    let mut buf = [0; 256];
    let two_seconds = std::time::Duration::from_millis(pause_duration);
    
    // In a loop, read data from the socket and write the data back.
    loop  {
        match TcpStream::connect(&partner_address).await {
            Ok(mut socket) => {
                loop {
                    println!("Connection made in check partner");
                    match socket.write_all(&format!("What's up?\n").as_bytes()).await {                        
                        Ok(_) => {
                            //------------------------
                            println!("data send in check partner");
                            match time::timeout(Duration::from_secs(2), socket.read(&mut buf)).await.unwrap_or(Ok(0)) {  
                                //unwrap_or pass Ok(0) instead of writing error handling for timeout for simplicity we pass Ok(0) means putting 
                                //agent to master mode, but it's not appropriate way and made false positive logs.         
                                // socket closed                    
                                Ok(n) if n==0 => { //0 means socket closed or timeout happend in above line
                                    println!("This agent got to master mode because Answer with 0 length received");
                                    *state.lock() = State::Master;
                                },
                                Ok(n) => {
                                    println!("Process_incoming, data len:{}",n);                                    
                                    let message= String::from_utf8(buf[..n].to_vec()).unwrap();
                                    if message=="I'm slave\n" {
                                        *state.lock() = State::Master;
                                        println!("This agent go to master mode because incorrect message received from master")
                                    }
                                    else { //If message!="I'm slave\n", It's I'm master+query config hash splitted by \n
                                        let message = message.split("\n").collect::<Vec<&str>>();
                                        if message.len()==2 {
                                            if message[1]!=config_hash {
                                                match socket.write_all(&format!("New config").as_bytes()).await {
                                                    Ok(n) => {
                                                        let mut msg="".to_owned();                                                            
                                                        while msg.len()<9 || &msg[msg.len()-9..]!="***END***" {
                                                            match time::timeout(Duration::from_secs(2), socket.read(&mut buf)).await.unwrap_or(Ok(0)) {   
                                                                Ok(n) => {                                                                        
                                                                    let data= String::from_utf8(buf[..n].to_vec()).unwrap();
                                                                    msg = format!("{}{}", msg,data);
                                                                },
                                                                Err(e) => println!("Error in receiving new config")
                                                            }
                                                        }

                                                        if msg.len()>=9 {
                                                            let log_sources :LogSources = toml::from_str(&msg[..msg.len()-9]).unwrap(); 
                                                            config.log_sources = log_sources.log_sources;
                                                            msg = toml::to_string(&config).unwrap();
                                                            std::fs::write("./config.toml", msg).expect("Unable to write to config.toml");
                                                            use std::os::unix::process::CommandExt;
                                                            std::process::Command::new("/proc/self/exe").exec();
                                                            std::process::exit(0);
                                                        }
                                                        
                                                        
                                                    },
                                                    Err(e) => println!("Error in requesting new config")
                                                }
                                            }
                                        }
                                        println!("This agent go to slave mode, Because other side is in master mode");
                                        *state.lock() = State::Slave;
                                        }
                                },
                                Err(e) => {                
                                    *state.lock() = State::Master;
                                    println!("This agent got to master mode because failed to read from socket; err = {:?}", e);
                                    break;//Break the loop to make a new connection
                                }
                            }
                            //------------------------
                        },
                        Err(e) => {
                            println!("An error hapeened in writing to connection!, Error: {}", e);
                            break; //Break the loop to make a new connection
                        }                        
                    }
                    std::thread::sleep(two_seconds);                 
                }
            },
            Err(e) => {
                println!("This agent go to master mode, Because of connection error: {}", e);
                *state.lock() = State::Master;
            }
        }
        std::thread::sleep(two_seconds);
    };   
}

pub async fn process_incominng(mut socket:tokio::net::TcpStream, state:Arc<Mutex<State>>,config_hash:String,log_sources_text:String) {
    {        
        let mut buf = [0; 256]; 
        // In a loop, read data from the socket and write the data back.
        loop {            
            let n = match socket.read(&mut buf).await {                    
                // socket closed                    
                Ok(n) if n ==0 => return,
                Ok(n) => {
                    println!("Process_incoming, data len:{}",n);
                    let message= String::from_utf8(buf[..n].to_vec()).unwrap();            
                    println!("{}",message);
                    if message=="What's up?\n" {
                        println!("whats up received!!!!");
                        if *state.lock()!=State::Slave {                        
                            println!("If passed!");
                            match socket.write_all(&format!("I'm master\n{}",config_hash).as_bytes()).await {
                                Ok(_) => {
                                    *state.lock() = State::Master; //??Can be optimized
                                    println!("State set to master")
                                },
                                Err(e) => println!("Error on writing data to client connection, Error: {}", e)
                            }
                        }
                        else {
                            match socket.write_all(&format!("I'm slave\n").as_bytes()).await {
                                Ok(_) => println!("Message sent to client"),
                                Err(e) => println!("Error on writing data to client connection, Error: {}", e)
                            }                        
                        }                    
                    }
                    else if message=="New config" {                    
                        match socket.write_all(format!("{}***END***",log_sources_text).as_bytes()).await {
                            Ok(_) => println!("New config sent"),
                            Err(e) => println!("Error in sending config, Error: {}",e)
                        }
                    }
                },
                Err(e) => {
                    eprintln!("failed to read from socket; err = {:?}", e);
                    return;
                }
            };
        }
    }
}