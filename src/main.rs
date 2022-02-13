extern crate static_vcruntime;
use tiberius::{Client, Config, AuthMethod};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use std::{error::Error};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{ time::{self, Duration}};
//Arc mutex for thread communication
use std::sync::Arc;
use parking_lot::Mutex;
//use std::env;

//---------------------------------------------------------
#[derive(PartialEq,Clone,Copy)]
enum State {
    Master,
    Slave
}

use serde_derive::Deserialize;
#[derive(Deserialize)]
struct ConfigData {
    listening_addr: String,
    peer_addr: String,   
    log_sources: Vec<LogSource>
}
#[derive(Deserialize)]
struct LogSource {
    name: String,
    addr: String,
    port: u16,
    username: String,
    pass: String,
    query: String
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config= std::fs::read_to_string("config.toml").expect("Can not read file").parse::<String>().expect("Error in parsing");
    let config: ConfigData = toml::from_str(&config).unwrap();

    //println!("call ./app listening_ip:listening_port remote_ip:remote_port");
    //let args: Vec<String> = env::args().collect();    
    //let partner = args[2].to_owned();
    let listener = TcpListener::bind(&config.listening_addr).await?;
    let state = Arc::new(Mutex::new(State::Master));
    tokio::spawn(check_partner_status(Arc::clone(&state), config.peer_addr));
    for log_source in config.log_sources {
        tokio::spawn(call_db(log_source));
    }
    
    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(process_incominng(socket, Arc::clone(&state)));
    }
    //tokio::spawn(print_events(Arc::clone(&state)));
}

async fn call_db(log_source_config:LogSource)  {
    let two_seconds = std::time::Duration::from_millis(2000);
    loop {
        let mut config = Config::new();
        config.host(&log_source_config.addr);
        config.port(log_source_config.port);
        config.authentication(AuthMethod::sql_server(&log_source_config.username, &log_source_config.pass));
        config.trust_cert(); // on production, it is not a good idea to do this           
        match TcpStream::connect(config.get_addr()).await {
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
                        loop {
                            let stream = client.query(&log_source_config.query, &[&-4i32]).await.unwrap();
                            match stream.into_first_result().await {
                                Ok(rows) => {
                                    for item in rows {
                                        let s: String = item.get::<&str, &str>("name").unwrap().to_owned();
                                        println!("log_source of {}: {}",log_source_config.name, s);
                                    }
                                },
                                Err(e) => {
                                    println!("Error on calling inot_first_result, Original: {}", e);
                                    break;
                                }
                            }
                            std::thread::sleep(two_seconds);
                        }
                    },
                    Err(e) => println!("An error happened!, Original: {}",e)
                }
            },
            Err(e) => println!("Error on connecting to databse, Original: {}", e)
        }
        std::thread::sleep(two_seconds);
    }       
}

async fn check_partner_status(state:Arc<Mutex<State>>,partner_address:String)   {        
    let mut buf = [0; 256];            
    let mut v = Vec::new();
    let two_seconds = std::time::Duration::from_millis(2000);
    
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
                                    for i in 0..n {
                                        v.push(buf[i]);               
                                    }
                                    let message= String::from_utf8(v).unwrap();
                                    v= vec![]; // Redefining v vector because it's borrowed in last statement
                                        if message!="I'm master\n" {
                                            *state.lock() = State::Master;
                                            println!("This agent go to master mode because incorrect message received from master")
                                        }
                                        else {
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

async fn process_incominng(mut socket:tokio::net::TcpStream, state:Arc<Mutex<State>>) {
    {
        let mut buf = [0; 256];            
        let mut v = Vec::new();
        // In a loop, read data from the socket and write the data back.
        loop {            
            let n = match socket.read(&mut buf).await {                    
                // socket closed                    
                Ok(n) if n ==0 => return,
                Ok(n) => {println!("Process_incoming, data len:{}",n);n},
                Err(e) => {
                    eprintln!("failed to read from socket; err = {:?}", e);
                    return;
                }
            };
            for i in 0..n {
                v.push(buf[i]);               
            }
            let message= String::from_utf8(v).unwrap();
            v= vec![]; // Redefining v vector because it's borrowed in last statement
            println!("{}",message);
                if message=="What's up?\n" {
                    println!("whats up received");
                    if *state.lock()==State::Master {
                        match socket.write_all(&format!("I'm master\n").as_bytes()).await {
                            Ok(_) => println!("Message sent to client"),
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
        }
    }
}


