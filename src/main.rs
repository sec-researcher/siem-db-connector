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
#[derive(PartialEq,Clone,Copy,Debug)]
enum State {
    MasterWaiting,
    Master,
    Slave
}

use serde_derive::Deserialize;
#[derive(Deserialize)]
struct ConfigData {
    listening_addr: String,
    peer_addr: String,   
    app_socket: String,
    default_role: String,
    pause_duration: u16, //In mili second
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
fn init(app_socket: String,state:Arc<Mutex<State>>)
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

#[tokio::main(flavor = "multi_thread", worker_threads = 100)]
async fn main() -> Result<(), Box<dyn Error>> {     
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    //hasher.update(b"hello world");
    //let result = hasher.finalize();
    //println!("{:x}", result);
 
    //Reading configuration and parse it
    let config= std::fs::read_to_string("config.toml").expect("Can not read file").parse::<String>().expect("Error in parsing");
    hasher.update(config.as_bytes());
    let config_hash = format!("{:x}",hasher.finalize());
    let config: ConfigData = toml::from_str(&config).unwrap();

    let state;
    if config.default_role=="master" {
        state = Arc::new(Mutex::new(State::MasterWaiting));
        
    }
    else {
        state = Arc::new(Mutex::new(State::Slave));
    }
    
    init(config.app_socket,Arc::clone(&state));
    println!("init ended");
    //println!("call ./app listening_ip:listening_port remote_ip:remote_port");
    //let args: Vec<String> = env::args().collect();    
    //let partner = args[2].to_owned();
    let listener = TcpListener::bind(&config.listening_addr).await?;
    println!("Listening on network started!");
    let x = tokio::spawn(check_partner_status(Arc::clone(&state), config.peer_addr,config_hash.to_owned()));
    println!("After tokio partner!");
    for log_source in config.log_sources {
        tokio::spawn(call_db(log_source));
    }
    
    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(process_incominng(socket, Arc::clone(&state),config_hash.to_owned()));
    }
    //tokio::spawn(print_events(Arc::clone(&state)));
}



async fn call_db(log_source_config:LogSource)  {
    println!("Call DB");
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

async fn check_partner_status(state:Arc<Mutex<State>>,partner_address:String, config_hash:String)   {        
    let pause_duration = 2000;
    println!("partner check started");
    while *state.lock()==State::MasterWaiting {
        std::thread::sleep(std::time::Duration::from_secs(1));
        println!("Waiting for an slave to connect!!");
    }
    let mut buf = [0; 256];            
    let mut v = Vec::new();
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
                                    for i in 0..n {
                                        v.push(buf[i]);               
                                    }
                                    let message= String::from_utf8(v).unwrap();
                                    v= vec![]; // Redefining v vector because it's borrowed in last statement
                                        if message=="I'm slave\n" {
                                            *state.lock() = State::Master;
                                            println!("This agent go to master mode because incorrect message received from master")
                                        }
                                        else {
                                            let mut split = "some string 123 ffd".split("123");
                                            let message = message.split("\n").collect::<Vec<&str>>();
                                            if message.len()==2 {
                                                if message[1]!=config_hash {
                                                    match socket.write_all(&format!("New config").as_bytes()).await {
                                                        Ok(n) => {
                                                            match time::timeout(Duration::from_secs(2), socket.read(&mut buf)).await.unwrap_or(Ok(0)) {   
                                                                Ok(n) => {
                                                                    let mut vv= vec![]; 
                                                                    for i in 0..n {
                                                                        vv.push(buf[i]);               
                                                                    }
                                                                    let data= String::from_utf8(vv).unwrap();
                                                                    std::fs::write("./config.toml", data).expect("Unable to write to config.toml");
                                                                },
                                                                Err(e) => println!("Error in receiving new config")
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

async fn process_incominng(mut socket:tokio::net::TcpStream, state:Arc<Mutex<State>>,config_hash:String) {
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
                    println!("whats up received!!!!");
                    println!("{:?}", *state.lock());
                    let x = *state.lock();
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
                    let config= std::fs::read_to_string("config.toml").expect("Can not read file").parse::<String>().expect("Error in parsing");
                    match socket.write_all(config.as_bytes()).await {
                        Ok(_) => println!("New config sent"),
                        Err(e) => println!("Error in sending config, Error: {}",e)
                    }

                }
        }
    }
}


