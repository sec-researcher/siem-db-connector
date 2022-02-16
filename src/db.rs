use tiberius::{Client, Config, AuthMethod};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use super::init::{LogSource};


pub async fn call_db(log_source_config:LogSource)  {
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