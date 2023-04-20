// Uncomment this block to pass the first stage
// use std::net::{TcpListener, TcpStream};
// use std::io::{Read, Write};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");    
    // TODO handle error
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (stream, _) = match listener.accept().await {
            Ok((_stream, addr)) => (_stream, addr),
            Err(e) => {
                eprintln!("Couldn't accept client: {:?}", e);
                break
            },
        };
        handle_stream_with_task(stream);
    }
}

fn handle_stream_with_task(mut stream: TcpStream) {
    // create task to handle each stream opened
    tokio::task::spawn(async move {
        let mut buffer = [0; 1024];
        loop {
            match stream.read(&mut buffer).await {
                Ok(0) => {
                    // conn closed
                    println!("Stream closed");
                    break;
                },
                Ok(n) => {
                    let message = &buffer[..n];
                    println!("Received: {:?}", String::from_utf8_lossy(message));
                    let res = "+PONG\r\n";
                    stream.write_all(res.as_bytes()).await.unwrap();
                },
                Err(_) => {
                    eprintln!("Error reading from stream: {:?}", stream);
                    break;
                }
            }
            buffer = [0; 1024];
        }        
    });   
}