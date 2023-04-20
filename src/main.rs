// Uncomment this block to pass the first stage
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                handle_stream(_stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_stream(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(n) => {
                // end of stream
                if n == 0 {
                    break
                }
                let res = "+PONG\r\n";

                stream.write_all(res.as_bytes()).unwrap();
                // clear buffer
                buffer =  [0; 1024]
            },
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
        }
    }

}