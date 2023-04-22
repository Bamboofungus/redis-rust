// use std::net::{TcpListener, TcpStream};
// use std::io::{Read, Write};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::collections::HashMap;


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
        handle_stream_with_task(stream).await;
    }
}

async fn handle_stream_with_task(mut stream: TcpStream) {
    // create task to handle each stream opened
    tokio::task::spawn(async move {
        let mut buffer = [0; 1024];
        let mut hashtable: HashMap<String, String> = HashMap::new();
        loop {
            match stream.read(&mut buffer).await {
                Ok(0) => {
                    // conn closed
                    println!("Stream closed");
                    break;
                },
                Ok(n) => {
                    let message: &[u8] = &buffer[..n];
                    let (value, _) = parse_resp(message).unwrap();
                    println!("Received: {:?}", value);
                    if let RespValue::Array(vector) = value {
                        let (command, arguments) = vector.split_first().unwrap();
                        let res = handle_command(command, arguments, &mut hashtable);
                        stream.write_all(res.as_bytes()).await.unwrap();
                    }
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

fn handle_command(command: &RespValue, arguments: &[RespValue], hashtable: &mut HashMap<String, String>) -> String {
    if let RespValue::BulkString(string) = command {
        match string.to_uppercase().as_str(){
            "ECHO" => { 
                if arguments.len() != 1 {
                    "".to_string()
                } else if let RespValue::BulkString(string) = &arguments[0] {
                    echo(string).to_string()
                } else {
                    "".to_string()
                }
            },
            "SET" => {
                if arguments.len() != 2 {
                    "".to_string()
                } else if let (RespValue::BulkString(key), RespValue::BulkString(val)) = (&arguments[0], &arguments[1]) {
                    set(key, val, hashtable)
                } else {
                    "".to_string()
                }

            },
            "GET" => {
                if arguments.len() != 1 {
                    "".to_string()
                } else if let RespValue::BulkString(string) = &arguments[0] {
                    get(string, hashtable)
                } else {
                    "".to_string()
                }

            },
            _ => {
                println!("{}",string);
                pong()
            }
        }
    } else {
        pong()
    }

}
#[derive(Debug)]
enum RespValue {
    // SimpleString(String),
    // Error(String),
    // Integer(i64),
    BulkString(String),
    Array(Vec<RespValue>),
}

fn parse_resp(resp: &[u8]) -> Option<(RespValue, &[u8])> {
    if resp.is_empty() {
        return None
    }
    let (data_type, rest) = resp.split_first().unwrap();
    let (value, rest) = match data_type {
        // TODO +, -, :
        b'$' => {
            let (length, rest) = read_line(rest)?;
            let length = length.parse::<usize>().ok()?;
            let (data, rest) = rest.split_at(length);
            (RespValue::BulkString(String::from_utf8_lossy(data).into_owned()), &rest[2..])
        },
        b'*' => {
            let (length, rest) = read_line(rest)?;
            let length = length.parse::<usize>().ok()?;
            let mut array = Vec::with_capacity(length);
            let mut current_rest = rest;
            // recurse to parse elements of array
            for _ in 0..length {
                let (value, new_rest) = parse_resp(current_rest)?;
                current_rest = new_rest;
                array.push(value);
            }
            (RespValue::Array(array), rest)
        },
        _ => {
            return None
        }
    };
    Some((value, rest))
}

fn read_line(input: &[u8]) -> Option<(String, &[u8])> {
    let (line, rest) = input.split_at(input.iter().position(|&b| b == b'\r')?);
    // pass \n after \r
    let rest = &rest[2..];
    Some((String::from_utf8_lossy(line).into_owned(), rest))
}

fn pong() -> String {
    "+PONG\r\n".to_string()
}

fn echo(message: &str) -> String {
    format!("+{message}\r\n")
}

fn set(key: &str, val: &str, hashtable: &mut HashMap<String, String>) -> String{
    hashtable.insert(key.to_string(), val.to_string());
    "+OK\r\n".to_string()
}

fn get(key: &str, hashtable: &mut HashMap<String, String>) -> String {
    let value = hashtable.get(key).unwrap();
    format!("+{value}\r\n")
}