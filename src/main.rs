// use std::net::{TcpListener, TcpStream};
// use std::io::{Read, Write};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{UNIX_EPOCH, SystemTime};

#[derive(Clone)]
struct HashtableValue {
    value: String,
    expiration_time: u128
}

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");   
    let shared_hashmap: Arc<Mutex<HashMap<String, HashtableValue>>> = Arc::new(Mutex::new(HashMap::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (stream, _) = match listener.accept().await {
            Ok((_stream, addr)) => (_stream, addr),
            Err(e) => {
                eprintln!("Couldn't accept client: {:?}", e);
                break
            },
        };
        handle_stream_with_task(stream, &shared_hashmap).await;
    }
}

async fn handle_stream_with_task(mut stream: TcpStream, shared_hashmap : &Arc<Mutex<HashMap<String, HashtableValue>>>) {
    let client_hashmap: Arc<Mutex<HashMap<String, HashtableValue>>> = shared_hashmap.clone();
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
                    let message: &[u8] = &buffer[..n];
                    let (value, _) = parse_resp(message).unwrap();
                    println!("Received: {:?}", value);
                    if let RespValue::Array(vector) = value {
                        let (command, arguments) = vector.split_first().unwrap();
                        let res = handle_command(command, arguments,&client_hashmap);
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

fn handle_command(command: &RespValue, arguments: &[RespValue], hashtable: &Arc<Mutex<HashMap<String, HashtableValue>>>) -> String {
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
                match arguments {
                    [RespValue::BulkString(key), RespValue::BulkString(val)] => {
                        set(key, val, -1, hashtable)
                    },
                    [RespValue::BulkString(key), RespValue::BulkString(val), RespValue::BulkString(command), RespValue::BulkString(expiry)] if command == "PX" => {
                        set(key, val, expiry.parse::<i32>().unwrap(), hashtable)
                    },
                    _ => {
                        RespValue::Null.encode()
                    }
                }
            },
            "GET" => {
                if arguments.len() != 1 {
                    "".to_string()
                } else if let RespValue::BulkString(string) = &arguments[0] {
                    if let Some(hashtable_value) = get(string, hashtable) {
                        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                        if hashtable_value.expiration_time == 0 || (hashtable_value.expiration_time > now) {
                            let message = hashtable_value.value;
                            format!("+{message}\r\n")
                        } else {
                            RespValue::Null.encode()
                        }
                    } else {
                        RespValue::Null.encode()
                    }
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
    Null,
    BulkString(String),
    Array(Vec<RespValue>),
}

impl RespValue {
    pub fn encode(self) -> String {
        match &self {
            // TODO impl other encodes
            RespValue::Null => "$-1\r\n".to_string(),
            _ => panic!("value encode not implemented for: {:?}", self),
        }
    }
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

fn set(key: &str, val: &str, duration: i32, hashtable: &Arc<Mutex<HashMap<String, HashtableValue>>>) -> String {
    if duration > 0 {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let expiration_time = now.as_millis() + (duration as u128);
        hashtable.lock().unwrap().insert(key.to_string(), HashtableValue {value: val.to_string(), expiration_time: expiration_time});
    } else {
        // no expiration
        hashtable.lock().unwrap().insert(key.to_string(), HashtableValue {value: val.to_string(), expiration_time: 0 });
    }
    "+OK\r\n".to_string()
}

fn get(key: &str, hashtable: &Arc<Mutex<HashMap<String, HashtableValue>>>) -> Option<HashtableValue> {
   hashtable.lock().unwrap().get(key).cloned()
}