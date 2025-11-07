use kv_shared::io::{KVKey, KVValue};
use nix::errno::Errno;
use nix::sys::socket::{UnixAddr};
use nix::unistd::{close};
use std::io::{self, Write};
use std::str::from_utf8;
use kv_client::{kvc_delete, kvc_get, kvc_set, new_client_kvconnection };

fn main() {
    
    println!("client: start");
    println!("usage:\n\tget <key>\n|\tset <key> <value>\n|\tdelete <key>\n|\texit");
    println!("--------");
    let mut connection = match new_client_kvconnection(){
        Ok(c) => c,
        Err(Errno::ENOENT) => {
            eprintln!("client: couldn't connect to server");
            println!("client: stop");
            return;
        },
        Err(e) => {
            return;
        }
    };

    let stdin = io::stdin();
    loop{
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        stdin.read_line(&mut input).unwrap();

        let mut input_split = input.trim().split_whitespace();
        let command = match input_split.next(){
            Some(v) => v,
            None => {
                println!("bad command");
                continue;
            }
        };
        let key = input_split.next();
        let value = input_split.collect::<Vec<_>>().join(" ");
        
        match command {
            "get" => {
                if key.is_none(){ 
                    println!("no key specified");
                    continue;
                }
                let key = key.unwrap();
                let get_key = KVKey::new(key).unwrap();
                let get_result = match kvc_get(&mut connection, get_key){
                    Ok(v) => v,
                    Err(Errno::EPIPE) => {
                        eprintln!("server disconnected");
                        break;
                    },
                    Err(e) => {
                        eprintln!("kvc_get failed: {}", e);
                        break;
                    }
                };
                println!("get: '{}'", get_result.as_str());
            },
            "set" => {
                if key.is_none(){ 
                    println!("no key specified");
                    continue;
                }
                if value.len() == 0{
                    println!("no value specified");
                    continue;
                }
                let key = key.unwrap();
                let set_key = KVKey::new(key).unwrap();
                let set_val = KVValue::new(&value).unwrap();
                let set_result = match kvc_set(&mut connection, set_key, set_val){
                    Ok(v) => v,
                    Err(Errno::EPIPE) => {
                        eprintln!("server disconnected");
                        break;
                    },
                    Err(e) => {
                        eprintln!("kvc_set failed: {}", e);
                        break;
                    }
                };
                println!("set: '{}'", set_result.as_str());
            },
            "delete" => {
                if key.is_none(){ 
                    println!("no key specified");
                    continue;
                }
                let key = key.unwrap();
                let del_key = KVKey::new(key).unwrap();
                let del_result = match kvc_delete(&mut connection, del_key){
                    Ok(v) => v,
                    Err(Errno::EPIPE) => {
                        eprintln!("server disconnected");
                        break;
                    },
                    Err(e) => {
                        eprintln!("kvc_delete failed: {}", e);
                        break;
                    }
                };
                println!("delete: '{}'", del_result.as_str());
            },
            "exit" => {
                break;
            }
            _ => {
                println!("unrecognized command!");
            }
        }
    }

    close(connection.fd).expect("close sockfd failed");
    println!("client: stop");

}
