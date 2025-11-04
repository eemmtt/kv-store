use kv_shared::io::{KVKey, KVValue, KVValueType};
use nix::sys::socket::{UnixAddr};
use nix::unistd::{close};
use std::io::{self, Write};
use std::str::from_utf8;
use kv_client::{kvc_delete, kvc_get, kvc_set, new_client_kvconnection };

fn main() {
    
    println!("client: start");
    println!("usage: [get|set|delete|exit] [key] [value]");
    let mut connection = new_client_kvconnection().unwrap();

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
                println!("bad command, try again?");
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
                let get_result = kvc_get(&mut connection, &get_key).unwrap();
                let get_msg = get_result.to_string().unwrap();
                println!("client: got '{}' from key '{}'", get_msg, key);
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
                let set_val = KVValue::new(KVValueType::String, value.as_bytes().to_vec());
                let set_result = kvc_set(&mut connection, &set_key, &set_val).unwrap();
                let set_msg = from_utf8(&set_result).expect("invalid utf-8");
                println!("client: got '{}' from setting key '{}'", set_msg, key);
            },
            "delete" => {
                if key.is_none(){ 
                    println!("no key specified");
                    continue;
                }
                let key = key.unwrap();
                let del_key = KVKey::new(key).unwrap();
                let del_result = kvc_delete(&mut connection, &del_key).unwrap();
                let del_msg = from_utf8(&del_result).expect("invalid utf-8");
                println!("client: got '{}' from deleting key '{}'", del_msg, key);
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
