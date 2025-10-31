use kv_shared::io::KVKey;
use nix::sys::socket::{UnixAddr};
use nix::unistd::{close};
use std::str::from_utf8;
use kv_client::{kvc_delete, kvc_get, kvc_set, new_client_kvconnection };

fn main() {
    
    // todo get cli args, parse them...
    
    println!("client: start");
    let mut connection = new_client_kvconnection().unwrap();

    /* try GET */
    let get_key = KVKey::new("test").unwrap();
    let get_result = kvc_get(&mut connection, &get_key).unwrap();
    let get_msg = from_utf8(&get_result).expect("invalid utf-8");
    println!("client: got '{}' from get()", get_msg);

    /* try SET */
    let set_key = KVKey::new("test").unwrap();
    let set_val: Vec<u8> = "hello darling".as_bytes().to_vec();
    let set_result = kvc_set(&mut connection, &set_key, &set_val).unwrap();
    let set_msg = from_utf8(&set_result).expect("invalid utf-8");
    println!("client: got '{}' from set()", set_msg);

    /* try DEL */
    let del_key = KVKey::new("test").unwrap();
    let del_result = kvc_delete(&mut connection, &del_key).unwrap();
    let del_msg = from_utf8(&del_result).expect("invalid utf-8");
    println!("client: got '{}' from del()", del_msg);

    close(connection.fd).expect("close sockfd failed");
    println!("client: stop");

}
