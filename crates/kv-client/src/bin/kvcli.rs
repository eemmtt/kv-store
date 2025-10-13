use nix::sys::socket::{UnixAddr};
use nix::unistd::{close};
use std::str::from_utf8;
use kv_client::{kvc_connect, kvc_get, kvc_set, kvc_delete };

fn main() {
    println!("client: start");

    /* get cli args, parse them... */
    let sock_addr = UnixAddr::new("./kv.sock").unwrap();

    let connection = kvc_connect(sock_addr).unwrap();

    let mut val = kvc_get(&connection, "test").unwrap();
    let mut msg_got = from_utf8(&val.data[..val.size]).expect("invalid utf-8");
    println!("client: got '{}' from get()", msg_got);

    val = kvc_set(&connection, "test", "oogabooga").unwrap();
    msg_got = from_utf8(&val.data[..val.size]).expect("invalid utf-8");
    println!("client: got '{}' from set()", msg_got);

    val = kvc_delete(&connection, "test").unwrap();
    msg_got = from_utf8(&val.data[..val.size]).expect("invalid utf-8");
    println!("client: got '{}' from del()", msg_got);

    close(connection.fd).expect("close sockfd failed");

    println!("client: stop");

}
