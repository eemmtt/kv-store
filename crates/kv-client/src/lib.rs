//will contain implementations for CLI get, set, delete...
use nix::sys::socket::{socket, connect, AddressFamily, SockFlag, SockType, UnixAddr};
use std::os::fd::{AsRawFd};
use nix::{errno::Errno};
use kv_shared::{KVConnection, KVValue, recv_all, send_all};


pub fn kvc_connect(sock_addr: UnixAddr) -> Result<KVConnection, Errno>{
    
    let sockfd = match socket(
        AddressFamily::Unix, 
        SockType::Stream,
        SockFlag::empty(),
        None, 
    ){
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("kv_client::kvc_connect socket error: {}", e);
            return Err(e);
        }
    };

    let _rc_conn = match connect(sockfd.as_raw_fd(), &sock_addr){
        Ok(rc) => rc,
        Err(e) => {
            eprintln!("kv_client::kvc_connect connect error: {}", e);
            return Err(e);
        }
    };

    let connection = KVConnection {
        fd: sockfd,
        mtu: 1024,
    };
    return Ok(connection);
}

pub fn kvc_get(connection: &KVConnection, key: &str) -> Result<KVValue, Errno> {

    // form msg
    let msg = format!("GET {}", key).into_bytes();

    send_all(connection, msg).unwrap();
    let result = recv_all(connection).unwrap();

    Ok(result)
}

pub fn kvc_set(connection: &KVConnection, key: &str, value: &str) -> Result<KVValue, Errno> {

    // do something better...
    let msg = format!("SET {} {}", key, value).into_bytes();

    send_all(connection, msg).unwrap();
    let result = recv_all(connection).unwrap();

    Ok(result)
}

pub fn kvc_delete(connection: &KVConnection, key: &str) -> Result<KVValue, Errno>{
    // form msg
    let msg = format!("DEL {}", key).into_bytes();

    send_all(connection, msg).unwrap();
    let result = recv_all(connection).unwrap();

    Ok(result)
}