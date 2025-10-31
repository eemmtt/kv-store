//will contain implementations for CLI get, set, delete...
use nix::sys::socket::{socket, connect, AddressFamily, SockFlag, SockType, UnixAddr};
use std::os::fd::{AsRawFd};
use nix::{errno::Errno};
use kv_shared::io::{KVConnection, KVKey, KVMsg, KVMsgType};

pub fn new_client_kvconnection() -> Result<KVConnection, Errno>{
    let sock_addr = UnixAddr::new("./kv.sock").unwrap();
    let sockfd = match socket(
        nix::sys::socket::AddressFamily::Unix, 
        nix::sys::socket::SockType::Stream,
        SockFlag::empty(),
        None, 
    ){
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("new_as_client socket error: {}", e);
            return Err(e);
        }
    };

    let _rc_conn = match connect(sockfd.as_raw_fd(), &sock_addr){
        Ok(rc) => rc,
        Err(e) => {
            eprintln!("new_as_client connect error: {}", e);
            return Err(e);
        } 
    };

    return Ok(KVConnection {
        fd: sockfd,
        mtu: 1024,
    });
}

pub fn kvc_get(connection: &mut KVConnection, key: &KVKey) -> Result<Vec<u8>, Errno> {

    let msg = KVMsg::new(KVMsgType::Get, key.to_bytes());

    match connection.send_kvmsg(msg){
        Ok(y) => y,
        Err(e) => return Err(e),
    };
    let response = connection.recv_kvmsg().unwrap();

    Ok(response.msg)
}

pub fn kvc_set(connection: &mut KVConnection, key: &KVKey, value: &Vec<u8>) -> Result<Vec<u8>, Errno> {

    /*todo: define a KVPAIR struct for serialization / deserialization? */
    let mut bytes: Vec<u8> = Vec::new();
    bytes.extend(key.to_bytes());
    bytes.extend(value);
    let msg = KVMsg::new(KVMsgType::Set, bytes);

    match connection.send_kvmsg(msg){
        Ok(y) => y,
        Err(e) => return Err(e),
    };
    let response = connection.recv_kvmsg().unwrap();

    /* todo: do something more specific here... */
    Ok(response.msg)
}

pub fn kvc_delete(connection: &mut KVConnection, key: &KVKey) -> Result<Vec<u8>, Errno>{
    let msg = KVMsg::new(KVMsgType::Delete, key.to_bytes());

    match connection.send_kvmsg(msg){
        Ok(y) => y,
        Err(e) => return Err(e),
    };
    let response = connection.recv_kvmsg().unwrap();

    Ok(response.msg)
}