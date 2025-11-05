//will contain implementations for CLI get, set, delete...
use nix::{libc::EINVAL, sys::socket::{AddressFamily, SockFlag, SockType, UnixAddr, connect, socket}};
use std::{os::fd::AsRawFd, time::Duration};
use nix::{errno::Errno};
use kv_shared::io::{KEY_SIZE, KVConnection, KVKey, KVMsg, KVMsgType, KVValue, VAL_SIZE};


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
            eprintln!("new_client_kvconnection socket error: {}", e);
            return Err(e);
        }
    };

    let _rc_conn = match connect(sockfd.as_raw_fd(), &sock_addr){
        Ok(rc) => rc,
        Err(e) => {
            eprintln!("new_client_kvconnection connect error: {}", e);
            return Err(e);
        } 
    };

    return Ok(KVConnection {
        fd: sockfd,
        mtu: 1024,
    });
}

pub fn kvc_get(connection: &mut KVConnection, key: KVKey) -> Result<KVValue, Errno> {

    let val = KVValue::new("").unwrap();
    let msg = KVMsg::new(KVMsgType::Get, key, val);

    match connection.send_kvmsg(msg){
        Ok(y) => y,
        Err(e) => return Err(e),
    };
    let response = connection.recv_kvmsg().expect("recv_kvmsg fail");

    let _return_key = KVKey::from_bytes(&response.data[..KEY_SIZE]).unwrap();
    let return_val = KVValue::from_bytes(&response.data[KEY_SIZE..KEY_SIZE + VAL_SIZE]).unwrap(); 

    if response.msgtype as u32 == KVMsgType::GetReturn as u32{
        Ok(return_val)
    } else {
        eprintln!("kvc_get: unhandled response msgtype {}", response.msgtype as u32);
        Err(Errno::EINVAL)
    }
}

pub fn kvc_set(connection: &mut KVConnection, key: KVKey, value: KVValue) -> Result<KVValue, Errno> {

    let msg = KVMsg::new(KVMsgType::Set, key, value);

    match connection.send_kvmsg(msg){
        Ok(y) => y,
        Err(e) => return Err(e),
    };
    let response = connection.recv_kvmsg().expect("recv_kvmsg fail");
    let _return_key = KVKey::from_bytes(&response.data[..KEY_SIZE]).unwrap();
    let return_val = KVValue::from_bytes(&response.data[KEY_SIZE..KEY_SIZE + VAL_SIZE]).unwrap(); 

    if response.msgtype as u32 == KVMsgType::SetReturn as u32{
        Ok(return_val)
    } else {
        eprintln!("kvc_set: unhandled response msgtype {}", response.msgtype as u32);
        Err(Errno::EINVAL)
    }
}

pub fn kvc_delete(connection: &mut KVConnection, key: KVKey) -> Result<KVValue, Errno>{
    
    let val = KVValue::new("").unwrap();
    let msg = KVMsg::new(KVMsgType::Delete, key, val);

    match connection.send_kvmsg(msg){
        Ok(y) => y,
        Err(e) => return Err(e),
    };
    let response = connection.recv_kvmsg().unwrap();
    let _return_key = KVKey::from_bytes(&response.data[..KEY_SIZE]).unwrap();
    let return_val = KVValue::from_bytes(&response.data[KEY_SIZE..KEY_SIZE + VAL_SIZE]).unwrap(); 

    if response.msgtype as u32 == KVMsgType::DeleteReturn as u32{
        Ok(return_val)
    } else {
        eprintln!("kvc_set: unhandled response msgtype {}", response.msgtype as u32);
        Err(Errno::EINVAL)
    }
}