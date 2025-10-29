use nix::sys::socket::{ send, recv, MsgFlags };
use std::{os::fd::{ AsRawFd, OwnedFd, RawFd}, path::Path, u32};
use nix::{libc, errno::Errno, libc::size_t};

pub struct KVConnection{
    pub fd: OwnedFd,
    pub mtu: size_t,
}

pub struct KVValue{
    pub data: Vec<u8>,
    pub size: usize,
}

/// Ensures full receipt of msg from KVConnection
pub fn recv_all(connection: &KVConnection) -> Result<KVValue, Errno>{

    // receive msg length
    let mut len_buf= [0u8;4];
    let mut nbytes_len_recvd: usize = 0;
    while nbytes_len_recvd < len_buf.len() {
        match recv(connection.fd.as_raw_fd(), &mut len_buf[nbytes_len_recvd..], MsgFlags::empty()){
            Ok(0) => return Err(Errno::ECONNRESET),
            Ok(n) => nbytes_len_recvd += n,
            Err(e) => {
                eprintln!("kv_shared::kvs_recv_all recv msg.len() error: {}", e);
                return Err(e);
            }
        }
    }

    // receive msg[length]
    let msg_len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; msg_len];
    let mut nbytes_recvd: usize = 0;
    while nbytes_recvd < msg_len {
        match recv(connection.fd.as_raw_fd(), &mut buf[nbytes_recvd..], MsgFlags::empty()){
            Ok(0) => return Err(Errno::ECONNRESET),
            Ok(n) => nbytes_recvd += n,
            Err(e) => {
                eprintln!("kv_shared::kvs_recv_all recv msg[len] error: {}", e);
                return Err(e);
            }
        }
    }

    let result = KVValue {
        data: buf,
        size: nbytes_recvd,
    };
    Ok(result)
}


/// Ensures full send of msg from KVConnection
pub fn send_all(connection: &KVConnection, msg: Vec<u8>) -> Result<(), Errno>{
    
    if msg.len() > u32::MAX as usize {
        return Err(Errno::EMSGSIZE);
    }

    let len = msg.len() as u32;
    let len_buf = len.to_be_bytes() as [u8; 4];
    let mut nbytes_len_sent: usize = 0;
    while nbytes_len_sent < 4 {
        match send(connection.fd.as_raw_fd(), &len_buf[nbytes_len_sent..], MsgFlags::empty()){
            Ok(n) => nbytes_len_sent += n,
            Err(e) => {
                eprintln!("kv_shared::kvs_send_all send msg.len() error: {}", e);
                return Err(e);
            }
        }
    }

    let mut nbytes_msg_sent: usize = 0;
    while nbytes_msg_sent < msg.len() {
         match send(connection.fd.as_raw_fd(), &msg[nbytes_msg_sent..], MsgFlags::empty()){
            Ok(n) => nbytes_msg_sent += n,
            Err(e) => {
                eprintln!("kv_shared::kvs_send_all send msg[len] error: {}", e);
                return Err(e);
            }
        }
    }

    Ok(())
}


pub mod rbuf {
    use std::os::fd::{OwnedFd};

    /* todo: implement ring buffer */
    const RING_BUFFER_SIZE: usize = 4096 / std::mem::size_of::<Option<OwnedFd>>();
    pub struct FdRingBuffer {
        buf: Vec<Option<OwnedFd>>,
        stored: usize,
        head: usize,
        tail: usize,
        mask: usize,
    }

    impl FdRingBuffer {
        pub fn init()-> Self{
            let mut newbuf: Vec<Option<OwnedFd>> = vec![];
            for _ in 0..RING_BUFFER_SIZE {
                newbuf.push(None);
            }
            Self {
                buf: newbuf,
                stored: 0,
                head: 0,
                tail: 0,
                mask: 0xFFF,
            }
        }
    
        pub fn put(&mut self, fd: OwnedFd) -> Result<(), OwnedFd>{
            if self.stored == self.buf.len() {
                return Err(fd)
            }
            let index = self.head & self.mask;
            self.buf[index] = Some(fd);

            self.head += 1;
            self.stored += 1;
            Ok(())
        }   

        pub fn get(&mut self) -> Option<OwnedFd>{
            if self.stored == 0 {
                return None;
            }

            let index = self.tail & self.mask;
            let fd = self.buf[index].take()?;

            self.tail += 1;
            self.stored -= 1;

            Some(fd)
        }
    }


    
}