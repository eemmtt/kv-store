use nix::{sys::socket::{ MsgFlags, recv, send }};
use std::{os::fd::{ AsRawFd, OwnedFd}, u32};
use nix::{errno::Errno, libc::size_t};

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


pub mod ringbuffer {
    use std::os::fd::{OwnedFd};
    use nix::libc::{pthread_mutex_t, sem_t};

    use crate::semaphores::{kv_mutex_init, kv_mutex_lock, kv_mutex_unlock, kv_sem_init, kv_sem_post, kv_sem_wait};

    /* todo: implement ring buffer */
    const RING_BUFFER_SIZE: usize = 4096 / std::mem::size_of::<Option<OwnedFd>>();
    pub struct FdRingBuffer {
        buf: Vec<Option<OwnedFd>>,
        head: usize,
        tail: usize,
        mask: usize,
        mtx: pthread_mutex_t,
        items: sem_t,
        spaces: sem_t,
    }

    impl FdRingBuffer {

        /// Init buffer
        pub fn init()-> Self{
            let mut newbuf: Vec<Option<OwnedFd>> = vec![];
            for _ in 0..RING_BUFFER_SIZE {
                newbuf.push(None);
            }
            
            let mtx = kv_mutex_init().unwrap();
            let items = kv_sem_init(0).unwrap();
            let spaces = kv_sem_init(RING_BUFFER_SIZE).unwrap();

            Self {
                buf: newbuf,
                head: 0,
                tail: 0,
                mask: 0xFFF,
                mtx: mtx,
                items: items,
                spaces: spaces,
            }
        }
    
        /// Put an fd in the buffer, blocking
        pub fn put(&mut self, fd: OwnedFd) -> Result<(), OwnedFd>{
            kv_sem_wait(&mut self.spaces).unwrap();
            kv_mutex_lock(&mut self.mtx).unwrap();

            let index = self.head & self.mask;
            self.buf[index] = Some(fd);
            self.head += 1;

            kv_mutex_unlock(&mut self.mtx).unwrap();
            kv_sem_post(&mut self.items).unwrap();
            
            Ok(())
        }   

        // Get an fd from the buffer, blocking
        pub fn get(&mut self) -> Option<OwnedFd>{
            kv_sem_wait(&mut self.items).unwrap();
            kv_mutex_lock(&mut self.mtx).unwrap();

            let index = self.tail & self.mask;
            let fd = self.buf[index].take()?;
            self.tail += 1;

            kv_mutex_unlock(&mut self.mtx).unwrap();
            kv_sem_post(&mut self.spaces).unwrap();

            Some(fd)
        }
    }


    
}

pub mod semaphores{
    use nix::{errno::Errno, libc::{pthread_mutex_init, pthread_mutex_lock, pthread_mutex_t, pthread_mutex_unlock, sem_init, sem_post, sem_t, sem_wait}};


    /// Wrapper for libc::phread_mutex_init()
    pub fn kv_mutex_init() -> Result<pthread_mutex_t, Errno> {
        let mut mtx: pthread_mutex_t = unsafe { std::mem::zeroed() };
        let res = unsafe {pthread_mutex_init(&mut mtx, std::ptr::null())};
        if res != 0 {
            return Err(Errno::from_raw(res));
        } 
        Ok(mtx)
    }
    
    /// Wrapper for libc::pthread_mutex_lock()
    pub fn kv_mutex_lock(mtx: &mut pthread_mutex_t) -> Result<(), Errno> {
        let res = unsafe {pthread_mutex_lock(mtx)};
        if res != 0 {
            return Err(Errno::from_raw(res));
        } 
        Ok(())
    }
    
    /// Wrapper for libc::pthread_mutex_unlock()
    pub fn kv_mutex_unlock(mtx: &mut pthread_mutex_t) -> Result<(), Errno> {
        let res = unsafe {pthread_mutex_unlock(mtx)};
        if res != 0 {
            return Err(Errno::from_raw(res));
        } 
        Ok(())
    }
    
    /// Wrapper for libc::sem_init()
    pub fn kv_sem_init(value: usize) -> Result<sem_t, Errno> {
        let mut sem: sem_t = unsafe { std::mem::zeroed() };
        let res = unsafe {
            sem_init(&mut sem, 0 /*0 => shared between threads */, value as u32)};
        if res == 0 {
            Ok(sem)
        } else {
            Err(Errno::from_raw(Errno::last_raw()))
        }
    }
    
    /// Wrapper for libc::sem_wait()
    pub fn kv_sem_wait(sem: &mut sem_t) -> Result<(), Errno>{
        let res = unsafe {sem_wait(sem)};
        if res == 0 {
            Ok(())
        } else {
            Err(Errno::from_raw(Errno::last_raw()))
        }
    }
    
    /// Wrapper for libc::sem_post()
    pub fn kv_sem_post(sem: &mut sem_t) -> Result<(), Errno>{
        let res = unsafe {sem_post(sem)};
        if res == 0 {
            Ok(())
        } else {
            Err(Errno::from_raw(Errno::last_raw()))
        }
    }
}