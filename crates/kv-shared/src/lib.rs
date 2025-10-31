
pub mod io {
    use std::{os::fd::{AsRawFd, OwnedFd},time::{Duration, SystemTime, UNIX_EPOCH}};

    use nix::{errno::Errno, libc::size_t, sys::socket::{MsgFlags, recv, send}};


    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    pub struct KVKey {
        data: [u8; 256],
        len: usize,
    }

    impl KVKey {
        pub const MAX_LEN: usize = 256;

        pub fn new(s: &str) -> Result<Self, ()> {
            if s.len() > Self::MAX_LEN {
                return Err(());
            }

            let mut data = [0u8; Self::MAX_LEN];
            data[..s.len()].copy_from_slice(s.as_bytes());
            Ok(Self { 
                data: data, 
                len: s.len() 
            })
        }

        pub fn as_str(&self) -> &str {
            std::str::from_utf8(&self.data[..self.len]).unwrap()
        }

        pub fn to_bytes(&self) -> Vec<u8> {
            let mut bytes: Vec<u8> = Vec::new();
            bytes.extend(&self.data);
            bytes.extend(&(self.len as u64).to_le_bytes());
            bytes
        }

        pub fn from_bytes(bytes: &[u8]) -> Result<Self, ()> {
            if bytes.len() < Self::MAX_LEN + 8 {
                return Err(());
            }
            let data: [u8;Self::MAX_LEN] = bytes[..Self::MAX_LEN].try_into().unwrap();
            let len = u64::from_le_bytes(bytes[Self::MAX_LEN..Self::MAX_LEN+8].try_into().unwrap()) as usize;
            Ok(Self { data: data, len: len })
        }
    }

    #[derive(Copy, Clone)]
    #[repr(u32)]
    pub enum KVMsgType {
        Get = 0,
        Set = 1,
        Delete = 2,
        GetReturn = 3,
        SetReturn = 4,
        DeleteReturn = 5,
    }

    impl KVMsgType{
        /* convert u32 to KVMsgType */
        fn from_u32(val: u32) -> Result<Self, ()>{
            match val {
                0 => Ok(KVMsgType::Get),
                1 => Ok(KVMsgType::Set),
                2 => Ok(KVMsgType::Delete),
                3 => Ok(KVMsgType::GetReturn),
                4 => Ok(KVMsgType::SetReturn),
                5 => Ok(KVMsgType::DeleteReturn),
                _ => Err(()),
            }
        }
    }
    
    pub struct KVMsg{
        pub msgtype: KVMsgType,
        pub sendtime: Duration,
        pub msg: Vec<u8>,
    }
    
    impl KVMsg{
        pub fn new(msgtype: KVMsgType, msg: Vec<u8>) -> Self{
            let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap();
            Self {
                msgtype: msgtype, 
                sendtime: t,
                msg: msg,
            }
        }
    
        pub fn to_bytes(&self) -> Vec<u8> {
            let mut bytes: Vec<u8> = Vec::new();
            bytes.extend(&(self.msgtype as u32).to_le_bytes());         // 4 bytes
            bytes.extend(&self.sendtime.as_secs().to_le_bytes());       // 8 bytes
            bytes.extend(&self.sendtime.subsec_nanos().to_le_bytes());  // 4 bytes
            bytes.extend(&(self.msg.len() as u64).to_le_bytes());       // 8 bytes
            bytes.extend(&self.msg);                                    // 0 - ? bytes 
            bytes
        }
        
        pub fn from_bytes(bytes: &[u8]) -> Result<Self, ()> {
            /* missing bytes, less than minimum */
            if bytes.len() < 24 {
                return Err(());
            }
            
            let msgtype = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
            let secs = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
            let nanos = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
            let msglen = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
            
            /* missing bytes from msg field */
            if bytes.len() < 24 + msglen {
                return Err(());
            }
            let msg: Vec<u8> = bytes[24..24+msglen].to_vec();
            
            Ok(KVMsg { 
                msgtype: KVMsgType::from_u32(msgtype).unwrap(), 
                sendtime: Duration::new(secs, nanos), 
                msg: msg,
            })
            
        }
    } 

    pub struct KVConnection{
        pub fd: OwnedFd,
        pub mtu: size_t,
    }
    
    impl KVConnection{
        
        /// Ensures full send of KVMsg over KVConnection
        pub fn send_kvmsg(&mut self, msg: KVMsg) -> Result<(), Errno>{
            
            let msg_bytes = msg.to_bytes();
            
            /* send msg length over */
            let msg_len = msg_bytes.len();
            let len_buf = msg_len.to_be_bytes() as [u8; 8];
            let mut nbytes_len_sent: usize = 0;
            while nbytes_len_sent < 8 {
                match send(self.fd.as_raw_fd(), &len_buf[nbytes_len_sent..], MsgFlags::empty()){
                    Ok(n) => nbytes_len_sent += n,
                    Err(e) => {
                        eprintln!("io::send_kvmsg send msg_len error: {}", e);
                        return Err(e);
                    }
                }
            }
        
            /* send msg */
            let mut nbytes_msg_sent: usize = 0;
            while nbytes_msg_sent < msg_len {
                 match send(self.fd.as_raw_fd(), &msg_bytes[nbytes_msg_sent..], MsgFlags::empty()){
                    Ok(n) => nbytes_msg_sent += n,
                    Err(e) => {
                        eprintln!("io::send_kvmsg send msg_bytes error: {}", e);
                        return Err(e);
                    }
                }
            }
        
            Ok(())
        }

        /// Ensures full recv of KVMsg over KVConnection
        pub fn recv_kvmsg(&mut self) -> Result<KVMsg, Errno>{
        
            /* receive msg length */ 
            let mut len_buf= [0u8;8]; /* expecting usize */
            let mut nbytes_len_recvd: usize = 0;
            while nbytes_len_recvd < len_buf.len() {
                match recv(self.fd.as_raw_fd(), &mut len_buf[nbytes_len_recvd..], MsgFlags::empty()){
                    Ok(0) => return Err(Errno::ECONNRESET),
                    Ok(n) => nbytes_len_recvd += n,
                    Err(e) => {
                        eprintln!("io::recv_kvmsg recv msg_len error: {}", e);
                        return Err(e);
                    }
                }
            }
            let msg_len = u64::from_be_bytes(len_buf) as usize;
            
            /* receive msg */
            let mut buf = vec![0u8; msg_len];
            let mut nbytes_recvd: usize = 0;
            while nbytes_recvd < msg_len {
                match recv(self.fd.as_raw_fd(), &mut buf[nbytes_recvd..], MsgFlags::empty()){
                    Ok(0) => return Err(Errno::ECONNRESET),
                    Ok(n) => nbytes_recvd += n,
                    Err(e) => {
                        eprintln!("io::recv_all recv msg[len] error: {}", e);
                        return Err(e);
                    }
                }
            }
        
            let result = KVMsg::from_bytes(&buf).unwrap();
            Ok(result)
        }
        
    }
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