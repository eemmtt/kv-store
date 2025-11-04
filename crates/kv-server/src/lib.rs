use std::{os::{fd::{AsRawFd, FromRawFd, OwnedFd, RawFd}}, path::Path};
use nix::{errno::Errno, fcntl::{OFlag, open}, libc::pthread_mutex_t, sys::{socket::{AddressFamily, Backlog, SockFlag, SockType, UnixAddr, accept, bind, listen, socket}, stat::Mode}, unistd::{Whence, close, lseek, unlink}};
use kv_shared::{io::{KVKey, KVLog, KVLogItem, KVValue}, ringbuffer::FdRingBuffer, semaphores::{kv_mutex_init, kv_mutex_lock, kv_mutex_unlock}, syncdindex::SyncdIndex};

/// Open log and load persisted index
pub fn kv_log_load(log_path: &Path) -> Result<KVLog, Errno>{
    let fd = match open(
        log_path, 
        OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_APPEND, 
        Mode::S_IRWXU,
    ){
        Ok(fd) => fd,
        Err(e) => {
            return Err(e);
        }
    };

    /* todo: load persisted index */
    let index = SyncdIndex::new();

    let mtx = kv_mutex_init().unwrap();

    Ok(KVLog{
        fd,
        index,
        mtx,
    })
}

/// Close log and persist index to file
pub fn kv_log_shutdown(log: KVLog) -> Result<(), Errno>{
    let _ = close(log.fd).unwrap();
    /* todo: compact log */
    /* todo: persist index */

    Ok(())
}

/// Compact log file to most recent undeleted data
pub fn kv_log_compact(){}

/// Get value from log
pub fn kv_log_get(log: &mut KVLog, key: &KVKey) -> Result<Option<KVValue>, Errno>{
    let logitem = match log.index.si_get(*key){
        Some(li) => li,
        None => {
            return Ok(None);
        }
    };

    /* seek to data location and read data_size bytes */
    kv_mutex_lock(&mut log.mtx).unwrap();
    lseek(&log.fd, logitem.file_offset as i64, Whence::SeekSet).unwrap();
    let mut bytes_read = 0;
    let mut buf = [0u8; 4 * 4096]; /* todo: figure out real size... */
    while bytes_read < logitem.data_size {
        match nix::unistd::read(&log.fd, &mut buf) {
            Ok(0) => break,
            Ok(n) => bytes_read += n,
            Err(e) => {
                return Err(e);
            }
        }
    }
    kv_mutex_unlock(&mut log.mtx).unwrap();

    let val = KVValue::from_bytes(&buf).unwrap();
    Ok(Some(val))

}

/// Set key value pair in log
pub fn kv_log_set(log: &mut KVLog, key: &KVKey, val: &KVValue) -> Result<(), Errno>{
    let logitem = log.index.si_get(*key);
    if logitem.is_none(){
        /* add new KV pair */
        /* append to log */
        kv_mutex_lock(&mut log.mtx).unwrap();
        let val_as_bytes = val.to_bytes();
        let data_size = val_as_bytes.len();
        let new_offset = lseek(&log.fd, 0, Whence::SeekEnd).unwrap();
        let mut bytes_written = 0;
        while bytes_written < data_size {
            match nix::unistd::write(&log.fd, &val_as_bytes){
                Ok(0) => break,
                Ok(n) => bytes_written += n,
                Err(e) => {
                    return Err(e);
                }
            }
        };
        kv_mutex_unlock(&mut log.mtx).unwrap();
        
        /* add logitem to index */
        let new_logitem = KVLogItem{
            file_offset: new_offset as isize,
            data_size: data_size,
            time_added: val.time_set.expect("val.timeset is none?"),
        };
        log.index.si_insert(*key, new_logitem).unwrap();

        Ok(())
    } else {
        /* update KV pair */
        /* check that new value is fresh */
        if logitem.unwrap().time_added > val.time_set.unwrap() {
            return Err(Errno::ESTALE);
        }

        /* append to log */
        kv_mutex_lock(&mut log.mtx).unwrap();
        let val_as_bytes = val.to_bytes();
        let data_size = val_as_bytes.len();
        let new_offset = lseek(&log.fd, 0, Whence::SeekEnd).unwrap();
        let mut bytes_written = 0;
        while bytes_written < data_size {
            match nix::unistd::write(&log.fd, &val_as_bytes){
                Ok(0) => break,
                Ok(n) => bytes_written += n,
                Err(e) => {
                    return Err(e);
                }
            }
        };
        kv_mutex_unlock(&mut log.mtx).unwrap();

        /* update index */
        let new_logitem = KVLogItem{
            file_offset: new_offset as isize,
            data_size: data_size,
            time_added: val.time_set.expect("val.timeset is none?"),
        };
        log.index.si_insert(*key, new_logitem).unwrap();

        Ok(())
    }

}

/// Delete key value pair from log
pub fn kv_log_del(){}


/// Open unix tcp socket, bind and listen
pub fn open_socket(path: &Path) -> Result<OwnedFd, Errno>{
    let socket_addr = UnixAddr::new(path).expect("open_socket: UnixAddr failed");

    let _ = match unlink(path){
        Ok(_) => (),
        Err(Errno::ENOENT) => (), /* .sock already exists, continue */
        Err(e) => {
            eprintln!("open_socket: unlink socket_path: {}", e);
            return Err(e);
        }
    };

    let sockfd = socket(
        AddressFamily::Unix, 
        SockType::Stream,
        SockFlag::empty(),
        None,
    ).expect("open_socket: socket failed");

    bind(sockfd.as_raw_fd(), &socket_addr).expect("open_socket: bind failed");
    listen(&sockfd, Backlog::MAXCONN).expect("open_socket: listen failed");

    Ok(sockfd)
}

pub fn accept_connection(socket_fd: &OwnedFd, rbuf: &mut FdRingBuffer) -> Result<(), Errno>{
    /* todo: write connectionfd into a buffer */

    let connfd_raw: RawFd = accept(socket_fd.as_raw_fd()).expect("accept failed");
    let connfd = unsafe { OwnedFd::from_raw_fd(connfd_raw) };
    rbuf.put(connfd).expect("FdRingBuffer full or bad put");
    return Ok(());
}

pub mod polling {
    use std::os::fd::OwnedFd;

    use nix::{errno::Errno, sys::epoll::{Epoll, EpollEvent, EpollFlags}};

    pub enum PollInterests {
        ListeningSocket = 0,
        TerminalInput = 1,
        SIGINT = 2,
    }

    pub fn kv_epoll_add(epoll: &Epoll, fd: &OwnedFd, flags: EpollFlags, interest: PollInterests) -> Result<(), Errno>{
        epoll.add(fd, EpollEvent::new(flags, interest as u64))?;
        Ok(())
    }
}

pub mod threading {
    use std::ffi::c_void;
    use nix::libc::{pthread_t, pthread_create, pthread_self, pthread_detach};
    use nix::errno::Errno;    
    
    /// Wrapper for libc::pthread_create, takes no attributes
    pub fn kv_pthread_create(
        thread: *mut pthread_t,  
        thread_fn: extern "C" fn(*mut c_void) -> *mut c_void, 
        fn_arg: *mut c_void 
    ) -> Result<(), Errno>{
        
        let res = unsafe { 
            pthread_create(thread, std::ptr::null(), thread_fn, fn_arg) 
        };
        if res == 0 {
            Ok(())
        } else {
            let e = Errno::from_raw(res);
            eprintln!("pthread_create: {}", e);
            Err(e)
        }
    }
    
    /// Wrapper for libc::pthread_detach, detaches the calling thread
    pub fn kv_pthread_detach() -> Result<(), Errno>{
    
        let tid = unsafe { pthread_self() };
        let res = unsafe { pthread_detach(tid) };
        if res == 0 {
            Ok(())
        } else {
            let e = Errno::from_raw(res);
            eprintln!("pthread_detach: {}", e);
            Err(e)
        }
    }
}

pub mod worker{
    use std::{ffi::c_void, os::fd::OwnedFd};

    use kv_shared::{io::{KVConnection, KVKey, KVLog, KVMsg, KVMsgType, KVValue, KVValueType}, ringbuffer::FdRingBuffer};
    use nix::errno::Errno;
    
    use crate::{kv_log_get, kv_log_set, threading::kv_pthread_detach};
    
    /// Data passed as arg to worker_thread
    pub struct WorkerData<'a>{
        pub id: u64,
        pub rbuf: &'a mut FdRingBuffer,
        pub log: &'a mut KVLog,
    }
    
    /// start routine for worker threads
    pub extern "C" fn worker_thread(arg: *mut c_void) -> *mut c_void{
        kv_pthread_detach().unwrap();
        let data = unsafe { Box::from_raw(arg as *mut WorkerData)};
        println!("Hello from worker thread #{}!", data.id);

        /* lock with timeout, check for shutdown flag */
        loop {
            let fd = match data.rbuf.get(){
                Some(fd) => fd,
                None => {
                    continue;
                }
            };
            handle_connection(fd, data.log,data.id).expect("oops at handle_connection");
        }
    
        std::ptr::null_mut()
    }

    fn handle_connection(fd: OwnedFd, log: &mut KVLog, workerid: u64) -> Result<(), Errno>{
    
        let mut connection = KVConnection{
            fd: fd,
            mtu: 1024,
        };
    
        #[allow(unused)]
        'receive_commands: loop {
            let msg = match connection.recv_kvmsg(){
                Ok(msg) => msg,
                Err(Errno::ECONNRESET) => {
                    println!("worker #{}: client disconnected", workerid);
                    break;
                },
                Err(e) => {
                    eprintln!("handle_connection recv_all: error {}", e);
                    return Err(e);
                }
            };
        
            match msg.msgtype {
                KVMsgType::Get => {
                    println!("worker #{}: start GET", workerid);
                    let key = KVKey::from_bytes(&msg.msg).unwrap();
                    let val = match kv_log_get(log, &key){
                        Ok(None) => Vec::new(),
                        Ok(v) => v.unwrap().to_bytes(),
                        Err(e) => {
                            eprint!("kv_log_get err: {}", e);
                            return Err(e);
                        }
                    };
                    let msg = KVMsg::new(KVMsgType::GetReturn, val);
                    connection.send_kvmsg(msg).unwrap();
                    println!("worker #{}: handled GET", workerid);
                },
                KVMsgType::Set => {                     
                    println!("worker #{}: start SET", workerid);
                    let key = KVKey::from_bytes(&msg.msg[..264]).unwrap();
                    let mut val = KVValue::from_bytes(&msg.msg[264..]).unwrap();
                    /* time needs to be passed over into value. the value vs. msg abstraction is not very good */
                    val.time_set = Some(msg.sendtime); 
                    let result = kv_log_set(log, &key, &val).unwrap();

                    let body: Vec<u8> = String::from("good set!").into_bytes();
                    let msg = KVMsg::new(KVMsgType::SetReturn, body);
                    connection.send_kvmsg(msg).unwrap();
                    println!("worker #{}: handled SET", workerid);
                },
                KVMsgType::Delete => {
                    println!("worker #{}: start DEL", workerid);
                    let body: Vec<u8> = String::from("good del!").into_bytes();
                    let msg = KVMsg::new(KVMsgType::DeleteReturn, body);
                    connection.send_kvmsg(msg).unwrap();
                    println!("worker #{}: handled DEL", workerid);
                },
                _ => {
                    println!("worker #{}: received unknown msg type", workerid);
                }
            }
        }
    
        Ok(())
    }
}

pub mod signaling{
    use std::{ffi::c_void, os::fd::{RawFd}};
    use nix::libc::{ c_int, write};
    use nix::sys::signal::{Signal};

    pub static mut PIPE_WRITE_FD: Option<RawFd> = None;

    /* this could probably just be handle_sigint */
    pub extern "C" fn handle_signal(signal: c_int) {
        let signal = Signal::try_from(signal).unwrap();

        /* write one bit to self-pipe */
        if signal == Signal::SIGINT {
            unsafe {
                match PIPE_WRITE_FD {
                    Some(fd) => {
                        let mut nbytes: isize = 0;
                        while nbytes == 0 {
                            nbytes = write(fd, &1u8 as *const u8 as *const c_void, 1);
                        }                        
                    },
                    None => {}
                }
            }
        }
    }
}