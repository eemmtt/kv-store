use std::{os::{fd::{AsRawFd, FromRawFd, OwnedFd, RawFd}}, path::Path};
use nix::{errno::Errno, sys::socket::{AddressFamily, Backlog, SockFlag, SockType, UnixAddr, accept, bind, listen, socket}, unistd::unlink};
use kv_shared::{ringbuffer::FdRingBuffer};

/// Get value from log
pub fn log_get(){}

/// Set key value pair in log
pub fn log_set(){}

/// Delete key value pair from log
pub fn log_del(){}

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

    use kv_shared::{io::{KVConnection, KVMsg, KVMsgType}, ringbuffer::FdRingBuffer};
    use nix::errno::Errno;
    
    use crate::threading::kv_pthread_detach;
    
    /// Data passed as arg to worker_thread
    pub struct WorkerData<'a>{
        pub id: u64,
        pub rbuf: &'a mut FdRingBuffer,
    }
    
    /// start routine for worker threads
    pub extern "C" fn worker_thread(arg: *mut c_void) -> *mut c_void{
        kv_pthread_detach().unwrap();
        let data = unsafe { Box::from_raw(arg as *mut WorkerData)};
        println!("Hello from worker thread #{}!", data.id);

        loop {
            let fd = match data.rbuf.get(){
                Some(fd) => fd,
                None => {
                    continue;
                }
            };
            handle_connection(fd,data.id).expect("oops at handle_connection");
        }
    
        std::ptr::null_mut()
    }

    fn handle_connection(fd: OwnedFd, workerid: u64) -> Result<(), Errno>{
    
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
                    let body: Vec<u8> = String::from("good get!").into_bytes();
                    let msg = KVMsg::new(KVMsgType::GetReturn, body);
                    connection.send_kvmsg(msg).unwrap();
                    println!("worker #{}: handled GET", workerid);
                },
                KVMsgType::Set => {
                    let body: Vec<u8> = String::from("good set!").into_bytes();
                    let msg = KVMsg::new(KVMsgType::SetReturn, body);
                    connection.send_kvmsg(msg).unwrap();
                    println!("worker #{}: handled SET", workerid);
                },
                KVMsgType::Delete => {
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