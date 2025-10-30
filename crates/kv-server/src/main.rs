use kv_shared::ringbuffer::FdRingBuffer;
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::libc::{pthread_t};
use nix::poll::PollTimeout;
use nix::sys::epoll::{Epoll, EpollCreateFlags, EpollEvent, EpollFlags};
use nix::sys::signal::{signal, SigHandler, Signal};
use nix::unistd::{close, pipe2, unlink};
use std::collections::HashMap;
use std::os::fd::{ AsRawFd, OwnedFd, RawFd };
use std::os::raw::c_void;
use std::path::Path;

use kv_server::{self, accept_connection, open_socket};
use kv_server::threading::{kv_pthread_create};
use kv_server::worker::{WorkerData, worker_thread};
use kv_server::polling::{PollInterests, kv_epoll_add};
use kv_server::signaling::{PIPE_WRITE_FD, handle_signal};


fn main() -> Result<(), Errno> {
    println!("server: start");

    /* init work ring buffer */
    let mut rbuf = FdRingBuffer::init();
    
    /* init worker thread pool */
    const THREAD_POOL_SIZE: usize = 5;
    for i in 0..THREAD_POOL_SIZE {
        let mut thread = 0 as pthread_t;
        let data = Box::new(WorkerData {
            id: i as u64,
            rbuf: &mut rbuf,
        });
        let arg = Box::into_raw(data) as *mut c_void;
        kv_pthread_create(&mut thread, worker_thread, arg).unwrap();
    }

    /* init listening socket */
    let socket_path = Path::new("./kv.sock");
    let socket_fd = match open_socket(socket_path){
        Ok(result) => result,
        Err(e) => {
            eprintln!("server: open_socket {}", e);
            return Err(e);
        }
    };

    /* init self pipe */
    let (pipe_rd_fd, pipe_wr_fd) = pipe2(OFlag::O_NONBLOCK).unwrap();
    unsafe {
        PIPE_WRITE_FD = Some(pipe_wr_fd.as_raw_fd());
    }

    /* install signal handler */
    unsafe {signal(Signal::SIGINT, SigHandler::Handler(handle_signal))}.unwrap();

    /* register PollInterests to interestfds */
    let mut interestfds: HashMap<u64, &OwnedFd> = HashMap::new();
    interestfds.insert(PollInterests::ListeningSocket as u64, &socket_fd);
    interestfds.insert(PollInterests::SIGINT as u64, &pipe_rd_fd);

    /* add interests to epoll */
    let epoll = Epoll::new(EpollCreateFlags::empty())?;
    kv_epoll_add(&epoll, &socket_fd, EpollFlags::EPOLLIN | EpollFlags::EPOLLET, PollInterests::ListeningSocket).unwrap();
    kv_epoll_add(&epoll, &pipe_rd_fd, EpollFlags::EPOLLIN | EpollFlags::EPOLLET, PollInterests::SIGINT).unwrap();

    /* todo: accept commands from stdin */

    /* start polling */
    let mut events = [EpollEvent::empty()];
    let mut poll_results_num: usize = 0;
                
    'polling: loop {
        println!("server: polling");
        poll_results_num = match epoll.wait(&mut events, PollTimeout::NONE){
            Ok(size) => size,
            Err(Errno::EINTR) => continue 'polling, /* todo: prevent polling msg from printing again? */
            Err(e) => {
                eprintln!("server: epoll.wait() {}", e);
                return Err(e);
            }
        };
        
        for event in events {
            println!("server: got a {:?} event on Listening Socket", event.events());
            if event.data() == PollInterests::ListeningSocket as u64 {
                let listenfd = interestfds.get(&(PollInterests::ListeningSocket as u64)).unwrap();
                accept_connection(listenfd, &mut rbuf).unwrap();
                println!("server: put accepted connection to buffer");
            } else if event.data() == PollInterests::SIGINT as u64 {
                break 'polling;
            } else {
                println!("Got an unhandled {:?} with data {:?}", event.events(), event.data());
            }
        }

    }

    println!("server: cleaning up");
    close(socket_fd).expect("close socket_fd failed");
    unlink(socket_path).expect("unlink failed");
    println!("server: stop");
    Ok(())
}
