use nix::errno::Errno;
use nix::poll::PollTimeout;
use nix::sys::epoll::{Epoll, EpollCreateFlags, EpollEvent, EpollFlags};
use nix::unistd::{close, unlink};
use std::collections::HashMap;
use std::os::fd::{ OwnedFd};
use std::path::Path;

use kv_server::{self, PollInterests, handle_connection, open_socket};

fn main() -> Result<(), Errno> {
    println!("server: start");

    /* todo: init prethreading */


    let socket_path = Path::new("./kv.sock");
    let socket_fd = match open_socket(socket_path){
        Ok(result) => result,
        Err(e) => {
            eprintln!("server: open_socket {}", e);
            return Err(e);
        }
    };

    /* register PollInterests to fds */
    let mut fds: HashMap<u64, &OwnedFd> = HashMap::new();
    fds.insert(PollInterests::ListeningSocket as u64, &socket_fd);

    /* add interests to epoll */
    let epoll = Epoll::new(EpollCreateFlags::empty())?;
    match epoll.add(&socket_fd, EpollEvent::new(EpollFlags::EPOLLIN | EpollFlags::EPOLLET, PollInterests::ListeningSocket as u64)){
        Ok(_) => (),
        Err(e) => {
            eprintln!("server: epoll.add() {}", e);
            return Err(e);
        }
    };

    /* todo: accept commands from stdin */
    /* todo: handle signals gracefully with signalfd */

    /* start polling */
    let mut events = [EpollEvent::empty()];
    let mut poll_results_num: usize = 0;
    'polling: loop {
        println!("server: polling");
        poll_results_num = match epoll.wait(&mut events, PollTimeout::NONE){
            Ok(size) => size,
            Err(e) => {
                eprintln!("server: epoll.wait() {}", e);
                return Err(e);
            }
        };
        
        for event in events {
            if event.data() == PollInterests::ListeningSocket as u64 {
                /* todo: write connectionfd into a buffer */
                println!("server: got a {:?} event on Listening Socket", event.events());
                handle_connection(fds.get(&(PollInterests::ListeningSocket as u64)).expect("get")).unwrap();
            } else {
                println!("Got an unhandled {:?} with data {:?}", event.events(), event.data());
            }
        }

    }

    close(socket_fd).expect("close socket_fd failed");
    unlink(socket_path).expect("unlink failed");
    println!("server: stop");
    Ok(())
}
