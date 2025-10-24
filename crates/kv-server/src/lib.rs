use std::{os::fd::{AsRawFd, OwnedFd}, path::Path};
use nix::{errno::Errno, sys::socket::{bind, listen, socket, AddressFamily, Backlog, SockFlag, SockType, UnixAddr}, unistd::unlink};

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