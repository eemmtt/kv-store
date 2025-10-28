use std::{os::fd::{RawFd, AsRawFd, OwnedFd, FromRawFd}, path::Path};
use nix::{errno::Errno, sys::socket::{accept,bind, listen, socket, AddressFamily, Backlog, SockFlag, SockType, UnixAddr}, unistd::unlink};
use kv_shared::{send_all, recv_all, KVConnection, KVValue};

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

pub enum PollInterests {
    ListeningSocket = 0,
    TerminalInput = 1,
    Signal = 2,
}

pub fn handle_connection(socket_fd: &OwnedFd) -> Result<(), Errno>{
    let connfd_raw: RawFd = accept(socket_fd.as_raw_fd()).expect("accept failed");
    let connfd = unsafe { OwnedFd::from_raw_fd(connfd_raw) };
    println!("server: connection accepted");

    let connection = KVConnection{
        fd: connfd,
        mtu: 1024,
    };

    loop {

        let result = match recv_all(&connection){
            Ok(kvv) => kvv,
            Err(Errno::ECONNRESET) => {
                println!("handle_connection: client disconnected");
                break;
            },
            Err(e) => {
                eprintln!("kv-server: kv-shared::recv_all: error {}", e);
                return Err(e);
            }
        };
    
        let result_as_str = String::from_utf8(result.data).unwrap();
        let mut parts = result_as_str.trim().splitn(3, ' ');
    
        let command = parts.next().unwrap_or("");
        let key = parts.next().unwrap_or("");
        let value = parts.next().unwrap_or("");
    
        match command {
            "GET" => {
                let msg = String::from("good get!").into_bytes();
                send_all(&connection, msg).unwrap();
                println!("handle_connection: handled GET");
            },
            "SET" => {
                let msg = String::from("good set!").into_bytes();
                send_all(&connection, msg).unwrap();
                println!("handle_connection: handled SET");
    
            },
            "DEL" => {
                let msg = String::from("good del!").into_bytes();
                send_all(&connection, msg).unwrap();
                println!("handle_connection: handled DEL");
    
            },
            _ => {
                println!("handle_connection: received unknown command {}", result_as_str);
            }
        }
    }

    Ok(())
}
