use kv_shared::{kvs_recv_all, kvs_send_all, KVConnection};
use nix::errno::Errno;
use nix::sys::socket::{accept, bind, listen, socket, AddressFamily, Backlog, SockFlag, SockType, UnixAddr};
use nix::unistd::{close, unlink};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;


fn main() -> Result<(), Errno> {
    println!("server: start");

    let socket_path = Path::new("./kv.sock");
    let _ = match unlink(socket_path){
        Ok(_) => (),
        Err(e) => {
            eprintln!("unlink socket_path: {}", e);
            return Err(e);
        }
    };

    let sockfd = socket(
        AddressFamily::Unix, 
        SockType::Stream,
        SockFlag::empty(),
        None,
    ).expect("socket failed");
    println!("server: unix socket created");

    let addr = UnixAddr::new("./kv.sock").expect("UnixAddr failed");
    bind(sockfd.as_raw_fd(), &addr).expect("bind failed");
    println!("server: socket binded");

    listen(&sockfd, Backlog::MAXCONN).expect("listen failed");
    
    'accept_connections: loop {
        println!("server: listening on socket");
        let connfd_raw: RawFd = accept(sockfd.as_raw_fd()).expect("accept failed");
        let connfd = unsafe { OwnedFd::from_raw_fd(connfd_raw) };
        println!("server: connection accepted");

        let connection = KVConnection{
            fd: connfd,
            mtu: 1024,
        };

        'connection: loop{
            let result = match kvs_recv_all(&connection){
                Ok(kvv) => kvv,
                Err(Errno::ECONNRESET) => {
                    println!("server: client disconnected");
                    break 'connection;
                },
                Err(e) => {
                    eprintln!("kv-server: kvs_recv_all: error {}", e);
                    return Err(e);
                }
            };

            let result_as_str = String::from_utf8(result.data).unwrap();
            let results_split: Vec<&str> = result_as_str.trim().split_whitespace().collect();
            match results_split.get(0) {
                Some(&"GET") => {
                    let msg = String::from("good get!").into_bytes();
                    kvs_send_all(&connection, msg).unwrap();
                    println!("server: handled GET");
                },
                Some(&"SET") => {
                    let msg = String::from("good set!").into_bytes();
                    kvs_send_all(&connection, msg).unwrap();
                    println!("server: handled SET");
    
                },
                Some(&"DEL") => {
                    let msg = String::from("good del!").into_bytes();
                    kvs_send_all(&connection, msg).unwrap();
                    println!("server: handled DEL");
    
                },
                _ => {
                    println!("server: received unknown command {}", result_as_str);
                }
            }
        }

        //break 'accept_connections somehow...
    }

    close(sockfd).expect("close sockfd failed");
    unlink(socket_path).expect("unlink failed");
    println!("server: stop");
    Ok(())
}
