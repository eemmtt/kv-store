use nix::sys::socket::{accept, bind, listen, send, recv, socket, AddressFamily, Backlog, MsgFlags, SockFlag, SockType, UnixAddr};
use nix::unistd::{close, unlink};
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;
use std::str::from_utf8;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("server: start");

    let socket_path = Path::new("./kv.sock");
    let _ = match unlink(socket_path){
        Ok(_) => (),
        Err(e) => {
            eprintln!("unlink socket_path: {}", e);
            ()
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
    
    loop {
        println!("server: listening on socket");
        let connfd: RawFd = accept(sockfd.as_raw_fd()).expect("accept failed");
        println!("server: connection accepted");

        let msg =  "Hello Darling";
        send(connfd, msg.as_bytes(), MsgFlags::empty()).expect("send failed");
        println!("server: msg sent");


        let mut buf = [0u8; 1024];
        match recv(connfd, &mut buf, MsgFlags::empty()){
            Ok(0) => {
                println!("server: client disconnected");
                close(connfd).expect("close connfd failed");
                continue;
            },
            Ok(n) => {
                let msg_recvd = from_utf8(&buf[..n]).expect("invalid utf-e");
                println!("server: received, '{}'", msg_recvd);
                if msg_recvd == "stop" {
                    break;
                }
                continue;
            }
            Err(e) => {
                eprint!("recv error: {}", e);
                close(connfd).expect("close connfd failed");
                break;
            }
        }
    }

    close(sockfd).expect("close sockfd failed");
    unlink(socket_path).expect("unlink failed");
    println!("server: stop");
    Ok(())
}
