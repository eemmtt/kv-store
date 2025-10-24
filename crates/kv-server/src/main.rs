use kv_shared::{recv_all, send_all, KVConnection};
use nix::errno::Errno;
use nix::sys::socket::{accept};
use nix::unistd::{close, unlink};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;

use kv_server::{self, open_socket};

fn main() -> Result<(), Errno> {
    println!("server: start");

    let socket_path = Path::new("./kv.sock");
    let socket_fd = match open_socket(socket_path){
        Ok(result) => result,
        Err(e) => {
            eprintln!("server: open_socket {}", e);
            return Err(e);
        }
    };
    
    println!("server: listening on socket");
    'accept_connections: loop {

        // add polling
        
        let connfd_raw: RawFd = accept(socket_fd.as_raw_fd()).expect("accept failed");
        let connfd = unsafe { OwnedFd::from_raw_fd(connfd_raw) };
        println!("server: connection accepted");

        let connection = KVConnection{
            fd: connfd,
            mtu: 1024,
        };

        'handle_connection: loop{
            let result = match recv_all(&connection){
                Ok(kvv) => kvv,
                Err(Errno::ECONNRESET) => {
                    println!("server: client disconnected");
                    break 'handle_connection;
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
                    println!("server: handled GET");
                },
                "SET" => {
                    let msg = String::from("good set!").into_bytes();
                    send_all(&connection, msg).unwrap();
                    println!("server: handled SET");
    
                },
                "DEL" => {
                    let msg = String::from("good del!").into_bytes();
                    send_all(&connection, msg).unwrap();
                    println!("server: handled DEL");
    
                },
                _ => {
                    println!("server: received unknown command {}", result_as_str);
                }
            }
        }

        //break 'accept_connections with stdin
    }

    close(socket_fd).expect("close socket_fd failed");
    unlink(socket_path).expect("unlink failed");
    println!("server: stop");
    Ok(())
}
