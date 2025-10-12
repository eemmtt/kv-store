use nix::sys::socket::{send, recv, socket, connect, AddressFamily, MsgFlags, SockFlag, SockType, UnixAddr};
use nix::unistd::{close};
use std::os::fd::{AsRawFd};
use std::str::from_utf8;

fn main() {
    println!("client: start");

    let sockfd = socket(
        AddressFamily::Unix, 
        SockType::Stream,
        SockFlag::empty(),
        None, 
    ).expect("socket failed");
    println!("client: unix socket created");


    let addr = UnixAddr::new("./kv.sock").expect("unix address fail");
    connect(sockfd.as_raw_fd(), &addr).expect("connect fail");

    let msg = "stop";
    let nbytes_sent = send(sockfd.as_raw_fd(), msg.as_bytes(), MsgFlags::empty()).expect("send failed");
    println!("client: msg sent");

    let mut buf: [u8;1024] = [0u8;1024];
    let nbytes_read = recv(sockfd.as_raw_fd(), &mut buf, MsgFlags::empty()).expect("recv failed");

    let msg_recvd = from_utf8(&buf[..nbytes_read]).expect("invalid utf-8");
    println!("client: recieved, '{}'", msg_recvd);

    close(sockfd).expect("close sockfd failed");
    println!("client: stop");


}
