//! Simple length-framed messages over a TCP socket.
//!
//! Framing is a 32-bit unigned length in network byte order, followed by the message content.

use crate::util::writeall;
use byteorder::{ByteOrder, NetworkEndian};
use std::net::{TcpListener, TcpStream};

pub fn send_message(sock: &mut TcpStream, msg: &[u8]) -> std::io::Result<()> {
    let mut lenbuf = [0u8; 4];
    let len: u32 = msg.len() as u32; // NOTE: truncates!

    NetworkEndian::write_u32(&mut lenbuf, len);
    writeall(sock, &lenbuf)?;
    writeall(sock, msg)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::util::readall;
    use std::thread;

    fn threaded_test(server_fn: fn(&mut TcpStream) -> (), client_fn: fn(&mut TcpStream) -> ()) {
        // establish a server..
        let server = TcpListener::bind("127.0.0.1:0").unwrap();
        let serveraddr = server.local_addr().unwrap();

        let server_thread = thread::spawn(move || {
            let (mut sock, _) = server.accept().unwrap();
            server_fn(&mut sock);
        });

        let client_thread = thread::spawn(move || {
            let mut sock = TcpStream::connect(serveraddr).unwrap();
            client_fn(&mut sock);
        });

        server_thread.join().unwrap();
        client_thread.join().unwrap();
    }

    #[test]
    fn send_short_message() {
        threaded_test(
            |sock| {
                send_message(sock, b"Hello").unwrap();
            },
            |sock| {
                let mut lenbuf = [0; 4];
                readall(sock, &mut lenbuf).unwrap();
                assert_eq!(lenbuf, [0u8, 0, 0, 5]);

                let mut buf = [0; 5];
                readall(sock, &mut buf).unwrap();
                assert_eq!(&buf[..], b"Hello");
            },
        )
    }
}
