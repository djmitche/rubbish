//! Simple length-framed messages over a TCP socket.
//!
//! Framing is a 32-bit unigned length in network byte order, followed by the message content.

use crate::util::{readall, writeall};
use byteorder::{ByteOrder, NetworkEndian};
use std::convert::TryFrom;
use std::net::TcpStream;

/// Send the given message over the given socket.  Note that this must not be called concurrently,
/// as the message framining may in that case be mixed up.
pub fn send_message(sock: &mut TcpStream, msg: &[u8]) -> std::io::Result<()> {
    let mut lenbuf = [0u8; 4];
    let len: u32 = u32::try_from(msg.len()).unwrap();

    NetworkEndian::write_u32(&mut lenbuf, len);
    writeall(sock, &lenbuf)?;
    writeall(sock, msg)
}

/// Receive a message from the given socket.
pub fn recv_message(sock: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut lenbuf = [0u8; 4];
    readall(sock, &mut lenbuf)?;

    let len = NetworkEndian::read_u32(&lenbuf) as usize;

    let mut msg = vec![0u8; len];
    readall(sock, &mut msg)?;
    Ok(msg)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

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
    fn test_send_message() {
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

    #[test]
    fn test_recv_message() {
        threaded_test(
            |sock| {
                // 4 bytes of length, 4 bytes of message
                let buf = [0u8, 0, 0, 4, 100, 101, 102, 103];
                writeall(sock, &buf).unwrap();
            },
            |sock| {
                let msg = recv_message(sock).unwrap();
                assert_eq!(&msg[..], &[100, 101, 102, 103]);
            },
        )
    }

    #[test]
    fn test_bidirectional() {
        threaded_test(
            |sock| {
                send_message(sock, b"Hello").unwrap();
                let msg1 = recv_message(sock).unwrap();
                assert_eq!(&msg1[..], b"Welcome");
            },
            |sock| {
                let msg1 = recv_message(sock).unwrap();
                assert_eq!(&msg1[..], b"Hello");
                send_message(sock, b"Welcome").unwrap();
            },
        )
    }

    #[test]
    fn test_multiple() {
        threaded_test(
            |sock| {
                send_message(sock, b"One").unwrap();
                send_message(sock, b"Two").unwrap();
                send_message(sock, b"Three").unwrap();
            },
            |sock| {
                // queue the sends up, so that we're confident we de-frame these
                thread::sleep(Duration::from_millis(10));
                let msg1 = recv_message(sock).unwrap();
                assert_eq!(&msg1[..], b"One");
                let msg2 = recv_message(sock).unwrap();
                assert_eq!(&msg2[..], b"Two");
                let msg3 = recv_message(sock).unwrap();
                assert_eq!(&msg3[..], b"Three");
            },
        )
    }
}
