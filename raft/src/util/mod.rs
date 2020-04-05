use failure::Fallible;
use std::io::{Read, Write};

pub(crate) fn writeall<W: Write>(stream: &mut W, buf: &[u8]) -> Fallible<()> {
    let mut pos = 0;
    while pos < buf.len() {
        pos += stream.write(&buf[pos..])?;
    }
    Ok(())
}

pub(crate) fn readall<R: Read>(stream: &mut R, buf: &mut [u8]) -> Fallible<()> {
    let mut pos = 0;
    while pos < buf.len() {
        pos += stream.read(&mut buf[pos..])?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    // utilities for tests..

    pub(crate) fn threaded_test(server_fn: fn(TcpStream) -> (), client_fn: fn(TcpStream) -> ()) {
        // establish a server..
        let server = TcpListener::bind("127.0.0.1:0").unwrap();
        let serveraddr = server.local_addr().unwrap();

        let server_thread = thread::spawn(move || {
            let (sock, _) = server.accept().unwrap();
            server_fn(sock);
        });

        let client_thread = thread::spawn(move || {
            let sock = TcpStream::connect(serveraddr).unwrap();
            client_fn(sock);
        });

        server_thread.join().unwrap();
        client_thread.join().unwrap();
    }
}
