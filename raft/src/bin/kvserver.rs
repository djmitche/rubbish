use failure::Fallible;
use raft::kv::{Local, Server};
use std::env;
use std::net::TcpListener;

fn main() -> Fallible<()> {
    let args: Vec<String> = env::args().collect();
    assert_eq!(args.len(), 2);
    let port = args[1].parse::<u16>()?;

    let sock = TcpListener::bind(format!("127.0.0.1:{}", port))?;
    Server::new(Local::new()).serve(sock);
}
