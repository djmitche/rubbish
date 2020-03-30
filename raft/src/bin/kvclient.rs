use failure::Fallible;
use raft::kv::Client;
use std::env;
use std::net::TcpStream;

fn main() -> Fallible<()> {
    let args: Vec<String> = env::args().collect();
    assert_eq!(args.len(), 2);
    let port = args[1].parse::<u16>()?;

    let sock = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    let mut client = Client::new(sock);

    println!("at start: {:?}", client.get("somekey")?);
    client.set("somekey", "a value")?;
    client.set("another", "value II")?;
    println!("at end: {:?}", client.get("somekey")?);
    client.delete("somekey")?;
    Ok(())
}
