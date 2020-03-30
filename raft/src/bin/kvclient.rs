use raft::transport::{recv_message, send_message};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;
use std::net::TcpStream;

#[derive(Serialize, Deserialize, Debug)]
struct Response {
    #[serde(default)]
    value: String,
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    assert_eq!(args.len(), 2);
    let port = args[1].parse::<u16>().unwrap(); // TODO use failure

    let mut sock = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

    let cmd = json!({"op": "set", "key": "keeey", "value": "VAL"});
    let msg = serde_json::to_vec(&cmd).unwrap();
    send_message(&mut sock, &msg[..]).unwrap();

    let v: Response = serde_json::from_slice(&recv_message(&mut sock).unwrap()).unwrap();
    println!("{:?}", v);

    let cmd = json!({"op": "set", "key": "kiiieeee", "value": "v"});
    let msg = serde_json::to_vec(&cmd).unwrap();
    send_message(&mut sock, &msg[..]).unwrap();

    let v: Response = serde_json::from_slice(&recv_message(&mut sock).unwrap()).unwrap();
    println!("{:?}", v);

    let cmd = json!({"op": "get", "key": "keeey"});
    let msg = serde_json::to_vec(&cmd).unwrap();
    send_message(&mut sock, &msg[..]).unwrap();

    let v: Response = serde_json::from_slice(&recv_message(&mut sock).unwrap()).unwrap();
    println!("{:?}", v);

    let cmd = json!({"op": "delete", "key": "kiiieeee"});
    let msg = serde_json::to_vec(&cmd).unwrap();
    send_message(&mut sock, &msg[..]).unwrap();

    let v: Response = serde_json::from_slice(&recv_message(&mut sock).unwrap()).unwrap();
    println!("{:?}", v);

    let cmd = json!({"op": "get", "key": "kiiieeee"});
    let msg = serde_json::to_vec(&cmd).unwrap();
    send_message(&mut sock, &msg[..]).unwrap();

    let v: Response = serde_json::from_slice(&recv_message(&mut sock).unwrap()).unwrap();
    println!("{:?}", v);

    Ok(())
}
