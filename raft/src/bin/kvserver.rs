use raft::transport::{recv_message, send_message};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    // Operation: get, set, or delete
    op: String,

    // key to get, set, or delete
    key: String,

    // value to set (ignored, and can be omitted, for get and delete)
    #[serde(default)]
    value: String,
}

type KV = Arc<Mutex<HashMap<String, String>>>;

fn server_fn(kv: KV, stream: &mut TcpStream) {
    loop {
        let msg = recv_message(stream).unwrap();
        let v: Message = serde_json::from_slice(&msg[..]).unwrap();
        println!("{:?}", v);

        match &v.op[..] {
            "set" => {
                kv.lock().unwrap().insert(v.key, v.value);
                send_message(stream, b"{}").unwrap();
            }
            "get" => {
                let locked = kv.lock().unwrap();
                if let Some(s) = locked.get(&v.key) {
                    let msg = serde_json::to_vec(&json!({ "value": s })).unwrap();
                    send_message(stream, &msg[..]).unwrap();
                } else {
                    let msg = serde_json::to_vec(&json!({})).unwrap();
                    send_message(stream, &msg[..]).unwrap();
                }
            }
            "delete" => {
                kv.lock().unwrap().remove(&v.key);
                send_message(stream, b"{}").unwrap();
            }
            _ => panic!("unknown op {:?}", v.op),
        }
    }
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    assert_eq!(args.len(), 2);
    let port = args[1].parse::<u16>().unwrap(); // TODO use failure

    let kv: KV = Arc::new(Mutex::new(HashMap::new()));

    let server = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    loop {
        let (mut sock, addr) = server.accept().unwrap();
        println!("connection from {:?}", addr);
        let kvcopy = kv.clone();
        thread::spawn(move || server_fn(kvcopy, &mut sock));
    }
}
