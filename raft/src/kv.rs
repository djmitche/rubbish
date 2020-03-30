//! Client and server implementations of a key-value storage system.

use crate::transport::{recv_message, send_message};
use failure::Fallible;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

/// Clients send Request objects (JSON-encoded) to the server.
#[derive(Serialize, Deserialize, Debug)]
struct Request {
    /// Operation: one of get, set, or delete
    op: String,

    /// key to get, set, or delete
    key: String,

    /// value to set (ignored, and can be omitted, for get and delete)
    #[serde(default)]
    value: String,
}

/// Response to a Request
#[derive(Serialize, Deserialize, Debug)]
struct Response {
    // TODO: a "present" property?
    #[serde(default)]
    value: Option<String>,
}

type KV = Arc<Mutex<HashMap<String, String>>>;

/// A KV server listens for TCP connections and services them.
pub struct Server {
    kv: KV,
}

impl Server {
    /// Create a new server
    pub fn new() -> Server {
        Server {
            kv: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Serve forever on this listening TCP socket (the result of TcpListener::bind)
    pub fn serve(self: Server, bound_sock: TcpListener) -> ! {
        loop {
            let (mut sock, addr) = bound_sock.accept().unwrap();
            println!("connection from {:?}", addr);
            let kv = self.kv.clone();
            thread::spawn(move || Server::serve_client(kv, &mut sock));
        }
    }

    fn serve_client(kv: KV, sock: &mut TcpStream) -> Fallible<()> {
        loop {
            let msg = recv_message(sock).unwrap();
            let v: Request = serde_json::from_slice(&msg[..]).unwrap();
            println!("{:?}", v);

            match &v.op[..] {
                "set" => {
                    kv.lock().unwrap().insert(v.key, v.value);
                    println!("insert {:?}", kv.lock().unwrap());
                    send_message(sock, b"{}").unwrap();
                }
                "get" => {
                    let locked = kv.lock().unwrap();
                    if let Some(s) = locked.get(&v.key) {
                        let msg = serde_json::to_vec(&json!({ "value": s })).unwrap();
                        send_message(sock, &msg[..]).unwrap();
                    } else {
                        let msg = serde_json::to_vec(&json!({})).unwrap();
                        send_message(sock, &msg[..]).unwrap();
                    }
                }
                "delete" => {
                    kv.lock().unwrap().remove(&v.key);
                    send_message(sock, b"{}").unwrap();
                }
                _ => panic!("unknown op {:?}", v.op),
            }
        }
    }
}

/// A KV client makes requests over a TCP socket, and waits for respones.
pub struct Client {
    sock: TcpStream,
}

impl Client {
    /// Build a new client, given a TCPStream already connected to the server.
    pub fn new(sock: TcpStream) -> Client {
        Client { sock }
    }

    /// Set the value for the given key.
    pub fn set(&mut self, key: &str, value: &str) -> Fallible<()> {
        let cmd = json!({
            "op": "set",
            "key": key,
            "value": value,
        });
        let msg = serde_json::to_vec(&cmd)?;
        send_message(&mut self.sock, &msg[..])?;

        // read the response to check for errros, but it has no content..
        serde_json::from_slice::<Response>(&recv_message(&mut self.sock)?)?;
        Ok(())
    }

    /// Delete the given key.
    pub fn delete(&mut self, key: &str) -> Fallible<()> {
        let cmd = json!({
            "op": "delete",
            "key": key,
        });
        let msg = serde_json::to_vec(&cmd)?;
        send_message(&mut self.sock, &msg[..])?;

        // read the response to check for errros, but it has no content..
        serde_json::from_slice::<Response>(&recv_message(&mut self.sock)?)?;
        Ok(())
    }

    /// Get the value for the given key.
    pub fn get(&mut self, key: &str) -> Fallible<Option<String>> {
        let cmd = json!({
            "op": "get",
            "key": key,
        });
        let msg = serde_json::to_vec(&cmd)?;
        send_message(&mut self.sock, &msg[..])?;

        let v: Response = serde_json::from_slice(&recv_message(&mut self.sock)?)?;
        if let Some(value) = v.value {
            return Ok(Some(value));
        }
        Ok(None)
    }
}
