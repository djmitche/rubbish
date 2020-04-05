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
    #[serde(default)]
    value: Option<String>,
}

type KV = Arc<Mutex<HashMap<String, String>>>;

/// A KV backend is responsible for storing keys and values.  It is cloned into
/// multiple threads.
pub trait Backend: Sync + Send + Clone {
    fn get(&self, key: &str) -> Option<String>;
    fn set(&self, key: String, value: String);
    fn delete(&self, key: &str);
}

/// Local is a KV Backend that just stores data in a hashmap.
#[derive(Debug, Clone)]
pub struct Local(KV);

impl Local {
    /// Create a new local backend.
    pub fn new() -> Local {
        Local(Arc::new(Mutex::new(HashMap::new())))
    }
}

impl Backend for Local {
    fn get(&self, key: &str) -> Option<String> {
        match self.0.lock().unwrap().get(key) {
            Some(s) => Some(s.clone()),
            None => None,
        }
    }
    fn set(&self, key: String, value: String) {
        self.0.lock().unwrap().insert(key, value);
    }
    fn delete(&self, key: &str) {
        self.0.lock().unwrap().remove(key);
    }
}

/// A KV server listens for TCP connections and services them.
pub struct Server<B: Backend> {
    backend: B,
}

impl<B: Backend + 'static> Server<B> {
    /// Create a new server
    pub fn new(backend: B) -> Server<B> {
        Server { backend }
    }

    /// Serve forever on this listening TCP socket (the result of TcpListener::bind)
    pub fn serve(self, bound_sock: TcpListener) -> ! {
        loop {
            let (mut sock, _) = bound_sock.accept().unwrap();
            let backend = self.backend.clone();
            thread::spawn(move || Server::serve_n(backend, &mut sock, 0));
        }
    }

    // serve a socket, handling N requests (or infinite if 0)
    fn serve_n(backend: B, sock: &mut TcpStream, mut n: u16) {
        loop {
            let msg = recv_message(sock).unwrap();
            let v: Request = serde_json::from_slice(&msg[..]).unwrap();

            match &v.op[..] {
                "set" => {
                    backend.set(v.key, v.value);
                    send_message(sock, b"{}").unwrap();
                }
                "get" => {
                    if let Some(s) = backend.get(&v.key) {
                        let msg = serde_json::to_vec(&json!({ "value": s })).unwrap();
                        send_message(sock, &msg[..]).unwrap();
                    } else {
                        let msg = serde_json::to_vec(&json!({})).unwrap();
                        send_message(sock, &msg[..]).unwrap();
                    }
                }
                "delete" => {
                    backend.delete(&v.key);
                    send_message(sock, b"{}").unwrap();
                }
                _ => panic!("unknown op {:?}", v.op),
            }

            if n != 0 {
                n = n - 1;
                if n == 0 {
                    break;
                }
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::util::test::threaded_test;

    #[test]
    fn test_with_local() {
        threaded_test(
            |mut sock| {
                let backend = Local::new();
                Server::serve_n(backend.clone(), &mut sock, 5);
            },
            |sock| {
                let mut client = Client::new(sock);
                assert_eq!(client.get("kk").unwrap(), None);
                client.set("kk", "vv").unwrap();
                assert_eq!(client.get("kk").unwrap(), Some(String::from("vv")));
                client.delete("kk").unwrap();
                assert_eq!(client.get("kk").unwrap(), None);
            },
        );
    }
}
