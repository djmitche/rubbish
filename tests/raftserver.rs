use failure::Fallible;
use rubbish::prax::raft::diststate::{self, DistributedState};
use rubbish::net::tcp::{TcpConfig, TcpNode};
use rubbish::net::NodeId;
use rubbish::prax::raft::server::RaftServer;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::delay_for;

#[derive(Clone, PartialEq, Debug)]
pub struct TestState(HashMap<String, String>);

/// Clients send Request objects (JSON-encoded) to the server.
#[derive(PartialEq, Debug, Clone, Default)]
pub struct Request {
    /// Operation: one of get, set, or delete
    pub op: String,

    /// key to get, set, or delete
    pub key: String,

    /// value to set (ignored, and can be omitted, for get and delete)
    pub value: String,
}

impl diststate::Request for Request {
    fn serialize(&self) -> serde_json::Value {
        json!({
            "op": self.op,
            "key": self.key,
            "value": self.value,
        })
    }
    fn deserialize(ser: &serde_json::Value) -> Self {
        Request {
            op: ser.get("op").unwrap().as_str().unwrap().to_owned(),
            key: ser.get("key").unwrap().as_str().unwrap().to_owned(),
            value: ser.get("value").unwrap().as_str().unwrap().to_owned(),
        }
    }
}

/// Response to a Request
#[derive(PartialEq, Debug, Clone, Default)]
pub struct Response {
    pub value: Option<String>,
}

impl diststate::Response for Response {}

impl DistributedState for TestState {
    type Request = Request;
    type Response = Response;

    fn new() -> Self {
        Self(HashMap::new())
    }

    fn dispatch(&mut self, request: &Request) -> Response {
        match &request.op[..] {
            "set" => {
                self.0.insert(request.key.clone(), request.value.clone());
                Response {
                    value: Some(request.value.clone()),
                }
            }
            "get" => Response {
                value: self.0.get(&request.key).cloned(),
            },
            "delete" => {
                self.0.remove(&request.key);
                Response { value: None }
            }
            _ => panic!("unknown op {:?}", request.op),
        }
    }
}

#[tokio::test]
async fn test() -> Fallible<()> {
    // TODO: load this from a config file
    let config: TcpConfig = vec![
        "127.0.0.1:25000".parse().unwrap(),
        "127.0.0.1:25001".parse().unwrap(),
        "127.0.0.1:25002".parse().unwrap(),
    ];

    let servers = config.iter().enumerate().map(|(node_id, _)| {
        let node = TcpNode::new(node_id as NodeId, config.clone());
        let server: RaftServer<TestState> = RaftServer::new(node);
        server
    });

    // TODO: when client interface is available, write, read, etc.
    delay_for(Duration::from_secs(1)).await;

    for server in servers {
        server.stop().await;
    }
    Ok(())
}
