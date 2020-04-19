use super::*;

use crate::prax::raft::diststate::{self, DistributedState};
use crate::prax::raft::server::state::Mode;
use crate::net::local::LocalNetwork;
use std::collections::HashMap;
use tokio::time::delay_for;
use serde_json::{self, json};
use failure::Fallible;
use crate::net::NodeId;
use std::time::Duration;

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
async fn elect_a_leader() -> Fallible<()> {
    let mut net = LocalNetwork::new(4);
    let mut servers: Vec<RaftServer<TestState>> = vec![
        RaftServer::new(net.take(0)),
        RaftServer::new(net.take(1)),
        RaftServer::new(net.take(2)),
        RaftServer::new(net.take(3)),
    ];

    async fn get_leader(servers: &mut Vec<RaftServer<TestState>>) -> Fallible<NodeId> {
        loop {
            for server in servers.iter_mut() {
                let state = server.get_state().await?;
                if state.mode == Mode::Leader {
                    return Ok(state.node_id);
                }
            }

            delay_for(Duration::from_millis(100)).await;
        }
    }

    async fn set(
        server: &mut RaftServer<TestState>,
        key: &str,
        value: &str,
    ) -> Fallible<Option<String>> {
        let res = server
            .request(Request {
                op: "set".into(),
                key: key.into(),
                value: value.into(),
            })
            .await?;
        Ok(res.value)
    }

    async fn get(server: &mut RaftServer<TestState>, key: &str) -> Fallible<Option<String>> {
        let res = server
            .request(Request {
                op: "get".into(),
                key: key.into(),
                value: "".into(),
            })
            .await?;
        Ok(res.value)
    }

    async fn delete(server: &mut RaftServer<TestState>, key: &str) -> Fallible<Option<String>> {
        let res = server
            .request(Request {
                op: "delete".into(),
                key: key.into(),
                value: "".into(),
            })
            .await?;
        Ok(res.value)
    }

    let leader = get_leader(&mut servers).await?;

    // get when nothing exists, set, then get again
    assert_eq!(get(&mut servers[leader], "k").await?, None);
    assert_eq!(
        set(&mut servers[leader], "k", "foo").await?,
        Some("foo".into())
    );
    assert_eq!(get(&mut servers[leader], "k").await?, Some("foo".into()));

    // make some more changes>.
    assert_eq!(delete(&mut servers[leader], "k").await?, None);
    assert_eq!(get(&mut servers[leader], "k").await?, None);

    // shut it all down
    for server in servers.drain(..) {
        server.stop().await;
    }

    Ok(())
}
