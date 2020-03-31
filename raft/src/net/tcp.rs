use crate::net::{Message, NodeId, RaftNetworkNode};
use async_trait::async_trait;
use failure::Fallible;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio::task;

// TODO: document the network of channels

/// A TcpConfig is a vector of nodes in the network, giving the address at which that node is
/// listening.
type TcpConfig = Vec<SocketAddr>;

/// A node on a TCP network, implementing RaftNetworkNode.
pub struct TcpNode {
    pub node_id: NodeId,

    /// Queues for all incoming and outgoing messages, used by the send and recv methods.
    outgoing_tx: mpsc::Sender<(NodeId, Message)>,
    incoming_rx: mpsc::Receiver<(NodeId, Message)>,

    /// The JoinHandle for the "worker" task, and a channel to indicate that it should stop
    task: Option<task::JoinHandle<()>>,
    stop_tx: mpsc::Sender<()>,
}

/// A TcpWorker does all of the "backend" work for a TCP node:
///
/// * Establishing connections to other nodes
/// * Routing outgoing messages to connections
/// * Collecting incoming messages
struct TcpWorker {
    /// channel containing messages to go out, multiplexed to the peers
    outgoing_rx: mpsc::Receiver<(NodeId, Message)>,

    /// A channel on which a message means "stop"
    stop_rx: mpsc::Receiver<()>,

    /// Outgoing message channels for each peer
    peer_outgoing_tx: Vec<mpsc::Sender<Message>>,

    /// Stop signals for each peer
    peer_stop_tx: Vec<mpsc::Sender<()>>,

    /// Stop signals for each peer
    peer_task: Vec<task::JoinHandle<()>>,
}

/// A connection to a network peer
struct TcpPeer {
    /// This node's ID
    node_id: NodeId,

    /// Peer's node ID
    peer_node_id: NodeId,

    /// Peer's address
    address: SocketAddr,

    /// channel for messages destined to this peer
    outgoing_rx: mpsc::Receiver<Message>,

    /// A channel on which a message means "stop"
    stop_rx: mpsc::Receiver<()>,

    /// channel over which any incoming messages are delivered
    incoming_tx: mpsc::Sender<(NodeId, Message)>,
}

impl TcpNode {
    pub fn new(node_id: NodeId, config: TcpConfig) -> TcpNode {
        let num_nodes = config.len();
        let (outgoing_tx, outgoing_rx) = mpsc::channel(100);
        let (incoming_tx, incoming_rx) = mpsc::channel(100);
        let (stop_tx, stop_rx) = mpsc::channel(1);

        let mut node = TcpNode {
            node_id,
            outgoing_tx,
            incoming_rx,
            task: None,
            stop_tx,
        };

        let mut worker = TcpWorker {
            outgoing_rx,
            stop_rx,
            peer_outgoing_tx: vec![],
            peer_stop_tx: vec![],
            peer_task: vec![],
        };

        for peer_node_id in 0..num_nodes {
            let (peer_outgoing_tx, peer_outgoing_rx) = mpsc::channel(100);
            let (peer_stop_tx, peer_stop_rx) = mpsc::channel(1);

            let peer = TcpPeer {
                node_id,
                peer_node_id,
                address: config[peer_node_id].clone(),
                outgoing_rx: peer_outgoing_rx,
                stop_rx: peer_stop_rx,
                incoming_tx: incoming_tx.clone(),
            };

            worker.peer_outgoing_tx.push(peer_outgoing_tx);
            worker.peer_stop_tx.push(peer_stop_tx);
            worker
                .peer_task
                .push(tokio::spawn(async move { peer.run().await }));
        }

        // start up a task that owns the worker, referenced by the JoinHandle
        // in the node
        node.task = Some(tokio::spawn(async move { worker.run().await }));
        node
    }

    pub async fn stop(&mut self) {
        self.stop_tx.send(()).await.unwrap();
        self.task.take().unwrap().await.unwrap();
    }
}

impl TcpWorker {
    async fn run(mut self) {
        loop {
            tokio::select! {
                // when we get a message on the stop channel, stop the peer
                // connections and then stop the loop..
                r = self.stop_rx.recv() => {
                    r.unwrap();
                    println!("stopping");
                    for mut tx in self.peer_stop_tx.drain(..) {
                        tx.send(()).await.unwrap();
                    }
                    for task in self.peer_task.drain(..) {
                        task.await.unwrap()
                    }
                    break;
                },

                // for outgoing messages, queue them in the appropriate peer
                Some((node_id, msg)) = self.outgoing_rx.recv() => {
                    println!("distribute {:?} to TcpPeer {:?}", msg, node_id);
                    self.peer_outgoing_tx[node_id].send(msg).await.unwrap();
                },
            }
        }
    }
}

impl TcpPeer {
    async fn run(mut self) {
        // TODO: some kind of state management for connecting, connected, waiting
        loop {
            tokio::select! {
                // when we get a message on the stop channel, stop the loop..
                r = self.stop_rx.recv() => {
                    r.unwrap();
                    println!("stopping peer {}", self.peer_node_id);
                    break;
                },

                // handle sending a message to this peer
                Some(msg) = self.outgoing_rx.recv() => {
                    // self-sends are pretty easy
                    if self.peer_node_id == self.node_id {
                        self.incoming_tx.send((self.node_id, msg)).await.unwrap();
                        continue;
                    }

                    // TODO
                    println!("send {:?} to {:?}", msg, self.peer_node_id);
                },
            }
        }
    }
}

#[async_trait]
impl RaftNetworkNode for TcpNode {
    async fn send(&mut self, dest: NodeId, msg: Message) -> Fallible<()> {
        self.outgoing_tx.send((dest, msg)).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Fallible<(NodeId, Message)> {
        if let Some((src, msg)) = self.incoming_rx.recv().await {
            return Ok((src, msg));
        }
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_stop() {
        let socket1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10000);
        let socket2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10001);
        let cfg = vec![socket1, socket2];
        let mut node = TcpNode::new(0, cfg);
        node.stop().await;
    }

    #[tokio::test]
    async fn test_self_send() {
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10000);
        let cfg = vec![socket];
        let mut node = TcpNode::new(0, cfg);
        node.send(0, b"Hello".to_vec()).await.unwrap();
        let (node_id, msg) = node.recv().await.unwrap();
        assert_eq!(node_id, 0);
        assert_eq!(msg, b"Hello");
        node.stop().await;
    }
}
