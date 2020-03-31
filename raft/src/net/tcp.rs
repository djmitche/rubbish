use crate::net::{Message, NodeId, RaftNetworkNode};
use async_trait::async_trait;
use failure::Fallible;
use net2::TcpBuilder;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::sync::mpsc;
use tokio::task;
use tokio::time::delay_for;

// TODO: document the network of channels

/// A TcpConfig is a vector of nodes in the network, giving the address at which that node is
/// listening.
type TcpConfig = Vec<SocketAddr>;

/// A node on a TCP network, implementing RaftNetworkNode.
pub struct TcpNode {
    /// This node's ID
    pub node_id: NodeId,

    /// Outoing messages, to any node
    outgoing_tx: mpsc::Sender<(NodeId, Message)>,

    /// Incoming messages, from any node
    incoming_rx: mpsc::Receiver<(NodeId, Message)>,

    /// The JoinHandle for the node-management task
    task: task::JoinHandle<()>,

    /// A channel to indicate that the node-management task should stop
    stop_tx: mpsc::Sender<()>,
}

/// A TcpNodeInner is the inner state of a TcpNode.  This data structure is entirely
/// owned by a task, and all communication with it is via chanels.  It is responsible
/// for managing TcpPeers:
///
/// * Creating and stopping TcpPeers
/// * Routing outgoing messages to ppers
/// * Collecting incoming messages from peers
struct TcpNodeInner {
    /// This node's ID
    node_id: NodeId,

    /// This node's address
    address: SocketAddr,

    /// channel containing messages to go out, multiplexed to the peers
    outgoing_rx: mpsc::Receiver<(NodeId, Message)>,

    /// A channel on which a message means "stop"
    stop_rx: mpsc::Receiver<()>,

    /// Peers of this node (including itself)
    peers: Vec<TcpPeer>,
}

/// A connection to a network peer.  This is responsible for connecting to the peer or
/// listening for incoming connections, queueing outgoing messages, and routing incoming
/// messages.
struct TcpPeer {
    /// Outgoing messages to this peer
    outgoing_tx: mpsc::Sender<Message>,

    /// Connections from this peer
    connection_tx: mpsc::Sender<TcpStream>,

    /// The JoinHandle for the peer-management task
    task: task::JoinHandle<()>,

    /// A channel to indicate that the peer-management task should stop
    stop_tx: mpsc::Sender<()>,
}

struct TcpPeerInner {
    /// This node's ID
    node_id: NodeId,

    /// This node's address
    address: SocketAddr,

    /// Peer's node ID
    peer_node_id: NodeId,

    /// Peer's address
    peer_address: SocketAddr,

    /// Socket connected to the peer
    sock: Option<TcpStream>,

    /// A channel for messages destined to this peer
    outgoing_rx: mpsc::Receiver<Message>,

    /// A cannel for connections made from this node
    connection_rx: mpsc::Receiver<TcpStream>,

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

        let mut inner = TcpNodeInner {
            node_id,
            address: config[node_id].clone(),
            outgoing_rx,
            stop_rx,
            peers: vec![],
        };

        for peer_node_id in 0..num_nodes {
            inner.peers.push(TcpPeer::new(
                node_id,
                peer_node_id,
                &config,
                incoming_tx.clone(),
            ));
        }

        // start up a task that owns the inner, referenced by the JoinHandle
        // in the node
        TcpNode {
            node_id,
            outgoing_tx,
            incoming_rx,
            task: tokio::spawn(async move { inner.run().await }),
            stop_tx,
        }
    }

    pub async fn stop(mut self) {
        self.stop_tx.send(()).await.unwrap();
        self.task.await.unwrap();
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

impl TcpNodeInner {
    async fn run(mut self) {
        let listener = TcpBuilder::new_v4()
            .unwrap()
            .reuse_address(true)
            .unwrap()
            .bind(self.address)
            .unwrap()
            .listen(5)
            .unwrap();
        let mut listener = TcpListener::from_std(listener).unwrap();
        let (connection_tx, mut connection_rx) = mpsc::channel(1);

        loop {
            tokio::select! {
                _ = self.stop_rx.recv() => break,

                // a new TCP connection has arrived, but we need to determine which peer it is from
                r = listener.accept() => {
                    if let Ok((sock, _)) = r {
                        let tx = connection_tx.clone();
                        tokio::spawn(async move {
                            TcpNodeInner::handle_accept(sock, tx.clone()).await;
                        });
                    }
                },

                // a new socket has come back from handle_accept..
                Some((node_id, sock)) = connection_rx.recv() => {
                    self.peers[node_id].connection_tx.send(sock).await.unwrap();
                },

                // for outgoing messages, queue them in the appropriate peer
                Some((node_id, msg)) = self.outgoing_rx.recv() => {
                    self.log(format!("distribute {:?} to TcpPeerInner {:?}", msg, node_id));
                    self.peers[node_id].outgoing_tx.send(msg).await.unwrap();
                },
            }
        }

        // when we get a message on the stop channel, stop the peer
        // connections and then stop the loop..
        self.log("stopping");
        for peer in self.peers.iter_mut() {
            peer.stop_tx.send(()).await.unwrap();
        }
        for peer in self.peers.drain(..) {
            peer.task.await.unwrap();
        }
    }

    /// Handle an incoming socket, reading the remote end's node_id from the socket and, if
    /// successful, sending the connection back with that node id
    async fn handle_accept(
        mut sock: TcpStream,
        mut connection_tx: mpsc::Sender<(NodeId, TcpStream)>,
    ) {
        match sock.read_u32().await {
            Ok(node_id) => connection_tx.send((node_id as NodeId, sock)).await.unwrap(),
            Err(_) => return, // ignore error
        }
    }

    fn log<S: AsRef<str>>(&self, msg: S) {
        println!("node={} - {}", self.node_id, msg.as_ref());
    }
}

impl TcpPeer {
    fn new(
        node_id: NodeId,
        peer_node_id: NodeId,
        config: &TcpConfig,
        incoming_tx: mpsc::Sender<(NodeId, Message)>,
    ) -> TcpPeer {
        let (peer_outgoing_tx, peer_outgoing_rx) = mpsc::channel(100);
        let (peer_connection_tx, peer_connection_rx) = mpsc::channel(1);
        let (peer_stop_tx, peer_stop_rx) = mpsc::channel(1);

        let inner = TcpPeerInner {
            node_id,
            address: config[node_id].clone(),
            peer_node_id,
            peer_address: config[peer_node_id].clone(),
            sock: None,
            outgoing_rx: peer_outgoing_rx,
            connection_rx: peer_connection_rx,
            stop_rx: peer_stop_rx,
            incoming_tx: incoming_tx,
        };

        // start up a task that owns the inner, referenced by the JoinHandle in the peer
        TcpPeer {
            outgoing_tx: peer_outgoing_tx,
            connection_tx: peer_connection_tx,
            stop_tx: peer_stop_tx,
            task: tokio::spawn(async move { inner.run().await }),
        }
    }
}

enum TcpPeerState {
    Start,
    Listen,
    Loopback,
    Connect,
    Connected,
    Wait,
    Stop,
}

impl TcpPeerInner {
    // This implements a simple state machine where the state is defined by the current method.
    // This results in a bit of duplicate code, but it's minimal

    async fn run(mut self) {
        let mut state = TcpPeerState::Start;

        loop {
            state = match state {
                TcpPeerState::Start => self.start().await,
                TcpPeerState::Listen => self.listen().await,
                TcpPeerState::Loopback => self.loopback().await,
                TcpPeerState::Connect => self.connect().await,
                TcpPeerState::Connected => self.connected().await,
                TcpPeerState::Wait => self.wait().await,
                TcpPeerState::Stop => return,
            }
        }
    }

    async fn start(&mut self) -> TcpPeerState {
        self.log("entering state start");
        if self.peer_node_id < self.node_id {
            return TcpPeerState::Listen;
        } else if self.peer_node_id == self.node_id {
            return TcpPeerState::Loopback;
        } else {
            return TcpPeerState::Connect;
        }
    }

    async fn listen(&mut self) -> TcpPeerState {
        self.log("entering state listen");

        loop {
            tokio::select! {
                _ = self.stop_rx.recv() => return TcpPeerState::Stop,

                Some(sock) = self.connection_rx.recv() => {
                    self.sock = Some(sock);
                    return TcpPeerState::Connected;
                },
            }
        }
    }

    async fn loopback(&mut self) -> TcpPeerState {
        self.log("entering state loopback");
        loop {
            tokio::select! {
                // as a loopback peer, we just add new messages to our own incoming
                // queue
                Some(msg) = self.outgoing_rx.recv() => {
                    self.incoming_tx.send((self.node_id, msg)).await.unwrap();
                },

                _ = self.stop_rx.recv() => return TcpPeerState::Stop,
            }
        }
    }

    async fn connect(&mut self) -> TcpPeerState {
        self.log("entering state connect");
        let sock = TcpBuilder::new_v4().unwrap();
        let sock = sock.reuse_address(true).unwrap();
        let sock = sock.bind(self.address).unwrap();
        // TODO: BLOCKING connect?!
        self.log(format!("Connecting to {:?}", self.address));
        let sock = match sock.connect(self.address) {
            Ok(s) => s,
            Err(e) => {
                self.log(format!("Connect failed (retrying): {:?}", e));
                return TcpPeerState::Wait;
            }
        };
        let sock = TcpStream::from_std(sock).unwrap();
        // XXX
        //sock.write_u32(self.node_id as u32).await.unwrap(); // TODO: handle failure
        self.sock = Some(sock);
        return TcpPeerState::Connected;
    }

    async fn connected(&mut self) -> TcpPeerState {
        self.log("entering state connected");
        assert!(self.sock.is_some());
        loop {
            tokio::select! {
                _ = self.stop_rx.recv() => return TcpPeerState::Stop,

                // an outgoing message is sent to the peer
                Some(msg) = self.outgoing_rx.recv() => {
                    // TODO: this might hold up this loop (maybe add another state for writing?)
                    // TODO: this might fail (in which case go to Wait)
                    self.sock.as_mut().unwrap().write_u32(msg.len() as u32).await.unwrap();
                    self.sock.as_mut().unwrap().write_all(&msg[..]).await.unwrap();
                },
            }
        }
    }

    async fn wait(&mut self) -> TcpPeerState {
        self.log("entering state wait");
        self.sock = None;
        // TODO: do this in a loop
        // TODO: start small, get up to 200ms or so
        delay_for(Duration::from_millis(100)).await;
        return TcpPeerState::Start;
    }

    fn log<S: AsRef<str>>(&self, msg: S) {
        println!(
            "node={} peer={} - {}",
            self.node_id,
            self.peer_node_id,
            msg.as_ref()
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_stop() {
        let socket1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 13000);
        let socket2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 13001);
        let cfg = vec![socket1, socket2];
        let node = TcpNode::new(0, cfg);
        node.stop().await;
    }

    #[tokio::test]
    async fn test_self_send() {
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 14000);
        let cfg = vec![socket];
        let mut node = TcpNode::new(0, cfg);
        node.send(0, b"Hello".to_vec()).await.unwrap();
        let (node_id, msg) = node.recv().await.unwrap();
        assert_eq!(node_id, 0);
        assert_eq!(msg, b"Hello");
        node.stop().await;
    }

    #[tokio::test]
    async fn test_send_uphill() {
        // "uphill" meaning from a low NodeId to a higher NodeId
        let socket0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 15000);
        let socket1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 15001);
        let cfg = vec![socket0, socket1];
        let mut node0 = TcpNode::new(0, cfg.clone());
        let mut node1 = TcpNode::new(1, cfg.clone());
        node0.send(1, b"Hello".to_vec()).await.unwrap();
        let (node_id, msg) = node1.recv().await.unwrap();
        assert_eq!(node_id, 0);
        assert_eq!(msg, b"Hello");
        node0.stop().await;
        node1.stop().await;
    }
}
