use crate::net::{Message, NodeId, NetworkNode};
use async_trait::async_trait;
use byteorder::{ByteOrder, NetworkEndian};
use failure::Fallible;
use net2::TcpBuilder;
use nix::sys::socket::sockopt::ReusePort;
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::sync::mpsc;
use tokio::task;
use tokio::time::delay_for;

/* A few words about the structure here.
 *
 * A network is composed of multiple nodes.  Typically only one node is running in a process, but
 * during testing multiple nodes may coexist within a single process.  This is represented by a
 * TcpNode, which implements NetworkNode.
 *
 * The node has a TcpPeer object for each node on the network (including itself) that handles
 * communication with that peer.
 *
 * The TcpNode itself and each TcpPeer have an "inner" object containing dynamic state as well as a
 * Tokio task to manage that state.  All communication between objects are handled via channels,
 * avoiding any shared state.  The two ends of each channel are named uniquely with <prefix>_tx for
 * the Sender end and <prefix>_rx for the Receiver end.  These two values typically end up in
 * different objects, but the names indicate the connection.
 *
 * The TcpNode, the public API, has three channels to communicate with its inner object:
 *
 *  - outgoing_tx is connected to the TcpNodeInner's outgoing_rx and handles outgoing messages,
 *    tagged with a destination node ID
 *  - incoming_rx is connected to incoming_tx on all TcpPeerInner objects and carries messages
 *    coming in to this node
 *  - stop_tx is connected to the TcpNodeInner's stop_rx and is a control channel used to signal
 *    the node's task to stop
 *
 * Each TcpPeer has three channels:
 *  - outgoing_tx is connected to the TcpPeerInner's outgoing_rx and carries messages destined
 *    for this peer.  TcpNode simply routes messages from its outgoing_rx to the appropriate peer's
 *    outgoing_tx.
 *  - connection_tx is connected to the TcpPeerInner's connection_rx and carries TcpStream objects
 *    from the TcpNode's listening socket to the appropriate TcpPeer.
 *  - stop_tx is connected to the TcpPeerInneres stop_rx and is a control channel used to signal
 *    the peer's task to stop.  The node uses this channel to stop all peer tasks before stopping
 *    itself.
 */

// set this to true to add a lot of println!
const DEBUG: bool = false;

/// A TcpConfig is a vector of nodes in the network, giving the address at which that node is
/// listening.
pub type TcpConfig = Vec<SocketAddr>;

/// A node on a TCP network, implementing NetworkNode.
pub struct TcpNode {
    /// Nodes in this network
    config: TcpConfig,

    /// This node's ID
    node_id: NodeId,

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

    /// Config for this network
    config: TcpConfig,

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

    /// Buffer for assembling incoming messages
    buffer: Vec<u8>,

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
        let network_size = config.len();
        let (outgoing_tx, outgoing_rx) = mpsc::channel(100);
        let (incoming_tx, incoming_rx) = mpsc::channel(100);
        let (stop_tx, stop_rx) = mpsc::channel(1);

        let mut inner = TcpNodeInner {
            node_id,
            address: config[node_id].clone(),
            config: config.clone(),
            outgoing_rx,
            stop_rx,
            peers: vec![],
        };

        for peer_node_id in 0..network_size {
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
            config,
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
impl NetworkNode for TcpNode {
    type Address = SocketAddr;

    fn network_size(&self) -> usize {
        self.config.len()
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn address_of(&self, node: NodeId) -> Option<Self::Address> {
        self.config.get(node).cloned()
    }

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
        let mut listener = {
            let listener = make_sock().unwrap();
            self.log(format!("binding listening socket to {}", self.address));
            let listener = listener.bind(self.address).unwrap();
            let listener = listener.listen(5).unwrap();
            TcpListener::from_std(listener).unwrap()
        };
        self.log(format!("listening on {}", self.address));

        'select: loop {
            self.log("LOOP");
            tokio::select! {
                _ = self.stop_rx.recv() => break,

                // a new TCP connection has arrived, but we need to determine which peer it is from
                r = listener.accept() => {
                    self.log("ACCEPT");
                    match r {
                        Ok((sock, addr)) => {
                            self.log(format!("got connection from {:?}", addr));

                            // find the peer with this address
                            for (node_id, peer_addr) in self.config.iter().enumerate() {
                                if peer_addr == &addr {
                                    self.log(format!("identified as node {}", node_id));
                                    self.peers[node_id].connection_tx.send(sock).await.unwrap();
                                    continue 'select;
                                }
                            }
                            self.log(format!("unrecognized source address"));
                            // note that if we didn't hand the socket off, we drop it here, closing it
                        },
                        Err(e) => {
                            self.log(format!("error from accept: {}", e));
                        },
                    };
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

    fn log<S: AsRef<str>>(&self, msg: S) {
        if cfg!(debug) && DEBUG {
            println!("node={} - {}", self.node_id, msg.as_ref());
        }
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
            buffer: vec![],
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
    /* This implements a simple state machine, with each state represented by an
     * enum value and a method of the same name.
     *
     * The state graph is
     *          +-> Listen ----+
     *         /                \
     * Start --+-> Loopback      +-> Connected --> Wait -+
     *   ^     \                /                        /
     *   |      +-> Connect ---+                        /
     *   +---------------------------------------------+
     *
     * The decision on which state to enter from Start is based on the relation
     * between this node's ID and the peer's ID.  Between any two nodes, the lower-
     * numbered node is responsible for connecting to the higher-numbered node.
     * The Loopback state is for communicating to the local node (peer == node_id).
     */

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

    /// Start communication with this peer; this is a transient state that decides
    /// how the socket connection will be initiated.
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

    /// Listen for an incoming connection from this peer.  Connections are delivered
    /// over a channel from a listening socket in the TcpNode.  Once we have a
    /// connection, enter the Connected state.
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

    /// The call is coming from inside the house.  The peer is this node, so simply
    /// loop messages back.
    async fn loopback(&mut self) -> TcpPeerState {
        self.log("entering state loopback");
        loop {
            tokio::select! {
                // as a loopback peer, we just add new messages to our own incoming
                // queue
                Some(msg) = self.outgoing_rx.recv() => {
                    self.incoming_tx.send((self.peer_node_id, msg)).await.unwrap();
                },

                _ = self.stop_rx.recv() => return TcpPeerState::Stop,
            }
        }
    }

    /// Connect to the peer, and once a connection is established, enter the Connected state.
    async fn connect(&mut self) -> TcpPeerState {
        self.log("entering state connect");
        assert!(self.sock.is_none());
        let sock = make_sock().unwrap();
        // bind the local end to our address/port, so we can be recognized by listeners
        let sock = match sock.bind(self.address) {
            Ok(s) => s,
            Err(e) => {
                self.log(format!("Error binding: {}", e));
                drop(sock);
                return TcpPeerState::Wait;
            }
        };
        // TODO: BLOCKING connect?!  How can we do a non-blocking connect with
        // a bound socket?
        self.log(format!("Connecting to {:?}", self.peer_address));
        let sock = match sock.connect(self.peer_address) {
            Ok(s) => s,
            Err(e) => {
                self.log(format!("Connect failed (retrying): {:?}", e));
                drop(sock);
                return TcpPeerState::Wait;
            }
        };
        let sock = TcpStream::from_std(sock).unwrap();
        self.sock = Some(sock);
        return TcpPeerState::Connected;
    }

    /// The peer is connected and `sock` is set correctly.  Communicate over that socket
    /// until an error occurs, at which point enter the Wait state.
    async fn connected(&mut self) -> TcpPeerState {
        self.log("entering state connected");

        let mut sock = self.sock.take().unwrap();
        let mut buf = [0u8; 4096];

        loop {
            tokio::select! {
                _ = self.stop_rx.recv() => return TcpPeerState::Stop,

                // an outgoing message is sent to the peer
                Some(msg) = self.outgoing_rx.recv() => {
                    self.log(format!("sending {:?}", msg));
                    // TODO: this might hold up this loop (maybe add another state for writing?)
                    // TODO: this might fail (in which case go to Wait)
                    sock.write_u32(msg.len() as u32).await.unwrap();
                    sock.write_all(&msg[..]).await.unwrap();
                },
                r = sock.read(&mut buf[..]) => {
                    self.log("READ");
                    match r {
                        Ok(0) => {
                            self.log(format!("EOF while reading"));
                            return TcpPeerState::Wait;
                        },
                        Ok(n) => {
                            self.log(format!("Ok({})", n));
                            // append this to the buffer..
                            self.buffer.extend_from_slice(&buf[..n]);
                            // ..and process messages from the buffer
                            while let Some(msg) = self.message_from_buffer() {
                                self.log(format!("read msg {:?}", msg));
                                self.incoming_tx.send((self.peer_node_id, msg)).await.unwrap();
                            }
                        },
                        Err(e) => {
                            self.log(format!("Error while reading: {}", e));
                            return TcpPeerState::Wait;
                        },
                    }
                },
            }
        }
    }

    /// Something went wrong, so wait a bit and try again. by returning to Start.
    async fn wait(&mut self) -> TcpPeerState {
        self.log("entering state wait");
        self.sock = None;

        tokio::select! {
            _ = self.stop_rx.recv() => return TcpPeerState::Stop,

            // TODO: start small, get up to 200ms or so
            _ = delay_for(Duration::from_millis(100)) => return TcpPeerState::Start,
        }
    }

    // utilities

    /// Extract a message from self.buffer, if the entire message is present, and
    /// leave any remaining bytes in the buffer.
    fn message_from_buffer(&mut self) -> Option<Message> {
        if self.buffer.len() < 4 {
            return None;
        }

        let len = NetworkEndian::read_u32(&self.buffer[..4]) as usize;
        if self.buffer.len() < 4 + len {
            return None;
        }

        // split off the message and keep the remaining bytes in self.buffer
        // TODO: extra allocations here..
        let remaining = self.buffer.split_off(4 + len);
        let msg = self.buffer[4..].to_vec();
        self.buffer = remaining;

        Some(msg)
    }

    fn log<S: AsRef<str>>(&self, msg: S) {
        if cfg!(test) && DEBUG {
            println!(
                "node={} peer={} - {}",
                self.node_id,
                self.peer_node_id,
                msg.as_ref()
            );
        }
    }
}

fn make_sock() -> Fallible<TcpBuilder> {
    let sock = TcpBuilder::new_v4()?;
    sock.reuse_address(true)?;

    // net2 doesn't support reuse_port, so we use nix..
    nix::sys::socket::setsockopt(sock.as_raw_fd(), ReusePort, &true)?;

    Ok(sock)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::atomic::{AtomicU16, Ordering};

    /// Because all of these tests can run concurrently, they must use distinct port numbers.  We
    /// cannot let the kernel choose the ports, because we must know all ports in advance.  So,
    /// we generate a sequence of port numbers in a thread-safe fashion.
    fn fresh_port() -> u16 {
        static mut NEXT_PORT: AtomicU16 = AtomicU16::new(10000);
        unsafe { NEXT_PORT.fetch_add(1, Ordering::SeqCst) }
    }

    #[tokio::test]
    async fn test_stop() {
        let socket1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
        let socket2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
        let cfg = vec![socket1, socket2];
        let node = TcpNode::new(0, cfg);
        node.stop().await;
    }

    #[tokio::test]
    async fn test_self_send() {
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
        let cfg = vec![socket];
        let mut node = TcpNode::new(0, cfg);
        node.send(0, b"Hello".to_vec()).await.unwrap();
        let (node_id, msg) = node.recv().await.unwrap();
        assert_eq!(node_id, 0);
        assert_eq!(msg, b"Hello");
        node.stop().await;
    }

    #[tokio::test]
    async fn test_net_size() {
        let socket0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
        let socket1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
        let cfg = vec![socket0, socket1];
        let node0 = TcpNode::new(0, cfg.clone());
        assert_eq!(node0.network_size(), 2);
        node0.stop().await;
    }

    #[tokio::test]
    async fn test_addr_of() {
        let socket0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
        let socket1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
        let cfg = vec![socket0, socket1];
        let node0 = TcpNode::new(0, cfg.clone());
        assert_eq!(node0.address_of(0), Some(socket0));
        assert_eq!(node0.address_of(1), Some(socket1));
        node0.stop().await;
    }

    #[tokio::test]
    async fn test_send_uphill() {
        // "uphill" meaning from a low NodeId to a higher NodeId
        let socket0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
        let socket1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
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

    #[tokio::test]
    async fn test_send_downhill() {
        // "downhill" meaning from a high NodeId to a lower NodeId
        let socket0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
        let socket1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
        let cfg = vec![socket0, socket1];
        let mut node0 = TcpNode::new(0, cfg.clone());
        let mut node1 = TcpNode::new(1, cfg.clone());
        node1.send(0, b"Hello".to_vec()).await.unwrap();
        let (node_id, msg) = node0.recv().await.unwrap();
        assert_eq!(node_id, 1);
        assert_eq!(msg, b"Hello");
        node0.stop().await;
        node1.stop().await;
    }

    #[tokio::test]
    async fn test_send_bidirectional() {
        // "downhill" meaning from a high NodeId to a lower NodeId
        let socket0 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
        let socket1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), fresh_port());
        let cfg = vec![socket0, socket1];
        let mut node0 = TcpNode::new(0, cfg.clone());
        let mut node1 = TcpNode::new(1, cfg.clone());

        node1.send(0, b"Hello".to_vec()).await.unwrap();
        node0.send(1, b"World".to_vec()).await.unwrap();

        let (node_id, msg) = node0.recv().await.unwrap();
        assert_eq!(node_id, 1);
        assert_eq!(msg, b"Hello");

        let (node_id, msg) = node1.recv().await.unwrap();
        assert_eq!(node_id, 0);
        assert_eq!(msg, b"World");

        node0.stop().await;
        node1.stop().await;
    }
}
