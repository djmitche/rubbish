use failure::Fallible;

pub mod local;

pub type NodeId = usize;

pub trait RaftNetworkNode {
    /// Make a best effort to send the given message to the given node
    fn send(&mut self, dest: NodeId, msg: Vec<u8>) -> Fallible<()>;

    /// Receive the next message from any other node
    fn recv(&mut self) -> Fallible<(NodeId, Vec<u8>)>;
}
