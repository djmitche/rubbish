use async_trait::async_trait;
use failure::Fallible;

pub mod local;
pub mod tcp;

pub type NodeId = usize;

pub type Message = Vec<u8>;

#[async_trait]
pub trait RaftNetworkNode {
    /// Get the size of the network (number of nodes)
    fn network_size(&self) -> usize;

    /// Get this node's ID
    fn node_id(&self) -> NodeId;

    /// Make a best effort to send the given message to the given node
    async fn send(&mut self, dest: NodeId, msg: Message) -> Fallible<()>;

    /// Receive the next message from any other node
    async fn recv(&mut self) -> Fallible<(NodeId, Message)>;
}
