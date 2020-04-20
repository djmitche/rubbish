use async_trait::async_trait;
use failure::Fallible;

pub mod local;
pub mod tcp;

/// An identifier for a node in the network.  These identifiers are global to the network
/// and must be managed externally.
pub type NodeId = usize;

/// A message to be sent or received on the network.
pub type Message = Vec<u8>;

#[async_trait]
pub trait NetworkNode {
    type Address;

    /// Get the size of the network (number of nodes)
    fn network_size(&self) -> usize;

    /// Get this node's ID
    fn node_id(&self) -> NodeId;

    /// Get another node's address
    fn address_of(&self, node: NodeId) -> Option<Self::Address>;

    /// Make a best effort to send the given message to the given node
    async fn send(&mut self, dest: NodeId, msg: Message) -> Fallible<()>;

    /// Receive the next message from any other node
    async fn recv(&mut self) -> Fallible<(NodeId, Message)>;
}
