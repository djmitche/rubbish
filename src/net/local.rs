use crate::net::{Message, NodeId, NetworkNode};
use async_trait::async_trait;
use failure::Fallible;
use tokio::sync::mpsc;

pub struct LocalNetwork {
    nodes: Vec<Option<LocalNode>>,
}

impl LocalNetwork {
    /// Create a local network with the given number of nodes.
    pub fn new(network_size: NodeId) -> LocalNetwork {
        let mut txs = vec![];
        let mut rxs = vec![];

        for _ in 0..network_size {
            let (tx, rx) = mpsc::channel(100);
            txs.push(tx);
            rxs.push(Some(rx));
        }

        let mut nodes = vec![];
        for i in 0..network_size {
            let node = LocalNode {
                network_size,
                node_id: i,
                incoming: rxs[i].take().unwrap(),
                network: txs.clone(),
            };
            nodes.push(Some(node));
        }

        LocalNetwork { nodes }
    }

    /// Move a node out of this network.  This will panic if a node
    /// is taken twice.
    pub fn take(&mut self, node_id: NodeId) -> LocalNode {
        self.nodes[node_id].take().unwrap()
    }
}

/// A node in the local network, implementing NetworkNode.
pub struct LocalNode {
    network_size: usize,
    node_id: NodeId,
    incoming: mpsc::Receiver<(NodeId, Message)>,
    network: Vec<mpsc::Sender<(NodeId, Message)>>,
}

#[async_trait]
impl NetworkNode for LocalNode {
    fn network_size(&self) -> usize {
        self.network_size
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    async fn send(&mut self, dest: NodeId, msg: Message) -> Fallible<()> {
        self.network[dest].send((self.node_id, msg)).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Fallible<(NodeId, Message)> {
        if let Some((src, msg)) = self.incoming.recv().await {
            return Ok((src, msg));
        }
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_two_node_network() -> Fallible<()> {
        let mut net = LocalNetwork::new(2);

        let mut tasks = vec![];
        for node_id in 0..2 {
            let mut node = net.take(node_id);
            tasks.push(tokio::spawn(async move {
                if node.node_id() == 0 {
                    node.send(1, vec![1, 2, 3]).await.unwrap();
                    node.send(1, vec![2, 3, 4]).await.unwrap();
                } else {
                    assert_eq!(node.recv().await.unwrap(), (0, vec![1, 2, 3]));
                    assert_eq!(node.recv().await.unwrap(), (0, vec![2, 3, 4]));
                }
            }));
        }

        for task in tasks {
            task.await?;
        }

        Ok(())
    }
}
