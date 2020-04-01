use crate::net::{Message, NodeId, RaftNetworkNode};
use async_trait::async_trait;
use failure::Fallible;
use tokio::sync::mpsc;

/// Run a local network with the given number of nodes, calling the node_fn to
/// represent each node.
pub fn make_network(network_size: NodeId) -> Vec<LocalNode> {
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
        nodes.push(node);
    }
    nodes
}

/// A node in the local network, implementing RaftNetworkNode.
pub struct LocalNode {
    network_size: usize,
    node_id: NodeId,
    incoming: mpsc::Receiver<(NodeId, Message)>,
    network: Vec<mpsc::Sender<(NodeId, Message)>>,
}

#[async_trait]
impl RaftNetworkNode for LocalNode {
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
        let mut net = make_network(2);

        let tasks = net.drain(..).map(move |mut node| {
            tokio::spawn(async move {
                if node.node_id == 0 {
                    node.send(1, vec![1, 2, 3]).await.unwrap();
                    node.send(1, vec![2, 3, 4]).await.unwrap();
                } else {
                    assert_eq!(node.recv().await.unwrap(), (0, vec![1, 2, 3]));
                    assert_eq!(node.recv().await.unwrap(), (0, vec![2, 3, 4]));
                }
            })
        });

        for task in tasks {
            task.await?;
        }

        Ok(())
    }
}
