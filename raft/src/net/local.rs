use crate::net::{NodeId, RaftNetworkNode};
use failure::Fallible;
use std::sync::mpsc;
use std::thread;

/// Run a local network with the given number of nodes, calling the node_fn to
/// represent each node.
pub fn run_network(num_nodes: NodeId, node_fn: fn(LocalNode)) {
    let mut txs = vec![];
    let mut rxs = vec![];

    for _ in 0..num_nodes {
        let (tx, rx) = mpsc::channel();
        txs.push(tx);
        rxs.push(Some(rx));
    }

    let mut threads = vec![];
    for i in 0..num_nodes {
        let node = LocalNode {
            node_id: i,
            incoming: rxs[i].take().unwrap(),
            network: txs.clone(),
        };
        threads.push(thread::spawn(move || node_fn(node)));
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

/// A node in the local network, implementing RaftNetworkNode.
pub struct LocalNode {
    pub node_id: NodeId,
    incoming: mpsc::Receiver<(NodeId, Vec<u8>)>,
    network: Vec<mpsc::Sender<(NodeId, Vec<u8>)>>,
}

impl RaftNetworkNode for LocalNode {
    fn send(&mut self, dest: NodeId, msg: Vec<u8>) -> Fallible<()> {
        self.network[dest].send((self.node_id, msg))?;
        Ok(())
    }

    fn recv(&mut self) -> Fallible<(NodeId, Vec<u8>)> {
        let (src, msg) = self.incoming.recv()?;
        Ok((src, msg))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_two_node_network() -> Fallible<()> {
        let net = run_network(2, |mut node| {
            if (node.node_id == 0) {
                node.send(1, vec![1, 2, 3]);
                node.send(1, vec![2, 3, 4]);
            } else {
                assert_eq!(node.recv().unwrap(), (0, vec![1, 2, 3]));
                assert_eq!(node.recv().unwrap(), (0, vec![2, 3, 4]));
            }
        });

        Ok(())
    }
}
