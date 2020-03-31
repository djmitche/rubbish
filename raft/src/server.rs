use crate::log::{LogEntry, RaftLog};
use crate::net::{NodeId, RaftNetworkNode};
use crate::{Index, Term};
use failure::Fallible;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task;

/// Set this to true to enable lots of println!
const DEBUG: bool = true;

/// A RaftServer represents a running server participating in a Raft.  Most of the
/// work of such a server occurs in the background.  In fact, all of the work occurs
/// in the background to simplify ownership of the data structures, and all
/// communication occurs via a control channel.
#[derive(Debug)]
pub struct RaftServer {
    /// The background task receiving messages for this server
    task: task::JoinHandle<()>,

    /// A channel to send control messages to the background task
    control_tx: mpsc::Sender<Control>,
}

#[derive(Debug)]
pub struct RaftServerInner<N: RaftNetworkNode + Sync + Send + 'static> {
    /// Number of nodes in the network
    network_size: usize,

    /// True if this server is the lader
    leader: bool,

    /// The network node, used for communication
    node: N,

    /// The local raft log
    log: RaftLog<char>,

    /// Channel indicating the task should stop
    control_rx: mpsc::Receiver<Control>,
}

/// Control messages sent to the background task
#[derive(Debug)]
enum Control {
    /// Stop the task
    Stop,

    /// Add a new entry
    Add(char),

    /// Return the current log for debugging
    #[cfg(test)]
    Debug(mpsc::Sender<RaftLog<char>>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum Message {
    AppendEntriesReq {
        index: Index,
        prev_term: Term,
        entries: Vec<LogEntry<char>>,
    },
    AppendEntriesRes {
        term: Term,
        success: bool,
    },
}

impl RaftServer {
    pub fn new<N: RaftNetworkNode + Sync + Send + 'static>(
        network_size: usize,
        leader: bool,
        node: N,
    ) -> RaftServer {
        let (control_tx, control_rx) = mpsc::channel(1);
        let inner = RaftServerInner {
            network_size,
            leader,
            node,
            log: RaftLog::new(),
            control_rx,
        };

        RaftServer {
            task: tokio::spawn(async move { inner.run().await }),
            control_tx,
        }
    }

    /// Get a copy of the current RaftLog (for testing)
    #[cfg(test)]
    async fn debug(&mut self) -> Fallible<RaftLog<char>> {
        let (log_tx, mut log_rx) = mpsc::channel(1);
        self.control_tx.send(Control::Debug(log_tx)).await?;
        Ok(log_rx.recv().await.unwrap())
    }

    /// Stop the server
    pub async fn stop(mut self) {
        self.control_tx.send(Control::Stop).await.unwrap();
        self.task.await.unwrap();
    }

    /// Add an entry to the log on the leader
    pub async fn add(&mut self, item: char) -> Fallible<()> {
        Ok(self.control_tx.send(Control::Add(item)).await?)
    }
}

impl<N: RaftNetworkNode + Sync + Send + 'static> RaftServerInner<N> {
    async fn run(mut self) {
        loop {
            tokio::select! {
                r = self.control_rx.recv() => {
                    if let Some(c) = r {
                        if self.handle_control(c).await.unwrap() {
                            break;
                        }
                    }
                },
                r = self.node.recv() => {
                    match r {
                        Ok((node_id, msg)) => self.handle_message(node_id, msg).await.unwrap(),
                        Err(e) => panic!("Error receiving from net: {}", e),
                    }
                }
            }
        }
    }

    async fn send_to(&mut self, peer: NodeId, message: &Message) -> Fallible<()> {
        let msg = serde_json::to_vec(message)?;
        self.node.send(peer, msg).await?;
        Ok(())
    }

    async fn handle_message(&mut self, node_id: NodeId, msg: Vec<u8>) -> Fallible<()> {
        let message: Message = serde_json::from_slice(&msg[..])?;
        self.log(format!("Handling message {:?} from {}", message, node_id));
        match message {
            Message::AppendEntriesReq {
                index,
                prev_term,
                entries,
            } => {
                let success = match self.log.append_entries(index, prev_term, entries) {
                    Ok(()) => true,
                    Err(_) => false,
                };
                self.send_to(node_id, &Message::AppendEntriesRes { term: 0, success })
                    .await?;

                Ok(())
            }
            Message::AppendEntriesRes { term, success } => {
                if !self.leader {
                    return Ok(());
                }

                // If the append wasn't successful, decrement the match index for this peer
                // and try again
                if !success {}
            }
        }
    }

    /// Handle a control message from the main process, and return true if the task should exit
    async fn handle_control(&mut self, c: Control) -> Fallible<bool> {
        self.log(format!("Handling Control message {:?}", c));
        match c {
            Control::Stop => Ok(true),

            Control::Add(item) => {
                assert!(self.leader);
                let term = 0; // TODO
                let index = self.log.len() as Index;
                let prev_term = if index > 0 {
                    self.log.get(index - 1).term
                } else {
                    term
                };
                let entry = LogEntry::new(term, item);
                self.log
                    .append_entries(index, prev_term, vec![entry.clone()])?;

                let message = Message::AppendEntriesReq {
                    index,
                    prev_term,
                    entries: vec![entry],
                };
                for follower in 0..self.network_size {
                    self.send_to(follower, &message).await?;
                }

                Ok(false)
            }

            #[cfg(test)]
            Control::Debug(mut tx) => {
                tx.send(self.log.clone()).await?;
                Ok(false)
            }
        }
    }

    fn log<S: AsRef<str>>(&self, msg: S) {
        if cfg!(debug) && DEBUG {
            // TODO: show node id?
            println!("leader={} - {}", self.leader, msg.as_ref());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::local::make_network;

    #[tokio::test]
    async fn append_entries() -> Fallible<()> {
        let mut net = make_network(2);
        let mut leader: RaftServer = RaftServer::new(2, true, net.remove(0));
        let mut follower: RaftServer = RaftServer::new(2, false, net.remove(0));

        leader.add('x').await?;
        leader.add('y').await?;

        let log = leader.debug().await?;
        assert_eq!(log.get(0), &LogEntry::new(0, 'x'));
        assert_eq!(log.get(1), &LogEntry::new(0, 'y'));

        // TODO: delay to let things settle?

        let log = follower.debug().await?;
        assert_eq!(log.get(0), &LogEntry::new(0, 'x'));
        assert_eq!(log.get(1), &LogEntry::new(0, 'y'));

        Ok(())
    }
}
