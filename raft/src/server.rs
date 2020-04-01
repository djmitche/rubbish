use crate::log::{LogEntry, RaftLog};
use crate::net::{NodeId, RaftNetworkNode};
use crate::{Index, Term};
use failure::Fallible;
use serde::{Deserialize, Serialize};
use std::cmp;
use std::iter;
use std::time::Duration;
use tokio::stream::StreamExt;
use tokio::sync::mpsc;
use tokio::task;
use tokio::time::{delay_queue, DelayQueue};

/// Set this to true to enable lots of println!
const DEBUG: bool = true;

/// Max time between AppendEntries calls
const HEARTBEAT: Duration = Duration::from_millis(100);

/// A RaftServer represents a running server participating in a Raft.
#[derive(Debug)]
pub struct RaftServer {
    /// The background task receiving messages for this server
    task: task::JoinHandle<()>,

    /// A channel to send control messages to the background task
    control_tx: mpsc::Sender<Control>,
}

/* Most of the work of a server occurs in a background task, reacting to messages and timers.  In
 * fact, all of the work occurs in the background to simplify ownership of the data structures, and
 * all communication occurs control_tx / control_rx.  Public methods simply send control messages
 * to the background task.  In cases where a reply is required, the control message contains a
 * transient channel to carry the response.
 */

#[derive(Debug)]
pub struct RaftServerInner<N: RaftNetworkNode + Sync + Send + 'static> {
    /*
     * Mechanics
     */
    /// The network node, used for communication
    node: N,

    /// Channel indicating the task should stop
    control_rx: mpsc::Receiver<Control>,

    /// A queue of Timer objects
    timers: DelayQueue<Timer>,

    /// DelayQueue keys for the last time AppendEntries was sent to this peer
    last_append_entries: Vec<Option<delay_queue::Key>>,

    /*
     * Algorithm State
     */
    /// Current server mode
    mode: Mode,

    /// "latest term the server has seen"
    current_term: Term,

    /// Current leader, if known
    current_leader: Option<NodeId>,

    /// "candidateId that received vote in current term (or null if none)"
    voted_for: Option<NodeId>,

    /// The log entries
    log: RaftLog<char>,

    /// Index of the highest log entry known to be committed
    commit_index: Index,

    /// Index of the highest log entry applied to state machine
    last_applied: Index,

    /// "for each server, index of the next log entry to send to that server"
    next_index: Vec<Index>,

    /// "for each server, index of the highest log entry known to be replicated on server"
    match_index: Vec<Index>,
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
    GetState(mpsc::Sender<ServerState>),
}

/// A Timer is an event that is scheduled at some future time.
#[derive(Debug)]
enum Timer {
    /// This follower may not have gotten an AppendEntriesReq in a while
    FollowerUpdate(NodeId),
}

/// The current mode of the server
#[derive(Debug, PartialEq, Clone, Copy)]
enum Mode {
    Follower,
    Candidate,
    Leader,
}

/// State of the server, used for debugging with get_state
#[cfg(test)]
#[derive(Debug)]
struct ServerState {
    mode: Mode,

    /// "latest term the server has seen"
    current_term: Term,

    /// Current leader, if known
    current_leader: Option<NodeId>,

    /// "candidateId that received vote in current term (or null if none)"
    voted_for: Option<NodeId>,

    /// The log entries
    log: RaftLog<char>,

    /// Index of the highest log entry known to be committed
    commit_index: Index,

    /// Index of the highest log entry applied to state machine
    last_applied: Index,

    /// "for each server, index of the next log entry to send to that server"
    next_index: Vec<Index>,

    /// "for each server, index of the highest log entry known to be replicated on server"
    match_index: Vec<Index>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum Message {
    AppendEntriesReq {
        term: Term,
        leader: NodeId,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry<char>>,
        leader_commit: Index,
    },
    AppendEntriesRep {
        term: Term,
        next_index: Index,
        success: bool,
    },
}

impl RaftServer {
    pub fn new<N: RaftNetworkNode + Sync + Send + 'static>(node: N) -> RaftServer {
        let (control_tx, control_rx) = mpsc::channel(1);
        let network_size = node.network_size();
        let inner = RaftServerInner {
            node,
            timers: DelayQueue::new(),
            last_append_entries: iter::repeat_with(|| None).take(network_size).collect(),
            control_rx,
            mode: Mode::Follower,
            current_term: 0,
            current_leader: None,
            voted_for: None,
            log: RaftLog::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: [1].repeat(network_size),
            match_index: [0].repeat(network_size),
        };

        RaftServer {
            task: tokio::spawn(async move { inner.run().await }),
            control_tx,
        }
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

    /// Get a copy of the current server state (for testing)
    #[cfg(test)]
    async fn get_state(&mut self) -> Fallible<ServerState> {
        let (log_tx, mut log_rx) = mpsc::channel(1);
        self.control_tx.send(Control::GetState(log_tx)).await?;
        Ok(log_rx.recv().await.unwrap())
    }
}

impl<N: RaftNetworkNode + Sync + Send + 'static> RaftServerInner<N> {
    // event handling

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
                        Ok((peer, msg)) => self.handle_message(peer, msg).await.unwrap(),
                        Err(e) => panic!("Error receiving from net: {}", e),
                    }
                },

                Some(t) = self.timers.next() => {
                    self.handle_timer(t.unwrap().into_inner()).await.unwrap();
                }
            }
        }
    }

    async fn handle_message(&mut self, peer: NodeId, msg: Vec<u8>) -> Fallible<()> {
        let message: Message = serde_json::from_slice(&msg[..])?;
        self.log(format!("Handling Message {:?} from {}", message, peer));
        match message {
            Message::AppendEntriesReq {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                // Reject this request if term < our current_term
                let mut success = term >= self.current_term;

                // Reject this request if the log does not apply cleanly
                if success {
                    success = match self
                        .log
                        .append_entries(prev_log_index, prev_log_term, entries)
                    {
                        Ok(()) => true,
                        Err(_) => false,
                    };
                }

                // If the update was successful, so do some bookkeeping:
                if success {
                    // Update our commit index based on what the leader has told us, but
                    // not beyond the entries we have received.
                    if leader_commit > self.commit_index {
                        self.commit_index = cmp::min(leader_commit, self.log.len() as Index);
                    }

                    // Update our current term if this is from a newer leader
                    self.current_term = term;
                    self.current_leader = Some(leader);
                }

                self.send_to(
                    peer,
                    &Message::AppendEntriesRep {
                        term: self.current_term,
                        success,
                        next_index: self.log.len() as Index + 1,
                    },
                )
                .await?;

                Ok(())
            }

            Message::AppendEntriesRep {
                term,
                success,
                next_index,
            } => {
                if self.mode != Mode::Leader {
                    // don't care anymore..
                    return Ok(());
                }

                if success {
                    // If the append was successful, then update next_index and match_index accordingly
                    self.next_index[peer] = next_index;
                    self.match_index[peer] = next_index - 1;
                } else {
                    // If the append wasn't successful, select a lower match index for this peer
                    // and try again.  The peer sends the index of the first empty slot in the log,
                    // but we may need to go back further than that, so decrease next_index by at
                    // least one, but stop at 1.
                    self.next_index[peer] =
                        cmp::max(1, cmp::min(self.next_index[peer] - 1, next_index));
                    self.send_append_entries(peer).await?;
                }
                Ok(())
            }
        }
    }

    async fn handle_timer(&mut self, t: Timer) -> Fallible<()> {
        self.log(format!("Handling Timer {:?}", t));
        match t {
            Timer::FollowerUpdate(node_id) => {
                // remove the fired timer from last_append_entries, or DelayQueue will panic
                self.last_append_entries[node_id] = None;
                self.send_append_entries(node_id).await?;
            }
        };
        Ok(())
    }

    /// Handle a control message from the main process, and return true if the task should exit
    async fn handle_control(&mut self, c: Control) -> Fallible<bool> {
        self.log(format!("Handling Control message {:?}", c));
        match c {
            Control::Stop => Ok(true),

            Control::Add(item) => {
                if self.mode != Mode::Leader {
                    // TODO: send a reply referring the caller to the leader..
                    return Ok(false);
                }
                let term = self.current_term;
                let entry = LogEntry::new(term, item);
                let prev_log_index = self.log.len() as Index;
                let prev_log_term = if prev_log_index > 1 {
                    self.log.get(prev_log_index).term
                } else {
                    0
                };

                // append one entry locally (this will always succeed)
                self.log
                    .append_entries(prev_log_index, prev_log_term, vec![entry.clone()])?;

                // then send AppendEntries to all nodes (including ourselves)
                for peer in 0..self.node.network_size() {
                    self.send_append_entries(peer).await?;
                }

                Ok(false)
            }

            #[cfg(test)]
            Control::GetState(mut tx) => {
                let state = ServerState {
                    mode: self.mode,
                    current_term: self.current_term,
                    current_leader: self.current_leader,
                    voted_for: self.voted_for,
                    log: self.log.clone(),
                    commit_index: self.commit_index,
                    last_applied: self.last_applied,
                    next_index: self.next_index.clone(),
                    match_index: self.match_index.clone(),
                };
                tx.send(state).await?;
                Ok(false)
            }
        }
    }

    // utility functions

    async fn send_to(&mut self, peer: NodeId, message: &Message) -> Fallible<()> {
        let msg = serde_json::to_vec(message)?;
        self.node.send(peer, msg).await?;
        Ok(())
    }

    /// Send an AppendEntriesReq to the given peer, and additionally update the timer
    /// so that another (heartbeat) entry is sent soon enough.
    async fn send_append_entries(&mut self, peer: NodeId) -> Fallible<()> {
        let prev_log_index = self.next_index[peer] - 1;
        let prev_log_term = if prev_log_index > 1 {
            self.log.get(prev_log_index).term
        } else {
            0
        };
        let message = Message::AppendEntriesReq {
            term: self.current_term,
            leader: self.node.node_id(),
            prev_log_index,
            prev_log_term,
            entries: self.log.slice(prev_log_index as usize + 1..).to_vec(),
            leader_commit: self.commit_index,
        };
        self.send_to(peer, &message).await?;

        // queue another AppendEntries well before the heartbeat expires
        if let Some(delay_key) = self.last_append_entries[peer].take() {
            self.timers.remove(&delay_key);
        }
        self.last_append_entries[peer] = Some(
            self.timers
                // TODO: something smarter than half the heartbeat..
                .insert(Timer::FollowerUpdate(peer), HEARTBEAT / 2),
        );

        Ok(())
    }

    fn log<S: AsRef<str>>(&self, msg: S) {
        if cfg!(test) && DEBUG {
            println!(
                "server={} mode={:?} - {}",
                self.node.node_id(),
                self.mode,
                msg.as_ref()
            );
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::local::make_network;
    use tokio::time::delay_for;

    #[tokio::test]
    async fn append_entries() -> Fallible<()> {
        let mut net = make_network(2);
        let mut leader = RaftServer::new(net.remove(0));
        let mut follower = RaftServer::new(net.remove(0));

        leader.add('x').await?;
        leader.add('y').await?;

        let state = leader.get_state().await?;
        assert_eq!(state.log.get(1), &LogEntry::new(0, 'x'));
        assert_eq!(state.log.get(2), &LogEntry::new(0, 'y'));

        let state = follower.get_state().await?;
        assert_eq!(state.log.get(1), &LogEntry::new(0, 'x'));
        assert_eq!(state.log.get(2), &LogEntry::new(0, 'y'));

        delay_for(Duration::from_secs(1)).await;

        leader.stop().await;
        follower.stop().await;
        Ok(())
    }
}
