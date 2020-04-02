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

/// Time after which a new election should be called; this should be at least
/// twice HEARTBEAT.
const ELECTION_TIMEOUT: Duration = Duration::from_millis(500);

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
    heartbeat_delay: Vec<Option<delay_queue::Key>>,

    /// Timeout related to elections; when this goes off, start a new election.
    election_timeout: Option<delay_queue::Key>,

    /// Raft-related state of the server
    state: RaftState,

    /// Actions on the server (re-used in place)
    actions: Actions,
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
    GetState(mpsc::Sender<RaftState>),

    /// Set the current log for debugging
    #[cfg(test)]
    SetState(RaftState),
}

/// A Timer is an event that is scheduled at some future time.
#[derive(Debug)]
enum Timer {
    /// This follower may not have gotten an AppendEntriesReq in a while
    Heartbeat(NodeId),

    /// We should start an election
    Election,
}

/// The current mode of the server
#[derive(Debug, PartialEq, Clone, Copy)]
enum Mode {
    Follower,
    Candidate,
    Leader,
}

/// Raft-related state of the server
#[derive(Debug, Clone)]
struct RaftState {
    /// This node
    node_id: NodeId,

    /// Number of nodes in the network
    network_size: usize,

    /// True if the server should stop after this iterataion
    stop: bool,

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

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct AppendEntriesReq {
    term: Term,
    leader: NodeId,
    prev_log_index: Index,
    prev_log_term: Term,
    entries: Vec<LogEntry<char>>,
    leader_commit: Index,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct AppendEntriesRep {
    term: Term,
    next_index: Index,
    success: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct RequestVoteReq {
    term: Term,
    candidate_id: NodeId,
    last_log_index: Index,
    last_log_term: Term,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct RequestVoteRep {
    term: Term,
    vote_granted: bool,
}

/// Messages transferred between Raft nodes
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
enum Message {
    AppendEntriesReq(AppendEntriesReq),
    AppendEntriesRep(AppendEntriesRep),
    RequestVoteReq(RequestVoteReq),
    RequestVoteRep(RequestVoteRep),
}

impl RaftServer {
    pub fn new<N: RaftNetworkNode + Sync + Send + 'static>(node: N) -> RaftServer {
        let (control_tx, control_rx) = mpsc::channel(1);
        let node_id = node.node_id();
        let network_size = node.network_size();
        let inner = RaftServerInner {
            node,
            timers: DelayQueue::new(),
            heartbeat_delay: iter::repeat_with(|| None).take(network_size).collect(),
            election_timeout: None,
            control_rx,
            state: RaftState {
                node_id,
                network_size,
                stop: false,
                mode: Mode::Follower,
                current_term: 0,
                current_leader: None,
                voted_for: None,
                log: RaftLog::new(),
                commit_index: 0,
                last_applied: 0,
                next_index: [1].repeat(network_size),
                match_index: [0].repeat(network_size),
            },
            actions: Actions::new(),
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
    async fn get_state(&mut self) -> Fallible<RaftState> {
        let (log_tx, mut log_rx) = mpsc::channel(1);
        self.control_tx.send(Control::GetState(log_tx)).await?;
        Ok(log_rx.recv().await.unwrap())
    }

    /// Set the current server state (for testing)
    #[cfg(test)]
    async fn set_state(&mut self, state: RaftState) -> Fallible<()> {
        self.control_tx.send(Control::SetState(state)).await?;
        Ok(())
    }
}

impl<N: RaftNetworkNode + Sync + Send + 'static> RaftServerInner<N> {
    // event handling

    async fn run(mut self) {
        #[cfg(test)]
        {
            self.actions.set_log_prefix(format!(
                "server={} mode={:?}",
                self.state.node_id, self.state.mode
            ));
        }

        while !self.state.stop {
            tokio::select! {
                // TODO: is pattern-matching the right idea here??
                Some(c) = self.control_rx.recv() => self.handle_control(c),
                Ok((peer, msg)) = self.node.recv() => self.handle_message(peer, msg),
                Some(t) = self.timers.next() => self.handle_timer(t.unwrap().into_inner()),
            }

            self.execute_actions().await.unwrap();
        }
    }

    fn handle_message(&mut self, peer: NodeId, msg: Vec<u8>) {
        let message: Message = match serde_json::from_slice(&msg[..]) {
            Ok(m) => m,
            Err(e) => {
                self.actions.log(format!("invalid message: {}", e));
                return;
            }
        };

        self.actions
            .log(format!("Handling Message {:?} from {}", message, peer));

        match message {
            Message::AppendEntriesReq(ref message) => {
                handle_append_entries_req(&mut self.state, &mut self.actions, peer, message)
            }
            Message::AppendEntriesRep(ref message) => {
                handle_append_entries_rep(&mut self.state, &mut self.actions, peer, message)
            }
            Message::RequestVoteReq(ref message) => {
                handle_request_vote_req(&mut self.state, &mut self.actions, peer, message)
            }
            Message::RequestVoteRep(ref message) => {
                handle_request_vote_rep(&mut self.state, &mut self.actions, peer, message)
            }
        }
    }

    fn handle_timer(&mut self, t: Timer) {
        self.actions.log(format!("Handling Timer {:?}", t));

        // NOTE: self.timers.remove will panic if called with a key that has already fired.  So
        // we are careful to delete the key before handling any of these timers.
        match t {
            Timer::Heartbeat(node_id) => {
                self.heartbeat_delay[node_id] = None;
                handle_heartbeat_timer(&mut self.state, &mut self.actions, node_id);
            }

            Timer::Election => {
                self.election_timeout = None;
                handle_election_timer(&mut self.state, &mut self.actions);
            }
        };
    }

    /// Handle a control message from the main process, and return true if the task should exit
    fn handle_control(&mut self, c: Control) {
        self.actions
            .log(format!("Handling Control message {:?}", c));

        match c {
            Control::Stop => self.state.stop = true,

            Control::Add(item) => handle_control_add(&mut self.state, &mut self.actions, item),

            #[cfg(test)]
            Control::GetState(tx) => self.actions.send_state(tx),

            #[cfg(test)]
            Control::SetState(state) => self.state = state,
        }
    }

    /// Execute the actions that a handler function specified.
    async fn execute_actions(&mut self) -> Fallible<()> {
        for action in self.actions.drain() {
            match action {
                Action::SetElectionTimer => {
                    if let Some(k) = self.election_timeout.take() {
                        self.timers.remove(&k);
                    }
                    self.election_timeout =
                        Some(self.timers.insert(Timer::Election, ELECTION_TIMEOUT));
                }
                Action::StopElectionTimer => {
                    if let Some(k) = self.election_timeout.take() {
                        self.timers.remove(&k);
                    }
                }
                Action::SetHeartbeatTimer(peer) => {
                    if let Some(delay_key) = self.heartbeat_delay[peer].take() {
                        self.timers.remove(&delay_key);
                    }
                    self.heartbeat_delay[peer] =
                        Some(self.timers.insert(Timer::Heartbeat(peer), HEARTBEAT));
                }
                Action::StopHeartbeatTimers => {
                    for delay in &mut self.heartbeat_delay.iter_mut() {
                        if let Some(k) = delay.take() {
                            self.timers.remove(&k);
                        }
                    }
                }
                Action::SendTo(peer, message) => {
                    let msg = serde_json::to_vec(&message)?;
                    self.node.send(peer, msg).await?;
                }
                #[cfg(test)]
                Action::SendState(mut tx) => {
                    tx.send(self.state.clone()).await?;
                }
            };
        }
        Ok(())
    }
}

/// Actions represent the changes that should be made in response to an algorithm event.
///
/// This abstraction is not necessary to the algorithm, but has a few advantages:
///  - all event handlers are synchronous
///  - side-effects are limited to state changes and actions
///  - event handlers are easy to test
///
/// The struct provides convenience functions to add an action; the RaftServerInner's
/// execute_actions method then actually performs the actions.
struct Actions {
    actions: Vec<Action>,
    #[cfg(test)]
    log_prefix: String,
}

/// See Actions
enum Action {
    /// Start the election_timeout timer (resetting any existing timer)
    SetElectionTimer,

    /// Stop the election_timeout timer.
    StopElectionTimer,

    /// Start the heartbeat timer for the given peer (resetting any existing timer)
    SetHeartbeatTimer(NodeId),

    /// Stop the heartbeat timer for all peers
    StopHeartbeatTimers,

    /// Send a message to a peer
    SendTo(NodeId, Message),

    /// Send current state via a channel
    #[cfg(test)]
    SendState(mpsc::Sender<RaftState>),
}

impl Actions {
    fn new() -> Actions {
        Actions {
            actions: vec![],
            #[cfg(test)]
            log_prefix: String::new(),
        }
    }

    #[cfg(test)]
    fn set_log_prefix(&mut self, log_prefix: String) {
        self.log_prefix = log_prefix;
    }

    #[cfg(not(test))]
    fn set_log_prefix(&mut self, log_prefix: String) {}

    fn drain(&mut self) -> std::vec::Drain<Action> {
        self.actions.drain(..)
    }

    fn set_election_timer(&mut self) {
        self.actions.push(Action::SetElectionTimer);
    }

    fn stop_election_timer(&mut self) {
        self.actions.push(Action::StopElectionTimer);
    }

    fn set_heartbeat_timer(&mut self, peer: NodeId) {
        self.actions.push(Action::SetHeartbeatTimer(peer));
    }

    fn stop_heartbeat_timers(&mut self) {
        self.actions.push(Action::StopHeartbeatTimers);
    }

    fn send_to(&mut self, peer: NodeId, message: Message) {
        self.actions.push(Action::SendTo(peer, message));
    }

    #[cfg(test)]
    fn send_state(&mut self, tx: mpsc::Sender<RaftState>) {
        self.actions.push(Action::SendState(tx));
    }

    /// Not quite an "action", but actions.log will output debug logging (immediately) on
    /// debug builds when DEBUG is set to true.
    #[cfg(test)]
    fn log<S: AsRef<str>>(&self, msg: S) {
        if DEBUG {
            println!("{} - {}", self.log_prefix, msg.as_ref());
        }
    }
    #[cfg(not(test))]
    fn log<S: AsRef<str>>(&self, msg: S) {}
}

//
// Event handlers
//

fn handle_control_add(state: &mut RaftState, actions: &mut Actions, item: char) {
    if state.mode != Mode::Leader {
        // TODO: send a reply referring the caller to the leader..
        return;
    }
    let term = state.current_term;
    let entry = LogEntry::new(term, item);
    let prev_log_index = state.log.len() as Index;
    let prev_log_term = if prev_log_index > 1 {
        state.log.get(prev_log_index).term
    } else {
        0
    };

    // append one entry locally (this will always succeed)
    state
        .log
        .append_entries(prev_log_index, prev_log_term, vec![entry.clone()])
        .unwrap();

    // then send AppendEntries to all nodes (including ourselves)
    for peer in 0..state.network_size {
        send_append_entries(state, actions, peer);
    }
}

fn handle_append_entries_req(
    state: &mut RaftState,
    actions: &mut Actions,
    peer: NodeId,
    message: &AppendEntriesReq,
) {
    if state.mode == Mode::Leader {
        // leaders don't respond to this message
        return;
    }

    // If we're a follower, then reset the election timeout, as we have just
    // heard from a real, live leader
    if state.mode == Mode::Follower {
        actions.set_election_timer();
    }

    // Reject this request if term < our current_term
    let mut success = message.term >= state.current_term;
    if !success {
        actions.log("Rejecting AppendEntries: term too old");
    }

    // Reject this request if the log does not apply cleanly
    if success {
        success = match state.log.append_entries(
            message.prev_log_index,
            message.prev_log_term,
            // TODO: take ref?
            message.entries.clone(),
        ) {
            Ok(()) => true,
            Err(e) => {
                actions.log(format!("Rejecting AppendEntries: {}", e));
                false
            }
        };
    }

    // If the update was successful, so do some bookkeeping:
    if success {
        // TODO: test
        if state.mode == Mode::Candidate {
            // we lost the elction, so transition back to a follower
            change_mode(state, actions, Mode::Follower);
        }

        // Update our commit index based on what the leader has told us, but
        // not beyond the entries we have received.
        if message.leader_commit > state.commit_index {
            state.commit_index = cmp::min(message.leader_commit, state.log.len() as Index);
        }

        // Update our current term if this is from a newer leader
        state.current_term = message.term;
        state.current_leader = Some(message.leader);
    }

    actions.send_to(
        peer,
        Message::AppendEntriesRep(AppendEntriesRep {
            term: state.current_term,
            success,
            next_index: state.log.len() as Index + 1,
        }),
    )
}

fn handle_append_entries_rep(
    state: &mut RaftState,
    actions: &mut Actions,
    peer: NodeId,
    message: &AppendEntriesRep,
) {
    if state.mode != Mode::Leader {
        // if we're no longer a leader, there's nothing to do with this response
        return;
    }

    if message.success {
        // If the append was successful, then update next_index and match_index accordingly
        state.next_index[peer] = message.next_index;
        state.match_index[peer] = message.next_index - 1;
    } else {
        if message.term > state.current_term {
            // If the append wasn't successful because another leader has been elected,
            // then transition back to follower state (but don't update current_term
            // yet - that will happen when we get an AppendEntries as a follower)
            // TODO: test
            change_mode(state, actions, Mode::Follower);
        } else {
            // If the append wasn't successful because of a log conflict, select a lower match index for this peer
            // and try again.  The peer sends the index of the first empty slot in the log,
            // but we may need to go back further than that, so decrease next_index by at
            // least one, but stop at 1.
            state.next_index[peer] =
                cmp::max(1, cmp::min(state.next_index[peer] - 1, message.next_index));
            send_append_entries(state, actions, peer);
        }
    }
}

fn handle_request_vote_req(
    state: &mut RaftState,
    actions: &mut Actions,
    peer: NodeId,
    message: &RequestVoteReq,
) {
    let mut vote_granted = true;

    // "Reply false if term < currentTerm"
    if message.term < state.current_term {
        vote_granted = false;
    }

    // "If votedFor is null or canidateId .."
    if vote_granted {
        if let Some(node_id) = state.voted_for {
            if message.candidate_id != node_id {
                vote_granted = false;
            }
        }
    }

    // ".. and candidates's log is at least as up-to-date as receiver's log"
    // §5.4.1: "Raft determines which of two logs is more up-to-date by comparing
    // the index and term of the last entries in the logs.  If the logs have last
    // entries with differen terms, then the log with the later term is more
    // up-to-date.  If the logs end with the same term, then whichever log is longer is
    // more up-to-date."
    if vote_granted {
        // TODO: might not have any entries
        let state_last_log_index = state.log.len() as Index;
        let receiver_last_log_term = state.log.get(state_last_log_index).term;
        if message.last_log_term < receiver_last_log_term {
            vote_granted = false;
        } else if message.last_log_term == receiver_last_log_term {
            if message.last_log_index < state_last_log_index {
                vote_granted = false;
            }
        }
    }

    actions.send_to(
        peer,
        Message::RequestVoteRep(RequestVoteRep {
            term: state.current_term,
            vote_granted,
        }),
    );
}

fn handle_request_vote_rep(
    state: &mut RaftState,
    actions: &mut Actions,
    peer: NodeId,
    message: &RequestVoteRep,
) {
    // TODO!!
    // need to track number of votes for us in state
}

fn handle_heartbeat_timer(state: &mut RaftState, actions: &mut Actions, peer: NodeId) {
    send_append_entries(state, actions, peer);
    actions.set_heartbeat_timer(peer);
}

fn handle_election_timer(state: &mut RaftState, actions: &mut Actions) {
    match state.mode {
        Mode::Follower => {
            change_mode(state, actions, Mode::Candidate);
        }
        Mode::Candidate => {
            start_election(state, actions);
        }
        Mode::Leader => unreachable!(),
    }
}

//
// Utility functions
//

fn send_append_entries(state: &mut RaftState, actions: &mut Actions, peer: NodeId) {
    let prev_log_index = state.next_index[peer] - 1;
    let prev_log_term = if prev_log_index > 1 {
        state.log.get(prev_log_index).term
    } else {
        0
    };
    let message = Message::AppendEntriesReq(AppendEntriesReq {
        term: state.current_term,
        leader: state.node_id,
        prev_log_index,
        prev_log_term,
        entries: state.log.slice(prev_log_index as usize + 1..).to_vec(),
        leader_commit: state.commit_index,
    });
    actions.send_to(peer, message);

    // set the timeout so we send another AppendEntries soon
    actions.set_heartbeat_timer(peer);
}

/// Change to a new mode, taking care of any necessary state maintenance
fn change_mode(state: &mut RaftState, actions: &mut Actions, new_mode: Mode) {
    actions.log(format!("Transitioning to mode {:?}", new_mode));

    let old_mode = state.mode;
    assert!(old_mode != new_mode);
    state.mode = new_mode;

    // shut down anything running for the old mode..
    match old_mode {
        Mode::Follower => {
            actions.stop_election_timer();
        }
        Mode::Candidate => {
            actions.stop_election_timer();
        }
        Mode::Leader => {
            actions.stop_heartbeat_timers();
        }
    };

    // .. and set up for the new mode
    match new_mode {
        Mode::Follower => {
            actions.set_election_timer();
        }
        Mode::Candidate => {
            start_election(state, actions);
        }
        Mode::Leader => {
            state.current_leader = Some(state.node_id);

            // re-initialize state tracking other nodes' logs
            for peer in 0..state.network_size {
                state.next_index[peer] = state.log.len() as Index + 1;
                state.match_index[peer] = 0;
            }

            // assert leadership by sending AppendEntriesReq to everyone
            for peer in 0..state.network_size {
                send_append_entries(state, actions, peer);
            }
        }
    };
}

/// Start a new election, including incrementing term, sending the necessary mesages, and
/// starting the election timer.
fn start_election(state: &mut RaftState, actions: &mut Actions) {
    assert!(state.mode == Mode::Candidate);

    let node_id = state.node_id;
    state.current_term += 1;
    state.voted_for = Some(node_id);

    for peer in 0..state.network_size {
        let message = Message::RequestVoteReq(RequestVoteReq {
            term: state.current_term,
            candidate_id: node_id,
            // TODO: might have no log entries - do this in a utility function?
            last_log_index: state.log.len() as Index,
            last_log_term: state.log.get(state.log.len() as Index).term,
        });
        actions.send_to(peer, message);
    }

    actions.set_election_timer();
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::local::{LocalNetwork, LocalNode};
    use tokio::time::delay_for;

    /// Creat a two node network, with a server on node 0 and a bare LocalNode for node 1
    fn two_node_network() -> (RaftServer, LocalNode) {
        let mut net = LocalNetwork::new(2);
        let server = RaftServer::new(net.take(0));
        let node = net.take(1);
        (server, node)
    }

    /// Update the state of the given server
    async fn update_state(server: &mut RaftServer, modifier: fn(&mut RaftState)) -> Fallible<()> {
        let mut state = server.get_state().await?;
        modifier(&mut state);
        server.set_state(state).await?;

        // since control and messages are on different mpsc channels, sleep a bit to make sure that
        // the control message arrives first..
        beat().await;

        Ok(())
    }

    /// Receive a message on behalf of the given node
    async fn recv_on_node(node: &mut LocalNode) -> Fallible<(NodeId, Message)> {
        let (node_id, msg) = node.recv().await?;
        let message: Message = serde_json::from_slice(&msg[..])?;
        Ok((node_id, message))
    }

    /// Send a emssage from the given node to the given node
    async fn send_from_node(node: &mut LocalNode, peer: NodeId, message: Message) -> Fallible<()> {
        let msg = serde_json::to_vec(&message)?;
        node.send(peer, msg).await?;
        Ok(())
    }

    /// Wait long enough for all of the mpsc channels to drain..
    async fn beat() {
        delay_for(Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn test_leader_add() -> Fallible<()> {
        let (mut leader, mut follower_node) = two_node_network();

        update_state(&mut leader, |state| state.mode = Mode::Leader).await?;

        // make a client call to add an entry
        leader.add('x').await?;

        // leader should send an AppendEntriesReq message to followers..
        let (_, message) = recv_on_node(&mut follower_node).await?;
        assert_eq!(
            message,
            Message::AppendEntriesReq(AppendEntriesReq {
                term: 0,
                leader: 0,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry::new(0, 'x')],
                leader_commit: 0
            })
        );

        // ..and update its own state
        let state = leader.get_state().await?;
        assert_eq!(state.log.get(1), &LogEntry::new(0, 'x'));

        leader.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_follower_replicate_success() -> Fallible<()> {
        let (mut follower, mut leader_node) = two_node_network();

        // build a state with some entries already in place
        update_state(&mut follower, |state| {
            state.mode = Mode::Follower;
            state.current_term = 5;
            let entries = vec![
                LogEntry::new(1, 'a'),
                LogEntry::new(3, 'b'), // <-- commit_index
                LogEntry::new(5, 'c'),
            ];
            state.log = RaftLog::new();
            state.log.append_entries(0, 0, entries).unwrap();
            state.commit_index = 2;
        })
        .await?;

        // simulate an incoming message from the leader
        send_from_node(
            &mut leader_node,
            0,
            Message::AppendEntriesReq(AppendEntriesReq {
                term: 6,
                leader: 1,
                prev_log_index: 3,
                prev_log_term: 5,
                entries: vec![LogEntry::new(5, 'x'), LogEntry::new(6, 'y')],
                leader_commit: 3,
            }),
        )
        .await?;

        // ..get the reply
        let (_, message) = recv_on_node(&mut leader_node).await?;
        assert_eq!(
            message,
            Message::AppendEntriesRep(AppendEntriesRep {
                term: 6,
                next_index: 6,
                success: true,
            })
        );

        // ..and check the state
        let state = follower.get_state().await?;
        //println!("state: {:#?}", state);
        //println!("state.log.len: {:#?}", state.log.len());
        assert_eq!(state.log.len(), 5);
        assert_eq!(state.log.get(4), &LogEntry::new(5, 'x'));
        assert_eq!(state.log.get(5), &LogEntry::new(6, 'y'));
        assert_eq!(state.commit_index, 3);
        assert_eq!(state.current_term, 6);
        assert_eq!(state.current_leader, Some(1));

        follower.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_follower_replicate_bad_term() -> Fallible<()> {
        let (mut follower, mut leader_node) = two_node_network();

        // build a state with some entries already in place
        update_state(&mut follower, |state| {
            state.mode = Mode::Follower;
            state.current_term = 5;
            let entries = vec![LogEntry::new(1, 'a')];
            state.log = RaftLog::new();
            state.log.append_entries(0, 0, entries).unwrap();
            state.commit_index = 2;
        })
        .await?;

        // simulate an incoming message from the leader
        send_from_node(
            &mut leader_node,
            0,
            Message::AppendEntriesReq(AppendEntriesReq {
                term: 3, // too early
                leader: 1,
                prev_log_index: 3,
                prev_log_term: 5,
                entries: vec![LogEntry::new(5, 'x')],
                leader_commit: 3,
            }),
        )
        .await?;

        // ..get the reply
        let (_, message) = recv_on_node(&mut leader_node).await?;
        assert_eq!(
            message,
            Message::AppendEntriesRep(AppendEntriesRep {
                term: 5,
                next_index: 2,
                success: false,
            })
        );

        // ..and check the state hasn't changed
        let state = follower.get_state().await?;
        assert_eq!(state.log.len(), 1);
        assert_eq!(state.commit_index, 2);
        assert_eq!(state.current_term, 5);
        assert_eq!(state.current_leader, None);

        follower.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_follower_replicate_log_mismatch() -> Fallible<()> {
        let (mut follower, mut leader_node) = two_node_network();

        // build a state with some entries already in place
        update_state(&mut follower, |state| {
            state.mode = Mode::Follower;
            state.current_term = 5;
            let entries = vec![LogEntry::new(1, 'a'), LogEntry::new(4, 'p')];
            state.log = RaftLog::new();
            state.log.append_entries(0, 0, entries).unwrap();
            state.commit_index = 2;
        })
        .await?;

        // simulate an incoming message from the leader
        send_from_node(
            &mut leader_node,
            0,
            Message::AppendEntriesReq(AppendEntriesReq {
                term: 5,
                leader: 1,
                prev_log_index: 2,
                prev_log_term: 5, // does not match (4, p)
                entries: vec![LogEntry::new(5, 'x')],
                leader_commit: 3,
            }),
        )
        .await?;

        // ..get the reply
        let (_, message) = recv_on_node(&mut leader_node).await?;
        assert_eq!(
            message,
            Message::AppendEntriesRep(AppendEntriesRep {
                term: 5,
                next_index: 3,
                success: false,
            })
        );

        // ..and check the state hasn't changed
        let state = follower.get_state().await?;
        assert_eq!(state.log.len(), 2);
        assert_eq!(state.commit_index, 2);
        assert_eq!(state.current_term, 5);
        assert_eq!(state.current_leader, None);

        follower.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_leader_apply_entries_rep_success() -> Fallible<()> {
        let (mut leader, mut follower_node) = two_node_network();

        update_state(&mut leader, |state| state.mode = Mode::Leader).await?;

        // simulate an incoming message from the follower
        send_from_node(
            &mut follower_node,
            0,
            Message::AppendEntriesRep(AppendEntriesRep {
                term: 5,
                next_index: 3,
                success: true,
            }),
        )
        .await?;

        beat().await;

        // and see that it updates it state
        let state = leader.get_state().await?;
        // TODO: assert_eq!(state.current_term, 5); // copied from follower
        assert_eq!(state.next_index[1], 3);
        assert_eq!(state.match_index[1], 2);

        leader.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_leader_apply_entries_rep_failure() -> Fallible<()> {
        let (mut leader, mut follower_node) = two_node_network();

        update_state(&mut leader, |state| {
            state.mode = Mode::Leader;
            let entries = vec![
                LogEntry::new(1, 'a'),
                LogEntry::new(3, 'b'),
                LogEntry::new(5, 'c'),
                LogEntry::new(5, 'd'),
                LogEntry::new(5, 'e'),
            ];
            state.log = RaftLog::new();
            state.log.append_entries(0, 0, entries).unwrap();
            state.current_term = 5;
            state.match_index[1] = 1;
            state.next_index[1] = 5;
        })
        .await?;

        // simulate an incoming message from the follower indicating that
        // it needs more log entries
        send_from_node(
            &mut follower_node,
            0,
            Message::AppendEntriesRep(AppendEntriesRep {
                term: 5,
                next_index: 3,
                success: false,
            }),
        )
        .await?;

        // leader should send an AppendEntriesReq message to followers..
        let (_, message) = recv_on_node(&mut follower_node).await?;
        assert_eq!(
            message,
            Message::AppendEntriesReq(AppendEntriesReq {
                term: 5,
                leader: 0,
                prev_log_index: 2,
                prev_log_term: 3,
                entries: vec![
                    LogEntry::new(5, 'c'),
                    LogEntry::new(5, 'd'),
                    LogEntry::new(5, 'e'),
                ],
                leader_commit: 0
            })
        );

        // and see that it updates it state
        let state = leader.get_state().await?;
        assert_eq!(state.current_term, 5);
        assert_eq!(state.next_index[1], 3);
        assert_eq!(state.match_index[1], 1);

        leader.stop().await;
        Ok(())
    }

    /*
    #[tokio::test] TODO once we have client responses..
    async fn replicate_client_call() -> Fallible<()> {
        let mut net = LocalNetwork::new(2);
        let mut leader = RaftServer::new(net.take(0));
        let mut follower = RaftServer::new(net.take(1));

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
    */
}
