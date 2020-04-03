use crate::diststate::{self, DistributedState, Request};
use crate::log::{LogEntry, RaftLog};
use crate::net::{NodeId, RaftNetworkNode};
use crate::{Index, Term};
use failure::Fallible;
use rand::{thread_rng, Rng};
use serde_json::{self, json};
use std::cmp;
use std::iter;
use std::time::Duration;
use tokio::stream::StreamExt;
use tokio::sync::mpsc;
use tokio::task;
use tokio::time::{delay_queue, DelayQueue};

#[cfg(test)]
use std::time::SystemTime;

/// Set this to true to enable lots of println!
const DEBUG: bool = true;

/// Time after which a new election should be called; this should be well over
/// the maximum RTT between two nodes, and well under the servers' MTBF.
const ELECTION_TIMEOUT: Duration = Duration::from_millis(500);

/// Range of random times around ELECTION_TIMEOUT in which to call an election.  This
/// must be smaller than ELECTION_TIMEOUT - HEARTBEAT.
const ELECTION_TIMEOUT_FUZZ: Duration = Duration::from_millis(100);

/// Maximum time between AppendEntries calls.  This should be at least an RTT less than
/// ELECTION_TIMEOUT.
const HEARTBEAT: Duration = Duration::from_millis(200);

/// A RaftServer represents a running server participating in a Raft.
#[derive(Debug)]
pub struct RaftServer<DS>
where
    DS: DistributedState,
{
    /// The background task receiving messages for this server
    task: task::JoinHandle<()>,

    /// A channel to send control messages to the background task
    control_tx: mpsc::Sender<Control<DS>>,

    /// A channel to receive control messages from the background task
    control_rx: mpsc::Receiver<Control<DS>>,
}

/* Most of the work of a server occurs in a background task, reacting to messages and timers.  In
 * fact, all of the work occurs in the background to simplify ownership of the data structures, and
 * all communication occurs control_tx / control_rx.  Public methods simply send control messages
 * to the background task.  In cases where a reply is required, the control message contains a
 * transient channel to carry the response.
 */

pub struct RaftServerInner<NODE, DS>
where
    NODE: RaftNetworkNode + Sync + Send + 'static,
    DS: DistributedState,
{
    /*
     * Mechanics
     */
    /// The network node, used for communication
    node: NODE,

    /// Channel containing control messages from other tasks
    control_rx: mpsc::Receiver<Control<DS>>,

    /// Channel for sending control information back to other tasks
    control_tx: mpsc::Sender<Control<DS>>,

    /// A queue of Timer objects
    timers: DelayQueue<Timer>,

    /// DelayQueue keys for the last time AppendEntries was sent to this peer
    heartbeat_delay: Vec<Option<delay_queue::Key>>,

    /// Timeout related to elections; when this goes off, start a new election.
    election_timeout: Option<delay_queue::Key>,

    /// Raft-related state of the server
    state: RaftState<DS>,

    /// Actions on the server (re-used in place)
    actions: Actions<DS>,
}

/// Control messages sent between RaftServer and RaftServerInner
#[derive(Debug, PartialEq)]
enum Control<DS>
where
    DS: DistributedState,
{
    /// (server -> inner) Stop the task
    Stop,

    /// (server -> inner) Client Request to the state machine
    Request(DS::Request),

    /// (server -> inner) Send a SetState back containing the current state
    #[cfg(test)]
    GetState,

    /// (server -> inner) Set the current raft state;
    /// (inner -> server) reply to GetState
    #[cfg(test)]
    SetState(RaftState<DS>),
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
#[derive(Debug, Clone, PartialEq)]
struct RaftState<DS>
where
    DS: DistributedState,
{
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
    log: RaftLog<LogItem<DS::Request>>,

    /// Index of the highest log entry known to be committed
    commit_index: Index,

    /// Index of the highest log entry applied to state machine
    last_applied: Index,

    /// "for each server, index of the next log entry to send to that server"
    next_index: Vec<Index>,

    /// "for each server, index of the highest log entry known to be replicated on server"
    match_index: Vec<Index>,

    /// true for all nodes that have voted for this node as leader
    voters: Vec<bool>,
}

#[derive(Debug, PartialEq, Default)]
struct AppendEntriesReq<DS>
where
    DS: DistributedState,
{
    term: Term,
    leader: NodeId,
    prev_log_index: Index,
    prev_log_term: Term,
    entries: Vec<LogEntry<LogItem<DS::Request>>>,
    leader_commit: Index,
}

#[derive(Debug, PartialEq, Default)]
struct AppendEntriesRep {
    term: Term,
    next_index: Index,
    success: bool,
}

#[derive(Debug, PartialEq, Default)]
struct RequestVoteReq {
    term: Term,
    candidate_id: NodeId,
    last_log_index: Index,
    last_log_term: Term,
}

#[derive(Debug, PartialEq, Default)]
struct RequestVoteRep {
    term: Term,
    vote_granted: bool,
}

/// Messages transferred between Raft nodes
#[derive(Debug, PartialEq)]
enum Message<DS>
where
    DS: DistributedState,
{
    AppendEntriesReq(AppendEntriesReq<DS>),
    AppendEntriesRep(AppendEntriesRep),
    RequestVoteReq(RequestVoteReq),
    RequestVoteRep(RequestVoteRep),
}

impl<DS> Message<DS>
where
    DS: DistributedState,
{
    fn serialize(&self) -> Vec<u8> {
        let value = match *self {
            Message::AppendEntriesReq(ref v) => {
                let entries: Vec<serde_json::Value> = v
                    .entries
                    .iter()
                    .map(|e| {
                        json!({
                            "term": e.term,
                            "item_req":e.item.req.serialize(),
                        })
                    })
                    .collect();

                json!({
                    "type": "AppendEntriesReq",
                    "term": v.term,
                    "leader": v.leader,
                    "prev_log_index": v.prev_log_index,
                    "prev_log_term": v.prev_log_term,
                    "entries": entries,
                    "leader_commit": v.leader_commit,
                })
            }
            Message::AppendEntriesRep(ref v) => json!( {
                "type": "AppendEntriesRep",
                "term": v.term,
                "next_index": v.next_index,
                "success": v.success,
            }),
            Message::RequestVoteReq(ref v) => json!({
                "type": "RequestVoteReq",
                "term": v.term,
                "candidate_id": v.candidate_id,
                "last_log_index": v.last_log_index,
                "last_log_term": v.last_log_term,
            }),
            Message::RequestVoteRep(ref v) => json!({
                "type": "RequestVoteRep",
                "term": v.term,
                "vote_granted": v.vote_granted,
            }),
        };
        // TODO: better way??
        value.to_string().as_bytes().to_vec()
    }

    fn deserialize(ser: &[u8]) -> Self {
        let value: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(ser).unwrap()).unwrap();
        let typ = value.get("type").unwrap().as_str().unwrap();
        match typ {
            "AppendEntriesReq" => {
                let entries = value
                    .get("entries")
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|e| {
                        let term = e.get("term").unwrap().as_u64().unwrap();
                        let item_req = e.get("item_req").unwrap();
                        LogEntry {
                            term,
                            item: LogItem {
                                req: <DS as diststate::DistributedState>::Request::deserialize(
                                    item_req,
                                ),
                            },
                        }
                    })
                    .collect();
                Message::AppendEntriesReq(AppendEntriesReq {
                    term: value.get("term").unwrap().as_u64().unwrap(),
                    leader: value.get("leader").unwrap().as_u64().unwrap() as usize,
                    prev_log_index: value.get("prev_log_index").unwrap().as_u64().unwrap(),
                    prev_log_term: value.get("prev_log_term").unwrap().as_u64().unwrap(),
                    entries,
                    leader_commit: value.get("leader_commit").unwrap().as_u64().unwrap(),
                })
            }
            "AppendEntriesRep" => Message::AppendEntriesRep(AppendEntriesRep {
                term: value.get("term").unwrap().as_u64().unwrap(),
                next_index: value.get("next_index").unwrap().as_u64().unwrap(),
                success: value.get("success").unwrap().as_bool().unwrap(),
            }),
            "RequestVoteReq" => Message::RequestVoteReq(RequestVoteReq {
                term: value.get("term").unwrap().as_u64().unwrap(),
                candidate_id: value.get("candidate_id").unwrap().as_u64().unwrap() as usize,
                last_log_index: value.get("last_log_index").unwrap().as_u64().unwrap(),
                last_log_term: value.get("last_log_term").unwrap().as_u64().unwrap(),
            }),
            "RequestVoteRep" => Message::RequestVoteRep(RequestVoteRep {
                term: value.get("term").unwrap().as_u64().unwrap(),
                vote_granted: value.get("vote_granted").unwrap().as_bool().unwrap(),
            }),
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LogItem<R>
where
    R: diststate::Request,
{
    req: R,
}

impl<DS> RaftServer<DS>
where
    DS: DistributedState + Clone,
{
    pub fn new<NODE>(node: NODE) -> RaftServer<DS>
    where
        NODE: RaftNetworkNode + Sync + Send + 'static,
    {
        let (control_tx_in, control_rx_in) = mpsc::channel(1);
        let (control_tx_out, control_rx_out) = mpsc::channel(1);
        let node_id = node.node_id();
        let network_size = node.network_size();
        let inner = RaftServerInner {
            node,
            timers: DelayQueue::new(),
            heartbeat_delay: iter::repeat_with(|| None).take(network_size).collect(),
            election_timeout: None,
            control_rx: control_rx_in,
            control_tx: control_tx_out,
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
                voters: [false].repeat(network_size),
            },
            actions: Actions::new(),
        };

        RaftServer {
            task: tokio::spawn(async move { inner.run().await }),
            control_tx: control_tx_in,
            control_rx: control_rx_out,
        }
    }

    /// Stop the server
    pub async fn stop(mut self) {
        self.control_tx.send(Control::Stop).await.unwrap();
        self.task.await.unwrap();
    }

    /// Make a request of the distributed state machine
    pub async fn request(&mut self, req: DS::Request) -> Fallible<()> {
        Ok(self.control_tx.send(Control::Request(req)).await?)
    }

    /// Get a copy of the current server state (for testing)
    #[cfg(test)]
    async fn get_state(&mut self) -> Fallible<RaftState<DS>> {
        self.control_tx.send(Control::GetState).await?;
        if let Control::SetState(state) = self.control_rx.recv().await.unwrap() {
            return Ok(state);
        } else {
            panic!("got unexpected control message");
        }
    }

    /// Set the current server state (for testing)
    #[cfg(test)]
    async fn set_state(&mut self, state: RaftState<DS>) -> Fallible<()> {
        self.control_tx.send(Control::SetState(state)).await?;
        Ok(())
    }
}

impl<NODE, DS> RaftServerInner<NODE, DS>
where
    NODE: RaftNetworkNode + Sync + Send + 'static,
    DS: DistributedState,
{
    // event handling

    async fn run(mut self) {
        #[cfg(test)]
        {
            self.actions
                .set_log_prefix(format!("server={}", self.state.node_id,));
        }

        // start things up..
        self.startup();
        self.execute_actions().await.unwrap();

        while !self.state.stop {
            #[cfg(test)]
            {
                self.actions.set_log_prefix(format!(
                    "server={} ({:?})",
                    self.state.node_id, self.state.mode
                ));
            }

            tokio::select! {
                // TODO: is pattern-matching the right idea here??
                Some(c) = self.control_rx.recv() => self.handle_control(c),
                Ok((peer, msg)) = self.node.recv() => self.handle_message(peer, msg),
                Some(t) = self.timers.next() => self.handle_timer(t.unwrap().into_inner()),
            }

            // execute any actions accumulated in this loop
            self.execute_actions().await.unwrap();
        }
    }

    fn startup(&mut self) {
        // on startup, set the election timer, so that we either learn about an existing leader
        // or try to become a leader
        self.actions.set_election_timer();
    }

    fn handle_message(&mut self, peer: NodeId, msg: Vec<u8>) {
        let message: Message<DS> = Message::deserialize(&msg);

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

        // self.timers.remove will panic if called with a key that has already fired, so we are
        // careful to delete the key before handling any of these timers.
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
    fn handle_control(&mut self, c: Control<DS>) {
        self.actions
            .log(format!("Handling Control message {:?}", c));

        match c {
            Control::Stop => self.state.stop = true,

            Control::Request(req) => handle_control_add(&mut self.state, &mut self.actions, req),

            #[cfg(test)]
            Control::GetState => handle_control_get_state(&mut self.state, &mut self.actions),

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
                    // select a time within ELECTION_TIMEOUT += ELECTION_TIMEOUT_FUZZ
                    let election_timeout = ELECTION_TIMEOUT.as_millis() as u64;
                    let fuzz = ELECTION_TIMEOUT_FUZZ.as_millis() as u64;
                    let mut rng = thread_rng();
                    let millis = rng.gen_range(election_timeout - fuzz, election_timeout + fuzz);
                    self.election_timeout = Some(
                        self.timers
                            .insert(Timer::Election, Duration::from_millis(millis)),
                    );
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
                Action::ApplyIndex(index) => {
                    // TODO
                    println!("Apply {:?}", self.state.log.get(index));
                }
                Action::SendTo(peer, message) => {
                    let msg = message.serialize();
                    self.node.send(peer, msg).await?;
                }
                Action::SendControl(control) => {
                    self.control_tx.send(control).await?;
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
#[derive(Debug, PartialEq)]
struct Actions<DS>
where
    DS: DistributedState,
{
    actions: Vec<Action<DS>>,
    #[cfg(test)]
    log_prefix: String,
}

/// See Actions
#[derive(Debug, PartialEq)]
enum Action<DS>
where
    DS: DistributedState,
{
    /// Start the election_timeout timer (resetting any existing timer)
    SetElectionTimer,

    /// Stop the election_timeout timer.
    StopElectionTimer,

    /// Start the heartbeat timer for the given peer (resetting any existing timer)
    SetHeartbeatTimer(NodeId),

    /// Stop the heartbeat timer for all peers
    StopHeartbeatTimers,

    /// Apply the entry at the given index to the state machine
    ApplyIndex(Index),

    /// Send a message to a peer
    SendTo(NodeId, Message<DS>),

    /// Send a message on the control channel
    SendControl(Control<DS>),
}

impl<DS> Actions<DS>
where
    DS: DistributedState,
{
    fn new() -> Actions<DS> {
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

    fn drain(&mut self) -> std::vec::Drain<Action<DS>> {
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

    fn apply_index(&mut self, index: Index) {
        self.actions.push(Action::ApplyIndex(index));
    }

    fn send_to(&mut self, peer: NodeId, message: Message<DS>) {
        self.actions.push(Action::SendTo(peer, message));
    }

    fn send_control(&mut self, control: Control<DS>) {
        self.actions.push(Action::SendControl(control));
    }

    /// Not quite an "action", but actions.log will output debug logging (immediately) on
    /// debug builds when DEBUG is set to true.
    #[cfg(test)]
    fn log<S: AsRef<str>>(&self, msg: S) {
        if DEBUG {
            let millis = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            println!("{}: {} - {}", millis, self.log_prefix, msg.as_ref());
        }
    }
    #[cfg(not(test))]
    fn log<S: AsRef<str>>(&self, msg: S) {}
}

//
// Event handlers
//

fn handle_control_add<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>, req: DS::Request)
where
    DS: DistributedState,
{
    if state.mode != Mode::Leader {
        // TODO: send a reply referring the caller to the leader..
        return;
    }
    let entry = LogEntry::new(state.current_term, LogItem { req });
    let (prev_log_index, prev_log_term) = prev_log_info(state, state.log.next_index());

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

#[cfg(test)]
fn handle_control_get_state<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>)
where
    DS: DistributedState,
{
    actions.send_control(Control::SetState(state.clone()));
}

fn handle_append_entries_req<DS>(
    state: &mut RaftState<DS>,
    actions: &mut Actions<DS>,
    peer: NodeId,
    message: &AppendEntriesReq<DS>,
) where
    DS: DistributedState,
{
    update_current_term(state, actions, message.term);

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
            &message.entries[..],
        ) {
            Ok(()) => true,
            Err(e) => {
                actions.log(format!("Rejecting AppendEntries: {}", e));
                false
            }
        };
    }

    // If the update was successful, do some bookkeeping:
    if success {
        if state.mode == Mode::Candidate {
            // we lost the elction, so transition back to a follower
            change_mode(state, actions, Mode::Follower);
        }

        // Update our commit index based on what the leader has told us, but
        // not beyond the entries we have received.
        if message.leader_commit > state.commit_index {
            state.commit_index =
                cmp::min(message.leader_commit, state.log.last_index().unwrap_or(0));
            update_commitment(state, actions);
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
            next_index: state.log.next_index(),
        }),
    )
}

fn handle_append_entries_rep<DS>(
    state: &mut RaftState<DS>,
    actions: &mut Actions<DS>,
    peer: NodeId,
    message: &AppendEntriesRep,
) where
    DS: DistributedState,
{
    update_current_term(state, actions, message.term);

    if state.mode != Mode::Leader {
        // if we're no longer a leader, there's nothing to do with this response
        return;
    }

    if message.success {
        // If the append was successful, then update next_index and match_index accordingly
        state.next_index[peer] = message.next_index;
        state.match_index[peer] = message.next_index - 1;
        update_commitment(state, actions);
    } else {
        // If the append wasn't successful because of a log conflict (and we are still leader),
        // select a lower match index for this peer and try again.  The peer sends the index of the
        // first empty slot in the log, but we may need to go back further than that, so decrease
        // next_index by at least one, but stop at 1.
        state.next_index[peer] =
            cmp::max(1, cmp::min(state.next_index[peer] - 1, message.next_index));
        send_append_entries(state, actions, peer);
    }
}

fn handle_request_vote_req<DS>(
    state: &mut RaftState<DS>,
    actions: &mut Actions<DS>,
    peer: NodeId,
    message: &RequestVoteReq,
) where
    DS: DistributedState,
{
    update_current_term(state, actions, message.term);

    // TODO: test
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
        let (last_log_index, last_log_term) = prev_log_info(state, state.log.next_index());
        if message.last_log_term < last_log_term {
            vote_granted = false;
        } else if message.last_log_term == last_log_term {
            if message.last_log_index < last_log_index {
                vote_granted = false;
            }
        }
    }

    actions.send_to(
        peer,
        Message::RequestVoteRep(RequestVoteRep {
            term: message.term,
            vote_granted,
        }),
    );
}

fn handle_request_vote_rep<DS>(
    state: &mut RaftState<DS>,
    actions: &mut Actions<DS>,
    peer: NodeId,
    message: &RequestVoteRep,
) where
    DS: DistributedState,
{
    update_current_term(state, actions, message.term);

    // TODO: test
    if state.mode != Mode::Candidate {
        // thank you for your vote .. but I'm not running!
        return;
    }

    if message.term < state.current_term {
        // message was for an old term
        return;
    }

    if message.vote_granted {
        state.voters[peer] = true;

        // have we won?
        let voters = state.voters.iter().filter(|&v| *v).count();
        actions.log(format!("Have {} voters", voters));
        if is_majority(state, voters) {
            change_mode(state, actions, Mode::Leader);
        }
    }
}

fn handle_heartbeat_timer<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>, peer: NodeId)
where
    DS: DistributedState,
{
    // TODO: test
    send_append_entries(state, actions, peer);
}

fn handle_election_timer<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>)
where
    DS: DistributedState,
{
    // TODO: test
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

/// Calculate prev_log_index and prev_log_term based on the given next_index.  This handles
/// the boundary condition of next_index == 1
fn prev_log_info<DS>(state: &RaftState<DS>, next_index: Index) -> (Index, Term)
where
    DS: DistributedState,
{
    if next_index == 1 {
        (0, 0)
    } else {
        (next_index - 1, state.log.get(next_index - 1).term)
    }
}

/// Determine whether N nodes form a majority of the network
fn is_majority<DS>(state: &RaftState<DS>, n: usize) -> bool
where
    DS: DistributedState,
{
    // note that this assumes the division operator rounds down, so e.g.,:
    //  - for a 5-node network, majority means n > 2
    //  - for a 6-node network, majority means n > 3
    n > state.network_size / 2
}

/// Update the current term based on the term in a message, and if not already
/// a follower, change to that mode.  Returns true if mode was changed.
fn update_current_term<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>, term: Term)
where
    DS: DistributedState,
{
    if term > state.current_term {
        state.current_term = term;
        if state.mode != Mode::Follower {
            change_mode(state, actions, Mode::Follower);
        }
    }
}

/// Send an AppendEntriesReq to the given peer, based on our stored next_index information,
/// and reset the heartbeat timer for that peer.
fn send_append_entries<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>, peer: NodeId)
where
    DS: DistributedState,
{
    assert_eq!(state.mode, Mode::Leader);
    let (prev_log_index, prev_log_term) = prev_log_info(state, state.next_index[peer]);
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
fn change_mode<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>, new_mode: Mode)
where
    DS: DistributedState,
{
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
                state.next_index[peer] = state.log.next_index();
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
fn start_election<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>)
where
    DS: DistributedState,
{
    assert!(state.mode == Mode::Candidate);

    // start a new term with only ourselves as a voter
    state.current_term += 1;
    state.voters = [false].repeat(state.network_size);
    state.voters[state.node_id] = true;
    state.voted_for = Some(state.node_id);

    let (last_log_index, last_log_term) = prev_log_info(state, state.log.next_index());

    for peer in 0..state.network_size {
        let message = Message::RequestVoteReq(RequestVoteReq {
            term: state.current_term,
            candidate_id: state.node_id,
            last_log_index,
            last_log_term,
        });
        actions.send_to(peer, message);
    }

    actions.set_election_timer();
}

/// Handle any changes to commit_index or match_index, committing and applying log entries
/// as necessary.
fn update_commitment<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>)
where
    DS: DistributedState,
{
    // 'If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and
    // log[N].term == currentTerm, set commitIndex = N"

    // the median element of match_index is the largest value on which a majority of peers
    // agree
    let mut sorted_match_index = state.match_index.clone();
    sorted_match_index.sort();
    let majority_index = sorted_match_index[state.network_size / 2 - 1];

    if majority_index > state.commit_index
        && state.log.get(majority_index).term == state.current_term
    {
        actions.log(format!("Increasing commit_index to {}", majority_index));
        state.commit_index = majority_index;
    }

    // "If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state
    // machine"
    while state.commit_index > state.last_applied {
        state.last_applied += 1;
        actions.apply_index(state.last_applied)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::local::LocalNetwork;
    use serde::{Deserialize, Serialize};

    use crate::diststate::{self, DistributedState};

    /// TestState just stores a string.  Requests change it.  It's easy.
    #[derive(Clone, PartialEq, Debug)]
    pub struct TestState(String);

    #[derive(PartialEq, Debug, Clone, Default)]
    pub struct Request(String);

    impl diststate::Request for Request {
        fn serialize(&self) -> serde_json::Value {
            self.0.clone().into()
        }
        fn deserialize(ser: &serde_json::Value) -> Self {
            Self(ser.as_str().unwrap().to_owned())
        }
    }

    /// Response to a Request
    #[derive(PartialEq, Debug, Clone, Default)]
    pub struct Response(String);

    impl diststate::Response for Response {}

    impl DistributedState for TestState {
        type Request = Request;
        type Response = Response;

        fn new() -> Self {
            Self("Empty".into())
        }

        fn dispatch(mut self, request: Request) -> (Self, Response) {
            self.0 = request.0;
            (self.clone(), Response(self.0))
        }
    }

    /// Set up for a handler test
    fn setup(network_size: usize) -> (RaftState<TestState>, Actions<TestState>) {
        let state = RaftState {
            node_id: 0,
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
            voters: [false].repeat(network_size),
        };
        let mut actions = Actions::new();
        actions.log_prefix = String::from("test");
        (state, actions)
    }

    /// Shorthand for making a client request
    fn request<S: Into<String>>(s: S) -> Request {
        Request(s.into())
    }

    /// Shorthand for making a log entry
    fn logentry(term: Term, item: &str) -> LogEntry<LogItem<Request>> {
        let req = request(item);
        LogEntry::new(term, LogItem { req })
    }

    /// Shorthand for making a vector of log entries
    fn logentries(tuples: Vec<(Term, &str)>) -> Vec<LogEntry<LogItem<Request>>> {
        let mut entries = vec![];
        for (t, i) in tuples {
            entries.push(logentry(t, i));
        }
        entries
    }

    #[test]
    fn test_handle_control_add_success() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Leader;
        state.current_term = 2;
        state.next_index[1] = 2;
        state.log.add(logentry(1, "a"));

        handle_control_add(&mut state, &mut actions, request("x"));

        assert_eq!(state.log.len(), 2);
        assert_eq!(state.log.get(2), &logentry(2, "x"));

        let mut expected: Actions<TestState> = Actions::new();
        expected.send_to(
            0,
            Message::AppendEntriesReq(AppendEntriesReq {
                term: 2,
                leader: 0,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: logentries(vec![(1, "a"), (2, "x")]),
                leader_commit: 0,
            }),
        );
        expected.set_heartbeat_timer(0);
        expected.send_to(
            1,
            Message::AppendEntriesReq(AppendEntriesReq {
                term: 2,
                leader: 0,
                // only appends one entry, as next_index was 2 for this peer
                prev_log_index: 1,
                prev_log_term: 1,
                entries: logentries(vec![(2, "x")]),
                leader_commit: 0,
            }),
        );
        expected.set_heartbeat_timer(1);

        assert_eq!(actions.actions, expected.actions);
    }

    #[test]
    fn test_handle_control_add_not_leader() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Follower;

        handle_control_add(&mut state, &mut actions, request("x"));

        assert_eq!(state.log.len(), 0);
        assert_eq!(actions.actions, vec![]);
    }

    #[test]
    fn test_append_entries_req_success() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Follower;
        state.log.add(logentry(1, "a"));
        state.current_term = 7;

        handle_append_entries_req(
            &mut state,
            &mut actions,
            1,
            &AppendEntriesReq {
                term: 7,
                leader: 1,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: logentries(vec![(7, "x")]),
                leader_commit: 1,
            },
        );

        assert_eq!(state.log.len(), 2);
        assert_eq!(state.log.get(2), &logentry(7, "x"));
        assert_eq!(state.commit_index, 1);
        assert_eq!(state.current_term, 7);
        assert_eq!(state.current_leader, Some(1));
        assert_eq!(
            actions.actions,
            vec![
                Action::SetElectionTimer,
                Action::ApplyIndex(1),
                Action::SendTo(
                    1,
                    Message::AppendEntriesRep(AppendEntriesRep {
                        term: 7,
                        next_index: 3,
                        success: true
                    })
                )
            ]
        );
    }

    #[test]
    fn test_append_entries_req_success_as_candidate_lost_election() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Candidate;
        state.log.add(logentry(1, "a"));
        state.current_term = 3;

        handle_append_entries_req(
            &mut state,
            &mut actions,
            1,
            &AppendEntriesReq {
                term: 7,
                leader: 1,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 4,
            },
        );

        assert_eq!(state.mode, Mode::Follower);
        assert_eq!(state.log.len(), 1);
        assert_eq!(state.commit_index, 1); // limited by number of entries..
        assert_eq!(state.current_term, 7);
        assert_eq!(state.current_leader, Some(1));
        assert_eq!(
            actions.actions,
            vec![
                // stop the Candidate election timer..
                Action::StopElectionTimer,
                // ..and set a new one as Follower
                Action::SetElectionTimer,
                // ..and set it again (simplifies the logic, doesn't hurt..)
                Action::SetElectionTimer,
                Action::ApplyIndex(1),
                Action::SendTo(
                    1,
                    Message::AppendEntriesRep(AppendEntriesRep {
                        term: 7,
                        next_index: 2,
                        success: true
                    })
                )
            ]
        );
    }

    #[test]
    fn test_append_entries_req_old_term() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Follower;
        state.current_term = 7;

        handle_append_entries_req(
            &mut state,
            &mut actions,
            1,
            &AppendEntriesReq {
                term: 3,
                leader: 0,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 4,
            },
        );

        assert_eq!(state.log.len(), 0);
        assert_eq!(state.current_term, 7);
        assert_eq!(
            actions.actions,
            vec![
                Action::SetElectionTimer,
                Action::SendTo(
                    1,
                    Message::AppendEntriesRep(AppendEntriesRep {
                        term: 7,
                        next_index: 1,
                        success: false
                    })
                )
            ]
        );
    }

    #[test]
    fn test_append_entries_req_log_gap() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Follower;
        state.current_term = 7;

        handle_append_entries_req(
            &mut state,
            &mut actions,
            1,
            &AppendEntriesReq {
                term: 7,
                leader: 0,
                prev_log_index: 5,
                prev_log_term: 7,
                entries: vec![],
                leader_commit: 0,
            },
        );

        assert_eq!(state.log.len(), 0);
        assert_eq!(state.current_term, 7);
        assert_eq!(
            actions.actions,
            vec![
                Action::SetElectionTimer,
                Action::SendTo(
                    1,
                    Message::AppendEntriesRep(AppendEntriesRep {
                        term: 7,
                        next_index: 1,
                        success: false
                    })
                )
            ]
        );
    }

    #[test]
    fn test_append_entries_req_as_leader() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Leader;

        handle_append_entries_req(
            &mut state,
            &mut actions,
            1,
            &AppendEntriesReq {
                term: 0,
                leader: 0,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
        );

        assert_eq!(state.log.len(), 0);
        assert_eq!(actions.actions, vec![]);
    }

    #[test]
    fn test_append_entries_rep_success() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Leader;

        handle_append_entries_rep(
            &mut state,
            &mut actions,
            1,
            &AppendEntriesRep {
                term: 0,
                next_index: 3,
                success: true,
            },
        );

        assert_eq!(state.next_index[1], 3);
        assert_eq!(state.match_index[1], 2);
        assert_eq!(actions.actions, vec![]);
    }

    #[test]
    fn test_append_entries_rep_not_success_decrement() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Leader;
        state.log.add(logentry(1, "a"));
        state.log.add(logentry(1, "b"));
        state.next_index[1] = 2;

        handle_append_entries_rep(
            &mut state,
            &mut actions,
            1,
            &AppendEntriesRep {
                term: 0,
                next_index: 2, // peer claims it has one entry that didn't match
                success: false,
            },
        );

        assert_eq!(state.next_index[1], 1); // decremented..
        assert_eq!(state.match_index[1], 0); // not changed
        assert_eq!(
            actions.actions,
            vec![
                Action::SendTo(
                    1,
                    Message::AppendEntriesReq(AppendEntriesReq {
                        term: 0,
                        leader: 0,
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: logentries(vec![(1, "a"), (1, "b")]),
                        leader_commit: 0
                    })
                ),
                Action::SetHeartbeatTimer(1),
            ]
        );
    }

    #[test]
    fn test_append_entries_rep_not_success_supplied_next_index() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Leader;
        state.log.add(logentry(1, "a"));
        state.log.add(logentry(2, "b"));
        state.log.add(logentry(2, "c"));
        state.log.add(logentry(2, "d"));
        state.next_index[1] = 4;

        handle_append_entries_rep(
            &mut state,
            &mut actions,
            1,
            &AppendEntriesRep {
                term: 0,
                next_index: 2, // peer says it has one entry
                success: false,
            },
        );

        assert_eq!(state.next_index[1], 2); // set per peer (down from 4)
        assert_eq!(state.match_index[1], 0); // not changed
        assert_eq!(
            actions.actions,
            vec![
                Action::SendTo(
                    1,
                    Message::AppendEntriesReq(AppendEntriesReq {
                        term: 0,
                        leader: 0,
                        prev_log_index: 1,
                        prev_log_term: 1,
                        entries: logentries(vec![(2, "b"), (2, "c"), (2, "d")]),
                        leader_commit: 0
                    })
                ),
                Action::SetHeartbeatTimer(1),
            ]
        );
    }

    #[test]
    fn test_append_entries_rep_not_success_new_term() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Leader;
        state.current_term = 4;

        handle_append_entries_rep(
            &mut state,
            &mut actions,
            1,
            &AppendEntriesRep {
                term: 5,
                next_index: 0,
                success: false,
            },
        );

        assert_eq!(state.current_term, 5);
        assert_eq!(state.mode, Mode::Follower);
        assert_eq!(
            actions.actions,
            vec![Action::StopHeartbeatTimers, Action::SetElectionTimer,]
        );
    }

    #[test]
    fn test_append_entries_rep_as_follower() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Follower;

        handle_append_entries_rep(
            &mut state,
            &mut actions,
            1,
            &AppendEntriesRep {
                term: 0,
                next_index: 0,
                success: false,
            },
        );

        assert_eq!(actions.actions, vec![]);
    }

    #[test]
    fn test_append_entries_rep_as_candidate() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Candidate;

        handle_append_entries_rep(
            &mut state,
            &mut actions,
            1,
            &AppendEntriesRep {
                term: 0,
                next_index: 0,
                success: false,
            },
        );

        assert_eq!(actions.actions, vec![]);
    }

    mod integration {
        use super::*;

        use crate::diststate::{self, DistributedState};
        use crate::net::local::{LocalNetwork, LocalNode};
        use std::collections::HashMap;
        use tokio::time::delay_for;

        #[derive(Clone, PartialEq, Debug)]
        pub struct TestState(HashMap<String, String>);

        /// Clients send Request objects (JSON-encoded) to the server.
        #[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
        pub struct Request {
            /// Operation: one of get, set, or delete
            pub op: String,

            /// key to get, set, or delete
            pub key: String,

            /// value to set (ignored, and can be omitted, for get and delete)
            #[serde(default)]
            pub value: String,
        }

        impl diststate::Request for Request {
            fn serialize(&self) -> serde_json::Value {
                json!({
                    "op": self.op,
                    "key": self.key,
                    "value": self.value,
                })
            }
            fn deserialize(ser: &serde_json::Value) -> Self {
                Request {
                    op: ser.get("op").unwrap().as_str().unwrap().to_owned(),
                    key: ser.get("key").unwrap().as_str().unwrap().to_owned(),
                    value: ser.get("value").unwrap().as_str().unwrap().to_owned(),
                }
            }
        }

        /// Response to a Request
        #[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
        pub struct Response {
            #[serde(default)]
            pub value: Option<String>,
        }

        impl diststate::Response for Response {}

        impl DistributedState for TestState {
            type Request = Request;
            type Response = Response;

            fn new() -> Self {
                Self(HashMap::new())
            }

            fn dispatch(mut self, request: Request) -> (Self, Response) {
                match &request.op[..] {
                    "set" => {
                        self.0.insert(request.key, request.value);
                        (self, Response { value: None })
                    }
                    "get" => {
                        let resp = Response {
                            value: self.0.get(&request.key).cloned(),
                        };
                        (self, resp)
                    }
                    "delete" => {
                        self.0.remove(&request.key);
                        (self, Response { value: None })
                    }
                    _ => panic!("unknown op {:?}", request.op),
                }
            }
        }

        #[tokio::test]
        async fn elect_a_leader() -> Fallible<()> {
            let mut net = LocalNetwork::new(4);
            let mut servers: Vec<RaftServer<TestState>> = vec![
                RaftServer::new(net.take(0)),
                RaftServer::new(net.take(1)),
                RaftServer::new(net.take(2)),
                RaftServer::new(net.take(3)),
            ];

            // wait until we get a leader (usually ELECTION_TIMEOUT, sometimes a multiple of that)
            'leader: loop {
                for server in &mut servers {
                    let state = server.get_state().await?;
                    if state.mode == Mode::Leader {
                        break 'leader;
                    }
                }

                delay_for(Duration::from_millis(100)).await;
            }

            for server in servers.drain(..) {
                server.stop().await;
            }

            Ok(())
        }
    }
}
