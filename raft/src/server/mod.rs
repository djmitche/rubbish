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

mod server;
pub use server::RaftServer;
mod inner;
use inner::RaftServerInner;
mod control;
mod handlers;
use control::Control;
mod state;
use state::{Mode, RaftState};

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

#[cfg(test)]
mod test {
    use super::handlers::*;
    use super::*;
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

        fn dispatch(&mut self, request: &Request) -> Response {
            self.0 = request.0.clone();
            Response(self.0.clone())
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
        assert_eq!(state.voted_for, None);
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
        state.voted_for = Some(1);

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
        assert_eq!(state.voted_for, None);
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
        use crate::net::local::LocalNetwork;
        use std::collections::HashMap;
        use tokio::time::delay_for;

        #[derive(Clone, PartialEq, Debug)]
        pub struct TestState(HashMap<String, String>);

        /// Clients send Request objects (JSON-encoded) to the server.
        #[derive(PartialEq, Debug, Clone, Default)]
        pub struct Request {
            /// Operation: one of get, set, or delete
            pub op: String,

            /// key to get, set, or delete
            pub key: String,

            /// value to set (ignored, and can be omitted, for get and delete)
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
        #[derive(PartialEq, Debug, Clone, Default)]
        pub struct Response {
            pub value: Option<String>,
        }

        impl diststate::Response for Response {}

        impl DistributedState for TestState {
            type Request = Request;
            type Response = Response;

            fn new() -> Self {
                Self(HashMap::new())
            }

            fn dispatch(&mut self, request: &Request) -> Response {
                match &request.op[..] {
                    "set" => {
                        self.0.insert(request.key.clone(), request.value.clone());
                        Response {
                            value: Some(request.value.clone()),
                        }
                    }
                    "get" => Response {
                        value: self.0.get(&request.key).cloned(),
                    },
                    "delete" => {
                        self.0.remove(&request.key);
                        Response { value: None }
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

            async fn get_leader(servers: &mut Vec<RaftServer<TestState>>) -> Fallible<NodeId> {
                loop {
                    for server in servers.iter_mut() {
                        let state = server.get_state().await?;
                        if state.mode == Mode::Leader {
                            return Ok(state.node_id);
                        }
                    }

                    delay_for(Duration::from_millis(100)).await;
                }
            }

            async fn set(
                server: &mut RaftServer<TestState>,
                key: &str,
                value: &str,
            ) -> Fallible<Option<String>> {
                let res = server
                    .request(Request {
                        op: "set".into(),
                        key: key.into(),
                        value: value.into(),
                    })
                    .await?;
                Ok(res.value)
            }

            async fn get(
                server: &mut RaftServer<TestState>,
                key: &str,
            ) -> Fallible<Option<String>> {
                let res = server
                    .request(Request {
                        op: "get".into(),
                        key: key.into(),
                        value: "".into(),
                    })
                    .await?;
                Ok(res.value)
            }

            async fn delete(
                server: &mut RaftServer<TestState>,
                key: &str,
            ) -> Fallible<Option<String>> {
                let res = server
                    .request(Request {
                        op: "delete".into(),
                        key: key.into(),
                        value: "".into(),
                    })
                    .await?;
                Ok(res.value)
            }

            let leader = get_leader(&mut servers).await?;

            // get when nothing exists, set, then get again
            assert_eq!(get(&mut servers[leader], "k").await?, None);
            assert_eq!(
                set(&mut servers[leader], "k", "foo").await?,
                Some("foo".into())
            );
            assert_eq!(get(&mut servers[leader], "k").await?, Some("foo".into()));

            // make some more changes>.
            assert_eq!(delete(&mut servers[leader], "k").await?, None);
            assert_eq!(get(&mut servers[leader], "k").await?, None);

            // shut it all down
            for server in servers.drain(..) {
                server.stop().await;
            }

            Ok(())
        }
    }
}
