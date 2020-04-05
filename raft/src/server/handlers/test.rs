use crate::server::inner::{Actions};
use crate::server::log::LogItem;
use crate::server::state::{Mode, RaftState};
use crate::diststate::{self, DistributedState};
use crate::log::{LogEntry, RaftLog};
use crate::Term;
use serde_json;

/// TestState just stores a string.  Requests change it.  It's easy.
#[derive(Clone, PartialEq, Debug)]
pub(super) struct TestState(String);

#[derive(PartialEq, Debug, Clone, Default)]
pub(super) struct Request(String);

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
pub(super) struct Response(String);

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
pub(super) fn setup(network_size: usize) -> (RaftState<TestState>, Actions<TestState>) {
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
    #[allow(unused_mut)]
    let mut actions = Actions::new();
    #[cfg(feature = "debugging")]
    actions.set_log_prefix("test".into());
    (state, actions)
}

/// Shorthand for making a client request
pub(super) fn request<S: Into<String>>(s: S) -> Request {
    Request(s.into())
}

/// Shorthand for making a log entry
pub(super) fn logentry(term: Term, item: &str) -> LogEntry<LogItem<Request>> {
    let req = request(item);
    LogEntry::new(term, LogItem { req })
}

/// Shorthand for making a vector of log entries
pub(super) fn logentries(tuples: Vec<(Term, &str)>) -> Vec<LogEntry<LogItem<Request>>> {
    let mut entries = vec![];
    for (t, i) in tuples {
        entries.push(logentry(t, i));
    }
    entries
}

