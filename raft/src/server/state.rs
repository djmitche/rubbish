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

use super::LogItem;

/// Raft-related state of the server
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RaftState<DS>
where
    DS: DistributedState,
{
    /// This node
    pub(super) node_id: NodeId,

    /// Number of nodes in the network
    pub(super) network_size: usize,

    /// True if the server should stop after this iterataion
    pub(super) stop: bool,

    /// Current server mode
    pub(super) mode: Mode,

    /// "latest term the server has seen"
    pub(super) current_term: Term,

    /// Current leader, if known
    pub(super) current_leader: Option<NodeId>,

    /// "candidateId that received vote in current term (or null if none)"
    pub(super) voted_for: Option<NodeId>,

    /// The log entries
    pub(super) log: RaftLog<LogItem<DS::Request>>,

    /// Index of the highest log entry known to be committed
    pub(super) commit_index: Index,

    /// Index of the highest log entry applied to state machine
    pub(super) last_applied: Index,

    /// "for each server, index of the next log entry to send to that server"
    pub(super) next_index: Vec<Index>,

    /// "for each server, index of the highest log entry known to be replicated on server"
    pub(super) match_index: Vec<Index>,

    /// true for all nodes that have voted for this node as leader
    pub(super) voters: Vec<bool>,
}

/// The current mode of the server
#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) enum Mode {
    Follower,
    Candidate,
    Leader,
}
