use super::log::LogItem;
use crate::prax::raft::diststate::DistributedState;
use crate::prax::raft::log::RaftLog;
use crate::net::NodeId;
use crate::prax::raft::{Index, Term};

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
