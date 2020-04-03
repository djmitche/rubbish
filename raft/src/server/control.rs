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

use super::state::RaftState;

/// Control messages are sent between RaftServer and RaftServerInner.
#[derive(Debug, PartialEq)]
pub(super) enum Control<DS>
where
    DS: DistributedState,
{
    /// (server -> inner) Stop the task
    Stop,

    /// (server -> inner) Client Request to the state machine
    Request(DS::Request),

    /// (inner -> server) Response from applying a request
    Response(DS::Response),

    /// (server -> inner) Send a SetState back containing the current state
    #[cfg(test)]
    GetState,

    /// (server -> inner) Set the current raft state;
    /// (inner -> server) reply to GetState
    #[cfg(test)]
    SetState(RaftState<DS>),
}
