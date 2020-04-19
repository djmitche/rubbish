use crate::prax::raft::diststate::DistributedState;

#[cfg(test)]
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
