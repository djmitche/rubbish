use crate::server::inner::Actions;
use crate::server::state::{Mode, RaftState};
use crate::diststate::DistributedState;
use crate::net::NodeId;
use super::utils::*;

pub(in crate::server) fn handle_heartbeat_timer<DS>(
    state: &mut RaftState<DS>,
    actions: &mut Actions<DS>,
    peer: NodeId,
) where
    DS: DistributedState,
{
    // TODO: test
    send_append_entries(state, actions, peer);
}

pub(in crate::server) fn handle_election_timer<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>)
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