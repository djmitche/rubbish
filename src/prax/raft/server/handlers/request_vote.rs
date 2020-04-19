use crate::prax::raft::server::inner::Actions;
use crate::prax::raft::server::message::*;
use crate::prax::raft::server::state::{Mode, RaftState};
use crate::prax::raft::diststate::DistributedState;
use crate::net::NodeId;
use super::utils::*;

pub(in crate::prax::raft::server) fn handle_request_vote_req<DS>(
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

pub(in crate::prax::raft::server) fn handle_request_vote_rep<DS>(
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
