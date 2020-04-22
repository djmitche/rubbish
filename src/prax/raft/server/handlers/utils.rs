use crate::net::NodeId;
use crate::prax::raft::diststate::DistributedState;
use crate::prax::raft::server::inner::Actions;
use crate::prax::raft::server::message::*;
use crate::prax::raft::server::state::{Mode, RaftState};
use crate::prax::raft::{Index, Term};

/// Calculate prev_log_index and prev_log_term based on the given next_index.  This handles
/// the boundary condition of next_index == 1
pub(super) fn prev_log_info<DS>(state: &RaftState<DS>, next_index: Index) -> (Index, Term)
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
pub(super) fn is_majority<DS>(state: &RaftState<DS>, n: usize) -> bool
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
pub(super) fn update_current_term<DS>(
    state: &mut RaftState<DS>,
    actions: &mut Actions<DS>,
    term: Term,
) where
    DS: DistributedState,
{
    if term > state.current_term {
        state.current_term = term;
        state.voted_for = None;
        if state.mode != Mode::Follower {
            change_mode(state, actions, Mode::Follower);
        }
    }
}

/// Send an AppendEntriesReq to the given peer, based on our stored next_index information,
/// and reset the heartbeat timer for that peer.
pub(super) fn send_append_entries<DS>(
    state: &mut RaftState<DS>,
    actions: &mut Actions<DS>,
    peer: NodeId,
) where
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
pub(super) fn change_mode<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>, new_mode: Mode)
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
pub(super) fn start_election<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>)
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
pub(super) fn update_commitment<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>)
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
