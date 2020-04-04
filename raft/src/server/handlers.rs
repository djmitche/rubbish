use super::inner::Actions;
use super::log::LogItem;
use super::message::*;
use super::state::{Mode, RaftState};
use crate::diststate::DistributedState;
use crate::log::LogEntry;
use crate::net::NodeId;
use crate::{Index, Term};
use std::cmp;

#[cfg(test)]
use super::control::Control;

pub(super) fn handle_control_add<DS>(
    state: &mut RaftState<DS>,
    actions: &mut Actions<DS>,
    req: DS::Request,
) where
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
pub(super) fn handle_control_get_state<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>)
where
    DS: DistributedState,
{
    actions.send_control(Control::SetState(state.clone()));
}

pub(super) fn handle_append_entries_req<DS>(
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

        // Update our notion of who is the current leader..
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

pub(super) fn handle_append_entries_rep<DS>(
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

pub(super) fn handle_request_vote_req<DS>(
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

pub(super) fn handle_request_vote_rep<DS>(
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

pub(super) fn handle_heartbeat_timer<DS>(
    state: &mut RaftState<DS>,
    actions: &mut Actions<DS>,
    peer: NodeId,
) where
    DS: DistributedState,
{
    // TODO: test
    send_append_entries(state, actions, peer);
}

pub(super) fn handle_election_timer<DS>(state: &mut RaftState<DS>, actions: &mut Actions<DS>)
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
        state.voted_for = None;
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
