use super::utils::*;
use crate::net::NodeId;
use crate::prax::raft::diststate::DistributedState;
use crate::prax::raft::server::inner::Actions;
use crate::prax::raft::server::message::*;
use crate::prax::raft::server::state::{Mode, RaftState};
use std::cmp;

pub(in crate::prax::raft::server) fn handle_append_entries_req<DS>(
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

pub(in crate::prax::raft::server) fn handle_append_entries_rep<DS>(
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::prax::raft::server::handlers::test::*;
    use crate::prax::raft::server::inner::Action;

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
}
