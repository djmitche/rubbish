use super::utils::*;
use crate::prax::raft::diststate::DistributedState;
use crate::prax::raft::log::LogEntry;
use crate::prax::raft::server::inner::Actions;
use crate::prax::raft::server::log::LogItem;
use crate::prax::raft::server::state::{Mode, RaftState};

#[cfg(test)]
use crate::prax::raft::server::control::Control;

pub(in crate::prax::raft::server) fn handle_control_add<DS>(
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
pub(in crate::prax::raft::server) fn handle_control_get_state<DS>(
    state: &mut RaftState<DS>,
    actions: &mut Actions<DS>,
) where
    DS: DistributedState,
{
    actions.send_control(Control::SetState(state.clone()));
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::prax::raft::server::handlers::test::*;
    use crate::prax::raft::server::message::*;

    #[test]
    fn test_handle_control_add_success() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Leader;
        state.current_term = 2;
        state.next_index[1] = 2;
        state.log.add(logentry(1, "a"));

        handle_control_add(&mut state, &mut actions, request("x"));

        assert_eq!(state.log.len(), 2);
        assert_eq!(state.log.get(2), &logentry(2, "x"));

        let mut expected: Actions<TestState> = Actions::new();
        expected.send_to(
            0,
            Message::AppendEntriesReq(AppendEntriesReq {
                term: 2,
                leader: 0,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: logentries(vec![(1, "a"), (2, "x")]),
                leader_commit: 0,
            }),
        );
        expected.set_heartbeat_timer(0);
        expected.send_to(
            1,
            Message::AppendEntriesReq(AppendEntriesReq {
                term: 2,
                leader: 0,
                // only appends one entry, as next_index was 2 for this peer
                prev_log_index: 1,
                prev_log_term: 1,
                entries: logentries(vec![(2, "x")]),
                leader_commit: 0,
            }),
        );
        expected.set_heartbeat_timer(1);

        assert_eq!(actions.actions, expected.actions);
    }

    #[test]
    fn test_handle_control_add_not_leader() {
        let (mut state, mut actions) = setup(2);
        state.mode = Mode::Follower;

        handle_control_add(&mut state, &mut actions, request("x"));

        assert_eq!(state.log.len(), 0);
        assert_eq!(actions.actions, vec![]);
    }
}
