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

use super::control::Control;
use super::handlers;
use super::state::{Mode, RaftState};
use super::{Actions, Message, *};

#[cfg(test)]
use std::time::SystemTime;

/// Container for the background task in a server.
///
/// Most of the work of a server occurs in a background task, reacting to messages and timers.  In
/// fact, all of the work occurs in the background to simplify ownership of the data structures, and
/// all communication occurs control_tx / control_rx.  Public methods simply send control messages
/// to the background task.  In cases where a reply is required, the control message contains a
/// transient channel to carry the response.
pub(super) struct RaftServerInner<NODE, DS>
where
    NODE: RaftNetworkNode + Sync + Send + 'static,
    DS: DistributedState,
{
    /*
     * Mechanics
     */
    /// The network node, used for communication
    node: NODE,

    /// Channel containing control messages from other tasks
    control_rx: mpsc::Receiver<Control<DS>>,

    /// Channel for sending control information back to other tasks
    control_tx: mpsc::Sender<Control<DS>>,

    /// A queue of Timer objects
    timers: DelayQueue<Timer>,

    /// DelayQueue keys for the last time AppendEntries was sent to this peer
    heartbeat_delay: Vec<Option<delay_queue::Key>>,

    /// Timeout related to elections; when this goes off, start a new election.
    election_timeout: Option<delay_queue::Key>,

    /// Raft-related state of the server
    state: RaftState<DS>,

    /// Our copy of the distributed state
    ds: DS,

    /// Actions on the server (re-used in place)
    actions: Actions<DS>,
}

/// A Timer is an event that is scheduled at some future time.
#[derive(Debug)]
enum Timer {
    /// This follower may not have gotten an AppendEntriesReq in a while
    Heartbeat(NodeId),

    /// We should start an election
    Election,
}

impl<NODE, DS> RaftServerInner<NODE, DS>
where
    NODE: RaftNetworkNode + Sync + Send + 'static,
    DS: DistributedState,
{
    pub(super) fn new(
        node: NODE,
        control_rx: mpsc::Receiver<Control<DS>>,
        control_tx: mpsc::Sender<Control<DS>>,
    ) -> RaftServerInner<NODE, DS> {
        let node_id = node.node_id();
        let network_size = node.network_size();
        RaftServerInner {
            node,
            timers: DelayQueue::new(),
            heartbeat_delay: iter::repeat_with(|| None).take(network_size).collect(),
            election_timeout: None,
            control_rx,
            control_tx,
            // TODO: RaftState::new(node_id, network_size)
            state: RaftState {
                node_id,
                network_size,
                stop: false,
                mode: Mode::Follower,
                current_term: 0,
                current_leader: None,
                voted_for: None,
                log: RaftLog::new(),
                commit_index: 0,
                last_applied: 0,
                next_index: [1].repeat(network_size),
                match_index: [0].repeat(network_size),
                voters: [false].repeat(network_size),
            },
            ds: DS::new(),
            actions: Actions::new(),
        }
    }

    // event handling

    pub(super) async fn run(mut self) {
        #[cfg(test)]
        {
            self.actions
                .set_log_prefix(format!("server={}", self.state.node_id,));
        }

        // start things up..
        self.startup();
        self.execute_actions().await.unwrap();

        while !self.state.stop {
            #[cfg(test)]
            {
                self.actions.set_log_prefix(format!(
                    "server={} ({:?})",
                    self.state.node_id, self.state.mode
                ));
            }

            tokio::select! {
                // TODO: is pattern-matching the right idea here??
                Some(c) = self.control_rx.recv() => self.handle_control(c),
                Ok((peer, msg)) = self.node.recv() => self.handle_message(peer, msg),
                Some(t) = self.timers.next() => self.handle_timer(t.unwrap().into_inner()),
            }

            // execute any actions accumulated in this loop
            self.execute_actions().await.unwrap();
        }
    }

    fn startup(&mut self) {
        // on startup, set the election timer, so that we either learn about an existing leader
        // or try to become a leader
        self.actions.set_election_timer();
    }

    fn handle_message(&mut self, peer: NodeId, msg: Vec<u8>) {
        let message: Message<DS> = Message::deserialize(&msg);

        self.actions
            .log(format!("Handling Message {:?} from {}", message, peer));

        match message {
            Message::AppendEntriesReq(ref message) => handlers::handle_append_entries_req(
                &mut self.state,
                &mut self.actions,
                peer,
                message,
            ),
            Message::AppendEntriesRep(ref message) => handlers::handle_append_entries_rep(
                &mut self.state,
                &mut self.actions,
                peer,
                message,
            ),
            Message::RequestVoteReq(ref message) => {
                handlers::handle_request_vote_req(&mut self.state, &mut self.actions, peer, message)
            }
            Message::RequestVoteRep(ref message) => {
                handlers::handle_request_vote_rep(&mut self.state, &mut self.actions, peer, message)
            }
        }
    }

    fn handle_timer(&mut self, t: Timer) {
        self.actions.log(format!("Handling Timer {:?}", t));

        // self.timers.remove will panic if called with a key that has already fired, so we are
        // careful to delete the key before handling any of these timers.
        match t {
            Timer::Heartbeat(node_id) => {
                self.heartbeat_delay[node_id] = None;
                handlers::handle_heartbeat_timer(&mut self.state, &mut self.actions, node_id);
            }

            Timer::Election => {
                self.election_timeout = None;
                handlers::handle_election_timer(&mut self.state, &mut self.actions);
            }
        };
    }

    /// Handle a control message from the main process, and return true if the task should exit
    fn handle_control(&mut self, c: Control<DS>) {
        self.actions
            .log(format!("Handling Control message {:?}", c));

        match c {
            Control::Stop => self.state.stop = true,

            Control::Request(req) => {
                handlers::handle_control_add(&mut self.state, &mut self.actions, req)
            }

            Control::Response(_) => unreachable!(),

            #[cfg(test)]
            Control::GetState => {
                handlers::handle_control_get_state(&mut self.state, &mut self.actions)
            }

            #[cfg(test)]
            Control::SetState(state) => self.state = state,
        }
    }

    /// Execute the actions that a handler function specified.
    async fn execute_actions(&mut self) -> Fallible<()> {
        for action in self.actions.drain() {
            match action {
                Action::SetElectionTimer => {
                    if let Some(k) = self.election_timeout.take() {
                        self.timers.remove(&k);
                    }
                    // select a time within ELECTION_TIMEOUT += ELECTION_TIMEOUT_FUZZ
                    let election_timeout = ELECTION_TIMEOUT.as_millis() as u64;
                    let fuzz = ELECTION_TIMEOUT_FUZZ.as_millis() as u64;
                    let mut rng = thread_rng();
                    let millis = rng.gen_range(election_timeout - fuzz, election_timeout + fuzz);
                    self.election_timeout = Some(
                        self.timers
                            .insert(Timer::Election, Duration::from_millis(millis)),
                    );
                }
                Action::StopElectionTimer => {
                    if let Some(k) = self.election_timeout.take() {
                        self.timers.remove(&k);
                    }
                }
                Action::SetHeartbeatTimer(peer) => {
                    if let Some(delay_key) = self.heartbeat_delay[peer].take() {
                        self.timers.remove(&delay_key);
                    }
                    self.heartbeat_delay[peer] =
                        Some(self.timers.insert(Timer::Heartbeat(peer), HEARTBEAT));
                }
                Action::StopHeartbeatTimers => {
                    for delay in &mut self.heartbeat_delay.iter_mut() {
                        if let Some(k) = delay.take() {
                            self.timers.remove(&k);
                        }
                    }
                }
                Action::ApplyIndex(index) => {
                    let entry = self.state.log.get(index);
                    let response = self.ds.dispatch(&entry.item.req);
                    println!("apply {:?} -> {:?}", entry.item, response);
                    // XXX TODO uhhh...
                    if self.state.mode == Mode::Leader {
                        self.control_tx.send(Control::Response(response)).await?;
                    }
                }
                Action::SendTo(peer, message) => {
                    let msg = message.serialize();
                    self.node.send(peer, msg).await?;
                }
                Action::SendControl(control) => {
                    self.control_tx.send(control).await?;
                }
            };
        }
        Ok(())
    }
}
