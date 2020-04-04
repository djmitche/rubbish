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
use super::inner::Actions;
use super::inner::RaftServerInner;
use super::state::{Mode, RaftState};

#[cfg(test)]
use std::time::SystemTime;

/// A RaftServer represents a running server participating in a Raft.
#[derive(Debug)]
pub struct RaftServer<DS>
where
    DS: DistributedState,
{
    /// The background task receiving messages for this server
    task: task::JoinHandle<()>,

    /// A channel to send control messages to the background task
    control_tx: mpsc::Sender<Control<DS>>,

    /// A channel to receive control messages from the background task
    control_rx: mpsc::Receiver<Control<DS>>,
}

impl<DS> RaftServer<DS>
where
    DS: DistributedState + Clone,
{
    pub fn new<NODE>(node: NODE) -> RaftServer<DS>
    where
        NODE: RaftNetworkNode + Sync + Send + 'static,
    {
        let (control_tx_in, control_rx_in) = mpsc::channel(1);
        let (control_tx_out, control_rx_out) = mpsc::channel(1);
        let node_id = node.node_id();
        let network_size = node.network_size();
        let inner = RaftServerInner::new(node, control_rx_in, control_tx_out);

        RaftServer {
            task: tokio::spawn(async move { inner.run().await }),
            control_tx: control_tx_in,
            control_rx: control_rx_out,
        }
    }

    /// Stop the server
    pub async fn stop(mut self) {
        self.control_tx.send(Control::Stop).await.unwrap();
        self.task.await.unwrap();
    }

    /// Make a request of the distributed state machine
    pub async fn request(&mut self, req: DS::Request) -> Fallible<DS::Response> {
        self.control_tx.send(Control::Request(req)).await?;
        if let Control::Response(resp) = self.control_rx.recv().await.unwrap() {
            return Ok(resp);
        } else {
            panic!("got unexpected control message");
        }
    }

    /// Get a copy of the current server state (for testing)
    #[cfg(test)]
    pub(super) async fn get_state(&mut self) -> Fallible<RaftState<DS>> {
        self.control_tx.send(Control::GetState).await?;
        if let Control::SetState(state) = self.control_rx.recv().await.unwrap() {
            return Ok(state);
        } else {
            panic!("got unexpected control message");
        }
    }

    /// Set the current server state (for testing)
    #[cfg(test)]
    pub(super) async fn set_state(&mut self, state: RaftState<DS>) -> Fallible<()> {
        self.control_tx.send(Control::SetState(state)).await?;
        Ok(())
    }
}
