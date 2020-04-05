use serde_json;
use std::fmt;

/// A Request has no methods, but a number of trait bounds
pub trait Request: fmt::Debug + Default + PartialEq + Clone + Sync + Send {
    /// Serialize this request for transmission over the network
    fn serialize(&self) -> serde_json::Value;

    /// Deserialize a representation created by Serialize.  Panic on failure.
    fn deserialize(ser: &serde_json::Value) -> Self;
}

/// A Request has no methods, but a number of trait bounds
pub trait Response: fmt::Debug + Default + PartialEq + Clone + Sync + Send {}

/// DistributedState defines the state that is maintained by the raft algorithm.
///
/// It is implemented as a state machine: all operations take the form of an
/// requeset, and dispatch of an request generates a result and an updated state.
pub trait DistributedState: Sized + fmt::Debug + Clone + Sync + Send + 'static {
    /// A request to modify the state
    type Request: Request;

    /// The result from dispatching a Request
    type Response: Response;

    /// Create a new, empty state
    fn new() -> Self;

    /// Dispatch a request to the state
    fn dispatch(&mut self, request: &Self::Request) -> Self::Response;
}
