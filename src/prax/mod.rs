//! A distributed consensus service.
//!
//! This service achieves consensus on a value of an arbitrary encodable type that implements
//! `DistributedState`. This state is modified by a fixed sequence of state-change operations,
//! represented as encodable values of associaed type Change.
//!
//! The state change operations cannot fail, as they are not applied until consensus is reached.
//! However, they can return None to indicate that the operation does not apply. This is useful
//! when the change includes some precondition which is not satisfied.
//!
//! For the moment (TODO!) this is not actually distributed, which makes consensus a great deal
//! easier to achieve.
//!
//! # Examples
//!
//! ```
//! use rubbish::prax::{DistributedState, Prax};
//!
//! #[derive(Clone, Debug, PartialEq)]
//! struct State(i32);
//! enum Change {
//!     Add(i32),
//!     Subtract(i32),
//! }
//!
//! impl DistributedState for State {
//!     type Change = Change;
//!
//!     fn update(&self, change: Change) -> Option<Self> {
//!         match change {
//!             Change::Add(i) => Some(State(self.0 + i)),
//!             Change::Subtract(i) => Some(State(self.0 - i)),
//!         }
//!     }
//! }
//!
//! fn main() {
//!     let mut prax = Prax::new(State(100));
//!     assert!(prax.update(Change::Add(5)));
//!     assert!(prax.update(Change::Subtract(7)));
//!     assert_eq!(prax.read(), State(98));
//! }
//! ```

pub trait DistributedState: Sized {
    type Change;

    /// Apply the given state change to this state, either returning a new state if applicable,
    /// or None to indicate that the change is not applicable.
    fn update(&self, change: Self::Change) -> Option<Self>;
}

pub struct Prax<S> {
    state: S,
}

impl<S> Prax<S>
    where S: DistributedState + Clone
{
    /// Create a new Prax instance, beginning with the given state
    pub fn new(state: S) -> Prax<S> {
        Prax { state: state }
    }

    /// Update the state by applying the given change.
    ///
    /// When this function returns, the change has been committed across the cluster.
    /// Returns true if the change was applicable, otherwise false.
    pub fn update(&mut self, change: S::Change) -> bool {
        match self.state.update(change) {
            Some(new) => {
                self.state = new;
                true
            }
            None => false,
        }
    }

    /// Get a copy of the current state.
    ///
    /// This state is "up to date": the method carries out a consensus transaction to
    /// ensure that this cluster member has not missed an update.  However, the state
    /// may change as this method returns.  The notion of "right now" is not valid in
    /// a distributed context!
    pub fn read(&self) -> S {
        self.state.clone()
    }
}
