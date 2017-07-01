//! An API for Rubbish clusters.
//!
//! The API provides methods for writing to and reading from a rubbish cluster, each of which
//! implements specific semantics regarding the distributed nature of the cluster.

mod error;
mod tip;

pub use tip::error::*;
pub use tip::tip::Tip;
