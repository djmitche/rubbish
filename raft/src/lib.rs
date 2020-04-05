#![allow(dead_code)]
#![allow(unused_variables)]

/// A Term describes the leadership term in which an entry was amde
type Term = u64;

/// An Index is a position within the raft log.
type Index = u64;

pub mod diststate;
mod errors;
pub mod kv;
pub mod log;
pub mod net;
pub mod server;
pub mod transport;
pub(crate) mod util;

pub use errors::Error;
