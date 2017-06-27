//! `cas` -- a distributed, content-addressible, in-memory storage system.  The
//! system uses a "gossip"-style protocol to ensure that all participants have
//! all content, and supports generational garbage collection and persistence
//! to disk.
//!
//! The API is in the `CAS` trait.
//!
//! # Warning
//!
//! This module will happily de-serialize an object as a different type than was used
//! to serialize it, if asked to.  In most cases, this will not result in an error,
//! just a bogus result value. The rationale for this design is that the stored data
//! is shared among multiple nodes in a network, so any guarantees within a sigle
//! instance of the application do not apply across the network.  For example, the
//! `user::types::Data` struct may have a different format on different nodes. The
//! user must guard against this possibility, so there is no need to waste time
//! verifying type IDs within this module.
//!
//! # TODO
//!
//!  * Use Serde instead of rustc_serialize
//!
//! # Examples
//!
//! ```
//! use rubbish::cas::CAS;
//! let mut storage = rubbish::cas::Storage::new();
//!
//! // store some things
//! let hash42 = storage.store(&42u32).unwrap();
//! let hash314 = storage.store(&"π".to_string()).unwrap();
//!
//! // and retrieve them, by type
//! assert_eq!(storage.retrieve::<u32>(&hash42).unwrap(), 42u32);
//! assert_eq!(storage.retrieve::<String>(&hash314).unwrap(), "π".to_string());
//! ```
//!
//! An example with a custom type, and using Arc to refer to Storage from multiple
//! threads:
//!
//! ```
//! extern crate rustc_serialize;
//! extern crate rubbish;
//!
//! use std::sync::Arc;
//! use std::thread;
//! use rubbish::cas::{CAS, Storage};
//! use rustc_serialize::{Decodable, Encodable};
//!
//! #[derive(Debug, RustcEncodable, RustcDecodable, PartialEq)]
//! struct Data(u32, u32);
//!
//! fn main() {
//!   let mut storage = Arc::new(Storage::new());
//!
//!   let thd = thread::spawn(move || {
//!     let hash = storage.store(&Data(10, 20)).unwrap();
//!     let result: Data = storage.retrieve(&hash).unwrap();
//!     assert_eq!(result, Data(10, 20));
//!   });
//!
//!   thd.join().unwrap();
//! }
//! ```

mod hash;
mod content;
mod storage;
mod traits;

pub use self::hash::Hash;
pub use self::storage::Storage;
pub use self::traits::CAS;

mod error;
pub use self::error::*;

// LocalStorage is for test use only
#[cfg(test)]
pub use self::storage::Storage as LocalStorage;
