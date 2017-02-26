//! `cas` -- a distributed, content-addressible, in-memory storage system.  The
//! system uses a "gossip"-style protocol to ensure that all participants have
//! all content, and supports generational garbage collection and persistence
//! to disk.
//!
//! The API is in the `CAS` trait.
//!
//! # Examples
//!
//! ```
//! use rubbish::cas::CAS;
//! let mut storage = rubbish::cas::Storage::new();
//! let hash = storage.store(&42u32);
//! let result : Option<u32> = storage.retrieve(&hash);
//! assert_eq!(result, Some(42u32));
//! ```

mod hash;
mod content;
mod storage;
mod traits;

pub use self::hash::Hash;
pub use self::storage::Storage;
pub use self::traits::CAS;

// LocalStorage is for test use only
#[cfg(test)]
mod localstorage;
#[cfg(test)]
pub use self::localstorage::LocalStorage;
