//! Content-Addressible Storage
//!
//! This crate provides a content-addressible storage pool with the following characteristics:
//!
//!  * Stores arbitrary data, in an encoded format.
//!  * Does not support deletion
//!
//! # Examples
//!
//! ```
//! let mut storage = cas::Storage::new();
//! let hash = storage.store(&42u32);
//! let result : Option<u32> = storage.retrieve(&hash);
//! assert_eq!(result, Some(42u32));
//! ```

mod hash;
mod content;
mod storage;

pub use self::storage::Storage;
