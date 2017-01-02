//! Content-Addressible Storage
//!
//! This module provides a content-addressible storage pool with the following characteristics:
//!
//!  * Stores arbitrary data, in an encoded format.
//!  * Does not support deletion
//!
//! Its API is in the `ContentAddressibleStorage` trait.
//!
//! # Examples
//!
//! ```
//! use rubbish::cas::ContentAddressibleStorage;
//! let mut storage = rubbish::cas::Storage::new();
//! let hash = storage.store(&42u32);
//! let result : Option<u32> = storage.retrieve(&hash);
//! assert_eq!(result, Some(42u32));
//! ```

mod hash;
mod content;
mod storage;
mod traits;

pub use self::storage::Storage;
pub use self::traits::ContentAddressibleStorage;

// LocalStorage is for test use only
#[cfg(test)]
mod localstorage;
#[cfg(test)]
pub use self::localstorage::LocalStorage;
