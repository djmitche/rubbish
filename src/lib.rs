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

extern crate crypto;
extern crate bincode;
extern crate rustc_serialize;

mod hash;
mod content;

use hash::Hash;
use content::Content;
use std::collections::HashMap;
use rustc_serialize::{Decodable, Encodable};

/// Type Storage provides a content-addressible storage pool.  The content
/// inserted into the mechanism can be of any type implementing the `rustc_serialize`
/// traits `Decodable` and `Encodable`.
#[derive(Debug)]
pub struct Storage<T: Encodable + Decodable> {
    map: HashMap<Hash, Content<T>>,
}

impl <T: Encodable + Decodable> Storage<T> {
    /// Create a new, empty storage pool.
    pub fn new() -> Storage<T> {
        Storage {
            map: HashMap::new(),
        }
    }

    /// Insert content into the storage pool, returning the Hash pointing to the content.
    ///
    /// Inserting the same content twice will result in the same Hash (and no additional
    /// use of space).
    pub fn store(&mut self, value: &T) -> Hash {
        let (hash, encoded) = Content::encode(value);
        self.map.insert(hash.clone(), encoded);
        // TODO: detect collisions (requires copying encoded?)
        return hash;
    }

    /// Retrieve content by hash.
    pub fn retrieve(&self, hash: &Hash) -> Option<T> {
        match self.map.get(hash) {
            None => None,
            Some(encoded) => Some(encoded.decode()),
        }
    }
}

#[cfg(test)]
mod tests {
    use hash::Hash;

    #[test]
    fn put_get_strings() {
        let mut storage = super::Storage::new();

        let hash1 = storage.store(&"one".to_string());
        let hash2 = storage.store(&"two".to_string());
        let badhash = Hash::from_hex("000000");

        assert_eq!(storage.retrieve(&hash1), Some("one".to_string()));
        assert_eq!(storage.retrieve(&hash2), Some("two".to_string()));
        assert_eq!(storage.retrieve(&badhash), None);
    }

    #[test]
    fn put_twice() {
        let mut storage = super::Storage::new();

        let hash1 = storage.store(&"xyz".to_string());
        let hash2 = storage.store(&"xyz".to_string());
        assert_eq!(hash1, hash2);
    }
}
