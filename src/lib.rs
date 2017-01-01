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

use hash::Hash;
use std::collections::HashMap;
use rustc_serialize::{Decodable, Encodable};
use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use crypto::digest::Digest;
use crypto::sha2::Sha256;

/// Type Content represents the encoded version of the caller's data.
#[derive(Debug, PartialEq)]
struct Content(Vec<u8>);

/// Type Storage provides a content-addressible storage pool.  The content
/// inserted into the mechanism can be of any type implementing the `rustc_serialize`
/// traits `Decodable` and `Encodable`.
#[derive(Debug)]
pub struct Storage {
    map: HashMap<Hash, Content>,
}

impl Storage {
    /// Create a new, empty storage pool.
    pub fn new() -> Storage {
        Storage {
            map: HashMap::new(),
        }
    }

    /// Insert content into the storage pool, returning the Hash pointing to the content.
    ///
    /// Inserting the same content twice will result in the same Hash (and no additional
    /// use of space).
    pub fn store<T: Encodable + Decodable>(&mut self, content: &T) -> Hash {
        let (hash, encoded) = hash_content(content);
        self.map.insert(hash.clone(), encoded);
        // TODO: detect collisions (requires copying encoded?)
        return hash;
    }

    /// Retrieve content by hash.
    ///
    /// The deserialized type must match the type used with `store()` or unpredictable
    /// results will be returned (or a panic).
    // TODO: fix that with Storage<T> somehow
    pub fn retrieve<T: Encodable + Decodable>(&self, hash: &Hash) -> Option<T> {
        match self.map.get(hash) {
            None => None,
            Some(encoded) => Some(decode_content(encoded)),
        }
    }
}

fn hash_content<T: Encodable + Decodable>(content: &T) -> (Hash, Content) {
    let encoded: Content = Content(encode(content, SizeLimit::Infinite).unwrap());

    let mut sha = Sha256::new();
    sha.input(&encoded.0[..]);

    let mut hash = Hash(vec![0; sha.output_bytes()]);
    sha.result(&mut hash.0);
    return (hash, encoded);
}

fn decode_content<T: Encodable + Decodable>(encoded: &Content) -> T {
    decode(&encoded.0).unwrap()
}

#[cfg(test)]
mod tests {
    use hash::Hash;

    #[test]
    fn put_get_strings() {
        let mut storage = super::Storage::new();

        let hash1 = storage.store(&"one".to_string());
        let hash2 = storage.store(&"two".to_string());
        let badhash = Hash(vec![0, 32]);

        assert_eq!(storage.retrieve::<String>(&hash1), Some("one".to_string()));
        assert_eq!(storage.retrieve::<String>(&hash2), Some("two".to_string()));
        assert_eq!(storage.retrieve::<String>(&badhash), None);
    }

    #[test]
    fn put_get_various_types() {
        let mut storage = super::Storage::new();

        let hash1 = storage.store(&1u32);
        let hash2 = storage.store(&0.25f64);

        assert_eq!(storage.retrieve::<u32>(&hash1), Some(1u32));
        assert_eq!(storage.retrieve::<f64>(&hash2), Some(0.25f64));
    }

    #[test]
    #[should_panic]
    fn put_get_different_types() {
        let mut storage = super::Storage::new();

        let hash = storage.store(&1u32);
        storage.retrieve::<f64>(&hash);
    }

    #[test]
    fn hash_content_of_string() {
        let (hash, encoded) = super::hash_content(&"abcd".to_string());
        println!("{:?}", hash);
        assert_eq!(hash, Hash::from_hex("9481cd49061765e353c25758440d21223df63044352cfde1775e0debc2116841"));
        assert_eq!(hash.to_hex(), "9481cd49061765e353c25758440d21223df63044352cfde1775e0debc2116841");
        assert_eq!(encoded, super::Content(vec![0u8, 0, 0, 0, 0, 0, 0, 4, 97, 98, 99, 100]));
    }

    #[test]
    fn decode_content_abcd() {
        assert_eq!(super::decode_content::<String>(&super::Content(vec![0u8, 0, 0, 0, 0, 0, 0, 4, 97, 98, 99, 100])), "abcd".to_string());
    }
}
