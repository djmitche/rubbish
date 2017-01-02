use super::hash::Hash;
use super::content::Content;
use std::collections::HashMap;
use rustc_serialize::{Decodable, Encodable};

/// Type LocalStorage provides a local content-addressible storage pool.  The content
/// inserted into the mechanism can be of any type implementing the `rustc_serialize`
/// traits `Decodable` and `Encodable`.
#[derive(Debug)]
pub struct LocalStorage<T: Encodable + Decodable> {
    map: HashMap<Hash, Content<T>>,
}

impl <T: Encodable + Decodable> LocalStorage<T> {
    /// Create a new, empty storage pool.
    pub fn new() -> LocalStorage<T> {
        LocalStorage {
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
    use super::LocalStorage;
    use super::super::hash::Hash;

    #[test]
    fn put_get_strings() {
        let mut storage = LocalStorage::new();

        let hash1 = storage.store(&"one".to_string());
        let hash2 = storage.store(&"two".to_string());
        let badhash = Hash::from_hex("000000");

        assert_eq!(storage.retrieve(&hash1), Some("one".to_string()));
        assert_eq!(storage.retrieve(&hash2), Some("two".to_string()));
        assert_eq!(storage.retrieve(&badhash), None);
    }

    #[test]
    fn put_twice() {
        let mut storage = super::LocalStorage::new();

        let hash1 = storage.store(&"xyz".to_string());
        let hash2 = storage.store(&"xyz".to_string());
        assert_eq!(hash1, hash2);
    }
}
