use super::hash::Hash;
use super::traits::ContentAddressibleStorage;
use super::content::Content;
use std::collections::HashMap;
use rustc_serialize::{Decodable, Encodable};

/// Type Storage provides a distributed content-addressible storage pool.  The content
/// inserted into the mechanism can be of any type implementing the `rustc_serialize`
/// traits `Decodable` and `Encodable`.
///
/// # TODO
///
/// * Actually be distributed
#[derive(Debug)]
pub struct Storage<T: Encodable + Decodable> {
    map: HashMap<Hash, Content<T>>,
}

impl<T: Encodable + Decodable> Storage<T> {
    /// Create a new, empty storage pool.
    pub fn new() -> Storage<T> {
        Storage { map: HashMap::new() }
    }
}

impl<T: Encodable + Decodable> ContentAddressibleStorage<T> for Storage<T> {
    fn store(&mut self, value: &T) -> Hash {
        let (hash, encoded) = Content::encode(value);
        self.map.insert(hash.clone(), encoded);
        // TODO: detect collisions (requires copying encoded?)
        return hash;
    }

    fn retrieve(&self, hash: &Hash) -> Option<T> {
        match self.map.get(hash) {
            None => None,
            Some(encoded) => Some(encoded.decode()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Storage;
    use super::super::hash::Hash;
    use super::super::traits::ContentAddressibleStorage;

    #[test]
    fn put_get_strings() {
        let mut storage = Storage::new();

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
