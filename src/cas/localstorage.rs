use super::hash::Hash;
use super::traits::ContentAddressibleStorage;
use super::content::Content;
use std::collections::HashMap;
use rustc_serialize::{Decodable, Encodable};

/// LocalStorage provides a local content-addressible storage pool.  The content inserted into the
/// mechanism can be of any type implementing the `rustc_serialize` traits `Decodable` and
/// `Encodable`.
#[derive(Debug)]
pub struct LocalStorage<T: Encodable + Decodable> {
    map: HashMap<Hash, Content<T>>,
}

impl<T: Encodable + Decodable> LocalStorage<T> {
    pub fn new() -> LocalStorage<T> {
        LocalStorage { map: HashMap::new() }
    }
}

impl<T: Encodable + Decodable> ContentAddressibleStorage<T> for LocalStorage<T> {
    fn store(&mut self, value: &T) -> Hash {
        let (hash, encoded) = Content::encode(value);
        // note that we assume no hash collisions of encoded values, since this is
        // not a security-sensitive context
        self.map.insert(hash.clone(), encoded);
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
    use super::LocalStorage;
    use super::super::hash::Hash;
    use super::super::traits::ContentAddressibleStorage;

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
