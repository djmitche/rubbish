use cas::error::*;
use super::hash::Hash;
use super::traits::CAS;
use super::content::Content;
use std::collections::HashMap;
use std::sync::RwLock;
use rustc_serialize::{Decodable, Encodable};

/// Type Storage provides a distributed content-addressible storage pool.  The content
/// inserted into the mechanism can be of any type implementing the `rustc_serialize`
/// traits `Decodable` and `Encodable`.
///
/// # TODO
///
/// * Actually be distributed
#[derive(Debug)]
pub struct Storage {
    map: RwLock<HashMap<Hash, Content>>,
}

impl Storage {
    /// Create a new, empty storage pool.
    pub fn new() -> Storage {
        Storage { map: RwLock::new(HashMap::new()) }
    }
}

impl CAS for Storage {
    fn store<T: Encodable + Decodable>(&self, value: &T) -> Result<Hash> {
        let content = Content::new(value)?;
        let hash = content.hash();
        self.map.write().unwrap().insert(hash.clone(), content); // XXX unwrap
        // note that we assume no hash collisions of encoded values, since this is
        // not a security-sensitive context
        Ok(hash)
    }

    fn retrieve<T: Encodable + Decodable>(&self, hash: &Hash) -> Result<T> {
        match self.map.read().unwrap().get(hash) { // XXX unwrap
            None => bail!("No object found"),
            Some(encoded) => Ok(encoded.decode()?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Storage;
    use cas::hash::Hash;
    use cas::traits::CAS;
    use std::thread;
    use std::sync::Arc;

    #[test]
    fn put_get_strings() {
        let storage = Storage::new();

        let hash1 = storage.store(&"one".to_string()).unwrap();
        let hash2 = storage.store(&"two".to_string()).unwrap();
        let badhash = Hash::from_hex("000000");

        assert_eq!(storage.retrieve::<String>(&hash1).unwrap(),
                   "one".to_string());
        assert_eq!(storage.retrieve::<String>(&hash2).unwrap(),
                   "two".to_string());
        assert!(storage.retrieve::<String>(&badhash).is_err());
    }

    #[test]
    fn test_parallel_access() {
        let storage = Arc::new(Storage::new());

        fn thd(storage: Arc<Storage>) {
            let mut hashes = vec![];
            for i in 0..100 {
                hashes.push(storage.store::<usize>(&i).unwrap());
            }
            for i in 0..100 {
                let hash = &hashes[i];
                let val = storage.retrieve::<usize>(&hash).unwrap();
                assert_eq!(val, i);
            }
        };

        let mut threads = vec![];
        for _ in 0..10 {
            let storage = storage.clone();
            threads.push(thread::spawn(move || thd(storage)));
        }

        for thd in threads {
            thd.join().unwrap();
        }
    }

    #[test]
    fn put_twice() {
        let storage = super::Storage::new();

        let hash1 = storage.store(&"xyz".to_string()).unwrap();
        let hash2 = storage.store(&"xyz".to_string()).unwrap();
        assert_eq!(hash1, hash2);
    }
}
