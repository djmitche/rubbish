use cas::error::*;
use super::hash::Hash;
use super::gc::{GarbageCollection, GarbageCollector};
use super::traits::CAS;
use super::content::Content;
use std::collections::HashMap;
use std::sync::RwLock;
use rustc_serialize::{Decodable, Encodable};

/// Type Storage provides a distributed content-addressible storage pool.  The content
/// inserted into the mechanism can be of any type implementing the `rustc_serialize`
/// traits `Decodable` and `Encodable`.
pub struct Storage(RwLock<Inner>);

struct Inner {
    map: HashMap<Hash, (u64, Content)>,
    garbage_generation: u64,
    cur_generation: u64,
}

impl Storage {
    /// Create a new, empty storage pool.
    pub fn new() -> Storage {
        Storage(RwLock::new(Inner {
                                map: HashMap::new(),
                                garbage_generation: 0,
                                cur_generation: 1,
                            }))
    }
}

impl CAS for Storage {
    fn store<T: Encodable + Decodable>(&self, value: &T) -> Result<Hash> {
        let mut inner = self.0.write().unwrap(); // XXX unwrap
        let cur_generation = inner.cur_generation;
        let content = Content::new(value)?;
        let hash = content.hash();
        inner.map.insert(hash.clone(), (cur_generation, content));
        // note that we assume no hash collisions of encoded values, since this is
        // not a security-sensitive context
        Ok(hash)
    }

    fn retrieve<T: Encodable + Decodable>(&self, hash: &Hash) -> Result<T> {
        let inner = self.0.read().unwrap(); // XXX unwrap
        match inner.map.get(hash) {
            None => bail!("No object found"),
            Some(tup) => Ok(tup.1.decode()?),
        }
    }

    fn touch(&self, hash: &Hash) -> Result<()> {
        let inner = self.0.write().unwrap(); // XXX unwrap
        match inner.map.get(hash) {
            None => bail!("No object found"),
            Some(_) => Ok(()),
        }
    }

    fn begin_gc(&self) -> GarbageCollection {
        let mut inner = self.0.write().unwrap(); // XXX unwrap
        inner.cur_generation += 1;
        GarbageCollection::new(self)
    }
}

impl GarbageCollector for Storage {
    fn end_gc(&self) {
        let mut inner = self.0.write().unwrap(); // XXX unwrap
        inner.garbage_generation += 1;
        let garbage_generation = inner.garbage_generation;

        // generate a new map containing only non-garbage
        let mut new_map = HashMap::new();
        for (k, v) in inner.map.drain() {
            if v.0 > garbage_generation {
                new_map.insert(k, v);
            }
        }
        inner.map = new_map;
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

    #[test]
    fn touch() {
        let storage = super::Storage::new();

        let hash1 = storage.store(&"xyz".to_string()).unwrap();
        storage.touch(&hash1).unwrap();
    }

    #[test]
    fn touch_fails() {
        let storage = super::Storage::new();

        assert!(storage.touch(&Hash::from_hex("1234")).is_err());
    }

    #[test]
    fn gc() {
        let storage = super::Storage::new();

        let hash1 = storage.store(&"xyz".to_string()).unwrap();
        let hash2 = storage.store(&"xyz".to_string()).unwrap();

        let gc = storage.begin_gc();
        storage.touch(&hash1).unwrap();
        drop(gc);

        // hash2 should be gone now
        assert!(storage.retrieve::<String>(&hash2).is_err());
    }
}
