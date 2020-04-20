use log::debug;
use failure::{bail, err_msg, Fallible};
use super::hash::Hash;
use super::traits::CAS;
use super::content::Content;
use std::collections::HashMap;
use std::sync::RwLock;
use rustc_serialize::{Decodable, Encodable};

/// Type Storage provides a distributed content-addressible storage pool.  The content
/// inserted into the mechanism can be of any type implementing the `rustc_serialize`
/// traits `Decodable` and `Encodable`.
pub struct Storage(RwLock<Inner>);

pub(crate) struct Inner {
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
    fn store<T: Encodable + Decodable>(&self, value: &T) -> Fallible<Hash> {
        let mut inner = self.0.write().map_err(|_| err_msg("Lock Poisoned"))?;

        let cur_generation = inner.cur_generation;
        let content = Content::new(value)?;
        let hash = content.hash();
        debug!("store content with hash {:?}", hash);
        inner.map.insert(hash.clone(), (cur_generation, content));
        // note that we assume no hash collisions of encoded values, since this is
        // not a security-sensitive context
        Ok(hash)
    }

    fn retrieve<T: Encodable + Decodable>(&self, hash: &Hash) -> Fallible<T> {
        let inner = self.0.read().map_err(|_| err_msg("Lock Poisoned"))?;

        debug!("retrieve content with hash {:?}", hash);
        match inner.map.get(hash) {
            None => bail!("No object found"),
            Some(tup) => Ok(tup.1.decode()?),
        }
    }

    fn touch(&self, hash: &Hash) -> Fallible<()> {
        let mut inner = self.0.write().map_err(|_| err_msg("Lock Poisoned"))?;

        debug!("touch content with hash {:?}", hash);
        let cur_generation = inner.cur_generation;
        match inner.map.remove(hash) {
            None => bail!("No object found"),
            Some(tup) => {
                inner.map.insert(hash.clone(), (cur_generation, tup.1));
                Ok(())
            }
        }
    }

    fn begin_gc(&self) -> Fallible<()> {
        let mut inner = self.0.write().map_err(|_| err_msg("Lock Poisoned"))?;
        inner.cur_generation += 1;
        debug!("begin_gc: cur_generation={}", inner.cur_generation);
        Ok(())
    }

    fn end_gc(&self) {
        if let Ok(mut inner) = self.0.write() {
            inner.garbage_generation += 1;
            debug!("endn_gc: garbage_generation={}", inner.garbage_generation);
            let garbage_generation = inner.garbage_generation;

            // generate a new map containing only non-garbage
            let mut new_map = HashMap::new();
            for (k, v) in inner.map.drain() {
                if v.0 > garbage_generation {
                    new_map.insert(k, v);
                }
            }
            inner.map = new_map;
        } else {
            // locking fails only with a PoisonError, which is basically fatal,
            // but we cannot return an error right now.  So, leave the GC cycle
            // running and let the next attempt to lock produce the same error.
        }
    }
}

/*

// error-chain does not support generic errors in foreign_links, so these
// implementations will reflect a PoisonError into a cas::Error.

impl<'a> From<PoisonError<RwLockReadGuard<'a, Inner>>> for Error {
    fn from(e: PoisonError<RwLockReadGuard<'a, Inner>>) -> Self {
        ErrorKind::LockError(format!("{}", e)).into()
    }
}

impl<'a> From<PoisonError<RwLockWriteGuard<'a, Inner>>> for Error {
    fn from(e: PoisonError<RwLockWriteGuard<'a, Inner>>) -> Self {
        ErrorKind::LockError(format!("{}", e)).into()
    }
}
*/

#[cfg(test)]
mod tests {
    use super::Storage;
    use crate::cas::hash::Hash;
    use crate::cas::traits::CAS;
    use std::thread;
    use std::sync::Arc;
    use crate::util::test::init_env_logger;

    #[test]
    fn put_get_strings() {
        init_env_logger();

        let storage = Storage::new();

        let hash1 = storage.store(&"one".to_string()).unwrap();
        let hash2 = storage.store(&"two".to_string()).unwrap();
        let badhash = Hash::from_hex("000000");

        assert_eq!(
            storage.retrieve::<String>(&hash1).unwrap(),
            "one".to_string()
        );
        assert_eq!(
            storage.retrieve::<String>(&hash2).unwrap(),
            "two".to_string()
        );
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

        let hash1 = storage.store(&"abc".to_string()).unwrap();
        let hash2 = storage.store(&"def".to_string()).unwrap();
        let hash3 = storage.store(&"ghi".to_string()).unwrap();
        let hash4 = storage.store(&"jkl".to_string()).unwrap();

        storage.begin_gc().unwrap();
        storage.touch(&hash1).unwrap();
        storage.retrieve::<String>(&hash2).unwrap();
        storage.store(&"ghi".to_string()).unwrap(); // hash3
        storage.end_gc();

        // hash4 should be gone now
        assert!(storage.retrieve::<String>(&hash1).is_ok()); // touched
        assert!(storage.retrieve::<String>(&hash2).is_err()); // retrieved
        assert!(storage.retrieve::<String>(&hash3).is_ok()); // stored
        assert!(storage.retrieve::<String>(&hash4).is_err()); // not referenced
    }
}
