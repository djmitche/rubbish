use super::hash::Hash;
use super::traits::{Content, CAS};
use failure::{bail, err_msg, Fallible};
use log::debug;
use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;

// TODO: is the RwLock required?

/// Type Storage provides a distributed content-addressible storage pool.  The content
/// inserted into the mechanism can be of any type implementing the `rustc_serialize`
/// traits `Decodable` and `Encodable`.
pub struct Storage(RwLock<Inner>);

#[derive(Debug)]
pub(crate) struct Inner {
    map: HashMap<Hash, (u64, Content)>,
    garbage_generation: u64,
    cur_generation: u64,
}

impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Point")
            .field("inner", &self.0.read())
            .finish()
    }
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
    fn store(&self, value: Content) -> Fallible<Hash> {
        let mut inner = self.0.write().map_err(|_| err_msg("Lock Poisoned"))?;

        let cur_generation = inner.cur_generation;
        let hash = Hash::for_bytes(&value);
        debug!("store content with hash {:?}", hash);
        inner.map.insert(hash.clone(), (cur_generation, value));
        // note that we assume no hash collisions of encoded values, since this is
        // not a security-sensitive context
        Ok(hash)
    }

    fn retrieve(&self, hash: &Hash) -> Fallible<Content> {
        let inner = self.0.read().map_err(|_| err_msg("Lock Poisoned"))?;

        debug!("retrieve content with hash {:?}", hash);
        match inner.map.get(hash) {
            None => bail!("No object found"),
            Some(tup) => Ok(tup.1.clone()),
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

#[cfg(test)]
mod tests {
    use super::Storage;
    use crate::cas::hash::Hash;
    use crate::cas::traits::CAS;
    use crate::util::test::init_env_logger;

    #[test]
    fn simple_put_get_strings() {
        init_env_logger();

        let storage = Storage::new();

        let hash1 = storage.store(b"one".to_vec()).unwrap();
        let hash2 = storage.store(b"two".to_vec()).unwrap();
        let badhash = Hash::from_hex("000000");

        assert_eq!(storage.retrieve(&hash1).unwrap(), b"one".to_vec());
        assert_eq!(storage.retrieve(&hash2).unwrap(), b"two".to_vec());
        assert!(storage.retrieve(&badhash).is_err());
    }

    #[test]
    fn put_twice() {
        let storage = super::Storage::new();

        let hash1 = storage.store(b"xyz".to_vec()).unwrap();
        let hash2 = storage.store(b"xyz".to_vec()).unwrap();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn touch() {
        let storage = super::Storage::new();

        let hash1 = storage.store(b"xyz".to_vec()).unwrap();
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

        let hash1 = storage.store(b"abc".to_vec()).unwrap();
        let hash2 = storage.store(b"def".to_vec()).unwrap();
        let hash3 = storage.store(b"ghi".to_vec()).unwrap();
        let hash4 = storage.store(b"jkl".to_vec()).unwrap();

        storage.begin_gc().unwrap();
        storage.touch(&hash1).unwrap();
        storage.retrieve(&hash2).unwrap();
        storage.store(b"ghi".to_vec()).unwrap(); // hash3
        storage.end_gc();

        // hash4 should be gone now
        assert!(storage.retrieve(&hash1).is_ok()); // touched
        assert!(storage.retrieve(&hash2).is_err()); // retrieved
        assert!(storage.retrieve(&hash3).is_ok()); // stored
        assert!(storage.retrieve(&hash4).is_err()); // not referenced
    }
}
