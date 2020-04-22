use super::content::Content;
use super::hash::Hash;
use super::traits::CAS;
use async_trait::async_trait;
use failure::{bail, err_msg, Fallible};
use log::debug;
use rustc_serialize::{Decodable, Encodable};
use std::collections::HashMap;
use std::sync::RwLock;

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

#[async_trait]
impl CAS for Storage {
    async fn store<T: Encodable + Decodable + Sync>(&self, value: &T) -> Fallible<Hash> {
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

    async fn retrieve<T: Encodable + Decodable + Sync>(&self, hash: &Hash) -> Fallible<T> {
        let inner = self.0.read().map_err(|_| err_msg("Lock Poisoned"))?;

        debug!("retrieve content with hash {:?}", hash);
        match inner.map.get(hash) {
            None => bail!("No object found"),
            Some(tup) => Ok(tup.1.decode()?),
        }
    }

    async fn touch(&self, hash: &Hash) -> Fallible<()> {
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

    #[tokio::test]
    async fn put_get_strings() {
        init_env_logger();

        let storage = Storage::new();

        let hash1 = storage.store(&"one".to_string()).await.unwrap();
        let hash2 = storage.store(&"two".to_string()).await.unwrap();
        let badhash = Hash::from_hex("000000");

        assert_eq!(
            storage.retrieve::<String>(&hash1).await.unwrap(),
            "one".to_string()
        );
        assert_eq!(
            storage.retrieve::<String>(&hash2).await.unwrap(),
            "two".to_string()
        );
        assert!(storage.retrieve::<String>(&badhash).await.is_err());
    }

    #[tokio::test]
    async fn put_twice() {
        let storage = super::Storage::new();

        let hash1 = storage.store(&"xyz".to_string()).await.unwrap();
        let hash2 = storage.store(&"xyz".to_string()).await.unwrap();
        assert_eq!(hash1, hash2);
    }

    #[tokio::test]
    async fn touch() {
        let storage = super::Storage::new();

        let hash1 = storage.store(&"xyz".to_string()).await.unwrap();
        storage.touch(&hash1).await.unwrap();
    }

    #[tokio::test]
    async fn touch_fails() {
        let storage = super::Storage::new();

        assert!(storage.touch(&Hash::from_hex("1234")).await.is_err());
    }

    #[tokio::test]
    async fn gc() {
        let storage = super::Storage::new();

        let hash1 = storage.store(&"abc".to_string()).await.unwrap();
        let hash2 = storage.store(&"def".to_string()).await.unwrap();
        let hash3 = storage.store(&"ghi".to_string()).await.unwrap();
        let hash4 = storage.store(&"jkl".to_string()).await.unwrap();

        storage.begin_gc().unwrap();
        storage.touch(&hash1).await.unwrap();
        storage.retrieve::<String>(&hash2).await.unwrap();
        storage.store(&"ghi".to_string()).await.unwrap(); // hash3
        storage.end_gc();

        // hash4 should be gone now
        assert!(storage.retrieve::<String>(&hash1).await.is_ok()); // touched
        assert!(storage.retrieve::<String>(&hash2).await.is_err()); // retrieved
        assert!(storage.retrieve::<String>(&hash3).await.is_ok()); // stored
        assert!(storage.retrieve::<String>(&hash4).await.is_err()); // not referenced
    }
}
