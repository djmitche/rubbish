use cas::error::*;
use super::hash::Hash;
use super::traits::CAS;
use super::content::Content;
use std::collections::HashMap;
use std::cell::RefCell;
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
    map: RefCell<HashMap<Hash, Content>>,
}

impl Storage {
    /// Create a new, empty storage pool.
    pub fn new() -> Storage {
        Storage { map: RefCell::new(HashMap::new()) }
    }
}

impl CAS for Storage {
    fn store<T: Encodable + Decodable>(&self, value: &T) -> Result<Hash> {
        let content = Content::new(value)?;
        let hash = content.hash();
        self.map.borrow_mut().insert(hash.clone(), content);
        // note that we assume no hash collisions of encoded values, since this is
        // not a security-sensitive context
        Ok(hash)
    }

    fn retrieve<T: Encodable + Decodable>(&self, hash: &Hash) -> Result<T> {
        match self.map.borrow().get(hash) {
            None => bail!("No object found"),
            Some(encoded) => Ok(encoded.decode()?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Storage;
    use super::super::hash::Hash;
    use super::super::traits::CAS;

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
    fn put_twice() {
        let storage = super::Storage::new();

        let hash1 = storage.store(&"xyz".to_string()).unwrap();
        let hash2 = storage.store(&"xyz".to_string()).unwrap();
        assert_eq!(hash1, hash2);
    }
}
