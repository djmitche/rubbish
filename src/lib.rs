extern crate crypto;

mod hash;

use hash::sha256;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Storage {
    map: HashMap<String, String>,
}

// TODO: generic based on a type that has the ContentHash trait
// TODO: make the hash type generic too

impl Storage {
    pub fn new() -> Storage {
        Storage {
            map: HashMap::new(),
        }
    }

    pub fn store(&mut self, content: String) -> String {
        let hash = sha256(&content);
        self.map.insert(hash.to_string(), content);
        // TODO: detect collisions :)
        return hash;
    }

    pub fn retrieve(&self, hash: &String) -> Option<&String> {
        self.map.get(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get() {
        let mut storage = Storage::new();

        let hash1 = storage.store("one".to_string());
        let hash2 = storage.store("two".to_string());

        assert_eq!(storage.retrieve(&hash1), Some(&"one".to_string()));
        assert_eq!(storage.retrieve(&hash2), Some(&"two".to_string()));
        assert_eq!(storage.retrieve(&"xxx".to_string()), None);
    }
}
