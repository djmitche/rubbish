extern crate crypto;
extern crate serde;
extern crate bincode;

use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use bincode::{SizeLimit};
use bincode::serde::{serialize, deserialize};
use crypto::digest::Digest;
use crypto::sha2::Sha256;

#[derive(Debug)]
pub struct Storage {
    map: HashMap<String, Vec<u8>>,
}

// TODO: use byte vector for hashes, too

impl Storage {
    pub fn new() -> Storage {
        Storage {
            map: HashMap::new(),
        }
    }

    pub fn store<T: Serialize + Deserialize>(&mut self, content: &T) -> String {
        let (hash, encoded) = hash_content(content);
        self.map.insert(hash.to_string(), encoded);
        // TODO: detect collisions (requires copying encoded?)
        return hash;
    }

    pub fn retrieve<T: Serialize + Deserialize>(&self, hash: &String) -> Option<T> {
        match self.map.get(hash) {
            None => None,
            Some(encoded) => Some(decode_content(encoded)),
        }
    }
}

fn hash_content<T: Serialize + Deserialize>(content: &T) -> (String, Vec<u8>) {
    let encoded: Vec<u8> = serialize(content, SizeLimit::Infinite).unwrap();

    let mut sha = Sha256::new();
    sha.input(&encoded[..]);
    return (sha.result_str(), encoded);
}

fn decode_content<T: Serialize + Deserialize>(encoded: &Vec<u8>) -> T {
    deserialize(encoded).unwrap()
}

#[cfg(test)]
mod tests {
    #[test]
    fn put_get_strings() {
        let mut storage = super::Storage::new();

        let hash1 = storage.store(&"one".to_string());
        let hash2 = storage.store(&"two".to_string());

        assert_eq!(storage.retrieve::<String>(&hash1), Some("one".to_string()));
        assert_eq!(storage.retrieve::<String>(&hash2), Some("two".to_string()));
        assert_eq!(storage.retrieve::<String>(&"xxx".to_string()), None);
    }

    #[test]
    fn put_get_various_types() {
        let mut storage = super::Storage::new();

        let hash1 = storage.store(&1u32);
        let hash2 = storage.store(&0.25f64);

        assert_eq!(storage.retrieve::<u32>(&hash1), Some(1u32));
        assert_eq!(storage.retrieve::<f64>(&hash2), Some(0.25f64));
    }

    #[test]
    #[should_panic]
    fn put_get_different_types() {
        let mut storage = super::Storage::new();

        let hash = storage.store(&1u32);
        storage.retrieve::<f64>(&hash);
    }

    #[test]
    fn hash_content_of_string() {
        let (hash, encoded) = super::hash_content(&"abcd".to_string());
        assert_eq!(hash, "9481cd49061765e353c25758440d21223df63044352cfde1775e0debc2116841");
        assert_eq!(encoded, vec![0u8, 0, 0, 0, 0, 0, 0, 4, 97, 98, 99, 100]);
    }

    #[test]
    fn decode_content_abcd() {
        assert_eq!(super::decode_content::<String>(&vec![0u8, 0, 0, 0, 0, 0, 0, 4, 97, 98, 99, 100]), "abcd".to_string());
    }
}
