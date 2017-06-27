use super::hash::Hash;
use rustc_serialize::{Decodable, Encodable};
use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};

/// Type Content represents the encoded version of the caller's data.
#[derive(Debug, PartialEq)]
pub struct Content(Vec<u8>);

impl Content {
    pub fn new<T: Encodable + Decodable>(value: &T) -> Content {
        let encoded = encode(value, SizeLimit::Infinite).unwrap();
        Content(encoded)
    }

    pub fn hash(&self) -> Hash {
        Hash::for_bytes(&self.0)
    }

    pub fn decode<T: Encodable + Decodable>(&self) -> T {
        decode(&self.0).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::Content;

    #[test]
    fn encode() {
        let content = Content::new(&"abcd".to_string());
        let hash = content.hash();
        assert_eq!(hash.to_hex(),
                   "9481cd49061765e353c25758440d21223df63044352cfde1775e0debc2116841");
        assert_eq!(content,
                   Content(vec![0u8, 0, 0, 0, 0, 0, 0, 4, 97, 98, 99, 100]));
    }

    #[test]
    fn decode_content_abcd() {
        assert_eq!(Content(vec![0u8, 0, 0, 0, 0, 0, 0, 4, 97, 98, 99, 100]).decode::<String>(),
                   "abcd".to_string());
    }
}
