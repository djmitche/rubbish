use super::hash::Hash;
use bincode::rustc_serialize::{decode, encode};
use bincode::SizeLimit;
use failure::Fallible;
use rustc_serialize::{Decodable, Encodable};

/// Type Content represents the encoded version of the caller's data.
#[derive(Debug, PartialEq)]
pub struct Content(Vec<u8>);

impl Content {
    pub fn new<T: Encodable + Decodable>(value: &T) -> Fallible<Content> {
        let encoded = encode(value, SizeLimit::Infinite)?;
        Ok(Content(encoded))
    }

    pub fn hash(&self) -> Hash {
        Hash::for_bytes(&self.0)
    }

    pub fn decode<T: Encodable + Decodable>(&self) -> Fallible<T> {
        Ok(decode(&self.0)?)
    }
}

#[cfg(test)]
mod tests {
    use super::Content;

    #[test]
    fn encode() {
        let content = Content::new(&"abcd".to_string()).unwrap();
        let hash = content.hash();
        assert_eq!(
            hash.to_hex(),
            "9481cd49061765e353c25758440d21223df63044352cfde1775e0debc2116841"
        );
        assert_eq!(
            content,
            Content(vec![0u8, 0, 0, 0, 0, 0, 0, 4, 97, 98, 99, 100])
        );
    }

    #[test]
    fn decode_content_abcd() {
        assert_eq!(
            Content(vec![0u8, 0, 0, 0, 0, 0, 0, 4, 97, 98, 99, 100])
                .decode::<String>()
                .unwrap(),
            "abcd".to_string()
        );
    }

    #[test]
    fn decode_content_invalid() {
        assert!(Content(vec![0u8, 159]).decode::<String>().is_err());
    }
}
