use crypto::digest::Digest;
use crypto::sha2::Sha256;
use rustc_serialize::hex::{FromHex, ToHex};
use std::fmt;

/// Type Hash represents the key under which content is stored.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, RustcDecodable, RustcEncodable)]
pub struct Hash(Vec<u8>);

impl Hash {
    /// Create a new hash, given a hex representation.
    pub fn from_hex(hex: &str) -> Hash {
        Hash(hex.from_hex().unwrap())
    }

    /// Create a new hash for the given content
    pub fn for_bytes(bytes: &Vec<u8>) -> Hash {
        let mut sha = Sha256::new();
        sha.input(bytes);
        let mut hash = Hash(vec![0; sha.output_bytes()]);
        sha.result(&mut hash.0);
        return hash;
    }

    // Get the hex representation of this hash.
    pub fn to_hex(&self) -> String {
        self.0.to_hex()
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::Hash;

    #[test]
    fn test_to_hex() {
        let hash = Hash(vec![0u8, 17, 34, 51, 68]);
        assert_eq!(hash.to_hex(), "0011223344");
    }

    #[test]
    fn test_from_hex() {
        let hash = Hash::from_hex(&"0011223344");
        assert_eq!(hash.0, vec![0u8, 17, 34, 51, 68]);
    }

    #[test]
    fn hash_bytes() {
        let hash = Hash::for_bytes(&vec![1u8, 2, 3, 4]);
        assert_eq!(
            hash.to_hex(),
            "9f64a747e1b97f131fabb6b447296c9b6f0201e79fb3c5356e6c77e89b6a806a"
        );
    }
}
