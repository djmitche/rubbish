use std::fmt;
use rustc_serialize::hex::{FromHex, ToHex};

/// Type Hash represents the key under which content is stored.
#[derive(PartialEq, Eq, Hash, Clone)]
pub struct Hash(pub Vec<u8>);

impl Hash {
    // Create a new hash, given a hex representation.
    pub fn from_hex(hex: &str) -> Hash {
        Hash(hex.from_hex().unwrap())
    }

    // Get the hex representation of this hash.
    pub fn to_hex(&self) -> String {
        self.0[..].to_hex()
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
}
