use super::hash::Hash;
use rustc_serialize::{Decodable, Encodable};

/// Content Addressible Storage
///
/// When values are stored in this structure, their contents are hashed and the hash
/// is returned.  The value can later be retrieved by that hash.
///
/// Content Addressible Storage is part of a cluster, and thus changes on its own, making
/// exterior mutability moot.
pub trait CAS {
    /// Store a value into the storage pool, returning its hash.
    ///
    /// Inserting the same value twice will result in the same Hash (and no additional use of
    /// space).
    fn store<T: Encodable + Decodable>(&self, value: &T) -> Hash;

    /// Retrieve a value by hash.
    fn retrieve<T: Encodable + Decodable>(&self, hash: &Hash) -> Option<T>;
}
