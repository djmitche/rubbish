use super::hash::Hash;

/// Content Addressible Storage
///
/// When values are stored in this structure, their contents are hashed and the hash
/// is returned.  The value can later be retrieved by that hash.
///
/// Content Addressible Storage is part of a cluster, and thus changes on its own, making
/// exterior mutability moot.
pub trait CAS<T> {
    /// Store a value into the storage pool, returning its hash.
    ///
    /// Inserting the same value twice will result in the same Hash (and no additional use of
    /// space).
    fn store(&self, value: &T) -> Hash;

    /// Retrieve a value by hash.
    fn retrieve(&self, hash: &Hash) -> Option<T>;
}
