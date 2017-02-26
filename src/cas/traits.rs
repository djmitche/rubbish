use super::hash::Hash;

pub trait CAS<T> {
    /// Store a value into the storage pool, returning its hash.
    ///
    /// Inserting the same value twice will result in the same Hash (and no additional use of
    /// space).
    fn store(&mut self, value: &T) -> Hash;

    /// Retrieve a value by hash.
    fn retrieve(&self, hash: &Hash) -> Option<T>;
}
