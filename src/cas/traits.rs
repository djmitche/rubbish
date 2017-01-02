use super::hash::Hash;

pub trait ContentAddressibleStorage<T> {
    /// Insert content into the storage pool, returning the Hash pointing to the content.
    ///
    /// Inserting the same content twice will result in the same Hash (and no additional
    /// use of space).
    fn store(&mut self, value: &T) -> Hash;

    /// Retrieve content by hash.
    fn retrieve(&self, hash: &Hash) -> Option<T>;
}
