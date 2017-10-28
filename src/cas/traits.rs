use cas::error::*;
use super::hash::Hash;
use rustc_serialize::{Decodable, Encodable};

/// Content Addressible Storage
///
/// When values are stored in this structure, their contents are hashed and the hash
/// is returned.  The value can later be retrieved by that hash.
///
/// ## Mutability
///
/// Content Addressible Storage is part of a cluster, and thus changes on its own, making
/// exterior mutability moot.
///
/// ## Garbage Collection
///
/// Garbage collection is implemented in "generations".  A garbage collection sweep begins with a
/// call to `begin_gc`.  The caller should then `touch` or `store` all non-garbage objects before
/// dropping the returned GarbageCollection instance.  Once that instance is dropped, all older
/// objects are considered garbage and may be deleted.  Note that retrieving an object does not
/// mark it as used.
///
/// This mode of garbage collection has additional benefits:
///
///  * It ensures that non-garbage objects are available locally (fetching from another node
///    if necessary)
///  * It provides a means to checkpoint storage to disk: each generation is written to a new
///    file, and once the scan is complete any previous files can be discarded.
///
/// Garbage collection runs can overlap, although this is not recommended.
pub trait CAS {
    /// Store a value into the storage pool, returning its hash.
    ///
    /// Inserting the same value twice will result in the same Hash (and no additional use of
    /// space).
    fn store<T: Encodable + Decodable>(&self, value: &T) -> Result<Hash>;

    /// Retrieve a value by hash.
    fn retrieve<T: Encodable + Decodable>(&self, hash: &Hash) -> Result<T>;

    /// Mark a value as part of the current garbage-collection generation.  This will fetch
    /// the value from another node if necessary and thus may fail.
    fn touch(&self, hash: &Hash) -> Result<()>;

    /// Begin a garbage collection round.  Before dropping the resulting `GarbageCollection`
    /// instance, `touch` or `store` all non-garbage objects.
    fn begin_gc(&self);

    /// Complete a garbage collection round.  This should be called exactly once per call
    /// to `begin_gc`.  Use `GarbageCollectionRound` to ensure this. (TODO)
    fn end_gc(&self);
}
