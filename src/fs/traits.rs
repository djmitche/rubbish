use cas::Hash;
use super::commit::Commit;

/// A filesystem layered over content-addressible storage.
pub trait FS {
    /// Get
    fn root_commit(&self) -> Commit;
    fn get_commit(&self, hash: Hash) -> Result<Commit, String>;
}
