use cas::Hash;

pub trait Commit {}

/// A filesystem layered over content-addressible storage.
pub trait FS {
    type Commit: Commit;

    // TODO: doc
    fn root_commit(&self) -> Self::Commit;
    fn get_commit(&self, hash: Hash) -> Result<Self::Commit, String>;
}
