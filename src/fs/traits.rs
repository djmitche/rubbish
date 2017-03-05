use cas::Hash;

/// A CommitUpdater is used to create a new Commit.  Get one from
/// a Commit's `update` method, and use the [builder
/// pattern](https://doc.rust-lang.org/book/method-syntax.html#builder-pattern)
/// to make changes.
///
/// The collected updates performed with an updater are "atomic": they will all
/// be represented in a single commit.  Any read operations come from the tree
/// (including any already-completed updates), and as such are unchangable.
pub trait CommitUpdater {
    type Commit: Commit;

    fn write(&mut self, path: &[&str], data: String) -> &mut Self;
    // TODO: remove
    // TODO: read

    fn commit(self) -> Self::Commit;
}

pub trait Commit {
    type CommitUpdater: CommitUpdater;

    fn update(&self) -> Self::CommitUpdater;
}

/// A filesystem layered over content-addressible storage.
pub trait FS {
    type Commit: Commit;
    
    // TODO: doc
    fn root_commit(&self) -> Self::Commit;
    fn get_commit(&self, hash: Hash) -> Result<Self::Commit, String>;
}
