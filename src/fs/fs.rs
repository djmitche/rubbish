use fs::error::*;
use fs::FS;
use fs::commit::StoredCommit;
use cas::Hash;
use cas::CAS;

pub struct FileSystem<'a, C: 'a + CAS> {
    storage: &'a C,
}

impl<'a, C> FileSystem<'a, C>
    where C: 'a + CAS
{
    pub fn new(storage: &'a C) -> FileSystem<'a, C> {
        FileSystem { storage: storage }
    }
}

impl<'a, C> FS for FileSystem<'a, C>
    where C: 'a + CAS
{
    type Commit = StoredCommit<'a, C>;

    fn root_commit(&self) -> Self::Commit {
        StoredCommit::root(self.storage)
    }

    fn get_commit(&self, hash: Hash) -> Result<Self::Commit> {
        StoredCommit::retrieve(self.storage, hash)
    }
}
