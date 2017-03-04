use fs::FS;
use fs::commit::StoredCommit;
use fs::Object;
use cas::Hash;
use cas::CAS;

pub struct FileSystem<'a, C: 'a + CAS<Object>> {
    storage: &'a C,
}

impl<'a, C> FileSystem<'a, C>
    where C: 'a + CAS<Object>
{
    pub fn new(storage: &'a C) -> FileSystem<'a, C> {
        FileSystem {
            storage: storage,
        }
    }
}

impl<'a, C> FS for FileSystem<'a, C> 
    where C: 'a + CAS<Object>
{
    type Commit = StoredCommit<'a, C>;

    fn root_commit(&self) -> Self::Commit {
        StoredCommit::root(self.storage)
    }

    fn get_commit(&self, hash: Hash) -> Result<Self::Commit, String> {
        StoredCommit::retrieve(self.storage, hash)
    }
}
