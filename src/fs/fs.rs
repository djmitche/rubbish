use fs::FS;
use fs::Commit;
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
    fn root_commit(&self) -> Commit {
        Commit::root()
    }

    fn get_commit(&self, hash: Hash) -> Result<Commit, String> {
        Commit::retrieve(self.storage, hash)
    }
}
