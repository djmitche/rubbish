use fs::commit::Commit;
use cas::Hash;
use cas::CAS;
use std::rc::Rc;

// TODO: use pub(crate)

/// A FileSystem encapsulates commits, trees, and so on. These objects are stored into and
/// retrieved from storage lazily (as necessary).  Reading occurs when values are requested, and
/// storage occurs when a hash is generated.
#[derive(Debug)]
pub struct FileSystem<'a, C: 'a + CAS> {
    pub storage: &'a C,
}

impl<'a, C> FileSystem<'a, C>
    where C: 'a + CAS
{
    pub fn new(storage: &'a C) -> FileSystem<'a, C> {
        FileSystem { storage: storage }
    }

    /// Get the root commit -- a well-known commit with no parents and an empty tree.
    fn root_commit(&self) -> Rc<Commit<C>> {
        Commit::root(self)
    }

    /// Get a commit given its hash.
    ///
    /// Note that this does not actually load the commit; that occurs lazily, later.
    fn get_commit(&self, hash: &Hash) -> Rc<Commit<C>> {
        // this function takes a reference to the hash because it may someday cache recently used
        // commits, at which point the hash would not be consumed.
        Commit::for_hash(self, hash)
    }
}

#[cfg(test)]
mod test {
    use super::FileSystem;
    use cas::LocalStorage;
    use cas::Hash;

    const ROOT_HASH: &'static str = "af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc";

    #[test]
    fn test_root() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        let root = fs.root_commit();
        assert_eq!(root.hash().unwrap(), &Hash::from_hex(ROOT_HASH));
        assert_eq!(root.parents().unwrap().len(), 0);
    }

    #[test]
    fn test_for_hash() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        let cmt = fs.get_commit(&Hash::from_hex("012345"));
        assert_eq!(cmt.hash().unwrap(), &Hash::from_hex("012345"));
        // there's no such object with that hash, so getting parents fails
        assert!(cmt.parents().is_err());
    }
}
