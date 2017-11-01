use fs::commit::Commit;
use cas::Hash;
use cas::CAS;

// TODO: use pub(crate)

/// A FileSystem encapsulates commits, trees, and so on. These objects are stored into and
/// retrieved from storage lazily (as necessary).  Reading occurs when values are requested, and
/// storage occurs when a hash is generated.
#[derive(Debug)]
pub struct FileSystem<'a, ST: 'a + CAS> {
    pub storage: &'a ST,
}

impl<'a, ST> FileSystem<'a, ST>
    where ST: 'a + CAS
{
    pub fn new(storage: &'a ST) -> FileSystem<'a, ST> {
        FileSystem { storage: storage }
    }

    /// Get the root commit -- a well-known commit with no parents and an empty tree.
    pub fn root_commit(&self) -> Commit<ST> {
        Commit::root(self)
    }

    /// Get a commit given its hash.
    ///
    /// Note that this does not actually load the commit; that occurs lazily, later.
    pub fn get_commit(&self, hash: &Hash) -> Commit<ST> {
        // this function takes a reference to the hash because it may someday cache recently used
        // commits, at which point the hash would not be consumed.
        Commit::for_hash(self, hash)
    }
}

#[cfg(test)]
mod test {
    use super::FileSystem;
    use fs::commit::Commit;
    use fs::tree::Tree;
    use cas::LocalStorage;
    use cas::Hash;

    const ROOT_HASH: &'static str = "86f8e00f8fdef1675b25f5a38abde52a7a9da0bf8506f137e32d6e3f37d88740";

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

    #[test]
    fn test_children() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        // make a grandchild of the root
        let cmt = Commit::root(&fs);
        let cmt = cmt.make_child(Tree::empty(&fs)).unwrap();
        let cmt = cmt.make_child(Tree::empty(&fs)).unwrap();

        let grandkid_hash = "d04edfc4e211330c9ec78651a026e4e63d12e38e82098f6c0c4e931a6f979dc8";
        assert_eq!(cmt.hash().unwrap(), &Hash::from_hex(grandkid_hash));

        // fetch that back..
        let cmt = Commit::for_hash(&fs, &Hash::from_hex(grandkid_hash));

        // check the parent hash
        let kid_hash = "7d134816ae341dd4cac908b4626f017412ea7d11536ad2db9ac014ff9772b129";
        assert_eq!(cmt.parents().unwrap()[0].hash().unwrap(),
                   &Hash::from_hex(kid_hash));
    }
}
