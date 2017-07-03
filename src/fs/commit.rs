use fs::error::*;
use fs::lazy::{LazyHashedObject, LazyContent};
use fs::fs::FileSystem;
use fs::tree::Tree;
use cas::Hash;
use cas::CAS;
use std::cell::RefCell;
use std::rc::Rc;

// TODO: use pub(crate)

/// A Commit represents the state of the filesystem, and links to previous (parent) commits.
#[derive(Debug)]
pub struct Commit<'a, C: 'a + CAS> {
    /// The filesystem within which this commit exists
    fs: &'a FileSystem<'a, C>,

    /// The lazily loaded data about this commit.
    inner: RefCell<LazyHashedObject<'a, CommitContent<'a, C>, C>>,
}

/// The lazily-loaded content of a Commit.
#[derive(Debug)]
struct CommitContent<'a, C: 'a + CAS> {
    /// Parent commits
    parents: Vec<Rc<Commit<'a, C>>>,
    tree: Rc<Tree<'a, C>>,
}

/// A raw commit, as stored in the content-addressible storage.
#[derive(Debug, RustcDecodable, RustcEncodable)]
struct RawCommit {
    parents: Vec<Hash>,
    tree: Hash,
}

impl<'a, C> Commit<'a, C>
    where C: 'a + CAS
{
    /// Return a refcounted root commit
    pub fn root<'b>(fs: &'b FileSystem<C>) -> Rc<Commit<'b, C>> {
        let content = CommitContent {
            parents: vec![],
            tree: Tree::empty(fs),
        };
        Rc::new(Commit {
                    fs: fs,
                    inner: RefCell::new(LazyHashedObject::for_content(content)),
                })
    }

    /// Return a refcounted commit for the given hash
    pub fn for_hash<'b>(fs: &'b FileSystem<C>, hash: &Hash) -> Rc<Commit<'b, C>> {
        Rc::new(Commit {
                    fs: fs,
                    inner: RefCell::new(LazyHashedObject::for_hash(hash)),
                })
    }

    /// Make a new commit that is a child of this one, with the given tree
    pub fn make_child<'b>(fs: &'b FileSystem<C>,
                          parent: Rc<Commit<'b, C>>,
                          tree: Rc<Tree<'b, C>>)
                          -> Result<Rc<Commit<'b, C>>> {
        let content = CommitContent {
            parents: vec![parent],
            tree: tree,
        };
        Ok(Rc::new(Commit {
                       fs: fs,
                       inner: RefCell::new(LazyHashedObject::for_content(content)),
                   }))
    }

    /// Get the hash for this commit
    pub fn hash(&self) -> Result<&Hash> {
        self.inner.borrow_mut().hash(self.fs)
    }

    /// Get the parents of this commit
    pub fn parents(&'a self) -> Result<&'a [Rc<Commit<'a, C>>]> {
        let content = self.inner.borrow_mut().content(self.fs)?;
        Ok(&content.parents[..])
    }

    /// Get the Tree associated with this commit
    pub fn tree(&'a self) -> Result<Rc<Tree<'a, C>>> {
        let content = self.inner.borrow_mut().content(self.fs)?;
        Ok(content.tree.clone())
    }
}

impl<'a, C> LazyContent<'a, C> for CommitContent<'a, C>
    where C: 'a + CAS
{
    fn retrieve_from(fs: &'a FileSystem<'a, C>, hash: &Hash) -> Result<Self> {
        let raw: RawCommit = fs.storage.retrieve(hash)?;

        let mut parents: Vec<Rc<Commit<'a, C>>> = vec![];
        for h in raw.parents.iter() {
            parents.push(Commit::for_hash(fs, h));
        }

        Ok(CommitContent {
               parents: parents,
               tree: Tree::for_hash(fs, &raw.tree),
           })
    }

    fn store_in(&self, fs: &FileSystem<'a, C>) -> Result<Hash> {
        let mut parent_hashes: Vec<Hash> = vec![];
        parent_hashes.reserve(self.parents.len());
        for p in self.parents.iter() {
            let phash = p.hash()?.clone();
            parent_hashes.push(phash);
        }
        let raw = RawCommit {
            parents: parent_hashes,
            tree: self.tree.hash()?.clone(),
        };
        Ok(fs.storage.store(&raw)?)
    }
}

#[cfg(test)]
mod test {
    use super::Commit;
    use fs::FileSystem;
    use cas::LocalStorage;
    use cas::Hash;

    const ROOT_HASH: &'static str = "86f8e00f8fdef1675b25f5a38abde52a7a9da0bf8506f137e32d6e3f37d88740";

    #[test]
    fn test_root() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        let root = Commit::root(&fs);
        assert_eq!(root.hash().unwrap(), &Hash::from_hex(ROOT_HASH));
        assert_eq!(root.parents().unwrap().len(), 0);
    }

    #[test]
    fn test_for_hash() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        let cmt = Commit::for_hash(&fs, &Hash::from_hex("012345"));
        assert_eq!(cmt.hash().unwrap(), &Hash::from_hex("012345"));
        // there's no such object with that hash, so getting parents or tree fails
        assert!(cmt.parents().is_err());
        assert!(cmt.tree().is_err());
    }
}
