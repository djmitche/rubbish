use super::error::*;
use super::lazy::{LazyHashedObject, LazyContent};
use super::fs::FileSystem;
use super::tree::Tree;
use crate::cas::Hash;
use crate::cas::CAS;
use std::rc::Rc;

// TODO: use pub(crate)

/// A Commit represents the state of the filesystem, and links to previous (parent) commits.
#[derive(Debug)]
pub struct Commit<'f, ST: 'f + CAS> {
    /// The filesystem within which this commit exists
    fs: &'f FileSystem<'f, ST>,

    /// The lazily loaded data about this commit.
    inner: Rc<LazyHashedObject<'f, ST, CommitContent<'f, ST>>>,
}

/// The lazily-loaded content of a Commit.
#[derive(Debug)]
struct CommitContent<'f, ST: 'f + CAS> {
    /// Parent commits
    parents: Vec<Commit<'f, ST>>,
    tree: Tree<'f, ST>,
}

/// A raw commit, as stored in the content-addressible storage.
#[derive(Debug, RustcDecodable, RustcEncodable)]
struct RawCommit {
    parents: Vec<Hash>,
    tree: Hash,
}

impl<'f, ST> Commit<'f, ST>
where
    ST: 'f + CAS,
{
    /// Return a refcounted root commit
    pub fn root(fs: &'f FileSystem<'f, ST>) -> Commit<'f, ST> {
        let content = CommitContent {
            parents: vec![],
            tree: Tree::empty(fs),
        };
        Commit {
            fs: fs,
            inner: Rc::new(LazyHashedObject::for_content(content)),
        }
    }

    /// Return a refcounted commit for the given hash
    pub fn for_hash(fs: &'f FileSystem<'f, ST>, hash: &Hash) -> Commit<'f, ST> {
        Commit {
            fs: fs,
            inner: Rc::new(LazyHashedObject::for_hash(hash)),
        }
    }

    /// Make a new commit that is a child of this one, with the given tree
    pub fn make_child(self: Commit<'f, ST>, tree: Tree<'f, ST>) -> Result<Commit<'f, ST>> {
        let fs = self.fs;
        let content = CommitContent {
            parents: vec![self],
            tree: tree,
        };
        Ok(Commit {
            fs: fs,
            inner: Rc::new(LazyHashedObject::for_content(content)),
        })
    }

    /// Get the hash for this commit
    pub fn hash(&self) -> Result<&Hash> {
        self.inner.hash(self.fs)
    }

    /// Get the parents of this commit
    pub fn parents(&self) -> Result<&[Commit<'f, ST>]> {
        let content = self.inner.content(self.fs)?;
        Ok(&content.parents[..])
    }

    /// Get the Tree associated with this commit
    pub fn tree(&self) -> Result<Tree<'f, ST>> {
        let content = self.inner.content(self.fs)?;
        Ok(content.tree.clone())
    }
}

impl<'f, ST: 'f + CAS> Clone for Commit<'f, ST> {
    fn clone(&self) -> Self {
        Commit {
            fs: self.fs,
            inner: self.inner.clone(),
        }
    }
}

impl<'f, ST> LazyContent<'f, ST> for CommitContent<'f, ST>
where
    ST: 'f + CAS,
{
    fn retrieve_from(fs: &'f FileSystem<'f, ST>, hash: &Hash) -> Result<CommitContent<'f, ST>> {
        let raw: RawCommit = fs.storage.retrieve(hash)?;

        let mut parents: Vec<Commit<'f, ST>> = vec![];
        for h in raw.parents.iter() {
            parents.push(Commit::for_hash(fs, h));
        }

        Ok(CommitContent {
            parents: parents,
            tree: Tree::for_hash(fs, &raw.tree),
        })
    }

    fn store_in(&self, fs: &FileSystem<'f, ST>) -> Result<Hash> {
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
    use crate::fs::FileSystem;
    use crate::fs::tree::Tree;
    use crate::cas::LocalStorage;
    use crate::cas::Hash;

    const ROOT_HASH: &'static str = "86f8e00f8fdef1675b25f5a38abde52a7a9da0bf8506f137e32d6e3f37d88740";
    const EMPTY_TREE_HASH: &'static str = "3e7077fd2f66d689e0cee6a7cf5b37bf2dca7c979af356d0a31cbc5c85605c7d";

    #[test]
    fn test_root() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        let root = Commit::root(&fs);
        assert_eq!(root.hash().unwrap(), &Hash::from_hex(ROOT_HASH));
        assert_eq!(root.parents().unwrap().len(), 0);

        // reload the root hash from storage
        let root = Commit::for_hash(&fs, &Hash::from_hex(ROOT_HASH));
        assert_eq!(root.parents().unwrap().len(), 0);
        assert_eq!(
            root.tree().unwrap().hash().unwrap(),
            &Hash::from_hex(EMPTY_TREE_HASH)
        );
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

    #[test]
    fn test_make_child() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);

        let tree = Tree::empty(&fs)
            .write(&["x", "1"], "one".to_string())
            .unwrap()
            .write(&["x", "2"], "three".to_string())
            .unwrap();
        let tree_hash = tree.hash().unwrap().clone();

        let cmt = Commit::root(&fs);
        let child = cmt.make_child(tree).unwrap();

        // hash it and re-retrieve it
        let child_hash = child.hash().unwrap();
        let child = Commit::for_hash(&fs, child_hash);

        let parents = child.parents().unwrap();
        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0].hash().unwrap(), &Hash::from_hex(ROOT_HASH));
        assert_eq!(child.tree().unwrap().hash().unwrap(), &tree_hash);
    }
}
