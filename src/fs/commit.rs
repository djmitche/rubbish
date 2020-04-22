use super::content::Content;
use super::fs::FileSystem;
use super::lazy::LazyHashedObject;
use super::tree::Tree;
use crate::cas::Hash;
use failure::Fallible;
use std::rc::Rc;

// TODO: use pub(crate)

/// A Commit represents the state of the filesystem, and links to previous (parent) commits.
#[derive(Debug, Clone)]
pub struct Commit {
    /// The lazily loaded data about this commit.
    inner: Rc<LazyHashedObject<Content>>,
}

impl Commit {
    /// Return a root commit
    pub fn root(fs: &FileSystem) -> Fallible<Commit> {
        let content = Content::Commit {
            parents: vec![],
            tree: Tree::empty().hash(fs)?.clone(),
        };
        Ok(Commit {
            inner: Rc::new(LazyHashedObject::for_content(content)),
        })
    }

    /// Return a commit for the given hash
    pub fn for_hash(hash: &Hash) -> Commit {
        Commit {
            inner: Rc::new(LazyHashedObject::for_hash(hash)),
        }
    }

    /// Make a new commit that is a child of this one, with the given tree
    pub fn make_child(&self, fs: &FileSystem, tree: &Tree) -> Fallible<Commit> {
        let content = Content::Commit {
            parents: vec![self.hash(fs)?.clone()],
            tree: tree.hash(fs)?.clone(),
        };
        Ok(Commit {
            inner: Rc::new(LazyHashedObject::for_content(content)),
        })
    }

    /// Get the hash for this commit
    pub fn hash(&self, fs: &FileSystem) -> Fallible<&Hash> {
        self.inner.hash(fs)
    }

    /// Get the parents of this commit
    pub fn parents(&self, fs: &FileSystem) -> Fallible<Vec<Commit>> {
        let content = self.inner.content(fs)?;
        if let Content::Commit { parents, tree: _ } = content {
            Ok(parents[..].iter().map(|h| Commit::for_hash(&h)).collect())
        } else {
            panic!("{:?} is not a commit", self.inner.hash(fs).unwrap());
        }
    }

    /// Get the Tree associated with this commit
    pub fn tree(&self, fs: &FileSystem) -> Fallible<Tree> {
        let content = self.inner.content(fs)?;
        if let Content::Commit { parents: _, tree } = content {
            Ok(Tree::for_hash(&tree))
        } else {
            panic!("{:?} is not a commit", self.inner.hash(fs).unwrap());
        }
    }
}

#[cfg(test)]
mod test {
    use super::Commit;
    use crate::cas::Hash;
    use crate::cas::LocalStorage;
    use crate::fs::hashes::{EMPTY_TREE_HASH, ROOT_HASH};
    use crate::fs::tree::Tree;
    use crate::fs::FileSystem;

    #[test]
    fn test_root() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        let root = Commit::root(&fs).unwrap();
        assert_eq!(root.hash(&fs).unwrap(), &Hash::from_hex(ROOT_HASH));
        assert_eq!(root.parents(&fs).unwrap().len(), 0);

        // reload the root hash from storage
        let root = Commit::for_hash(&Hash::from_hex(ROOT_HASH));
        assert_eq!(root.parents(&fs).unwrap().len(), 0);

        assert_eq!(
            root.tree(&fs).unwrap().hash(&fs).unwrap(),
            &Hash::from_hex(EMPTY_TREE_HASH)
        );
    }

    #[test]
    fn test_for_hash() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        let cmt = Commit::for_hash(&Hash::from_hex("012345"));
        assert_eq!(cmt.hash(&fs).unwrap(), &Hash::from_hex("012345"));
        // there's no such object with that hash, so getting parents or tree fails
        assert!(cmt.parents(&fs).is_err());
        assert!(cmt.tree(&fs).is_err());
    }

    #[test]
    fn test_make_child() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        let tree = Tree::empty()
            .write(&fs, &["x", "1"], vec![1])
            .unwrap()
            .write(&fs, &["x", "2"], vec![2, 2, 2])
            .unwrap();
        let tree_hash = tree.hash(&fs).unwrap().clone();

        let cmt = Commit::root(&fs).unwrap();
        let child = cmt.make_child(&fs, &tree).unwrap();

        // hash it and re-retrieve it
        let child_hash = child.hash(&fs).unwrap();
        let child = Commit::for_hash(child_hash);

        let parents = child.parents(&fs).unwrap();
        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0].hash(&fs).unwrap(), &Hash::from_hex(ROOT_HASH));
        assert_eq!(child.tree(&fs).unwrap().hash(&fs).unwrap(), &tree_hash);
    }
}
