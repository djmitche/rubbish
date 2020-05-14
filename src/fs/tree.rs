use super::content::Content;
use super::fs::FileSystem;
use super::lazy::LazyHashedObject;
use crate::cas::Hash;
use failure::Fallible;
use std::collections::HashMap;
use std::fmt::{Debug, Error as FmtError, Formatter};
use std::rc::Rc;
use std::result::Result as StdResult;

/// A Tree represents an image of a tree-shaped data structure, sort of like a filesystem directoy.
/// However, directories can have associated data (that is, there can be data at `foo/bar` and at
/// `foo/bar/bing`).
#[derive(Clone)]
pub struct Tree {
    /// The lazily loaded data about this commit.
    inner: Rc<LazyHashedObject<Content>>,
}

impl Tree {
    /// Create a new, empty tree
    pub fn empty() -> Tree {
        Tree::for_content(Content::Tree {
            data: None,
            children: HashMap::new(),
        })
    }

    /// Return a Tree for the given hash
    pub fn for_hash(hash: &Hash) -> Tree {
        Tree {
            inner: Rc::new(LazyHashedObject::for_hash(hash)),
        }
    }

    /// return a Tree for the given TreeContent
    fn for_content(content: Content) -> Tree {
        Tree {
            inner: Rc::new(LazyHashedObject::for_content(content)),
        }
    }

    /// Get the hash for this tree
    pub fn hash(&self, fs: &FileSystem) -> Fallible<&Hash> {
        self.inner.hash(fs)
    }

    /// Utility function to get the content or panic trying
    fn content(&self, fs: &FileSystem) -> Fallible<(&Option<Vec<u8>>, &HashMap<String, Hash>)> {
        let content = self.inner.content(fs)?;
        if let Content::Tree { data, children } = content {
            Ok((data, children))
        } else {
            panic!("{:?} is not a tree", self.inner.hash(fs).unwrap());
        }
    }

    /// Get the children of this tree.
    pub fn children(&self, fs: &FileSystem) -> Fallible<HashMap<String, Tree>> {
        let (_, children) = self.content(fs)?;
        Ok(children
            .iter()
            .map(|(n, h)| (n.clone(), Tree::for_hash(h)))
            .collect())
    }

    /// Get a child of this tree, if it exists
    pub fn child(&self, fs: &FileSystem, name: &str) -> Fallible<Option<Tree>> {
        let (_, children) = self.content(fs)?;
        if let Some(h) = children.get(name) {
            Ok(Some(Tree::for_hash(h)))
        } else {
            Ok(None)
        }
    }

    // TODO: iter_children

    /// Get the data at this tree.
    pub fn data(&self, fs: &FileSystem) -> Fallible<Option<Vec<u8>>> {
        let (data, _) = self.content(fs)?;
        Ok(data.clone())
    }

    /// Return a tree containing new value at the designated path, replacing any
    /// existing value at that path.  The storage is used to read any unresolved
    /// tree nodes, but nothing is written to storage.
    ///
    /// Note that path elements and data can coexist, unlike a UNIX filesystem; that is, writing a
    /// value to "usr/bin" will not invalidate paths like "usr/bin/rustc".
    ///
    /// Writing uses path copying to copy a minimal amount of tree data such that the
    /// original tree is not modified and a new tree is returned, sharing data where
    /// possible.
    pub fn write(&self, fs: &FileSystem, path: &[&str], data: Vec<u8>) -> Fallible<Tree> {
        self.modify(fs, path, Some(data))
    }

    /// Return a tree with the value at the given path removed.  Empty directories will
    /// be removed.  The storage is used to read any unresolved tree nodes, but nothing is
    /// written to storage.  If the path is already missing, an unchanged copy of the
    /// tree is returned.
    ///
    /// This operation uses path copying to copy a minimal amount of tree data such that the
    /// original tree is not modified and a new tree is returned, sharing data where
    /// possible.
    pub fn remove(self, fs: &FileSystem, path: &[&str]) -> Fallible<Tree> {
        self.modify(fs, path, None)
    }

    /// Read the value at the given path in this tree, if it is set.
    pub fn read(&self, fs: &FileSystem, path: &[&str]) -> Fallible<Option<Vec<u8>>> {
        if path.len() > 0 {
            match self.children(fs)?.get(path[0]) {
                None => Ok(None),
                Some(ref sub) => sub.read(fs, &path[1..]),
            }
        } else {
            Ok(self.data(fs)?)
        }
    }

    /// Set the data at the given path, returning a new Tree that shares some nodes with the
    /// original via path copying.
    fn modify(&self, fs: &FileSystem, path: &[&str], newdata: Option<Vec<u8>>) -> Fallible<Tree> {
        // Begin by iterating along the path, generating a vec of Option<Tree> containing existing
        // trees along that path, or None where no such tree exists.  Trees[i] is the tree at
        // path[..i], so trees[0] is the root.
        let mut trees = vec![Some(self.clone())];
        for elt in path {
            if let Some(ref t) = trees[trees.len() - 1] {
                if let Some(c) = t.child(fs, elt)? {
                    trees.push(Some(c));
                } else {
                    trees.push(None);
                }
            } else {
                trees.push(None);
            }
        }

        if let Some(newdata) = newdata {
            // we are adding data, so write that data in subtree
            let subtree = trees.pop().unwrap();
            let mut subtree = if let Some(ref st) = subtree {
                let (_, children) = st.content(fs)?;
                Tree::for_content(Content::Tree {
                    data: Some(newdata),
                    children: children.clone(),
                })
            } else {
                Tree::for_content(Content::Tree {
                    data: Some(newdata),
                    children: HashMap::new(),
                })
            };

            // then work backward, updating trees along the way
            for (elt, mut tree) in path.iter().zip(trees.drain(..)).rev() {
                if let Some(t) = tree.take() {
                    // create a clone of t with subtree as a child
                    let (data, children) = t.content(fs)?;
                    let (data, mut children) = (data.clone(), children.clone());
                    children.insert(elt.to_string(), subtree.hash(fs)?.clone());
                    subtree = Tree::for_content(Content::Tree { data, children });
                } else {
                    // create a new tree with subtree as child
                    let mut children = HashMap::new();
                    children.insert(elt.to_string(), subtree.hash(fs)?.clone());
                    subtree = Tree::for_content(Content::Tree {
                        data: None,
                        children,
                    });
                }
            }

            Ok(subtree)
        } else {
            // newdata is None so we are deleting data; start by deleting the data from the leaf
            let mut subtree = trees.pop().unwrap();
            if let Some(ref st) = subtree {
                let (_, children) = st.content(fs)?;
                if children.len() > 0 {
                    subtree = Some(Tree::for_content(Content::Tree {
                        data: None,
                        children: children.clone(),
                    }))
                } else {
                    // this leaf node is now empty, so drop it
                    subtree = None
                }
            } else {
                // no data here to delete -- no change
                return Ok(self.clone());
            }
            println!("modified subtree={:?}", subtree);

            // and now work backward, deleting empty trees
            for (elt, mut tree) in path.iter().zip(trees.drain(..)).rev() {
                match (subtree.take(), tree.take()) {
                    (Some(st), Some(t)) => {
                        // create a clone of t with st as a child
                        let (data, children) = t.content(fs)?;
                        let (data, mut children) = (data.clone(), children.clone());
                        children.insert(elt.to_string(), st.hash(fs)?.clone());
                        subtree = Some(Tree::for_content(Content::Tree { data, children }));
                    }
                    (Some(st), None) => {
                        // create a new tree with st as child
                        let mut children = HashMap::new();
                        children.insert(elt.to_string(), st.hash(fs)?.clone());
                        subtree = Some(Tree::for_content(Content::Tree {
                            data: None,
                            children,
                        }));
                    }
                    (None, Some(t)) => {
                        // create a clone of t with elt removed, or None if t only contains elt
                        let (data, children) = t.content(fs)?;
                        if children.len() == 1 && children.keys().next().unwrap() == elt {
                            subtree = None;
                        } else {
                            let (data, mut children) = (data.clone(), children.clone());
                            children.remove(&elt[..]);
                            subtree = Some(Tree::for_content(Content::Tree { data, children }));
                        }
                    }
                    (None, None) => {
                        // new subtree is still None
                    }
                };
            }

            if let Some(subtree) = subtree.take() {
                Ok(subtree)
            } else {
                Ok(Tree::empty())
            }
        }
    }
}

impl Debug for Tree {
    fn fmt(&self, f: &mut Formatter) -> StdResult<(), FmtError> {
        write!(f, "Tree")?;
        if let Some(h) = self.inner.maybe_hash() {
            write!(f, "@{:?}", h)?;
        }
        if let Some(c) = self.inner.maybe_content() {
            if let Content::Tree { data, children } = c {
                write!(f, " [{:?}", data)?;
                let mut names: Vec<&String> = children.keys().collect();
                names.sort();
                for name in names.drain(..) {
                    write!(f, ", {}: {:?}", name, children.get(name).unwrap())?;
                }
                write!(f, "]")?;
            } else {
                write!(f, "(not a tree!)")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::FileSystem;
    use super::*;
    use crate::cas::Hash;
    use crate::cas::LocalStorage;
    use crate::fs::hashes::EMPTY_TREE_HASH;

    #[test]
    fn test_empty() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        let empty = Tree::empty();
        assert_eq!(empty.hash(&fs).unwrap(), &Hash::from_hex(EMPTY_TREE_HASH));
        assert_eq!(empty.children(&fs).unwrap().len(), 0);
        assert!(empty.data(&fs).unwrap().is_none())
    }

    #[test]
    fn test_for_hash() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        let tree = Tree::for_hash(&Hash::from_hex("012345"));
        assert_eq!(tree.hash(&fs).unwrap(), &Hash::from_hex("012345"));
        // there's no such object with that hash, so getting children or data fails
        assert!(tree.children(&fs).is_err());
        assert!(tree.data(&fs).is_err());
    }

    fn make_test_tree(fs: &FileSystem) -> Tree {
        let mut rv = Tree::empty();
        rv = rv.write(fs, &["sub", "one"], vec![1]).unwrap();
        rv = rv.write(fs, &["sub", "two"], vec![2]).unwrap();
        rv = rv.write(fs, &["three"], vec![3]).unwrap();
        rv
    }

    fn dump_tree(tree: &Tree, fs: &FileSystem, prefix: &str) {
        let (_, children) = tree.content(&fs).unwrap();
        println!("{}: {:?}", prefix, tree);
        for (name, tree) in children.iter() {
            dump_tree(&Tree::for_hash(tree), fs, &format!("{}.{}", prefix, name));
        }
    }

    #[test]
    fn test_write_and_read() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        let sub_hash =
            Hash::from_hex("66e724410d6ddb17259a949475931a12790b5b0b5414f979074c6f5b9d0fa331");
        let three_hash =
            Hash::from_hex("d37516d409d79a5427f10f75b94e56f9bf8016d68938c6567e3a421c57559955");

        let tree = make_test_tree(&fs);
        assert_eq!(
            format!("{:?}", tree),
            format!("Tree [None, sub: {}, three: {}]", sub_hash, three_hash)
        );

        let hash = tree.hash(&fs).unwrap();
        let tree = Tree::for_hash(hash);
        assert_eq!(format!("{:?}", tree), format!("Tree@{}", hash));

        tree.read(&fs, &["sub"]).unwrap();
        assert_eq!(
            format!("{:?}", tree),
            format!(
                "Tree@{} [None, sub: {}, three: {}]",
                hash, sub_hash, three_hash
            )
        );
    }

    #[test]
    fn read_exists() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        let tree = make_test_tree(&fs);
        assert_eq!(tree.read(&fs, &["three"]).unwrap(), Some(vec![3]));
    }

    #[test]
    fn read_exists_from_storage() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        // create the tree and write it to storage by getting its hash
        let tree = make_test_tree(&fs);
        let hash = tree.hash(&fs).unwrap();

        // then re-load it from the storage
        let tree = Tree::for_hash(&hash);
        assert_eq!(tree.read(&fs, &["sub", "two"]).unwrap(), Some(vec![2]));
    }

    #[test]
    fn read_not_found() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        let tree = make_test_tree(&fs);
        assert_eq!(tree.read(&fs, &[]).unwrap(), None);
        assert_eq!(tree.read(&fs, &["notathing"]).unwrap(), None);
        assert_eq!(tree.read(&fs, &["sub", "sub2", "sub"]).unwrap(), None);
        // "sub" exists but there's no data there
        assert_eq!(tree.read(&fs, &["sub"]).unwrap(), None);
    }

    #[test]
    fn test_overwrite() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        let tree = Tree::empty();
        let tree = tree.write(&fs, &["foo", "bar"], vec![1]).unwrap();

        // reload and write a new value
        let tree = Tree::for_hash(tree.hash(&fs).unwrap());
        let tree = tree.write(&fs, &["foo", "bar"], vec![2]).unwrap();

        assert_eq!(tree.read(&fs, &["foo", "bar"]).unwrap(), Some(vec![2]));
        assert_eq!(
            tree.hash(&fs).unwrap(),
            &Hash::from_hex("bf6d7abc6060919b29a08149d1fe4c2cb2d18f109b022c5ed0d18ac07ed48aca",)
        );
    }

    #[test]
    fn remove_nonexistent() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        let tree = make_test_tree(&fs);
        let tree = tree.remove(&fs, &["sub", "no", "such", "value"]).unwrap();

        let tree = Tree::for_hash(tree.hash(&fs).unwrap());

        assert_eq!(
            tree.read(&fs, &["sub", "no", "such", "value"]).unwrap(),
            None
        );
    }

    #[test]
    fn remove_leaf() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        let tree = make_test_tree(&fs);
        let tree = tree.remove(&fs, &["sub", "one"]).unwrap();

        let tree = Tree::for_hash(tree.hash(&fs).unwrap());

        assert_eq!(tree.read(&fs, &["sub", "one"]).unwrap(), None);
    }

    #[test]
    fn remove_deep_from_storage() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));

        let tree = Tree::empty()
            .write(&fs, &["a", "b", "c", "d"], vec![1, 2, 3])
            .unwrap();
        let tree = Tree::for_hash(tree.hash(&fs).unwrap());
        let tree = tree.remove(&fs, &["a", "b", "c", "d"]).unwrap();
        dump_tree(&tree, &fs, "tree");

        // should trim empty directories, so this should be empty
        assert_eq!(tree.hash(&fs).unwrap(), &Hash::from_hex(EMPTY_TREE_HASH));
    }
}
