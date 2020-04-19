use super::error::*;
use super::lazy::{LazyHashedObject, LazyContent};
use super::fs::FileSystem;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter, Error as FmtError};
use std::result::Result as StdResult;
use crate::cas::Hash;
use crate::cas::CAS;
use std::rc::Rc;

/// A Tree represents an image of a tree-shaped data structure, sort of like a filesystem directoy.
/// However, directories can have associated data (that is, there can be data at `foo/bar` and at
/// `foo/bar/bing`).
pub struct Tree<'f, ST: 'f + CAS>
where
    ST: 'f + CAS,
{
    /// The filesystem within which this Tree exists
    fs: &'f FileSystem<'f, ST>,

    /// The lazily loaded data about this commit.
    inner: Rc<LazyHashedObject<'f, ST, TreeContent<'f, ST>>>,
}

/// The lazily-loaded content of a tree
#[derive(Debug)]
struct TreeContent<'f, ST: 'f + CAS> {
    data: Option<String>,
    children: HashMap<String, Tree<'f, ST>>,
}

/// A raw tree, as stored in the content-addressible storage.
#[derive(Debug, RustcDecodable, RustcEncodable)]
struct RawTree {
    data: Option<String>,
    children: Vec<(String, Hash)>,
}


impl<'f, ST: 'f + CAS> Tree<'f, ST> {
    /// Return a Tree for the given hash
    pub fn for_hash(fs: &'f FileSystem<'f, ST>, hash: &Hash) -> Tree<'f, ST> {
        Tree {
            fs: fs,
            inner: Rc::new(LazyHashedObject::for_hash(hash)),
        }
    }

    /// return a Tree for the given TreeContent
    fn for_content(fs: &'f FileSystem<ST>, content: TreeContent<'f, ST>) -> Tree<'f, ST> {
        Tree {
            fs: fs,
            inner: Rc::new(LazyHashedObject::for_content(content)),
        }
    }

    /// Create a new, empty tree
    pub fn empty(fs: &'f FileSystem<ST>) -> Tree<'f, ST> {
        Tree::for_content(
            fs,
            TreeContent {
                data: None,
                children: HashMap::new(),
            },
        )
    }

    /// Get the hash for this tree
    pub fn hash(&self) -> Result<&Hash> {
        self.inner.hash(self.fs)
    }

    /// Get the children of this tree.
    pub fn children(&self) -> Result<&HashMap<String, Tree<'f, ST>>> {
        let content = self.inner.content(self.fs)?;
        Ok(&content.children)
    }

    /// Get the data at this tree.
    pub fn data(&self) -> Result<Option<&str>> {
        let content = self.inner.content(self.fs)?;
        Ok(match content.data {
            None => None,
            Some(ref s) => Some(s),
        })
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
    pub fn write(&self, path: &[&str], data: String) -> Result<Tree<'f, ST>> {
        self.modify(path, Some(data))
    }

    /// Return a tree with the value at the given path removed.  Empty directories will
    /// be removed.  The storage is used to read any unresolved tree nodes, but nothing is
    /// written to storage.  If the path is already missing, an unchanged copy of the
    /// tree is returned.
    ///
    /// This operation uses path copying to copy a minimal amount of tree data such that the
    /// original tree is not modified and a new tree is returned, sharing data where
    /// possible.
    pub fn remove(self, path: &[&str]) -> Result<Tree<'f, ST>> {
        self.modify(path, None)
    }

    /// Read the value at the given path in this tree, if it is set.
    pub fn read(&self, path: &[&str]) -> Result<Option<&str>> {
        if path.len() > 0 {
            match self.children()?.get(path[0]) {
                None => Ok(None),
                Some(ref sub) => sub.read(&path[1..]),
            }
        } else {
            self.data()
        }
    }

    /// Set the data at the given path, returning a new Tree that shares some nodes with the
    /// original via path copying.
    fn modify(&self, path: &[&str], data: Option<String>) -> Result<Tree<'f, ST>> {
        if path.len() > 0 {
            let elt = path[0];
            let mut newchildren = self.children()?.clone();
            let subtree = match newchildren.get(elt) {
                // TODO: this could be a lot more efficient than creating an empty subtree and
                // modifying it
                None => Tree::empty(self.fs).modify(&path[1..], data)?,
                Some(ref sub) => sub.modify(&path[1..], data)?,
            };

            // only keep non-empty subtrees
            if subtree.data()?.is_some() || subtree.children()?.len() > 0 {
                newchildren.insert(elt.to_string(), subtree);
            } else {
                if newchildren.get(elt).is_some() {
                    newchildren.remove(elt);
                }
            }

            let content = TreeContent {
                data: match self.data()? {
                    None => None,
                    Some(s) => Some(s.to_string()),
                },
                children: newchildren,
            };
            return Ok(Tree::for_content(self.fs, content));
        } else {
            let changed = match self.data()? {
                None => !data.is_none(),
                Some(ref oldvalue) => {
                    match data {
                        None => true,
                        Some(ref newvalue) => oldvalue != newvalue,
                    }
                }
            };

            if changed {
                // create a new tree containing this data
                let content = TreeContent {
                    data: data,
                    children: self.children()?.clone(),
                };
                return Ok(Tree::for_content(self.fs, content));
            } else {
                // no change -> no need to do anything
                return Ok(self.clone());
            }
        }
    }
}

impl<'f, ST: 'f + CAS> Clone for Tree<'f, ST> {
    fn clone(&self) -> Self {
        Tree {
            fs: self.fs,
            inner: self.inner.clone(),
        }
    }
}

impl<'f, ST: 'f + CAS> Debug for Tree<'f, ST> {
    fn fmt(&self, f: &mut Formatter) -> StdResult<(), FmtError> {
        write!(f, "Tree")?;
        if self.inner.has_hash() {
            write!(f, "@{:?}", self.hash()?)?;
        }
        if self.inner.has_content() {
            write!(f, " [{:?}", self.data()?)?;
            let children = self.children()?;
            let mut names: Vec<&String> = children.keys().collect();
            names.sort();
            for name in names.drain(..) {
                write!(f, ", {}: {:?}", name, children.get(name).unwrap())?;
            }
            write!(f, "]")?;
        }
        Ok(())
    }
}

impl<'f, ST> LazyContent<'f, ST> for TreeContent<'f, ST>
where
    ST: 'f + CAS,
{
    fn retrieve_from(fs: &'f FileSystem<'f, ST>, hash: &Hash) -> Result<Self> {
        let raw: RawTree = fs.storage.retrieve(hash)?;
        let mut children: HashMap<String, Tree<'f, ST>> = HashMap::new();
        for elt in raw.children.iter() {
            children.insert(elt.0.clone(), Tree::for_hash(fs, &elt.1));
        }
        Ok(TreeContent {
            data: raw.data,
            children: children,
        })
    }

    fn store_in(&self, fs: &'f FileSystem<'f, ST>) -> Result<Hash> {
        let mut children: Vec<(String, Hash)> = vec![];
        children.reserve(self.children.len());
        for (k, v) in self.children.iter() {
            children.push((k.clone(), v.hash()?.clone()));
        }
        children.sort();
        let raw = RawTree {
            data: self.data.clone(),
            children: children,
        };
        Ok(fs.storage.store(&raw)?)
    }
}

#[cfg(test)]
mod test {
    use super::FileSystem;
    use crate::cas::CAS;
    use super::*;
    use crate::cas::LocalStorage;
    use crate::cas::Hash;

    const EMPTY_HASH: &'static str = "3e7077fd2f66d689e0cee6a7cf5b37bf2dca7c979af356d0a31cbc5c85605c7d";

    #[test]
    fn test_empty() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        let empty = Tree::empty(&fs);
        assert_eq!(empty.hash().unwrap(), &Hash::from_hex(EMPTY_HASH));
        assert_eq!(empty.children().unwrap().len(), 0);
        assert!(empty.data().unwrap().is_none())
    }

    #[test]
    fn test_for_hash() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        let cmt = Tree::for_hash(&fs, &Hash::from_hex("012345"));
        assert_eq!(cmt.hash().unwrap(), &Hash::from_hex("012345"));
        // there's no such object with that hash, so getting children or data fails
        assert!(cmt.children().is_err());
        assert!(cmt.data().is_err());
    }

    fn make_test_tree<'f, ST: 'f + CAS>(fs: &'f FileSystem<ST>) -> Tree<'f, ST> {
        Tree::empty(fs)
            .write(&["sub", "one"], "1".to_string())
            .unwrap()
            .write(&["sub", "two"], "2".to_string())
            .unwrap()
            .write(&["three"], "3".to_string())
            .unwrap()
    }

    #[test]
    fn test_write_and_incremental_load() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        let tree = make_test_tree(&fs);
        assert_eq!(
            format!("{:?}", tree),
            "Tree [None, \
                        sub: Tree [None, \
                            one: Tree [Some(\"1\")], \
                            two: Tree [Some(\"2\")]], \
                        three: Tree [Some(\"3\")]]"
        );

        let tree = Tree::for_hash(&fs, tree.hash().unwrap());
        assert_eq!(
            format!("{:?}", tree),
            "Tree@9b29101a4fae1ba244f7e8b0103f130114861718ed30d3b16d8b30d155592001"
        );

        tree.read(&["sub"]).unwrap();
        assert_eq!(
            format!("{:?}", tree),
            "Tree@9b29101a4fae1ba244f7e8b0103f130114861718ed30d3b16d8b30d155592001 [None, \
                        sub: Tree@e126739679be2e5f3eaa8d45a5737c15ead9ff0987af862176ec5b0cbb5fb92f [None, \
                            one: Tree@ce9fff9bafb150d24fe2efa3aba6257548c9bd182173e51041cbff7948286c35, \
                            two: Tree@0e9c430779ed1a97e66b6bbff2dc41caef63d63558182c451237085b806d841a], \
                        three: Tree@4c5f9e63a341421c9382639826104e0133ea8604134b410574f5734bbddd9c3e]"
        );

        tree.read(&["sub", "two"]).unwrap();
        assert_eq!(
            format!("{:?}", tree),
            "Tree@9b29101a4fae1ba244f7e8b0103f130114861718ed30d3b16d8b30d155592001 [None, \
                        sub: Tree@e126739679be2e5f3eaa8d45a5737c15ead9ff0987af862176ec5b0cbb5fb92f [None, \
                            one: Tree@ce9fff9bafb150d24fe2efa3aba6257548c9bd182173e51041cbff7948286c35, \
                            two: Tree@0e9c430779ed1a97e66b6bbff2dc41caef63d63558182c451237085b806d841a [Some(\"2\")]], \
                        three: Tree@4c5f9e63a341421c9382639826104e0133ea8604134b410574f5734bbddd9c3e]"
        );
    }

    #[test]
    fn read_exists() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);

        let tree = make_test_tree(&fs);
        assert_eq!(tree.read(&["three"]).unwrap(), Some("3"));
    }

    #[test]
    fn read_exists_from_storage() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);

        // create the tree and write it to storage by getting its hash
        let tree = make_test_tree(&fs);
        let hash = tree.hash().unwrap();

        // then re-load it from the storage
        let tree = Tree::for_hash(&fs, &hash);
        assert_eq!(tree.read(&["sub", "two"]).unwrap(), Some("2"));
    }

    #[test]
    fn read_not_found() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);

        let tree = make_test_tree(&fs);
        assert_eq!(tree.read(&[]).unwrap(), None);
        assert_eq!(tree.read(&["notathing"]).unwrap(), None);;
        assert_eq!(tree.read(&["sub", "sub2", "sub"]).unwrap(), None);;
        // "sub" exists but there's no data there
        assert_eq!(tree.read(&["sub"]).unwrap(), None);;
    }

    #[test]
    fn test_overwrite() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);

        let tree = Tree::empty(&fs);
        let tree = tree.write(&["foo", "bar"], "abc".to_string()).unwrap();

        // reload and write a new value
        let tree = Tree::for_hash(&fs, tree.hash().unwrap());
        let tree = tree.write(&["foo", "bar"], "def".to_string()).unwrap();

        assert_eq!(tree.read(&["foo", "bar"]).unwrap(), Some("def"));
        assert_eq!(
            tree.hash().unwrap(),
            &Hash::from_hex(
                "09b0b66433fe7cd79470acbe3e4d490c33bcc8396607201f0d288e328e81e1be",
            )
        );
    }

    #[test]
    fn remove_leaf() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);

        let tree = make_test_tree(&fs);
        let tree = tree.remove(&["sub", "one"]).unwrap();

        let tree = Tree::for_hash(&fs, tree.hash().unwrap());

        assert_eq!(tree.read(&["sub", "one"]).unwrap(), None);
    }

    #[test]
    fn remove_deep_from_storage() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);

        let tree = Tree::empty(&fs)
            .write(&["a", "b", "c", "d"], "value".to_string())
            .unwrap();
        let tree = Tree::for_hash(&fs, tree.hash().unwrap());
        let tree = tree.remove(&["a", "b", "c", "d"]).unwrap();

        // should trim empty directories, so this should be empty
        assert_eq!(tree.hash().unwrap(), &Hash::from_hex(EMPTY_HASH));
    }
}
