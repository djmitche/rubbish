use fs::Tree;
use fs::Object;
use cas::Hash;
use cas::CAS;

#[derive(Debug, Clone)]
enum Parent {
    Unresolved(Hash),
    Resolved(Commit),
}

#[derive(Debug, Clone)]
pub struct Commit {
    tree: Tree,
    parents: Vec<Parent>,
}

impl Commit {
    /// Create the root commit (no parents, empty tree)
    pub fn root() -> Commit {
        Commit {
            tree: Tree::empty(),
            parents: vec![],
        }
    }

    /// Get the tree at this commit
    pub fn tree(&self) -> Tree {
        self.tree.clone()
    }

    /// Create a child commit based on this one, applying the modifier function to the enclosed
    /// tree.  This function can call any Tree methods, or even return an entirely unrelated Tree.
    /// If the modifier returns an error, make_child does as well.
    pub fn make_child<F>(&self, mut modifier: F) -> Result<Commit, String>
        where F: FnMut(Tree) -> Result<Tree, String> {
        let new_tree = try!(modifier(self.tree.clone()));
        Ok(Commit {
            tree: new_tree,
            parents: vec![Parent::Resolved((*self).clone())],
        })
    }

    /// Get a commit from storage, given its hash
    pub fn retrieve(storage: &CAS<Object>, commit: Hash) -> Result<Commit, String> {
        if let Some(obj) = storage.retrieve(&commit) {
            if let Object::Commit{tree, parents} = obj {
                let mut parent_commits = vec![];
                parent_commits.reserve(parents.len());
                for parent_hash in parents {
                    parent_commits.push(Parent::Unresolved(parent_hash));
                }
                Ok(Commit{tree: Tree::for_root(tree), parents: parent_commits})
            } else {
                Err("not a commit".to_string())
            }
        } else {
            Err("no object with that hash".to_string())
        }
    }

    /// Store this commit and return the hash
    pub fn store(&self, storage: &mut CAS<Object>) -> Hash {
        let mut parent_hashes = vec![];
        parent_hashes.reserve(self.parents.len());
        for parent in &self.parents {
            match parent {
                &Parent::Unresolved(ref hash) => {
                    parent_hashes.push(hash.clone());
                },
                &Parent::Resolved(ref commit) => {
                    parent_hashes.push(commit.store(storage));
                }
            }
        }

        let tree_hash = self.tree.store(storage);

        let obj = Object::Commit {
            tree: tree_hash,
            parents: parent_hashes,
        };
        storage.store(&obj)
    }
}

#[cfg(test)]
mod test {
    use super::Commit;
    use fs::tree::Tree;
    use fs::Object;
    use cas::{LocalStorage, CAS, Hash};

    const ROOT_HASH: &'static str = "4e4792b3a91c2cea55575345f94bb20c2d6b8d62a34f7e6099e7fd3a40944836";

    #[test]
    fn test_root() {
        let mut storage = LocalStorage::new();
        assert_eq!(
            Commit::root().store(&mut storage),
            Hash::from_hex(&ROOT_HASH));
    }

    #[test]
    fn test_make_child() {
        let mut storage = LocalStorage::new();
        let child = Commit::root().make_child(&mut |tree: Tree| -> Result<Tree, String> {
            let tree = try!(tree.write(&mut storage, &["x", "y"], "Y".to_string()));
            let tree = try!(tree.write(&mut storage, &["x", "z"], "Z".to_string()));
            Ok(tree)
        }).unwrap();
        println!("child commit: {:?}", child);

        let child_hash = child.store(&mut storage);
        println!("child hash: {:?}", child_hash);

        // unpack those objects from storage to verify their form..

        println!("UNPACKING");
        let child_obj = storage.retrieve(&child_hash).unwrap();
        println!("child object: {:?} = {:?}", child_hash, child_obj);
        let (tree_hash, parents) = match child_obj {
            Object::Commit{tree, parents} => (tree, parents),
            _ => panic!("not a commit"),
        };

        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0], Hash::from_hex(&ROOT_HASH));

        let tree_obj = storage.retrieve(&tree_hash).unwrap();
        println!("tree object: {:?} = {:?}", tree_hash, tree_obj);
        let (data, children) = match tree_obj {
            Object::Tree{data, children} => (data, children),
            _ => panic!("not a tree"),
        };

        assert_eq!(data, None);
        assert_eq!(children.len(), 1);
        let (ref child_name_x, ref child_hash_x) = children[0];
        assert_eq!(child_name_x, &"x".to_string());

        let tree_obj = storage.retrieve(&child_hash_x).unwrap();
        println!("tree object: {:?} = {:?}", child_hash_x, tree_obj);
        let (data, children) = match tree_obj {
            Object::Tree{data, children} => (data, children),
            _ => panic!("not a tree"),
        };

        assert_eq!(data, None);
        assert_eq!(children.len(), 2);
        let (ref child_name_y, ref child_hash_y) = children[0];
        assert_eq!(child_name_y, &"y".to_string());
        let (ref child_name_z, ref child_hash_z) = children[1];
        assert_eq!(child_name_z, &"z".to_string());

        let tree_obj = storage.retrieve(&child_hash_y).unwrap();
        println!("tree object: {:?} = {:?}", child_hash_y, tree_obj);
        let (data, children) = match tree_obj {
            Object::Tree{data, children} => (data, children),
            _ => panic!("not a tree"),
        };
        assert_eq!(data, Some("Y".to_string()));
        assert_eq!(children.len(), 0);

        let tree_obj = storage.retrieve(&child_hash_z).unwrap();
        println!("tree object: {:?} = {:?}", child_hash_z, tree_obj);
        let (data, children) = match tree_obj {
            Object::Tree{data, children} => (data, children),
            _ => panic!("not a tree"),
        };
        assert_eq!(data, Some("Z".to_string()));
        assert_eq!(children.len(), 0);
    }
}
