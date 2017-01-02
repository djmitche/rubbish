use cas::{Hash, ContentAddressibleStorage};
use std::collections::HashMap;

mod tree;
pub use self::tree::TreeEntry;

mod commit;
pub use self::commit::Commit;

/// Objects get encoded into the CAS, but are interlinked with hashes
/// instead of references.
#[derive(RustcDecodable, RustcEncodable)]
pub enum Object {  // TODO: make non pub (need to make a trait)
    // A commit represents the root of a tree, as evolved from its parents
    Commit {
        tree: Hash,
        parents: Vec<Hash>,
    },

    // A tree represents a "directory", containing either blobs or more trees
    Tree {
        children: HashMap<String, Hash>,
    },

    // A blob represents data (like a file)
    Blob {
        data: Vec<u8>,
    },
}

pub struct Treeish<C: ContentAddressibleStorage<Object>> {
    storage: C,
}

impl <C: ContentAddressibleStorage<Object>> Treeish<C> {
    pub fn new(storage: C) -> Treeish<C> {
        Treeish {
            storage: storage,
        }
    }

    pub fn get_commit(&self, hash: &Hash) -> Commit {
        let obj : Object = self.storage.retrieve(hash).unwrap();
        match obj {
            Object::Commit{ tree, parents } => {
                return Commit::new(self.get_tree_entry(&tree), parents);
            },
            _ => panic!("{:?} is not a commit", hash),
        };
    }

    fn get_tree_entry(&self, hash: &Hash) -> TreeEntry {
        let obj : Object = self.storage.retrieve(hash).unwrap();
        match obj {
            Object::Tree{ children } => {
                let mut subtree = TreeEntry::new_tree();
                for (name, hash) in children.iter() {
                    subtree.add_child(name.clone(), self.get_tree_entry(&hash));
                }
                return subtree;
            },
            Object::Blob{ data } => {
                return TreeEntry::new_blob(data);
            },
            _ => panic!("{:?} is not a tree or a blob", hash),
        };
    }
}

#[cfg(test)]
mod test {
    use super::{Treeish, Object, TreeEntry};
    use cas::{LocalStorage, ContentAddressibleStorage, Hash};
    use std::collections::HashMap;

    #[test]
    fn get_commit() {
        let mut treeish : Treeish<LocalStorage<Object>> = Treeish::new(LocalStorage::new());

        // add a commit with a tree directly to storage
        let d1 = treeish.storage.store(&Object::Blob{ data: vec![1] });
        let d2 = treeish.storage.store(&Object::Blob{ data: vec![2] });
        let mut children: HashMap<String, Hash> = HashMap::new();
        children.insert("one".to_string(), d1);
        children.insert("two".to_string(), d2);
        let tree = treeish.storage.store(&Object::Tree{ children: children });
        let commit = treeish.storage.store(&Object::Commit{
            tree: tree,
            parents: vec![],
        });

        // create the equivalent TreeEntry
        let mut tree = TreeEntry::new_tree();
        tree.add_child("one".to_string(), TreeEntry::new_blob(vec![1]));
        tree.add_child("two".to_string(), TreeEntry::new_blob(vec![2]));

        // unpack and verify the commit
        let commit = treeish.get_commit(&commit);
        assert_eq!(commit.root, tree);
        assert_eq!(commit.parents, vec![]);
    }
}
