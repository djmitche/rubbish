use cas::{Hash, ContentAddressibleStorage};
use super::commit::Commit;
use super::tree::TreeEntry;
use super::object::Object;

pub struct Treeish<C: ContentAddressibleStorage<Object>> {
    storage: C,
}

impl <C: ContentAddressibleStorage<Object>> Treeish<C> {
    pub fn new(storage: C) -> Treeish<C> {
        Treeish {
            storage: storage,
        }
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

    pub fn get_commit(&self, hash: &Hash) -> Commit {
        let obj = self.storage.retrieve(hash).unwrap();
        match obj {
            Object::Commit{ tree, parents } => {
                return Commit::new(self.get_tree_entry(&tree), parents);
            },
            _ => panic!("{:?} is not a commit", hash),
        };
    }
}

#[cfg(test)]
mod test {
    use super::Treeish;
    use super::super::tree::TreeEntry;
    use super::super::object::Object;
    use cas::{LocalStorage, ContentAddressibleStorage, Hash};
    use std::collections::HashMap;

    #[test]
    fn get_commit() {
        let mut storage = LocalStorage::new();

        // add a commit with a tree directly to storage
        let d1 = storage.store(&Object::Blob{ data: vec![1] });
        let d2 = storage.store(&Object::Blob{ data: vec![2] });
        let mut children: HashMap<String, Hash> = HashMap::new();
        children.insert("one".to_string(), d1);
        children.insert("two".to_string(), d2);
        let tree = storage.store(&Object::Tree{ children: children });
        let commit = storage.store(&Object::Commit{
            tree: tree,
            parents: vec![],
        });

        // create the equivalent TreeEntry
        let mut tree = TreeEntry::new_tree();
        tree.add_child("one".to_string(), TreeEntry::new_blob(vec![1]));
        tree.add_child("two".to_string(), TreeEntry::new_blob(vec![2]));

        // unpack and verify the commit
        let treeish = Treeish::new(storage);

        let commit = treeish.get_commit(&commit);
        assert_eq!(commit.root, tree);
        assert_eq!(commit.parents, vec![]);
    }
}

