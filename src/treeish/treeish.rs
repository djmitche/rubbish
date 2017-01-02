use std::collections::HashMap;
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

    fn add_tree_entry(&mut self, tree_entry: &TreeEntry) -> Hash {
        let obj;
        // TODO: optimize by using an existing hash if one is found
        match tree_entry {
            &TreeEntry::SubTree{ ref children } => {
                let mut child_objects = HashMap::new();
                for (name, subtree) in children.iter() {
                    child_objects.insert(name.clone(), self.add_tree_entry(&subtree));
                }
                obj = Object::Tree{ children: child_objects };
            },
            &TreeEntry::Blob{ ref data } => {
                obj = Object::Blob{ data: data.clone() };
            },
        };

        self.storage.store(&obj)
    }

    pub fn add_commit(&mut self, commit: &Commit) -> Hash {
        let tree = self.add_tree_entry(&commit.root);
        let obj = Object::Commit{ tree: tree, parents: commit.parents.clone() };
        self.storage.store(&obj)
    }
}

#[cfg(test)]
mod test {
    use super::Treeish;
    use super::super::{TreeEntry, Commit};
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

    fn unwrap_commit(object: &Object) -> (&Hash, &Vec<Hash>) {
        if let &Object::Commit{ ref tree, ref parents } = object {
            return (tree, parents);
        } else {
            panic!("Not a commit");
        }
    }

    fn unwrap_tree(object: &Object) -> &HashMap<String, Hash> {
        if let &Object::Tree{ ref children } = object {
            return children;
        } else {
            panic!("Not a tree");
        }
    }

    fn unwrap_blob(object: &Object) -> &Vec<u8> {
        if let &Object::Blob{ ref data } = object {
            return data;
        } else {
            panic!("Not a blob");
        }
    }

    #[test]
    fn add_commit() {
        let mut treeish = Treeish::new(LocalStorage::new());

        // create a TreeEntry
        let mut tree = TreeEntry::new_tree();
        tree.add_child("one".to_string(), TreeEntry::new_blob(vec![1]));
        tree.add_child("two".to_string(), TreeEntry::new_blob(vec![2]));
        let commit = Commit::new(tree, vec![]);

        // Add it
        let hash = treeish.add_commit(&commit);

        // look for it in storage
        let storage = treeish.storage;
        let commit = storage.retrieve(&hash).unwrap();
        let (root, parents) = unwrap_commit(&commit);
        assert_eq!(parents, &vec![]);

        let root = storage.retrieve(&root).unwrap();
        let children = unwrap_tree(&root);
        assert_eq!(children.len(), 2);

        let one = storage.retrieve(children.get(&"one".to_string()).unwrap()).unwrap();
        assert_eq!(unwrap_blob(&one), &vec![1u8]);

        let two = storage.retrieve(children.get(&"two".to_string()).unwrap()).unwrap();
        assert_eq!(unwrap_blob(&two), &vec![2u8]);
    }
}

