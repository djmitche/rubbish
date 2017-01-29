use super::object::Object;
use fs::Tree;
use cas::Hash;
use cas::ContentAddressibleStorage;

pub struct Commit {
    tree: Tree,
    parents: Vec<Hash>,
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

    /// Get a commit from storage, given its hash
    pub fn retrieve(storage: &ContentAddressibleStorage<Object>, commit: Hash) -> Result<Commit, String> {
        if let Some(obj) = storage.retrieve(&commit) {
            if let Object::Commit{tree, parents} = obj {
                Ok(Commit{tree: Tree::for_root(tree), parents: parents})
            } else {
                Err("not a commit".to_string())
            }
        } else {
            Err("no object with that hash".to_string())
        }
    }

    /// Store this commit and return the hash
    pub fn store(&self, storage: &mut ContentAddressibleStorage<Object>) -> Hash {
        let root_hash = self.tree.store(storage);
        let obj = Object::Commit {
            tree: root_hash,
            parents: self.parents.clone(),
        };
        storage.store(&obj)
    }
}

#[cfg(test)]
mod test {
    use super::Commit;
    use cas::{LocalStorage, ContentAddressibleStorage, Hash};

    #[test]
    fn test_root() {
        let mut storage = LocalStorage::new();
        assert_eq!(
            Commit::root().store(&mut storage),
            Hash::from_hex(&"4e4792b3a91c2cea55575345f94bb20c2d6b8d62a34f7e6099e7fd3a40944836"));
    }
}
