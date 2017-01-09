use cas::Hash;
use std::result::Result;
use super::TreeEntry;

/// A commit represents an "expanded" commit object, complete with a tree.  The
/// parents are still represented as hashes, though.
#[derive(PartialEq, Debug)]
pub struct Commit {
    pub root: TreeEntry,
    pub parents: Vec<Hash>,
}

impl Commit {
    pub fn new(root: TreeEntry, parents: Vec<Hash>) -> Commit {
        Commit {
            root: root,
            parents: parents,
        }
    }

    // Create a commit with no parents an an empty tree
    pub fn empty() -> Commit {
        Commit {
            root: TreeEntry::new(),
            parents: vec![],
        }
    }

    pub fn read(&self, path: &[&str]) -> Result<&Vec<u8>, String> {
        self.root.read(path)
    }
}

#[cfg(test)]
mod test {
    use super::Commit;
    use super::super::TreeEntry;

    fn make_test_commit() -> Commit {
        let tree = TreeEntry::new().write(&["six"], vec![6]);
        Commit::new(tree, vec![])
    }

    #[test]
    fn read_exists() {
        let commit = make_test_commit();
        assert_eq!(commit.read(&["six"]), Ok(&vec![6u8]));
    }

    #[test]
    fn read_nonexistent() {
        let commit = make_test_commit();
        assert_eq!(commit.read(&["xxx"]),
                   Err("[\"xxx\"] not found".to_string()));
    }
}
