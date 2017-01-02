use cas::Hash;
use super::TreeEntry;

/// A commit represents an "expanded" commit object, complete with a tree.  The
/// parents are still represented as hashes, though.
#[derive(PartialEq, Debug)]
pub struct Commit {
    pub root: TreeEntry,
    pub parents: Vec<Hash>
}

impl Commit {
    pub fn new(root: TreeEntry, parents: Vec<Hash>) -> Commit {
        Commit{root: root, parents: parents}
    }
}
