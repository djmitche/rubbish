use cas::Hash;
use std::collections::HashMap;

/// Objects get encoded into the CAS, but are interlinked with hashes
/// instead of references.
#[derive(RustcDecodable, RustcEncodable)]
pub enum Object {
    // A commit represents the root of a tree, as evolved from its parents
    Commit {
        tree: Hash,  // TODO: root
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
