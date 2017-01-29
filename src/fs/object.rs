use cas::Hash;
use std::collections::HashMap;

/// Objects get encoded into the CAS, but are interlinked with hashes
/// instead of references.
#[derive(Debug, RustcDecodable, RustcEncodable)]
pub enum Object {
    /// A commit represents the root of a tree, as evolved from its parents
    Commit { tree: Hash, parents: Vec<Hash> },

    /// A tree represents a "directory", containing more trees; children
    /// are (name, hash_of_value) pairs, ordered by name, with duplicate
    /// names forbidden.
    Tree { data: Option<String>, children: Vec<(String, Hash)> },
}
