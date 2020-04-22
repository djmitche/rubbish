use super::fs::FileSystem;
use super::lazy::LazyContent;
use crate::cas::Hash;
use bincode::{
    rustc_serialize::{decode, encode},
    SizeLimit,
};
use failure::Fallible;
use rustc_serialize::{Encodable, Encoder};
use std::collections::HashMap;

/// Content is the data type that FS stores.
#[derive(RustcDecodable, PartialEq, Debug)]
pub enum Content {
    Commit {
        parents: Vec<Hash>,
        tree: Hash,
    },
    Tree {
        data: Option<Vec<u8>>,
        children: HashMap<String, Hash>,
    },
}

impl Encodable for Content {
    // We need a stable encoding, so override encode to generate a value that will
    // decode using the derived implementation, but which sorts tree children in
    // order.  Note that this assumes rustc's default implementation of Encodable
    // numbers enum variants from 0.
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        s.emit_enum("Content", |s| match self {
            Content::Commit { parents, tree } => s.emit_enum_struct_variant("Commit", 0, 2, |s| {
                s.emit_enum_struct_variant_field("parents", 0, |s| parents.encode(s))?;
                s.emit_enum_struct_variant_field("tree", 1, |s| tree.encode(s))?;
                Ok(())
            }),
            Content::Tree { data, children } => s.emit_enum_struct_variant("Tree", 1, 2, |s| {
                s.emit_enum_struct_variant_field("data", 0, |s| data.encode(s))?;
                s.emit_enum_struct_variant_field("children", 1, |s| {
                    s.emit_map(children.len(), |s| {
                        let mut by_name: Vec<(&String, &Hash)> = children.iter().collect();
                        by_name.sort();
                        for (i, (n, t)) in by_name.iter().enumerate() {
                            s.emit_map_elt_key(i, |s| n.encode(s))?;
                            s.emit_map_elt_key(i, |s| t.encode(s))?;
                        }
                        Ok(())
                    })
                })?;
                Ok(())
            }),
        })
    }
}

impl LazyContent for Content {
    fn retrieve_from(fs: &FileSystem, hash: &Hash) -> Fallible<Self> {
        let bytes = fs.storage.retrieve(hash)?;
        Ok(decode(&bytes)?)
    }

    fn store_in(&self, fs: &FileSystem) -> Fallible<Hash> {
        let encoded = encode(self, SizeLimit::Infinite)?;
        Ok(fs.storage.store(encoded)?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::cas::LocalStorage;
    use crate::fs::hashes::EMPTY_TREE_HASH;
    use crate::fs::FileSystem;

    #[test]
    fn test_store_and_retrieve() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));
        let content = Content::Commit {
            tree: Hash::from_hex(EMPTY_TREE_HASH),
            parents: vec![],
        };

        let hash = content.store_in(&fs).unwrap();
        let content2 = Content::retrieve_from(&fs, &hash).unwrap();
        assert_eq!(content, content2);
    }
}
