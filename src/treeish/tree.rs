use std::collections::HashMap;
use std::result::Result;

// TODO: use std::borrow::Cow to return maybe-copied stuff

/// A TreeEntry represents a fully-linked tree of data
#[derive(PartialEq, Debug)]
pub enum TreeEntry {
    // TODO: use references to the data in the underlying Object here (need to sort out lifetimes)
    SubTree { children: HashMap<String, TreeEntry>, },

    Blob { data: Vec<u8> },
}

impl TreeEntry {
    /// Create a new Blob entry, given the bytestring it should contain
    pub fn new_blob(data: Vec<u8>) -> TreeEntry {
        TreeEntry::Blob { data: data }
    }

    // Create a new, empty Tree entry
    pub fn new_tree() -> TreeEntry {
        TreeEntry::SubTree { children: HashMap::new() }
    }

    pub fn add_child(&mut self, name: String, child: TreeEntry) {
        if let &mut TreeEntry::SubTree { ref mut children } = self {
            if children.contains_key(&name) {
                panic!("key {} already exists", name);
            }
            children.insert(name, child);
        } else {
            panic!("not a SubTree");
        }
    }

    pub fn read(&self, fullpath: &[&str]) -> Result<&Vec<u8>, String> {
        let mut path = fullpath;
        let mut te = self;

        loop {
            if path.len() == 0 {
                if let &TreeEntry::Blob { ref data } = te {
                    return Ok(data);
                } else {
                    return Err(format!("{:?} is not a blob", fullpath));
                }
            } else {
                if let &TreeEntry::SubTree { ref children } = te {
                    if let Some(sub) = children.get(path[0]) {
                        te = sub;
                        path = &path[1..];
                    } else {
                        return Err(format!("{:?} not found", fullpath));
                    }
                } else {
                    return Err(format!("{:?} is not a subtree", fullpath));
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::TreeEntry;

    fn make_test_tree() -> TreeEntry {
        let mut subtree = TreeEntry::new_tree();
        subtree.add_child("one".to_string(), TreeEntry::new_blob(vec![1]));
        subtree.add_child("two".to_string(), TreeEntry::new_blob(vec![2]));
        let mut tree = TreeEntry::new_tree();
        tree.add_child("sub".to_string(), subtree);
        tree.add_child("three".to_string(), TreeEntry::new_blob(vec![3]));
        tree
    }

    #[test]
    fn read_exists() {
        let tree = make_test_tree();
        assert_eq!(tree.read(&["three"]), Ok(&vec![3u8]));
    }

    #[test]
    fn read_empty_path() {
        let tree = make_test_tree();
        assert_eq!(tree.read(&[]), Err("[] is not a blob".to_string()));
    }

    #[test]
    fn read_not_found() {
        let tree = make_test_tree();
        assert_eq!(tree.read(&["notathing"]),
                   Err("[\"notathing\"] not found".to_string()));
    }

    #[test]
    fn read_blob_name_nonterminal() {
        let tree = make_test_tree();
        assert_eq!(tree.read(&["three", "subtree"]),
                   Err("[\"three\", \"subtree\"] is not a subtree".to_string()));
    }
}
