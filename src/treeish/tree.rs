use std::collections::HashMap;

/// A TreeEntry represents a fully-linked tree of data
#[derive(PartialEq, Debug)]
pub enum TreeEntry {
    // TODO: use references to the data in the underlying Object here (need to sort out lifetimes)
    SubTree {
        children: HashMap<String, TreeEntry>,
    },

    Blob {
        data: Vec<u8>,
    },
}

impl TreeEntry {
    /// Create a new Blob entry, given the bytestring it should contain
    pub fn new_blob(data: Vec<u8>) -> TreeEntry {
        TreeEntry::Blob{ data: data }
    }

    // Create a new, empty Tree entry
    pub fn new_tree() -> TreeEntry {
        TreeEntry::SubTree{ children: HashMap::new() }
    }

    pub fn add_child(&mut self, name: String, child: TreeEntry) {
        if let &mut TreeEntry::SubTree{ ref mut children } = self {
            if children.contains_key(&name) {
                panic!("key {} already exists", name);
            }
            children.insert(name, child);
        } else {
            panic!("not a SubTree");
        }
    }
}

#[cfg(test)]
mod test {
    use super::TreeEntry;

    #[test]
    fn build_tree() {
        let mut tree = TreeEntry::new_tree();
        let mut subtree = TreeEntry::new_tree();
        subtree.add_child("one".to_string(), TreeEntry::new_blob(vec![1]));
        subtree.add_child("two".to_string(), TreeEntry::new_blob(vec![2]));
        tree.add_child("sub".to_string(), subtree);
    }
}
