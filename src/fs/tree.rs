use std::collections::HashMap;
use std::sync::Arc;
use fs::Object;
use fs::ObjectStorage;
use cas::Hash;

/// A Tree represents an image of a tree-shaped data structure, sort of like a filesystem directoy.
/// However, directories can have associated data (that is, there can be data at `foo/bar` and at
/// `foo/bar/bing`).
#[derive(Clone, Debug)]
pub struct Tree {
    root: SubTree,
}

#[derive(Clone, Debug)]
struct Node {
    data: Option<String>,
    children: HashMap<String, SubTree>,
}

#[derive(Clone, Debug)]
enum SubTree {
    Unresolved(Hash),
    // TODO: Rc might be sufficient
    Resolved(Arc<Node>),
}

impl Tree {
    /// Create a new tree with the given root hash
    pub fn for_root(root: Hash) -> Tree {
        Tree {
            root: SubTree::Unresolved(root),
        }
    }

    /// Create a new, empty tree
    pub fn empty() -> Tree {
        let root = SubTree::Resolved(Arc::new(Node{data: None, children: HashMap::new()}));
        Tree {
            root: root,
        }
    }

    fn store_subtree(storage: &mut ObjectStorage, subtree: &SubTree) -> Hash {
        match subtree {
            &SubTree::Unresolved(ref hash) => hash.clone(),
            &SubTree::Resolved(ref node) => {
                let mut children = vec![];
                let mut keys = node.children.keys().collect::<Vec<&String>>();
                keys.sort();
                children.reserve(keys.len());

                for name in keys {
                    let subtree = node.children.get(name).unwrap();
                    children.push((name.clone(), Tree::store_subtree(storage, &subtree)));
                }

                let obj = Object::Tree {
                    data: node.data.clone(),
                    children: children,
                };
                storage.store(&obj)
            }
        }
    }

    /// Store this tree into the given storage, returning its hash.
    pub fn store(&self, storage: &mut ObjectStorage) -> Hash {
        Tree::store_subtree(storage, &self.root)
    }

    /// Return a tree containing new value at the designated path, replacing any
    /// existing value at that path.  The storage is used to read any unresolved
    /// tree nodes, but nothing is written to storage.
    ///
    /// Note that path elements and data can coexist, unlike a UNIX filesystem; that is, writing a
    /// value to "usr/bin" will not invalidate paths like "usr/bin/rustc".
    ///
    /// Writing uses path copying to copy a minimal amount of tree data such that the
    /// original tree is not modified and a new tree is returned, sharing data where
    /// possible.
    pub fn write(self, storage: &ObjectStorage, 
                 path: &[&str], data: String) -> Result<Tree, String> {
        let resolved: Arc<Node> = try!(self.root.resolve(storage));

        // first, make a stack of owned nodes, creating or cloning them as necessary
        let mut node_stack: Vec<Node> = vec![(*resolved).clone()];
        for name in path {
            let new_node = {
                let node: &Node = node_stack.last().unwrap();
                match node.children.get(&name.to_string()) {
                    Some(ref subtree) => {
                        let resolved = try!(subtree.resolve(storage));
                        (*resolved).clone()
                    },
                    None => {
                        // push a new, empty node onto the stack
                        Node{data: None, children: HashMap::new()}
                    }
                }
            };
            node_stack.push(new_node);
        }

        // write the data to the leaf node
        let mut leaf = node_stack.pop().unwrap();
        leaf.data = Some(data);
        node_stack.push(leaf);

        // finally, stitch the tree back together by modifying nodes back up to the
        // root
        let mut iter: Node = node_stack.pop().unwrap();
        while node_stack.len() > 0 {
            let mut parent: Node = node_stack.pop().unwrap();
            parent.children.insert(path[node_stack.len()].to_string(),
                                   SubTree::Resolved(Arc::new(iter)));
            iter = parent;
        }

        // return a new tree, rooted at the final new node
        return Ok(Tree{root: SubTree::Resolved(Arc::new(iter))});
    }

    /// Read the value at the given path in this tree, returning an error if this fails.
    /// If no value is set at the given path, that is considered an error.
    pub fn read(&self, storage: &ObjectStorage, path: &[&str]) -> Result<String, String> {
        let mut node = try!(self.root.resolve(storage));

        for name in path {
            node = match node.children.get(&name.to_string()) {
                Some(ref subtree) => {
                    try!(subtree.resolve(storage))
                },
                None => {
                    return Err("path not found".to_string());
                },
            }
        }
        match node.data {
            Some(ref value) => Ok(value.clone()),
            None => Err("path not found".to_string()),
        }
    }
}

impl SubTree {
    /// Resolve this SubTree to an Arc<Node>, retrieving if necessary.
    fn resolve(&self, storage: &ObjectStorage) -> Result<Arc<Node>, String> {
        match self {
            &SubTree::Unresolved(ref hash) => {
                if let Some(obj) = storage.retrieve(hash) {
                    if let Object::Tree{data, children} = obj {
                        let mut childmap = HashMap::new();
                        for (name, hash) in children {
                            match childmap.get(&name) {
                                None => {
                                    childmap.insert(name, SubTree::Unresolved(hash));
                                },
                                _ => {
                                    return Err("corrupt tree: duplicate child names".to_string());
                                }
                            }
                        }

                        let node = Node {
                            data: data,
                            children: childmap,
                        };
                        Ok(Arc::new(node))
                    } else {
                        Err("not a tree".to_string())
                    }
                } else {
                    Err("no object with that hash".to_string())
                }
            },
            &SubTree::Resolved(ref node_arc) => {
                Ok(node_arc.clone())
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Tree, SubTree};
    use fs::ObjectStorage;
    use cas::{LocalStorage, Hash};

    fn make_test_tree(storage: &mut ObjectStorage) -> Tree {
        Tree::empty()
            .write(storage, &["sub", "one"], "1".to_string()).unwrap()
            .write(storage, &["sub", "two"], "2".to_string()).unwrap()
            .write(storage, &["three"], "3".to_string()).unwrap()
    }

    fn rep_subtree(subtree: &SubTree) -> String {
        match subtree {
            &SubTree::Unresolved(ref hash) => format!("<{}>", hash.to_hex()),
            &SubTree::Resolved(ref node) => {
                let mut keys = node.children.keys().collect::<Vec<&String>>();
                keys.sort();
                let reps = keys.iter()
                    .map(|k| format!("{}: {}", k, rep_subtree(&node.children.get(&k[..]).unwrap())))
                    .collect::<Vec<String>>();
                format!("{{{:?}; {}}}", node.data, reps.join(", "))
            }
        }
    }

    #[test]
    fn test_rep_subtree() {
        let mut storage = LocalStorage::new();
        let tree = make_test_tree(&mut storage);
        assert_eq!(rep_subtree(&tree.root),
                   "{None; sub: {None; one: {Some(\"1\"); }, two: {Some(\"2\"); }}, three: {Some(\"3\"); }}".to_string());
    }

    #[test]
    fn test_empty() {
        let mut storage = LocalStorage::new();
        let tree = Tree::empty();
        println!("{}", rep_subtree(&tree.root));
        assert_eq!(
            tree.store(&mut storage),
            Hash::from_hex(&"387dc3282dea8a6824ddcdafe9f48296118d6ecc20dc5f13bc84ae952510d801"));
    }

    #[test]
    fn test_for_root() {
        let mut storage = LocalStorage::new();
        let tree = Tree::for_root(Hash::from_hex(&"abcdef"));
        println!("{}", rep_subtree(&tree.root));
        assert_eq!(
            tree.store(&mut storage),
            Hash::from_hex(&"abcdef"));
    }

    #[test]
    fn test_write() {
        let mut storage = LocalStorage::new();
        let tree = Tree::empty()
            .write(&storage, &[], "rt".to_string()).unwrap()
            .write(&storage, &["foo", "bar"], "xyz".to_string()).unwrap()
            .write(&storage, &["foo", "bing"], "ggg".to_string()).unwrap()
            .write(&storage, &["foo"], "short".to_string()).unwrap()
            .write(&storage, &["foo", "bar", "qux"], "qqq".to_string()).unwrap();
        assert_eq!(
            rep_subtree(&tree.root),
            "{Some(\"rt\"); foo: {Some(\"short\"); bar: {Some(\"xyz\"); qux: {Some(\"qqq\"); }}, bing: {Some(\"ggg\"); }}}");
        assert_eq!(
            tree.store(&mut storage),
            Hash::from_hex(&"4dea115efe72d154edf7af8cd9cdd952a556ebd2ea9239f789835003a1abad08"));
    }

    #[test]
    fn test_overwrite() {
        let mut storage = LocalStorage::new();
        let tree = Tree::empty()
            .write(&storage, &["foo", "bar"], "abc".to_string()).unwrap()
            .write(&storage, &["foo", "bar"], "def".to_string()).unwrap();
        assert_eq!(
            rep_subtree(&tree.root),
            "{None; foo: {None; bar: {Some(\"def\"); }}}");
        assert_eq!(
            tree.store(&mut storage),
            Hash::from_hex(&"f1e01ab2ce24cc5e686f862dd80eca137d6897f8e23ae63c2c29b349278803cc"));
    }

    #[test]
    fn read_exists() {
        let mut storage = LocalStorage::new();
        let tree = make_test_tree(&mut storage);
        assert_eq!(tree.read(&storage, &["three"]), Ok("3".to_string()));
    }

    #[test]
    fn read_exists_from_storage() {
        let mut storage = LocalStorage::new();
        let tree = make_test_tree(&mut storage);
        let hash = tree.store(&mut storage);
        let tree = Tree::for_root(hash);
        assert_eq!(tree.read(&storage, &["sub", "two"]), Ok("2".to_string()));
    }

    #[test]
    fn read_empty_path() {
        let mut storage = LocalStorage::new();
        let tree = make_test_tree(&mut storage);
        assert_eq!(tree.read(&storage, &[]), Err("path not found".to_string()));
    }

    #[test]
    fn read_not_found() {
        let mut storage = LocalStorage::new();
        let tree = make_test_tree(&mut storage);
        assert_eq!(tree.read(&storage, &["notathing"]), Err("path not found".to_string()));
    }

    #[test]
    fn read_blob_name_nonterminal() {
        let mut storage = LocalStorage::new();
        let tree = make_test_tree(&mut storage);
        assert_eq!(tree.read(&storage, &["three", "subtree"]), Err("path not found".to_string()));
    }
}
