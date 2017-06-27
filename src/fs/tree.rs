use std::collections::HashMap;
use std::sync::Arc;
use fs::Object;
use cas::Hash;
use cas::CAS;

/// A Tree represents an image of a tree-shaped data structure, sort of like a filesystem directoy.
/// However, directories can have associated data (that is, there can be data at `foo/bar` and at
/// `foo/bar/bing`).
#[derive(Debug)]
pub struct Tree<'a, C>
    where C: 'a + CAS
{
    storage: &'a C,
    root: SubTree<'a, C>,
}

#[derive(Debug)]
struct Node<'a, C>
    where C: 'a + CAS
{
    storage: &'a C,
    data: Option<String>,
    children: HashMap<String, SubTree<'a, C>>,
}

#[derive(Debug)]
enum SubTree<'a, C>
    where C: 'a + CAS
{
    Unresolved(Hash),
    // TODO: Rc might be sufficient
    Resolved(Arc<Node<'a, C>>),
}

impl<'a, C> Clone for Tree<'a, C>
    where C: 'a + CAS
{
    fn clone(&self) -> Self {
        Tree {
            storage: self.storage,
            root: self.root.clone(),
        }
    }
}

impl<'a, C> Tree<'a, C>
    where C: 'a + CAS
{
    /// Create a new tree with the given root hash
    pub fn for_root(storage: &'a C, root: Hash) -> Tree<'a, C> {
        Tree {
            storage: storage,
            root: SubTree::Unresolved(root),
        }
    }

    /// Create a new, empty tree
    pub fn empty(storage: &'a C) -> Tree<'a, C> {
        let root = SubTree::Resolved(Arc::new(Node {
                                                  storage: storage,
                                                  data: None,
                                                  children: HashMap::new(),
                                              }));
        Tree {
            storage: storage,
            root: root,
        }
    }

    fn store_subtree(storage: &'a C, subtree: &SubTree<'a, C>) -> Hash {
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
    pub fn store(&self, storage: &'a C) -> Hash {
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
    pub fn write<'b>(self, path: &'b [&str], data: String) -> Result<Tree<'a, C>, String> {
        self.modify(path, Some(data))
    }

    /// Return a tree with the value at the given path removed.  Empty directories will
    /// be removed.  The storage is used to read any unresolvedtree nodes, but nothing is
    /// written to storage.  If the path is already missing, an unchanged copy of the
    /// tree is returned.
    ///
    /// This operation uses path copying to copy a minimal amount of tree data such that the
    /// original tree is not modified and a new tree is returned, sharing data where
    /// possible.
    pub fn remove(self, path: &[&str]) -> Result<Tree<'a, C>, String> {
        self.modify(path, None)
    }

    /// Read the value at the given path in this tree, returning an error if this fails.
    /// If no value is set at the given path, that is considered an error.
    pub fn read(&self, storage: &'a C, path: &[&str]) -> Result<String, String> {
        let mut node = try!(self.root.resolve(storage));

        for name in path {
            node = match node.children.get(&name.to_string()) {
                Some(ref subtree) => try!(subtree.resolve(storage)),
                None => {
                    return Err("path not found".to_string());
                }
            }
        }
        match node.data {
            Some(ref value) => Ok(value.clone()),
            None => Err("path not found".to_string()),
        }
    }

    /// Set the data at the given path, returning a new Tree that shares
    /// some nodes with the original via path copying.
    ///
    /// This prunes empty directories.
    fn modify(self, path: &[&str], data: Option<String>) -> Result<Tree<'a, C>, String> {
        let resolved: Arc<Node<'a, C>> = try!(self.root.resolve(self.storage));

        // first, make a stack of owned nodes, creating or cloning them as necessary
        let mut node_stack: Vec<Node<'a, C>> = vec![(*resolved).clone()];
        for name in path {
            let new_node = {
                let node: &Node<'a, C> = node_stack.last().unwrap();
                match node.children.get(&name.to_string()) {
                    Some(ref subtree) => {
                        let resolved = try!(subtree.resolve(self.storage));
                        (*resolved).clone()
                    }
                    None => {
                        // push a new, empty node onto the stack
                        Node {
                            storage: self.storage,
                            data: None,
                            children: HashMap::new(),
                        }
                    }
                }
            };
            node_stack.push(new_node);
        }

        // write the data to the leaf node
        let mut leaf = node_stack.pop().unwrap();
        leaf.data = data;
        node_stack.push(leaf);

        // finally, stitch the tree back together by modifying nodes back up to the
        // root
        let mut iter: Node<'a, C> = node_stack.pop().unwrap();
        while node_stack.len() > 0 {
            let mut parent: Node<'a, C> = node_stack.pop().unwrap();
            let name = path[node_stack.len()].to_string();

            // if iter is empty, omit it from its parent
            if iter.data == None && iter.children.len() == 0 {
                parent.children.remove(&name);
            } else {
                parent
                    .children
                    .insert(name, SubTree::Resolved(Arc::new(iter)));
            }
            iter = parent;
        }

        // return a new tree, rooted at the final new node
        return Ok(Tree {
                      storage: self.storage,
                      root: SubTree::Resolved(Arc::new(iter)),
                  });
    }
}

impl<'a, C> Clone for Node<'a, C>
    where C: 'a + CAS
{
    fn clone(&self) -> Self {
        Node {
            storage: self.storage,
            data: self.data.clone(),
            children: self.children.clone(),
        }
    }
}

impl<'a, C> Clone for SubTree<'a, C>
    where C: 'a + CAS
{
    fn clone(&self) -> Self {
        match *self {
            SubTree::Unresolved(ref h) => SubTree::Unresolved(h.clone()),
            SubTree::Resolved(ref n) => SubTree::Resolved(n.clone()),
        }
    }
}

impl<'a, C> SubTree<'a, C>
    where C: 'a + CAS
{
    /// Resolve this SubTree to an Arc<Node>, retrieving if necessary.
    fn resolve(&self, storage: &'a C) -> Result<Arc<Node<'a, C>>, String> {
        match self {
            &SubTree::Unresolved(ref hash) => {
                if let Ok(obj) = storage.retrieve(hash) {
                    if let Object::Tree { data, children } = obj {
                        let mut childmap = HashMap::new();
                        for (name, hash) in children {
                            match childmap.get(&name) {
                                None => {
                                    childmap.insert(name, SubTree::Unresolved(hash));
                                }
                                _ => {
                                    return Err("corrupt tree: duplicate child names".to_string());
                                }
                            }
                        }

                        let node = Node {
                            storage: storage,
                            data: data,
                            children: childmap,
                        };
                        Ok(Arc::new(node))
                    } else {
                        Err("not a tree".to_string())
                    }
                } else {
                    // TODO: pass on error
                    Err("no object with that hash".to_string())
                }
            }
            &SubTree::Resolved(ref node_arc) => Ok(node_arc.clone()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Tree, SubTree};
    use cas::{LocalStorage, Hash, CAS};

    fn make_test_tree<'a, C>(storage: &'a C) -> Tree<'a, C>
        where C: 'a + CAS
    {
        Tree::empty(storage)
            .write(&["sub", "one"], "1".to_string())
            .unwrap()
            .write(&["sub", "two"], "2".to_string())
            .unwrap()
            .write(&["three"], "3".to_string())
            .unwrap()
    }

    fn rep_subtree<'a, C>(subtree: &SubTree<'a, C>) -> String
        where C: 'a + CAS
    {
        match subtree {
            &SubTree::Unresolved(ref hash) => format!("<{}>", hash.to_hex()),
            &SubTree::Resolved(ref node) => {
                let mut keys = node.children.keys().collect::<Vec<&String>>();
                keys.sort();
                let reps = keys.iter()
                    .map(|k| {
                             format!("{}: {}",
                                     k,
                                     rep_subtree(&node.children.get(&k[..]).unwrap()))
                         })
                    .collect::<Vec<String>>();
                format!("{{{:?}; {}}}", node.data, reps.join(", "))
            }
        }
    }

    #[test]
    fn test_rep_subtree() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        assert_eq!(rep_subtree(&tree.root),
                   "{None; sub: {None; one: {Some(\"1\"); }, two: {Some(\"2\"); }}, three: {Some(\"3\"); }}".to_string());
    }

    #[test]
    fn test_empty() {
        let storage = LocalStorage::new();
        let tree = Tree::empty(&storage);
        println!("{}", rep_subtree(&tree.root));
        assert_eq!(tree.store(&storage),
                   Hash::from_hex(&"387dc3282dea8a6824ddcdafe9f48296118d6ecc20dc5f13bc84ae952510d801"));
    }

    #[test]
    fn test_for_root() {
        let storage = LocalStorage::new();
        let tree = Tree::for_root(&storage, Hash::from_hex(&"abcdef"));
        println!("{}", rep_subtree(&tree.root));
        assert_eq!(tree.store(&storage), Hash::from_hex(&"abcdef"));
    }

    #[test]
    fn test_write() {
        let storage = LocalStorage::new();
        let tree = Tree::empty(&storage)
            .write(&[], "rt".to_string())
            .unwrap()
            .write(&["foo", "bar"], "xyz".to_string())
            .unwrap()
            .write(&["foo", "bing"], "ggg".to_string())
            .unwrap()
            .write(&["foo"], "short".to_string())
            .unwrap()
            .write(&["foo", "bar", "qux"], "qqq".to_string())
            .unwrap();
        assert_eq!(rep_subtree(&tree.root),
                   "{Some(\"rt\"); foo: {Some(\"short\"); bar: {Some(\"xyz\"); qux: {Some(\"qqq\"); }}, bing: {Some(\"ggg\"); }}}");
        assert_eq!(tree.store(&storage),
                   Hash::from_hex(&"4dea115efe72d154edf7af8cd9cdd952a556ebd2ea9239f789835003a1abad08"));
    }

    #[test]
    fn test_overwrite() {
        let storage = LocalStorage::new();
        let tree = Tree::empty(&storage)
            .write(&["foo", "bar"], "abc".to_string())
            .unwrap()
            .write(&["foo", "bar"], "def".to_string())
            .unwrap();
        assert_eq!(rep_subtree(&tree.root),
                   "{None; foo: {None; bar: {Some(\"def\"); }}}");
        assert_eq!(tree.store(&storage),
                   Hash::from_hex(&"f1e01ab2ce24cc5e686f862dd80eca137d6897f8e23ae63c2c29b349278803cc"));
    }

    #[test]
    fn remove_leaf() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        let tree = tree.remove(&["sub", "one"]).unwrap();
        assert_eq!(rep_subtree(&tree.root),
                   "{None; sub: {None; two: {Some(\"2\"); }}, three: {Some(\"3\"); }}");
    }

    #[test]
    fn remove_deep_from_storage() {
        let storage = LocalStorage::new();
        let tree = Tree::empty(&storage)
            .write(&["a", "b", "c", "d"], "value".to_string())
            .unwrap();
        let hash = tree.store(&storage);
        let tree = Tree::for_root(&storage, hash);
        let tree = tree.remove(&["a", "b", "c", "d"]).unwrap();
        assert_eq!(rep_subtree(&tree.root), "{None; }");
    }

    #[test]
    fn read_exists() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        assert_eq!(tree.read(&storage, &["three"]), Ok("3".to_string()));
    }

    #[test]
    fn read_exists_from_storage() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        let hash = tree.store(&storage);
        let tree = Tree::for_root(&storage, hash);
        assert_eq!(tree.read(&storage, &["sub", "two"]), Ok("2".to_string()));
    }

    #[test]
    fn read_empty_path() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        assert_eq!(tree.read(&storage, &[]), Err("path not found".to_string()));
    }

    #[test]
    fn read_not_found() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        assert_eq!(tree.read(&storage, &["notathing"]),
                   Err("path not found".to_string()));
    }

    #[test]
    fn read_blob_name_nonterminal() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        assert_eq!(tree.read(&storage, &["three", "subtree"]),
                   Err("path not found".to_string()));
    }
}
