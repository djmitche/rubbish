use cas::CAS;
use cas::Hash;
use fs::traits::Commit as CommitTrait;
use fs::traits::CommitUpdater as CommitUpdaterTrait;
use fs::traits::FS;
use std::collections::HashMap;
use std::sync::Arc;

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

pub struct FileSystem<'a, ST: 'a + CAS<Object>> {
    storage: &'a ST,
}

impl<'a, ST> FileSystem<'a, ST>
    where ST: 'a + CAS<Object>
{
    pub fn new(storage: &'a ST) -> FileSystem<'a, ST> {
        FileSystem {
            storage: storage,
        }
    }
}

impl<'a, ST> FS for FileSystem<'a, ST> 
    where ST: 'a + CAS<Object>
{
    type Commit = Commit<'a, ST>;

    fn root_commit(&self) -> Self::Commit {
        Commit {
            storage: self.storage,
            tree: Tree::empty(self.storage),
            parents: vec![],
        }
    }

    fn get_commit(&self, hash: Hash) -> Result<Self::Commit, String> {
        Commit::retrieve(self.storage, hash)
    }
}

/// A Tree represents an image of a tree-shaped data structure, sort of like a filesystem directoy.
/// However, directories can have associated data (that is, there can be data at `foo/bar` and at
/// `foo/bar/bing`).
#[derive(Debug)]
pub struct Tree<'a, ST>
    where ST: 'a + CAS<Object>
{
    storage: &'a ST,
    root: SubTree<'a, ST>,
}

#[derive(Debug)]
struct Node<'a, ST>
    where ST: 'a + CAS<Object>
{
    storage: &'a ST,
    data: Option<String>,
    children: HashMap<String, SubTree<'a, ST>>,
}

#[derive(Debug)]
enum SubTree<'a, ST>
    where ST: 'a + CAS<Object>
{
    Unresolved(Hash),
    // TODO: Rc might be sufficient
    Resolved(Arc<Node<'a, ST>>),
}

impl<'a, ST> Clone for Tree<'a, ST>
    where ST: 'a + CAS<Object>
{
    fn clone(&self) -> Self {
        Tree {
            storage: self.storage,
            root: self.root.clone(),
        }
    }
}

impl<'a, ST> Tree<'a, ST>
    where ST: 'a + CAS<Object>
{
    /// Create a new tree with the given root hash
    pub fn for_root(storage: &'a ST, root: Hash) -> Tree<'a, ST> {
        Tree {
            storage: storage,
            root: SubTree::Unresolved(root),
        }
    }

    /// Create a new, empty tree
    pub fn empty(storage: &'a ST) -> Tree<'a, ST> {
        let root = SubTree::Resolved(Arc::new(Node{
            storage: storage,
            data: None,
            children: HashMap::new(),
        }));
        Tree {
            storage: storage,
            root: root,
        }
    }

    fn store_subtree(storage: &'a ST, subtree: &SubTree<'a, ST>) -> Hash {
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
    pub fn store(&self) -> Hash {
        Tree::store_subtree(self.storage, &self.root)
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
    pub fn write<'b>(&self, path: &'b [&str], data: String) -> Result<Tree<'a, ST>, String> {
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
    pub fn remove(&self, path: &[&str]) -> Result<Tree<'a, ST>, String> {
        self.modify(path, None)
    }

    /// Read the value at the given path in this tree, returning an error if this fails.
    /// If no value is set at the given path, that is considered an error.
    pub fn read(&self, path: &[&str]) -> Result<String, String> {
        let mut node = try!(self.root.resolve(self.storage));

        for name in path {
            node = match node.children.get(&name.to_string()) {
                Some(ref subtree) => {
                    try!(subtree.resolve(self.storage))
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

    /// Set the data at the given path, returning a new Tree that shares
    /// some nodes with the original via path copying.
    /// 
    /// This prunes empty directories.
    fn modify(&self, path: &[&str], data: Option<String>) -> Result<Tree<'a, ST>, String> {
        let resolved: Arc<Node<'a, ST>> = try!(self.root.resolve(self.storage));

        // first, make a stack of owned nodes, creating or cloning them as necessary
        let mut node_stack: Vec<Node<'a, ST>> = vec![(*resolved).clone()];
        for name in path {
            let new_node = {
                let node: &Node<'a, ST> = node_stack.last().unwrap();
                match node.children.get(&name.to_string()) {
                    Some(ref subtree) => {
                        let resolved = try!(subtree.resolve(self.storage));
                        (*resolved).clone()
                    },
                    None => {
                        // push a new, empty node onto the stack
                        Node{
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
        let mut iter: Node<'a, ST> = node_stack.pop().unwrap();
        while node_stack.len() > 0 {
            let mut parent: Node<'a, ST> = node_stack.pop().unwrap();
            let name = path[node_stack.len()].to_string();

            // if iter is empty, omit it from its parent
            if iter.data == None && iter.children.len() == 0 {
                parent.children.remove(&name);
            } else {
                parent.children.insert(name, SubTree::Resolved(Arc::new(iter)));
            }
            iter = parent;
        }

        // return a new tree, rooted at the final new node
        return Ok(Tree{
            storage: self.storage,
            root: SubTree::Resolved(Arc::new(iter)),
        });
    }
}

impl<'a, ST> Clone for Node<'a, ST>
    where ST: 'a + CAS<Object>
{
    fn clone(&self) -> Self {
        Node {
            storage: self.storage,
            data: self.data.clone(),
            children: self.children.clone(),
        }
    }
}

impl<'a, ST> Clone for SubTree<'a, ST>
    where ST: 'a + CAS<Object>
{
    fn clone(&self) -> Self {
        match *self {
            SubTree::Unresolved(ref h) => SubTree::Unresolved(h.clone()),
            SubTree::Resolved(ref n) => SubTree::Resolved(n.clone()),
        }
    }
}

impl<'a, ST> SubTree<'a, ST>
    where ST: 'a + CAS<Object>
{
    /// Resolve this SubTree to an Arc<Node>, retrieving if necessary.
    fn resolve(&self, storage: &'a ST) -> Result<Arc<Node<'a, ST>>, String> {
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
                            storage: storage,
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

#[derive(Debug)]
enum Parent<'a, ST>
    where ST: 'a + CAS<Object>
{
    Unresolved(Hash),
    Resolved(Commit<'a, ST>),
}

pub struct CommitUpdater<'a, ST: 'a + CAS<Object>>{
    parent: Commit<'a, ST>,
    tree: Tree<'a, ST>,
}

impl<'a, ST> CommitUpdaterTrait for CommitUpdater<'a, ST>
    where ST: 'a + CAS<Object>
{
    type Commit = Commit<'a, ST>;

    fn write(&mut self, path: &[&str], data: String) -> &mut Self {
        self.tree = self.tree.write(path, data).unwrap(); // TODO: keep errors until end
        self
    }

    fn commit(&self) -> Commit<'a, ST> {
        Commit {
            storage: self.parent.storage,
            tree: self.tree.clone(),
            parents: vec![Parent::Resolved(self.parent.clone())],
        }
    }
}

#[derive(Debug)]
pub struct Commit<'a, ST: 'a + CAS<Object>> {
    storage: &'a ST,
    tree: Tree<'a, ST>,
    parents: Vec<Parent<'a, ST>>,
}

// TODO: quit it with the clonin'
impl<'a, ST> Clone for Commit<'a, ST>
    where ST: 'a + CAS<Object>
{
    fn clone(&self) -> Self {
        Commit {
            storage: self.storage,
            tree: self.tree.clone(),
            parents: self.parents.clone(),
        }
    }
}

// TODO: quit it with the clonin'
impl<'a, ST> Clone for Parent<'a, ST>
    where ST: 'a + CAS<Object>
{
    fn clone(&self) -> Self {
        match *self {
            Parent::Unresolved(ref h) => Parent::Unresolved(h.clone()),
            Parent::Resolved(ref c) => Parent::Resolved(c.clone()),
        }
    }
}

impl<'a, ST> CommitTrait for Commit<'a, ST>
    where ST: 'a + CAS<Object>
{
    type CommitUpdater = CommitUpdater<'a, ST>;

    fn update(&self) -> CommitUpdater<'a, ST> {
        CommitUpdater{
            parent: self.clone(),
            tree: self.tree.clone(),
        }
    }
}

impl<'a, ST> Commit<'a, ST>
    where ST: 'a + CAS<Object>
{
    /// Get the tree at this commit
    pub fn tree(&self) -> Tree<'a, ST> {
        self.tree.clone()
    }

    /// Create a child commit based on this one, applying the modifier function to the enclosed
    /// tree.  This function can call any Tree methods, or even return an entirely unrelated Tree.
    /// If the modifier returns an error, make_child does as well.
    pub fn make_child<F>(&self, mut modifier: F) -> Result<Commit<'a, ST>, String>
        where F: FnMut(Tree<'a, ST>) -> Result<Tree<'a, ST>, String> {
        let new_tree = try!(modifier(self.tree.clone()));
        Ok(Commit {
            storage: self.storage,
            tree: new_tree,
            parents: vec![Parent::Resolved((*self).clone())],
        })
    }

    /// Get a commit from storage, given its hash
    pub fn retrieve(storage: &'a ST, commit: Hash) -> Result<Commit<'a, ST>, String> {
        if let Some(obj) = storage.retrieve(&commit) {
            if let Object::Commit{tree, parents} = obj {
                let mut parent_commits = vec![];
                parent_commits.reserve(parents.len());
                for parent_hash in parents {
                    parent_commits.push(Parent::Unresolved(parent_hash));
                }
                Ok(Commit{
                    storage: storage,
                    tree: Tree::for_root(storage, tree),
                    parents: parent_commits,
                })
            } else {
                Err("not a commit".to_string())
            }
        } else {
            Err("no object with that hash".to_string())
        }
    }

    /// Store this commit and return the hash
    pub fn store(&self) -> Hash {
        let mut parent_hashes = vec![];
        parent_hashes.reserve(self.parents.len());
        for parent in &self.parents {
            match parent {
                &Parent::Unresolved(ref hash) => {
                    parent_hashes.push(hash.clone());
                },
                &Parent::Resolved(ref commit) => {
                    parent_hashes.push(commit.store());
                }
            }
        }

        let tree_hash = self.tree.store();

        let obj = Object::Commit {
            tree: tree_hash,
            parents: parent_hashes,
        };
        self.storage.store(&obj)
    }
}

#[cfg(test)]
mod test {
    use fs::FS;
    use super::{FileSystem, Tree, SubTree, Object};
    use cas::{LocalStorage, Hash, CAS};


    const ROOT_HASH: &'static str = "4e4792b3a91c2cea55575345f94bb20c2d6b8d62a34f7e6099e7fd3a40944836";

    #[test]
    fn test_root() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        assert_eq!(
            fs.root_commit().store(),
            Hash::from_hex(&ROOT_HASH));
    }

    #[test]
    fn test_make_child() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        fn mutator<'a>(tree: Tree<'a, LocalStorage<Object>>) -> Result<Tree<'a, LocalStorage<Object>>, String> {
            let tree = try!(tree.write(&["x", "y"], "Y".to_string()));
            let tree = try!(tree.write(&["x", "z"], "Z".to_string()));
            Ok(tree)
        }

        let child = fs.root_commit().make_child(mutator).unwrap();
        println!("child commit: {:?}", child);

        let child_hash = child.store();
        println!("child hash: {:?}", child_hash);

        // unpack those objects from storage to verify their form..

        println!("UNPACKING");
        let child_obj = storage.retrieve(&child_hash).unwrap();
        println!("child object: {:?} = {:?}", child_hash, child_obj);
        let (tree_hash, parents) = match child_obj {
            Object::Commit{tree, parents} => (tree, parents),
            _ => panic!("not a commit"),
        };

        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0], Hash::from_hex(&ROOT_HASH));

        let tree_obj = storage.retrieve(&tree_hash).unwrap();
        println!("tree object: {:?} = {:?}", tree_hash, tree_obj);
        let (data, children) = match tree_obj {
            Object::Tree{data, children} => (data, children),
            _ => panic!("not a tree"),
        };

        assert_eq!(data, None);
        assert_eq!(children.len(), 1);
        let (ref child_name_x, ref child_hash_x) = children[0];
        assert_eq!(child_name_x, &"x".to_string());

        let tree_obj = storage.retrieve(&child_hash_x).unwrap();
        println!("tree object: {:?} = {:?}", child_hash_x, tree_obj);
        let (data, children) = match tree_obj {
            Object::Tree{data, children} => (data, children),
            _ => panic!("not a tree"),
        };

        assert_eq!(data, None);
        assert_eq!(children.len(), 2);
        let (ref child_name_y, ref child_hash_y) = children[0];
        assert_eq!(child_name_y, &"y".to_string());
        let (ref child_name_z, ref child_hash_z) = children[1];
        assert_eq!(child_name_z, &"z".to_string());

        let tree_obj = storage.retrieve(&child_hash_y).unwrap();
        println!("tree object: {:?} = {:?}", child_hash_y, tree_obj);
        let (data, children) = match tree_obj {
            Object::Tree{data, children} => (data, children),
            _ => panic!("not a tree"),
        };
        assert_eq!(data, Some("Y".to_string()));
        assert_eq!(children.len(), 0);

        let tree_obj = storage.retrieve(&child_hash_z).unwrap();
        println!("tree object: {:?} = {:?}", child_hash_z, tree_obj);
        let (data, children) = match tree_obj {
            Object::Tree{data, children} => (data, children),
            _ => panic!("not a tree"),
        };
        assert_eq!(data, Some("Z".to_string()));
        assert_eq!(children.len(), 0);
    }

    fn make_test_tree<'a, ST>(storage: &'a ST) -> Tree<'a, ST>
        where ST: 'a + CAS<Object>
    {
        Tree::empty(storage)
            .write(&["sub", "one"], "1".to_string()).unwrap()
            .write(&["sub", "two"], "2".to_string()).unwrap()
            .write(&["three"], "3".to_string()).unwrap()
    }

    fn rep_subtree<'a, ST>(subtree: &SubTree<'a, ST>) -> String
        where ST: 'a + CAS<Object>
    {
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
        assert_eq!(
            tree.store(),
            Hash::from_hex(&"387dc3282dea8a6824ddcdafe9f48296118d6ecc20dc5f13bc84ae952510d801"));
    }

    #[test]
    fn test_for_root() {
        let storage = LocalStorage::new();
        let tree = Tree::for_root(&storage, Hash::from_hex(&"abcdef"));
        println!("{}", rep_subtree(&tree.root));
        assert_eq!(
            tree.store(),
            Hash::from_hex(&"abcdef"));
    }

    #[test]
    fn test_write() {
        let storage = LocalStorage::new();
        let tree = Tree::empty(&storage)
            .write(&[], "rt".to_string()).unwrap()
            .write(&["foo", "bar"], "xyz".to_string()).unwrap()
            .write(&["foo", "bing"], "ggg".to_string()).unwrap()
            .write(&["foo"], "short".to_string()).unwrap()
            .write(&["foo", "bar", "qux"], "qqq".to_string()).unwrap();
        assert_eq!(
            rep_subtree(&tree.root),
            "{Some(\"rt\"); foo: {Some(\"short\"); bar: {Some(\"xyz\"); qux: {Some(\"qqq\"); }}, bing: {Some(\"ggg\"); }}}");
        assert_eq!(
            tree.store(),
            Hash::from_hex(&"4dea115efe72d154edf7af8cd9cdd952a556ebd2ea9239f789835003a1abad08"));
    }

    #[test]
    fn test_overwrite() {
        let storage = LocalStorage::new();
        let tree = Tree::empty(&storage)
            .write(&["foo", "bar"], "abc".to_string()).unwrap()
            .write(&["foo", "bar"], "def".to_string()).unwrap();
        assert_eq!(
            rep_subtree(&tree.root),
            "{None; foo: {None; bar: {Some(\"def\"); }}}");
        assert_eq!(
            tree.store(),
            Hash::from_hex(&"f1e01ab2ce24cc5e686f862dd80eca137d6897f8e23ae63c2c29b349278803cc"));
    }

    #[test]
    fn remove_leaf() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        let tree = tree.remove(&["sub", "one"]).unwrap();
        assert_eq!(
            rep_subtree(&tree.root),
            "{None; sub: {None; two: {Some(\"2\"); }}, three: {Some(\"3\"); }}");
    }

    #[test]
    fn remove_deep_from_storage() {
        let storage = LocalStorage::new();
        let tree = Tree::empty(&storage)
            .write(&["a", "b", "c", "d"], "value".to_string()).unwrap();
        let hash = tree.store();
        let tree = Tree::for_root(&storage, hash);
        let tree = tree.remove(&["a", "b", "c", "d"]).unwrap();
        assert_eq!(rep_subtree(&tree.root), "{None; }");
    }

    #[test]
    fn read_exists() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        assert_eq!(tree.read(&["three"]), Ok("3".to_string()));
    }

    #[test]
    fn read_exists_from_storage() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        let hash = tree.store();
        let tree = Tree::for_root(&storage, hash);
        assert_eq!(tree.read(&["sub", "two"]), Ok("2".to_string()));
    }

    #[test]
    fn read_empty_path() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        assert_eq!(tree.read(&[]), Err("path not found".to_string()));
    }

    #[test]
    fn read_not_found() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        assert_eq!(tree.read(&["notathing"]), Err("path not found".to_string()));
    }

    #[test]
    fn read_blob_name_nonterminal() {
        let storage = LocalStorage::new();
        let tree = make_test_tree(&storage);
        assert_eq!(tree.read(&["three", "subtree"]), Err("path not found".to_string()));
    }
}
