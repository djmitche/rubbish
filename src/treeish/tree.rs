use std::collections::HashMap;
use std::result::Result;
use std::sync::Arc;

// TODO: use std::borrow::Cow to return maybe-copied stuff
// TODO: use strings instead of vectors
// TODO: write a remove method
// TODO: when testing, use a generation number or something to check path-copying

/// A Tree represents an image of a tree-shaped data structure, sort of like a filesystem directoy.
/// However, directories can have associated data (that is, there can be data at `foo/bar` and at
/// `foo/bar/bing`).
#[derive(PartialEq, Debug)]
pub struct Tree {
    data: Option<Vec<u8>>,
    children: HashMap<String, Arc<Tree>>,
}

impl Tree {
    /// Create a new, empty Tree
    pub fn new() -> Tree {
        Tree {
            data: None,
            children: HashMap::new(),
        }
    }

    /// Read the value at the given path in this tree, returning an error if this fails,
    /// such as if no such value has been set.
    // TODO: Option instead of Result
    pub fn read(&self, fullpath: &[&str]) -> Result<&Vec<u8>, String> {
        let mut path = fullpath;
        let mut tree = self;

        while path.len() > 0 {
            if let Some(sub) = tree.children.get(path[0]) {
                tree = sub;
            } else {
                return Err(format!("{:?} not found", fullpath));
            }
            path = &path[1..];
        }
        match tree.data {
            Some(ref data) => Ok(data),
            None => Err(format!("{:?} not found", fullpath)),
        }
    }

    /// Write a new value to the tree, overwriting anything already at that path.  This
    /// includes overwriting "directories" unlike UNIX filesystems.
    ///
    /// Writing uses path copying to copy a minimal amount of tree data such that the
    /// original tree is not modified and a new tree is returned, sharing data where
    /// possible.
    pub fn write(&self, path: &[&str], data: Vec<u8>) -> Tree {
        if path.len() == 0 {
            let mut tree = *self.clone();
            tree.data = Some(data);
            return tree;
        }

        match self.children.get(path[0]) {
            Some(ref child) => {
                let mut tree = *self.clone();
                let newchild = child.write(&path[1..], data);
                tree.children.insert(path[0], Arc::new(newchild));
                return tree;
            }
            None => {
                let mut tree = *self.clone();
                let newchild = Tree::new();
            }
            if let Some(sub) = iter.children.get(path[0]) {
                iter = &mut *sub.clone();
            } else {
                let newtree = Arc::new(Tree::new());
                iter.children.insert(path[0], newtree);

                iter = Tree::new();
            }
            path = &path[1..];
        }
        if path.len() == 0 {
            let mut tree = self.clone(); // TODO: what if the data hasn't changed?
            tree.data = Some(data);
            return tree;
        }

        match self {
            // overwrite a leaf node with a subtree
            &Tree::Blob { data: _ } => Tree::_linear_tree(path, data),
            &Tree::SubTree { ref children } => {
                let mut newchildren = children.clone();
                let newchild = match children.get(path[0]) {
                    // write into an existing subtree
                    Some(subtree) => subtree.write(&path[1..], data),
                    // create a new subtree
                    None => Tree::_linear_tree(&path[1..], data),
                };
                newchildren.insert(path[0].to_string(), Arc::new(newchild));
                Tree::SubTree { children: newchildren }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::Tree;

    fn make_test_tree() -> Tree {
        Tree::new()
            .write(&["sub", "one"], vec![1])
            .write(&["sub", "two"], vec![2])
            .write(&["three"], vec![3])
    }

    fn rep_tree(tree: &Tree) -> String {
        match tree {
            &Tree::Blob { ref data } => format!("{:?}", data),
            &Tree::SubTree { ref children } => {
                let mut keys = children.keys().collect::<Vec<&String>>();
                keys.sort();
                let reps = keys.iter()
                    .map(|k| format!("{}: {}", k, rep_tree(&children.get(&k[..]).unwrap())))
                    .collect::<Vec<String>>();
                format!("{{{}}}", reps.join(", "))
            }
        }
    }

    #[test]
    fn test_rep_tree() {
        let tree = make_test_tree();
        assert_eq!(rep_tree(&tree),
                   "{sub: {one: [1], two: [2]}, three: [3]}".to_string());
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

    #[test]
    fn test_new_tree() {
        assert_eq!(rep_tree(&Tree::new()), "{}".to_string());
    }

    #[test]
    fn test_write_deep() {
        let tree = Tree::new().write(&["a", "b", "c", "d", "e"], vec![1, 2]);
        assert_eq!(rep_tree(&tree),
                   "{a: {b: {c: {d: {e: [1, 2]}}}}}".to_string());
    }

    #[test]
    fn test_write_subdir() {
        let tree = Tree::new()
            .write(&["a", "b", "x", "y"], vec![7])
            .write(&["a", "b", "c", "d", "e"], vec![1, 2]);
        assert_eq!(rep_tree(&tree),
                   "{a: {b: {c: {d: {e: [1, 2]}}, x: {y: [7]}}}}".to_string());
    }

    #[test]
    fn test_write_toplevel() {
        let tree = Tree::new()
            .write(&["x", "y"], vec![7])
            .write(&["a", "b", "c", "d", "e"], vec![1, 2]);
        assert_eq!(rep_tree(&tree),
                   "{a: {b: {c: {d: {e: [1, 2]}}}}, x: {y: [7]}}".to_string());
    }

    #[test]
    fn test_write_overwrite_blob_with_dir() {
        let tree = Tree::new()
            .write(&["x", "y"], vec![7])
            .write(&["x", "y", "z"], vec![8]);
        assert_eq!(rep_tree(&tree), "{x: {y: {z: [8]}}}".to_string());
    }

    #[test]
    fn test_write_overwrite_dir_with_blob() {
        let tree = Tree::new()
            .write(&["x", "y", "z"], vec![8])
            .write(&["x", "y"], vec![7]);
        assert_eq!(rep_tree(&tree), "{x: {y: [7]}}".to_string());
    }

    #[test]
    fn test_write_overwrite_blob_with_blob() {
        let tree = Tree::new()
            .write(&["x", "y"], vec![8])
            .write(&["x", "y"], vec![7]);
        assert_eq!(rep_tree(&tree), "{x: {y: [7]}}".to_string());
    }
}
