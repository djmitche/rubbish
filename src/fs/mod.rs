//! A Git-like versioned filesystem, based on `cas`.  This includes
//! the idea of a "commit" with parent commits and a nested tree structure
//! associated with each commit.
//!
//! # Examples
//!
//! ```
//! use rubbish::cas::Storage;
//! use rubbish::fs::Commit;
//! use rubbish::fs::Tree;
//! let mut storage = Storage::new();
//! // fetch the root (empty) commit
//! let root_commit = Commit::root();
//! // make a child commit with some tree modifications
//! let child = Commit::root().make_child(&mut |tree: Tree| -> Result<Tree, String> {
//!     let tree = try!(tree.write(&mut storage, &["x", "y"], "z".to_string()));
//!     let tree = try!(tree.write(&mut storage, &["x", "z"], "y".to_string()));
//!     Ok(tree)
//! }).unwrap();
//! // store that modified commit
//! println!("{:?}", child.store(&mut storage));
//! ```

mod object;
pub use self::object::Object;

mod commit;
pub use self::commit::Commit;

mod tree;
pub use self::tree::Tree;

// type alias for brevity
use cas::ContentAddressibleStorage;
pub type ObjectStorage = ContentAddressibleStorage<Object>;
