//! A Git-like versioned filesystem, based on `cas`.  This includes
//! the idea of a "commit" with parent commits and a nested tree structure
//! associated with each commit.
//!
//! # Examples
//!
//! ```
//! use rubbish::cas::Storage;
//! use rubbish::fs::{FileSystem, FS};
//! use rubbish::fs::Tree;
//! let mut storage = Storage::new();
//! let mut fs = FileSystem::new(&storage);
//! // make a child commit with some tree modifications
//! let child = fs.root_commit().make_child(&mut |tree: Tree| -> Result<Tree, String> {
//!     let tree = try!(tree.write(&storage, &["x", "y"], "z".to_string()));
//!     let tree = try!(tree.write(&storage, &["x", "z"], "y".to_string()));
//!     Ok(tree)
//! }).unwrap();
//! // store that modified commit
//! println!("{:?}", child.store(&storage));
//! ```

mod object;
mod fs;
mod commit;
mod tree;
mod traits;

pub use self::object::Object;
pub use self::commit::Commit;
pub use self::tree::Tree;
pub use self::fs::FileSystem;
pub use self::traits::FS;
