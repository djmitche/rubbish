//! `fs` implements A Git-like versioned filesystem, based on `cas`.  This includes the idea of a
//! "commit" with parent commits and a nested tree structure associated with each commit.
//!
//! # Examples
//!
//! ```
//! use rubbish::cas::{Storage, CAS};
//! use rubbish::fs::{FileSystem, Commit, Error};
//! let storage = Storage::new();
//! let fs = FileSystem::new(&storage);
//!
//! fn child<'f, C: 'f + CAS>(parent: Commit<'f, C>,
//!                           path: &[&str],
//!                           data: String)
//!                           -> Result<Commit<'f, C>, Error> {
//!     let child_tree = parent.tree()?.write(path, data)?;
//!     let child = parent.make_child(child_tree)?;
//!     Ok(child)
//! }
//!
//! // make a series of commits, each with one change
//! let cmt = fs.root_commit();
//! let cmt = child(cmt, &["a"], "Apple".to_string()).unwrap();
//! let cmt = child(cmt, &["b"], "Banana".to_string()).unwrap();
//! let cmt = child(cmt, &["c"], "Cantaloupe".to_string()).unwrap();
//! let hash = cmt.hash().unwrap();
//!
//! // reload that based on its hash and verify the contents
//! let cmt = fs.get_commit(hash);
//! let tree = cmt.tree().unwrap();
//! assert_eq!(tree.read(&["b"]).unwrap(), Some("Banana"));
//! ```

mod fs;
mod lazy;
mod commit;
mod tree;

mod error;
pub use self::error::*;

pub use self::fs::FileSystem;
pub use self::commit::Commit;
pub use self::tree::Tree;
