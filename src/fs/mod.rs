//! `fs` implements A Git-like versioned filesystem, based on `cas`.  This includes the idea of a
//! "commit" with parent commits and a nested tree structure associated with each commit.
//!
//! # Examples
//!
//! ```
//! use rubbish::cas::{Storage, CAS};
//! use failure::Fallible;
//! use rubbish::fs::{FileSystem, Commit, Error};
//! let storage = Storage::new();
//! let fs = FileSystem::new(Box::new(storage));
//!
//! fn child(fs: &FileSystem, parent: Commit, path: &[&str], data: Vec<u8>) -> Fallible<Commit> {
//!     let child_tree = parent.tree(fs)?.write(fs, path, data)?;
//!     let child = parent.make_child(&fs, &child_tree)?;
//!     Ok(child)
//! }
//!
//! // make a series of commits, each with one change
//! let cmt = Commit::root(&fs).unwrap();
//! let cmt = child(&fs, cmt, &["a"], vec![1]).unwrap();
//! let cmt = child(&fs, cmt, &["b"], vec![2, 2]).unwrap();
//! let cmt = child(&fs, cmt, &["c"], vec![3, 3, 3]).unwrap();
//! let hash = cmt.hash(&fs).unwrap();
//!
//! // reload that based on its hash and verify the contents
//! let cmt = Commit::for_hash(hash);
//! let tree = cmt.tree(&fs).unwrap();
//! assert_eq!(tree.read(&fs, &["b"]).unwrap(), Some(vec![2, 2]));
//! ```

mod commit;
mod content;
mod fs;
mod lazy;
mod tree;

#[cfg(test)]
mod hashes;

mod error;
pub use self::error::*;

pub use self::commit::Commit;
pub use self::fs::FileSystem;
pub use self::tree::Tree;
