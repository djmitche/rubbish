//! A Git-like versioned filesystem, based on `cas`.  This includes
//! the idea of a "commit" with parent commits and a nested tree structure
//! associated with each commit.
//!
//! # Examples
//!
//! ```
//! use rubbish::cas::Storage;
//! use rubbish::fs::{FileSystem, FS, Object};
//! // use rubbish::fs::Tree;
//! let mut storage = Storage::new();
//! let mut fs = FileSystem::new(&storage);
//! // make a child commit with some tree modifications
//! /*
//! // prototype CommitBuilder interface..
//! let child = fs.root_commit().update()
//!     .write(&["x", "y"], "z".to_string())
//!     .write(&["x", "z"], "y".to_string())
//!     commit().unwrap();
//! // store that modified commit
//! println!("{:?}", child.store(&storage));
//! */
//! ```

mod object;
mod fs;
mod commit;
mod tree;
mod traits;

mod error;
pub use self::error::*;

pub use self::object::Object;
pub use self::fs::FileSystem;
pub use self::traits::FS;
