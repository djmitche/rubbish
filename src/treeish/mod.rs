//! A Git-like versioned filesystem, based on `cas`.  This includes
//! the idea of a "commit" with parent commits and a nested tree structure
//! associated with each commit.  It does not attempt to store a deep history
//! for the filesystem, instead garbage collecting commits beyond configured
//! thresholds even if they are part of the active history.
//!
//! # Examples
//!
//! ```
//! use rubbish::treeish::{Treeish, Commit};
//! use rubbish::cas::Storage;
//! let mut treeish = Treeish::new(Storage::new());
//! let commit = Commit::empty();
//! let commit_hash = treeish.add_commit(&commit);
//! ```

mod object;

mod tree;
pub use self::tree::TreeEntry;

mod commit;
pub use self::commit::Commit;

mod treeish;
pub use self::treeish::Treeish;
