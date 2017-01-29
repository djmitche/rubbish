//! A Git-like versioned filesystem, based on `cas`.  This includes
//! the idea of a "commit" with parent commits and a nested tree structure
//! associated with each commit.  It does not attempt to store a deep history
//! for the filesystem, instead garbage collecting commits beyond configured
//! thresholds even if they are part of the active history.
//!
//! # Examples
//!
//! ```
//! use rubbish::cas::Storage;
//! use rubbish::fs::Commit;
//! let mut storage = Storage::new();
//! let root_commit = Commit::root();
//! let new_tree = root_commit.tree()
//!     .write(&storage, &["a", "b"], "some-value".to_string()).unwrap()
//!     .write(&storage, &["a", "c"], "another-value".to_string()).unwrap();
//! println!("{:?}", new_tree.store(&mut storage));
//! // TODO: make a new child commit
//! ```

// TEMPORARY
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

// TODO: use a type alias for ContentAddressibleStorage<Object>

mod object;
pub use self::object::Object;

mod commit;
pub use self::commit::Commit;

mod tree;
pub use self::tree::Tree;
