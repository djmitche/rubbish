use crate::cas::CAS;

// TODO: use pub(crate)

/// A FileSystem encapsulates commits, trees, and so on. These objects are stored into and
/// retrieved from storage lazily (as necessary).  Reading occurs when values are requested, and
/// storage occurs when a hash is generated.
#[derive(Debug)]
pub struct FileSystem {
    pub storage: Box<dyn CAS>,
}

impl FileSystem {
    pub fn new(storage: Box<dyn CAS>) -> FileSystem {
        FileSystem { storage: storage }
    }
}
