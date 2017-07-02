use fs::error::*;
use cas::Hash;
use cas::CAS;
use std::cell::RefCell;
use std::rc::Rc;

/// A FileSystem encapsulates commits, trees, and so on. These objects are stored into and
/// retrieved from storage lazily (as necessary).  Reading occurs when values are requested, and
/// storage occurs when a hash is generated.
#[derive(Debug)]
pub struct FileSystem<'a, C: 'a + CAS> {
    storage: &'a C,
}

impl<'a, C> FileSystem<'a, C>
    where C: 'a + CAS
{
    pub fn new(storage: &'a C) -> FileSystem<'a, C> {
        FileSystem { storage: storage }
    }

    /// Get the root commit -- a well-known commit with no parents and an empty tree.
    fn root_commit(&self) -> Rc<Commit<C>> {
        Commit::root(self)
    }

    /// Get a commit given its hash.
    ///
    /// Note that this does not actually load the commit; that occurs lazily, later.
    fn get_commit(&self, hash: &Hash) -> Rc<Commit<C>> {
        // this function takes a reference to the hash because it may someday cache recently used
        // commits, at which point the hash would not be consumed.
        Commit::for_hash(self, hash)
    }
}

// Each object (tree or commit) has a hash and can either be loaded (full of data) or not (just a
// hash). Each object carries around a reference to its parent FileSystem. Each object carries
// an Option<Hash> which is only set if the hash is known and is never *unset*.. how can we tell
// Rust this? (unsafe!)

#[derive(Debug)]
pub struct Commit<'a, C: 'a + CAS> {
    /// The filesystem within which this commit exists
    fs: &'a FileSystem<'a, C>,
    inner: RefCell<InnerCommit<'a, C>>,
}

#[derive(Debug)]
struct InnerCommit<'a, C: 'a + CAS> {
    /// The hash of this commit, if it has already been calculated
    hash: Option<Hash>,

    /// The content of this commit, if it has already been loaded
    content: Option<CommitContent<'a, C>>,
}

#[derive(Debug)]
struct CommitContent<'a, C: 'a + CAS> {
    /// Parent commits
    parents: Vec<Rc<Commit<'a, C>>>,
    // TODO: tree
}

#[derive(Debug, RustcDecodable, RustcEncodable)]
struct RawCommit {
    parents: Vec<Hash>,
}

impl<'a, C> Commit<'a, C>
    where C: 'a + CAS
{
    /// Return a refcounted root commit
    fn root<'b>(fs: &'b FileSystem<C>) -> Rc<Commit<'b, C>> {
        Rc::new(Commit {
                    fs: fs,
                    inner: RefCell::new(InnerCommit::for_content(vec![])),
                })
    }

    /// Return a refcounted commit for the given hash
    fn for_hash<'b>(fs: &'b FileSystem<C>, hash: &Hash) -> Rc<Commit<'b, C>> {
        Rc::new(Commit {
                    fs: fs,
                    inner: RefCell::new(InnerCommit::for_hash(hash)),
                })
    }

    /// Get the hash for this commit
    pub fn hash(&self) -> Result<&Hash> {
        self.inner.borrow_mut().hash(self.fs)
    }

    pub fn parents(&'a self) -> Result<&'a [Rc<Commit<'a, C>>]> {
        let content = self.inner.borrow_mut().content(self.fs)?;
        Ok(&content.parents[..])
    }
}


// INVARIANT: at least one of `hash` and `content` is always set, and once set, neither is modified.
impl<'a, C> InnerCommit<'a, C>
    where C: 'a + CAS
{
    fn for_content(parents: Vec<Rc<Commit<'a, C>>>) -> Self {
        InnerCommit {
            hash: None,
            content: Some(CommitContent { parents: parents }),
        }
    }

    fn for_hash(hash: &Hash) -> Self {
        InnerCommit {
            hash: Some(hash.clone()),
            content: None,
        }
    }

    /// Ensure that self.hash is not None. This may write the commit to storage,
    /// so it may fail and thus returns a Result.
    fn ensure_hash(&mut self, fs: &FileSystem<'a, C>) -> Result<()> {
        if let Some(_) = self.hash {
            return Ok(());
        }

        match self.content {
            // based on the invariant, since hash is not set, content is set
            None => unreachable!(),
            Some(ref c) => {
                let mut parent_hashes: Vec<Hash> = vec![];
                parent_hashes.reserve(c.parents.len());
                for p in c.parents.iter() {
                    let phash = p.hash()?.clone();
                    parent_hashes.push(phash);
                }
                let raw = RawCommit { parents: parent_hashes };
                let hash = fs.storage.store(&raw)?;
                self.hash = Some(hash);
                Ok(())
            }
        }
    }

    fn hash(&mut self, fs: &FileSystem<'a, C>) -> Result<&'a Hash> {
        self.ensure_hash(fs)?;
        match self.hash {
            None => unreachable!(),
            Some(ref h) => {
                Ok(unsafe {
                       // "upgrade" the lifetime of h to 'a based on the invariant
                       (h as *const Hash).as_ref().unwrap()
                   })
            }
        }
    }

    fn ensure_content(&mut self, fs: &'a FileSystem<'a, C>) -> Result<()> {
        if let Some(_) = self.content {
            return Ok(());
        }

        match self.hash {
            // based on the invariant, since content is not set, hash is set
            None => unreachable!(),
            Some(ref hash) => {
                let raw: RawCommit = fs.storage.retrieve(hash)?;
                let mut parents: Vec<Rc<Commit<'a, C>>> = vec![];
                for h in raw.parents.iter() {
                    parents.push(Commit::for_hash(fs, h));
                }
                self.content = Some(CommitContent::<'a, C> { parents: parents });
                Ok(())
            }
        }
    }

    fn content(&mut self, fs: &'a FileSystem<'a, C>) -> Result<&'a CommitContent<'a, C>> {
        self.ensure_content(fs)?;
        match self.content {
            None => unreachable!(),
            Some(ref c) => {
                Ok(unsafe {
                       // "upgrade" the lifetime of c to 'a based on the invariant
                       (c as *const CommitContent<'a, C>).as_ref().unwrap()
                   })
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::FileSystem;
    use cas::LocalStorage;
    use cas::Hash;

    const ROOT_HASH: &'static str = "af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc";

    #[test]
    fn test_root() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        let root = fs.root_commit();
        assert_eq!(root.hash().unwrap(), &Hash::from_hex(ROOT_HASH));
        assert_eq!(root.parents().unwrap().len(), 0);
    }

    #[test]
    fn test_for_hash() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);
        let root = fs.get_commit(&Hash::from_hex("012345"));
        assert_eq!(root.hash().unwrap(), &Hash::from_hex("012345"));
        // there's no such object with that hash, so getting parents fails
        assert!(root.parents().is_err());
    }
}
