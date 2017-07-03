use fs::error::*;
use fs::fs::FileSystem;
use cas::Hash;
use cas::CAS;
use std::marker::PhantomData;

// TODO: use pub(crate)

/// Each object (tree or commit) has a hash and can either be loaded (full of data) or not (just a
/// hash).  INVARIANT: at least one of `hash` and `content` is always set, and once set, neither is
/// modified.
#[derive(Debug)]
pub struct LazyHashedObject<'a, T, C>
    where C: 'a + CAS,
          T: LazyContent<'a, C>
{
    /// The hash of this object, if it has already been calculated
    hash: Option<Hash>,

    /// The content of this object, if it has already been loaded
    content: Option<T>,

    // this seems pretty stupid..
    _phantom: PhantomData<&'a C>,
}


pub trait LazyContent<'a, C>: Sized
    where C: 'a + CAS
{
    /// Retrive this content from the given storage
    fn retrieve_from(fs: &'a FileSystem<'a, C>, hash: &Hash) -> Result<Self>;
    fn store_in(&self, fs: &FileSystem<'a, C>) -> Result<Hash>;
}

impl<'a, C, T> LazyHashedObject<'a, T, C>
    where C: 'a + CAS,
          T: LazyContent<'a, C>
{
    pub fn for_content(content: T) -> Self {
        LazyHashedObject {
            hash: None,
            content: Some(content),
            _phantom: PhantomData {},
        }
    }

    pub fn for_hash(hash: &Hash) -> Self {
        LazyHashedObject {
            hash: Some(hash.clone()),
            content: None,
            _phantom: PhantomData {},
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
                self.hash = Some(c.store_in(fs)?);
                Ok(())
            }
        }
    }

    pub fn hash(&mut self, fs: &FileSystem<'a, C>) -> Result<&'a Hash> {
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
                self.content = Some(T::retrieve_from(fs, hash)?);
                Ok(())
            }
        }
    }

    pub fn content(&mut self, fs: &'a FileSystem<'a, C>) -> Result<&'a T> {
        self.ensure_content(fs)?;
        match self.content {
            None => unreachable!(),
            Some(ref c) => {
                Ok(unsafe {
                       // "upgrade" the lifetime of c to 'a based on the invariant
                       (c as *const T).as_ref().unwrap()
                   })
            }
        }
    }
}
