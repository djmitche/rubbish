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
pub struct LazyHashedObject<'f, C: 'f + CAS, T>
    where T: LazyContent<'f, C>
{
    /// The hash of this object, if it has already been calculated
    hash: Option<Hash>,

    /// The content of this object, if it has already been loaded
    content: Option<T>,

    // use the type parameters, since rust does not consider a trait bound
    // to be a "use' of a type paramter
    _phantom: &'f PhantomData<C>,
}

pub trait LazyContent<'f, C: 'f + CAS>: Sized {
    /// Retrive this content from the given storage
    fn retrieve_from(fs: &'f FileSystem<'f, C>, hash: &Hash) -> Result<Self>;
    fn store_in<'a>(&'a self, fs: &'f FileSystem<'f, C>) -> Result<Hash>;
}

impl<'f, C: 'f + CAS, T> LazyHashedObject<'f, C, T>
    where T: LazyContent<'f, C>
{
    /// Return a reference to PhantomData with the appropriate lifetime.  PhantomData is a
    /// zero-byte data structure, so lifetime isn't a relevant concept and upgrading its
    /// lifetime is a harmless hack.
    fn phantom_hack() -> &'f PhantomData<C> {
        let zero_bytes_live_forever: &PhantomData<C> = &PhantomData;
        unsafe {
            (zero_bytes_live_forever as *const PhantomData<C>)
                .as_ref()
                .unwrap()
        }
    }

    pub fn for_content(content: T) -> Self {
        LazyHashedObject {
            hash: None,
            content: Some(content),
            _phantom: LazyHashedObject::<'f, C, T>::phantom_hack(),
        }
    }

    pub fn for_hash(hash: &Hash) -> Self {
        LazyHashedObject {
            hash: Some(hash.clone()),
            content: None,
            _phantom: LazyHashedObject::<'f, C, T>::phantom_hack(),
        }
    }

    /// Ensure that self.hash is not None. This may write the commit to storage,
    /// so it may fail and thus returns a Result.
    fn ensure_hash<'a>(&'a mut self, fs: &'f FileSystem<'f, C>) -> Result<()> {
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

    // TODO: I think 'b here means "however long the caller wants it to last" which isn't
    // what we want..
    pub fn hash<'a, 'b>(&'a mut self, fs: &'f FileSystem<'f, C>) -> Result<&'b Hash> {
        self.ensure_hash(fs)?;
        match self.hash {
            None => unreachable!(),
            Some(ref h) => {
                Ok(unsafe {
                       // "upgrade" the lifetime of h to that of self based on the invariant
                       (h as *const Hash).as_ref().unwrap()
                   })
            }
        }
    }

    fn ensure_content<'a>(&'a mut self, fs: &'f FileSystem<'f, C>) -> Result<()> {
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

    // TODO: I think 'b here means "however long the caller wants it to last" which isn't
    // what we want..
    pub fn content<'a, 'b>(&'a mut self, fs: &'f FileSystem<'f, C>) -> Result<&'b T> {
        self.ensure_content(fs)?;
        match self.content {
            None => unreachable!(),
            Some(ref c) => {
                Ok(unsafe {
                       // "upgrade" the lifetime of c to that of self based on the invariant
                       (c as *const T).as_ref().unwrap()
                   })
            }
        }
    }
}
