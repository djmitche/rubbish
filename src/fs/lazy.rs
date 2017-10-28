use fs::error::*;
use fs::fs::FileSystem;
use cas::Hash;
use cas::CAS;
use std::cell::RefCell;
use std::marker::PhantomData;

// TODO: use pub(crate)

/// A LazyHashedObject is an object which can be stored by hash in a FileSystem and retrieved from
/// one.  It can be created with a hash, in which case the object's content (of type T) is loaded
/// only when requested; or it can be created with content, in which case the hash is only
/// determined when requested (with the object stored in the FileSystem at that time).
#[derive(Debug)]
pub struct LazyHashedObject<'f, ST: 'f + CAS, T: LazyContent<'f, ST>>(RefCell<LazyInner<'f, ST, T>>);

/// LazyInner proivdes interior mutability for LazyHashedObject.
///
/// INVARIANT α: at least one of `hash` and `content` is always `Some(_)`.
/// INVARIANT β: a `Some(_)` value for `hash` or `content` is immutable.
#[derive(Debug)]
struct LazyInner<'f, ST: 'f + CAS, T>
where
    T: LazyContent<'f, ST>,
{
    /// The hash of this object, if it has already been calculated
    hash: Option<Hash>,

    /// The content of this object, if it has already been loaded
    content: Option<T>,

    // use the type parameters, since rust does not consider a trait bound
    // to be a "use' of a type paramter
    _phantom: &'f PhantomData<ST>,
}

/// LazyContent bounds content that can be stored as a `LazyHashedObject`, providing
/// methods to retrieve and store the content in a FileSystem.
pub trait LazyContent<'f, ST: 'f + CAS>: Sized {
    /// Retrive this content from the given FileSystem
    fn retrieve_from(fs: &'f FileSystem<'f, ST>, hash: &Hash) -> Result<Self>;

    /// Store the content in the given FileSystem, returning its hash
    fn store_in<'a>(&'a self, fs: &'f FileSystem<'f, ST>) -> Result<Hash>;
}

impl<'f, ST: 'f + CAS, T> LazyHashedObject<'f, ST, T>
where
    T: LazyContent<'f, ST>,
{
    /// Create a new LazyHashedObject containing the given content.  This is a lazy operation, so
    /// no storage occurs until the object's hash is requested.
    pub fn for_content(content: T) -> Self {
        LazyHashedObject(RefCell::new(LazyInner::for_content(content)))
    }

    /// Create a new LazyHashedObject with the given hash.  This is a lazy operation, so the
    /// content is not loaded until requested.
    pub fn for_hash(hash: &Hash) -> Self {
        LazyHashedObject(RefCell::new(LazyInner::for_hash(hash)))
    }

    /// Get the hash for this object, writing its content to the FileSystem first if necessary.
    pub fn hash(&self, fs: &'f FileSystem<'f, ST>) -> Result<&Hash> {
        let mut borrow = self.0.borrow_mut();
        let h = borrow.hash(fs)?;
        Ok(unsafe {
            // "upgrade" h's lifetime from that of the mutable borrow, based on invariant β
            (h as *const Hash).as_ref().unwrap()
        })
    }

    /// Get the content of this object, retrieving it from the FileSystem first if necessary.
    pub fn content(&self, fs: &'f FileSystem<'f, ST>) -> Result<&T> {
        let mut borrow = self.0.borrow_mut();
        let c = borrow.content(fs)?;
        Ok(unsafe {
            // "upgrade" c's lifetime from that of the mutable borrow, based on invariant β
            (c as *const T).as_ref().unwrap()
        })
    }

    /// Does this lazy object already have a hash?
    pub(crate) fn has_hash(&self) -> bool {
        let borrow = self.0.borrow();
        borrow.hash.is_some()
    }

    pub(crate) fn has_content(&self) -> bool {
        let borrow = self.0.borrow();
        borrow.content.is_some()
    }
}

impl<'f, ST: 'f + CAS, T> LazyInner<'f, ST, T>
where
    T: LazyContent<'f, ST>,
{
    // Return a reference to PhantomData with the appropriate lifetime.  PhantomData is a zero-byte
    // data structure, so lifetime isn't a relevant concept and upgrading its lifetime is a
    // harmless hack.
    fn phantom_hack() -> &'f PhantomData<ST> {
        let zero_bytes_live_forever: &PhantomData<ST> = &PhantomData;
        unsafe {
            (zero_bytes_live_forever as *const PhantomData<ST>)
                .as_ref()
                .unwrap()
        }
    }

    fn for_content(content: T) -> Self {
        LazyInner {
            hash: None,
            content: Some(content),
            _phantom: LazyInner::<'f, ST, T>::phantom_hack(),
        }
    }

    fn for_hash(hash: &Hash) -> Self {
        LazyInner {
            hash: Some(hash.clone()),
            content: None,
            _phantom: LazyInner::<'f, ST, T>::phantom_hack(),
        }
    }

    /// Ensure that self.hash is not None. This may write the commit to storage,
    /// so it may fail and thus returns a Result.
    fn ensure_hash<'a>(&'a mut self, fs: &'f FileSystem<'f, ST>) -> Result<()> {
        if let Some(_) = self.hash {
            return Ok(());
        }

        match self.content {
            // based on invariant α, since hash is None, content is Some(_)
            None => unreachable!(),
            Some(ref c) => {
                self.hash = Some(c.store_in(fs)?);
                Ok(())
            }
        }
    }

    fn hash(&mut self, fs: &'f FileSystem<'f, ST>) -> Result<&Hash> {
        self.ensure_hash(fs)?;
        match self.hash {
            None => unreachable!(),
            Some(ref h) => {
                Ok(unsafe {
                    // "upgrade" the lifetime of h to that of self based on invariant β
                    (h as *const Hash).as_ref().unwrap()
                })
            }
        }
    }

    /// Ensure that self.content is not None.  This may require reading the content
    /// from storage, so it may fail and thus returns a result.
    fn ensure_content<'a>(&'a mut self, fs: &'f FileSystem<'f, ST>) -> Result<()> {
        if let Some(_) = self.content {
            return Ok(());
        }

        match self.hash {
            // based on invariant α, since content None, hash is Some(_)
            None => unreachable!(),
            Some(ref hash) => {
                self.content = Some(T::retrieve_from(fs, hash)?);
                Ok(())
            }
        }
    }

    fn content(&mut self, fs: &'f FileSystem<'f, ST>) -> Result<&T> {
        self.ensure_content(fs)?;
        match self.content {
            None => unreachable!(),
            Some(ref c) => {
                Ok(unsafe {
                    // "upgrade" the lifetime of h to that of self based on invariant β
                    (c as *const T).as_ref().unwrap()
                })
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use cas::{LocalStorage, CAS};
    use fs::FileSystem;
    use cas::Hash;

    #[derive(Debug, RustcDecodable, RustcEncodable)]
    struct TestContent(String);

    impl<'f, ST> LazyContent<'f, ST> for TestContent
    where
        ST: 'f + CAS,
    {
        fn retrieve_from(fs: &'f FileSystem<'f, ST>, hash: &Hash) -> Result<Self> {
            let val: TestContent = fs.storage.retrieve(hash)?;
            Ok(val)
        }

        fn store_in(&self, fs: &'f FileSystem<'f, ST>) -> Result<Hash> {
            Ok(fs.storage.store(self)?)
        }
    }

    const HELLO_WORLD_HASH: &'static str = "6142e96d0071656be3de08f89fc7ab9d374f74428ce61bd8c693efeac4d831aa";

    #[test]
    fn test_store() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);

        // write a value as a lazy object, by getting its hash
        let lho = LazyHashedObject::for_content(TestContent("hello, world".to_string()));
        let hash = lho.hash(&fs).unwrap();
        assert_eq!(hash, &Hash::from_hex(HELLO_WORLD_HASH));

        // check that it's stored
        assert!(storage.retrieve::<TestContent>(hash).is_ok());
    }

    #[test]
    fn test_retrieve() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);

        // write a value directly to storage
        storage
            .store(&TestContent("hello, world".to_string()))
            .unwrap();

        // and retrieve it as a lazy object
        let lho = LazyHashedObject::for_hash(&Hash::from_hex(HELLO_WORLD_HASH));
        let content: &TestContent = lho.content(&fs).unwrap();
        assert_eq!(content.0, "hello, world".to_string());
    }

    #[test]
    fn test_retrieve_fails() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(&storage);

        // and retrieve it as a lazy object
        let lho = LazyHashedObject::for_hash(&Hash::from_hex(HELLO_WORLD_HASH));
        let res: Result<&TestContent> = lho.content(&fs);
        assert!(res.is_err());
    }
}
