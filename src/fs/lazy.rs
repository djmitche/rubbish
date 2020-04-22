use super::fs::FileSystem;
use crate::cas::Hash;
use failure::Fallible;
use std::cell::RefCell;

// TODO: use pub(crate)

/// A LazyHashedObject is an object which can be stored by hash in a FileSystem and retrieved from
/// one.  It can be created with a hash, in which case the object's content (of type T) is loaded
/// only when requested; or it can be created with content, in which case the hash is only
/// determined when requested (with the object stored in the FileSystem at that time).
#[derive(Debug)]
pub struct LazyHashedObject<T: LazyContent>(RefCell<LazyInner<T>>);

/// LazyInner proivdes interior mutability for LazyHashedObject.
///
/// INVARIANT α: at least one of `hash` and `content` is always `Some(_)`.
/// INVARIANT β: a `Some(_)` value for `hash` or `content` is immutable.
#[derive(Debug)]
struct LazyInner<T>
where
    T: LazyContent,
{
    /// The hash of this object, if it has already been calculated
    hash: Option<Hash>,

    /// The content of this object, if it has already been loaded
    content: Option<T>,
}

/// LazyContent bounds content that can be stored as a `LazyHashedObject`, providing
/// methods to retrieve and store the content in a FileSystem.
pub trait LazyContent: Sized {
    /// Retrive this content from the given FileSystem
    fn retrieve_from(fs: &FileSystem, hash: &Hash) -> Fallible<Self>;

    /// Store the content in the given FileSystem, returning its hash
    fn store_in(&self, fs: &FileSystem) -> Fallible<Hash>;
}

impl<T> LazyHashedObject<T>
where
    T: LazyContent,
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
    pub fn hash(&self, fs: &FileSystem) -> Fallible<&Hash> {
        let mut inner = self.0.borrow_mut();
        let h = inner.hash(fs)?;
        Ok(unsafe {
            // "upgrade" h's lifetime from that of the mutable borrow, based on invariant β
            (h as *const Hash).as_ref().unwrap()
        })
    }

    /// Get the hash, if it is set.
    pub fn maybe_hash(&self) -> Option<&Hash> {
        let inner = self.0.borrow();
        let h = &inner.hash;
        match h {
            None => None,
            Some(h) => Some(unsafe {
                // "upgrade" h's lifetime from that of the mutable borrow, based on invariant β
                (h as *const Hash).as_ref().unwrap()
            }),
        }
    }

    /// Get the content of this object, retrieving it from the FileSystem first if necessary.
    pub fn content(&self, fs: &FileSystem) -> Fallible<&T> {
        let mut inner = self.0.borrow_mut();
        let c = inner.content(fs)?;
        Ok(unsafe {
            // "upgrade" c's lifetime from that of the mutable borrow, based on invariant β
            (c as *const T).as_ref().unwrap()
        })
    }

    /// Get the content, if it is set.
    pub fn maybe_content(&self) -> Option<&T> {
        let inner = self.0.borrow();
        let c = &inner.content;
        match c {
            None => None,
            Some(c) => Some(unsafe {
                // "upgrade" c's lifetime from that of the mutable borrow, based on invariant β
                (c as *const T).as_ref().unwrap()
            }),
        }
    }

    /// Does this lazy object already have a hash?
    pub(crate) fn has_hash(&self) -> bool {
        let inner = self.0.borrow();
        inner.hash.is_some()
    }

    pub(crate) fn has_content(&self) -> bool {
        let inner = self.0.borrow();
        inner.content.is_some()
    }
}

impl<T> LazyInner<T>
where
    T: LazyContent,
{
    fn for_content(content: T) -> Self {
        LazyInner {
            hash: None,
            content: Some(content),
        }
    }

    fn for_hash(hash: &Hash) -> Self {
        LazyInner {
            hash: Some(hash.clone()),
            content: None,
        }
    }

    /// Ensure that self.hash is not None. This may write the commit to storage,
    /// so it may fail and thus returns a Result.
    fn ensure_hash<'a>(&'a mut self, fs: &FileSystem) -> Fallible<()> {
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

    fn hash(&mut self, fs: &FileSystem) -> Fallible<&Hash> {
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
    fn ensure_content<'a>(&'a mut self, fs: &FileSystem) -> Fallible<()> {
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

    fn content(&mut self, fs: &FileSystem) -> Fallible<&T> {
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
    use crate::cas::Hash;
    use crate::cas::LocalStorage;
    use crate::fs::FileSystem;

    #[derive(Debug, Clone, PartialEq)]
    struct TestContent(String);

    impl LazyContent for TestContent {
        fn retrieve_from(fs: &FileSystem, hash: &Hash) -> Fallible<Self> {
            Ok(TestContent(String::from_utf8(fs.storage.retrieve(hash)?)?))
        }

        fn store_in(&self, fs: &FileSystem) -> Fallible<Hash> {
            Ok(fs.storage.store(self.0.as_bytes().to_vec())?)
        }
    }

    const HELLO_WORLD_HASH: &'static str =
        "09ca7e4eaa6e8ae9c7d261167129184883644d07dfba7cbfbc4c8a2e08360d5b";

    #[test]
    fn test_for_content() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));
        let hello_content = TestContent("hello, world".to_string());
        let hello_hash = Hash::from_hex(HELLO_WORLD_HASH);

        // create an Lho
        let lho = LazyHashedObject::for_content(hello_content.clone());

        assert!(lho.has_content());
        assert_eq!(lho.maybe_content(), Some(&hello_content));
        assert!(!lho.has_hash());
        assert_eq!(lho.maybe_hash(), None);
        assert!(fs.storage.retrieve(&hello_hash).is_err());

        // get its content, changing nothing..
        let content = lho.content(&fs).unwrap();

        assert_eq!(content, &hello_content);
        assert!(lho.has_content());
        assert_eq!(lho.maybe_content(), Some(&hello_content));
        assert!(!lho.has_hash());
        assert!(fs.storage.retrieve(&hello_hash).is_err());

        // get its hash, writing it to storage in the process
        let hash = lho.hash(&fs).unwrap();

        assert_eq!(hash, &hello_hash);
        assert!(lho.has_content());
        assert_eq!(lho.maybe_content(), Some(&hello_content));
        assert!(lho.has_hash());
        assert_eq!(lho.maybe_hash(), Some(&hello_hash));
        assert!(fs.storage.retrieve(hash).is_ok());
    }

    #[test]
    fn test_for_hash() {
        let storage = LocalStorage::new();
        let fs = FileSystem::new(Box::new(storage));
        let hello_content = TestContent("hello, world".to_string());
        let hello_hash = Hash::from_hex(HELLO_WORLD_HASH);

        // create an lho
        let lho: LazyHashedObject<TestContent> = LazyHashedObject::for_hash(&hello_hash);

        assert!(!lho.has_content());
        assert!(lho.content(&fs).is_err());
        assert_eq!(lho.maybe_content(), None);
        assert!(lho.has_hash());
        assert_eq!(lho.maybe_hash(), Some(&hello_hash));

        // get its hash, changing nothing..
        let hash = lho.hash(&fs).unwrap();

        assert_eq!(hash, &hello_hash);
        assert!(!lho.has_content());
        assert!(lho.content(&fs).is_err());
        assert_eq!(lho.maybe_content(), None);
        assert!(lho.has_hash());
        assert_eq!(lho.maybe_hash(), Some(&hello_hash));

        // write a value directly to storage
        fs.storage
            .store(hello_content.0.as_bytes().to_vec())
            .unwrap();

        // and retrieve it as a lazy object
        let content = lho.content(&fs).unwrap();

        assert_eq!(&content.0, "hello, world");
        assert!(lho.has_content());
        assert_eq!(lho.maybe_content(), Some(&hello_content));
        assert!(lho.has_hash());
    }
}
