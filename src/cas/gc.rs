//! Support for garbage collection cycles.

use super::traits::CAS;
use failure::Fallible;

/// Type GarbageCycle represents a garbage-collection cycle.  Between creation and destruction of
/// an object of this type, touch or store every non-garbage object.  Any objects not touched
/// may be purged from the storage after this object is destroyed.
///
/// # Examples
///
/// ```
/// use rubbish::cas::{Storage, GarbageCycle, CAS};
/// let mut storage = Storage::new();
///
/// let hash1 = storage.store(vec![1, 2]).unwrap();
/// let hash2 = storage.store(vec![3, 4]).unwrap();
/// let hash3;
/// {
///     let gc = GarbageCycle::new(&storage);
///     hash3 = storage.store(vec![5, 6]).unwrap();
///     storage.touch(&hash1).unwrap();
/// }
///
/// // hash2 has been garbage-collected..
/// assert!(storage.retrieve(&hash2).is_err());
/// ```
pub struct GarbageCycle<'a, ST: 'a + CAS> {
    storage: &'a ST,
}

impl<'a, ST: 'a + CAS> GarbageCycle<'a, ST> {
    pub fn new(storage: &'a ST) -> Fallible<GarbageCycle<'a, ST>> {
        storage.begin_gc()?;
        Ok(GarbageCycle { storage: storage })
    }
}

impl<'a, ST: 'a + CAS> Drop for GarbageCycle<'a, ST> {
    fn drop(&mut self) {
        self.storage.end_gc()
    }
}
