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
/// #[tokio::main]
/// async fn main() {
///   let mut storage = Storage::new();
///
///   let hash1 = storage.store(&"abc".to_string()).await.unwrap();
///   let hash2 = storage.store(&"def".to_string()).await.unwrap();
///   let hash3;
///   {
///       let gc = GarbageCycle::new(&storage);
///       hash3 = storage.store(&"ghi".to_string()).await.unwrap();
///     storage.touch(&hash1).await.unwrap();
///   }
///   
///   // hash2 has been garbage-collected..
///   assert!(storage.retrieve::<String>(&hash2).await.is_err());
/// }
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
