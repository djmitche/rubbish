pub(crate) trait GarbageCollector {
    fn end_gc(&self);
}

/// Internal representation of an ongoing garbage-collection run.  When this object is dropped,
/// the run is considered complete and any un-touched objects are considered garbage.
pub struct GarbageCollection<'a>(&'a GarbageCollector);

impl<'a> GarbageCollection<'a> {
    pub(crate) fn new(gc: &'a GarbageCollector) -> GarbageCollection<'a> {
        GarbageCollection(gc)
    }
}

impl<'a> Drop for GarbageCollection<'a> {
    fn drop(&mut self) {
        self.0.end_gc()
    }
}
