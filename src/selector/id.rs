use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    sync::Weak,
};

use crate::task::Task;

/// A unique id of a task stored in a [`Selector`](crate::selector::Selector).
///
/// See [example](https://github.com/Razz4780/async-selector/blob/main/examples/map.rs)
/// of how it can be leverage to use the selector like a map.
///
/// Mind that this keeping this id alive prevents the selector
/// from deallocating memory used to store the task.
pub struct Id<P>(pub(super) Weak<Task<P>>);

impl<P> Clone for Id<P> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<P> fmt::Debug for Id<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Weak::as_ptr(&self.0).fmt(f)
    }
}

impl<P> PartialEq for Id<P> {
    fn eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
}

impl<P> Eq for Id<P> {}

impl<P> Hash for Id<P> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::hash(self.0.as_ptr(), state);
    }
}

impl<P> PartialOrd for Id<P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<P> Ord for Id<P> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_ptr().cmp(&other.0.as_ptr())
    }
}

unsafe impl<P> Send for Id<P> {}
unsafe impl<P> Sync for Id<P> {}
