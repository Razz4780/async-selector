use std::{
    fmt, hash,
    sync::{Arc, Weak},
};

use crate::list::Node;

/// Unique ID of a task pushed into a [`Selector`](crate::selector::Selector).
///
/// This ID is always unique relative to all other IDs,
/// including IDs obtained from other selectors.
#[repr(transparent)]
pub struct Id<C>(Arc<Node<C>>);

impl<C> Id<C> {
    pub(super) fn new(node: &Arc<Node<C>>) -> &Self {
        let ptr = std::ptr::from_ref(node) as *const Self;
        unsafe { &*ptr }
    }

    pub(super) fn get(&self) -> &Arc<Node<C>> {
        &self.0
    }

    /// Manually wakes this task.
    ///
    /// Does nothing if the task is no longer stored in the selector.
    pub fn wake(&self) {
        self.0.enqueue_by_ref();
    }
}

impl<C> Clone for Id<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<C> PartialEq for Id<C> {
    fn eq(&self, other: &Self) -> bool {
        let this = Arc::as_ptr(&self.0);
        let other = Arc::as_ptr(&other.0);
        this.eq(&other)
    }
}

impl<C> Eq for Id<C> {}

impl<C> hash::Hash for Id<C> {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

impl<C> fmt::Debug for Id<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Id")
            .field("task_ptr", &Arc::as_ptr(&self.0))
            .field("queue_ptr", &Weak::as_ptr(self.0.queue()))
            .finish()
    }
}
