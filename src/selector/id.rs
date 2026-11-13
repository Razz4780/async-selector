use std::{
    fmt, hash,
    sync::{Arc, Weak},
};

use crate::list::Node;

/// Unique ID of a task pushed into a [`Selector`](crate::selector::Selector).
///
/// You get the ID from the reference returned by [`Selector::push`](crate::selector::Selector::push),
/// from [`Borrowed::id`](crate::selector::Borrowed::id), and from the items yielded by
/// the `*WithId` and `*Reclaim` selectors (e.g. [`StreamWithIdSelector`](crate::StreamWithIdSelector)).
/// Use it to access, remove, or wake the task later.
///
/// IDs are cheap to clone, and they implement [`Eq`] and [`Hash`](std::hash::Hash),
/// so you can use them as map keys.
/// An ID is unique among all IDs, including IDs of tasks pushed into other selectors,
/// and it is never reused while it exists.
///
/// An ID keeps the task's heap allocation alive (but not the task itself),
/// so don't keep IDs of removed tasks around forever.
///
/// ```
/// use async_selector::FutureSelector;
///
/// let mut selector = FutureSelector::default();
/// let id = selector.push(std::future::ready(1)).id().clone();
///
/// // The ID gives O(1) access to the task...
/// assert!(selector.get(&id).is_some());
///
/// // ...even if it's a clone.
/// let cloned = id.clone();
/// assert_eq!(cloned, id);
/// assert!(selector.remove(&cloned).is_some());
///
/// // Once the task is removed, the ID no longer matches anything.
/// assert!(selector.get(&id).is_none());
/// ```
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

    /// Wakes the task, so that the selector polls it the next time the selector is polled.
    ///
    /// This has the same effect as waking the [`Waker`](std::task::Waker)
    /// that was passed to the task when it was polled.
    /// Use it after you modify the task in a way that can make it ready
    /// (see [wakeups](crate::selector::Selector#wakeups)).
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
