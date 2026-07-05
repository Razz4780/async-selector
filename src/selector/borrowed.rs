use std::{
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::Arc,
};

use crate::{
    list::cursor::{BorrowedNode, BorrowedNodeMut},
    mpsc,
    selector::Id,
    task::Task,
};

/// An immutable borrow of a task stored in a [`Selector`](crate::selector::Selector).
pub struct Borrowed<'a, P> {
    pub(super) node: BorrowedNode<'a, Task<P>>,
    pub(super) queue: &'a mpsc::Receiver<Task<P>>,
}

impl<P> Borrowed<'_, P> {
    /// Manually wakes the task.
    pub fn wake(&self) {
        let node = self.node.node().clone();
        self.queue.send(node);
    }

    /// Returns the id of this task.
    pub fn id(&self) -> Id<P> {
        Id(Arc::downgrade(self.node.node()))
    }
}

impl<P> Deref for Borrowed<'_, P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        self.node.get_protected()
    }
}

/// Mutable borrow of a task stored in a [`Selector`](crate::selector::Selector).
///
/// **Important:** before modifying tasks stored in the selector, see the wakeups [section](crate::selector::Selector#wakeups).
pub struct BorrowedMut<'a, P> {
    pub(super) node: BorrowedNodeMut<'a, Task<P>>,
    pub(super) queue: &'a mpsc::Receiver<Task<P>>,
}

impl<P> BorrowedMut<'_, P> {
    /// Manually wakes the task.
    pub fn wake(&self) {
        let node = self.node.node().clone();
        self.queue.send(node);
    }

    /// Returns the id of this task.
    pub fn id(&self) -> Id<P> {
        Id(Arc::downgrade(self.node.node()))
    }

    /// Returns a pinned reference to the task.
    ///
    /// **Important:** before modifying tasks stored in the selector, see the wakeups [section](crate::selector::Selector#wakeups).
    pub fn get_mut(&mut self) -> Pin<&mut P> {
        self.node.get_protected_mut()
    }
}

impl<P> Deref for BorrowedMut<'_, P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        self.node.get_protected()
    }
}

impl<P: Unpin> DerefMut for BorrowedMut<'_, P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_mut().get_mut()
    }
}
