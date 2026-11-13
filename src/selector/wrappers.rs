use std::{
    ops::{Deref, DerefMut},
    pin::Pin,
};

use crate::{list, selector::Id};

/// Shared reference to a task stored in a [`Selector`](super::Selector).
///
/// Returned from [`Selector::get`](super::Selector::get) and [`Selector::iter`](super::Selector::iter).
///
/// Dereferences to the task, so you can call the task's methods directly.
/// Additionally, it gives you the task's [`Id`].
pub struct Borrowed<'a, T>(pub(super) list::Borrowed<&'a T>);

impl<'a, T> Borrowed<'a, T> {
    /// Returns the ID of the task.
    pub fn id(&self) -> &Id<T> {
        Id::new(self.0.node())
    }

    /// Returns a pinned reference to the task.
    pub fn get_pin(&self) -> Pin<&T> {
        self.0.get_pin()
    }

    /// Consumes this wrapper, returning a pinned reference to the task.
    pub fn into_pin(self) -> Pin<&'a T> {
        self.0.into_pin()
    }
}

impl<C> Deref for Borrowed<'_, C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.get_pin().get_ref()
    }
}

/// Mutable reference to a task stored in a [`Selector`](super::Selector).
///
/// Returned from [`Selector::push`](super::Selector::push),
/// [`Selector::get_mut`](super::Selector::get_mut) and [`Selector::iter_mut`](super::Selector::iter_mut),
/// and passed to [`Task::transform_cont`](crate::task::Task::transform_cont).
///
/// The selector keeps the tasks pinned, so this type gives you a pinned mutable reference
/// ([`get_pin_mut`](Self::get_pin_mut)).
/// If the task is [`Unpin`], it also dereferences mutably to the task.
/// Additionally, it gives you the task's [`Id`].
///
/// ```
/// use async_selector::StreamSelector;
/// use futures::channel::mpsc;
///
/// let mut selector = StreamSelector::default();
/// let (tx, rx) = mpsc::unbounded::<u32>();
///
/// // `push` returns a `BorrowedMut`.
/// let mut receiver = selector.push(rx);
/// let id = receiver.id().clone();
///
/// // The receiver is `Unpin`, so we can call its `&mut self` methods directly.
/// receiver.close();
/// assert!(tx.is_closed());
/// # drop(id);
/// ```
pub struct BorrowedMut<'a, T>(pub(super) list::Borrowed<&'a mut T>);

impl<'a, T> BorrowedMut<'a, T> {
    /// Returns the ID of the task.
    pub fn id(&self) -> &Id<T> {
        Id::new(self.0.node())
    }

    /// Returns a pinned reference to the task.
    pub fn get_pin(&self) -> Pin<&T> {
        self.0.get_pin()
    }

    /// Consumes this wrapper, returning a pinned reference to the task.
    pub fn into_pin(self) -> Pin<&'a T> {
        self.0.into_pin_mut().into_ref()
    }

    /// Returns a mutable pinned reference to the task.
    pub fn get_pin_mut(&mut self) -> Pin<&mut T> {
        self.0.get_pin_mut()
    }

    /// Consumes this wrapper, returning a mutable pinned reference to the task.
    pub fn into_pin_mut(self) -> Pin<&'a mut T> {
        self.0.into_pin_mut()
    }
}

impl<C> Deref for BorrowedMut<'_, C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.get_pin().get_ref()
    }
}

impl<C: Unpin> DerefMut for BorrowedMut<'_, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_pin_mut().get_mut()
    }
}

/// Task removed from a [`Selector`](super::Selector).
///
/// Returned from [`Selector::remove`](super::Selector::remove) and the other removing methods,
/// and passed to [`Task::transform_break`](crate::task::Task::transform_break).
///
/// It owns the task, and drops it when dropped.
/// It dereferences to the task, and gives you the task's [`Id`].
/// If the task is [`Unpin`], you can take it out with [`into_inner`](Self::into_inner).
///
/// The task stays in the same heap allocation that it had in the selector,
/// which is why it is wrapped in this type, and not returned directly.
pub struct Removed<T>(pub(super) list::Removed<T>);

impl<T> Removed<T> {
    /// Returns the ID of the task.
    pub fn id(&self) -> &Id<T> {
        Id::new(self.0.node())
    }

    /// Returns a pinned reference to the task.
    pub fn get_pin(&self) -> Pin<&T> {
        self.0.get_pin()
    }

    /// Returns a mutable pinned reference to the task.
    pub fn get_pin_mut(&mut self) -> Pin<&mut T> {
        self.0.get_pin_mut()
    }

    /// Consumes this wrapper, returning the task.
    ///
    /// Since this method moves the task in memory,
    /// it is only available when `T` is [`Unpin`].
    pub fn into_inner(self) -> T
    where
        T: Unpin,
    {
        self.0.into_inner()
    }
}

impl<T> Deref for Removed<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.get_pin().get_ref()
    }
}

impl<T: Unpin> DerefMut for Removed<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_pin_mut().get_mut()
    }
}
