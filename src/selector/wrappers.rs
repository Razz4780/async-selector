use std::{
    ops::{Deref, DerefMut},
    pin::Pin,
};

use crate::{list, selector::Id};

/// Immutable reference to a task stored in a [`Selector`](super::Selector).
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
