use std::{
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::Arc,
};

use crate::{list, selector::Id, task::Task};

/// Task removed from a [`Selector`](crate::selector::Selector).
///
/// **Important:** before removing tasks from the selector, see the removal [section](crate::selector::Selector#removal).
#[must_use = "memory occupied by a removed task is not freed immediately, see Selector docs"]
pub struct Removed<P>(pub(super) list::Removed<Task<P>>);

impl<P> Removed<P> {
    /// Returns the id of this task.
    pub fn id(&self) -> Id<P> {
        Id(Arc::downgrade(self.0.node()))
    }

    /// Returns a pinned reference to the task.
    pub fn get_mut(&mut self) -> Pin<&mut P> {
        self.0.protected_mut()
    }

    /// Consumes this wrapper and returns the task.
    pub fn into_inner(self) -> P
    where
        P: Unpin,
    {
        self.0.into_inner()
    }
}

impl<P> AsRef<P> for Removed<P> {
    fn as_ref(&self) -> &P {
        self.0.protected()
    }
}

impl<P> Deref for Removed<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        self.0.protected()
    }
}

impl<P: Unpin> DerefMut for Removed<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.protected_mut().get_mut()
    }
}
