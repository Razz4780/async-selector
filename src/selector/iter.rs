//! Types for iterating over tasks stored in a [`Selector`](crate::selector::Selector).

use std::pin::Pin;

use crate::{
    list::{
        IntrusiveList,
        cursor::{Cursor, CursorMut},
    },
    mpsc,
    selector::{
        Removed,
        borrowed::{Borrowed, BorrowedMut},
    },
    task::Task,
};

/// Iterator that visits tasks stored in a [`Selector`](crate::selector::Selector).
///
/// Tasks are visited in the insertion order.
pub struct Iter<'a, P> {
    pub(super) cursor: Cursor<'a, Task<P>>,
    pub(super) queue: &'a mpsc::Receiver<Task<P>>,
}

impl<'a, P> Iterator for Iter<'a, P> {
    type Item = Borrowed<'a, P>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(Borrowed {
            node: self.cursor.pop_front()?,
            queue: self.queue,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.cursor.len();
        (len, Some(len))
    }
}

impl<P> ExactSizeIterator for Iter<'_, P> {
    fn len(&self) -> usize {
        self.cursor.len()
    }
}

impl<P> DoubleEndedIterator for Iter<'_, P> {
    fn next_back(&mut self) -> Option<Self::Item> {
        Some(Borrowed {
            node: self.cursor.pop_back()?,
            queue: self.queue,
        })
    }
}

/// Iterator that allows for modifying tasks stored in a [`Selector`](crate::selector::Selector).
///
/// Tasks are visited in the insertion order.
///
/// **Important:** before modifying tasks stored in the selector, see the wakeups [section](crate::selector::Selector#wakeups).
pub struct IterMut<'a, P> {
    pub(super) cursor: CursorMut<'a, Task<P>>,
    pub(super) queue: &'a mpsc::Receiver<Task<P>>,
}

impl<'a, P> Iterator for IterMut<'a, P> {
    type Item = BorrowedMut<'a, P>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(BorrowedMut {
            node: self.cursor.pop_front()?,
            queue: self.queue,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.cursor.len();
        (len, Some(len))
    }
}

impl<P> ExactSizeIterator for IterMut<'_, P> {
    fn len(&self) -> usize {
        self.cursor.len()
    }
}

impl<P> DoubleEndedIterator for IterMut<'_, P> {
    fn next_back(&mut self) -> Option<Self::Item> {
        Some(BorrowedMut {
            node: self.cursor.pop_back()?,
            queue: self.queue,
        })
    }
}

/// Iterator that returns tasks stored previously in a [`Selector`](crate::selector::Selector).
///
/// If the iterator is not exhausted, e.g. because it is dropped without iterating or the iteration short-circuits,
/// then the remaining tasks are dropped.
pub struct IntoIter<P>(pub(super) IntrusiveList<Task<P>>);

impl<P> Iterator for IntoIter<P> {
    type Item = Removed<P>;

    fn next(&mut self) -> Option<Self::Item> {
        CursorMut::new(&mut self.0).remove_front().map(Removed)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.0.len();
        (len, Some(len))
    }
}

impl<P> ExactSizeIterator for IntoIter<P> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<P> DoubleEndedIterator for IntoIter<P> {
    fn next_back(&mut self) -> Option<Self::Item> {
        CursorMut::new(&mut self.0).remove_back().map(Removed)
    }
}

/// Iterator which uses a closure to determine if a task should be removed from a [`Selector`](crate::selector::Selector).
///
/// If the closure returns true, the task is removed from the selector and yielded.
/// If the closure returns false, or panics, the element remains in the task and will not be yielded.
///
/// If the iterator is not exhausted, e.g. because it is dropped without iterating or the iteration short-circuits,
/// then the remaining tasks will be retained.
///
/// **Important:** before removing tasks from the selector, see the removal [section](crate::selector::Selector#removal).
pub struct ExtractIf<'a, P, F>
where
    F: FnMut(Pin<&mut P>) -> bool,
{
    pub(super) cursor: CursorMut<'a, Task<P>>,
    pub(super) pred: F,
}

impl<'a, P, F> Iterator for ExtractIf<'a, P, F>
where
    F: FnMut(Pin<&mut P>) -> bool,
{
    type Item = Removed<P>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut front = self.cursor.peek_front()?;
            if (self.pred)(front.get_protected_mut()) {
                return self.cursor.remove_front().map(Removed);
            } else {
                self.cursor.pop_front();
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.cursor.len();
        (0, Some(len))
    }
}

impl<'a, P, F> DoubleEndedIterator for ExtractIf<'a, P, F>
where
    F: FnMut(Pin<&mut P>) -> bool,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            let mut back = self.cursor.peek_back()?;
            if (self.pred)(back.get_protected_mut()) {
                return self.cursor.remove_back().map(Removed);
            } else {
                self.cursor.pop_back();
            }
        }
    }
}
