//! [`Selector`](super::Selector) visitors.

use crate::{
    list::{Cursor, List},
    selector::{Borrowed, BorrowedMut, Removed},
};

/// Returned from [`Selector::into_iter`](super::Selector::into_iter).
pub struct IntoIter<T>(pub(super) List<T>);

impl<T> Iterator for IntoIter<T> {
    type Item = Removed<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.cursor_mut().remove_front().map(Removed)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.0.len();
        (len, Some(len))
    }
}

impl<T> ExactSizeIterator for IntoIter<T> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T> DoubleEndedIterator for IntoIter<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.cursor_mut().remove_back().map(Removed)
    }
}

/// Returned from [`Selector::iter`](super::Selector::iter).
pub struct Iter<'a, T>(pub(super) Cursor<T, &'a List<T>>);

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = Borrowed<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front().map(Borrowed)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<T> ExactSizeIterator for Iter<'_, T> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T> DoubleEndedIterator for Iter<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.pop_back().map(Borrowed)
    }
}

/// Returned from [`Selector::iter_mut`](super::Selector::iter_mut).
pub struct IterMut<'a, T>(pub(super) Cursor<T, &'a mut List<T>>);

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = BorrowedMut<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front().map(BorrowedMut)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<T> ExactSizeIterator for IterMut<'_, T> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T> DoubleEndedIterator for IterMut<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.pop_back().map(BorrowedMut)
    }
}

/// Returned from [`Selector::extract_if`](super::Selector::extract_if).
pub struct ExtractIf<'a, T, F> {
    pub(super) cursor: Cursor<T, &'a mut List<T>>,
    pub(super) pred: F,
}

impl<'a, T, F> Iterator for ExtractIf<'a, T, F>
where
    F: for<'b> FnMut(BorrowedMut<'b, T>) -> bool,
{
    type Item = Removed<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next = self.cursor.peek_front()?;
            if (self.pred)(BorrowedMut(next)) {
                break self.cursor.remove_front().map(Removed);
            } else {
                self.cursor.pop_front();
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl<'a, T, F> DoubleEndedIterator for ExtractIf<'a, T, F>
where
    F: for<'b> FnMut(BorrowedMut<'b, T>) -> bool,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            let back = self.cursor.peek_back()?;
            if (self.pred)(BorrowedMut(back)) {
                break self.cursor.remove_back().map(Removed);
            } else {
                self.cursor.pop_back();
            }
        }
    }
}
