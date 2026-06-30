//! Fast and flexible [`Future`]/[`Stream`](futures::Stream) selector.

use std::{
    fmt,
    ops::{ControlFlow, Not},
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;

use crate::{
    list::{
        IntrusiveList,
        cursor::{Cursor, CursorMut},
    },
    mpsc,
    pollable::{PollStrategy, PollWith},
    selector::iter::{ExtractIf, IntoIter, Iter, IterMut},
    task::Task,
};

pub use borrowed::{Borrowed, BorrowedMut};
pub use id::Id;
pub use removed::Removed;

mod borrowed;
mod id;
pub mod iter;
mod removed;

/// Selector over a dynamic set of pollable tasks, for example [`Future`]s and [`Stream`]s.
///
/// Designed for optimal performance when polling a large number of tasks.
///
/// Allows for:
/// 1. Safely injecting shared state into the tasks (see [`PollWith`]).
/// 2. Accessing and removing the tasks by unique ids.
///
/// # Removal
///
/// TODO document memory gc mechanism for removed tasks
///
/// # Wakeups
///
/// TODO document smart polling and how it affects modifying the tasks from the outside / polling with different extension types
pub struct Selector<S: PollStrategy> {
    /// Queue of tasks that were woken.
    ready_rx: mpsc::Receiver<Task<S::Pollable>>,
    /// List of all tasks.
    list: IntrusiveList<Task<S::Pollable>>,
    /// [`PollStrategy`] determining how we poll tasks.
    strategy: S,
}

impl<S: PollStrategy> Selector<S> {
    /// Creates a new selector that will use the given [`PollStrategy`] for polling tasks.
    pub fn new(strategy: S) -> Self {
        Self {
            ready_rx: mpsc::Receiver::new(|weak| Task::new(weak.clone())),
            list: Default::default(),
            strategy,
        }
    }

    /// Pushes a new task into the selector.
    pub fn push(&mut self, pollable: S::Pollable) {
        let node = self
            .list
            .insert(Task::new(self.ready_rx.weak_sender()), pollable);
        self.ready_rx.send(node);
    }

    /// Pushes a new task into the selector and returns its unique id.
    pub fn push_with_id(&mut self, pollable: S::Pollable) -> Id {
        let node = self
            .list
            .insert(Task::new(self.ready_rx.weak_sender()), pollable);
        let id = Id::new(&node);
        self.ready_rx.send(node);
        id
    }

    /// Returns whether the selector is empty.
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Returns the number of tasks in the selector.
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Manually wakes all tasks in the selector.
    pub fn wake_all(&self) {
        self.iter().for_each(|borrowed| borrowed.wake());
    }

    /// Returns an iterator over the tasks in the selector.
    ///
    /// The tasks are visited in the insertion order.
    pub fn iter(&self) -> Iter<'_, S::Pollable> {
        Iter {
            cursor: Cursor::new(&self.list),
            queue: &self.ready_rx,
        }
    }

    /// Returns an iterator that allows modifying each task in the selector.
    ///
    /// The tasks are visited in the insertion order.
    ///
    /// **Important:** before modifying tasks stored in the selector, see the wakeups [section](Selector#wakeups).
    pub fn iter_mut(&mut self) -> IterMut<'_, S::Pollable> {
        IterMut {
            cursor: CursorMut::new(&mut self.list),
            queue: &self.ready_rx,
        }
    }

    /// Creates an iterator which uses a closure to determine if a task should be removed.
    ///
    /// If the closure returns true, the task is removed from the selector and yielded.
    /// If the closure returns false, or panics, the element remains in the task and will not be yielded.
    ///
    /// If the returned [`ExtractIf`] is not exhausted, e.g. because it is dropped without iterating or the iteration short-circuits,
    /// then the remaining tasks will be retained.
    ///
    /// **Important:** before removing tasks from the selector, see the removal [section](Selector#removal).
    #[must_use = "ExtractIf does not remove any elements unless consumed"]
    pub fn extract_if<F>(&mut self, pred: F) -> ExtractIf<'_, S::Pollable, F>
    where
        F: FnMut(Pin<&mut S::Pollable>) -> bool,
    {
        ExtractIf {
            cursor: CursorMut::new(&mut self.list),
            pred,
        }
    }

    /// Returns an immutable reference to the task with the given id.
    ///
    /// Returns `None` if the task is not found in this selector,
    /// for example because it was removed or has already finished.
    pub fn get(&self, id: &Id) -> Option<Borrowed<'_, S::Pollable>> {
        if std::ptr::addr_eq(self.ready_rx.as_ptr(), id.sender_ptr()).not() {
            return None;
        }
        let node = unsafe {
            // SAFETY: we just checked that this id comes from this selector.
            // Therefore, the task cannot be stored in any other list.
            let task = id.task::<S::Pollable>()?;
            self.list.get(&task)
        }?;
        Some(Borrowed {
            node,
            queue: &self.ready_rx,
        })
    }

    /// Returns a mutable reference to the target with the given id.
    ///
    /// Returns `None` if the task is not found in this selector,
    /// for example because it was removed or has already finished.
    ///
    /// **Important:** before modifying tasks stored in the selector, see the wakeups [section](Selector#wakeups).
    pub fn get_mut(&mut self, id: &Id) -> Option<BorrowedMut<'_, S::Pollable>> {
        if std::ptr::addr_eq(self.ready_rx.as_ptr(), id.sender_ptr()).not() {
            return None;
        }
        let node = unsafe {
            // SAFETY: we just checked that this id comes from this selector.
            // Therefore, the task cannot be stored in any other list.
            let task = id.task::<S::Pollable>()?;
            self.list.get_mut(&task)
        }?;
        Some(BorrowedMut {
            node,
            queue: &self.ready_rx,
        })
    }

    /// Removes the task with the given id from the selector.
    ///
    /// Returns `None` if the task is not found in the selector,
    /// for example because it was removed or has already finished.
    ///
    /// **Important:** before removing tasks from the selector, see the removal [section](Selector#removal).
    pub fn remove(&mut self, id: &Id) -> Option<Removed<S::Pollable>> {
        if std::ptr::addr_eq(self.ready_rx.as_ptr(), id.sender_ptr()).not() {
            return None;
        }
        let removed = unsafe {
            // SAFETY: we just checked that this id comes from this selector.
            // Therefore, the task cannot be stored in any other list.
            let task = id.task::<S::Pollable>()?;
            self.list.remove(&task)?
        };
        Some(Removed(removed))
    }

    /// Returns the next ready item from one of the tasks stored in the selector.
    ///
    /// Provided extensions will be passed down to the tasks as arguments to [`PollWith::poll_progress`].
    ///
    /// Returns `None` if the selector is empty.
    ///
    /// **Important:** before polling the tasks with different extension types, see the wakeups [section](Selector#wakeups).
    pub fn poll_next_with_ext<'a, E, EMut>(
        &mut self,
        ext: &'a E,
        ext_mut: &mut EMut,
        cx: &mut Context<'_>,
    ) -> Poll<Option<<S as PollWith<'a, E, EMut>>::Item>>
    where
        S: PollWith<'a, E, EMut>,
        E: ?Sized,
        EMut: ?Sized,
    {
        let marker = self.ready_rx.register(cx.waker());
        if marker.is_null() {
            return if self.list.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Pending
            };
        }

        let mut polled_all_queue = false;
        while polled_all_queue.not() {
            let task = match self.ready_rx.recv() {
                Some(task) => {
                    polled_all_queue = std::ptr::eq(task.as_ref(), marker);
                    task
                }
                None if self.list.is_empty() => return Poll::Ready(None),
                None => return Poll::Pending,
            };

            let mut guard = {
                let guard = unsafe {
                    // SAFETY: we received this task from our ready queue,
                    // so it must be ours.
                    self.list.access(&task)
                };
                match guard {
                    Some(guard) => guard,
                    None => continue,
                }
            };
            let waker = task.borrow_waker();
            let mut cx = Context::from_waker(&waker);
            let result = S::poll_progress(guard.get(), ext, ext_mut, &mut cx);
            match result {
                Poll::Ready(output) => match self.strategy.try_unwrap(output) {
                    ControlFlow::Break(Some(item)) => {
                        return Poll::Ready(Some(item));
                    }
                    ControlFlow::Break(None) => {}
                    ControlFlow::Continue(item) => {
                        guard.forget();
                        self.ready_rx.send(task);
                        return Poll::Ready(Some(item));
                    }
                },
                Poll::Pending => guard.forget(),
            }
        }

        cx.waker().wake_by_ref();
        Poll::Pending
    }

    /// Async sugar for [`Self::poll_next_with_ext`].
    pub async fn next_with_ext<'a, E, EMut>(
        &mut self,
        ext: &'a E,
        ext_mut: &mut EMut,
    ) -> Option<S::Item>
    where
        S: PollWith<'a, E, EMut>,
        E: ?Sized,
        EMut: ?Sized,
    {
        futures::future::poll_fn(|cx| self.poll_next_with_ext(ext, ext_mut, cx)).await
    }
}

impl<S: PollWith<'static, (), ()>> Stream for Selector<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe {
            // SAFETY: no field is ever moved in memory.
            self.get_unchecked_mut()
        };
        this.poll_next_with_ext(&(), &mut (), cx)
    }
}

impl<S: PollStrategy + Default> Default for Selector<S> {
    fn default() -> Self {
        Self {
            ready_rx: mpsc::Receiver::new(Task::new),
            list: Default::default(),
            strategy: Default::default(),
        }
    }
}

impl<S: PollStrategy> fmt::Debug for Selector<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Selector")
            .field("len", &self.len())
            .finish()
    }
}

impl<S: PollStrategy> Extend<S::Pollable> for Selector<S> {
    fn extend<T: IntoIterator<Item = S::Pollable>>(&mut self, iter: T) {
        for pollable in iter {
            self.push(pollable);
        }
    }
}

impl<S: PollStrategy + Default> FromIterator<S::Pollable> for Selector<S> {
    fn from_iter<T: IntoIterator<Item = S::Pollable>>(iter: T) -> Self {
        let mut this = Self::default();
        this.extend(iter);
        this
    }
}

impl<S: PollStrategy> IntoIterator for Selector<S> {
    type IntoIter = IntoIter<S::Pollable>;
    type Item = Removed<S::Pollable>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter(self.list)
    }
}

#[cfg(test)]
mod test {
    use std::{
        ops::Not,
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    use futures::{FutureExt, StreamExt, channel::oneshot};
    use rstest::rstest;

    use crate::{pollable::PollAsFuture, selector::Selector};

    #[tokio::test]
    async fn basic() {
        let (tx, rx) = oneshot::channel::<()>();
        let mut selector = Selector::<PollAsFuture<_>>::default();
        selector.push(rx);
        assert!(selector.next().now_or_never().is_none());
        assert_eq!(selector.len(), 1);
        tx.send(()).unwrap();
        assert!(selector.next().await.is_some());
        assert_eq!(selector.len(), 0);
    }

    /// Verifies that [`Selector`] respects the inner item's yield when polled,
    /// and does not poll the same item twice in a single [`Selector::poll_next_with_ext`] call.
    #[rstest]
    #[tokio::test]
    async fn yield_respected(#[values(1, 4, 8)] futures: usize) {
        struct Fut {
            polled: bool,
        }

        impl Future for Fut {
            type Output = ();

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.get_mut();
                if this.polled.not() {
                    this.polled = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }
        }

        let mut selector = Selector::<PollAsFuture<_>>::default();
        for _ in 0..futures {
            selector.push(Fut { polled: false });
        }

        assert!(
            selector
                .poll_next_with_ext(&(), &mut (), &mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        for fut in selector.iter() {
            assert!(fut.polled);
        }

        for _ in 0..futures {
            assert_eq!(
                selector.poll_next_with_ext(&(), &mut (), &mut Context::from_waker(Waker::noop())),
                Poll::Ready(Some(())),
            );
        }
    }
}
