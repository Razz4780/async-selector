//! Fast and flexible [`Future`]/[`Stream`] selector.

use std::{
    fmt,
    marker::PhantomData,
    ops::{ControlFlow, Not},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;

use crate::{
    list::{
        IntrusiveList,
        cursor::{Cursor, CursorMut},
    },
    mpsc,
    pollable::{PollDirect, PollProxy},
    selector::{
        ext::{WithExt, WithExtAndId, WithId},
        iter::{ExtractIf, IntoIter, Iter, IterMut},
    },
    task::Task,
};

pub use borrowed::{Borrowed, BorrowedMut};
pub use id::Id;
pub use removed::Removed;

mod borrowed;
pub mod ext;
mod id;
pub mod iter;
mod removed;

/// Selector over a dynamic set of pollable tasks, for example [`Future`]s and [`Stream`]s.
///
/// Designed for optimal performance when polling a large number of tasks
/// (see [example](https://github.com/Razz4780/async-selector/blob/main/examples/speed.rs)).
///
/// Allows for:
/// 1. Safely injecting shared state into the tasks (see [`Pollable`](crate::pollable::Pollable)).
/// 2. Accessing and removing the tasks by unique ids.
///
/// Unless you want to exercise the full flexibility of this type,
/// you can use the specialized [`FutureSelector`](crate::FutureSelector)
/// and [`StreamSelector`](crate::StreamSelector).
///
/// # Removal
///
/// The selector creates a heap allocation for each stored task.
/// Removing a task from the selector does not instantly free that memory.
/// The memory can only be freed when:
/// 1. all [`Id`] instances for this task are dropped, AND
/// 2. [`Removed`] instance is dropped or consumed, AND
/// 3. the [`Waker`](std::task::Waker) (and all its clones)
///    passed when polling the task are dropped or consumed, AND
/// 4. the selector observes the task removal.
///
/// The selector always eventually observes the removal when polled.
///
/// # Wakeups
///
/// The selector uses a smart strategy for polling the tasks.
/// A task is **only** polled in the following cases:
/// 1. after it is pushed into the selector
/// 2. after it yields a non-terminal value
/// 3. after the waker passed to [`PollProxy::poll_progress`] inside [`Context`] is woken
///
/// To avoid nasty surprises, keep this in mind when:
/// 1. Modifying a task borrowed from the selector
/// 2. Polling the selector with different extension types
///
/// See [example](https://github.com/Razz4780/async-selector/blob/main/examples/extensions.rs).
///
/// # Panic
///
/// If the task panics when polled, the task is removed from the selector and dropped.
/// The selector remains valid.
pub struct Selector<T, P = PollDirect> {
    /// Queue of tasks that were woken.
    ready_rx: mpsc::Receiver<Task<T>>,
    /// List of all tasks.
    list: IntrusiveList<Task<T>>,
    _proxy: PhantomData<fn() -> P>,
}

impl<T, P> Selector<T, P> {
    /// Pushes a new task into the selector.
    ///
    /// This method is O(1).
    pub fn push(&mut self, task: T) {
        let task = self
            .list
            .insert(Task::empty(self.ready_rx.weak_sender()), task);
        self.ready_rx.send(task);
    }

    /// Pushes a new task into the selector and returns its unique id.
    ///
    /// This method is O(1).
    pub fn push_with_id(&mut self, task: T) -> Id<T> {
        let task = self
            .list
            .insert(Task::empty(self.ready_rx.weak_sender()), task);
        let id = Id(Arc::downgrade(&task));
        self.ready_rx.send(task);
        id
    }

    /// Returns whether the selector is empty.
    ///
    /// This method is O(1).
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Returns the number of tasks in the selector.
    ///
    /// This method is O(1).
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Manually wakes all tasks in the selector.
    ///
    /// Depending on the [`PollProxy::poll_progress`] implementation,
    /// this might be required when polling with different extension types.
    /// See the wakeups [section](Selector#wakeups).
    ///
    /// This method is O(n).
    pub fn wake_all(&self) {
        self.iter().for_each(|borrowed| borrowed.wake());
    }

    /// Returns an iterator over the tasks in the selector.
    ///
    /// The tasks are visited in the insertion order.
    pub fn iter(&self) -> Iter<'_, T> {
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
    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            cursor: CursorMut::new(&mut self.list),
            queue: &self.ready_rx,
        }
    }

    /// Creates an iterator which uses a closure to determine if a task should be removed.
    ///
    /// If the closure returns true, the task is removed from the selector and yielded.
    ///
    /// If the returned [`ExtractIf`] is not exhausted, e.g. because it is dropped without iterating or the iteration short-circuits,
    /// then the remaining tasks will be retained.
    ///
    /// **Important:** before removing tasks from the selector, see the removal [section](Selector#removal).
    #[must_use = "ExtractIf does not remove any elements unless consumed"]
    pub fn extract_if<F>(&mut self, pred: F) -> ExtractIf<'_, T, F>
    where
        F: FnMut(Pin<&mut T>) -> bool,
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
    ///
    /// This method is O(1).
    pub fn get(&self, id: &Id<T>) -> Option<Borrowed<'_, T>> {
        let task = id.0.upgrade()?;
        if self.ready_rx.is_parent(task.ready_tx()).not() {
            return None;
        }
        let node = unsafe {
            // SAFETY: we just checked that this id comes from this selector.
            // Therefore, the task cannot be stored in any other list.
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
    /// This method is O(1).
    ///
    /// **Important:** before modifying tasks stored in the selector, see the wakeups [section](Selector#wakeups).
    pub fn get_mut(&mut self, id: &Id<T>) -> Option<BorrowedMut<'_, T>> {
        let task = id.0.upgrade()?;
        if self.ready_rx.is_parent(task.ready_tx()).not() {
            return None;
        }
        let node = unsafe {
            // SAFETY: we just checked that this id comes from this selector.
            // Therefore, the task cannot be stored in any other list.
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
    /// This method is O(1).
    ///
    /// **Important:** before removing tasks from the selector, see the removal [section](Selector#removal).
    pub fn remove(&mut self, id: &Id<T>) -> Option<Removed<T>> {
        let task = id.0.upgrade()?;
        if self.ready_rx.is_parent(task.ready_tx()).not() {
            return None;
        }
        let removed = unsafe {
            // SAFETY: we just checked that this id comes from this selector.
            // Therefore, the task cannot be stored in any other list.
            self.list.remove(&task)?
        };
        Some(Removed(removed))
    }

    /// Borrows a [`Stream`] that will pass the given extensions to the tasks when polling.
    ///
    /// **Important:** before polling the tasks with different extension types, see the wakeups [section](Selector#wakeups).
    pub fn with_ext<'s, 'e, 'emut, E, EMut>(
        &'s mut self,
        ext: &'e E,
        ext_mut: &'emut mut EMut,
    ) -> WithExt<'s, 'e, 'emut, T, P, E, EMut>
    where
        P: PollProxy<'e, T, E, EMut>,
        E: ?Sized,
        EMut: ?Sized,
    {
        WithExt {
            selector: self,
            ext,
            ext_mut,
        }
    }

    /// Borrows a [`Stream`] that will return a task [`Id`] with every item.
    pub fn with_id(&mut self) -> WithId<'_, T, P>
    where
        P: PollProxy<'static, T, (), ()>,
    {
        WithId { selector: self }
    }

    /// Borrows a [`Stream`] that will pass the given extensions to the tasks when polling,
    /// and return a task [`Id`] with every item.
    ///
    /// **Important:** before polling the tasks with different extension types, see the wakeups [section](Selector#wakeups).
    pub fn with_ext_and_id<'s, 'e, 'emut, E, EMut>(
        &'s mut self,
        ext: &'e E,
        ext_mut: &'emut mut EMut,
    ) -> WithExtAndId<'s, 'e, 'emut, T, P, E, EMut>
    where
        P: PollProxy<'e, T, E, EMut>,
        E: ?Sized,
        EMut: ?Sized,
    {
        WithExtAndId {
            selector: self,
            ext,
            ext_mut,
        }
    }

    /// Returns the next ready item from one of the tasks stored in the selector.
    ///
    /// Provided extensions will be passed down to the tasks as arguments to [`PollWith::poll_progress`].
    /// Provided `extra` lambda can be used to extract additional data from the [`Task`] instance.
    ///
    /// Returns `None` if the selector is empty.
    #[allow(clippy::type_complexity)]
    fn poll_next_inner<'a, E, EMut, F, I>(
        &mut self,
        ext: &'a E,
        ext_mut: &mut EMut,
        extra: F,
        cx: &mut Context<'_>,
    ) -> Poll<Option<(P::Progress, I)>>
    where
        P: PollProxy<'a, T, E, EMut>,
        E: ?Sized,
        EMut: ?Sized,
        F: FnOnce(&Arc<Task<T>>) -> I,
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

            // If this method panics, the guard will automatically remove the task from the list and drop it.
            let result = P::poll_progress(guard.get(), ext, ext_mut, &mut cx);

            match result {
                Poll::Ready(ControlFlow::Continue(item)) => {
                    guard.forget();
                    let extra = extra(&task);
                    self.ready_rx.send(task);
                    return Poll::Ready(Some((item, extra)));
                }
                Poll::Ready(ControlFlow::Break(Some(item))) => {
                    let extra = extra(&task);
                    return Poll::Ready(Some((item, extra)));
                }
                Poll::Ready(ControlFlow::Break(None)) => {}
                Poll::Pending => guard.forget(),
            }
        }

        if self.list.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<T, P> Stream for Selector<T, P>
where
    P: PollProxy<'static, T, (), ()>,
{
    type Item = P::Progress;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe {
            // SAFETY: no field is ever moved in memory.
            self.get_unchecked_mut()
        };
        this.poll_next_inner(&(), &mut (), |_| (), cx)
            .map(|opt| opt.map(|(item, ())| item))
    }
}

impl<T, P> Default for Selector<T, P> {
    fn default() -> Self {
        Self {
            ready_rx: mpsc::Receiver::new(Task::empty),
            list: Default::default(),
            _proxy: Default::default(),
        }
    }
}

impl<T, P> fmt::Debug for Selector<T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Selector")
            .field("len", &self.len())
            .finish()
    }
}

impl<T, P> Extend<T> for Selector<T, P> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for pollable in iter {
            self.push(pollable);
        }
    }
}

impl<T, S> FromIterator<T> for Selector<T, S> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut this = Self::default();
        this.extend(iter);
        this
    }
}

impl<T, S> IntoIterator for Selector<T, S> {
    type IntoIter = IntoIter<T>;
    type Item = Removed<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter(self.list)
    }
}

#[cfg(test)]
mod test {
    use std::{
        ops::Not,
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        sync::Arc,
        task::{Context, Poll, Waker},
    };

    use futures::{FutureExt, StreamExt, channel::oneshot, task::AtomicWaker};
    use rstest::rstest;

    use crate::{pollable::PollFuture, selector::Selector};

    #[tokio::test]
    async fn basic() {
        let (tx, rx) = oneshot::channel::<()>();
        let mut selector = Selector::<_, PollFuture>::default();
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
    async fn task_yield_is_respected(#[values(1, 4, 8)] futures: usize) {
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

        let mut selector = Selector::<_, PollFuture>::default();
        for _ in 0..futures {
            selector.push(Fut { polled: false });
        }

        assert!(
            selector
                .poll_next_unpin(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        for fut in selector.iter() {
            assert!(fut.polled);
        }

        for _ in 0..futures {
            assert_eq!(
                selector.poll_next_unpin(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Some(())),
            );
        }
    }

    #[test]
    fn stale_wakeups_on_removed_tasks_still_report_empty_selector() {
        struct StoreWaker(Arc<AtomicWaker>);

        impl Future for StoreWaker {
            type Output = usize;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                self.0.register(cx.waker());
                Poll::Pending
            }
        }

        let slot = Arc::new(AtomicWaker::new());
        let mut selector = Selector::<_, PollFuture>::default();
        let id = selector.push_with_id(StoreWaker(slot.clone()).boxed());
        let mut cx = Context::from_waker(Waker::noop());

        assert!(selector.poll_next_unpin(&mut cx).is_pending());
        let waker = slot.take().unwrap();
        let _ = selector.remove(&id).unwrap();
        assert!(selector.is_empty());

        for _ in 0..3 {
            waker.wake_by_ref();
            assert_eq!(selector.poll_next_unpin(&mut cx), Poll::Ready(None));
        }

        selector.push(std::future::ready(7).boxed());
        assert_eq!(selector.poll_next_unpin(&mut cx), Poll::Ready(Some(7)));
        assert_eq!(selector.poll_next_unpin(&mut cx), Poll::Ready(None));
    }

    #[test]
    fn panicking_task_is_removed_and_selector_remains_valid() {
        struct PanicOnPoll {
            _shared: Arc<()>,
        }

        impl Future for PanicOnPoll {
            type Output = usize;

            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
                panic!("boom");
            }
        }

        let drops = Arc::new(());
        let drops_weak = Arc::downgrade(&drops);
        let mut selector = Selector::<_, PollFuture>::default();
        selector.push(PanicOnPoll { _shared: drops }.boxed());

        let mut cx = Context::from_waker(Waker::noop());
        let result = catch_unwind(AssertUnwindSafe(|| selector.poll_next_unpin(&mut cx)));

        assert!(result.is_err());
        assert!(selector.is_empty());
        assert_eq!(selector.len(), 0);
        assert!(drops_weak.upgrade().is_none());

        selector.push(std::future::ready(11).boxed());
        assert_eq!(selector.poll_next_unpin(&mut cx), Poll::Ready(Some(11)));
        assert_eq!(selector.poll_next_unpin(&mut cx), Poll::Ready(None));
    }
}
