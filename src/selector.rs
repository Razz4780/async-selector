mod id;
pub mod iter;
mod waker;
mod wrappers;

use std::{
    cell::Cell,
    fmt,
    ops::{ControlFlow, Index, IndexMut, Not},
    pin::Pin,
    rc::Rc,
    sync::{Arc, Weak},
    task::{Context, Poll},
};

use futures::Stream;

use crate::{
    list::{self, List, Node},
    queue::Receiver,
    selector::{iter::ExtractIf, waker::NodeWaker},
    task::Task,
};

pub use crate::selector::{
    id::Id,
    wrappers::{Borrowed, BorrowedMut, Removed},
};

/// Selector over a dynamic set of [`Task`]s (generalized [`Future`]s/[`Stream`]s).
///
/// Inspired by [`FuturesUnordered`](futures::stream::FuturesUnordered),
/// designed for flexibility and optimal performance when polling a large number of tasks.
///
/// Unless you want to exercise the full flexibility of this type,
/// you can stick to the specializations exposed in the root of this crate
/// (e.g. [`FutureSelector`](crate::FutureSelector) and [`StreamSelector`](crate::StreamSelector)).
///
/// # Removal
///
/// The selector creates a heap allocation for each stored task.
/// Removing a task from the selector does not instantly free that memory.
/// The memory can only be freed when:
/// 1. all [`Id`] instances for this task are dropped, AND
/// 2. [`Removed`] instance is dropped, AND
/// 3. the [`Waker`](std::task::Waker) (and all its clones)
///    passed when polling the task is dropped, AND
/// 4. the selector observes the task removal
///    (which happens when the selector is polled).
///
/// # Wakeups
///
/// The selector uses a smart strategy for polling the tasks.
/// A task is **only** polled in the following cases:
/// 1. after it is pushed into the selector
/// 2. after it yields a non-terminal value
/// 3. after the waker passed to [`Task::poll_progress`] receives a wakeup
///
/// To avoid nasty surprises, keep this in mind when:
/// 1. Modifying a task borrowed from the selector
/// 2. Changing the strategy used by the selector
///    (see [example](https://github.com/Razz4780/async-selector/blob/main/examples/custom.rs))
///
/// The wakeups are stored in a FIFO queue. This implies that the selector
/// processes ready tasks in a round-robin fashion.
///
/// # Panic
///
/// If the [`Task`] implementation panics, the task is removed from the selector and dropped,
/// and the panic propagates. The selector remains valid.
pub struct Selector<T, S> {
    /// Queue of tasks that received a wakeup.
    queue: Receiver<list::ListProtected<T>>,
    /// List of all tasks.
    list: List<T>,
    /// Strategy that determines how the selector polls the tasks.
    strategy: S,
}

impl<T, S> Selector<T, S> {
    /// Creates an empty selector with the given strategy.
    pub fn new(strategy: S) -> Self {
        Self {
            queue: Default::default(),
            list: Default::default(),
            strategy,
        }
    }

    /// Pushes the given task into the selector, returning a mutable reference to the task.
    ///
    /// The reference can be used to obtain the task's [`Id`].
    ///
    /// This method is O(1).
    pub fn push(&mut self, task: T) -> BorrowedMut<'_, T> {
        BorrowedMut(self.list.push_back(self.queue.queue(), task))
    }

    /// Returns the number of tasks stored in the selector.
    ///
    /// This method is O(1).
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Returns whether the selector is empty.
    ///
    /// This method is O(1).
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Returns whether the selector contains a task with the given [`Id`].
    ///
    /// This method is O(1).
    pub fn contains(&self, id: &Id<T>) -> bool {
        self.get(id).is_some()
    }

    /// If the selector contains a task with the given [`Id`], returns a reference to it.
    ///
    /// This method is O(1).
    pub fn get<'a>(&'a self, id: &Id<T>) -> Option<Borrowed<'a, T>> {
        if self.created(id.get()).not() {
            return None;
        }
        unsafe { self.list.get(id.get()).map(Borrowed) }
    }

    /// If the selector contains a task with the given [`Id`], returns a mutable reference to it.
    ///
    /// This method is O(1).
    pub fn get_mut<'a>(&'a mut self, id: &Id<T>) -> Option<BorrowedMut<'a, T>> {
        if self.created(id.get()).not() {
            return None;
        }
        unsafe { self.list.get_mut(id.get()).map(BorrowedMut) }
    }

    /// If the selector contains a task with the given [`Id`], removes it.
    ///
    /// This method is O(1).
    pub fn remove(&mut self, id: &Id<T>) -> Option<Removed<T>> {
        if self.created(id.get()).not() {
            return None;
        }
        unsafe { self.list.remove(id.get()).map(Removed) }
    }

    /// Returns a reference to the strategy used by this selector.
    pub fn strategy(&self) -> &S {
        &self.strategy
    }

    /// Returns a mutable reference to the strategy used by this selector.
    pub fn strategy_mut(&mut self) -> &mut S {
        &mut self.strategy
    }

    /// Returns a new selector with the same state, but different strategy.
    pub fn with_strategy<S1>(self, strategy: S1) -> Selector<T, S1> {
        Selector {
            queue: self.queue,
            list: self.list,
            strategy,
        }
    }

    /// Returns an iterator over all tasks in the selector.
    ///
    /// The tasks are visited in the insertion order.
    pub fn iter(&self) -> iter::Iter<'_, T> {
        iter::Iter(self.list.cursor())
    }

    /// Returns an iterator that allows for modifying each task in the selector.
    ///
    /// The tasks are visited in the insertion order.
    pub fn iter_mut(&mut self) -> iter::IterMut<'_, T> {
        iter::IterMut(self.list.cursor_mut())
    }

    /// Creates an iterator which uses a closure to determine if a task should be removed.
    ///
    /// If the closure returns true, the task is removed from the selector and yielded.
    /// The tasks are visited in the insertion order.
    ///
    /// If the returned [`ExtractIf`] is not exhausted, e.g. because it is dropped without iterating or the iteration short-circuits,
    /// then the remaining tasks will be retained.
    #[must_use = "ExtractIf does not remove any elements unless consumed"]
    pub fn extract_if<F>(&mut self, pred: F) -> ExtractIf<'_, T, F>
    where
        F: for<'b> FnMut(BorrowedMut<'b, T>) -> bool,
    {
        ExtractIf {
            cursor: self.list.cursor_mut(),
            pred,
        }
    }

    /// Retains only the tasks specified by the predicate.
    ///
    /// In other words, remove all tasks for which the predicate returns false.
    /// The tasks are visited in the insertion order.
    pub fn retain<F>(&mut self, mut pred: F)
    where
        F: for<'b> FnMut(BorrowedMut<'b, T>) -> bool,
    {
        for _ in self.extract_if(|borrowed| pred(borrowed).not()) {}
    }

    /// Manually wakes all tasks in the selector.
    pub fn wake_all(&self) {
        self.iter().for_each(|task| task.id().wake());
    }

    fn created(&self, task: &Node<T>) -> bool {
        let this_queue_ptr = Arc::as_ptr(self.queue.queue());
        let task_queue_ptr = Weak::as_ptr(task.queue());
        this_queue_ptr == task_queue_ptr
    }
}

impl<T, S> Extend<T> for Selector<T, S> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for task in iter {
            self.push(task);
        }
    }
}

impl<T, S> FromIterator<T> for Selector<T, S>
where
    S: Default,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut this = Self::default();
        this.extend(iter);
        this
    }
}

impl<T, S> IntoIterator for Selector<T, S> {
    type IntoIter = iter::IntoIter<T>;
    type Item = Removed<T>;

    fn into_iter(self) -> Self::IntoIter {
        iter::IntoIter(self.list)
    }
}

impl<'a, T, S> IntoIterator for &'a Selector<T, S> {
    type IntoIter = iter::Iter<'a, T>;
    type Item = Borrowed<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T, S> IntoIterator for &'a mut Selector<T, S> {
    type IntoIter = iter::IterMut<'a, T>;
    type Item = BorrowedMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<T, S> Stream for Selector<T, S>
where
    T: Task<S>,
    S: Unpin,
{
    type Item = T::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let marker = this.queue.register_waker(cx.waker());
        if marker.is_null() {
            return if this.list.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Pending
            };
        }

        while let Some(node) = this.queue.dequeue() {
            let is_last = Arc::as_ptr(node.get()) == marker;

            let guard = unsafe { this.list.access(node.get()) };
            let Some(mut guard) = guard else {
                if is_last {
                    break;
                } else {
                    continue;
                }
            };

            let node = node.into_inner();

            let result = {
                let waker = NodeWaker::new(&node);
                guard
                    .borrow_mut()
                    .get_pin_mut()
                    .poll_progress(&mut this.strategy, &mut Context::from_waker(&waker))
            };

            match result {
                Poll::Ready(ControlFlow::Continue(val)) => {
                    unsafe {
                        // SAFETY: node was dequeued from this queue
                        this.queue.queue().enqueue(node);
                    }
                    let output =
                        T::transform_cont(BorrowedMut(guard.borrow_mut()), &mut this.strategy, val);
                    guard.forget();
                    if output.is_some() {
                        return Poll::Ready(output);
                    }
                }

                Poll::Ready(ControlFlow::Break(val)) => {
                    let node = guard.remove_now();
                    let output = T::transform_break(Removed(node), &mut this.strategy, val);
                    if output.is_some() {
                        return Poll::Ready(output);
                    }
                }

                Poll::Pending => {
                    guard.forget();
                }
            }

            if is_last {
                break;
            }
        }

        if this.list.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.list.is_empty().then_some(0))
    }
}

impl<T, S> Default for Selector<T, S>
where
    S: Default,
{
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<T, S> fmt::Debug for Selector<T, S>
where
    S: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Selector")
            .field("tasks", &self.list.len())
            .field("strategy", &self.strategy)
            .field("queue_ptr", &Arc::as_ptr(self.queue.queue()))
            .finish_non_exhaustive()
    }
}

impl<T, S> Index<&Id<T>> for Selector<T, S> {
    type Output = T;

    fn index(&self, id: &Id<T>) -> &Self::Output {
        self.get(id).expect("task not found").into_pin().get_ref()
    }
}

impl<T, S> Index<Id<T>> for Selector<T, S> {
    type Output = T;

    fn index(&self, id: Id<T>) -> &Self::Output {
        &self[&id]
    }
}

impl<T, S> IndexMut<&Id<T>> for Selector<T, S>
where
    T: Unpin,
{
    fn index_mut(&mut self, id: &Id<T>) -> &mut Self::Output {
        self.get_mut(id)
            .expect("task not found")
            .into_pin_mut()
            .get_mut()
    }
}

impl<T, S> IndexMut<Id<T>> for Selector<T, S>
where
    T: Unpin,
{
    fn index_mut(&mut self, id: Id<T>) -> &mut Self::Output {
        &mut self[&id]
    }
}

unsafe impl<T, S> Send for Selector<T, S>
where
    T: Send,
    S: Send,
{
}

unsafe impl<T, S> Sync for Selector<T, S>
where
    T: Sync,
    S: Sync,
{
}

static_assertions::assert_impl_all!(Selector<(), ()>: Send, Sync);
static_assertions::assert_impl_all!(Selector<Cell<()>, Cell<()>>: Send);
static_assertions::assert_not_impl_any!(Selector<Cell<()>, ()>: Sync);
static_assertions::assert_not_impl_any!(Selector<(), Cell<()>>: Sync);
static_assertions::assert_not_impl_any!(Selector<Rc<()>, ()>: Send, Sync);
static_assertions::assert_not_impl_any!(Selector<(), Rc<()>>: Send, Sync);

#[cfg(test)]
mod test {
    use std::{
        ops::ControlFlow,
        panic::AssertUnwindSafe,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll, Waker},
    };

    use futures::{FutureExt, StreamExt};

    use crate::{
        FutureSelector, StreamSelector,
        selector::{BorrowedMut, Removed, Selector},
        task::Task,
    };

    #[test]
    fn retain_removes_correct_tasks() {
        let mut selector = (-3_i32..=3).collect::<FutureSelector<_>>();
        selector.retain(|task| task.is_positive());
        let retained = selector
            .into_iter()
            .map(Removed::into_inner)
            .collect::<Vec<_>>();
        assert_eq!(retained, &[1, 2, 3],);
    }

    #[test]
    fn extract_if_removes_correct_tasks() {
        let mut selector = (-3_i32..=3).collect::<FutureSelector<_>>();
        let iter = selector.extract_if(|task| task.is_positive());
        assert_eq!(
            iter.map(Removed::into_inner).collect::<Vec<_>>(),
            vec![1, 2, 3],
        );
        let retained = selector
            .into_iter()
            .map(Removed::into_inner)
            .collect::<Vec<_>>();
        assert_eq!(retained, &[-3, -2, -1, 0],);
    }

    #[test]
    fn extract_if_retains_tasks_when_dropped() {
        let mut selector = (-3_i32..=3).collect::<FutureSelector<_>>();
        let iter = selector.extract_if(|task| task.is_positive());
        assert_eq!(
            iter.take(1).map(Removed::into_inner).collect::<Vec<_>>(),
            vec![1],
        );
        let retained = selector
            .into_iter()
            .map(Removed::into_inner)
            .collect::<Vec<_>>();
        assert_eq!(retained, &[-3, -2, -1, 0, 2, 3],);
    }

    #[test]
    fn single_selector_poll_polls_each_task_at_most_once() {
        #[derive(Clone)]
        struct Task(usize);

        impl Future for Task {
            type Output = ();

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let this = self.get_mut();
                this.0 += 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }

        let mut selector = std::iter::repeat_n(Task(0), 3).collect::<FutureSelector<_>>();
        for i in 0..=3 {
            selector
                .iter()
                .for_each(|borrowed| assert_eq!(borrowed.get_pin().0, i));
            assert!(
                selector
                    .poll_next_unpin(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
    }

    #[tokio::test]
    async fn selector_respects_strategy_and_round_robin_order() {
        #[derive(Clone)]
        struct MyTask(usize);

        impl Task for MyTask {
            type Cont = usize;
            type Break = usize;
            type Output = usize;

            fn poll_progress(
                self: Pin<&mut Self>,
                _: &mut (),
                _: &mut Context<'_>,
            ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
                let state = self.get_mut();
                state.0 += 1;
                if state.0 > 5 {
                    Poll::Ready(ControlFlow::Break(state.0))
                } else {
                    Poll::Ready(ControlFlow::Continue(state.0))
                }
            }

            fn transform_cont(
                _: BorrowedMut<'_, Self>,
                _: &mut (),
                value: Self::Cont,
            ) -> Option<Self::Output> {
                value.is_power_of_two().then_some(value)
            }

            fn transform_break(
                _: Removed<Self>,
                _: &mut (),
                value: Self::Break,
            ) -> Option<Self::Output> {
                Some(value * 2)
            }
        }

        let selector = std::iter::repeat_n(MyTask(0), 3).collect::<Selector<_, ()>>();
        let results = selector.collect::<Vec<_>>().await;
        assert_eq!(results, vec![1, 1, 1, 2, 2, 2, 4, 4, 4, 12, 12, 12],);
    }

    #[tokio::test]
    async fn selector_returns_valid_ids() {
        let mut selector = StreamSelector::default();
        let id_0 = selector.push(futures::stream::repeat(0)).id().clone();
        let id_1 = selector.push(futures::stream::repeat(1)).id().clone();
        assert!(selector.contains(&id_0));
        assert!(selector.contains(&id_1));
        assert_eq!(selector.next().await.unwrap(), 0);
        assert_eq!(selector.next().await.unwrap(), 1);
        assert_eq!(selector.next().await.unwrap(), 0);
        assert_eq!(selector.next().await.unwrap(), 1);
        selector.remove(&id_0);
        assert_eq!(selector.next().await.unwrap(), 1);
        assert_eq!(selector.next().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn selector_handles_strategy_panic() {
        struct MyTask(usize);

        impl Task for MyTask {
            type Cont = usize;
            type Break = usize;
            type Output = usize;

            fn poll_progress(
                self: Pin<&mut Self>,
                _: &mut (),
                _: &mut Context<'_>,
            ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
                match self.0 {
                    1 => panic!("poll panic"),
                    2 => Poll::Ready(ControlFlow::Continue(2)),
                    n => Poll::Ready(ControlFlow::Break(n)),
                }
            }

            fn transform_cont(
                _: BorrowedMut<'_, Self>,
                _: &mut (),
                _: Self::Cont,
            ) -> Option<Self::Output> {
                panic!("cont panic")
            }

            fn transform_break(
                _: Removed<Self>,
                _: &mut (),
                value: Self::Break,
            ) -> Option<Self::Output> {
                if value == 3 {
                    panic!("break panic")
                } else {
                    Some(value)
                }
            }
        }

        let mut selector = (0..=4).map(MyTask).collect::<Selector<_, ()>>();
        assert_eq!(selector.len(), 5);

        assert_eq!(selector.next().await.unwrap(), 0);
        assert_eq!(selector.len(), 4);

        let err = AssertUnwindSafe(selector.next())
            .catch_unwind()
            .await
            .unwrap_err();
        assert_eq!(*err.downcast_ref::<&'static str>().unwrap(), "poll panic",);
        assert_eq!(selector.len(), 3);

        let err = AssertUnwindSafe(selector.next())
            .catch_unwind()
            .await
            .unwrap_err();
        assert_eq!(*err.downcast_ref::<&'static str>().unwrap(), "cont panic",);
        assert_eq!(selector.len(), 2);

        let err = AssertUnwindSafe(selector.next())
            .catch_unwind()
            .await
            .unwrap_err();
        assert_eq!(*err.downcast_ref::<&'static str>().unwrap(), "break panic",);
        assert_eq!(selector.len(), 1);

        assert_eq!(selector.next().await.unwrap(), 4);
        assert_eq!(selector.len(), 0);
    }

    #[tokio::test]
    async fn selector_handles_drop_panic() {
        struct Task(usize);

        impl Future for Task {
            type Output = usize;

            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
                Poll::Ready(self.0)
            }
        }

        impl Drop for Task {
            fn drop(&mut self) {
                if self.0 < 2 {
                    panic!();
                }
            }
        }

        let mut selector = (0..3).map(Task).collect::<FutureSelector<_>>();
        let ids = selector
            .iter()
            .map(|task| task.id().clone())
            .collect::<Vec<_>>();

        AssertUnwindSafe(selector.next())
            .catch_unwind()
            .await
            .unwrap_err();
        assert_eq!(selector.len(), 2);
        assert_eq!(Arc::strong_count(ids[0].get()), 1);

        let selector = AssertUnwindSafe(selector);
        std::panic::catch_unwind(|| drop(selector)).unwrap_err();

        for id in ids {
            assert_eq!(Arc::strong_count(id.get()), 1);
        }
    }
}
