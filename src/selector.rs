//! The [`Selector`] type, along with the types used to access the tasks stored in it.
//!
//! * [`Id`] identifies a task stored in a selector.
//! * [`Borrowed`] and [`BorrowedMut`] are references to a task stored in a selector.
//! * [`Removed`] is a task removed from a selector.
//! * The [`iter`] module contains iterators over the stored tasks.

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
/// You push tasks into the selector, and then poll it as a [`Stream`].
/// The selector polls the tasks that are ready to make progress,
/// and yields their results (the [`Task::Output`] values).
///
/// The type parameters are:
/// * `T` - the type of the stored tasks.
/// * `S` - the strategy, which decides how the tasks are polled and what the selector yields.
///   See [`Task`] and [`strategy`](crate::task::strategy).
///
/// Usually you don't need to name the strategy yourself.
/// The crate root exposes a type alias for each of the built-in strategies,
/// e.g. [`FutureSelector`](crate::FutureSelector) and [`StreamSelector`](crate::StreamSelector)
/// (see [choosing a selector](crate#choosing-a-selector)).
///
/// # Example
///
/// A typical event loop, which accepts new jobs and collects the results of the finished ones
/// at the same time:
///
/// ```
/// use std::time::Duration;
///
/// use async_selector::FutureSelector;
/// use futures::StreamExt;
/// use tokio::sync::mpsc;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// // Jobs are submitted through a channel.
/// // Each job is the number of milliseconds it takes to complete.
/// let (job_tx, mut job_rx) = mpsc::unbounded_channel::<u64>();
/// for millis in [30, 10, 20] {
///     job_tx.send(millis).unwrap();
/// }
/// // Close the channel, so that the loop below knows that no more jobs are coming.
/// drop(job_tx);
///
/// let mut jobs = FutureSelector::default();
/// let mut results = Vec::new();
/// loop {
///     tokio::select! {
///         // Got a new job, start running it.
///         Some(millis) = job_rx.recv() => {
///             jobs.push(async move {
///                 tokio::time::sleep(Duration::from_millis(millis)).await;
///                 millis
///             });
///         }
///
///         // A job finished.
///         // When the selector is empty, `next` returns `None`, which does not match
///         // the pattern. This disables the branch, instead of spinning in the loop.
///         Some(millis) = jobs.next() => results.push(millis),
///
///         // The channel is closed and all jobs finished.
///         else => break,
///     }
/// }
///
/// results.sort();
/// assert_eq!(results, [10, 20, 30]);
/// # }
/// ```
///
/// # Wakeups
///
/// The selector does not poll all of its tasks every time it is polled.
/// A task is polled **only**:
/// 1. right after it is pushed into the selector,
/// 2. right after it yields a non-terminal value ([`ControlFlow::Continue`]),
///    so that it can produce the next one,
/// 3. after the [`Waker`](std::task::Waker) passed to [`Task::poll_progress`] is woken.
///
/// This is what makes the selector fast when it holds many idle tasks,
/// but it can surprise you when you change something that affects a task's readiness
/// without waking the task, for example when you:
/// 1. modify a task through [`get_mut`](Self::get_mut) or [`iter_mut`](Self::iter_mut),
/// 2. change the strategy with [`strategy_mut`](Self::strategy_mut)
///    or [`with_strategy`](Self::with_strategy).
///
/// In such cases, wake the affected tasks manually with [`Id::wake`] or [`Selector::wake_all`]
/// (see the [custom task example](https://github.com/Razz4780/async-selector/blob/main/examples/custom.rs)).
///
/// Woken tasks are polled in the order in which they were woken (FIFO).
/// This means that ready tasks take turns, and no task can starve the others.
///
/// # Memory
///
/// Each task lives in its own heap allocation.
///
/// When you remove a task, you get back a [`Removed`] handle, which owns the task.
/// Dropping the handle drops the task right away.
/// However, the allocation itself is freed only when all of the following are dropped:
/// 1. all [`Id`]s of the task,
/// 2. the [`Removed`] handle,
/// 3. all clones of the [`Waker`](std::task::Waker) that was passed to the task when it was polled,
/// 4. the selector's own reference, if the task was woken before it was removed.
///    The selector releases it the next time it is polled.
///
/// # Panics
///
/// If the [`Task`] implementation panics while the selector polls it,
/// the task is removed from the selector and dropped, and the panic propagates to the caller.
/// The selector remains valid, and you can keep using it.
pub struct Selector<T, S> {
    tasks: Tasks<T>,
    /// Strategy that determines how the selector polls the tasks.
    strategy: S,
}

/// List of all tasks, bundled with the queue of tasks that received a wakeup.
///
/// The queue requires that all tasks are removed from the list before the queue is dropped
/// (see [`Receiver`]). This type exists to enforce that in one place,
/// instead of relying on the order of fields in every type that owns both.
struct Tasks<T> {
    /// Queue of tasks that received a wakeup.
    queue: Receiver<list::ListProtected<T>>,
    /// List of all tasks.
    list: List<T>,
}

impl<T> Drop for Tasks<T> {
    fn drop(&mut self) {
        // Runs before any field is dropped.
        // If a task panics when dropped, the list still removes all the remaining tasks.
        self.list.clear();
    }
}

impl<T, S> Selector<T, S> {
    /// Creates an empty selector with the given strategy.
    ///
    /// If the strategy implements [`Default`] (all built-in strategies do),
    /// you can use [`Selector::default`] instead.
    ///
    /// ```
    /// use async_selector::{selector::Selector, task::strategy::FutureBasic};
    ///
    /// let selector = Selector::<std::future::Ready<u32>, _>::new(FutureBasic);
    /// assert!(selector.is_empty());
    /// ```
    pub fn new(strategy: S) -> Self {
        Self {
            tasks: Tasks {
                queue: Default::default(),
                list: Default::default(),
            },
            strategy,
        }
    }

    /// Pushes a task into the selector.
    ///
    /// The task will be polled the next time the selector is polled.
    ///
    /// Returns a mutable reference to the stored task,
    /// which you can use to get the task's [`Id`].
    /// If you don't need it, simply ignore the return value.
    ///
    /// This method is O(1).
    ///
    /// ```
    /// use async_selector::FutureSelector;
    ///
    /// let mut selector = FutureSelector::default();
    ///
    /// // Keep the ID to access the task later...
    /// let id = selector.push(std::future::ready(1)).id().clone();
    /// // ...or just ignore it.
    /// selector.push(std::future::ready(2));
    ///
    /// assert_eq!(selector.len(), 2);
    /// assert!(selector.contains(&id));
    /// ```
    pub fn push(&mut self, task: T) -> BorrowedMut<'_, T> {
        BorrowedMut(self.tasks.list.push_back(self.tasks.queue.queue(), task))
    }

    /// Returns the number of tasks stored in the selector.
    ///
    /// This method is O(1).
    pub fn len(&self) -> usize {
        self.tasks.list.len()
    }

    /// Returns `true` if the selector holds no tasks.
    ///
    /// This method is O(1).
    pub fn is_empty(&self) -> bool {
        self.tasks.list.is_empty()
    }

    /// Returns `true` if the selector holds the task with the given [`Id`].
    ///
    /// Returns `false` if the task was removed (or has finished),
    /// or if the [`Id`] comes from a different selector.
    ///
    /// This method is O(1).
    ///
    /// ```
    /// use async_selector::FutureSelector;
    ///
    /// let mut selector = FutureSelector::default();
    /// let id = selector.push(std::future::ready(1)).id().clone();
    /// assert!(selector.contains(&id));
    ///
    /// selector.remove(&id);
    /// assert!(!selector.contains(&id));
    /// ```
    pub fn contains(&self, id: &Id<T>) -> bool {
        self.get(id).is_some()
    }

    /// Returns a reference to the task with the given [`Id`],
    /// or [`None`] if the selector does not hold it.
    ///
    /// The returned [`Borrowed`] dereferences to the task.
    /// You can also use indexing (`selector[&id]`), which panics if the task is not found.
    ///
    /// This method is O(1).
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use async_selector::FutureSelector;
    /// use tokio::time::{Instant, sleep};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let mut timers = FutureSelector::default();
    /// let id = timers.push(sleep(Duration::from_secs(60))).id().clone();
    ///
    /// // `Borrowed` dereferences to `Sleep`, so we can call its methods directly.
    /// let timer = timers.get(&id).unwrap();
    /// assert!(timer.deadline() > Instant::now());
    ///
    /// // The same, using indexing.
    /// assert!(timers[&id].deadline() > Instant::now());
    /// # }
    /// ```
    pub fn get<'a>(&'a self, id: &Id<T>) -> Option<Borrowed<'a, T>> {
        if self.created(id.get()).not() {
            return None;
        }
        unsafe { self.tasks.list.get(id.get()).map(Borrowed) }
    }

    /// Returns a mutable reference to the task with the given [`Id`],
    /// or [`None`] if the selector does not hold it.
    ///
    /// The selector stores the tasks pinned, so the returned [`BorrowedMut`] gives you
    /// a [`Pin<&mut T>`](BorrowedMut::get_pin_mut).
    /// If the task is [`Unpin`], [`BorrowedMut`] also dereferences mutably to it,
    /// and you can use indexing (`selector[&id]`), which panics if the task is not found.
    ///
    /// Keep in mind that the selector does not know that you modified the task.
    /// If your change can make the task ready, wake it with [`Id::wake`]
    /// (see [wakeups](Self#wakeups)).
    ///
    /// This method is O(1).
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use async_selector::FutureSelector;
    /// use futures::StreamExt;
    /// use tokio::time::{Instant, sleep};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let mut timers = FutureSelector::default();
    /// let id = timers.push(sleep(Duration::from_secs(60))).id().clone();
    ///
    /// // `Sleep` is not `Unpin`, so we need a pinned reference to reset it.
    /// let mut timer = timers.get_mut(&id).unwrap();
    /// timer.get_pin_mut().reset(Instant::now());
    ///
    /// // Make sure the selector polls the timer again.
    /// id.wake();
    ///
    /// // The timer fires right away, instead of after 60 seconds.
    /// assert_eq!(timers.next().await, Some(()));
    /// # }
    /// ```
    pub fn get_mut<'a>(&'a mut self, id: &Id<T>) -> Option<BorrowedMut<'a, T>> {
        if self.created(id.get()).not() {
            return None;
        }
        unsafe { self.tasks.list.get_mut(id.get()).map(BorrowedMut) }
    }

    /// Removes the task with the given [`Id`] from the selector,
    /// or returns [`None`] if the selector does not hold it.
    ///
    /// The returned [`Removed`] owns the task.
    /// Drop it to drop the task, or take the task out with [`Removed::into_inner`].
    ///
    /// This method is O(1).
    ///
    /// ```
    /// use async_selector::StreamSelector;
    /// use futures::channel::mpsc;
    ///
    /// let mut selector = StreamSelector::default();
    /// let (_tx, rx) = mpsc::unbounded::<u32>();
    /// let id = selector.push(rx).id().clone();
    ///
    /// // Take the receiver back out of the selector.
    /// let _rx = selector.remove(&id).unwrap().into_inner();
    /// assert!(selector.is_empty());
    ///
    /// // The task is gone, so removing it again returns `None`.
    /// assert!(selector.remove(&id).is_none());
    /// ```
    pub fn remove(&mut self, id: &Id<T>) -> Option<Removed<T>> {
        if self.created(id.get()).not() {
            return None;
        }
        unsafe { self.tasks.list.remove(id.get()).map(Removed) }
    }

    /// Returns a reference to the strategy used by this selector.
    pub fn strategy(&self) -> &S {
        &self.strategy
    }

    /// Returns a mutable reference to the strategy used by this selector.
    ///
    /// If your change to the strategy can make some tasks ready,
    /// wake them with [`Id::wake`] or [`Selector::wake_all`] (see [wakeups](Self#wakeups)).
    pub fn strategy_mut(&mut self) -> &mut S {
        &mut self.strategy
    }

    /// Converts this selector into one that uses a different strategy, keeping all the tasks.
    ///
    /// This is useful when your tasks implement [`Task`] for more than one strategy,
    /// and you want to change the way they are polled,
    /// e.g. switch from reading data to a graceful shutdown.
    ///
    /// Tasks that are waiting for a wakeup will not be polled with the new strategy
    /// until they are woken. If they should be, call [`wake_all`](Self::wake_all)
    /// (see the [custom task example](https://github.com/Razz4780/async-selector/blob/main/examples/custom.rs)).
    ///
    /// ```
    /// use async_selector::{StreamSelector, task::strategy::StreamWithId};
    /// use futures::StreamExt;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let mut selector = StreamSelector::default();
    /// selector.push(futures::stream::repeat(1));
    /// assert_eq!(selector.next().await, Some(1));
    ///
    /// // Same streams, but now each item comes with the ID of its stream.
    /// let mut selector = selector.with_strategy(StreamWithId);
    /// let (_id, item) = selector.next().await.unwrap();
    /// assert_eq!(item, 1);
    /// # }
    /// ```
    pub fn with_strategy<S1>(self, strategy: S1) -> Selector<T, S1> {
        Selector {
            tasks: self.tasks,
            strategy,
        }
    }

    /// Returns an iterator over references to all tasks in the selector,
    /// in the order in which they were pushed.
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use async_selector::FutureSelector;
    /// use tokio::time::sleep;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let mut timers = FutureSelector::default();
    /// for secs in [3, 1, 2] {
    ///     timers.push(sleep(Duration::from_secs(secs)));
    /// }
    ///
    /// // Find the deadline of the timer that fires first.
    /// let first = timers.iter().map(|timer| timer.deadline()).min().unwrap();
    ///
    /// // The timers are visited in the order in which they were pushed,
    /// // so the 1 second timer is the second one.
    /// assert_eq!(first, timers.iter().nth(1).unwrap().deadline());
    /// # }
    /// ```
    pub fn iter(&self) -> iter::Iter<'_, T> {
        iter::Iter(self.tasks.list.cursor())
    }

    /// Returns an iterator over mutable references to all tasks in the selector,
    /// in the order in which they were pushed.
    ///
    /// Keep in mind that the selector does not know that you modified the tasks.
    /// If your change can make them ready, wake them (see [wakeups](Self#wakeups)).
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use async_selector::FutureSelector;
    /// use tokio::time::sleep;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let mut timers = FutureSelector::default();
    /// for secs in [3, 1, 2] {
    ///     timers.push(sleep(Duration::from_secs(secs)));
    /// }
    ///
    /// // Postpone all timers by one second.
    /// // This makes them fire later, so there is no need to wake them.
    /// for mut timer in timers.iter_mut() {
    ///     let deadline = timer.deadline();
    ///     timer.get_pin_mut().reset(deadline + Duration::from_secs(1));
    /// }
    /// # }
    /// ```
    pub fn iter_mut(&mut self) -> iter::IterMut<'_, T> {
        iter::IterMut(self.tasks.list.cursor_mut())
    }

    /// Returns an iterator that removes and yields the tasks
    /// for which the given predicate returns `true`.
    ///
    /// The tasks are visited in the order in which they were pushed.
    /// The predicate receives a mutable reference to each task.
    ///
    /// The tasks are removed lazily, as you iterate.
    /// If you drop the returned [`ExtractIf`] before it is exhausted,
    /// the remaining tasks stay in the selector.
    /// To simply drop the matching tasks, use [`retain`](Self::retain).
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use async_selector::FutureSelector;
    /// use tokio::time::{Instant, sleep};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let mut timers = FutureSelector::default();
    /// for secs in [3, 1, 2] {
    ///     timers.push(sleep(Duration::from_secs(secs)));
    /// }
    ///
    /// // Take out all timers that fire within 2 seconds.
    /// let soon = Instant::now() + Duration::from_secs(2);
    /// let removed = timers
    ///     .extract_if(|timer| timer.deadline() <= soon)
    ///     .collect::<Vec<_>>();
    ///
    /// assert_eq!(removed.len(), 2);
    /// assert_eq!(timers.len(), 1);
    /// # }
    /// ```
    #[must_use = "ExtractIf does not remove any elements unless consumed"]
    pub fn extract_if<F>(&mut self, pred: F) -> ExtractIf<'_, T, F>
    where
        F: for<'b> FnMut(BorrowedMut<'b, T>) -> bool,
    {
        ExtractIf {
            cursor: self.tasks.list.cursor_mut(),
            pred,
        }
    }

    /// Keeps only the tasks for which the given predicate returns `true`,
    /// and drops the rest.
    ///
    /// The tasks are visited in the order in which they were pushed.
    /// The predicate receives a mutable reference to each task.
    /// To get the removed tasks back, use [`extract_if`](Self::extract_if).
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use async_selector::FutureSelector;
    /// use tokio::time::{Instant, sleep};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let mut timers = FutureSelector::default();
    /// for secs in [3, 1, 2] {
    ///     timers.push(sleep(Duration::from_secs(secs)));
    /// }
    ///
    /// // Drop all timers that fire later than in 2 seconds.
    /// let cutoff = Instant::now() + Duration::from_secs(2);
    /// timers.retain(|timer| timer.deadline() <= cutoff);
    ///
    /// assert_eq!(timers.len(), 2);
    /// # }
    /// ```
    pub fn retain<F>(&mut self, mut pred: F)
    where
        F: for<'b> FnMut(BorrowedMut<'b, T>) -> bool,
    {
        for _ in self.extract_if(|borrowed| pred(borrowed).not()) {}
    }

    /// Wakes all tasks in the selector,
    /// so that each of them is polled the next time the selector is polled.
    ///
    /// Use this after a change that can make the tasks ready without waking them,
    /// e.g. after switching the strategy with [`with_strategy`](Self::with_strategy)
    /// (see [wakeups](Self#wakeups)).
    /// To wake a single task, use [`Id::wake`].
    pub fn wake_all(&self) {
        let queue = self.tasks.queue.queue();
        self.iter().for_each(|task| unsafe {
            // SAFETY: the task was created from this queue.
            queue.enqueue_silent(task.id().get());
        });
        queue.notify();
    }

    fn created(&self, task: &Node<T>) -> bool {
        let this_queue_ptr = Arc::as_ptr(self.tasks.queue.queue());
        let task_queue_ptr = Weak::as_ptr(task.queue());
        this_queue_ptr == task_queue_ptr
    }
}

/// Pushes all tasks from the iterator into the selector.
impl<T, S> Extend<T> for Selector<T, S> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for task in iter {
            self.push(task);
        }
    }
}

/// Creates a selector with the default strategy, holding all tasks from the iterator.
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

/// Removes all tasks from the selector, in the order in which they were pushed.
impl<T, S> IntoIterator for Selector<T, S> {
    type IntoIter = iter::IntoIter<T>;
    type Item = Removed<T>;

    fn into_iter(self) -> Self::IntoIter {
        iter::IntoIter(self.tasks)
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

/// Polls the woken tasks, and yields the first [`Task::Output`] they produce.
///
/// Yields [`None`] when the selector is empty.
/// This does not mean that the selector is done for good:
/// you can push more tasks and keep polling it.
impl<T, S> Stream for Selector<T, S>
where
    T: Task<S>,
    S: Unpin,
{
    type Item = T::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let marker = this.tasks.queue.marker();
        if marker.is_null().not() {
            while let Some(dequeued) = this.tasks.queue.dequeue() {
                let is_last = dequeued.as_ptr() == marker;

                let node = unsafe {
                    // SAFETY: nodes are kept alive by the list until they are retired.
                    dequeued.into_live()
                };
                let Some(node) = node else {
                    // Task was removed.
                    if is_last {
                        break;
                    } else {
                        continue;
                    }
                };

                let mut guard = unsafe {
                    // SAFETY: node was dequeued from our queue and is not retired,
                    // so it is stored in our list.
                    this.tasks.list.access(&node)
                };

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
                            // SAFETY: node was dequeued from this queue and is not retired
                            this.tasks.queue.requeue(&node);
                        }
                        let output = T::transform_cont(
                            BorrowedMut(guard.borrow_mut()),
                            &mut this.strategy,
                            val,
                        );
                        guard.forget();
                        if output.is_some() {
                            return Poll::Ready(output);
                        }
                    },

                    Poll::Ready(ControlFlow::Break(val)) => {
                        let node = guard.remove_now();
                        let output = T::transform_break(Removed(node), &mut this.strategy, val);
                        if output.is_some() {
                            return Poll::Ready(output);
                        }
                    },

                    Poll::Pending => {
                        guard.forget();
                    },
                }

                if is_last {
                    break;
                }
            }
        }

        if this.tasks.list.is_empty() {
            return Poll::Ready(None);
        }

        // We only get here when we have nothing to yield.
        // 1. If there may be tasks waiting in the queue (we finished the pass over the snapshot,
        //    or an enqueue is in progress), yield to the executor and come back.
        // 2. Otherwise, go to sleep. This is the only place where we register the waker.
        if this.tasks.queue.has_pending() || this.tasks.queue.park(cx.waker()).not() {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.tasks.list.is_empty().then_some(0))
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
            .field("tasks", &self.tasks.list.len())
            .field("strategy", &self.strategy)
            .field("queue_ptr", &Arc::as_ptr(self.tasks.queue.queue()))
            .finish_non_exhaustive()
    }
}

/// Returns a reference to the task with the given [`Id`].
///
/// # Panics
///
/// Panics if the selector does not hold the task. Use [`Selector::get`] to avoid that.
impl<T, S> Index<&Id<T>> for Selector<T, S> {
    type Output = T;

    fn index(&self, id: &Id<T>) -> &Self::Output {
        self.get(id).expect("task not found").into_pin().get_ref()
    }
}

/// Returns a reference to the task with the given [`Id`].
///
/// # Panics
///
/// Panics if the selector does not hold the task. Use [`Selector::get`] to avoid that.
impl<T, S> Index<Id<T>> for Selector<T, S> {
    type Output = T;

    fn index(&self, id: Id<T>) -> &Self::Output {
        &self[&id]
    }
}

/// Returns a mutable reference to the task with the given [`Id`].
///
/// Only available when the task is [`Unpin`]. Otherwise, use [`Selector::get_mut`].
///
/// # Panics
///
/// Panics if the selector does not hold the task. Use [`Selector::get_mut`] to avoid that.
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

/// Returns a mutable reference to the task with the given [`Id`].
///
/// Only available when the task is [`Unpin`]. Otherwise, use [`Selector::get_mut`].
///
/// # Panics
///
/// Panics if the selector does not hold the task. Use [`Selector::get_mut`] to avoid that.
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
        ops::{ControlFlow, Not},
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

    /// Stream fed from other threads.
    #[derive(Default)]
    struct Feed {
        ready: std::sync::atomic::AtomicUsize,
        waker: std::sync::Mutex<Option<Waker>>,
    }

    impl Feed {
        fn send(&self, by_ref: bool) {
            self.ready.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if by_ref {
                if let Some(waker) = &*self.waker.lock().unwrap() {
                    waker.wake_by_ref();
                }
            } else {
                let waker = self.waker.lock().unwrap().take();
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
        }
    }

    struct FeedStream(Arc<Feed>);

    impl futures::Stream for FeedStream {
        type Item = ();

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            use std::sync::atomic::Ordering;

            let take = |feed: &Feed| {
                #[allow(deprecated)] // AtomicUsize::fetch_update deprecated since 1.100.0
                feed.ready
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
            };
            if take(&self.0) {
                return Poll::Ready(Some(()));
            }
            *self.0.waker.lock().unwrap() = Some(cx.waker().clone());
            if take(&self.0) {
                Poll::Ready(Some(()))
            } else {
                Poll::Pending
            }
        }
    }

    struct ThreadWaker(std::thread::Thread, std::sync::atomic::AtomicBool);

    impl std::task::Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.1.store(true, std::sync::atomic::Ordering::SeqCst);
            self.0.unpark();
        }
    }

    /// Tasks are woken from other threads while the selector goes to sleep and wakes up.
    /// If a wakeup is lost, this test hangs.
    #[test]
    fn cross_thread_wakeups_are_not_lost() {
        use std::sync::atomic::Ordering;

        const SENDERS: usize = 3;
        const FEEDS_PER_SENDER: usize = 4;
        const ITEMS_PER_FEED: usize = if cfg!(miri) { 16 } else { 20_000 };

        let feeds = std::iter::repeat_with(|| Arc::new(Feed::default()))
            .take(SENDERS * FEEDS_PER_SENDER)
            .collect::<Vec<_>>();
        let mut selector = feeds
            .iter()
            .cloned()
            .map(FeedStream)
            .collect::<StreamSelector<_>>();
        let nodes = selector
            .iter()
            .map(|task| Arc::downgrade(task.id().get()))
            .collect::<Vec<_>>();

        let senders = feeds
            .chunks(FEEDS_PER_SENDER)
            .map(|feeds| {
                let feeds = feeds.to_vec();
                std::thread::spawn(move || {
                    for i in 0..ITEMS_PER_FEED {
                        for feed in &feeds {
                            feed.send(i % 2 == 0);
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        let thread_waker = Arc::new(ThreadWaker(std::thread::current(), true.into()));
        let waker = Waker::from(thread_waker.clone());
        let mut cx = Context::from_waker(&waker);
        let mut remaining = feeds.len() * ITEMS_PER_FEED;
        while remaining > 0 {
            while thread_waker.1.swap(false, Ordering::SeqCst).not() {
                std::thread::park();
            }
            while let Poll::Ready(item) = selector.poll_next_unpin(&mut cx) {
                item.unwrap();
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }

        senders
            .into_iter()
            .for_each(|sender| sender.join().unwrap());
        assert!(selector.poll_next_unpin(&mut cx).is_pending());
        drop(selector);
        drop(feeds);
        for node in nodes {
            assert!(node.upgrade().is_none(), "node leaked");
        }
    }

    /// Tasks are woken from other threads while they are removed from the selector,
    /// and while the selector is dropped. Nothing should leak (or crash).
    #[test]
    fn cross_thread_wakeups_race_with_removal_and_drop() {
        use std::sync::atomic::{AtomicBool, Ordering};

        const SENDERS: usize = 3;
        const FEEDS_PER_SENDER: usize = 4;
        const ROUNDS: usize = if cfg!(miri) { 4 } else { 300 };

        for round in 0..ROUNDS {
            let feeds = std::iter::repeat_with(|| Arc::new(Feed::default()))
                .take(SENDERS * FEEDS_PER_SENDER)
                .collect::<Vec<_>>();
            let mut selector = feeds
                .iter()
                .cloned()
                .map(FeedStream)
                .collect::<crate::StreamWithIdSelector<_>>();
            let ids = selector
                .iter()
                .map(|task| task.id().clone())
                .collect::<Vec<_>>();
            let nodes = ids
                .iter()
                .map(|id| Arc::downgrade(id.get()))
                .collect::<Vec<_>>();

            let stop = Arc::new(AtomicBool::new(false));
            let senders = feeds
                .chunks(FEEDS_PER_SENDER)
                .map(|feeds| {
                    let feeds = feeds.to_vec();
                    let stop = stop.clone();
                    std::thread::spawn(move || {
                        let mut i = 0_usize;
                        while stop.load(Ordering::Relaxed).not() {
                            for feed in &feeds {
                                feed.send(i % 2 == 0);
                            }
                            i += 1;
                        }
                    })
                })
                .collect::<Vec<_>>();

            let mut cx = Context::from_waker(Waker::noop());
            let poll_some = |selector: &mut crate::StreamWithIdSelector<FeedStream>,
                             cx: &mut Context<'_>| {
                for _ in 0..(50 + round) {
                    let _ = selector.poll_next_unpin(cx);
                }
            };

            poll_some(&mut selector, &mut cx);
            for id in ids.iter().step_by(2) {
                assert!(selector.remove(id).is_some());
            }
            poll_some(&mut selector, &mut cx);
            selector.wake_all();
            poll_some(&mut selector, &mut cx);
            if round % 2 == 0 {
                drop(selector);
            } else {
                drop(selector.into_iter().next());
            }

            stop.store(true, Ordering::Relaxed);
            senders
                .into_iter()
                .for_each(|sender| sender.join().unwrap());
            drop(ids);
            for feed in &feeds {
                assert_eq!(Arc::strong_count(feed), 1, "task leaked");
                feed.waker.lock().unwrap().take();
            }
            for node in nodes {
                assert!(node.upgrade().is_none(), "node leaked");
            }
        }
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
