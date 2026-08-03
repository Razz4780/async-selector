use std::{
    cell::UnsafeCell,
    mem::{ManuallyDrop, MaybeUninit},
    ops::Not,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicPtr, Ordering},
    },
    task::Waker,
};

use futures::task::AtomicWaker;

/// Receiver handle for a [`Queue`].
///
/// Since the queue is MPSC, there can be only one instance.
pub struct Receiver<T> {
    queue: Arc<Queue<T>>,
    stub_predecessor: *const Node<T>,
}

impl<T> Receiver<T> {
    pub fn queue(&self) -> &Arc<Queue<T>> {
        &self.queue
    }

    /// Registers the given [`Waker`] and returns a pointer to the last node in the queue.
    ///
    /// The [`Waker`] will receive a wakeup when the next node is fully enqueued.
    ///
    /// The returned pointer can be used as a snapshot marker.
    /// If the queue is empty, this method returns [`std::ptr::null`].
    pub fn register_waker(&mut self, waker: &Waker) -> *const Node<T> {
        self.queue.waker.register(waker);

        let last = self.queue.head.load(Ordering::Acquire);
        if std::ptr::eq(last, &*self.queue.stub).not() {
            return last;
        }

        let tail = unsafe {
            // SAFETY: `tail` is only used from the `Receiver`,
            // and we have a mutable reference.
            *self.queue.tail.get()
        };
        if std::ptr::eq(tail, &*self.queue.stub) {
            return std::ptr::null();
        }

        self.stub_predecessor
    }

    /// Dequeues the oldest node in the queue.
    ///
    /// Returns [`None`] if the queue is empty or the queue is short and there is an enqueue race.
    pub fn dequeue(&mut self) -> Option<Dequeued<T>> {
        let mut tail = unsafe {
            // SAFETY: `tail` is only used from the `Receiver`,
            // and we have a mutable reference.
            *self.queue.tail.get()
        };
        let mut next = unsafe { &*tail }.next_enqueued.load(Ordering::Acquire);

        if std::ptr::eq(tail, &*self.queue.stub) {
            if next.is_null() {
                return None;
            }
            unsafe { *self.queue.tail.get() = next };
            tail = next;
            next = unsafe { &*next }.next_enqueued.load(Ordering::Acquire);
        }

        if next.is_null().not() {
            unsafe { *self.queue.tail.get() = next };
            debug_assert_ne!(
                tail,
                std::ptr::from_ref(&*self.queue.stub),
                "popping stub, this should never happen",
            );
            let node = unsafe { Arc::from_raw(tail) };
            return Some(Dequeued(node));
        }

        if std::ptr::eq(tail, self.queue.head.load(Ordering::Acquire)).not() {
            // Enqueue is in progress.
            return None;
        }

        // Enqueue the stub.
        self.queue
            .stub
            .next_enqueued
            .store(std::ptr::null_mut(), Ordering::Relaxed);
        let stub = std::ptr::from_ref(&*self.queue.stub).cast_mut();
        let stub_predecessor = self.queue.head.swap(stub, Ordering::AcqRel);
        unsafe { &*stub_predecessor }
            .next_enqueued
            .store(stub, Ordering::Release);
        self.stub_predecessor = stub_predecessor;

        next = unsafe { &*tail }.next_enqueued.load(Ordering::Acquire);
        if next.is_null() {
            // Enqueue is in progress.
            None
        } else {
            unsafe { *self.queue.tail.get() = next };
            debug_assert_ne!(
                tail,
                std::ptr::from_ref(&*self.queue.stub),
                "popping stub, this should never happen",
            );
            let node = unsafe { Arc::from_raw(tail) };
            Some(Dequeued(node))
        }
    }
}

impl<T> Default for Receiver<T> {
    fn default() -> Self {
        let queue = Arc::new(Queue {
            waker: Default::default(),
            head: Default::default(),
            tail: UnsafeCell::new(std::ptr::null()),
            stub: ManuallyDrop::new(Node {
                queue: Weak::new(),
                is_enqueued: Default::default(),
                next_enqueued: Default::default(),
                value: MaybeUninit::uninit(),
            }),
        });

        let stub_ptr = std::ptr::from_ref(&*queue.stub).cast_mut();
        queue.head.store(stub_ptr, Ordering::Release);
        unsafe {
            *queue.tail.get() = stub_ptr;
        }

        Self {
            queue,
            stub_predecessor: std::ptr::null(),
        }
    }
}

unsafe impl<T: Send> Send for Receiver<T> {}

unsafe impl<T: Send> Sync for Receiver<T> {}

/// Vyukov-style intrusive MPSC queue.
pub struct Queue<T> {
    waker: AtomicWaker,
    /// Always points to a valid node.
    head: AtomicPtr<Node<T>>,
    /// Always points to a valid node.
    tail: UnsafeCell<*const Node<T>>,
    /// Used to ensure that the queue always has at least one node.
    stub: ManuallyDrop<Node<T>>,
}

impl<T> Queue<T> {
    /// Creates a new [`Node`] bound to this queue.
    ///
    /// The node is not enqueued.
    pub fn create(self: &Arc<Self>, value: T) -> Arc<Node<T>> {
        Arc::new(Node {
            queue: Arc::downgrade(self),
            is_enqueued: AtomicBool::new(false),
            next_enqueued: Default::default(),
            value: MaybeUninit::new(value),
        })
    }

    /// Enqueues the given [`Node`], unless it is already in the queue.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the [`Node`] belongs to this queue instance.
    pub unsafe fn enqueue(self: &Arc<Self>, node: Arc<Node<T>>) {
        if node.is_enqueued.swap(true, Ordering::AcqRel) {
            return;
        }
        unsafe { self.enqueue_inner(node) };
    }

    /// Enqueues the given [`Node`].
    ///
    /// # Safety
    ///
    /// Caller must ensure that the [`Node`]:
    /// 1. belongs to this queue instance
    /// 2. [`Node::is_enqueued`] lock was acquired
    unsafe fn enqueue_inner(self: &Arc<Self>, node: Arc<Node<T>>) {
        debug_assert!(node.is_enqueued.load(Ordering::Relaxed));
        debug_assert_eq!(Arc::as_ptr(self), Weak::as_ptr(node.queue()));

        node.next_enqueued
            .store(std::ptr::null_mut(), Ordering::Relaxed);
        let node = Arc::into_raw(node).cast_mut();
        let head = self.head.swap(node, Ordering::AcqRel);
        let head = unsafe { &*head };
        head.next_enqueued.store(node, Ordering::Release);
        self.waker.wake();
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        struct Guard<T> {
            stub: *const Node<T>,
            tail: *const Node<T>,
        }

        impl<T> Drop for Guard<T> {
            fn drop(&mut self) {
                let mut inner = Self {
                    stub: self.stub,
                    tail: self.tail,
                };

                while inner.tail.is_null().not() {
                    // `tail` may contain an old pointer tag for the embedded
                    // stub. Use the fresh pointer created inside Queue::drop.
                    // This makes Miri happy.
                    if std::ptr::eq(inner.tail, inner.stub) {
                        inner.tail = unsafe { &*inner.stub }
                            .next_enqueued
                            .load(Ordering::Acquire);
                        continue;
                    }

                    let next = unsafe { &*inner.tail }
                        .next_enqueued
                        .load(Ordering::Acquire);
                    let to_drop = inner.tail;
                    inner.tail = next;

                    let _ = unsafe { Arc::from_raw(to_drop) };
                }

                let _ = ManuallyDrop::new(inner);
            }
        }

        drop(Guard {
            stub: std::ptr::from_ref(&*self.stub),
            tail: *self.tail.get_mut(),
        });

        unsafe {
            let stub = ManuallyDrop::take(&mut self.stub);
            stub.drop_ignore_value();
        }
    }
}

unsafe impl<T: Send> Send for Queue<T> {}

unsafe impl<T: Send> Sync for Queue<T> {}

/// Node of a [`Queue`].
///
/// Always belongs to a single [`Queue`].
pub struct Node<T> {
    queue: Weak<Queue<T>>,
    is_enqueued: AtomicBool,
    next_enqueued: AtomicPtr<Self>,
    /// Always initialized, except for the [`Queue::stub`] node.
    value: MaybeUninit<T>,
}

impl<T> Node<T> {
    /// Returns a weak reference to the queue instance to which this node belongs.
    pub fn queue(&self) -> &Weak<Queue<T>> {
        &self.queue
    }

    /// Enqueues this node into its parent queue.
    ///
    /// If the node is already in the queue, does nothing.
    pub fn enqueue(self: Arc<Self>) {
        if self.is_enqueued.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(queue) = self.queue.upgrade() {
            unsafe { queue.enqueue_inner(self) };
        }
    }

    /// Enqueues this node into its parent queue.
    ///
    /// If the node is already in the queue, does nothing.
    pub fn enqueue_by_ref(self: &Arc<Self>) {
        if self.is_enqueued.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(queue) = self.queue.upgrade() {
            unsafe { queue.enqueue_inner(self.clone()) };
        }
    }

    pub fn value(&self) -> &T {
        unsafe { self.value.assume_init_ref() }
    }

    /// Drops all fields of the node, ignoring [`Self::value`].
    ///
    /// This is used when dropping [`Queue::stub`].
    fn drop_ignore_value(self) {
        let mut this = ManuallyDrop::new(self);
        let Self {
            queue,
            is_enqueued,
            next_enqueued,
            value: _value,
        } = &mut *this;
        unsafe {
            std::ptr::drop_in_place(queue);
            std::ptr::drop_in_place(is_enqueued);
            std::ptr::drop_in_place(next_enqueued);
        }
    }
}

impl<T> Drop for Node<T> {
    fn drop(&mut self) {
        unsafe {
            self.value.assume_init_drop();
        }
    }
}

/// [`Node`] dequeued from a [`Receiver`].
///
/// Until the [`Node`] is recovered with [`Dequeued::into_inner`],
/// it cannot be enqueued again.
pub struct Dequeued<T>(Arc<Node<T>>);

impl<T> Dequeued<T> {
    pub fn get(&self) -> &Arc<Node<T>> {
        &self.0
    }

    /// Recovers the [`Node`], marking it as enqueueable.
    pub fn into_inner(self) -> Arc<Node<T>> {
        self.0.is_enqueued.store(false, Ordering::Release);
        self.0
    }
}

#[cfg(test)]
mod test {
    use std::{
        panic::AssertUnwindSafe,
        sync::{Arc, Barrier, atomic::Ordering},
        task::{Poll, Waker},
    };
    use tokio::sync::Barrier as TokioBarrier;

    use crate::queue::Receiver;

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn snapshots_work() {
        const SENDERS: usize = 4;
        const ITERATIONS: usize = if cfg!(miri) { 32 } else { 16 * 1024 };

        let mut queue = Receiver::<usize>::default();
        let barrier = Arc::new(TokioBarrier::new(SENDERS + 1));

        for i in 0..SENDERS {
            let queue = queue.queue().clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                let node = queue.create(i);
                for _ in 0..ITERATIONS {
                    while node.is_enqueued.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    node.enqueue_by_ref();
                }
            });
        }

        let mut counts = [0; SENDERS];
        let mut remaining = SENDERS * ITERATIONS;
        barrier.wait().await;

        while remaining > 0 {
            futures::future::poll_fn(|cx| {
                let marker = queue.register_waker(cx.waker());
                if marker.is_null() {
                    return Poll::Pending;
                }

                while let Some(node) = queue.dequeue() {
                    let is_last = Arc::as_ptr(node.get()) == marker;
                    remaining -= 1;
                    counts[*node.get().value()] += 1;
                    node.into_inner();
                    if is_last {
                        return Poll::Ready(());
                    }
                }

                Poll::Ready(())
            })
            .await;
        }

        for count in counts {
            assert_eq!(count, ITERATIONS);
        }
    }

    #[test]
    fn concurrent_enqueues_work() {
        const SENDERS: usize = 4;
        const ITERATIONS: usize = if cfg!(miri) { 32 } else { 16 * 1024 };

        let mut queue = Receiver::<usize>::default();
        let barrier = Arc::new(Barrier::new(SENDERS + 1));

        for i in 0..SENDERS {
            let queue = queue.queue().clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let node = queue.create(i);
                for _ in 0..ITERATIONS {
                    while node.is_enqueued.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    node.enqueue_by_ref();
                }
            });
        }

        let mut counts = [0; SENDERS];
        let mut remaining = SENDERS * ITERATIONS;
        barrier.wait();

        while remaining > 0 {
            let node = loop {
                if let Some(node) = queue.dequeue() {
                    break node.into_inner();
                } else {
                    std::hint::spin_loop();
                }
            };
            remaining -= 1;
            counts[*node.value()] += 1;
        }

        for _ in 0..100 {
            assert!(queue.dequeue().is_none());
        }
        for count in counts {
            assert_eq!(count, ITERATIONS);
        }
    }

    #[test]
    fn dropped_queue_drops_nodes() {
        let queue = Receiver::<()>::default();
        let nodes = std::iter::repeat_with(|| queue.queue().create(()))
            .take(10)
            .map(|strong| {
                let weak = Arc::downgrade(&strong);
                strong.enqueue();
                weak
            })
            .collect::<Vec<_>>();
        drop(queue);
        for node in nodes {
            assert!(node.upgrade().is_none());
        }
    }

    #[test]
    fn dropped_queue_drops_remaining_nodes_after_panic() {
        struct Node(bool);

        impl Drop for Node {
            fn drop(&mut self) {
                if self.0 {
                    panic!("test panic")
                }
            }
        }

        let queue = Receiver::<Node>::default();
        let nodes = std::iter::repeat_n(false, 10)
            .chain(std::iter::once(true))
            .chain(std::iter::repeat_n(false, 10))
            .map(|should_panic| queue.queue().create(Node(should_panic)))
            .map(|strong| {
                let weak = Arc::downgrade(&strong);
                strong.enqueue();
                weak
            })
            .collect::<Vec<_>>();

        let queue = AssertUnwindSafe(queue);
        std::panic::catch_unwind(|| drop(queue)).expect_err("should panic when dropped");
        for node in nodes {
            assert!(node.upgrade().is_none());
        }
    }

    #[test]
    fn queue_returns_correct_tail_marker() {
        let mut queue = Receiver::<()>::default();
        assert!(queue.register_waker(Waker::noop()).is_null());
        let node = queue.queue().create(());
        node.enqueue_by_ref();
        assert_eq!(queue.register_waker(Waker::noop()), Arc::as_ptr(&node));
        let _ = queue.dequeue().unwrap();
        assert!(queue.register_waker(Waker::noop()).is_null());
        let node_1 = queue.queue().create(());
        node_1.enqueue_by_ref();
        assert_eq!(queue.register_waker(Waker::noop()), Arc::as_ptr(&node_1));
    }
}
