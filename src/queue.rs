use std::{
    cell::UnsafeCell,
    mem::{ManuallyDrop, MaybeUninit},
    ops::Not,
    sync::{Arc, Weak},
    task::Waker,
};

#[cfg(loom)]
use loom::sync::atomic::{AtomicPtr, AtomicU8, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicPtr, AtomicU8, Ordering};

use crate::queue::waker_slot::WakerSlot;

mod waker_slot;

/// [`Node::state`] bit: the node is in the queue (or an enqueue is in progress).
///
/// Whoever sets this bit owns the right to enqueue the node.
/// Only the [`Receiver`] clears it (when the node is dequeued).
const ENQUEUED: u8 = 0b01;
/// [`Node::state`] bit: the owner of the node gave it up (see [`Node::retire`]).
///
/// Always set together with [`ENQUEUED`], and never cleared.
/// This means that a retired node can never be enqueued again.
const RETIRED: u8 = 0b10;

/// Receiver handle for a [`Queue`].
///
/// Since the queue is MPSC, there can be only one instance.
///
/// # Ownership of enqueued nodes
///
/// The queue stores raw pointers and does **not** own a reference to a node
/// for as long as the node is owned by somebody else (the "owner", in practice
/// the [`List`](crate::list::List)).
/// This saves a reference count round trip on every wakeup.
///
/// Before the owner gives the node up, it must call [`Node::retire`].
/// If the node is enqueued at that point, the queue receives its own reference,
/// which is released when the node is dequeued (or when the queue is dropped).
///
/// The receiver must be dropped only after all nodes created from its queue were retired.
pub struct Receiver<T> {
    queue: Arc<Queue<T>>,
    /// Oldest node in the queue. Always points to a valid node.
    ///
    /// Lives here (and not in the shared [`Queue`]), because only the receiver uses it.
    /// This way dequeues do not invalidate the cache line that the senders write to.
    tail: *const Node<T>,
    stub_predecessor: *const Node<T>,
    /// Private front of the queue: if not null, this node is dequeued before all others.
    ///
    /// Used by [`Receiver::requeue`] when the shared queue is empty.
    /// Saves the two swaps that it takes to push a node through an empty queue
    /// (one to enqueue the node, one to enqueue the stub behind it when the node is dequeued).
    front: *const Node<T>,
}

impl<T> Receiver<T> {
    pub fn queue(&self) -> &Arc<Queue<T>> {
        &self.queue
    }

    fn stub(&self) -> *const Node<T> {
        std::ptr::from_ref(&*self.queue.stub)
    }

    /// Returns a pointer to the last node in the queue.
    ///
    /// The returned pointer can be used as a snapshot marker, it must not be dereferenced.
    /// If the queue is empty, this method returns [`std::ptr::null`].
    pub fn marker(&self) -> *const Node<T> {
        let last = self.queue.head.load(Ordering::Acquire);
        if std::ptr::eq(last, self.stub()).not() {
            return last;
        }

        if std::ptr::eq(self.tail, self.stub()) {
            // Shared queue is empty, `front` is the only node (or null).
            return self.front;
        }

        self.stub_predecessor
    }

    /// Returns whether the queue may contain a node, or a node may be in the middle of an enqueue.
    /// If so, the receiver should be polled again right away.
    ///
    /// `false` is a guarantee: there is no node in the queue,
    /// and every enqueue that started after this check will observe
    /// a waker registered **before** this check ([`Receiver::park`] relies on this:
    /// it calls this method again after it registers the waker).
    pub fn has_pending(&self) -> bool {
        self.front.is_null().not()
            || std::ptr::eq(self.tail, self.stub()).not()
            || std::ptr::eq(self.queue.head.load(Ordering::SeqCst), self.stub()).not()
    }

    /// Registers the given [`Waker`] to be woken when the next node is enqueued.
    ///
    /// Returns `false` if the registration could not be trusted
    /// (a node was enqueued concurrently, or a wakeup is being delivered right now).
    /// In this case, the caller should wake the waker on its own.
    ///
    /// Only call this when the queue looks empty, this is the slow path.
    pub fn park(&mut self, waker: &Waker) -> bool {
        if self.queue.waker.register(waker).not() {
            return false;
        }
        if self.has_pending() {
            // The caller is going to wake the waker on its own.
            // Take it back, so that the next enqueue does not wake it for the second time.
            self.queue.waker.disarm();
            return false;
        }
        true
    }

    /// Dequeues the oldest node in the queue.
    ///
    /// Returns [`None`] if the queue is empty or the queue is short and there is an enqueue race.
    pub fn dequeue(&mut self) -> Option<Dequeued<T>> {
        if self.front.is_null().not() {
            let front = std::mem::replace(&mut self.front, std::ptr::null());
            return Some(Dequeued(front));
        }

        let stub = self.stub();
        let mut tail = self.tail;
        let mut next = unsafe { &*tail }.next_enqueued.load(Ordering::Acquire);

        if std::ptr::eq(tail, stub) {
            if next.is_null() {
                return None;
            }
            self.tail = next;
            tail = next;
            next = unsafe { &*next }.next_enqueued.load(Ordering::Acquire);
        }

        if next.is_null().not() {
            self.tail = next;
            debug_assert_ne!(tail, stub, "popping stub, this should never happen");
            return Some(Dequeued(tail));
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
        let stub_predecessor = self.queue.head.swap(stub.cast_mut(), Ordering::SeqCst);
        unsafe { &*stub_predecessor }
            .next_enqueued
            .store(stub.cast_mut(), Ordering::Release);
        self.stub_predecessor = stub_predecessor;

        next = unsafe { &*tail }.next_enqueued.load(Ordering::Acquire);
        if next.is_null() {
            // Enqueue is in progress.
            None
        } else {
            self.tail = next;
            debug_assert_ne!(tail, stub, "popping stub, this should never happen");
            Some(Dequeued(tail))
        }
    }

    /// Enqueues the given [`Node`] again, unless it is already in the queue.
    ///
    /// Meant to be called by the receiver itself,
    /// so it skips everything that only matters to a foreign sender:
    /// it does not need to check whether the queue is still alive,
    /// and it does not wake the receiver.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the [`Node`] belongs to this queue instance and was not retired.
    pub unsafe fn requeue(&mut self, node: &Arc<Node<T>>) {
        if node.state.fetch_or(ENQUEUED, Ordering::AcqRel) & ENQUEUED != 0 {
            return;
        }
        // If the queue is empty, the node would be the first one to be dequeued anyway.
        // (A sender may be racing with this check. In this case its node ends up behind ours,
        // which is one of the orders that could have happened without this shortcut.)
        if self.front.is_null()
            && std::ptr::eq(self.tail, self.stub())
            && std::ptr::eq(self.queue.head.load(Ordering::Acquire), self.stub())
        {
            self.front = Arc::as_ptr(node);
        } else {
            unsafe { self.queue.enqueue_inner(Arc::as_ptr(node)) };
        }
    }
}

impl<T> Default for Receiver<T> {
    fn default() -> Self {
        let queue = Arc::new(Queue {
            head: Default::default(),
            waker: Default::default(),
            tail_on_drop: UnsafeCell::new(std::ptr::null()),
            #[cfg(loom)]
            arc_sync: loom::sync::atomic::AtomicUsize::new(0),
            stub: ManuallyDrop::new(Node {
                queue: Weak::new(),
                state: AtomicU8::new(0),
                next_enqueued: Default::default(),
                value: MaybeUninit::uninit(),
            }),
        });

        let stub_ptr = std::ptr::from_ref(&*queue.stub).cast_mut();
        queue.head.store(stub_ptr, Ordering::Release);

        Self {
            queue,
            tail: stub_ptr,
            stub_predecessor: std::ptr::null(),
            front: std::ptr::null(),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if self.front.is_null().not() {
            // Same rule as for the nodes in the shared queue:
            // all nodes are retired by now, and we own a reference to this one.
            debug_assert!(
                unsafe { &*self.front }.state.load(Ordering::Relaxed) & RETIRED != 0,
                "receiver dropped before a node was retired",
            );
            let _ = unsafe { Arc::from_raw(self.front) };
        }

        // Hand the tail over, the queue needs it to release the remaining nodes.
        // The queue may be dropped later than the receiver,
        // if some sender is in the middle of an enqueue.
        unsafe {
            // SAFETY: only the receiver and `Queue::drop` access this field.
            *self.queue.tail_on_drop.get() = self.tail;
        }
        #[cfg(loom)]
        self.queue.arc_sync.fetch_add(1, Ordering::AcqRel);
    }
}

unsafe impl<T: Send> Send for Receiver<T> {}

unsafe impl<T: Send> Sync for Receiver<T> {}

/// Vyukov-style intrusive MPSC queue.
pub struct Queue<T> {
    /// Always points to a valid node.
    head: AtomicPtr<Node<T>>,
    /// Waker of the receiver, if the receiver is parked.
    waker: WakerSlot,
    /// Set when the [`Receiver`] is dropped.
    tail_on_drop: UnsafeCell<*const Node<T>>,
    /// Loom does not see the synchronization provided by the reference count of [`Arc`]
    /// (it is not a Loom type): everything done by a thread that released
    /// its reference happens-before [`Queue::drop`].
    /// This field replays the reference count operations for Loom.
    #[cfg(loom)]
    arc_sync: loom::sync::atomic::AtomicUsize,
    /// Used to ensure that the queue always has at least one node.
    stub: ManuallyDrop<Node<T>>,
}

impl<T> Queue<T> {
    /// Creates a new [`Node`] bound to this queue.
    ///
    /// The node is marked as enqueued, but it is **not** in the queue yet.
    /// The caller is expected to follow up with [`Queue::enqueue_created`].
    pub fn create_enqueued(self: &Arc<Self>, value: T) -> Arc<Node<T>> {
        Arc::new(Node {
            queue: Arc::downgrade(self),
            state: AtomicU8::new(ENQUEUED),
            next_enqueued: Default::default(),
            value: MaybeUninit::new(value),
        })
    }

    /// Creates a new [`Node`] bound to this queue.
    ///
    /// The node is not enqueued.
    #[cfg(test)]
    pub fn create(self: &Arc<Self>, value: T) -> Arc<Node<T>> {
        let node = self.create_enqueued(value);
        node.state.store(0, Ordering::Release);
        node
    }

    /// Enqueues a [`Node`] obtained from [`Queue::create_enqueued`].
    ///
    /// # Safety
    ///
    /// Caller must ensure that the [`Node`]:
    /// 1. was created with [`Queue::create_enqueued`] called on this queue instance
    /// 2. was not passed to this function before
    pub unsafe fn enqueue_created(&self, node: &Arc<Node<T>>) {
        unsafe { self.enqueue_inner(Arc::as_ptr(node)) };
        self.waker.notify();
    }

    /// Wakes the receiver, if it is parked.
    pub fn notify(&self) {
        self.waker.notify();
    }

    /// Enqueues the given [`Node`], unless it is already in the queue.
    ///
    /// Does not wake the receiver, see [`Queue::notify`].
    ///
    /// # Safety
    ///
    /// Caller must ensure that the [`Node`] belongs to this queue instance.
    pub unsafe fn enqueue_silent(&self, node: &Arc<Node<T>>) {
        if node.state.fetch_or(ENQUEUED, Ordering::AcqRel) & ENQUEUED != 0 {
            return;
        }
        unsafe { self.enqueue_inner(Arc::as_ptr(node)) };
    }

    /// Links the given [`Node`] into the queue.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the [`Node`]:
    /// 1. belongs to this queue instance
    /// 2. has the [`ENQUEUED`] bit set, and the caller is the one who set it
    unsafe fn enqueue_inner(&self, node: *const Node<T>) {
        let node = node.cast_mut();
        unsafe {
            debug_assert!((*node).state.load(Ordering::Relaxed) & ENQUEUED != 0);
            debug_assert_eq!(std::ptr::from_ref(self), Weak::as_ptr(&(*node).queue));
            (*node)
                .next_enqueued
                .store(std::ptr::null_mut(), Ordering::Relaxed);
        }
        // `SeqCst` is required here. See `WakerSlot`.
        let head = self.head.swap(node, Ordering::SeqCst);
        // SAFETY: the previous head cannot leave the queue before its `next_enqueued` is set.
        let head = unsafe { &*head };
        head.next_enqueued.store(node, Ordering::Release);
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        #[cfg(loom)]
        self.arc_sync.fetch_add(1, Ordering::AcqRel);

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

                    // The receiver is gone, so all nodes were retired.
                    // Retired nodes found in the queue are owned by the queue.
                    debug_assert!(
                        unsafe { &*to_drop }.state.load(Ordering::Relaxed) & RETIRED != 0,
                        "receiver dropped before a node was retired",
                    );
                    let _ = unsafe { Arc::from_raw(to_drop) };
                }

                let _ = ManuallyDrop::new(inner);
            }
        }

        drop(Guard {
            stub: std::ptr::from_ref(&*self.stub),
            tail: *self.tail_on_drop.get_mut(),
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
    /// Combination of [`ENQUEUED`] and [`RETIRED`].
    state: AtomicU8,
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
        self.enqueue_by_ref();
    }

    /// Enqueues this node into its parent queue.
    ///
    /// If the node is already in the queue, does nothing.
    pub fn enqueue_by_ref(self: &Arc<Self>) {
        if self.state.fetch_or(ENQUEUED, Ordering::AcqRel) & ENQUEUED != 0 {
            return;
        }
        // Loom only switches threads on operations it knows about, and `Weak::upgrade`
        // is not one of them. Without this load, Loom would never run
        // the receiver between the `fetch_or` above and the upgrade below.
        #[cfg(loom)]
        let _ = self.state.load(Ordering::Relaxed);
        match self.queue.upgrade() {
            Some(queue) => {
                unsafe { queue.enqueue_inner(Arc::as_ptr(self)) };
                queue.waker.notify();
                #[cfg(loom)]
                queue.arc_sync.fetch_add(1, Ordering::AcqRel);
            },
            None => {
                // The receiver is gone, so this node was retired,
                // and that happened after we set the `ENQUEUED` bit
                // (otherwise we would have seen the bit set by `Node::retire`).
                // The queue was given a reference to this node,
                // but the node will never make it into the queue.
                // Release that reference here.
                //
                // The order of this decrement and the increment in `Node::retire`
                // does not matter: we hold our own reference to the node.
                unsafe { Arc::decrement_strong_count(Arc::as_ptr(self)) };
            },
        }
    }

    /// Marks this node as given up by its owner.
    ///
    /// After this call, the node will never be enqueued again.
    /// If the node is in the queue right now, the queue receives its own reference to the node.
    ///
    /// # Safety
    ///
    /// Caller must ensure that:
    /// 1. this method is called at most once for each node
    /// 2. this call is synchronized with all uses of the [`Receiver`]
    ///    (in practice: the caller has unique access to the receiver's owner)
    pub unsafe fn retire(self: &Arc<Self>) {
        let prev = self.state.fetch_or(ENQUEUED | RETIRED, Ordering::AcqRel);
        debug_assert!(prev & RETIRED == 0, "node retired twice");
        if prev & ENQUEUED != 0 {
            std::mem::forget(self.clone());
        }
    }

    pub fn value(&self) -> &T {
        unsafe { self.value.assume_init_ref() }
    }

    #[cfg(test)]
    fn is_enqueued(&self) -> bool {
        self.state.load(Ordering::Acquire) & ENQUEUED != 0
    }

    /// Drops all fields of the node, ignoring [`Self::value`].
    ///
    /// This is used when dropping [`Queue::stub`].
    fn drop_ignore_value(self) {
        let mut this = ManuallyDrop::new(self);
        let Self {
            queue,
            state,
            next_enqueued,
            value: _value,
        } = &mut *this;
        unsafe {
            std::ptr::drop_in_place(queue);
            std::ptr::drop_in_place(state);
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
/// Until the [`Node`] is recovered with [`Dequeued::into_live`],
/// it cannot be enqueued again.
#[must_use = "a dequeued node must be recovered with `into_live`"]
pub struct Dequeued<T>(*const Node<T>);

impl<T> Dequeued<T> {
    pub fn as_ptr(&self) -> *const Node<T> {
        self.0
    }

    /// Recovers the [`Node`], marking it as enqueueable.
    ///
    /// Returns [`None`] if the node was retired.
    /// In this case, the reference owned by the queue is released.
    ///
    /// The returned handle does not own a reference to the node.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the owner of the node keeps it alive
    /// for as long as the returned handle is used (unless the node is retired first).
    pub unsafe fn into_live(self) -> Option<ManuallyDrop<Arc<Node<T>>>> {
        let node = unsafe { Arc::from_raw(self.0) };
        // `RETIRED` is only set under the synchronization required by `Node::retire`,
        // so a plain load is enough to see it.
        if node.state.load(Ordering::Relaxed) & RETIRED != 0 {
            drop(node);
            return None;
        }
        // This must be a read-modify-write, a plain store is not enough.
        // A sender that wants to wake this node publishes its data first,
        // and then does `fetch_or(ENQUEUED)`. Both operations modify the same atomic, so:
        // * either the sender goes first, sees `ENQUEUED` and backs off. Then this swap
        //   reads what the sender wrote, which makes the sender's data visible to the caller
        //   (who is about to poll the task),
        // * or this swap goes first, and the sender enqueues the node again.
        // With a plain store there is a third outcome: the sender backs off,
        // and the poll does not see its data (the store may still sit in the store buffer
        // of this thread when the sender looks at the state).
        //
        // Do not clear `RETIRED` here. The check above makes sure that it is not set.
        let prev = node.state.swap(0, Ordering::AcqRel);
        debug_assert_eq!(prev, ENQUEUED);
        Some(ManuallyDrop::new(node))
    }
}

#[cfg(test)]
mod test {
    use std::{
        panic::AssertUnwindSafe,
        sync::{Arc, Barrier},
        task::{Poll, Wake, Waker},
    };
    use tokio::sync::Barrier as TokioBarrier;

    use crate::queue::Receiver;

    #[cfg(loom)]
    #[test]
    fn loom_concurrent_enqueues_work() {
        loom::model(|| {
            let mut receiver = Receiver::default();
            let queue = receiver.queue().clone();
            let first = queue.create(1);
            let second = queue.create(2);

            let first_sender = {
                let first = first.clone();
                loom::thread::spawn(move || first.enqueue())
            };
            let second_sender = {
                let second = second.clone();
                loom::thread::spawn(move || second.enqueue())
            };

            let mut values = Vec::new();
            while values.len() < 2 {
                let Some(node) = receiver.dequeue() else {
                    loom::thread::yield_now();
                    continue;
                };
                values.push(*unsafe { node.into_live() }.unwrap().value());
            }

            first_sender.join().unwrap();
            second_sender.join().unwrap();

            assert!(receiver.dequeue().is_none());

            values.sort_unstable();
            assert_eq!(values, [1, 2]);

            unsafe {
                first.retire();
                second.retire();
            }
        });
    }

    /// The receiver goes to sleep while a sender enqueues a node.
    /// Either the receiver sees the node, or the sender wakes the receiver.
    #[cfg(loom)]
    #[test]
    fn loom_no_lost_wakeup() {
        use loom::sync::atomic::{AtomicBool, Ordering};

        struct Flag(AtomicBool);

        impl Wake for Flag {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        loom::model(|| {
            let mut receiver = Receiver::default();
            let node = receiver.queue().create(());
            let flag = Arc::new(Flag(AtomicBool::new(false)));
            let waker = Waker::from(flag.clone());

            let sender = {
                let node = node.clone();
                loom::thread::spawn(move || node.enqueue())
            };

            let dequeued = match receiver.dequeue() {
                Some(dequeued) => dequeued,
                None => {
                    let parked = receiver.park(&waker);
                    sender.join().unwrap();
                    if parked {
                        assert!(flag.0.load(Ordering::SeqCst), "lost wakeup");
                    }
                    receiver.dequeue().expect("node should be in the queue")
                },
            };
            assert!(unsafe { dequeued.into_live() }.is_some());

            unsafe { node.retire() };
        });
    }

    /// A sender publishes data and wakes a node that is already dequeued, but not polled yet.
    /// Either the poll sees the data, or the node is enqueued again.
    #[cfg(loom)]
    #[test]
    fn loom_wake_between_dequeue_and_poll_is_not_lost() {
        use loom::sync::atomic::{AtomicBool, Ordering};

        loom::model(|| {
            let mut receiver = Receiver::default();
            let node = receiver.queue().create(());
            let data = Arc::new(AtomicBool::new(false));
            node.enqueue_by_ref();
            let dequeued = receiver.dequeue().unwrap();

            let sender = {
                let node = node.clone();
                let data = data.clone();
                loom::thread::spawn(move || {
                    data.store(true, Ordering::Release);
                    node.enqueue();
                })
            };

            // This is what the selector does before it polls the task.
            assert!(unsafe { dequeued.into_live() }.is_some());
            let seen = data.load(Ordering::Acquire);

            sender.join().unwrap();
            if !seen {
                let again = receiver.dequeue().expect("lost wakeup");
                assert!(unsafe { again.into_live() }.is_some());
            }

            unsafe { node.retire() };
        });
    }

    /// The owner retires the node and drops the receiver while a sender enqueues the node.
    /// The node must be freed, no matter who wins.
    #[cfg(loom)]
    #[test]
    fn loom_retire_races_with_enqueue() {
        loom::model(|| {
            let receiver = Receiver::default();
            let node = receiver.queue().create(());
            let weak = Arc::downgrade(&node);

            let sender = {
                let node = node.clone();
                loom::thread::spawn(move || node.enqueue())
            };

            unsafe { node.retire() };
            drop(node);
            drop(receiver);
            sender.join().unwrap();

            assert!(weak.upgrade().is_none(), "node leaked");
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn snapshots_work() {
        const SENDERS: usize = 4;
        const ITERATIONS: usize = if cfg!(miri) { 32 } else { 16 * 1024 };

        let mut queue = Receiver::<usize>::default();
        let barrier = Arc::new(TokioBarrier::new(SENDERS + 1));
        let nodes = (0..SENDERS)
            .map(|i| queue.queue().create(i))
            .collect::<Vec<_>>();

        for node in &nodes {
            let node = node.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                for _ in 0..ITERATIONS {
                    while node.is_enqueued() {
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
                let marker = queue.marker();
                if marker.is_null() {
                    if queue.park(cx.waker()) {
                        return Poll::Pending;
                    }
                    cx.waker().wake_by_ref();
                    return Poll::Ready(());
                }

                while let Some(node) = queue.dequeue() {
                    let is_last = node.as_ptr() == marker;
                    remaining -= 1;
                    let node = unsafe { node.into_live() }.unwrap();
                    counts[*node.value()] += 1;
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
        for node in &nodes {
            unsafe { node.retire() };
        }
    }

    #[test]
    fn concurrent_enqueues_work() {
        const SENDERS: usize = 4;
        const ITERATIONS: usize = if cfg!(miri) { 32 } else { 16 * 1024 };

        let mut queue = Receiver::<usize>::default();
        let barrier = Arc::new(Barrier::new(SENDERS + 1));
        let nodes = (0..SENDERS)
            .map(|i| queue.queue().create(i))
            .collect::<Vec<_>>();

        let handles = nodes
            .iter()
            .cloned()
            .map(|node| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..ITERATIONS {
                        while node.is_enqueued() {
                            std::hint::spin_loop();
                        }
                        node.enqueue_by_ref();
                    }
                })
            })
            .collect::<Vec<_>>();

        let mut counts = [0; SENDERS];
        let mut remaining = SENDERS * ITERATIONS;
        barrier.wait();

        while remaining > 0 {
            let node = loop {
                if let Some(node) = queue.dequeue() {
                    break unsafe { node.into_live() }.unwrap();
                } else {
                    std::hint::spin_loop();
                }
            };
            remaining -= 1;
            counts[*node.value()] += 1;
        }

        for handle in handles {
            handle.join().unwrap();
        }
        for _ in 0..100 {
            assert!(queue.dequeue().is_none());
        }
        for count in counts {
            assert_eq!(count, ITERATIONS);
        }
        for node in &nodes {
            unsafe { node.retire() };
        }
    }

    #[test]
    fn parked_receiver_is_woken_once() {
        struct Counter(std::sync::atomic::AtomicUsize);

        impl Wake for Counter {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let counter = Arc::new(Counter(Default::default()));
        let waker = Waker::from(counter.clone());
        let wakeups = || counter.0.load(std::sync::atomic::Ordering::SeqCst);

        let mut queue = Receiver::<()>::default();
        let first = queue.queue().create(());
        let second = queue.queue().create(());

        // Nobody is parked.
        first.enqueue_by_ref();
        assert_eq!(wakeups(), 0);
        assert!(queue.has_pending());
        // Queue is not empty, parking must be refused.
        assert!(!queue.park(&waker));
        assert!(unsafe { queue.dequeue().unwrap().into_live() }.is_some());
        // The refused park must not leave the waker behind.
        assert_eq!(Arc::strong_count(&counter), 2);
        second.enqueue_by_ref();
        assert_eq!(wakeups(), 0);
        assert!(unsafe { queue.dequeue().unwrap().into_live() }.is_some());

        assert!(!queue.has_pending());
        assert!(queue.park(&waker));
        assert!(queue.park(&waker));
        assert_eq!(Arc::strong_count(&counter), 3);
        first.enqueue_by_ref();
        assert_eq!(wakeups(), 1);
        second.enqueue_by_ref();
        assert_eq!(wakeups(), 1);

        unsafe {
            first.retire();
            second.retire();
        }
        drop(queue);
        assert_eq!(Arc::strong_count(&first), 1);
        assert_eq!(Arc::strong_count(&second), 1);
        assert_eq!(Arc::strong_count(&counter), 2);
    }

    #[test]
    fn retired_node_is_never_enqueued_again() {
        let mut queue = Receiver::<()>::default();

        // Retired while idle.
        let node = queue.queue().create(());
        unsafe { node.retire() };
        node.enqueue_by_ref();
        assert!(queue.dequeue().is_none());
        assert_eq!(Arc::strong_count(&node), 1);

        // Retired while enqueued.
        let node = queue.queue().create(());
        node.enqueue_by_ref();
        assert_eq!(Arc::strong_count(&node), 1);
        unsafe { node.retire() };
        assert_eq!(Arc::strong_count(&node), 2);
        assert!(unsafe { queue.dequeue().unwrap().into_live() }.is_none());
        assert_eq!(Arc::strong_count(&node), 1);
        node.enqueue_by_ref();
        assert!(queue.dequeue().is_none());

        // Retired, and then the receiver is dropped during an "enqueue".
        let node = queue.queue().create(());
        let weak = Arc::downgrade(&node);
        unsafe { node.retire() };
        drop(queue);
        node.enqueue();
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn requeue_keeps_fifo_order() {
        let mut queue = Receiver::<usize>::default();
        let nodes = (0..3).map(|i| queue.queue().create(i)).collect::<Vec<_>>();
        let pop = |queue: &mut Receiver<usize>| {
            let node = unsafe { queue.dequeue()?.into_live() }.unwrap();
            Some(*node.value())
        };

        // Empty queue: the node goes to the private slot.
        unsafe { queue.requeue(&nodes[0]) };
        assert!(queue.has_pending());
        assert_eq!(queue.marker(), Arc::as_ptr(&nodes[0]));
        // Requeue of an enqueued node is a no-op, and so is a wakeup.
        unsafe { queue.requeue(&nodes[0]) };
        nodes[0].enqueue_by_ref();
        // Nodes enqueued later come out later.
        nodes[1].enqueue_by_ref();
        assert_eq!(queue.marker(), Arc::as_ptr(&nodes[1]));
        unsafe { queue.requeue(&nodes[2]) };
        assert_eq!(queue.marker(), Arc::as_ptr(&nodes[2]));
        assert_eq!(pop(&mut queue), Some(0));
        assert_eq!(pop(&mut queue), Some(1));
        assert_eq!(pop(&mut queue), Some(2));
        assert_eq!(pop(&mut queue), None);
        assert!(queue.marker().is_null());
        assert!(!queue.has_pending());

        // Node retired while it sits in the private slot is released on dequeue...
        unsafe {
            queue.requeue(&nodes[0]);
            nodes[0].retire();
        }
        assert_eq!(Arc::strong_count(&nodes[0]), 2);
        assert!(unsafe { queue.dequeue().unwrap().into_live() }.is_none());
        assert_eq!(Arc::strong_count(&nodes[0]), 1);
        assert!(queue.dequeue().is_none());

        // ...or when the receiver is dropped.
        unsafe {
            queue.requeue(&nodes[1]);
            nodes[1].retire();
            nodes[2].retire();
        }
        assert_eq!(Arc::strong_count(&nodes[1]), 2);
        drop(queue);
        assert_eq!(Arc::strong_count(&nodes[1]), 1);
    }

    #[test]
    fn dropped_queue_drops_nodes() {
        let queue = Receiver::<()>::default();
        let nodes = std::iter::repeat_with(|| queue.queue().create(()))
            .take(10)
            .map(|strong| {
                let weak = Arc::downgrade(&strong);
                strong.enqueue_by_ref();
                unsafe { strong.retire() };
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
                strong.enqueue_by_ref();
                unsafe { strong.retire() };
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
        assert!(queue.marker().is_null());
        let node = queue.queue().create(());
        node.enqueue_by_ref();
        assert_eq!(queue.marker(), Arc::as_ptr(&node));
        assert!(unsafe { queue.dequeue().unwrap().into_live() }.is_some());
        assert!(queue.marker().is_null());
        let node_1 = queue.queue().create(());
        node_1.enqueue_by_ref();
        assert_eq!(queue.marker(), Arc::as_ptr(&node_1));
        unsafe {
            node.retire();
            node_1.retire();
        }
    }
}
