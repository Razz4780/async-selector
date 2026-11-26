use std::{mem::ManuallyDrop, ops::Not, sync::Arc, task::Waker};

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

/// Value of [`Queue::inbox`] after the [`Receiver`] is dropped.
///
/// Never dereferenced. Cannot be an address of a node, because nodes are aligned.
fn closed<T>() -> *mut Node<T> {
    std::ptr::without_provenance_mut(1)
}

/// Receiver handle for a [`Queue`].
///
/// Since the queue is MPSC, there can be only one instance.
///
/// # Structure
///
/// The queue consists of two parts:
/// 1. The inbox ([`Queue::inbox`]), shared with the senders. This is a Treiber stack:
///    a sender pushes one node with a compare-and-swap,
///    and the receiver takes **all** nodes at once with a single swap.
/// 2. A FIFO list of nodes that were already taken from the inbox,
///    private to the receiver (no atomic operations are needed to use it).
///
/// The inbox hands the nodes over in the LIFO order, so the receiver reverses each batch
/// before it appends the batch to the private list. As a result, the nodes are dequeued
/// in the order in which they were enqueued.
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
/// which is released when the node is dequeued (or when the receiver is dropped).
///
/// The receiver must be dropped only after all nodes created from its queue were retired.
pub struct Receiver<T> {
    queue: Arc<Queue<T>>,
    /// Oldest node in the private list, null if the list is empty.
    first: *const Node<T>,
    /// Newest node in the private list, null if the list is empty.
    ///
    /// The [`Node::next_enqueued`] of this node is always null.
    last: *const Node<T>,
}

impl<T> Receiver<T> {
    pub fn queue(&self) -> &Arc<Queue<T>> {
        &self.queue
    }

    /// Returns a pointer to the last node in the queue.
    ///
    /// The returned pointer can be used as a snapshot marker, it must not be dereferenced.
    /// If the queue is empty, this method returns [`std::ptr::null`].
    pub fn marker(&mut self) -> *const Node<T> {
        self.collect();
        self.last
    }

    /// Returns whether the queue contains a node.
    /// If so, the receiver should be polled again right away.
    ///
    /// `false` is a guarantee: there is no node in the queue,
    /// and every enqueue that completes after this check will observe
    /// a waker registered **before** this check ([`Receiver::park`] relies on this:
    /// it calls this method again after it registers the waker).
    pub fn has_pending(&self) -> bool {
        self.first.is_null().not() || self.queue.inbox.load(Ordering::SeqCst).is_null().not()
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
    /// Returns [`None`] if the queue is empty.
    pub fn dequeue(&mut self) -> Option<Dequeued<T>> {
        if self.first.is_null() {
            self.collect();
            if self.first.is_null() {
                return None;
            }
        }

        let node = self.first;
        // The link must be read before the node is handed out:
        // once `Dequeued::into_live` clears the `ENQUEUED` bit, the senders may overwrite it.
        let next = unsafe { &*node }.next_enqueued.load(Ordering::Relaxed);
        self.first = next;
        if next.is_null() {
            self.last = std::ptr::null();
        }
        Some(Dequeued(node))
    }

    /// Enqueues the given [`Node`] again, unless it is already in the queue.
    ///
    /// Meant to be called by the receiver itself, so it does not touch the inbox
    /// unless some sender did (the node goes straight to the private list),
    /// and it does not wake the receiver.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the [`Node`] belongs to this queue instance and was not retired.
    pub unsafe fn requeue(&mut self, node: &Arc<Node<T>>) {
        if node.state.fetch_or(ENQUEUED, Ordering::AcqRel) & ENQUEUED != 0 {
            return;
        }
        unsafe { self.enqueue_private(Arc::as_ptr(node)) };
    }

    /// Enqueues a [`Node`] obtained from [`Queue::create_enqueued`].
    ///
    /// Having unique access to the receiver, this method skips the inbox.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the [`Node`]:
    /// 1. was created with [`Queue::create_enqueued`] called on this queue instance
    /// 2. was not passed to this function before
    pub unsafe fn enqueue_created(&mut self, node: &Arc<Node<T>>) {
        unsafe { self.enqueue_private(Arc::as_ptr(node)) };
        self.queue.waker.notify();
    }

    /// Appends the node to the private list.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the [`Node`]:
    /// 1. belongs to this queue instance
    /// 2. has the [`ENQUEUED`] bit set, and the caller is the one who set it
    unsafe fn enqueue_private(&mut self, node: *const Node<T>) {
        // Nodes that the senders enqueued before this call go first.
        self.collect();

        unsafe { &*node }
            .next_enqueued
            .store(std::ptr::null_mut(), Ordering::Relaxed);
        if self.last.is_null() {
            self.first = node;
        } else {
            unsafe { &*self.last }
                .next_enqueued
                .store(node.cast_mut(), Ordering::Relaxed);
        }
        self.last = node;
    }

    /// Moves all nodes from the inbox to the private list.
    #[inline]
    fn collect(&mut self) {
        // A sender that we are not synchronized with may be missed here.
        // This is fine, it is as if that sender came right after this check.
        if self.queue.inbox.load(Ordering::Relaxed).is_null() {
            return;
        }
        self.collect_slow();
    }

    fn collect_slow(&mut self) {
        let newest = self
            .queue
            .inbox
            .swap(std::ptr::null_mut(), Ordering::AcqRel);
        if newest.is_null() {
            return;
        }

        // The batch is linked from the newest node to the oldest one. Reverse it.
        let mut node = newest;
        let mut newer = std::ptr::null_mut();
        let oldest = loop {
            // Load and store, not `swap`: the receiver has exclusive access to the link,
            // and an atomic swap is a locked instruction on x86.
            let link = &unsafe { &*node }.next_enqueued;
            let older = link.load(Ordering::Relaxed);
            link.store(newer, Ordering::Relaxed);
            if older.is_null() {
                break node;
            }
            newer = node;
            node = older;
        };

        if self.last.is_null() {
            self.first = oldest;
        } else {
            unsafe { &*self.last }
                .next_enqueued
                .store(oldest, Ordering::Relaxed);
        }
        self.last = newest;
    }
}

impl<T> Default for Receiver<T> {
    fn default() -> Self {
        Self {
            queue: Arc::new(Queue {
                inbox: Default::default(),
                waker: Default::default(),
            }),
            first: std::ptr::null(),
            last: std::ptr::null(),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        /// Releases the nodes found in the queue.
        ///
        /// If dropping a node panics, the remaining nodes are still released.
        struct Guard<T> {
            /// Remaining nodes of the private list.
            private: *const Node<T>,
            /// Remaining nodes of the inbox.
            inbox: *const Node<T>,
        }

        impl<T> Drop for Guard<T> {
            fn drop(&mut self) {
                let mut inner = Self {
                    private: self.private,
                    inbox: self.inbox,
                };

                loop {
                    let chain = if inner.private.is_null() {
                        &mut inner.inbox
                    } else {
                        &mut inner.private
                    };
                    let to_drop = *chain;
                    if to_drop.is_null() {
                        break;
                    }
                    *chain = unsafe { &*to_drop }.next_enqueued.load(Ordering::Relaxed);

                    // The receiver is going away, so all nodes were retired.
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

        // From now on, every enqueue fails (see `Queue::push`),
        // so nothing gets stranded in the inbox.
        let inbox = self.queue.inbox.swap(closed(), Ordering::AcqRel);
        let guard = Guard {
            private: self.first,
            inbox,
        };
        // The queue itself lives for as long as any of its nodes.
        // Do not let it hold on to the waker.
        drop(self.queue.waker.disarm());
        drop(guard);
    }
}

unsafe impl<T: Send> Send for Receiver<T> {}

unsafe impl<T: Send> Sync for Receiver<T> {}

/// Intrusive MPSC queue, shared between the [`Receiver`] and the [`Node`]s.
///
/// See [`Receiver`] for the description of the structure.
pub struct Queue<T> {
    /// Treiber stack of the nodes enqueued by the senders. The newest node is on top.
    ///
    /// * null - the inbox is empty
    /// * [`closed`] - the [`Receiver`] was dropped
    inbox: AtomicPtr<Node<T>>,
    /// Waker of the receiver, if the receiver is parked.
    waker: WakerSlot,
}

impl<T> Queue<T> {
    /// Creates a new [`Node`] bound to this queue.
    ///
    /// The node is marked as enqueued, but it is **not** in the queue yet.
    /// The caller is expected to follow up with [`Receiver::enqueue_created`].
    pub fn create_enqueued(self: &Arc<Self>, value: T) -> Arc<Node<T>> {
        Arc::new(Node {
            queue: self.clone(),
            state: AtomicU8::new(ENQUEUED),
            next_enqueued: Default::default(),
            value,
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

    /// Enqueues all of the given [`Node`]s that are not in the queue yet,
    /// and wakes the receiver.
    ///
    /// The whole batch is linked locally, and then pushed with a single compare-and-swap.
    ///
    /// # Safety
    ///
    /// Caller must ensure that:
    /// 1. the [`Node`]s are alive and belong to this queue instance
    /// 2. the [`Receiver`] is alive throughout this call
    pub unsafe fn enqueue_all(&self, nodes: impl IntoIterator<Item = *const Node<T>>) {
        let mut oldest = std::ptr::null_mut::<Node<T>>();
        let mut newest = std::ptr::null_mut::<Node<T>>();
        for node in nodes {
            let node = node.cast_mut();
            if unsafe { &*node }.state.fetch_or(ENQUEUED, Ordering::AcqRel) & ENQUEUED != 0 {
                continue;
            }
            if oldest.is_null() {
                oldest = node;
            } else {
                unsafe { &*node }
                    .next_enqueued
                    .store(newest, Ordering::Relaxed);
            }
            newest = node;
        }

        if oldest.is_null() {
            return;
        }
        let pushed = unsafe { self.push(oldest, newest) };
        debug_assert!(pushed, "receiver dropped during `enqueue_all`");
        self.waker.notify();
    }

    /// Pushes a chain of nodes to the inbox.
    ///
    /// The chain must be linked with [`Node::next_enqueued`] from `newest` to `oldest`
    /// (a single node is a chain with `newest == oldest`).
    /// The link of `oldest` is overwritten.
    ///
    /// Returns `false` if the [`Receiver`] is gone. In this case, the chain is not pushed.
    ///
    /// # Safety
    ///
    /// Caller must ensure that all nodes in the chain:
    /// 1. belong to this queue instance
    /// 2. have the [`ENQUEUED`] bit set, and the caller is the one who set it
    unsafe fn push(&self, oldest: *mut Node<T>, newest: *mut Node<T>) -> bool {
        let oldest = unsafe { &*oldest };
        debug_assert!(oldest.state.load(Ordering::Relaxed) & ENQUEUED != 0);
        debug_assert_eq!(std::ptr::from_ref(self), Arc::as_ptr(&oldest.queue));

        let mut top = self.inbox.load(Ordering::Relaxed);
        loop {
            if top == closed() {
                return false;
            }
            // The previous top is never dereferenced here, so it does not matter
            // whether it is still in the inbox when the exchange succeeds: if the top is the same
            // node again (ABA), the node below ours is exactly the one we linked.
            //
            // One corner of ABA is worth knowing about. The old top may be dequeued, retired
            // and freed, and a new node allocated at the same address may become the top
            // before the exchange. The exchange compares addresses, so it succeeds, and the link
            // stored below has the right address, but it was derived from the freed allocation.
            // This is fine for the machine, but a provenance-tracking checker (Miri) would report
            // the receiver following that link, if it ever produced this interleaving.
            oldest.next_enqueued.store(top, Ordering::Relaxed);
            // `SeqCst` is required here. See `WakerSlot`.
            match self
                .inbox
                .compare_exchange_weak(top, newest, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(current) => top = current,
            }
        }
    }
}

unsafe impl<T: Send> Send for Queue<T> {}

unsafe impl<T: Send> Sync for Queue<T> {}

/// Node of a [`Queue`].
///
/// Always belongs to a single [`Queue`].
pub struct Node<T> {
    /// Keeps the queue alive, so that the node can always be pushed to the inbox
    /// without checking whether the queue still exists.
    /// A dropped [`Receiver`] is detected through the inbox itself (see [`closed`]).
    queue: Arc<Queue<T>>,
    /// Combination of [`ENQUEUED`] and [`RETIRED`].
    state: AtomicU8,
    /// Link to another node in the queue.
    ///
    /// * In the inbox: the node enqueued right before this one.
    /// * In the private list of the [`Receiver`]: the node enqueued right after this one.
    ///
    /// Only accessed by whoever holds the node at the moment:
    /// the sender that set the [`ENQUEUED`] bit until it pushes the node, and then the receiver.
    /// The atomic is here only to make the hand-off sound, all operations are relaxed.
    next_enqueued: AtomicPtr<Self>,
    value: T,
}

impl<T> Node<T> {
    /// Returns the queue instance to which this node belongs.
    pub fn queue(&self) -> &Arc<Queue<T>> {
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
        let node = Arc::as_ptr(self).cast_mut();
        if unsafe { self.queue.push(node, node) } {
            self.queue.waker.notify();
        } else {
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
        &self.value
    }

    #[cfg(test)]
    fn is_enqueued(&self) -> bool {
        self.state.load(Ordering::Acquire) & ENQUEUED != 0
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

    /// A sender enqueues two nodes, while the receiver enqueues its own node and dequeues.
    /// Batches taken from the inbox must come out in the order in which the sender pushed them,
    /// no matter how the pushes are split between the batches.
    #[cfg(loom)]
    #[test]
    fn loom_fifo_order_is_kept() {
        loom::model(|| {
            let mut receiver = Receiver::default();
            let queue = receiver.queue().clone();
            let nodes = (0..3).map(|i| queue.create(i)).collect::<Vec<_>>();

            let sender = {
                let first = nodes[0].clone();
                let second = nodes[1].clone();
                loom::thread::spawn(move || {
                    first.enqueue();
                    second.enqueue();
                })
            };

            unsafe { receiver.requeue(&nodes[2]) };
            let mut values = Vec::new();
            while values.len() < 3 {
                let Some(node) = receiver.dequeue() else {
                    loom::thread::yield_now();
                    continue;
                };
                values.push(*unsafe { node.into_live() }.unwrap().value());
            }

            sender.join().unwrap();
            assert!(receiver.dequeue().is_none());

            let position = |value| values.iter().position(|v| *v == value).unwrap();
            assert!(
                position(0) < position(1),
                "sender order not kept: {values:?}"
            );

            for node in &nodes {
                unsafe { node.retire() };
            }
        });
    }

    /// A batch enqueue races with a sender that enqueues one of the nodes from the batch.
    /// Every node must end up in the queue exactly once.
    #[cfg(loom)]
    #[test]
    fn loom_enqueue_all_races_with_enqueue() {
        loom::model(|| {
            let mut receiver = Receiver::default();
            let queue = receiver.queue().clone();
            let nodes = (0..3).map(|i| queue.create(i)).collect::<Vec<_>>();

            let sender = {
                let shared = nodes[1].clone();
                let other = nodes[2].clone();
                loom::thread::spawn(move || {
                    shared.enqueue();
                    other.enqueue();
                })
            };

            unsafe { queue.enqueue_all(nodes[..2].iter().map(Arc::as_ptr)) };
            // Dequeue only after both are done. Otherwise the shared node
            // could be legitimately enqueued for the second time.
            sender.join().unwrap();

            let mut values = Vec::new();
            while let Some(node) = receiver.dequeue() {
                values.push(*unsafe { node.into_live() }.unwrap().value());
            }
            values.sort_unstable();
            assert_eq!(values, [0, 1, 2]);

            for node in &nodes {
                unsafe { node.retire() };
            }
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

        // Empty queue.
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

        // Node retired while it sits in the queue is released on dequeue...
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
    fn batches_are_dequeued_in_fifo_order() {
        let mut queue = Receiver::<usize>::default();
        let nodes = (0..8).map(|i| queue.queue().create(i)).collect::<Vec<_>>();
        let pop = |queue: &mut Receiver<usize>| {
            let node = unsafe { queue.dequeue()?.into_live() }.unwrap();
            Some(*node.value())
        };

        // First batch.
        for node in &nodes[..3] {
            node.enqueue_by_ref();
        }
        assert_eq!(queue.marker(), Arc::as_ptr(&nodes[2]));
        assert_eq!(pop(&mut queue), Some(0));
        // Second batch, taken from the inbox while the first one is not consumed yet.
        for node in &nodes[3..5] {
            node.enqueue_by_ref();
        }
        assert_eq!(queue.marker(), Arc::as_ptr(&nodes[4]));
        // Third batch, mixed with nodes enqueued by the receiver itself.
        nodes[5].enqueue_by_ref();
        unsafe { queue.requeue(&nodes[6]) };
        nodes[7].enqueue_by_ref();
        unsafe { queue.requeue(&nodes[0]) };
        assert_eq!(queue.marker(), Arc::as_ptr(&nodes[0]));

        for expected in [1, 2, 3, 4, 5, 6, 7, 0] {
            assert_eq!(pop(&mut queue), Some(expected));
        }
        assert_eq!(pop(&mut queue), None);
        assert!(queue.marker().is_null());
        assert!(!queue.has_pending());

        for node in &nodes {
            unsafe { node.retire() };
        }
    }

    #[test]
    fn enqueue_all_keeps_order_and_skips_enqueued_nodes() {
        let mut queue = Receiver::<usize>::default();
        let nodes = (0..5).map(|i| queue.queue().create(i)).collect::<Vec<_>>();
        let pop = |queue: &mut Receiver<usize>| {
            let node = unsafe { queue.dequeue()?.into_live() }.unwrap();
            Some(*node.value())
        };
        let all = |nodes: &[Arc<super::Node<usize>>]| {
            nodes
                .iter()
                .map(Arc::as_ptr)
                .collect::<Vec<_>>()
                .into_iter()
        };

        // Nothing to enqueue.
        unsafe { queue.queue().enqueue_all(all(&[])) };
        assert!(!queue.has_pending());

        nodes[3].enqueue_by_ref();
        unsafe { queue.requeue(&nodes[1]) };
        unsafe { queue.queue().enqueue_all(all(&nodes)) };
        for expected in [3, 1, 0, 2, 4] {
            assert_eq!(pop(&mut queue), Some(expected));
        }
        assert_eq!(pop(&mut queue), None);

        // Everything is enqueued already.
        unsafe { queue.queue().enqueue_all(all(&nodes)) };
        unsafe { queue.queue().enqueue_all(all(&nodes)) };
        for expected in 0..5 {
            assert_eq!(pop(&mut queue), Some(expected));
        }
        assert_eq!(pop(&mut queue), None);

        for node in &nodes {
            unsafe { node.retire() };
        }
    }

    #[test]
    fn enqueue_all_wakes_parked_receiver() {
        struct Counter(std::sync::atomic::AtomicUsize);

        impl Wake for Counter {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let counter = Arc::new(Counter(Default::default()));
        let waker = Waker::from(counter.clone());

        let mut queue = Receiver::<()>::default();
        let node = queue.queue().create(());
        assert!(queue.park(&waker));
        unsafe { queue.queue().enqueue_all([Arc::as_ptr(&node)]) };
        assert_eq!(counter.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        unsafe { node.retire() };
    }

    /// The queue outlives the receiver for as long as any node exists.
    /// It must not keep the waker of the receiver, or the retired nodes.
    #[test]
    fn dropped_receiver_releases_waker_and_nodes() {
        let mut queue = Receiver::<()>::default();
        let idle = queue.queue().create(());
        let in_inbox = queue.queue().create(());
        let in_private_list = queue.queue().create(());

        assert!(queue.park(Waker::noop()));
        let waker_owner = Arc::new(());
        struct Owned(#[allow(dead_code)] Arc<()>);
        #[allow(clippy::manual_noop_waker)] // the waker must own a reference
        impl Wake for Owned {
            fn wake(self: Arc<Self>) {}
        }
        let waker = Waker::from(Arc::new(Owned(waker_owner.clone())));
        assert!(queue.park(&waker));
        drop(waker);
        assert_eq!(Arc::strong_count(&waker_owner), 2);

        unsafe { queue.requeue(&in_private_list) };
        // Takes the waker. Park again, so that the receiver is dropped with a waker registered.
        in_inbox.enqueue_by_ref();
        assert_eq!(Arc::strong_count(&waker_owner), 1);
        assert_eq!(queue.marker(), Arc::as_ptr(&in_inbox));
        assert!(unsafe { queue.dequeue().unwrap().into_live() }.is_some());
        assert!(unsafe { queue.dequeue().unwrap().into_live() }.is_some());
        let waker = Waker::from(Arc::new(Owned(waker_owner.clone())));
        assert!(queue.park(&waker));
        drop(waker);
        assert_eq!(Arc::strong_count(&waker_owner), 2);
        unsafe { queue.requeue(&in_private_list) };
        in_inbox.enqueue_by_ref();
        assert_eq!(Arc::strong_count(&waker_owner), 1);
        let waker = Waker::from(Arc::new(Owned(waker_owner.clone())));
        // The queue is not empty, so the registration is refused.
        assert!(!queue.park(&waker));
        assert_eq!(Arc::strong_count(&waker_owner), 2);
        drop(waker);
        assert_eq!(Arc::strong_count(&waker_owner), 1);

        unsafe {
            idle.retire();
            in_inbox.retire();
            in_private_list.retire();
        }
        assert_eq!(Arc::strong_count(&idle), 1);
        assert_eq!(Arc::strong_count(&in_inbox), 2);
        assert_eq!(Arc::strong_count(&in_private_list), 2);
        drop(queue);
        assert_eq!(Arc::strong_count(&in_inbox), 1);
        assert_eq!(Arc::strong_count(&in_private_list), 1);

        // Late wakeups are ignored.
        idle.enqueue_by_ref();
        in_inbox.enqueue_by_ref();
        assert_eq!(Arc::strong_count(&idle), 1);
        assert_eq!(Arc::strong_count(&in_inbox), 1);
    }

    /// A sender wins the right to enqueue a node, the node is retired,
    /// and the receiver is dropped before the sender gets to push the node.
    #[test]
    fn push_after_receiver_drop_is_refused() {
        use std::sync::atomic::Ordering;

        let queue = Receiver::<()>::default();
        let node = queue.queue().create(());
        let ptr = Arc::as_ptr(&node).cast_mut();

        // First half of `Node::enqueue_by_ref`.
        assert_eq!(node.state.fetch_or(super::ENQUEUED, Ordering::AcqRel), 0);
        unsafe { node.retire() };
        assert_eq!(Arc::strong_count(&node), 2);
        drop(queue);
        // Second half.
        assert!(unsafe { !node.queue().push(ptr, ptr) });
        unsafe { Arc::decrement_strong_count(ptr) };
        assert_eq!(Arc::strong_count(&node), 1);
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
