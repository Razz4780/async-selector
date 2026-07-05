use std::{
    cell::UnsafeCell,
    mem::ManuallyDrop,
    ops::Not,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicPtr, Ordering},
    },
    task::Waker,
};

use futures::task::AtomicWaker;

/// Trait for values that can be stored in a [`Queue`].
///
/// # Safety
///
/// Implementor must ensure that [`StoredInQueue::queue_link`]
/// always returns the exact same [`QueueLink`] instance.
pub unsafe trait StoredInQueue: Sized {
    fn queue_link(&self) -> &QueueLink<Self>;
}

/// Injected [`Queue`] links.
pub struct QueueLink<T> {
    /// Points to the next node in the queue.
    next: AtomicPtr<T>,
    /// This flag ensures that the node is never enqueued
    /// while already in the queue.
    in_queue: AtomicBool,
}

impl<T> Default for QueueLink<T> {
    fn default() -> Self {
        Self {
            next: Default::default(),
            in_queue: Default::default(),
        }
    }
}

/// 1024cores intrusive MPSC node-based queue.
///
/// # Source
///
/// This is mostly a Rust translation of
/// the [original C code](https://sites.google.com/site/1024cores/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue),
/// with one change in the [`Self::dequeue`] flow - if we enqueue the stub, we store the pointer to its predecessor in [`Self::stub_predecessor`].
/// This allows for optimized receive flow with [`Receiver::register`].
pub struct Queue<T: StoredInQueue> {
    waker: AtomicWaker,
    /// Current head of the queue.
    ///
    /// Always points to a valid node.
    head: AtomicPtr<T>,
    /// Tail of the queue.
    ///
    /// Always points to a valid node.
    tail: UnsafeCell<*mut T>,
    /// Predecessor of the enqueued stub node.
    ///
    /// If the stub node is not enqueued or has no predecessor, this value has no meaning.
    stub_predecessor: UnsafeCell<*mut T>,
    /// At all times, the queue must have at least one node.
    /// This stub is used to ensure it.
    ///
    /// Its address is stable because this struct is:
    /// 1. always stored on heap, and
    /// 2. never moved.
    stub: T,
}

impl<T: StoredInQueue> Queue<T> {
    /// Enqueues the given node, unless already stored in the queue.
    fn enqueue(&self, node: Arc<T>) {
        if node.queue_link().in_queue.swap(true, Ordering::AcqRel) {
            return;
        }
        unsafe {
            // SAFETY: the swap above acts as a mutex.
            // Only one enqueueing thread can reach this line.
            // Dequeueing thread releases the mutex as the last step.
            *node.queue_link().next.as_ptr().as_mut_unchecked() = std::ptr::null_mut();
        }
        let node = Arc::into_raw(node);

        unsafe {
            // SAFETY: `self.head` always points to a valid node.
            self.head
                .swap(node.cast_mut(), Ordering::AcqRel)
                .as_ref_unchecked()
                .queue_link()
                .next
                .store(node.cast_mut(), Ordering::Release);
        }

        self.waker.wake();
    }

    /// Dequeues the oldest node in the queue.
    ///
    /// Returns nothing if the queue is empty.
    /// Might also return nothing if the queue is short (less than 2 nodes)
    /// and some other thread is concurrently calling [`Self::enqueue`].
    ///
    /// # Safety
    ///
    /// Even though this method takes an immutable reference,
    /// it must not be called concurrently.
    /// Also, [`Self::tail`] and [`Self::stub_predecessor`] must not be accessed concurrently.
    unsafe fn dequeue(&self) -> Option<Arc<T>> {
        let mut tail = unsafe {
            // SAFETY: caller guarantees no concurrent access.
            *self.tail.get().as_ref_unchecked()
        };
        let mut next = unsafe {
            // SAFETY: tail always points to a valid node.
            tail.as_ref_unchecked()
                .queue_link()
                .next
                .load(Ordering::Acquire)
        };

        // If tail is stub, we need to skip it.
        if std::ptr::eq(tail, &self.stub) {
            if next.is_null() {
                // If tail is stub, and there's nothing after it,
                // the queue is empty.
                return None;
            }
            unsafe {
                // SAFETY: caller guarantees no concurrent access.
                *self.tail.get().as_mut_unchecked() = next;
            }
            tail = next;
            next = unsafe {
                // SAFETY: we just checked that next is not null.
                // If not null, it must be pointing to a valid node.
                next.as_mut_unchecked()
                    .queue_link()
                    .next
                    .load(Ordering::Acquire)
            };
        }

        // If the queue has at least two nodes, we can safely pop the oldest.
        // Concurrent enqueues do not access it.
        if next.is_null().not() {
            unsafe {
                // SAFETY: caller guarantees no concurrent access.
                *self.tail.get().as_mut_unchecked() = next;
            }
            debug_assert_ne!(
                tail.cast_const(),
                std::ptr::from_ref(&self.stub),
                "popping stub, this should never happen",
            );
            let elem = unsafe {
                // SAFETY: we ensured above that tail is not stub.
                Arc::from_raw(tail)
            };
            elem.queue_link().in_queue.store(false, Ordering::Release);
            return Some(elem);
        }

        // The queue has only one node, this is where it gets tricky.
        // We can't just pop the node, because the queue cannot be empty.

        if std::ptr::eq(tail, self.head.load(Ordering::Acquire)).not() {
            // Enqueue is in progress.
            // The enqueuing thread might be using the stub,
            // so we can't enqueue the stub ourselves.
            // Eventually some subsequent call to this method will observe
            // that the queue has more than one node.
            return None;
        }

        // Enqueue the stub.
        unsafe {
            // SAFETY: the queue has a stable non-stub node, so no enqueueing thread uses the stub.
            // Caller guarantees no concurrent calls to this method.
            *self.stub.queue_link().next.as_ptr().as_mut_unchecked() = std::ptr::null_mut();
        }
        let stub = std::ptr::from_ref(&self.stub).cast_mut();
        let stub_predecessor = self.head.swap(stub, Ordering::AcqRel);
        unsafe {
            // SAFETY: head always points to a valid node.
            stub_predecessor
                .as_ref_unchecked()
                .queue_link()
                .next
                .store(stub, Ordering::Release);
        }
        unsafe {
            // SAFETY: caller guarantees no concurrent access.
            *self.stub_predecessor.get().as_mut_unchecked() = stub_predecessor;
        }

        next = unsafe {
            // SAFETY: tail always points to a valid node,
            // caller guarantees no concurrent calls.
            tail.as_ref_unchecked()
                .queue_link()
                .next
                .load(Ordering::Acquire)
        };
        if next.is_null() {
            // Enqueue is in progress.
            None
        } else {
            unsafe {
                // SAFETY: caller guarantees no concurrent access.
                *self.tail.get().as_mut_unchecked() = next;
            }
            debug_assert_ne!(
                tail.cast_const(),
                std::ptr::from_ref(&self.stub),
                "popping stub, this should never happen",
            );
            let elem = unsafe { Arc::from_raw(tail) };
            elem.queue_link().in_queue.store(false, Ordering::Release);
            Some(elem)
        }
    }
}

impl<T: StoredInQueue> Drop for Queue<T> {
    fn drop(&mut self) {
        struct DestroyOnDrop<'a, T: StoredInQueue>(&'a Queue<T>);

        impl<T: StoredInQueue> Drop for DestroyOnDrop<'_, T> {
            fn drop(&mut self) {
                let inner_guard = DestroyOnDrop(self.0);
                while unsafe { self.0.dequeue().is_some() } {}
                let _ = ManuallyDrop::new(inner_guard);
            }
        }

        DestroyOnDrop(self);
    }
}

unsafe impl<T: StoredInQueue + Send> Send for Queue<T> {}
unsafe impl<T: StoredInQueue + Send> Sync for Queue<T> {}

pub struct Receiver<T: StoredInQueue>(Arc<Queue<T>>);

impl<T: StoredInQueue> Receiver<T> {
    /// Creates a new receiver and its [`Queue`].
    ///
    /// The given function will be used to create a reusable stub node
    /// (implementation details require that the queue always has at least one node).
    pub fn new<F>(make_stub: F) -> Self
    where
        F: FnOnce(WeakSender<T>) -> T,
    {
        let queue = Arc::<Queue<T>>::new_cyclic(|weak| {
            let stub = make_stub(WeakSender(weak.clone()));
            Queue {
                waker: Default::default(),
                head: AtomicPtr::new(std::ptr::null_mut()),
                tail: UnsafeCell::new(std::ptr::null_mut()),
                stub_predecessor: UnsafeCell::new(std::ptr::null_mut()),
                stub,
            }
        });
        let stub = std::ptr::from_ref(&queue.stub).cast_mut();
        unsafe {
            // SAFETY: we've just created this node, there can be no concurrent access.
            *queue.head.as_ptr().as_mut_unchecked() = stub;
            *queue.tail.get().as_mut_unchecked() = stub;
        }
        Self(queue)
    }

    /// Registers the given waker and returns a hint to the last value in the queue ([`std::ptr::null`] if the queue is empty).
    ///
    /// The waker will be woken when a new value is sent to the queue.
    ///
    /// # Returns
    ///
    /// The returned pointer is just a hint. It might happen that [`Self::recv`] returns none before the hinted value.
    /// This can happen if the hinted value is being sent at the same time when this method is called.
    /// The waker will be woken when the hinted value send has finished.
    ///
    /// The returned pointer can be used to process a "snapshot" of the queue state:
    /// 1. Consumer calls [`Self::register`] and obtains the pointer to the last value.
    /// 2. Consumer calls [`Self::recv`] in a loop until that value is returned.
    /// 3. Consumer yields and is woken when a new value is sent.
    ///
    /// This serves two purposes:
    /// 1. consumer does not have to register its [`Waker`] with every call to [`Self::recv`], and
    /// 2. consumer does not process the same node twice between yields (processing a value might trigger sending it again).
    pub fn register(&mut self, waker: &Waker) -> *const T {
        self.0.waker.register(waker);

        let last = self.0.head.load(Ordering::Acquire);
        if std::ptr::eq(last, &self.0.stub).not() {
            return last;
        }

        let tail = unsafe {
            // SAFETY: we have &mut reference and receiver is not cloneable,
            // so no other thread can be calling this concurrently.
            *self.0.tail.get().as_ref_unchecked()
        };
        if std::ptr::eq(tail, &self.0.stub) {
            return std::ptr::null();
        }

        unsafe {
            // SAFETY: we have &mut reference and receiver is not cloneable,
            // so no other thread can be calling this concurrently.
            *self.0.stub_predecessor.get().as_ref_unchecked()
        }
    }

    /// Pops the oldest value in the queue.
    pub fn recv(&mut self) -> Option<Arc<T>> {
        unsafe {
            // SAFETY: we have &mut reference and receiver is not cloneable,
            // so no other thread can be calling this concurrently.
            self.0.dequeue()
        }
    }

    /// Sends the given value to the queue.
    pub fn send(&self, value: Arc<T>) {
        self.0.enqueue(value);
    }

    /// Obtains a [`WeakSender`].
    pub fn weak_sender(&self) -> WeakSender<T> {
        WeakSender(Arc::downgrade(&self.0))
    }

    /// Returns whether the given [`WeakSender`] was created from this [`Receiver`] with [`Self::weak_sender`].
    pub fn is_parent(&self, weak: &WeakSender<T>) -> bool {
        std::ptr::eq(Arc::as_ptr(&self.0), weak.0.as_ptr())
    }
}

/// Allows for sending values to a [`Queue`].
pub struct Sender<T: StoredInQueue>(Arc<Queue<T>>);

impl<T: StoredInQueue> Sender<T> {
    /// Sends the given value to the queue.
    pub fn send(&self, value: Arc<T>) {
        self.0.enqueue(value);
    }
}

/// Weak reference to a [`Sender`].
///
/// Can be upgraded with [`Self::upgrade`].
pub struct WeakSender<T: StoredInQueue>(Weak<Queue<T>>);

impl<T: StoredInQueue> WeakSender<T> {
    /// Attempts to upgrade into a [`Sender`].
    ///
    /// Upgrade fails if there is no live [`Receiver`]/[`Sender`].
    pub fn upgrade(&self) -> Option<Sender<T>> {
        self.0.upgrade().map(Sender)
    }
}

impl<T: StoredInQueue> Clone for WeakSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::{Arc, Barrier, atomic::Ordering},
        task::Waker,
        time::{Duration, Instant},
    };

    use rstest::rstest;

    use crate::mpsc::{QueueLink, Receiver, StoredInQueue};

    #[derive(Default)]
    struct TestNode {
        sender: usize,
        link: QueueLink<Self>,
    }

    unsafe impl StoredInQueue for TestNode {
        fn queue_link(&self) -> &QueueLink<Self> {
            &self.link
        }
    }

    #[rstest]
    #[test]
    fn concurrent_senders(#[values(1, 2, 4)] senders: usize) {
        const SENT_BY_EACH: usize = 64 * 1024;

        let barrier = Arc::new(Barrier::new(senders + 1));
        let mut receiver = Receiver::new(|_| TestNode::default());

        for i in 0..senders {
            let barrier = barrier.clone();
            let sender = receiver.weak_sender().upgrade().unwrap();
            std::thread::spawn(move || {
                let node = Arc::new(TestNode {
                    sender: i,
                    link: Default::default(),
                });
                let mut remaining = SENT_BY_EACH;
                barrier.wait();

                while remaining > 0 {
                    if node.queue_link().in_queue.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                        continue;
                    }
                    sender.send(node.clone());
                    remaining -= 1;
                }
            });
        }

        let mut expected = vec![SENT_BY_EACH; senders];
        let mut total_expected = SENT_BY_EACH * senders;
        barrier.wait();

        let start = Instant::now();
        while total_expected > 0 {
            let Some(node) = receiver.recv() else {
                if start.elapsed() > Duration::from_secs(10) {
                    panic!("{expected:?}");
                }
                std::hint::spin_loop();
                continue;
            };

            expected[node.sender] = expected[node.sender].checked_sub(1).unwrap();
            total_expected = total_expected.checked_sub(1).unwrap();
        }

        assert_eq!(expected, vec![0; senders],);
    }

    #[test]
    fn queue_cleanup() {
        let receiver = Receiver::new(|_| TestNode {
            sender: 0,
            link: Default::default(),
        });

        let nodes = (0..10)
            .map(|_| TestNode::default())
            .map(Arc::new)
            .map(|node| {
                let weak = Arc::downgrade(&node);
                receiver.send(node);
                weak
            })
            .collect::<Vec<_>>();

        drop(receiver);
        nodes.iter().for_each(|node| {
            assert!(node.upgrade().is_none());
        });
    }

    #[test]
    fn marker() {
        let mut receiver = Receiver::new(|_| TestNode::default());
        assert_eq!(receiver.register(Waker::noop()), std::ptr::null());

        let node = Arc::new(TestNode::default());
        receiver.send(node.clone());
        assert_eq!(receiver.register(Waker::noop()), Arc::as_ptr(&node));
    }
}
