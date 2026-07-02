use std::{
    mem::ManuallyDrop,
    ops::Deref,
    sync::Arc,
    task::{RawWaker, RawWakerVTable, Waker},
};

use crate::{
    list::{ListProtected, StoredInList},
    mpsc::{QueueLink, StoredInQueue, WeakSender},
};

/// Wrapper for the pollable tasks stored in a [`Selector`](crate::selector::Selector).
///
/// Meant to be passed around in [`Arc`].
pub struct Task<P> {
    /// Weak reference to the ready tasks queue.
    ready_tx: WeakSender<Self>,
    /// State owned by the queue of ready tasks.
    queue_link: QueueLink<Self>,
    /// State owned by the list of all tasks.
    ///
    /// This is where the task is stored.
    /// We never move this value, and the tasks are always stored on heap.
    /// `P` can be safely polled, even if it's not [`Unpin`].
    list_protected: ListProtected<Self>,
}

impl<P> Task<P> {
    /// Creates an empty task that has no `P`.
    pub fn empty(ready_tx: WeakSender<Self>) -> Self {
        Self {
            ready_tx,
            queue_link: Default::default(),
            list_protected: Default::default(),
        }
    }

    pub fn ready_tx(&self) -> &WeakSender<Self> {
        &self.ready_tx
    }

    /// Returns a [`Waker`] wrapper that should be used when polling the task inside the selector.
    pub fn borrow_waker(self: &Arc<Self>) -> WakerRef<'_, P> {
        let data = Arc::as_ptr(self).cast();
        let waker = unsafe { Waker::new(data, &Self::WAKER_VTABLE) };
        WakerRef {
            waker: ManuallyDrop::new(waker),
            _borrowed_from: self,
        }
    }

    const WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::raw_clone,
        Self::raw_wake,
        Self::raw_wake_by_ref,
        Self::raw_drop,
    );

    unsafe fn raw_clone(data: *const ()) -> RawWaker {
        unsafe {
            Arc::increment_strong_count(data.cast::<Self>());
        };
        RawWaker::new(data, &Self::WAKER_VTABLE)
    }

    unsafe fn raw_wake(data: *const ()) {
        let task = unsafe { Arc::from_raw(data.cast::<Self>()) };
        let Some(sender) = task.ready_tx.upgrade() else {
            return;
        };
        sender.send(task);
    }

    unsafe fn raw_wake_by_ref(data: *const ()) {
        let task = data.cast::<Self>();
        let task = unsafe { task.as_ref_unchecked() };
        let Some(tx) = task.ready_tx.upgrade() else {
            return;
        };
        let task = unsafe {
            Arc::increment_strong_count(task);
            Arc::from_raw(task)
        };
        tx.send(task);
    }

    unsafe fn raw_drop(data: *const ()) {
        unsafe {
            Arc::decrement_strong_count(data.cast::<Self>());
        }
    }
}

/// SAFETY: [`QueueLink`] is stored as a plain field and never overwritten.
unsafe impl<P> StoredInQueue for Task<P> {
    fn queue_link(&self) -> &QueueLink<Self> {
        &self.queue_link
    }
}

/// SAFETY: [`ListProtected`] is stored as a plain field and never overwritten.
unsafe impl<P> StoredInList for Task<P> {
    type Protected = P;

    fn list_protected(&self) -> &ListProtected<Self> {
        &self.list_protected
    }
}

/// Borrowed [`Waker`] for a specific [`Task`].
pub struct WakerRef<'a, P> {
    /// We don't actually own the waker.
    ///
    /// Waker assumes that it owns an [`Arc`].
    /// In [`Task::borrow_waker`] we did not call [`Arc::into_raw`].
    /// Instead, we called [`Arc::as_ptr`] in order to avoid unnecessary cloning.
    /// Therefore, we cannot drop this waker. [`ManuallyDrop`] wrapper ensures that.
    waker: ManuallyDrop<Waker>,
    /// Since [`Self::waker`] is only borrowed, we artificially shorten the lifetime of this struct.
    _borrowed_from: &'a Task<P>,
}

impl<P> Deref for WakerRef<'_, P> {
    type Target = Waker;

    fn deref(&self) -> &Self::Target {
        &self.waker
    }
}
