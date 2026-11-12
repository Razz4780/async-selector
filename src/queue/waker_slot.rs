#[cfg(loom)]
use loom::sync::atomic::{AtomicU8, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU8, Ordering};
use std::{cell::UnsafeCell, task::Waker};

const EMPTY: u8 = 0;
const ARMED: u8 = 1;
const BUSY: u8 = 2;

/// Slot for the queue [`Receiver`](super::Receiver)'s [`Waker`].
///
/// Tailored replacement for [`AtomicWaker`](futures::task::AtomicWaker).
/// The difference is that [`WakerSlot::notify`] costs a single atomic load
/// when no waker is registered, which is the common case:
/// the receiver only registers its waker right before it goes to sleep.
pub struct WakerSlot {
    /// * [`EMPTY`] - no waker is registered, only the receiver may access `waker`
    /// * [`ARMED`] - waker is registered, nobody may access `waker`
    /// * [`BUSY`] - whoever moved the state from [`ARMED`] may access `waker`
    state: AtomicU8,
    waker: UnsafeCell<Option<Waker>>,
}

impl WakerSlot {
    /// Registers the waker. Must only be called by the receiver.
    ///
    /// Returns `false` if a concurrent [`WakerSlot::notify`] got in the way.
    /// In this case, the waker might not be registered.
    pub fn register(&self, waker: &Waker) -> bool {
        match self.state.load(Ordering::Acquire) {
            EMPTY => {
                unsafe {
                    // SAFETY: state is `EMPTY`, and we are the receiver.
                    *self.waker.get() = Some(waker.clone());
                }
                self.state.store(ARMED, Ordering::SeqCst);
                #[cfg(loom)]
                loom::sync::atomic::fence(Ordering::SeqCst);
                true
            }
            ARMED => {
                if self
                    .state
                    .compare_exchange(ARMED, BUSY, Ordering::SeqCst, Ordering::Acquire)
                    .is_err()
                {
                    return false;
                }
                // If the assignment panics, the slot stays `BUSY` forever,
                // and the receiver degrades to busy polling. This is still correct.
                unsafe {
                    // SAFETY: we moved the state from `ARMED` to `BUSY`.
                    let slot = &mut *self.waker.get();
                    match slot {
                        Some(old) if old.will_wake(waker) => {}
                        _ => *slot = Some(waker.clone()),
                    }
                }
                self.state.store(ARMED, Ordering::SeqCst);
                #[cfg(loom)]
                loom::sync::atomic::fence(Ordering::SeqCst);
                true
            }
            _ => false,
        }
    }

    /// Wakes the registered waker, if there is one.
    pub fn notify(&self) {
        #[cfg(loom)]
        loom::sync::atomic::fence(Ordering::SeqCst);
        if self.state.load(Ordering::SeqCst) != ARMED {
            return;
        }
        self.notify_slow();
    }

    /// Removes the registered waker, if there is one. Must only be called by the receiver.
    pub fn disarm(&self) {
        if self
            .state
            .compare_exchange(ARMED, BUSY, Ordering::SeqCst, Ordering::Acquire)
            .is_err()
        {
            // A sender is waking the waker right now (or just did).
            return;
        }
        let waker = unsafe {
            // SAFETY: we moved the state from `ARMED` to `BUSY`.
            (*self.waker.get()).take()
        };
        self.state.store(EMPTY, Ordering::Release);
        drop(waker);
    }

    #[cold]
    fn notify_slow(&self) {
        if self
            .state
            .compare_exchange(ARMED, BUSY, Ordering::SeqCst, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let waker = unsafe {
            // SAFETY: we moved the state from `ARMED` to `BUSY`.
            (*self.waker.get()).take()
        };
        self.state.store(EMPTY, Ordering::Release);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Default for WakerSlot {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(EMPTY),
            waker: UnsafeCell::new(None),
        }
    }
}
