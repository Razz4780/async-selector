//! Helpers shared by all benchmarks.
//!
//! Most benchmarks drive the selectors with a hand-written poll loop ([`Driver`]) instead of an
//! async runtime, so that the measurements contain only the cost of the selectors themselves.

#![allow(dead_code)]

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use criterion::Criterion;
use futures::{Stream, task::AtomicWaker};

/// Numbers of tasks used by most benchmarks.
pub const SIZES: [usize; 3] = [64, 1024, 16 * 1024];

/// Number of items produced by a single task, where applicable.
pub const ITEMS_PER_TASK: usize = 64;

/// Shorter warm-up and measurement than the criterion defaults,
/// so that the whole suite finishes in reasonable time.
///
/// Can be overridden from the command line, e.g. `cargo bench -- --measurement-time 10`.
pub fn config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
}

/// Waker that records whether it was woken.
#[derive(Default)]
struct Flag(AtomicBool);

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Polls streams on the current thread, without an async runtime.
///
/// Selectors may return [`Poll::Pending`] after waking the outer waker,
/// to yield control back to the executor. The driver detects that and polls again.
/// A [`Poll::Pending`] without a wakeup means that the stream has stalled.
pub struct Driver {
    flag: Arc<Flag>,
    waker: Waker,
}

impl Default for Driver {
    fn default() -> Self {
        let flag = Arc::new(Flag::default());
        let waker = Waker::from(flag.clone());
        Self { flag, waker }
    }
}

impl Driver {
    /// Polls the stream for the next item.
    ///
    /// Returns [`None`] if the stream has stalled.
    #[inline]
    pub fn next<S: Stream + Unpin>(&self, stream: &mut S) -> Option<Option<S::Item>> {
        let mut cx = Context::from_waker(&self.waker);
        loop {
            match Pin::new(&mut *stream).poll_next(&mut cx) {
                Poll::Ready(item) => break Some(item),
                Poll::Pending if self.flag.0.swap(false, Ordering::Relaxed) => {},
                Poll::Pending => break None,
            }
        }
    }

    /// Polls the stream until it ends or stalls, passing all items to the given closure.
    ///
    /// Returns whether the stream has ended.
    #[inline]
    pub fn drain<S: Stream + Unpin>(&self, stream: &mut S, mut f: impl FnMut(S::Item)) -> bool {
        loop {
            match self.next(stream) {
                Some(Some(item)) => f(item),
                Some(None) => break true,
                None => break false,
            }
        }
    }

    /// Polls the stream until it ends, returning the number of produced items.
    ///
    /// # Panics
    ///
    /// If the stream stalls.
    #[inline]
    pub fn count_to_end<S: Stream + Unpin>(&self, stream: &mut S) -> usize {
        let mut count = 0;
        let ended = self.drain(stream, |item| {
            std::hint::black_box(item);
            count += 1;
        });
        assert!(ended, "stream stalled");
        count
    }
}

/// Stream that produces the given number of items, each ready immediately.
///
/// This is the case where [`SelectAll`](futures::stream::SelectAll) allocates on every item.
#[derive(Debug)]
pub struct ReadyStream(usize);

impl ReadyStream {
    pub fn new(items: usize) -> Self {
        Self(items)
    }
}

impl Stream for ReadyStream {
    type Item = usize;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.0 == 0 {
            return Poll::Ready(None);
        }
        this.0 -= 1;
        Poll::Ready(Some(this.0))
    }
}

/// Stream that produces the given number of items,
/// returning [`Poll::Pending`] and waking itself before each one.
#[derive(Debug)]
pub struct YieldStream {
    remaining: usize,
    yielded: bool,
}

impl YieldStream {
    pub fn new(items: usize) -> Self {
        Self {
            remaining: items,
            yielded: false,
        }
    }
}

impl Stream for YieldStream {
    type Item = usize;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.remaining == 0 {
            return Poll::Ready(None);
        }
        if this.yielded {
            this.yielded = false;
            this.remaining -= 1;
            Poll::Ready(Some(this.remaining))
        } else {
            this.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Future that returns [`Poll::Pending`] and wakes itself the given number of times,
/// and then resolves.
#[derive(Debug)]
pub struct YieldFuture(usize);

impl YieldFuture {
    pub fn new(yields: usize) -> Self {
        Self(yields)
    }
}

impl Future for YieldFuture {
    type Output = usize;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.0 == 0 {
            return Poll::Ready(0);
        }
        this.0 -= 1;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Notification counter that can be triggered from any thread.
#[derive(Default)]
pub struct Signal {
    count: AtomicUsize,
    waker: AtomicWaker,
}

impl Signal {
    /// Records one notification and wakes the [`SignalStream`].
    #[inline]
    pub fn notify(&self) {
        self.count.fetch_add(1, Ordering::Release);
        self.waker.wake();
    }

    #[inline]
    fn take(&self) -> usize {
        self.count.swap(0, Ordering::Acquire)
    }
}

/// Stream of notifications sent through a [`Signal`].
///
/// Each item is the number of notifications received since the previous item.
/// The stream ends after receiving the given total number of notifications.
pub struct SignalStream {
    signal: Arc<Signal>,
    received: usize,
    target: usize,
}

impl SignalStream {
    pub fn new(signal: Arc<Signal>, target: usize) -> Self {
        Self {
            signal,
            received: 0,
            target,
        }
    }

    /// Stream that never ends.
    pub fn endless(signal: Arc<Signal>) -> Self {
        Self::new(signal, usize::MAX)
    }
}

impl Stream for SignalStream {
    type Item = usize;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.received >= this.target {
            return Poll::Ready(None);
        }

        let mut count = this.signal.take();
        if count == 0 {
            this.signal.waker.register(cx.waker());
            count = this.signal.take();
        }

        if count == 0 {
            Poll::Pending
        } else {
            this.received += count;
            Poll::Ready(Some(count))
        }
    }
}

/// Returns a deterministic pseudo-random permutation of `0..n`.
pub fn shuffled(n: usize) -> Vec<usize> {
    // xorshift64
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };

    let mut values = (0..n).collect::<Vec<_>>();
    for i in (1..n).rev() {
        let j = usize::try_from(next() % (i as u64 + 1)).unwrap();
        values.swap(i, j);
    }
    values
}
