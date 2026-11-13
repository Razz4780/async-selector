//! Cost of handling wakeups that come from outside of the selector.

mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use async_selector::StreamSelector;
use common::{Driver, SIZES, Signal, SignalStream, shuffled};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::{Stream, stream::SelectAll};

/// Hold `n` idle streams, wake `k` of them, and drain the selector.
///
/// The cost should depend only on `k`, not on `n`.
fn sparse_wakeups(c: &mut Criterion) {
    for k in [1, 16] {
        let mut group = c.benchmark_group(format!("sparse_wakeups/k={k}"));
        group.throughput(Throughput::Elements(k as u64));

        for n in SIZES {
            bench_sparse(&mut group, "StreamSelector", n, k, |streams| {
                streams.collect::<StreamSelector<_>>()
            });
            bench_sparse(&mut group, "SelectAll", n, k, |streams| {
                streams.collect::<SelectAll<_>>()
            });
        }

        group.finish();
    }
}

fn bench_sparse<S, F>(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &str,
    n: usize,
    k: usize,
    build: F,
) where
    S: Stream<Item = usize> + Unpin,
    F: FnOnce(std::vec::IntoIter<SignalStream>) -> S,
{
    let signals = (0..n)
        .map(|_| Arc::new(Signal::default()))
        .collect::<Vec<_>>();
    let streams = signals
        .iter()
        .cloned()
        .map(SignalStream::endless)
        .collect::<Vec<_>>();
    let mut selector = build(streams.into_iter());
    let driver = Driver::default();
    // First poll registers the wakers.
    assert!(!driver.drain(&mut selector, |_| {}));

    let order = shuffled(n);
    let mut position = 0;

    group.bench_function(BenchmarkId::new(name, n), |b| {
        b.iter(|| {
            for _ in 0..k {
                signals[order[position]].notify();
                position = (position + 1) % n;
            }
            let mut received = 0;
            let ended = driver.drain(&mut selector, |count| received += count);
            debug_assert!(!ended);
            debug_assert_eq!(received, k);
        })
    });
}

/// Hold [`STREAMS`] idle streams. In each round, `p` producer threads send one notification
/// to every stream, while the consumer drains the selector concurrently.
///
/// Measures the cost of wakeups sent from other threads,
/// including contention between the producers and the consumer on the selector's wakeup queue.
///
/// [`FuturesUnordered`](futures::stream::FuturesUnordered) is not included,
/// because [`SelectAll`] is built on top of it and already shows its behavior.
fn cross_thread(c: &mut Criterion) {
    const STREAMS: usize = 1024;

    let mut group = c.benchmark_group("cross_thread");
    group.throughput(Throughput::Elements(STREAMS as u64));

    for producers in [1, 4] {
        group.bench_function(BenchmarkId::new("StreamSelector", producers), |b| {
            b.iter_custom(|iters| {
                run_cross_thread(iters, producers, STREAMS, |streams| {
                    streams.collect::<StreamSelector<_>>()
                })
            })
        });

        group.bench_function(BenchmarkId::new("SelectAll", producers), |b| {
            b.iter_custom(|iters| {
                run_cross_thread(iters, producers, STREAMS, |streams| {
                    streams.collect::<SelectAll<_>>()
                })
            })
        });
    }

    group.finish();
}

fn run_cross_thread<S, F>(iters: u64, producers: usize, streams: usize, build: F) -> Duration
where
    S: Stream<Item = usize> + Unpin,
    F: FnOnce(std::vec::IntoIter<SignalStream>) -> S,
{
    let signals = (0..streams)
        .map(|_| Arc::new(Signal::default()))
        .collect::<Vec<_>>();
    let mut selector = build(
        signals
            .iter()
            .cloned()
            .map(SignalStream::endless)
            .collect::<Vec<_>>()
            .into_iter(),
    );
    let driver = Driver::default();
    // First poll registers the wakers.
    assert!(!driver.drain(&mut selector, |_| {}));

    // Number of the round that the producers should execute.
    // Spinning instead of blocking keeps the thread wakeup latency out of the measurement.
    let round = AtomicU64::new(0);

    thread::scope(|scope| {
        for chunk in signals.chunks(streams.div_ceil(producers)) {
            let round = &round;
            scope.spawn(move || {
                for current in 1..=iters {
                    while round.load(Ordering::Acquire) < current {
                        std::hint::spin_loop();
                    }
                    for signal in chunk {
                        signal.notify();
                    }
                }
            });
        }

        let mut elapsed = Duration::ZERO;
        for current in 1..=iters {
            let start = Instant::now();
            round.store(current, Ordering::Release);
            let mut received = 0;
            while received < streams {
                let ended = driver.drain(&mut selector, |count| received += count);
                debug_assert!(!ended);
            }
            elapsed += start.elapsed();
            assert_eq!(received, streams);
        }
        elapsed
    })
}

criterion_group! {
    name = benches;
    config = common::config();
    targets = sparse_wakeups, cross_thread
}
criterion_main!(benches);
