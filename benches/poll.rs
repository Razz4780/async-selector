//! Single-threaded polling throughput.
//!
//! [`StreamMap`] is included as the alternative that supports keyed access.
//! It holds futures wrapped with [`stream::once`].

mod common;

use std::hint::black_box;

use async_selector::{
    FutureReclaimSelector, FutureSelector, StreamReclaimSelector, StreamSelector,
    StreamWithIdSelector,
};
use common::{Driver, ITEMS_PER_TASK, ReadyStream, SIZES, YieldFuture, YieldStream};
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
    measurement::WallTime,
};
use futures::{
    Stream,
    stream::{self, FuturesUnordered, SelectAll},
};
use tokio_stream::StreamMap;

/// Benchmarks draining a stream built with `build`.
///
/// Building and dropping the stream is not measured.
fn bench_drain<S, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    n: usize,
    expected_items: usize,
    build: F,
) where
    S: Stream + Unpin,
    F: Fn() -> S,
{
    let driver = Driver::default();
    group.bench_function(BenchmarkId::new(name, n), |b| {
        b.iter_batched(
            &build,
            |mut stream| {
                assert_eq!(driver.count_to_end(&mut stream), expected_items);
                stream
            },
            BatchSize::LargeInput,
        )
    });
}

/// Benchmarks building a stream with `build` and draining it.
fn bench_build_and_drain<S, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    n: usize,
    expected_items: usize,
    build: F,
) where
    S: Stream + Unpin,
    F: Fn() -> S,
{
    let driver = Driver::default();
    group.bench_function(BenchmarkId::new(name, n), |b| {
        b.iter(|| {
            let mut stream = build();
            assert_eq!(driver.count_to_end(&mut stream), expected_items);
        })
    });
}

/// Push `n` futures that are ready immediately, and drain the selector.
///
/// Measures the fixed cost of a short-lived task: allocation, first poll and removal.
fn ready_futures(c: &mut Criterion) {
    let mut group = c.benchmark_group("ready_futures");

    for n in SIZES {
        group.throughput(Throughput::Elements(n as u64));
        let futures = || (0..n).map(std::future::ready);

        bench_build_and_drain(&mut group, "FutureSelector", n, n, || {
            futures().collect::<FutureSelector<_>>()
        });
        bench_build_and_drain(&mut group, "FutureReclaimSelector", n, n, || {
            futures().collect::<FutureReclaimSelector<_>>()
        });
        bench_build_and_drain(&mut group, "FuturesUnordered", n, n, || {
            futures().collect::<FuturesUnordered<_>>()
        });
        bench_build_and_drain(&mut group, "StreamMap", n, n, || {
            futures()
                .map(stream::once)
                .enumerate()
                .collect::<StreamMap<_, _>>()
        });
    }

    group.finish();
}

/// Drain `n` streams, each producing [`ITEMS_PER_TASK`] items that are ready immediately.
///
/// [`SelectAll`] allocates a new task for each produced item, selectors from this crate do not.
fn ready_streams(c: &mut Criterion) {
    let mut group = c.benchmark_group("ready_streams");

    for n in SIZES {
        group.throughput(Throughput::Elements((n * ITEMS_PER_TASK) as u64));
        let streams = || (0..n).map(|_| ReadyStream::new(ITEMS_PER_TASK));

        bench_drain(&mut group, "StreamSelector", n, n * ITEMS_PER_TASK, || {
            streams().collect::<StreamSelector<_>>()
        });
        bench_drain(
            &mut group,
            "StreamWithIdSelector",
            n,
            n * ITEMS_PER_TASK,
            || streams().collect::<StreamWithIdSelector<_>>(),
        );
        bench_drain(
            &mut group,
            "StreamReclaimSelector",
            n,
            // One more item for each stream: the exhausted stream itself.
            n * (ITEMS_PER_TASK + 1),
            || streams().collect::<StreamReclaimSelector<_>>(),
        );
        bench_drain(&mut group, "SelectAll", n, n * ITEMS_PER_TASK, || {
            streams().collect::<SelectAll<_>>()
        });
        bench_drain(&mut group, "StreamMap", n, n * ITEMS_PER_TASK, || {
            streams().enumerate().collect::<StreamMap<_, _>>()
        });
    }

    group.finish();
}

/// Drain `n` streams, each producing [`ITEMS_PER_TASK`] items,
/// and waking itself before producing each item.
///
/// Measures the hot path: wakeup, enqueue, dequeue and poll.
fn yield_streams(c: &mut Criterion) {
    let mut group = c.benchmark_group("yield_streams");

    for n in SIZES {
        group.throughput(Throughput::Elements((n * ITEMS_PER_TASK) as u64));
        let streams = || (0..n).map(|_| YieldStream::new(ITEMS_PER_TASK));

        bench_drain(&mut group, "StreamSelector", n, n * ITEMS_PER_TASK, || {
            streams().collect::<StreamSelector<_>>()
        });
        bench_drain(&mut group, "SelectAll", n, n * ITEMS_PER_TASK, || {
            streams().collect::<SelectAll<_>>()
        });
        bench_drain(&mut group, "StreamMap", n, n * ITEMS_PER_TASK, || {
            streams().enumerate().collect::<StreamMap<_, _>>()
        });
    }

    group.finish();
}

/// Drain `n` futures, each waking itself [`ITEMS_PER_TASK`] times before resolving.
///
/// Measures the hot path: wakeup, enqueue, dequeue and poll.
fn yield_futures(c: &mut Criterion) {
    let mut group = c.benchmark_group("yield_futures");

    for n in SIZES {
        group.throughput(Throughput::Elements((n * ITEMS_PER_TASK) as u64));
        let futures = || (0..n).map(|_| YieldFuture::new(ITEMS_PER_TASK));

        bench_drain(&mut group, "FutureSelector", n, n, || {
            futures().collect::<FutureSelector<_>>()
        });
        bench_drain(&mut group, "FuturesUnordered", n, n, || {
            futures().collect::<FuturesUnordered<_>>()
        });
        bench_drain(&mut group, "StreamMap", n, n, || {
            futures()
                .map(stream::once)
                .enumerate()
                .collect::<StreamMap<_, _>>()
        });
    }

    group.finish();
}

/// Keep `n` short-lived futures in flight, replacing each completed future with a new one,
/// until `16 * n` futures complete.
///
/// Models a server that handles a stream of short requests.
///
/// [`StreamMap`] insertion is O(n), so it is only benchmarked with smaller sizes.
fn churn(c: &mut Criterion) {
    const YIELDS: usize = 4;
    const ROUNDS: usize = 16;

    let mut group = c.benchmark_group("churn");

    for n in SIZES {
        let total = n * ROUNDS;
        group.throughput(Throughput::Elements(total as u64));
        let future = || YieldFuture::new(YIELDS);

        bench_churn(
            &mut group,
            "FutureSelector",
            n,
            total,
            || (0..n).map(|_| future()).collect::<FutureSelector<_>>(),
            |selector, _| {
                selector.push(future());
            },
        );
        bench_churn(
            &mut group,
            "FuturesUnordered",
            n,
            total,
            || (0..n).map(|_| future()).collect::<FuturesUnordered<_>>(),
            |selector, _| selector.push(future()),
        );
        if n <= 1024 {
            bench_churn(
                &mut group,
                "StreamMap",
                n,
                total,
                || {
                    (0..n)
                        .map(|key| (key, stream::once(future())))
                        .collect::<StreamMap<_, _>>()
                },
                |map, key| {
                    map.insert(key, stream::once(future()));
                },
            );
        }
    }

    group.finish();
}

/// Benchmarks [`churn`] for one selector.
///
/// `push` receives a unique number for the new task, which can be used as a key.
fn bench_churn<S, B, P>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    n: usize,
    total: usize,
    build: B,
    push: P,
) where
    S: Stream + Unpin,
    B: Fn() -> S,
    P: Fn(&mut S, usize),
{
    let driver = Driver::default();
    group.bench_function(BenchmarkId::new(name, n), |b| {
        b.iter_batched(
            &build,
            |mut selector| {
                let mut pushed = n;
                while let Some(Some(output)) = driver.next(&mut selector) {
                    black_box(output);
                    if pushed < total {
                        push(&mut selector, pushed);
                        pushed += 1;
                    }
                }
                assert_eq!(pushed, total);
                selector
            },
            BatchSize::LargeInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = common::config();
    targets = ready_futures, ready_streams, yield_streams, yield_futures, churn
}
criterion_main!(benches);
