//! Single-threaded polling throughput.

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
    stream::{FuturesUnordered, SelectAll},
};

/// Benchmarks draining a stream built with `build`.
///
/// Building and dropping the stream is not measured.
fn bench_drain<S, F>(group: &mut BenchmarkGroup<'_, WallTime>, name: &str, n: usize, build: F)
where
    S: Stream + Unpin,
    F: Fn() -> S,
{
    let driver = Driver::default();
    group.bench_function(BenchmarkId::new(name, n), |b| {
        b.iter_batched(
            &build,
            |mut stream| {
                black_box(driver.count_to_end(&mut stream));
                stream
            },
            BatchSize::LargeInput,
        )
    });
}

/// Push `n` futures that are ready immediately, and drain the selector.
///
/// Measures the fixed cost of a short-lived task: allocation, first poll and removal.
fn ready_futures(c: &mut Criterion) {
    let mut group = c.benchmark_group("ready_futures");

    for n in SIZES {
        group.throughput(Throughput::Elements(n as u64));
        let driver = Driver::default();

        group.bench_function(BenchmarkId::new("FutureSelector", n), |b| {
            b.iter(|| {
                let mut selector = (0..n)
                    .map(std::future::ready)
                    .collect::<FutureSelector<_>>();
                driver.count_to_end(&mut selector)
            })
        });

        group.bench_function(BenchmarkId::new("FutureReclaimSelector", n), |b| {
            b.iter(|| {
                let mut selector = (0..n)
                    .map(std::future::ready)
                    .collect::<FutureReclaimSelector<_>>();
                driver.count_to_end(&mut selector)
            })
        });

        group.bench_function(BenchmarkId::new("FuturesUnordered", n), |b| {
            b.iter(|| {
                let mut selector = (0..n)
                    .map(std::future::ready)
                    .collect::<FuturesUnordered<_>>();
                driver.count_to_end(&mut selector)
            })
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

        bench_drain(&mut group, "StreamSelector", n, || {
            streams().collect::<StreamSelector<_>>()
        });
        bench_drain(&mut group, "StreamWithIdSelector", n, || {
            streams().collect::<StreamWithIdSelector<_>>()
        });
        bench_drain(&mut group, "StreamReclaimSelector", n, || {
            streams().collect::<StreamReclaimSelector<_>>()
        });
        bench_drain(&mut group, "SelectAll", n, || {
            streams().collect::<SelectAll<_>>()
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

        bench_drain(&mut group, "StreamSelector", n, || {
            streams().collect::<StreamSelector<_>>()
        });
        bench_drain(&mut group, "SelectAll", n, || {
            streams().collect::<SelectAll<_>>()
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

        bench_drain(&mut group, "FutureSelector", n, || {
            futures().collect::<FutureSelector<_>>()
        });
        bench_drain(&mut group, "FuturesUnordered", n, || {
            futures().collect::<FuturesUnordered<_>>()
        });
    }

    group.finish();
}

/// Keep `n` short-lived futures in flight, replacing each completed future with a new one,
/// until `16 * n` futures complete.
///
/// Models a server that handles a stream of short requests.
fn churn(c: &mut Criterion) {
    const YIELDS: usize = 4;
    const ROUNDS: usize = 16;

    let mut group = c.benchmark_group("churn");

    for n in SIZES {
        let total = n * ROUNDS;
        group.throughput(Throughput::Elements(total as u64));
        let driver = Driver::default();

        group.bench_function(BenchmarkId::new("FutureSelector", n), |b| {
            b.iter_batched(
                || {
                    (0..n)
                        .map(|_| YieldFuture::new(YIELDS))
                        .collect::<FutureSelector<_>>()
                },
                |mut selector| {
                    let mut pushed = n;
                    while let Some(Some(output)) = driver.next(&mut selector) {
                        black_box(output);
                        if pushed < total {
                            selector.push(YieldFuture::new(YIELDS));
                            pushed += 1;
                        }
                    }
                    selector
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(BenchmarkId::new("FuturesUnordered", n), |b| {
            b.iter_batched(
                || {
                    (0..n)
                        .map(|_| YieldFuture::new(YIELDS))
                        .collect::<FuturesUnordered<_>>()
                },
                |mut selector| {
                    let mut pushed = n;
                    while let Some(Some(output)) = driver.next(&mut selector) {
                        black_box(output);
                        if pushed < total {
                            selector.push(YieldFuture::new(YIELDS));
                            pushed += 1;
                        }
                    }
                    selector
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = common::config();
    targets = ready_futures, ready_streams, yield_streams, yield_futures, churn
}
criterion_main!(benches);
