//! Independent selector instances running concurrently, one per OS thread.
//!
//! The instances share nothing but the global allocator. A selector that allocates in its hot path
//! (like [`SelectAll`], which allocates a new task for every item produced by a stream)
//! competes with the other instances for the allocator, and scales worse with the number of threads.
//!
//! The reported time is the average time it takes **one instance** to drain its tasks.
//! With perfect scaling, it stays flat as the number of threads grows.

mod common;

use std::{
    hint::black_box,
    sync::Barrier,
    thread,
    time::{Duration, Instant},
};

use async_selector::{FutureSelector, StreamSelector};
use common::{Driver, ReadyStream, YieldFuture, YieldStream};
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
    measurement::WallTime,
};
use futures::{
    Stream,
    stream::{FuturesUnordered, SelectAll},
};

/// Number of tasks in one selector instance.
const TASKS: usize = 1024;

/// Number of items produced by one task.
const ITEMS_PER_TASK: usize = 64;

/// Thread counts to benchmark: powers of two up to the available parallelism,
/// and the available parallelism itself.
fn thread_counts() -> Vec<usize> {
    let max = thread::available_parallelism().map_or(1, |n| n.get());
    let mut counts = std::iter::successors(Some(1_usize), |n| n.checked_mul(2))
        .take_while(|&n| n < max)
        .collect::<Vec<_>>();
    counts.push(max);
    counts
}

/// Runs `iters` iterations on each of `threads` threads concurrently,
/// and returns the average time spent draining by a single thread.
///
/// Building and dropping the instances is not measured.
fn run_parallel<S, F>(threads: usize, iters: u64, build: &F) -> Duration
where
    S: Stream + Unpin,
    F: Fn() -> S + Sync,
{
    let barrier = Barrier::new(threads);
    let total: Duration = thread::scope(|scope| {
        let handles = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let driver = Driver::default();
                    barrier.wait();
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let mut stream = build();
                        let start = Instant::now();
                        black_box(driver.count_to_end(&mut stream));
                        elapsed += start.elapsed();
                        drop(stream);
                    }
                    elapsed
                })
            })
            .collect::<Vec<_>>();
        handles.into_iter().map(|h| h.join().unwrap()).sum()
    });
    total / u32::try_from(threads).unwrap()
}

fn bench_parallel<S, F>(group: &mut BenchmarkGroup<'_, WallTime>, name: &str, build: F)
where
    S: Stream + Unpin,
    F: Fn() -> S + Sync,
{
    for threads in thread_counts() {
        group.bench_function(BenchmarkId::new(name, threads), |b| {
            b.iter_custom(|iters| run_parallel(threads, iters, &build))
        });
    }
}

/// Streams that produce items without ever returning [`Poll::Pending`](std::task::Poll::Pending).
///
/// [`SelectAll`] allocates for every item, [`StreamSelector`] does not.
fn ready_streams(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel/ready_streams");
    group.throughput(Throughput::Elements((TASKS * ITEMS_PER_TASK) as u64));
    let streams = || (0..TASKS).map(|_| ReadyStream::new(ITEMS_PER_TASK));

    bench_parallel(&mut group, "StreamSelector", || {
        streams().collect::<StreamSelector<_>>()
    });
    bench_parallel(&mut group, "SelectAll", || {
        streams().collect::<SelectAll<_>>()
    });

    group.finish();
}

/// Streams that wake themselves before producing each item.
///
/// [`SelectAll`] allocates for every item, [`StreamSelector`] does not.
fn yield_streams(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel/yield_streams");
    group.throughput(Throughput::Elements((TASKS * ITEMS_PER_TASK) as u64));
    let streams = || (0..TASKS).map(|_| YieldStream::new(ITEMS_PER_TASK));

    bench_parallel(&mut group, "StreamSelector", || {
        streams().collect::<StreamSelector<_>>()
    });
    bench_parallel(&mut group, "SelectAll", || {
        streams().collect::<SelectAll<_>>()
    });

    group.finish();
}

/// Futures that wake themselves multiple times before resolving.
///
/// Neither selector allocates in the hot path. This is the control group.
fn yield_futures(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel/yield_futures");
    group.throughput(Throughput::Elements((TASKS * ITEMS_PER_TASK) as u64));
    let futures = || (0..TASKS).map(|_| YieldFuture::new(ITEMS_PER_TASK));

    bench_parallel(&mut group, "FutureSelector", || {
        futures().collect::<FutureSelector<_>>()
    });
    bench_parallel(&mut group, "FuturesUnordered", || {
        futures().collect::<FuturesUnordered<_>>()
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = common::config();
    targets = ready_streams, yield_streams, yield_futures
}
criterion_main!(benches);
