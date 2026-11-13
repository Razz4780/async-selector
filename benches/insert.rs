//! Cost of filling a selector with tasks and of dropping it.

mod common;

use std::{future::Pending, hint::black_box, ops::Not};

use async_selector::FutureSelector;
use common::{Driver, SIZES};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::stream::FuturesUnordered;

/// Push `n` tasks into an empty selector.
///
/// The selector is dropped outside of the measurement.
fn push(c: &mut Criterion) {
    let mut group = c.benchmark_group("push");

    for n in SIZES {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_function(BenchmarkId::new("FutureSelector", n), |b| {
            b.iter_with_large_drop(|| {
                let mut selector = FutureSelector::default();
                for _ in 0..n {
                    selector.push(std::future::pending::<usize>());
                }
                selector
            })
        });

        group.bench_function(BenchmarkId::new("FuturesUnordered", n), |b| {
            b.iter_with_large_drop(|| {
                let selector = FuturesUnordered::new();
                for _ in 0..n {
                    selector.push(std::future::pending::<usize>());
                }
                selector
            })
        });
    }

    group.finish();
}

/// Drop a selector that holds `n` idle tasks (all already polled once).
fn drop_idle(c: &mut Criterion) {
    let mut group = c.benchmark_group("drop_idle");

    for n in SIZES {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_function(BenchmarkId::new("FutureSelector", n), |b| {
            let driver = Driver::default();
            b.iter_batched(
                || {
                    let mut selector = (0..n)
                        .map(|_| std::future::pending::<usize>())
                        .collect::<FutureSelector<Pending<usize>>>();
                    assert!(driver.drain(&mut selector, |_| {}).not());
                    selector
                },
                |selector| drop(black_box(selector)),
                BatchSize::LargeInput,
            )
        });

        group.bench_function(BenchmarkId::new("FuturesUnordered", n), |b| {
            let driver = Driver::default();
            b.iter_batched(
                || {
                    let mut selector = (0..n)
                        .map(|_| std::future::pending::<usize>())
                        .collect::<FuturesUnordered<_>>();
                    assert!(driver.drain(&mut selector, |_| {}).not());
                    selector
                },
                |selector| drop(black_box(selector)),
                BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = common::config();
    targets = push, drop_idle
}
criterion_main!(benches);
