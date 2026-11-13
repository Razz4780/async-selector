//! Cost of removing tasks by key.

mod common;

use async_selector::StreamSelector;
use common::{Driver, shuffled};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::{
    future::AbortHandle,
    stream::{self, SelectAll},
};
use tokio_stream::StreamMap;

/// Hold `n` idle streams, and remove all of them in random order.
///
/// [`StreamMap`] removal is O(n), so the sizes here are smaller than in other benchmarks.
fn remove_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("remove_all");

    for n in [64, 1024, 4096] {
        group.throughput(Throughput::Elements(n as u64));
        let order = shuffled(n);
        let driver = Driver::default();

        // Removal is O(1). Afterwards, the selector is polled once,
        // so that it observes the removals and frees the memory.
        group.bench_function(BenchmarkId::new("StreamSelector", n), |b| {
            b.iter_batched(
                || {
                    let mut selector = StreamSelector::default();
                    let ids = (0..n)
                        .map(|_| selector.push(stream::pending::<usize>()).id().clone())
                        .collect::<Vec<_>>();
                    assert!(!driver.drain(&mut selector, |_| {}));
                    let ids = order.iter().map(|&i| ids[i].clone()).collect::<Vec<_>>();
                    (selector, ids)
                },
                |(mut selector, ids)| {
                    for id in &ids {
                        selector.remove(id).unwrap();
                    }
                    assert_eq!(driver.next(&mut selector), Some(None));
                    (selector, ids)
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(BenchmarkId::new("StreamMap", n), |b| {
            b.iter_batched(
                || {
                    let mut map = StreamMap::new();
                    for key in 0..n {
                        map.insert(key, stream::pending::<usize>());
                    }
                    map
                },
                |mut map| {
                    for key in &order {
                        drop(map.remove(key).unwrap());
                    }
                    map
                },
                BatchSize::LargeInput,
            )
        });

        // The "remove by key" pattern for `SelectAll`: abort the stream through its handle,
        // and let the `SelectAll` drop it on the next poll.
        group.bench_function(BenchmarkId::new("SelectAll+Abortable", n), |b| {
            b.iter_batched(
                || {
                    let (handles, mut selector): (Vec<AbortHandle>, SelectAll<_>) = (0..n)
                        .map(|_| {
                            let (stream, handle) = stream::abortable(stream::pending::<usize>());
                            (handle, stream)
                        })
                        .unzip();
                    assert!(!driver.drain(&mut selector, |_| {}));
                    let handles = order
                        .iter()
                        .map(|&i| handles[i].clone())
                        .collect::<Vec<_>>();
                    (selector, handles)
                },
                |(mut selector, handles)| {
                    for handle in &handles {
                        handle.abort();
                    }
                    assert_eq!(driver.count_to_end(&mut selector), 0);
                    (selector, handles)
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
    targets = remove_all
}
criterion_main!(benches);
