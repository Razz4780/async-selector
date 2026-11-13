# async-selector

[![crates.io](https://img.shields.io/crates/v/async-selector.svg)](https://crates.io/crates/async-selector)
[![Released API docs](https://docs.rs/async-selector/badge.svg)](https://docs.rs/async-selector)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![CI](https://github.com/Razz4780/async-selector/actions/workflows/ci.yaml/badge.svg)](https://github.com/Razz4780/async-selector/actions/workflows/ci.yaml)
[![MSRV](https://img.shields.io/crates/msrv/async-selector)](https://crates.io/crates/async-selector)

Fast and flexible selector for asynchronous tasks (generalized `Future`s and `Stream`s).

Inspired by [`FuturesUnordered`](https://docs.rs/futures/latest/futures/stream/struct.FuturesUnordered.html),
but more flexible and optimized. Provides optimal performance when polling a large number of tasks
(see [benchmarks](https://github.com/Razz4780/async-selector/tree/main/benches)).

Allows for:
* Polling multiple tasks concurrently on the same thread
* Safe injection of mutable shared state into the polling logic,
  meaning that caller code can provide a custom polling function with a strongly typed context
* O(1) task access and removal with unique IDs

The main struct is `Selector`, which works with any `Task` implementor.
For convenience, this crate exposes multiple specializations of the selector,
including `FutureSelector` and `StreamSelector` (*almost* API-compatible with
`FuturesUnordered`/`SelectAll`).

## Examples

Simply flatten a set of streams:

```rust
let mut selector = StreamSelector::default();
for i in 0..3 {
    let stream = futures::stream::repeat(i);
    selector.push(stream);
}
let collected = (&mut selector).take(6).collect::<Vec<_>>().await;
assert_eq!(
    collected,
    vec![0, 1, 2, 0, 1, 2],
);
```

Use as a map of streams:

```rust
let mut selector = StreamWithIdSelector::default();
let ids = (0..3)
    .map(|i| {
        let stream = futures::stream::repeat(i);
        selector.push(stream).id().clone()
    })
    .collect::<Vec<_>>();
let item = selector.next().await.unwrap();
assert_eq!(item.0, ids[0]);
assert_eq!(item.1, 0);
selector.remove(&ids[1]);
let item = selector.next().await.unwrap();
assert_eq!(item.0, ids[2]);
assert_eq!(item.1, 2);
```

More examples live [here](https://github.com/Razz4780/async-selector/tree/main/examples).

## Performance

The implementation of `Selector` is very similar to that of `FuturesUnordered` – they both use an [intrusive MPSC node-based queue](https://sites.google.com/site/1024cores/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue).
However, some optimizations were made:
1. Reduced the number of CAS instructions
2. Removed repeated memory allocations when polling `Stream`s

The crate comes with a set of [criterion](https://docs.rs/criterion) benchmarks, which live
[here](https://github.com/Razz4780/async-selector/tree/main/benches).
They compare the selectors with their counterparts from `futures`
(and `tokio-stream` where keyed removal is available). Run them with `cargo bench`.

Selected results (median times), obtained with:
* rustc 1.100.0
* 13th Gen Intel(R) Core(TM) i9-13900HX (32 threads)
* Linux, glibc allocator

| Benchmark | `async-selector` | Alternative | Speedup |
|---|---|---|---|
| `ready_streams`: 1k streams, 64 immediately ready items each | `StreamSelector` 1.14 ms | `SelectAll` 3.53 ms | 3.1x |
| `yield_streams`: 1k streams, 64 items each, self-wake before each item | `StreamSelector` 2.56 ms | `SelectAll` 6.84 ms | 2.7x |
| `yield_futures`: 1k futures, each self-waking 64 times | `FutureSelector` 1.54 ms | `FuturesUnordered` 3.29 ms | 2.1x |
| `ready_futures`: push and resolve 1k ready futures | `FutureSelector` 51.9 µs | `FuturesUnordered` 71.3 µs | 1.4x |
| `churn`: 1k short-lived futures in flight, 16k completions | `FutureSelector` 2.05 ms | `FuturesUnordered` 3.91 ms | 1.9x |
| `sparse_wakeups`: wake and drain 1 of 16k idle streams | `StreamSelector` 107 ns | `SelectAll` 192 ns | 1.8x |
| `cross_thread`: 4 threads wake 1k streams, drained concurrently | `StreamSelector` 113 µs | `SelectAll` 242 µs | 2.1x |
| `parallel/ready_streams`: like `ready_streams`, 32 instances on 32 threads (time per instance) | `StreamSelector` 1.73 ms | `SelectAll` 6.04 ms | 3.5x |
| `remove_all`: remove 4k idle streams in random order | `StreamSelector` 34.9 µs | `StreamMap` (tokio-stream) 779 µs | 22x |
