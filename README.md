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
They compare the selectors with `FuturesUnordered`/`SelectAll` from `futures`,
and with `StreamMap` from `tokio-stream`. Run them with `cargo bench`.

Selected results, obtained with:
* rustc 1.100.0
* 13th Gen Intel(R) Core(TM) i9-13900HX (32 threads)
* Linux, glibc allocator

`StreamMap` holds futures wrapped with `futures::stream::once`.

| Benchmark | `async-selector` | `futures` | `tokio-stream` |
|---|---|---|---|
| `ready_streams`: 1k streams, 64 immediately ready items each | `StreamSelector` 1.14 ms | `SelectAll` 3.53 ms (3.1x) | `StreamMap` 143 µs (0.13x) |
| `yield_streams`: 1k streams, 64 items each, self-wake before each item | `StreamSelector` 2.58 ms | `SelectAll` 6.71 ms (2.6x) | `StreamMap` 787 µs (0.31x) |
| `yield_futures`: 1k futures, each self-waking 64 times | `FutureSelector` 1.44 ms | `FuturesUnordered` 3.14 ms (2.2x) | `StreamMap` 205 µs (0.14x) |
| `ready_futures`: push and resolve 1k ready futures | `FutureSelector` 50.9 µs | `FuturesUnordered` 70.5 µs (1.4x) | `StreamMap` 117 µs (2.3x) |
| `churn`: 1k short-lived futures in flight, 16k completions | `FutureSelector` 2.07 ms | `FuturesUnordered` 3.90 ms (1.9x) | `StreamMap` 4.33 ms (2.1x) |
| `sparse_wakeups`: wake and drain 1 of 16k idle streams | `StreamSelector` 108 ns | `SelectAll` 191 ns (1.8x) | `StreamMap` 612 µs (5600x) |
| `cross_thread`: 4 threads wake 1k streams, drained concurrently | `StreamSelector` 114 µs | `SelectAll` 230 µs (2.0x) | `StreamMap` 411 µs (3.6x) |
| `parallel/ready_streams`: like `ready_streams`, 32 instances on 32 threads (time per instance) | `StreamSelector` 1.62 ms | `SelectAll` 6.31 ms (3.9x) | `StreamMap` 287 µs (0.18x) |
| `remove_all`: remove 4k idle streams in random order | `StreamSelector` 34.8 µs | `SelectAll` + `Abortable` 333 µs (9.6x) | `StreamMap` 777 µs (22x) |

`StreamMap` keeps its streams in a `Vec`, and polls them one by one on every wakeup.
This makes it the fastest option when all tasks are ready all the time,
but it comes with overhead on each wakeup, insertion and removal.
