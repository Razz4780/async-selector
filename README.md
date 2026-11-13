# async-selector

[![crates.io](https://img.shields.io/crates/v/async-selector.svg)](https://crates.io/crates/async-selector)
[![Released API docs](https://docs.rs/async-selector/badge.svg)](https://docs.rs/async-selector)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![CI](https://github.com/Razz4780/async-selector/actions/workflows/ci.yaml/badge.svg)](https://github.com/Razz4780/async-selector/actions/workflows/ci.yaml)
[![MSRV](https://img.shields.io/crates/msrv/async-selector)](https://crates.io/crates/async-selector)

Fast and flexible selector for asynchronous tasks (generalized `Future`s and `Stream`s).

A selector holds a dynamic set of tasks and polls them all concurrently, on the same thread.
You push tasks into it, and then use it as a single `Stream`
that yields the tasks' results as soon as they are ready.

It is inspired by [`FuturesUnordered`](https://docs.rs/futures/latest/futures/stream/struct.FuturesUnordered.html),
but it is more flexible, and faster when polling a large number of tasks
(see [performance](#performance)).

## Features

* Polls many tasks concurrently on the same thread, without spawning them.
* Only polls the tasks that were woken up, in a round-robin fashion.
* Gives each task a unique ID, which allows for O(1) access and removal.
* Lets you inject mutable shared state into the polling logic
  (e.g. a buffer shared by all tasks), without any synchronization or `unsafe` code.
  See [`Task`](https://docs.rs/async-selector/latest/async_selector/task/trait.Task.html) for details.

## Quick start

Poll a set of futures, and receive their outputs in completion order:

```rust
use async_selector::FutureSelector;
use futures::{StreamExt, channel::oneshot};

let mut selector = FutureSelector::default();

// Each oneshot receiver is a future that resolves when its sender sends a value.
let mut senders = Vec::new();
for _ in 0..3 {
    let (tx, rx) = oneshot::channel::<&str>();
    senders.push(tx);
    selector.push(rx);
}
assert_eq!(selector.len(), 3);

// The selector is a `Stream` of the futures' outputs,
// so you can await the next one with `StreamExt::next`.
senders.pop().unwrap().send("third").unwrap();
assert_eq!(selector.next().await, Some(Ok("third")));

// A future is dropped as soon as it resolves.
assert_eq!(selector.len(), 2);

senders.remove(0).send("first").unwrap();
assert_eq!(selector.next().await, Some(Ok("first")));
```

Merge a set of streams into one:

```rust
use async_selector::StreamSelector;
use futures::StreamExt;

// Push three infinite streams that repeat 0, 1, and 2 respectively.
let mut selector = StreamSelector::default();
for i in 0..3 {
    selector.push(futures::stream::repeat(i));
}

// Ready streams take turns, so no stream can starve the others.
let items = (&mut selector).take(6).collect::<Vec<_>>().await;
assert_eq!(items, [0, 1, 2, 0, 1, 2]);
```

## Choosing a selector

The main type is `Selector`, which can hold any type implementing `Task`.
What the selector yields is decided by its *strategy*.
The strategies that cover the common cases live in the `task::strategy` module,
and the crate root exposes a type alias for each of them:

| Alias                      | Holds        | Yields                                                             | When a task finishes               |
|----------------------------|--------------|--------------------------------------------------------------------|------------------------------------|
| `FutureSelector`           | `Future`s    | `F::Output`                                                        | it's dropped                       |
| `FutureReclaimSelector`    | `Future`s    | `(Removed<F>, F::Output)`                                          | it's returned with the output      |
| `StreamSelector`           | `Stream`s    | `S::Item`                                                          | it's dropped                       |
| `StreamWithIdSelector`     | `Stream`s    | `(Id<S>, S::Item)`                                                 | it's dropped                       |
| `StreamReclaimSelector`    | `Stream`s    | `ControlFlow<Removed<S>, (Id<S>, S::Item)>`                        | it's returned as `Break`           |
| `TryStreamSelector`        | `TryStream`s | `Result<S::Ok, S::Error>`                                          | it's dropped (also after an error) |
| `TryStreamWithIdSelector`  | `TryStream`s | `(Id<S>, Result<S::Ok, S::Error>)`                                 | it's dropped (also after an error) |
| `TryStreamReclaimSelector` | `TryStream`s | `ControlFlow<(Removed<S>, Result<(), S::Error>), (Id<S>, S::Ok)>`  | it's returned as `Break`           |

`TryStream` selectors stop polling a stream after its first error.

`FutureSelector` and `StreamSelector` are *almost* API-compatible with
`FuturesUnordered` and `SelectAll`, so they can serve as drop-in replacements.

If none of the strategies fit your use case, you can implement `Task` with your own strategy
(see the [custom task example](https://github.com/Razz4780/async-selector/blob/main/examples/custom.rs)).

## Working with task IDs

Every task pushed into a selector gets a unique `Id`.
You can use it to access or remove the task later,
and the `*WithId` selectors attach it to each yielded item,
so you know which task produced it.

```rust
use async_selector::StreamWithIdSelector;
use futures::{StreamExt, channel::mpsc};

let mut selector = StreamWithIdSelector::default();

// `push` returns a reference to the stored task, which knows the task's ID.
let (tx_a, rx_a) = mpsc::unbounded::<&str>();
let (tx_b, rx_b) = mpsc::unbounded::<&str>();
let id_a = selector.push(rx_a).id().clone();
let id_b = selector.push(rx_b).id().clone();

// Each item is paired with the ID of the stream that produced it.
tx_b.unbounded_send("hello from b").unwrap();
let (id, item) = selector.next().await.unwrap();
assert_eq!(id, id_b);
assert_eq!(item, "hello from b");

// Remove a stream by its ID. This gives the stream back to you.
let removed = selector.remove(&id_a).unwrap();
assert!(!selector.contains(&id_a));
let rx_a = removed.into_inner();
```

For a bigger example, see the
[map example](https://github.com/Razz4780/async-selector/blob/main/examples/map.rs),
which builds a stream map keyed by arbitrary keys on top of `StreamSelector`.

## Things to keep in mind

* **A task is only polled after it is woken up** (or right after it is pushed,
  or right after it yields an item).
  If you change a task or the strategy in a way that should make the task ready,
  wake it manually with `Id::wake` or `Selector::wake_all`.
* **The selector yields `None` when it is empty**, but it is not exhausted for good.
  You can push more tasks and keep polling.
* **Every task lives in its own heap allocation.**
  An `Id` keeps that allocation alive (but not the task itself),
  so don't keep IDs of removed tasks around forever.

More examples live [here](https://github.com/Razz4780/async-selector/tree/main/examples),
and the full API documentation is on [docs.rs](https://docs.rs/async-selector).

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

| Benchmark | `async-selector` | `futures 0.3.34` | `tokio-stream 0.1.19`<sup>1, 2</sup> |
|---|---|---|---|
| `ready_streams`: 1k streams, 64 immediately ready items each | `StreamSelector` 1.14 ms | `SelectAll` 3.53 ms (3.1x) | `StreamMap` 143 µs (0.13x) |
| `parallel/ready_streams`: like `ready_streams`, 32 instances on 32 threads (time per instance) | `StreamSelector` 1.62 ms | `SelectAll` 6.31 ms (3.9x) | `StreamMap` 287 µs (0.18x) |
| `yield_streams`: 1k streams, 64 items each, self-wake before each item | `StreamSelector` 2.58 ms | `SelectAll` 6.71 ms (2.6x) | `StreamMap` 787 µs (0.31x) |
| `parallel/yield_streams`: like `yield_streams`, 32 instances on 32 threads (time per instance) | `StreamSelector` 4.17 ms | `SelectAll` 12.2 ms (2.9x) | `StreamMap` 1.28 ms (0.31x) |
| `yield_futures`: 1k futures, each self-waking 64 times | `FutureSelector` 1.44 ms | `FuturesUnordered` 3.14 ms (2.2x) | `StreamMap` 205 µs (0.14x) |
| `parallel/yield_futures`: like `yield_futures`, 32 instances on 32 threads (time per instance) | `FutureSelector` 2.67 ms | `FuturesUnordered` 5.71 ms (2.1x) | `StreamMap` 279 µs (0.10x) |
| `ready_futures`: push and resolve 1k ready futures | `FutureSelector` 50.9 µs | `FuturesUnordered` 70.5 µs (1.4x) | `StreamMap` 117 µs (2.3x) |
| `churn`: 1k short-lived futures in flight, 16k completions | `FutureSelector` 2.07 ms | `FuturesUnordered` 3.90 ms (1.9x) | `StreamMap` 4.33 ms (2.1x) |
| `sparse_wakeups`: wake and drain 1 of 16k idle streams | `StreamSelector` 108 ns | `SelectAll` 191 ns (1.8x) | `StreamMap` 612 µs (5600x) |
| `cross_thread`: 4 threads wake 1k streams, drained concurrently | `StreamSelector` 114 µs | `SelectAll` 230 µs (2.0x) | `StreamMap` 411 µs (3.6x) |
| `remove_all`: remove 4k idle streams in random order | `StreamSelector` 34.8 µs | `SelectAll` + `Abortable` 333 µs (9.6x) | `StreamMap` 777 µs (22x) |

1. When holding futures, `StreamMap` wraps them with `futures::stream::once`.
2. `StreamMap` keeps its streams in a `Vec`, and polls them one by one on every wakeup. This makes it the fastest option when all tasks are ready all the time, but it comes with overhead on each wakeup, insertion and removal. The overhead grows with the size of the map.
