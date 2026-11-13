//! # async-selector
//!
//! [![crates.io](https://img.shields.io/crates/v/async-selector.svg)](https://crates.io/crates/async-selector)
//! [![Released API docs](https://docs.rs/async-selector/badge.svg)](https://docs.rs/async-selector)
//! [![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
//! [![CI](https://github.com/Razz4780/async-selector/actions/workflows/ci.yaml/badge.svg)](https://github.com/Razz4780/async-selector/actions/workflows/ci.yaml)
//! [![MSRV](https://img.shields.io/crates/msrv/async-selector)](https://crates.io/crates/async-selector)
//!
//! Fast and flexible selector for asynchronous tasks (generalized [`Future`]s and [`Stream`](futures::Stream)s).
//!
//! A selector holds a dynamic set of tasks and polls them all concurrently, on the same thread.
//! You push tasks into it, and then use it as a single [`Stream`](futures::Stream)
//! that yields the tasks' results as soon as they are ready.
//!
//! It is inspired by [`FuturesUnordered`](futures::stream::FuturesUnordered),
//! but it is more flexible, and faster when polling a large number of tasks
//! (see [benchmarks](https://github.com/Razz4780/async-selector/tree/main/benches)).
//!
//! ## Features
//!
//! * Polls many tasks concurrently on the same thread, without spawning them.
//! * Only polls the tasks that were woken up, in a round-robin fashion.
//! * Gives each task a unique [`Id`](selector::Id), which allows for O(1) access and removal.
//! * Lets you inject mutable shared state into the polling logic
//!   (e.g. a buffer shared by all tasks), without any synchronization or `unsafe` code.
//!   See [`Task`](task::Task) for details.
//!
//! ## Quick start
//!
//! Poll a set of futures, and receive their outputs in completion order:
//!
//! ```rust
//! use async_selector::FutureSelector;
//! use futures::{StreamExt, channel::oneshot};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let mut selector = FutureSelector::default();
//!
//! // Each oneshot receiver is a future that resolves when its sender sends a value.
//! let mut senders = Vec::new();
//! for _ in 0..3 {
//!     let (tx, rx) = oneshot::channel::<&str>();
//!     senders.push(tx);
//!     selector.push(rx);
//! }
//! assert_eq!(selector.len(), 3);
//!
//! // The selector is a `Stream` of the futures' outputs,
//! // so you can await the next one with `StreamExt::next`.
//! senders.pop().unwrap().send("third").unwrap();
//! assert_eq!(selector.next().await, Some(Ok("third")));
//!
//! // A future is dropped as soon as it resolves.
//! assert_eq!(selector.len(), 2);
//!
//! senders.remove(0).send("first").unwrap();
//! assert_eq!(selector.next().await, Some(Ok("first")));
//! # }
//! ```
//!
//! Merge a set of streams into one:
//!
//! ```rust
//! use async_selector::StreamSelector;
//! use futures::StreamExt;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! // Push three infinite streams that repeat 0, 1, and 2 respectively.
//! let mut selector = StreamSelector::default();
//! for i in 0..3 {
//!     selector.push(futures::stream::repeat(i));
//! }
//!
//! // Ready streams take turns, so no stream can starve the others.
//! let items = (&mut selector).take(6).collect::<Vec<_>>().await;
//! assert_eq!(items, [0, 1, 2, 0, 1, 2]);
//! # }
//! ```
//!
//! ## Choosing a selector
//!
//! The main type is [`Selector`], which can hold any type implementing [`Task`](task::Task).
//! What the selector yields is decided by its *strategy*.
//! The strategies that cover the common cases live in [`task::strategy`],
//! and this crate root exposes a type alias for each of them:
//!
//! | Alias                       | Holds                             | Yields                                                              | When a task finishes                 |
//! |-----------------------------|-----------------------------------|---------------------------------------------------------------------|--------------------------------------|
//! | [`FutureSelector`]          | [`Future`]s                       | `F::Output`                                                         | it's dropped                         |
//! | [`FutureReclaimSelector`]   | [`Future`]s                       | `(Removed<F>, F::Output)`                                           | it's returned with the output        |
//! | [`StreamSelector`]          | [`Stream`](futures::Stream)s      | `S::Item`                                                           | it's dropped                         |
//! | [`StreamWithIdSelector`]    | [`Stream`](futures::Stream)s      | `(Id<S>, S::Item)`                                                  | it's dropped                         |
//! | [`StreamReclaimSelector`]   | [`Stream`](futures::Stream)s      | `ControlFlow<Removed<S>, (Id<S>, S::Item)>`                         | it's returned as `Break`             |
//! | [`TryStreamSelector`]       | [`TryStream`](futures::TryStream)s | `Result<S::Ok, S::Error>`                                          | it's dropped (also after an error)   |
//! | [`TryStreamWithIdSelector`] | [`TryStream`](futures::TryStream)s | `(Id<S>, Result<S::Ok, S::Error>)`                                 | it's dropped (also after an error)   |
//! | [`TryStreamReclaimSelector`]| [`TryStream`](futures::TryStream)s | `ControlFlow<(Removed<S>, Result<(), S::Error>), (Id<S>, S::Ok)>`  | it's returned as `Break`             |
//!
//! `TryStream` selectors stop polling a stream after its first error.
//!
//! [`FutureSelector`] and [`StreamSelector`] are *almost* API-compatible with
//! [`FuturesUnordered`](futures::stream::FuturesUnordered) and
//! [`SelectAll`](futures::stream::SelectAll), so they can serve as drop-in replacements.
//!
//! If none of the strategies fit your use case,
//! you can implement [`Task`](task::Task) with your own strategy.
//!
//! ## Working with task IDs
//!
//! Every task pushed into a selector gets a unique [`Id`](selector::Id).
//! You can use it to access or remove the task later,
//! and the `*WithId` selectors attach it to each yielded item,
//! so you know which task produced it.
//!
//! ```rust
//! use async_selector::StreamWithIdSelector;
//! use futures::{StreamExt, channel::mpsc};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let mut selector = StreamWithIdSelector::default();
//!
//! // `push` returns a reference to the stored task, which knows the task's ID.
//! let (tx_a, rx_a) = mpsc::unbounded::<&str>();
//! let (tx_b, rx_b) = mpsc::unbounded::<&str>();
//! let id_a = selector.push(rx_a).id().clone();
//! let id_b = selector.push(rx_b).id().clone();
//!
//! // Each item is paired with the ID of the stream that produced it.
//! tx_b.unbounded_send("hello from b").unwrap();
//! let (id, item) = selector.next().await.unwrap();
//! assert_eq!(id, id_b);
//! assert_eq!(item, "hello from b");
//!
//! // Remove a stream by its ID. This gives the stream back to you.
//! let removed = selector.remove(&id_a).unwrap();
//! assert!(!selector.contains(&id_a));
//! let rx_a = removed.into_inner();
//! # drop((tx_a, rx_a));
//! # }
//! ```
//!
//! For a bigger example, see the
//! [map example](https://github.com/Razz4780/async-selector/blob/main/examples/map.rs),
//! which builds a stream map keyed by arbitrary keys on top of [`StreamSelector`].
//!
//! ## Things to keep in mind
//!
//! * **A task is only polled after it is woken up** (or right after it is pushed,
//!   or right after it yields an item).
//!   If you change a task or the strategy in a way that should make the task ready,
//!   wake it manually with [`Id::wake`](selector::Id::wake) or [`Selector::wake_all`].
//!   See the [`Selector`] docs for details.
//! * **The selector yields [`None`] when it is empty**, but it is not exhausted for good.
//!   You can push more tasks and keep polling.
//! * **Every task lives in its own heap allocation.**
//!   An [`Id`](selector::Id) keeps that allocation alive (but not the task itself),
//!   so don't keep IDs of removed tasks around forever.
//!
//! More examples live [here](https://github.com/Razz4780/async-selector/tree/main/examples).
//!
//! ## Performance
//!
//! The implementation of [`Selector`] is very similar to that of [`FuturesUnordered`](futures::stream::FuturesUnordered).
//! However, some optimizations were made:
//! 1. Reduced the number of CAS instructions
//! 2. Removed repeated memory allocations when polling [`Stream`](futures::Stream)s
//!
//! Benchmark results are available in the
//! [README](https://github.com/Razz4780/async-selector/blob/main/README.md#performance).

#![deny(unused_crate_dependencies)]

// Dev-dependencies used only by benchmarks and integration tests.
#[cfg(test)]
use criterion as _;
#[cfg(test)]
use tokio_stream as _;

use crate::{selector::Selector, task::strategy};

mod list;
mod queue;
pub mod selector;
pub mod task;

/// [`Selector`] over [`Future`]s, which yields their outputs.
///
/// A future is dropped as soon as it resolves.
/// This is the closest equivalent of [`FuturesUnordered`](futures::stream::FuturesUnordered).
///
/// Uses [`strategy::FutureBasic`], see its docs for an example.
pub type FutureSelector<F> = Selector<F, strategy::FutureBasic>;

/// [`Selector`] over [`Future`]s, which yields their outputs together with the resolved futures.
///
/// Each item is a `(Removed<F>, F::Output)` pair.
/// Useful when you want to reuse the future (or some state stored inside it) after it resolves.
///
/// Uses [`strategy::FutureReclaim`], see its docs for an example.
pub type FutureReclaimSelector<F> = Selector<F, strategy::FutureReclaim>;

/// [`Selector`] over [`Stream`](futures::Stream)s, which yields their items.
///
/// A stream is dropped as soon as it is exhausted.
/// This is the closest equivalent of [`SelectAll`](futures::stream::SelectAll).
///
/// Uses [`strategy::StreamBasic`], see its docs for an example.
pub type StreamSelector<S> = Selector<S, strategy::StreamBasic>;

/// [`Selector`] over [`Stream`](futures::Stream)s, which yields their items
/// paired with the [`Id`](selector::Id) of the stream that produced them.
///
/// Each item is an `(Id<S>, S::Item)` pair.
/// A stream is dropped as soon as it is exhausted.
///
/// Uses [`strategy::StreamWithId`], see its docs for an example.
pub type StreamWithIdSelector<S> = Selector<S, strategy::StreamWithId>;

/// [`Selector`] over [`Stream`](futures::Stream)s, which yields their items
/// paired with the [`Id`](selector::Id) of the stream that produced them,
/// and then the exhausted streams themselves.
///
/// Each item is a [`ControlFlow`](std::ops::ControlFlow):
/// * `Continue((Id<S>, S::Item))` for every item produced by a stream
/// * `Break(Removed<S>)` when a stream is exhausted
///
/// Uses [`strategy::StreamReclaim`], see its docs for an example.
pub type StreamReclaimSelector<S> = Selector<S, strategy::StreamReclaim>;

/// [`Selector`] over [`TryStream`](futures::TryStream)s, which yields their items and errors.
///
/// A stream is dropped as soon as it is exhausted or yields an error.
///
/// Uses [`strategy::TryStreamBasic`], see its docs for an example.
pub type TryStreamSelector<S> = Selector<S, strategy::TryStreamBasic>;

/// [`Selector`] over [`TryStream`](futures::TryStream)s, which yields their items and errors
/// paired with the [`Id`](selector::Id) of the stream that produced them.
///
/// Each item is an `(Id<S>, Result<S::Ok, S::Error>)` pair.
/// A stream is dropped as soon as it is exhausted or yields an error.
///
/// Uses [`strategy::TryStreamWithId`], see its docs for an example.
pub type TryStreamWithIdSelector<S> = Selector<S, strategy::TryStreamWithId>;

/// [`Selector`] over [`TryStream`](futures::TryStream)s, which yields their items
/// paired with the [`Id`](selector::Id) of the stream that produced them,
/// and then the finished streams themselves.
///
/// Each item is a [`ControlFlow`](std::ops::ControlFlow):
/// * `Continue((Id<S>, S::Ok))` for every successful item produced by a stream
/// * `Break((Removed<S>, Result<(), S::Error>))` when a stream is exhausted (`Ok`)
///   or yields an error (`Err`)
///
/// Uses [`strategy::TryStreamReclaim`], see its docs for an example.
pub type TryStreamReclaimSelector<S> = Selector<S, strategy::TryStreamReclaim>;
