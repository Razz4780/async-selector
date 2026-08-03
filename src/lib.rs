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
//! Inspired by [`FuturesUnordered`](futures::stream::FuturesUnordered), but more flexible and optimized.
//! Provides optimal performance when polling a large number of tasks
//! (see [example](https://github.com/Razz4780/async-selector/blob/main/examples/speed.rs)).
//!
//! Allows for:
//! * Polling multiple tasks concurrently on the same thread
//! * Safe injection of mutable shared state into the polling logic,
//!   meaning that caller code can provide a custom polling function with a strongly typed context
//! * O(1) task access and removal with unique IDs
//!
//! The main struct is [`Selector`], which works with any [`Task`](crate::task::Task) implementor.
//! For convenience, this crate exposes multiple specializations of the selector,
//! including [`FutureSelector`] and [`StreamSelector`] (*almost* API-compatible with
//! [`FuturesUnordered`](futures::stream::FuturesUnordered)/[`SelectAll`](futures::stream::SelectAll)).
//!
//! ## Examples
//!
//! Simply flatten a set of streams:
//!
//! ```rust
//! # use async_selector::StreamSelector;
//! # use futures::{StreamExt, channel::mpsc};
//! #
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let mut selector = StreamSelector::default();
//! for i in 0..3 {
//!     let stream = futures::stream::repeat(i);
//!     selector.push(stream);
//! }
//! let collected = (&mut selector).take(6).collect::<Vec<_>>().await;
//! assert_eq!(
//!     collected,
//!     vec![0, 1, 2, 0, 1, 2],
//! );
//! # }
//! ```
//!
//! Use as a map of streams:
//!
//! ```rust
//! # use async_selector::StreamWithIdSelector;
//! # use futures::{SinkExt, StreamExt, channel::mpsc};
//! #
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let mut selector = StreamWithIdSelector::default();
//! let ids = (0..3)
//!     .map(|i| {
//!         let stream = futures::stream::repeat(i);
//!         selector.push(stream).id().clone()
//!     })
//!     .collect::<Vec<_>>();
//! let item = selector.next().await.unwrap();
//! assert_eq!(item.0, ids[0]);
//! assert_eq!(item.1, 0);
//! selector.remove(&ids[1]);
//! let item = selector.next().await.unwrap();
//! assert_eq!(item.0, ids[2]);
//! assert_eq!(item.1, 2);
//! # }
//! ```
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
//! [Here](https://github.com/Razz4780/async-selector/tree/main/examples/speed.rs) lives the source code
//! of an example used to compare performance. It is not a proper benchmark,
//! but strongly suggests that this implementation is at least as fast as
//! [`FuturesUnordered`](futures::stream::FuturesUnordered).
//!
//! The results below were obtained with:
//! * rustc 1.96.0
//! * 13th Gen Intel(R) Core(TM) i9-13900HX
//! * Tokio runtime with 32 worker threads
//! * `cargo run --example speed --profile release`
//!
//! **Scenario 1**: concurrently drain 32 instances, each draining 1k streams, each stream producing 16k values (yielding once before producing each value)
//!
//! * [`StreamSelector`] - 2.522s
//! * [`SelectAll`](futures::stream::SelectAll) - 5.979s
//!
//! **Scenario 2**: concurrently drain 32 instances, each resolving 1k futures, each future yielding 16k times
//!
//! * [`FutureSelector`] - 1.479s
//! * [`FuturesUnordered`](futures::stream::FuturesUnordered) - 4.416s

#![deny(unused_crate_dependencies)]

use crate::{selector::Selector, task::strategy};

mod list;
mod queue;
pub mod selector;
pub mod task;

/// [`Selector`] using [`strategy::FutureBasic`].
pub type FutureSelector<F> = Selector<F, strategy::FutureBasic>;

/// [`Selector`] using [`strategy::FutureReclaim`].
pub type FutureReclaimSelector<F> = Selector<F, strategy::FutureReclaim>;

/// [`Selector`] using [`strategy::StreamBasic`].
pub type StreamSelector<S> = Selector<S, strategy::StreamBasic>;

/// [`Selector`] using [`strategy::StreamWithId`].
pub type StreamWithIdSelector<S> = Selector<S, strategy::StreamWithId>;

/// [`Selector`] using [`strategy::StreamReclaim`].
pub type StreamReclaimSelector<S> = Selector<S, strategy::StreamReclaim>;

/// [`Selector`] using [`strategy::TryStreamBasic`].
pub type TryStreamSelector<S> = Selector<S, strategy::TryStreamBasic>;

/// [`Selector`] using [`strategy::TryStreamWithId`].
pub type TryStreamWithIdSelector<S> = Selector<S, strategy::TryStreamWithId>;

/// [`Selector`] using [`strategy::TryStreamReclaim`].
pub type TryStreamReclaimSelector<S> = Selector<S, strategy::TryStreamReclaim>;
