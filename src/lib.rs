//! Fast and flexible [`Future`]/[`Stream`](futures::Stream)/task selector.
//!
//! Designed for optimal performance when polling a large number of tasks
//! (see [example](https://github.com/Razz4780/async-selector/blob/main/examples/speed.rs)).
//!
//! Allows for:
//! 1. Polling multiple tasks concurrently on the same thread
//! 2. Safely injecting shared state into polling logic (see [`Pollable`](crate::pollable::Pollable))
//! 3. Accessing and removing the tasks by automatically assigned unique ids
//!
//! # Examples
//!
//! Simply flatten a set of streams:
//!
//! ```
//! # use async_selector::StreamSelector;
//! # use futures::{StreamExt, channel::mpsc};
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let mut selector = StreamSelector::default();
//! (0..5).for_each(|i| {
//!     let (tx, rx) = mpsc::unbounded();
//!     selector.push(rx);
//!     tx.unbounded_send(i).unwrap();
//! });
//! let collected = selector.collect::<Vec<_>>().await;
//! assert_eq!(
//!     collected,
//!     vec![0, 1, 2, 3, 4],
//! );
//! # }
//! ```
//!
//! Use as a map of streams:
//!
//! ```
//! # use async_selector::StreamSelector;
//! # use futures::{SinkExt, StreamExt, channel::mpsc};
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let mut selector = StreamSelector::default();
//! let txs = (0..10)
//!     .map(|_| {
//!         let (tx, rx) = mpsc::channel::<()>(8);
//!         let id = selector.push_with_id(rx);
//!         (tx, id)
//!     })
//!     .collect::<Vec<_>>();
//! for (mut tx, saved_id) in txs {
//!     tx.send(()).await.unwrap();
//!     let ((), received_id) = selector.with_id().next().await.unwrap();
//!     assert_eq!(received_id, saved_id);
//! }
//! # }
//! ```
//!
//! More examples live [here](https://github.com/Razz4780/async-selector/tree/main/examples).

#![deny(unused_crate_dependencies)]

use crate::{
    pollable::{PollFuture, PollStream},
    selector::Selector,
};

mod list;
mod mpsc;
pub mod pollable;
pub mod selector;
mod task;

/// [`Selector`] specialized for polling [`Future`]s.
///
/// Behaves much like [`FuturesUnordered`](futures::stream::FuturesUnordered).
pub type FutureSelector<F> = Selector<F, PollFuture>;
/// [`Selector`] specialized for polling [`Stream`](futures::stream::Stream)s.
///
/// Behaves much like [`SelectAll`](futures::stream::SelectAll).
pub type StreamSelector<S> = Selector<S, PollStream>;
