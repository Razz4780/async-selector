//! Fast and flexible [`Future`]/[`Stream`](futures::Stream) selector.
//!
//! TODO more docsss

#![deny(missing_docs, unused_crate_dependencies)]

use crate::{
    pollable::{PollAsFuture, PollAsStream},
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
pub type FutureSelector<F> = Selector<PollAsFuture<F>>;
/// [`Selector`] specialized for polling [`Stream`](futures::stream::Stream)s.
///
/// Behaves much like [`SelectAll`](futures::stream::SelectAll).
pub type StreamSelector<S> = Selector<PollAsStream<S>>;
