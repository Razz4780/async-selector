//! Ready-to-go strategies, which implement [`Task`] for all [`Future`]s, [`Stream`]s,
//! and [`TryStream`]s.
//!
//! The strategy decides what the selector yields, and what happens to finished tasks.
//! The crate root exposes a type alias for each strategy in this module,
//! e.g. [`StreamSelector<S>`](crate::StreamSelector) is `Selector<S, StreamBasic>`.
//! See [choosing a selector](crate#choosing-a-selector) for an overview.

use std::{
    convert::Infallible,
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, TryStream};

use crate::{
    selector::{BorrowedMut, Id, Removed},
    task::Task,
};

/// Strategy for [`Future`]s, which yields their outputs.
///
/// Each future is polled until it resolves.
/// Then the selector drops the future, and yields its output.
///
/// Used by [`FutureSelector`](crate::FutureSelector).
///
/// ```
/// use async_selector::FutureSelector;
/// use futures::StreamExt;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// // Same as `Selector<_, FutureBasic>`.
/// let mut selector = (0..3)
///     .map(std::future::ready)
///     .collect::<FutureSelector<_>>();
///
/// // All futures are ready, so they resolve in the order in which they were pushed.
/// let outputs = (&mut selector).collect::<Vec<_>>().await;
/// assert_eq!(outputs, [0, 1, 2]);
///
/// // Resolved futures are dropped.
/// assert!(selector.is_empty());
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct FutureBasic;

impl<F: Future> Task<FutureBasic> for F {
    type Cont = Infallible;
    type Break = F::Output;
    type Output = F::Output;

    fn poll_progress(
        self: Pin<&mut Self>,
        _: &mut FutureBasic,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        self.poll(cx).map(ControlFlow::Break)
    }

    fn transform_cont(
        _: BorrowedMut<'_, Self>,
        _: &mut FutureBasic,
        _: Self::Cont,
    ) -> Option<Self::Output> {
        unreachable!("cannot construct std::convert::Infallible")
    }

    fn transform_break(
        _: Removed<F>,
        _: &mut FutureBasic,
        value: Self::Break,
    ) -> Option<Self::Output> {
        Some(value)
    }
}

/// Strategy for [`Future`]s, which yields their outputs together with the resolved futures.
///
/// Each future is polled until it resolves.
/// Then the selector removes the future, and yields a `(Removed<F>, F::Output)` pair.
/// Useful when the future holds some state that you want back after it resolves.
///
/// Used by [`FutureReclaimSelector`](crate::FutureReclaimSelector).
///
/// ```
/// use async_selector::FutureReclaimSelector;
/// use futures::{StreamExt, future::FusedFuture};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// // Same as `Selector<_, FutureReclaim>`.
/// let mut selector = (0..3)
///     .map(futures::future::ready)
///     .collect::<FutureReclaimSelector<_>>();
///
/// for i in 0..3 {
///     let (future, output) = selector.next().await.unwrap();
///     assert_eq!(output, i);
///     // We get the future back. It has already resolved.
///     assert!(future.is_terminated());
/// }
///
/// assert!(selector.is_empty());
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct FutureReclaim;

impl<F: Future> Task<FutureReclaim> for F {
    type Cont = Infallible;
    type Break = F::Output;
    type Output = (Removed<F>, F::Output);

    fn poll_progress(
        self: Pin<&mut Self>,
        _: &mut FutureReclaim,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        self.poll(cx).map(ControlFlow::Break)
    }

    fn transform_cont(
        _: BorrowedMut<'_, F>,
        _: &mut FutureReclaim,
        _: Self::Cont,
    ) -> Option<Self::Output> {
        unreachable!("cannot construct std::convert::Infallible")
    }

    fn transform_break(
        task: Removed<F>,
        _: &mut FutureReclaim,
        value: Self::Break,
    ) -> Option<Self::Output> {
        Some((task, value))
    }
}

/// Strategy for [`Stream`]s, which yields their items.
///
/// Each stream is polled until it is exhausted.
/// The selector yields all of its items, and then silently drops it.
///
/// Used by [`StreamSelector`](crate::StreamSelector).
///
/// ```
/// use async_selector::StreamSelector;
/// use futures::StreamExt;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// // Same as `Selector<_, StreamBasic>`.
/// let mut selector = StreamSelector::default();
///
/// // Two streams, each with two items.
/// selector.push(futures::stream::iter(["a1", "a2"]));
/// selector.push(futures::stream::iter(["b1", "b2"]));
///
/// // Ready streams take turns.
/// let items = (&mut selector).collect::<Vec<_>>().await;
/// assert_eq!(items, ["a1", "b1", "a2", "b2"]);
///
/// // Exhausted streams are dropped.
/// assert!(selector.is_empty());
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct StreamBasic;

impl<S: Stream> Task<StreamBasic> for S {
    type Cont = S::Item;
    type Break = ();
    type Output = S::Item;

    fn poll_progress(
        self: Pin<&mut Self>,
        _: &mut StreamBasic,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        match std::task::ready!(self.poll_next(cx)) {
            Some(item) => Poll::Ready(ControlFlow::Continue(item)),
            None => Poll::Ready(ControlFlow::Break(())),
        }
    }

    fn transform_cont(
        _: BorrowedMut<'_, Self>,
        _: &mut StreamBasic,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Some(value)
    }

    fn transform_break(
        _: Removed<Self>,
        _: &mut StreamBasic,
        _: Self::Break,
    ) -> Option<Self::Output> {
        None
    }
}

/// Strategy for [`Stream`]s, which yields their items
/// paired with the [`Id`] of the stream that produced them.
///
/// Each stream is polled until it is exhausted.
/// The selector yields all of its items as `(Id<S>, S::Item)` pairs, and then silently drops it.
///
/// Used by [`StreamWithIdSelector`](crate::StreamWithIdSelector).
///
/// ```
/// use async_selector::StreamWithIdSelector;
/// use futures::{StreamExt, channel::mpsc};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// // Same as `Selector<_, StreamWithId>`.
/// let mut selector = StreamWithIdSelector::default();
///
/// let (tx_a, rx_a) = mpsc::unbounded::<&str>();
/// let (tx_b, rx_b) = mpsc::unbounded::<&str>();
/// let id_a = selector.push(rx_a).id().clone();
/// let id_b = selector.push(rx_b).id().clone();
///
/// // Each item comes with the ID of the stream that produced it.
/// tx_b.unbounded_send("hello").unwrap();
/// assert_eq!(selector.next().await, Some((id_b, "hello")));
/// tx_a.unbounded_send("hi").unwrap();
/// assert_eq!(selector.next().await, Some((id_a, "hi")));
///
/// // Closing the channels exhausts the receivers, and the selector drops them.
/// drop((tx_a, tx_b));
/// assert_eq!(selector.next().await, None);
/// assert!(selector.is_empty());
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct StreamWithId;

impl<S: Stream> Task<StreamWithId> for S {
    type Cont = S::Item;
    type Break = ();
    type Output = (Id<S>, S::Item);

    fn poll_progress(
        self: Pin<&mut Self>,
        _: &mut StreamWithId,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        match std::task::ready!(self.poll_next(cx)) {
            Some(item) => Poll::Ready(ControlFlow::Continue(item)),
            None => Poll::Ready(ControlFlow::Break(())),
        }
    }

    fn transform_cont(
        task: BorrowedMut<'_, Self>,
        _: &mut StreamWithId,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Some((task.id().clone(), value))
    }

    fn transform_break(
        _: Removed<Self>,
        _: &mut StreamWithId,
        _: Self::Break,
    ) -> Option<Self::Output> {
        None
    }
}

/// Strategy for [`Stream`]s, which yields their items
/// paired with the [`Id`] of the stream that produced them,
/// and then the exhausted streams themselves.
///
/// Each stream is polled until it is exhausted.
/// The selector yields a [`ControlFlow`]:
/// * `Continue((Id<S>, S::Item))` for every item,
/// * `Break(Removed<S>)` when the stream is exhausted.
///
/// Used by [`StreamReclaimSelector`](crate::StreamReclaimSelector).
///
/// ```
/// use std::ops::ControlFlow;
///
/// use async_selector::StreamReclaimSelector;
/// use futures::{StreamExt, channel::mpsc};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// // Same as `Selector<_, StreamReclaim>`.
/// let mut selector = StreamReclaimSelector::default();
///
/// let (tx, rx) = mpsc::unbounded::<u32>();
/// let id = selector.push(rx).id().clone();
///
/// // Send two items, and close the channel.
/// tx.unbounded_send(1).unwrap();
/// tx.unbounded_send(2).unwrap();
/// drop(tx);
///
/// let mut items = Vec::new();
/// while let Some(event) = selector.next().await {
///     match event {
///         // The stream produced an item.
///         ControlFlow::Continue((stream_id, item)) => {
///             assert_eq!(stream_id, id);
///             items.push(item);
///         }
///         // The stream is exhausted, and we get it back.
///         ControlFlow::Break(stream) => {
///             assert_eq!(stream.id(), &id);
///             let _rx: mpsc::UnboundedReceiver<u32> = stream.into_inner();
///         }
///     }
/// }
///
/// assert_eq!(items, [1, 2]);
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct StreamReclaim;

impl<S: Stream> Task<StreamReclaim> for S {
    type Cont = S::Item;
    type Break = ();
    type Output = ControlFlow<Removed<S>, (Id<S>, S::Item)>;

    fn poll_progress(
        self: Pin<&mut Self>,
        _: &mut StreamReclaim,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        match std::task::ready!(self.poll_next(cx)) {
            Some(item) => Poll::Ready(ControlFlow::Continue(item)),
            None => Poll::Ready(ControlFlow::Break(())),
        }
    }

    fn transform_cont(
        task: BorrowedMut<'_, Self>,
        _: &mut StreamReclaim,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Some(ControlFlow::Continue((task.id().clone(), value)))
    }

    fn transform_break(
        task: Removed<Self>,
        _: &mut StreamReclaim,
        _: Self::Break,
    ) -> Option<Self::Output> {
        Some(ControlFlow::Break(task))
    }
}

/// Strategy for [`TryStream`]s, which yields their items and errors.
///
/// Each stream is polled until it is exhausted, or until it yields an error.
/// The selector yields all of its items and the error (if any),
/// and then silently drops the stream.
///
/// Used by [`TryStreamSelector`](crate::TryStreamSelector).
///
/// ```
/// use async_selector::TryStreamSelector;
/// use futures::StreamExt;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// // Same as `Selector<_, TryStreamBasic>`.
/// let mut selector = TryStreamSelector::default();
///
/// // Two streams of results. The first one fails midway.
/// selector.push(futures::stream::iter([Ok(1), Err("oops"), Ok(2)]));
/// selector.push(futures::stream::iter([Ok(10), Ok(20), Ok(30)]));
///
/// let items = (&mut selector).collect::<Vec<_>>().await;
///
/// // The first stream was dropped after the error, so `Ok(2)` never arrives.
/// assert_eq!(items, [Ok(1), Ok(10), Err("oops"), Ok(20), Ok(30)]);
/// assert!(selector.is_empty());
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct TryStreamBasic;

impl<S: TryStream> Task<TryStreamBasic> for S {
    type Cont = S::Ok;
    type Break = Result<(), S::Error>;
    type Output = Result<S::Ok, S::Error>;

    fn poll_progress(
        self: Pin<&mut Self>,
        _: &mut TryStreamBasic,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        match std::task::ready!(self.try_poll_next(cx)) {
            Some(Ok(item)) => Poll::Ready(ControlFlow::Continue(item)),
            Some(Err(error)) => Poll::Ready(ControlFlow::Break(Err(error))),
            None => Poll::Ready(ControlFlow::Break(Ok(()))),
        }
    }

    fn transform_cont(
        _: BorrowedMut<'_, Self>,
        _: &mut TryStreamBasic,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Some(Ok(value))
    }

    fn transform_break(
        _: Removed<Self>,
        _: &mut TryStreamBasic,
        value: Self::Break,
    ) -> Option<Self::Output> {
        value.err().map(Err)
    }
}

/// Strategy for [`TryStream`]s, which yields their items and errors
/// paired with the [`Id`] of the stream that produced them.
///
/// Each stream is polled until it is exhausted, or until it yields an error.
/// The selector yields all of its items and the error (if any)
/// as `(Id<S>, Result<S::Ok, S::Error>)` pairs, and then silently drops the stream.
///
/// Used by [`TryStreamWithIdSelector`](crate::TryStreamWithIdSelector).
///
/// ```
/// use async_selector::TryStreamWithIdSelector;
/// use futures::StreamExt;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// // Same as `Selector<_, TryStreamWithId>`.
/// let mut selector = TryStreamWithIdSelector::default();
///
/// // Two streams of results. The first one fails midway.
/// let a = selector
///     .push(futures::stream::iter([Ok(1), Err("oops"), Ok(2)]))
///     .id()
///     .clone();
/// let b = selector
///     .push(futures::stream::iter([Ok(10), Ok(20), Ok(30)]))
///     .id()
///     .clone();
///
/// let items = (&mut selector).collect::<Vec<_>>().await;
///
/// // Each item and error comes with the ID of the stream that produced it.
/// // The first stream was dropped after the error, so `Ok(2)` never arrives.
/// assert_eq!(
///     items,
///     [
///         (a.clone(), Ok(1)),
///         (b.clone(), Ok(10)),
///         (a.clone(), Err("oops")),
///         (b.clone(), Ok(20)),
///         (b.clone(), Ok(30)),
///     ],
/// );
/// assert!(selector.is_empty());
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct TryStreamWithId;

impl<S: TryStream> Task<TryStreamWithId> for S {
    type Cont = S::Ok;
    type Break = Result<(), S::Error>;
    type Output = (Id<S>, Result<S::Ok, S::Error>);

    fn poll_progress(
        self: Pin<&mut Self>,
        _: &mut TryStreamWithId,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        match std::task::ready!(self.try_poll_next(cx)) {
            Some(Ok(item)) => Poll::Ready(ControlFlow::Continue(item)),
            Some(Err(error)) => Poll::Ready(ControlFlow::Break(Err(error))),
            None => Poll::Ready(ControlFlow::Break(Ok(()))),
        }
    }

    fn transform_cont(
        task: BorrowedMut<'_, Self>,
        _: &mut TryStreamWithId,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Some((task.id().clone(), Ok(value)))
    }

    fn transform_break(
        task: Removed<Self>,
        _: &mut TryStreamWithId,
        value: Self::Break,
    ) -> Option<Self::Output> {
        match value {
            Ok(()) => None,
            Err(error) => Some((task.id().clone(), Err(error))),
        }
    }
}

/// Strategy for [`TryStream`]s, which yields their items
/// paired with the [`Id`] of the stream that produced them,
/// and then the finished streams themselves.
///
/// Each stream is polled until it is exhausted, or until it yields an error.
/// The selector yields a [`ControlFlow`]:
/// * `Continue((Id<S>, S::Ok))` for every successful item,
/// * `Break((Removed<S>, Result<(), S::Error>))` when the stream is exhausted (`Ok(())`)
///   or yields an error (`Err`).
///
/// Used by [`TryStreamReclaimSelector`](crate::TryStreamReclaimSelector).
///
/// ```
/// use std::ops::ControlFlow;
///
/// use async_selector::TryStreamReclaimSelector;
/// use futures::StreamExt;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// // Same as `Selector<_, TryStreamReclaim>`.
/// let mut selector = TryStreamReclaimSelector::default();
///
/// // Two streams of results. The first one fails midway.
/// selector.push(futures::stream::iter([Ok(1), Err("oops"), Ok(2)]));
/// selector.push(futures::stream::iter([Ok(10), Ok(20), Ok(30)]));
///
/// let mut items = Vec::new();
/// let mut endings = Vec::new();
/// while let Some(event) = selector.next().await {
///     match event {
///         // A stream produced an item.
///         ControlFlow::Continue((_id, item)) => items.push(item),
///         // A stream failed (`Err`) or was exhausted (`Ok`).
///         // Either way, we get the stream back.
///         ControlFlow::Break((_stream, result)) => endings.push(result),
///     }
/// }
///
/// assert_eq!(items, [1, 10, 20, 30]);
/// assert_eq!(endings, [Err("oops"), Ok(())]);
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct TryStreamReclaim;

impl<S: TryStream> Task<TryStreamReclaim> for S {
    type Cont = S::Ok;
    type Break = Result<(), S::Error>;
    type Output = ControlFlow<(Removed<S>, Result<(), S::Error>), (Id<S>, S::Ok)>;

    fn poll_progress(
        self: Pin<&mut Self>,
        _: &mut TryStreamReclaim,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        match std::task::ready!(self.try_poll_next(cx)) {
            Some(Ok(item)) => Poll::Ready(ControlFlow::Continue(item)),
            Some(Err(error)) => Poll::Ready(ControlFlow::Break(Err(error))),
            None => Poll::Ready(ControlFlow::Break(Ok(()))),
        }
    }

    fn transform_cont(
        task: BorrowedMut<'_, Self>,
        _: &mut TryStreamReclaim,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Some(ControlFlow::Continue((task.id().clone(), value)))
    }

    fn transform_break(
        task: Removed<Self>,
        _: &mut TryStreamReclaim,
        value: Self::Break,
    ) -> Option<Self::Output> {
        Some(ControlFlow::Break((task, value)))
    }
}
