//! Set of strategies that provide blanket [`Task`] implementations
//! for implementors of common traits: [`Future`], [`Stream`] and [`TryStream`].

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

/// Yield future's output.
///
/// Enables [`Task`] implementation on any type that implements [`Future`].
///
/// The future will be polled until it resolves.
/// After that, the [`Selector`](crate::Selector) will
/// silently drop it and yield the output.
///
/// ```
/// # use async_selector::{
/// #     selector::Selector,
/// #     task::strategy::FutureBasic,
/// # };
/// # use futures::StreamExt;
/// # #[tokio::main]
/// # async fn main() {
/// let mut selector = (0..4)
///     .map(std::future::ready)
///     .collect::<Selector<_, FutureBasic>>();
/// for i in 0..4 {
///     let j: i32 = selector.next().await.unwrap();
///     assert_eq!(j, i);
/// }
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

/// Yield future's output and the future itself.
///
/// Enables [`Task`] implementation on any type that implements [`Future`].
///
/// The future will be polled until it resolves.
/// After that, the [`Selector`](crate::Selector) will
/// yield the output and the future itself.
///
/// ```
/// # use async_selector::{
/// #     selector::{Removed, Selector},
/// #     task::strategy::FutureReclaim,
/// # };
/// # use futures::{future::{FusedFuture, Ready}, StreamExt};
/// # #[tokio::main]
/// # async fn main() {
/// let mut selector = (0..4)
///     .map(futures::future::ready)
///     .collect::<Selector<_, FutureReclaim>>();
/// for i in 0..4 {
///     let item: (Removed<Ready<i32>>, i32) = selector.next().await.unwrap();
///     assert!(item.0.is_terminated());
///     assert_eq!(item.1, i);
/// }
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

/// Yield stream's items.
///
/// Enables [`Task`] implementation on any type that implements [`Stream`].
///
/// The stream will be polled for items until it is exhausted.
/// [`Selector`](crate::Selector) will yield all items.
/// After that, the [`Selector`](crate::Selector) will silently drop the stream.
///
/// ```
/// # use async_selector::{
/// #     selector::Selector,
/// #     task::strategy::StreamBasic,
/// # };
/// # use futures::StreamExt;
/// # #[tokio::main]
/// # async fn main() {
/// let mut selector = (0..4)
///     .map(|i| futures::stream::repeat(i).take(2))
///     .collect::<Selector<_, StreamBasic>>();
/// for i in 0..8 {
///     let j: i32 = selector.next().await.unwrap();
///     assert_eq!(j, i % 4);
/// }
/// assert!(selector.next().await.is_none());
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

/// Yield stream's items annotated with stream [`Id`].
///
/// Enables [`Task`] implementation on any type that implements [`Stream`].
///
/// The stream will be polled for items until it is exhausted.
/// [`Selector`](crate::Selector) will yield all items, attaching stream's task [`Id`] to each.
/// After that, the [`Selector`](crate::Selector) will silently drop the stream.
///
/// ```
/// # use async_selector::{
/// #     selector::{Id, Selector},
/// #     task::strategy::StreamWithId,
/// # };
/// # use futures::{channel::mpsc, StreamExt};
/// # #[tokio::main]
/// # async fn main() {
/// let (tx, rx) = mpsc::unbounded::<i32>();
/// let mut selector = Selector::<_, StreamWithId>::default();
/// let id = selector.push(rx).id().clone();
/// tx.unbounded_send(1).unwrap();
/// let item: (Id<_>, i32) = selector.next().await.unwrap();
/// assert_eq!(item.0, id);
/// assert_eq!(item.1, 1);
/// drop(tx);
/// assert!(selector.next().await.is_none());
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

/// Yield stream's items annotated with stream [`Id`], and the exhausted stream itself.
///
/// Enables [`Task`] implementation on any type that implements [`Stream`].
///
/// The stream will be polled for items until it is exhausted.
/// [`Selector`](crate::Selector) will yield all items, attaching stream's task [`Id`] to each.
/// After that, the [`Selector`](crate::Selector) will yield the exhausted stream.
///
/// ```
/// # use async_selector::{
/// #     selector::{Id, Removed, Selector},
/// #     task::strategy::StreamReclaim,
/// # };
/// # use futures::{channel::mpsc, StreamExt};
/// # use std::ops::ControlFlow;
/// # #[tokio::main]
/// # async fn main() {
/// let (tx, rx) = mpsc::unbounded::<i32>();
/// let mut selector = Selector::<_, StreamReclaim>::default();
/// let id = selector.push(rx).id().clone();
/// tx.unbounded_send(1).unwrap();
/// match selector.next().await.unwrap() {
///     ControlFlow::Continue(item) => {
///         assert_eq!(item.0, id);
///         assert_eq!(item.1, 1);       
///     }
///     ControlFlow::Break(..) => unreachable!("channel is still open"),
/// }
/// drop(tx);
/// match selector.next().await.unwrap() {
///     ControlFlow::Continue(..) => {
///         unreachable!("channel was closed");
///     }
///     ControlFlow::Break((item)) => {
///         let rx: mpsc::UnboundedReceiver<i32> = item.into_inner();
///     }
/// }
/// assert!(selector.is_empty());
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

/// Yield stream's items (stopping after first error).
///
/// Enables [`Task`] implementation on any type that implements [`TryStream`].
///
/// The stream will be polled for items until it is exhausted or yields an error.
/// [`Selector`](crate::Selector) will yield all items and the first error.
/// After that, the [`Selector`](crate::Selector) will silently drop the stream.
///
/// ```
/// # use async_selector::{
/// #     selector::Selector,
/// #     task::strategy::TryStreamBasic,
/// # };
/// # use futures::{channel::mpsc, StreamExt};
/// # #[tokio::main]
/// # async fn main() {
/// let (tx_1, rx_1) = mpsc::unbounded::<Result<i32, &'static str>>();
/// let (tx_2, rx_2) = mpsc::unbounded::<Result<i32, &'static str>>();
/// let mut selector = [rx_1, rx_2].into_iter().collect::<Selector::<_, TryStreamBasic>>();
/// tx_1.unbounded_send(Err("error")).unwrap();
/// assert_eq!(
///     selector.next().await.unwrap(),
///     Err("error"),
/// );
/// tx_2.unbounded_send(Ok(1)).unwrap();
/// assert_eq!(
///     selector.next().await.unwrap(),
///     Ok(1),
/// );
/// drop(tx_2);
/// assert!(selector.next().await.is_none());
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

/// Yield stream's items annotated with stream [`Id`] (stopping after first error).
///
/// Enables [`Task`] implementation on any type that implements [`TryStream`].
///
/// The stream will be polled for items until it is exhausted or yields an error.
/// [`Selector`](crate::Selector) will yield all items and the first error,
/// attaching stream's task [`Id`] to each.
/// After that, the [`Selector`](crate::Selector) will silently drop the stream.
///
/// ```
/// # use async_selector::{
/// #     selector::{Id, Selector},
/// #     task::strategy::TryStreamWithId,
/// # };
/// # use futures::{channel::mpsc, StreamExt};
/// # #[tokio::main]
/// # async fn main() {
/// let (tx_1, rx_1) = mpsc::unbounded::<Result<i32, &'static str>>();
/// let (tx_2, rx_2) = mpsc::unbounded::<Result<i32, &'static str>>();
/// let mut selector = Selector::<_, TryStreamWithId>::default();
/// let id_1 = selector.push(rx_1).id().clone();
/// let id_2 = selector.push(rx_2).id().clone();
/// tx_1.unbounded_send(Err("error")).unwrap();
/// assert_eq!(
///     selector.next().await.unwrap(),
///     (id_1, Err("error")),
/// );
/// tx_2.unbounded_send(Ok(1)).unwrap();
/// assert_eq!(
///     selector.next().await.unwrap(),
///     (id_2, Ok(1)),
/// );
/// drop(tx_2);
/// assert!(selector.next().await.is_none());
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

/// Yield stream's items annotated with stream [`Id`] (stopping after first error),
/// and the exhausted/failed stream itself.
///
/// Enables [`Task`] implementation on any type that implements [`TryStream`].
///
/// The stream will be polled for items until it is exhausted or yields an error.
/// [`Selector`](crate::Selector) will yield all items and the first error,
/// attaching stream's task [`Id`] to each.
/// After that, the [`Selector`](crate::Selector) will yield the exhausted/failed stream.
///
/// ```
/// # use async_selector::{
/// #     selector::{Id, Removed, Selector},
/// #     task::strategy::TryStreamReclaim,
/// # };
/// # use futures::{channel::mpsc, StreamExt};
/// # #[tokio::main]
/// # async fn main() {
/// let (tx_1, rx_1) = mpsc::unbounded::<Result<i32, &'static str>>();
/// let (tx_2, rx_2) = mpsc::unbounded::<Result<i32, &'static str>>();
/// let mut selector = Selector::<_, TryStreamReclaim>::default();
/// let id_1 = selector.push(rx_1).id().clone();
/// let id_2 = selector.push(rx_2).id().clone();
/// tx_1.unbounded_send(Err("error")).unwrap();
/// let item: (Removed<_>, Result<_, _>) = selector
///     .next()
///     .await
///     .unwrap()
///     .break_value()
///     .unwrap();
/// assert_eq!(item.0.id(), &id_1);
/// assert_eq!(
///     item.1,
///     Err("error"),
/// );
/// tx_2.unbounded_send(Ok(1)).unwrap();
/// let item: (Id<_>, i32) = selector
///     .next()
///     .await
///     .unwrap()
///     .continue_value()
///     .unwrap();
/// assert_eq!(item.0, id_2);
/// assert_eq!(item.1, 1);
/// drop(tx_2);
/// let item: (Removed<_>, Result<_, _>) = selector
///     .next()
///     .await
///     .unwrap()
///     .break_value()
///     .unwrap();
/// assert_eq!(item.0.id(), &id_2);
/// assert_eq!(item.1, Ok(()));
/// assert!(selector.is_empty());
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
