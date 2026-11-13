//! The [`Task`] trait, which defines how a [`Selector`](crate::selector::Selector)
//! polls the tasks it holds.
//!
//! You don't need anything from this module to use the selector with [`Future`]s and
//! [`Stream`](futures::Stream)s, the [`strategy`] module has you covered.
//! Implement [`Task`] yourself when you want to customize the selector's behavior.

use std::{
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
};

use crate::selector::{BorrowedMut, Removed};

pub mod strategy;

/// Asynchronous task that a [`Selector`](crate::selector::Selector) can poll with strategy `S`.
///
/// This trait generalizes [`Future`] and [`Stream`](futures::Stream):
/// a task can produce any number of intermediate values, and then a final one.
///
/// You don't have to implement this trait to use the selector with futures and streams.
/// The [`strategy`] module provides ready-to-go implementations for them.
///
/// # How the selector uses a task
///
/// Every time the selector polls a task, it calls [`poll_progress`](Self::poll_progress).
/// Depending on the result:
///
/// * [`Poll::Pending`] - the task stays in the selector, and waits for a wakeup.
/// * `Poll::Ready(ControlFlow::Continue(value))` - the task produced a value, but it's not finished.
///   It stays in the selector, and it will be polled again.
///   The selector passes the value to [`transform_cont`](Self::transform_cont),
///   together with a reference to the task.
/// * `Poll::Ready(ControlFlow::Break(value))` - the task is finished.
///   The selector removes it, and passes the value to [`transform_break`](Self::transform_break),
///   together with the removed task.
///
/// The transform methods turn the value into the item that the selector yields ([`Output`](Self::Output)).
/// If they return [`None`], the selector yields nothing for this value, and moves on.
///
/// # The strategy parameter
///
/// The strategy `S` is a value stored in the selector.
/// The selector passes it by mutable reference to every method of this trait.
/// This allows for:
/// 1. **Sharing state between all tasks**, e.g. a buffer, a configuration, or some statistics.
///    No synchronization or `unsafe` code is needed, because the selector polls one task at a time.
/// 2. **Multiple implementations for a single type**,
///    e.g. one for reading data, and another one for a graceful shutdown.
///    You can switch between them with [`Selector::with_strategy`](crate::selector::Selector::with_strategy).
/// 3. **Implementations for foreign types.** A local strategy type lets you implement
///    this trait for types from other crates. This is how the [`strategy`] module implements it
///    for all [`Future`]s and [`Stream`](futures::Stream)s.
///
/// If you don't need any of this, use `()` as the strategy (it's the default).
///
/// This trait is also implemented for the `&mut S` and `Box<S>` strategies,
/// whenever it is implemented for `S`.
///
/// # Example
///
/// The example below polls a set of tokio's
/// [`UdpSocket`](https://docs.rs/tokio/latest/tokio/net/struct.UdpSocket.html)s
/// for incoming datagrams.
///
/// Note that:
/// 1. `UdpSocket` is a foreign type, and so is this trait (from the perspective of the example).
///    The local `RecvBuffer` strategy type makes the implementation possible.
/// 2. The strategy holds the receive buffer, so the whole selector needs only one,
///    no matter how many sockets it holds.
///    Each datagram is copied out of the shared buffer in [`transform_cont`](Self::transform_cont),
///    before the next poll can overwrite it.
/// 3. A failing socket is removed from the selector, because the failure is reported
///    with [`ControlFlow::Break`].
///
/// ```
/// use std::{
///     io,
///     net::SocketAddr,
///     ops::ControlFlow,
///     pin::Pin,
///     task::{Context, Poll, ready},
/// };
///
/// use async_selector::{
///     selector::{BorrowedMut, Removed, Selector},
///     task::Task,
/// };
/// use futures::StreamExt;
/// use tokio::{io::ReadBuf, net::UdpSocket};
///
/// /// Strategy holding a receive buffer that is shared by all sockets in the selector.
/// struct RecvBuffer(Vec<u8>);
///
/// impl Task<RecvBuffer> for UdpSocket {
///     /// Address of the peer and length of the received datagram,
///     /// which sits in the shared buffer.
///     type Cont = (SocketAddr, usize);
///     /// Socket error. The socket is removed from the selector after it fails.
///     type Break = io::Error;
///     /// What the selector yields: received datagrams and socket errors.
///     type Output = io::Result<(SocketAddr, Vec<u8>)>;
///
///     fn poll_progress(
///         self: Pin<&mut Self>,
///         buffer: &mut RecvBuffer,
///         cx: &mut Context<'_>,
///     ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
///         // Receive the next datagram into the shared buffer.
///         let mut buf = ReadBuf::new(&mut buffer.0);
///         let flow = match ready!(self.poll_recv_from(cx, &mut buf)) {
///             Ok(peer) => ControlFlow::Continue((peer, buf.filled().len())),
///             Err(error) => ControlFlow::Break(error),
///         };
///         Poll::Ready(flow)
///     }
///
///     fn transform_cont(
///         _socket: BorrowedMut<'_, Self>,
///         buffer: &mut RecvBuffer,
///         (peer, len): Self::Cont,
///     ) -> Option<Self::Output> {
///         // Copy the datagram out of the shared buffer.
///         Some(Ok((peer, buffer.0[..len].to_vec())))
///     }
///
///     fn transform_break(
///         _socket: Removed<Self>,
///         _buffer: &mut RecvBuffer,
///         error: Self::Break,
///     ) -> Option<Self::Output> {
///         // We don't need the failed socket anymore, so we let it drop.
///         Some(Err(error))
///     }
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> io::Result<()> {
/// // A single buffer, large enough for any datagram, serves all sockets.
/// let mut selector = Selector::new(RecvBuffer(vec![0; u16::MAX as usize]));
///
/// // Bind a couple of sockets, and push them into the selector.
/// let mut addrs = Vec::new();
/// for _ in 0..2 {
///     let socket = UdpSocket::bind("127.0.0.1:0").await?;
///     addrs.push(socket.local_addr()?);
///     selector.push(socket);
/// }
///
/// // Send a datagram to each socket, and receive it through the selector.
/// let sender = UdpSocket::bind("127.0.0.1:0").await?;
/// for addr in &addrs {
///     sender.send_to(b"hello", addr).await?;
///     let (peer, data) = selector.next().await.unwrap()?;
///     assert_eq!(peer, sender.local_addr()?);
///     assert_eq!(data, b"hello");
/// }
/// # Ok(())
/// # }
/// ```
pub trait Task<S: ?Sized = ()>: Sized {
    /// Value returned from [`poll_progress`](Self::poll_progress)
    /// when the task produces a value, but is not finished yet.
    ///
    /// Passed to [`transform_cont`](Self::transform_cont).
    type Cont;

    /// Value returned from [`poll_progress`](Self::poll_progress)
    /// when the task finishes.
    ///
    /// Passed to [`transform_break`](Self::transform_break).
    type Break;

    /// Item yielded by the selector.
    ///
    /// Produced from [`Cont`](Self::Cont) and [`Break`](Self::Break) values by
    /// [`transform_cont`](Self::transform_cont) and [`transform_break`](Self::transform_break).
    type Output;

    /// Attempts to make progress on this task.
    ///
    /// Works like [`Future::poll`] and [`Stream::poll_next`](futures::Stream::poll_next):
    /// if the task cannot make progress yet, it returns [`Poll::Pending`],
    /// and arranges for the waker from `cx` to be woken when it can.
    ///
    /// # Returns
    ///
    /// * [`Poll::Pending`] if the task cannot make progress yet.
    /// * `Poll::Ready(ControlFlow::Continue(value))` if the task produced a value,
    ///   but has not finished yet, and **can** be polled again.
    /// * `Poll::Ready(ControlFlow::Break(value))` if the task has finished,
    ///   and **must not** be polled again.
    ///   The selector removes the task before passing the value to
    ///   [`transform_break`](Self::transform_break).
    fn poll_progress(
        self: Pin<&mut Self>,
        strategy: &mut S,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>>;

    /// Turns a [`Cont`](Self::Cont) value returned from [`poll_progress`](Self::poll_progress)
    /// into the item yielded by the selector.
    ///
    /// The task is still stored in the selector, and you get a mutable reference to it.
    /// This is the place to:
    /// 1. enrich the value with some properties of the task, e.g. its [`Id`](crate::selector::Id),
    /// 2. update the task's state,
    /// 3. skip the value by returning [`None`] (the selector yields nothing for it).
    fn transform_cont(
        task: BorrowedMut<'_, Self>,
        strategy: &mut S,
        value: Self::Cont,
    ) -> Option<Self::Output>;

    /// Turns a [`Break`](Self::Break) value returned from [`poll_progress`](Self::poll_progress)
    /// into the item yielded by the selector.
    ///
    /// The task has already been removed from the selector, and you own it.
    /// This is the place to:
    /// 1. enrich the value with some properties of the task, e.g. its [`Id`](crate::selector::Id),
    /// 2. give the task back to the caller as part of the output,
    ///    or let it drop together with the [`Removed`] handle,
    /// 3. skip the value by returning [`None`] (the selector yields nothing for it).
    fn transform_break(
        task: Removed<Self>,
        strategy: &mut S,
        value: Self::Break,
    ) -> Option<Self::Output>;
}

impl<S, T> Task<&mut S> for T
where
    S: ?Sized,
    T: Task<S>,
{
    type Cont = <Self as Task<S>>::Cont;
    type Break = <Self as Task<S>>::Break;
    type Output = <Self as Task<S>>::Output;

    fn poll_progress(
        self: Pin<&mut Self>,
        strategy: &mut &mut S,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        self.poll_progress(&mut **strategy, cx)
    }

    fn transform_cont(
        task: BorrowedMut<'_, Self>,
        strategy: &mut &mut S,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Self::transform_cont(task, &mut **strategy, value)
    }

    fn transform_break(
        task: Removed<Self>,
        strategy: &mut &mut S,
        value: Self::Break,
    ) -> Option<Self::Output> {
        Self::transform_break(task, &mut **strategy, value)
    }
}

impl<S, T> Task<Box<S>> for T
where
    S: ?Sized,
    T: Task<S>,
{
    type Cont = <Self as Task<S>>::Cont;
    type Break = <Self as Task<S>>::Break;
    type Output = <Self as Task<S>>::Output;

    fn poll_progress(
        self: Pin<&mut Self>,
        strategy: &mut Box<S>,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
        self.poll_progress(strategy.as_mut(), cx)
    }

    fn transform_cont(
        task: BorrowedMut<'_, Self>,
        strategy: &mut Box<S>,
        value: Self::Cont,
    ) -> Option<Self::Output> {
        Self::transform_cont(task, strategy.as_mut(), value)
    }

    fn transform_break(
        task: Removed<Self>,
        strategy: &mut Box<S>,
        value: Self::Break,
    ) -> Option<Self::Output> {
        Self::transform_break(task, strategy.as_mut(), value)
    }
}
