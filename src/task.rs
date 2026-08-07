use std::{
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
};

use crate::selector::{BorrowedMut, Removed};

pub mod strategy;

/// Asynchronous task that can be polled with strategy `S`.
///
/// Strategy parameter allows for:
/// 1. Convenient blanket implementations on external types ([`Future`]s and [`Stream`](futures::Stream)s)
/// 2. Multiple implementations on a single type
/// 3. Exposing shared data to the task
///
/// Unless you want to customize [`Selector`](crate::Selector)'s behavior,
/// you don't have to manually implement this trait.
/// You can use one of ready-to-go strategies from [`strategy`].
///
/// # Custom task example
///
/// The example below polls a set of [`UdpSocket`](https://docs.rs/tokio/latest/tokio/net/struct.UdpSocket.html)s
/// for incoming datagrams.
///
/// Note that:
/// 1. [`UdpSocket`](https://docs.rs/tokio/latest/tokio/net/struct.UdpSocket.html) is an external type,
///    and so is this trait (from the perspective of the implementing crate).
///    The local strategy type makes the implementation possible.
/// 2. The strategy holds the receive buffer, so the whole selector needs only one,
///    no matter how many sockets it holds. Each datagram is copied out of the shared buffer
///    in [`Task::transform_cont`], before the next poll can overwrite it.
/// 3. A failed socket is removed from the selector, because the failure is reported
///    with [`ControlFlow::Break`].
///
/// ```
/// # use std::{
/// #     io,
/// #     mem::MaybeUninit,
/// #     net::SocketAddr,
/// #     ops::ControlFlow,
/// #     pin::Pin,
/// #     task::{Context, Poll, ready},
/// # };
/// # use async_selector::{
/// #     selector::{BorrowedMut, Removed, Selector},
/// #     task::Task,
/// # };
/// # use futures::StreamExt;
/// # use tokio::{io::ReadBuf, net::UdpSocket};
/// /// Receive buffer shared by all sockets in the selector.
/// ///
/// /// Large enough to hold any datagram.
/// struct RecvBuffer(Box<[MaybeUninit<u8>; u16::MAX as usize]>);
///
/// impl Task<RecvBuffer> for UdpSocket {
///     /// Address of the peer and length of the datagram,
///     /// which sits in the shared buffer.
///     type Cont = (SocketAddr, Vec<u8>);
///     /// Fatal socket error.
///     type Break = io::Error;
///     type Output = io::Result<(SocketAddr, Vec<u8>)>;
///
///     fn poll_progress(
///         self: Pin<&mut Self>,
///         buffer: &mut RecvBuffer,
///         cx: &mut Context<'_>,
///     ) -> Poll<ControlFlow<Self::Break, Self::Cont>> {
///         let mut buf = ReadBuf::uninit(buffer.0.as_mut_slice());
///         match ready!(self.poll_recv_from(cx, &mut buf)) {
///             Ok(peer) => Poll::Ready(ControlFlow::Continue((peer, buf.filled().to_vec()))),
///             Err(error) => Poll::Ready(ControlFlow::Break(error)),
///         }
///     }
///
///     fn transform_cont(
///         _: BorrowedMut<'_, Self>,
///         buffer: &mut RecvBuffer,
///         value: Self::Cont,
///     ) -> Option<Self::Output> {
///         Some(Ok(value))
///     }
///
///     fn transform_break(
///         _: Removed<Self>,
///         _: &mut RecvBuffer,
///         error: Self::Break,
///     ) -> Option<Self::Output> {
///         Some(Err(error))
///     }
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> io::Result<()> {
/// let mut selector = Selector::new(RecvBuffer(Box::new([MaybeUninit::uninit(); u16::MAX as usize])));
/// let mut addrs = Vec::new();
/// for _ in 0..2 {
///     let socket = UdpSocket::bind("127.0.0.1:0").await?;
///     addrs.push(socket.local_addr()?);
///     selector.push(socket);
/// }
///
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
    /// Type returned from [`Self::poll_progress`]
    /// when the task produces some value, but has not finished yet.
    type Cont;
    /// Type returned from [`Self::poll_progress`]
    /// when the task produces its last value.
    type Break;
    /// Final value type, produced from [`Self::Cont`]/[`Self::Break`]
    /// in [`Self::transform_cont`]/[`Self::transform_break`].
    type Output;

    /// Polls progress on this task using the given strategy.
    ///
    /// # Returns
    ///
    /// * [`ControlFlow::Break`], if task has finished,
    ///   and **should not** be polled again.
    /// * [`ControlFlow::Continue`], if the task has produced a value,
    ///   but has not finished yet and **can** be polled again.
    fn poll_progress(
        self: Pin<&mut Self>,
        strategy: &mut S,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Self::Break, Self::Cont>>;

    /// Transforms [`Self::Cont`] value obtained from [`Self::poll_progress`]
    /// into the final value type [`Self::Output`].
    ///
    /// This is the place to:
    /// 1. Enrich the value with some properties of the task, passed as [`BorrowedMut`]
    /// 2. Silently ignore the value by returning [`None`]
    fn transform_cont(
        task: BorrowedMut<'_, Self>,
        strategy: &mut S,
        value: Self::Cont,
    ) -> Option<Self::Output>;

    /// Transforms [`Self::Break`] value obtained from [`Self::poll_progress`]
    /// into the final value type [`Self::Output`].
    ///
    /// This is the place to:
    /// 1. Enrich the value with some properties of the task, passed as [`Removed`]
    /// 2. Silently ignore the value by returning [`None`]
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
