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
