//! Traits that make the [`Selector`](crate::selector::Selector) generic over asynchronous tasks and polling logic.
//!
//! Unless you want to exercise the full flexibility of the selector, you don't need to use these traits.
//! You can use plain [`FutureSelector`](crate::FutureSelector) and [`StreamSelector`](crate::StreamSelector).

use std::{
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;

/// Task that can be polled inside a [`Selector`](crate::selector::Selector).
pub trait Pollable<'a, E: ?Sized, EMut: ?Sized> {
    /// Type of values returned when polling the task.
    type Progress;

    /// Polls the given task with references to strongly typed extensions.
    ///
    /// # Extensions
    ///
    /// This method supports passing references to immutable and mutable extensions
    /// that can be used by the task. This allows the tasks for operating on a shared state
    /// without any unsafe code or synchronization primitives.
    /// Even more, extensions allow for static polymorphism in the types returned by the task.
    /// See relevant [example](https://github.com/Razz4780/async-selector/blob/main/examples/extensions.rs).
    ///
    /// # Returns
    ///
    /// * [`ControlFlow::Continue`] if the task has not finished and might yield more values
    /// * [`ControlFlow::Break`] if the task has finished
    fn poll_progress(
        self: Pin<&mut Self>,
        ext: &'a E,
        ext_mut: &mut EMut,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Option<Self::Progress>, Self::Progress>>;
}

/// Sealed complementary trait used internally by [`Selector`](crate::selector::Selector).
///
/// Allows for handling plain [`Future`]s and [`Stream`]s as [`Pollable`]s through [`PollFuture`] and [`PollStream`].
pub trait PollProxy<'a, P, E: ?Sized, EMut: ?Sized>: sealed::Sealed {
    type Progress;

    fn poll_progress(
        state: Pin<&mut P>,
        ext: &'a E,
        ext_mut: &mut EMut,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Option<Self::Progress>, Self::Progress>>;
}

/// [`PollProxy`] that transforms [`Future`]s into [`Pollable`] tasks.
#[derive(Debug, Default, Clone, Copy)]
pub struct PollFuture;

impl sealed::Sealed for PollFuture {}

impl<'a, F: Future> PollProxy<'a, F, (), ()> for PollFuture {
    type Progress = F::Output;

    fn poll_progress(
        state: Pin<&mut F>,
        _: &'a (),
        _: &mut (),
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Option<Self::Progress>, Self::Progress>> {
        state.poll(cx).map(Some).map(ControlFlow::Break)
    }
}

/// [`PollProxy`] that transforms [`Stream`]s into [`Pollable`] tasks.
#[derive(Debug, Default, Clone, Copy)]
pub struct PollStream;

impl sealed::Sealed for PollStream {}

impl<'a, S: Stream> PollProxy<'a, S, (), ()> for PollStream {
    type Progress = S::Item;

    fn poll_progress(
        state: Pin<&mut S>,
        _: &'a (),
        _: &mut (),
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Option<Self::Progress>, Self::Progress>> {
        state.poll_next(cx).map(|opt| {
            opt.map(ControlFlow::Continue)
                .unwrap_or(ControlFlow::Break(None))
        })
    }
}

/// Noop [`PollProxy`] that only works with [`Pollable`]s.
#[derive(Debug, Default, Clone, Copy)]
pub struct PollDirect;

impl sealed::Sealed for PollDirect {}

impl<'a, P, E, EMut> PollProxy<'a, P, E, EMut> for PollDirect
where
    P: Pollable<'a, E, EMut>,
    E: ?Sized,
    EMut: ?Sized,
{
    type Progress = P::Progress;

    fn poll_progress(
        state: Pin<&mut P>,
        ext: &'a E,
        ext_mut: &mut EMut,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Option<Self::Progress>, Self::Progress>> {
        state.poll_progress(ext, ext_mut, cx)
    }
}

mod sealed {
    pub trait Sealed {}
}
