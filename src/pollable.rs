//! Traits that make the [`Selector`](crate::selector::Selector) generic over asynchronous tasks and polling logic.
//!
//! Unless you want to exercise the full flexibility of the selector, you don't need to use these traits.
//! You can use plain [`FutureSelector`](crate::FutureSelector) and [`StreamSelector`](crate::StreamSelector).

use std::{
    marker::PhantomData,
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;

/// Determines the type of pollable task that can be stored in a [`Selector`](crate::selector::Selector).
///
/// This trait exists solely to enable non-conflicting blanket implementations for [`Future`]s and [`Stream`]s.
pub trait PollStrategy {
    /// Type of the task that will be stored in the [`Selector`](crate::selector::Selector).
    type Pollable;
}

/// Marker trait that allows for a blanket implementation of [`PollWith`] on all [`Future`]s.
pub struct PollAsFuture<F>(PhantomData<fn() -> F>);

impl<F: Future> PollStrategy for PollAsFuture<F> {
    type Pollable = F;
}

/// Marker trait that allows for a blanket implementation of [`PollWith`] on all [`Stream`]s.
pub struct PollAsStream<S>(PhantomData<fn() -> S>);

impl<S: Stream> PollStrategy for PollAsStream<S> {
    type Pollable = S;
}

/// Determines how the [`Selector`](crate::selector::Selector) polls the tasks and interprets their outputs.
pub trait PollWith<'a, E: ?Sized, EMut: ?Sized>: PollStrategy {
    /// Type of values returned when polling the task.
    type Progress;

    /// Polls the given task, providing references to strongly typed extensions.
    ///
    /// # Extensions
    ///
    /// This method supports passing references to immutable and immutable extensions
    /// that can be used by the task. This allows the tasks for operating on a shared state
    /// without any unsafe code or synchronization primitives.
    /// Even more, extensions allow for static polymorphism in the types returned by the task.
    /// See relevant [example](https://github.com/Razz4780/async-selector/blob/main/examples/extensions.rs).
    ///
    /// # Returns
    ///
    /// * `ControlFlow::Continue` if the task has not finished and might yield more values
    /// * `ControlFlow::Break` if the task has finished
    fn poll_progress(
        state: Pin<&mut Self::Pollable>,
        ext: &'a E,
        ext_mut: &mut EMut,
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Option<Self::Progress>, Self::Progress>>;
}

impl<'a, F: Future> PollWith<'a, (), ()> for PollAsFuture<F> {
    type Progress = F::Output;

    fn poll_progress(
        state: Pin<&mut Self::Pollable>,
        _: &'a (),
        _: &mut (),
        cx: &mut Context<'_>,
    ) -> Poll<ControlFlow<Option<Self::Progress>, Self::Progress>> {
        state.poll(cx).map(Some).map(ControlFlow::Break)
    }
}

impl<'a, S: Stream> PollWith<'a, (), ()> for PollAsStream<S> {
    type Progress = S::Item;

    fn poll_progress(
        state: Pin<&mut Self::Pollable>,
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
