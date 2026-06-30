//! Traits that make the [`Selector`](crate::selector::Selector) generic over asynchronous tasks and polling logic.
//!
//! Unless you want to exercise the full flexibility of the selector, you don't need to use these traits.
//! You can use plain [`FutureSelector`](crate::FutureSelector) and [`StreamSelector`](crate::StreamSelector).

use std::{
    fmt,
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

impl<F> Default for PollAsFuture<F> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<F> fmt::Debug for PollAsFuture<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PollAsFuture")
    }
}

/// Marker trait that allows for a blanket implementation of [`PollWith`] on all [`Stream`]s.
pub struct PollAsStream<S>(PhantomData<fn() -> S>);

impl<S: Stream> PollStrategy for PollAsStream<S> {
    type Pollable = S;
}

impl<S> Default for PollAsStream<S> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<S> fmt::Debug for PollAsStream<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PollAsStream")
    }
}

/// Determines how the [`Selector`](crate::selector::Selector) polls the tasks and interprets their outputs.
pub trait PollWith<'a, E: ?Sized, EMut: ?Sized>: PollStrategy {
    /// Type of values returned when polling the task.
    type Progress;
    /// Type of values that should be returned from the selector.
    type Item;

    /// Polls the given task, providing references strongly typed extensions.
    ///
    /// # Extensions
    ///
    /// This method supports passing references to immutable and immutable extensions
    /// that can be used by the task. This allows the tasks for operating on a shared state
    /// without any unsafe code or synchronization primitives.
    /// Even more, extensions allow for static polymorphism in the types returned by the task.
    /// See relevant [example](https://todo.i.do.not.exist.yet).
    fn poll_progress(
        state: Pin<&mut Self::Pollable>,
        ext: &'a E,
        ext_mut: &mut EMut,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Progress>;

    /// Attemps to unwrap progress made by the task into a value that can be returned from the selector.
    ///
    /// # Returns
    ///
    /// * `ControlFlow::Break` if the task has finished.
    /// * `ControlFlow::Continue` if the task has not finished yet.
    fn try_unwrap(&mut self, output: Self::Progress)
    -> ControlFlow<Option<Self::Item>, Self::Item>;
}

impl<'a, F: Future> PollWith<'a, (), ()> for PollAsFuture<F> {
    type Progress = F::Output;
    type Item = F::Output;

    fn poll_progress(
        state: Pin<&mut Self::Pollable>,
        _: &'a (),
        _: &mut (),
        cx: &mut Context<'_>,
    ) -> Poll<Self::Progress> {
        state.poll(cx)
    }

    fn try_unwrap(
        &mut self,
        output: Self::Progress,
    ) -> ControlFlow<Option<Self::Item>, Self::Item> {
        ControlFlow::Break(Some(output))
    }
}

impl<'a, S: Stream> PollWith<'a, (), ()> for PollAsStream<S> {
    type Progress = Option<S::Item>;
    type Item = S::Item;

    fn poll_progress(
        state: Pin<&mut Self::Pollable>,
        _: &'a (),
        _: &mut (),
        cx: &mut Context<'_>,
    ) -> Poll<Self::Progress> {
        state.poll_next(cx)
    }

    fn try_unwrap(
        &mut self,
        output: Self::Progress,
    ) -> ControlFlow<Option<Self::Item>, Self::Item> {
        output
            .map(ControlFlow::Continue)
            .unwrap_or(ControlFlow::Break(None))
    }
}
