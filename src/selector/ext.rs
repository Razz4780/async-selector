//! Wrapper types for [`Selector`] that allow for customizing polling behavior.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;

use crate::{
    pollable::{PollStrategy, PollWith},
    selector::{Id, Selector},
};

/// Borrowed [`Stream`] that will poll the inner [`Selector`]
/// and pass the extensions to the inner tasks.
///
/// Created with [`Selector::with_ext`].
///
/// **Important:** before polling the tasks with different extension types, see the wakeups [section](Selector#wakeups).
pub struct WithExt<'s, 'e, 'emut, S: PollStrategy, E: ?Sized, EMut: ?Sized> {
    pub(super) selector: &'s mut Selector<S>,
    pub(super) ext: &'e E,
    pub(super) ext_mut: &'emut mut EMut,
}

impl<'e, S, E, EMut> Stream for WithExt<'_, 'e, '_, S, E, EMut>
where
    S: PollStrategy,
    S: PollWith<'e, E, EMut>,
    E: ?Sized,
    EMut: ?Sized,
{
    type Item = S::Progress;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.selector
            .poll_next_inner(this.ext, this.ext_mut, |_| (), cx)
            .map(|opt| opt.map(|(result, ())| result))
    }
}

/// Borrowed [`Stream`] that will poll the inner [`Selector`] and attach the origin task [`Id`] to every item.
///
/// Created with [`Selector::with_id`].
pub struct WithId<'s, S: PollStrategy> {
    pub(super) selector: &'s mut Selector<S>,
}

impl<S> Stream for WithId<'_, S>
where
    S: PollStrategy,
    S: PollWith<'static, (), ()>,
{
    type Item = (S::Progress, Id<S::Pollable>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .selector
            .poll_next_inner(&(), &mut (), |task| Id(Arc::downgrade(task)), cx)
    }
}

/// Borrowed [`Stream`] that will poll the inner [`Selector`], pass the extensions the inner tasks,
/// and attach the origin task [`Id`] to every item.
///
/// Created with [`Selector::with_ext_and_id`].
///
/// **Important:** before polling the tasks with different extension types, see the wakeups [section](Selector#wakeups).
pub struct WithExtAndId<'s, 'e, 'emut, S: PollStrategy, E: ?Sized, EMut: ?Sized> {
    pub(super) selector: &'s mut Selector<S>,
    pub(super) ext: &'e E,
    pub(super) ext_mut: &'emut mut EMut,
}

impl<'e, S, E, EMut> Stream for WithExtAndId<'_, 'e, '_, S, E, EMut>
where
    S: PollStrategy,
    S: PollWith<'e, E, EMut>,
    E: ?Sized,
    EMut: ?Sized,
{
    type Item = (S::Progress, Id<S::Pollable>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.selector
            .poll_next_inner(this.ext, this.ext_mut, |task| Id(Arc::downgrade(task)), cx)
    }
}
