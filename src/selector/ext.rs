//! Wrapper types for [`Selector`] that allow for customizing polling behavior.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;

use crate::{
    pollable::PollProxy,
    selector::{Id, Selector},
};

/// Borrowed [`Stream`] that will poll the inner [`Selector`]
/// and pass the extensions to the inner tasks.
///
/// Created with [`Selector::with_ext`].
///
/// **Important:** before polling the tasks with different extension types, see the wakeups [section](Selector#wakeups).
pub struct WithExt<'s, 'e, 'emut, T, P, E: ?Sized, EMut: ?Sized> {
    pub(super) selector: &'s mut Selector<T, P>,
    pub(super) ext: &'e E,
    pub(super) ext_mut: &'emut mut EMut,
}

impl<'e, T, P, E, EMut> Stream for WithExt<'_, 'e, '_, T, P, E, EMut>
where
    P: PollProxy<'e, T, E, EMut>,
    E: ?Sized,
    EMut: ?Sized,
{
    type Item = P::Progress;

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
pub struct WithId<'s, T, P> {
    pub(super) selector: &'s mut Selector<T, P>,
}

impl<T, P> Stream for WithId<'_, T, P>
where
    P: PollProxy<'static, T, (), ()>,
{
    type Item = (P::Progress, Id<T>);

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
pub struct WithExtAndId<'s, 'e, 'emut, T, P, E: ?Sized, EMut: ?Sized> {
    pub(super) selector: &'s mut Selector<T, P>,
    pub(super) ext: &'e E,
    pub(super) ext_mut: &'emut mut EMut,
}

impl<'e, T, P, E, EMut> Stream for WithExtAndId<'_, 'e, '_, T, P, E, EMut>
where
    P: PollProxy<'e, T, E, EMut>,
    E: ?Sized,
    EMut: ?Sized,
{
    type Item = (P::Progress, Id<T>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.selector
            .poll_next_inner(this.ext, this.ext_mut, |task| Id(Arc::downgrade(task)), cx)
    }
}
