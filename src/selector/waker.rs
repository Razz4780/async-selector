use std::{
    mem::ManuallyDrop,
    ops::Deref,
    sync::Arc,
    task::{RawWaker, RawWakerVTable, Waker},
};

use crate::queue::Node;

pub struct NodeWaker<'a, T> {
    waker: ManuallyDrop<Waker>,
    _lifetime_guard: &'a Arc<Node<T>>,
}

impl<'a, T> NodeWaker<'a, T> {
    pub fn new(node: &'a Arc<Node<T>>) -> Self {
        let data = Arc::as_ptr(node).cast();
        let waker = unsafe { Waker::new(data, &Self::VTABLE) };
        Self {
            waker: ManuallyDrop::new(waker),
            _lifetime_guard: node,
        }
    }

    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::do_clone,
        Self::do_wake,
        Self::do_wake_by_ref,
        Self::do_drop,
    );

    unsafe fn do_clone(data: *const ()) -> RawWaker {
        let typed = data.cast::<Node<T>>();
        unsafe {
            Arc::increment_strong_count(typed);
        }
        RawWaker::new(data, &Self::VTABLE)
    }

    unsafe fn do_wake(data: *const ()) {
        let typed = data.cast::<Node<T>>();
        let node = unsafe { Arc::from_raw(typed) };
        node.enqueue();
    }

    unsafe fn do_wake_by_ref(data: *const ()) {
        let typed = data.cast::<Node<T>>();
        let node = unsafe { Arc::from_raw(typed) };
        let node = ManuallyDrop::new(node);
        node.enqueue_by_ref();
    }

    unsafe fn do_drop(data: *const ()) {
        let typed = data.cast::<Node<T>>();
        unsafe {
            Arc::decrement_strong_count(typed);
        }
    }
}

impl<C> Deref for NodeWaker<'_, C> {
    type Target = Waker;

    fn deref(&self) -> &Self::Target {
        &self.waker
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::{queue::Receiver, selector::waker::NodeWaker};

    #[allow(clippy::waker_clone_wake, clippy::drop_non_drop)]
    #[test]
    fn refcount_and_wakeups() {
        let mut queue = Receiver::default();
        let node = queue.queue().create(());

        let waker = NodeWaker::new(&node);
        assert_eq!(waker.data(), Arc::as_ptr(&node).cast(),);

        let cloned = waker.clone();
        assert_eq!(Arc::strong_count(&node), 2);
        drop(cloned);
        assert_eq!(Arc::strong_count(&node), 1);

        waker.wake_by_ref();
        assert_eq!(Arc::strong_count(&node), 2);
        queue.dequeue().unwrap().into_inner();
        assert_eq!(Arc::strong_count(&node), 1);

        waker.clone().wake();
        assert_eq!(Arc::strong_count(&node), 2);
        queue.dequeue().unwrap().into_inner();
        assert_eq!(Arc::strong_count(&node), 1);

        drop(waker);
        assert_eq!(Arc::strong_count(&node), 1);
    }
}
