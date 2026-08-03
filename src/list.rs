use std::{
    cell::{Cell, UnsafeCell},
    marker::PhantomData,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut, Not},
    pin::Pin,
    rc::Rc,
    sync::Arc,
};

use crate::queue::{self, Queue};

/// Instrusive doubly-linked list of [`Node`]s.
///
/// `T` values are stored inside shared allocations,
/// but the list remains their only owner.
/// The values:
/// 1. Can only be accessed through the list
/// 2. Do not outlive the list (unless the [`Node`] is removed)
pub struct List<T> {
    /// Pointer to the front node.
    ///
    /// [`std::ptr::null`] if the list is empty.
    front: *const Node<T>,
    /// Cound of nodes stored in the list.
    len: usize,
}

impl<T> List<T> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Builds a [`Node`], stores it in this list, and enqueues it in the given [`Queue`].
    ///
    /// Returns a mutable reference to the value.
    pub fn push_back<'a>(
        &'a mut self,
        queue: &Arc<Queue<ListProtected<T>>>,
        value: T,
    ) -> Borrowed<&'a mut T> {
        let node = queue.create(ListProtected {
            next: UnsafeCell::new(std::ptr::null()),
            prev: UnsafeCell::new(std::ptr::null()),
            value: UnsafeCell::new(ManuallyDrop::new(value)),
        });

        let node_ptr = Arc::as_ptr(&node);
        let (prev, next) = if self.front.is_null() {
            self.front = node_ptr;
            (node_ptr, node_ptr)
        } else {
            let back = unsafe { *(&*self.front).value().prev.get() };
            unsafe {
                *(&*self.front).value().prev.get() = node_ptr;
                *(&*back).value().next.get() = node_ptr;
            }
            (back, self.front)
        };

        unsafe {
            *node.value().prev.get() = prev;
            *node.value().next.get() = next;
        }
        self.len += 1;
        let borrowed = Borrowed(ManuallyDrop::new(node), PhantomData);

        unsafe {
            queue.enqueue(borrowed.node().clone());
        }

        borrowed
    }

    /// Returns a reference to the value stored in the given [`Node`].
    ///
    /// Returns [`None`] if that value was removed.
    ///
    /// # Safety
    ///
    /// Caller must ensure that this node either:
    /// 1. is stored in this list
    /// 2. was removed from this list
    pub unsafe fn get<'a>(&'a self, node: &Arc<Node<T>>) -> Option<Borrowed<&'a T>> {
        let unlinked = unsafe { (*node.value().next.get()).is_null() };
        if unlinked {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(Arc::as_ptr(node)) });
        Some(Borrowed(node, PhantomData))
    }

    /// Returns a reference to the value stored in the given [`Node`].
    ///
    /// Returns [`None`] if that value was removed.
    ///
    /// # Safety
    ///
    /// Caller must ensure that this node either:
    /// 1. is stored in this list
    /// 2. was removed from this list
    pub unsafe fn get_mut<'a>(&'a mut self, node: &Arc<Node<T>>) -> Option<Borrowed<&'a mut T>> {
        let unlinked = unsafe { (*node.value().next.get()).is_null() };
        if unlinked {
            return None;
        }
        let node: ManuallyDrop<Arc<queue::Node<ListProtected<T>>>> =
            ManuallyDrop::new(unsafe { Arc::from_raw(Arc::as_ptr(node)) });
        Some(Borrowed(node, PhantomData))
    }

    /// Removed the value stored in the given [`Node`].
    ///
    /// Returns [`None`] if that value was removed.
    ///
    /// # Safety
    ///
    /// Caller must ensure that this node either:
    /// 1. is stored in this list
    /// 2. was removed from this list
    pub unsafe fn remove(&mut self, node: &Arc<Node<T>>) -> Option<Removed<T>> {
        let unlinked = unsafe { (*node.value().next.get()).is_null() };
        if unlinked {
            return None;
        }
        Some(unsafe { self.remove_unchecked(Arc::as_ptr(node)) })
    }

    /// Returns an [`AccessGuard`] for the value stored in the given [`Node`].
    ///
    /// Returns [`None`] if that value was removed.
    ///
    /// # Safety
    ///
    /// Caller must ensure that this node either:
    /// 1. is stored in this list
    /// 2. was removed from this list
    pub unsafe fn access<'a>(&'a mut self, node: &Arc<Node<T>>) -> Option<AccessGuard<'a, T>> {
        let unlinked = unsafe { (*node.value().next.get()).is_null() };
        if unlinked {
            return None;
        }
        Some(AccessGuard {
            list: self,
            node: Arc::as_ptr(node),
        })
    }

    pub fn cursor(&self) -> Cursor<T, &'_ Self> {
        let front = self.front;
        let back = if front.is_null() {
            std::ptr::null()
        } else {
            unsafe { *(&*front).value().prev.get() }
        };
        Cursor {
            state: CursorState {
                front,
                back,
                popped: 0,
            },
            list: self,
        }
    }

    pub fn cursor_mut(&mut self) -> Cursor<T, &'_ mut Self> {
        let front = self.front;
        let back = if front.is_null() {
            std::ptr::null()
        } else {
            unsafe { *(&*front).value().prev.get() }
        };
        Cursor {
            state: CursorState {
                front,
                back,
                popped: 0,
            },
            list: self,
        }
    }

    unsafe fn remove_unchecked(&mut self, node: *const Node<T>) -> Removed<T> {
        let prev = unsafe { *(&*node).value().prev.get() };
        let next = unsafe { *(&*node).value().next.get() };

        unsafe {
            *(&*prev).value().next.get() = next;
            *(&*next).value().prev.get() = prev;
            *(&*node).value().next.get() = std::ptr::null();
            *(&*node).value().prev.get() = std::ptr::null();
        }

        self.len -= 1;
        if self.len == 0 {
            self.front = std::ptr::null();
        } else if self.front == node {
            self.front = next;
        }

        Removed(
            ManuallyDrop::new(unsafe { Arc::from_raw(node) }),
            PhantomData,
        )
    }
}

impl<T> Default for List<T> {
    fn default() -> Self {
        Self {
            front: std::ptr::null(),
            len: 0,
        }
    }
}

impl<T> Drop for List<T> {
    fn drop(&mut self) {
        struct Guard<'a, T>(&'a mut List<T>);

        impl<T> Drop for Guard<'_, T> {
            fn drop(&mut self) {
                let inner = Guard(self.0);

                while inner.0.front.is_null().not() {
                    unsafe { inner.0.remove_unchecked(inner.0.front) };
                }

                let _ = ManuallyDrop::new(inner);
            }
        }

        drop(Guard(self));
    }
}

unsafe impl<T: Send> Send for List<T> {}

unsafe impl<T: Sync> Sync for List<T> {}

/// Opaque storage for values stored in a [`List`].
///
/// Can only be accessed through the list.
pub struct ListProtected<T> {
    next: UnsafeCell<*const Node<T>>,
    prev: UnsafeCell<*const Node<T>>,
    value: UnsafeCell<ManuallyDrop<T>>,
}

unsafe impl<T> Send for ListProtected<T> {}

unsafe impl<T> Sync for ListProtected<T> {}

pub type Node<T> = queue::Node<ListProtected<T>>;

/// Reference to a value stored in the list.
pub struct Borrowed<T>(ManuallyDrop<Arc<Node<T::Target>>>, PhantomData<T>)
where
    T: Deref,
    T::Target: Sized;

impl<T> Borrowed<T>
where
    T: Deref,
    T::Target: Sized,
{
    pub fn get_pin(&self) -> Pin<&T::Target> {
        unsafe {
            let value = &*self.0.value().value.get();
            Pin::new_unchecked(value)
        }
    }

    pub fn node(&self) -> &Arc<Node<T::Target>> {
        &self.0
    }
}

static_assertions::assert_impl_all!(Borrowed<&'static ()>: Send, Sync);
static_assertions::assert_not_impl_any!(Borrowed<&'static Cell<()>>: Send, Sync);
static_assertions::assert_not_impl_any!(Borrowed<&'static Rc<()>>: Send, Sync);

static_assertions::assert_impl_all!(Borrowed<&'static mut ()>: Send, Sync);
static_assertions::assert_impl_all!(Borrowed<&'static mut Cell<()>>: Send);
static_assertions::assert_not_impl_any!(Borrowed<&'static mut Cell<()>>: Sync);
static_assertions::assert_not_impl_any!(Borrowed<&'static mut Rc<()>>: Send, Sync);

impl<'a, T> Borrowed<&'a T> {
    pub fn into_pin(self) -> Pin<&'a T> {
        unsafe {
            let value = &*self.0.value().value.get();
            Pin::new_unchecked(value)
        }
    }
}

impl<T> Borrowed<T>
where
    T: DerefMut,
    T::Target: Sized,
{
    pub fn get_pin_mut(&mut self) -> Pin<&mut T::Target> {
        unsafe {
            let value = &mut *self.0.value().value.get();
            Pin::new_unchecked(&mut *value)
        }
    }
}

impl<'a, T> Borrowed<&'a mut T> {
    pub fn into_pin_mut(self) -> Pin<&'a mut T> {
        unsafe {
            let value = &mut *self.0.value().value.get();
            Pin::new_unchecked(&mut *value)
        }
    }
}

/// Wrapper for a value removed from the list.
///
/// Owns the `T` value.
pub struct Removed<T>(ManuallyDrop<Arc<Node<T>>>, PhantomData<T>);

impl<T> Removed<T> {
    pub fn get_pin(&self) -> Pin<&T> {
        unsafe {
            let value = &*self.0.value().value.get();
            Pin::new_unchecked(value)
        }
    }

    pub fn get_pin_mut(&mut self) -> Pin<&mut T> {
        unsafe {
            let value = &mut *self.0.value().value.get();
            Pin::new_unchecked(&mut *value)
        }
    }

    pub fn node(&self) -> &Arc<Node<T>> {
        &self.0
    }

    pub fn into_inner(self) -> T
    where
        T: Unpin,
    {
        let mut this = ManuallyDrop::new(self);
        unsafe {
            let node = ManuallyDrop::take(&mut this.0);
            let value = &mut *node.value().value.get();
            ManuallyDrop::take(value)
        }
    }
}

impl<T> Drop for Removed<T> {
    fn drop(&mut self) {
        unsafe {
            let node = ManuallyDrop::take(&mut self.0);
            let value = &mut *node.value().value.get();
            ManuallyDrop::drop(value);
        }
    }
}

static_assertions::assert_impl_all!(Removed<()>: Send, Sync);
static_assertions::assert_impl_all!(Removed<Cell<()>>: Send);
static_assertions::assert_not_impl_any!(Removed<Cell<()>>: Sync);
static_assertions::assert_not_impl_any!(Removed<Rc<()>>: Send, Sync);

struct CursorState<T> {
    front: *const Node<T>,
    back: *const Node<T>,
    popped: usize,
}

unsafe impl<T> Send for CursorState<T> {}

unsafe impl<T> Sync for CursorState<T> {}

/// Double-ended cursor over a [`List`].
pub struct Cursor<T, L> {
    state: CursorState<T>,
    list: L,
}

impl<'a, T> Cursor<T, &'a List<T>> {
    pub fn len(&self) -> usize {
        self.list.len - self.state.popped
    }

    pub fn is_empty(&self) -> bool {
        self.list.len == self.state.popped
    }

    pub fn pop_front(&mut self) -> Option<Borrowed<&'a T>> {
        if self.is_empty() {
            None
        } else {
            let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.state.front) });
            self.state.popped += 1;
            self.state.front = unsafe { *node.value().next.get() };
            Some(Borrowed(node, PhantomData))
        }
    }

    pub fn pop_back(&mut self) -> Option<Borrowed<&'a T>> {
        if self.is_empty() {
            None
        } else {
            let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.state.back) });
            self.state.popped += 1;
            self.state.back = unsafe { *node.value().prev.get() };
            Some(Borrowed(node, PhantomData))
        }
    }
}

impl<'a, T> Cursor<T, &'a mut List<T>> {
    pub fn len(&self) -> usize {
        self.list.len - self.state.popped
    }

    pub fn is_empty(&self) -> bool {
        self.list.len == self.state.popped
    }

    pub fn pop_front(&mut self) -> Option<Borrowed<&'a mut T>> {
        if self.is_empty() {
            None
        } else {
            let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.state.front) });
            self.state.popped += 1;
            self.state.front = unsafe { *node.value().next.get() };
            Some(Borrowed(node, PhantomData))
        }
    }

    pub fn peek_front(&mut self) -> Option<Borrowed<&mut T>> {
        if self.is_empty() {
            None
        } else {
            let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.state.front) });
            Some(Borrowed(node, PhantomData))
        }
    }

    pub fn remove_front(&mut self) -> Option<Removed<T>> {
        if self.is_empty() {
            None
        } else {
            let to_remove = self.state.front;
            self.state.front = unsafe { *(&*to_remove).value().next.get() };
            let node = unsafe { self.list.remove_unchecked(to_remove) };
            Some(node)
        }
    }

    pub fn pop_back(&mut self) -> Option<Borrowed<&'a mut T>> {
        if self.is_empty() {
            None
        } else {
            let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.state.back) });
            self.state.popped += 1;
            self.state.back = unsafe { *node.value().prev.get() };
            Some(Borrowed(node, PhantomData))
        }
    }

    pub fn peek_back(&mut self) -> Option<Borrowed<&mut T>> {
        if self.is_empty() {
            None
        } else {
            let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.state.back) });
            Some(Borrowed(node, PhantomData))
        }
    }

    pub fn remove_back(&mut self) -> Option<Removed<T>> {
        if self.is_empty() {
            None
        } else {
            let to_remove = self.state.back;
            self.state.back = unsafe { *(&*to_remove).value().prev.get() };
            let node = unsafe { self.list.remove_unchecked(to_remove) };
            Some(node)
        }
    }
}

static_assertions::assert_impl_all!(Cursor<(), &'static List<()>>: Send, Sync);
static_assertions::assert_not_impl_any!(Cursor<Cell<()>, &'static List<Cell<()>>>: Send, Sync);
static_assertions::assert_not_impl_any!(Cursor<Rc<()>, &'static List<Rc<()>>>: Send, Sync);

static_assertions::assert_impl_all!(Cursor<(), &'static mut List<()>>: Send, Sync);
static_assertions::assert_impl_all!(Cursor<Cell<()>, &'static mut List<Cell<()>>>: Send);
static_assertions::assert_not_impl_any!(Cursor<Cell<()>, &'static mut List<Cell<()>>>: Sync);
static_assertions::assert_not_impl_any!(Cursor<Rc<()>, &'static mut List<Rc<()>>>: Send, Sync);

/// Guard for accessing a value stored in a [`List`].
///
/// Removes the value from the list on drop.
pub struct AccessGuard<'a, T> {
    list: &'a mut List<T>,
    node: *const Node<T>,
}

impl<T> AccessGuard<'_, T> {
    pub fn borrow_mut(&mut self) -> Borrowed<&'_ mut T> {
        Borrowed(
            ManuallyDrop::new(unsafe { Arc::from_raw(self.node) }),
            PhantomData,
        )
    }

    pub fn remove_now(self) -> Removed<T> {
        let mut this = ManuallyDrop::new(self);
        let node = this.node;
        unsafe { this.list.remove_unchecked(node) }
    }

    /// Consumes this guard without removing the value from the list.
    pub fn forget(self) {
        let _ = ManuallyDrop::new(self);
    }
}

impl<T> Drop for AccessGuard<'_, T> {
    fn drop(&mut self) {
        unsafe { self.list.remove_unchecked(self.node) };
    }
}

#[cfg(test)]
mod test {
    use std::{collections::VecDeque, ops::Not, panic::AssertUnwindSafe, sync::Arc};

    use crate::{list::List, queue::Receiver};

    #[test]
    fn list_drops_values_on_drop() {
        let queue = Receiver::default();
        let mut list = List::default();

        let nodes = std::iter::repeat_with(|| {
            let node = Arc::new(());
            let weak = Arc::downgrade(&node);
            list.push_back(queue.queue(), node);
            weak
        })
        .take(4)
        .collect::<Vec<_>>();

        for node in &nodes {
            assert!(node.upgrade().is_some());
        }
        drop(list);
        for node in &nodes {
            assert!(node.upgrade().is_none());
        }
    }

    #[test]
    fn list_drops_values_on_drop_after_panic() {
        struct Node(bool);

        impl Drop for Node {
            fn drop(&mut self) {
                if self.0 {
                    panic!("test panic");
                }
            }
        }

        let queue = Receiver::default();
        let mut list = List::default();

        let nodes = std::iter::repeat_n(false, 10)
            .chain(std::iter::once(true))
            .chain(std::iter::repeat_n(false, 10))
            .map(|should_panic| {
                let node = Arc::new(Node(should_panic));
                let weak = Arc::downgrade(&node);
                list.push_back(queue.queue(), node);
                weak
            })
            .collect::<Vec<_>>();

        for node in &nodes {
            assert!(node.upgrade().is_some());
        }

        let list = AssertUnwindSafe(list);
        std::panic::catch_unwind(|| drop(list)).unwrap_err();

        for node in &nodes {
            assert!(node.upgrade().is_none());
        }
    }

    #[test]
    fn list_allows_node_access() {
        let queue = Receiver::default();
        let mut list = List::default();

        let node = list.push_back(queue.queue(), 0);
        assert_eq!(*node.get_pin(), 0);

        let node = node.node().clone();
        assert_eq!(list.len(), 1);
        assert!(list.is_empty().not());

        let borrowed = unsafe { list.get(&node) }.unwrap();
        assert_eq!(*borrowed.get_pin(), 0);
        assert_eq!(Arc::as_ptr(borrowed.node()), Arc::as_ptr(&node));

        let borrowed_2 = unsafe { list.get(&node) }.unwrap();
        assert_eq!(*borrowed_2.get_pin(), 0);
        assert_eq!(Arc::as_ptr(borrowed.node()), Arc::as_ptr(borrowed_2.node()));

        let mut borrowed = unsafe { list.get_mut(&node) }.unwrap();
        assert_eq!(*borrowed.get_pin(), 0);
        *borrowed.get_pin_mut() += 1;

        let removed = unsafe { list.remove(&node) }.unwrap();
        assert_eq!(*removed.get_pin(), 1);

        assert_eq!(list.len(), 0);
        assert!(list.is_empty());
    }

    #[test]
    fn cursor_order() {
        let queue = Receiver::default();
        let mut list = List::default();

        for i in 0..10 {
            list.push_back(queue.queue(), i);
        }

        let mut cursor = list.cursor();
        let mut expected = (0..10).collect::<VecDeque<_>>();

        let mut front = true;
        while expected.is_empty().not() {
            assert_eq!(cursor.len(), expected.len());
            assert!(cursor.is_empty().not());

            if front {
                let found = *cursor.pop_front().unwrap().get_pin();
                let expected = expected.pop_front().unwrap();
                assert_eq!(found, expected);
            } else {
                let found = *cursor.pop_back().unwrap().get_pin();
                let expected = expected.pop_back().unwrap();
                assert_eq!(found, expected);
            }
            front = front.not();
        }

        assert_eq!(cursor.len(), 0);
        assert!(cursor.is_empty());
        assert!(cursor.pop_front().is_none());
        assert!(cursor.pop_back().is_none());
    }

    #[test]
    fn cursor_mut_order() {
        let queue = Receiver::default();
        let mut list = List::default();

        for i in 0..10 {
            list.push_back(queue.queue(), i);
        }

        let mut cursor = list.cursor_mut();
        let mut expected = (0..10).collect::<VecDeque<_>>();

        let mut front = true;
        while expected.is_empty().not() {
            assert_eq!(cursor.len(), expected.len());
            assert!(cursor.is_empty().not());

            if front {
                let found_1 = *cursor.peek_front().unwrap().get_pin();
                let found_2 = *cursor.pop_front().unwrap().get_pin();
                let expected = expected.pop_front().unwrap();
                assert_eq!(found_1, expected);
                assert_eq!(found_2, expected);
            } else {
                let found_1 = *cursor.peek_back().unwrap().get_pin();
                let found_2 = *cursor.pop_back().unwrap().get_pin();
                let expected = expected.pop_back().unwrap();
                assert_eq!(found_1, expected);
                assert_eq!(found_2, expected);
            }
            front = front.not();
        }

        assert_eq!(cursor.len(), 0);
        assert!(cursor.is_empty());
        assert!(cursor.pop_front().is_none());
        assert!(cursor.pop_back().is_none());
    }

    #[test]
    fn removal_with_cursor_mut() {
        let queue = Receiver::default();
        let mut list = List::default();

        for i in 0_u32..10 {
            list.push_back(queue.queue(), i);
        }

        let mut cursor = list.cursor_mut();
        while let Some(front) = cursor.peek_front() {
            if front.get_pin().is_power_of_two() {
                cursor.remove_front();
            } else {
                cursor.pop_front();
            }
        }

        assert_eq!(list.len(), 6);
        assert!(list.is_empty().not());
        let mut cursor = list.cursor();
        while let Some(elem) = cursor.pop_front() {
            assert!(elem.get_pin().is_power_of_two().not());
        }

        let mut cursor = list.cursor_mut();
        while cursor.remove_back().is_some() {}

        assert_eq!(list.len(), 0);
        assert!(list.is_empty());
    }
}
