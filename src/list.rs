use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    pin::Pin,
    sync::Arc,
};

use crate::list::cursor::{BorrowedNode, BorrowedNodeMut};

pub mod cursor;

/// # Safety
///
/// Implementor must ensure that returned addresses **never** change
/// and [`ListProtected`] is never modified.
pub unsafe trait StoredInList: Sized {
    type Protected;

    fn list_protected(&self) -> &ListProtected<Self>;
}

pub struct ListProtected<T: StoredInList>(UnsafeCell<ListProtectedInner<T>>);

impl<T: StoredInList> ListProtected<T> {
    /// Returns an immutable reference to the data.
    ///
    /// # Safety
    ///
    /// The returned value must not be concurrently mutated.
    unsafe fn get_unchecked(&self) -> &ListProtectedInner<T> {
        unsafe { self.0.get().as_ref_unchecked() }
    }

    /// Returns a mutable reference to the data.
    ///
    /// # Safety
    ///
    /// The returned value must not be concurrently accessed in any way.
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut_unchecked(&self) -> &mut ListProtectedInner<T> {
        unsafe { self.0.get().as_mut_unchecked() }
    }
}

impl<T: StoredInList> Default for ListProtected<T> {
    fn default() -> Self {
        Self(UnsafeCell::new(ListProtectedInner {
            next: std::ptr::null(),
            prev: std::ptr::null(),
            data: MaybeUninit::uninit(),
        }))
    }
}

/// SAFETY: [`ListProtected`] is only accessed from [`IntrusiveList`]/[`Removed`]/[`BorrowedNode`]/[`BorrowedNodeMut`].
/// These types uses stricter `impl Send` guard.
unsafe impl<T: StoredInList> Send for ListProtected<T> {}

/// SAFETY: [`ListProtected`] is only accessed from [`IntrusiveList`]/[`Removed`]/[`BorrowedNode`]/[`BorrowedNodeMut`].
/// The list uses stricter `impl Sync` guard.
unsafe impl<T: StoredInList> Sync for ListProtected<T> {}

/// This type is private, ensuring that no code outside this module has access to it.
struct ListProtectedInner<T: StoredInList> {
    next: *const T,
    prev: *const T,
    data: MaybeUninit<T::Protected>,
}

impl<T: StoredInList> ListProtectedInner<T> {
    fn is_unlinked(&self) -> bool {
        self.next.is_null()
    }

    fn unlink(&mut self) {
        self.next = std::ptr::null_mut();
        self.prev = std::ptr::null_mut();
    }
}

/// Intrusive doubly-linked list of [`Arc`] nodes.
///
/// The list is a single owner of the [`ListProtected`] values that live in the stored nodes.
/// [`StoredInList::Protected`] data of the nodes can only be accessed through the list,
/// ensuring that no borrowing rules are violated.
/// When dropped, the list drops [`StoredInList::Protected`] in all nodes.
/// Therefore:
/// 1. [`StoredInList::Protected`] never outlives the list (unless the node is removed)
/// 2. [`ListProtected`] is always [`Send`] and [`Sync`], regardless of [`StoredInList::Protected`]
pub struct IntrusiveList<T: StoredInList> {
    /// Pointer to the head node.
    ///
    /// [`std::ptr::null`] if the list is empty.
    head: *const T,
    /// Count of all nodes in the list.
    len: usize,
}

impl<T: StoredInList> IntrusiveList<T> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn insert(&mut self, node: T, data: T::Protected) -> Arc<T> {
        let new_node = Arc::new(node);
        unsafe {
            new_node
                .list_protected()
                .get_mut_unchecked()
                .data
                .write(data);
        }
        let new_node_ptr = Arc::as_ref(&new_node);

        unsafe {
            let protected = new_node.list_protected().get_mut_unchecked();
            match self.head.as_ref() {
                Some(head) => {
                    protected.next = head;
                    protected.prev = head.list_protected().get_unchecked().prev;
                    head.list_protected()
                        .get_unchecked()
                        .prev
                        .as_ref_unchecked()
                        .list_protected()
                        .get_mut_unchecked()
                        .next = new_node_ptr;
                    head.list_protected().get_mut_unchecked().prev = new_node_ptr;
                }
                None => {
                    protected.next = new_node_ptr;
                    protected.prev = new_node_ptr;
                    self.head = new_node_ptr;
                }
            }
        }

        unsafe {
            Arc::increment_strong_count(new_node_ptr);
        }
        self.len += 1;

        new_node
    }

    /// Returns an [`AccessMut`] guard to the node, if the node is currently stored in the list.
    ///
    /// Returns `None` if the node is stored in the list.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the node is not stored in any other list.
    pub unsafe fn access<'a>(&'a mut self, node: &'a T) -> Option<AccessMut<'a, T>> {
        let is_unlinked = unsafe { node.list_protected().get_unchecked().is_unlinked() };
        if is_unlinked {
            return None;
        }
        Some(AccessMut { list: self, node })
    }

    /// Removes the node from the list.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the item is stored in the list.
    unsafe fn remove_unchecked(&mut self, node: &T) -> Removed<T> {
        let (item, next) = unsafe {
            let next = node.list_protected().get_unchecked().next;
            let prev = node.list_protected().get_unchecked().prev;
            next.as_ref_unchecked()
                .list_protected()
                .get_mut_unchecked()
                .prev = prev;
            prev.as_ref_unchecked()
                .list_protected()
                .get_mut_unchecked()
                .next = next;
            node.list_protected().get_mut_unchecked().unlink();
            (Arc::from_raw(node), next)
        };

        self.len -= 1;

        if self.len == 0 {
            self.head = std::ptr::null_mut();
        } else if std::ptr::eq(self.head, Arc::as_ptr(&item)) {
            self.head = next;
        }

        Removed(ManuallyDrop::new(item), PhantomData)
    }

    /// Returns a reference to the node, if the node is currently stored in the list.
    ///
    /// Returns `None` if the node is stored in the list.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the node is not stored in any other list.
    pub unsafe fn get(&self, node: &T) -> Option<BorrowedNode<'_, T>> {
        let is_unlinked = unsafe { node.list_protected().get_unchecked().is_unlinked() };
        if is_unlinked {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(node) });
        Some(BorrowedNode {
            node,
            _lifetime_guard: PhantomData,
        })
    }

    /// Returns a mutable reference to the node, if the node is currently stored in the list.
    ///
    /// Returns `None` if the node is stored in the list.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the node is not stored in any other list.
    pub unsafe fn get_mut(&mut self, node: &T) -> Option<BorrowedNodeMut<'_, T>> {
        let is_unlinked = unsafe { node.list_protected().get_unchecked().is_unlinked() };
        if is_unlinked {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(node) });
        Some(BorrowedNodeMut {
            node,
            _lifetime_guard: PhantomData,
        })
    }

    /// Removes the node, if the node is currently stored in the list.
    ///
    /// Returns `None` if the node is stored in the list.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the node is not stored in any other list.
    pub unsafe fn remove(&mut self, node: &T) -> Option<Removed<T>> {
        let is_unlinked = unsafe { node.list_protected().get_unchecked().is_unlinked() };
        if is_unlinked {
            return None;
        }
        let node = unsafe { self.remove_unchecked(node) };
        Some(node)
    }
}

impl<T: StoredInList> Default for IntrusiveList<T> {
    fn default() -> Self {
        Self {
            head: std::ptr::null(),
            len: 0,
        }
    }
}

impl<T: StoredInList> Drop for IntrusiveList<T> {
    fn drop(&mut self) {
        struct DestroyOnDrop<'a, T: StoredInList>(&'a mut IntrusiveList<T>);

        impl<T: StoredInList> Drop for DestroyOnDrop<'_, T> {
            fn drop(&mut self) {
                let inner_guard = DestroyOnDrop(self.0);
                while let Some(head) = unsafe { inner_guard.0.head.as_ref() } {
                    unsafe { inner_guard.0.remove_unchecked(head) };
                }
                let _ = ManuallyDrop::new(inner_guard);
            }
        }

        DestroyOnDrop(self);
    }
}

unsafe impl<T> Send for IntrusiveList<T>
where
    T: StoredInList + Send,
    T::Protected: Send,
{
}

unsafe impl<T> Sync for IntrusiveList<T>
where
    T: StoredInList + Send + Sync,
    T::Protected: Send + Sync,
{
}

/// Node removed from an [`IntrusiveList`].
pub struct Removed<T: StoredInList>(
    ManuallyDrop<Arc<T>>,
    /// This marker ensures that the struct has correct [`Send`] and [`Sync`].
    PhantomData<Box<T::Protected>>,
);

impl<T: StoredInList> Removed<T> {
    pub fn node(&self) -> &Arc<T> {
        &self.0
    }

    pub fn protected(&self) -> &T::Protected {
        unsafe {
            self.0
                .list_protected()
                .get_unchecked()
                .data
                .assume_init_ref()
        }
    }

    pub fn protected_mut(&mut self) -> Pin<&mut T::Protected> {
        unsafe {
            let as_mut = self
                .0
                .list_protected()
                .get_mut_unchecked()
                .data
                .assume_init_mut();
            Pin::new_unchecked(as_mut)
        }
    }

    pub fn into_inner(mut self) -> T::Protected
    where
        T::Protected: Unpin,
    {
        let data = unsafe {
            self.0
                .list_protected()
                .get_mut_unchecked()
                .data
                .assume_init_read()
        };
        let node = unsafe { ManuallyDrop::take(&mut self.0) };
        let _ = ManuallyDrop::new(self);
        drop(node);
        data
    }
}

impl<T: StoredInList> Drop for Removed<T> {
    fn drop(&mut self) {
        unsafe {
            self.0
                .list_protected()
                .get_mut_unchecked()
                .data
                .assume_init_drop();
            ManuallyDrop::drop(&mut self.0);
        }
    }
}

/// Access guard for a node stored in an [`IntrusiveList`].
///
/// Allows for accessing [`StoredInList::Protected`] data.
///
/// Removed the node from the list on drop.
pub struct AccessMut<'a, T: StoredInList> {
    list: &'a mut IntrusiveList<T>,
    node: &'a T,
}

impl<T: StoredInList> AccessMut<'_, T> {
    pub fn get(&mut self) -> Pin<&mut T::Protected> {
        unsafe {
            Pin::new_unchecked(
                self.node
                    .list_protected()
                    .get_mut_unchecked()
                    .data
                    .assume_init_mut(),
            )
        }
    }

    /// Forgets this guard without removing the node from the list.
    pub fn forget(self) {
        let _ = ManuallyDrop::new(self);
    }
}

impl<T: StoredInList> Drop for AccessMut<'_, T> {
    fn drop(&mut self) {
        unsafe {
            self.list.remove_unchecked(self.node);
        }
    }
}

#[cfg(test)]
mod test_helpers {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::{IntrusiveList, ListProtected, StoredInList};

    pub(super) struct TestNode {
        list_protected: ListProtected<Self>,
        node_drops: Arc<AtomicUsize>,
    }

    impl TestNode {
        pub(super) fn new(node_drops: &Arc<AtomicUsize>) -> Self {
            Self {
                list_protected: Default::default(),
                node_drops: Arc::clone(node_drops),
            }
        }
    }

    impl Drop for TestNode {
        fn drop(&mut self) {
            self.node_drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub(super) struct ProtectedValue {
        pub(super) value: usize,
        protected_drops: Arc<AtomicUsize>,
    }

    impl ProtectedValue {
        pub(super) fn new(value: usize, protected_drops: &Arc<AtomicUsize>) -> Self {
            Self {
                value,
                protected_drops: Arc::clone(protected_drops),
            }
        }
    }

    impl Drop for ProtectedValue {
        fn drop(&mut self) {
            self.protected_drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    unsafe impl StoredInList for TestNode {
        type Protected = ProtectedValue;

        fn list_protected(&self) -> &ListProtected<Self> {
            &self.list_protected
        }
    }

    pub(super) fn insert_nodes(
        list: &mut IntrusiveList<TestNode>,
        ids: &[usize],
        node_drops: &Arc<AtomicUsize>,
        protected_drops: &Arc<AtomicUsize>,
    ) -> Vec<Arc<TestNode>> {
        ids.iter()
            .map(|id| {
                list.insert(
                    TestNode::new(node_drops),
                    ProtectedValue::new(*id * 10, protected_drops),
                )
            })
            .collect()
    }

    pub(super) fn raw_node(node_drops: &Arc<AtomicUsize>) -> Arc<TestNode> {
        Arc::new(TestNode::new(node_drops))
    }

    pub(super) fn assert_ring(list: &IntrusiveList<TestNode>, expected: &[&Arc<TestNode>]) {
        assert_eq!(list.len(), expected.len());
        assert_eq!(list.is_empty(), expected.is_empty());

        if expected.is_empty() {
            assert!(list.head.is_null());
            return;
        }

        assert!(std::ptr::eq(list.head, Arc::as_ptr(expected[0])));
        for (index, node) in expected.iter().enumerate() {
            let protected = unsafe { node.list_protected().get_unchecked() };
            let next = expected[(index + 1) % expected.len()];
            let prev = expected[(index + expected.len() - 1) % expected.len()];

            assert!(!protected.is_unlinked());
            assert!(std::ptr::eq(protected.next, Arc::as_ptr(next)));
            assert!(std::ptr::eq(protected.prev, Arc::as_ptr(prev)));
        }
    }

    pub(super) fn assert_unlinked(node: &Arc<TestNode>) {
        let protected = unsafe { node.list_protected().get_unchecked() };
        assert!(protected.is_unlinked());
        assert!(protected.prev.is_null());
    }
}

#[cfg(test)]
mod test {
    use std::{
        cell::Cell,
        ops::Not,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use crate::list::{
        ListProtected, Removed, StoredInList,
        cursor::{BorrowedNode, BorrowedNodeMut},
    };

    use super::{
        IntrusiveList,
        test_helpers::{assert_ring, assert_unlinked, insert_nodes, raw_node},
    };

    #[test]
    fn insert_and_remove_preserve_ring_invariants() {
        let node_drops = Arc::new(AtomicUsize::new(0));
        let protected_drops = Arc::new(AtomicUsize::new(0));
        let mut list = IntrusiveList::default();
        let nodes = insert_nodes(&mut list, &[1, 2, 3], &node_drops, &protected_drops);

        assert_ring(&list, &[&nodes[0], &nodes[1], &nodes[2]]);
        assert!(list.is_empty().not());
        for node in &nodes {
            assert_eq!(Arc::strong_count(node), 2);
        }

        let middle = unsafe { list.remove(nodes[1].as_ref()) }.unwrap();
        assert_ring(&list, &[&nodes[0], &nodes[2]]);
        assert!(Arc::ptr_eq(middle.node(), &nodes[1]));
        assert_eq!(middle.protected().value, 20);
        drop(middle);
        assert_eq!(protected_drops.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&nodes[1]), 1);

        let head = unsafe { list.remove(nodes[0].as_ref()) }.unwrap();
        assert_ring(&list, &[&nodes[2]]);
        drop(head);

        let tail = unsafe { list.remove(nodes[2].as_ref()) }.unwrap();
        assert_ring(&list, &[]);
        drop(tail);

        assert_eq!(protected_drops.load(Ordering::SeqCst), 3);
        assert_eq!(node_drops.load(Ordering::SeqCst), 0);
        drop(nodes);
        assert_eq!(node_drops.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn unlinked_nodes_are_rejected_by_all_accessors() {
        let node_drops = Arc::new(AtomicUsize::new(0));
        let protected_drops = Arc::new(AtomicUsize::new(0));
        let mut list = IntrusiveList::default();
        let raw = raw_node(&node_drops);

        assert!(unsafe { list.access(raw.as_ref()) }.is_none());
        assert!(unsafe { list.get(raw.as_ref()) }.is_none());
        assert!(unsafe { list.get_mut(raw.as_ref()) }.is_none());
        assert!(unsafe { list.remove(raw.as_ref()) }.is_none());

        let linked = insert_nodes(&mut list, &[1], &node_drops, &protected_drops)
            .into_iter()
            .next()
            .unwrap();
        let removed = unsafe { list.remove(linked.as_ref()) }.unwrap();

        assert_unlinked(removed.node());
        assert!(unsafe { list.access(removed.node().as_ref()) }.is_none());
        assert!(unsafe { list.get(removed.node().as_ref()) }.is_none());
        assert!(unsafe { list.get_mut(removed.node().as_ref()) }.is_none());
        assert!(unsafe { list.remove(removed.node().as_ref()) }.is_none());

        drop(removed);
        drop(linked);
        drop(raw);

        assert_eq!(protected_drops.load(Ordering::SeqCst), 1);
        assert_eq!(node_drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn get_and_access_mut_expose_protected_data_and_respect_forget() {
        let node_drops = Arc::new(AtomicUsize::new(0));
        let protected_drops = Arc::new(AtomicUsize::new(0));
        let mut list = IntrusiveList::default();
        let nodes = insert_nodes(&mut list, &[1, 2], &node_drops, &protected_drops);

        let borrowed = unsafe { list.get(nodes[0].as_ref()) }.unwrap();
        assert!(Arc::ptr_eq(borrowed.node(), &nodes[0]));
        assert_eq!(borrowed.get_protected().value, 10);

        let mut borrowed = unsafe { list.get_mut(nodes[0].as_ref()) }.unwrap();
        assert!(Arc::ptr_eq(borrowed.node(), &nodes[0]));
        assert_eq!(borrowed.get_protected().value, 10);
        borrowed.get_protected_mut().as_mut().get_mut().value = 11;

        let mut guard = unsafe { list.access(nodes[0].as_ref()) }.unwrap();
        let mut protected = guard.get();
        assert_eq!(protected.as_ref().get_ref().value, 11);
        protected.as_mut().get_mut().value = 12;
        guard.forget();

        assert_ring(&list, &[&nodes[0], &nodes[1]]);
        assert_eq!(
            unsafe { list.get(nodes[0].as_ref()) }
                .unwrap()
                .get_protected()
                .value,
            12
        );

        {
            let mut guard = unsafe { list.access(nodes[1].as_ref()) }.unwrap();
            guard.get().as_mut().get_mut().value = 99;
        }

        assert_ring(&list, &[&nodes[0]]);
        assert_unlinked(&nodes[1]);
        assert!(unsafe { list.get(nodes[1].as_ref()) }.is_none());
        assert_eq!(protected_drops.load(Ordering::SeqCst), 1);
        assert_eq!(node_drops.load(Ordering::SeqCst), 0);

        drop(nodes);
        assert_eq!(node_drops.load(Ordering::SeqCst), 1);
        drop(list);
        assert_eq!(protected_drops.load(Ordering::SeqCst), 2);
        assert_eq!(node_drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn removed_drop_releases_protected_but_not_external_node() {
        let node_drops = Arc::new(AtomicUsize::new(0));
        let protected_drops = Arc::new(AtomicUsize::new(0));
        let mut list = IntrusiveList::default();
        let node = insert_nodes(&mut list, &[1], &node_drops, &protected_drops)
            .into_iter()
            .next()
            .unwrap();

        let mut removed = unsafe { list.remove(node.as_ref()) }.unwrap();
        assert!(Arc::ptr_eq(removed.node(), &node));
        assert_eq!(removed.protected().value, 10);
        removed.protected_mut().as_mut().get_mut().value = 42;
        assert_eq!(removed.protected().value, 42);

        drop(removed);
        assert_eq!(protected_drops.load(Ordering::SeqCst), 1);
        assert_eq!(node_drops.load(Ordering::SeqCst), 0);
        assert_eq!(Arc::strong_count(&node), 1);
        assert_unlinked(&node);

        drop(node);
        assert_eq!(node_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn removed_into_inner_returns_protected_without_immediate_drop() {
        let node_drops = Arc::new(AtomicUsize::new(0));
        let protected_drops = Arc::new(AtomicUsize::new(0));
        let mut list = IntrusiveList::default();
        let node = insert_nodes(&mut list, &[1], &node_drops, &protected_drops)
            .into_iter()
            .next()
            .unwrap();

        let mut removed = unsafe { list.remove(node.as_ref()) }.unwrap();
        removed.protected_mut().as_mut().get_mut().value = 77;

        let protected = removed.into_inner();
        assert_eq!(protected.value, 77);
        assert_eq!(protected_drops.load(Ordering::SeqCst), 0);
        assert_eq!(node_drops.load(Ordering::SeqCst), 0);
        assert_eq!(Arc::strong_count(&node), 1);
        assert_unlinked(&node);

        drop(node);
        assert_eq!(node_drops.load(Ordering::SeqCst), 1);
        drop(protected);
        assert_eq!(protected_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn dropping_list_drops_protected_and_unlinks_every_node() {
        let node_drops = Arc::new(AtomicUsize::new(0));
        let protected_drops = Arc::new(AtomicUsize::new(0));
        let nodes = {
            let mut list = IntrusiveList::default();
            let nodes = insert_nodes(&mut list, &[1, 2, 3], &node_drops, &protected_drops);
            assert_ring(&list, &[&nodes[0], &nodes[1], &nodes[2]]);
            nodes
        };

        assert_eq!(protected_drops.load(Ordering::SeqCst), 3);
        assert_eq!(node_drops.load(Ordering::SeqCst), 0);
        for node in &nodes {
            assert_eq!(Arc::strong_count(node), 1);
            assert_unlinked(node);
        }

        drop(nodes);
        assert_eq!(node_drops.load(Ordering::SeqCst), 3);
    }

    struct TestStoredInList<T>(ListProtected<Self>);

    unsafe impl<T> StoredInList for TestStoredInList<T> {
        type Protected = T;

        fn list_protected(&self) -> &ListProtected<Self> {
            &self.0
        }
    }

    static_assertions::assert_impl_all!(IntrusiveList<TestStoredInList<usize>>: Send, Sync);
    static_assertions::assert_impl_all!(Removed<TestStoredInList<usize>>: Send, Sync);
    static_assertions::assert_impl_all!(BorrowedNode<'static, TestStoredInList<usize>>: Send, Sync);
    static_assertions::assert_impl_all!(BorrowedNodeMut<'static, TestStoredInList<usize>>: Send, Sync);

    static_assertions::assert_impl_all!(IntrusiveList<TestStoredInList<Cell<usize>>>: Send);
    static_assertions::assert_not_impl_any!(IntrusiveList<TestStoredInList<Cell<usize>>>: Sync);
    static_assertions::assert_impl_all!(Removed<TestStoredInList<Cell<usize>>>: Send);
    static_assertions::assert_not_impl_any!(Removed<TestStoredInList<Cell<usize>>>: Sync);
    static_assertions::assert_not_impl_any!(BorrowedNode<'static, TestStoredInList<Cell<usize>>>: Send, Sync);
    static_assertions::assert_impl_all!(BorrowedNodeMut<'static, TestStoredInList<Cell<usize>>>: Send);
    static_assertions::assert_not_impl_any!(BorrowedNodeMut<'static, TestStoredInList<Cell<usize>>>: Sync);

    static_assertions::assert_not_impl_any!(IntrusiveList<TestStoredInList<Rc<usize>>>: Send, Sync);
    static_assertions::assert_not_impl_any!(Removed<TestStoredInList<Rc<usize>>>: Send, Sync);
    static_assertions::assert_not_impl_any!(BorrowedNode<'static, TestStoredInList<Rc<usize>>>: Send, Sync);
    static_assertions::assert_not_impl_any!(BorrowedNodeMut<'static, TestStoredInList<Rc<usize>>>: Send, Sync);
}
