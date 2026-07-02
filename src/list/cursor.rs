use std::{marker::PhantomData, mem::ManuallyDrop, pin::Pin, sync::Arc};

use crate::list::{IntrusiveList, Removed, StoredInList};

/// Double-ended cursos that visits all nodes stored in an [`IntrusiveList`].
pub struct Cursor<'a, T: StoredInList> {
    list: &'a IntrusiveList<T>,
    front: *const T,
    back: *const T,
    skipped: usize,
}

impl<'a, T: StoredInList> Cursor<'a, T> {
    pub fn new(list: &'a IntrusiveList<T>) -> Self {
        let front = list.head;
        let back = unsafe {
            list.head
                .as_ref()
                .map(|head| head.list_protected().get_unchecked().prev)
                .unwrap_or(std::ptr::null())
        };
        Self {
            list,
            front,
            back,
            skipped: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.list.len() - self.skipped
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the element at the front of the cursor, and moves the front.
    pub fn pop_front(&mut self) -> Option<BorrowedNode<'a, T>> {
        if self.is_empty() {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.front) });
        self.front = unsafe { node.list_protected().get_unchecked().next };
        self.skipped += 1;
        Some(BorrowedNode {
            node,
            _lifetime_guard: PhantomData,
        })
    }

    /// Returns the element at the back of the cursor, and moves the back.
    pub fn pop_back(&mut self) -> Option<BorrowedNode<'a, T>> {
        if self.is_empty() {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.back) });
        self.back = unsafe { node.list_protected().get_unchecked().prev };
        self.skipped += 1;
        Some(BorrowedNode {
            node,
            _lifetime_guard: PhantomData,
        })
    }
}

/// Variant of [`Cursor`] that provides mutable access to the nodes.
pub struct CursorMut<'a, T: StoredInList> {
    list: &'a mut IntrusiveList<T>,
    front: *const T,
    back: *const T,
    skipped: usize,
}

impl<'a, T: StoredInList> CursorMut<'a, T> {
    pub fn new(list: &'a mut IntrusiveList<T>) -> Self {
        let front = list.head;
        let back = unsafe {
            list.head
                .as_ref()
                .map(|head| head.list_protected().get_unchecked().prev)
                .unwrap_or(std::ptr::null())
        };
        Self {
            list,
            front,
            back,
            skipped: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.list.len() - self.skipped
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the element at the front of the cursor, and moves the front.
    pub fn pop_front(&mut self) -> Option<BorrowedNodeMut<'a, T>> {
        if self.is_empty() {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.front) });
        self.front = unsafe { node.list_protected().get_unchecked().next };
        self.skipped += 1;
        Some(BorrowedNodeMut {
            node,
            _lifetime_guard: PhantomData,
        })
    }

    /// Returns the element at the front of the cursor.
    pub fn peek_front(&mut self) -> Option<BorrowedNodeMut<'a, T>> {
        if self.is_empty() {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.front) });
        Some(BorrowedNodeMut {
            node,
            _lifetime_guard: PhantomData,
        })
    }

    /// Removes the element at the front of the cursor.
    pub fn remove_front(&mut self) -> Option<Removed<T>> {
        if self.is_empty() {
            return None;
        }
        let to_remove = self.front;
        self.front = unsafe {
            self.front
                .as_ref_unchecked()
                .list_protected()
                .get_unchecked()
                .next
        };
        let removed = unsafe { self.list.remove_unchecked(to_remove.as_ref_unchecked()) };
        Some(removed)
    }

    /// Returns the element at the back of the cursor, and moves the back.
    pub fn pop_back(&mut self) -> Option<BorrowedNodeMut<'a, T>> {
        if self.is_empty() {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.back) });
        self.back = unsafe { node.list_protected().get_unchecked().prev };
        self.skipped += 1;
        Some(BorrowedNodeMut {
            node,
            _lifetime_guard: PhantomData,
        })
    }

    /// Returns the element at the back of the cursor.
    pub fn peek_back(&mut self) -> Option<BorrowedNodeMut<'a, T>> {
        if self.is_empty() {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.back) });
        Some(BorrowedNodeMut {
            node,
            _lifetime_guard: PhantomData,
        })
    }

    /// Removes the element at the back of the cursor.
    pub fn remove_back(&mut self) -> Option<Removed<T>> {
        if self.is_empty() {
            return None;
        }
        let to_remove = self.back;
        self.back = unsafe {
            self.back
                .as_ref_unchecked()
                .list_protected()
                .get_unchecked()
                .prev
        };
        let removed = unsafe { self.list.remove_unchecked(to_remove.as_ref_unchecked()) };
        Some(removed)
    }
}

/// Reference to a node stored in an [`IntrusiveList`].
pub struct BorrowedNode<'a, T: StoredInList> {
    pub(super) node: ManuallyDrop<Arc<T>>,
    /// This allows us to be sure that borrowing rules are not violated.
    pub(super) _lifetime_guard: PhantomData<&'a ()>,
}

impl<T: StoredInList> BorrowedNode<'_, T> {
    pub fn get_protected(&self) -> &T::Protected {
        unsafe {
            self.node
                .list_protected()
                .get_unchecked()
                .data
                .assume_init_ref()
        }
    }

    pub fn node(&self) -> &Arc<T> {
        &self.node
    }
}

/// Mutable reference to a node stored in an [`IntrusiveList`].
pub struct BorrowedNodeMut<'a, T: StoredInList> {
    pub(super) node: ManuallyDrop<Arc<T>>,
    /// This allows us to be sure that borrowing rules are not violated.
    pub(super) _lifetime_guard: PhantomData<&'a ()>,
}

impl<T: StoredInList> BorrowedNodeMut<'_, T> {
    pub fn get_protected(&self) -> &T::Protected {
        unsafe {
            self.node
                .list_protected()
                .get_unchecked()
                .data
                .assume_init_ref()
        }
    }

    pub fn get_protected_mut(&mut self) -> Pin<&mut T::Protected> {
        unsafe {
            let raw = self
                .node
                .list_protected()
                .get_mut_unchecked()
                .data
                .assume_init_mut();
            Pin::new_unchecked(raw)
        }
    }

    pub fn node(&self) -> &Arc<T> {
        &self.node
    }
}

#[cfg(test)]
mod test {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::{Cursor, CursorMut};
    use crate::list::{
        IntrusiveList,
        test_helpers::{TestNode, assert_ring, assert_unlinked, insert_nodes},
    };

    #[test]
    fn empty_cursors_report_empty_and_yield_nothing() {
        let list = IntrusiveList::<TestNode>::default();
        let mut cursor = Cursor::new(&list);
        assert_eq!(cursor.len(), 0);
        assert!(cursor.is_empty());
        assert!(cursor.pop_front().is_none());
        assert!(cursor.pop_back().is_none());

        let mut list = IntrusiveList::<TestNode>::default();
        let mut cursor = CursorMut::new(&mut list);
        assert_eq!(cursor.len(), 0);
        assert!(cursor.is_empty());
        assert!(cursor.peek_front().is_none());
        assert!(cursor.peek_back().is_none());
        assert!(cursor.pop_front().is_none());
        assert!(cursor.pop_back().is_none());
        assert!(cursor.remove_front().is_none());
        assert!(cursor.remove_back().is_none());
    }

    #[test]
    fn cursor_visits_nodes_from_both_ends_without_duplication() {
        let node_drops = Arc::new(AtomicUsize::new(0));
        let protected_drops = Arc::new(AtomicUsize::new(0));
        let mut list = IntrusiveList::default();
        let nodes = insert_nodes(&mut list, &[1, 2, 3, 4], &node_drops, &protected_drops);

        let mut cursor = Cursor::new(&list);
        assert_eq!(cursor.len(), 4);
        assert!(!cursor.is_empty());

        let front = cursor.pop_front().unwrap();
        assert!(Arc::ptr_eq(front.node(), &nodes[0]));
        assert_eq!(front.get_protected().value, 10);
        assert_eq!(cursor.len(), 3);

        let back = cursor.pop_back().unwrap();
        assert!(Arc::ptr_eq(back.node(), &nodes[3]));
        assert_eq!(back.get_protected().value, 40);
        assert_eq!(cursor.len(), 2);

        let next_front = cursor.pop_front().unwrap();
        assert!(Arc::ptr_eq(next_front.node(), &nodes[1]));
        assert_eq!(next_front.get_protected().value, 20);
        assert_eq!(cursor.len(), 1);

        let next_back = cursor.pop_back().unwrap();
        assert!(Arc::ptr_eq(next_back.node(), &nodes[2]));
        assert_eq!(next_back.get_protected().value, 30);

        assert!(cursor.is_empty());
        assert_eq!(cursor.len(), 0);
        assert!(cursor.pop_front().is_none());
        assert!(cursor.pop_back().is_none());
        assert_ring(&list, &[&nodes[0], &nodes[1], &nodes[2], &nodes[3]]);
        assert_eq!(protected_drops.load(Ordering::SeqCst), 0);
        assert_eq!(node_drops.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn cursor_mut_peek_pop_and_remove_update_lengths_and_links() {
        let node_drops = Arc::new(AtomicUsize::new(0));
        let protected_drops = Arc::new(AtomicUsize::new(0));
        let mut list = IntrusiveList::default();
        let nodes = insert_nodes(&mut list, &[1, 2, 3, 4], &node_drops, &protected_drops);

        let mut cursor = CursorMut::new(&mut list);
        assert_eq!(cursor.len(), 4);

        let mut front = cursor.peek_front().unwrap();
        assert!(Arc::ptr_eq(front.node(), &nodes[0]));
        assert_eq!(front.get_protected().value, 10);
        front.get_protected_mut().as_mut().get_mut().value = 11;
        assert_eq!(cursor.len(), 4);

        let mut back = cursor.peek_back().unwrap();
        assert!(Arc::ptr_eq(back.node(), &nodes[3]));
        assert_eq!(back.get_protected().value, 40);
        back.get_protected_mut().as_mut().get_mut().value = 44;
        assert_eq!(cursor.len(), 4);

        let mut front = cursor.pop_front().unwrap();
        assert!(Arc::ptr_eq(front.node(), &nodes[0]));
        assert_eq!(front.get_protected().value, 11);
        front.get_protected_mut().as_mut().get_mut().value = 12;
        assert_eq!(cursor.len(), 3);

        let mut back = cursor.pop_back().unwrap();
        assert!(Arc::ptr_eq(back.node(), &nodes[3]));
        assert_eq!(back.get_protected().value, 44);
        back.get_protected_mut().as_mut().get_mut().value = 45;
        assert_eq!(cursor.len(), 2);

        let mut removed_front = cursor.remove_front().unwrap();
        assert!(Arc::ptr_eq(removed_front.node(), &nodes[1]));
        assert_eq!(removed_front.protected().value, 20);
        removed_front.protected_mut().as_mut().get_mut().value = 22;
        drop(removed_front);
        assert_ring(cursor.list, &[&nodes[0], &nodes[2], &nodes[3]]);
        assert_eq!(cursor.len(), 1);
        assert_eq!(protected_drops.load(Ordering::SeqCst), 1);

        let removed_back = cursor.remove_back().unwrap();
        assert!(Arc::ptr_eq(removed_back.node(), &nodes[2]));
        assert_eq!(removed_back.protected().value, 30);
        drop(removed_back);

        assert_ring(cursor.list, &[&nodes[0], &nodes[3]]);
        assert_eq!(cursor.len(), 0);
        assert!(cursor.is_empty());
        assert!(cursor.peek_front().is_none());
        assert!(cursor.peek_back().is_none());
        assert!(cursor.pop_front().is_none());
        assert!(cursor.pop_back().is_none());
        assert!(cursor.remove_front().is_none());
        assert!(cursor.remove_back().is_none());
        assert_eq!(
            unsafe { cursor.list.get(nodes[0].as_ref()) }
                .unwrap()
                .get_protected()
                .value,
            12
        );
        assert_eq!(
            unsafe { cursor.list.get(nodes[3].as_ref()) }
                .unwrap()
                .get_protected()
                .value,
            45
        );
        assert_unlinked(&nodes[1]);
        assert_unlinked(&nodes[2]);
        assert_eq!(protected_drops.load(Ordering::SeqCst), 2);
        assert_eq!(node_drops.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn cursor_mut_single_item_removal_clears_the_list() {
        let node_drops = Arc::new(AtomicUsize::new(0));
        let protected_drops = Arc::new(AtomicUsize::new(0));
        let mut list = IntrusiveList::default();
        let node = insert_nodes(&mut list, &[1], &node_drops, &protected_drops)
            .into_iter()
            .next()
            .unwrap();

        let mut cursor = CursorMut::new(&mut list);
        let removed = cursor.remove_front().unwrap();
        assert!(Arc::ptr_eq(removed.node(), &node));
        drop(removed);

        assert!(cursor.list.is_empty());
        assert_eq!(cursor.len(), 0);
        assert!(cursor.is_empty());
        assert!(cursor.remove_back().is_none());
        assert!(cursor.peek_front().is_none());
        assert!(cursor.peek_back().is_none());
        assert_unlinked(&node);
        assert_eq!(protected_drops.load(Ordering::SeqCst), 1);
        assert_eq!(node_drops.load(Ordering::SeqCst), 0);
    }
}
