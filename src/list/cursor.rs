//! TODO safety comments on all unsafe code

use std::{marker::PhantomData, mem::ManuallyDrop, pin::Pin, sync::Arc};

use crate::list::{IntrusiveList, Removed, StoredInList};

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
                .map(|head| head.list_protected().0.get().as_ref_unchecked().prev)
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

    pub fn pop_front(&mut self) -> Option<BorrowedNode<'a, T>> {
        if self.is_empty() {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.front) });
        self.front = unsafe { node.list_protected().0.get().as_ref_unchecked().next };
        self.skipped += 1;
        Some(BorrowedNode {
            node,
            _lifetime_guard: PhantomData,
        })
    }

    pub fn pop_back(&mut self) -> Option<BorrowedNode<'a, T>> {
        if self.is_empty() {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.back) });
        self.back = unsafe { node.list_protected().0.get().as_ref_unchecked().prev };
        self.skipped += 1;
        Some(BorrowedNode {
            node,
            _lifetime_guard: PhantomData,
        })
    }
}

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
                .map(|head| head.list_protected().0.get().as_ref_unchecked().prev)
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

    pub fn pop_front(&mut self) -> Option<BorrowedNodeMut<'a, T>> {
        if self.is_empty() {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.front) });
        self.front = unsafe { node.list_protected().0.get().as_ref_unchecked().next };
        self.skipped += 1;
        Some(BorrowedNodeMut {
            node,
            _lifetime_guard: PhantomData,
        })
    }

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

    pub fn remove_front(&mut self) -> Option<Removed<T>> {
        if self.is_empty() {
            return None;
        }
        let to_remove = self.front;
        self.front = unsafe {
            self.front
                .as_ref_unchecked()
                .list_protected()
                .0
                .get()
                .as_ref_unchecked()
                .next
        };
        let removed = unsafe { self.list.remove_unchecked(to_remove.as_ref_unchecked()) };
        Some(removed)
    }

    pub fn pop_back(&mut self) -> Option<BorrowedNodeMut<'a, T>> {
        if self.is_empty() {
            return None;
        }
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(self.back) });
        self.back = unsafe { node.list_protected().0.get().as_ref_unchecked().prev };
        self.skipped += 1;
        Some(BorrowedNodeMut {
            node,
            _lifetime_guard: PhantomData,
        })
    }

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

    pub fn remove_back(&mut self) -> Option<Removed<T>> {
        if self.is_empty() {
            return None;
        }
        let to_remove = self.back;
        self.back = unsafe {
            self.back
                .as_ref_unchecked()
                .list_protected()
                .0
                .get()
                .as_ref_unchecked()
                .prev
        };
        let removed = unsafe { self.list.remove_unchecked(to_remove.as_ref_unchecked()) };
        Some(removed)
    }
}

pub struct BorrowedNode<'a, T: StoredInList> {
    pub(super) node: ManuallyDrop<Arc<T>>,
    pub(super) _lifetime_guard: PhantomData<&'a ()>,
}

impl<T: StoredInList> BorrowedNode<'_, T> {
    pub fn get_protected(&self) -> &T::Protected {
        unsafe {
            self.node
                .list_protected()
                .0
                .get()
                .as_ref_unchecked()
                .data
                .assume_init_ref()
        }
    }

    pub fn node(&self) -> &Arc<T> {
        &self.node
    }
}

pub struct BorrowedNodeMut<'a, T: StoredInList> {
    pub(super) node: ManuallyDrop<Arc<T>>,
    pub(super) _lifetime_guard: PhantomData<&'a ()>,
}

impl<T: StoredInList> BorrowedNodeMut<'_, T> {
    pub fn get_protected(&self) -> &T::Protected {
        unsafe {
            self.node
                .list_protected()
                .0
                .get()
                .as_ref_unchecked()
                .data
                .assume_init_ref()
        }
    }

    pub fn get_protected_mut(&mut self) -> Pin<&mut T::Protected> {
        unsafe {
            let raw = self
                .node
                .list_protected()
                .0
                .get()
                .as_mut_unchecked()
                .data
                .assume_init_mut();
            Pin::new_unchecked(raw)
        }
    }

    pub fn node(&self) -> &Arc<T> {
        &self.node
    }
}
