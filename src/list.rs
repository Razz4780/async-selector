//! TODO safety comments on all unsafe code

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

impl<T: StoredInList> Default for ListProtected<T> {
    fn default() -> Self {
        Self(UnsafeCell::new(ListProtectedInner {
            next: std::ptr::null(),
            prev: std::ptr::null(),
            data: MaybeUninit::uninit(),
        }))
    }
}

/// SAFETY: [`ListProtected`] is only accessed from the [`IntrusiveList`].
/// The list uses stricter `impl Send` guard.
unsafe impl<T: StoredInList> Send for ListProtected<T> {}
/// SAFETY: [`ListProtected`] is only accessed from the [`IntrusiveList`].
/// The list uses stricter `impl Sync` guard.
unsafe impl<T: StoredInList> Sync for ListProtected<T> {}

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
/// The list owns the [`ListProtected`] that is stored in the node,
/// and stores an [`Arc`] clone of each node.
///
/// The list ensures that [`StoredInList::Protected`] from all stored nodes is dropped
/// when the list itself is dropped.
pub struct IntrusiveList<T: StoredInList> {
    head: *const T,
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
        let node = Arc::new(node);
        unsafe {
            node.list_protected()
                .0
                .get()
                .as_mut_unchecked()
                .data
                .write(data);
        }

        let ptr = Arc::as_ref(&node);
        unsafe {
            let protected = node.list_protected().0.get().as_mut_unchecked();
            match self.head.as_ref() {
                Some(head) => {
                    protected.next = head;
                    protected.prev = head.list_protected().0.get().as_ref_unchecked().prev;
                    head.list_protected()
                        .0
                        .get()
                        .as_ref_unchecked()
                        .prev
                        .as_ref_unchecked()
                        .list_protected()
                        .0
                        .get()
                        .as_mut_unchecked()
                        .next = ptr;
                    head.list_protected().0.get().as_mut_unchecked().prev = ptr;
                }
                None => {
                    protected.next = ptr;
                    protected.prev = ptr;
                    self.head = ptr;
                }
            }
        }

        unsafe {
            Arc::increment_strong_count(ptr);
        }

        self.len += 1;

        node
    }

    /// Returns an [`AccessMut`] guard to the node, if the node is currently stored in the list.
    ///
    /// Returns `None` if the node is stored in the list.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the node is not stored in any other list.
    pub unsafe fn access<'a>(&'a mut self, node: &'a T) -> Option<AccessMut<'a, T>> {
        let is_unlinked = unsafe {
            node.list_protected()
                .0
                .get()
                .as_ref_unchecked()
                .is_unlinked()
        };
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
            let next = node.list_protected().0.get().as_ref_unchecked().next;
            let prev = node.list_protected().0.get().as_ref_unchecked().prev;
            next.as_ref_unchecked()
                .list_protected()
                .0
                .get()
                .as_mut_unchecked()
                .prev = prev;
            prev.as_ref_unchecked()
                .list_protected()
                .0
                .get()
                .as_mut_unchecked()
                .next = next;
            node.list_protected().0.get().as_mut_unchecked().unlink();
            (Arc::from_raw(node), next)
        };

        self.len -= 1;

        if self.len == 0 {
            self.head = std::ptr::null_mut();
        } else if std::ptr::eq(self.head, Arc::as_ptr(&item)) {
            self.head = next;
        }

        Removed(ManuallyDrop::new(item))
    }

    /// Returns a reference to the node, if the node is currently stored in the list.
    ///
    /// Returns `None` if the node is stored in the list.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the node is not stored in any other list.
    pub unsafe fn get(&self, node: &T) -> Option<BorrowedNode<'_, T>> {
        let is_unlinked = unsafe {
            node.list_protected()
                .0
                .get()
                .as_ref_unchecked()
                .is_unlinked()
        };
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
        let is_unlinked = unsafe {
            node.list_protected()
                .0
                .get()
                .as_ref_unchecked()
                .is_unlinked()
        };
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
        let is_unlinked = unsafe {
            node.list_protected()
                .0
                .get()
                .as_ref_unchecked()
                .is_unlinked()
        };
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
pub struct Removed<T: StoredInList>(ManuallyDrop<Arc<T>>);

impl<T: StoredInList> Removed<T> {
    pub fn node(&self) -> &Arc<T> {
        &self.0
    }

    pub fn protected(&self) -> &T::Protected {
        unsafe {
            self.0
                .list_protected()
                .0
                .get()
                .as_ref_unchecked()
                .data
                .assume_init_ref()
        }
    }

    pub fn protected_mut(&mut self) -> Pin<&mut T::Protected> {
        unsafe {
            let as_mut = self
                .0
                .list_protected()
                .0
                .get()
                .as_mut_unchecked()
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
                .0
                .get()
                .as_mut_unchecked()
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
                .0
                .get()
                .as_mut_unchecked()
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
                    .0
                    .get()
                    .as_mut_unchecked()
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
mod test {
    use std::{
        mem::ManuallyDrop,
        ops::Not,
        sync::{Arc, Weak},
    };

    use crate::list::{IntrusiveList, ListProtected, StoredInList};

    #[test]
    fn basic_list() {
        struct MyNode {
            list_protected: ListProtected<Self>,
        }

        unsafe impl StoredInList for MyNode {
            type Protected = Arc<()>;

            fn list_protected(&self) -> &ListProtected<Self> {
                &self.list_protected
            }
        }

        let mut list = IntrusiveList::<MyNode>::default();
        assert_eq!(list.len(), 0);
        assert!(list.is_empty());

        let data = Arc::new(());
        let node = list.insert(
            MyNode {
                list_protected: Default::default(),
            },
            data.clone(),
        );
        assert_eq!(list.len(), 1);
        assert!(list.is_empty().not());

        let mut guard = unsafe { list.access(&node).unwrap() };
        let stored = guard.get().get_mut();
        assert_eq!(Arc::as_ptr(stored), Arc::as_ptr(&data));
        let data_weak = Arc::downgrade(&data);
        drop(data);
        let _ = ManuallyDrop::new(guard);

        let mut guard = unsafe { list.access(&node).unwrap() };
        let stored = guard.get().get_mut();
        assert_eq!(Arc::as_ptr(stored), Weak::as_ptr(&data_weak));
        let _ = ManuallyDrop::new(guard);

        drop(list);
        assert!(data_weak.upgrade().is_none());
    }
}
