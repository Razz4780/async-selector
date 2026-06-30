use std::{
    fmt,
    mem::ManuallyDrop,
    sync::{Arc, Weak},
};

use crate::{
    mpsc::{Queue, WeakSender},
    task::Task,
};

/// A unique id of a task stored in a [`Selector`](crate::selector::Selector).
///
/// Mind that this keeping this id alive prevents the selector
/// from deallocating memory used to store the task.
pub struct Id {
    /// Type erased pointer to the selector's queue.
    sender_ptr: *const (),
    /// Type erased pointer to the task.
    task_ptr: *const (),
    /// Virtual methods table that allows us to clone and drop this id,
    /// even though the types were erased.
    vtable: &'static IdVTable,
}

impl Id {
    pub(super) fn new<P>(node: &Arc<Task<P>>) -> Self {
        let sender_ptr = node.ready_tx().clone().into_raw().cast();
        let task_ptr = Arc::downgrade(node).into_raw().cast();
        Self {
            sender_ptr,
            task_ptr,
            vtable: &Task::<P>::ID_VTABLE,
        }
    }

    pub(super) fn sender_ptr(&self) -> *const () {
        self.sender_ptr
    }

    /// Recovers the task, if it's still alive.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the type of the task matches.
    pub(super) unsafe fn task<P>(&self) -> Option<Arc<Task<P>>> {
        let weak = unsafe { Weak::from_raw(self.task_ptr.cast::<Task<P>>()) };
        let strong = weak.upgrade();
        let _ = ManuallyDrop::new(weak);
        strong
    }
}

impl Clone for Id {
    fn clone(&self) -> Self {
        unsafe { (self.vtable.clone_raw)(self.sender_ptr, self.task_ptr) }
    }
}

impl Drop for Id {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop_raw)(self.sender_ptr, self.task_ptr) }
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Id")
            .field("parent", &self.sender_ptr)
            .field("task", &self.task_ptr)
            .finish_non_exhaustive()
    }
}

unsafe impl Send for Id {}
unsafe impl Sync for Id {}

impl<P> Task<P> {
    const ID_VTABLE: IdVTable = IdVTable {
        clone_raw: Self::clone_raw,
        drop_raw: Self::drop_raw,
    };

    unsafe fn clone_raw(sender_ptr: *const (), task_ptr: *const ()) -> Id {
        let sender = unsafe { WeakSender::from_raw(sender_ptr.cast::<Queue<Self>>()) };
        let cloned = sender.clone();
        let _ = ManuallyDrop::new(sender);
        let _ = ManuallyDrop::new(cloned);

        let weak = unsafe { Weak::from_raw(task_ptr.cast::<Self>()) };
        let cloned = weak.clone();
        let _ = ManuallyDrop::new(weak);
        let _ = ManuallyDrop::new(cloned);

        Id {
            sender_ptr,
            task_ptr,
            vtable: &Self::ID_VTABLE,
        }
    }

    unsafe fn drop_raw(sender_ptr: *const (), task_ptr: *const ()) {
        let sender = unsafe { WeakSender::from_raw(sender_ptr.cast::<Queue<Self>>()) };
        drop(sender);
        let task = unsafe { Weak::from_raw(task_ptr.cast::<Self>()) };
        drop(task);
    }
}

/// Allows for cloning and dropping type-erased [`Id`]s.
struct IdVTable {
    clone_raw: unsafe fn(*const (), *const ()) -> Id,
    drop_raw: unsafe fn(*const (), *const ()),
}
