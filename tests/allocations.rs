//! Verifies that selectors do not allocate in the polling hot path.
//!
//! Uses a global allocator that counts allocations made by the current thread.

#[path = "../benches/common/mod.rs"]
mod common;

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

use async_selector::{FutureSelector, StreamSelector};
use common::{Driver, ReadyStream, YieldFuture, YieldStream};
use futures::{Stream, stream::SelectAll};

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;

impl CountingAllocator {
    fn record() {
        // Ignore the error, which occurs only during thread teardown.
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        Self::record();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        Self::record();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        Self::record();
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const TASKS: usize = 64;
const ITEMS_PER_TASK: usize = 64;

/// Drains the stream, and returns the number of allocations made while draining.
fn allocations_while_draining<S: Stream + Unpin>(mut stream: S) -> usize {
    let driver = Driver::default();
    let before = ALLOCATIONS.with(Cell::get);
    let items = driver.count_to_end(&mut stream);
    let after = ALLOCATIONS.with(Cell::get);
    assert_eq!(items, TASKS * ITEMS_PER_TASK);
    after - before
}

#[test]
fn stream_selector_ready_streams() {
    let selector = (0..TASKS)
        .map(|_| ReadyStream::new(ITEMS_PER_TASK))
        .collect::<StreamSelector<_>>();
    assert_eq!(allocations_while_draining(selector), 0);
}

#[test]
fn stream_selector_yield_streams() {
    let selector = (0..TASKS)
        .map(|_| YieldStream::new(ITEMS_PER_TASK))
        .collect::<StreamSelector<_>>();
    assert_eq!(allocations_while_draining(selector), 0);
}

#[test]
fn future_selector_yield_futures() {
    let selector = (0..TASKS)
        .map(|_| YieldFuture::new(ITEMS_PER_TASK))
        .collect::<FutureSelector<_>>();
    // Each future produces one item.
    let driver = Driver::default();
    let mut selector = selector;
    let before = ALLOCATIONS.with(Cell::get);
    assert_eq!(driver.count_to_end(&mut selector), TASKS);
    let after = ALLOCATIONS.with(Cell::get);
    assert_eq!(after - before, 0);
}

/// Documents the allocation this crate avoids: [`SelectAll`] allocates for every produced item.
#[test]
fn select_all_allocates_per_item() {
    let selector = (0..TASKS)
        .map(|_| ReadyStream::new(ITEMS_PER_TASK))
        .collect::<SelectAll<_>>();
    assert!(allocations_while_draining(selector) >= TASKS * ITEMS_PER_TASK);
}
