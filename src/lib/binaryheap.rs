//! Translation of postgres/src/include/lib/binaryheap.h
//!                + postgres/src/common/binaryheap.c
//!
//! A simple binary heap implementation.
//!
//! Portions Copyright (c) 2012-2025, PostgreSQL Global Development Group
//!
//! The .c file includes both postgres_fe.h and postgres.h then branches on
//! FRONTEND. We translate the BACKEND path here (palloc/pfree, elog!/Assert!).
//! TODO(pg-port): the FRONTEND path (void *-based bh_node_type, malloc/free,
//! pg_fatal) is not translated; see the bh_node_type alias and "out of binary
//! heap slots" error sites below.

use crate::prelude::*;
use core::ffi::{c_int, c_void};

/*
 * We provide a Datum-based API for backend code and a void *-based API for
 * frontend code (since the Datum definitions are not available to frontend
 * code).  You should typically avoid using bh_node_type directly and instead
 * use Datum or void * as appropriate.
 */
// TODO(pg-port): FRONTEND defines `typedef void *bh_node_type;`. We only
// translate the BACKEND path, where bh_node_type is Datum.
pub type bh_node_type = Datum;

/*
 * For a max-heap, the comparator must return <0 iff a < b, 0 iff a == b,
 * and >0 iff a > b.  For a min-heap, the conditions are reversed.
 */
pub type binaryheap_comparator =
    unsafe fn(a: bh_node_type, b: bh_node_type, arg: *mut c_void) -> c_int;

/*
 * binaryheap
 *
 *		bh_size			how many nodes are currently in "nodes"
 *		bh_space		how many nodes can be stored in "nodes"
 *		bh_has_heap_property	no unordered operations since last heap build
 *		bh_compare		comparison function to define the heap property
 *		bh_arg			user data for comparison function
 *		bh_nodes		variable-length array of "space" nodes
 */
#[repr(C)]
pub struct binaryheap {
    pub bh_size: c_int,
    pub bh_space: c_int,
    pub bh_has_heap_property: bool, /* debugging cross-check */
    pub bh_compare: binaryheap_comparator,
    pub bh_arg: *mut c_void,
    pub bh_nodes: [bh_node_type; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/*
 * binaryheap_empty / binaryheap_size / binaryheap_get_node
 *
 * These were #define macros in the header; translate them to inline fns.
 */
#[inline]
pub unsafe fn binaryheap_empty(h: *const binaryheap) -> bool {
    (*h).bh_size == 0
}

#[inline]
pub unsafe fn binaryheap_size(h: *const binaryheap) -> c_int {
    (*h).bh_size
}

#[inline]
pub unsafe fn binaryheap_get_node(h: *const binaryheap, n: c_int) -> bh_node_type {
    *(*h).bh_nodes.as_ptr().add(n as usize)
}

/*
 * binaryheap_allocate
 *
 * Returns a pointer to a newly-allocated heap that has the capacity to
 * store the given number of nodes, with the heap property defined by
 * the given comparator function, which will be invoked with the additional
 * argument specified by 'arg'.
 */
pub unsafe fn binaryheap_allocate(
    capacity: c_int,
    compare: binaryheap_comparator,
    arg: *mut c_void,
) -> *mut binaryheap {
    let sz: c_int;
    let heap: *mut binaryheap;

    sz = (core::mem::offset_of!(binaryheap, bh_nodes)
        + core::mem::size_of::<bh_node_type>() * capacity as usize) as c_int;
    heap = palloc(sz as Size) as *mut binaryheap;
    (*heap).bh_space = capacity;
    (*heap).bh_compare = compare;
    (*heap).bh_arg = arg;

    (*heap).bh_size = 0;
    (*heap).bh_has_heap_property = true;

    heap
}

/*
 * binaryheap_reset
 *
 * Resets the heap to an empty state, losing its data content but not the
 * parameters passed at allocation.
 */
pub unsafe fn binaryheap_reset(heap: *mut binaryheap) {
    (*heap).bh_size = 0;
    (*heap).bh_has_heap_property = true;
}

/*
 * binaryheap_free
 *
 * Releases memory used by the given binaryheap.
 */
pub unsafe fn binaryheap_free(heap: *mut binaryheap) {
    pfree(heap as *mut c_void);
}

/*
 * These utility functions return the offset of the left child, right
 * child, and parent of the node at the given index, respectively.
 *
 * The heap is represented as an array of nodes, with the root node
 * stored at index 0. The left child of node i is at index 2*i+1, and
 * the right child at 2*i+2. The parent of node i is at index (i-1)/2.
 */

#[inline]
fn left_offset(i: c_int) -> c_int {
    2 * i + 1
}

#[inline]
fn right_offset(i: c_int) -> c_int {
    2 * i + 2
}

#[inline]
fn parent_offset(i: c_int) -> c_int {
    (i - 1) / 2
}

/*
 * binaryheap_add_unordered
 *
 * Adds the given datum to the end of the heap's list of nodes in O(1) without
 * preserving the heap property. This is a convenience to add elements quickly
 * to a new heap. To obtain a valid heap, one must call binaryheap_build()
 * afterwards.
 */
pub unsafe fn binaryheap_add_unordered(heap: *mut binaryheap, d: bh_node_type) {
    if (*heap).bh_size >= (*heap).bh_space {
        // TODO(pg-port): FRONTEND uses pg_fatal("out of binary heap slots");
        elog!(ERROR, "out of binary heap slots");
    }
    (*heap).bh_has_heap_property = false;
    *(*heap).bh_nodes.as_mut_ptr().add((*heap).bh_size as usize) = d;
    (*heap).bh_size += 1;
}

/*
 * binaryheap_build
 *
 * Assembles a valid heap in O(n) from the nodes added by
 * binaryheap_add_unordered(). Not needed otherwise.
 */
pub unsafe fn binaryheap_build(heap: *mut binaryheap) {
    let mut i: c_int;

    i = parent_offset((*heap).bh_size - 1);
    while i >= 0 {
        sift_down(heap, i);
        i -= 1;
    }
    (*heap).bh_has_heap_property = true;
}

/*
 * binaryheap_add
 *
 * Adds the given datum to the heap in O(log n) time, while preserving
 * the heap property.
 */
pub unsafe fn binaryheap_add(heap: *mut binaryheap, d: bh_node_type) {
    if (*heap).bh_size >= (*heap).bh_space {
        // TODO(pg-port): FRONTEND uses pg_fatal("out of binary heap slots");
        elog!(ERROR, "out of binary heap slots");
    }
    *(*heap).bh_nodes.as_mut_ptr().add((*heap).bh_size as usize) = d;
    (*heap).bh_size += 1;
    sift_up(heap, (*heap).bh_size - 1);
}

/*
 * binaryheap_first
 *
 * Returns a pointer to the first (root, topmost) node in the heap
 * without modifying the heap. The caller must ensure that this
 * routine is not used on an empty heap. Always O(1).
 */
pub unsafe fn binaryheap_first(heap: *mut binaryheap) -> bh_node_type {
    Assert!(!binaryheap_empty(heap) && (*heap).bh_has_heap_property);
    *(*heap).bh_nodes.as_ptr()
}

/*
 * binaryheap_remove_first
 *
 * Removes the first (root, topmost) node in the heap and returns a
 * pointer to it after rebalancing the heap. The caller must ensure
 * that this routine is not used on an empty heap. O(log n) worst
 * case.
 */
pub unsafe fn binaryheap_remove_first(heap: *mut binaryheap) -> bh_node_type {
    let result: bh_node_type;

    Assert!(!binaryheap_empty(heap) && (*heap).bh_has_heap_property);

    /* extract the root node, which will be the result */
    result = *(*heap).bh_nodes.as_ptr();

    /* easy if heap contains one element */
    if (*heap).bh_size == 1 {
        (*heap).bh_size -= 1;
        return result;
    }

    /*
     * Remove the last node, placing it in the vacated root entry, and sift
     * the new root node down to its correct position.
     */
    (*heap).bh_size -= 1;
    let last = (*heap).bh_size;
    *(*heap).bh_nodes.as_mut_ptr() = *(*heap).bh_nodes.as_ptr().add(last as usize);
    sift_down(heap, 0);

    result
}

/*
 * binaryheap_remove_node
 *
 * Removes the nth (zero based) node from the heap.  The caller must ensure
 * that there are at least (n + 1) nodes in the heap.  O(log n) worst case.
 */
pub unsafe fn binaryheap_remove_node(heap: *mut binaryheap, n: c_int) {
    let cmp: c_int;

    Assert!(!binaryheap_empty(heap) && (*heap).bh_has_heap_property);
    Assert!(n >= 0 && n < (*heap).bh_size);

    /* compare last node to the one that is being removed */
    (*heap).bh_size -= 1;
    let last = (*heap).bh_size;
    cmp = ((*heap).bh_compare)(
        *(*heap).bh_nodes.as_ptr().add(last as usize),
        *(*heap).bh_nodes.as_ptr().add(n as usize),
        (*heap).bh_arg,
    );

    /* remove the last node, placing it in the vacated entry */
    *(*heap).bh_nodes.as_mut_ptr().add(n as usize) =
        *(*heap).bh_nodes.as_ptr().add(last as usize);

    /* sift as needed to preserve the heap property */
    if cmp > 0 {
        sift_up(heap, n);
    } else if cmp < 0 {
        sift_down(heap, n);
    }
}

/*
 * binaryheap_replace_first
 *
 * Replace the topmost element of a non-empty heap, preserving the heap
 * property.  O(1) in the best case, or O(log n) if it must fall back to
 * sifting the new node down.
 */
pub unsafe fn binaryheap_replace_first(heap: *mut binaryheap, d: bh_node_type) {
    Assert!(!binaryheap_empty(heap) && (*heap).bh_has_heap_property);

    *(*heap).bh_nodes.as_mut_ptr() = d;

    if (*heap).bh_size > 1 {
        sift_down(heap, 0);
    }
}

/*
 * Sift a node up to the highest position it can hold according to the
 * comparator.
 */
unsafe fn sift_up(heap: *mut binaryheap, mut node_off: c_int) {
    let node_val: bh_node_type = *(*heap).bh_nodes.as_ptr().add(node_off as usize);

    /*
     * Within the loop, the node_off'th array entry is a "hole" that
     * notionally holds node_val, but we don't actually store node_val there
     * till the end, saving some unnecessary data copying steps.
     */
    while node_off != 0 {
        let cmp: c_int;
        let parent_off: c_int;
        let parent_val: bh_node_type;

        /*
         * If this node is smaller than its parent, the heap condition is
         * satisfied, and we're done.
         */
        parent_off = parent_offset(node_off);
        parent_val = *(*heap).bh_nodes.as_ptr().add(parent_off as usize);
        cmp = ((*heap).bh_compare)(node_val, parent_val, (*heap).bh_arg);
        if cmp <= 0 {
            break;
        }

        /*
         * Otherwise, swap the parent value with the hole, and go on to check
         * the node's new parent.
         */
        *(*heap).bh_nodes.as_mut_ptr().add(node_off as usize) = parent_val;
        node_off = parent_off;
    }
    /* Re-fill the hole */
    *(*heap).bh_nodes.as_mut_ptr().add(node_off as usize) = node_val;
}

/*
 * Sift a node down from its current position to satisfy the heap
 * property.
 */
unsafe fn sift_down(heap: *mut binaryheap, mut node_off: c_int) {
    let node_val: bh_node_type = *(*heap).bh_nodes.as_ptr().add(node_off as usize);

    /*
     * Within the loop, the node_off'th array entry is a "hole" that
     * notionally holds node_val, but we don't actually store node_val there
     * till the end, saving some unnecessary data copying steps.
     */
    loop {
        let left_off: c_int = left_offset(node_off);
        let right_off: c_int = right_offset(node_off);
        let mut swap_off: c_int = left_off;

        /* Is the right child larger than the left child? */
        if right_off < (*heap).bh_size
            && ((*heap).bh_compare)(
                *(*heap).bh_nodes.as_ptr().add(left_off as usize),
                *(*heap).bh_nodes.as_ptr().add(right_off as usize),
                (*heap).bh_arg,
            ) < 0
        {
            swap_off = right_off;
        }

        /*
         * If no children or parent is >= the larger child, heap condition is
         * satisfied, and we're done.
         */
        if left_off >= (*heap).bh_size
            || ((*heap).bh_compare)(
                node_val,
                *(*heap).bh_nodes.as_ptr().add(swap_off as usize),
                (*heap).bh_arg,
            ) >= 0
        {
            break;
        }

        /*
         * Otherwise, swap the hole with the child that violates the heap
         * property; then go on to check its children.
         */
        *(*heap).bh_nodes.as_mut_ptr().add(node_off as usize) =
            *(*heap).bh_nodes.as_ptr().add(swap_off as usize);
        node_off = swap_off;
    }
    /* Re-fill the hole */
    *(*heap).bh_nodes.as_mut_ptr().add(node_off as usize) = node_val;
}
