//! Translation of postgres/src/include/lib/pairingheap.h
//!                + postgres/src/backend/lib/pairingheap.c
//!
//! A Pairing Heap implementation.
//!
//! A pairing heap is a data structure that's useful for implementing
//! priority queues. It is simple to implement, and provides amortized O(1)
//! insert and find-min operations, and amortized O(log n) delete-min.
//!
//! The pairing heap was first described in this paper:
//!
//!  Michael L. Fredman, Robert Sedgewick, Daniel D. Sleator, and Robert E.
//!   Tarjan. 1986.
//!  The pairing heap: a new form of self-adjusting heap.
//!  Algorithmica 1, 1 (January 1986), pages 111-129. DOI: 10.1007/BF01840439
//!
//! Portions Copyright (c) 2012-2025, PostgreSQL Global Development Group

use crate::prelude::*;
use crate::lib::stringinfo::{
    appendStringInfoChar, appendStringInfoSpaces, initStringInfo, StringInfo, StringInfoData,
};
use core::ffi::{c_int, c_void};

/* Enable if you need the pairingheap_dump() debug function */
/* (this corresponds to `#define PAIRINGHEAP_DEBUG`) */

/*
 * This represents an element stored in the heap. Embed this in a larger
 * struct containing the actual data you're storing.
 *
 * A node can have multiple children, which form a double-linked list.
 * first_child points to the node's first child, and the subsequent children
 * can be found by following the next_sibling pointers. The last child has
 * next_sibling == NULL. The prev_or_parent pointer points to the node's
 * previous sibling, or if the node is its parent's first child, to the
 * parent.
 */
#[repr(C)]
pub struct pairingheap_node {
    pub first_child: *mut pairingheap_node,
    pub next_sibling: *mut pairingheap_node,
    pub prev_or_parent: *mut pairingheap_node,
}

/*
 * Return the containing struct of 'type' where 'membername' is the
 * pairingheap_node pointed at by 'ptr'.
 *
 * This is used to convert a pairingheap_node * back to its containing struct.
 *
 * The C macros pairingheap_container / pairingheap_const_container also assert
 * that the member is of type pairingheap_node; in Rust the offset_of! macro
 * already enforces that the field exists, so we provide a single macro covering
 * both the mutable and const cases (Rust does not distinguish const pointers at
 * the type level here).
 */
#[macro_export]
macro_rules! pairingheap_container {
    ($type:ty, $membername:ident, $ptr:expr) => {
        ($ptr as *mut ::core::ffi::c_char)
            .sub(::core::mem::offset_of!($type, $membername)) as *mut $type
    };
}

/*
 * Like pairingheap_container, but used when the pointer is 'const ptr'.
 */
#[macro_export]
macro_rules! pairingheap_const_container {
    ($type:ty, $membername:ident, $ptr:expr) => {
        ($ptr as *const ::core::ffi::c_char)
            .sub(::core::mem::offset_of!($type, $membername)) as *const $type
    };
}

/*
 * For a max-heap, the comparator must return <0 iff a < b, 0 iff a == b,
 * and >0 iff a > b.  For a min-heap, the conditions are reversed.
 */
pub type pairingheap_comparator =
    unsafe fn(a: *const pairingheap_node, b: *const pairingheap_node, arg: *mut c_void) -> c_int;

/*
 * A pairing heap.
 *
 * You can use pairingheap_allocate() to create a new palloc'd heap, or embed
 * this in a larger struct, set ph_compare and ph_arg directly and initialize
 * ph_root to NULL.
 */
#[repr(C)]
pub struct pairingheap {
    pub ph_compare: pairingheap_comparator, /* comparison function */
    pub ph_arg: *mut c_void,                 /* opaque argument to ph_compare */
    pub ph_root: *mut pairingheap_node,      /* current root of the heap */
}

/*
 * pairingheap_allocate
 *
 * Returns a pointer to a newly-allocated heap, with the heap property defined
 * by the given comparator function, which will be invoked with the additional
 * argument specified by 'arg'.
 */
pub unsafe fn pairingheap_allocate(
    compare: pairingheap_comparator,
    arg: *mut c_void,
) -> *mut pairingheap {
    let heap: *mut pairingheap;

    heap = palloc(core::mem::size_of::<pairingheap>()) as *mut pairingheap;
    (*heap).ph_compare = compare;
    (*heap).ph_arg = arg;

    (*heap).ph_root = core::ptr::null_mut();

    heap
}

/*
 * pairingheap_free
 *
 * Releases memory used by the given pairingheap.
 *
 * Note: The nodes in the heap are not freed!
 */
pub unsafe fn pairingheap_free(heap: *mut pairingheap) {
    pfree(heap as *mut c_void);
}

/*
 * A helper function to merge two subheaps into one.
 *
 * The subheap with smaller value is put as a child of the other one (assuming
 * a max-heap).
 *
 * The next_sibling and prev_or_parent pointers of the input nodes are
 * ignored. On return, the returned node's next_sibling and prev_or_parent
 * pointers are garbage.
 */
unsafe fn merge(
    heap: *mut pairingheap,
    mut a: *mut pairingheap_node,
    mut b: *mut pairingheap_node,
) -> *mut pairingheap_node {
    if a.is_null() {
        return b;
    }
    if b.is_null() {
        return a;
    }

    /* swap 'a' and 'b' so that 'a' is the one with larger value */
    if ((*heap).ph_compare)(a, b, (*heap).ph_arg) < 0 {
        let tmp: *mut pairingheap_node;

        tmp = a;
        a = b;
        b = tmp;
    }

    /* and put 'b' as a child of 'a' */
    if !(*a).first_child.is_null() {
        (*(*a).first_child).prev_or_parent = b;
    }
    (*b).prev_or_parent = a;
    (*b).next_sibling = (*a).first_child;
    (*a).first_child = b;

    a
}

/*
 * pairingheap_add
 *
 * Adds the given node to the heap in O(1) time.
 */
pub unsafe fn pairingheap_add(heap: *mut pairingheap, node: *mut pairingheap_node) {
    (*node).first_child = core::ptr::null_mut();

    /* Link the new node as a new tree */
    (*heap).ph_root = merge(heap, (*heap).ph_root, node);
    (*(*heap).ph_root).prev_or_parent = core::ptr::null_mut();
    (*(*heap).ph_root).next_sibling = core::ptr::null_mut();
}

/*
 * pairingheap_first
 *
 * Returns a pointer to the first (root, topmost) node in the heap without
 * modifying the heap. The caller must ensure that this routine is not used on
 * an empty heap. Always O(1).
 */
pub unsafe fn pairingheap_first(heap: *mut pairingheap) -> *mut pairingheap_node {
    Assert!(!pairingheap_is_empty(heap));

    (*heap).ph_root
}

/*
 * pairingheap_remove_first
 *
 * Removes the first (root, topmost) node in the heap and returns a pointer to
 * it after rebalancing the heap. The caller must ensure that this routine is
 * not used on an empty heap. O(log n) amortized.
 */
pub unsafe fn pairingheap_remove_first(heap: *mut pairingheap) -> *mut pairingheap_node {
    let result: *mut pairingheap_node;
    let children: *mut pairingheap_node;

    Assert!(!pairingheap_is_empty(heap));

    /* Remove the root, and form a new heap of its children. */
    result = (*heap).ph_root;
    children = (*result).first_child;

    (*heap).ph_root = merge_children(heap, children);
    if !(*heap).ph_root.is_null() {
        (*(*heap).ph_root).prev_or_parent = core::ptr::null_mut();
        (*(*heap).ph_root).next_sibling = core::ptr::null_mut();
    }

    result
}

/*
 * Remove 'node' from the heap. O(log n) amortized.
 */
pub unsafe fn pairingheap_remove(heap: *mut pairingheap, node: *mut pairingheap_node) {
    let children: *mut pairingheap_node;
    let replacement: *mut pairingheap_node;
    let next_sibling: *mut pairingheap_node;
    let prev_ptr: *mut *mut pairingheap_node;

    /*
     * If the removed node happens to be the root node, do it with
     * pairingheap_remove_first().
     */
    if node == (*heap).ph_root {
        let _ = pairingheap_remove_first(heap);
        return;
    }

    /*
     * Before we modify anything, remember the removed node's first_child and
     * next_sibling pointers.
     */
    children = (*node).first_child;
    next_sibling = (*node).next_sibling;

    /*
     * Also find the pointer to the removed node in its previous sibling, or
     * if this is the first child of its parent, in its parent.
     */
    if (*(*node).prev_or_parent).first_child == node {
        prev_ptr = &mut (*(*node).prev_or_parent).first_child;
    } else {
        prev_ptr = &mut (*(*node).prev_or_parent).next_sibling;
    }
    Assert!(*prev_ptr == node);

    /*
     * If this node has children, make a new subheap of the children and link
     * the subheap in place of the removed node. Otherwise just unlink this
     * node.
     */
    if !children.is_null() {
        replacement = merge_children(heap, children);

        (*replacement).prev_or_parent = (*node).prev_or_parent;
        (*replacement).next_sibling = (*node).next_sibling;
        *prev_ptr = replacement;
        if !next_sibling.is_null() {
            (*next_sibling).prev_or_parent = replacement;
        }
    } else {
        *prev_ptr = next_sibling;
        if !next_sibling.is_null() {
            (*next_sibling).prev_or_parent = (*node).prev_or_parent;
        }
    }
}

/*
 * Merge a list of subheaps into a single heap.
 *
 * This implements the basic two-pass merging strategy, first forming pairs
 * from left to right, and then merging the pairs.
 */
unsafe fn merge_children(
    heap: *mut pairingheap,
    children: *mut pairingheap_node,
) -> *mut pairingheap_node {
    let mut curr: *mut pairingheap_node;
    let mut next: *mut pairingheap_node;
    let mut pairs: *mut pairingheap_node;
    let mut newroot: *mut pairingheap_node;

    if children.is_null() || (*children).next_sibling.is_null() {
        return children;
    }

    /* Walk the subheaps from left to right, merging in pairs */
    next = children;
    pairs = core::ptr::null_mut();
    loop {
        curr = next;

        if curr.is_null() {
            break;
        }

        if (*curr).next_sibling.is_null() {
            /* last odd node at the end of list */
            (*curr).next_sibling = pairs;
            pairs = curr;
            break;
        }

        next = (*(*curr).next_sibling).next_sibling;

        /* merge this and the next subheap, and add to 'pairs' list. */

        curr = merge(heap, curr, (*curr).next_sibling);
        (*curr).next_sibling = pairs;
        pairs = curr;
    }

    /*
     * Merge all the pairs together to form a single heap.
     */
    newroot = pairs;
    next = (*pairs).next_sibling;
    while !next.is_null() {
        curr = next;
        next = (*curr).next_sibling;

        newroot = merge(heap, newroot, curr);
    }

    newroot
}

/* Resets the heap to be empty. */
#[inline]
pub unsafe fn pairingheap_reset(h: *mut pairingheap) {
    (*h).ph_root = core::ptr::null_mut();
}

/* Is the heap empty? */
#[inline]
pub unsafe fn pairingheap_is_empty(h: *mut pairingheap) -> bool {
    (*h).ph_root.is_null()
}

/* Is there exactly one node in the heap? */
#[inline]
pub unsafe fn pairingheap_is_singular(h: *mut pairingheap) -> bool {
    !(*h).ph_root.is_null() && (*(*h).ph_root).first_child.is_null()
}

/*
 * A debug function to dump the contents of the heap as a string.
 *
 * The 'dumpfunc' callback appends a string representation of a single node
 * to the StringInfo. 'opaque' can be used to pass more information to the
 * callback.
 *
 * This corresponds to the `#ifdef PAIRINGHEAP_DEBUG` section. It is translated
 * unconditionally as dead-code-allowed; it is fine if unused.
 */
#[allow(dead_code)]
pub type pairingheap_dumpfunc =
    unsafe fn(node: *mut pairingheap_node, buf: StringInfo, opaque: *mut c_void);

#[allow(dead_code)]
unsafe fn pairingheap_dump_recurse(
    buf: StringInfo,
    mut node: *mut pairingheap_node,
    dumpfunc: pairingheap_dumpfunc,
    opaque: *mut c_void,
    depth: c_int,
    mut prev_or_parent: *mut pairingheap_node,
) {
    while !node.is_null() {
        Assert!((*node).prev_or_parent == prev_or_parent);

        appendStringInfoSpaces(buf, depth * 4);
        dumpfunc(node, buf, opaque);
        appendStringInfoChar(buf, b'\n' as core::ffi::c_char);
        if !(*node).first_child.is_null() {
            pairingheap_dump_recurse(
                buf,
                (*node).first_child,
                dumpfunc,
                opaque,
                depth + 1,
                node,
            );
        }
        prev_or_parent = node;
        node = (*node).next_sibling;
    }
}

#[allow(dead_code)]
pub unsafe fn pairingheap_dump(
    heap: *mut pairingheap,
    dumpfunc: pairingheap_dumpfunc,
    opaque: *mut c_void,
) -> *mut core::ffi::c_char {
    let mut buf: StringInfoData = core::mem::zeroed();

    if (*heap).ph_root.is_null() {
        return pstrdup(c"(empty)".as_ptr());
    }

    initStringInfo(&mut buf);

    pairingheap_dump_recurse(&mut buf, (*heap).ph_root, dumpfunc, opaque, 0, core::ptr::null_mut());

    buf.data
}
