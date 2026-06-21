//! Translation of postgres/src/include/lib/ilist.h
//!                + postgres/src/backend/lib/ilist.c
//!
//! Integrated/inline doubly- and singly-linked lists.
//!
//! These list types are useful when there are only a predetermined set of
//! lists that an object could be in.  List links are embedded directly into
//! the objects, and thus no extra memory management overhead is required.
//!
//! The doubly-linked list comes in 2 forms.  `dlist_head` defines a head of a
//! doubly-linked list of `dlist_node`s, whereas `dclist_head` defines the head
//! of a doubly-linked list of `dlist_node`s with an additional 'count' field to
//! keep track of how many items are contained within the given list.  For
//! simplicity, `dlist_head` and `dclist_head` share the same node and iterator
//! types.  Functions to manipulate a `dlist_head` always have a name starting
//! with "dlist", whereas functions to manipulate a `dclist_head` have a name
//! starting with "dclist".
//!
//! None of the functions here allocate any memory; they just manipulate
//! externally managed memory.
//!
//! Because these structures are intrusive and pointer-linked, the bodies are
//! translated literally with raw pointers and `unsafe`.  NULL becomes
//! `core::ptr::null_mut()` / `.is_null()`.
//!
//! Note on the C `AssertVariableIsOfTypeMacro` checks embedded in the original
//! `dlist_container`/`dlist_foreach`/... macros: these are compile-time type
//! assertions that have no faithful, type-generic Rust equivalent in a
//! `macro_rules!` expansion (the type to check against is itself a macro
//! argument).  Rust's type system enforces the relevant invariants at the
//! expansion site instead, so those `AssertVariableIsOfTypeMacro` clauses are
//! dropped.  See the individual macros below.

use crate::prelude::*;
use core::ffi::{c_char, c_void};

/*
 * Enable for extra debugging. This is rather expensive, so it's not enabled by
 * default even when USE_ASSERT_CHECKING.
 */
/* #define ILIST_DEBUG */
//
// In the C source this is a (commented-out) `#define`.  We expose it as a
// compile-time `const bool` so the `*_check` helpers below can branch on it and
// be optimized away when false, mirroring the C preprocessor behavior.
pub const ILIST_DEBUG: bool = false;

/*
 * Node of a doubly linked list.
 *
 * Embed this in structs that need to be part of a doubly linked list.
 */
#[repr(C)]
pub struct dlist_node {
    pub prev: *mut dlist_node,
    pub next: *mut dlist_node,
}

/*
 * Head of a doubly linked list.
 *
 * Non-empty lists are internally circularly linked.  Circular lists have the
 * advantage of not needing any branches in the most common list manipulations.
 * An empty list can also be represented as a pair of NULL pointers, making
 * initialization easier.
 */
#[repr(C)]
pub struct dlist_head {
    /*
     * head.next either points to the first element of the list; to &head if
     * it's a circular empty list; or to NULL if empty and not circular.
     *
     * head.prev either points to the last element of the list; to &head if
     * it's a circular empty list; or to NULL if empty and not circular.
     */
    pub head: dlist_node,
}

/*
 * Doubly linked list iterator type for dlist_head and dclist_head types.
 *
 * Used as state in dlist_foreach() and dlist_reverse_foreach() (and the
 * dclist variant thereof).
 *
 * To get the current element of the iteration use the 'cur' member.
 *
 * Iterations using this are *not* allowed to change the list while iterating!
 *
 * NB: We use an extra "end" field here to avoid multiple evaluations of
 * arguments in the dlist_foreach() and dclist_foreach() macros.
 */
#[repr(C)]
pub struct dlist_iter {
    pub cur: *mut dlist_node,  /* current element */
    pub end: *mut dlist_node,  /* last node we'll iterate to */
}

/*
 * Doubly linked list iterator for both dlist_head and dclist_head types.
 * This iterator type allows some modifications while iterating.
 *
 * Used as state in dlist_foreach_modify() and dclist_foreach_modify().
 *
 * To get the current element of the iteration use the 'cur' member.
 *
 * Iterations using this are only allowed to change the list at the current
 * point of iteration. It is fine to delete the current node, but it is *not*
 * fine to insert or delete adjacent nodes.
 *
 * NB: We need a separate type for mutable iterations so that we can store
 * the 'next' node of the current node in case it gets deleted or modified.
 */
#[repr(C)]
pub struct dlist_mutable_iter {
    pub cur: *mut dlist_node,   /* current element */
    pub next: *mut dlist_node,  /* next node we'll iterate to */
    pub end: *mut dlist_node,   /* last node we'll iterate to */
}

/*
 * Head of a doubly linked list with a count of the number of items
 *
 * This internally makes use of a dlist to implement the actual list.  When
 * items are added or removed from the list the count is updated to reflect
 * the current number of items in the list.
 */
#[repr(C)]
pub struct dclist_head {
    pub dlist: dlist_head,  /* the actual list header */
    pub count: uint32,      /* the number of items in the list */
}

/*
 * Node of a singly linked list.
 *
 * Embed this in structs that need to be part of a singly linked list.
 */
#[repr(C)]
pub struct slist_node {
    pub next: *mut slist_node,
}

/*
 * Head of a singly linked list.
 *
 * Singly linked lists are not circularly linked, in contrast to doubly linked
 * lists; we just set head.next to NULL if empty.  This doesn't incur any
 * additional branches in the usual manipulations.
 */
#[repr(C)]
pub struct slist_head {
    pub head: slist_node,
}

/*
 * Singly linked list iterator.
 *
 * Used as state in slist_foreach(). To get the current element of the
 * iteration use the 'cur' member.
 *
 * It's allowed to modify the list while iterating, with the exception of
 * deleting the iterator's current node; deletion of that node requires
 * care if the iteration is to be continued afterward.  (Doing so and also
 * deleting or inserting adjacent list elements might misbehave; also, if
 * the user frees the current node's storage, continuing the iteration is
 * not safe.)
 *
 * NB: this wouldn't really need to be an extra struct, we could use an
 * slist_node * directly. We prefer a separate type for consistency.
 */
#[repr(C)]
pub struct slist_iter {
    pub cur: *mut slist_node,
}

/*
 * Singly linked list iterator allowing some modifications while iterating.
 *
 * Used as state in slist_foreach_modify(). To get the current element of the
 * iteration use the 'cur' member.
 *
 * The only list modification allowed while iterating is to remove the current
 * node via slist_delete_current() (*not* slist_delete()).  Insertion or
 * deletion of nodes adjacent to the current node would misbehave.
 */
#[repr(C)]
pub struct slist_mutable_iter {
    pub cur: *mut slist_node,   /* current element */
    pub next: *mut slist_node,  /* next node we'll iterate to */
    pub prev: *mut slist_node,  /* prev node, for deletions */
}

/* Static initializers */
//
// The C `DLIST_STATIC_INIT(name)` etc. take a *name* and build the struct
// literal referencing `&name.head` (so the empty list is circular, pointing at
// itself).  In Rust this requires the address of the storage, which is only
// available at runtime; these are therefore translated as `macro_rules!` that
// expand to an assignment given a place expression `$name` (an lvalue).  Use
// e.g. `DLIST_STATIC_INIT!(my_head);` rather than as a struct initializer.
#[macro_export]
macro_rules! DLIST_STATIC_INIT {
    ($name:expr) => {{
        let __p = &mut $name as *mut $crate::lib::ilist::dlist_head;
        (*__p).head.next = &mut (*__p).head;
        (*__p).head.prev = &mut (*__p).head;
    }};
}

#[macro_export]
macro_rules! DCLIST_STATIC_INIT {
    ($name:expr) => {{
        let __p = &mut $name as *mut $crate::lib::ilist::dclist_head;
        (*__p).dlist.head.next = &mut (*__p).dlist.head;
        (*__p).dlist.head.prev = &mut (*__p).dlist.head;
        (*__p).count = 0;
    }};
}

#[macro_export]
macro_rules! SLIST_STATIC_INIT {
    ($name:expr) => {{
        let __p = &mut $name as *mut $crate::lib::ilist::slist_head;
        (*__p).head.next = core::ptr::null_mut();
    }};
}

/* ---------------------------------------------------------------------------
 * Prototypes for functions too big to be inline (and the ILIST_DEBUG checks).
 *
 * In the C header, `dlist_member_check`, `dlist_check`, and `slist_check` are
 * either `extern` (ILIST_DEBUG) or `#define`d to `((void)(head))` no-ops.  We
 * implement them as `#[inline] pub unsafe fn`s that early-return when
 * `ILIST_DEBUG` is false, so callers can invoke them unconditionally exactly as
 * the C code does and the compiler drops the body in non-debug builds.
 * ------------------------------------------------------------------------- */

/* doubly linked list implementation */

/*
 * Initialize a doubly linked list.
 * Previous state will be thrown away without any cleanup.
 */
#[inline]
pub unsafe fn dlist_init(head: *mut dlist_head) {
    (*head).head.next = &mut (*head).head;
    (*head).head.prev = &mut (*head).head;
}

/*
 * Initialize a doubly linked list element.
 *
 * This is only needed when dlist_node_is_detached() may be needed.
 */
#[inline]
pub unsafe fn dlist_node_init(node: *mut dlist_node) {
    (*node).next = core::ptr::null_mut();
    (*node).prev = core::ptr::null_mut();
}

/*
 * Is the list empty?
 *
 * An empty list has either its first 'next' pointer set to NULL, or to itself.
 */
#[inline]
pub unsafe fn dlist_is_empty(head: *const dlist_head) -> bool {
    dlist_check(head);

    (*head).head.next.is_null() || (*head).head.next == (&(*head).head as *const dlist_node as *mut dlist_node)
}

/*
 * Insert a node at the beginning of the list.
 */
#[inline]
pub unsafe fn dlist_push_head(head: *mut dlist_head, node: *mut dlist_node) {
    if (*head).head.next.is_null() {
        /* convert NULL header to circular */
        dlist_init(head);
    }

    (*node).next = (*head).head.next;
    (*node).prev = &mut (*head).head;
    (*(*node).next).prev = node;
    (*head).head.next = node;

    dlist_check(head);
}

/*
 * Insert a node at the end of the list.
 */
#[inline]
pub unsafe fn dlist_push_tail(head: *mut dlist_head, node: *mut dlist_node) {
    if (*head).head.next.is_null() {
        /* convert NULL header to circular */
        dlist_init(head);
    }

    (*node).next = &mut (*head).head;
    (*node).prev = (*head).head.prev;
    (*(*node).prev).next = node;
    (*head).head.prev = node;

    dlist_check(head);
}

/*
 * Insert a node after another *in the same list*
 */
#[inline]
pub unsafe fn dlist_insert_after(after: *mut dlist_node, node: *mut dlist_node) {
    (*node).prev = after;
    (*node).next = (*after).next;
    (*after).next = node;
    (*(*node).next).prev = node;
}

/*
 * Insert a node before another *in the same list*
 */
#[inline]
pub unsafe fn dlist_insert_before(before: *mut dlist_node, node: *mut dlist_node) {
    (*node).prev = (*before).prev;
    (*node).next = before;
    (*before).prev = node;
    (*(*node).prev).next = node;
}

/*
 * Delete 'node' from its list (it must be in one).
 */
#[inline]
pub unsafe fn dlist_delete(node: *mut dlist_node) {
    (*(*node).prev).next = (*node).next;
    (*(*node).next).prev = (*node).prev;
}

/*
 * Like dlist_delete(), but also sets next/prev to NULL to signal not being in
 * a list.
 */
#[inline]
pub unsafe fn dlist_delete_thoroughly(node: *mut dlist_node) {
    (*(*node).prev).next = (*node).next;
    (*(*node).next).prev = (*node).prev;
    (*node).next = core::ptr::null_mut();
    (*node).prev = core::ptr::null_mut();
}

/*
 * Same as dlist_delete, but performs checks in ILIST_DEBUG builds to ensure
 * that 'node' belongs to 'head'.
 */
#[inline]
pub unsafe fn dlist_delete_from(head: *mut dlist_head, node: *mut dlist_node) {
    dlist_member_check(head, node);
    dlist_delete(node);
}

/*
 * Like dlist_delete_from, but also sets next/prev to NULL to signal not
 * being in a list.
 */
#[inline]
pub unsafe fn dlist_delete_from_thoroughly(head: *mut dlist_head, node: *mut dlist_node) {
    dlist_member_check(head, node);
    dlist_delete_thoroughly(node);
}

/*
 * Remove and return the first node from a list (there must be one).
 */
#[inline]
pub unsafe fn dlist_pop_head_node(head: *mut dlist_head) -> *mut dlist_node {
    let node: *mut dlist_node;

    Assert!(!dlist_is_empty(head));
    node = (*head).head.next;
    dlist_delete(node);
    node
}

/*
 * Move element from its current position in the list to the head position in
 * the same list.
 *
 * Undefined behaviour if 'node' is not already part of the list.
 */
#[inline]
pub unsafe fn dlist_move_head(head: *mut dlist_head, node: *mut dlist_node) {
    /* fast path if it's already at the head */
    if (*head).head.next == node {
        return;
    }

    dlist_delete(node);
    dlist_push_head(head, node);

    dlist_check(head);
}

/*
 * Move element from its current position in the list to the tail position in
 * the same list.
 *
 * Undefined behaviour if 'node' is not already part of the list.
 */
#[inline]
pub unsafe fn dlist_move_tail(head: *mut dlist_head, node: *mut dlist_node) {
    /* fast path if it's already at the tail */
    if (*head).head.prev == node {
        return;
    }

    dlist_delete(node);
    dlist_push_tail(head, node);

    dlist_check(head);
}

/*
 * Check whether 'node' has a following node.
 * Caution: unreliable if 'node' is not in the list.
 */
#[inline]
pub unsafe fn dlist_has_next(head: *const dlist_head, node: *const dlist_node) -> bool {
    (*node).next != (&(*head).head as *const dlist_node as *mut dlist_node)
}

/*
 * Check whether 'node' has a preceding node.
 * Caution: unreliable if 'node' is not in the list.
 */
#[inline]
pub unsafe fn dlist_has_prev(head: *const dlist_head, node: *const dlist_node) -> bool {
    (*node).prev != (&(*head).head as *const dlist_node as *mut dlist_node)
}

/*
 * Check if node is detached. A node is only detached if it either has been
 * initialized with dlist_init_node(), or deleted with
 * dlist_delete_thoroughly() / dlist_delete_from_thoroughly() /
 * dclist_delete_from_thoroughly().
 */
#[inline]
pub unsafe fn dlist_node_is_detached(node: *const dlist_node) -> bool {
    Assert!(
        ((*node).next.is_null() && (*node).prev.is_null())
            || (!(*node).next.is_null() && !(*node).prev.is_null())
    );

    (*node).next.is_null()
}

/*
 * Return the next node in the list (there must be one).
 */
#[inline]
pub unsafe fn dlist_next_node(head: *mut dlist_head, node: *mut dlist_node) -> *mut dlist_node {
    Assert!(dlist_has_next(head, node));
    (*node).next
}

/*
 * Return previous node in the list (there must be one).
 */
#[inline]
pub unsafe fn dlist_prev_node(head: *mut dlist_head, node: *mut dlist_node) -> *mut dlist_node {
    Assert!(dlist_has_prev(head, node));
    (*node).prev
}

/* internal support function to get address of head element's struct */
#[inline]
pub unsafe fn dlist_head_element_off(head: *mut dlist_head, off: usize) -> *mut c_void {
    Assert!(!dlist_is_empty(head));
    ((*head).head.next as *mut c_char).sub(off) as *mut c_void
}

/*
 * Return the first node in the list (there must be one).
 */
#[inline]
pub unsafe fn dlist_head_node(head: *mut dlist_head) -> *mut dlist_node {
    dlist_head_element_off(head, 0) as *mut dlist_node
}

/* internal support function to get address of tail element's struct */
#[inline]
pub unsafe fn dlist_tail_element_off(head: *mut dlist_head, off: usize) -> *mut c_void {
    Assert!(!dlist_is_empty(head));
    ((*head).head.prev as *mut c_char).sub(off) as *mut c_void
}

/*
 * Return the last node in the list (there must be one).
 */
#[inline]
pub unsafe fn dlist_tail_node(head: *mut dlist_head) -> *mut dlist_node {
    dlist_tail_element_off(head, 0) as *mut dlist_node
}

/*
 * Return the containing struct of 'type' where 'membername' is the dlist_node
 * pointed at by 'ptr'.
 *
 * This is used to convert a dlist_node * back to its containing struct.
 *
 * The C macro embeds `AssertVariableIsOfTypeMacro` compile-time type checks;
 * see the module-level note - those are dropped here, as the cast itself is the
 * faithful translation of the pointer arithmetic.
 */
#[macro_export]
macro_rules! dlist_container {
    ($type:ty, $membername:ident, $ptr:expr) => {
        (($ptr as *mut core::ffi::c_char)
            .sub(core::mem::offset_of!($type, $membername))) as *mut $type
    };
}

/*
 * Return the address of the first element in the list.
 *
 * The list must not be empty.
 */
#[macro_export]
macro_rules! dlist_head_element {
    ($type:ty, $membername:ident, $lhead:expr) => {
        $crate::lib::ilist::dlist_head_element_off($lhead, core::mem::offset_of!($type, $membername))
            as *mut $type
    };
}

/*
 * Return the address of the last element in the list.
 *
 * The list must not be empty.
 */
#[macro_export]
macro_rules! dlist_tail_element {
    ($type:ty, $membername:ident, $lhead:expr) => {
        $crate::lib::ilist::dlist_tail_element_off($lhead, core::mem::offset_of!($type, $membername))
            as *mut $type
    };
}

/*
 * Iterate through the list pointed at by 'lhead' storing the state in 'iter'.
 *
 * Access the current element with iter.cur.
 *
 * It is *not* allowed to manipulate the list during iteration.
 *
 * The C `for(;;)`-with-comma-operator construct is rendered as a Rust block
 * that runs `$body` for each element.  `$iter` must be a place (lvalue) of type
 * `dlist_iter`; `$lhead` a `*mut dlist_head`.
 */
#[macro_export]
macro_rules! dlist_foreach {
    ($iter:expr, $lhead:expr, $body:block) => {{
        $iter.end = &mut (*$lhead).head;
        let mut __dl_cur = if !$iter.end.is_null() && !(*$iter.end).next.is_null() {
            (*$iter.end).next
        } else {
            $iter.end
        };
        while __dl_cur != $iter.end {
            $iter.cur = __dl_cur;
            __dl_cur = (*__dl_cur).next; // advance before body so `continue` is safe
            $body
        }
    }};
}

/*
 * Iterate through the list pointed at by 'lhead' storing the state in 'iter'.
 *
 * Access the current element with iter.cur.
 *
 * Iterations using this are only allowed to change the list at the current
 * point of iteration. It is fine to delete the current node, but it is *not*
 * fine to insert or delete adjacent nodes.
 */
#[macro_export]
macro_rules! dlist_foreach_modify {
    ($iter:expr, $lhead:expr, $body:block) => {{
        $iter.end = &mut (*$lhead).head;
        let mut __dl_cur = if !$iter.end.is_null() && !(*$iter.end).next.is_null() {
            (*$iter.end).next
        } else {
            $iter.end
        };
        while __dl_cur != $iter.end {
            $iter.cur = __dl_cur;
            $iter.next = (*__dl_cur).next;
            __dl_cur = $iter.next; // advance before body so `continue` is safe
            $body
        }
    }};
}

/*
 * Iterate through the list in reverse order.
 *
 * It is *not* allowed to manipulate the list during iteration.
 */
#[macro_export]
macro_rules! dlist_reverse_foreach {
    ($iter:expr, $lhead:expr, $body:block) => {{
        $iter.end = &mut (*$lhead).head;
        let mut __dl_cur = if !$iter.end.is_null() && !(*$iter.end).prev.is_null() {
            (*$iter.end).prev
        } else {
            $iter.end
        };
        while __dl_cur != $iter.end {
            $iter.cur = __dl_cur;
            __dl_cur = (*__dl_cur).prev; // advance before body so `continue` is safe
            $body
        }
    }};
}

/* doubly-linked count list implementation */

/*
 * dclist_init
 *		Initialize a doubly linked count list.
 *
 * Previous state will be thrown away without any cleanup.
 */
#[inline]
pub unsafe fn dclist_init(head: *mut dclist_head) {
    dlist_init(&mut (*head).dlist);
    (*head).count = 0;
}

/*
 * dclist_is_empty
 *		Returns true if the list is empty, otherwise false.
 */
#[inline]
pub unsafe fn dclist_is_empty(head: *const dclist_head) -> bool {
    Assert!(dlist_is_empty(&(*head).dlist) == ((*head).count == 0));
    (*head).count == 0
}

/*
 * dclist_push_head
 *		Insert a node at the beginning of the list.
 */
#[inline]
pub unsafe fn dclist_push_head(head: *mut dclist_head, node: *mut dlist_node) {
    if (*head).dlist.head.next.is_null() {
        /* convert NULL header to circular */
        dclist_init(head);
    }

    dlist_push_head(&mut (*head).dlist, node);
    (*head).count += 1;

    Assert!((*head).count > 0); /* count overflow check */
}

/*
 * dclist_push_tail
 *		Insert a node at the end of the list.
 */
#[inline]
pub unsafe fn dclist_push_tail(head: *mut dclist_head, node: *mut dlist_node) {
    if (*head).dlist.head.next.is_null() {
        /* convert NULL header to circular */
        dclist_init(head);
    }

    dlist_push_tail(&mut (*head).dlist, node);
    (*head).count += 1;

    Assert!((*head).count > 0); /* count overflow check */
}

/*
 * dclist_insert_after
 *		Insert a node after another *in the same list*
 *
 * Caution: 'after' must be a member of 'head'.
 */
#[inline]
pub unsafe fn dclist_insert_after(head: *mut dclist_head, after: *mut dlist_node, node: *mut dlist_node) {
    dlist_member_check(&mut (*head).dlist, after);
    Assert!((*head).count > 0); /* must be at least 1 already */

    dlist_insert_after(after, node);
    (*head).count += 1;

    Assert!((*head).count > 0); /* count overflow check */
}

/*
 * dclist_insert_before
 *		Insert a node before another *in the same list*
 *
 * Caution: 'before' must be a member of 'head'.
 */
#[inline]
pub unsafe fn dclist_insert_before(head: *mut dclist_head, before: *mut dlist_node, node: *mut dlist_node) {
    dlist_member_check(&mut (*head).dlist, before);
    Assert!((*head).count > 0); /* must be at least 1 already */

    dlist_insert_before(before, node);
    (*head).count += 1;

    Assert!((*head).count > 0); /* count overflow check */
}

/*
 * dclist_delete_from
 *		Deletes 'node' from 'head'.
 *
 * Caution: 'node' must be a member of 'head'.
 */
#[inline]
pub unsafe fn dclist_delete_from(head: *mut dclist_head, node: *mut dlist_node) {
    Assert!((*head).count > 0);

    dlist_delete_from(&mut (*head).dlist, node);
    (*head).count -= 1;
}

/*
 * Like dclist_delete_from(), but also sets next/prev to NULL to signal not
 * being in a list.
 */
#[inline]
pub unsafe fn dclist_delete_from_thoroughly(head: *mut dclist_head, node: *mut dlist_node) {
    Assert!((*head).count > 0);

    dlist_delete_from_thoroughly(&mut (*head).dlist, node);
    (*head).count -= 1;
}

/*
 * dclist_pop_head_node
 *		Remove and return the first node from a list (there must be one).
 */
#[inline]
pub unsafe fn dclist_pop_head_node(head: *mut dclist_head) -> *mut dlist_node {
    let node: *mut dlist_node;

    Assert!((*head).count > 0);

    node = dlist_pop_head_node(&mut (*head).dlist);
    (*head).count -= 1;
    node
}

/*
 * dclist_move_head
 *		Move 'node' from its current position in the list to the head position
 *		in 'head'.
 *
 * Caution: 'node' must be a member of 'head'.
 */
#[inline]
pub unsafe fn dclist_move_head(head: *mut dclist_head, node: *mut dlist_node) {
    dlist_member_check(&mut (*head).dlist, node);
    Assert!((*head).count > 0);

    dlist_move_head(&mut (*head).dlist, node);
}

/*
 * dclist_move_tail
 *		Move 'node' from its current position in the list to the tail position
 *		in 'head'.
 *
 * Caution: 'node' must be a member of 'head'.
 */
#[inline]
pub unsafe fn dclist_move_tail(head: *mut dclist_head, node: *mut dlist_node) {
    dlist_member_check(&mut (*head).dlist, node);
    Assert!((*head).count > 0);

    dlist_move_tail(&mut (*head).dlist, node);
}

/*
 * dclist_has_next
 *		Check whether 'node' has a following node.
 *
 * Caution: 'node' must be a member of 'head'.
 */
#[inline]
pub unsafe fn dclist_has_next(head: *const dclist_head, node: *const dlist_node) -> bool {
    dlist_member_check(&(*head).dlist, node);
    Assert!((*head).count > 0);

    dlist_has_next(&(*head).dlist, node)
}

/*
 * dclist_has_prev
 *		Check whether 'node' has a preceding node.
 *
 * Caution: 'node' must be a member of 'head'.
 */
#[inline]
pub unsafe fn dclist_has_prev(head: *const dclist_head, node: *const dlist_node) -> bool {
    dlist_member_check(&(*head).dlist, node);
    Assert!((*head).count > 0);

    dlist_has_prev(&(*head).dlist, node)
}

/*
 * dclist_next_node
 *		Return the next node in the list (there must be one).
 */
#[inline]
pub unsafe fn dclist_next_node(head: *mut dclist_head, node: *mut dlist_node) -> *mut dlist_node {
    Assert!((*head).count > 0);

    dlist_next_node(&mut (*head).dlist, node)
}

/*
 * dclist_prev_node
 *		Return the prev node in the list (there must be one).
 */
#[inline]
pub unsafe fn dclist_prev_node(head: *mut dclist_head, node: *mut dlist_node) -> *mut dlist_node {
    Assert!((*head).count > 0);

    dlist_prev_node(&mut (*head).dlist, node)
}

/* internal support function to get address of head element's struct */
#[inline]
pub unsafe fn dclist_head_element_off(head: *mut dclist_head, off: usize) -> *mut c_void {
    Assert!(!dclist_is_empty(head));

    ((*head).dlist.head.next as *mut c_char).sub(off) as *mut c_void
}

/*
 * dclist_head_node
 *		Return the first node in the list (there must be one).
 */
#[inline]
pub unsafe fn dclist_head_node(head: *mut dclist_head) -> *mut dlist_node {
    Assert!((*head).count > 0);

    dlist_head_element_off(&mut (*head).dlist, 0) as *mut dlist_node
}

/* internal support function to get address of tail element's struct */
#[inline]
pub unsafe fn dclist_tail_element_off(head: *mut dclist_head, off: usize) -> *mut c_void {
    Assert!(!dclist_is_empty(head));

    ((*head).dlist.head.prev as *mut c_char).sub(off) as *mut c_void
}

/*
 * Return the last node in the list (there must be one).
 */
#[inline]
pub unsafe fn dclist_tail_node(head: *mut dclist_head) -> *mut dlist_node {
    Assert!((*head).count > 0);

    dlist_tail_element_off(&mut (*head).dlist, 0) as *mut dlist_node
}

/*
 * dclist_count
 *		Returns the stored number of entries in 'head'
 */
#[inline]
pub unsafe fn dclist_count(head: *const dclist_head) -> uint32 {
    Assert!(dlist_is_empty(&(*head).dlist) == ((*head).count == 0));

    (*head).count
}

/*
 * Return the containing struct of 'type' where 'membername' is the dlist_node
 * pointed at by 'ptr'.
 *
 * This is used to convert a dlist_node * back to its containing struct.
 *
 * Note: This is effectively just the same as dlist_container, so reuse that.
 */
#[macro_export]
macro_rules! dclist_container {
    ($type:ty, $membername:ident, $ptr:expr) => {
        $crate::dlist_container!($type, $membername, $ptr)
    };
}

/*
 * Return the address of the first element in the list.
 *
 * The list must not be empty.
 */
#[macro_export]
macro_rules! dclist_head_element {
    ($type:ty, $membername:ident, $lhead:expr) => {
        $crate::lib::ilist::dclist_head_element_off($lhead, core::mem::offset_of!($type, $membername))
            as *mut $type
    };
}

/*
 * Return the address of the last element in the list.
 *
 * The list must not be empty.
 */
#[macro_export]
macro_rules! dclist_tail_element {
    ($type:ty, $membername:ident, $lhead:expr) => {
        $crate::lib::ilist::dclist_tail_element_off($lhead, core::mem::offset_of!($type, $membername))
            as *mut $type
    };
}

/* Iterators for dclists */
#[macro_export]
macro_rules! dclist_foreach {
    ($iter:expr, $lhead:expr, $body:block) => {
        $crate::dlist_foreach!($iter, &mut (*$lhead).dlist, $body)
    };
}

#[macro_export]
macro_rules! dclist_foreach_modify {
    ($iter:expr, $lhead:expr, $body:block) => {
        $crate::dlist_foreach_modify!($iter, &mut (*$lhead).dlist, $body)
    };
}

#[macro_export]
macro_rules! dclist_reverse_foreach {
    ($iter:expr, $lhead:expr, $body:block) => {
        $crate::dlist_reverse_foreach!($iter, &mut (*$lhead).dlist, $body)
    };
}

/* singly linked list implementation */

/*
 * Initialize a singly linked list.
 * Previous state will be thrown away without any cleanup.
 */
#[inline]
pub unsafe fn slist_init(head: *mut slist_head) {
    (*head).head.next = core::ptr::null_mut();
}

/*
 * Is the list empty?
 */
#[inline]
pub unsafe fn slist_is_empty(head: *const slist_head) -> bool {
    slist_check(head);

    (*head).head.next.is_null()
}

/*
 * Insert a node at the beginning of the list.
 */
#[inline]
pub unsafe fn slist_push_head(head: *mut slist_head, node: *mut slist_node) {
    (*node).next = (*head).head.next;
    (*head).head.next = node;

    slist_check(head);
}

/*
 * Insert a node after another *in the same list*
 */
#[inline]
pub unsafe fn slist_insert_after(after: *mut slist_node, node: *mut slist_node) {
    (*node).next = (*after).next;
    (*after).next = node;
}

/*
 * Remove and return the first node from a list (there must be one).
 */
#[inline]
pub unsafe fn slist_pop_head_node(head: *mut slist_head) -> *mut slist_node {
    let node: *mut slist_node;

    Assert!(!slist_is_empty(head));
    node = (*head).head.next;
    (*head).head.next = (*node).next;
    slist_check(head);
    node
}

/*
 * Check whether 'node' has a following node.
 */
#[inline]
pub unsafe fn slist_has_next(head: *const slist_head, node: *const slist_node) -> bool {
    slist_check(head);

    !(*node).next.is_null()
}

/*
 * Return the next node in the list (there must be one).
 */
#[inline]
pub unsafe fn slist_next_node(head: *mut slist_head, node: *mut slist_node) -> *mut slist_node {
    Assert!(slist_has_next(head, node));
    (*node).next
}

/* internal support function to get address of head element's struct */
#[inline]
pub unsafe fn slist_head_element_off(head: *mut slist_head, off: usize) -> *mut c_void {
    Assert!(!slist_is_empty(head));
    ((*head).head.next as *mut c_char).sub(off) as *mut c_void
}

/*
 * Return the first node in the list (there must be one).
 */
#[inline]
pub unsafe fn slist_head_node(head: *mut slist_head) -> *mut slist_node {
    slist_head_element_off(head, 0) as *mut slist_node
}

/*
 * Delete the list element the iterator currently points to.
 *
 * Caution: this modifies iter->cur, so don't use that again in the current
 * loop iteration.
 */
#[inline]
pub unsafe fn slist_delete_current(iter: *mut slist_mutable_iter) {
    /*
     * Update previous element's forward link.  If the iteration is at the
     * first list element, iter->prev will point to the list header's "head"
     * field, so we don't need a special case for that.
     */
    (*(*iter).prev).next = (*iter).next;

    /*
     * Reset cur to prev, so that prev will continue to point to the prior
     * valid list element after slist_foreach_modify() advances to the next.
     */
    (*iter).cur = (*iter).prev;
}

/*
 * Return the containing struct of 'type' where 'membername' is the slist_node
 * pointed at by 'ptr'.
 *
 * This is used to convert a slist_node * back to its containing struct.
 *
 * As with dlist_container, the C `AssertVariableIsOfTypeMacro` checks are
 * dropped; see the module-level note.
 */
#[macro_export]
macro_rules! slist_container {
    ($type:ty, $membername:ident, $ptr:expr) => {
        (($ptr as *mut core::ffi::c_char)
            .sub(core::mem::offset_of!($type, $membername))) as *mut $type
    };
}

/*
 * Return the address of the first element in the list.
 *
 * The list must not be empty.
 */
#[macro_export]
macro_rules! slist_head_element {
    ($type:ty, $membername:ident, $lhead:expr) => {
        $crate::lib::ilist::slist_head_element_off($lhead, core::mem::offset_of!($type, $membername))
            as *mut $type
    };
}

/*
 * Iterate through the list pointed at by 'lhead' storing the state in 'iter'.
 *
 * Access the current element with iter.cur.
 *
 * It's allowed to modify the list while iterating, with the exception of
 * deleting the iterator's current node; deletion of that node requires
 * care if the iteration is to be continued afterward.  (Doing so and also
 * deleting or inserting adjacent list elements might misbehave; also, if
 * the user frees the current node's storage, continuing the iteration is
 * not safe.)
 */
#[macro_export]
macro_rules! slist_foreach {
    ($iter:expr, $lhead:expr, $body:block) => {{
        let mut __cur = (*$lhead).head.next;
        while !__cur.is_null() {
            $iter.cur = __cur;
            __cur = (*__cur).next; // prefetch so `continue` still advances (C for-loop semantics)
            $body
        }
    }};
}

/*
 * Iterate through the list pointed at by 'lhead' storing the state in 'iter'.
 *
 * Access the current element with iter.cur.
 *
 * The only list modification allowed while iterating is to remove the current
 * node via slist_delete_current() (*not* slist_delete()).  Insertion or
 * deletion of nodes adjacent to the current node would misbehave.
 */
#[macro_export]
macro_rules! slist_foreach_modify {
    ($iter:expr, $lhead:expr, $body:block) => {{
        $iter.prev = &mut (*$lhead).head;
        $iter.cur = (*$iter.prev).next;
        $iter.next = if !$iter.cur.is_null() {
            (*$iter.cur).next
        } else {
            core::ptr::null_mut()
        };
        while !$iter.cur.is_null() {
            $body
            $iter.prev = $iter.cur;
            $iter.cur = $iter.next;
            $iter.next = if !$iter.next.is_null() {
                (*$iter.next).next
            } else {
                core::ptr::null_mut()
            };
        }
    }};
}

/* ===========================================================================
 * Functions too big to be inline (from src/backend/lib/ilist.c).
 * =========================================================================== */

/*
 * Delete 'node' from list.
 *
 * It is not allowed to delete a 'node' which is not in the list 'head'
 *
 * Caution: this is O(n); consider using slist_delete_current() instead.
 */
pub unsafe fn slist_delete(head: *mut slist_head, node: *const slist_node) {
    let mut last: *mut slist_node = &mut (*head).head;
    let mut cur: *mut slist_node;
    /* PG_USED_FOR_ASSERTS_ONLY */
    let mut found: bool = false;

    loop {
        cur = (*last).next;
        if cur.is_null() {
            break;
        }
        if cur == (node as *mut slist_node) {
            (*last).next = (*cur).next;
            if cfg!(debug_assertions) {
                found = true;
            }
            break;
        }
        last = cur;
    }
    Assert!(found);
    /* keep the compiler quiet about `found` in non-assert builds */
    let _ = found;

    slist_check(head);
}

/*
 * dlist_member_check
 *		Validate that 'node' is a member of 'head'
 *
 * In C this is compiled only under ILIST_DEBUG; otherwise the header `#define`s
 * it to `((void)(head))`.  Here the function always exists but early-returns
 * (a no-op, the cast-to-void analog) unless ILIST_DEBUG is enabled.
 */
#[inline]
pub unsafe fn dlist_member_check(head: *const dlist_head, node: *const dlist_node) {
    if !ILIST_DEBUG {
        /* these seemingly useless casts keep the args "used" in non-debug */
        let _ = head;
        let _ = node;
        return;
    }

    let mut cur: *const dlist_node;

    /* iteration open-coded to due to the use of const */
    cur = (*head).head.next;
    while cur != (&(*head).head as *const dlist_node) {
        if cur == node {
            return;
        }
        cur = (*cur).next;
    }
    elog!(ERROR, "double linked list member check failure");
}

/*
 * Verify integrity of a doubly linked list
 *
 * Compiled only under ILIST_DEBUG in C (otherwise `#define`d to a no-op).
 */
#[inline]
pub unsafe fn dlist_check(head: *const dlist_head) {
    if !ILIST_DEBUG {
        let _ = head;
        return;
    }

    let mut cur: *mut dlist_node;

    if head.is_null() {
        elog!(ERROR, "doubly linked list head address is NULL");
    }

    if (*head).head.next.is_null() && (*head).head.prev.is_null() {
        return; /* OK, initialized as zeroes */
    }

    let head_node = &(*head).head as *const dlist_node as *mut dlist_node;

    /* iterate in forward direction */
    cur = (*head).head.next;
    while cur != head_node {
        if cur.is_null()
            || (*cur).next.is_null()
            || (*cur).prev.is_null()
            || (*(*cur).prev).next != cur
            || (*(*cur).next).prev != cur
        {
            elog!(ERROR, "doubly linked list is corrupted");
        }
        cur = (*cur).next;
    }

    /* iterate in backward direction */
    cur = (*head).head.prev;
    while cur != head_node {
        if cur.is_null()
            || (*cur).next.is_null()
            || (*cur).prev.is_null()
            || (*(*cur).prev).next != cur
            || (*(*cur).next).prev != cur
        {
            elog!(ERROR, "doubly linked list is corrupted");
        }
        cur = (*cur).prev;
    }
}

/*
 * Verify integrity of a singly linked list
 *
 * Compiled only under ILIST_DEBUG in C (otherwise `#define`d to a no-op).
 */
#[inline]
pub unsafe fn slist_check(head: *const slist_head) {
    if !ILIST_DEBUG {
        let _ = head;
        return;
    }

    let mut cur: *mut slist_node;

    if head.is_null() {
        elog!(ERROR, "singly linked list head address is NULL");
    }

    /*
     * there isn't much we can test in a singly linked list except that it
     * actually ends sometime, i.e. hasn't introduced a cycle or similar
     */
    cur = (*head).head.next;
    while !cur.is_null() {
        cur = (*cur).next;
    }
}

/*
 * dclist_member_check
 *		Validate that 'node' is a member of 'head'.
 *
 * The header does not declare a dedicated dclist_member_check; dclist functions
 * call dlist_member_check on the embedded dlist.  Provided here for symmetry
 * with the module guidance and to mirror the dclist call sites; it simply
 * forwards to dlist_member_check on the inner dlist.
 */
#[inline]
pub unsafe fn dclist_member_check(head: *const dclist_head, node: *const dlist_node) {
    dlist_member_check(&(*head).dlist, node);
}

#[cfg(test)]
mod tests {
    use super::*;

    // A container struct embedding a dlist_node, to exercise the container
    // arithmetic and basic push/pop operations.
    #[repr(C)]
    struct Item {
        value: i32,
        node: dlist_node,
    }

    #[test]
    fn dlist_push_pop_container() {
        unsafe {
            let mut head: dlist_head = core::mem::zeroed();
            dlist_init(&mut head);
            Assert!(dlist_is_empty(&head));

            let mut a = Item { value: 1, node: core::mem::zeroed() };
            let mut b = Item { value: 2, node: core::mem::zeroed() };

            dlist_push_head(&mut head, &mut a.node);
            dlist_push_tail(&mut head, &mut b.node);
            assert!(!dlist_is_empty(&head));

            let first = dlist_pop_head_node(&mut head);
            let item = dlist_container!(Item, node, first);
            assert_eq!((*item).value, 1);

            let second = dlist_pop_head_node(&mut head);
            let item2 = dlist_container!(Item, node, second);
            assert_eq!((*item2).value, 2);
            assert!(dlist_is_empty(&head));
        }
    }

    #[test]
    fn dclist_count_tracks_items() {
        unsafe {
            let mut head: dclist_head = core::mem::zeroed();
            dclist_init(&mut head);
            assert_eq!(dclist_count(&head), 0);

            let mut a = Item { value: 10, node: core::mem::zeroed() };
            let mut b = Item { value: 20, node: core::mem::zeroed() };
            dclist_push_head(&mut head, &mut a.node);
            dclist_push_tail(&mut head, &mut b.node);
            assert_eq!(dclist_count(&head), 2);

            let _ = dclist_pop_head_node(&mut head);
            assert_eq!(dclist_count(&head), 1);
        }
    }

    #[repr(C)]
    struct SItem {
        value: i32,
        node: slist_node,
    }

    #[test]
    fn slist_push_pop_and_delete() {
        unsafe {
            let mut head: slist_head = core::mem::zeroed();
            slist_init(&mut head);
            assert!(slist_is_empty(&head));

            let mut a = SItem { value: 1, node: core::mem::zeroed() };
            let mut b = SItem { value: 2, node: core::mem::zeroed() };
            slist_push_head(&mut head, &mut b.node);
            slist_push_head(&mut head, &mut a.node);
            assert!(!slist_is_empty(&head));

            // delete b via the O(n) slist_delete
            slist_delete(&mut head, &b.node);

            let popped = slist_pop_head_node(&mut head);
            let item = slist_container!(SItem, node, popped);
            assert_eq!((*item).value, 1);
            assert!(slist_is_empty(&head));
        }
    }
}
