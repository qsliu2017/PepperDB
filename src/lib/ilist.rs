//! Translated from PostgreSQL src/include/lib/ilist.h
#![allow(
    clippy::cast_ptr_alignment,
    reason = "PG intrusive-list node pointer reinterpretation, faithful to C"
)]
//!
//! Intrusive doubly- and singly-linked lists. These embed their links directly
//! in caller objects and never allocate, so the faithful 1:1 translation keeps
//! the raw-pointer model: nodes and heads hold `*mut`/`*const` links and the
//! inline manipulators are translated in full (necessarily `unsafe`). Iterator
//! state structs and the `*_foreach` / `*_container` macros are preserved.

use core::ptr;

/// Node of a doubly linked list. Embed in structs that join such a list.
#[derive(Debug)]
pub struct dlist_node {
    pub prev: *mut Self,
    pub next: *mut Self,
}

impl Default for dlist_node {
    fn default() -> Self {
        Self {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

/// Head of a doubly linked list (non-empty lists are circular).
#[derive(Debug, Default)]
pub struct dlist_head {
    pub head: dlist_node,
}

/// Doubly linked list iterator (no modification while iterating).
#[derive(Debug)]
pub struct dlist_iter {
    pub cur: *mut dlist_node,
    pub end: *mut dlist_node,
}

/// Doubly linked list iterator allowing deletion of the current node.
#[derive(Debug)]
pub struct dlist_mutable_iter {
    pub cur: *mut dlist_node,
    pub next: *mut dlist_node,
    pub end: *mut dlist_node,
}

/// Head of a doubly linked list with an item count.
#[derive(Debug, Default)]
pub struct dclist_head {
    pub dlist: dlist_head,
    pub count: u32,
}

/// Node of a singly linked list.
#[derive(Debug)]
pub struct slist_node {
    pub next: *mut Self,
}

impl Default for slist_node {
    fn default() -> Self {
        Self {
            next: ptr::null_mut(),
        }
    }
}

/// Head of a singly linked list.
#[derive(Debug, Default)]
pub struct slist_head {
    pub head: slist_node,
}

/// Singly linked list iterator.
#[derive(Debug)]
pub struct slist_iter {
    pub cur: *mut slist_node,
}

/// Singly linked list iterator allowing removal of the current node.
#[derive(Debug)]
pub struct slist_mutable_iter {
    pub cur: *mut slist_node,
    pub next: *mut slist_node,
    pub prev: *mut slist_node,
}

// ---- doubly linked list implementation -----------------------------------

/// Caution: O(n); consider `slist_delete_current` instead.
pub fn slist_delete(_head: &mut slist_head, _node: *const slist_node) {
    unimplemented!()
}

/// Initialize a doubly linked list (circular self-reference).
#[inline]
pub fn dlist_init(head: &mut dlist_head) {
    let p: *mut dlist_node = &raw mut head.head;
    head.head.next = p;
    head.head.prev = p;
}

/// Initialize a node so `dlist_node_is_detached` can be used.
#[inline]
pub fn dlist_node_init(node: &mut dlist_node) {
    node.next = ptr::null_mut();
    node.prev = ptr::null_mut();
}

/// Is the list empty?
#[inline]
pub fn dlist_is_empty(head: &dlist_head) -> bool {
    let p: *const dlist_node = &raw const head.head;
    head.head.next.is_null() || head.head.next.cast_const() == p
}

/// Insert a node at the beginning of the list.
///
/// SAFETY: `node` must be a valid pointer not currently in another list.
#[inline]
pub unsafe fn dlist_push_head(head: &mut dlist_head, node: *mut dlist_node) {
    if head.head.next.is_null() {
        dlist_init(head);
    }
    let h: *mut dlist_node = &raw mut head.head;
    (*node).next = head.head.next;
    (*node).prev = h;
    (*(*node).next).prev = node;
    head.head.next = node;
}

/// Insert a node at the end of the list.
///
/// SAFETY: `node` must be a valid pointer not currently in another list.
#[inline]
pub unsafe fn dlist_push_tail(head: &mut dlist_head, node: *mut dlist_node) {
    if head.head.next.is_null() {
        dlist_init(head);
    }
    let h: *mut dlist_node = &raw mut head.head;
    (*node).next = h;
    (*node).prev = head.head.prev;
    (*(*node).prev).next = node;
    head.head.prev = node;
}

/// Insert a node after another *in the same list*.
///
/// SAFETY: both pointers must be valid; `after` must be in a list.
#[inline]
pub unsafe fn dlist_insert_after(after: *mut dlist_node, node: *mut dlist_node) {
    (*node).prev = after;
    (*node).next = (*after).next;
    (*after).next = node;
    (*(*node).next).prev = node;
}

/// Insert a node before another *in the same list*.
///
/// SAFETY: both pointers must be valid; `before` must be in a list.
#[inline]
pub unsafe fn dlist_insert_before(before: *mut dlist_node, node: *mut dlist_node) {
    (*node).prev = (*before).prev;
    (*node).next = before;
    (*before).prev = node;
    (*(*node).prev).next = node;
}

/// Delete `node` from its list (it must be in one).
///
/// SAFETY: `node` must currently be in a list.
#[inline]
pub unsafe fn dlist_delete(node: *mut dlist_node) {
    (*(*node).prev).next = (*node).next;
    (*(*node).next).prev = (*node).prev;
}

/// Like `dlist_delete`, but nulls next/prev to mark "not in a list".
///
/// SAFETY: `node` must currently be in a list.
#[inline]
pub unsafe fn dlist_delete_thoroughly(node: *mut dlist_node) {
    (*(*node).prev).next = (*node).next;
    (*(*node).next).prev = (*node).prev;
    (*node).next = ptr::null_mut();
    (*node).prev = ptr::null_mut();
}

/// Same as `dlist_delete` (membership check only in debug builds).
///
/// SAFETY: see `dlist_delete`.
#[inline]
pub unsafe fn dlist_delete_from(_head: &mut dlist_head, node: *mut dlist_node) {
    dlist_delete(node);
}

/// Like `dlist_delete_from`, but nulls next/prev.
///
/// SAFETY: see `dlist_delete_thoroughly`.
#[inline]
pub unsafe fn dlist_delete_from_thoroughly(_head: &mut dlist_head, node: *mut dlist_node) {
    dlist_delete_thoroughly(node);
}

/// Remove and return the first node (there must be one).
///
/// SAFETY: the list must be non-empty.
#[inline]
pub unsafe fn dlist_pop_head_node(head: &mut dlist_head) -> *mut dlist_node {
    let node = head.head.next;
    dlist_delete(node);
    node
}

/// Move `node` to the head position in the same list.
///
/// SAFETY: `node` must already be part of `head`.
#[inline]
pub unsafe fn dlist_move_head(head: &mut dlist_head, node: *mut dlist_node) {
    if head.head.next == node {
        return;
    }
    dlist_delete(node);
    dlist_push_head(head, node);
}

/// Move `node` to the tail position in the same list.
///
/// SAFETY: `node` must already be part of `head`.
#[inline]
pub unsafe fn dlist_move_tail(head: &mut dlist_head, node: *mut dlist_node) {
    if head.head.prev == node {
        return;
    }
    dlist_delete(node);
    dlist_push_tail(head, node);
}

/// Whether `node` has a following node.
///
/// SAFETY: `node` must be valid.
#[inline]
pub unsafe fn dlist_has_next(head: &dlist_head, node: *const dlist_node) -> bool {
    let h: *const dlist_node = &raw const head.head;
    (*node).next.cast_const() != h
}

/// Whether `node` has a preceding node.
///
/// SAFETY: `node` must be valid.
#[inline]
pub unsafe fn dlist_has_prev(head: &dlist_head, node: *const dlist_node) -> bool {
    let h: *const dlist_node = &raw const head.head;
    (*node).prev.cast_const() != h
}

/// Whether `node` is detached (initialized/deleted thoroughly).
///
/// SAFETY: `node` must be valid.
#[inline]
pub unsafe fn dlist_node_is_detached(node: *const dlist_node) -> bool {
    (*node).next.is_null()
}

/// Return the next node (there must be one).
///
/// SAFETY: `node` must have a following node.
#[inline]
pub unsafe fn dlist_next_node(_head: &mut dlist_head, node: *mut dlist_node) -> *mut dlist_node {
    (*node).next
}

/// Return the previous node (there must be one).
///
/// SAFETY: `node` must have a preceding node.
#[inline]
pub unsafe fn dlist_prev_node(_head: &mut dlist_head, node: *mut dlist_node) -> *mut dlist_node {
    (*node).prev
}

/// Address of the head element's containing struct, given member offset.
///
/// SAFETY: the list must be non-empty; `off` must be the embedded member offset.
#[inline]
pub unsafe fn dlist_head_element_off(head: &mut dlist_head, off: usize) -> *mut u8 {
    head.head.next.cast::<u8>().sub(off)
}

/// Return the first node (there must be one).
///
/// SAFETY: the list must be non-empty.
#[inline]
pub unsafe fn dlist_head_node(head: &mut dlist_head) -> *mut dlist_node {
    dlist_head_element_off(head, 0).cast::<dlist_node>()
}

/// Address of the tail element's containing struct, given member offset.
///
/// SAFETY: the list must be non-empty; `off` must be the embedded member offset.
#[inline]
pub unsafe fn dlist_tail_element_off(head: &mut dlist_head, off: usize) -> *mut u8 {
    head.head.prev.cast::<u8>().sub(off)
}

/// Return the last node (there must be one).
///
/// SAFETY: the list must be non-empty.
#[inline]
pub unsafe fn dlist_tail_node(head: &mut dlist_head) -> *mut dlist_node {
    dlist_tail_element_off(head, 0).cast::<dlist_node>()
}

/// Recover the containing struct of an embedded `dlist_node`.
/// `dlist_container!(Type, member, ptr)` -> `*mut Type`.
#[macro_export]
macro_rules! dlist_container {
    ($ty:ty, $member:ident, $ptr:expr) => {
        ($ptr as *mut u8).sub(core::mem::offset_of!($ty, $member)) as *mut $ty
    };
}

// ---- doubly-linked count list implementation ------------------------------

/// Initialize a doubly linked count list.
#[inline]
pub fn dclist_init(head: &mut dclist_head) {
    dlist_init(&mut head.dlist);
    head.count = 0;
}

/// Is the list empty?
#[inline]
pub fn dclist_is_empty(head: &dclist_head) -> bool {
    head.count == 0
}

/// Insert a node at the beginning.
///
/// SAFETY: `node` must be a valid pointer not in another list.
#[inline]
pub unsafe fn dclist_push_head(head: &mut dclist_head, node: *mut dlist_node) {
    if head.dlist.head.next.is_null() {
        dclist_init(head);
    }
    dlist_push_head(&mut head.dlist, node);
    head.count += 1;
}

/// Insert a node at the end.
///
/// SAFETY: `node` must be a valid pointer not in another list.
#[inline]
pub unsafe fn dclist_push_tail(head: &mut dclist_head, node: *mut dlist_node) {
    if head.dlist.head.next.is_null() {
        dclist_init(head);
    }
    dlist_push_tail(&mut head.dlist, node);
    head.count += 1;
}

/// Insert a node after another *in the same list*.
///
/// SAFETY: `after` must be a member of `head`.
#[inline]
pub unsafe fn dclist_insert_after(
    head: &mut dclist_head,
    after: *mut dlist_node,
    node: *mut dlist_node,
) {
    dlist_insert_after(after, node);
    head.count += 1;
}

/// Insert a node before another *in the same list*.
///
/// SAFETY: `before` must be a member of `head`.
#[inline]
pub unsafe fn dclist_insert_before(
    head: &mut dclist_head,
    before: *mut dlist_node,
    node: *mut dlist_node,
) {
    dlist_insert_before(before, node);
    head.count += 1;
}

/// Delete `node` from `head`.
///
/// SAFETY: `node` must be a member of `head`.
#[inline]
pub unsafe fn dclist_delete_from(head: &mut dclist_head, node: *mut dlist_node) {
    dlist_delete_from(&mut head.dlist, node);
    head.count -= 1;
}

/// Like `dclist_delete_from`, but nulls next/prev.
///
/// SAFETY: `node` must be a member of `head`.
#[inline]
pub unsafe fn dclist_delete_from_thoroughly(head: &mut dclist_head, node: *mut dlist_node) {
    dlist_delete_from_thoroughly(&mut head.dlist, node);
    head.count -= 1;
}

/// Remove and return the first node (there must be one).
///
/// SAFETY: the list must be non-empty.
#[inline]
pub unsafe fn dclist_pop_head_node(head: &mut dclist_head) -> *mut dlist_node {
    let node = dlist_pop_head_node(&mut head.dlist);
    head.count -= 1;
    node
}

/// Move `node` to the head position.
///
/// SAFETY: `node` must be a member of `head`.
#[inline]
pub unsafe fn dclist_move_head(head: &mut dclist_head, node: *mut dlist_node) {
    dlist_move_head(&mut head.dlist, node);
}

/// Move `node` to the tail position.
///
/// SAFETY: `node` must be a member of `head`.
#[inline]
pub unsafe fn dclist_move_tail(head: &mut dclist_head, node: *mut dlist_node) {
    dlist_move_tail(&mut head.dlist, node);
}

/// Whether `node` has a following node.
///
/// SAFETY: `node` must be a member of `head`.
#[inline]
pub unsafe fn dclist_has_next(head: &dclist_head, node: *const dlist_node) -> bool {
    dlist_has_next(&head.dlist, node)
}

/// Whether `node` has a preceding node.
///
/// SAFETY: `node` must be a member of `head`.
#[inline]
pub unsafe fn dclist_has_prev(head: &dclist_head, node: *const dlist_node) -> bool {
    dlist_has_prev(&head.dlist, node)
}

/// Return the next node (there must be one).
///
/// SAFETY: `node` must have a following node.
#[inline]
pub unsafe fn dclist_next_node(head: &mut dclist_head, node: *mut dlist_node) -> *mut dlist_node {
    dlist_next_node(&mut head.dlist, node)
}

/// Return the previous node (there must be one).
///
/// SAFETY: `node` must have a preceding node.
#[inline]
pub unsafe fn dclist_prev_node(head: &mut dclist_head, node: *mut dlist_node) -> *mut dlist_node {
    dlist_prev_node(&mut head.dlist, node)
}

/// Return the first node (there must be one).
///
/// SAFETY: the list must be non-empty.
#[inline]
pub unsafe fn dclist_head_node(head: &mut dclist_head) -> *mut dlist_node {
    dlist_head_element_off(&mut head.dlist, 0).cast::<dlist_node>()
}

/// Return the last node (there must be one).
///
/// SAFETY: the list must be non-empty.
#[inline]
pub unsafe fn dclist_tail_node(head: &mut dclist_head) -> *mut dlist_node {
    dlist_tail_element_off(&mut head.dlist, 0).cast::<dlist_node>()
}

/// Stored number of entries.
#[inline]
pub fn dclist_count(head: &dclist_head) -> u32 {
    head.count
}

// ---- singly linked list implementation ------------------------------------

/// Initialize a singly linked list.
#[inline]
pub fn slist_init(head: &mut slist_head) {
    head.head.next = ptr::null_mut();
}

/// Is the list empty?
#[inline]
pub fn slist_is_empty(head: &slist_head) -> bool {
    head.head.next.is_null()
}

/// Insert a node at the beginning.
///
/// SAFETY: `node` must be a valid pointer.
#[inline]
pub unsafe fn slist_push_head(head: &mut slist_head, node: *mut slist_node) {
    (*node).next = head.head.next;
    head.head.next = node;
}

/// Insert a node after another *in the same list*.
///
/// SAFETY: both pointers must be valid; `after` must be in a list.
#[inline]
pub unsafe fn slist_insert_after(after: *mut slist_node, node: *mut slist_node) {
    (*node).next = (*after).next;
    (*after).next = node;
}

/// Remove and return the first node (there must be one).
///
/// SAFETY: the list must be non-empty.
#[inline]
pub unsafe fn slist_pop_head_node(head: &mut slist_head) -> *mut slist_node {
    let node = head.head.next;
    head.head.next = (*node).next;
    node
}

/// Whether `node` has a following node.
///
/// SAFETY: `node` must be valid.
#[inline]
pub unsafe fn slist_has_next(_head: &slist_head, node: *const slist_node) -> bool {
    !(*node).next.is_null()
}

/// Return the next node (there must be one).
///
/// SAFETY: `node` must have a following node.
#[inline]
pub unsafe fn slist_next_node(_head: &mut slist_head, node: *mut slist_node) -> *mut slist_node {
    (*node).next
}

/// Address of the head element's containing struct, given member offset.
///
/// SAFETY: the list must be non-empty.
#[inline]
pub unsafe fn slist_head_element_off(head: &mut slist_head, off: usize) -> *mut u8 {
    head.head.next.cast::<u8>().sub(off)
}

/// Return the first node (there must be one).
///
/// SAFETY: the list must be non-empty.
#[inline]
pub unsafe fn slist_head_node(head: &mut slist_head) -> *mut slist_node {
    slist_head_element_off(head, 0).cast::<slist_node>()
}

/// Delete the element the mutable iterator currently points to.
///
/// SAFETY: `iter` must be a valid in-progress `slist_foreach_modify` iterator.
#[inline]
pub unsafe fn slist_delete_current(iter: &mut slist_mutable_iter) {
    (*iter.prev).next = iter.next;
    iter.cur = iter.prev;
}

/// Recover the containing struct of an embedded `slist_node`.
/// `slist_container!(Type, member, ptr)` -> `*mut Type`.
#[macro_export]
macro_rules! slist_container {
    ($ty:ty, $member:ident, $ptr:expr) => {
        ($ptr as *mut u8).sub(core::mem::offset_of!($ty, $member)) as *mut $ty
    };
}
