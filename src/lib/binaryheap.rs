//! Translated from PostgreSQL src/include/lib/binaryheap.h
//!
//! A simple binary heap. `bh_node_type` is `Datum` in the backend. The C
//! `binaryheap_comparator` plus opaque `void *arg` collapse to a generic
//! comparator closure `C` stored on the heap (capturing its state, so `arg`
//! disappears -- function-mapping 6.3); the FLEXIBLE_ARRAY_MEMBER node array
//! becomes a `Vec`.

use crate::postgres::Datum;
use core::cmp::Ordering;

pub type bh_node_type = Datum;

/// A binary heap (`bh_size`/`bh_space` are implicit in the `Vec`). Generic over
/// the comparator closure `C`: for a max-heap it returns Less iff a < b, Equal
/// iff a == b, Greater iff a > b; for a min-heap the conditions are reversed.
pub struct binaryheap<C: Fn(bh_node_type, bh_node_type) -> Ordering> {
    pub bh_has_heap_property: bool, // debugging cross-check
    pub bh_compare: C,
    pub bh_nodes: Vec<bh_node_type>,
}

/// binaryheap_allocate: create an empty heap with the given capacity/comparator.
pub fn binaryheap_allocate<C: Fn(bh_node_type, bh_node_type) -> Ordering>(
    capacity: i32,
    compare: C,
) -> binaryheap<C> {
    binaryheap {
        bh_has_heap_property: true,
        bh_compare: compare,
        bh_nodes: Vec::with_capacity(capacity.max(0) as usize),
    }
}

/// binaryheap_reset: empty the heap, keeping its allocation.
pub fn binaryheap_reset<C: Fn(bh_node_type, bh_node_type) -> Ordering>(heap: &mut binaryheap<C>) {
    heap.bh_nodes.clear();
    heap.bh_has_heap_property = true;
}

/// binaryheap_free: drop the heap (RAII; provided for parity).
pub fn binaryheap_free<C: Fn(bh_node_type, bh_node_type) -> Ordering>(heap: binaryheap<C>) {
    drop(heap);
}

/// binaryheap_add_unordered: append without restoring the heap property.
pub fn binaryheap_add_unordered<C: Fn(bh_node_type, bh_node_type) -> Ordering>(
    _heap: &mut binaryheap<C>,
    _d: bh_node_type,
) {
    unimplemented!()
}

/// binaryheap_build: restore the heap property over all nodes.
pub fn binaryheap_build<C: Fn(bh_node_type, bh_node_type) -> Ordering>(_heap: &mut binaryheap<C>) {
    unimplemented!()
}

/// binaryheap_add: insert one node, maintaining the heap property.
pub fn binaryheap_add<C: Fn(bh_node_type, bh_node_type) -> Ordering>(
    _heap: &mut binaryheap<C>,
    _d: bh_node_type,
) {
    unimplemented!()
}

/// binaryheap_first: peek at the root (there must be one).
pub fn binaryheap_first<C: Fn(bh_node_type, bh_node_type) -> Ordering>(
    _heap: &binaryheap<C>,
) -> bh_node_type {
    unimplemented!()
}

/// binaryheap_remove_first: pop the root.
pub fn binaryheap_remove_first<C: Fn(bh_node_type, bh_node_type) -> Ordering>(
    _heap: &mut binaryheap<C>,
) -> bh_node_type {
    unimplemented!()
}

/// binaryheap_remove_node: remove the node at index `n`.
pub fn binaryheap_remove_node<C: Fn(bh_node_type, bh_node_type) -> Ordering>(
    _heap: &mut binaryheap<C>,
    _n: i32,
) {
    unimplemented!()
}

/// binaryheap_replace_first: replace the root then sift down.
pub fn binaryheap_replace_first<C: Fn(bh_node_type, bh_node_type) -> Ordering>(
    _heap: &mut binaryheap<C>,
    _d: bh_node_type,
) {
    unimplemented!()
}

#[inline]
pub fn binaryheap_empty<C: Fn(bh_node_type, bh_node_type) -> Ordering>(h: &binaryheap<C>) -> bool {
    h.bh_nodes.is_empty()
}

#[inline]
pub fn binaryheap_size<C: Fn(bh_node_type, bh_node_type) -> Ordering>(h: &binaryheap<C>) -> i32 {
    h.bh_nodes.len() as i32
}

#[inline]
pub fn binaryheap_get_node<C: Fn(bh_node_type, bh_node_type) -> Ordering>(
    h: &binaryheap<C>,
    n: i32,
) -> bh_node_type {
    h.bh_nodes[n as usize]
}
