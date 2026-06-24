//! Translated from PostgreSQL src/include/lib/integerset.h
//!
//! In-memory set of u64 integers stored compactly. `IntegerSet` is opaque in C.

/// A compact set of integers (opaque; internal layout in the implementation).
pub struct IntegerSet {
    _private: (),
}

/// intset_create: create an empty set.
pub fn intset_create() -> IntegerSet {
    unimplemented!()
}

/// intset_add_member: add `x`. Values must be added in ascending order.
pub fn intset_add_member(_intset: &mut IntegerSet, _x: u64) {
    unimplemented!()
}

/// intset_is_member: test membership.
pub fn intset_is_member(_intset: &IntegerSet, _x: u64) -> bool {
    unimplemented!()
}

/// intset_num_entries: number of members.
pub fn intset_num_entries(_intset: &IntegerSet) -> u64 {
    unimplemented!()
}

/// intset_memory_usage: bytes of memory used.
pub fn intset_memory_usage(_intset: &IntegerSet) -> u64 {
    unimplemented!()
}

/// intset_begin_iterate: start an in-order iteration.
pub fn intset_begin_iterate(_intset: &mut IntegerSet) {
    unimplemented!()
}

/// intset_iterate_next: next member, or None when exhausted.
/// (C's `bool` return plus `uint64 *next` out-param collapses to `Option`.)
pub fn intset_iterate_next(_intset: &mut IntegerSet) -> Option<u64> {
    unimplemented!()
}
