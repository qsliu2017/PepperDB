//! Translated from PostgreSQL src/include/utils/tuplestore.h
//!
//! Generalized temporary tuple storage (Materialize nodes, hashjoin batches,
//! cursors, ...). Stores a sequence of MinimalTuples, spilling to a temp file
//! past a size limit; supports multiple independent read pointers.

use crate::access::htup::HeapTuple;
use crate::access::tupdesc::TupleDesc;
use crate::executor::tuptable::TupleTableSlot;
use crate::postgres::Datum;

/// Tuplestorestate - opaque (details private to tuplestore.c). Bespoke
/// spill-to-disk container; kept as an opaque type.
pub struct Tuplestorestate {
    _private: (),
}

pub fn tuplestore_begin_heap(_random_access: bool, _inter_xact: bool, _max_kbytes: i32) -> *mut Tuplestorestate {
    unimplemented!()
}

pub fn tuplestore_set_eflags(_state: &mut Tuplestorestate, _eflags: i32) {
    unimplemented!()
}

pub fn tuplestore_puttupleslot(_state: &mut Tuplestorestate, _slot: &mut TupleTableSlot) {
    unimplemented!()
}

pub fn tuplestore_puttuple(_state: &mut Tuplestorestate, _tuple: HeapTuple) {
    unimplemented!()
}

pub fn tuplestore_putvalues(_state: &mut Tuplestorestate, _tdesc: TupleDesc, _values: &[Datum], _isnull: &[bool]) {
    unimplemented!()
}

pub fn tuplestore_alloc_read_pointer(_state: &mut Tuplestorestate, _eflags: i32) -> i32 {
    unimplemented!()
}

pub fn tuplestore_select_read_pointer(_state: &mut Tuplestorestate, _ptr: i32) {
    unimplemented!()
}

pub fn tuplestore_copy_read_pointer(_state: &mut Tuplestorestate, _srcptr: i32, _destptr: i32) {
    unimplemented!()
}

pub fn tuplestore_trim(_state: &mut Tuplestorestate) {
    unimplemented!()
}

/// `tuplestore_get_stats` - out-params `max_storage_type` + `max_space` -> tuple.
pub fn tuplestore_get_stats(_state: &mut Tuplestorestate) -> (String, i64) {
    unimplemented!()
}

pub fn tuplestore_in_memory(_state: &mut Tuplestorestate) -> bool {
    unimplemented!()
}

pub fn tuplestore_gettupleslot(_state: &mut Tuplestorestate, _forward: bool, _copy: bool, _slot: &mut TupleTableSlot) -> bool {
    unimplemented!()
}

pub fn tuplestore_advance(_state: &mut Tuplestorestate, _forward: bool) -> bool {
    unimplemented!()
}

pub fn tuplestore_skiptuples(_state: &mut Tuplestorestate, _ntuples: i64, _forward: bool) -> bool {
    unimplemented!()
}

pub fn tuplestore_tuple_count(_state: &mut Tuplestorestate) -> i64 {
    unimplemented!()
}

pub fn tuplestore_ateof(_state: &mut Tuplestorestate) -> bool {
    unimplemented!()
}

pub fn tuplestore_rescan(_state: &mut Tuplestorestate) {
    unimplemented!()
}

pub fn tuplestore_clear(_state: &mut Tuplestorestate) {
    unimplemented!()
}

pub fn tuplestore_end(_state: &mut Tuplestorestate) {
    unimplemented!()
}
