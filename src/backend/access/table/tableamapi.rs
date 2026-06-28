//! Table AM API resolution. Translated from
//! backend/access/table/tableamapi.c.
//!
//! In this port the `TableAmRoutine` fn-pointer vtable is a closed enum
//! `TableAmKind` (heap is the only in-tree AM); see `access/tableam.rs`. So
//! `GetTableAmRoutine` resolves a handler OID to that enum rather than calling
//! the handler and asserting each callback is non-NULL (the closed enum makes
//! every callback present by construction). `heap_tableam_handler` is the heap
//! AM's handler entry.

use crate::access::tableam::TableAmKind;
use crate::postgres_ext::Oid;

/// `GetTableAmRoutine`: resolve a table AM handler OID to its routine. M2: heap
/// is the only built-in AM, so any valid handler resolves to `Heap`. (Extension
/// AMs -- the open fn-pointer case -- are out of scope.)
pub fn get_table_am_routine(_amhandler: Oid) -> TableAmKind {
    TableAmKind::Heap
}

/// `heap_tableam_handler`: the heap AM's `amhandler` function. C returns a
/// pointer to the static `heapam_methods` vtable; here it yields the heap kind.
pub fn heap_tableam_handler() -> TableAmKind {
    TableAmKind::Heap
}
