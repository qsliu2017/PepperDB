//! Header for PostgreSQL src/include/utils/tuplestore.h.
//!
//! The materialized tuple store is translated in
//! `crate::backend::utils::sort::tuplestore` (step 24). This header re-exports
//! the implementation under the C-facing path so existing `crate::utils::
//! tuplestore::*` call sites keep resolving. The store is now an owned
//! `Tuplestorestate` (genuinely `Send`), not the former opaque stub.

pub use crate::backend::utils::sort::tuplestore::{
    tuplestore_advance, tuplestore_alloc_read_pointer, tuplestore_ateof, tuplestore_begin_heap,
    tuplestore_clear, tuplestore_copy_read_pointer, tuplestore_end, tuplestore_get_stats,
    tuplestore_gettupleslot, tuplestore_in_memory, tuplestore_markpos, tuplestore_putvalues,
    tuplestore_puttupleslot, tuplestore_rescan, tuplestore_restorepos, tuplestore_select_read_pointer,
    tuplestore_set_eflags, tuplestore_set_tupdesc, tuplestore_skiptuples, tuplestore_trim,
    tuplestore_tuple_count, StoredTuple, Tuplestorestate, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
    EXEC_FLAG_REWIND,
};
