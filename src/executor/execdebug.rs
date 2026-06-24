//! Translated from PostgreSQL src/include/executor/execdebug.h
//!
//! Debug-only executor printf scaffolding. The EXEC_*DEBUG groups are compiled
//! out by default in C; under this port they collapse to no-ops. Newer code uses
//! elog() instead, so this header is effectively a tombstone. Only the two pure
//! formatting helpers carry over.

use crate::executor::tuptable::TupleTableSlot;

/// C: `T_OR_F(b)` -> "true"/"false".
pub fn T_OR_F(b: bool) -> &'static str {
    if b {
        "true"
    } else {
        "false"
    }
}

/// C: `NULL_OR_TUPLE(slot)` -> "null"/"a tuple" (TupIsNull check).
pub fn NULL_OR_TUPLE(slot: Option<&TupleTableSlot>) -> &'static str {
    match slot {
        None => "null",
        Some(_) => "a tuple",
    }
}

// All NL_*/SO_*/MJ_* debugging macros are no-ops in the default (non-debug)
// build; intentionally not translated.
