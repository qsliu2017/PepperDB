//! Translated from PostgreSQL src/include/commands/copyapi.h

use crate::access::tupdesc::TupleDesc;
use crate::commands::copy::{CopyFromState, CopyToState};
use crate::executor::tuptable::TupleTableSlot;
use crate::fmgr::FmgrInfo;
use crate::nodes::execnodes::ExprContext;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// CopyToRoutine / CopyFromRoutine are routine structs (struct of fn pointers).
// routine-struct.md group C: per-instance behaviour table, "all required" -- so
// each maps to one base trait with no optional supertraits. The built-in formats
// (text/csv/binary) are the closed set; dispatch statically over an `enum` in
// Phase 2 (no `&dyn`).

/// API for a COPY TO format implementation (was `CopyToRoutine`). All required.
pub trait CopyToRoutine {
    /// Set output function info; called once at start of COPY TO. `finfo` may be
    /// optionally filled with the output function's catalog info.
    fn out_func(&self, cstate: CopyToState, atttypid: Oid, finfo: &mut FmgrInfo);

    /// Start a COPY TO; called once at the beginning.
    fn start(&self, cstate: CopyToState, tup_desc: TupleDesc);

    /// Write one row stored in `slot` to the destination.
    fn one_row(&self, cstate: CopyToState, slot: &mut TupleTableSlot);

    /// End a COPY TO; called once at the end.
    fn end(&self, cstate: CopyToState);
}

/// API for a COPY FROM format implementation (was `CopyFromRoutine`). All required.
pub trait CopyFromRoutine {
    /// Set input function info; called once at start of COPY FROM. `finfo` and
    /// `typioparam` may be optionally filled (the type to pass to the input fn).
    fn in_func(&self, cstate: CopyFromState, atttypid: Oid, finfo: &mut FmgrInfo,
               typioparam: &mut Oid);

    /// Start a COPY FROM; called once at the beginning.
    fn start(&self, cstate: CopyFromState, tup_desc: TupleDesc);

    /// Read one row from the source, filling `values`/`nulls`. `econtext`
    /// evaluates default expressions (None if no defaults). Returns false when
    /// there are no more tuples.
    fn one_row(&self, cstate: CopyFromState, econtext: Option<&mut ExprContext>,
               values: &mut [Datum], nulls: &mut [bool]) -> bool;

    /// End a COPY FROM; called once at the end.
    fn end(&self, cstate: CopyFromState);
}
