//! Translated from PostgreSQL src/include/executor/execScan.h
//!
//! Inline-able support functions for Scan nodes. The C `ExecScanAccessMtd` /
//! `ExecScanRecheckMtd` fn-pointer typedefs already live in
//! `crate::executor::executor`; re-exported here for call-site parity. The two
//! `pg_attribute_always_inline` helpers below carry the EPQ-recheck control
//! flow; bodies are stubbed pending the EState<'_>/EPQ field model in Phase 2.

use crate::executor::executor::{ExecScanAccessMtd, ExecScanRecheckMtd};
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::execnodes::{EPQState, ExprState, ProjectionInfo, ScanState};

/// ExecScanFetch -- check interrupts & fetch next potential tuple.
///
/// Substitutes a test tuple when inside an EvalPlanQual recheck, else runs the
/// access method's next-tuple routine. `epqstate == NULL` -> `None`. Returns
/// `None` for the C `NULL`/empty-slot cases.
pub fn ExecScanFetch(
    _node: &mut ScanState,
    _epqstate: Option<&mut EPQState>,
    _access_mtd: ExecScanAccessMtd,
    _recheck_mtd: ExecScanRecheckMtd,
) -> Option<Box<TupleTableSlot>> {
    unimplemented!()
}

/// ExecScanExtended -- scan with optional qual and projection.
///
/// Alternative to `ExecScan` for callers that may omit `qual`/`projInfo`
/// (`None`). Loops until a tuple passes the qual; applies the projection when
/// present, else returns the raw scan tuple. `None` signals end of scan.
pub fn ExecScanExtended(
    _node: &mut ScanState,
    _access_mtd: ExecScanAccessMtd,
    _recheck_mtd: ExecScanRecheckMtd,
    _epqstate: Option<&mut EPQState>,
    _qual: Option<&mut ExprState>,
    _proj_info: Option<&mut ProjectionInfo>,
) -> Option<Box<TupleTableSlot>> {
    unimplemented!()
}
