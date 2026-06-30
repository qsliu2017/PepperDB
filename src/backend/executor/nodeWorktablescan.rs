//! WorkTableScan node executor. Translated from
//! backend/executor/nodeWorktablescan.c (disposition: full leaf for the M12
//! recursive-CTE working-table scan, with the generic scan qual + projection so the
//! recursive term -- e.g. `SELECT n+1 FROM t WHERE n < 5` over the worktable `t` --
//! is a single self-contained, rescannable WorkTableScan).
//!
//! The recursive term of a RecursiveUnion scans the *current* working table -- the
//! rows the previous iteration produced. PG reaches the working tuplestore through
//! `es_param_exec_vals[wtParam]` -> the RecursiveUnionState; this port shares it via
//! a `WorkTableRef` (an `Arc<Mutex<Vec<Row>>>`) the RecursiveUnion registered on the
//! EState and the WorkTableScan picked up at init. On each (re)scan the node copies
//! the working table's current contents, then drives the generic scan loop: for each
//! source row it sets `ecxt_scantuple`, evaluates the WHERE qual, and projects the
//! target list (the `n+1` expression).
//!
//! Async coloring: kept `async` for dispatch uniformity; it reaches no I/O leaf and
//! holds no lock across an `.await`.

use std::sync::Arc;

use crate::access::tupdesc::TupleDesc;
use crate::backend::executor::execScan::exec_scan;
use crate::backend::executor::execTuples::{exec_type_from_tl, make_tuple_table_slot, TTS_OPS_VIRTUAL};
use crate::backend::executor::execUtils::{create_expr_context, exec_assign_projection_info};
use crate::backend::executor::nodeRecursiveunion::{Row, WorkTableRef};
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, PlanState, ScanState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::WorkTableScan;

/// Run-state pairing the PG `WorkTableScanState` shell with the shared working table
/// and the per-scan snapshot cursor.
pub struct WorkTableScanRun {
    pub ss: Box<ScanState>,
    /// the shared working table (filled by the owning RecursiveUnion).
    pub working_table: WorkTableRef,
    /// the snapshot of the working table taken at the start of this scan.
    snapshot: Vec<Row>,
    /// read cursor into `snapshot`.
    cur: usize,
    /// whether `snapshot` has been taken for the current scan pass.
    loaded: bool,
}

/// PG `ExecInitWorkTableScan`: build the WorkTableScanState. The scan rowtype is the
/// working table's rowtype (`desc` = the recursive CTE's columns); the result
/// rowtype + projection come from the node's target list, and the qual from its
/// `plan.qual` (so the recursive term's `n+1` projection and `n<5` filter run here).
pub fn exec_init_work_table_scan(
    node: &WorkTableScan,
    desc: &TupleDesc,
    estate: &mut EState<'_>,
    working_table: WorkTableRef,
) -> Box<WorkTableScanRun> {
    let scan_desc = Arc::clone(desc);
    let scan_slot = make_tuple_table_slot(Some(Arc::clone(&scan_desc)), &TTS_OPS_VIRTUAL);

    let mut ps = PlanState {
        plan: Some(Node::WorkTableScan(Box::new(node.clone()))),
        scandesc: Some(Arc::clone(&scan_desc)),
        scanops: Some(&TTS_OPS_VIRTUAL),
        scanopsset: true,
        scanopsfixed: true,
        ..PlanState::default()
    };
    ps.ps_expr_context = Some(create_expr_context(estate));

    // Result type + slot from the plan target list (the recursive-term projection).
    let result_desc = exec_type_from_tl(&node.scan.plan.targetlist);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(&result_desc)), &TTS_OPS_VIRTUAL);
    ps.ps_result_tuple_desc = Some(result_desc);
    ps.ps_result_tuple_slot = Some(result_slot);
    ps.resultops = Some(&TTS_OPS_VIRTUAL);
    ps.resultopsset = true;
    ps.resultopsfixed = true;

    // Projection over the scan slot (its Vars resolve against the worktable rowtype).
    exec_assign_projection_info(&mut ps, Some(Arc::clone(&scan_desc)));
    // WHERE qual compiled against the scan slot.
    ps.qual = crate::backend::executor::execExpr::exec_init_qual(&node.scan.plan.qual, None);

    let ss = ScanState {
        ps,
        ss_current_relation: None,
        ss_current_scan_desc: None,
        ss_scan_tuple_slot: Some(scan_slot),
    };

    Box::new(WorkTableScanRun {
        ss: Box::new(ss),
        working_table,
        snapshot: Vec::new(),
        cur: 0,
        loaded: false,
    })
}

/// Load the next working-table row into the scan slot. Returns false at end.
fn work_next(run: &mut WorkTableScanRun) -> bool {
    if !run.loaded {
        let wt = run.working_table.lock();
        run.snapshot.clone_from(&wt);
        drop(wt);
        run.cur = 0;
        run.loaded = true;
    }
    if run.cur >= run.snapshot.len() {
        if let Some(slot) = run.ss.ss_scan_tuple_slot.as_mut() {
            ExecClearTuple(slot);
        }
        return false;
    }
    let (values, isnull) = (run.snapshot[run.cur].0.clone(), run.snapshot[run.cur].1.clone());
    run.cur += 1;

    let slot = run
        .ss
        .ss_scan_tuple_slot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("WorkTableScan: no scan tuple slot"));
    ExecClearTuple(slot);
    let n = values.len();
    slot.values[..n].copy_from_slice(&values);
    slot.isnull[..n].copy_from_slice(&isnull);
    crate::backend::executor::execTuples::exec_store_virtual_tuple(slot);
    true
}

/// PG `ExecWorkTableScan`: drive the generic scan loop over the current working
/// table -- fetch a row, evaluate the qual, project -- returning the projection
/// result slot (or None at end / when all remaining rows fail the qual).
pub async fn exec_work_table_scan(run: &mut WorkTableScanRun) -> Option<&mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();
    loop {
        if !work_next(run) {
            return None;
        }
        if exec_scan(&mut run.ss).is_some() {
            return run
                .ss
                .ps
                .ps_proj_info
                .as_mut()
                .and_then(|p| p.state.resultslot.as_deref_mut());
        }
    }
}

/// PG `ExecReScanWorkTableScan`: drop the snapshot so the next call reloads the
/// (now-swapped) working table.
pub fn exec_rescan_work_table_scan(run: &mut WorkTableScanRun) {
    run.loaded = false;
    run.snapshot.clear();
    run.cur = 0;
    if let Some(slot) = run.ss.ss_scan_tuple_slot.as_mut() {
        ExecClearTuple(slot);
    }
}

/// PG `ExecEndWorkTableScan`: nothing to release (the working table is owned by the
/// RecursiveUnion).
pub fn exec_end_work_table_scan(_run: &mut WorkTableScanRun) {}
