//! TidScan node executor. Translated from
//! backend/executor/nodeTidscan.c (disposition: full leaf for M8, step 34).
//!
//! `ExecTidScan` scans the specific TIDs named by the node's `tidquals` (e.g.
//! `WHERE ctid = '(0,1)'` or `WHERE CURRENT OF cursor`). It evaluates the tidquals
//! into a TID list, then fetches each heap tuple by TID (table_tuple_fetch_row_version
//! -> heap_fetch_tid) under the query snapshot, deforming it into the scan slot.
//!
//! The leaf is translated faithfully (the TID-list build + the per-TID fetch loop).
//! The tidqual EXPRESSION evaluation that produces the TID list -- a `ctid = <const>`
//! OpExpr or a `ctid = ANY(array)` ScalarArrayOpExpr over the system `ctid` column --
//! needs system-column-Var / array evaluation that is staged (no cursor / WHERE
//! CURRENT OF reaches the executor this milestone). So `TidListEval` is a clean
//! grow guard; the surrounding scan mechanics are complete.

use std::sync::Arc;

use crate::nodes::execnodes::{EState, ScanState, TidScanState, TupleTableSlot};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::TidScan;
use crate::shared_state::SharedState;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::rel::RelationData;

/// Run-state pairing the PG `TidScanState` with the borrowed scan relation + snapshot.
pub struct TidScanRun<'rel> {
    pub state: Box<TidScanState>,
    /// the scanned relation, borrowed from the EState range table.
    pub relation: &'rel RelationData,
    /// the query snapshot, borrowed from the EState.
    pub snapshot: &'rel crate::utils::snapshot::SnapshotData,
    /// the TID quals (raw expressions producing the TID list).
    pub tidquals: Vec<Node>,
    /// the materialized TID list (built lazily on the first `ExecTidScan`).
    pub tids: Option<Vec<ItemPointerData>>,
    /// the next TID index to fetch.
    pub cursor: usize,
}

/// PG `ExecInitTidScan`: open the scanned relation (borrowed), set up the scan slot,
/// and stash the tidquals for lazy TID-list evaluation.
pub fn exec_init_tid_scan<'rel>(
    node: &TidScan,
    estate: &mut EState<'rel>,
    _eflags: i32,
) -> Box<TidScanRun<'rel>> {
    let rti = node.scan.scanrelid;
    crate::assert!(rti > 0);
    let relation = estate
        .es_range_table_rels
        .get(rti - 1)
        .copied()
        .flatten()
        .unwrap_or_else(|| unimplemented!("ExecInitTidScan: scan relation not registered for RTI"));
    let snapshot = estate
        .es_snapshot_ref
        .unwrap_or_else(|| unimplemented!("ExecInitTidScan: no query snapshot on the EState"));

    let desc = relation
        .rd_att
        .clone()
        .unwrap_or_else(|| unimplemented!("ExecInitTidScan: relation has no descriptor"));
    let scan_slot =
        crate::backend::executor::execTuples::make_tuple_table_slot(Some(desc), &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL);

    let ss = ScanState {
        ss_scan_tuple_slot: Some(scan_slot),
        ..ScanState::default()
    };
    let state = Box::new(TidScanState { ss, ..TidScanState::default() });

    Box::new(TidScanRun {
        state,
        relation,
        snapshot,
        tidquals: node.tidquals.clone(),
        tids: None,
        cursor: 0,
    })
}

/// PG `ExecTidScan` (via `TidNext`): fetch the next tuple by TID. Builds the TID list
/// on the first call (TidListEval), then fetches each TID's heap tuple under the
/// snapshot, deforming it into the scan slot. `None` at the end of the TID list.
pub async fn exec_tid_scan<'r>(
    shared: &Arc<SharedState>,
    run: &'r mut TidScanRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    if run.tids.is_none() {
        run.tids = Some(tid_list_eval(&run.tidquals));
    }
    let tids = run
        .tids
        .as_ref()
        .unwrap_or_else(|| unreachable!("TID list just built"));

    while run.cursor < tids.len() {
        let tid = tids[run.cursor];
        run.cursor += 1;
        let Some(tuple) = crate::backend::access::heap::heapam::heap_fetch_tid(
            shared,
            run.relation,
            &tid,
            run.snapshot,
        )
        .await
        else {
            continue; // not visible / dead -> try the next TID
        };

        let desc = run
            .relation
            .rd_att
            .clone()
            .unwrap_or_else(|| unimplemented!("ExecTidScan: relation has no descriptor"));
        // SAFETY: `tuple` is an owned heap-tuple copy with a valid body.
        let (values, isnull) = unsafe {
            crate::backend::access::common::heaptuple::heap_deform_tuple(&tuple, &desc)
        };
        let slot = run
            .state
            .ss
            .ss_scan_tuple_slot
            .as_mut()
            .unwrap_or_else(|| unimplemented!("ExecTidScan: no scan tuple slot"));
        crate::executor::tuptable::ExecClearTuple(slot);
        let n = values.len();
        slot.values[..n].copy_from_slice(&values);
        slot.isnull[..n].copy_from_slice(&isnull);
        crate::backend::executor::execTuples::exec_store_virtual_tuple(slot);
        slot.tid = tuple.t_self;
        return run.state.ss.ss_scan_tuple_slot.as_deref_mut();
    }
    None
}

/// PG `ExecEndTidScan`: nothing to release (the relation is caller-owned, the slot
/// drops with the node).
pub fn exec_end_tid_scan(_shared: &Arc<SharedState>, _run: &mut TidScanRun<'_>) {}

/// PG `TidListEval`: evaluate the tidquals into a TID list. The `ctid = <const>` /
/// `ctid = ANY(array)` / `CURRENT OF` expression forms need system-column-Var / array
/// evaluation that is staged (no cursor reaches the executor this milestone); the
/// scan mechanics around this are complete (`exec_tid_scan`).
fn tid_list_eval(tidquals: &[Node]) -> Vec<ItemPointerData> {
    if tidquals.is_empty() {
        return Vec::new();
    }
    unimplemented!("TidListEval: ctid-qual TID extraction not yet reachable for this milestone");
}
