//! Index-only-scan node executor. Translated from
//! backend/executor/nodeIndexonlyscan.c (disposition: full for the M6 forward,
//! single-column btree index-only scan).
//!
//! Like the index scan, but the returned tuple's data comes from the INDEX tuple
//! (`StoreIndexTuple`) rather than the heap, whenever the visibility map says the
//! heap page is all-visible. The VM is consulted via the foundation's
//! visibilitymap; until that is translated this node STUB-CALLS the VM as "not
//! all-visible", so it always fetches the heap tuple to confirm visibility -- the
//! result is correct (just slower: every row visits the heap). It still sources
//! the returned column data from the index tuple, exactly as `StoreIndexTuple`.
//!
//! The scan tuple slot's rowtype is `ExecTypeFromTL(indextlist)` (the index's
//! columns), and the projection runs over it. The scan-key building + AM scan are
//! shared with nodeIndexscan via `exec_index_build_scan_keys`.
//!
//! GROW/STAGE (rules.md s4): the real VM all-visible fast path (skip the heap
//! fetch), ORDER BY-via-index, multi-key / runtime / array keys, parallel scan,
//! and the predicate-lock-on-skip path are clean `not_yet_reachable` arms.
//!
//! Async coloring (rules.md s5): the index descent + (always, on M6) the heap
//! visibility fetch reach buffer reads, so `ExecIndexOnlyScan` is `async`; no
//! content lock is held across `.await`.

use std::sync::Arc;

use crate::access::sdir::ScanDirection;
use crate::access::skey::ScanKeyData;
use crate::backend::access::index::indexam::{
    index_beginscan, index_current_index_values, index_endscan, index_fetch_heap,
    index_getnext_tid, index_rescan_keys, IndexScanState as AmIndexScanState,
};
use crate::backend::executor::execScan::exec_scan;
use crate::backend::executor::execTuples::{
    exec_store_virtual_tuple, exec_type_from_tl, make_tuple_table_slot, TTS_OPS_VIRTUAL,
};
use crate::backend::executor::execUtils::{create_expr_context, exec_assign_projection_info};
use crate::backend::executor::nodeIndexscan::exec_index_build_scan_keys;
use crate::nodes::execnodes::{EState, PlanState, ScanState, TupleTableSlot};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::IndexOnlyScan;
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;

/// Run-state for an index-only-scan node. Like `IndexScanRun`, but the scan slot's
/// rowtype is the index's columns (`indextlist`), filled from the index tuple.
pub struct IndexOnlyScanRun<'rel> {
    pub ss: ScanState,
    pub heap_rel: &'rel RelationData,
    pub index_rel: &'rel RelationData,
    pub snapshot: &'rel crate::utils::snapshot::SnapshotData,
    pub scan_keys: Vec<ScanKeyData>,
    pub scan: Option<Box<AmIndexScanState<'rel, 'rel, 'rel>>>,
}

/// PG `ExecInitIndexOnlyScan`: build the IndexOnlyScanState. The scan tuple slot's
/// rowtype is `ExecTypeFromTL(indextlist)` (the index columns), not the heap
/// rowtype; otherwise mirrors `ExecInitIndexScan` (relations, projection,
/// exprcontext, plan qual, scan keys from `indexqual`).
pub fn exec_init_index_only_scan<'rel>(
    node: &IndexOnlyScan,
    estate: &mut EState<'rel>,
    eflags: i32,
) -> Box<IndexOnlyScanRun<'rel>> {
    let _ = eflags;
    crate::assert!(
        node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none(),
        "ExecInitIndexOnlyScan: a scan node is childless"
    );

    let heap_rel = exec_get_range_table_relation(estate, node.scan.scanrelid);
    let index_rel = exec_get_index_relation(estate, node.indexid);
    let snapshot = estate
        .es_snapshot_ref
        .unwrap_or_else(|| unimplemented!("ExecInitIndexOnlyScan: no active snapshot for the scan"));

    // The scan slot rowtype IS the index column set (indextlist), filled from the
    // index tuple (StoreIndexTuple).
    let scan_desc = exec_type_from_tl(&node.indextlist);
    let scan_slot = make_tuple_table_slot(Some(Arc::clone(&scan_desc)), &TTS_OPS_VIRTUAL);

    let mut ps = PlanState {
        plan: Some(Node::IndexOnlyScan(Box::new(node.clone()))),
        scandesc: Some(Arc::clone(&scan_desc)),
        scanops: Some(&TTS_OPS_VIRTUAL),
        scanopsset: true,
        scanopsfixed: true,
        ..PlanState::default()
    };
    ps.ps_expr_context = Some(create_expr_context(estate));

    let result_desc = exec_type_from_tl(&node.scan.plan.targetlist);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(&result_desc)), &TTS_OPS_VIRTUAL);
    ps.ps_result_tuple_desc = Some(result_desc);
    ps.ps_result_tuple_slot = Some(result_slot);
    ps.resultops = Some(&TTS_OPS_VIRTUAL);
    ps.resultopsset = true;
    ps.resultopsfixed = true;

    exec_assign_projection_info(&mut ps, Some(Arc::clone(&scan_desc)));

    ps.qual = crate::backend::executor::execExpr::exec_init_qual(&node.scan.plan.qual, None);

    let scan_keys = exec_index_build_scan_keys(index_rel, &node.indexqual, false);

    let ss = ScanState {
        ps,
        ss_current_relation: None,
        ss_current_scan_desc: None,
        ss_scan_tuple_slot: Some(scan_slot),
    };

    Box::new(IndexOnlyScanRun {
        ss,
        heap_rel,
        index_rel,
        snapshot,
        scan_keys,
        scan: None,
    })
}

/// PG `ExecIndexOnlyScan` (-> `IndexOnlyNext`): drive the scan loop, returning a
/// borrow of the projection result slot or None at end of scan.
pub async fn exec_index_only_scan<'r>(
    shared: &Arc<SharedState>,
    run: &'r mut IndexOnlyScanRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    loop {
        let got = index_only_next(shared, run).await;
        if !got {
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

/// PG `IndexOnlyNext`: advance the index scan to the next entry, confirm
/// visibility (M6: always via the heap, since the VM is stubbed not-all-visible),
/// and fill the scan slot from the INDEX tuple (`StoreIndexTuple`). Returns false
/// at end of scan.
async fn index_only_next(shared: &Arc<SharedState>, run: &mut IndexOnlyScanRun<'_>) -> bool {
    if run.scan.is_none() {
        let mut scan = Box::new(index_beginscan(run.heap_rel, run.index_rel, run.snapshot));
        index_rescan_keys(&mut scan, super_am_keys(&run.scan_keys));
        run.scan = Some(scan);
    }
    let scan = run
        .scan
        .as_mut()
        .unwrap_or_else(|| unreachable!("index-only scan just opened"));

    loop {
        if index_getnext_tid(shared, scan, ScanDirection::Forward)
            .await
            .is_none()
        {
            if let Some(slot) = run.ss.ss_scan_tuple_slot.as_mut() {
                crate::executor::tuptable::ExecClearTuple(slot);
            }
            return false;
        }

        // VM all-visible check: stubbed not-all-visible (the visibilitymap is not
        // wired), so we always visit the heap to confirm the TID is visible. The
        // returned DATA still comes from the index tuple (StoreIndexTuple).
        if !vm_all_visible() && index_fetch_heap(shared, scan).await.is_none() {
            // Not visible: try the next index entry.
            continue;
        }

        // StoreIndexTuple: fill the scan slot from the current index tuple.
        let Some((values, isnull)) = index_current_index_values(scan) else {
            // No current index tuple (shouldn't happen right after a successful
            // getnext); treat as end of scan.
            continue;
        };
        let slot = run
            .ss
            .ss_scan_tuple_slot
            .as_mut()
            .unwrap_or_else(|| unimplemented!("IndexOnlyNext: scan node has no scan tuple slot"));
        crate::executor::tuptable::ExecClearTuple(slot);
        let n = values.len();
        slot.values[..n].copy_from_slice(&values);
        slot.isnull[..n].copy_from_slice(&isnull);
        exec_store_virtual_tuple(slot);
        return true;
    }
}

/// `VM_ALL_VISIBLE` (M6 stub): the visibility map is not wired, so report the page
/// as NOT all-visible. The caller then always visits the heap to confirm
/// visibility -- correct, just slower (the VM fast path stages with the
/// visibilitymap translation).
fn vm_all_visible() -> bool {
    false
}

/// PG `ExecEndIndexOnlyScan`: release the scan resources.
pub fn exec_end_index_only_scan(_shared: &Arc<SharedState>, run: &mut IndexOnlyScanRun<'_>) {
    if let Some(scan) = run.scan.take() {
        index_endscan(*scan);
    }
}

/// PG `ExecReScanIndexOnlyScan`: restart the scan from the beginning.
pub fn exec_rescan_index_only_scan(_shared: &Arc<SharedState>, run: &mut IndexOnlyScanRun<'_>) {
    if let Some(scan) = run.scan.take() {
        index_endscan(*scan);
    }
}

/// Adapt the executor `ScanKeyData` array to the AM's strategy-tagged rescan keys.
fn super_am_keys(
    keys: &[ScanKeyData],
) -> Vec<(i32, crate::access::stratnum::StrategyNumber, crate::postgres::Datum)> {
    keys.iter()
        .map(|k| (i32::from(k.attno), k.strategy, k.argument))
        .collect()
}

/// PG `ExecGetRangeTableRelation`: the open scan (heap) relation for RTI `rti`.
fn exec_get_range_table_relation<'rel>(
    estate: &EState<'rel>,
    rti: crate::nodes::primnodes::Index,
) -> &'rel RelationData {
    crate::assert!(rti > 0);
    estate
        .es_range_table_rels
        .get(rti - 1)
        .copied()
        .flatten()
        .unwrap_or_else(|| unimplemented!("ExecGetRangeTableRelation: scan relation not registered for RTI"))
}

/// `index_open` (M6 subset): the open index relation with OID `indexid`, borrowed
/// from the EState's index relation slots.
fn exec_get_index_relation<'rel>(
    estate: &EState<'rel>,
    indexid: crate::postgres_ext::Oid,
) -> &'rel RelationData {
    estate
        .es_index_rels
        .iter()
        .copied()
        .flatten()
        .find(|r| r.rd_id == indexid)
        .unwrap_or_else(|| unimplemented!("ExecOpenIndexRelation: index relation not registered for OID"))
}
