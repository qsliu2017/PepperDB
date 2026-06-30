//! Index-scan node executor. Translated from
//! backend/executor/nodeIndexscan.c (disposition: full for the M6 forward,
//! single-column btree index scan + heap fetch + qual + projection).
//!
//! `ExecInitIndexScan` opens the scan (heap) relation + the index relation, builds
//! the scan tuple slot + projection, and builds the index scan keys from the
//! `IndexScan` plan node's `indexqual` (`ExecIndexBuildScanKeys`). `ExecIndexScan`
//! drives `IndexNext`: open the index scan (`index_beginscan`) on the first call,
//! pass the scan keys (`index_rescan`), then loop `index_getnext_tid` +
//! `index_fetch_heap`, deform the heap tuple into the scan slot, recheck the index
//! quals if the AM reported a lossy match (btree never is, so a stub no-op here),
//! evaluate the plan qual, and project (`ExecScan` tail). `ExecEndIndexScan`
//! releases the scan; `ExecReScanIndexScan` restarts it.
//!
//! GROW/STAGE (rules.md s4): ORDER BY-via-index (mark/restore, the reorder queue),
//! multi-key / array / runtime scan keys beyond a leading const comparison,
//! parallel index scan, and the SnapshotAny/dirty special cases are clean
//! `not_yet_reachable` arms.
//!
//! Slot ownership mirrors nodeSeqscan: the node OWNS its scan tuple slot; each
//! fetched heap tuple is deformed into it, `exec_scan` aliases it into
//! `ecxt_scantuple`, runs the qual + projection, and returns a borrow of the
//! projection result slot. No per-tuple deep clone on the hot path.
//!
//! Async coloring (rules.md s5): the index descent + heap fetch reach buffer
//! reads, so `ExecIndexScan` is `async`. No content lock is held across `.await`
//! (the btree scan copies each leaf page out; the heap fetch copies the tuple out).

use std::sync::Arc;

use crate::access::sdir::ScanDirection;
use crate::access::skey::{ScanKeyData, ScanKeyFlags};
use crate::access::stratnum::{
    StrategyNumber, BT_EQUAL_STRATEGY_NUMBER, BT_GREATER_EQUAL_STRATEGY_NUMBER,
    BT_GREATER_STRATEGY_NUMBER, BT_LESS_EQUAL_STRATEGY_NUMBER, BT_LESS_STRATEGY_NUMBER,
};
use crate::backend::access::common::heaptuple::heap_deform_tuple;
use crate::backend::access::index::indexam::{
    index_beginscan, index_endscan, index_fetch_heap, index_getnext_tid, index_rescan_keys,
    IndexScanState as AmIndexScanState,
};
use crate::backend::executor::execScan::exec_scan;
use crate::backend::executor::execTuples::{
    exec_type_from_tl, make_tuple_table_slot, TTS_OPS_VIRTUAL,
};
use crate::backend::executor::execUtils::{create_expr_context, exec_assign_projection_info};
use crate::nodes::execnodes::{EState, PlanState, ScanState, TupleTableSlot};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::IndexScan;
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;

/// Run-state for an index-scan node. Like `SeqScanRun`, this pairs the PG
/// `IndexScanState` (here folded into the embedded [`ScanState`]) with the open
/// index-scan descriptor + the borrowed relations the C node holds by pointer.
///
/// Borrow-based ownership (relation-ownership-plan): the heap relation, the index
/// relation, and the query snapshot are BORROWED from the command frame via the
/// `EState` (the `'rel` root strictly encloses the executor run); the AM scan
/// descriptor borrows them for the scan's life, so it rides every `.await` soundly.
pub struct IndexScanRun<'rel> {
    pub ss: ScanState,
    /// the scan (heap) relation borrowed from the EState range-table.
    pub heap_rel: &'rel RelationData,
    /// the index relation borrowed from the EState index slots.
    pub index_rel: &'rel RelationData,
    /// the query snapshot borrowed from the EState.
    pub snapshot: &'rel crate::utils::snapshot::SnapshotData,
    /// the index scan keys built from `indexqual` (PG `iss_ScanKeys`).
    pub scan_keys: Vec<ScanKeyData>,
    /// `indexqualorig` compiled, run only on a lossy (recheck) match (PG never sets
    /// recheck for btree, so this is unused on the M6 path).
    pub indexqualorig: Option<Box<crate::nodes::execnodes::ExprState>>,
    /// the open AM scan descriptor, created on the first `ExecIndexScan`.
    pub scan: Option<Box<AmIndexScanState<'rel, 'rel, 'rel>>>,
}

/// PG `ExecInitIndexScan`: build the IndexScanState. Opens the scan + index
/// relations (from the EState slots), builds the scan tuple slot from the heap
/// rowtype, the result slot + projection, the per-node exprcontext, the plan qual,
/// and the index scan keys from `indexqual`. The AM scan descriptor opens lazily.
pub fn exec_init_index_scan<'rel>(
    node: &IndexScan,
    estate: &mut EState<'rel>,
    eflags: i32,
) -> Box<IndexScanRun<'rel>> {
    let _ = eflags;
    crate::assert!(
        node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none(),
        "ExecInitIndexScan: a scan node is childless"
    );

    let heap_rel = exec_get_range_table_relation(estate, node.scan.scanrelid);
    let index_rel = exec_get_index_relation(estate, node.indexid);
    let snapshot = estate
        .es_snapshot_ref
        .unwrap_or_else(|| unimplemented!("ExecInitIndexScan: no active snapshot for the scan"));

    // The scan slot's descriptor IS the heap rowtype (the heap fetch yields tuples
    // of that shape, deformed into a virtual slot).
    let scan_desc = relation_tupdesc(heap_rel);
    let scan_slot = make_tuple_table_slot(Some(Arc::clone(&scan_desc)), &TTS_OPS_VIRTUAL);

    let mut ps = PlanState {
        plan: Some(Node::IndexScan(Box::new(node.clone()))),
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

    // ExecInitQual: the plan qual (the WHERE clause not pushed into the index).
    ps.qual = crate::backend::executor::execExpr::exec_init_qual(&node.scan.plan.qual, None);
    // indexqualorig: only run on a lossy recheck (btree is exact, so unused on M6).
    let indexqualorig = crate::backend::executor::execExpr::exec_init_qual(&node.indexqualorig, None);

    // Build the index scan keys from the indexqual.
    let scan_keys = exec_index_build_scan_keys(index_rel, &node.indexqual, false);

    let ss = ScanState {
        ps,
        ss_current_relation: None,
        ss_current_scan_desc: None,
        ss_scan_tuple_slot: Some(scan_slot),
    };

    Box::new(IndexScanRun {
        ss,
        heap_rel,
        index_rel,
        snapshot,
        scan_keys,
        indexqualorig,
        scan: None,
    })
}

/// PG `ExecIndexScan` (-> `IndexNext`): drive the index scan loop. Open the AM scan
/// on the first call and pass the scan keys, then loop fetching the next heap tuple
/// via the index, deform it into the scan slot, recheck (lossy only), evaluate the
/// plan qual, and project. Returns a borrow of the projection result slot, or None
/// at end of scan.
pub async fn exec_index_scan<'r>(
    shared: &Arc<SharedState>,
    run: &'r mut IndexScanRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    loop {
        let got = index_next(shared, run).await;
        if !got {
            return None;
        }
        // ExecScan tail: qual-check the scan slot; if it passes, project and stop.
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

/// PG `IndexNext`: fetch the next visible heap tuple via the index and deform it
/// into the scan slot. Returns false at end of scan. (The recheck of `indexqualorig`
/// for a lossy AM is a no-op for btree, which always reports exact matches.)
async fn index_next(shared: &Arc<SharedState>, run: &mut IndexScanRun<'_>) -> bool {
    if run.scan.is_none() {
        let mut scan = Box::new(index_beginscan(run.heap_rel, run.index_rel, run.snapshot));
        // index_rescan: pass the scan keys to the AM (PG passes ScanKeys here).
        index_rescan_keys(&mut scan, am_keys(&run.scan_keys));
        run.scan = Some(scan);
    }
    let scan = run
        .scan
        .as_mut()
        .unwrap_or_else(|| unreachable!("index scan just opened"));

    loop {
        if index_getnext_tid(shared, scan, ScanDirection::Forward)
            .await
            .is_none()
        {
            // End of scan: empty the scan slot.
            if let Some(slot) = run.ss.ss_scan_tuple_slot.as_mut() {
                crate::executor::tuptable::ExecClearTuple(slot);
            }
            return false;
        }
        let Some(tuple) = index_fetch_heap(shared, scan).await else {
            // TID not visible: try the next index entry.
            continue;
        };

        // Deform the heap tuple into the scan slot's value/null arrays.
        let desc = scan_slot_desc(&run.ss);
        // SAFETY: `tuple` is an owned heap tuple copy whose body outlives the
        // deform; deform reads the header + data bytes.
        let (values, isnull) = unsafe { heap_deform_tuple(&tuple, &desc) };

        let slot = run
            .ss
            .ss_scan_tuple_slot
            .as_mut()
            .unwrap_or_else(|| unimplemented!("IndexNext: scan node has no scan tuple slot"));
        crate::executor::tuptable::ExecClearTuple(slot);
        let n = values.len();
        slot.values[..n].copy_from_slice(&values);
        slot.isnull[..n].copy_from_slice(&isnull);
        crate::backend::executor::execTuples::exec_store_virtual_tuple(slot);
        return true;
    }
}

/// PG `ExecEndIndexScan`: release the scan resources. Closes the AM index scan; the
/// relations are owned by the caller (borrowed via EState). Slots drop with state.
pub fn exec_end_index_scan(_shared: &Arc<SharedState>, run: &mut IndexScanRun<'_>) {
    if let Some(scan) = run.scan.take() {
        index_endscan(*scan);
    }
}

/// PG `ExecReScanIndexScan`: restart the scan from the beginning. Drop the open
/// descriptor so the next `ExecIndexScan` re-opens it and re-passes the keys.
pub fn exec_rescan_index_scan(_shared: &Arc<SharedState>, run: &mut IndexScanRun<'_>) {
    if let Some(scan) = run.scan.take() {
        index_endscan(*scan);
    }
}

/// PG `ExecIndexBuildScanKeys` (M6 form, shared with the index-only scan): convert
/// each `OpExpr` in `quals` (`indexkey op const`) into a single [`ScanKeyData`].
/// The left operand is the index-key `Var` (`varno == INDEX_VAR`, `varattno` =
/// index column); the right operand is the comparison `Const`. The operator's btree
/// strategy number is derived from its OID, and the comparison support fn
/// (`BTORDER_PROC`) is resolved from the index opclass via `index_getprocinfo`.
///
/// GROW (rules.md s4): runtime keys (non-const RHS), array keys (`= ANY`), and
/// `RowCompareExpr`/`ScalarArrayOpExpr`/`NullTest` clauses are deferred; an
/// unsupported clause hits a `not_yet_reachable` arm. `isorderby` ORDER BY keys
/// are not yet built (`ORDER BY` via index stages).
#[must_use]
pub fn exec_index_build_scan_keys(
    index: &RelationData,
    quals: &[Node],
    isorderby: bool,
) -> Vec<ScanKeyData> {
    crate::assert!(!isorderby, "ExecIndexBuildScanKeys: ORDER BY-via-index keys stage at a later milestone");
    let indnkeyatts = i32::from(index.index_number_of_key_attributes());

    quals
        .iter()
        .map(|clause| build_one_scan_key(index, clause, indnkeyatts))
        .collect()
}

/// Convert one `indexkey op const` `OpExpr` into a [`ScanKeyData`].
fn build_one_scan_key(index: &RelationData, clause: &Node, indnkeyatts: i32) -> ScanKeyData {
    let Node::OpExpr(op) = clause else {
        unimplemented!("ExecIndexBuildScanKeys: only OpExpr indexquals are supported (M6)")
    };

    // leftop should be the index-key Var (possibly relabeled).
    let leftop = strip_relabel(op.args.first());
    let Some(Node::Var(var)) = leftop else {
        crate::elog!(crate::utils::elog::ERROR, "indexqual doesn't have key on left side");
        unreachable!("elog ERROR unwinds");
    };
    crate::assert!(
        var.varno == crate::nodes::primnodes::INDEX_VAR,
        "indexqual left Var is not an INDEX_VAR"
    );
    let varattno = i32::from(var.varattno);
    if varattno < 1 || varattno > indnkeyatts {
        crate::elog!(crate::utils::elog::ERROR, "bogus index qualification");
    }

    // The operator's btree strategy number (cross-checks the op matches the index).
    let strategy = op_btree_strategy(op.opno);

    // rightop is the comparison constant (M6: const only; runtime keys grow).
    let rightop = strip_relabel(op.args.get(1));
    let (argument, isnull) = match rightop {
        Some(Node::Const(c)) => (c.constvalue, c.constisnull),
        _ => unimplemented!("ExecIndexBuildScanKeys: only a constant RHS is supported (M6)"),
    };

    // Resolve the comparison support fn (BTORDER_PROC) from the index opclass.
    let func = crate::backend::access::index::indexam::index_getprocinfo(
        index,
        varattno,
        crate::access::nbtree::BTORDER_PROC,
    );

    let mut flags = 0i32;
    if isnull {
        flags |= ScanKeyFlags::ISNULL.bits();
    }

    ScanKeyData {
        flags,
        attno: varattno as i16,
        strategy,
        subtype: crate::postgres_ext::InvalidOid,
        collation: op.inputcollid,
        func,
        argument,
    }
}

/// Strip a single `RelabelType` wrapper (a no-op coercion), returning the inner
/// node (PG: `leftop = ((RelabelType *) leftop)->arg`).
fn strip_relabel(node: Option<&Node>) -> Option<&Node> {
    match node {
        Some(Node::RelabelType(r)) => r.arg.as_ref(),
        other => other,
    }
}

/// Map a btree comparison operator OID to its strategy number (PG
/// `get_op_opfamily_properties` against the index opfamily). M6: the int4 btree
/// operators; other operators stage with the planner's index-path selection.
fn op_btree_strategy(opno: crate::postgres_ext::Oid) -> StrategyNumber {
    match opno.get() {
        96 => BT_EQUAL_STRATEGY_NUMBER,         // int4 =
        97 => BT_LESS_STRATEGY_NUMBER,          // int4 <
        521 => BT_GREATER_STRATEGY_NUMBER,      // int4 >
        523 => BT_LESS_EQUAL_STRATEGY_NUMBER,   // int4 <=
        525 => BT_GREATER_EQUAL_STRATEGY_NUMBER, // int4 >=
        other => unimplemented!("op_btree_strategy: operator {other} not a known int4 btree op (M6)"),
    }
}

/// Adapt the executor `ScanKeyData` array to the AM's strategy-tagged rescan keys
/// `(attno, strategy, argument)`.
fn am_keys(keys: &[ScanKeyData]) -> Vec<(i32, StrategyNumber, crate::postgres::Datum)> {
    keys.iter()
        .map(|k| (i32::from(k.attno), k.strategy, k.argument))
        .collect()
}

/// PG `ExecGetRangeTableRelation`: the open scan (heap) relation for RTI `rti`,
/// borrowed from the EState range-table (shared with nodeSeqscan's helper).
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
/// from the EState's index relation slots (the command frame opened + published
/// them, mirroring the range-table relations). The heavyweight-lock `index_open`
/// path grows with the relcache lock-manager wiring.
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

/// The rowtype descriptor of a relation (`RelationGetDescr`).
fn relation_tupdesc(relation: &RelationData) -> crate::access::tupdesc::TupleDesc {
    relation
        .rd_att
        .clone()
        .unwrap_or_else(|| unimplemented!("relation_tupdesc: relation has no rowtype descriptor"))
}

/// The scan slot's descriptor (clone of the scan tuple desc).
fn scan_slot_desc(ss: &ScanState) -> crate::access::tupdesc::TupleDesc {
    ss.ps
        .scandesc
        .clone()
        .unwrap_or_else(|| unimplemented!("scan_slot_desc: no scan descriptor"))
}

#[cfg(test)]
#[path = "nodeIndexscan_tests.rs"]
mod tests;
