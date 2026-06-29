//! Bitmap heap-scan node. Translated from
//! backend/executor/nodeBitmapHeapscan.c (disposition: full for the M6 serial
//! single-process path).
//!
//! The bitmap heap scan is the parent of a bitmap producer (a BitmapIndexScan,
//! BitmapAnd, or BitmapOr). On the first call it `MultiExecProcNode`s its child to
//! get a `TIDBitmap`, begins a (private) iteration, and then walks the bitmap page
//! by page: for an EXACT page it fetches the recorded tuple offsets; for a LOSSY
//! page it must examine every tuple on the page (the bitmap only recorded that the
//! page must be visited). Each candidate heap tuple is fetched by TID (applying the
//! MVCC visibility test), deformed into the scan slot, and -- when the page or the
//! AM demanded a recheck -- re-evaluated against `bitmapqualorig`; survivors run the
//! node's plan qual and projection (`ExecScan` tail).
//!
//! GROW/STAGE (rules.md s4): the parallel (shared-DSA) bitmap, prefetch
//! (`prefetch_iterator`), and the lazy-tbm cost machinery are deferred. The serial
//! exact + lossy-recheck path is COMPLETE.
//!
//! Async coloring (rules.md s5): the child MultiExec + the per-tuple heap fetch
//! reach buffer reads, so `ExecBitmapHeapScan` is `async`. No content lock is held
//! across an `.await` (the heap fetch copies each tuple out, like the index scan).

use std::sync::Arc;

use crate::backend::access::heap::heapam::{heap_block_max_offset, heap_fetch_tid};
use crate::backend::access::common::heaptuple::heap_deform_tuple;
use crate::backend::executor::execProcnode::{exec_end_node, multi_exec_proc_node, PlanStateNode};
use crate::backend::executor::execScan::exec_scan;
use crate::backend::executor::execTuples::{
    exec_store_virtual_tuple, exec_type_from_tl, make_tuple_table_slot, TTS_OPS_VIRTUAL,
};
use crate::backend::executor::execUtils::{create_expr_context, exec_assign_projection_info};
use crate::backend::nodes::tidbitmap::{
    tbm_begin_iterate, tbm_end_iterate, tbm_free, tbm_iterate, TBMIterateResult, TBMIterator,
    TIDBitmap,
};
use crate::nodes::execnodes::{EState, ExprState, PlanState, ScanState, TupleTableSlot};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::BitmapHeapScan;
use crate::shared_state::SharedState;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::utils::rel::RelationData;

/// Run-state for a bitmap heap-scan node. Pairs the PG `BitmapHeapScanState`
/// (folded into [`ScanState`]) with the borrowed heap relation + snapshot, the
/// `bitmapqualorig` recheck expression, the initialized bitmap-producer child, and
/// the iteration cursor (the materialized `TIDBitmap` + its iterator + the current
/// page being drained).
pub struct BitmapHeapScanRun<'rel> {
    pub ss: ScanState,
    pub heap_rel: &'rel RelationData,
    pub snapshot: &'rel crate::utils::snapshot::SnapshotData,
    /// `bitmapqualorig` compiled: the recheck qual, run for lossy/recheck pages.
    pub bitmapqualorig: Option<Box<ExprState>>,
    /// The bitmap-producer child (BitmapIndexScan / BitmapAnd / BitmapOr).
    pub child: PlanStateNode<'rel>,
    /// The materialized bitmap, owned after the first MultiExec of the child.
    pub tbm: Option<Box<TIDBitmap>>,
    /// The private iterator over `tbm` (the bitmap is logically read-only while it
    /// exists; we hold the box separately so we can free it at end).
    pub iterator: Option<TBMIterator>,
    /// The page currently being drained + the offsets still to visit on it.
    pub cur_page: Option<CurrentPage>,
}

/// The per-page cursor: the block being drained, the (1-based) tuple offsets left
/// to fetch on it, and whether tuples from this page require a recheck.
pub struct CurrentPage {
    block: crate::storage::block::BlockNumber,
    offsets: std::collections::VecDeque<OffsetNumber>,
    recheck: bool,
}

/// PG `ExecInitBitmapHeapScan`: build the state. Like the index scan we do NOT
/// open/lock the base relation here -- the relations are borrowed from the EState
/// (the command frame holds the locks). Builds the scan tuple slot from the heap
/// rowtype, the projection, the per-node exprcontext, the plan qual, and the
/// `bitmapqualorig` recheck qual; then inits the bitmap-producer child.
pub fn exec_init_bitmap_heap_scan<'rel>(
    node: &BitmapHeapScan,
    estate: &mut EState<'rel>,
    eflags: i32,
    child: PlanStateNode<'rel>,
) -> Box<BitmapHeapScanRun<'rel>> {
    let _ = eflags;
    let heap_rel = exec_get_range_table_relation(estate, node.scan.scanrelid);
    let snapshot = estate
        .es_snapshot_ref
        .unwrap_or_else(|| unimplemented!("ExecInitBitmapHeapScan: no active snapshot for the scan"));

    let scan_desc = relation_tupdesc(heap_rel);
    let scan_slot = make_tuple_table_slot(Some(Arc::clone(&scan_desc)), &TTS_OPS_VIRTUAL);

    let mut ps = PlanState {
        plan: Some(Node::BitmapHeapScan(Box::new(node.clone()))),
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
    let bitmapqualorig =
        crate::backend::executor::execExpr::exec_init_qual(&node.bitmapqualorig, None);

    let ss = ScanState {
        ps,
        ss_current_relation: None,
        ss_current_scan_desc: None,
        ss_scan_tuple_slot: Some(scan_slot),
    };

    Box::new(BitmapHeapScanRun {
        ss,
        heap_rel,
        snapshot,
        bitmapqualorig,
        child,
        tbm: None,
        iterator: None,
        cur_page: None,
    })
}

/// PG `ExecBitmapHeapScan` (-> `BitmapHeapNext`): on the first call run the child to
/// get the bitmap and begin iterating; then fetch the next visible heap tuple the
/// bitmap points at, recheck the qual on lossy pages, and project. Returns a borrow
/// of the projection result slot, or `None` at end of scan.
pub async fn exec_bitmap_heap_scan<'r>(
    shared: &Arc<SharedState>,
    run: &'r mut BitmapHeapScanRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    if run.iterator.is_none() {
        bitmap_table_scan_setup(shared, run).await;
    }

    loop {
        let Some((tid, recheck)) = bitmap_next_tid(shared, run).await else {
            if let Some(slot) = run.ss.ss_scan_tuple_slot.as_mut() {
                crate::executor::tuptable::ExecClearTuple(slot);
            }
            return None;
        };

        // Fetch + deform the candidate into the scan slot; an invisible tuple (or
        // an absent line pointer on a lossy page) is skipped.
        if !fetch_into_scan_slot(shared, run, &tid).await {
            continue;
        }

        // Lossy / AM-rechecked pages re-evaluate bitmapqualorig against the row.
        if recheck && !recheck_passes(run) {
            if let Some(slot) = run.ss.ss_scan_tuple_slot.as_mut() {
                crate::executor::tuptable::ExecClearTuple(slot);
            }
            continue;
        }

        // ExecScan tail: the plan qual + projection.
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

/// PG `BitmapTableScanSetup`: run the bitmap-producer child to completion, take its
/// `TIDBitmap`, and begin a (private) iteration.
async fn bitmap_table_scan_setup(shared: &Arc<SharedState>, run: &mut BitmapHeapScanRun<'_>) {
    let mut tbm = multi_exec_proc_node(shared, &mut run.child, None).await;
    let iterator = tbm_begin_iterate(&mut tbm);
    run.tbm = Some(tbm);
    run.iterator = Some(iterator);
}

/// The next candidate `(TID, recheck)` from the bitmap, draining the current page
/// before advancing to the next. Returns `None` when the bitmap is exhausted.
async fn bitmap_next_tid(
    shared: &Arc<SharedState>,
    run: &mut BitmapHeapScanRun<'_>,
) -> Option<(ItemPointerData, bool)> {
    loop {
        if let Some(page) = run.cur_page.as_mut() {
            if let Some(off) = page.offsets.pop_front() {
                let mut tid = ItemPointerData {
                    blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
                    posid: 0,
                };
                tid.set(page.block, off);
                return Some((tid, page.recheck));
            }
            run.cur_page = None;
        }

        let iterator = run.iterator.as_mut()?;
        let result = tbm_iterate(iterator)?;
        run.cur_page = Some(page_cursor(shared, run.heap_rel, &result).await);
    }
}

/// Build the per-page cursor for one `TBMIterateResult`: an exact page lists its
/// recorded offsets; a lossy page must visit every line pointer on the page.
async fn page_cursor(
    shared: &Arc<SharedState>,
    heap_rel: &RelationData,
    result: &TBMIterateResult,
) -> CurrentPage {
    let offsets: std::collections::VecDeque<OffsetNumber> = if result.lossy {
        let max = heap_block_max_offset(shared, heap_rel, result.blockno).await;
        (1..=max).collect()
    } else {
        result.offsets.iter().copied().collect()
    };
    CurrentPage {
        block: result.blockno,
        offsets,
        recheck: result.recheck,
    }
}

/// Fetch the heap tuple at `tid` (MVCC-tested) and deform it into the scan slot.
/// Returns false if the tuple is absent/invisible (the caller skips it).
async fn fetch_into_scan_slot(
    shared: &Arc<SharedState>,
    run: &mut BitmapHeapScanRun<'_>,
    tid: &ItemPointerData,
) -> bool {
    let Some(tuple) = heap_fetch_tid(shared, run.heap_rel, tid, run.snapshot).await else {
        return false;
    };

    let desc = scan_slot_desc(&run.ss);
    // SAFETY: `tuple` is an owned heap tuple copy whose body outlives the deform.
    let (values, isnull) = unsafe { heap_deform_tuple(&tuple, &desc) };

    let slot = run
        .ss
        .ss_scan_tuple_slot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("BitmapHeapNext: scan node has no scan tuple slot"));
    crate::executor::tuptable::ExecClearTuple(slot);
    let n = values.len();
    slot.values[..n].copy_from_slice(&values);
    slot.isnull[..n].copy_from_slice(&isnull);
    exec_store_virtual_tuple(slot);
    true
}

/// PG `BitmapHeapRecheck`/`ExecQualAndReset`: re-evaluate `bitmapqualorig` against
/// the scan slot already loaded into the slot. No recheck qual means always-pass.
fn recheck_passes(run: &mut BitmapHeapScanRun<'_>) -> bool {
    let Some(_) = run.bitmapqualorig.as_ref() else {
        return true;
    };
    let scan_slot = run
        .ss
        .ss_scan_tuple_slot
        .take()
        .unwrap_or_else(|| unimplemented!("BitmapHeapRecheck: no scan tuple slot"));
    let mut econtext = run
        .ss
        .ps
        .ps_expr_context
        .take()
        .unwrap_or_else(|| unimplemented!("BitmapHeapRecheck: no exprcontext"));
    econtext.ecxt_scantuple = Some(scan_slot);

    let passes =
        crate::backend::executor::execExpr::exec_qual(run.bitmapqualorig.as_deref_mut(), &mut econtext);

    run.ss.ss_scan_tuple_slot = econtext.ecxt_scantuple.take();
    run.ss.ps.ps_expr_context = Some(econtext);
    passes
}

/// PG `ExecEndBitmapHeapScan`: free the bitmap + iterator, then tear down the child.
pub fn exec_end_bitmap_heap_scan(shared: Option<&Arc<SharedState>>, run: &mut BitmapHeapScanRun<'_>) {
    if let Some(mut it) = run.iterator.take() {
        tbm_end_iterate(&mut it);
    }
    if let Some(tbm) = run.tbm.take() {
        tbm_free(tbm);
    }
    run.cur_page = None;
    exec_end_node(shared, &mut run.child);
}

/// PG `ExecReScanBitmapHeapScan`: drop the bitmap so the next call rebuilds it, and
/// rescan the child.
pub fn exec_rescan_bitmap_heap_scan(shared: &Arc<SharedState>, run: &mut BitmapHeapScanRun<'_>) {
    if let Some(mut it) = run.iterator.take() {
        tbm_end_iterate(&mut it);
    }
    if let Some(tbm) = run.tbm.take() {
        tbm_free(tbm);
    }
    run.cur_page = None;
    crate::backend::executor::execAmi::exec_rescan(shared, &mut run.child);
}

/// PG `ExecGetRangeTableRelation`: the heap relation for RTI `rti`, borrowed from
/// the EState range-table.
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
        .unwrap_or_else(|| {
            unimplemented!("ExecGetRangeTableRelation: scan relation not registered for RTI")
        })
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
#[path = "nodeBitmapHeapscan_tests.rs"]
mod tests;
