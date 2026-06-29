//! Bitmap index-scan node. Translated from
//! backend/executor/nodeBitmapIndexscan.c (disposition: full for the M6 serial
//! single-column btree bitmap path).
//!
//! Unlike a plain index scan, this node does not return tuples one-by-one: it runs
//! the index scan to completion and collects every matching heap TID into a
//! `TIDBitmap` (`index_getbitmap`/`amgetbitmap`), which it hands back to its parent
//! (a BitmapHeapScan, BitmapAnd, or BitmapOr). PG marks `ExecProcNode` as an error
//! stub for this node and routes it through `MultiExecProcNode` instead; we do the
//! same via the `multi_exec_proc_node` dispatch (a separate return path yielding a
//! `Box<TIDBitmap>` rather than a slot).
//!
//! PG lets the parent pre-stash a result bitmap into `biss_result` so a BitmapOr
//! child ORs directly into the shared bitmap (saving an explicit union step). We
//! reproduce that: `MultiExecBitmapIndexScan` accepts an optional pre-made bitmap.
//!
//! GROW/STAGE (rules.md s4): runtime keys, array keys (`= ANY`), and the parallel
//! (shared-DSA) bitmap are deferred -- the M6 path builds the scan keys from
//! constant indexquals at init and passes them to the AM once.

use std::sync::Arc;

use crate::access::skey::ScanKeyData;
use crate::backend::access::index::indexam::{
    index_beginscan, index_endscan, index_getbitmap, index_rescan_keys,
    IndexScanState as AmIndexScanState,
};
use crate::backend::executor::nodeIndexscan::exec_index_build_scan_keys;
use crate::backend::nodes::tidbitmap::{tbm_create, TIDBitmap};
use crate::nodes::execnodes::{EState, PlanState, ScanState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::BitmapIndexScan;
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;

/// Run-state for a bitmap index-scan node. Like `IndexScanRun` it pairs the PG
/// `BitmapIndexScanState` (folded into [`ScanState`]) with the borrowed relations +
/// scan keys; no scan/result tuple slot is needed (this node yields a bitmap).
pub struct BitmapIndexScanRun<'rel> {
    pub ss: ScanState,
    pub heap_rel: &'rel RelationData,
    pub index_rel: &'rel RelationData,
    pub snapshot: &'rel crate::utils::snapshot::SnapshotData,
    /// The index scan keys built from `indexqual` (PG `biss_ScanKeys`).
    pub scan_keys: Vec<ScanKeyData>,
    /// The open AM scan descriptor, created on the first MultiExec.
    pub scan: Option<Box<AmIndexScanState<'rel, 'rel, 'rel>>>,
}

/// PG `ExecInitBitmapIndexScan`: build the state. We do NOT open/lock the base
/// relation here -- an ancestor BitmapHeapScan holds the lock and the relations are
/// borrowed from the EState (as for the plain index scan). Builds the scan keys
/// from `indexqual`; the AM descriptor opens lazily on the first MultiExec.
pub fn exec_init_bitmap_index_scan<'rel>(
    node: &BitmapIndexScan,
    estate: &mut EState<'rel>,
    eflags: i32,
) -> Box<BitmapIndexScanRun<'rel>> {
    let _ = eflags;
    let heap_rel = exec_get_range_table_relation(estate, node.scan.scanrelid);
    let index_rel = exec_get_index_relation(estate, node.indexid);
    let snapshot = estate.es_snapshot_ref.unwrap_or_else(|| {
        unimplemented!("ExecInitBitmapIndexScan: no active snapshot for the scan")
    });

    let scan_keys = exec_index_build_scan_keys(index_rel, &node.indexqual, false);

    let ps = PlanState {
        plan: Some(Node::BitmapIndexScan(Box::new(node.clone()))),
        ..PlanState::default()
    };
    let ss = ScanState {
        ps,
        ss_current_relation: None,
        ss_current_scan_desc: None,
        ss_scan_tuple_slot: None,
    };

    Box::new(BitmapIndexScanRun {
        ss,
        heap_rel,
        index_rel,
        snapshot,
        scan_keys,
        scan: None,
    })
}

/// PG `MultiExecBitmapIndexScan`: run the index scan to completion, collecting all
/// matching heap TIDs into a `TIDBitmap`. If `result` is `Some`, OR the TIDs into
/// it (the parent pre-made it, e.g. BitmapOr); otherwise create a fresh bitmap.
pub async fn multi_exec_bitmap_index_scan(
    shared: &Arc<SharedState>,
    run: &mut BitmapIndexScanRun<'_>,
    result: Option<Box<TIDBitmap>>,
) -> Box<TIDBitmap> {
    if run.scan.is_none() {
        let mut scan = Box::new(index_beginscan(run.heap_rel, run.index_rel, run.snapshot));
        index_rescan_keys(&mut scan, am_keys(&run.scan_keys));
        run.scan = Some(scan);
    }
    let scan = run
        .scan
        .as_mut()
        .unwrap_or_else(|| unreachable!("bitmap index scan just opened"));

    // work_mem is GUC-driven in PG; M6 uses a fixed budget (4 MB) until the GUC
    // wiring lands.
    let mut tbm = result.unwrap_or_else(|| tbm_create(4 * 1024 * 1024));
    index_getbitmap(shared, scan, &mut tbm).await;
    tbm
}

/// PG `ExecEndBitmapIndexScan`: release the AM scan (the relations are borrowed).
pub fn exec_end_bitmap_index_scan(_shared: &Arc<SharedState>, run: &mut BitmapIndexScanRun<'_>) {
    if let Some(scan) = run.scan.take() {
        index_endscan(*scan);
    }
}

/// PG `ExecReScanBitmapIndexScan`: drop the open descriptor so the next MultiExec
/// re-opens it and re-passes the keys (M6 has no runtime keys to recompute).
pub fn exec_rescan_bitmap_index_scan(_shared: &Arc<SharedState>, run: &mut BitmapIndexScanRun<'_>) {
    if let Some(scan) = run.scan.take() {
        index_endscan(*scan);
    }
}

fn am_keys(keys: &[ScanKeyData]) -> Vec<(i32, crate::access::stratnum::StrategyNumber, crate::postgres::Datum)> {
    keys.iter()
        .map(|k| (i32::from(k.attno), k.strategy, k.argument))
        .collect()
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

/// `index_open` (M6 subset): the open index relation with OID `indexid`, borrowed
/// from the EState index slots.
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
        .unwrap_or_else(|| {
            unimplemented!("ExecOpenIndexRelation: index relation not registered for OID")
        })
}
