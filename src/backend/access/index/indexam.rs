//! Generic index access manager. Translated from the M2-reachable parts of
//! `src/backend/access/index/indexam.c`.
//!
//! M2 scope (step 13-rest): `index_getprocinfo` (resolve a cached support proc's
//! `FmgrInfo` from the relcache `rd_support`/`rd_supportinfo` arrays via fmgr) and
//! the async index-scan API the executor + systable path drive over the btree AM:
//! `index_open`/`index_close`, `index_beginscan`/`index_rescan`/`index_endscan`,
//! `index_getnext_tid`, `index_fetch_heap`, `index_getnext_slot`, `index_insert`.
//!
//! Async coloring (rules.md s5): the scan descends the btree (buffer reads) and
//! the heap fetch reads a heap buffer, so the getnext/fetch entry points are
//! `async`. The C `IndexScanDesc = *mut IndexScanDescData` raw handle is replaced
//! on this path by an owned [`IndexScanState`] box; the descent never holds a
//! content lock across an `.await` (the btree scan copies each leaf page out).

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: the index scan holds per-backend raw Relation handles (task-confined for the scan's lifetime); the futures never migrate the pointee between tasks. await_holding_lock is clean (enforced)."
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "index-access routines take raw Relation/FmgrInfo pointers per the C API; the deref is faithful to C (callers pass live handles)"
)]

use std::sync::Arc;

use crate::access::sdir::ScanDirection;
use crate::backend::access::nbtree::nbtsearch::{BtScan, bt_first, bt_next};
use crate::backend::utils::fmgr::fmgr::fmgr_info;
use crate::fmgr::FmgrInfo;
use crate::shared_state::SharedState;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::relcache::Relation;
use crate::utils::snapshot::Snapshot;

/// `index_getprocinfo`: return the cached `FmgrInfo` for support procedure
/// `procnum` of key column `attnum` (1-based) of index `irel`. The lookup info is
/// computed lazily: the first time a slot is requested its `fn_oid` is
/// `InvalidOid`, so we look up `rd_support[procindex]` and `fmgr_info` it into
/// `rd_supportinfo[procindex]`.
///
/// `amsupport` (BTNProcs for btree) is taken from the relcache array layout
/// (sized `indnatts * BT_AMSUPPORT`). Returns a raw `&mut FmgrInfo` into the
/// relcache-owned array (valid while the index relation is open), matching the C
/// `FmgrInfo *` return.
#[must_use]
pub fn index_getprocinfo(irel: Relation, attnum: i32, procnum: u16) -> *mut FmgrInfo {
    use crate::access::nbtree::BTNProcs;
    let nproc = i32::from(BTNProcs); // btree amsupport
    debug_assert!(procnum > 0 && i32::from(procnum) <= nproc);

    let procindex = (nproc * (attnum - 1)) + (i32::from(procnum) - 1);

    // SAFETY: live index relation; the support arrays were allocated by
    // relation_init_index_access_info sized indnatts*BT_AMSUPPORT.
    let rel = unsafe { &*irel };
    debug_assert!(!rel.rd_supportinfo.is_null(), "index support arrays not initialized");
    // SAFETY: procindex < indnatts*BT_AMSUPPORT (asserted by the contract).
    let locinfo: *mut FmgrInfo = unsafe { rel.rd_supportinfo.add(procindex as usize) };

    // Initialize the lookup info the first time through.
    // SAFETY: locinfo points into the relcache support-info array.
    if unsafe { (*locinfo).oid } == crate::postgres_ext::InvalidOid {
        // SAFETY: rd_support parallels rd_supportinfo.
        let proc_id = unsafe { *rel.rd_support.add(procindex as usize) };
        if proc_id == crate::postgres_ext::InvalidOid {
            crate::elog!(
                crate::utils::elog::ERROR,
                format!("missing support function {procnum} for attribute {attnum} of index")
            );
        }
        // SAFETY: locinfo is a valid FmgrInfo slot.
        fmgr_info(proc_id, unsafe { &mut *locinfo });
    }

    locinfo
}

/// An owned index-scan handle for the M2 async path. Wraps the per-AM btree scan
/// state plus the heap relation and snapshot needed for `index_fetch_heap`. The C
/// `IndexScanDescData` raw struct is replaced by this box on the async path.
pub struct IndexScanState {
    pub heap_rel: Relation,
    pub index_rel: Relation,
    pub snapshot: Snapshot,
    /// The btree scan position (the AM's `so` opaque, owned).
    pub bt: BtScan,
    /// The most recent TID returned by `index_getnext_tid`.
    pub xs_heaptid: Option<ItemPointerData>,
}

// SAFETY: the raw Relation handles are task-confined for the scan's lifetime
// (same contract as HeapScanDescData / SysScanState).
#[allow(
    clippy::non_send_fields_in_send_ty,
    reason = "deliberate: raw Relation pointers are task-confined for the scan's lifetime (matches HeapScanDescData's Send impl)"
)]
unsafe impl Send for IndexScanState {}

/// `index_beginscan`: prepare an index scan over `index_rel` (looking up tuples in
/// `heap_rel`). The scan keys are supplied later by `index_rescan` (PG separates
/// begin from key setup). Returns an owned [`IndexScanState`].
#[must_use]
pub fn index_beginscan(
    heap_rel: Relation,
    index_rel: Relation,
    snapshot: Snapshot,
) -> Box<IndexScanState> {
    Box::new(IndexScanState {
        heap_rel,
        index_rel,
        snapshot,
        bt: BtScan::new(index_rel),
        xs_heaptid: None,
    })
}

/// `index_rescan`: (re)start the scan with new equality scan keys. The keys are
/// `(attno, argument)` pairs against the index's key columns (M2 supports the
/// equality search the executor + systable path needs). An empty key set is a
/// full forward scan.
pub fn index_rescan(scan: &mut IndexScanState, keys: Vec<(i32, crate::postgres::Datum)>) {
    scan.bt.set_search_keys(keys);
    scan.xs_heaptid = None;
}

/// `index_getnext_tid`: advance the scan and return the next matching heap TID, or
/// `None` at end of scan. Drives the btree `_bt_first`/`_bt_next`.
pub async fn index_getnext_tid(
    shared: &Arc<SharedState>,
    scan: &mut IndexScanState,
    direction: ScanDirection,
) -> Option<ItemPointerData> {
    let tid = if scan.bt.started {
        bt_next(shared, &mut scan.bt, direction).await
    } else {
        bt_first(shared, &mut scan.bt, direction).await
    };
    scan.xs_heaptid = tid;
    tid
}

/// `index_fetch_heap`: fetch the heap tuple for the current `xs_heaptid` into a
/// freshly allocated owned `HeapTupleData` (M2 form: the executor `TupleTableSlot`
/// machinery is deferred; the systable path consumes the owned tuple). Returns the
/// owned tuple copy, or `None` if the TID is not visible / not present.
///
/// This is the table-AM index fetch (PG `table_index_fetch_tuple` -> heap). M2:
/// no visibility recheck beyond the snapshot the heap scan applies; the catalog
/// path uses the catalog snapshot.
pub async fn index_fetch_heap(
    shared: &Arc<SharedState>,
    scan: &mut IndexScanState,
) -> Option<Box<crate::access::htup::HeapTupleData>> {
    let tid = scan.xs_heaptid?;
    crate::backend::access::heap::heapam::heap_fetch_tid(
        shared,
        scan.heap_rel,
        &tid,
        scan.snapshot.clone(),
    )
    .await
}

/// `index_endscan`: release the scan. Owned state drops here. Takes the boxed
/// handle by value (the C API frees a heap-allocated `IndexScanDesc`).
#[allow(clippy::boxed_local, reason = "mirrors the C index_endscan(IndexScanDesc): consumes a heap handle")]
pub fn index_endscan(_scan: Box<IndexScanState>) {}

/// `index_insert`: insert one index entry for the heap tuple at `heap_tid` into
/// `index_rel` (M2: drives the btree `btinsert`). Returns the AM's bool.
pub async fn index_insert(
    shared: &Arc<SharedState>,
    index_rel: Relation,
    values: &[crate::postgres::Datum],
    isnull: &[bool],
    heap_tid: &ItemPointerData,
) -> bool {
    crate::backend::access::nbtree::nbtree::btinsert(shared, index_rel, values, isnull, heap_tid)
        .await
}

/// `index_getnext_slot`-equivalent for M2: get the next visible heap tuple via the
/// index, as an owned tuple copy. Combines `index_getnext_tid` + `index_fetch_heap`
/// in the loop PG does (skip TIDs whose heap tuple is invisible).
pub async fn index_getnext_heaptuple(
    shared: &Arc<SharedState>,
    scan: &mut IndexScanState,
    direction: ScanDirection,
) -> Option<Box<crate::access::htup::HeapTupleData>> {
    while index_getnext_tid(shared, scan, direction).await.is_some() {
        if let Some(tup) = index_fetch_heap(shared, scan).await {
            return Some(tup);
        }
        // Heap tuple not visible; continue to the next TID.
    }
    None
}
