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

use std::sync::Arc;

use crate::access::sdir::ScanDirection;
use crate::backend::access::nbtree::nbtsearch::{BtScan, bt_first, bt_next};
use crate::backend::utils::fmgr::fmgr::fmgr_info;
use crate::fmgr::FmgrInfo;
use crate::shared_state::SharedState;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::rel::RelationData;
use crate::utils::snapshot::SnapshotData;

/// `index_getprocinfo`: return the cached `FmgrInfo` for support procedure
/// `procnum` of key column `attnum` (1-based) of index `irel`. The lookup info is
/// computed lazily: the first time a slot is requested its `fn_oid` is
/// `InvalidOid`, so we look up `rd_support[procindex]` and `fmgr_info` it into
/// `rd_supportinfo[procindex]`.
///
/// `amsupport` (BTNProcs for btree) is taken from the relcache array layout
/// (sized `indnatts * BT_AMSUPPORT`). Returns an owned `FmgrInfo` resolved from
/// `rd_support[procindex]` (the C `FmgrInfo *` aliases a relcache-cached slot;
/// here every caller copies the result, so we resolve a fresh owned value -- the
/// `fmgr_info` lookup is a cheap builtin-table hit).
#[must_use]
pub fn index_getprocinfo(irel: &RelationData, attnum: i32, procnum: u16) -> FmgrInfo {
    use crate::access::nbtree::BTNProcs;
    let nproc = i32::from(BTNProcs); // btree amsupport
    debug_assert!(procnum > 0 && i32::from(procnum) <= nproc);

    let procindex = ((nproc * (attnum - 1)) + (i32::from(procnum) - 1)) as usize;

    // The support arrays were allocated by relation_init_index_access_info sized
    // indnatts*BT_AMSUPPORT.
    debug_assert!(!irel.rd_supportinfo.is_empty(), "index support arrays not initialized");
    let proc_id = irel.rd_support[procindex];
    if proc_id == crate::postgres_ext::InvalidOid {
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("missing support function {procnum} for attribute {attnum} of index")
        );
    }
    let mut finfo = crate::backend::utils::fmgr::fmgr::empty_flinfo();
    fmgr_info(proc_id, &mut finfo);
    finfo
}

/// An owned index-scan handle for the M2 async path. Wraps the per-AM btree scan
/// state plus the heap relation and snapshot needed for `index_fetch_heap`. The C
/// `IndexScanDescData` raw struct is replaced by this box on the async path.
///
/// Borrow-based ownership (relation-ownership-plan step 2): the heap relation, the
/// index relation, and the snapshot are BORROWS (`&'rel`/`&'irel`/`&'snap`); the
/// owner (the caller's statement/build frame) holds the `Arc`s above and outlives
/// the scan. The btree (`bt`) walks the INDEX relation, hence the distinct `'irel`.
pub struct IndexScanState<'rel, 'irel, 'snap> {
    pub heap_rel: &'rel RelationData,
    pub index_rel: &'irel RelationData,
    pub snapshot: &'snap SnapshotData,
    /// The btree scan position (the AM's `so` opaque, borrowing the index rel).
    pub bt: BtScan<'irel>,
    /// The most recent TID returned by `index_getnext_tid`.
    pub xs_heaptid: Option<ItemPointerData>,
}

/// `index_beginscan`: prepare an index scan over `index_rel` (looking up tuples in
/// `heap_rel`). The scan keys are supplied later by `index_rescan` (PG separates
/// begin from key setup). Returns an owned [`IndexScanState`] borrowing the
/// relations/snapshot from the caller's frame.
#[must_use]
pub fn index_beginscan<'rel, 'irel, 'snap>(
    heap_rel: &'rel RelationData,
    index_rel: &'irel RelationData,
    snapshot: &'snap SnapshotData,
) -> IndexScanState<'rel, 'irel, 'snap> {
    let bt = BtScan::new(index_rel);
    IndexScanState {
        heap_rel,
        index_rel,
        snapshot,
        bt,
        xs_heaptid: None,
    }
}

/// `index_rescan`: (re)start the scan with new equality scan keys. The keys are
/// `(attno, argument)` pairs against the index's key columns (M2 supports the
/// equality search the executor + systable path needs). An empty key set is a
/// full forward scan.
pub fn index_rescan(scan: &mut IndexScanState<'_, '_, '_>, keys: Vec<(i32, crate::postgres::Datum)>) {
    scan.bt.set_search_keys(keys);
    scan.xs_heaptid = None;
}

/// `index_getnext_tid`: advance the scan and return the next matching heap TID, or
/// `None` at end of scan. Drives the btree `_bt_first`/`_bt_next`.
pub async fn index_getnext_tid(
    shared: &Arc<SharedState>,
    scan: &mut IndexScanState<'_, '_, '_>,
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
    scan: &mut IndexScanState<'_, '_, '_>,
) -> Option<Box<crate::access::htup::HeapTupleData>> {
    let tid = scan.xs_heaptid?;
    crate::backend::access::heap::heapam::heap_fetch_tid(
        shared,
        scan.heap_rel,
        &tid,
        scan.snapshot,
    )
    .await
}

/// `index_endscan`: release the scan. Owned state drops here (the borrowed
/// relations/snapshot are released by their owner's frame). Takes the value by
/// move, mirroring the C API that frees a heap-allocated `IndexScanDesc`.
pub fn index_endscan(_scan: IndexScanState<'_, '_, '_>) {}

/// `index_insert`: insert one index entry for the heap tuple at `heap_tid` into
/// `index_rel` (M2: drives the btree `btinsert`). Returns the AM's bool.
pub async fn index_insert(
    shared: &Arc<SharedState>,
    index_rel: &RelationData,
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
    scan: &mut IndexScanState<'_, '_, '_>,
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
