//! Heap-or-index access to system catalogs. Translated from the systable_* half
//! of `src/backend/access/index/genam.c`.
//!
//! These routines let catalog code (catcache/relcache) read a catalog either
//! through its system index or, when the indexes are not yet available (initdb,
//! pre-`criticalRelcachesBuilt` startup), through a plain heap sequential scan.
//! Step 14 implements the HEAP-SCAN path; the INDEX-SCAN path is STAGED against
//! the btree/indexam that completes in step 13-rest (see `systable_beginscan`).
//!
//! Async coloring (rules.md s5): the heap scan reaches the buffer pool, so
//! `systable_getnext`/`systable_endscan` are `async` and thread `&Arc<SharedState>`
//! (exactly as `heap_getnext` does). The returned `HeapTuple` references data in a
//! pinned buffer; per PG it must be copied before the next getnext/endscan.


use std::sync::Arc;

use crate::access::htup::{HeapTuple, HeapTupleData, HeapTupleIsValid};
use crate::access::skey::ScanKeyData;
use crate::access::sdir::ScanDirection;
use crate::backend::access::heap::heapam::{
    heap_beginscan, heap_endscan, heap_getnext, HeapScanDescData,
};
use crate::access::tableam::ScanOptions;
use crate::backend::access::common::heaptuple::{heap_copytuple, heap_freetuple, heap_getattr};
use crate::backend::utils::time::snapmgr::GetCatalogSnapshot;
use crate::postgres::{Datum, DatumGetInt16, DatumGetInt32, DatumGetObjectId};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;
use crate::utils::snapshot::{Snapshot, SnapshotData};

/// The systable scan state. The C `SysScanDescData` is split: the heap-scan arm
/// owns the `HeapScanDescData`; the index arm drives an `IndexScanState`. We also
/// carry the per-call scan-key copy (applied post-fetch, since the M2 heap AM scans
/// with no pushed-down keys) and the just-copied current tuple (owned, freed on the
/// next getnext / endscan).
///
/// Borrow-based ownership (relation-ownership-plan step 3): the catalog heap
/// relation and the registered snapshot are BORROWS (`&'rel`/`&'snap`). Every
/// systable caller (relcache build, catcache/namespace/catalog) opens the catalog
/// `Arc` into its own frame, drives the scan, then closes -- the owner strictly
/// encloses the scan, so the scan can hold `&'rel RelationData` (and a
/// `HeapScanDescData<'rel,'snap>` borrowing the same) across its `.await`s without a
/// self-referential struct or the chunk-1 resume-cursor staging.
pub struct SysScanState<'rel, 'snap> {
    heap_rel: &'rel RelationData,
    /// Heap-scan arm. `None` when the index arm is in use (`iscan` is `Some`). Holds
    /// the `HeapScanDescData` directly (borrowing the same `&'rel`/`&'snap` this
    /// state borrows) -- no resume-cursor staging now the caller owns the `Arc`s.
    hscan: Option<Box<HeapScanDescData<'rel, 'snap>>>,
    /// Index-scan arm (the index-scan path, used when the catalog has a usable
    /// unique index -- Decision 3). `None` on the heap-scan path. The index over the
    /// catalog heap shares the heap's `'rel` lifetime (both opened in the caller's
    /// frame).
    iscan: Option<crate::backend::access::index::indexam::IndexScanState<'rel, 'rel, 'snap>>,
    /// Borrowed catalog snapshot for the scan (owned by the caller's frame).
    snapshot: &'snap SnapshotData,
    /// The scan keys as (heap attno, equality argument) pairs. `ScanKeyData`
    /// carries an `FmgrInfo` (not `Clone`); genam applies equality directly for the
    /// catalog key types, so only attno + argument are needed.
    keys: Vec<(i16, Datum)>,
    /// The current owned tuple copy returned by the last getnext, if any.
    cur: Option<Box<HeapTupleData>>, //Option<HeapTupleData>
}

/// `systable_beginscan`: set up a heap-or-index scan of a catalog.
///
/// `heap_relation` must already be open (via the relcache) and suitably locked.
/// `index_id`/`index_ok` select the index path; in step 14 we always take the
/// HEAP-SCAN path (the index AM is not complete until step 13-rest). When
/// `snapshot` is `None`, a fresh catalog snapshot is taken (PG `GetCatalogSnapshot`
/// + `RegisterSnapshot`).
///
/// The INDEX-SCAN path is STAGED: when `index_ok` and the btree index AM is
/// available (step 13-rest), this will `index_open(index_id)`, convert the heap
/// attnos in `key` to index column numbers, and drive `index_beginscan`. Until
/// then we faithfully fall back to the heap scan (PG does the same before
/// `criticalRelcachesBuilt`).
#[must_use]
pub fn systable_beginscan<'rel, 'snap>(
    _shared: &Arc<SharedState>, // narrow scope
    heap_relation: &'rel RelationData,
    _index_id: Oid,
    _index_ok: bool,
    snapshot: &'snap SnapshotData,
    keys: &[ScanKeyData],
) -> SysScanState<'rel, 'snap> {
    // STAGED (step 13-rest): the index-scan arm. PG takes it when
    //   index_ok && !IgnoreSystemIndexes && !ReindexIsProcessingIndex(index_id)
    //   && criticalRelcachesBuilt. It calls index_open(index_id, AccessShareLock),
    //   remaps key attnos to index columns, then index_beginscan/index_rescan.
    // We always heap-scan in step 14 (faithful: PG also heap-scans before the
    // critical relcache entries exist).

    // M2 heap AM scans with no pushed-down keys; keys are applied post-fetch.
    let hscan = heap_beginscan(heap_relation, snapshot, 0, ScanOptions::ALLOW_PAGEMODE);

    let key_pairs: Vec<(i16, Datum)> = keys.iter().map(|k| (k.attno, k.argument)).collect();
    SysScanState {
        heap_rel: heap_relation,
        hscan: Some(hscan),
        iscan: None,
        snapshot,
        keys: key_pairs,
        cur: None,
    }
}

/// Obtain the catalog snapshot for a systable scan as an OWNED `Arc<SnapshotData>`
/// the caller binds in its frame (the scan then borrows `&*binding`). When the
/// caller already has a snapshot it passes it through; otherwise this takes the
/// per-backend catalog snapshot (PG `GetCatalogSnapshot`). The owned `Arc` is the
/// `'snap` owner that must strictly enclose the scan (relation-ownership-plan
/// step 3: the snapshot owner, like the relation owner, lives ABOVE the scan).
#[must_use]
pub fn systable_scan_snapshot(
    shared: &Arc<SharedState>,
    heap_relation: &RelationData,
    snapshot: Snapshot,
) -> Arc<SnapshotData> {
    snapshot
        .or_else(|| GetCatalogSnapshot(shared, heap_relation.rd_id))
        .unwrap_or_else(|| unreachable!("catalog snapshot must be available for a systable scan"))
}

/// `systable_beginscan` (INDEX path, Decision 3): scan a catalog through its unique
/// btree index. `index_relation` must be an open, built index over `heap_relation`
/// whose key columns map 1:1 to the heap key columns named in `keys` (the catalog
/// indexes are plain column indexes). Equality keys are pushed into the index scan;
/// `systable_getnext` then fetches each matching heap tuple via `index_fetch_heap`.
///
/// This is the faithful path PG takes once `criticalRelcachesBuilt`; the heap-scan
/// `systable_beginscan` stays the fallback used before the critical indexes exist.
#[must_use]
pub fn systable_beginscan_indexed<'rel, 'snap>(
    _shared: &Arc<SharedState>,
    heap_relation: &'rel RelationData,
    index_relation: &'rel RelationData,
    snapshot: &'snap SnapshotData,
    keys: &[ScanKeyData],
) -> SysScanState<'rel, 'snap> {
    use crate::backend::access::index::indexam::{index_beginscan, index_rescan};

    let mut iscan = index_beginscan(heap_relation, index_relation, snapshot);
    // The catalog index columns are 1:1 with the heap key columns in `keys`; remap
    // each heap attno to its index column position (1-based, in key order).
    let index_keys: Vec<(i32, Datum)> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| ((i + 1) as i32, k.argument))
        .collect();
    index_rescan(&mut iscan, index_keys);

    let key_pairs: Vec<(i16, Datum)> = keys.iter().map(|k| (k.attno, k.argument)).collect();
    SysScanState {
        heap_rel: heap_relation,
        hscan: None,
        iscan: Some(iscan),
        snapshot,
        keys: key_pairs,
        cur: None,
    }
}

/// `systable_getnext`: return the next tuple matching the scan keys, or `None` at
/// end of scan. The found tuple is an owned copy held by the scan (`sysscan.cur`,
/// PG returns a buffer reference; we copy eagerly so the borrow stays valid across
/// the caller's processing without holding the buffer pin) and is returned as a
/// borrow rooted in that owner (rule 10: `Option<&HeapTupleData>`, `None` = the C
/// NULL). The borrow lives until the caller's next `systable_getnext`/
/// `systable_endscan` (both reborrow `sysscan` mutably, ending it); the scan owner
/// sits in the caller's frame above every such use. The previous tuple is freed
/// here at the start of the next call.
pub async fn systable_getnext<'scan>(
    shared: &Arc<SharedState>,
    sysscan: &'scan mut SysScanState<'_, '_>,
) -> Option<&'scan HeapTupleData> {
    // Free the previously returned copy.
    if let Some(old) = sysscan.cur.take() {
        heap_freetuple(*old); // frees the copied body once
    }

    // Clone the catalog's tuple descriptor (an Arc, cheap) up front so the `&mut
    // sysscan` reborrow below (heap arm) does not conflict with reading `heap_rel`.
    let tupdesc = sysscan.heap_rel.rd_att.clone()
        .unwrap_or_else(|| unreachable!("open catalog has a tuple descriptor"));

    // INDEX path (Decision 3): drive the btree scan + heap fetch. The index already
    // applies the equality keys, but we re-check post-fetch (the catalog key types
    // compare directly) for safety against any non-key columns.
    if sysscan.iscan.is_some() {
        let iscan = sysscan.iscan.as_mut().unwrap_or_else(|| unreachable!());
        while let Some(tup) =
            crate::backend::access::index::indexam::index_getnext_heaptuple(shared, iscan, ScanDirection::Forward).await
        {
            if scankeys_match(&tup, &tupdesc, &sysscan.keys) {
                // Stash the owned copy; return a borrow rooted in `sysscan.cur`.
                return Some(&**sysscan.cur.insert(tup));
            }
            // Else free and continue (index_getnext_heaptuple returns an owned copy).
            heap_freetuple(*tup);
        }
        return None;
    }

    // Heap arm: the descriptor borrows the same `&'rel`/`&'snap` this state borrows,
    // so it advances directly (no resume-cursor re-supply). Disjoint field access:
    // `hscan`/`cur` mutable, `keys` immutable.
    let SysScanState { hscan, keys, cur, .. } = sysscan;
    let hscan = hscan
        .as_mut()
        .unwrap_or_else(|| unreachable!("systable scan has a heap or index arm"));

    while let Some(tup) = heap_getnext(shared, hscan, ScanDirection::Forward).await {
        // SAFETY: tup points into the pinned scan buffer; valid until next getnext.
        let tref: &HeapTupleData = unsafe { &*tup };
        if scankeys_match(tref, &tupdesc, keys) {
            // Copy before the buffer can be reused (PG: caller must copy), then
            // return a borrow rooted in `sysscan.cur` (the scan owns the copy).
            // SAFETY: tref is a live tuple over the pinned page.
            let copy = unsafe { heap_copytuple(tref) };
            return Some(&**cur.insert(Box::new(copy)));
        }
    }
    None
}

/// `systable_endscan`: close the scan and release resources. The owned tuple copy
/// is freed here; the borrowed relation/snapshot are released by the caller's frame.
pub fn systable_endscan(shared: &Arc<SharedState>, sysscan: &mut SysScanState<'_, '_>) {
    if let Some(old) = sysscan.cur.take() {
        heap_freetuple(*old);
    }
    if let Some(mut hscan) = sysscan.hscan.take() {
        heap_endscan(shared, &mut hscan);
    }
    if let Some(iscan) = sysscan.iscan.take() {
        crate::backend::access::index::indexam::index_endscan(iscan);
    }
}

/// Test whether a heap tuple satisfies every scan key (equality only, as used by
/// the catalog caches). The catalog index key columns are exactly the by-value
/// types oid/int2/int4 and the fixed `name` type; we compare those directly,
/// matching catcache's fast-equal semantics without depending on a fully wired
/// fmgr. An empty key set matches everything (a full-table catalog scan).
fn scankeys_match(
    tuple: &HeapTupleData,
    tupdesc: &crate::access::tupdesc::TupleDescData,
    keys: &[(i16, Datum)],
) -> bool {
    for &(attno, argument) in keys {
        // SAFETY: attno is a valid 1-based attribute number for tupdesc.
        let (val, isnull) = unsafe { heap_getattr(tuple, i32::from(attno), tupdesc) };
        if isnull {
            return false;
        }
        if !datum_eq_by_type(tupdesc, attno, val, argument) {
            return false;
        }
    }
    true
}

/// Equality of a catalog key column against a scan-key argument, dispatched on the
/// column's physical type (attbyval/attlen). Covers the oid/int2/int4 by-value
/// keys and the by-ref fixed-length `name` key used by the M2 catalog indexes.
fn datum_eq_by_type(
    tupdesc: &crate::access::tupdesc::TupleDescData,
    attno: i16,
    a: Datum,
    b: Datum,
) -> bool {
    let att = tupdesc.attr((attno - 1) as usize);
    match (att.attbyval, att.attlen) {
        (true, 4) => DatumGetInt32(a) == DatumGetInt32(b), // oid/int4: low 32 bits
        (true, 2) => DatumGetInt16(a) == DatumGetInt16(b),
        (false, 64) => name_eq(a, b), // NAMEDATALEN fixed name
        // by-ref varlena (attlen -1): compare the value bytes (the oidvector
        // proargtypes key of PROCNAMEARGSNSP). The catalog never toasts these keys.
        (false, -1) => varlena_eq(a, b),
        // by-value other widths, and other by-ref: compare datum bits.
        _ => a.0 == b.0,
    }
}

/// Compare two 4-byte-header varlena datums by their full bytes (the oidvector
/// catalog key). A null pointer compares equal only to another null pointer.
fn varlena_eq(a: Datum, b: Datum) -> bool {
    if a.0 == 0 || b.0 == 0 {
        return a.0 == b.0;
    }
    // SAFETY: each Datum points at a 4-byte-header varlena; VARSIZE bounds the slice.
    unsafe {
        let pa = a.0 as *const u8;
        let pb = b.0 as *const u8;
        let la = crate::varatt::VARSIZE(pa) as usize;
        let lb = crate::varatt::VARSIZE(pb) as usize;
        la == lb && std::slice::from_raw_parts(pa, la) == std::slice::from_raw_parts(pb, lb)
    }
}

/// Compare two `name` datums (pointers to NUL-padded `NameData`).
fn name_eq(a: Datum, b: Datum) -> bool {
    if a.0 == 0 || b.0 == 0 {
        return a.0 == b.0;
    }
    // SAFETY: a name Datum points at a NameData (NAMEDATALEN bytes).
    let pa = a.0 as *const crate::c::NameData;
    let pb = b.0 as *const crate::c::NameData;
    unsafe { (*pa).data == (*pb).data }
}

/// `systable_recheck_tuple`: STAGED. Used to recheck visibility after waiting for a
/// lock; not on the M2 cache-population path.
#[must_use]
pub fn systable_recheck_tuple(_sysscan: &SysScanState<'_, '_>, _tup: HeapTuple) -> bool {
    unimplemented!("systable_recheck_tuple: not on the M2 path")
}

/// Whether a returned systable tuple handle is valid.
#[must_use]
pub fn systable_tuple_is_valid(tup: Option<HeapTuple>) -> bool {
    // SAFETY: pointer validity is the caller's contract; mirror HeapTupleIsValid.
    HeapTupleIsValid(tup.map(|t| unsafe { &*t }))
}

/// `index_bulk_delete` (M13 AM-dispatch): remove `index`'s entries that point at
/// the dead heap TIDs in `dead`, returning `(tuples_removed, tuples_remaining)`.
/// This is the genam entry the executor/VACUUM calls; it dispatches to the index
/// AM's `ambulkdelete`. Translated from the `index_bulk_delete` half of
/// `access/index/genam.c` (`indexRelation->rd_indam->ambulkdelete`).
///
/// M13 supports the btree AM (relam 403 / `BTREE_AM_OID`); other AMs are grow
/// guards. PG passes an `IndexBulkDeleteCallback` closure; the port passes the dead
/// TID set directly (the btree scan checks membership), which is the same
/// information without threading a sync callback through the async page walk.
#[allow(clippy::implicit_hasher, reason = "internal caller builds the set with the default hasher")]
pub async fn index_vacuum_bulk_delete(
    shared: &Arc<SharedState>,
    index: &RelationData,
    dead: &std::collections::HashSet<crate::storage::itemptr::ItemPointerData>,
) -> (u64, u64) {
    const BTREE_AM_OID: u32 = 403;
    let relam = index.form().relam;
    if relam.get() == BTREE_AM_OID {
        crate::backend::access::nbtree::nbtree::btbulkdelete(shared, index, dead).await
    } else {
        unimplemented!("index_vacuum_bulk_delete: only the btree AM (403) is supported at M13");
    }
}
