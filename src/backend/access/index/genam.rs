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

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: the catalog caches are PER-BACKEND task-confined state (raw HeapTuple/FmgrInfo pointers); their populate futures never migrate threads mid-await. await_holding_lock/refcell are clean (enforced)."
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "catalog-cache routines take raw Relation/HeapTuple pointers per the C API; the deref is faithful to C (callers pass live handles)"
)]
use std::sync::Arc;

use crate::access::htup::{HeapTuple, HeapTupleData, HeapTupleIsValid};
use crate::access::skey::ScanKeyData;
use crate::access::sdir::ScanDirection;
use crate::backend::access::heap::heapam::{
    heap_beginscan, heap_endscan, heap_getnext, HeapScanDescData, SendPtr,
};
use crate::access::tableam::ScanOptions;
use crate::backend::access::common::heaptuple::{heap_copytuple, heap_freetuple, heap_getattr};
use crate::backend::utils::time::snapmgr::GetCatalogSnapshot;
use crate::postgres::{Datum, DatumGetInt16, DatumGetInt32, DatumGetObjectId};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;
use crate::utils::relcache::Relation;
use crate::utils::snapshot::{Snapshot, SnapshotData};

/// The systable scan state. The C `SysScanDescData` is split: the heap-scan arm
/// owns the boxed `HeapScanDescData`; the index arm is staged. We also carry the
/// per-call scan-key copy (applied post-fetch, since the M2 heap AM scans with no
/// pushed-down keys) and the just-copied current tuple (owned, freed on the next
/// getnext / endscan).
pub struct SysScanState {
    heap_rel: Relation,
    /// Boxed heap scan descriptor (heap-scan arm). `None` once the index arm
    /// lands (step 13-rest).
    hscan: Option<Box<HeapScanDescData>>,
    /// Registered catalog snapshot for the scan (kept alive for its lifetime).
    snapshot: Snapshot,
    /// The scan keys as (heap attno, equality argument) pairs. `ScanKeyData`
    /// carries an `FmgrInfo` (not `Clone`); genam applies equality directly for the
    /// catalog key types, so only attno + argument are needed.
    keys: Vec<(i16, Datum)>,
    /// The current owned tuple copy returned by the last getnext, if any.
    cur: Option<Box<HeapTupleData>>,
}

// The only non-Send field is the raw `Relation` pointer (heap_rel) and the boxed
// heap scan (which itself is `unsafe impl Send`). Backends run on the tokio
// multi-thread runtime; the scan is owned by one task for its whole lifetime, so
// the handle never races. Same contract as `HeapScanDescData`'s Send impl.
#[allow(
    clippy::non_send_fields_in_send_ty,
    reason = "deliberate: the raw Relation pointer is task-confined for the scan's lifetime (same contract as HeapScanDescData's Send impl)"
)]
unsafe impl Send for SysScanState {}

/// A `SysScanDesc` handle. The header's `SysScanDesc = *mut SysScanDescData`
/// pointer alias is replaced by an owned box handle for the M2 path.
pub type SysScanDesc = Box<SysScanState>;

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
pub fn systable_beginscan(
    shared: &Arc<SharedState>,
    heap_relation: Relation,
    _index_id: Oid,
    _index_ok: bool,
    snapshot: Snapshot,
    keys: &[ScanKeyData],
) -> SysScanDesc {
    // SAFETY: caller passes a live, open relation.
    let relid = unsafe { (*heap_relation).rd_id };

    // STAGED (step 13-rest): the index-scan arm. PG takes it when
    //   index_ok && !IgnoreSystemIndexes && !ReindexIsProcessingIndex(index_id)
    //   && criticalRelcachesBuilt. It calls index_open(index_id, AccessShareLock),
    //   remaps key attnos to index columns, then index_beginscan/index_rescan.
    // We always heap-scan in step 14 (faithful: PG also heap-scans before the
    // critical relcache entries exist).

    let snap = snapshot.map_or_else(|| GetCatalogSnapshot(shared, relid), Some);

    let scan_snapshot: Arc<SnapshotData> = snap
        .clone()
        .unwrap_or_else(|| unreachable!("catalog snapshot must be available for a systable scan"));

    // M2 heap AM scans with no pushed-down keys; keys are applied post-fetch.
    let hscan = heap_beginscan(
        SendPtr(heap_relation),
        scan_snapshot,
        0,
        ScanOptions::ALLOW_PAGEMODE,
    );

    let key_pairs: Vec<(i16, Datum)> = keys.iter().map(|k| (k.attno, k.argument)).collect();
    Box::new(SysScanState {
        heap_rel: heap_relation,
        hscan: Some(hscan),
        snapshot: snap,
        keys: key_pairs,
        cur: None,
    })
}

/// `systable_getnext`: return the next tuple matching the scan keys, or `None` at
/// end of scan. The returned `HeapTuple` is an owned copy held by the scan (PG
/// returns a buffer reference; we copy eagerly so the pointer stays valid across
/// the caller's processing without holding the buffer pin). The previous tuple is
/// freed here.
pub async fn systable_getnext(shared: &Arc<SharedState>, sysscan: &mut SysScanState) -> Option<HeapTuple> {
    // Free the previously returned copy.
    if let Some(old) = sysscan.cur.take() {
        heap_freetuple(*old); // frees the copied body once
    }

    // Clone the catalog's tuple descriptor (an Arc, Send) up front so we do not
    // hold a `&RelationData` borrow across the `.await` (keeps the future Send and
    // avoids await_holding a non-Send reference).
    // SAFETY: live relation for the scan.
    let tupdesc = unsafe { (*sysscan.heap_rel).rd_att.clone() }
        .unwrap_or_else(|| unreachable!("open catalog has a tuple descriptor"));

    let hscan = sysscan
        .hscan
        .as_mut()
        .unwrap_or_else(|| unreachable!("step-14 systable scan always has a heap scan"));

    while let Some(tup) = heap_getnext(shared, hscan, ScanDirection::Forward).await {
        // SAFETY: tup points into the pinned scan buffer; valid until next getnext.
        let tref: &HeapTupleData = unsafe { &*tup };
        if scankeys_match(tref, &tupdesc, &sysscan.keys) {
            // Copy before the buffer can be reused (PG: caller must copy).
            // SAFETY: tref is a live tuple over the pinned page.
            let copy = unsafe { heap_copytuple(tref) };
            let mut boxed = Box::new(copy);
            let ptr: *mut HeapTupleData = std::ptr::from_mut::<HeapTupleData>(boxed.as_mut());
            sysscan.cur = Some(boxed);
            return Some(ptr);
        }
    }
    None
}

/// `systable_endscan`: close the scan and release resources. The owned tuple copy
/// and registered snapshot are dropped here.
pub fn systable_endscan(shared: &Arc<SharedState>, sysscan: &mut SysScanState) {
    if let Some(old) = sysscan.cur.take() {
        heap_freetuple(*old);
    }
    if let Some(mut hscan) = sysscan.hscan.take() {
        heap_endscan(shared, &mut hscan);
    }
    // The snapshot Arc is released when `sysscan.snapshot` drops.
    sysscan.snapshot = None;
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
        // by-value other widths, and by-ref pointer/other: compare datum bits.
        _ => a.0 == b.0,
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
pub fn systable_recheck_tuple(_sysscan: &SysScanState, _tup: HeapTuple) -> bool {
    unimplemented!("systable_recheck_tuple: not on the M2 path")
}

/// Whether a returned systable tuple handle is valid.
#[must_use]
pub fn systable_tuple_is_valid(tup: Option<HeapTuple>) -> bool {
    // SAFETY: pointer validity is the caller's contract; mirror HeapTupleIsValid.
    HeapTupleIsValid(tup.map(|t| unsafe { &*t }))
}
