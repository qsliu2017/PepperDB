//! spgscan.rs
//!   routines for scanning SP-GiST indexes
//!
//! Translated 1:1 from postgres/src/backend/access/spgist/spgscan.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!         src/backend/access/spgist/spgscan.c
//!
//! #include mapping:
//!   "postgres.h"                 -> crate::prelude::*
//!   "access/genam.h"             -> IndexOrderByDistance (spgist_private), RelationGetIndexScan (genam)
//!   "access/relscan.h"           -> IndexScanDesc/IndexScanDescData (crate::access::relscan)
//!   "access/spgist_private.h"    -> crate::access::spgist::spgist_private + ::spgist + ::spgutils
//!   "miscadmin.h"                -> CHECK_FOR_INTERRUPTS (crate::miscadmin)
//!   "pgstat.h"                   -> pgstat_count_index_scan (STUB below)
//!   "storage/bufmgr.h"           -> Read/Lock/.. buffer routines (STUB below)
//!   "utils/datum.h"              -> datumCopy (crate::utils::adt::datum)
//!   "utils/float.h"              -> get_float8_infinity (crate::utils::adt::float)
//!   "utils/lsyscache.h"          -> get_func_rettype (STUB below)
//!   "utils/memutils.h"           -> MemoryContext* (crate::prelude), AllocSetContextCreate
//!   "utils/rel.h"                -> crate::utils::rel (RelationGetDescr)

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;
use crate::access::attnum::AttrNumber;

// Real AM-layer routines (access/index/genam.rs + indexam.rs are now wired).
use crate::access::index::genam::RelationGetIndexScan;
use crate::access::index::indexam::{index_getprocinfo, index_store_float8_orderby_distances};


use crate::{ereport, errmsg, Assert, AllocSetContextCreate};

use std::ffi::{c_char, c_int, c_void};
use std::mem::size_of;
use std::ptr;

// --- access/relscan.h (REAL) -------------------------------------------------
use crate::access::relscan::{IndexScanDesc, IndexScanDescData};

// --- access/genam.h (REAL) ---------------------------------------------------

// --- spgist_private.h (REAL) -------------------------------------------------
use crate::access::spgist::spgist_private::{
    getSpGistTupleDesc, initSpGistState, spgDeformLeafTuple, spgExtractNodeLabels, spgGetCache,
    IndexOrderByDistance, MaxIndexTuplesPerPage, SGLT_GET_NEXTOFFSET, SpGistCache,
    SpGistDeadTuple, SpGistInnerTuple, SpGistInnerTupleData, SpGistLeafTuple, SpGistLeafTupleData,
    SpGistNodeTuple, SpGistScanOpaque, SpGistScanOpaqueData, SpGistSearchItem, SpGistState,
    SpGistTypeDesc, spgFirstIncludeColumn, spgKeyColumn, SpGistBlockIsRoot, SPGIST_DEAD,
    SPGIST_LEAF, SPGIST_LIVE, SPGIST_METAPAGE_BLKNO, SPGIST_NULLS, SPGIST_NULL_BLKNO,
    SPGIST_REDIRECT, SPGIST_ROOT_BLKNO,
};

// --- access/spgist.h support function numbers + arg structs (REAL) -----------
use crate::access::spgist::spgist::{
    spgInnerConsistentIn, spgInnerConsistentOut, spgLeafConsistentIn, spgLeafConsistentOut,
    SPGIST_INNER_CONSISTENT_PROC, SPGIST_LEAF_CONSISTENT_PROC,
};

// --- access/common/scankey.h (REAL) ------------------------------------------
use crate::access::common::scankey::{ScanKey, ScanKeyData, SK_ISNULL, SK_SEARCHNOTNULL, SK_SEARCHNULL};

// --- access/common/tupdesc.h (REAL) ------------------------------------------
use crate::access::common::tupdesc::{FreeTupleDesc, TupleDesc};

// --- access/common/heaptuple.h (REAL) ----------------------------------------
use crate::access::common::heaptuple::heap_form_tuple;

// --- access/htup.h (REAL) ----------------------------------------------------
use crate::access::htup_details::HeapTuple;

// --- access/index/indexam.c (REAL) -------------------------------------------

// --- lib/pairingheap (REAL) --------------------------------------------------
use crate::lib::pairingheap::{
    pairingheap, pairingheap_add, pairingheap_allocate, pairingheap_is_empty, pairingheap_node,
    pairingheap_remove_first,
};
use crate::pairingheap_container;

// --- nodes/tidbitmap.c (REAL) ------------------------------------------------
use crate::nodes::tidbitmap::{tbm_add_tuples, TIDBitmap};

// --- storage (REAL) ----------------------------------------------------------
use crate::storage::block::BlockNumber;
use crate::storage::buf::{Buffer, InvalidBuffer};
use crate::storage::bufpage::{
    Page, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageGetSpecialPointer,
};
use crate::storage::itemptr::{
    ItemPointer, ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
    ItemPointerIsValid, ItemPointerSet,
};
use crate::storage::off::{FirstOffsetNumber, InvalidOffsetNumber, MaxOffsetNumber, OffsetNumber};

// --- utils (REAL) ------------------------------------------------------------
use crate::utils::adt::datum::datumCopy;
use crate::utils::adt::float::get_float8_infinity;
use crate::utils::fmgr::{fmgr_info_copy, FmgrInfo, FunctionCall2Coll};
use crate::utils::rel::{Relation, RelationGetDescr};

// --- miscadmin.h (REAL) ------------------------------------------------------
use crate::miscadmin::CHECK_FOR_INTERRUPTS;

// --- access/sdir.h (REAL) ----------------------------------------------------
use crate::access::sdir::{ForwardScanDirection, ScanDirection};

use crate::access::spgist::spgist_private::SpGistPageOpaqueData;

// ===========================================================================
// Stubs for symbols whose home isn't translated yet.  Matches how sibling
// spgist files stub deep dependencies.
// ===========================================================================

// ---- access/spgist_private.h page-flag accessor macros ---------------------
// SpGistPageGetOpaque/IsLeaf/StoresNulls.  Mirror the C macros directly here
// (PageGetSpecialPointer is REAL).  spgutils.rs/spgvacuum.rs keep local copies.
type SpGistPageOpaque = *mut SpGistPageOpaqueData;
#[inline]
unsafe fn SpGistPageGetOpaque(page: Page) -> SpGistPageOpaque {
    PageGetSpecialPointer(page) as SpGistPageOpaque
}
#[inline]
unsafe fn SpGistPageIsLeaf(page: Page) -> bool {
    ((*SpGistPageGetOpaque(page)).flags & SPGIST_LEAF as uint16) != 0
}
#[inline]
unsafe fn SpGistPageStoresNulls(page: Page) -> bool {
    ((*SpGistPageGetOpaque(page)).flags & SPGIST_NULLS as uint16) != 0
}

// ---- SP-GiST tuple bit-field accessors (packed into bits_) -----------------
// tupstate is the low 2 bits of bits_ for both leaf and inner tuples.
#[inline]
unsafe fn LT_TUPSTATE(lt: SpGistLeafTuple) -> c_int {
    ((*lt).bits_ & 0x3) as c_int
}
#[inline]
unsafe fn IT_TUPSTATE(it: SpGistInnerTuple) -> c_int {
    ((*it).bits_ & 0x3) as c_int
}
// allTheSame is bit 2 (after tupstate:2).
#[inline]
unsafe fn IT_ALLTHESAME(it: SpGistInnerTuple) -> bool {
    ((*it).bits_ & (1 << 2)) != 0
}
// nNodes is bits 3..15 (after tupstate:2, allTheSame:1).
#[inline]
unsafe fn IT_NNODES(it: SpGistInnerTuple) -> c_int {
    (((*it).bits_ >> 3) & 0x1FFF) as c_int
}
// prefixSize is bits 16..31.
#[inline]
unsafe fn IT_PREFIXSIZE(it: SpGistInnerTuple) -> c_uint {
    ((*it).bits_ >> 16) & 0xFFFF
}
// leaf tuple size is bits 2..31.
#[inline]
unsafe fn LT_SIZE(lt: SpGistLeafTuple) -> c_uint {
    ((*lt).bits_ >> 2) & 0x3FFF_FFFF
}

// ---- SGLTHDRSZ / index tuple size for SGITITERATE --------------------------
use crate::access::common::indextuple::{IndexTupleSize, IndexAttributeBitMapData, INDEX_MAX_KEYS};
#[inline]
unsafe fn SGLTHDRSZ(hasnulls: bool) -> Size {
    if hasnulls {
        MAXALIGN(size_of::<SpGistLeafTupleData>() + size_of::<IndexAttributeBitMapData>())
    } else {
        MAXALIGN(size_of::<SpGistLeafTupleData>())
    }
}
use crate::access::spgist::spgist_private::SGLT_GET_HASNULLMASK;
#[inline]
unsafe fn SGLTDATAPTR(x: SpGistLeafTuple) -> *mut c_char {
    (x as *mut c_char).add(SGLTHDRSZ(SGLT_GET_HASNULLMASK(x)))
}
use crate::access::tupmacs::fetch_att;
// SGLTDATUM(x, s) = fetch_att(SGLTDATAPTR(x), attLeafType.attbyval, attLeafType.attlen)
#[inline]
unsafe fn SGLTDATUM(x: SpGistLeafTuple, s: *mut SpGistState) -> Datum {
    fetch_att(
        SGLTDATAPTR(x) as *const c_void,
        (*s).attLeafType.attbyval,
        (*s).attLeafType.attlen as c_int,
    )
}

// SGITHDRSZ / _SGITDATA / SGITDATAPTR / SGITDATUM / SGITNODEPTR
#[inline]
fn SGITHDRSZ() -> Size {
    MAXALIGN(size_of::<SpGistInnerTupleData>())
}
#[inline]
unsafe fn _SGITDATA(x: SpGistInnerTuple) -> *mut c_char {
    (x as *mut c_char).add(SGITHDRSZ())
}
// SGITDATUM(x, s) = prefixSize ? (attbyval ? *(Datum*)_SGITDATA : Pointer..) : 0
#[inline]
unsafe fn SGITDATUM(x: SpGistInnerTuple, s: *mut SpGistState) -> Datum {
    if IT_PREFIXSIZE(x) != 0 {
        if (*s).attPrefixType.attbyval {
            *(_SGITDATA(x) as *mut Datum)
        } else {
            PointerGetDatum(_SGITDATA(x) as *const c_void)
        }
    } else {
        0 as Datum
    }
}
#[inline]
unsafe fn SGITNODEPTR(x: SpGistInnerTuple) -> SpGistNodeTuple {
    _SGITDATA(x).add(IT_PREFIXSIZE(x) as usize) as SpGistNodeTuple
}

// ---- IndexScanInstrumentation shim (relscan.rs keeps it opaque) ------------
// Access nsearches via a layout-compatible shim, as hashsearch.rs does.
#[repr(C)]
struct IndexScanInstrumentationShim {
    nsearches: u64,
}

// ---- pgstat.h (NOT ported) -------------------------------------------------
unsafe fn pgstat_count_index_scan(rel: Relation) {
    // TODO(pg-port): real pgstat_count_index_scan lives in pgstat.h
}

// ---- utils/lsyscache.c (NOT ported) ----------------------------------------
unsafe fn get_func_rettype(funcid: Oid) -> Oid {
    unimplemented!() // TODO(pg-port): real get_func_rettype lives in utils/cache/lsyscache.c
}

// ---- storage/bufmgr.c (NOT ported) -----------------------------------------
const BUFFER_LOCK_SHARE: c_int = 1;
unsafe fn ReadBuffer(reln: Relation, blockNum: BlockNumber) -> Buffer {
    unimplemented!() // TODO(pg-port): real ReadBuffer lives in storage/buffer/bufmgr.c
}
unsafe fn LockBuffer(buffer: Buffer, mode: c_int) {
    unimplemented!() // TODO(pg-port): real LockBuffer lives in storage/buffer/bufmgr.c
}
unsafe fn UnlockReleaseBuffer(buffer: Buffer) {
    unimplemented!() // TODO(pg-port): real UnlockReleaseBuffer lives in storage/buffer/bufmgr.c
}
unsafe fn BufferGetPage(buffer: Buffer) -> Page {
    unimplemented!() // TODO(pg-port): real BufferGetPage lives in storage/bufmgr.h
}
unsafe fn BufferGetBlockNumber(buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO(pg-port): real BufferGetBlockNumber lives in storage/buffer/bufmgr.c
}

// ===========================================================================
// spgscan.c
// ===========================================================================

// SizeOfSpGistSearchItem(n_distances) =
//   offsetof(SpGistSearchItem, distances) + sizeof(double) * n_distances
#[inline]
fn SizeOfSpGistSearchItem(n_distances: c_int) -> Size {
    core::mem::offset_of!(SpGistSearchItem, distances) + size_of::<f64>() * n_distances as usize
}

type storeRes_func = unsafe fn(
    so: SpGistScanOpaque,
    heapPtr: ItemPointer,
    leafValue: Datum,
    isNull: bool,
    leafTuple: SpGistLeafTuple,
    recheck: bool,
    recheckDistances: bool,
    distances: *mut f64,
);

/*
 * Pairing heap comparison function for the SpGistSearchItem queue.
 * KNN-searches currently only support NULLS LAST.  So, preserve this logic
 * here.
 */
unsafe fn pairingheap_SpGistSearchItem_cmp(
    a: *const pairingheap_node,
    b: *const pairingheap_node,
    arg: *mut c_void,
) -> c_int {
    let sa: *const SpGistSearchItem = pairingheap_container!(SpGistSearchItem, phNode, a);
    let sb: *const SpGistSearchItem = pairingheap_container!(SpGistSearchItem, phNode, b);
    let so: SpGistScanOpaque = arg as SpGistScanOpaque;
    let mut i: c_int;

    if (*sa).isNull {
        if !(*sb).isNull {
            return -1;
        }
    } else if (*sb).isNull {
        return 1;
    } else {
        /* Order according to distance comparison */
        i = 0;
        while i < (*so).numberOfNonNullOrderBys {
            let da = *(*sa).distances.as_ptr().add(i as usize);
            let db = *(*sb).distances.as_ptr().add(i as usize);
            if da.is_nan() && db.is_nan() {
                i += 1;
                continue; /* NaN == NaN */
            }
            if da.is_nan() {
                return -1; /* NaN > number */
            }
            if db.is_nan() {
                return 1; /* number < NaN */
            }
            if da != db {
                return if da < db { 1 } else { -1 };
            }
            i += 1;
        }
    }

    /* Leaf items go before inner pages, to ensure a depth-first search */
    if (*sa).isLeaf && !(*sb).isLeaf {
        return 1;
    }
    if !(*sa).isLeaf && (*sb).isLeaf {
        return -1;
    }

    0
}

unsafe fn spgFreeSearchItem(so: SpGistScanOpaque, item: *mut SpGistSearchItem) {
    /* value is of type attType if isLeaf, else of type attLeafType */
    /* (no, that is not backwards; yes, it's confusing) */
    if !(if (*item).isLeaf {
        (*so).state.attType.attbyval
    } else {
        (*so).state.attLeafType.attbyval
    }) && !DatumGetPointer((*item).value).is_null()
    {
        pfree(DatumGetPointer((*item).value) as *mut c_void);
    }

    if !(*item).leafTuple.is_null() {
        pfree((*item).leafTuple as *mut c_void);
    }

    if !(*item).traversalValue.is_null() {
        pfree((*item).traversalValue);
    }

    pfree(item as *mut c_void);
}

/*
 * Add SpGistSearchItem to queue
 *
 * Called in queue context
 */
unsafe fn spgAddSearchItemToQueue(so: SpGistScanOpaque, item: *mut SpGistSearchItem) {
    pairingheap_add((*so).scanQueue, &mut (*item).phNode);
}

unsafe fn spgAllocSearchItem(
    so: SpGistScanOpaque,
    isnull: bool,
    distances: *mut f64,
) -> *mut SpGistSearchItem {
    /* allocate distance array only for non-NULL items */
    let item: *mut SpGistSearchItem = palloc(SizeOfSpGistSearchItem(if isnull {
        0
    } else {
        (*so).numberOfNonNullOrderBys
    })) as *mut SpGistSearchItem;

    (*item).isNull = isnull;

    if !isnull && (*so).numberOfNonNullOrderBys > 0 {
        ptr::copy_nonoverlapping(
            distances,
            (*item).distances.as_mut_ptr(),
            (*so).numberOfNonNullOrderBys as usize,
        );
    }

    item
}

unsafe fn spgAddStartItem(so: SpGistScanOpaque, isnull: bool) {
    let startEntry: *mut SpGistSearchItem = spgAllocSearchItem(so, isnull, (*so).zeroDistances);

    ItemPointerSet(
        &mut (*startEntry).heapPtr,
        if isnull {
            SPGIST_NULL_BLKNO
        } else {
            SPGIST_ROOT_BLKNO
        },
        FirstOffsetNumber,
    );
    (*startEntry).isLeaf = false;
    (*startEntry).level = 0;
    (*startEntry).value = 0 as Datum;
    (*startEntry).leafTuple = ptr::null_mut();
    (*startEntry).traversalValue = ptr::null_mut();
    (*startEntry).recheck = false;
    (*startEntry).recheckDistances = false;

    spgAddSearchItemToQueue(so, startEntry);
}

/*
 * Initialize queue to search the root page, resetting
 * any previously active scan
 */
unsafe fn resetSpGistScanOpaque(so: SpGistScanOpaque) {
    let oldCtx: MemoryContext;

    MemoryContextReset((*so).traversalCxt as crate::utils::palloc::MemoryContext);

    oldCtx = MemoryContextSwitchTo((*so).traversalCxt as crate::utils::palloc::MemoryContext);

    /* initialize queue only for distance-ordered scans */
    (*so).scanQueue = pairingheap_allocate(
        pairingheap_SpGistSearchItem_cmp,
        so as *mut c_void,
    );

    if (*so).searchNulls {
        /* Add a work item to scan the null index entries */
        spgAddStartItem(so, true);
    }

    if (*so).searchNonNulls {
        /* Add a work item to scan the non-null index entries */
        spgAddStartItem(so, false);
    }

    MemoryContextSwitchTo(oldCtx);

    if (*so).numberOfOrderBys > 0 {
        /* Must pfree distances to avoid memory leak */
        let mut i: c_int = 0;
        while i < (*so).nPtrs {
            if !(*so).distances[i as usize].is_null() {
                pfree((*so).distances[i as usize] as *mut c_void);
            }
            i += 1;
        }
    }

    if (*so).want_itup {
        /* Must pfree reconstructed tuples to avoid memory leak */
        let mut i: c_int = 0;
        while i < (*so).nPtrs {
            pfree((*so).reconTups[i as usize] as *mut c_void);
            i += 1;
        }
    }
    (*so).iPtr = 0;
    (*so).nPtrs = 0;
}

/*
 * Prepare scan keys in SpGistScanOpaque from caller-given scan keys
 *
 * Sets searchNulls, searchNonNulls, numberOfKeys, keyData fields of *so.
 *
 * The point here is to eliminate null-related considerations from what the
 * opclass consistent functions need to deal with.  We assume all SPGiST-
 * indexable operators are strict, so any null RHS value makes the scan
 * condition unsatisfiable.  We also pull out any IS NULL/IS NOT NULL
 * conditions; their effect is reflected into searchNulls/searchNonNulls.
 */
unsafe fn spgPrepareScanKeys(scan: IndexScanDesc) {
    let so: SpGistScanOpaque = (*scan).opaque as SpGistScanOpaque;
    let mut qual_ok: bool;
    let mut haveIsNull: bool;
    let mut haveNotNull: bool;
    let mut nkeys: c_int;
    let mut i: c_int;

    (*so).numberOfOrderBys = (*scan).numberOfOrderBys;
    (*so).orderByData = (*scan).orderByData as ScanKey;

    if (*so).numberOfOrderBys <= 0 {
        (*so).numberOfNonNullOrderBys = 0;
    } else {
        let mut j: c_int = 0;

        /*
         * Remove all NULL keys, but remember their offsets in the original
         * array.
         */
        i = 0;
        while i < (*scan).numberOfOrderBys {
            let skey: ScanKey = (*so).orderByData.add(i as usize);

            if (*skey).sk_flags & SK_ISNULL != 0 {
                *(*so).nonNullOrderByOffsets.add(i as usize) = -1;
            } else {
                if i != j {
                    *(*so).orderByData.add(j as usize) = core::ptr::read(skey);
                }

                *(*so).nonNullOrderByOffsets.add(i as usize) = j;
                j += 1;
            }
            i += 1;
        }

        (*so).numberOfNonNullOrderBys = j;
    }

    if (*scan).numberOfKeys <= 0 {
        /* If no quals, whole-index scan is required */
        (*so).searchNulls = true;
        (*so).searchNonNulls = true;
        (*so).numberOfKeys = 0;
        return;
    }

    /* Examine the given quals */
    qual_ok = true;
    haveIsNull = false;
    haveNotNull = false;
    nkeys = 0;
    i = 0;
    while i < (*scan).numberOfKeys {
        let skey: ScanKey = ((*scan).keyData as ScanKey).add(i as usize);

        if (*skey).sk_flags & SK_SEARCHNULL != 0 {
            haveIsNull = true;
        } else if (*skey).sk_flags & SK_SEARCHNOTNULL != 0 {
            haveNotNull = true;
        } else if (*skey).sk_flags & SK_ISNULL != 0 {
            /* ordinary qual with null argument - unsatisfiable */
            qual_ok = false;
            break;
        } else {
            /* ordinary qual, propagate into so->keyData */
            *(*so).keyData.add(nkeys as usize) = core::ptr::read(skey);
            nkeys += 1;
            /* this effectively creates a not-null requirement */
            haveNotNull = true;
        }
        i += 1;
    }

    /* IS NULL in combination with something else is unsatisfiable */
    if haveIsNull && haveNotNull {
        qual_ok = false;
    }

    /* Emit results */
    if qual_ok {
        (*so).searchNulls = haveIsNull;
        (*so).searchNonNulls = haveNotNull;
        (*so).numberOfKeys = nkeys;
    } else {
        (*so).searchNulls = false;
        (*so).searchNonNulls = false;
        (*so).numberOfKeys = 0;
    }
}

pub unsafe fn spgbeginscan(rel: Relation, keysz: c_int, orderbysz: c_int) -> IndexScanDesc {
    let scan: IndexScanDesc;
    let so: SpGistScanOpaque;
    let mut i: c_int;

    scan = RelationGetIndexScan(rel, keysz, orderbysz) as IndexScanDesc;

    so = palloc0(size_of::<SpGistScanOpaqueData>()) as SpGistScanOpaque;
    if keysz > 0 {
        (*so).keyData = palloc(size_of::<ScanKeyData>() * keysz as usize) as ScanKey;
    } else {
        (*so).keyData = ptr::null_mut();
    }
    initSpGistState(&mut (*so).state, (*scan).indexRelation);

    (*so).tempCxt = AllocSetContextCreate!(
        CurrentMemoryContext as crate::utils::mmgr::memnodes::MemoryContext,
        c"SP-GiST search temporary context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    (*so).traversalCxt = AllocSetContextCreate!(
        CurrentMemoryContext as crate::utils::mmgr::memnodes::MemoryContext,
        c"SP-GiST traversal-value context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    /*
     * Set up reconTupDesc and xs_hitupdesc in case it's an index-only scan,
     * making sure that the key column is shown as being of type attType.
     * (It's rather annoying to do this work when it might be wasted, but for
     * most opclasses we can re-use the index reldesc instead of making one.)
     */
    (*so).reconTupDesc = getSpGistTupleDesc(rel, &mut (*so).state.attType);
    (*scan).xs_hitupdesc = (*so).reconTupDesc as *mut crate::access::relscan::TupleDescData;

    /* Allocate various arrays needed for order-by scans */
    if (*scan).numberOfOrderBys > 0 {
        /* This will be filled in spgrescan, but allocate the space here */
        (*so).orderByTypes =
            palloc(size_of::<Oid>() * (*scan).numberOfOrderBys as usize) as *mut Oid;
        (*so).nonNullOrderByOffsets =
            palloc(size_of::<c_int>() * (*scan).numberOfOrderBys as usize) as *mut c_int;

        /* These arrays have constant contents, so we can fill them now */
        (*so).zeroDistances =
            palloc(size_of::<f64>() * (*scan).numberOfOrderBys as usize) as *mut f64;
        (*so).infDistances =
            palloc(size_of::<f64>() * (*scan).numberOfOrderBys as usize) as *mut f64;

        i = 0;
        while i < (*scan).numberOfOrderBys {
            *(*so).zeroDistances.add(i as usize) = 0.0;
            *(*so).infDistances.add(i as usize) = get_float8_infinity();
            i += 1;
        }

        (*scan).xs_orderbyvals =
            palloc0(size_of::<Datum>() * (*scan).numberOfOrderBys as usize) as *mut Datum;
        (*scan).xs_orderbynulls =
            palloc(size_of::<bool>() * (*scan).numberOfOrderBys as usize) as *mut bool;
        /* memset(..., true, ...) sets each byte to 1, i.e. each bool to true */
        ptr::write_bytes(
            (*scan).xs_orderbynulls,
            1u8,
            (*scan).numberOfOrderBys as usize,
        );
    }

    fmgr_info_copy(
        &mut (*so).innerConsistentFn,
        index_getprocinfo(rel, 1, SPGIST_INNER_CONSISTENT_PROC as uint16),
        CurrentMemoryContext,
    );

    fmgr_info_copy(
        &mut (*so).leafConsistentFn,
        index_getprocinfo(rel, 1, SPGIST_LEAF_CONSISTENT_PROC as uint16),
        CurrentMemoryContext,
    );

    (*so).indexCollation = *(*rel).rd_indcollation;

    (*scan).opaque = so as *mut c_void;

    scan
}

pub unsafe fn spgrescan(
    scan: IndexScanDesc,
    scankey: ScanKey,
    nscankeys: c_int,
    orderbys: ScanKey,
    norderbys: c_int,
) {
    let so: SpGistScanOpaque = (*scan).opaque as SpGistScanOpaque;

    /* copy scankeys into local storage */
    if !scankey.is_null() && (*scan).numberOfKeys > 0 {
        ptr::copy_nonoverlapping(
            scankey,
            (*scan).keyData as ScanKey,
            (*scan).numberOfKeys as usize,
        );
    }

    /* initialize order-by data if needed */
    if !orderbys.is_null() && (*scan).numberOfOrderBys > 0 {
        let mut i: c_int;

        ptr::copy_nonoverlapping(
            orderbys,
            (*scan).orderByData as ScanKey,
            (*scan).numberOfOrderBys as usize,
        );

        i = 0;
        while i < (*scan).numberOfOrderBys {
            let skey: ScanKey = ((*scan).orderByData as ScanKey).add(i as usize);

            /*
             * Look up the datatype returned by the original ordering
             * operator. SP-GiST always uses a float8 for the distance
             * function, but the ordering operator could be anything else.
             *
             * XXX: The distance function is only allowed to be lossy if the
             * ordering operator's result type is float4 or float8.  Otherwise
             * we don't know how to return the distance to the executor.  But
             * we cannot check that here, as we won't know if the distance
             * function is lossy until it returns *recheck = true for the
             * first time.
             */
            *(*so).orderByTypes.add(i as usize) = get_func_rettype((*skey).sk_func.fn_oid);
            i += 1;
        }
    }

    /* preprocess scankeys, set up the representation in *so */
    spgPrepareScanKeys(scan);

    /* set up starting queue entries */
    resetSpGistScanOpaque(so);

    /* count an indexscan for stats */
    pgstat_count_index_scan((*scan).indexRelation);
    if !(*scan).instrument.is_null() {
        (*((*scan).instrument as *mut IndexScanInstrumentationShim)).nsearches += 1;
    }
}

pub unsafe fn spgendscan(scan: IndexScanDesc) {
    let so: SpGistScanOpaque = (*scan).opaque as SpGistScanOpaque;

    MemoryContextDelete((*so).tempCxt as crate::utils::palloc::MemoryContext);
    MemoryContextDelete((*so).traversalCxt as crate::utils::palloc::MemoryContext);

    if !(*so).keyData.is_null() {
        pfree((*so).keyData as *mut c_void);
    }

    if !(*so).state.leafTupDesc.is_null()
        && (*so).state.leafTupDesc != RelationGetDescr((*so).state.index)
    {
        FreeTupleDesc((*so).state.leafTupDesc);
    }

    if !(*so).state.deadTupleStorage.is_null() {
        pfree((*so).state.deadTupleStorage as *mut c_void);
    }

    if (*scan).numberOfOrderBys > 0 {
        pfree((*so).orderByTypes as *mut c_void);
        pfree((*so).nonNullOrderByOffsets as *mut c_void);
        pfree((*so).zeroDistances as *mut c_void);
        pfree((*so).infDistances as *mut c_void);
        pfree((*scan).xs_orderbyvals as *mut c_void);
        pfree((*scan).xs_orderbynulls as *mut c_void);
    }

    pfree(so as *mut c_void);
}

/*
 * Leaf SpGistSearchItem constructor, called in queue context
 */
unsafe fn spgNewHeapItem(
    so: SpGistScanOpaque,
    level: c_int,
    leafTuple: SpGistLeafTuple,
    leafValue: Datum,
    recheck: bool,
    recheckDistances: bool,
    isnull: bool,
    distances: *mut f64,
) -> *mut SpGistSearchItem {
    let item: *mut SpGistSearchItem = spgAllocSearchItem(so, isnull, distances);

    (*item).level = level;
    (*item).heapPtr = (*leafTuple).heapPtr;

    /*
     * If we need the reconstructed value, copy it to queue cxt out of tmp
     * cxt.  Caution: the leaf_consistent method may not have supplied a value
     * if we didn't ask it to, and mildly-broken methods might supply one of
     * the wrong type.  The correct leafValue type is attType not leafType.
     */
    if (*so).want_itup {
        (*item).value = if isnull {
            0 as Datum
        } else {
            datumCopy(
                leafValue,
                (*so).state.attType.attbyval,
                (*so).state.attType.attlen as c_int,
            )
        };

        /*
         * If we're going to need to reconstruct INCLUDE attributes, store the
         * whole leaf tuple so we can get the INCLUDE attributes out of it.
         */
        if (*(*so).state.leafTupDesc).natts > 1 {
            (*item).leafTuple = palloc(LT_SIZE(leafTuple) as Size) as SpGistLeafTuple;
            ptr::copy_nonoverlapping(
                leafTuple as *const u8,
                (*item).leafTuple as *mut u8,
                LT_SIZE(leafTuple) as usize,
            );
        } else {
            (*item).leafTuple = ptr::null_mut();
        }
    } else {
        (*item).value = 0 as Datum;
        (*item).leafTuple = ptr::null_mut();
    }
    (*item).traversalValue = ptr::null_mut();
    (*item).isLeaf = true;
    (*item).recheck = recheck;
    (*item).recheckDistances = recheckDistances;

    item
}

/*
 * Test whether a leaf tuple satisfies all the scan keys
 *
 * *reportedSome is set to true if:
 *		the scan is not ordered AND the item satisfies the scankeys
 */
unsafe fn spgLeafTest(
    so: SpGistScanOpaque,
    item: *mut SpGistSearchItem,
    leafTuple: SpGistLeafTuple,
    isnull: bool,
    reportedSome: *mut bool,
    storeRes: storeRes_func,
) -> bool {
    let leafValue: Datum;
    let distances: *mut f64;
    let result: bool;
    let recheck: bool;
    let recheckDistances: bool;

    if isnull {
        /* Should not have arrived on a nulls page unless nulls are wanted */
        Assert!((*so).searchNulls);
        leafValue = 0 as Datum;
        distances = ptr::null_mut();
        recheck = false;
        recheckDistances = false;
        result = true;
    } else {
        let mut in_: spgLeafConsistentIn = std::mem::zeroed();
        let mut out: spgLeafConsistentOut = std::mem::zeroed();

        /* use temp context for calling leaf_consistent */
        let oldCxt: MemoryContext = MemoryContextSwitchTo((*so).tempCxt as crate::utils::palloc::MemoryContext);

        in_.scankeys = (*so).keyData;
        in_.nkeys = (*so).numberOfKeys;
        in_.orderbys = (*so).orderByData;
        in_.norderbys = (*so).numberOfNonNullOrderBys;
        Assert!(!(*item).isLeaf); /* else reconstructedValue would be wrong type */
        in_.reconstructedValue = (*item).value;
        in_.traversalValue = (*item).traversalValue;
        in_.level = (*item).level;
        in_.returnData = (*so).want_itup;
        in_.leafDatum = SGLTDATUM(leafTuple, &mut (*so).state);

        out.leafValue = 0 as Datum;
        out.recheck = false;
        out.distances = ptr::null_mut();
        out.recheckDistances = false;

        result = DatumGetBool(FunctionCall2Coll(
            &mut (*so).leafConsistentFn,
            (*so).indexCollation,
            PointerGetDatum(&mut in_ as *mut spgLeafConsistentIn as *mut c_void),
            PointerGetDatum(&mut out as *mut spgLeafConsistentOut as *mut c_void),
        ));
        recheck = out.recheck;
        recheckDistances = out.recheckDistances;
        leafValue = out.leafValue;
        distances = out.distances;

        MemoryContextSwitchTo(oldCxt);
    }

    if result {
        /* item passes the scankeys */
        if (*so).numberOfNonNullOrderBys > 0 {
            /* the scan is ordered -> add the item to the queue */
            let oldCxt: MemoryContext = MemoryContextSwitchTo((*so).traversalCxt as crate::utils::palloc::MemoryContext);
            let heapItem: *mut SpGistSearchItem = spgNewHeapItem(
                so,
                (*item).level,
                leafTuple,
                leafValue,
                recheck,
                recheckDistances,
                isnull,
                distances,
            );

            spgAddSearchItemToQueue(so, heapItem);

            MemoryContextSwitchTo(oldCxt);
        } else {
            /* non-ordered scan, so report the item right away */
            Assert!(!recheckDistances);
            storeRes(
                so,
                &mut (*leafTuple).heapPtr,
                leafValue,
                isnull,
                leafTuple,
                recheck,
                false,
                ptr::null_mut(),
            );
            *reportedSome = true;
        }
    }

    result
}

/* A bundle initializer for inner_consistent methods */
unsafe fn spgInitInnerConsistentIn(
    in_: *mut spgInnerConsistentIn,
    so: SpGistScanOpaque,
    item: *mut SpGistSearchItem,
    innerTuple: SpGistInnerTuple,
) {
    (*in_).scankeys = (*so).keyData;
    (*in_).orderbys = (*so).orderByData;
    (*in_).nkeys = (*so).numberOfKeys;
    (*in_).norderbys = (*so).numberOfNonNullOrderBys;
    Assert!(!(*item).isLeaf); /* else reconstructedValue would be wrong type */
    (*in_).reconstructedValue = (*item).value;
    (*in_).traversalMemoryContext = (*so).traversalCxt;
    (*in_).traversalValue = (*item).traversalValue;
    (*in_).level = (*item).level;
    (*in_).returnData = (*so).want_itup;
    (*in_).allTheSame = IT_ALLTHESAME(innerTuple);
    (*in_).hasPrefix = IT_PREFIXSIZE(innerTuple) > 0;
    (*in_).prefixDatum = SGITDATUM(innerTuple, &mut (*so).state);
    (*in_).nNodes = IT_NNODES(innerTuple);
    (*in_).nodeLabels = spgExtractNodeLabels(&mut (*so).state, innerTuple);
}

unsafe fn spgMakeInnerItem(
    so: SpGistScanOpaque,
    parentItem: *mut SpGistSearchItem,
    tuple: SpGistNodeTuple,
    out: *mut spgInnerConsistentOut,
    i: c_int,
    isnull: bool,
    distances: *mut f64,
) -> *mut SpGistSearchItem {
    let item: *mut SpGistSearchItem = spgAllocSearchItem(so, isnull, distances);

    (*item).heapPtr = (*tuple).t_tid;
    (*item).level = if !(*out).levelAdds.is_null() {
        (*parentItem).level + *(*out).levelAdds.add(i as usize)
    } else {
        (*parentItem).level
    };

    /* Must copy value out of temp context */
    /* (recall that reconstructed values are of type leafType) */
    (*item).value = if !(*out).reconstructedValues.is_null() {
        datumCopy(
            *(*out).reconstructedValues.add(i as usize),
            (*so).state.attLeafType.attbyval,
            (*so).state.attLeafType.attlen as c_int,
        )
    } else {
        0 as Datum
    };

    (*item).leafTuple = ptr::null_mut();

    /*
     * Elements of out.traversalValues should be allocated in
     * in.traversalMemoryContext, which is actually a long lived context of
     * index scan.
     */
    (*item).traversalValue = if !(*out).traversalValues.is_null() {
        *(*out).traversalValues.add(i as usize)
    } else {
        ptr::null_mut()
    };

    (*item).isLeaf = false;
    (*item).recheck = false;
    (*item).recheckDistances = false;

    item
}

unsafe fn spgInnerTest(
    so: SpGistScanOpaque,
    item: *mut SpGistSearchItem,
    innerTuple: SpGistInnerTuple,
    isnull: bool,
) {
    let oldCxt: MemoryContext = MemoryContextSwitchTo((*so).tempCxt as crate::utils::palloc::MemoryContext);
    let mut out: spgInnerConsistentOut = std::mem::zeroed();
    let nNodes: c_int = IT_NNODES(innerTuple);
    let mut i: c_int;

    /* memset(&out, 0, sizeof(out)) already done via zeroed() above */

    if !isnull {
        let mut in_: spgInnerConsistentIn = std::mem::zeroed();

        spgInitInnerConsistentIn(&mut in_, so, item, innerTuple);

        /* use user-defined inner consistent method */
        FunctionCall2Coll(
            &mut (*so).innerConsistentFn,
            (*so).indexCollation,
            PointerGetDatum(&mut in_ as *mut spgInnerConsistentIn as *mut c_void),
            PointerGetDatum(&mut out as *mut spgInnerConsistentOut as *mut c_void),
        );
    } else {
        /* force all children to be visited */
        out.nNodes = nNodes;
        out.nodeNumbers = palloc(size_of::<c_int>() * nNodes as usize) as *mut c_int;
        i = 0;
        while i < nNodes {
            *out.nodeNumbers.add(i as usize) = i;
            i += 1;
        }
    }

    /* If allTheSame, they should all or none of them match */
    if IT_ALLTHESAME(innerTuple) && out.nNodes != 0 && out.nNodes != nNodes {
        elog!(
            ERROR,
            "inconsistent inner_consistent results for allTheSame inner tuple"
        );
    }

    if out.nNodes != 0 {
        /* collect node pointers */
        let mut node: SpGistNodeTuple;
        let nodes: *mut SpGistNodeTuple =
            palloc(size_of::<SpGistNodeTuple>() * nNodes as usize) as *mut SpGistNodeTuple;

        /* SGITITERATE(innerTuple, i, node) */
        i = 0;
        node = SGITNODEPTR(innerTuple);
        while i < nNodes {
            *nodes.add(i as usize) = node;
            i += 1;
            node = (node as *mut c_char).add(IndexTupleSize(node)) as SpGistNodeTuple;
        }

        MemoryContextSwitchTo((*so).traversalCxt as crate::utils::palloc::MemoryContext);

        i = 0;
        while i < out.nNodes {
            let nodeN: c_int = *out.nodeNumbers.add(i as usize);
            let innerItem: *mut SpGistSearchItem;
            let distances: *mut f64;

            Assert!(nodeN >= 0 && nodeN < nNodes);

            node = *nodes.add(nodeN as usize);

            if !ItemPointerIsValid(&(*node).t_tid) {
                i += 1;
                continue;
            }

            /*
             * Use infinity distances if innerConsistentFn() failed to return
             * them or if is a NULL item (their distances are really unused).
             */
            distances = if !out.distances.is_null() {
                *out.distances.add(i as usize)
            } else {
                (*so).infDistances
            };

            innerItem = spgMakeInnerItem(so, item, node, &mut out, i, isnull, distances);

            spgAddSearchItemToQueue(so, innerItem);
            i += 1;
        }
    }

    MemoryContextSwitchTo(oldCxt);
}

/* Returns a next item in an (ordered) scan or null if the index is exhausted */
unsafe fn spgGetNextQueueItem(so: SpGistScanOpaque) -> *mut SpGistSearchItem {
    if pairingheap_is_empty((*so).scanQueue) {
        return ptr::null_mut(); /* Done when both heaps are empty */
    }

    /* Return item; caller is responsible to pfree it */
    pairingheap_container!(
        SpGistSearchItem,
        phNode,
        pairingheap_remove_first((*so).scanQueue)
    )
}

/* enum SpGistSpecialOffsetNumbers */
const SpGistBreakOffsetNumber: OffsetNumber = InvalidOffsetNumber;
const SpGistRedirectOffsetNumber: OffsetNumber = MaxOffsetNumber + 1;
const SpGistErrorOffsetNumber: OffsetNumber = MaxOffsetNumber + 2;

unsafe fn spgTestLeafTuple(
    so: SpGistScanOpaque,
    item: *mut SpGistSearchItem,
    page: Page,
    offset: OffsetNumber,
    isnull: bool,
    isroot: bool,
    reportedSome: *mut bool,
    storeRes: storeRes_func,
) -> OffsetNumber {
    let leafTuple: SpGistLeafTuple =
        PageGetItem(page, PageGetItemId(page, offset)) as SpGistLeafTuple;

    if LT_TUPSTATE(leafTuple) != SPGIST_LIVE {
        if !isroot {
            /* all tuples on root should be live */
            if LT_TUPSTATE(leafTuple) == SPGIST_REDIRECT {
                /* redirection tuple should be first in chain */
                Assert!(offset == ItemPointerGetOffsetNumber(&(*item).heapPtr));
                /* transfer attention to redirect point */
                (*item).heapPtr = (*(leafTuple as SpGistDeadTuple)).pointer;
                Assert!(
                    ItemPointerGetBlockNumber(&(*item).heapPtr) != SPGIST_METAPAGE_BLKNO
                );
                return SpGistRedirectOffsetNumber;
            }

            if LT_TUPSTATE(leafTuple) == SPGIST_DEAD {
                /* dead tuple should be first in chain */
                Assert!(offset == ItemPointerGetOffsetNumber(&(*item).heapPtr));
                /* No live entries on this page */
                Assert!(SGLT_GET_NEXTOFFSET(leafTuple) == InvalidOffsetNumber);
                return SpGistBreakOffsetNumber;
            }
        }

        /* We should not arrive at a placeholder */
        elog!(
            ERROR,
            "unexpected SPGiST tuple state: {}",
            LT_TUPSTATE(leafTuple)
        );
        return SpGistErrorOffsetNumber;
    }

    Assert!(ItemPointerIsValid(&(*leafTuple).heapPtr));

    spgLeafTest(so, item, leafTuple, isnull, reportedSome, storeRes);

    SGLT_GET_NEXTOFFSET(leafTuple)
}

/*
 * Walk the tree and report all tuples passing the scan quals to the storeRes
 * subroutine.
 *
 * If scanWholeIndex is true, we'll do just that.  If not, we'll stop at the
 * next page boundary once we have reported at least one tuple.
 */
unsafe fn spgWalk(
    index: Relation,
    so: SpGistScanOpaque,
    scanWholeIndex: bool,
    storeRes: storeRes_func,
) {
    let mut buffer: Buffer = InvalidBuffer;
    let mut reportedSome: bool = false;

    while scanWholeIndex || !reportedSome {
        let item: *mut SpGistSearchItem = spgGetNextQueueItem(so);

        if item.is_null() {
            break; /* No more items in queue -> done */
        }

        'redirect: loop {
            /* Check for interrupts, just in case of infinite loop */
            CHECK_FOR_INTERRUPTS();

            if (*item).isLeaf {
                /* We store heap items in the queue only in case of ordered search */
                Assert!((*so).numberOfNonNullOrderBys > 0);
                storeRes(
                    so,
                    &mut (*item).heapPtr,
                    (*item).value,
                    (*item).isNull,
                    (*item).leafTuple,
                    (*item).recheck,
                    (*item).recheckDistances,
                    (*item).distances.as_mut_ptr(),
                );
                reportedSome = true;
            } else {
                let blkno: BlockNumber = ItemPointerGetBlockNumber(&(*item).heapPtr);
                let mut offset: OffsetNumber = ItemPointerGetOffsetNumber(&(*item).heapPtr);
                let page: Page;
                let isnull: bool;

                if buffer == InvalidBuffer {
                    buffer = ReadBuffer(index, blkno);
                    LockBuffer(buffer, BUFFER_LOCK_SHARE);
                } else if blkno != BufferGetBlockNumber(buffer) {
                    UnlockReleaseBuffer(buffer);
                    buffer = ReadBuffer(index, blkno);
                    LockBuffer(buffer, BUFFER_LOCK_SHARE);
                }

                /* else new pointer points to the same page, no work needed */

                page = BufferGetPage(buffer);

                isnull = SpGistPageStoresNulls(page);

                if SpGistPageIsLeaf(page) {
                    /* Page is a leaf - that is, all its tuples are heap items */
                    let max: OffsetNumber = PageGetMaxOffsetNumber(page);

                    if SpGistBlockIsRoot(blkno) {
                        /* When root is a leaf, examine all its tuples */
                        offset = FirstOffsetNumber;
                        while offset <= max {
                            spgTestLeafTuple(
                                so,
                                item,
                                page,
                                offset,
                                isnull,
                                true,
                                &mut reportedSome,
                                storeRes,
                            );
                            offset += 1;
                        }
                    } else {
                        /* Normal case: just examine the chain we arrived at */
                        let mut do_redirect = false;
                        while offset != InvalidOffsetNumber {
                            Assert!(offset >= FirstOffsetNumber && offset <= max);
                            offset = spgTestLeafTuple(
                                so,
                                item,
                                page,
                                offset,
                                isnull,
                                false,
                                &mut reportedSome,
                                storeRes,
                            );
                            if offset == SpGistRedirectOffsetNumber {
                                do_redirect = true;
                                break;
                            }
                        }
                        if do_redirect {
                            continue 'redirect;
                        }
                    }
                } else {
                    /* page is inner */
                    let innerTuple: SpGistInnerTuple =
                        PageGetItem(page, PageGetItemId(page, offset)) as SpGistInnerTuple;

                    if IT_TUPSTATE(innerTuple) != SPGIST_LIVE {
                        if IT_TUPSTATE(innerTuple) == SPGIST_REDIRECT {
                            /* transfer attention to redirect point */
                            (*item).heapPtr = (*(innerTuple as SpGistDeadTuple)).pointer;
                            Assert!(
                                ItemPointerGetBlockNumber(&(*item).heapPtr)
                                    != SPGIST_METAPAGE_BLKNO
                            );
                            continue 'redirect;
                        }
                        elog!(
                            ERROR,
                            "unexpected SPGiST tuple state: {}",
                            IT_TUPSTATE(innerTuple)
                        );
                    }

                    spgInnerTest(so, item, innerTuple, isnull);
                }
            }

            break 'redirect;
        }

        /* done with this scan item */
        spgFreeSearchItem(so, item);
        /* clear temp context before proceeding to the next one */
        MemoryContextReset((*so).tempCxt as crate::utils::palloc::MemoryContext);
    }

    if buffer != InvalidBuffer {
        UnlockReleaseBuffer(buffer);
    }
}

/* storeRes subroutine for getbitmap case */
unsafe fn storeBitmap(
    so: SpGistScanOpaque,
    heapPtr: ItemPointer,
    leafValue: Datum,
    isnull: bool,
    leafTuple: SpGistLeafTuple,
    recheck: bool,
    recheckDistances: bool,
    distances: *mut f64,
) {
    Assert!(!recheckDistances && distances.is_null());
    tbm_add_tuples((*so).tbm, heapPtr, 1, recheck);
    (*so).ntids += 1;
}

pub unsafe fn spggetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 {
    let so: SpGistScanOpaque = (*scan).opaque as SpGistScanOpaque;

    /* Copy want_itup to *so so we don't need to pass it around separately */
    (*so).want_itup = false;

    (*so).tbm = tbm;
    (*so).ntids = 0;

    spgWalk((*scan).indexRelation, so, true, storeBitmap);

    (*so).ntids
}

/* storeRes subroutine for gettuple case */
unsafe fn storeGettuple(
    so: SpGistScanOpaque,
    heapPtr: ItemPointer,
    leafValue: Datum,
    isnull: bool,
    leafTuple: SpGistLeafTuple,
    recheck: bool,
    recheckDistances: bool,
    nonNullDistances: *mut f64,
) {
    Assert!((*so).nPtrs < MaxIndexTuplesPerPage as c_int);
    (*so).heapPtrs[(*so).nPtrs as usize] = *heapPtr;
    (*so).recheck[(*so).nPtrs as usize] = recheck;
    (*so).recheckDistances[(*so).nPtrs as usize] = recheckDistances;

    if (*so).numberOfOrderBys > 0 {
        if isnull || (*so).numberOfNonNullOrderBys <= 0 {
            (*so).distances[(*so).nPtrs as usize] = ptr::null_mut();
        } else {
            let distances: *mut IndexOrderByDistance = palloc(
                size_of::<IndexOrderByDistance>() * (*so).numberOfOrderBys as usize,
            ) as *mut IndexOrderByDistance;
            let mut i: c_int = 0;

            while i < (*so).numberOfOrderBys {
                let offset: c_int = *(*so).nonNullOrderByOffsets.add(i as usize);

                if offset >= 0 {
                    /* Copy non-NULL distance value */
                    (*distances.add(i as usize)).value = *nonNullDistances.add(offset as usize);
                    (*distances.add(i as usize)).isnull = false;
                } else {
                    /* Set distance's NULL flag. */
                    (*distances.add(i as usize)).value = 0.0;
                    (*distances.add(i as usize)).isnull = true;
                }
                i += 1;
            }

            (*so).distances[(*so).nPtrs as usize] = distances;
        }
    }

    if (*so).want_itup {
        /*
         * Reconstruct index data.  We have to copy the datum out of the temp
         * context anyway, so we may as well create the tuple here.
         */
        let mut leafDatums: [Datum; INDEX_MAX_KEYS] = [0 as Datum; INDEX_MAX_KEYS];
        let mut leafIsnulls: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];

        /* We only need to deform the old tuple if it has INCLUDE attributes */
        if (*(*so).state.leafTupDesc).natts > 1 {
            spgDeformLeafTuple(
                leafTuple,
                (*so).state.leafTupDesc,
                leafDatums.as_mut_ptr(),
                leafIsnulls.as_mut_ptr(),
                isnull,
            );
        }

        leafDatums[spgKeyColumn as usize] = leafValue;
        leafIsnulls[spgKeyColumn as usize] = isnull;

        (*so).reconTups[(*so).nPtrs as usize] = heap_form_tuple(
            (*so).reconTupDesc,
            leafDatums.as_mut_ptr(),
            leafIsnulls.as_mut_ptr(),
        );
    }
    (*so).nPtrs += 1;
}

pub unsafe fn spggettuple(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    let so: SpGistScanOpaque = (*scan).opaque as SpGistScanOpaque;

    if dir != ForwardScanDirection {
        elog!(ERROR, "SP-GiST only supports forward scan direction");
    }

    /* Copy want_itup to *so so we don't need to pass it around separately */
    (*so).want_itup = (*scan).xs_want_itup;

    loop {
        if (*so).iPtr < (*so).nPtrs {
            /* continuing to return reported tuples */
            (*scan).xs_heaptid = (*so).heapPtrs[(*so).iPtr as usize];
            (*scan).xs_recheck = (*so).recheck[(*so).iPtr as usize];
            (*scan).xs_hitup = (*so).reconTups[(*so).iPtr as usize];

            if (*so).numberOfOrderBys > 0 {
                index_store_float8_orderby_distances(
                    scan as *mut crate::access::relscan::IndexScanDescData,
                    (*so).orderByTypes,
                    (*so).distances[(*so).iPtr as usize]
                        as *mut crate::access::index::indexam::IndexOrderByDistance,
                    (*so).recheckDistances[(*so).iPtr as usize],
                );
            }
            (*so).iPtr += 1;
            return true;
        }

        if (*so).numberOfOrderBys > 0 {
            /* Must pfree distances to avoid memory leak */
            let mut i: c_int = 0;
            while i < (*so).nPtrs {
                if !(*so).distances[i as usize].is_null() {
                    pfree((*so).distances[i as usize] as *mut c_void);
                }
                i += 1;
            }
        }

        if (*so).want_itup {
            /* Must pfree reconstructed tuples to avoid memory leak */
            let mut i: c_int = 0;
            while i < (*so).nPtrs {
                pfree((*so).reconTups[i as usize] as *mut c_void);
                i += 1;
            }
        }
        (*so).iPtr = 0;
        (*so).nPtrs = 0;

        spgWalk((*scan).indexRelation, so, false, storeGettuple);

        if (*so).nPtrs == 0 {
            break; /* must have completed scan */
        }
    }

    false
}

pub unsafe fn spgcanreturn(index: Relation, attno: c_int) -> bool {
    let cache: *mut SpGistCache;

    /* INCLUDE attributes can always be fetched for index-only scans */
    if attno > 1 {
        return true;
    }

    /* We can do it if the opclass config function says so */
    cache = spgGetCache(index);

    (*cache).config.canReturnData
}
