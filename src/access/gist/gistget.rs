//! src/backend/access/gist/gistget.c
//!
//! gistget.c
//!	  fetch tuples from a GiST scan.
//!
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/access/gist/gistget.c

use crate::prelude::*;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::block::BlockNumber;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use std::ffi::c_int;
use std::ptr;

use crate::c::int64;

// ---------------------------------------------------------------------------
// Stub types/aliases for dependencies not yet ported.
// ---------------------------------------------------------------------------
type IndexScanDesc = *mut IndexScanDescData;
type GISTScanOpaque = *mut GISTScanOpaqueData;
type GISTSTATE = GISTSTATEData;
type ScanKey = *mut ScanKeyData;
type Relation = *mut RelationData;
type Buffer = c_int;
type Page = *mut std::ffi::c_void;
type GISTPageOpaque = *mut GISTPageOpaqueData;
type OffsetNumber = u16;
type ItemId = *mut ItemIdData;
type IndexTuple = *mut IndexTupleData;
type MemoryContextT = crate::utils::palloc::MemoryContext;
type TIDBitmap = std::ffi::c_void;
type GISTENTRY = GISTENTRYData;
type IndexOrderByDistance = IndexOrderByDistanceData;
type GISTSearchItem = GISTSearchItemData;
type ScanDirection = c_int;
type GistNSN = crate::access::transam::xlogdefs::XLogRecPtr;

// Opaque stub structs (real definitions live in other modules).
#[repr(C)]
pub struct IndexScanDescData {
    pub indexRelation: Relation,
    pub opaque: *mut std::ffi::c_void,
    pub keyData: ScanKey,
    pub numberOfKeys: c_int,
    pub orderByData: ScanKey,
    pub numberOfOrderBys: c_int,
    pub xs_snapshot: *mut std::ffi::c_void,
    pub xs_want_itup: bool,
    pub xs_hitup: *mut std::ffi::c_void,
    pub xs_heaptid: ItemPointerData,
    pub xs_recheck: bool,
    pub ignore_killed_tuples: bool,
    pub kill_prior_tuple: bool,
    pub instrument: *mut IndexScanInstrumentation,
}

#[repr(C)]
pub struct IndexScanInstrumentation {
    pub nsearches: u64,
}

#[repr(C)]
pub struct GISTScanOpaqueData {
    pub giststate: *mut GISTSTATEData,
    pub queue: *mut pairingheap,
    pub queueCxt: MemoryContextT,
    pub firstCall: bool,
    pub distances: *mut IndexOrderByDistanceData,
    pub orderByTypes: *mut Oid,
    pub pageData: *mut GISTSearchHeapItem,
    pub nPageData: c_int,
    pub curPageData: c_int,
    pub pageDataCxt: MemoryContextT,
    pub qual_ok: bool,
    pub curBlkno: BlockNumber,
    pub curPageLSN: crate::access::transam::xlogdefs::XLogRecPtr,
    pub killedItems: *mut OffsetNumber,
    pub numKilled: c_int,
}

#[repr(C)]
pub struct GISTSTATEData {
    pub scanCxt: MemoryContextT,
    pub tempCxt: MemoryContextT,
    pub leafTupdesc: crate::access::common::tupdesc::TupleDesc,
}

#[repr(C)]
pub struct ScanKeyData {
    pub sk_flags: c_int,
    pub sk_attno: crate::access::attnum::AttrNumber,
    pub sk_strategy: u16,
    pub sk_subtype: Oid,
    pub sk_collation: Oid,
    pub sk_func: FmgrInfo,
    pub sk_argument: Datum,
}

#[repr(C)]
pub struct FmgrInfo {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct RelationData {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct GISTPageOpaqueData {
    pub rightlink: BlockNumber,
}

#[repr(C)]
pub struct ItemIdData {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct IndexTupleData {
    pub t_tid: ItemPointerData,
}

#[repr(C)]
pub struct GISTENTRYData {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct IndexOrderByDistanceData {
    pub value: f64,
    pub isnull: bool,
}

#[repr(C)]
pub struct pairingheap {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct pairingheap_node {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct GISTSearchHeapItem {
    pub heapPtr: ItemPointerData,
    pub recheck: bool,
    pub recheckDistances: bool,
    pub offnum: OffsetNumber,
    pub recontup: *mut std::ffi::c_void,
}

#[repr(C)]
pub struct GISTSearchItemData {
    pub phNode: pairingheap_node,
    pub blkno: BlockNumber,
    pub data: GISTSearchItemUnion,
    pub distances: [IndexOrderByDistanceData; 0], // FLEXIBLE_ARRAY_MEMBER
}

#[repr(C)]
pub union GISTSearchItemUnion {
    pub parentlsn: GistNSN,
    pub heap: GISTSearchHeapItemUnion,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct GISTSearchHeapItemUnion {
    pub heapPtr: ItemPointerData,
    pub recheck: bool,
    pub recheckDistances: bool,
    pub recontup: *mut std::ffi::c_void,
}

// ---------------------------------------------------------------------------
// Stub free functions for unported dependencies.
// ---------------------------------------------------------------------------
unsafe fn ReadBuffer(_reln: Relation, _blockNum: BlockNumber) -> Buffer {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn BufferIsValid(_buffer: Buffer) -> bool { crate::access::nbtree::nbtpage::BufferIsValid(_buffer) }
unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/buffer/bufmgr.h
}
unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn BufferGetLSNAtomic(_buffer: Buffer) -> crate::access::transam::xlogdefs::XLogRecPtr {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn MarkBufferDirtyHint(_buffer: Buffer, _buffer_std: bool) { crate::storage::buffer::bufmgr::MarkBufferDirtyHint(_buffer, _buffer_std) }
unsafe fn PredicateLockPage(_relation: Relation, _blkno: BlockNumber, _snapshot: *mut std::ffi::c_void) {
    unimplemented!() // TODO: storage/lmgr/predicate.c
}
unsafe fn gistcheckpage(_rel: Relation, _buf: Buffer) { unimplemented!() }
unsafe fn GistPageGetOpaque(_page: Page) -> GISTPageOpaque {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn GistPageIsLeaf(_page: Page) -> bool {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn GistPageIsDeleted(_page: Page) -> bool {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn GistFollowRight(_page: Page) -> bool {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn GistPageGetNSN(_page: Page) -> GistNSN {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn GistMarkPageHasGarbage(_page: Page) {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn GistTupleIsInvalid(_tuple: IndexTuple) -> bool { unimplemented!() }
unsafe fn PageGetItemId(_page: Page, _offnum: OffsetNumber) -> ItemId {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetItem(_page: Page, _iid: ItemId) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetMaxOffsetNumber(_page: Page) -> OffsetNumber {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn ItemIdMarkDead(_iid: ItemId) { unimplemented!() }
unsafe fn ItemIdIsDead(_iid: ItemId) -> bool {
    unimplemented!() // TODO: storage/itemid.h
}
unsafe fn OffsetNumberNext(offset: OffsetNumber) -> OffsetNumber {
    offset + 1
}
unsafe fn ItemPointerGetBlockNumber(_pointer: *mut ItemPointerData) -> BlockNumber {
    unimplemented!() // TODO: storage/itemptr.h
}
unsafe fn XLogRecPtrIsInvalid(lsn: crate::access::transam::xlogdefs::XLogRecPtr) -> bool {
    lsn == 0
}
unsafe fn index_getattr(
    _tuple: IndexTuple,
    _attnum: c_int,
    _tupleDesc: crate::access::common::tupdesc::TupleDesc,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: access/itup.h
}
unsafe fn gistdentryinit(
    _giststate: *mut GISTSTATEData,
    _nkey: c_int,
    _e: *mut GISTENTRY,
    _k: Datum,
    _r: Relation,
    _pg: Page,
    _o: OffsetNumber,
    _l: bool,
    _isNull: bool,
) { unimplemented!() }
unsafe fn gistFetchTuple(
    _giststate: *mut GISTSTATEData,
    _r: Relation,
    _tuple: IndexTuple,
) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: access/gist/gistutil.c
}
unsafe fn FunctionCall5Coll(
    _flinfo: *mut FmgrInfo,
    _collation: Oid,
    _arg1: Datum,
    _arg2: Datum,
    _arg3: Datum,
    _arg4: Datum,
    _arg5: Datum,
) -> Datum { unimplemented!() }
unsafe fn pairingheap_is_empty(_heap: *mut pairingheap) -> bool { unimplemented!() }
unsafe fn pairingheap_remove_first(_heap: *mut pairingheap) -> *mut pairingheap_node { unimplemented!() }
unsafe fn pairingheap_add(_heap: *mut pairingheap, _node: *mut pairingheap_node) { unimplemented!() }
unsafe fn tbm_add_tuples(
    _tbm: *mut TIDBitmap,
    _tids: *mut ItemPointerData,
    _ntids: c_int,
    _recheck: bool,
) {
    unimplemented!() // TODO: nodes/tidbitmap.c
}
unsafe fn index_store_float8_orderby_distances(
    _scan: IndexScanDesc,
    _orderByTypes: *mut Oid,
    _distances: *mut IndexOrderByDistance,
    _recheckOrderBy: bool,
) { unimplemented!() }
unsafe fn index_getprocid(_irel: Relation, _attnum: c_int, _procnum: u16) -> Oid { unimplemented!() }
unsafe fn IndexRelationGetNumberOfKeyAttributes(_relation: Relation) -> c_int { unimplemented!() }
unsafe fn get_float8_infinity() -> f64 {
    unimplemented!() // TODO: utils/float.h
}
unsafe fn pgstat_count_index_scan(_rel: Relation) {
    unimplemented!() // TODO: pgstat.h
}
unsafe fn SizeOfGISTSearchItem(n_distances: c_int) -> Size {
    // offsetof(GISTSearchItemData, distances) + sizeof(IndexOrderByDistance) * n
    (core::mem::offset_of!(GISTSearchItemData, distances)
        + std::mem::size_of::<IndexOrderByDistanceData>() * n_distances as usize) as Size
}
unsafe fn GISTSearchItemIsHeap(item: &GISTSearchItemData) -> bool {
    item.blkno == InvalidBlockNumber
}

// Constants
const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
const FirstOffsetNumber: OffsetNumber = 1;
const GIST_ROOT_BLKNO: BlockNumber = 0;
const GIST_SHARE: c_int = 0; // BUFFER_LOCK_SHARE placeholder
const SK_ISNULL: c_int = 0x0001;
const SK_SEARCHNULL: c_int = 0x0010;
const SK_SEARCHNOTNULL: c_int = 0x0020;
const ForwardScanDirection: ScanDirection = 1;
const GIST_FETCH_PROC: u16 = 9;
const GIST_COMPRESS_PROC: u16 = 3;
const MaxIndexTuplesPerPage: c_int = 1000; // placeholder, see itup.h

/*
 * gistkillitems() -- set LP_DEAD state for items an indexscan caller has
 * told us were killed.
 *
 * We re-read page here, so it's important to check page LSN. If the page
 * has been modified since the last read (as determined by LSN), we cannot
 * flag any entries because it is possible that the old entry was vacuumed
 * away and the TID was re-used by a completely different heap tuple.
 */
unsafe fn gistkillitems(scan: IndexScanDesc) {
    let so: GISTScanOpaque = (*scan).opaque as GISTScanOpaque;
    let buffer: Buffer;
    let page: Page;
    let mut offnum: OffsetNumber;
    let mut iid: ItemId;
    let mut i: c_int;
    let mut killedsomething: bool = false;

    Assert!((*so).curBlkno != InvalidBlockNumber);
    Assert!(!XLogRecPtrIsInvalid((*so).curPageLSN));
    Assert!(!(*so).killedItems.is_null());

    buffer = ReadBuffer((*scan).indexRelation, (*so).curBlkno);
    if !BufferIsValid(buffer) {
        return;
    }

    LockBuffer(buffer, GIST_SHARE);
    gistcheckpage((*scan).indexRelation, buffer);
    page = BufferGetPage(buffer);

    /*
     * If page LSN differs it means that the page was modified since the last
     * read. killedItems could be not valid so LP_DEAD hints applying is not
     * safe.
     */
    if BufferGetLSNAtomic(buffer) != (*so).curPageLSN {
        UnlockReleaseBuffer(buffer);
        (*so).numKilled = 0; /* reset counter */
        return;
    }

    Assert!(GistPageIsLeaf(page));

    /*
     * Mark all killedItems as dead. We need no additional recheck, because,
     * if page was modified, curPageLSN must have changed.
     */
    i = 0;
    while i < (*so).numKilled {
        offnum = *(*so).killedItems.add(i as usize);
        iid = PageGetItemId(page, offnum);
        ItemIdMarkDead(iid);
        killedsomething = true;
        i += 1;
    }

    if killedsomething {
        GistMarkPageHasGarbage(page);
        MarkBufferDirtyHint(buffer, true);
    }

    UnlockReleaseBuffer(buffer);

    /*
     * Always reset the scan state, so we don't look for same items on other
     * pages.
     */
    (*so).numKilled = 0;
}

/*
 * gistindex_keytest() -- does this index tuple satisfy the scan key(s)?
 *
 * The index tuple might represent either a heap tuple or a lower index page,
 * depending on whether the containing page is a leaf page or not.
 *
 * On success return for a heap tuple, *recheck_p is set to indicate whether
 * the quals need to be rechecked.  We recheck if any of the consistent()
 * functions request it.  recheck is not interesting when examining a non-leaf
 * entry, since we must visit the lower index page if there's any doubt.
 * Similarly, *recheck_distances_p is set to indicate whether the distances
 * need to be rechecked, and it is also ignored for non-leaf entries.
 *
 * If we are doing an ordered scan, so->distances[] is filled with distance
 * data from the distance() functions before returning success.
 *
 * We must decompress the key in the IndexTuple before passing it to the
 * sk_funcs (which actually are the opclass Consistent or Distance methods).
 *
 * Note that this function is always invoked in a short-lived memory context,
 * so we don't need to worry about cleaning up allocated memory, either here
 * or in the implementation of any Consistent or Distance methods.
 */
unsafe fn gistindex_keytest(
    scan: IndexScanDesc,
    tuple: IndexTuple,
    page: Page,
    offset: OffsetNumber,
    recheck_p: *mut bool,
    recheck_distances_p: *mut bool,
) -> bool {
    let so: GISTScanOpaque = (*scan).opaque as GISTScanOpaque;
    let giststate: *mut GISTSTATE = (*so).giststate;
    let mut key: ScanKey = (*scan).keyData;
    let mut keySize: c_int = (*scan).numberOfKeys;
    let mut distance_p: *mut IndexOrderByDistance;
    let r: Relation = (*scan).indexRelation;

    *recheck_p = false;
    *recheck_distances_p = false;

    /*
     * If it's a leftover invalid tuple from pre-9.1, treat it as a match with
     * minimum possible distances.  This means we'll always follow it to the
     * referenced page.
     */
    if GistTupleIsInvalid(tuple) {
        let mut i: c_int;

        if GistPageIsLeaf(page) {
            /* shouldn't happen */
            elog!(ERROR, "invalid GiST tuple found on leaf page");
        }
        i = 0;
        while i < (*scan).numberOfOrderBys {
            (*(*so).distances.add(i as usize)).value = -get_float8_infinity();
            (*(*so).distances.add(i as usize)).isnull = false;
            i += 1;
        }
        return true;
    }

    /* Check whether it matches according to the Consistent functions */
    while keySize > 0 {
        let datum: Datum;
        let mut isNull: bool = false;

        datum = index_getattr(
            tuple,
            (*key).sk_attno as c_int,
            (*giststate).leafTupdesc,
            &mut isNull,
        );

        if ((*key).sk_flags & SK_ISNULL) != 0 {
            /*
             * On non-leaf page we can't conclude that child hasn't NULL
             * values because of assumption in GiST: union (VAL, NULL) is VAL.
             * But if on non-leaf page key IS NULL, then all children are
             * NULL.
             */
            if ((*key).sk_flags & SK_SEARCHNULL) != 0 {
                if GistPageIsLeaf(page) && !isNull {
                    return false;
                }
            } else {
                Assert!(((*key).sk_flags & SK_SEARCHNOTNULL) != 0);
                if isNull {
                    return false;
                }
            }
        } else if isNull {
            return false;
        } else {
            let test: Datum;
            let recheck: bool;
            let mut de: GISTENTRY = std::mem::zeroed();

            gistdentryinit(
                giststate,
                (*key).sk_attno as c_int - 1,
                &mut de,
                datum,
                r,
                page,
                offset,
                false,
                isNull,
            );

            /*
             * Call the Consistent function to evaluate the test.  The
             * arguments are the index datum (as a GISTENTRY*), the comparison
             * datum, the comparison operator's strategy number and subtype
             * from pg_amop, and the recheck flag.
             *
             * (Presently there's no need to pass the subtype since it'll
             * always be zero, but might as well pass it for possible future
             * use.)
             *
             * We initialize the recheck flag to true (the safest assumption)
             * in case the Consistent function forgets to set it.
             */
            recheck = true;

            test = FunctionCall5Coll(
                &mut (*key).sk_func,
                (*key).sk_collation,
                PointerGetDatum(&de as *const _ as *const std::ffi::c_void),
                (*key).sk_argument,
                Int16GetDatum((*key).sk_strategy as i16),
                ObjectIdGetDatum((*key).sk_subtype),
                PointerGetDatum(&recheck as *const _ as *const std::ffi::c_void),
            );

            if !DatumGetBool(test) {
                return false;
            }
            *recheck_p |= recheck;
        }

        key = key.add(1);
        keySize -= 1;
    }

    /* OK, it passes --- now let's compute the distances */
    key = (*scan).orderByData;
    distance_p = (*so).distances;
    keySize = (*scan).numberOfOrderBys;
    while keySize > 0 {
        let datum: Datum;
        let mut isNull: bool = false;

        datum = index_getattr(
            tuple,
            (*key).sk_attno as c_int,
            (*giststate).leafTupdesc,
            &mut isNull,
        );

        if ((*key).sk_flags & SK_ISNULL) != 0 || isNull {
            /* Assume distance computes as null */
            (*distance_p).value = 0.0;
            (*distance_p).isnull = true;
        } else {
            let dist: Datum;
            let recheck: bool;
            let mut de: GISTENTRY = std::mem::zeroed();

            gistdentryinit(
                giststate,
                (*key).sk_attno as c_int - 1,
                &mut de,
                datum,
                r,
                page,
                offset,
                false,
                isNull,
            );

            /*
             * Call the Distance function to evaluate the distance.  The
             * arguments are the index datum (as a GISTENTRY*), the comparison
             * datum, the ordering operator's strategy number and subtype from
             * pg_amop, and the recheck flag.
             *
             * (Presently there's no need to pass the subtype since it'll
             * always be zero, but might as well pass it for possible future
             * use.)
             *
             * If the function sets the recheck flag, the returned distance is
             * a lower bound on the true distance and needs to be rechecked.
             * We initialize the flag to 'false'.  This flag was added in
             * version 9.5; distance functions written before that won't know
             * about the flag, but are expected to never be lossy.
             */
            recheck = false;
            dist = FunctionCall5Coll(
                &mut (*key).sk_func,
                (*key).sk_collation,
                PointerGetDatum(&de as *const _ as *const std::ffi::c_void),
                (*key).sk_argument,
                Int16GetDatum((*key).sk_strategy as i16),
                ObjectIdGetDatum((*key).sk_subtype),
                PointerGetDatum(&recheck as *const _ as *const std::ffi::c_void),
            );
            *recheck_distances_p |= recheck;
            (*distance_p).value = DatumGetFloat8(dist);
            (*distance_p).isnull = false;
        }

        key = key.add(1);
        distance_p = distance_p.add(1);
        keySize -= 1;
    }

    true
}

/*
 * Scan all items on the GiST index page identified by *pageItem, and insert
 * them into the queue (or directly to output areas)
 *
 * scan: index scan we are executing
 * pageItem: search queue item identifying an index page to scan
 * myDistances: distances array associated with pageItem, or NULL at the root
 * tbm: if not NULL, gistgetbitmap's output bitmap
 * ntids: if not NULL, gistgetbitmap's output tuple counter
 *
 * If tbm/ntids aren't NULL, we are doing an amgetbitmap scan, and heap
 * tuples should be reported directly into the bitmap.  If they are NULL,
 * we're doing a plain or ordered indexscan.  For a plain indexscan, heap
 * tuple TIDs are returned into so->pageData[].  For an ordered indexscan,
 * heap tuple TIDs are pushed into individual search queue items.  In an
 * index-only scan, reconstructed index tuples are returned along with the
 * TIDs.
 *
 * If we detect that the index page has split since we saw its downlink
 * in the parent, we push its new right sibling onto the queue so the
 * sibling will be processed next.
 */
unsafe fn gistScanPage(
    scan: IndexScanDesc,
    pageItem: *mut GISTSearchItem,
    myDistances: *mut IndexOrderByDistance,
    tbm: *mut TIDBitmap,
    ntids: *mut int64,
) {
    let so: GISTScanOpaque = (*scan).opaque as GISTScanOpaque;
    let giststate: *mut GISTSTATE = (*so).giststate;
    let r: Relation = (*scan).indexRelation;
    let buffer: Buffer;
    let page: Page;
    let opaque: GISTPageOpaque;
    let maxoff: OffsetNumber;
    let mut i: OffsetNumber;
    let mut oldcxt: MemoryContextT;

    Assert!(!GISTSearchItemIsHeap(&*pageItem));

    buffer = ReadBuffer((*scan).indexRelation, (*pageItem).blkno);
    LockBuffer(buffer, GIST_SHARE);
    PredicateLockPage(r, BufferGetBlockNumber(buffer), (*scan).xs_snapshot);
    gistcheckpage((*scan).indexRelation, buffer);
    page = BufferGetPage(buffer);
    opaque = GistPageGetOpaque(page);

    /*
     * Check if we need to follow the rightlink. We need to follow it if the
     * page was concurrently split since we visited the parent (in which case
     * parentlsn < nsn), or if the system crashed after a page split but
     * before the downlink was inserted into the parent.
     */
    if !XLogRecPtrIsInvalid((*pageItem).data.parentlsn)
        && (GistFollowRight(page) || (*pageItem).data.parentlsn < GistPageGetNSN(page))
        && (*opaque).rightlink != InvalidBlockNumber
    /* sanity check */
    {
        /* There was a page split, follow right link to add pages */
        let item: *mut GISTSearchItem;

        /* This can't happen when starting at the root */
        Assert!(!myDistances.is_null());

        oldcxt = MemoryContextSwitchTo((*so).queueCxt);

        /* Create new GISTSearchItem for the right sibling index page */
        item = palloc(SizeOfGISTSearchItem((*scan).numberOfOrderBys)) as *mut GISTSearchItem;
        (*item).blkno = (*opaque).rightlink;
        (*item).data.parentlsn = (*pageItem).data.parentlsn;

        /* Insert it into the queue using same distances as for this page */
        ptr::copy_nonoverlapping(
            myDistances,
            (*item).distances.as_mut_ptr(),
            (*scan).numberOfOrderBys as usize,
        );

        pairingheap_add((*so).queue, &mut (*item).phNode);

        MemoryContextSwitchTo(oldcxt);
    }

    /*
     * Check if the page was deleted after we saw the downlink. There's
     * nothing of interest on a deleted page. Note that we must do this after
     * checking the NSN for concurrent splits! It's possible that the page
     * originally contained some tuples that are visible to us, but was split
     * so that all the visible tuples were moved to another page, and then
     * this page was deleted.
     */
    if GistPageIsDeleted(page) {
        UnlockReleaseBuffer(buffer);
        return;
    }

    (*so).nPageData = 0;
    (*so).curPageData = 0;
    (*scan).xs_hitup = ptr::null_mut(); /* might point into pageDataCxt */
    if !(*so).pageDataCxt.is_null() {
        MemoryContextReset((*so).pageDataCxt);
    }

    /*
     * We save the LSN of the page as we read it, so that we know whether it
     * safe to apply LP_DEAD hints to the page later. This allows us to drop
     * the pin for MVCC scans, which allows vacuum to avoid blocking.
     */
    (*so).curPageLSN = BufferGetLSNAtomic(buffer);

    /*
     * check all tuples on page
     */
    maxoff = PageGetMaxOffsetNumber(page);
    i = FirstOffsetNumber;
    while i <= maxoff {
        let iid: ItemId = PageGetItemId(page, i);
        let it: IndexTuple;
        let match_: bool;
        let mut recheck: bool = false;
        let mut recheck_distances: bool = false;

        /*
         * If the scan specifies not to return killed tuples, then we treat a
         * killed tuple as not passing the qual.
         */
        if (*scan).ignore_killed_tuples && ItemIdIsDead(iid) {
            i = OffsetNumberNext(i);
            continue;
        }

        it = PageGetItem(page, iid) as IndexTuple;

        /*
         * Must call gistindex_keytest in tempCxt, and clean up any leftover
         * junk afterward.
         */
        oldcxt = MemoryContextSwitchTo((*(*so).giststate).tempCxt);

        match_ = gistindex_keytest(scan, it, page, i, &mut recheck, &mut recheck_distances);

        MemoryContextSwitchTo(oldcxt);
        MemoryContextReset((*(*so).giststate).tempCxt);

        /* Ignore tuple if it doesn't match */
        if !match_ {
            i = OffsetNumberNext(i);
            continue;
        }

        if !tbm.is_null() && GistPageIsLeaf(page) {
            /*
             * getbitmap scan, so just push heap tuple TIDs into the bitmap
             * without worrying about ordering
             */
            tbm_add_tuples(tbm, &mut (*it).t_tid, 1, recheck);
            *ntids += 1;
        } else if (*scan).numberOfOrderBys == 0 && GistPageIsLeaf(page) {
            /*
             * Non-ordered scan, so report tuples in so->pageData[]
             */
            let pd = (*so).pageData.add((*so).nPageData as usize);
            (*pd).heapPtr = ptr::read(&(*it).t_tid);
            (*pd).recheck = recheck;
            (*pd).offnum = i;

            /*
             * In an index-only scan, also fetch the data from the tuple.  The
             * reconstructed tuples are stored in pageDataCxt.
             */
            if (*scan).xs_want_itup {
                oldcxt = MemoryContextSwitchTo((*so).pageDataCxt);
                (*pd).recontup = gistFetchTuple(giststate, r, it);
                MemoryContextSwitchTo(oldcxt);
            }
            (*so).nPageData += 1;
        } else {
            /*
             * Must push item into search queue.  We get here for any lower
             * index page, and also for heap tuples if doing an ordered
             * search.
             */
            let item: *mut GISTSearchItem;
            let nOrderBys: c_int = (*scan).numberOfOrderBys;

            oldcxt = MemoryContextSwitchTo((*so).queueCxt);

            /* Create new GISTSearchItem for this item */
            item = palloc(SizeOfGISTSearchItem((*scan).numberOfOrderBys)) as *mut GISTSearchItem;

            if GistPageIsLeaf(page) {
                /* Creating heap-tuple GISTSearchItem */
                (*item).blkno = InvalidBlockNumber;
                (*item).data.heap.heapPtr = ptr::read(&(*it).t_tid);
                (*item).data.heap.recheck = recheck;
                (*item).data.heap.recheckDistances = recheck_distances;

                /*
                 * In an index-only scan, also fetch the data from the tuple.
                 */
                if (*scan).xs_want_itup {
                    (*item).data.heap.recontup = gistFetchTuple(giststate, r, it);
                }
            } else {
                /* Creating index-page GISTSearchItem */
                (*item).blkno = ItemPointerGetBlockNumber(&mut (*it).t_tid);

                /*
                 * LSN of current page is lsn of parent page for child. We
                 * only have a shared lock, so we need to get the LSN
                 * atomically.
                 */
                (*item).data.parentlsn = BufferGetLSNAtomic(buffer);
            }

            /* Insert it into the queue using new distance data */
            ptr::copy_nonoverlapping(
                (*so).distances,
                (*item).distances.as_mut_ptr(),
                nOrderBys as usize,
            );

            pairingheap_add((*so).queue, &mut (*item).phNode);

            MemoryContextSwitchTo(oldcxt);
        }

        i = OffsetNumberNext(i);
    }

    UnlockReleaseBuffer(buffer);
}

/*
 * Extract next item (in order) from search queue
 *
 * Returns a GISTSearchItem or NULL.  Caller must pfree item when done with it.
 */
unsafe fn getNextGISTSearchItem(so: GISTScanOpaque) -> *mut GISTSearchItem {
    let item: *mut GISTSearchItem;

    if !pairingheap_is_empty((*so).queue) {
        item = pairingheap_remove_first((*so).queue) as *mut GISTSearchItem;
    } else {
        /* Done when both heaps are empty */
        item = ptr::null_mut();
    }

    /* Return item; caller is responsible to pfree it */
    item
}

/*
 * Fetch next heap tuple in an ordered search
 */
unsafe fn getNextNearest(scan: IndexScanDesc) -> bool {
    let so: GISTScanOpaque = (*scan).opaque as GISTScanOpaque;
    let mut res: bool = false;

    if !(*scan).xs_hitup.is_null() {
        /* free previously returned tuple */
        pfree((*scan).xs_hitup);
        (*scan).xs_hitup = ptr::null_mut();
    }

    loop {
        let item: *mut GISTSearchItem = getNextGISTSearchItem(so);

        if item.is_null() {
            break;
        }

        if GISTSearchItemIsHeap(&*item) {
            /* found a heap item at currently minimal distance */
            (*scan).xs_heaptid = ptr::read(&(*item).data.heap.heapPtr);
            (*scan).xs_recheck = (*item).data.heap.recheck;

            index_store_float8_orderby_distances(
                scan,
                (*so).orderByTypes,
                (*item).distances.as_mut_ptr(),
                (*item).data.heap.recheckDistances,
            );

            /* in an index-only scan, also return the reconstructed tuple. */
            if (*scan).xs_want_itup {
                (*scan).xs_hitup = (*item).data.heap.recontup;
            }
            res = true;
        } else {
            /* visit an index page, extract its items into queue */
            CHECK_FOR_INTERRUPTS();

            gistScanPage(scan, item, (*item).distances.as_mut_ptr(), ptr::null_mut(), ptr::null_mut());
        }

        pfree(item as *mut std::ffi::c_void);

        if res {
            break;
        }
    }

    res
}

/*
 * gistgettuple() -- Get the next tuple in the scan
 */
pub unsafe fn gistgettuple(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    let so: GISTScanOpaque = (*scan).opaque as GISTScanOpaque;

    if dir != ForwardScanDirection {
        elog!(ERROR, "GiST only supports forward scan direction");
    }

    if !(*so).qual_ok {
        return false;
    }

    if (*so).firstCall {
        /* Begin the scan by processing the root page */
        let mut fakeItem: GISTSearchItem = std::mem::zeroed();

        pgstat_count_index_scan((*scan).indexRelation);
        if !(*scan).instrument.is_null() {
            (*(*scan).instrument).nsearches += 1;
        }

        (*so).firstCall = false;
        (*so).curPageData = 0;
        (*so).nPageData = 0;
        (*scan).xs_hitup = ptr::null_mut();
        if !(*so).pageDataCxt.is_null() {
            MemoryContextReset((*so).pageDataCxt);
        }

        fakeItem.blkno = GIST_ROOT_BLKNO;
        ptr::write_bytes(
            &mut fakeItem.data.parentlsn as *mut GistNSN,
            0,
            1,
        );
        gistScanPage(scan, &mut fakeItem, ptr::null_mut(), ptr::null_mut(), ptr::null_mut());
    }

    if (*scan).numberOfOrderBys > 0 {
        /* Must fetch tuples in strict distance order */
        return getNextNearest(scan);
    } else {
        /* Fetch tuples index-page-at-a-time */
        loop {
            if (*so).curPageData < (*so).nPageData {
                if (*scan).kill_prior_tuple && (*so).curPageData > 0 {
                    if (*so).killedItems.is_null() {
                        let oldCxt: MemoryContextT =
                            MemoryContextSwitchTo((*(*so).giststate).scanCxt);

                        (*so).killedItems = palloc(
                            (MaxIndexTuplesPerPage as usize)
                                * std::mem::size_of::<OffsetNumber>(),
                        ) as *mut OffsetNumber;

                        MemoryContextSwitchTo(oldCxt);
                    }
                    if (*so).numKilled < MaxIndexTuplesPerPage {
                        *(*so).killedItems.add((*so).numKilled as usize) =
                            (*(*so).pageData.add(((*so).curPageData - 1) as usize)).offnum;
                        (*so).numKilled += 1;
                    }
                }
                /* continuing to return tuples from a leaf page */
                let pd = (*so).pageData.add((*so).curPageData as usize);
                (*scan).xs_heaptid = ptr::read(&(*pd).heapPtr);
                (*scan).xs_recheck = (*pd).recheck;

                /* in an index-only scan, also return the reconstructed tuple */
                if (*scan).xs_want_itup {
                    (*scan).xs_hitup = (*pd).recontup;
                }

                (*so).curPageData += 1;

                return true;
            }

            /*
             * Check the last returned tuple and add it to killedItems if
             * necessary
             */
            if (*scan).kill_prior_tuple
                && (*so).curPageData > 0
                && (*so).curPageData == (*so).nPageData
            {
                if (*so).killedItems.is_null() {
                    let oldCxt: MemoryContextT =
                        MemoryContextSwitchTo((*(*so).giststate).scanCxt);

                    (*so).killedItems = palloc(
                        (MaxIndexTuplesPerPage as usize) * std::mem::size_of::<OffsetNumber>(),
                    ) as *mut OffsetNumber;

                    MemoryContextSwitchTo(oldCxt);
                }
                if (*so).numKilled < MaxIndexTuplesPerPage {
                    *(*so).killedItems.add((*so).numKilled as usize) =
                        (*(*so).pageData.add(((*so).curPageData - 1) as usize)).offnum;
                    (*so).numKilled += 1;
                }
            }
            /* find and process the next index page */
            loop {
                let item: *mut GISTSearchItem;

                if ((*so).curBlkno != InvalidBlockNumber) && ((*so).numKilled > 0) {
                    gistkillitems(scan);
                }

                item = getNextGISTSearchItem(so);

                if item.is_null() {
                    return false;
                }

                CHECK_FOR_INTERRUPTS();

                /* save current item BlockNumber for next gistkillitems() call */
                (*so).curBlkno = (*item).blkno;

                /*
                 * While scanning a leaf page, ItemPointers of matching heap
                 * tuples are stored in so->pageData.  If there are any on
                 * this page, we fall out of the inner "do" and loop around to
                 * return them.
                 */
                gistScanPage(scan, item, (*item).distances.as_mut_ptr(), ptr::null_mut(), ptr::null_mut());

                pfree(item as *mut std::ffi::c_void);

                if (*so).nPageData != 0 {
                    break;
                }
            }
        }
    }
}

/*
 * gistgetbitmap() -- Get a bitmap of all heap tuple locations
 */
pub unsafe fn gistgetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 {
    let so: GISTScanOpaque = (*scan).opaque as GISTScanOpaque;
    let mut ntids: int64 = 0;
    let mut fakeItem: GISTSearchItem = std::mem::zeroed();

    if !(*so).qual_ok {
        return 0;
    }

    pgstat_count_index_scan((*scan).indexRelation);
    if !(*scan).instrument.is_null() {
        (*(*scan).instrument).nsearches += 1;
    }

    /* Begin the scan by processing the root page */
    (*so).curPageData = 0;
    (*so).nPageData = 0;
    (*scan).xs_hitup = ptr::null_mut();
    if !(*so).pageDataCxt.is_null() {
        MemoryContextReset((*so).pageDataCxt);
    }

    fakeItem.blkno = GIST_ROOT_BLKNO;
    ptr::write_bytes(&mut fakeItem.data.parentlsn as *mut GistNSN, 0, 1);
    gistScanPage(scan, &mut fakeItem, ptr::null_mut(), tbm, &mut ntids);

    /*
     * While scanning a leaf page, ItemPointers of matching heap tuples will
     * be stored directly into tbm, so we don't need to deal with them here.
     */
    loop {
        let item: *mut GISTSearchItem = getNextGISTSearchItem(so);

        if item.is_null() {
            break;
        }

        CHECK_FOR_INTERRUPTS();

        gistScanPage(scan, item, (*item).distances.as_mut_ptr(), tbm, &mut ntids);

        pfree(item as *mut std::ffi::c_void);
    }

    ntids
}

/*
 * Can we do index-only scans on the given index column?
 *
 * Opclasses that implement a fetch function support index-only scans.
 * Opclasses without compression functions also support index-only scans.
 * Included attributes always can be fetched for index-only scans.
 */
pub unsafe fn gistcanreturn(index: Relation, attno: c_int) -> bool {
    if attno > IndexRelationGetNumberOfKeyAttributes(index)
        || OidIsValid(index_getprocid(index, attno, GIST_FETCH_PROC))
        || !OidIsValid(index_getprocid(index, attno, GIST_COMPRESS_PROC))
    {
        true
    } else {
        false
    }
}
