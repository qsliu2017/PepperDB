/*-------------------------------------------------------------------------
 *
 * gist.c
 *	  interface routines for the postgres GiST index access method.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/gist/gist.c
 *
 *-------------------------------------------------------------------------
 */
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;

use crate::{ereport, errmsg, list_make1, makeNode, Assert};

// --- access/gist_private.h (REAL) -------------------------------------------
use crate::access::gist::gist_private::{
    GistTupleIsInvalid, GistTupleSetValid, GISTInsertStack, GISTInsertState, GISTPageSplitInfo,
    GISTSTATE, GistSplitVector, SplitPageLayout, GIST_EXCLUSIVE, GIST_MAX_SPLIT_PAGES,
    GIST_ROOT_BLKNO, GIST_SHARE, GIST_UNLOCK,
};
// gistutil.c / gistsplit.c / gistget.c / gistvalidate.c / gistvacuum.c /
// gistbuild.c entry points called from this file. Declared (as plain unsafe fn)
// in gist_private; imported here.
use crate::access::gist::gist_private::{
    gistSplit, gistSplitByKey, gistFormTuple, gistGetFakeLSN, gistXLogDelete, gistXLogSplit,
    gistXLogUpdate, gistcheckpage, gistchoose, gistextractpage, gistfillbuffer, gistfillitupvec,
    gistfitpage, gistgetadjusted, gistjoinvector, gistnospace, GISTInitBuffer,
};

// --- access/gistscan.h (REAL) ----------------------------------------------
// (gistadjustscans is not used here)

// --- access/amapi.h (REAL) --------------------------------------------------
use crate::access::index::amapi::{
    IndexAMProperty, IndexAmRoutine, IndexBuildResult, IndexBulkDeleteCallback,
    IndexBulkDeleteResult, IndexPath, IndexScanDesc, IndexUniqueCheck, IndexVacuumInfo,
    PlannerInfo, ScanKey, TIDBitmap,
};
// The amapi function-pointer table types `indexInfo` as `*mut c_void` (amapi's
// IndexInfo alias); the real struct with ii_AmCache/ii_Context lives in
// nodes/execnodes.h. Use the concrete struct in the function bodies and bridge
// in the extern "C" callback wrappers.
use crate::nodes::execnodes::IndexInfo;
use crate::nodes::nodes::{Cost, Selectivity};
use crate::nodes::pg_list::{
    lappend, lcons, linitial, list_delete_first, list_length, list_nth, lsecond, List, NIL,
};
use crate::nodes::plannodes::ScanDirection;

// --- commands/vacuum.h (REAL) -----------------------------------------------
use crate::commands::vacuumparallel::{
    VACUUM_OPTION_PARALLEL_BULKDEL, VACUUM_OPTION_PARALLEL_COND_CLEANUP,
};

// --- miscadmin.h (REAL) -----------------------------------------------------
use crate::miscadmin::{check_stack_depth, END_CRIT_SECTION, START_CRIT_SECTION};

// --- catalog/pg_collation.h (REAL) ------------------------------------------
use crate::catalog::pg_collation::DEFAULT_COLLATION_OID;

// --- storage/predicate.h (REAL) ---------------------------------------------
use crate::storage::lmgr::predicate::{CheckForSerializableConflictIn, PredicateLockPageSplit};

// --- storage/bufmgr.h (REAL) ------------------------------------------------
use crate::storage::buffer::bufmgr::{
    BufferGetBlockNumber, BufferGetLSNAtomic, BufferGetPage, ExtendBufferedRel, LockBuffer,
    MarkBufferDirty, ReadBuffer, ReleaseBuffer, UnlockReleaseBuffer, BMR_REL,
};

// --- storage/bufpage.h (REAL) -----------------------------------------------
use crate::storage::bufpage::{
    Item, Page, PageAddItem, PageGetItem, PageGetItemId, PageGetLSN, PageGetMaxOffsetNumber,
    PageGetTempPageCopySpecial, PageIndexMultiDelete, PageIndexTupleDelete,
    PageIndexTupleOverwrite, PageRestoreTempPage, PageSetLSN,
};

// --- storage/block.h (REAL) -------------------------------------------------
use crate::storage::block::BlockNumber;

// --- storage/buf.h (REAL) ---------------------------------------------------
use crate::storage::buf::Buffer;

// --- storage/itemid.h (REAL) ------------------------------------------------
use crate::storage::itemid::{ItemId, ItemIdIsDead};

// --- storage/itemptr.h (REAL) -----------------------------------------------
use crate::storage::itemptr::{
    ItemPointerData, ItemPointerEquals, ItemPointerGetBlockNumber, ItemPointerSetBlockNumber,
};

// --- storage/off.h (REAL) ---------------------------------------------------
use crate::storage::off::{
    OffsetNumber, OffsetNumberIsValid, OffsetNumberNext, FirstOffsetNumber, InvalidOffsetNumber,
};

// --- access/common/indextuple.h (REAL) --------------------------------------
use crate::access::common::indextuple::{CopyIndexTuple, IndexTuple, IndexTupleData};

// --- access/common/tupdesc.h (REAL) -----------------------------------------
use crate::access::common::tupdesc::CreateTupleDescTruncatedCopy;

// --- access/index/indexam.h (REAL) ------------------------------------------
use crate::access::index::indexam::{index_getprocid, index_getprocinfo};

// --- access/attnum.h (REAL) -------------------------------------------------
use crate::access::attnum::AttrNumber;

// --- access/index/genam.h (REAL) --------------------------------------------
use crate::access::index::genam::index_compute_xid_horizon_for_tuples;

// --- access/transam/xlogdefs.h (REAL) ---------------------------------------
use crate::access::transam::xlogdefs::{XLogRecPtr, XLogRecPtrIsInvalid};

// --- access/transam/xloginsert.h (REAL) -------------------------------------
use crate::access::transam::xloginsert::{log_newpage_buffer, XLogEnsureRecordSpace};

// --- access/transam.h (REAL) ------------------------------------------------
use crate::access::transam::xact::InvalidTransactionId;

// --- utils/fmgr.h (REAL) ----------------------------------------------------
use crate::utils::fmgr::{fmgr_info_copy, FunctionCallInfo, PointerGetDatum};

// --- utils/memutils.h (REAL) ------------------------------------------------
use crate::utils::memutils::{MemoryContextDelete, MemoryContextReset};

// --- utils/rel.h (REAL) -----------------------------------------------------
use crate::utils::rel::{Relation, RelationGetRelationName};

use core::ffi::CStr;

// ===========================================================================
// Locally-stubbed types / consts / helpers (homes not yet ported in their
// canonical module). Mirror the sibling gist files' conventions; dedup later.
// ===========================================================================

// access/gist.h: GistNSN is just an LSN. TODO: dedup once access/gist.h ported.
type GistNSN = XLogRecPtr;

// access/gist.h: GiST opclass support-function numbers and counts.
// TODO: dedup once access/gist.h is ported (also in gistvalidate.rs).
const GIST_CONSISTENT_PROC: c_int = 1;
const GIST_UNION_PROC: c_int = 2;
const GIST_COMPRESS_PROC: c_int = 3;
const GIST_DECOMPRESS_PROC: c_int = 4;
const GIST_PENALTY_PROC: c_int = 5;
const GIST_PICKSPLIT_PROC: c_int = 6;
const GIST_EQUAL_PROC: c_int = 7;
const GIST_DISTANCE_PROC: c_int = 8;
const GIST_FETCH_PROC: c_int = 9;
const GIST_OPTIONS_PROC: c_int = 10;
const GISTNProcs: c_int = 12;

// access/gist.h: GiST page flag bits. TODO: dedup once access/gist.h ported.
const F_LEAF: uint16 = 1 << 0;
const F_DELETED: uint16 = 1 << 1;
const F_TUPLES_DELETED: uint16 = 1 << 2;
const F_FOLLOW_RIGHT: uint16 = 1 << 3;
const F_HAS_GARBAGE: uint16 = 1 << 4;

// access/gist.h: #define GistBuildLSN ((XLogRecPtr) 1)
const GistBuildLSN: XLogRecPtr = 1;

// access/gist.h: special-area struct at the end of every GiST index page.
// TODO: dedup once access/gist.h is ported (also in gistutil.rs).
#[repr(C)]
struct GISTPageOpaqueData {
    nsn: GistNSN,           /* this page's update LSN */
    rightlink: BlockNumber, /* next page if any */
    flags: uint16,          /* see bit definitions above */
    gist_page_id: uint16,   /* for identification of GiST indexes */
}
type GISTPageOpaque = *mut GISTPageOpaqueData;

// #define GistPageGetOpaque(page) ((GISTPageOpaque) PageGetSpecialPointer(page))
#[inline]
unsafe fn GistPageGetOpaque(page: Page) -> GISTPageOpaque {
    PageGetSpecialPointer(page) as GISTPageOpaque
}

// #define GistPageIsLeaf(page) (GistPageGetOpaque(page)->flags & F_LEAF)
#[inline]
unsafe fn GistPageIsLeaf(page: Page) -> bool {
    ((*GistPageGetOpaque(page)).flags & F_LEAF) != 0
}

// #define GistPageIsDeleted(page) (GistPageGetOpaque(page)->flags & F_DELETED)
#[inline]
unsafe fn GistPageIsDeleted(page: Page) -> bool {
    ((*GistPageGetOpaque(page)).flags & F_DELETED) != 0
}

// #define GistPageHasGarbage(page) (GistPageGetOpaque(page)->flags & F_HAS_GARBAGE)
#[inline]
unsafe fn GistPageHasGarbage(page: Page) -> bool {
    ((*GistPageGetOpaque(page)).flags & F_HAS_GARBAGE) != 0
}

// #define GistClearPageHasGarbage(page) (...flags &= ~F_HAS_GARBAGE)
#[inline]
unsafe fn GistClearPageHasGarbage(page: Page) {
    (*GistPageGetOpaque(page)).flags &= !F_HAS_GARBAGE;
}

// #define GistFollowRight(page) (GistPageGetOpaque(page)->flags & F_FOLLOW_RIGHT)
#[inline]
unsafe fn GistFollowRight(page: Page) -> bool {
    ((*GistPageGetOpaque(page)).flags & F_FOLLOW_RIGHT) != 0
}

// #define GistMarkFollowRight(page) (...flags |= F_FOLLOW_RIGHT)
#[inline]
unsafe fn GistMarkFollowRight(page: Page) {
    (*GistPageGetOpaque(page)).flags |= F_FOLLOW_RIGHT;
}

// #define GistClearFollowRight(page) (...flags &= ~F_FOLLOW_RIGHT)
#[inline]
unsafe fn GistClearFollowRight(page: Page) {
    (*GistPageGetOpaque(page)).flags &= !F_FOLLOW_RIGHT;
}

// #define GistPageGetNSN(page) (PageXLogRecPtrGet(GistPageGetOpaque(page)->nsn))
#[inline]
unsafe fn GistPageGetNSN(page: Page) -> GistNSN {
    (*GistPageGetOpaque(page)).nsn
}

// #define GistPageSetNSN(page, val) (PageXLogRecPtrSet(GistPageGetOpaque(page)->nsn, val))
#[inline]
unsafe fn GistPageSetNSN(page: Page, val: GistNSN) {
    (*GistPageGetOpaque(page)).nsn = val;
}

// storage/bufpage.h: PageGetSpecialPointer. TODO: dedup once exported.
unsafe fn PageGetSpecialPointer(page: Page) -> *mut c_char {
    crate::storage::bufpage::PageGetSpecialPointer(page)
}

// storage/bufmgr.h: BufferIsValid (private in bufmgr.rs). TODO: dedup.
const InvalidBuffer: Buffer = 0;
#[inline]
fn BufferIsValid(bufnum: Buffer) -> bool {
    bufnum != InvalidBuffer
}

// storage/block.h: InvalidBlockNumber. TODO: dedup once exported.
const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;

// access/htup.h: XLogStandbyInfoActive (xlog.h). TODO: dedup once ported.
unsafe fn XLogStandbyInfoActive() -> bool {
    // TODO(pg-port): real definition lives in access/xlog.h.
    false
}

// access/itup.h: IndexTupleSize macro. TODO: dedup once itup.h exports it.
#[inline]
unsafe fn IndexTupleSize(itup: IndexTuple) -> Size {
    const INDEX_SIZE_MASK: u16 = 0x1FFF;
    ((*itup).t_info & INDEX_SIZE_MASK) as Size
}

// utils/rel.h: RelationNeedsWAL. TODO: dedup once rel.h exports it.
unsafe fn RelationNeedsWAL(relation: Relation) -> bool {
    // TODO(pg-port): full macro consults rd_rel->relpersistence and rd_*Subid.
    crate::access::gin::gininsert::RelationNeedsWAL(relation)
}

// utils/rel.h: IndexRelationGetNumberOfKeyAttributes. TODO: dedup once exported.
unsafe fn IndexRelationGetNumberOfKeyAttributes(relation: Relation) -> c_int {
    crate::access::gin::gininsert::IndexRelationGetNumberOfKeyAttributes(relation)
}

// access/gist.h: GiSTPageSize. TODO: dedup once access/gist.h is ported.
#[inline]
fn GiSTPageSize() -> Size {
    crate::access::gist::gist_private::GiSTPageSize()
}

// catalog/index.h: INDEX_MAX_KEYS. TODO: dedup once exported.
use crate::access::common::indextuple::INDEX_MAX_KEYS;

// ===========================================================================
// Dependencies in OTHER .c files, not yet ported in their canonical modules.
// Stubbed with TODO(pg-port) bodies.
// ===========================================================================

// gistbuild.c: gistbuild. TODO(pg-port): import once gistbuild.c is ported.
unsafe fn gistbuild(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult {
    let _ = (heap, index, indexInfo);
    unimplemented!() // TODO(pg-port)
}

// gistvacuum.c: gistbulkdelete / gistvacuumcleanup. TODO(pg-port): import once ported.
unsafe fn gistbulkdelete(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    let _ = (info, stats, callback, callback_state);
    unimplemented!() // TODO(pg-port)
}
unsafe fn gistvacuumcleanup(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    let _ = (info, stats);
    unimplemented!() // TODO(pg-port)
}

// gistget.c: gistcanreturn / gistgettuple / gistgetbitmap. TODO(pg-port).
unsafe fn gistcanreturn(index: Relation, attno: c_int) -> bool {
    let _ = (index, attno);
    unimplemented!() // TODO(pg-port)
}
unsafe fn gistgettuple(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    let _ = (scan, dir);
    unimplemented!() // TODO(pg-port)
}
unsafe fn gistgetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 {
    let _ = (scan, tbm);
    unimplemented!() // TODO(pg-port)
}

// selfuncs.c: gistcostestimate. TODO(pg-port): import once ported.
unsafe fn gistcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    let _ = (
        root,
        path,
        loop_count,
        indexStartupCost,
        indexTotalCost,
        indexSelectivity,
        indexCorrelation,
        indexPages,
    );
    unimplemented!() // TODO(pg-port)
}

// gistutil.c: gistoptions / gistproperty. TODO(pg-port): import once ported.
unsafe fn gistoptions(reloptions: Datum, validate: bool) -> *mut bytea {
    let _ = (reloptions, validate);
    unimplemented!() // TODO(pg-port)
}
unsafe fn gistproperty(
    index_oid: Oid,
    attno: c_int,
    prop: IndexAMProperty,
    propname: *const c_char,
    res: *mut bool,
    isnull: *mut bool,
) -> bool {
    let _ = (index_oid, attno, prop, propname, res, isnull);
    unimplemented!() // TODO(pg-port)
}

// gistvalidate.c: gistvalidate / gistadjustmembers / gisttranslatecmptype. TODO(pg-port).
unsafe fn gistvalidate(opclassoid: Oid) -> bool {
    let _ = opclassoid;
    unimplemented!() // TODO(pg-port)
}
unsafe fn gistadjustmembers(
    opfamilyoid: Oid,
    opclassoid: Oid,
    operators: *mut List,
    functions: *mut List,
) {
    let _ = (opfamilyoid, opclassoid, operators, functions);
    unimplemented!() // TODO(pg-port)
}
unsafe fn gisttranslatecmptype(cmptype: c_int, opfamily: Oid) -> uint16 {
    let _ = (cmptype, opfamily);
    unimplemented!() // TODO(pg-port)
}

// gistscan.c: gistbeginscan / gistrescan / gistendscan. TODO(pg-port).
unsafe fn gistbeginscan(r: Relation, nkeys: c_int, norderbys: c_int) -> IndexScanDesc {
    let _ = (r, nkeys, norderbys);
    unimplemented!() // TODO(pg-port)
}
unsafe fn gistrescan(
    scan: IndexScanDesc,
    key: ScanKey,
    nkeys: c_int,
    orderbys: ScanKey,
    norderbys: c_int,
) {
    let _ = (scan, key, nkeys, orderbys, norderbys);
    unimplemented!() // TODO(pg-port)
}
unsafe fn gistendscan(scan: IndexScanDesc) {
    let _ = scan;
    unimplemented!() // TODO(pg-port)
}

// gist.c (forward decls; defined later in this file)

// #define ROTATEDIST(d) do { ... } while(0)
//
// Allocates a new SplitPageLayout, links it at the head of the chain `$d`, and
// reassigns `$d` to it. Modelled as a statement macro mutating its lvalue arg.
macro_rules! ROTATEDIST {
    ($d:expr) => {{
        let tmp: *mut SplitPageLayout =
            palloc0(core::mem::size_of::<SplitPageLayout>()) as *mut SplitPageLayout;
        (*tmp).block.blkno = InvalidBlockNumber;
        (*tmp).buffer = InvalidBuffer;
        (*tmp).next = $d;
        $d = tmp;
    }};
}

/*
 * GiST handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
pub unsafe fn gisthandler(fcinfo: FunctionCallInfo) -> Datum {
    let amroutine: *mut IndexAmRoutine = makeNode!(IndexAmRoutine, T_IndexAmRoutine);

    (*amroutine).amstrategies = 0;
    (*amroutine).amsupport = GISTNProcs as uint16;
    (*amroutine).amoptsprocnum = GIST_OPTIONS_PROC as uint16;
    (*amroutine).amcanorder = false;
    (*amroutine).amcanorderbyop = true;
    (*amroutine).amcanhash = false;
    (*amroutine).amconsistentequality = false;
    (*amroutine).amconsistentordering = false;
    (*amroutine).amcanbackward = false;
    (*amroutine).amcanunique = false;
    (*amroutine).amcanmulticol = true;
    (*amroutine).amoptionalkey = true;
    (*amroutine).amsearcharray = false;
    (*amroutine).amsearchnulls = true;
    (*amroutine).amstorage = true;
    (*amroutine).amclusterable = true;
    (*amroutine).ampredlocks = true;
    (*amroutine).amcanparallel = false;
    (*amroutine).amcanbuildparallel = false;
    (*amroutine).amcaninclude = true;
    (*amroutine).amusemaintenanceworkmem = false;
    (*amroutine).amsummarizing = false;
    (*amroutine).amparallelvacuumoptions =
        VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;
    (*amroutine).amkeytype = InvalidOid;

    (*amroutine).ambuild = Some(gistbuild_cb);
    (*amroutine).ambuildempty = Some(gistbuildempty_cb);
    (*amroutine).aminsert = Some(gistinsert_cb);
    (*amroutine).aminsertcleanup = None;
    (*amroutine).ambulkdelete = Some(gistbulkdelete_cb);
    (*amroutine).amvacuumcleanup = Some(gistvacuumcleanup_cb);
    (*amroutine).amcanreturn = Some(gistcanreturn_cb);
    (*amroutine).amcostestimate = Some(gistcostestimate_cb);
    (*amroutine).amgettreeheight = None;
    (*amroutine).amoptions = Some(gistoptions_cb);
    (*amroutine).amproperty = Some(gistproperty_cb);
    (*amroutine).ambuildphasename = None;
    (*amroutine).amvalidate = Some(gistvalidate_cb);
    (*amroutine).amadjustmembers = Some(gistadjustmembers_cb);
    (*amroutine).ambeginscan = Some(gistbeginscan_cb);
    (*amroutine).amrescan = Some(gistrescan_cb);
    (*amroutine).amgettuple = Some(gistgettuple_cb);
    (*amroutine).amgetbitmap = Some(gistgetbitmap_cb);
    (*amroutine).amendscan = Some(gistendscan_cb);
    (*amroutine).ammarkpos = None;
    (*amroutine).amrestrpos = None;
    (*amroutine).amestimateparallelscan = None;
    (*amroutine).aminitparallelscan = None;
    (*amroutine).amparallelrescan = None;
    (*amroutine).amtranslatestrategy = None;
    (*amroutine).amtranslatecmptype = Some(gisttranslatecmptype_cb);

    PG_RETURN_POINTER(amroutine)
}

// PG_RETURN_POINTER. TODO: dedup once fmgr.h exports it.
#[inline]
unsafe fn PG_RETURN_POINTER<T>(x: *mut T) -> Datum {
    PointerGetDatum(x as *mut c_void)
}

// extern "C" callback wrappers bridging the plain `unsafe fn` AM entry points to
// the IndexAmRoutine function-pointer ABI, matching the sibling AM files.
unsafe extern "C" fn gistbuild_cb(
    heap: Relation,
    index: Relation,
    indexInfo: *mut c_void,
) -> *mut IndexBuildResult {
    gistbuild(heap, index, indexInfo as *mut IndexInfo)
}
unsafe extern "C" fn gistbuildempty_cb(index: Relation) {
    gistbuildempty(index)
}
unsafe extern "C" fn gistinsert_cb(
    r: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    ht_ctid: *mut ItemPointerData,
    heapRel: Relation,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
    indexInfo: *mut c_void,
) -> bool {
    gistinsert(
        r,
        values,
        isnull,
        ht_ctid,
        heapRel,
        checkUnique,
        indexUnchanged,
        indexInfo as *mut IndexInfo,
    )
}
unsafe extern "C" fn gistbulkdelete_cb(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    gistbulkdelete(info, stats, callback, callback_state)
}
unsafe extern "C" fn gistvacuumcleanup_cb(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    gistvacuumcleanup(info, stats)
}
unsafe extern "C" fn gistcanreturn_cb(index: Relation, attno: c_int) -> bool {
    gistcanreturn(index, attno)
}
unsafe extern "C" fn gistcostestimate_cb(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    gistcostestimate(
        root,
        path,
        loop_count,
        indexStartupCost,
        indexTotalCost,
        indexSelectivity,
        indexCorrelation,
        indexPages,
    )
}
unsafe extern "C" fn gistoptions_cb(reloptions: Datum, validate: bool) -> *mut bytea {
    gistoptions(reloptions, validate)
}
unsafe extern "C" fn gistproperty_cb(
    index_oid: Oid,
    attno: c_int,
    prop: IndexAMProperty,
    propname: *const c_char,
    res: *mut bool,
    isnull: *mut bool,
) -> bool {
    gistproperty(index_oid, attno, prop, propname, res, isnull)
}
unsafe extern "C" fn gistvalidate_cb(opclassoid: Oid) -> bool {
    gistvalidate(opclassoid)
}
unsafe extern "C" fn gistadjustmembers_cb(
    opfamilyoid: Oid,
    opclassoid: Oid,
    operators: *mut List,
    functions: *mut List,
) {
    gistadjustmembers(opfamilyoid, opclassoid, operators, functions)
}
unsafe extern "C" fn gistbeginscan_cb(r: Relation, nkeys: c_int, norderbys: c_int) -> IndexScanDesc {
    gistbeginscan(r, nkeys, norderbys)
}
unsafe extern "C" fn gistrescan_cb(
    scan: IndexScanDesc,
    key: ScanKey,
    nkeys: c_int,
    orderbys: ScanKey,
    norderbys: c_int,
) {
    gistrescan(scan, key, nkeys, orderbys, norderbys)
}
unsafe extern "C" fn gistgettuple_cb(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    gistgettuple(scan, dir)
}
unsafe extern "C" fn gistgetbitmap_cb(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 {
    gistgetbitmap(scan, tbm)
}
unsafe extern "C" fn gistendscan_cb(scan: IndexScanDesc) {
    gistendscan(scan)
}
unsafe extern "C" fn gisttranslatecmptype_cb(cmptype: c_int, opfamily: Oid) -> uint16 {
    gisttranslatecmptype(cmptype, opfamily)
}

/*
 * Create and return a temporary memory context for use by GiST. We
 * _always_ invoke user-provided methods in a temporary memory
 * context, so that memory leaks in those functions cannot cause
 * problems. Also, we use some additional temporary contexts in the
 * GiST code itself, to avoid the need to do some awkward manual
 * memory management.
 */
pub unsafe fn createTempGistContext() -> MemoryContext {
    AllocSetContextCreate(
        CurrentMemoryContext,
        c"GiST temporary context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    )
}

/*
 *	gistbuildempty() -- build an empty gist index in the initialization fork
 */
pub unsafe fn gistbuildempty(index: Relation) {
    let buffer: Buffer;

    /* Initialize the root page */
    buffer = ExtendBufferedRel(
        BMR_REL(index),
        INIT_FORKNUM,
        null_mut(),
        EB_SKIP_EXTENSION_LOCK | EB_LOCK_FIRST,
    );

    /* Initialize and xlog buffer */
    START_CRIT_SECTION();
    GISTInitBuffer(buffer, F_LEAF as uint32);
    MarkBufferDirty(buffer);
    log_newpage_buffer(buffer, true);
    END_CRIT_SECTION();

    /* Unlock and release the buffer */
    UnlockReleaseBuffer(buffer);
}

// storage/bufmgr.h: ExtendBufferedRel flags / fork. TODO: dedup once exported.
use crate::access::transam::xlogutils::{EB_SKIP_EXTENSION_LOCK, INIT_FORKNUM};
use crate::access::spgist::spgutils::EB_LOCK_FIRST;

/*
 *	gistinsert -- wrapper for GiST tuple insertion.
 *
 *	  This is the public interface routine for tuple insertion in GiSTs.
 *	  It doesn't do any work; just locks the relation and passes the buck.
 */
pub unsafe fn gistinsert(
    r: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    ht_ctid: *mut ItemPointerData,
    heapRel: Relation,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
    indexInfo: *mut IndexInfo,
) -> bool {
    let mut giststate: *mut GISTSTATE = (*indexInfo).ii_AmCache as *mut GISTSTATE;
    let itup: IndexTuple;
    let oldCxt: MemoryContext;

    /* Initialize GISTSTATE cache if first call in this statement */
    if giststate.is_null() {
        let oldCxt = MemoryContextSwitchTo((*indexInfo).ii_Context);
        giststate = initGISTstate(r);
        (*giststate).tempCxt = createTempGistContext();
        (*indexInfo).ii_AmCache = giststate as *mut c_void;
        MemoryContextSwitchTo(oldCxt);
    }

    oldCxt = MemoryContextSwitchTo((*giststate).tempCxt);

    itup = gistFormTuple(giststate, r, values, isnull, true);
    (*itup).t_tid = *ht_ctid;

    gistdoinsert(r, itup, 0, giststate, heapRel, false);

    /* cleanup */
    MemoryContextSwitchTo(oldCxt);
    MemoryContextReset((*giststate).tempCxt);

    false
}

/*
 * Place tuples from 'itup' to 'buffer'. If 'oldoffnum' is valid, the tuple
 * at that offset is atomically removed along with inserting the new tuples.
 * This is used to replace a tuple with a new one.
 *
 * If 'leftchildbuf' is valid, we're inserting the downlink for the page
 * to the right of 'leftchildbuf', or updating the downlink for 'leftchildbuf'.
 * F_FOLLOW_RIGHT flag on 'leftchildbuf' is cleared and NSN is set.
 *
 * If 'markfollowright' is true and the page is split, the left child is
 * marked with F_FOLLOW_RIGHT flag. That is the normal case. During buffered
 * index build, however, there is no concurrent access and the page splitting
 * is done in a slightly simpler fashion, and false is passed.
 *
 * If there is not enough room on the page, it is split. All the split
 * pages are kept pinned and locked and returned in *splitinfo, the caller
 * is responsible for inserting the downlinks for them. However, if
 * 'buffer' is the root page and it needs to be split, gistplacetopage()
 * performs the split as one atomic operation, and *splitinfo is set to NIL.
 * In that case, we continue to hold the root page locked, and the child
 * pages are released; note that new tuple(s) are *not* on the root page
 * but in one of the new child pages.
 *
 * If 'newblkno' is not NULL, returns the block number of page the first
 * new/updated tuple was inserted to. Usually it's the given page, but could
 * be its right sibling if the page was split.
 *
 * Returns 'true' if the page was split, 'false' otherwise.
 */
pub unsafe fn gistplacetopage(
    rel: Relation,
    freespace: Size,
    giststate: *mut GISTSTATE,
    buffer: Buffer,
    itup: *mut IndexTuple,
    ntup: c_int,
    oldoffnum: OffsetNumber,
    newblkno: *mut BlockNumber,
    leftchildbuf: Buffer,
    splitinfo: *mut *mut List,
    markfollowright: bool,
    heapRel: Relation,
    is_build: bool,
) -> bool {
    let blkno: BlockNumber = BufferGetBlockNumber(buffer);
    let page: Page = BufferGetPage(buffer);
    let is_leaf: bool = if GistPageIsLeaf(page) { true } else { false };
    let mut recptr: XLogRecPtr = 0;
    let mut is_split: bool;

    /*
     * Refuse to modify a page that's incompletely split. This should not
     * happen because we finish any incomplete splits while we walk down the
     * tree. However, it's remotely possible that another concurrent inserter
     * splits a parent page, and errors out before completing the split. We
     * will just throw an error in that case, and leave any split we had in
     * progress unfinished too. The next insert that comes along will clean up
     * the mess.
     */
    if GistFollowRight(page) {
        elog!(ERROR, "concurrent GiST page split was incomplete");
    }

    /* should never try to insert to a deleted page */
    Assert!(!GistPageIsDeleted(page));

    *splitinfo = NIL;

    /*
     * if isupdate, remove old key: This node's key has been modified, either
     * because a child split occurred or because we needed to adjust our key
     * for an insert in a child node. Therefore, remove the old version of
     * this node's key.
     *
     * for WAL replay, in the non-split case we handle this by setting up a
     * one-element todelete array; in the split case, it's handled implicitly
     * because the tuple vector passed to gistSplit won't include this tuple.
     */
    is_split = gistnospace(page, itup, ntup, oldoffnum, freespace);

    /*
     * If leaf page is full, try at first to delete dead tuples. And then
     * check again.
     */
    if is_split && GistPageIsLeaf(page) && GistPageHasGarbage(page) {
        gistprunepage(rel, page, buffer, heapRel);
        is_split = gistnospace(page, itup, ntup, oldoffnum, freespace);
    }

    if is_split {
        /* no space for insertion */
        let mut itvec: *mut IndexTuple;
        let mut tlen: c_int = 0;
        let mut dist: *mut SplitPageLayout = null_mut();
        let mut ptr: *mut SplitPageLayout;
        let mut oldrlink: BlockNumber = InvalidBlockNumber;
        let mut oldnsn: GistNSN = 0;
        let mut rootpg: SplitPageLayout = core::mem::zeroed();
        let is_rootsplit: bool;
        let mut npage: c_int;

        is_rootsplit = blkno == GIST_ROOT_BLKNO;

        /*
         * Form index tuples vector to split. If we're replacing an old tuple,
         * remove the old version from the vector.
         */
        itvec = gistextractpage(page, &mut tlen);
        if OffsetNumberIsValid(oldoffnum) {
            /* on inner page we should remove old tuple */
            let pos: c_int = (oldoffnum - FirstOffsetNumber) as c_int;

            tlen -= 1;
            if pos != tlen {
                core::ptr::copy(
                    itvec.add((pos + 1) as usize),
                    itvec.add(pos as usize),
                    (tlen - pos) as usize,
                );
            }
        }
        itvec = gistjoinvector(itvec, &mut tlen, itup, ntup);
        dist = gistSplit(rel, page, itvec, tlen, giststate);

        /*
         * Check that split didn't produce too many pages.
         */
        npage = 0;
        ptr = dist;
        while !ptr.is_null() {
            npage += 1;
            ptr = (*ptr).next;
        }
        /* in a root split, we'll add one more page to the list below */
        if is_rootsplit {
            npage += 1;
        }
        if npage > GIST_MAX_SPLIT_PAGES {
            elog!(
                ERROR,
                "GiST page split into too many halves ({}, maximum {})",
                npage,
                GIST_MAX_SPLIT_PAGES
            );
        }

        /*
         * Set up pages to work with. Allocate new buffers for all but the
         * leftmost page. The original page becomes the new leftmost page, and
         * is just replaced with the new contents.
         *
         * For a root-split, allocate new buffers for all child pages, the
         * original page is overwritten with new root page containing
         * downlinks to the new child pages.
         */
        ptr = dist;
        if !is_rootsplit {
            /* save old rightlink and NSN */
            oldrlink = (*GistPageGetOpaque(page)).rightlink;
            oldnsn = GistPageGetNSN(page);

            (*dist).buffer = buffer;
            (*dist).block.blkno = BufferGetBlockNumber(buffer);
            (*dist).page = PageGetTempPageCopySpecial(BufferGetPage(buffer));

            /* clean all flags except F_LEAF */
            (*GistPageGetOpaque((*dist).page)).flags = if is_leaf { F_LEAF } else { 0 };

            ptr = (*ptr).next;
        }
        while !ptr.is_null() {
            /* Allocate new page */
            (*ptr).buffer = gistNewBuffer(rel, heapRel);
            GISTInitBuffer((*ptr).buffer, if is_leaf { F_LEAF } else { 0 } as uint32);
            (*ptr).page = BufferGetPage((*ptr).buffer);
            (*ptr).block.blkno = BufferGetBlockNumber((*ptr).buffer);
            PredicateLockPageSplit(
                rel,
                BufferGetBlockNumber(buffer),
                BufferGetBlockNumber((*ptr).buffer),
            );
            ptr = (*ptr).next;
        }

        /*
         * Now that we know which blocks the new pages go to, set up downlink
         * tuples to point to them.
         */
        ptr = dist;
        while !ptr.is_null() {
            ItemPointerSetBlockNumber(&mut (*(*ptr).itup).t_tid, (*ptr).block.blkno);
            GistTupleSetValid((*ptr).itup);
            ptr = (*ptr).next;
        }

        /*
         * If this is a root split, we construct the new root page with the
         * downlinks here directly, instead of requiring the caller to insert
         * them. Add the new root page to the list along with the child pages.
         */
        if is_rootsplit {
            let downlinks: *mut IndexTuple;
            let mut ndownlinks: c_int = 0;
            let mut i: c_int;

            rootpg.buffer = buffer;
            rootpg.page = PageGetTempPageCopySpecial(BufferGetPage(rootpg.buffer));
            (*GistPageGetOpaque(rootpg.page)).flags = 0;

            /* Prepare a vector of all the downlinks */
            ptr = dist;
            while !ptr.is_null() {
                ndownlinks += 1;
                ptr = (*ptr).next;
            }
            downlinks = palloc(core::mem::size_of::<IndexTuple>() * ndownlinks as usize)
                as *mut IndexTuple;
            i = 0;
            ptr = dist;
            while !ptr.is_null() {
                *downlinks.add(i as usize) = (*ptr).itup;
                i += 1;
                ptr = (*ptr).next;
            }

            rootpg.block.blkno = GIST_ROOT_BLKNO;
            rootpg.block.num = ndownlinks;
            rootpg.list = gistfillitupvec(downlinks, ndownlinks, &mut rootpg.lenlist);
            rootpg.itup = null_mut();

            rootpg.next = dist;
            dist = &mut rootpg;
        } else {
            /* Prepare split-info to be returned to caller */
            ptr = dist;
            while !ptr.is_null() {
                let si: *mut GISTPageSplitInfo =
                    palloc(core::mem::size_of::<GISTPageSplitInfo>()) as *mut GISTPageSplitInfo;

                (*si).buf = (*ptr).buffer;
                (*si).downlink = (*ptr).itup;
                *splitinfo = lappend(*splitinfo, si as *mut c_void);
                ptr = (*ptr).next;
            }
        }

        /*
         * Fill all pages. All the pages are new, ie. freshly allocated empty
         * pages, or a temporary copy of the old page.
         */
        ptr = dist;
        while !ptr.is_null() {
            let mut data: *mut c_char = (*ptr).list as *mut c_char;

            for i in 0..(*ptr).block.num {
                let thistup: IndexTuple = data as IndexTuple;

                if PageAddItem(
                    (*ptr).page,
                    data as Item,
                    IndexTupleSize(thistup),
                    (i + FirstOffsetNumber as c_int) as OffsetNumber,
                    false,
                    false,
                ) == InvalidOffsetNumber
                {
                    elog!(
                        ERROR,
                        "failed to add item to index page in \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                    );
                }

                /*
                 * If this is the first inserted/updated tuple, let the caller
                 * know which page it landed on.
                 */
                if !newblkno.is_null()
                    && ItemPointerEquals(&mut (*thistup).t_tid, &mut (**itup).t_tid)
                {
                    *newblkno = (*ptr).block.blkno;
                }

                data = data.add(IndexTupleSize(thistup));
            }

            /* Set up rightlinks */
            if !(*ptr).next.is_null() && (*ptr).block.blkno != GIST_ROOT_BLKNO {
                (*GistPageGetOpaque((*ptr).page)).rightlink = (*(*ptr).next).block.blkno;
            } else {
                (*GistPageGetOpaque((*ptr).page)).rightlink = oldrlink;
            }

            /*
             * Mark the all but the right-most page with the follow-right
             * flag. It will be cleared as soon as the downlink is inserted
             * into the parent, but this ensures that if we error out before
             * that, the index is still consistent. (in buffering build mode,
             * any error will abort the index build anyway, so this is not
             * needed.)
             */
            if !(*ptr).next.is_null() && !is_rootsplit && markfollowright {
                GistMarkFollowRight((*ptr).page);
            } else {
                GistClearFollowRight((*ptr).page);
            }

            /*
             * Copy the NSN of the original page to all pages. The
             * F_FOLLOW_RIGHT flags ensure that scans will follow the
             * rightlinks until the downlinks are inserted.
             */
            GistPageSetNSN((*ptr).page, oldnsn);
            ptr = (*ptr).next;
        }

        /*
         * gistXLogSplit() needs to WAL log a lot of pages, prepare WAL
         * insertion for that. NB: The number of pages and data segments
         * specified here must match the calculations in gistXLogSplit()!
         */
        if !is_build && RelationNeedsWAL(rel) {
            XLogEnsureRecordSpace(npage, 1 + npage * 2);
        }

        START_CRIT_SECTION();

        /*
         * Must mark buffers dirty before XLogInsert, even though we'll still
         * be changing their opaque fields below.
         */
        ptr = dist;
        while !ptr.is_null() {
            MarkBufferDirty((*ptr).buffer);
            ptr = (*ptr).next;
        }
        if BufferIsValid(leftchildbuf) {
            MarkBufferDirty(leftchildbuf);
        }

        /*
         * The first page in the chain was a temporary working copy meant to
         * replace the old page. Copy it over the old page.
         */
        PageRestoreTempPage((*dist).page, BufferGetPage((*dist).buffer));
        (*dist).page = BufferGetPage((*dist).buffer);

        /*
         * Write the WAL record.
         *
         * If we're building a new index, however, we don't WAL-log changes
         * yet. The LSN-NSN interlock between parent and child requires that
         * LSNs never move backwards, so set the LSNs to a value that's
         * smaller than any real or fake unlogged LSN that might be generated
         * later. (There can't be any concurrent scans during index build, so
         * we don't need to be able to detect concurrent splits yet.)
         */
        if is_build {
            recptr = GistBuildLSN;
        } else {
            if RelationNeedsWAL(rel) {
                recptr = gistXLogSplit(
                    is_leaf,
                    dist,
                    oldrlink,
                    oldnsn,
                    leftchildbuf,
                    markfollowright,
                );
            } else {
                recptr = gistGetFakeLSN(rel);
            }
        }

        ptr = dist;
        while !ptr.is_null() {
            PageSetLSN((*ptr).page, recptr);
            ptr = (*ptr).next;
        }

        /*
         * Return the new child buffers to the caller.
         *
         * If this was a root split, we've already inserted the downlink
         * pointers, in the form of a new root page. Therefore we can release
         * all the new buffers, and keep just the root page locked.
         */
        if is_rootsplit {
            ptr = (*dist).next;
            while !ptr.is_null() {
                UnlockReleaseBuffer((*ptr).buffer);
                ptr = (*ptr).next;
            }
        }
    } else {
        /*
         * Enough space.  We always get here if ntup==0.
         */
        START_CRIT_SECTION();

        /*
         * Delete old tuple if any, then insert new tuple(s) if any.  If
         * possible, use the fast path of PageIndexTupleOverwrite.
         */
        if OffsetNumberIsValid(oldoffnum) {
            if ntup == 1 {
                /* One-for-one replacement, so use PageIndexTupleOverwrite */
                if !PageIndexTupleOverwrite(
                    page,
                    oldoffnum,
                    *itup as Item,
                    IndexTupleSize(*itup),
                ) {
                    elog!(
                        ERROR,
                        "failed to add item to index page in \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                    );
                }
            } else {
                /* Delete old, then append new tuple(s) to page */
                PageIndexTupleDelete(page, oldoffnum);
                gistfillbuffer(page, itup, ntup, InvalidOffsetNumber);
            }
        } else {
            /* Just append new tuples at the end of the page */
            gistfillbuffer(page, itup, ntup, InvalidOffsetNumber);
        }

        MarkBufferDirty(buffer);

        if BufferIsValid(leftchildbuf) {
            MarkBufferDirty(leftchildbuf);
        }

        if is_build {
            recptr = GistBuildLSN;
        } else {
            if RelationNeedsWAL(rel) {
                let mut ndeloffs: OffsetNumber = 0;
                let mut deloffs: [OffsetNumber; 1] = [0; 1];

                if OffsetNumberIsValid(oldoffnum) {
                    deloffs[0] = oldoffnum;
                    ndeloffs = 1;
                }

                recptr = gistXLogUpdate(
                    buffer,
                    deloffs.as_mut_ptr(),
                    ndeloffs as c_int,
                    itup,
                    ntup,
                    leftchildbuf,
                );
            } else {
                recptr = gistGetFakeLSN(rel);
            }
        }
        PageSetLSN(page, recptr);

        if !newblkno.is_null() {
            *newblkno = blkno;
        }
    }

    /*
     * If we inserted the downlink for a child page, set NSN and clear
     * F_FOLLOW_RIGHT flag on the left child, so that concurrent scans know to
     * follow the rightlink if and only if they looked at the parent page
     * before we inserted the downlink.
     *
     * Note that we do this *after* writing the WAL record. That means that
     * the possible full page image in the WAL record does not include these
     * changes, and they must be replayed even if the page is restored from
     * the full page image. There's a chicken-and-egg problem: if we updated
     * the child pages first, we wouldn't know the recptr of the WAL record
     * we're about to write.
     */
    if BufferIsValid(leftchildbuf) {
        let leftpg: Page = BufferGetPage(leftchildbuf);

        GistPageSetNSN(leftpg, recptr);
        GistClearFollowRight(leftpg);

        PageSetLSN(leftpg, recptr);
    }

    END_CRIT_SECTION();

    is_split
}

// gistutil.c: gistNewBuffer. TODO(pg-port): import once gistutil.c is ported.
unsafe fn gistNewBuffer(r: Relation, heaprel: Relation) -> Buffer {
    crate::access::gist::gist_private::gistNewBuffer(r, heaprel)
}

/*
 * Workhorse routine for doing insertion into a GiST index. Note that
 * this routine assumes it is invoked in a short-lived memory context,
 * so it does not bother releasing palloc'd allocations.
 */
pub unsafe fn gistdoinsert(
    r: Relation,
    itup: IndexTuple,
    freespace: Size,
    giststate: *mut GISTSTATE,
    heapRel: Relation,
    is_build: bool,
) {
    let mut iid: ItemId;
    let mut idxtuple: IndexTuple;
    let mut firststack: GISTInsertStack = core::mem::zeroed();
    let mut stack: *mut GISTInsertStack;
    let mut state: GISTInsertState = core::mem::zeroed();
    let mut xlocked: bool = false;

    // memset(&state, 0, sizeof(GISTInsertState)); -- zeroed above
    state.freespace = freespace;
    state.r = r;
    state.heapRel = heapRel;
    state.is_build = is_build;

    /* Start from the root */
    firststack.blkno = GIST_ROOT_BLKNO;
    firststack.lsn = 0;
    firststack.retry_from_parent = false;
    firststack.parent = null_mut();
    firststack.downlinkoffnum = InvalidOffsetNumber;
    stack = &mut firststack;
    state.stack = stack;

    /*
     * Walk down along the path of smallest penalty, updating the parent
     * pointers with the key we're inserting as we go. If we crash in the
     * middle, the tree is consistent, although the possible parent updates
     * were a waste.
     */
    loop {
        /*
         * If we split an internal page while descending the tree, we have to
         * retry at the parent. (Normally, the LSN-NSN interlock below would
         * also catch this and cause us to retry. But LSNs are not updated
         * during index build.)
         */
        while (*stack).retry_from_parent {
            if xlocked {
                LockBuffer((*stack).buffer, GIST_UNLOCK);
            }
            xlocked = false;
            ReleaseBuffer((*stack).buffer);
            stack = (*stack).parent;
            state.stack = stack;
        }

        if XLogRecPtrIsInvalid((*stack).lsn) {
            (*stack).buffer = ReadBuffer(state.r, (*stack).blkno);
        }

        /*
         * Be optimistic and grab shared lock first. Swap it for an exclusive
         * lock later if we need to update the page.
         */
        if !xlocked {
            LockBuffer((*stack).buffer, GIST_SHARE);
            gistcheckpage(state.r, (*stack).buffer);
        }

        (*stack).page = BufferGetPage((*stack).buffer) as Page;
        (*stack).lsn = if xlocked {
            PageGetLSN((*stack).page)
        } else {
            BufferGetLSNAtomic((*stack).buffer)
        };
        Assert!(!RelationNeedsWAL(state.r) || !XLogRecPtrIsInvalid((*stack).lsn));

        /*
         * If this page was split but the downlink was never inserted to the
         * parent because the inserting backend crashed before doing that, fix
         * that now.
         */
        if GistFollowRight((*stack).page) {
            if !xlocked {
                LockBuffer((*stack).buffer, GIST_UNLOCK);
                LockBuffer((*stack).buffer, GIST_EXCLUSIVE);
                xlocked = true;
                /* someone might've completed the split when we unlocked */
                if !GistFollowRight((*stack).page) {
                    continue;
                }
            }
            gistfixsplit(&mut state, giststate);

            UnlockReleaseBuffer((*stack).buffer);
            xlocked = false;
            stack = (*stack).parent;
            state.stack = stack;
            continue;
        }

        if ((*stack).blkno != GIST_ROOT_BLKNO
            && (*(*stack).parent).lsn < GistPageGetNSN((*stack).page))
            || GistPageIsDeleted((*stack).page)
        {
            /*
             * Concurrent split or page deletion detected. There's no
             * guarantee that the downlink for this page is consistent with
             * the tuple we're inserting anymore, so go back to parent and
             * rechoose the best child.
             */
            UnlockReleaseBuffer((*stack).buffer);
            xlocked = false;
            stack = (*stack).parent;
            state.stack = stack;
            continue;
        }

        if !GistPageIsLeaf((*stack).page) {
            /*
             * This is an internal page so continue to walk down the tree.
             * Find the child node that has the minimum insertion penalty.
             */
            let childblkno: BlockNumber;
            let newtup: IndexTuple;
            let item: *mut GISTInsertStack;
            let downlinkoffnum: OffsetNumber;

            downlinkoffnum = gistchoose(state.r, (*stack).page, itup, giststate);
            iid = PageGetItemId((*stack).page, downlinkoffnum);
            idxtuple = PageGetItem((*stack).page, iid) as IndexTuple;
            childblkno = ItemPointerGetBlockNumber(&(*idxtuple).t_tid);

            /*
             * Check that it's not a leftover invalid tuple from pre-9.1
             */
            if GistTupleIsInvalid(idxtuple) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "index \"{}\" contains an inner tuple marked as invalid",
                        CStr::from_ptr(RelationGetRelationName(r)).to_string_lossy()
                    )
                );
                // C also: errdetail("This is caused by an incomplete page split
                // at crash recovery before upgrading to PostgreSQL 9.1.")
                //         errhint("Please REINDEX it.")
            }

            /*
             * Check that the key representing the target child node is
             * consistent with the key we're inserting. Update it if it's not.
             */
            newtup = gistgetadjusted(state.r, idxtuple, itup, giststate);
            if !newtup.is_null() {
                /*
                 * Swap shared lock for an exclusive one. Beware, the page may
                 * change while we unlock/lock the page...
                 */
                if !xlocked {
                    LockBuffer((*stack).buffer, GIST_UNLOCK);
                    LockBuffer((*stack).buffer, GIST_EXCLUSIVE);
                    xlocked = true;
                    (*stack).page = BufferGetPage((*stack).buffer) as Page;

                    if PageGetLSN((*stack).page) != (*stack).lsn {
                        /* the page was changed while we unlocked it, retry */
                        continue;
                    }
                }

                /*
                 * Update the tuple.
                 *
                 * We still hold the lock after gistinserttuple(), but it
                 * might have to split the page to make the updated tuple fit.
                 * In that case the updated tuple might migrate to the other
                 * half of the split, so we have to go back to the parent and
                 * descend back to the half that's a better fit for the new
                 * tuple.
                 */
                if gistinserttuple(&mut state, stack, giststate, newtup, downlinkoffnum) {
                    /*
                     * If this was a root split, the root page continues to be
                     * the parent and the updated tuple went to one of the
                     * child pages, so we just need to retry from the root
                     * page.
                     */
                    if (*stack).blkno != GIST_ROOT_BLKNO {
                        UnlockReleaseBuffer((*stack).buffer);
                        xlocked = false;
                        stack = (*stack).parent;
                        state.stack = stack;
                    }
                    continue;
                }
            }
            LockBuffer((*stack).buffer, GIST_UNLOCK);
            xlocked = false;

            /* descend to the chosen child */
            item = palloc0(core::mem::size_of::<GISTInsertStack>()) as *mut GISTInsertStack;
            (*item).blkno = childblkno;
            (*item).parent = stack;
            (*item).downlinkoffnum = downlinkoffnum;
            stack = item;
            state.stack = stack;
        } else {
            /*
             * Leaf page. Insert the new key. We've already updated all the
             * parents on the way down, but we might have to split the page if
             * it doesn't fit. gistinserttuple() will take care of that.
             */

            /*
             * Swap shared lock for an exclusive one. Be careful, the page may
             * change while we unlock/lock the page...
             */
            if !xlocked {
                LockBuffer((*stack).buffer, GIST_UNLOCK);
                LockBuffer((*stack).buffer, GIST_EXCLUSIVE);
                xlocked = true;
                (*stack).page = BufferGetPage((*stack).buffer) as Page;
                (*stack).lsn = PageGetLSN((*stack).page);

                if (*stack).blkno == GIST_ROOT_BLKNO {
                    /*
                     * the only page that can become inner instead of leaf is
                     * the root page, so for root we should recheck it
                     */
                    if !GistPageIsLeaf((*stack).page) {
                        /*
                         * very rare situation: during unlock/lock index with
                         * number of pages = 1 was increased
                         */
                        LockBuffer((*stack).buffer, GIST_UNLOCK);
                        xlocked = false;
                        continue;
                    }

                    /*
                     * we don't need to check root split, because checking
                     * leaf/inner is enough to recognize split for root
                     */
                } else if (GistFollowRight((*stack).page)
                    || (*(*stack).parent).lsn < GistPageGetNSN((*stack).page))
                    || GistPageIsDeleted((*stack).page)
                {
                    /*
                     * The page was split or deleted while we momentarily
                     * unlocked the page. Go back to parent.
                     */
                    UnlockReleaseBuffer((*stack).buffer);
                    xlocked = false;
                    stack = (*stack).parent;
                    state.stack = stack;
                    continue;
                }
            }

            /* now state.stack->(page, buffer and blkno) points to leaf page */

            gistinserttuple(&mut state, stack, giststate, itup, InvalidOffsetNumber);
            LockBuffer((*stack).buffer, GIST_UNLOCK);

            /* Release any pins we might still hold before exiting */
            while !stack.is_null() {
                ReleaseBuffer((*stack).buffer);
                stack = (*stack).parent;
            }
            break;
        }
    }
}

/*
 * Traverse the tree to find path from root page to specified "child" block.
 *
 * returns a new insertion stack, starting from the parent of "child", up
 * to the root. *downlinkoffnum is set to the offset of the downlink in the
 * direct parent of child.
 *
 * To prevent deadlocks, this should lock only one page at a time.
 */
unsafe fn gistFindPath(
    r: Relation,
    child: BlockNumber,
    downlinkoffnum: *mut OffsetNumber,
) -> *mut GISTInsertStack {
    let mut page: Page;
    let mut buffer: Buffer;
    let mut i: OffsetNumber;
    let mut maxoff: OffsetNumber;
    let mut iid: ItemId;
    let mut idxtuple: IndexTuple;
    let mut fifo: *mut List;
    let mut top: *mut GISTInsertStack;
    let mut ptr: *mut GISTInsertStack;
    let mut blkno: BlockNumber;

    top = palloc0(core::mem::size_of::<GISTInsertStack>()) as *mut GISTInsertStack;
    (*top).blkno = GIST_ROOT_BLKNO;
    (*top).downlinkoffnum = InvalidOffsetNumber;

    fifo = list_make1!(top as *mut c_void);
    while fifo != NIL {
        /* Get next page to visit */
        top = linitial(fifo) as *mut GISTInsertStack;
        fifo = list_delete_first(fifo);

        buffer = ReadBuffer(r, (*top).blkno);
        LockBuffer(buffer, GIST_SHARE);
        gistcheckpage(r, buffer);
        page = BufferGetPage(buffer) as Page;

        if GistPageIsLeaf(page) {
            /*
             * Because we scan the index top-down, all the rest of the pages
             * in the queue must be leaf pages as well.
             */
            UnlockReleaseBuffer(buffer);
            break;
        }

        /* currently, internal pages are never deleted */
        Assert!(!GistPageIsDeleted(page));

        (*top).lsn = BufferGetLSNAtomic(buffer);

        /*
         * If F_FOLLOW_RIGHT is set, the page to the right doesn't have a
         * downlink. This should not normally happen..
         */
        if GistFollowRight(page) {
            elog!(ERROR, "concurrent GiST page split was incomplete");
        }

        if !(*top).parent.is_null()
            && (*(*top).parent).lsn < GistPageGetNSN(page)
            && (*GistPageGetOpaque(page)).rightlink != InvalidBlockNumber
        /* sanity check */
        {
            /*
             * Page was split while we looked elsewhere. We didn't see the
             * downlink to the right page when we scanned the parent, so add
             * it to the queue now.
             *
             * Put the right page ahead of the queue, so that we visit it
             * next. That's important, because if this is the lowest internal
             * level, just above leaves, we might already have queued up some
             * leaf pages, and we assume that there can't be any non-leaf
             * pages behind leaf pages.
             */
            ptr = palloc0(core::mem::size_of::<GISTInsertStack>()) as *mut GISTInsertStack;
            (*ptr).blkno = (*GistPageGetOpaque(page)).rightlink;
            (*ptr).downlinkoffnum = InvalidOffsetNumber;
            (*ptr).parent = (*top).parent;

            fifo = lcons(ptr as *mut c_void, fifo);
        }

        maxoff = PageGetMaxOffsetNumber(page);

        i = FirstOffsetNumber;
        while i <= maxoff {
            iid = PageGetItemId(page, i);
            idxtuple = PageGetItem(page, iid) as IndexTuple;
            blkno = ItemPointerGetBlockNumber(&(*idxtuple).t_tid);
            if blkno == child {
                /* Found it! */
                UnlockReleaseBuffer(buffer);
                *downlinkoffnum = i;
                return top;
            } else {
                /* Append this child to the list of pages to visit later */
                ptr = palloc0(core::mem::size_of::<GISTInsertStack>()) as *mut GISTInsertStack;
                (*ptr).blkno = blkno;
                (*ptr).downlinkoffnum = i;
                (*ptr).parent = top;

                fifo = lappend(fifo, ptr as *mut c_void);
            }
            i = OffsetNumberNext(i);
        }

        UnlockReleaseBuffer(buffer);
    }

    elog!(
        ERROR,
        "failed to re-find parent of a page in index \"{}\", block {}",
        CStr::from_ptr(RelationGetRelationName(r)).to_string_lossy(),
        child
    );
    null_mut() /* keep compiler quiet */
}

/*
 * Updates the stack so that child->parent is the correct parent of the
 * child. child->parent must be exclusively locked on entry, and will
 * remain so at exit, but it might not be the same page anymore.
 */
unsafe fn gistFindCorrectParent(r: Relation, child: *mut GISTInsertStack, is_build: bool) {
    let parent: *mut GISTInsertStack = (*child).parent;
    let mut iid: ItemId;
    let mut idxtuple: IndexTuple;
    let mut maxoff: OffsetNumber;
    let mut ptr: *mut GISTInsertStack;

    gistcheckpage(r, (*parent).buffer);
    (*parent).page = BufferGetPage((*parent).buffer) as Page;
    maxoff = PageGetMaxOffsetNumber((*parent).page);

    /* Check if the downlink is still where it was before */
    if (*child).downlinkoffnum != InvalidOffsetNumber && (*child).downlinkoffnum <= maxoff {
        iid = PageGetItemId((*parent).page, (*child).downlinkoffnum);
        idxtuple = PageGetItem((*parent).page, iid) as IndexTuple;
        if ItemPointerGetBlockNumber(&(*idxtuple).t_tid) == (*child).blkno {
            return; /* still there */
        }
    }

    /*
     * The page has changed since we looked. During normal operation, every
     * update of a page changes its LSN, so the LSN we memorized should have
     * changed too.
     *
     * During index build, however, we don't WAL-log the changes until we have
     * built the index, so the LSN doesn't change. There is no concurrent
     * activity during index build, but we might have changed the parent
     * ourselves.
     *
     * We will also get here if child->downlinkoffnum is invalid. That happens
     * if 'parent' had been updated by an earlier call to this function on its
     * grandchild, which had to move right.
     */
    Assert!(
        (*parent).lsn != PageGetLSN((*parent).page)
            || is_build
            || (*child).downlinkoffnum == InvalidOffsetNumber
    );

    /*
     * Scan the page to re-find the downlink. If the page was split, it might
     * have moved to a different page, so follow the right links until we find
     * it.
     */
    loop {
        let mut i: OffsetNumber;

        maxoff = PageGetMaxOffsetNumber((*parent).page);
        i = FirstOffsetNumber;
        while i <= maxoff {
            iid = PageGetItemId((*parent).page, i);
            idxtuple = PageGetItem((*parent).page, iid) as IndexTuple;
            if ItemPointerGetBlockNumber(&(*idxtuple).t_tid) == (*child).blkno {
                /* yes!!, found */
                (*child).downlinkoffnum = i;
                return;
            }
            i = OffsetNumberNext(i);
        }

        (*parent).blkno = (*GistPageGetOpaque((*parent).page)).rightlink;
        (*parent).downlinkoffnum = InvalidOffsetNumber;
        UnlockReleaseBuffer((*parent).buffer);
        if (*parent).blkno == InvalidBlockNumber {
            /*
             * End of chain and still didn't find parent. It's a very-very
             * rare situation when the root was split.
             */
            break;
        }
        (*parent).buffer = ReadBuffer(r, (*parent).blkno);
        LockBuffer((*parent).buffer, GIST_EXCLUSIVE);
        gistcheckpage(r, (*parent).buffer);
        (*parent).page = BufferGetPage((*parent).buffer) as Page;
    }

    /*
     * awful!!, we need search tree to find parent ... , but before we should
     * release all old parent
     */

    ptr = (*(*child).parent).parent; /* child->parent already released above */
    while !ptr.is_null() {
        ReleaseBuffer((*ptr).buffer);
        ptr = (*ptr).parent;
    }

    /* ok, find new path */
    let parent2: *mut GISTInsertStack =
        gistFindPath(r, (*child).blkno, &mut (*child).downlinkoffnum);
    ptr = parent2;

    /* read all buffers as expected by caller */
    /* note we don't lock them or gistcheckpage them here! */
    while !ptr.is_null() {
        (*ptr).buffer = ReadBuffer(r, (*ptr).blkno);
        (*ptr).page = BufferGetPage((*ptr).buffer) as Page;
        ptr = (*ptr).parent;
    }

    /* install new chain of parents to stack */
    (*child).parent = parent2;

    /* make recursive call to normal processing */
    LockBuffer((*(*child).parent).buffer, GIST_EXCLUSIVE);
    gistFindCorrectParent(r, child, is_build);
}

/*
 * Form a downlink pointer for the page in 'buf'.
 */
unsafe fn gistformdownlink(
    rel: Relation,
    buf: Buffer,
    giststate: *mut GISTSTATE,
    stack: *mut GISTInsertStack,
    is_build: bool,
) -> IndexTuple {
    let page: Page = BufferGetPage(buf);
    let maxoff: OffsetNumber;
    let mut offset: OffsetNumber;
    let mut downlink: IndexTuple = null_mut();

    maxoff = PageGetMaxOffsetNumber(page);
    offset = FirstOffsetNumber;
    while offset <= maxoff {
        let ituple: IndexTuple = PageGetItem(page, PageGetItemId(page, offset)) as IndexTuple;

        if downlink.is_null() {
            downlink = CopyIndexTuple(ituple);
        } else {
            let newdownlink: IndexTuple;

            newdownlink = gistgetadjusted(rel, downlink, ituple, giststate);
            if !newdownlink.is_null() {
                downlink = newdownlink;
            }
        }
        offset = OffsetNumberNext(offset);
    }

    /*
     * If the page is completely empty, we can't form a meaningful downlink
     * for it. But we have to insert a downlink for the page. Any key will do,
     * as long as its consistent with the downlink of parent page, so that we
     * can legally insert it to the parent. A minimal one that matches as few
     * scans as possible would be best, to keep scans from doing useless work,
     * but we don't know how to construct that. So we just use the downlink of
     * the original page that was split - that's as far from optimal as it can
     * get but will do..
     */
    if downlink.is_null() {
        let iid: ItemId;

        LockBuffer((*(*stack).parent).buffer, GIST_EXCLUSIVE);
        gistFindCorrectParent(rel, stack, is_build);
        iid = PageGetItemId((*(*stack).parent).page, (*stack).downlinkoffnum);
        downlink = PageGetItem((*(*stack).parent).page, iid) as IndexTuple;
        downlink = CopyIndexTuple(downlink);
        LockBuffer((*(*stack).parent).buffer, GIST_UNLOCK);
    }

    ItemPointerSetBlockNumber(&mut (*downlink).t_tid, BufferGetBlockNumber(buf));
    GistTupleSetValid(downlink);

    downlink
}

/*
 * Complete the incomplete split of state->stack->page.
 */
unsafe fn gistfixsplit(state: *mut GISTInsertState, giststate: *mut GISTSTATE) {
    let stack: *mut GISTInsertStack = (*state).stack;
    let mut buf: Buffer;
    let mut page: Page;
    let mut splitinfo: *mut List = NIL;

    ereport!(
        LOG,
        errmsg!(
            "fixing incomplete split in index \"{}\", block {}",
            CStr::from_ptr(RelationGetRelationName((*state).r)).to_string_lossy(),
            (*stack).blkno
        )
    );

    Assert!(GistFollowRight((*stack).page));
    Assert!(OffsetNumberIsValid((*stack).downlinkoffnum));

    buf = (*stack).buffer;

    /*
     * Read the chain of split pages, following the rightlinks. Construct a
     * downlink tuple for each page.
     */
    loop {
        let si: *mut GISTPageSplitInfo =
            palloc(core::mem::size_of::<GISTPageSplitInfo>()) as *mut GISTPageSplitInfo;
        let downlink: IndexTuple;

        page = BufferGetPage(buf);

        /* Form the new downlink tuples to insert to parent */
        downlink = gistformdownlink((*state).r, buf, giststate, stack, (*state).is_build);

        (*si).buf = buf;
        (*si).downlink = downlink;

        splitinfo = lappend(splitinfo, si as *mut c_void);

        if GistFollowRight(page) {
            /* lock next page */
            buf = ReadBuffer((*state).r, (*GistPageGetOpaque(page)).rightlink);
            LockBuffer(buf, GIST_EXCLUSIVE);
        } else {
            break;
        }
    }

    /* Insert the downlinks */
    gistfinishsplit(state, stack, giststate, splitinfo, false);
}

/*
 * Insert or replace a tuple in stack->buffer. If 'oldoffnum' is valid, the
 * tuple at 'oldoffnum' is replaced, otherwise the tuple is inserted as new.
 * 'stack' represents the path from the root to the page being updated.
 *
 * The caller must hold an exclusive lock on stack->buffer.  The lock is still
 * held on return, but the page might not contain the inserted tuple if the
 * page was split. The function returns true if the page was split, false
 * otherwise.
 */
unsafe fn gistinserttuple(
    state: *mut GISTInsertState,
    stack: *mut GISTInsertStack,
    giststate: *mut GISTSTATE,
    tuple: IndexTuple,
    oldoffnum: OffsetNumber,
) -> bool {
    let mut tuple = tuple;
    gistinserttuples(
        state,
        stack,
        giststate,
        &mut tuple,
        1,
        oldoffnum,
        InvalidBuffer,
        InvalidBuffer,
        false,
        false,
    )
}

/* ----------------
 * An extended workhorse version of gistinserttuple(). This version allows
 * inserting multiple tuples, or replacing a single tuple with multiple tuples.
 * This is used to recursively update the downlinks in the parent when a page
 * is split.
 *
 * If leftchild and rightchild are valid, we're inserting/replacing the
 * downlink for rightchild, and leftchild is its left sibling. We clear the
 * F_FOLLOW_RIGHT flag and update NSN on leftchild, atomically with the
 * insertion of the downlink.
 *
 * To avoid holding locks for longer than necessary, when recursing up the
 * tree to update the parents, the locking is a bit peculiar here. On entry,
 * the caller must hold an exclusive lock on stack->buffer, as well as
 * leftchild and rightchild if given. On return:
 *
 *	- Lock on stack->buffer is released, if 'unlockbuf' is true. The page is
 *	  always kept pinned, however.
 *	- Lock on 'leftchild' is released, if 'unlockleftchild' is true. The page
 *	  is kept pinned.
 *	- Lock and pin on 'rightchild' are always released.
 *
 * Returns 'true' if the page had to be split. Note that if the page was
 * split, the inserted/updated tuples might've been inserted to a right
 * sibling of stack->buffer instead of stack->buffer itself.
 */
unsafe fn gistinserttuples(
    state: *mut GISTInsertState,
    stack: *mut GISTInsertStack,
    giststate: *mut GISTSTATE,
    tuples: *mut IndexTuple,
    ntup: c_int,
    oldoffnum: OffsetNumber,
    leftchild: Buffer,
    rightchild: Buffer,
    unlockbuf: bool,
    unlockleftchild: bool,
) -> bool {
    let mut splitinfo: *mut List = null_mut();
    let is_split: bool;

    /*
     * Check for any rw conflicts (in serializable isolation level) just
     * before we intend to modify the page
     */
    CheckForSerializableConflictIn((*state).r, null_mut(), BufferGetBlockNumber((*stack).buffer));

    /* Insert the tuple(s) to the page, splitting the page if necessary */
    is_split = gistplacetopage(
        (*state).r,
        (*state).freespace,
        giststate,
        (*stack).buffer,
        tuples,
        ntup,
        oldoffnum,
        null_mut(),
        leftchild,
        &mut splitinfo,
        true,
        (*state).heapRel,
        (*state).is_build,
    );

    /*
     * Before recursing up in case the page was split, release locks on the
     * child pages. We don't need to keep them locked when updating the
     * parent.
     */
    if BufferIsValid(rightchild) {
        UnlockReleaseBuffer(rightchild);
    }
    if BufferIsValid(leftchild) && unlockleftchild {
        LockBuffer(leftchild, GIST_UNLOCK);
    }

    /*
     * If we had to split, insert/update the downlinks in the parent. If the
     * caller requested us to release the lock on stack->buffer, tell
     * gistfinishsplit() to do that as soon as it's safe to do so. If we
     * didn't have to split, release it ourselves.
     */
    if !splitinfo.is_null() {
        gistfinishsplit(state, stack, giststate, splitinfo, unlockbuf);
    } else if unlockbuf {
        LockBuffer((*stack).buffer, GIST_UNLOCK);
    }

    is_split
}

/*
 * Finish an incomplete split by inserting/updating the downlinks in parent
 * page. 'splitinfo' contains all the child pages involved in the split,
 * from left-to-right.
 *
 * On entry, the caller must hold a lock on stack->buffer and all the child
 * pages in 'splitinfo'. If 'unlockbuf' is true, the lock on stack->buffer is
 * released on return. The child pages are always unlocked and unpinned.
 */
unsafe fn gistfinishsplit(
    state: *mut GISTInsertState,
    stack: *mut GISTInsertStack,
    giststate: *mut GISTSTATE,
    splitinfo: *mut List,
    unlockbuf: bool,
) {
    let mut right: *mut GISTPageSplitInfo;
    let mut left: *mut GISTPageSplitInfo;
    let mut tuples: [IndexTuple; 2] = [null_mut(); 2];

    /* A split always contains at least two halves */
    Assert!(list_length(splitinfo) >= 2);

    /*
     * We need to insert downlinks for each new page, and update the downlink
     * for the original (leftmost) page in the split. Begin at the rightmost
     * page, inserting one downlink at a time until there's only two pages
     * left. Finally insert the downlink for the last new page and update the
     * downlink for the original page as one operation.
     */
    LockBuffer((*(*stack).parent).buffer, GIST_EXCLUSIVE);

    /*
     * Insert downlinks for the siblings from right to left, until there are
     * only two siblings left.
     */
    let mut pos: c_int = list_length(splitinfo) - 1;
    while pos > 1 {
        right = list_nth(splitinfo, pos) as *mut GISTPageSplitInfo;
        left = list_nth(splitinfo, pos - 1) as *mut GISTPageSplitInfo;

        gistFindCorrectParent((*state).r, stack, (*state).is_build);
        if gistinserttuples(
            state,
            (*stack).parent,
            giststate,
            &mut (*right).downlink,
            1,
            InvalidOffsetNumber,
            (*left).buf,
            (*right).buf,
            false,
            false,
        ) {
            /*
             * If the parent page was split, the existing downlink might have
             * moved.
             */
            (*stack).downlinkoffnum = InvalidOffsetNumber;
        }
        /* gistinserttuples() released the lock on right->buf. */
        pos -= 1;
    }

    right = lsecond(splitinfo) as *mut GISTPageSplitInfo;
    left = linitial(splitinfo) as *mut GISTPageSplitInfo;

    /*
     * Finally insert downlink for the remaining right page and update the
     * downlink for the original page to not contain the tuples that were
     * moved to the new pages.
     */
    tuples[0] = (*left).downlink;
    tuples[1] = (*right).downlink;
    gistFindCorrectParent((*state).r, stack, (*state).is_build);
    let _ = gistinserttuples(
        state,
        (*stack).parent,
        giststate,
        tuples.as_mut_ptr(),
        2,
        (*stack).downlinkoffnum,
        (*left).buf,
        (*right).buf,
        true,       /* Unlock parent */
        unlockbuf,  /* Unlock stack->buffer if caller wants that */
    );

    /*
     * The downlink might have moved when we updated it. Even if the page
     * wasn't split, because gistinserttuples() implements updating the old
     * tuple by removing and re-inserting it!
     */
    (*stack).downlinkoffnum = InvalidOffsetNumber;

    Assert!((*left).buf == (*stack).buffer);

    /*
     * If we split the page because we had to adjust the downlink on an
     * internal page, while descending the tree for inserting a new tuple,
     * then this might no longer be the correct page for the new tuple. The
     * downlink to this page might not cover the new tuple anymore, it might
     * need to go to the newly-created right sibling instead. Tell the caller
     * to walk back up the stack, to re-check at the parent which page to
     * insert to.
     *
     * Normally, the LSN-NSN interlock during the tree descend would also
     * detect that a concurrent split happened (by ourselves), and cause us to
     * retry at the parent. But that mechanism doesn't work during index
     * build, because we don't do WAL-logging, and don't update LSNs, during
     * index build.
     */
    (*stack).retry_from_parent = true;
}

/*
 * gistSplit -- split a page in the tree and fill struct
 * used for XLOG and real writes buffers. Function is recursive, ie
 * it will split page until keys will fit in every page.
 */
pub unsafe fn gistSplit(
    r: Relation,
    page: Page,
    itup: *mut IndexTuple, /* contains compressed entry */
    len: c_int,
    giststate: *mut GISTSTATE,
) -> *mut SplitPageLayout {
    let mut lvectup: *mut IndexTuple;
    let mut rvectup: *mut IndexTuple;
    let mut v: GistSplitVector = core::mem::zeroed();
    let mut i: c_int;
    let mut res: *mut SplitPageLayout = null_mut();

    /* this should never recurse very deeply, but better safe than sorry */
    check_stack_depth();

    /* there's no point in splitting an empty page */
    Assert!(len > 0);

    /*
     * If a single tuple doesn't fit on a page, no amount of splitting will
     * help.
     */
    if len == 1 {
        ereport!(
            ERROR,
            errmsg!(
                "index row size {} exceeds maximum {} for index \"{}\"",
                IndexTupleSize(*itup.add(0)),
                GiSTPageSize(),
                CStr::from_ptr(RelationGetRelationName(r)).to_string_lossy()
            )
        );
        // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
    }

    core::ptr::write_bytes(
        v.spl_lisnull.as_mut_ptr(),
        1,
        (*(*giststate).nonLeafTupdesc).natts as usize,
    );
    core::ptr::write_bytes(
        v.spl_risnull.as_mut_ptr(),
        1,
        (*(*giststate).nonLeafTupdesc).natts as usize,
    );
    gistSplitByKey(r, page, itup, len, giststate, &mut v, 0);

    /* form left and right vector */
    lvectup = palloc(core::mem::size_of::<IndexTuple>() * (len + 1) as usize) as *mut IndexTuple;
    rvectup = palloc(core::mem::size_of::<IndexTuple>() * (len + 1) as usize) as *mut IndexTuple;

    i = 0;
    while i < v.splitVector.spl_nleft {
        *lvectup.add(i as usize) = *itup.add((*v.splitVector.spl_left.add(i as usize) - 1) as usize);
        i += 1;
    }

    i = 0;
    while i < v.splitVector.spl_nright {
        *rvectup.add(i as usize) =
            *itup.add((*v.splitVector.spl_right.add(i as usize) - 1) as usize);
        i += 1;
    }

    /* finalize splitting (may need another split) */
    if !gistfitpage(rvectup, v.splitVector.spl_nright) {
        res = gistSplit(r, page, rvectup, v.splitVector.spl_nright, giststate);
    } else {
        ROTATEDIST!(res);
        (*res).block.num = v.splitVector.spl_nright;
        (*res).list = gistfillitupvec(rvectup, v.splitVector.spl_nright, &mut (*res).lenlist);
        (*res).itup = gistFormTuple(
            giststate,
            r,
            v.spl_rattr.as_ptr(),
            v.spl_risnull.as_ptr(),
            false,
        );
    }

    if !gistfitpage(lvectup, v.splitVector.spl_nleft) {
        let mut resptr: *mut SplitPageLayout;
        let subres: *mut SplitPageLayout;

        subres = gistSplit(r, page, lvectup, v.splitVector.spl_nleft, giststate);
        resptr = subres;

        /* install on list's tail */
        while !(*resptr).next.is_null() {
            resptr = (*resptr).next;
        }

        (*resptr).next = res;
        res = subres;
    } else {
        ROTATEDIST!(res);
        (*res).block.num = v.splitVector.spl_nleft;
        (*res).list = gistfillitupvec(lvectup, v.splitVector.spl_nleft, &mut (*res).lenlist);
        (*res).itup = gistFormTuple(
            giststate,
            r,
            v.spl_lattr.as_ptr(),
            v.spl_lisnull.as_ptr(),
            false,
        );
    }

    res
}

/*
 * Create a GISTSTATE and fill it with information about the index
 */
pub unsafe fn initGISTstate(index: Relation) -> *mut GISTSTATE {
    let giststate: *mut GISTSTATE;
    let scanCxt: MemoryContext;
    let oldCxt: MemoryContext;
    let mut i: c_int;

    /* safety check to protect fixed-size arrays in GISTSTATE */
    if (*(*index).rd_att).natts > INDEX_MAX_KEYS as c_int {
        elog!(
            ERROR,
            "numberOfAttributes {} > {}",
            (*(*index).rd_att).natts,
            INDEX_MAX_KEYS
        );
    }

    /* Create the memory context that will hold the GISTSTATE */
    scanCxt = AllocSetContextCreate(
        CurrentMemoryContext,
        c"GiST scan context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );
    oldCxt = MemoryContextSwitchTo(scanCxt);

    /* Create and fill in the GISTSTATE */
    giststate = palloc(core::mem::size_of::<GISTSTATE>()) as *mut GISTSTATE;

    (*giststate).scanCxt = scanCxt;
    (*giststate).tempCxt = scanCxt; /* caller must change this if needed */
    (*giststate).leafTupdesc = (*index).rd_att;

    /*
     * The truncated tupdesc for non-leaf index tuples, which doesn't contain
     * the INCLUDE attributes.
     *
     * It is used to form tuples during tuple adjustment and page split.
     * B-tree creates shortened tuple descriptor for every truncated tuple,
     * because it is doing this less often: it does not have to form truncated
     * tuples during page split.  Also, B-tree is not adjusting tuples on
     * internal pages the way GiST does.
     */
    (*giststate).nonLeafTupdesc = CreateTupleDescTruncatedCopy(
        (*index).rd_att,
        IndexRelationGetNumberOfKeyAttributes(index),
    );

    i = 0;
    while i < IndexRelationGetNumberOfKeyAttributes(index) {
        fmgr_info_copy(
            &mut (*giststate).consistentFn[i as usize],
            index_getprocinfo(index, (i + 1) as AttrNumber, GIST_CONSISTENT_PROC as uint16),
            scanCxt,
        );
        fmgr_info_copy(
            &mut (*giststate).unionFn[i as usize],
            index_getprocinfo(index, (i + 1) as AttrNumber, GIST_UNION_PROC as uint16),
            scanCxt,
        );

        /* opclasses are not required to provide a Compress method */
        if OidIsValid(index_getprocid(index, (i + 1) as AttrNumber, GIST_COMPRESS_PROC as uint16)) {
            fmgr_info_copy(
                &mut (*giststate).compressFn[i as usize],
                index_getprocinfo(index, (i + 1) as AttrNumber, GIST_COMPRESS_PROC as uint16),
                scanCxt,
            );
        } else {
            (*giststate).compressFn[i as usize].fn_oid = InvalidOid;
        }

        /* opclasses are not required to provide a Decompress method */
        if OidIsValid(index_getprocid(index, (i + 1) as AttrNumber, GIST_DECOMPRESS_PROC as uint16)) {
            fmgr_info_copy(
                &mut (*giststate).decompressFn[i as usize],
                index_getprocinfo(index, (i + 1) as AttrNumber, GIST_DECOMPRESS_PROC as uint16),
                scanCxt,
            );
        } else {
            (*giststate).decompressFn[i as usize].fn_oid = InvalidOid;
        }

        fmgr_info_copy(
            &mut (*giststate).penaltyFn[i as usize],
            index_getprocinfo(index, (i + 1) as AttrNumber, GIST_PENALTY_PROC as uint16),
            scanCxt,
        );
        fmgr_info_copy(
            &mut (*giststate).picksplitFn[i as usize],
            index_getprocinfo(index, (i + 1) as AttrNumber, GIST_PICKSPLIT_PROC as uint16),
            scanCxt,
        );
        fmgr_info_copy(
            &mut (*giststate).equalFn[i as usize],
            index_getprocinfo(index, (i + 1) as AttrNumber, GIST_EQUAL_PROC as uint16),
            scanCxt,
        );

        /* opclasses are not required to provide a Distance method */
        if OidIsValid(index_getprocid(index, (i + 1) as AttrNumber, GIST_DISTANCE_PROC as uint16)) {
            fmgr_info_copy(
                &mut (*giststate).distanceFn[i as usize],
                index_getprocinfo(index, (i + 1) as AttrNumber, GIST_DISTANCE_PROC as uint16),
                scanCxt,
            );
        } else {
            (*giststate).distanceFn[i as usize].fn_oid = InvalidOid;
        }

        /* opclasses are not required to provide a Fetch method */
        if OidIsValid(index_getprocid(index, (i + 1) as AttrNumber, GIST_FETCH_PROC as uint16)) {
            fmgr_info_copy(
                &mut (*giststate).fetchFn[i as usize],
                index_getprocinfo(index, (i + 1) as AttrNumber, GIST_FETCH_PROC as uint16),
                scanCxt,
            );
        } else {
            (*giststate).fetchFn[i as usize].fn_oid = InvalidOid;
        }

        /*
         * If the index column has a specified collation, we should honor that
         * while doing comparisons.  However, we may have a collatable storage
         * type for a noncollatable indexed data type.  If there's no index
         * collation then specify default collation in case the support
         * functions need collation.  This is harmless if the support
         * functions don't care about collation, so we just do it
         * unconditionally.  (We could alternatively call get_typcollation,
         * but that seems like expensive overkill --- there aren't going to be
         * any cases where a GiST storage type has a nondefault collation.)
         */
        if OidIsValid(*(*index).rd_indcollation.add(i as usize)) {
            (*giststate).supportCollation[i as usize] = *(*index).rd_indcollation.add(i as usize);
        } else {
            (*giststate).supportCollation[i as usize] = DEFAULT_COLLATION_OID;
        }
        i += 1;
    }

    /* No opclass information for INCLUDE attributes */
    while i < (*(*index).rd_att).natts {
        (*giststate).consistentFn[i as usize].fn_oid = InvalidOid;
        (*giststate).unionFn[i as usize].fn_oid = InvalidOid;
        (*giststate).compressFn[i as usize].fn_oid = InvalidOid;
        (*giststate).decompressFn[i as usize].fn_oid = InvalidOid;
        (*giststate).penaltyFn[i as usize].fn_oid = InvalidOid;
        (*giststate).picksplitFn[i as usize].fn_oid = InvalidOid;
        (*giststate).equalFn[i as usize].fn_oid = InvalidOid;
        (*giststate).distanceFn[i as usize].fn_oid = InvalidOid;
        (*giststate).fetchFn[i as usize].fn_oid = InvalidOid;
        (*giststate).supportCollation[i as usize] = InvalidOid;
        i += 1;
    }

    MemoryContextSwitchTo(oldCxt);

    giststate
}

pub unsafe fn freeGISTstate(giststate: *mut GISTSTATE) {
    /* It's sufficient to delete the scanCxt */
    MemoryContextDelete((*giststate).scanCxt);
}

/*
 * gistprunepage() -- try to remove LP_DEAD items from the given page.
 * Function assumes that buffer is exclusively locked.
 */
unsafe fn gistprunepage(rel: Relation, page: Page, buffer: Buffer, heapRel: Relation) {
    let mut deletable: [OffsetNumber; MaxIndexTuplesPerPage] = [0; MaxIndexTuplesPerPage];
    let mut ndeletable: c_int = 0;
    let mut offnum: OffsetNumber;
    let maxoff: OffsetNumber;

    Assert!(GistPageIsLeaf(page));

    /*
     * Scan over all items to see which ones need to be deleted according to
     * LP_DEAD flags.
     */
    maxoff = PageGetMaxOffsetNumber(page);
    offnum = FirstOffsetNumber;
    while offnum <= maxoff {
        let itemId: ItemId = PageGetItemId(page, offnum);

        if ItemIdIsDead(itemId) {
            deletable[ndeletable as usize] = offnum;
            ndeletable += 1;
        }
        offnum = OffsetNumberNext(offnum);
    }

    if ndeletable > 0 {
        let mut snapshotConflictHorizon: TransactionId = InvalidTransactionId;

        if XLogStandbyInfoActive() && RelationNeedsWAL(rel) {
            snapshotConflictHorizon = index_compute_xid_horizon_for_tuples(
                rel,
                heapRel,
                buffer,
                deletable.as_mut_ptr(),
                ndeletable,
            );
        }

        START_CRIT_SECTION();

        PageIndexMultiDelete(page, deletable.as_mut_ptr(), ndeletable);

        /*
         * Mark the page as not containing any LP_DEAD items.  This is not
         * certainly true (there might be some that have recently been marked,
         * but weren't included in our target-item list), but it will almost
         * always be true and it doesn't seem worth an additional page scan to
         * check it. Remember that F_HAS_GARBAGE is only a hint anyway.
         */
        GistClearPageHasGarbage(page);

        MarkBufferDirty(buffer);

        /* XLOG stuff */
        if RelationNeedsWAL(rel) {
            let recptr: XLogRecPtr;

            recptr = gistXLogDelete(
                buffer,
                deletable.as_mut_ptr(),
                ndeletable,
                snapshotConflictHorizon,
                heapRel,
            );

            PageSetLSN(page, recptr);
        } else {
            PageSetLSN(page, gistGetFakeLSN(rel));
        }

        END_CRIT_SECTION();
    }

    /*
     * Note: if we didn't find any LP_DEAD items, then the page's
     * F_HAS_GARBAGE hint bit is falsely set.  We do not bother expending a
     * separate write to clear it, however.  We will clear it when we split
     * the page.
     */
}

// access/itup.h: MaxIndexTuplesPerPage. Used as a fixed array length so it must
// be a usize const. C value is 1358 (BLCKSZ-based). TODO: dedup once exported.
const MaxIndexTuplesPerPage: usize = 1358;
