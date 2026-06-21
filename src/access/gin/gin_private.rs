//! access/gin_private.h - header file for postgres inverted index access method implementation.
//!
//! Portions Copyright (c) 2006-2025, PostgreSQL Global Development Group

use crate::prelude::*;

use crate::access::common::indextuple::{IndexTuple, INDEX_MAX_KEYS};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::gin::gin::{GinStatsData, GinTernaryValue};
use crate::access::gin::ginblock::{GinNullCategory, GinPostingList, PostingItem};
use crate::access::index::amapi::{
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexScanDesc,
    IndexUniqueCheck, IndexVacuumInfo,
};
use crate::access::stratnum::StrategyNumber;
use crate::common::int::pg_cmp_u64;
use crate::lib::rbtree::{RBTNode, RBTree, RBTreeIterator};
use crate::nodes::pg_list::List;
use crate::nodes::tidbitmap::{TBMIterateResult, TBMPrivateIterator, TIDBitmap, TBM_MAX_TUPLES_PER_PAGE};
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::Page;
use crate::storage::buf::Buffer;
use crate::storage::itemptr::{ItemPointer, ItemPointerData};
use crate::storage::off::OffsetNumber;
use crate::utils::fmgr::FmgrInfo;
use crate::utils::rel::Relation;
use std::ffi::c_int;

// `IndexInfo` is referenced only behind `struct IndexInfo *` (an opaque forward
// declaration in the C header). Use a local opaque alias to avoid a hard
// dependency on nodes/execnodes here.
// TODO: dedup - canonical is crate::nodes::execnodes::IndexInfo.
pub type IndexInfo = c_void;

// `ScanKey` is `*mut ScanKeyData`; the canonical type lives in
// access/common/scankey. Imported here for the rescan prototype.
use crate::access::common::scankey::ScanKey;

/*
 * Storage type for GIN's reloptions
 */
#[repr(C)]
pub struct GinOptions {
    pub vl_len_: int32,          /* varlena header (do not touch directly!) */
    pub useFastUpdate: bool,     /* use fast updates? */
    pub pendingListCleanupSize: c_int, /* maximum size of pending list */
}

pub const GIN_DEFAULT_USE_FASTUPDATE: bool = true;

// GinGetUseFastUpdate / GinGetPendingListCleanupSize are relation-accessor
// macros that dereference rd_rel / rd_options and assert relkind/relam. They are
// best translated at the call sites that have RelationData in scope; omitted as
// inline fns here to avoid pulling in the full relcache layout.

/* Macros for buffer lock/unlock operations */
// BUFFER_LOCK_* live in storage/bufmgr.h, which has no shared Rust module yet.
// TODO: dedup - replace with crate::storage::bufmgr::BUFFER_LOCK_* once available.
const BUFFER_LOCK_UNLOCK: c_int = 0;
const BUFFER_LOCK_SHARE: c_int = 1;
const BUFFER_LOCK_EXCLUSIVE: c_int = 2;
pub const GIN_UNLOCK: c_int = BUFFER_LOCK_UNLOCK;
pub const GIN_SHARE: c_int = BUFFER_LOCK_SHARE;
pub const GIN_EXCLUSIVE: c_int = BUFFER_LOCK_EXCLUSIVE;

/*
 * GinState: working data structure describing the index being worked on
 */
#[repr(C)]
pub struct GinState {
    pub index: Relation,
    pub oneCol: bool, /* true if single-column index */

    /*
     * origTupdesc is the nominal tuple descriptor of the index. In a
     * single-column index this describes the actual leaf index tuples. In a
     * multi-column index, the actual leaf tuples contain a smallint column
     * number followed by a key datum of the appropriate type for that column.
     */
    pub origTupdesc: TupleDesc,
    pub tupdesc: [TupleDesc; INDEX_MAX_KEYS],

    /*
     * Per-index-column opclass support functions
     */
    pub compareFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub extractValueFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub extractQueryFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub consistentFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub triConsistentFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub comparePartialFn: [FmgrInfo; INDEX_MAX_KEYS], /* optional method */
    /* canPartialMatch[i] is true if comparePartialFn[i] is valid */
    pub canPartialMatch: [bool; INDEX_MAX_KEYS],
    /* Collations to pass to the support functions */
    pub supportCollation: [Oid; INDEX_MAX_KEYS],
}

/* ginutil.c */
pub unsafe fn ginoptions(reloptions: Datum, validate: bool) -> *mut bytea { unimplemented!() }
pub unsafe fn initGinState(state: *mut GinState, index: Relation) { unimplemented!() }
pub unsafe fn GinNewBuffer(index: Relation) -> Buffer { unimplemented!() }
pub unsafe fn GinInitBuffer(b: Buffer, f: uint32) { unimplemented!() }
pub unsafe fn GinInitPage(page: Page, f: uint32, pageSize: Size) { unimplemented!() }
pub unsafe fn GinInitMetabuffer(b: Buffer) { unimplemented!() }
pub unsafe fn ginCompareEntries(
    ginstate: *mut GinState,
    attnum: OffsetNumber,
    a: Datum,
    categorya: GinNullCategory,
    b: Datum,
    categoryb: GinNullCategory,
) -> c_int { unimplemented!() }
pub unsafe fn ginCompareAttEntries(
    ginstate: *mut GinState,
    attnuma: OffsetNumber,
    a: Datum,
    categorya: GinNullCategory,
    attnumb: OffsetNumber,
    b: Datum,
    categoryb: GinNullCategory,
) -> c_int { unimplemented!() }
pub unsafe fn ginExtractEntries(
    ginstate: *mut GinState,
    attnum: OffsetNumber,
    value: Datum,
    isNull: bool,
    nentries: *mut int32,
    categories: *mut *mut GinNullCategory,
) -> *mut Datum { unimplemented!() }

pub unsafe fn gintuple_get_attrnum(ginstate: *mut GinState, tuple: IndexTuple) -> OffsetNumber { unimplemented!() }
pub unsafe fn gintuple_get_key(
    ginstate: *mut GinState,
    tuple: IndexTuple,
    category: *mut GinNullCategory,
) -> Datum { unimplemented!() }
pub unsafe fn ginbuildphasename(phasenum: int64) -> *mut c_char { unimplemented!() }

/* gininsert.c */
pub unsafe fn ginbuild(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult { unimplemented!() }
pub unsafe fn ginbuildempty(index: Relation) { unimplemented!() }
pub unsafe fn gininsert(
    index: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    ht_ctid: ItemPointer,
    heapRel: Relation,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
    indexInfo: *mut IndexInfo,
) -> bool { unimplemented!() }
pub unsafe fn ginEntryInsert(
    ginstate: *mut GinState,
    attnum: OffsetNumber,
    key: Datum,
    category: GinNullCategory,
    items: *mut ItemPointerData,
    nitem: uint32,
    buildStats: *mut GinStatsData,
) { unimplemented!() }

/* ginbtree.c */

#[repr(C)]
pub struct GinBtreeStack {
    pub blkno: BlockNumber,
    pub buffer: Buffer,
    pub off: OffsetNumber,
    pub iptr: ItemPointerData,
    /* predictNumber contains predicted number of pages on current level */
    pub predictNumber: uint32,
    pub parent: *mut GinBtreeStack,
}

pub type GinBtree = *mut GinBtreeData;

/* Return codes for GinBtreeData.beginPlaceToPage method */
pub type GinPlaceToPageRC = c_int;
pub const GPTP_NO_WORK: GinPlaceToPageRC = 0;
pub const GPTP_INSERT: GinPlaceToPageRC = 1;
pub const GPTP_SPLIT: GinPlaceToPageRC = 2;

#[repr(C)]
pub struct GinBtreeData {
    /* search methods */
    pub findChildPage: Option<unsafe extern "C" fn(GinBtree, *mut GinBtreeStack) -> BlockNumber>,
    pub getLeftMostChild: Option<unsafe extern "C" fn(GinBtree, Page) -> BlockNumber>,
    pub isMoveRight: Option<unsafe extern "C" fn(GinBtree, Page) -> bool>,
    pub findItem: Option<unsafe extern "C" fn(GinBtree, *mut GinBtreeStack) -> bool>,

    /* insert methods */
    pub findChildPtr:
        Option<unsafe extern "C" fn(GinBtree, Page, BlockNumber, OffsetNumber) -> OffsetNumber>,
    pub beginPlaceToPage: Option<
        unsafe extern "C" fn(
            GinBtree,
            Buffer,
            *mut GinBtreeStack,
            *mut c_void,
            BlockNumber,
            *mut *mut c_void,
            *mut Page,
            *mut Page,
        ) -> GinPlaceToPageRC,
    >,
    pub execPlaceToPage: Option<
        unsafe extern "C" fn(
            GinBtree,
            Buffer,
            *mut GinBtreeStack,
            *mut c_void,
            BlockNumber,
            *mut c_void,
        ),
    >,
    pub prepareDownlink: Option<unsafe extern "C" fn(GinBtree, Buffer) -> *mut c_void>,
    pub fillRoot:
        Option<unsafe extern "C" fn(GinBtree, Page, BlockNumber, Page, BlockNumber, Page)>,

    pub isData: bool,

    pub index: Relation,
    pub rootBlkno: BlockNumber,
    pub ginstate: *mut GinState, /* not valid in a data scan */
    pub fullScan: bool,
    pub isBuild: bool,

    /* Search key for Entry tree */
    pub entryAttnum: OffsetNumber,
    pub entryKey: Datum,
    pub entryCategory: GinNullCategory,

    /* Search key for data tree (posting tree) */
    pub itemptr: ItemPointerData,
}

/* This represents a tuple to be inserted to entry tree. */
#[repr(C)]
pub struct GinBtreeEntryInsertData {
    pub entry: IndexTuple, /* tuple to insert */
    pub isDelete: bool,    /* delete old tuple at same offset? */
}

/*
 * This represents an itempointer, or many itempointers, to be inserted to
 * a data (posting tree) leaf page
 */
#[repr(C)]
pub struct GinBtreeDataLeafInsertData {
    pub items: *mut ItemPointerData,
    pub nitem: uint32,
    pub curitem: uint32,
}

/*
 * For internal data (posting tree) pages, the insertion payload is a
 * PostingItem
 */

pub unsafe fn ginFindLeafPage(
    btree: GinBtree,
    searchMode: bool,
    rootConflictCheck: bool,
) -> *mut GinBtreeStack { unimplemented!() }
pub unsafe fn ginStepRight(buffer: Buffer, index: Relation, lockmode: c_int) -> Buffer { unimplemented!() }
pub unsafe fn freeGinBtreeStack(stack: *mut GinBtreeStack) { unimplemented!() }
pub unsafe fn ginInsertValue(
    btree: GinBtree,
    stack: *mut GinBtreeStack,
    insertdata: *mut c_void,
    buildStats: *mut GinStatsData,
) { unimplemented!() }

/* ginentrypage.c */
pub unsafe fn GinFormTuple(
    ginstate: *mut GinState,
    attnum: OffsetNumber,
    key: Datum,
    category: GinNullCategory,
    data: Pointer,
    dataSize: Size,
    nipd: c_int,
    errorTooBig: bool,
) -> IndexTuple { crate::access::gin::ginentrypage::GinFormTuple(ginstate, attnum, key, category, data, dataSize, nipd, errorTooBig) }
pub unsafe fn ginPrepareEntryScan(
    btree: GinBtree,
    attnum: OffsetNumber,
    key: Datum,
    category: GinNullCategory,
    ginstate: *mut GinState,
) { crate::access::gin::ginentrypage::ginPrepareEntryScan(btree, attnum, key, category, ginstate) }
pub unsafe fn ginEntryFillRoot(
    btree: GinBtree,
    root: Page,
    lblkno: BlockNumber,
    lpage: Page,
    rblkno: BlockNumber,
    rpage: Page,
) { crate::access::gin::ginentrypage::ginEntryFillRoot(btree, root, lblkno, lpage, rblkno, rpage) }
pub unsafe fn ginReadTuple(
    ginstate: *mut GinState,
    attnum: OffsetNumber,
    itup: IndexTuple,
    nitems: *mut c_int,
) -> ItemPointer { crate::access::gin::ginentrypage::ginReadTuple(ginstate, attnum, itup, nitems) }

/* gindatapage.c */
pub unsafe fn GinDataLeafPageGetItems(
    page: Page,
    nitems: *mut c_int,
    advancePast: ItemPointerData,
) -> ItemPointer { unimplemented!() }
pub unsafe fn GinDataLeafPageGetItemsToTbm(page: Page, tbm: *mut TIDBitmap) -> c_int { unimplemented!() }
pub unsafe fn createPostingTree(
    index: Relation,
    items: *mut ItemPointerData,
    nitems: uint32,
    buildStats: *mut GinStatsData,
    entrybuffer: Buffer,
) -> BlockNumber { unimplemented!() }
pub unsafe fn GinDataPageAddPostingItem(page: Page, data: *mut PostingItem, offset: OffsetNumber) { unimplemented!() }
pub unsafe fn GinPageDeletePostingItem(page: Page, offset: OffsetNumber) { unimplemented!() }
pub unsafe fn ginInsertItemPointers(
    index: Relation,
    rootBlkno: BlockNumber,
    items: *mut ItemPointerData,
    nitem: uint32,
    buildStats: *mut GinStatsData,
) { unimplemented!() }
pub unsafe fn ginScanBeginPostingTree(
    btree: GinBtree,
    index: Relation,
    rootBlkno: BlockNumber,
) -> *mut GinBtreeStack { unimplemented!() }
pub unsafe fn ginDataFillRoot(
    btree: GinBtree,
    root: Page,
    lblkno: BlockNumber,
    lpage: Page,
    rblkno: BlockNumber,
    rpage: Page,
) { unimplemented!() }

/*
 * This is declared in ginvacuum.c, but is passed between ginVacuumItemPointers
 * and ginVacuumPostingTreeLeaf and as an opaque struct, so we need a forward
 * declaration for it.
 */
pub type GinVacuumState = c_void;

pub unsafe fn ginVacuumPostingTreeLeaf(
    indexrel: Relation,
    buffer: Buffer,
    gvs: *mut GinVacuumState,
) { unimplemented!() }

/* ginscan.c */

pub type GinScanKey = *mut GinScanKeyData;

pub type GinScanEntry = *mut GinScanEntryData;

#[repr(C)]
pub struct GinScanKeyData {
    /* Real number of entries in scanEntry[] (always > 0) */
    pub nentries: uint32,
    /* Number of entries that extractQueryFn and consistentFn know about */
    pub nuserentries: uint32,

    /* array of GinScanEntry pointers, one per extracted search condition */
    pub scanEntry: *mut GinScanEntry,

    /*
     * At least one of the entries in requiredEntries must be present for a
     * tuple to match the overall qual.
     */
    pub requiredEntries: *mut GinScanEntry,
    pub nrequired: c_int,
    pub additionalEntries: *mut GinScanEntry,
    pub nadditional: c_int,

    /* array of check flags, reported to consistentFn */
    pub entryRes: *mut GinTernaryValue,
    pub boolConsistentFn: Option<unsafe extern "C" fn(GinScanKey) -> bool>,
    pub triConsistentFn: Option<unsafe extern "C" fn(GinScanKey) -> GinTernaryValue>,
    pub consistentFmgrInfo: *mut FmgrInfo,
    pub triConsistentFmgrInfo: *mut FmgrInfo,
    pub collation: Oid,

    /* other data needed for calling consistentFn */
    pub query: Datum,
    /* NB: these three arrays have only nuserentries elements! */
    pub queryValues: *mut Datum,
    pub queryCategories: *mut GinNullCategory,
    pub extra_data: *mut Pointer,
    pub strategy: StrategyNumber,
    pub searchMode: int32,
    pub attnum: OffsetNumber,

    /*
     * An excludeOnly scan key is not able to enumerate all matching tuples.
     */
    pub excludeOnly: bool,

    /*
     * Match status data. curItem is the TID most recently tested.
     */
    pub curItem: ItemPointerData,
    pub curItemMatches: bool,
    pub recheckCurItem: bool,
    pub isFinished: bool,
}

#[repr(C)]
pub struct GinScanEntryData {
    /* query key and other information from extractQueryFn */
    pub queryKey: Datum,
    pub queryCategory: GinNullCategory,
    pub isPartialMatch: bool,
    pub extra_data: Pointer,
    pub strategy: StrategyNumber,
    pub searchMode: int32,
    pub attnum: OffsetNumber,

    /* Current page in posting tree */
    pub buffer: Buffer,

    /* current ItemPointer to heap */
    pub curItem: ItemPointerData,

    /* for a partial-match or full-scan query, we accumulate all TIDs here */
    pub matchBitmap: *mut TIDBitmap,
    pub matchIterator: *mut TBMPrivateIterator,

    /*
     * If blockno is InvalidBlockNumber, all of the other fields in the
     * matchResult are meaningless.
     */
    pub matchResult: TBMIterateResult,
    pub matchOffsets: [OffsetNumber; TBM_MAX_TUPLES_PER_PAGE as usize],
    pub matchNtuples: c_int,

    /* used for Posting list and one page in Posting tree */
    pub list: *mut ItemPointerData,
    pub nlist: c_int,
    pub offset: OffsetNumber,

    pub isFinished: bool,
    pub reduceResult: bool,
    pub predictNumberResult: uint32,
    pub btree: GinBtreeData,
}

#[repr(C)]
pub struct GinScanOpaqueData {
    pub tempCtx: MemoryContext,
    pub ginstate: GinState,

    pub keys: GinScanKey, /* one per scan qualifier expr */
    pub nkeys: uint32,

    pub entries: *mut GinScanEntry, /* one per index search condition */
    pub totalentries: uint32,
    pub allocentries: uint32, /* allocated length of entries[] */

    pub keyCtx: MemoryContext, /* used to hold key and entry data */

    pub isVoidRes: bool, /* true if query is unsatisfiable */
}

pub type GinScanOpaque = *mut GinScanOpaqueData;

pub unsafe fn ginbeginscan(rel: Relation, nkeys: c_int, norderbys: c_int) -> IndexScanDesc { unimplemented!() }
pub unsafe fn ginendscan(scan: IndexScanDesc) { unimplemented!() }
pub unsafe fn ginrescan(
    scan: IndexScanDesc,
    scankey: ScanKey,
    nscankeys: c_int,
    orderbys: ScanKey,
    norderbys: c_int,
) { unimplemented!() }
pub unsafe fn ginNewScanKey(scan: IndexScanDesc) { unimplemented!() }
pub unsafe fn ginFreeScanKeys(so: GinScanOpaque) { unimplemented!() }

/* ginget.c */
pub unsafe fn gingetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 { unimplemented!() }

/* ginlogic.c */
pub unsafe fn ginInitConsistentFunction(ginstate: *mut GinState, key: GinScanKey) { unimplemented!() }

/* ginvacuum.c */
pub unsafe fn ginbulkdelete(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult { unimplemented!() }
pub unsafe fn ginvacuumcleanup(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult { unimplemented!() }
pub unsafe fn ginVacuumItemPointers(
    gvs: *mut GinVacuumState,
    items: *mut ItemPointerData,
    nitem: c_int,
    nremaining: *mut c_int,
) -> ItemPointer { unimplemented!() }

/* ginvalidate.c */
pub unsafe fn ginvalidate(opclassoid: Oid) -> bool { crate::access::gin::ginvalidate::ginvalidate(opclassoid) }
pub unsafe fn ginadjustmembers(
    opfamilyoid: Oid,
    opclassoid: Oid,
    operators: *mut List,
    functions: *mut List,
) { crate::access::gin::ginvalidate::ginadjustmembers(opfamilyoid, opclassoid, operators, functions) }

/* ginbulk.c */
#[repr(C)]
pub struct GinEntryAccumulator {
    pub rbtnode: RBTNode,
    pub key: Datum,
    pub category: GinNullCategory,
    pub attnum: OffsetNumber,
    pub shouldSort: bool,
    pub list: *mut ItemPointerData,
    pub maxcount: uint32, /* allocated size of list[] */
    pub count: uint32,    /* current number of list[] entries */
}

#[repr(C)]
pub struct BuildAccumulator {
    pub ginstate: *mut GinState,
    pub allocatedMemory: Size,
    pub entryallocator: *mut GinEntryAccumulator,
    pub eas_used: uint32,
    pub tree: *mut RBTree,
    pub tree_walk: RBTreeIterator,
}

pub unsafe fn ginInitBA(accum: *mut BuildAccumulator) { unimplemented!() }
pub unsafe fn ginInsertBAEntries(
    accum: *mut BuildAccumulator,
    heapptr: ItemPointer,
    attnum: OffsetNumber,
    entries: *mut Datum,
    categories: *mut GinNullCategory,
    nentries: int32,
) { unimplemented!() }
pub unsafe fn ginBeginBAScan(accum: *mut BuildAccumulator) { unimplemented!() }
pub unsafe fn ginGetBAEntry(
    accum: *mut BuildAccumulator,
    attnum: *mut OffsetNumber,
    key: *mut Datum,
    category: *mut GinNullCategory,
    n: *mut uint32,
) -> *mut ItemPointerData { unimplemented!() }

/* ginfast.c */

#[repr(C)]
pub struct GinTupleCollector {
    pub tuples: *mut IndexTuple,
    pub ntuples: uint32,
    pub lentuples: uint32,
    pub sumsize: uint32,
}

pub unsafe fn ginHeapTupleFastInsert(ginstate: *mut GinState, collector: *mut GinTupleCollector) { unimplemented!() }
pub unsafe fn ginHeapTupleFastCollect(
    ginstate: *mut GinState,
    collector: *mut GinTupleCollector,
    attnum: OffsetNumber,
    value: Datum,
    isNull: bool,
    ht_ctid: ItemPointer,
) { unimplemented!() }
pub unsafe fn ginInsertCleanup(
    ginstate: *mut GinState,
    full_clean: bool,
    fill_fsm: bool,
    forceCleanup: bool,
    stats: *mut IndexBulkDeleteResult,
) { unimplemented!() }

/* ginpostinglist.c */

pub unsafe fn ginCompressPostingList(
    ipd: ItemPointer,
    nipd: c_int,
    maxsize: c_int,
    nwritten: *mut c_int,
) -> *mut GinPostingList { unimplemented!() }
pub unsafe fn ginPostingListDecodeAllSegmentsToTbm(
    ptr: *mut GinPostingList,
    len: c_int,
    tbm: *mut TIDBitmap,
) -> c_int { unimplemented!() }

pub unsafe fn ginPostingListDecodeAllSegments(
    segment: *mut GinPostingList,
    len: c_int,
    ndecoded_out: *mut c_int,
) -> ItemPointer { unimplemented!() }
pub unsafe fn ginPostingListDecode(
    plist: *mut GinPostingList,
    ndecoded_out: *mut c_int,
) -> ItemPointer { unimplemented!() }
pub unsafe fn ginMergeItemPointers(
    a: *mut ItemPointerData,
    na: uint32,
    b: *mut ItemPointerData,
    nb: uint32,
    nmerged: *mut c_int,
) -> ItemPointer { crate::access::gin::ginpostinglist::ginMergeItemPointers(a, na, b, nb, nmerged) }

/*
 * Merging the results of several gin scans compares item pointers a lot,
 * so we want this to be inlined.
 */
#[inline]
pub unsafe fn ginCompareItemPointers(a: ItemPointer, b: ItemPointer) -> c_int {
    use crate::access::gin::ginblock::{GinItemPointerGetBlockNumber, GinItemPointerGetOffsetNumber};
    let ia: uint64 = (GinItemPointerGetBlockNumber(a) as uint64) << 32
        | GinItemPointerGetOffsetNumber(a) as uint64;
    let ib: uint64 = (GinItemPointerGetBlockNumber(b) as uint64) << 32
        | GinItemPointerGetOffsetNumber(b) as uint64;

    pg_cmp_u64(ia, ib)
}

pub unsafe fn ginTraverseLock(buffer: Buffer, searchMode: bool) -> c_int { crate::access::gin::ginbtree::ginTraverseLock(buffer, searchMode) }
