/*-------------------------------------------------------------------------
 *
 * gistbuild.c
 *	  build algorithm for GiST indexes implementation.
 *
 * There are two different strategies:
 *
 * 1. Sort all input tuples, pack them into GiST leaf pages in the sorted
 *    order, and create downlinks and internal pages as we go. This builds
 *    the index from the bottom up, similar to how B-tree index build
 *    works.
 *
 * 2. Start with an empty index, and insert all tuples one by one.
 *
 * The sorted method is used if the operator classes for all columns have
 * a 'sortsupport' defined. Otherwise, we resort to the second strategy.
 *
 * The second strategy can optionally use buffers at different levels of
 * the tree to reduce I/O, see "Buffering build algorithm" in the README
 * for a more detailed explanation. It initially calls insert over and
 * over, but switches to the buffered algorithm after a certain number of
 * tuples (unless buffering mode is disabled).
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/gist/gistbuild.c
 *
 *-------------------------------------------------------------------------
 */
use crate::prelude::*;

use core::ffi::CStr;

use crate::access::common::indextuple::{IndexTuple, IndexTupleData, IndexTupleSize, INDEX_MAX_KEYS};
use crate::access::gist::gist::{
    createTempGistContext, gistdoinsert, gistplacetopage, initGISTstate,
};
use crate::access::gist::gist_private::{
    freeGISTstate, gistFreeBuildBuffers, gistGetNodeBuffer, gistInitBuildBuffers,
    gistPopItupFromNodeBuffer, gistPushItupToNodeBuffer, gistRelocateBuildBuffersOnSplit,
    gistUnloadNodeBuffers, BUFFER_OVERFLOWED, GISTBuildBuffers, GISTNodeBuffer, GISTPageSplitInfo,
    GISTSTATE, GIST_DEFAULT_FILLFACTOR, GIST_EXCLUSIVE, GIST_OPTION_BUFFERING_OFF,
    GIST_OPTION_BUFFERING_ON, GIST_ROOT_BLKNO, GIST_SHARE, GiSTOptions, LEVEL_HAS_BUFFERS,
    SplitPageLayout,
};
use crate::access::gist::gistsplit::gistSplit;
use crate::access::gist::gistutil::{
    gistCompressValues, gistFormTuple, gistNewBuffer, gistcheckpage, gistchoose, gistextractpage,
    gistfillbuffer, gistfillitupvec, gistgetadjusted, gistinitpage, gistjoinvector, gistunion,
    GISTInitBuffer, F_LEAF,
};
use crate::access::gist::gistvalidate::GIST_SORTSUPPORT_PROC;
use crate::nodes::pg_list::{
    lcons, linitial, list_delete_first, list_free_deep, list_length, List, NIL,
};
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::utils::sort::tuplesort::{
    tuplesort_end, tuplesort_performsort, Tuplesortstate, TUPLESORT_NONE,
};
use crate::utils::sort::tuplesortvariants::{
    tuplesort_begin_index_gist, tuplesort_getindextuple, tuplesort_putindextuplevalues,
};

// ----------------------------------------------------------------
// Local type aliases / dependent declarations from other translation units.
// ----------------------------------------------------------------

pub type Buffer = c_int;
pub type OffsetNumber = u16;
pub type Oid = c_uint;
pub type Page = *mut c_char;
pub type Relation = *mut crate::utils::rel::RelationData;
pub type ItemPointer = *mut ItemPointerData;

// storage/itemptr.h
#[repr(C)]
pub struct ItemPointerData {
    pub ip_blkid: [u16; 2],
    pub ip_posid: u16,
}

// storage/bufmgr.h
pub const InvalidBuffer: Buffer = 0;
pub const MAIN_FORKNUM: c_int = 0;

// storage/off.h
pub const InvalidOffsetNumber: OffsetNumber = 0;
pub const FirstOffsetNumber: OffsetNumber = 1;
#[inline]
fn OffsetNumberNext(offsetNumber: OffsetNumber) -> OffsetNumber {
    offsetNumber + 1
}

const BLCKSZ: usize = 8192;
const SizeOfPageHeaderData: Size = 24;
const VARHDRSZ: Size = 4;

/* storage/itemid.h: ItemIdData is 4 bytes. */
#[repr(C)]
pub struct ItemIdData {
    pub bits: u32,
}

/* Step of index tuples for check whether to switch to buffering build mode */
const BUFFERING_MODE_SWITCH_CHECK_STEP: int64 = 256;

/*
 * Number of tuples to process in the slow way before switching to buffering
 * mode, when buffering is explicitly turned on. Also, the number of tuples
 * to process between readjusting the buffer size parameter, while in
 * buffering mode.
 */
const BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET: int64 = 4096;

/*
 * Strategy used to build the index. It can change between the
 * GIST_BUFFERING_* modes on the fly, but if the Sorted method is used,
 * that needs to be decided up-front and cannot be changed afterwards.
 */
#[derive(PartialEq, Eq, Clone, Copy)]
#[repr(C)]
pub enum GistBuildMode {
    GIST_SORTED_BUILD,       /* bottom-up build by sorting */
    GIST_BUFFERING_DISABLED, /* in regular build mode and aren't going to switch */
    GIST_BUFFERING_AUTO,     /* in regular build mode, but will switch to
                              * buffering build mode if the index grows too big */
    GIST_BUFFERING_STATS,    /* gathering statistics of index tuple size
                              * before switching to the buffering build mode */
    GIST_BUFFERING_ACTIVE,   /* in buffering build mode */
}
pub use GistBuildMode::*;

/* Working state for gistbuild and its callback */
#[repr(C)]
pub struct GISTBuildState {
    pub indexrel: Relation,
    pub heaprel: Relation,
    pub giststate: *mut GISTSTATE,

    pub freespace: Size, /* amount of free space to leave on pages */

    pub buildMode: GistBuildMode,

    pub indtuples: int64, /* number of tuples indexed */

    /*
     * Extra data structures used during a buffering build. 'gfbb' contains
     * information related to managing the build buffers. 'parentMap' is a
     * lookup table of the parent of each internal page.
     */
    pub indtuplesSize: int64, /* total size of all indexed tuples */
    pub gfbb: *mut GISTBuildBuffers,
    pub parentMap: *mut HTAB,

    /*
     * Extra data structures used during a sorting build.
     */
    pub sortstate: *mut Tuplesortstate, /* state data for tuplesort.c */

    pub pages_allocated: BlockNumber,

    pub bulkstate: *mut BulkWriteState,
}

const GIST_SORTED_BUILD_PAGE_NUM: usize = 4;

/*
 * In sorted build, we use a stack of these structs, one for each level,
 * to hold an in-memory buffer of last pages at the level.
 *
 * Sorting GiST build requires good linearization of the sort opclass. This is
 * not always the case in multidimensional data. To tackle the anomalies, we
 * buffer index tuples and apply picksplit that can be multidimension-aware.
 */
#[repr(C)]
pub struct GistSortedBuildLevelState {
    pub current_page: c_int,
    pub last_blkno: BlockNumber,
    pub parent: *mut GistSortedBuildLevelState, /* Upper level, if any */
    pub pages: [Page; GIST_SORTED_BUILD_PAGE_NUM],
}

// ----------------------------------------------------------------
// GiST page macros (private copies; access/gist.h not yet pub-exporting them).
// ----------------------------------------------------------------

/// access/gist.h: special-area struct at the end of every GiST index page.
#[repr(C)]
pub struct GISTPageOpaqueData {
    pub nsn: XLogRecPtr,
    pub rightlink: BlockNumber,
    pub flags: uint16,
    pub gist_page_id: uint16,
}

/* #define GistPageGetOpaque(page) ((GISTPageOpaque) PageGetSpecialPointer(page)) */
#[inline]
unsafe fn GistPageGetOpaque(page: Page) -> *mut GISTPageOpaqueData {
    PageGetSpecialPointer(page) as *mut GISTPageOpaqueData
}

/* #define GistPageIsLeaf(page) (GistPageGetOpaque(page)->flags & F_LEAF) */
#[inline]
unsafe fn GistPageIsLeaf(page: Page) -> bool {
    ((*GistPageGetOpaque(page)).flags & F_LEAF) != 0
}

// ----------------------------------------------------------------
// TODO(pg-port): dependencies that live in other .c files not yet wired here.
// ----------------------------------------------------------------

pub type HTAB = c_void;
pub type BulkWriteState = c_void;
pub type BulkWriteBuffer = *mut c_void;
pub type IndexInfo = c_void;

// nodes/execnodes.h: IndexBuildResult
#[repr(C)]
pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

// utils/hsearch.h
#[repr(C)]
pub struct HASHCTL {
    pub keysize: Size,
    pub entrysize: Size,
    pub hcxt: MemoryContext,
}
pub const HASH_ELEM: c_int = 0x0008;
pub const HASH_BLOBS: c_int = 0x0010;
pub const HASH_CONTEXT: c_int = 0x0040;
#[derive(PartialEq, Eq)]
#[repr(C)]
pub enum HASHACTION {
    HASH_FIND,
    HASH_ENTER,
    HASH_REMOVE,
    HASH_ENTER_NULL,
}
pub use HASHACTION::*;

pub type AttrNumber = i16;

/* GUC variables (other .c). */
pub static mut maintenance_work_mem: c_int = 0;
pub static mut effective_cache_size: c_int = 0;

/* access/gist.h: WAL-skipping LSN sentinel used during index build. */
pub const GistBuildLSN: XLogRecPtr = 0;

// CompactAttribute (access/tupdesc.h) accessor.
#[repr(C)]
pub struct CompactAttribute {
    pub attlen: i16,
}

// IndexBuildCallback (access/tableam.h)
pub type IndexBuildCallback = unsafe fn(
    index: Relation,
    tid: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
    tupleIsAlive: bool,
    state: *mut c_void,
);

extern "C" {
    fn rint(x: f64) -> f64;
    fn pow(x: f64, y: f64) -> f64;
}

// ----------------------------------------------------------------
// TODO(pg-port) stubbed externs (defined in other .c translation units).
// ----------------------------------------------------------------

unsafe fn RelationGetNumberOfBlocks(_relation: Relation) -> BlockNumber { crate::access::nbtree::nbtpage::RelationGetNumberOfBlocks(_relation) }
unsafe fn RelationGetRelationName(_relation: Relation) -> *const c_char {
    // TODO(pg-port): utils/rel.h
    unimplemented!("RelationGetRelationName")
}
unsafe fn IndexRelationGetNumberOfKeyAttributes(_relation: Relation) -> c_int { crate::access::nbtree::nbtdedup::IndexRelationGetNumberOfKeyAttributes(_relation) }
unsafe fn RelationNeedsWAL(_relation: Relation) -> bool { crate::access::nbtree::nbtdedup::RelationNeedsWAL(_relation) }
unsafe fn RelationGetSmgr(_reln: Relation) -> *mut c_void {
    // TODO(pg-port): utils/rel.h
    unimplemented!("RelationGetSmgr")
}
unsafe fn index_getprocid(_irel: Relation, _attnum: AttrNumber, _procnum: u16) -> Oid { crate::access::index::indexam::index_getprocid(_irel, _attnum, _procnum as _) as _ }
unsafe fn OidIsValid(objectId: Oid) -> bool {
    objectId != 0
}
unsafe fn table_index_build_scan(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _indexInfo: *mut IndexInfo,
    _allow_sync: bool,
    _anyvisible: bool,
    _callback: IndexBuildCallback,
    _callback_state: *mut c_void,
    _scan: *mut c_void,
) -> f64 {
    // TODO(pg-port): access/tableam.h
    unimplemented!("table_index_build_scan")
}
unsafe fn smgrnblocks(_reln: *mut c_void, _forknum: c_int) -> BlockNumber {
    // TODO(pg-port): storage/smgr/smgr.c
    unimplemented!("smgrnblocks")
}
unsafe fn log_newpage_range(
    _rel: Relation,
    _forknum: c_int,
    _startblk: BlockNumber,
    _endblk: BlockNumber,
    _page_std: bool,
) {
    // TODO(pg-port): access/transam/xloginsert.c
    unimplemented!("log_newpage_range")
}
unsafe fn smgr_bulk_start_rel(_rel: Relation, _forknum: c_int) -> *mut BulkWriteState {
    // TODO(pg-port): storage/smgr/bulk_write.c
    unimplemented!("smgr_bulk_start_rel")
}
unsafe fn smgr_bulk_get_buf(_bulkstate: *mut BulkWriteState) -> BulkWriteBuffer { crate::storage::smgr::bulk_write::smgr_bulk_get_buf(_bulkstate) }
unsafe fn smgr_bulk_write(
    _bulkstate: *mut BulkWriteState,
    _blocknum: BlockNumber,
    _buf: BulkWriteBuffer,
    _page_std: bool,
) { crate::storage::smgr::bulk_write::smgr_bulk_write(_bulkstate, _blocknum, _buf, _page_std) }
unsafe fn smgr_bulk_finish(_bulkstate: *mut BulkWriteState) { crate::storage::smgr::bulk_write::smgr_bulk_finish(_bulkstate) }
unsafe fn hash_create(
    _tabname: *const c_char,
    _nelem: c_long,
    _info: *mut HASHCTL,
    _flags: c_int,
) -> *mut HTAB {
    // TODO(pg-port): utils/hash/dynahash.c
    unimplemented!("hash_create")
}
unsafe fn hash_search(
    _hashp: *mut HTAB,
    _keyPtr: *const c_void,
    _action: HASHACTION,
    _foundPtr: *mut bool,
) -> *mut c_void {
    // TODO(pg-port): utils/hash/dynahash.c
    unimplemented!("hash_search")
}
unsafe fn ReadBuffer(_reln: Relation, _blockNum: BlockNumber) -> Buffer {
    // TODO(pg-port): storage/buffer/bufmgr.c
    unimplemented!("ReadBuffer")
}
unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    // TODO(pg-port): storage/buffer/bufmgr.c
    unimplemented!("LockBuffer")
}
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    // TODO(pg-port): storage/buffer/bufmgr.c
    unimplemented!("MarkBufferDirty")
}
unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    // TODO(pg-port): storage/buffer/bufmgr.c
    unimplemented!("UnlockReleaseBuffer")
}
unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    // TODO(pg-port): storage/buffer/bufmgr.c
    unimplemented!("BufferGetPage")
}
unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    // TODO(pg-port): storage/buffer/bufmgr.c
    unimplemented!("BufferGetBlockNumber")
}
unsafe fn PageGetSpecialPointer(_page: Page) -> *mut c_char { crate::storage::bufpage::PageGetSpecialPointer(_page) }
unsafe fn PageGetItemId(_page: Page, _offsetNumber: OffsetNumber) -> *mut ItemIdData {
    // TODO(pg-port): storage/bufpage.h
    unimplemented!("PageGetItemId")
}
unsafe fn PageGetItem(_page: Page, _itemId: *mut ItemIdData) -> *mut c_char {
    // TODO(pg-port): storage/bufpage.h
    unimplemented!("PageGetItem")
}
unsafe fn PageGetMaxOffsetNumber(_page: Page) -> OffsetNumber {
    // TODO(pg-port): storage/bufpage.h
    unimplemented!("PageGetMaxOffsetNumber")
}
unsafe fn PageGetFreeSpace(_page: Page) -> Size {
    // TODO(pg-port): storage/bufpage.c
    unimplemented!("PageGetFreeSpace")
}
unsafe fn PageAddItem(
    _page: Page,
    _item: *mut c_char,
    _size: Size,
    _offsetNumber: OffsetNumber,
    _overwrite: bool,
    _is_heap: bool,
) -> OffsetNumber {
    // TODO(pg-port): storage/bufpage.c
    unimplemented!("PageAddItem")
}
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) { crate::storage::bufpage::PageSetLSN(_page, _lsn) }
unsafe fn ItemPointerGetBlockNumber(_pointer: *const ItemPointerData) -> BlockNumber {
    // TODO(pg-port): storage/itemptr.h
    unimplemented!("ItemPointerGetBlockNumber")
}
unsafe fn ItemPointerSetBlockNumber(_pointer: *mut ItemPointerData, _blockNumber: BlockNumber) { crate::storage::itemptr::ItemPointerSetBlockNumber(_pointer, _blockNumber) }
unsafe fn TupleDescCompactAttr(_tupdesc: *mut c_void, _i: c_int) -> *mut CompactAttribute {
    // TODO(pg-port): access/tupdesc.h
    unimplemented!("TupleDescCompactAttr")
}
unsafe fn START_CRIT_SECTION() {
    // TODO(pg-port): miscadmin.h
    unimplemented!("START_CRIT_SECTION")
}
unsafe fn END_CRIT_SECTION() {
    // TODO(pg-port): miscadmin.h
    unimplemented!("END_CRIT_SECTION")
}
unsafe fn CHECK_FOR_INTERRUPTS() {
    // TODO(pg-port): miscadmin.h
}

/*
 * Main entry point to GiST index build.
 */
pub unsafe fn gistbuild(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult {
    let result: *mut IndexBuildResult;
    let reltuples: f64;
    let mut buildstate: GISTBuildState = core::mem::zeroed();
    let oldcxt: MemoryContext = CurrentMemoryContext;
    let fillfactor: c_int;
    let mut SortSupportFnOids: [Oid; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let options: *mut GiSTOptions = (*index).rd_options as *mut GiSTOptions;

    /*
     * We expect to be called exactly once for any index relation. If that's
     * not the case, big trouble's what we have.
     */
    if RelationGetNumberOfBlocks(index) != 0 {
        elog!(
            ERROR,
            "index \"{}\" already contains data",
            CStr::from_ptr(RelationGetRelationName(index)).to_string_lossy()
        );
    }

    buildstate.indexrel = index;
    buildstate.heaprel = heap;
    buildstate.sortstate = null_mut();
    buildstate.giststate = initGISTstate(index);

    /*
     * Create a temporary memory context that is reset once for each tuple
     * processed.  (Note: we don't bother to make this a child of the
     * giststate's scanCxt, so we have to delete it separately at the end.)
     */
    (*buildstate.giststate).tempCxt = createTempGistContext();

    /*
     * Choose build strategy.  First check whether the user specified to use
     * buffering mode.  (The use-case for that in the field is somewhat
     * questionable perhaps, but it's important for testing purposes.)
     */
    if !options.is_null() {
        if (*options).buffering_mode == GIST_OPTION_BUFFERING_ON {
            buildstate.buildMode = GIST_BUFFERING_STATS;
        } else if (*options).buffering_mode == GIST_OPTION_BUFFERING_OFF {
            buildstate.buildMode = GIST_BUFFERING_DISABLED;
        } else {
            /* must be "auto" */
            buildstate.buildMode = GIST_BUFFERING_AUTO;
        }
    } else {
        buildstate.buildMode = GIST_BUFFERING_AUTO;
    }

    /*
     * Unless buffering mode was forced, see if we can use sorting instead.
     */
    if buildstate.buildMode != GIST_BUFFERING_STATS {
        let mut hasallsortsupports = true;
        let keyscount = IndexRelationGetNumberOfKeyAttributes(index);

        for i in 0..keyscount {
            SortSupportFnOids[i as usize] =
                index_getprocid(index, (i + 1) as AttrNumber, GIST_SORTSUPPORT_PROC as u16);
            if !OidIsValid(SortSupportFnOids[i as usize]) {
                hasallsortsupports = false;
                break;
            }
        }
        if hasallsortsupports {
            buildstate.buildMode = GIST_SORTED_BUILD;
        }
    }

    /*
     * Calculate target amount of free space to leave on pages.
     */
    fillfactor = if !options.is_null() {
        (*options).fillfactor
    } else {
        GIST_DEFAULT_FILLFACTOR
    };
    buildstate.freespace = (BLCKSZ as c_int * (100 - fillfactor) / 100) as Size;

    /*
     * Build the index using the chosen strategy.
     */
    buildstate.indtuples = 0;
    buildstate.indtuplesSize = 0;

    if buildstate.buildMode == GIST_SORTED_BUILD {
        /*
         * Sort all data, build the index from bottom up.
         */
        buildstate.sortstate = tuplesort_begin_index_gist(
            heap,
            index,
            maintenance_work_mem,
            null_mut(),
            TUPLESORT_NONE,
        );

        /* Scan the table, adding all tuples to the tuplesort */
        reltuples = table_index_build_scan(
            heap,
            index,
            indexInfo,
            true,
            true,
            gistSortedBuildCallback,
            (&mut buildstate as *mut GISTBuildState) as *mut c_void,
            null_mut(),
        );

        /*
         * Perform the sort and build index pages.
         */
        tuplesort_performsort(buildstate.sortstate);

        gist_indexsortbuild(&mut buildstate);

        tuplesort_end(buildstate.sortstate);
    } else {
        /*
         * Initialize an empty index and insert all tuples, possibly using
         * buffers on intermediate levels.
         */
        let buffer: Buffer;
        let page: Page;

        /* initialize the root page */
        buffer = gistNewBuffer(index, heap);
        Assert!(BufferGetBlockNumber(buffer) == GIST_ROOT_BLKNO);
        page = BufferGetPage(buffer);

        START_CRIT_SECTION();

        GISTInitBuffer(buffer, F_LEAF as uint32);

        MarkBufferDirty(buffer);
        PageSetLSN(page, GistBuildLSN);

        UnlockReleaseBuffer(buffer);

        END_CRIT_SECTION();

        /* Scan the table, inserting all the tuples to the index. */
        reltuples = table_index_build_scan(
            heap,
            index,
            indexInfo,
            true,
            true,
            gistBuildCallback,
            (&mut buildstate as *mut GISTBuildState) as *mut c_void,
            null_mut(),
        );

        /*
         * If buffering was used, flush out all the tuples that are still in
         * the buffers.
         */
        if buildstate.buildMode == GIST_BUFFERING_ACTIVE {
            elog!(DEBUG1, "all tuples processed, emptying buffers");
            gistEmptyAllBuffers(&mut buildstate);
            gistFreeBuildBuffers(buildstate.gfbb);
        }

        /*
         * We didn't write WAL records as we built the index, so if
         * WAL-logging is required, write all pages to the WAL now.
         */
        if RelationNeedsWAL(index) {
            log_newpage_range(
                index,
                MAIN_FORKNUM,
                0,
                RelationGetNumberOfBlocks(index),
                true,
            );
        }
    }

    /* okay, all heap tuples are indexed */
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete((*buildstate.giststate).tempCxt);

    freeGISTstate(buildstate.giststate);

    /*
     * Return statistics
     */
    result = palloc(core::mem::size_of::<IndexBuildResult>()) as *mut IndexBuildResult;

    (*result).heap_tuples = reltuples;
    (*result).index_tuples = buildstate.indtuples as f64;

    result
}

/*-------------------------------------------------------------------------
 * Routines for sorted build
 *-------------------------------------------------------------------------
 */

/*
 * Per-tuple callback for table_index_build_scan.
 */
unsafe fn gistSortedBuildCallback(
    index: Relation,
    tid: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
    _tupleIsAlive: bool,
    state: *mut c_void,
) {
    let buildstate: *mut GISTBuildState = state as *mut GISTBuildState;
    let oldCtx: MemoryContext;
    let mut compressed_values: [Datum; INDEX_MAX_KEYS] = [Datum::from(0u64); INDEX_MAX_KEYS];

    oldCtx = MemoryContextSwitchTo((*(*buildstate).giststate).tempCxt);

    /* Form an index tuple and point it at the heap tuple */
    gistCompressValues(
        (*buildstate).giststate,
        index,
        values,
        isnull,
        true,
        compressed_values.as_mut_ptr(),
    );

    tuplesort_putindextuplevalues(
        (*buildstate).sortstate,
        (*buildstate).indexrel,
        tid,
        compressed_values.as_mut_ptr(),
        isnull,
    );

    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset((*(*buildstate).giststate).tempCxt);

    /* Update tuple count. */
    (*buildstate).indtuples += 1;
}

/*
 * Build GiST index from bottom up from pre-sorted tuples.
 */
unsafe fn gist_indexsortbuild(state: *mut GISTBuildState) {
    let mut itup: IndexTuple;
    let mut levelstate: *mut GistSortedBuildLevelState;
    let rootbuf: BulkWriteBuffer;

    /* Reserve block 0 for the root page */
    (*state).pages_allocated = 1;

    (*state).bulkstate = smgr_bulk_start_rel((*state).indexrel, MAIN_FORKNUM);

    /* Allocate a temporary buffer for the first leaf page batch. */
    levelstate = palloc0(core::mem::size_of::<GistSortedBuildLevelState>())
        as *mut GistSortedBuildLevelState;
    (*levelstate).pages[0] = palloc(BLCKSZ) as Page;
    (*levelstate).parent = null_mut();
    gistinitpage((*levelstate).pages[0], F_LEAF as uint32);

    /*
     * Fill index pages with tuples in the sorted order.
     */
    loop {
        itup = tuplesort_getindextuple((*state).sortstate, true);
        if itup.is_null() {
            break;
        }
        gist_indexsortbuild_levelstate_add(state, levelstate, itup);
        MemoryContextReset((*(*state).giststate).tempCxt);
    }

    /*
     * Write out the partially full non-root pages.
     *
     * Keep in mind that flush can build a new root. If number of pages is > 1
     * then new root is required.
     */
    while !(*levelstate).parent.is_null() || (*levelstate).current_page != 0 {
        let parent: *mut GistSortedBuildLevelState;

        gist_indexsortbuild_levelstate_flush(state, levelstate);
        parent = (*levelstate).parent;
        for i in 0..GIST_SORTED_BUILD_PAGE_NUM {
            if !(*levelstate).pages[i].is_null() {
                pfree((*levelstate).pages[i] as *mut c_void);
            }
        }
        pfree(levelstate as *mut c_void);
        levelstate = parent;
    }

    /* Write out the root */
    PageSetLSN((*levelstate).pages[0], GistBuildLSN);
    rootbuf = smgr_bulk_get_buf((*state).bulkstate);
    core::ptr::copy_nonoverlapping(
        (*levelstate).pages[0] as *const u8,
        rootbuf as *mut u8,
        BLCKSZ,
    );
    smgr_bulk_write((*state).bulkstate, GIST_ROOT_BLKNO, rootbuf, true);

    pfree(levelstate as *mut c_void);

    smgr_bulk_finish((*state).bulkstate);
}

/*
 * Add tuple to a page. If the pages are full, write them out and re-initialize
 * a new page first.
 */
unsafe fn gist_indexsortbuild_levelstate_add(
    state: *mut GISTBuildState,
    levelstate: *mut GistSortedBuildLevelState,
    mut itup: IndexTuple,
) {
    let sizeNeeded: Size;

    /* Check if tuple can be added to the current page */
    sizeNeeded = IndexTupleSize(itup) as Size + core::mem::size_of::<ItemIdData>() as Size; /* fillfactor ignored */
    if PageGetFreeSpace((*levelstate).pages[(*levelstate).current_page as usize]) < sizeNeeded {
        let newPage: Page;
        let old_page: Page = (*levelstate).pages[(*levelstate).current_page as usize];
        let old_page_flags: uint16 = (*GistPageGetOpaque(old_page)).flags;

        if (*levelstate).current_page as usize + 1 == GIST_SORTED_BUILD_PAGE_NUM {
            gist_indexsortbuild_levelstate_flush(state, levelstate);
        } else {
            (*levelstate).current_page += 1;
        }

        if (*levelstate).pages[(*levelstate).current_page as usize].is_null() {
            (*levelstate).pages[(*levelstate).current_page as usize] = palloc0(BLCKSZ) as Page;
        }

        newPage = (*levelstate).pages[(*levelstate).current_page as usize];
        gistinitpage(newPage, old_page_flags as uint32);
    }

    gistfillbuffer(
        (*levelstate).pages[(*levelstate).current_page as usize],
        &mut itup,
        1,
        InvalidOffsetNumber,
    );
}

unsafe fn gist_indexsortbuild_levelstate_flush(
    state: *mut GISTBuildState,
    levelstate: *mut GistSortedBuildLevelState,
) {
    let mut parent: *mut GistSortedBuildLevelState;
    let mut blkno: BlockNumber;
    let oldCtx: MemoryContext;
    let mut union_tuple: IndexTuple;
    let mut dist: *mut SplitPageLayout;
    let mut itvec: *mut IndexTuple;
    let mut vect_len: c_int = 0;
    let isleaf: bool = GistPageIsLeaf((*levelstate).pages[0]);

    CHECK_FOR_INTERRUPTS();

    oldCtx = MemoryContextSwitchTo((*(*state).giststate).tempCxt);

    /* Get index tuples from first page */
    itvec = gistextractpage((*levelstate).pages[0], &mut vect_len);
    if (*levelstate).current_page > 0 {
        /* Append tuples from each page */
        for i in 1..(*levelstate).current_page + 1 {
            let mut len_local: c_int = 0;
            let itvec_local: *mut IndexTuple =
                gistextractpage((*levelstate).pages[i as usize], &mut len_local);

            itvec = gistjoinvector(itvec, &mut vect_len, itvec_local, len_local);
            pfree(itvec_local as *mut c_void);
        }

        /* Apply picksplit to list of all collected tuples */
        dist = gistSplit(
            (*state).indexrel,
            (*levelstate).pages[0],
            itvec,
            vect_len,
            (*state).giststate,
        );
    } else {
        /* Create split layout from single page */
        dist = palloc0(core::mem::size_of::<SplitPageLayout>()) as *mut SplitPageLayout;
        union_tuple = gistunion((*state).indexrel, itvec, vect_len, (*state).giststate);
        (*dist).itup = union_tuple;
        (*dist).list = gistfillitupvec(itvec, vect_len, &mut (*dist).lenlist);
        (*dist).block.num = vect_len;
    }

    MemoryContextSwitchTo(oldCtx);

    /* Reset page counter */
    (*levelstate).current_page = 0;

    /* Create pages for all partitions in split result */
    while !dist.is_null() {
        let mut data: *mut c_char;
        let buf: BulkWriteBuffer;
        let target: Page;

        /* check once per page */
        CHECK_FOR_INTERRUPTS();

        /* Create page and copy data */
        data = (*dist).list as *mut c_char;
        buf = smgr_bulk_get_buf((*state).bulkstate);
        target = buf as Page;
        gistinitpage(target, if isleaf { F_LEAF as uint32 } else { 0 });
        for i in 0..(*dist).block.num {
            let thistup: IndexTuple = data as IndexTuple;

            if PageAddItem(
                target,
                data,
                IndexTupleSize(thistup) as Size,
                (i + FirstOffsetNumber as c_int) as OffsetNumber,
                false,
                false,
            ) == InvalidOffsetNumber
            {
                elog!(
                    ERROR,
                    "failed to add item to index page in \"{}\"",
                    CStr::from_ptr(RelationGetRelationName((*state).indexrel)).to_string_lossy()
                );
            }

            data = data.add(IndexTupleSize(thistup) as usize);
        }
        union_tuple = (*dist).itup;

        /*
         * Set the right link to point to the previous page. This is just for
         * debugging purposes: GiST only follows the right link if a page is
         * split concurrently to a scan, and that cannot happen during index
         * build.
         *
         * It's a bit counterintuitive that we set the right link on the new
         * page to point to the previous page, not the other way around. But
         * GiST pages are not ordered like B-tree pages are, so as long as the
         * right-links form a chain through all the pages at the same level,
         * the order doesn't matter.
         */
        if (*levelstate).last_blkno != 0 {
            (*GistPageGetOpaque(target)).rightlink = (*levelstate).last_blkno;
        }

        /*
         * The page is now complete. Assign a block number to it, and pass it
         * to the bulk writer.
         */
        blkno = (*state).pages_allocated;
        (*state).pages_allocated += 1;
        PageSetLSN(target, GistBuildLSN);
        smgr_bulk_write((*state).bulkstate, blkno, buf, true);
        ItemPointerSetBlockNumber(&mut (*union_tuple).t_tid, blkno);
        (*levelstate).last_blkno = blkno;

        /*
         * Insert the downlink to the parent page. If this was the root,
         * create a new page as the parent, which becomes the new root.
         */
        parent = (*levelstate).parent;
        if parent.is_null() {
            parent = palloc0(core::mem::size_of::<GistSortedBuildLevelState>())
                as *mut GistSortedBuildLevelState;
            (*parent).pages[0] = palloc(BLCKSZ) as Page;
            (*parent).parent = null_mut();
            gistinitpage((*parent).pages[0], 0);

            (*levelstate).parent = parent;
        }
        gist_indexsortbuild_levelstate_add(state, parent, union_tuple);

        dist = (*dist).next;
    }
}

/*-------------------------------------------------------------------------
 * Routines for non-sorted build
 *-------------------------------------------------------------------------
 */

/*
 * Attempt to switch to buffering mode.
 *
 * If there is not enough memory for buffering build, sets bufferingMode
 * to GIST_BUFFERING_DISABLED, so that we don't bother to try the switch
 * anymore. Otherwise initializes the build buffers, and sets bufferingMode to
 * GIST_BUFFERING_ACTIVE.
 */
unsafe fn gistInitBuffering(buildstate: *mut GISTBuildState) {
    let index: Relation = (*buildstate).indexrel;
    let pagesPerBuffer: c_int;
    let pageFreeSpace: Size;
    let itupAvgSize: Size;
    let mut itupMinSize: Size;
    let avgIndexTuplesPerPage: f64;
    let maxIndexTuplesPerPage: f64;
    let mut levelStep: c_int;

    /* Calc space of index page which is available for index tuples */
    pageFreeSpace = BLCKSZ as Size
        - SizeOfPageHeaderData
        - core::mem::size_of::<GISTPageOpaqueData>() as Size
        - core::mem::size_of::<ItemIdData>() as Size
        - (*buildstate).freespace;

    /*
     * Calculate average size of already inserted index tuples using gathered
     * statistics.
     */
    itupAvgSize =
        ((*buildstate).indtuplesSize as f64 / (*buildstate).indtuples as f64) as Size;

    /*
     * Calculate minimal possible size of index tuple by index metadata.
     * Minimal possible size of varlena is VARHDRSZ.
     *
     * XXX: that's not actually true, as a short varlen can be just 2 bytes.
     * And we should take padding into account here.
     */
    itupMinSize = MAXALIGN(core::mem::size_of::<IndexTupleData>() as Size) as Size;
    for i in 0..(*(*index).rd_att).natts {
        let attr: *mut CompactAttribute = TupleDescCompactAttr((*index).rd_att as *mut c_void, i);

        if (*attr).attlen < 0 {
            itupMinSize += VARHDRSZ;
        } else {
            itupMinSize += (*attr).attlen as Size;
        }
    }

    /* Calculate average and maximal number of index tuples which fit to page */
    avgIndexTuplesPerPage = pageFreeSpace as f64 / itupAvgSize as f64;
    maxIndexTuplesPerPage = pageFreeSpace as f64 / itupMinSize as f64;

    /*
     * We need to calculate two parameters for the buffering algorithm:
     * levelStep and pagesPerBuffer.
     *
     * levelStep determines the size of subtree that we operate on, while
     * emptying a buffer. A higher value is better, as you need fewer buffer
     * emptying steps to build the index. However, if you set it too high, the
     * subtree doesn't fit in cache anymore, and you quickly lose the benefit
     * of the buffers.
     *
     * In Arge et al's paper, levelStep is chosen as logB(M/4B), where B is
     * the number of tuples on page (ie. fanout), and M is the amount of
     * internal memory available. Curiously, they doesn't explain *why* that
     * setting is optimal. We calculate it by taking the highest levelStep so
     * that a subtree still fits in cache. For a small B, our way of
     * calculating levelStep is very close to Arge et al's formula. For a
     * large B, our formula gives a value that is 2x higher.
     *
     * The average size (in pages) of a subtree of depth n can be calculated
     * as a geometric series:
     *
     * B^0 + B^1 + B^2 + ... + B^n = (1 - B^(n + 1)) / (1 - B)
     *
     * where B is the average number of index tuples on page. The subtree is
     * cached in the shared buffer cache and the OS cache, so we choose
     * levelStep so that the subtree size is comfortably smaller than
     * effective_cache_size, with a safety factor of 4.
     *
     * The estimate on the average number of index tuples on page is based on
     * average tuple sizes observed before switching to buffered build, so the
     * real subtree size can be somewhat larger. Also, it would selfish to
     * gobble the whole cache for our index build. The safety factor of 4
     * should account for those effects.
     *
     * The other limiting factor for setting levelStep is that while
     * processing a subtree, we need to hold one page for each buffer at the
     * next lower buffered level. The max. number of buffers needed for that
     * is maxIndexTuplesPerPage^levelStep. This is very conservative, but
     * hopefully maintenance_work_mem is set high enough that you're
     * constrained by effective_cache_size rather than maintenance_work_mem.
     *
     * XXX: the buffer hash table consumes a fair amount of memory too per
     * buffer, but that is not currently taken into account. That scales on
     * the total number of buffers used, ie. the index size and on levelStep.
     * Note that a higher levelStep *reduces* the amount of memory needed for
     * the hash table.
     */
    levelStep = 1;
    loop {
        let subtreesize: f64;
        let maxlowestlevelpages: f64;

        /* size of an average subtree at this levelStep (in pages). */
        subtreesize = (1.0 - pow(avgIndexTuplesPerPage, (levelStep + 1) as f64))
            / (1.0 - avgIndexTuplesPerPage);

        /* max number of pages at the lowest level of a subtree */
        maxlowestlevelpages = pow(maxIndexTuplesPerPage, levelStep as f64);

        /* subtree must fit in cache (with safety factor of 4) */
        if subtreesize > effective_cache_size as f64 / 4.0 {
            break;
        }

        /* each node in the lowest level of a subtree has one page in memory */
        if maxlowestlevelpages > (maintenance_work_mem as f64 * 1024.0) / BLCKSZ as f64 {
            break;
        }

        /* Good, we can handle this levelStep. See if we can go one higher. */
        levelStep += 1;
    }

    /*
     * We just reached an unacceptable value of levelStep in previous loop.
     * So, decrease levelStep to get last acceptable value.
     */
    levelStep -= 1;

    /*
     * If there's not enough cache or maintenance_work_mem, fall back to plain
     * inserts.
     */
    if levelStep <= 0 {
        elog!(DEBUG1, "failed to switch to buffered GiST build");
        (*buildstate).buildMode = GIST_BUFFERING_DISABLED;
        return;
    }

    /*
     * The second parameter to set is pagesPerBuffer, which determines the
     * size of each buffer. We adjust pagesPerBuffer also during the build,
     * which is why this calculation is in a separate function.
     */
    pagesPerBuffer = calculatePagesPerBuffer(buildstate, levelStep);

    /* Initialize GISTBuildBuffers with these parameters */
    (*buildstate).gfbb = gistInitBuildBuffers(pagesPerBuffer, levelStep, gistGetMaxLevel(index));

    gistInitParentMap(buildstate);

    (*buildstate).buildMode = GIST_BUFFERING_ACTIVE;

    elog!(
        DEBUG1,
        "switched to buffered GiST build; level step = {}, pagesPerBuffer = {}",
        levelStep,
        pagesPerBuffer
    );
}

/*
 * Calculate pagesPerBuffer parameter for the buffering algorithm.
 *
 * Buffer size is chosen so that assuming that tuples are distributed
 * randomly, emptying half a buffer fills on average one page in every buffer
 * at the next lower level.
 */
unsafe fn calculatePagesPerBuffer(buildstate: *mut GISTBuildState, levelStep: c_int) -> c_int {
    let pagesPerBuffer: f64;
    let avgIndexTuplesPerPage: f64;
    let itupAvgSize: f64;
    let pageFreeSpace: Size;

    /* Calc space of index page which is available for index tuples */
    pageFreeSpace = BLCKSZ as Size
        - SizeOfPageHeaderData
        - core::mem::size_of::<GISTPageOpaqueData>() as Size
        - core::mem::size_of::<ItemIdData>() as Size
        - (*buildstate).freespace;

    /*
     * Calculate average size of already inserted index tuples using gathered
     * statistics.
     */
    itupAvgSize = (*buildstate).indtuplesSize as f64 / (*buildstate).indtuples as f64;

    avgIndexTuplesPerPage = pageFreeSpace as f64 / itupAvgSize;

    /*
     * Recalculate required size of buffers.
     */
    pagesPerBuffer = 2.0 * pow(avgIndexTuplesPerPage, levelStep as f64);

    rint(pagesPerBuffer) as c_int
}

/*
 * Per-tuple callback for table_index_build_scan.
 */
unsafe fn gistBuildCallback(
    index: Relation,
    tid: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
    _tupleIsAlive: bool,
    state: *mut c_void,
) {
    let buildstate: *mut GISTBuildState = state as *mut GISTBuildState;
    let itup: IndexTuple;
    let oldCtx: MemoryContext;

    oldCtx = MemoryContextSwitchTo((*(*buildstate).giststate).tempCxt);

    /* form an index tuple and point it at the heap tuple */
    itup = gistFormTuple((*buildstate).giststate, index, values, isnull, true);
    (*itup).t_tid = core::ptr::read(tid);

    /* Update tuple count and total size. */
    (*buildstate).indtuples += 1;
    (*buildstate).indtuplesSize += IndexTupleSize(itup) as int64;

    /*
     * XXX In buffering builds, the tempCxt is also reset down inside
     * gistProcessEmptyingQueue().  This is not great because it risks
     * confusion and possible use of dangling pointers (for example, itup
     * might be already freed when control returns here).  It's generally
     * better that a memory context be "owned" by only one function.  However,
     * currently this isn't causing issues so it doesn't seem worth the amount
     * of refactoring that would be needed to avoid it.
     */
    if (*buildstate).buildMode == GIST_BUFFERING_ACTIVE {
        /* We have buffers, so use them. */
        gistBufferingBuildInsert(buildstate, itup);
    } else {
        /*
         * There's no buffers (yet). Since we already have the index relation
         * locked, we call gistdoinsert directly.
         */
        gistdoinsert(
            index,
            itup,
            (*buildstate).freespace,
            (*buildstate).giststate,
            (*buildstate).heaprel,
            true,
        );
    }

    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset((*(*buildstate).giststate).tempCxt);

    if (*buildstate).buildMode == GIST_BUFFERING_ACTIVE
        && (*buildstate).indtuples % BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET == 0
    {
        /* Adjust the target buffer size now */
        (*(*buildstate).gfbb).pagesPerBuffer =
            calculatePagesPerBuffer(buildstate, (*(*buildstate).gfbb).levelStep);
    }

    /*
     * In 'auto' mode, check if the index has grown too large to fit in cache,
     * and switch to buffering mode if it has.
     *
     * To avoid excessive calls to smgrnblocks(), only check this every
     * BUFFERING_MODE_SWITCH_CHECK_STEP index tuples.
     *
     * In 'stats' state, switch as soon as we have seen enough tuples to have
     * some idea of the average tuple size.
     */
    if ((*buildstate).buildMode == GIST_BUFFERING_AUTO
        && (*buildstate).indtuples % BUFFERING_MODE_SWITCH_CHECK_STEP == 0
        && (effective_cache_size as BlockNumber)
            < smgrnblocks(RelationGetSmgr(index), MAIN_FORKNUM))
        || ((*buildstate).buildMode == GIST_BUFFERING_STATS
            && (*buildstate).indtuples >= BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET)
    {
        /*
         * Index doesn't fit in effective cache anymore. Try to switch to
         * buffering build mode.
         */
        gistInitBuffering(buildstate);
    }
}

/*
 * Insert function for buffering index build.
 */
unsafe fn gistBufferingBuildInsert(buildstate: *mut GISTBuildState, itup: IndexTuple) {
    /* Insert the tuple to buffers. */
    gistProcessItup(buildstate, itup, 0, (*(*buildstate).gfbb).rootlevel);

    /* If we filled up (half of a) buffer, process buffer emptying. */
    gistProcessEmptyingQueue(buildstate);
}

/*
 * Process an index tuple. Runs the tuple down the tree until we reach a leaf
 * page or node buffer, and inserts the tuple there. Returns true if we have
 * to stop buffer emptying process (because one of child buffers can't take
 * index tuples anymore).
 */
unsafe fn gistProcessItup(
    buildstate: *mut GISTBuildState,
    itup: IndexTuple,
    startblkno: BlockNumber,
    startlevel: c_int,
) -> bool {
    let giststate: *mut GISTSTATE = (*buildstate).giststate;
    let gfbb: *mut GISTBuildBuffers = (*buildstate).gfbb;
    let indexrel: Relation = (*buildstate).indexrel;
    let mut childblkno: BlockNumber;
    let mut buffer: Buffer;
    let mut result: bool = false;
    let mut blkno: BlockNumber;
    let mut level: c_int;
    let mut downlinkoffnum: OffsetNumber = InvalidOffsetNumber;
    let mut parentblkno: BlockNumber = InvalidBlockNumber;

    CHECK_FOR_INTERRUPTS();

    /*
     * Loop until we reach a leaf page (level == 0) or a level with buffers
     * (not including the level we start at, because we would otherwise make
     * no progress).
     */
    blkno = startblkno;
    level = startlevel;
    loop {
        let iid: *mut ItemIdData;
        let idxtuple: IndexTuple;
        let newtup: IndexTuple;
        let page: Page;
        let childoffnum: OffsetNumber;

        /* Have we reached a level with buffers? */
        if LEVEL_HAS_BUFFERS(level, gfbb) && level != startlevel {
            break;
        }

        /* Have we reached a leaf page? */
        if level == 0 {
            break;
        }

        /*
         * Nope. Descend down to the next level then. Choose a child to
         * descend down to.
         */

        buffer = ReadBuffer(indexrel, blkno);
        LockBuffer(buffer, GIST_EXCLUSIVE);

        page = BufferGetPage(buffer);
        childoffnum = gistchoose(indexrel, page, itup, giststate);
        iid = PageGetItemId(page, childoffnum);
        idxtuple = PageGetItem(page, iid) as IndexTuple;
        childblkno = ItemPointerGetBlockNumber(&(*idxtuple).t_tid);

        if level > 1 {
            gistMemorizeParent(buildstate, childblkno, blkno);
        }

        /*
         * Check that the key representing the target child node is consistent
         * with the key we're inserting. Update it if it's not.
         */
        newtup = gistgetadjusted(indexrel, idxtuple, itup, giststate);
        if !newtup.is_null() {
            let mut newtup_mut = newtup;
            blkno = gistbufferinginserttuples(
                buildstate,
                buffer,
                level,
                &mut newtup_mut,
                1,
                childoffnum,
                InvalidBlockNumber,
                InvalidOffsetNumber,
            );
            /* gistbufferinginserttuples() released the buffer */
        } else {
            UnlockReleaseBuffer(buffer);
        }

        /* Descend to the child */
        parentblkno = blkno;
        blkno = childblkno;
        downlinkoffnum = childoffnum;
        Assert!(level > 0);
        level -= 1;
    }

    if LEVEL_HAS_BUFFERS(level, gfbb) {
        /*
         * We've reached level with buffers. Place the index tuple to the
         * buffer, and add the buffer to the emptying queue if it overflows.
         */
        let childNodeBuffer: *mut GISTNodeBuffer;

        /* Find the buffer or create a new one */
        childNodeBuffer = gistGetNodeBuffer(gfbb, giststate, blkno, level);

        /* Add index tuple to it */
        gistPushItupToNodeBuffer(gfbb, childNodeBuffer, itup);

        if BUFFER_OVERFLOWED(childNodeBuffer, gfbb) {
            result = true;
        }
    } else {
        /*
         * We've reached a leaf page. Place the tuple here.
         */
        Assert!(level == 0);
        let mut itup_mut = itup;
        buffer = ReadBuffer(indexrel, blkno);
        LockBuffer(buffer, GIST_EXCLUSIVE);
        gistbufferinginserttuples(
            buildstate,
            buffer,
            level,
            &mut itup_mut,
            1,
            InvalidOffsetNumber,
            parentblkno,
            downlinkoffnum,
        );
        /* gistbufferinginserttuples() released the buffer */
    }

    result
}

/*
 * Insert tuples to a given page.
 *
 * This is analogous with gistinserttuples() in the regular insertion code.
 *
 * Returns the block number of the page where the (first) new or updated tuple
 * was inserted. Usually that's the original page, but might be a sibling page
 * if the original page was split.
 *
 * Caller should hold a lock on 'buffer' on entry. This function will unlock
 * and unpin it.
 */
unsafe fn gistbufferinginserttuples(
    buildstate: *mut GISTBuildState,
    buffer: Buffer,
    level: c_int,
    itup: *mut IndexTuple,
    ntup: c_int,
    oldoffnum: OffsetNumber,
    mut parentblk: BlockNumber,
    mut downlinkoffnum: OffsetNumber,
) -> BlockNumber {
    let gfbb: *mut GISTBuildBuffers = (*buildstate).gfbb;
    let mut splitinfo: *mut List = null_mut();
    let is_split: bool;
    let mut placed_to_blk: BlockNumber = InvalidBlockNumber;

    is_split = gistplacetopage(
        (*buildstate).indexrel,
        (*buildstate).freespace,
        (*buildstate).giststate,
        buffer,
        itup,
        ntup,
        oldoffnum,
        &mut placed_to_blk,
        InvalidBuffer,
        &mut splitinfo,
        false,
        (*buildstate).heaprel,
        true,
    );

    /*
     * If this is a root split, update the root path item kept in memory. This
     * ensures that all path stacks are always complete, including all parent
     * nodes up to the root. That simplifies the algorithm to re-find correct
     * parent.
     */
    if is_split && BufferGetBlockNumber(buffer) == GIST_ROOT_BLKNO {
        let page: Page = BufferGetPage(buffer);
        let mut off: OffsetNumber;
        let maxoff: OffsetNumber;

        Assert!(level == (*gfbb).rootlevel);
        (*gfbb).rootlevel += 1;

        elog!(
            DEBUG2,
            "splitting GiST root page, now {} levels deep",
            (*gfbb).rootlevel
        );

        /*
         * All the downlinks on the old root page are now on one of the child
         * pages. Visit all the new child pages to memorize the parents of the
         * grandchildren.
         */
        if (*gfbb).rootlevel > 1 {
            maxoff = PageGetMaxOffsetNumber(page);
            off = FirstOffsetNumber;
            while off <= maxoff {
                let iid: *mut ItemIdData = PageGetItemId(page, off);
                let idxtuple: IndexTuple = PageGetItem(page, iid) as IndexTuple;
                let childblkno: BlockNumber = ItemPointerGetBlockNumber(&(*idxtuple).t_tid);
                let childbuf: Buffer = ReadBuffer((*buildstate).indexrel, childblkno);

                LockBuffer(childbuf, GIST_SHARE);
                gistMemorizeAllDownlinks(buildstate, childbuf);
                UnlockReleaseBuffer(childbuf);

                /*
                 * Also remember that the parent of the new child page is the
                 * root block.
                 */
                gistMemorizeParent(buildstate, childblkno, GIST_ROOT_BLKNO);

                off += 1;
            }
        }
    }

    if !splitinfo.is_null() {
        /*
         * Insert the downlinks to the parent. This is analogous with
         * gistfinishsplit() in the regular insertion code, but the locking is
         * simpler, and we have to maintain the buffers on internal nodes and
         * the parent map.
         */
        let downlinks: *mut IndexTuple;
        let ndownlinks: c_int;
        let mut i: c_int;
        let parentBuffer: Buffer;
        // ListCell *lc is declared by the foreach! macro below

        /* Parent may have changed since we memorized this path. */
        parentBuffer = gistBufferingFindCorrectParent(
            buildstate,
            BufferGetBlockNumber(buffer),
            level,
            &mut parentblk,
            &mut downlinkoffnum,
        );

        /*
         * If there's a buffer associated with this page, that needs to be
         * split too. gistRelocateBuildBuffersOnSplit() will also adjust the
         * downlinks in 'splitinfo', to make sure they're consistent not only
         * with the tuples already on the pages, but also the tuples in the
         * buffers that will eventually be inserted to them.
         */
        gistRelocateBuildBuffersOnSplit(
            gfbb,
            (*buildstate).giststate,
            (*buildstate).indexrel,
            level,
            buffer,
            splitinfo,
        );

        /* Create an array of all the downlink tuples */
        ndownlinks = list_length(splitinfo);
        downlinks = palloc(core::mem::size_of::<IndexTuple>() * ndownlinks as usize)
            as *mut IndexTuple;
        i = 0;
        foreach!(lc, splitinfo, {
            let splitinfo: *mut GISTPageSplitInfo = lfirst!(lc) as *mut GISTPageSplitInfo;

            /*
             * Remember the parent of each new child page in our parent map.
             * This assumes that the downlinks fit on the parent page. If the
             * parent page is split, too, when we recurse up to insert the
             * downlinks, the recursive gistbufferinginserttuples() call will
             * update the map again.
             */
            if level > 0 {
                gistMemorizeParent(
                    buildstate,
                    BufferGetBlockNumber((*splitinfo).buf),
                    BufferGetBlockNumber(parentBuffer),
                );
            }

            /*
             * Also update the parent map for all the downlinks that got moved
             * to a different page. (actually this also loops through the
             * downlinks that stayed on the original page, but it does no
             * harm).
             */
            if level > 1 {
                gistMemorizeAllDownlinks(buildstate, (*splitinfo).buf);
            }

            /*
             * Since there's no concurrent access, we can release the lower
             * level buffers immediately. This includes the original page.
             */
            UnlockReleaseBuffer((*splitinfo).buf);
            *downlinks.add(i as usize) = (*splitinfo).downlink;
            i += 1;
        });

        /* Insert them into parent. */
        gistbufferinginserttuples(
            buildstate,
            parentBuffer,
            level + 1,
            downlinks,
            ndownlinks,
            downlinkoffnum,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        );

        list_free_deep(splitinfo); /* we don't need this anymore */
    } else {
        UnlockReleaseBuffer(buffer);
    }

    placed_to_blk
}

/*
 * Find the downlink pointing to a child page.
 *
 * 'childblkno' indicates the child page to find the parent for. 'level' is
 * the level of the child. On entry, *parentblkno and *downlinkoffnum can
 * point to a location where the downlink used to be - we will check that
 * location first, and save some cycles if it hasn't moved. The function
 * returns a buffer containing the downlink, exclusively-locked, and
 * *parentblkno and *downlinkoffnum are set to the real location of the
 * downlink.
 *
 * If the child page is a leaf (level == 0), the caller must supply a correct
 * parentblkno. Otherwise we use the parent map hash table to find the parent
 * block.
 *
 * This function serves the same purpose as gistFindCorrectParent() during
 * normal index inserts, but this is simpler because we don't need to deal
 * with concurrent inserts.
 */
unsafe fn gistBufferingFindCorrectParent(
    buildstate: *mut GISTBuildState,
    childblkno: BlockNumber,
    level: c_int,
    parentblkno: *mut BlockNumber,
    downlinkoffnum: *mut OffsetNumber,
) -> Buffer {
    let parent: BlockNumber;
    let buffer: Buffer;
    let page: Page;
    let maxoff: OffsetNumber;
    let mut off: OffsetNumber;

    if level > 0 {
        parent = gistGetParent(buildstate, childblkno);
    } else {
        /*
         * For a leaf page, the caller must supply a correct parent block
         * number.
         */
        if *parentblkno == InvalidBlockNumber {
            elog!(ERROR, "no parent buffer provided of child {}", childblkno);
        }
        parent = *parentblkno;
    }

    buffer = ReadBuffer((*buildstate).indexrel, parent);
    page = BufferGetPage(buffer);
    LockBuffer(buffer, GIST_EXCLUSIVE);
    gistcheckpage((*buildstate).indexrel, buffer);
    maxoff = PageGetMaxOffsetNumber(page);

    /* Check if it was not moved */
    if parent == *parentblkno
        && *parentblkno != InvalidBlockNumber
        && *downlinkoffnum != InvalidOffsetNumber
        && *downlinkoffnum <= maxoff
    {
        let iid: *mut ItemIdData = PageGetItemId(page, *downlinkoffnum);
        let idxtuple: IndexTuple = PageGetItem(page, iid) as IndexTuple;

        if ItemPointerGetBlockNumber(&(*idxtuple).t_tid) == childblkno {
            /* Still there */
            return buffer;
        }
    }

    /*
     * Downlink was not at the offset where it used to be. Scan the page to
     * find it. During normal gist insertions, it might've moved to another
     * page, to the right, but during a buffering build, we keep track of the
     * parent of each page in the lookup table so we should always know what
     * page it's on.
     */
    off = FirstOffsetNumber;
    while off <= maxoff {
        let iid: *mut ItemIdData = PageGetItemId(page, off);
        let idxtuple: IndexTuple = PageGetItem(page, iid) as IndexTuple;

        if ItemPointerGetBlockNumber(&(*idxtuple).t_tid) == childblkno {
            /* yes!!, found it */
            *downlinkoffnum = off;
            return buffer;
        }
        off = OffsetNumberNext(off);
    }

    elog!(ERROR, "failed to re-find parent for block {}", childblkno);
    #[allow(unreachable_code)]
    {
        InvalidBuffer /* keep compiler quiet */
    }
}

/*
 * Process buffers emptying stack. Emptying of one buffer can cause emptying
 * of other buffers. This function iterates until this cascading emptying
 * process finished, e.g. until buffers emptying stack is empty.
 */
unsafe fn gistProcessEmptyingQueue(buildstate: *mut GISTBuildState) {
    let gfbb: *mut GISTBuildBuffers = (*buildstate).gfbb;

    /* Iterate while we have elements in buffers emptying stack. */
    while (*gfbb).bufferEmptyingQueue != NIL {
        let emptyingNodeBuffer: *mut GISTNodeBuffer;

        /* Get node buffer from emptying stack. */
        emptyingNodeBuffer = linitial((*gfbb).bufferEmptyingQueue) as *mut GISTNodeBuffer;
        (*gfbb).bufferEmptyingQueue = list_delete_first((*gfbb).bufferEmptyingQueue);
        (*emptyingNodeBuffer).queuedForEmptying = false;

        /*
         * We are going to load last pages of buffers where emptying will be
         * to. So let's unload any previously loaded buffers.
         */
        gistUnloadNodeBuffers(gfbb);

        /*
         * Pop tuples from the buffer and run them down to the buffers at
         * lower level, or leaf pages. We continue until one of the lower
         * level buffers fills up, or this buffer runs empty.
         *
         * In Arge et al's paper, the buffer emptying is stopped after
         * processing 1/2 node buffer worth of tuples, to avoid overfilling
         * any of the lower level buffers. However, it's more efficient to
         * keep going until one of the lower level buffers actually fills up,
         * so that's what we do. This doesn't need to be exact, if a buffer
         * overfills by a few tuples, there's no harm done.
         */
        loop {
            let mut itup: IndexTuple = null_mut();

            /* Get next index tuple from the buffer */
            if !gistPopItupFromNodeBuffer(gfbb, emptyingNodeBuffer, &mut itup) {
                break;
            }

            /*
             * Run it down to the underlying node buffer or leaf page.
             *
             * Note: it's possible that the buffer we're emptying splits as a
             * result of this call. If that happens, our emptyingNodeBuffer
             * points to the left half of the split. After split, it's very
             * likely that the new left buffer is no longer over the half-full
             * threshold, but we might as well keep flushing tuples from it
             * until we fill a lower-level buffer.
             */
            if gistProcessItup(
                buildstate,
                itup,
                (*emptyingNodeBuffer).nodeBlocknum,
                (*emptyingNodeBuffer).level,
            ) {
                /*
                 * A lower level buffer filled up. Stop emptying this buffer,
                 * to avoid overflowing the lower level buffer.
                 */
                break;
            }

            /* Free all the memory allocated during index tuple processing */
            MemoryContextReset((*(*buildstate).giststate).tempCxt);
        }
    }
}

/*
 * Empty all node buffers, from top to bottom. This is done at the end of
 * index build to flush all remaining tuples to the index.
 *
 * Note: This destroys the buffersOnLevels lists, so the buffers should not
 * be inserted to after this call.
 */
unsafe fn gistEmptyAllBuffers(buildstate: *mut GISTBuildState) {
    let gfbb: *mut GISTBuildBuffers = (*buildstate).gfbb;
    let oldCtx: MemoryContext;

    oldCtx = MemoryContextSwitchTo((*(*buildstate).giststate).tempCxt);

    /*
     * Iterate through the levels from top to bottom.
     */
    let mut i: c_int = (*gfbb).buffersOnLevelsLen - 1;
    while i >= 0 {
        /*
         * Empty all buffers on this level. Note that new buffers can pop up
         * in the list during the processing, as a result of page splits, so a
         * simple walk through the list won't work. We remove buffers from the
         * list when we see them empty; a buffer can't become non-empty once
         * it's been fully emptied.
         */
        while *(*gfbb).buffersOnLevels.add(i as usize) != NIL {
            let nodeBuffer: *mut GISTNodeBuffer;

            nodeBuffer =
                linitial(*(*gfbb).buffersOnLevels.add(i as usize)) as *mut GISTNodeBuffer;

            if (*nodeBuffer).blocksCount != 0 {
                /*
                 * Add this buffer to the emptying queue, and proceed to empty
                 * the queue.
                 */
                if !(*nodeBuffer).queuedForEmptying {
                    MemoryContextSwitchTo((*gfbb).context);
                    (*nodeBuffer).queuedForEmptying = true;
                    (*gfbb).bufferEmptyingQueue =
                        lcons(nodeBuffer as *mut c_void, (*gfbb).bufferEmptyingQueue);
                    MemoryContextSwitchTo((*(*buildstate).giststate).tempCxt);
                }
                gistProcessEmptyingQueue(buildstate);
            } else {
                *(*gfbb).buffersOnLevels.add(i as usize) =
                    list_delete_first(*(*gfbb).buffersOnLevels.add(i as usize));
            }
        }
        elog!(DEBUG2, "emptied all buffers at level {}", i);
        i -= 1;
    }
    MemoryContextSwitchTo(oldCtx);
}

/*
 * Get the depth of the GiST index.
 */
unsafe fn gistGetMaxLevel(index: Relation) -> c_int {
    let mut maxLevel: c_int;
    let mut blkno: BlockNumber;

    /*
     * Traverse down the tree, starting from the root, until we hit the leaf
     * level.
     */
    maxLevel = 0;
    blkno = GIST_ROOT_BLKNO;
    loop {
        let buffer: Buffer;
        let page: Page;
        let itup: IndexTuple;

        buffer = ReadBuffer(index, blkno);

        /*
         * There's no concurrent access during index build, so locking is just
         * pro forma.
         */
        LockBuffer(buffer, GIST_SHARE);
        page = BufferGetPage(buffer);

        if GistPageIsLeaf(page) {
            /* We hit the bottom, so we're done. */
            UnlockReleaseBuffer(buffer);
            break;
        }

        /*
         * Pick the first downlink on the page, and follow it. It doesn't
         * matter which downlink we choose, the tree has the same depth
         * everywhere, so we just pick the first one.
         */
        itup = PageGetItem(page, PageGetItemId(page, FirstOffsetNumber)) as IndexTuple;
        blkno = ItemPointerGetBlockNumber(&(*itup).t_tid);
        UnlockReleaseBuffer(buffer);

        /*
         * We're going down on the tree. It means that there is yet one more
         * level in the tree.
         */
        maxLevel += 1;
    }
    maxLevel
}

/*
 * Routines for managing the parent map.
 *
 * Whenever a page is split, we need to insert the downlinks into the parent.
 * We need to somehow find the parent page to do that. In normal insertions,
 * we keep a stack of nodes visited when we descend the tree. However, in
 * buffering build, we can start descending the tree from any internal node,
 * when we empty a buffer by cascading tuples to its children. So we don't
 * have a full stack up to the root available at that time.
 *
 * So instead, we maintain a hash table to track the parent of every internal
 * page. We don't need to track the parents of leaf nodes, however. Whenever
 * we insert to a leaf, we've just descended down from its parent, so we know
 * its immediate parent already. This helps a lot to limit the memory used
 * by this hash table.
 *
 * Whenever an internal node is split, the parent map needs to be updated.
 * the parent of the new child page needs to be recorded, and also the
 * entries for all page whose downlinks are moved to a new page at the split
 * needs to be updated.
 *
 * We also update the parent map whenever we descend the tree. That might seem
 * unnecessary, because we maintain the map whenever a downlink is moved or
 * created, but it is needed because we switch to buffering mode after
 * creating a tree with regular index inserts. Any pages created before
 * switching to buffering mode will not be present in the parent map initially,
 * but will be added there the first time we visit them.
 */

#[repr(C)]
pub struct ParentMapEntry {
    pub childblkno: BlockNumber, /* hash key */
    pub parentblkno: BlockNumber,
}

unsafe fn gistInitParentMap(buildstate: *mut GISTBuildState) {
    let mut hashCtl: HASHCTL = core::mem::zeroed();

    hashCtl.keysize = core::mem::size_of::<BlockNumber>() as Size;
    hashCtl.entrysize = core::mem::size_of::<ParentMapEntry>() as Size;
    hashCtl.hcxt = CurrentMemoryContext;
    (*buildstate).parentMap = hash_create(
        c"gistbuild parent map".as_ptr(),
        1024,
        &mut hashCtl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );
}

unsafe fn gistMemorizeParent(
    buildstate: *mut GISTBuildState,
    child: BlockNumber,
    parent: BlockNumber,
) {
    let entry: *mut ParentMapEntry;
    let mut found: bool = false;

    entry = hash_search(
        (*buildstate).parentMap,
        &child as *const BlockNumber as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut ParentMapEntry;
    (*entry).parentblkno = parent;
}

/*
 * Scan all downlinks on a page, and memorize their parent.
 */
unsafe fn gistMemorizeAllDownlinks(buildstate: *mut GISTBuildState, parentbuf: Buffer) {
    let maxoff: OffsetNumber;
    let mut off: OffsetNumber;
    let parentblkno: BlockNumber = BufferGetBlockNumber(parentbuf);
    let page: Page = BufferGetPage(parentbuf);

    Assert!(!GistPageIsLeaf(page));

    maxoff = PageGetMaxOffsetNumber(page);
    off = FirstOffsetNumber;
    while off <= maxoff {
        let iid: *mut ItemIdData = PageGetItemId(page, off);
        let idxtuple: IndexTuple = PageGetItem(page, iid) as IndexTuple;
        let childblkno: BlockNumber = ItemPointerGetBlockNumber(&(*idxtuple).t_tid);

        gistMemorizeParent(buildstate, childblkno, parentblkno);
        off += 1;
    }
}

unsafe fn gistGetParent(buildstate: *mut GISTBuildState, child: BlockNumber) -> BlockNumber {
    let entry: *mut ParentMapEntry;
    let mut found: bool = false;

    /* Find node buffer in hash table */
    entry = hash_search(
        (*buildstate).parentMap,
        &child as *const BlockNumber as *const c_void,
        HASH_FIND,
        &mut found,
    ) as *mut ParentMapEntry;
    if !found {
        elog!(
            ERROR,
            "could not find parent of block {} in lookup table",
            child
        );
    }

    (*entry).parentblkno
}
