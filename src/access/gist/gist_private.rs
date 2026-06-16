//! access/gist_private.h - private declarations for GiST internals.
//!
//! Faithful 1:1 translation of PostgreSQL 18.3
//! src/include/access/gist_private.h. Covers GISTSTATE, the search-queue item
//! types, scan opaque state, split/insert working structures, build-buffer
//! structures, reloptions, and all the function prototypes (gist.c, gistxlog.c,
//! gistget.c, gistvalidate.c, gistutil.c, gistvacuum.c, gistsplit.c,
//! gistbuild.c, gistbuildbuffers.c).

use crate::prelude::*;

use crate::access::common::indextuple::{IndexTuple, IndexTupleData, INDEX_MAX_KEYS};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::HeapTuple;
use crate::access::index::amapi::{
    IndexAMProperty, IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInfo,
    IndexScanDesc, IndexUniqueCheck, IndexVacuumInfo, TIDBitmap,
};
use crate::access::transam::FullTransactionId;
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::lib::pairingheap::{pairingheap, pairingheap_node};
use crate::nodes::pg_list::List;
use crate::nodes::plannodes::ScanDirection;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::Page;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::utils::fmgr::FmgrInfo;
use crate::utils::hash::dynahash::HTAB;
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::rel::Relation;

// ===========================================================================
// Locally-stubbed types: these come from headers not yet ported in their own
// canonical module. Minimal stubs matching the C definitions; dedup later.
// ===========================================================================

/// access/gist.h: `typedef XLogRecPtr GistNSN;`
/// TODO: dedup once access/gist.h is ported (also defined in
/// access/rmgrdesc/gistdesc.rs).
pub type GistNSN = XLogRecPtr;

/// utils/sortsupport.h / access/genam.h: distance value returned by an index
/// scan's ordering operators.
/// TODO: dedup once access/genam.h (IndexOrderByDistance) is ported (also
/// defined in access/spgist/spgist_private.rs).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexOrderByDistance {
    pub value: f64,
    pub isnull: bool,
}

/// access/gist.h: per-attribute entry passed to opclass support functions.
/// TODO: dedup once access/gist.h is ported (also defined in
/// utils/adt/tsgistidx.rs and friends).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GISTENTRY {
    pub key: Datum,
    pub rel: Relation,
    pub page: Page,
    pub offset: OffsetNumber,
    pub leafkey: bool,
}

/// access/gist.h: result vector of the PickSplit opclass method.
/// TODO: dedup once access/gist.h is ported.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GIST_SPLITVEC {
    pub spl_left: *mut OffsetNumber,
    pub spl_nleft: c_int,
    pub spl_ldatum: Datum,
    pub spl_ldatum_exists: bool,

    pub spl_right: *mut OffsetNumber,
    pub spl_nright: c_int,
    pub spl_rdatum: Datum,
    pub spl_rdatum_exists: bool,
}

/// storage/buffile.h: opaque BufFile.
/// TODO: dedup once storage/buffile.h is ported.
pub type BufFile = c_void;

// ===========================================================================
// Constants
// ===========================================================================

/*
 * Maximum number of "halves" a page can be split into in one operation.
 * (See the long comment in the C header for rationale.)
 */
pub const GIST_MAX_SPLIT_PAGES: c_int = 75;

/* Buffer lock modes.
 *
 * In C these alias BUFFER_LOCK_SHARE / BUFFER_LOCK_EXCLUSIVE /
 * BUFFER_LOCK_UNLOCK from storage/bufmgr.h, which are 1 / 2 / 0 respectively.
 * storage/bufmgr.h is not yet ported here, so the underlying values are
 * inlined; dedup once bufmgr.h provides BUFFER_LOCK_*.
 * TODO: dedup BUFFER_LOCK_* from storage/bufmgr.h.
 */
pub const GIST_SHARE: c_int = 1; /* BUFFER_LOCK_SHARE */
pub const GIST_EXCLUSIVE: c_int = 2; /* BUFFER_LOCK_EXCLUSIVE */
pub const GIST_UNLOCK: c_int = 0; /* BUFFER_LOCK_UNLOCK */

/* root page of a gist index */
pub const GIST_ROOT_BLKNO: BlockNumber = 0;

/*
 * "invalid tuple" sentinel offset numbers (on-disk pg_upgrade compatibility).
 */
pub const TUPLE_IS_VALID: OffsetNumber = 0xffff;
pub const TUPLE_IS_INVALID: OffsetNumber = 0xfffe;

pub const GIST_MIN_FILLFACTOR: c_int = 10;
pub const GIST_DEFAULT_FILLFACTOR: c_int = 90;

// ===========================================================================
// GISTNodeBufferPage and its inline macros
// ===========================================================================

/// Node buffer page header. In C `tupledata` is a FLEXIBLE_ARRAY_MEMBER; here
/// it is a zero-length array and the payload follows in memory.
#[repr(C)]
pub struct GISTNodeBufferPage {
    pub prev: BlockNumber,
    pub freespace: uint32,
    pub tupledata: [c_char; 0], // FLEXIBLE_ARRAY_MEMBER
}

/*
 * #define BUFFER_PAGE_DATA_OFFSET MAXALIGN(offsetof(GISTNodeBufferPage, tupledata))
 *
 * Offset of the variable data within a GISTNodeBufferPage, MAXALIGN'd.
 * MAXALIGN and offsetof live in c.h / pg_config_manual.h; the offset of the
 * trailing flexible array is computed via the struct's fixed size.
 * TODO: use the canonical MAXALIGN once available.
 */
#[inline]
pub fn BUFFER_PAGE_DATA_OFFSET() -> Size {
    // offsetof(GISTNodeBufferPage, tupledata) == size of the fixed header.
    MAXALIGN(core::mem::size_of::<GISTNodeBufferPage>())
}

/// Returns free space in node buffer page.
#[inline]
pub unsafe fn PAGE_FREE_SPACE(nbp: *const GISTNodeBufferPage) -> uint32 {
    (*nbp).freespace
}

/// Checks if node buffer page is empty.
#[inline]
pub unsafe fn PAGE_IS_EMPTY(nbp: *const GISTNodeBufferPage) -> bool {
    (*nbp).freespace == (BLCKSZ as Size - BUFFER_PAGE_DATA_OFFSET()) as uint32
}

/// Checks if node buffers page don't contain sufficient space for index tuple.
#[inline]
pub unsafe fn PAGE_NO_SPACE(nbp: *const GISTNodeBufferPage, itup: IndexTuple) -> bool {
    (PAGE_FREE_SPACE(nbp) as Size) < MAXALIGN(IndexTupleSize(itup))
}

// Local MAXALIGN helper mirroring c.h's MAXALIGN; dedup once c.h MAXALIGN is
// universally exported. ALIGNOF_MAXALIGN_INT is 8 on virtually all targets.
// TODO: dedup MAXALIGN from c.h.
#[inline]
fn MAXALIGN(len: Size) -> Size {
    const MAXIMUM_ALIGNOF: Size = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

// Local IndexTupleSize helper mirroring itup.h's IndexTupleSize macro; dedup
// once access/itup.h exports it. TODO: dedup IndexTupleSize.
#[inline]
unsafe fn IndexTupleSize(itup: IndexTuple) -> Size {
    // (Size)((itup)->t_info & INDEX_SIZE_MASK)
    const INDEX_SIZE_MASK: u16 = 0x1FFF;
    ((*itup).t_info & INDEX_SIZE_MASK) as Size
}

// ===========================================================================
// GISTSTATE
// ===========================================================================

/// GISTSTATE: information needed for any GiST index operation.
#[repr(C)]
pub struct GISTSTATE {
    pub scanCxt: MemoryContext,
    pub tempCxt: MemoryContext,

    pub leafTupdesc: TupleDesc,
    pub nonLeafTupdesc: TupleDesc,
    pub fetchTupdesc: TupleDesc,

    pub consistentFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub unionFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub compressFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub decompressFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub penaltyFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub picksplitFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub equalFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub distanceFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub fetchFn: [FmgrInfo; INDEX_MAX_KEYS],

    /* Collations to pass to the support functions */
    pub supportCollation: [Oid; INDEX_MAX_KEYS],
}

// ===========================================================================
// Search queue item types
// ===========================================================================

/// Individual heap tuple to be visited.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GISTSearchHeapItem {
    pub heapPtr: ItemPointerData,
    pub recheck: bool,
    pub recheckDistances: bool,
    pub recontup: HeapTuple,
    pub offnum: OffsetNumber,
}

/// Union member of GISTSearchItem; members derive Clone, Copy.
#[repr(C)]
#[derive(Clone, Copy)]
pub union GISTSearchItemData {
    /// parent page's LSN, if index page
    pub parentlsn: GistNSN,
    /// heap info, if heap tuple
    pub heap: GISTSearchHeapItem,
}

/// Unvisited item, either index page or heap tuple. In C `distances` is a
/// FLEXIBLE_ARRAY_MEMBER (numberOfOrderBys entries); modelled as a zero-length
/// trailing array.
#[repr(C)]
pub struct GISTSearchItem {
    pub phNode: pairingheap_node,
    pub blkno: BlockNumber, /* index page number, or InvalidBlockNumber */
    pub data: GISTSearchItemData,
    /* numberOfOrderBys entries */
    pub distances: [IndexOrderByDistance; 0], // FLEXIBLE_ARRAY_MEMBER
}

/// #define GISTSearchItemIsHeap(item) ((item).blkno == InvalidBlockNumber)
#[inline]
pub fn GISTSearchItemIsHeap(item: &GISTSearchItem) -> bool {
    item.blkno == InvalidBlockNumber
}

/// #define SizeOfGISTSearchItem(n_distances) ...
#[inline]
pub fn SizeOfGISTSearchItem(n_distances: c_int) -> Size {
    // offsetof(GISTSearchItem, distances) is the fixed-size prefix.
    let off = core::mem::size_of::<GISTSearchItem>();
    off + core::mem::size_of::<IndexOrderByDistance>() * (n_distances as Size)
}

// Local InvalidBlockNumber mirroring storage/block.h; dedup once it is exported.
// TODO: dedup InvalidBlockNumber from storage/block.h.
const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;

// BLCKSZ from pg_config.h (default 8192). TODO: dedup BLCKSZ from pg_config.h.
const BLCKSZ: c_int = 8192;

// ===========================================================================
// GISTScanOpaqueData
// ===========================================================================

/// Number of pageData entries: BLCKSZ / sizeof(IndexTupleData).
const GIST_SCAN_PAGEDATA_LEN: usize =
    (BLCKSZ as usize) / core::mem::size_of::<IndexTupleData>();

/// GISTScanOpaqueData: private state for a scan of a GiST index.
#[repr(C)]
pub struct GISTScanOpaqueData {
    pub giststate: *mut GISTSTATE,
    pub orderByTypes: *mut Oid,

    pub queue: *mut pairingheap,
    pub queueCxt: MemoryContext,
    pub qual_ok: bool,
    pub firstCall: bool,

    /* pre-allocated workspace arrays */
    pub distances: *mut IndexOrderByDistance,

    /* info about killed items if any (killedItems is NULL if never used) */
    pub killedItems: *mut OffsetNumber,
    pub numKilled: c_int,
    pub curBlkno: BlockNumber,
    pub curPageLSN: GistNSN,

    /* In a non-ordered search, returnable heap items are stored here: */
    pub pageData: [GISTSearchHeapItem; GIST_SCAN_PAGEDATA_LEN],
    pub nPageData: OffsetNumber,
    pub curPageData: OffsetNumber,
    pub pageDataCxt: MemoryContext,
}

pub type GISTScanOpaque = *mut GISTScanOpaqueData;

// ===========================================================================
// xlog page descriptor / split layout
// ===========================================================================

/// despite the name, gistxlogPage is not part of any xlog record.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct gistxlogPage {
    pub blkno: BlockNumber,
    pub num: c_int, /* number of index tuples following */
}

/// SplitPageLayout - gistSplit function result.
#[repr(C)]
pub struct SplitPageLayout {
    pub block: gistxlogPage,
    pub list: *mut IndexTupleData,
    pub lenlist: c_int,
    pub itup: IndexTuple, /* union key for page */
    pub page: Page,       /* to operate */
    pub buffer: Buffer,   /* to write after all proceed */

    pub next: *mut SplitPageLayout,
}

// ===========================================================================
// Insert stack / split vector / insert state
// ===========================================================================

/// GISTInsertStack used for locking buffers and transfer arguments during
/// insertion.
#[repr(C)]
pub struct GISTInsertStack {
    /* current page */
    pub blkno: BlockNumber,
    pub buffer: Buffer,
    pub page: Page,

    pub lsn: GistNSN,

    pub retry_from_parent: bool,

    /* offset of the downlink in the parent page, that points to this page */
    pub downlinkoffnum: OffsetNumber,

    /* pointer to parent */
    pub parent: *mut GISTInsertStack,
}

/// Working state and results for multi-column split logic in gistsplit.c.
#[repr(C)]
pub struct GistSplitVector {
    pub splitVector: GIST_SPLITVEC, /* passed to/from user PickSplit method */

    pub spl_lattr: [Datum; INDEX_MAX_KEYS],
    pub spl_lisnull: [bool; INDEX_MAX_KEYS],

    pub spl_rattr: [Datum; INDEX_MAX_KEYS],
    pub spl_risnull: [bool; INDEX_MAX_KEYS],

    pub spl_dontcare: *mut bool,
}

/// (anonymous struct) GISTInsertState.
#[repr(C)]
pub struct GISTInsertState {
    pub r: Relation,
    pub heapRel: Relation,
    pub freespace: Size, /* free space to be left */
    pub is_build: bool,

    pub stack: *mut GISTInsertStack,
}

// ===========================================================================
// "invalid tuple" inline helpers
// ===========================================================================

/// #define GistTupleIsInvalid(itup) ...
#[inline]
pub unsafe fn GistTupleIsInvalid(itup: IndexTuple) -> bool {
    ItemPointerGetOffsetNumber(&(*itup).t_tid) == TUPLE_IS_INVALID
}

/// #define GistTupleSetValid(itup) ...
#[inline]
pub unsafe fn GistTupleSetValid(itup: IndexTuple) {
    ItemPointerSetOffsetNumber(&mut (*itup).t_tid, TUPLE_IS_VALID);
}

// Local ItemPointer offset accessors mirroring storage/itemptr.h; dedup once
// they are exported. TODO: dedup ItemPointerGet/SetOffsetNumber.
#[inline]
unsafe fn ItemPointerGetOffsetNumber(pointer: *const ItemPointerData) -> OffsetNumber {
    (*pointer).ip_posid
}

#[inline]
unsafe fn ItemPointerSetOffsetNumber(pointer: *mut ItemPointerData, offsetNumber: OffsetNumber) {
    (*pointer).ip_posid = offsetNumber;
}

// ===========================================================================
// Build buffers (buffering-mode index build)
// ===========================================================================

/// (anonymous struct) GISTNodeBuffer: a buffer attached to an internal node,
/// used when building an index in buffering mode.
#[repr(C)]
pub struct GISTNodeBuffer {
    pub nodeBlocknum: BlockNumber, /* index block # this buffer is for */
    pub blocksCount: int32,        /* current # of blocks occupied by buffer */

    pub pageBlocknum: BlockNumber, /* temporary file block # */
    pub pageBuffer: *mut GISTNodeBufferPage, /* in-memory buffer page */

    /* is this buffer queued for emptying? */
    pub queuedForEmptying: bool,

    /* is this a temporary copy, not in the hash table? */
    pub isTemp: bool,

    pub level: c_int, /* 0 == leaf */
}

/// #define LEVEL_HAS_BUFFERS(nlevel, gfbb) ...
#[inline]
pub unsafe fn LEVEL_HAS_BUFFERS(nlevel: c_int, gfbb: *const GISTBuildBuffers) -> bool {
    nlevel != 0 && nlevel % (*gfbb).levelStep == 0 && nlevel != (*gfbb).rootlevel
}

/// #define BUFFER_HALF_FILLED(nodeBuffer, gfbb) ...
#[inline]
pub unsafe fn BUFFER_HALF_FILLED(
    nodeBuffer: *const GISTNodeBuffer,
    gfbb: *const GISTBuildBuffers,
) -> bool {
    (*nodeBuffer).blocksCount > (*gfbb).pagesPerBuffer / 2
}

/// #define BUFFER_OVERFLOWED(nodeBuffer, gfbb) ...
#[inline]
pub unsafe fn BUFFER_OVERFLOWED(
    nodeBuffer: *const GISTNodeBuffer,
    gfbb: *const GISTBuildBuffers,
) -> bool {
    (*nodeBuffer).blocksCount > (*gfbb).pagesPerBuffer
}

/// Data structure with general information about build buffers.
#[repr(C)]
pub struct GISTBuildBuffers {
    /* Persistent memory context for the buffers and metadata. */
    pub context: MemoryContext,

    pub pfile: *mut BufFile, /* Temporary file to store buffers in */
    pub nFileBlocks: c_long, /* Current size of the temporary file */

    /* resizable array of free blocks. */
    pub freeBlocks: *mut c_long,
    pub nFreeBlocks: c_int,   /* # of currently free blocks in the array */
    pub freeBlocksLen: c_int, /* current allocated length of the array */

    /* Hash for buffers by block number */
    pub nodeBuffersTab: *mut HTAB,

    /* List of buffers scheduled for emptying */
    pub bufferEmptyingQueue: *mut List,

    pub levelStep: c_int,
    pub pagesPerBuffer: c_int,

    /* Array of lists of buffers on each level, for final emptying */
    pub buffersOnLevels: *mut *mut List,
    pub buffersOnLevelsLen: c_int,

    /*
     * Dynamically-sized array of buffers that currently have their last page
     * loaded in main memory.
     */
    pub loadedBuffers: *mut *mut GISTNodeBuffer,
    pub loadedBuffersCount: c_int, /* # of entries in loadedBuffers */
    pub loadedBuffersLen: c_int,   /* allocated size of loadedBuffers */

    /* Level of the current root node (= height of the index tree - 1) */
    pub rootlevel: c_int,
}

// ===========================================================================
// Reloptions
// ===========================================================================

/* GiSTOptions->buffering_mode values */
pub type GistOptBufferingMode = c_int;
pub const GIST_OPTION_BUFFERING_AUTO: GistOptBufferingMode = 0;
pub const GIST_OPTION_BUFFERING_ON: GistOptBufferingMode = 1;
pub const GIST_OPTION_BUFFERING_OFF: GistOptBufferingMode = 2;

/// Storage type for GiST's reloptions.
#[repr(C)]
pub struct GiSTOptions {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub fillfactor: c_int, /* page fill factor in percent (0..100) */
    pub buffering_mode: GistOptBufferingMode, /* buffering build mode */
}

/// #define GiSTPageSize ...
///
/// ( BLCKSZ - SizeOfPageHeaderData - MAXALIGN(sizeof(GISTPageOpaqueData)) )
/// SizeOfPageHeaderData comes from storage/bufpage.h, GISTPageOpaqueData from
/// access/gist.h - both not yet ported here. The sizes are inlined to their
/// concrete C values: SizeOfPageHeaderData == 24, sizeof(GISTPageOpaqueData)
/// == 16 (rightlink, nsn(8), flags, gist_page_id).
/// TODO: dedup SizeOfPageHeaderData and GISTPageOpaqueData.
#[inline]
pub fn GiSTPageSize() -> Size {
    const SizeOfPageHeaderData: Size = 24;
    const SizeOfGISTPageOpaqueData: Size = 16;
    (BLCKSZ as Size) - SizeOfPageHeaderData - MAXALIGN(SizeOfGISTPageOpaqueData)
}

// ===========================================================================
// gistplacetopage split-info result
// ===========================================================================

/// (anonymous struct) GISTPageSplitInfo: A List of these is returned from
/// gistplacetopage() in *splitinfo.
#[repr(C)]
pub struct GISTPageSplitInfo {
    pub buf: Buffer,          /* the split page "half" */
    pub downlink: IndexTuple, /* downlink for this half. */
}

// ===========================================================================
// Function prototypes
// ===========================================================================

/* gist.c */
pub unsafe fn gistbuildempty(index: Relation) {
    unimplemented!()
}
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
    unimplemented!()
}
pub unsafe fn createTempGistContext() -> MemoryContext {
    unimplemented!()
}
pub unsafe fn initGISTstate(index: Relation) -> *mut GISTSTATE {
    unimplemented!()
}
pub unsafe fn freeGISTstate(giststate: *mut GISTSTATE) {
    unimplemented!()
}
pub unsafe fn gistdoinsert(
    r: Relation,
    itup: IndexTuple,
    freespace: Size,
    giststate: *mut GISTSTATE,
    heapRel: Relation,
    is_build: bool,
) {
    unimplemented!()
}

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
    unimplemented!()
}

pub unsafe fn gistSplit(
    r: Relation,
    page: Page,
    itup: *mut IndexTuple,
    len: c_int,
    giststate: *mut GISTSTATE,
) -> *mut SplitPageLayout {
    unimplemented!()
}

/* gistxlog.c */
pub unsafe fn gistXLogPageDelete(
    buffer: Buffer,
    xid: FullTransactionId,
    parentBuffer: Buffer,
    downlinkOffset: OffsetNumber,
) -> XLogRecPtr {
    unimplemented!()
}

pub unsafe fn gistXLogPageReuse(
    rel: Relation,
    heaprel: Relation,
    blkno: BlockNumber,
    deleteXid: FullTransactionId,
) {
    unimplemented!()
}

pub unsafe fn gistXLogUpdate(
    buffer: Buffer,
    todelete: *mut OffsetNumber,
    ntodelete: c_int,
    itup: *mut IndexTuple,
    ituplen: c_int,
    leftchildbuf: Buffer,
) -> XLogRecPtr {
    unimplemented!()
}

pub unsafe fn gistXLogDelete(
    buffer: Buffer,
    todelete: *mut OffsetNumber,
    ntodelete: c_int,
    snapshotConflictHorizon: TransactionId,
    heaprel: Relation,
) -> XLogRecPtr {
    unimplemented!()
}

pub unsafe fn gistXLogSplit(
    page_is_leaf: bool,
    dist: *mut SplitPageLayout,
    origrlink: BlockNumber,
    orignsn: GistNSN,
    leftchildbuf: Buffer,
    markfollowright: bool,
) -> XLogRecPtr {
    unimplemented!()
}

pub unsafe fn gistXLogAssignLSN() -> XLogRecPtr {
    unimplemented!()
}

/* gistget.c */
pub unsafe fn gistgettuple(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    unimplemented!()
}
pub unsafe fn gistgetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 {
    unimplemented!()
}
pub unsafe fn gistcanreturn(index: Relation, attno: c_int) -> bool {
    unimplemented!()
}

/* gistvalidate.c */
pub unsafe fn gistvalidate(opclassoid: Oid) -> bool {
    unimplemented!()
}
pub unsafe fn gistadjustmembers(
    opfamilyoid: Oid,
    opclassoid: Oid,
    operators: *mut List,
    functions: *mut List,
) {
    unimplemented!()
}

/* gistutil.c */
pub unsafe fn gistoptions(reloptions: Datum, validate: bool) -> *mut bytea {
    unimplemented!()
}
pub unsafe fn gistproperty(
    index_oid: Oid,
    attno: c_int,
    prop: IndexAMProperty,
    propname: *const c_char,
    res: *mut bool,
    isnull: *mut bool,
) -> bool {
    unimplemented!()
}
pub unsafe fn gistfitpage(itvec: *mut IndexTuple, len: c_int) -> bool {
    unimplemented!()
}
pub unsafe fn gistnospace(
    page: Page,
    itvec: *mut IndexTuple,
    len: c_int,
    todelete: OffsetNumber,
    freespace: Size,
) -> bool {
    unimplemented!()
}
pub unsafe fn gistcheckpage(rel: Relation, buf: Buffer) {
    unimplemented!()
}
pub unsafe fn gistNewBuffer(r: Relation, heaprel: Relation) -> Buffer {
    unimplemented!()
}
/* Can this page be recycled yet? */
pub unsafe fn gistPageRecyclable(page: Page) -> bool {
    if PageIsNew(page) {
        return true;
    }
    if GistPageIsDeleted(page) {
        /*
         * The page was deleted, but when? If it was just deleted, a scan
         * might have seen the downlink to it, and will read the page later.
         * As long as that can happen, we must keep the deleted page around as
         * a tombstone.
         *
         * For that check if the deletion XID could still be visible to
         * anyone. If not, then no scan that's still in progress could have
         * seen its downlink, and we can recycle it.
         */
        let deletexid_full: FullTransactionId = GistPageGetDeleteXid(page);

        return GlobalVisCheckRemovableFullXid(null_mut(), deletexid_full);
    }
    false
}

// TODO(pg-port): genuinely-unported deps for gistPageRecyclable.
unsafe fn PageIsNew(_page: Page) -> bool {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn GistPageIsDeleted(_page: Page) -> bool {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn GistPageGetDeleteXid(_page: Page) -> FullTransactionId {
    unimplemented!() // TODO: access/gist.h
}
unsafe fn GlobalVisCheckRemovableFullXid(
    _rel: Relation,
    _xid: FullTransactionId,
) -> bool {
    unimplemented!() // TODO: utils/snapmgr.h
}
pub unsafe fn gistfillbuffer(page: Page, itup: *mut IndexTuple, len: c_int, off: OffsetNumber) {
    unimplemented!()
}
pub unsafe fn gistextractpage(page: Page, len: *mut c_int /* out */) -> *mut IndexTuple {
    unimplemented!()
}
pub unsafe fn gistjoinvector(
    itvec: *mut IndexTuple,
    len: *mut c_int,
    additvec: *mut IndexTuple,
    addlen: c_int,
) -> *mut IndexTuple {
    unimplemented!()
}
pub unsafe fn gistfillitupvec(
    vec: *mut IndexTuple,
    veclen: c_int,
    memlen: *mut c_int,
) -> *mut IndexTupleData {
    unimplemented!()
}

pub unsafe fn gistunion(
    r: Relation,
    itvec: *mut IndexTuple,
    len: c_int,
    giststate: *mut GISTSTATE,
) -> IndexTuple {
    unimplemented!()
}
pub unsafe fn gistgetadjusted(
    r: Relation,
    oldtup: IndexTuple,
    addtup: IndexTuple,
    giststate: *mut GISTSTATE,
) -> IndexTuple {
    unimplemented!()
}
pub unsafe fn gistFormTuple(
    giststate: *mut GISTSTATE,
    r: Relation,
    attdata: *const Datum,
    isnull: *const bool,
    isleaf: bool,
) -> IndexTuple {
    unimplemented!()
}
pub unsafe fn gistCompressValues(
    giststate: *mut GISTSTATE,
    r: Relation,
    attdata: *const Datum,
    isnull: *const bool,
    isleaf: bool,
    compatt: *mut Datum,
) {
    unimplemented!()
}

pub unsafe fn gistchoose(
    r: Relation,
    p: Page,
    it: IndexTuple,
    giststate: *mut GISTSTATE,
) -> OffsetNumber {
    unimplemented!()
}

pub unsafe fn GISTInitBuffer(b: Buffer, f: uint32) {
    unimplemented!()
}
pub unsafe fn gistinitpage(page: Page, f: uint32) {
    unimplemented!()
}
pub unsafe fn gistdentryinit(
    giststate: *mut GISTSTATE,
    nkey: c_int,
    e: *mut GISTENTRY,
    k: Datum,
    r: Relation,
    pg: Page,
    o: OffsetNumber,
    l: bool,
    isNull: bool,
) {
    unimplemented!()
}

pub unsafe fn gistpenalty(
    giststate: *mut GISTSTATE,
    attno: c_int,
    orig: *mut GISTENTRY,
    isNullOrig: bool,
    add: *mut GISTENTRY,
    isNullAdd: bool,
) -> f32 {
    unimplemented!()
}
pub unsafe fn gistMakeUnionItVec(
    giststate: *mut GISTSTATE,
    itvec: *mut IndexTuple,
    len: c_int,
    attr: *mut Datum,
    isnull: *mut bool,
) {
    unimplemented!()
}
pub unsafe fn gistKeyIsEQ(giststate: *mut GISTSTATE, attno: c_int, a: Datum, b: Datum) -> bool {
    unimplemented!()
}
pub unsafe fn gistDeCompressAtt(
    giststate: *mut GISTSTATE,
    r: Relation,
    tuple: IndexTuple,
    p: Page,
    o: OffsetNumber,
    attdata: *mut GISTENTRY,
    isnull: *mut bool,
) {
    unimplemented!()
}
pub unsafe fn gistFetchTuple(
    giststate: *mut GISTSTATE,
    r: Relation,
    tuple: IndexTuple,
) -> HeapTuple {
    unimplemented!()
}
pub unsafe fn gistMakeUnionKey(
    giststate: *mut GISTSTATE,
    attno: c_int,
    entry1: *mut GISTENTRY,
    isnull1: bool,
    entry2: *mut GISTENTRY,
    isnull2: bool,
    dst: *mut Datum,
    dstisnull: *mut bool,
) {
    unimplemented!()
}

pub unsafe fn gistGetFakeLSN(rel: Relation) -> XLogRecPtr {
    unimplemented!()
}

/* gistvacuum.c */
pub unsafe fn gistbulkdelete(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}
pub unsafe fn gistvacuumcleanup(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}

/* gistsplit.c */
pub unsafe fn gistSplitByKey(
    r: Relation,
    page: Page,
    itup: *mut IndexTuple,
    len: c_int,
    giststate: *mut GISTSTATE,
    v: *mut GistSplitVector,
    attno: c_int,
) {
    unimplemented!()
}

/* gistbuild.c */
pub unsafe fn gistbuild(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult {
    unimplemented!()
}

/* gistbuildbuffers.c */
pub unsafe fn gistInitBuildBuffers(
    pagesPerBuffer: c_int,
    levelStep: c_int,
    maxLevel: c_int,
) -> *mut GISTBuildBuffers {
    unimplemented!()
}
pub unsafe fn gistGetNodeBuffer(
    gfbb: *mut GISTBuildBuffers,
    giststate: *mut GISTSTATE,
    nodeBlocknum: BlockNumber,
    level: c_int,
) -> *mut GISTNodeBuffer {
    unimplemented!()
}
pub unsafe fn gistPushItupToNodeBuffer(
    gfbb: *mut GISTBuildBuffers,
    nodeBuffer: *mut GISTNodeBuffer,
    itup: IndexTuple,
) {
    unimplemented!()
}
pub unsafe fn gistPopItupFromNodeBuffer(
    gfbb: *mut GISTBuildBuffers,
    nodeBuffer: *mut GISTNodeBuffer,
    itup: *mut IndexTuple,
) -> bool {
    unimplemented!()
}
pub unsafe fn gistFreeBuildBuffers(gfbb: *mut GISTBuildBuffers) {
    unimplemented!()
}
pub unsafe fn gistRelocateBuildBuffersOnSplit(
    gfbb: *mut GISTBuildBuffers,
    giststate: *mut GISTSTATE,
    r: Relation,
    level: c_int,
    buffer: Buffer,
    splitinfo: *mut List,
) {
    unimplemented!()
}
pub unsafe fn gistUnloadNodeBuffers(gfbb: *mut GISTBuildBuffers) {
    unimplemented!()
}
