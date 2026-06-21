//! access/ginblock.h - details of structures stored in GIN index blocks

use std::ffi::c_char;
use std::mem::{offset_of, size_of};

use crate::c::{
    int32, int64, uint16, uint32, Pointer, Size, FLEXIBLE_ARRAY_MEMBER, MAXALIGN, MAXALIGN_DOWN,
    SHORTALIGN,
};
use crate::pg_config::BLCKSZ;

use crate::access::common::indextuple::{IndexInfoFindDataOffset, INDEX_SIZE_MASK};
use crate::c::TransactionId;
use crate::storage::block::{
    BlockIdData, BlockIdGetBlockNumber, BlockIdSet, BlockNumber, InvalidBlockNumber,
};
use crate::storage::bufpage::{
    Page, PageGetContents, PageGetExactFreeSpace, PageGetSpecialPointer, PageHeader,
    SizeOfPageHeaderData,
};
use crate::storage::itemid::ItemIdData;
use crate::storage::itemptr::{
    ItemPointer, ItemPointerData, ItemPointerGetBlockNumberNoCheck,
    ItemPointerGetOffsetNumberNoCheck, ItemPointerSet, ItemPointerSetBlockNumber,
    ItemPointerSetOffsetNumber,
};
use crate::storage::off::{InvalidOffsetNumber, OffsetNumber};

/*
 * Page opaque data in an inverted index page.
 */
#[repr(C)]
pub struct GinPageOpaqueData {
    pub rightlink: BlockNumber, /* next page if any */
    pub maxoff: OffsetNumber,   /* number of PostingItems on GIN_DATA & ~GIN_LEAF page.
                                 * On GIN_LIST page, number of heap tuples. */
    pub flags: uint16, /* see bit definitions below */
}

pub type GinPageOpaque = *mut GinPageOpaqueData;

pub const GIN_DATA: uint16 = 1 << 0;
pub const GIN_LEAF: uint16 = 1 << 1;
pub const GIN_DELETED: uint16 = 1 << 2;
pub const GIN_META: uint16 = 1 << 3;
pub const GIN_LIST: uint16 = 1 << 4;
pub const GIN_LIST_FULLROW: uint16 = 1 << 5; /* makes sense only on GIN_LIST page */
pub const GIN_INCOMPLETE_SPLIT: uint16 = 1 << 6; /* page was split, but parent not updated */
pub const GIN_COMPRESSED: uint16 = 1 << 7;

/* Page numbers of fixed-location pages */
pub const GIN_METAPAGE_BLKNO: BlockNumber = 0;
pub const GIN_ROOT_BLKNO: BlockNumber = 1;

#[repr(C)]
pub struct GinMetaPageData {
    /*
     * Pointers to head and tail of pending list, which consists of GIN_LIST
     * pages.
     */
    pub head: BlockNumber,
    pub tail: BlockNumber,

    /*
     * Free space in bytes in the pending list's tail page.
     */
    pub tailFreeSize: uint32,

    /*
     * Number of pages and number of heap tuples that are in the pending list.
     */
    pub nPendingPages: BlockNumber,
    pub nPendingHeapTuples: int64,

    /*
     * Statistics for planner use (accurate as of last VACUUM)
     */
    pub nTotalPages: BlockNumber,
    pub nEntryPages: BlockNumber,
    pub nDataPages: BlockNumber,
    pub nEntries: int64,

    /*
     * GIN version number.
     */
    pub ginVersion: int32,
}

pub const GIN_CURRENT_VERSION: int32 = 2;

#[inline]
pub unsafe fn GinPageGetMeta(p: Page) -> *mut GinMetaPageData {
    PageGetContents(p) as *mut GinMetaPageData
}

/*
 * Macros for accessing a GIN index page's opaque data
 */
#[inline]
pub unsafe fn GinPageGetOpaque(page: Page) -> GinPageOpaque {
    PageGetSpecialPointer(page) as GinPageOpaque
}

#[inline]
pub unsafe fn GinPageIsLeaf(page: Page) -> bool {
    ((*GinPageGetOpaque(page)).flags & GIN_LEAF) != 0
}
#[inline]
pub unsafe fn GinPageSetLeaf(page: Page) {
    (*GinPageGetOpaque(page)).flags |= GIN_LEAF;
}
#[inline]
pub unsafe fn GinPageSetNonLeaf(page: Page) {
    (*GinPageGetOpaque(page)).flags &= !GIN_LEAF;
}
#[inline]
pub unsafe fn GinPageIsData(page: Page) -> bool {
    ((*GinPageGetOpaque(page)).flags & GIN_DATA) != 0
}
#[inline]
pub unsafe fn GinPageSetData(page: Page) {
    (*GinPageGetOpaque(page)).flags |= GIN_DATA;
}
#[inline]
pub unsafe fn GinPageIsList(page: Page) -> bool {
    ((*GinPageGetOpaque(page)).flags & GIN_LIST) != 0
}
#[inline]
pub unsafe fn GinPageSetList(page: Page) {
    (*GinPageGetOpaque(page)).flags |= GIN_LIST;
}
#[inline]
pub unsafe fn GinPageHasFullRow(page: Page) -> bool {
    ((*GinPageGetOpaque(page)).flags & GIN_LIST_FULLROW) != 0
}
#[inline]
pub unsafe fn GinPageSetFullRow(page: Page) {
    (*GinPageGetOpaque(page)).flags |= GIN_LIST_FULLROW;
}
#[inline]
pub unsafe fn GinPageIsCompressed(page: Page) -> bool {
    ((*GinPageGetOpaque(page)).flags & GIN_COMPRESSED) != 0
}
#[inline]
pub unsafe fn GinPageSetCompressed(page: Page) {
    (*GinPageGetOpaque(page)).flags |= GIN_COMPRESSED;
}

#[inline]
pub unsafe fn GinPageIsDeleted(page: Page) -> bool {
    ((*GinPageGetOpaque(page)).flags & GIN_DELETED) != 0
}
#[inline]
pub unsafe fn GinPageSetDeleted(page: Page) {
    (*GinPageGetOpaque(page)).flags |= GIN_DELETED;
}
#[inline]
pub unsafe fn GinPageSetNonDeleted(page: Page) {
    (*GinPageGetOpaque(page)).flags &= !GIN_DELETED;
}
#[inline]
pub unsafe fn GinPageIsIncompleteSplit(page: Page) -> bool {
    ((*GinPageGetOpaque(page)).flags & GIN_INCOMPLETE_SPLIT) != 0
}

#[inline]
pub unsafe fn GinPageRightMost(page: Page) -> bool {
    (*GinPageGetOpaque(page)).rightlink == InvalidBlockNumber
}

/*
 * We should reclaim deleted page only once every transaction started before
 * its deletion is over.
 */
#[inline]
pub unsafe fn GinPageGetDeleteXid(page: Page) -> TransactionId {
    (*(page as PageHeader)).pd_prune_xid
}
#[inline]
pub unsafe fn GinPageSetDeleteXid(page: Page, xid: TransactionId) {
    (*(page as PageHeader)).pd_prune_xid = xid;
}

pub unsafe fn GinPageIsRecyclable(page: Page) -> bool { unimplemented!() }

/*
 * We use our own ItemPointerGet(BlockNumber|OffsetNumber)
 * to avoid Asserts, since sometimes the ip_posid isn't "valid"
 */
#[inline]
pub unsafe fn GinItemPointerGetBlockNumber(pointer: *const ItemPointerData) -> BlockNumber {
    ItemPointerGetBlockNumberNoCheck(pointer)
}

#[inline]
pub unsafe fn GinItemPointerGetOffsetNumber(pointer: *const ItemPointerData) -> OffsetNumber {
    ItemPointerGetOffsetNumberNoCheck(pointer)
}

#[inline]
pub unsafe fn GinItemPointerSetBlockNumber(pointer: *mut ItemPointerData, blkno: BlockNumber) {
    ItemPointerSetBlockNumber(pointer, blkno);
}

#[inline]
pub unsafe fn GinItemPointerSetOffsetNumber(pointer: *mut ItemPointerData, offnum: OffsetNumber) {
    ItemPointerSetOffsetNumber(pointer, offnum);
}

/*
 * Special-case item pointer values needed by the GIN search logic.
 */
#[inline]
pub unsafe fn ItemPointerSetMin(p: *mut ItemPointerData) {
    ItemPointerSet(p, 0 as BlockNumber, 0 as OffsetNumber);
}
#[inline]
pub unsafe fn ItemPointerIsMin(p: *const ItemPointerData) -> bool {
    GinItemPointerGetOffsetNumber(p) == 0 as OffsetNumber
        && GinItemPointerGetBlockNumber(p) == 0 as BlockNumber
}
#[inline]
pub unsafe fn ItemPointerSetMax(p: *mut ItemPointerData) {
    ItemPointerSet(p, InvalidBlockNumber, 0xffff as OffsetNumber);
}
#[inline]
pub unsafe fn ItemPointerSetLossyPage(p: *mut ItemPointerData, b: BlockNumber) {
    ItemPointerSet(p, b, 0xffff as OffsetNumber);
}
#[inline]
pub unsafe fn ItemPointerIsLossyPage(p: *const ItemPointerData) -> bool {
    GinItemPointerGetOffsetNumber(p) == 0xffff as OffsetNumber
        && GinItemPointerGetBlockNumber(p) != InvalidBlockNumber
}

/*
 * Posting item in a non-leaf posting-tree page
 */
#[repr(C)]
pub struct PostingItem {
    /* We use BlockIdData not BlockNumber to avoid padding space wastage */
    pub child_blkno: BlockIdData,
    pub key: ItemPointerData,
}

#[inline]
pub unsafe fn PostingItemGetBlockNumber(pointer: *const PostingItem) -> BlockNumber {
    BlockIdGetBlockNumber(&(*pointer).child_blkno)
}

#[inline]
pub unsafe fn PostingItemSetBlockNumber(pointer: *mut PostingItem, blockNumber: BlockNumber) {
    BlockIdSet(&mut (*pointer).child_blkno, blockNumber);
}

/*
 * Category codes to distinguish placeholder nulls from ordinary NULL keys.
 */
pub type GinNullCategory = i8;

pub const GIN_CAT_NORM_KEY: GinNullCategory = 0; /* normal, non-null key value */
pub const GIN_CAT_NULL_KEY: GinNullCategory = 1; /* null key value */
pub const GIN_CAT_EMPTY_ITEM: GinNullCategory = 2; /* placeholder for zero-key item */
pub const GIN_CAT_NULL_ITEM: GinNullCategory = 3; /* placeholder for null item */
pub const GIN_CAT_EMPTY_QUERY: GinNullCategory = -1; /* placeholder for full-scan query */

/*
 * Access macros for null category byte in entry tuples
 *
 * Note: these reference IndexTupleData (t_info) and GinState (oneCol), which
 * live in other modules; the accessors take raw pointers to match C semantics.
 */
// TODO: dedup - IndexTupleData and GinState come from indextuple.rs / gin.rs.
#[inline]
pub unsafe fn GinCategoryOffset(t_info: uint16, oneCol: bool) -> Size {
    IndexInfoFindDataOffset(t_info) + (if oneCol { 0 } else { size_of::<i16>() })
}

/*
 * Maximum size of an item on entry tree page. Make sure that we fit at least
 * three items on each page.
 */
#[inline]
pub fn GinMaxItemSize() -> usize {
    crate::c::Min(
        INDEX_SIZE_MASK as usize,
        MAXALIGN_DOWN(
            ((BLCKSZ as usize)
                - MAXALIGN(SizeOfPageHeaderData + 3 * size_of::<ItemIdData>())
                - MAXALIGN(size_of::<GinPageOpaqueData>()))
                / 3,
        ),
    )
}

/*
 * Access macros for leaf-page entry tuples (see discussion in README)
 *
 * These operate on the t_tid field of an IndexTupleData; callers pass a
 * pointer to that ItemPointerData directly.
 */
pub const GIN_TREE_POSTING: OffsetNumber = 0xffff;

#[inline]
pub unsafe fn GinGetNPosting(t_tid: *const ItemPointerData) -> OffsetNumber {
    GinItemPointerGetOffsetNumber(t_tid)
}
#[inline]
pub unsafe fn GinSetNPosting(t_tid: *mut ItemPointerData, n: OffsetNumber) {
    ItemPointerSetOffsetNumber(t_tid, n);
}
#[inline]
pub unsafe fn GinIsPostingTree(t_tid: *const ItemPointerData) -> bool {
    GinGetNPosting(t_tid) == GIN_TREE_POSTING
}
#[inline]
pub unsafe fn GinSetPostingTree(t_tid: *mut ItemPointerData, blkno: BlockNumber) {
    GinSetNPosting(t_tid, GIN_TREE_POSTING);
    ItemPointerSetBlockNumber(t_tid, blkno);
}
#[inline]
pub unsafe fn GinGetPostingTree(t_tid: *const ItemPointerData) -> BlockNumber {
    GinItemPointerGetBlockNumber(t_tid)
}

pub const GIN_ITUP_COMPRESSED: uint32 = 1u32 << 31;

#[inline]
pub unsafe fn GinGetPostingOffset(t_tid: *const ItemPointerData) -> uint32 {
    GinItemPointerGetBlockNumber(t_tid) & (!GIN_ITUP_COMPRESSED)
}
#[inline]
pub unsafe fn GinSetPostingOffset(t_tid: *mut ItemPointerData, n: uint32) {
    ItemPointerSetBlockNumber(t_tid, n | GIN_ITUP_COMPRESSED);
}
/// Returns a pointer to the posting data given the tuple base pointer and its t_tid.
#[inline]
pub unsafe fn GinGetPosting(itup: *const c_char, t_tid: *const ItemPointerData) -> Pointer {
    (itup as *mut c_char).add(GinGetPostingOffset(t_tid) as usize) as Pointer
}
#[inline]
pub unsafe fn GinItupIsCompressed(t_tid: *const ItemPointerData) -> bool {
    (GinItemPointerGetBlockNumber(t_tid) & GIN_ITUP_COMPRESSED) != 0
}

/*
 * Access macros for non-leaf entry tuples
 */
#[inline]
pub unsafe fn GinGetDownlink(t_tid: *const ItemPointerData) -> BlockNumber {
    GinItemPointerGetBlockNumber(t_tid)
}
#[inline]
pub unsafe fn GinSetDownlink(t_tid: *mut ItemPointerData, blkno: BlockNumber) {
    ItemPointerSet(t_tid, blkno, InvalidOffsetNumber);
}

/*
 * Data (posting tree) pages
 */
#[inline]
pub unsafe fn GinDataLeafPageGetPostingList(page: Page) -> *mut GinPostingList {
    PageGetContents(page).add(MAXALIGN(size_of::<ItemPointerData>())) as *mut GinPostingList
}
#[inline]
pub unsafe fn GinDataLeafPageGetPostingListSize(page: Page) -> usize {
    (*(page as PageHeader)).pd_lower as usize
        - MAXALIGN(SizeOfPageHeaderData)
        - MAXALIGN(size_of::<ItemPointerData>())
}

#[inline]
pub unsafe fn GinDataLeafPageIsEmpty(page: Page) -> bool {
    if GinPageIsCompressed(page) {
        GinDataLeafPageGetPostingListSize(page) == 0
    } else {
        (*GinPageGetOpaque(page)).maxoff < crate::storage::off::FirstOffsetNumber
    }
}

#[inline]
pub unsafe fn GinDataLeafPageGetFreeSpace(page: Page) -> Size {
    PageGetExactFreeSpace(page)
}

#[inline]
pub unsafe fn GinDataPageGetRightBound(page: Page) -> ItemPointer {
    PageGetContents(page) as ItemPointer
}
/*
 * Pointer to the data portion of a posting tree page.
 */
#[inline]
pub unsafe fn GinDataPageGetData(page: Page) -> *mut c_char {
    PageGetContents(page).add(MAXALIGN(size_of::<ItemPointerData>()))
}
/* non-leaf pages contain PostingItems */
#[inline]
pub unsafe fn GinDataPageGetPostingItem(page: Page, i: usize) -> *mut PostingItem {
    GinDataPageGetData(page).add((i - 1) * size_of::<PostingItem>()) as *mut PostingItem
}

/*
 * Note: there is no GinDataPageGetDataSize macro.
 */
#[inline]
pub unsafe fn GinDataPageSetDataSize(page: Page, size: usize) {
    debug_assert!(size <= GinDataPageMaxDataSize());
    (*(page as PageHeader)).pd_lower =
        (size + MAXALIGN(SizeOfPageHeaderData) + MAXALIGN(size_of::<ItemPointerData>())) as _;
}

#[inline]
pub unsafe fn GinNonLeafDataPageGetFreeSpace(page: Page) -> usize {
    GinDataPageMaxDataSize()
        - (*GinPageGetOpaque(page)).maxoff as usize * size_of::<PostingItem>()
}

#[inline]
pub fn GinDataPageMaxDataSize() -> usize {
    (BLCKSZ as usize)
        - MAXALIGN(SizeOfPageHeaderData)
        - MAXALIGN(size_of::<ItemPointerData>())
        - MAXALIGN(size_of::<GinPageOpaqueData>())
}

/*
 * List pages
 */
#[inline]
pub fn GinListPageSize() -> usize {
    (BLCKSZ as usize) - SizeOfPageHeaderData - MAXALIGN(size_of::<GinPageOpaqueData>())
}

/*
 * A compressed posting list.
 *
 * Note: This requires 2-byte alignment.
 */
#[repr(C)]
pub struct GinPostingList {
    pub first: ItemPointerData, /* first item in this posting list (unpacked) */
    pub nbytes: uint16,         /* number of bytes that follow */
    pub bytes: [u8; FLEXIBLE_ARRAY_MEMBER], /* varbyte encoded items */
}

#[inline]
pub unsafe fn SizeOfGinPostingList(plist: *const GinPostingList) -> usize {
    offset_of!(GinPostingList, bytes) + SHORTALIGN((*plist).nbytes as usize)
}
#[inline]
pub unsafe fn GinNextPostingListSegment(cur: *const GinPostingList) -> *mut GinPostingList {
    (cur as *mut c_char).add(SizeOfGinPostingList(cur)) as *mut GinPostingList
}
