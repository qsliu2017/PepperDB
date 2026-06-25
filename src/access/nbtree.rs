//! Translated from PostgreSQL src/include/access/nbtree.h
//! header file for postgres btree access method implementation.
//!
//! On-disk: BTPageOpaqueData (page special space), BTMetaPageData, and
//! BTDeletedPageData are `#[repr(C)]` with layout asserts. BTreeTupleData is just
//! the shared IndexTuple (access/itup) -- nbtree's pivot/posting metadata is
//! bit-packed into the tuple's t_info bit and into tid's offset-number word
//! (BT_OFFSET_MASK low 12 bits = count, BT_STATUS_OFFSET_MASK high 4 bits =
//! status); kept as raw masks + accessor fns (bitflags-port.md appendix C), NOT
//! bitflags. BTP_* page flags and SK_BT_* scan-key flags ARE clean single-bit
//! sets -> bitflags (GOOD). BTScanOpaqueData/BTScanPosData and the insert/dedup
//! working areas are in-memory state. AM entry points -> stubs.

use bitflags::bitflags;

use crate::access::amapi::{IndexAMProperty, OpFamilyMember};
use crate::access::cmptype::CompareType;
use crate::access::genam::{
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexUniqueCheck,
    IndexVacuumInfo,
};
use crate::access::itup::{
    IndexTupleData, IndexTupleSize, INDEX_AM_RESERVED_BIT, INDEX_SIZE_MASK,
};
use crate::access::parallel::{dsm_segment, shm_toc};
use crate::access::relscan::IndexScanDesc;
use crate::access::sdir::ScanDirection;
use crate::access::skey::{ScanKey, ScanKeyData};
use crate::access::stratnum::{StrategyNumber, BT_MAX_STRATEGY_NUMBER};
use crate::access::transam::{FullTransactionId, FIRST_NORMAL_FULL_TRANSACTION_ID};
use crate::access::xlogdefs::XLogRecPtr;
use crate::c::{bytea, float8};
use crate::catalog::pg_index::IndOption;
use crate::fmgr::{FmgrInfo, MemoryContext};
use crate::nodes::execnodes::IndexInfo;
use crate::nodes::tidbitmap::TIDBitmap;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::buf::Buffer;
use crate::storage::bufmgr::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE};
use crate::storage::bufpage::{LocationIndex, Page, SizeOfPageHeaderData};
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::utils::relcache::Relation;
use crate::utils::skipsupport::SkipSupport;

/// C `ItemPointer` is `ItemPointerData *`; itemptr.rs only exports the value type.
pub type ItemPointer = *mut ItemPointerData; // TODO(ptr)

/// There's room for a 16-bit vacuum cycle ID in BTPageOpaqueData.
pub type BTCycleId = u16;

/// BTPageOpaqueData -- stored at the end of every btree page (special space):
/// sibling links, level, flag bits, and the vacuum cycle ID. On-disk.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BTPageOpaqueData {
    pub prev: BlockNumber,   // left sibling, or P_NONE if leftmost
    pub next: BlockNumber,   // right sibling, or P_NONE if rightmost
    pub level: u32,          // tree level --- zero for leaf pages
    pub flags: u16,          // flag bits, see BTP_*
    pub cycleid: BTCycleId,  // vacuum cycle ID of latest split
}
pub type BTPageOpaque = *mut BTPageOpaqueData; // TODO(ptr)

const _: () = assert!(core::mem::size_of::<BTPageOpaqueData>() == 16);
const _: () = assert!(core::mem::offset_of!(BTPageOpaqueData, prev) == 0);
const _: () = assert!(core::mem::offset_of!(BTPageOpaqueData, next) == 4);
const _: () = assert!(core::mem::offset_of!(BTPageOpaqueData, level) == 8);
const _: () = assert!(core::mem::offset_of!(BTPageOpaqueData, flags) == 12);
const _: () = assert!(core::mem::offset_of!(BTPageOpaqueData, cycleid) == 14);

/// BTPageGetOpaque - the page's btree opaque area (special pointer).
pub fn BTPageGetOpaque(page: &Page) -> BTPageOpaque {
    page.get_special_pointer().as_ptr() as BTPageOpaque
}

bitflags! {
    /// Bits defined in flags. Clean single-bit page-status set (GOOD).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BTPageFlags: u16 {
        const LEAF            = 1 << 0; // leaf page, i.e. not internal page
        const ROOT            = 1 << 1; // root page (has no parent)
        const DELETED         = 1 << 2; // page has been deleted from tree
        const META            = 1 << 3; // meta-page
        const HALF_DEAD       = 1 << 4; // empty, but still in tree
        const SPLIT_END       = 1 << 5; // rightmost page of split group
        const HAS_GARBAGE     = 1 << 6; // page has LP_DEAD tuples (deprecated)
        const INCOMPLETE_SPLIT = 1 << 7; // right sibling's downlink is missing
        const HAS_FULLXID     = 1 << 8; // contains BTDeletedPageData
    }
}

// Raw bit values for direct flags masking (keep alongside the bitflags set).
pub const BTP_LEAF: u16 = 1 << 0;
pub const BTP_ROOT: u16 = 1 << 1;
pub const BTP_DELETED: u16 = 1 << 2;
pub const BTP_META: u16 = 1 << 3;
pub const BTP_HALF_DEAD: u16 = 1 << 4;
pub const BTP_SPLIT_END: u16 = 1 << 5;
pub const BTP_HAS_GARBAGE: u16 = 1 << 6;
pub const BTP_INCOMPLETE_SPLIT: u16 = 1 << 7;
pub const BTP_HAS_FULLXID: u16 = 1 << 8;

/// Max allowed cycle ID -- leaves the last 2 special-space bytes usable as a
/// pg_filedump index-type indicator.
pub const MAX_BT_CYCLE_ID: u16 = 0xFF7F;

/// BTMetaPageData -- the meta page (always the first page in the index). On-disk.
#[repr(C)]
pub struct BTMetaPageData {
    pub magic: u32,       // should contain BTREE_MAGIC
    pub version: u32,     // nbtree version (always <= BTREE_VERSION)
    pub root: BlockNumber, // current root location
    pub level: u32,       // tree level of the root page
    pub fastroot: BlockNumber, // current "fast" root location
    pub fastlevel: u32,   // tree level of the "fast" root page
    // remaining fields only valid when version >= BTREE_NOVAC_VERSION
    pub last_cleanup_num_delpages: u32, // # deleted, non-recyclable pages, last cleanup
    pub last_cleanup_num_heap_tuples: float8, // # heap tuples, last cleanup (deprecated)
    pub allequalimage: bool, // are all columns "equalimage"?
}

/// BTPageGetMeta - the meta page contents.
pub fn BTPageGetMeta(p: &Page) -> *mut BTMetaPageData {
    p.get_contents().as_ptr() as *mut BTMetaPageData
}

pub const BTREE_METAPAGE: BlockNumber = 0; // first page is meta
pub const BTREE_MAGIC: u32 = 0x053162; // magic number in metapage
pub const BTREE_VERSION: u32 = 4; // current version number
pub const BTREE_MIN_VERSION: u32 = 2; // minimum supported version
pub const BTREE_NOVAC_VERSION: u32 = 3; // version with all meta fields set

/// Maximum size of a btree index entry, including its tuple header. Restricts any
/// one item to 1/3 the per-page available space (so three fit on every page),
/// less room for a possible tiebreaker heap TID added by _bt_truncate().
pub const fn bt_max_item_size() -> usize {
    let avail = crate::pg_config::BLCKSZ as usize
        - maxalign(SizeOfPageHeaderData + 3 * core::mem::size_of::<crate::storage::itemid::ItemIdData>())
        - maxalign(core::mem::size_of::<BTPageOpaqueData>());
    maxalign_down(avail / 3) - maxalign(core::mem::size_of::<ItemPointerData>())
}
pub const fn bt_max_item_size_no_heap_tid() -> usize {
    let avail = crate::pg_config::BLCKSZ as usize
        - maxalign(SizeOfPageHeaderData + 3 * core::mem::size_of::<crate::storage::itemid::ItemIdData>())
        - maxalign(core::mem::size_of::<BTPageOpaqueData>());
    maxalign_down(avail / 3)
}

const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}
const fn maxalign_down(n: usize) -> usize {
    n & !7
}

/// MaxTIDsPerBTreePage -- upper bound on heap TIDs storable on a btree leaf page.
pub const MaxTIDsPerBTreePage: usize = (crate::pg_config::BLCKSZ as usize
    - SizeOfPageHeaderData
    - core::mem::size_of::<BTPageOpaqueData>())
    / core::mem::size_of::<ItemPointerData>();

pub const BTREE_MIN_FILLFACTOR: i32 = 10;
pub const BTREE_DEFAULT_FILLFACTOR: i32 = 90;
pub const BTREE_NONLEAF_FILLFACTOR: i32 = 70;
pub const BTREE_SINGLEVAL_FILLFACTOR: i32 = 96;

/// Special "no page number" sentinel (zero -- the meta page is never pointed to).
pub const P_NONE: BlockNumber = 0;

// Macros to test page state kept in the opaque data -> accessor fns.
pub fn P_LEFTMOST(opaque: &BTPageOpaqueData) -> bool {
    opaque.prev == P_NONE
}
pub fn P_RIGHTMOST(opaque: &BTPageOpaqueData) -> bool {
    opaque.next == P_NONE
}
pub fn P_ISLEAF(opaque: &BTPageOpaqueData) -> bool {
    opaque.flags & BTP_LEAF != 0
}
pub fn P_ISROOT(opaque: &BTPageOpaqueData) -> bool {
    opaque.flags & BTP_ROOT != 0
}
pub fn P_ISDELETED(opaque: &BTPageOpaqueData) -> bool {
    opaque.flags & BTP_DELETED != 0
}
pub fn P_ISMETA(opaque: &BTPageOpaqueData) -> bool {
    opaque.flags & BTP_META != 0
}
pub fn P_ISHALFDEAD(opaque: &BTPageOpaqueData) -> bool {
    opaque.flags & BTP_HALF_DEAD != 0
}
pub fn P_IGNORE(opaque: &BTPageOpaqueData) -> bool {
    opaque.flags & (BTP_DELETED | BTP_HALF_DEAD) != 0
}
pub fn P_HAS_GARBAGE(opaque: &BTPageOpaqueData) -> bool {
    opaque.flags & BTP_HAS_GARBAGE != 0
}
pub fn P_INCOMPLETE_SPLIT(opaque: &BTPageOpaqueData) -> bool {
    opaque.flags & BTP_INCOMPLETE_SPLIT != 0
}
pub fn P_HAS_FULLXID(opaque: &BTPageOpaqueData) -> bool {
    opaque.flags & BTP_HAS_FULLXID != 0
}

/// BTDeletedPageData -- the page contents of a deleted page. On-disk.
#[repr(C)]
pub struct BTDeletedPageData {
    pub safexid: FullTransactionId, // See BTPageIsRecyclable()
}
const _: () = assert!(core::mem::size_of::<BTDeletedPageData>() == 8);

/// Mark a page deleted, storing safexid in the page's tuple area (static inline).
pub fn BTPageSetDeleted(_page: &mut Page, _safexid: FullTransactionId) {
    // Mutates BTPageOpaque flags + PageHeader lower/upper + writes
    // BTDeletedPageData into PageGetContents; needs mutable Page + PageHeader
    // access not yet wired in the skeleton.
    unimplemented!()
}

/// Get a deleted page's safexid (static inline). pg_upgrade'd pages without a
/// full xid are always safe to recycle (return FirstNormalFullTransactionId).
pub fn BTPageGetDeleteXid(page: &Page) -> FullTransactionId {
    let opaque = unsafe { &*BTPageGetOpaque(page) };
    if !P_HAS_FULLXID(opaque) {
        return FIRST_NORMAL_FULL_TRANSACTION_ID;
    }
    let contents = page.get_contents().as_ptr() as *const BTDeletedPageData;
    unsafe { (*contents).safexid }
}

/// Is an existing page recyclable? (static inline)
pub fn BTPageIsRecyclable(page: &Page, heaprel: Relation) -> bool {
    let opaque = unsafe { &*BTPageGetOpaque(page) };
    if P_ISDELETED(opaque) {
        let safexid = BTPageGetDeleteXid(page);
        // Recycle iff the deletion XID can no longer be visible to any scan.
        return crate::utils::snapmgr::GlobalVisCheckRemovableFullXid(heaprel, safexid);
    }
    false
}

/// BTPendingFSM -- one newly deleted page pending return to the FSM.
pub struct BTPendingFSM {
    pub target: BlockNumber,       // Page deleted by current VACUUM
    pub safexid: FullTransactionId, // Page's BTDeletedPageData.safexid
}

/// BTVacState -- private nbtree.c VACUUM state (exported for nbtpage.c).
pub struct BTVacState<'a> {
    pub info: *mut IndexVacuumInfo,        // TODO(ptr)
    pub stats: *mut IndexBulkDeleteResult, // TODO(ptr)
    pub callback: Option<Box<IndexBulkDeleteCallback<'a>>>, // void *callback_state -> closure
    pub cycleid: BTCycleId,
    pub pagedelcontext: MemoryContext,

    // _bt_pendingfsm_finalize() state
    pub bufsize: i32,    // pendingpages space (in # elements)
    pub maxbufsize: i32, // max bufsize that respects work_mem
    pub pendingpages: Vec<BTPendingFSM>, // one entry per newly deleted page
    pub npendingpages: i32, // current # valid pendingpages
}

/// High key / first data key offset numbers (Lehman & Yao high key in item 1).
pub const P_HIKEY: OffsetNumber = 1;
pub const P_FIRSTKEY: OffsetNumber = 2;
/// First data key: rightmost pages have no high key, so data begins in item 1.
pub fn P_FIRSTDATAKEY(opaque: &BTPageOpaqueData) -> OffsetNumber {
    if P_RIGHTMOST(opaque) {
        P_HIKEY
    } else {
        P_FIRSTKEY
    }
}

// === B-Tree tuple format: pivot/posting metadata packed into t_info + tid ===

/// INDEX_ALT_TID_MASK: the t_info bit (INDEX_AM_RESERVED_BIT) marking a tuple as
/// using the alternative tid representation (pivot or posting).
pub const INDEX_ALT_TID_MASK: u16 = INDEX_AM_RESERVED_BIT;

// Item pointer offset-number bit masks (packs a count beside 4 status bits;
// on-disk word, NOT a flag set -- raw masks + accessors, bitflags-port.md app. C).
pub const BT_OFFSET_MASK: u16 = 0x0FFF; // low 12 bits: # keys / # heap TIDs
pub const BT_STATUS_OFFSET_MASK: u16 = 0xF000; // high 4 bits: status
// BT_STATUS_OFFSET_MASK status bits
pub const BT_PIVOT_HEAP_TID_ATTR: u16 = 0x1000;
pub const BT_IS_POSTING: u16 = 0x2000;

// The key-count mask must fit the max possible number of index attributes.
const _: () = assert!(BT_OFFSET_MASK >= crate::pg_config_manual::INDEX_MAX_KEYS as u16);

/// True iff `itup` is a pivot tuple. Can have false negatives (not positives)
/// with !heapkeyspace indexes.
pub fn BTreeTupleIsPivot(itup: &IndexTupleData) -> bool {
    if itup.t_info & INDEX_ALT_TID_MASK == 0 {
        return false;
    }
    // absence of BT_IS_POSTING in offset number indicates pivot tuple
    itup.tid.offset_number_no_check() & BT_IS_POSTING == 0
}

/// True iff `itup` is a posting-list tuple.
pub fn BTreeTupleIsPosting(itup: &IndexTupleData) -> bool {
    if itup.t_info & INDEX_ALT_TID_MASK == 0 {
        return false;
    }
    // presence of BT_IS_POSTING in offset number indicates posting tuple
    itup.tid.offset_number_no_check() & BT_IS_POSTING != 0
}

/// Make `itup` a posting-list tuple: nhtids in the offset field (with BT_IS_POSTING),
/// posting list byte offset in the block-number field.
pub fn BTreeTupleSetPosting(itup: &mut IndexTupleData, nhtids: u16, postingoffset: usize) {
    debug_assert!(nhtids > 1);
    debug_assert!(nhtids & BT_STATUS_OFFSET_MASK == 0);
    debug_assert!(postingoffset == maxalign(postingoffset));
    debug_assert!(postingoffset < INDEX_SIZE_MASK as usize);
    debug_assert!(!BTreeTupleIsPivot(itup));

    itup.t_info |= INDEX_ALT_TID_MASK;
    itup.tid.set_offset_number(nhtids | BT_IS_POSTING);
    itup.tid.set_block_number(postingoffset as BlockNumber);
}

/// Number of heap TIDs in a posting-list tuple (low 12 bits of the offset field).
pub fn BTreeTupleGetNPosting(posting: &IndexTupleData) -> u16 {
    debug_assert!(BTreeTupleIsPosting(posting));
    posting.tid.offset_number_no_check() & BT_OFFSET_MASK
}

/// Byte offset of the posting list within a posting-list tuple (block-number field).
pub fn BTreeTupleGetPostingOffset(posting: &IndexTupleData) -> u32 {
    debug_assert!(BTreeTupleIsPosting(posting));
    posting.tid.block_number_no_check()
}

/// Pointer to the posting list (TID array) inside a posting-list tuple.
pub fn BTreeTupleGetPosting(posting: &IndexTupleData) -> ItemPointer {
    let off = BTreeTupleGetPostingOffset(posting) as usize;
    unsafe { (posting as *const IndexTupleData as *const u8).add(off) as ItemPointer }
}

/// Pointer to the n-th posting-list TID.
pub fn BTreeTupleGetPostingN(posting: &IndexTupleData, n: i32) -> ItemPointer {
    unsafe { BTreeTupleGetPosting(posting).add(n as usize) }
}

/// Get downlink block number from a pivot tuple's tid (no pivot assert).
pub fn BTreeTupleGetDownLink(pivot: &IndexTupleData) -> BlockNumber {
    pivot.tid.block_number_no_check()
}

/// Set downlink block number in a pivot tuple's tid.
pub fn BTreeTupleSetDownLink(pivot: &mut IndexTupleData, blkno: BlockNumber) {
    pivot.tid.set_block_number(blkno);
}

/// Number of attributes within a tuple (excludes implicit heap-TID tiebreaker).
/// C macro (avoids including rel.h); `rel` only used in the non-pivot branch.
pub fn BTreeTupleGetNAtts(itup: &IndexTupleData, rel: Relation) -> u16 {
    if BTreeTupleIsPivot(itup) {
        itup.tid.offset_number_no_check() & BT_OFFSET_MASK
    } else {
        IndexRelationGetNumberOfAttributes(rel)
    }
}

/// Set number of key attributes in a tuple; optionally flag a trailing heap TID.
pub fn BTreeTupleSetNAtts(itup: &mut IndexTupleData, nkeyatts: u16, heaptid: bool) {
    debug_assert!(nkeyatts <= crate::pg_config_manual::INDEX_MAX_KEYS as u16);
    debug_assert!(nkeyatts & BT_STATUS_OFFSET_MASK == 0);
    debug_assert!(!heaptid || nkeyatts > 0);
    debug_assert!(!BTreeTupleIsPivot(itup) || nkeyatts == 0);

    itup.t_info |= INDEX_ALT_TID_MASK;

    let off = if heaptid {
        nkeyatts | BT_PIVOT_HEAP_TID_ATTR
    } else {
        nkeyatts
    };
    // BT_IS_POSTING bit is deliberately unset here.
    itup.tid.set_offset_number(off);
    debug_assert!(BTreeTupleIsPivot(itup));
}

/// Get a leaf page's "top parent" link from its high key (page deletion).
pub fn BTreeTupleGetTopParent(leafhikey: &IndexTupleData) -> BlockNumber {
    leafhikey.tid.block_number_no_check()
}

/// Set a leaf page's "top parent" link in its high key (page deletion).
pub fn BTreeTupleSetTopParent(leafhikey: &mut IndexTupleData, blkno: BlockNumber) {
    leafhikey.tid.set_block_number(blkno);
    BTreeTupleSetNAtts(leafhikey, 0, false);
}

/// Tiebreaker heap TID, if any (lowest TID for a posting list). None if truncated.
pub fn BTreeTupleGetHeapTID(itup: &IndexTupleData) -> Option<ItemPointer> {
    if BTreeTupleIsPivot(itup) {
        if itup.tid.offset_number_no_check() & BT_PIVOT_HEAP_TID_ATTR != 0 {
            let p = unsafe {
                (itup as *const IndexTupleData as *const u8)
                    .add(IndexTupleSize(itup) - core::mem::size_of::<ItemPointerData>())
            } as ItemPointer;
            return Some(p);
        }
        // Heap TID attribute was truncated.
        None
    } else if BTreeTupleIsPosting(itup) {
        Some(BTreeTupleGetPosting(itup))
    } else {
        Some(&itup.tid as *const ItemPointerData as ItemPointer)
    }
}

/// Maximum heap TID (the only TID for a plain non-pivot tuple). Non-pivot only.
pub fn BTreeTupleGetMaxHeapTID(itup: &IndexTupleData) -> ItemPointer {
    debug_assert!(!BTreeTupleIsPivot(itup));
    if BTreeTupleIsPosting(itup) {
        let nposting = BTreeTupleGetNPosting(itup);
        return BTreeTupleGetPostingN(itup, nposting as i32 - 1);
    }
    &itup.tid as *const ItemPointerData as ItemPointer
}

/// Commute a btree strategy number by subtraction.
pub const fn BTCommuteStrategyNumber(strat: StrategyNumber) -> StrategyNumber {
    BT_MAX_STRATEGY_NUMBER + 1 - strat
}

// amproc procedure numbers for a btree operator class.
pub const BTORDER_PROC: u16 = 1;
pub const BTSORTSUPPORT_PROC: u16 = 2;
pub const BTINRANGE_PROC: u16 = 3;
pub const BTEQUALIMAGE_PROC: u16 = 4;
pub const BTOPTIONS_PROC: u16 = 5;
pub const BTSKIPSUPPORT_PROC: u16 = 6;
pub const BTNProcs: u16 = 6;

/// Read vs write page-lock requests (mapped to buffer lock modes).
pub const BT_READ: i32 = BUFFER_LOCK_SHARE;
pub const BT_WRITE: i32 = BUFFER_LOCK_EXCLUSIVE;

/// BTStackData -- stack of pivot-tuple locations recorded while descending the
/// tree, walked back up to insert into parents after a split. In-memory.
pub struct BTStackData {
    pub blkno: BlockNumber,
    pub offset: OffsetNumber,
    pub parent: Option<Box<BTStackData>>,
}
pub type BTStack = Option<Box<BTStackData>>;

/// BTScanInsertData -- the insertion scankey used to descend a B-Tree via
/// _bt_search (an "insertion" scankey, not a search scankey). In-memory. The C
/// stack-allocated FAM `scankeys[INDEX_MAX_KEYS]` becomes a Vec; `keysz` = len().
pub struct BTScanInsertData {
    pub heapkeyspace: bool,
    pub allequalimage: bool,
    pub anynullkeys: bool,
    pub nextkey: bool,
    pub backward: bool, // backward index scan?
    pub scantid: Option<ItemPointer>, // tiebreaker for scankeys (TODO(ptr))
    pub keysz: i32,     // Size of scankeys array
    pub scankeys: Vec<ScanKeyData>,
}
pub type BTScanInsert = *mut BTScanInsertData; // TODO(ptr)

/// BTInsertStateData -- working area used during insertion. In-memory.
pub struct BTInsertStateData {
    pub itup: *mut IndexTupleData, // Item we're inserting (TODO(ptr))
    pub itemsz: usize,             // Size of itup -- MAXALIGN()'d
    pub itup_key: BTScanInsert,    // Insertion scankey

    pub buf: Buffer, // leaf page we're likely to insert itup on

    // Cached bounds within buf (only with _bt_check_unique).
    pub bounds_valid: bool,
    pub low: OffsetNumber,
    pub stricthigh: OffsetNumber,

    // Position inside an existing posting list, or -1 (LP_DEAD overlap sentinel).
    pub postingoff: i32,
}
pub type BTInsertState = *mut BTInsertStateData; // TODO(ptr)

/// One pending tuple interval during deduplication. In-memory.
pub struct BTDedupInterval {
    pub baseoff: OffsetNumber,
    pub nitems: u16,
}

/// BTDedupStateData -- working area used during deduplication. In-memory. The C
/// FAM `intervals[MaxIndexTuplesPerPage]` becomes a Vec.
pub struct BTDedupStateData {
    // Deduplication status info for entire pass over page
    pub deduplicate: bool,     // Still deduplicating page?
    pub nmaxitems: i32,        // Number of max-sized tuples so far
    pub maxpostingsize: usize, // Limit on size of final tuple

    // Metadata about base tuple of current pending posting list
    pub base: *mut IndexTupleData, // Use to form new posting list (TODO(ptr))
    pub baseoff: OffsetNumber,     // page offset of base
    pub basetupsize: usize,        // base size without original posting list

    // Other metadata about pending posting list
    pub htids: ItemPointer, // Heap TIDs in pending posting list (TODO(ptr))
    pub nhtids: i32,        // Number of heap TIDs in htids array
    pub nitems: i32,        // Number of existing tuples/line pointers
    pub phystupsize: usize, // Includes line pointer overhead

    pub nintervals: i32, // current number of intervals in array
    pub intervals: Vec<BTDedupInterval>,
}
pub type BTDedupState = *mut BTDedupStateData; // TODO(ptr)

/// BTVacuumPostingData -- how to VACUUM (or delete) some TIDs of a posting list
/// tuple. In-memory; C FAM `deletetids[]` becomes a Vec.
pub struct BTVacuumPostingData {
    pub itup: *mut IndexTupleData, // Tuple that will be/was updated (TODO(ptr))
    pub updatedoffset: OffsetNumber,

    // State needed to describe final itup in WAL
    pub ndeletedtids: u16,
    pub deletetids: Vec<u16>,
}
pub type BTVacuumPosting = *mut BTVacuumPostingData; // TODO(ptr)

/// What we remember about each match in a scan position. In-memory.
pub struct BTScanPosItem {
    pub heapTid: ItemPointerData,  // TID of referenced heap item
    pub indexOffset: OffsetNumber, // index item's location within page
    pub tupleOffset: LocationIndex, // IndexTuple's offset in workspace, if any
}

/// BTScanPosData -- the data for one scan position (current or marked). In-memory.
/// C FAM `items[MaxTIDsPerBTreePage]` becomes a Vec.
pub struct BTScanPosData {
    pub buf: Buffer, // currPage buf (invalid means unpinned)

    // page details as of the saved position's _bt_readpage call
    pub currPage: BlockNumber,
    pub prevPage: BlockNumber,
    pub nextPage: BlockNumber,
    pub lsn: XLogRecPtr, // currPage's LSN (when so->dropPin)

    pub dir: ScanDirection, // scan direction for the saved position

    pub nextTupleOffset: i32, // first free location in tuple storage workspace

    pub moreLeft: bool,
    pub moreRight: bool,

    pub firstItem: i32, // first valid index in items[]
    pub lastItem: i32,  // last valid index in items[]
    pub itemIndex: i32, // current index in items[]

    pub items: Vec<BTScanPosItem>, // ordered in index order
}
pub type BTScanPos = *mut BTScanPosData; // TODO(ptr)

/// True iff the scan position currently holds a buffer pin.
pub fn BTScanPosIsPinned(scanpos: &BTScanPosData) -> bool {
    scanpos.buf.is_valid()
}
/// Release the scan position's buffer pin.
pub fn BTScanPosUnpin(scanpos: &mut BTScanPosData) {
    // nbtree is a deferred mechanical port still referencing the C-named
    // ReleaseBuffer shim; the real call is `shared.buffers().release_buffer`.
    // TODO(nbtree): route through the pool once the scan carries a SharedState.
    #[allow(deprecated)]
    crate::storage::bufmgr::ReleaseBuffer(scanpos.buf);
    scanpos.buf = crate::storage::buf::INVALID_BUFFER;
}
pub fn BTScanPosUnpinIfPinned(scanpos: &mut BTScanPosData) {
    if BTScanPosIsPinned(scanpos) {
        BTScanPosUnpin(scanpos);
    }
}
/// True iff the scan position is valid (references a valid page).
pub fn BTScanPosIsValid(scanpos: &BTScanPosData) -> bool {
    crate::storage::block::block_number_is_valid(scanpos.currPage)
}
pub fn BTScanPosInvalidate(scanpos: &mut BTScanPosData) {
    scanpos.buf = crate::storage::buf::INVALID_BUFFER;
    scanpos.currPage = INVALID_BLOCK_NUMBER;
}

/// Per equality-type SK_SEARCHARRAY scan key (SAOP arrays and skip arrays).
pub struct BTArrayKeyInfo<'a> {
    // set for both kinds of array
    pub scan_key: i32,  // index of associated key in keyData
    pub num_elems: i32, // number of elems (-1 means skip array)

    // ScalarArrayOpExpr arrays only
    pub elem_values: *mut Datum, // array of num_elems Datums (TODO(ptr))
    pub cur_elem: i32,           // index of current element in elem_values

    // skip arrays only
    pub attlen: i16,    // attr's length, in bytes
    pub attbyval: bool, // attr's FormData_pg_attribute.attbyval
    pub null_elem: bool, // NULL is lowest/highest element?
    pub sksup: SkipSupport<'a>, // skip support (None if opclass lacks it)
    pub low_compare: Option<ScanKey<'a>>, // array's > or >= lower bound
    pub high_compare: Option<ScanKey<'a>>, // array's < or <= upper bound
}

/// BTScanOpaqueData -- the btree-private state needed for an indexscan. In-memory.
pub struct BTScanOpaqueData<'a> {
    // set by _bt_preprocess_keys()
    pub qual_ok: bool,       // false if qual can never be satisfied
    pub numberOfKeys: i32,   // number of preprocessed scan keys
    pub keyData: *mut ScanKeyData, // array of preprocessed scan keys (TODO(ptr))

    // workspace for SK_SEARCHARRAY support
    pub numArrayKeys: i32, // number of equality-type array keys
    pub skipScan: bool,    // At least one skip array in arrayKeys[]?
    pub needPrimScan: bool, // New prim scan to continue in current dir?
    pub scanBehind: bool,  // Check scan not still behind on next page?
    pub oppositeDirCheck: bool, // scanBehind opposite-scan-dir check?
    pub arrayKeys: *mut BTArrayKeyInfo<'a>, // info about each array key (TODO(ptr))
    pub orderProcs: *mut FmgrInfo, // ORDER procs for required equality keys (TODO(ptr))
    pub arrayContext: MemoryContext, // scan-lifespan context for array data

    // info about killed items (killedItems is None if never used)
    pub killedItems: *mut i32, // currPos.items indexes of killed items (TODO(ptr))
    pub numKilled: i32,        // number of currently stored items
    pub dropPin: bool,         // drop leaf pin before btgettuple returns?

    // index-only scan tuple storage workspaces (each BLCKSZ bytes)
    pub currTuples: *mut u8, // tuple storage for currPos (TODO(ptr))
    pub markTuples: *mut u8, // tuple storage for markPos (TODO(ptr))

    pub markItemIndex: i32, // itemIndex, or -1 if not valid

    // keep these last in struct for efficiency
    pub currPos: BTScanPosData, // current position data
    pub markPos: BTScanPosData, // marked position, if any
}
pub type BTScanOpaque = *mut BTScanOpaqueData<'static>; // TODO(ptr)

/// _bt_readpage state used across _bt_checkkeys calls for a page. In-memory.
pub struct BTReadPageState {
    // Input parameters, set by _bt_readpage for _bt_checkkeys
    pub minoff: OffsetNumber, // Lowest non-pivot tuple's offset
    pub maxoff: OffsetNumber, // Highest non-pivot tuple's offset
    pub finaltup: *mut IndexTupleData, // Needed by scans with array keys (TODO(ptr))
    pub page: *mut u8,        // Page being read (TODO(ptr))
    pub firstpage: bool,      // page is first for primitive scan?
    pub forcenonrequired: bool, // treat all keys as nonrequired?
    pub startikey: i32,       // start comparisons from this scan key

    pub offnum: OffsetNumber, // current tuple's page offset number

    // Output parameters, set by _bt_checkkeys for _bt_readpage
    pub skip: OffsetNumber,    // Array keys "look ahead" skip offnum
    pub continuescan: bool,    // Terminate ongoing (primitive) index scan?

    // Private _bt_checkkeys state for "look ahead" / primscan scheduling
    pub rechecks: i16,
    pub targetdistance: i16,
    pub nskipadvances: i16,
}

bitflags! {
    /// Private flags bits in preprocessed scan keys (bits 16-31 are available;
    /// see skey.h). Clean single-bit set (GOOD). The DESC/NULLS_FIRST bits are
    /// remapped from pg_index's indoption[] into the uppermost byte.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SkBtFlags: i32 {
        const REQFWD  = 0x00010000; // required to continue forward scan
        const REQBKWD = 0x00020000; // required to continue backward scan
        const SKIP    = 0x00040000; // skip array on column without input =
        // SK_BT_SKIP-only flags (set/unset by array advancement)
        const MINVAL  = 0x00080000; // invalid argument, use low_compare
        const MAXVAL  = 0x00100000; // invalid argument, use high_compare
        const NEXT    = 0x00200000; // positions the scan > argument
        const PRIOR   = 0x00400000; // positions the scan < argument
    }
}

pub const SK_BT_REQFWD: i32 = 0x00010000;
pub const SK_BT_REQBKWD: i32 = 0x00020000;
pub const SK_BT_SKIP: i32 = 0x00040000;
pub const SK_BT_MINVAL: i32 = 0x00080000;
pub const SK_BT_MAXVAL: i32 = 0x00100000;
pub const SK_BT_NEXT: i32 = 0x00200000;
pub const SK_BT_PRIOR: i32 = 0x00400000;

/// Shift to remap pg_index flag bits to the uppermost SK_BT_* byte.
pub const SK_BT_INDOPTION_SHIFT: i32 = 24;
pub const SK_BT_DESC: i32 = (IndOption::DESC.bits() as i32) << SK_BT_INDOPTION_SHIFT;
pub const SK_BT_NULLS_FIRST: i32 = (IndOption::NULLS_FIRST.bits() as i32) << SK_BT_INDOPTION_SHIFT;

/// BTOptions -- btree reloptions (parsed). On-disk-ish reloptions blob header.
#[repr(C)]
pub struct BTOptions {
    pub varlena_header_: i32, // varlena header (do not touch directly!)
    pub fillfactor: i32,      // page fill factor in percent (0..100)
    pub vacuum_cleanup_index_scale_factor: float8, // deprecated
    pub deduplicate_items: bool, // Try to deduplicate items?
}

/// BTGetFillFactor -- reloption fillfactor, or BTREE_DEFAULT_FILLFACTOR. Reaches
/// into RelationData (rd_options/rd_rel) not yet available in the skeleton.
pub fn BTGetFillFactor(_relation: Relation) -> i32 {
    unimplemented!()
}
/// BTGetTargetPageFreeSpace -- target free space from the fill factor.
pub fn BTGetTargetPageFreeSpace(relation: Relation) -> i32 {
    crate::pg_config::BLCKSZ as i32 * (100 - BTGetFillFactor(relation)) / 100
}
/// BTGetDeduplicateItems -- reloption deduplicate_items (default true).
pub fn BTGetDeduplicateItems(_relation: Relation) -> bool {
    unimplemented!()
}

// Progress-reporting phase numbers (must match btbuildphasename).
pub const PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN: i32 = 2;
pub const PROGRESS_BTREE_PHASE_PERFORMSORT_1: i32 = 3;
pub const PROGRESS_BTREE_PHASE_PERFORMSORT_2: i32 = 4;
pub const PROGRESS_BTREE_PHASE_LEAF_LOAD: i32 = 5;

// === external entry points for btree, in nbtree.c (stubs) ===

pub fn btbuildempty(_index: Relation) {
    unimplemented!()
}

pub fn btinsert(
    _rel: Relation,
    _values: &[Datum],
    _isnull: &[bool],
    _ht_ctid: ItemPointer,
    _heap_rel: Relation,
    _check_unique: IndexUniqueCheck,
    _index_unchanged: bool,
    _index_info: &mut IndexInfo,
) -> bool {
    unimplemented!()
}

pub fn btbeginscan(_rel: Relation, _nkeys: i32, _norderbys: i32) -> IndexScanDesc {
    unimplemented!()
}

pub fn btestimateparallelscan(_rel: Relation, _nkeys: i32, _norderbys: i32) -> usize {
    unimplemented!()
}

pub fn btinitparallelscan(_target: &mut crate::access::relscan::ParallelIndexScanDescData) {
    unimplemented!()
}

pub fn btgettuple(_scan: IndexScanDesc, _dir: ScanDirection) -> bool {
    unimplemented!()
}

pub fn btgetbitmap(_scan: IndexScanDesc, _tbm: &mut TIDBitmap) -> i64 {
    unimplemented!()
}

pub fn btrescan(
    _scan: IndexScanDesc,
    _scankey: ScanKey,
    _nscankeys: i32,
    _orderbys: ScanKey,
    _norderbys: i32,
) {
    unimplemented!()
}

pub fn btparallelrescan(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn btendscan(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn btmarkpos(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn btrestrpos(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn btbulkdelete(
    _info: &mut IndexVacuumInfo,
    _stats: Option<Box<IndexBulkDeleteResult>>,
    _callback: &mut IndexBulkDeleteCallback,
) -> Option<Box<IndexBulkDeleteResult>> {
    unimplemented!()
}

pub fn btvacuumcleanup(
    _info: &mut IndexVacuumInfo,
    _stats: Option<Box<IndexBulkDeleteResult>>,
) -> Option<Box<IndexBulkDeleteResult>> {
    unimplemented!()
}

pub fn btcanreturn(_index: Relation, _attno: i32) -> bool {
    unimplemented!()
}

pub fn btgettreeheight(_rel: Relation) -> i32 {
    unimplemented!()
}

pub fn bttranslatestrategy(_strategy: StrategyNumber, _opfamily: Oid) -> CompareType {
    unimplemented!()
}

pub fn bttranslatecmptype(_cmptype: CompareType, _opfamily: Oid) -> StrategyNumber {
    unimplemented!()
}

// === prototypes for internal functions in nbtree.c (stubs) ===

/// Returns (seized, next_scan_page, last_curr_page). C `*next_scan_page`/
/// `*last_curr_page` out-params folded into the return tuple.
pub fn _bt_parallel_seize(
    _scan: IndexScanDesc,
    _first: bool,
) -> (bool, BlockNumber, BlockNumber) {
    unimplemented!()
}

pub fn _bt_parallel_release(
    _scan: IndexScanDesc,
    _next_scan_page: BlockNumber,
    _curr_page: BlockNumber,
) {
    unimplemented!()
}

pub fn _bt_parallel_done(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn _bt_parallel_primscan_schedule(_scan: IndexScanDesc, _curr_page: BlockNumber) {
    unimplemented!()
}

// === prototypes for functions in nbtdedup.c (stubs) ===

pub fn _bt_dedup_pass(
    _rel: Relation,
    _buf: Buffer,
    _newitem: *mut IndexTupleData,
    _newitemsz: usize,
    _bottomupdedup: bool,
) {
    unimplemented!()
}

pub fn _bt_bottomupdel_pass(
    _rel: Relation,
    _buf: Buffer,
    _heap_rel: Relation,
    _newitemsz: usize,
) -> bool {
    unimplemented!()
}

pub fn _bt_dedup_start_pending(
    _state: BTDedupState,
    _base: *mut IndexTupleData,
    _baseoff: OffsetNumber,
) {
    unimplemented!()
}

pub fn _bt_dedup_save_htid(_state: BTDedupState, _itup: *mut IndexTupleData) -> bool {
    unimplemented!()
}

pub fn _bt_dedup_finish_pending(_newpage: &mut Page, _state: BTDedupState) -> usize {
    unimplemented!()
}

pub fn _bt_form_posting(
    _base: *mut IndexTupleData,
    _htids: ItemPointer,
    _nhtids: i32,
) -> *mut IndexTupleData {
    unimplemented!()
}

pub fn _bt_update_posting(_vacposting: BTVacuumPosting) {
    unimplemented!()
}

pub fn _bt_swap_posting(
    _newitem: *mut IndexTupleData,
    _oposting: *mut IndexTupleData,
    _postingoff: i32,
) -> *mut IndexTupleData {
    unimplemented!()
}

// === prototypes for functions in nbtinsert.c (stubs) ===

pub fn _bt_doinsert(
    _rel: Relation,
    _itup: *mut IndexTupleData,
    _check_unique: IndexUniqueCheck,
    _index_unchanged: bool,
    _heap_rel: Relation,
) -> bool {
    unimplemented!()
}

pub fn _bt_finish_split(_rel: Relation, _heaprel: Relation, _lbuf: Buffer, _stack: BTStack) {
    unimplemented!()
}

pub fn _bt_getstackbuf(
    _rel: Relation,
    _heaprel: Relation,
    _stack: BTStack,
    _child: BlockNumber,
) -> Buffer {
    unimplemented!()
}

// === prototypes for functions in nbtsplitloc.c (stubs) ===

/// Returns (split_offset, newitemonleft). C `*newitemonleft` out-param folded in.
pub fn _bt_findsplitloc(
    _rel: Relation,
    _origpage: &Page,
    _newitemoff: OffsetNumber,
    _newitemsz: usize,
    _newitem: *mut IndexTupleData,
) -> (OffsetNumber, bool) {
    unimplemented!()
}

// === prototypes for functions in nbtpage.c (stubs) ===

pub fn _bt_initmetapage(
    _page: &mut Page,
    _rootbknum: BlockNumber,
    _level: u32,
    _allequalimage: bool,
) {
    unimplemented!()
}

pub fn _bt_vacuum_needs_cleanup(_rel: Relation) -> bool {
    unimplemented!()
}

pub fn _bt_set_cleanup_info(_rel: Relation, _num_delpages: BlockNumber) {
    unimplemented!()
}

pub fn _bt_upgrademetapage(_page: &mut Page) {
    unimplemented!()
}

pub fn _bt_getroot(_rel: Relation, _heaprel: Relation, _access: i32) -> Buffer {
    unimplemented!()
}

pub fn _bt_gettrueroot(_rel: Relation) -> Buffer {
    unimplemented!()
}

pub fn _bt_getrootheight(_rel: Relation) -> i32 {
    unimplemented!()
}

/// Returns (heapkeyspace, allequalimage). C bool out-params folded into a tuple.
pub fn _bt_metaversion(_rel: Relation) -> (bool, bool) {
    unimplemented!()
}

pub fn _bt_checkpage(_rel: Relation, _buf: Buffer) {
    unimplemented!()
}

pub fn _bt_getbuf(_rel: Relation, _blkno: BlockNumber, _access: i32) -> Buffer {
    unimplemented!()
}

pub fn _bt_allocbuf(_rel: Relation, _heaprel: Relation) -> Buffer {
    unimplemented!()
}

pub fn _bt_relandgetbuf(_rel: Relation, _obuf: Buffer, _blkno: BlockNumber, _access: i32) -> Buffer {
    unimplemented!()
}

pub fn _bt_relbuf(_rel: Relation, _buf: Buffer) {
    unimplemented!()
}

pub fn _bt_lockbuf(_rel: Relation, _buf: Buffer, _access: i32) {
    unimplemented!()
}

pub fn _bt_unlockbuf(_rel: Relation, _buf: Buffer) {
    unimplemented!()
}

pub fn _bt_conditionallockbuf(_rel: Relation, _buf: Buffer) -> bool {
    unimplemented!()
}

pub fn _bt_upgradelockbufcleanup(_rel: Relation, _buf: Buffer) {
    unimplemented!()
}

pub fn _bt_pageinit(_page: &mut Page, _size: usize) {
    unimplemented!()
}

pub fn _bt_delitems_vacuum(
    _rel: Relation,
    _buf: Buffer,
    _deletable: &[OffsetNumber],
    _updatable: &mut [BTVacuumPosting],
) {
    unimplemented!()
}

pub fn _bt_delitems_delete_check(
    _rel: Relation,
    _buf: Buffer,
    _heap_rel: Relation,
    _delstate: &mut crate::access::tableam::TM_IndexDeleteOp,
) {
    unimplemented!()
}

pub fn _bt_pagedel(_rel: Relation, _leafbuf: Buffer, _vstate: &mut BTVacState) {
    unimplemented!()
}

pub fn _bt_pendingfsm_init(_rel: Relation, _vstate: &mut BTVacState, _cleanuponly: bool) {
    unimplemented!()
}

pub fn _bt_pendingfsm_finalize(_rel: Relation, _vstate: &mut BTVacState) {
    unimplemented!()
}

// === prototypes for functions in nbtpreprocesskeys.c (stubs) ===

pub fn _bt_preprocess_keys(_scan: IndexScanDesc) {
    unimplemented!()
}

// === prototypes for functions in nbtsearch.c (stubs) ===

/// Returns (stack, buf). C `*bufP` out-param folded into the return tuple.
pub fn _bt_search(
    _rel: Relation,
    _heaprel: Relation,
    _key: BTScanInsert,
    _access: i32,
) -> (BTStack, Buffer) {
    unimplemented!()
}

pub fn _bt_binsrch_insert(_rel: Relation, _insertstate: BTInsertState) -> OffsetNumber {
    unimplemented!()
}

pub fn _bt_compare(
    _rel: Relation,
    _key: BTScanInsert,
    _page: &Page,
    _offnum: OffsetNumber,
) -> i32 {
    unimplemented!()
}

pub fn _bt_first(_scan: IndexScanDesc, _dir: ScanDirection) -> bool {
    unimplemented!()
}

pub fn _bt_next(_scan: IndexScanDesc, _dir: ScanDirection) -> bool {
    unimplemented!()
}

pub fn _bt_get_endpoint(_rel: Relation, _level: u32, _rightmost: bool) -> Buffer {
    unimplemented!()
}

// === prototypes for functions in nbtutils.c (stubs) ===

pub fn _bt_mkscankey(_rel: Relation, _itup: *mut IndexTupleData) -> BTScanInsert {
    unimplemented!()
}

pub fn _bt_freestack(_stack: BTStack) {
    unimplemented!()
}

pub fn _bt_start_prim_scan(_scan: IndexScanDesc, _dir: ScanDirection) -> bool {
    unimplemented!()
}

/// Returns (n_advanced, set_elem_result). C `*set_elem_result` out-param folded in.
pub fn _bt_binsrch_array_skey(
    _orderproc: &mut FmgrInfo,
    _cur_elem_trig: bool,
    _dir: ScanDirection,
    _tupdatum: Datum,
    _tupnull: bool,
    _array: &mut BTArrayKeyInfo,
    _cur: ScanKey,
) -> (i32, i32) {
    unimplemented!()
}

pub fn _bt_start_array_keys(_scan: IndexScanDesc, _dir: ScanDirection) {
    unimplemented!()
}

pub fn _bt_checkkeys(
    _scan: IndexScanDesc,
    _pstate: &mut BTReadPageState,
    _array_keys: bool,
    _tuple: *mut IndexTupleData,
    _tupnatts: i32,
) -> bool {
    unimplemented!()
}

pub fn _bt_scanbehind_checkkeys(
    _scan: IndexScanDesc,
    _dir: ScanDirection,
    _finaltup: *mut IndexTupleData,
) -> bool {
    unimplemented!()
}

pub fn _bt_set_startikey(_scan: IndexScanDesc, _pstate: &mut BTReadPageState) {
    unimplemented!()
}

pub fn _bt_killitems(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn _bt_vacuum_cycleid(_rel: Relation) -> BTCycleId {
    unimplemented!()
}

pub fn _bt_start_vacuum(_rel: Relation) -> BTCycleId {
    unimplemented!()
}

pub fn _bt_end_vacuum(_rel: Relation) {
    unimplemented!()
}

pub fn _bt_end_vacuum_callback(_code: i32, _arg: Datum) {
    unimplemented!()
}

pub fn BTreeShmemSize() -> usize {
    unimplemented!()
}

pub fn BTreeShmemInit() {
    unimplemented!()
}

pub fn btoptions(_reloptions: Datum, _validate: bool) -> *mut bytea {
    unimplemented!()
}

/// Returns (matched, res, isnull). C `*res`/`*isnull` out-params folded in.
pub fn btproperty(
    _index_oid: Oid,
    _attno: i32,
    _prop: IndexAMProperty,
    _propname: &str,
) -> (bool, bool, bool) {
    unimplemented!()
}

pub fn btbuildphasename(_phasenum: i64) -> Option<String> {
    unimplemented!()
}

pub fn _bt_truncate(
    _rel: Relation,
    _lastleft: *mut IndexTupleData,
    _firstright: *mut IndexTupleData,
    _itup_key: BTScanInsert,
) -> *mut IndexTupleData {
    unimplemented!()
}

pub fn _bt_keep_natts_fast(
    _rel: Relation,
    _lastleft: *mut IndexTupleData,
    _firstright: *mut IndexTupleData,
) -> i32 {
    unimplemented!()
}

pub fn _bt_check_natts(
    _rel: Relation,
    _heapkeyspace: bool,
    _page: &Page,
    _offnum: OffsetNumber,
) -> bool {
    unimplemented!()
}

pub fn _bt_check_third_page(
    _rel: Relation,
    _heap: Relation,
    _needheaptidspace: bool,
    _page: &Page,
    _newtup: *mut IndexTupleData,
) {
    unimplemented!()
}

pub fn _bt_allequalimage(_rel: Relation, _debugmessage: bool) -> bool {
    unimplemented!()
}

// === prototypes for functions in nbtvalidate.c (stubs) ===

pub fn btvalidate(_opclassoid: Oid) -> bool {
    unimplemented!()
}

pub fn btadjustmembers(
    _opfamilyoid: Oid,
    _opclassoid: Oid,
    _operators: Vec<OpFamilyMember>,
    _functions: Vec<OpFamilyMember>,
) {
    unimplemented!()
}

// === prototypes for functions in nbtsort.c (stubs) ===

pub fn btbuild(_heap: Relation, _index: Relation, _index_info: &mut IndexInfo) -> IndexBuildResult {
    unimplemented!()
}

pub fn _bt_parallel_build_main(_seg: &mut dsm_segment, _toc: &mut shm_toc) {
    unimplemented!()
}

/// IndexRelationGetNumberOfAttributes(rel) -- rel->rd_index->indnatts. Reaches
/// into RelationData not yet available in the skeleton.
pub fn IndexRelationGetNumberOfAttributes(_rel: Relation) -> u16 {
    unimplemented!()
}
