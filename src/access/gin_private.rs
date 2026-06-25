//! Translated from PostgreSQL src/include/access/gin_private.h

use bitflags::bitflags;

use crate::access::gin::{GinStatsData, GinTernaryValue};
use crate::access::ginblock::{GinNullCategory, GinPostingList, PostingItem};
use crate::access::genam::{
    IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexScanDesc, IndexUniqueCheck,
    IndexVacuumInfo, IndexBuildResult,
};
use crate::access::itup::IndexTuple;
use crate::access::skey::ScanKey;
use crate::access::stratnum::StrategyNumber;
use crate::common::int::pg_cmp_u64;
use crate::fmgr::FmgrInfo;
use crate::nodes::execnodes::IndexInfo;
use crate::nodes::tidbitmap::{TBMIterateResult, TBMPrivateIterator, TIDBitmap, TBM_MAX_TUPLES_PER_PAGE};
use crate::pg_config_manual::INDEX_MAX_KEYS;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufmgr::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK};
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::access::tupdesc::TupleDesc;
use crate::c::{Pointer, Size};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::rel::Relation;

// C: `typedef ItemPointerData *ItemPointer`. itemptr.rs aliases only the value
// struct; define the pointer handle locally.
pub type ItemPointer = *mut ItemPointerData; // TODO(ptr)

// Page is `bufpage::Page<'a> = &'a [u8]`; in raw fn-pointer/handle positions we
// use `*mut u8` (cf. gist_private's `page: *mut u8 // Page`). TODO(ptr).
type Page = *mut u8;

/// Storage type for GIN's reloptions. vl_len_ is the varlena header (on-disk
/// reloptions blob), but the struct is used in-memory after detoasting.
pub struct GinOptions {
    pub vl_len_: i32,                // varlena header (do not touch directly!)
    pub use_fast_update: bool,       // use fast updates?
    pub pending_list_cleanup_size: i32, // maximum size of pending list
}

pub const GIN_DEFAULT_USE_FASTUPDATE: bool = true;

// GinGetUseFastUpdate / GinGetPendingListCleanupSize are accessor macros over a
// relation's rd_options; they become methods on Relation in Phase 2. Omitted
// here (they reference rd_options/gin_pending_list_limit not yet available).

// Buffer lock/unlock operation codes (aliases of bufmgr's BUFFER_LOCK_*).
pub const GIN_UNLOCK: i32 = BUFFER_LOCK_UNLOCK;
pub const GIN_SHARE: i32 = BUFFER_LOCK_SHARE;
pub const GIN_EXCLUSIVE: i32 = BUFFER_LOCK_EXCLUSIVE;

/// Working data structure describing the index being worked on. In-memory.
pub struct GinState {
    pub index: Relation,
    pub one_col: bool, // true if single-column index

    /// Nominal tuple descriptor of the index (key types per column).
    pub orig_tupdesc: TupleDesc,
    pub tupdesc: [TupleDesc; INDEX_MAX_KEYS],

    // Per-index-column opclass support functions
    pub compare_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub extract_value_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub extract_query_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub consistent_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub tri_consistent_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub compare_partial_fn: [FmgrInfo; INDEX_MAX_KEYS], // optional method
    /// canPartialMatch[i] is true if comparePartialFn[i] is valid.
    pub can_partial_match: [bool; INDEX_MAX_KEYS],
    /// Collations to pass to the support functions.
    pub support_collation: [Oid; INDEX_MAX_KEYS],
}

// ginutil.c

/// reloptions blob; None if no options.
pub fn ginoptions(_reloptions: Datum, _validate: bool) -> Option<Vec<u8>> {
    unimplemented!()
}

pub fn initGinState(_state: &mut GinState, _index: Relation) {
    unimplemented!()
}

pub fn GinNewBuffer(_index: Relation) -> Buffer {
    unimplemented!()
}

pub fn GinInitBuffer(_b: Buffer, _f: u32) {
    unimplemented!()
}

pub fn GinInitPage(_page: Page, _f: u32, _page_size: Size) {
    unimplemented!()
}

pub fn GinInitMetabuffer(_b: Buffer) {
    unimplemented!()
}

pub fn ginCompareEntries(_ginstate: &GinState, _attnum: OffsetNumber,
                         _a: Datum, _categorya: GinNullCategory,
                         _b: Datum, _categoryb: GinNullCategory) -> i32 {
    unimplemented!()
}

pub fn ginCompareAttEntries(_ginstate: &GinState,
                            _attnuma: OffsetNumber, _a: Datum, _categorya: GinNullCategory,
                            _attnumb: OffsetNumber, _b: Datum, _categoryb: GinNullCategory) -> i32 {
    unimplemented!()
}

/// Extracted entries plus their null categories (was Datum* + nentries +
/// GinNullCategory** out-params).
pub fn ginExtractEntries(_ginstate: &GinState, _attnum: OffsetNumber,
                         _value: Datum, _is_null: bool) -> (Vec<Datum>, Vec<GinNullCategory>) {
    unimplemented!()
}

pub fn gintuple_get_attrnum(_ginstate: &GinState, _tuple: IndexTuple) -> OffsetNumber {
    unimplemented!()
}

/// Key datum plus its null category (was GinNullCategory* out-param).
pub fn gintuple_get_key(_ginstate: &GinState, _tuple: IndexTuple) -> (Datum, GinNullCategory) {
    unimplemented!()
}

pub fn ginbuildphasename(_phasenum: i64) -> String {
    unimplemented!()
}

// gininsert.c

pub fn ginbuild(_heap: Relation, _index: Relation, _index_info: &mut IndexInfo)
    -> *mut IndexBuildResult {
    unimplemented!() // TODO(ptr)
}

pub fn ginbuildempty(_index: Relation) {
    unimplemented!()
}

pub fn gininsert(_index: Relation, _values: &[Datum], _isnull: &[bool],
                 _ht_ctid: ItemPointer, _heap_rel: Relation,
                 _check_unique: IndexUniqueCheck, _index_unchanged: bool,
                 _index_info: &mut IndexInfo) -> bool {
    unimplemented!()
}

pub fn ginEntryInsert(_ginstate: &mut GinState, _attnum: OffsetNumber, _key: Datum,
                      _category: GinNullCategory, _items: &mut [ItemPointerData],
                      _nitem: u32, _build_stats: &mut GinStatsData) {
    unimplemented!()
}

// ginbtree.c

/// One level of a GIN btree descent stack.
pub struct GinBtreeStack {
    pub blkno: BlockNumber,
    pub buffer: Buffer,
    pub off: OffsetNumber,
    pub iptr: ItemPointerData,
    /// Predicted number of pages on current level.
    pub predict_number: u32,
    pub parent: *mut GinBtreeStack, // TODO(ptr)
}

pub type GinBtree = *mut GinBtreeData; // TODO(ptr)

/// Return codes for GinBtreeData.beginPlaceToPage. POOR (sequential ordinal) -> enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GinPlaceToPageRC {
    NoWork,
    Insert,
    Split,
}

/// GIN btree behaviour vtable. routine-struct: a struct of fn pointers, but the
/// search/insert methods pass an opaque `void *` insertion payload between calls
/// (`GinBtreeEntryInsertData` for entry trees, `GinBtreeDataLeafInsertData` /
/// `PostingItem` for data trees). That payload polymorphism resists a clean
/// trait in the skeleton, so keep the fn-pointer table faithfully. void* -> Pointer.
pub struct GinBtreeData {
    // search methods
    pub find_child_page: Option<fn(GinBtree, *mut GinBtreeStack) -> BlockNumber>,
    pub get_left_most_child: Option<fn(GinBtree, Page) -> BlockNumber>,
    pub is_move_right: Option<fn(GinBtree, Page) -> bool>,
    pub find_item: Option<fn(GinBtree, *mut GinBtreeStack) -> bool>,

    // insert methods
    pub find_child_ptr: Option<fn(GinBtree, Page, BlockNumber, OffsetNumber) -> OffsetNumber>,
    pub begin_place_to_page: Option<
        fn(GinBtree, Buffer, *mut GinBtreeStack, Pointer, BlockNumber,
           *mut Pointer, *mut Page, *mut Page) -> GinPlaceToPageRC>,
    pub exec_place_to_page:
        Option<fn(GinBtree, Buffer, *mut GinBtreeStack, Pointer, BlockNumber, Pointer)>,
    pub prepare_downlink: Option<fn(GinBtree, Buffer) -> Pointer>,
    pub fill_root: Option<fn(GinBtree, Page, BlockNumber, Page, BlockNumber, Page)>,

    pub is_data: bool,

    pub index: Relation,
    pub root_blkno: BlockNumber,
    pub ginstate: *mut GinState, // not valid in a data scan; TODO(ptr)
    pub full_scan: bool,
    pub is_build: bool,

    // Search key for Entry tree
    pub entry_attnum: OffsetNumber,
    pub entry_key: Datum,
    pub entry_category: GinNullCategory,

    // Search key for data tree (posting tree)
    pub itemptr: ItemPointerData,
}

/// A tuple to be inserted into the entry tree (entry-tree insert payload).
pub struct GinBtreeEntryInsertData {
    pub entry: IndexTuple,  // tuple to insert
    pub is_delete: bool,    // delete old tuple at same offset?
}

/// Itempointer(s) to be inserted into a data (posting tree) leaf page.
pub struct GinBtreeDataLeafInsertData {
    pub items: *mut ItemPointerData, // TODO(ptr)
    pub nitem: u32,
    pub curitem: u32,
}

// For internal data (posting tree) pages, the insertion payload is a PostingItem
// (imported from ginblock).

pub fn ginFindLeafPage(_btree: GinBtree, _search_mode: bool, _root_conflict_check: bool)
    -> *mut GinBtreeStack {
    unimplemented!() // TODO(ptr)
}

pub fn ginStepRight(_buffer: Buffer, _index: Relation, _lockmode: i32) -> Buffer {
    unimplemented!()
}

pub fn freeGinBtreeStack(_stack: *mut GinBtreeStack) {
    unimplemented!()
}

pub fn ginInsertValue(_btree: GinBtree, _stack: *mut GinBtreeStack, _insertdata: Pointer,
                      _build_stats: &mut GinStatsData) {
    unimplemented!()
}

// ginentrypage.c

pub fn GinFormTuple(_ginstate: &GinState, _attnum: OffsetNumber, _key: Datum,
                    _category: GinNullCategory, _data: Pointer, _data_size: Size,
                    _nipd: i32, _error_too_big: bool) -> IndexTuple {
    unimplemented!()
}

pub fn ginPrepareEntryScan(_btree: GinBtree, _attnum: OffsetNumber, _key: Datum,
                           _category: GinNullCategory, _ginstate: &GinState) {
    unimplemented!()
}

pub fn ginEntryFillRoot(_btree: GinBtree, _root: Page, _lblkno: BlockNumber, _lpage: Page,
                        _rblkno: BlockNumber, _rpage: Page) {
    unimplemented!()
}

/// Item pointers from a tuple (was int* nitems out-param).
pub fn ginReadTuple(_ginstate: &GinState, _attnum: OffsetNumber, _itup: IndexTuple)
    -> Vec<ItemPointerData> {
    unimplemented!()
}

// gindatapage.c

pub fn GinDataLeafPageGetItems(_page: Page, _advance_past: ItemPointerData)
    -> Vec<ItemPointerData> {
    unimplemented!()
}

pub fn GinDataLeafPageGetItemsToTbm(_page: Page, _tbm: &mut TIDBitmap) -> i32 {
    unimplemented!()
}

pub fn createPostingTree(_index: Relation, _items: &mut [ItemPointerData], _nitems: u32,
                         _build_stats: &mut GinStatsData, _entrybuffer: Buffer) -> BlockNumber {
    unimplemented!()
}

pub fn GinDataPageAddPostingItem(_page: Page, _data: &PostingItem, _offset: OffsetNumber) {
    unimplemented!()
}

pub fn GinPageDeletePostingItem(_page: Page, _offset: OffsetNumber) {
    unimplemented!()
}

pub fn ginInsertItemPointers(_index: Relation, _root_blkno: BlockNumber,
                             _items: &mut [ItemPointerData], _nitem: u32,
                             _build_stats: &mut GinStatsData) {
    unimplemented!()
}

pub fn ginScanBeginPostingTree(_btree: GinBtree, _index: Relation, _root_blkno: BlockNumber)
    -> *mut GinBtreeStack {
    unimplemented!() // TODO(ptr)
}

pub fn ginDataFillRoot(_btree: GinBtree, _root: Page, _lblkno: BlockNumber, _lpage: Page,
                       _rblkno: BlockNumber, _rpage: Page) {
    unimplemented!()
}

/// Opaque; private vacuum state defined in ginvacuum.c, not ported.
pub struct GinVacuumState;

pub fn ginVacuumPostingTreeLeaf(_indexrel: Relation, _buffer: Buffer, _gvs: &mut GinVacuumState) {
    unimplemented!()
}

// ginscan.c

pub type GinScanKey = *mut GinScanKeyData; // TODO(ptr)
pub type GinScanEntry = *mut GinScanEntryData; // TODO(ptr)

/// A single GIN index qualifier expression. In-memory.
pub struct GinScanKeyData {
    /// Real number of entries in scanEntry[] (always > 0).
    pub nentries: u32,
    /// Number of entries extractQueryFn and consistentFn know about.
    pub nuserentries: u32,

    /// One GinScanEntry per extracted search condition.
    pub scan_entry: *mut GinScanEntry, // TODO(ptr)

    pub required_entries: *mut GinScanEntry, // TODO(ptr)
    pub nrequired: i32,
    pub additional_entries: *mut GinScanEntry, // TODO(ptr)
    pub nadditional: i32,

    /// Check flags reported to consistentFn.
    pub entry_res: *mut GinTernaryValue, // TODO(ptr)
    pub bool_consistent_fn: Option<fn(GinScanKey) -> bool>,
    pub tri_consistent_fn: Option<fn(GinScanKey) -> GinTernaryValue>,
    pub consistent_fmgr_info: *mut FmgrInfo, // TODO(ptr)
    pub tri_consistent_fmgr_info: *mut FmgrInfo, // TODO(ptr)
    pub collation: Oid,

    // other data needed for calling consistentFn
    pub query: Datum,
    // NB: these three arrays have only nuserentries elements!
    pub query_values: *mut Datum,             // TODO(ptr)
    pub query_categories: *mut GinNullCategory, // TODO(ptr)
    pub extra_data: *mut Pointer,             // TODO(ptr)
    pub strategy: StrategyNumber,
    pub search_mode: i32,
    pub attnum: OffsetNumber,

    /// An excludeOnly scan key cannot enumerate all matching tuples on its own.
    pub exclude_only: bool,

    // Match status data.
    pub cur_item: ItemPointerData,
    pub cur_item_matches: bool,
    pub recheck_cur_item: bool,
    pub is_finished: bool,
}

/// A specific GIN search condition. In-memory.
pub struct GinScanEntryData {
    // query key and other information from extractQueryFn
    pub query_key: Datum,
    pub query_category: GinNullCategory,
    pub is_partial_match: bool,
    pub extra_data: Pointer, // TODO(ptr)
    pub strategy: StrategyNumber,
    pub search_mode: i32,
    pub attnum: OffsetNumber,

    // Current page in posting tree
    pub buffer: Buffer,

    // current ItemPointer to heap
    pub cur_item: ItemPointerData,

    // for a partial-match or full-scan query, we accumulate all TIDs here
    pub match_bitmap: *mut TIDBitmap,           // TODO(ptr)
    pub match_iterator: *mut TBMPrivateIterator, // TODO(ptr)

    pub match_result: TBMIterateResult,
    pub match_offsets: [OffsetNumber; TBM_MAX_TUPLES_PER_PAGE as usize],
    pub match_ntuples: i32,

    // used for Posting list and one page in Posting tree
    pub list: *mut ItemPointerData, // TODO(ptr)
    pub nlist: i32,
    pub offset: OffsetNumber,

    pub is_finished: bool,
    pub reduce_result: bool,
    pub predict_number_result: u32,
    pub btree: GinBtreeData,
}

/// Per-scan opaque state for a GIN scan. In-memory.
pub struct GinScanOpaqueData {
    pub temp_ctx: crate::utils::palloc::MemoryContext,
    pub ginstate: GinState,

    pub keys: GinScanKey, // one per scan qualifier expr
    pub nkeys: u32,

    pub entries: *mut GinScanEntry, // one per index search condition; TODO(ptr)
    pub totalentries: u32,
    pub allocentries: u32, // allocated length of entries[]

    pub key_ctx: crate::utils::palloc::MemoryContext, // holds key and entry data

    pub is_void_res: bool, // true if query is unsatisfiable
}

pub type GinScanOpaque = *mut GinScanOpaqueData; // TODO(ptr)

pub fn ginbeginscan(_rel: Relation, _nkeys: i32, _norderbys: i32) -> IndexScanDesc {
    unimplemented!()
}

pub fn ginendscan(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn ginrescan(_scan: IndexScanDesc, _scankey: ScanKey, _nscankeys: i32,
                 _orderbys: ScanKey, _norderbys: i32) {
    unimplemented!()
}

pub fn ginNewScanKey(_scan: IndexScanDesc) {
    unimplemented!()
}

pub fn ginFreeScanKeys(_so: GinScanOpaque) {
    unimplemented!()
}

// ginget.c

pub fn gingetbitmap(_scan: IndexScanDesc, _tbm: &mut TIDBitmap) -> i64 {
    unimplemented!()
}

// ginlogic.c

pub fn ginInitConsistentFunction(_ginstate: &GinState, _key: GinScanKey) {
    unimplemented!()
}

// ginvacuum.c

pub fn ginbulkdelete(_info: &IndexVacuumInfo, _stats: Option<IndexBulkDeleteResult>,
                     _callback: &mut IndexBulkDeleteCallback) -> *mut IndexBulkDeleteResult {
    unimplemented!() // TODO(ptr)
}

pub fn ginvacuumcleanup(_info: &IndexVacuumInfo, _stats: Option<IndexBulkDeleteResult>)
    -> *mut IndexBulkDeleteResult {
    unimplemented!() // TODO(ptr)
}

/// Remaining items (was int* nremaining out-param).
pub fn ginVacuumItemPointers(_gvs: &mut GinVacuumState, _items: &mut [ItemPointerData],
                             _nitem: i32) -> Vec<ItemPointerData> {
    unimplemented!()
}

// ginvalidate.c

pub fn ginvalidate(_opclassoid: Oid) -> bool {
    unimplemented!()
}

pub fn ginadjustmembers(_opfamilyoid: Oid, _opclassoid: Oid,
                        _operators: Vec<crate::nodes::nodes::Node>,
                        _functions: Vec<crate::nodes::nodes::Node>) {
    unimplemented!()
}

// ginbulk.c

// Opaque; lib/rbtree.h is tombstoned (-> BTreeMap), accumulator not yet reworked.
pub struct RBTNode;
pub struct RBTree;
pub struct RBTreeIterator;

pub struct GinEntryAccumulator {
    pub rbtnode: RBTNode,
    pub key: Datum,
    pub category: GinNullCategory,
    pub attnum: OffsetNumber,
    pub should_sort: bool,
    pub list: *mut ItemPointerData, // TODO(ptr)
    pub maxcount: u32, // allocated size of list[]
    pub count: u32,    // current number of list[] entries
}

pub struct BuildAccumulator {
    pub ginstate: *mut GinState, // TODO(ptr)
    pub allocated_memory: Size,
    pub entryallocator: *mut GinEntryAccumulator, // TODO(ptr)
    pub eas_used: u32,
    pub tree: *mut RBTree,            // TODO(ptr)
    pub tree_walk: RBTreeIterator,
}

pub fn ginInitBA(_accum: &mut BuildAccumulator) {
    unimplemented!()
}

pub fn ginInsertBAEntries(_accum: &mut BuildAccumulator, _heapptr: ItemPointer,
                          _attnum: OffsetNumber, _entries: &[Datum],
                          _categories: &[GinNullCategory], _nentries: i32) {
    unimplemented!()
}

pub fn ginBeginBAScan(_accum: &mut BuildAccumulator) {
    unimplemented!()
}

/// Next accumulator entry: TID list plus (attnum, key, category, n) outputs;
/// None when the scan is exhausted.
pub fn ginGetBAEntry(_accum: &mut BuildAccumulator)
    -> Option<(Vec<ItemPointerData>, OffsetNumber, Datum, GinNullCategory, u32)> {
    unimplemented!()
}

// ginfast.c

pub struct GinTupleCollector {
    pub tuples: *mut IndexTuple, // TODO(ptr)
    pub ntuples: u32,
    pub lentuples: u32,
    pub sumsize: u32,
}

pub fn ginHeapTupleFastInsert(_ginstate: &GinState, _collector: &mut GinTupleCollector) {
    unimplemented!()
}

pub fn ginHeapTupleFastCollect(_ginstate: &GinState, _collector: &mut GinTupleCollector,
                               _attnum: OffsetNumber, _value: Datum, _is_null: bool,
                               _ht_ctid: ItemPointer) {
    unimplemented!()
}

pub fn ginInsertCleanup(_ginstate: &GinState, _full_clean: bool, _fill_fsm: bool,
                        _force_cleanup: bool, _stats: &mut IndexBulkDeleteResult) {
    unimplemented!()
}

// ginpostinglist.c

/// Compressed posting list plus the number of bytes written (was int* nwritten).
pub fn ginCompressPostingList(_ipd: ItemPointer, _nipd: i32, _maxsize: i32)
    -> (*mut GinPostingList, i32) {
    unimplemented!() // TODO(ptr)
}

pub fn ginPostingListDecodeAllSegmentsToTbm(_ptr: *mut GinPostingList, _len: i32,
                                            _tbm: &mut TIDBitmap) -> i32 {
    unimplemented!()
}

/// Decoded item pointers (was int* ndecoded_out).
pub fn ginPostingListDecodeAllSegments(_segment: *mut GinPostingList, _len: i32)
    -> Vec<ItemPointerData> {
    unimplemented!()
}

/// Decoded item pointers (was int* ndecoded_out).
pub fn ginPostingListDecode(_plist: *mut GinPostingList) -> Vec<ItemPointerData> {
    unimplemented!()
}

/// Merged item pointers (was int* nmerged out-param).
pub fn ginMergeItemPointers(_a: &[ItemPointerData], _na: u32,
                            _b: &[ItemPointerData], _nb: u32) -> Vec<ItemPointerData> {
    unimplemented!()
}

/// Compares item pointers; inlined for hot merge loops.
pub fn ginCompareItemPointers(a: &ItemPointerData, b: &ItemPointerData) -> i32 {
    let ia = (a.block_number() as u64) << 32 | a.offset_number() as u64;
    let ib = (b.block_number() as u64) << 32 | b.offset_number() as u64;
    pg_cmp_u64(ia, ib)
}

pub fn ginTraverseLock(_buffer: Buffer, _search_mode: bool) -> i32 {
    unimplemented!()
}
