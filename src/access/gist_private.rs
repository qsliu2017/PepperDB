//! Translated from PostgreSQL src/include/access/gist_private.h

use crate::access::genam::{
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInfo,
    IndexOrderByDistance, IndexScanDesc, IndexUniqueCheck, IndexVacuumInfo,
};
use crate::access::amapi::{IndexAMProperty, OpFamilyMember};
use crate::access::gist::{GistNSN, GISTENTRY, GIST_SPLITVEC};
use crate::access::itup::{IndexTuple, IndexTupleData};
use crate::access::tupdesc::TupleDesc;
use crate::access::transam::FullTransactionId;
use crate::access::xlogdefs::XLogRecPtr;
use crate::c::{bytea, TransactionId};
use crate::fmgr::FmgrInfo;
use crate::pg_config_manual::INDEX_MAX_KEYS;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::buffile::BufFile;
use crate::storage::bufmgr::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK};
use crate::storage::bufpage::Page;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::utils::hsearch::HTAB;
use crate::access::htup::HeapTuple;
use crate::utils::memutils::MemoryContext;
use crate::utils::rel::Relation;

/// Maximum number of "halves" a page can be split into in one operation.
pub const GIST_MAX_SPLIT_PAGES: usize = 75;

/* Buffer lock modes */
pub const GIST_SHARE: i32 = BUFFER_LOCK_SHARE;
pub const GIST_EXCLUSIVE: i32 = BUFFER_LOCK_EXCLUSIVE;
pub const GIST_UNLOCK: i32 = BUFFER_LOCK_UNLOCK;

/// A node-buffer page spilled to a temp file during buffering build. On-disk
/// layout in the temp file: fixed header then `tupledata` FAM (accessed via
/// BUFFER_PAGE_DATA_OFFSET).
#[repr(C)]
pub struct GISTNodeBufferPage {
    pub prev: BlockNumber,
    pub freespace: u32,
    // char tupledata[FLEXIBLE_ARRAY_MEMBER] - trailing bytes in the buffer.
}

/// MAXALIGN(offsetof(GISTNodeBufferPage, tupledata)); tupledata starts after the
/// 8-byte header, MAXALIGN'd to 8.
pub const BUFFER_PAGE_DATA_OFFSET: usize = 8;

impl GISTNodeBufferPage {
    /// PAGE_FREE_SPACE
    pub fn free_space(&self) -> u32 {
        self.freespace
    }
    /// PAGE_IS_EMPTY
    pub fn is_empty(&self) -> bool {
        self.freespace as usize == crate::pg_config::BLCKSZ as usize - BUFFER_PAGE_DATA_OFFSET
    }
}
// PAGE_NO_SPACE(nbp, itup) needs IndexTupleSize(itup); a fn at the call site.

/// GISTSTATE: information needed for any GiST index operation (in-memory).
pub struct GISTSTATE {
    /// context for scan-lifespan data
    pub scan_cxt: MemoryContext,
    /// short-term context for calling functions
    pub temp_cxt: MemoryContext,

    /// index's tuple descriptor
    pub leaf_tupdesc: TupleDesc,
    /// truncated tuple descriptor for non-leaf pages
    pub non_leaf_tupdesc: TupleDesc,
    /// tuple descriptor for tuples returned in an index-only scan
    pub fetch_tupdesc: TupleDesc,

    pub consistent_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub union_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub compress_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub decompress_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub penalty_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub picksplit_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub equal_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub distance_fn: [FmgrInfo; INDEX_MAX_KEYS],
    pub fetch_fn: [FmgrInfo; INDEX_MAX_KEYS],

    /// Collations to pass to the support functions
    pub support_collation: [Oid; INDEX_MAX_KEYS],
}

/// Individual heap tuple to be visited (in-memory search queue item).
pub struct GISTSearchHeapItem {
    pub heap_ptr: ItemPointerData,
    /// T if quals must be rechecked
    pub recheck: bool,
    /// T if distances must be rechecked
    pub recheck_distances: bool,
    /// data reconstructed from the index, used in index-only scans
    pub recontup: HeapTuple,
    /// track offset in page to mark tuple as LP_DEAD
    pub offnum: OffsetNumber,
}

/// C union: index-page parent LSN vs heap-tuple info, tagged by blkno validity.
pub enum GISTSearchItemData {
    /// index page: parent page's LSN (to detect splits)
    ParentLsn(GistNSN),
    /// heap tuple info
    Heap(GISTSearchHeapItem),
}

/// Unvisited item, either index page or heap tuple (in-memory). The intrusive
/// `pairingheap_node phNode` is dropped: the arena-based PairingHeap owns links.
pub struct GISTSearchItem {
    /// index page number, or InvalidBlockNumber
    pub blkno: BlockNumber,
    pub data: GISTSearchItemData,
    /// numberOfOrderBys entries (on-disk FAM -> Vec)
    pub distances: Vec<IndexOrderByDistance>,
}

impl GISTSearchItem {
    /// GISTSearchItemIsHeap
    pub fn is_heap(&self) -> bool {
        self.blkno == crate::storage::block::INVALID_BLOCK_NUMBER
    }
}

/// GISTScanOpaqueData: private state for a scan of a GiST index (in-memory).
pub struct GISTScanOpaqueData {
    /// index information
    pub giststate: *mut GISTSTATE, // TODO(ptr)
    /// datatypes of ORDER BY expressions
    pub order_by_types: Vec<Oid>,

    /// queue of unvisited items (C: pairingheap *). The comparator type isn't
    /// known here; model as an owned queue in Phase 2.
    pub queue: *mut core::ffi::c_void, // TODO(ptr): crate::lib::pairingheap::PairingHeap
    /// context holding the queue
    pub queue_cxt: MemoryContext,
    /// false if qual can never be satisfied
    pub qual_ok: bool,
    /// true until first gistgettuple call
    pub first_call: bool,

    /// output area for gistindex_keytest
    pub distances: Vec<IndexOrderByDistance>,

    /// offset numbers of killed items (None if never used)
    pub killed_items: Option<Vec<OffsetNumber>>,
    /// number of currently stored items
    pub num_killed: i32,
    /// current number of block
    pub cur_blkno: BlockNumber,
    /// pos in the WAL stream when page was read
    pub cur_page_lsn: GistNSN,

    /// returnable heap items (cap BLCKSZ / sizeof(IndexTupleData))
    pub page_data: Vec<GISTSearchHeapItem>,
    /// number of valid items in array
    pub n_page_data: OffsetNumber,
    /// next item to return
    pub cur_page_data: OffsetNumber,
    /// context holding the fetched tuples, for index-only scans
    pub page_data_cxt: MemoryContext,
}

pub type GISTScanOpaque = *mut GISTScanOpaqueData; // TODO(ptr)

/// despite the name, gistxlogPage is not part of any xlog record (in-memory).
pub struct gistxlogPage {
    pub blkno: BlockNumber,
    /// number of index tuples following
    pub num: i32,
}

/// SplitPageLayout - gistSplit function result (in-memory linked list).
pub struct SplitPageLayout {
    pub block: gistxlogPage,
    pub list: *mut IndexTupleData, // TODO(ptr)
    pub lenlist: i32,
    /// union key for page
    pub itup: IndexTuple,
    /// page to operate on
    pub page: &'static mut Page, // TODO(ptr): Page is a borrowed slice
    /// buffer to write after all proceed
    pub buffer: Buffer,
    pub next: Option<Box<Self>>,
}

/// GISTInsertStack: locking buffers and transfer args during insertion.
pub struct GISTInsertStack {
    /// current page
    pub blkno: BlockNumber,
    pub buffer: Buffer,
    pub page: &'static mut Page, // TODO(ptr)
    /// page LSN, to recognize page update/split vs nsn
    pub lsn: GistNSN,
    /// set if we split the page during descent; need to retry from parent
    pub retry_from_parent: bool,
    /// offset of the downlink in the parent page pointing to this page
    pub downlinkoffnum: OffsetNumber,
    /// pointer to parent
    pub parent: *mut Self, // TODO(ptr)
}

/// Working state and results for multi-column split logic in gistsplit.c.
pub struct GistSplitVector {
    /// passed to/from user PickSplit method
    pub split_vector: GIST_SPLITVEC,
    /// Union of subkeys in splitVector.left
    pub spl_lattr: [Datum; INDEX_MAX_KEYS],
    pub spl_lisnull: [bool; INDEX_MAX_KEYS],
    /// Union of subkeys in splitVector.right
    pub spl_rattr: [Datum; INDEX_MAX_KEYS],
    pub spl_risnull: [bool; INDEX_MAX_KEYS],
    /// flags tuples that could go to either side for zero penalty
    pub spl_dontcare: Option<Vec<bool>>,
}

/// GISTInsertState (anonymous struct in C).
pub struct GISTInsertState {
    pub r: Relation,
    pub heap_rel: Relation,
    /// free space to be left
    pub freespace: usize,
    pub is_build: bool,
    pub stack: *mut GISTInsertStack, // TODO(ptr)
}

/// root page of a gist index
pub const GIST_ROOT_BLKNO: BlockNumber = 0;

/*
 * Invalid-tuple compatibility markers (offset numbers stored on inner tuples).
 */
pub const TUPLE_IS_VALID: u16 = 0xffff;
pub const TUPLE_IS_INVALID: u16 = 0xfffe;

// GistTupleIsInvalid / GistTupleSetValid operate on itup->tid offset; fns at
// the call site (need ItemPointer accessors).

/// A buffer attached to an internal node, used in buffering-mode build.
pub struct GISTNodeBuffer {
    /// index block # this buffer is for
    pub node_blocknum: BlockNumber,
    /// current # of blocks occupied by buffer
    pub blocks_count: i32,
    /// temporary file block #
    pub page_blocknum: BlockNumber,
    /// in-memory buffer page
    pub page_buffer: *mut GISTNodeBufferPage, // TODO(ptr)
    /// is this buffer queued for emptying?
    pub queued_for_emptying: bool,
    /// is this a temporary copy, not in the hash table?
    pub is_temp: bool,
    /// 0 == leaf
    pub level: i32,
}

// LEVEL_HAS_BUFFERS / BUFFER_HALF_FILLED / BUFFER_OVERFLOWED are small predicates
// over GISTBuildBuffers + a GISTNodeBuffer; fns at the call site.

/// General information about build buffers (in-memory).
pub struct GISTBuildBuffers {
    /// Persistent memory context for the buffers and metadata
    pub context: MemoryContext,
    /// Temporary file to store buffers in
    pub pfile: *mut BufFile, // TODO(ptr)
    /// Current size of the temporary file
    pub n_file_blocks: i64,
    /// resizable array of free blocks
    pub free_blocks: Vec<i64>,
    /// # of currently free blocks in the array
    pub n_free_blocks: i32,
    /// current allocated length of the array
    pub free_blocks_len: i32,
    /// Hash for buffers by block number
    pub node_buffers_tab: *mut HTAB, // TODO(ptr)
    /// List of buffers scheduled for emptying
    pub buffer_emptying_queue: Vec<*mut GISTNodeBuffer>, // TODO(ptr)
    /// levelStep determines which levels have buffers
    pub level_step: i32,
    /// how large each buffer is
    pub pages_per_buffer: i32,
    /// Array of lists of buffers on each level, for final emptying
    pub buffers_on_levels: Vec<Vec<*mut GISTNodeBuffer>>, // TODO(ptr)
    pub buffers_on_levels_len: i32,
    /// buffers that currently have their last page in main memory
    pub loaded_buffers: Vec<*mut GISTNodeBuffer>, // TODO(ptr)
    /// # of entries in loadedBuffers
    pub loaded_buffers_count: i32,
    /// allocated size of loadedBuffers
    pub loaded_buffers_len: i32,
    /// Level of the current root node (= height of the index tree - 1)
    pub rootlevel: i32,
}

/// GiSTOptions->buffering_mode values.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GistOptBufferingMode {
    Auto = 0,
    On,
    Off,
}

/// Storage type for GiST's reloptions (leading varlena header).
pub struct GiSTOptions {
    /// varlena header (do not touch directly!)
    pub vl_len_: i32,
    /// page fill factor in percent (0..100)
    pub fillfactor: i32,
    /// buffering build mode
    pub buffering_mode: GistOptBufferingMode,
}

/* gist.c */
pub fn gistbuildempty(_index: Relation) {
    unimplemented!()
}
pub fn gistinsert(
    _r: Relation,
    _values: &[Datum],
    _isnull: &[bool],
    _ht_ctid: &mut ItemPointerData,
    _heap_rel: Relation,
    _check_unique: IndexUniqueCheck,
    _index_unchanged: bool,
    _index_info: &mut IndexInfo,
) -> bool {
    unimplemented!()
}
pub fn create_temp_gist_context() -> MemoryContext {
    unimplemented!()
}
pub fn init_gist_state(_index: Relation) -> *mut GISTSTATE {
    unimplemented!()
}
pub fn free_gist_state(_giststate: &mut GISTSTATE) {
    unimplemented!()
}
pub fn gistdoinsert(
    _r: Relation,
    _itup: IndexTuple,
    _freespace: usize,
    _giststate: &mut GISTSTATE,
    _heap_rel: Relation,
    _is_build: bool,
) {
    unimplemented!()
}

/// A List of these is returned from gistplacetopage() in *splitinfo.
pub struct GISTPageSplitInfo {
    /// the split page "half"
    pub buf: Buffer,
    /// downlink for this half
    pub downlink: IndexTuple,
}

#[allow(clippy::too_many_arguments)]
pub fn gistplacetopage(
    _rel: Relation,
    _freespace: usize,
    _giststate: &mut GISTSTATE,
    _buffer: Buffer,
    _itup: &mut [IndexTuple],
    _ntup: i32,
    _oldoffnum: OffsetNumber,
    _newblkno: Option<&mut BlockNumber>,
    _leftchildbuf: Buffer,
    _splitinfo: &mut Vec<GISTPageSplitInfo>,
    _markfollowright: bool,
    _heap_rel: Relation,
    _is_build: bool,
) -> bool {
    unimplemented!()
}

pub fn gistSplit(
    _r: Relation,
    _page: &Page,
    _itup: &mut [IndexTuple],
    _len: i32,
    _giststate: &mut GISTSTATE,
) -> *mut SplitPageLayout {
    unimplemented!()
}

/* gistxlog.c */
pub fn gist_xlog_page_delete(
    _buffer: Buffer,
    _xid: FullTransactionId,
    _parent_buffer: Buffer,
    _downlink_offset: OffsetNumber,
) -> XLogRecPtr {
    unimplemented!()
}
pub fn gist_xlog_page_reuse(
    _rel: Relation,
    _heaprel: Relation,
    _blkno: BlockNumber,
    _delete_xid: FullTransactionId,
) {
    unimplemented!()
}
pub fn gist_xlog_update(
    _buffer: Buffer,
    _todelete: &[OffsetNumber],
    _ntodelete: i32,
    _itup: &[IndexTuple],
    _ituplen: i32,
    _leftchildbuf: Buffer,
) -> XLogRecPtr {
    unimplemented!()
}
pub fn gist_xlog_delete(
    _buffer: Buffer,
    _todelete: &[OffsetNumber],
    _ntodelete: i32,
    _snapshot_conflict_horizon: TransactionId,
    _heaprel: Relation,
) -> XLogRecPtr {
    unimplemented!()
}
pub fn gist_xlog_split(
    _page_is_leaf: bool,
    _dist: *mut SplitPageLayout,
    _origrlink: BlockNumber,
    _orignsn: GistNSN,
    _leftchildbuf: Buffer,
    _markfollowright: bool,
) -> XLogRecPtr {
    unimplemented!()
}
pub fn gist_xlog_assign_lsn() -> XLogRecPtr {
    unimplemented!()
}

/* gistget.c */
pub fn gistgettuple(_scan: IndexScanDesc, _dir: crate::access::sdir::ScanDirection) -> bool {
    unimplemented!()
}
pub fn gistgetbitmap(_scan: IndexScanDesc, _tbm: &mut crate::nodes::tidbitmap::TIDBitmap) -> i64 {
    unimplemented!()
}
pub fn gistcanreturn(_index: Relation, _attno: i32) -> bool {
    unimplemented!()
}

/* gistvalidate.c */
pub fn gistvalidate(_opclassoid: Oid) -> bool {
    unimplemented!()
}
pub fn gistadjustmembers(
    _opfamilyoid: Oid,
    _opclassoid: Oid,
    _operators: &mut [OpFamilyMember],
    _functions: &mut [OpFamilyMember],
) {
    unimplemented!()
}

/* gistutil.c */

/// GiSTPageSize = BLCKSZ - SizeOfPageHeaderData - MAXALIGN(sizeof(GISTPageOpaqueData)).
pub const GIST_MIN_FILLFACTOR: i32 = 10;
pub const GIST_DEFAULT_FILLFACTOR: i32 = 90;

pub fn gistoptions(_reloptions: Datum, _validate: bool) -> *mut bytea {
    unimplemented!()
}
pub fn gistproperty(
    _index_oid: Oid,
    _attno: i32,
    _prop: IndexAMProperty,
    _propname: &str,
) -> Option<(bool, bool)> {
    // C out-params (*res, *isnull) -> (res, isnull); None on "not handled".
    unimplemented!()
}
pub fn gistfitpage(_itvec: &[IndexTuple], _len: i32) -> bool {
    unimplemented!()
}
pub fn gistnospace(
    _page: &Page,
    _itvec: &[IndexTuple],
    _len: i32,
    _todelete: OffsetNumber,
    _freespace: usize,
) -> bool {
    unimplemented!()
}
pub fn gistcheckpage(_rel: Relation, _buf: Buffer) {
    unimplemented!()
}
pub fn gist_new_buffer(_r: Relation, _heaprel: Relation) -> Buffer {
    unimplemented!()
}
pub fn gist_page_recyclable(_page: &Page) -> bool {
    unimplemented!()
}
pub fn gistfillbuffer(_page: &mut Page, _itup: &[IndexTuple], _len: i32, _off: OffsetNumber) {
    unimplemented!()
}
pub fn gistextractpage(_page: &Page) -> Vec<IndexTuple> {
    // C: returns array + *len out-param -> Vec.
    unimplemented!()
}
pub fn gistjoinvector(
    _itvec: &mut [IndexTuple],
    _len: &mut i32,
    _additvec: &[IndexTuple],
    _addlen: i32,
) -> Vec<IndexTuple> {
    unimplemented!()
}
pub fn gistfillitupvec(_vec: &[IndexTuple], _veclen: i32) -> (*mut IndexTupleData, i32) {
    // C returns the buffer and writes *memlen.
    unimplemented!()
}

pub fn gistunion(
    _r: Relation,
    _itvec: &[IndexTuple],
    _len: i32,
    _giststate: &mut GISTSTATE,
) -> IndexTuple {
    unimplemented!()
}
pub fn gistgetadjusted(
    _r: Relation,
    _oldtup: IndexTuple,
    _addtup: IndexTuple,
    _giststate: &mut GISTSTATE,
) -> IndexTuple {
    unimplemented!()
}
pub fn gist_form_tuple(
    _giststate: &mut GISTSTATE,
    _r: Relation,
    _attdata: &[Datum],
    _isnull: &[bool],
    _isleaf: bool,
) -> IndexTuple {
    unimplemented!()
}
pub fn gist_compress_values(
    _giststate: &mut GISTSTATE,
    _r: Relation,
    _attdata: &[Datum],
    _isnull: &[bool],
    _isleaf: bool,
    _compatt: &mut [Datum],
) {
    unimplemented!()
}

pub fn gistchoose(
    _r: Relation,
    _p: &Page,
    _it: IndexTuple,
    _giststate: &mut GISTSTATE,
) -> OffsetNumber {
    unimplemented!()
}

pub fn gist_init_buffer(_b: Buffer, _f: u32) {
    unimplemented!()
}
pub fn gistinitpage(_page: &mut Page, _f: u32) {
    unimplemented!()
}
#[allow(clippy::too_many_arguments)]
pub fn gistdentryinit(
    _giststate: &mut GISTSTATE,
    _nkey: i32,
    _e: &mut GISTENTRY,
    _k: Datum,
    _r: Relation,
    _pg: &Page,
    _o: OffsetNumber,
    _l: bool,
    _is_null: bool,
) {
    unimplemented!()
}

pub fn gistpenalty(
    _giststate: &mut GISTSTATE,
    _attno: i32,
    _orig: &mut GISTENTRY,
    _is_null_orig: bool,
    _add: &mut GISTENTRY,
    _is_null_add: bool,
) -> f32 {
    unimplemented!()
}
pub fn gist_make_union_it_vec(
    _giststate: &mut GISTSTATE,
    _itvec: &[IndexTuple],
    _len: i32,
    _attr: &mut [Datum],
    _isnull: &mut [bool],
) {
    unimplemented!()
}
pub fn gist_key_is_eq(_giststate: &mut GISTSTATE, _attno: i32, _a: Datum, _b: Datum) -> bool {
    unimplemented!()
}
#[allow(clippy::too_many_arguments)]
pub fn gist_decompress_att(
    _giststate: &mut GISTSTATE,
    _r: Relation,
    _tuple: IndexTuple,
    _p: &Page,
    _o: OffsetNumber,
    _attdata: &mut GISTENTRY,
    _isnull: &mut [bool],
) {
    unimplemented!()
}
pub fn gist_fetch_tuple(_giststate: &mut GISTSTATE, _r: Relation, _tuple: IndexTuple) -> HeapTuple {
    unimplemented!()
}
#[allow(clippy::too_many_arguments)]
pub fn gist_make_union_key(
    _giststate: &mut GISTSTATE,
    _attno: i32,
    _entry1: &mut GISTENTRY,
    _isnull1: bool,
    _entry2: &mut GISTENTRY,
    _isnull2: bool,
    _dst: &mut [Datum],
    _dstisnull: &mut [bool],
) {
    unimplemented!()
}

pub fn gist_get_fake_lsn(_rel: Relation) -> XLogRecPtr {
    unimplemented!()
}

/* gistvacuum.c */
pub fn gistbulkdelete(
    _info: &mut IndexVacuumInfo,
    _stats: *mut IndexBulkDeleteResult,
    _callback: &mut IndexBulkDeleteCallback,
    _callback_state: *mut core::ffi::c_void,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}
pub fn gistvacuumcleanup(
    _info: &mut IndexVacuumInfo,
    _stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}

/* gistsplit.c */
pub fn gist_split_by_key(
    _r: Relation,
    _page: &Page,
    _itup: &mut [IndexTuple],
    _len: i32,
    _giststate: &mut GISTSTATE,
    _v: &mut GistSplitVector,
    _attno: i32,
) {
    unimplemented!()
}

/* gistbuild.c */
pub fn gistbuild(
    _heap: Relation,
    _index: Relation,
    _index_info: &mut IndexInfo,
) -> *mut IndexBuildResult {
    unimplemented!()
}

/* gistbuildbuffers.c */
pub fn gist_init_build_buffers(
    _pages_per_buffer: i32,
    _level_step: i32,
    _max_level: i32,
) -> *mut GISTBuildBuffers {
    unimplemented!()
}
pub fn gist_get_node_buffer(
    _gfbb: &mut GISTBuildBuffers,
    _giststate: &mut GISTSTATE,
    _node_blocknum: BlockNumber,
    _level: i32,
) -> *mut GISTNodeBuffer {
    unimplemented!()
}
pub fn gist_push_itup_to_node_buffer(
    _gfbb: &mut GISTBuildBuffers,
    _node_buffer: &mut GISTNodeBuffer,
    _itup: IndexTuple,
) {
    unimplemented!()
}
pub fn gist_pop_itup_from_node_buffer(
    _gfbb: &mut GISTBuildBuffers,
    _node_buffer: &mut GISTNodeBuffer,
) -> Option<IndexTuple> {
    // C: bool return + *itup out-param -> Option.
    unimplemented!()
}
pub fn gist_free_build_buffers(_gfbb: &mut GISTBuildBuffers) {
    unimplemented!()
}
pub fn gist_relocate_build_buffers_on_split(
    _gfbb: &mut GISTBuildBuffers,
    _giststate: &mut GISTSTATE,
    _r: Relation,
    _level: i32,
    _buffer: Buffer,
    _splitinfo: &[GISTPageSplitInfo],
) {
    unimplemented!()
}
pub fn gist_unload_node_buffers(_gfbb: &mut GISTBuildBuffers) {
    unimplemented!()
}
