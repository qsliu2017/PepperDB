//! Translated from PostgreSQL src/include/access/hash.h

use bitflags::bitflags;

use crate::access::genam::{
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInfo, IndexScanDesc,
    IndexUniqueCheck, IndexVacuumInfo,
};
use crate::access::amapi::OpFamilyMember;
use crate::access::cmptype::CompareType;
use crate::access::itup::{IndexTuple, MaxIndexTuplesPerPage};
use crate::access::sdir::ScanDirection;
use crate::access::skey::ScanKey;
use crate::access::stratnum::StrategyNumber;
use crate::c::{bytea, RegProcedure};
use crate::common::relpath::ForkNumber;
use crate::nodes::tidbitmap::TIDBitmap;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::buf::{Buffer, BufferAccessStrategy};
use crate::storage::bufmgr::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE};
use crate::storage::bufpage::Page;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::utils::rel::Relation;

/// Mapping from hash bucket number to physical block number of bucket's
/// starting page.
pub type Bucket = u32;

pub const InvalidBucket: Bucket = 0xFFFF_FFFF;

// BUCKET_TO_BLKNO(metap, B) needs metapage state + _hash_spareindex; a fn that
// takes the metapage. Body stubbed (depends on _hash_spareindex).
pub fn bucket_to_blkno(_metap: &HashMetaPageData, _b: Bucket) -> BlockNumber {
    unimplemented!()
}

bitflags! {
    /// flag page-type code + flag bits. The page-type bits (LH_PAGE_TYPE)
    /// are distinct single bits so they can be OR'd as an allowable-types mask;
    /// callers must still ensure exactly one page-type bit is set on a real page.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct LhFlags: u16 {
        const OVERFLOW_PAGE             = 1 << 0;
        const BUCKET_PAGE              = 1 << 1;
        const BITMAP_PAGE             = 1 << 2;
        const META_PAGE              = 1 << 3;
        const BUCKET_BEING_POPULATED  = 1 << 4;
        const BUCKET_BEING_SPLIT      = 1 << 5;
        const BUCKET_NEEDS_SPLIT_CLEANUP = 1 << 6;
        const PAGE_HAS_DEAD_TUPLES    = 1 << 7;
        /// LH_PAGE_TYPE: mask of the four page-type bits.
        const PAGE_TYPE = Self::OVERFLOW_PAGE.bits()
            | Self::BUCKET_PAGE.bits()
            | Self::BITMAP_PAGE.bits()
            | Self::META_PAGE.bits();
    }
}

impl LhFlags {
    /// LH_UNUSED_PAGE (0).
    pub const UNUSED_PAGE: Self = Self::empty();
}

/// Special space for hash index pages (ON-DISK page special area).
#[repr(C)]
pub struct HashPageOpaqueData {
    /// see header notes
    pub prevblkno: BlockNumber,
    /// next page in bucket chain, or InvalidBlockNumber
    pub nextblkno: BlockNumber,
    /// bucket number this page belongs to
    pub bucket: Bucket,
    /// page type code + flag bits (raw word; use LhFlags accessors)
    pub flag: u16,
    /// for identification of hash indexes
    pub page_id: u16,
}
const _: () = assert!(core::mem::size_of::<HashPageOpaqueData>() == 16);
const _: () = assert!(core::mem::offset_of!(HashPageOpaqueData, flag) == 12);

pub type HashPageOpaque = *mut HashPageOpaqueData; // TODO(ptr)

impl HashPageOpaqueData {
    pub fn flags(&self) -> LhFlags {
        LhFlags::from_bits_retain(self.flag)
    }
    pub fn needs_split_cleanup(&self) -> bool {
        self.flags().contains(LhFlags::BUCKET_NEEDS_SPLIT_CLEANUP)
    }
    pub fn bucket_being_split(&self) -> bool {
        self.flags().contains(LhFlags::BUCKET_BEING_SPLIT)
    }
    pub fn bucket_being_populated(&self) -> bool {
        self.flags().contains(LhFlags::BUCKET_BEING_POPULATED)
    }
    pub fn has_dead_tuples(&self) -> bool {
        self.flags().contains(LhFlags::PAGE_HAS_DEAD_TUPLES)
    }
}

/// page ID; should be the last 2 bytes on the page.
pub const HASHO_PAGE_ID: u16 = 0xFF80;

/// what we remember about each match (in-memory scan state).
pub struct HashScanPosItem {
    /// TID of referenced heap item
    pub heap_tid: ItemPointerData,
    /// index item's location within page
    pub index_offset: OffsetNumber,
}

/// in-memory scan position. C uses a fixed items[MaxIndexTuplesPerPage] FAM-ish
/// tail; in-memory state -> Vec.
pub struct HashScanPosData {
    /// if valid, the buffer is pinned
    pub buf: Buffer,
    /// current hash index page
    pub curr_page: BlockNumber,
    /// next overflow page
    pub next_page: BlockNumber,
    /// prev overflow or bucket page
    pub prev_page: BlockNumber,
    /// first valid index in items[]
    pub first_item: i32,
    /// last valid index in items[]
    pub last_item: i32,
    /// current index in items[]
    pub item_index: i32,
    /// matches in index order; cap is MaxIndexTuplesPerPage
    pub items: Vec<HashScanPosItem>,
}

/// HashScanOpaqueData is private state for a hash index scan (in-memory).
pub struct HashScanOpaqueData {
    /// Hash value of the scan key
    pub hashso_sk_hash: u32,
    /// buffer associated with primary bucket
    pub hashso_bucket_buf: Buffer,
    /// buffer of primary bucket page of bucket being split
    pub hashso_split_bucket_buf: Buffer,
    /// scan starts on bucket being populated due to split
    pub hashso_buc_populated: bool,
    /// scanning bucket being split (only meaningful when populated is true)
    pub hashso_buc_split: bool,
    /// currPos.items indexes of killed items (None if never used)
    pub killed_items: Option<Vec<i32>>,
    /// number of currently stored killed items
    pub num_killed: i32,
    /// current position data
    pub curr_pos: HashScanPosData,
}

pub type HashScanOpaque = *mut HashScanOpaqueData; // TODO(ptr)

/*
 * Definitions for metapage.
 */
pub const HASH_METAPAGE: BlockNumber = 0; // metapage is always block 0

pub const HASH_MAGIC: u32 = 0x6440640;
pub const HASH_VERSION: u32 = 4;

pub const HASH_MAX_BITMAPS: usize = {
    let by_blcksz = (crate::pg_config::BLCKSZ / 8) as usize;
    if by_blcksz < 1024 {
        by_blcksz
    } else {
        1024
    }
};

pub const HASH_SPLITPOINT_PHASE_BITS: u32 = 2;
pub const HASH_SPLITPOINT_PHASES_PER_GRP: u32 = 1 << HASH_SPLITPOINT_PHASE_BITS;
pub const HASH_SPLITPOINT_PHASE_MASK: u32 = HASH_SPLITPOINT_PHASES_PER_GRP - 1;
pub const HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE: u32 = 10;

/// max number of splitpoint phases a hash index can have
pub const HASH_MAX_SPLITPOINT_GROUP: u32 = 32;
pub const HASH_MAX_SPLITPOINTS: usize = (((HASH_MAX_SPLITPOINT_GROUP
    - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
    * HASH_SPLITPOINT_PHASES_PER_GRP)
    + HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE) as usize;

/// metapage data (ON-DISK).
#[repr(C)]
pub struct HashMetaPageData {
    /// magic no. for hash tables
    pub magic: u32,
    /// version ID
    pub version: u32,
    /// number of tuples stored in the table
    pub ntuples: f64,
    /// target fill factor (tuples/bucket)
    pub ffactor: u16,
    /// index page size (bytes)
    pub bsize: u16,
    /// bitmap array size (bytes) - must be a power of 2
    pub bmsize: u16,
    /// log2(bitmap array size in BITS)
    pub bmshift: u16,
    /// ID of maximum bucket in use
    pub maxbucket: u32,
    /// mask to modulo into entire table
    pub highmask: u32,
    /// mask to modulo into lower half of table
    pub lowmask: u32,
    /// splitpoint from which ovflpage being allocated
    pub ovflpoint: u32,
    /// lowest-number free ovflpage (bit#)
    pub firstfree: u32,
    /// number of bitmap pages
    pub nmaps: u32,
    /// hash function id from pg_proc
    pub procid: RegProcedure,
    /// spare pages before each splitpoint
    pub spares: [u32; HASH_MAX_SPLITPOINTS],
    /// blknos of ovfl bitmaps
    pub mapp: [BlockNumber; HASH_MAX_BITMAPS],
}
// Layout depends on HASH_MAX_SPLITPOINTS=98, HASH_MAX_BITMAPS=1024 (8K block).
const _: () = assert!(HASH_MAX_SPLITPOINTS == 98);
const _: () = assert!(HASH_MAX_BITMAPS == 1024);
const _: () = assert!(core::mem::offset_of!(HashMetaPageData, ntuples) == 8);
const _: () = assert!(core::mem::offset_of!(HashMetaPageData, spares) == 52);
const _: () = assert!(core::mem::size_of::<HashMetaPageData>() == 4544);

pub type HashMetaPage = *mut HashMetaPageData; // TODO(ptr)

/// hash reloptions storage (leading varlena header).
pub struct HashOptions {
    /// varlena header (do not touch directly!)
    pub varlena_header_: i32,
    /// page fill factor in percent (0..100)
    pub fillfactor: i32,
}

// HashGetFillFactor / HashGetTargetPageUsage / HashMaxItemSize depend on
// Relation rd_options and page layout; bodies stubbed.
pub fn hash_get_fill_factor(_relation: Relation) -> i32 {
    unimplemented!()
}
pub fn hash_get_target_page_usage(_relation: Relation) -> i32 {
    unimplemented!()
}
pub fn hash_max_item_size(_page: &Page) -> usize {
    unimplemented!()
}

/// INDEX_MOVED_BY_SPLIT_MASK = INDEX_AM_RESERVED_BIT.
pub const INDEX_MOVED_BY_SPLIT_MASK: u16 = crate::access::itup::INDEX_AM_RESERVED_BIT;

pub const HASH_MIN_FILLFACTOR: i32 = 10;
pub const HASH_DEFAULT_FILLFACTOR: i32 = 75;

/*
 * Constants
 */
pub const BYTE_TO_BIT: u32 = 3; // 2^3 bits/byte
pub const ALL_SET: u32 = !0;

// Bitmap page helpers (operate on the metapage / a bitmap word array).
pub const fn bmpgsz_byte(metap: &HashMetaPageData) -> u16 {
    metap.bmsize
}
pub const fn bmpgsz_bit(metap: &HashMetaPageData) -> u32 {
    (metap.bmsize as u32) << BYTE_TO_BIT
}
pub const fn bmpg_shift(metap: &HashMetaPageData) -> u16 {
    metap.bmshift
}
pub const fn bmpg_mask(metap: &HashMetaPageData) -> u32 {
    bmpgsz_bit(metap) - 1
}

// HashPageGetBitmap / HashGetMaxBitmapSize / HashPageGetMeta read page contents.
pub fn hash_page_get_bitmap(_page: &mut Page) -> *mut u32 {
    unimplemented!()
}
pub fn hash_get_max_bitmap_size(_page: &Page) -> usize {
    unimplemented!()
}
pub fn hash_page_get_meta(_page: &mut Page) -> HashMetaPage {
    unimplemented!()
}

/// Number of bits in an ovflpage bitmap word.
pub const BITS_PER_MAP: u32 = 32;

/// clear the nth bit in a bitmap word array
pub fn clrbit(a: &mut [u32], n: u32) {
    a[(n / BITS_PER_MAP) as usize] &= !(1 << (n % BITS_PER_MAP));
}
/// set the nth bit in a bitmap word array
pub fn setbit(a: &mut [u32], n: u32) {
    a[(n / BITS_PER_MAP) as usize] |= 1 << (n % BITS_PER_MAP);
}
/// test the nth bit in a bitmap word array
pub fn isset(a: &[u32], n: u32) -> u32 {
    a[(n / BITS_PER_MAP) as usize] & (1 << (n % BITS_PER_MAP))
}

/*
 * page-level and high-level locking modes (see README)
 */
pub const HASH_READ: i32 = BUFFER_LOCK_SHARE;
pub const HASH_WRITE: i32 = BUFFER_LOCK_EXCLUSIVE;
pub const HASH_NOLOCK: i32 = -1;

/*
 * Hash opclass amproc numbers.
 */
pub const HASHSTANDARD_PROC: u16 = 1;
pub const HASHEXTENDED_PROC: u16 = 2;
pub const HASHOPTIONS_PROC: u16 = 3;
pub const HASHNProcs: u16 = 3;

/* public routines */

pub fn hashbuild(
    _heap: Relation,
    _index: Relation,
    _index_info: &mut IndexInfo,
) -> *mut IndexBuildResult {
    unimplemented!()
}
pub fn hashbuildempty(_index: Relation) {
    unimplemented!()
}
pub fn hashinsert(
    _rel: Relation,
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
pub fn hashgettuple(_scan: IndexScanDesc, _dir: ScanDirection) -> bool {
    unimplemented!()
}
pub fn hashgetbitmap(_scan: IndexScanDesc, _tbm: &mut TIDBitmap) -> i64 {
    unimplemented!()
}
pub fn hashbeginscan(_rel: Relation, _nkeys: i32, _norderbys: i32) -> IndexScanDesc {
    unimplemented!()
}
pub fn hashrescan(
    _scan: IndexScanDesc,
    _scankey: ScanKey,
    _nscankeys: i32,
    _orderbys: ScanKey,
    _norderbys: i32,
) {
    unimplemented!()
}
pub fn hashendscan(_scan: IndexScanDesc) {
    unimplemented!()
}
pub fn hashbulkdelete(
    _info: &mut IndexVacuumInfo,
    _stats: *mut IndexBulkDeleteResult,
    _callback: &mut IndexBulkDeleteCallback,
    _callback_state: *mut core::ffi::c_void,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}
pub fn hashvacuumcleanup(
    _info: &mut IndexVacuumInfo,
    _stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}
pub fn hashoptions(_reloptions: Datum, _validate: bool) -> *mut bytea {
    unimplemented!()
}
pub fn hashvalidate(_opclassoid: Oid) -> bool {
    unimplemented!()
}
pub fn hashadjustmembers(
    _opfamilyoid: Oid,
    _opclassoid: Oid,
    _operators: &mut [OpFamilyMember],
    _functions: &mut [OpFamilyMember],
) {
    unimplemented!()
}

pub fn hashtranslatestrategy(_strategy: StrategyNumber, _opfamily: Oid) -> CompareType {
    unimplemented!()
}
pub fn hashtranslatecmptype(_cmptype: CompareType, _opfamily: Oid) -> StrategyNumber {
    unimplemented!()
}

/* private routines */

/* hashinsert.c */
pub fn _hash_doinsert(_rel: Relation, _itup: IndexTuple, _heap_rel: Relation, _sorted: bool) {
    unimplemented!()
}
pub fn _hash_pgaddtup(
    _rel: Relation,
    _buf: Buffer,
    _itemsize: usize,
    _itup: IndexTuple,
    _appendtup: bool,
) -> OffsetNumber {
    unimplemented!()
}
pub fn _hash_pgaddmultitup(
    _rel: Relation,
    _buf: Buffer,
    _itups: &[IndexTuple],
    _itup_offsets: &[OffsetNumber],
    _nitups: u16,
) {
    unimplemented!()
}

/* hashovfl.c */
pub fn _hash_addovflpage(_rel: Relation, _metabuf: Buffer, _buf: Buffer, _retain_pin: bool) -> Buffer {
    unimplemented!()
}
pub fn _hash_freeovflpage(
    _rel: Relation,
    _bucketbuf: Buffer,
    _ovflbuf: Buffer,
    _wbuf: Buffer,
    _itups: &[IndexTuple],
    _itup_offsets: &[OffsetNumber],
    _tups_size: &[usize],
    _nitups: u16,
    _bstrategy: *mut BufferAccessStrategy,
) -> BlockNumber {
    unimplemented!()
}
pub fn _hash_initbitmapbuffer(_buf: Buffer, _bmsize: u16, _initpage: bool) {
    unimplemented!()
}
pub fn _hash_squeezebucket(
    _rel: Relation,
    _bucket: Bucket,
    _bucket_blkno: BlockNumber,
    _bucket_buf: Buffer,
    _bstrategy: *mut BufferAccessStrategy,
) {
    unimplemented!()
}
pub fn _hash_ovflblkno_to_bitno(_metap: HashMetaPage, _ovflblkno: BlockNumber) -> u32 {
    unimplemented!()
}

/* hashpage.c */
pub fn _hash_getbuf(_rel: Relation, _blkno: BlockNumber, _access: i32, _flags: i32) -> Buffer {
    unimplemented!()
}
pub fn _hash_getbuf_with_condlock_cleanup(
    _rel: Relation,
    _blkno: BlockNumber,
    _flags: i32,
) -> Buffer {
    unimplemented!()
}
pub fn _hash_getcachedmetap(
    _rel: Relation,
    _metabuf: &mut Buffer,
    _force_refresh: bool,
) -> HashMetaPage {
    unimplemented!()
}
pub fn _hash_getbucketbuf_from_hashkey(
    _rel: Relation,
    _hashkey: u32,
    _access: i32,
    _cachedmetap: &mut HashMetaPage,
) -> Buffer {
    unimplemented!()
}
pub fn _hash_getinitbuf(_rel: Relation, _blkno: BlockNumber) -> Buffer {
    unimplemented!()
}
pub fn _hash_initbuf(_buf: Buffer, _max_bucket: u32, _num_bucket: u32, _flag: u32, _initpage: bool) {
    unimplemented!()
}
pub fn _hash_getnewbuf(_rel: Relation, _blkno: BlockNumber, _fork_num: ForkNumber) -> Buffer {
    unimplemented!()
}
pub fn _hash_getbuf_with_strategy(
    _rel: Relation,
    _blkno: BlockNumber,
    _access: i32,
    _flags: i32,
    _bstrategy: *mut BufferAccessStrategy,
) -> Buffer {
    unimplemented!()
}
pub fn _hash_relbuf(_rel: Relation, _buf: Buffer) {
    unimplemented!()
}
pub fn _hash_dropbuf(_rel: Relation, _buf: Buffer) {
    unimplemented!()
}
pub fn _hash_dropscanbuf(_rel: Relation, _so: HashScanOpaque) {
    unimplemented!()
}
pub fn _hash_init(_rel: Relation, _num_tuples: f64, _fork_num: ForkNumber) -> u32 {
    unimplemented!()
}
pub fn _hash_init_metabuffer(
    _buf: Buffer,
    _num_tuples: f64,
    _procid: RegProcedure,
    _ffactor: u16,
    _initpage: bool,
) {
    unimplemented!()
}
pub fn _hash_pageinit(_page: &mut Page, _size: usize) {
    unimplemented!()
}
pub fn _hash_expandtable(_rel: Relation, _metabuf: Buffer) {
    unimplemented!()
}
pub fn _hash_finish_split(
    _rel: Relation,
    _metabuf: Buffer,
    _obuf: Buffer,
    _obucket: Bucket,
    _maxbucket: u32,
    _highmask: u32,
    _lowmask: u32,
) {
    unimplemented!()
}

/* hashsearch.c */
pub fn _hash_next(_scan: IndexScanDesc, _dir: ScanDirection) -> bool {
    unimplemented!()
}
pub fn _hash_first(_scan: IndexScanDesc, _dir: ScanDirection) -> bool {
    unimplemented!()
}

/* hashsort.c */
/// opaque struct in hashsort.c
pub struct HSpool {
    _private: [u8; 0],
}

pub fn _h_spoolinit(_heap: Relation, _index: Relation, _num_buckets: u32) -> *mut HSpool {
    unimplemented!()
}
pub fn _h_spooldestroy(_hspool: &mut HSpool) {
    unimplemented!()
}
pub fn _h_spool(_hspool: &mut HSpool, _self: &mut ItemPointerData, _values: &[Datum], _isnull: &[bool]) {
    unimplemented!()
}
pub fn _h_indexbuild(_hspool: &mut HSpool, _heap_rel: Relation) {
    unimplemented!()
}

/* hashutil.c */
pub fn _hash_checkqual(_scan: IndexScanDesc, _itup: IndexTuple) -> bool {
    unimplemented!()
}
pub fn _hash_datum2hashkey(_rel: Relation, _key: Datum) -> u32 {
    unimplemented!()
}
pub fn _hash_datum2hashkey_type(_rel: Relation, _key: Datum, _keytype: Oid) -> u32 {
    unimplemented!()
}
pub fn _hash_hashkey2bucket(_hashkey: u32, _maxbucket: u32, _highmask: u32, _lowmask: u32) -> Bucket {
    unimplemented!()
}
pub fn _hash_spareindex(_num_bucket: u32) -> u32 {
    unimplemented!()
}
pub fn _hash_get_totalbuckets(_splitpoint_phase: u32) -> u32 {
    unimplemented!()
}
pub fn _hash_checkpage(_rel: Relation, _buf: Buffer, _flags: i32) {
    unimplemented!()
}
pub fn _hash_get_indextuple_hashkey(_itup: IndexTuple) -> u32 {
    unimplemented!()
}
pub fn _hash_convert_tuple(
    _index: Relation,
    _user_values: &[Datum],
    _user_isnull: &[bool],
    _index_values: &mut [Datum],
    _index_isnull: &mut [bool],
) -> bool {
    unimplemented!()
}
pub fn _hash_binsearch(_page: &Page, _hash_value: u32) -> OffsetNumber {
    unimplemented!()
}
pub fn _hash_binsearch_last(_page: &Page, _hash_value: u32) -> OffsetNumber {
    unimplemented!()
}
pub fn _hash_get_oldblock_from_newbucket(_rel: Relation, _new_bucket: Bucket) -> BlockNumber {
    unimplemented!()
}
pub fn _hash_get_newblock_from_oldbucket(_rel: Relation, _old_bucket: Bucket) -> BlockNumber {
    unimplemented!()
}
pub fn _hash_get_newbucket_from_oldbucket(
    _rel: Relation,
    _old_bucket: Bucket,
    _lowmask: u32,
    _maxbucket: u32,
) -> Bucket {
    unimplemented!()
}
pub fn _hash_kill_items(_scan: IndexScanDesc) {
    unimplemented!()
}

/* hash.c */
#[allow(clippy::too_many_arguments)]
pub fn hashbucketcleanup(
    _rel: Relation,
    _cur_bucket: Bucket,
    _bucket_buf: Buffer,
    _bucket_blkno: BlockNumber,
    _bstrategy: *mut BufferAccessStrategy,
    _maxbucket: u32,
    _highmask: u32,
    _lowmask: u32,
    _tuples_removed: &mut f64,
    _num_index_tuples: &mut f64,
    _split_cleanup: bool,
    _callback: &mut IndexBulkDeleteCallback,
    _callback_state: *mut core::ffi::c_void,
) {
    unimplemented!()
}

// Keep MaxIndexTuplesPerPage referenced (cap of HashScanPosData.items).
const _: () = assert!(MaxIndexTuplesPerPage > 0);
