//! hashpage.rs
//!   Hash table page management code for the Postgres hash access method
//! Translated 1:1 from postgres/src/backend/access/hash/hashpage.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/hash/hashpage.c
//!
//! NOTES
//!   Postgres hash pages look like ordinary relation pages.  The opaque
//!   data at high addresses includes information about the page including
//!   whether a page is an overflow page or a true bucket, the bucket
//!   number, and the block numbers of the preceding and following pages
//!   in the same bucket.
//!
//!   The first page in a hash relation, page zero, is special -- it stores
//!   information describing the hash table; it is referred to as the
//!   "meta page." Pages one and higher store the actual data.
//!
//!   There are also bitmap pages, which are not manipulated here;
//!   see hashovfl.c.

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::access::common::indextuple::{
    CopyIndexTuple, IndexTuple, IndexTupleData, IndexTupleSize,
};
use crate::access::hash::hash::HashScanOpaque;
use crate::access::rmgrdesc::hashdesc::{
    xl_hash_init_bitmap_page, xl_hash_init_meta_page, xl_hash_split_allocate_page,
    xl_hash_split_complete, SizeOfHashInitBitmapPage, SizeOfHashInitMetaPage,
    SizeOfHashSplitAllocPage, SizeOfHashSplitComplete, XLH_SPLIT_META_UPDATE_MASKS,
    XLH_SPLIT_META_UPDATE_SPLITPOINT, XLOG_HASH_INIT_BITMAP_PAGE, XLOG_HASH_INIT_META_PAGE,
    XLOG_HASH_SPLIT_ALLOCATE_PAGE, XLOG_HASH_SPLIT_COMPLETE, XLOG_HASH_SPLIT_PAGE,
};
use crate::access::rmgrlist::RM_HASH_ID;
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::common::relpath::{ForkNumber, INIT_FORKNUM, MAIN_FORKNUM};
use crate::elog;
use crate::miscadmin::{END_CRIT_SECTION, START_CRIT_SECTION};
use crate::pg_config::BLCKSZ;
use crate::port::pg_bitutils::{pg_leftmost_one_pos32, pg_nextpower2_32};
use crate::postgres_ext::Oid;
use crate::storage::block::{BlockNumber, BlockNumberIsValid, InvalidBlockNumber};
use crate::storage::buf::{Buffer, BufferAccessStrategy, InvalidBuffer};
use crate::storage::bufpage::{
    Item, Page, PageGetFreeSpaceForMultipleTuples, PageGetItem, PageGetItemId,
    PageGetMaxOffsetNumber, PageSetLSN,
};
use crate::storage::itemid::{ItemId, ItemIdData, ItemIdIsDead};
use crate::storage::off::{FirstOffsetNumber, OffsetNumber, OffsetNumberNext};
use crate::utils::elog::ERROR;
use crate::utils::hash::dynahash::HASHACTION::{HASH_ENTER, HASH_FIND};
use crate::utils::hash::dynahash::{
    hash_create, hash_destroy, hash_search, HASHCTL, HASH_BLOBS, HASH_CONTEXT, HASH_ELEM, HTAB,
};
use crate::utils::mmgr::mcxt::{pfree, MemoryContextAlloc};
use crate::utils::rel::{Relation, RelationGetRelationName};
use crate::Assert;

// ---------------------------------------------------------------------------
// Declarations merged from access/hash.h that are needed by hashpage.c.
// These mirror the sibling hash files (hash.rs, hashovfl.rs, etc.). The
// canonical definitions live in access/hash.h (ported in hash.rs).
// ---------------------------------------------------------------------------

pub type Bucket = uint32;
pub type RegProcedure = Oid; // postgres_ext.h

pub const InvalidBucket: Bucket = 0xFFFFFFFF as Bucket;

/* page-type flags (hash.h) */
pub const LH_UNUSED_PAGE: uint16 = 0;
pub const LH_OVERFLOW_PAGE: uint16 = 1 << 0;
pub const LH_BUCKET_PAGE: uint16 = 1 << 1;
pub const LH_BITMAP_PAGE: uint16 = 1 << 2;
pub const LH_META_PAGE: uint16 = 1 << 3;
pub const LH_BUCKET_BEING_POPULATED: uint16 = 1 << 4;
pub const LH_BUCKET_BEING_SPLIT: uint16 = 1 << 5;
pub const LH_BUCKET_NEEDS_SPLIT_CLEANUP: uint16 = 1 << 6;
pub const LH_PAGE_HAS_DEAD_TUPLES: uint16 = 1 << 7;

pub const HASHO_PAGE_ID: uint16 = 0xFF80;

#[repr(C)]
pub struct HashPageOpaqueData {
    pub hasho_prevblkno: BlockNumber,
    pub hasho_nextblkno: BlockNumber,
    pub hasho_bucket: Bucket,
    pub hasho_flag: uint16,
    pub hasho_page_id: uint16,
}

pub type HashPageOpaque = *mut HashPageOpaqueData;

/* H_NEEDS_SPLIT_CLEANUP(opaque) etc. (hash.h) */
#[inline]
unsafe fn H_NEEDS_SPLIT_CLEANUP(opaque: HashPageOpaque) -> bool {
    ((*opaque).hasho_flag & LH_BUCKET_NEEDS_SPLIT_CLEANUP) != 0
}
#[inline]
unsafe fn H_BUCKET_BEING_SPLIT(opaque: HashPageOpaque) -> bool {
    ((*opaque).hasho_flag & LH_BUCKET_BEING_SPLIT) != 0
}
#[inline]
unsafe fn H_BUCKET_BEING_POPULATED(opaque: HashPageOpaque) -> bool {
    ((*opaque).hasho_flag & LH_BUCKET_BEING_POPULATED) != 0
}

/* HASH_MAX_BITMAPS = Min(BLCKSZ / 8, 1024) */
pub const HASH_MAX_BITMAPS: usize = {
    let v = (BLCKSZ / 8) as usize;
    if v < 1024 {
        v
    } else {
        1024
    }
};

/* metapage block number (hash.h) */
pub const HASH_METAPAGE: BlockNumber = 0;

/* special page id for hash AM (hash.h) */
pub const HASH_MAGIC: uint32 = 0x6440640;
pub const HASH_VERSION: uint32 = 4;

/* P_NEW (bufmgr.h) */
pub const P_NEW: BlockNumber = InvalidBlockNumber;

/* _hash_getbuf flags (hash.h) */
pub const HASH_NOLOCK: c_int = -1;
pub const HASH_READ: c_int = BUFFER_LOCK_SHARE;
pub const HASH_WRITE: c_int = BUFFER_LOCK_EXCLUSIVE;

pub const BYTE_TO_BIT: uint32 = 3; /* 2^3 bits per byte */
pub const BITS_PER_MAP: uint32 = 32; /* Number of bits in uint32 */

/* INDEX_MOVED_BY_SPLIT_MASK (hash.h / itup.h) */
pub const INDEX_MOVED_BY_SPLIT_MASK: uint16 = 0x2000;

/* support function number (hash.h) */
pub const HASHSTANDARD_PROC: AttrNumber = 1;

/* buffer lock modes (bufmgr.h) */
const BUFFER_LOCK_UNLOCK: c_int = 0;
const BUFFER_LOCK_SHARE: c_int = 1;
const BUFFER_LOCK_EXCLUSIVE: c_int = 2;

/* ReadBufferMode (bufmgr.h) */
pub type ReadBufferMode = c_int;
const RBM_NORMAL: ReadBufferMode = 0;
const RBM_ZERO_AND_LOCK: ReadBufferMode = 1;

/* ExtendBufferedRel flags (bufmgr.h) */
const EB_SKIP_EXTENSION_LOCK: uint32 = 1 << 0;
const EB_LOCK_FIRST: uint32 = 1 << 1;

/* REGBUF flags (xloginsert.h) */
const REGBUF_FORCE_IMAGE: uint8 = 0x01;
const REGBUF_NO_IMAGE: uint8 = 0x02;
const REGBUF_WILL_INIT: uint8 = 0x04 | 0x02; /* page will be re-initialized at replay */
const REGBUF_STANDARD: uint8 = 0x08;
const REGBUF_KEEP_DATA: uint8 = 0x10;
const REGBUF_NO_CHANGE: uint8 = 0x20; /* intentionally register clean buffer */

/* ERRCODE_PROGRAM_LIMIT_EXCEEDED (errcodes.h) */
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;

/* MaxIndexTuplesPerPage (itup.h) */
pub const MaxIndexTuplesPerPage: usize = 1358;

// ---------------------------------------------------------------------------
// HashMetaPageData and accessor helpers (access/hash.h).
// ---------------------------------------------------------------------------

pub const HASH_MAX_SPLITPOINT_GROUP: uint32 = 32;
pub const HASH_MAX_SPLITPOINTS: uint32 = ((HASH_MAX_SPLITPOINT_GROUP - 10) * 32) + 10;

#[repr(C)]
pub struct HashMetaPageData {
    pub hashm_magic: uint32,
    pub hashm_version: uint32,
    pub hashm_ntuples: f64,
    pub hashm_ffactor: uint16,
    pub hashm_bsize: uint16,
    pub hashm_bmsize: uint16,
    pub hashm_bmshift: uint16,
    pub hashm_maxbucket: uint32,
    pub hashm_highmask: uint32,
    pub hashm_lowmask: uint32,
    pub hashm_ovflpoint: uint32,
    pub hashm_firstfree: uint32,
    pub hashm_nmaps: uint32,
    pub hashm_procid: RegProcedure,
    pub hashm_spares: [uint32; HASH_MAX_SPLITPOINTS as usize],
    pub hashm_mapp: [BlockNumber; HASH_MAX_BITMAPS],
}

pub type HashMetaPage = *mut HashMetaPageData;

/* BMPG_SHIFT(metap) */
#[inline]
unsafe fn BMPG_SHIFT(metap: HashMetaPage) -> uint32 {
    (*metap).hashm_bmshift as uint32
}
/* BMPG_MASK(metap) */
#[inline]
unsafe fn BMPG_MASK(metap: HashMetaPage) -> uint32 {
    (((*metap).hashm_bmsize as uint32) << BYTE_TO_BIT) - 1
}

/*
 * BUCKET_TO_BLKNO(metap, B)
 *	(BlockNumber)((B) + ((B) ? metap->hashm_spares[_hash_spareindex((B)+1)-1] : 0) + 1)
 */
#[inline]
unsafe fn BUCKET_TO_BLKNO(metap: HashMetaPage, b: Bucket) -> BlockNumber {
    let extra: uint32 = if b != 0 {
        (*metap).hashm_spares[(_hash_spareindex(b + 1) - 1) as usize]
    } else {
        0
    };
    (b + extra + 1) as BlockNumber
}

/* HashPageGetOpaque(page) */
#[inline]
unsafe fn HashPageGetOpaque(page: Page) -> HashPageOpaque {
    // PageGetSpecialPointer(page)
    let phdr = page as *mut u8;
    let special = *(phdr.add(core::mem::offset_of!(PageHeaderShim, pd_special)) as *const u16);
    phdr.add(special as usize) as HashPageOpaque
}

/* HashPageGetMeta(page) */
#[inline]
unsafe fn HashPageGetMeta(page: Page) -> HashMetaPage {
    // (HashMetaPage) PageGetContents(page)
    let phdr = page as *mut u8;
    phdr.add(MAXALIGN(core::mem::size_of::<PageHeaderShim>())) as HashMetaPage
}

/* HashGetMaxBitmapSize(page) and HashGetTargetPageUsage(rel) live in hash.h */

// Minimal page header shim to locate pd_special / set pd_lower
// (storage/bufpage.h).
#[repr(C)]
struct PageHeaderShim {
    pd_lsn: u64,
    pd_checksum: u16,
    pd_flags: u16,
    pd_lower: u16,
    pd_upper: u16,
    pd_special: u16,
}

type PageHeader = *mut PageHeaderShim;

#[inline]
fn MAXALIGN(len: usize) -> usize {
    (len + 7) & !7
}

/* PGIOAlignedBlock (c.h) */
#[repr(C, align(4096))]
pub struct PGIOAlignedBlock {
    pub data: [c_char; BLCKSZ as usize],
}

/* ItemPointerData (itemptr.h) */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ItemPointerData {
    pub ip_blkid: [uint16; 2],
    pub ip_posid: uint16,
}

// ---------------------------------------------------------------------------
// Stubbed callees from other (not-yet-ported) translation units.
// ---------------------------------------------------------------------------

unsafe fn _hash_spareindex(_num_bucket: uint32) -> uint32 { crate::access::hash::hashutil::_hash_spareindex(_num_bucket) }

unsafe fn _hash_get_totalbuckets(_splitpoint_phase: uint32) -> uint32 { crate::access::hash::hashutil::_hash_get_totalbuckets(_splitpoint_phase) }

unsafe fn _hash_checkpage(_rel: Relation, _buf: Buffer, _flags: c_int) { crate::access::hash::hashutil::_hash_checkpage(_rel, _buf, _flags) }

unsafe fn _hash_hashkey2bucket(
    _hashkey: uint32,
    _maxbucket: uint32,
    _highmask: uint32,
    _lowmask: uint32,
) -> Bucket { crate::access::hash::hashutil::_hash_hashkey2bucket(_hashkey, _maxbucket, _highmask, _lowmask) }

unsafe fn _hash_get_indextuple_hashkey(_itup: IndexTuple) -> uint32 { crate::access::hash::hashutil::_hash_get_indextuple_hashkey(_itup) }

unsafe fn _hash_get_newblock_from_oldbucket(_rel: Relation, _old_bucket: Bucket) -> BlockNumber { crate::access::hash::hashutil::_hash_get_newblock_from_oldbucket(_rel, _old_bucket) }

unsafe fn _hash_pgaddmultitup(
    _rel: Relation,
    _buf: Buffer,
    _itups: *mut IndexTuple,
    _itup_offsets: *mut OffsetNumber,
    _nitups: uint16,
) { crate::access::hash::hashinsert::_hash_pgaddmultitup(_rel, _buf, _itups, _itup_offsets, _nitups) }

unsafe fn _hash_addovflpage(
    _rel: Relation,
    _metabuf: Buffer,
    _buf: Buffer,
    _retain_pin: bool,
) -> Buffer { crate::access::hash::hashovfl::_hash_addovflpage(_rel, _metabuf, _buf, _retain_pin) }

unsafe fn _hash_initbitmapbuffer(_buf: Buffer, _bmsize: uint16, _initpage: bool) { crate::access::hash::hashovfl::_hash_initbitmapbuffer(_buf, _bmsize, _initpage) }

unsafe fn hashbucketcleanup(
    _rel: Relation,
    _cur_bucket: Bucket,
    _bucket_buf: Buffer,
    _bucket_blkno: BlockNumber,
    _bstrategy: BufferAccessStrategy,
    _maxbucket: uint32,
    _highmask: uint32,
    _lowmask: uint32,
    _tuples_removed: *mut f64,
    _num_index_tuples: *mut f64,
    _split_cleanup: bool,
    _callback: *mut c_void,
    _callback_state: *mut c_void,
) {
    unimplemented!() // TODO(pg-port): access/hash.c
}

unsafe fn index_getprocid(_irel: Relation, _attnum: AttrNumber, _procnum: uint16) -> RegProcedure { crate::access::index::indexam::index_getprocid(_irel, _attnum, _procnum) }

type AttrNumber = i16;

unsafe fn HashGetTargetPageUsage(_rel: Relation) -> uint32 {
    unimplemented!() // TODO(pg-port): access/hash.h
}

unsafe fn HashGetMaxBitmapSize(_page: Page) -> uint16 {
    unimplemented!() // TODO(pg-port): access/hash.h
}

// ---------------------------------------------------------------------------
// bufmgr.h / bufmgr.c stubs.
// ---------------------------------------------------------------------------

unsafe fn ReadBuffer(_reln: Relation, _blockNum: BlockNumber) -> Buffer {
    unimplemented!() // TODO(pg-port): storage/buffer/bufmgr.c
}

unsafe fn ReadBufferExtended(
    _reln: Relation,
    _forkNum: ForkNumber,
    _blockNum: BlockNumber,
    _mode: ReadBufferMode,
    _strategy: BufferAccessStrategy,
) -> Buffer {
    unimplemented!() // TODO(pg-port): storage/buffer/bufmgr.c
}

unsafe fn ExtendBufferedRel(
    _bmr: BufferManagerRelation,
    _forkNum: ForkNumber,
    _strategy: BufferAccessStrategy,
    _flags: uint32,
) -> Buffer { crate::storage::buffer::bufmgr::ExtendBufferedRel(_bmr, _forkNum, _strategy as _, _flags) }

type BufferManagerRelation = crate::storage::buffer::bufmgr::BufferManagerRelation;

unsafe fn BMR_REL(_rel: Relation) -> BufferManagerRelation { crate::storage::buffer::bufmgr::BMR_REL(_rel as _) }

unsafe fn ReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO(pg-port): storage/buffer/bufmgr.c
}

unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO(pg-port): storage/buffer/bufmgr.c
}

unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO(pg-port): storage/buffer/bufmgr.c
}

unsafe fn ConditionalLockBufferForCleanup(_buffer: Buffer) -> bool { crate::storage::buffer::bufmgr::ConditionalLockBufferForCleanup(_buffer) }

unsafe fn IsBufferCleanupOK(_buffer: Buffer) -> bool { crate::storage::buffer::bufmgr::IsBufferCleanupOK(_buffer) }

unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO(pg-port): storage/buffer/bufmgr.c
}

unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO(pg-port): storage/bufmgr.h
}

unsafe fn BufferGetPageSize(_buffer: Buffer) -> Size { crate::access::nbtree::nbtpage::BufferGetPageSize(_buffer) }

unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO(pg-port): storage/buffer/bufmgr.c
}

unsafe fn BufferIsValid(_buffer: Buffer) -> bool { crate::access::nbtree::nbtpage::BufferIsValid(_buffer) }

unsafe fn PageInit(_page: Page, _pageSize: Size, _specialSize: Size) {
    unimplemented!() // TODO(pg-port): storage/bufpage.c
}

unsafe fn PageSetChecksumInplace(_page: Page, _blkno: BlockNumber) {
    unimplemented!() // TODO(pg-port): storage/bufpage.c
}

unsafe fn RelationGetNumberOfBlocksInFork(_relation: Relation, _forkNum: ForkNumber) -> BlockNumber { crate::storage::buffer::bufmgr::RelationGetNumberOfBlocksInFork(_relation, _forkNum) }

unsafe fn RelationGetSmgr(_rel: Relation) -> SMgrRelation { crate::storage::buffer::bufmgr::RelationGetSmgr(_rel) as _ }

type SMgrRelation = *mut c_void;

unsafe fn smgrextend(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffer: *const c_char,
    _skipFsync: bool,
) { crate::storage::smgr::smgr::smgrextend(_reln as _, _forknum, _blocknum, _buffer as _, _skipFsync) }

unsafe fn RelationNeedsWAL(_relation: Relation) -> bool { crate::access::nbtree::nbtdedup::RelationNeedsWAL(_relation) }

unsafe fn PredicateLockPageSplit(_rel: Relation, _oldblkno: BlockNumber, _newblkno: BlockNumber) { crate::storage::lmgr::predicate::PredicateLockPageSplit(_rel as _, _oldblkno, _newblkno) }

unsafe fn CHECK_FOR_INTERRUPTS() {
    unimplemented!() // TODO(pg-port): miscadmin.h
}

unsafe fn log_newpage(
    _rlocator: *mut RelFileLocator,
    _forknum: ForkNumber,
    _blkno: BlockNumber,
    _page: Page,
    _page_std: bool,
) -> XLogRecPtr { crate::access::transam::xloginsert::log_newpage(_rlocator as _, _forknum, _blkno, _page, _page_std) }

type RelFileLocator = c_void;

// ---------------------------------------------------------------------------
// XLog insertion helpers (access/xloginsert.h). Local stubs mirror siblings.
// ---------------------------------------------------------------------------

unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO(pg-port): access/xloginsert.c
}

unsafe fn XLogRegisterData(_data: *mut c_void, _len: c_int) {
    unimplemented!() // TODO(pg-port): access/xloginsert.c
}

unsafe fn XLogRegisterBuffer(_block_id: uint8, _buffer: Buffer, _flags: uint8) {
    unimplemented!() // TODO(pg-port): access/xloginsert.c
}

unsafe fn XLogRegisterBufData(_block_id: uint8, _data: *mut c_void, _len: c_int) {
    unimplemented!() // TODO(pg-port): access/xloginsert.c
}

unsafe fn XLogInsert(_rmid: u8, _info: uint8) -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): access/xloginsert.c
}

// ---------------------------------------------------------------------------

/*
 *	_hash_getbuf() -- Get a buffer by block number for read or write.
 *
 *		'access' must be HASH_READ, HASH_WRITE, or HASH_NOLOCK.
 *		'flags' is a bitwise OR of the allowed page types.
 *
 *		This must be used only to fetch pages that are expected to be valid
 *		already.  _hash_checkpage() is applied using the given flags.
 *
 *		When this routine returns, the appropriate lock is set on the
 *		requested buffer and its reference count has been incremented
 *		(ie, the buffer is "locked and pinned").
 *
 *		P_NEW is disallowed because this routine can only be used
 *		to access pages that are known to be before the filesystem EOF.
 *		Extending the index should be done with _hash_getnewbuf.
 */
pub unsafe fn _hash_getbuf(rel: Relation, blkno: BlockNumber, access: c_int, flags: c_int) -> Buffer {
    let buf: Buffer;

    if blkno == P_NEW {
        elog!(ERROR, "hash AM does not use P_NEW");
    }

    buf = ReadBuffer(rel, blkno);

    if access != HASH_NOLOCK {
        LockBuffer(buf, access);
    }

    /* ref count and lock type are correct */

    _hash_checkpage(rel, buf, flags);

    buf
}

/*
 * _hash_getbuf_with_condlock_cleanup() -- Try to get a buffer for cleanup.
 *
 *		We read the page and try to acquire a cleanup lock.  If we get it,
 *		we return the buffer; otherwise, we return InvalidBuffer.
 */
pub unsafe fn _hash_getbuf_with_condlock_cleanup(
    rel: Relation,
    blkno: BlockNumber,
    flags: c_int,
) -> Buffer {
    let buf: Buffer;

    if blkno == P_NEW {
        elog!(ERROR, "hash AM does not use P_NEW");
    }

    buf = ReadBuffer(rel, blkno);

    if !ConditionalLockBufferForCleanup(buf) {
        ReleaseBuffer(buf);
        return InvalidBuffer as Buffer;
    }

    /* ref count and lock type are correct */

    _hash_checkpage(rel, buf, flags);

    buf
}

/*
 *	_hash_getinitbuf() -- Get and initialize a buffer by block number.
 *
 *		This must be used only to fetch pages that are known to be before
 *		the index's filesystem EOF, but are to be filled from scratch.
 *		_hash_pageinit() is applied automatically.  Otherwise it has
 *		effects similar to _hash_getbuf() with access = HASH_WRITE.
 *
 *		When this routine returns, a write lock is set on the
 *		requested buffer and its reference count has been incremented
 *		(ie, the buffer is "locked and pinned").
 *
 *		P_NEW is disallowed because this routine can only be used
 *		to access pages that are known to be before the filesystem EOF.
 *		Extending the index should be done with _hash_getnewbuf.
 */
pub unsafe fn _hash_getinitbuf(rel: Relation, blkno: BlockNumber) -> Buffer {
    let buf: Buffer;

    if blkno == P_NEW {
        elog!(ERROR, "hash AM does not use P_NEW");
    }

    buf = ReadBufferExtended(
        rel,
        MAIN_FORKNUM,
        blkno,
        RBM_ZERO_AND_LOCK,
        core::ptr::null_mut(),
    );

    /* ref count and lock type are correct */

    /* initialize the page */
    _hash_pageinit(BufferGetPage(buf), BufferGetPageSize(buf));

    buf
}

/*
 *	_hash_initbuf() -- Get and initialize a buffer by bucket number.
 */
pub unsafe fn _hash_initbuf(
    buf: Buffer,
    max_bucket: uint32,
    num_bucket: uint32,
    flag: uint32,
    initpage: bool,
) {
    let pageopaque: HashPageOpaque;
    let page: Page;

    page = BufferGetPage(buf);

    /* initialize the page */
    if initpage {
        _hash_pageinit(page, BufferGetPageSize(buf));
    }

    pageopaque = HashPageGetOpaque(page);

    /*
     * Set hasho_prevblkno with current hashm_maxbucket. This value will be
     * used to validate cached HashMetaPageData. See
     * _hash_getbucketbuf_from_hashkey().
     */
    (*pageopaque).hasho_prevblkno = max_bucket;
    (*pageopaque).hasho_nextblkno = InvalidBlockNumber;
    (*pageopaque).hasho_bucket = num_bucket;
    (*pageopaque).hasho_flag = flag as uint16;
    (*pageopaque).hasho_page_id = HASHO_PAGE_ID;
}

/*
 *	_hash_getnewbuf() -- Get a new page at the end of the index.
 *
 *		This has the same API as _hash_getinitbuf, except that we are adding
 *		a page to the index, and hence expect the page to be past the
 *		logical EOF.  (However, we have to support the case where it isn't,
 *		since a prior try might have crashed after extending the filesystem
 *		EOF but before updating the metapage to reflect the added page.)
 *
 *		It is caller's responsibility to ensure that only one process can
 *		extend the index at a time.  In practice, this function is called
 *		only while holding write lock on the metapage, because adding a page
 *		is always associated with an update of metapage data.
 */
pub unsafe fn _hash_getnewbuf(rel: Relation, blkno: BlockNumber, forkNum: ForkNumber) -> Buffer {
    let nblocks: BlockNumber = RelationGetNumberOfBlocksInFork(rel, forkNum);
    let buf: Buffer;

    if blkno == P_NEW {
        elog!(ERROR, "hash AM does not use P_NEW");
    }
    if blkno > nblocks {
        elog!(
            ERROR,
            "access to noncontiguous page in hash index \"{}\"",
            std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }

    /* smgr insists we explicitly extend the relation */
    if blkno == nblocks {
        buf = ExtendBufferedRel(
            BMR_REL(rel),
            forkNum,
            core::ptr::null_mut(),
            EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK,
        );
        if BufferGetBlockNumber(buf) != blkno {
            elog!(
                ERROR,
                "unexpected hash relation size: {}, should be {}",
                BufferGetBlockNumber(buf),
                blkno
            );
        }
    } else {
        buf = ReadBufferExtended(rel, forkNum, blkno, RBM_ZERO_AND_LOCK, core::ptr::null_mut());
    }

    /* ref count and lock type are correct */

    /* initialize the page */
    _hash_pageinit(BufferGetPage(buf), BufferGetPageSize(buf));

    buf
}

/*
 *	_hash_getbuf_with_strategy() -- Get a buffer with nondefault strategy.
 *
 *		This is identical to _hash_getbuf() but also allows a buffer access
 *		strategy to be specified.  We use this for VACUUM operations.
 */
pub unsafe fn _hash_getbuf_with_strategy(
    rel: Relation,
    blkno: BlockNumber,
    access: c_int,
    flags: c_int,
    bstrategy: BufferAccessStrategy,
) -> Buffer {
    let buf: Buffer;

    if blkno == P_NEW {
        elog!(ERROR, "hash AM does not use P_NEW");
    }

    buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, bstrategy);

    if access != HASH_NOLOCK {
        LockBuffer(buf, access);
    }

    /* ref count and lock type are correct */

    _hash_checkpage(rel, buf, flags);

    buf
}

/*
 *	_hash_relbuf() -- release a locked buffer.
 *
 * Lock and pin (refcount) are both dropped.
 */
pub unsafe fn _hash_relbuf(rel: Relation, buf: Buffer) {
    UnlockReleaseBuffer(buf);
}

/*
 *	_hash_dropbuf() -- release an unlocked buffer.
 *
 * This is used to unpin a buffer on which we hold no lock.
 */
pub unsafe fn _hash_dropbuf(rel: Relation, buf: Buffer) {
    ReleaseBuffer(buf);
}

/*
 *	_hash_dropscanbuf() -- release buffers used in scan.
 *
 * This routine unpins the buffers used during scan on which we
 * hold no lock.
 */
pub unsafe fn _hash_dropscanbuf(rel: Relation, so: HashScanOpaque) {
    /* release pin we hold on primary bucket page */
    if BufferIsValid((*so).hashso_bucket_buf) && (*so).hashso_bucket_buf != (*so).currPos.buf {
        _hash_dropbuf(rel, (*so).hashso_bucket_buf);
    }
    (*so).hashso_bucket_buf = InvalidBuffer as Buffer;

    /* release pin we hold on primary bucket page  of bucket being split */
    if BufferIsValid((*so).hashso_split_bucket_buf)
        && (*so).hashso_split_bucket_buf != (*so).currPos.buf
    {
        _hash_dropbuf(rel, (*so).hashso_split_bucket_buf);
    }
    (*so).hashso_split_bucket_buf = InvalidBuffer as Buffer;

    /* release any pin we still hold */
    if BufferIsValid((*so).currPos.buf) {
        _hash_dropbuf(rel, (*so).currPos.buf);
    }
    (*so).currPos.buf = InvalidBuffer as Buffer;

    /* reset split scan */
    (*so).hashso_buc_populated = false;
    (*so).hashso_buc_split = false;
}

/*
 *	_hash_init() -- Initialize the metadata page of a hash index,
 *				the initial buckets, and the initial bitmap page.
 *
 * The initial number of buckets is dependent on num_tuples, an estimate
 * of the number of tuples to be loaded into the index initially.  The
 * chosen number of buckets is returned.
 *
 * We are fairly cavalier about locking here, since we know that no one else
 * could be accessing this index.  In particular the rule about not holding
 * multiple buffer locks is ignored.
 */
pub unsafe fn _hash_init(rel: Relation, num_tuples: f64, forkNum: ForkNumber) -> uint32 {
    let metabuf: Buffer;
    let mut buf: Buffer;
    let bitmapbuf: Buffer;
    let pg: Page;
    let metap: HashMetaPage;
    let procid: RegProcedure;
    let data_width: int32;
    let item_width: int32;
    let mut ffactor: int32;
    let num_buckets: uint32;
    let mut i: uint32;
    let use_wal: bool;

    /* safety check */
    if RelationGetNumberOfBlocksInFork(rel, forkNum) != 0 {
        elog!(
            ERROR,
            "cannot initialize non-empty hash index \"{}\"",
            std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }

    /*
     * WAL log creation of pages if the relation is persistent, or this is the
     * init fork.  Init forks for unlogged relations always need to be WAL
     * logged.
     */
    use_wal = RelationNeedsWAL(rel) || forkNum == INIT_FORKNUM;

    /*
     * Determine the target fill factor (in tuples per bucket) for this index.
     * The idea is to make the fill factor correspond to pages about as full
     * as the user-settable fillfactor parameter says.  We can compute it
     * exactly since the index datatype (i.e. uint32 hash key) is fixed-width.
     */
    data_width = core::mem::size_of::<uint32>() as int32;
    item_width = (MAXALIGN(core::mem::size_of::<IndexTupleData>()) as int32)
        + (MAXALIGN(data_width as usize) as int32)
        + (core::mem::size_of::<ItemIdData>() as int32); /* include the line pointer */
    ffactor = (HashGetTargetPageUsage(rel) as int32) / item_width;
    /* keep to a sane range */
    if ffactor < 10 {
        ffactor = 10;
    }

    procid = index_getprocid(rel, 1, HASHSTANDARD_PROC as u16);

    /*
     * We initialize the metapage, the first N bucket pages, and the first
     * bitmap page in sequence, using _hash_getnewbuf to cause smgrextend()
     * calls to occur.  This ensures that the smgr level has the right idea of
     * the physical index length.
     *
     * Critical section not required, because on error the creation of the
     * whole relation will be rolled back.
     */
    metabuf = _hash_getnewbuf(rel, HASH_METAPAGE, forkNum);
    _hash_init_metabuffer(metabuf, num_tuples, procid, ffactor as uint16, false);
    MarkBufferDirty(metabuf);

    pg = BufferGetPage(metabuf);
    metap = HashPageGetMeta(pg);

    /* XLOG stuff */
    if use_wal {
        let mut xlrec: xl_hash_init_meta_page = core::mem::zeroed();
        let recptr: XLogRecPtr;

        xlrec.num_tuples = num_tuples;
        xlrec.procid = (*metap).hashm_procid;
        xlrec.ffactor = (*metap).hashm_ffactor;

        XLogBeginInsert();
        XLogRegisterData(
            &mut xlrec as *mut _ as *mut c_void,
            SizeOfHashInitMetaPage as c_int,
        );
        XLogRegisterBuffer(0, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

        recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_INIT_META_PAGE);

        PageSetLSN(BufferGetPage(metabuf), recptr);
    }

    num_buckets = (*metap).hashm_maxbucket + 1;

    /*
     * Release buffer lock on the metapage while we initialize buckets.
     * Otherwise, we'll be in interrupt holdoff and the CHECK_FOR_INTERRUPTS
     * won't accomplish anything.  It's a bad idea to hold buffer locks for
     * long intervals in any case, since that can block the bgwriter.
     */
    LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

    /*
     * Initialize and WAL Log the first N buckets
     */
    i = 0;
    while i < num_buckets {
        let blkno: BlockNumber;

        /* Allow interrupts, in case N is huge */
        CHECK_FOR_INTERRUPTS();

        blkno = BUCKET_TO_BLKNO(metap, i);
        buf = _hash_getnewbuf(rel, blkno, forkNum);
        _hash_initbuf(buf, (*metap).hashm_maxbucket, i, LH_BUCKET_PAGE as uint32, false);
        MarkBufferDirty(buf);

        if use_wal {
            log_newpage(
                &mut (*rel).rd_locator as *mut _ as *mut RelFileLocator,
                forkNum,
                blkno,
                BufferGetPage(buf),
                true,
            );
        }
        _hash_relbuf(rel, buf);

        i += 1;
    }

    /* Now reacquire buffer lock on metapage */
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

    /*
     * Initialize bitmap page
     */
    bitmapbuf = _hash_getnewbuf(rel, num_buckets + 1, forkNum);
    _hash_initbitmapbuffer(bitmapbuf, (*metap).hashm_bmsize, false);
    MarkBufferDirty(bitmapbuf);

    /* add the new bitmap page to the metapage's list of bitmaps */
    /* metapage already has a write lock */
    if (*metap).hashm_nmaps >= HASH_MAX_BITMAPS as uint32 {
        ereport!(
            ERROR,
            errmsg!(
                "out of overflow pages in hash index \"{}\"",
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
        // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
    }

    (*metap).hashm_mapp[(*metap).hashm_nmaps as usize] = num_buckets + 1;

    (*metap).hashm_nmaps += 1;
    MarkBufferDirty(metabuf);

    /* XLOG stuff */
    if use_wal {
        let mut xlrec: xl_hash_init_bitmap_page = core::mem::zeroed();
        let recptr: XLogRecPtr;

        xlrec.bmsize = (*metap).hashm_bmsize;

        XLogBeginInsert();
        XLogRegisterData(
            &mut xlrec as *mut _ as *mut c_void,
            SizeOfHashInitBitmapPage as c_int,
        );
        XLogRegisterBuffer(0, bitmapbuf, REGBUF_WILL_INIT);

        /*
         * This is safe only because nobody else can be modifying the index at
         * this stage; it's only visible to the transaction that is creating
         * it.
         */
        XLogRegisterBuffer(1, metabuf, REGBUF_STANDARD);

        recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_INIT_BITMAP_PAGE);

        PageSetLSN(BufferGetPage(bitmapbuf), recptr);
        PageSetLSN(BufferGetPage(metabuf), recptr);
    }

    /* all done */
    _hash_relbuf(rel, bitmapbuf);
    _hash_relbuf(rel, metabuf);

    num_buckets
}

/*
 *	_hash_init_metabuffer() -- Initialize the metadata page of a hash index.
 */
pub unsafe fn _hash_init_metabuffer(
    buf: Buffer,
    num_tuples: f64,
    procid: RegProcedure,
    ffactor: uint16,
    initpage: bool,
) {
    let metap: HashMetaPage;
    let pageopaque: HashPageOpaque;
    let page: Page;
    let dnumbuckets: f64;
    let num_buckets: uint32;
    let spare_index: uint32;
    let lshift: uint32;

    /*
     * Choose the number of initial bucket pages to match the fill factor
     * given the estimated number of tuples.  We round up the result to the
     * total number of buckets which has to be allocated before using its
     * hashm_spares element. However always force at least 2 bucket pages. The
     * upper limit is determined by considerations explained in
     * _hash_expandtable().
     */
    dnumbuckets = num_tuples / ffactor as f64;
    if dnumbuckets <= 2.0 {
        num_buckets = 2;
    } else if dnumbuckets >= 0x40000000 as f64 {
        num_buckets = 0x40000000;
    } else {
        num_buckets = _hash_get_totalbuckets(_hash_spareindex(dnumbuckets as uint32));
    }

    spare_index = _hash_spareindex(num_buckets);
    Assert!(spare_index < HASH_MAX_SPLITPOINTS);

    page = BufferGetPage(buf);
    if initpage {
        _hash_pageinit(page, BufferGetPageSize(buf));
    }

    pageopaque = HashPageGetOpaque(page);
    (*pageopaque).hasho_prevblkno = InvalidBlockNumber;
    (*pageopaque).hasho_nextblkno = InvalidBlockNumber;
    (*pageopaque).hasho_bucket = InvalidBucket;
    (*pageopaque).hasho_flag = LH_META_PAGE;
    (*pageopaque).hasho_page_id = HASHO_PAGE_ID;

    metap = HashPageGetMeta(page);

    (*metap).hashm_magic = HASH_MAGIC;
    (*metap).hashm_version = HASH_VERSION;
    (*metap).hashm_ntuples = 0.0;
    (*metap).hashm_nmaps = 0;
    (*metap).hashm_ffactor = ffactor;
    (*metap).hashm_bsize = HashGetMaxBitmapSize(page);

    /* find largest bitmap array size that will fit in page size */
    lshift = pg_leftmost_one_pos32((*metap).hashm_bsize as uint32) as uint32;
    Assert!(lshift > 0);
    (*metap).hashm_bmsize = (1u32 << lshift) as uint16;
    (*metap).hashm_bmshift = (lshift + BYTE_TO_BIT) as uint16;
    Assert!((1u32 << BMPG_SHIFT(metap)) == (BMPG_MASK(metap) + 1));

    /*
     * Label the index with its primary hash support function's OID.  This is
     * pretty useless for normal operation (in fact, hashm_procid is not used
     * anywhere), but it might be handy for forensic purposes so we keep it.
     */
    (*metap).hashm_procid = procid;

    /*
     * We initialize the index with N buckets, 0 .. N-1, occupying physical
     * blocks 1 to N.  The first freespace bitmap page is in block N+1.
     */
    (*metap).hashm_maxbucket = num_buckets - 1;

    /*
     * Set highmask as next immediate ((2 ^ x) - 1), which should be
     * sufficient to cover num_buckets.
     */
    (*metap).hashm_highmask = pg_nextpower2_32(num_buckets + 1) - 1;
    (*metap).hashm_lowmask = (*metap).hashm_highmask >> 1;

    /* MemSet(metap->hashm_spares, 0, sizeof(metap->hashm_spares)); */
    (*metap).hashm_spares.iter_mut().for_each(|x| *x = 0);
    /* MemSet(metap->hashm_mapp, 0, sizeof(metap->hashm_mapp)); */
    (*metap).hashm_mapp.iter_mut().for_each(|x| *x = 0);

    /* Set up mapping for one spare page after the initial splitpoints */
    (*metap).hashm_spares[spare_index as usize] = 1;
    (*metap).hashm_ovflpoint = spare_index;
    (*metap).hashm_firstfree = 0;

    /*
     * Set pd_lower just past the end of the metadata.  This is essential,
     * because without doing so, metadata will be lost if xlog.c compresses
     * the page.
     */
    (*(page as PageHeader)).pd_lower = (((metap as *mut c_char)
        .add(core::mem::size_of::<HashMetaPageData>()))
    .offset_from(page as *mut c_char)) as u16;
}

/*
 *	_hash_pageinit() -- Initialize a new hash index page.
 */
pub unsafe fn _hash_pageinit(page: Page, size: Size) {
    PageInit(page, size, core::mem::size_of::<HashPageOpaqueData>());
}

/*
 * Attempt to expand the hash table by creating one new bucket.
 *
 * This will silently do nothing if we don't get cleanup lock on old or
 * new bucket.
 *
 * Complete the pending splits and remove the tuples from old bucket,
 * if there are any left over from the previous split.
 *
 * The caller must hold a pin, but no lock, on the metapage buffer.
 * The buffer is returned in the same state.
 */
pub unsafe fn _hash_expandtable(rel: Relation, metabuf: Buffer) {
    let mut metap: HashMetaPage;
    let mut old_bucket: Bucket;
    let mut new_bucket: Bucket;
    let mut spare_ndx: uint32;
    let mut start_oblkno: BlockNumber;
    let mut start_nblkno: BlockNumber;
    let mut buf_nblkno: Buffer;
    let mut buf_oblkno: Buffer;
    let mut opage: Page;
    let mut npage: Page;
    let mut oopaque: HashPageOpaque;
    let mut nopaque: HashPageOpaque;
    let mut maxbucket: uint32;
    let mut highmask: uint32;
    let mut lowmask: uint32;
    let mut metap_update_masks: bool = false;
    let mut metap_update_splitpoint: bool = false;

    'restart_expand: loop {
        /*
         * Write-lock the meta page.  It used to be necessary to acquire a
         * heavyweight lock to begin a split, but that is no longer required.
         */
        LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

        _hash_checkpage(rel, metabuf, LH_META_PAGE as c_int);
        metap = HashPageGetMeta(BufferGetPage(metabuf));

        'fail: {
            /*
             * Check to see if split is still needed; someone else might have
             * already done one while we waited for the lock.
             *
             * Make sure this stays in sync with _hash_doinsert()
             */
            if (*metap).hashm_ntuples
                <= (*metap).hashm_ffactor as f64 * ((*metap).hashm_maxbucket + 1) as f64
            {
                break 'fail;
            }

            /*
             * Can't split anymore if maxbucket has reached its maximum possible
             * value.
             *
             * Ideally we'd allow bucket numbers up to UINT_MAX-1 (no higher
             * because the calculation maxbucket+1 mustn't overflow).  Currently
             * we restrict to half that to prevent failure of pg_ceil_log2_32()
             * and insufficient space in hashm_spares[].  It's moot anyway
             * because an index with 2^32 buckets would certainly overflow
             * BlockNumber and hence _hash_alloc_buckets() would fail, but if we
             * supported buckets smaller than a disk block then this would be an
             * independent constraint.
             *
             * If you change this, see also the maximum initial number of
             * buckets in _hash_init().
             */
            if (*metap).hashm_maxbucket >= 0x7FFFFFFE_u32 {
                break 'fail;
            }

            /*
             * Determine which bucket is to be split, and attempt to take cleanup
             * lock on the old bucket.  If we can't get the lock, give up.
             *
             * The cleanup lock protects us not only against other backends, but
             * against our own backend as well.
             *
             * The cleanup lock is mainly to protect the split from concurrent
             * inserts. See src/backend/access/hash/README, Lock Definitions for
             * further details.  Due to this locking restriction, if there is any
             * pending scan, the split will give up which is not good, but
             * harmless.
             */
            new_bucket = (*metap).hashm_maxbucket + 1;

            old_bucket = new_bucket & (*metap).hashm_lowmask;

            start_oblkno = BUCKET_TO_BLKNO(metap, old_bucket);

            buf_oblkno =
                _hash_getbuf_with_condlock_cleanup(rel, start_oblkno, LH_BUCKET_PAGE as c_int);
            if buf_oblkno == 0 {
                break 'fail;
            }

            opage = BufferGetPage(buf_oblkno);
            oopaque = HashPageGetOpaque(opage);

            /*
             * We want to finish the split from a bucket as there is no apparent
             * benefit by not doing so and it will make the code complicated to
             * finish the split that involves multiple buckets considering the
             * case where new split also fails.  We don't need to consider the
             * new bucket for completing the split here as it is not possible
             * that a re-split of new bucket starts when there is still a pending
             * split from old bucket.
             */
            if H_BUCKET_BEING_SPLIT(oopaque) {
                /*
                 * Copy bucket mapping info now; refer the comment in code below
                 * where we copy this information before calling
                 * _hash_splitbucket to see why this is okay.
                 */
                maxbucket = (*metap).hashm_maxbucket;
                highmask = (*metap).hashm_highmask;
                lowmask = (*metap).hashm_lowmask;

                /*
                 * Release the lock on metapage and old_bucket, before completing
                 * the split.
                 */
                LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
                LockBuffer(buf_oblkno, BUFFER_LOCK_UNLOCK);

                _hash_finish_split(
                    rel, metabuf, buf_oblkno, old_bucket, maxbucket, highmask, lowmask,
                );

                /* release the pin on old buffer and retry for expand. */
                _hash_dropbuf(rel, buf_oblkno);

                continue 'restart_expand;
            }

            /*
             * Clean the tuples remained from the previous split.  This operation
             * requires cleanup lock and we already have one on the old bucket,
             * so let's do it. We also don't want to allow further splits from
             * the bucket till the garbage of previous split is cleaned.  This
             * has two advantages; first, it helps in avoiding the bloat due to
             * garbage and second is, during cleanup of bucket, we are always
             * sure that the garbage tuples belong to most recently split
             * bucket.  On the contrary, if we allow cleanup of bucket after meta
             * page is updated to indicate the new split and before the actual
             * split, the cleanup operation won't be able to decide whether the
             * tuple has been moved to the newly created bucket and ended up
             * deleting such tuples.
             */
            if H_NEEDS_SPLIT_CLEANUP(oopaque) {
                /*
                 * Copy bucket mapping info now; refer to the comment in code
                 * below where we copy this information before calling
                 * _hash_splitbucket to see why this is okay.
                 */
                maxbucket = (*metap).hashm_maxbucket;
                highmask = (*metap).hashm_highmask;
                lowmask = (*metap).hashm_lowmask;

                /* Release the metapage lock. */
                LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

                hashbucketcleanup(
                    rel,
                    old_bucket,
                    buf_oblkno,
                    start_oblkno,
                    core::ptr::null_mut(),
                    maxbucket,
                    highmask,
                    lowmask,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                    true,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                );

                _hash_dropbuf(rel, buf_oblkno);

                continue 'restart_expand;
            }

            /*
             * There shouldn't be any active scan on new bucket.
             *
             * Note: it is safe to compute the new bucket's blkno here, even
             * though we may still need to update the BUCKET_TO_BLKNO mapping.
             * This is because the current value of
             * hashm_spares[hashm_ovflpoint] correctly shows where we are going
             * to put a new splitpoint's worth of buckets.
             */
            start_nblkno = BUCKET_TO_BLKNO(metap, new_bucket);

            /*
             * If the split point is increasing we need to allocate a new batch
             * of bucket pages.
             */
            spare_ndx = _hash_spareindex(new_bucket + 1);
            if spare_ndx > (*metap).hashm_ovflpoint {
                let buckets_to_add: uint32;

                Assert!(spare_ndx == (*metap).hashm_ovflpoint + 1);

                /*
                 * We treat allocation of buckets as a separate WAL-logged
                 * action. Even if we fail after this operation, won't leak
                 * bucket pages; rather, the next split will consume this space.
                 * In any case, even without failure we don't use all the space
                 * in one split operation.
                 */
                buckets_to_add = _hash_get_totalbuckets(spare_ndx) - new_bucket;
                if !_hash_alloc_buckets(rel, start_nblkno, buckets_to_add) {
                    /* can't split due to BlockNumber overflow */
                    _hash_relbuf(rel, buf_oblkno);
                    break 'fail;
                }
            }

            /*
             * Physically allocate the new bucket's primary page.  We want to do
             * this before changing the metapage's mapping info, in case we can't
             * get the disk space.
             *
             * XXX It doesn't make sense to call _hash_getnewbuf first, zeroing
             * the buffer, and then only afterwards check whether we have a
             * cleanup lock. However, since no scan can be accessing the buffer
             * yet, any concurrent accesses will just be from processes like the
             * bgwriter or checkpointer which don't care about its contents, so
             * it doesn't really matter.
             */
            buf_nblkno = _hash_getnewbuf(rel, start_nblkno, MAIN_FORKNUM);
            if !IsBufferCleanupOK(buf_nblkno) {
                _hash_relbuf(rel, buf_oblkno);
                _hash_relbuf(rel, buf_nblkno);
                break 'fail;
            }

            /*
             * Since we are scribbling on the pages in the shared buffers,
             * establish a critical section.  Any failure in this next code
             * leaves us with a big problem: the metapage is effectively corrupt
             * but could get written back to disk.
             */
            START_CRIT_SECTION();

            /*
             * Okay to proceed with split.  Update the metapage bucket mapping
             * info.
             */
            (*metap).hashm_maxbucket = new_bucket;

            if new_bucket > (*metap).hashm_highmask {
                /* Starting a new doubling */
                (*metap).hashm_lowmask = (*metap).hashm_highmask;
                (*metap).hashm_highmask = new_bucket | (*metap).hashm_lowmask;
                metap_update_masks = true;
            }

            /*
             * If the split point is increasing we need to adjust the
             * hashm_spares[] array and hashm_ovflpoint so that future overflow
             * pages will be created beyond this new batch of bucket pages.
             */
            if spare_ndx > (*metap).hashm_ovflpoint {
                (*metap).hashm_spares[spare_ndx as usize] =
                    (*metap).hashm_spares[(*metap).hashm_ovflpoint as usize];
                (*metap).hashm_ovflpoint = spare_ndx;
                metap_update_splitpoint = true;
            }

            MarkBufferDirty(metabuf);

            /*
             * Copy bucket mapping info now; this saves re-accessing the meta
             * page inside _hash_splitbucket's inner loop.  Note that once we
             * drop the split lock, other splits could begin, so these values
             * might be out of date before _hash_splitbucket finishes.  That's
             * okay, since all it needs is to tell which of these two buckets to
             * map hashkeys into.
             */
            maxbucket = (*metap).hashm_maxbucket;
            highmask = (*metap).hashm_highmask;
            lowmask = (*metap).hashm_lowmask;

            opage = BufferGetPage(buf_oblkno);
            oopaque = HashPageGetOpaque(opage);

            /*
             * Mark the old bucket to indicate that split is in progress.  (At
             * operation end, we will clear the split-in-progress flag.)  Also,
             * for a primary bucket page, hasho_prevblkno stores the number of
             * buckets that existed as of the last split, so we must update that
             * value here.
             */
            (*oopaque).hasho_flag |= LH_BUCKET_BEING_SPLIT;
            (*oopaque).hasho_prevblkno = maxbucket;

            MarkBufferDirty(buf_oblkno);

            npage = BufferGetPage(buf_nblkno);

            /*
             * initialize the new bucket's primary page and mark it to indicate
             * that split is in progress.
             */
            nopaque = HashPageGetOpaque(npage);
            (*nopaque).hasho_prevblkno = maxbucket;
            (*nopaque).hasho_nextblkno = InvalidBlockNumber;
            (*nopaque).hasho_bucket = new_bucket;
            (*nopaque).hasho_flag = LH_BUCKET_PAGE | LH_BUCKET_BEING_POPULATED;
            (*nopaque).hasho_page_id = HASHO_PAGE_ID;

            MarkBufferDirty(buf_nblkno);

            /* XLOG stuff */
            if RelationNeedsWAL(rel) {
                let mut xlrec: xl_hash_split_allocate_page = core::mem::zeroed();
                let recptr: XLogRecPtr;

                xlrec.new_bucket = maxbucket;
                xlrec.old_bucket_flag = (*oopaque).hasho_flag;
                xlrec.new_bucket_flag = (*nopaque).hasho_flag;
                xlrec.flags = 0;

                XLogBeginInsert();

                XLogRegisterBuffer(0, buf_oblkno, REGBUF_STANDARD);
                XLogRegisterBuffer(1, buf_nblkno, REGBUF_WILL_INIT);
                XLogRegisterBuffer(2, metabuf, REGBUF_STANDARD);

                if metap_update_masks {
                    xlrec.flags |= XLH_SPLIT_META_UPDATE_MASKS as u8;
                    XLogRegisterBufData(
                        2,
                        &mut (*metap).hashm_lowmask as *mut _ as *mut c_void,
                        core::mem::size_of::<uint32>() as c_int,
                    );
                    XLogRegisterBufData(
                        2,
                        &mut (*metap).hashm_highmask as *mut _ as *mut c_void,
                        core::mem::size_of::<uint32>() as c_int,
                    );
                }

                if metap_update_splitpoint {
                    xlrec.flags |= XLH_SPLIT_META_UPDATE_SPLITPOINT as u8;
                    XLogRegisterBufData(
                        2,
                        &mut (*metap).hashm_ovflpoint as *mut _ as *mut c_void,
                        core::mem::size_of::<uint32>() as c_int,
                    );
                    XLogRegisterBufData(
                        2,
                        &mut (*metap).hashm_spares[(*metap).hashm_ovflpoint as usize] as *mut _
                            as *mut c_void,
                        core::mem::size_of::<uint32>() as c_int,
                    );
                }

                XLogRegisterData(
                    &mut xlrec as *mut _ as *mut c_void,
                    SizeOfHashSplitAllocPage as c_int,
                );

                recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_SPLIT_ALLOCATE_PAGE);

                PageSetLSN(BufferGetPage(buf_oblkno), recptr);
                PageSetLSN(BufferGetPage(buf_nblkno), recptr);
                PageSetLSN(BufferGetPage(metabuf), recptr);
            }

            END_CRIT_SECTION();

            /* drop lock, but keep pin */
            LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

            /* Relocate records to the new bucket */
            _hash_splitbucket(
                rel,
                metabuf,
                old_bucket,
                new_bucket,
                buf_oblkno,
                buf_nblkno,
                core::ptr::null_mut(),
                maxbucket,
                highmask,
                lowmask,
            );

            /* all done, now release the pins on primary buckets. */
            _hash_dropbuf(rel, buf_oblkno);
            _hash_dropbuf(rel, buf_nblkno);

            return;
        } // 'fail

        /* Here if decide not to split or fail to acquire old bucket lock */

        /* We didn't write the metapage, so just drop lock */
        LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
        return;
    }
}

/*
 * _hash_alloc_buckets -- allocate a new splitpoint's worth of bucket pages
 *
 * This does not need to initialize the new bucket pages; we'll do that as
 * each one is used by _hash_expandtable().  But we have to extend the logical
 * EOF to the end of the splitpoint; this keeps smgr's idea of the EOF in
 * sync with ours, so that we don't get complaints from smgr.
 *
 * We do this by writing a page of zeroes at the end of the splitpoint range.
 * We expect that the filesystem will ensure that the intervening pages read
 * as zeroes too.  On many filesystems this "hole" will not be allocated
 * immediately, which means that the index file may end up more fragmented
 * than if we forced it all to be allocated now; but since we don't scan
 * hash indexes sequentially anyway, that probably doesn't matter.
 *
 * XXX It's annoying that this code is executed with the metapage lock held.
 * We need to interlock against _hash_addovflpage() adding a new overflow page
 * concurrently, but it'd likely be better to use LockRelationForExtension
 * for the purpose.  OTOH, adding a splitpoint is a very infrequent operation,
 * so it may not be worth worrying about.
 *
 * Returns true if successful, or false if allocation failed due to
 * BlockNumber overflow.
 */
unsafe fn _hash_alloc_buckets(rel: Relation, firstblock: BlockNumber, nblocks: uint32) -> bool {
    let lastblock: BlockNumber;
    let mut zerobuf: PGIOAlignedBlock = core::mem::zeroed();
    let page: Page;
    let ovflopaque: HashPageOpaque;

    lastblock = firstblock + nblocks - 1;

    /*
     * Check for overflow in block number calculation; if so, we cannot extend
     * the index anymore.
     */
    if lastblock < firstblock || lastblock == InvalidBlockNumber {
        return false;
    }

    page = zerobuf.data.as_mut_ptr() as Page;

    /*
     * Initialize the page.  Just zeroing the page won't work; see
     * _hash_freeovflpage for similar usage.  We take care to make the special
     * space valid for the benefit of tools such as pageinspect.
     */
    _hash_pageinit(page, BLCKSZ as Size);

    ovflopaque = HashPageGetOpaque(page);

    (*ovflopaque).hasho_prevblkno = InvalidBlockNumber;
    (*ovflopaque).hasho_nextblkno = InvalidBlockNumber;
    (*ovflopaque).hasho_bucket = InvalidBucket;
    (*ovflopaque).hasho_flag = LH_UNUSED_PAGE;
    (*ovflopaque).hasho_page_id = HASHO_PAGE_ID;

    if RelationNeedsWAL(rel) {
        log_newpage(
            &mut (*rel).rd_locator as *mut _ as *mut RelFileLocator,
            MAIN_FORKNUM,
            lastblock,
            zerobuf.data.as_mut_ptr() as Page,
            true,
        );
    }

    PageSetChecksumInplace(page, lastblock);
    smgrextend(
        RelationGetSmgr(rel),
        MAIN_FORKNUM,
        lastblock,
        zerobuf.data.as_ptr(),
        false,
    );

    true
}

/*
 * _hash_splitbucket -- split 'obucket' into 'obucket' and 'nbucket'
 *
 * This routine is used to partition the tuples between old and new bucket and
 * is used to finish the incomplete split operations.  To finish the previously
 * interrupted split operation, the caller needs to fill htab.  If htab is set,
 * then we skip the movement of tuples that exists in htab, otherwise NULL
 * value of htab indicates movement of all the tuples that belong to the new
 * bucket.
 *
 * We are splitting a bucket that consists of a base bucket page and zero
 * or more overflow (bucket chain) pages.  We must relocate tuples that
 * belong in the new bucket.
 *
 * The caller must hold cleanup locks on both buckets to ensure that
 * no one else is trying to access them (see README).
 *
 * The caller must hold a pin, but no lock, on the metapage buffer.
 * The buffer is returned in the same state.  (The metapage is only
 * touched if it becomes necessary to add or remove overflow pages.)
 *
 * Split needs to retain pin on primary bucket pages of both old and new
 * buckets till end of operation.  This is to prevent vacuum from starting
 * while a split is in progress.
 *
 * In addition, the caller must have created the new bucket's base page,
 * which is passed in buffer nbuf, pinned and write-locked.  The lock will be
 * released here and pin must be released by the caller.  (The API is set up
 * this way because we must do _hash_getnewbuf() before releasing the metapage
 * write lock.  So instead of passing the new bucket's start block number, we
 * pass an actual buffer.)
 */
unsafe fn _hash_splitbucket(
    rel: Relation,
    metabuf: Buffer,
    obucket: Bucket,
    nbucket: Bucket,
    obuf: Buffer,
    nbuf: Buffer,
    htab: *mut HTAB,
    maxbucket: uint32,
    highmask: uint32,
    lowmask: uint32,
) {
    let mut obuf = obuf;
    let mut nbuf = nbuf;
    let bucket_obuf: Buffer;
    let bucket_nbuf: Buffer;
    let mut opage: Page;
    let mut npage: Page;
    let mut oopaque: HashPageOpaque;
    let mut nopaque: HashPageOpaque;
    let mut itup_offsets: [OffsetNumber; MaxIndexTuplesPerPage] =
        [0; MaxIndexTuplesPerPage];
    let mut itups: [IndexTuple; MaxIndexTuplesPerPage] =
        [core::ptr::null_mut(); MaxIndexTuplesPerPage];
    let mut all_tups_size: Size = 0;
    let mut i: c_int;
    let mut nitups: uint16 = 0;

    bucket_obuf = obuf;
    opage = BufferGetPage(obuf);
    oopaque = HashPageGetOpaque(opage);

    bucket_nbuf = nbuf;
    npage = BufferGetPage(nbuf);
    nopaque = HashPageGetOpaque(npage);

    /* Copy the predicate locks from old bucket to new bucket. */
    PredicateLockPageSplit(
        rel,
        BufferGetBlockNumber(bucket_obuf),
        BufferGetBlockNumber(bucket_nbuf),
    );

    /*
     * Partition the tuples in the old bucket between the old bucket and the
     * new bucket, advancing along the old bucket's overflow bucket chain and
     * adding overflow pages to the new bucket as needed.  Outer loop iterates
     * once per page in old bucket.
     */
    loop {
        let oblkno: BlockNumber;
        let mut ooffnum: OffsetNumber;
        let omaxoffnum: OffsetNumber;

        /* Scan each tuple in old page */
        omaxoffnum = PageGetMaxOffsetNumber(opage);
        ooffnum = FirstOffsetNumber;
        while ooffnum <= omaxoffnum {
            let itup: IndexTuple;
            let mut itemsz: Size;
            let bucket: Bucket;
            let mut found: bool = false;

            /* skip dead tuples */
            if ItemIdIsDead(PageGetItemId(opage, ooffnum)) {
                ooffnum = OffsetNumberNext(ooffnum);
                continue;
            }

            /*
             * Before inserting a tuple, probe the hash table containing TIDs
             * of tuples belonging to new bucket, if we find a match, then
             * skip that tuple, else fetch the item's hash key (conveniently
             * stored in the item) and determine which bucket it now belongs
             * in.
             */
            itup =
                PageGetItem(opage, PageGetItemId(opage, ooffnum)) as IndexTuple;

            if !htab.is_null() {
                hash_search(
                    htab,
                    &mut (*itup).t_tid as *mut _ as *mut c_void,
                    HASH_FIND,
                    &mut found,
                );
            }

            if found {
                ooffnum = OffsetNumberNext(ooffnum);
                continue;
            }

            bucket = _hash_hashkey2bucket(
                _hash_get_indextuple_hashkey(itup),
                maxbucket,
                highmask,
                lowmask,
            );

            if bucket == nbucket {
                let new_itup: IndexTuple;

                /*
                 * make a copy of index tuple as we have to scribble on it.
                 */
                new_itup = CopyIndexTuple(itup);

                /*
                 * mark the index tuple as moved by split, such tuples are
                 * skipped by scan if there is split in progress for a bucket.
                 */
                (*new_itup).t_info |= INDEX_MOVED_BY_SPLIT_MASK;

                /*
                 * insert the tuple into the new bucket.  if it doesn't fit on
                 * the current page in the new bucket, we must allocate a new
                 * overflow page and place the tuple on that page instead.
                 */
                itemsz = IndexTupleSize(new_itup);
                itemsz = MAXALIGN(itemsz as usize) as Size;

                if PageGetFreeSpaceForMultipleTuples(npage, (nitups + 1) as c_int)
                    < (all_tups_size + itemsz)
                {
                    /*
                     * Change the shared buffer state in critical section,
                     * otherwise any error could make it unrecoverable.
                     */
                    START_CRIT_SECTION();

                    _hash_pgaddmultitup(
                        rel,
                        nbuf,
                        itups.as_mut_ptr(),
                        itup_offsets.as_mut_ptr(),
                        nitups,
                    );
                    MarkBufferDirty(nbuf);
                    /* log the split operation before releasing the lock */
                    log_split_page(rel, nbuf);

                    END_CRIT_SECTION();

                    /* drop lock, but keep pin */
                    LockBuffer(nbuf, BUFFER_LOCK_UNLOCK);

                    /* be tidy */
                    i = 0;
                    while i < nitups as c_int {
                        pfree(itups[i as usize] as *mut c_void);
                        i += 1;
                    }
                    nitups = 0;
                    all_tups_size = 0;

                    /* chain to a new overflow page */
                    nbuf = _hash_addovflpage(rel, metabuf, nbuf, nbuf == bucket_nbuf);
                    npage = BufferGetPage(nbuf);
                    nopaque = HashPageGetOpaque(npage);
                }

                itups[nitups as usize] = new_itup;
                nitups += 1;
                all_tups_size += itemsz;
            } else {
                /*
                 * the tuple stays on this page, so nothing to do.
                 */
                Assert!(bucket == obucket);
            }

            ooffnum = OffsetNumberNext(ooffnum);
        }

        oblkno = (*oopaque).hasho_nextblkno;

        /* retain the pin on the old primary bucket */
        if obuf == bucket_obuf {
            LockBuffer(obuf, BUFFER_LOCK_UNLOCK);
        } else {
            _hash_relbuf(rel, obuf);
        }

        /* Exit loop if no more overflow pages in old bucket */
        if !BlockNumberIsValid(oblkno) {
            /*
             * Change the shared buffer state in critical section, otherwise
             * any error could make it unrecoverable.
             */
            START_CRIT_SECTION();

            _hash_pgaddmultitup(
                rel,
                nbuf,
                itups.as_mut_ptr(),
                itup_offsets.as_mut_ptr(),
                nitups,
            );
            MarkBufferDirty(nbuf);
            /* log the split operation before releasing the lock */
            log_split_page(rel, nbuf);

            END_CRIT_SECTION();

            if nbuf == bucket_nbuf {
                LockBuffer(nbuf, BUFFER_LOCK_UNLOCK);
            } else {
                _hash_relbuf(rel, nbuf);
            }

            /* be tidy */
            i = 0;
            while i < nitups as c_int {
                pfree(itups[i as usize] as *mut c_void);
                i += 1;
            }
            break;
        }

        /* Else, advance to next old page */
        obuf = _hash_getbuf(rel, oblkno, HASH_READ, LH_OVERFLOW_PAGE as c_int);
        opage = BufferGetPage(obuf);
        oopaque = HashPageGetOpaque(opage);
    }

    /*
     * We're at the end of the old bucket chain, so we're done partitioning
     * the tuples.  Mark the old and new buckets to indicate split is
     * finished.
     *
     * To avoid deadlocks due to locking order of buckets, first lock the old
     * bucket and then the new bucket.
     */
    LockBuffer(bucket_obuf, BUFFER_LOCK_EXCLUSIVE);
    opage = BufferGetPage(bucket_obuf);
    oopaque = HashPageGetOpaque(opage);

    LockBuffer(bucket_nbuf, BUFFER_LOCK_EXCLUSIVE);
    npage = BufferGetPage(bucket_nbuf);
    nopaque = HashPageGetOpaque(npage);

    START_CRIT_SECTION();

    (*oopaque).hasho_flag &= !LH_BUCKET_BEING_SPLIT;
    (*nopaque).hasho_flag &= !LH_BUCKET_BEING_POPULATED;

    /*
     * After the split is finished, mark the old bucket to indicate that it
     * contains deletable tuples.  We will clear split-cleanup flag after
     * deleting such tuples either at the end of split or at the next split
     * from old bucket or at the time of vacuum.
     */
    (*oopaque).hasho_flag |= LH_BUCKET_NEEDS_SPLIT_CLEANUP;

    /*
     * now write the buffers, here we don't release the locks as caller is
     * responsible to release locks.
     */
    MarkBufferDirty(bucket_obuf);
    MarkBufferDirty(bucket_nbuf);

    if RelationNeedsWAL(rel) {
        let recptr: XLogRecPtr;
        let mut xlrec: xl_hash_split_complete = core::mem::zeroed();

        xlrec.old_bucket_flag = (*oopaque).hasho_flag;
        xlrec.new_bucket_flag = (*nopaque).hasho_flag;

        XLogBeginInsert();

        XLogRegisterData(
            &mut xlrec as *mut _ as *mut c_void,
            SizeOfHashSplitComplete as c_int,
        );

        XLogRegisterBuffer(0, bucket_obuf, REGBUF_STANDARD);
        XLogRegisterBuffer(1, bucket_nbuf, REGBUF_STANDARD);

        recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_SPLIT_COMPLETE);

        PageSetLSN(BufferGetPage(bucket_obuf), recptr);
        PageSetLSN(BufferGetPage(bucket_nbuf), recptr);
    }

    END_CRIT_SECTION();

    /*
     * If possible, clean up the old bucket.  We might not be able to do this
     * if someone else has a pin on it, but if not then we can go ahead.  This
     * isn't absolutely necessary, but it reduces bloat; if we don't do it
     * now, VACUUM will do it eventually, but maybe not until new overflow
     * pages have been allocated.  Note that there's no need to clean up the
     * new bucket.
     */
    if IsBufferCleanupOK(bucket_obuf) {
        LockBuffer(bucket_nbuf, BUFFER_LOCK_UNLOCK);
        hashbucketcleanup(
            rel,
            obucket,
            bucket_obuf,
            BufferGetBlockNumber(bucket_obuf),
            core::ptr::null_mut(),
            maxbucket,
            highmask,
            lowmask,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            true,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        );
    } else {
        LockBuffer(bucket_nbuf, BUFFER_LOCK_UNLOCK);
        LockBuffer(bucket_obuf, BUFFER_LOCK_UNLOCK);
    }
}

/*
 *	_hash_finish_split() -- Finish the previously interrupted split operation
 *
 * To complete the split operation, we form the hash table of TIDs in new
 * bucket which is then used by split operation to skip tuples that are
 * already moved before the split operation was previously interrupted.
 *
 * The caller must hold a pin, but no lock, on the metapage and old bucket's
 * primary page buffer.  The buffers are returned in the same state.  (The
 * metapage is only touched if it becomes necessary to add or remove overflow
 * pages.)
 */
pub unsafe fn _hash_finish_split(
    rel: Relation,
    metabuf: Buffer,
    obuf: Buffer,
    obucket: Bucket,
    maxbucket: uint32,
    highmask: uint32,
    lowmask: uint32,
) {
    let mut hash_ctl: HASHCTL = core::mem::zeroed();
    let tidhtab: *mut HTAB;
    let mut bucket_nbuf: Buffer = InvalidBuffer as Buffer;
    let mut nbuf: Buffer;
    let mut npage: Page;
    let mut nblkno: BlockNumber;
    let bucket_nblkno: BlockNumber;
    let mut npageopaque: HashPageOpaque;
    let nbucket: Bucket;
    let mut found: bool = false;

    /* Initialize hash tables used to track TIDs */
    hash_ctl.keysize = core::mem::size_of::<ItemPointerData>() as Size;
    hash_ctl.entrysize = core::mem::size_of::<ItemPointerData>() as Size;
    hash_ctl.hcxt = crate::utils::mmgr::mcxt::CurrentMemoryContext;

    tidhtab = hash_create(
        c"bucket ctids".as_ptr(),
        256, /* arbitrary initial size */
        &hash_ctl,
        (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT) as c_int,
    );

    nblkno = _hash_get_newblock_from_oldbucket(rel, obucket);
    bucket_nblkno = nblkno;

    /*
     * Scan the new bucket and build hash table of TIDs
     */
    loop {
        let mut noffnum: OffsetNumber;
        let nmaxoffnum: OffsetNumber;

        nbuf = _hash_getbuf(
            rel,
            nblkno,
            HASH_READ,
            (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as c_int,
        );

        /* remember the primary bucket buffer to acquire cleanup lock on it. */
        if nblkno == bucket_nblkno {
            bucket_nbuf = nbuf;
        }

        npage = BufferGetPage(nbuf);
        npageopaque = HashPageGetOpaque(npage);

        /* Scan each tuple in new page */
        nmaxoffnum = PageGetMaxOffsetNumber(npage);
        noffnum = FirstOffsetNumber;
        while noffnum <= nmaxoffnum {
            let itup: IndexTuple;

            /* Fetch the item's TID and insert it in hash table. */
            itup =
                PageGetItem(npage, PageGetItemId(npage, noffnum)) as IndexTuple;

            hash_search(
                tidhtab,
                &mut (*itup).t_tid as *mut _ as *mut c_void,
                HASH_ENTER,
                &mut found,
            );

            Assert!(!found);

            noffnum = OffsetNumberNext(noffnum);
        }

        nblkno = (*npageopaque).hasho_nextblkno;

        /*
         * release our write lock without modifying buffer and ensure to
         * retain the pin on primary bucket.
         */
        if nbuf == bucket_nbuf {
            LockBuffer(nbuf, BUFFER_LOCK_UNLOCK);
        } else {
            _hash_relbuf(rel, nbuf);
        }

        /* Exit loop if no more overflow pages in new bucket */
        if !BlockNumberIsValid(nblkno) {
            break;
        }
    }

    /*
     * Conditionally get the cleanup lock on old and new buckets to perform
     * the split operation.  If we don't get the cleanup locks, silently give
     * up and next insertion on old bucket will try again to complete the
     * split.
     */
    if !ConditionalLockBufferForCleanup(obuf) {
        hash_destroy(tidhtab);
        return;
    }
    if !ConditionalLockBufferForCleanup(bucket_nbuf) {
        LockBuffer(obuf, BUFFER_LOCK_UNLOCK);
        hash_destroy(tidhtab);
        return;
    }

    npage = BufferGetPage(bucket_nbuf);
    npageopaque = HashPageGetOpaque(npage);
    nbucket = (*npageopaque).hasho_bucket;

    _hash_splitbucket(
        rel, metabuf, obucket, nbucket, obuf, bucket_nbuf, tidhtab, maxbucket, highmask, lowmask,
    );

    _hash_dropbuf(rel, bucket_nbuf);
    hash_destroy(tidhtab);
}

/*
 *	log_split_page() -- Log the split operation
 *
 *	We log the split operation when the new page in new bucket gets full,
 *	so we log the entire page.
 *
 *	'buf' must be locked by the caller which is also responsible for unlocking
 *	it.
 */
unsafe fn log_split_page(rel: Relation, buf: Buffer) {
    if RelationNeedsWAL(rel) {
        let recptr: XLogRecPtr;

        XLogBeginInsert();

        XLogRegisterBuffer(0, buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

        recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_SPLIT_PAGE);

        PageSetLSN(BufferGetPage(buf), recptr);
    }
}

/*
 *	_hash_getcachedmetap() -- Returns cached metapage data.
 *
 *	If metabuf is not InvalidBuffer, caller must hold a pin, but no lock, on
 *	the metapage.  If not set, we'll set it before returning if we have to
 *	refresh the cache, and return with a pin but no lock on it; caller is
 *	responsible for releasing the pin.
 *
 *	We refresh the cache if it's not initialized yet or force_refresh is true.
 */
pub unsafe fn _hash_getcachedmetap(
    rel: Relation,
    metabuf: *mut Buffer,
    force_refresh: bool,
) -> HashMetaPage {
    let page: Page;

    Assert!(!metabuf.is_null());
    if force_refresh || (*rel).rd_amcache.is_null() {
        let mut cache: *mut c_char = core::ptr::null_mut();

        /*
         * It's important that we don't set rd_amcache to an invalid value.
         * Either MemoryContextAlloc or _hash_getbuf could fail, so don't
         * install a pointer to the newly-allocated storage in the actual
         * relcache entry until both have succeeded.
         */
        if (*rel).rd_amcache.is_null() {
            cache = MemoryContextAlloc(
                (*rel).rd_indexcxt as MemoryContext,
                core::mem::size_of::<HashMetaPageData>() as Size,
            ) as *mut c_char;
        }

        /* Read the metapage. */
        if BufferIsValid(*metabuf) {
            LockBuffer(*metabuf, BUFFER_LOCK_SHARE);
        } else {
            *metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_READ, LH_META_PAGE as c_int);
        }
        page = BufferGetPage(*metabuf);

        /* Populate the cache. */
        if (*rel).rd_amcache.is_null() {
            (*rel).rd_amcache = cache as *mut c_void;
        }
        core::ptr::copy_nonoverlapping(
            HashPageGetMeta(page) as *const u8,
            (*rel).rd_amcache as *mut u8,
            core::mem::size_of::<HashMetaPageData>(),
        );

        /* Release metapage lock, but keep the pin. */
        LockBuffer(*metabuf, BUFFER_LOCK_UNLOCK);
    }

    (*rel).rd_amcache as HashMetaPage
}

/*
 *	_hash_getbucketbuf_from_hashkey() -- Get the bucket's buffer for the given
 *										 hashkey.
 *
 *	Bucket pages do not move or get removed once they are allocated. This give
 *	us an opportunity to use the previously saved metapage contents to reach
 *	the target bucket buffer, instead of reading from the metapage every time.
 *	This saves one buffer access every time we want to reach the target bucket
 *	buffer, which is very helpful savings in bufmgr traffic and contention.
 *
 *	The access type parameter (HASH_READ or HASH_WRITE) indicates whether the
 *	bucket buffer has to be locked for reading or writing.
 *
 *	The out parameter cachedmetap is set with metapage contents used for
 *	hashkey to bucket buffer mapping. Some callers need this info to reach the
 *	old bucket in case of bucket split, see _hash_doinsert().
 */
pub unsafe fn _hash_getbucketbuf_from_hashkey(
    rel: Relation,
    hashkey: uint32,
    access: c_int,
    cachedmetap: *mut HashMetaPage,
) -> Buffer {
    let mut metap: HashMetaPage;
    let mut buf: Buffer;
    let mut metabuf: Buffer = InvalidBuffer as Buffer;
    let mut page: Page;
    let mut bucket: Bucket;
    let mut blkno: BlockNumber;
    let mut opaque: HashPageOpaque;

    /* We read from target bucket buffer, hence locking is must. */
    Assert!(access == HASH_READ || access == HASH_WRITE);

    metap = _hash_getcachedmetap(rel, &mut metabuf, false);
    Assert!(!metap.is_null());

    /*
     * Loop until we get a lock on the correct target bucket.
     */
    loop {
        /*
         * Compute the target bucket number, and convert to block number.
         */
        bucket = _hash_hashkey2bucket(
            hashkey,
            (*metap).hashm_maxbucket,
            (*metap).hashm_highmask,
            (*metap).hashm_lowmask,
        );

        blkno = BUCKET_TO_BLKNO(metap, bucket);

        /* Fetch the primary bucket page for the bucket */
        buf = _hash_getbuf(rel, blkno, access, LH_BUCKET_PAGE as c_int);
        page = BufferGetPage(buf);
        opaque = HashPageGetOpaque(page);
        Assert!((*opaque).hasho_bucket == bucket);
        Assert!((*opaque).hasho_prevblkno != InvalidBlockNumber);

        /*
         * If this bucket hasn't been split, we're done.
         */
        if (*opaque).hasho_prevblkno <= (*metap).hashm_maxbucket {
            break;
        }

        /* Drop lock on this buffer, update cached metapage, and retry. */
        _hash_relbuf(rel, buf);
        metap = _hash_getcachedmetap(rel, &mut metabuf, true);
        Assert!(!metap.is_null());
    }

    if BufferIsValid(metabuf) {
        _hash_dropbuf(rel, metabuf);
    }

    if !cachedmetap.is_null() {
        *cachedmetap = metap;
    }

    buf
}
