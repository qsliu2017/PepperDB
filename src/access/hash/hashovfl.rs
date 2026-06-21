//! hashovfl.rs
//!   Overflow page management code for the Postgres hash access method
//! Translated 1:1 from postgres/src/backend/access/hash/hashovfl.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/hash/hashovfl.c
//!
//! NOTES
//!   Overflow pages look like ordinary relation pages.

#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::access::common::indextuple::{
    CopyIndexTuple, IndexTuple, IndexTupleData, IndexTupleSize,
};
use crate::storage::buf::BufferAccessStrategy;
use crate::access::rmgrdesc::hashdesc::{
    xl_hash_add_ovfl_page, xl_hash_move_page_contents, xl_hash_squeeze_page,
    HASH_XLOG_FREE_OVFL_BUFS, SizeOfHashAddOvflPage, SizeOfHashMovePageContents,
    SizeOfHashSqueezePage, XLOG_HASH_ADD_OVFL_PAGE, XLOG_HASH_MOVE_PAGE_CONTENTS,
    XLOG_HASH_SQUEEZE_PAGE,
};
use crate::access::rmgrlist::RM_HASH_ID;
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::elog;
use crate::miscadmin::{END_CRIT_SECTION, START_CRIT_SECTION};
use crate::pg_config::BLCKSZ;
use crate::common::relpath::MAIN_FORKNUM;
use crate::storage::block::{BlockNumber, BlockNumberIsValid, InvalidBlockNumber};
use crate::storage::buf::{Buffer, InvalidBuffer};
use crate::storage::bufpage::{
    Item, Page, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageIndexMultiDelete,
    PageIsEmpty, PageGetFreeSpaceForMultipleTuples, PageSetLSN,
};
use crate::storage::itemid::{ItemId, ItemIdIsDead};
use crate::storage::off::{
    FirstOffsetNumber, MaxOffsetNumber, OffsetNumber, OffsetNumberNext,
};
use crate::utils::elog::ERROR;
use crate::utils::mmgr::mcxt::pfree;
use crate::utils::rel::{Relation, RelationGetRelationName};
use crate::Assert;

// ---------------------------------------------------------------------------
// Declarations merged from access/hash.h that are needed by hashovfl.c.
// These mirror the sibling hash files (hash.rs, hashinsert.rs, etc.). The
// canonical definitions live in access/hash.h (ported in hash.rs).
// ---------------------------------------------------------------------------

pub type Bucket = uint32;

pub const InvalidBucket: Bucket = 0xFFFFFFFF as Bucket;

/* page-type flags (hash.h) */
pub const LH_UNUSED_PAGE: uint16 = 0;
pub const LH_OVERFLOW_PAGE: uint16 = 1 << 0;
pub const LH_BUCKET_PAGE: uint16 = 1 << 1;
pub const LH_BITMAP_PAGE: uint16 = 1 << 2;
pub const LH_META_PAGE: uint16 = 1 << 3;
pub const LH_PAGE_TYPE: uint16 =
    LH_OVERFLOW_PAGE | LH_BUCKET_PAGE | LH_BITMAP_PAGE | LH_META_PAGE;

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

/* _hash_getbuf flags (hash.h) */
pub const HASH_NOLOCK: c_int = -1;
pub const HASH_READ: c_int = BUFFER_LOCK_SHARE;
pub const HASH_WRITE: c_int = BUFFER_LOCK_EXCLUSIVE;

/* values for ALL_SET / BITS_PER_MAP (hash.h) */
pub const ALL_SET: uint32 = !0u32;

/*
 * Bitmap pages do not contain tuples.  They do contain the standard page
 * header and trailer, with a "special space" that stores nothing.
 */
pub const BYTE_TO_BIT: uint32 = 3; /* 2^3 bits per byte */
pub const BITS_PER_MAP: uint32 = 32; /* Number of bits in uint32 */

/* buffer lock modes (bufmgr.h) */
const BUFFER_LOCK_UNLOCK: c_int = 0;
const BUFFER_LOCK_SHARE: c_int = 1;
const BUFFER_LOCK_EXCLUSIVE: c_int = 2;

/* REGBUF flags (xloginsert.h) */
const REGBUF_FORCE_IMAGE: uint8 = 0x01;
const REGBUF_NO_IMAGE: uint8 = 0x02;
const REGBUF_WILL_INIT: uint8 = 0x04 | 0x02; /* page will be re-initialized at replay */
const REGBUF_STANDARD: uint8 = 0x08;
const REGBUF_KEEP_DATA: uint8 = 0x10;
const REGBUF_NO_CHANGE: uint8 = 0x20; /* intentionally register clean buffer */

// ---------------------------------------------------------------------------
// HashMetaPageData and bitmap accessor helpers (access/hash.h).
// ---------------------------------------------------------------------------

/*
 * Maximum size of a hash index item (it's okay to have only one per page)
 */
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
    pub hashm_procid: crate::postgres_ext::Oid,
    pub hashm_spares: [uint32; HASH_MAX_SPLITPOINTS as usize],
    pub hashm_mapp: [BlockNumber; HASH_MAX_BITMAPS],
}

pub type HashMetaPage = *mut HashMetaPageData;

/* BMPGSZ_BYTE(metap) */
#[inline]
unsafe fn BMPGSZ_BYTE(metap: HashMetaPage) -> uint32 {
    (*metap).hashm_bmsize as uint32
}
/* BMPGSZ_BIT(metap) */
#[inline]
unsafe fn BMPGSZ_BIT(metap: HashMetaPage) -> uint32 {
    ((*metap).hashm_bmsize as uint32) << BYTE_TO_BIT
}
/* BMPG_SHIFT(metap) */
#[inline]
unsafe fn BMPG_SHIFT(metap: HashMetaPage) -> uint32 {
    (*metap).hashm_bmshift as uint32
}
/* BMPG_MASK(metap) */
#[inline]
unsafe fn BMPG_MASK(metap: HashMetaPage) -> uint32 {
    BMPGSZ_BIT(metap) - 1
}

/* HashPageGetBitmap(page) */
#[inline]
unsafe fn HashPageGetBitmap(page: Page) -> *mut uint32 {
    // (uint32 *) PageGetContents(page)
    let phdr = page as *mut u8;
    phdr.add(MAXALIGN(core::mem::size_of::<PageHeaderShim>())) as *mut uint32
}

/* CLRBIT(A, N) */
#[inline]
unsafe fn CLRBIT(A: *mut uint32, N: uint32) {
    let idx = (N / BITS_PER_MAP) as usize;
    *A.add(idx) &= !(1u32 << (N % BITS_PER_MAP));
}
/* SETBIT(A, N) */
#[inline]
unsafe fn SETBIT(A: *mut uint32, N: uint32) {
    let idx = (N / BITS_PER_MAP) as usize;
    *A.add(idx) |= 1u32 << (N % BITS_PER_MAP);
}
/* ISSET(A, N) */
#[inline]
unsafe fn ISSET(A: *mut uint32, N: uint32) -> uint32 {
    let idx = (N / BITS_PER_MAP) as usize;
    *A.add(idx) & (1u32 << (N % BITS_PER_MAP))
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

// ---------------------------------------------------------------------------
// Stubbed callees from other (not-yet-ported) translation units.
// ---------------------------------------------------------------------------

unsafe fn _hash_get_totalbuckets(_splitpoint_phase: uint32) -> uint32 { crate::access::hash::hashutil::_hash_get_totalbuckets(_splitpoint_phase) }

unsafe fn _hash_checkpage(_rel: Relation, _buf: Buffer, _flags: c_int) { crate::access::hash::hashutil::_hash_checkpage(_rel, _buf, _flags) }

unsafe fn _hash_pageinit(_page: Page, _size: Size) { unimplemented!() }

unsafe fn _hash_pgaddmultitup(
    _rel: Relation,
    _buf: Buffer,
    _itups: *mut IndexTuple,
    _itup_offsets: *mut OffsetNumber,
    _nitups: uint16,
) { crate::access::hash::hashinsert::_hash_pgaddmultitup(_rel, _buf, _itups, _itup_offsets, _nitups) }

unsafe fn _hash_getbuf(
    _rel: Relation,
    _blkno: BlockNumber,
    _access: c_int,
    _flags: c_int,
) -> Buffer { unimplemented!() }

unsafe fn _hash_getbuf_with_strategy(
    _rel: Relation,
    _blkno: BlockNumber,
    _access: c_int,
    _flags: c_int,
    _bstrategy: BufferAccessStrategy,
) -> Buffer { unimplemented!() }

unsafe fn _hash_getinitbuf(_rel: Relation, _blkno: BlockNumber) -> Buffer { unimplemented!() }

unsafe fn _hash_getnewbuf(
    _rel: Relation,
    _blkno: BlockNumber,
    _forkNum: c_int,
) -> Buffer {
    unimplemented!() // TODO: access/hashpage.c
}

unsafe fn _hash_relbuf(_rel: Relation, _buf: Buffer) { unimplemented!() }

unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.h
}

unsafe fn BufferGetPageSize(_buffer: Buffer) -> Size { crate::access::nbtree::nbtpage::BufferGetPageSize(_buffer) }

unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn BufferIsValid(_buffer: Buffer) -> bool { crate::access::nbtree::nbtpage::BufferIsValid(_buffer) }

unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn RelationNeedsWAL(_relation: Relation) -> bool { crate::access::nbtree::nbtdedup::RelationNeedsWAL(_relation) }

// ---------------------------------------------------------------------------
// XLog insertion helpers (access/xloginsert.h). Local stubs mirror siblings.
// ---------------------------------------------------------------------------

unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: access/xloginsert.c
}

unsafe fn XLogRegisterData(_data: *mut c_void, _len: c_int) {
    unimplemented!() // TODO: access/xloginsert.c
}

unsafe fn XLogRegisterBuffer(_block_id: uint8, _buffer: Buffer, _flags: uint8) {
    unimplemented!() // TODO: access/xloginsert.c
}

unsafe fn XLogRegisterBufData(_block_id: uint8, _data: *mut c_void, _len: c_int) {
    unimplemented!() // TODO: access/xloginsert.c
}

unsafe fn XLogInsert(_rmid: u8, _info: uint8) -> XLogRecPtr {
    unimplemented!() // TODO: access/xloginsert.c
}

unsafe fn XLogEnsureRecordSpace(_max_block_id: c_int, _ndatas: c_int) { crate::access::transam::xloginsert::XLogEnsureRecordSpace(_max_block_id, _ndatas) }

// ---------------------------------------------------------------------------

/*
 * Convert overflow page bit number (its index in the free-page bitmaps)
 * to block number within the index.
 */
unsafe fn bitno_to_blkno(metap: HashMetaPage, ovflbitnum: uint32) -> BlockNumber {
    let splitnum: uint32 = (*metap).hashm_ovflpoint;
    let mut i: uint32;

    /* Convert zero-based bitnumber to 1-based page number */
    let mut ovflbitnum = ovflbitnum;
    ovflbitnum += 1;

    /* Determine the split number for this page (must be >= 1) */
    i = 1;
    while i < splitnum && ovflbitnum > (*metap).hashm_spares[i as usize] {
        /* loop */
        i += 1;
    }

    /*
     * Convert to absolute page number by adding the number of bucket pages
     * that exist before this split point.
     */
    (_hash_get_totalbuckets(i) + ovflbitnum) as BlockNumber
}

/*
 * _hash_ovflblkno_to_bitno
 *
 * Convert overflow page block number to bit number for free-page bitmap.
 */
pub unsafe fn _hash_ovflblkno_to_bitno(metap: HashMetaPage, ovflblkno: BlockNumber) -> uint32 {
    let splitnum: uint32 = (*metap).hashm_ovflpoint;
    let mut i: uint32;
    let mut bitnum: uint32 = 0;

    /* Determine the split number containing this page */
    i = 1;
    while i <= splitnum {
        if ovflblkno <= _hash_get_totalbuckets(i) as BlockNumber {
            break; /* oops */
        }
        bitnum = ovflblkno - _hash_get_totalbuckets(i) as BlockNumber;

        /*
         * bitnum has to be greater than number of overflow page added in
         * previous split point. The overflow page at this splitnum (i) if any
         * should start from (_hash_get_totalbuckets(i) +
         * metap->hashm_spares[i - 1] + 1).
         */
        if bitnum > (*metap).hashm_spares[(i - 1) as usize]
            && bitnum <= (*metap).hashm_spares[i as usize]
        {
            return bitnum - 1; /* -1 to convert 1-based to 0-based */
        }
        i += 1;
    }

    ereport!(ERROR, "invalid overflow block number");
    0 /* keep compiler quiet */
}

/*
 *	_hash_addovflpage
 *
 *	Add an overflow page to the bucket whose last page is pointed to by 'buf'.
 *
 *	On entry, the caller must hold a pin but no lock on 'buf'.  The pin is
 *	dropped before exiting (we assume the caller is not interested in 'buf'
 *	anymore) if not asked to retain.  The pin will be retained only for the
 *	primary bucket.  The returned overflow page will be pinned and
 *	write-locked; it is guaranteed to be empty.
 *
 *	The caller must hold a pin, but no lock, on the metapage buffer.
 *	That buffer is returned in the same state.
 *
 * NB: since this could be executed concurrently by multiple processes,
 * one should not assume that the returned overflow page will be the
 * immediate successor of the originally passed 'buf'.  Additional overflow
 * pages might have been added to the bucket chain in between.
 */
pub unsafe fn _hash_addovflpage(
    rel: Relation,
    metabuf: Buffer,
    buf: Buffer,
    retain_pin: bool,
) -> Buffer {
    let mut buf = buf;
    let mut retain_pin = retain_pin;
    let mut ovflbuf: Buffer = InvalidBuffer;
    let mut page: Page;
    let ovflpage: Page;
    let mut pageopaque: HashPageOpaque = core::ptr::null_mut();
    let ovflopaque: HashPageOpaque;
    let metap: HashMetaPage;
    let mut mapbuf: Buffer = InvalidBuffer;
    let mut newmapbuf: Buffer = InvalidBuffer;
    let mut blkno: BlockNumber = InvalidBlockNumber;
    let orig_firstfree: uint32;
    let mut splitnum: uint32;
    let mut freep: *mut uint32 = core::ptr::null_mut();
    let mut max_ovflpg: uint32;
    let mut bit: uint32;
    let mut bitmap_page_bit: uint32 = 0;
    let first_page: uint32;
    let mut last_bit: uint32 = 0;
    let mut last_page: uint32;
    let mut i: uint32;
    let mut j: uint32;
    let mut page_found: bool = false;

    /*
     * Write-lock the tail page.  Here, we need to maintain locking order such
     * that, first acquire the lock on tail page of bucket, then on meta page
     * to find and lock the bitmap page and if it is found, then lock on meta
     * page is released, then finally acquire the lock on new overflow buffer.
     * We need this locking order to avoid deadlock with backends that are
     * doing inserts.
     *
     * Note: We could have avoided locking many buffers here if we made two
     * WAL records for acquiring an overflow page (one to allocate an overflow
     * page and another to add it to overflow bucket chain).  However, doing
     * so can leak an overflow page, if the system crashes after allocation.
     * Needless to say, it is better to have a single record from a
     * performance point of view as well.
     */
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    /* probably redundant... */
    _hash_checkpage(rel, buf, (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as c_int);

    /* loop to find current tail page, in case someone else inserted too */
    loop {
        let nextblkno: BlockNumber;

        page = BufferGetPage(buf);
        pageopaque = HashPageGetOpaque(page);
        nextblkno = (*pageopaque).hasho_nextblkno;

        if !BlockNumberIsValid(nextblkno) {
            break;
        }

        /* we assume we do not need to write the unmodified page */
        if retain_pin {
            /* pin will be retained only for the primary bucket page */
            Assert!(((*pageopaque).hasho_flag & LH_PAGE_TYPE) == LH_BUCKET_PAGE);
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        } else {
            _hash_relbuf(rel, buf);
        }

        retain_pin = false;

        buf = _hash_getbuf(rel, nextblkno, HASH_WRITE, LH_OVERFLOW_PAGE as c_int);
    }

    /* Get exclusive lock on the meta page */
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

    _hash_checkpage(rel, metabuf, LH_META_PAGE as c_int);
    metap = HashPageGetMeta(BufferGetPage(metabuf));

    /* start search at hashm_firstfree */
    orig_firstfree = (*metap).hashm_firstfree;
    first_page = orig_firstfree >> BMPG_SHIFT(metap);
    bit = orig_firstfree & BMPG_MASK(metap);
    i = first_page;
    j = bit / BITS_PER_MAP;
    bit &= !(BITS_PER_MAP - 1);

    /* outer loop iterates once per bitmap page */
    'outer: loop {
        let mapblkno: BlockNumber;
        let mappage: Page;
        let last_inpage: uint32;

        /* want to end search with the last existing overflow page */
        splitnum = (*metap).hashm_ovflpoint;
        max_ovflpg = (*metap).hashm_spares[splitnum as usize] - 1;
        last_page = max_ovflpg >> BMPG_SHIFT(metap);
        last_bit = max_ovflpg & BMPG_MASK(metap);

        if i > last_page {
            break;
        }

        Assert!(i < (*metap).hashm_nmaps);
        mapblkno = (*metap).hashm_mapp[i as usize];

        if i == last_page {
            last_inpage = last_bit;
        } else {
            last_inpage = BMPGSZ_BIT(metap) - 1;
        }

        /* Release exclusive lock on metapage while reading bitmap page */
        LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

        mapbuf = _hash_getbuf(rel, mapblkno, HASH_WRITE, LH_BITMAP_PAGE as c_int);
        mappage = BufferGetPage(mapbuf);
        freep = HashPageGetBitmap(mappage);

        while bit <= last_inpage {
            if *freep.add(j as usize) != ALL_SET {
                page_found = true;

                /* Reacquire exclusive lock on the meta page */
                LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

                /* convert bit to bit number within page */
                bit += _hash_firstfreebit(*freep.add(j as usize));
                bitmap_page_bit = bit;

                /* convert bit to absolute bit number */
                bit += i << BMPG_SHIFT(metap);
                /* Calculate address of the recycled overflow page */
                blkno = bitno_to_blkno(metap, bit);

                /* Fetch and init the recycled page */
                ovflbuf = _hash_getinitbuf(rel, blkno);

                break 'outer; // goto found;
            }

            j += 1;
            bit += BITS_PER_MAP;
        }

        /* No free space here, try to advance to next map page */
        _hash_relbuf(rel, mapbuf);
        mapbuf = InvalidBuffer;
        i += 1;
        j = 0; /* scan from start of next map page */
        bit = 0;

        /* Reacquire exclusive lock on the meta page */
        LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
    }

    if !page_found {
        /*
         * No free pages --- have to extend the relation to add an overflow
         * page.  First, check to see if we have to add a new bitmap page too.
         */
        if last_bit == (BMPGSZ_BIT(metap) - 1) {
            /*
             * We create the new bitmap page with all pages marked "in use".
             * Actually two pages in the new bitmap's range will exist
             * immediately: the bitmap page itself, and the following page which
             * is the one we return to the caller.  Both of these are correctly
             * marked "in use".  Subsequent pages do not exist yet, but it is
             * convenient to pre-mark them as "in use" too.
             */
            bit = (*metap).hashm_spares[splitnum as usize];

            /* metapage already has a write lock */
            if (*metap).hashm_nmaps >= HASH_MAX_BITMAPS as uint32 {
                ereport!(ERROR, "out of overflow pages in hash index");
            }

            newmapbuf = _hash_getnewbuf(rel, bitno_to_blkno(metap, bit), MAIN_FORKNUM as c_int);
        } else {
            /*
             * Nothing to do here; since the page will be past the last used
             * page, we know its bitmap bit was preinitialized to "in use".
             */
        }

        /* Calculate address of the new overflow page */
        bit = if BufferIsValid(newmapbuf) {
            (*metap).hashm_spares[splitnum as usize] + 1
        } else {
            (*metap).hashm_spares[splitnum as usize]
        };
        blkno = bitno_to_blkno(metap, bit);

        /*
         * Fetch the page with _hash_getnewbuf to ensure smgr's idea of the
         * relation length stays in sync with ours.  XXX It's annoying to do
         * this with metapage write lock held; would be better to use a lock
         * that doesn't block incoming searches.
         *
         * It is okay to hold two buffer locks here (one on tail page of bucket
         * and other on new overflow page) since there cannot be anyone else
         * contending for access to ovflbuf.
         */
        ovflbuf = _hash_getnewbuf(rel, blkno, MAIN_FORKNUM as c_int);
    }

    // found:

    /*
     * Do the update.  No ereport(ERROR) until changes are logged. We want to
     * log the changes for bitmap page and overflow page together to avoid
     * loss of pages in case the new page is added.
     */
    START_CRIT_SECTION();

    if page_found {
        Assert!(BufferIsValid(mapbuf));

        /* mark page "in use" in the bitmap */
        SETBIT(freep, bitmap_page_bit);
        MarkBufferDirty(mapbuf);
    } else {
        /* update the count to indicate new overflow page is added */
        (*metap).hashm_spares[splitnum as usize] += 1;

        if BufferIsValid(newmapbuf) {
            _hash_initbitmapbuffer(newmapbuf, (*metap).hashm_bmsize, false);
            MarkBufferDirty(newmapbuf);

            /* add the new bitmap page to the metapage's list of bitmaps */
            (*metap).hashm_mapp[(*metap).hashm_nmaps as usize] = BufferGetBlockNumber(newmapbuf);
            (*metap).hashm_nmaps += 1;
            (*metap).hashm_spares[splitnum as usize] += 1;
        }

        MarkBufferDirty(metabuf);

        /*
         * for new overflow page, we don't need to explicitly set the bit in
         * bitmap page, as by default that will be set to "in use".
         */
    }

    /*
     * Adjust hashm_firstfree to avoid redundant searches.  But don't risk
     * changing it if someone moved it while we were searching bitmap pages.
     */
    if (*metap).hashm_firstfree == orig_firstfree {
        (*metap).hashm_firstfree = bit + 1;
        MarkBufferDirty(metabuf);
    }

    /* initialize new overflow page */
    ovflpage = BufferGetPage(ovflbuf);
    ovflopaque = HashPageGetOpaque(ovflpage);
    (*ovflopaque).hasho_prevblkno = BufferGetBlockNumber(buf);
    (*ovflopaque).hasho_nextblkno = InvalidBlockNumber;
    (*ovflopaque).hasho_bucket = (*pageopaque).hasho_bucket;
    (*ovflopaque).hasho_flag = LH_OVERFLOW_PAGE;
    (*ovflopaque).hasho_page_id = HASHO_PAGE_ID;

    MarkBufferDirty(ovflbuf);

    /* logically chain overflow page to previous page */
    (*pageopaque).hasho_nextblkno = BufferGetBlockNumber(ovflbuf);

    MarkBufferDirty(buf);

    /* XLOG stuff */
    if RelationNeedsWAL(rel) {
        let recptr: XLogRecPtr;
        let mut xlrec: xl_hash_add_ovfl_page = core::mem::zeroed();

        xlrec.bmpage_found = page_found;
        xlrec.bmsize = (*metap).hashm_bmsize;

        XLogBeginInsert();
        XLogRegisterData(&raw mut xlrec as *mut c_void, SizeOfHashAddOvflPage as c_int);

        XLogRegisterBuffer(0, ovflbuf, REGBUF_WILL_INIT);
        XLogRegisterBufData(
            0,
            &raw mut (*pageopaque).hasho_bucket as *mut c_void,
            core::mem::size_of::<Bucket>() as c_int,
        );

        XLogRegisterBuffer(1, buf, REGBUF_STANDARD);

        if BufferIsValid(mapbuf) {
            XLogRegisterBuffer(2, mapbuf, REGBUF_STANDARD);
            XLogRegisterBufData(
                2,
                &raw mut bitmap_page_bit as *mut c_void,
                core::mem::size_of::<uint32>() as c_int,
            );
        }

        if BufferIsValid(newmapbuf) {
            XLogRegisterBuffer(3, newmapbuf, REGBUF_WILL_INIT);
        }

        XLogRegisterBuffer(4, metabuf, REGBUF_STANDARD);
        XLogRegisterBufData(
            4,
            &raw mut (*metap).hashm_firstfree as *mut c_void,
            core::mem::size_of::<uint32>() as c_int,
        );

        recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_ADD_OVFL_PAGE);

        PageSetLSN(BufferGetPage(ovflbuf), recptr);
        PageSetLSN(BufferGetPage(buf), recptr);

        if BufferIsValid(mapbuf) {
            PageSetLSN(BufferGetPage(mapbuf), recptr);
        }

        if BufferIsValid(newmapbuf) {
            PageSetLSN(BufferGetPage(newmapbuf), recptr);
        }

        PageSetLSN(BufferGetPage(metabuf), recptr);
    }

    END_CRIT_SECTION();

    if retain_pin {
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    } else {
        _hash_relbuf(rel, buf);
    }

    if BufferIsValid(mapbuf) {
        _hash_relbuf(rel, mapbuf);
    }

    LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

    if BufferIsValid(newmapbuf) {
        _hash_relbuf(rel, newmapbuf);
    }

    ovflbuf
}

/*
 *	_hash_firstfreebit()
 *
 *	Return the number of the first bit that is not set in the word 'map'.
 */
unsafe fn _hash_firstfreebit(map: uint32) -> uint32 {
    let mut i: uint32;
    let mut mask: uint32;

    mask = 0x1;
    i = 0;
    while i < BITS_PER_MAP {
        if (mask & map) == 0 {
            return i;
        }
        mask <<= 1;
        i += 1;
    }

    elog!(ERROR, "firstfreebit found no free bit");

    0 /* keep compiler quiet */
}

/*
 *	_hash_freeovflpage() -
 *
 *	Remove this overflow page from its bucket's chain, and mark the page as
 *	free.  On entry, ovflbuf is write-locked; it is released before exiting.
 *
 *	Add the tuples (itups) to wbuf in this function.  We could do that in the
 *	caller as well, but the advantage of doing it here is we can easily write
 *	the WAL for XLOG_HASH_SQUEEZE_PAGE operation.  Addition of tuples and
 *	removal of overflow page has to done as an atomic operation, otherwise
 *	during replay on standby users might find duplicate records.
 *
 *	Since this function is invoked in VACUUM, we provide an access strategy
 *	parameter that controls fetches of the bucket pages.
 *
 *	Returns the block number of the page that followed the given page
 *	in the bucket, or InvalidBlockNumber if no following page.
 *
 *	NB: caller must not hold lock on metapage, nor on page, that's next to
 *	ovflbuf in the bucket chain.  We don't acquire the lock on page that's
 *	prior to ovflbuf in chain if it is same as wbuf because the caller already
 *	has a lock on same.
 */
pub unsafe fn _hash_freeovflpage(
    rel: Relation,
    bucketbuf: Buffer,
    ovflbuf: Buffer,
    wbuf: Buffer,
    itups: *mut IndexTuple,
    itup_offsets: *mut OffsetNumber,
    tups_size: *mut Size,
    nitups: uint16,
    bstrategy: BufferAccessStrategy,
) -> BlockNumber {
    let metap: HashMetaPage;
    let metabuf: Buffer;
    let mapbuf: Buffer;
    let ovflblkno: BlockNumber;
    let prevblkno: BlockNumber;
    let blkno: BlockNumber;
    let nextblkno: BlockNumber;
    let writeblkno: BlockNumber;
    let mut ovflopaque: HashPageOpaque;
    let ovflpage: Page;
    let mappage: Page;
    let freep: *mut uint32;
    let ovflbitno: uint32;
    let bitmappage: int32;
    let bitmapbit: int32;
    let bucket: Bucket; /* PG_USED_FOR_ASSERTS_ONLY */
    let mut prevbuf: Buffer = InvalidBuffer;
    let mut nextbuf: Buffer = InvalidBuffer;
    let mut update_metap: bool = false;

    /* Get information from the doomed page */
    _hash_checkpage(rel, ovflbuf, LH_OVERFLOW_PAGE as c_int);
    ovflblkno = BufferGetBlockNumber(ovflbuf);
    ovflpage = BufferGetPage(ovflbuf);
    ovflopaque = HashPageGetOpaque(ovflpage);
    nextblkno = (*ovflopaque).hasho_nextblkno;
    prevblkno = (*ovflopaque).hasho_prevblkno;
    writeblkno = BufferGetBlockNumber(wbuf);
    bucket = (*ovflopaque).hasho_bucket;

    /*
     * Fix up the bucket chain.  this is a doubly-linked list, so we must fix
     * up the bucket chain members behind and ahead of the overflow page being
     * deleted.  Concurrency issues are avoided by using lock chaining as
     * described atop hashbucketcleanup.
     */
    if BlockNumberIsValid(prevblkno) {
        if prevblkno == writeblkno {
            prevbuf = wbuf;
        } else {
            prevbuf = _hash_getbuf_with_strategy(
                rel,
                prevblkno,
                HASH_WRITE,
                (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as c_int,
                bstrategy,
            );
        }
    }
    if BlockNumberIsValid(nextblkno) {
        nextbuf = _hash_getbuf_with_strategy(
            rel,
            nextblkno,
            HASH_WRITE,
            LH_OVERFLOW_PAGE as c_int,
            bstrategy,
        );
    }

    /* Note: bstrategy is intentionally not used for metapage and bitmap */

    /* Read the metapage so we can determine which bitmap page to use */
    metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_READ, LH_META_PAGE as c_int);
    metap = HashPageGetMeta(BufferGetPage(metabuf));

    /* Identify which bit to set */
    ovflbitno = _hash_ovflblkno_to_bitno(metap, ovflblkno);

    bitmappage = (ovflbitno >> BMPG_SHIFT(metap)) as int32;
    bitmapbit = (ovflbitno & BMPG_MASK(metap)) as int32;

    if bitmappage >= (*metap).hashm_nmaps as int32 {
        elog!(ERROR, "invalid overflow bit number {}", ovflbitno);
    }
    blkno = (*metap).hashm_mapp[bitmappage as usize];

    /* Release metapage lock while we access the bitmap page */
    LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

    /* read the bitmap page to clear the bitmap bit */
    mapbuf = _hash_getbuf(rel, blkno, HASH_WRITE, LH_BITMAP_PAGE as c_int);
    mappage = BufferGetPage(mapbuf);
    freep = HashPageGetBitmap(mappage);
    Assert!(ISSET(freep, bitmapbit as uint32) != 0);

    /* Get write-lock on metapage to update firstfree */
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

    /* This operation needs to log multiple tuples, prepare WAL for that */
    if RelationNeedsWAL(rel) {
        XLogEnsureRecordSpace(HASH_XLOG_FREE_OVFL_BUFS, 4 + nitups as c_int);
    }

    START_CRIT_SECTION();

    /*
     * we have to insert tuples on the "write" page, being careful to preserve
     * hashkey ordering.  (If we insert many tuples into the same "write" page
     * it would be worth qsort'ing them).
     */
    if nitups > 0 {
        _hash_pgaddmultitup(rel, wbuf, itups, itup_offsets, nitups);
        MarkBufferDirty(wbuf);
    }

    /*
     * Reinitialize the freed overflow page.  Just zeroing the page won't
     * work, because WAL replay routines expect pages to be initialized. See
     * explanation of RBM_NORMAL mode atop XLogReadBufferExtended.  We are
     * careful to make the special space valid here so that tools like
     * pageinspect won't get confused.
     */
    _hash_pageinit(ovflpage, BufferGetPageSize(ovflbuf));

    ovflopaque = HashPageGetOpaque(ovflpage);

    (*ovflopaque).hasho_prevblkno = InvalidBlockNumber;
    (*ovflopaque).hasho_nextblkno = InvalidBlockNumber;
    (*ovflopaque).hasho_bucket = InvalidBucket;
    (*ovflopaque).hasho_flag = LH_UNUSED_PAGE;
    (*ovflopaque).hasho_page_id = HASHO_PAGE_ID;

    MarkBufferDirty(ovflbuf);

    if BufferIsValid(prevbuf) {
        let prevpage: Page = BufferGetPage(prevbuf);
        let prevopaque: HashPageOpaque = HashPageGetOpaque(prevpage);

        Assert!((*prevopaque).hasho_bucket == bucket);
        (*prevopaque).hasho_nextblkno = nextblkno;
        MarkBufferDirty(prevbuf);
    }
    if BufferIsValid(nextbuf) {
        let nextpage: Page = BufferGetPage(nextbuf);
        let nextopaque: HashPageOpaque = HashPageGetOpaque(nextpage);

        Assert!((*nextopaque).hasho_bucket == bucket);
        (*nextopaque).hasho_prevblkno = prevblkno;
        MarkBufferDirty(nextbuf);
    }

    /* Clear the bitmap bit to indicate that this overflow page is free */
    CLRBIT(freep, bitmapbit as uint32);
    MarkBufferDirty(mapbuf);

    /* if this is now the first free page, update hashm_firstfree */
    if ovflbitno < (*metap).hashm_firstfree {
        (*metap).hashm_firstfree = ovflbitno;
        update_metap = true;
        MarkBufferDirty(metabuf);
    }

    /* XLOG stuff */
    if RelationNeedsWAL(rel) {
        let mut xlrec: xl_hash_squeeze_page = core::mem::zeroed();
        let recptr: XLogRecPtr;
        let mut i: c_int;
        let mut mod_wbuf: bool = false;

        xlrec.prevblkno = prevblkno;
        xlrec.nextblkno = nextblkno;
        xlrec.ntups = nitups;
        xlrec.is_prim_bucket_same_wrt = wbuf == bucketbuf;
        xlrec.is_prev_bucket_same_wrt = wbuf == prevbuf;

        XLogBeginInsert();
        XLogRegisterData(&raw mut xlrec as *mut c_void, SizeOfHashSqueezePage as c_int);

        /*
         * bucket buffer was not changed, but still needs to be registered to
         * ensure that we can acquire a cleanup lock on it during replay.
         */
        if !xlrec.is_prim_bucket_same_wrt {
            let flags: uint8 = REGBUF_STANDARD | REGBUF_NO_IMAGE | REGBUF_NO_CHANGE;

            XLogRegisterBuffer(0, bucketbuf, flags);
        }

        if xlrec.ntups > 0 {
            XLogRegisterBuffer(1, wbuf, REGBUF_STANDARD);

            /* Remember that wbuf is modified. */
            mod_wbuf = true;

            XLogRegisterBufData(
                1,
                itup_offsets as *mut c_void,
                (nitups as usize * core::mem::size_of::<OffsetNumber>()) as c_int,
            );
            i = 0;
            while i < nitups as c_int {
                XLogRegisterBufData(
                    1,
                    *itups.add(i as usize) as *mut c_void,
                    *tups_size.add(i as usize) as c_int,
                );
                i += 1;
            }
        } else if xlrec.is_prim_bucket_same_wrt || xlrec.is_prev_bucket_same_wrt {
            let mut wbuf_flags: uint8;

            /*
             * A write buffer needs to be registered even if no tuples are
             * added to it to ensure that we can acquire a cleanup lock on it
             * if it is the same as primary bucket buffer or update the
             * nextblkno if it is same as the previous bucket buffer.
             */
            Assert!(xlrec.ntups == 0);

            wbuf_flags = REGBUF_STANDARD;
            if !xlrec.is_prev_bucket_same_wrt {
                wbuf_flags |= REGBUF_NO_CHANGE;
            } else {
                /* Remember that wbuf is modified. */
                mod_wbuf = true;
            }
            XLogRegisterBuffer(1, wbuf, wbuf_flags);
        }

        XLogRegisterBuffer(2, ovflbuf, REGBUF_STANDARD);

        /*
         * If prevpage and the writepage (block in which we are moving tuples
         * from overflow) are same, then no need to separately register
         * prevpage.  During replay, we can directly update the nextblock in
         * writepage.
         */
        if BufferIsValid(prevbuf) && !xlrec.is_prev_bucket_same_wrt {
            XLogRegisterBuffer(3, prevbuf, REGBUF_STANDARD);
        }

        if BufferIsValid(nextbuf) {
            XLogRegisterBuffer(4, nextbuf, REGBUF_STANDARD);
        }

        XLogRegisterBuffer(5, mapbuf, REGBUF_STANDARD);
        XLogRegisterBufData(
            5,
            &raw const bitmapbit as *mut c_void,
            core::mem::size_of::<uint32>() as c_int,
        );

        if update_metap {
            XLogRegisterBuffer(6, metabuf, REGBUF_STANDARD);
            XLogRegisterBufData(
                6,
                &raw mut (*metap).hashm_firstfree as *mut c_void,
                core::mem::size_of::<uint32>() as c_int,
            );
        }

        recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_SQUEEZE_PAGE);

        /* Set LSN iff wbuf is modified. */
        if mod_wbuf {
            PageSetLSN(BufferGetPage(wbuf), recptr);
        }

        PageSetLSN(BufferGetPage(ovflbuf), recptr);

        if BufferIsValid(prevbuf) && !xlrec.is_prev_bucket_same_wrt {
            PageSetLSN(BufferGetPage(prevbuf), recptr);
        }
        if BufferIsValid(nextbuf) {
            PageSetLSN(BufferGetPage(nextbuf), recptr);
        }

        PageSetLSN(BufferGetPage(mapbuf), recptr);

        if update_metap {
            PageSetLSN(BufferGetPage(metabuf), recptr);
        }
    }

    END_CRIT_SECTION();

    /* release previous bucket if it is not same as write bucket */
    if BufferIsValid(prevbuf) && prevblkno != writeblkno {
        _hash_relbuf(rel, prevbuf);
    }

    if BufferIsValid(ovflbuf) {
        _hash_relbuf(rel, ovflbuf);
    }

    if BufferIsValid(nextbuf) {
        _hash_relbuf(rel, nextbuf);
    }

    _hash_relbuf(rel, mapbuf);
    _hash_relbuf(rel, metabuf);

    nextblkno
}

/*
 *	_hash_initbitmapbuffer()
 *
 *	 Initialize a new bitmap page.  All bits in the new bitmap page are set to
 *	 "1", indicating "in use".
 */
pub unsafe fn _hash_initbitmapbuffer(buf: Buffer, bmsize: uint16, initpage: bool) {
    let pg: Page;
    let op: HashPageOpaque;
    let freep: *mut uint32;

    pg = BufferGetPage(buf);

    /* initialize the page */
    if initpage {
        _hash_pageinit(pg, BufferGetPageSize(buf));
    }

    /* initialize the page's special space */
    op = HashPageGetOpaque(pg);
    (*op).hasho_prevblkno = InvalidBlockNumber;
    (*op).hasho_nextblkno = InvalidBlockNumber;
    (*op).hasho_bucket = InvalidBucket;
    (*op).hasho_flag = LH_BITMAP_PAGE;
    (*op).hasho_page_id = HASHO_PAGE_ID;

    /* set all of the bits to 1 */
    freep = HashPageGetBitmap(pg);
    core::ptr::write_bytes(freep as *mut u8, 0xFF, bmsize as usize);

    /*
     * Set pd_lower just past the end of the bitmap page data.  We could even
     * set pd_lower equal to pd_upper, but this is more precise and makes the
     * page look compressible to xlog.c.
     */
    let phdr = pg as *mut PageHeaderShim;
    (*phdr).pd_lower =
        (((freep as *mut c_char).add(bmsize as usize) as isize) - (pg as isize)) as u16;
}

/*
 *	_hash_squeezebucket(rel, bucket)
 *
 *	Try to squeeze the tuples onto pages occurring earlier in the
 *	bucket chain in an attempt to free overflow pages. When we start
 *	the "squeezing", the page from which we start taking tuples (the
 *	"read" page) is the last bucket in the bucket chain and the page
 *	onto which we start squeezing tuples (the "write" page) is the
 *	first page in the bucket chain.  The read page works backward and
 *	the write page works forward; the procedure terminates when the
 *	read page and write page are the same page.
 *
 *	At completion of this procedure, it is guaranteed that all pages in
 *	the bucket are nonempty, unless the bucket is totally empty (in
 *	which case all overflow pages will be freed).  The original implementation
 *	required that to be true on entry as well, but it's a lot easier for
 *	callers to leave empty overflow pages and let this guy clean it up.
 *
 *	Caller must acquire cleanup lock on the primary page of the target
 *	bucket to exclude any scans that are in progress, which could easily
 *	be confused into returning the same tuple more than once or some tuples
 *	not at all by the rearrangement we are performing here.  To prevent
 *	any concurrent scan to cross the squeeze scan we use lock chaining
 *	similar to hashbucketcleanup.  Refer comments atop hashbucketcleanup.
 *
 *	We need to retain a pin on the primary bucket to ensure that no concurrent
 *	split can start.
 *
 *	Since this function is invoked in VACUUM, we provide an access strategy
 *	parameter that controls fetches of the bucket pages.
 */
pub unsafe fn _hash_squeezebucket(
    rel: Relation,
    bucket: Bucket,
    bucket_blkno: BlockNumber,
    bucket_buf: Buffer,
    bstrategy: BufferAccessStrategy,
) {
    let mut wblkno: BlockNumber;
    let mut rblkno: BlockNumber;
    let mut wbuf: Buffer;
    let mut rbuf: Buffer;
    let mut wpage: Page;
    let mut rpage: Page;
    let mut wopaque: HashPageOpaque;
    let mut ropaque: HashPageOpaque;

    /*
     * start squeezing into the primary bucket page.
     */
    wblkno = bucket_blkno;
    wbuf = bucket_buf;
    wpage = BufferGetPage(wbuf);
    wopaque = HashPageGetOpaque(wpage);

    /*
     * if there aren't any overflow pages, there's nothing to squeeze. caller
     * is responsible for releasing the pin on primary bucket page.
     */
    if !BlockNumberIsValid((*wopaque).hasho_nextblkno) {
        LockBuffer(wbuf, BUFFER_LOCK_UNLOCK);
        return;
    }

    /*
     * Find the last page in the bucket chain by starting at the base bucket
     * page and working forward.  Note: we assume that a hash bucket chain is
     * usually smaller than the buffer ring being used by VACUUM, else using
     * the access strategy here would be counterproductive.
     */
    rbuf = InvalidBuffer;
    ropaque = wopaque;
    loop {
        rblkno = (*ropaque).hasho_nextblkno;
        if rbuf != InvalidBuffer {
            _hash_relbuf(rel, rbuf);
        }
        rbuf = _hash_getbuf_with_strategy(
            rel,
            rblkno,
            HASH_WRITE,
            LH_OVERFLOW_PAGE as c_int,
            bstrategy,
        );
        rpage = BufferGetPage(rbuf);
        ropaque = HashPageGetOpaque(rpage);
        Assert!((*ropaque).hasho_bucket == bucket);

        if !BlockNumberIsValid((*ropaque).hasho_nextblkno) {
            break;
        }
    }

    /*
     * squeeze the tuples.
     */
    loop {
        let mut roffnum: OffsetNumber;
        let mut maxroffnum: OffsetNumber;
        let mut deletable: [OffsetNumber; MaxOffsetNumber as usize] =
            [0; MaxOffsetNumber as usize];
        let mut itups: [IndexTuple; MaxIndexTuplesPerPage] =
            [core::ptr::null_mut(); MaxIndexTuplesPerPage];
        let mut tups_size: [Size; MaxIndexTuplesPerPage] = [0; MaxIndexTuplesPerPage];
        let mut itup_offsets: [OffsetNumber; MaxIndexTuplesPerPage] =
            [0; MaxIndexTuplesPerPage];
        let mut ndeletable: uint16 = 0;
        let mut nitups: uint16 = 0;
        let mut all_tups_size: Size = 0;
        let mut i: c_int;
        let mut retain_pin: bool = false;

        // readpage:
        'readpage: loop {
            /* Scan each tuple in "read" page */
            maxroffnum = PageGetMaxOffsetNumber(rpage);
            roffnum = FirstOffsetNumber;
            while roffnum <= maxroffnum {
                let itup: IndexTuple;
                let mut itemsz: Size;

                /* skip dead tuples */
                if ItemIdIsDead(PageGetItemId(rpage, roffnum)) {
                    roffnum = OffsetNumberNext(roffnum);
                    continue;
                }

                itup = PageGetItem(rpage, PageGetItemId(rpage, roffnum)) as IndexTuple;
                itemsz = IndexTupleSize(itup as *const IndexTupleData);
                itemsz = MAXALIGN(itemsz);

                /*
                 * Walk up the bucket chain, looking for a page big enough for
                 * this item and all other accumulated items.  Exit if we reach
                 * the read page.
                 */
                while PageGetFreeSpaceForMultipleTuples(wpage, (nitups + 1) as c_int)
                    < (all_tups_size + itemsz)
                {
                    let mut next_wbuf: Buffer = InvalidBuffer;
                    let mut tups_moved: bool = false;

                    Assert!(!PageIsEmpty(wpage));

                    if wblkno == bucket_blkno {
                        retain_pin = true;
                    }

                    wblkno = (*wopaque).hasho_nextblkno;
                    Assert!(BlockNumberIsValid(wblkno));

                    /* don't need to move to next page if we reached the read page */
                    if wblkno != rblkno {
                        next_wbuf = _hash_getbuf_with_strategy(
                            rel,
                            wblkno,
                            HASH_WRITE,
                            LH_OVERFLOW_PAGE as c_int,
                            bstrategy,
                        );
                    }

                    if nitups > 0 {
                        Assert!(nitups == ndeletable);

                        /*
                         * This operation needs to log multiple tuples, prepare
                         * WAL for that.
                         */
                        if RelationNeedsWAL(rel) {
                            XLogEnsureRecordSpace(0, 3 + nitups as c_int);
                        }

                        START_CRIT_SECTION();

                        /*
                         * we have to insert tuples on the "write" page, being
                         * careful to preserve hashkey ordering.  (If we insert
                         * many tuples into the same "write" page it would be
                         * worth qsort'ing them).
                         */
                        _hash_pgaddmultitup(
                            rel,
                            wbuf,
                            itups.as_mut_ptr(),
                            itup_offsets.as_mut_ptr(),
                            nitups,
                        );
                        MarkBufferDirty(wbuf);

                        /* Delete tuples we already moved off read page */
                        PageIndexMultiDelete(rpage, deletable.as_mut_ptr(), ndeletable as c_int);
                        MarkBufferDirty(rbuf);

                        /* XLOG stuff */
                        if RelationNeedsWAL(rel) {
                            let recptr: XLogRecPtr;
                            let mut xlrec: xl_hash_move_page_contents = core::mem::zeroed();

                            xlrec.ntups = nitups;
                            xlrec.is_prim_bucket_same_wrt = wbuf == bucket_buf;

                            XLogBeginInsert();
                            XLogRegisterData(
                                &raw mut xlrec as *mut c_void,
                                SizeOfHashMovePageContents as c_int,
                            );

                            /*
                             * bucket buffer was not changed, but still needs to
                             * be registered to ensure that we can acquire a
                             * cleanup lock on it during replay.
                             */
                            if !xlrec.is_prim_bucket_same_wrt {
                                let flags: c_int = (REGBUF_STANDARD
                                    | REGBUF_NO_IMAGE
                                    | REGBUF_NO_CHANGE)
                                    as c_int;

                                XLogRegisterBuffer(0, bucket_buf, flags as uint8);
                            }

                            XLogRegisterBuffer(1, wbuf, REGBUF_STANDARD);
                            XLogRegisterBufData(
                                1,
                                itup_offsets.as_mut_ptr() as *mut c_void,
                                (nitups as usize * core::mem::size_of::<OffsetNumber>())
                                    as c_int,
                            );
                            i = 0;
                            while i < nitups as c_int {
                                XLogRegisterBufData(
                                    1,
                                    itups[i as usize] as *mut c_void,
                                    tups_size[i as usize] as c_int,
                                );
                                i += 1;
                            }

                            XLogRegisterBuffer(2, rbuf, REGBUF_STANDARD);
                            XLogRegisterBufData(
                                2,
                                deletable.as_mut_ptr() as *mut c_void,
                                (ndeletable as usize * core::mem::size_of::<OffsetNumber>())
                                    as c_int,
                            );

                            recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_MOVE_PAGE_CONTENTS);

                            PageSetLSN(BufferGetPage(wbuf), recptr);
                            PageSetLSN(BufferGetPage(rbuf), recptr);
                        }

                        END_CRIT_SECTION();

                        tups_moved = true;
                    }

                    /*
                     * release the lock on previous page after acquiring the lock
                     * on next page
                     */
                    if retain_pin {
                        LockBuffer(wbuf, BUFFER_LOCK_UNLOCK);
                    } else {
                        _hash_relbuf(rel, wbuf);
                    }

                    /* nothing more to do if we reached the read page */
                    if rblkno == wblkno {
                        _hash_relbuf(rel, rbuf);
                        return;
                    }

                    wbuf = next_wbuf;
                    wpage = BufferGetPage(wbuf);
                    wopaque = HashPageGetOpaque(wpage);
                    Assert!((*wopaque).hasho_bucket == bucket);
                    retain_pin = false;

                    /* be tidy */
                    i = 0;
                    while i < nitups as c_int {
                        pfree(itups[i as usize] as *mut c_void);
                        i += 1;
                    }
                    nitups = 0;
                    all_tups_size = 0;
                    ndeletable = 0;

                    /*
                     * after moving the tuples, rpage would have been compacted,
                     * so we need to rescan it.
                     */
                    if tups_moved {
                        continue 'readpage;
                    }
                }

                /* remember tuple for deletion from "read" page */
                deletable[ndeletable as usize] = roffnum;
                ndeletable += 1;

                /*
                 * we need a copy of index tuples as they can be freed as part of
                 * overflow page, however we need them to write a WAL record in
                 * _hash_freeovflpage.
                 */
                itups[nitups as usize] = CopyIndexTuple(itup);
                tups_size[nitups as usize] = itemsz;
                nitups += 1;
                all_tups_size += itemsz;

                roffnum = OffsetNumberNext(roffnum);
            }

            break 'readpage;
        }

        /*
         * If we reach here, there are no live tuples on the "read" page ---
         * it was empty when we got to it, or we moved them all.  So we can
         * just free the page without bothering with deleting tuples
         * individually.  Then advance to the previous "read" page.
         *
         * Tricky point here: if our read and write pages are adjacent in the
         * bucket chain, our write lock on wbuf will conflict with
         * _hash_freeovflpage's attempt to update the sibling links of the
         * removed page.  In that case, we don't need to lock it again.
         */
        rblkno = (*ropaque).hasho_prevblkno;
        Assert!(BlockNumberIsValid(rblkno));

        /* free this overflow page (releases rbuf) */
        _hash_freeovflpage(
            rel,
            bucket_buf,
            rbuf,
            wbuf,
            itups.as_mut_ptr(),
            itup_offsets.as_mut_ptr(),
            tups_size.as_mut_ptr(),
            nitups,
            bstrategy,
        );

        /* be tidy */
        i = 0;
        while i < nitups as c_int {
            pfree(itups[i as usize] as *mut c_void);
            i += 1;
        }

        /* are we freeing the page adjacent to wbuf? */
        if rblkno == wblkno {
            /* retain the pin on primary bucket page till end of bucket scan */
            if wblkno == bucket_blkno {
                LockBuffer(wbuf, BUFFER_LOCK_UNLOCK);
            } else {
                _hash_relbuf(rel, wbuf);
            }
            return;
        }

        rbuf = _hash_getbuf_with_strategy(
            rel,
            rblkno,
            HASH_WRITE,
            LH_OVERFLOW_PAGE as c_int,
            bstrategy,
        );
        rpage = BufferGetPage(rbuf);
        ropaque = HashPageGetOpaque(rpage);
        Assert!((*ropaque).hasho_bucket == bucket);
    }

    /* NOTREACHED */
}

// ---------------------------------------------------------------------------
// Constants/types referenced above that have no canonical home yet.
// ---------------------------------------------------------------------------

// MaxIndexTuplesPerPage comes from access/itup.h (mirrors spgist_private.rs).
// TODO(pg-port): real MaxIndexTuplesPerPage lives in access/itup.h.
const MaxIndexTuplesPerPage: usize = 407;
