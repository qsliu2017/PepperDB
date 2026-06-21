//! hash_xlog.rs
//!   WAL replay logic for hash index.
//! Translated 1:1 from postgres/src/backend/access/hash/hash_xlog.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/hash/hash_xlog.c

#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{uint16, uint32, uint8, Size};

use crate::access::common::bufmask::{
    mask_lp_flags, mask_page_content, mask_page_hint_bits, mask_page_lsn_and_checksum,
    mask_unused_space,
};
use crate::access::common::indextuple::{IndexTuple, IndexTupleData, IndexTupleSize};
use crate::access::rmgrdesc::hashdesc::{
    xl_hash_add_ovfl_page, xl_hash_delete, xl_hash_init_bitmap_page, xl_hash_init_meta_page,
    xl_hash_insert, xl_hash_move_page_contents, xl_hash_split_allocate_page,
    xl_hash_split_complete, xl_hash_squeeze_page, xl_hash_update_meta_page,
    xl_hash_vacuum_one_page, XLH_SPLIT_META_UPDATE_MASKS, XLH_SPLIT_META_UPDATE_SPLITPOINT,
    XLOG_HASH_ADD_OVFL_PAGE, XLOG_HASH_DELETE, XLOG_HASH_INIT_BITMAP_PAGE,
    XLOG_HASH_INIT_META_PAGE, XLOG_HASH_INSERT, XLOG_HASH_MOVE_PAGE_CONTENTS,
    XLOG_HASH_SPLIT_ALLOCATE_PAGE, XLOG_HASH_SPLIT_CLEANUP, XLOG_HASH_SPLIT_COMPLETE,
    XLOG_HASH_SPLIT_PAGE, XLOG_HASH_SQUEEZE_PAGE, XLOG_HASH_UPDATE_META_PAGE,
    XLOG_HASH_VACUUM_ONE_PAGE,
};
use crate::access::transam::xlogreader::{
    RelFileLocator, XLogReaderState, XLogRecGetBlockData, XLogRecGetBlockTag, XLogRecGetData,
    XLogRecGetInfo, XLogRecHasBlockRef, XLR_INFO_MASK,
};
use crate::access::transam::xlogutils::{
    InHotStandby, XLogInitBufferForRedo, XLogReadBufferForRedo, XLogReadBufferForRedoExtended,
    XLogRedoAction, BLK_NEEDS_REDO, BLK_NOTFOUND, BLK_RESTORED, RBM_NORMAL,
    RBM_ZERO_AND_CLEANUP_LOCK,
};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::common::relpath::{ForkNumber, INIT_FORKNUM};
use crate::c::MAXALIGN;
use crate::storage::block::{BlockNumber, BlockNumberIsValid, InvalidBlockNumber};
use crate::storage::buf::{Buffer, InvalidBuffer};
use crate::storage::bufpage::{
    Item, Page, PageAddItem, PageGetSpecialPointer, PageIndexMultiDelete, PageSetLSN,
};
use crate::storage::off::{InvalidOffsetNumber, OffsetNumber};
use crate::{elog, Assert};

// ---------------------------------------------------------------------------
// Declarations merged from access/hash.h needed by hash_xlog.c. These mirror
// the sibling hash files (hash.rs, hashovfl.rs): each translation unit carries
// the hash-specific helper definitions it needs until access/hash.h gets a
// single canonical home.
// ---------------------------------------------------------------------------

pub type Bucket = uint32;

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
pub const LH_PAGE_TYPE: uint16 =
    LH_OVERFLOW_PAGE | LH_BUCKET_PAGE | LH_BITMAP_PAGE | LH_META_PAGE;

pub const HASHO_PAGE_ID: uint16 = 0xFF80;

pub const BYTE_TO_BIT: uint32 = 3; /* 2^3 bits/byte */
pub const BITS_PER_MAP: uint32 = 32; /* Number of bits in uint32 */

/* HASH_MAX_BITMAPS = Min(BLCKSZ / 8, 1024) */
pub const HASH_MAX_BITMAPS: usize = {
    let a = (crate::pg_config::BLCKSZ as usize) / 8;
    if a < 1024 {
        a
    } else {
        1024
    }
};

pub const HASH_MAX_SPLITPOINT_GROUP: uint32 = 32;
pub const HASH_MAX_SPLITPOINTS: uint32 = ((HASH_MAX_SPLITPOINT_GROUP - 10) * 32) + 10;

#[repr(C)]
pub struct HashPageOpaqueData {
    pub hasho_prevblkno: BlockNumber, /* see above */
    pub hasho_nextblkno: BlockNumber, /* see above */
    pub hasho_bucket: Bucket,         /* bucket number this pg belongs to */
    pub hasho_flag: uint16,           /* page type code + flag bits, see above */
    pub hasho_page_id: uint16,        /* for identification of hash indexes */
}

pub type HashPageOpaque = *mut HashPageOpaqueData;

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

// Minimal page header shim to locate the special area / contents area
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

/* HashPageGetOpaque(page) -> PageGetSpecialPointer(page) */
#[inline]
unsafe fn HashPageGetOpaque(page: Page) -> HashPageOpaque {
    PageGetSpecialPointer(page) as HashPageOpaque
}

/* HashPageGetMeta(page) -> (HashMetaPage) PageGetContents(page) */
#[inline]
unsafe fn HashPageGetMeta(page: Page) -> HashMetaPage {
    let phdr = page as *mut u8;
    phdr.add(MAXALIGN(core::mem::size_of::<PageHeaderShim>())) as HashMetaPage
}

/* HashPageGetBitmap(page) -> (uint32 *) PageGetContents(page) */
#[inline]
unsafe fn HashPageGetBitmap(page: Page) -> *mut uint32 {
    let phdr = page as *mut u8;
    phdr.add(MAXALIGN(core::mem::size_of::<PageHeaderShim>())) as *mut uint32
}

/* SETBIT(A, N) */
#[inline]
unsafe fn SETBIT(A: *mut uint32, N: uint32) {
    let idx = (N / BITS_PER_MAP) as usize;
    *A.add(idx) |= 1u32 << (N % BITS_PER_MAP);
}

/* CLRBIT(A, N) */
#[inline]
unsafe fn CLRBIT(A: *mut uint32, N: uint32) {
    let idx = (N / BITS_PER_MAP) as usize;
    *A.add(idx) &= !(1u32 << (N % BITS_PER_MAP));
}

// ---------------------------------------------------------------------------
// Stubbed callees from other (not-yet-ported) translation units.
// ---------------------------------------------------------------------------

unsafe fn _hash_init_metabuffer(
    _buf: Buffer,
    _num_tuples: f64,
    _procid: crate::postgres_ext::Oid,
    _ffactor: uint16,
    _initpage: bool,
) {
    unimplemented!() // TODO(pg-port): real _hash_init_metabuffer lives in access/hashpage.c
}

unsafe fn _hash_initbitmapbuffer(_buf: Buffer, _bmsize: uint16, _initpage: bool) { crate::access::hash::hashovfl::_hash_initbitmapbuffer(_buf, _bmsize, _initpage) }

unsafe fn _hash_initbuf(
    _buf: Buffer,
    _max_bucket: uint32,
    _num_bucket: uint32,
    _flag: uint32,
    _initpage: bool,
) { crate::access::hash::hashpage::_hash_initbuf(_buf, _max_bucket, _num_bucket, _flag, _initpage) }

unsafe fn _hash_pageinit(_page: Page, _size: Size) { crate::access::hash::hashpage::_hash_pageinit(_page, _size) }

unsafe fn FlushOneBuffer(_buffer: Buffer) { crate::storage::buffer::bufmgr::FlushOneBuffer(_buffer) }

unsafe fn ResolveRecoveryConflictWithSnapshot(
    _snapshotConflictHorizon: crate::c::TransactionId,
    _isCatalogRel: bool,
    _locator: RelFileLocator,
) {
    unimplemented!() // TODO(pg-port): real ResolveRecoveryConflictWithSnapshot lives in storage/ipc/standby.c
}

// ---------------------------------------------------------------------------
// Buffer-manager accessors (storage/bufmgr.h). Stubbed as siblings do until
// storage/buffer/bufmgr.c is ported.
// ---------------------------------------------------------------------------

unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO(pg-port): real BufferGetPage lives in storage/buffer/bufmgr.c
}

unsafe fn BufferGetPageSize(_buffer: Buffer) -> Size { crate::access::nbtree::nbtpage::BufferGetPageSize(_buffer) }

unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO(pg-port): real BufferGetBlockNumber lives in storage/buffer/bufmgr.c
}

unsafe fn BufferIsValid(_buffer: Buffer) -> bool { crate::access::nbtree::nbtpage::BufferIsValid(_buffer) }

unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO(pg-port): real MarkBufferDirty lives in storage/buffer/bufmgr.c
}

unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO(pg-port): real UnlockReleaseBuffer lives in storage/buffer/bufmgr.c
}

/*
 * replay a hash index meta page
 */
unsafe fn hash_xlog_init_meta_page(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let page: Page;
    let metabuf: Buffer;
    let mut forknum: ForkNumber = 0;

    let xlrec = XLogRecGetData(record) as *mut xl_hash_init_meta_page;

    /* create the index' metapage */
    metabuf = XLogInitBufferForRedo(record as _, 0);
    Assert!(BufferIsValid(metabuf));
    _hash_init_metabuffer(
        metabuf,
        (*xlrec).num_tuples,
        (*xlrec).procid,
        (*xlrec).ffactor,
        true,
    );
    page = BufferGetPage(metabuf) as Page;
    PageSetLSN(page, lsn);
    MarkBufferDirty(metabuf);

    /*
     * Force the on-disk state of init forks to always be in sync with the
     * state in shared buffers.  See XLogReadBufferForRedoExtended.  We need
     * special handling for init forks as create index operations don't log a
     * full page image of the metapage.
     */
    XLogRecGetBlockTag(record, 0, null_mut(), &raw mut forknum, null_mut());
    if forknum == INIT_FORKNUM {
        FlushOneBuffer(metabuf);
    }

    /* all done */
    UnlockReleaseBuffer(metabuf);
}

/*
 * replay a hash index bitmap page
 */
unsafe fn hash_xlog_init_bitmap_page(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let bitmapbuf: Buffer;
    let mut metabuf: Buffer = InvalidBuffer;
    let page: Page;
    let metap: HashMetaPage;
    let num_buckets: uint32;
    let mut forknum: ForkNumber = 0;

    let xlrec = XLogRecGetData(record) as *mut xl_hash_init_bitmap_page;

    /*
     * Initialize bitmap page
     */
    bitmapbuf = XLogInitBufferForRedo(record as _, 0);
    _hash_initbitmapbuffer(bitmapbuf, (*xlrec).bmsize, true);
    PageSetLSN(BufferGetPage(bitmapbuf), lsn);
    MarkBufferDirty(bitmapbuf);

    /*
     * Force the on-disk state of init forks to always be in sync with the
     * state in shared buffers.  See XLogReadBufferForRedoExtended.  We need
     * special handling for init forks as create index operations don't log a
     * full page image of the metapage.
     */
    XLogRecGetBlockTag(record, 0, null_mut(), &raw mut forknum, null_mut());
    if forknum == INIT_FORKNUM {
        FlushOneBuffer(bitmapbuf);
    }
    UnlockReleaseBuffer(bitmapbuf);

    /* add the new bitmap page to the metapage's list of bitmaps */
    if XLogReadBufferForRedo(record as _, 1, &raw mut metabuf) == BLK_NEEDS_REDO {
        /*
         * Note: in normal operation, we'd update the metapage while still
         * holding lock on the bitmap page.  But during replay it's not
         * necessary to hold that lock, since nobody can see it yet; the
         * creating transaction hasn't yet committed.
         */
        page = BufferGetPage(metabuf);
        metap = HashPageGetMeta(page);

        num_buckets = (*metap).hashm_maxbucket + 1;
        (*metap).hashm_mapp[(*metap).hashm_nmaps as usize] = num_buckets + 1;
        (*metap).hashm_nmaps += 1;

        PageSetLSN(page, lsn);
        MarkBufferDirty(metabuf);

        XLogRecGetBlockTag(record, 1, null_mut(), &raw mut forknum, null_mut());
        if forknum == INIT_FORKNUM {
            FlushOneBuffer(metabuf);
        }
    }
    if BufferIsValid(metabuf) {
        UnlockReleaseBuffer(metabuf);
    }
}

/*
 * replay a hash index insert without split
 */
unsafe fn hash_xlog_insert(record: *mut XLogReaderState) {
    let metap: HashMetaPage;
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec = XLogRecGetData(record) as *mut xl_hash_insert;
    let mut buffer: Buffer = InvalidBuffer;
    let mut page: Page;

    if XLogReadBufferForRedo(record as _, 0, &raw mut buffer) == BLK_NEEDS_REDO {
        let mut datalen: Size = 0;
        let datapos = XLogRecGetBlockData(record, 0, &raw mut datalen);

        page = BufferGetPage(buffer);

        if PageAddItem(
            page,
            datapos as Item,
            datalen,
            (*xlrec).offnum,
            false,
            false,
        ) == InvalidOffsetNumber
        {
            elog!(PANIC, "hash_xlog_insert: failed to add item");
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    if XLogReadBufferForRedo(record as _, 1, &raw mut buffer) == BLK_NEEDS_REDO {
        /*
         * Note: in normal operation, we'd update the metapage while still
         * holding lock on the page we inserted into.  But during replay it's
         * not necessary to hold that lock, since no other index updates can
         * be happening concurrently.
         */
        page = BufferGetPage(buffer);
        metap = HashPageGetMeta(page);
        (*metap).hashm_ntuples += 1.0;

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

/*
 * replay addition of overflow page for hash index
 */
unsafe fn hash_xlog_add_ovfl_page(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec = XLogRecGetData(record) as *mut xl_hash_add_ovfl_page;
    let mut leftbuf: Buffer = InvalidBuffer;
    let ovflbuf: Buffer;
    let mut metabuf: Buffer = InvalidBuffer;
    let mut leftblk: BlockNumber = 0;
    let mut rightblk: BlockNumber = 0;
    let mut newmapblk: BlockNumber = InvalidBlockNumber;
    let ovflpage: Page;
    let ovflopaque: HashPageOpaque;
    let num_bucket: *mut uint32;
    let mut data: *mut c_char;
    let mut datalen: Size = 0; /* PG_USED_FOR_ASSERTS_ONLY */
    let mut new_bmpage: bool = false;

    XLogRecGetBlockTag(record, 0, null_mut(), null_mut(), &raw mut rightblk);
    XLogRecGetBlockTag(record, 1, null_mut(), null_mut(), &raw mut leftblk);

    ovflbuf = XLogInitBufferForRedo(record as _, 0);
    Assert!(BufferIsValid(ovflbuf));

    data = XLogRecGetBlockData(record, 0, &raw mut datalen);
    num_bucket = data as *mut uint32;
    Assert!(datalen == core::mem::size_of::<uint32>());
    _hash_initbuf(
        ovflbuf,
        InvalidBlockNumber,
        *num_bucket,
        LH_OVERFLOW_PAGE as uint32,
        true,
    );
    /* update backlink */
    ovflpage = BufferGetPage(ovflbuf);
    ovflopaque = HashPageGetOpaque(ovflpage);
    (*ovflopaque).hasho_prevblkno = leftblk;

    PageSetLSN(ovflpage, lsn);
    MarkBufferDirty(ovflbuf);

    if XLogReadBufferForRedo(record as _, 1, &raw mut leftbuf) == BLK_NEEDS_REDO {
        let leftpage: Page;
        let leftopaque: HashPageOpaque;

        leftpage = BufferGetPage(leftbuf);
        leftopaque = HashPageGetOpaque(leftpage);
        (*leftopaque).hasho_nextblkno = rightblk;

        PageSetLSN(leftpage, lsn);
        MarkBufferDirty(leftbuf);
    }

    if BufferIsValid(leftbuf) {
        UnlockReleaseBuffer(leftbuf);
    }
    UnlockReleaseBuffer(ovflbuf);

    /*
     * Note: in normal operation, we'd update the bitmap and meta page while
     * still holding lock on the overflow pages.  But during replay it's not
     * necessary to hold those locks, since no other index updates can be
     * happening concurrently.
     */
    if XLogRecHasBlockRef(record, 2) {
        let mut mapbuffer: Buffer = InvalidBuffer;

        if XLogReadBufferForRedo(record as _, 2, &raw mut mapbuffer) == BLK_NEEDS_REDO {
            let mappage: Page = BufferGetPage(mapbuffer) as Page;
            let freep: *mut uint32;
            let bitmap_page_bit: *mut uint32;

            freep = HashPageGetBitmap(mappage);

            data = XLogRecGetBlockData(record, 2, &raw mut datalen);
            bitmap_page_bit = data as *mut uint32;

            SETBIT(freep, *bitmap_page_bit);

            PageSetLSN(mappage, lsn);
            MarkBufferDirty(mapbuffer);
        }
        if BufferIsValid(mapbuffer) {
            UnlockReleaseBuffer(mapbuffer);
        }
    }

    if XLogRecHasBlockRef(record, 3) {
        let newmapbuf: Buffer;

        newmapbuf = XLogInitBufferForRedo(record as _, 3);

        _hash_initbitmapbuffer(newmapbuf, (*xlrec).bmsize, true);

        new_bmpage = true;
        newmapblk = BufferGetBlockNumber(newmapbuf);

        MarkBufferDirty(newmapbuf);
        PageSetLSN(BufferGetPage(newmapbuf), lsn);

        UnlockReleaseBuffer(newmapbuf);
    }

    if XLogReadBufferForRedo(record as _, 4, &raw mut metabuf) == BLK_NEEDS_REDO {
        let metap: HashMetaPage;
        let page: Page;
        let firstfree_ovflpage: *mut uint32;

        data = XLogRecGetBlockData(record, 4, &raw mut datalen);
        firstfree_ovflpage = data as *mut uint32;

        page = BufferGetPage(metabuf);
        metap = HashPageGetMeta(page);
        (*metap).hashm_firstfree = *firstfree_ovflpage;

        if !(*xlrec).bmpage_found {
            (*metap).hashm_spares[(*metap).hashm_ovflpoint as usize] += 1;

            if new_bmpage {
                Assert!(BlockNumberIsValid(newmapblk));

                (*metap).hashm_mapp[(*metap).hashm_nmaps as usize] = newmapblk;
                (*metap).hashm_nmaps += 1;
                (*metap).hashm_spares[(*metap).hashm_ovflpoint as usize] += 1;
            }
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(metabuf);
    }
    if BufferIsValid(metabuf) {
        UnlockReleaseBuffer(metabuf);
    }
}

/*
 * replay allocation of page for split operation
 */
unsafe fn hash_xlog_split_allocate_page(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec = XLogRecGetData(record) as *mut xl_hash_split_allocate_page;
    let mut oldbuf: Buffer = InvalidBuffer;
    let mut newbuf: Buffer = InvalidBuffer;
    let mut metabuf: Buffer = InvalidBuffer;
    let mut datalen: Size = 0; /* PG_USED_FOR_ASSERTS_ONLY */
    let mut data: *mut c_char;
    let action: XLogRedoAction;

    /*
     * To be consistent with normal operation, here we take cleanup locks on
     * both the old and new buckets even though there can't be any concurrent
     * inserts.
     */

    /* replay the record for old bucket */
    action = XLogReadBufferForRedoExtended(record as _, 0, RBM_NORMAL, true, &raw mut oldbuf);

    /*
     * Note that we still update the page even if it was restored from a full
     * page image, because the special space is not included in the image.
     */
    if action == BLK_NEEDS_REDO || action == BLK_RESTORED {
        let oldpage: Page;
        let oldopaque: HashPageOpaque;

        oldpage = BufferGetPage(oldbuf);
        oldopaque = HashPageGetOpaque(oldpage);

        (*oldopaque).hasho_flag = (*xlrec).old_bucket_flag;
        (*oldopaque).hasho_prevblkno = (*xlrec).new_bucket;

        PageSetLSN(oldpage, lsn);
        MarkBufferDirty(oldbuf);
    }

    /* replay the record for new bucket */
    XLogReadBufferForRedoExtended(
        record as _,
        1,
        RBM_ZERO_AND_CLEANUP_LOCK,
        true,
        &raw mut newbuf,
    );
    _hash_initbuf(
        newbuf,
        (*xlrec).new_bucket,
        (*xlrec).new_bucket,
        (*xlrec).new_bucket_flag as uint32,
        true,
    );
    MarkBufferDirty(newbuf);
    PageSetLSN(BufferGetPage(newbuf), lsn);

    /*
     * We can release the lock on old bucket early as well but doing here to
     * consistent with normal operation.
     */
    if BufferIsValid(oldbuf) {
        UnlockReleaseBuffer(oldbuf);
    }
    if BufferIsValid(newbuf) {
        UnlockReleaseBuffer(newbuf);
    }

    /*
     * Note: in normal operation, we'd update the meta page while still
     * holding lock on the old and new bucket pages.  But during replay it's
     * not necessary to hold those locks, since no other bucket splits can be
     * happening concurrently.
     */

    /* replay the record for metapage changes */
    if XLogReadBufferForRedo(record as _, 2, &raw mut metabuf) == BLK_NEEDS_REDO {
        let page: Page;
        let metap: HashMetaPage;

        page = BufferGetPage(metabuf);
        metap = HashPageGetMeta(page);
        (*metap).hashm_maxbucket = (*xlrec).new_bucket;

        data = XLogRecGetBlockData(record, 2, &raw mut datalen);

        if (*xlrec).flags & XLH_SPLIT_META_UPDATE_MASKS != 0 {
            let mut lowmask: uint32 = 0;
            let highmask: *mut uint32;

            /* extract low and high masks. */
            core::ptr::copy_nonoverlapping(
                data as *const u8,
                &raw mut lowmask as *mut u8,
                core::mem::size_of::<uint32>(),
            );
            highmask = (data as *mut c_char).add(core::mem::size_of::<uint32>()) as *mut uint32;

            /* update metapage */
            (*metap).hashm_lowmask = lowmask;
            (*metap).hashm_highmask = *highmask;

            data = data.add(core::mem::size_of::<uint32>() * 2);
        }

        if (*xlrec).flags & XLH_SPLIT_META_UPDATE_SPLITPOINT != 0 {
            let mut ovflpoint: uint32 = 0;
            let ovflpages: *mut uint32;

            /* extract information of overflow pages. */
            core::ptr::copy_nonoverlapping(
                data as *const u8,
                &raw mut ovflpoint as *mut u8,
                core::mem::size_of::<uint32>(),
            );
            ovflpages = (data as *mut c_char).add(core::mem::size_of::<uint32>()) as *mut uint32;

            /* update metapage */
            (*metap).hashm_spares[ovflpoint as usize] = *ovflpages;
            (*metap).hashm_ovflpoint = ovflpoint;
        }

        MarkBufferDirty(metabuf);
        PageSetLSN(BufferGetPage(metabuf), lsn);
    }

    if BufferIsValid(metabuf) {
        UnlockReleaseBuffer(metabuf);
    }
}

/*
 * replay of split operation
 */
unsafe fn hash_xlog_split_page(record: *mut XLogReaderState) {
    let mut buf: Buffer = InvalidBuffer;

    if XLogReadBufferForRedo(record as _, 0, &raw mut buf) != BLK_RESTORED {
        elog!(ERROR, "Hash split record did not contain a full-page image");
    }

    UnlockReleaseBuffer(buf);
}

/*
 * replay completion of split operation
 */
unsafe fn hash_xlog_split_complete(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec = XLogRecGetData(record) as *mut xl_hash_split_complete;
    let mut oldbuf: Buffer = InvalidBuffer;
    let mut newbuf: Buffer = InvalidBuffer;
    let action: XLogRedoAction;

    /* replay the record for old bucket */
    action = XLogReadBufferForRedo(record as _, 0, &raw mut oldbuf);

    /*
     * Note that we still update the page even if it was restored from a full
     * page image, because the bucket flag is not included in the image.
     */
    if action == BLK_NEEDS_REDO || action == BLK_RESTORED {
        let oldpage: Page;
        let oldopaque: HashPageOpaque;

        oldpage = BufferGetPage(oldbuf);
        oldopaque = HashPageGetOpaque(oldpage);

        (*oldopaque).hasho_flag = (*xlrec).old_bucket_flag;

        PageSetLSN(oldpage, lsn);
        MarkBufferDirty(oldbuf);
    }
    if BufferIsValid(oldbuf) {
        UnlockReleaseBuffer(oldbuf);
    }

    /* replay the record for new bucket */
    let action: XLogRedoAction = XLogReadBufferForRedo(record as _, 1, &raw mut newbuf);

    /*
     * Note that we still update the page even if it was restored from a full
     * page image, because the bucket flag is not included in the image.
     */
    if action == BLK_NEEDS_REDO || action == BLK_RESTORED {
        let newpage: Page;
        let nopaque: HashPageOpaque;

        newpage = BufferGetPage(newbuf);
        nopaque = HashPageGetOpaque(newpage);

        (*nopaque).hasho_flag = (*xlrec).new_bucket_flag;

        PageSetLSN(newpage, lsn);
        MarkBufferDirty(newbuf);
    }
    if BufferIsValid(newbuf) {
        UnlockReleaseBuffer(newbuf);
    }
}

/*
 * replay move of page contents for squeeze operation of hash index
 */
unsafe fn hash_xlog_move_page_contents(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xldata = XLogRecGetData(record) as *mut xl_hash_move_page_contents;
    let mut bucketbuf: Buffer = InvalidBuffer;
    let mut writebuf: Buffer = InvalidBuffer;
    let mut deletebuf: Buffer = InvalidBuffer;
    let action: XLogRedoAction;

    /*
     * Ensure we have a cleanup lock on primary bucket page before we start
     * with the actual replay operation.  This is to ensure that neither a
     * scan can start nor a scan can be already-in-progress during the replay
     * of this operation.  If we allow scans during this operation, then they
     * can miss some records or show the same record multiple times.
     */
    if (*xldata).is_prim_bucket_same_wrt {
        action = XLogReadBufferForRedoExtended(record as _, 1, RBM_NORMAL, true, &raw mut writebuf);
    } else {
        /*
         * we don't care for return value as the purpose of reading bucketbuf
         * is to ensure a cleanup lock on primary bucket page.
         */
        XLogReadBufferForRedoExtended(record as _, 0, RBM_NORMAL, true, &raw mut bucketbuf);

        action = XLogReadBufferForRedo(record as _, 1, &raw mut writebuf);
    }

    /* replay the record for adding entries in overflow buffer */
    if action == BLK_NEEDS_REDO {
        let writepage: Page;
        let begin: *mut c_char;
        let mut data: *mut c_char;
        let mut datalen: Size = 0;
        let mut ninserted: uint16 = 0;

        data = XLogRecGetBlockData(record, 1, &raw mut datalen);
        begin = data;

        writepage = BufferGetPage(writebuf) as Page;

        if (*xldata).ntups > 0 {
            let towrite = data as *mut OffsetNumber;

            data = data.add(core::mem::size_of::<OffsetNumber>() * (*xldata).ntups as usize);

            while (data as usize) - (begin as usize) < datalen {
                let itup = data as IndexTuple;
                let mut itemsz: Size;
                let l: OffsetNumber;

                itemsz = IndexTupleSize(itup as *const IndexTupleData);
                itemsz = MAXALIGN(itemsz);

                data = data.add(itemsz);

                l = PageAddItem(
                    writepage,
                    itup as Item,
                    itemsz,
                    *towrite.add(ninserted as usize),
                    false,
                    false,
                );
                if l == InvalidOffsetNumber {
                    elog!(
                        ERROR,
                        "hash_xlog_move_page_contents: failed to add item to hash index page, size {} bytes",
                        itemsz as c_int
                    );
                }

                ninserted += 1;
            }
        }

        /*
         * number of tuples inserted must be same as requested in REDO record.
         */
        Assert!(ninserted == (*xldata).ntups);

        PageSetLSN(writepage, lsn);
        MarkBufferDirty(writebuf);
    }

    /* replay the record for deleting entries from overflow buffer */
    if XLogReadBufferForRedo(record as _, 2, &raw mut deletebuf) == BLK_NEEDS_REDO {
        let page: Page;
        let ptr: *mut c_char;
        let mut len: Size = 0;

        ptr = XLogRecGetBlockData(record, 2, &raw mut len);

        page = BufferGetPage(deletebuf) as Page;

        if len > 0 {
            let unused: *mut OffsetNumber;
            let unend: *mut OffsetNumber;

            unused = ptr as *mut OffsetNumber;
            unend = (ptr as *mut c_char).add(len) as *mut OffsetNumber;

            if (unend as usize) > (unused as usize) {
                PageIndexMultiDelete(
                    page,
                    unused,
                    (unend.offset_from(unused)) as c_int,
                );
            }
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(deletebuf);
    }

    /*
     * Replay is complete, now we can release the buffers. We release locks at
     * end of replay operation to ensure that we hold lock on primary bucket
     * page till end of operation.  We can optimize by releasing the lock on
     * write buffer as soon as the operation for same is complete, if it is
     * not same as primary bucket page, but that doesn't seem to be worth
     * complicating the code.
     */
    if BufferIsValid(deletebuf) {
        UnlockReleaseBuffer(deletebuf);
    }

    if BufferIsValid(writebuf) {
        UnlockReleaseBuffer(writebuf);
    }

    if BufferIsValid(bucketbuf) {
        UnlockReleaseBuffer(bucketbuf);
    }
}

/*
 * replay squeeze page operation of hash index
 */
unsafe fn hash_xlog_squeeze_page(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xldata = XLogRecGetData(record) as *mut xl_hash_squeeze_page;
    let mut bucketbuf: Buffer = InvalidBuffer;
    let mut writebuf: Buffer = InvalidBuffer;
    let mut ovflbuf: Buffer = InvalidBuffer;
    let mut prevbuf: Buffer = InvalidBuffer;
    let mut mapbuf: Buffer = InvalidBuffer;
    let action: XLogRedoAction;

    /*
     * Ensure we have a cleanup lock on primary bucket page before we start
     * with the actual replay operation.  This is to ensure that neither a
     * scan can start nor a scan can be already-in-progress during the replay
     * of this operation.  If we allow scans during this operation, then they
     * can miss some records or show the same record multiple times.
     */
    if (*xldata).is_prim_bucket_same_wrt {
        action = XLogReadBufferForRedoExtended(record as _, 1, RBM_NORMAL, true, &raw mut writebuf);
    } else {
        /*
         * we don't care for return value as the purpose of reading bucketbuf
         * is to ensure a cleanup lock on primary bucket page.
         */
        XLogReadBufferForRedoExtended(record as _, 0, RBM_NORMAL, true, &raw mut bucketbuf);

        if (*xldata).ntups > 0 || (*xldata).is_prev_bucket_same_wrt {
            action = XLogReadBufferForRedo(record as _, 1, &raw mut writebuf);
        } else {
            action = BLK_NOTFOUND;
        }
    }

    /* replay the record for adding entries in overflow buffer */
    if action == BLK_NEEDS_REDO {
        let writepage: Page;
        let begin: *mut c_char;
        let mut data: *mut c_char;
        let mut datalen: Size = 0;
        let mut ninserted: uint16 = 0;
        let mut mod_wbuf: bool = false;

        data = XLogRecGetBlockData(record, 1, &raw mut datalen);
        begin = data;

        writepage = BufferGetPage(writebuf) as Page;

        if (*xldata).ntups > 0 {
            let towrite = data as *mut OffsetNumber;

            data = data.add(core::mem::size_of::<OffsetNumber>() * (*xldata).ntups as usize);

            while (data as usize) - (begin as usize) < datalen {
                let itup = data as IndexTuple;
                let mut itemsz: Size;
                let l: OffsetNumber;

                itemsz = IndexTupleSize(itup as *const IndexTupleData);
                itemsz = MAXALIGN(itemsz);

                data = data.add(itemsz);

                l = PageAddItem(
                    writepage,
                    itup as Item,
                    itemsz,
                    *towrite.add(ninserted as usize),
                    false,
                    false,
                );
                if l == InvalidOffsetNumber {
                    elog!(
                        ERROR,
                        "hash_xlog_squeeze_page: failed to add item to hash index page, size {} bytes",
                        itemsz as c_int
                    );
                }

                ninserted += 1;
            }

            mod_wbuf = true;
        } else {
            /*
             * Ensure that the required flags are set when there are no
             * tuples.  See _hash_freeovflpage().
             */
            Assert!((*xldata).is_prim_bucket_same_wrt || (*xldata).is_prev_bucket_same_wrt);
        }

        /*
         * number of tuples inserted must be same as requested in REDO record.
         */
        Assert!(ninserted == (*xldata).ntups);

        /*
         * if the page on which are adding tuples is a page previous to freed
         * overflow page, then update its nextblkno.
         */
        if (*xldata).is_prev_bucket_same_wrt {
            let writeopaque: HashPageOpaque = HashPageGetOpaque(writepage);

            (*writeopaque).hasho_nextblkno = (*xldata).nextblkno;
            mod_wbuf = true;
        }

        /* Set LSN and mark writebuf dirty iff it is modified */
        if mod_wbuf {
            PageSetLSN(writepage, lsn);
            MarkBufferDirty(writebuf);
        }
    }

    /* replay the record for initializing overflow buffer */
    if XLogReadBufferForRedo(record as _, 2, &raw mut ovflbuf) == BLK_NEEDS_REDO {
        let ovflpage: Page;
        let ovflopaque: HashPageOpaque;

        ovflpage = BufferGetPage(ovflbuf);

        _hash_pageinit(ovflpage, BufferGetPageSize(ovflbuf));

        ovflopaque = HashPageGetOpaque(ovflpage);

        (*ovflopaque).hasho_prevblkno = InvalidBlockNumber;
        (*ovflopaque).hasho_nextblkno = InvalidBlockNumber;
        (*ovflopaque).hasho_bucket = InvalidBucket;
        (*ovflopaque).hasho_flag = LH_UNUSED_PAGE;
        (*ovflopaque).hasho_page_id = HASHO_PAGE_ID;

        PageSetLSN(ovflpage, lsn);
        MarkBufferDirty(ovflbuf);
    }
    if BufferIsValid(ovflbuf) {
        UnlockReleaseBuffer(ovflbuf);
    }

    /* replay the record for page previous to the freed overflow page */
    if !(*xldata).is_prev_bucket_same_wrt
        && XLogReadBufferForRedo(record as _, 3, &raw mut prevbuf) == BLK_NEEDS_REDO
    {
        let prevpage: Page = BufferGetPage(prevbuf);
        let prevopaque: HashPageOpaque = HashPageGetOpaque(prevpage);

        (*prevopaque).hasho_nextblkno = (*xldata).nextblkno;

        PageSetLSN(prevpage, lsn);
        MarkBufferDirty(prevbuf);
    }
    if BufferIsValid(prevbuf) {
        UnlockReleaseBuffer(prevbuf);
    }

    /* replay the record for page next to the freed overflow page */
    if XLogRecHasBlockRef(record, 4) {
        let mut nextbuf: Buffer = InvalidBuffer;

        if XLogReadBufferForRedo(record as _, 4, &raw mut nextbuf) == BLK_NEEDS_REDO {
            let nextpage: Page = BufferGetPage(nextbuf);
            let nextopaque: HashPageOpaque = HashPageGetOpaque(nextpage);

            (*nextopaque).hasho_prevblkno = (*xldata).prevblkno;

            PageSetLSN(nextpage, lsn);
            MarkBufferDirty(nextbuf);
        }
        if BufferIsValid(nextbuf) {
            UnlockReleaseBuffer(nextbuf);
        }
    }

    if BufferIsValid(writebuf) {
        UnlockReleaseBuffer(writebuf);
    }

    if BufferIsValid(bucketbuf) {
        UnlockReleaseBuffer(bucketbuf);
    }

    /*
     * Note: in normal operation, we'd update the bitmap and meta page while
     * still holding lock on the primary bucket page and overflow pages.  But
     * during replay it's not necessary to hold those locks, since no other
     * index updates can be happening concurrently.
     */
    /* replay the record for bitmap page */
    if XLogReadBufferForRedo(record as _, 5, &raw mut mapbuf) == BLK_NEEDS_REDO {
        let mappage: Page = BufferGetPage(mapbuf) as Page;
        let freep: *mut uint32;
        let data: *mut c_char;
        let bitmap_page_bit: *mut uint32;
        let mut datalen: Size = 0;

        freep = HashPageGetBitmap(mappage);

        data = XLogRecGetBlockData(record, 5, &raw mut datalen);
        bitmap_page_bit = data as *mut uint32;

        CLRBIT(freep, *bitmap_page_bit);

        PageSetLSN(mappage, lsn);
        MarkBufferDirty(mapbuf);
    }
    if BufferIsValid(mapbuf) {
        UnlockReleaseBuffer(mapbuf);
    }

    /* replay the record for meta page */
    if XLogRecHasBlockRef(record, 6) {
        let mut metabuf: Buffer = InvalidBuffer;

        if XLogReadBufferForRedo(record as _, 6, &raw mut metabuf) == BLK_NEEDS_REDO {
            let metap: HashMetaPage;
            let page: Page;
            let data: *mut c_char;
            let firstfree_ovflpage: *mut uint32;
            let mut datalen: Size = 0;

            data = XLogRecGetBlockData(record, 6, &raw mut datalen);
            firstfree_ovflpage = data as *mut uint32;

            page = BufferGetPage(metabuf);
            metap = HashPageGetMeta(page);
            (*metap).hashm_firstfree = *firstfree_ovflpage;

            PageSetLSN(page, lsn);
            MarkBufferDirty(metabuf);
        }
        if BufferIsValid(metabuf) {
            UnlockReleaseBuffer(metabuf);
        }
    }
}

/*
 * replay delete operation of hash index
 */
unsafe fn hash_xlog_delete(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xldata = XLogRecGetData(record) as *mut xl_hash_delete;
    let mut bucketbuf: Buffer = InvalidBuffer;
    let mut deletebuf: Buffer = InvalidBuffer;
    let page: Page;
    let action: XLogRedoAction;

    /*
     * Ensure we have a cleanup lock on primary bucket page before we start
     * with the actual replay operation.  This is to ensure that neither a
     * scan can start nor a scan can be already-in-progress during the replay
     * of this operation.  If we allow scans during this operation, then they
     * can miss some records or show the same record multiple times.
     */
    if (*xldata).is_primary_bucket_page {
        action = XLogReadBufferForRedoExtended(record as _, 1, RBM_NORMAL, true, &raw mut deletebuf);
    } else {
        /*
         * we don't care for return value as the purpose of reading bucketbuf
         * is to ensure a cleanup lock on primary bucket page.
         */
        XLogReadBufferForRedoExtended(record as _, 0, RBM_NORMAL, true, &raw mut bucketbuf);

        action = XLogReadBufferForRedo(record as _, 1, &raw mut deletebuf);
    }

    /* replay the record for deleting entries in bucket page */
    if action == BLK_NEEDS_REDO {
        let ptr: *mut c_char;
        let mut len: Size = 0;

        ptr = XLogRecGetBlockData(record, 1, &raw mut len);

        page = BufferGetPage(deletebuf) as Page;

        if len > 0 {
            let unused: *mut OffsetNumber;
            let unend: *mut OffsetNumber;

            unused = ptr as *mut OffsetNumber;
            unend = (ptr as *mut c_char).add(len) as *mut OffsetNumber;

            if (unend as usize) > (unused as usize) {
                PageIndexMultiDelete(
                    page,
                    unused,
                    (unend.offset_from(unused)) as c_int,
                );
            }
        }

        /*
         * Mark the page as not containing any LP_DEAD items only if
         * clear_dead_marking flag is set to true. See comments in
         * hashbucketcleanup() for details.
         */
        if (*xldata).clear_dead_marking {
            let pageopaque: HashPageOpaque;

            pageopaque = HashPageGetOpaque(page);
            (*pageopaque).hasho_flag &= !LH_PAGE_HAS_DEAD_TUPLES;
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(deletebuf);
    }
    if BufferIsValid(deletebuf) {
        UnlockReleaseBuffer(deletebuf);
    }

    if BufferIsValid(bucketbuf) {
        UnlockReleaseBuffer(bucketbuf);
    }
}

/*
 * replay split cleanup flag operation for primary bucket page.
 */
unsafe fn hash_xlog_split_cleanup(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut buffer: Buffer = InvalidBuffer;
    let page: Page;

    if XLogReadBufferForRedo(record as _, 0, &raw mut buffer) == BLK_NEEDS_REDO {
        let bucket_opaque: HashPageOpaque;

        page = BufferGetPage(buffer) as Page;

        bucket_opaque = HashPageGetOpaque(page);
        (*bucket_opaque).hasho_flag &= !LH_BUCKET_NEEDS_SPLIT_CLEANUP;
        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

/*
 * replay for update meta page
 */
unsafe fn hash_xlog_update_meta_page(record: *mut XLogReaderState) {
    let metap: HashMetaPage;
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xldata = XLogRecGetData(record) as *mut xl_hash_update_meta_page;
    let mut metabuf: Buffer = InvalidBuffer;
    let page: Page;

    if XLogReadBufferForRedo(record as _, 0, &raw mut metabuf) == BLK_NEEDS_REDO {
        page = BufferGetPage(metabuf);
        metap = HashPageGetMeta(page);

        (*metap).hashm_ntuples = (*xldata).ntuples;

        PageSetLSN(page, lsn);
        MarkBufferDirty(metabuf);
    }
    if BufferIsValid(metabuf) {
        UnlockReleaseBuffer(metabuf);
    }
}

/*
 * replay delete operation in hash index to remove
 * tuples marked as DEAD during index tuple insertion.
 */
unsafe fn hash_xlog_vacuum_one_page(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xldata: *mut xl_hash_vacuum_one_page;
    let mut buffer: Buffer = InvalidBuffer;
    let mut metabuf: Buffer = InvalidBuffer;
    let page: Page;
    let action: XLogRedoAction;
    let pageopaque: HashPageOpaque;
    let toDelete: *mut OffsetNumber;

    xldata = XLogRecGetData(record) as *mut xl_hash_vacuum_one_page;
    /* offsets[] is the flexible array member immediately past the header */
    toDelete = (xldata as *mut c_char).add(core::mem::size_of::<xl_hash_vacuum_one_page>())
        as *mut OffsetNumber;

    /*
     * If we have any conflict processing to do, it must happen before we
     * update the page.
     *
     * Hash index records that are marked as LP_DEAD and being removed during
     * hash index tuple insertion can conflict with standby queries. You might
     * think that vacuum records would conflict as well, but we've handled
     * that already.  XLOG_HEAP2_PRUNE_VACUUM_SCAN records provide the highest
     * xid cleaned by the vacuum of the heap and so we can resolve any
     * conflicts just once when that arrives.  After that we know that no
     * conflicts exist from individual hash index vacuum records on that
     * index.
     */
    if InHotStandby() {
        let mut rlocator: RelFileLocator = core::mem::zeroed();

        XLogRecGetBlockTag(record, 0, &raw mut rlocator, null_mut(), null_mut());
        ResolveRecoveryConflictWithSnapshot(
            (*xldata).snapshotConflictHorizon,
            (*xldata).isCatalogRel,
            rlocator,
        );
    }

    action = XLogReadBufferForRedoExtended(record as _, 0, RBM_NORMAL, true, &raw mut buffer);

    if action == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer) as Page;

        PageIndexMultiDelete(page, toDelete, (*xldata).ntuples as c_int);

        /*
         * Mark the page as not containing any LP_DEAD items. See comments in
         * _hash_vacuum_one_page() for details.
         */
        pageopaque = HashPageGetOpaque(page);
        (*pageopaque).hasho_flag &= !LH_PAGE_HAS_DEAD_TUPLES;

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    if XLogReadBufferForRedo(record as _, 1, &raw mut metabuf) == BLK_NEEDS_REDO {
        let metapage: Page;
        let metap: HashMetaPage;

        metapage = BufferGetPage(metabuf);
        metap = HashPageGetMeta(metapage);

        (*metap).hashm_ntuples -= (*xldata).ntuples as f64;

        PageSetLSN(metapage, lsn);
        MarkBufferDirty(metabuf);
    }
    if BufferIsValid(metabuf) {
        UnlockReleaseBuffer(metabuf);
    }
}

pub unsafe fn hash_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    match info {
        XLOG_HASH_INIT_META_PAGE => {
            hash_xlog_init_meta_page(record);
        }
        XLOG_HASH_INIT_BITMAP_PAGE => {
            hash_xlog_init_bitmap_page(record);
        }
        XLOG_HASH_INSERT => {
            hash_xlog_insert(record);
        }
        XLOG_HASH_ADD_OVFL_PAGE => {
            hash_xlog_add_ovfl_page(record);
        }
        XLOG_HASH_SPLIT_ALLOCATE_PAGE => {
            hash_xlog_split_allocate_page(record);
        }
        XLOG_HASH_SPLIT_PAGE => {
            hash_xlog_split_page(record);
        }
        XLOG_HASH_SPLIT_COMPLETE => {
            hash_xlog_split_complete(record);
        }
        XLOG_HASH_MOVE_PAGE_CONTENTS => {
            hash_xlog_move_page_contents(record);
        }
        XLOG_HASH_SQUEEZE_PAGE => {
            hash_xlog_squeeze_page(record);
        }
        XLOG_HASH_DELETE => {
            hash_xlog_delete(record);
        }
        XLOG_HASH_SPLIT_CLEANUP => {
            hash_xlog_split_cleanup(record);
        }
        XLOG_HASH_UPDATE_META_PAGE => {
            hash_xlog_update_meta_page(record);
        }
        XLOG_HASH_VACUUM_ONE_PAGE => {
            hash_xlog_vacuum_one_page(record);
        }
        _ => {
            elog!(PANIC, "hash_redo: unknown op code {}", info);
        }
    }
}

/*
 * Mask a hash page before performing consistency checks on it.
 */
pub unsafe fn hash_mask(pagedata: *mut c_char, blkno: BlockNumber) {
    let page: Page = pagedata as Page;
    let opaque: HashPageOpaque;
    let pagetype: c_int;

    mask_page_lsn_and_checksum(page);

    mask_page_hint_bits(page);
    mask_unused_space(page);

    opaque = HashPageGetOpaque(page);

    pagetype = ((*opaque).hasho_flag & LH_PAGE_TYPE) as c_int;
    if pagetype == LH_UNUSED_PAGE as c_int {
        /*
         * Mask everything on a UNUSED page.
         */
        mask_page_content(page);
    } else if pagetype == LH_BUCKET_PAGE as c_int || pagetype == LH_OVERFLOW_PAGE as c_int {
        /*
         * In hash bucket and overflow pages, it is possible to modify the
         * LP_FLAGS without emitting any WAL record. Hence, mask the line
         * pointer flags. See hashgettuple(), _hash_kill_items() for details.
         */
        mask_lp_flags(page);
    }

    /*
     * It is possible that the hint bit LH_PAGE_HAS_DEAD_TUPLES may remain
     * unlogged. So, mask it. See _hash_kill_items() for details.
     */
    (*opaque).hasho_flag &= !LH_PAGE_HAS_DEAD_TUPLES;
}
