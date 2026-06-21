//! brin_pageops.c
//!     Page-handling routines for BRIN indexes
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!       src/backend/access/brin/brin_pageops.c
//!
//! Companion header: src/include/access/brin_pageops.h

use crate::prelude::*;
use crate::utils::elog::DEBUG2;

use crate::storage::block::BlockNumber;

// ---------------------------------------------------------------------------
// Stub type aliases / imports from not-yet-ported modules.
// ---------------------------------------------------------------------------

use crate::utils::rel::Relation;
type Buffer = c_int;
type Page = *mut c_char;
type PageHeader = *mut c_void;
type OffsetNumber = uint16;
type ItemId = *mut c_void;
type Item = *mut c_char;
use crate::access::brin::brin_revmap::BrinRevmap;
use crate::access::brin::brin_tuple::BrinTuple;
type XLogRecPtr = u64;

#[repr(C)]
struct ItemPointerData {
    // opaque layout; faithful structure handled where ItemPointerSet is used
    _bytes: [u8; 6],
}

#[repr(C)]
struct BrinMetaPageData {
    brinMagic: u32,
    brinVersion: u32,
    pagesPerRange: BlockNumber,
    lastRevmapPage: BlockNumber,
}

#[repr(C)]
struct BrinSpecialSpace {
    flags: uint16,
}

// XLOG record structs used by this file.
#[repr(C)]
struct xl_brin_samepage_update {
    offnum: OffsetNumber,
}

#[repr(C)]
struct xl_brin_insert {
    heapBlk: BlockNumber,
    pagesPerRange: BlockNumber,
    offnum: OffsetNumber,
}

#[repr(C)]
struct xl_brin_update {
    oldOffnum: OffsetNumber,
    insert: xl_brin_insert,
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const InvalidBlockNumber: BlockNumber = 0xFFFFFFFF;
const InvalidBuffer: Buffer = 0;
const InvalidOffsetNumber: OffsetNumber = 0;
const FirstOffsetNumber: OffsetNumber = 1;

const BLCKSZ: usize = 8192;

// Buffer lock modes
const BUFFER_LOCK_UNLOCK: c_int = 0;
const BUFFER_LOCK_SHARE: c_int = 1;
const BUFFER_LOCK_EXCLUSIVE: c_int = 2;

// lock modes
const ShareLock: c_int = 5;
const ExclusiveLock: c_int = 7;

// BRIN page types/flags
const BRIN_PAGETYPE_META: uint16 = 0xF091;
const BRIN_PAGETYPE_REVMAP: uint16 = 0xF092;
const BRIN_PAGETYPE_REGULAR: uint16 = 0xF093;

const BRIN_EVACUATE_PAGE: uint16 = 0x0001;

const BRIN_META_MAGIC: u32 = 0xA8109CFA;

// P_NEW
const P_NEW: BlockNumber = InvalidBlockNumber;

// XLOG info bits
const XLOG_BRIN_INSERT: uint8 = 0x00;
const XLOG_BRIN_UPDATE: uint8 = 0x10;
const XLOG_BRIN_SAMEPAGE_UPDATE: uint8 = 0x20;
const XLOG_BRIN_INIT_PAGE: uint8 = 0x80;

const RM_BRIN_ID: u8 = 13;

// REGBUF flags
const REGBUF_STANDARD: u8 = 0x04;
const REGBUF_WILL_INIT: u8 = 0x01;

// SizeOf macros
const SizeOfBrinSamepageUpdate: Size = std::mem::size_of::<xl_brin_samepage_update>();
const SizeOfBrinInsert: Size = std::mem::size_of::<xl_brin_insert>();
const SizeOfBrinUpdate: Size = std::mem::size_of::<xl_brin_update>();

// ELOG levels (ERROR, DEBUG2) come from crate::prelude (crate::utils::elog).

// Sizes referenced by BrinMaxItemSize.
const SizeOfPageHeaderData: usize = 24; // MAXALIGN(offsetof(PageHeaderData, pd_linp))
#[repr(C)]
struct ItemIdData {
    bits: u32,
}

// MAXALIGN helpers (MAXIMUM_ALIGNOF = 8)
const fn MAXALIGN(len: usize) -> usize {
    (len + 7) & !7
}
const fn MAXALIGN_DOWN(len: usize) -> usize {
    len & !7
}

/*
 * Maximum size of an entry in a BRIN_PAGETYPE_REGULAR page.  We can tolerate
 * a single item per page, unlike other index AMs.
 */
const BrinMaxItemSize: usize = MAXALIGN_DOWN(
    BLCKSZ
        - (MAXALIGN(SizeOfPageHeaderData + std::mem::size_of::<ItemIdData>())
            + MAXALIGN(std::mem::size_of::<BrinSpecialSpace>())),
);

// ---------------------------------------------------------------------------
// Critical-section / xlog macros expressed as helper calls.
// ---------------------------------------------------------------------------

macro_rules! START_CRIT_SECTION {
    () => {
        StartCritSection()
    };
}
macro_rules! END_CRIT_SECTION {
    () => {
        EndCritSection()
    };
}

// ---------------------------------------------------------------------------
// Local stubs for unported helpers.
// ---------------------------------------------------------------------------

unsafe fn StartCritSection() {
    // TODO: miscadmin.h (START_CRIT_SECTION)
}
unsafe fn EndCritSection() {
    // TODO: miscadmin.h (END_CRIT_SECTION)
}

unsafe fn BufferIsValid(bufnum: Buffer) -> bool {
    bufnum != InvalidBuffer // TODO: storage/bufmgr.h
}
unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn MarkBufferDirtyHint(_buffer: Buffer, _buffer_std: bool) { crate::storage::buffer::bufmgr::MarkBufferDirtyHint(_buffer, _buffer_std) }
unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn ReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn ReadBuffer(_reln: Relation, _blockNum: BlockNumber) -> Buffer {
    unimplemented!() // TODO: storage/bufmgr.h
}

unsafe fn PageGetItemId(_page: Page, _offsetNumber: OffsetNumber) -> ItemId {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetItem(_page: Page, _itemId: ItemId) -> Item {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetMaxOffsetNumber(_page: Page) -> OffsetNumber {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageIsNew(_page: Page) -> bool {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetContents(_page: Page) -> *mut c_char { crate::storage::bufpage::PageGetContents(_page) }
unsafe fn PageGetFreeSpace(_page: Page) -> Size {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetExactFreeSpace(_page: Page) -> Size {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageInit(_page: Page, _pageSize: Size, _specialSize: Size) {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) { crate::storage::bufpage::PageSetLSN(_page, _lsn) }
unsafe fn PageAddItem(
    _page: Page,
    _item: Item,
    _size: Size,
    _offsetNumber: OffsetNumber,
    _overwrite: bool,
    _is_heap: bool,
) -> OffsetNumber { crate::storage::bufpage::PageAddItem(_page, _item, _size, _offsetNumber, _overwrite, _is_heap) }
unsafe fn PageIndexTupleOverwrite(
    _page: Page,
    _offnum: OffsetNumber,
    _newtup: Item,
    _newsize: Size,
) -> bool {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageIndexTupleDeleteNoCompact(_page: Page, _offnum: OffsetNumber) {
    unimplemented!() // TODO: storage/bufpage.h
}

unsafe fn ItemIdIsNormal(_itemId: ItemId) -> bool { unimplemented!() }
unsafe fn ItemIdIsUsed(_itemId: ItemId) -> bool {
    unimplemented!() // TODO: storage/itemid.h
}
unsafe fn ItemIdGetLength(_itemId: ItemId) -> Size {
    unimplemented!() // TODO: storage/itemid.h
}

unsafe fn ItemPointerSet(_pointer: *mut ItemPointerData, _blockNumber: BlockNumber, _offNum: OffsetNumber) {
    unimplemented!() // TODO: storage/itemptr.h
}

unsafe fn RelationGetRelationName(_rel: Relation) -> *const c_char {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn RelationNeedsWAL(_rel: Relation) -> bool { crate::access::nbtree::nbtdedup::RelationNeedsWAL(_rel) }
unsafe fn RelationGetTargetBlock(_rel: Relation) -> BlockNumber {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn RelationSetTargetBlock(_rel: Relation, _block: BlockNumber) {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn RELATION_IS_LOCAL(_relation: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn LockRelationForExtension(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: storage/lmgr.h
}
unsafe fn UnlockRelationForExtension(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: storage/lmgr.h
}

unsafe fn GetPageWithFreeSpace(_rel: Relation, _spaceNeeded: Size) -> BlockNumber { crate::storage::freespace::freespace::GetPageWithFreeSpace(_rel, _spaceNeeded) }
unsafe fn RecordPageWithFreeSpace(_rel: Relation, _heapBlk: BlockNumber, _spaceAvail: Size) { crate::storage::freespace::freespace::RecordPageWithFreeSpace(_rel, _heapBlk, _spaceAvail) }
unsafe fn RecordAndGetPageWithFreeSpace(
    _rel: Relation,
    _oldPage: BlockNumber,
    _oldSpaceAvail: Size,
    _spaceNeeded: Size,
) -> BlockNumber { crate::storage::freespace::freespace::RecordAndGetPageWithFreeSpace(_rel, _oldPage, _oldSpaceAvail, _spaceNeeded) }
unsafe fn FreeSpaceMapVacuumRange(_rel: Relation, _start: BlockNumber, _end: BlockNumber) { crate::storage::freespace::freespace::FreeSpaceMapVacuumRange(_rel, _start, _end) }

unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterData(_data: *mut c_void, _len: Size) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterBuffer(_block_id: u8, _buffer: Buffer, _flags: u8) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterBufData(_block_id: u8, _data: *const c_void, _len: Size) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogInsert(_rmid: u8, _info: uint8) -> XLogRecPtr {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn log_newpage_buffer(_buffer: Buffer, _page_std: bool) -> XLogRecPtr { crate::access::transam::xloginsert::log_newpage_buffer(_buffer, _page_std) }

// brin_revmap.h
unsafe fn brinRevmapExtend(_revmap: *mut BrinRevmap, _heapBlk: BlockNumber) { crate::access::brin::brin_revmap::brinRevmapExtend(_revmap, _heapBlk) }
unsafe fn brinLockRevmapPageForUpdate(_revmap: *mut BrinRevmap, _heapBlk: BlockNumber) -> Buffer { crate::access::brin::brin_revmap::brinLockRevmapPageForUpdate(_revmap, _heapBlk) }
unsafe fn brinSetHeapBlockItemptr(
    _buf: Buffer,
    _pagesPerRange: BlockNumber,
    _heapBlk: BlockNumber,
    _tid: ItemPointerData,
) { unimplemented!() }

// brin_tuple.c
unsafe fn brin_tuples_equal(_a: *const BrinTuple, _alen: Size, _b: *const BrinTuple, _blen: Size) -> bool { crate::access::brin::brin_tuple::brin_tuples_equal(_a, _alen, _b, _blen) }
unsafe fn brin_copy_tuple(
    _tuple: *mut BrinTuple,
    _len: Size,
    _dest: *mut BrinTuple,
    _destsz: *mut Size,
) -> *mut BrinTuple { crate::access::brin::brin_tuple::brin_copy_tuple(_tuple, _len, _dest, _destsz) }

// brin_page.h accessor macros (expressed as helpers).
unsafe fn BRIN_IS_REGULAR_PAGE(_page: Page) -> bool { crate::access::brin::brin_page::BRIN_IS_REGULAR_PAGE(_page) }
unsafe fn BRIN_IS_META_PAGE(_page: Page) -> bool { crate::access::brin::brin_page::BRIN_IS_META_PAGE(_page) }
unsafe fn BRIN_IS_REVMAP_PAGE(_page: Page) -> bool { crate::access::brin::brin_page::BRIN_IS_REVMAP_PAGE(_page) }
/// Returns a mutable reference to the flags field of the page's special space,
/// matching the C lvalue macro BrinPageFlags(page).
unsafe fn BrinPageFlags(_page: Page) -> &'static mut uint16 {
    unimplemented!() // TODO: access/brin_page.h
}
/// Returns a mutable reference to the type field of the page's special space,
/// matching the C lvalue macro BrinPageType(page).
unsafe fn BrinPageType(_page: Page) -> &'static mut uint16 {
    unimplemented!() // TODO: access/brin_page.h
}

// bt_blkno accessor for a BrinTuple.
unsafe fn brin_tuple_blkno(_tup: *mut BrinTuple) -> BlockNumber {
    unimplemented!() // TODO: access/brin_tuple.h (tup->bt_blkno)
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

/*
 * Update tuple origtup (size origsz), located in offset oldoff of buffer
 * oldbuf, to newtup (size newsz) as summary tuple for the page range starting
 * at heapBlk.  oldbuf must not be locked on entry, and is not locked at exit.
 *
 * If samepage is true, attempt to put the new tuple in the same page, but if
 * there's no room, use some other one.
 *
 * If the update is successful, return true; the revmap is updated to point to
 * the new tuple.  If the update is not done for whatever reason, return false.
 * Caller may retry the update if this happens.
 */
pub unsafe fn brin_doupdate(
    idxrel: Relation,
    pagesPerRange: BlockNumber,
    revmap: *mut BrinRevmap,
    heapBlk: BlockNumber,
    oldbuf: Buffer,
    oldoff: OffsetNumber,
    origtup: *const BrinTuple,
    origsz: Size,
    newtup: *const BrinTuple,
    newsz: Size,
    samepage: bool,
) -> bool {
    let oldpage: Page;
    let oldlp: ItemId;
    let oldtup: *mut BrinTuple;
    let oldsz: Size;
    let mut newbuf: Buffer;
    let mut newblk: BlockNumber = InvalidBlockNumber;
    let mut extended: bool = false;

    Assert!(newsz == MAXALIGN(newsz));

    /* If the item is oversized, don't bother. */
    if newsz > BrinMaxItemSize {
        elog!(
            ERROR,
            "index row size {} exceeds maximum {} for index \"{}\"",
            newsz,
            BrinMaxItemSize,
            CStr_to_str(RelationGetRelationName(idxrel))
        );
        return false; /* keep compiler quiet */
    }

    /* make sure the revmap is long enough to contain the entry we need */
    brinRevmapExtend(revmap, heapBlk);

    if !samepage {
        /* need a page on which to put the item */
        newbuf = brin_getinsertbuffer(idxrel, oldbuf, newsz, &mut extended);
        if !BufferIsValid(newbuf) {
            Assert!(!extended);
            return false;
        }

        /*
         * Note: it's possible (though unlikely) that the returned newbuf is
         * the same as oldbuf, if brin_getinsertbuffer determined that the old
         * buffer does in fact have enough space.
         */
        if newbuf == oldbuf {
            Assert!(!extended);
            newbuf = InvalidBuffer;
        } else {
            newblk = BufferGetBlockNumber(newbuf);
        }
    } else {
        LockBuffer(oldbuf, BUFFER_LOCK_EXCLUSIVE);
        newbuf = InvalidBuffer;
        extended = false;
    }
    oldpage = BufferGetPage(oldbuf);
    oldlp = PageGetItemId(oldpage, oldoff);

    /*
     * Check that the old tuple wasn't updated concurrently: it might have
     * moved someplace else entirely, and for that matter the whole page
     * might've become a revmap page.  Note that in the first two cases
     * checked here, the "oldlp" we just calculated is garbage; but
     * PageGetItemId() is simple enough that it was safe to do that
     * calculation anyway.
     */
    if !BRIN_IS_REGULAR_PAGE(oldpage)
        || oldoff > PageGetMaxOffsetNumber(oldpage)
        || !ItemIdIsNormal(oldlp)
    {
        LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);

        /*
         * If this happens, and the new buffer was obtained by extending the
         * relation, then we need to ensure we don't leave it uninitialized or
         * forget about it.
         */
        if BufferIsValid(newbuf) {
            if extended {
                brin_initialize_empty_new_buffer(idxrel, newbuf);
            }
            UnlockReleaseBuffer(newbuf);
            if extended {
                FreeSpaceMapVacuumRange(idxrel, newblk, newblk + 1);
            }
        }
        return false;
    }

    oldsz = ItemIdGetLength(oldlp);
    oldtup = PageGetItem(oldpage, oldlp) as *mut BrinTuple;

    /*
     * ... or it might have been updated in place to different contents.
     */
    if !brin_tuples_equal(oldtup, oldsz, origtup, origsz) {
        LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);
        if BufferIsValid(newbuf) {
            /* As above, initialize and record new page if we got one */
            if extended {
                brin_initialize_empty_new_buffer(idxrel, newbuf);
            }
            UnlockReleaseBuffer(newbuf);
            if extended {
                FreeSpaceMapVacuumRange(idxrel, newblk, newblk + 1);
            }
        }
        return false;
    }

    /*
     * Great, the old tuple is intact.  We can proceed with the update.
     *
     * If there's enough room in the old page for the new tuple, replace it.
     *
     * Note that there might now be enough space on the page even though the
     * caller told us there isn't, if a concurrent update moved another tuple
     * elsewhere or replaced a tuple with a smaller one.
     */
    if (*BrinPageFlags(oldpage) & BRIN_EVACUATE_PAGE) == 0
        && brin_can_do_samepage_update(oldbuf, origsz, newsz)
    {
        START_CRIT_SECTION!();
        if !PageIndexTupleOverwrite(oldpage, oldoff, newtup as *mut BrinTuple as Item, newsz) {
            elog!(ERROR, "failed to replace BRIN tuple");
        }
        MarkBufferDirty(oldbuf);

        /* XLOG stuff */
        if RelationNeedsWAL(idxrel) {
            let mut xlrec: xl_brin_samepage_update = std::mem::zeroed();
            let recptr: XLogRecPtr;
            let info: uint8 = XLOG_BRIN_SAMEPAGE_UPDATE;

            xlrec.offnum = oldoff;

            XLogBeginInsert();
            XLogRegisterData(&mut xlrec as *mut _ as *mut c_void, SizeOfBrinSamepageUpdate);

            XLogRegisterBuffer(0, oldbuf, REGBUF_STANDARD);
            XLogRegisterBufData(0, newtup as *const c_void, newsz);

            recptr = XLogInsert(RM_BRIN_ID, info);

            PageSetLSN(oldpage, recptr);
        }

        END_CRIT_SECTION!();

        LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);

        if BufferIsValid(newbuf) {
            /* As above, initialize and record new page if we got one */
            if extended {
                brin_initialize_empty_new_buffer(idxrel, newbuf);
            }
            UnlockReleaseBuffer(newbuf);
            if extended {
                FreeSpaceMapVacuumRange(idxrel, newblk, newblk + 1);
            }
        }

        return true;
    } else if newbuf == InvalidBuffer {
        /*
         * Not enough space, but caller said that there was. Tell them to
         * start over.
         */
        LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);
        return false;
    } else {
        /*
         * Not enough free space on the oldpage. Put the new tuple on the new
         * page, and update the revmap.
         */
        let newpage: Page = BufferGetPage(newbuf);
        let revmapbuf: Buffer;
        let mut newtid: ItemPointerData = std::mem::zeroed();
        let newoff: OffsetNumber;
        let mut freespace: Size = 0;

        revmapbuf = brinLockRevmapPageForUpdate(revmap, heapBlk);

        START_CRIT_SECTION!();

        /*
         * We need to initialize the page if it's newly obtained.  Note we
         * will WAL-log the initialization as part of the update, so we don't
         * need to do that here.
         */
        if extended {
            brin_page_init(newpage, BRIN_PAGETYPE_REGULAR);
        }

        PageIndexTupleDeleteNoCompact(oldpage, oldoff);
        newoff = PageAddItem(
            newpage,
            newtup as *mut BrinTuple as Item,
            newsz,
            InvalidOffsetNumber,
            false,
            false,
        );
        if newoff == InvalidOffsetNumber {
            elog!(ERROR, "failed to add BRIN tuple to new page");
        }
        MarkBufferDirty(oldbuf);
        MarkBufferDirty(newbuf);

        /* needed to update FSM below */
        if extended {
            freespace = br_page_get_freespace(newpage);
        }

        ItemPointerSet(&mut newtid, newblk, newoff);
        brinSetHeapBlockItemptr(revmapbuf, pagesPerRange, heapBlk, newtid);
        MarkBufferDirty(revmapbuf);

        /* XLOG stuff */
        if RelationNeedsWAL(idxrel) {
            let mut xlrec: xl_brin_update = std::mem::zeroed();
            let recptr: XLogRecPtr;
            let info: uint8;

            info = XLOG_BRIN_UPDATE | (if extended { XLOG_BRIN_INIT_PAGE } else { 0 });

            xlrec.insert.offnum = newoff;
            xlrec.insert.heapBlk = heapBlk;
            xlrec.insert.pagesPerRange = pagesPerRange;
            xlrec.oldOffnum = oldoff;

            XLogBeginInsert();

            /* new page */
            XLogRegisterData(&mut xlrec as *mut _ as *mut c_void, SizeOfBrinUpdate);

            XLogRegisterBuffer(
                0,
                newbuf,
                REGBUF_STANDARD | (if extended { REGBUF_WILL_INIT } else { 0 }),
            );
            XLogRegisterBufData(0, newtup as *const c_void, newsz);

            /* revmap page */
            XLogRegisterBuffer(1, revmapbuf, 0);

            /* old page */
            XLogRegisterBuffer(2, oldbuf, REGBUF_STANDARD);

            recptr = XLogInsert(RM_BRIN_ID, info);

            PageSetLSN(oldpage, recptr);
            PageSetLSN(newpage, recptr);
            PageSetLSN(BufferGetPage(revmapbuf), recptr);
        }

        END_CRIT_SECTION!();

        LockBuffer(revmapbuf, BUFFER_LOCK_UNLOCK);
        LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);
        UnlockReleaseBuffer(newbuf);

        if extended {
            RecordPageWithFreeSpace(idxrel, newblk, freespace);
            FreeSpaceMapVacuumRange(idxrel, newblk, newblk + 1);
        }

        return true;
    }
}

/*
 * Return whether brin_doupdate can do a samepage update.
 */
pub unsafe fn brin_can_do_samepage_update(buffer: Buffer, origsz: Size, newsz: Size) -> bool {
    (newsz <= origsz) || PageGetExactFreeSpace(BufferGetPage(buffer)) >= (newsz - origsz)
}

/*
 * Insert an index tuple into the index relation.  The revmap is updated to
 * mark the range containing the given page as pointing to the inserted entry.
 * A WAL record is written.
 *
 * The buffer, if valid, is first checked for free space to insert the new
 * entry; if there isn't enough, a new buffer is obtained and pinned.  No
 * buffer lock must be held on entry, no buffer lock is held on exit.
 *
 * Return value is the offset number where the tuple was inserted.
 */
pub unsafe fn brin_doinsert(
    idxrel: Relation,
    pagesPerRange: BlockNumber,
    revmap: *mut BrinRevmap,
    buffer: *mut Buffer,
    heapBlk: BlockNumber,
    tup: *mut BrinTuple,
    itemsz: Size,
) -> OffsetNumber {
    let page: Page;
    let blk: BlockNumber;
    let off: OffsetNumber;
    let mut freespace: Size = 0;
    let revmapbuf: Buffer;
    let mut tid: ItemPointerData = std::mem::zeroed();
    let mut extended: bool = false;

    Assert!(itemsz == MAXALIGN(itemsz));

    /* If the item is oversized, don't even bother. */
    if itemsz > BrinMaxItemSize {
        elog!(
            ERROR,
            "index row size {} exceeds maximum {} for index \"{}\"",
            itemsz,
            BrinMaxItemSize,
            CStr_to_str(RelationGetRelationName(idxrel))
        );
        return InvalidOffsetNumber; /* keep compiler quiet */
    }

    /* Make sure the revmap is long enough to contain the entry we need */
    brinRevmapExtend(revmap, heapBlk);

    /*
     * Acquire lock on buffer supplied by caller, if any.  If it doesn't have
     * enough space, unpin it to obtain a new one below.
     */
    if BufferIsValid(*buffer) {
        /*
         * It's possible that another backend (or ourselves!) extended the
         * revmap over the page we held a pin on, so we cannot assume that
         * it's still a regular page.
         */
        LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
        if br_page_get_freespace(BufferGetPage(*buffer)) < itemsz {
            UnlockReleaseBuffer(*buffer);
            *buffer = InvalidBuffer;
        }
    }

    /*
     * If we still don't have a usable buffer, have brin_getinsertbuffer
     * obtain one for us.
     */
    if !BufferIsValid(*buffer) {
        loop {
            *buffer = brin_getinsertbuffer(idxrel, InvalidBuffer, itemsz, &mut extended);
            if BufferIsValid(*buffer) {
                break;
            }
        }
    } else {
        extended = false;
    }

    /* Now obtain lock on revmap buffer */
    revmapbuf = brinLockRevmapPageForUpdate(revmap, heapBlk);

    page = BufferGetPage(*buffer);
    blk = BufferGetBlockNumber(*buffer);

    /* Execute the actual insertion */
    START_CRIT_SECTION!();
    if extended {
        brin_page_init(page, BRIN_PAGETYPE_REGULAR);
    }
    off = PageAddItem(page, tup as Item, itemsz, InvalidOffsetNumber, false, false);
    if off == InvalidOffsetNumber {
        elog!(ERROR, "failed to add BRIN tuple to new page");
    }
    MarkBufferDirty(*buffer);

    /* needed to update FSM below */
    if extended {
        freespace = br_page_get_freespace(page);
    }

    ItemPointerSet(&mut tid, blk, off);
    brinSetHeapBlockItemptr(revmapbuf, pagesPerRange, heapBlk, tid);
    MarkBufferDirty(revmapbuf);

    /* XLOG stuff */
    if RelationNeedsWAL(idxrel) {
        let mut xlrec: xl_brin_insert = std::mem::zeroed();
        let recptr: XLogRecPtr;
        let info: uint8;

        info = XLOG_BRIN_INSERT | (if extended { XLOG_BRIN_INIT_PAGE } else { 0 });
        xlrec.heapBlk = heapBlk;
        xlrec.pagesPerRange = pagesPerRange;
        xlrec.offnum = off;

        XLogBeginInsert();
        XLogRegisterData(&mut xlrec as *mut _ as *mut c_void, SizeOfBrinInsert);

        XLogRegisterBuffer(
            0,
            *buffer,
            REGBUF_STANDARD | (if extended { REGBUF_WILL_INIT } else { 0 }),
        );
        XLogRegisterBufData(0, tup as *const c_void, itemsz);

        XLogRegisterBuffer(1, revmapbuf, 0);

        recptr = XLogInsert(RM_BRIN_ID, info);

        PageSetLSN(page, recptr);
        PageSetLSN(BufferGetPage(revmapbuf), recptr);
    }

    END_CRIT_SECTION!();

    /* Tuple is firmly on buffer; we can release our locks */
    LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
    LockBuffer(revmapbuf, BUFFER_LOCK_UNLOCK);

    BRIN_elog!(
        DEBUG2,
        "inserted tuple ({},{}) for range starting at {}",
        blk,
        off,
        heapBlk
    );

    if extended {
        RecordPageWithFreeSpace(idxrel, blk, freespace);
        FreeSpaceMapVacuumRange(idxrel, blk, blk + 1);
    }

    off
}

/*
 * Initialize a page with the given type.
 *
 * Caller is responsible for marking it dirty, as appropriate.
 */
pub unsafe fn brin_page_init(page: Page, type_: uint16) {
    PageInit(page, BLCKSZ, std::mem::size_of::<BrinSpecialSpace>());

    *BrinPageType(page) = type_;
}

/*
 * Initialize a new BRIN index's metapage.
 */
pub unsafe fn brin_metapage_init(page: Page, pagesPerRange: BlockNumber, version: uint16) {
    let metadata: *mut BrinMetaPageData;

    brin_page_init(page, BRIN_PAGETYPE_META);

    metadata = PageGetContents(page) as *mut BrinMetaPageData;

    (*metadata).brinMagic = BRIN_META_MAGIC;
    (*metadata).brinVersion = version as u32;
    (*metadata).pagesPerRange = pagesPerRange;

    /*
     * Note we cheat here a little.  0 is not a valid revmap block number
     * (because it's the metapage buffer), but doing this enables the first
     * revmap page to be created when the index is.
     */
    (*metadata).lastRevmapPage = 0;

    /*
     * Set pd_lower just past the end of the metadata.  This is essential,
     * because without doing so, metadata will be lost if xlog.c compresses
     * the page.
     */
    (*(page as PageHeader as *mut PageHeaderData)).pd_lower =
        (((metadata as *mut c_char).add(std::mem::size_of::<BrinMetaPageData>())) as isize
            - (page as *mut c_char) as isize) as u16;
}

/*
 * Initiate page evacuation protocol.
 *
 * The page must be locked in exclusive mode by the caller.
 *
 * If the page is not yet initialized or empty, return false without doing
 * anything; it can be used for revmap without any further changes.  If it
 * contains tuples, mark it for evacuation and return true.
 */
pub unsafe fn brin_start_evacuating_page(_idxRel: Relation, buf: Buffer) -> bool {
    let mut off: OffsetNumber;
    let maxoff: OffsetNumber;
    let page: Page;

    page = BufferGetPage(buf);

    if PageIsNew(page) {
        return false;
    }

    maxoff = PageGetMaxOffsetNumber(page);
    off = FirstOffsetNumber;
    while off <= maxoff {
        let lp: ItemId;

        lp = PageGetItemId(page, off);
        if ItemIdIsUsed(lp) {
            /*
             * Prevent other backends from adding more stuff to this page:
             * BRIN_EVACUATE_PAGE informs br_page_get_freespace that this page
             * can no longer be used to add new tuples.  Note that this flag
             * is not WAL-logged, except accidentally.
             */
            *BrinPageFlags(page) |= BRIN_EVACUATE_PAGE;
            MarkBufferDirtyHint(buf, true);

            return true;
        }
        off += 1;
    }
    false
}

/*
 * Move all tuples out of a page.
 *
 * The caller must hold lock on the page. The lock and pin are released.
 */
pub unsafe fn brin_evacuate_page(
    idxRel: Relation,
    pagesPerRange: BlockNumber,
    revmap: *mut BrinRevmap,
    buf: Buffer,
) {
    let mut off: OffsetNumber;
    let maxoff: OffsetNumber;
    let page: Page;
    let btup: *mut BrinTuple = std::ptr::null_mut();
    let mut btupsz: Size = 0;

    page = BufferGetPage(buf);

    Assert!(*BrinPageFlags(page) & BRIN_EVACUATE_PAGE != 0);

    maxoff = PageGetMaxOffsetNumber(page);
    off = FirstOffsetNumber;
    while off <= maxoff {
        let mut tup: *mut BrinTuple;
        let sz: Size;
        let lp: ItemId;

        CHECK_FOR_INTERRUPTS!();

        lp = PageGetItemId(page, off);
        if ItemIdIsUsed(lp) {
            sz = ItemIdGetLength(lp);
            tup = PageGetItem(page, lp) as *mut BrinTuple;
            tup = brin_copy_tuple(tup, sz, btup, &mut btupsz);

            LockBuffer(buf, BUFFER_LOCK_UNLOCK);

            if !brin_doupdate(
                idxRel,
                pagesPerRange,
                revmap,
                brin_tuple_blkno(tup),
                buf,
                off,
                tup,
                sz,
                tup,
                sz,
                false,
            ) {
                off -= 1; /* retry */
            }

            LockBuffer(buf, BUFFER_LOCK_SHARE);

            /* It's possible that someone extended the revmap over this page */
            if !BRIN_IS_REGULAR_PAGE(page) {
                break;
            }
        }
        off += 1;
    }

    let _ = btup;

    UnlockReleaseBuffer(buf);
}

/*
 * Given a BRIN index page, initialize it if necessary, and record its
 * current free space in the FSM.
 *
 * The main use for this is when, during vacuuming, an uninitialized page is
 * found, which could be the result of relation extension followed by a crash
 * before the page can be used.
 *
 * Here, we don't bother to update upper FSM pages, instead expecting that our
 * caller (brin_vacuum_scan) will fix them at the end of the scan.  Elsewhere
 * in this file, it's generally a good idea to propagate additions of free
 * space into the upper FSM pages immediately.
 */
pub unsafe fn brin_page_cleanup(idxrel: Relation, buf: Buffer) {
    let page: Page = BufferGetPage(buf);

    /*
     * If a page was left uninitialized, initialize it now; also record it in
     * FSM.
     *
     * Somebody else might be extending the relation concurrently.  To avoid
     * re-initializing the page before they can grab the buffer lock, we
     * acquire the extension lock momentarily.  Since they hold the extension
     * lock from before getting the page and after its been initialized, we're
     * sure to see their initialization.
     */
    if PageIsNew(page) {
        LockRelationForExtension(idxrel, ShareLock);
        UnlockRelationForExtension(idxrel, ShareLock);

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        if PageIsNew(page) {
            brin_initialize_empty_new_buffer(idxrel, buf);
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            return;
        }
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    }

    /* Nothing to be done for non-regular index pages */
    if BRIN_IS_META_PAGE(BufferGetPage(buf)) || BRIN_IS_REVMAP_PAGE(BufferGetPage(buf)) {
        return;
    }

    /* Measure free space and record it */
    RecordPageWithFreeSpace(idxrel, BufferGetBlockNumber(buf), br_page_get_freespace(page));
}

/*
 * Return a pinned and exclusively locked buffer which can be used to insert an
 * index item of size itemsz (caller must ensure not to request sizes
 * impossible to fulfill).  If oldbuf is a valid buffer, it is also locked (in
 * an order determined to avoid deadlocks).
 *
 * If we find that the old page is no longer a regular index page (because
 * of a revmap extension), the old buffer is unlocked and we return
 * InvalidBuffer.
 *
 * If there's no existing page with enough free space to accommodate the new
 * item, the relation is extended.  If this happens, *extended is set to true,
 * and it is the caller's responsibility to initialize the page (and WAL-log
 * that fact) prior to use.  The caller should also update the FSM with the
 * page's remaining free space after the insertion.
 *
 * Note that the caller is not expected to update FSM unless *extended is set
 * true.  This policy means that we'll update FSM when a page is created, and
 * when it's found to have too little space for a desired tuple insertion,
 * but not every single time we add a tuple to the page.
 *
 * Note that in some corner cases it is possible for this routine to extend
 * the relation and then not return the new page.  It is this routine's
 * responsibility to WAL-log the page initialization and to record the page in
 * FSM if that happens, since the caller certainly can't do it.
 */
unsafe fn brin_getinsertbuffer(
    irel: Relation,
    oldbuf: Buffer,
    itemsz: Size,
    extended: *mut bool,
) -> Buffer {
    let oldblk: BlockNumber;
    let mut newblk: BlockNumber;
    let mut page: Page;
    let mut freespace: Size;

    /* callers must have checked */
    Assert!(itemsz <= BrinMaxItemSize);

    if BufferIsValid(oldbuf) {
        oldblk = BufferGetBlockNumber(oldbuf);
    } else {
        oldblk = InvalidBlockNumber;
    }

    /* Choose initial target page, re-using existing target if known */
    newblk = RelationGetTargetBlock(irel);
    if newblk == InvalidBlockNumber {
        newblk = GetPageWithFreeSpace(irel, itemsz);
    }

    /*
     * Loop until we find a page with sufficient free space.  By the time we
     * return to caller out of this loop, both buffers are valid and locked;
     * if we have to restart here, neither page is locked and newblk isn't
     * pinned (if it's even valid).
     */
    loop {
        let buf: Buffer;
        let mut extensionLockHeld: bool = false;

        CHECK_FOR_INTERRUPTS!();

        *extended = false;

        if newblk == InvalidBlockNumber {
            /*
             * There's not enough free space in any existing index page,
             * according to the FSM: extend the relation to obtain a shiny new
             * page.
             *
             * XXX: It's likely possible to use RBM_ZERO_AND_LOCK here,
             * which'd avoid the need to hold the extension lock during buffer
             * reclaim.
             */
            if !RELATION_IS_LOCAL(irel) {
                LockRelationForExtension(irel, ExclusiveLock);
                extensionLockHeld = true;
            }
            buf = ReadBuffer(irel, P_NEW);
            newblk = BufferGetBlockNumber(buf);
            *extended = true;

            BRIN_elog!(
                DEBUG2,
                "brin_getinsertbuffer: extending to page {}",
                BufferGetBlockNumber(buf)
            );
        } else if newblk == oldblk {
            /*
             * There's an odd corner-case here where the FSM is out-of-date,
             * and gave us the old page.
             */
            buf = oldbuf;
        } else {
            buf = ReadBuffer(irel, newblk);
        }

        /*
         * We lock the old buffer first, if it's earlier than the new one; but
         * then we need to check that it hasn't been turned into a revmap page
         * concurrently.  If we detect that that happened, give up and tell
         * caller to start over.
         */
        if BufferIsValid(oldbuf) && oldblk < newblk {
            LockBuffer(oldbuf, BUFFER_LOCK_EXCLUSIVE);
            if !BRIN_IS_REGULAR_PAGE(BufferGetPage(oldbuf)) {
                LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);

                /*
                 * It is possible that the new page was obtained from
                 * extending the relation.  In that case, we must be sure to
                 * record it in the FSM before leaving, because otherwise the
                 * space would be lost forever.  However, we cannot let an
                 * uninitialized page get in the FSM, so we need to initialize
                 * it first.
                 */
                if *extended {
                    brin_initialize_empty_new_buffer(irel, buf);
                }

                if extensionLockHeld {
                    UnlockRelationForExtension(irel, ExclusiveLock);
                }

                ReleaseBuffer(buf);

                if *extended {
                    FreeSpaceMapVacuumRange(irel, newblk, newblk + 1);
                    /* shouldn't matter, but don't confuse caller */
                    *extended = false;
                }

                return InvalidBuffer;
            }
        }

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        if extensionLockHeld {
            UnlockRelationForExtension(irel, ExclusiveLock);
        }

        page = BufferGetPage(buf);

        /*
         * We have a new buffer to insert into.  Check that the new page has
         * enough free space, and return it if it does; otherwise start over.
         * (br_page_get_freespace also checks that the FSM didn't hand us a
         * page that has since been repurposed for the revmap.)
         */
        freespace = if *extended {
            BrinMaxItemSize
        } else {
            br_page_get_freespace(page)
        };
        if freespace >= itemsz {
            RelationSetTargetBlock(irel, newblk);

            /*
             * Lock the old buffer if not locked already.  Note that in this
             * case we know for sure it's a regular page: it's later than the
             * new page we just got, which is not a revmap page, and revmap
             * pages are always consecutive.
             */
            if BufferIsValid(oldbuf) && oldblk > newblk {
                LockBuffer(oldbuf, BUFFER_LOCK_EXCLUSIVE);
                Assert!(BRIN_IS_REGULAR_PAGE(BufferGetPage(oldbuf)));
            }

            return buf;
        }

        /* This page is no good. */

        /*
         * If an entirely new page does not contain enough free space for the
         * new item, then surely that item is oversized.  Complain loudly; but
         * first make sure we initialize the page and record it as free, for
         * next time.
         */
        if *extended {
            brin_initialize_empty_new_buffer(irel, buf);
            /* since this should not happen, skip FreeSpaceMapVacuum */

            elog!(
                ERROR,
                "index row size {} exceeds maximum {} for index \"{}\"",
                itemsz,
                freespace,
                CStr_to_str(RelationGetRelationName(irel))
            );
            return InvalidBuffer; /* keep compiler quiet */
        }

        if newblk != oldblk {
            UnlockReleaseBuffer(buf);
        }
        if BufferIsValid(oldbuf) && oldblk <= newblk {
            LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);
        }

        /*
         * Update the FSM with the new, presumably smaller, freespace value
         * for this page, then search for a new target page.
         */
        newblk = RecordAndGetPageWithFreeSpace(irel, newblk, freespace, itemsz);
    }
}

/*
 * Initialize a page as an empty regular BRIN page, WAL-log this, and record
 * the page in FSM.
 *
 * There are several corner situations in which we extend the relation to
 * obtain a new page and later find that we cannot use it immediately.  When
 * that happens, we don't want to leave the page go unrecorded in FSM, because
 * there is no mechanism to get the space back and the index would bloat.
 * Also, because we would not WAL-log the action that would initialize the
 * page, the page would go uninitialized in a standby (or after recovery).
 *
 * While we record the page in FSM here, caller is responsible for doing FSM
 * upper-page update if that seems appropriate.
 */
unsafe fn brin_initialize_empty_new_buffer(idxrel: Relation, buffer: Buffer) {
    let page: Page;

    BRIN_elog!(
        DEBUG2,
        "brin_initialize_empty_new_buffer: initializing blank page {}",
        BufferGetBlockNumber(buffer)
    );

    START_CRIT_SECTION!();
    page = BufferGetPage(buffer);
    brin_page_init(page, BRIN_PAGETYPE_REGULAR);
    MarkBufferDirty(buffer);

    /* XLOG stuff */
    if RelationNeedsWAL(idxrel) {
        log_newpage_buffer(buffer, true);
    }

    END_CRIT_SECTION!();

    /*
     * We update the FSM for this page, but this is not WAL-logged.  This is
     * acceptable because VACUUM will scan the index and update the FSM with
     * pages whose FSM records were forgotten in a crash.
     */
    RecordPageWithFreeSpace(idxrel, BufferGetBlockNumber(buffer), br_page_get_freespace(page));
}

/*
 * Return the amount of free space on a regular BRIN index page.
 *
 * If the page is not a regular page, or has been marked with the
 * BRIN_EVACUATE_PAGE flag, returns 0.
 */
unsafe fn br_page_get_freespace(page: Page) -> Size {
    if !BRIN_IS_REGULAR_PAGE(page) || (*BrinPageFlags(page) & BRIN_EVACUATE_PAGE) != 0 {
        0
    } else {
        PageGetFreeSpace(page)
    }
}

// ---------------------------------------------------------------------------
// Local helper macros/stubs for items used above.
// ---------------------------------------------------------------------------

// PageHeaderData layout fragment used by brin_metapage_init for pd_lower.
#[repr(C)]
struct PageHeaderData {
    _lsn: [u8; 8],
    _checksum: u16,
    _flags: u16,
    pd_lower: u16,
    // ... remaining fields omitted; faithful structure handled elsewhere
}

// BRIN_elog((level, fmt, ...)) maps to elog! gated on debug build; here we
// translate it to elog! directly (the BRIN_DEBUG compile-time gate defaults
// off in C, but we preserve the call faithfully as elog!).
#[allow(unused_macros)]
macro_rules! BRIN_elog {
    ($level:expr, $($arg:tt)*) => {{
        // BRIN_elog is compiled out unless BRIN_DEBUG is defined.
        let _ = &$level;
        let _ = format_args!($($arg)*);
    }};
}
use BRIN_elog;

// Convert a NUL-terminated C string pointer to a &str for elog! formatting.
unsafe fn CStr_to_str(s: *const c_char) -> &'static str {
    if s.is_null() {
        return "";
    }
    std::ffi::CStr::from_ptr(s).to_str().unwrap_or("")
}

// CHECK_FOR_INTERRUPTS is provided by crate::miscadmin; re-exported locally
// for use within macro expansions in this file.
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{ /* TODO: miscadmin.h CHECK_FOR_INTERRUPTS() */ }};
}
use CHECK_FOR_INTERRUPTS;

// Assert is provided by crate::prelude (crate-root #[macro_export] macro).
