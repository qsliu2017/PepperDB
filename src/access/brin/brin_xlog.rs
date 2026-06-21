//! brin_xlog.c - XLog replay routines for BRIN indexes

use crate::prelude::*;

use crate::elog;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{
    Page, PageAddItem, PageGetContents, PageGetMaxOffsetNumber, PageHeader,
    PageIndexTupleDeleteNoCompact, PageIndexTupleOverwrite, PageSetLSN, SizeOfPageHeaderData,
    Item, LocationIndex, PageGetSpecialPointer,
};
use crate::storage::itemptr::{ItemPointerData, ItemPointerSet, ItemPointerSetInvalid};
use crate::storage::off::{InvalidOffsetNumber, OffsetNumber};

use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetBlockData, XLogRecGetBlockTag, XLogRecGetData, XLogRecGetInfo,
    XLR_INFO_MASK,
};

use crate::access::brin::brin_page::{
    BrinMetaPageData, BrinSpecialSpace, BRIN_EVACUATE_PAGE, BRIN_IS_META_PAGE, BRIN_IS_REGULAR_PAGE,
    BRIN_PAGETYPE_REGULAR, BRIN_PAGETYPE_REVMAP,
};

use crate::access::common::bufmask::{mask_page_hint_bits, mask_page_lsn_and_checksum, mask_unused_space};

// Re-use the WAL record structs already defined for the rmgr descriptor.
use crate::access::rmgrdesc::brindesc::{
    xl_brin_createidx, xl_brin_desummarize, xl_brin_insert, xl_brin_revmap_extend,
    xl_brin_samepage_update, xl_brin_update,
};

// ---------------------------------------------------------------------------
// WAL record opcode definitions (from access/brin_xlog.h).
// ---------------------------------------------------------------------------
const XLOG_BRIN_CREATE_INDEX: uint8 = 0x00;
const XLOG_BRIN_INSERT: uint8 = 0x10;
const XLOG_BRIN_UPDATE: uint8 = 0x20;
const XLOG_BRIN_SAMEPAGE_UPDATE: uint8 = 0x30;
const XLOG_BRIN_REVMAP_EXTEND: uint8 = 0x40;
const XLOG_BRIN_DESUMMARIZE: uint8 = 0x50;

const XLOG_BRIN_OPMASK: uint8 = 0x70;

// When we insert the first item on a new page, we restore the entire page in
// redo.
const XLOG_BRIN_INIT_PAGE: uint8 = 0x80;

// ---------------------------------------------------------------------------
// xlogutils.h: result of XLogRead/InitBufferForRedo helpers (not yet ported).
// ---------------------------------------------------------------------------
#[allow(non_camel_case_types)]
type XLogRedoAction = c_int;
const BLK_NEEDS_REDO: XLogRedoAction = 0;

unsafe fn XLogInitBufferForRedo(_record: *mut XLogReaderState, _block_id: uint8) -> Buffer { unimplemented!() }

unsafe fn XLogReadBufferForRedo(
    _record: *mut XLogReaderState,
    _block_id: uint8,
    _buf: *mut Buffer,
) -> XLogRedoAction { unimplemented!() }

// ---------------------------------------------------------------------------
// bufmgr.h: buffer-manager helpers (not yet ported).
// ---------------------------------------------------------------------------
unsafe fn BufferIsValid(_bufnum: Buffer) -> bool { crate::access::nbtree::nbtpage::BufferIsValid(_bufnum) }

unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    // TODO: bufmgr.c not ported
    unimplemented!("BufferGetPage: bufmgr.c (deferred)")
}

unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    // TODO: bufmgr.c not ported
    unimplemented!("BufferGetBlockNumber: bufmgr.c (deferred)")
}

unsafe fn MarkBufferDirty(_buffer: Buffer) {
    // TODO: bufmgr.c not ported
    unimplemented!("MarkBufferDirty: bufmgr.c (deferred)")
}

unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    // TODO: bufmgr.c not ported
    unimplemented!("UnlockReleaseBuffer: bufmgr.c (deferred)")
}

// ---------------------------------------------------------------------------
// brin_page.c / brin_pageops.c / brin_revmap.c helpers (not yet ported).
// ---------------------------------------------------------------------------
unsafe fn brin_metapage_init(_page: Page, _pagesPerRange: BlockNumber, _version: uint16) { crate::access::brin::brin_pageops::brin_metapage_init(_page, _pagesPerRange, _version) }

unsafe fn brin_page_init(_page: Page, _type_: uint16) { crate::access::brin::brin_pageops::brin_page_init(_page, _type_) }

unsafe fn brinSetHeapBlockItemptr(
    _buf: Buffer,
    _pagesPerRange: BlockNumber,
    _heapBlk: BlockNumber,
    _tid: ItemPointerData,
) { crate::access::brin::brin_revmap::brinSetHeapBlockItemptr(_buf, _pagesPerRange, _heapBlk, _tid) }

// ---------------------------------------------------------------------------
// BrinTuple (access/brin_tuple.h) - not yet ported.
// ---------------------------------------------------------------------------
#[repr(C)]
struct BrinTuple {
    bt_blkno: BlockNumber,
    // ... remaining fields elided; only bt_blkno is referenced here.
}

// ---------------------------------------------------------------------------
// xlog replay routines
// ---------------------------------------------------------------------------
unsafe fn brin_xlog_createidx(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec = XLogRecGetData(record) as *mut xl_brin_createidx;
    let buf: Buffer;
    let page: Page;

    /* create the index' metapage */
    buf = XLogInitBufferForRedo(record, 0);
    Assert!(BufferIsValid(buf));
    page = BufferGetPage(buf) as Page;
    brin_metapage_init(page, (*xlrec).pagesPerRange, (*xlrec).version);
    PageSetLSN(page, lsn);
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

/*
 * Common part of an insert or update. Inserts the new tuple and updates the
 * revmap.
 */
unsafe fn brin_xlog_insert_update(record: *mut XLogReaderState, xlrec: *mut xl_brin_insert) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut buffer: Buffer = 0;
    let regpgno: BlockNumber;
    let mut page: Page;
    let mut action: XLogRedoAction;

    /*
     * If we inserted the first and only tuple on the page, re-initialize the
     * page from scratch.
     */
    if (XLogRecGetInfo(record) & XLOG_BRIN_INIT_PAGE) != 0 {
        buffer = XLogInitBufferForRedo(record, 0);
        page = BufferGetPage(buffer);
        brin_page_init(page, BRIN_PAGETYPE_REGULAR);
        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, 0, &raw mut buffer);
    }

    /* need this page's blkno to store in revmap */
    regpgno = BufferGetBlockNumber(buffer);

    /* insert the index item into the page */
    if action == BLK_NEEDS_REDO {
        let mut offnum: OffsetNumber;
        let tuple: *mut BrinTuple;
        let mut tuplen: Size = 0;

        tuple = XLogRecGetBlockData(record, 0, &raw mut tuplen) as *mut BrinTuple;

        Assert!((*tuple).bt_blkno == (*xlrec).heapBlk);

        page = BufferGetPage(buffer) as Page;
        offnum = (*xlrec).offnum;
        if (PageGetMaxOffsetNumber(page) as c_int) + 1 < offnum as c_int {
            elog!(PANIC, "brin_xlog_insert_update: invalid max offset number");
        }

        offnum = PageAddItem(page, tuple as Item, tuplen, offnum, true, false);
        if offnum == InvalidOffsetNumber {
            elog!(PANIC, "brin_xlog_insert_update: failed to add tuple");
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    /* update the revmap */
    action = XLogReadBufferForRedo(record, 1, &raw mut buffer);
    if action == BLK_NEEDS_REDO {
        let mut tid: ItemPointerData = core::mem::zeroed();

        ItemPointerSet(&raw mut tid, regpgno, (*xlrec).offnum);
        page = BufferGetPage(buffer) as Page;

        brinSetHeapBlockItemptr(buffer, (*xlrec).pagesPerRange, (*xlrec).heapBlk, tid);
        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    /* XXX no FSM updates here ... */
}

/*
 * replay a BRIN index insertion
 */
unsafe fn brin_xlog_insert(record: *mut XLogReaderState) {
    let xlrec = XLogRecGetData(record) as *mut xl_brin_insert;

    brin_xlog_insert_update(record, xlrec);
}

/*
 * replay a BRIN index update
 */
unsafe fn brin_xlog_update(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec = XLogRecGetData(record) as *mut xl_brin_update;
    let mut buffer: Buffer = 0;
    let action: XLogRedoAction;

    /* First remove the old tuple */
    action = XLogReadBufferForRedo(record, 2, &raw mut buffer);
    if action == BLK_NEEDS_REDO {
        let page: Page;
        let offnum: OffsetNumber;

        page = BufferGetPage(buffer) as Page;

        offnum = (*xlrec).oldOffnum;

        PageIndexTupleDeleteNoCompact(page, offnum);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    /* Then insert the new tuple and update revmap, like in an insertion. */
    brin_xlog_insert_update(record, &raw mut (*xlrec).insert);

    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

/*
 * Update a tuple on a single page.
 */
unsafe fn brin_xlog_samepage_update(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_brin_samepage_update;
    let mut buffer: Buffer = 0;
    let action: XLogRedoAction;

    xlrec = XLogRecGetData(record) as *mut xl_brin_samepage_update;
    action = XLogReadBufferForRedo(record, 0, &raw mut buffer);
    if action == BLK_NEEDS_REDO {
        let mut tuplen: Size = 0;
        let brintuple: *mut BrinTuple;
        let page: Page;
        let offnum: OffsetNumber;

        brintuple = XLogRecGetBlockData(record, 0, &raw mut tuplen) as *mut BrinTuple;

        page = BufferGetPage(buffer) as Page;

        offnum = (*xlrec).offnum;

        if !PageIndexTupleOverwrite(page, offnum, brintuple as Item, tuplen) {
            elog!(PANIC, "brin_xlog_samepage_update: failed to replace tuple");
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    /* XXX no FSM updates here ... */
}

/*
 * Replay a revmap page extension
 */
unsafe fn brin_xlog_revmap_extend(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_brin_revmap_extend;
    let mut metabuf: Buffer = 0;
    let buf: Buffer;
    let page: Page;
    let mut targetBlk: BlockNumber = 0;
    let action: XLogRedoAction;

    xlrec = XLogRecGetData(record) as *mut xl_brin_revmap_extend;

    XLogRecGetBlockTag(record, 1, null_mut(), null_mut(), &raw mut targetBlk);
    Assert!((*xlrec).targetBlk == targetBlk);

    /* Update the metapage */
    action = XLogReadBufferForRedo(record, 0, &raw mut metabuf);
    if action == BLK_NEEDS_REDO {
        let metapg: Page;
        let metadata: *mut BrinMetaPageData;

        metapg = BufferGetPage(metabuf);
        metadata = PageGetContents(metapg) as *mut BrinMetaPageData;

        Assert!((*metadata).lastRevmapPage == (*xlrec).targetBlk - 1);
        (*metadata).lastRevmapPage = (*xlrec).targetBlk;

        PageSetLSN(metapg, lsn);

        /*
         * Set pd_lower just past the end of the metadata.  This is essential,
         * because without doing so, metadata will be lost if xlog.c
         * compresses the page.  (We must do this here because pre-v11
         * versions of PG did not set the metapage's pd_lower correctly, so a
         * pg_upgraded index might contain the wrong value.)
         */
        (*(metapg as PageHeader)).pd_lower = ((metadata as *mut c_char)
            .add(size_of::<BrinMetaPageData>()))
        .offset_from(metapg) as LocationIndex;

        MarkBufferDirty(metabuf);
    }

    /*
     * Re-init the target block as a revmap page.  There's never a full- page
     * image here.
     */

    buf = XLogInitBufferForRedo(record, 1);
    page = BufferGetPage(buf) as Page;
    brin_page_init(page, BRIN_PAGETYPE_REVMAP);

    PageSetLSN(page, lsn);
    MarkBufferDirty(buf);

    UnlockReleaseBuffer(buf);
    if BufferIsValid(metabuf) {
        UnlockReleaseBuffer(metabuf);
    }
}

unsafe fn brin_xlog_desummarize_page(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_brin_desummarize;
    let mut buffer: Buffer = 0;
    let mut action: XLogRedoAction;

    xlrec = XLogRecGetData(record) as *mut xl_brin_desummarize;

    /* Update the revmap */
    action = XLogReadBufferForRedo(record, 0, &raw mut buffer);
    if action == BLK_NEEDS_REDO {
        let mut iptr: ItemPointerData = core::mem::zeroed();

        ItemPointerSetInvalid(&raw mut iptr);
        brinSetHeapBlockItemptr(buffer, (*xlrec).pagesPerRange, (*xlrec).heapBlk, iptr);

        PageSetLSN(BufferGetPage(buffer), lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    /* remove the leftover entry from the regular page */
    action = XLogReadBufferForRedo(record, 1, &raw mut buffer);
    if action == BLK_NEEDS_REDO {
        let regPg: Page = BufferGetPage(buffer);

        PageIndexTupleDeleteNoCompact(regPg, (*xlrec).regOffset);

        PageSetLSN(regPg, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

pub unsafe fn brin_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    match info & XLOG_BRIN_OPMASK {
        XLOG_BRIN_CREATE_INDEX => brin_xlog_createidx(record),
        XLOG_BRIN_INSERT => brin_xlog_insert(record),
        XLOG_BRIN_UPDATE => brin_xlog_update(record),
        XLOG_BRIN_SAMEPAGE_UPDATE => brin_xlog_samepage_update(record),
        XLOG_BRIN_REVMAP_EXTEND => brin_xlog_revmap_extend(record),
        XLOG_BRIN_DESUMMARIZE => brin_xlog_desummarize_page(record),
        _ => {
            elog!(PANIC, "brin_redo: unknown op code {}", info as c_uint);
        }
    }
}

/*
 * Mask a BRIN page before doing consistency checks.
 */
pub unsafe fn brin_mask(pagedata: *mut c_char, _blkno: BlockNumber) {
    let page: Page = pagedata as Page;
    let pagehdr: PageHeader = page as PageHeader;

    mask_page_lsn_and_checksum(page);

    mask_page_hint_bits(page);

    /*
     * Regular brin pages contain unused space which needs to be masked.
     * Similarly for meta pages, but mask it only if pd_lower appears to have
     * been set correctly.
     */
    if BRIN_IS_REGULAR_PAGE(page)
        || (BRIN_IS_META_PAGE(page) && (*pagehdr).pd_lower as usize > SizeOfPageHeaderData)
    {
        mask_unused_space(page);
    }

    /*
     * BRIN_EVACUATE_PAGE is not WAL-logged, since it's of no use in recovery.
     * Mask it.  See brin_start_evacuating_page() for details.
     */
    // BrinPageFlags(page) &= ~BRIN_EVACUATE_PAGE;  (lvalue inlined below)
    let flags_ptr = brin_page_flags_ptr(page);
    *flags_ptr &= !BRIN_EVACUATE_PAGE;
}

/*
 * Helper to obtain a mutable pointer to the BRIN special-space flags word, so
 * we can express the C macro `BrinPageFlags(page) &= ...` lvalue assignment.
 * Mirrors the layout used by BrinPageFlags() in brin_page.rs.
 */
#[inline]
unsafe fn brin_page_flags_ptr(page: Page) -> *mut uint16 {
    let sp = PageGetSpecialPointer(page) as *mut BrinSpecialSpace;
    (&raw mut (*sp).vector[MAXALIGN(1) / size_of::<uint16>() - 2]) as *mut uint16
}
