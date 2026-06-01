//! spgxlog.c
//!   WAL replay logic for SP-GiST
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! src/backend/access/spgist/spgxlog.c

use crate::prelude::*;
use crate::AllocSetContextCreate;

use std::ffi::{c_char, c_int, c_uint, c_void};

use crate::c::{int8, uint8, uint16, Size, TransactionId};

use crate::access::common::bufmask::{
    mask_page_hint_bits, mask_page_lsn_and_checksum, mask_unused_space,
};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::access::transam::xlogreader::{
    XLogReaderState, XLogRecGetBlockTag, XLogRecGetData, XLogRecGetInfo, XLogRecHasBlockRef,
    XLR_INFO_MASK,
};
use crate::access::transam::xlogutils::{
    XLogInitBufferForRedo, XLogReadBufferForRedo, XLogRedoAction, BLK_NEEDS_REDO,
};
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::storage::buf::{Buffer, InvalidBuffer};
use crate::storage::bufpage::{
    Page, PageAddItem, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageGetSpecialPointer,
    PageHeader, PageIndexMultiDelete, PageIndexTupleDelete, PageSetLSN, SizeOfPageHeaderData,
};
use crate::storage::item::Item;
use crate::storage::itemid::ItemId;
use crate::storage::itemptr::ItemPointerSetInvalid;
use crate::storage::off::{InvalidOffsetNumber, OffsetNumber};
use crate::utils::palloc::MemoryContext;

use crate::access::spgist::spgist_private::{
    spgFormDeadTuple, spgPageIndexMultiDelete, spgUpdateNodeLink, SpGistDeadTuple,
    SpGistInitBuffer, SpGistInnerTuple, SpGistInnerTupleData, SpGistLeafTuple,
    SpGistLeafTupleData, SpGistPageOpaqueData, SpGistState, SGLT_GET_NEXTOFFSET,
    SGLT_SET_NEXTOFFSET, SPGIST_DEAD, SPGIST_LEAF, SPGIST_LIVE, SPGIST_NULLS,
    SPGIST_PLACEHOLDER, SPGIST_REDIRECT,
};

// spgxlog.h structs and XLOG_SPGIST_* opcodes live in rmgrdesc/spgdesc.rs.
use crate::access::rmgrdesc::spgdesc::{
    spgxlogAddLeaf, spgxlogAddNode, spgxlogMoveLeafs, spgxlogPickSplit, spgxlogSplitTuple,
    spgxlogState, spgxlogVacuumLeaf, spgxlogVacuumRedirect, spgxlogVacuumRoot,
    SizeOfSpgxlogMoveLeafs, SizeOfSpgxlogPickSplit, SizeOfSpgxlogVacuumLeaf,
    XLOG_SPGIST_ADD_LEAF, XLOG_SPGIST_ADD_NODE, XLOG_SPGIST_MOVE_LEAFS, XLOG_SPGIST_PICKSPLIT,
    XLOG_SPGIST_SPLIT_TUPLE, XLOG_SPGIST_VACUUM_LEAF, XLOG_SPGIST_VACUUM_REDIRECT,
    XLOG_SPGIST_VACUUM_ROOT,
};

static mut opCtx: MemoryContext = std::ptr::null_mut(); /* working memory for operations */

// ---------------------------------------------------------------------------
// Local helpers for the packed bit-field accessors (tupstate:2, size:30) of
// SpGistDeadTupleData and the size field of SpGistInner/LeafTupleData.
// ---------------------------------------------------------------------------

#[inline]
unsafe fn SGDT_GET_TUPSTATE(dt: SpGistDeadTuple) -> c_int {
    ((*dt).bits_ & 0x03) as c_int
}

#[inline]
unsafe fn SGDT_SET_TUPSTATE(dt: SpGistDeadTuple, tupstate: c_int) {
    (*dt).bits_ = ((*dt).bits_ & !0x03u32) | ((tupstate as c_uint) & 0x03);
}

#[inline]
unsafe fn SGDT_GET_SIZE(dt: SpGistDeadTuple) -> c_uint {
    (*dt).bits_ >> 2
}

#[inline]
unsafe fn SGLT_GET_TUPSTATE(lt: SpGistLeafTuple) -> c_int {
    ((*lt).bits_ & 0x03) as c_int
}

/*
 * Prepare a dummy SpGistState, with just the minimum info needed for replay.
 *
 * At present, all we need is enough info to support spgFormDeadTuple(),
 * plus the isBuild flag.
 */
unsafe fn fillFakeState(state: *mut SpGistState, stateSrc: spgxlogState) {
    std::ptr::write_bytes(state as *mut u8, 0, std::mem::size_of::<SpGistState>());

    (*state).redirectXid = stateSrc.redirectXid;
    (*state).isBuild = stateSrc.isBuild;
    (*state).deadTupleStorage = palloc0(SGDTSIZE) as *mut c_char;
}

/*
 * Add a leaf tuple, or replace an existing placeholder tuple.  This is used
 * to replay SpGistPageAddNewItem() operations.  If the offset points at an
 * existing tuple, it had better be a placeholder tuple.
 */
unsafe fn addOrReplaceTuple(page: Page, tuple: Item, size: c_int, offset: OffsetNumber) {
    if offset <= PageGetMaxOffsetNumber(page) {
        let dt: SpGistDeadTuple =
            PageGetItem(page, PageGetItemId(page, offset)) as SpGistDeadTuple;

        if SGDT_GET_TUPSTATE(dt) != SPGIST_PLACEHOLDER {
            elog!(ERROR, "SPGiST tuple to be replaced is not a placeholder");
        }

        Assert!((*SpGistPageGetOpaque(page)).nPlaceholder > 0);
        (*SpGistPageGetOpaque(page)).nPlaceholder -= 1;

        PageIndexTupleDelete(page, offset);
    }

    Assert!(offset <= PageGetMaxOffsetNumber(page) + 1);

    if PageAddItem(page, tuple, size as Size, offset, false, false) != offset {
        elog!(
            ERROR,
            "failed to add item of size {} to SPGiST index page",
            size
        );
    }
}

unsafe fn spgRedoAddLeaf(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut ptr: *mut c_char = XLogRecGetData(record);
    let xldata: *mut spgxlogAddLeaf = ptr as *mut spgxlogAddLeaf;
    let leafTuple: *mut c_char;
    let mut leafTupleHdr: SpGistLeafTupleData = std::mem::zeroed();
    let mut buffer: Buffer = 0;
    let page: Page;
    let action: XLogRedoAction;

    ptr = ptr.add(std::mem::size_of::<spgxlogAddLeaf>());
    leafTuple = ptr;
    /* the leaf tuple is unaligned, so make a copy to access its header */
    std::ptr::copy_nonoverlapping(
        leafTuple,
        &mut leafTupleHdr as *mut SpGistLeafTupleData as *mut c_char,
        std::mem::size_of::<SpGistLeafTupleData>(),
    );

    /*
     * In normal operation we would have both current and parent pages locked
     * simultaneously; but in WAL replay it should be safe to update the leaf
     * page before updating the parent.
     */
    if (*xldata).newPage {
        buffer = XLogInitBufferForRedo(record, 0);
        SpGistInitBuffer(
            buffer,
            (SPGIST_LEAF | (if (*xldata).storesNulls { SPGIST_NULLS } else { 0 })) as uint16,
        );
        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, 0, &mut buffer);
    }

    if action == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer);

        /* insert new tuple */
        if (*xldata).offnumLeaf != (*xldata).offnumHeadLeaf {
            /* normal cases, tuple was added by SpGistPageAddNewItem */
            addOrReplaceTuple(
                page,
                leafTuple as Item,
                getLeafSize(&leafTupleHdr),
                (*xldata).offnumLeaf,
            );

            /* update head tuple's chain link if needed */
            if (*xldata).offnumHeadLeaf != InvalidOffsetNumber {
                let head: SpGistLeafTuple = PageGetItem(
                    page,
                    PageGetItemId(page, (*xldata).offnumHeadLeaf),
                ) as SpGistLeafTuple;
                Assert!(
                    SGLT_GET_NEXTOFFSET(head)
                        == SGLT_GET_NEXTOFFSET(&mut leafTupleHdr as SpGistLeafTuple)
                );
                SGLT_SET_NEXTOFFSET(head, (*xldata).offnumLeaf);
            }
        } else {
            /* replacing a DEAD tuple */
            PageIndexTupleDelete(page, (*xldata).offnumLeaf);
            if PageAddItem(
                page,
                leafTuple as Item,
                getLeafSize(&leafTupleHdr) as Size,
                (*xldata).offnumLeaf,
                false,
                false,
            ) != (*xldata).offnumLeaf
            {
                elog!(
                    ERROR,
                    "failed to add item of size {} to SPGiST index page",
                    getLeafSize(&leafTupleHdr)
                );
            }
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    /* update parent downlink if necessary */
    if (*xldata).offnumParent != InvalidOffsetNumber {
        if XLogReadBufferForRedo(record, 1, &mut buffer) == BLK_NEEDS_REDO {
            let tuple: SpGistInnerTuple;
            let mut blknoLeaf: BlockNumber = 0;

            XLogRecGetBlockTag(record, 0, std::ptr::null_mut(), std::ptr::null_mut(), &mut blknoLeaf);

            page = BufferGetPage(buffer);

            tuple = PageGetItem(page, PageGetItemId(page, (*xldata).offnumParent))
                as SpGistInnerTuple;

            spgUpdateNodeLink(
                tuple,
                (*xldata).nodeI as c_int,
                blknoLeaf,
                (*xldata).offnumLeaf,
            );

            PageSetLSN(page, lsn);
            MarkBufferDirty(buffer);
        }
        if BufferIsValid(buffer) {
            UnlockReleaseBuffer(buffer);
        }
    }
}

unsafe fn spgRedoMoveLeafs(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut ptr: *mut c_char = XLogRecGetData(record);
    let xldata: *mut spgxlogMoveLeafs = ptr as *mut spgxlogMoveLeafs;
    let mut state: SpGistState = std::mem::zeroed();
    let toDelete: *mut OffsetNumber;
    let toInsert: *mut OffsetNumber;
    let nInsert: c_int;
    let mut buffer: Buffer = 0;
    let page: Page;
    let action: XLogRedoAction;
    let mut blknoDst: BlockNumber = 0;

    XLogRecGetBlockTag(record, 1, std::ptr::null_mut(), std::ptr::null_mut(), &mut blknoDst);

    fillFakeState(&mut state, (*xldata).stateSrc);

    nInsert = if (*xldata).replaceDead { 1 } else { (*xldata).nMoves as c_int + 1 };

    ptr = ptr.add(SizeOfSpgxlogMoveLeafs);
    toDelete = ptr as *mut OffsetNumber;
    ptr = ptr.add(std::mem::size_of::<OffsetNumber>() * (*xldata).nMoves as usize);
    toInsert = ptr as *mut OffsetNumber;
    ptr = ptr.add(std::mem::size_of::<OffsetNumber>() * nInsert as usize);

    /* now ptr points to the list of leaf tuples */

    /*
     * In normal operation we would have all three pages (source, dest, and
     * parent) locked simultaneously; but in WAL replay it should be safe to
     * update them one at a time, as long as we do it in the right order.
     */

    /* Insert tuples on the dest page (do first, so redirect is valid) */
    if (*xldata).newPage {
        buffer = XLogInitBufferForRedo(record, 1);
        SpGistInitBuffer(
            buffer,
            (SPGIST_LEAF | (if (*xldata).storesNulls { SPGIST_NULLS } else { 0 })) as uint16,
        );
        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, 1, &mut buffer);
    }

    if action == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer);

        for i in 0..nInsert {
            let leafTuple: *mut c_char;
            let mut leafTupleHdr: SpGistLeafTupleData = std::mem::zeroed();

            /*
             * the tuples are not aligned, so must copy to access the size
             * field.
             */
            leafTuple = ptr;
            std::ptr::copy_nonoverlapping(
                leafTuple,
                &mut leafTupleHdr as *mut SpGistLeafTupleData as *mut c_char,
                std::mem::size_of::<SpGistLeafTupleData>(),
            );

            addOrReplaceTuple(
                page,
                leafTuple as Item,
                getLeafSize(&leafTupleHdr),
                *toInsert.add(i as usize),
            );
            ptr = ptr.add(getLeafSize(&leafTupleHdr) as usize);
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    /* Delete tuples from the source page, inserting a redirection pointer */
    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer);

        spgPageIndexMultiDelete(
            &mut state,
            page,
            toDelete,
            (*xldata).nMoves as c_int,
            if state.isBuild { SPGIST_PLACEHOLDER } else { SPGIST_REDIRECT },
            SPGIST_PLACEHOLDER,
            blknoDst,
            *toInsert.add((nInsert - 1) as usize),
        );

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    /* And update the parent downlink */
    if XLogReadBufferForRedo(record, 2, &mut buffer) == BLK_NEEDS_REDO {
        let tuple: SpGistInnerTuple;

        page = BufferGetPage(buffer);

        tuple = PageGetItem(page, PageGetItemId(page, (*xldata).offnumParent))
            as SpGistInnerTuple;

        spgUpdateNodeLink(
            tuple,
            (*xldata).nodeI as c_int,
            blknoDst,
            *toInsert.add((nInsert - 1) as usize),
        );

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

unsafe fn spgRedoAddNode(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut ptr: *mut c_char = XLogRecGetData(record);
    let xldata: *mut spgxlogAddNode = ptr as *mut spgxlogAddNode;
    let innerTuple: *mut c_char;
    let mut innerTupleHdr: SpGistInnerTupleData = std::mem::zeroed();
    let mut state: SpGistState = std::mem::zeroed();
    let mut buffer: Buffer = 0;
    let page: Page;
    let action: XLogRedoAction;

    ptr = ptr.add(std::mem::size_of::<spgxlogAddNode>());
    innerTuple = ptr;
    /* the tuple is unaligned, so make a copy to access its header */
    std::ptr::copy_nonoverlapping(
        innerTuple,
        &mut innerTupleHdr as *mut SpGistInnerTupleData as *mut c_char,
        std::mem::size_of::<SpGistInnerTupleData>(),
    );

    fillFakeState(&mut state, (*xldata).stateSrc);

    if !XLogRecHasBlockRef(record, 1) {
        /* update in place */
        Assert!((*xldata).parentBlk == -1);
        if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
            page = BufferGetPage(buffer);

            PageIndexTupleDelete(page, (*xldata).offnum);
            if PageAddItem(
                page,
                innerTuple as Item,
                innerTupleHdr.size as Size,
                (*xldata).offnum,
                false,
                false,
            ) != (*xldata).offnum
            {
                elog!(
                    ERROR,
                    "failed to add item of size {} to SPGiST index page",
                    innerTupleHdr.size
                );
            }

            PageSetLSN(page, lsn);
            MarkBufferDirty(buffer);
        }
        if BufferIsValid(buffer) {
            UnlockReleaseBuffer(buffer);
        }
    } else {
        let mut blkno: BlockNumber = 0;
        let mut blknoNew: BlockNumber = 0;

        XLogRecGetBlockTag(record, 0, std::ptr::null_mut(), std::ptr::null_mut(), &mut blkno);
        XLogRecGetBlockTag(record, 1, std::ptr::null_mut(), std::ptr::null_mut(), &mut blknoNew);
        let _ = blkno;

        /*
         * In normal operation we would have all three pages (source, dest,
         * and parent) locked simultaneously; but in WAL replay it should be
         * safe to update them one at a time, as long as we do it in the right
         * order. We must insert the new tuple before replacing the old tuple
         * with the redirect tuple.
         */

        /* Install new tuple first so redirect is valid */
        if (*xldata).newPage {
            /* AddNode is not used for nulls pages */
            buffer = XLogInitBufferForRedo(record, 1);
            SpGistInitBuffer(buffer, 0);
            action = BLK_NEEDS_REDO;
        } else {
            action = XLogReadBufferForRedo(record, 1, &mut buffer);
        }
        if action == BLK_NEEDS_REDO {
            page = BufferGetPage(buffer);

            addOrReplaceTuple(
                page,
                innerTuple as Item,
                innerTupleHdr.size as c_int,
                (*xldata).offnumNew,
            );

            /*
             * If parent is in this same page, update it now.
             */
            if (*xldata).parentBlk == 1 {
                let parentTuple: SpGistInnerTuple = PageGetItem(
                    page,
                    PageGetItemId(page, (*xldata).offnumParent),
                ) as SpGistInnerTuple;

                spgUpdateNodeLink(
                    parentTuple,
                    (*xldata).nodeI as c_int,
                    blknoNew,
                    (*xldata).offnumNew,
                );
            }
            PageSetLSN(page, lsn);
            MarkBufferDirty(buffer);
        }
        if BufferIsValid(buffer) {
            UnlockReleaseBuffer(buffer);
        }

        /* Delete old tuple, replacing it with redirect or placeholder tuple */
        if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
            let dt: SpGistDeadTuple;

            page = BufferGetPage(buffer);

            if state.isBuild {
                dt = spgFormDeadTuple(
                    &mut state,
                    SPGIST_PLACEHOLDER,
                    InvalidBlockNumber,
                    InvalidOffsetNumber,
                );
            } else {
                dt = spgFormDeadTuple(
                    &mut state,
                    SPGIST_REDIRECT,
                    blknoNew,
                    (*xldata).offnumNew,
                );
            }

            PageIndexTupleDelete(page, (*xldata).offnum);
            if PageAddItem(
                page,
                dt as Item,
                SGDT_GET_SIZE(dt) as Size,
                (*xldata).offnum,
                false,
                false,
            ) != (*xldata).offnum
            {
                elog!(
                    ERROR,
                    "failed to add item of size {} to SPGiST index page",
                    SGDT_GET_SIZE(dt)
                );
            }

            if state.isBuild {
                (*SpGistPageGetOpaque(page)).nPlaceholder += 1;
            } else {
                (*SpGistPageGetOpaque(page)).nRedirection += 1;
            }

            /*
             * If parent is in this same page, update it now.
             */
            if (*xldata).parentBlk == 0 {
                let parentTuple: SpGistInnerTuple = PageGetItem(
                    page,
                    PageGetItemId(page, (*xldata).offnumParent),
                ) as SpGistInnerTuple;

                spgUpdateNodeLink(
                    parentTuple,
                    (*xldata).nodeI as c_int,
                    blknoNew,
                    (*xldata).offnumNew,
                );
            }
            PageSetLSN(page, lsn);
            MarkBufferDirty(buffer);
        }
        if BufferIsValid(buffer) {
            UnlockReleaseBuffer(buffer);
        }

        /*
         * Update parent downlink (if we didn't do it as part of the source or
         * destination page update already).
         */
        if (*xldata).parentBlk == 2 {
            if XLogReadBufferForRedo(record, 2, &mut buffer) == BLK_NEEDS_REDO {
                let parentTuple: SpGistInnerTuple;

                page = BufferGetPage(buffer);

                parentTuple = PageGetItem(
                    page,
                    PageGetItemId(page, (*xldata).offnumParent),
                ) as SpGistInnerTuple;

                spgUpdateNodeLink(
                    parentTuple,
                    (*xldata).nodeI as c_int,
                    blknoNew,
                    (*xldata).offnumNew,
                );

                PageSetLSN(page, lsn);
                MarkBufferDirty(buffer);
            }
            if BufferIsValid(buffer) {
                UnlockReleaseBuffer(buffer);
            }
        }
    }
}

unsafe fn spgRedoSplitTuple(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut ptr: *mut c_char = XLogRecGetData(record);
    let xldata: *mut spgxlogSplitTuple = ptr as *mut spgxlogSplitTuple;
    let prefixTuple: *mut c_char;
    let mut prefixTupleHdr: SpGistInnerTupleData = std::mem::zeroed();
    let postfixTuple: *mut c_char;
    let mut postfixTupleHdr: SpGistInnerTupleData = std::mem::zeroed();
    let mut buffer: Buffer = 0;
    let page: Page;
    let action: XLogRedoAction;

    ptr = ptr.add(std::mem::size_of::<spgxlogSplitTuple>());
    prefixTuple = ptr;
    /* the prefix tuple is unaligned, so make a copy to access its header */
    std::ptr::copy_nonoverlapping(
        prefixTuple,
        &mut prefixTupleHdr as *mut SpGistInnerTupleData as *mut c_char,
        std::mem::size_of::<SpGistInnerTupleData>(),
    );
    ptr = ptr.add(prefixTupleHdr.size as usize);
    postfixTuple = ptr;
    /* postfix tuple is also unaligned */
    std::ptr::copy_nonoverlapping(
        postfixTuple,
        &mut postfixTupleHdr as *mut SpGistInnerTupleData as *mut c_char,
        std::mem::size_of::<SpGistInnerTupleData>(),
    );

    /*
     * In normal operation we would have both pages locked simultaneously; but
     * in WAL replay it should be safe to update them one at a time, as long
     * as we do it in the right order.
     */

    /* insert postfix tuple first to avoid dangling link */
    if !(*xldata).postfixBlkSame {
        if (*xldata).newPage {
            buffer = XLogInitBufferForRedo(record, 1);
            /* SplitTuple is not used for nulls pages */
            SpGistInitBuffer(buffer, 0);
            action = BLK_NEEDS_REDO;
        } else {
            action = XLogReadBufferForRedo(record, 1, &mut buffer);
        }
        if action == BLK_NEEDS_REDO {
            page = BufferGetPage(buffer);

            addOrReplaceTuple(
                page,
                postfixTuple as Item,
                postfixTupleHdr.size as c_int,
                (*xldata).offnumPostfix,
            );

            PageSetLSN(page, lsn);
            MarkBufferDirty(buffer);
        }
        if BufferIsValid(buffer) {
            UnlockReleaseBuffer(buffer);
        }
    }

    /* now handle the original page */
    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer);

        PageIndexTupleDelete(page, (*xldata).offnumPrefix);
        if PageAddItem(
            page,
            prefixTuple as Item,
            prefixTupleHdr.size as Size,
            (*xldata).offnumPrefix,
            false,
            false,
        ) != (*xldata).offnumPrefix
        {
            elog!(
                ERROR,
                "failed to add item of size {} to SPGiST index page",
                prefixTupleHdr.size
            );
        }

        if (*xldata).postfixBlkSame {
            addOrReplaceTuple(
                page,
                postfixTuple as Item,
                postfixTupleHdr.size as c_int,
                (*xldata).offnumPostfix,
            );
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

unsafe fn spgRedoPickSplit(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut ptr: *mut c_char = XLogRecGetData(record);
    let xldata: *mut spgxlogPickSplit = ptr as *mut spgxlogPickSplit;
    let innerTuple: *mut c_char;
    let mut innerTupleHdr: SpGistInnerTupleData = std::mem::zeroed();
    let mut state: SpGistState = std::mem::zeroed();
    let toDelete: *mut OffsetNumber;
    let toInsert: *mut OffsetNumber;
    let leafPageSelect: *mut uint8;
    let mut srcBuffer: Buffer;
    let mut destBuffer: Buffer;
    let mut innerBuffer: Buffer = 0;
    let mut srcPage: Page;
    let mut destPage: Page;
    let mut page: Page;
    let mut blknoInner: BlockNumber = 0;
    let action: XLogRedoAction;

    XLogRecGetBlockTag(record, 2, std::ptr::null_mut(), std::ptr::null_mut(), &mut blknoInner);

    fillFakeState(&mut state, (*xldata).stateSrc);

    ptr = ptr.add(SizeOfSpgxlogPickSplit);
    toDelete = ptr as *mut OffsetNumber;
    ptr = ptr.add(std::mem::size_of::<OffsetNumber>() * (*xldata).nDelete as usize);
    toInsert = ptr as *mut OffsetNumber;
    ptr = ptr.add(std::mem::size_of::<OffsetNumber>() * (*xldata).nInsert as usize);
    leafPageSelect = ptr as *mut uint8;
    ptr = ptr.add(std::mem::size_of::<uint8>() * (*xldata).nInsert as usize);

    innerTuple = ptr;
    /* the inner tuple is unaligned, so make a copy to access its header */
    std::ptr::copy_nonoverlapping(
        innerTuple,
        &mut innerTupleHdr as *mut SpGistInnerTupleData as *mut c_char,
        std::mem::size_of::<SpGistInnerTupleData>(),
    );
    ptr = ptr.add(innerTupleHdr.size as usize);

    /* now ptr points to the list of leaf tuples */

    if (*xldata).isRootSplit {
        /* when splitting root, we touch it only in the guise of new inner */
        srcBuffer = InvalidBuffer;
        srcPage = std::ptr::null_mut();
    } else if (*xldata).initSrc {
        /* just re-init the source page */
        srcBuffer = XLogInitBufferForRedo(record, 0);
        srcPage = BufferGetPage(srcBuffer) as Page;

        SpGistInitBuffer(
            srcBuffer,
            (SPGIST_LEAF | (if (*xldata).storesNulls { SPGIST_NULLS } else { 0 })) as uint16,
        );
        /* don't update LSN etc till we're done with it */
    } else {
        /*
         * Delete the specified tuples from source page.  (In case we're in
         * Hot Standby, we need to hold lock on the page till we're done
         * inserting leaf tuples and the new inner tuple, else the added
         * redirect tuple will be a dangling link.)
         */
        srcPage = std::ptr::null_mut();
        srcBuffer = 0;
        if XLogReadBufferForRedo(record, 0, &mut srcBuffer) == BLK_NEEDS_REDO {
            srcPage = BufferGetPage(srcBuffer);

            /*
             * We have it a bit easier here than in doPickSplit(), because we
             * know the inner tuple's location already, so we can inject the
             * correct redirection tuple now.
             */
            if !state.isBuild {
                spgPageIndexMultiDelete(
                    &mut state,
                    srcPage,
                    toDelete,
                    (*xldata).nDelete as c_int,
                    SPGIST_REDIRECT,
                    SPGIST_PLACEHOLDER,
                    blknoInner,
                    (*xldata).offnumInner,
                );
            } else {
                spgPageIndexMultiDelete(
                    &mut state,
                    srcPage,
                    toDelete,
                    (*xldata).nDelete as c_int,
                    SPGIST_PLACEHOLDER,
                    SPGIST_PLACEHOLDER,
                    InvalidBlockNumber,
                    InvalidOffsetNumber,
                );
            }

            /* don't update LSN etc till we're done with it */
        }
    }

    /* try to access dest page if any */
    if !XLogRecHasBlockRef(record, 1) {
        destBuffer = InvalidBuffer;
        destPage = std::ptr::null_mut();
    } else if (*xldata).initDest {
        /* just re-init the dest page */
        destBuffer = XLogInitBufferForRedo(record, 1);
        destPage = BufferGetPage(destBuffer) as Page;

        SpGistInitBuffer(
            destBuffer,
            (SPGIST_LEAF | (if (*xldata).storesNulls { SPGIST_NULLS } else { 0 })) as uint16,
        );
        /* don't update LSN etc till we're done with it */
    } else {
        /*
         * We could probably release the page lock immediately in the
         * full-page-image case, but for safety let's hold it till later.
         */
        destBuffer = 0;
        if XLogReadBufferForRedo(record, 1, &mut destBuffer) == BLK_NEEDS_REDO {
            destPage = BufferGetPage(destBuffer) as Page;
        } else {
            destPage = std::ptr::null_mut(); /* don't do any page updates */
        }
    }

    /* restore leaf tuples to src and/or dest page */
    for i in 0..(*xldata).nInsert as c_int {
        let leafTuple: *mut c_char;
        let mut leafTupleHdr: SpGistLeafTupleData = std::mem::zeroed();

        /* the tuples are not aligned, so must copy to access the size field. */
        leafTuple = ptr;
        std::ptr::copy_nonoverlapping(
            leafTuple,
            &mut leafTupleHdr as *mut SpGistLeafTupleData as *mut c_char,
            std::mem::size_of::<SpGistLeafTupleData>(),
        );
        ptr = ptr.add(getLeafSize(&leafTupleHdr) as usize);

        page = if *leafPageSelect.add(i as usize) != 0 { destPage } else { srcPage };
        if page.is_null() {
            continue; /* no need to touch this page */
        }

        addOrReplaceTuple(
            page,
            leafTuple as Item,
            getLeafSize(&leafTupleHdr),
            *toInsert.add(i as usize),
        );
    }

    /* Now update src and dest page LSNs if needed */
    if !srcPage.is_null() {
        PageSetLSN(srcPage, lsn);
        MarkBufferDirty(srcBuffer);
    }
    if !destPage.is_null() {
        PageSetLSN(destPage, lsn);
        MarkBufferDirty(destBuffer);
    }

    /* restore new inner tuple */
    if (*xldata).initInner {
        innerBuffer = XLogInitBufferForRedo(record, 2);
        SpGistInitBuffer(
            innerBuffer,
            (if (*xldata).storesNulls { SPGIST_NULLS } else { 0 }) as uint16,
        );
        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, 2, &mut innerBuffer);
    }

    if action == BLK_NEEDS_REDO {
        page = BufferGetPage(innerBuffer);

        addOrReplaceTuple(
            page,
            innerTuple as Item,
            innerTupleHdr.size as c_int,
            (*xldata).offnumInner,
        );

        /* if inner is also parent, update link while we're here */
        if (*xldata).innerIsParent {
            let parent: SpGistInnerTuple = PageGetItem(
                page,
                PageGetItemId(page, (*xldata).offnumParent),
            ) as SpGistInnerTuple;
            spgUpdateNodeLink(
                parent,
                (*xldata).nodeI as c_int,
                blknoInner,
                (*xldata).offnumInner,
            );
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(innerBuffer);
    }
    if BufferIsValid(innerBuffer) {
        UnlockReleaseBuffer(innerBuffer);
    }

    /*
     * Now we can release the leaf-page locks.  It's okay to do this before
     * updating the parent downlink.
     */
    if BufferIsValid(srcBuffer) {
        UnlockReleaseBuffer(srcBuffer);
    }
    if BufferIsValid(destBuffer) {
        UnlockReleaseBuffer(destBuffer);
    }

    /* update parent downlink, unless we did it above */
    if XLogRecHasBlockRef(record, 3) {
        let mut parentBuffer: Buffer = 0;

        if XLogReadBufferForRedo(record, 3, &mut parentBuffer) == BLK_NEEDS_REDO {
            let parent: SpGistInnerTuple;

            page = BufferGetPage(parentBuffer);

            parent = PageGetItem(page, PageGetItemId(page, (*xldata).offnumParent))
                as SpGistInnerTuple;
            spgUpdateNodeLink(
                parent,
                (*xldata).nodeI as c_int,
                blknoInner,
                (*xldata).offnumInner,
            );

            PageSetLSN(page, lsn);
            MarkBufferDirty(parentBuffer);
        }
        if BufferIsValid(parentBuffer) {
            UnlockReleaseBuffer(parentBuffer);
        }
    } else {
        Assert!((*xldata).innerIsParent || (*xldata).isRootSplit);
    }
}

unsafe fn spgRedoVacuumLeaf(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut ptr: *mut c_char = XLogRecGetData(record);
    let xldata: *mut spgxlogVacuumLeaf = ptr as *mut spgxlogVacuumLeaf;
    let toDead: *mut OffsetNumber;
    let toPlaceholder: *mut OffsetNumber;
    let moveSrc: *mut OffsetNumber;
    let moveDest: *mut OffsetNumber;
    let chainSrc: *mut OffsetNumber;
    let chainDest: *mut OffsetNumber;
    let mut state: SpGistState = std::mem::zeroed();
    let mut buffer: Buffer = 0;
    let page: Page;

    fillFakeState(&mut state, (*xldata).stateSrc);

    ptr = ptr.add(SizeOfSpgxlogVacuumLeaf);
    toDead = ptr as *mut OffsetNumber;
    ptr = ptr.add(std::mem::size_of::<OffsetNumber>() * (*xldata).nDead as usize);
    toPlaceholder = ptr as *mut OffsetNumber;
    ptr = ptr.add(std::mem::size_of::<OffsetNumber>() * (*xldata).nPlaceholder as usize);
    moveSrc = ptr as *mut OffsetNumber;
    ptr = ptr.add(std::mem::size_of::<OffsetNumber>() * (*xldata).nMove as usize);
    moveDest = ptr as *mut OffsetNumber;
    ptr = ptr.add(std::mem::size_of::<OffsetNumber>() * (*xldata).nMove as usize);
    chainSrc = ptr as *mut OffsetNumber;
    ptr = ptr.add(std::mem::size_of::<OffsetNumber>() * (*xldata).nChain as usize);
    chainDest = ptr as *mut OffsetNumber;

    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer);

        spgPageIndexMultiDelete(
            &mut state,
            page,
            toDead,
            (*xldata).nDead as c_int,
            SPGIST_DEAD,
            SPGIST_DEAD,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        );

        spgPageIndexMultiDelete(
            &mut state,
            page,
            toPlaceholder,
            (*xldata).nPlaceholder as c_int,
            SPGIST_PLACEHOLDER,
            SPGIST_PLACEHOLDER,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        );

        /* see comments in vacuumLeafPage() */
        for i in 0..(*xldata).nMove as c_int {
            let idSrc: ItemId = PageGetItemId(page, *moveSrc.add(i as usize));
            let idDest: ItemId = PageGetItemId(page, *moveDest.add(i as usize));

            let tmp = *idSrc;
            *idSrc = *idDest;
            *idDest = tmp;
        }

        spgPageIndexMultiDelete(
            &mut state,
            page,
            moveSrc,
            (*xldata).nMove as c_int,
            SPGIST_PLACEHOLDER,
            SPGIST_PLACEHOLDER,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        );

        for i in 0..(*xldata).nChain as c_int {
            let lt: SpGistLeafTuple = PageGetItem(
                page,
                PageGetItemId(page, *chainSrc.add(i as usize)),
            ) as SpGistLeafTuple;
            Assert!(SGLT_GET_TUPSTATE(lt) == SPGIST_LIVE);
            SGLT_SET_NEXTOFFSET(lt, *chainDest.add(i as usize));
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

unsafe fn spgRedoVacuumRoot(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let ptr: *mut c_char = XLogRecGetData(record);
    let xldata: *mut spgxlogVacuumRoot = ptr as *mut spgxlogVacuumRoot;
    let toDelete: *mut OffsetNumber;
    let mut buffer: Buffer = 0;
    let page: Page;

    /* the offsets array follows the fixed part of the struct */
    toDelete = (xldata as *mut c_char).add(std::mem::size_of::<spgxlogVacuumRoot>())
        as *mut OffsetNumber;

    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer);

        /* The tuple numbers are in order */
        PageIndexMultiDelete(page, toDelete, (*xldata).nDelete as c_int);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

unsafe fn spgRedoVacuumRedirect(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let ptr: *mut c_char = XLogRecGetData(record);
    let xldata: *mut spgxlogVacuumRedirect = ptr as *mut spgxlogVacuumRedirect;
    let itemToPlaceholder: *mut OffsetNumber;
    let mut buffer: Buffer = 0;

    /* the offsets array follows the fixed part of the struct */
    itemToPlaceholder = (xldata as *mut c_char)
        .add(std::mem::size_of::<spgxlogVacuumRedirect>())
        as *mut OffsetNumber;

    /*
     * If any redirection tuples are being removed, make sure there are no
     * live Hot Standby transactions that might need to see them.
     */
    if InHotStandby {
        let mut locator: RelFileLocator = std::mem::zeroed();

        XLogRecGetBlockTag(record, 0, &mut locator, std::ptr::null_mut(), std::ptr::null_mut());
        ResolveRecoveryConflictWithSnapshot(
            (*xldata).snapshotConflictHorizon,
            (*xldata).isCatalogRel,
            locator,
        );
    }

    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        let page: Page = BufferGetPage(buffer);
        let opaque: *mut SpGistPageOpaqueData = SpGistPageGetOpaque(page);

        /* Convert redirect pointers to plain placeholders */
        for i in 0..(*xldata).nToPlaceholder as c_int {
            let dt: SpGistDeadTuple = PageGetItem(
                page,
                PageGetItemId(page, *itemToPlaceholder.add(i as usize)),
            ) as SpGistDeadTuple;
            Assert!(SGDT_GET_TUPSTATE(dt) == SPGIST_REDIRECT);
            SGDT_SET_TUPSTATE(dt, SPGIST_PLACEHOLDER);
            ItemPointerSetInvalid(&mut (*dt).pointer);
        }

        Assert!((*opaque).nRedirection >= (*xldata).nToPlaceholder);
        (*opaque).nRedirection -= (*xldata).nToPlaceholder;
        (*opaque).nPlaceholder += (*xldata).nToPlaceholder;

        /* Remove placeholder tuples at end of page */
        if (*xldata).firstPlaceholder != InvalidOffsetNumber {
            let max: c_int = PageGetMaxOffsetNumber(page) as c_int;
            let toDelete: *mut OffsetNumber;

            toDelete = palloc(std::mem::size_of::<OffsetNumber>() * max as usize)
                as *mut OffsetNumber;

            let mut i = (*xldata).firstPlaceholder as c_int;
            while i <= max {
                *toDelete.add((i - (*xldata).firstPlaceholder as c_int) as usize) =
                    i as OffsetNumber;
                i += 1;
            }

            let i = max - (*xldata).firstPlaceholder as c_int + 1;
            Assert!((*opaque).nPlaceholder as c_int >= i);
            (*opaque).nPlaceholder -= i as uint16;

            /* The array is sorted, so can use PageIndexMultiDelete */
            PageIndexMultiDelete(page, toDelete, i);

            pfree(toDelete as *mut c_void);
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

pub unsafe fn spg_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;
    let oldCxt: MemoryContext;

    oldCxt = MemoryContextSwitchTo(opCtx);
    match info {
        XLOG_SPGIST_ADD_LEAF => spgRedoAddLeaf(record),
        XLOG_SPGIST_MOVE_LEAFS => spgRedoMoveLeafs(record),
        XLOG_SPGIST_ADD_NODE => spgRedoAddNode(record),
        XLOG_SPGIST_SPLIT_TUPLE => spgRedoSplitTuple(record),
        XLOG_SPGIST_PICKSPLIT => spgRedoPickSplit(record),
        XLOG_SPGIST_VACUUM_LEAF => spgRedoVacuumLeaf(record),
        XLOG_SPGIST_VACUUM_ROOT => spgRedoVacuumRoot(record),
        XLOG_SPGIST_VACUUM_REDIRECT => spgRedoVacuumRedirect(record),
        _ => {
            elog!(PANIC, "spg_redo: unknown op code {}", info);
        }
    }

    MemoryContextSwitchTo(oldCxt);
    MemoryContextReset(opCtx);
}

pub unsafe fn spg_xlog_startup() {
    opCtx = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"SP-GiST temporary context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
}

pub unsafe fn spg_xlog_cleanup() {
    MemoryContextDelete(opCtx);
    opCtx = std::ptr::null_mut();
}

/*
 * Mask a SpGist page before performing consistency checks on it.
 */
pub unsafe fn spg_mask(pagedata: *mut c_char, blkno: BlockNumber) {
    let _ = blkno;
    let page: Page = pagedata as Page;
    let pagehdr: PageHeader = page as PageHeader;

    mask_page_lsn_and_checksum(page);

    mask_page_hint_bits(page);

    /*
     * Mask the unused space, but only if the page's pd_lower appears to have
     * been set correctly.
     */
    if (*pagehdr).pd_lower as usize >= SizeOfPageHeaderData {
        mask_unused_space(page);
    }
}

// ---------------------------------------------------------------------------
// Helpers for unaligned leaf-tuple size access (size is the upper 30 bits of
// the packed bits_ field of SpGistLeafTupleData).
// ---------------------------------------------------------------------------

#[inline]
unsafe fn getLeafSize(hdr: *const SpGistLeafTupleData) -> c_int {
    ((*hdr).bits_ >> 2) as c_int
}

// ---------------------------------------------------------------------------
// Local stubs for unported dependencies.
// ---------------------------------------------------------------------------

// SGDTSIZE = MAXALIGN(sizeof(SpGistDeadTupleData)).
const SGDTSIZE: Size =
    MAXALIGN(std::mem::size_of::<crate::access::spgist::spgist_private::SpGistDeadTupleData>());

// RelFileLocator as used by XLogRecGetBlockTag (access/xlogreader.h).
use crate::access::transam::xlogreader::RelFileLocator;

const InHotStandby: bool = false; // TODO: storage/standby.h

#[inline]
unsafe fn SpGistPageGetOpaque(page: Page) -> *mut SpGistPageOpaqueData {
    PageGetSpecialPointer(page) as *mut SpGistPageOpaqueData
} // TODO: access/spgist_private.h

#[allow(non_snake_case)]
unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.h
}

#[allow(non_snake_case)]
unsafe fn BufferIsValid(_buffer: Buffer) -> bool {
    unimplemented!() // TODO: storage/bufmgr.h
}

#[allow(non_snake_case)]
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.c
}

#[allow(non_snake_case)]
unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.c
}

#[allow(non_snake_case)]
unsafe fn ResolveRecoveryConflictWithSnapshot(
    _snapshotConflictHorizon: TransactionId,
    _isCatalogRel: bool,
    _locator: RelFileLocator,
) {
    unimplemented!() // TODO: storage/standby.c
}
