//! access/spgist/spgvacuum.c - vacuum for SP-GiST.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::access::spgist::spgist_private::{
    initSpGistState, spgPageIndexMultiDelete, SGLT_GET_NEXTOFFSET, SGLT_SET_NEXTOFFSET,
    SpGistBlockIsRoot, SpGistDeadTuple, SpGistDeadTupleData, SpGistInnerTuple,
    SpGistInnerTupleData, SpGistLeafTuple, SpGistLeafTupleData, SpGistNodeTuple, SpGistPageOpaque,
    SpGistPageOpaqueData, SpGistSetLastUsedPage, SpGistState, SpGistUpdateMetaPage,
    MaxIndexTuplesPerPage, SPGIST_DEAD, SPGIST_LAST_FIXED_BLKNO, SPGIST_LIVE,
    SPGIST_METAPAGE_BLKNO, SPGIST_PLACEHOLDER, SPGIST_REDIRECT,
};
use crate::access::rmgrlist::RmgrId;
use crate::access::transam::transam::{TransactionIdFollowsOrEquals, TransactionIdPrecedes};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::miscadmin::{END_CRIT_SECTION, START_CRIT_SECTION};
use crate::storage::block::BlockNumber;
use crate::storage::off::OffsetNumber;
use crate::utils::rel::Relation;

use std::mem::size_of;
use std::ptr;

/* ------------------------------------------------------------------ */
/* Local type aliases / stub types (faithful structure first)         */
/* ------------------------------------------------------------------ */

pub type Buffer = c_int;
pub type Page = *mut c_char;
pub type ItemId = *mut ItemIdData;
pub type ItemPointer = *mut ItemPointerData;
pub type IndexTuple = *mut IndexTupleData;
pub type GlobalVisState = c_void;

#[repr(C)]
pub struct ItemPointerData {
    pub dummy: u8,
}

#[repr(C)]
pub struct ItemIdData {
    pub dummy: c_uint,
}

#[repr(C)]
pub struct IndexTupleData {
    pub t_tid: ItemPointerData,
}

#[repr(C)]
pub struct IndexVacuumInfo {
    pub index: Relation,
    pub heaprel: Relation,
    pub analyze_only: bool,
    pub estimated_count: bool,
    pub num_heap_tuples: f64,
    pub strategy: *mut c_void,
}

#[repr(C)]
pub struct IndexBulkDeleteResult {
    pub num_pages: BlockNumber,
    pub estimated_count: bool,
    pub num_index_tuples: f64,
    pub tuples_removed: f64,
    pub pages_newly_deleted: BlockNumber,
    pub pages_deleted: BlockNumber,
    pub pages_free: BlockNumber,
}

pub type IndexBulkDeleteCallback =
    Option<unsafe extern "C" fn(itemptr: ItemPointer, state: *mut c_void) -> bool>;

#[repr(C)]
pub struct BlockRangeReadStreamPrivate {
    pub current_blocknum: BlockNumber,
    pub last_exclusive: BlockNumber,
}

pub enum ReadStream {}

/* xlog record types (access/spgxlog.h) */
pub const XLOG_SPGIST_VACUUM_LEAF: uint8 = 0x60;
pub const XLOG_SPGIST_VACUUM_ROOT: uint8 = 0x70;
pub const XLOG_SPGIST_VACUUM_REDIRECT: uint8 = 0x80;

/* spgxlogState (access/spgxlog.h) */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct spgxlogState {
    pub redirectXid: TransactionId,
    pub isBuild: bool,
}

#[repr(C)]
pub struct spgxlogVacuumLeaf {
    pub nDead: uint16,        /* number of tuples to become DEAD */
    pub nPlaceholder: uint16, /* number of tuples to become PLACEHOLDER */
    pub nMove: uint16,        /* number of tuples to move */
    pub nChain: uint16,       /* number of tuples to re-chain */
    pub stateSrc: spgxlogState,
    /* offsets[] follows */
}
pub const SizeOfSpgxlogVacuumLeaf: usize = core::mem::offset_of!(spgxlogVacuumLeaf, stateSrc)
    + size_of::<spgxlogState>();

#[repr(C)]
pub struct spgxlogVacuumRoot {
    pub nDelete: uint16, /* number of tuples to delete */
    pub stateSrc: spgxlogState,
    /* offsets[] follows */
}
pub const SizeOfSpgxlogVacuumRoot: usize = core::mem::offset_of!(spgxlogVacuumRoot, stateSrc)
    + size_of::<spgxlogState>();

#[repr(C)]
pub struct spgxlogVacuumRedirect {
    pub nToPlaceholder: uint16,             /* number of redirects to make placeholders */
    pub firstPlaceholder: OffsetNumber,     /* first placeholder tuple to remove */
    pub snapshotConflictHorizon: TransactionId, /* newest XID of removed redirects */
    pub isCatalogRel: bool,                 /* recovery conflict handling on standby */
    /* offsets[] follows */
}
pub const SizeOfSpgxlogVacuumRedirect: usize =
    core::mem::offset_of!(spgxlogVacuumRedirect, isCatalogRel) + size_of::<bool>();

/* Snapshot, used via GetActiveSnapshot()->xmin */
#[repr(C)]
pub struct SnapshotData {
    pub xmin: TransactionId,
}
pub type Snapshot = *mut SnapshotData;

/* constants */
pub const MAIN_FORKNUM: c_int = 0;
pub const ExclusiveLock: c_int = 7;
pub const RBM_NORMAL: c_int = 0;
pub const BUFFER_LOCK_EXCLUSIVE: c_int = 2;
pub const InvalidBuffer: Buffer = 0;
pub const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
pub const FirstOffsetNumber: OffsetNumber = 1;
pub const InvalidOffsetNumber: OffsetNumber = 0;
pub const InvalidTransactionId: TransactionId = 0;
pub const REGBUF_STANDARD: c_int = 0x04;
pub const RM_SPGIST_ID: RmgrId = 19;

/* read_stream.h flags */
pub const READ_STREAM_MAINTENANCE: c_int = 0x01;
pub const READ_STREAM_FULL: c_int = 0x04;
pub const READ_STREAM_USE_BATCHING: c_int = 0x10;

pub type ReadStreamBlockNumberCB = Option<
    unsafe extern "C" fn(
        stream: *mut ReadStream,
        callback_private_data: *mut c_void,
        per_buffer_data: *mut c_void,
    ) -> BlockNumber,
>;

/* ------------------------------------------------------------------ */
/* Local bitfield accessors (spgist_private.h stores these in bits_)  */
/* ------------------------------------------------------------------ */

/* tupstate is the low 2 bits of bits_ for all three tuple kinds */
#[inline]
unsafe fn LT_TUPSTATE(lt: SpGistLeafTuple) -> c_int {
    ((*lt).bits_ & 0x3) as c_int
}
#[inline]
unsafe fn DT_TUPSTATE(dt: SpGistDeadTuple) -> c_int {
    ((*dt).bits_ & 0x3) as c_int
}
#[inline]
unsafe fn DT_SET_TUPSTATE(dt: SpGistDeadTuple, st: c_int) {
    (*dt).bits_ = ((*dt).bits_ & !0x3u32) | ((st as c_uint) & 0x3);
}
#[inline]
unsafe fn IT_TUPSTATE(it: SpGistInnerTuple) -> c_int {
    ((*it).bits_ & 0x3) as c_int
}
/* nNodes is bits 3..15 (after tupstate:2, allTheSame:1) */
#[inline]
unsafe fn IT_NNODES(it: SpGistInnerTuple) -> c_int {
    (((*it).bits_ >> 3) & 0x1FFF) as c_int
}

/* ------------------------------------------------------------------ */

/* Entry in pending-list of TIDs we need to revisit */
#[repr(C)]
struct spgVacPendingItem {
    tid: ItemPointerData,                   /* redirection target to visit */
    done: bool,                             /* have we dealt with this? */
    next: *mut spgVacPendingItem,           /* list link */
}

/* Local state for vacuum operations */
#[repr(C)]
struct spgBulkDeleteState {
    /* Parameters passed in to spgvacuumscan */
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,

    /* Additional working state */
    spgstate: SpGistState,                  /* for SPGiST operations that need one */
    pendingList: *mut spgVacPendingItem,    /* TIDs we need to (re)visit */
    myXmin: TransactionId,                  /* for detecting newly-added redirects */
    lastFilledBlock: BlockNumber,           /* last non-deletable block */
}

/*
 * Add TID to pendingList, but only if not already present.
 *
 * Note that new items are always appended at the end of the list; this
 * ensures that scans of the list don't miss items added during the scan.
 */
unsafe fn spgAddPendingTID(bds: *mut spgBulkDeleteState, tid: ItemPointer) {
    let pitem: *mut spgVacPendingItem;
    let mut listLink: *mut *mut spgVacPendingItem;

    /* search the list for pre-existing entry */
    listLink = &mut (*bds).pendingList;
    while !(*listLink).is_null() {
        let cur = *listLink;
        if ItemPointerEquals(tid, &raw mut (*cur).tid) {
            return; /* already in list, do nothing */
        }
        listLink = &mut (*cur).next;
    }
    /* not there, so append new entry */
    pitem = palloc(size_of::<spgVacPendingItem>()) as *mut spgVacPendingItem;
    (*pitem).tid = ptr::read(tid);
    (*pitem).done = false;
    (*pitem).next = ptr::null_mut();
    *listLink = pitem;
}

/*
 * Clear pendingList
 */
unsafe fn spgClearPendingList(bds: *mut spgBulkDeleteState) {
    let mut pitem = (*bds).pendingList;
    while !pitem.is_null() {
        let nitem = (*pitem).next;
        /* All items in list should have been dealt with */
        Assert!((*pitem).done);
        pfree(pitem as *mut c_void);
        pitem = nitem;
    }
    (*bds).pendingList = ptr::null_mut();
}

/*
 * Vacuum a regular (non-root) leaf page
 *
 * We must delete tuples that are targeted for deletion by the VACUUM,
 * but not move any tuples that are referenced by outside links; we assume
 * those are the ones that are heads of chains.
 *
 * If we find a REDIRECT that was made by a concurrently-running transaction,
 * we must add its target TID to pendingList.  (We don't try to visit the
 * target immediately, first because we don't want VACUUM locking more than
 * one buffer at a time, and second because the duplicate-filtering logic
 * in spgAddPendingTID is useful to ensure we can't get caught in an infinite
 * loop in the face of continuous concurrent insertions.)
 *
 * If forPending is true, we are examining the page as a consequence of
 * chasing a redirect link, not as part of the normal sequential scan.
 * We still vacuum the page normally, but we don't increment the stats
 * about live tuples; else we'd double-count those tuples, since the page
 * has been or will be visited in the sequential scan as well.
 */
unsafe fn vacuumLeafPage(
    bds: *mut spgBulkDeleteState,
    index: Relation,
    buffer: Buffer,
    forPending: bool,
) {
    let page: Page = BufferGetPage(buffer);
    let mut xlrec: spgxlogVacuumLeaf = std::mem::zeroed();
    let mut toDead: [OffsetNumber; MaxIndexTuplesPerPage] = [0; MaxIndexTuplesPerPage];
    let mut toPlaceholder: [OffsetNumber; MaxIndexTuplesPerPage] = [0; MaxIndexTuplesPerPage];
    let mut moveSrc: [OffsetNumber; MaxIndexTuplesPerPage] = [0; MaxIndexTuplesPerPage];
    let mut moveDest: [OffsetNumber; MaxIndexTuplesPerPage] = [0; MaxIndexTuplesPerPage];
    let mut chainSrc: [OffsetNumber; MaxIndexTuplesPerPage] = [0; MaxIndexTuplesPerPage];
    let mut chainDest: [OffsetNumber; MaxIndexTuplesPerPage] = [0; MaxIndexTuplesPerPage];
    let mut predecessor: [OffsetNumber; MaxIndexTuplesPerPage + 1] =
        [0; MaxIndexTuplesPerPage + 1];
    let mut deletable: [bool; MaxIndexTuplesPerPage + 1] = [false; MaxIndexTuplesPerPage + 1];
    let mut nDeletable: c_int;
    let mut i: OffsetNumber;
    let max: OffsetNumber = PageGetMaxOffsetNumber(page);

    /* predecessor and deletable are already zero-initialized above */
    nDeletable = 0;

    /* Scan page, identify tuples to delete, accumulate stats */
    i = FirstOffsetNumber;
    while i <= max {
        let lt: SpGistLeafTuple =
            PageGetItem(page, PageGetItemId(page, i)) as SpGistLeafTuple;
        if LT_TUPSTATE(lt) == SPGIST_LIVE {
            Assert!(ItemPointerIsValid(&raw mut (*lt).heapPtr));

            if (*bds).callback.unwrap()(&raw mut (*lt).heapPtr, (*bds).callback_state) {
                (*(*bds).stats).tuples_removed += 1.0;
                deletable[i as usize] = true;
                nDeletable += 1;
            } else {
                if !forPending {
                    (*(*bds).stats).num_index_tuples += 1.0;
                }
            }

            /* Form predecessor map, too */
            if SGLT_GET_NEXTOFFSET(lt) != InvalidOffsetNumber {
                /* paranoia about corrupted chain links */
                if SGLT_GET_NEXTOFFSET(lt) < FirstOffsetNumber
                    || SGLT_GET_NEXTOFFSET(lt) > max
                    || predecessor[SGLT_GET_NEXTOFFSET(lt) as usize] != InvalidOffsetNumber
                {
                    elog!(
                        ERROR,
                        "inconsistent tuple chain links in page {} of index \"{}\"",
                        BufferGetBlockNumber(buffer),
                        RelationGetRelationName(index)
                    );
                }
                predecessor[SGLT_GET_NEXTOFFSET(lt) as usize] = i;
            }
        } else if LT_TUPSTATE(lt) == SPGIST_REDIRECT {
            let dt: SpGistDeadTuple = lt as SpGistDeadTuple;

            Assert!(SGLT_GET_NEXTOFFSET(lt) == InvalidOffsetNumber);
            Assert!(ItemPointerIsValid(&raw mut (*dt).pointer));

            /*
             * Add target TID to pending list if the redirection could have
             * happened since VACUUM started.  (If xid is invalid, assume it
             * must have happened before VACUUM started, since REINDEX
             * CONCURRENTLY locks out VACUUM.)
             *
             * Note: we could make a tighter test by seeing if the xid is
             * "running" according to the active snapshot; but snapmgr.c
             * doesn't currently export a suitable API, and it's not entirely
             * clear that a tighter test is worth the cycles anyway.
             */
            if TransactionIdFollowsOrEquals((*dt).xid, (*bds).myXmin) {
                spgAddPendingTID(bds, &raw mut (*dt).pointer);
            }
        } else {
            Assert!(SGLT_GET_NEXTOFFSET(lt) == InvalidOffsetNumber);
        }

        i += 1;
    }

    if nDeletable == 0 {
        return; /* nothing more to do */
    }

    /*----------
     * Figure out exactly what we have to do.  We do this separately from
     * actually modifying the page, mainly so that we have a representation
     * that can be dumped into WAL and then the replay code can do exactly
     * the same thing.  The output of this step consists of six arrays
     * describing four kinds of operations, to be performed in this order:
     *
     * toDead[]: tuple numbers to be replaced with DEAD tuples
     * toPlaceholder[]: tuple numbers to be replaced with PLACEHOLDER tuples
     * moveSrc[]: tuple numbers that need to be relocated to another offset
     * (replacing the tuple there) and then replaced with PLACEHOLDER tuples
     * moveDest[]: new locations for moveSrc tuples
     * chainSrc[]: tuple numbers whose chain links (nextOffset) need updates
     * chainDest[]: new values of nextOffset for chainSrc members
     *
     * It's easiest to figure out what we have to do by processing tuple
     * chains, so we iterate over all the tuples (not just the deletable
     * ones!) to identify chain heads, then chase down each chain and make
     * work item entries for deletable tuples within the chain.
     *----------
     */
    xlrec.nDead = 0;
    xlrec.nPlaceholder = 0;
    xlrec.nMove = 0;
    xlrec.nChain = 0;

    i = FirstOffsetNumber;
    while i <= max {
        let head: SpGistLeafTuple =
            PageGetItem(page, PageGetItemId(page, i)) as SpGistLeafTuple;
        if LT_TUPSTATE(head) != SPGIST_LIVE {
            i += 1;
            continue; /* can't be a chain member */
        }
        if predecessor[i as usize] != 0 {
            i += 1;
            continue; /* not a chain head */
        }

        /* initialize ... */
        let mut interveningDeletable: bool = false;
        let mut prevLive: OffsetNumber = if deletable[i as usize] {
            InvalidOffsetNumber
        } else {
            i
        };

        /* scan down the chain ... */
        let mut j: OffsetNumber = SGLT_GET_NEXTOFFSET(head);
        while j != InvalidOffsetNumber {
            let lt: SpGistLeafTuple =
                PageGetItem(page, PageGetItemId(page, j)) as SpGistLeafTuple;
            if LT_TUPSTATE(lt) != SPGIST_LIVE {
                /* all tuples in chain should be live */
                elog!(ERROR, "unexpected SPGiST tuple state: {}", LT_TUPSTATE(lt));
            }

            if deletable[j as usize] {
                /* This tuple should be replaced by a placeholder */
                toPlaceholder[xlrec.nPlaceholder as usize] = j;
                xlrec.nPlaceholder += 1;
                /* previous live tuple's chain link will need an update */
                interveningDeletable = true;
            } else if prevLive == InvalidOffsetNumber {
                /*
                 * This is the first live tuple in the chain.  It has to move
                 * to the head position.
                 */
                moveSrc[xlrec.nMove as usize] = j;
                moveDest[xlrec.nMove as usize] = i;
                xlrec.nMove += 1;
                /* Chain updates will be applied after the move */
                prevLive = i;
                interveningDeletable = false;
            } else {
                /*
                 * Second or later live tuple.  Arrange to re-chain it to the
                 * previous live one, if there was a gap.
                 */
                if interveningDeletable {
                    chainSrc[xlrec.nChain as usize] = prevLive;
                    chainDest[xlrec.nChain as usize] = j;
                    xlrec.nChain += 1;
                }
                prevLive = j;
                interveningDeletable = false;
            }

            j = SGLT_GET_NEXTOFFSET(lt);
        }

        if prevLive == InvalidOffsetNumber {
            /* The chain is entirely removable, so we need a DEAD tuple */
            toDead[xlrec.nDead as usize] = i;
            xlrec.nDead += 1;
        } else if interveningDeletable {
            /* One or more deletions at end of chain, so close it off */
            chainSrc[xlrec.nChain as usize] = prevLive;
            chainDest[xlrec.nChain as usize] = InvalidOffsetNumber;
            xlrec.nChain += 1;
        }

        i += 1;
    }

    /* sanity check ... */
    if nDeletable != (xlrec.nDead + xlrec.nPlaceholder + xlrec.nMove) as c_int {
        elog!(ERROR, "inconsistent counts of deletable tuples");
    }

    /* Do the updates */
    START_CRIT_SECTION();

    spgPageIndexMultiDelete(
        &mut (*bds).spgstate,
        page,
        toDead.as_mut_ptr(),
        xlrec.nDead as c_int,
        SPGIST_DEAD,
        SPGIST_DEAD,
        InvalidBlockNumber,
        InvalidOffsetNumber,
    );

    spgPageIndexMultiDelete(
        &mut (*bds).spgstate,
        page,
        toPlaceholder.as_mut_ptr(),
        xlrec.nPlaceholder as c_int,
        SPGIST_PLACEHOLDER,
        SPGIST_PLACEHOLDER,
        InvalidBlockNumber,
        InvalidOffsetNumber,
    );

    /*
     * We implement the move step by swapping the line pointers of the source
     * and target tuples, then replacing the newly-source tuples with
     * placeholders.  This is perhaps unduly friendly with the page data
     * representation, but it's fast and doesn't risk page overflow when a
     * tuple to be relocated is large.
     */
    let mut k: c_int = 0;
    while k < xlrec.nMove as c_int {
        let idSrc: ItemId = PageGetItemId(page, moveSrc[k as usize]);
        let idDest: ItemId = PageGetItemId(page, moveDest[k as usize]);

        let tmp: ItemIdData = ptr::read(idSrc);
        ptr::write(idSrc, ptr::read(idDest));
        ptr::write(idDest, tmp);

        k += 1;
    }

    spgPageIndexMultiDelete(
        &mut (*bds).spgstate,
        page,
        moveSrc.as_mut_ptr(),
        xlrec.nMove as c_int,
        SPGIST_PLACEHOLDER,
        SPGIST_PLACEHOLDER,
        InvalidBlockNumber,
        InvalidOffsetNumber,
    );

    let mut c: c_int = 0;
    while c < xlrec.nChain as c_int {
        let lt: SpGistLeafTuple =
            PageGetItem(page, PageGetItemId(page, chainSrc[c as usize])) as SpGistLeafTuple;
        Assert!(LT_TUPSTATE(lt) == SPGIST_LIVE);
        SGLT_SET_NEXTOFFSET(lt, chainDest[c as usize]);
        c += 1;
    }

    MarkBufferDirty(buffer);

    if RelationNeedsWAL(index) {
        let recptr: XLogRecPtr;

        XLogBeginInsert();

        STORE_STATE(&mut (*bds).spgstate, &mut xlrec.stateSrc);

        XLogRegisterData(
            &mut xlrec as *mut _ as *mut c_char,
            SizeOfSpgxlogVacuumLeaf as c_int,
        );
        /* sizeof(xlrec) should be a multiple of sizeof(OffsetNumber) */
        XLogRegisterData(
            toDead.as_mut_ptr() as *mut c_char,
            (size_of::<OffsetNumber>() * xlrec.nDead as usize) as c_int,
        );
        XLogRegisterData(
            toPlaceholder.as_mut_ptr() as *mut c_char,
            (size_of::<OffsetNumber>() * xlrec.nPlaceholder as usize) as c_int,
        );
        XLogRegisterData(
            moveSrc.as_mut_ptr() as *mut c_char,
            (size_of::<OffsetNumber>() * xlrec.nMove as usize) as c_int,
        );
        XLogRegisterData(
            moveDest.as_mut_ptr() as *mut c_char,
            (size_of::<OffsetNumber>() * xlrec.nMove as usize) as c_int,
        );
        XLogRegisterData(
            chainSrc.as_mut_ptr() as *mut c_char,
            (size_of::<OffsetNumber>() * xlrec.nChain as usize) as c_int,
        );
        XLogRegisterData(
            chainDest.as_mut_ptr() as *mut c_char,
            (size_of::<OffsetNumber>() * xlrec.nChain as usize) as c_int,
        );

        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

        recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_VACUUM_LEAF);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();
}

/*
 * Vacuum a root page when it is also a leaf
 *
 * On the root, we just delete any dead leaf tuples; no fancy business
 */
unsafe fn vacuumLeafRoot(bds: *mut spgBulkDeleteState, index: Relation, buffer: Buffer) {
    let page: Page = BufferGetPage(buffer);
    let mut xlrec: spgxlogVacuumRoot = std::mem::zeroed();
    let mut toDelete: [OffsetNumber; MaxIndexTuplesPerPage] = [0; MaxIndexTuplesPerPage];
    let mut i: OffsetNumber;
    let max: OffsetNumber = PageGetMaxOffsetNumber(page);

    xlrec.nDelete = 0;

    /* Scan page, identify tuples to delete, accumulate stats */
    i = FirstOffsetNumber;
    while i <= max {
        let lt: SpGistLeafTuple =
            PageGetItem(page, PageGetItemId(page, i)) as SpGistLeafTuple;
        if LT_TUPSTATE(lt) == SPGIST_LIVE {
            Assert!(ItemPointerIsValid(&raw mut (*lt).heapPtr));

            if (*bds).callback.unwrap()(&raw mut (*lt).heapPtr, (*bds).callback_state) {
                (*(*bds).stats).tuples_removed += 1.0;
                toDelete[xlrec.nDelete as usize] = i;
                xlrec.nDelete += 1;
            } else {
                (*(*bds).stats).num_index_tuples += 1.0;
            }
        } else {
            /* all tuples on root should be live */
            elog!(ERROR, "unexpected SPGiST tuple state: {}", LT_TUPSTATE(lt));
        }

        i += 1;
    }

    if xlrec.nDelete == 0 {
        return; /* nothing more to do */
    }

    /* Do the update */
    START_CRIT_SECTION();

    /* The tuple numbers are in order, so we can use PageIndexMultiDelete */
    PageIndexMultiDelete(page, toDelete.as_mut_ptr(), xlrec.nDelete as c_int);

    MarkBufferDirty(buffer);

    if RelationNeedsWAL(index) {
        let recptr: XLogRecPtr;

        XLogBeginInsert();

        /* Prepare WAL record */
        STORE_STATE(&mut (*bds).spgstate, &mut xlrec.stateSrc);

        XLogRegisterData(
            &mut xlrec as *mut _ as *mut c_char,
            SizeOfSpgxlogVacuumRoot as c_int,
        );
        /* sizeof(xlrec) should be a multiple of sizeof(OffsetNumber) */
        XLogRegisterData(
            toDelete.as_mut_ptr() as *mut c_char,
            (size_of::<OffsetNumber>() * xlrec.nDelete as usize) as c_int,
        );

        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

        recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_VACUUM_ROOT);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();
}

/*
 * Clean up redirect and placeholder tuples on the given page
 *
 * Redirect tuples can be marked placeholder once they're old enough.
 * Placeholder tuples can be removed if it won't change the offsets of
 * non-placeholder ones.
 *
 * Unlike the routines above, this works on both leaf and inner pages.
 */
unsafe fn vacuumRedirectAndPlaceholder(index: Relation, heaprel: Relation, buffer: Buffer) {
    let page: Page = BufferGetPage(buffer);
    let opaque: SpGistPageOpaque = SpGistPageGetOpaque(page);
    let max: OffsetNumber = PageGetMaxOffsetNumber(page);
    let mut i: OffsetNumber;
    let mut firstPlaceholder: OffsetNumber = InvalidOffsetNumber;
    let mut hasNonPlaceholder: bool = false;
    let mut hasUpdate: bool = false;
    let mut itemToPlaceholder: [OffsetNumber; MaxIndexTuplesPerPage] =
        [0; MaxIndexTuplesPerPage];
    let mut itemnos: [OffsetNumber; MaxIndexTuplesPerPage] = [0; MaxIndexTuplesPerPage];
    let mut xlrec: spgxlogVacuumRedirect = std::mem::zeroed();
    let vistest: *mut GlobalVisState;

    xlrec.isCatalogRel = RelationIsAccessibleInLogicalDecoding(heaprel);
    xlrec.nToPlaceholder = 0;
    xlrec.snapshotConflictHorizon = InvalidTransactionId;

    vistest = GlobalVisTestFor(heaprel);

    START_CRIT_SECTION();

    /*
     * Scan backwards to convert old redirection tuples to placeholder tuples,
     * and identify location of last non-placeholder tuple while at it.
     */
    i = max;
    while i >= FirstOffsetNumber && ((*opaque).nRedirection > 0 || !hasNonPlaceholder) {
        let dt: SpGistDeadTuple =
            PageGetItem(page, PageGetItemId(page, i)) as SpGistDeadTuple;

        /*
         * We can convert a REDIRECT to a PLACEHOLDER if there could no longer
         * be any index scans "in flight" to it.  Such an index scan would
         * have to be in a transaction whose snapshot sees the REDIRECT's XID
         * as still running, so comparing the XID against global xmin is a
         * conservatively safe test.  If the XID is invalid, it must have been
         * inserted by REINDEX CONCURRENTLY, so we can zap it immediately.
         */
        if DT_TUPSTATE(dt) == SPGIST_REDIRECT
            && (!TransactionIdIsValid((*dt).xid)
                || GlobalVisTestIsRemovableXid(vistest, (*dt).xid))
        {
            DT_SET_TUPSTATE(dt, SPGIST_PLACEHOLDER);
            Assert!((*opaque).nRedirection > 0);
            (*opaque).nRedirection -= 1;
            (*opaque).nPlaceholder += 1;

            /* remember newest XID among the removed redirects */
            if !TransactionIdIsValid(xlrec.snapshotConflictHorizon)
                || TransactionIdPrecedes(xlrec.snapshotConflictHorizon, (*dt).xid)
            {
                xlrec.snapshotConflictHorizon = (*dt).xid;
            }

            ItemPointerSetInvalid(&raw mut (*dt).pointer);

            itemToPlaceholder[xlrec.nToPlaceholder as usize] = i;
            xlrec.nToPlaceholder += 1;

            hasUpdate = true;
        }

        if DT_TUPSTATE(dt) == SPGIST_PLACEHOLDER {
            if !hasNonPlaceholder {
                firstPlaceholder = i;
            }
        } else {
            hasNonPlaceholder = true;
        }

        i -= 1;
    }

    /*
     * Any placeholder tuples at the end of page can safely be removed.  We
     * can't remove ones before the last non-placeholder, though, because we
     * can't alter the offset numbers of non-placeholder tuples.
     */
    if firstPlaceholder != InvalidOffsetNumber {
        /*
         * We do not store this array to rdata because it's easy to recreate.
         */
        i = firstPlaceholder;
        while i <= max {
            itemnos[(i - firstPlaceholder) as usize] = i;
            i += 1;
        }

        let n: OffsetNumber = max - firstPlaceholder + 1;
        Assert!((*opaque).nPlaceholder >= n as _);
        (*opaque).nPlaceholder -= n as _;

        /* The array is surely sorted, so can use PageIndexMultiDelete */
        PageIndexMultiDelete(page, itemnos.as_mut_ptr(), n as c_int);

        hasUpdate = true;
    }

    xlrec.firstPlaceholder = firstPlaceholder;

    if hasUpdate {
        MarkBufferDirty(buffer);
    }

    if hasUpdate && RelationNeedsWAL(index) {
        let recptr: XLogRecPtr;

        XLogBeginInsert();

        XLogRegisterData(
            &mut xlrec as *mut _ as *mut c_char,
            SizeOfSpgxlogVacuumRedirect as c_int,
        );
        XLogRegisterData(
            itemToPlaceholder.as_mut_ptr() as *mut c_char,
            (size_of::<OffsetNumber>() * xlrec.nToPlaceholder as usize) as c_int,
        );

        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

        recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_VACUUM_REDIRECT);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();
}

/*
 * Process one page during a bulkdelete scan
 */
unsafe fn spgvacuumpage(bds: *mut spgBulkDeleteState, buffer: Buffer) {
    let index: Relation = (*(*bds).info).index;
    let blkno: BlockNumber = BufferGetBlockNumber(buffer);
    let page: Page;

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buffer);

    if PageIsNew(page) {
        /*
         * We found an all-zero page, which could happen if the database
         * crashed just after extending the file.  Recycle it.
         */
    } else if PageIsEmpty(page) {
        /* nothing to do */
    } else if SpGistPageIsLeaf(page) {
        if SpGistBlockIsRoot(blkno) {
            vacuumLeafRoot(bds, index, buffer);
            /* no need for vacuumRedirectAndPlaceholder */
        } else {
            vacuumLeafPage(bds, index, buffer, false);
            vacuumRedirectAndPlaceholder(index, (*(*bds).info).heaprel, buffer);
        }
    } else {
        /* inner page */
        vacuumRedirectAndPlaceholder(index, (*(*bds).info).heaprel, buffer);
    }

    /*
     * The root pages must never be deleted, nor marked as available in FSM,
     * because we don't want them ever returned by a search for a place to put
     * a new tuple.  Otherwise, check for empty page, and make sure the FSM
     * knows about it.
     */
    if !SpGistBlockIsRoot(blkno) {
        if PageIsNew(page) || PageIsEmpty(page) {
            RecordFreeIndexPage(index, blkno);
            (*(*bds).stats).pages_deleted += 1;
        } else {
            SpGistSetLastUsedPage(index, buffer);
            (*bds).lastFilledBlock = blkno;
        }
    }

    UnlockReleaseBuffer(buffer);
}

/*
 * Process the pending-TID list between pages of the main scan
 */
unsafe fn spgprocesspending(bds: *mut spgBulkDeleteState) {
    let index: Relation = (*(*bds).info).index;
    let mut pitem: *mut spgVacPendingItem;
    let mut nitem: *mut spgVacPendingItem;
    let blkno: BlockNumber;
    let buffer: Buffer;
    let page: Page;

    pitem = (*bds).pendingList;
    while !pitem.is_null() {
        if (*pitem).done {
            pitem = (*pitem).next;
            continue; /* ignore already-done items */
        }

        /* call vacuum_delay_point while not holding any buffer lock */
        vacuum_delay_point(false);

        /* examine the referenced page */
        let blkno = ItemPointerGetBlockNumber(&raw mut (*pitem).tid);
        let buffer = ReadBufferExtended(
            index,
            MAIN_FORKNUM,
            blkno,
            RBM_NORMAL,
            (*(*bds).info).strategy,
        );
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        let page: Page = BufferGetPage(buffer);

        if PageIsNew(page) || SpGistPageIsDeleted(page) {
            /* Probably shouldn't happen, but ignore it */
        } else if SpGistPageIsLeaf(page) {
            if SpGistBlockIsRoot(blkno) {
                /* this should definitely not happen */
                elog!(
                    ERROR,
                    "redirection leads to root page of index \"{}\"",
                    RelationGetRelationName(index)
                );
            }

            /* deal with any deletable tuples */
            vacuumLeafPage(bds, index, buffer, true);
            /* might as well do this while we are here */
            vacuumRedirectAndPlaceholder(index, (*(*bds).info).heaprel, buffer);

            SpGistSetLastUsedPage(index, buffer);

            /*
             * We can mark as done not only this item, but any later ones
             * pointing at the same page, since we vacuumed the whole page.
             */
            (*pitem).done = true;
            nitem = (*pitem).next;
            while !nitem.is_null() {
                if ItemPointerGetBlockNumber(&raw mut (*nitem).tid) == blkno {
                    (*nitem).done = true;
                }
                nitem = (*nitem).next;
            }
        } else {
            /*
             * On an inner page, visit the referenced inner tuple and add all
             * its downlinks to the pending list.  We might have pending items
             * for more than one inner tuple on the same page (in fact this is
             * pretty likely given the way space allocation works), so get
             * them all while we are here.
             */
            nitem = pitem;
            while !nitem.is_null() {
                if (*nitem).done {
                    nitem = (*nitem).next;
                    continue;
                }
                if ItemPointerGetBlockNumber(&raw mut (*nitem).tid) == blkno {
                    let offset: OffsetNumber = ItemPointerGetOffsetNumber(&raw mut (*nitem).tid);
                    let innerTuple: SpGistInnerTuple =
                        PageGetItem(page, PageGetItemId(page, offset)) as SpGistInnerTuple;
                    if IT_TUPSTATE(innerTuple) == SPGIST_LIVE {
                        let mut node: SpGistNodeTuple;
                        let mut ni: c_int;

                        /* SGITITERATE(innerTuple, ni, node) */
                        ni = 0;
                        node = SGITNODEPTR(innerTuple);
                        while ni < IT_NNODES(innerTuple) {
                            if ItemPointerIsValid(&raw mut (*node).t_tid) {
                                spgAddPendingTID(bds, &raw mut (*node).t_tid);
                            }
                            ni += 1;
                            node = (node as *mut c_char)
                                .add(IndexTupleSize(node) as usize)
                                as SpGistNodeTuple;
                        }
                    } else if IT_TUPSTATE(innerTuple) == SPGIST_REDIRECT {
                        /* transfer attention to redirect point */
                        spgAddPendingTID(
                            bds,
                            &mut (*(innerTuple as SpGistDeadTuple)).pointer,
                        );
                    } else {
                        elog!(
                            ERROR,
                            "unexpected SPGiST tuple state: {}",
                            IT_TUPSTATE(innerTuple)
                        );
                    }

                    (*nitem).done = true;
                }
                nitem = (*nitem).next;
            }
        }

        UnlockReleaseBuffer(buffer);

        pitem = (*pitem).next;
    }

    spgClearPendingList(bds);
}

/*
 * Perform a bulkdelete scan
 */
unsafe fn spgvacuumscan(bds: *mut spgBulkDeleteState) {
    let index: Relation = (*(*bds).info).index;
    let needLock: bool;
    let mut num_pages: BlockNumber;
    let mut p: BlockRangeReadStreamPrivate = std::mem::zeroed();
    let stream: *mut ReadStream;

    /* Finish setting up spgBulkDeleteState */
    initSpGistState(&mut (*bds).spgstate, index);
    (*bds).pendingList = ptr::null_mut();
    (*bds).myXmin = (*GetActiveSnapshot()).xmin;
    (*bds).lastFilledBlock = SPGIST_LAST_FIXED_BLKNO;

    /*
     * Reset counts that will be incremented during the scan; needed in case
     * of multiple scans during a single VACUUM command
     */
    (*(*bds).stats).estimated_count = false;
    (*(*bds).stats).num_index_tuples = 0.0;
    (*(*bds).stats).pages_deleted = 0;

    /* We can skip locking for new or temp relations */
    needLock = !RELATION_IS_LOCAL(index);
    p.current_blocknum = SPGIST_METAPAGE_BLKNO + 1;

    /*
     * It is safe to use batchmode as block_range_read_stream_cb takes no
     * locks.
     */
    stream = read_stream_begin_relation(
        READ_STREAM_MAINTENANCE | READ_STREAM_FULL | READ_STREAM_USE_BATCHING,
        (*(*bds).info).strategy,
        index,
        MAIN_FORKNUM,
        Some(block_range_read_stream_cb),
        &mut p as *mut _ as *mut c_void,
        0,
    );

    /*
     * The outer loop iterates over all index pages except the metapage, in
     * physical order (we hope the kernel will cooperate in providing
     * read-ahead for speed).  It is critical that we visit all leaf pages,
     * including ones added after we start the scan, else we might fail to
     * delete some deletable tuples.  See more extensive comments about this
     * in btvacuumscan().
     */
    loop {
        /* Get the current relation length */
        if needLock {
            LockRelationForExtension(index, ExclusiveLock);
        }
        num_pages = RelationGetNumberOfBlocks(index);
        if needLock {
            UnlockRelationForExtension(index, ExclusiveLock);
        }

        /* Quit if we've scanned the whole relation */
        if p.current_blocknum >= num_pages {
            break;
        }

        p.last_exclusive = num_pages;

        /* Iterate over pages, then loop back to recheck length */
        loop {
            /* call vacuum_delay_point while not holding any buffer lock */
            vacuum_delay_point(false);

            let buf: Buffer = read_stream_next_buffer(stream, ptr::null_mut());

            if !BufferIsValid(buf) {
                break;
            }

            spgvacuumpage(bds, buf);

            /* empty the pending-list after each page */
            if !(*bds).pendingList.is_null() {
                spgprocesspending(bds);
            }
        }

        /*
         * We have to reset the read stream to use it again. After returning
         * InvalidBuffer, the read stream API won't invoke our callback again
         * until the stream has been reset.
         */
        read_stream_reset(stream);
    }

    read_stream_end(stream);

    /* Propagate local lastUsedPages cache to metablock */
    SpGistUpdateMetaPage(index);

    /*
     * If we found any empty pages (and recorded them in the FSM), then
     * forcibly update the upper-level FSM pages to ensure that searchers can
     * find them.  It's possible that the pages were also found during
     * previous scans and so this is a waste of time, but it's cheap enough
     * relative to scanning the index that it shouldn't matter much, and
     * making sure that free pages are available sooner not later seems
     * worthwhile.
     *
     * Note that if no empty pages exist, we don't bother vacuuming the FSM at
     * all.
     */
    if (*(*bds).stats).pages_deleted > 0 {
        IndexFreeSpaceMapVacuum(index);
    }

    /*
     * Truncate index if possible
     *
     * XXX disabled because it's unsafe due to possible concurrent inserts.
     * We'd have to rescan the pages to make sure they're still empty, and it
     * doesn't seem worth it.  Note that btree doesn't do this either.
     *
     * Another reason not to truncate is that it could invalidate the cached
     * pages-with-freespace pointers in the metapage and other backends'
     * relation caches, that is leave them pointing to nonexistent pages.
     * Adding RelationGetNumberOfBlocks calls to protect the places that use
     * those pointers would be unduly expensive.
     */
    /* #ifdef NOT_USED ... #endif (truncation disabled) */

    /* Report final stats */
    (*(*bds).stats).num_pages = num_pages;
    (*(*bds).stats).pages_newly_deleted = (*(*bds).stats).pages_deleted;
    (*(*bds).stats).pages_free = (*(*bds).stats).pages_deleted;
}

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
pub unsafe fn spgbulkdelete(
    info: *mut IndexVacuumInfo,
    mut stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    let mut bds: spgBulkDeleteState = std::mem::zeroed();

    /* allocate stats if first time through, else re-use existing struct */
    if stats.is_null() {
        stats = palloc0(size_of::<IndexBulkDeleteResult>()) as *mut IndexBulkDeleteResult;
    }
    bds.info = info;
    bds.stats = stats;
    bds.callback = callback;
    bds.callback_state = callback_state;

    spgvacuumscan(&mut bds);

    stats
}

/* Dummy callback to delete no tuples during spgvacuumcleanup */
unsafe extern "C" fn dummy_callback(_itemptr: ItemPointer, _state: *mut c_void) -> bool {
    false
}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
pub unsafe fn spgvacuumcleanup(
    info: *mut IndexVacuumInfo,
    mut stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    let mut bds: spgBulkDeleteState = std::mem::zeroed();

    /* No-op in ANALYZE ONLY mode */
    if (*info).analyze_only {
        return stats;
    }

    /*
     * We don't need to scan the index if there was a preceding bulkdelete
     * pass.  Otherwise, make a pass that won't delete any live tuples, but
     * might still accomplish useful stuff with redirect/placeholder cleanup
     * and/or FSM housekeeping, and in any case will provide stats.
     */
    if stats.is_null() {
        stats = palloc0(size_of::<IndexBulkDeleteResult>()) as *mut IndexBulkDeleteResult;
        bds.info = info;
        bds.stats = stats;
        bds.callback = Some(dummy_callback);
        bds.callback_state = ptr::null_mut();

        spgvacuumscan(&mut bds);
    }

    /*
     * It's quite possible for us to be fooled by concurrent tuple moves into
     * double-counting some index tuples, so disbelieve any total that exceeds
     * the underlying heap's count ... if we know that accurately.  Otherwise
     * this might just make matters worse.
     */
    if !(*info).estimated_count {
        if (*stats).num_index_tuples > (*info).num_heap_tuples {
            (*stats).num_index_tuples = (*info).num_heap_tuples;
        }
    }

    stats
}

/* ------------------------------------------------------------------ */
/* STORE_STATE macro (access/spgist_private.h)                        */
/* ------------------------------------------------------------------ */
#[inline]
unsafe fn STORE_STATE(s: *mut SpGistState, d: *mut spgxlogState) {
    (*d).redirectXid = (*s).redirectXid;
    (*d).isBuild = (*s).isBuild;
}

/* ------------------------------------------------------------------ */
/* Local stubs for unported helpers                                   */
/* ------------------------------------------------------------------ */

unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn BufferIsValid(_buffer: Buffer) -> bool {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn ReadBufferExtended(
    _reln: Relation,
    _forkNum: c_int,
    _blockNum: BlockNumber,
    _mode: c_int,
    _strategy: *mut c_void,
) -> Buffer {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn PageGetItem(_page: Page, _itemId: ItemId) -> *mut c_void {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetItemId(_page: Page, _offsetNumber: OffsetNumber) -> ItemId {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetMaxOffsetNumber(_page: Page) -> OffsetNumber {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageIsNew(_page: Page) -> bool {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageIsEmpty(_page: Page) -> bool {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageIndexMultiDelete(_page: Page, _itemnos: *mut OffsetNumber, _nitems: c_int) {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn SpGistPageGetOpaque(_page: Page) -> SpGistPageOpaque {
    unimplemented!() // TODO: access/spgist_private.h
}
unsafe fn SpGistPageIsLeaf(_page: Page) -> bool {
    unimplemented!() // TODO: access/spgist_private.h
}
unsafe fn SpGistPageIsDeleted(_page: Page) -> bool {
    unimplemented!() // TODO: access/spgist_private.h
}
unsafe fn SGITNODEPTR(_innerTuple: SpGistInnerTuple) -> SpGistNodeTuple {
    unimplemented!() // TODO: access/spgist_private.h
}
unsafe fn IndexTupleSize(_itup: SpGistNodeTuple) -> Size {
    unimplemented!() // TODO: access/itup.h
}
unsafe fn RelationNeedsWAL(_rel: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn RelationGetRelationName(_rel: Relation) -> *const c_char {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn RelationGetNumberOfBlocks(_rel: Relation) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn RelationIsAccessibleInLogicalDecoding(_rel: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn RELATION_IS_LOCAL(_rel: Relation) -> bool {
    unimplemented!() // TODO: access/spgist_private.h
}
unsafe fn LockRelationForExtension(_rel: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: storage/lmgr.h
}
unsafe fn UnlockRelationForExtension(_rel: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: storage/lmgr.h
}
unsafe fn RecordFreeIndexPage(_rel: Relation, _freeBlock: BlockNumber) {
    unimplemented!() // TODO: storage/indexfsm.h
}
unsafe fn IndexFreeSpaceMapVacuum(_rel: Relation) {
    unimplemented!() // TODO: storage/indexfsm.h
}
unsafe fn vacuum_delay_point(_is_analyze: bool) {
    unimplemented!() // TODO: commands/vacuum.h
}
unsafe fn GetActiveSnapshot() -> Snapshot {
    unimplemented!() // TODO: utils/snapmgr.h
}
unsafe fn GlobalVisTestFor(_rel: Relation) -> *mut GlobalVisState {
    unimplemented!() // TODO: utils/snapmgr.h
}
unsafe fn GlobalVisTestIsRemovableXid(_state: *mut GlobalVisState, _xid: TransactionId) -> bool {
    unimplemented!() // TODO: utils/snapmgr.h
}
unsafe fn ItemPointerEquals(_pointer1: ItemPointer, _pointer2: ItemPointer) -> bool {
    unimplemented!() // TODO: storage/itemptr.h
}
unsafe fn ItemPointerIsValid(_pointer: ItemPointer) -> bool {
    unimplemented!() // TODO: storage/itemptr.h
}
unsafe fn ItemPointerSetInvalid(_pointer: ItemPointer) {
    unimplemented!() // TODO: storage/itemptr.h
}
unsafe fn ItemPointerGetBlockNumber(_pointer: ItemPointer) -> BlockNumber {
    unimplemented!() // TODO: storage/itemptr.h
}
unsafe fn ItemPointerGetOffsetNumber(_pointer: ItemPointer) -> OffsetNumber {
    unimplemented!() // TODO: storage/itemptr.h
}
unsafe fn TransactionIdIsValid(_xid: TransactionId) -> bool {
    unimplemented!() // TODO: access/transam.h
}
unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterData(_data: *mut c_char, _len: c_int) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterBuffer(_block_id: uint8, _buffer: Buffer, _flags: c_int) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogInsert(_rmid: RmgrId, _info: uint8) -> XLogRecPtr {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn read_stream_begin_relation(
    _flags: c_int,
    _strategy: *mut c_void,
    _rel: Relation,
    _forknum: c_int,
    _callback: ReadStreamBlockNumberCB,
    _callback_private_data: *mut c_void,
    _per_buffer_data_size: Size,
) -> *mut ReadStream {
    unimplemented!() // TODO: storage/read_stream.h
}
unsafe fn read_stream_next_buffer(
    _stream: *mut ReadStream,
    _per_buffer_data: *mut c_void,
) -> Buffer {
    unimplemented!() // TODO: storage/read_stream.h
}
unsafe fn read_stream_reset(_stream: *mut ReadStream) {
    unimplemented!() // TODO: storage/read_stream.h
}
unsafe fn read_stream_end(_stream: *mut ReadStream) {
    unimplemented!() // TODO: storage/read_stream.h
}
unsafe extern "C" fn block_range_read_stream_cb(
    _stream: *mut ReadStream,
    _callback_private_data: *mut c_void,
    _per_buffer_data: *mut c_void,
) -> BlockNumber {
    unimplemented!() // TODO: storage/read_stream.h
}
