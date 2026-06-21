//! spgdoinsert.c
//!   implementation of insert algorithm
//!
//! Translated 1:1 from postgres/src/backend/access/spgist/spgdoinsert.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!         src/backend/access/spgist/spgdoinsert.c
//!
//! #include mapping:
//!   "postgres.h"                 -> crate::prelude::*
//!   "access/genam.h"             -> (IndexOrderByDistance etc.; unused symbols here)
//!   "access/spgist_private.h"    -> crate::access::spgist::spgist_private + spgist
//!   "access/spgxlog.h"           -> crate::access::rmgrdesc::spgdesc (WAL structs/opcodes)
//!   "access/xloginsert.h"        -> crate::access::transam::xloginsert (XLog* + REGBUF_*)
//!   "common/int.h"               -> crate::common::int::pg_cmp_u16
//!   "common/pg_prng.h"           -> crate::common::pg_prng
//!   "miscadmin.h"                -> crate::miscadmin (interrupt + crit-section helpers)
//!   "storage/bufmgr.h"           -> buffer routines (STUB below)
//!   "utils/rel.h"                -> crate::utils::rel (Relation, RelationGetRelationName)

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;

use crate::{ereport, errmsg, elog, Assert, PG_DETOAST_DATUM};

use core::mem::size_of;
use core::ffi::CStr;

// --- spgist_private.h (REAL) -------------------------------------------------
use crate::access::spgist::spgist_private::{
    spgDeformLeafTuple, spgExtractNodeLabels, spgFormDeadTuple, spgFormInnerTuple,
    spgFormLeafTuple, spgFormNodeTuple, spgKeyColumn, spgFirstIncludeColumn,
    SpGistGetLeafTupleSize, SpGistGetBuffer, SpGistInitBuffer, SpGistPageAddNewItem,
    SpGistSetLastUsedPage, SGLT_GET_NEXTOFFSET, SGLT_SET_NEXTOFFSET, SpGistBlockIsRoot,
    SpGistDeadTuple, SpGistDeadTupleData, SpGistInnerTuple, SpGistInnerTupleData,
    SpGistLeafTuple, SpGistLeafTupleData, SpGistNodeTuple, SpGistNodeTupleData, SpGistState,
    MaxIndexTuplesPerPage, SGITMAXNNODES, GBUF_INNER_PARITY, GBUF_LEAF, GBUF_NULLS,
    SPGIST_LEAF, SPGIST_NULLS, SPGIST_LIVE, SPGIST_DEAD, SPGIST_REDIRECT, SPGIST_PLACEHOLDER,
    SPGIST_METAPAGE_BLKNO, SPGIST_ROOT_BLKNO, SPGIST_NULL_BLKNO,
};

// --- access/spgist.h support function numbers + choose/picksplit structs ----
use crate::access::spgist::spgist::{
    spgChooseIn, spgChooseOut, spgPickSplitIn, spgPickSplitOut, spgMatchNode, spgAddNode,
    spgSplitTuple, SPGIST_CHOOSE_PROC, SPGIST_PICKSPLIT_PROC, SPGIST_COMPRESS_PROC,
};

// --- access/spgxlog.h WAL record structs + opcodes (rmgrdesc/spgdesc) -------
use crate::access::rmgrdesc::spgdesc::{
    spgxlogAddLeaf, spgxlogAddNode, spgxlogMoveLeafs, spgxlogPickSplit, spgxlogSplitTuple,
    spgxlogState, SizeOfSpgxlogMoveLeafs, SizeOfSpgxlogPickSplit, XLOG_SPGIST_ADD_LEAF,
    XLOG_SPGIST_ADD_NODE, XLOG_SPGIST_MOVE_LEAFS, XLOG_SPGIST_PICKSPLIT, XLOG_SPGIST_SPLIT_TUPLE,
};

// --- WAL insertion (access/xloginsert.h) (REAL) -----------------------------
use crate::access::transam::xloginsert::{
    XLogBeginInsert, XLogInsert, XLogRegisterBuffer, XLogRegisterData, REGBUF_STANDARD,
    REGBUF_WILL_INIT,
};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::access::rmgrlist::RM_SPGIST_ID;

// --- common/int.h, common/pg_prng.h (REAL) ----------------------------------
use crate::common::int::pg_cmp_u16;
use crate::common::pg_prng::{pg_global_prng_state, pg_prng_uint64_range};

// --- miscadmin.h (REAL) -----------------------------------------------------
use crate::miscadmin::{
    CHECK_FOR_INTERRUPTS, END_CRIT_SECTION, INTERRUPTS_CAN_BE_PROCESSED,
    INTERRUPTS_PENDING_CONDITION, START_CRIT_SECTION,
};

// --- bufpage page accessors (REAL) ------------------------------------------
use crate::storage::bufpage::{
    Page, PageAddItem, PageGetExactFreeSpace, PageGetItem, PageGetItemId,
    PageGetMaxOffsetNumber, PageIndexMultiDelete, PageIndexTupleDelete, PageSetLSN,
};
use crate::storage::item::Item;
use crate::storage::itemid::ItemIdData;
use crate::storage::itemptr::{
    ItemPointer, ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
    ItemPointerIsValid, ItemPointerSet,
};

// --- block / offset / buffer (REAL) -----------------------------------------
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::storage::buf::{Buffer, InvalidBuffer};
use crate::storage::off::{FirstOffsetNumber, InvalidOffsetNumber, OffsetNumber};

// --- tupdesc helpers (REAL) --------------------------------------------------
use crate::access::common::tupdesc::{TupleDesc, TupleDescCompactAttr};

// --- fmgr (REAL) -------------------------------------------------------------
use crate::utils::fmgr::{FmgrInfo, FunctionCall1Coll, FunctionCall2Coll};

// --- relcache (REAL) ---------------------------------------------------------
use crate::utils::rel::{Relation, RelationGetRelationName};

// --- index AM accessors (REAL) ----------------------------------------------
use crate::access::index::indexam::{index_getprocid, index_getprocinfo};

// ===========================================================================
// Local mirrors of access/spgist_private.h macros that are not exported by the
// sibling Rust module yet (the GREEN siblings each keep their own copies).
// TODO(pg-port): dedup once spgist_private.rs exports them.
// ===========================================================================

// SPGIST_PAGE_CAPACITY = BLCKSZ - SizeOfPageHeaderData - MAXALIGN(special).
const SPGIST_PAGE_CAPACITY: c_int = 8192;

// SGDTSIZE = MAXALIGN(sizeof(SpGistDeadTupleData)).
const SGDTSIZE: Size = MAXALIGN(size_of::<SpGistDeadTupleData>());

// INDEX_MAX_KEYS (pg_config_manual.h).
const INDEX_MAX_KEYS: usize = 32;

// BLCKSZ (pg_config.h).
const BLCKSZ: c_int = 8192;

// STORE_STATE(s, d): copy redirectXid + isBuild into a spgxlogState.
#[inline]
unsafe fn STORE_STATE(state: *mut SpGistState, dst: *mut spgxlogState) {
    (*dst).redirectXid = (*state).redirectXid;
    (*dst).isBuild = (*state).isBuild;
}

// SGITMAXSIZE-related and packed bit-field accessors.
// SGIT nNodes accessor: tupstate:2, allTheSame:1, nNodes:13, prefixSize:16.
#[inline]
unsafe fn SGITGetNNodes(tup: SpGistInnerTuple) -> c_int {
    (((*tup).bits_ >> 3) & 0x1FFF) as c_int
}
#[inline]
unsafe fn SGITGetPrefixSize(tup: SpGistInnerTuple) -> c_int {
    (((*tup).bits_ >> 16) & 0xFFFF) as c_int
}
#[inline]
unsafe fn SGITGetAllTheSame(tup: SpGistInnerTuple) -> bool {
    (((*tup).bits_ >> 2) & 0x1) != 0
}
#[inline]
unsafe fn SGITSetAllTheSame(tup: SpGistInnerTuple, v: bool) {
    (*tup).bits_ = ((*tup).bits_ & !(0x1u32 << 2)) | ((v as c_uint) << 2);
}
#[inline]
unsafe fn SGLTGetTupState(tup: SpGistLeafTuple) -> c_int {
    ((*tup).bits_ & 0x3) as c_int
}
#[inline]
unsafe fn SGLTGetSize(tup: SpGistLeafTuple) -> c_uint {
    (*tup).bits_ >> 2
}
#[inline]
unsafe fn SGDTGetTupState(tup: SpGistDeadTuple) -> c_int {
    ((*tup).bits_ & 0x3) as c_int
}
#[inline]
unsafe fn SGDTGetSize(tup: SpGistDeadTuple) -> c_uint {
    (*tup).bits_ >> 2
}

// SGITITERATE(x, i, nt): iterate over an inner tuple's node tuples.  In C this
// is a for-loop macro; here we provide the per-step primitives used to expand
// it inline (matching how spgvacuum.rs/spgutils.rs expand it).
#[inline]
unsafe fn SGITNODEPTR(tup: SpGistInnerTuple) -> SpGistNodeTuple {
    // SGITDATAPTR(tup) + MAXALIGN(prefixSize); SGITDATAPTR = (char*)x + SGITHDRSZ.
    let hdr = MAXALIGN(size_of::<SpGistInnerTupleData>());
    let base = (tup as *mut c_char).add(hdr);
    base.add(MAXALIGN((*tup).prefixSize() as usize)) as SpGistNodeTuple
}

// SGITDATUM(x, s): the prefix datum of an inner tuple.
unsafe fn SGITDATUM(tup: SpGistInnerTuple, state: *mut SpGistState) -> Datum {
    unimplemented!() // TODO(pg-port): access/spgist_private.h (SGITDATUM)
}

// SGLTDATUM(x, s): the key datum of a leaf tuple.
#[inline]
unsafe fn SGLTDATUM(tup: SpGistLeafTuple, state: *mut SpGistState) -> Datum {
    unimplemented!() // TODO(pg-port): access/spgist_private.h (SGLTDATUM)
}

// ---- helper trait-ish accessors for packed inner-tuple fields --------------
impl SpGistInnerTupleData {
    #[inline]
    unsafe fn prefixSize(&self) -> c_int {
        ((self.bits_ >> 16) & 0xFFFF) as c_int
    }
    #[inline]
    unsafe fn nNodes(&self) -> c_int {
        ((self.bits_ >> 3) & 0x1FFF) as c_int
    }
    #[inline]
    unsafe fn allTheSame(&self) -> bool {
        ((self.bits_ >> 2) & 0x1) != 0
    }
}

/*
 * SPPageDesc tracks all info about a page we are inserting into.  In some
 * situations it actually identifies a tuple, or even a specific node within
 * an inner tuple.  But any of the fields can be invalid.  If the buffer
 * field is valid, it implies we hold pin and exclusive lock on that buffer.
 * page pointer should be valid exactly when buffer is.
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct SPPageDesc {
    blkno: BlockNumber,   /* block number, or InvalidBlockNumber */
    buffer: Buffer,       /* page's buffer number, or InvalidBuffer */
    page: Page,           /* pointer to page buffer, or NULL */
    offnum: OffsetNumber, /* offset of tuple, or InvalidOffsetNumber */
    node: c_int,          /* node number within inner tuple, or -1 */
}

/*
 * Set the item pointer in the nodeN'th entry in inner tuple tup.  This
 * is used to update the parent inner tuple's downlink after a move or
 * split operation.
 */
pub unsafe fn spgUpdateNodeLink(
    tup: SpGistInnerTuple,
    nodeN: c_int,
    blkno: BlockNumber,
    offset: OffsetNumber,
) {
    let mut i: c_int;
    let mut node: SpGistNodeTuple;

    // SGITITERATE(tup, i, node)
    i = 0;
    node = SGITNODEPTR(tup);
    while i < (*tup).nNodes() {
        if i == nodeN {
            ItemPointerSet(&mut (*node).t_tid, blkno, offset);
            return;
        }
        i += 1;
        node = (node as *mut c_char).add(IndexTupleSize(node) as usize) as SpGistNodeTuple;
    }

    elog!(
        ERROR,
        "failed to find requested node {} in SPGiST inner tuple",
        nodeN
    );
}

/*
 * Form a new inner tuple containing one more node than the given one, with
 * the specified label datum, inserted at offset "offset" in the node array.
 * The new tuple's prefix is the same as the old one's.
 *
 * Note that the new node initially has an invalid downlink.  We'll find a
 * page to point it to later.
 */
unsafe fn addNode(
    state: *mut SpGistState,
    tuple: SpGistInnerTuple,
    label: Datum,
    mut offset: c_int,
) -> SpGistInnerTuple {
    let mut node: SpGistNodeTuple;
    let nodes: *mut SpGistNodeTuple;
    let mut i: c_int;

    /* if offset is negative, insert at end */
    if offset < 0 {
        offset = (*tuple).nNodes();
    } else if offset > (*tuple).nNodes() {
        elog!(ERROR, "invalid offset for adding node to SPGiST inner tuple");
    }

    nodes = palloc((size_of::<SpGistNodeTuple>() * ((*tuple).nNodes() as usize + 1)) as Size)
        as *mut SpGistNodeTuple;
    // SGITITERATE(tuple, i, node)
    i = 0;
    node = SGITNODEPTR(tuple);
    while i < (*tuple).nNodes() {
        if i < offset {
            *nodes.add(i as usize) = node;
        } else {
            *nodes.add((i + 1) as usize) = node;
        }
        i += 1;
        node = (node as *mut c_char).add(IndexTupleSize(node) as usize) as SpGistNodeTuple;
    }

    *nodes.add(offset as usize) = spgFormNodeTuple(state, label, false);

    spgFormInnerTuple(
        state,
        (*tuple).prefixSize() > 0,
        SGITDATUM(tuple, state),
        (*tuple).nNodes() + 1,
        nodes,
    )
}

/* qsort comparator for sorting OffsetNumbers */
unsafe fn cmpOffsetNumbers(a: *const c_void, b: *const c_void) -> c_int {
    pg_cmp_u16(*(a as *const OffsetNumber), *(b as *const OffsetNumber))
}

/*
 * Delete multiple tuples from an index page, preserving tuple offset numbers.
 *
 * The first tuple in the given list is replaced with a dead tuple of type
 * "firststate" (REDIRECT/DEAD/PLACEHOLDER); the remaining tuples are replaced
 * with dead tuples of type "reststate".  If either firststate or reststate
 * is REDIRECT, blkno/offnum specify where to link to.
 *
 * NB: this is used during WAL replay, so beware of trying to make it too
 * smart.  In particular, it shouldn't use "state" except for calling
 * spgFormDeadTuple().  This is also used in a critical section, so no
 * pallocs either!
 */
pub unsafe fn spgPageIndexMultiDelete(
    state: *mut SpGistState,
    page: Page,
    itemnos: *mut OffsetNumber,
    nitems: c_int,
    firststate: c_int,
    reststate: c_int,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) {
    let firstItem: OffsetNumber;
    let mut sortednos: [OffsetNumber; MaxIndexTuplesPerPage] = [0; MaxIndexTuplesPerPage];
    let mut tuple: SpGistDeadTuple = core::ptr::null_mut();
    let mut i: c_int;

    if nitems == 0 {
        return; /* nothing to do */
    }

    /*
     * For efficiency we want to use PageIndexMultiDelete, which requires the
     * targets to be listed in sorted order, so we have to sort the itemnos
     * array.  (This also greatly simplifies the math for reinserting the
     * replacement tuples.)  However, we must not scribble on the caller's
     * array, so we have to make a copy.
     */
    core::ptr::copy_nonoverlapping(itemnos, sortednos.as_mut_ptr(), nitems as usize);
    if nitems > 1 {
        qsort(
            sortednos.as_mut_ptr() as *mut c_void,
            nitems as usize,
            size_of::<OffsetNumber>(),
            cmpOffsetNumbers,
        );
    }

    PageIndexMultiDelete(page, sortednos.as_mut_ptr(), nitems);

    firstItem = *itemnos.add(0);

    i = 0;
    while i < nitems {
        let itemno: OffsetNumber = sortednos[i as usize];
        let tupstate: c_int = if itemno == firstItem { firststate } else { reststate };

        if tuple.is_null() || SGDTGetTupState(tuple) != tupstate {
            tuple = spgFormDeadTuple(state, tupstate, blkno, offnum);
        }

        if PageAddItem(
            page,
            tuple as Item,
            SGDTGetSize(tuple) as Size,
            itemno,
            false,
            false,
        ) != itemno
        {
            elog!(
                ERROR,
                "failed to add item of size {} to SPGiST index page",
                SGDTGetSize(tuple)
            );
        }

        if tupstate == SPGIST_REDIRECT {
            (*SpGistPageGetOpaque(page)).nRedirection += 1;
        } else if tupstate == SPGIST_PLACEHOLDER {
            (*SpGistPageGetOpaque(page)).nPlaceholder += 1;
        }

        i += 1;
    }
}

/*
 * Update the parent inner tuple's downlink, and mark the parent buffer
 * dirty (this must be the last change to the parent page in the current
 * WAL action).
 */
unsafe fn saveNodeLink(
    index: Relation,
    parent: *mut SPPageDesc,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) {
    let innerTuple: SpGistInnerTuple;

    innerTuple = PageGetItem(
        (*parent).page,
        PageGetItemId((*parent).page, (*parent).offnum),
    ) as SpGistInnerTuple;

    spgUpdateNodeLink(innerTuple, (*parent).node, blkno, offnum);

    MarkBufferDirty((*parent).buffer);
}

/*
 * Add a leaf tuple to a leaf page where there is known to be room for it
 */
unsafe fn addLeafTuple(
    index: Relation,
    state: *mut SpGistState,
    leafTuple: SpGistLeafTuple,
    current: *mut SPPageDesc,
    parent: *mut SPPageDesc,
    isNulls: bool,
    isNew: bool,
) {
    let mut xlrec: spgxlogAddLeaf = core::mem::zeroed();

    xlrec.newPage = isNew;
    xlrec.storesNulls = isNulls;

    /* these will be filled below as needed */
    xlrec.offnumLeaf = InvalidOffsetNumber;
    xlrec.offnumHeadLeaf = InvalidOffsetNumber;
    xlrec.offnumParent = InvalidOffsetNumber;
    xlrec.nodeI = 0;

    START_CRIT_SECTION();

    if (*current).offnum == InvalidOffsetNumber || SpGistBlockIsRoot((*current).blkno) {
        /* Tuple is not part of a chain */
        SGLT_SET_NEXTOFFSET(leafTuple, InvalidOffsetNumber);
        (*current).offnum = SpGistPageAddNewItem(
            state,
            (*current).page,
            leafTuple as Item,
            SGLTGetSize(leafTuple) as Size,
            core::ptr::null_mut(),
            false,
        );

        xlrec.offnumLeaf = (*current).offnum;

        /* Must update parent's downlink if any */
        if (*parent).buffer != InvalidBuffer {
            xlrec.offnumParent = (*parent).offnum;
            xlrec.nodeI = (*parent).node as uint16;

            saveNodeLink(index, parent, (*current).blkno, (*current).offnum);
        }
    } else {
        /*
         * Tuple must be inserted into existing chain.  We mustn't change the
         * chain's head address, but we don't need to chase the entire chain
         * to put the tuple at the end; we can insert it second.
         *
         * Also, it's possible that the "chain" consists only of a DEAD tuple,
         * in which case we should replace the DEAD tuple in-place.
         */
        let mut head: SpGistLeafTuple;
        let offnum: OffsetNumber;

        head = PageGetItem(
            (*current).page,
            PageGetItemId((*current).page, (*current).offnum),
        ) as SpGistLeafTuple;
        if SGLTGetTupState(head) == SPGIST_LIVE {
            SGLT_SET_NEXTOFFSET(leafTuple, SGLT_GET_NEXTOFFSET(head));
            offnum = SpGistPageAddNewItem(
                state,
                (*current).page,
                leafTuple as Item,
                SGLTGetSize(leafTuple) as Size,
                core::ptr::null_mut(),
                false,
            );

            /*
             * re-get head of list because it could have been moved on page,
             * and set new second element
             */
            head = PageGetItem(
                (*current).page,
                PageGetItemId((*current).page, (*current).offnum),
            ) as SpGistLeafTuple;
            SGLT_SET_NEXTOFFSET(head, offnum);

            xlrec.offnumLeaf = offnum;
            xlrec.offnumHeadLeaf = (*current).offnum;
        } else if SGLTGetTupState(head) == SPGIST_DEAD {
            SGLT_SET_NEXTOFFSET(leafTuple, InvalidOffsetNumber);
            PageIndexTupleDelete((*current).page, (*current).offnum);
            if PageAddItem(
                (*current).page,
                leafTuple as Item,
                SGLTGetSize(leafTuple) as Size,
                (*current).offnum,
                false,
                false,
            ) != (*current).offnum
            {
                elog!(
                    ERROR,
                    "failed to add item of size {} to SPGiST index page",
                    SGLTGetSize(leafTuple)
                );
            }

            /* WAL replay distinguishes this case by equal offnums */
            xlrec.offnumLeaf = (*current).offnum;
            xlrec.offnumHeadLeaf = (*current).offnum;
        } else {
            elog!(
                ERROR,
                "unexpected SPGiST tuple state: {}",
                SGLTGetTupState(head)
            );
        }
    }

    MarkBufferDirty((*current).buffer);

    if RelationNeedsWAL(index) && !(*state).isBuild {
        let recptr: XLogRecPtr;
        let mut flags: c_int;

        XLogBeginInsert();
        XLogRegisterData(
            &xlrec as *const _ as *const c_void,
            size_of::<spgxlogAddLeaf>() as u32,
        );
        XLogRegisterData(leafTuple as *const c_void, SGLTGetSize(leafTuple));

        flags = REGBUF_STANDARD as c_int;
        if xlrec.newPage {
            flags |= REGBUF_WILL_INIT as c_int;
        }
        XLogRegisterBuffer(0, (*current).buffer, flags as u8);
        if xlrec.offnumParent != InvalidOffsetNumber {
            XLogRegisterBuffer(1, (*parent).buffer, REGBUF_STANDARD);
        }

        recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_ADD_LEAF);

        PageSetLSN((*current).page, recptr);

        /* update parent only if we actually changed it */
        if xlrec.offnumParent != InvalidOffsetNumber {
            PageSetLSN((*parent).page, recptr);
        }
    }

    END_CRIT_SECTION();
}

/*
 * Count the number and total size of leaf tuples in the chain starting at
 * current->offnum.  Return number into *nToSplit and total size as function
 * result.
 *
 * Klugy special case when considering the root page (i.e., root is a leaf
 * page, but we're about to split for the first time): return fake large
 * values to force spgdoinsert() to take the doPickSplit rather than
 * moveLeafs code path.  moveLeafs is not prepared to deal with root page.
 */
unsafe fn checkSplitConditions(
    index: Relation,
    state: *mut SpGistState,
    current: *mut SPPageDesc,
    nToSplit: *mut c_int,
) -> c_int {
    let mut i: c_int;
    let mut n: c_int = 0;
    let mut totalSize: c_int = 0;

    if SpGistBlockIsRoot((*current).blkno) {
        /* return impossible values to force split */
        *nToSplit = BLCKSZ;
        return BLCKSZ;
    }

    i = (*current).offnum as c_int;
    while i != InvalidOffsetNumber as c_int {
        let it: SpGistLeafTuple;

        Assert!(
            i >= FirstOffsetNumber as c_int
                && i <= PageGetMaxOffsetNumber((*current).page) as c_int
        );
        it = PageGetItem((*current).page, PageGetItemId((*current).page, i as OffsetNumber))
            as SpGistLeafTuple;
        if SGLTGetTupState(it) == SPGIST_LIVE {
            n += 1;
            totalSize += SGLTGetSize(it) as c_int + size_of::<ItemIdData>() as c_int;
        } else if SGLTGetTupState(it) == SPGIST_DEAD {
            /* We could see a DEAD tuple as first/only chain item */
            Assert!(i == (*current).offnum as c_int);
            Assert!(SGLT_GET_NEXTOFFSET(it) == InvalidOffsetNumber);
            /* Don't count it in result, because it won't go to other page */
        } else {
            elog!(ERROR, "unexpected SPGiST tuple state: {}", SGLTGetTupState(it));
        }

        i = SGLT_GET_NEXTOFFSET(it) as c_int;
    }

    *nToSplit = n;

    totalSize
}

/*
 * current points to a leaf-tuple chain that we wanted to add newLeafTuple to,
 * but the chain has to be moved because there's not enough room to add
 * newLeafTuple to its page.  We use this method when the chain contains
 * very little data so a split would be inefficient.  We are sure we can
 * fit the chain plus newLeafTuple on one other page.
 */
unsafe fn moveLeafs(
    index: Relation,
    state: *mut SpGistState,
    current: *mut SPPageDesc,
    parent: *mut SPPageDesc,
    newLeafTuple: SpGistLeafTuple,
    isNulls: bool,
) {
    let mut i: c_int;
    let mut nDelete: c_int;
    let mut nInsert: c_int;
    let mut size: c_int;
    let nbuf: Buffer;
    let npage: Page;
    let mut r: OffsetNumber = InvalidOffsetNumber;
    let mut startOffset: OffsetNumber = InvalidOffsetNumber;
    let mut replaceDead: bool = false;
    let toDelete: *mut OffsetNumber;
    let toInsert: *mut OffsetNumber;
    let nblkno: BlockNumber;
    let mut xlrec: spgxlogMoveLeafs = core::mem::zeroed();
    let leafdata: *mut c_char;
    let mut leafptr: *mut c_char;

    /* This doesn't work on root page */
    Assert!((*parent).buffer != InvalidBuffer);
    Assert!((*parent).buffer != (*current).buffer);

    /* Locate the tuples to be moved, and count up the space needed */
    i = PageGetMaxOffsetNumber((*current).page) as c_int;
    toDelete = palloc((size_of::<OffsetNumber>() * i as usize) as Size) as *mut OffsetNumber;
    toInsert = palloc((size_of::<OffsetNumber>() * (i as usize + 1)) as Size) as *mut OffsetNumber;

    size = SGLTGetSize(newLeafTuple) as c_int + size_of::<ItemIdData>() as c_int;

    nDelete = 0;
    i = (*current).offnum as c_int;
    while i != InvalidOffsetNumber as c_int {
        let it: SpGistLeafTuple;

        Assert!(
            i >= FirstOffsetNumber as c_int
                && i <= PageGetMaxOffsetNumber((*current).page) as c_int
        );
        it = PageGetItem((*current).page, PageGetItemId((*current).page, i as OffsetNumber))
            as SpGistLeafTuple;

        if SGLTGetTupState(it) == SPGIST_LIVE {
            *toDelete.add(nDelete as usize) = i as OffsetNumber;
            size += SGLTGetSize(it) as c_int + size_of::<ItemIdData>() as c_int;
            nDelete += 1;
        } else if SGLTGetTupState(it) == SPGIST_DEAD {
            /* We could see a DEAD tuple as first/only chain item */
            Assert!(i == (*current).offnum as c_int);
            Assert!(SGLT_GET_NEXTOFFSET(it) == InvalidOffsetNumber);
            /* We don't want to move it, so don't count it in size */
            *toDelete.add(nDelete as usize) = i as OffsetNumber;
            nDelete += 1;
            replaceDead = true;
        } else {
            elog!(ERROR, "unexpected SPGiST tuple state: {}", SGLTGetTupState(it));
        }

        i = SGLT_GET_NEXTOFFSET(it) as c_int;
    }

    /* Find a leaf page that will hold them */
    nbuf = SpGistGetBuffer(
        index,
        GBUF_LEAF | (if isNulls { GBUF_NULLS } else { 0 }),
        size,
        &mut xlrec.newPage,
    );
    npage = BufferGetPage(nbuf);
    nblkno = BufferGetBlockNumber(nbuf);
    Assert!(nblkno != (*current).blkno);

    leafdata = palloc(size as Size) as *mut c_char;
    leafptr = leafdata;

    START_CRIT_SECTION();

    /* copy all the old tuples to new page, unless they're dead */
    nInsert = 0;
    if !replaceDead {
        i = 0;
        while i < nDelete {
            let it: SpGistLeafTuple;

            it = PageGetItem(
                (*current).page,
                PageGetItemId((*current).page, *toDelete.add(i as usize)),
            ) as SpGistLeafTuple;
            Assert!(SGLTGetTupState(it) == SPGIST_LIVE);

            /*
             * Update chain link (notice the chain order gets reversed, but we
             * don't care).  We're modifying the tuple on the source page
             * here, but it's okay since we're about to delete it.
             */
            SGLT_SET_NEXTOFFSET(it, r);

            r = SpGistPageAddNewItem(
                state,
                npage,
                it as Item,
                SGLTGetSize(it) as Size,
                &mut startOffset,
                false,
            );

            *toInsert.add(nInsert as usize) = r;
            nInsert += 1;

            /* save modified tuple into leafdata as well */
            core::ptr::copy_nonoverlapping(it as *const c_char, leafptr, SGLTGetSize(it) as usize);
            leafptr = leafptr.add(SGLTGetSize(it) as usize);

            i += 1;
        }
    }

    /* add the new tuple as well */
    SGLT_SET_NEXTOFFSET(newLeafTuple, r);
    r = SpGistPageAddNewItem(
        state,
        npage,
        newLeafTuple as Item,
        SGLTGetSize(newLeafTuple) as Size,
        &mut startOffset,
        false,
    );
    *toInsert.add(nInsert as usize) = r;
    nInsert += 1;
    core::ptr::copy_nonoverlapping(
        newLeafTuple as *const c_char,
        leafptr,
        SGLTGetSize(newLeafTuple) as usize,
    );
    leafptr = leafptr.add(SGLTGetSize(newLeafTuple) as usize);

    /*
     * Now delete the old tuples, leaving a redirection pointer behind for the
     * first one, unless we're doing an index build; in which case there can't
     * be any concurrent scan so we need not provide a redirect.
     */
    spgPageIndexMultiDelete(
        state,
        (*current).page,
        toDelete,
        nDelete,
        if (*state).isBuild { SPGIST_PLACEHOLDER } else { SPGIST_REDIRECT },
        SPGIST_PLACEHOLDER,
        nblkno,
        r,
    );

    /* Update parent's downlink and mark parent page dirty */
    saveNodeLink(index, parent, nblkno, r);

    /* Mark the leaf pages too */
    MarkBufferDirty((*current).buffer);
    MarkBufferDirty(nbuf);

    if RelationNeedsWAL(index) && !(*state).isBuild {
        let recptr: XLogRecPtr;

        /* prepare WAL info */
        STORE_STATE(state, &mut xlrec.stateSrc);

        xlrec.nMoves = nDelete as uint16;
        xlrec.replaceDead = replaceDead;
        xlrec.storesNulls = isNulls;

        xlrec.offnumParent = (*parent).offnum;
        xlrec.nodeI = (*parent).node as uint16;

        XLogBeginInsert();
        XLogRegisterData(&xlrec as *const _ as *const c_void, SizeOfSpgxlogMoveLeafs as u32);
        XLogRegisterData(
            toDelete as *const c_void,
            (size_of::<OffsetNumber>() * nDelete as usize) as u32,
        );
        XLogRegisterData(
            toInsert as *const c_void,
            (size_of::<OffsetNumber>() * nInsert as usize) as u32,
        );
        XLogRegisterData(
            leafdata as *const c_void,
            leafptr.offset_from(leafdata) as u32,
        );

        XLogRegisterBuffer(0, (*current).buffer, REGBUF_STANDARD);
        XLogRegisterBuffer(
            1,
            nbuf,
            REGBUF_STANDARD | (if xlrec.newPage { REGBUF_WILL_INIT } else { 0 }),
        );
        XLogRegisterBuffer(2, (*parent).buffer, REGBUF_STANDARD);

        recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_MOVE_LEAFS);

        PageSetLSN((*current).page, recptr);
        PageSetLSN(npage, recptr);
        PageSetLSN((*parent).page, recptr);
    }

    END_CRIT_SECTION();

    /* Update local free-space cache and release new buffer */
    SpGistSetLastUsedPage(index, nbuf);
    UnlockReleaseBuffer(nbuf);
}

/*
 * Update previously-created redirection tuple with appropriate destination
 *
 * We use this when it's not convenient to know the destination first.
 * The tuple should have been made with the "impossible" destination of
 * the metapage.
 */
unsafe fn setRedirectionTuple(
    current: *mut SPPageDesc,
    position: OffsetNumber,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) {
    let dt: SpGistDeadTuple;

    dt = PageGetItem((*current).page, PageGetItemId((*current).page, position)) as SpGistDeadTuple;
    Assert!(SGDTGetTupState(dt) == SPGIST_REDIRECT);
    Assert!(ItemPointerGetBlockNumber(&(*dt).pointer) == SPGIST_METAPAGE_BLKNO);
    ItemPointerSet(&mut (*dt).pointer, blkno, offnum);
}

/*
 * Test to see if the user-defined picksplit function failed to do its job,
 * ie, it put all the leaf tuples into the same node.
 * If so, randomly divide the tuples into several nodes (all with the same
 * label) and return true to select allTheSame mode for this inner tuple.
 *
 * (This code is also used to forcibly select allTheSame mode for nulls.)
 *
 * If we know that the leaf tuples wouldn't all fit on one page, then we
 * exclude the last tuple (which is the incoming new tuple that forced a split)
 * from the check to see if more than one node is used.  The reason for this
 * is that if the existing tuples are put into only one chain, then even if
 * we move them all to an empty page, there would still not be room for the
 * new tuple, so we'd get into an infinite loop of picksplit attempts.
 * Forcing allTheSame mode dodges this problem by ensuring the old tuples will
 * be split across pages.  (Exercise for the reader: figure out why this
 * fixes the problem even when there is only one old tuple.)
 */
unsafe fn checkAllTheSame(
    in_: *mut spgPickSplitIn,
    out: *mut spgPickSplitOut,
    tooBig: bool,
    includeNew: *mut bool,
) -> bool {
    let theNode: c_int;
    let limit: c_int;
    let mut i: c_int;

    /* For the moment, assume we can include the new leaf tuple */
    *includeNew = true;

    /* If there's only the new leaf tuple, don't select allTheSame mode */
    if (*in_).nTuples <= 1 {
        return false;
    }

    /* If tuple set doesn't fit on one page, ignore the new tuple in test */
    limit = if tooBig { (*in_).nTuples - 1 } else { (*in_).nTuples };

    /* Check to see if more than one node is populated */
    theNode = *(*out).mapTuplesToNodes.add(0);
    i = 1;
    while i < limit {
        if *(*out).mapTuplesToNodes.add(i as usize) != theNode {
            return false;
        }
        i += 1;
    }

    /* Nope, so override the picksplit function's decisions */

    /* If the new tuple is in its own node, it can't be included in split */
    if tooBig && *(*out).mapTuplesToNodes.add(((*in_).nTuples - 1) as usize) != theNode {
        *includeNew = false;
    }

    (*out).nNodes = 8; /* arbitrary number of child nodes */

    /* Random assignment of tuples to nodes (note we include new tuple) */
    i = 0;
    while i < (*in_).nTuples {
        *(*out).mapTuplesToNodes.add(i as usize) = i % (*out).nNodes;
        i += 1;
    }

    /* The opclass may not use node labels, but if it does, duplicate 'em */
    if !(*out).nodeLabels.is_null() {
        let theLabel: Datum = *(*out).nodeLabels.add(theNode as usize);

        (*out).nodeLabels =
            palloc((size_of::<Datum>() * (*out).nNodes as usize) as Size) as *mut Datum;
        i = 0;
        while i < (*out).nNodes {
            *(*out).nodeLabels.add(i as usize) = theLabel;
            i += 1;
        }
    }

    /* We don't touch the prefix or the leaf tuple datum assignments */

    true
}

/*
 * current points to a leaf-tuple chain that we wanted to add newLeafTuple to,
 * but the chain has to be split because there's not enough room to add
 * newLeafTuple to its page.
 *
 * This function splits the leaf tuple set according to picksplit's rules,
 * creating one or more new chains that are spread across the current page
 * and an additional leaf page (we assume that two leaf pages will be
 * sufficient).  A new inner tuple is created, and the parent downlink
 * pointer is updated to point to that inner tuple instead of the leaf chain.
 *
 * On exit, current contains the address of the new inner tuple.
 *
 * Returns true if we successfully inserted newLeafTuple during this function,
 * false if caller still has to do it (meaning another picksplit operation is
 * probably needed).  Failure could occur if the picksplit result is fairly
 * unbalanced, or if newLeafTuple is just plain too big to fit on a page.
 * Because we force the picksplit result to be at least two chains, each
 * cycle will get rid of at least one leaf tuple from the chain, so the loop
 * will eventually terminate if lack of balance is the issue.  If the tuple
 * is too big, we assume that repeated picksplit operations will eventually
 * make it small enough by repeated prefix-stripping.  A broken opclass could
 * make this an infinite loop, though, so spgdoinsert() checks that the
 * leaf datums get smaller each time.
 */
unsafe fn doPickSplit(
    index: Relation,
    state: *mut SpGistState,
    current: *mut SPPageDesc,
    parent: *mut SPPageDesc,
    newLeafTuple: SpGistLeafTuple,
    level: c_int,
    isNulls: bool,
    isNew: bool,
) -> bool {
    let mut insertedNew: bool = false;
    let mut in_: spgPickSplitIn = core::mem::zeroed();
    let mut out: spgPickSplitOut = core::mem::zeroed();
    let procinfo: *mut FmgrInfo;
    let mut includeNew: bool = false;
    let mut i: c_int;
    let max: c_int;
    let mut n: c_int;
    let innerTuple: SpGistInnerTuple;
    let mut node: SpGistNodeTuple;
    let nodes: *mut SpGistNodeTuple;
    let mut newInnerBuffer: Buffer;
    let mut newLeafBuffer: Buffer;
    let leafPageSelect: *mut uint8;
    let leafSizes: *mut c_int;
    let toDelete: *mut OffsetNumber;
    let toInsert: *mut OffsetNumber;
    let mut redirectTuplePos: OffsetNumber = InvalidOffsetNumber;
    let mut startOffsets: [OffsetNumber; 2] = [0; 2];
    let oldLeafs: *mut SpGistLeafTuple;
    let newLeafs: *mut SpGistLeafTuple;
    let mut leafDatums: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let mut leafIsnulls: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let mut spaceToDelete: c_int;
    let currentFreeSpace: c_int;
    let mut totalLeafSizes: c_int;
    let allTheSame: bool;
    let mut xlrec: spgxlogPickSplit = core::mem::zeroed();
    let leafdata: *mut c_char;
    let mut leafptr: *mut c_char;
    let mut saveCurrent: SPPageDesc;
    let mut nToDelete: c_int;
    let mut nToInsert: c_int;
    let maxToInclude: c_int;

    in_.level = level;

    /*
     * Allocate per-leaf-tuple work arrays with max possible size
     */
    max = PageGetMaxOffsetNumber((*current).page) as c_int;
    n = max + 1;
    in_.datums = palloc((size_of::<Datum>() * n as usize) as Size) as *mut Datum;
    toDelete = palloc((size_of::<OffsetNumber>() * n as usize) as Size) as *mut OffsetNumber;
    toInsert = palloc((size_of::<OffsetNumber>() * n as usize) as Size) as *mut OffsetNumber;
    oldLeafs = palloc((size_of::<SpGistLeafTuple>() * n as usize) as Size) as *mut SpGistLeafTuple;
    newLeafs = palloc((size_of::<SpGistLeafTuple>() * n as usize) as Size) as *mut SpGistLeafTuple;
    leafPageSelect = palloc((size_of::<uint8>() * n as usize) as Size) as *mut uint8;

    STORE_STATE(state, &mut xlrec.stateSrc);

    /*
     * Form list of leaf tuples which will be distributed as split result;
     * also, count up the amount of space that will be freed from current.
     * (Note that in the non-root case, we won't actually delete the old
     * tuples, only replace them with redirects or placeholders.)
     */
    nToInsert = 0;
    nToDelete = 0;
    spaceToDelete = 0;
    if SpGistBlockIsRoot((*current).blkno) {
        /*
         * We are splitting the root (which up to now is also a leaf page).
         * Its tuples are not linked, so scan sequentially to get them all. We
         * ignore the original value of current->offnum.
         */
        i = FirstOffsetNumber as c_int;
        while i <= max {
            let it: SpGistLeafTuple;

            it = PageGetItem((*current).page, PageGetItemId((*current).page, i as OffsetNumber))
                as SpGistLeafTuple;
            if SGLTGetTupState(it) == SPGIST_LIVE {
                *in_.datums.add(nToInsert as usize) =
                    if isNulls { 0 as Datum } else { SGLTDATUM(it, state) };
                *oldLeafs.add(nToInsert as usize) = it;
                nToInsert += 1;
                *toDelete.add(nToDelete as usize) = i as OffsetNumber;
                nToDelete += 1;
                /* we will delete the tuple altogether, so count full space */
                spaceToDelete += SGLTGetSize(it) as c_int + size_of::<ItemIdData>() as c_int;
            } else {
                /* tuples on root should be live */
                elog!(ERROR, "unexpected SPGiST tuple state: {}", SGLTGetTupState(it));
            }
            i += 1;
        }
    } else {
        /* Normal case, just collect the leaf tuples in the chain */
        i = (*current).offnum as c_int;
        while i != InvalidOffsetNumber as c_int {
            let it: SpGistLeafTuple;

            Assert!(i >= FirstOffsetNumber as c_int && i <= max);
            it = PageGetItem((*current).page, PageGetItemId((*current).page, i as OffsetNumber))
                as SpGistLeafTuple;
            if SGLTGetTupState(it) == SPGIST_LIVE {
                *in_.datums.add(nToInsert as usize) =
                    if isNulls { 0 as Datum } else { SGLTDATUM(it, state) };
                *oldLeafs.add(nToInsert as usize) = it;
                nToInsert += 1;
                *toDelete.add(nToDelete as usize) = i as OffsetNumber;
                nToDelete += 1;
                /* we will not delete the tuple, only replace with dead */
                Assert!(SGLTGetSize(it) as Size >= SGDTSIZE);
                spaceToDelete += SGLTGetSize(it) as c_int - SGDTSIZE as c_int;
            } else if SGLTGetTupState(it) == SPGIST_DEAD {
                /* We could see a DEAD tuple as first/only chain item */
                Assert!(i == (*current).offnum as c_int);
                Assert!(SGLT_GET_NEXTOFFSET(it) == InvalidOffsetNumber);
                *toDelete.add(nToDelete as usize) = i as OffsetNumber;
                nToDelete += 1;
                /* replacing it with redirect will save no space */
            } else {
                elog!(ERROR, "unexpected SPGiST tuple state: {}", SGLTGetTupState(it));
            }

            i = SGLT_GET_NEXTOFFSET(it) as c_int;
        }
    }
    in_.nTuples = nToInsert;

    /*
     * We may not actually insert new tuple because another picksplit may be
     * necessary due to too large value, but we will try to allocate enough
     * space to include it; and in any case it has to be included in the input
     * for the picksplit function.  So don't increment nToInsert yet.
     */
    *in_.datums.add(in_.nTuples as usize) =
        if isNulls { 0 as Datum } else { SGLTDATUM(newLeafTuple, state) };
    *oldLeafs.add(in_.nTuples as usize) = newLeafTuple;
    in_.nTuples += 1;

    core::ptr::write_bytes(&mut out as *mut spgPickSplitOut, 0, 1);

    if !isNulls {
        /*
         * Perform split using user-defined method.
         */
        procinfo = index_getprocinfo(index, 1, SPGIST_PICKSPLIT_PROC as uint16);
        FunctionCall2Coll(
            procinfo,
            *(*index).rd_indcollation.add(0),
            PointerGetDatum(&in_ as *const _ as *const c_void),
            PointerGetDatum(&out as *const _ as *const c_void),
        );

        /*
         * Form new leaf tuples and count up the total space needed.
         */
        totalLeafSizes = 0;
        i = 0;
        while i < in_.nTuples {
            if (*(*state).leafTupDesc).natts > 1 {
                spgDeformLeafTuple(
                    *oldLeafs.add(i as usize),
                    (*state).leafTupDesc,
                    leafDatums.as_mut_ptr(),
                    leafIsnulls.as_mut_ptr(),
                    isNulls,
                );
            }

            leafDatums[spgKeyColumn as usize] = *out.leafTupleDatums.add(i as usize);
            leafIsnulls[spgKeyColumn as usize] = false;

            *newLeafs.add(i as usize) = spgFormLeafTuple(
                state,
                &mut (**oldLeafs.add(i as usize)).heapPtr,
                leafDatums.as_ptr(),
                leafIsnulls.as_ptr(),
            );
            totalLeafSizes +=
                SGLTGetSize(*newLeafs.add(i as usize)) as c_int + size_of::<ItemIdData>() as c_int;
            i += 1;
        }
    } else {
        /*
         * Perform dummy split that puts all tuples into one node.
         * checkAllTheSame will override this and force allTheSame mode.
         */
        out.hasPrefix = false;
        out.nNodes = 1;
        out.nodeLabels = core::ptr::null_mut();
        out.mapTuplesToNodes =
            palloc0((size_of::<c_int>() * in_.nTuples as usize) as Size) as *mut c_int;

        /*
         * Form new leaf tuples and count up the total space needed.
         */
        totalLeafSizes = 0;
        i = 0;
        while i < in_.nTuples {
            if (*(*state).leafTupDesc).natts > 1 {
                spgDeformLeafTuple(
                    *oldLeafs.add(i as usize),
                    (*state).leafTupDesc,
                    leafDatums.as_mut_ptr(),
                    leafIsnulls.as_mut_ptr(),
                    isNulls,
                );
            }

            /*
             * Nulls tree can contain only null key values.
             */
            leafDatums[spgKeyColumn as usize] = 0 as Datum;
            leafIsnulls[spgKeyColumn as usize] = true;

            *newLeafs.add(i as usize) = spgFormLeafTuple(
                state,
                &mut (**oldLeafs.add(i as usize)).heapPtr,
                leafDatums.as_ptr(),
                leafIsnulls.as_ptr(),
            );
            totalLeafSizes +=
                SGLTGetSize(*newLeafs.add(i as usize)) as c_int + size_of::<ItemIdData>() as c_int;
            i += 1;
        }
    }

    /*
     * Check to see if the picksplit function failed to separate the values,
     * ie, it put them all into the same child node.  If so, select allTheSame
     * mode and create a random split instead.  See comments for
     * checkAllTheSame as to why we need to know if the new leaf tuples could
     * fit on one page.
     */
    allTheSame = checkAllTheSame(
        &mut in_,
        &mut out,
        totalLeafSizes > SPGIST_PAGE_CAPACITY,
        &mut includeNew,
    );

    /*
     * If checkAllTheSame decided we must exclude the new tuple, don't
     * consider it any further.
     */
    if includeNew {
        maxToInclude = in_.nTuples;
    } else {
        maxToInclude = in_.nTuples - 1;
        totalLeafSizes -=
            SGLTGetSize(*newLeafs.add((in_.nTuples - 1) as usize)) as c_int
                + size_of::<ItemIdData>() as c_int;
    }

    /*
     * Allocate per-node work arrays.  Since checkAllTheSame could replace
     * out.nNodes with a value larger than the number of tuples on the input
     * page, we can't allocate these arrays before here.
     */
    nodes = palloc((size_of::<SpGistNodeTuple>() * out.nNodes as usize) as Size)
        as *mut SpGistNodeTuple;
    leafSizes = palloc0((size_of::<c_int>() * out.nNodes as usize) as Size) as *mut c_int;

    /*
     * Form nodes of inner tuple and inner tuple itself
     */
    i = 0;
    while i < out.nNodes {
        let mut label: Datum = 0 as Datum;
        let labelisnull: bool = out.nodeLabels.is_null();

        if !labelisnull {
            label = *out.nodeLabels.add(i as usize);
        }
        *nodes.add(i as usize) = spgFormNodeTuple(state, label, labelisnull);
        i += 1;
    }
    innerTuple = spgFormInnerTuple(state, out.hasPrefix, out.prefixDatum, out.nNodes, nodes);
    SGITSetAllTheSame(innerTuple, allTheSame);

    /*
     * Update nodes[] array to point into the newly formed innerTuple, so that
     * we can adjust their downlinks below.
     */
    // SGITITERATE(innerTuple, i, node)
    i = 0;
    node = SGITNODEPTR(innerTuple);
    while i < (*innerTuple).nNodes() {
        *nodes.add(i as usize) = node;
        i += 1;
        node = (node as *mut c_char).add(IndexTupleSize(node) as usize) as SpGistNodeTuple;
    }

    /*
     * Re-scan new leaf tuples and count up the space needed under each node.
     */
    i = 0;
    while i < maxToInclude {
        n = *out.mapTuplesToNodes.add(i as usize);
        if n < 0 || n >= out.nNodes {
            elog!(ERROR, "inconsistent result of SPGiST picksplit function");
        }
        *leafSizes.add(n as usize) +=
            SGLTGetSize(*newLeafs.add(i as usize)) as c_int + size_of::<ItemIdData>() as c_int;
        i += 1;
    }

    /*
     * To perform the split, we must insert a new inner tuple, which can't go
     * on a leaf page; and unless we are splitting the root page, we must then
     * update the parent tuple's downlink to point to the inner tuple.  If
     * there is room, we'll put the new inner tuple on the same page as the
     * parent tuple, otherwise we need another non-leaf buffer. But if the
     * parent page is the root, we can't add the new inner tuple there,
     * because the root page must have only one inner tuple.
     */
    xlrec.initInner = false;
    if (*parent).buffer != InvalidBuffer
        && !SpGistBlockIsRoot((*parent).blkno)
        && (SpGistPageGetFreeSpace((*parent).page, 1)
            >= (*innerTuple).size as c_int + size_of::<ItemIdData>() as c_int)
    {
        /* New inner tuple will fit on parent page */
        newInnerBuffer = (*parent).buffer;
    } else if (*parent).buffer != InvalidBuffer {
        /* Send tuple to page with next triple parity (see README) */
        newInnerBuffer = SpGistGetBuffer(
            index,
            GBUF_INNER_PARITY((*parent).blkno + 1) | (if isNulls { GBUF_NULLS } else { 0 }),
            (*innerTuple).size as c_int + size_of::<ItemIdData>() as c_int,
            &mut xlrec.initInner,
        );
    } else {
        /* Root page split ... inner tuple will go to root page */
        newInnerBuffer = InvalidBuffer;
    }

    /*
     * The new leaf tuples converted from the existing ones should require the
     * same or less space, and therefore should all fit onto one page
     * (although that's not necessarily the current page, since we can't
     * delete the old tuples but only replace them with placeholders).
     * However, the incoming new tuple might not also fit, in which case we
     * might need another picksplit cycle to reduce it some more.
     *
     * If there's not room to put everything back onto the current page, then
     * we decide on a per-node basis which tuples go to the new page. (We do
     * it like that because leaf tuple chains can't cross pages, so we must
     * place all leaf tuples belonging to the same parent node on the same
     * page.)
     *
     * If we are splitting the root page (turning it from a leaf page into an
     * inner page), then no leaf tuples can go back to the current page; they
     * must all go somewhere else.
     */
    if !SpGistBlockIsRoot((*current).blkno) {
        currentFreeSpace = PageGetExactFreeSpace((*current).page) as c_int + spaceToDelete;
    } else {
        currentFreeSpace = 0; /* prevent assigning any tuples to current */
    }

    xlrec.initDest = false;

    if totalLeafSizes <= currentFreeSpace {
        /* All the leaf tuples will fit on current page */
        newLeafBuffer = InvalidBuffer;
        /* mark new leaf tuple as included in insertions, if allowed */
        if includeNew {
            nToInsert += 1;
            insertedNew = true;
        }
        i = 0;
        while i < nToInsert {
            *leafPageSelect.add(i as usize) = 0; /* signifies current page */
            i += 1;
        }
    } else if in_.nTuples == 1 && totalLeafSizes > SPGIST_PAGE_CAPACITY {
        /*
         * We're trying to split up a long value by repeated suffixing, but
         * it's not going to fit yet.  Don't bother allocating a second leaf
         * buffer that we won't be able to use.
         */
        newLeafBuffer = InvalidBuffer;
        Assert!(includeNew);
        Assert!(nToInsert == 0);
    } else {
        /* We will need another leaf page */
        let nodePageSelect: *mut uint8;
        let mut curspace: c_int;
        let mut newspace: c_int;

        newLeafBuffer = SpGistGetBuffer(
            index,
            GBUF_LEAF | (if isNulls { GBUF_NULLS } else { 0 }),
            Min(totalLeafSizes, SPGIST_PAGE_CAPACITY),
            &mut xlrec.initDest,
        );

        /*
         * Attempt to assign node groups to the two pages.  We might fail to
         * do so, even if totalLeafSizes is less than the available space,
         * because we can't split a group across pages.
         */
        nodePageSelect = palloc((size_of::<uint8>() * out.nNodes as usize) as Size) as *mut uint8;

        curspace = currentFreeSpace;
        newspace = PageGetExactFreeSpace(BufferGetPage(newLeafBuffer)) as c_int;
        i = 0;
        while i < out.nNodes {
            if *leafSizes.add(i as usize) <= curspace {
                *nodePageSelect.add(i as usize) = 0; /* signifies current page */
                curspace -= *leafSizes.add(i as usize);
            } else {
                *nodePageSelect.add(i as usize) = 1; /* signifies new leaf page */
                newspace -= *leafSizes.add(i as usize);
            }
            i += 1;
        }
        if curspace >= 0 && newspace >= 0 {
            /* Successful assignment, so we can include the new leaf tuple */
            if includeNew {
                nToInsert += 1;
                insertedNew = true;
            }
        } else if includeNew {
            /* We must exclude the new leaf tuple from the split */
            let nodeOfNewTuple: c_int = *out.mapTuplesToNodes.add((in_.nTuples - 1) as usize);

            *leafSizes.add(nodeOfNewTuple as usize) -=
                SGLTGetSize(*newLeafs.add((in_.nTuples - 1) as usize)) as c_int
                    + size_of::<ItemIdData>() as c_int;

            /* Repeat the node assignment process --- should succeed now */
            curspace = currentFreeSpace;
            newspace = PageGetExactFreeSpace(BufferGetPage(newLeafBuffer)) as c_int;
            i = 0;
            while i < out.nNodes {
                if *leafSizes.add(i as usize) <= curspace {
                    *nodePageSelect.add(i as usize) = 0; /* signifies current page */
                    curspace -= *leafSizes.add(i as usize);
                } else {
                    *nodePageSelect.add(i as usize) = 1; /* signifies new leaf page */
                    newspace -= *leafSizes.add(i as usize);
                }
                i += 1;
            }
            if curspace < 0 || newspace < 0 {
                elog!(ERROR, "failed to divide leaf tuple groups across pages");
            }
        } else {
            /* oops, we already excluded new tuple ... should not get here */
            elog!(ERROR, "failed to divide leaf tuple groups across pages");
        }
        /* Expand the per-node assignments to be shown per leaf tuple */
        i = 0;
        while i < nToInsert {
            n = *out.mapTuplesToNodes.add(i as usize);
            *leafPageSelect.add(i as usize) = *nodePageSelect.add(n as usize);
            i += 1;
        }
    }

    /* Start preparing WAL record */
    xlrec.nDelete = 0;
    xlrec.initSrc = isNew;
    xlrec.storesNulls = isNulls;
    xlrec.isRootSplit = SpGistBlockIsRoot((*current).blkno);

    leafdata = palloc(totalLeafSizes as Size) as *mut c_char;
    leafptr = leafdata;

    /* Here we begin making the changes to the target pages */
    START_CRIT_SECTION();

    /*
     * Delete old leaf tuples from current buffer, except when we're splitting
     * the root; in that case there's no need because we'll re-init the page
     * below.  We do this first to make room for reinserting new leaf tuples.
     */
    if !SpGistBlockIsRoot((*current).blkno) {
        /*
         * Init buffer instead of deleting individual tuples, but only if
         * there aren't any other live tuples and only during build; otherwise
         * we need to set a redirection tuple for concurrent scans.
         */
        if (*state).isBuild
            && nToDelete + (*SpGistPageGetOpaque((*current).page)).nPlaceholder as c_int
                == PageGetMaxOffsetNumber((*current).page) as c_int
        {
            SpGistInitBuffer(
                (*current).buffer,
                (SPGIST_LEAF | (if isNulls { SPGIST_NULLS } else { 0 })) as uint16,
            );
            xlrec.initSrc = true;
        } else if isNew {
            /* don't expose the freshly init'd buffer as a backup block */
            Assert!(nToDelete == 0);
        } else {
            xlrec.nDelete = nToDelete as uint16;

            if !(*state).isBuild {
                /*
                 * Need to create redirect tuple (it will point to new inner
                 * tuple) but right now the new tuple's location is not known
                 * yet.  So, set the redirection pointer to "impossible" value
                 * and remember its position to update tuple later.
                 */
                if nToDelete > 0 {
                    redirectTuplePos = *toDelete.add(0);
                }
                spgPageIndexMultiDelete(
                    state,
                    (*current).page,
                    toDelete,
                    nToDelete,
                    SPGIST_REDIRECT,
                    SPGIST_PLACEHOLDER,
                    SPGIST_METAPAGE_BLKNO,
                    FirstOffsetNumber,
                );
            } else {
                /*
                 * During index build there is not concurrent searches, so we
                 * don't need to create redirection tuple.
                 */
                spgPageIndexMultiDelete(
                    state,
                    (*current).page,
                    toDelete,
                    nToDelete,
                    SPGIST_PLACEHOLDER,
                    SPGIST_PLACEHOLDER,
                    InvalidBlockNumber,
                    InvalidOffsetNumber,
                );
            }
        }
    }

    /*
     * Put leaf tuples on proper pages, and update downlinks in innerTuple's
     * nodes.
     */
    startOffsets[0] = InvalidOffsetNumber;
    startOffsets[1] = InvalidOffsetNumber;
    i = 0;
    while i < nToInsert {
        let it: SpGistLeafTuple = *newLeafs.add(i as usize);
        let leafBuffer: Buffer;
        let leafBlock: BlockNumber;
        let newoffset: OffsetNumber;

        /* Which page is it going to? */
        leafBuffer = if *leafPageSelect.add(i as usize) != 0 {
            newLeafBuffer
        } else {
            (*current).buffer
        };
        leafBlock = BufferGetBlockNumber(leafBuffer);

        /* Link tuple into correct chain for its node */
        n = *out.mapTuplesToNodes.add(i as usize);

        if ItemPointerIsValid(&(**nodes.add(n as usize)).t_tid) {
            Assert!(
                ItemPointerGetBlockNumber(&(**nodes.add(n as usize)).t_tid) == leafBlock
            );
            SGLT_SET_NEXTOFFSET(
                it,
                ItemPointerGetOffsetNumber(&(**nodes.add(n as usize)).t_tid),
            );
        } else {
            SGLT_SET_NEXTOFFSET(it, InvalidOffsetNumber);
        }

        /* Insert it on page */
        newoffset = SpGistPageAddNewItem(
            state,
            BufferGetPage(leafBuffer),
            it as Item,
            SGLTGetSize(it) as Size,
            &mut startOffsets[*leafPageSelect.add(i as usize) as usize],
            false,
        );
        *toInsert.add(i as usize) = newoffset;

        /* ... and complete the chain linking */
        ItemPointerSet(&mut (**nodes.add(n as usize)).t_tid, leafBlock, newoffset);

        /* Also copy leaf tuple into WAL data */
        core::ptr::copy_nonoverlapping(
            *newLeafs.add(i as usize) as *const c_char,
            leafptr,
            SGLTGetSize(*newLeafs.add(i as usize)) as usize,
        );
        leafptr = leafptr.add(SGLTGetSize(*newLeafs.add(i as usize)) as usize);
        i += 1;
    }

    /*
     * We're done modifying the other leaf buffer (if any), so mark it dirty.
     * current->buffer will be marked below, after we're entirely done
     * modifying it.
     */
    if newLeafBuffer != InvalidBuffer {
        MarkBufferDirty(newLeafBuffer);
    }

    /* Remember current buffer, since we're about to change "current" */
    saveCurrent = *current;

    /*
     * Store the new innerTuple
     */
    if newInnerBuffer == (*parent).buffer && newInnerBuffer != InvalidBuffer {
        /*
         * new inner tuple goes to parent page
         */
        Assert!((*current).buffer != (*parent).buffer);

        /* Repoint "current" at the new inner tuple */
        (*current).blkno = (*parent).blkno;
        (*current).buffer = (*parent).buffer;
        (*current).page = (*parent).page;
        (*current).offnum = SpGistPageAddNewItem(
            state,
            (*current).page,
            innerTuple as Item,
            (*innerTuple).size as Size,
            core::ptr::null_mut(),
            false,
        );
        xlrec.offnumInner = (*current).offnum;

        /*
         * Update parent node link and mark parent page dirty
         */
        xlrec.innerIsParent = true;
        xlrec.offnumParent = (*parent).offnum;
        xlrec.nodeI = (*parent).node as uint16;
        saveNodeLink(index, parent, (*current).blkno, (*current).offnum);

        /*
         * Update redirection link (in old current buffer)
         */
        if redirectTuplePos != InvalidOffsetNumber {
            setRedirectionTuple(
                &mut saveCurrent,
                redirectTuplePos,
                (*current).blkno,
                (*current).offnum,
            );
        }

        /* Done modifying old current buffer, mark it dirty */
        MarkBufferDirty(saveCurrent.buffer);
    } else if (*parent).buffer != InvalidBuffer {
        /*
         * new inner tuple will be stored on a new page
         */
        Assert!(newInnerBuffer != InvalidBuffer);

        /* Repoint "current" at the new inner tuple */
        (*current).buffer = newInnerBuffer;
        (*current).blkno = BufferGetBlockNumber((*current).buffer);
        (*current).page = BufferGetPage((*current).buffer);
        (*current).offnum = SpGistPageAddNewItem(
            state,
            (*current).page,
            innerTuple as Item,
            (*innerTuple).size as Size,
            core::ptr::null_mut(),
            false,
        );
        xlrec.offnumInner = (*current).offnum;

        /* Done modifying new current buffer, mark it dirty */
        MarkBufferDirty((*current).buffer);

        /*
         * Update parent node link and mark parent page dirty
         */
        xlrec.innerIsParent = (*parent).buffer == (*current).buffer;
        xlrec.offnumParent = (*parent).offnum;
        xlrec.nodeI = (*parent).node as uint16;
        saveNodeLink(index, parent, (*current).blkno, (*current).offnum);

        /*
         * Update redirection link (in old current buffer)
         */
        if redirectTuplePos != InvalidOffsetNumber {
            setRedirectionTuple(
                &mut saveCurrent,
                redirectTuplePos,
                (*current).blkno,
                (*current).offnum,
            );
        }

        /* Done modifying old current buffer, mark it dirty */
        MarkBufferDirty(saveCurrent.buffer);
    } else {
        /*
         * Splitting root page, which was a leaf but now becomes inner page
         * (and so "current" continues to point at it)
         */
        Assert!(SpGistBlockIsRoot((*current).blkno));
        Assert!(redirectTuplePos == InvalidOffsetNumber);

        SpGistInitBuffer((*current).buffer, (if isNulls { SPGIST_NULLS } else { 0 }) as uint16);
        xlrec.initInner = true;
        xlrec.innerIsParent = false;

        (*current).offnum = PageAddItem(
            (*current).page,
            innerTuple as Item,
            (*innerTuple).size as Size,
            InvalidOffsetNumber,
            false,
            false,
        );
        xlrec.offnumInner = (*current).offnum;
        if (*current).offnum != FirstOffsetNumber {
            elog!(
                ERROR,
                "failed to add item of size {} to SPGiST index page",
                (*innerTuple).size
            );
        }

        /* No parent link to update, nor redirection to do */
        xlrec.offnumParent = InvalidOffsetNumber;
        xlrec.nodeI = 0;

        /* Done modifying new current buffer, mark it dirty */
        MarkBufferDirty((*current).buffer);

        /* saveCurrent doesn't represent a different buffer */
        saveCurrent.buffer = InvalidBuffer;
    }

    if RelationNeedsWAL(index) && !(*state).isBuild {
        let recptr: XLogRecPtr;
        let mut flags: c_int;

        XLogBeginInsert();

        xlrec.nInsert = nToInsert as uint16;
        XLogRegisterData(&xlrec as *const _ as *const c_void, SizeOfSpgxlogPickSplit as u32);

        XLogRegisterData(
            toDelete as *const c_void,
            (size_of::<OffsetNumber>() * xlrec.nDelete as usize) as u32,
        );
        XLogRegisterData(
            toInsert as *const c_void,
            (size_of::<OffsetNumber>() * xlrec.nInsert as usize) as u32,
        );
        XLogRegisterData(
            leafPageSelect as *const c_void,
            (size_of::<uint8>() * xlrec.nInsert as usize) as u32,
        );
        XLogRegisterData(innerTuple as *const c_void, (*innerTuple).size as u32);
        XLogRegisterData(leafdata as *const c_void, leafptr.offset_from(leafdata) as u32);

        /* Old leaf page */
        if BufferIsValid(saveCurrent.buffer) {
            flags = REGBUF_STANDARD as c_int;
            if xlrec.initSrc {
                flags |= REGBUF_WILL_INIT as c_int;
            }
            XLogRegisterBuffer(0, saveCurrent.buffer, flags as u8);
        }

        /* New leaf page */
        if BufferIsValid(newLeafBuffer) {
            flags = REGBUF_STANDARD as c_int;
            if xlrec.initDest {
                flags |= REGBUF_WILL_INIT as c_int;
            }
            XLogRegisterBuffer(1, newLeafBuffer, flags as u8);
        }

        /* Inner page */
        flags = REGBUF_STANDARD as c_int;
        if xlrec.initInner {
            flags |= REGBUF_WILL_INIT as c_int;
        }
        XLogRegisterBuffer(2, (*current).buffer, flags as u8);

        /* Parent page, if different from inner page */
        if (*parent).buffer != InvalidBuffer {
            if (*parent).buffer != (*current).buffer {
                XLogRegisterBuffer(3, (*parent).buffer, REGBUF_STANDARD);
            } else {
                Assert!(xlrec.innerIsParent);
            }
        }

        /* Issue the WAL record */
        recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_PICKSPLIT);

        /* Update page LSNs on all affected pages */
        if newLeafBuffer != InvalidBuffer {
            let page: Page = BufferGetPage(newLeafBuffer);

            PageSetLSN(page, recptr);
        }

        if saveCurrent.buffer != InvalidBuffer {
            let page: Page = BufferGetPage(saveCurrent.buffer);

            PageSetLSN(page, recptr);
        }

        PageSetLSN((*current).page, recptr);

        if (*parent).buffer != InvalidBuffer {
            PageSetLSN((*parent).page, recptr);
        }
    }

    END_CRIT_SECTION();

    /* Update local free-space cache and unlock buffers */
    if newLeafBuffer != InvalidBuffer {
        SpGistSetLastUsedPage(index, newLeafBuffer);
        UnlockReleaseBuffer(newLeafBuffer);
    }
    if saveCurrent.buffer != InvalidBuffer {
        SpGistSetLastUsedPage(index, saveCurrent.buffer);
        UnlockReleaseBuffer(saveCurrent.buffer);
    }

    insertedNew
}

/*
 * spgMatchNode action: descend to N'th child node of current inner tuple
 */
unsafe fn spgMatchNodeAction(
    index: Relation,
    state: *mut SpGistState,
    innerTuple: SpGistInnerTuple,
    current: *mut SPPageDesc,
    parent: *mut SPPageDesc,
    nodeN: c_int,
) {
    let mut i: c_int;
    let mut node: SpGistNodeTuple;

    /* Release previous parent buffer if any */
    if (*parent).buffer != InvalidBuffer && (*parent).buffer != (*current).buffer {
        SpGistSetLastUsedPage(index, (*parent).buffer);
        UnlockReleaseBuffer((*parent).buffer);
    }

    /* Repoint parent to specified node of current inner tuple */
    (*parent).blkno = (*current).blkno;
    (*parent).buffer = (*current).buffer;
    (*parent).page = (*current).page;
    (*parent).offnum = (*current).offnum;
    (*parent).node = nodeN;

    /* Locate that node */
    // SGITITERATE(innerTuple, i, node)
    i = 0;
    node = SGITNODEPTR(innerTuple);
    while i < (*innerTuple).nNodes() {
        if i == nodeN {
            break;
        }
        i += 1;
        node = (node as *mut c_char).add(IndexTupleSize(node) as usize) as SpGistNodeTuple;
    }

    if i != nodeN {
        elog!(
            ERROR,
            "failed to find requested node {} in SPGiST inner tuple",
            nodeN
        );
    }

    /* Point current to the downlink location, if any */
    if ItemPointerIsValid(&(*node).t_tid) {
        (*current).blkno = ItemPointerGetBlockNumber(&(*node).t_tid);
        (*current).offnum = ItemPointerGetOffsetNumber(&(*node).t_tid);
    } else {
        /* Downlink is empty, so we'll need to find a new page */
        (*current).blkno = InvalidBlockNumber;
        (*current).offnum = InvalidOffsetNumber;
    }

    (*current).buffer = InvalidBuffer;
    (*current).page = core::ptr::null_mut();
}

/*
 * spgAddNode action: add a node to the inner tuple at current
 */
unsafe fn spgAddNodeAction(
    index: Relation,
    state: *mut SpGistState,
    innerTuple: SpGistInnerTuple,
    current: *mut SPPageDesc,
    parent: *mut SPPageDesc,
    nodeN: c_int,
    nodeLabel: Datum,
) {
    let newInnerTuple: SpGistInnerTuple;
    let mut xlrec: spgxlogAddNode = core::mem::zeroed();

    /* Should not be applied to nulls */
    Assert!(!SpGistPageStoresNulls((*current).page));

    /* Construct new inner tuple with additional node */
    newInnerTuple = addNode(state, innerTuple, nodeLabel, nodeN);

    /* Prepare WAL record */
    STORE_STATE(state, &mut xlrec.stateSrc);
    xlrec.offnum = (*current).offnum;

    /* we don't fill these unless we need to change the parent downlink */
    xlrec.parentBlk = -1;
    xlrec.offnumParent = InvalidOffsetNumber;
    xlrec.nodeI = 0;

    /* we don't fill these unless tuple has to be moved */
    xlrec.offnumNew = InvalidOffsetNumber;
    xlrec.newPage = false;

    if PageGetExactFreeSpace((*current).page) as c_int
        >= (*newInnerTuple).size as c_int - (*innerTuple).size as c_int
    {
        /*
         * We can replace the inner tuple by new version in-place
         */
        START_CRIT_SECTION();

        PageIndexTupleDelete((*current).page, (*current).offnum);
        if PageAddItem(
            (*current).page,
            newInnerTuple as Item,
            (*newInnerTuple).size as Size,
            (*current).offnum,
            false,
            false,
        ) != (*current).offnum
        {
            elog!(
                ERROR,
                "failed to add item of size {} to SPGiST index page",
                (*newInnerTuple).size
            );
        }

        MarkBufferDirty((*current).buffer);

        if RelationNeedsWAL(index) && !(*state).isBuild {
            let recptr: XLogRecPtr;

            XLogBeginInsert();
            XLogRegisterData(
                &xlrec as *const _ as *const c_void,
                size_of::<spgxlogAddNode>() as u32,
            );
            XLogRegisterData(newInnerTuple as *const c_void, (*newInnerTuple).size as u32);

            XLogRegisterBuffer(0, (*current).buffer, REGBUF_STANDARD);

            recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_ADD_NODE);

            PageSetLSN((*current).page, recptr);
        }

        END_CRIT_SECTION();
    } else {
        /*
         * move inner tuple to another page, and update parent
         */
        let dt: SpGistDeadTuple;
        let mut saveCurrent: SPPageDesc;

        /*
         * It should not be possible to get here for the root page, since we
         * allow only one inner tuple on the root page, and spgFormInnerTuple
         * always checks that inner tuples don't exceed the size of a page.
         */
        if SpGistBlockIsRoot((*current).blkno) {
            elog!(ERROR, "cannot enlarge root tuple any more");
        }
        Assert!((*parent).buffer != InvalidBuffer);

        saveCurrent = *current;

        xlrec.offnumParent = (*parent).offnum;
        xlrec.nodeI = (*parent).node as uint16;

        /*
         * obtain new buffer with the same parity as current, since it will be
         * a child of same parent tuple
         */
        (*current).buffer = SpGistGetBuffer(
            index,
            GBUF_INNER_PARITY((*current).blkno),
            (*newInnerTuple).size as c_int + size_of::<ItemIdData>() as c_int,
            &mut xlrec.newPage,
        );
        (*current).blkno = BufferGetBlockNumber((*current).buffer);
        (*current).page = BufferGetPage((*current).buffer);

        /*
         * Let's just make real sure new current isn't same as old.  Right now
         * that's impossible, but if SpGistGetBuffer ever got smart enough to
         * delete placeholder tuples before checking space, maybe it wouldn't
         * be impossible.  The case would appear to work except that WAL
         * replay would be subtly wrong, so I think a mere assert isn't enough
         * here.
         */
        if (*current).blkno == saveCurrent.blkno {
            elog!(ERROR, "SPGiST new buffer shouldn't be same as old buffer");
        }

        /*
         * New current and parent buffer will both be modified; but note that
         * parent buffer could be same as either new or old current.
         */
        if (*parent).buffer == saveCurrent.buffer {
            xlrec.parentBlk = 0;
        } else if (*parent).buffer == (*current).buffer {
            xlrec.parentBlk = 1;
        } else {
            xlrec.parentBlk = 2;
        }

        START_CRIT_SECTION();

        /* insert new ... */
        (*current).offnum = SpGistPageAddNewItem(
            state,
            (*current).page,
            newInnerTuple as Item,
            (*newInnerTuple).size as Size,
            core::ptr::null_mut(),
            false,
        );
        xlrec.offnumNew = (*current).offnum;

        MarkBufferDirty((*current).buffer);

        /* update parent's downlink and mark parent page dirty */
        saveNodeLink(index, parent, (*current).blkno, (*current).offnum);

        /*
         * Replace old tuple with a placeholder or redirection tuple.  Unless
         * doing an index build, we have to insert a redirection tuple for
         * possible concurrent scans.  We can't just delete it in any case,
         * because that could change the offsets of other tuples on the page,
         * breaking downlinks from their parents.
         */
        if (*state).isBuild {
            dt = spgFormDeadTuple(
                state,
                SPGIST_PLACEHOLDER,
                InvalidBlockNumber,
                InvalidOffsetNumber,
            );
        } else {
            dt = spgFormDeadTuple(
                state,
                SPGIST_REDIRECT,
                (*current).blkno,
                (*current).offnum,
            );
        }

        PageIndexTupleDelete(saveCurrent.page, saveCurrent.offnum);
        if PageAddItem(
            saveCurrent.page,
            dt as Item,
            SGDTGetSize(dt) as Size,
            saveCurrent.offnum,
            false,
            false,
        ) != saveCurrent.offnum
        {
            elog!(
                ERROR,
                "failed to add item of size {} to SPGiST index page",
                SGDTGetSize(dt)
            );
        }

        if (*state).isBuild {
            (*SpGistPageGetOpaque(saveCurrent.page)).nPlaceholder += 1;
        } else {
            (*SpGistPageGetOpaque(saveCurrent.page)).nRedirection += 1;
        }

        MarkBufferDirty(saveCurrent.buffer);

        if RelationNeedsWAL(index) && !(*state).isBuild {
            let recptr: XLogRecPtr;
            let mut flags: c_int;

            XLogBeginInsert();

            /* orig page */
            XLogRegisterBuffer(0, saveCurrent.buffer, REGBUF_STANDARD);
            /* new page */
            flags = REGBUF_STANDARD as c_int;
            if xlrec.newPage {
                flags |= REGBUF_WILL_INIT as c_int;
            }
            XLogRegisterBuffer(1, (*current).buffer, flags as u8);
            /* parent page (if different from orig and new) */
            if xlrec.parentBlk == 2 {
                XLogRegisterBuffer(2, (*parent).buffer, REGBUF_STANDARD);
            }

            XLogRegisterData(
                &xlrec as *const _ as *const c_void,
                size_of::<spgxlogAddNode>() as u32,
            );
            XLogRegisterData(newInnerTuple as *const c_void, (*newInnerTuple).size as u32);

            recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_ADD_NODE);

            /* we don't bother to check if any of these are redundant */
            PageSetLSN((*current).page, recptr);
            PageSetLSN((*parent).page, recptr);
            PageSetLSN(saveCurrent.page, recptr);
        }

        END_CRIT_SECTION();

        /* Release saveCurrent if it's not same as current or parent */
        if saveCurrent.buffer != (*current).buffer && saveCurrent.buffer != (*parent).buffer {
            SpGistSetLastUsedPage(index, saveCurrent.buffer);
            UnlockReleaseBuffer(saveCurrent.buffer);
        }
    }
}

/*
 * spgSplitNode action: split inner tuple at current into prefix and postfix
 */
unsafe fn spgSplitNodeAction(
    index: Relation,
    state: *mut SpGistState,
    innerTuple: SpGistInnerTuple,
    current: *mut SPPageDesc,
    out: *mut spgChooseOut,
) {
    let mut prefixTuple: SpGistInnerTuple;
    let postfixTuple: SpGistInnerTuple;
    let mut node: SpGistNodeTuple;
    let mut nodes: *mut SpGistNodeTuple;
    let postfixBlkno: BlockNumber;
    let postfixOffset: OffsetNumber;
    let mut i: c_int;
    let mut xlrec: spgxlogSplitTuple = core::mem::zeroed();
    let mut newBuffer: Buffer = InvalidBuffer;

    /* Should not be applied to nulls */
    Assert!(!SpGistPageStoresNulls((*current).page));

    /* Check opclass gave us sane values */
    if (*out).result.splitTuple.prefixNNodes <= 0
        || (*out).result.splitTuple.prefixNNodes > SGITMAXNNODES
    {
        elog!(
            ERROR,
            "invalid number of prefix nodes: {}",
            (*out).result.splitTuple.prefixNNodes
        );
    }
    if (*out).result.splitTuple.childNodeN < 0
        || (*out).result.splitTuple.childNodeN >= (*out).result.splitTuple.prefixNNodes
    {
        elog!(
            ERROR,
            "invalid child node number: {}",
            (*out).result.splitTuple.childNodeN
        );
    }

    /*
     * Construct new prefix tuple with requested number of nodes.  We'll fill
     * in the childNodeN'th node's downlink below.
     */
    nodes = palloc(
        (size_of::<SpGistNodeTuple>() * (*out).result.splitTuple.prefixNNodes as usize) as Size,
    ) as *mut SpGistNodeTuple;

    i = 0;
    while i < (*out).result.splitTuple.prefixNNodes {
        let mut label: Datum = 0 as Datum;
        let labelisnull: bool;

        labelisnull = (*out).result.splitTuple.prefixNodeLabels.is_null();
        if !labelisnull {
            label = *(*out).result.splitTuple.prefixNodeLabels.add(i as usize);
        }
        *nodes.add(i as usize) = spgFormNodeTuple(state, label, labelisnull);
        i += 1;
    }

    prefixTuple = spgFormInnerTuple(
        state,
        (*out).result.splitTuple.prefixHasPrefix,
        (*out).result.splitTuple.prefixPrefixDatum,
        (*out).result.splitTuple.prefixNNodes,
        nodes,
    );

    /* it must fit in the space that innerTuple now occupies */
    if (*prefixTuple).size > (*innerTuple).size {
        elog!(ERROR, "SPGiST inner-tuple split must not produce longer prefix");
    }

    /*
     * Construct new postfix tuple, containing all nodes of innerTuple with
     * same node datums, but with the prefix specified by the picksplit
     * function.
     */
    nodes = palloc((size_of::<SpGistNodeTuple>() * (*innerTuple).nNodes() as usize) as Size)
        as *mut SpGistNodeTuple;
    // SGITITERATE(innerTuple, i, node)
    i = 0;
    node = SGITNODEPTR(innerTuple);
    while i < (*innerTuple).nNodes() {
        *nodes.add(i as usize) = node;
        i += 1;
        node = (node as *mut c_char).add(IndexTupleSize(node) as usize) as SpGistNodeTuple;
    }

    postfixTuple = spgFormInnerTuple(
        state,
        (*out).result.splitTuple.postfixHasPrefix,
        (*out).result.splitTuple.postfixPrefixDatum,
        (*innerTuple).nNodes(),
        nodes,
    );

    /* Postfix tuple is allTheSame if original tuple was */
    SGITSetAllTheSame(postfixTuple, (*innerTuple).allTheSame());

    /* prep data for WAL record */
    xlrec.newPage = false;

    /*
     * If we can't fit both tuples on the current page, get a new page for the
     * postfix tuple.  In particular, can't split to the root page.
     *
     * For the space calculation, note that prefixTuple replaces innerTuple
     * but postfixTuple will be a new entry.
     */
    if SpGistBlockIsRoot((*current).blkno)
        || SpGistPageGetFreeSpace((*current).page, 1) + (*innerTuple).size as c_int
            < (*prefixTuple).size as c_int
                + (*postfixTuple).size as c_int
                + size_of::<ItemIdData>() as c_int
    {
        /*
         * Choose page with next triple parity, because postfix tuple is a
         * child of prefix one
         */
        newBuffer = SpGistGetBuffer(
            index,
            GBUF_INNER_PARITY((*current).blkno + 1),
            (*postfixTuple).size as c_int + size_of::<ItemIdData>() as c_int,
            &mut xlrec.newPage,
        );
    }

    START_CRIT_SECTION();

    /*
     * Replace old tuple by prefix tuple
     */
    PageIndexTupleDelete((*current).page, (*current).offnum);
    xlrec.offnumPrefix = PageAddItem(
        (*current).page,
        prefixTuple as Item,
        (*prefixTuple).size as Size,
        (*current).offnum,
        false,
        false,
    );
    if xlrec.offnumPrefix != (*current).offnum {
        elog!(
            ERROR,
            "failed to add item of size {} to SPGiST index page",
            (*prefixTuple).size
        );
    }

    /*
     * put postfix tuple into appropriate page
     */
    if newBuffer == InvalidBuffer {
        postfixBlkno = (*current).blkno;
        postfixOffset = SpGistPageAddNewItem(
            state,
            (*current).page,
            postfixTuple as Item,
            (*postfixTuple).size as Size,
            core::ptr::null_mut(),
            false,
        );
        xlrec.offnumPostfix = postfixOffset;
        xlrec.postfixBlkSame = true;
    } else {
        postfixBlkno = BufferGetBlockNumber(newBuffer);
        postfixOffset = SpGistPageAddNewItem(
            state,
            BufferGetPage(newBuffer),
            postfixTuple as Item,
            (*postfixTuple).size as Size,
            core::ptr::null_mut(),
            false,
        );
        xlrec.offnumPostfix = postfixOffset;
        MarkBufferDirty(newBuffer);
        xlrec.postfixBlkSame = false;
    }

    /*
     * And set downlink pointer in the prefix tuple to point to postfix tuple.
     * (We can't avoid this step by doing the above two steps in opposite
     * order, because there might not be enough space on the page to insert
     * the postfix tuple first.)  We have to update the local copy of the
     * prefixTuple too, because that's what will be written to WAL.
     */
    spgUpdateNodeLink(
        prefixTuple,
        (*out).result.splitTuple.childNodeN,
        postfixBlkno,
        postfixOffset,
    );
    prefixTuple = PageGetItem(
        (*current).page,
        PageGetItemId((*current).page, (*current).offnum),
    ) as SpGistInnerTuple;
    spgUpdateNodeLink(
        prefixTuple,
        (*out).result.splitTuple.childNodeN,
        postfixBlkno,
        postfixOffset,
    );

    MarkBufferDirty((*current).buffer);

    if RelationNeedsWAL(index) && !(*state).isBuild {
        let recptr: XLogRecPtr;

        XLogBeginInsert();
        XLogRegisterData(
            &xlrec as *const _ as *const c_void,
            size_of::<spgxlogSplitTuple>() as u32,
        );
        XLogRegisterData(prefixTuple as *const c_void, (*prefixTuple).size as u32);
        XLogRegisterData(postfixTuple as *const c_void, (*postfixTuple).size as u32);

        XLogRegisterBuffer(0, (*current).buffer, REGBUF_STANDARD);
        if newBuffer != InvalidBuffer {
            let mut flags: c_int;

            flags = REGBUF_STANDARD as c_int;
            if xlrec.newPage {
                flags |= REGBUF_WILL_INIT as c_int;
            }
            XLogRegisterBuffer(1, newBuffer, flags as u8);
        }

        recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_SPLIT_TUPLE);

        PageSetLSN((*current).page, recptr);

        if newBuffer != InvalidBuffer {
            PageSetLSN(BufferGetPage(newBuffer), recptr);
        }
    }

    END_CRIT_SECTION();

    /* Update local free-space cache and release buffer */
    if newBuffer != InvalidBuffer {
        SpGistSetLastUsedPage(index, newBuffer);
        UnlockReleaseBuffer(newBuffer);
    }
}

/*
 * Insert one item into the index.
 *
 * Returns true on success, false if we failed to complete the insertion
 * (typically because of conflict with a concurrent insert).  In the latter
 * case, caller should re-call spgdoinsert() with the same args.
 */
pub unsafe fn spgdoinsert(
    index: Relation,
    state: *mut SpGistState,
    heapPtr: ItemPointer,
    datums: *mut Datum,
    isnulls: *mut bool,
) -> bool {
    let mut result: bool = true;
    let leafDescriptor: TupleDesc = (*state).leafTupDesc;
    let isnull: bool = *isnulls.add(spgKeyColumn as usize);
    let mut level: c_int = 0;
    let mut leafDatums: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let mut leafSize: c_int;
    let mut bestLeafSize: c_int;
    let mut numNoProgressCycles: c_int = 0;
    let mut current: SPPageDesc = core::mem::zeroed();
    let mut parent: SPPageDesc = core::mem::zeroed();
    let mut procinfo: *mut FmgrInfo = core::ptr::null_mut();

    /*
     * Look up FmgrInfo of the user-defined choose function once, to save
     * cycles in the loop below.
     */
    if !isnull {
        procinfo = index_getprocinfo(index, 1, SPGIST_CHOOSE_PROC as uint16);
    }

    /*
     * Prepare the leaf datum to insert.
     *
     * If an optional "compress" method is provided, then call it to form the
     * leaf key datum from the input datum.  Otherwise, store the input datum
     * as is.  Since we don't use index_form_tuple in this AM, we have to make
     * sure value to be inserted is not toasted; FormIndexDatum doesn't
     * guarantee that.  But we assume the "compress" method to return an
     * untoasted value.
     */
    if !isnull {
        if OidIsValid(index_getprocid(index, 1, SPGIST_COMPRESS_PROC as uint16)) {
            let compressProcinfo: *mut FmgrInfo;

            compressProcinfo = index_getprocinfo(index, 1, SPGIST_COMPRESS_PROC as uint16);
            leafDatums[spgKeyColumn as usize] = FunctionCall1Coll(
                compressProcinfo,
                *(*index).rd_indcollation.add(spgKeyColumn as usize),
                *datums.add(spgKeyColumn as usize),
            );
        } else {
            Assert!((*state).attLeafType.type_ == (*state).attType.type_);

            if (*state).attType.attlen == -1 {
                leafDatums[spgKeyColumn as usize] =
                    PointerGetDatum(PG_DETOAST_DATUM!(*datums.add(spgKeyColumn as usize)) as *const c_void);
            } else {
                leafDatums[spgKeyColumn as usize] = *datums.add(spgKeyColumn as usize);
            }
        }
    } else {
        leafDatums[spgKeyColumn as usize] = 0 as Datum;
    }

    /* Likewise, ensure that any INCLUDE values are not toasted */
    {
        let mut i: c_int = spgFirstIncludeColumn;
        while i < (*leafDescriptor).natts {
            if !*isnulls.add(i as usize) {
                if (*TupleDescCompactAttr(leafDescriptor, i)).attlen == -1 {
                    leafDatums[i as usize] =
                        PointerGetDatum(PG_DETOAST_DATUM!(*datums.add(i as usize)) as *const c_void);
                } else {
                    leafDatums[i as usize] = *datums.add(i as usize);
                }
            } else {
                leafDatums[i as usize] = 0 as Datum;
            }
            i += 1;
        }
    }

    /*
     * Compute space needed for a leaf tuple containing the given data.
     */
    leafSize = SpGistGetLeafTupleSize(leafDescriptor, leafDatums.as_ptr(), isnulls) as c_int;
    /* Account for an item pointer, too */
    leafSize += size_of::<ItemIdData>() as c_int;

    /*
     * If it isn't gonna fit, and the opclass can't reduce the datum size by
     * suffixing, bail out now rather than doing a lot of useless work.
     */
    if leafSize > SPGIST_PAGE_CAPACITY && (isnull || !(*state).config.longValuesOK) {
        // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errhint("Values larger
        //         than a buffer page cannot be indexed.")
        ereport!(
            ERROR,
            errmsg!(
                "index row size {} exceeds maximum {} for index \"{}\"",
                leafSize as Size - size_of::<ItemIdData>(),
                SPGIST_PAGE_CAPACITY as Size - size_of::<ItemIdData>(),
                CStr::from_ptr(RelationGetRelationName(index)).to_string_lossy()
            )
        );
    }
    bestLeafSize = leafSize;

    /* Initialize "current" to the appropriate root page */
    current.blkno = if isnull { SPGIST_NULL_BLKNO } else { SPGIST_ROOT_BLKNO };
    current.buffer = InvalidBuffer;
    current.page = core::ptr::null_mut();
    current.offnum = FirstOffsetNumber;
    current.node = -1;

    /* "parent" is invalid for the moment */
    parent.blkno = InvalidBlockNumber;
    parent.buffer = InvalidBuffer;
    parent.page = core::ptr::null_mut();
    parent.offnum = InvalidOffsetNumber;
    parent.node = -1;

    /*
     * Before entering the loop, try to clear any pending interrupt condition.
     * If a query cancel is pending, we might as well accept it now not later;
     * while if a non-canceling condition is pending, servicing it here avoids
     * having to restart the insertion and redo all the work so far.
     */
    CHECK_FOR_INTERRUPTS();

    'outer: loop {
        let mut isNew: bool = false;

        /*
         * Bail out if query cancel is pending.  We must have this somewhere
         * in the loop since a broken opclass could produce an infinite
         * picksplit loop.  However, because we'll be holding buffer lock(s)
         * after the first iteration, ProcessInterrupts() wouldn't be able to
         * throw a cancel error here.  Hence, if we see that an interrupt is
         * pending, break out of the loop and deal with the situation below.
         * Set result = false because we must restart the insertion if the
         * interrupt isn't a query-cancel-or-die case.
         */
        if INTERRUPTS_PENDING_CONDITION() {
            result = false;
            break 'outer;
        }

        if current.blkno == InvalidBlockNumber {
            /*
             * Create a leaf page.  If leafSize is too large to fit on a page,
             * we won't actually use the page yet, but it simplifies the API
             * for doPickSplit to always have a leaf page at hand; so just
             * quietly limit our request to a page size.
             */
            current.buffer = SpGistGetBuffer(
                index,
                GBUF_LEAF | (if isnull { GBUF_NULLS } else { 0 }),
                Min(leafSize, SPGIST_PAGE_CAPACITY),
                &mut isNew,
            );
            current.blkno = BufferGetBlockNumber(current.buffer);
        } else if parent.buffer == InvalidBuffer {
            /* we hold no parent-page lock, so no deadlock is possible */
            current.buffer = ReadBuffer(index, current.blkno);
            LockBuffer(current.buffer, BUFFER_LOCK_EXCLUSIVE);
        } else if current.blkno != parent.blkno {
            /* descend to a new child page */
            current.buffer = ReadBuffer(index, current.blkno);

            /*
             * Attempt to acquire lock on child page.  We must beware of
             * deadlock against another insertion process descending from that
             * page to our parent page (see README).  If we fail to get lock,
             * abandon the insertion and tell our caller to start over.
             *
             * XXX this could be improved, because failing to get lock on a
             * buffer is not proof of a deadlock situation; the lock might be
             * held by a reader, or even just background writer/checkpointer
             * process.  Perhaps it'd be worth retrying after sleeping a bit?
             */
            if !ConditionalLockBuffer(current.buffer) {
                ReleaseBuffer(current.buffer);
                UnlockReleaseBuffer(parent.buffer);
                return false;
            }
        } else {
            /* inner tuple can be stored on the same page as parent one */
            current.buffer = parent.buffer;
        }
        current.page = BufferGetPage(current.buffer);

        /* should not arrive at a page of the wrong type */
        if if isnull {
            !SpGistPageStoresNulls(current.page)
        } else {
            SpGistPageStoresNulls(current.page)
        } {
            elog!(
                ERROR,
                "SPGiST index page {} has wrong nulls flag",
                current.blkno
            );
        }

        if SpGistPageIsLeaf(current.page) {
            let leafTuple: SpGistLeafTuple;
            let mut nToSplit: c_int = 0;
            let sizeToSplit: c_int;

            leafTuple = spgFormLeafTuple(state, heapPtr, leafDatums.as_ptr(), isnulls);
            if SGLTGetSize(leafTuple) as c_int + size_of::<ItemIdData>() as c_int
                <= SpGistPageGetFreeSpace(current.page, 1)
            {
                /* it fits on page, so insert it and we're done */
                addLeafTuple(index, state, leafTuple, &mut current, &mut parent, isnull, isNew);
                break 'outer;
            } else if {
                sizeToSplit = checkSplitConditions(index, state, &mut current, &mut nToSplit);
                sizeToSplit < SPGIST_PAGE_CAPACITY / 2
            } && nToSplit < 64
                && SGLTGetSize(leafTuple) as c_int + size_of::<ItemIdData>() as c_int + sizeToSplit
                    <= SPGIST_PAGE_CAPACITY
            {
                /*
                 * the amount of data is pretty small, so just move the whole
                 * chain to another leaf page rather than splitting it.
                 */
                Assert!(!isNew);
                moveLeafs(index, state, &mut current, &mut parent, leafTuple, isnull);
                break 'outer; /* we're done */
            } else {
                /* picksplit */
                if doPickSplit(
                    index, state, &mut current, &mut parent, leafTuple, level, isnull, isNew,
                ) {
                    break 'outer; /* doPickSplit installed new tuples */
                }

                /* leaf tuple will not be inserted yet */
                pfree(leafTuple as *mut c_void);

                /*
                 * current now describes new inner tuple, go insert into it
                 */
                Assert!(!SpGistPageIsLeaf(current.page));
                // goto process_inner_tuple;
            }
        }

        /* non-leaf page (or fell through from picksplit's goto) */
        {
            /*
             * Apply the opclass choose function to figure out how to insert
             * the given datum into the current inner tuple.
             */
            let mut innerTuple: SpGistInnerTuple;
            let mut in_: spgChooseIn = core::mem::zeroed();
            let mut out: spgChooseOut = core::mem::zeroed();

            /*
             * spgAddNode and spgSplitTuple cases will loop back to here to
             * complete the insertion operation.  Just in case the choose
             * function is broken and produces add or split requests
             * repeatedly, check for query cancel (see comments above).
             */
            'process_inner_tuple: loop {
                if INTERRUPTS_PENDING_CONDITION() {
                    result = false;
                    break 'outer;
                }

                innerTuple = PageGetItem(
                    current.page,
                    PageGetItemId(current.page, current.offnum),
                ) as SpGistInnerTuple;

                in_.datum = *datums.add(spgKeyColumn as usize);
                in_.leafDatum = leafDatums[spgKeyColumn as usize];
                in_.level = level;
                in_.allTheSame = (*innerTuple).allTheSame();
                in_.hasPrefix = (*innerTuple).prefixSize() > 0;
                in_.prefixDatum = SGITDATUM(innerTuple, state);
                in_.nNodes = (*innerTuple).nNodes();
                in_.nodeLabels = spgExtractNodeLabels(state, innerTuple);

                core::ptr::write_bytes(&mut out as *mut spgChooseOut, 0, 1);

                if !isnull {
                    /* use user-defined choose method */
                    FunctionCall2Coll(
                        procinfo,
                        *(*index).rd_indcollation.add(0),
                        PointerGetDatum(&in_ as *const _ as *const c_void),
                        PointerGetDatum(&out as *const _ as *const c_void),
                    );
                } else {
                    /* force "match" action (to insert to random subnode) */
                    out.resultType = spgMatchNode;
                }

                if (*innerTuple).allTheSame() {
                    /*
                     * It's not allowed to do an AddNode at an allTheSame tuple.
                     * Opclass must say "match", in which case we choose a random
                     * one of the nodes to descend into, or "split".
                     */
                    if out.resultType == spgAddNode {
                        elog!(ERROR, "cannot add a node to an allTheSame inner tuple");
                    } else if out.resultType == spgMatchNode {
                        out.result.matchNode.nodeN = pg_prng_uint64_range(
                            &raw mut pg_global_prng_state,
                            0,
                            ((*innerTuple).nNodes() - 1) as uint64,
                        ) as c_int;
                    }
                }

                match out.resultType {
                    x if x == spgMatchNode => {
                        /* Descend to N'th child node */
                        spgMatchNodeAction(
                            index,
                            state,
                            innerTuple,
                            &mut current,
                            &mut parent,
                            out.result.matchNode.nodeN,
                        );
                        /* Adjust level as per opclass request */
                        level += out.result.matchNode.levelAdd;
                        /* Replace leafDatum and recompute leafSize */
                        if !isnull {
                            leafDatums[spgKeyColumn as usize] = out.result.matchNode.restDatum;
                            leafSize = SpGistGetLeafTupleSize(
                                leafDescriptor,
                                leafDatums.as_ptr(),
                                isnulls,
                            ) as c_int;
                            leafSize += size_of::<ItemIdData>() as c_int;
                        }

                        /*
                         * Check new tuple size; fail if it can't fit, unless the
                         * opclass says it can handle the situation by suffixing.
                         *
                         * However, the opclass can only shorten the leaf datum,
                         * which may not be enough to ever make the tuple fit,
                         * since INCLUDE columns might alone use more than a page.
                         * Depending on the opclass' behavior, that could lead to
                         * an infinite loop --- spgtextproc.c, for example, will
                         * just repeatedly generate an empty-string leaf datum
                         * once it runs out of data.  Actual bugs in opclasses
                         * might cause infinite looping, too.  To detect such a
                         * loop, check to see if we are making progress by
                         * reducing the leafSize in each pass.  This is a bit
                         * tricky though.  Because of alignment considerations,
                         * the total tuple size might not decrease on every pass.
                         * Also, there are edge cases where the choose method
                         * might seem to not make progress for a cycle or two.
                         * Somewhat arbitrarily, we allow up to 10 no-progress
                         * iterations before failing.  (This limit should be more
                         * than MAXALIGN, to accommodate opclasses that trim one
                         * byte from the leaf datum per pass.)
                         */
                        if leafSize > SPGIST_PAGE_CAPACITY {
                            let mut ok: bool = false;

                            if (*state).config.longValuesOK && !isnull {
                                if leafSize < bestLeafSize {
                                    ok = true;
                                    bestLeafSize = leafSize;
                                    numNoProgressCycles = 0;
                                } else {
                                    numNoProgressCycles += 1;
                                    if numNoProgressCycles < 10 {
                                        ok = true;
                                    }
                                }
                            }
                            if !ok {
                                // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                                //         errhint("Values larger than a buffer page
                                //         cannot be indexed.")
                                ereport!(
                                    ERROR,
                                    errmsg!(
                                        "index row size {} exceeds maximum {} for index \"{}\"",
                                        leafSize as Size - size_of::<ItemIdData>(),
                                        SPGIST_PAGE_CAPACITY as Size - size_of::<ItemIdData>(),
                                        CStr::from_ptr(RelationGetRelationName(index))
                                            .to_string_lossy()
                                    )
                                );
                            }
                        }

                        /*
                         * Loop around and attempt to insert the new leafDatum at
                         * "current" (which might reference an existing child
                         * tuple, or might be invalid to force us to find a new
                         * page for the tuple).
                         */
                        break 'process_inner_tuple;
                    }
                    x if x == spgAddNode => {
                        /* AddNode is not sensible if nodes don't have labels */
                        if in_.nodeLabels.is_null() {
                            elog!(
                                ERROR,
                                "cannot add a node to an inner tuple without node labels"
                            );
                        }
                        /* Add node to inner tuple, per request */
                        spgAddNodeAction(
                            index,
                            state,
                            innerTuple,
                            &mut current,
                            &mut parent,
                            out.result.addNode.nodeN,
                            out.result.addNode.nodeLabel,
                        );

                        /*
                         * Retry insertion into the enlarged node.  We assume that
                         * we'll get a MatchNode result this time.
                         */
                        // goto process_inner_tuple;
                        continue 'process_inner_tuple;
                    }
                    x if x == spgSplitTuple => {
                        /* Split inner tuple, per request */
                        spgSplitNodeAction(index, state, innerTuple, &mut current, &mut out);

                        /* Retry insertion into the split node */
                        // goto process_inner_tuple;
                        continue 'process_inner_tuple;
                    }
                    _ => {
                        elog!(
                            ERROR,
                            "unrecognized SPGiST choose result: {}",
                            out.resultType as c_int
                        );
                    }
                }
            }
        }
    } /* end loop */

    /*
     * Release any buffers we're still holding.  Beware of possibility that
     * current and parent reference same buffer.
     */
    if current.buffer != InvalidBuffer {
        SpGistSetLastUsedPage(index, current.buffer);
        UnlockReleaseBuffer(current.buffer);
    }
    if parent.buffer != InvalidBuffer && parent.buffer != current.buffer {
        SpGistSetLastUsedPage(index, parent.buffer);
        UnlockReleaseBuffer(parent.buffer);
    }

    /*
     * We do not support being called while some outer function is holding a
     * buffer lock (or any other reason to postpone query cancels).  If that
     * were the case, telling the caller to retry would create an infinite
     * loop.
     */
    Assert!(INTERRUPTS_CAN_BE_PROCESSED());

    /*
     * Finally, check for interrupts again.  If there was a query cancel,
     * ProcessInterrupts() will be able to throw the error here.  If it was
     * some other kind of interrupt that can just be cleared, return false to
     * tell our caller to retry.
     */
    CHECK_FOR_INTERRUPTS();

    result
}

// ===========================================================================
// Local stubs for dependencies declared in OTHER .c files / headers that are
// not yet ported.  TODO(pg-port): replace with real imports as they land.
// ===========================================================================

// ---- libc qsort (port/qsort.c / system libc) -------------------------------
unsafe fn qsort(
    base: *mut c_void,
    nmemb: usize,
    size: usize,
    compar: unsafe fn(*const c_void, *const c_void) -> c_int,
) {
    unimplemented!() // TODO(pg-port): src/port/qsort.c
}

// ---- access/itup.h ---------------------------------------------------------
unsafe fn IndexTupleSize(itup: SpGistNodeTuple) -> Size {
    unimplemented!() // TODO(pg-port): src/include/access/itup.h
}

// ---- access/spgist_private.h page/data accessor macros ---------------------
unsafe fn SpGistPageGetOpaque(page: Page) -> crate::access::spgist::spgist_private::SpGistPageOpaque {
    unimplemented!() // TODO(pg-port): access/spgist_private.h (PageGetSpecialPointer)
}
unsafe fn SpGistPageIsLeaf(page: Page) -> bool {
    unimplemented!() // TODO(pg-port): access/spgist_private.h
}
unsafe fn SpGistPageStoresNulls(page: Page) -> bool {
    unimplemented!() // TODO(pg-port): access/spgist_private.h
}
unsafe fn SpGistPageGetFreeSpace(page: Page, n: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): access/spgist_private.h
}

// ---- utils/rel.h -----------------------------------------------------------
unsafe fn RelationNeedsWAL(rel: Relation) -> bool { crate::access::nbtree::nbtdedup::RelationNeedsWAL(rel) }

// ---- storage/bufmgr.c / storage/bufmgr.h -----------------------------------
pub const BUFFER_LOCK_EXCLUSIVE: c_int = 2;

unsafe fn ReadBuffer(reln: Relation, blockNum: BlockNumber) -> Buffer {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn LockBuffer(buffer: Buffer, mode: c_int) {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn ConditionalLockBuffer(buffer: Buffer) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn UnlockReleaseBuffer(buffer: Buffer) {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn ReleaseBuffer(buffer: Buffer) {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn MarkBufferDirty(buffer: Buffer) {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn BufferGetPage(buffer: Buffer) -> Page {
    unimplemented!() // TODO(pg-port): src/include/storage/bufmgr.h
}
unsafe fn BufferGetBlockNumber(buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn BufferIsValid(buffer: Buffer) -> bool { crate::access::nbtree::nbtpage::BufferIsValid(buffer) }
