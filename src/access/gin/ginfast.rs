//! src/backend/access/gin/ginfast.c
//!
//! Fast insert routines for the Postgres inverted index access method.
//!   Pending entries are stored in linear list of pages.  Later on
//!   (typically during VACUUM), ginInsertCleanup() will be invoked to
//!   transfer pending entries into the regular index structure.  This
//!   wins because bulk insertion is much more efficient than retail.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int};

// Crate-root #[macro_export] macros used with `!` call syntax below.
use crate::{PG_GETARG_OID, PG_RETURN_INT64};

// Real (already-ported) types and helpers.
use crate::access::common::indextuple::{IndexTuple, IndexTupleData, IndexTupleSize};
use crate::access::gin::ginvacuum::IndexBulkDeleteResult;
use crate::access::rmgrlist::{RmgrId, RM_GIN_ID};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::catalog::catalog_oids::RelationRelationId;
use crate::catalog::pg_class::RELKIND_INDEX;
use crate::catalog::pg_known_oids::GIN_AM_OID;
use crate::common::blkreftable::RelFileLocator;
use crate::miscadmin::{END_CRIT_SECTION, START_CRIT_SECTION, maintenance_work_mem, work_mem};
use crate::pg_config::BLCKSZ;
use crate::port::pg_bitutils::pg_nextpower2_32;
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::storage::buf::{Buffer, InvalidBuffer};
use crate::storage::bufpage::{
    Item, Page, PageAddItem, PageGetExactFreeSpace, PageGetItem, PageGetItemId,
    PageGetMaxOffsetNumber, PageHeader, PageIsEmpty, PageSetLSN, SizeOfPageHeaderData,
};
use crate::storage::file::buffile::PGAlignedBlock;
use crate::storage::itemid::ItemIdData;
use crate::storage::itemptr::{
    ItemPointer, ItemPointerData, ItemPointerEquals, ItemPointerIsValid, ItemPointerSetInvalid,
};
use crate::storage::lockdefs::{ExclusiveLock, RowExclusiveLock};
use crate::storage::off::{FirstOffsetNumber, InvalidOffsetNumber, OffsetNumber, OffsetNumberNext};
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::rel::{Relation, RelationGetRelationName};

// GUC parameter (canonical definition lives in access::gin::gin; declared here
// without #[no_mangle] to avoid a duplicate exported symbol).
pub static mut gin_pending_list_limit: c_int = 0;

// GUC parameter from postmaster/autovacuum.c (not yet ported); local stub.
static mut autovacuum_work_mem: c_int = -1;

// ===== Local GIN type/const definitions =====
// These mirror the C declarations in gin_private.h / ginblock.h / ginxlog.h
// that ginfast.c includes.  They are kept local (matching the convention used
// by the other translated GIN units) so this file's int-typed logic stays
// internally consistent and the xlog structs share the same RelFileLocator /
// metadata shapes used here.

pub type GinNullCategory = i8;

#[repr(C)]
pub struct GinPageOpaqueData {
    rightlink: BlockNumber,
    maxoff: OffsetNumber,
    flags: u16,
}

#[repr(C)]
pub struct GinMetaPageData {
    head: BlockNumber,
    tail: BlockNumber,
    tailFreeSize: uint32,
    nPendingPages: BlockNumber,
    nPendingHeapTuples: int64,
    nTotalPages: BlockNumber,
    nEntryPages: BlockNumber,
    nDataPages: BlockNumber,
    nEntries: int64,
    ginVersion: int32,
}

#[repr(C)]
pub struct GinState {
    index: Relation,
    // remaining fields of GinState are unused by ginfast.
}

#[repr(C)]
pub struct GinTupleCollector {
    tuples: *mut IndexTuple,
    ntuples: int32,
    lentuples: int32,
    sumsize: int32,
}

#[repr(C)]
pub struct BuildAccumulator {
    ginstate: *mut GinState,
    allocatedMemory: Size,
    // remaining fields of BuildAccumulator are unused by ginfast.
}

pub type GinStatsData = c_void;

// ginxlog.h record structs (kept local with matching field types).
#[repr(C)]
pub struct ginxlogUpdateMeta {
    locator: RelFileLocator,
    metadata: GinMetaPageData,
    prevTail: BlockNumber,
    newRightlink: BlockNumber,
    ntuples: int32,
}

#[repr(C)]
pub struct ginxlogInsertListPage {
    rightlink: BlockNumber,
    ntuples: int32,
}

#[repr(C)]
pub struct ginxlogDeleteListPages {
    metadata: GinMetaPageData,
    ndeleted: int32,
}

// GIN page-type flags / fixed block / lock modes (ginblock.h, gin_private.h).
const GIN_DELETED: u16 = 1 << 2;
const GIN_LIST: u16 = 1 << 4;
const GIN_METAPAGE_BLKNO: BlockNumber = 0;
const GIN_UNLOCK: c_int = 0;
const GIN_SHARE: c_int = 1;
const GIN_EXCLUSIVE: c_int = 2;
const GIN_NDELETE_AT_ONCE: c_int = 16;

// Pending-list page size budget (ginfast.c GinListPageSize).
#[inline]
unsafe fn GinListPageSize() -> usize {
    (BLCKSZ as usize) - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(std::mem::size_of::<GinPageOpaqueData>())
}

// rmgr info value used by XLogInsert (XLOG_GIN_* low byte | info bits).
const XLOG_GIN_INSERT_LISTPAGE: u8 = 0x70;
const XLOG_GIN_UPDATE_META_PAGE: u8 = 0x60;
const XLOG_GIN_DELETE_LISTPAGE: u8 = 0x80;

// xloginsert.h register flags.
const REGBUF_WILL_INIT: c_int = 0x10;
const REGBUF_STANDARD: c_int = 0x04;

// aclchk.h / parsenodes.h enums used by gin_clean_pending_list (stubbed).
pub type AclResult = c_int;
pub type ObjectType = c_int;
const ACLCHECK_NOT_OWNER: AclResult = 1;
const OBJECT_INDEX: ObjectType = 0;

// #define GIN_PAGE_FREESIZE \
//   ( (Size) BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(GinPageOpaqueData)) )
#[inline]
unsafe fn GIN_PAGE_FREESIZE() -> Size {
    (BLCKSZ as Size) - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(std::mem::size_of::<GinPageOpaqueData>())
}

#[repr(C)]
pub struct KeyArray {
    keys: *mut Datum,                 // expansible array
    categories: *mut GinNullCategory, // another expansible array
    nvalues: int32,                   // current number of valid entries
    maxvalues: int32,                 // allocated size of arrays
}

/*
 * Build a pending-list page from the given array of tuples, and write it out.
 *
 * Returns amount of free space left on the page.
 */
unsafe fn writeListPage(
    index: Relation,
    buffer: Buffer,
    tuples: *mut IndexTuple,
    ntuples: int32,
    rightlink: BlockNumber,
) -> int32 {
    let page: Page = BufferGetPage(buffer);
    let mut i: int32;
    let freesize: int32;
    let mut size: int32 = 0;
    let l: OffsetNumber;
    let mut off: OffsetNumber;
    let mut workspace: PGAlignedBlock = std::mem::zeroed();
    let mut ptr: *mut c_char;

    START_CRIT_SECTION();

    GinInitBuffer(buffer, GIN_LIST as u32);

    off = FirstOffsetNumber;
    ptr = workspace.data.as_mut_ptr();

    i = 0;
    while i < ntuples {
        let this_size: c_int = IndexTupleSize(*tuples.offset(i as isize)) as c_int;

        memcpy(
            ptr as *mut c_void,
            *tuples.offset(i as isize) as *const c_void,
            this_size as usize,
        );
        ptr = ptr.offset(this_size as isize);
        size += this_size;

        let l2 = PageAddItem(
            page,
            *tuples.offset(i as isize) as Item,
            this_size as Size,
            off,
            false,
            false,
        );

        if l2 == InvalidOffsetNumber {
            elog!(
                ERROR,
                "failed to add item to index page in \"{:?}\"",
                RelationGetRelationName(index)
            );
        }

        off += 1;
        i += 1;
    }

    Assert!(size as Size <= BLCKSZ as Size); // else we overran workspace

    (*GinPageGetOpaque(page)).rightlink = rightlink;

    /*
     * tail page may contain only whole row(s) or final part of row placed on
     * previous pages (a "row" here meaning all the index tuples generated for
     * one heap tuple)
     */
    if rightlink == InvalidBlockNumber {
        GinPageSetFullRow(page);
        (*GinPageGetOpaque(page)).maxoff = 1;
    } else {
        (*GinPageGetOpaque(page)).maxoff = 0;
    }

    MarkBufferDirty(buffer);

    if RelationNeedsWAL(index) {
        let mut data: ginxlogInsertListPage = std::mem::zeroed();
        let recptr: XLogRecPtr;

        data.rightlink = rightlink;
        data.ntuples = ntuples;

        XLogBeginInsert();
        XLogRegisterData(
            &mut data as *mut _ as *mut c_char,
            std::mem::size_of::<ginxlogInsertListPage>() as c_int,
        );

        XLogRegisterBuffer(0, buffer, REGBUF_WILL_INIT as u8);
        XLogRegisterBufData(0, workspace.data.as_mut_ptr(), size as c_int);

        recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_INSERT_LISTPAGE);
        PageSetLSN(page, recptr);
    }

    let _ = l;

    /* get free space before releasing buffer */
    freesize = PageGetExactFreeSpace(page) as int32;

    UnlockReleaseBuffer(buffer);

    END_CRIT_SECTION();

    freesize
}

unsafe fn makeSublist(
    index: Relation,
    tuples: *mut IndexTuple,
    ntuples: int32,
    res: *mut GinMetaPageData,
) {
    let mut curBuffer: Buffer = InvalidBuffer;
    let mut prevBuffer: Buffer = InvalidBuffer;
    let mut i: c_int;
    let mut size: c_int = 0;
    let mut tupsize: c_int;
    let mut startTuple: c_int = 0;

    Assert!(ntuples > 0);

    /*
     * Split tuples into pages
     */
    i = 0;
    while i < ntuples {
        if curBuffer == InvalidBuffer {
            curBuffer = GinNewBuffer(index);

            if prevBuffer != InvalidBuffer {
                (*res).nPendingPages += 1;
                writeListPage(
                    index,
                    prevBuffer,
                    tuples.offset(startTuple as isize),
                    i - startTuple,
                    BufferGetBlockNumber(curBuffer),
                );
            } else {
                (*res).head = BufferGetBlockNumber(curBuffer);
            }

            prevBuffer = curBuffer;
            startTuple = i;
            size = 0;
        }

        tupsize = (MAXALIGN(IndexTupleSize(*tuples.offset(i as isize)))
            + std::mem::size_of::<ItemIdData>()) as c_int;

        if size + tupsize > GinListPageSize() as c_int {
            /* won't fit, force a new page and reprocess */
            i -= 1;
            curBuffer = InvalidBuffer;
        } else {
            size += tupsize;
        }

        i += 1;
    }

    let _ = tupsize;

    /*
     * Write last page
     */
    (*res).tail = BufferGetBlockNumber(curBuffer);
    (*res).tailFreeSize = writeListPage(
        index,
        curBuffer,
        tuples.offset(startTuple as isize),
        ntuples - startTuple,
        InvalidBlockNumber,
    ) as u32;
    (*res).nPendingPages += 1;
    /* that was only one heap tuple */
    (*res).nPendingHeapTuples = 1;
}

/*
 * Write the index tuples contained in *collector into the index's
 * pending list.
 *
 * Function guarantees that all these tuples will be inserted consecutively,
 * preserving order
 */
#[no_mangle]
pub unsafe extern "C" fn ginHeapTupleFastInsert(
    ginstate: *mut GinState,
    collector: *mut GinTupleCollector,
) {
    let index: Relation = (*ginstate).index;
    let metabuffer: Buffer;
    let metapage: Page;
    let mut metadata: *mut GinMetaPageData = std::ptr::null_mut();
    let mut buffer: Buffer = InvalidBuffer;
    let mut page: Page = std::ptr::null_mut();
    let mut data: ginxlogUpdateMeta = std::mem::zeroed();
    let mut separateList: bool = false;
    let mut needCleanup: bool = false;
    let cleanupSize: c_int;
    let needWal: bool;

    if (*collector).ntuples == 0 {
        return;
    }

    needWal = RelationNeedsWAL(index);

    data.locator = (*index).rd_locator;
    data.ntuples = 0;
    data.newRightlink = InvalidBlockNumber;
    data.prevTail = InvalidBlockNumber;

    metabuffer = ReadBuffer(index, GIN_METAPAGE_BLKNO);
    metapage = BufferGetPage(metabuffer);

    /*
     * An insertion to the pending list could logically belong anywhere in the
     * tree, so it conflicts with all serializable scans.  All scans acquire a
     * predicate lock on the metabuffer to represent that.  Therefore we'll
     * check for conflicts in, but not until we have the page locked and are
     * ready to modify the page.
     */

    if (*collector).sumsize as usize
        + (*collector).ntuples as usize * std::mem::size_of::<ItemIdData>()
        > GinListPageSize() as usize
    {
        /*
         * Total size is greater than one page => make sublist
         */
        separateList = true;
    } else {
        LockBuffer(metabuffer, GIN_EXCLUSIVE);
        metadata = GinPageGetMeta(metapage);

        if (*metadata).head == InvalidBlockNumber
            || (*collector).sumsize as usize
                + (*collector).ntuples as usize * std::mem::size_of::<ItemIdData>()
                > (*metadata).tailFreeSize as usize
        {
            /*
             * Pending list is empty or total size is greater than freespace
             * on tail page => make sublist
             *
             * We unlock metabuffer to keep high concurrency
             */
            separateList = true;
            LockBuffer(metabuffer, GIN_UNLOCK);
        }
    }

    if separateList {
        /*
         * We should make sublist separately and append it to the tail
         */
        let mut sublist: GinMetaPageData = std::mem::zeroed();

        memset(
            &mut sublist as *mut _ as *mut c_void,
            0,
            std::mem::size_of::<GinMetaPageData>(),
        );
        makeSublist(
            index,
            (*collector).tuples,
            (*collector).ntuples,
            &mut sublist,
        );

        /*
         * metapage was unlocked, see above
         */
        LockBuffer(metabuffer, GIN_EXCLUSIVE);
        metadata = GinPageGetMeta(metapage);

        CheckForSerializableConflictIn(index, std::ptr::null_mut(), GIN_METAPAGE_BLKNO);

        if (*metadata).head == InvalidBlockNumber {
            /*
             * Main list is empty, so just insert sublist as main list
             */
            START_CRIT_SECTION();

            (*metadata).head = sublist.head;
            (*metadata).tail = sublist.tail;
            (*metadata).tailFreeSize = sublist.tailFreeSize;

            (*metadata).nPendingPages = sublist.nPendingPages;
            (*metadata).nPendingHeapTuples = sublist.nPendingHeapTuples;

            if needWal {
                XLogBeginInsert();
            }
        } else {
            /*
             * Merge lists
             */
            data.prevTail = (*metadata).tail;
            data.newRightlink = sublist.head;

            buffer = ReadBuffer(index, (*metadata).tail);
            LockBuffer(buffer, GIN_EXCLUSIVE);
            page = BufferGetPage(buffer);

            Assert!((*GinPageGetOpaque(page)).rightlink == InvalidBlockNumber);

            START_CRIT_SECTION();

            (*GinPageGetOpaque(page)).rightlink = sublist.head;

            MarkBufferDirty(buffer);

            (*metadata).tail = sublist.tail;
            (*metadata).tailFreeSize = sublist.tailFreeSize;

            (*metadata).nPendingPages += sublist.nPendingPages;
            (*metadata).nPendingHeapTuples += sublist.nPendingHeapTuples;

            if needWal {
                XLogBeginInsert();
                XLogRegisterBuffer(1, buffer, REGBUF_STANDARD as u8);
            }
        }
    } else {
        /*
         * Insert into tail page.  Metapage is already locked
         */
        let mut l: OffsetNumber;
        let mut off: OffsetNumber;
        let mut i: c_int;
        let mut tupsize: c_int;
        let mut ptr: *mut c_char;
        let collectordata: *mut c_char;

        CheckForSerializableConflictIn(index, std::ptr::null_mut(), GIN_METAPAGE_BLKNO);

        buffer = ReadBuffer(index, (*metadata).tail);
        LockBuffer(buffer, GIN_EXCLUSIVE);
        page = BufferGetPage(buffer);

        off = if PageIsEmpty(page) {
            FirstOffsetNumber
        } else {
            OffsetNumberNext(PageGetMaxOffsetNumber(page))
        };

        collectordata = palloc((*collector).sumsize as Size) as *mut c_char;
        ptr = collectordata;

        data.ntuples = (*collector).ntuples;

        START_CRIT_SECTION();

        if needWal {
            XLogBeginInsert();
        }

        /*
         * Increase counter of heap tuples
         */
        Assert!((*GinPageGetOpaque(page)).maxoff as i64 <= (*metadata).nPendingHeapTuples);
        (*GinPageGetOpaque(page)).maxoff += 1;
        (*metadata).nPendingHeapTuples += 1;

        i = 0;
        while i < (*collector).ntuples {
            tupsize = IndexTupleSize(*(*collector).tuples.offset(i as isize)) as c_int;
            l = PageAddItem(
                page,
                *(*collector).tuples.offset(i as isize) as Item,
                tupsize as Size,
                off,
                false,
                false,
            );

            if l == InvalidOffsetNumber {
                elog!(
                    ERROR,
                    "failed to add item to index page in \"{:?}\"",
                    RelationGetRelationName(index)
                );
            }

            memcpy(
                ptr as *mut c_void,
                *(*collector).tuples.offset(i as isize) as *const c_void,
                tupsize as usize,
            );
            ptr = ptr.offset(tupsize as isize);

            off += 1;
            i += 1;
        }

        Assert!((ptr as isize - collectordata as isize) <= (*collector).sumsize as isize);

        MarkBufferDirty(buffer);

        if needWal {
            XLogRegisterBuffer(1, buffer, REGBUF_STANDARD as u8);
            XLogRegisterBufData(1, collectordata, (*collector).sumsize as c_int);
        }

        (*metadata).tailFreeSize = PageGetExactFreeSpace(page) as u32;
    }

    /*
     * Set pd_lower just past the end of the metadata.  This is essential,
     * because without doing so, metadata will be lost if xlog.c compresses
     * the page.  (We must do this here because pre-v11 versions of PG did not
     * set the metapage's pd_lower correctly, so a pg_upgraded index might
     * contain the wrong value.)
     */
    (*(metapage as PageHeader)).pd_lower = ((metadata as *mut c_char)
        .offset(std::mem::size_of::<GinMetaPageData>() as isize) as isize
        - metapage as isize) as u16;

    /*
     * Write metabuffer, make xlog entry
     */
    MarkBufferDirty(metabuffer);

    if needWal {
        let recptr: XLogRecPtr;

        memcpy(
            &mut data.metadata as *mut _ as *mut c_void,
            metadata as *const c_void,
            std::mem::size_of::<GinMetaPageData>(),
        );

        XLogRegisterBuffer(
            0,
            metabuffer,
            (REGBUF_WILL_INIT | REGBUF_STANDARD) as u8,
        );
        XLogRegisterData(
            &mut data as *mut _ as *mut c_char,
            std::mem::size_of::<ginxlogUpdateMeta>() as c_int,
        );

        recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_UPDATE_META_PAGE);
        PageSetLSN(metapage, recptr);

        if buffer != InvalidBuffer {
            PageSetLSN(page, recptr);
        }
    }

    if buffer != InvalidBuffer {
        UnlockReleaseBuffer(buffer);
    }

    /*
     * Force pending list cleanup when it becomes too long. And,
     * ginInsertCleanup could take significant amount of time, so we prefer to
     * call it when it can do all the work in a single collection cycle. In
     * non-vacuum mode, it shouldn't require maintenance_work_mem, so fire it
     * while pending list is still small enough to fit into
     * gin_pending_list_limit.
     *
     * ginInsertCleanup() should not be called inside our CRIT_SECTION.
     */
    cleanupSize = GinGetPendingListCleanupSize(index);
    if (*metadata).nPendingPages as Size * GIN_PAGE_FREESIZE() > cleanupSize as Size * 1024 {
        needCleanup = true;
    }

    UnlockReleaseBuffer(metabuffer);

    END_CRIT_SECTION();

    /*
     * Since it could contend with concurrent cleanup process we cleanup
     * pending list not forcibly.
     */
    if needCleanup {
        ginInsertCleanup(ginstate, false, true, false, std::ptr::null_mut());
    }
}

/*
 * Create temporary index tuples for a single indexable item (one index column
 * for the heap tuple specified by ht_ctid), and append them to the array
 * in *collector.  They will subsequently be written out using
 * ginHeapTupleFastInsert.  Note that to guarantee consistent state, all
 * temp tuples for a given heap tuple must be written in one call to
 * ginHeapTupleFastInsert.
 */
#[no_mangle]
pub unsafe extern "C" fn ginHeapTupleFastCollect(
    ginstate: *mut GinState,
    collector: *mut GinTupleCollector,
    attnum: OffsetNumber,
    value: Datum,
    isNull: bool,
    ht_ctid: ItemPointer,
) {
    let entries: *mut Datum;
    let categories: *mut GinNullCategory;
    let mut i: int32;
    let mut nentries: int32 = 0;

    /*
     * Extract the key values that need to be inserted in the index
     */
    let mut categories_ptr: *mut GinNullCategory = std::ptr::null_mut();
    entries = ginExtractEntries(
        ginstate,
        attnum,
        value,
        isNull,
        &mut nentries,
        &mut categories_ptr,
    );
    categories = categories_ptr;

    /*
     * Protect against integer overflow in allocation calculations
     */
    if nentries < 0
        || (*collector).ntuples as Size + nentries as Size
            > MaxAllocSize / std::mem::size_of::<IndexTuple>()
    {
        elog!(ERROR, "too many entries for GIN index");
    }

    /*
     * Allocate/reallocate memory for storing collected tuples
     */
    if (*collector).tuples.is_null() {
        /*
         * Determine the number of elements to allocate in the tuples array
         * initially.  Make it a power of 2 to avoid wasting memory when
         * resizing (since palloc likes powers of 2).
         */
        (*collector).lentuples = pg_nextpower2_32(Max(16, nentries) as u32) as c_int;
        (*collector).tuples = palloc_array::<IndexTuple>((*collector).lentuples as usize);
    } else if (*collector).lentuples < (*collector).ntuples + nentries {
        /*
         * Advance lentuples to the next suitable power of 2.  This won't
         * overflow, though we could get to a value that exceeds
         * MaxAllocSize/sizeof(IndexTuple), causing an error in repalloc.
         */
        (*collector).lentuples =
            pg_nextpower2_32(((*collector).ntuples + nentries) as u32) as c_int;
        (*collector).tuples = repalloc_array::<IndexTuple>(
            (*collector).tuples,
            (*collector).lentuples as usize,
        );
    }

    /*
     * Build an index tuple for each key value, and add to array.  In pending
     * tuples we just stick the heap TID into t_tid.
     */
    i = 0;
    while i < nentries {
        let itup: IndexTuple;

        itup = GinFormTuple(
            ginstate,
            attnum,
            *entries.offset(i as isize),
            *categories.offset(i as isize),
            std::ptr::null_mut(),
            0,
            0,
            true,
        );
        (*itup).t_tid = *ht_ctid;
        *(*collector).tuples.offset((*collector).ntuples as isize) = itup;
        (*collector).ntuples += 1;
        (*collector).sumsize += IndexTupleSize(itup) as int32;

        i += 1;
    }
}

/*
 * Deletes pending list pages up to (not including) newHead page.
 * If newHead == InvalidBlockNumber then function drops the whole list.
 *
 * metapage is pinned and exclusive-locked throughout this function.
 */
unsafe fn shiftList(
    index: Relation,
    metabuffer: Buffer,
    newHead: BlockNumber,
    fill_fsm: bool,
    stats: *mut IndexBulkDeleteResult,
) {
    let metapage: Page;
    let metadata: *mut GinMetaPageData;
    let mut blknoToDelete: BlockNumber;

    metapage = BufferGetPage(metabuffer);
    metadata = GinPageGetMeta(metapage);
    blknoToDelete = (*metadata).head;

    loop {
        let mut page: Page;
        let mut i: c_int;
        let mut nDeletedHeapTuples: int64 = 0;
        let mut data: ginxlogDeleteListPages = std::mem::zeroed();
        let mut buffers: [Buffer; GIN_NDELETE_AT_ONCE as usize] =
            [0; GIN_NDELETE_AT_ONCE as usize];
        let mut freespace: [BlockNumber; GIN_NDELETE_AT_ONCE as usize] =
            [0; GIN_NDELETE_AT_ONCE as usize];

        data.ndeleted = 0;
        while data.ndeleted < GIN_NDELETE_AT_ONCE as c_int && blknoToDelete != newHead {
            freespace[data.ndeleted as usize] = blknoToDelete;
            buffers[data.ndeleted as usize] = ReadBuffer(index, blknoToDelete);
            LockBuffer(buffers[data.ndeleted as usize], GIN_EXCLUSIVE);
            page = BufferGetPage(buffers[data.ndeleted as usize]);

            data.ndeleted += 1;

            Assert!(!GinPageIsDeleted(page));

            nDeletedHeapTuples += (*GinPageGetOpaque(page)).maxoff as int64;
            blknoToDelete = (*GinPageGetOpaque(page)).rightlink;
        }

        if !stats.is_null() {
            (*stats).pages_deleted += data.ndeleted as BlockNumber;
        }

        /*
         * This operation touches an unusually large number of pages, so
         * prepare the XLogInsert machinery for that before entering the
         * critical section.
         */
        if RelationNeedsWAL(index) {
            XLogEnsureRecordSpace(data.ndeleted, 0);
        }

        START_CRIT_SECTION();

        (*metadata).head = blknoToDelete;

        Assert!((*metadata).nPendingPages >= data.ndeleted as u32);
        (*metadata).nPendingPages -= data.ndeleted as u32;
        Assert!((*metadata).nPendingHeapTuples >= nDeletedHeapTuples);
        (*metadata).nPendingHeapTuples -= nDeletedHeapTuples;

        if blknoToDelete == InvalidBlockNumber {
            (*metadata).tail = InvalidBlockNumber;
            (*metadata).tailFreeSize = 0;
            (*metadata).nPendingPages = 0;
            (*metadata).nPendingHeapTuples = 0;
        }

        /*
         * Set pd_lower just past the end of the metadata.  This is essential,
         * because without doing so, metadata will be lost if xlog.c
         * compresses the page.  (We must do this here because pre-v11
         * versions of PG did not set the metapage's pd_lower correctly, so a
         * pg_upgraded index might contain the wrong value.)
         */
        (*(metapage as PageHeader)).pd_lower = ((metadata as *mut c_char)
            .offset(std::mem::size_of::<GinMetaPageData>() as isize)
            as isize
            - metapage as isize) as u16;

        MarkBufferDirty(metabuffer);

        i = 0;
        while i < data.ndeleted {
            page = BufferGetPage(buffers[i as usize]);
            (*GinPageGetOpaque(page)).flags = GIN_DELETED as u16;
            MarkBufferDirty(buffers[i as usize]);
            i += 1;
        }

        if RelationNeedsWAL(index) {
            let recptr: XLogRecPtr;

            XLogBeginInsert();
            XLogRegisterBuffer(
                0,
                metabuffer,
                (REGBUF_WILL_INIT | REGBUF_STANDARD) as u8,
            );
            i = 0;
            while i < data.ndeleted {
                XLogRegisterBuffer((i + 1) as u8, buffers[i as usize], REGBUF_WILL_INIT as u8);
                i += 1;
            }

            memcpy(
                &mut data.metadata as *mut _ as *mut c_void,
                metadata as *const c_void,
                std::mem::size_of::<GinMetaPageData>(),
            );

            XLogRegisterData(
                &mut data as *mut _ as *mut c_char,
                std::mem::size_of::<ginxlogDeleteListPages>() as c_int,
            );

            recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_DELETE_LISTPAGE);
            PageSetLSN(metapage, recptr);

            i = 0;
            while i < data.ndeleted {
                page = BufferGetPage(buffers[i as usize]);
                PageSetLSN(page, recptr);
                i += 1;
            }
        }

        i = 0;
        while i < data.ndeleted {
            UnlockReleaseBuffer(buffers[i as usize]);
            i += 1;
        }

        END_CRIT_SECTION();

        i = 0;
        while fill_fsm && i < data.ndeleted {
            RecordFreeIndexPage(index, freespace[i as usize]);
            i += 1;
        }

        if blknoToDelete == newHead {
            break;
        }
    }
}

/* Initialize empty KeyArray */
unsafe fn initKeyArray(keys: *mut KeyArray, maxvalues: int32) {
    (*keys).keys = palloc_array::<Datum>(maxvalues as usize);
    (*keys).categories = palloc_array::<GinNullCategory>(maxvalues as usize);
    (*keys).nvalues = 0;
    (*keys).maxvalues = maxvalues;
}

/* Add datum to KeyArray, resizing if needed */
unsafe fn addDatum(keys: *mut KeyArray, datum: Datum, category: GinNullCategory) {
    if (*keys).nvalues >= (*keys).maxvalues {
        (*keys).maxvalues *= 2;
        (*keys).keys = repalloc_array::<Datum>((*keys).keys, (*keys).maxvalues as usize);
        (*keys).categories = repalloc_array::<GinNullCategory>(
            (*keys).categories,
            (*keys).maxvalues as usize,
        );
    }

    *(*keys).keys.offset((*keys).nvalues as isize) = datum;
    *(*keys).categories.offset((*keys).nvalues as isize) = category;
    (*keys).nvalues += 1;
}

/*
 * Collect data from a pending-list page in preparation for insertion into
 * the main index.
 *
 * Go through all tuples >= startoff on page and collect values in accum
 *
 * Note that ka is just workspace --- it does not carry any state across
 * calls.
 */
unsafe fn processPendingPage(
    accum: *mut BuildAccumulator,
    ka: *mut KeyArray,
    page: Page,
    startoff: OffsetNumber,
) {
    let mut heapptr: ItemPointerData = std::mem::zeroed();
    let mut i: OffsetNumber;
    let maxoff: OffsetNumber;
    let mut attrnum: OffsetNumber;

    /* reset *ka to empty */
    (*ka).nvalues = 0;

    maxoff = PageGetMaxOffsetNumber(page);
    Assert!(maxoff >= FirstOffsetNumber);
    ItemPointerSetInvalid(&mut heapptr);
    attrnum = 0;

    i = startoff;
    while i <= maxoff {
        let itup: IndexTuple =
            PageGetItem(page, PageGetItemId(page, i)) as IndexTuple;
        let curattnum: OffsetNumber;
        let curkey: Datum;
        let mut curcategory: GinNullCategory = 0;

        /* Check for change of heap TID or attnum */
        curattnum = gintuple_get_attrnum((*accum).ginstate, itup);

        if !ItemPointerIsValid(&heapptr) {
            heapptr = (*itup).t_tid;
            attrnum = curattnum;
        } else if !(ItemPointerEquals(&mut heapptr, &mut (*itup).t_tid)
            && curattnum == attrnum)
        {
            /*
             * ginInsertBAEntries can insert several datums per call, but only
             * for one heap tuple and one column.  So call it at a boundary,
             * and reset ka.
             */
            ginInsertBAEntries(
                accum,
                &mut heapptr,
                attrnum,
                (*ka).keys,
                (*ka).categories,
                (*ka).nvalues,
            );
            (*ka).nvalues = 0;
            heapptr = (*itup).t_tid;
            attrnum = curattnum;
        }

        /* Add key to KeyArray */
        curkey = gintuple_get_key((*accum).ginstate, itup, &mut curcategory);
        addDatum(ka, curkey, curcategory);

        i = OffsetNumberNext(i);
    }

    /* Dump out all remaining keys */
    ginInsertBAEntries(
        accum,
        &mut heapptr,
        attrnum,
        (*ka).keys,
        (*ka).categories,
        (*ka).nvalues,
    );
}

/*
 * Move tuples from pending pages into regular GIN structure.
 *
 * On first glance it looks completely not crash-safe. But if we crash
 * after posting entries to the main index and before removing them from the
 * pending list, it's okay because when we redo the posting later on, nothing
 * bad will happen.
 *
 * fill_fsm indicates that ginInsertCleanup should add deleted pages
 * to FSM otherwise caller is responsible to put deleted pages into
 * FSM.
 *
 * If stats isn't null, we count deleted pending pages into the counts.
 */
#[no_mangle]
pub unsafe extern "C" fn ginInsertCleanup(
    ginstate: *mut GinState,
    full_clean: bool,
    fill_fsm: bool,
    forceCleanup: bool,
    stats: *mut IndexBulkDeleteResult,
) {
    let index: Relation = (*ginstate).index;
    let metabuffer: Buffer;
    let mut buffer: Buffer;
    let metapage: Page;
    let mut page: Page;
    let metadata: *mut GinMetaPageData;
    let opCtx: MemoryContext;
    let oldCtx: MemoryContext;
    let mut accum: BuildAccumulator = std::mem::zeroed();
    let mut datums: KeyArray = std::mem::zeroed();
    let mut blkno: BlockNumber;
    let blknoFinish: BlockNumber;
    let mut cleanupFinish: bool = false;
    let mut fsm_vac: bool = false;
    let workMemory: c_int;

    /*
     * We would like to prevent concurrent cleanup process. For that we will
     * lock metapage in exclusive mode using LockPage() call. Nobody other
     * will use that lock for metapage, so we keep possibility of concurrent
     * insertion into pending list
     */

    if forceCleanup {
        /*
         * We are called from [auto]vacuum/analyze or gin_clean_pending_list()
         * and we would like to wait concurrent cleanup to finish.
         */
        LockPage(index, GIN_METAPAGE_BLKNO, ExclusiveLock as c_int);
        workMemory = if AmAutoVacuumWorkerProcess() && autovacuum_work_mem != -1 {
            autovacuum_work_mem
        } else {
            maintenance_work_mem
        };
    } else {
        /*
         * We are called from regular insert and if we see concurrent cleanup
         * just exit in hope that concurrent process will clean up pending
         * list.
         */
        if !ConditionalLockPage(index, GIN_METAPAGE_BLKNO, ExclusiveLock as c_int) {
            return;
        }
        workMemory = work_mem;
    }

    metabuffer = ReadBuffer(index, GIN_METAPAGE_BLKNO);
    LockBuffer(metabuffer, GIN_SHARE);
    metapage = BufferGetPage(metabuffer);
    metadata = GinPageGetMeta(metapage);

    if (*metadata).head == InvalidBlockNumber {
        /* Nothing to do */
        UnlockReleaseBuffer(metabuffer);
        UnlockPage(index, GIN_METAPAGE_BLKNO, ExclusiveLock as c_int);
        return;
    }

    /*
     * Remember a tail page to prevent infinite cleanup if other backends add
     * new tuples faster than we can cleanup.
     */
    blknoFinish = (*metadata).tail;

    /*
     * Read and lock head of pending list
     */
    blkno = (*metadata).head;
    buffer = ReadBuffer(index, blkno);
    LockBuffer(buffer, GIN_SHARE);
    page = BufferGetPage(buffer);

    LockBuffer(metabuffer, GIN_UNLOCK);

    /*
     * Initialize.  All temporary space will be in opCtx
     */
    opCtx = AllocSetContextCreate(
        CurrentMemoryContext,
        c"GIN insert cleanup temporary context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );

    oldCtx = MemoryContextSwitchTo(opCtx);

    initKeyArray(&mut datums, 128);
    ginInitBA(&mut accum);
    accum.ginstate = ginstate;

    /*
     * At the top of this loop, we have pin and lock on the current page of
     * the pending list.  However, we'll release that before exiting the loop.
     * Note we also have pin but not lock on the metapage.
     */
    loop {
        Assert!(!GinPageIsDeleted(page));

        /*
         * Are we walk through the page which as we remember was a tail when
         * we start our cleanup?  But if caller asks us to clean up whole
         * pending list then ignore old tail, we will work until list becomes
         * empty.
         */
        if blkno == blknoFinish && full_clean == false {
            cleanupFinish = true;
        }

        /*
         * read page's datums into accum
         */
        processPendingPage(&mut accum, &mut datums, page, FirstOffsetNumber);

        vacuum_delay_point(false);

        /*
         * Is it time to flush memory to disk?	Flush if we are at the end of
         * the pending list, or if we have a full row and memory is getting
         * full.
         */
        if (*GinPageGetOpaque(page)).rightlink == InvalidBlockNumber
            || (GinPageHasFullRow(page)
                && accum.allocatedMemory >= workMemory as Size * 1024)
        {
            let mut list: *mut ItemPointerData;
            let mut nlist: uint32 = 0;
            let mut key: Datum = 0;
            let mut category: GinNullCategory = 0;
            let maxoff: OffsetNumber;
            let mut attnum: OffsetNumber = 0;

            /*
             * Unlock current page to increase performance. Changes of page
             * will be checked later by comparing maxoff after completion of
             * memory flush.
             */
            maxoff = PageGetMaxOffsetNumber(page);
            LockBuffer(buffer, GIN_UNLOCK);

            /*
             * Moving collected data into regular structure can take
             * significant amount of time - so, run it without locking pending
             * list.
             */
            ginBeginBAScan(&mut accum);
            loop {
                list = ginGetBAEntry(
                    &mut accum,
                    &mut attnum,
                    &mut key,
                    &mut category,
                    &mut nlist,
                );
                if list.is_null() {
                    break;
                }
                ginEntryInsert(
                    ginstate,
                    attnum,
                    key,
                    category,
                    list,
                    nlist,
                    std::ptr::null_mut(),
                );
                vacuum_delay_point(false);
            }

            /*
             * Lock the whole list to remove pages
             */
            LockBuffer(metabuffer, GIN_EXCLUSIVE);
            LockBuffer(buffer, GIN_SHARE);

            Assert!(!GinPageIsDeleted(page));

            /*
             * While we left the page unlocked, more stuff might have gotten
             * added to it.  If so, process those entries immediately.  There
             * shouldn't be very many, so we don't worry about the fact that
             * we're doing this with exclusive lock. Insertion algorithm
             * guarantees that inserted row(s) will not continue on next page.
             * NOTE: intentionally no vacuum_delay_point in this loop.
             */
            if PageGetMaxOffsetNumber(page) != maxoff {
                ginInitBA(&mut accum);
                processPendingPage(&mut accum, &mut datums, page, maxoff + 1);

                ginBeginBAScan(&mut accum);
                loop {
                    list = ginGetBAEntry(
                        &mut accum,
                        &mut attnum,
                        &mut key,
                        &mut category,
                        &mut nlist,
                    );
                    if list.is_null() {
                        break;
                    }
                    ginEntryInsert(
                        ginstate,
                        attnum,
                        key,
                        category,
                        list,
                        nlist,
                        std::ptr::null_mut(),
                    );
                }
            }

            /*
             * Remember next page - it will become the new list head
             */
            blkno = (*GinPageGetOpaque(page)).rightlink;
            UnlockReleaseBuffer(buffer); /* shiftList will do exclusive
                                          * locking */

            /*
             * remove read pages from pending list, at this point all content
             * of read pages is in regular structure
             */
            shiftList(index, metabuffer, blkno, fill_fsm, stats);

            /* At this point, some pending pages have been freed up */
            fsm_vac = true;

            Assert!(blkno == (*metadata).head);
            LockBuffer(metabuffer, GIN_UNLOCK);

            /*
             * if we removed the whole pending list or we cleanup tail (which
             * we remembered on start our cleanup process) then just exit
             */
            if blkno == InvalidBlockNumber || cleanupFinish {
                break;
            }

            /*
             * release memory used so far and reinit state
             */
            MemoryContextReset(opCtx);
            initKeyArray(&mut datums, datums.maxvalues);
            ginInitBA(&mut accum);
        } else {
            blkno = (*GinPageGetOpaque(page)).rightlink;
            UnlockReleaseBuffer(buffer);
        }

        /*
         * Read next page in pending list
         */
        vacuum_delay_point(false);
        buffer = ReadBuffer(index, blkno);
        LockBuffer(buffer, GIN_SHARE);
        page = BufferGetPage(buffer);
    }

    UnlockPage(index, GIN_METAPAGE_BLKNO, ExclusiveLock as c_int);
    ReleaseBuffer(metabuffer);

    /*
     * As pending list pages can have a high churn rate, it is desirable to
     * recycle them immediately to the FreeSpaceMap when ordinary backends
     * clean the list.
     */
    if fsm_vac && fill_fsm {
        IndexFreeSpaceMapVacuum(index);
    }

    /* Clean up temporary space */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(opCtx);
}

/*
 * SQL-callable function to clean the insert pending list
 */
#[no_mangle]
pub unsafe extern "C" fn gin_clean_pending_list(fcinfo: FunctionCallInfo) -> Datum {
    let indexoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let indexRel: Relation = index_open(indexoid, RowExclusiveLock as c_int);
    let mut stats: IndexBulkDeleteResult = std::mem::zeroed();

    if RecoveryInProgress() {
        ereport!(ERROR, "recovery is in progress");
        unreachable!();
    }

    /* Must be a GIN index */
    if (*(*indexRel).rd_rel).relkind != RELKIND_INDEX as c_char
        || (*(*indexRel).rd_rel).relam != GIN_AM_OID
    {
        elog!(
            ERROR,
            "\"{:?}\" is not a GIN index",
            RelationGetRelationName(indexRel)
        );
    }

    /*
     * Reject attempts to read non-local temporary relations; we would be
     * likely to get wrong data since we have no visibility into the owning
     * session's local buffers.
     */
    if RELATION_IS_OTHER_TEMP(indexRel) {
        ereport!(ERROR, "cannot access temporary indexes of other sessions");
        unreachable!();
    }

    /* User must own the index (comparable to privileges needed for VACUUM) */
    if !object_ownercheck(RelationRelationId, indexoid, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_INDEX,
            RelationGetRelationName(indexRel),
        );
    }

    memset(
        &mut stats as *mut _ as *mut c_void,
        0,
        std::mem::size_of::<IndexBulkDeleteResult>(),
    );

    /*
     * Can't assume anything about the content of an !indisready index.  Make
     * those a no-op, not an error, so users can just run this function on all
     * indexes of the access method.  Since an indisready&&!indisvalid index
     * is merely awaiting missed aminsert calls, we're capable of processing
     * it.  Decline to do so, out of an abundance of caution.
     */
    if (*(*indexRel).rd_index).indisvalid {
        let mut ginstate: GinState = std::mem::zeroed();

        initGinState(&mut ginstate, indexRel);
        ginInsertCleanup(&mut ginstate, true, true, true, &mut stats);
    } else {
        ereport!(DEBUG1, "index is not valid");
    }

    index_close(indexRel, RowExclusiveLock as c_int);

    PG_RETURN_INT64!(stats.pages_deleted as int64)
}

// ===== Local stubs for unported helpers =====

unsafe fn GinInitBuffer(_buffer: Buffer, _flags: u32) { unimplemented!() }

unsafe fn GinPageSetFullRow(_page: Page) { crate::access::gin::ginblock::GinPageSetFullRow(_page) }

unsafe fn GinPageGetOpaque(_page: Page) -> *mut GinPageOpaqueData {
    unimplemented!() // TODO: access/gin/gin_private.h
}

unsafe fn GinPageGetMeta(_page: Page) -> *mut GinMetaPageData { unimplemented!() }

unsafe fn GinPageIsDeleted(_page: Page) -> bool { crate::access::gin::ginblock::GinPageIsDeleted(_page) }

unsafe fn GinPageHasFullRow(_page: Page) -> bool { crate::access::gin::ginblock::GinPageHasFullRow(_page) }

unsafe fn GinNewBuffer(_index: Relation) -> Buffer { unimplemented!() }

unsafe fn GinGetPendingListCleanupSize(_index: Relation) -> c_int {
    unimplemented!() // TODO: access/gin/gin_private.h
}

unsafe fn ginExtractEntries(
    _ginstate: *mut GinState,
    _attnum: OffsetNumber,
    _value: Datum,
    _isNull: bool,
    _nentries: *mut int32,
    _categories: *mut *mut GinNullCategory,
) -> *mut Datum { unimplemented!() }

unsafe fn GinFormTuple(
    _ginstate: *mut GinState,
    _attnum: OffsetNumber,
    _key: Datum,
    _category: GinNullCategory,
    _data: *mut c_char,
    _dataSize: Size,
    _nipd: c_int,
    _errorTooBig: bool,
) -> IndexTuple {
    unimplemented!() // TODO: access/gin/ginentrypage.c
}

unsafe fn gintuple_get_attrnum(_ginstate: *mut GinState, _tuple: IndexTuple) -> OffsetNumber { unimplemented!() }

unsafe fn gintuple_get_key(
    _ginstate: *mut GinState,
    _tuple: IndexTuple,
    _category: *mut GinNullCategory,
) -> Datum { unimplemented!() }

unsafe fn ginInitBA(_accum: *mut BuildAccumulator) { unimplemented!() }

unsafe fn ginInsertBAEntries(
    _accum: *mut BuildAccumulator,
    _heapptr: *mut ItemPointerData,
    _attnum: OffsetNumber,
    _entries: *mut Datum,
    _categories: *mut GinNullCategory,
    _nentries: int32,
) {
    unimplemented!() // TODO: access/gin/ginbulk.c
}

unsafe fn ginBeginBAScan(_accum: *mut BuildAccumulator) { unimplemented!() }

unsafe fn ginGetBAEntry(
    _accum: *mut BuildAccumulator,
    _attnum: *mut OffsetNumber,
    _key: *mut Datum,
    _category: *mut GinNullCategory,
    _n: *mut uint32,
) -> *mut ItemPointerData { unimplemented!() }

unsafe fn ginEntryInsert(
    _ginstate: *mut GinState,
    _attnum: OffsetNumber,
    _key: Datum,
    _category: GinNullCategory,
    _items: *mut ItemPointerData,
    _nitem: uint32,
    _buildStats: *mut GinStatsData,
) { unimplemented!() }

unsafe fn initGinState(_state: *mut GinState, _index: Relation) { unimplemented!() }

unsafe fn RecordFreeIndexPage(_rel: Relation, _freeBlock: BlockNumber) { crate::storage::freespace::indexfsm::RecordFreeIndexPage(_rel, _freeBlock) }

unsafe fn IndexFreeSpaceMapVacuum(_rel: Relation) { crate::storage::freespace::indexfsm::IndexFreeSpaceMapVacuum(_rel) }

unsafe fn vacuum_delay_point(_is_analyze: bool) { crate::commands::vacuum::vacuum_delay_point(_is_analyze) }

unsafe fn AmAutoVacuumWorkerProcess() -> bool { crate::miscadmin::AmAutoVacuumWorkerProcess() }

unsafe fn LockPage(_relation: Relation, _blkno: BlockNumber, _lockmode: c_int) {
    unimplemented!() // TODO: storage/lmgr/lmgr.c
}

unsafe fn ConditionalLockPage(_relation: Relation, _blkno: BlockNumber, _lockmode: c_int) -> bool {
    unimplemented!() // TODO: storage/lmgr/lmgr.c
}

unsafe fn UnlockPage(_relation: Relation, _blkno: BlockNumber, _lockmode: c_int) {
    unimplemented!() // TODO: storage/lmgr/lmgr.c
}

unsafe fn CheckForSerializableConflictIn(
    _relation: Relation,
    _tuple: ItemPointer,
    _blkno: BlockNumber,
) { unimplemented!() }

unsafe fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> bool {
    unimplemented!() // TODO: catalog/aclchk.c
}

unsafe fn aclcheck_error(_aclerr: AclResult, _objtype: ObjectType, _objectname: *const c_char) {
    unimplemented!() // TODO: catalog/aclchk.c
}

unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }

unsafe fn RecoveryInProgress() -> bool { crate::access::transam::xlog::RecoveryInProgress() }

unsafe fn index_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/index/indexam.c
}

unsafe fn index_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: access/index/indexam.c
}

unsafe fn RELATION_IS_OTHER_TEMP(_relation: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn AllocSetContextCreate(
    _parent: MemoryContext,
    _name: *const c_char,
    _sizes: (Size, Size, Size),
) -> MemoryContext { crate::utils::mmgr::aset::AllocSetContextCreate(_parent, _name, _sizes) }

unsafe fn XLogEnsureRecordSpace(_max_block_id: c_int, _ndatas: c_int) { crate::access::transam::xloginsert::XLogEnsureRecordSpace(_max_block_id, _ndatas) }

unsafe fn XLogRegisterBufData(_block_id: u8, _data: *mut c_char, _len: c_int) {
    unimplemented!() // TODO: access/transam/xloginsert.c
}

// ----- Buffer manager (storage/buffer/bufmgr.c) -----

unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn ReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn ReadBuffer(_reln: Relation, _blockNum: BlockNumber) -> Buffer {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn RelationNeedsWAL(_relation: Relation) -> bool { crate::access::nbtree::nbtdedup::RelationNeedsWAL(_relation) }

// ----- WAL insertion (access/transam/xloginsert.c) -----

unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: access/transam/xloginsert.c
}

unsafe fn XLogRegisterData(_data: *mut c_char, _len: c_int) {
    unimplemented!() // TODO: access/transam/xloginsert.c
}

unsafe fn XLogRegisterBuffer(_block_id: u8, _buffer: Buffer, _flags: u8) {
    unimplemented!() // TODO: access/transam/xloginsert.c
}

unsafe fn XLogInsert(_rmid: RmgrId, _info: u8) -> XLogRecPtr {
    unimplemented!() // TODO: access/transam/xloginsert.c
}

// ----- palloc array helpers (utils/palloc.h) -----

unsafe fn palloc_array<T>(_n: usize) -> *mut T {
    unimplemented!() // TODO: utils/mmgr/mcxt.c (palloc_array)
}

unsafe fn repalloc_array<T>(_ptr: *mut T, _n: usize) -> *mut T {
    unimplemented!() // TODO: utils/mmgr/mcxt.c (repalloc_array)
}

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}
