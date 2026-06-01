//! heapam_xlog.rs
//!   WAL replay logic for heap access method.
//!
//! Translated 1:1 from postgres/src/backend/access/heap/heapam_xlog.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/access/heap/heapam_xlog.c

use crate::prelude::*; // postgres.h: c types, Datum, palloc, elog!/ereport!/errmsg!/Assert!, MemSet helpers, FirstCommandId, MAXALIGN/SHORTALIGN

use core::ffi::c_char;
use core::ffi::c_int;
use core::ffi::c_void;

use crate::c::uint8;
use crate::c::uint16;
use crate::c::uint32;
use crate::c::int64;
use crate::c::Size;
use crate::c::TransactionId;
use crate::c::CommandId;

// postgres_ext.h
use crate::postgres_ext::Oid;

// pg_config.h
use crate::pg_config::BLCKSZ;

// storage/block.h
use crate::storage::block::BlockNumber;

// storage/off.h
use crate::storage::off::OffsetNumber;
use crate::storage::off::FirstOffsetNumber;
use crate::storage::off::InvalidOffsetNumber;

// storage/itemid.h
use crate::storage::itemid::ItemId;
use crate::storage::itemid::ItemIdGetLength;
use crate::storage::itemid::ItemIdGetOffset;
use crate::storage::itemid::ItemIdHasStorage;
use crate::storage::itemid::ItemIdIsNormal;

// storage/itemptr.h
use crate::storage::itemptr::ItemPointerData;
use crate::storage::itemptr::ItemPointerSet;
use crate::storage::itemptr::ItemPointerSetBlockNumber;
use crate::storage::itemptr::ItemPointerSetOffsetNumber;

// storage/bufpage.h
use crate::storage::bufpage::Page;
use crate::storage::bufpage::Item;
use crate::storage::bufpage::PageGetItem;
use crate::storage::bufpage::PageGetItemId;
use crate::storage::bufpage::PageGetMaxOffsetNumber;
use crate::storage::bufpage::PageGetFreeSpace;
use crate::storage::bufpage::PageInit;
use crate::storage::bufpage::PageIsNew;
use crate::storage::bufpage::PageAddItem;
use crate::storage::bufpage::PageSetAllVisible;
use crate::storage::bufpage::PageClearAllVisible;

// access/transam.h
use crate::access::transam::InvalidTransactionId;

// access/transam/xlogdefs.h
use crate::access::transam::xlogdefs::XLogRecPtr;

// access/htup_details.h
use crate::access::htup_details::HeapTupleData;
use crate::access::htup_details::HeapTupleHeader;
use crate::access::htup_details::HeapTupleHeaderData;
use crate::access::htup_details::MaxHeapTupleSize;
use crate::access::htup_details::SizeofHeapTupleHeader;
use crate::access::htup_details::HeapTupleHeaderSetXmin;
use crate::access::htup_details::HeapTupleHeaderSetXmax;
use crate::access::htup_details::HeapTupleHeaderSetCmin;
use crate::access::htup_details::HeapTupleHeaderSetCmax;
use crate::access::htup_details::HeapTupleHeaderSetHotUpdated;
use crate::access::htup_details::HeapTupleHeaderClearHotUpdated;
use crate::access::htup_details::HeapTupleHeaderSetMovedPartitions;
use crate::access::htup_details::HeapTupleHeaderXminFrozen;
use crate::access::htup_details::HeapTupleHeaderIsSpeculative;
use crate::access::htup_details::HEAP_XMAX_IS_LOCKED_ONLY;
use crate::access::htup_details::HEAP_XMAX_BITS;
use crate::access::htup_details::HEAP_MOVED;
use crate::access::htup_details::HEAP_KEYS_UPDATED;
use crate::access::htup_details::HEAP_XACT_MASK;
use crate::access::htup_details::HEAP_XMAX_INVALID;
use crate::access::htup_details::HEAP_XMAX_COMMITTED;
use crate::access::htup_details::HEAP_XMAX_IS_MULTI;
use crate::access::htup_details::HEAP_XMAX_LOCK_ONLY;
use crate::access::htup_details::HEAP_XMAX_KEYSHR_LOCK;
use crate::access::htup_details::HEAP_XMAX_EXCL_LOCK;

// access/heapam_xlog.h  (WAL record structs live in rmgrdesc/heapdesc.rs)
use crate::access::rmgrdesc::heapdesc::xl_heap_prune;
use crate::access::rmgrdesc::heapdesc::xl_heap_visible;
use crate::access::rmgrdesc::heapdesc::xl_heap_delete;
use crate::access::rmgrdesc::heapdesc::xl_heap_insert;
use crate::access::rmgrdesc::heapdesc::xl_heap_multi_insert;
use crate::access::rmgrdesc::heapdesc::xl_heap_update;
use crate::access::rmgrdesc::heapdesc::xl_heap_confirm;
use crate::access::rmgrdesc::heapdesc::xl_heap_lock;
use crate::access::rmgrdesc::heapdesc::xl_heap_lock_updated;
use crate::access::rmgrdesc::heapdesc::xl_heap_inplace;
use crate::access::rmgrdesc::heapdesc::xl_heap_header;
use crate::access::rmgrdesc::heapdesc::xl_multi_insert_tuple;
use crate::access::rmgrdesc::heapdesc::xlhp_freeze_plan;
use crate::access::rmgrdesc::heapdesc::SizeOfHeapPrune;
use crate::access::rmgrdesc::heapdesc::heap_xlog_deserialize_prune_and_freeze;
use crate::access::rmgrdesc::heapdesc::XLHP_CLEANUP_LOCK;
use crate::access::rmgrdesc::heapdesc::XLHP_HAS_REDIRECTIONS;
use crate::access::rmgrdesc::heapdesc::XLHP_HAS_DEAD_ITEMS;
use crate::access::rmgrdesc::heapdesc::XLHP_HAS_NOW_UNUSED_ITEMS;
use crate::access::rmgrdesc::heapdesc::XLHP_HAS_CONFLICT_HORIZON;
use crate::access::rmgrdesc::heapdesc::XLHP_IS_CATALOG_REL;
use crate::access::rmgrdesc::heapdesc::XLHL_XMAX_IS_MULTI;
use crate::access::rmgrdesc::heapdesc::XLHL_XMAX_LOCK_ONLY;
use crate::access::rmgrdesc::heapdesc::XLHL_XMAX_EXCL_LOCK;
use crate::access::rmgrdesc::heapdesc::XLHL_XMAX_KEYSHR_LOCK;
use crate::access::rmgrdesc::heapdesc::XLHL_KEYS_UPDATED;
use crate::access::rmgrdesc::heapdesc::XLH_DELETE_ALL_VISIBLE_CLEARED;
use crate::access::rmgrdesc::heapdesc::XLH_DELETE_IS_SUPER;
use crate::access::rmgrdesc::heapdesc::XLH_DELETE_IS_PARTITION_MOVE;
use crate::access::rmgrdesc::heapdesc::XLH_INSERT_ALL_VISIBLE_CLEARED;
use crate::access::rmgrdesc::heapdesc::XLH_INSERT_ALL_FROZEN_SET;
use crate::access::rmgrdesc::heapdesc::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED;
use crate::access::rmgrdesc::heapdesc::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED;
use crate::access::rmgrdesc::heapdesc::XLH_UPDATE_PREFIX_FROM_OLD;
use crate::access::rmgrdesc::heapdesc::XLH_UPDATE_SUFFIX_FROM_OLD;
use crate::access::rmgrdesc::heapdesc::XLH_LOCK_ALL_FROZEN_CLEARED;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP_INSERT;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP_DELETE;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP_UPDATE;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP_TRUNCATE;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP_HOT_UPDATE;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP_CONFIRM;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP_LOCK;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP_INPLACE;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP_INIT_PAGE;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP_OPMASK;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP2_PRUNE_ON_ACCESS;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP2_PRUNE_VACUUM_SCAN;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP2_PRUNE_VACUUM_CLEANUP;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP2_VISIBLE;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP2_MULTI_INSERT;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP2_LOCK_UPDATED;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP2_NEW_CID;
use crate::access::rmgrdesc::heapdesc::XLOG_HEAP2_REWRITE;

// access/visibilitymapdefs.h
use crate::access::visibilitymapdefs::VISIBILITYMAP_VALID_BITS;
use crate::access::visibilitymapdefs::VISIBILITYMAP_ALL_FROZEN;
use crate::access::visibilitymapdefs::VISIBILITYMAP_XLOG_VALID_BITS;
use crate::access::visibilitymapdefs::VISIBILITYMAP_XLOG_CATALOG_REL;

// access/xlogreader.h
use crate::access::transam::xlogreader::XLogReaderState;
use crate::access::transam::xlogreader::XLogRecGetData;
use crate::access::transam::xlogreader::XLogRecGetInfo;
use crate::access::transam::xlogreader::XLogRecGetXid;
use crate::access::transam::xlogreader::XLogRecGetBlockTag;
use crate::access::transam::xlogreader::XLogRecGetBlockTagExtended;
use crate::access::transam::xlogreader::XLogRecGetBlockData;
use crate::access::transam::xlogreader::XLR_INFO_MASK;

// access/xlogutils.h
use crate::access::transam::xlogutils::XLogRedoAction;
use crate::access::transam::xlogutils::BLK_NEEDS_REDO;
use crate::access::transam::xlogutils::BLK_RESTORED;
use crate::access::transam::xlogutils::RBM_NORMAL;
use crate::access::transam::xlogutils::RBM_ZERO_ON_ERROR;

// access/common/bufmask.h
use crate::access::common::bufmask::MASK_MARKER;
use crate::access::common::bufmask::mask_page_lsn_and_checksum;
use crate::access::common::bufmask::mask_page_hint_bits;
use crate::access::common::bufmask::mask_unused_space;

// access/heap/pruneheap.c
use crate::access::heap::pruneheap::HeapTupleFreeze;
use crate::access::heap::pruneheap::heap_page_prune_execute;

// access/heap/rewriteheap.c
use crate::access::heap::rewriteheap::heap_xlog_logical_rewrite;

// ----------------------------------------------------------------------------
// SizeOf macros from access/heapam_xlog.h (this header is what we are porting).
// ----------------------------------------------------------------------------

/* #define SizeOfHeapHeader (offsetof(xl_heap_header, t_hoff) + sizeof(uint8)) */
const SizeOfHeapHeader: Size =
    (core::mem::offset_of!(xl_heap_header, t_hoff) + core::mem::size_of::<uint8>()) as Size;

/* #define SizeOfMultiInsertTuple (offsetof(xl_multi_insert_tuple, t_hoff) + sizeof(uint8)) */
const SizeOfMultiInsertTuple: Size =
    (core::mem::offset_of!(xl_multi_insert_tuple, t_hoff) + core::mem::size_of::<uint8>()) as Size;

// ----------------------------------------------------------------------------
// Local type aliases / stubs for symbols owned by other .c files that are not
// yet ported, mirroring the convention used by the sibling heap files.
// ----------------------------------------------------------------------------

// storage/buf.h
type Buffer = c_int;
const InvalidBuffer: Buffer = 0; // TODO(pg-port): real InvalidBuffer lives in storage/buf.h

// utils/rel.h
type Relation = *mut c_void; // TODO(pg-port): real Relation lives in utils/rel.h

// storage/relfilelocator.h
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: Oid,
} // TODO(pg-port): real RelFileLocator lives in storage/relfilelocator.h

// storage/sinval.h
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SharedInvalidationMessage {
    pub id: i8,
    pub _pad: [u8; 15],
} // TODO(pg-port): real SharedInvalidationMessage lives in storage/sinval.h

const BUFFER_LOCK_UNLOCK: c_int = 0; // TODO(pg-port): real BUFFER_LOCK_UNLOCK lives in storage/bufmgr.h

const InHotStandby: bool = false; // TODO(pg-port): real InHotStandby lives in storage/standby.h

unsafe extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void; // TODO(pg-port): libc memcpy
    fn memset(dest: *mut c_void, c: c_int, n: usize) -> *mut c_void; // TODO(pg-port): libc memset
}

unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO(pg-port): real BufferGetPage lives in storage/bufmgr.h
}
unsafe fn BufferGetPageSize(_buffer: Buffer) -> Size {
    unimplemented!() // TODO(pg-port): real BufferGetPageSize lives in storage/bufmgr.h
}
unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO(pg-port): real BufferGetBlockNumber lives in storage/buffer/bufmgr.c
}
unsafe fn BufferIsValid(_buffer: Buffer) -> bool {
    unimplemented!() // TODO(pg-port): real BufferIsValid lives in storage/bufmgr.h
}
unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO(pg-port): real LockBuffer lives in storage/buffer/bufmgr.c
}
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO(pg-port): real MarkBufferDirty lives in storage/buffer/bufmgr.c
}
unsafe fn ReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO(pg-port): real ReleaseBuffer lives in storage/buffer/bufmgr.c
}
unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO(pg-port): real UnlockReleaseBuffer lives in storage/buffer/bufmgr.c
}
unsafe fn PageGetHeapFreeSpace(_page: Page) -> Size {
    unimplemented!() // TODO(pg-port): real PageGetHeapFreeSpace lives in storage/bufpage.c
}
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) {
    unimplemented!() // TODO(pg-port): real PageSetLSN lives in storage/bufpage.h
}
unsafe fn PageSetPrunable(_page: Page, _xid: TransactionId) {
    unimplemented!() // TODO(pg-port): real PageSetPrunable lives in storage/bufpage.h
}
unsafe fn XLogHintBitIsNeeded() -> bool {
    unimplemented!() // TODO(pg-port): real XLogHintBitIsNeeded lives in access/xlog.h
}
unsafe fn XLogReadBufferForRedo(
    _record: *mut XLogReaderState,
    _block_id: uint8,
    _buf: *mut Buffer,
) -> XLogRedoAction {
    unimplemented!() // TODO(pg-port): real XLogReadBufferForRedo lives in access/transam/xlogutils.c
}
unsafe fn XLogReadBufferForRedoExtended(
    _record: *mut XLogReaderState,
    _block_id: uint8,
    _mode: c_int,
    _get_cleanup_lock: bool,
    _buf: *mut Buffer,
) -> XLogRedoAction {
    unimplemented!() // TODO(pg-port): real XLogReadBufferForRedoExtended lives in access/transam/xlogutils.c
}
unsafe fn XLogInitBufferForRedo(_record: *mut XLogReaderState, _block_id: uint8) -> Buffer {
    unimplemented!() // TODO(pg-port): real XLogInitBufferForRedo lives in access/transam/xlogutils.c
}
unsafe fn XLogRecordPageWithFreeSpace(
    _rlocator: RelFileLocator,
    _heapBlk: BlockNumber,
    _spaceAvail: Size,
) {
    unimplemented!() // TODO(pg-port): real XLogRecordPageWithFreeSpace lives in storage/freespace/freespace.c
}
unsafe fn ResolveRecoveryConflictWithSnapshot(
    _snapshotConflictHorizon: TransactionId,
    _isCatalogRel: bool,
    _locator: RelFileLocator,
) {
    unimplemented!() // TODO(pg-port): real ResolveRecoveryConflictWithSnapshot lives in storage/ipc/standby.c
}
unsafe fn CreateFakeRelcacheEntry(_rlocator: RelFileLocator) -> Relation {
    unimplemented!() // TODO(pg-port): real CreateFakeRelcacheEntry lives in access/transam/xlogutils.c
}
unsafe fn FreeFakeRelcacheEntry(_fakerel: Relation) {
    unimplemented!() // TODO(pg-port): real FreeFakeRelcacheEntry lives in access/transam/xlogutils.c
}
unsafe fn visibilitymap_pin(_rel: Relation, _heapBlk: BlockNumber, _vmbuf: *mut Buffer) {
    unimplemented!() // TODO(pg-port): real visibilitymap_pin lives in access/heap/visibilitymap.c
}
unsafe fn visibilitymap_clear(
    _rel: Relation,
    _heapBlk: BlockNumber,
    _vmbuf: Buffer,
    _flags: uint8,
) -> bool {
    unimplemented!() // TODO(pg-port): real visibilitymap_clear lives in access/heap/visibilitymap.c
}
unsafe fn visibilitymap_set(
    _rel: Relation,
    _heapBlk: BlockNumber,
    _heapBuf: Buffer,
    _recptr: XLogRecPtr,
    _vmBuf: Buffer,
    _cutoff_xid: TransactionId,
    _flags: uint8,
) -> uint8 {
    unimplemented!() // TODO(pg-port): real visibilitymap_set lives in access/heap/visibilitymap.c
}
unsafe fn heap_execute_freeze_tuple(_tuple: HeapTupleHeader, _frz: *mut HeapTupleFreeze) {
    unimplemented!() // TODO(pg-port): real heap_execute_freeze_tuple lives in access/heap/heapam.c
}
unsafe fn ProcessCommittedInvalidationMessages(
    _msgs: *mut SharedInvalidationMessage,
    _nmsgs: c_int,
    _RelcacheInitFileInval: bool,
    _dbid: Oid,
    _tsid: Oid,
) {
    unimplemented!() // TODO(pg-port): real ProcessCommittedInvalidationMessages lives in utils/cache/inval.c
}

// ----------------------------------------------------------------------------
// Functions
// ----------------------------------------------------------------------------

/*
 * Replay XLOG_HEAP2_PRUNE_* records.
 */
unsafe fn heap_xlog_prune_freeze(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let mut maindataptr: *mut c_char = XLogRecGetData(record);
    let mut xlrec: xl_heap_prune = core::mem::zeroed();
    let mut buffer: Buffer = 0;
    let mut rlocator: RelFileLocator = core::mem::zeroed();
    let mut blkno: BlockNumber = 0;
    let action: XLogRedoAction;

    XLogRecGetBlockTag(record, 0, &mut rlocator as *mut _ as *mut _, null_mut(), &mut blkno);
    memcpy(
        &mut xlrec as *mut _ as *mut c_void,
        maindataptr as *const c_void,
        SizeOfHeapPrune as usize,
    );
    maindataptr = maindataptr.add(SizeOfHeapPrune as usize);

    /*
     * We will take an ordinary exclusive lock or a cleanup lock depending on
     * whether the XLHP_CLEANUP_LOCK flag is set.  With an ordinary exclusive
     * lock, we better not be doing anything that requires moving existing
     * tuple data.
     */
    Assert!(
        (xlrec.flags & XLHP_CLEANUP_LOCK) != 0
            || (xlrec.flags & (XLHP_HAS_REDIRECTIONS | XLHP_HAS_DEAD_ITEMS)) == 0
    );

    /*
     * We are about to remove and/or freeze tuples.  In Hot Standby mode,
     * ensure that there are no queries running for which the removed tuples
     * are still visible or which still consider the frozen xids as running.
     * The conflict horizon XID comes after xl_heap_prune.
     */
    if (xlrec.flags & XLHP_HAS_CONFLICT_HORIZON) != 0 {
        let mut snapshot_conflict_horizon: TransactionId = 0;

        /* memcpy() because snapshot_conflict_horizon is stored unaligned */
        memcpy(
            &mut snapshot_conflict_horizon as *mut _ as *mut c_void,
            maindataptr as *const c_void,
            core::mem::size_of::<TransactionId>(),
        );
        maindataptr = maindataptr.add(core::mem::size_of::<TransactionId>());

        if InHotStandby {
            ResolveRecoveryConflictWithSnapshot(
                snapshot_conflict_horizon,
                (xlrec.flags & XLHP_IS_CATALOG_REL) != 0,
                rlocator,
            );
        }
    }

    /*
     * If we have a full-page image, restore it and we're done.
     */
    action = XLogReadBufferForRedoExtended(
        record,
        0,
        RBM_NORMAL,
        (xlrec.flags & XLHP_CLEANUP_LOCK) != 0,
        &mut buffer,
    );
    if action == BLK_NEEDS_REDO {
        let page: Page = BufferGetPage(buffer) as Page;
        let mut redirected: *mut OffsetNumber = null_mut();
        let mut nowdead: *mut OffsetNumber = null_mut();
        let mut nowunused: *mut OffsetNumber = null_mut();
        let mut nredirected: c_int = 0;
        let mut ndead: c_int = 0;
        let mut nunused: c_int = 0;
        let mut nplans: c_int = 0;
        let mut datalen: Size = 0;
        let mut plans: *mut xlhp_freeze_plan = null_mut();
        let mut frz_offsets: *mut OffsetNumber = null_mut();
        let dataptr: *mut c_char = XLogRecGetBlockData(record, 0, &mut datalen);

        heap_xlog_deserialize_prune_and_freeze(
            dataptr,
            xlrec.flags,
            &mut nplans,
            &mut plans,
            &mut frz_offsets,
            &mut nredirected,
            &mut redirected,
            &mut ndead,
            &mut nowdead,
            &mut nunused,
            &mut nowunused,
        );

        /*
         * Update all line pointers per the record, and repair fragmentation
         * if needed.
         */
        if nredirected > 0 || ndead > 0 || nunused > 0 {
            heap_page_prune_execute(
                buffer,
                (xlrec.flags & XLHP_CLEANUP_LOCK) == 0,
                redirected,
                nredirected,
                nowdead,
                ndead,
                nowunused,
                nunused,
            );
        }

        /* Freeze tuples */
        for p in 0..nplans {
            let mut frz: HeapTupleFreeze = core::mem::zeroed();

            /*
             * Convert freeze plan representation from WAL record into
             * per-tuple format used by heap_execute_freeze_tuple
             */
            frz.xmax = (*plans.add(p as usize)).xmax;
            frz.t_infomask2 = (*plans.add(p as usize)).t_infomask2;
            frz.t_infomask = (*plans.add(p as usize)).t_infomask;
            frz.frzflags = (*plans.add(p as usize)).frzflags;
            frz.offset = InvalidOffsetNumber; /* unused, but be tidy */

            for _i in 0..(*plans.add(p as usize)).ntuples {
                let offset: OffsetNumber = *frz_offsets;
                frz_offsets = frz_offsets.add(1);
                let lp: ItemId;
                let tuple: HeapTupleHeader;

                lp = PageGetItemId(page, offset);
                tuple = PageGetItem(page, lp) as HeapTupleHeader;
                heap_execute_freeze_tuple(tuple, &mut frz);
            }
        }

        /* There should be no more data */
        Assert!(frz_offsets as *const c_char == dataptr.add(datalen as usize) as *const c_char);

        /*
         * Note: we don't worry about updating the page's prunability hints.
         * At worst this will cause an extra prune cycle to occur soon.
         */

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    /*
     * If we released any space or line pointers, update the free space map.
     *
     * Do this regardless of a full-page image being applied, since the FSM
     * data is not in the page anyway.
     */
    if BufferIsValid(buffer) {
        if xlrec.flags
            & (XLHP_HAS_REDIRECTIONS | XLHP_HAS_DEAD_ITEMS | XLHP_HAS_NOW_UNUSED_ITEMS)
            != 0
        {
            let freespace: Size = PageGetHeapFreeSpace(BufferGetPage(buffer) as Page);

            UnlockReleaseBuffer(buffer);

            XLogRecordPageWithFreeSpace(rlocator, blkno, freespace);
        } else {
            UnlockReleaseBuffer(buffer);
        }
    }
}

/*
 * Replay XLOG_HEAP2_VISIBLE records.
 *
 * The critical integrity requirement here is that we must never end up with
 * a situation where the visibility map bit is set, and the page-level
 * PD_ALL_VISIBLE bit is clear.  If that were to occur, then a subsequent
 * page modification would fail to clear the visibility map bit.
 */
unsafe fn heap_xlog_visible(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_heap_visible = XLogRecGetData(record) as *mut xl_heap_visible;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let mut buffer: Buffer = 0;
    let page: Page;
    let mut rlocator: RelFileLocator = core::mem::zeroed();
    let mut blkno: BlockNumber = 0;
    let action: XLogRedoAction;

    Assert!(((*xlrec).flags & VISIBILITYMAP_XLOG_VALID_BITS) == (*xlrec).flags);

    XLogRecGetBlockTag(record, 1, &mut rlocator as *mut _ as *mut _, null_mut(), &mut blkno);

    /*
     * If there are any Hot Standby transactions running that have an xmin
     * horizon old enough that this page isn't all-visible for them, they
     * might incorrectly decide that an index-only scan can skip a heap fetch.
     *
     * NB: It might be better to throw some kind of "soft" conflict here that
     * forces any index-only scan that is in flight to perform heap fetches,
     * rather than killing the transaction outright.
     */
    if InHotStandby {
        ResolveRecoveryConflictWithSnapshot(
            (*xlrec).snapshotConflictHorizon,
            (*xlrec).flags & VISIBILITYMAP_XLOG_CATALOG_REL != 0,
            rlocator,
        );
    }

    /*
     * Read the heap page, if it still exists. If the heap file has dropped or
     * truncated later in recovery, we don't need to update the page, but we'd
     * better still update the visibility map.
     */
    action = XLogReadBufferForRedo(record, 1, &mut buffer);
    if action == BLK_NEEDS_REDO {
        /*
         * We don't bump the LSN of the heap page when setting the visibility
         * map bit (unless checksums or wal_hint_bits is enabled, in which
         * case we must). This exposes us to torn page hazards, but since
         * we're not inspecting the existing page contents in any way, we
         * don't care.
         */
        page = BufferGetPage(buffer) as Page;

        PageSetAllVisible(page);

        if XLogHintBitIsNeeded() {
            PageSetLSN(page, lsn);
        }

        MarkBufferDirty(buffer);
    } else if action == BLK_RESTORED {
        /*
         * If heap block was backed up, we already restored it and there's
         * nothing more to do. (This can only happen with checksums or
         * wal_log_hints enabled.)
         */
    }

    if BufferIsValid(buffer) {
        let space: Size = PageGetFreeSpace(BufferGetPage(buffer) as Page);

        UnlockReleaseBuffer(buffer);

        /*
         * Since FSM is not WAL-logged and only updated heuristically, it
         * easily becomes stale in standbys.  If the standby is later promoted
         * and runs VACUUM, it will skip updating individual free space
         * figures for pages that became all-visible (or all-frozen, depending
         * on the vacuum mode,) which is troublesome when FreeSpaceMapVacuum
         * propagates too optimistic free space values to upper FSM layers;
         * later inserters try to use such pages only to find out that they
         * are unusable.  This can cause long stalls when there are many such
         * pages.
         *
         * Forestall those problems by updating FSM's idea about a page that
         * is becoming all-visible or all-frozen.
         *
         * Do this regardless of a full-page image being applied, since the
         * FSM data is not in the page anyway.
         */
        if (*xlrec).flags & VISIBILITYMAP_VALID_BITS != 0 {
            XLogRecordPageWithFreeSpace(rlocator, blkno, space);
        }
    }

    /*
     * Even if we skipped the heap page update due to the LSN interlock, it's
     * still safe to update the visibility map.  Any WAL record that clears
     * the visibility map bit does so before checking the page LSN, so any
     * bits that need to be cleared will still be cleared.
     */
    if XLogReadBufferForRedoExtended(record, 0, RBM_ZERO_ON_ERROR, false, &mut vmbuffer)
        == BLK_NEEDS_REDO
    {
        let vmpage: Page = BufferGetPage(vmbuffer) as Page;
        let reln: Relation;
        let vmbits: uint8;

        /* initialize the page if it was read as zeros */
        if PageIsNew(vmpage) {
            PageInit(vmpage, BLCKSZ as Size, 0);
        }

        /* remove VISIBILITYMAP_XLOG_* */
        vmbits = (*xlrec).flags & VISIBILITYMAP_VALID_BITS;

        /*
         * XLogReadBufferForRedoExtended locked the buffer. But
         * visibilitymap_set will handle locking itself.
         */
        LockBuffer(vmbuffer, BUFFER_LOCK_UNLOCK);

        reln = CreateFakeRelcacheEntry(rlocator);
        visibilitymap_pin(reln, blkno, &mut vmbuffer);

        visibilitymap_set(
            reln,
            blkno,
            InvalidBuffer,
            lsn,
            vmbuffer,
            (*xlrec).snapshotConflictHorizon,
            vmbits,
        );

        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    } else if BufferIsValid(vmbuffer) {
        UnlockReleaseBuffer(vmbuffer);
    }
}

/*
 * Given an "infobits" field from an XLog record, set the correct bits in the
 * given infomask and infomask2 for the tuple touched by the record.
 *
 * (This is the reverse of compute_infobits).
 */
unsafe fn fix_infomask_from_infobits(infobits: uint8, infomask: *mut uint16, infomask2: *mut uint16) {
    *infomask &= !(HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK
        | HEAP_XMAX_EXCL_LOCK);
    *infomask2 &= !HEAP_KEYS_UPDATED;

    if infobits & XLHL_XMAX_IS_MULTI != 0 {
        *infomask |= HEAP_XMAX_IS_MULTI;
    }
    if infobits & XLHL_XMAX_LOCK_ONLY != 0 {
        *infomask |= HEAP_XMAX_LOCK_ONLY;
    }
    if infobits & XLHL_XMAX_EXCL_LOCK != 0 {
        *infomask |= HEAP_XMAX_EXCL_LOCK;
    }
    /* note HEAP_XMAX_SHR_LOCK isn't considered here */
    if infobits & XLHL_XMAX_KEYSHR_LOCK != 0 {
        *infomask |= HEAP_XMAX_KEYSHR_LOCK;
    }

    if infobits & XLHL_KEYS_UPDATED != 0 {
        *infomask2 |= HEAP_KEYS_UPDATED;
    }
}

/*
 * Replay XLOG_HEAP_DELETE records.
 */
unsafe fn heap_xlog_delete(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_heap_delete = XLogRecGetData(record) as *mut xl_heap_delete;
    let mut buffer: Buffer = 0;
    let page: Page;
    let mut lp: ItemId = null_mut();
    let htup: HeapTupleHeader;
    let mut blkno: BlockNumber = 0;
    let mut target_locator: RelFileLocator = core::mem::zeroed();
    let mut target_tid: ItemPointerData = core::mem::zeroed();

    XLogRecGetBlockTag(record, 0, &mut target_locator as *mut _ as *mut _, null_mut(), &mut blkno);
    ItemPointerSetBlockNumber(&mut target_tid, blkno);
    ItemPointerSetOffsetNumber(&mut target_tid, (*xlrec).offnum);

    /*
     * The visibility map may need to be fixed even if the heap page is
     * already up-to-date.
     */
    if (*xlrec).flags & XLH_DELETE_ALL_VISIBLE_CLEARED != 0 {
        let reln: Relation = CreateFakeRelcacheEntry(target_locator);
        let mut vmbuffer: Buffer = InvalidBuffer;

        visibilitymap_pin(reln, blkno, &mut vmbuffer);
        visibilitymap_clear(reln, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer) as Page;

        if PageGetMaxOffsetNumber(page) >= (*xlrec).offnum {
            lp = PageGetItemId(page, (*xlrec).offnum);
        }

        if PageGetMaxOffsetNumber(page) < (*xlrec).offnum || !ItemIdIsNormal(lp) {
            elog!(PANIC, "invalid lp");
        }

        htup = PageGetItem(page, lp) as HeapTupleHeader;

        (*htup).t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        (*htup).t_infomask2 &= !HEAP_KEYS_UPDATED;
        HeapTupleHeaderClearHotUpdated(htup);
        fix_infomask_from_infobits(
            (*xlrec).infobits_set,
            &mut (*htup).t_infomask,
            &mut (*htup).t_infomask2,
        );
        if !((*xlrec).flags & XLH_DELETE_IS_SUPER != 0) {
            HeapTupleHeaderSetXmax(htup, (*xlrec).xmax);
        } else {
            HeapTupleHeaderSetXmin(htup, InvalidTransactionId);
        }
        HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

        /* Mark the page as a candidate for pruning */
        PageSetPrunable(page, XLogRecGetXid(record));

        if (*xlrec).flags & XLH_DELETE_ALL_VISIBLE_CLEARED != 0 {
            PageClearAllVisible(page);
        }

        /* Make sure t_ctid is set correctly */
        if (*xlrec).flags & XLH_DELETE_IS_PARTITION_MOVE != 0 {
            HeapTupleHeaderSetMovedPartitions(htup);
        } else {
            (*htup).t_ctid = target_tid;
        }
        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

/*
 * Replay XLOG_HEAP_INSERT records.
 */
unsafe fn heap_xlog_insert(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_heap_insert = XLogRecGetData(record) as *mut xl_heap_insert;
    let mut buffer: Buffer = 0;
    let mut page: Page;
    // union { HeapTupleHeaderData hdr; char data[MaxHeapTupleSize]; } tbuf;
    let mut tbuf: [u8; MaxHeapTupleSize] = [0; MaxHeapTupleSize];
    let htup: HeapTupleHeader;
    let mut xlhdr: xl_heap_header = core::mem::zeroed();
    let mut newlen: uint32;
    let mut freespace: Size = 0;
    let mut target_locator: RelFileLocator = core::mem::zeroed();
    let mut blkno: BlockNumber = 0;
    let mut target_tid: ItemPointerData = core::mem::zeroed();
    let action: XLogRedoAction;

    XLogRecGetBlockTag(record, 0, &mut target_locator as *mut _ as *mut _, null_mut(), &mut blkno);
    ItemPointerSetBlockNumber(&mut target_tid, blkno);
    ItemPointerSetOffsetNumber(&mut target_tid, (*xlrec).offnum);

    /* No freezing in the heap_insert() code path */
    Assert!(!((*xlrec).flags & XLH_INSERT_ALL_FROZEN_SET != 0));

    /*
     * The visibility map may need to be fixed even if the heap page is
     * already up-to-date.
     */
    if (*xlrec).flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 {
        let reln: Relation = CreateFakeRelcacheEntry(target_locator);
        let mut vmbuffer: Buffer = InvalidBuffer;

        visibilitymap_pin(reln, blkno, &mut vmbuffer);
        visibilitymap_clear(reln, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    /*
     * If we inserted the first and only tuple on the page, re-initialize the
     * page from scratch.
     */
    if XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE != 0 {
        buffer = XLogInitBufferForRedo(record, 0);
        page = BufferGetPage(buffer) as Page;
        PageInit(page, BufferGetPageSize(buffer), 0);
        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, 0, &mut buffer);
    }
    if action == BLK_NEEDS_REDO {
        let mut datalen: Size = 0;
        let mut data: *mut c_char;

        page = BufferGetPage(buffer) as Page;

        if PageGetMaxOffsetNumber(page) + 1 < (*xlrec).offnum {
            elog!(PANIC, "invalid max offset number");
        }

        data = XLogRecGetBlockData(record, 0, &mut datalen);

        newlen = datalen as uint32 - SizeOfHeapHeader as uint32;
        Assert!(datalen > SizeOfHeapHeader && newlen as usize <= MaxHeapTupleSize);
        memcpy(
            &mut xlhdr as *mut _ as *mut c_void,
            data as *const c_void,
            SizeOfHeapHeader as usize,
        );
        data = data.add(SizeOfHeapHeader as usize);

        htup = tbuf.as_mut_ptr() as *mut HeapTupleHeaderData;
        MemSet(htup as *mut c_void, 0, SizeofHeapTupleHeader as Size);
        /* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
        memcpy(
            (htup as *mut c_char).add(SizeofHeapTupleHeader) as *mut c_void,
            data as *const c_void,
            newlen as usize,
        );
        newlen += SizeofHeapTupleHeader as uint32;
        (*htup).t_infomask2 = xlhdr.t_infomask2;
        (*htup).t_infomask = xlhdr.t_infomask;
        (*htup).t_hoff = xlhdr.t_hoff;
        HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
        HeapTupleHeaderSetCmin(htup, FirstCommandId);
        (*htup).t_ctid = target_tid;

        if PageAddItem(page, htup as Item, newlen as Size, (*xlrec).offnum, true, true)
            == InvalidOffsetNumber
        {
            elog!(PANIC, "failed to add tuple");
        }

        freespace = PageGetHeapFreeSpace(page); /* needed to update FSM below */

        PageSetLSN(page, lsn);

        if (*xlrec).flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 {
            PageClearAllVisible(page);
        }

        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    /*
     * If the page is running low on free space, update the FSM as well.
     * Arbitrarily, our definition of "low" is less than 20%. We can't do much
     * better than that without knowing the fill-factor for the table.
     *
     * XXX: Don't do this if the page was restored from full page image. We
     * don't bother to update the FSM in that case, it doesn't need to be
     * totally accurate anyway.
     */
    if action == BLK_NEEDS_REDO && freespace < (BLCKSZ / 5) as Size {
        XLogRecordPageWithFreeSpace(target_locator, blkno, freespace);
    }
}

/*
 * Replay XLOG_HEAP2_MULTI_INSERT records.
 */
unsafe fn heap_xlog_multi_insert(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_heap_multi_insert;
    let mut rlocator: RelFileLocator = core::mem::zeroed();
    let mut blkno: BlockNumber = 0;
    let mut buffer: Buffer = 0;
    let mut page: Page;
    // union { HeapTupleHeaderData hdr; char data[MaxHeapTupleSize]; } tbuf;
    let mut tbuf: [u8; MaxHeapTupleSize] = [0; MaxHeapTupleSize];
    let htup: HeapTupleHeader;
    let mut newlen: uint32;
    let mut freespace: Size = 0;
    let mut i: c_int;
    let isinit: bool = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;
    let action: XLogRedoAction;

    /*
     * Insertion doesn't overwrite MVCC data, so no conflict processing is
     * required.
     */
    xlrec = XLogRecGetData(record) as *mut xl_heap_multi_insert;

    XLogRecGetBlockTag(record, 0, &mut rlocator as *mut _ as *mut _, null_mut(), &mut blkno);

    /* check that the mutually exclusive flags are not both set */
    Assert!(
        !(((*xlrec).flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0)
            && ((*xlrec).flags & XLH_INSERT_ALL_FROZEN_SET != 0))
    );

    /*
     * The visibility map may need to be fixed even if the heap page is
     * already up-to-date.
     */
    if (*xlrec).flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 {
        let reln: Relation = CreateFakeRelcacheEntry(rlocator);
        let mut vmbuffer: Buffer = InvalidBuffer;

        visibilitymap_pin(reln, blkno, &mut vmbuffer);
        visibilitymap_clear(reln, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    if isinit {
        buffer = XLogInitBufferForRedo(record, 0);
        page = BufferGetPage(buffer) as Page;
        PageInit(page, BufferGetPageSize(buffer), 0);
        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, 0, &mut buffer);
    }
    if action == BLK_NEEDS_REDO {
        let mut tupdata: *mut c_char;
        let endptr: *mut c_char;
        let mut len: Size = 0;

        /* Tuples are stored as block data */
        tupdata = XLogRecGetBlockData(record, 0, &mut len);
        endptr = tupdata.add(len as usize);

        page = BufferGetPage(buffer) as Page;

        i = 0;
        while i < (*xlrec).ntuples as c_int {
            let mut offnum: OffsetNumber;
            let xlhdr: *mut xl_multi_insert_tuple;

            /*
             * If we're reinitializing the page, the tuples are stored in
             * order from FirstOffsetNumber. Otherwise there's an array of
             * offsets in the WAL record, and the tuples come after that.
             */
            if isinit {
                offnum = FirstOffsetNumber + i as OffsetNumber;
            } else {
                offnum = *(*xlrec).offsets.as_ptr().add(i as usize);
            }
            if PageGetMaxOffsetNumber(page) + 1 < offnum {
                elog!(PANIC, "invalid max offset number");
            }

            xlhdr = SHORTALIGN(tupdata as usize) as *mut xl_multi_insert_tuple;
            tupdata = (xlhdr as *mut c_char).add(SizeOfMultiInsertTuple as usize);

            newlen = (*xlhdr).datalen as uint32;
            Assert!(newlen as usize <= MaxHeapTupleSize);
            htup = tbuf.as_mut_ptr() as *mut HeapTupleHeaderData;
            MemSet(htup as *mut c_void, 0, SizeofHeapTupleHeader as Size);
            /* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
            memcpy(
                (htup as *mut c_char).add(SizeofHeapTupleHeader) as *mut c_void,
                tupdata as *const c_void,
                newlen as usize,
            );
            tupdata = tupdata.add(newlen as usize);

            newlen += SizeofHeapTupleHeader as uint32;
            (*htup).t_infomask2 = (*xlhdr).t_infomask2;
            (*htup).t_infomask = (*xlhdr).t_infomask;
            (*htup).t_hoff = (*xlhdr).t_hoff;
            HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
            HeapTupleHeaderSetCmin(htup, FirstCommandId);
            ItemPointerSetBlockNumber(&mut (*htup).t_ctid, blkno);
            ItemPointerSetOffsetNumber(&mut (*htup).t_ctid, offnum);

            offnum = PageAddItem(page, htup as Item, newlen as Size, offnum, true, true);
            if offnum == InvalidOffsetNumber {
                elog!(PANIC, "failed to add tuple");
            }
            i += 1;
        }
        if tupdata != endptr {
            elog!(PANIC, "total tuple length mismatch");
        }

        freespace = PageGetHeapFreeSpace(page); /* needed to update FSM below */

        PageSetLSN(page, lsn);

        if (*xlrec).flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 {
            PageClearAllVisible(page);
        }

        /* XLH_INSERT_ALL_FROZEN_SET implies that all tuples are visible */
        if (*xlrec).flags & XLH_INSERT_ALL_FROZEN_SET != 0 {
            PageSetAllVisible(page);
        }

        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    /*
     * If the page is running low on free space, update the FSM as well.
     * Arbitrarily, our definition of "low" is less than 20%. We can't do much
     * better than that without knowing the fill-factor for the table.
     *
     * XXX: Don't do this if the page was restored from full page image. We
     * don't bother to update the FSM in that case, it doesn't need to be
     * totally accurate anyway.
     */
    if action == BLK_NEEDS_REDO && freespace < (BLCKSZ / 5) as Size {
        XLogRecordPageWithFreeSpace(rlocator, blkno, freespace);
    }
}

/*
 * Replay XLOG_HEAP_UPDATE and XLOG_HEAP_HOT_UPDATE records.
 */
unsafe fn heap_xlog_update(record: *mut XLogReaderState, hot_update: bool) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_heap_update = XLogRecGetData(record) as *mut xl_heap_update;
    let mut rlocator: RelFileLocator = core::mem::zeroed();
    let mut oldblk: BlockNumber = 0;
    let mut newblk: BlockNumber = 0;
    let mut newtid: ItemPointerData = core::mem::zeroed();
    let mut obuffer: Buffer = 0;
    let mut nbuffer: Buffer;
    let mut page: Page;
    let mut offnum: OffsetNumber;
    let mut lp: ItemId = null_mut();
    let mut oldtup: HeapTupleData = core::mem::zeroed();
    let mut htup: HeapTupleHeader;
    let mut prefixlen: uint16 = 0;
    let mut suffixlen: uint16 = 0;
    let mut newp: *mut c_char;
    // union { HeapTupleHeaderData hdr; char data[MaxHeapTupleSize]; } tbuf;
    let mut tbuf: [u8; MaxHeapTupleSize] = [0; MaxHeapTupleSize];
    let mut xlhdr: xl_heap_header = core::mem::zeroed();
    let newlen: uint32;
    let mut freespace: Size = 0;
    let oldaction: XLogRedoAction;
    let newaction: XLogRedoAction;

    /* initialize to keep the compiler quiet */
    oldtup.t_data = null_mut();
    oldtup.t_len = 0;

    XLogRecGetBlockTag(record, 0, &mut rlocator as *mut _ as *mut _, null_mut(), &mut newblk);
    if XLogRecGetBlockTagExtended(record, 1, null_mut(), null_mut(), &mut oldblk, null_mut()) {
        /* HOT updates are never done across pages */
        Assert!(!hot_update);
    } else {
        oldblk = newblk;
    }

    ItemPointerSet(&mut newtid, newblk, (*xlrec).new_offnum);

    /*
     * The visibility map may need to be fixed even if the heap page is
     * already up-to-date.
     */
    if (*xlrec).flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED != 0 {
        let reln: Relation = CreateFakeRelcacheEntry(rlocator);
        let mut vmbuffer: Buffer = InvalidBuffer;

        visibilitymap_pin(reln, oldblk, &mut vmbuffer);
        visibilitymap_clear(reln, oldblk, vmbuffer, VISIBILITYMAP_VALID_BITS);
        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    /*
     * In normal operation, it is important to lock the two pages in
     * page-number order, to avoid possible deadlocks against other update
     * operations going the other way.  However, during WAL replay there can
     * be no other update happening, so we don't need to worry about that. But
     * we *do* need to worry that we don't expose an inconsistent state to Hot
     * Standby queries --- so the original page can't be unlocked before we've
     * added the new tuple to the new page.
     */

    /* Deal with old tuple version */
    oldaction = XLogReadBufferForRedo(
        record,
        if oldblk == newblk { 0 } else { 1 },
        &mut obuffer,
    );
    if oldaction == BLK_NEEDS_REDO {
        page = BufferGetPage(obuffer) as Page;
        offnum = (*xlrec).old_offnum;
        if PageGetMaxOffsetNumber(page) >= offnum {
            lp = PageGetItemId(page, offnum);
        }

        if PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp) {
            elog!(PANIC, "invalid lp");
        }

        htup = PageGetItem(page, lp) as HeapTupleHeader;

        oldtup.t_data = htup;
        oldtup.t_len = ItemIdGetLength(lp) as u32;

        (*htup).t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        (*htup).t_infomask2 &= !HEAP_KEYS_UPDATED;
        if hot_update {
            HeapTupleHeaderSetHotUpdated(htup);
        } else {
            HeapTupleHeaderClearHotUpdated(htup);
        }
        fix_infomask_from_infobits(
            (*xlrec).old_infobits_set,
            &mut (*htup).t_infomask,
            &mut (*htup).t_infomask2,
        );
        HeapTupleHeaderSetXmax(htup, (*xlrec).old_xmax);
        HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
        /* Set forward chain link in t_ctid */
        (*htup).t_ctid = newtid;

        /* Mark the page as a candidate for pruning */
        PageSetPrunable(page, XLogRecGetXid(record));

        if (*xlrec).flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED != 0 {
            PageClearAllVisible(page);
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(obuffer);
    }

    /*
     * Read the page the new tuple goes into, if different from old.
     */
    if oldblk == newblk {
        nbuffer = obuffer;
        newaction = oldaction;
    } else if XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE != 0 {
        nbuffer = XLogInitBufferForRedo(record, 0);
        page = BufferGetPage(nbuffer) as Page;
        PageInit(page, BufferGetPageSize(nbuffer), 0);
        newaction = BLK_NEEDS_REDO;
    } else {
        newaction = XLogReadBufferForRedo(record, 0, &mut nbuffer);
    }

    /*
     * The visibility map may need to be fixed even if the heap page is
     * already up-to-date.
     */
    if (*xlrec).flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED != 0 {
        let reln: Relation = CreateFakeRelcacheEntry(rlocator);
        let mut vmbuffer: Buffer = InvalidBuffer;

        visibilitymap_pin(reln, newblk, &mut vmbuffer);
        visibilitymap_clear(reln, newblk, vmbuffer, VISIBILITYMAP_VALID_BITS);
        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    /* Deal with new tuple */
    if newaction == BLK_NEEDS_REDO {
        let mut recdata: *mut c_char;
        let recdata_end: *mut c_char;
        let mut datalen: Size = 0;
        let tuplen: Size;

        recdata = XLogRecGetBlockData(record, 0, &mut datalen);
        recdata_end = recdata.add(datalen as usize);

        page = BufferGetPage(nbuffer) as Page;

        offnum = (*xlrec).new_offnum;
        if PageGetMaxOffsetNumber(page) + 1 < offnum {
            elog!(PANIC, "invalid max offset number");
        }

        if (*xlrec).flags & XLH_UPDATE_PREFIX_FROM_OLD != 0 {
            Assert!(newblk == oldblk);
            memcpy(
                &mut prefixlen as *mut _ as *mut c_void,
                recdata as *const c_void,
                core::mem::size_of::<uint16>(),
            );
            recdata = recdata.add(core::mem::size_of::<uint16>());
        }
        if (*xlrec).flags & XLH_UPDATE_SUFFIX_FROM_OLD != 0 {
            Assert!(newblk == oldblk);
            memcpy(
                &mut suffixlen as *mut _ as *mut c_void,
                recdata as *const c_void,
                core::mem::size_of::<uint16>(),
            );
            recdata = recdata.add(core::mem::size_of::<uint16>());
        }

        memcpy(
            &mut xlhdr as *mut _ as *mut c_void,
            recdata as *const c_void,
            SizeOfHeapHeader as usize,
        );
        recdata = recdata.add(SizeOfHeapHeader as usize);

        tuplen = (recdata_end as usize - recdata as usize) as Size;
        Assert!(tuplen as usize <= MaxHeapTupleSize);

        htup = tbuf.as_mut_ptr() as *mut HeapTupleHeaderData;
        MemSet(htup as *mut c_void, 0, SizeofHeapTupleHeader as Size);

        /*
         * Reconstruct the new tuple using the prefix and/or suffix from the
         * old tuple, and the data stored in the WAL record.
         */
        newp = (htup as *mut c_char).add(SizeofHeapTupleHeader);
        if prefixlen > 0 {
            let mut len: c_int;

            /* copy bitmap [+ padding] [+ oid] from WAL record */
            len = xlhdr.t_hoff as c_int - SizeofHeapTupleHeader as c_int;
            memcpy(newp as *mut c_void, recdata as *const c_void, len as usize);
            recdata = recdata.add(len as usize);
            newp = newp.add(len as usize);

            /* copy prefix from old tuple */
            memcpy(
                newp as *mut c_void,
                (oldtup.t_data as *mut c_char).add((*oldtup.t_data).t_hoff as usize)
                    as *const c_void,
                prefixlen as usize,
            );
            newp = newp.add(prefixlen as usize);

            /* copy new tuple data from WAL record */
            len = tuplen as c_int - (xlhdr.t_hoff as c_int - SizeofHeapTupleHeader as c_int);
            memcpy(newp as *mut c_void, recdata as *const c_void, len as usize);
            recdata = recdata.add(len as usize);
            newp = newp.add(len as usize);
        } else {
            /*
             * copy bitmap [+ padding] [+ oid] + data from record, all in one
             * go
             */
            memcpy(newp as *mut c_void, recdata as *const c_void, tuplen as usize);
            recdata = recdata.add(tuplen as usize);
            newp = newp.add(tuplen as usize);
        }
        Assert!(recdata == recdata_end);

        /* copy suffix from old tuple */
        if suffixlen > 0 {
            memcpy(
                newp as *mut c_void,
                (oldtup.t_data as *mut c_char)
                    .add(oldtup.t_len as usize - suffixlen as usize) as *const c_void,
                suffixlen as usize,
            );
        }

        newlen = SizeofHeapTupleHeader as uint32 + tuplen as uint32 + prefixlen as uint32
            + suffixlen as uint32;
        (*htup).t_infomask2 = xlhdr.t_infomask2;
        (*htup).t_infomask = xlhdr.t_infomask;
        (*htup).t_hoff = xlhdr.t_hoff;

        HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
        HeapTupleHeaderSetCmin(htup, FirstCommandId);
        HeapTupleHeaderSetXmax(htup, (*xlrec).new_xmax);
        /* Make sure there is no forward chain link in t_ctid */
        (*htup).t_ctid = newtid;

        offnum = PageAddItem(page, htup as Item, newlen as Size, offnum, true, true);
        if offnum == InvalidOffsetNumber {
            elog!(PANIC, "failed to add tuple");
        }

        if (*xlrec).flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED != 0 {
            PageClearAllVisible(page);
        }

        freespace = PageGetHeapFreeSpace(page); /* needed to update FSM below */

        PageSetLSN(page, lsn);
        MarkBufferDirty(nbuffer);
    }

    if BufferIsValid(nbuffer) && nbuffer != obuffer {
        UnlockReleaseBuffer(nbuffer);
    }
    if BufferIsValid(obuffer) {
        UnlockReleaseBuffer(obuffer);
    }

    /*
     * If the new page is running low on free space, update the FSM as well.
     * Arbitrarily, our definition of "low" is less than 20%. We can't do much
     * better than that without knowing the fill-factor for the table.
     *
     * However, don't update the FSM on HOT updates, because after crash
     * recovery, either the old or the new tuple will certainly be dead and
     * prunable. After pruning, the page will have roughly as much free space
     * as it did before the update, assuming the new tuple is about the same
     * size as the old one.
     *
     * XXX: Don't do this if the page was restored from full page image. We
     * don't bother to update the FSM in that case, it doesn't need to be
     * totally accurate anyway.
     */
    if newaction == BLK_NEEDS_REDO && !hot_update && freespace < (BLCKSZ / 5) as Size {
        XLogRecordPageWithFreeSpace(rlocator, newblk, freespace);
    }
}

/*
 * Replay XLOG_HEAP_CONFIRM records.
 */
unsafe fn heap_xlog_confirm(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_heap_confirm = XLogRecGetData(record) as *mut xl_heap_confirm;
    let mut buffer: Buffer = 0;
    let page: Page;
    let offnum: OffsetNumber;
    let mut lp: ItemId = null_mut();
    let htup: HeapTupleHeader;

    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer) as Page;

        offnum = (*xlrec).offnum;
        if PageGetMaxOffsetNumber(page) >= offnum {
            lp = PageGetItemId(page, offnum);
        }

        if PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp) {
            elog!(PANIC, "invalid lp");
        }

        htup = PageGetItem(page, lp) as HeapTupleHeader;

        /*
         * Confirm tuple as actually inserted
         */
        ItemPointerSet(&mut (*htup).t_ctid, BufferGetBlockNumber(buffer), offnum);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

/*
 * Replay XLOG_HEAP_LOCK records.
 */
unsafe fn heap_xlog_lock(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_heap_lock = XLogRecGetData(record) as *mut xl_heap_lock;
    let mut buffer: Buffer = 0;
    let page: Page;
    let offnum: OffsetNumber;
    let mut lp: ItemId = null_mut();
    let htup: HeapTupleHeader;

    /*
     * The visibility map may need to be fixed even if the heap page is
     * already up-to-date.
     */
    if (*xlrec).flags & XLH_LOCK_ALL_FROZEN_CLEARED != 0 {
        let mut rlocator: RelFileLocator = core::mem::zeroed();
        let mut vmbuffer: Buffer = InvalidBuffer;
        let mut block: BlockNumber = 0;
        let reln: Relation;

        XLogRecGetBlockTag(record, 0, &mut rlocator as *mut _ as *mut _, null_mut(), &mut block);
        reln = CreateFakeRelcacheEntry(rlocator);

        visibilitymap_pin(reln, block, &mut vmbuffer);
        visibilitymap_clear(reln, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN);

        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer) as Page;

        offnum = (*xlrec).offnum;
        if PageGetMaxOffsetNumber(page) >= offnum {
            lp = PageGetItemId(page, offnum);
        }

        if PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp) {
            elog!(PANIC, "invalid lp");
        }

        htup = PageGetItem(page, lp) as HeapTupleHeader;

        (*htup).t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        (*htup).t_infomask2 &= !HEAP_KEYS_UPDATED;
        fix_infomask_from_infobits(
            (*xlrec).infobits_set,
            &mut (*htup).t_infomask,
            &mut (*htup).t_infomask2,
        );

        /*
         * Clear relevant update flags, but only if the modified infomask says
         * there's no update.
         */
        if HEAP_XMAX_IS_LOCKED_ONLY((*htup).t_infomask) {
            HeapTupleHeaderClearHotUpdated(htup);
            /* Make sure there is no forward chain link in t_ctid */
            ItemPointerSet(&mut (*htup).t_ctid, BufferGetBlockNumber(buffer), offnum);
        }
        HeapTupleHeaderSetXmax(htup, (*xlrec).xmax);
        HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

/*
 * Replay XLOG_HEAP2_LOCK_UPDATED records.
 */
unsafe fn heap_xlog_lock_updated(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_heap_lock_updated;
    let mut buffer: Buffer = 0;
    let page: Page;
    let offnum: OffsetNumber;
    let mut lp: ItemId = null_mut();
    let htup: HeapTupleHeader;

    xlrec = XLogRecGetData(record) as *mut xl_heap_lock_updated;

    /*
     * The visibility map may need to be fixed even if the heap page is
     * already up-to-date.
     */
    if (*xlrec).flags & XLH_LOCK_ALL_FROZEN_CLEARED != 0 {
        let mut rlocator: RelFileLocator = core::mem::zeroed();
        let mut vmbuffer: Buffer = InvalidBuffer;
        let mut block: BlockNumber = 0;
        let reln: Relation;

        XLogRecGetBlockTag(record, 0, &mut rlocator as *mut _ as *mut _, null_mut(), &mut block);
        reln = CreateFakeRelcacheEntry(rlocator);

        visibilitymap_pin(reln, block, &mut vmbuffer);
        visibilitymap_clear(reln, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN);

        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        page = BufferGetPage(buffer) as Page;

        offnum = (*xlrec).offnum;
        if PageGetMaxOffsetNumber(page) >= offnum {
            lp = PageGetItemId(page, offnum);
        }

        if PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp) {
            elog!(PANIC, "invalid lp");
        }

        htup = PageGetItem(page, lp) as HeapTupleHeader;

        (*htup).t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        (*htup).t_infomask2 &= !HEAP_KEYS_UPDATED;
        fix_infomask_from_infobits(
            (*xlrec).infobits_set,
            &mut (*htup).t_infomask,
            &mut (*htup).t_infomask2,
        );
        HeapTupleHeaderSetXmax(htup, (*xlrec).xmax);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }
}

/*
 * Replay XLOG_HEAP_INPLACE records.
 */
unsafe fn heap_xlog_inplace(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = (*record).EndRecPtr;
    let xlrec: *mut xl_heap_inplace = XLogRecGetData(record) as *mut xl_heap_inplace;
    let mut buffer: Buffer = 0;
    let page: Page;
    let offnum: OffsetNumber;
    let mut lp: ItemId = null_mut();
    let htup: HeapTupleHeader;
    let oldlen: uint32;
    let mut newlen: Size = 0;

    if XLogReadBufferForRedo(record, 0, &mut buffer) == BLK_NEEDS_REDO {
        let newtup: *mut c_char = XLogRecGetBlockData(record, 0, &mut newlen);

        page = BufferGetPage(buffer) as Page;

        offnum = (*xlrec).offnum;
        if PageGetMaxOffsetNumber(page) >= offnum {
            lp = PageGetItemId(page, offnum);
        }

        if PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp) {
            elog!(PANIC, "invalid lp");
        }

        htup = PageGetItem(page, lp) as HeapTupleHeader;

        oldlen = ItemIdGetLength(lp) - (*htup).t_hoff as uint32;
        if oldlen as Size != newlen {
            elog!(PANIC, "wrong tuple length");
        }

        memcpy(
            (htup as *mut c_char).add((*htup).t_hoff as usize) as *mut c_void,
            newtup as *const c_void,
            newlen as usize,
        );

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }
    if BufferIsValid(buffer) {
        UnlockReleaseBuffer(buffer);
    }

    ProcessCommittedInvalidationMessages(
        (*xlrec).msgs.as_mut_ptr(),
        (*xlrec).nmsgs,
        (*xlrec).relcacheInitFileInval,
        (*xlrec).dbId,
        (*xlrec).tsId,
    );
}

pub unsafe fn heap_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    /*
     * These operations don't overwrite MVCC data so no conflict processing is
     * required. The ones in heap2 rmgr do.
     */

    match info & XLOG_HEAP_OPMASK {
        XLOG_HEAP_INSERT => {
            heap_xlog_insert(record);
        }
        XLOG_HEAP_DELETE => {
            heap_xlog_delete(record);
        }
        XLOG_HEAP_UPDATE => {
            heap_xlog_update(record, false);
        }
        XLOG_HEAP_TRUNCATE => {
            /*
             * TRUNCATE is a no-op because the actions are already logged as
             * SMGR WAL records.  TRUNCATE WAL record only exists for logical
             * decoding.
             */
        }
        XLOG_HEAP_HOT_UPDATE => {
            heap_xlog_update(record, true);
        }
        XLOG_HEAP_CONFIRM => {
            heap_xlog_confirm(record);
        }
        XLOG_HEAP_LOCK => {
            heap_xlog_lock(record);
        }
        XLOG_HEAP_INPLACE => {
            heap_xlog_inplace(record);
        }
        _ => {
            elog!(PANIC, "heap_redo: unknown op code {}", info);
        }
    }
}

pub unsafe fn heap2_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    match info & XLOG_HEAP_OPMASK {
        XLOG_HEAP2_PRUNE_ON_ACCESS | XLOG_HEAP2_PRUNE_VACUUM_SCAN
        | XLOG_HEAP2_PRUNE_VACUUM_CLEANUP => {
            heap_xlog_prune_freeze(record);
        }
        XLOG_HEAP2_VISIBLE => {
            heap_xlog_visible(record);
        }
        XLOG_HEAP2_MULTI_INSERT => {
            heap_xlog_multi_insert(record);
        }
        XLOG_HEAP2_LOCK_UPDATED => {
            heap_xlog_lock_updated(record);
        }
        XLOG_HEAP2_NEW_CID => {
            /*
             * Nothing to do on a real replay, only used during logical
             * decoding.
             */
        }
        XLOG_HEAP2_REWRITE => {
            heap_xlog_logical_rewrite(record);
        }
        _ => {
            elog!(PANIC, "heap2_redo: unknown op code {}", info);
        }
    }
}

/*
 * Mask a heap page before performing consistency checks on it.
 */
pub unsafe fn heap_mask(pagedata: *mut c_char, blkno: BlockNumber) {
    let page: Page = pagedata as Page;
    let mut off: OffsetNumber;

    mask_page_lsn_and_checksum(page);

    mask_page_hint_bits(page);
    mask_unused_space(page);

    off = 1;
    while off <= PageGetMaxOffsetNumber(page) {
        let iid: ItemId = PageGetItemId(page, off);
        let page_item: *mut c_char;

        page_item = (page as *mut c_char).add(ItemIdGetOffset(iid) as usize);

        if ItemIdIsNormal(iid) {
            let page_htup: HeapTupleHeader = page_item as HeapTupleHeader;

            /*
             * If xmin of a tuple is not yet frozen, we should ignore
             * differences in hint bits, since they can be set without
             * emitting WAL.
             */
            if !HeapTupleHeaderXminFrozen(page_htup) {
                (*page_htup).t_infomask &= !HEAP_XACT_MASK;
            } else {
                /* Still we need to mask xmax hint bits. */
                (*page_htup).t_infomask &= !HEAP_XMAX_INVALID;
                (*page_htup).t_infomask &= !HEAP_XMAX_COMMITTED;
            }

            /*
             * During replay, we set Command Id to FirstCommandId. Hence, mask
             * it. See heap_xlog_insert() for details.
             */
            (*page_htup).t_choice.t_heap.t_field3.t_cid = MASK_MARKER as CommandId;

            /*
             * For a speculative tuple, heap_insert() does not set ctid in the
             * caller-passed heap tuple itself, leaving the ctid field to
             * contain a speculative token value - a per-backend monotonically
             * increasing identifier. Besides, it does not WAL-log ctid under
             * any circumstances.
             *
             * During redo, heap_xlog_insert() sets t_ctid to current block
             * number and self offset number. It doesn't care about any
             * speculative insertions on the primary. Hence, we set t_ctid to
             * current block number and self offset number to ignore any
             * inconsistency.
             */
            if HeapTupleHeaderIsSpeculative(page_htup) {
                ItemPointerSet(&mut (*page_htup).t_ctid, blkno, off);
            }

            /*
             * NB: Not ignoring ctid changes due to the tuple having moved
             * (i.e. HeapTupleHeaderIndicatesMovedPartitions), because that's
             * important information that needs to be in-sync between primary
             * and standby, and thus is WAL logged.
             */
        }

        /*
         * Ignore any padding bytes after the tuple, when the length of the
         * item is not MAXALIGNed.
         */
        if ItemIdHasStorage(iid) {
            let len: c_int = ItemIdGetLength(iid) as c_int;
            let padlen: c_int = MAXALIGN(len as usize) as c_int - len;

            if padlen > 0 {
                memset(page_item.add(len as usize) as *mut c_void, MASK_MARKER, padlen as usize);
            }
        }

        off += 1;
    }
}
