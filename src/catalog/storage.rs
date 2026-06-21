//! src/backend/catalog/storage.c
//!   code to create and destroy physical storage for relations
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! NOTES
//!   Some of this code used to be in storage/smgr/smgr.c, and the
//!   function names still reflect that.
//!
//! Merged header: src/include/catalog/storage.h

use crate::prelude::*;
use crate::access::transam::xlogdefs::XLogRecPtr;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{uint64, uint8, Size};
use crate::common::relpath::ForkNumber;
use crate::storage::block::BlockNumber;
use crate::storage::relfilelocator::{RelFileLocator, RelFileLocatorBackend};

// ----------------------------------------------------------------------------
// Stub types for dependencies not yet ported.
// ----------------------------------------------------------------------------

pub type SMgrRelation = *mut c_void;
pub type Relation = *mut c_void;
pub type HTAB = c_void;
pub type ProcNumber = c_int;
pub type Page = *mut c_void;
pub type BulkWriteState = c_void;
pub type BulkWriteBuffer = *mut c_void;
pub type XLogReaderState = c_void;
pub type RelPathStr = [c_char; 1024];

// ----------------------------------------------------------------------------
// Constants from various headers.
// ----------------------------------------------------------------------------

const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char;
const RELPERSISTENCE_UNLOGGED: c_char = b'u' as c_char;
const RELPERSISTENCE_TEMP: c_char = b't' as c_char;

const INVALID_PROC_NUMBER: ProcNumber = -1;

const MAIN_FORKNUM: ForkNumber = 0;
const FSM_FORKNUM: ForkNumber = 1;
const VISIBILITYMAP_FORKNUM: ForkNumber = 2;
const INIT_FORKNUM: ForkNumber = 3;
const MAX_FORKNUM: ForkNumber = INIT_FORKNUM;

const InvalidBlockNumber: BlockNumber = 0xFFFFFFFF;
const BLCKSZ: u64 = 8192;

const ERROR: c_int = 21;
const PANIC: c_int = 23;

const HASH_ELEM: c_int = 0x0008;
const HASH_BLOBS: c_int = 0x0020;
const HASH_CONTEXT: c_int = 0x0400;

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum HASHACTION {
    HASH_FIND = 0,
    HASH_ENTER,
    HASH_REMOVE,
    HASH_ENTER_NULL,
}
use HASHACTION::*;

// XLog / rmgr constants
const RM_SMGR_ID: u8 = 11;
const XLOG_SMGR_CREATE: uint8 = 0x10;
const XLOG_SMGR_TRUNCATE: uint8 = 0x20;
const XLR_SPECIAL_REL_UPDATE: uint8 = 0x01;
const XLR_INFO_MASK: uint8 = 0x0F;

const SMGR_TRUNCATE_HEAP: c_int = 0x0001;
const SMGR_TRUNCATE_VM: c_int = 0x0002;
const SMGR_TRUNCATE_FSM: c_int = 0x0004;
const SMGR_TRUNCATE_ALL: c_int = SMGR_TRUNCATE_HEAP | SMGR_TRUNCATE_VM | SMGR_TRUNCATE_FSM;

// PageIsVerified flags
const PIV_LOG_WARNING: c_int = 1 << 0;
const PIV_IGNORE_CHECKSUM_FAILURE: c_int = 1 << 1;

// DelayChkpt flags
const DELAY_CHKPT_START: c_int = 1 << 0;
const DELAY_CHKPT_COMPLETE: c_int = 1 << 1;

const ERRCODE_DATA_CORRUPTED: c_int = 0;

// ----------------------------------------------------------------------------
// xlog record structs from catalog/storage_xlog.h
// ----------------------------------------------------------------------------

#[repr(C)]
struct xl_smgr_create {
    rlocator: RelFileLocator,
    forkNum: ForkNumber,
}

#[repr(C)]
struct xl_smgr_truncate {
    blkno: BlockNumber,
    rlocator: RelFileLocator,
    flags: c_int,
}

// ----------------------------------------------------------------------------
// HASHCTL / HASH_SEQ_STATUS stubs (utils/hsearch.h)
// ----------------------------------------------------------------------------

#[repr(C)]
struct HASHCTL {
    keysize: Size,
    entrysize: Size,
    hcxt: *mut c_void, // MemoryContext
    // other fields omitted
    _pad: [u8; 64],
}

impl HASHCTL {
    fn zeroed() -> Self {
        unsafe { core::mem::zeroed() }
    }
}

#[repr(C)]
struct HASH_SEQ_STATUS {
    _opaque: [u8; 32],
}

impl HASH_SEQ_STATUS {
    fn zeroed() -> Self {
        unsafe { core::mem::zeroed() }
    }
}

// ----------------------------------------------------------------------------
// GUC variables
// ----------------------------------------------------------------------------

/// in kilobytes
#[no_mangle]
pub static mut wal_skip_threshold: c_int = 2048;

/// from bufpage / GUC
static mut ignore_checksum_failure: bool = false;

// ----------------------------------------------------------------------------
// Pending-relation lists
// ----------------------------------------------------------------------------

#[repr(C)]
struct PendingRelDelete {
    rlocator: RelFileLocator,      // relation that may need to be deleted
    procNumber: ProcNumber,        // INVALID_PROC_NUMBER if not a temp rel
    atCommit: bool,                // T=delete at commit; F=delete at abort
    nestLevel: c_int,              // xact nesting level of request
    next: *mut PendingRelDelete,   // linked-list link
}

#[repr(C)]
struct PendingRelSync {
    rlocator: RelFileLocator,
    is_truncated: bool, // Has the file experienced truncation?
}

static mut pendingDeletes: *mut PendingRelDelete = std::ptr::null_mut(); // head of linked list
static mut pendingSyncHash: *mut HTAB = std::ptr::null_mut();

// ----------------------------------------------------------------------------
// AddPendingSync
//   Queue an at-commit fsync.
// ----------------------------------------------------------------------------

unsafe fn AddPendingSync(rlocator: *const RelFileLocator) {
    let pending: *mut PendingRelSync;
    let mut found: bool = false;

    /* create the hash if not yet */
    if pendingSyncHash.is_null() {
        let mut ctl: HASHCTL = HASHCTL::zeroed();

        ctl.keysize = core::mem::size_of::<RelFileLocator>();
        ctl.entrysize = core::mem::size_of::<PendingRelSync>();
        ctl.hcxt = TopTransactionContext;
        pendingSyncHash = hash_create(
            c"pending sync hash".as_ptr(),
            16,
            &mut ctl,
            HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
        );
    }

    pending = hash_search(
        pendingSyncHash,
        rlocator as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut PendingRelSync;
    Assert(!found);
    (*pending).is_truncated = false;
}

// ----------------------------------------------------------------------------
// RelationCreateStorage
//   Create physical storage for a relation.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RelationCreateStorage(
    rlocator: RelFileLocator,
    relpersistence: c_char,
    register_delete: bool,
) -> SMgrRelation {
    let srel: SMgrRelation;
    let procNumber: ProcNumber;
    let needs_wal: bool;

    Assert(!IsInParallelMode()); /* couldn't update pendingSyncHash */

    if relpersistence == RELPERSISTENCE_TEMP {
        procNumber = ProcNumberForTempRelations();
        needs_wal = false;
    } else if relpersistence == RELPERSISTENCE_UNLOGGED {
        procNumber = INVALID_PROC_NUMBER;
        needs_wal = false;
    } else if relpersistence == RELPERSISTENCE_PERMANENT {
        procNumber = INVALID_PROC_NUMBER;
        needs_wal = true;
    } else {
        elog!(ERROR, "invalid relpersistence: {}", relpersistence as u8 as char);
        return std::ptr::null_mut(); /* placate compiler */
    }

    srel = smgropen(rlocator, procNumber);
    smgrcreate(srel, MAIN_FORKNUM, false);

    if needs_wal {
        log_smgrcreate(smgr_rlocator_locator(srel), MAIN_FORKNUM);
    }

    /*
     * Add the relation to the list of stuff to delete at abort, if we are
     * asked to do so.
     */
    if register_delete {
        let pending: *mut PendingRelDelete;

        pending = MemoryContextAlloc(
            TopMemoryContext as *mut _,
            core::mem::size_of::<PendingRelDelete>(),
        ) as *mut PendingRelDelete;
        (*pending).rlocator = rlocator;
        (*pending).procNumber = procNumber;
        (*pending).atCommit = false; /* delete if abort */
        (*pending).nestLevel = GetCurrentTransactionNestLevel();
        (*pending).next = pendingDeletes;
        pendingDeletes = pending;
    }

    if relpersistence == RELPERSISTENCE_PERMANENT && !XLogIsNeeded() {
        Assert(procNumber == INVALID_PROC_NUMBER);
        AddPendingSync(&rlocator);
    }

    srel
}

// ----------------------------------------------------------------------------
// Perform XLogInsert of an XLOG_SMGR_CREATE record to WAL.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn log_smgrcreate(rlocator: *const RelFileLocator, forkNum: ForkNumber) {
    let mut xlrec: xl_smgr_create = core::mem::zeroed();

    /*
     * Make an XLOG entry reporting the file creation.
     */
    xlrec.rlocator = *rlocator;
    xlrec.forkNum = forkNum;

    XLogBeginInsert();
    XLogRegisterData(
        &mut xlrec as *mut _ as *mut c_char,
        core::mem::size_of::<xl_smgr_create>(),
    );
    XLogInsert(RM_SMGR_ID, XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE);
}

// ----------------------------------------------------------------------------
// RelationDropStorage
//   Schedule unlinking of physical storage at transaction commit.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RelationDropStorage(rel: Relation) {
    let pending: *mut PendingRelDelete;

    /* Add the relation to the list of stuff to delete at commit */
    pending = MemoryContextAlloc(
        TopMemoryContext as *mut _,
        core::mem::size_of::<PendingRelDelete>(),
    ) as *mut PendingRelDelete;
    (*pending).rlocator = rel_rd_locator(rel);
    (*pending).procNumber = rel_rd_backend(rel);
    (*pending).atCommit = true; /* delete if commit */
    (*pending).nestLevel = GetCurrentTransactionNestLevel();
    (*pending).next = pendingDeletes;
    pendingDeletes = pending;

    /*
     * NOTE: if the relation was created in this transaction, it will now be
     * present in the pending-delete list twice, once with atCommit true and
     * once with atCommit false.  Hence, it will be physically deleted at end
     * of xact in either case (and the other entry will be ignored by
     * smgrDoPendingDeletes, so no error will occur).  We could instead remove
     * the existing list entry and delete the physical file immediately, but
     * for now I'll keep the logic simple.
     */

    RelationCloseSmgr(rel);
}

// ----------------------------------------------------------------------------
// RelationPreserveStorage
//   Mark a relation as not to be deleted after all.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RelationPreserveStorage(rlocator: RelFileLocator, atCommit: bool) {
    let mut pending: *mut PendingRelDelete;
    let mut prev: *mut PendingRelDelete;
    let mut next: *mut PendingRelDelete;

    prev = std::ptr::null_mut();
    pending = pendingDeletes;
    while !pending.is_null() {
        next = (*pending).next;
        if RelFileLocatorEquals(rlocator, (*pending).rlocator) && (*pending).atCommit == atCommit {
            /* unlink and delete list entry */
            if !prev.is_null() {
                (*prev).next = next;
            } else {
                pendingDeletes = next;
            }
            pfree(pending as *mut c_void);
            /* prev does not change */
        } else {
            /* unrelated entry, don't touch it */
            prev = pending;
        }
        pending = next;
    }
}

// ----------------------------------------------------------------------------
// RelationTruncate
//   Physically truncate a relation to the specified number of blocks.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RelationTruncate(rel: Relation, nblocks: BlockNumber) {
    let fsm: bool;
    let vm: bool;
    let mut need_fsm_vacuum: bool = false;
    let mut forks: [ForkNumber; MAX_FORKNUM as usize] = [0; MAX_FORKNUM as usize];
    let mut old_blocks: [BlockNumber; MAX_FORKNUM as usize] = [0; MAX_FORKNUM as usize];
    let mut blocks: [BlockNumber; MAX_FORKNUM as usize] = [0; MAX_FORKNUM as usize];
    let mut nforks: c_int = 0;
    let reln: SMgrRelation;

    /*
     * Make sure smgr_targblock etc aren't pointing somewhere past new end.
     * (Note: don't rely on this reln pointer below this loop.)
     */
    reln = RelationGetSmgr(rel);
    smgr_set_targblock(reln, InvalidBlockNumber);
    for i in 0..=(MAX_FORKNUM as usize) {
        smgr_set_cached_nblocks(reln, i, InvalidBlockNumber);
    }

    /* Prepare for truncation of MAIN fork of the relation */
    forks[nforks as usize] = MAIN_FORKNUM;
    old_blocks[nforks as usize] = smgrnblocks(reln, MAIN_FORKNUM);
    blocks[nforks as usize] = nblocks;
    nforks += 1;

    /* Prepare for truncation of the FSM if it exists */
    fsm = smgrexists(RelationGetSmgr(rel), FSM_FORKNUM);
    if fsm {
        blocks[nforks as usize] = FreeSpaceMapPrepareTruncateRel(rel, nblocks);
        if BlockNumberIsValid(blocks[nforks as usize]) {
            forks[nforks as usize] = FSM_FORKNUM;
            old_blocks[nforks as usize] = smgrnblocks(reln, FSM_FORKNUM);
            nforks += 1;
            need_fsm_vacuum = true;
        }
    }

    /* Prepare for truncation of the visibility map too if it exists */
    vm = smgrexists(RelationGetSmgr(rel), VISIBILITYMAP_FORKNUM);
    if vm {
        blocks[nforks as usize] = visibilitymap_prepare_truncate(rel, nblocks);
        if BlockNumberIsValid(blocks[nforks as usize]) {
            forks[nforks as usize] = VISIBILITYMAP_FORKNUM;
            old_blocks[nforks as usize] = smgrnblocks(reln, VISIBILITYMAP_FORKNUM);
            nforks += 1;
        }
    }

    RelationPreTruncate(rel);

    /*
     * The code which follows can interact with concurrent checkpoints in two
     * separate ways.  (See C source for full discussion.)
     */
    Assert((MyProc_delayChkptFlags() & (DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE)) == 0);
    MyProc_set_delayChkptFlags(MyProc_delayChkptFlags() | DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE);

    /*
     * We WAL-log the truncation first and then truncate in a critical
     * section.  (See C source for full discussion.)
     */
    START_CRIT_SECTION();

    if RelationNeedsWAL(rel) {
        /*
         * Make an XLOG entry reporting the file truncation.
         */
        let lsn: XLogRecPtr;
        let mut xlrec: xl_smgr_truncate = core::mem::zeroed();

        xlrec.blkno = nblocks;
        xlrec.rlocator = rel_rd_locator(rel);
        xlrec.flags = SMGR_TRUNCATE_ALL;

        XLogBeginInsert();
        XLogRegisterData(
            &mut xlrec as *mut _ as *mut c_char,
            core::mem::size_of::<xl_smgr_truncate>(),
        );

        lsn = XLogInsert(RM_SMGR_ID, XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE);

        /*
         * Flush, because otherwise the truncation of the main relation might
         * hit the disk before the WAL record, and the truncation of the FSM
         * or visibility map.
         */
        XLogFlush(lsn);
    }

    /*
     * This will first remove any buffers from the buffer pool that should no
     * longer exist after truncation is complete, and then truncate the
     * corresponding files on disk.
     */
    smgrtruncate(
        RelationGetSmgr(rel),
        forks.as_mut_ptr(),
        nforks,
        old_blocks.as_mut_ptr(),
        blocks.as_mut_ptr(),
    );

    END_CRIT_SECTION();

    /* We've done all the critical work, so checkpoints are OK now. */
    MyProc_set_delayChkptFlags(
        MyProc_delayChkptFlags() & !(DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE),
    );

    /*
     * Update upper-level FSM pages to account for the truncation.
     */
    if need_fsm_vacuum {
        FreeSpaceMapVacuumRange(rel, nblocks, InvalidBlockNumber);
    }
}

// ----------------------------------------------------------------------------
// RelationPreTruncate
//   Perform AM-independent work before a physical truncation.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RelationPreTruncate(rel: Relation) {
    let pending: *mut PendingRelSync;

    if pendingSyncHash.is_null() {
        return;
    }

    pending = hash_search(
        pendingSyncHash,
        smgr_rlocator_locator(RelationGetSmgr(rel)) as *const c_void,
        HASH_FIND,
        std::ptr::null_mut(),
    ) as *mut PendingRelSync;
    if !pending.is_null() {
        (*pending).is_truncated = true;
    }
}

// ----------------------------------------------------------------------------
// RelationCopyStorage
//   Copy a fork's data, block by block.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RelationCopyStorage(
    src: SMgrRelation,
    dst: SMgrRelation,
    forkNum: ForkNumber,
    relpersistence: c_char,
) {
    let use_wal: bool;
    let copying_initfork: bool;
    let nblocks: BlockNumber;
    let mut blkno: BlockNumber;
    let bulkstate: *mut BulkWriteState;

    /*
     * The init fork for an unlogged relation in many respects has to be
     * treated the same as normal relation, changes need to be WAL logged and
     * it needs to be synced to disk.
     */
    copying_initfork =
        relpersistence == RELPERSISTENCE_UNLOGGED && forkNum == INIT_FORKNUM;

    /*
     * We need to log the copied data in WAL iff WAL archiving/streaming is
     * enabled AND it's a permanent relation.
     */
    use_wal = XLogIsNeeded()
        && (relpersistence == RELPERSISTENCE_PERMANENT || copying_initfork);

    bulkstate = smgr_bulk_start_smgr(dst, forkNum, use_wal);

    nblocks = smgrnblocks(src, forkNum);

    blkno = 0;
    while blkno < nblocks {
        let buf: BulkWriteBuffer;
        let mut piv_flags: c_int;
        let mut checksum_failure: bool = false;
        let verified: bool;

        /* If we got a cancel signal during the copy of the data, quit */
        CHECK_FOR_INTERRUPTS();

        buf = smgr_bulk_get_buf(bulkstate);
        smgrread(src, forkNum, blkno, buf as Page);

        piv_flags = PIV_LOG_WARNING;
        if ignore_checksum_failure {
            piv_flags |= PIV_IGNORE_CHECKSUM_FAILURE;
        }
        verified = PageIsVerified(buf as Page, blkno, piv_flags, &mut checksum_failure);
        if checksum_failure {
            let rloc: RelFileLocatorBackend = smgr_rlocator(src);

            pgstat_prepare_report_checksum_failure(rloc.locator.dbOid);
            pgstat_report_checksum_failures_in_db(rloc.locator.dbOid, 1);
        }

        if !verified {
            /*
             * For paranoia's sake, capture the file path before invoking the
             * ereport machinery.
             */
            let relpath: RelPathStr = relpathbackend(
                smgr_rlocator(src).locator,
                smgr_rlocator(src).backend,
                forkNum,
            );

            elog!(
                ERROR,
                "invalid page in block {} of relation \"{}\"",
                blkno,
                relpathstr_to_string(&relpath)
            );
            unreachable!();
        }

        /*
         * Queue the page for WAL-logging and writing out.
         */
        smgr_bulk_write(bulkstate, blkno, buf, false);
        blkno += 1;
    }
    smgr_bulk_finish(bulkstate);
}

// ----------------------------------------------------------------------------
// RelFileLocatorSkippingWAL
//   Check if a BM_PERMANENT relfilelocator is using WAL.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RelFileLocatorSkippingWAL(rlocator: RelFileLocator) -> bool {
    if pendingSyncHash.is_null()
        || hash_search(
            pendingSyncHash,
            &rlocator as *const RelFileLocator as *const c_void,
            HASH_FIND,
            std::ptr::null_mut(),
        )
        .is_null()
    {
        return false;
    }

    true
}

// ----------------------------------------------------------------------------
// EstimatePendingSyncsSpace
//   Estimate space needed to pass syncs to parallel workers.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn EstimatePendingSyncsSpace() -> Size {
    let entries: i64;

    entries = if !pendingSyncHash.is_null() {
        hash_get_num_entries(pendingSyncHash)
    } else {
        0
    };
    mul_size(
        (1 + entries) as Size,
        core::mem::size_of::<RelFileLocator>(),
    )
}

// ----------------------------------------------------------------------------
// SerializePendingSyncs
//   Serialize syncs for parallel workers.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn SerializePendingSyncs(maxSize: Size, startAddress: *mut c_char) {
    let _ = maxSize;
    let tmphash: *mut HTAB;
    let mut ctl: HASHCTL = HASHCTL::zeroed();
    let mut scan: HASH_SEQ_STATUS = HASH_SEQ_STATUS::zeroed();
    let mut sync: *mut PendingRelSync;
    let mut delete: *mut PendingRelDelete;
    let mut src: *mut RelFileLocator;
    let mut dest: *mut RelFileLocator = startAddress as *mut RelFileLocator;

    'terminate: {
        if pendingSyncHash.is_null() {
            break 'terminate;
        }

        /* Create temporary hash to collect active relfilelocators */
        ctl.keysize = core::mem::size_of::<RelFileLocator>();
        ctl.entrysize = core::mem::size_of::<RelFileLocator>();
        ctl.hcxt = CurrentMemoryContext as *mut _;
        tmphash = hash_create(
            c"tmp relfilelocators".as_ptr(),
            hash_get_num_entries(pendingSyncHash),
            &mut ctl,
            HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
        );

        /* collect all rlocator from pending syncs */
        hash_seq_init(&mut scan, pendingSyncHash);
        loop {
            sync = hash_seq_search(&mut scan) as *mut PendingRelSync;
            if sync.is_null() {
                break;
            }
            hash_search(
                tmphash,
                &(*sync).rlocator as *const RelFileLocator as *const c_void,
                HASH_ENTER,
                std::ptr::null_mut(),
            );
        }

        /* remove deleted rnodes */
        delete = pendingDeletes;
        while !delete.is_null() {
            if (*delete).atCommit {
                hash_search(
                    tmphash,
                    &(*delete).rlocator as *const RelFileLocator as *const c_void,
                    HASH_REMOVE,
                    std::ptr::null_mut(),
                );
            }
            delete = (*delete).next;
        }

        hash_seq_init(&mut scan, tmphash);
        loop {
            src = hash_seq_search(&mut scan) as *mut RelFileLocator;
            if src.is_null() {
                break;
            }
            *dest = *src;
            dest = dest.add(1);
        }

        hash_destroy(tmphash);
    }

    // terminate:
    MemSet(
        dest as *mut c_void,
        0,
        core::mem::size_of::<RelFileLocator>(),
    );
}

// ----------------------------------------------------------------------------
// RestorePendingSyncs
//   Restore syncs within a parallel worker.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn RestorePendingSyncs(startAddress: *mut c_char) {
    let mut rlocator: *mut RelFileLocator;

    Assert(pendingSyncHash.is_null());
    rlocator = startAddress as *mut RelFileLocator;
    while (*rlocator).relNumber != 0 {
        AddPendingSync(rlocator);
        rlocator = rlocator.add(1);
    }
}

// ----------------------------------------------------------------------------
// smgrDoPendingDeletes() -- Take care of relation deletes at end of xact.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn smgrDoPendingDeletes(isCommit: bool) {
    let nestLevel: c_int = GetCurrentTransactionNestLevel();
    let mut pending: *mut PendingRelDelete;
    let mut prev: *mut PendingRelDelete;
    let mut next: *mut PendingRelDelete;
    let mut nrels: c_int = 0;
    let mut maxrels: c_int = 0;
    let mut srels: *mut SMgrRelation = std::ptr::null_mut();

    prev = std::ptr::null_mut();
    pending = pendingDeletes;
    while !pending.is_null() {
        next = (*pending).next;
        if (*pending).nestLevel < nestLevel {
            /* outer-level entries should not be processed yet */
            prev = pending;
        } else {
            /* unlink list entry first, so we don't retry on failure */
            if !prev.is_null() {
                (*prev).next = next;
            } else {
                pendingDeletes = next;
            }
            /* do deletion if called for */
            if (*pending).atCommit == isCommit {
                let srel: SMgrRelation;

                srel = smgropen((*pending).rlocator, (*pending).procNumber);

                /* allocate the initial array, or extend it, if needed */
                if maxrels == 0 {
                    maxrels = 8;
                    srels = palloc(
                        core::mem::size_of::<SMgrRelation>() * maxrels as usize,
                    ) as *mut SMgrRelation;
                } else if maxrels <= nrels {
                    maxrels *= 2;
                    srels = repalloc(
                        srels as *mut c_void,
                        core::mem::size_of::<SMgrRelation>() * maxrels as usize,
                    ) as *mut SMgrRelation;
                }

                *srels.add(nrels as usize) = srel;
                nrels += 1;
            }
            /* must explicitly free the list entry */
            pfree(pending as *mut c_void);
            /* prev does not change */
        }
        pending = next;
    }

    if nrels > 0 {
        smgrdounlinkall(srels, nrels, false);

        for i in 0..nrels {
            smgrclose(*srels.add(i as usize));
        }

        pfree(srels as *mut c_void);
    }
}

// ----------------------------------------------------------------------------
// smgrDoPendingSyncs() -- Take care of relation syncs at end of xact.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn smgrDoPendingSyncs(isCommit: bool, isParallelWorker: bool) {
    let mut pending: *mut PendingRelDelete;
    let mut nrels: c_int = 0;
    let mut maxrels: c_int = 0;
    let mut srels: *mut SMgrRelation = std::ptr::null_mut();
    let mut scan: HASH_SEQ_STATUS = HASH_SEQ_STATUS::zeroed();
    let mut pendingsync: *mut PendingRelSync;

    Assert(GetCurrentTransactionNestLevel() == 1);

    if pendingSyncHash.is_null() {
        return; /* no relation needs sync */
    }

    /* Abort -- just throw away all pending syncs */
    if !isCommit {
        pendingSyncHash = std::ptr::null_mut();
        return;
    }

    AssertPendingSyncs_RelationCache();

    /* Parallel worker -- just throw away all pending syncs */
    if isParallelWorker {
        pendingSyncHash = std::ptr::null_mut();
        return;
    }

    /* Skip syncing nodes that smgrDoPendingDeletes() will delete. */
    pending = pendingDeletes;
    while !pending.is_null() {
        if (*pending).atCommit {
            hash_search(
                pendingSyncHash,
                &(*pending).rlocator as *const RelFileLocator as *const c_void,
                HASH_REMOVE,
                std::ptr::null_mut(),
            );
        }
        pending = (*pending).next;
    }

    hash_seq_init(&mut scan, pendingSyncHash);
    loop {
        pendingsync = hash_seq_search(&mut scan) as *mut PendingRelSync;
        if pendingsync.is_null() {
            break;
        }

        let mut fork: ForkNumber;
        let mut nblocks: [BlockNumber; (MAX_FORKNUM + 1) as usize] =
            [0; (MAX_FORKNUM + 1) as usize];
        let mut total_blocks: uint64 = 0;
        let srel: SMgrRelation;

        srel = smgropen((*pendingsync).rlocator, INVALID_PROC_NUMBER);

        /*
         * We emit newpage WAL records for smaller relations.
         */
        if !(*pendingsync).is_truncated {
            fork = 0;
            while fork <= MAX_FORKNUM {
                if smgrexists(srel, fork) {
                    let n: BlockNumber = smgrnblocks(srel, fork);

                    /* we shouldn't come here for unlogged relations */
                    Assert(fork != INIT_FORKNUM);
                    nblocks[fork as usize] = n;
                    total_blocks += n as uint64;
                } else {
                    nblocks[fork as usize] = InvalidBlockNumber;
                }
                fork += 1;
            }
        }

        /*
         * Sync file or emit WAL records for its contents.
         */
        if (*pendingsync).is_truncated
            || total_blocks >= wal_skip_threshold as uint64 * 1024u64 / BLCKSZ
        {
            /* allocate the initial array, or extend it, if needed */
            if maxrels == 0 {
                maxrels = 8;
                srels = palloc(
                    core::mem::size_of::<SMgrRelation>() * maxrels as usize,
                ) as *mut SMgrRelation;
            } else if maxrels <= nrels {
                maxrels *= 2;
                srels = repalloc(
                    srels as *mut c_void,
                    core::mem::size_of::<SMgrRelation>() * maxrels as usize,
                ) as *mut SMgrRelation;
            }

            *srels.add(nrels as usize) = srel;
            nrels += 1;
        } else {
            /* Emit WAL records for all blocks.  The file is small enough. */
            fork = 0;
            while fork <= MAX_FORKNUM {
                let n: c_int = nblocks[fork as usize] as c_int;
                let rel: Relation;

                if !BlockNumberIsValid(n as BlockNumber) {
                    fork += 1;
                    continue;
                }

                /*
                 * Emit WAL for the whole file.
                 */
                rel = CreateFakeRelcacheEntry(smgr_rlocator(srel).locator);
                log_newpage_range(rel, fork, 0u32, n as u32, false);
                FreeFakeRelcacheEntry(rel);
                fork += 1;
            }
        }
    }

    pendingSyncHash = std::ptr::null_mut();

    if nrels > 0 {
        smgrdosyncall(srels, nrels);
        pfree(srels as *mut c_void);
    }
}

// ----------------------------------------------------------------------------
// smgrGetPendingDeletes() -- Get a list of non-temp relations to be deleted.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn smgrGetPendingDeletes(
    forCommit: bool,
    ptr: *mut *mut RelFileLocator,
) -> c_int {
    let nestLevel: c_int = GetCurrentTransactionNestLevel();
    let mut nrels: c_int;
    let rptr: *mut RelFileLocator;
    let mut rcur: *mut RelFileLocator;
    let mut pending: *mut PendingRelDelete;

    nrels = 0;
    pending = pendingDeletes;
    while !pending.is_null() {
        if (*pending).nestLevel >= nestLevel
            && (*pending).atCommit == forCommit
            && (*pending).procNumber == INVALID_PROC_NUMBER
        {
            nrels += 1;
        }
        pending = (*pending).next;
    }
    if nrels == 0 {
        *ptr = std::ptr::null_mut();
        return 0;
    }
    rptr = palloc(nrels as usize * core::mem::size_of::<RelFileLocator>())
        as *mut RelFileLocator;
    *ptr = rptr;
    rcur = rptr;
    pending = pendingDeletes;
    while !pending.is_null() {
        if (*pending).nestLevel >= nestLevel
            && (*pending).atCommit == forCommit
            && (*pending).procNumber == INVALID_PROC_NUMBER
        {
            *rcur = (*pending).rlocator;
            rcur = rcur.add(1);
        }
        pending = (*pending).next;
    }
    nrels
}

// ----------------------------------------------------------------------------
// PostPrepare_smgr -- Clean up after a successful PREPARE
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn PostPrepare_smgr() {
    let mut pending: *mut PendingRelDelete;
    let mut next: *mut PendingRelDelete;

    pending = pendingDeletes;
    while !pending.is_null() {
        next = (*pending).next;
        pendingDeletes = next;
        /* must explicitly free the list entry */
        pfree(pending as *mut c_void);
        pending = next;
    }
}

// ----------------------------------------------------------------------------
// AtSubCommit_smgr() --- Take care of subtransaction commit.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn AtSubCommit_smgr() {
    let nestLevel: c_int = GetCurrentTransactionNestLevel();
    let mut pending: *mut PendingRelDelete;

    pending = pendingDeletes;
    while !pending.is_null() {
        if (*pending).nestLevel >= nestLevel {
            (*pending).nestLevel = nestLevel - 1;
        }
        pending = (*pending).next;
    }
}

// ----------------------------------------------------------------------------
// AtSubAbort_smgr() --- Take care of subtransaction abort.
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn AtSubAbort_smgr() {
    smgrDoPendingDeletes(false);
}

// ----------------------------------------------------------------------------
// smgr_redo
// ----------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn smgr_redo(record: *mut XLogReaderState) {
    let lsn: XLogRecPtr = XLogReader_EndRecPtr(record);
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    /* Backup blocks are not used in smgr records */
    Assert(!XLogRecHasAnyBlockRefs(record));

    if info == XLOG_SMGR_CREATE {
        let xlrec: *mut xl_smgr_create = XLogRecGetData(record) as *mut xl_smgr_create;
        let reln: SMgrRelation;

        reln = smgropen((*xlrec).rlocator, INVALID_PROC_NUMBER);
        smgrcreate(reln, (*xlrec).forkNum, true);
    } else if info == XLOG_SMGR_TRUNCATE {
        let xlrec: *mut xl_smgr_truncate = XLogRecGetData(record) as *mut xl_smgr_truncate;
        let reln: SMgrRelation;
        let rel: Relation;
        let mut forks: [ForkNumber; MAX_FORKNUM as usize] = [0; MAX_FORKNUM as usize];
        let mut blocks: [BlockNumber; MAX_FORKNUM as usize] = [0; MAX_FORKNUM as usize];
        let mut old_blocks: [BlockNumber; MAX_FORKNUM as usize] = [0; MAX_FORKNUM as usize];
        let mut nforks: c_int = 0;
        let mut need_fsm_vacuum: bool = false;

        reln = smgropen((*xlrec).rlocator, INVALID_PROC_NUMBER);

        /*
         * Forcibly create relation if it doesn't exist (which suggests that
         * it was dropped somewhere later in the WAL sequence).
         */
        smgrcreate(reln, MAIN_FORKNUM, true);

        /*
         * Before we perform the truncation, update minimum recovery point to
         * cover this WAL record.
         */
        XLogFlush(lsn);

        /* Prepare for truncation of MAIN fork */
        if ((*xlrec).flags & SMGR_TRUNCATE_HEAP) != 0 {
            forks[nforks as usize] = MAIN_FORKNUM;
            old_blocks[nforks as usize] = smgrnblocks(reln, MAIN_FORKNUM);
            blocks[nforks as usize] = (*xlrec).blkno;
            nforks += 1;

            /* Also tell xlogutils.c about it */
            XLogTruncateRelation((*xlrec).rlocator, MAIN_FORKNUM, (*xlrec).blkno);
        }

        /* Prepare for truncation of FSM and VM too */
        rel = CreateFakeRelcacheEntry((*xlrec).rlocator);

        if ((*xlrec).flags & SMGR_TRUNCATE_FSM) != 0 && smgrexists(reln, FSM_FORKNUM) {
            blocks[nforks as usize] = FreeSpaceMapPrepareTruncateRel(rel, (*xlrec).blkno);
            if BlockNumberIsValid(blocks[nforks as usize]) {
                forks[nforks as usize] = FSM_FORKNUM;
                old_blocks[nforks as usize] = smgrnblocks(reln, FSM_FORKNUM);
                nforks += 1;
                need_fsm_vacuum = true;
            }
        }
        if ((*xlrec).flags & SMGR_TRUNCATE_VM) != 0 && smgrexists(reln, VISIBILITYMAP_FORKNUM) {
            blocks[nforks as usize] = visibilitymap_prepare_truncate(rel, (*xlrec).blkno);
            if BlockNumberIsValid(blocks[nforks as usize]) {
                forks[nforks as usize] = VISIBILITYMAP_FORKNUM;
                old_blocks[nforks as usize] = smgrnblocks(reln, VISIBILITYMAP_FORKNUM);
                nforks += 1;
            }
        }

        /* Do the real work to truncate relation forks */
        if nforks > 0 {
            START_CRIT_SECTION();
            smgrtruncate(
                reln,
                forks.as_mut_ptr(),
                nforks,
                old_blocks.as_mut_ptr(),
                blocks.as_mut_ptr(),
            );
            END_CRIT_SECTION();
        }

        /*
         * Update upper-level FSM pages to account for the truncation.
         */
        if need_fsm_vacuum {
            FreeSpaceMapVacuumRange(rel, (*xlrec).blkno, InvalidBlockNumber);
        }

        FreeFakeRelcacheEntry(rel);
    } else {
        elog!(PANIC, "smgr_redo: unknown op code {}", info);
    }
}

// ============================================================================
// Local stubs for unported helper functions / accessors.
// ============================================================================

#[inline]
unsafe fn Assert(_cond: bool) {}

#[inline]
unsafe fn BlockNumberIsValid(blockNumber: BlockNumber) -> bool {
    blockNumber != InvalidBlockNumber
}

unsafe fn IsInParallelMode() -> bool {
    false
}

unsafe fn ProcNumberForTempRelations() -> ProcNumber {
    crate::storage::procnumber::ProcNumberForTempRelations() as _
}

unsafe fn GetCurrentTransactionNestLevel() -> c_int {
    crate::access::transam::xact::GetCurrentTransactionNestLevel()
}

unsafe fn XLogIsNeeded() -> bool {
    false
}

#[no_mangle]
unsafe fn RelationNeedsWAL(_rel: Relation) -> bool { crate::access::nbtree::nbtdedup::RelationNeedsWAL(_rel as _) }

unsafe fn RelationGetSmgr(_rel: Relation) -> SMgrRelation {
    crate::storage::buffer::bufmgr::RelationGetSmgr(_rel as _) as _
}

unsafe fn RelationCloseSmgr(rel: Relation) {
    let rel = rel as crate::utils::rel::Relation;
    if !(*rel).rd_smgr.is_null() {
        crate::storage::smgr::smgr::smgrunpin((*rel).rd_smgr as _);
        crate::storage::smgr::smgr::smgrclose((*rel).rd_smgr as _);
        (*rel).rd_smgr = core::ptr::null_mut();
    }
}

unsafe fn RelFileLocatorEquals(a: RelFileLocator, b: RelFileLocator) -> bool {
    a.spcOid == b.spcOid && a.dbOid == b.dbOid && a.relNumber == b.relNumber
}

// smgr.c accessors / functions
unsafe fn smgropen(rlocator: RelFileLocator, procNumber: ProcNumber) -> SMgrRelation {
    crate::storage::smgr::smgr::smgropen(core::mem::transmute(rlocator), procNumber as _) as _
}
unsafe fn smgrcreate(reln: SMgrRelation, forknum: ForkNumber, isRedo: bool) {
    crate::storage::smgr::smgr::smgrcreate(reln as _, core::mem::transmute(forknum), isRedo)
}
unsafe fn smgrexists(reln: SMgrRelation, forknum: ForkNumber) -> bool {
    crate::storage::smgr::smgr::smgrexists(reln as _, core::mem::transmute(forknum))
}
unsafe fn smgrnblocks(reln: SMgrRelation, forknum: ForkNumber) -> BlockNumber {
    crate::storage::smgr::smgr::smgrnblocks(reln as _, core::mem::transmute(forknum))
}
unsafe fn smgrtruncate(
    reln: SMgrRelation,
    forknum: *mut ForkNumber,
    nforks: c_int,
    old_nblocks: *mut BlockNumber,
    nblocks: *mut BlockNumber,
) {
    crate::storage::smgr::smgr::smgrtruncate(reln as _, forknum as _, nforks, old_nblocks, nblocks)
}
unsafe fn smgrread(_reln: SMgrRelation, _forknum: ForkNumber, _blocknum: BlockNumber, _buffer: Page) {
    let mut buf = _buffer as *mut c_void;
    crate::storage::smgr::smgr::smgrreadv(
        _reln as _,
        core::mem::transmute(_forknum),
        _blocknum,
        &mut buf,
        1,
    )
}
unsafe fn smgrclose(reln: SMgrRelation) {
    crate::storage::smgr::smgr::smgrclose(reln as _)
}
unsafe fn smgrdounlinkall(rels: *mut SMgrRelation, nrels: c_int, isRedo: bool) {
    crate::storage::smgr::smgr::smgrdounlinkall(rels as _, nrels, isRedo)
}
unsafe fn smgrdosyncall(rels: *mut SMgrRelation, nrels: c_int) {
    crate::storage::smgr::smgr::smgrdosyncall(rels as _, nrels)
}
unsafe fn smgr_rlocator(reln: SMgrRelation) -> RelFileLocatorBackend {
    core::mem::transmute((*(reln as *mut crate::storage::smgr::smgr::SMgrRelationData)).smgr_rlocator)
}
unsafe fn smgr_rlocator_locator(reln: SMgrRelation) -> *const RelFileLocator {
    &(*(reln as *mut crate::storage::smgr::smgr::SMgrRelationData)).smgr_rlocator.locator as *const _ as *const RelFileLocator
}
unsafe fn smgr_set_targblock(reln: SMgrRelation, blk: BlockNumber) {
    (*(reln as *mut crate::storage::smgr::smgr::SMgrRelationData)).smgr_targblock = blk;
}
unsafe fn smgr_set_cached_nblocks(reln: SMgrRelation, fork: usize, blk: BlockNumber) {
    (*(reln as *mut crate::storage::smgr::smgr::SMgrRelationData)).smgr_cached_nblocks[fork] = blk;
}

// Relation field accessors
unsafe fn rel_rd_locator(rel: Relation) -> RelFileLocator {
    core::mem::transmute((*(rel as crate::utils::rel::Relation)).rd_locator)
}
unsafe fn rel_rd_backend(rel: Relation) -> ProcNumber {
    (*(rel as crate::utils::rel::Relation)).rd_backend as ProcNumber
}

// bulk_write.c
unsafe fn smgr_bulk_start_smgr(
    _smgr: SMgrRelation,
    _forknum: ForkNumber,
    _use_wal: bool,
) -> *mut BulkWriteState {
    crate::storage::smgr::bulk_write::smgr_bulk_start_smgr(_smgr as _, core::mem::transmute(_forknum), _use_wal) as _
}
unsafe fn smgr_bulk_get_buf(_bulkstate: *mut BulkWriteState) -> BulkWriteBuffer {
    crate::storage::smgr::bulk_write::smgr_bulk_get_buf(_bulkstate as _) as _
}
unsafe fn smgr_bulk_write(
    _bulkstate: *mut BulkWriteState,
    _blocknum: BlockNumber,
    _buf: BulkWriteBuffer,
    _page_std: bool,
) { crate::storage::smgr::bulk_write::smgr_bulk_write(_bulkstate as _, _blocknum as _, _buf as _, _page_std as _) }
unsafe fn smgr_bulk_finish(_bulkstate: *mut BulkWriteState) { crate::storage::smgr::bulk_write::smgr_bulk_finish(_bulkstate as _) }

// bufpage.c
unsafe fn PageIsVerified(
    _page: Page,
    _blkno: BlockNumber,
    _flags: c_int,
    _checksum_failure_p: *mut bool,
) -> bool {
    crate::storage::bufpage::PageIsVerified(_page as _, _blkno as _, _flags, _checksum_failure_p)
}

// pgstat
unsafe fn pgstat_prepare_report_checksum_failure(_dboid: Oid) { crate::utils::activity::pgstat_database::pgstat_prepare_report_checksum_failure(_dboid as _) }
unsafe fn pgstat_report_checksum_failures_in_db(_dboid: Oid, _failurecount: c_int) { crate::utils::activity::pgstat_database::pgstat_report_checksum_failures_in_db(_dboid as _, _failurecount as _) }

// relpath
unsafe fn relpathbackend(
    _rlocator: RelFileLocator,
    _backend: ProcNumber,
    _forknum: ForkNumber,
) -> RelPathStr {
    let rp = crate::common::relpath::GetRelationPath(
        _rlocator.dbOid,
        _rlocator.spcOid,
        _rlocator.relNumber,
        _backend as c_int,
        core::mem::transmute(_forknum),
    );
    let mut out: RelPathStr = [0; 1024];
    let n = rp.str.len();
    core::ptr::copy_nonoverlapping(rp.str.as_ptr(), out.as_mut_ptr(), n);
    out
}
unsafe fn relpathstr_to_string(_relpath: &RelPathStr) -> std::string::String {
    let cstr = core::ffi::CStr::from_ptr(_relpath.as_ptr());
    cstr.to_string_lossy().into_owned()
}

// freespace.c
unsafe fn FreeSpaceMapPrepareTruncateRel(_rel: Relation, _nblocks: BlockNumber) -> BlockNumber { crate::storage::freespace::freespace::FreeSpaceMapPrepareTruncateRel(_rel as _, _nblocks as _) }
unsafe fn FreeSpaceMapVacuumRange(_rel: Relation, _start: BlockNumber, _end: BlockNumber) { crate::storage::freespace::freespace::FreeSpaceMapVacuumRange(_rel as _, _start as _, _end as _) }

// visibilitymap.c
unsafe fn visibilitymap_prepare_truncate(_rel: Relation, _nheapblocks: BlockNumber) -> BlockNumber {
    crate::access::heap::visibilitymap::visibilitymap_prepare_truncate(_rel as _, _nheapblocks as _)
}

// xlog / xloginsert
unsafe fn XLogBeginInsert() {
    crate::access::transam::xloginsert::XLogBeginInsert()
}
unsafe fn XLogRegisterData(data: *mut c_char, len: Size) {
    crate::access::transam::xloginsert::XLogRegisterData(data as _, len as _)
}
unsafe fn XLogInsert(rmid: u8, info: uint8) -> XLogRecPtr {
    core::mem::transmute(crate::access::transam::xloginsert::XLogInsert(core::mem::transmute(rmid), info))
}
unsafe fn XLogFlush(record: XLogRecPtr) {
    crate::access::transam::xlog::XLogFlush(core::mem::transmute(record))
}
unsafe fn log_newpage_range(
    _rel: Relation,
    _forknum: ForkNumber,
    _startblk: BlockNumber,
    _endblk: BlockNumber,
    _page_std: bool,
) { crate::access::transam::xloginsert::log_newpage_range(_rel as _, _forknum as _, _startblk as _, _endblk as _, _page_std as _) }

// xlogutils.c
unsafe fn CreateFakeRelcacheEntry(_rlocator: RelFileLocator) -> Relation {
    crate::access::transam::xlogutils::CreateFakeRelcacheEntry(core::mem::transmute(_rlocator)) as _
}
unsafe fn FreeFakeRelcacheEntry(_fakerel: Relation) { crate::access::transam::xlogutils::FreeFakeRelcacheEntry(_fakerel as _) }
unsafe fn XLogTruncateRelation(_rlocator: RelFileLocator, _forkNum: ForkNumber, _nblocks: BlockNumber) {
    crate::access::transam::xlogutils::XLogTruncateRelation(core::mem::transmute(_rlocator), core::mem::transmute(_forkNum), _nblocks as _)
}

// xlogreader / xlogrecord accessors
unsafe fn XLogReader_EndRecPtr(_record: *mut XLogReaderState) -> XLogRecPtr {
    core::mem::transmute((*(_record as *mut crate::access::transam::xlogreader::XLogReaderState)).EndRecPtr)
}
unsafe fn XLogRecGetInfo(_record: *mut XLogReaderState) -> uint8 { crate::access::transam::xlogreader::XLogRecGetInfo(_record as _) }
unsafe fn XLogRecGetData(_record: *mut XLogReaderState) -> *mut c_char { crate::access::transam::xlogreader::XLogRecGetData(_record as _) }
unsafe fn XLogRecHasAnyBlockRefs(_record: *mut XLogReaderState) -> bool { crate::access::transam::xlogreader::XLogRecHasAnyBlockRefs(_record as _) }

// xact.c
unsafe fn AssertPendingSyncs_RelationCache() {
    // assertion-only routine in C; no-op here (Asserts are no-ops in this file)
}

// MyProc->delayChkptFlags accessors (storage/proc.h)
unsafe fn MyProc_delayChkptFlags() -> c_int {
    (*crate::access::transam::xact::MyProc).delayChkptFlags as c_int
}
unsafe fn MyProc_set_delayChkptFlags(_flags: c_int) {
    (*crate::access::transam::xact::MyProc).delayChkptFlags = _flags as _;
}

// CRIT section / interrupts (miscadmin.h)
unsafe fn START_CRIT_SECTION() {
    crate::miscadmin::START_CRIT_SECTION()
}
unsafe fn END_CRIT_SECTION() {
    crate::miscadmin::END_CRIT_SECTION()
}
unsafe fn CHECK_FOR_INTERRUPTS() {
    crate::miscadmin::CHECK_FOR_INTERRUPTS()
}

// hsearch.c
unsafe fn hash_create(
    tabname: *const c_char,
    nelem: i64,
    info: *mut HASHCTL,
    flags: c_int,
) -> *mut HTAB {
    crate::utils::hash::dynahash::hash_create(tabname, nelem as _, info as _, flags) as _
}
unsafe fn hash_search(
    hashp: *mut HTAB,
    keyPtr: *const c_void,
    action: HASHACTION,
    foundPtr: *mut bool,
) -> *mut c_void {
    crate::utils::hash::dynahash::hash_search(hashp as _, keyPtr, core::mem::transmute(action), foundPtr)
}
unsafe fn hash_seq_init(status: *mut HASH_SEQ_STATUS, hashp: *mut HTAB) {
    crate::utils::hash::dynahash::hash_seq_init(status as _, hashp as _)
}
unsafe fn hash_seq_search(status: *mut HASH_SEQ_STATUS) -> *mut c_void {
    crate::utils::hash::dynahash::hash_seq_search(status as _)
}
unsafe fn hash_destroy(hashp: *mut HTAB) {
    crate::utils::hash::dynahash::hash_destroy(hashp as _)
}
unsafe fn hash_get_num_entries(hashp: *mut HTAB) -> i64 {
    crate::utils::hash::dynahash::hash_get_num_entries(hashp as _) as i64
}

// memutils.c
unsafe fn mul_size(_s1: Size, _s2: Size) -> Size {
    crate::storage::ipc::shmem::mul_size(_s1 as _, _s2 as _) as _
}
unsafe fn MemSet(_start: *mut c_void, _val: c_int, _len: Size) { crate::c::MemSet(_start as _, _val as _, _len as _) }

// MemoryContexts (utils/memutils.h)
#[allow(non_upper_case_globals)]
static mut TopMemoryContext: *mut c_void = std::ptr::null_mut();
#[allow(non_upper_case_globals)]
static mut TopTransactionContext: *mut c_void = std::ptr::null_mut();
