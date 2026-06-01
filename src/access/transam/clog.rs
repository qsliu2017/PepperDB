//! clog.rs
//!   PostgreSQL transaction-commit-log manager
//! Translated 1:1 from postgres/src/backend/access/transam/clog.c
//!
//! This module stores two bits per transaction regarding its commit/abort
//! status; the status for four transactions fit in a byte.
//!
//! This would be a pretty simple abstraction on top of slru.c, except that
//! for performance reasons we allow multiple transactions that are
//! committing concurrently to form a queue, so that a single process can
//! update the status for all of them within a single lock acquisition run.
//!
//! XLOG interactions: this module generates an XLOG record whenever a new
//! CLOG page is initialized to zeroes.  Other writes of CLOG come from
//! recording of transaction commit or abort in xact.c, which generates its
//! own XLOG records for these events and will re-perform the status update
//! on redo; so we need make no additional XLOG entry here.  For synchronous
//! transaction commits, the XLOG is guaranteed flushed through the XLOG commit
//! record before we are called to log a commit, so the WAL rule "write xlog
//! before data" is satisfied automatically.  However, for async commits we
//! must track the latest LSN affecting each CLOG page, so that we can flush
//! XLOG that far and satisfy the WAL rule.  We don't have to worry about this
//! for aborts (whether sync or async), since the post-crash assumption would
//! be that such transactions failed anyway.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! src/backend/access/transam/clog.c

use crate::prelude::*;
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::pg_config::BLCKSZ;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int64, uint8, uint32, Size, TransactionId};

// StaticAssertDecl --- TODO(pg-port): real macro lives in c.h
macro_rules! StaticAssertDecl {
    ($cond:expr, $msg:expr) => {
        const _: () = assert!($cond, $msg);
    };
}

// ----------------------------------------------------------------------------
// clog.h
//
// src/include/access/clog.h
// ----------------------------------------------------------------------------

/*
 * Possible transaction statuses --- note that all-zeroes is the initial
 * state.
 *
 * A "subcommitted" transaction is a committed subtransaction whose parent
 * hasn't committed or aborted yet.
 */
pub type XidStatus = c_int;

pub const TRANSACTION_STATUS_IN_PROGRESS: XidStatus = 0x00;
pub const TRANSACTION_STATUS_COMMITTED: XidStatus = 0x01;
pub const TRANSACTION_STATUS_ABORTED: XidStatus = 0x02;
pub const TRANSACTION_STATUS_SUB_COMMITTED: XidStatus = 0x03;

#[repr(C)]
pub struct xl_clog_truncate {
    pub pageno: int64,
    pub oldestXact: TransactionId,
    pub oldestXactDb: Oid,
}

/* XLOG stuff */
pub const CLOG_ZEROPAGE: uint8 = 0x00;
pub const CLOG_TRUNCATE: uint8 = 0x10;

// ----------------------------------------------------------------------------
// clog.c
// ----------------------------------------------------------------------------

/*
 * Defines for CLOG page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CLOG page numbering also wraps around at 0xFFFFFFFF/CLOG_XACTS_PER_PAGE,
 * and CLOG segment numbering at
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCLOG (see CLOGPagePrecedes).
 */

/* We need two bits per xact, so four xacts fit in a byte */
const CLOG_BITS_PER_XACT: c_int = 2;
const CLOG_XACTS_PER_BYTE: c_int = 4;
const CLOG_XACTS_PER_PAGE: c_int = BLCKSZ as c_int * CLOG_XACTS_PER_BYTE;
const CLOG_XACT_BITMASK: c_int = (1 << CLOG_BITS_PER_XACT) - 1;

/*
 * Because space used in CLOG by each transaction is so small, we place a
 * smaller limit on the number of CLOG buffers than SLRU allows.  No other
 * SLRU needs this.
 */
const CLOG_MAX_ALLOWED_BUFFERS: c_int = Min_const(
    SLRU_MAX_ALLOWED_BUFFERS,
    (((MaxTransactionId / 2) + (CLOG_XACTS_PER_PAGE as TransactionId - 1))
        / CLOG_XACTS_PER_PAGE as TransactionId) as c_int,
);

/*
 * Although we return an int64 the actual value can't currently exceed
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE.
 */
#[inline]
fn TransactionIdToPage(xid: TransactionId) -> int64 {
    xid as int64 / CLOG_XACTS_PER_PAGE as int64
}

#[inline]
fn TransactionIdToPgIndex(xid: TransactionId) -> TransactionId {
    xid % (CLOG_XACTS_PER_PAGE as TransactionId)
}

#[inline]
fn TransactionIdToByte(xid: TransactionId) -> TransactionId {
    TransactionIdToPgIndex(xid) / CLOG_XACTS_PER_BYTE as TransactionId
}

#[inline]
fn TransactionIdToBIndex(xid: TransactionId) -> TransactionId {
    xid % (CLOG_XACTS_PER_BYTE as TransactionId)
}

/* We store the latest async LSN for each group of transactions */
const CLOG_XACTS_PER_LSN_GROUP: c_int = 32; /* keep this a power of 2 */
const CLOG_LSNS_PER_PAGE: c_int = CLOG_XACTS_PER_PAGE / CLOG_XACTS_PER_LSN_GROUP;

#[inline]
fn GetLSNIndex(slotno: c_int, xid: TransactionId) -> c_int {
    slotno * CLOG_LSNS_PER_PAGE
        + ((xid % (CLOG_XACTS_PER_PAGE as TransactionId)) / CLOG_XACTS_PER_LSN_GROUP as TransactionId)
            as c_int
}

/*
 * The number of subtransactions below which we consider to apply clog group
 * update optimization.  Testing reveals that the number higher than this can
 * hurt performance.
 */
const THRESHOLD_SUBTRANS_CLOG_OPT: c_int = 5;

/*
 * Link to shared-memory data structures for CLOG control
 */
static mut XactCtlData: SlruCtlData = unsafe { core::mem::zeroed() };

#[inline]
fn XactCtl() -> SlruCtl {
    core::ptr::addr_of_mut!(XactCtlData)
}

/*
 * TransactionIdSetTreeStatus
 *
 * Record the final state of transaction entries in the commit log for
 * a transaction and its subtransaction tree. Take care to ensure this is
 * efficient, and as atomic as possible.
 *
 * xid is a single xid to set status for. This will typically be
 * the top level transactionid for a top level commit or abort. It can
 * also be a subtransaction when we record transaction aborts.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 *
 * lsn must be the WAL location of the commit record when recording an async
 * commit.  For a synchronous commit it can be InvalidXLogRecPtr, since the
 * caller guarantees the commit record is already flushed in that case.  It
 * should be InvalidXLogRecPtr for abort cases, too.
 *
 * In the commit case, atomicity is limited by whether all the subxids are in
 * the same CLOG page as xid.  If they all are, then the lock will be grabbed
 * only once, and the status will be set to committed directly.  Otherwise
 * we must
 *	 1. set sub-committed all subxids that are not on the same page as the
 *		main xid
 *	 2. atomically set committed the main xid and the subxids on the same page
 *	 3. go over the first bunch again and set them committed
 * Note that as far as concurrent checkers are concerned, main transaction
 * commit as a whole is still atomic.
 *
 * Example:
 *		TransactionId t commits and has subxids t1, t2, t3, t4
 *		t is on page p1, t1 is also on p1, t2 and t3 are on p2, t4 is on p3
 *		1. update pages2-3:
 *					page2: set t2,t3 as sub-committed
 *					page3: set t4 as sub-committed
 *		2. update page1:
 *					page1: set t,t1 as committed
 *		3. update pages2-3:
 *					page2: set t2,t3 as committed
 *					page3: set t4 as committed
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; functions in transam.c are the intended callers.
 *
 * XXX Think about issuing POSIX_FADV_WILLNEED on pages that we will need,
 * but aren't yet in cache, as well as hinting pages not to fall out of
 * cache yet.
 */
pub unsafe fn TransactionIdSetTreeStatus(
    xid: TransactionId,
    nsubxids: c_int,
    subxids: *mut TransactionId,
    status: XidStatus,
    lsn: XLogRecPtr,
) {
    let mut pageno: int64 = TransactionIdToPage(xid); /* get page of parent */
    let mut i: c_int;

    Assert(
        status == TRANSACTION_STATUS_COMMITTED || status == TRANSACTION_STATUS_ABORTED,
    );

    /*
     * See how many subxids, if any, are on the same page as the parent, if
     * any.
     */
    i = 0;
    while i < nsubxids {
        if TransactionIdToPage(*subxids.add(i as usize)) != pageno {
            break;
        }
        i += 1;
    }

    /*
     * Do all items fit on a single page?
     */
    if i == nsubxids {
        /*
         * Set the parent and all subtransactions in a single call
         */
        TransactionIdSetPageStatus(xid, nsubxids, subxids, status, lsn, pageno, true);
    } else {
        let nsubxids_on_first_page: c_int = i;

        /*
         * If this is a commit then we care about doing this correctly (i.e.
         * using the subcommitted intermediate status).  By here, we know
         * we're updating more than one page of clog, so we must mark entries
         * that are *not* on the first page so that they show as subcommitted
         * before we then return to update the status to fully committed.
         *
         * To avoid touching the first page twice, skip marking subcommitted
         * for the subxids on that first page.
         */
        if status == TRANSACTION_STATUS_COMMITTED {
            set_status_by_pages(
                nsubxids - nsubxids_on_first_page,
                subxids.add(nsubxids_on_first_page as usize),
                TRANSACTION_STATUS_SUB_COMMITTED,
                lsn,
            );
        }

        /*
         * Now set the parent and subtransactions on same page as the parent,
         * if any
         */
        pageno = TransactionIdToPage(xid);
        TransactionIdSetPageStatus(
            xid,
            nsubxids_on_first_page,
            subxids,
            status,
            lsn,
            pageno,
            false,
        );

        /*
         * Now work through the rest of the subxids one clog page at a time,
         * starting from the second page onwards, like we did above.
         */
        set_status_by_pages(
            nsubxids - nsubxids_on_first_page,
            subxids.add(nsubxids_on_first_page as usize),
            status,
            lsn,
        );
    }
}

/*
 * Helper for TransactionIdSetTreeStatus: set the status for a bunch of
 * transactions, chunking in the separate CLOG pages involved. We never
 * pass the whole transaction tree to this function, only subtransactions
 * that are on different pages to the top level transaction id.
 */
unsafe fn set_status_by_pages(
    nsubxids: c_int,
    subxids: *mut TransactionId,
    status: XidStatus,
    lsn: XLogRecPtr,
) {
    let mut pageno: int64 = TransactionIdToPage(*subxids.add(0));
    let mut offset: c_int = 0;
    let mut i: c_int = 0;

    Assert(nsubxids > 0); /* else the pageno fetch above is unsafe */

    while i < nsubxids {
        let mut num_on_page: c_int = 0;
        let mut nextpageno: int64;

        loop {
            nextpageno = TransactionIdToPage(*subxids.add(i as usize));
            if nextpageno != pageno {
                break;
            }
            num_on_page += 1;
            i += 1;
            if !(i < nsubxids) {
                break;
            }
        }

        TransactionIdSetPageStatus(
            InvalidTransactionId,
            num_on_page,
            subxids.add(offset as usize),
            status,
            lsn,
            pageno,
            false,
        );
        offset = i;
        pageno = nextpageno;
    }
}

/*
 * Record the final state of transaction entries in the commit log for all
 * entries on a single page.  Atomic only on this page.
 */
unsafe fn TransactionIdSetPageStatus(
    xid: TransactionId,
    nsubxids: c_int,
    subxids: *mut TransactionId,
    status: XidStatus,
    lsn: XLogRecPtr,
    pageno: int64,
    all_xact_same_page: bool,
) {
    let lock: *mut LWLock;

    /* Can't use group update when PGPROC overflows. */
    StaticAssertDecl!(
        THRESHOLD_SUBTRANS_CLOG_OPT <= PGPROC_MAX_CACHED_SUBXIDS,
        "group clog threshold less than PGPROC cached subxids"
    );

    /* Get the SLRU bank lock for the page we are going to access. */
    lock = SimpleLruGetBankLock(XactCtl(), pageno);

    /*
     * When there is contention on the SLRU bank lock we need, we try to group
     * multiple updates; a single leader process will perform transaction
     * status updates for multiple backends so that the number of times the
     * bank lock needs to be acquired is reduced.
     *
     * For this optimization to be safe, the XID and subxids in MyProc must be
     * the same as the ones for which we're setting the status.  Check that
     * this is the case.
     *
     * For this optimization to be efficient, we shouldn't have too many
     * sub-XIDs and all of the XIDs for which we're adjusting clog should be
     * on the same page.  Check those conditions, too.
     */
    if all_xact_same_page
        && xid == (*MyProc).xid
        && nsubxids <= THRESHOLD_SUBTRANS_CLOG_OPT
        && nsubxids == (*MyProc).subxidStatus.count as c_int
        && (nsubxids == 0
            || memcmp(
                subxids as *const c_void,
                (*MyProc).subxids.xids.as_ptr() as *const c_void,
                nsubxids as usize * core::mem::size_of::<TransactionId>(),
            ) == 0)
    {
        /*
         * If we can immediately acquire the lock, we update the status of our
         * own XID and release the lock.  If not, try use group XID update. If
         * that doesn't work out, fall back to waiting for the lock to perform
         * an update for this transaction only.
         */
        if LWLockConditionalAcquire(lock, LW_EXCLUSIVE) {
            /* Got the lock without waiting!  Do the update. */
            TransactionIdSetPageStatusInternal(xid, nsubxids, subxids, status, lsn, pageno);
            LWLockRelease(lock);
            return;
        } else if TransactionGroupUpdateXidStatus(xid, status, lsn, pageno) {
            /* Group update mechanism has done the work. */
            return;
        }

        /* Fall through only if update isn't done yet. */
    }

    /* Group update not applicable, or couldn't accept this page number. */
    LWLockAcquire(lock, LW_EXCLUSIVE);
    TransactionIdSetPageStatusInternal(xid, nsubxids, subxids, status, lsn, pageno);
    LWLockRelease(lock);
}

/*
 * Record the final state of transaction entry in the commit log
 *
 * We don't do any locking here; caller must handle that.
 */
unsafe fn TransactionIdSetPageStatusInternal(
    xid: TransactionId,
    nsubxids: c_int,
    subxids: *mut TransactionId,
    status: XidStatus,
    lsn: XLogRecPtr,
    pageno: int64,
) {
    let slotno: c_int;
    let mut i: c_int;

    Assert(
        status == TRANSACTION_STATUS_COMMITTED
            || status == TRANSACTION_STATUS_ABORTED
            || (status == TRANSACTION_STATUS_SUB_COMMITTED && !TransactionIdIsValid(xid)),
    );
    Assert(LWLockHeldByMeInMode(
        SimpleLruGetBankLock(XactCtl(), pageno),
        LW_EXCLUSIVE,
    ));

    /*
     * If we're doing an async commit (ie, lsn is valid), then we must wait
     * for any active write on the page slot to complete.  Otherwise our
     * update could reach disk in that write, which will not do since we
     * mustn't let it reach disk until we've done the appropriate WAL flush.
     * But when lsn is invalid, it's OK to scribble on a page while it is
     * write-busy, since we don't care if the update reaches disk sooner than
     * we think.
     */
    slotno = SimpleLruReadPage(XactCtl(), pageno, XLogRecPtrIsInvalid(lsn), xid);

    /*
     * Set the main transaction id, if any.
     *
     * If we update more than one xid on this page while it is being written
     * out, we might find that some of the bits go to disk and others don't.
     * If we are updating commits on the page with the top-level xid that
     * could break atomicity, so we subcommit the subxids first before we mark
     * the top-level commit.
     */
    if TransactionIdIsValid(xid) {
        /* Subtransactions first, if needed ... */
        if status == TRANSACTION_STATUS_COMMITTED {
            i = 0;
            while i < nsubxids {
                Assert(
                    *(*(*XactCtl()).shared).page_number.add(slotno as usize)
                        == TransactionIdToPage(*subxids.add(i as usize)),
                );
                TransactionIdSetStatusBit(
                    *subxids.add(i as usize),
                    TRANSACTION_STATUS_SUB_COMMITTED,
                    lsn,
                    slotno,
                );
                i += 1;
            }
        }

        /* ... then the main transaction */
        TransactionIdSetStatusBit(xid, status, lsn, slotno);
    }

    /* Set the subtransactions */
    i = 0;
    while i < nsubxids {
        Assert(
            *(*(*XactCtl()).shared).page_number.add(slotno as usize)
                == TransactionIdToPage(*subxids.add(i as usize)),
        );
        TransactionIdSetStatusBit(*subxids.add(i as usize), status, lsn, slotno);
        i += 1;
    }

    *(*(*XactCtl()).shared).page_dirty.add(slotno as usize) = true;
}

/*
 * Subroutine for TransactionIdSetPageStatus, q.v.
 *
 * When we cannot immediately acquire the SLRU bank lock in exclusive mode at
 * commit time, add ourselves to a list of processes that need their XIDs
 * status update.  The first process to add itself to the list will acquire
 * the lock in exclusive mode and set transaction status as required on behalf
 * of all group members.  This avoids a great deal of contention when many
 * processes are trying to commit at once, since the lock need not be
 * repeatedly handed off from one committing process to the next.
 *
 * Returns true when transaction status has been updated in clog; returns
 * false if we decided against applying the optimization because the page
 * number we need to update differs from those processes already waiting.
 */
unsafe fn TransactionGroupUpdateXidStatus(
    xid: TransactionId,
    status: XidStatus,
    lsn: XLogRecPtr,
    pageno: int64,
) -> bool {
    let procglobal: *mut PROC_HDR = ProcGlobal;
    let proc: *mut PGPROC = MyProc;
    let mut nextidx: uint32;
    let mut wakeidx: uint32;
    let mut prevpageno: int64;
    let mut prevlock: *mut LWLock = core::ptr::null_mut();

    /* We should definitely have an XID whose status needs to be updated. */
    Assert(TransactionIdIsValid(xid));

    /*
     * Prepare to add ourselves to the list of processes needing a group XID
     * status update.
     */
    (*proc).clogGroupMember = true;
    (*proc).clogGroupMemberXid = xid;
    (*proc).clogGroupMemberXidStatus = status;
    (*proc).clogGroupMemberPage = pageno;
    (*proc).clogGroupMemberLsn = lsn;

    /*
     * We put ourselves in the queue by writing MyProcNumber to
     * ProcGlobal->clogGroupFirst.  However, if there's already a process
     * listed there, we compare our pageno with that of that process; if it
     * differs, we cannot participate in the group, so we return for caller to
     * update pg_xact in the normal way.
     *
     * If we're not the first process in the list, we must follow the leader.
     * We do this by storing the data we want updated in our PGPROC entry
     * where the leader can find it, then going to sleep.
     *
     * If no process is already in the list, we're the leader; our first step
     * is to lock the SLRU bank to which our page belongs, then we close out
     * the group by resetting the list pointer from ProcGlobal->clogGroupFirst
     * (this lets other processes set up other groups later); finally we do
     * the SLRU updates, release the SLRU bank lock, and wake up the sleeping
     * processes.
     *
     * If another group starts to update a page in a different SLRU bank, they
     * can proceed concurrently, since the bank lock they're going to use is
     * different from ours.  If another group starts to update a page in the
     * same bank as ours, they wait until we release the lock.
     */
    nextidx = pg_atomic_read_u32(&mut (*procglobal).clogGroupFirst);

    loop {
        /*
         * Add the proc to list, if the clog page where we need to update the
         * current transaction status is same as group leader's clog page.
         *
         * There is a race condition here, which is that after doing the below
         * check and before adding this proc's clog update to a group, the
         * group leader might have already finished the group update for this
         * page and becomes group leader of another group, updating a
         * different page.  This will lead to a situation where a single group
         * can have different clog page updates.  This isn't likely and will
         * still work, just less efficiently -- we handle this case by
         * switching to a different bank lock in the loop below.
         */
        if nextidx != INVALID_PROC_NUMBER as uint32
            && (*GetPGProcByNumber(nextidx as c_int)).clogGroupMemberPage
                != (*proc).clogGroupMemberPage
        {
            /*
             * Ensure that this proc is not a member of any clog group that
             * needs an XID status update.
             */
            (*proc).clogGroupMember = false;
            pg_atomic_write_u32(&mut (*proc).clogGroupNext, INVALID_PROC_NUMBER as uint32);
            return false;
        }

        pg_atomic_write_u32(&mut (*proc).clogGroupNext, nextidx);

        if pg_atomic_compare_exchange_u32(
            &mut (*procglobal).clogGroupFirst,
            &mut nextidx,
            MyProcNumber as uint32,
        ) {
            break;
        }
    }

    /*
     * If the list was not empty, the leader will update the status of our
     * XID. It is impossible to have followers without a leader because the
     * first process that has added itself to the list will always have
     * nextidx as INVALID_PROC_NUMBER.
     */
    if nextidx != INVALID_PROC_NUMBER as uint32 {
        let mut extraWaits: c_int = 0;

        /* Sleep until the leader updates our XID status. */
        pgstat_report_wait_start(WAIT_EVENT_XACT_GROUP_UPDATE);
        loop {
            /* acts as a read barrier */
            PGSemaphoreLock((*proc).sem);
            if !(*proc).clogGroupMember {
                break;
            }
            extraWaits += 1;
        }
        pgstat_report_wait_end();

        Assert(
            pg_atomic_read_u32(&mut (*proc).clogGroupNext) == INVALID_PROC_NUMBER as uint32,
        );

        /* Fix semaphore count for any absorbed wakeups */
        while extraWaits > 0 {
            extraWaits -= 1;
            PGSemaphoreUnlock((*proc).sem);
        }
        return true;
    }

    /*
     * By here, we know we're the leader process.  Acquire the SLRU bank lock
     * that corresponds to the page we originally wanted to modify.
     */
    prevpageno = (*proc).clogGroupMemberPage;
    prevlock = SimpleLruGetBankLock(XactCtl(), prevpageno);
    LWLockAcquire(prevlock, LW_EXCLUSIVE);

    /*
     * Now that we've got the lock, clear the list of processes waiting for
     * group XID status update, saving a pointer to the head of the list.
     * (Trying to pop elements one at a time could lead to an ABA problem.)
     *
     * At this point, any processes trying to do this would create a separate
     * group.
     */
    nextidx = pg_atomic_exchange_u32(
        &mut (*procglobal).clogGroupFirst,
        INVALID_PROC_NUMBER as uint32,
    );

    /* Remember head of list so we can perform wakeups after dropping lock. */
    wakeidx = nextidx;

    /* Walk the list and update the status of all XIDs. */
    while nextidx != INVALID_PROC_NUMBER as uint32 {
        let nextproc: *mut PGPROC =
            core::ptr::addr_of_mut!(*(*ProcGlobal).allProcs.add(nextidx as usize));
        let thispageno: int64 = (*nextproc).clogGroupMemberPage;

        /*
         * If the page to update belongs to a different bank than the previous
         * one, exchange bank lock to the new one.  This should be quite rare,
         * as described above.
         *
         * (We could try to optimize this by waking up the processes for which
         * we have already updated the status while we exchange the lock, but
         * the code doesn't do that at present.  I think it'd require
         * additional bookkeeping, making the common path slower in order to
         * improve an infrequent case.)
         */
        if thispageno != prevpageno {
            let lock: *mut LWLock = SimpleLruGetBankLock(XactCtl(), thispageno);

            if prevlock != lock {
                LWLockRelease(prevlock);
                LWLockAcquire(lock, LW_EXCLUSIVE);
            }
            prevlock = lock;
            prevpageno = thispageno;
        }

        /*
         * Transactions with more than THRESHOLD_SUBTRANS_CLOG_OPT sub-XIDs
         * should not use group XID status update mechanism.
         */
        Assert((*nextproc).subxidStatus.count as c_int <= THRESHOLD_SUBTRANS_CLOG_OPT);

        TransactionIdSetPageStatusInternal(
            (*nextproc).clogGroupMemberXid,
            (*nextproc).subxidStatus.count as c_int,
            (*nextproc).subxids.xids.as_mut_ptr(),
            (*nextproc).clogGroupMemberXidStatus,
            (*nextproc).clogGroupMemberLsn,
            (*nextproc).clogGroupMemberPage,
        );

        /* Move to next proc in list. */
        nextidx = pg_atomic_read_u32(&mut (*nextproc).clogGroupNext);
    }

    /* We're done with the lock now. */
    if !prevlock.is_null() {
        LWLockRelease(prevlock);
    }

    /*
     * Now that we've released the lock, go back and wake everybody up.  We
     * don't do this under the lock so as to keep lock hold times to a
     * minimum.
     *
     * (Perhaps we could do this in two passes, the first setting
     * clogGroupNext to invalid while saving the semaphores to an array, then
     * a single write barrier, then another pass unlocking the semaphores.)
     */
    while wakeidx != INVALID_PROC_NUMBER as uint32 {
        let wakeproc: *mut PGPROC =
            core::ptr::addr_of_mut!(*(*ProcGlobal).allProcs.add(wakeidx as usize));

        wakeidx = pg_atomic_read_u32(&mut (*wakeproc).clogGroupNext);
        pg_atomic_write_u32(&mut (*wakeproc).clogGroupNext, INVALID_PROC_NUMBER as uint32);

        /* ensure all previous writes are visible before follower continues. */
        pg_write_barrier();

        (*wakeproc).clogGroupMember = false;

        if wakeproc != MyProc {
            PGSemaphoreUnlock((*wakeproc).sem);
        }
    }

    true
}

/*
 * Sets the commit status of a single transaction.
 *
 * Caller must hold the corresponding SLRU bank lock, will be held at exit.
 */
unsafe fn TransactionIdSetStatusBit(
    xid: TransactionId,
    status: XidStatus,
    lsn: XLogRecPtr,
    slotno: c_int,
) {
    let byteno: c_int = TransactionIdToByte(xid) as c_int;
    let bshift: c_int = TransactionIdToBIndex(xid) as c_int * CLOG_BITS_PER_XACT;
    let byteptr: *mut c_char;
    let mut byteval: c_char;
    let curval: c_char;

    Assert(
        *(*(*XactCtl()).shared).page_number.add(slotno as usize) == TransactionIdToPage(xid),
    );
    Assert(LWLockHeldByMeInMode(
        SimpleLruGetBankLock(
            XactCtl(),
            *(*(*XactCtl()).shared).page_number.add(slotno as usize),
        ),
        LW_EXCLUSIVE,
    ));

    byteptr = (*(*(*XactCtl()).shared).page_buffer.add(slotno as usize)).add(byteno as usize);
    curval = (*byteptr >> bshift) & CLOG_XACT_BITMASK as c_char;

    /*
     * When replaying transactions during recovery we still need to perform
     * the two phases of subcommit and then commit. However, some transactions
     * are already correctly marked, so we just treat those as a no-op which
     * allows us to keep the following Assert as restrictive as possible.
     */
    if InRecovery
        && status == TRANSACTION_STATUS_SUB_COMMITTED
        && curval as c_int == TRANSACTION_STATUS_COMMITTED
    {
        return;
    }

    /*
     * Current state change should be from 0 or subcommitted to target state
     * or we should already be there when replaying changes during recovery.
     */
    Assert(
        curval == 0
            || (curval as c_int == TRANSACTION_STATUS_SUB_COMMITTED
                && status != TRANSACTION_STATUS_IN_PROGRESS)
            || curval as c_int == status,
    );

    /* note this assumes exclusive access to the clog page */
    byteval = *byteptr;
    byteval &= !((((1 << CLOG_BITS_PER_XACT) - 1) << bshift) as c_char);
    byteval |= (status << bshift) as c_char;
    *byteptr = byteval;

    /*
     * Update the group LSN if the transaction completion LSN is higher.
     *
     * Note: lsn will be invalid when supplied during InRecovery processing,
     * so we don't need to do anything special to avoid LSN updates during
     * recovery. After recovery completes the next clog change will set the
     * LSN correctly.
     */
    if !XLogRecPtrIsInvalid(lsn) {
        let lsnindex: c_int = GetLSNIndex(slotno, xid);

        if *(*(*XactCtl()).shared).group_lsn.add(lsnindex as usize) < lsn {
            *(*(*XactCtl()).shared).group_lsn.add(lsnindex as usize) = lsn;
        }
    }
}

/*
 * Interrogate the state of a transaction in the commit log.
 *
 * Aside from the actual commit status, this function returns (into *lsn)
 * an LSN that is late enough to be able to guarantee that if we flush up to
 * that LSN then we will have flushed the transaction's commit record to disk.
 * The result is not necessarily the exact LSN of the transaction's commit
 * record!	For example, for long-past transactions (those whose clog pages
 * already migrated to disk), we'll return InvalidXLogRecPtr.  Also, because
 * we group transactions on the same clog page to conserve storage, we might
 * return the LSN of a later transaction that falls into the same group.
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; TransactionLogFetch() in transam.c is the intended caller.
 */
pub unsafe fn TransactionIdGetStatus(xid: TransactionId, lsn: *mut XLogRecPtr) -> XidStatus {
    let pageno: int64 = TransactionIdToPage(xid);
    let byteno: c_int = TransactionIdToByte(xid) as c_int;
    let bshift: c_int = TransactionIdToBIndex(xid) as c_int * CLOG_BITS_PER_XACT;
    let slotno: c_int;
    let lsnindex: c_int;
    let byteptr: *mut c_char;
    let status: XidStatus;

    /* lock is acquired by SimpleLruReadPage_ReadOnly */

    slotno = SimpleLruReadPage_ReadOnly(XactCtl(), pageno, xid);
    byteptr = (*(*(*XactCtl()).shared).page_buffer.add(slotno as usize)).add(byteno as usize);

    status = ((*byteptr >> bshift) & CLOG_XACT_BITMASK as c_char) as XidStatus;

    lsnindex = GetLSNIndex(slotno, xid);
    *lsn = *(*(*XactCtl()).shared).group_lsn.add(lsnindex as usize);

    LWLockRelease(SimpleLruGetBankLock(XactCtl(), pageno));

    status
}

/*
 * Number of shared CLOG buffers.
 *
 * If asked to autotune, use 2MB for every 1GB of shared buffers, up to 8MB.
 * Otherwise just cap the configured amount to be between 16 and the maximum
 * allowed.
 */
unsafe fn CLOGShmemBuffers() -> c_int {
    /* auto-tune based on shared buffers */
    if transaction_buffers == 0 {
        return SimpleLruAutotuneBuffers(512, 1024);
    }

    Min(Max(16, transaction_buffers), CLOG_MAX_ALLOWED_BUFFERS)
}

/*
 * Initialization of shared memory for CLOG
 */
pub unsafe fn CLOGShmemSize() -> Size {
    SimpleLruShmemSize(CLOGShmemBuffers(), CLOG_LSNS_PER_PAGE)
}

pub unsafe fn CLOGShmemInit() {
    /* If auto-tuning is requested, now is the time to do it */
    if transaction_buffers == 0 {
        let mut buf: [c_char; 32] = [0; 32];

        snprintf(
            buf.as_mut_ptr(),
            core::mem::size_of_val(&buf),
            c"%d".as_ptr(),
            CLOGShmemBuffers(),
        );
        SetConfigOption(
            c"transaction_buffers".as_ptr(),
            buf.as_ptr(),
            PGC_POSTMASTER,
            PGC_S_DYNAMIC_DEFAULT,
        );

        /*
         * We prefer to report this value's source as PGC_S_DYNAMIC_DEFAULT.
         * However, if the DBA explicitly set transaction_buffers = 0 in the
         * config file, then PGC_S_DYNAMIC_DEFAULT will fail to override that
         * and we must force the matter with PGC_S_OVERRIDE.
         */
        if transaction_buffers == 0
        /* failed to apply it? */
        {
            SetConfigOption(
                c"transaction_buffers".as_ptr(),
                buf.as_ptr(),
                PGC_POSTMASTER,
                PGC_S_OVERRIDE,
            );
        }
    }
    Assert(transaction_buffers != 0);

    (*XactCtl()).PagePrecedes = Some(CLOGPagePrecedes);
    SimpleLruInit(
        XactCtl(),
        c"transaction".as_ptr(),
        CLOGShmemBuffers(),
        CLOG_LSNS_PER_PAGE,
        c"pg_xact".as_ptr(),
        LWTRANCHE_XACT_BUFFER,
        LWTRANCHE_XACT_SLRU,
        SYNC_HANDLER_CLOG,
        false,
    );
    SlruPagePrecedesUnitTests(XactCtl(), CLOG_XACTS_PER_PAGE);
}

/*
 * GUC check_hook for transaction_buffers
 */
pub unsafe fn check_transaction_buffers(
    newval: *mut c_int,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    check_slru_buffers(c"transaction_buffers".as_ptr(), newval)
}

/*
 * This func must be called ONCE on system install.  It creates
 * the initial CLOG segment.  (The CLOG directory is assumed to
 * have been created by initdb, and CLOGShmemInit must have been
 * called already.)
 */
pub unsafe fn BootStrapCLOG() {
    let slotno: c_int;
    let lock: *mut LWLock = SimpleLruGetBankLock(XactCtl(), 0);

    LWLockAcquire(lock, LW_EXCLUSIVE);

    /* Create and zero the first page of the commit log */
    slotno = ZeroCLOGPage(0, false);

    /* Make sure it's written out */
    SimpleLruWritePage(XactCtl(), slotno);
    Assert(!*(*(*XactCtl()).shared).page_dirty.add(slotno as usize));

    LWLockRelease(lock);
}

/*
 * Initialize (or reinitialize) a page of CLOG to zeroes.
 * If writeXlog is true, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
unsafe fn ZeroCLOGPage(pageno: int64, writeXlog: bool) -> c_int {
    let slotno: c_int;

    slotno = SimpleLruZeroPage(XactCtl(), pageno);

    if writeXlog {
        WriteZeroPageXlogRec(pageno);
    }

    slotno
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized TransamVariables->nextXid.
 */
pub unsafe fn StartupCLOG() {
    let xid: TransactionId = XidFromFullTransactionId(core::ptr::read(&(*TransamVariables).nextXid));
    let pageno: int64 = TransactionIdToPage(xid);

    /*
     * Initialize our idea of the latest page number.
     */
    pg_atomic_write_u64(
        &mut (*(*XactCtl()).shared).latest_page_number,
        pageno as u64,
    );
}

/*
 * This must be called ONCE at the end of startup/recovery.
 */
pub unsafe fn TrimCLOG() {
    let xid: TransactionId = XidFromFullTransactionId(core::ptr::read(&(*TransamVariables).nextXid));
    let pageno: int64 = TransactionIdToPage(xid);
    let lock: *mut LWLock = SimpleLruGetBankLock(XactCtl(), pageno);

    LWLockAcquire(lock, LW_EXCLUSIVE);

    /*
     * Zero out the remainder of the current clog page.  Under normal
     * circumstances it should be zeroes already, but it seems at least
     * theoretically possible that XLOG replay will have settled on a nextXID
     * value that is less than the last XID actually used and marked by the
     * previous database lifecycle (since subtransaction commit writes clog
     * but makes no WAL entry).  Let's just be safe. (We need not worry about
     * pages beyond the current one, since those will be zeroed when first
     * used.  For the same reason, there is no need to do anything when
     * nextXid is exactly at a page boundary; and it's likely that the
     * "current" page doesn't exist yet in that case.)
     */
    if TransactionIdToPgIndex(xid) != 0 {
        let byteno: c_int = TransactionIdToByte(xid) as c_int;
        let bshift: c_int = TransactionIdToBIndex(xid) as c_int * CLOG_BITS_PER_XACT;
        let slotno: c_int;
        let byteptr: *mut c_char;

        slotno = SimpleLruReadPage(XactCtl(), pageno, false, xid);
        byteptr = (*(*(*XactCtl()).shared).page_buffer.add(slotno as usize)).add(byteno as usize);

        /* Zero so-far-unused positions in the current byte */
        *byteptr &= ((1 << bshift) - 1) as c_char;
        /* Zero the rest of the page */
        MemSet(
            byteptr.add(1) as *mut c_void,
            0,
            (BLCKSZ as c_int - byteno - 1) as Size,
        );

        *(*(*XactCtl()).shared).page_dirty.add(slotno as usize) = true;
    }

    LWLockRelease(lock);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
pub unsafe fn CheckPointCLOG() {
    /*
     * Write dirty CLOG pages to disk.  This may result in sync requests
     * queued for later handling by ProcessSyncRequests(), as part of the
     * checkpoint.
     */
    TRACE_POSTGRESQL_CLOG_CHECKPOINT_START(true);
    SimpleLruWriteAll(XactCtl(), true);
    TRACE_POSTGRESQL_CLOG_CHECKPOINT_DONE(true);
}

/*
 * Make sure that CLOG has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty clog or xlog page to make room
 * in shared memory.
 */
pub unsafe fn ExtendCLOG(newestXact: TransactionId) {
    let pageno: int64;
    let lock: *mut LWLock;

    /*
     * No work except at first XID of a page.  But beware: just after
     * wraparound, the first XID of page zero is FirstNormalTransactionId.
     */
    if TransactionIdToPgIndex(newestXact) != 0
        && !TransactionIdEquals(newestXact, FirstNormalTransactionId)
    {
        return;
    }

    pageno = TransactionIdToPage(newestXact);
    lock = SimpleLruGetBankLock(XactCtl(), pageno);

    LWLockAcquire(lock, LW_EXCLUSIVE);

    /* Zero the page and make an XLOG entry about it */
    ZeroCLOGPage(pageno, true);

    LWLockRelease(lock);
}

/*
 * Remove all CLOG segments before the one holding the passed transaction ID
 *
 * Before removing any CLOG data, we must flush XLOG to disk, to ensure that
 * any recently-emitted records with freeze plans have reached disk; otherwise
 * a crash and restart might leave us with some unfrozen tuples referencing
 * removed CLOG data.  We choose to emit a special TRUNCATE XLOG record too.
 * Replaying the deletion from XLOG is not critical, since the files could
 * just as well be removed later, but doing so prevents a long-running hot
 * standby server from acquiring an unreasonably bloated CLOG directory.
 *
 * Since CLOG segments hold a large number of transactions, the opportunity to
 * actually remove a segment is fairly rare, and so it seems best not to do
 * the XLOG flush unless we have confirmed that there is a removable segment.
 */
pub unsafe fn TruncateCLOG(oldestXact: TransactionId, oldestxid_datoid: Oid) {
    let mut cutoffPage: int64;

    /*
     * The cutoff point is the start of the segment containing oldestXact. We
     * pass the *page* containing oldestXact to SimpleLruTruncate.
     */
    cutoffPage = TransactionIdToPage(oldestXact);

    /* Check to see if there's any files that could be removed */
    if !SlruScanDirectory(
        XactCtl(),
        SlruScanDirCbReportPresence,
        core::ptr::addr_of_mut!(cutoffPage) as *mut c_void,
    ) {
        return; /* nothing to remove */
    }

    /*
     * Advance oldestClogXid before truncating clog, so concurrent xact status
     * lookups can ensure they don't attempt to access truncated-away clog.
     *
     * It's only necessary to do this if we will actually truncate away clog
     * pages.
     */
    AdvanceOldestClogXid(oldestXact);

    /*
     * Write XLOG record and flush XLOG to disk. We record the oldest xid
     * we're keeping information about here so we can ensure that it's always
     * ahead of clog truncation in case we crash, and so a standby finds out
     * the new valid xid before the next checkpoint.
     */
    WriteTruncateXlogRec(cutoffPage, oldestXact, oldestxid_datoid);

    /* Now we can remove the old CLOG segment(s) */
    SimpleLruTruncate(XactCtl(), cutoffPage);
}

/*
 * Decide whether a CLOG page number is "older" for truncation purposes.
 *
 * We need to use comparison of TransactionIds here in order to do the right
 * thing with wraparound XID arithmetic.  However, TransactionIdPrecedes()
 * would get weird about permanent xact IDs.  So, offset both such that xid1,
 * xid2, and xid2 + CLOG_XACTS_PER_PAGE - 1 are all normal XIDs; this offset
 * is relevant to page 0 and to the page preceding page 0.
 *
 * The page containing oldestXact-2^31 is the important edge case.  The
 * portion of that page equaling or following oldestXact-2^31 is expendable,
 * but the portion preceding oldestXact-2^31 is not.  When oldestXact-2^31 is
 * the first XID of a page and segment, the entire page and segment is
 * expendable, and we could truncate the segment.  Recognizing that case would
 * require making oldestXact, not just the page containing oldestXact,
 * available to this callback.  The benefit would be rare and small, so we
 * don't optimize that edge case.
 */
unsafe extern "C" fn CLOGPagePrecedes(page1: int64, page2: int64) -> bool {
    let mut xid1: TransactionId;
    let mut xid2: TransactionId;

    xid1 = (page1 as TransactionId).wrapping_mul(CLOG_XACTS_PER_PAGE as TransactionId);
    xid1 = xid1.wrapping_add(FirstNormalTransactionId + 1);
    xid2 = (page2 as TransactionId).wrapping_mul(CLOG_XACTS_PER_PAGE as TransactionId);
    xid2 = xid2.wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(
            xid1,
            xid2.wrapping_add(CLOG_XACTS_PER_PAGE as TransactionId - 1),
        )
}

/*
 * Write a ZEROPAGE xlog record
 */
unsafe fn WriteZeroPageXlogRec(pageno: int64) {
    let pageno = pageno;
    XLogBeginInsert();
    XLogRegisterData(
        core::ptr::addr_of!(pageno) as *mut c_char,
        core::mem::size_of_val(&pageno),
    );
    XLogInsert(RM_CLOG_ID, CLOG_ZEROPAGE);
}

/*
 * Write a TRUNCATE xlog record
 *
 * We must flush the xlog record to disk before returning --- see notes
 * in TruncateCLOG().
 */
unsafe fn WriteTruncateXlogRec(pageno: int64, oldestXact: TransactionId, oldestXactDb: Oid) {
    let recptr: XLogRecPtr;
    let xlrec = xl_clog_truncate {
        pageno,
        oldestXact,
        oldestXactDb,
    };

    XLogBeginInsert();
    XLogRegisterData(
        core::ptr::addr_of!(xlrec) as *mut c_char,
        core::mem::size_of::<xl_clog_truncate>(),
    );
    recptr = XLogInsert(RM_CLOG_ID, CLOG_TRUNCATE);
    XLogFlush(recptr);
}

/*
 * CLOG resource manager's routines
 */
pub unsafe fn clog_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    /* Backup blocks are not used in clog records */
    Assert(!XLogRecHasAnyBlockRefs(record));

    if info == CLOG_ZEROPAGE {
        let mut pageno: int64 = 0;
        let slotno: c_int;
        let lock: *mut LWLock;

        memcpy(
            core::ptr::addr_of_mut!(pageno) as *mut c_void,
            XLogRecGetData(record) as *const c_void,
            core::mem::size_of_val(&pageno),
        );

        lock = SimpleLruGetBankLock(XactCtl(), pageno);
        LWLockAcquire(lock, LW_EXCLUSIVE);

        slotno = ZeroCLOGPage(pageno, false);
        SimpleLruWritePage(XactCtl(), slotno);
        Assert(!*(*(*XactCtl()).shared).page_dirty.add(slotno as usize));

        LWLockRelease(lock);
    } else if info == CLOG_TRUNCATE {
        let mut xlrec: xl_clog_truncate = core::mem::zeroed();

        memcpy(
            core::ptr::addr_of_mut!(xlrec) as *mut c_void,
            XLogRecGetData(record) as *const c_void,
            core::mem::size_of::<xl_clog_truncate>(),
        );

        AdvanceOldestClogXid(xlrec.oldestXact);

        SimpleLruTruncate(XactCtl(), xlrec.pageno);
    } else {
        elog!(PANIC, "clog_redo: unknown op code {}", info);
    }
}

/*
 * Entrypoint for sync.c to sync clog files.
 */
pub unsafe fn clogsyncfiletag(ftag: *const FileTag, path: *mut c_char) -> c_int {
    SlruSyncFileTag(XactCtl(), ftag, path)
}

// ----------------------------------------------------------------------------
// Local stubs for unported dependencies.
//
// clog.c is built on the SLRU layer (access/slru.c) which is not yet ported;
// the sibling commit_ts.rs carries the same self-contained stub set, so we
// mirror it here.  Each stub is marked with the C file where the real symbol
// lives.
// ----------------------------------------------------------------------------

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int;
}

// TODO(pg-port): real MemSet lives in c.h (utils macro)
#[inline]
unsafe fn MemSet(start: *mut c_void, val: c_int, len: Size) {
    extern "C" {
        fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    }
    memset(start, val, len as usize);
}

// GUC types -------------------------------------------------------------------
pub type GucSource = c_int; // TODO(pg-port): real GucSource lives in utils/guc.h
pub const PGC_S_DYNAMIC_DEFAULT: GucSource = 0; // TODO(pg-port): real value lives in utils/guc.h
pub const PGC_S_OVERRIDE: GucSource = 0; // TODO(pg-port): real value lives in utils/guc.h
pub const PGC_POSTMASTER: c_int = 0; // TODO(pg-port): real value lives in utils/guc.h

// SLRU --- TODO(pg-port): real definitions live in access/slru.h / slru.c -----
#[repr(C)]
pub struct SlruSharedData {
    pub page_buffer: *mut *mut c_char,
    pub page_dirty: *mut bool,
    pub page_number: *mut int64,
    pub group_lsn: *mut XLogRecPtr,
    pub latest_page_number: pg_atomic_uint64,
    // ... TODO(pg-port): access/slru.h
}
pub type SlruShared = *mut SlruSharedData;

pub type SlruPagePrecedesFunction = unsafe extern "C" fn(int64, int64) -> bool;

#[repr(C)]
pub struct SlruCtlData {
    pub shared: SlruShared,
    pub PagePrecedes: Option<SlruPagePrecedesFunction>,
    // ... TODO(pg-port): access/slru.h
}
pub type SlruCtl = *mut SlruCtlData;

pub const SLRU_MAX_ALLOWED_BUFFERS: c_int = 0; // TODO(pg-port): real value lives in access/slru.h

pub type SlruScanCallback =
    unsafe extern "C" fn(SlruCtl, *mut c_char, int64, *mut c_void) -> bool;

unsafe fn SimpleLruGetBankLock(_ctl: SlruCtl, _pageno: int64) -> *mut LWLock {
    unimplemented!() // TODO(pg-port): real SimpleLruGetBankLock lives in access/slru.c
}
unsafe fn SimpleLruReadPage(
    _ctl: SlruCtl,
    _pageno: int64,
    _write_ok: bool,
    _xid: TransactionId,
) -> c_int {
    unimplemented!() // TODO(pg-port): real SimpleLruReadPage lives in access/slru.c
}
unsafe fn SimpleLruReadPage_ReadOnly(_ctl: SlruCtl, _pageno: int64, _xid: TransactionId) -> c_int {
    unimplemented!() // TODO(pg-port): real SimpleLruReadPage_ReadOnly lives in access/slru.c
}
unsafe fn SimpleLruZeroPage(_ctl: SlruCtl, _pageno: int64) -> c_int {
    unimplemented!() // TODO(pg-port): real SimpleLruZeroPage lives in access/slru.c
}
unsafe fn SimpleLruWritePage(_ctl: SlruCtl, _slotno: c_int) {
    unimplemented!() // TODO(pg-port): real SimpleLruWritePage lives in access/slru.c
}
unsafe fn SimpleLruWriteAll(_ctl: SlruCtl, _allow_redirtied: bool) {
    unimplemented!() // TODO(pg-port): real SimpleLruWriteAll lives in access/slru.c
}
unsafe fn SimpleLruTruncate(_ctl: SlruCtl, _cutoffPage: int64) {
    unimplemented!() // TODO(pg-port): real SimpleLruTruncate lives in access/slru.c
}
unsafe fn SimpleLruInit(
    _ctl: SlruCtl,
    _name: *const c_char,
    _nslots: c_int,
    _nlsns: c_int,
    _subdir: *const c_char,
    _buffer_tranche_id: c_int,
    _bank_tranche_id: c_int,
    _sync_handler: c_int,
    _long_segment_names: bool,
) {
    unimplemented!() // TODO(pg-port): real SimpleLruInit lives in access/slru.c
}
unsafe fn SimpleLruShmemSize(_nslots: c_int, _nlsns: c_int) -> Size {
    unimplemented!() // TODO(pg-port): real SimpleLruShmemSize lives in access/slru.c
}
unsafe fn SimpleLruAutotuneBuffers(_divisor: c_int, _max: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): real SimpleLruAutotuneBuffers lives in access/slru.c
}
unsafe fn SlruPagePrecedesUnitTests(_ctl: SlruCtl, _per_page: c_int) {
    unimplemented!() // TODO(pg-port): real SlruPagePrecedesUnitTests lives in access/slru.c
}
unsafe fn SlruScanDirectory(_ctl: SlruCtl, _callback: SlruScanCallback, _data: *mut c_void) -> bool {
    unimplemented!() // TODO(pg-port): real SlruScanDirectory lives in access/slru.c
}
unsafe fn SlruSyncFileTag(_ctl: SlruCtl, _ftag: *const FileTag, _path: *mut c_char) -> c_int {
    unimplemented!() // TODO(pg-port): real SlruSyncFileTag lives in access/slru.c
}
unsafe extern "C" fn SlruScanDirCbReportPresence(
    _ctl: SlruCtl,
    _filename: *mut c_char,
    _segpage: int64,
    _data: *mut c_void,
) -> bool {
    unimplemented!() // TODO(pg-port): real SlruScanDirCbReportPresence lives in access/slru.c
}
unsafe fn check_slru_buffers(_name: *const c_char, _newval: *mut c_int) -> bool {
    unimplemented!() // TODO(pg-port): real check_slru_buffers lives in access/slru.c
}

// GUC variable --- TODO(pg-port): real transaction_buffers lives in access/slru.c
pub static mut transaction_buffers: c_int = 0;

// LWLock --- TODO(pg-port): real definitions live in storage/lwlock.h ---------
#[repr(C)]
pub struct LWLock {
    _private: [u8; 0],
}

pub const LW_EXCLUSIVE: c_int = 0; // TODO(pg-port): real value lives in storage/lwlock.h

pub const LWTRANCHE_XACT_BUFFER: c_int = 0; // TODO(pg-port): real value lives in storage/lwlock.h
pub const LWTRANCHE_XACT_SLRU: c_int = 0; // TODO(pg-port): real value lives in storage/lwlock.h

pub const SYNC_HANDLER_CLOG: c_int = 0; // TODO(pg-port): real value lives in storage/sync.h

unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port): real LWLockAcquire lives in storage/lwlock.c
}
unsafe fn LWLockConditionalAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port): real LWLockConditionalAcquire lives in storage/lwlock.c
}
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    unimplemented!() // TODO(pg-port): real LWLockRelease lives in storage/lwlock.c
}
unsafe fn LWLockHeldByMeInMode(_lock: *mut LWLock, _mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port): real LWLockHeldByMeInMode lives in storage/lwlock.c
}

// Sync / file tags --- TODO(pg-port): real FileTag lives in storage/sync.h ----
#[repr(C)]
pub struct FileTag {
    _private: [u8; 0],
}

// XLOG --- TODO(pg-port): real definitions live in access/xlog*.h -------------
#[repr(C)]
pub struct XLogReaderState {
    _private: [u8; 0],
}

pub const RM_CLOG_ID: u8 = 0; // TODO(pg-port): real value lives in access/rmgrlist.h
pub const XLR_INFO_MASK: uint8 = 0x0F; // TODO(pg-port): real value lives in access/xlogrecord.h

unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO(pg-port): real XLogBeginInsert lives in access/xloginsert.c
}
unsafe fn XLogRegisterData(_data: *mut c_char, _len: usize) {
    unimplemented!() // TODO(pg-port): real XLogRegisterData lives in access/xloginsert.c
}
unsafe fn XLogInsert(_rmid: u8, _info: uint8) -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): real XLogInsert lives in access/xloginsert.c
}
unsafe fn XLogFlush(_record: XLogRecPtr) {
    unimplemented!() // TODO(pg-port): real XLogFlush lives in access/xlog.c
}
unsafe fn XLogRecGetInfo(_record: *mut XLogReaderState) -> uint8 {
    unimplemented!() // TODO(pg-port): real XLogRecGetInfo lives in access/xlogreader.h
}
unsafe fn XLogRecGetData(_record: *mut XLogReaderState) -> *mut c_char {
    unimplemented!() // TODO(pg-port): real XLogRecGetData lives in access/xlogreader.h
}
unsafe fn XLogRecHasAnyBlockRefs(_record: *mut XLogReaderState) -> bool {
    unimplemented!() // TODO(pg-port): real XLogRecHasAnyBlockRefs lives in access/xlogreader.h
}

#[inline]
unsafe fn XLogRecPtrIsInvalid(lsn: XLogRecPtr) -> bool {
    lsn == 0 // TODO(pg-port): real XLogRecPtrIsInvalid lives in access/xlogdefs.h
}

// Recovery state --- TODO(pg-port): real InRecovery lives in access/xlogutils.h
pub static mut InRecovery: bool = false;

// Config / port --- TODO(pg-port): real SetConfigOption lives in utils/misc/guc.c
unsafe fn SetConfigOption(
    _name: *const c_char,
    _value: *const c_char,
    _context: c_int,
    _source: GucSource,
) {
    unimplemented!() // TODO(pg-port): real SetConfigOption lives in utils/misc/guc.c
}

// Atomics --- TODO(pg-port): real pg_atomic_* live in port/atomics.h ----------
#[repr(C)]
pub struct pg_atomic_uint64 {
    pub value: u64,
}
#[repr(C)]
pub struct pg_atomic_uint32 {
    pub value: u32,
}

unsafe fn pg_atomic_read_u32(_ptr: *mut pg_atomic_uint32) -> uint32 {
    unimplemented!() // TODO(pg-port): real pg_atomic_read_u32 lives in port/atomics.h
}
unsafe fn pg_atomic_write_u32(_ptr: *mut pg_atomic_uint32, _val: uint32) {
    unimplemented!() // TODO(pg-port): real pg_atomic_write_u32 lives in port/atomics.h
}
unsafe fn pg_atomic_exchange_u32(_ptr: *mut pg_atomic_uint32, _newval: uint32) -> uint32 {
    unimplemented!() // TODO(pg-port): real pg_atomic_exchange_u32 lives in port/atomics.h
}
unsafe fn pg_atomic_compare_exchange_u32(
    _ptr: *mut pg_atomic_uint32,
    _expected: *mut uint32,
    _newval: uint32,
) -> bool {
    unimplemented!() // TODO(pg-port): real pg_atomic_compare_exchange_u32 lives in port/atomics.h
}
unsafe fn pg_atomic_write_u64(_ptr: *mut pg_atomic_uint64, _val: u64) {
    unimplemented!() // TODO(pg-port): real pg_atomic_write_u64 lives in port/atomics.h
}
unsafe fn pg_write_barrier() {
    // TODO(pg-port): real pg_write_barrier lives in port/atomics.h
}

// Transaction id helpers --- TODO(pg-port): real defs live in access/transam.* */
#[repr(C)]
pub struct FullTransactionId {
    pub value: u64,
}

pub const InvalidTransactionId: TransactionId = 0; // TODO(pg-port): real value lives in access/transam.h
pub const FirstNormalTransactionId: TransactionId = 3; // TODO(pg-port): real value lives in access/transam.h
pub const MaxTransactionId: TransactionId = 0xFFFFFFFF; // TODO(pg-port): real value lives in access/transam.h

unsafe fn XidFromFullTransactionId(_fxid: FullTransactionId) -> TransactionId {
    unimplemented!() // TODO(pg-port): real XidFromFullTransactionId lives in access/transam.h
}
unsafe fn TransactionIdPrecedes(_id1: TransactionId, _id2: TransactionId) -> bool {
    unimplemented!() // TODO(pg-port): real TransactionIdPrecedes lives in access/transam.c
}
unsafe fn TransactionIdIsValid(_xid: TransactionId) -> bool {
    unimplemented!() // TODO(pg-port): real TransactionIdIsValid lives in access/transam.h
}
unsafe fn TransactionIdEquals(_id1: TransactionId, _id2: TransactionId) -> bool {
    unimplemented!() // TODO(pg-port): real TransactionIdEquals lives in access/transam.h
}
unsafe fn AdvanceOldestClogXid(_oldest_xact: TransactionId) {
    unimplemented!() // TODO(pg-port): real AdvanceOldestClogXid lives in access/transam/varsup.c
}

#[repr(C)]
pub struct TransamVariablesData {
    pub nextXid: FullTransactionId,
    // ... TODO(pg-port): access/transam.h
}
pub static mut TransamVariables: *mut TransamVariablesData = core::ptr::null_mut(); // TODO(pg-port): real TransamVariables lives in access/transam/varsup.c

// PGPROC / proc globals --- TODO(pg-port): real defs live in storage/proc.h ---
#[repr(C)]
pub struct PGPROC {
    pub xid: TransactionId,
    pub subxidStatus: XidCacheStatus,
    pub subxids: SubXidCache,
    pub sem: PGSemaphore,
    pub clogGroupMember: bool,
    pub clogGroupNext: pg_atomic_uint32,
    pub clogGroupMemberXid: TransactionId,
    pub clogGroupMemberXidStatus: XidStatus,
    pub clogGroupMemberPage: int64,
    pub clogGroupMemberLsn: XLogRecPtr,
    // ... TODO(pg-port): storage/proc.h
}

#[repr(C)]
pub struct XidCacheStatus {
    pub count: uint8,
    pub overflowed: bool,
}

pub const PGPROC_MAX_CACHED_SUBXIDS: c_int = 64; // TODO(pg-port): real value lives in storage/proc.h

#[repr(C)]
pub struct SubXidCache {
    pub xids: [TransactionId; PGPROC_MAX_CACHED_SUBXIDS as usize],
}

#[repr(C)]
pub struct PROC_HDR {
    pub allProcs: *mut PGPROC,
    pub clogGroupFirst: pg_atomic_uint32,
    // ... TODO(pg-port): storage/proc.h
}

pub static mut ProcGlobal: *mut PROC_HDR = core::ptr::null_mut(); // TODO(pg-port): real ProcGlobal lives in storage/lmgr/proc.c
pub static mut MyProc: *mut PGPROC = core::ptr::null_mut(); // TODO(pg-port): real MyProc lives in storage/lmgr/proc.c

// ProcNumber --- TODO(pg-port): real defs live in storage/procnumber.h --------
pub static mut MyProcNumber: c_int = 0; // TODO(pg-port): real MyProcNumber lives in storage/procnumber.h
pub const INVALID_PROC_NUMBER: c_int = -1; // TODO(pg-port): real value lives in storage/procnumber.h

unsafe fn GetPGProcByNumber(_n: c_int) -> *mut PGPROC {
    unimplemented!() // TODO(pg-port): real GetPGProcByNumber lives in storage/proc.h
}

// Semaphore --- TODO(pg-port): real defs live in storage/pg_sema.h ------------
pub type PGSemaphore = *mut c_void;
unsafe fn PGSemaphoreLock(_sema: PGSemaphore) {
    unimplemented!() // TODO(pg-port): real PGSemaphoreLock lives in port/*_sema.c
}
unsafe fn PGSemaphoreUnlock(_sema: PGSemaphore) {
    unimplemented!() // TODO(pg-port): real PGSemaphoreUnlock lives in port/*_sema.c
}

// pgstat wait events --- TODO(pg-port): real defs live in utils/wait_event.h --
pub const WAIT_EVENT_XACT_GROUP_UPDATE: uint32 = 0; // TODO(pg-port): real value lives in utils/wait_event.h
unsafe fn pgstat_report_wait_start(_wait_event_info: uint32) {
    unimplemented!() // TODO(pg-port): real pgstat_report_wait_start lives in utils/activity/wait_event.c
}
unsafe fn pgstat_report_wait_end() {
    unimplemented!() // TODO(pg-port): real pgstat_report_wait_end lives in utils/activity/wait_event.c
}

// Tracepoints --- TODO(pg-port): real defs live in pg_trace.h (DTrace) --------
#[inline]
unsafe fn TRACE_POSTGRESQL_CLOG_CHECKPOINT_START(_arg: bool) {}
#[inline]
unsafe fn TRACE_POSTGRESQL_CLOG_CHECKPOINT_DONE(_arg: bool) {}

// Misc helpers ----------------------------------------------------------------
#[allow(non_snake_case)]
unsafe fn Min(a: c_int, b: c_int) -> c_int {
    if a < b {
        a
    } else {
        b
    }
}
#[allow(non_snake_case)]
unsafe fn Max(a: c_int, b: c_int) -> c_int {
    if a > b {
        a
    } else {
        b
    }
}
#[allow(non_snake_case)]
const fn Min_const(a: c_int, b: c_int) -> c_int {
    if a < b {
        a
    } else {
        b
    }
}
#[allow(non_snake_case)]
unsafe fn Assert(_cond: bool) {}
