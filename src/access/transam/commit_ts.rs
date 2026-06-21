//! src/backend/access/transam/commit_ts.c
//!
//! PostgreSQL commit timestamp manager
//!
//! This module is a pg_xact-like system that stores the commit timestamp
//! for each transaction.
//!
//! XLOG interactions: this module generates an XLOG record whenever a new
//! CommitTs page is initialized to zeroes.  Other writes of CommitTS come
//! from recording of transaction commit in xact.c, which generates its own
//! XLOG records for these events and will re-perform the status update on
//! redo; so we need make no additional XLOG entry here.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! src/backend/access/transam/commit_ts.c

use crate::prelude::*;
use crate::miscadmin::TimestampTz;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::pg_config::BLCKSZ;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int64, uint8, Size, TransactionId};

// ----------------------------------------------------------------------------
// commit_ts.h
//
// src/include/access/commit_ts.h
// ----------------------------------------------------------------------------

/* XLOG stuff */
pub const COMMIT_TS_ZEROPAGE: uint8 = 0x00;
pub const COMMIT_TS_TRUNCATE: uint8 = 0x10;

#[repr(C)]
pub struct xl_commit_ts_set {
    pub timestamp: TimestampTz,
    pub nodeid: RepOriginId,
    pub mainxid: TransactionId,
    /* subxact Xids follow */
}

pub const SizeOfCommitTsSet: usize =
    core::mem::offset_of!(xl_commit_ts_set, mainxid) + core::mem::size_of::<TransactionId>();

#[repr(C)]
pub struct xl_commit_ts_truncate {
    pub pageno: int64,
    pub oldestXid: TransactionId,
}

pub const SizeOfCommitTsTruncate: usize =
    core::mem::offset_of!(xl_commit_ts_truncate, oldestXid) + core::mem::size_of::<TransactionId>();

// ----------------------------------------------------------------------------
// commit_ts.c
// ----------------------------------------------------------------------------

/*
 * Defines for CommitTs page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CommitTs page numbering also wraps around at
 * 0xFFFFFFFF/COMMIT_TS_XACTS_PER_PAGE, and CommitTs segment numbering at
 * 0xFFFFFFFF/COMMIT_TS_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCommitTs (see CommitTsPagePrecedes).
 */

/*
 * We need 8+2 bytes per xact.  Note that enlarging this struct might mean
 * the largest possible file name is more than 5 chars long; see
 * SlruScanDirectory.
 */
#[repr(C)]
struct CommitTimestampEntry {
    time: TimestampTz,
    nodeid: RepOriginId,
}

const SizeOfCommitTimestampEntry: usize =
    core::mem::offset_of!(CommitTimestampEntry, nodeid) + core::mem::size_of::<RepOriginId>();

const COMMIT_TS_XACTS_PER_PAGE: usize = BLCKSZ as usize / SizeOfCommitTimestampEntry;

/*
 * Although we return an int64 the actual value can't currently exceed
 * 0xFFFFFFFF/COMMIT_TS_XACTS_PER_PAGE.
 */
#[inline]
fn TransactionIdToCTsPage(xid: TransactionId) -> int64 {
    xid as int64 / COMMIT_TS_XACTS_PER_PAGE as int64
}

#[inline]
fn TransactionIdToCTsEntry(xid: TransactionId) -> TransactionId {
    xid % (COMMIT_TS_XACTS_PER_PAGE as TransactionId)
}

/*
 * Link to shared-memory data structures for CommitTs control
 */
static mut CommitTsCtlData: SlruCtlData = unsafe { core::mem::zeroed() };

#[inline]
fn CommitTsCtl() -> SlruCtl {
    core::ptr::addr_of_mut!(CommitTsCtlData)
}

/*
 * We keep a cache of the last value set in shared memory.
 *
 * This is also good place to keep the activation status.  We keep this
 * separate from the GUC so that the standby can activate the module if the
 * primary has it active independently of the value of the GUC.
 *
 * This is protected by CommitTsLock.  In some places, we use commitTsActive
 * without acquiring the lock; where this happens, a comment explains the
 * rationale for it.
 */
#[repr(C)]
struct CommitTimestampShared {
    xidLastCommit: TransactionId,
    dataLastCommit: CommitTimestampEntry,
    commitTsActive: bool,
}

static mut commitTsShared: *mut CommitTimestampShared = core::ptr::null_mut();

/* GUC variable */
#[no_mangle]
pub static mut track_commit_timestamp: bool = false;

/*
 * TransactionTreeSetCommitTsData
 *
 * Record the final commit timestamp of transaction entries in the commit log
 * for a transaction and its subtransaction tree, as efficiently as possible.
 *
 * xid is the top level transaction id.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 * The reason why tracking just the parent xid commit timestamp is not enough
 * is that the subtrans SLRU does not stay valid across crashes (it's not
 * permanent) so we need to keep the information about them here. If the
 * subtrans implementation changes in the future, we might want to revisit the
 * decision of storing timestamp info for each subxid.
 */
pub unsafe fn TransactionTreeSetCommitTsData(
    xid: TransactionId,
    nsubxids: c_int,
    subxids: *mut TransactionId,
    timestamp: TimestampTz,
    nodeid: RepOriginId,
) {
    let i: c_int;
    let mut headxid: TransactionId;
    let newestXact: TransactionId;

    /*
     * No-op if the module is not active.
     *
     * An unlocked read here is fine, because in a standby (the only place
     * where the flag can change in flight) this routine is only called by the
     * recovery process, which is also the only process which can change the
     * flag.
     */
    if !(*commitTsShared).commitTsActive {
        return;
    }

    /*
     * Figure out the latest Xid in this batch: either the last subxid if
     * there's any, otherwise the parent xid.
     */
    if nsubxids > 0 {
        newestXact = *subxids.add((nsubxids - 1) as usize);
    } else {
        newestXact = xid;
    }

    /*
     * We split the xids to set the timestamp to in groups belonging to the
     * same SLRU page; the first element in each such set is its head.  The
     * first group has the main XID as the head; subsequent sets use the first
     * subxid not on the previous page as head.  This way, we only have to
     * lock/modify each SLRU page once.
     */
    headxid = xid;
    let i = 0;
    let _ = i;
    let mut i: c_int = 0;
    loop {
        let pageno = TransactionIdToCTsPage(headxid);
        let mut j: c_int;

        j = i;
        while j < nsubxids {
            if TransactionIdToCTsPage(*subxids.add(j as usize)) != pageno {
                break;
            }
            j += 1;
        }
        /* subxids[i..j] are on the same page as the head */

        SetXidCommitTsInPage(
            headxid,
            j - i,
            subxids.add(i as usize),
            timestamp,
            nodeid,
            pageno,
        );

        /* if we wrote out all subxids, we're done. */
        if j >= nsubxids {
            break;
        }

        /*
         * Set the new head and skip over it, as well as over the subxids we
         * just wrote.
         */
        headxid = *subxids.add(j as usize);
        i = j + 1;
    }
    let _ = i;

    /* update the cached value in shared memory */
    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE);
    (*commitTsShared).xidLastCommit = xid;
    (*commitTsShared).dataLastCommit.time = timestamp;
    (*commitTsShared).dataLastCommit.nodeid = nodeid;

    /* and move forwards our endpoint, if needed */
    if TransactionIdPrecedes((*TransamVariables).newestCommitTsXid, newestXact) {
        (*TransamVariables).newestCommitTsXid = newestXact;
    }
    LWLockRelease(CommitTsLock());
}

/*
 * Record the commit timestamp of transaction entries in the commit log for all
 * entries on a single page.  Atomic only on this page.
 */
unsafe fn SetXidCommitTsInPage(
    xid: TransactionId,
    nsubxids: c_int,
    subxids: *mut TransactionId,
    ts: TimestampTz,
    nodeid: RepOriginId,
    pageno: int64,
) {
    let lock = SimpleLruGetBankLock(CommitTsCtl(), pageno);
    let slotno: c_int;

    LWLockAcquire(lock, LW_EXCLUSIVE);

    slotno = SimpleLruReadPage(CommitTsCtl(), pageno, true, xid);

    TransactionIdSetCommitTs(xid, ts, nodeid, slotno);
    let mut i = 0;
    while i < nsubxids {
        TransactionIdSetCommitTs(*subxids.add(i as usize), ts, nodeid, slotno);
        i += 1;
    }

    *(*(*CommitTsCtl()).shared).page_dirty.add(slotno as usize) = true;

    LWLockRelease(lock);
}

/*
 * Sets the commit timestamp of a single transaction.
 *
 * Caller must hold the correct SLRU bank lock, will be held at exit
 */
unsafe fn TransactionIdSetCommitTs(
    xid: TransactionId,
    ts: TimestampTz,
    nodeid: RepOriginId,
    slotno: c_int,
) {
    let entryno = TransactionIdToCTsEntry(xid);
    let entry: CommitTimestampEntry;

    Assert(TransactionIdIsNormal(xid));

    entry = CommitTimestampEntry {
        time: ts,
        nodeid,
    };

    memcpy(
        (*(*(*CommitTsCtl()).shared).page_buffer.add(slotno as usize))
            .add(SizeOfCommitTimestampEntry * entryno as usize) as *mut c_void,
        core::ptr::addr_of!(entry) as *const c_void,
        SizeOfCommitTimestampEntry,
    );
}

/*
 * Interrogate the commit timestamp of a transaction.
 *
 * The return value indicates whether a commit timestamp record was found for
 * the given xid.  The timestamp value is returned in *ts (which may not be
 * null), and the origin node for the Xid is returned in *nodeid, if it's not
 * null.
 */
pub unsafe fn TransactionIdGetCommitTsData(
    xid: TransactionId,
    ts: *mut TimestampTz,
    nodeid: *mut RepOriginId,
) -> bool {
    let pageno = TransactionIdToCTsPage(xid);
    let entryno = TransactionIdToCTsEntry(xid);
    let slotno: c_int;
    let mut entry: CommitTimestampEntry = core::mem::zeroed();
    let oldestCommitTsXid: TransactionId;
    let newestCommitTsXid: TransactionId;

    if !TransactionIdIsValid(xid) {
        elog!(
            ERROR,
            "cannot retrieve commit timestamp for transaction {}",
            xid
        );
        unreachable!();
    } else if !TransactionIdIsNormal(xid) {
        /* frozen and bootstrap xids are always committed far in the past */
        *ts = 0;
        if !nodeid.is_null() {
            *nodeid = 0;
        }
        return false;
    }

    LWLockAcquire(CommitTsLock(), LW_SHARED);

    /* Error if module not enabled */
    if !(*commitTsShared).commitTsActive {
        error_commit_ts_disabled();
    }

    /*
     * If we're asked for the cached value, return that.  Otherwise, fall
     * through to read from SLRU.
     */
    if (*commitTsShared).xidLastCommit == xid {
        *ts = (*commitTsShared).dataLastCommit.time;
        if !nodeid.is_null() {
            *nodeid = (*commitTsShared).dataLastCommit.nodeid;
        }

        LWLockRelease(CommitTsLock());
        return *ts != 0;
    }

    oldestCommitTsXid = (*TransamVariables).oldestCommitTsXid;
    newestCommitTsXid = (*TransamVariables).newestCommitTsXid;
    /* neither is invalid, or both are */
    Assert(TransactionIdIsValid(oldestCommitTsXid) == TransactionIdIsValid(newestCommitTsXid));
    LWLockRelease(CommitTsLock());

    /*
     * Return empty if the requested value is outside our valid range.
     */
    if !TransactionIdIsValid(oldestCommitTsXid)
        || TransactionIdPrecedes(xid, oldestCommitTsXid)
        || TransactionIdPrecedes(newestCommitTsXid, xid)
    {
        *ts = 0;
        if !nodeid.is_null() {
            *nodeid = InvalidRepOriginId;
        }
        return false;
    }

    /* lock is acquired by SimpleLruReadPage_ReadOnly */
    slotno = SimpleLruReadPage_ReadOnly(CommitTsCtl(), pageno, xid);
    memcpy(
        core::ptr::addr_of_mut!(entry) as *mut c_void,
        (*(*(*CommitTsCtl()).shared).page_buffer.add(slotno as usize))
            .add(SizeOfCommitTimestampEntry * entryno as usize) as *const c_void,
        SizeOfCommitTimestampEntry,
    );

    *ts = entry.time;
    if !nodeid.is_null() {
        *nodeid = entry.nodeid;
    }

    LWLockRelease(SimpleLruGetBankLock(CommitTsCtl(), pageno));
    *ts != 0
}

/*
 * Return the Xid of the latest committed transaction.  (As far as this module
 * is concerned, anyway; it's up to the caller to ensure the value is useful
 * for its purposes.)
 *
 * ts and nodeid are filled with the corresponding data; they can be passed
 * as NULL if not wanted.
 */
pub unsafe fn GetLatestCommitTsData(
    ts: *mut TimestampTz,
    nodeid: *mut RepOriginId,
) -> TransactionId {
    let xid: TransactionId;

    LWLockAcquire(CommitTsLock(), LW_SHARED);

    /* Error if module not enabled */
    if !(*commitTsShared).commitTsActive {
        error_commit_ts_disabled();
    }

    xid = (*commitTsShared).xidLastCommit;
    if !ts.is_null() {
        *ts = (*commitTsShared).dataLastCommit.time;
    }
    if !nodeid.is_null() {
        *nodeid = (*commitTsShared).dataLastCommit.nodeid;
    }
    LWLockRelease(CommitTsLock());

    xid
}

unsafe fn error_commit_ts_disabled() {
    /*
     * ereport(ERROR,
     *   (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
     *    errmsg("could not get commit timestamp data"),
     *    RecoveryInProgress() ?
     *    errhint("Make sure the configuration parameter \"%s\" is set on the primary server.",
     *            "track_commit_timestamp") :
     *    errhint("Make sure the configuration parameter \"%s\" is set.",
     *            "track_commit_timestamp")));
     */
    ereport!(ERROR, "could not get commit timestamp data");
    unreachable!();
}

/*
 * SQL-callable wrapper to obtain commit time of a transaction
 */
pub unsafe fn pg_xact_commit_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let xid: TransactionId = PG_GETARG_TRANSACTIONID(fcinfo, 0);
    let mut ts: TimestampTz = 0;
    let found: bool;

    found = TransactionIdGetCommitTsData(xid, &mut ts, core::ptr::null_mut());

    if !found {
        return PG_RETURN_NULL(fcinfo);
    }

    PG_RETURN_TIMESTAMPTZ(ts)
}

/*
 * pg_last_committed_xact
 *
 * SQL-callable wrapper to obtain some information about the latest
 * committed transaction: transaction ID, timestamp and replication
 * origin.
 */
pub unsafe fn pg_last_committed_xact(fcinfo: FunctionCallInfo) -> Datum {
    let xid: TransactionId;
    let mut nodeid: RepOriginId = 0;
    let mut ts: TimestampTz = 0;
    let mut values: [Datum; 3] = [0; 3];
    let mut nulls: [bool; 3] = [false; 3];
    let mut tupdesc: TupleDesc = core::ptr::null_mut();
    let htup: HeapTuple;

    /* and construct a tuple with our data */
    xid = GetLatestCommitTsData(&mut ts, &mut nodeid);

    if get_call_result_type(fcinfo, core::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    if !TransactionIdIsNormal(xid) {
        memset(
            nulls.as_mut_ptr() as *mut c_void,
            true as c_int,
            core::mem::size_of_val(&nulls),
        );
    } else {
        values[0] = TransactionIdGetDatum(xid);
        nulls[0] = false;

        values[1] = TimestampTzGetDatum(ts);
        nulls[1] = false;

        values[2] = ObjectIdGetDatum(nodeid as Oid);
        nulls[2] = false;
    }

    htup = heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());

    PG_RETURN_DATUM(HeapTupleGetDatum(htup))
}

/*
 * pg_xact_commit_timestamp_origin
 *
 * SQL-callable wrapper to obtain commit timestamp and replication origin
 * of a given transaction.
 */
pub unsafe fn pg_xact_commit_timestamp_origin(fcinfo: FunctionCallInfo) -> Datum {
    let xid: TransactionId = PG_GETARG_TRANSACTIONID(fcinfo, 0);
    let mut nodeid: RepOriginId = 0;
    let mut ts: TimestampTz = 0;
    let mut values: [Datum; 2] = [0; 2];
    let mut nulls: [bool; 2] = [false; 2];
    let mut tupdesc: TupleDesc = core::ptr::null_mut();
    let htup: HeapTuple;
    let found: bool;

    found = TransactionIdGetCommitTsData(xid, &mut ts, &mut nodeid);

    if get_call_result_type(fcinfo, core::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    if !found {
        memset(
            nulls.as_mut_ptr() as *mut c_void,
            true as c_int,
            core::mem::size_of_val(&nulls),
        );
    } else {
        values[0] = TimestampTzGetDatum(ts);
        nulls[0] = false;

        values[1] = ObjectIdGetDatum(nodeid as Oid);
        nulls[1] = false;
    }

    htup = heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());

    PG_RETURN_DATUM(HeapTupleGetDatum(htup))
}

/*
 * Number of shared CommitTS buffers.
 *
 * If asked to autotune, use 2MB for every 1GB of shared buffers, up to 8MB.
 * Otherwise just cap the configured amount to be between 16 and the maximum
 * allowed.
 */
unsafe fn CommitTsShmemBuffers() -> c_int {
    /* auto-tune based on shared buffers */
    if commit_timestamp_buffers == 0 {
        return SimpleLruAutotuneBuffers(512, 1024);
    }

    Min(Max(16, commit_timestamp_buffers), SLRU_MAX_ALLOWED_BUFFERS)
}

/*
 * Shared memory sizing for CommitTs
 */
pub unsafe fn CommitTsShmemSize() -> Size {
    SimpleLruShmemSize(CommitTsShmemBuffers(), 0)
        + core::mem::size_of::<CommitTimestampShared>()
}

/*
 * Initialize CommitTs at system startup (postmaster start or standalone
 * backend)
 */
pub unsafe fn CommitTsShmemInit() {
    let mut found: bool = false;

    /* If auto-tuning is requested, now is the time to do it */
    if commit_timestamp_buffers == 0 {
        let mut buf: [c_char; 32] = [0; 32];

        snprintf(
            buf.as_mut_ptr(),
            core::mem::size_of_val(&buf),
            c"%d".as_ptr(),
            CommitTsShmemBuffers(),
        );
        SetConfigOption(
            c"commit_timestamp_buffers".as_ptr(),
            buf.as_ptr(),
            PGC_POSTMASTER,
            PGC_S_DYNAMIC_DEFAULT,
        );

        /*
         * We prefer to report this value's source as PGC_S_DYNAMIC_DEFAULT.
         * However, if the DBA explicitly set commit_timestamp_buffers = 0 in
         * the config file, then PGC_S_DYNAMIC_DEFAULT will fail to override
         * that and we must force the matter with PGC_S_OVERRIDE.
         */
        if commit_timestamp_buffers == 0
        /* failed to apply it? */
        {
            SetConfigOption(
                c"commit_timestamp_buffers".as_ptr(),
                buf.as_ptr(),
                PGC_POSTMASTER,
                PGC_S_OVERRIDE,
            );
        }
    }
    Assert(commit_timestamp_buffers != 0);

    (*CommitTsCtl()).PagePrecedes = Some(CommitTsPagePrecedes);
    SimpleLruInit(
        CommitTsCtl(),
        c"commit_timestamp".as_ptr(),
        CommitTsShmemBuffers(),
        0,
        c"pg_commit_ts".as_ptr(),
        LWTRANCHE_COMMITTS_BUFFER,
        LWTRANCHE_COMMITTS_SLRU,
        SYNC_HANDLER_COMMIT_TS,
        false,
    );
    SlruPagePrecedesUnitTests(CommitTsCtl(), COMMIT_TS_XACTS_PER_PAGE as c_int);

    commitTsShared = ShmemInitStruct(
        c"CommitTs shared".as_ptr(),
        core::mem::size_of::<CommitTimestampShared>(),
        &mut found,
    ) as *mut CommitTimestampShared;

    if !IsUnderPostmaster {
        Assert(!found);

        (*commitTsShared).xidLastCommit = InvalidTransactionId;
        TIMESTAMP_NOBEGIN(&mut (*commitTsShared).dataLastCommit.time);
        (*commitTsShared).dataLastCommit.nodeid = InvalidRepOriginId;
        (*commitTsShared).commitTsActive = false;
    } else {
        Assert(found);
    }
}

/*
 * GUC check_hook for commit_timestamp_buffers
 */
pub unsafe fn check_commit_ts_buffers(
    newval: *mut c_int,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    check_slru_buffers(c"commit_timestamp_buffers".as_ptr(), newval)
}

/*
 * This function must be called ONCE on system install.
 *
 * (The CommitTs directory is assumed to have been created by initdb, and
 * CommitTsShmemInit must have been called already.)
 */
pub unsafe fn BootStrapCommitTs() {
    /*
     * Nothing to do here at present, unlike most other SLRU modules; segments
     * are created when the server is started with this module enabled. See
     * ActivateCommitTs.
     */
}

/*
 * Initialize (or reinitialize) a page of CommitTs to zeroes.
 * If writeXlog is true, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
unsafe fn ZeroCommitTsPage(pageno: int64, writeXlog: bool) -> c_int {
    let slotno: c_int;

    slotno = SimpleLruZeroPage(CommitTsCtl(), pageno);

    if writeXlog {
        WriteZeroPageXlogRec(pageno);
    }

    slotno
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized TransamVariables->nextXid.
 */
pub unsafe fn StartupCommitTs() {
    ActivateCommitTs();
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after recovery has finished.
 */
pub unsafe fn CompleteCommitTsInitialization() {
    /*
     * If the feature is not enabled, turn it off for good.  This also removes
     * any leftover data.
     *
     * Conversely, we activate the module if the feature is enabled.  This is
     * necessary for primary and standby as the activation depends on the
     * control file contents at the beginning of recovery or when a
     * XLOG_PARAMETER_CHANGE is replayed.
     */
    if !track_commit_timestamp {
        DeactivateCommitTs();
    } else {
        ActivateCommitTs();
    }
}

/*
 * Activate or deactivate CommitTs' upon reception of a XLOG_PARAMETER_CHANGE
 * XLog record during recovery.
 */
pub unsafe fn CommitTsParameterChange(newvalue: bool, _oldvalue: bool) {
    /*
     * If the commit_ts module is disabled in this server and we get word from
     * the primary server that it is enabled there, activate it so that we can
     * replay future WAL records involving it; also mark it as active on
     * pg_control.  If the old value was already set, we already did this, so
     * don't do anything.
     *
     * If the module is disabled in the primary, disable it here too, unless
     * the module is enabled locally.
     *
     * Note this only runs in the recovery process, so an unlocked read is
     * fine.
     */
    if newvalue {
        if !(*commitTsShared).commitTsActive {
            ActivateCommitTs();
        }
    } else if (*commitTsShared).commitTsActive {
        DeactivateCommitTs();
    }
}

/*
 * Activate this module whenever necessary.
 *		This must happen during postmaster or standalone-backend startup,
 *		or during WAL replay anytime the track_commit_timestamp setting is
 *		changed in the primary.
 *
 * The reason why this SLRU needs separate activation/deactivation functions is
 * that it can be enabled/disabled during start and the activation/deactivation
 * on the primary is propagated to the standby via replay. Other SLRUs don't
 * have this property and they can be just initialized during normal startup.
 *
 * This is in charge of creating the currently active segment, if it's not
 * already there.  The reason for this is that the server might have been
 * running with this module disabled for a while and thus might have skipped
 * the normal creation point.
 */
unsafe fn ActivateCommitTs() {
    let xid: TransactionId;
    let pageno: int64;

    /*
     * During bootstrap, we should not register commit timestamps so skip the
     * activation in this case.
     */
    if IsBootstrapProcessingMode() {
        return;
    }

    /* If we've done this already, there's nothing to do */
    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE);
    if (*commitTsShared).commitTsActive {
        LWLockRelease(CommitTsLock());
        return;
    }
    LWLockRelease(CommitTsLock());

    xid = XidFromFullTransactionId(core::ptr::read(&(*TransamVariables).nextXid));
    pageno = TransactionIdToCTsPage(xid);

    /*
     * Re-Initialize our idea of the latest page number.
     */
    pg_atomic_write_u64(
        &raw mut (*(*CommitTsCtl()).shared).latest_page_number as *mut _,
        pageno as u64,
    );

    /*
     * If CommitTs is enabled, but it wasn't in the previous server run, we
     * need to set the oldest and newest values to the next Xid; that way, we
     * will not try to read data that might not have been set.
     *
     * XXX does this have a problem if a server is started with commitTs
     * enabled, then started with commitTs disabled, then restarted with it
     * enabled again?  It doesn't look like it does, because there should be a
     * checkpoint that sets the value to InvalidTransactionId at end of
     * recovery; and so any chance of injecting new transactions without
     * CommitTs values would occur after the oldestCommitTsXid has been set to
     * Invalid temporarily.
     */
    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE);
    if (*TransamVariables).oldestCommitTsXid == InvalidTransactionId {
        let next = ReadNextTransactionId();
        (*TransamVariables).newestCommitTsXid = next;
        (*TransamVariables).oldestCommitTsXid = next;
    }
    LWLockRelease(CommitTsLock());

    /* Create the current segment file, if necessary */
    if !SimpleLruDoesPhysicalPageExist(CommitTsCtl(), pageno) {
        let lock = SimpleLruGetBankLock(CommitTsCtl(), pageno);
        let slotno: c_int;

        LWLockAcquire(lock, LW_EXCLUSIVE);
        slotno = ZeroCommitTsPage(pageno, false);
        SimpleLruWritePage(CommitTsCtl(), slotno);
        Assert(!*(*(*CommitTsCtl()).shared).page_dirty.add(slotno as usize));
        LWLockRelease(lock);
    }

    /* Change the activation status in shared memory. */
    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE);
    (*commitTsShared).commitTsActive = true;
    LWLockRelease(CommitTsLock());
}

/*
 * Deactivate this module.
 *
 * This must be called when the track_commit_timestamp parameter is turned off.
 * This happens during postmaster or standalone-backend startup, or during WAL
 * replay.
 *
 * Resets CommitTs into invalid state to make sure we don't hand back
 * possibly-invalid data; also removes segments of old data.
 */
unsafe fn DeactivateCommitTs() {
    /*
     * Cleanup the status in the shared memory.
     *
     * We reset everything in the commitTsShared record to prevent user from
     * getting confusing data about last committed transaction on the standby
     * when the module was activated repeatedly on the primary.
     */
    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE);

    (*commitTsShared).commitTsActive = false;
    (*commitTsShared).xidLastCommit = InvalidTransactionId;
    TIMESTAMP_NOBEGIN(&mut (*commitTsShared).dataLastCommit.time);
    (*commitTsShared).dataLastCommit.nodeid = InvalidRepOriginId;

    (*TransamVariables).oldestCommitTsXid = InvalidTransactionId;
    (*TransamVariables).newestCommitTsXid = InvalidTransactionId;

    /*
     * Remove *all* files.  This is necessary so that there are no leftover
     * files; in the case where this feature is later enabled after running
     * with it disabled for some time there may be a gap in the file sequence.
     * (We can probably tolerate out-of-sequence files, as they are going to
     * be overwritten anyway when we wrap around, but it seems better to be
     * tidy.)
     *
     * Note that we do this with CommitTsLock acquired in exclusive mode. This
     * is very heavy-handed, but since this routine can only be called in the
     * replica and should happen very rarely, we don't worry too much about
     * it.  Note also that no process should be consulting this SLRU if we
     * have just deactivated it.
     */
    SlruScanDirectory(CommitTsCtl(), SlruScanDirCbDeleteAll, core::ptr::null_mut());

    LWLockRelease(CommitTsLock());
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
pub unsafe fn CheckPointCommitTs() {
    /*
     * Write dirty CommitTs pages to disk.  This may result in sync requests
     * queued for later handling by ProcessSyncRequests(), as part of the
     * checkpoint.
     */
    SimpleLruWriteAll(CommitTsCtl(), true);
}

/*
 * Make sure that CommitTs has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty CommitTs or xlog page to make room
 * in shared memory.
 *
 * NB: the current implementation relies on track_commit_timestamp being
 * PGC_POSTMASTER.
 */
pub unsafe fn ExtendCommitTs(newestXact: TransactionId) {
    let pageno: int64;
    let lock: *mut LWLock;

    /*
     * Nothing to do if module not enabled.  Note we do an unlocked read of
     * the flag here, which is okay because this routine is only called from
     * GetNewTransactionId, which is never called in a standby.
     */
    Assert(!InRecovery);
    if !(*commitTsShared).commitTsActive {
        return;
    }

    /*
     * No work except at first XID of a page.  But beware: just after
     * wraparound, the first XID of page zero is FirstNormalTransactionId.
     */
    if TransactionIdToCTsEntry(newestXact) != 0
        && !TransactionIdEquals(newestXact, FirstNormalTransactionId)
    {
        return;
    }

    pageno = TransactionIdToCTsPage(newestXact);

    lock = SimpleLruGetBankLock(CommitTsCtl(), pageno);

    LWLockAcquire(lock, LW_EXCLUSIVE);

    /* Zero the page and make an XLOG entry about it */
    ZeroCommitTsPage(pageno, !InRecovery);

    LWLockRelease(lock);
}

/*
 * Remove all CommitTs segments before the one holding the passed
 * transaction ID.
 *
 * Note that we don't need to flush XLOG here.
 */
pub unsafe fn TruncateCommitTs(oldestXact: TransactionId) {
    let mut cutoffPage: int64;

    /*
     * The cutoff point is the start of the segment containing oldestXact. We
     * pass the *page* containing oldestXact to SimpleLruTruncate.
     */
    cutoffPage = TransactionIdToCTsPage(oldestXact);

    /* Check to see if there's any files that could be removed */
    if !SlruScanDirectory(
        CommitTsCtl(),
        SlruScanDirCbReportPresence,
        core::ptr::addr_of_mut!(cutoffPage) as *mut c_void,
    ) {
        return; /* nothing to remove */
    }

    /* Write XLOG record */
    WriteTruncateXlogRec(cutoffPage, oldestXact);

    /* Now we can remove the old CommitTs segment(s) */
    SimpleLruTruncate(CommitTsCtl(), cutoffPage);
}

/*
 * Set the limit values between which commit TS can be consulted.
 */
pub unsafe fn SetCommitTsLimit(oldestXact: TransactionId, newestXact: TransactionId) {
    /*
     * Be careful not to overwrite values that are either further into the
     * "future" or signal a disabled committs.
     */
    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE);
    if (*TransamVariables).oldestCommitTsXid != InvalidTransactionId {
        if TransactionIdPrecedes((*TransamVariables).oldestCommitTsXid, oldestXact) {
            (*TransamVariables).oldestCommitTsXid = oldestXact;
        }
        if TransactionIdPrecedes(newestXact, (*TransamVariables).newestCommitTsXid) {
            (*TransamVariables).newestCommitTsXid = newestXact;
        }
    } else {
        Assert((*TransamVariables).newestCommitTsXid == InvalidTransactionId);
        (*TransamVariables).oldestCommitTsXid = oldestXact;
        (*TransamVariables).newestCommitTsXid = newestXact;
    }
    LWLockRelease(CommitTsLock());
}

/*
 * Move forwards the oldest commitTS value that can be consulted
 */
pub unsafe fn AdvanceOldestCommitTsXid(oldestXact: TransactionId) {
    LWLockAcquire(CommitTsLock(), LW_EXCLUSIVE);
    if (*TransamVariables).oldestCommitTsXid != InvalidTransactionId
        && TransactionIdPrecedes((*TransamVariables).oldestCommitTsXid, oldestXact)
    {
        (*TransamVariables).oldestCommitTsXid = oldestXact;
    }
    LWLockRelease(CommitTsLock());
}

/*
 * Decide whether a commitTS page number is "older" for truncation purposes.
 * Analogous to CLOGPagePrecedes().
 *
 * At default BLCKSZ, (1 << 31) % COMMIT_TS_XACTS_PER_PAGE == 128.  This
 * introduces differences compared to CLOG and the other SLRUs having (1 <<
 * 31) % per_page == 0.  This function never tests exactly
 * TransactionIdPrecedes(x-2^31, x).  When the system reaches xidStopLimit,
 * there are two possible counts of page boundaries between oldestXact and the
 * latest XID assigned, depending on whether oldestXact is within the first
 * 128 entries of its page.  Since this function doesn't know the location of
 * oldestXact within page2, it returns false for one page that actually is
 * expendable.  This is a wider (yet still negligible) version of the
 * truncation opportunity that CLOGPagePrecedes() cannot recognize.
 *
 * For the sake of a worked example, number entries with decimal values such
 * that page1==1 entries range from 1.0 to 1.999.  Let N+0.15 be the number of
 * pages that 2^31 entries will span (N is an integer).  If oldestXact=N+2.1,
 * then the final safe XID assignment leaves newestXact=1.95.  We keep page 2,
 * because entry=2.85 is the border that toggles whether entries precede the
 * last entry of the oldestXact page.  While page 2 is expendable at
 * oldestXact=N+2.1, it would be precious at oldestXact=N+2.9.
 */
unsafe extern "C" fn CommitTsPagePrecedes(page1: int64, page2: int64) -> bool {
    let mut xid1: TransactionId;
    let mut xid2: TransactionId;

    xid1 = (page1 as TransactionId).wrapping_mul(COMMIT_TS_XACTS_PER_PAGE as TransactionId);
    xid1 = xid1.wrapping_add(FirstNormalTransactionId + 1);
    xid2 = (page2 as TransactionId).wrapping_mul(COMMIT_TS_XACTS_PER_PAGE as TransactionId);
    xid2 = xid2.wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(
            xid1,
            xid2.wrapping_add(COMMIT_TS_XACTS_PER_PAGE as TransactionId - 1),
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
    XLogInsert(RM_COMMIT_TS_ID, COMMIT_TS_ZEROPAGE);
}

/*
 * Write a TRUNCATE xlog record
 */
unsafe fn WriteTruncateXlogRec(pageno: int64, oldestXid: TransactionId) {
    let xlrec = xl_commit_ts_truncate { pageno, oldestXid };

    XLogBeginInsert();
    XLogRegisterData(
        core::ptr::addr_of!(xlrec) as *mut c_char,
        SizeOfCommitTsTruncate,
    );
    XLogInsert(RM_COMMIT_TS_ID, COMMIT_TS_TRUNCATE);
}

/*
 * CommitTS resource manager's routines
 */
pub unsafe fn commit_ts_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    /* Backup blocks are not used in commit_ts records */
    Assert(!XLogRecHasAnyBlockRefs(record));

    if info == COMMIT_TS_ZEROPAGE {
        let mut pageno: int64 = 0;
        let slotno: c_int;
        let lock: *mut LWLock;

        memcpy(
            core::ptr::addr_of_mut!(pageno) as *mut c_void,
            XLogRecGetData(record) as *const c_void,
            core::mem::size_of_val(&pageno),
        );

        lock = SimpleLruGetBankLock(CommitTsCtl(), pageno);
        LWLockAcquire(lock, LW_EXCLUSIVE);

        slotno = ZeroCommitTsPage(pageno, false);
        SimpleLruWritePage(CommitTsCtl(), slotno);
        Assert(!*(*(*CommitTsCtl()).shared).page_dirty.add(slotno as usize));

        LWLockRelease(lock);
    } else if info == COMMIT_TS_TRUNCATE {
        let trunc = XLogRecGetData(record) as *mut xl_commit_ts_truncate;

        AdvanceOldestCommitTsXid((*trunc).oldestXid);

        /*
         * During XLOG replay, latest_page_number isn't set up yet; insert a
         * suitable value to bypass the sanity test in SimpleLruTruncate.
         */
        pg_atomic_write_u64(
            &raw mut (*(*CommitTsCtl()).shared).latest_page_number as *mut _,
            (*trunc).pageno as u64,
        );

        SimpleLruTruncate(CommitTsCtl(), (*trunc).pageno);
    } else {
        elog!(PANIC, "commit_ts_redo: unknown op code {}", info);
    }
}

/*
 * Entrypoint for sync.c to sync commit_ts files.
 */
pub unsafe fn committssyncfiletag(ftag: *const FileTag, path: *mut c_char) -> c_int {
    SlruSyncFileTag(CommitTsCtl(), ftag, path)
}

// ----------------------------------------------------------------------------
// Local stubs for unported dependencies.
// ----------------------------------------------------------------------------

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// Types
pub type RepOriginId = u16; // replication/origin.h
pub const InvalidRepOriginId: RepOriginId = 0;

pub type GucSource = c_int; // utils/guc.h
pub const PGC_S_DYNAMIC_DEFAULT: GucSource = 1; // utils/guc.h: GucSource::PGC_S_DYNAMIC_DEFAULT
pub const PGC_S_OVERRIDE: GucSource = 10; // utils/guc.h: GucSource::PGC_S_OVERRIDE
pub const PGC_POSTMASTER: c_int = 1; // utils/guc.h: GucContext::PGC_POSTMASTER

#[repr(C)]
pub struct SlruSharedData {
    pub page_buffer: *mut *mut c_char,
    pub page_dirty: *mut bool,
    pub latest_page_number: pg_atomic_uint64,
    // ... TODO: access/slru.h
}
pub type SlruShared = *mut SlruSharedData;

pub type SlruPagePrecedesFunction = unsafe extern "C" fn(int64, int64) -> bool;

pub use crate::access::transam::slru::{SlruCtlData, SlruCtl};

#[repr(C)]
pub struct pg_atomic_uint64 {
    pub value: u64,
}

#[repr(C)]
pub struct LWLock {
    _private: [u8; 0],
}

pub const LW_EXCLUSIVE: c_int = 0; // TODO: storage/lwlock.h
pub const LW_SHARED: c_int = 1; // TODO: storage/lwlock.h

pub const LWTRANCHE_COMMITTS_BUFFER: c_int = 0; // TODO: storage/lwlock.h
pub const LWTRANCHE_COMMITTS_SLRU: c_int = 0; // TODO: storage/lwlock.h

pub const SYNC_HANDLER_COMMIT_TS: c_int = 0; // TODO: storage/sync.h

#[repr(C)]
pub struct FileTag {
    _private: [u8; 0],
}

#[repr(C)]
pub struct XLogReaderState {
    _private: [u8; 0],
}

pub const RM_COMMIT_TS_ID: u8 = 0; // TODO: access/rmgrlist.h
pub const XLR_INFO_MASK: uint8 = 0x0F; // TODO: access/xlogrecord.h

pub const SLRU_MAX_ALLOWED_BUFFERS: c_int = 0; // TODO: access/slru.h

// GUC
pub static mut commit_timestamp_buffers: c_int = 0; // TODO: access/slru.c

// Globals from other modules
pub static mut IsUnderPostmaster: bool = false; // TODO: miscadmin.h
pub static mut InRecovery: bool = false; // TODO: access/xlogutils.h

#[inline]
unsafe fn CommitTsLock() -> *mut LWLock {
    crate::backend_link_shims::CommitTsLock as *mut LWLock
}

unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    let mode = if _mode == LW_EXCLUSIVE {
        crate::storage::lmgr::lwlock::LWLockMode::LW_EXCLUSIVE
    } else {
        crate::storage::lmgr::lwlock::LWLockMode::LW_SHARED
    };
    crate::storage::lmgr::lwlock::LWLockAcquire(_lock as *mut crate::storage::lmgr::lwlock::LWLock, mode)
}
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    crate::storage::lmgr::lwlock::LWLockRelease(_lock as *mut crate::storage::lmgr::lwlock::LWLock)
}

unsafe fn SimpleLruGetBankLock(_ctl: SlruCtl, _pageno: int64) -> *mut LWLock {
    crate::access::transam::slru::SimpleLruGetBankLock(_ctl as crate::access::transam::slru::SlruCtl, _pageno) as *mut LWLock
}
unsafe fn SimpleLruReadPage(_ctl: SlruCtl, _pageno: int64, _write_ok: bool, _xid: TransactionId) -> c_int {
    crate::access::transam::slru::SimpleLruReadPage(_ctl as crate::access::transam::slru::SlruCtl, _pageno, _write_ok, _xid)
}
unsafe fn SimpleLruReadPage_ReadOnly(_ctl: SlruCtl, _pageno: int64, _xid: TransactionId) -> c_int {
    crate::access::transam::slru::SimpleLruReadPage_ReadOnly(_ctl as crate::access::transam::slru::SlruCtl, _pageno, _xid)
}
unsafe fn SimpleLruZeroPage(_ctl: SlruCtl, _pageno: int64) -> c_int {
    crate::access::transam::slru::SimpleLruZeroPage(_ctl as crate::access::transam::slru::SlruCtl, _pageno)
}
unsafe fn SimpleLruWritePage(_ctl: SlruCtl, _slotno: c_int) {
    crate::access::transam::slru::SimpleLruWritePage(_ctl as crate::access::transam::slru::SlruCtl, _slotno)
}
unsafe fn SimpleLruWriteAll(_ctl: SlruCtl, _allow_redirtied: bool) {
    crate::access::transam::slru::SimpleLruWriteAll(_ctl as crate::access::transam::slru::SlruCtl, _allow_redirtied)
}
unsafe fn SimpleLruTruncate(_ctl: SlruCtl, _cutoffPage: int64) {
    crate::access::transam::slru::SimpleLruTruncate(_ctl as crate::access::transam::slru::SlruCtl, _cutoffPage)
}
unsafe fn SimpleLruDoesPhysicalPageExist(_ctl: SlruCtl, _pageno: int64) -> bool {
    crate::access::transam::slru::SimpleLruDoesPhysicalPageExist(_ctl as crate::access::transam::slru::SlruCtl, _pageno)
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
    crate::access::transam::slru::SimpleLruInit(
        _ctl as crate::access::transam::slru::SlruCtl,
        _name,
        _nslots,
        _nlsns,
        _subdir,
        _buffer_tranche_id,
        _bank_tranche_id,
        _sync_handler,
        _long_segment_names,
    )
}
unsafe fn SimpleLruShmemSize(_nslots: c_int, _nlsns: c_int) -> Size {
    crate::access::transam::slru::SimpleLruShmemSize(_nslots, _nlsns)
}
unsafe fn SimpleLruAutotuneBuffers(_divisor: c_int, _max: c_int) -> c_int {
    crate::access::transam::slru::SimpleLruAutotuneBuffers(_divisor, _max)
}
unsafe fn SlruPagePrecedesUnitTests(_ctl: SlruCtl, _per_page: c_int) {
    #[cfg(debug_assertions)]
    #[cfg(debug_assertions)] crate::access::transam::slru::SlruPagePrecedesUnitTests(_ctl as crate::access::transam::slru::SlruCtl, _per_page);
}
unsafe fn SlruScanDirectory(
    _ctl: SlruCtl,
    _callback: SlruScanCallback,
    _data: *mut c_void,
) -> bool {
    crate::access::transam::slru::SlruScanDirectory(
        _ctl as crate::access::transam::slru::SlruCtl,
        core::mem::transmute::<SlruScanCallback, crate::access::transam::slru::SlruScanCallback>(_callback),
        _data,
    )
}
unsafe fn SlruSyncFileTag(_ctl: SlruCtl, _ftag: *const FileTag, _path: *mut c_char) -> c_int {
    crate::access::transam::slru::SlruSyncFileTag(
        _ctl as crate::access::transam::slru::SlruCtl,
        _ftag as *const crate::access::transam::slru::FileTag,
        _path,
    )
}

pub type SlruScanCallback =
    unsafe extern "C" fn(SlruCtl, *mut c_char, int64, *mut c_void) -> bool;

unsafe extern "C" fn SlruScanDirCbDeleteAll(
    _ctl: SlruCtl,
    _filename: *mut c_char,
    _segpage: int64,
    _data: *mut c_void,
) -> bool {
    crate::access::transam::slru::SlruScanDirCbDeleteAll(
        _ctl as crate::access::transam::slru::SlruCtl,
        _filename,
        _segpage,
        _data,
    )
}
unsafe extern "C" fn SlruScanDirCbReportPresence(
    _ctl: SlruCtl,
    _filename: *mut c_char,
    _segpage: int64,
    _data: *mut c_void,
) -> bool {
    crate::access::transam::slru::SlruScanDirCbReportPresence(
        _ctl as crate::access::transam::slru::SlruCtl,
        _filename,
        _segpage,
        _data,
    )
}

unsafe fn check_slru_buffers(_name: *const c_char, _newval: *mut c_int) -> bool {
    crate::access::transam::slru::check_slru_buffers(_name, _newval)
}

unsafe fn SetConfigOption(
    _name: *const c_char,
    _value: *const c_char,
    _context: c_int,
    _source: GucSource,
) {
    crate::utils::misc::guc::SetConfigOption(
        _name,
        _value,
        core::mem::transmute::<c_int, crate::utils::misc::guc::GucContext>(_context),
        core::mem::transmute::<c_int, crate::utils::misc::guc::GucSource>(_source),
    )
}

unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _found: *mut bool) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(_name, _size, _found)
}

unsafe fn pg_atomic_write_u64(_ptr: *mut pg_atomic_uint64, _val: u64) {
    crate::port::atomics::generic::pg_atomic_write_u64_impl(
        &*(_ptr as *const crate::port::atomics::pg_atomic_uint64),
        _val,
    )
}

unsafe fn XLogBeginInsert() {
    crate::access::transam::xloginsert::XLogBeginInsert()
}
unsafe fn XLogRegisterData(_data: *mut c_char, _len: usize) {
    crate::access::transam::xloginsert::XLogRegisterData(_data as *const c_void, _len as u32)
}
unsafe fn XLogInsert(_rmid: u8, _info: uint8) -> XLogRecPtr {
    crate::access::transam::xloginsert::XLogInsert(_rmid, _info)
}
unsafe fn XLogRecGetInfo(_record: *mut XLogReaderState) -> uint8 {
    crate::access::transam::xlogreader::XLogRecGetInfo(_record as *mut crate::access::transam::xlogreader::XLogReaderState)
}
unsafe fn XLogRecGetData(_record: *mut XLogReaderState) -> *mut c_char {
    crate::access::transam::xlogreader::XLogRecGetData(_record as *mut crate::access::transam::xlogreader::XLogReaderState)
}
unsafe fn XLogRecHasAnyBlockRefs(_record: *mut XLogReaderState) -> bool {
    crate::access::transam::xlogreader::XLogRecHasAnyBlockRefs(_record as *mut crate::access::transam::xlogreader::XLogReaderState)
}

unsafe fn IsBootstrapProcessingMode() -> bool {
    crate::miscadmin::IsBootstrapProcessingMode()
}
unsafe fn RecoveryInProgress() -> bool {
    crate::access::transam::xlog::RecoveryInProgress()
}

unsafe fn XidFromFullTransactionId(_fxid: crate::access::transam::FullTransactionId) -> TransactionId {
    crate::access::transam::XidFromFullTransactionId(_fxid)
}
unsafe fn ReadNextTransactionId() -> TransactionId {
    unimplemented!() // TODO: access/transam.h
}
unsafe fn TransactionIdPrecedes(_id1: TransactionId, _id2: TransactionId) -> bool {
    crate::access::transam::transam::TransactionIdPrecedes(_id1, _id2)
}
unsafe fn TransactionIdIsValid(_xid: TransactionId) -> bool {
    crate::access::transam::TransactionIdIsValid(_xid)
}
unsafe fn TransactionIdIsNormal(_xid: TransactionId) -> bool {
    crate::access::transam::TransactionIdIsNormal(_xid)
}
unsafe fn TransactionIdEquals(_id1: TransactionId, _id2: TransactionId) -> bool {
    crate::access::transam::TransactionIdEquals(_id1, _id2)
}

pub const InvalidTransactionId: TransactionId = 0; // TODO: access/transam.h
pub const FirstNormalTransactionId: TransactionId = 3; // TODO: access/transam.h

pub use crate::access::transam::varsup::TransamVariablesData;
pub use crate::access::transam::varsup::TransamVariables;

unsafe fn TIMESTAMP_NOBEGIN(_t: *mut TimestampTz) {
    *_t = TimestampTz::MIN; // DT_NOBEGIN
}

// fmgr / type helpers
unsafe fn PG_GETARG_TRANSACTIONID(_fcinfo: FunctionCallInfo, _n: c_int) -> TransactionId {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_RETURN_NULL(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_RETURN_TIMESTAMPTZ(_x: TimestampTz) -> Datum {
    unimplemented!() // TODO: utils/timestamp.h
}
unsafe fn PG_RETURN_DATUM(_x: Datum) -> Datum {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn TransactionIdGetDatum(_xid: TransactionId) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn TimestampTzGetDatum(_ts: TimestampTz) -> Datum {
    unimplemented!() // TODO: utils/timestamp.h
}
unsafe fn ObjectIdGetDatum(_oid: Oid) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn HeapTupleGetDatum(_htup: HeapTuple) -> Datum {
    unimplemented!() // TODO: funcapi.h
}
unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> TypeFuncClass {
    unimplemented!() // TODO: utils/funcapi.c
}
unsafe fn heap_form_tuple(
    _tupleDescriptor: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple.c
}

pub type TypeFuncClass = c_int; // utils/funcapi.h
pub const TYPEFUNC_COMPOSITE: TypeFuncClass = 1; // TODO: utils/funcapi.h

pub type FunctionCallInfo = *mut c_void; // fmgr.h
pub type HeapTuple = *mut c_void; // access/htup.h

#[allow(non_snake_case)]
unsafe fn Min(a: c_int, b: c_int) -> c_int {
    if a < b { a } else { b }
}
#[allow(non_snake_case)]
unsafe fn Max(a: c_int, b: c_int) -> c_int {
    if a > b { a } else { b }
}
#[allow(non_snake_case)]
unsafe fn Assert(_cond: bool) {}
