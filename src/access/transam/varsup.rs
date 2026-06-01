//! varsup.c
//!   postgres OID & XID variables support routines
//!
//! Copyright (c) 2000-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/access/transam/varsup.c

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::c::uint32;
use crate::miscadmin::{IsPostmasterEnvironment, IsUnderPostmaster};

// ---------------------------------------------------------------------------
// Local type aliases / stubs for unported dependencies
// ---------------------------------------------------------------------------

type TransactionId = crate::c::TransactionId;
type FullTransactionId = crate::access::transam::FullTransactionId;

// Stub for the shared-memory TransamVariables struct.  See access/transam.h.
#[repr(C)]
pub struct TransamVariablesData {
    pub nextOid: Oid,
    pub oidCount: uint32,
    pub nextXid: FullTransactionId,
    pub oldestXid: TransactionId,
    pub xidVacLimit: TransactionId,
    pub xidWarnLimit: TransactionId,
    pub xidStopLimit: TransactionId,
    pub xidWrapLimit: TransactionId,
    pub oldestXidDB: Oid,
    pub oldestClogXid: TransactionId,
    pub latestCompletedXid: FullTransactionId,
}

/* Number of OIDs to prefetch (preallocate) per XLOG write */
const VAR_OID_PREFETCH: c_int = 8192;

/* pointer to variables struct in shared memory */
#[no_mangle]
pub static mut TransamVariables: *mut TransamVariablesData = std::ptr::null_mut();

/*
 * Initialization of shared memory for TransamVariables.
 */
#[no_mangle]
pub unsafe extern "C" fn VarsupShmemSize() -> Size {
    std::mem::size_of::<TransamVariablesData>()
}

#[no_mangle]
pub unsafe extern "C" fn VarsupShmemInit() {
    let mut found: bool = false;

    /* Initialize our shared state struct */
    TransamVariables = ShmemInitStruct(
        c"TransamVariables".as_ptr(),
        std::mem::size_of::<TransamVariablesData>(),
        &mut found,
    ) as *mut TransamVariablesData;
    if !IsUnderPostmaster {
        Assert!(!found);
        std::ptr::write_bytes(TransamVariables as *mut u8, 0, std::mem::size_of::<TransamVariablesData>());
    } else {
        Assert!(found);
    }
}

/*
 * Allocate the next FullTransactionId for a new transaction or
 * subtransaction.
 *
 * The new XID is also stored into MyProc->xid/ProcGlobal->xids[] before
 * returning.
 *
 * Note: when this is called, we are actually already inside a valid
 * transaction, since XIDs are now not allocated until the transaction
 * does something.  So it is safe to do a database lookup if we want to
 * issue a warning about XID wrap.
 */
#[no_mangle]
pub unsafe extern "C" fn GetNewTransactionId(isSubXact: bool) -> FullTransactionId {
    let mut full_xid: FullTransactionId;
    let mut xid: TransactionId;

    /*
     * Workers synchronize transaction state at the beginning of each parallel
     * operation, so we can't account for new XIDs after that point.
     */
    if IsInParallelMode() {
        elog!(ERROR, "cannot assign TransactionIds during a parallel operation");
    }

    /*
     * During bootstrap initialization, we return the special bootstrap
     * transaction id.
     */
    if IsBootstrapProcessingMode() {
        Assert!(!isSubXact);
        (*MyProc).xid = BootstrapTransactionId;
        (*ProcGlobal).xids.add((*MyProc).pgxactoff as usize).write(BootstrapTransactionId);
        return FullTransactionIdFromEpochAndXid(0, BootstrapTransactionId);
    }

    /* safety check, we should never get this far in a HS standby */
    if RecoveryInProgress() {
        elog!(ERROR, "cannot assign TransactionIds during recovery");
    }

    LWLockAcquire(XidGenLock, LW_EXCLUSIVE);

    full_xid = (*TransamVariables).nextXid;
    xid = XidFromFullTransactionId(full_xid);

    /*----------
     * Check to see if it's safe to assign another XID.  This protects against
     * catastrophic data loss due to XID wraparound.  The basic rules are:
     *
     * If we're past xidVacLimit, start trying to force autovacuum cycles.
     * If we're past xidWarnLimit, start issuing warnings.
     * If we're past xidStopLimit, refuse to execute transactions, unless
     * we are running in single-user mode (which gives an escape hatch
     * to the DBA who somehow got past the earlier defenses).
     *
     * Note that this coding also appears in GetNewMultiXactId.
     *----------
     */
    if TransactionIdFollowsOrEquals(xid, (*TransamVariables).xidVacLimit) {
        /*
         * For safety's sake, we release XidGenLock while sending signals,
         * warnings, etc.  This is not so much because we care about
         * preserving concurrency in this situation, as to avoid any
         * possibility of deadlock while doing get_database_name(). First,
         * copy all the shared values we'll need in this path.
         */
        let xidWarnLimit: TransactionId = (*TransamVariables).xidWarnLimit;
        let xidStopLimit: TransactionId = (*TransamVariables).xidStopLimit;
        let xidWrapLimit: TransactionId = (*TransamVariables).xidWrapLimit;
        let oldest_datoid: Oid = (*TransamVariables).oldestXidDB;

        LWLockRelease(XidGenLock);

        /*
         * To avoid swamping the postmaster with signals, we issue the autovac
         * request only once per 64K transaction starts.  This still gives
         * plenty of chances before we get into real trouble.
         */
        if IsUnderPostmaster && (xid % 65536) == 0 {
            SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);
        }

        if IsUnderPostmaster && TransactionIdFollowsOrEquals(xid, xidStopLimit) {
            let oldest_datname: *mut c_char = get_database_name(oldest_datoid);

            /* complain even if that DB has disappeared */
            if !oldest_datname.is_null() {
                elog!(ERROR,
                    "database is not accepting commands that assign new transaction IDs to avoid wraparound data loss in database \"{}\"",
                    CStr_to_str(oldest_datname));
            } else {
                elog!(ERROR,
                    "database is not accepting commands that assign new transaction IDs to avoid wraparound data loss in database with OID {}",
                    oldest_datoid);
            }
        } else if TransactionIdFollowsOrEquals(xid, xidWarnLimit) {
            let oldest_datname: *mut c_char = get_database_name(oldest_datoid);

            /* complain even if that DB has disappeared */
            if !oldest_datname.is_null() {
                elog!(WARNING,
                    "database \"{}\" must be vacuumed within {} transactions",
                    CStr_to_str(oldest_datname),
                    xidWrapLimit.wrapping_sub(xid));
            } else {
                elog!(WARNING,
                    "database with OID {} must be vacuumed within {} transactions",
                    oldest_datoid,
                    xidWrapLimit.wrapping_sub(xid));
            }
        }

        /* Re-acquire lock and start over */
        LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
        full_xid = (*TransamVariables).nextXid;
        xid = XidFromFullTransactionId(full_xid);
    }

    /*
     * If we are allocating the first XID of a new page of the commit log,
     * zero out that commit-log page before returning. We must do this while
     * holding XidGenLock, else another xact could acquire and commit a later
     * XID before we zero the page.  Fortunately, a page of the commit log
     * holds 32K or more transactions, so we don't have to do this very often.
     *
     * Extend pg_subtrans and pg_commit_ts too.
     */
    ExtendCLOG(xid);
    ExtendCommitTs(xid);
    ExtendSUBTRANS(xid);

    /*
     * Now advance the nextXid counter.  This must not happen until after we
     * have successfully completed ExtendCLOG() --- if that routine fails, we
     * want the next incoming transaction to try it again.  We cannot assign
     * more XIDs until there is CLOG space for them.
     */
    FullTransactionIdAdvance(&mut (*TransamVariables).nextXid);

    /*
     * We must store the new XID into the shared ProcArray before releasing
     * XidGenLock.  This ensures that every active XID older than
     * latestCompletedXid is present in the ProcArray, which is essential for
     * correct OldestXmin tracking; see src/backend/access/transam/README.
     *
     * Note that readers of ProcGlobal->xids/PGPROC->xid should be careful to
     * fetch the value for each proc only once, rather than assume they can
     * read a value multiple times and get the same answer each time.  Note we
     * are assuming that TransactionId and int fetch/store are atomic.
     *
     * The same comments apply to the subxact xid count and overflow fields.
     *
     * Use of a write barrier prevents dangerous code rearrangement in this
     * function; other backends could otherwise e.g. be examining my subxids
     * info concurrently, and we don't want them to see an invalid
     * intermediate state, such as an incremented nxids before the array entry
     * is filled.
     *
     * Other processes that read nxids should do so before reading xids
     * elements with a pg_read_barrier() in between, so that they can be sure
     * not to read an uninitialized array element; see
     * src/backend/storage/lmgr/README.barrier.
     *
     * If there's no room to fit a subtransaction XID into PGPROC, set the
     * cache-overflowed flag instead.  This forces readers to look in
     * pg_subtrans to map subtransaction XIDs up to top-level XIDs. There is a
     * race-condition window, in that the new XID will not appear as running
     * until its parent link has been placed into pg_subtrans. However, that
     * will happen before anyone could possibly have a reason to inquire about
     * the status of the XID, so it seems OK.  (Snapshots taken during this
     * window *will* include the parent XID, so they will deliver the correct
     * answer later on when someone does have a reason to inquire.)
     */
    if !isSubXact {
        Assert!((*((*ProcGlobal).subxidStates).add((*MyProc).pgxactoff as usize)).count == 0);
        Assert!(!(*((*ProcGlobal).subxidStates).add((*MyProc).pgxactoff as usize)).overflowed);
        Assert!((*MyProc).subxidStatus.count == 0);
        Assert!(!(*MyProc).subxidStatus.overflowed);

        /* LWLockRelease acts as barrier */
        (*MyProc).xid = xid;
        (*ProcGlobal).xids.add((*MyProc).pgxactoff as usize).write(xid);
    } else {
        let substat: *mut XidCacheStatus =
            ((*ProcGlobal).subxidStates).add((*MyProc).pgxactoff as usize);
        let nxids: c_int = (*MyProc).subxidStatus.count as c_int;

        Assert!((*substat).count == (*MyProc).subxidStatus.count);
        Assert!((*substat).overflowed == (*MyProc).subxidStatus.overflowed);

        if nxids < PGPROC_MAX_CACHED_SUBXIDS {
            (*MyProc).subxids.xids[nxids as usize] = xid;
            pg_write_barrier();
            (*MyProc).subxidStatus.count = (nxids + 1) as u8;
            (*substat).count = (nxids + 1) as u8;
        } else {
            (*MyProc).subxidStatus.overflowed = true;
            (*substat).overflowed = true;
        }
    }

    LWLockRelease(XidGenLock);

    full_xid
}

/*
 * Read nextXid but don't allocate it.
 */
#[no_mangle]
pub unsafe extern "C" fn ReadNextFullTransactionId() -> FullTransactionId {
    let fullXid: FullTransactionId;

    LWLockAcquire(XidGenLock, LW_SHARED);
    fullXid = (*TransamVariables).nextXid;
    LWLockRelease(XidGenLock);

    fullXid
}

/*
 * Advance nextXid to the value after a given xid.  The epoch is inferred.
 * This must only be called during recovery or from two-phase start-up code.
 */
#[no_mangle]
pub unsafe extern "C" fn AdvanceNextFullTransactionIdPastXid(xid: TransactionId) {
    let newNextFullXid: FullTransactionId;
    let next_xid: TransactionId;
    let mut epoch: uint32;
    let mut xid = xid;

    /*
     * It is safe to read nextXid without a lock, because this is only called
     * from the startup process or single-process mode, meaning that no other
     * process can modify it.
     */
    Assert!(AmStartupProcess() || !IsUnderPostmaster);

    /* Fast return if this isn't an xid high enough to move the needle. */
    next_xid = XidFromFullTransactionId((*TransamVariables).nextXid);
    if !TransactionIdFollowsOrEquals(xid, next_xid) {
        return;
    }

    /*
     * Compute the FullTransactionId that comes after the given xid.  To do
     * this, we preserve the existing epoch, but detect when we've wrapped
     * into a new epoch.  This is necessary because WAL records and 2PC state
     * currently contain 32 bit xids.  The wrap logic is safe in those cases
     * because the span of active xids cannot exceed one epoch at any given
     * point in the WAL stream.
     */
    TransactionIdAdvance(&mut xid);
    epoch = EpochFromFullTransactionId((*TransamVariables).nextXid);
    if unlikely(xid < next_xid) {
        epoch += 1;
    }
    newNextFullXid = FullTransactionIdFromEpochAndXid(epoch, xid);

    /*
     * We still need to take a lock to modify the value when there are
     * concurrent readers.
     */
    LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
    (*TransamVariables).nextXid = newNextFullXid;
    LWLockRelease(XidGenLock);
}

/*
 * Advance the cluster-wide value for the oldest valid clog entry.
 *
 * We must acquire XactTruncationLock to advance the oldestClogXid. It's not
 * necessary to hold the lock during the actual clog truncation, only when we
 * advance the limit, as code looking up arbitrary xids is required to hold
 * XactTruncationLock from when it tests oldestClogXid through to when it
 * completes the clog lookup.
 */
#[no_mangle]
pub unsafe extern "C" fn AdvanceOldestClogXid(oldest_datfrozenxid: TransactionId) {
    LWLockAcquire(XactTruncationLock, LW_EXCLUSIVE);
    if TransactionIdPrecedes((*TransamVariables).oldestClogXid, oldest_datfrozenxid) {
        (*TransamVariables).oldestClogXid = oldest_datfrozenxid;
    }
    LWLockRelease(XactTruncationLock);
}

/*
 * Determine the last safe XID to allocate using the currently oldest
 * datfrozenxid (ie, the oldest XID that might exist in any database
 * of our cluster), and the OID of the (or a) database with that value.
 */
#[no_mangle]
pub unsafe extern "C" fn SetTransactionIdLimit(
    oldest_datfrozenxid: TransactionId,
    oldest_datoid: Oid,
) {
    let xidVacLimit: TransactionId;
    let xidWarnLimit: TransactionId;
    let xidStopLimit: TransactionId;
    let mut xidWrapLimit: TransactionId;
    let curXid: TransactionId;

    Assert!(TransactionIdIsNormal(oldest_datfrozenxid));

    /*
     * The place where we actually get into deep trouble is halfway around
     * from the oldest potentially-existing XID.  (This calculation is
     * probably off by one or two counts, because the special XIDs reduce the
     * size of the loop a little bit.  But we throw in plenty of slop below,
     * so it doesn't matter.)
     */
    xidWrapLimit = oldest_datfrozenxid.wrapping_add(MaxTransactionId >> 1);
    if xidWrapLimit < FirstNormalTransactionId {
        xidWrapLimit = xidWrapLimit.wrapping_add(FirstNormalTransactionId);
    }

    /*
     * We'll refuse to continue assigning XIDs in interactive mode once we get
     * within 3M transactions of data loss.  This leaves lots of room for the
     * DBA to fool around fixing things in a standalone backend, while not
     * being significant compared to total XID space. (VACUUM requires an XID
     * if it truncates at wal_level!=minimal.  "VACUUM (ANALYZE)", which a DBA
     * might do by reflex, assigns an XID.  Hence, we had better be sure
     * there's lots of XIDs left...)  Also, at default BLCKSZ, this leaves two
     * completely-idle segments.  In the event of edge-case bugs involving
     * page or segment arithmetic, idle segments render the bugs unreachable
     * outside of single-user mode.
     */
    let mut xidStopLimit_v = xidWrapLimit.wrapping_sub(3000000);
    if xidStopLimit_v < FirstNormalTransactionId {
        xidStopLimit_v = xidStopLimit_v.wrapping_sub(FirstNormalTransactionId);
    }
    xidStopLimit = xidStopLimit_v;

    /*
     * We'll start complaining loudly when we get within 40M transactions of
     * data loss.  This is kind of arbitrary, but if you let your gas gauge
     * get down to 2% of full, would you be looking for the next gas station?
     * We need to be fairly liberal about this number because there are lots
     * of scenarios where most transactions are done by automatic clients that
     * won't pay attention to warnings.  (No, we're not gonna make this
     * configurable.  If you know enough to configure it, you know enough to
     * not get in this kind of trouble in the first place.)
     */
    let mut xidWarnLimit_v = xidWrapLimit.wrapping_sub(40000000);
    if xidWarnLimit_v < FirstNormalTransactionId {
        xidWarnLimit_v = xidWarnLimit_v.wrapping_sub(FirstNormalTransactionId);
    }
    xidWarnLimit = xidWarnLimit_v;

    /*
     * We'll start trying to force autovacuums when oldest_datfrozenxid gets
     * to be more than autovacuum_freeze_max_age transactions old.
     *
     * Note: guc.c ensures that autovacuum_freeze_max_age is in a sane range,
     * so that xidVacLimit will be well before xidWarnLimit.
     *
     * Note: autovacuum_freeze_max_age is a PGC_POSTMASTER parameter so that
     * we don't have to worry about dealing with on-the-fly changes in its
     * value.  It doesn't look practical to update shared state from a GUC
     * assign hook (too many processes would try to execute the hook,
     * resulting in race conditions as well as crashes of those not connected
     * to shared memory).  Perhaps this can be improved someday.  See also
     * SetMultiXactIdLimit.
     */
    let mut xidVacLimit_v = oldest_datfrozenxid.wrapping_add(autovacuum_freeze_max_age as TransactionId);
    if xidVacLimit_v < FirstNormalTransactionId {
        xidVacLimit_v = xidVacLimit_v.wrapping_add(FirstNormalTransactionId);
    }
    xidVacLimit = xidVacLimit_v;

    /* Grab lock for just long enough to set the new limit values */
    LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
    (*TransamVariables).oldestXid = oldest_datfrozenxid;
    (*TransamVariables).xidVacLimit = xidVacLimit;
    (*TransamVariables).xidWarnLimit = xidWarnLimit;
    (*TransamVariables).xidStopLimit = xidStopLimit;
    (*TransamVariables).xidWrapLimit = xidWrapLimit;
    (*TransamVariables).oldestXidDB = oldest_datoid;
    curXid = XidFromFullTransactionId((*TransamVariables).nextXid);
    LWLockRelease(XidGenLock);

    /* Log the info */
    elog!(DEBUG1,
        "transaction ID wrap limit is {}, limited by database with OID {}",
        xidWrapLimit, oldest_datoid);

    /*
     * If past the autovacuum force point, immediately signal an autovac
     * request.  The reason for this is that autovac only processes one
     * database per invocation.  Once it's finished cleaning up the oldest
     * database, it'll call here, and we'll signal the postmaster to start
     * another iteration immediately if there are still any old databases.
     */
    if TransactionIdFollowsOrEquals(curXid, xidVacLimit) && IsUnderPostmaster && !InRecovery {
        SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);
    }

    /* Give an immediate warning if past the wrap warn point */
    if TransactionIdFollowsOrEquals(curXid, xidWarnLimit) && !InRecovery {
        let oldest_datname: *mut c_char;

        /*
         * We can be called when not inside a transaction, for example during
         * StartupXLOG().  In such a case we cannot do database access, so we
         * must just report the oldest DB's OID.
         *
         * Note: it's also possible that get_database_name fails and returns
         * NULL, for example because the database just got dropped.  We'll
         * still warn, even though the warning might now be unnecessary.
         */
        if IsTransactionState() {
            oldest_datname = get_database_name(oldest_datoid);
        } else {
            oldest_datname = std::ptr::null_mut();
        }

        if !oldest_datname.is_null() {
            elog!(WARNING,
                "database \"{}\" must be vacuumed within {} transactions",
                CStr_to_str(oldest_datname),
                xidWrapLimit.wrapping_sub(curXid));
        } else {
            elog!(WARNING,
                "database with OID {} must be vacuumed within {} transactions",
                oldest_datoid,
                xidWrapLimit.wrapping_sub(curXid));
        }
    }
}

/*
 * ForceTransactionIdLimitUpdate -- does the XID wrap-limit data need updating?
 *
 * We primarily check whether oldestXidDB is valid.  The cases we have in
 * mind are that that database was dropped, or the field was reset to zero
 * by pg_resetwal.  In either case we should force recalculation of the
 * wrap limit.  Also do it if oldestXid is old enough to be forcing
 * autovacuums or other actions; this ensures we update our state as soon
 * as possible once extra overhead is being incurred.
 */
#[no_mangle]
pub unsafe extern "C" fn ForceTransactionIdLimitUpdate() -> bool {
    let nextXid: TransactionId;
    let xidVacLimit: TransactionId;
    let oldestXid: TransactionId;
    let oldestXidDB: Oid;

    /* Locking is probably not really necessary, but let's be careful */
    LWLockAcquire(XidGenLock, LW_SHARED);
    nextXid = XidFromFullTransactionId((*TransamVariables).nextXid);
    xidVacLimit = (*TransamVariables).xidVacLimit;
    oldestXid = (*TransamVariables).oldestXid;
    oldestXidDB = (*TransamVariables).oldestXidDB;
    LWLockRelease(XidGenLock);

    if !TransactionIdIsNormal(oldestXid) {
        return true; /* shouldn't happen, but just in case */
    }
    if !TransactionIdIsValid(xidVacLimit) {
        return true; /* this shouldn't happen anymore either */
    }
    if TransactionIdFollowsOrEquals(nextXid, xidVacLimit) {
        return true; /* past xidVacLimit, don't delay updating */
    }
    if !SearchSysCacheExists1(DATABASEOID, ObjectIdGetDatum(oldestXidDB)) {
        return true; /* could happen, per comments above */
    }
    false
}

/*
 * GetNewObjectId -- allocate a new OID
 *
 * OIDs are generated by a cluster-wide counter.  Since they are only 32 bits
 * wide, counter wraparound will occur eventually, and therefore it is unwise
 * to assume they are unique unless precautions are taken to make them so.
 * Hence, this routine should generally not be used directly.  The only direct
 * callers should be GetNewOidWithIndex() and GetNewRelFileNumber() in
 * catalog/catalog.c.
 */
#[no_mangle]
pub unsafe extern "C" fn GetNewObjectId() -> Oid {
    let result: Oid;

    /* safety check, we should never get this far in a HS standby */
    if RecoveryInProgress() {
        elog!(ERROR, "cannot assign OIDs during recovery");
    }

    LWLockAcquire(OidGenLock, LW_EXCLUSIVE);

    /*
     * Check for wraparound of the OID counter.  We *must* not return 0
     * (InvalidOid), and in normal operation we mustn't return anything below
     * FirstNormalObjectId since that range is reserved for initdb (see
     * IsCatalogRelationOid()).  Note we are relying on unsigned comparison.
     *
     * During initdb, we start the OID generator at FirstGenbkiObjectId, so we
     * only wrap if before that point when in bootstrap or standalone mode.
     * The first time through this routine after normal postmaster start, the
     * counter will be forced up to FirstNormalObjectId.  This mechanism
     * leaves the OIDs between FirstGenbkiObjectId and FirstNormalObjectId
     * available for automatic assignment during initdb, while ensuring they
     * will never conflict with user-assigned OIDs.
     */
    if (*TransamVariables).nextOid < (FirstNormalObjectId as Oid) {
        if IsPostmasterEnvironment {
            /* wraparound, or first post-initdb assignment, in normal mode */
            (*TransamVariables).nextOid = FirstNormalObjectId as Oid;
            (*TransamVariables).oidCount = 0;
        } else {
            /* we may be bootstrapping, so don't enforce the full range */
            if (*TransamVariables).nextOid < (FirstGenbkiObjectId as Oid) {
                /* wraparound in standalone mode (unlikely but possible) */
                (*TransamVariables).nextOid = FirstNormalObjectId as Oid;
                (*TransamVariables).oidCount = 0;
            }
        }
    }

    /* If we run out of logged for use oids then we must log more */
    if (*TransamVariables).oidCount == 0 {
        XLogPutNextOid((*TransamVariables).nextOid + VAR_OID_PREFETCH as Oid);
        (*TransamVariables).oidCount = VAR_OID_PREFETCH as uint32;
    }

    result = (*TransamVariables).nextOid;

    (*TransamVariables).nextOid += 1;
    (*TransamVariables).oidCount -= 1;

    LWLockRelease(OidGenLock);

    result
}

/*
 * SetNextObjectId
 *
 * This may only be called during initdb; it advances the OID counter
 * to the specified value.
 */
unsafe fn SetNextObjectId(nextOid: Oid) {
    /* Safety check, this is only allowable during initdb */
    if IsPostmasterEnvironment {
        elog!(ERROR, "cannot advance OID counter anymore");
    }

    /* Taking the lock is, therefore, just pro forma; but do it anyway */
    LWLockAcquire(OidGenLock, LW_EXCLUSIVE);

    if (*TransamVariables).nextOid > nextOid {
        elog!(ERROR, "too late to advance OID counter to {}, it is now {}",
            nextOid, (*TransamVariables).nextOid);
    }

    (*TransamVariables).nextOid = nextOid;
    (*TransamVariables).oidCount = 0;

    LWLockRelease(OidGenLock);
}

/*
 * StopGeneratingPinnedObjectIds
 *
 * This is called once during initdb to force the OID counter up to
 * FirstUnpinnedObjectId.  This supports letting initdb's post-bootstrap
 * processing create some pinned objects early on.  Once it's done doing
 * so, it calls this (via pg_stop_making_pinned_objects()) so that the
 * remaining objects it makes will be considered un-pinned.
 */
#[no_mangle]
pub unsafe extern "C" fn StopGeneratingPinnedObjectIds() {
    SetNextObjectId(FirstUnpinnedObjectId as Oid);
}

/*
 * Assert that xid is between [oldestXid, nextXid], which is the range we
 * expect XIDs coming from tables etc to be in.
 *
 * As TransamVariables->oldestXid could change just after this call without
 * further precautions, and as a wrapped-around xid could again fall within
 * the valid range, this assertion can only detect if something is definitely
 * wrong, but not establish correctness.
 *
 * This intentionally does not expose a return value, to avoid code being
 * introduced that depends on the return value.
 */
#[cfg(debug_assertions)]
#[no_mangle]
pub unsafe extern "C" fn AssertTransactionIdInAllowableRange(xid: TransactionId) {
    let oldest_xid: TransactionId;
    let next_xid: TransactionId;

    Assert!(TransactionIdIsValid(xid));

    /* we may see bootstrap / frozen */
    if !TransactionIdIsNormal(xid) {
        return;
    }

    /*
     * We can't acquire XidGenLock, as this may be called with XidGenLock
     * already held (or with other locks that don't allow XidGenLock to be
     * nested). That's ok for our purposes though, since we already rely on
     * 32bit reads to be atomic. While nextXid is 64 bit, we only look at the
     * lower 32bit, so a skewed read doesn't hurt.
     *
     * There's no increased danger of falling outside [oldest, next] by
     * accessing them without a lock. xid needs to have been created with
     * GetNewTransactionId() in the originating session, and the locks there
     * pair with the memory barrier below.  We do however accept xid to be <=
     * to next_xid, instead of just <, as xid could be from the procarray,
     * before we see the updated nextXid value.
     */
    pg_memory_barrier();
    oldest_xid = (*TransamVariables).oldestXid;
    next_xid = XidFromFullTransactionId((*TransamVariables).nextXid);

    Assert!(TransactionIdFollowsOrEquals(xid, oldest_xid)
        || TransactionIdPrecedesOrEquals(xid, next_xid));
}

// ---------------------------------------------------------------------------
// Local stubs for unported helpers / externs
// ---------------------------------------------------------------------------

// Shared/global externs (stubs)
#[allow(non_upper_case_globals)]
static mut XidGenLock: *mut core::ffi::c_void = std::ptr::null_mut();
#[allow(non_upper_case_globals)]
static mut OidGenLock: *mut core::ffi::c_void = std::ptr::null_mut();
#[allow(non_upper_case_globals)]
static mut XactTruncationLock: *mut core::ffi::c_void = std::ptr::null_mut();

const LW_EXCLUSIVE: c_int = 0;
const LW_SHARED: c_int = 1;

const PMSIGNAL_START_AUTOVAC_LAUNCHER: c_int = 0;

const PGPROC_MAX_CACHED_SUBXIDS: c_int = 64;

const DATABASEOID: c_int = 0;

// Special transaction ids (access/transam.h)
const BootstrapTransactionId: TransactionId = 1;
const FirstNormalTransactionId: TransactionId = 3;
const MaxTransactionId: TransactionId = 0xFFFF_FFFF;

// OID ranges (access/transam.h)
const FirstGenbkiObjectId: c_int = 10000;
const FirstNormalObjectId: c_int = 16384;
const FirstUnpinnedObjectId: c_int = 12000;

// autovacuum GUC
#[allow(non_upper_case_globals)]
static mut autovacuum_freeze_max_age: c_int = 0;

// InRecovery / xlogutils flag
#[allow(non_upper_case_globals)]
static mut InRecovery: bool = false;

// Stub for PGPROC subxid status (storage/proc.h)
#[repr(C)]
pub struct XidCacheStatus {
    pub count: u8,
    pub overflowed: bool,
}

#[repr(C)]
pub struct SubXids {
    pub xids: [TransactionId; PGPROC_MAX_CACHED_SUBXIDS as usize],
}

#[repr(C)]
pub struct PGPROC {
    pub xid: TransactionId,
    pub pgxactoff: c_int,
    pub subxidStatus: XidCacheStatus,
    pub subxids: SubXids,
}

#[repr(C)]
pub struct PROC_HDR {
    pub xids: *mut TransactionId,
    pub subxidStates: *mut XidCacheStatus,
}

#[allow(non_upper_case_globals)]
static mut MyProc: *mut PGPROC = std::ptr::null_mut();
#[allow(non_upper_case_globals)]
static mut ProcGlobal: *mut PROC_HDR = std::ptr::null_mut();

#[inline]
unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _found: *mut bool) -> *mut core::ffi::c_void {
    unimplemented!() // TODO: storage/ipc/shmem.c
}

#[inline]
unsafe fn LWLockAcquire(_lock: *mut core::ffi::c_void, _mode: c_int) -> bool {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

#[inline]
unsafe fn LWLockRelease(_lock: *mut core::ffi::c_void) {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

#[inline]
unsafe fn IsInParallelMode() -> bool {
    unimplemented!() // TODO: access/transam/xact.c
}

#[inline]
unsafe fn IsBootstrapProcessingMode() -> bool {
    unimplemented!() // TODO: utils/init/miscinit.c
}

#[inline]
unsafe fn RecoveryInProgress() -> bool {
    unimplemented!() // TODO: access/transam/xlog.c
}

#[inline]
unsafe fn IsTransactionState() -> bool {
    unimplemented!() // TODO: access/transam/xact.c
}

#[inline]
unsafe fn AmStartupProcess() -> bool {
    unimplemented!() // TODO: storage/ipc/standby.c / miscadmin.h
}

#[inline]
unsafe fn SendPostmasterSignal(_reason: c_int) {
    unimplemented!() // TODO: storage/ipc/pmsignal.c
}

#[inline]
unsafe fn get_database_name(_dbid: Oid) -> *mut c_char {
    unimplemented!() // TODO: commands/dbcommands.c
}

#[inline]
unsafe fn ExtendCLOG(_newestXact: TransactionId) {
    unimplemented!() // TODO: access/transam/clog.c
}

#[inline]
unsafe fn ExtendCommitTs(_newestXact: TransactionId) {
    unimplemented!() // TODO: access/transam/commit_ts.c
}

#[inline]
unsafe fn ExtendSUBTRANS(_newestXact: TransactionId) {
    unimplemented!() // TODO: access/transam/subtrans.c
}

#[inline]
unsafe fn XLogPutNextOid(_nextOid: Oid) {
    unimplemented!() // TODO: access/transam/xlog.c
}

#[inline]
unsafe fn SearchSysCacheExists1(_cacheId: c_int, _key1: Datum) -> bool {
    unimplemented!() // TODO: utils/cache/syscache.c
}

#[inline]
unsafe fn pg_write_barrier() {
    std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
}

#[cfg(debug_assertions)]
#[inline]
unsafe fn pg_memory_barrier() {
    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
}

// FullTransactionId helpers (access/transam.h)
#[inline]
fn XidFromFullTransactionId(x: FullTransactionId) -> TransactionId {
    (x.value & 0xFFFF_FFFF) as TransactionId
}

#[inline]
fn EpochFromFullTransactionId(x: FullTransactionId) -> uint32 {
    (x.value >> 32) as uint32
}

#[inline]
fn FullTransactionIdFromEpochAndXid(epoch: uint32, xid: TransactionId) -> FullTransactionId {
    FullTransactionId {
        value: ((epoch as u64) << 32) | (xid as u64),
    }
}

#[inline]
unsafe fn FullTransactionIdAdvance(dest: *mut FullTransactionId) {
    (*dest).value += 1;
    /* Skip over the non-normal XIDs. */
    while XidFromFullTransactionId(*dest) < FirstNormalTransactionId {
        (*dest).value += 1;
    }
}

#[inline]
unsafe fn TransactionIdAdvance(dest: *mut TransactionId) {
    *dest = (*dest).wrapping_add(1);
    while *dest < FirstNormalTransactionId {
        *dest = (*dest).wrapping_add(1);
    }
}

#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != 0 /* InvalidTransactionId */
}

#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    /* If either ID is a permanent XID then we can just do unsigned comparison */
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    let diff = (id1.wrapping_sub(id2)) as i32;
    diff < 0
}

#[inline]
fn TransactionIdFollowsOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 >= id2;
    }
    let diff = (id1.wrapping_sub(id2)) as i32;
    diff >= 0
}

#[cfg(debug_assertions)]
#[inline]
fn TransactionIdPrecedesOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 <= id2;
    }
    let diff = (id1.wrapping_sub(id2)) as i32;
    diff <= 0
}

#[inline]
fn unlikely(b: bool) -> bool {
    b
}

#[inline]
unsafe fn CStr_to_str<'a>(p: *const c_char) -> &'a str {
    std::ffi::CStr::from_ptr(p).to_str().unwrap_or("")
}
