//! access/transam/transam.c - postgres transaction (commit) log interface routines.
//!
//! High level access-method interface to the transaction system.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::miscadmin::TimestampTz;

use crate::access::transam::{
    BootstrapTransactionId, FrozenTransactionId, InvalidTransactionId, TransactionIdEquals,
    TransactionIdIsNormal, TransactionIdIsValid,
};
use crate::access::transam::xlogdefs::{InvalidXLogRecPtr, XLogRecPtr};

/*
 * XidStatus and the commit-log status codes come from access/clog.h, which is
 * not ported yet.  Stub the type + constants locally so transam's logic can be
 * translated faithfully.
 */
// TODO: replace with crate::access::clog once ported.
pub type XidStatus = c_int;
pub const TRANSACTION_STATUS_IN_PROGRESS: XidStatus = 0x00;
pub const TRANSACTION_STATUS_COMMITTED: XidStatus = 0x01;
pub const TRANSACTION_STATUS_ABORTED: XidStatus = 0x02;
pub const TRANSACTION_STATUS_SUB_COMMITTED: XidStatus = 0x03;

/*
 * External functions not ported yet (access/clog.c, access/subtrans.c,
 * utils/time/snapmgr.c).  Stub locally so call sites translate 1:1.
 */
// TODO: import from crate::access::clog once ported.
unsafe fn TransactionIdGetStatus(_xid: TransactionId, _lsn: *mut XLogRecPtr) -> XidStatus {
    unimplemented!()
}
// TODO: import from crate::access::clog once ported.
unsafe fn TransactionIdSetTreeStatus(
    _xid: TransactionId,
    _nsubxids: c_int,
    _subxids: *mut TransactionId,
    _status: XidStatus,
    _lsn: XLogRecPtr,
) {
    unimplemented!()
}
// TODO: import from crate::access::subtrans once ported.
unsafe fn SubTransGetParent(_xid: TransactionId) -> TransactionId {
    unimplemented!()
}

/*
 * TransactionXmin lives in utils/time/snapmgr.c, not ported yet.
 */
// TODO: import from crate::utils::snapmgr once ported.
#[allow(non_upper_case_globals)]
static mut TransactionXmin: TransactionId = InvalidTransactionId;

/*
 * Single-item cache for results of TransactionLogFetch.  It's worth having
 * such a cache because we frequently find ourselves repeatedly checking the
 * same XID, for example when scanning a table just after a bulk insert,
 * update, or delete.
 */
#[allow(non_upper_case_globals)]
static mut cachedFetchXid: TransactionId = InvalidTransactionId;
#[allow(non_upper_case_globals)]
static mut cachedFetchXidStatus: XidStatus = 0;
#[allow(non_upper_case_globals)]
static mut cachedCommitLSN: XLogRecPtr = 0;

/* ----------------------------------------------------------------
 *		Postgres log access method interface
 *
 *		TransactionLogFetch
 * ----------------------------------------------------------------
 */

/*
 * TransactionLogFetch --- fetch commit status of specified transaction id
 */
unsafe fn TransactionLogFetch(transactionId: TransactionId) -> XidStatus {
    let xidstatus: XidStatus;
    let mut xidlsn: XLogRecPtr = 0;

    /*
     * Before going to the commit log manager, check our single item cache to
     * see if we didn't just check the transaction status a moment ago.
     */
    if TransactionIdEquals(transactionId, cachedFetchXid) {
        return cachedFetchXidStatus;
    }

    /*
     * Also, check to see if the transaction ID is a permanent one.
     */
    if !TransactionIdIsNormal(transactionId) {
        if TransactionIdEquals(transactionId, BootstrapTransactionId) {
            return TRANSACTION_STATUS_COMMITTED;
        }
        if TransactionIdEquals(transactionId, FrozenTransactionId) {
            return TRANSACTION_STATUS_COMMITTED;
        }
        return TRANSACTION_STATUS_ABORTED;
    }

    /*
     * Get the transaction status.
     */
    xidstatus = TransactionIdGetStatus(transactionId, &mut xidlsn);

    /*
     * Cache it, but DO NOT cache status for unfinished or sub-committed
     * transactions!  We only cache status that is guaranteed not to change.
     */
    if xidstatus != TRANSACTION_STATUS_IN_PROGRESS
        && xidstatus != TRANSACTION_STATUS_SUB_COMMITTED
    {
        cachedFetchXid = transactionId;
        cachedFetchXidStatus = xidstatus;
        cachedCommitLSN = xidlsn;
    }

    xidstatus
}

/* ----------------------------------------------------------------
 *						Interface functions
 *
 *		TransactionIdDidCommit
 *		TransactionIdDidAbort
 *		========
 *		   these functions test the transaction status of
 *		   a specified transaction id.
 *
 *		TransactionIdCommitTree
 *		TransactionIdAsyncCommitTree
 *		TransactionIdAbortTree
 *		========
 *		   these functions set the transaction status of the specified
 *		   transaction tree.
 *
 * See also TransactionIdIsInProgress, which once was in this module
 * but now lives in procarray.c, as well as comments at the top of
 * heapam_visibility.c that explain how everything fits together.
 * ----------------------------------------------------------------
 */

/*
 * TransactionIdDidCommit
 *		True iff transaction associated with the identifier did commit.
 *
 * Note:
 *		Assumes transaction identifier is valid and exists in clog.
 */
pub unsafe fn TransactionIdDidCommit(transactionId: TransactionId) -> bool {
    let xidstatus: XidStatus;

    xidstatus = TransactionLogFetch(transactionId);

    /*
     * If it's marked committed, it's committed.
     */
    if xidstatus == TRANSACTION_STATUS_COMMITTED {
        return true;
    }

    /*
     * If it's marked subcommitted, we have to check the parent recursively.
     * However, if it's older than TransactionXmin, we can't look at
     * pg_subtrans; instead assume that the parent crashed without cleaning up
     * its children.
     *
     * Originally we Assert'ed that the result of SubTransGetParent was not
     * zero. However with the introduction of prepared transactions, there can
     * be a window just after database startup where we do not have complete
     * knowledge in pg_subtrans of the transactions after TransactionXmin.
     * StartupSUBTRANS() has ensured that any missing information will be
     * zeroed.  Since this case should not happen under normal conditions, it
     * seems reasonable to emit a WARNING for it.
     */
    if xidstatus == TRANSACTION_STATUS_SUB_COMMITTED {
        let parentXid: TransactionId;

        if TransactionIdPrecedes(transactionId, TransactionXmin) {
            return false;
        }
        parentXid = SubTransGetParent(transactionId);
        if !TransactionIdIsValid(parentXid) {
            elog!(WARNING, "no pg_subtrans entry for subcommitted XID {}", transactionId);
            return false;
        }
        return TransactionIdDidCommit(parentXid);
    }

    /*
     * It's not committed.
     */
    false
}

/*
 * TransactionIdDidAbort
 *		True iff transaction associated with the identifier did abort.
 *
 * Note:
 *		Assumes transaction identifier is valid and exists in clog.
 *
 *		Returns true only for explicitly aborted transactions, as transactions
 *		implicitly aborted due to a crash will commonly still appear to be
 *		in-progress in the clog.  Most of the time TransactionIdDidCommit(),
 *		with a preceding TransactionIdIsInProgress() check, should be used
 *		instead of TransactionIdDidAbort().
 */
pub unsafe fn TransactionIdDidAbort(transactionId: TransactionId) -> bool {
    let xidstatus: XidStatus;

    xidstatus = TransactionLogFetch(transactionId);

    /*
     * If it's marked aborted, it's aborted.
     */
    if xidstatus == TRANSACTION_STATUS_ABORTED {
        return true;
    }

    /*
     * If it's marked subcommitted, we have to check the parent recursively.
     * However, if it's older than TransactionXmin, we can't look at
     * pg_subtrans; instead assume that the parent crashed without cleaning up
     * its children.
     */
    if xidstatus == TRANSACTION_STATUS_SUB_COMMITTED {
        let parentXid: TransactionId;

        if TransactionIdPrecedes(transactionId, TransactionXmin) {
            return true;
        }
        parentXid = SubTransGetParent(transactionId);
        if !TransactionIdIsValid(parentXid) {
            /* see notes in TransactionIdDidCommit */
            elog!(WARNING, "no pg_subtrans entry for subcommitted XID {}", transactionId);
            return true;
        }
        return TransactionIdDidAbort(parentXid);
    }

    /*
     * It's not aborted.
     */
    false
}

/*
 * TransactionIdCommitTree
 *		Marks the given transaction and children as committed
 *
 * "xid" is a toplevel transaction commit, and the xids array contains its
 * committed subtransactions.
 *
 * This commit operation is not guaranteed to be atomic, but if not, subxids
 * are correctly marked subcommit first.
 */
pub unsafe fn TransactionIdCommitTree(xid: TransactionId, nxids: c_int, xids: *mut TransactionId) {
    TransactionIdSetTreeStatus(
        xid,
        nxids,
        xids,
        TRANSACTION_STATUS_COMMITTED,
        InvalidXLogRecPtr,
    );
}

/*
 * TransactionIdAsyncCommitTree
 *		Same as above, but for async commits.  The commit record LSN is needed.
 */
pub unsafe fn TransactionIdAsyncCommitTree(
    xid: TransactionId,
    nxids: c_int,
    xids: *mut TransactionId,
    lsn: XLogRecPtr,
) {
    TransactionIdSetTreeStatus(xid, nxids, xids, TRANSACTION_STATUS_COMMITTED, lsn);
}

/*
 * TransactionIdAbortTree
 *		Marks the given transaction and children as aborted.
 *
 * "xid" is a toplevel transaction commit, and the xids array contains its
 * committed subtransactions.
 *
 * We don't need to worry about the non-atomic behavior, since any onlookers
 * will consider all the xacts as not-yet-committed anyway.
 */
pub unsafe fn TransactionIdAbortTree(xid: TransactionId, nxids: c_int, xids: *mut TransactionId) {
    TransactionIdSetTreeStatus(
        xid,
        nxids,
        xids,
        TRANSACTION_STATUS_ABORTED,
        InvalidXLogRecPtr,
    );
}

/*
 * TransactionIdPrecedes --- is id1 logically < id2?
 */
pub fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    /*
     * If either ID is a permanent XID then we can just do unsigned
     * comparison.  If both are normal, do a modulo-2^32 comparison.
     */
    let diff: int32;

    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }

    diff = id1.wrapping_sub(id2) as int32;
    diff < 0
}

/*
 * TransactionIdPrecedesOrEquals --- is id1 logically <= id2?
 */
pub fn TransactionIdPrecedesOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    let diff: int32;

    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 <= id2;
    }

    diff = id1.wrapping_sub(id2) as int32;
    diff <= 0
}

/*
 * TransactionIdFollows --- is id1 logically > id2?
 */
pub fn TransactionIdFollows(id1: TransactionId, id2: TransactionId) -> bool {
    let diff: int32;

    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 > id2;
    }

    diff = id1.wrapping_sub(id2) as int32;
    diff > 0
}

/*
 * TransactionIdFollowsOrEquals --- is id1 logically >= id2?
 */
pub fn TransactionIdFollowsOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    let diff: int32;

    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 >= id2;
    }

    diff = id1.wrapping_sub(id2) as int32;
    diff >= 0
}

/*
 * TransactionIdLatest --- get latest XID among a main xact and its children
 */
pub fn TransactionIdLatest(
    mainxid: TransactionId,
    mut nxids: c_int,
    xids: *const TransactionId,
) -> TransactionId {
    let mut result: TransactionId;

    /*
     * In practice it is highly likely that the xids[] array is sorted, and so
     * we could save some cycles by just taking the last child XID, but this
     * probably isn't so performance-critical that it's worth depending on
     * that assumption.  But just to show we're not totally stupid, scan the
     * array back-to-front to avoid useless assignments.
     */
    result = mainxid;
    nxids -= 1;
    while nxids >= 0 {
        let x = unsafe { *xids.offset(nxids as isize) };
        if TransactionIdPrecedes(result, x) {
            result = x;
        }
        nxids -= 1;
    }
    result
}

/*
 * TransactionIdGetCommitLSN
 *
 * This function returns an LSN that is late enough to be able
 * to guarantee that if we flush up to the LSN returned then we
 * will have flushed the transaction's commit record to disk.
 *
 * The result is not necessarily the exact LSN of the transaction's
 * commit record!  For example, for long-past transactions (those whose
 * clog pages already migrated to disk), we'll return InvalidXLogRecPtr.
 * Also, because we group transactions on the same clog page to conserve
 * storage, we might return the LSN of a later transaction that falls into
 * the same group.
 */
pub unsafe fn TransactionIdGetCommitLSN(xid: TransactionId) -> XLogRecPtr {
    let mut result: XLogRecPtr = 0;

    /*
     * Currently, all uses of this function are for xids that were just
     * reported to be committed by TransactionLogFetch, so we expect that
     * checking TransactionLogFetch's cache will usually succeed and avoid an
     * extra trip to shared memory.
     */
    if TransactionIdEquals(xid, cachedFetchXid) {
        return cachedCommitLSN;
    }

    /* Special XIDs are always known committed */
    if !TransactionIdIsNormal(xid) {
        return InvalidXLogRecPtr;
    }

    /*
     * Get the transaction status.
     */
    let _ = TransactionIdGetStatus(xid, &mut result);

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::transam::{
        FirstNormalTransactionId, FrozenTransactionId, MaxTransactionId,
    };

    #[test]
    fn precedes_follows_modulo() {
        // Both normal: modulo-2^32 comparison.
        let a = FirstNormalTransactionId + 5;
        let b = FirstNormalTransactionId + 10;
        assert!(TransactionIdPrecedes(a, b));
        assert!(!TransactionIdPrecedes(b, a));
        assert!(TransactionIdFollows(b, a));
        assert!(TransactionIdPrecedesOrEquals(a, a));
        assert!(TransactionIdFollowsOrEquals(a, a));

        // Wraparound: a near-MaxTransactionId precedes a small normal id.
        let near_max = MaxTransactionId - 1;
        assert!(TransactionIdPrecedes(near_max, FirstNormalTransactionId + 1));

        // Non-normal id: plain unsigned comparison.
        assert!(TransactionIdPrecedes(FrozenTransactionId, FirstNormalTransactionId));
        assert!(!TransactionIdPrecedes(FirstNormalTransactionId, FrozenTransactionId));
    }

    #[test]
    fn latest_picks_max_logical() {
        let main = FirstNormalTransactionId + 100;
        let xids = [
            FirstNormalTransactionId + 50,
            FirstNormalTransactionId + 200,
            FirstNormalTransactionId + 75,
        ];
        let got = TransactionIdLatest(main, xids.len() as c_int, xids.as_ptr());
        assert_eq!(got, FirstNormalTransactionId + 200);

        // No children: returns mainxid.
        assert_eq!(
            TransactionIdLatest(main, 0, core::ptr::null()),
            main
        );
    }
}
