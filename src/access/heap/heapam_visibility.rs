//! heapam_visibility.rs
//!   Tuple visibility rules for tuples stored in heap.
//!
//! Translated 1:1 from postgres/src/backend/access/heap/heapam_visibility.c
//!
//! NOTE: all the HeapTupleSatisfies routines will update the tuple's
//! "hint" status bits if we see that the inserting or deleting transaction
//! has now committed or aborted (and it is safe to set the hint bits).
//! If the hint bits are changed, MarkBufferDirtyHint is called on
//! the passed-in buffer.  The caller must hold not only a pin, but at least
//! shared buffer content lock on the buffer containing the tuple.
//!
//! NOTE: When using a non-MVCC snapshot, we must check
//! TransactionIdIsInProgress (which looks in the PGPROC array) before
//! TransactionIdDidCommit (which look in pg_xact).  Otherwise we have a race
//! condition: we might decide that a just-committed transaction crashed,
//! because none of the tests succeed.  xact.c is careful to record
//! commit/abort in pg_xact before it unsets MyProc->xid in the PGPROC array.
//! That fixes that problem, but it also means there is a window where
//! TransactionIdIsInProgress and TransactionIdDidCommit will both return true.
//! If we check only TransactionIdDidCommit, we could consider a tuple
//! committed when a later GetSnapshotData call will still think the
//! originating transaction is in progress, which leads to application-level
//! inconsistency.  The upshot is that we gotta check TransactionIdIsInProgress
//! first in all code paths, except for a few cases where we are looking at
//! subtransactions of our own main transaction and so there can't be any race
//! condition.
//!
//! We can't use TransactionIdDidAbort here because it won't treat transactions
//! that were in progress during a crash as aborted.  We determine that
//! transactions aborted/crashed through process of elimination instead.
//!
//! When using an MVCC snapshot, we rely on XidInMVCCSnapshot rather than
//! TransactionIdIsInProgress, but the logic is otherwise the same: do not
//! check pg_xact until after deciding that the xact is no longer in progress.
//!
//!
//! Summary of visibility functions:
//!
//!	 HeapTupleSatisfiesMVCC()
//!		  visible to supplied snapshot, excludes current command
//!	 HeapTupleSatisfiesUpdate()
//!		  visible to instant snapshot, with user-supplied command
//!		  counter and more complex result
//!	 HeapTupleSatisfiesSelf()
//!		  visible to instant snapshot and current command
//!	 HeapTupleSatisfiesDirty()
//!		  like HeapTupleSatisfiesSelf(), but includes open transactions
//!	 HeapTupleSatisfiesVacuum()
//!		  visible to any running transaction, used by VACUUM
//!	 HeapTupleSatisfiesNonVacuumable()
//!		  Snapshot-style API for HeapTupleSatisfiesVacuum
//!	 HeapTupleSatisfiesToast()
//!		  visible unless part of interrupted vacuum, used for TOAST
//!	 HeapTupleSatisfiesAny()
//!		  all tuples are visible
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/access/heap/heapam_visibility.c

use crate::prelude::*; // postgres.h: c types, Datum, palloc, elog!/ereport!/errmsg!/Assert!, Max/Min

use std::ffi::c_int;
use std::ffi::c_void;

use crate::c::CommandId;
use crate::c::InvalidCommandId;
use crate::c::Size;
use crate::c::TransactionId;
use crate::c::uint16;

// postgres_ext.h
use crate::postgres_ext::InvalidOid;

// access/htup_details.h
use crate::access::htup_details::HeapTuple;
use crate::access::htup_details::HeapTupleHeader;
use crate::access::htup_details::HeapTupleGetUpdateXid;
use crate::access::htup_details::HeapTupleHeaderGetCmax;
use crate::access::htup_details::HeapTupleHeaderGetCmin;
use crate::access::htup_details::HeapTupleHeaderGetRawCommandId;
use crate::access::htup_details::HeapTupleHeaderGetRawXmax;
use crate::access::htup_details::HeapTupleHeaderGetRawXmin;
use crate::access::htup_details::HeapTupleHeaderGetSpeculativeToken;
use crate::access::htup_details::HeapTupleHeaderGetUpdateXid;
use crate::access::htup_details::HeapTupleHeaderGetXmin;
use crate::access::htup_details::HeapTupleHeaderGetXvac;
use crate::access::htup_details::HeapTupleHeaderIsSpeculative;
use crate::access::htup_details::HeapTupleHeaderXminCommitted;
use crate::access::htup_details::HeapTupleHeaderXminFrozen;
use crate::access::htup_details::HeapTupleHeaderXminInvalid;
use crate::access::htup_details::HEAP_LOCKED_UPGRADED;
use crate::access::htup_details::HEAP_MOVED_IN;
use crate::access::htup_details::HEAP_MOVED_OFF;
use crate::access::htup_details::HEAP_XMAX_COMMITTED;
use crate::access::htup_details::HEAP_XMAX_INVALID;
use crate::access::htup_details::HEAP_XMAX_IS_LOCKED_ONLY;
use crate::access::htup_details::HEAP_XMAX_IS_MULTI;
use crate::access::htup_details::HEAP_XMAX_LOCK_ONLY;
use crate::access::htup_details::HEAP_XMIN_COMMITTED;
use crate::access::htup_details::HEAP_XMIN_INVALID;

// access/multixact.h
use crate::access::transam::multixact::MultiXactIdIsRunning;

// access/tableam.h
use crate::access::table::tableam::TM_Result;
use crate::access::table::tableam::TM_Ok;
use crate::access::table::tableam::TM_Invisible;
use crate::access::table::tableam::TM_SelfModified;
use crate::access::table::tableam::TM_Updated;
use crate::access::table::tableam::TM_Deleted;
use crate::access::table::tableam::TM_BeingModified;

// access/transam.h
use crate::access::transam::InvalidTransactionId;
use crate::access::transam::TransactionIdIsValid;
use crate::access::transam::transam::TransactionIdDidCommit;
use crate::access::transam::transam::TransactionIdFollowsOrEquals;
use crate::access::transam::transam::TransactionIdGetCommitLSN;
use crate::access::transam::transam::TransactionIdPrecedes;

// access/xlogdefs.h
use crate::access::transam::xlogdefs::XLogRecPtr;

// storage/buf.h
use crate::storage::buf::Buffer;

// storage/itemptr.h
use crate::storage::itemptr::ItemPointerEquals;
use crate::storage::itemptr::ItemPointerIsValid;

// utils/snapshot.h
use crate::utils::snapshot::GlobalVisState;
use crate::utils::snapshot::Snapshot;
use crate::utils::snapshot::SNAPSHOT_ANY;
use crate::utils::snapshot::SNAPSHOT_DIRTY;
use crate::utils::snapshot::SNAPSHOT_HISTORIC_MVCC;
use crate::utils::snapshot::SNAPSHOT_MVCC;
use crate::utils::snapshot::SNAPSHOT_NON_VACUUMABLE;
use crate::utils::snapshot::SNAPSHOT_SELF;
use crate::utils::snapshot::SNAPSHOT_TOAST;

// utils/adt/xid.c
use crate::utils::adt::xid::xidComparator;

/*
 * HTSV_Result is the result type for HeapTupleSatisfiesVacuum and friends.
 * (C enum HTSV_Result -> type alias + consts, per project convention.)
 *
 * NB: real HTSV_Result/HEAPTUPLE_* lives in access/heapam.h; once that lands
 * this module should import it instead of re-declaring it.
 */
// TODO(pg-port): real HTSV_Result enum lives in access/heapam.h
pub type HTSV_Result = c_int;
/* Either DEAD, or recently dead and not vacuumable */
pub const HEAPTUPLE_DEAD: HTSV_Result = 0;
/* Visible to all transactions */
pub const HEAPTUPLE_LIVE: HTSV_Result = 1;
/* Recently dead but might still be visible to some */
pub const HEAPTUPLE_RECENTLY_DEAD: HTSV_Result = 2;
/* Inserting xact is still in progress */
pub const HEAPTUPLE_INSERT_IN_PROGRESS: HTSV_Result = 3;
/* Deleting xact is still in progress */
pub const HEAPTUPLE_DELETE_IN_PROGRESS: HTSV_Result = 4;

/* ----------------------------------------------------------------
 *		stubs for symbols that have no home in the port yet
 * ---------------------------------------------------------------- */

// TODO(pg-port): real TransactionIdIsCurrentTransactionId lives in access/transam/xact.c
unsafe fn TransactionIdIsCurrentTransactionId(_xid: TransactionId) -> bool {
    crate::access::transam::xact::TransactionIdIsCurrentTransactionId(_xid)
}

unsafe fn TransactionIdIsInProgress(_xid: TransactionId) -> bool {
    crate::storage::ipc::procarray::TransactionIdIsInProgress(_xid)
}

unsafe fn XidInMVCCSnapshot(_xid: TransactionId, _snapshot: Snapshot) -> bool {
    crate::utils::time::snapmgr::XidInMVCCSnapshot(_xid, _snapshot as _)
}

// TODO(pg-port): real GlobalVisTestIsRemovableXid lives in storage/ipc/procarray.c
unsafe fn GlobalVisTestIsRemovableXid(_state: *mut GlobalVisState, _xid: TransactionId) -> bool {
    false
}

// TODO(pg-port): real MarkBufferDirtyHint lives in storage/buffer/bufmgr.c
unsafe fn MarkBufferDirtyHint(_buffer: Buffer, _buffer_std: bool) {}

// TODO(pg-port): real BufferIsPermanent lives in storage/buffer/bufmgr.c
unsafe fn BufferIsPermanent(_buffer: Buffer) -> bool {
    true
}

// TODO(pg-port): real BufferGetLSNAtomic lives in storage/buffer/bufmgr.c
unsafe fn BufferGetLSNAtomic(_buffer: Buffer) -> XLogRecPtr {
    0
}

// TODO(pg-port): real XLogNeedsFlush lives in access/transam/xlog.c
unsafe fn XLogNeedsFlush(_record: XLogRecPtr) -> bool {
    false
}

// TODO(pg-port): real ResolveCminCmaxDuringDecoding lives in replication/logical/reorderbuffer.c
unsafe fn ResolveCminCmaxDuringDecoding(
    _tuplecid_data: *mut c_void,
    _snapshot: Snapshot,
    _htup: HeapTuple,
    _buffer: Buffer,
    _cmin: *mut CommandId,
    _cmax: *mut CommandId,
) -> bool {
    false
}

// TODO(pg-port): real HistoricSnapshotGetTupleCids lives in utils/time/snapmgr.c
unsafe fn HistoricSnapshotGetTupleCids() -> *mut c_void {
    std::ptr::null_mut()
}

/*
 * SetHintBits()
 *
 * Set commit/abort hint bits on a tuple, if appropriate at this time.
 *
 * It is only safe to set a transaction-committed hint bit if we know the
 * transaction's commit record is guaranteed to be flushed to disk before the
 * buffer, or if the table is temporary or unlogged and will be obliterated by
 * a crash anyway.  We cannot change the LSN of the page here, because we may
 * hold only a share lock on the buffer, so we can only use the LSN to
 * interlock this if the buffer's LSN already is newer than the commit LSN;
 * otherwise we have to just refrain from setting the hint bit until some
 * future re-examination of the tuple.
 *
 * We can always set hint bits when marking a transaction aborted.  (Some
 * code in heapam.c relies on that!)
 *
 * Also, if we are cleaning up HEAP_MOVED_IN or HEAP_MOVED_OFF entries, then
 * we can always set the hint bits, since pre-9.0 VACUUM FULL always used
 * synchronous commits and didn't move tuples that weren't previously
 * hinted.  (This is not known by this subroutine, but is applied by its
 * callers.)  Note: old-style VACUUM FULL is gone, but we have to keep this
 * module's support for MOVED_OFF/MOVED_IN flag bits for as long as we
 * support in-place update from pre-9.0 databases.
 *
 * Normal commits may be asynchronous, so for those we need to get the LSN
 * of the transaction and then check whether this is flushed.
 *
 * The caller should pass xid as the XID of the transaction to check, or
 * InvalidTransactionId if no check is needed.
 */
#[inline]
unsafe fn SetHintBits(tuple: HeapTupleHeader, buffer: Buffer, infomask: uint16, xid: TransactionId) {
    if TransactionIdIsValid(xid) {
        /* NB: xid must be known committed here! */
        let commitLSN: XLogRecPtr = TransactionIdGetCommitLSN(xid);

        if BufferIsPermanent(buffer)
            && XLogNeedsFlush(commitLSN)
            && BufferGetLSNAtomic(buffer) < commitLSN
        {
            /* not flushed and no LSN interlock, so don't set hint */
            return;
        }
    }

    (*tuple).t_infomask |= infomask;
    MarkBufferDirtyHint(buffer, true);
}

/*
 * HeapTupleSetHintBits --- exported version of SetHintBits()
 *
 * This must be separate because of C99's brain-dead notions about how to
 * implement inline functions.
 */
pub unsafe fn HeapTupleSetHintBits(
    tuple: HeapTupleHeader,
    buffer: Buffer,
    infomask: uint16,
    xid: TransactionId,
) {
    SetHintBits(tuple, buffer, infomask, xid);
}

/*
 * HeapTupleSatisfiesSelf
 *		True iff heap tuple is valid "for itself".
 *
 * See SNAPSHOT_MVCC's definition for the intended behaviour.
 *
 * Note:
 *		Assumes heap tuple is valid.
 *
 * The satisfaction of "itself" requires the following:
 *
 * ((Xmin == my-transaction &&				the row was updated by the current transaction, and
 *		(Xmax is null						it was not deleted
 *		 [|| Xmax != my-transaction)])			[or it was deleted by another transaction]
 * ||
 *
 * (Xmin is committed &&					the row was modified by a committed transaction, and
 *		(Xmax is null ||					the row has not been deleted, or
 *			(Xmax != my-transaction &&			the row was deleted by another transaction
 *			 Xmax is not committed)))			that has not been committed
 */
unsafe fn HeapTupleSatisfiesSelf(htup: HeapTuple, _snapshot: Snapshot, buffer: Buffer) -> bool {
    let tuple: HeapTupleHeader = (*htup).t_data;

    Assert!(ItemPointerIsValid(&(*htup).t_self));
    Assert!((*htup).t_tableOid != InvalidOid);

    if !HeapTupleHeaderXminCommitted(tuple) {
        if HeapTupleHeaderXminInvalid(tuple) {
            return false;
        }

        /* Used by pre-9.0 binary upgrades */
        if ((*tuple).t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

            if TransactionIdIsCurrentTransactionId(xvac) {
                return false;
            }
            if !TransactionIdIsInProgress(xvac) {
                if TransactionIdDidCommit(xvac) {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                    return false;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
            }
        }
        /* Used by pre-9.0 binary upgrades */
        else if ((*tuple).t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

            if !TransactionIdIsCurrentTransactionId(xvac) {
                if TransactionIdIsInProgress(xvac) {
                    return false;
                }
                if TransactionIdDidCommit(xvac) {
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
                } else {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                    return false;
                }
            }
        } else if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)) {
            if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
                /* xid invalid */
                return true;
            }

            if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
                /* not deleter */
                return true;
            }

            if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                let xmax: TransactionId;

                xmax = HeapTupleGetUpdateXid(tuple);

                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert!(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                if !TransactionIdIsCurrentTransactionId(xmax) {
                    return true;
                } else {
                    return false;
                }
            }

            if !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
                return true;
            }

            return false;
        } else if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)) {
            return false;
        } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)) {
            SetHintBits(
                tuple,
                buffer,
                HEAP_XMIN_COMMITTED,
                HeapTupleHeaderGetRawXmin(tuple),
            );
        } else {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return false;
        }
    }

    /* by here, the inserting transaction has committed */

    if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
        /* xid invalid or aborted */
        return true;
    }

    if ((*tuple).t_infomask & HEAP_XMAX_COMMITTED) != 0 {
        if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
            return true;
        }
        return false; /* updated by other */
    }

    if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        let xmax: TransactionId;

        if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
            return true;
        }

        xmax = HeapTupleGetUpdateXid(tuple);

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsCurrentTransactionId(xmax) {
            return false;
        }
        if TransactionIdIsInProgress(xmax) {
            return true;
        }
        if TransactionIdDidCommit(xmax) {
            return false;
        }
        /* it must have aborted or crashed */
        return true;
    }

    if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
        if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
            return true;
        }
        return false;
    }

    if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)) {
        return true;
    }

    if !TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)) {
        /* it must have aborted or crashed */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    /* xmax transaction committed */

    if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    SetHintBits(
        tuple,
        buffer,
        HEAP_XMAX_COMMITTED,
        HeapTupleHeaderGetRawXmax(tuple),
    );
    false
}

/*
 * HeapTupleSatisfiesAny
 *		Dummy "satisfies" routine: any tuple satisfies SnapshotAny.
 */
unsafe fn HeapTupleSatisfiesAny(_htup: HeapTuple, _snapshot: Snapshot, _buffer: Buffer) -> bool {
    true
}

/*
 * HeapTupleSatisfiesToast
 *		True iff heap tuple is valid as a TOAST row.
 *
 * See SNAPSHOT_TOAST's definition for the intended behaviour.
 *
 * This is a simplified version that only checks for VACUUM moving conditions.
 * It's appropriate for TOAST usage because TOAST really doesn't want to do
 * its own time qual checks; if you can see the main table row that contains
 * a TOAST reference, you should be able to see the TOASTed value.  However,
 * vacuuming a TOAST table is independent of the main table, and in case such
 * a vacuum fails partway through, we'd better do this much checking.
 *
 * Among other things, this means you can't do UPDATEs of rows in a TOAST
 * table.
 */
unsafe fn HeapTupleSatisfiesToast(htup: HeapTuple, _snapshot: Snapshot, buffer: Buffer) -> bool {
    let tuple: HeapTupleHeader = (*htup).t_data;

    Assert!(ItemPointerIsValid(&(*htup).t_self));
    Assert!((*htup).t_tableOid != InvalidOid);

    if !HeapTupleHeaderXminCommitted(tuple) {
        if HeapTupleHeaderXminInvalid(tuple) {
            return false;
        }

        /* Used by pre-9.0 binary upgrades */
        if ((*tuple).t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

            if TransactionIdIsCurrentTransactionId(xvac) {
                return false;
            }
            if !TransactionIdIsInProgress(xvac) {
                if TransactionIdDidCommit(xvac) {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                    return false;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
            }
        }
        /* Used by pre-9.0 binary upgrades */
        else if ((*tuple).t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

            if !TransactionIdIsCurrentTransactionId(xvac) {
                if TransactionIdIsInProgress(xvac) {
                    return false;
                }
                if TransactionIdDidCommit(xvac) {
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
                } else {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                    return false;
                }
            }
        }
        /*
         * An invalid Xmin can be left behind by a speculative insertion that
         * is canceled by super-deleting the tuple.  This also applies to
         * TOAST tuples created during speculative insertion.
         */
        else if !TransactionIdIsValid(HeapTupleHeaderGetXmin(tuple)) {
            return false;
        }
    }

    /* otherwise assume the tuple is valid for TOAST. */
    true
}

/*
 * HeapTupleSatisfiesUpdate
 *
 *	This function returns a more detailed result code than most of the
 *	functions in this file, since UPDATE needs to know more than "is it
 *	visible?".  It also allows for user-supplied CommandId rather than
 *	relying on CurrentCommandId.
 *
 *	The possible return codes are:
 *
 *	TM_Invisible: the tuple didn't exist at all when the scan started, e.g. it
 *	was created by a later CommandId.
 *
 *	TM_Ok: The tuple is valid and visible, so it may be updated.
 *
 *	TM_SelfModified: The tuple was updated by the current transaction, after
 *	the current scan started.
 *
 *	TM_Updated: The tuple was updated by a committed transaction (including
 *	the case where the tuple was moved into a different partition).
 *
 *	TM_Deleted: The tuple was deleted by a committed transaction.
 *
 *	TM_BeingModified: The tuple is being updated by an in-progress transaction
 *	other than the current transaction.  (Note: this includes the case where
 *	the tuple is share-locked by a MultiXact, even if the MultiXact includes
 *	the current transaction.  Callers that want to distinguish that case must
 *	test for it themselves.)
 */
pub unsafe fn HeapTupleSatisfiesUpdate(htup: HeapTuple, curcid: CommandId, buffer: Buffer) -> TM_Result {
    let tuple: HeapTupleHeader = (*htup).t_data;

    Assert!(ItemPointerIsValid(&(*htup).t_self));
    Assert!((*htup).t_tableOid != InvalidOid);

    if !HeapTupleHeaderXminCommitted(tuple) {
        if HeapTupleHeaderXminInvalid(tuple) {
            return TM_Invisible;
        }

        /* Used by pre-9.0 binary upgrades */
        if ((*tuple).t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

            if TransactionIdIsCurrentTransactionId(xvac) {
                return TM_Invisible;
            }
            if !TransactionIdIsInProgress(xvac) {
                if TransactionIdDidCommit(xvac) {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                    return TM_Invisible;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
            }
        }
        /* Used by pre-9.0 binary upgrades */
        else if ((*tuple).t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

            if !TransactionIdIsCurrentTransactionId(xvac) {
                if TransactionIdIsInProgress(xvac) {
                    return TM_Invisible;
                }
                if TransactionIdDidCommit(xvac) {
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
                } else {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                    return TM_Invisible;
                }
            }
        } else if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)) {
            if HeapTupleHeaderGetCmin(tuple) >= curcid {
                return TM_Invisible; /* inserted after scan started */
            }

            if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
                /* xid invalid */
                return TM_Ok;
            }

            if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
                let xmax: TransactionId;

                xmax = HeapTupleHeaderGetRawXmax(tuple);

                /*
                 * Careful here: even though this tuple was created by our own
                 * transaction, it might be locked by other transactions, if
                 * the original version was key-share locked when we updated
                 * it.
                 */

                if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                    if MultiXactIdIsRunning(xmax, true) {
                        return TM_BeingModified;
                    } else {
                        return TM_Ok;
                    }
                }

                /*
                 * If the locker is gone, then there is nothing of interest
                 * left in this Xmax; otherwise, report the tuple as
                 * locked/updated.
                 */
                if !TransactionIdIsInProgress(xmax) {
                    return TM_Ok;
                }
                return TM_BeingModified;
            }

            if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                let xmax: TransactionId;

                xmax = HeapTupleGetUpdateXid(tuple);

                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert!(TransactionIdIsValid(xmax));

                /* deleting subtransaction must have aborted */
                if !TransactionIdIsCurrentTransactionId(xmax) {
                    if MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false) {
                        return TM_BeingModified;
                    }
                    return TM_Ok;
                } else {
                    if HeapTupleHeaderGetCmax(tuple) >= curcid {
                        return TM_SelfModified; /* updated after scan started */
                    } else {
                        return TM_Invisible; /* updated before scan started */
                    }
                }
            }

            if !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
                return TM_Ok;
            }

            if HeapTupleHeaderGetCmax(tuple) >= curcid {
                return TM_SelfModified; /* updated after scan started */
            } else {
                return TM_Invisible; /* updated before scan started */
            }
        } else if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)) {
            return TM_Invisible;
        } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)) {
            SetHintBits(
                tuple,
                buffer,
                HEAP_XMIN_COMMITTED,
                HeapTupleHeaderGetRawXmin(tuple),
            );
        } else {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return TM_Invisible;
        }
    }

    /* by here, the inserting transaction has committed */

    if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
        /* xid invalid or aborted */
        return TM_Ok;
    }

    if ((*tuple).t_infomask & HEAP_XMAX_COMMITTED) != 0 {
        if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
            return TM_Ok;
        }
        if !ItemPointerEquals(&raw mut (*htup).t_self, &raw mut (*tuple).t_ctid) {
            return TM_Updated; /* updated by other */
        } else {
            return TM_Deleted; /* deleted by other */
        }
    }

    if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        let xmax: TransactionId;

        if HEAP_LOCKED_UPGRADED((*tuple).t_infomask) {
            return TM_Ok;
        }

        if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
            if MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true) {
                return TM_BeingModified;
            }

            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return TM_Ok;
        }

        xmax = HeapTupleGetUpdateXid(tuple);
        if !TransactionIdIsValid(xmax) {
            if MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false) {
                return TM_BeingModified;
            }
        }

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsCurrentTransactionId(xmax) {
            if HeapTupleHeaderGetCmax(tuple) >= curcid {
                return TM_SelfModified; /* updated after scan started */
            } else {
                return TM_Invisible; /* updated before scan started */
            }
        }

        if MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false) {
            return TM_BeingModified;
        }

        if TransactionIdDidCommit(xmax) {
            if !ItemPointerEquals(&raw mut (*htup).t_self, &raw mut (*tuple).t_ctid) {
                return TM_Updated;
            } else {
                return TM_Deleted;
            }
        }

        /*
         * By here, the update in the Xmax is either aborted or crashed, but
         * what about the other members?
         */

        if !MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false) {
            /*
             * There's no member, even just a locker, alive anymore, so we can
             * mark the Xmax as invalid.
             */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return TM_Ok;
        } else {
            /* There are lockers running */
            return TM_BeingModified;
        }
    }

    if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
        if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
            return TM_BeingModified;
        }
        if HeapTupleHeaderGetCmax(tuple) >= curcid {
            return TM_SelfModified; /* updated after scan started */
        } else {
            return TM_Invisible; /* updated before scan started */
        }
    }

    if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)) {
        return TM_BeingModified;
    }

    if !TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)) {
        /* it must have aborted or crashed */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return TM_Ok;
    }

    /* xmax transaction committed */

    if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return TM_Ok;
    }

    SetHintBits(
        tuple,
        buffer,
        HEAP_XMAX_COMMITTED,
        HeapTupleHeaderGetRawXmax(tuple),
    );
    if !ItemPointerEquals(&raw mut (*htup).t_self, &raw mut (*tuple).t_ctid) {
        TM_Updated /* updated by other */
    } else {
        TM_Deleted /* deleted by other */
    }
}

/*
 * HeapTupleSatisfiesDirty
 *		True iff heap tuple is valid including effects of open transactions.
 *
 * See SNAPSHOT_DIRTY's definition for the intended behaviour.
 *
 * This is essentially like HeapTupleSatisfiesSelf as far as effects of
 * the current transaction and committed/aborted xacts are concerned.
 * However, we also include the effects of other xacts still in progress.
 *
 * A special hack is that the passed-in snapshot struct is used as an
 * output argument to return the xids of concurrent xacts that affected the
 * tuple.  snapshot->xmin is set to the tuple's xmin if that is another
 * transaction that's still in progress; or to InvalidTransactionId if the
 * tuple's xmin is committed good, committed dead, or my own xact.
 * Similarly for snapshot->xmax and the tuple's xmax.  If the tuple was
 * inserted speculatively, meaning that the inserter might still back down
 * on the insertion without aborting the whole transaction, the associated
 * token is also returned in snapshot->speculativeToken.
 */
unsafe fn HeapTupleSatisfiesDirty(htup: HeapTuple, snapshot: Snapshot, buffer: Buffer) -> bool {
    let tuple: HeapTupleHeader = (*htup).t_data;

    Assert!(ItemPointerIsValid(&(*htup).t_self));
    Assert!((*htup).t_tableOid != InvalidOid);

    (*snapshot).xmax = InvalidTransactionId;
    (*snapshot).xmin = (*snapshot).xmax;
    (*snapshot).speculativeToken = 0;

    if !HeapTupleHeaderXminCommitted(tuple) {
        if HeapTupleHeaderXminInvalid(tuple) {
            return false;
        }

        /* Used by pre-9.0 binary upgrades */
        if ((*tuple).t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

            if TransactionIdIsCurrentTransactionId(xvac) {
                return false;
            }
            if !TransactionIdIsInProgress(xvac) {
                if TransactionIdDidCommit(xvac) {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                    return false;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
            }
        }
        /* Used by pre-9.0 binary upgrades */
        else if ((*tuple).t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

            if !TransactionIdIsCurrentTransactionId(xvac) {
                if TransactionIdIsInProgress(xvac) {
                    return false;
                }
                if TransactionIdDidCommit(xvac) {
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
                } else {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                    return false;
                }
            }
        } else if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)) {
            if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
                /* xid invalid */
                return true;
            }

            if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
                /* not deleter */
                return true;
            }

            if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                let xmax: TransactionId;

                xmax = HeapTupleGetUpdateXid(tuple);

                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert!(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                if !TransactionIdIsCurrentTransactionId(xmax) {
                    return true;
                } else {
                    return false;
                }
            }

            if !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
                return true;
            }

            return false;
        } else if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)) {
            /*
             * Return the speculative token to caller.  Caller can worry about
             * xmax, since it requires a conclusively locked row version, and
             * a concurrent update to this tuple is a conflict of its
             * purposes.
             */
            if HeapTupleHeaderIsSpeculative(tuple) {
                (*snapshot).speculativeToken = HeapTupleHeaderGetSpeculativeToken(tuple);

                Assert!((*snapshot).speculativeToken != 0);
            }

            (*snapshot).xmin = HeapTupleHeaderGetRawXmin(tuple);
            /* XXX shouldn't we fall through to look at xmax? */
            return true; /* in insertion by other */
        } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)) {
            SetHintBits(
                tuple,
                buffer,
                HEAP_XMIN_COMMITTED,
                HeapTupleHeaderGetRawXmin(tuple),
            );
        } else {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return false;
        }
    }

    /* by here, the inserting transaction has committed */

    if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
        /* xid invalid or aborted */
        return true;
    }

    if ((*tuple).t_infomask & HEAP_XMAX_COMMITTED) != 0 {
        if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
            return true;
        }
        return false; /* updated by other */
    }

    if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        let xmax: TransactionId;

        if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
            return true;
        }

        xmax = HeapTupleGetUpdateXid(tuple);

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsCurrentTransactionId(xmax) {
            return false;
        }
        if TransactionIdIsInProgress(xmax) {
            (*snapshot).xmax = xmax;
            return true;
        }
        if TransactionIdDidCommit(xmax) {
            return false;
        }
        /* it must have aborted or crashed */
        return true;
    }

    if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
        if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
            return true;
        }
        return false;
    }

    if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)) {
        if !HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
            (*snapshot).xmax = HeapTupleHeaderGetRawXmax(tuple);
        }
        return true;
    }

    if !TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)) {
        /* it must have aborted or crashed */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    /* xmax transaction committed */

    if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    SetHintBits(
        tuple,
        buffer,
        HEAP_XMAX_COMMITTED,
        HeapTupleHeaderGetRawXmax(tuple),
    );
    false /* updated by other */
}

/*
 * HeapTupleSatisfiesMVCC
 *		True iff heap tuple is valid for the given MVCC snapshot.
 *
 * See SNAPSHOT_MVCC's definition for the intended behaviour.
 *
 * Notice that here, we will not update the tuple status hint bits if the
 * inserting/deleting transaction is still running according to our snapshot,
 * even if in reality it's committed or aborted by now.  This is intentional.
 * Checking the true transaction state would require access to high-traffic
 * shared data structures, creating contention we'd rather do without, and it
 * would not change the result of our visibility check anyway.  The hint bits
 * will be updated by the first visitor that has a snapshot new enough to see
 * the inserting/deleting transaction as done.  In the meantime, the cost of
 * leaving the hint bits unset is basically that each HeapTupleSatisfiesMVCC
 * call will need to run TransactionIdIsCurrentTransactionId in addition to
 * XidInMVCCSnapshot (but it would have to do the latter anyway).  In the old
 * coding where we tried to set the hint bits as soon as possible, we instead
 * did TransactionIdIsInProgress in each call --- to no avail, as long as the
 * inserting/deleting transaction was still running --- which was more cycles
 * and more contention on ProcArrayLock.
 */
unsafe fn HeapTupleSatisfiesMVCC(htup: HeapTuple, snapshot: Snapshot, buffer: Buffer) -> bool {
    let tuple: HeapTupleHeader = (*htup).t_data;
    if std::env::var_os("PDB_BT").is_some() {
        let rx = HeapTupleHeaderGetRawXmin(tuple);
        if rx > 700 {
            eprintln!("PDB_BT MVCC-call tableOid={} xmin={} xminCommitted={} snap_xmax={}",
                (*htup).t_tableOid, rx, HeapTupleHeaderXminCommitted(tuple), (*snapshot).xmax);
        }
    }

    /*
     * Assert that the caller has registered the snapshot.  This function
     * doesn't care about the registration as such, but in general you
     * shouldn't try to use a snapshot without registration because it might
     * get invalidated while it's still in use, and this is a convenient place
     * to check for that.
     */
    Assert!((*snapshot).regd_count > 0 || (*snapshot).active_count > 0);

    Assert!(ItemPointerIsValid(&(*htup).t_self));
    Assert!((*htup).t_tableOid != InvalidOid);

    if !HeapTupleHeaderXminCommitted(tuple) {
        if std::env::var("PDB_BT").is_ok() {
            let rx = HeapTupleHeaderGetRawXmin(tuple);
            if rx > 700 {
                eprintln!("PDB_BT MVCC noncommitted xmin={} isCurrent={} xmininvalid={} topxid={}", rx, TransactionIdIsCurrentTransactionId(rx), HeapTupleHeaderXminInvalid(tuple), crate::access::transam::xact::GetTopTransactionIdIfAny());
            }
        }
        if HeapTupleHeaderXminInvalid(tuple) {
            return false;
        }

        /* Used by pre-9.0 binary upgrades */
        if ((*tuple).t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

            if TransactionIdIsCurrentTransactionId(xvac) {
                return false;
            }
            if !XidInMVCCSnapshot(xvac, snapshot) {
                if TransactionIdDidCommit(xvac) {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                    return false;
                }
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
            }
        }
        /* Used by pre-9.0 binary upgrades */
        else if ((*tuple).t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

            if !TransactionIdIsCurrentTransactionId(xvac) {
                if XidInMVCCSnapshot(xvac, snapshot) {
                    return false;
                }
                if TransactionIdDidCommit(xvac) {
                    SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
                } else {
                    SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                    return false;
                }
            }
        } else if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)) {
            if std::env::var("PDB_BT").is_ok() {
                eprintln!("PDB_BT MVCC own-xact xmin={} cmin={} curcid={} -> {}", HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetCmin(tuple), (*snapshot).curcid, HeapTupleHeaderGetCmin(tuple) < (*snapshot).curcid);
            }
            if HeapTupleHeaderGetCmin(tuple) >= (*snapshot).curcid {
                return false; /* inserted after scan started */
            }

            if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
                /* xid invalid */
                return true;
            }

            if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
                /* not deleter */
                return true;
            }

            if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                let xmax: TransactionId;

                xmax = HeapTupleGetUpdateXid(tuple);

                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert!(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                if !TransactionIdIsCurrentTransactionId(xmax) {
                    return true;
                } else if HeapTupleHeaderGetCmax(tuple) >= (*snapshot).curcid {
                    return true; /* updated after scan started */
                } else {
                    return false; /* updated before scan started */
                }
            }

            if !TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
                /* deleting subtransaction must have aborted */
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
                return true;
            }

            if HeapTupleHeaderGetCmax(tuple) >= (*snapshot).curcid {
                return true; /* deleted after scan started */
            } else {
                return false; /* deleted before scan started */
            }
        } else if XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot) {
            return false;
        } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)) {
            SetHintBits(
                tuple,
                buffer,
                HEAP_XMIN_COMMITTED,
                HeapTupleHeaderGetRawXmin(tuple),
            );
        } else {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return false;
        }
    } else {
        /* xmin is committed, but maybe not according to our snapshot */
        if !HeapTupleHeaderXminFrozen(tuple)
            && XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot)
        {
            return false; /* treat as still in progress */
        }
    }

    /* by here, the inserting transaction has committed */

    if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
        /* xid invalid or aborted */
        return true;
    }

    if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
        return true;
    }

    if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        let xmax: TransactionId;

        /* already checked above */
        Assert!(!HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask));

        xmax = HeapTupleGetUpdateXid(tuple);

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsCurrentTransactionId(xmax) {
            if HeapTupleHeaderGetCmax(tuple) >= (*snapshot).curcid {
                return true; /* deleted after scan started */
            } else {
                return false; /* deleted before scan started */
            }
        }
        if XidInMVCCSnapshot(xmax, snapshot) {
            return true;
        }
        if TransactionIdDidCommit(xmax) {
            return false; /* updating transaction committed */
        }
        /* it must have aborted or crashed */
        return true;
    }

    if ((*tuple).t_infomask & HEAP_XMAX_COMMITTED) == 0 {
        if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)) {
            if HeapTupleHeaderGetCmax(tuple) >= (*snapshot).curcid {
                return true; /* deleted after scan started */
            } else {
                return false; /* deleted before scan started */
            }
        }

        if XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot) {
            return true;
        }

        if !TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)) {
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return true;
        }

        /* xmax transaction committed */
        SetHintBits(
            tuple,
            buffer,
            HEAP_XMAX_COMMITTED,
            HeapTupleHeaderGetRawXmax(tuple),
        );
    } else {
        /* xmax is committed, but maybe not according to our snapshot */
        if XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot) {
            return true; /* treat as still in progress */
        }
    }

    /* xmax transaction committed */

    false
}

/*
 * HeapTupleSatisfiesVacuum
 *
 *	Determine the status of tuples for VACUUM purposes.  Here, what
 *	we mainly want to know is if a tuple is potentially visible to *any*
 *	running transaction.  If so, it can't be removed yet by VACUUM.
 *
 * OldestXmin is a cutoff XID (obtained from
 * GetOldestNonRemovableTransactionId()).  Tuples deleted by XIDs >=
 * OldestXmin are deemed "recently dead"; they might still be visible to some
 * open transaction, so we can't remove them, even if we see that the deleting
 * transaction has committed.
 */
pub unsafe fn HeapTupleSatisfiesVacuum(
    htup: HeapTuple,
    OldestXmin: TransactionId,
    buffer: Buffer,
) -> HTSV_Result {
    let mut dead_after: TransactionId = InvalidTransactionId;
    let mut res: HTSV_Result;

    res = HeapTupleSatisfiesVacuumHorizon(htup, buffer, &raw mut dead_after);

    if res == HEAPTUPLE_RECENTLY_DEAD {
        Assert!(TransactionIdIsValid(dead_after));

        if TransactionIdPrecedes(dead_after, OldestXmin) {
            res = HEAPTUPLE_DEAD;
        }
    } else {
        Assert!(!TransactionIdIsValid(dead_after));
    }

    res
}

/*
 * Work horse for HeapTupleSatisfiesVacuum and similar routines.
 *
 * In contrast to HeapTupleSatisfiesVacuum this routine, when encountering a
 * tuple that could still be visible to some backend, stores the xid that
 * needs to be compared with the horizon in *dead_after, and returns
 * HEAPTUPLE_RECENTLY_DEAD. The caller then can perform the comparison with
 * the horizon.  This is e.g. useful when comparing with different horizons.
 *
 * Note: HEAPTUPLE_DEAD can still be returned here, e.g. if the inserting
 * transaction aborted.
 */
pub unsafe fn HeapTupleSatisfiesVacuumHorizon(
    htup: HeapTuple,
    buffer: Buffer,
    dead_after: *mut TransactionId,
) -> HTSV_Result {
    let tuple: HeapTupleHeader = (*htup).t_data;

    Assert!(ItemPointerIsValid(&(*htup).t_self));
    Assert!((*htup).t_tableOid != InvalidOid);
    Assert!(!dead_after.is_null());

    *dead_after = InvalidTransactionId;

    /*
     * Has inserting transaction committed?
     *
     * If the inserting transaction aborted, then the tuple was never visible
     * to any other transaction, so we can delete it immediately.
     */
    if !HeapTupleHeaderXminCommitted(tuple) {
        if HeapTupleHeaderXminInvalid(tuple) {
            return HEAPTUPLE_DEAD;
        }
        /* Used by pre-9.0 binary upgrades */
        else if ((*tuple).t_infomask & HEAP_MOVED_OFF) != 0 {
            let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

            if TransactionIdIsCurrentTransactionId(xvac) {
                return HEAPTUPLE_DELETE_IN_PROGRESS;
            }
            if TransactionIdIsInProgress(xvac) {
                return HEAPTUPLE_DELETE_IN_PROGRESS;
            }
            if TransactionIdDidCommit(xvac) {
                SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                return HEAPTUPLE_DEAD;
            }
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
        }
        /* Used by pre-9.0 binary upgrades */
        else if ((*tuple).t_infomask & HEAP_MOVED_IN) != 0 {
            let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

            if TransactionIdIsCurrentTransactionId(xvac) {
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            }
            if TransactionIdIsInProgress(xvac) {
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            }
            if TransactionIdDidCommit(xvac) {
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, InvalidTransactionId);
            } else {
                SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
                return HEAPTUPLE_DEAD;
            }
        } else if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)) {
            if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
                /* xid invalid */
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            }
            /* only locked? run infomask-only check first, for performance */
            if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) || HeapTupleHeaderIsOnlyLocked(tuple) {
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            }
            /* inserted and then deleted by same xact */
            if TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(tuple)) {
                return HEAPTUPLE_DELETE_IN_PROGRESS;
            }
            /* deleting subtransaction must have aborted */
            return HEAPTUPLE_INSERT_IN_PROGRESS;
        } else if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmin(tuple)) {
            /*
             * It'd be possible to discern between INSERT/DELETE in progress
             * here by looking at xmax - but that doesn't seem beneficial for
             * the majority of callers and even detrimental for some. We'd
             * rather have callers look at/wait for xmin than xmax. It's
             * always correct to return INSERT_IN_PROGRESS because that's
             * what's happening from the view of other backends.
             */
            return HEAPTUPLE_INSERT_IN_PROGRESS;
        } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)) {
            SetHintBits(
                tuple,
                buffer,
                HEAP_XMIN_COMMITTED,
                HeapTupleHeaderGetRawXmin(tuple),
            );
        } else {
            /*
             * Not in Progress, Not Committed, so either Aborted or crashed
             */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return HEAPTUPLE_DEAD;
        }

        /*
         * At this point the xmin is known committed, but we might not have
         * been able to set the hint bit yet; so we can no longer Assert that
         * it's set.
         */
    }

    /*
     * Okay, the inserter committed, so it was good at some point.  Now what
     * about the deleting transaction?
     */
    if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
        return HEAPTUPLE_LIVE;
    }

    if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
        /*
         * "Deleting" xact really only locked it, so the tuple is live in any
         * case.  However, we should make sure that either XMAX_COMMITTED or
         * XMAX_INVALID gets set once the xact is gone, to reduce the costs of
         * examining the tuple for future xacts.
         */
        if ((*tuple).t_infomask & HEAP_XMAX_COMMITTED) == 0 {
            if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
                /*
                 * If it's a pre-pg_upgrade tuple, the multixact cannot
                 * possibly be running; otherwise have to check.
                 */
                if !HEAP_LOCKED_UPGRADED((*tuple).t_infomask)
                    && MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true)
                {
                    return HEAPTUPLE_LIVE;
                }
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            } else {
                if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)) {
                    return HEAPTUPLE_LIVE;
                }
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            }
        }

        /*
         * We don't really care whether xmax did commit, abort or crash. We
         * know that xmax did lock the tuple, but it did not and will never
         * actually update it.
         */

        return HEAPTUPLE_LIVE;
    }

    if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        let xmax: TransactionId = HeapTupleGetUpdateXid(tuple);

        /* already checked above */
        Assert!(!HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask));

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert!(TransactionIdIsValid(xmax));

        if TransactionIdIsInProgress(xmax) {
            return HEAPTUPLE_DELETE_IN_PROGRESS;
        } else if TransactionIdDidCommit(xmax) {
            /*
             * The multixact might still be running due to lockers.  Need to
             * allow for pruning if below the xid horizon regardless --
             * otherwise we could end up with a tuple where the updater has to
             * be removed due to the horizon, but is not pruned away.  It's
             * not a problem to prune that tuple, because any remaining
             * lockers will also be present in newer tuple versions.
             */
            *dead_after = xmax;
            return HEAPTUPLE_RECENTLY_DEAD;
        } else if !MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false) {
            /*
             * Not in Progress, Not Committed, so either Aborted or crashed.
             * Mark the Xmax as invalid.
             */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        }

        return HEAPTUPLE_LIVE;
    }

    if ((*tuple).t_infomask & HEAP_XMAX_COMMITTED) == 0 {
        if TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)) {
            return HEAPTUPLE_DELETE_IN_PROGRESS;
        } else if TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)) {
            SetHintBits(
                tuple,
                buffer,
                HEAP_XMAX_COMMITTED,
                HeapTupleHeaderGetRawXmax(tuple),
            );
        } else {
            /*
             * Not in Progress, Not Committed, so either Aborted or crashed
             */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return HEAPTUPLE_LIVE;
        }

        /*
         * At this point the xmax is known committed, but we might not have
         * been able to set the hint bit yet; so we can no longer Assert that
         * it's set.
         */
    }

    /*
     * Deleter committed, allow caller to check if it was recent enough that
     * some open transactions could still see the tuple.
     */
    *dead_after = HeapTupleHeaderGetRawXmax(tuple);
    HEAPTUPLE_RECENTLY_DEAD
}

/*
 * HeapTupleSatisfiesNonVacuumable
 *
 *	True if tuple might be visible to some transaction; false if it's
 *	surely dead to everyone, ie, vacuumable.
 *
 *	See SNAPSHOT_NON_VACUUMABLE's definition for the intended behaviour.
 *
 *	This is an interface to HeapTupleSatisfiesVacuum that's callable via
 *	HeapTupleSatisfiesSnapshot, so it can be used through a Snapshot.
 *	snapshot->vistest must have been set up with the horizon to use.
 */
unsafe fn HeapTupleSatisfiesNonVacuumable(
    htup: HeapTuple,
    snapshot: Snapshot,
    buffer: Buffer,
) -> bool {
    let mut dead_after: TransactionId = InvalidTransactionId;
    let mut res: HTSV_Result;

    res = HeapTupleSatisfiesVacuumHorizon(htup, buffer, &raw mut dead_after);

    if res == HEAPTUPLE_RECENTLY_DEAD {
        Assert!(TransactionIdIsValid(dead_after));

        if GlobalVisTestIsRemovableXid((*snapshot).vistest, dead_after) {
            res = HEAPTUPLE_DEAD;
        }
    } else {
        Assert!(!TransactionIdIsValid(dead_after));
    }

    res != HEAPTUPLE_DEAD
}

/*
 * HeapTupleIsSurelyDead
 *
 *	Cheaply determine whether a tuple is surely dead to all onlookers.
 *	We sometimes use this in lieu of HeapTupleSatisfiesVacuum when the
 *	tuple has just been tested by another visibility routine (usually
 *	HeapTupleSatisfiesMVCC) and, therefore, any hint bits that can be set
 *	should already be set.  We assume that if no hint bits are set, the xmin
 *	or xmax transaction is still running.  This is therefore faster than
 *	HeapTupleSatisfiesVacuum, because we consult neither procarray nor CLOG.
 *	It's okay to return false when in doubt, but we must return true only
 *	if the tuple is removable.
 */
pub unsafe fn HeapTupleIsSurelyDead(htup: HeapTuple, vistest: *mut GlobalVisState) -> bool {
    let tuple: HeapTupleHeader = (*htup).t_data;

    Assert!(ItemPointerIsValid(&(*htup).t_self));
    Assert!((*htup).t_tableOid != InvalidOid);

    /*
     * If the inserting transaction is marked invalid, then it aborted, and
     * the tuple is definitely dead.  If it's marked neither committed nor
     * invalid, then we assume it's still alive (since the presumption is that
     * all relevant hint bits were just set moments ago).
     */
    if !HeapTupleHeaderXminCommitted(tuple) {
        return HeapTupleHeaderXminInvalid(tuple);
    }

    /*
     * If the inserting transaction committed, but any deleting transaction
     * aborted, the tuple is still alive.
     */
    if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
        return false;
    }

    /*
     * If the XMAX is just a lock, the tuple is still alive.
     */
    if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
        return false;
    }

    /*
     * If the Xmax is a MultiXact, it might be dead or alive, but we cannot
     * know without checking pg_multixact.
     */
    if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        return false;
    }

    /* If deleter isn't known to have committed, assume it's still running. */
    if ((*tuple).t_infomask & HEAP_XMAX_COMMITTED) == 0 {
        return false;
    }

    /* Deleter committed, so tuple is dead if the XID is old enough. */
    GlobalVisTestIsRemovableXid(vistest, HeapTupleHeaderGetRawXmax(tuple))
}

/*
 * Is the tuple really only locked?  That is, is it not updated?
 *
 * It's easy to check just infomask bits if the locker is not a multi; but
 * otherwise we need to verify that the updating transaction has not aborted.
 *
 * This function is here because it follows the same visibility rules laid out
 * at the top of this file.
 */
pub unsafe fn HeapTupleHeaderIsOnlyLocked(tuple: HeapTupleHeader) -> bool {
    let xmax: TransactionId;

    /* if there's no valid Xmax, then there's obviously no update either */
    if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
        return true;
    }

    if ((*tuple).t_infomask & HEAP_XMAX_LOCK_ONLY) != 0 {
        return true;
    }

    /* invalid xmax means no update */
    if !TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)) {
        return true;
    }

    /*
     * if HEAP_XMAX_LOCK_ONLY is not set and not a multi, then this must
     * necessarily have been updated
     */
    if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) == 0 {
        return false;
    }

    /* ... but if it's a multi, then perhaps the updating Xid aborted. */
    xmax = HeapTupleGetUpdateXid(tuple);

    /* not LOCKED_ONLY, so it has to have an xmax */
    Assert!(TransactionIdIsValid(xmax));

    if TransactionIdIsCurrentTransactionId(xmax) {
        return false;
    }
    if TransactionIdIsInProgress(xmax) {
        return false;
    }
    if TransactionIdDidCommit(xmax) {
        return false;
    }

    /*
     * not current, not in progress, not committed -- must have aborted or
     * crashed
     */
    true
}

extern "C" {
    fn bsearch(
        key: *const c_void,
        base: *const c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    ) -> *const c_void;
}

/*
 * C-ABI shim around xidComparator (which uses the Rust ABI) so that it can be
 * handed to bsearch().
 */
unsafe extern "C" fn xidComparator_c(arg1: *const c_void, arg2: *const c_void) -> c_int {
    xidComparator(arg1, arg2)
}

/*
 * check whether the transaction id 'xid' is in the pre-sorted array 'xip'.
 */
unsafe fn TransactionIdInArray(xid: TransactionId, xip: *mut TransactionId, num: Size) -> bool {
    num > 0
        && !bsearch(
            &xid as *const TransactionId as *const c_void,
            xip as *const c_void,
            num as usize,
            std::mem::size_of::<TransactionId>(),
            xidComparator_c,
        )
        .is_null()
}

/*
 * See the comments for HeapTupleSatisfiesMVCC for the semantics this function
 * obeys.
 *
 * Only usable on tuples from catalog tables!
 *
 * We don't need to support HEAP_MOVED_(IN|OFF) for now because we only support
 * reading catalog pages which couldn't have been created in an older version.
 *
 * We don't set any hint bits in here as it seems unlikely to be beneficial as
 * those should already be set by normal access and it seems to be too
 * dangerous to do so as the semantics of doing so during timetravel are more
 * complicated than when dealing "only" with the present.
 */
unsafe fn HeapTupleSatisfiesHistoricMVCC(htup: HeapTuple, snapshot: Snapshot, buffer: Buffer) -> bool {
    let tuple: HeapTupleHeader = (*htup).t_data;
    let xmin: TransactionId = HeapTupleHeaderGetXmin(tuple);
    let mut xmax: TransactionId = HeapTupleHeaderGetRawXmax(tuple);

    Assert!(ItemPointerIsValid(&(*htup).t_self));
    Assert!((*htup).t_tableOid != InvalidOid);

    /* inserting transaction aborted */
    if HeapTupleHeaderXminInvalid(tuple) {
        Assert!(!TransactionIdDidCommit(xmin));
        return false;
    }
    /* check if it's one of our txids, toplevel is also in there */
    else if TransactionIdInArray(xmin, (*snapshot).subxip, (*snapshot).subxcnt as Size) {
        let resolved: bool;
        let mut cmin: CommandId = HeapTupleHeaderGetRawCommandId(tuple);
        let mut cmax: CommandId = InvalidCommandId;

        /*
         * another transaction might have (tried to) delete this tuple or
         * cmin/cmax was stored in a combo CID. So we need to lookup the
         * actual values externally.
         */
        resolved = ResolveCminCmaxDuringDecoding(
            HistoricSnapshotGetTupleCids(),
            snapshot,
            htup,
            buffer,
            &raw mut cmin,
            &raw mut cmax,
        );

        /*
         * If we haven't resolved the combo CID to cmin/cmax, that means we
         * have not decoded the combo CID yet. That means the cmin is
         * definitely in the future, and we're not supposed to see the tuple
         * yet.
         *
         * XXX This only applies to decoding of in-progress transactions. In
         * regular logical decoding we only execute this code at commit time,
         * at which point we should have seen all relevant combo CIDs. So
         * ideally, we should error out in this case but in practice, this
         * won't happen. If we are too worried about this then we can add an
         * elog inside ResolveCminCmaxDuringDecoding.
         *
         * XXX For the streaming case, we can track the largest combo CID
         * assigned, and error out based on this (when unable to resolve combo
         * CID below that observed maximum value).
         */
        if !resolved {
            return false;
        }

        Assert!(cmin != InvalidCommandId);

        if cmin >= (*snapshot).curcid {
            return false; /* inserted after scan started */
        }
        /* fall through */
    }
    /* committed before our xmin horizon. Do a normal visibility check. */
    else if TransactionIdPrecedes(xmin, (*snapshot).xmin) {
        Assert!(!(HeapTupleHeaderXminCommitted(tuple) && !TransactionIdDidCommit(xmin)));

        /* check for hint bit first, consult clog afterwards */
        if !HeapTupleHeaderXminCommitted(tuple) && !TransactionIdDidCommit(xmin) {
            return false;
        }
        /* fall through */
    }
    /* beyond our xmax horizon, i.e. invisible */
    else if TransactionIdFollowsOrEquals(xmin, (*snapshot).xmax) {
        return false;
    }
    /* check if it's a committed transaction in [xmin, xmax) */
    else if TransactionIdInArray(xmin, (*snapshot).xip, (*snapshot).xcnt as Size) {
        /* fall through */
    }
    /*
     * none of the above, i.e. between [xmin, xmax) but hasn't committed. I.e.
     * invisible.
     */
    else {
        return false;
    }

    /* at this point we know xmin is visible, go on to check xmax */

    /* xid invalid or aborted */
    if ((*tuple).t_infomask & HEAP_XMAX_INVALID) != 0 {
        return true;
    }
    /* locked tuples are always visible */
    else if HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
        return true;
    }
    /*
     * We can see multis here if we're looking at user tables or if somebody
     * SELECT ... FOR SHARE/UPDATE a system table.
     */
    else if ((*tuple).t_infomask & HEAP_XMAX_IS_MULTI) != 0 {
        xmax = HeapTupleGetUpdateXid(tuple);
    }

    /* check if it's one of our txids, toplevel is also in there */
    if TransactionIdInArray(xmax, (*snapshot).subxip, (*snapshot).subxcnt as Size) {
        let resolved: bool;
        let mut cmin: CommandId = InvalidCommandId;
        let mut cmax: CommandId = HeapTupleHeaderGetRawCommandId(tuple);

        /* Lookup actual cmin/cmax values */
        resolved = ResolveCminCmaxDuringDecoding(
            HistoricSnapshotGetTupleCids(),
            snapshot,
            htup,
            buffer,
            &raw mut cmin,
            &raw mut cmax,
        );

        /*
         * If we haven't resolved the combo CID to cmin/cmax, that means we
         * have not decoded the combo CID yet. That means the cmax is
         * definitely in the future, and we're still supposed to see the
         * tuple.
         *
         * XXX This only applies to decoding of in-progress transactions. In
         * regular logical decoding we only execute this code at commit time,
         * at which point we should have seen all relevant combo CIDs. So
         * ideally, we should error out in this case but in practice, this
         * won't happen. If we are too worried about this then we can add an
         * elog inside ResolveCminCmaxDuringDecoding.
         *
         * XXX For the streaming case, we can track the largest combo CID
         * assigned, and error out based on this (when unable to resolve combo
         * CID below that observed maximum value).
         */
        if !resolved || cmax == InvalidCommandId {
            return true;
        }

        if cmax >= (*snapshot).curcid {
            return true; /* deleted after scan started */
        } else {
            return false; /* deleted before scan started */
        }
    }
    /* below xmin horizon, normal transaction state is valid */
    else if TransactionIdPrecedes(xmax, (*snapshot).xmin) {
        Assert!(!(((*tuple).t_infomask & HEAP_XMAX_COMMITTED) != 0 && !TransactionIdDidCommit(xmax)));

        /* check hint bit first */
        if ((*tuple).t_infomask & HEAP_XMAX_COMMITTED) != 0 {
            return false;
        }

        /* check clog */
        return !TransactionIdDidCommit(xmax);
    }
    /* above xmax horizon, we cannot possibly see the deleting transaction */
    else if TransactionIdFollowsOrEquals(xmax, (*snapshot).xmax) {
        true
    }
    /* xmax is between [xmin, xmax), check known committed array */
    else if TransactionIdInArray(xmax, (*snapshot).xip, (*snapshot).xcnt as Size) {
        false
    }
    /* xmax is between [xmin, xmax), but known not to have committed yet */
    else {
        true
    }
}

/*
 * HeapTupleSatisfiesVisibility
 *		True iff heap tuple satisfies a time qual.
 *
 * Notes:
 *	Assumes heap tuple is valid, and buffer at least share locked.
 *
 *	Hint bits in the HeapTuple's t_infomask may be updated as a side effect;
 *	if so, the indicated buffer is marked dirty.
 */
pub unsafe fn HeapTupleSatisfiesVisibility(htup: HeapTuple, snapshot: Snapshot, buffer: Buffer) -> bool {
    match (*snapshot).snapshot_type {
        SNAPSHOT_MVCC => HeapTupleSatisfiesMVCC(htup, snapshot, buffer),
        SNAPSHOT_SELF => HeapTupleSatisfiesSelf(htup, snapshot, buffer),
        SNAPSHOT_ANY => HeapTupleSatisfiesAny(htup, snapshot, buffer),
        SNAPSHOT_TOAST => HeapTupleSatisfiesToast(htup, snapshot, buffer),
        SNAPSHOT_DIRTY => HeapTupleSatisfiesDirty(htup, snapshot, buffer),
        SNAPSHOT_HISTORIC_MVCC => HeapTupleSatisfiesHistoricMVCC(htup, snapshot, buffer),
        SNAPSHOT_NON_VACUUMABLE => HeapTupleSatisfiesNonVacuumable(htup, snapshot, buffer),
        _ => false, /* keep compiler quiet */
    }
}
