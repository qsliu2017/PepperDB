//! storage/predicate_internals.h - POSTGRES internal predicate locking definitions.

use std::ffi::{c_int, c_void};

use crate::c::{uint32, uint64, Size, TransactionId, MAXALIGN, PG_UINT64_MAX};
use crate::lib::ilist::{dlist_head, dlist_node};
use crate::postgres_ext::Oid;
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::storage::off::{InvalidOffsetNumber, OffsetNumber};

// storage/lock.h + storage/lwlock.h not yet ported; minimal local stubs.
// TODO: dedup when those headers land.
pub type VirtualTransactionId = c_void;
pub type LWLock = c_void;

/*
 * Commit number.
 */
pub type SerCommitSeqNo = uint64;

/*
 * Reserved commit sequence numbers:
 *	- 0 is reserved to indicate a non-existent SLRU entry; it cannot be
 *	  used as a SerCommitSeqNo, even an invalid one
 *	- InvalidSerCommitSeqNo is used to indicate a transaction that
 *	  hasn't committed yet, so use a number greater than all valid
 *	  ones to make comparison do the expected thing
 *	- RecoverySerCommitSeqNo is used to refer to transactions that
 *	  happened before a crash/recovery, since we restart the sequence
 *	  at that point.  It's earlier than all normal sequence numbers,
 *	  and is only used by recovered prepared transactions
 */
pub const InvalidSerCommitSeqNo: SerCommitSeqNo = PG_UINT64_MAX as SerCommitSeqNo;
pub const RecoverySerCommitSeqNo: SerCommitSeqNo = 1;
pub const FirstNormalSerCommitSeqNo: SerCommitSeqNo = 2;

/*
 * The "two numbers are not both interesting at the same time" union of
 * SERIALIZABLEXACT.
 */
#[repr(C)]
pub union SERIALIZABLEXACT_SeqNo {
    /// when committed with conflict out
    pub earliestOutConflictCommit: SerCommitSeqNo,
    /// when not committed or no conflict out
    pub lastCommitBeforeSnapshot: SerCommitSeqNo,
}

/*
 * The SERIALIZABLEXACT struct contains information needed for each
 * serializable database transaction to support SSI techniques.
 */
#[repr(C)]
pub struct SERIALIZABLEXACT {
    /// The executing process always has one of these.
    pub vxid: VirtualTransactionId,

    pub prepareSeqNo: SerCommitSeqNo,
    pub commitSeqNo: SerCommitSeqNo,

    /// these values are not both interesting at the same time
    pub SeqNo: SERIALIZABLEXACT_SeqNo,
    /// list of write transactions whose data we couldn't read.
    pub outConflicts: dlist_head,
    /// list of read transactions which couldn't see our write.
    pub inConflicts: dlist_head,
    /// list of associated PREDICATELOCK objects
    pub predicateLocks: dlist_head,
    /// list link in FinishedSerializableTransactions
    pub finishedLink: dlist_node,
    /// PredXact->activeList/availableList
    pub xactLink: dlist_node,

    /*
     * perXactPredicateListLock is only used in parallel queries: it protects
     * this SERIALIZABLEXACT's predicate lock list against other workers of
     * the same session.
     */
    pub perXactPredicateListLock: LWLock,

    /*
     * for r/o transactions: list of concurrent r/w transactions that we could
     * potentially have conflicts with, and vice versa for r/w transactions
     */
    pub possibleUnsafeConflicts: dlist_head,

    /// top level xid for the transaction, if one exists; else invalid
    pub topXid: TransactionId,
    /// invalid means still running; else the struct expires when no
    /// serializable xids are before this.
    pub finishedBefore: TransactionId,
    /// the transaction's snapshot xmin
    pub xmin: TransactionId,
    /// OR'd combination of values defined below
    pub flags: uint32,
    /// pid of associated process
    pub pid: c_int,
    /// pgprocno of associated process
    pub pgprocno: c_int,
}

pub const SXACT_FLAG_COMMITTED: u32 = 0x00000001; /* already committed */
pub const SXACT_FLAG_PREPARED: u32 = 0x00000002; /* about to commit */
pub const SXACT_FLAG_ROLLED_BACK: u32 = 0x00000004; /* already rolled back */
pub const SXACT_FLAG_DOOMED: u32 = 0x00000008; /* will roll back */
/*
 * The following flag actually means that the flagged transaction has a
 * conflict out *to a transaction which committed ahead of it*.  It's hard
 * to get that into a name of a reasonable length.
 */
pub const SXACT_FLAG_CONFLICT_OUT: u32 = 0x00000010;
pub const SXACT_FLAG_READ_ONLY: u32 = 0x00000020;
pub const SXACT_FLAG_DEFERRABLE_WAITING: u32 = 0x00000040;
pub const SXACT_FLAG_RO_SAFE: u32 = 0x00000080;
pub const SXACT_FLAG_RO_UNSAFE: u32 = 0x00000100;
pub const SXACT_FLAG_SUMMARY_CONFLICT_IN: u32 = 0x00000200;
pub const SXACT_FLAG_SUMMARY_CONFLICT_OUT: u32 = 0x00000400;
/*
 * The following flag means the transaction has been partially released
 * already, but is being preserved because parallel workers might have a
 * reference to it.  It'll be recycled by the leader at end-of-transaction.
 */
pub const SXACT_FLAG_PARTIALLY_RELEASED: u32 = 0x00000800;

#[repr(C)]
pub struct PredXactListData {
    pub availableList: dlist_head,
    pub activeList: dlist_head,

    /*
     * These global variables are maintained when registering and cleaning up
     * serializable transactions.  They must be global across all backends,
     * but are not needed outside the predicate.c source file. Protected by
     * SerializableXactHashLock.
     */
    /// global xmin for active serializable transactions
    pub SxactGlobalXmin: TransactionId,
    /// how many active serializable transactions have this xmin
    pub SxactGlobalXminCount: c_int,
    /// how many non-read-only serializable transactions are active
    pub WritableSxactCount: c_int,
    /// a strictly monotonically increasing number for commits of
    /// serializable transactions
    pub LastSxactCommitSeqNo: SerCommitSeqNo,
    /* Protected by SerializableXactHashLock. */
    /// can clear predicate locks and inConflicts for committed transactions
    /// through this seq no
    pub CanPartialClearThrough: SerCommitSeqNo,
    /* Protected by SerializableFinishedListLock. */
    /// have cleared through this seq no
    pub HavePartialClearedThrough: SerCommitSeqNo,
    /// shared copy of dummy sxact
    pub OldCommittedSxact: *mut SERIALIZABLEXACT,

    pub element: *mut SERIALIZABLEXACT,
}

pub type PredXactList = *mut PredXactListData;

#[inline]
pub fn PredXactListDataSize() -> Size {
    MAXALIGN(std::mem::size_of::<PredXactListData>()) as Size
}

/*
 * The following types are used to provide lists of rw-conflicts between
 * pairs of transactions.
 */
#[repr(C)]
pub struct RWConflictData {
    /// link for list of conflicts out from a sxact
    pub outLink: dlist_node,
    /// link for list of conflicts in to a sxact
    pub inLink: dlist_node,
    pub sxactOut: *mut SERIALIZABLEXACT,
    pub sxactIn: *mut SERIALIZABLEXACT,
}

pub type RWConflict = *mut RWConflictData;

#[inline]
pub fn RWConflictDataSize() -> Size {
    MAXALIGN(std::mem::size_of::<RWConflictData>()) as Size
}

#[repr(C)]
pub struct RWConflictPoolHeaderData {
    pub availableList: dlist_head,
    pub element: RWConflict,
}

pub type RWConflictPoolHeader = *mut RWConflictPoolHeaderData;

#[inline]
pub fn RWConflictPoolHeaderDataSize() -> Size {
    MAXALIGN(std::mem::size_of::<RWConflictPoolHeaderData>()) as Size
}

/*
 * The SERIALIZABLEXIDTAG struct identifies an xid assigned to a serializable
 * transaction or any of its subtransactions.
 */
#[repr(C)]
pub struct SERIALIZABLEXIDTAG {
    pub xid: TransactionId,
}

/*
 * The SERIALIZABLEXID struct provides a link from a TransactionId for a
 * serializable transaction to the related SERIALIZABLEXACT record, even if
 * the transaction has completed and its connection has been closed.
 */
#[repr(C)]
pub struct SERIALIZABLEXID {
    /* hash key */
    pub tag: SERIALIZABLEXIDTAG,

    /* data */
    /// pointer to the top level transaction data
    pub myXact: *mut SERIALIZABLEXACT,
}

/*
 * The PREDICATELOCKTARGETTAG struct identifies a database object which can
 * be the target of predicate locks.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PREDICATELOCKTARGETTAG {
    /// a 32-bit ID field
    pub locktag_field1: uint32,
    /// a 32-bit ID field
    pub locktag_field2: uint32,
    /// a 32-bit ID field
    pub locktag_field3: uint32,
    /// a 32-bit ID field
    pub locktag_field4: uint32,
}

/*
 * The PREDICATELOCKTARGET struct represents a database object on which there
 * are predicate locks.
 */
#[repr(C)]
pub struct PREDICATELOCKTARGET {
    /* hash key */
    /// unique identifier of lockable object
    pub tag: PREDICATELOCKTARGETTAG,

    /* data */
    /// list of PREDICATELOCK objects assoc. with predicate lock target
    pub predicateLocks: dlist_head,
}

/*
 * The PREDICATELOCKTAG struct identifies an individual predicate lock.
 */
#[repr(C)]
pub struct PREDICATELOCKTAG {
    pub myTarget: *mut PREDICATELOCKTARGET,
    pub myXact: *mut SERIALIZABLEXACT,
}

/*
 * The PREDICATELOCK struct represents an individual lock.
 */
#[repr(C)]
pub struct PREDICATELOCK {
    /* hash key */
    /// unique identifier of lock
    pub tag: PREDICATELOCKTAG,

    /* data */
    /// list link in PREDICATELOCKTARGET's list of predicate locks
    pub targetLink: dlist_node,
    /// list link in SERIALIZABLEXACT's list of predicate locks
    pub xactLink: dlist_node,
    /// only used for summarized predicate locks
    pub commitSeqNo: SerCommitSeqNo,
}

/*
 * The LOCALPREDICATELOCK struct represents a local copy of data which is
 * also present in the PREDICATELOCK table, organized for fast access without
 * needing to acquire a LWLock.  It is strictly for optimization.
 */
#[repr(C)]
pub struct LOCALPREDICATELOCK {
    /* hash key */
    /// unique identifier of lockable object
    pub tag: PREDICATELOCKTARGETTAG,

    /* data */
    /// is lock held, or just its children?
    pub held: bool,
    /// number of child locks currently held
    pub childLocks: c_int,
}

/*
 * The types of predicate locks which can be acquired.
 */
pub type PredicateLockTargetType = c_int;
pub const PREDLOCKTAG_RELATION: PredicateLockTargetType = 0;
pub const PREDLOCKTAG_PAGE: PredicateLockTargetType = 1;
pub const PREDLOCKTAG_TUPLE: PredicateLockTargetType = 2;
/* TODO SSI: Other types may be needed for index locking */

/*
 * This structure is used to quickly capture a copy of all predicate
 * locks.  This is currently used only by the pg_lock_status function,
 * which in turn is used by the pg_locks view.
 */
#[repr(C)]
pub struct PredicateLockData {
    pub nelements: c_int,
    pub locktags: *mut PREDICATELOCKTARGETTAG,
    pub xacts: *mut SERIALIZABLEXACT,
}

/*
 * These macros define how we map logical IDs of lockable objects into the
 * physical fields of PREDICATELOCKTARGETTAG.   Use these to set up values,
 * rather than accessing the fields directly.
 */
#[inline]
pub fn SET_PREDICATELOCKTARGETTAG_RELATION(
    locktag: &mut PREDICATELOCKTARGETTAG,
    dboid: uint32,
    reloid: uint32,
) {
    locktag.locktag_field1 = dboid;
    locktag.locktag_field2 = reloid;
    locktag.locktag_field3 = InvalidBlockNumber;
    locktag.locktag_field4 = InvalidOffsetNumber as uint32;
}

#[inline]
pub fn SET_PREDICATELOCKTARGETTAG_PAGE(
    locktag: &mut PREDICATELOCKTARGETTAG,
    dboid: uint32,
    reloid: uint32,
    blocknum: BlockNumber,
) {
    locktag.locktag_field1 = dboid;
    locktag.locktag_field2 = reloid;
    locktag.locktag_field3 = blocknum;
    locktag.locktag_field4 = InvalidOffsetNumber as uint32;
}

#[inline]
pub fn SET_PREDICATELOCKTARGETTAG_TUPLE(
    locktag: &mut PREDICATELOCKTARGETTAG,
    dboid: uint32,
    reloid: uint32,
    blocknum: BlockNumber,
    offnum: OffsetNumber,
) {
    locktag.locktag_field1 = dboid;
    locktag.locktag_field2 = reloid;
    locktag.locktag_field3 = blocknum;
    locktag.locktag_field4 = offnum as uint32;
}

#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_DB(locktag: &PREDICATELOCKTARGETTAG) -> Oid {
    locktag.locktag_field1 as Oid
}

#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_RELATION(locktag: &PREDICATELOCKTARGETTAG) -> Oid {
    locktag.locktag_field2 as Oid
}

#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_PAGE(locktag: &PREDICATELOCKTARGETTAG) -> BlockNumber {
    locktag.locktag_field3 as BlockNumber
}

#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_OFFSET(locktag: &PREDICATELOCKTARGETTAG) -> OffsetNumber {
    locktag.locktag_field4 as OffsetNumber
}

#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_TYPE(
    locktag: &PREDICATELOCKTARGETTAG,
) -> PredicateLockTargetType {
    if locktag.locktag_field4 != InvalidOffsetNumber as uint32 {
        PREDLOCKTAG_TUPLE
    } else if locktag.locktag_field3 != InvalidBlockNumber {
        PREDLOCKTAG_PAGE
    } else {
        PREDLOCKTAG_RELATION
    }
}

/*
 * Two-phase commit statefile records. There are two types: for each
 * transaction, we generate one per-transaction record and a variable
 * number of per-predicate-lock records.
 */
pub type TwoPhasePredicateRecordType = c_int;
pub const TWOPHASEPREDICATERECORD_XACT: TwoPhasePredicateRecordType = 0;
pub const TWOPHASEPREDICATERECORD_LOCK: TwoPhasePredicateRecordType = 1;

/*
 * Per-transaction information to reconstruct a SERIALIZABLEXACT.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TwoPhasePredicateXactRecord {
    pub xmin: TransactionId,
    pub flags: uint32,
}

/* Per-lock state */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TwoPhasePredicateLockRecord {
    pub target: PREDICATELOCKTARGETTAG,
    /// to avoid length change in back-patched fix
    pub filler: uint32,
}

#[repr(C)]
pub union TwoPhasePredicateRecord_data {
    pub xactRecord: TwoPhasePredicateXactRecord,
    pub lockRecord: TwoPhasePredicateLockRecord,
}

#[repr(C)]
pub struct TwoPhasePredicateRecord {
    pub r#type: TwoPhasePredicateRecordType,
    pub data: TwoPhasePredicateRecord_data,
}

/*
 * Define a macro to use for an "empty" SERIALIZABLEXACT reference.
 */
#[inline]
pub fn InvalidSerializableXact() -> *mut SERIALIZABLEXACT {
    std::ptr::null_mut()
}

/*
 * Function definitions for functions needing awareness of predicate
 * locking internals.
 */
pub unsafe fn GetPredicateLockStatusData() -> *mut PredicateLockData {
    crate::storage::lmgr::predicate::GetPredicateLockStatusData()
}

pub unsafe fn GetSafeSnapshotBlockingPids(
    blocked_pid: c_int,
    output: *mut c_int,
    output_size: c_int,
) -> c_int {
    crate::storage::lmgr::predicate::GetSafeSnapshotBlockingPids(blocked_pid, output, output_size)
}
