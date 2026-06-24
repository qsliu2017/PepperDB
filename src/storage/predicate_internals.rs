//! Translated from PostgreSQL src/include/storage/predicate_internals.h
//!
//! Internal predicate-locking (SSI) definitions. In-memory; the shmem home-grown
//! lists collapse under the single-process model. Intrusive dlist links
//! (`dlist_head`/`dlist_node`) are dropped in favor of owned Vec collections; the
//! `LWLock perXactPredicateListLock` is tombstoned (parking_lot at the owning
//! site). `SXACT_FLAG_*` -> bitflags. Hash tables (`HTAB`) -> HashMap.

use crate::c::TransactionId;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::lock::VirtualTransactionId;
use crate::storage::off::OffsetNumber;

/// `SerCommitSeqNo` - commit sequence number.
pub type SerCommitSeqNo = u64;

/// 0 is reserved (non-existent SLRU entry). `InvalidSerCommitSeqNo` marks a
/// transaction that hasn't committed yet (greater than all valid ones).
pub const INVALID_SER_COMMIT_SEQ_NO: SerCommitSeqNo = u64::MAX;
pub const RECOVERY_SER_COMMIT_SEQ_NO: SerCommitSeqNo = 1;
pub const FIRST_NORMAL_SER_COMMIT_SEQ_NO: SerCommitSeqNo = 2;

/// `SXACT_FLAG_*` - flags on a SERIALIZABLEXACT. Clean single-bit set
/// (bitflags-port.md appendix A -> GOOD).
use bitflags::bitflags;
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SxactFlag: u32 {
        const COMMITTED            = 0x00000001;
        const PREPARED             = 0x00000002;
        const ROLLED_BACK          = 0x00000004;
        const DOOMED               = 0x00000008;
        /// Has a conflict out to a transaction that committed ahead of it.
        const CONFLICT_OUT         = 0x00000010;
        const READ_ONLY            = 0x00000020;
        const DEFERRABLE_WAITING   = 0x00000040;
        const RO_SAFE              = 0x00000080;
        const RO_UNSAFE            = 0x00000100;
        const SUMMARY_CONFLICT_IN  = 0x00000200;
        const SUMMARY_CONFLICT_OUT = 0x00000400;
        const PARTIALLY_RELEASED   = 0x00000800;
    }
}

/// Either-or seqno member of SERIALIZABLEXACT (C union; only one is meaningful
/// at a time).
pub enum SerXactSeqNo {
    /// When committed with conflict out.
    EarliestOutConflictCommit(SerCommitSeqNo),
    /// When not committed or no conflict out.
    LastCommitBeforeSnapshot(SerCommitSeqNo),
}

/// `SERIALIZABLEXACT` - per serializable transaction SSI state. Intrusive dlist
/// links -> owned Vec; perXactPredicateListLock tombstoned.
pub struct SerializableXact {
    pub vxid: VirtualTransactionId,
    pub prepare_seq_no: SerCommitSeqNo,
    pub commit_seq_no: SerCommitSeqNo,
    pub seq_no: SerXactSeqNo,
    /// Write transactions whose data we couldn't read (was dlist_head).
    pub out_conflicts: Vec<RWConflictData>,
    /// Read transactions which couldn't see our write (was dlist_head).
    pub in_conflicts: Vec<RWConflictData>,
    /// Associated PREDICATELOCK objects (was dlist_head).
    pub predicate_locks: Vec<PredicateLock>,
    // finishedLink / xactLink: intrusive list links -> dropped (membership held
    // by the owning PredXactList collections).
    // perXactPredicateListLock: LWLock -> tombstoned (parking_lot at owner).
    /// r/o: concurrent r/w transactions we could conflict with, and vice versa.
    pub possible_unsafe_conflicts: Vec<*mut SerializableXact>, // TODO(ptr)
    /// Top level xid, or invalid.
    pub top_xid: TransactionId,
    /// Invalid means still running.
    pub finished_before: TransactionId,
    /// The transaction's snapshot xmin.
    pub xmin: TransactionId,
    pub flags: SxactFlag,
    pub pid: i32,
    pub pgprocno: i32,
}

/// `PredXactListData` - the list of serializable transactions plus globals.
/// availableList/activeList (dlist) -> Vec; the `element` FAM base -> Vec.
pub struct PredXactListData {
    pub available_list: Vec<SerializableXact>,
    pub active_list: Vec<SerializableXact>,
    /// Global xmin for active serializable transactions.
    pub sxact_global_xmin: TransactionId,
    pub sxact_global_xmin_count: i32,
    pub writable_sxact_count: i32,
    pub last_sxact_commit_seq_no: SerCommitSeqNo,
    pub can_partial_clear_through: SerCommitSeqNo,
    pub have_partial_cleared_through: SerCommitSeqNo,
    /// Shared copy of dummy sxact.
    pub old_committed_sxact: *mut SerializableXact, // TODO(ptr)
}

/// `PredXactList` (C: pointer to PredXactListData).
pub type PredXactList = *mut PredXactListData; // TODO(ptr)

/// `RWConflictData` - a rw-conflict (or possible-unsafe relationship) between a
/// pair of transactions. Intrusive links dropped (membership in owner Vecs).
pub struct RWConflictData {
    pub sxact_out: *mut SerializableXact, // TODO(ptr)
    pub sxact_in: *mut SerializableXact,  // TODO(ptr)
}

/// `RWConflict` (C: pointer to RWConflictData).
pub type RWConflict = *mut RWConflictData; // TODO(ptr)

/// `RWConflictPoolHeaderData` - pool of available RWConflictData. availableList
/// (dlist) -> Vec; `element` FAM base -> Vec.
pub struct RWConflictPoolHeaderData {
    pub available_list: Vec<RWConflictData>,
}

pub type RWConflictPoolHeader = *mut RWConflictPoolHeaderData; // TODO(ptr)

/// `SERIALIZABLEXIDTAG` - hash key identifying an xid of a serializable xact.
pub struct SerializableXidTag {
    pub xid: TransactionId,
}

/// `SERIALIZABLEXID` - links a TransactionId to its SERIALIZABLEXACT.
pub struct SerializableXid {
    pub tag: SerializableXidTag,
    pub my_xact: *mut SerializableXact, // TODO(ptr)
}

/// `PREDICATELOCKTARGETTAG` - identifies a lockable database object. On-disk-ish
/// hash key (four 32-bit ID fields), kept as a plain struct.
pub struct PredicateLockTargetTag {
    pub locktag_field1: u32,
    pub locktag_field2: u32,
    pub locktag_field3: u32,
    pub locktag_field4: u32,
}

/// `PREDICATELOCKTARGET` - an object with predicate locks. predicateLocks
/// (dlist) -> Vec.
pub struct PredicateLockTarget {
    pub tag: PredicateLockTargetTag,
    pub predicate_locks: Vec<PredicateLock>,
}

/// `PREDICATELOCKTAG` - identifies an individual predicate lock.
pub struct PredicateLockTag {
    pub my_target: *mut PredicateLockTarget, // TODO(ptr)
    pub my_xact: *mut SerializableXact,      // TODO(ptr)
}

/// `PREDICATELOCK` - an individual lock. Intrusive links dropped.
pub struct PredicateLock {
    pub tag: PredicateLockTag,
    /// Only used for summarized predicate locks.
    pub commit_seq_no: SerCommitSeqNo,
}

/// `LOCALPREDICATELOCK` - per-transaction local copy for fast access.
pub struct LocalPredicateLock {
    pub tag: PredicateLockTargetTag,
    /// Is lock held, or just its children?
    pub held: bool,
    /// Number of child locks currently held.
    pub child_locks: i32,
}

/// `PredicateLockTargetType` - kinds of predicate locks. Sequential ordinal.
#[repr(i32)]
pub enum PredicateLockTargetType {
    Relation,
    Page,
    Tuple,
}

/// `PredicateLockData` - snapshot of all predicate locks (for pg_locks).
pub struct PredicateLockData {
    pub nelements: i32,
    pub locktags: Vec<PredicateLockTargetTag>,
    pub xacts: Vec<SerializableXact>,
}

// --- PREDICATELOCKTARGETTAG field setters/getters (macros -> functions) ---

pub fn set_predicatelocktargettag_relation(
    locktag: &mut PredicateLockTargetTag,
    dboid: Oid,
    reloid: Oid,
) {
    locktag.locktag_field1 = dboid.0;
    locktag.locktag_field2 = reloid.0;
    locktag.locktag_field3 = crate::storage::block::INVALID_BLOCK_NUMBER;
    locktag.locktag_field4 = crate::storage::off::INVALID_OFFSET_NUMBER as u32;
}

pub fn set_predicatelocktargettag_page(
    locktag: &mut PredicateLockTargetTag,
    dboid: Oid,
    reloid: Oid,
    blocknum: BlockNumber,
) {
    locktag.locktag_field1 = dboid.0;
    locktag.locktag_field2 = reloid.0;
    locktag.locktag_field3 = blocknum;
    locktag.locktag_field4 = crate::storage::off::INVALID_OFFSET_NUMBER as u32;
}

pub fn set_predicatelocktargettag_tuple(
    locktag: &mut PredicateLockTargetTag,
    dboid: Oid,
    reloid: Oid,
    blocknum: BlockNumber,
    offnum: OffsetNumber,
) {
    locktag.locktag_field1 = dboid.0;
    locktag.locktag_field2 = reloid.0;
    locktag.locktag_field3 = blocknum;
    locktag.locktag_field4 = offnum as u32;
}

pub fn get_predicatelocktargettag_db(locktag: &PredicateLockTargetTag) -> Oid {
    Oid(locktag.locktag_field1)
}
pub fn get_predicatelocktargettag_relation(locktag: &PredicateLockTargetTag) -> Oid {
    Oid(locktag.locktag_field2)
}
pub fn get_predicatelocktargettag_page(locktag: &PredicateLockTargetTag) -> BlockNumber {
    locktag.locktag_field3
}
pub fn get_predicatelocktargettag_offset(locktag: &PredicateLockTargetTag) -> OffsetNumber {
    locktag.locktag_field4 as OffsetNumber
}
pub fn get_predicatelocktargettag_type(
    locktag: &PredicateLockTargetTag,
) -> PredicateLockTargetType {
    if locktag.locktag_field4 != crate::storage::off::INVALID_OFFSET_NUMBER as u32 {
        PredicateLockTargetType::Tuple
    } else if locktag.locktag_field3 != crate::storage::block::INVALID_BLOCK_NUMBER {
        PredicateLockTargetType::Page
    } else {
        PredicateLockTargetType::Relation
    }
}

/// `TwoPhasePredicateRecordType` - 2PC statefile record kind. Sequential ordinal.
#[repr(i32)]
pub enum TwoPhasePredicateRecordType {
    Xact,
    Lock,
}

/// `TwoPhasePredicateXactRecord` - per-transaction 2PC reconstruction info.
pub struct TwoPhasePredicateXactRecord {
    pub xmin: TransactionId,
    pub flags: u32,
}

/// `TwoPhasePredicateLockRecord` - per-lock 2PC state.
pub struct TwoPhasePredicateLockRecord {
    pub target: PredicateLockTargetTag,
    /// To avoid length change in back-patched fix.
    pub filler: u32,
}

/// `TwoPhasePredicateRecord` - C tagged union; modeled as a Rust enum.
pub enum TwoPhasePredicateRecord {
    Xact(TwoPhasePredicateXactRecord),
    Lock(TwoPhasePredicateLockRecord),
}

// --- functions needing predicate-locking internals awareness ---

pub fn get_predicate_lock_status_data() -> PredicateLockData {
    unimplemented!()
}

/// Returns the blocking pids written into `output`; returns the count.
pub fn get_safe_snapshot_blocking_pids(_blocked_pid: i32, _output: &mut [i32]) -> i32 {
    unimplemented!()
}
