//! Translated from PostgreSQL src/include/storage/proc.h
//!
//! Per-process backend state. In C this is a shared-memory slot (`PGPROC`) with
//! a global directory (`PROC_HDR`/ProcGlobal). STUB. Under the single-process
//! async model each backend's `PGPROC` becomes per-task owned in-memory state,
//! the dense ProcGlobal mirror arrays collapse into owned collections, and the
//! semaphore/latch waits become tokio primitives. The big struct is modeled
//! in-memory (no layout contract); the proc operations are stub signatures.
//!
//! This RESOLVES the level-4 `storage::lock::PGPROC` forward declaration: the
//! real `PGPROC` is defined here.
// TODO(lock-manager): per-task proc, async waits

use crate::access::clog::XidStatus;
use crate::access::xlogdefs::XLogRecPtr;
use crate::c::{LocalTransactionId, TransactionId};
use crate::lib::stringinfo::StringInfo;
use crate::postgres_ext::Oid;
use crate::storage::lock::{LockMethod, LOCALLOCK, LOCK, LOCKMODE, PROCLOCK};
use crate::storage::lockdefs::LOCKMASK;
use crate::storage::procnumber::ProcNumber;

use bitflags::bitflags;

// Tombstoned includes applied (not imported):
//   storage/latch.h    -> Latch becomes tokio::sync::Notify (procLatch field dropped)
//   storage/pg_sema.h  -> PGSemaphore becomes a tokio wait primitive (sem field dropped)
//   storage/lwlock.h   -> LWLock becomes a parking_lot/std lock (fpInfoLock dropped)
//   lib/ilist.h        -> intrusive dlist/dclist links become owned collections later
//   storage/proclist_types.h -> proclist_node links dropped (owned queues later)

/// Each backend advertises up to this many subxact TransactionIds.
pub const PGPROC_MAX_CACHED_SUBXIDS: usize = 64;

/// In C, NUM_LOCK_PARTITIONS is defined in the tombstoned lwlock.h; kept here as
/// the size of PGPROC.myProcLocks. (1 << 4)
pub const LOG2_NUM_LOCK_PARTITIONS: usize = 4;
pub const NUM_LOCK_PARTITIONS: usize = 1 << LOG2_NUM_LOCK_PARTITIONS;

/// Cached-subxid bookkeeping mirrored into ProcGlobal->subxidStates[].
#[derive(Debug, Clone, Copy, Default)]
pub struct XidCacheStatus {
    /// Number of cached subxids, never more than PGPROC_MAX_CACHED_SUBXIDS.
    pub count: u8,
    /// Has PGPROC->subxids overflowed.
    pub overflowed: bool,
}

/// Cache of subtransaction XIDs for the current top transaction.
pub struct XidCache {
    pub xids: [TransactionId; PGPROC_MAX_CACHED_SUBXIDS],
}

bitflags! {
    /// Flags for PGPROC.statusFlags and PROC_HDR.statusFlags[]. Single-bit set
    /// (composite masks below).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ProcStatusFlags: u8 {
        /// Is it an autovac worker?
        const PROC_IS_AUTOVACUUM = 0x01;
        /// Currently running lazy vacuum.
        const PROC_IN_VACUUM = 0x02;
        /// Currently running CREATE INDEX / REINDEX CONCURRENTLY on a simple index.
        const PROC_IN_SAFE_IC = 0x04;
        /// Set by autovac only: vacuum for wraparound.
        const PROC_VACUUM_FOR_WRAPAROUND = 0x08;
        /// Currently doing logical decoding outside xact.
        const PROC_IN_LOGICAL_DECODING = 0x10;
        /// This proc's xmin must be included in vacuum horizons in all databases.
        const PROC_AFFECTS_ALL_HORIZONS = 0x20;

        /// Flags reset at EOXact.
        const PROC_VACUUM_STATE_MASK =
            Self::PROC_IN_VACUUM.bits()
            | Self::PROC_IN_SAFE_IC.bits()
            | Self::PROC_VACUUM_FOR_WRAPAROUND.bits();

        /// Xmin-related flags affecting VACUUM's interpretation of this proc's xmin.
        const PROC_XMIN_FLAGS =
            Self::PROC_IN_VACUUM.bits() | Self::PROC_IN_SAFE_IC.bits();
    }
}

/// GUC: number of fast-path locking groups per backend.
// TODO(global): GUCs become session/global config under the async model.
pub static mut FastPathLockGroupsPerBackend: i32 = 0;

/// Max number of fast-path locking groups per backend (power of two).
pub const FP_LOCK_GROUPS_PER_BACKEND_MAX: i32 = 1024;
/// Fast-path lock slots per group (don't change).
pub const FP_LOCK_SLOTS_PER_GROUP: i32 = 16;

/// C macro `FastPathLockSlotsPerBackend()`.
pub fn fast_path_lock_slots_per_backend() -> i32 {
    FP_LOCK_SLOTS_PER_GROUP * unsafe { FastPathLockGroupsPerBackend }
}

bitflags! {
    /// Flags for PGPROC.delayChkptFlags: delay start/completion of a checkpoint.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DelayChkptFlags: i32 {
        /// Prevent moving from checkpoint phase 1 to phase 2.
        const DELAY_CHKPT_START = 1 << 0;
        /// Prevent moving from checkpoint phase 2 to phase 3.
        const DELAY_CHKPT_COMPLETE = 1 << 1;
    }
}

/// Result of a lock-wait attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ProcWaitStatus {
    PROC_WAIT_STATUS_OK = 0,
    PROC_WAIT_STATUS_WAITING,
    PROC_WAIT_STATUS_ERROR,
}

/// Currently-running top-level transaction's virtual xid. Held as two fields
/// (not VirtualTransactionId) because it is not atomically assignable as a whole.
pub struct PgProcVxid {
    /// For regular backends == GetNumberFromPGProc(proc); for prepared xacts the
    /// original backend's ID; for unused entries INVALID_PROC_NUMBER.
    pub proc_number: ProcNumber,
    /// Local id of the top-level transaction, if running.
    pub lxid: LocalTransactionId,
}

/// Each backend's per-process state. In C a shared-memory slot; here in-memory,
/// owned per task. The semaphore/latch and intrusive list links are dropped or
/// replaced by tokio/owned collections (see tombstone notes at the top).
pub struct PGPROC {
    // links / procgloballist: intrusive freelist links -> owned by ProcGlobal later.
    // sem (PGSemaphore): -> tokio wait primitive (dropped).
    /// Lock-wait result.
    pub wait_status: ProcWaitStatus,
    // procLatch (Latch): -> tokio::sync::Notify (dropped).

    /// Top-level xact's XID if running and assigned, else InvalidTransactionId.
    /// Mirrored in ProcGlobal->xids[pgxactoff].
    pub xid: TransactionId,
    /// Minimal running XID at xact start, excluding LAZY VACUUM.
    pub xmin: TransactionId,

    /// Backend's process ID; 0 if prepared xact.
    pub pid: i32,
    /// Offset into ProcGlobal dense arrays mirrored from this PGPROC.
    pub pgxactoff: i32,

    /// Virtual xid of the current top-level transaction.
    pub vxid: PgProcVxid,

    /// OID of the database this backend is using (0 while starting up).
    pub database_id: Oid,
    /// OID of the role using this backend (0 while starting up).
    pub role_id: Oid,
    /// OID of the temp schema this backend is using.
    pub temp_namespace_id: Oid,

    /// True if it's a regular backend.
    pub is_regular_backend: bool,
    /// Hot-standby conflict signal pending for the current transaction.
    pub recovery_conflict_pending: bool,

    /// LWLock the process is waiting for (see LWLockWaitState). lwWaitLink (the
    /// intrusive position in the LW wait list) is dropped.
    pub lw_waiting: u8,
    /// LWLock mode being waited for.
    pub lw_wait_mode: u8,
    // lwWaitLink / cvWaitLink (proclist_node): -> owned wait queues later.

    /// Lock object we're sleeping on, or None. // TODO(ptr): ownership unclear.
    pub wait_lock: Option<*mut LOCK>,
    /// Per-holder info for the awaited lock, or None. // TODO(ptr)
    pub wait_proc_lock: Option<*mut PROCLOCK>,
    /// Type of lock we're waiting for.
    pub wait_lock_mode: LOCKMODE,
    /// Bitmask of lock types already held on this lock object by this backend.
    pub held_locks: LOCKMASK,
    /// Time at which the lock-acquisition wait started (was pg_atomic_uint64).
    pub wait_start: u64,

    /// DELAY_CHKPT_* flags.
    pub delay_chkpt_flags: DelayChkptFlags,

    /// This backend's status flags; mirrored in ProcGlobal->statusFlags[pgxactoff].
    pub status_flags: ProcStatusFlags,

    /// Waiting for this LSN or higher (InvalidXLogRecPtr if not waiting).
    pub wait_lsn: XLogRecPtr,
    /// Wait state for sync rep.
    pub sync_rep_state: i32,
    // syncRepLinks (dlist_node): -> owned syncrep queue later.

    /// PROCLOCKs held/awaited by this backend, partitioned by lock partition.
    /// Was an array of intrusive dlist heads; owned collections now.
    pub my_proc_locks: [Vec<*mut PROCLOCK>; NUM_LOCK_PARTITIONS], // TODO(ptr)

    /// Mirrored with ProcGlobal->subxidStates[i].
    pub subxid_status: XidCacheStatus,
    /// Cache of subtransaction XIDs.
    pub subxids: XidCache,

    /// Member of ProcArray group waiting for XID clear.
    pub proc_array_group_member: bool,
    /// Next ProcArray group member waiting for XID clear (was pg_atomic_uint32).
    pub proc_array_group_next: u32,
    /// Latest xid among the transaction's main XID and subtransactions.
    pub proc_array_group_member_xid: TransactionId,

    /// Proc's wait information.
    pub wait_event_info: u32,

    /// Member of clog group.
    pub clog_group_member: bool,
    /// Next clog group member (was pg_atomic_uint32).
    pub clog_group_next: u32,
    /// Transaction id of the clog group member.
    pub clog_group_member_xid: TransactionId,
    /// Transaction status of the clog group member.
    pub clog_group_member_xid_status: XidStatus,
    /// Clog page for the clog group member's transaction id.
    pub clog_group_member_page: i64,
    /// WAL location of the commit record for the clog group member.
    pub clog_group_member_lsn: XLogRecPtr,

    // fpInfoLock (LWLock): -> parking_lot/std lock protecting fast-path state (dropped).
    /// Lock modes held for each fast-path slot.
    pub fp_lock_bits: Vec<u64>,
    /// Slots for rel oids.
    pub fp_rel_id: Vec<Oid>,
    /// Are we holding a fast-path VXID lock?
    pub fp_vxid_lock: bool,
    /// lxid for the fast-path VXID lock.
    pub fp_local_transaction_id: LocalTransactionId,

    /// Lock group leader, if I'm a member. // TODO(ptr)
    pub lock_group_leader: Option<*mut PGPROC>,
    /// List of members, if I'm a leader (was dlist_head). // TODO(ptr)
    pub lock_group_members: Vec<*mut PGPROC>,
    // lockGroupLink (dlist_node): my member link -> owned by the leader's Vec.
}

/// C global `PGPROC *MyProc`. // TODO(global): becomes task-local under async.
pub static mut MyProc: Option<*mut PGPROC> = None;

/// Cluster-wide proc directory. The dense mirror arrays and intrusive free lists
/// become owned collections; the atomic group-list heads become plain fields.
pub struct PROC_HDR {
    /// All PGPROC structures (not including dummies for prepared txns). // TODO(ptr)
    pub all_procs: Vec<*mut PGPROC>,
    /// Mirror of PGPROC.xid for each PGPROC in the procarray.
    pub xids: Vec<TransactionId>,
    /// Mirror of PGPROC.subxidStatus for each PGPROC in the procarray.
    pub subxid_states: Vec<XidCacheStatus>,
    /// Mirror of PGPROC.statusFlags for each PGPROC in the procarray.
    pub status_flags: Vec<u8>,
    /// Length of all_procs.
    pub all_proc_count: u32,

    /// Free PGPROC structures (was dlist_head freeProcs). // TODO(ptr)
    pub free_procs: Vec<*mut PGPROC>,
    /// Free autovacuum & special worker PGPROCs.
    pub autovac_free_procs: Vec<*mut PGPROC>,
    /// Free bgworker PGPROCs.
    pub bgworker_free_procs: Vec<*mut PGPROC>,
    /// Free walsender PGPROCs.
    pub walsender_free_procs: Vec<*mut PGPROC>,

    /// First pgproc waiting for group XID clear (was pg_atomic_uint32).
    pub proc_array_group_first: u32,
    /// First pgproc waiting for group transaction status update.
    pub clog_group_first: u32,

    /// Current slot number of the WAL writer.
    pub walwriter_proc: ProcNumber,
    /// Current slot number of the checkpointer.
    pub checkpointer_proc: ProcNumber,

    /// Current shared estimate of spins_per_delay.
    pub spins_per_delay: i32,
    /// Buffer id the Startup process waits for pin on, or -1.
    pub startup_buffer_pin_wait_buf_id: i32,
}

/// C global `PROC_HDR *ProcGlobal`. // TODO(global)
pub static mut ProcGlobal: Option<*mut PROC_HDR> = None;

/// C global `PGPROC *PreparedXactProcs`. // TODO(global)
pub static mut PreparedXactProcs: Option<*mut PGPROC> = None;

/// Extra PGPROCs for "special worker" processes (autovacuum launcher, slotsync).
pub const NUM_SPECIAL_WORKER_PROCS: i32 = 2;

/// Extra PGPROCs for auxiliary processes.
pub const MAX_IO_WORKERS: i32 = 32;
pub const NUM_AUXILIARY_PROCS: i32 = 6 + MAX_IO_WORKERS;

// configurable options (GUCs). TODO(global)
pub static mut DeadlockTimeout: i32 = 1000;
pub static mut StatementTimeout: i32 = 0;
pub static mut LockTimeout: i32 = 0;
pub static mut IdleInTransactionSessionTimeout: i32 = 0;
pub static mut TransactionTimeout: i32 = 0;
pub static mut IdleSessionTimeout: i32 = 0;
pub static mut log_lock_waits: bool = false;

// === Function Prototypes (stubs) ===

pub fn ProcGlobalSemas() -> i32 {
    unimplemented!()
}

pub fn ProcGlobalShmemSize() -> usize {
    unimplemented!()
}

pub fn InitProcGlobal() {
    unimplemented!()
}

pub fn InitProcess() {
    unimplemented!()
}

pub fn InitProcessPhase2() {
    unimplemented!()
}

pub fn InitAuxiliaryProcess() {
    unimplemented!()
}

pub fn SetStartupBufferPinWaitBufId(_bufid: i32) {
    unimplemented!()
}

pub fn GetStartupBufferPinWaitBufId() -> i32 {
    unimplemented!()
}

/// Returns (have_enough, n_free): whether at least `n` free procs exist + count.
pub fn HaveNFreeProcs(_n: i32) -> (bool, i32) {
    unimplemented!()
}

pub fn ProcReleaseLocks(_is_commit: bool) {
    unimplemented!()
}

pub fn JoinWaitQueue(
    _locallock: &mut LOCALLOCK,
    _lock_method_table: LockMethod,
    _dont_wait: bool,
) -> ProcWaitStatus {
    unimplemented!()
}

pub fn ProcSleep(_locallock: &mut LOCALLOCK) -> ProcWaitStatus {
    unimplemented!()
}

pub fn ProcWakeup(_proc: &mut PGPROC, _wait_status: ProcWaitStatus) {
    unimplemented!()
}

pub fn ProcLockWakeup(_lock_method_table: LockMethod, _lock: &mut LOCK) {
    unimplemented!()
}

pub fn CheckDeadLockAlert() {
    unimplemented!()
}

pub fn LockErrorCleanup() {
    unimplemented!()
}

/// Returns the number of lock holders (C `*lockHoldersNum` out-param folded in);
/// holder/waiter descriptions are appended to the provided StringInfos.
pub fn GetLockHoldersAndWaiters(
    _locallock: &mut LOCALLOCK,
    _lock_holders_sbuf: &mut StringInfo,
    _lock_waiters_sbuf: &mut StringInfo,
) -> i32 {
    unimplemented!()
}

pub fn ProcWaitForSignal(_wait_event_info: u32) {
    unimplemented!()
}

pub fn ProcSendSignal(_proc_number: ProcNumber) {
    unimplemented!()
}

/// Returns the auxiliary proc with the given pid, or None.
pub fn AuxiliaryPidGetProc(_pid: i32) -> Option<*mut PGPROC> {
    unimplemented!()
}

pub fn BecomeLockGroupLeader() {
    unimplemented!()
}

pub fn BecomeLockGroupMember(_leader: &mut PGPROC, _pid: i32) -> bool {
    unimplemented!()
}
