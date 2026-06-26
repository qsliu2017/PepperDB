//! Translated from PostgreSQL src/include/storage/proc.h
//!
//! Per-process backend state. In C this is a shared-memory slot (`PGPROC`) with a
//! global directory (`PROC_HDR`/ProcGlobal). The proc.c bodies live in
//! `src/backend/storage/lmgr/proc.rs`; this header keeps the types, consts, GUCs,
//! and the accessor shims for the (former) `static mut MyProc`/`ProcGlobal`.
//!
//! Representation (design step15 s0): PGPROCs live in a FIXED, process-lifetime
//! arena (`ProcGlobal`), allocated once by `InitProcGlobal`. References that cross
//! tasks or live in shared structures use `ProcNumber` (i32 index into the arena),
//! NOT `*mut PGPROC` -- a raw `*mut` is `!Send` and our per-task/shared state must
//! be Send (rules s6.1). The arena is `Arc<ProcGlobal>` reachable via the
//! process-wide `proc_global()` accessor (published by `InitProcGlobal`); `MyProc`
//! is a per-task `task_local` holding this backend's `ProcNumber`.
//!
//! This RESOLVES the level-4 `storage::lock::PGPROC` forward declaration: the real
//! `PGPROC` is defined here.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::access::clog::XidStatus;
use crate::access::xlogdefs::XLogRecPtr;
use crate::c::{LocalTransactionId, TransactionId};
use crate::postgres_ext::Oid;
use crate::storage::lock::{LOCK, LOCKMODE, PROCLOCK};
use crate::storage::lockdefs::LOCKMASK;
use crate::storage::procnumber::{INVALID_PROC_NUMBER, ProcNumber};

use bitflags::bitflags;

// Tombstoned includes applied (not imported):
//   storage/pg_sema.h  -> PGSemaphore becomes a tokio wait primitive (sem dropped)
//   storage/lwlock.h   -> LWLock becomes a parking_lot/std lock (fpInfoLock dropped)
//   lib/ilist.h        -> intrusive dlist/dclist links become ProcNumber-keyed
//   storage/proclist_types.h -> proclist_node links dropped (owned queues later)
//
// storage/latch.h IS used now: each PGPROC owns a `Latch` (procLatch) for the
// grant-wait wake (ProcSleep/ProcWakeup); design step15 s1.

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
    OK = 0,
    WAITING,
    ERROR,
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

/// Each backend's per-process state. In C a shared-memory slot; here a slot in
/// the `ProcGlobal` arena, reached by `ProcNumber`.
///
/// Locking of the field groups (design step15 s0):
/// - The lock-free-read MVCC fields (`xid`, `xmin`, `subxid_status`/`subxids`,
///   `status_flags`) are scanned by procarray; their authoritative shared copies
///   are the `ProcGlobal` mirror arrays (written under ProcArrayLock). The PGPROC
///   copies here are written by the owning backend (also under ProcArrayLock when
///   advertising/clearing an xid).
/// - The lock-wait fields (`wait_status`, `wait_lock`, `wait_proc_lock`,
///   `wait_lock_mode`, `held_locks`, `proc_latch`) are written under the lock
///   partition Mutex (lock.c, 15b) and read by the waiter; `proc_latch` is the
///   grant-wait wake and is itself internally synchronized.
/// - Identity/lifecycle fields (`pid`, `vxid`, `pgxactoff`, the group links) are
///   set under the `ProcStructLock` (lifecycle) or by the owning backend.
pub struct PGPROC {
    /// Which free list this PGPROC belongs to (so ProcKill returns it correctly).
    /// `ProcGlobalList::None` for auxiliary/prepared-xact slots.
    pub proc_global_list: ProcGlobalList,
    /// Lock-wait result.
    pub wait_status: ProcWaitStatus,
    /// Grant-wait wake primitive (PG procLatch). ProcWakeup sets it; ProcSleep
    /// awaits it.
    pub proc_latch: Arc<crate::storage::latch::Latch>,
    /// Top-level xact's XID if running and assigned, else InvalidTransactionId.
    /// Mirrored in ProcGlobal->xids[pgxactoff].
    pub xid: TransactionId,
    /// Minimal running XID at xact start, excluding LAZY VACUUM.
    pub xmin: TransactionId,

    /// Backend's process ID; 0 if prepared xact or unused.
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

    /// LWLock the process is waiting for (see LWLockWaitState). lwWaitLink dropped.
    pub lw_waiting: u8,
    /// LWLock mode being waited for.
    pub lw_wait_mode: u8,
    /// Lock object we're sleeping on, or None (lock.c LOCK, resolved in 15b).
    pub wait_lock: Option<*mut LOCK>,
    /// Per-holder info for the awaited lock, or None (lock.c PROCLOCK, 15b).
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
    /// PROCLOCKs held/awaited by this backend, partitioned by lock partition.
    /// (lock.c PROCLOCK, resolved in 15b.)
    pub my_proc_locks: [Vec<*mut PROCLOCK>; NUM_LOCK_PARTITIONS],

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

    /// Serializes access to this proc's fast-path arrays (PG `fpInfoLock`, an
    /// LWLock -> std Mutex per rules s9). A bare token Mutex: the fast-path data
    /// lives in the sibling fields, reached through the arena's UnsafeCell while
    /// this guard is held (another backend may touch our arrays in
    /// FastPathTransferRelationLocks / GetLockConflicts).
    pub fp_info_lock: Mutex<()>,
    /// Lock modes held for each fast-path slot.
    pub fp_lock_bits: Vec<u64>,
    /// Slots for rel oids.
    pub fp_rel_id: Vec<Oid>,
    /// Are we holding a fast-path VXID lock?
    pub fp_vxid_lock: bool,
    /// lxid for the fast-path VXID lock.
    pub fp_local_transaction_id: LocalTransactionId,

    /// This backend's role in its lock group (PG `lockGroupLeader`/
    /// `lockGroupMembers`). `None` = not in a group; `Leader` = I am the leader
    /// (PG's `lockGroupLeader == self`), holding the member list; `Member` = I am
    /// a member of `leader`'s group (PG's `lockGroupLeader == leader`).
    pub lock_group_role: LockGroupRole,
}

/// A backend's lock-group membership. Replaces the PG sentinel-field pair
/// `lockGroupLeader` (INVALID if none / self if leader) + `lockGroupMembers`.
#[derive(Debug, Default)]
pub enum LockGroupRole {
    /// Not part of any lock group (PG `lockGroupLeader == NULL`).
    #[default]
    None,
    /// This backend is the group leader (PG `lockGroupLeader == self`). The
    /// member list includes the leader itself (PG pushes self in
    /// `BecomeLockGroupLeader`).
    Leader { members: Vec<ProcNumber> },
    /// This backend is a member of `leader`'s group (PG `lockGroupLeader ==
    /// leader`, with `leader != self`).
    Member { leader: ProcNumber },
}

/// Which ProcGlobal free list a PGPROC was drawn from (PG `procgloballist`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcGlobalList {
    /// Aux/prepared-xact slots are never freelisted.
    None,
    Free,
    Autovac,
    Bgworker,
    Walsender,
}

impl PGPROC {
    /// A freshly-zeroed PGPROC (InitProcGlobal zero-inits each slot).
    pub fn new() -> Self {
        Self {
            proc_global_list: ProcGlobalList::None,
            wait_status: ProcWaitStatus::OK,
            proc_latch: Arc::new(crate::storage::latch::Latch::new()),
            xid: TransactionId(0),
            xmin: TransactionId(0),
            pid: 0,
            pgxactoff: 0,
            vxid: PgProcVxid {
                proc_number: INVALID_PROC_NUMBER,
                lxid: LocalTransactionId(0),
            },
            database_id: Oid(0),
            role_id: Oid(0),
            temp_namespace_id: Oid(0),
            is_regular_backend: false,
            recovery_conflict_pending: false,
            lw_waiting: 0,
            lw_wait_mode: 0,
            wait_lock: None,
            wait_proc_lock: None,
            wait_lock_mode: 0,
            held_locks: 0,
            wait_start: 0,
            delay_chkpt_flags: DelayChkptFlags::empty(),
            status_flags: ProcStatusFlags::empty(),
            wait_lsn: XLogRecPtr(0),
            sync_rep_state: 0,
            my_proc_locks: std::array::from_fn(|_| Vec::new()),
            subxid_status: XidCacheStatus::default(),
            subxids: XidCache {
                xids: [TransactionId(0); PGPROC_MAX_CACHED_SUBXIDS],
            },
            proc_array_group_member: false,
            proc_array_group_next: INVALID_PROC_NUMBER as u32,
            proc_array_group_member_xid: TransactionId(0),
            wait_event_info: 0,
            clog_group_member: false,
            clog_group_next: INVALID_PROC_NUMBER as u32,
            clog_group_member_xid: TransactionId(0),
            clog_group_member_xid_status: XidStatus::InProgress,
            clog_group_member_page: -1,
            clog_group_member_lsn: XLogRecPtr(0),
            fp_info_lock: Mutex::new(()),
            fp_lock_bits: Vec::new(),
            fp_rel_id: Vec::new(),
            fp_vxid_lock: false,
            fp_local_transaction_id: LocalTransactionId(0),
            lock_group_role: LockGroupRole::None,
        }
    }
}

impl Default for PGPROC {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// The PGPROC arena (interior-mutable, ProcNumber-indexed)
// ---------------------------------------------------------------------------

/// One arena slot: an `UnsafeCell<PGPROC>` so the fixed arena can be mutated
/// in-place by the owning backend / under the relevant lock without forming
/// `&mut` aliasing UB (mirrors the buffer pool's `PageCell`).
pub struct ProcCell(UnsafeCell<PGPROC>);

impl ProcCell {
    fn new(proc: PGPROC) -> Self {
        Self(UnsafeCell::new(proc))
    }

    /// Shared reference to the slot's PGPROC.
    ///
    /// # Safety
    /// The caller must ensure no `&mut` to this slot exists concurrently. Reads of
    /// the MVCC-scanned fields are gated by ProcArrayLock; reads of the wait fields
    /// by the lock partition Mutex; the rest by the owning backend.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get(&self) -> &PGPROC {
        unsafe { &*self.0.get() }
    }

    /// Exclusive reference to the slot's PGPROC.
    ///
    /// # Safety
    /// The caller must hold the lock that gates the field group it mutates (see the
    /// PGPROC doc), and ensure no aliasing `&`/`&mut` to this slot exists.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get_mut(&self) -> &mut PGPROC {
        unsafe { &mut *self.0.get() }
    }
}

// SAFETY: ProcCell wraps an UnsafeCell<PGPROC> in a fixed, never-resized arena.
// Soundness comes from the documented locking discipline (design step15 s0): the
// MVCC-scanned fields are accessed under ProcArrayLock, the wait fields under the
// lock partition Mutex, lifecycle fields under ProcStructLock, and the remaining
// per-backend fields only by the owning task. No two tasks form overlapping
// references to the same slot's mutated field group at the same time. The raw
// `*mut LOCK`/`*mut PROCLOCK` fields (PGPROC.wait_lock/wait_proc_lock/myProcLocks)
// point at lock.c structures whose own locking (15b) governs them; they are never
// dereferenced through a shared ProcCell ref without that lock held. These raw
// pointers are what make PGPROC `!Send`/`!Sync` by default; the arena is shared by
// `Arc` across the multi-thread runtime (tokio::spawn migrates tasks), so the
// whole `ProcGlobal` -- and thus `ProcCell` -- must be Send + Sync. That is sound
// because the arena is never moved and the locking above serializes mutation.
unsafe impl Sync for ProcCell {}
#[allow(clippy::non_send_fields_in_send_ty, reason = "PGPROC arena Send is gated by lock discipline per the # Safety docs")]
unsafe impl Send for ProcCell {}

/// Cluster-wide proc directory (PG `PROC_HDR`). The arena + the dense MVCC mirror
/// arrays + the free lists. Allocated once by `InitProcGlobal`, never resized.
/// Shared as `Arc<ProcGlobal>`; published process-wide by `set_proc_global`.
pub struct ProcGlobal {
    /// All PGPROC structures (arena), indexed by ProcNumber. Includes the aux +
    /// prepared-xact slots; never resized after InitProcGlobal.
    all_procs: Vec<ProcCell>,

    /// Mirror of PGPROC.xid, indexed by pgxactoff (dense; written under
    /// ProcArrayLock). Authoritative shared copy for the lock-free snapshot scan.
    pub xids: Vec<AtomicU32>,
    /// Mirror of PGPROC.subxidStatus, indexed by pgxactoff (under ProcArrayLock).
    /// Packed as count|overflowed; use `xid_cache_status_{load,store}`.
    pub subxid_states: Vec<AtomicU32>,
    /// Mirror of PGPROC.statusFlags, indexed by pgxactoff (under ProcArrayLock).
    pub status_flags: Vec<AtomicU32>,
    /// Length of all_procs excluding prepared-xact slots (PG `allProcCount`).
    pub all_proc_count: u32,

    /// Free lists + spins_per_delay, behind the ex-`ProcStructLock` spinlock.
    free: Mutex<ProcFreeLists>,

    /// First pgproc waiting for group XID clear (was pg_atomic_uint32).
    pub proc_array_group_first: AtomicU32,
    /// First pgproc waiting for group transaction status update.
    pub clog_group_first: AtomicU32,

    /// Current slot number of the WAL writer.
    pub walwriter_proc: AtomicI32,
    /// Current slot number of the checkpointer.
    pub checkpointer_proc: AtomicI32,
    /// Proc number of the autovacuum launcher, or INVALID when none. PG advertises
    /// the launcher PID in `AutoVacuumShmem->av_launcherpid`; under our model a
    /// worker rings the launcher by its proc number on exit (FreeWorkerInfo).
    pub autovacuum_launcher_proc: AtomicI32,

    /// Buffer id the Startup process waits for pin on, or -1.
    pub startup_buffer_pin_wait_buf_id: AtomicI32,

    /// ProcNumber of the first auxiliary slot (PG `AuxiliaryProcs`).
    pub aux_proc_base: ProcNumber,
    /// ProcNumber of the first prepared-xact slot (PG `PreparedXactProcs`).
    pub prepared_xact_base: ProcNumber,
}

/// The free lists protected by ProcStructLock (PG `freeProcs` et al + the
/// shared spins_per_delay estimate).
struct ProcFreeLists {
    free_procs: Vec<ProcNumber>,
    autovac_free_procs: Vec<ProcNumber>,
    bgworker_free_procs: Vec<ProcNumber>,
    walsender_free_procs: Vec<ProcNumber>,
    spins_per_delay: i32,
}

/// PG `DEFAULT_SPINS_PER_DELAY`.
const DEFAULT_SPINS_PER_DELAY: i32 = 100;

impl ProcGlobal {
    /// Build the arena. `counts` carries the per-category slot counts computed by
    /// `InitProcGlobal` from the sizing GUCs. Called once.
    pub(crate) fn new(counts: ProcCounts) -> Self {
        let total = counts.total();
        let mut all_procs = Vec::with_capacity(total);
        let mut free = ProcFreeLists {
            free_procs: Vec::new(),
            autovac_free_procs: Vec::new(),
            bgworker_free_procs: Vec::new(),
            walsender_free_procs: Vec::new(),
            spins_per_delay: DEFAULT_SPINS_PER_DELAY,
        };

        for i in 0..total {
            let mut proc = PGPROC::new();
            let procno = i as ProcNumber;
            // Match InitProcGlobal's freelist construction (proc.c).
            if i < counts.max_connections {
                proc.proc_global_list = ProcGlobalList::Free;
                free.free_procs.push(procno);
            } else if i < counts.max_connections + counts.autovac_special {
                proc.proc_global_list = ProcGlobalList::Autovac;
                free.autovac_free_procs.push(procno);
            } else if i < counts.max_connections + counts.autovac_special + counts.bgworkers {
                proc.proc_global_list = ProcGlobalList::Bgworker;
                free.bgworker_free_procs.push(procno);
            } else if i < counts.max_backends {
                proc.proc_global_list = ProcGlobalList::Walsender;
                free.walsender_free_procs.push(procno);
            }
            // else: auxiliary or prepared-xact slot -> ProcGlobalList::None.
            all_procs.push(ProcCell::new(proc));
        }

        let mirror_len = total;
        Self {
            all_procs,
            xids: (0..mirror_len).map(|_| AtomicU32::new(0)).collect(),
            subxid_states: (0..mirror_len).map(|_| AtomicU32::new(0)).collect(),
            status_flags: (0..mirror_len).map(|_| AtomicU32::new(0)).collect(),
            all_proc_count: (counts.max_backends + counts.num_auxiliary) as u32,
            free: Mutex::new(free),
            proc_array_group_first: AtomicU32::new(INVALID_PROC_NUMBER as u32),
            clog_group_first: AtomicU32::new(INVALID_PROC_NUMBER as u32),
            walwriter_proc: AtomicI32::new(INVALID_PROC_NUMBER),
            checkpointer_proc: AtomicI32::new(INVALID_PROC_NUMBER),
            autovacuum_launcher_proc: AtomicI32::new(INVALID_PROC_NUMBER),
            startup_buffer_pin_wait_buf_id: AtomicI32::new(-1),
            aux_proc_base: counts.max_backends as ProcNumber,
            prepared_xact_base: (counts.max_backends + counts.num_auxiliary) as ProcNumber,
        }
    }

    /// Number of PGPROC slots in the arena (including aux + prepared-xact).
    pub fn len(&self) -> usize {
        self.all_procs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.all_procs.is_empty()
    }

    /// Shared reference to slot `procno`, or None if out of range.
    ///
    /// # Safety
    /// Caller must respect the slot's locking discipline (PGPROC doc): no `&mut`
    /// to the same field group may exist concurrently.
    pub unsafe fn proc(&self, procno: ProcNumber) -> Option<&PGPROC> {
        self.all_procs
            .get(procno as usize)
            .map(|c| unsafe { c.get() })
    }

    /// Exclusive reference to slot `procno`, or None if out of range.
    ///
    /// # Safety
    /// Caller must hold the lock gating the mutated field group (PGPROC doc).
    #[allow(
        clippy::mut_from_ref,
        reason = "PGPROC arena interior-mutability accessor; caller holds the gating lock per the # Safety doc"
    )]
    pub unsafe fn proc_mut(&self, procno: ProcNumber) -> Option<&mut PGPROC> {
        self.all_procs
            .get(procno as usize)
            .map(|c| unsafe { c.get_mut() })
    }

    /// The raw arena cell (for callers that manage their own access).
    pub fn cell(&self, procno: ProcNumber) -> Option<&ProcCell> {
        self.all_procs.get(procno as usize)
    }

    // --- ProcStructLock-protected free-list operations ---

    /// Pop a free PGPROC from the list for `kind`, or None if exhausted. Also
    /// snapshots spins_per_delay into the caller's return.
    pub(crate) fn alloc_proc(&self, kind: ProcGlobalList) -> Option<ProcNumber> {
        let mut f = self.free.lock().unwrap();
        let list = f.list_mut(kind);
        list.pop()
    }

    /// Return a PGPROC to its free list (ProcKill).
    pub(crate) fn free_proc(&self, kind: ProcGlobalList, procno: ProcNumber) {
        let mut f = self.free.lock().unwrap();
        f.list_mut(kind).push(procno);
    }

    /// PG `InitAuxiliaryProcess` scan+claim, serialized under the ex-`ProcStructLock`.
    /// Aux PGPROCs have no freelist; PG scans `AuxiliaryProcs` for a slot with
    /// `pid == 0` and claims it by writing `pid`. The whole claim AND the owner's
    /// field init (`init`) run under `ProcStructLock`: another task's scan reads the
    /// `pid` of every slot, so the slot's own field init -- which also touches
    /// `pid` -- must not run concurrently with a scan, or the two data-race on the
    /// PGPROC. Returns the claimed slot, or None if all aux slots are in use.
    pub(crate) fn claim_aux_slot(
        &self,
        pid: i32,
        init: impl FnOnce(&mut PGPROC, ProcNumber),
    ) -> Option<ProcNumber> {
        let _f = self.free.lock().unwrap();
        for i in 0..NUM_AUXILIARY_PROCS {
            let procno = self.aux_proc_base + i;
            // SAFETY: the ProcStructLock (`free`) is held for the whole claim +
            // init, gating every PGPROC field a concurrent scan/claim could read.
            let proc = unsafe { self.proc_mut(procno)? };
            if proc.pid == 0 {
                proc.pid = pid;
                init(proc, procno);
                return Some(procno);
            }
        }
        None
    }

    /// PG `AuxiliaryProcKill`: release an aux PGPROC, serialized under the
    /// ex-`ProcStructLock` so the owner's final field clears (`clear`) and the
    /// `pid` release cannot race a concurrent `claim_aux_slot` scan/init.
    pub(crate) fn release_aux_slot(&self, procno: ProcNumber, clear: impl FnOnce(&mut PGPROC)) {
        let _f = self.free.lock().unwrap();
        // SAFETY: the ProcStructLock (`free`) is held, gating every PGPROC field a
        // concurrent scan/claim could read.
        if let Some(proc) = unsafe { self.proc_mut(procno) } {
            clear(proc);
            proc.pid = 0;
        }
    }

    /// Count of free regular-backend PGPROCs, capped at `n` (PG HaveNFreeProcs).
    pub(crate) fn n_free_regular(&self, n: i32) -> i32 {
        let f = self.free.lock().unwrap();
        (f.free_procs.len() as i32).min(n)
    }

    pub(crate) fn spins_per_delay(&self) -> i32 {
        self.free.lock().unwrap().spins_per_delay
    }
}

impl ProcFreeLists {
    fn list_mut(&mut self, kind: ProcGlobalList) -> &mut Vec<ProcNumber> {
        #[allow(clippy::match_same_arms, reason = "variants kept separate for 1:1 PG ProcGlobalList mapping")]
        match kind {
            ProcGlobalList::Free => &mut self.free_procs,
            ProcGlobalList::Autovac => &mut self.autovac_free_procs,
            ProcGlobalList::Bgworker => &mut self.bgworker_free_procs,
            ProcGlobalList::Walsender => &mut self.walsender_free_procs,
            ProcGlobalList::None => &mut self.free_procs,
        }
    }
}

/// Per-category PGPROC slot counts (PG InitProcGlobal sizing). All are prefix
/// boundaries except `total`.
#[derive(Debug, Clone, Copy)]
pub struct ProcCounts {
    pub max_connections: usize,
    /// max_connections + autovac_worker_slots + NUM_SPECIAL_WORKER_PROCS boundary
    /// is `max_connections + autovac_special`.
    pub autovac_special: usize,
    pub bgworkers: usize,
    /// PG `MaxBackends`.
    pub max_backends: usize,
    /// PG `NUM_AUXILIARY_PROCS`.
    pub num_auxiliary: usize,
    /// PG `max_prepared_xacts`.
    pub max_prepared_xacts: usize,
}

impl ProcCounts {
    pub fn total(&self) -> usize {
        self.max_backends + self.num_auxiliary + self.max_prepared_xacts
    }
}

/// Pack a `XidCacheStatus` into the mirror `AtomicU32` (count in low byte,
/// overflowed in bit 8).
pub fn xid_cache_status_pack(s: XidCacheStatus) -> u32 {
    u32::from(s.count) | if s.overflowed { 0x100 } else { 0 }
}

/// Unpack a mirror `AtomicU32` into a `XidCacheStatus`.
pub fn xid_cache_status_unpack(v: u32) -> XidCacheStatus {
    XidCacheStatus {
        count: (v & 0xff) as u8,
        overflowed: (v & 0x100) != 0,
    }
}

// ---------------------------------------------------------------------------
// Process-wide ProcGlobal + per-task MyProc accessors (ex `static mut`)
// ---------------------------------------------------------------------------

/// The one `Arc<ProcGlobal>` for this process, published by `InitProcGlobal`
/// (single-process model: exactly one). Replaces C `PROC_HDR *ProcGlobal`.
static PROC_GLOBAL: OnceLock<Arc<ProcGlobal>> = OnceLock::new();

/// Publish the process-wide ProcGlobal (InitProcGlobal). Ignores a second publish
/// so tests building multiple arenas do not panic; the first wins, and a test
/// builds its own via `SharedState`. Returns whether this call won.
pub fn set_proc_global(g: Arc<ProcGlobal>) -> bool {
    PROC_GLOBAL.set(g).is_ok()
}

/// The process-wide ProcGlobal, if `InitProcGlobal` has run. Replaces reads of
/// the C `static mut ProcGlobal`.
pub fn proc_global() -> Option<&'static Arc<ProcGlobal>> {
    PROC_GLOBAL.get()
}

tokio::task_local! {
    /// This backend's ProcNumber (PG `MyProc`/`MyProcNumber`). Set by `InitProcess`
    /// / `InitAuxiliaryProcess` for the backend task; only the owning task reads it
    /// for its own slot. `Copy` ProcNumber, so holding it across `.await` is Send.
    static MY_PROC_NUMBER: std::cell::Cell<ProcNumber>;
}

/// This backend's ProcNumber, or INVALID_PROC_NUMBER outside a backend scope.
pub fn current_proc_number() -> ProcNumber {
    MY_PROC_NUMBER
        .try_with(std::cell::Cell::get)
        .unwrap_or(INVALID_PROC_NUMBER)
}

/// Set this backend's ProcNumber (InitProcess). Requires a `my_proc_scope`.
pub fn set_current_proc_number(procno: ProcNumber) {
    let _ = MY_PROC_NUMBER.try_with(|c| c.set(procno));
}

/// Run `f` with a fresh `MyProc` task-local slot (initialized to INVALID). The
/// backend task wraps its body in this so `InitProcess` can publish its slot.
pub async fn my_proc_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    MY_PROC_NUMBER
        .scope(std::cell::Cell::new(INVALID_PROC_NUMBER), f)
        .await
}

/// `true` if this backend has a live PGPROC (PG `MyProc != NULL`).
pub fn has_my_proc() -> bool {
    current_proc_number() != INVALID_PROC_NUMBER
}

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

// === Function prototypes: rewired to the backend module (proc.c bodies) ===

pub use crate::backend::storage::lmgr::proc::{
    AuxiliaryPidGetProc, BecomeLockGroupLeader, BecomeLockGroupMember, CheckDeadLockAlert,
    GetLockHoldersAndWaiters, GetStartupBufferPinWaitBufId, HaveNFreeProcs, InitAuxiliaryProcess,
    InitProcGlobal, InitProcess, InitProcessPhase2, JoinWaitQueue, LockErrorCleanup,
    ProcGlobalSemas, ProcGlobalShmemSize, ProcKill, ProcLockWakeup, ProcReleaseLocks,
    ProcSendSignal, ProcSleep, ProcWaitForSignal, ProcWakeup, SetStartupBufferPinWaitBufId,
};
