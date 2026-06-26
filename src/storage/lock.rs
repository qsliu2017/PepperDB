//! Translated from PostgreSQL src/include/storage/lock.h
//!
//! STUB. The heavyweight lock manager. PG keeps lock/proclock state in
//! partitioned shared-memory hash tables, blocks via `PGPROC` semaphores, and
//! runs a deadlock detector. Under the single-process async model this becomes
//! sharded in-process tables, a per-task proc, async waits, and an async-aware
//! deadlock detector. The key types and on-disk-ish `LOCKTAG` are translated;
//! the `Lock*` operations are stub signatures.
// TODO(lock-manager): sharded tables, per-task proc, async waits, deadlock detector

use crate::c::{LocalTransactionId, TransactionId};
use crate::datatype::timestamp::TimestampTz;
use crate::storage::lockdefs::{xl_standby_lock, LOCKMASK};
pub use crate::storage::proc::PGPROC;
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};
use crate::utils::resowner::ResourceOwner;

/// `LOCKMODE` is `typedef int` in C: an integer 1..N (0 = `NoLock`). Kept as a
/// plain int here because lock.h indexes arrays by it and compares it freely;
/// the named modes live in `lockdefs::LockMode`.
pub type LOCKMODE = i32;

// GUC variables.
// TODO(global): GUCs become session/global config under the async model.
pub static mut MAX_LOCKS_PER_XACT: i32 = 64;
pub static mut LOG_LOCK_FAILURES: bool = false;

/// Identifies a top-level transaction by proc number + local xid. Never stored
/// on disk (reused across restart / XID wraparound).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VirtualTransactionId {
    pub proc_number: ProcNumber,
    pub local_transaction_id: LocalTransactionId,
}

pub const INVALID_LOCAL_TRANSACTION_ID: LocalTransactionId = LocalTransactionId(0);

pub fn local_transaction_id_is_valid(lxid: LocalTransactionId) -> bool {
    lxid != INVALID_LOCAL_TRANSACTION_ID
}

impl VirtualTransactionId {
    pub fn is_valid(self) -> bool {
        local_transaction_id_is_valid(self.local_transaction_id)
    }
    pub const fn is_recovered_prepared_xact(self) -> bool {
        self.proc_number == INVALID_PROC_NUMBER
    }
    pub const fn invalid() -> Self {
        Self {
            proc_number: INVALID_PROC_NUMBER,
            local_transaction_id: INVALID_LOCAL_TRANSACTION_ID,
        }
    }
}

/// MAX_LOCKMODES cannot be larger than the # of bits in LOCKMASK.
pub const MAX_LOCKMODES: usize = 10;

pub const fn lockbit_on(lockmode: LOCKMODE) -> LOCKMASK {
    1 << lockmode
}
pub const fn lockbit_off(lockmode: LOCKMODE) -> LOCKMASK {
    !(1 << lockmode)
}

/// Locking semantics for a "lock method": conflict table and mode names. All
/// const tables in C. The `trace_flag` GUC pointer is dropped here.
pub struct LockMethodData {
    pub num_lock_modes: i32,
    pub conflict_tab: &'static [LOCKMASK],
    pub lock_mode_names: &'static [&'static str],
}

pub type LockMethod = &'static LockMethodData;

/// Lock methods are identified by LOCKMETHODID (constrained to 256 by LOCKTAG).
pub type LOCKMETHODID = u16;

pub const DEFAULT_LOCKMETHOD: LOCKMETHODID = 1;
pub const USER_LOCKMETHOD: LOCKMETHODID = 2;

/// The kinds of objects we can lock (up to 256). Sequential ordinal -> enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LockTagType {
    Relation = 0,            // whole relation
    RelationExtend,          // the right to extend a relation
    DatabaseFrozenIds,       // pg_database.datfrozenxid
    Page,                    // one page of a relation
    Tuple,                   // one physical tuple
    Transaction,             // wait for xact done
    VirtualTransaction,      // wait for virtual xact done
    SpeculativeToken,        // speculative insertion Xid and token
    Object,                  // non-relation database object
    UserLock,                // reserved for old contrib/userlock code
    Advisory,                // advisory user locks
    ApplyTransaction,        // xact being applied on a logical-repl subscriber
}

pub const LOCKTAG_LAST_TYPE: LockTagType = LockTagType::ApplyTransaction;

/// LOCKTAG: the lookup key for a lockable object. Deliberately laid out to fit
/// 16 bytes with no padding (on-disk-ish hash key); kept `#[repr(C)]` with the
/// raw fields. Set via the `set_*` accessors, not field-by-field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct LOCKTAG {
    pub locktag_field1: u32, // a 32-bit ID field
    pub locktag_field2: u32, // a 32-bit ID field
    pub locktag_field3: u32, // a 32-bit ID field
    pub locktag_field4: u16, // a 16-bit ID field
    pub locktag_type: u8,    // see LockTagType
    pub locktag_lockmethodid: u8,
}

const _: () = assert!(core::mem::size_of::<LOCKTAG>() == 16);
const _: () = assert!(core::mem::offset_of!(LOCKTAG, locktag_field4) == 12);
const _: () = assert!(core::mem::offset_of!(LOCKTAG, locktag_type) == 14);

impl LOCKTAG {
    /// SET_LOCKTAG_RELATION: DB OID + REL OID (DB OID = 0 if shared).
    pub fn set_relation(dboid: u32, reloid: u32) -> Self {
        Self::new(dboid, reloid, 0, 0, LockTagType::Relation, DEFAULT_LOCKMETHOD)
    }
    /// SET_LOCKTAG_RELATION_EXTEND: same ID info as RELATION.
    pub fn set_relation_extend(dboid: u32, reloid: u32) -> Self {
        Self::new(dboid, reloid, 0, 0, LockTagType::RelationExtend, DEFAULT_LOCKMETHOD)
    }
    /// SET_LOCKTAG_DATABASE_FROZEN_IDS: DB OID.
    pub fn set_database_frozen_ids(dboid: u32) -> Self {
        Self::new(dboid, 0, 0, 0, LockTagType::DatabaseFrozenIds, DEFAULT_LOCKMETHOD)
    }
    /// SET_LOCKTAG_PAGE: RELATION info + BlockNumber.
    pub fn set_page(dboid: u32, reloid: u32, blocknum: u32) -> Self {
        Self::new(dboid, reloid, blocknum, 0, LockTagType::Page, DEFAULT_LOCKMETHOD)
    }
    /// SET_LOCKTAG_TUPLE: PAGE info + OffsetNumber.
    pub fn set_tuple(dboid: u32, reloid: u32, blocknum: u32, offnum: u16) -> Self {
        Self::new(dboid, reloid, blocknum, offnum, LockTagType::Tuple, DEFAULT_LOCKMETHOD)
    }
    /// SET_LOCKTAG_TRANSACTION: a TransactionId.
    pub fn set_transaction(xid: u32) -> Self {
        Self::new(xid, 0, 0, 0, LockTagType::Transaction, DEFAULT_LOCKMETHOD)
    }
    /// SET_LOCKTAG_VIRTUALTRANSACTION: a VirtualTransactionId.
    pub fn set_virtual_transaction(vxid: VirtualTransactionId) -> Self {
        Self::new(
            vxid.proc_number as u32,
            vxid.local_transaction_id.0,
            0,
            0,
            LockTagType::VirtualTransaction,
            DEFAULT_LOCKMETHOD,
        )
    }
    /// SET_LOCKTAG_SPECULATIVE_INSERTION: TRANSACTION info + speculative counter.
    pub fn set_speculative_insertion(xid: u32, token: u32) -> Self {
        Self::new(xid, token, 0, 0, LockTagType::SpeculativeToken, DEFAULT_LOCKMETHOD)
    }
    /// SET_LOCKTAG_OBJECT: DB OID + CLASS OID + OBJECT OID + SUBID (16-bit).
    pub fn set_object(dboid: u32, classoid: u32, objoid: u32, objsubid: u16) -> Self {
        Self::new(dboid, classoid, objoid, objsubid, LockTagType::Object, DEFAULT_LOCKMETHOD)
    }
    /// SET_LOCKTAG_ADVISORY: four user IDs, via USER_LOCKMETHOD.
    pub fn set_advisory(id1: u32, id2: u32, id3: u32, id4: u16) -> Self {
        Self::new(id1, id2, id3, id4, LockTagType::Advisory, USER_LOCKMETHOD)
    }
    /// SET_LOCKTAG_APPLY_TRANSACTION: DB OID + SUB OID + XID + OBJID.
    pub fn set_apply_transaction(dboid: u32, suboid: u32, xid: u32, objid: u16) -> Self {
        Self::new(dboid, suboid, xid, objid, LockTagType::ApplyTransaction, DEFAULT_LOCKMETHOD)
    }

    fn new(f1: u32, f2: u32, f3: u32, f4: u16, ty: LockTagType, method: LOCKMETHODID) -> Self {
        Self {
            locktag_field1: f1,
            locktag_field2: f2,
            locktag_field3: f3,
            locktag_field4: f4,
            locktag_type: ty as u8,
            locktag_lockmethodid: method as u8,
        }
    }

    pub fn lockmethod(&self) -> LOCKMETHODID {
        LOCKMETHODID::from(self.locktag_lockmethodid)
    }
}

/// Per-locked-object lock information. In-memory; the intrusive proclock/waitproc
/// lists become owned collections.
pub struct LOCK {
    pub tag: LOCKTAG,            // hash key
    pub grant_mask: LOCKMASK,    // lock types already granted
    pub wait_mask: LOCKMASK,     // lock types awaited
    pub proc_locks: Vec<PROCLOCK>, // PROCLOCKs assoc. with this lock (was dlist)
    pub wait_procs: Vec<ProcNumber>, // PGPROCs waiting on lock, in queue order (was dclist)
    pub requested: [i32; MAX_LOCKMODES],
    pub n_requested: i32,
    pub granted: [i32; MAX_LOCKMODES],
    pub n_granted: i32,
}

/// Key for a PROCLOCK: which lock + which proc. C uses pointers (only unique for
/// the proclock's lifespan).
pub struct PROCLOCKTAG {
    pub lock: *mut LOCK,   // link to per-lockable-object information // TODO(ptr)
    pub proc: *mut PGPROC, // link to PGPROC of owning backend // TODO(ptr)
}

/// Per-holder/waiter info for a lockable object. Intrusive list links drop out
/// (the lists become owned collections on `LOCK`/`PGPROC`).
pub struct PROCLOCK {
    pub tag: PROCLOCKTAG,
    pub group_leader: ProcNumber, // lock group leader's ProcNumber, or own
    pub hold_mask: LOCKMASK,       // lock types currently held
    pub release_mask: LOCKMASK,    // lock types to be released (LockReleaseAll)
}

/// Key for a LOCALLOCK entry: lockable object + mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LOCALLOCKTAG {
    pub lock: LOCKTAG,
    pub mode: LOCKMODE,
}

/// Per-owner hold count for a local lock. `owner == None` -> held for the
/// session; otherwise held for the current transaction.
pub struct LOCALLOCKOWNER {
    pub owner: Option<ResourceOwner>, // None -> session lock; else txn-scoped
    pub n_locks: i64,
}

/// Backend-local record of a lock it is interested in (acquire-count cache, plus
/// per-ResourceOwner accounting). `lock`/`proclock` are NULL for fast-path locks
/// and are garbage when `n_locks == 0`.
pub struct LOCALLOCK {
    pub tag: LOCALLOCKTAG,
    pub hashcode: u32,           // copy of LOCKTAG's hash value
    pub lock: *mut LOCK,         // associated LOCK, if any // TODO(ptr)
    pub proclock: *mut PROCLOCK, // associated PROCLOCK, if any // TODO(ptr)
    pub n_locks: i64,            // total times lock is held
    pub lock_owners: Vec<LOCALLOCKOWNER>, // was a resizable array + counts
    pub holds_strong_lock_count: bool,
    pub lock_cleared: bool,      // we read all sinval msgs for lock
}

/// Per-PROCLOCK row for the lock-listing user functions (lockfuncs.c).
pub struct LockInstanceData {
    pub locktag: LOCKTAG,
    pub hold_mask: LOCKMASK,
    pub wait_lock_mode: LOCKMODE,
    pub vxid: VirtualTransactionId,
    pub wait_start: TimestampTz,
    pub pid: i32,
    pub leader_pid: i32, // group leader pid; = pid if no group
    pub fastpath: bool,
}

/// Result of `GetLockStatusData`: C returned `{nelements, *locks}`; in Rust the
/// array is just the `Vec`.
pub struct LockData {
    pub locks: Vec<LockInstanceData>,
}

/// Per-blocked-proc info; the index fields refer into `BlockedProcsData`.
pub struct BlockedProcData {
    pub pid: i32,
    pub first_lock: i32,
    pub num_locks: i32,
    pub first_waiter: i32,
    pub num_waiters: i32,
}

/// Result of `GetBlockerStatusData`: the C struct kept parallel arrays with
/// explicit lengths; in Rust those are `Vec`s.
pub struct BlockedProcsData {
    pub procs: Vec<BlockedProcData>,
    pub locks: Vec<LockInstanceData>,
    pub waiter_pids: Vec<i32>,
}

/// Result codes for LockAcquire().
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockAcquireResult {
    NotAvail,     // lock not available, and dontWait=true
    Ok,           // lock successfully acquired
    AlreadyHeld,  // incremented count for lock already held
    AlreadyClear, // incremented count for lock already clear
}

/// Deadlock states identified by DeadLockCheck().
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadLockState {
    NotYetChecked,
    NoDeadlock,
    SoftDeadlock,        // avoided by queue rearrangement
    HardDeadlock,        // no way out but ERROR
    BlockedByAutovacuum, // queue blocked by autovacuum worker
}

// --- function prototypes: rewired to the backend module (lock.c bodies) ---
//
// The lock.c definitions live in `crate::backend::storage::lmgr::lock` (15b).
// The header re-exports them under both the C name (PascalCase) and the
// snake_case names plan-001 used / that proc.c (15a) calls. The LockManager
// shared type + the per-task LOCALLOCK scope are exported too.

pub use crate::backend::storage::lmgr::lock::{
    AbortStrongLockAcquire, AtPrepare_Locks, DoLockModesConflict, GetLockConflicts,
    GetLockTagsMethodTable, GetLocksMethodTable, GetLockmodeName, GetRunningTransactionLocks,
    GrantAwaitedLock, InitLockManagerAccess, LockAcquire, LockAcquireExtended, LockCheckConflicts,
    LockHasWaiters, LockHeldByMe, LockManager, LockManagerShmemInit, LockManagerShmemSize,
    LockRelease, LockReleaseAll, LockReleaseSession, LockTagHashCode, LockWaiterCount, MarkLockClear,
    PostPrepare_Locks, VirtualXactLock, VirtualXactLockTableCleanup, VirtualXactLockTableInsert,
    local_lock_scope, lock_manager, lock_manager_shared,
};

// snake_case aliases (plan-001 names + the ones proc.c calls):
pub use crate::backend::storage::lmgr::lock::{
    AbortStrongLockAcquire as abort_strong_lock_acquire, GetLockConflicts as get_lock_conflicts,
    GetLockmodeName as get_lockmode_name, GrantLock as grant_lock,
    LockCheckConflicts as lock_check_conflicts, LockManagerShmemInit as lock_manager_shmem_init,
    LockManagerShmemSize as lock_manager_shmem_size, LockTagHashCode as lock_tag_hash_code,
    LockWaiterCount as lock_waiter_count, MarkLockClear as mark_lock_clear,
    RememberSimpleDeadLock as remember_simple_dead_lock, ResetAwaitedLock as reset_awaited_lock,
    lock_twophase_postabort, lock_twophase_postcommit, lock_twophase_recover,
    lock_twophase_standby_recover,
};

/// `GrantLock` (PG name); the backend export. Re-exported for the C name.
pub use crate::backend::storage::lmgr::lock::GrantLock;
/// `RemoveFromWaitQueue` (PG name); pulls a waiter out on deadlock/abort.
pub use crate::backend::storage::lmgr::lock::RemoveFromWaitQueue;
/// `RememberSimpleDeadLock` (PG name).
pub use crate::backend::storage::lmgr::lock::RememberSimpleDeadLock;
/// `GetAwaitedLock` (PG name) + its snake alias.
pub use crate::backend::storage::lmgr::lock::GetAwaitedLock;
pub use crate::backend::storage::lmgr::lock::GetAwaitedLock as get_awaited_lock;
/// `ResetAwaitedLock` (PG name).
pub use crate::backend::storage::lmgr::lock::ResetAwaitedLock;

// --- still-stubbed: the lockfuncs/pg_locks status views ---

/// `GetLockStatusData` (pg_locks). TODO(lockfuncs).
pub use crate::backend::storage::lmgr::lock::GetLockStatusData;
pub use crate::backend::storage::lmgr::lock::GetLockStatusData as get_lock_status_data;
/// `GetBlockerStatusData`. TODO(lockfuncs).
pub use crate::backend::storage::lmgr::lock::GetBlockerStatusData;
pub use crate::backend::storage::lmgr::lock::GetBlockerStatusData as get_blocker_status_data;

// --- deadlock.c (15c): the waits-for-graph detector ---
//
// The bodies live in `crate::backend::storage::lmgr::deadlock`; they operate over
// `ProcNumber` (the arena identity), called by CheckDeadLock (proc.c) with all
// partition locks held.

pub use crate::backend::storage::lmgr::deadlock::DeadLockCheck;
pub use crate::backend::storage::lmgr::deadlock::DeadLockCheck as dead_lock_check;
pub use crate::backend::storage::lmgr::deadlock::GetBlockingAutoVacuumPgproc;
pub use crate::backend::storage::lmgr::deadlock::GetBlockingAutoVacuumPgproc as get_blocking_autovacuum_pgproc;
pub use crate::backend::storage::lmgr::deadlock::InitDeadLockChecking;
pub use crate::backend::storage::lmgr::deadlock::InitDeadLockChecking as init_dead_lock_checking;
pub use crate::backend::storage::lmgr::deadlock::DeadLockReport;
pub use crate::backend::storage::lmgr::deadlock::DeadLockReport as dead_lock_report;
