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
use crate::utils::resowner::ResourceOwnerData;

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
        VirtualTransactionId {
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
        LOCKTAG {
            locktag_field1: f1,
            locktag_field2: f2,
            locktag_field3: f3,
            locktag_field4: f4,
            locktag_type: ty as u8,
            locktag_lockmethodid: method as u8,
        }
    }

    pub fn lockmethod(&self) -> LOCKMETHODID {
        self.locktag_lockmethodid as LOCKMETHODID
    }
}

/// Per-locked-object lock information. In-memory; the intrusive proclock/waitproc
/// lists become owned collections.
pub struct LOCK {
    pub tag: LOCKTAG,            // hash key
    pub grant_mask: LOCKMASK,    // lock types already granted
    pub wait_mask: LOCKMASK,     // lock types awaited
    pub proc_locks: Vec<PROCLOCK>, // PROCLOCKs assoc. with this lock (was dlist)
    pub wait_procs: Vec<*mut PGPROC>, // PGPROCs waiting on lock (was dclist) // TODO(ptr)
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
    pub group_leader: *mut PGPROC, // lock group leader, or proc itself // TODO(ptr)
    pub hold_mask: LOCKMASK,       // lock types currently held
    pub release_mask: LOCKMASK,    // lock types to be released (LockReleaseAll)
}

/// Key for a LOCALLOCK entry: lockable object + mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LOCALLOCKTAG {
    pub lock: LOCKTAG,
    pub mode: LOCKMODE,
}

/// Per-owner hold count for a local lock. `owner == None` -> held for the
/// session; otherwise held for the current transaction.
pub struct LOCALLOCKOWNER {
    pub owner: *mut ResourceOwnerData, // TODO(ptr); forward-decl above
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

// --- function prototypes (stub bodies) ---

pub fn lock_manager_shmem_init() {
    unimplemented!()
}
pub fn lock_manager_shmem_size() -> usize {
    unimplemented!()
}
pub fn init_lock_manager_access() {
    unimplemented!()
}
pub fn get_locks_method_table(_lock: &LOCK) -> LockMethod {
    unimplemented!()
}
pub fn get_lock_tags_method_table(_locktag: &LOCKTAG) -> LockMethod {
    unimplemented!()
}
pub fn lock_tag_hash_code(_locktag: &LOCKTAG) -> u32 {
    unimplemented!()
}
pub fn do_lock_modes_conflict(_mode1: LOCKMODE, _mode2: LOCKMODE) -> bool {
    unimplemented!()
}
pub fn lock_acquire(
    _locktag: &LOCKTAG,
    _lockmode: LOCKMODE,
    _session_lock: bool,
    _dont_wait: bool,
) -> LockAcquireResult {
    unimplemented!()
}
/// `LockAcquireExtended`: the `LOCALLOCK **locallockp` out-param is folded into
/// the return (function-mapping section 5).
pub fn lock_acquire_extended(
    _locktag: &LOCKTAG,
    _lockmode: LOCKMODE,
    _session_lock: bool,
    _dont_wait: bool,
    _report_memory_error: bool,
    _log_lock_failure: bool,
) -> (LockAcquireResult, Option<*mut LOCALLOCK>) {
    unimplemented!()
}
pub fn abort_strong_lock_acquire() {
    unimplemented!()
}
pub fn mark_lock_clear(_locallock: &mut LOCALLOCK) {
    unimplemented!()
}
pub fn lock_release(_locktag: &LOCKTAG, _lockmode: LOCKMODE, _session_lock: bool) -> bool {
    unimplemented!()
}
pub fn lock_release_all(_lockmethodid: LOCKMETHODID, _all_locks: bool) {
    unimplemented!()
}
pub fn lock_release_session(_lockmethodid: LOCKMETHODID) {
    unimplemented!()
}
pub fn lock_release_current_owner(_locallocks: &mut [*mut LOCALLOCK]) {
    unimplemented!()
}
pub fn lock_reassign_current_owner(_locallocks: &mut [*mut LOCALLOCK]) {
    unimplemented!()
}
pub fn lock_held_by_me(_locktag: &LOCKTAG, _lockmode: LOCKMODE, _orstronger: bool) -> bool {
    unimplemented!()
}
pub fn lock_has_waiters(_locktag: &LOCKTAG, _lockmode: LOCKMODE, _session_lock: bool) -> bool {
    unimplemented!()
}
/// `GetLockConflicts`: the `*countp` out-param vanishes (the `Vec` carries len).
pub fn get_lock_conflicts(_locktag: &LOCKTAG, _lockmode: LOCKMODE) -> Vec<VirtualTransactionId> {
    unimplemented!()
}
pub fn at_prepare_locks() {
    unimplemented!()
}
pub fn post_prepare_locks(_xid: TransactionId) {
    unimplemented!()
}
pub fn lock_check_conflicts(
    _lock_method_table: LockMethod,
    _lockmode: LOCKMODE,
    _lock: &mut LOCK,
    _proclock: &mut PROCLOCK,
) -> bool {
    unimplemented!()
}
pub fn grant_lock(_lock: &mut LOCK, _proclock: &mut PROCLOCK, _lockmode: LOCKMODE) {
    unimplemented!()
}
pub fn grant_awaited_lock() {
    unimplemented!()
}
pub fn get_awaited_lock() -> Option<*mut LOCALLOCK> {
    unimplemented!()
}
pub fn reset_awaited_lock() {
    unimplemented!()
}
pub fn remove_from_wait_queue(_proc: *mut PGPROC, _hashcode: u32) {
    unimplemented!()
}
pub fn get_lock_status_data() -> LockData {
    unimplemented!()
}
pub fn get_blocker_status_data(_blocked_pid: i32) -> BlockedProcsData {
    unimplemented!()
}
/// `GetRunningTransactionLocks`: the `*nlocks` out-param vanishes.
pub fn get_running_transaction_locks() -> Vec<xl_standby_lock> {
    unimplemented!()
}
pub fn get_lockmode_name(_lockmethodid: LOCKMETHODID, _mode: LOCKMODE) -> &'static str {
    unimplemented!()
}
pub fn lock_twophase_recover(_xid: TransactionId, _info: u16, _recdata: &[u8]) {
    unimplemented!()
}
pub fn lock_twophase_postcommit(_xid: TransactionId, _info: u16, _recdata: &[u8]) {
    unimplemented!()
}
pub fn lock_twophase_postabort(_xid: TransactionId, _info: u16, _recdata: &[u8]) {
    unimplemented!()
}
pub fn lock_twophase_standby_recover(_xid: TransactionId, _info: u16, _recdata: &[u8]) {
    unimplemented!()
}
pub fn dead_lock_check(_proc: *mut PGPROC) -> DeadLockState {
    unimplemented!()
}
pub fn get_blocking_autovacuum_pgproc() -> Option<*mut PGPROC> {
    unimplemented!()
}
/// `DeadLockReport` is `pg_noreturn` in C; it raises ERROR (panic per error model).
#[deprecated(note = "TODO(panic): migrate to Result + ?")]
pub fn dead_lock_report() -> ! {
    unimplemented!()
}
pub fn remember_simple_dead_lock(
    _proc1: *mut PGPROC,
    _lockmode: LOCKMODE,
    _lock: &mut LOCK,
    _proc2: *mut PGPROC,
) {
    unimplemented!()
}
pub fn init_dead_lock_checking() {
    unimplemented!()
}
pub fn lock_waiter_count(_locktag: &LOCKTAG) -> i32 {
    unimplemented!()
}

// Lock a VXID (used to wait for a transaction to finish).
pub fn virtual_xact_lock_table_insert(_vxid: VirtualTransactionId) {
    unimplemented!()
}
pub fn virtual_xact_lock_table_cleanup() {
    unimplemented!()
}
pub fn virtual_xact_lock(_vxid: VirtualTransactionId, _wait: bool) -> bool {
    unimplemented!()
}
