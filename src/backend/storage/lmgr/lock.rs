//! The POSTGRES primary (heavyweight) lock mechanism. Translated from backend/storage/lmgr/lock.c.
//!
//! This is the regular lock manager: a lock table keyed by `LOCKTAG`, the
//! conflict-mode semantics for the standard lock methods, and the principal
//! entry points `LockAcquire`, `LockRelease`, `LockReleaseAll`,
//! `LockCheckConflicts`, and `GrantLock`. When a process tries to acquire a
//! lock of a type that conflicts with existing locks, it is put to sleep using
//! the routines in storage/lmgr/proc. For the most part this code is invoked
//! through lmgr or another lock-management module rather than directly. The
//! large lock structures (`LOCK`, `PROCLOCK`, `LOCALLOCK`, `LOCKTAG`,
//! `LockAcquireResult`) are defined in `crate::storage::lock`.
//!
//! In PepperDB the shared-memory hash tables become process-internal state.
//! The `LOCK` and `PROCLOCK` tables are split into `NUM_LOCK_PARTITIONS`
//! shards, each a `parking_lot::Mutex<LockShard>` that subsumes the per-
//! partition LWLock of the C original; the partition for a tag is its hash
//! modulo the partition count. Each shard holds boxed `LOCK` and `PROCLOCK`
//! entries so that the raw pointers held by proc and by the per-task
//! `LOCALLOCK` remain stable for the lifetime of the entry. The `LOCALLOCK`
//! table and the fast-path local-use counts are per-backend rather than
//! per-process: they live in a tokio task-local cell and must not be borrowed
//! across an await. The per-backend fast-path arrays remain on the `PGPROC`,
//! guarded by that proc's fast-path lock.
//!
//! Lock acquisition is asynchronous. The grant path runs synchronously under
//! the partition shard mutex; on the wait path the shard mutex is released
//! before sleeping on the proc-wait future, so no shard lock is held across an
//! await. Release paths run synchronously under the shard mutex. Deadlock
//! detection and the higher-level lmgr wrappers live in their own modules, and
//! lock groups are treated as single-member.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

use crate::postgres_ext::{InvalidOid, Oid};
use crate::storage::lock::{
    DEFAULT_LOCKMETHOD, LOCALLOCK, LOCALLOCKOWNER, LOCALLOCKTAG, LOCK, LOCKMETHODID, LOCKMODE,
    LOCKTAG, LockAcquireResult, LockMethod, LockMethodData, LockTagType, MAX_LOCKMODES, PROCLOCK,
    USER_LOCKMETHOD, VirtualTransactionId, lockbit_off, lockbit_on,
};
use crate::storage::lockdefs::{LOCKMASK, LockMode, MAX_LOCK_MODE, xl_standby_lock};
use crate::storage::proc::{
    NUM_LOCK_PARTITIONS, PGPROC, ProcGlobal, ProcWaitStatus, current_proc_number,
};
use crate::storage::procnumber::{INVALID_PROC_NUMBER, ProcNumber};
use crate::utils::resowner::ResourceOwner;

// ---------------------------------------------------------------------------
// Lock method tables (the conflict tables; const in C)
// ---------------------------------------------------------------------------

/// `LOCKBIT_ON(mode)` as a const expression (the header `lockbit_on` is a const
/// fn but closures aren't const-callable, so we use it directly per entry).
const fn on(m: LockMode) -> LOCKMASK {
    lockbit_on(m as LOCKMODE)
}

/// PG `LockConflicts[]`: for each lock mode, the bitmask of modes it conflicts
/// with. Index 0 is unused (NoLock).
const LOCK_CONFLICTS: [LOCKMASK; MAX_LOCKMODES] = {
    use LockMode::{AccessShareLock, AccessExclusiveLock, RowShareLock, ExclusiveLock, RowExclusiveLock, ShareLock, ShareRowExclusiveLock, ShareUpdateExclusiveLock};
    let mut t = [0i32; MAX_LOCKMODES];
    t[AccessShareLock as usize] = on(AccessExclusiveLock);
    t[RowShareLock as usize] = on(ExclusiveLock) | on(AccessExclusiveLock);
    t[RowExclusiveLock as usize] = on(ShareLock)
        | on(ShareRowExclusiveLock)
        | on(ExclusiveLock)
        | on(AccessExclusiveLock);
    t[ShareUpdateExclusiveLock as usize] = on(ShareUpdateExclusiveLock)
        | on(ShareLock)
        | on(ShareRowExclusiveLock)
        | on(ExclusiveLock)
        | on(AccessExclusiveLock);
    t[ShareLock as usize] = on(RowExclusiveLock)
        | on(ShareUpdateExclusiveLock)
        | on(ShareRowExclusiveLock)
        | on(ExclusiveLock)
        | on(AccessExclusiveLock);
    t[ShareRowExclusiveLock as usize] = on(RowExclusiveLock)
        | on(ShareUpdateExclusiveLock)
        | on(ShareLock)
        | on(ShareRowExclusiveLock)
        | on(ExclusiveLock)
        | on(AccessExclusiveLock);
    t[ExclusiveLock as usize] = on(RowShareLock)
        | on(RowExclusiveLock)
        | on(ShareUpdateExclusiveLock)
        | on(ShareLock)
        | on(ShareRowExclusiveLock)
        | on(ExclusiveLock)
        | on(AccessExclusiveLock);
    t[AccessExclusiveLock as usize] = on(AccessShareLock)
        | on(RowShareLock)
        | on(RowExclusiveLock)
        | on(ShareUpdateExclusiveLock)
        | on(ShareLock)
        | on(ShareRowExclusiveLock)
        | on(ExclusiveLock)
        | on(AccessExclusiveLock);
    t
};

const LOCK_MODE_NAMES: [&str; 9] = [
    "INVALID",
    "AccessShareLock",
    "RowShareLock",
    "RowExclusiveLock",
    "ShareUpdateExclusiveLock",
    "ShareLock",
    "ShareRowExclusiveLock",
    "ExclusiveLock",
    "AccessExclusiveLock",
];

static DEFAULT_LOCK_METHOD: LockMethodData = LockMethodData {
    num_lock_modes: MAX_LOCK_MODE,
    conflict_tab: &LOCK_CONFLICTS,
    lock_mode_names: &LOCK_MODE_NAMES,
};

static USER_LOCK_METHOD: LockMethodData = LockMethodData {
    num_lock_modes: MAX_LOCK_MODE,
    conflict_tab: &LOCK_CONFLICTS,
    lock_mode_names: &LOCK_MODE_NAMES,
};

/// PG `LockMethods[]`: index by LOCKMETHODID (1 = default, 2 = user).
fn lock_methods(id: LOCKMETHODID) -> Option<LockMethod> {
    match id {
        DEFAULT_LOCKMETHOD => Some(&DEFAULT_LOCK_METHOD),
        USER_LOCKMETHOD => Some(&USER_LOCK_METHOD),
        _ => None,
    }
}

/// PG `GetLocksMethodTable`.
pub fn GetLocksMethodTable(lock: &LOCK) -> LockMethod {
    lock_methods(lock.tag.lockmethod()).expect("unrecognized lock method")
}

/// PG `GetLockTagsMethodTable`.
pub fn GetLockTagsMethodTable(locktag: &LOCKTAG) -> LockMethod {
    lock_methods(locktag.lockmethod()).expect("unrecognized lock method")
}

/// PG `GetLockmodeName`.
pub fn GetLockmodeName(lockmethodid: LOCKMETHODID, mode: LOCKMODE) -> &'static str {
    let table = lock_methods(lockmethodid).expect("unrecognized lock method");
    table.lock_mode_names[mode as usize]
}

/// PG `DoLockModesConflict`.
pub fn DoLockModesConflict(mode1: LOCKMODE, mode2: LOCKMODE) -> bool {
    let table = &DEFAULT_LOCK_METHOD;
    (table.conflict_tab[mode1 as usize] & lockbit_on(mode2)) != 0
}

// ---------------------------------------------------------------------------
// Hash codes + partitioning
// ---------------------------------------------------------------------------

/// PG `LockTagHashCode`. The low `LOG2_NUM_LOCK_PARTITIONS` bits select the
/// partition (PG relies on dynahash putting the partition in the low bits; we
/// reproduce that by using the same hash here and masking).
pub fn LockTagHashCode(locktag: &LOCKTAG) -> u32 {
    let mut h = DefaultHasher::new();
    locktag.hash(&mut h);
    h.finish() as u32
}

/// Partition index for a hashcode (PG `LockHashPartition`).
fn lock_hash_partition(hashcode: u32) -> usize {
    (hashcode as usize) % NUM_LOCK_PARTITIONS
}

/// PG `FAST_PATH_STRONG_LOCK_HASH_PARTITIONS`.
const FAST_PATH_STRONG_LOCK_HASH_BITS: u32 = 10;
const FAST_PATH_STRONG_LOCK_HASH_PARTITIONS: usize = 1 << FAST_PATH_STRONG_LOCK_HASH_BITS;
fn fast_path_strong_lock_hash_partition(hashcode: u32) -> usize {
    (hashcode as usize) % FAST_PATH_STRONG_LOCK_HASH_PARTITIONS
}

// ---------------------------------------------------------------------------
// Fast-path layout (lock.c macros)
// ---------------------------------------------------------------------------

const FP_LOCK_SLOTS_PER_GROUP: u32 = crate::storage::proc::FP_LOCK_SLOTS_PER_GROUP as u32;
const FAST_PATH_BITS_PER_SLOT: u32 = 3;
const FAST_PATH_LOCKNUMBER_OFFSET: u32 = 1;
const FAST_PATH_MASK: u64 = (1 << FAST_PATH_BITS_PER_SLOT) - 1;

fn fast_path_groups() -> u32 {
    // PG `FastPathLockGroupsPerBackend` (power of two). The GUC machinery is not
    // wired; PG defaults max_locks_per_xact=64 -> 4 groups. TODO(guc).
    let g = unsafe { crate::storage::proc::FastPathLockGroupsPerBackend };
    if g > 0 { g as u32 } else { 4 }
}

fn fast_path_slots_per_backend() -> u32 {
    FP_LOCK_SLOTS_PER_GROUP * fast_path_groups()
}

/// PG `FAST_PATH_REL_GROUP`.
fn fast_path_rel_group(relid: Oid) -> u32 {
    ((u64::from(relid.0).wrapping_mul(49157)) & (u64::from(fast_path_groups()) - 1)) as u32
}

/// PG `FAST_PATH_SLOT(group, index)`.
fn fast_path_slot(group: u32, index: u32) -> u32 {
    group * FP_LOCK_SLOTS_PER_GROUP + index
}

fn fast_path_group_of(slot: u32) -> u32 {
    slot / FP_LOCK_SLOTS_PER_GROUP
}
fn fast_path_index_of(slot: u32) -> u32 {
    slot % FP_LOCK_SLOTS_PER_GROUP
}

/// The 3-bit mode mask of fast-path slot `n` (PG `FAST_PATH_GET_BITS`).
fn fast_path_get_bits(proc: &PGPROC, n: u32) -> u64 {
    let word = proc.fp_lock_bits[fast_path_group_of(n) as usize];
    (word >> (FAST_PATH_BITS_PER_SLOT * fast_path_index_of(n))) & FAST_PATH_MASK
}

/// Bit position of mode `l` in fast-path slot `n` (PG `FAST_PATH_BIT_POSITION`).
fn fast_path_bit_position(n: u32, l: LOCKMODE) -> u32 {
    (l as u32 - FAST_PATH_LOCKNUMBER_OFFSET) + FAST_PATH_BITS_PER_SLOT * fast_path_index_of(n)
}

fn fast_path_set_lockmode(proc: &mut PGPROC, n: u32, l: LOCKMODE) {
    proc.fp_lock_bits[fast_path_group_of(n) as usize] |= 1u64 << fast_path_bit_position(n, l);
}
fn fast_path_clear_lockmode(proc: &mut PGPROC, n: u32, l: LOCKMODE) {
    proc.fp_lock_bits[fast_path_group_of(n) as usize] &= !(1u64 << fast_path_bit_position(n, l));
}
fn fast_path_check_lockmode(proc: &PGPROC, n: u32, l: LOCKMODE) -> bool {
    (proc.fp_lock_bits[fast_path_group_of(n) as usize] & (1u64 << fast_path_bit_position(n, l))) != 0
}

/// PG `EligibleForRelationFastPath`. `MyDatabaseId` is the connected DB OID
/// (session-local).
fn eligible_for_relation_fast_path(locktag: &LOCKTAG, mode: LOCKMODE) -> bool {
    let mydb = my_database_id();
    locktag.locktag_lockmethodid == DEFAULT_LOCKMETHOD as u8
        && locktag.locktag_type == LockTagType::Relation as u8
        && Oid(locktag.locktag_field1) == mydb
        && mydb != InvalidOid
        && mode < LockMode::ShareUpdateExclusiveLock as LOCKMODE
}

/// PG `ConflictsWithRelationFastPath`.
fn conflicts_with_relation_fast_path(locktag: &LOCKTAG, mode: LOCKMODE) -> bool {
    locktag.locktag_lockmethodid == DEFAULT_LOCKMETHOD as u8
        && locktag.locktag_type == LockTagType::Relation as u8
        && Oid(locktag.locktag_field1) != InvalidOid
        && mode > LockMode::ShareUpdateExclusiveLock as LOCKMODE
}

fn my_database_id() -> Oid {
    crate::session::try_current()
        .map_or(InvalidOid, |s| s.database_id())
}

/// Run `f` with `&mut PGPROC` for `procno` while holding that proc's fast-path
/// lock (PG fpInfoLock). The guard is taken via a shared cell ref and the `&mut`
/// via the arena's UnsafeCell, so the two borrows don't alias the same `proc`
/// binding -- soundness comes from the held guard serializing fast-path access.
fn with_fp_locked<R>(procno: ProcNumber, f: impl FnOnce(&mut PGPROC) -> R) -> Option<R> {
    let g = ProcGlobal::get()?;
    let cell = g.cell(procno)?;
    // SAFETY: shared ref to the slot only to reach the Mutex; the Mutex is
    // internally synchronized.
    let guard = unsafe { cell.get().fp_info_lock.lock() };
    // SAFETY: the fpInfoLock guard serializes all fast-path access to this slot;
    // no other task forms a &mut to the fast-path fields while we hold it.
    let proc = unsafe { cell.get_mut() };
    let r = f(proc);
    drop(guard);
    Some(r)
}

// ---------------------------------------------------------------------------
// The sharded lock tables (LockManager)
// ---------------------------------------------------------------------------

/// Logical PROCLOCK key (PG `PROCLOCKTAG` keyed by the LOCK tag + owning proc).
/// The header `PROCLOCKTAG` stores raw pointers (only unique for the proclock's
/// lifetime); for the HashMap we key on the stable logical identity instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ProcLockKey {
    lock: LOCKTAG,
    proc: ProcNumber,
}

/// One lock partition's data (PG's slice of `LockMethodLockHash` +
/// `LockMethodProcLockHash`). Boxed entries keep stable heap addresses for the
/// raw pointers proc.c (15a) holds.
pub struct LockShard {
    locks: HashMap<LOCKTAG, Box<LOCK>>,
    proclocks: HashMap<ProcLockKey, Box<PROCLOCK>>,
}

impl LockShard {
    fn new() -> Self {
        Self {
            locks: HashMap::new(),
            proclocks: HashMap::new(),
        }
    }
}

/// PG `FastPathStrongRelationLockData`: per-partition strong-locker counts that
/// gate fast-path use. The spinlock becomes the wrapping `Mutex`.
struct FastPathStrongLocks {
    count: [u32; FAST_PATH_STRONG_LOCK_HASH_PARTITIONS],
}

/// The heavyweight lock manager: the sharded tables + the strong-lock counts.
/// On `SharedState` as `Arc<LockManager>` (the ex shared-memory hash tables).
#[pepperdb_derive::process_global]
pub struct LockManager {
    shards: Vec<Mutex<LockShard>>,
    strong: Mutex<FastPathStrongLocks>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            shards: (0..NUM_LOCK_PARTITIONS).map(|_| Mutex::new(LockShard::new())).collect(),
            strong: Mutex::new(FastPathStrongLocks {
                count: [0; FAST_PATH_STRONG_LOCK_HASH_PARTITIONS],
            }),
        }
    }

    fn shard(&self, hashcode: u32) -> &Mutex<LockShard> {
        &self.shards[lock_hash_partition(hashcode)]
    }

    /// Run `f` with ALL `NUM_LOCK_PARTITIONS` partition Mutexes held, acquired in
    /// index order and released in reverse (PG `CheckDeadLock` / the deadlock
    /// detector hold every partition lock). The closure receives a read view over
    /// the locked tables (`LockTablesView`) so the deadlock detector can enumerate
    /// a lock's holders/waiters without re-locking (std::Mutex is not reentrant).
    /// The closure is SYNC -- no `.await` may occur while the guards are held
    /// (rules s5).
    pub fn with_all_partitions_locked<R>(&self, f: impl FnOnce(&LockTablesView) -> R) -> R {
        let mut guards: Vec<_> = self.shards.iter().map(|s| s.lock()).collect();
        // Raw `*mut LockShard` per partition, taken once from each held guard. Sound
        // because the guards live (and exclusively hold every partition Mutex) for
        // the whole `f` call; the view is the sole accessor. Lets the deadlock
        // give-up path mutate a shard (orphan-PROCLOCK GC) under the all-held locks.
        let shards: Vec<*mut LockShard> =
            guards.iter_mut().map(|g| &raw mut **g).collect();
        let view = LockTablesView { shards };
        let r = f(&view);
        // Release in reverse index order (PG releases the partition LWLocks high
        // to low). Drop the view's borrow, then pop guards high-to-low.
        drop(view);
        while guards.pop().is_some() {}
        r
    }

    /// PG `LockHashPartitionLockByProc(leader)`: run `f` holding the single
    /// partition Mutex that guards `leader`'s lock-group fields (lockGroupLeader /
    /// lockGroupMembers). The partition is `leader_procno % NUM_LOCK_PARTITIONS`,
    /// chosen by the leader's ProcNumber alone (not its contents), so the right
    /// lock is taken even if the leader PGPROC is being recycled. The deadlock
    /// detector holds every partition Mutex via `with_all_partitions_locked`, so
    /// it reads those group fields safely without extra locking. SYNC; no `.await`
    /// while the guard is held (rules s5).
    pub fn with_proc_partition_locked<R>(&self, procno: ProcNumber, f: impl FnOnce() -> R) -> R {
        let idx = (procno as usize) % NUM_LOCK_PARTITIONS;
        let _guard = self.shards[idx].lock();
        f()
    }
}

/// A read view over the lock tables with ALL partition Mutexes held (built by
/// `with_all_partitions_locked`). The deadlock detector uses it to enumerate a
/// lock's holders + waiters by tag without re-acquiring the partition Mutex.
pub struct LockTablesView {
    shards: Vec<*mut LockShard>,
}

impl LockTablesView {
    fn shard_for(&self, locktag: &LOCKTAG) -> &LockShard {
        let hashcode = LockTagHashCode(locktag);
        // SAFETY: the partition Mutex is held for this view's lifetime; the boxed
        // shard is alive and exclusively owned, so the shared ref does not alias.
        unsafe { &*self.shards[lock_hash_partition(hashcode)] }
    }

    /// The procs holding any lock mode on `locktag`, with their hold masks (PG
    /// walks `lock->procLocks`). 15b keys PROCLOCKs by `(tag, proc)` in the shard.
    pub fn holders_of(&self, locktag: &LOCKTAG) -> Vec<(ProcNumber, LOCKMASK)> {
        let shard = self.shard_for(locktag);
        shard
            .proclocks
            .iter()
            .filter(|(k, _)| k.lock == *locktag)
            .map(|(k, pl)| (k.proc, pl.hold_mask))
            .collect()
    }

    /// `&mut LockShard` for `locktag`'s partition while ALL partitions are held.
    /// SAFETY note: `with_all_partitions_locked` holds every partition Mutex
    /// exclusively for the lifetime of this view, so no other task can touch any
    /// shard; the immutable `&MutexGuard` we hold is the sole live reference, so
    /// forming a `&mut LockShard` through it does not alias. Used by the deadlock
    /// give-up path (RemoveFromWaitQueue) to GC the orphan PROCLOCK / empty LOCK.
    #[allow(
        clippy::mut_from_ref,
        reason = "all partitions held by with_all_partitions_locked; sole live reference, no alias"
    )]
    fn shard_for_mut(&self, locktag: &LOCKTAG) -> &mut LockShard {
        let hashcode = LockTagHashCode(locktag);
        let p = self.shards[lock_hash_partition(hashcode)];
        // SAFETY: the partition Mutex is held for this view's lifetime and the view
        // is the sole accessor; the give-up path runs a single bounded mutation
        // (no overlapping &mut to the same shard is taken).
        unsafe { &mut *p }
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: LockManager holds boxed LOCK/PROCLOCK whose `PROCLOCKTAG` carries raw
// `*mut LOCK`/`*mut PGPROC` (which make them `!Send`/`!Sync` by default). Those
// pointers reference the same shards' boxed entries and the process-lifetime
// PGPROC arena; every access to a boxed entry is serialized by the entry's
// partition shard `Mutex` (and the arena's own locking, design step15 s0). The
// shards/arena are never moved, so the pointers stay valid. The LockManager is
// shared by `Arc` across the tokio multi-thread runtime, so it must be
// Send + Sync; that is sound given the partition-Mutex discipline. Mirrors the
// `unsafe impl` on `ProcCell`/`ProcGlobal`.
#[allow(
    clippy::non_send_fields_in_send_ty,
    reason = "raw ptrs in shards' boxed entries gated by per-shard partition Mutex; see SAFETY"
)]
unsafe impl Send for LockManager {}
unsafe impl Sync for LockManager {}

/// PG `LockManagerShmemInit`: build + publish the lock tables. `SharedState::new`
/// also constructs one (so a SharedState is self-contained) and publishes the
/// same handle for the process-wide accessor.
pub fn LockManagerShmemInit() {
    if LockManager::get().is_some() {
        return;
    }
    LockManager::set(Arc::new(LockManager::new()));
}

/// Build a fresh `Arc<LockManager>` for `SharedState::new`, also publishing it
/// process-wide if none is published yet.
pub fn lock_manager_shared() -> Arc<LockManager> {
    let m = Arc::new(LockManager::new());
    LockManager::set(m.clone());
    m
}

/// PG `LockManagerShmemSize`: bytes the shmem hash tables would occupy. No
/// segment is allocated under the Arc model; this is an estimate.
pub fn LockManagerShmemSize() -> usize {
    let n = nlockents();
    n * std::mem::size_of::<LOCK>() + 2 * n * std::mem::size_of::<PROCLOCK>()
}

/// PG `NLOCKENTS()` = max_locks_per_xact * (MaxBackends + max_prepared_xacts).
fn nlockents() -> usize {
    let per = unsafe { crate::storage::lock::MAX_LOCKS_PER_XACT }.max(1) as usize;
    let backends = unsafe { crate::miscadmin::MaxBackends }.max(1) as usize;
    per * backends
}

/// PG `InitLockManagerAccess`: build the backend-private LOCALLOCK table. Under
/// the async model this is the per-task `task_local`; nothing global to do.
pub fn InitLockManagerAccess() {}

// ---------------------------------------------------------------------------
// Per-task LOCALLOCK table + fast-path local-use counts (rules s6.1)
// ---------------------------------------------------------------------------

/// PG's backend-private `LockMethodLocalHash` + `FastPathLocalUseCounts` +
/// `StrongLockInProgress`/`awaitedLock`. All per-backend, held across `.await`,
/// so a tokio `task_local` (never `thread_local`).
struct LocalLockTable {
    locks: HashMap<LOCALLOCKTAG, Box<LOCALLOCK>>,
    /// PG `FastPathLocalUseCounts[group]`.
    fast_path_use: Vec<u32>,
    /// PG `StrongLockInProgress` (the LOCALLOCK tag of an in-progress strong
    /// acquire, for error cleanup).
    strong_in_progress: Option<LOCALLOCKTAG>,
    /// PG `awaitedLock`/`awaitedOwner`: the lock we are currently WaitOnLock'ing.
    awaited_lock: Option<LOCALLOCKTAG>,
    awaited_owner: Option<ResourceOwner>,
    /// Stand-in for PG `CurrentResourceOwner` until the resowner subsystem is
    /// wired to lock accounting (TODO(15d/resowner)). A non-None owner marks a
    /// lock as transaction-scoped (vs session-scoped, owner == None), so
    /// LockReleaseAll(allLocks=false) can release xact locks while keeping
    /// session locks -- matching PG's owner==NULL distinction.
    current_owner: ResourceOwner,
}

impl LocalLockTable {
    fn new() -> Self {
        Self {
            locks: HashMap::new(),
            fast_path_use: vec![0; fast_path_groups() as usize],
            strong_in_progress: None,
            awaited_lock: None,
            awaited_owner: None,
            current_owner: ResourceOwner::create(None, "LockOwner"),
        }
    }
}

// SAFETY: LocalLockTable holds `LOCALLOCK`s whose `lock`/`proclock` are raw
// `*mut LOCK`/`*mut PROCLOCK` into the shared shards (making LOCALLOCK `!Send`).
// The table is per-backend state held across `.await`, and backends run on the
// tokio multi-thread runtime (a task may migrate threads at an await point), so
// the table MUST be Send (rules s6.1). It is sound because the raw pointers are
// only ever DEREFERENCED by the owning task while holding the target entry's
// partition shard `Mutex`; merely moving the pointer values across threads is
// safe. No other task touches this table (it is task_local).
#[allow(
    clippy::non_send_fields_in_send_ty,
    reason = "raw ptrs in LOCALLOCK gated by the target entry's partition shard Mutex; see SAFETY"
)]
unsafe impl Send for LocalLockTable {}

tokio::task_local! {
    static LOCAL_LOCKS: RefCell<LocalLockTable>;
}

/// Run `f` with a fresh per-task LOCALLOCK table (the backend task wraps its
/// body in this, alongside `my_proc_scope`).
pub async fn local_lock_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    LOCAL_LOCKS.scope(RefCell::new(LocalLockTable::new()), f).await
}

/// Access the per-task LOCALLOCK table. Panics outside a `local_lock_scope`
/// (a backend always has one). NEVER hold the returned borrow across `.await`.
fn with_local<R>(f: impl FnOnce(&mut LocalLockTable) -> R) -> R {
    LOCAL_LOCKS.with(|c| f(&mut c.borrow_mut()))
}

// ---------------------------------------------------------------------------
// Raw-pointer helpers for the boxed LOCK/PROCLOCK (15a interop)
// ---------------------------------------------------------------------------

fn lock_ptr(b: &mut Box<LOCK>) -> *mut LOCK {
    &raw mut **b
}
fn proclock_ptr(b: &mut Box<PROCLOCK>) -> *mut PROCLOCK {
    &raw mut **b
}

// ---------------------------------------------------------------------------
// Conflict internals (the stubs 15a's proc.c calls)
// ---------------------------------------------------------------------------

/// PG `LockCheckConflicts`: does the requested mode conflict with already-granted
/// locks (after subtracting this holder's own + lock-group members')? Returns
/// true if conflict.
///
/// Lock groups are single-member until F4: the group-subtraction loop over other
/// proclocks is a no-op for a single-member group, so we keep only the "subtract
/// my own held modes" path. TODO(F4): full group-aware subtraction.
pub fn LockCheckConflicts(
    lock_method_table: LockMethod,
    lockmode: LOCKMODE,
    lock: &mut LOCK,
    proclock: &mut PROCLOCK,
) -> bool {
    let num_lock_modes = lock_method_table.num_lock_modes;
    let conflict_mask = lock_method_table.conflict_tab[lockmode as usize];

    // Global check: nothing granted conflicts -> no conflict.
    if (conflict_mask & lock.grant_mask) == 0 {
        return false;
    }

    // Subtract out locks I hold myself.
    let my_locks = proclock.hold_mask;
    let mut total_remaining = 0i32;
    for i in 1..=num_lock_modes {
        if (conflict_mask & lockbit_on(i)) == 0 {
            continue;
        }
        let mut remaining = lock.granted[i as usize];
        if (my_locks & lockbit_on(i)) != 0 {
            remaining -= 1;
        }
        total_remaining += remaining;
    }

    // No conflicts remain -> we get the lock.
    if total_remaining == 0 {
        return false;
    }

    // Single-member lock groups (F4 pending): a remaining conflict is real.
    // PG additionally subtracts modes held by other members of our lock group;
    // with single-member groups there are none. The relation-extension special
    // case (conflicts even within a group) is therefore also subsumed here.
    true
}

/// PG `GrantLock`: record that `lockmode` is granted on `lock` to `proclock`.
pub fn GrantLock(lock: &mut LOCK, proclock: &mut PROCLOCK, lockmode: LOCKMODE) {
    lock.n_granted += 1;
    lock.granted[lockmode as usize] += 1;
    lock.grant_mask |= lockbit_on(lockmode);
    if lock.granted[lockmode as usize] == lock.requested[lockmode as usize] {
        lock.wait_mask &= lockbit_off(lockmode);
    }
    proclock.hold_mask |= lockbit_on(lockmode);
    debug_assert!(lock.n_granted > 0 && lock.granted[lockmode as usize] > 0);
    debug_assert!(lock.n_granted <= lock.n_requested);
}

/// PG `UnGrantLock`: opposite of GrantLock. Returns whether ProcLockWakeup is
/// needed (the released mode conflicts with at least one waiter).
fn un_grant_lock(
    lock: &mut LOCK,
    lockmode: LOCKMODE,
    proclock: &mut PROCLOCK,
    lock_method_table: LockMethod,
) -> bool {
    debug_assert!(lock.n_requested > 0 && lock.requested[lockmode as usize] > 0);
    debug_assert!(lock.n_granted > 0 && lock.granted[lockmode as usize] > 0);

    lock.n_requested -= 1;
    lock.requested[lockmode as usize] -= 1;
    lock.n_granted -= 1;
    lock.granted[lockmode as usize] -= 1;

    if lock.granted[lockmode as usize] == 0 {
        lock.grant_mask &= lockbit_off(lockmode);
    }

    let wakeup_needed = (lock_method_table.conflict_tab[lockmode as usize] & lock.wait_mask) != 0;

    proclock.hold_mask &= lockbit_off(lockmode);
    wakeup_needed
}

/// PG `RememberSimpleDeadLock`: record an early hard deadlock detected while
/// joining the wait queue, so `DeadLockReport` can describe it. Delegates to
/// deadlock.c (15c). Takes ProcNumbers (the arena identity).
pub fn RememberSimpleDeadLock(
    proc1: ProcNumber,
    lockmode: LOCKMODE,
    lock: &LOCK,
    proc2: ProcNumber,
) {
    crate::backend::storage::lmgr::deadlock::RememberSimpleDeadLock(proc1, lockmode, lock, proc2);
}

// ---------------------------------------------------------------------------
// Error-cleanup state (PG StrongLockInProgress / awaitedLock)
// ---------------------------------------------------------------------------

/// PG `AbortStrongLockAcquire`: undo a `BeginStrongLockAcquire` on error.
pub fn AbortStrongLockAcquire() {
    let Some((tag, Some(hashcode))) = with_local(|l| {
        l.strong_in_progress.map(|t| {
            l.strong_in_progress = None;
            (t, l.locks.get(&t).map(|ll| ll.hashcode))
        })
    }) else {
        return;
    };
    if let Some(m) = LockManager::get() {
        let fasthash = fast_path_strong_lock_hash_partition(hashcode);
        let mut s = m.strong.lock();
        if s.count[fasthash] > 0 {
            s.count[fasthash] -= 1;
        }
    }
    with_local(|l| {
        if let Some(ll) = l.locks.get_mut(&tag) {
            ll.holds_strong_lock_count = false;
        }
    });
}

/// PG `GetAwaitedLock`: a sentinel pointer if we are currently WaitOnLock'ing.
/// (15a only checks `is_none()`; the actual LOCALLOCK lives in the task_local.)
pub fn GetAwaitedLock() -> Option<*mut LOCALLOCK> {
    with_local(|l| {
        l.awaited_lock
            .and_then(|t| l.locks.get_mut(&t).map(|b| &raw mut **b))
    })
}

/// PG `ResetAwaitedLock`.
pub fn ResetAwaitedLock() {
    with_local(|l| {
        l.awaited_lock = None;
        l.awaited_owner = None;
    });
}

/// PG `GrantAwaitedLock`: GrantLockLocal for the lock we are WaitOnLock'ing.
pub fn GrantAwaitedLock() {
    with_local(|l| {
        if let Some(tag) = l.awaited_lock {
            let owner = l.awaited_owner.clone();
            grant_lock_local(l, tag, owner);
        }
    });
}

/// PG `MarkLockClear`.
pub fn MarkLockClear(locallock: &mut LOCALLOCK) {
    debug_assert!(locallock.n_locks > 0);
    locallock.lock_cleared = true;
}

// ---------------------------------------------------------------------------
// LOCALLOCK helpers (GrantLockLocal / RemoveLocalLock)
// ---------------------------------------------------------------------------

/// PG `GrantLockLocal`: bump the per-task hold count for `tag`, by owner.
fn grant_lock_local(l: &mut LocalLockTable, tag: LOCALLOCKTAG, owner: Option<ResourceOwner>) {
    let ll = l.locks.get_mut(&tag).expect("locallock present");
    ll.n_locks += 1;
    for lo in &mut ll.lock_owners {
        if owners_eq(lo.owner.as_ref(), owner.as_ref()) {
            lo.n_locks += 1;
            return;
        }
    }
    ll.lock_owners.push(LOCALLOCKOWNER {
        owner,
        n_locks: 1,
    });
}

fn owners_eq(a: Option<&ResourceOwner>, b: Option<&ResourceOwner>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => x.ptr_eq(y),
        _ => false,
    }
}

/// PG `RemoveLocalLock`: drop the LOCALLOCK entry, reverting any strong-lock
/// count it held.
fn remove_local_lock(l: &mut LocalLockTable, tag: LOCALLOCKTAG) {
    let holds_strong = l.locks.get(&tag).is_some_and(|ll| ll.holds_strong_lock_count);
    let hashcode = l.locks.get(&tag).map_or(0, |ll| ll.hashcode);
    if holds_strong
        && let Some(m) = LockManager::get() {
            let fasthash = fast_path_strong_lock_hash_partition(hashcode);
            let mut s = m.strong.lock();
            if s.count[fasthash] > 0 {
                s.count[fasthash] -= 1;
            }
        }
    l.locks.remove(&tag);
}

// ---------------------------------------------------------------------------
// SetupLockInTable -- find/create LOCK + PROCLOCK under the shard Mutex
// ---------------------------------------------------------------------------

/// PG `SetupLockInTable`: find/create the LOCK and PROCLOCK for (`proc`,
/// `locktag`, `lockmode`) and bump the request counts. The caller holds the
/// shard Mutex. Returns the raw stable pointers (the boxed entries live in the
/// shard). Group leader is single-member (F4 pending).
#[allow(clippy::type_complexity)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "Option mirrors C SetupLockInTable NULL-on-OOM contract; caller has the OOM path"
)]
fn setup_lock_in_table(
    shard: &mut LockShard,
    proc: ProcNumber,
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
) -> Option<(*mut LOCK, *mut PROCLOCK)> {
    // Find or create the LOCK.
    let lock_is_new = !shard.locks.contains_key(locktag);
    if lock_is_new {
        shard.locks.insert(*locktag, Box::new(new_lock(*locktag)));
    }

    let pl_key = ProcLockKey {
        lock: *locktag,
        proc,
    };
    let proclock_is_new = !shard.proclocks.contains_key(&pl_key);
    if proclock_is_new {
        // Group leader == self for single-member groups (F4 pending).
        shard.proclocks.insert(
            pl_key,
            Box::new(new_proclock(*locktag, proc)),
        );
    }

    // Bump request counts on the LOCK (covers granted + waiting).
    {
        let lock = shard.locks.get_mut(locktag).unwrap();
        lock.n_requested += 1;
        lock.requested[lockmode as usize] += 1;
    }

    // We shouldn't already hold the desired mode.
    {
        let pl = shard.proclocks.get(&pl_key).unwrap();
        // PG elog(ERROR). TODO(panic).
        assert!((pl.hold_mask & lockbit_on(lockmode)) == 0, "lock already held");
    }

    let lp = lock_ptr(shard.locks.get_mut(locktag).unwrap());
    let plp = proclock_ptr(shard.proclocks.get_mut(&pl_key).unwrap());
    Some((lp, plp))
}

fn new_lock(tag: LOCKTAG) -> LOCK {
    LOCK {
        tag,
        grant_mask: 0,
        wait_mask: 0,
        proc_locks: Vec::new(),
        wait_procs: Vec::new(),
        requested: [0; MAX_LOCKMODES],
        n_requested: 0,
        granted: [0; MAX_LOCKMODES],
        n_granted: 0,
    }
}

fn new_proclock(_tag: LOCKTAG, proc: ProcNumber) -> PROCLOCK {
    use crate::storage::lock::PROCLOCKTAG;
    PROCLOCK {
        tag: PROCLOCKTAG {
            lock: std::ptr::null_mut(),
            proc: std::ptr::null_mut(),
        },
        group_leader: proc, // single-member group (F4 pending)
        hold_mask: 0,
        release_mask: 0,
    }
}

// ---------------------------------------------------------------------------
// CleanUpLock -- garbage-collect + wake waiters under the shard Mutex
// ---------------------------------------------------------------------------

/// PG `CleanUpLock`: after releasing, delete the proclock if it holds nothing,
/// delete the lock if no requests remain, else ProcLockWakeup. Caller holds the
/// shard Mutex.
fn clean_up_lock(
    shard: &mut LockShard,
    locktag: &LOCKTAG,
    proc: ProcNumber,
    lock_method_table: LockMethod,
    wakeup_needed: bool,
) {
    let pl_key = ProcLockKey {
        lock: *locktag,
        proc,
    };
    // Delete the proclock if it now holds nothing.
    let proclock_empty = shard
        .proclocks
        .get(&pl_key)
        .is_some_and(|p| p.hold_mask == 0);
    if proclock_empty {
        shard.proclocks.remove(&pl_key);
    }

    let n_requested = shard.locks.get(locktag).map_or(0, |l| l.n_requested);
    if n_requested == 0 {
        shard.locks.remove(locktag);
    } else if wakeup_needed {
        // Wake newly-grantable waiters. ProcLockWakeup (15a) is sync and runs
        // under the shard Mutex; it calls back into GrantLock via the raw lock
        // pointer.
        if let Some(lb) = shard.locks.get_mut(locktag) {
            let lp = lock_ptr(lb);
            // SAFETY: shard Mutex held; the boxed LOCK is alive in the map.
            crate::storage::proc::ProcLockWakeup(lock_method_table, unsafe { &mut *lp });
        }
    }
}

/// The core of PG `RemoveFromWaitQueue` + the `CleanUpLock(true)` it calls:
/// pull a still-waiting `procno` out of its awaited lock's wait queue, undo its
/// request-count increments, clear the waitMask bit, set waitStatus = ERROR, then
/// delete the now-orphan PROCLOCK / GC the empty LOCK / ProcLockWakeup the
/// trailing waiters. Operates on a `&mut LockShard` (the partition the awaited
/// lock lives in); the caller supplies it with the partition Mutex held.
///
/// IDEMPOTENT: returns immediately (no decrements) if `procno` is not currently
/// WAITING -- so the deadlock path (CheckDeadLock) and the give-up guard cannot
/// double-clean the same wait. After it runs once, `wait_lock`/`wait_status` are
/// cleared and a second call is a no-op. SYNC.
fn remove_from_wait_queue_in_shard(shard: &mut LockShard, procno: ProcNumber) {
    let Some(g) = ProcGlobal::get() else {
        return;
    };
    let g = g.clone();

    // Read the proc's wait state + clear it. The WAITING gate is the idempotency
    // guard: once cleared we never decrement counts again.
    // SAFETY: the partition Mutex held by the caller gates these wait fields.
    let (locktag, lockmode) = unsafe {
        let Some(proc) = g.proc_mut(procno) else {
            return;
        };
        if proc.wait_status != ProcWaitStatus::WAITING {
            return;
        }
        let lp = proc.wait_lock;
        let lm = proc.wait_lock_mode;
        proc.wait_lock = None;
        proc.wait_proc_lock = None;
        proc.wait_status = ProcWaitStatus::ERROR;
        proc.wait_start = 0;
        let Some(lp) = lp else {
            return;
        };
        // SAFETY: the boxed LOCK is alive under the held partition Mutex.
        ((*lp).tag, lm)
    };

    let Some(lock_method_table) = lock_methods(locktag.lockmethod()) else {
        return;
    };

    let wakeup_needed = {
        let Some(lock) = shard.locks.get_mut(&locktag).map(|lb| &mut **lb) else {
            return;
        };
        // Remove from the LOCK.wait_procs queue + undo request counts.
        if let Some(pos) = lock.wait_procs.iter().position(|&p| p == procno) {
            lock.wait_procs.remove(pos);
        }
        if lock.n_requested > 0 {
            lock.n_requested -= 1;
        }
        if lock.requested[lockmode as usize] > 0 {
            lock.requested[lockmode as usize] -= 1;
        }
        if lock.granted[lockmode as usize] == lock.requested[lockmode as usize] {
            lock.wait_mask &= lockbit_off(lockmode);
        }
        // PG CleanUpLock is always called with wakeupNeeded=true here.
        true
    };

    // Delete the orphan PROCLOCK if it now holds nothing, GC the empty LOCK, and
    // wake the now-grantable trailing waiters (PG CleanUpLock).
    clean_up_lock(shard, &locktag, procno, lock_method_table, wakeup_needed);
}

/// PG `RemoveFromWaitQueue`: the HARD-deadlock give-up, called by CheckDeadLock
/// (proc.c) with ALL partition Mutexes already held (it passes the all-partitions
/// `view`). Locates the awaited lock's shard through the view -- WITHOUT re-locking
/// (std::Mutex is not reentrant) -- and runs the shared cleanup core. SYNC.
pub fn RemoveFromWaitQueue(procno: ProcNumber, view: &LockTablesView) {
    let Some(g) = ProcGlobal::get() else {
        return;
    };
    let g = g.clone();
    // The awaited lock's tag (to pick the partition). Read under the held locks.
    // SAFETY: all partition Mutexes held by the caller (the view).
    let locktag = unsafe {
        let Some(lp) = g.proc(procno).and_then(|p| p.wait_lock) else {
            return;
        };
        (*lp).tag
    };
    let shard = view.shard_for_mut(&locktag);
    remove_from_wait_queue_in_shard(shard, procno);
}

/// Cleanup for a TIMEOUT / cancellation give-up: take the SINGLE partition Mutex
/// of the awaited lock and run the full PG RemoveFromWaitQueue + CleanUpLock +
/// AbortStrongLockAcquire + RemoveLocalLock. Used by the `wait_on_lock` RAII
/// guard. Idempotent via `remove_from_wait_queue_in_shard` (a no-op if the proc
/// was already dequeued by a grantor or by CheckDeadLock). SYNC -- no `.await`.
fn clean_up_after_give_up(procno: ProcNumber, hashcode: u32, localtag: LOCALLOCKTAG) {
    if let Some(m) = LockManager::get() {
        let mut shard = m.shard(hashcode).lock();
        remove_from_wait_queue_in_shard(&mut shard, procno);
        drop(shard);
    }
    AbortStrongLockAcquire();
    with_local(|l| {
        let empty = l.locks.get(&localtag).is_some_and(|x| x.n_locks == 0);
        if empty {
            remove_local_lock(l, localtag);
        }
    });
}

// ---------------------------------------------------------------------------
// Strong-lock-count + fast-path transfer
// ---------------------------------------------------------------------------

/// PG `BeginStrongLockAcquire`: bump the strong-lock count for the partition and
/// remember the in-progress LOCALLOCK for error cleanup.
fn begin_strong_lock_acquire(l: &mut LocalLockTable, tag: LOCALLOCKTAG, fasthash: usize) {
    if let Some(m) = LockManager::get() {
        let mut s = m.strong.lock();
        s.count[fasthash] += 1;
    }
    if let Some(ll) = l.locks.get_mut(&tag) {
        ll.holds_strong_lock_count = true;
    }
    l.strong_in_progress = Some(tag);
}

/// PG `FinishStrongLockAcquire`.
fn finish_strong_lock_acquire(l: &mut LocalLockTable) {
    l.strong_in_progress = None;
}

/// PG `FastPathGrantRelationLock`: grant a relation lock in the per-proc
/// fast-path array, if there's room. Caller holds the proc's fp_info_lock.
fn fast_path_grant_relation_lock(
    proc: &mut PGPROC,
    fast_path_use: &mut [u32],
    relid: Oid,
    lockmode: LOCKMODE,
) -> bool {
    let group = fast_path_rel_group(relid);
    let mut unused_slot = fast_path_slots_per_backend();
    for i in 0..FP_LOCK_SLOTS_PER_GROUP {
        let f = fast_path_slot(group, i);
        if fast_path_get_bits(proc, f) == 0 {
            unused_slot = f;
        } else if proc.fp_rel_id[f as usize] == relid {
            fast_path_set_lockmode(proc, f, lockmode);
            return true;
        }
    }
    if unused_slot < fast_path_slots_per_backend() {
        proc.fp_rel_id[unused_slot as usize] = relid;
        fast_path_set_lockmode(proc, unused_slot, lockmode);
        fast_path_use[group as usize] += 1;
        return true;
    }
    false
}

/// PG `FastPathUnGrantRelationLock`: release a relation lock from the fast-path
/// array, recomputing the local-use count. Caller holds the proc's fp_info_lock.
fn fast_path_un_grant_relation_lock(
    proc: &mut PGPROC,
    fast_path_use: &mut [u32],
    relid: Oid,
    lockmode: LOCKMODE,
) -> bool {
    let group = fast_path_rel_group(relid);
    let mut result = false;
    fast_path_use[group as usize] = 0;
    for i in 0..FP_LOCK_SLOTS_PER_GROUP {
        let f = fast_path_slot(group, i);
        if proc.fp_rel_id[f as usize] == relid && fast_path_check_lockmode(proc, f, lockmode) {
            fast_path_clear_lockmode(proc, f, lockmode);
            result = true;
        }
        if fast_path_get_bits(proc, f) != 0 {
            fast_path_use[group as usize] += 1;
        }
    }
    result
}

/// PG `FastPathTransferRelationLocks`: move any matching fast-path locks held by
/// any backend into the main lock table. Caller holds NO partition lock (we take
/// the shard Mutex + each proc's fp_info_lock here). Returns false on out-of-mem
/// (never under the Arc model).
fn fast_path_transfer_relation_locks(
    m: &LockManager,
    lock_method_table: LockMethod,
    locktag: &LOCKTAG,
    hashcode: u32,
) -> bool {
    let Some(g) = ProcGlobal::get() else {
        return true;
    };
    let _ = lock_method_table;
    let relid = Oid(locktag.locktag_field2);
    let group = fast_path_rel_group(relid);

    for i in 0..g.all_proc_count {
        let procno = i as ProcNumber;
        with_fp_locked(procno, |proc| {
            if proc.database_id != Oid(locktag.locktag_field1)
                || proc.fp_lock_bits.get(group as usize).copied().unwrap_or(0) == 0
            {
                return;
            }
            for j in 0..FP_LOCK_SLOTS_PER_GROUP {
                let f = fast_path_slot(group, j);
                if proc.fp_rel_id[f as usize] != relid || fast_path_get_bits(proc, f) == 0 {
                    continue;
                }
                let mut shard = m.shard(hashcode).lock();
                for lockmode in FAST_PATH_LOCKNUMBER_OFFSET
                    ..(FAST_PATH_LOCKNUMBER_OFFSET + FAST_PATH_BITS_PER_SLOT)
                {
                    let lm = lockmode as LOCKMODE;
                    if !fast_path_check_lockmode(proc, f, lm) {
                        continue;
                    }
                    if let Some((lp, plp)) = setup_lock_in_table(&mut shard, procno, locktag, lm) {
                        // SAFETY: shard Mutex held; boxed entries alive.
                        GrantLock(unsafe { &mut *lp }, unsafe { &mut *plp }, lm);
                        wire_proclock(&mut shard, locktag, procno);
                        fast_path_clear_lockmode(proc, f, lm);
                    }
                }
                drop(shard);
                break;
            }
        });
    }
    true
}

/// Wire a freshly-created PROCLOCK's raw-pointer tag (PG keeps `myLock`/`myProc`
/// pointers in the tag; here we set them from the stable boxed addresses so
/// proc.c's `proclock.tag` reads work). The holder list PG keeps in
/// `LOCK.procLocks` is instead the shard's proclock HashMap filtered by tag
/// (`holders_of`); `LOCK.proc_locks` is unused. Caller holds the shard Mutex.
fn wire_proclock(shard: &mut LockShard, locktag: &LOCKTAG, proc: ProcNumber) {
    let pl_key = ProcLockKey {
        lock: *locktag,
        proc,
    };
    let Some(lp) = shard.locks.get_mut(locktag).map(lock_ptr) else {
        return;
    };
    let g = ProcGlobal::get();
    // SAFETY: the arena slot is stable for the process lifetime.
    let proc_ptr = g.map_or(std::ptr::null_mut(), |g| {
        unsafe { g.proc_mut(proc).map(std::ptr::from_mut::<PGPROC>) }
            .unwrap_or(std::ptr::null_mut())
    });
    if let Some(plb) = shard.proclocks.get_mut(&pl_key) {
        plb.tag.lock = lp;
        plb.tag.proc = proc_ptr;
    }
}

// ---------------------------------------------------------------------------
// LockAcquire / LockAcquireExtended (ASYNC)
// ---------------------------------------------------------------------------

/// PG `LockAcquire`.
pub async fn LockAcquire(
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
    session_lock: bool,
    dont_wait: bool,
) -> LockAcquireResult {
    LockAcquireExtended(locktag, lockmode, session_lock, dont_wait, true, false)
        .await
        .0
}

/// PG `LockAcquireExtended`. ASYNC: the WAIT path drops the shard Mutex before
/// `ProcSleep().await`. The `LOCALLOCK **locallockp` out-param is folded into the
/// return (we return a sentinel pointer into the task_local table).
#[allow(
    clippy::too_many_lines,
    reason = "1:1 port of C LockAcquireExtended; splitting would diverge from PG structure"
)]
#[allow(clippy::fn_params_excessive_bools, reason = "mirrors C signature")]
pub async fn LockAcquireExtended(
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
    session_lock: bool,
    dont_wait: bool,
    report_memory_error: bool,
    _log_lock_failure: bool,
) -> (LockAcquireResult, Option<*mut LOCALLOCK>) {
    let lockmethodid = locktag.lockmethod();
    let Some(lock_method_table) = lock_methods(lockmethodid) else {
        panic!("unrecognized lock method: {lockmethodid}");
    };
    assert!(!(lockmode <= 0 || lockmode > lock_method_table.num_lock_modes), "unrecognized lock mode: {lockmode}");

    // TODO(recovery/standby): PG (lock.c:863-871) raises ERROR for relation/object
    // locks > RowExclusiveLock while RecoveryInProgress(), and logs
    // AccessExclusiveLock for standby replay (lock.c:967-974/1257-1266). Deferred
    // with the recovery + hot-standby WAL subsystem (RecoveryInProgress is a stub).

    let m = LockManager::expect().clone();
    let procno = current_proc_number();
    assert!(procno != INVALID_PROC_NUMBER, "LockAcquire without a PGPROC");

    // PG: owner = sessionLock ? NULL : CurrentResourceOwner. We stand in with the
    // per-task current_owner marker until resowner is wired (TODO(15d/resowner)).
    let owner = if session_lock {
        None
    } else {
        Some(with_local(|l| l.current_owner.clone()))
    };

    let localtag = LOCALLOCKTAG {
        lock: *locktag,
        mode: lockmode,
    };
    let hashcode = LockTagHashCode(locktag);

    // Find or create the LOCALLOCK; if already held, just bump locally.
    let already = with_local(|l| {
        let entry = l.locks.entry(localtag).or_insert_with(|| {
            Box::new(new_locallock(localtag, hashcode))
        });
        if entry.n_locks > 0 {
            let cleared = entry.lock_cleared;
            grant_lock_local(l, localtag, owner.clone());
            Some(if cleared {
                LockAcquireResult::AlreadyClear
            } else {
                LockAcquireResult::AlreadyHeld
            })
        } else {
            None
        }
    });
    if let Some(res) = already {
        let p = locallock_ptr(localtag);
        return (res, p);
    }

    // Attempt the fast path, if eligible and the local-use group has room.
    if eligible_for_relation_fast_path(locktag, lockmode) {
        let group = fast_path_rel_group(Oid(locktag.locktag_field2));
        let has_room = with_local(|l| {
            l.fast_path_use.get(group as usize).copied().unwrap_or(0)
                < FP_LOCK_SLOTS_PER_GROUP
        });
        if has_room {
            let fasthash = fast_path_strong_lock_hash_partition(hashcode);
            // PG checks the strong-lock count and grants the fast-path bit inside
            // ONE fpInfoLock critical section (lock.c:998-1004); the atomicity is
            // what orders against FastPathTransferRelationLocks taking the same
            // fpInfoLock. m.strong is a leaf lock (no await) taken while held.
            let acquired = with_fp_locked(procno, |proc| {
                if m.strong.lock().count[fasthash] != 0 {
                    return false;
                }
                with_local(|l| {
                    fast_path_grant_relation_lock(
                        proc,
                        &mut l.fast_path_use,
                        Oid(locktag.locktag_field2),
                        lockmode,
                    )
                })
            })
            .unwrap_or(false);
            if acquired {
                with_local(|l| {
                    if let Some(ll) = l.locks.get_mut(&localtag) {
                        ll.lock = std::ptr::null_mut();
                        ll.proclock = std::ptr::null_mut();
                    }
                    grant_lock_local(l, localtag, owner.clone());
                });
                return (LockAcquireResult::Ok, locallock_ptr(localtag));
            }
        }
    }

    // If this strong lock could have been taken fast-path by others, disable the
    // fast path for this tag and migrate any such locks to the main table.
    if conflicts_with_relation_fast_path(locktag, lockmode) {
        let fasthash = fast_path_strong_lock_hash_partition(hashcode);
        with_local(|l| begin_strong_lock_acquire(l, localtag, fasthash));
        if !fast_path_transfer_relation_locks(&m, lock_method_table, locktag, hashcode) {
            AbortStrongLockAcquire();
            with_local(|l| {
                let empty = l.locks.get(&localtag).is_some_and(|x| x.n_locks == 0);
                if empty {
                    remove_local_lock(l, localtag);
                }
            });
            assert!(!report_memory_error, "out of shared memory");
            return (LockAcquireResult::NotAvail, None);
        }
    }

    // The shared-table work runs in a block that holds the partition shard Mutex
    // and the raw LOCK/PROCLOCK pointers; both are DROPPED before any `.await`
    // (rules s5: never hold the shard Mutex / a !Send raw pointer across await).
    // The block yields only a plain (Send) ProcWaitStatus / early result.
    #[allow(clippy::items_after_statements, reason = "local helper scoped to this fn's await discipline")]
    enum Pre {
        Done(LockAcquireResult),
        Waiting,
        Granted,
    }

    let pre = {
        let mut shard = m.shard(hashcode).lock();

        let Some((lock_ptr_raw, proclock_ptr_raw)) =
            setup_lock_in_table(&mut shard, procno, locktag, lockmode)
        else {
            drop(shard);
            AbortStrongLockAcquire();
            with_local(|l| {
                let empty = l.locks.get(&localtag).is_some_and(|x| x.n_locks == 0);
                if empty {
                    remove_local_lock(l, localtag);
                }
            });
            assert!(!report_memory_error, "out of shared memory");
            return (LockAcquireResult::NotAvail, None);
        };
        wire_proclock(&mut shard, locktag, procno);

        // Record the LOCK/PROCLOCK pointers in the LOCALLOCK.
        with_local(|l| {
            if let Some(ll) = l.locks.get_mut(&localtag) {
                ll.lock = lock_ptr_raw;
                ll.proclock = proclock_ptr_raw;
            }
        });

        // Conflict check: against waiters first, then against held.
        // SAFETY: shard Mutex held; the boxed entries are alive.
        let found_conflict = {
            let lock_ref = unsafe { &mut *lock_ptr_raw };
            let proclock_ref = unsafe { &mut *proclock_ptr_raw };
            if (lock_method_table.conflict_tab[lockmode as usize] & lock_ref.wait_mask) != 0 {
                true
            } else {
                LockCheckConflicts(lock_method_table, lockmode, lock_ref, proclock_ref)
            }
        };

        let wait_result = if found_conflict {
            // JoinWaitQueue (15a, sync) reads lock/proclock via the raw pointers
            // we stored. It runs under the shard Mutex.
            join_wait_queue(localtag, lock_method_table, dont_wait)
        } else {
            // SAFETY: shard Mutex held.
            GrantLock(
                unsafe { &mut *lock_ptr_raw },
                unsafe { &mut *proclock_ptr_raw },
                lockmode,
            );
            ProcWaitStatus::OK
        };

        if wait_result == ProcWaitStatus::ERROR {
            // Deadlock while joining, or dontWait and we'd have to wait. Undo the
            // shared-entry changes before releasing the shard Mutex.
            AbortStrongLockAcquire();
            // SAFETY: shard Mutex held.
            let pl_hold = unsafe { (*proclock_ptr_raw).hold_mask };
            if pl_hold == 0 {
                let pl_key = ProcLockKey {
                    lock: *locktag,
                    proc: procno,
                };
                shard.proclocks.remove(&pl_key);
            }
            if let Some(lb) = shard.locks.get_mut(locktag) {
                lb.n_requested -= 1;
                lb.requested[lockmode as usize] -= 1;
            }
            drop(shard);
            with_local(|l| {
                let empty = l.locks.get(&localtag).is_some_and(|x| x.n_locks == 0);
                if empty {
                    remove_local_lock(l, localtag);
                }
            });
            if dont_wait {
                Pre::Done(LockAcquireResult::NotAvail)
            } else {
                // DeadLockReport: raise ERROR (panic). TODO(panic): 15c deadlock.
                #[allow(deprecated)]
                crate::storage::lock::dead_lock_report();
            }
        } else if wait_result == ProcWaitStatus::WAITING {
            Pre::Waiting
        } else {
            Pre::Granted
        }
        // shard + raw pointers dropped here (end of block) before any await.
    };

    let wait_result = match pre {
        Pre::Done(r) => return (r, None),
        Pre::Granted => ProcWaitStatus::OK,
        Pre::Waiting => {
            debug_assert!(!dont_wait);
            // THE drop-shard-before-await point (rules s5): the shard Mutex was
            // already dropped at the end of the block above; now ProcSleep().await.
            wait_on_lock(localtag, owner.clone()).await
        }
    };

    {
        // Do NOT do any material state change between the wait and return: the
        // shared-table give-up (wait-queue unlink + request-count undo + orphan
        // PROCLOCK delete + ProcLockWakeup of trailing waiters + strong-lock abort
        // + RemoveLocalLock) was already done by the `wait_on_lock` WaitGuard's
        // Drop under the partition Mutex (the SAME path cancellation takes). So
        // here we only translate the outcome.
        if wait_result == ProcWaitStatus::ERROR {
            // Deadlock / lock-timeout while waiting. PG raises DeadLockReport on a
            // hard deadlock; a dontWait==false lock-timeout maps to NotAvail here.
            // TODO(deadlock-report): a timer-detected hard deadlock surfaces as
            // NotAvail rather than the "deadlock detected" ERROR (the cycle IS
            // detected + broken by CheckDeadLock; only the report is missing). The
            // JoinWaitQueue early-deadlock path already calls dead_lock_report();
            // route this path through it too when the panic->Result error model lands.
            return (LockAcquireResult::NotAvail, None);
        }
    }

    debug_assert_eq!(wait_result, ProcWaitStatus::OK);

    // The lock was granted. Update the LOCALLOCK.
    with_local(|l| grant_lock_local(l, localtag, owner.clone()));
    with_local(finish_strong_lock_acquire);

    (LockAcquireResult::Ok, locallock_ptr(localtag))
}

fn new_locallock(tag: LOCALLOCKTAG, hashcode: u32) -> LOCALLOCK {
    LOCALLOCK {
        tag,
        hashcode,
        lock: std::ptr::null_mut(),
        proclock: std::ptr::null_mut(),
        n_locks: 0,
        lock_owners: Vec::new(),
        holds_strong_lock_count: false,
        lock_cleared: false,
    }
}

fn locallock_ptr(tag: LOCALLOCKTAG) -> Option<*mut LOCALLOCK> {
    with_local(|l| l.locks.get_mut(&tag).map(|b| &raw mut **b))
}

/// PG `JoinWaitQueue` is in proc.c (15a). It is sync and runs under the shard
/// Mutex; it needs the LOCALLOCK by value-ish access. 15a takes a `&mut LOCALLOCK`
/// + table + dont_wait, reading the lock/proclock via the raw pointers it stored.
fn join_wait_queue(
    tag: LOCALLOCKTAG,
    lock_method_table: LockMethod,
    dont_wait: bool,
) -> ProcWaitStatus {
    // SAFETY: the LOCALLOCK lives in the task_local table; we hand 15a a &mut to
    // it. 15a reads .lock/.proclock raw pointers (boxed, alive under the shard
    // Mutex which the caller holds).
    let Some(llp) = locallock_ptr(tag) else {
        return ProcWaitStatus::ERROR;
    };
    crate::storage::proc::JoinWaitQueue(unsafe { &mut *llp }, lock_method_table, dont_wait)
}

/// RAII give-up guard for `wait_on_lock` (PG's PG_TRY/PG_CATCH around ProcSleep,
/// LockErrorCleanup). Armed by default; `disarm()` is called only once the lock
/// is truly granted (ProcSleep returned OK). If the future is DROPPED while still
/// waiting -- query cancel, `select!` loser, task abort, or a normal ERROR exit --
/// `Drop` takes the awaited lock's SINGLE partition Mutex and runs the full
/// partition-locked cleanup (RemoveFromWaitQueue, CleanUpLock,
/// AbortStrongLockAcquire, RemoveLocalLock). Cancellation, timeout, and the
/// deadlock-ERROR exit thus all funnel through ONE cleanup. The cleanup is
/// idempotent (`remove_from_wait_queue_in_shard` is a no-op once dequeued), so a
/// hard-deadlock that CheckDeadLock already cleaned is not double-decremented.
struct WaitGuard {
    procno: ProcNumber,
    hashcode: u32,
    localtag: LOCALLOCKTAG,
    granted: bool,
}

impl WaitGuard {
    fn disarm(&mut self) {
        self.granted = true;
    }
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        if self.granted {
            return;
        }
        // SYNC cleanup; takes the single partition Mutex briefly (no `.await`).
        clean_up_after_give_up(self.procno, self.hashcode, self.localtag);
        // PG LockErrorCleanup also clears awaitedLock here.
        ResetAwaitedLock();
    }
}

/// PG `WaitOnLock`: a wrapper around `ProcSleep` (15a) with awaited-lock
/// bookkeeping for `LockErrorCleanup`. ASYNC; entered with NO shard Mutex held.
async fn wait_on_lock(tag: LOCALLOCKTAG, owner: Option<ResourceOwner>) -> ProcWaitStatus {
    let hashcode = with_local(|l| l.locks.get(&tag).map_or(0, |x| x.hashcode));
    let procno = current_proc_number();
    with_local(|l| {
        l.awaited_lock = Some(tag);
        l.awaited_owner = owner;
    });

    // Cancellation-safe guard (rules s5): on ANY non-granted exit -- including the
    // future being dropped mid-await -- Drop runs the partition-locked cleanup.
    let mut guard = WaitGuard {
        procno,
        hashcode,
        localtag: tag,
        granted: false,
    };

    // ProcSleep reads the awaited lock from the PGPROC wait fields JoinWaitQueue
    // set; no LOCALLOCK reference is held across its await (keeps the future Send).
    let result = crate::storage::proc::ProcSleep().await;

    if result == ProcWaitStatus::OK {
        // Granted: disarm the guard and clear awaitedLock (PG's OK path). On the
        // ERROR path we leave awaitedLock set and let the guard's Drop clean up
        // (PG's LockErrorCleanup safety net).
        guard.disarm();
        with_local(|l| {
            l.awaited_lock = None;
            l.awaited_owner = None;
        });
    }
    result
}

// ---------------------------------------------------------------------------
// LockRelease / LockReleaseAll
// ---------------------------------------------------------------------------

/// PG `LockRelease`: release one mode on `locktag`.
pub fn LockRelease(locktag: &LOCKTAG, lockmode: LOCKMODE, session_lock: bool) -> bool {
    let lockmethodid = locktag.lockmethod();
    let Some(lock_method_table) = lock_methods(lockmethodid) else {
        panic!("unrecognized lock method: {lockmethodid}");
    };
    assert!(!(lockmode <= 0 || lockmode > lock_method_table.num_lock_modes), "unrecognized lock mode: {lockmode}");
    let m = LockManager::expect().clone();
    let procno = current_proc_number();

    let localtag = LOCALLOCKTAG {
        lock: *locktag,
        mode: lockmode,
    };

    // Decrement the per-owner + total local counts; if still held, done.
    let owner = if session_lock {
        None
    } else {
        Some(with_local(|l| l.current_owner.clone()))
    };
    let decision = with_local(|l| release_local(l, localtag, owner.as_ref()));
    match decision {
        LocalReleaseDecision::NotHeld => return false,
        LocalReleaseDecision::StillHeld => return true,
        LocalReleaseDecision::Released => {}
    }

    let hashcode = with_local(|l| l.locks.get(&localtag).map_or(0, |x| x.hashcode));

    // Try the fast path.
    if eligible_for_relation_fast_path(locktag, lockmode) {
        let group = fast_path_rel_group(Oid(locktag.locktag_field2));
        let in_use = with_local(|l| l.fast_path_use.get(group as usize).copied().unwrap_or(0) > 0);
        if in_use {
            let released = with_fp_locked(procno, |proc| {
                with_local(|l| {
                    fast_path_un_grant_relation_lock(
                        proc,
                        &mut l.fast_path_use,
                        Oid(locktag.locktag_field2),
                        lockmode,
                    )
                })
            })
            .unwrap_or(false);
            if released {
                with_local(|l| remove_local_lock(l, localtag));
                return true;
            }
        }
    }

    // Mess with the shared lock table under the partition shard Mutex.
    let mut shard = m.shard(hashcode).lock();

    // Re-find the lock/proclock. Under fast-path transfer the LOCALLOCK.lock can
    // be null; re-find by tag.
    let pl_key = ProcLockKey {
        lock: *locktag,
        proc: procno,
    };
    if !shard.locks.contains_key(locktag) || !shard.proclocks.contains_key(&pl_key) {
        // Could be a fast-path lock that was never transferred (shouldn't reach
        // here) or already gone. Warn-and-return like PG.
        drop(shard);
        with_local(|l| remove_local_lock(l, localtag));
        return false;
    }

    let lp = lock_ptr(shard.locks.get_mut(locktag).unwrap());
    let plp = proclock_ptr(shard.proclocks.get_mut(&pl_key).unwrap());
    // SAFETY: shard Mutex held; boxed entries alive.
    let lock_ref = unsafe { &mut *lp };
    let proclock_ref = unsafe { &mut *plp };

    if (proclock_ref.hold_mask & lockbit_on(lockmode)) == 0 {
        drop(shard);
        with_local(|l| remove_local_lock(l, localtag));
        return false;
    }

    let wakeup_needed = un_grant_lock(lock_ref, lockmode, proclock_ref, lock_method_table);
    clean_up_lock(&mut shard, locktag, procno, lock_method_table, wakeup_needed);
    drop(shard);

    with_local(|l| remove_local_lock(l, localtag));
    true
}

enum LocalReleaseDecision {
    NotHeld,
    StillHeld,
    Released,
}

/// Decrement the LOCALLOCK's per-owner + total counts (PG LockRelease's local
/// half). Returns whether the lock is fully released locally.
fn release_local(
    l: &mut LocalLockTable,
    tag: LOCALLOCKTAG,
    owner: Option<&ResourceOwner>,
) -> LocalReleaseDecision {
    let ll = match l.locks.get_mut(&tag) {
        Some(ll) if ll.n_locks > 0 => ll,
        _ => return LocalReleaseDecision::NotHeld,
    };
    // Find the owner's slot and decrement.
    let mut found = false;
    let mut remove_idx = None;
    for (i, lo) in ll.lock_owners.iter_mut().enumerate() {
        if owners_eq(lo.owner.as_ref(), owner) {
            lo.n_locks -= 1;
            if lo.n_locks == 0 {
                remove_idx = Some(i);
            }
            found = true;
            break;
        }
    }
    if !found {
        return LocalReleaseDecision::NotHeld;
    }
    if let Some(i) = remove_idx {
        ll.lock_owners.swap_remove(i);
    }
    ll.n_locks -= 1;
    if ll.n_locks > 0 {
        return LocalReleaseDecision::StillHeld;
    }
    ll.lock_cleared = false;
    LocalReleaseDecision::Released
}

/// PG `LockReleaseAll`: release all locks of a method held by this backend.
pub fn LockReleaseAll(lockmethodid: LOCKMETHODID, all_locks: bool) {
    let Some(lock_method_table) = lock_methods(lockmethodid) else {
        panic!("unrecognized lock method: {lockmethodid}");
    };
    let Some(m) = LockManager::get() else {
        return;
    };
    let m = m.clone();
    let procno = current_proc_number();
    if procno == INVALID_PROC_NUMBER {
        return;
    }

    // The fast-path VXID lock is released only here (top-level xact end).
    if lockmethodid == DEFAULT_LOCKMETHOD {
        VirtualXactLockTableCleanup();
    }

    // Collect this method's held locks from the per-task table, classifying
    // fast-path vs main-table, then act. We snapshot to avoid holding the
    // task_local borrow across the shard locks.
    #[allow(clippy::items_after_statements, reason = "local snapshot type scoped to this fn")]
    struct Held {
        tag: LOCALLOCKTAG,
        is_fast_path: bool,
        modes: LOCKMASK, // for main-table: which modes to release
    }
    let held: Vec<Held> = with_local(|l| {
        let mut out = Vec::new();
        let tags: Vec<LOCALLOCKTAG> = l.locks.keys().copied().collect();
        for tag in tags {
            let ll = l.locks.get(&tag).unwrap();
            if ll.n_locks == 0 {
                out.push(Held { tag, is_fast_path: false, modes: 0 });
                continue;
            }
            if tag.lock.lockmethod() != lockmethodid {
                continue;
            }
            // Session-lock retention (not all_locks, lock.c:2337-2362): forget the
            // xact owners, but if a session owner (owner==None) remains, rewrite
            // the locallock to show just the session count and KEEP the heavyweight
            // lock -- regardless of whether xact owners were also present.
            if !all_locks {
                let session_n_locks: i64 = ll
                    .lock_owners
                    .iter()
                    .find(|o| o.owner.is_none())
                    .map_or(0, |o| o.n_locks);
                if session_n_locks > 0 {
                    let ll = l.locks.get_mut(&tag).unwrap();
                    ll.lock_owners.retain(|o| o.owner.is_none());
                    ll.n_locks = session_n_locks;
                    continue; // session hold retained
                }
            }
            let ll = l.locks.get(&tag).unwrap();
            let is_fp = ll.lock.is_null() || ll.proclock.is_null();
            out.push(Held {
                tag,
                is_fast_path: is_fp,
                modes: lockbit_on(tag.mode),
            });
        }
        out
    });

    for h in held {
        if h.modes == 0 && !h.is_fast_path {
            // Unused locallock (acquire failed): just forget it.
            with_local(|l| remove_local_lock(l, h.tag));
            continue;
        }
        let relid = Oid(h.tag.lock.locktag_field2);
        let lockmode = h.tag.mode;
        if h.is_fast_path {
            // Try fast-path release.
            let released = with_fp_locked(procno, |proc| {
                with_local(|l| {
                    fast_path_un_grant_relation_lock(proc, &mut l.fast_path_use, relid, lockmode)
                })
            })
            .unwrap_or(false);
            if released {
                with_local(|l| remove_local_lock(l, h.tag));
                continue;
            }
            // Transferred to the main table: release there.
            lock_refind_and_release(&m, procno, &h.tag.lock, lockmode, false);
            with_local(|l| remove_local_lock(l, h.tag));
            continue;
        }
        // Main-table lock: release the mode.
        lock_refind_and_release(&m, procno, &h.tag.lock, lockmode, false);
        with_local(|l| remove_local_lock(l, h.tag));
    }
}

/// PG `LockReleaseSession`.
pub fn LockReleaseSession(lockmethodid: LOCKMETHODID) {
    assert!(lock_methods(lockmethodid).is_some(), "unrecognized lock method: {lockmethodid}");
    // Release session-level holds (owner == None). Snapshot then release.
    let tags: Vec<LOCALLOCKTAG> = with_local(|l| {
        l.locks
            .iter()
            .filter(|(t, ll)| {
                t.lock.lockmethod() == lockmethodid
                    && ll.lock_owners.iter().any(|o| o.owner.is_none())
            })
            .map(|(t, _)| *t)
            .collect()
    });
    for tag in tags {
        let _ = LockRelease(&tag.lock, tag.mode, true);
    }
}

/// PG `LockRefindAndRelease`: re-find a lock in the shared table and release one
/// mode. Caller holds NO shard Mutex (we take it here).
fn lock_refind_and_release(
    m: &LockManager,
    proc: ProcNumber,
    locktag: &LOCKTAG,
    lockmode: LOCKMODE,
    decrement_strong_lock_count: bool,
) {
    let lock_method_table = GetLockTagsMethodTable(locktag);
    let hashcode = LockTagHashCode(locktag);
    let mut shard = m.shard(hashcode).lock();

    let pl_key = ProcLockKey {
        lock: *locktag,
        proc,
    };
    if !shard.locks.contains_key(locktag) || !shard.proclocks.contains_key(&pl_key) {
        // PG elog(PANIC, "failed to re-find shared lock object"). Be lenient.
        return;
    }
    let lp = lock_ptr(shard.locks.get_mut(locktag).unwrap());
    let plp = proclock_ptr(shard.proclocks.get_mut(&pl_key).unwrap());
    // SAFETY: shard Mutex held.
    let lock_ref = unsafe { &mut *lp };
    let proclock_ref = unsafe { &mut *plp };
    if (proclock_ref.hold_mask & lockbit_on(lockmode)) == 0 {
        return;
    }
    let wakeup_needed = un_grant_lock(lock_ref, lockmode, proclock_ref, lock_method_table);
    clean_up_lock(&mut shard, locktag, proc, lock_method_table, wakeup_needed);
    drop(shard);

    if decrement_strong_lock_count && conflicts_with_relation_fast_path(locktag, lockmode) {
        let fasthash = fast_path_strong_lock_hash_partition(hashcode);
        let mut s = m.strong.lock();
        if s.count[fasthash] > 0 {
            s.count[fasthash] -= 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

/// PG `LockHeldByMe`.
pub fn LockHeldByMe(locktag: &LOCKTAG, lockmode: LOCKMODE, orstronger: bool) -> bool {
    let held = with_local(|l| {
        l.locks
            .get(&LOCALLOCKTAG {
                lock: *locktag,
                mode: lockmode,
            })
            .is_some_and(|ll| ll.n_locks > 0)
    });
    if held {
        return true;
    }
    if orstronger {
        for slock in (lockmode + 1)..=MAX_LOCK_MODE {
            if LockHeldByMe(locktag, slock, false) {
                return true;
            }
        }
    }
    false
}

/// PG `LockHasWaiters`.
pub fn LockHasWaiters(locktag: &LOCKTAG, lockmode: LOCKMODE, _session_lock: bool) -> bool {
    let lockmethodid = locktag.lockmethod();
    let Some(lock_method_table) = lock_methods(lockmethodid) else {
        panic!("unrecognized lock method: {lockmethodid}");
    };
    let Some(m) = LockManager::get() else {
        return false;
    };
    let localtag = LOCALLOCKTAG {
        lock: *locktag,
        mode: lockmode,
    };
    let (hashcode, held) = with_local(|l| {
        l.locks
            .get(&localtag)
            .map_or((0, false), |ll| (ll.hashcode, ll.n_locks > 0))
    });
    if !held {
        return false;
    }
    let shard = m.shard(hashcode).lock();
    
    shard
        .locks
        .get(locktag)
        .is_some_and(|lock| (lock_method_table.conflict_tab[lockmode as usize] & lock.wait_mask) != 0)
}

/// PG `LockWaiterCount`: returns `nRequested` (granted + waiting) for the lock.
pub fn LockWaiterCount(locktag: &LOCKTAG) -> i32 {
    let lockmethodid = locktag.lockmethod();
    assert!(lock_methods(lockmethodid).is_some(), "unrecognized lock method: {lockmethodid}");
    let Some(m) = LockManager::get() else {
        return 0;
    };
    let hashcode = LockTagHashCode(locktag);
    let shard = m.shard(hashcode).lock();
    shard.locks.get(locktag).map_or(0, |l| l.n_requested)
}

/// PG `GetLockConflicts`: VXIDs of xacts holding conflicting locks. The fast-path
/// scan + the main-table proclock scan, deduped. Lock groups single-member.
pub fn GetLockConflicts(locktag: &LOCKTAG, lockmode: LOCKMODE) -> Vec<VirtualTransactionId> {
    let lockmethodid = locktag.lockmethod();
    let Some(lock_method_table) = lock_methods(lockmethodid) else {
        panic!("unrecognized lock method: {lockmethodid}");
    };
    let Some(m) = LockManager::get() else {
        return Vec::new();
    };
    let conflict_mask = lock_method_table.conflict_tab[lockmode as usize];
    let hashcode = LockTagHashCode(locktag);
    let myproc = current_proc_number();

    let mut out: Vec<VirtualTransactionId> = Vec::new();

    // Fast-path scan if the lock could conflict with fast-path locks.
    if conflicts_with_relation_fast_path(locktag, lockmode)
        && let Some(g) = ProcGlobal::get() {
            let relid = Oid(locktag.locktag_field2);
            let group = fast_path_rel_group(relid);
            for i in 0..g.all_proc_count {
                let procno = i as ProcNumber;
                if procno == myproc {
                    continue;
                }
                with_fp_locked(procno, |proc| {
                    if proc.database_id != Oid(locktag.locktag_field1)
                        || proc.fp_lock_bits.get(group as usize).copied().unwrap_or(0) == 0
                    {
                        return;
                    }
                    for j in 0..FP_LOCK_SLOTS_PER_GROUP {
                        let f = fast_path_slot(group, j);
                        if proc.fp_rel_id[f as usize] != relid {
                            continue;
                        }
                        let mut lockmask = fast_path_get_bits(proc, f);
                        if lockmask == 0 {
                            continue;
                        }
                        lockmask <<= FAST_PATH_LOCKNUMBER_OFFSET;
                        if (lockmask as i32 & conflict_mask) == 0 {
                            break;
                        }
                        let vxid = VirtualTransactionId {
                            proc_number: proc.vxid.proc_number,
                            local_transaction_id: proc.vxid.lxid,
                        };
                        if vxid.is_valid() {
                            out.push(vxid);
                        }
                        break;
                    }
                });
            }
        }
    let fast_count = out.len();

    // Main-table scan.
    let shard = m.shard(hashcode).lock();
    if let Some(lock) = shard.locks.get(locktag) {
        let _ = lock;
        for (key, proclock) in &shard.proclocks {
            if key.lock != *locktag {
                continue;
            }
            if (conflict_mask & proclock.hold_mask) == 0 {
                continue;
            }
            let proc = key.proc;
            if proc == myproc {
                continue;
            }
            if let Some(g) = ProcGlobal::get() {
                // SAFETY: read of vxid under the shard Mutex; benign-race in pg_locks.
                let vxid = unsafe {
                    g.proc(proc).map(|p| VirtualTransactionId {
                        proc_number: p.vxid.proc_number,
                        local_transaction_id: p.vxid.lxid,
                    })
                };
                if let Some(vxid) = vxid
                    && vxid.is_valid() && !out[..fast_count].contains(&vxid) {
                        out.push(vxid);
                    }
            }
        }
    }
    drop(shard);
    out
}

/// PG `GetRunningTransactionLocks`: AccessExclusiveLocks on relations, for
/// LogStandbySnapshot.
pub fn GetRunningTransactionLocks() -> Vec<xl_standby_lock> {
    let Some(m) = LockManager::get() else {
        return Vec::new();
    };
    let aex = lockbit_on(LockMode::AccessExclusiveLock as LOCKMODE);
    let g = ProcGlobal::get();
    // Take all partitions (shared semantics; we use the Mutex). Ascending.
    let guards: Vec<_> = m.shards.iter().map(|s| s.lock()).collect();
    let out: Vec<xl_standby_lock> = guards
        .iter()
        .flat_map(|shard| shard.proclocks.iter())
        .filter_map(|(key, proclock)| {
            if (proclock.hold_mask & aex) == 0
                || key.lock.locktag_type != LockTagType::Relation as u8
            {
                return None;
            }
            // SAFETY: read of xid under the held partition Mutexes.
            let xid = unsafe { g?.proc(key.proc).map(|p| p.xid)? };
            (xid != crate::access::transam::INVALID_TRANSACTION_ID).then_some(xl_standby_lock {
                xid,
                db_oid: Oid(key.lock.locktag_field1),
                rel_oid: Oid(key.lock.locktag_field2),
            })
        })
        .collect();
    drop(guards);
    out
}

// ---------------------------------------------------------------------------
// VirtualXact locks
// ---------------------------------------------------------------------------

/// PG `VirtualXactLockTableInsert`: take our VXID lock via the fast path.
pub fn VirtualXactLockTableInsert(vxid: VirtualTransactionId) {
    debug_assert!(vxid.is_valid());
    let procno = current_proc_number();
    if procno == INVALID_PROC_NUMBER {
        return;
    }
    with_fp_locked(procno, |proc| {
        proc.fp_vxid_lock = true;
        proc.fp_local_transaction_id = vxid.local_transaction_id;
    });
}

/// PG `VirtualXactLockTableCleanup`: release our VXID lock wherever it lives.
pub fn VirtualXactLockTableCleanup() {
    let procno = current_proc_number();
    if procno == INVALID_PROC_NUMBER {
        return;
    }
    let Some((fastpath, lxid)) = with_fp_locked(procno, |proc| {
        let fastpath = proc.fp_vxid_lock;
        let lxid = proc.fp_local_transaction_id;
        proc.fp_vxid_lock = false;
        proc.fp_local_transaction_id = crate::c::LocalTransactionId(0);
        (fastpath, lxid)
    }) else {
        return;
    };
    if !fastpath && crate::storage::lock::local_transaction_id_is_valid(lxid) {
        // Materialized into the main table by another backend; release there.
        let vxid = VirtualTransactionId {
            proc_number: procno,
            local_transaction_id: lxid,
        };
        let locktag = LOCKTAG::set_virtual_transaction(vxid);
        if let Some(m) = LockManager::get() {
            lock_refind_and_release(
                m,
                procno,
                &locktag,
                LockMode::ExclusiveLock as LOCKMODE,
                false,
            );
        }
    }
}

/// PG `VirtualXactLock`: wait for / test whether `vxid` is still running. ASYNC
/// (it may LockAcquire.await the materialized VXID lock).
pub async fn VirtualXactLock(vxid: VirtualTransactionId, wait: bool) -> bool {
    debug_assert!(vxid.is_valid());
    // Recovered prepared xacts and the 2PC path need twophase (deferred); the
    // common case is a live backend's fast-path VXID lock.
    if vxid.is_recovered_prepared_xact() {
        // TODO(15d/twophase): XactLockForVirtualXact.
        return true;
    }
    // PG holds fpInfoLock as ONE critical section spanning the still-running
    // recheck, the fpVXIDLock clear, AND the SetupLockInTable/GrantLock
    // materialization (lock.c:4744-4809). That section is what excludes
    // VirtualXactLockTableCleanup, so the VXID lock is never absent from both the
    // fast path and the main table. Do it all inside one with_fp_locked; the
    // shard Mutex nests inside fpInfoLock (same order as
    // fast_path_transfer_relation_locks).
    let locktag = LOCKTAG::set_virtual_transaction(vxid);
    let hashcode = LockTagHashCode(&locktag);
    let Some(m) = LockManager::get() else {
        return true;
    };
    let proceed = with_fp_locked(vxid.proc_number, |proc| {
        if proc.vxid.proc_number != vxid.proc_number
            || proc.fp_local_transaction_id != vxid.local_transaction_id
        {
            return true; // vxid ended (caller returns true)
        }
        if !wait {
            return false; // still running, not asked to wait
        }
        if proc.fp_vxid_lock {
            let mut shard = m.shard(hashcode).lock();
            if let Some((lp, plp)) = setup_lock_in_table(
                &mut shard,
                vxid.proc_number,
                &locktag,
                LockMode::ExclusiveLock as LOCKMODE,
            ) {
                // SAFETY: shard Mutex held.
                GrantLock(
                    unsafe { &mut *lp },
                    unsafe { &mut *plp },
                    LockMode::ExclusiveLock as LOCKMODE,
                );
                wire_proclock(&mut shard, &locktag, vxid.proc_number);
            }
            drop(shard);
            proc.fp_vxid_lock = false;
        }
        false // materialized; fall through to wait
    })
    .unwrap_or(true); // proc gone
    if proceed {
        return true;
    }
    if !wait {
        return false;
    }
    let _ = LockAcquire(&locktag, LockMode::ShareLock as LOCKMODE, false, false).await;
    let _ = LockRelease(&locktag, LockMode::ShareLock as LOCKMODE, false);
    true
}

// ---------------------------------------------------------------------------
// Deferred: 2PC, status data (need twophase / lockfuncs, not in this step)
// ---------------------------------------------------------------------------

/// PG `AtPrepare_Locks`. TODO(15d/twophase): materialize fast-path locks +
/// RegisterTwoPhaseRecord. The twophase subsystem is deferred.
pub fn AtPrepare_Locks() {
    // TODO(twophase): see lock.c AtPrepare_Locks.
}

/// PG `PostPrepare_Locks`. TODO(15d/twophase).
pub fn PostPrepare_Locks(_xid: crate::c::TransactionId) {
    // TODO(twophase).
}

/// PG `lock_twophase_recover`. TODO(twophase/recovery).
pub fn lock_twophase_recover(_xid: crate::c::TransactionId, _info: u16, _recdata: &[u8]) {}
/// PG `lock_twophase_postcommit`. TODO(twophase).
pub fn lock_twophase_postcommit(_xid: crate::c::TransactionId, _info: u16, _recdata: &[u8]) {}
/// PG `lock_twophase_postabort`. TODO(twophase).
pub fn lock_twophase_postabort(xid: crate::c::TransactionId, info: u16, recdata: &[u8]) {
    lock_twophase_postcommit(xid, info, recdata);
}
/// PG `lock_twophase_standby_recover`. TODO(twophase/standby).
pub fn lock_twophase_standby_recover(_xid: crate::c::TransactionId, _info: u16, _recdata: &[u8]) {}

/// PG `GetLockStatusData`. TODO(lockfuncs): the pg_locks snapshot.
pub fn GetLockStatusData() -> crate::storage::lock::LockData {
    crate::storage::lock::LockData { locks: Vec::new() }
}

/// PG `GetBlockerStatusData`. TODO(lockfuncs).
pub fn GetBlockerStatusData(_blocked_pid: i32) -> crate::storage::lock::BlockedProcsData {
    crate::storage::lock::BlockedProcsData {
        procs: Vec::new(),
        locks: Vec::new(),
        waiter_pids: Vec::new(),
    }
}

#[cfg(test)]
mod tests;
