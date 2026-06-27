//! Translated from PostgreSQL src/backend/storage/lmgr/proc.c
//!
//! Per-process backend state lifecycle (`InitProcGlobal`/`InitProcess`/`ProcKill`)
//! and the lock grant-wait machinery (`JoinWaitQueue`/`ProcSleep`/`ProcWakeup`/
//! `ProcLockWakeup`/`CheckDeadLock`). The big `PGPROC`/`ProcGlobal` types live in
//! the header `src/storage/proc.rs`.
//!
//! Representation (design step15 s0): the PGPROC arena is a fixed `Arc<ProcGlobal>`
//! published process-wide by `InitProcGlobal`; cross-task references are
//! `ProcNumber` indices. `MyProc` is a per-task `task_local` ProcNumber. The
//! grant-wait wake is each PGPROC's `Latch`.
//!
//! Async coloring (design step15 s6): `ProcSleep` is ASYNC -- it is entered with NO
//! lock partition Mutex held (the lock.c caller drops it first) and `tokio::select!`s
//! over the proc's Latch (woken by `ProcWakeup`), a deadlock-timeout timer (->
//! `CheckDeadLock`), and a lock-timeout timer. NEVER hold a sync guard across that
//! await. `JoinWaitQueue`/`ProcWakeup`/`ProcLockWakeup` are SYNC (the caller holds
//! the partition Mutex). `CheckDeadLock` runs `DeadLockCheck` (deadlock.c, 15c
//! stub) with all partition locks held -- no `.await` while any is held.
//!
//! Staging: the LOCK/PROCLOCK tables are lock.c (15b). Where these functions need
//! lock-conflict internals (`LockCheckConflicts`, `GrantLock`, `RememberSimpleDeadLock`,
//! `RemoveFromWaitQueue`, `GetAwaitedLock`) they call the existing `storage::lock`
//! stubs, which 15b fills in. `ProcSleep`'s select! structure + latch wake + timers
//! are REAL here (the deliverable).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::sync::Arc;
use std::time::Duration;

use crate::c::LocalTransactionId;
use crate::lib::stringinfo::StringInfo;
use crate::miscadmin::BackendType;
use crate::storage::lock::{DeadLockState, LOCALLOCK, LOCK, LockMethod};
use crate::storage::proc::{
    LockGroupRole, NUM_AUXILIARY_PROCS, NUM_SPECIAL_WORKER_PROCS, PGPROC, ProcCounts, ProcGlobal,
    ProcGlobalList, ProcWaitStatus, current_proc_number, has_my_proc, proc_global,
    set_current_proc_number, set_proc_global,
};
use crate::storage::procnumber::{INVALID_PROC_NUMBER, ProcNumber};

use crate::access::transam::INVALID_TRANSACTION_ID;

// got_deadlock_timeout / deadlock_state are per-wait locals in the async model
// (ProcSleep owns them across its select!), not file statics as in C.

// ---------------------------------------------------------------------------
// Sizing
// ---------------------------------------------------------------------------

/// Read the PGPROC sizing knobs from the process config / miscadmin GUCs.
/// Mirrors proc.c's use of MaxConnections / autovacuum_worker_slots /
/// max_worker_processes / MaxBackends / max_prepared_xacts.
fn proc_counts() -> ProcCounts {
    // The GUC subsystem is not wired yet; read the miscadmin static-mut bridge
    // (set by InitializeMaxBackends at startup) and fall back to test defaults so
    // a SharedState built without startup still gets a usable arena.
    // TODO(guc): source these from ProcessConfig once the GUC machinery lands.
    let max_connections = read_guc(unsafe { crate::miscadmin::MaxConnections }, 100) as usize;
    let max_worker_processes =
        read_guc(unsafe { crate::miscadmin::max_worker_processes }, 8) as usize;
    let max_backends = read_guc(unsafe { crate::miscadmin::MaxBackends }, 0);
    // autovacuum_worker_slots is not yet a global; PG defaults to 16. Special
    // workers add NUM_SPECIAL_WORKER_PROCS. TODO(guc).
    let autovac_workers = 16usize;
    let autovac_special = autovac_workers + NUM_SPECIAL_WORKER_PROCS as usize;

    // max_backends, if unset, is derived as PG's InitializeMaxBackends would:
    // MaxConnections + autovac slots + special + bgworkers (+ wal senders).
    let max_backends = if max_backends > 0 {
        max_backends as usize
    } else {
        max_connections + autovac_special + max_worker_processes
    };

    ProcCounts {
        max_connections,
        autovac_special,
        bgworkers: max_worker_processes,
        max_backends,
        num_auxiliary: NUM_AUXILIARY_PROCS as usize,
        // max_prepared_xacts default 0 until twophase GUC lands.
        max_prepared_xacts: 0,
    }
}

fn read_guc(v: i32, default: i32) -> i32 {
    if v > 0 { v } else { default }
}

// ---------------------------------------------------------------------------
// Shmem sizing reports
// ---------------------------------------------------------------------------

/// PG `NumProcStateSlots` = MaxBackends + NUM_AUXILIARY_PROCS: the sinval
/// per-backend slot count, indexed by ProcNumber.
pub fn num_proc_state_slots() -> usize {
    let c = proc_counts();
    c.max_backends + NUM_AUXILIARY_PROCS as usize
}

/// PG `ProcGlobalSemas`: one sema per backend + one per auxiliary process. Under
/// the async model there are no SysV semaphores, but the count is still reported
/// for compatibility / startup sizing checks.
pub fn ProcGlobalSemas() -> i32 {
    let c = proc_counts();
    c.max_backends as i32 + NUM_AUXILIARY_PROCS
}

/// PG `ProcGlobalShmemSize`: bytes the PGPROC arena + mirror arrays would occupy.
/// No segment is allocated under the Arc model; this is an estimate.
pub fn ProcGlobalShmemSize() -> usize {
    let c = proc_counts();
    let total = c.total();
    total * std::mem::size_of::<PGPROC>()
        + total * std::mem::size_of::<u32>() * 3 // xids/subxidStates/statusFlags mirrors
        + std::mem::size_of::<ProcGlobal>()
}

// ---------------------------------------------------------------------------
// InitProcGlobal -- allocate the arena (once)
// ---------------------------------------------------------------------------

/// PG `InitProcGlobal`: build the PGPROC arena, the mirror arrays, and the free
/// lists, then publish it process-wide. Called once at startup (after the lock
/// tables in ipci.c order). Under the Arc model the arena is `Arc<ProcGlobal>`;
/// `SharedState::new` also constructs it (so a SharedState is self-contained) and
/// this publishes the same handle for the process-wide `proc_global()` accessor.
pub fn InitProcGlobal() {
    if proc_global().is_some() {
        return; // already initialized
    }
    let g = Arc::new(ProcGlobal::new(proc_counts()));
    set_proc_global(g);
}

/// Build a fresh `Arc<ProcGlobal>` for `SharedState::new` (ipci.c ProcArray slot
/// neighbor). Also publishes it process-wide if none is published yet, so the
/// procarray's `proc_global()` reads the same arena a test's SharedState built.
pub fn init_proc_global_shared() -> Arc<ProcGlobal> {
    let g = Arc::new(ProcGlobal::new(proc_counts()));
    set_proc_global(g.clone());
    g
}

// ---------------------------------------------------------------------------
// InitProcess / InitAuxiliaryProcess / ProcKill -- per-backend lifecycle
// ---------------------------------------------------------------------------

/// PG `InitProcess`: claim a free PGPROC slot for this backend and publish its
/// ProcNumber as MyProc. Must run inside a `my_proc_scope` (the backend task
/// wrapper) and after `InitProcGlobal`.
pub fn InitProcess() {
    let g = proc_global().expect("proc header uninitialized").clone();
    // PG: elog(ERROR, "you already exist"). TODO(panic).
    assert!(!has_my_proc(), "you already exist");

    // Decide which freelist supplies our PGPROC (must match InitProcGlobal).
    // PG: autovac worker / special worker -> autovacFreeProcs; bgworker ->
    // bgworkerFreeProcs; wal sender -> walsenderFreeProcs; else freeProcs.
    let kind = match crate::session::try_current().map(|s| s.backend_type()) {
        Some(BackendType::AUTOVAC_WORKER | BackendType::SLOTSYNC_WORKER) => ProcGlobalList::Autovac,
        Some(BackendType::BG_WORKER) => ProcGlobalList::Bgworker,
        Some(BackendType::WAL_SENDER) => ProcGlobalList::Walsender,
        _ => ProcGlobalList::Free,
    };

    let Some(procno) = g.alloc_proc(kind) else {
        // PG: ereport(FATAL, too many clients). TODO(panic).
        panic!("sorry, too many clients already");
    };

    set_current_proc_number(procno);

    // Initialize all fields of MyProc, except those set by InitProcGlobal.
    // SAFETY: this backend exclusively owns its just-claimed slot; no other task
    // references it (it was on the free list until alloc_proc).
    let proc = unsafe { g.proc_mut(procno).unwrap() };
    let pid = crate::session::try_current()
        .map_or(0, |s| s.proc_pid());
    init_backend_proc_fields(proc, procno, pid, /* regular */ true);
    proc.proc_latch.init();
}

/// PG `InitProcessPhase2`: make MyProc visible in the shared ProcArray.
pub fn InitProcessPhase2() {
    let procno = current_proc_number();
    assert!(procno != INVALID_PROC_NUMBER, "InitProcess not done");
    // ProcArrayAdd publishes the proc into the snapshot scan.
    if let Some(s) = current_shared_proc_array() {
        s.proc_array_add(procno);
    }
}

/// PG `InitAuxiliaryProcess`: claim one of the auxiliary PGPROC slots (linear
/// search for a free one, no freelist).
pub fn InitAuxiliaryProcess() {
    let g = proc_global().expect("proc header uninitialized").clone();
    assert!(!has_my_proc(), "you already exist");

    // PG scans + claims the slot under ProcStructLock so two aux tasks starting
    // concurrently never pick (and `&mut`-alias) the same PGPROC. The full field
    // init runs under that same lock: a concurrent scan reads every slot's `pid`,
    // and the init re-touches `pid`, so the two must not overlap.
    let pid = crate::session::try_current().map_or(0, |s| s.proc_pid());
    let procno = g
        .claim_aux_slot(pid, |proc, procno| {
            init_backend_proc_fields(proc, procno, pid, /* regular */ false);
            // Aux procs don't get a VXID.
            proc.vxid.proc_number = INVALID_PROC_NUMBER;
            proc.proc_latch.init();
        })
        .unwrap_or_else(|| panic!("all AuxiliaryProcs are in use"));
    set_current_proc_number(procno);
}

/// Shared field init for InitProcess / InitAuxiliaryProcess.
fn init_backend_proc_fields(proc: &mut PGPROC, procno: ProcNumber, pid: i32, regular: bool) {
    proc.wait_status = ProcWaitStatus::OK;
    proc.fp_vxid_lock = false;
    proc.fp_local_transaction_id = LocalTransactionId(0);
    // Size the fast-path arrays (PG InitProcGlobal sizes them from
    // FastPathLockGroupsPerBackend; default 4 groups = 64 slots). Done here so a
    // proc claimed without the GUC machinery still has usable arrays.
    let groups = {
        let g = unsafe { crate::storage::proc::FastPathLockGroupsPerBackend };
        if g > 0 { g as usize } else { 4 }
    };
    let slots = groups * crate::storage::proc::FP_LOCK_SLOTS_PER_GROUP as usize;
    proc.fp_lock_bits = vec![0u64; groups];
    proc.fp_rel_id = vec![crate::postgres_ext::Oid(0); slots];
    proc.xid = INVALID_TRANSACTION_ID;
    proc.xmin = INVALID_TRANSACTION_ID;
    proc.pid = pid;
    proc.vxid.proc_number = procno;
    proc.vxid.lxid = LocalTransactionId(0);
    proc.database_id = crate::postgres_ext::Oid(0);
    proc.role_id = crate::postgres_ext::Oid(0);
    proc.temp_namespace_id = crate::postgres_ext::Oid(0);
    proc.is_regular_backend = regular;
    proc.delay_chkpt_flags = crate::storage::proc::DelayChkptFlags::empty();
    proc.status_flags = crate::storage::proc::ProcStatusFlags::empty();
    proc.lw_waiting = 0;
    proc.lw_wait_mode = 0;
    proc.wait_lock = None;
    proc.wait_proc_lock = None;
    proc.wait_start = 0;
    proc.recovery_conflict_pending = false;
    proc.wait_lsn = crate::access::xlogdefs::XLogRecPtr(0);
    proc.sync_rep_state = 0;
    proc.proc_array_group_member = false;
    proc.proc_array_group_member_xid = INVALID_TRANSACTION_ID;
    proc.proc_array_group_next = INVALID_PROC_NUMBER as u32;
    proc.wait_event_info = 0;
    proc.clog_group_member = false;
    proc.clog_group_member_xid = INVALID_TRANSACTION_ID;
    proc.clog_group_member_xid_status = crate::access::clog::XidStatus::InProgress;
    proc.clog_group_member_page = -1;
    proc.clog_group_member_lsn = crate::access::xlogdefs::XLogRecPtr(0);
    proc.clog_group_next = INVALID_PROC_NUMBER as u32;
    proc.lock_group_role = crate::storage::proc::LockGroupRole::None;
}

/// PG `ProcKill`: return this backend's PGPROC to its free list and clear MyProc.
/// In C an on_shmem_exit callback; here called explicitly at backend teardown (or
/// from a RAII guard, 15b). Also runs RemoveProcFromArray (ProcArrayRemove).
pub fn ProcKill() {
    let procno = current_proc_number();
    if procno == INVALID_PROC_NUMBER {
        return;
    }
    let Some(g) = proc_global() else {
        return;
    };
    let g = g.clone();

    // RemoveProcFromArray: drop ourselves from the snapshot scan first.
    if let Some(s) = current_shared_proc_array() {
        s.proc_array_remove(
            current_variable_cache().as_deref().unwrap(),
            procno,
            INVALID_TRANSACTION_ID,
        );
    }

    // SAFETY: exclusive owner at teardown; read-only field needed below.
    let kind = unsafe { g.proc_mut(procno).unwrap() }.proc_global_list;
    if kind == ProcGlobalList::None {
        // Aux PGPROC: no freelist. PG `AuxiliaryProcKill` clears the slot under
        // ProcStructLock; we do the same so the field clears + the `pid` release
        // cannot race a concurrent `claim_aux_slot` scan/init on this slot.
        g.release_aux_slot(procno, |proc| {
            proc.vxid.proc_number = INVALID_PROC_NUMBER;
            proc.vxid.lxid = LocalTransactionId(0);
            proc.proc_latch.reset();
        });
    } else {
        // SAFETY: exclusive owner; the freelist push below makes it claimable.
        let proc = unsafe { g.proc_mut(procno).unwrap() };
        proc.vxid.proc_number = INVALID_PROC_NUMBER;
        proc.vxid.lxid = LocalTransactionId(0);
        proc.proc_latch.reset();
        proc.pid = 0;
        g.free_proc(kind, procno);
    }
    set_current_proc_number(INVALID_PROC_NUMBER);
}

// Helpers reaching the shared subsystems without a SharedState handle (proc.c's
// callers hold one; the procarray rewiring uses the process-wide ProcGlobal).
fn current_shared_proc_array() -> Option<Arc<crate::backend::storage::ipc::procarray::ProcArray>> {
    crate::backend::storage::ipc::procarray::current_proc_array()
}
fn current_variable_cache(
) -> Option<Arc<crate::backend::access::transam::transam::VariableCache>> {
    crate::backend::access::transam::transam::current_variable_cache()
}

// ---------------------------------------------------------------------------
// Startup buffer-pin wait id / free-proc count
// ---------------------------------------------------------------------------

/// PG `SetStartupBufferPinWaitBufId`.
pub fn SetStartupBufferPinWaitBufId(bufid: i32) {
    if let Some(g) = proc_global() {
        g.startup_buffer_pin_wait_buf_id
            .store(bufid, std::sync::atomic::Ordering::Relaxed);
    }
}

/// PG `GetStartupBufferPinWaitBufId`.
pub fn GetStartupBufferPinWaitBufId() -> i32 {
    proc_global()
        .map_or(-1, |g| g.startup_buffer_pin_wait_buf_id.load(std::sync::atomic::Ordering::Relaxed))
}

/// PG `HaveNFreeProcs`: (have_enough, n_free) for at least `n` free regular procs.
pub fn HaveNFreeProcs(n: i32) -> (bool, i32) {
    let nfree = proc_global().map_or(0, |g| g.n_free_regular(n));
    (nfree == n, nfree)
}

// ---------------------------------------------------------------------------
// ProcReleaseLocks / LockErrorCleanup
// ---------------------------------------------------------------------------

/// PG `ProcReleaseLocks`: release locks at top-level commit/abort. The
/// LockReleaseAll machinery is lock.c (15b); the wait-queue cleanup is here.
pub fn ProcReleaseLocks(_is_commit: bool) {
    if !has_my_proc() {
        return;
    }
    LockErrorCleanup();
    // LockReleaseAll(DEFAULT_LOCKMETHOD, !is_commit) / (USER_LOCKMETHOD, false):
    // lock.c (15b).
}

/// PG `LockErrorCleanup`: cancel a pending lock wait when aborting. The strong-
/// lock-count revert + wait-queue unlink are lock.c (15b); we call its stubs.
pub fn LockErrorCleanup() {
    crate::storage::lock::abort_strong_lock_acquire();
    if crate::storage::lock::get_awaited_lock().is_none() {
        return;
    }
    // Disable the deadlock/lock timers (ProcSleep owns them in the async model;
    // nothing to disable here). Unlink from the wait queue under the partition
    // lock + GrantAwaitedLock if already granted: lock.c (15b).
    crate::storage::lock::reset_awaited_lock();
}

// ---------------------------------------------------------------------------
// JoinWaitQueue / ProcSleep / ProcWakeup / ProcLockWakeup -- grant-wait
// ---------------------------------------------------------------------------

/// PG `JoinWaitQueue`: insert MyProc into the lock's wait queue at the right spot
/// (soft-deadlock avoidance), set its wait-state, and may detect an early hard
/// deadlock. SYNC; the caller holds the lock partition Mutex.
///
/// Staging: the conflict/grant internals (`LockCheckConflicts`, `GrantLock`,
/// `RememberSimpleDeadLock`) are lock.c (15b); this implements the queue-position
/// and PGPROC wait-state bookkeeping over `LOCK.wait_procs: Vec<ProcNumber>`.
pub fn JoinWaitQueue(
    locallock: &mut LOCALLOCK,
    lock_method_table: LockMethod,
    dont_wait: bool,
) -> ProcWaitStatus {
    let Some(g) = proc_global() else {
        return ProcWaitStatus::ERROR;
    };
    let g = g.clone();
    let procno = current_proc_number();
    if procno == INVALID_PROC_NUMBER {
        return ProcWaitStatus::ERROR;
    }

    let lockmode = locallock.tag.mode;
    // SAFETY: lock.c holds the partition Mutex for `lock` (15b); here the lock
    // pointer comes from the LOCALLOCK the caller owns under that Mutex.
    let lock: &mut LOCK = if locallock.lock.is_null() { return ProcWaitStatus::ERROR } else { unsafe { &mut *locallock.lock } };

    // Set bitmask of locks we already hold on this object (from our PROCLOCK).
    // proclock.hold_mask -> heldLocks: lock.c owns PROCLOCK; staged as 0 until 15b.
    let my_held_locks = held_mask_from_proclock(locallock);
    // SAFETY: exclusive access to our own slot's wait fields under the partition
    // Mutex held by the caller.
    let me = unsafe { g.proc_mut(procno).unwrap() };
    me.held_locks = my_held_locks;

    // Determine insertion point: before the first waiter we'd conflict with.
    let mut insert_before: Option<usize> = None;
    let mut early_deadlock = false;
    if my_held_locks != 0 && !lock.wait_procs.is_empty() {
        let mut ahead_requests: crate::storage::lockdefs::LOCKMASK = 0;
        for (idx, &waiter) in lock.wait_procs.iter().enumerate() {
            // SAFETY: read-only of another waiter's wait fields under the
            // partition Mutex (held by caller); waiters do not mutate them.
            let (w_wait_mode, w_held) = unsafe {
                let w = g.proc(waiter).unwrap();
                (w.wait_lock_mode, w.held_locks)
            };
            // Must the waiter wait for me?
            if conflicts(lock_method_table, w_wait_mode, my_held_locks) {
                // Must I wait for him?
                if conflicts(lock_method_table, lockmode, w_held) {
                    // Deadlock: record + bail (the cleanup happens once on queue).
                    crate::storage::lock::remember_simple_dead_lock(
                        procno, lockmode, lock, waiter,
                    );
                    early_deadlock = true;
                    break;
                }
                // I must go before this waiter; check the special immediate-grant.
                if (lock_method_table.conflict_tab[lockmode as usize] & ahead_requests) == 0
                    && !lock_check_conflicts_staged(lock_method_table, lockmode, locallock)
                {
                    crate::storage::lock::grant_lock(
                        lock,
                        unsafe { &mut *locallock.proclock },
                        lockmode,
                    );
                    return ProcWaitStatus::OK;
                }
                insert_before = Some(idx);
                break;
            }
            ahead_requests |= crate::storage::lock::lockbit_on(w_wait_mode);
        }
    }

    if early_deadlock {
        return ProcWaitStatus::ERROR;
    }
    if dont_wait {
        return ProcWaitStatus::ERROR;
    }

    // Insert self into the queue at the chosen position.
    match insert_before {
        Some(idx) => lock.wait_procs.insert(idx, procno),
        None => lock.wait_procs.push(procno),
    }
    lock.wait_mask |= crate::storage::lock::lockbit_on(lockmode);

    // SAFETY: our own slot, partition Mutex held by caller.
    let me = unsafe { g.proc_mut(procno).unwrap() };
    me.held_locks = my_held_locks;
    me.wait_lock = Some(locallock.lock);
    me.wait_proc_lock = Some(locallock.proclock);
    me.wait_lock_mode = lockmode;
    me.wait_status = ProcWaitStatus::WAITING;
    me.proc_latch.reset();

    ProcWaitStatus::WAITING
}

/// PG `ProcSleep`: await the lock grant. ASYNC; entered with NO partition Mutex
/// held (the lock.c caller drops it). Awaits the proc's Latch (set by ProcWakeup),
/// a deadlock-timeout timer (-> CheckDeadLock), and a lock-timeout timer. NEVER
/// holds a sync guard across the await. Returns OK (granted) or ERROR (deadlock /
/// timeout / cancel).
///
/// PG's `ProcSleep(LOCALLOCK*)` uses the locallock only to find the lock the
/// JoinWaitQueue caller already recorded in the PGPROC wait fields; we read those
/// directly, so no `LOCALLOCK` argument is needed. Dropping it also keeps the
/// future `Send` (a `&mut LOCALLOCK` carries the !Send raw lock pointers).
pub async fn ProcSleep() -> ProcWaitStatus {
    let Some(g) = proc_global() else {
        return ProcWaitStatus::ERROR;
    };
    let g = g.clone();
    let procno = current_proc_number();
    if procno == INVALID_PROC_NUMBER {
        return ProcWaitStatus::ERROR;
    }

    let deadlock_ms = read_guc(unsafe { crate::storage::proc::DeadlockTimeout }, 1000).max(0) as u64;
    let lock_timeout_ms = unsafe { crate::storage::proc::LockTimeout }.max(0) as u64;

    // Record wait-start (no partition lock needed; benign-race in pg_locks).
    {
        // SAFETY: our own slot; scalar write, no aliasing reader relies on it.
        let me = unsafe { g.proc_mut(procno).unwrap() };
        me.wait_start = now_micros();
    }

    let mut deadlock_state = DeadLockState::NotYetChecked;
    // Deadlock timer fires once after deadlock_ms; afterwards we keep waiting on
    // the latch (PG re-arms via the timeout subsystem; we just stop re-arming).
    let deadlock_timer = tokio::time::sleep(Duration::from_millis(deadlock_ms));
    tokio::pin!(deadlock_timer);
    let mut deadlock_armed = true;

    // Lock timeout arm: a far-future sleep when disabled (== 0).
    let lock_timer = tokio::time::sleep(if lock_timeout_ms > 0 {
        Duration::from_millis(lock_timeout_ms)
    } else {
        Duration::from_secs(u64::MAX / 2)
    });
    tokio::pin!(lock_timer);

    loop {
        let latch_ref = {
            // SAFETY: the Latch lives in the fixed arena for the process lifetime;
            // taking a &Latch here is sound (the slot is never freed/moved while we
            // hold MyProc). We do NOT hold any sync guard across the await below.
            let proc = unsafe { g.proc(procno).unwrap() };
            &proc.proc_latch
        };

        tokio::select! {
            biased;
            () = latch_ref.wait() => {
                latch_ref.reset();
                // Woken: read waitStatus once.
                let status = unsafe { g.proc(procno).unwrap().wait_status };
                if status != ProcWaitStatus::WAITING {
                    return status;
                }
                // Spurious wake (other latch source): loop and wait again.
            }
            () = &mut deadlock_timer, if deadlock_armed => {
                deadlock_armed = false;
                deadlock_state = CheckDeadLock(procno, &g);
                if deadlock_state == DeadLockState::HardDeadlock {
                    // CheckDeadLock set waitStatus = ERROR via RemoveFromWaitQueue.
                    let status = unsafe { g.proc(procno).unwrap().wait_status };
                    if status != ProcWaitStatus::WAITING {
                        return status;
                    }
                }
                // Soft/no deadlock: keep waiting on the latch.
            }
            () = &mut lock_timer, if lock_timeout_ms > 0 => {
                // Lock timeout: just SIGNAL the outcome. We do NOT touch the LOCK
                // wait queue here (doing so lock-free would race a concurrent
                // releaser's ProcLockWakeup under the partition Mutex). lock.c's
                // `wait_on_lock` WaitGuard does the partition-locked cleanup
                // (RemoveFromWaitQueue + CleanUpLock) -- it knows the partition.
                let _ = deadlock_state;
                return ProcWaitStatus::ERROR;
            }
        }
    }
}

/// PG `ProcWakeup`: wake `procno` by setting its latch, after removing it from the
/// lock's wait queue and passing it `wait_status`. SYNC; caller holds the
/// partition Mutex.
pub fn ProcWakeup(procno: ProcNumber, wait_status: ProcWaitStatus) {
    let Some(g) = proc_global() else {
        return;
    };
    let g = g.clone();
    // SAFETY: partition Mutex held by caller gates the wait fields + wait_lock's
    // wait_procs queue.
    let (lock_ptr, latch_ok) = unsafe {
        let Some(proc) = g.proc_mut(procno) else {
            return;
        };
        if proc.wait_status != ProcWaitStatus::WAITING {
            return;
        }
        let lock_ptr = proc.wait_lock;
        proc.wait_lock = None;
        proc.wait_proc_lock = None;
        proc.wait_status = wait_status;
        proc.wait_start = 0;
        (lock_ptr, true)
    };
    // Remove from the LOCK.wait_procs queue.
    if let Some(lp) = lock_ptr {
        // SAFETY: lock.c partition Mutex held by caller.
        let lock = unsafe { &mut *lp };
        if let Some(pos) = lock.wait_procs.iter().position(|&p| p == procno) {
            lock.wait_procs.remove(pos);
        }
    }
    if latch_ok {
        // SAFETY: Latch lives in the process-lifetime arena.
        unsafe { g.proc(procno).unwrap().proc_latch.set() };
    }
}

/// PG `ProcLockWakeup`: scan a released lock's waiters and grant + wake those no
/// longer blocked. SYNC; caller holds the partition Mutex.
///
/// Staging: `LockCheckConflicts`/`GrantLock` are lock.c (15b). We walk
/// `LOCK.wait_procs` and call the (stub) conflict check; once 15b lands the grant
/// logic is complete.
pub fn ProcLockWakeup(lock_method_table: LockMethod, lock: &mut LOCK) {
    let Some(g) = proc_global() else {
        return;
    };
    let g = g.clone();
    if lock.wait_procs.is_empty() {
        return;
    }
    let mut ahead_requests: crate::storage::lockdefs::LOCKMASK = 0;
    // Snapshot the queue order; ProcWakeup mutates wait_procs as it grants.
    let waiters: Vec<ProcNumber> = lock.wait_procs.clone();
    for procno in waiters {
        // SAFETY: partition Mutex held by caller.
        let (lockmode, wait_proc_lock) = unsafe {
            let Some(proc) = g.proc(procno) else {
                continue;
            };
            (proc.wait_lock_mode, proc.wait_proc_lock)
        };
        let conflicts_ahead =
            (lock_method_table.conflict_tab[lockmode as usize] & ahead_requests) != 0;
        if !conflicts_ahead && !lock_check_conflicts_proclock(lock_method_table, lockmode, lock, wait_proc_lock)
        {
            if let Some(pl) = wait_proc_lock {
                // SAFETY: lock.c PROCLOCK under the partition Mutex.
                crate::storage::lock::grant_lock(lock, unsafe { &mut *pl }, lockmode);
            }
            ProcWakeup(procno, ProcWaitStatus::OK);
        } else {
            ahead_requests |= crate::storage::lock::lockbit_on(lockmode);
        }
    }
}

/// PG `CheckDeadLock`: acquire ALL partition locks in index order, run
/// `DeadLockCheck` (deadlock.c, 15c), apply soft rearrangement / set the hard
/// error / handle a blocking autovacuum, release the partition locks in reverse.
/// SYNC; no `.await` while any partition lock is held (rules s5).
fn CheckDeadLock(procno: ProcNumber, g: &ProcGlobal) -> DeadLockState {
    let Some(m) = crate::storage::lock::lock_manager() else {
        return DeadLockState::NoDeadlock;
    };
    let m = m.clone();

    // Hold every partition Mutex across the whole graph walk so it can deref the
    // boxed LOCK/PROCLOCK + read any PGPROC's wait fields safely.
    m.with_all_partitions_locked(|view| {
        // If we were granted in the interim (no longer queued), happy day.
        // SAFETY: all partition locks held; read of our wait_lock.
        let still_waiting = unsafe {
            g.proc(procno)
                .is_some_and(|p| p.wait_lock.is_some() && p.wait_status == ProcWaitStatus::WAITING)
        };
        if !still_waiting {
            return DeadLockState::NoDeadlock;
        }

        // Run the deadlock check (deadlock.c, 15c). Operates over ProcNumber,
        // reading holders/waiters through the locked-tables view.
        let state = crate::storage::lock::dead_lock_check(procno, view);

        #[allow(clippy::match_same_arms, reason = "arms kept separate for port clarity")]
        match state {
            DeadLockState::HardDeadlock => {
                // RemoveFromWaitQueue (lock.c): unlink us + fix the lock's request
                // counts + wake trailing waiters + set waitStatus = ERROR, so
                // ProcSleep returns ERROR and LockAcquire raises the deadlock report.
                crate::storage::lock::RemoveFromWaitQueue(procno, view);
            }
            DeadLockState::SoftDeadlock => {
                // dead_lock_check already rearranged the wait queues + ran
                // ProcLockWakeup on the now-grantable; we keep waiting on the latch.
            }
            DeadLockState::BlockedByAutovacuum => {
                // PG sends SIGINT to the autovac worker (GetBlockingAutoVacuumPgproc)
                // then keeps waiting. The autovac-worker cancel path is not wired
                // yet; surface the proc for the caller and keep waiting.
                // TODO(autovac): cancel the blocking autovacuum worker.
                let _ = crate::storage::lock::get_blocking_autovacuum_pgproc();
            }
            DeadLockState::NoDeadlock | DeadLockState::NotYetChecked => {}
        }
        state
    })
}

/// PG `CheckDeadLockAlert`: the deadlock_timeout handler. In C it sets a flag +
/// the latch from a signal handler; in the async model ProcSleep's `select!`
/// deadlock arm IS the timer, so this is only the latch nudge for a backend whose
/// timer the supervisor wants to force.
pub fn CheckDeadLockAlert() {
    let Some(g) = proc_global() else {
        return;
    };
    let procno = current_proc_number();
    if procno != INVALID_PROC_NUMBER {
        // SAFETY: Latch in the process-lifetime arena.
        unsafe { g.proc(procno).unwrap().proc_latch.set() };
    }
}

// --- lock-conflict shims (lock.c internals staged) ---

/// `lockMethodTable->conflictTab[mode] & mask != 0`.
fn conflicts(
    table: LockMethod,
    mode: crate::storage::lock::LOCKMODE,
    mask: crate::storage::lockdefs::LOCKMASK,
) -> bool {
    (table.conflict_tab[mode as usize] & mask) != 0
}

/// Bitmask of locks held on this object from our PROCLOCK. lock.c owns PROCLOCK;
/// staged as 0 (no held locks) until 15b populates `locallock.proclock`.
fn held_mask_from_proclock(locallock: &LOCALLOCK) -> crate::storage::lockdefs::LOCKMASK {
    if locallock.proclock.is_null() {
        0
    } else {
        // SAFETY: lock.c PROCLOCK under the partition Mutex (15b path).
        unsafe { (*locallock.proclock).hold_mask }
    }
}

/// `LockCheckConflicts` over the LOCALLOCK's proclock (lock.c, 15b stub).
fn lock_check_conflicts_staged(
    table: LockMethod,
    mode: crate::storage::lock::LOCKMODE,
    locallock: &mut LOCALLOCK,
) -> bool {
    if locallock.lock.is_null() || locallock.proclock.is_null() {
        return false;
    }
    // SAFETY: lock.c LOCK/PROCLOCK under the partition Mutex (15b path).
    unsafe {
        crate::storage::lock::lock_check_conflicts(
            table,
            mode,
            &mut *locallock.lock,
            &mut *locallock.proclock,
        )
    }
}

/// `LockCheckConflicts` over a waiter's PROCLOCK pointer (lock.c, 15b stub).
fn lock_check_conflicts_proclock(
    table: LockMethod,
    mode: crate::storage::lock::LOCKMODE,
    lock: &mut LOCK,
    proclock: Option<*mut crate::storage::lock::PROCLOCK>,
) -> bool {
    // SAFETY: lock.c PROCLOCK under the partition Mutex (15b path).
    proclock.is_some_and(|pl| unsafe {
        crate::storage::lock::lock_check_conflicts(table, mode, lock, &mut *pl)
    })
}

// ---------------------------------------------------------------------------
// GetLockHoldersAndWaiters / signals / lock groups
// ---------------------------------------------------------------------------

/// PG `GetLockHoldersAndWaiters`: append holder/waiter PIDs into the StringInfos,
/// return holder count. The PROCLOCK walk is lock.c (15b); staged empty.
pub fn GetLockHoldersAndWaiters(
    _locallock: &mut LOCALLOCK,
    _lock_holders_sbuf: &mut StringInfo,
    _lock_waiters_sbuf: &mut StringInfo,
) -> i32 {
    // The lock->procLocks list lives in lock.c (15b); nothing to enumerate yet.
    0
}

/// PG `ProcWaitForSignal`: wait on MyProc's latch for a generic signal.
pub async fn ProcWaitForSignal(_wait_event_info: u32) {
    let Some(g) = proc_global() else {
        return;
    };
    let g = g.clone();
    let procno = current_proc_number();
    if procno == INVALID_PROC_NUMBER {
        return;
    }
    // SAFETY: Latch in the process-lifetime arena; no sync guard across await.
    let latch = unsafe { &g.proc(procno).unwrap().proc_latch };
    latch.wait().await;
    latch.reset();
}

/// PG `ProcSendSignal`: set the latch of the backend identified by `procno`.
pub fn ProcSendSignal(procno: ProcNumber) {
    let Some(g) = proc_global() else {
        return;
    };
    if procno < 0 || procno as u32 >= g.all_proc_count {
        return; // PG: elog(ERROR, procNumber out of range)
    }
    // SAFETY: Latch in the process-lifetime arena.
    if let Some(p) = unsafe { g.proc(procno) } {
        p.proc_latch.set();
    }
}

/// PG `AuxiliaryPidGetProc`: ProcNumber of the auxiliary proc with `pid`, or None.
pub fn AuxiliaryPidGetProc(pid: i32) -> Option<ProcNumber> {
    if pid == 0 {
        return None;
    }
    let g = proc_global()?;
    let base = g.aux_proc_base;
    for i in 0..NUM_AUXILIARY_PROCS {
        let procno = base + i;
        // SAFETY: lifecycle read; pid is set under ProcStructLock.
        if unsafe { g.proc(procno).is_some_and(|p| p.pid == pid) } {
            return Some(procno);
        }
    }
    None
}

/// PG `BecomeLockGroupLeader`: make MyProc a single-member lock group leader.
pub fn BecomeLockGroupLeader() {
    let Some(g) = proc_global() else {
        return;
    };
    let procno = current_proc_number();
    if procno == INVALID_PROC_NUMBER {
        return;
    }
    // SAFETY: lock.c partition-by-proc Mutex would gate this (15b); the group
    // links are otherwise touched only by this backend at setup.
    let me = unsafe { g.proc_mut(procno).unwrap() };
    // Already a leader (PG `lockGroupLeader == procno`)? Nothing to do.
    if matches!(me.lock_group_role, LockGroupRole::Leader { .. }) {
        return;
    }
    // PG asserts `lockGroupLeader == INVALID` here -- we must not be in a group.
    debug_assert!(matches!(me.lock_group_role, LockGroupRole::None));
    // Become a leader of my own single-member group (the self-membership PG adds).
    me.lock_group_role = LockGroupRole::Leader {
        members: vec![procno],
    };
}

/// PG `BecomeLockGroupMember`: join `leader`'s lock group, gated on its pid as an
/// interlock against PGPROC recycling. Returns whether we joined.
pub fn BecomeLockGroupMember(leader: ProcNumber, pid: i32) -> bool {
    let Some(g) = proc_global() else {
        return false;
    };
    let procno = current_proc_number();
    if procno == INVALID_PROC_NUMBER || procno == leader || pid == 0 {
        return false;
    }
    // SAFETY: lock.c partition-by-proc Mutex would gate this (15b).
    let (leader_pid, leader_is_leader) = unsafe {
        let Some(l) = g.proc(leader) else {
            return false;
        };
        // PG checks `leader->lockGroupLeader == leader`: the leader must actually
        // be a group leader.
        (l.pid, matches!(l.lock_group_role, LockGroupRole::Leader { .. }))
    };
    if leader_pid == pid && leader_is_leader {
        unsafe {
            g.proc_mut(procno).unwrap().lock_group_role = LockGroupRole::Member { leader };
            if let LockGroupRole::Leader { members } =
                &mut g.proc_mut(leader).unwrap().lock_group_role
            {
                members.push(procno);
            }
        }
        true
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

/// Microseconds since the Unix epoch (wait-start timestamp; benign-race hint).
fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_state::{SharedState, SharedStateConfig};
    use crate::storage::proc::my_proc_scope;

    fn shared() -> Arc<SharedState> {
        SharedState::new(SharedStateConfig::default())
    }

    #[test]
    fn init_proc_global_populates_arena() {
        let _s = shared(); // builds + publishes ProcGlobal
        let g = proc_global().expect("ProcGlobal published by SharedState::new");
        assert!(!g.is_empty(), "arena has slots");
        // A regular-backend slot is available on the free list.
        let (have, n) = HaveNFreeProcs(1);
        assert!(have, "at least one free regular proc");
        assert_eq!(n, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn init_process_claims_and_frees_a_slot() {
        let s = shared();
        my_proc_scope(crate::session::scope(
            Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND)),
            async {
                assert!(!has_my_proc());
                InitProcess();
                assert!(has_my_proc());
                let procno = current_proc_number();
                // The claimed slot carries our pid + a valid vxid proc_number.
                let g = proc_global().unwrap();
                let pid = unsafe { g.proc(procno).unwrap().pid };
                assert!(pid != 0);
                // Make it visible in the procarray, then tear down.
                InitProcessPhase2();
                let _ = &s;
                ProcKill();
                assert!(!has_my_proc());
            },
        ))
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn procsleep_wakes_on_procwakeup() {
        let s = shared();
        let _ = &s;
        let g = proc_global().unwrap().clone();
        my_proc_scope(crate::session::scope(
            Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND)),
            async move {
                InitProcess();
                let procno = current_proc_number();
                // Put ourselves in WAITING (as JoinWaitQueue would, minus a LOCK).
                unsafe {
                    let me = g.proc_mut(procno).unwrap();
                    me.wait_status = ProcWaitStatus::WAITING;
                    me.proc_latch.reset();
                }
                // Wake from another task after a short delay.
                let g2 = g.clone();
                let waker = tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    // ProcWakeup with OK: set status + latch.
                    unsafe {
                        g2.proc_mut(procno).unwrap().wait_status = ProcWaitStatus::OK;
                        g2.proc(procno).unwrap().proc_latch.set();
                    }
                });
                // No lock timeout, large deadlock timeout: must wake via latch.
                unsafe {
                    crate::storage::proc::LockTimeout = 0;
                    crate::storage::proc::DeadlockTimeout = 60_000;
                }
                let status = ProcSleep().await;
                assert_eq!(status, ProcWaitStatus::OK);
                waker.await.unwrap();
                ProcKill();
            },
        ))
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn procsleep_times_out_on_lock_timeout() {
        let s = shared();
        let _ = &s;
        let g = proc_global().unwrap().clone();
        my_proc_scope(crate::session::scope(
            Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND)),
            async move {
                InitProcess();
                let procno = current_proc_number();
                unsafe {
                    let me = g.proc_mut(procno).unwrap();
                    me.wait_status = ProcWaitStatus::WAITING;
                    me.wait_lock = None;
                    me.proc_latch.reset();
                    // Short lock timeout, long deadlock timeout.
                    crate::storage::proc::LockTimeout = 30;
                    crate::storage::proc::DeadlockTimeout = 60_000;
                }
                let status = ProcSleep().await;
                assert_eq!(status, ProcWaitStatus::ERROR, "lock timeout -> ERROR");
                // Reset GUC so other tests are unaffected.
                unsafe { crate::storage::proc::LockTimeout = 0 };
                ProcKill();
            },
        ))
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn procsleep_runs_deadlock_check_on_timeout() {
        let s = shared();
        let _ = &s;
        let g = proc_global().unwrap().clone();
        my_proc_scope(crate::session::scope(
            Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND)),
            async move {
                InitProcess();
                let procno = current_proc_number();
                unsafe {
                    let me = g.proc_mut(procno).unwrap();
                    me.wait_status = ProcWaitStatus::WAITING;
                    me.wait_lock = None; // no LOCK: CheckDeadLock sees not-queued
                    me.proc_latch.reset();
                    // Deadlock fires fast; no lock timeout. With the 15c stub
                    // returning NoDeadlock and us not actually queued, the check
                    // returns NoDeadlock and we keep waiting -> wake via latch.
                    crate::storage::proc::LockTimeout = 0;
                    crate::storage::proc::DeadlockTimeout = 20;
                }
                // After the deadlock check runs, wake us OK from another task.
                let g2 = g.clone();
                let waker = tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    unsafe {
                        g2.proc_mut(procno).unwrap().wait_status = ProcWaitStatus::OK;
                        g2.proc(procno).unwrap().proc_latch.set();
                    }
                });
                let status = ProcSleep().await;
                assert_eq!(status, ProcWaitStatus::OK);
                waker.await.unwrap();
                unsafe { crate::storage::proc::DeadlockTimeout = 1000 };
                ProcKill();
            },
        ))
        .await;
    }

    fn dummy_locallock() -> LOCALLOCK {
        LOCALLOCK {
            tag: crate::storage::lock::LOCALLOCKTAG {
                lock: crate::storage::lock::LOCKTAG::set_relation(1, 2),
                mode: 1,
            },
            hashcode: 0,
            lock: std::ptr::null_mut(),
            proclock: std::ptr::null_mut(),
            n_locks: 0,
            lock_owners: Vec::new(),
            holds_strong_lock_count: false,
            lock_cleared: false,
        }
    }
}
