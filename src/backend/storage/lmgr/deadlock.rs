//! Translated from PostgreSQL src/backend/storage/lmgr/deadlock.c
//!
//! The deadlock detector: `DeadLockCheck` builds the waits-for graph from the
//! lock tables, looks for a cycle (FindLockCycle DFS, distinguishing HARD edges
//! -- a proc holds a conflicting lock -- from SOFT edges -- a proc is merely
//! ahead in the same wait queue and could be reordered), and tries to find a
//! deadlock-free reordering of the soft edges (DeadLockCheckRecurse ->
//! TestConfiguration -> ExpandConstraints -> TopoSort). A soft solution is
//! applied by rewriting each LOCK.wait_procs queue and waking the now-grantable
//! waiters (ProcLockWakeup); no solution is a hard deadlock (ERROR).
//!
//! Locking (design step15 s4): `DeadLockCheck` is SYNC and runs with ALL
//! `NUM_LOCK_PARTITIONS` partition Mutexes held (acquired by `CheckDeadLock` in
//! proc.c). Because every partition is locked, the graph walk can safely deref the
//! `*mut LOCK`/`*mut PROCLOCK` it finds and read any PGPROC slot's wait fields. No
//! `.await` happens here.
//!
//! Representation: PG keeps `PGPROC *`/`LOCK *` in EDGE/WAIT_ORDER; we use
//! `ProcNumber` for procs (the arena identity, Send-friendly) and `*mut LOCK` for
//! locks (the boxed shard entries are stable while their partition is locked). The
//! per-call workspaces (visited/topo/constraints/wait-orders) are allocated on
//! demand sized from `MaxBackends` (NOT shmem); PG does this once per backend in
//! `InitDeadLockChecking`.
//!
//! Lock groups are single-member until F4: a proc's group leader is itself, so the
//! group-member loops collapse. The structure is faithful so F4 can fill them.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use crate::storage::lock::{DeadLockState, LOCK, LOCKMODE, LOCKTAG, LockTagType, lockbit_on};
use crate::storage::lockdefs::LOCKMASK;
use crate::storage::proc::{LockGroupRole, ProcGlobal, proc_global};
use crate::storage::procnumber::{INVALID_PROC_NUMBER, ProcNumber};

use super::lock::{GetLocksMethodTable, LockTablesView};
use super::proc::ProcLockWakeup;

// ---------------------------------------------------------------------------
// Workspace types (deadlock.c-local EDGE / WAIT_ORDER / DEADLOCK_INFO)
// ---------------------------------------------------------------------------

/// One edge in the waits-for graph (PG `EDGE`). `waiter`/`blocker` are the lock
/// group leaders (== the proc itself for single-member groups).
#[derive(Clone, Copy)]
struct Edge {
    waiter: ProcNumber,
    blocker: ProcNumber,
    lock: *mut LOCK,
    /// Workspace for TopoSort: array index of the before-proc.
    pred: i32,
    /// Workspace for TopoSort: list link (1-based, 0 = end).
    link: i32,
}

/// One potential reordering of a lock's wait queue (PG `WAIT_ORDER`).
struct WaitOrder {
    lock: *mut LOCK,
    /// Procs in the new wait order.
    procs: Vec<ProcNumber>,
}

/// Info saved about each edge in a detected cycle, for the error message
/// (PG `DEADLOCK_INFO`). Extracted so it survives releasing the partition locks.
#[derive(Clone, Copy)]
struct DeadlockInfo {
    locktag: LOCKTAG,
    lockmode: LOCKMODE,
    pid: i32,
}

impl DeadlockInfo {
    fn zero() -> Self {
        Self {
            locktag: LOCKTAG::set_relation(0, 0),
            lockmode: 0,
            pid: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Per-call working space (PG's TopMemoryContext statics, allocated per backend)
// ---------------------------------------------------------------------------

/// All the deadlock-detector scratch arrays. PG sizes these once in
/// `InitDeadLockChecking` from MaxBackends and reuses them; we allocate one per
/// `DeadLockCheck` call (MaxBackends is bounded, and a deadlock check is rare).
struct DeadLockCtx {
    max_backends: usize,

    /// FindLockCycle: visited procs (PG `visitedProcs`).
    visited_procs: Vec<ProcNumber>,
    /// FindLockCycle output: edges of the detected cycle (PG `deadlockDetails`).
    deadlock_details: Vec<DeadlockInfo>,
    n_deadlock_details: usize,

    /// TopoSort: counts of remaining before-constraints (PG `beforeConstraints`).
    before_constraints: Vec<i32>,
    /// TopoSort: list head per proc for after-constraints (PG `afterConstraints`).
    after_constraints: Vec<i32>,

    /// ExpandConstraints output (PG `waitOrders`).
    wait_orders: Vec<WaitOrder>,

    /// The constraint set under consideration (PG `curConstraints`).
    cur_constraints: Vec<Edge>,
    /// Saved edge lists from FindLockCycle (PG `possibleConstraints`).
    possible_constraints: Vec<Edge>,
    n_possible_constraints: usize,
    max_possible_constraints: usize,

    /// PGPROC of any blocking autovacuum worker found (PG
    /// `blocking_autovacuum_proc`).
    blocking_autovacuum_proc: ProcNumber,
}

impl DeadLockCtx {
    fn new(max_backends: usize) -> Self {
        // PG InitDeadLockChecking sizing:
        //   visitedProcs/deadlockDetails: MaxBackends
        //   topoProcs (== visitedProcs), before/afterConstraints: MaxBackends
        //   curConstraints: maxCurConstraints = MaxBackends
        //   possibleConstraints: 4*MaxBackends (last MaxBackends reserved for the
        //     FindLockCycle output window).
        let max_possible = max_backends * 4;
        Self {
            max_backends,
            visited_procs: Vec::with_capacity(max_backends),
            deadlock_details: vec![DeadlockInfo::zero(); max_backends],
            n_deadlock_details: 0,
            before_constraints: vec![0; max_backends],
            after_constraints: vec![0; max_backends],
            wait_orders: Vec::new(),
            cur_constraints: Vec::with_capacity(max_backends),
            possible_constraints: vec![
                Edge {
                    waiter: INVALID_PROC_NUMBER,
                    blocker: INVALID_PROC_NUMBER,
                    lock: std::ptr::null_mut(),
                    pred: 0,
                    link: 0,
                };
                max_possible
            ],
            n_possible_constraints: 0,
            max_possible_constraints: max_possible,
            blocking_autovacuum_proc: INVALID_PROC_NUMBER,
        }
    }
}

fn max_backends() -> usize {
    let m = unsafe { crate::miscadmin::MaxBackends };
    if m > 0 { m as usize } else { 256 }
}

// ---------------------------------------------------------------------------
// Arena access helpers (all under the held partition locks)
// ---------------------------------------------------------------------------

/// The group leader of `procno` (PG `proc->lockGroupLeader`); itself if none.
/// SAFETY: caller holds all partition locks.
unsafe fn leader_of(g: &ProcGlobal, procno: ProcNumber) -> ProcNumber {
    // PG `proc->lockGroupLeader`: a member points at its leader; a leader points
    // at itself; not-in-a-group is NULL. All non-Member cases resolve to `procno`.
    unsafe { g.proc(procno) }.map_or(procno, |p| match p.lock_group_role {
        LockGroupRole::Member { leader } => leader,
        LockGroupRole::Leader { .. } | LockGroupRole::None => procno,
    })
}

/// PG `LOCK_LOCKTAG(*lock)`: the lock's tag type.
unsafe fn lock_locktag(lock: *mut LOCK) -> u8 {
    unsafe { (*lock).tag.locktag_type }
}

// ---------------------------------------------------------------------------
// DeadLockCheck -- the entry point (SYNC, all partition locks held)
// ---------------------------------------------------------------------------

/// PG `DeadLockCheck`. Looks for deadlocks involving `proc`; tries to rearrange
/// wait queues to resolve them. Returns the resulting `DeadLockState`. On a hard
/// deadlock the cycle is recorded in the (per-call) details for `DeadLockReport`.
///
/// Caller (CheckDeadLock in proc.c) must already hold all partition locks; the
/// `view` over those locked tables enumerates each lock's holders.
pub fn DeadLockCheck(proc: ProcNumber, view: &LockTablesView) -> DeadLockState {
    let Some(g) = proc_global() else {
        return DeadLockState::NoDeadlock;
    };
    let g = g.clone();
    let mut ctx = DeadLockCtx::new(max_backends());

    // Initialize to "no constraints" + not blocked by autovacuum.
    ctx.cur_constraints.clear();
    ctx.n_possible_constraints = 0;
    ctx.wait_orders.clear();
    ctx.blocking_autovacuum_proc = INVALID_PROC_NUMBER;

    // Search for deadlocks and possible fixes.
    if dead_lock_check_recurse(&mut ctx, &g, view, proc) {
        // Re-run FindLockCycle once on the basic (un-rearranged) state to record
        // the correct deadlockDetails[] for the report.
        ctx.wait_orders.clear();
        let mut soft_edges = Vec::new();
        // PG elog(FATAL). TODO(panic): "deadlock seems to have disappeared".
        assert!(find_lock_cycle(&mut ctx, &g, view, proc, &mut soft_edges), "deadlock seems to have disappeared");
        publish_deadlock_details(&ctx);
        return DeadLockState::HardDeadlock;
    }

    // Apply any needed rearrangements of wait queues.
    let n_wait_orders = ctx.wait_orders.len();
    for i in 0..n_wait_orders {
        let lock = ctx.wait_orders[i].lock;
        // Reset the queue and re-add procs in the desired order.
        // SAFETY: all partition locks held; the boxed LOCK is alive.
        unsafe {
            (*lock).wait_procs.clear();
            for &procno in &ctx.wait_orders[i].procs {
                (*lock).wait_procs.push(procno);
            }
            // See if any waiters for the lock can be woken up now.
            let method = GetLocksMethodTable(&*lock);
            ProcLockWakeup(method, &mut *lock);
        }
    }

    // Return code tells the caller whether we escaped a deadlock.
    if n_wait_orders > 0 {
        DeadLockState::SoftDeadlock
    } else if ctx.blocking_autovacuum_proc != INVALID_PROC_NUMBER {
        // Publish it for GetBlockingAutoVacuumPgproc (the caller cancels it).
        publish_blocking_autovacuum(ctx.blocking_autovacuum_proc);
        DeadLockState::BlockedByAutovacuum
    } else {
        DeadLockState::NoDeadlock
    }
}

// ---------------------------------------------------------------------------
// Per-call details / blocking-autovac handoff
// ---------------------------------------------------------------------------

// PG keeps deadlockDetails[] + blocking_autovacuum_proc as file statics so
// DeadLockReport/GetBlockingAutoVacuumPgproc can read them after the partition
// locks drop. We mirror that with module statics published at the end of a check
// (a check runs once at a time, under all partition locks, in one task).

use parking_lot::Mutex;

struct PublishedDetails {
    details: Vec<DeadlockInfo>,
    n: usize,
    blocking_autovacuum_proc: ProcNumber,
}

static PUBLISHED: Mutex<Option<PublishedDetails>> = Mutex::new(None);

fn publish_deadlock_details(ctx: &DeadLockCtx) {
    let mut p = PUBLISHED.lock();
    *p = Some(PublishedDetails {
        details: ctx.deadlock_details[..ctx.n_deadlock_details].to_vec(),
        n: ctx.n_deadlock_details,
        blocking_autovacuum_proc: ctx.blocking_autovacuum_proc,
    });
}

/// PG `GetBlockingAutoVacuumPgproc`: the autovacuum worker blocking our proc, if
/// the last check found one. Reset as soon as it is passed back.
pub fn GetBlockingAutoVacuumPgproc() -> Option<ProcNumber> {
    let mut p = PUBLISHED.lock();
    let pd = p.as_mut()?;
    let v = pd.blocking_autovacuum_proc;
    pd.blocking_autovacuum_proc = INVALID_PROC_NUMBER;
    if v == INVALID_PROC_NUMBER { None } else { Some(v) }
}

/// Record the blocking-autovac proc found during a check (DeadLockCheck stores it
/// in the ctx; this is the cross-call handoff for the non-hard-deadlock case).
fn publish_blocking_autovacuum(procno: ProcNumber) {
    let mut p = PUBLISHED.lock();
    match p.as_mut() {
        Some(pd) => pd.blocking_autovacuum_proc = procno,
        None => {
            *p = Some(PublishedDetails {
                details: Vec::new(),
                n: 0,
                blocking_autovacuum_proc: procno,
            });
        }
    }
}

// ---------------------------------------------------------------------------
// DeadLockCheckRecurse -- try soft-edge reversals to escape the cycle
// ---------------------------------------------------------------------------

/// PG `DeadLockCheckRecurse`. Returns true if NO solution exists (hard deadlock),
/// false if a deadlock-free state is attainable (waitOrders shows the needed
/// rearrangements).
fn dead_lock_check_recurse(
    ctx: &mut DeadLockCtx,
    g: &ProcGlobal,
    view: &LockTablesView,
    proc: ProcNumber,
) -> bool {
    let n_edges = test_configuration(ctx, g, view, proc);
    if n_edges < 0 {
        return true; // hard deadlock -- no solution
    }
    if n_edges == 0 {
        return false; // good configuration found
    }
    if ctx.cur_constraints.len() >= ctx.max_backends {
        return true; // out of room for active constraints
    }

    let old_possible_constraints = ctx.n_possible_constraints;
    let saved_list =
        ctx.n_possible_constraints + n_edges as usize + ctx.max_backends
            <= ctx.max_possible_constraints;
    if saved_list {
        ctx.n_possible_constraints += n_edges as usize;
    }

    // Try each available soft edge as an addition to the configuration.
    for i in 0..n_edges as usize {
        if !saved_list && i > 0 {
            // Regenerate the list of possible added constraints.
            assert!(n_edges == test_configuration(ctx, g, view, proc), "inconsistent results during deadlock check");
        }
        let edge = ctx.possible_constraints[old_possible_constraints + i];
        ctx.cur_constraints.push(edge);
        if !dead_lock_check_recurse(ctx, g, view, proc) {
            return false; // found a valid solution!
        }
        // Give up on that added constraint, try again.
        ctx.cur_constraints.pop();
    }
    ctx.n_possible_constraints = old_possible_constraints;
    true // no solution found
}

// ---------------------------------------------------------------------------
// TestConfiguration -- validate the current constraint set
// ---------------------------------------------------------------------------

/// PG `TestConfiguration`. Returns 0 (good), -1 (hard deadlock / inconsistent),
/// or >0 (number of soft edges in one chosen soft cycle, written into
/// possibleConstraints+nPossibleConstraints).
fn test_configuration(
    ctx: &mut DeadLockCtx,
    g: &ProcGlobal,
    view: &LockTablesView,
    start_proc: ProcNumber,
) -> i32 {
    // Make sure we have room for FindLockCycle's output.
    if ctx.n_possible_constraints + ctx.max_backends > ctx.max_possible_constraints {
        return -1;
    }

    // Expand the current constraints into wait orderings; fail if inconsistent.
    if !expand_constraints(ctx, g) {
        return -1;
    }

    let mut soft_found = 0i32;

    // Check for cycles involving startProc or any proc mentioned in constraints.
    // We check startProc last (so a soft cycle on it is dealt with first).
    let n_cur = ctx.cur_constraints.len();
    for i in 0..n_cur {
        let waiter = ctx.cur_constraints[i].waiter;
        let blocker = ctx.cur_constraints[i].blocker;

        let mut soft_edges = Vec::new();
        if find_lock_cycle(ctx, g, view, waiter, &mut soft_edges) {
            if soft_edges.is_empty() {
                return -1; // hard deadlock
            }
            soft_found = save_soft_edges(ctx, &soft_edges);
        }
        let mut soft_edges = Vec::new();
        if find_lock_cycle(ctx, g, view, blocker, &mut soft_edges) {
            if soft_edges.is_empty() {
                return -1;
            }
            soft_found = save_soft_edges(ctx, &soft_edges);
        }
    }
    let mut soft_edges = Vec::new();
    if find_lock_cycle(ctx, g, view, start_proc, &mut soft_edges) {
        if soft_edges.is_empty() {
            return -1;
        }
        soft_found = save_soft_edges(ctx, &soft_edges);
    }
    soft_found
}

/// Stash one soft-cycle's edges at possibleConstraints+nPossibleConstraints (the
/// FindLockCycle output window PG returns directly). Returns the edge count.
fn save_soft_edges(ctx: &mut DeadLockCtx, soft_edges: &[Edge]) -> i32 {
    let base = ctx.n_possible_constraints;
    for (i, e) in soft_edges.iter().enumerate() {
        if base + i < ctx.possible_constraints.len() {
            ctx.possible_constraints[base + i] = *e;
        }
    }
    soft_edges.len() as i32
}

// ---------------------------------------------------------------------------
// FindLockCycle -- detect a cycle through the given proc
// ---------------------------------------------------------------------------

/// PG `FindLockCycle`. Scans outward from `check_proc`; returns true if a cycle
/// through it exists, filling `soft_edges` with the cycle's soft edges and the
/// per-call deadlockDetails[] with the cycle's info.
fn find_lock_cycle(
    ctx: &mut DeadLockCtx,
    g: &ProcGlobal,
    view: &LockTablesView,
    check_proc: ProcNumber,
    soft_edges: &mut Vec<Edge>,
) -> bool {
    ctx.visited_procs.clear();
    ctx.n_deadlock_details = 0;
    soft_edges.clear();
    find_lock_cycle_recurse(ctx, g, view, check_proc, 0, soft_edges)
}

fn find_lock_cycle_recurse(
    ctx: &mut DeadLockCtx,
    g: &ProcGlobal,
    view: &LockTablesView,
    mut check_proc: ProcNumber,
    depth: usize,
    soft_edges: &mut Vec<Edge>,
) -> bool {
    // If a lock group member, check the leader instead (no-op for single-member).
    // SAFETY: all partition locks held.
    check_proc = unsafe { leader_of(g, check_proc) };

    // Have we already seen this proc?
    for i in 0..ctx.visited_procs.len() {
        if ctx.visited_procs[i] == check_proc {
            if i == 0 {
                // Returned to the start point -> a deadlock cycle. Outer levels
                // fill deadlockDetails[]; record the cycle length.
                debug_assert!(depth <= ctx.max_backends);
                ctx.n_deadlock_details = depth;
                return true;
            }
            // A cycle not including the start point -> "no deadlock".
            return false;
        }
    }
    // Mark proc as seen.
    debug_assert!(ctx.visited_procs.len() < ctx.max_backends);
    ctx.visited_procs.push(check_proc);

    // If the process is waiting, an outgoing edge to each proc that blocks it.
    // SAFETY: all partition locks held; the waiter's wait fields are stable.
    let (is_waiting, wait_lock) = unsafe {
        g.proc(check_proc)
            .map_or((false, None), |p| (p.wait_lock.is_some(), p.wait_lock))
    };
    if is_waiting
        && let Some(_lock) = wait_lock
            && find_lock_cycle_recurse_member(
                ctx, g, view, check_proc, check_proc, depth, soft_edges,
            ) {
                return true;
            }

    // Lock-group members may have outgoing edges even if this proc isn't waiting.
    // Only a leader has members; single-member groups -> just self -> no-op.
    // SAFETY: all partition locks held.
    let members: Vec<ProcNumber> = unsafe {
        match g.proc(check_proc).map(|p| &p.lock_group_role) {
            Some(LockGroupRole::Leader { members }) => members.clone(),
            _ => Vec::new(),
        }
    };
    for member in members {
        if member == check_proc {
            continue;
        }
        // SAFETY: all partition locks held.
        let member_waiting = unsafe {
            g.proc(member)
                .is_some_and(|p| p.wait_lock.is_some())
        };
        if member_waiting
            && find_lock_cycle_recurse_member(
                ctx, g, view, member, check_proc, depth, soft_edges,
            )
        {
            return true;
        }
    }

    false
}

#[allow(
    clippy::too_many_lines,
    reason = "1:1 port of C FindLockCycleRecurseMember; splitting would diverge from PG structure"
)]
fn find_lock_cycle_recurse_member(
    ctx: &mut DeadLockCtx,
    g: &ProcGlobal,
    view: &LockTablesView,
    check_proc: ProcNumber,
    check_proc_leader: ProcNumber,
    depth: usize,
    soft_edges: &mut Vec<Edge>,
) -> bool {
    // SAFETY: all partition locks held; the waiter's wait fields are stable.
    let (lock, wait_lock_mode) = unsafe {
        let Some((l, mode)) = g
            .proc(check_proc)
            .and_then(|p| p.wait_lock.map(|l| (l, p.wait_lock_mode)))
        else {
            return false;
        };
        (l, mode)
    };

    // The relation-extension lock can never be in an actual deadlock cycle.
    // SAFETY: lock alive under the partition locks.
    if unsafe { lock_locktag(lock) } == LockTagType::RelationExtend as u8 {
        return false;
    }

    // SAFETY: lock alive under the partition locks.
    let lock_method_table = unsafe { GetLocksMethodTable(&*lock) };
    let num_lock_modes = lock_method_table.num_lock_modes;
    let conflict_mask = lock_method_table.conflict_tab[wait_lock_mode as usize];
    // SAFETY: lock alive.
    let lock_tag = unsafe { (*lock).tag };
    // SAFETY: our own slot.
    let check_pid = unsafe { g.proc(check_proc).map_or(0, |p| p.pid) };

    // Scan for procs holding conflicting locks: these are HARD edges. The holder
    // list comes from the locked-tables view (15b keys PROCLOCKs by (tag, proc)).
    let holders: Vec<(ProcNumber, LOCKMASK)> = view.holders_of(&lock_tag);

    for (proc, hold_mask) in holders {
        // SAFETY: all partition locks held.
        let leader = unsafe { leader_of(g, proc) };
        // A proc never blocks itself or another member of its own group.
        if leader == check_proc_leader {
            continue;
        }
        for lm in 1..=num_lock_modes {
            if (hold_mask & lockbit_on(lm)) != 0 && (conflict_mask & lockbit_on(lm)) != 0 {
                // This proc HARD-blocks check_proc.
                if find_lock_cycle_recurse(ctx, g, view, proc, depth + 1, soft_edges) {
                    // Fill deadlockDetails[depth].
                    if depth < ctx.deadlock_details.len() {
                        ctx.deadlock_details[depth] = DeadlockInfo {
                            locktag: lock_tag,
                            lockmode: wait_lock_mode,
                            pid: check_pid,
                        };
                    }
                    return true;
                }
                // No deadlock here; if this is an autovac directly hard-blocking
                // OUR proc, remember it so the caller can cancel it.
                if check_proc == our_checking_proc(g)
                    && proc_is_autovacuum(g, proc)
                {
                    ctx.blocking_autovacuum_proc = proc;
                }
                break; // done looking at this proclock
            }
        }
    }

    // Scan for procs ahead in the wait queue with conflicting requests: SOFT
    // edges. Done after the hard-block search (so a proc that both hard- and
    // soft-blocks is called hard). Use a proposed re-ordering if one exists.
    let wait_order_idx = ctx
        .wait_orders
        .iter()
        .position(|w| w.lock == lock);

    if let Some(wo_idx) = wait_order_idx {
        // Use the hypothetical wait queue order.
        let procs = ctx.wait_orders[wo_idx].procs.clone();
        for proc in procs {
            // SAFETY: all partition locks held.
            let leader = unsafe { leader_of(g, proc) };
            // TopoSort keeps group members adjacent; once we reach our own group
            // we've seen all conflicts preceding any group member.
            if leader == check_proc_leader {
                break;
            }
            // SAFETY: another waiter's wait fields, stable under partition locks.
            let p_wait_mode = unsafe { g.proc(proc).map_or(0, |p| p.wait_lock_mode) };
            if (lockbit_on(p_wait_mode) & conflict_mask) != 0
                && find_lock_cycle_recurse(ctx, g, view, proc, depth + 1, soft_edges)
            {
                if depth < ctx.deadlock_details.len() {
                    ctx.deadlock_details[depth] = DeadlockInfo {
                        locktag: lock_tag,
                        lockmode: wait_lock_mode,
                        pid: check_pid,
                    };
                }
                soft_edges.push(Edge {
                    waiter: check_proc_leader,
                    blocker: leader,
                    lock,
                    pred: 0,
                    link: 0,
                });
                return true;
            }
        }
    } else {
        // Use the true lock wait queue order.
        // SAFETY: lock alive under the partition locks.
        let wait_queue: Vec<ProcNumber> = unsafe { (*lock).wait_procs.clone() };

        // Find the last member of our lock group present in the queue; anything
        // after it is not a soft conflict. Single-member: it's check_proc itself.
        // SAFETY: all partition locks held.
        let check_leader = unsafe { leader_of(g, check_proc) };
        let last_group_member = if check_leader == check_proc {
            // No group leader other than self.
            // (PG: lockGroupLeader == NULL -> lastGroupMember = checkProc.)
            check_proc
        } else {
            let mut last = check_proc;
            for &proc in &wait_queue {
                // SAFETY: all partition locks held.
                if unsafe { leader_of(g, proc) } == check_proc_leader {
                    last = proc;
                }
            }
            last
        };

        for proc in wait_queue {
            // SAFETY: all partition locks held.
            let leader = unsafe { leader_of(g, proc) };
            // Done when we reach the target proc.
            if proc == last_group_member {
                break;
            }
            // SAFETY: another waiter's wait fields, stable under partition locks.
            let p_wait_mode = unsafe { g.proc(proc).map_or(0, |p| p.wait_lock_mode) };
            if (lockbit_on(p_wait_mode) & conflict_mask) != 0
                && leader != check_proc_leader
                && find_lock_cycle_recurse(ctx, g, view, proc, depth + 1, soft_edges)
            {
                if depth < ctx.deadlock_details.len() {
                    ctx.deadlock_details[depth] = DeadlockInfo {
                        locktag: lock_tag,
                        lockmode: wait_lock_mode,
                        pid: check_pid,
                    };
                }
                soft_edges.push(Edge {
                    waiter: check_proc_leader,
                    blocker: leader,
                    lock,
                    pred: 0,
                    link: 0,
                });
                return true;
            }
        }
    }

    false
}

/// Whether `proc` is an autovacuum worker (PG reads statusFlags without locking;
/// PROC_IS_AUTOVACUUM is set at start and never reset).
fn proc_is_autovacuum(g: &ProcGlobal, proc: ProcNumber) -> bool {
    // SAFETY: reading the never-reset PROC_IS_AUTOVACUUM bit; benign without lock.
    unsafe {
        g.proc(proc)
            .is_some_and(|p| {
                p.status_flags
                    .contains(crate::storage::proc::ProcStatusFlags::PROC_IS_AUTOVACUUM)
            })
    }
}

/// The proc currently being checked (PG `MyProc`). DeadLockCheck is run for the
/// current backend; the autovac-cancel rule only fires for OUR proc's direct
/// blocker.
fn our_checking_proc(g: &ProcGlobal) -> ProcNumber {
    let _ = g;
    crate::storage::proc::current_proc_number()
}

// ---------------------------------------------------------------------------
// ExpandConstraints -- constraint set -> wait orderings
// ---------------------------------------------------------------------------

/// PG `ExpandConstraints`. Builds the new orderings for each affected wait queue
/// into ctx.wait_orders. Returns false if the constraints are contradictory.
fn expand_constraints(ctx: &mut DeadLockCtx, g: &ProcGlobal) -> bool {
    ctx.wait_orders.clear();

    // Scan the constraint list backwards (the last-added is the only one that can
    // fail, so test it for inconsistency first).
    let n_constraints = ctx.cur_constraints.len();
    let mut i = n_constraints;
    while i > 0 {
        i -= 1;
        let lock = ctx.cur_constraints[i].lock;

        // Already made a list for this lock?
        if ctx.wait_orders.iter().any(|w| w.lock == lock) {
            continue;
        }

        // SAFETY: lock alive under the partition locks.
        let queue_len = unsafe { (*lock).wait_procs.len() };
        debug_assert!(
            ctx.wait_orders.iter().map(|w| w.procs.len()).sum::<usize>() + queue_len
                <= ctx.max_backends
        );

        // Topo sort: only constraints up through i matter for this lock (later
        // ones are for different locks).
        let mut ordering = vec![INVALID_PROC_NUMBER; queue_len];
        if !topo_sort(ctx, g, lock, i + 1, &mut ordering) {
            return false;
        }
        ctx.wait_orders.push(WaitOrder { lock, procs: ordering });
    }
    true
}

// ---------------------------------------------------------------------------
// TopoSort -- topological sort of a wait queue under the constraints
// ---------------------------------------------------------------------------

/// PG `TopoSort`. Reorder `lock`'s wait queue to satisfy the partial order in
/// `constraints[0..n_constraints]` (each EDGE means waiter must come before
/// blocker), minimizing change. Output to `ordering`. Returns false on a
/// contradiction.
#[allow(
    clippy::too_many_lines,
    reason = "1:1 port of C TopoSort; splitting would diverge from PG structure"
)]
fn topo_sort(
    ctx: &mut DeadLockCtx,
    g: &ProcGlobal,
    lock: *mut LOCK,
    n_constraints: usize,
    ordering: &mut [ProcNumber],
) -> bool {
    // SAFETY: lock alive under the partition locks.
    let topo_procs_src: Vec<ProcNumber> = unsafe { (*lock).wait_procs.clone() };
    let queue_size = topo_procs_src.len();
    debug_assert_eq!(queue_size, ordering.len());

    // topoProcs[]: current order; None = already emitted.
    let mut topo_procs: Vec<Option<ProcNumber>> =
        topo_procs_src.iter().map(|&p| Some(p)).collect();

    // beforeConstraints[j]: # constraints saying topoProcs[j] must precede;
    //   -1 marks a non-representative group member.
    // afterConstraints[k]: 1-based list head of constraints after topoProcs[k].
    for v in &mut ctx.before_constraints[..queue_size] {
        *v = 0;
    }
    for v in &mut ctx.after_constraints[..queue_size] {
        *v = 0;
    }

    for i in 0..n_constraints {
        // Representative waiter on the queue + part of the waiting group.
        let waiter_leader = ctx.cur_constraints[i].waiter;
        let mut jj: i32 = -1;
        let mut j = queue_size;
        while j > 0 {
            j -= 1;
            let Some(waiter) = topo_procs[j] else {
                continue;
            };
            // SAFETY: all partition locks held.
            let is_member =
                waiter == waiter_leader || unsafe { leader_of(g, waiter) } == waiter_leader;
            if is_member {
                if jj == -1 {
                    jj = j as i32;
                } else {
                    debug_assert!(ctx.before_constraints[j] <= 0);
                    ctx.before_constraints[j] = -1;
                }
            }
        }
        if jj < 0 {
            continue; // not relevant to this lock
        }

        // Representative blocker on the queue + waiting for the blocking group.
        let blocker_leader = ctx.cur_constraints[i].blocker;
        let mut kk: i32 = -1;
        let mut k = queue_size;
        while k > 0 {
            k -= 1;
            let Some(blocker) = topo_procs[k] else {
                continue;
            };
            // SAFETY: all partition locks held.
            let is_member =
                blocker == blocker_leader || unsafe { leader_of(g, blocker) } == blocker_leader;
            if is_member {
                if kk == -1 {
                    kk = k as i32;
                } else {
                    debug_assert!(ctx.before_constraints[k] <= 0);
                    ctx.before_constraints[k] = -1;
                }
            }
        }
        if kk < 0 {
            continue;
        }

        debug_assert!(ctx.before_constraints[jj as usize] >= 0);
        ctx.before_constraints[jj as usize] += 1; // waiter must come before
        // Add to the blocker's after-constraints list.
        ctx.cur_constraints[i].pred = jj;
        ctx.cur_constraints[i].link = ctx.after_constraints[kk as usize];
        ctx.after_constraints[kk as usize] = i as i32 + 1;
    }

    // Scan topoProcs backwards: at each step output the last proc with no
    // remaining before-constraints, plus its group members, then decrement the
    // before-counts of the procs it was constrained against.
    let mut last: i32 = queue_size as i32 - 1;
    let mut i: i32 = queue_size as i32 - 1;
    while i >= 0 {
        // Find next candidate.
        while last >= 0 && topo_procs[last as usize].is_none() {
            last -= 1;
        }
        let mut j: i32 = last;
        while j >= 0 {
            if topo_procs[j as usize].is_some() && ctx.before_constraints[j as usize] == 0 {
                break;
            }
            j -= 1;
        }
        if j < 0 {
            return false; // topological sort fails -- contradictory
        }

        // Output everything in this proc's lock group (group members consecutive).
        let mut proc = topo_procs[j as usize].unwrap();
        // SAFETY: all partition locks held.
        proc = unsafe { leader_of(g, proc) };

        let mut n_matches = 0i32;
        for c in 0..=last {
            let Some(cur) = topo_procs[c as usize] else {
                continue;
            };
            // SAFETY: all partition locks held.
            let is_group = cur == proc || unsafe { leader_of(g, cur) } == proc;
            if is_group {
                ordering[(i - n_matches) as usize] = cur;
                topo_procs[c as usize] = None;
                n_matches += 1;
            }
        }
        debug_assert!(n_matches > 0);
        i -= n_matches;

        // Update before-counts of j's predecessors via the after-constraints list.
        let mut k = ctx.after_constraints[j as usize];
        while k > 0 {
            let pred = ctx.cur_constraints[(k - 1) as usize].pred;
            ctx.before_constraints[pred as usize] -= 1;
            k = ctx.cur_constraints[(k - 1) as usize].link;
        }
    }

    true
}

// ---------------------------------------------------------------------------
// DeadLockReport / RememberSimpleDeadLock / InitDeadLockChecking
// ---------------------------------------------------------------------------

/// PG `DeadLockReport`: raise the deadlock ERROR with the recorded cycle details.
/// `pg_noreturn` in C; here a panic carrying the error (rules s6.2).
// TODO(panic): migrate to Result + ?.
pub fn DeadLockReport() -> ! {
    let detail = {
        let p = PUBLISHED.lock();
        match p.as_ref() {
            Some(pd) if pd.n > 0 => describe_cycle(&pd.details[..pd.n]),
            _ => String::from("deadlock detected"),
        }
    };
    // PG ereport(ERROR, ERRCODE_T_R_DEADLOCK_DETECTED, "deadlock detected", detail).
    panic!("deadlock detected: {detail}");
}

/// Build the "Process N waits for ... blocked by process M" detail lines.
fn describe_cycle(details: &[DeadlockInfo]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for (i, info) in details.iter().enumerate() {
        let next_pid = if i < details.len() - 1 {
            details[i + 1].pid
        } else {
            details[0].pid
        };
        let mut tagbuf = String::new();
        crate::storage::lmgr::DescribeLockTag(&mut tagbuf, &info.locktag);
        let modename = crate::backend::storage::lmgr::lock::GetLockmodeName(
            u16::from(info.locktag.locktag_lockmethodid),
            info.lockmode,
        );
        if i > 0 {
            out.push('\n');
        }
        write!(
            out,
            "Process {} waits for {} on {}; blocked by process {}.",
            info.pid, modename, tagbuf, next_pid
        )
        .unwrap();
    }
    out
}

/// PG `RememberSimpleDeadLock`: set up the report info for a trivial two-way
/// deadlock detected by JoinWaitQueue (proc1 wants `lockmode` on `lock`, proc2 is
/// already waiting and would be blocked by proc1).
pub fn RememberSimpleDeadLock(
    proc1: ProcNumber,
    lockmode: LOCKMODE,
    lock: &LOCK,
    proc2: ProcNumber,
) {
    let Some(g) = proc_global() else {
        return;
    };
    // SAFETY: caller holds the partition lock for `lock`; reads pids + proc2's
    // wait fields, stable under it.
    let (proc1_pid, proc2_pid, proc2_wait_tag, proc2_wait_mode) = unsafe {
        let p1 = g.proc(proc1).map_or(0, |p| p.pid);
        let p2 = g.proc(proc2);
        let p2_pid = p2.map_or(0, |p| p.pid);
        let (tag, mode) = match p2.and_then(|p| p.wait_lock.map(|l| (l, p.wait_lock_mode))) {
            Some((l, m)) => ((*l).tag, m),
            None => (lock.tag, lockmode),
        };
        (p1, p2_pid, tag, mode)
    };
    let details = vec![
        DeadlockInfo {
            locktag: lock.tag,
            lockmode,
            pid: proc1_pid,
        },
        DeadlockInfo {
            locktag: proc2_wait_tag,
            lockmode: proc2_wait_mode,
            pid: proc2_pid,
        },
    ];
    let mut p = PUBLISHED.lock();
    *p = Some(PublishedDetails {
        details,
        n: 2,
        blocking_autovacuum_proc: INVALID_PROC_NUMBER,
    });
}

/// PG `InitDeadLockChecking`: per-backend allocation of the detector workspace.
/// Under the Arc model the workspace is allocated per `DeadLockCheck` call (sized
/// from MaxBackends, bounded), so there is nothing to pre-allocate here.
pub fn InitDeadLockChecking() {}

#[cfg(test)]
mod tests;
