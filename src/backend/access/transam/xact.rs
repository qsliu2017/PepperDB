//! Translated from PostgreSQL src/backend/access/transam/xact.c
//!
//! The top-level transaction state machine: the `TransactionState` stack, the
//! low-level (`TransState`) and block-level (`TBlockState`) state transitions,
//! commit/abort durability, and savepoints. This ties together the xid
//! generator (varsup), clog/subtrans, procarray, snapshot manager, combocid,
//! the resource owner, and WAL.
//!
//! Per-task model (rules s6.1 / s7): xact.c's process globals -- the
//! `CurrentTransactionState` stack, `XactTopFullTransactionId`,
//! `currentCommandId`, the timestamps, `XactIsoLevel`/`XactReadOnly`, etc. --
//! become one per-task [`XactState`] published as a `task_local!`
//! `RefCell<XactState>`. The `RefCell` borrow is never held across an `.await`
//! (we borrow, copy/decide, drop the borrow, then await). The state is `Send`
//! (owned data only), so a backend future can migrate threads.
//!
//! Async coloring (rules s5): `StartTransaction`/`CommitTransaction`/
//! `AbortTransaction` and their command-level drivers are `async` because they
//! await WAL flush, clog writes and snapshot acquisition. The block-state
//! mutators (`BeginTransactionBlock` etc.) and the read-only accessors stay
//! sync. `GetCurrentTransactionId` is `async` because it may assign an xid
//! (`GetNewTransactionId`, async); the `*IfAny` variants stay sync.
//!
//! Staging: invalidation messages, two-phase prepare, multixact, pgstat,
//! replication origins, large objects, portals, triggers and GUC nest levels
//! are deferred subsystems reached through their existing stubs (`TODO`s mark
//! each). `xact_redo` is recovery and stays a stub (`TODO(recovery)`).

use std::cell::RefCell;
use std::sync::Arc;

use crate::access::transam::{
    FullTransactionId, INVALID_FULL_TRANSACTION_ID, INVALID_TRANSACTION_ID,
};
use crate::access::xact::{
    MinSizeOfXactAbort, MinSizeOfXactRelfileLocators, MinSizeOfXactSubxacts, SYNCHRONOUS_COMMIT_ON,
    SavedTransactionCharacteristics, SubXactCallback, SubXactEvent,
    XACT_COMPLETION_FORCE_SYNC_COMMIT, XACT_COMPLETION_UPDATE_RELCACHE_FILE,
    XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK, XACT_READ_COMMITTED, XACT_XINFO_HAS_AE_LOCKS,
    XACT_XINFO_HAS_RELFILELOCATORS, XACT_XINFO_HAS_SUBXACTS, XLOG_XACT_ABORT, XLOG_XACT_COMMIT,
    XLOG_XACT_HAS_INFO, XactCallback, XactEvent, XactFlags, xl_xact_abort, xl_xact_commit,
    xl_xact_dbinfo, xl_xact_origin, xl_xact_relfilelocators, xl_xact_stats_item, xl_xact_subxacts,
    xl_xact_xinfo,
};
use crate::access::xlogdefs::{INVALID_XLOG_REC_PTR, XLogRecPtr};
use crate::access::xlogrecord::XLR_SPECIAL_REL_UPDATE;
use crate::c::{
    CommandId, FirstCommandId, InvalidCommandId, InvalidSubTransactionId, SubTransactionId,
    TopSubTransactionId, TransactionId,
};
use crate::datatype::timestamp::TimestampTz;
use crate::shared_state::SharedState;
use crate::storage::relfilelocator::RelFileLocator;

use crate::access::transam::{
    full_transaction_id_is_valid as FullTransactionIdIsValid,
    transaction_id_equals as TransactionIdEquals,
    xid_from_full_transaction_id as XidFromFullTransactionId,
};
use crate::backend::access::transam::transam::{
    transaction_id_abort_tree, transaction_id_async_commit_tree, transaction_id_commit_tree,
    transaction_id_did_commit, transaction_id_latest as TransactionIdLatest,
};

// ---------------------------------------------------------------------------
// synchronous_commit levels (xact.h SyncCommitLevel ordinals)
// ---------------------------------------------------------------------------
const SYNCHRONOUS_COMMIT_OFF: i32 = 0;
const SYNCHRONOUS_COMMIT_DEFAULT: i32 = SYNCHRONOUS_COMMIT_ON as i32;

// ---------------------------------------------------------------------------
// TransState / TBlockState
// ---------------------------------------------------------------------------

/// xact.c `TransState`: low-level transaction state, server perspective.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransState {
    Default,    // idle
    Start,      // transaction starting
    InProgress, // inside a valid transaction
    Commit,     // commit in progress
    Abort,      // abort in progress
    Prepare,    // prepare in progress
}

/// xact.c `TBlockState`: transaction-block state of client queries. The `Sub*`
/// states are used only for non-topmost transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TBlockState {
    // not-in-transaction-block states
    Default,
    Started,
    // transaction block states
    Begin,
    InProgress,
    ImplicitInProgress,
    ParallelInProgress,
    End,
    Abort,
    AbortEnd,
    AbortPending,
    Prepare,
    // subtransaction states
    SubBegin,
    SubInProgress,
    SubRelease,
    SubCommit,
    SubAbort,
    SubAbortEnd,
    SubAbortPending,
    SubRestart,
    SubAbortRestart,
}

fn trans_state_as_string(s: TransState) -> &'static str {
    match s {
        TransState::Default => "DEFAULT",
        TransState::Start => "START",
        TransState::InProgress => "INPROGRESS",
        TransState::Commit => "COMMIT",
        TransState::Abort => "ABORT",
        TransState::Prepare => "PREPARE",
    }
}

fn block_state_as_string(s: TBlockState) -> &'static str {
    use TBlockState::{Default, Started, Begin, InProgress, ImplicitInProgress, ParallelInProgress, End, Abort, AbortEnd, AbortPending, Prepare, SubBegin, SubInProgress, SubRelease, SubCommit, SubAbort, SubAbortEnd, SubAbortPending, SubRestart, SubAbortRestart};
    match s {
        Default => "DEFAULT",
        Started => "STARTED",
        Begin => "BEGIN",
        InProgress => "INPROGRESS",
        ImplicitInProgress => "IMPLICIT_INPROGRESS",
        ParallelInProgress => "PARALLEL_INPROGRESS",
        End => "END",
        Abort => "ABORT",
        AbortEnd => "ABORT_END",
        AbortPending => "ABORT_PENDING",
        Prepare => "PREPARE",
        SubBegin => "SUBBEGIN",
        SubInProgress => "SUBINPROGRESS",
        SubRelease => "SUBRELEASE",
        SubCommit => "SUBCOMMIT",
        SubAbort => "SUBABORT",
        SubAbortEnd => "SUBABORT_END",
        SubAbortPending => "SUBABORT_PENDING",
        SubRestart => "SUBRESTART",
        SubAbortRestart => "SUBABORT_RESTART",
    }
}

// ---------------------------------------------------------------------------
// TransactionStateData (one stack frame)
// ---------------------------------------------------------------------------

/// xact.c `TransactionStateData`. The C `parent` back-pointer is modeled by the
/// frame's position in [`XactState::stack`] (parent = the frame below). The
/// transaction-lifetime memory context is dropped (MemoryContext is tombstoned,
/// rules s8); per-frame resources live on the [`ResourceOwner`] tree.
#[derive(Clone)]
pub struct TransactionStateData {
    pub full_transaction_id: FullTransactionId,
    pub subtransaction_id: SubTransactionId,
    pub name: Option<String>,
    pub savepoint_level: i32,
    pub state: TransState,
    pub block_state: TBlockState,
    pub nesting_level: i32,
    pub guc_nest_level: i32,
    pub child_xids: Vec<TransactionId>,
    pub prev_user: crate::postgres_ext::Oid,
    pub prev_sec_context: i32,
    pub prev_xact_read_only: bool,
    pub started_in_recovery: bool,
    pub did_log_xid: bool,
    pub parallel_mode_level: i32,
    pub parallel_child_xact: bool,
    pub chain: bool,
    pub top_xid_logged: bool,
}

impl TransactionStateData {
    /// The static `TopTransactionStateData` initializer (xact.c): idle top frame.
    fn top() -> Self {
        Self {
            full_transaction_id: INVALID_FULL_TRANSACTION_ID,
            subtransaction_id: InvalidSubTransactionId,
            name: None,
            savepoint_level: 0,
            state: TransState::Default,
            block_state: TBlockState::Default,
            nesting_level: 0,
            guc_nest_level: 0,
            child_xids: Vec::new(),
            prev_user: crate::postgres_ext::Oid(0),
            prev_sec_context: 0,
            prev_xact_read_only: false,
            started_in_recovery: false,
            did_log_xid: false,
            parallel_mode_level: 0,
            parallel_child_xact: false,
            chain: false,
            top_xid_logged: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Per-task xact state (the xact.c process globals)
// ---------------------------------------------------------------------------

/// All of xact.c's per-backend mutable state, made per-task. The stack's last
/// element is `CurrentTransactionState`; the first is `TopTransactionStateData`.
pub struct XactState {
    /// `CurrentTransactionState` stack. Always non-empty (index 0 = top frame).
    stack: Vec<TransactionStateData>,
    /// `XactTopFullTransactionId`.
    xact_top_full_xid: FullTransactionId,
    /// `currentSubTransactionId` / `currentCommandId` / `currentCommandIdUsed`.
    current_sub_transaction_id: SubTransactionId,
    current_command_id: CommandId,
    current_command_id_used: bool,
    /// transaction/statement/stop timestamps.
    xact_start_timestamp: TimestampTz,
    stmt_start_timestamp: TimestampTz,
    xact_stop_timestamp: TimestampTz,
    /// `XactIsoLevel` / `XactReadOnly` / `XactDeferrable` (the GUC-derived
    /// per-xact characteristics; defaults come from the Default* GUCs).
    xact_iso_level: i32,
    xact_read_only: bool,
    xact_deferrable: bool,
    /// `MyXactFlags`.
    my_xact_flags: i32,
    /// `forceSyncCommit`.
    force_sync_commit: bool,
    /// `nUnreportedXids` (the unreportedXids array contents are only needed for
    /// hot standby, which is out of foundation; we keep just the count).
    n_unreported_xids: i32,
    /// Per-backend WAL bookkeeping (PG's `XactLastRecEnd` / `XactLastCommitEnd`,
    /// normally maintained by `XLogInsertRecord`). We set `xact_last_rec_end`
    /// from each xact WAL record's end LSN.
    xact_last_rec_end: XLogRecPtr,
    xact_last_commit_end: XLogRecPtr,
    /// add-on xact / subxact callbacks.
    xact_callbacks: Vec<XactCallback>,
    subxact_callbacks: Vec<SubXactCallback>,
}

impl XactState {
    fn new() -> Self {
        Self {
            stack: vec![TransactionStateData::top()],
            xact_top_full_xid: INVALID_FULL_TRANSACTION_ID,
            current_sub_transaction_id: InvalidSubTransactionId,
            current_command_id: FirstCommandId,
            current_command_id_used: false,
            xact_start_timestamp: 0,
            stmt_start_timestamp: 0,
            xact_stop_timestamp: 0,
            xact_iso_level: XACT_READ_COMMITTED,
            xact_read_only: false,
            xact_deferrable: false,
            my_xact_flags: 0,
            force_sync_commit: false,
            n_unreported_xids: 0,
            xact_last_rec_end: INVALID_XLOG_REC_PTR,
            xact_last_commit_end: INVALID_XLOG_REC_PTR,
            xact_callbacks: Vec::new(),
            subxact_callbacks: Vec::new(),
        }
    }

    #[inline]
    fn cur(&self) -> &TransactionStateData {
        self.stack.last().unwrap()
    }
    #[inline]
    fn cur_mut(&mut self) -> &mut TransactionStateData {
        self.stack.last_mut().unwrap()
    }
    #[inline]
    fn top(&self) -> &TransactionStateData {
        &self.stack[0]
    }
    #[inline]
    fn top_mut(&mut self) -> &mut TransactionStateData {
        &mut self.stack[0]
    }
    #[inline]
    fn is_sub(&self) -> bool {
        self.stack.len() >= 2
    }
}

tokio::task_local! {
    /// The current task's transaction state. Established by [`xact_scope`].
    static XACT: RefCell<XactState>;
}

/// Run `f` with a fresh per-task transaction state in scope. A backend task
/// wraps its body in this once (mirrors PG having one process-wide state). The
/// snapshot-manager and combo-cid scopes are normally nested inside this.
pub async fn xact_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    XACT.scope(RefCell::new(XactState::new()), f).await
}

/// True iff a per-task xact state is in scope (some unit tests run outside one).
fn in_scope() -> bool {
    XACT.try_with(|_| ()).is_ok()
}

/// Borrow the per-task xact state synchronously. NEVER hold the returned borrow
/// across an `.await` (rules s5).
fn with_xact<R>(f: impl FnOnce(&mut XactState) -> R) -> R {
    XACT.with(|cell| f(&mut cell.borrow_mut()))
}

/// Like [`with_xact`] but returns `default` when no xact scope is active. Used by
/// the read-only accessors that other subsystems (snapmgr, combocid) may call
/// from contexts without a full transaction scope (matches the 14c placeholder
/// behavior those callers relied on).
fn with_xact_or<R>(default: R, f: impl FnOnce(&XactState) -> R) -> R {
    XACT.try_with(|cell| f(&cell.borrow())).unwrap_or(default)
}

// ===========================================================================
// transaction state accessors
// ===========================================================================

// Per-task characteristic accessors (xact.h's former process globals). These
// back the header's `XactIsoLevel`/`XactReadOnly`/`XactDeferrable`/`MyXactFlags`
// re-exports. They are sync reads/writes of per-task state.
pub fn xact_iso_level() -> i32 {
    with_xact_or(XACT_READ_COMMITTED, |x| x.xact_iso_level)
}
pub fn set_xact_iso_level(v: i32) {
    with_xact(|x| x.xact_iso_level = v);
}
pub fn xact_read_only() -> bool {
    with_xact_or(false, |x| x.xact_read_only)
}
pub fn set_xact_read_only(v: bool) {
    with_xact(|x| x.xact_read_only = v);
}
pub fn xact_deferrable() -> bool {
    with_xact(|x| x.xact_deferrable)
}
pub fn set_xact_deferrable(v: bool) {
    with_xact(|x| x.xact_deferrable = v);
}
pub fn my_xact_flags() -> i32 {
    with_xact(|x| x.my_xact_flags)
}
pub fn set_my_xact_flags(v: i32) {
    with_xact(|x| x.my_xact_flags = v);
}

/// xact.c `IsTransactionState`: true iff inside a valid (TRANS_INPROGRESS) xact.
pub fn IsTransactionState() -> bool {
    with_xact(|x| x.cur().state == TransState::InProgress)
}

/// Like [`IsTransactionState`] but returns `false` when no xact scope is active,
/// for assert-only callers (e.g. inval's AcceptInvalidationMessages) that may run
/// outside a transaction scope.
pub fn is_transaction_state_or_false() -> bool {
    with_xact_or(false, |x| x.cur().state == TransState::InProgress)
}

/// xact.c `IsAbortedTransactionBlockState`.
pub fn IsAbortedTransactionBlockState() -> bool {
    with_xact(|x| {
        matches!(
            x.cur().block_state,
            TBlockState::Abort | TBlockState::SubAbort
        )
    })
}

/// xact.c `GetTopTransactionId`: assigns an xid to the top frame if needed.
pub async fn GetTopTransactionId(shared: &Arc<SharedState>) -> TransactionId {
    let have = with_xact(|x| FullTransactionIdIsValid(x.xact_top_full_xid));
    if !have {
        assign_transaction_id(shared, 0).await;
    }
    with_xact(|x| XidFromFullTransactionId(x.xact_top_full_xid))
}

/// xact.c `GetTopTransactionIdIfAny`.
pub fn GetTopTransactionIdIfAny() -> Option<TransactionId> {
    with_xact_or(None, |x| {
        let xid = XidFromFullTransactionId(x.xact_top_full_xid);
        xid.is_valid().then_some(xid)
    })
}

/// xact.c `GetCurrentTransactionId`: assigns an xid to the current frame if
/// needed (hence async -- assignment generates a new xid).
pub async fn GetCurrentTransactionId(shared: &Arc<SharedState>) -> TransactionId {
    let (have, idx) = with_xact(|x| {
        (
            FullTransactionIdIsValid(x.cur().full_transaction_id),
            x.stack.len() - 1,
        )
    });
    if !have {
        assign_transaction_id(shared, idx).await;
    }
    with_xact(|x| XidFromFullTransactionId(x.cur().full_transaction_id))
}

/// xact.c `GetCurrentTransactionIdIfAny`.
pub fn GetCurrentTransactionIdIfAny() -> Option<TransactionId> {
    with_xact(|x| {
        let xid = XidFromFullTransactionId(x.cur().full_transaction_id);
        xid.is_valid().then_some(xid)
    })
}

/// xact.c `GetTopFullTransactionId`.
pub async fn GetTopFullTransactionId(shared: &Arc<SharedState>) -> FullTransactionId {
    let have = with_xact(|x| FullTransactionIdIsValid(x.xact_top_full_xid));
    if !have {
        assign_transaction_id(shared, 0).await;
    }
    with_xact(|x| x.xact_top_full_xid)
}

/// xact.c `GetTopFullTransactionIdIfAny`.
pub fn GetTopFullTransactionIdIfAny() -> FullTransactionId {
    with_xact(|x| x.xact_top_full_xid)
}

/// xact.c `GetCurrentFullTransactionId`.
pub async fn GetCurrentFullTransactionId(shared: &Arc<SharedState>) -> FullTransactionId {
    let (have, idx) = with_xact(|x| {
        (
            FullTransactionIdIsValid(x.cur().full_transaction_id),
            x.stack.len() - 1,
        )
    });
    if !have {
        assign_transaction_id(shared, idx).await;
    }
    with_xact(|x| x.cur().full_transaction_id)
}

/// xact.c `GetCurrentFullTransactionIdIfAny`.
pub fn GetCurrentFullTransactionIdIfAny() -> FullTransactionId {
    with_xact(|x| x.cur().full_transaction_id)
}

/// xact.c `MarkCurrentTransactionIdLoggedIfAny`.
pub fn MarkCurrentTransactionIdLoggedIfAny() {
    with_xact(|x| {
        if FullTransactionIdIsValid(x.cur().full_transaction_id) {
            x.cur_mut().did_log_xid = true;
        }
    });
}

/// xact.c `IsSubxactTopXidLogPending`. wal_level>=logical is never active in the
/// foundation, so this is always false.
pub fn IsSubxactTopXidLogPending() -> bool {
    false
}

/// xact.c `MarkSubxactTopXidLogged`.
pub fn MarkSubxactTopXidLogged() {
    with_xact(|x| x.cur_mut().top_xid_logged = true);
}

/// xact.c `GetStableLatestTransactionId`. Without a per-backend lxid cache yet,
/// return the top xid if assigned, else the next-to-be-assigned xid.
pub fn GetStableLatestTransactionId(shared: &Arc<SharedState>) -> TransactionId {
    GetTopTransactionIdIfAny().unwrap_or_else(|| {
        XidFromFullTransactionId(shared.variable_cache().read_next_full_transaction_id())
    })
}

/// xact.c `AssignTransactionId`: assign a permanent FullTransactionId to the
/// stack frame at `idx`, recursively assigning to unassigned parents first so a
/// child's xid always follows its parent's.
async fn assign_transaction_id(shared: &Arc<SharedState>, idx: usize) {
    // Assert caller didn't screw up.
    debug_assert!(with_xact(|x| !FullTransactionIdIsValid(
        x.stack[idx].full_transaction_id
    ) && x.stack[idx].state
        == TransState::InProgress));

    let is_sub_xact = idx != 0;

    if IsInParallelMode() || crate::access::parallel::IsParallelWorker() {
        // TODO(panic): migrate to Result + ?
        crate::elog!(
            crate::utils::elog::ERROR,
            "cannot assign transaction IDs during a parallel operation".to_string()
        );
    }

    // Ensure parent(s) have xids first (iterative, deepest-unassigned-first).
    if is_sub_xact {
        let parents: Vec<usize> = with_xact(|x| {
            let mut v = Vec::new();
            let mut p = idx;
            while p > 0 && !FullTransactionIdIsValid(x.stack[p - 1].full_transaction_id) {
                v.push(p - 1);
                p -= 1;
            }
            v
        });
        // Assign shallowest-needed first so each child follows its parent.
        for p in parents.into_iter().rev() {
            Box::pin(assign_transaction_id(shared, p)).await;
        }
    }

    // Generate the new FullTransactionId (extends clog/subtrans as needed).
    let full = shared
        .variable_cache()
        .get_new_transaction_id(shared.clog(), shared.subtrans(), is_sub_xact)
        .await;
    let (xid, parent_xid) = with_xact(|x| {
        x.stack[idx].full_transaction_id = full;
        if !is_sub_xact {
            x.xact_top_full_xid = full;
        }
        let parent_xid = if is_sub_xact {
            XidFromFullTransactionId(x.stack[idx - 1].full_transaction_id)
        } else {
            INVALID_TRANSACTION_ID
        };
        (XidFromFullTransactionId(full), parent_xid)
    });

    // Record the subxact->parent link BEFORE the xid is visible elsewhere.
    if is_sub_xact {
        shared
            .subtrans()
            .sub_trans_set_parent(xid, parent_xid)
            .await;
    }

    // RegisterPredicateLockingXid (predicate.c) is a stub. TODO(predicate-lock).
    // XactLockTableInsert (lmgr) is a stub. TODO(lock-manager).
    // XLOG_XACT_ASSIGNMENT (hot standby) is not emitted: wal_level<logical and
    // standby is out of foundation. TODO(hot-standby).
    let _ = xid;
}

/// xact.c `GetCurrentSubTransactionId`.
pub fn GetCurrentSubTransactionId() -> SubTransactionId {
    with_xact(|x| x.cur().subtransaction_id)
}

/// xact.c `SubTransactionIsActive`.
pub fn SubTransactionIsActive(subxid: SubTransactionId) -> bool {
    with_xact(|x| {
        x.stack
            .iter()
            .rev()
            .any(|s| s.state != TransState::Abort && s.subtransaction_id == subxid)
    })
}

/// xact.c `GetCurrentCommandId`.
pub fn GetCurrentCommandId(used: bool) -> CommandId {
    if !used && !in_scope() {
        // Read-only fetch outside a transaction scope (e.g. snapmgr building a
        // snapshot in a thin test context): the foundation default command id.
        return FirstCommandId;
    }
    with_xact(|x| {
        if used {
            // Forbid in a parallel worker; we can't communicate this back.
            // TODO(panic): migrate to Result + ?
            assert!(!crate::access::parallel::IsParallelWorker(), "cannot modify data in a parallel worker");
            x.current_command_id_used = true;
        }
        x.current_command_id
    })
}

/// xact.c `SetParallelStartTimestamps`.
pub fn SetParallelStartTimestamps(xact_ts: TimestampTz, stmt_ts: TimestampTz) {
    with_xact(|x| {
        x.xact_start_timestamp = xact_ts;
        x.stmt_start_timestamp = stmt_ts;
    });
}

/// xact.c `GetCurrentTransactionStartTimestamp`.
pub fn GetCurrentTransactionStartTimestamp() -> TimestampTz {
    with_xact(|x| x.xact_start_timestamp)
}

/// xact.c `GetCurrentStatementStartTimestamp`.
pub fn GetCurrentStatementStartTimestamp() -> TimestampTz {
    with_xact(|x| x.stmt_start_timestamp)
}

/// xact.c `GetCurrentTransactionStopTimestamp`: sets it on first call after
/// commit/abort processing if a WAL record was skipped.
pub fn GetCurrentTransactionStopTimestamp() -> TimestampTz {
    let now = crate::utils::timestamp::GetCurrentTimestamp();
    with_xact(|x| {
        if x.xact_stop_timestamp == 0 {
            x.xact_stop_timestamp = now;
        }
        x.xact_stop_timestamp
    })
}

/// xact.c `SetCurrentStatementStartTimestamp`.
pub fn SetCurrentStatementStartTimestamp() {
    if !crate::access::parallel::IsParallelWorker() {
        let now = crate::utils::timestamp::GetCurrentTimestamp();
        with_xact(|x| x.stmt_start_timestamp = now);
    }
}

/// xact.c `GetCurrentTransactionNestLevel`: 0 outside any xact, 1 at top level.
pub fn GetCurrentTransactionNestLevel() -> i32 {
    // Default 1 (top level) outside a scope: matches the 14c placeholder so
    // snapmgr callers in thin contexts behave as before.
    with_xact_or(1, |x| x.cur().nesting_level)
}

/// xact.c `TransactionIdIsCurrentTransactionId`. Walks the state stack; does not
/// assign xids, so it stays sync.
pub fn TransactionIdIsCurrentTransactionId(xid: TransactionId) -> bool {
    if !xid.is_normal() {
        return false;
    }
    if let Some(top) = GetTopTransactionIdIfAny()
        && TransactionIdEquals(xid, top) {
            return true;
        }
    // ParallelCurrentXids (parallel worker) is out of foundation.
    with_xact_or(false, |x| {
        for s in x.stack.iter().rev() {
            if s.state == TransState::Abort {
                continue;
            }
            if !FullTransactionIdIsValid(s.full_transaction_id) {
                continue;
            }
            if TransactionIdEquals(xid, XidFromFullTransactionId(s.full_transaction_id)) {
                return true;
            }
            // childXids is kept in ascending order -> binary search.
            if s.child_xids.binary_search_by(|p| p.0.cmp(&xid.0)).is_ok() {
                return true;
            }
        }
        false
    })
}

/// xact.c `TransactionStartedDuringRecovery`.
pub fn TransactionStartedDuringRecovery() -> bool {
    with_xact(|x| x.cur().started_in_recovery)
}

/// xact.c `EnterParallelMode`.
pub fn EnterParallelMode() {
    with_xact(|x| x.cur_mut().parallel_mode_level += 1);
}

/// xact.c `ExitParallelMode`.
pub fn ExitParallelMode() {
    with_xact(|x| {
        debug_assert!(x.cur().parallel_mode_level > 0);
        x.cur_mut().parallel_mode_level -= 1;
    });
}

/// xact.c `IsInParallelMode`.
pub fn IsInParallelMode() -> bool {
    with_xact_or(false, |x| {
        x.cur().parallel_mode_level != 0 || x.cur().parallel_child_xact
    })
}

/// xact.c `CommandCounterIncrement`. Stays sync: it bumps the command id and
/// processes local invalidation (the invalidation send is sync; that subsystem
/// is a stub here).
pub fn CommandCounterIncrement() {
    let bumped = with_xact(|x| {
        if !x.current_command_id_used {
            return None;
        }
        if x.cur().parallel_mode_level != 0
            || x.cur().parallel_child_xact
            || crate::access::parallel::IsParallelWorker()
        {
            // TODO(panic): migrate to Result + ?
            panic!("cannot start commands during a parallel operation");
        }
        x.current_command_id = CommandId(x.current_command_id.0 + 1);
        if x.current_command_id == InvalidCommandId {
            x.current_command_id = CommandId(x.current_command_id.0 - 1);
            // TODO(panic): migrate to Result + ?
            panic!("cannot have more than 2^32-2 commands in a transaction");
        }
        x.current_command_id_used = false;
        Some(x.current_command_id)
    });
    if let Some(cid) = bumped {
        // Propagate into static snapshots, then make catalog changes visible.
        crate::backend::utils::time::snapmgr::SnapshotSetCommandId(cid);
        at_cci_local_cache();
    }
}

/// xact.c `ForceSyncCommit`.
pub fn ForceSyncCommit() {
    with_xact(|x| x.force_sync_commit = true);
}

// ===========================================================================
// StartTransaction
// ===========================================================================

/// xact.c `AtStart_Cache`: AcceptInvalidationMessages (inval is a stub).
fn at_start_cache() {
    // TODO(inval): AcceptInvalidationMessages.
}

/// xact.c `StartTransaction`.
fn start_transaction(shared: &Arc<SharedState>) {
    // Reset to the (single) top frame; assert the stack was empty.
    with_xact(|x| {
        debug_assert!(!FullTransactionIdIsValid(x.xact_top_full_xid));
        debug_assert_eq!(x.stack.len(), 1);
        debug_assert_eq!(x.cur().state, TransState::Default);

        let started_in_recovery = crate::access::xlog::recovery_in_progress();
        // xact_read_only / iso level come from the Default* GUCs (defaults).
        x.xact_read_only = started_in_recovery; // DefaultXactReadOnly=false
        x.xact_deferrable = false; // DefaultXactDeferrable
        x.xact_iso_level = XACT_READ_COMMITTED; // DefaultXactIsoLevel
        x.force_sync_commit = false;
        x.my_xact_flags = 0;
        x.current_sub_transaction_id = TopSubTransactionId;
        x.current_command_id = FirstCommandId;
        x.current_command_id_used = false;
        x.n_unreported_xids = 0;
        x.xact_stop_timestamp = 0;

        let s = x.top_mut();
        s.state = TransState::Start;
        s.full_transaction_id = INVALID_FULL_TRANSACTION_ID;
        s.nesting_level = 1;
        s.guc_nest_level = 1;
        s.child_xids.clear();
        s.subtransaction_id = TopSubTransactionId;
        s.started_in_recovery = started_in_recovery;
        s.did_log_xid = false;
        s.parallel_mode_level = 0;
        s.parallel_child_xact = false;
        // GetUserIdAndSecContext: identity lives on Session.
        if let Some(sess) = crate::session::try_current() {
            s.prev_user = sess.current_user_id();
            s.prev_sec_context = sess.sec_context();
        }
    });

    // must initialize resource-management first (AtStart_ResourceOwner): the
    // task already runs inside a transaction ResourceOwner scope; nothing to do.

    // transaction_timestamp() = first statement_timestamp() (no fresh clock).
    with_xact(|x| {
        if x.xact_start_timestamp == 0 {
            x.xact_start_timestamp = x.stmt_start_timestamp;
        }
    });

    // AtStart_GUC / AtStart_Cache / AfterTriggerBeginXact: stubs.
    at_start_cache();

    with_xact(|x| x.top_mut().state = TransState::InProgress);
    let _ = shared;
}

// ===========================================================================
// RecordTransactionCommit -- THE durability ordering
// ===========================================================================

/// xact.c `AtCCI_LocalCache`: make catalog changes visible for the next command
/// (relation-map + command-end invalidation). Both subsystems are stubs.
fn at_cci_local_cache() {
    // TODO(inval): AtCCI_RelationMap + CommandEndInvalidationMessages.
}

/// xact.c `RecordTransactionCommit`. Returns the latest xid among the xact and
/// its children, or InvalidTransactionId if it has no xid.
///
/// THE durability ordering (load-bearing): assemble + insert the commit WAL
/// record, get its end LSN, and -- in the SYNC-commit case -- flush the WAL to
/// disk BEFORE writing COMMITTED into clog. The clog must NEVER report a commit
/// before the WAL that records it is durable, or a crash between the two would
/// lose a "committed" transaction. The ASYNC-commit case records the commit LSN
/// in clog and requests (but does not wait for) a flush.
async fn record_transaction_commit(shared: &Arc<SharedState>) -> TransactionId {
    let xid = GetTopTransactionIdIfAny();
    let mark_xid_committed = xid.is_some();
    let mut latest_xid = INVALID_TRANSACTION_ID;

    // smgrGetPendingDeletes: pending file drops live in storage (step F1). We
    // have no committed-drop list wired here yet -> none. TODO(pending-deletes).
    let nrels = 0usize;
    let children = xactGetCommittedChildren();
    // pgstat dropped stats / standby inval messages: deferred subsystems.
    let wrote_xlog = with_xact(|x| !x.xact_last_rec_end.is_invalid());

    let Some(xid) = xid else {
        // No xid: can neither nor want to write a COMMIT record.
        // TODO(panic): migrate to Result + ?
        assert!(nrels == 0, "cannot commit a transaction that deleted files but has no xid");
        debug_assert!(children.is_empty());
        // If we wrote WAL (e.g. HOT pruning), trigger a flush like a commit
        // would; otherwise we are done.
        if !wrote_xlog {
            return latest_xid;
        }
        // Fall through to the flush/async decision below with mark_xid=false.
        return record_commit_finish(
            shared,
            INVALID_TRANSACTION_ID,
            &[],
            nrels,
            false,
            wrote_xlog,
        )
        .await;
    };

    // Mark our commit critical section: forces a concurrent checkpoint to wait
    // until we've updated pg_xact. (delayChkptFlags on MyProc -- step 15.)
    crate::session::current().inc_crit_section_count();

    // Insert the commit XLOG record.
    let commit_time = GetCurrentTransactionStopTimestamp();
    let my_xact_flags = with_xact(|x| x.my_xact_flags);
    // Box the WAL-assembly future to keep this commit path's future small.
    let recptr = Box::pin(XactLogCommitRecord(
        shared,
        commit_time,
        &children,
        &[],
        &[],
        &[],
        false,
        my_xact_flags,
        INVALID_TRANSACTION_ID,
        "",
    ))
    .await;
    with_xact(|x| x.xact_last_rec_end = recptr);

    // Commit-timestamp recording (commit_ts) is a deferred subsystem.

    let r = record_commit_finish(shared, xid, &children, nrels, true, wrote_xlog).await;

    // Leave the commit critical section.
    crate::session::current().dec_crit_section_count();

    latest_xid = r;
    latest_xid
}

/// The flush/async-commit tail shared by the xid and no-xid paths of
/// `record_transaction_commit`. `mark_xid_committed` says whether we wrote a
/// COMMIT record for `xid`.
async fn record_commit_finish(
    shared: &Arc<SharedState>,
    xid: TransactionId,
    children: &[TransactionId],
    nrels: usize,
    mark_xid_committed: bool,
    wrote_xlog: bool,
) -> TransactionId {
    let force_sync = with_xact(|x| x.force_sync_commit);
    let recptr = with_xact(|x| x.xact_last_rec_end);
    // TODO(guc): read the synchronous_commit GUC; hardcoded to the safe ON side
    // (clog is never set committed before the WAL is durable) until GUCs land.
    let sync = SYNCHRONOUS_COMMIT_DEFAULT;

    if (wrote_xlog && mark_xid_committed && sync > SYNCHRONOUS_COMMIT_OFF)
        || force_sync
        || nrels > 0
    {
        // SYNC commit: flush the WAL to disk BEFORE updating clog.
        crate::backend::access::transam::xlog::xlog_flush(shared.xlog(), recptr).await;
        if mark_xid_committed {
            transaction_id_commit_tree(shared.clog(), xid, children).await;
        }
    } else {
        // ASYNC commit: record the commit LSN in clog and request a future
        // flush, WITHOUT waiting. A crash may lose this commit -- acceptable
        // for synchronous_commit=off (see xact.c).
        shared.xlog().set_async_xact_lsn(recptr);
        if mark_xid_committed {
            transaction_id_async_commit_tree(shared.clog(), xid, children, recptr).await;
        }
    }

    let latest_xid = if mark_xid_committed {
        TransactionIdLatest(xid, children)
    } else {
        INVALID_TRANSACTION_ID
    };

    // SyncRepWaitForLSN (synchronous replication) is out of foundation.

    with_xact(|x| {
        x.xact_last_commit_end = x.xact_last_rec_end;
        x.xact_last_rec_end = INVALID_XLOG_REC_PTR;
    });
    latest_xid
}

// ===========================================================================
// CommitTransaction
// ===========================================================================

/// xact.c `CommitTransaction`.
async fn commit_transaction(shared: &Arc<SharedState>) {
    let state = with_xact(|x| x.cur().state);
    if state != TransState::InProgress {
        // TODO(panic): WARNING only.
        crate::elog!(
            crate::utils::elog::WARNING,
            format!(
                "CommitTransaction while in {} state",
                trans_state_as_string(state)
            )
        );
    }
    debug_assert!(with_xact(|x| !x.is_sub()));

    // Pre-commit user code (triggers / portals) is deferred; nothing to loop.
    call_xact_callbacks(XactEvent::PreCommit);

    // smgrDoPendingSyncs / large object / NOTIFY / serializable: deferred.

    // Prevent cancel/die interrupt while cleaning up.
    crate::session::current().inc_interrupt_holdoff_count();

    // Transition to TRANS_COMMIT.
    with_xact(|x| {
        x.cur_mut().state = TransState::Commit;
        x.cur_mut().parallel_mode_level = 0;
        x.cur_mut().parallel_child_xact = false;
    });

    // Durably commit: mark our xids committed in pg_xact.
    let latest_xid = record_transaction_commit(shared).await;

    // Let others know no transaction is in progress by me (procarray). This
    // must be after RecordTransactionCommit and before releasing locks.
    proc_array_end_transaction(shared, latest_xid);

    // Post-commit cleanup (noncritical resource release, phased).
    call_xact_callbacks(XactEvent::Commit);
    release_resources_phased(true);

    // AtEOXact_* coordinators: most are deferred subsystems (stubs).
    at_eoxact_combo_cid();
    at_eoxact_snapshot(true, false);

    // Reset the top frame to idle.
    with_xact(|x| {
        let s = x.top_mut();
        s.full_transaction_id = INVALID_FULL_TRANSACTION_ID;
        s.subtransaction_id = InvalidSubTransactionId;
        s.nesting_level = 0;
        s.guc_nest_level = 0;
        s.child_xids.clear();
        x.xact_top_full_xid = INVALID_FULL_TRANSACTION_ID;
        x.top_mut().state = TransState::Default;
    });

    crate::session::current().dec_interrupt_holdoff_count();
}

// ===========================================================================
// RecordTransactionAbort / AbortTransaction
// ===========================================================================

/// xact.c `RecordTransactionAbort`.
async fn record_transaction_abort(shared: &Arc<SharedState>, is_sub_xact: bool) -> TransactionId {
    let xid = GetCurrentTransactionIdIfAny();
    let Some(xid) = xid else {
        // No xid: nobody cares whether we aborted.
        if !is_sub_xact {
            with_xact(|x| x.xact_last_rec_end = INVALID_XLOG_REC_PTR);
        }
        return INVALID_TRANSACTION_ID;
    };

    // Check we haven't aborted halfway through commit.
    if transaction_id_did_commit(
        shared.clog(),
        shared.subtrans(),
        xid,
        INVALID_TRANSACTION_ID,
    )
    .await
    {
        // PANIC: cannot abort an already-committed transaction.
        std::process::abort();
    }

    let nrels = 0usize; // smgrGetPendingDeletes -- see record_transaction_commit.
    let children = xactGetCommittedChildren();

    // Critical section around the WAL+clog window.
    crate::session::current().inc_crit_section_count();

    let xact_time = if is_sub_xact {
        crate::utils::timestamp::GetCurrentTimestamp()
    } else {
        GetCurrentTransactionStopTimestamp()
    };
    let my_xact_flags = with_xact(|x| x.my_xact_flags);
    // Box the WAL-assembly future to keep this abort path's future small.
    let recptr = Box::pin(XactLogAbortRecord(
        shared,
        xact_time,
        &children,
        &[],
        &[],
        my_xact_flags,
        INVALID_TRANSACTION_ID,
        "",
    ))
    .await;
    with_xact(|x| x.xact_last_rec_end = recptr);

    // Nudge the WAL writer (abort durability is best-effort; no forced flush).
    if !is_sub_xact {
        shared.xlog().set_async_xact_lsn(recptr);
    }

    // Mark aborted in clog. OK without flushing -- a crash assumes abort anyway.
    transaction_id_abort_tree(shared.clog(), xid, &children).await;

    crate::session::current().dec_crit_section_count();

    let latest_xid = TransactionIdLatest(xid, &children);

    // XidCacheRemoveRunningXids for subxacts -- step 15 (ProcGlobal).

    if !is_sub_xact {
        with_xact(|x| x.xact_last_rec_end = INVALID_XLOG_REC_PTR);
    }
    let _ = nrels;
    latest_xid
}

/// xact.c `AbortTransaction`.
async fn abort_transaction(shared: &Arc<SharedState>) {
    crate::session::current().inc_interrupt_holdoff_count();

    // LWLockReleaseAll / UnlockBuffers / XLogResetInsertion / CV cancel /
    // LockErrorCleanup: lock-manager + buffer cleanup hooks. Reset WAL staging.
    crate::backend::access::transam::xloginsert::reset_insertion();

    let state = with_xact(|x| x.cur().state);
    if state != TransState::InProgress && state != TransState::Prepare {
        // TODO(panic): WARNING only.
        crate::elog!(
            crate::utils::elog::WARNING,
            format!(
                "AbortTransaction while in {} state",
                trans_state_as_string(state)
            )
        );
    }
    debug_assert!(with_xact(|x| !x.is_sub()));

    with_xact(|x| x.cur_mut().state = TransState::Abort);

    // Reset user id which might have been changed transiently.
    if let Some(sess) = crate::session::try_current() {
        let (u, ctx) = with_xact(|x| (x.cur().prev_user, x.cur().prev_sec_context));
        sess.set_current_user_id(u);
        sess.set_sec_context(ctx);
    }

    with_xact(|x| {
        x.cur_mut().parallel_mode_level = 0;
        x.cur_mut().parallel_child_xact = false;
    });

    // do abort processing: triggers/portals/largeobject/notify/twophase deferred.

    // Advertise abort in pg_xact (if we got an xid).
    let latest_xid = record_transaction_abort(shared, false).await;

    proc_array_end_transaction(shared, latest_xid);

    // Post-abort cleanup (phased, is_commit=false).
    call_xact_callbacks(XactEvent::Abort);
    release_resources_phased(false);
    at_eoxact_combo_cid();

    crate::session::current().dec_interrupt_holdoff_count();
    // State remains TRANS_ABORT until CleanupTransaction.
}

/// xact.c `CleanupTransaction`.
fn cleanup_transaction() {
    let state = with_xact(|x| x.cur().state);
    if state != TransState::Abort {
        // TODO(panic): FATAL.
        crate::elog!(
            crate::utils::elog::FATAL,
            format!(
                "CleanupTransaction: unexpected state {}",
                trans_state_as_string(state)
            )
        );
    }
    at_eoxact_snapshot(false, true);
    with_xact(|x| {
        let s = x.top_mut();
        s.full_transaction_id = INVALID_FULL_TRANSACTION_ID;
        s.subtransaction_id = InvalidSubTransactionId;
        s.nesting_level = 0;
        s.guc_nest_level = 0;
        s.child_xids.clear();
        s.parallel_mode_level = 0;
        s.parallel_child_xact = false;
        x.xact_top_full_xid = INVALID_FULL_TRANSACTION_ID;
        x.top_mut().state = TransState::Default;
    });
}

// ===========================================================================
// Phased resource release + per-EOXact coordinators (thin wrappers)
// ===========================================================================

/// The CommitTransaction/AbortTransaction ResourceOwner release order
/// (BEFORE_LOCKS -> LOCKS -> AFTER_LOCKS), rules s8.
fn release_resources_phased(is_commit: bool) {
    use crate::backend::utils::resowner::resowner;
    use crate::utils::resowner::ResourceReleasePhase;
    if let Some(owner) = resowner::try_current() {
        owner.release(ResourceReleasePhase::BeforeLocks, is_commit, true);
        owner.release(ResourceReleasePhase::Locks, is_commit, true);
        owner.release(ResourceReleasePhase::AfterLocks, is_commit, true);
    }
}

fn at_eoxact_combo_cid() {
    crate::backend::utils::time::combocid::at_eo_xact_combo_cid();
}

fn at_eoxact_snapshot(is_commit: bool, reset_xmin: bool) {
    crate::backend::utils::time::snapmgr::AtEOXact_Snapshot(is_commit, reset_xmin);
}

fn at_sub_commit_snapshot(level: i32) {
    crate::backend::utils::time::snapmgr::AtSubCommit_Snapshot(level);
}

fn at_sub_abort_snapshot(level: i32) {
    crate::backend::utils::time::snapmgr::AtSubAbort_Snapshot(level);
}

/// procarray.c `ProcArrayEndTransaction`. Clear the real MyProc slot (xid,
/// subxid cache, mirror) and advance latestCompletedXid / xactCompletionCount.
/// No-op when this backend has no live PGPROC (bootstrap / thin tests).
fn proc_array_end_transaction(shared: &Arc<SharedState>, latest_xid: TransactionId) {
    let procno = crate::storage::proc::current_proc_number();
    if procno == crate::storage::procnumber::INVALID_PROC_NUMBER {
        return;
    }
    let Some(g) = crate::storage::proc::proc_global() else {
        return;
    };
    // SAFETY: we own our own slot; proc_array_end_transaction takes ProcArrayLock
    // for the shared bookkeeping (mirror + latestCompletedXid).
    let Some(proc) = (unsafe { g.proc_mut(procno) }) else {
        return;
    };
    shared
        .proc_array()
        .proc_array_end_transaction(shared.variable_cache(), proc, latest_xid);
}

// ===========================================================================
// Command-level drivers (block state machine)
// ===========================================================================

/// xact.c `StartTransactionCommand`.
pub async fn StartTransactionCommand(shared: &Arc<SharedState>) {
    use TBlockState::{Default, Started, InProgress, ImplicitInProgress, SubInProgress, Abort, SubAbort};
    let bs = with_xact(|x| x.cur().block_state);
    match bs {
        Default => {
            start_transaction(shared);
            with_xact(|x| x.cur_mut().block_state = Started);
        }
        InProgress | ImplicitInProgress | SubInProgress | Abort | SubAbort => {}
        _ => {
            // TODO(panic): migrate to Result + ?
            crate::elog!(
                crate::utils::elog::ERROR,
                format!(
                    "StartTransactionCommand: unexpected state {}",
                    block_state_as_string(bs)
                )
            );
        }
    }
}

/// xact.c `SaveTransactionCharacteristics`.
pub fn SaveTransactionCharacteristics(s: &mut SavedTransactionCharacteristics) {
    with_xact(|x| {
        s.XactIsoLevel = x.xact_iso_level;
        s.XactReadOnly = x.xact_read_only;
        s.XactDeferrable = x.xact_deferrable;
    });
}

/// xact.c `RestoreTransactionCharacteristics`.
pub fn RestoreTransactionCharacteristics(s: &SavedTransactionCharacteristics) {
    with_xact(|x| {
        x.xact_iso_level = s.XactIsoLevel;
        x.xact_read_only = s.XactReadOnly;
        x.xact_deferrable = s.XactDeferrable;
    });
}

/// xact.c `CommitTransactionCommand` (loops the internal iterator).
pub async fn CommitTransactionCommand(shared: &Arc<SharedState>) {
    while !commit_transaction_command_internal(shared).await {}
}

#[allow(clippy::too_many_lines, reason = "1:1 port of C CommitTransactionCommand; splitting would diverge from PG structure")]
async fn commit_transaction_command_internal(shared: &Arc<SharedState>) -> bool {
    use TBlockState::{Default, ParallelInProgress, Started, Begin, InProgress, ImplicitInProgress, SubInProgress, End, Abort, SubAbort, AbortEnd, AbortPending, Prepare, SubBegin, SubRelease, SubCommit, SubAbortEnd, SubAbortPending, SubRestart, SubAbortRestart};
    let mut savetc = SavedTransactionCharacteristics {
        XactIsoLevel: 0,
        XactReadOnly: false,
        XactDeferrable: false,
    };
    SaveTransactionCharacteristics(&mut savetc);

    let bs = with_xact(|x| x.cur().block_state);
    match bs {
        Default | ParallelInProgress => {
            // TODO(panic): FATAL.
            crate::elog!(
                crate::utils::elog::FATAL,
                format!(
                    "CommitTransactionCommand: unexpected state {}",
                    block_state_as_string(bs)
                )
            );
        }
        Started => {
            Box::pin(commit_transaction(shared)).await;
            with_xact(|x| x.cur_mut().block_state = Default);
        }
        Begin => {
            with_xact(|x| x.cur_mut().block_state = InProgress);
        }
        InProgress | ImplicitInProgress | SubInProgress => {
            CommandCounterIncrement();
        }
        End => {
            Box::pin(commit_transaction(shared)).await;
            with_xact(|x| x.cur_mut().block_state = Default);
            maybe_chain(shared, &savetc);
        }
        Abort | SubAbort => {}
        AbortEnd => {
            cleanup_transaction();
            with_xact(|x| x.cur_mut().block_state = Default);
            maybe_chain(shared, &savetc);
        }
        AbortPending => {
            Box::pin(abort_transaction(shared)).await;
            cleanup_transaction();
            with_xact(|x| x.cur_mut().block_state = Default);
            maybe_chain(shared, &savetc);
        }
        Prepare => {
            // PrepareTransaction (two-phase) is a stub. TODO(twophase).
            prepare_transaction_stub();
            with_xact(|x| x.cur_mut().block_state = Default);
        }
        SubBegin => {
            // SAVEPOINT pushed a SUBBEGIN frame; finish starting the subxact.
            start_sub_transaction(shared);
            with_xact(|x| x.cur_mut().block_state = SubInProgress);
        }
        SubRelease => loop {
            commit_sub_transaction(shared);
            let s = with_xact(|x| x.cur().block_state);
            if s != SubRelease {
                break;
            }
        },
        SubCommit => {
            loop {
                commit_sub_transaction(shared);
                let s = with_xact(|x| x.cur().block_state);
                if s != SubCommit {
                    break;
                }
            }
            let s = with_xact(|x| x.cur().block_state);
            if s == End {
                Box::pin(commit_transaction(shared)).await;
                with_xact(|x| x.cur_mut().block_state = Default);
                maybe_chain(shared, &savetc);
            } else if s == Prepare {
                prepare_transaction_stub();
                with_xact(|x| x.cur_mut().block_state = Default);
            } else {
                // TODO(panic): migrate to Result + ?
                crate::elog!(
                    crate::utils::elog::ERROR,
                    format!(
                        "CommitTransactionCommand: unexpected state {}",
                        block_state_as_string(s)
                    )
                );
            }
        }
        SubAbortEnd => {
            cleanup_sub_transaction();
            return false;
        }
        SubAbortPending => {
            Box::pin(abort_sub_transaction(shared)).await;
            cleanup_sub_transaction();
            return false;
        }
        SubRestart => {
            let (name, level) = with_xact(|x| (x.cur_mut().name.take(), x.cur().savepoint_level));
            Box::pin(abort_sub_transaction(shared)).await;
            cleanup_sub_transaction();
            DefineSavepoint(None);
            with_xact(|x| {
                x.cur_mut().name = name;
                x.cur_mut().savepoint_level = level;
            });
            start_sub_transaction(shared);
            with_xact(|x| x.cur_mut().block_state = SubInProgress);
        }
        SubAbortRestart => {
            let (name, level) = with_xact(|x| (x.cur_mut().name.take(), x.cur().savepoint_level));
            cleanup_sub_transaction();
            DefineSavepoint(None);
            with_xact(|x| {
                x.cur_mut().name = name;
                x.cur_mut().savepoint_level = level;
            });
            start_sub_transaction(shared);
            with_xact(|x| x.cur_mut().block_state = SubInProgress);
        }
    }
    true
}

/// The `s->chain` re-start tail shared by several CommitTransactionCommand arms.
fn maybe_chain(shared: &Arc<SharedState>, savetc: &SavedTransactionCharacteristics) {
    let chain = with_xact(|x| x.cur().chain);
    if chain {
        start_transaction(shared);
        with_xact(|x| {
            x.cur_mut().block_state = TBlockState::InProgress;
            x.cur_mut().chain = false;
        });
        RestoreTransactionCharacteristics(savetc);
    }
}

/// PrepareTransaction is two-phase commit (twophase.c), a deferred subsystem.
fn prepare_transaction_stub() {
    // TODO(twophase): PrepareTransaction.
    unimplemented!("two-phase PrepareTransaction is deferred (twophase.c)");
}

/// xact.c `AbortCurrentTransaction` (loops the internal iterator).
pub async fn AbortCurrentTransaction(shared: &Arc<SharedState>) {
    while !abort_current_transaction_internal(shared).await {}
}

async fn abort_current_transaction_internal(shared: &Arc<SharedState>) -> bool {
    use TBlockState::{Default, Started, ImplicitInProgress, Begin, InProgress, ParallelInProgress, Abort, End, SubAbort, AbortEnd, AbortPending, Prepare, SubInProgress, SubBegin, SubRelease, SubCommit, SubAbortPending, SubRestart, SubAbortEnd, SubAbortRestart};
    let (bs, low_state) = with_xact(|x| (x.cur().block_state, x.cur().state));
    match bs {
        Default => {
            if low_state == TransState::Default {
                // idle, nothing to do
            } else {
                if low_state == TransState::Start {
                    with_xact(|x| x.cur_mut().state = TransState::InProgress);
                }
                Box::pin(abort_transaction(shared)).await;
                cleanup_transaction();
            }
        }
        Started | ImplicitInProgress => {
            Box::pin(abort_transaction(shared)).await;
            cleanup_transaction();
            with_xact(|x| x.cur_mut().block_state = Default);
        }
        Begin => {
            Box::pin(abort_transaction(shared)).await;
            cleanup_transaction();
            with_xact(|x| x.cur_mut().block_state = Default);
        }
        InProgress | ParallelInProgress => {
            Box::pin(abort_transaction(shared)).await;
            with_xact(|x| x.cur_mut().block_state = Abort);
        }
        End => {
            Box::pin(abort_transaction(shared)).await;
            cleanup_transaction();
            with_xact(|x| x.cur_mut().block_state = Default);
        }
        Abort | SubAbort => {}
        AbortEnd => {
            cleanup_transaction();
            with_xact(|x| x.cur_mut().block_state = Default);
        }
        AbortPending => {
            Box::pin(abort_transaction(shared)).await;
            cleanup_transaction();
            with_xact(|x| x.cur_mut().block_state = Default);
        }
        Prepare => {
            Box::pin(abort_transaction(shared)).await;
            cleanup_transaction();
            with_xact(|x| x.cur_mut().block_state = Default);
        }
        SubInProgress => {
            Box::pin(abort_sub_transaction(shared)).await;
            with_xact(|x| x.cur_mut().block_state = SubAbort);
        }
        SubBegin | SubRelease | SubCommit | SubAbortPending | SubRestart => {
            Box::pin(abort_sub_transaction(shared)).await;
            cleanup_sub_transaction();
            return false;
        }
        SubAbortEnd | SubAbortRestart => {
            cleanup_sub_transaction();
            return false;
        }
    }
    true
}

// ===========================================================================
// CheckTransactionBlock helpers
// ===========================================================================

/// xact.c `PreventInTransactionBlock`.
pub fn PreventInTransactionBlock(is_top_level: bool, stmt_type: &str) {
    if IsTransactionBlock() {
        // TODO(panic): migrate to Result + ?
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("{stmt_type} cannot run inside a transaction block")
        );
    }
    if IsSubTransaction() {
        // TODO(panic): migrate to Result + ?
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("{stmt_type} cannot run inside a subtransaction")
        );
    }
    if !is_top_level {
        // TODO(panic): migrate to Result + ?
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("{stmt_type} cannot be executed from a function")
        );
    }
    let bs = with_xact(|x| x.cur().block_state);
    if bs != TBlockState::Default && bs != TBlockState::Started {
        // TODO(panic): FATAL.
        crate::elog!(
            crate::utils::elog::FATAL,
            "cannot prevent transaction chain".to_string()
        );
    }
    with_xact(|x| x.my_xact_flags |= XactFlags::NEEDIMMEDIATECOMMIT.bits() as i32);
}

/// xact.c `WarnNoTransactionBlock`.
pub fn WarnNoTransactionBlock(is_top_level: bool, stmt_type: &str) {
    check_transaction_block(is_top_level, false, stmt_type);
}

/// xact.c `RequireTransactionBlock`.
pub fn RequireTransactionBlock(is_top_level: bool, stmt_type: &str) {
    check_transaction_block(is_top_level, true, stmt_type);
}

fn check_transaction_block(is_top_level: bool, throw_error: bool, stmt_type: &str) {
    if IsTransactionBlock() || IsSubTransaction() || !is_top_level {
        return;
    }
    let level = if throw_error {
        crate::utils::elog::ERROR
    } else {
        crate::utils::elog::WARNING
    };
    // TODO(panic): ERROR variant panics, WARNING logs.
    crate::elog!(
        level,
        format!("{stmt_type} can only be used in transaction blocks")
    );
}

/// xact.c `IsInTransactionBlock`.
pub fn IsInTransactionBlock(is_top_level: bool) -> bool {
    if IsTransactionBlock() || IsSubTransaction() || !is_top_level {
        return true;
    }
    let bs = with_xact(|x| x.cur().block_state);
    bs != TBlockState::Default && bs != TBlockState::Started
}

// ===========================================================================
// Callbacks
// ===========================================================================

/// xact.c `RegisterXactCallback`.
pub fn RegisterXactCallback(callback: XactCallback) {
    with_xact(|x| x.xact_callbacks.push(callback));
}

/// xact.c `UnregisterXactCallback`.
pub fn UnregisterXactCallback(callback: XactCallback) {
    with_xact(|x| {
        if let Some(pos) = x
            .xact_callbacks
            .iter()
            .position(|&c| std::ptr::fn_addr_eq(c, callback))
        {
            x.xact_callbacks.remove(pos);
        }
    });
}

fn call_xact_callbacks(event: XactEvent) {
    let cbs = with_xact(|x| x.xact_callbacks.clone());
    for cb in cbs {
        cb(event);
    }
}

/// xact.c `RegisterSubXactCallback`.
pub fn RegisterSubXactCallback(callback: SubXactCallback) {
    with_xact(|x| x.subxact_callbacks.push(callback));
}

/// xact.c `UnregisterSubXactCallback`.
pub fn UnregisterSubXactCallback(callback: SubXactCallback) {
    with_xact(|x| {
        if let Some(pos) = x
            .subxact_callbacks
            .iter()
            .position(|&c| std::ptr::fn_addr_eq(c, callback))
        {
            x.subxact_callbacks.remove(pos);
        }
    });
}

fn call_sub_xact_callbacks(
    event: SubXactEvent,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
) {
    let cbs = with_xact(|x| x.subxact_callbacks.clone());
    for cb in cbs {
        cb(event, my_subid, parent_subid);
    }
}

// ===========================================================================
// Transaction block support (BEGIN/COMMIT/ROLLBACK/SAVEPOINT)
// ===========================================================================

/// xact.c `BeginTransactionBlock` (BEGIN).
pub fn BeginTransactionBlock() {
    use TBlockState::{Started, ImplicitInProgress, Begin, InProgress, ParallelInProgress, SubInProgress, Abort, SubAbort};
    let bs = with_xact(|x| x.cur().block_state);
    match bs {
        Started | ImplicitInProgress => {
            with_xact(|x| x.cur_mut().block_state = Begin);
        }
        InProgress | ParallelInProgress | SubInProgress | Abort | SubAbort => {
            // TODO(panic): WARNING only.
            crate::elog!(
                crate::utils::elog::WARNING,
                "there is already a transaction in progress".to_string()
            );
        }
        _ => {
            // TODO(panic): FATAL.
            crate::elog!(
                crate::utils::elog::FATAL,
                format!(
                    "BeginTransactionBlock: unexpected state {}",
                    block_state_as_string(bs)
                )
            );
        }
    }
}

/// xact.c `PrepareTransactionBlock` (PREPARE). Two-phase is deferred, but the
/// block-state transition is translated faithfully.
pub fn PrepareTransactionBlock(gid: &str) -> bool {
    let result = EndTransactionBlock(false);
    if result {
        // Walk to the top frame.
        let top_bs = with_xact(|x| x.top().block_state);
        if top_bs == TBlockState::End {
            // prepareGID storage -> TODO(twophase). Set the block state.
            let _ = gid;
            with_xact(|x| x.top_mut().block_state = TBlockState::Prepare);
            return true;
        }
        return false;
    }
    result
}

/// xact.c `EndTransactionBlock` (COMMIT). Returns true for COMMIT, false for the
/// ROLLBACK-equivalent path.
pub fn EndTransactionBlock(chain: bool) -> bool {
    use TBlockState::{InProgress, End, ImplicitInProgress, Abort, AbortEnd, SubInProgress, SubCommit, SubAbort, SubAbortPending, SubAbortEnd, AbortPending, Started, ParallelInProgress};
    let bs = with_xact(|x| x.cur().block_state);
    let mut result = false;
    match bs {
        InProgress => {
            with_xact(|x| x.cur_mut().block_state = End);
            result = true;
        }
        ImplicitInProgress => {
            if chain {
                // TODO(panic): migrate to Result + ?
                crate::elog!(
                    crate::utils::elog::ERROR,
                    "COMMIT AND CHAIN can only be used in transaction blocks".to_string()
                );
            } else {
                // TODO(panic): WARNING only.
                crate::elog!(
                    crate::utils::elog::WARNING,
                    "there is no transaction in progress".to_string()
                );
            }
            with_xact(|x| x.cur_mut().block_state = End);
            result = true;
        }
        Abort => {
            with_xact(|x| x.cur_mut().block_state = AbortEnd);
        }
        SubInProgress => {
            // subcommit all open subxacts, then commit the main xact.
            with_xact(|x| {
                for i in (1..x.stack.len()).rev() {
                    debug_assert_eq!(x.stack[i].block_state, SubInProgress);
                    x.stack[i].block_state = SubCommit;
                }
                debug_assert_eq!(x.stack[0].block_state, InProgress);
                x.stack[0].block_state = End;
            });
            result = true;
        }
        SubAbort => {
            // treat COMMIT as ROLLBACK: abort everything.
            with_xact(|x| {
                for i in (1..x.stack.len()).rev() {
                    x.stack[i].block_state = match x.stack[i].block_state {
                        SubInProgress => SubAbortPending,
                        SubAbort => SubAbortEnd,
                        _ => unreachable!("EndTransactionBlock: unexpected sub state"),
                    };
                }
                x.stack[0].block_state = match x.stack[0].block_state {
                    InProgress => AbortPending,
                    Abort => AbortEnd,
                    _ => unreachable!("EndTransactionBlock: unexpected top state"),
                };
            });
        }
        Started => {
            if chain {
                // TODO(panic): migrate to Result + ?
                crate::elog!(
                    crate::utils::elog::ERROR,
                    "COMMIT AND CHAIN can only be used in transaction blocks".to_string()
                );
            } else {
                // TODO(panic): WARNING only.
                crate::elog!(
                    crate::utils::elog::WARNING,
                    "there is no transaction in progress".to_string()
                );
            }
            result = true;
        }
        ParallelInProgress => {
            // TODO(panic): FATAL.
            crate::elog!(
                crate::utils::elog::FATAL,
                "cannot commit during a parallel operation".to_string()
            );
        }
        _ => {
            // TODO(panic): FATAL.
            crate::elog!(
                crate::utils::elog::FATAL,
                format!(
                    "EndTransactionBlock: unexpected state {}",
                    block_state_as_string(bs)
                )
            );
        }
    }
    with_xact(|x| x.cur_mut().chain = chain);
    result
}

/// xact.c `UserAbortTransactionBlock` (ROLLBACK).
pub fn UserAbortTransactionBlock(chain: bool) {
    use TBlockState::{InProgress, AbortPending, Abort, AbortEnd, SubInProgress, SubAbort, SubAbortPending, SubAbortEnd, Started, ImplicitInProgress, ParallelInProgress};
    let bs = with_xact(|x| x.cur().block_state);
    match bs {
        InProgress => {
            with_xact(|x| x.cur_mut().block_state = AbortPending);
        }
        Abort => {
            with_xact(|x| x.cur_mut().block_state = AbortEnd);
        }
        SubInProgress | SubAbort => {
            with_xact(|x| {
                for i in (1..x.stack.len()).rev() {
                    x.stack[i].block_state = match x.stack[i].block_state {
                        SubInProgress => SubAbortPending,
                        SubAbort => SubAbortEnd,
                        _ => unreachable!("UserAbortTransactionBlock: unexpected sub state"),
                    };
                }
                x.stack[0].block_state = match x.stack[0].block_state {
                    InProgress => AbortPending,
                    Abort => AbortEnd,
                    _ => unreachable!("UserAbortTransactionBlock: unexpected top state"),
                };
            });
        }
        Started | ImplicitInProgress => {
            if chain {
                // TODO(panic): migrate to Result + ?
                crate::elog!(
                    crate::utils::elog::ERROR,
                    "ROLLBACK AND CHAIN can only be used in transaction blocks".to_string()
                );
            } else {
                // TODO(panic): WARNING only.
                crate::elog!(
                    crate::utils::elog::WARNING,
                    "there is no transaction in progress".to_string()
                );
            }
            with_xact(|x| x.cur_mut().block_state = AbortPending);
        }
        ParallelInProgress => {
            // TODO(panic): FATAL.
            crate::elog!(
                crate::utils::elog::FATAL,
                "cannot abort during a parallel operation".to_string()
            );
        }
        _ => {
            // TODO(panic): FATAL.
            crate::elog!(
                crate::utils::elog::FATAL,
                format!(
                    "UserAbortTransactionBlock: unexpected state {}",
                    block_state_as_string(bs)
                )
            );
        }
    }
    with_xact(|x| x.cur_mut().chain = chain);
}

/// xact.c `BeginImplicitTransactionBlock`.
pub fn BeginImplicitTransactionBlock() {
    with_xact(|x| {
        if x.cur().block_state == TBlockState::Started {
            x.cur_mut().block_state = TBlockState::ImplicitInProgress;
        }
    });
}

/// xact.c `EndImplicitTransactionBlock`.
pub fn EndImplicitTransactionBlock() {
    with_xact(|x| {
        if x.cur().block_state == TBlockState::ImplicitInProgress {
            x.cur_mut().block_state = TBlockState::Started;
        }
    });
}

/// xact.c `DefineSavepoint` (SAVEPOINT).
pub fn DefineSavepoint(name: Option<&str>) {
    use TBlockState::{InProgress, SubInProgress, ImplicitInProgress};
    if IsInParallelMode() || crate::access::parallel::IsParallelWorker() {
        // TODO(panic): migrate to Result + ?
        crate::elog!(
            crate::utils::elog::ERROR,
            "cannot define savepoints during a parallel operation".to_string()
        );
    }
    let bs = with_xact(|x| x.cur().block_state);
    match bs {
        InProgress | SubInProgress => {
            push_transaction();
            if let Some(n) = name {
                with_xact(|x| x.cur_mut().name = Some(n.to_string()));
            }
        }
        ImplicitInProgress => {
            // TODO(panic): migrate to Result + ?
            crate::elog!(
                crate::utils::elog::ERROR,
                "SAVEPOINT can only be used in transaction blocks".to_string()
            );
        }
        _ => {
            // TODO(panic): FATAL.
            crate::elog!(
                crate::utils::elog::FATAL,
                format!(
                    "DefineSavepoint: unexpected state {}",
                    block_state_as_string(bs)
                )
            );
        }
    }
}

/// xact.c `ReleaseSavepoint` (RELEASE).
pub fn ReleaseSavepoint(name: &str) {
    use TBlockState::{InProgress, ImplicitInProgress, SubInProgress};
    if IsInParallelMode() || crate::access::parallel::IsParallelWorker() {
        // TODO(panic): migrate to Result + ?
        crate::elog!(
            crate::utils::elog::ERROR,
            "cannot release savepoints during a parallel operation".to_string()
        );
    }
    let bs = with_xact(|x| x.cur().block_state);
    match bs {
        InProgress => {
            // TODO(panic): migrate to Result + ?
            crate::elog!(
                crate::utils::elog::ERROR,
                format!("savepoint \"{name}\" does not exist")
            );
        }
        ImplicitInProgress => {
            // TODO(panic): migrate to Result + ?
            crate::elog!(
                crate::utils::elog::ERROR,
                "RELEASE SAVEPOINT can only be used in transaction blocks".to_string()
            );
        }
        SubInProgress => {}
        _ => {
            // TODO(panic): FATAL.
            crate::elog!(
                crate::utils::elog::FATAL,
                format!(
                    "ReleaseSavepoint: unexpected state {}",
                    block_state_as_string(bs)
                )
            );
        }
    }

    let target = find_savepoint_target(name);
    let cur_level = with_xact(|x| x.cur().savepoint_level);
    let Some(target_idx) = target else {
        // TODO(panic): migrate to Result + ?
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("savepoint \"{name}\" does not exist")
        );
        unreachable!()
    };
    if with_xact(|x| x.stack[target_idx].savepoint_level) != cur_level {
        // TODO(panic): migrate to Result + ?
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("savepoint \"{name}\" does not exist within current savepoint level")
        );
    }
    // Mark "commit pending" from current down to the target (inclusive).
    with_xact(|x| {
        for i in (target_idx..x.stack.len()).rev() {
            debug_assert_eq!(x.stack[i].block_state, TBlockState::SubInProgress);
            x.stack[i].block_state = TBlockState::SubRelease;
        }
    });
}

/// xact.c `RollbackToSavepoint` (ROLLBACK TO).
pub fn RollbackToSavepoint(name: &str) {
    use TBlockState::{InProgress, Abort, ImplicitInProgress, SubInProgress, SubAbort};
    if IsInParallelMode() || crate::access::parallel::IsParallelWorker() {
        // TODO(panic): migrate to Result + ?
        crate::elog!(
            crate::utils::elog::ERROR,
            "cannot rollback to savepoints during a parallel operation".to_string()
        );
    }
    let bs = with_xact(|x| x.cur().block_state);
    match bs {
        InProgress | Abort => {
            // TODO(panic): migrate to Result + ?
            crate::elog!(
                crate::utils::elog::ERROR,
                format!("savepoint \"{name}\" does not exist")
            );
        }
        ImplicitInProgress => {
            // TODO(panic): migrate to Result + ?
            crate::elog!(
                crate::utils::elog::ERROR,
                "ROLLBACK TO SAVEPOINT can only be used in transaction blocks".to_string()
            );
        }
        SubInProgress | SubAbort => {}
        _ => {
            // TODO(panic): FATAL.
            crate::elog!(
                crate::utils::elog::FATAL,
                format!(
                    "RollbackToSavepoint: unexpected state {}",
                    block_state_as_string(bs)
                )
            );
        }
    }

    let Some(target_idx) = find_savepoint_target(name) else {
        // TODO(panic): migrate to Result + ?
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("savepoint \"{name}\" does not exist")
        );
        unreachable!()
    };
    let cur_level = with_xact(|x| x.cur().savepoint_level);
    if with_xact(|x| x.stack[target_idx].savepoint_level) != cur_level {
        // TODO(panic): migrate to Result + ?
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("savepoint \"{name}\" does not exist within current savepoint level")
        );
    }
    // Mark "abort pending" from current down to (but excluding) target; the
    // target becomes "restart pending".
    with_xact(|x| {
        for i in (target_idx + 1..x.stack.len()).rev() {
            x.stack[i].block_state = match x.stack[i].block_state {
                TBlockState::SubInProgress => TBlockState::SubAbortPending,
                TBlockState::SubAbort => TBlockState::SubAbortEnd,
                _ => unreachable!("RollbackToSavepoint: unexpected state"),
            };
        }
        x.stack[target_idx].block_state = match x.stack[target_idx].block_state {
            TBlockState::SubInProgress => TBlockState::SubRestart,
            TBlockState::SubAbort => TBlockState::SubAbortRestart,
            _ => unreachable!("RollbackToSavepoint: unexpected target state"),
        };
    });
}

/// Find the stack index of the savepoint named `name`, searching from the
/// current frame towards the top.
fn find_savepoint_target(name: &str) -> Option<usize> {
    with_xact(|x| {
        x.stack
            .iter()
            .enumerate()
            .rev()
            .find(|(_, s)| s.name.as_deref() == Some(name))
            .map(|(i, _)| i)
    })
}

/// xact.c `BeginInternalSubTransaction`.
pub async fn BeginInternalSubTransaction(shared: &Arc<SharedState>, name: Option<&str>) {
    use TBlockState::{Started, InProgress, ImplicitInProgress, ParallelInProgress, End, Prepare, SubInProgress};
    let bs = with_xact(|x| x.cur().block_state);
    match bs {
        Started | InProgress | ImplicitInProgress | ParallelInProgress | End | Prepare
        | SubInProgress => {
            push_transaction();
            if let Some(n) = name {
                with_xact(|x| x.cur_mut().name = Some(n.to_string()));
            }
        }
        _ => {
            // TODO(panic): FATAL.
            crate::elog!(
                crate::utils::elog::FATAL,
                format!(
                    "BeginInternalSubTransaction: unexpected state {}",
                    block_state_as_string(bs)
                )
            );
        }
    }
    CommitTransactionCommand(shared).await;
    StartTransactionCommand(shared).await;
}

/// xact.c `ReleaseCurrentSubTransaction`.
pub async fn ReleaseCurrentSubTransaction(shared: &Arc<SharedState>) {
    let bs = with_xact(|x| x.cur().block_state);
    if bs != TBlockState::SubInProgress {
        // TODO(panic): migrate to Result + ?
        crate::elog!(
            crate::utils::elog::ERROR,
            format!(
                "ReleaseCurrentSubTransaction: unexpected state {}",
                block_state_as_string(bs)
            )
        );
    }
    commit_sub_transaction(shared);
}

/// xact.c `RollbackAndReleaseCurrentSubTransaction`.
pub async fn RollbackAndReleaseCurrentSubTransaction(shared: &Arc<SharedState>) {
    use TBlockState::{SubInProgress, SubAbort};
    let bs = with_xact(|x| x.cur().block_state);
    match bs {
        SubInProgress | SubAbort => {}
        _ => {
            // TODO(panic): FATAL.
            crate::elog!(
                crate::utils::elog::FATAL,
                format!(
                    "RollbackAndReleaseCurrentSubTransaction: unexpected state {}",
                    block_state_as_string(bs)
                )
            );
        }
    }
    if bs == SubInProgress {
        Box::pin(abort_sub_transaction(shared)).await;
    }
    cleanup_sub_transaction();
}

/// xact.c `AbortOutOfAnyTransaction`.
pub async fn AbortOutOfAnyTransaction(shared: &Arc<SharedState>) {
    use TBlockState::{Default, Started, Begin, InProgress, ImplicitInProgress, ParallelInProgress, End, AbortPending, Prepare, Abort, AbortEnd, SubBegin, SubInProgress, SubRelease, SubCommit, SubAbortPending, SubRestart, SubAbort, SubAbortEnd, SubAbortRestart};
    loop {
        let (bs, low_state) = with_xact(|x| (x.cur().block_state, x.cur().state));
        match bs {
            Default => {
                if low_state != TransState::Default {
                    if low_state == TransState::Start {
                        with_xact(|x| x.cur_mut().state = TransState::InProgress);
                    }
                    Box::pin(abort_transaction(shared)).await;
                    cleanup_transaction();
                }
            }
            Started | Begin | InProgress | ImplicitInProgress | ParallelInProgress | End
            | AbortPending | Prepare => {
                Box::pin(abort_transaction(shared)).await;
                cleanup_transaction();
                with_xact(|x| x.cur_mut().block_state = Default);
            }
            Abort | AbortEnd => {
                cleanup_transaction();
                with_xact(|x| x.cur_mut().block_state = Default);
            }
            SubBegin | SubInProgress | SubRelease | SubCommit | SubAbortPending | SubRestart => {
                Box::pin(abort_sub_transaction(shared)).await;
                cleanup_sub_transaction();
            }
            SubAbort | SubAbortEnd | SubAbortRestart => {
                cleanup_sub_transaction();
            }
        }
        if with_xact(|x| x.cur().block_state == Default) {
            break;
        }
    }
    debug_assert!(with_xact(|x| !x.is_sub()));
}

/// xact.c `IsTransactionBlock`.
pub fn IsTransactionBlock() -> bool {
    with_xact(|x| {
        !matches!(
            x.cur().block_state,
            TBlockState::Default | TBlockState::Started
        )
    })
}

/// xact.c `IsTransactionOrTransactionBlock`.
pub fn IsTransactionOrTransactionBlock() -> bool {
    with_xact(|x| x.cur().block_state != TBlockState::Default)
}

/// xact.c `TransactionBlockStatusCode`: 'I' idle, 'T' in xact, 'E' failed.
pub fn TransactionBlockStatusCode() -> i8 {
    use TBlockState::{Default, Started, Begin, SubBegin, InProgress, ImplicitInProgress, ParallelInProgress, SubInProgress, End, SubRelease, SubCommit, Prepare, Abort, SubAbort, AbortEnd, SubAbortEnd, AbortPending, SubAbortPending, SubRestart, SubAbortRestart};
    let bs = with_xact(|x| x.cur().block_state);
    let c = match bs {
        Default | Started => b'I',
        Begin | SubBegin | InProgress | ImplicitInProgress | ParallelInProgress | SubInProgress
        | End | SubRelease | SubCommit | Prepare => b'T',
        Abort | SubAbort | AbortEnd | SubAbortEnd | AbortPending | SubAbortPending | SubRestart
        | SubAbortRestart => b'E',
    };
    c as i8
}

/// xact.c `IsSubTransaction`.
pub fn IsSubTransaction() -> bool {
    with_xact_or(false, |x| x.cur().nesting_level >= 2)
}

// ===========================================================================
// Sub-transaction lifecycle
// ===========================================================================

/// xact.c `StartSubTransaction`.
fn start_sub_transaction(shared: &Arc<SharedState>) {
    let state = with_xact(|x| x.cur().state);
    if state != TransState::Default {
        // TODO(panic): WARNING only.
        crate::elog!(
            crate::utils::elog::WARNING,
            format!(
                "StartSubTransaction while in {} state",
                trans_state_as_string(state)
            )
        );
    }
    with_xact(|x| x.cur_mut().state = TransState::Start);
    // AtSubStart_Memory/ResourceOwner: resource owner tree handled by resowner.
    // AfterTriggerBeginSubXact: deferred.
    with_xact(|x| x.cur_mut().state = TransState::InProgress);
    let (my_subid, parent_subid) = with_xact(|x| {
        (
            x.cur().subtransaction_id,
            x.stack[x.stack.len() - 2].subtransaction_id,
        )
    });
    call_sub_xact_callbacks(SubXactEvent::StartSub, my_subid, parent_subid);
    let _ = shared;
}

/// xact.c `CommitSubTransaction`.
fn commit_sub_transaction(shared: &Arc<SharedState>) {
    let state = with_xact(|x| x.cur().state);
    if state != TransState::InProgress {
        // TODO(panic): WARNING only.
        crate::elog!(
            crate::utils::elog::WARNING,
            format!(
                "CommitSubTransaction while in {} state",
                trans_state_as_string(state)
            )
        );
    }
    let (my_subid, parent_subid, nest_level, guc_nest_level) = with_xact(|x| {
        (
            x.cur().subtransaction_id,
            x.stack[x.stack.len() - 2].subtransaction_id,
            x.cur().nesting_level,
            x.cur().guc_nest_level,
        )
    });
    call_sub_xact_callbacks(SubXactEvent::PreCommitSub, my_subid, parent_subid);

    with_xact(|x| x.cur_mut().state = TransState::Commit);

    // Must CCI so the subtransaction's commands are seen as done.
    CommandCounterIncrement();

    // Roll my xid + childXids up to my parent (AtSubCommit_childXids).
    let has_xid = with_xact(|x| FullTransactionIdIsValid(x.cur().full_transaction_id));
    if has_xid {
        with_xact(|x| {
            let n = x.stack.len();
            let my_xid = XidFromFullTransactionId(x.stack[n - 1].full_transaction_id);
            let mut mine = std::mem::take(&mut x.stack[n - 1].child_xids);
            let parent = &mut x.stack[n - 2].child_xids;
            parent.push(my_xid);
            parent.append(&mut mine);
        });
    }
    call_sub_xact_callbacks(SubXactEvent::CommitSub, my_subid, parent_subid);

    // Phased resource release (locks transfer to parent owner).
    release_resources_phased(true);
    at_sub_commit_snapshot(nest_level);
    let _ = guc_nest_level;

    // Restore the parent's read-only state.
    with_xact(|x| x.xact_read_only = x.cur().prev_xact_read_only);

    with_xact(|x| x.cur_mut().state = TransState::Default);
    pop_transaction();
    let _ = shared;
}

/// xact.c `AbortSubTransaction`.
async fn abort_sub_transaction(shared: &Arc<SharedState>) {
    crate::session::current().inc_interrupt_holdoff_count();
    crate::backend::access::transam::xloginsert::reset_insertion();

    let state = with_xact(|x| x.cur().state);
    if state != TransState::InProgress {
        // TODO(panic): WARNING only.
        crate::elog!(
            crate::utils::elog::WARNING,
            format!(
                "AbortSubTransaction while in {} state",
                trans_state_as_string(state)
            )
        );
    }
    with_xact(|x| x.cur_mut().state = TransState::Abort);

    // Reset user id (see AbortTransaction).
    if let Some(sess) = crate::session::try_current() {
        let (u, ctx) = with_xact(|x| (x.cur().prev_user, x.cur().prev_sec_context));
        sess.set_current_user_id(u);
        sess.set_sec_context(ctx);
    }
    with_xact(|x| x.cur_mut().parallel_mode_level = 0);

    let (my_subid, parent_subid, nest_level) = with_xact(|x| {
        (
            x.cur().subtransaction_id,
            x.stack[x.stack.len() - 2].subtransaction_id,
            x.cur().nesting_level,
        )
    });

    // triggers/portals/largeobject/notify deferred.
    record_transaction_abort(shared, true).await;

    // AtSubAbort_childXids.
    with_xact(|x| x.cur_mut().child_xids.clear());
    call_sub_xact_callbacks(SubXactEvent::AbortSub, my_subid, parent_subid);
    release_resources_phased(false);
    at_sub_abort_snapshot(nest_level);

    with_xact(|x| x.xact_read_only = x.cur().prev_xact_read_only);
    crate::session::current().dec_interrupt_holdoff_count();
}

/// xact.c `CleanupSubTransaction`.
fn cleanup_sub_transaction() {
    let state = with_xact(|x| x.cur().state);
    if state != TransState::Abort {
        // TODO(panic): WARNING only.
        crate::elog!(
            crate::utils::elog::WARNING,
            format!(
                "CleanupSubTransaction while in {} state",
                trans_state_as_string(state)
            )
        );
    }
    with_xact(|x| x.cur_mut().state = TransState::Default);
    pop_transaction();
}

/// xact.c `PushTransaction`: create a new subtransaction stack frame.
fn push_transaction() {
    with_xact(|x| {
        x.current_sub_transaction_id = SubTransactionId(x.current_sub_transaction_id.0 + 1);
        if x.current_sub_transaction_id == InvalidSubTransactionId {
            x.current_sub_transaction_id = SubTransactionId(x.current_sub_transaction_id.0 - 1);
            // TODO(panic): migrate to Result + ?
            panic!("cannot have more than 2^32-1 subtransactions in a transaction");
        }
        let p = x.cur();
        let new = TransactionStateData {
            full_transaction_id: INVALID_FULL_TRANSACTION_ID,
            subtransaction_id: x.current_sub_transaction_id,
            name: None,
            savepoint_level: p.savepoint_level,
            state: TransState::Default,
            block_state: TBlockState::SubBegin,
            nesting_level: p.nesting_level + 1,
            guc_nest_level: p.guc_nest_level + 1, // NewGUCNestLevel (GUC deferred)
            child_xids: Vec::new(),
            prev_user: crate::session::try_current()
                .map_or(crate::postgres_ext::Oid(0), |s| s.current_user_id()),
            prev_sec_context: crate::session::try_current()
                .map_or(0, |s| s.sec_context()),
            prev_xact_read_only: x.xact_read_only,
            started_in_recovery: p.started_in_recovery,
            did_log_xid: false,
            parallel_mode_level: 0,
            parallel_child_xact: p.parallel_mode_level != 0 || p.parallel_child_xact,
            chain: false,
            top_xid_logged: false,
        };
        x.stack.push(new);
    });
}

/// xact.c `PopTransaction`: pop back to the parent frame.
fn pop_transaction() {
    with_xact(|x| {
        let state = x.cur().state;
        if state != TransState::Default {
            // TODO(panic): WARNING only.
            crate::elog!(
                crate::utils::elog::WARNING,
                format!(
                    "PopTransaction while in {} state",
                    trans_state_as_string(state)
                )
            );
        }
        if x.stack.len() < 2 {
            // TODO(panic): FATAL.
            crate::elog!(
                crate::utils::elog::FATAL,
                "PopTransaction with no parent".to_string()
            );
        }
        x.stack.pop();
    });
}

// ===========================================================================
// Parallel-worker state serialization (deferred -- parallel query out of scope)
// ===========================================================================

/// xact.c `EstimateTransactionStateSpace`.
pub fn EstimateTransactionStateSpace() -> usize {
    // TODO(parallel): parallel query is out of foundation.
    unimplemented!("parallel transaction state serialization is deferred")
}

/// xact.c `SerializeTransactionState`.
pub fn SerializeTransactionState(_maxsize: usize, _start_address: &mut [u8]) {
    // TODO(parallel): parallel query is out of foundation.
    unimplemented!("parallel transaction state serialization is deferred")
}

/// xact.c `StartParallelWorkerTransaction`.
pub fn StartParallelWorkerTransaction(_tstatespace: &[u8]) {
    // TODO(parallel): parallel query is out of foundation.
    unimplemented!("parallel worker transaction is deferred")
}

/// xact.c `EndParallelWorkerTransaction`.
pub fn EndParallelWorkerTransaction() {
    // TODO(parallel): parallel query is out of foundation.
    unimplemented!("parallel worker transaction is deferred")
}

// ===========================================================================
// xactGetCommittedChildren
// ===========================================================================

/// xact.c `xactGetCommittedChildren`: the committed children of the current xact.
pub fn xactGetCommittedChildren() -> Vec<TransactionId> {
    with_xact_or(Vec::new(), |x| x.cur().child_xids.clone())
}

// ===========================================================================
// WAL record assembly
// ===========================================================================

/// xact.c `XactLogCommitRecord`. Assembles + inserts a commit WAL record and
/// returns its end LSN. Two-phase (twophase_xid), invalidations, dropped stats
/// and replication origin are out of foundation; we emit the plain commit body
/// plus the subxacts and relfilelocators sub-records.
#[allow(clippy::too_many_arguments)]
pub async fn XactLogCommitRecord(
    shared: &Arc<SharedState>,
    commit_time: TimestampTz,
    subxacts: &[TransactionId],
    rels: &[RelFileLocator],
    _droppedstats: &[xl_xact_stats_item],
    _msgs: &[crate::storage::sinval::SharedInvalidationMessage],
    relcache_inval: bool,
    xactflags: i32,
    twophase_xid: TransactionId,
    _twophase_gid: &str,
) -> XLogRecPtr {
    use crate::backend::access::transam::xloginsert as xli;
    debug_assert!(crate::session::current().crit_section_count() > 0);
    debug_assert!(
        !twophase_xid.is_valid(),
        "two-phase commit records are deferred"
    );

    let mut xinfo = xl_xact_xinfo { xinfo: 0 };
    let mut info = XLOG_XACT_COMMIT;

    let xlrec = xl_xact_commit {
        xact_time: commit_time,
    };

    if relcache_inval {
        xinfo.xinfo |= XACT_COMPLETION_UPDATE_RELCACHE_FILE;
    }
    if with_xact(|x| x.force_sync_commit) {
        xinfo.xinfo |= XACT_COMPLETION_FORCE_SYNC_COMMIT;
    }
    if (xactflags & XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK as i32) != 0 {
        xinfo.xinfo |= XACT_XINFO_HAS_AE_LOCKS;
    }
    if !subxacts.is_empty() {
        xinfo.xinfo |= XACT_XINFO_HAS_SUBXACTS;
    }
    if !rels.is_empty() {
        xinfo.xinfo |= XACT_XINFO_HAS_RELFILELOCATORS;
        info |= XLR_SPECIAL_REL_UPDATE;
    }
    if xinfo.xinfo != 0 {
        info |= XLOG_XACT_HAS_INFO;
    }

    xli::with_insertion(async {
        xli::begin_insert();
        xli::register_data(&commit_bytes(&xlrec));
        if xinfo.xinfo != 0 {
            xli::register_data(&xinfo.xinfo.to_ne_bytes());
        }
        if xinfo.xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
            xli::register_data(&subxacts_header_bytes(subxacts.len()));
            xli::register_data(&xids_to_bytes(subxacts));
        }
        if xinfo.xinfo & XACT_XINFO_HAS_RELFILELOCATORS != 0 {
            xli::register_data(&relfilelocators_header_bytes(rels.len()));
            xli::register_data(&relfilelocators_to_bytes(rels));
        }
        xli::xlog_insert(
            shared.xlog(),
            crate::access::rmgrlist::RmgrId::Xact as u8,
            info,
        )
        .await
    })
    .await
}

/// xact.c `XactLogAbortRecord`.
#[allow(clippy::too_many_arguments)]
pub async fn XactLogAbortRecord(
    shared: &Arc<SharedState>,
    abort_time: TimestampTz,
    subxacts: &[TransactionId],
    rels: &[RelFileLocator],
    _droppedstats: &[xl_xact_stats_item],
    xactflags: i32,
    twophase_xid: TransactionId,
    _twophase_gid: &str,
) -> XLogRecPtr {
    use crate::backend::access::transam::xloginsert as xli;
    debug_assert!(crate::session::current().crit_section_count() > 0);
    debug_assert!(
        !twophase_xid.is_valid(),
        "two-phase abort records are deferred"
    );

    let mut xinfo = xl_xact_xinfo { xinfo: 0 };
    let mut info = XLOG_XACT_ABORT;
    let xlrec = xl_xact_abort {
        xact_time: abort_time,
    };

    if (xactflags & XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK as i32) != 0 {
        xinfo.xinfo |= XACT_XINFO_HAS_AE_LOCKS;
    }
    if !subxacts.is_empty() {
        xinfo.xinfo |= XACT_XINFO_HAS_SUBXACTS;
    }
    if !rels.is_empty() {
        xinfo.xinfo |= XACT_XINFO_HAS_RELFILELOCATORS;
        info |= XLR_SPECIAL_REL_UPDATE;
    }
    if xinfo.xinfo != 0 {
        info |= XLOG_XACT_HAS_INFO;
    }

    xli::with_insertion(async {
        xli::begin_insert();
        xli::register_data(&abort_bytes(&xlrec));
        if xinfo.xinfo != 0 {
            xli::register_data(&xinfo_bytes(&xinfo));
        }
        if xinfo.xinfo & XACT_XINFO_HAS_SUBXACTS != 0 {
            xli::register_data(&subxacts_header_bytes(subxacts.len()));
            xli::register_data(&xids_to_bytes(subxacts));
        }
        if xinfo.xinfo & XACT_XINFO_HAS_RELFILELOCATORS != 0 {
            xli::register_data(&relfilelocators_header_bytes(rels.len()));
            xli::register_data(&relfilelocators_to_bytes(rels));
        }
        xli::xlog_insert(
            shared.xlog(),
            crate::access::rmgrlist::RmgrId::Xact as u8,
            info,
        )
        .await
    })
    .await
}

// --- on-disk byte helpers (match the C structs' #[repr(C)] layouts) ---------

fn commit_bytes(r: &xl_xact_commit) -> Vec<u8> {
    r.xact_time.to_ne_bytes().to_vec()
}
fn abort_bytes(r: &xl_xact_abort) -> Vec<u8> {
    r.xact_time.to_ne_bytes().to_vec()
}
fn xinfo_bytes(r: &xl_xact_xinfo) -> Vec<u8> {
    r.xinfo.to_ne_bytes().to_vec()
}
fn subxacts_header_bytes(n: usize) -> Vec<u8> {
    (n as i32).to_ne_bytes().to_vec()
}
fn xids_to_bytes(xids: &[TransactionId]) -> Vec<u8> {
    let mut v = Vec::with_capacity(xids.len() * 4);
    for x in xids {
        v.extend_from_slice(&x.0.to_ne_bytes());
    }
    v
}
fn relfilelocators_header_bytes(n: usize) -> Vec<u8> {
    (n as i32).to_ne_bytes().to_vec()
}
fn relfilelocators_to_bytes(rels: &[RelFileLocator]) -> Vec<u8> {
    let mut v = Vec::with_capacity(rels.len() * 12);
    for r in rels {
        v.extend_from_slice(&r.spcOid.0.to_ne_bytes());
        v.extend_from_slice(&r.dbOid.0.to_ne_bytes());
        v.extend_from_slice(&r.relNumber.0.to_ne_bytes());
    }
    v
}

// Keep the imported header consts/types referenced so the byte helpers stay in
// sync with the on-disk record layout the header documents.
const _: () = {
    let _ = MinSizeOfXactCommit_CHECK;
};
#[allow(non_upper_case_globals)]
const MinSizeOfXactCommit_CHECK: usize = crate::access::xact::MinSizeOfXactCommit
    + MinSizeOfXactAbort
    + MinSizeOfXactSubxacts
    + MinSizeOfXactRelfileLocators;
const _: () = {
    // Reference the deferred record sub-structs so their definitions stay used.
    #[allow(clippy::no_effect_underscore_binding, reason = "compile-time type reference keeps struct defs used")]
    let _f =
        |_: xl_xact_dbinfo, _: xl_xact_origin, _: xl_xact_relfilelocators, _: xl_xact_subxacts| {};
};

// ===========================================================================
// Recovery / rmgr description (out of foundation)
// ===========================================================================

/// xact.c `xact_redo`. WAL replay is recovery, out of the foundation.
pub fn xact_redo(_record: &mut crate::access::xlogreader::XLogReaderState) {
    // TODO(recovery): xact_redo_commit / xact_redo_abort / prepare / assignment.
    unimplemented!("xact_redo is recovery (out of foundation)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_state::{SharedState, SharedStateConfig};

    fn new_shared() -> Arc<SharedState> {
        let dir = std::env::temp_dir().join(format!(
            "pepperdb-xact-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        })
    }

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

    /// Wrap a test body in the full per-task scope set xact relies on.
    async fn in_all_scopes<F, Fut, T>(shared: Arc<SharedState>, f: F) -> T
    where
        F: FnOnce(Arc<SharedState>) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        use crate::backend::access::transam::xloginsert::with_insertion;
        use crate::backend::utils::time::{combocid::combocid_scope, snapmgr::snapmgr_scope};
        let sess = Arc::new(crate::session::Session::new(
            crate::miscadmin::BackendType::BACKEND,
        ));
        let owner = crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "Test");
        crate::session::scope(
            sess,
            crate::backend::utils::resowner::resowner::scope(
                owner,
                xact_scope(snapmgr_scope(combocid_scope(with_insertion(f(shared))))),
            ),
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn start_commit_cycle_advances_state() {
        let shared = new_shared();
        in_all_scopes(shared.clone(), |shared| async move {
            assert_eq!(with_xact(|x| x.cur().state), TransState::Default);
            StartTransactionCommand(&shared).await;
            assert_eq!(with_xact(|x| x.cur().state), TransState::InProgress);
            assert_eq!(with_xact(|x| x.cur().block_state), TBlockState::Started);
            // No writes -> no xid -> trivial commit.
            CommitTransactionCommand(&shared).await;
            assert_eq!(with_xact(|x| x.cur().state), TransState::Default);
            assert_eq!(with_xact(|x| x.cur().block_state), TBlockState::Default);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn command_counter_increment_bumps_id() {
        let shared = new_shared();
        in_all_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            BeginTransactionBlock();
            CommitTransactionCommand(&shared).await; // BEGIN -> INPROGRESS
            let before = GetCurrentCommandId(true); // mark used
            CommandCounterIncrement();
            let after = GetCurrentCommandId(false);
            assert_eq!(after.0, before.0 + 1);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_with_xid_flushes_wal_before_clog() {
        let shared = new_shared();
        Box::pin(in_all_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            // Force an xid + pretend WAL was written.
            let xid = GetCurrentTransactionId(&shared).await;
            assert!(xid.is_valid());
            with_xact(|x| x.xact_last_rec_end = XLogRecPtr(1)); // wrote_xlog
            // Not committed yet.
            assert!(
                !transaction_id_did_commit(
                    shared.clog(),
                    shared.subtrans(),
                    xid,
                    INVALID_TRANSACTION_ID
                )
                .await
            );

            CommitTransactionCommand(&shared).await;

            // After a SYNC commit: clog shows COMMITTED *and* the WAL flushed LSN
            // covers the commit record. The sync branch awaits xlog_flush(recptr)
            // BEFORE transaction_id_commit_tree, so the only way clog can report
            // COMMITTED is if the flush already reached >= the commit record end.
            assert!(
                transaction_id_did_commit(
                    shared.clog(),
                    shared.subtrans(),
                    xid,
                    INVALID_TRANSACTION_ID
                )
                .await
            );
            // The real commit record's end LSN (set inside RecordTransactionCommit)
            // is remembered as xact_last_commit_end.
            let commit_recptr = with_xact(|x| x.xact_last_commit_end);
            let flushed = shared.xlog().get_flush_rec_ptr();
            assert!(
                !commit_recptr.is_invalid() && flushed.0 >= commit_recptr.0,
                "WAL flushed ({}) must cover the commit record ({}) before clog was set",
                flushed.0,
                commit_recptr.0
            );
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn savepoint_define_release_roundtrip() {
        let shared = new_shared();
        in_all_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            BeginTransactionBlock();
            CommitTransactionCommand(&shared).await; // INPROGRESS

            assert_eq!(GetCurrentTransactionNestLevel(), 1);
            DefineSavepoint(Some("sp1"));
            CommitTransactionCommand(&shared).await; // SUBBEGIN -> SUBINPROGRESS
            assert_eq!(GetCurrentTransactionNestLevel(), 2);
            assert!(IsSubTransaction());

            ReleaseSavepoint("sp1");
            CommitTransactionCommand(&shared).await; // SUBRELEASE -> pop
            assert_eq!(GetCurrentTransactionNestLevel(), 1);
            assert!(!IsSubTransaction());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn savepoint_rollback_roundtrip() {
        let shared = new_shared();
        in_all_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            BeginTransactionBlock();
            CommitTransactionCommand(&shared).await;

            DefineSavepoint(Some("sp1"));
            CommitTransactionCommand(&shared).await;
            assert_eq!(GetCurrentTransactionNestLevel(), 2);

            RollbackToSavepoint("sp1");
            // SUBRESTART: abort + cleanup + redefine + restart the subxact.
            CommitTransactionCommand(&shared).await;
            assert_eq!(GetCurrentTransactionNestLevel(), 2);
            assert!(IsSubTransaction());
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn abort_current_transaction_from_error() {
        let shared = new_shared();
        in_all_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            BeginTransactionBlock();
            CommitTransactionCommand(&shared).await; // INPROGRESS
            // Simulate an error: AbortCurrentTransaction puts us in TBLOCK_ABORT.
            AbortCurrentTransaction(&shared).await;
            assert_eq!(with_xact(|x| x.cur().block_state), TBlockState::Abort);
            assert_eq!(with_xact(|x| x.cur().state), TransState::Abort);
            // ROLLBACK -> cleanup -> idle.
            UserAbortTransactionBlock(false);
            CommitTransactionCommand(&shared).await;
            assert_eq!(with_xact(|x| x.cur().state), TransState::Default);
            assert_eq!(with_xact(|x| x.cur().block_state), TBlockState::Default);
        })
        .await;
    }
}
