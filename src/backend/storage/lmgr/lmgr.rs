//! POSTGRES high-level lock manager. Translated from backend/storage/lmgr/lmgr.c.
//!
//! This is the object-oriented layer over the heavyweight lock manager: a family
//! of thin wrappers that lock and unlock named database objects - relations,
//! pages, tuples, transactions, databases, and general catalog objects. Each
//! wrapper builds the appropriate `LOCKTAG` from the object's identity and then
//! defers to the underlying `LockAcquire`/`LockAcquireExtended`/`LockRelease`
//! routines. It also provides the routines that wait on another transaction to
//! finish (`XactLockTableWait`), the speculative-insertion locks used to
//! interlock concurrent unique inserts, the `WaitForLockers` helpers used by
//! CREATE/REINDEX INDEX CONCURRENTLY, and `DescribeLockTag`/`GetLockNameFromTagType`,
//! which render a lock tag for deadlock and error reports.
//!
//! In PepperDB the lock manager keeps the same shared lock table, so the
//! `LockAcquire` family resolves it internally and these wrappers remain plain
//! free functions. Acquisition can block, so any wrapper that may reach an
//! acquire is `async` and awaits it; the pure tag builders, the `Unlock*`
//! release paths, and the lock-description routines stay synchronous. The
//! per-backend speculative-insertion token counter, a static global in
//! PostgreSQL, is a per-task cell here so concurrent backends never share it.
//!
//! `XactLockTableWait`, `ConditionalXactLockTableWait`, and the `WaitForLockers`
//! routines must re-check whether a transaction is still running, which consults
//! the procarray, commit log, and subtransaction map; these take an explicit
//! reference to the shared state rather than reaching a process global. The
//! transaction-wait error-context message, which PostgreSQL pushes as an error
//! callback, is instead captured eagerly into an owned string in the synchronous
//! prologue so the relation reference never crosses an await point. Invalidation
//! processing, relcache integration, and the CREATE INDEX CONCURRENTLY progress
//! reporting currently call into not-yet-implemented subsystems.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::sync::Arc;

use crate::access::transam::INVALID_TRANSACTION_ID;
use crate::c::TransactionId;
use crate::catalog::catalog::IsSharedRelation;
use crate::commands::progress::ProgressWaitfor;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::lmgr::XLTW_Oper;
use crate::storage::lock::{
    LOCKMODE, LOCKTAG, LockAcquireResult, LockTagType, VirtualTransactionId,
};
use crate::storage::lockdefs::LockMode;
use crate::utils::inval::AcceptInvalidationMessages;
use crate::utils::rel::{LockRelId, RelationData};

use super::lock::{
    GetLockConflicts, LockAcquire, LockAcquireExtended, LockHasWaiters, LockHeldByMe, LockRelease,
    LockWaiterCount, MarkLockClear, VirtualXactLock,
};

// Per-backend counter for generating speculative-insertion tokens (PG
// `speculativeInsertionToken`). Per-task because two backends inserting
// speculatively must not share the counter; held only briefly so a tokio
// `task_local` cell is the right home (rules s6.1).
tokio::task_local! {
    static SPECULATIVE_INSERTION_TOKEN: std::cell::Cell<u32>;
}

/// Run `f` with the per-task speculative-insertion token cell. Falls back to a
/// fresh cell outside a backend scope (the wrappers are only ever called from a
/// backend, but the SLRU/test harness may not establish the scope).
fn with_spec_token<R>(f: impl FnOnce(&std::cell::Cell<u32>) -> R) -> R {
    let mut held = Some(f);
    // map_or_else would move `held` into both closures; the match runs only one.
    #[allow(clippy::option_if_let_else, reason = "both arms move-take the same `held`")]
    match SPECULATIVE_INSERTION_TOKEN.try_with(|c| (held.take().unwrap())(c)) {
        Ok(r) => r,
        // Outside a backend scope: a fresh cell (the wrappers are always called
        // from a backend, so this is only defensive).
        Err(_) => (held.take().unwrap())(&std::cell::Cell::new(0)),
    }
}

/// Scope a backend body so `SpeculativeInsertion*` have a token cell. Composes
/// with `local_lock_scope`/`my_proc_scope`.
pub async fn speculative_token_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    SPECULATIVE_INSERTION_TOKEN
        .scope(std::cell::Cell::new(0), f)
        .await
}

/// PG `MyDatabaseId`: the connected database OID (session-local).
fn my_database_id() -> Oid {
    crate::session::try_current()
        .map_or(InvalidOid, |s| s.database_id())
}

// ---------------------------------------------------------------------------
// Relation lock info + locktag builders
// ---------------------------------------------------------------------------

/// PG `RelationInitLockInfo`: initialize a reldesc's lock info. relcache.c calls
/// this when creating a reldesc.
pub fn RelationInitLockInfo(relation: &mut RelationData) {
    let relisshared = relation.form().relisshared;
    relation.rd_lockInfo.lockRelId.relId = relation.rd_id;
    relation.rd_lockInfo.lockRelId.dbId = if relisshared {
        InvalidOid
    } else {
        my_database_id()
    };
}

/// PG `SetLocktagRelationOid`: locktag for a relation given only its OID.
fn set_locktag_relation_oid(relid: Oid) -> LOCKTAG {
    let dbid = if IsSharedRelation(relid) {
        InvalidOid
    } else {
        my_database_id()
    };
    LOCKTAG::set_relation(dbid.0, relid.0)
}

/// The lockRelId (dbId, relId) carried inside a Relation.
fn rel_lock_id(relation: &RelationData) -> LockRelId {
    relation.rd_lockInfo.lockRelId
}

fn rel_locktag(relation: &RelationData) -> LOCKTAG {
    let id = rel_lock_id(relation);
    LOCKTAG::set_relation(id.dbId.0, id.relId.0)
}

// ---------------------------------------------------------------------------
// Relation locks
// ---------------------------------------------------------------------------

/// PG `LockRelationOid`: lock a relation given only its OID. Generally used
/// before opening the relation's relcache entry.
pub async fn LockRelationOid(relid: Oid, lockmode: LOCKMODE) {
    let tag = set_locktag_relation_oid(relid);
    let (res, locallock) =
        LockAcquireExtended(&tag, lockmode, false, false, true, false).await;
    accept_inval_after_acquire(res, locallock);
}

/// PG `ConditionalLockRelationOid`: lock only if obtainable without blocking.
pub async fn ConditionalLockRelationOid(relid: Oid, lockmode: LOCKMODE) -> bool {
    let tag = set_locktag_relation_oid(relid);
    let (res, locallock) = LockAcquireExtended(&tag, lockmode, false, true, true, false).await;
    if res == LockAcquireResult::NotAvail {
        return false;
    }
    accept_inval_after_acquire(res, locallock);
    true
}

/// PG `LockRelationId`: lock given a LockRelId.
pub async fn LockRelationId(relid: &LockRelId, lockmode: LOCKMODE) {
    let tag = LOCKTAG::set_relation(relid.dbId.0, relid.relId.0);
    let (res, locallock) =
        LockAcquireExtended(&tag, lockmode, false, false, true, false).await;
    accept_inval_after_acquire(res, locallock);
}

/// PG `UnlockRelationId`: preferred over UnlockRelationOid for speed.
pub fn UnlockRelationId(relid: &LockRelId, lockmode: LOCKMODE) {
    let tag = LOCKTAG::set_relation(relid.dbId.0, relid.relId.0);
    LockRelease(&tag, lockmode, false);
}

/// PG `UnlockRelationOid`: unlock given only a relation OID.
pub fn UnlockRelationOid(relid: Oid, lockmode: LOCKMODE) {
    let tag = set_locktag_relation_oid(relid);
    LockRelease(&tag, lockmode, false);
}

/// PG `LockRelation`: an additional lock on an already-open relation.
///
/// Sync wrapper: derive the Send `LOCKTAG` from the `!Send` raw `Relation` here
/// so it never enters the returned future (same shape as `XactLockTableWait`).
pub fn LockRelation(
    relation: &RelationData,
    lockmode: LOCKMODE,
) -> impl std::future::Future<Output = ()> + Send {
    let tag = rel_locktag(relation);
    async move {
        let (res, locallock) =
            LockAcquireExtended(&tag, lockmode, false, false, true, false).await;
        accept_inval_after_acquire(res, locallock);
    }
}

/// PG `ConditionalLockRelation`.
pub fn ConditionalLockRelation(
    relation: &RelationData,
    lockmode: LOCKMODE,
) -> impl std::future::Future<Output = bool> + Send {
    let tag = rel_locktag(relation);
    async move {
        let (res, locallock) = LockAcquireExtended(&tag, lockmode, false, true, true, false).await;
        if res == LockAcquireResult::NotAvail {
            return false;
        }
        accept_inval_after_acquire(res, locallock);
        true
    }
}

/// PG `UnlockRelation`.
pub fn UnlockRelation(relation: &RelationData, lockmode: LOCKMODE) {
    let tag = rel_locktag(relation);
    LockRelease(&tag, lockmode, false);
}

/// Shared epilogue of the LockRelation* family: absorb inval messages unless the
/// lock was already clear, then MarkLockClear. `locallock` is the sentinel
/// pointer into the per-task LOCALLOCK table (15b). Lands on the sinval stub.
fn accept_inval_after_acquire(res: LockAcquireResult, locallock: Option<*mut LOCALLOCK>) {
    if res != LockAcquireResult::AlreadyClear {
        AcceptInvalidationMessages();
        if let Some(ll) = locallock {
            // SAFETY: the sentinel points into this task's LOCALLOCK table, alive
            // for the duration of this synchronous epilogue (no await between the
            // acquire and here).
            MarkLockClear(unsafe { &mut *ll });
        }
    }
}

/// PG `CheckRelationLockedByMe`.
pub fn CheckRelationLockedByMe(relation: &RelationData, lockmode: LOCKMODE, orstronger: bool) -> bool {
    let tag = rel_locktag(relation);
    LockHeldByMe(&tag, lockmode, orstronger)
}

/// PG `CheckRelationOidLockedByMe`.
pub fn CheckRelationOidLockedByMe(relid: Oid, lockmode: LOCKMODE, orstronger: bool) -> bool {
    let tag = set_locktag_relation_oid(relid);
    LockHeldByMe(&tag, lockmode, orstronger)
}

/// PG `LockHasWaitersRelation`: is someone else waiting for a lock we hold?
pub fn LockHasWaitersRelation(relation: &RelationData, lockmode: LOCKMODE) -> bool {
    let tag = rel_locktag(relation);
    LockHasWaiters(&tag, lockmode, false)
}

/// PG `LockRelationIdForSession`: a session-level lock on a relation (persists
/// across transaction boundaries).
pub async fn LockRelationIdForSession(relid: &LockRelId, lockmode: LOCKMODE) {
    let tag = LOCKTAG::set_relation(relid.dbId.0, relid.relId.0);
    let _ = LockAcquire(&tag, lockmode, true, false).await;
}

/// PG `UnlockRelationIdForSession`.
pub fn UnlockRelationIdForSession(relid: &LockRelId, lockmode: LOCKMODE) {
    let tag = LOCKTAG::set_relation(relid.dbId.0, relid.relId.0);
    LockRelease(&tag, lockmode, true);
}

// ---------------------------------------------------------------------------
// Relation extension / database-frozen-ids / page / tuple locks
// ---------------------------------------------------------------------------

/// PG `LockRelationForExtension`: interlock addition of pages to a relation. The
/// caller already holds a regular lock, so no AcceptInvalidationMessages here.
pub fn LockRelationForExtension(
    relation: &RelationData,
    lockmode: LOCKMODE,
) -> impl std::future::Future<Output = ()> + Send {
    let id = rel_lock_id(relation);
    let tag = LOCKTAG::set_relation_extend(id.dbId.0, id.relId.0);
    async move {
        let _ = LockAcquire(&tag, lockmode, false, false).await;
    }
}

/// PG `ConditionalLockRelationForExtension`.
pub fn ConditionalLockRelationForExtension(
    relation: &RelationData,
    lockmode: LOCKMODE,
) -> impl std::future::Future<Output = bool> + Send {
    let id = rel_lock_id(relation);
    let tag = LOCKTAG::set_relation_extend(id.dbId.0, id.relId.0);
    async move { LockAcquire(&tag, lockmode, false, true).await != LockAcquireResult::NotAvail }
}

/// PG `RelationExtensionLockWaiterCount`.
pub fn RelationExtensionLockWaiterCount(relation: &RelationData) -> i32 {
    let id = rel_lock_id(relation);
    let tag = LOCKTAG::set_relation_extend(id.dbId.0, id.relId.0);
    LockWaiterCount(&tag)
}

/// PG `UnlockRelationForExtension`.
pub fn UnlockRelationForExtension(relation: &RelationData, lockmode: LOCKMODE) {
    let id = rel_lock_id(relation);
    let tag = LOCKTAG::set_relation_extend(id.dbId.0, id.relId.0);
    LockRelease(&tag, lockmode, false);
}

/// PG `LockDatabaseFrozenIds`: one backend per database may run
/// vac_update_datfrozenxid().
pub async fn LockDatabaseFrozenIds(lockmode: LOCKMODE) {
    let tag = LOCKTAG::set_database_frozen_ids(my_database_id().0);
    let _ = LockAcquire(&tag, lockmode, false, false).await;
}

/// PG `LockPage`: a page-level lock (used by some index AMs).
pub fn LockPage(
    relation: &RelationData,
    blkno: BlockNumber,
    lockmode: LOCKMODE,
) -> impl std::future::Future<Output = ()> + Send {
    let id = rel_lock_id(relation);
    let tag = LOCKTAG::set_page(id.dbId.0, id.relId.0, blkno);
    async move {
        let _ = LockAcquire(&tag, lockmode, false, false).await;
    }
}

/// PG `ConditionalLockPage`.
pub fn ConditionalLockPage(
    relation: &RelationData,
    blkno: BlockNumber,
    lockmode: LOCKMODE,
) -> impl std::future::Future<Output = bool> + Send {
    let id = rel_lock_id(relation);
    let tag = LOCKTAG::set_page(id.dbId.0, id.relId.0, blkno);
    async move { LockAcquire(&tag, lockmode, false, true).await != LockAcquireResult::NotAvail }
}

/// PG `UnlockPage`.
pub fn UnlockPage(relation: &RelationData, blkno: BlockNumber, lockmode: LOCKMODE) {
    let id = rel_lock_id(relation);
    let tag = LOCKTAG::set_page(id.dbId.0, id.relId.0, blkno);
    LockRelease(&tag, lockmode, false);
}

/// PG `LockTuple`: a tuple-level lock (see heap_lock_tuple before using).
pub fn LockTuple(
    relation: &RelationData,
    tid: &ItemPointerData,
    lockmode: LOCKMODE,
) -> impl std::future::Future<Output = ()> + Send {
    let id = rel_lock_id(relation);
    let tag = LOCKTAG::set_tuple(
        id.dbId.0,
        id.relId.0,
        tid.block_number(),
        tid.offset_number(),
    );
    async move {
        let _ = LockAcquire(&tag, lockmode, false, false).await;
    }
}

/// PG `ConditionalLockTuple`.
pub fn ConditionalLockTuple(
    relation: &RelationData,
    tid: &ItemPointerData,
    lockmode: LOCKMODE,
    log_lock_failure: bool,
) -> impl std::future::Future<Output = bool> + Send {
    let id = rel_lock_id(relation);
    let tag = LOCKTAG::set_tuple(
        id.dbId.0,
        id.relId.0,
        tid.block_number(),
        tid.offset_number(),
    );
    async move {
        LockAcquireExtended(&tag, lockmode, false, true, true, log_lock_failure)
            .await
            .0
            != LockAcquireResult::NotAvail
    }
}

/// PG `UnlockTuple`.
pub fn UnlockTuple(relation: &RelationData, tid: &ItemPointerData, lockmode: LOCKMODE) {
    let id = rel_lock_id(relation);
    let tag = LOCKTAG::set_tuple(
        id.dbId.0,
        id.relId.0,
        tid.block_number(),
        tid.offset_number(),
    );
    LockRelease(&tag, lockmode, false);
}

// ---------------------------------------------------------------------------
// XID locks (wait for a transaction to finish)
// ---------------------------------------------------------------------------

/// PG `XactLockTableInsert`: insert a lock showing `xid` is running. Taken as an
/// EXCLUSIVE lock (held to xact end) so other backends can wait on it.
pub async fn XactLockTableInsert(xid: TransactionId) {
    let tag = LOCKTAG::set_transaction(xid.0);
    let _ = LockAcquire(&tag, LockMode::ExclusiveLock as LOCKMODE, false, false).await;
}

/// PG `XactLockTableDelete`: drop the running-xid lock. Used only for subxids
/// (main xids release implicitly at xact end).
pub fn XactLockTableDelete(xid: TransactionId) {
    let tag = LOCKTAG::set_transaction(xid.0);
    LockRelease(&tag, LockMode::ExclusiveLock as LOCKMODE, false);
}

/// PG `XactLockTableWait`: wait for `xid` to commit or abort.
///
/// The loop mirrors lmgr.c: take + immediately release a SHARE lock on the xid's
/// LOCKTAG (the holder's EXCLUSIVE lock blocks us until its xact ends), then
/// re-check `TransactionIdIsInProgress`. A subtransaction releases its xid lock
/// when it ends, so if the xid is still in progress we climb to its topmost
/// parent and wait again (a short sleep avoids busy-spinning the corner case
/// where the xid is in ProcArray but not yet in the locktable).
pub fn XactLockTableWait<'a>(
    shared: &'a Arc<SharedState>,
    xid: TransactionId,
    rel: Option<&RelationData>,
    ctid: &ItemPointerData,
    oper: XLTW_Oper,
) -> impl std::future::Future<Output = ()> + Send + 'a {
    // PG sets an error-context callback (XactLockTableWaitErrorCb) describing the
    // tuple being waited for. The elog error-context stack is not wired for async
    // waits yet, so we eagerly capture the info into a Send `String` HERE, in this
    // synchronous wrapper, so the `!Send` raw `Relation`/ctid never enter the
    // returned future. TODO(panic): push this onto the error-context stack once it
    // is async-aware.
    let wait_ctx = xact_lock_wait_context(oper, rel, *ctid);
    xact_lock_table_wait_inner(shared, xid, wait_ctx)
}

async fn xact_lock_table_wait_inner(
    shared: &Arc<SharedState>,
    mut xid: TransactionId,
    _wait_ctx: Option<String>,
) {
    let mut first = true;
    loop {
        debug_assert!(xid.is_valid());

        let tag = LOCKTAG::set_transaction(xid.0);
        let _ = LockAcquire(&tag, LockMode::ShareLock as LOCKMODE, false, false).await;
        LockRelease(&tag, LockMode::ShareLock as LOCKMODE, false);

        if !xid_is_in_progress(shared, xid).await {
            break;
        }

        // See lmgr.c: a finished subxid drops its lock, so wait on the topmost.
        if !first {
            crate::miscadmin::check_for_interrupts();
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        first = false;
        xid = topmost_transaction(shared, xid).await;
    }
}

/// PG `ConditionalXactLockTableWait`: as above but never blocks; returns false if
/// the SHARE lock could not be taken without waiting.
pub async fn ConditionalXactLockTableWait(
    shared: &Arc<SharedState>,
    mut xid: TransactionId,
    log_lock_failure: bool,
) -> bool {
    let mut first = true;
    loop {
        debug_assert!(xid.is_valid());

        let tag = LOCKTAG::set_transaction(xid.0);
        if LockAcquireExtended(
            &tag,
            LockMode::ShareLock as LOCKMODE,
            false,
            true,
            true,
            log_lock_failure,
        )
        .await
        .0
            == LockAcquireResult::NotAvail
        {
            return false;
        }
        LockRelease(&tag, LockMode::ShareLock as LOCKMODE, false);

        if !xid_is_in_progress(shared, xid).await {
            break;
        }

        if !first {
            crate::miscadmin::check_for_interrupts();
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        first = false;
        xid = topmost_transaction(shared, xid).await;
    }
    true
}

/// PG `TransactionIdIsInProgress` via the procarray (consults clog/subtrans).
async fn xid_is_in_progress(shared: &Arc<SharedState>, xid: TransactionId) -> bool {
    shared
        .proc_array()
        .transaction_id_is_in_progress(
            shared.variable_cache(),
            shared.clog(),
            shared.subtrans(),
            xid,
        )
        .await
}

/// PG `XactLockTableWaitErrorCb`: the verbose wait error-context string. Built
/// eagerly + synchronously (the `!Send` `Relation` must not cross an `.await`).
/// Returns None when no operation is specified (PG sets no callback).
fn xact_lock_wait_context(
    oper: XLTW_Oper,
    rel: Option<&RelationData>,
    ctid: ItemPointerData,
) -> Option<String> {
    if oper == XLTW_Oper::XltwNone || !ctid.is_valid() {
        return None;
    }
    let rel = rel?;
    let relname = crate::utils::rel::relation_get_relation_name(rel);
    let blk = ctid.block_number();
    let off = ctid.offset_number();
    let msg = match oper {
        XLTW_Oper::XltwUpdate => format!("while updating tuple ({blk},{off}) in relation \"{relname}\""),
        XLTW_Oper::XltwDelete => format!("while deleting tuple ({blk},{off}) in relation \"{relname}\""),
        XLTW_Oper::XltwLock => format!("while locking tuple ({blk},{off}) in relation \"{relname}\""),
        XLTW_Oper::XltwLockUpdated => {
            format!("while locking updated version ({blk},{off}) of tuple in relation \"{relname}\"")
        }
        XLTW_Oper::XltwInsertIndex => {
            format!("while inserting index tuple ({blk},{off}) in relation \"{relname}\"")
        }
        XLTW_Oper::XltwInsertIndexUnique => {
            format!("while checking uniqueness of tuple ({blk},{off}) in relation \"{relname}\"")
        }
        XLTW_Oper::XltwFetchUpdated => {
            format!("while rechecking updated tuple ({blk},{off}) in relation \"{relname}\"")
        }
        XLTW_Oper::XltwRecheckExclusionConstr => {
            format!("while checking exclusion constraint on tuple ({blk},{off}) in relation \"{relname}\"")
        }
        XLTW_Oper::XltwNone => return None,
    };
    Some(msg)
}

/// PG `SubTransGetTopmostTransaction` bounded by the running xmin.
async fn topmost_transaction(shared: &Arc<SharedState>, xid: TransactionId) -> TransactionId {
    shared
        .subtrans()
        .sub_trans_get_topmost_transaction(
            xid,
            crate::backend::storage::ipc::procarray::transaction_xmin(),
        )
        .await
}

// ---------------------------------------------------------------------------
// Speculative insertion locks
// ---------------------------------------------------------------------------

/// PG `SpeculativeInsertionLockAcquire`: lock showing `xid` is speculatively
/// inserting a tuple. Returns the token distinguishing multiple insertions.
pub async fn SpeculativeInsertionLockAcquire(xid: TransactionId) -> u32 {
    let token = with_spec_token(|c| {
        let mut t = c.get().wrapping_add(1);
        if t == 0 {
            t = 1; // zero means "no token held"
        }
        c.set(t);
        t
    });
    let tag = LOCKTAG::set_speculative_insertion(xid.0, token);
    let _ = LockAcquire(&tag, LockMode::ExclusiveLock as LOCKMODE, false, false).await;
    token
}

/// PG `SpeculativeInsertionLockRelease`.
pub fn SpeculativeInsertionLockRelease(xid: TransactionId) {
    let token = with_spec_token(std::cell::Cell::get);
    let tag = LOCKTAG::set_speculative_insertion(xid.0, token);
    LockRelease(&tag, LockMode::ExclusiveLock as LOCKMODE, false);
}

/// PG `SpeculativeInsertionWait`: wait for the insertion to finish or abort.
pub async fn SpeculativeInsertionWait(xid: TransactionId, token: u32) {
    let tag = LOCKTAG::set_speculative_insertion(xid.0, token);
    debug_assert!(xid.is_valid());
    debug_assert!(token != 0);
    let _ = LockAcquire(&tag, LockMode::ShareLock as LOCKMODE, false, false).await;
    LockRelease(&tag, LockMode::ShareLock as LOCKMODE, false);
}

// ---------------------------------------------------------------------------
// WaitForLockers (CREATE/REINDEX INDEX CONCURRENTLY)
// ---------------------------------------------------------------------------

/// PG `WaitForLockersMultiple`: wait until no transaction holds locks conflicting
/// with the given locktags at `lockmode`. Collects the current lockers' VXIDs
/// (GetLockConflicts) and waits on each (VirtualXactLock).
pub async fn WaitForLockersMultiple(
    locktags: &[LOCKTAG],
    lockmode: LOCKMODE,
    progress: bool,
) {
    if locktags.is_empty() {
        return;
    }

    let holders: Vec<Vec<VirtualTransactionId>> = locktags
        .iter()
        .map(|locktag| GetLockConflicts(locktag, lockmode))
        .collect();
    let total: usize = holders.iter().map(Vec::len).sum();

    if progress {
        // Progress reporting lands on the backend-progress stub. TODO(progress).
        pgstat_progress_update_param_total(total as i64);
    }

    // GetLockConflicts never reports our own xid, and prepared xacts are awaited.
    let mut done = 0i64;
    for lockholders in holders {
        for vxid in lockholders {
            if progress {
                // PG publishes the holder pid here; ProcNumberGetProc is a stub.
                // TODO(progress).
            }
            VirtualXactLock(vxid, true).await;
            if progress {
                done += 1;
                pgstat_progress_update_param_done(done);
            }
        }
    }

    if progress {
        // Reset TOTAL, DONE, CURRENT_PID together (lmgr.c uses multi_param).
        crate::utils::backend_progress::pgstat_progress_update_multi_param(
            &[
                ProgressWaitfor::Total as i32,
                ProgressWaitfor::Done as i32,
                ProgressWaitfor::CurrentPid as i32,
            ],
            &[0, 0, 0],
        );
    }
}

/// PG `WaitForLockers`: single-tag convenience over WaitForLockersMultiple.
pub async fn WaitForLockers(heaplocktag: LOCKTAG, lockmode: LOCKMODE, progress: bool) {
    WaitForLockersMultiple(&[heaplocktag], lockmode, progress).await;
}

/// PROGRESS_WAITFOR_TOTAL update (lands on the backend-progress stub).
fn pgstat_progress_update_param_total(v: i64) {
    crate::utils::backend_progress::pgstat_progress_update_param(
        ProgressWaitfor::Total as i32,
        v,
    );
}
/// PROGRESS_WAITFOR_DONE update (lands on the backend-progress stub).
fn pgstat_progress_update_param_done(v: i64) {
    crate::utils::backend_progress::pgstat_progress_update_param(
        ProgressWaitfor::Done as i32,
        v,
    );
}

// ---------------------------------------------------------------------------
// Database / shared object locks
// ---------------------------------------------------------------------------

/// PG `LockDatabaseObject`: lock a general object of the current database.
pub async fn LockDatabaseObject(classid: Oid, objid: Oid, objsubid: u16, lockmode: LOCKMODE) {
    let tag = LOCKTAG::set_object(my_database_id().0, classid.0, objid.0, objsubid);
    let _ = LockAcquire(&tag, lockmode, false, false).await;
    AcceptInvalidationMessages();
}

/// PG `ConditionalLockDatabaseObject`.
pub async fn ConditionalLockDatabaseObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> bool {
    let tag = LOCKTAG::set_object(my_database_id().0, classid.0, objid.0, objsubid);
    let (res, locallock) = LockAcquireExtended(&tag, lockmode, false, true, true, false).await;
    if res == LockAcquireResult::NotAvail {
        return false;
    }
    accept_inval_after_acquire(res, locallock);
    true
}

/// PG `UnlockDatabaseObject`.
pub fn UnlockDatabaseObject(classid: Oid, objid: Oid, objsubid: u16, lockmode: LOCKMODE) {
    let tag = LOCKTAG::set_object(my_database_id().0, classid.0, objid.0, objsubid);
    LockRelease(&tag, lockmode, false);
}

/// PG `LockSharedObject`: lock a shared-across-databases object.
pub async fn LockSharedObject(classid: Oid, objid: Oid, objsubid: u16, lockmode: LOCKMODE) {
    let tag = LOCKTAG::set_object(InvalidOid.0, classid.0, objid.0, objsubid);
    let _ = LockAcquire(&tag, lockmode, false, false).await;
    AcceptInvalidationMessages();
}

/// PG `ConditionalLockSharedObject`.
pub async fn ConditionalLockSharedObject(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) -> bool {
    let tag = LOCKTAG::set_object(InvalidOid.0, classid.0, objid.0, objsubid);
    let (res, locallock) = LockAcquireExtended(&tag, lockmode, false, true, true, false).await;
    if res == LockAcquireResult::NotAvail {
        return false;
    }
    accept_inval_after_acquire(res, locallock);
    true
}

/// PG `UnlockSharedObject`.
pub fn UnlockSharedObject(classid: Oid, objid: Oid, objsubid: u16, lockmode: LOCKMODE) {
    let tag = LOCKTAG::set_object(InvalidOid.0, classid.0, objid.0, objsubid);
    LockRelease(&tag, lockmode, false);
}

/// PG `LockSharedObjectForSession`: a session-level lock on a shared object.
pub async fn LockSharedObjectForSession(
    classid: Oid,
    objid: Oid,
    objsubid: u16,
    lockmode: LOCKMODE,
) {
    let tag = LOCKTAG::set_object(InvalidOid.0, classid.0, objid.0, objsubid);
    let _ = LockAcquire(&tag, lockmode, true, false).await;
}

/// PG `UnlockSharedObjectForSession`.
pub fn UnlockSharedObjectForSession(classid: Oid, objid: Oid, objsubid: u16, lockmode: LOCKMODE) {
    let tag = LOCKTAG::set_object(InvalidOid.0, classid.0, objid.0, objsubid);
    LockRelease(&tag, lockmode, true);
}

/// PG `LockApplyTransactionForSession`: a session-level lock on a transaction
/// being applied on a logical-replication subscriber.
pub async fn LockApplyTransactionForSession(
    suboid: Oid,
    xid: TransactionId,
    objid: u16,
    lockmode: LOCKMODE,
) {
    let tag = LOCKTAG::set_apply_transaction(my_database_id().0, suboid.0, xid.0, objid);
    let _ = LockAcquire(&tag, lockmode, true, false).await;
}

/// PG `UnlockApplyTransactionForSession`.
pub fn UnlockApplyTransactionForSession(
    suboid: Oid,
    xid: TransactionId,
    objid: u16,
    lockmode: LOCKMODE,
) {
    let tag = LOCKTAG::set_apply_transaction(my_database_id().0, suboid.0, xid.0, objid);
    LockRelease(&tag, lockmode, true);
}

// ---------------------------------------------------------------------------
// DescribeLockTag / GetLockNameFromTagType (sync)
// ---------------------------------------------------------------------------

/// PG `DescribeLockTag`: append a human-readable description of a lockable object
/// to `buf` (StringInfo -> &mut String). Used for deadlock/error reports.
pub fn DescribeLockTag(buf: &mut String, tag: &LOCKTAG) {
    use std::fmt::Write;
    let f1 = tag.locktag_field1;
    let f2 = tag.locktag_field2;
    let f3 = tag.locktag_field3;
    let f4 = tag.locktag_field4;
    let ty = lock_tag_type_of(tag.locktag_type);
    let _ = match ty {
        Some(LockTagType::Relation) => write!(buf, "relation {f2} of database {f1}"),
        Some(LockTagType::RelationExtend) => {
            write!(buf, "extension of relation {f2} of database {f1}")
        }
        Some(LockTagType::DatabaseFrozenIds) => {
            write!(buf, "pg_database.datfrozenxid of database {f1}")
        }
        Some(LockTagType::Page) => write!(buf, "page {f3} of relation {f2} of database {f1}"),
        Some(LockTagType::Tuple) => {
            write!(buf, "tuple ({f3},{f4}) of relation {f2} of database {f1}")
        }
        Some(LockTagType::Transaction) => write!(buf, "transaction {f1}"),
        Some(LockTagType::VirtualTransaction) => {
            write!(buf, "virtual transaction {}/{f2}", f1 as i32)
        }
        Some(LockTagType::SpeculativeToken) => {
            write!(buf, "speculative token {f2} of transaction {f1}")
        }
        Some(LockTagType::Object) => write!(buf, "object {f3} of class {f2} of database {f1}"),
        Some(LockTagType::UserLock) => write!(buf, "user lock [{f1},{f2},{f3}]"),
        Some(LockTagType::Advisory) => write!(buf, "advisory lock [{f1},{f2},{f3},{f4}]"),
        Some(LockTagType::ApplyTransaction) => write!(
            buf,
            "remote transaction {f3} of subscription {f2} of database {f1}"
        ),
        None => write!(buf, "unrecognized locktag type {}", i32::from(tag.locktag_type)),
    };
}

fn lock_tag_type_of(t: u8) -> Option<LockTagType> {
    use LockTagType::{Relation, RelationExtend, DatabaseFrozenIds, Page, Tuple, Transaction, VirtualTransaction, SpeculativeToken, Object, UserLock, Advisory, ApplyTransaction};
    Some(match t {
        0 => Relation,
        1 => RelationExtend,
        2 => DatabaseFrozenIds,
        3 => Page,
        4 => Tuple,
        5 => Transaction,
        6 => VirtualTransaction,
        7 => SpeculativeToken,
        8 => Object,
        9 => UserLock,
        10 => Advisory,
        11 => ApplyTransaction,
        _ => return None,
    })
}

/// PG `GetLockNameFromTagType`: the lock-tag-type name for a numeric type.
pub fn GetLockNameFromTagType(locktag_type: u16) -> &'static str {
    const NAMES: [&str; 12] = [
        "relation",
        "extend",
        "frozenid",
        "page",
        "tuple",
        "transactionid",
        "virtualxid",
        "spectoken",
        "object",
        "userlock",
        "advisory",
        "applytransaction",
    ];
    NAMES.get(locktag_type as usize).copied().unwrap_or("???")
}

use crate::storage::lock::LOCALLOCK;

#[cfg(test)]
mod tests;
