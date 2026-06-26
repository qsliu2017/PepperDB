//! Translated from PostgreSQL src/include/access/transam.h

use crate::access::xlogdefs::XLogRecPtr;
use crate::c::TransactionId;
use crate::postgres_ext::Oid;

// ---- Special transaction ID values ----
pub const INVALID_TRANSACTION_ID: TransactionId = TransactionId(0);
pub const BOOTSTRAP_TRANSACTION_ID: TransactionId = TransactionId(1);
pub const FROZEN_TRANSACTION_ID: TransactionId = TransactionId(2);
pub const FIRST_NORMAL_TRANSACTION_ID: TransactionId = TransactionId(3);
pub const MAX_TRANSACTION_ID: TransactionId = TransactionId(0xFFFFFFFF);

// ---- transaction ID manipulation ----
// (compare/arith on `.0` so these stay `const fn`; the newtype's derived
// PartialEq/Ord are not const.)
pub const fn transaction_id_is_valid(xid: TransactionId) -> bool {
    xid.0 != INVALID_TRANSACTION_ID.0
}

pub const fn transaction_id_is_normal(xid: TransactionId) -> bool {
    xid.0 >= FIRST_NORMAL_TRANSACTION_ID.0
}

pub const fn transaction_id_equals(id1: TransactionId, id2: TransactionId) -> bool {
    id1.0 == id2.0
}

pub const fn epoch_from_full_transaction_id(x: FullTransactionId) -> u32 {
    (x.value >> 32) as u32
}

pub const fn xid_from_full_transaction_id(x: FullTransactionId) -> TransactionId {
    TransactionId(x.value as u32)
}

pub const fn u64_from_full_transaction_id(x: FullTransactionId) -> u64 {
    x.value
}

pub const fn full_transaction_id_equals(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value == b.value
}

pub const fn full_transaction_id_precedes(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value < b.value
}

pub const fn full_transaction_id_precedes_or_equals(
    a: FullTransactionId,
    b: FullTransactionId,
) -> bool {
    a.value <= b.value
}

pub const fn full_transaction_id_follows(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value > b.value
}

pub const fn full_transaction_id_follows_or_equals(
    a: FullTransactionId,
    b: FullTransactionId,
) -> bool {
    a.value >= b.value
}

pub const fn full_transaction_id_is_valid(x: FullTransactionId) -> bool {
    transaction_id_is_valid(xid_from_full_transaction_id(x))
}

pub const INVALID_FULL_TRANSACTION_ID: FullTransactionId =
    full_transaction_id_from_epoch_and_xid(0, INVALID_TRANSACTION_ID);
pub const FIRST_NORMAL_FULL_TRANSACTION_ID: FullTransactionId =
    full_transaction_id_from_epoch_and_xid(0, FIRST_NORMAL_TRANSACTION_ID);

pub const fn full_transaction_id_is_normal(x: FullTransactionId) -> bool {
    full_transaction_id_follows_or_equals(x, FIRST_NORMAL_FULL_TRANSACTION_ID)
}

/// A 64-bit value containing an epoch and a TransactionId. Wrapped in a struct to
/// prevent implicit conversion to/from TransactionId.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FullTransactionId {
    pub value: u64,
}

pub const fn full_transaction_id_from_epoch_and_xid(
    epoch: u32,
    xid: TransactionId,
) -> FullTransactionId {
    FullTransactionId {
        value: ((epoch as u64) << 32) | xid.0 as u64,
    }
}

pub const fn full_transaction_id_from_u64(value: u64) -> FullTransactionId {
    FullTransactionId { value }
}

/// Advance a transaction ID, handling wraparound (skip the special XIDs).
pub fn transaction_id_advance(dest: &mut TransactionId) {
    dest.0 = dest.0.wrapping_add(1);
    if dest.0 < FIRST_NORMAL_TRANSACTION_ID.0 {
        *dest = FIRST_NORMAL_TRANSACTION_ID;
    }
}

/// Retreat a FullTransactionId, stepping over xids that look special as 32-bit.
pub fn full_transaction_id_retreat(dest: &mut FullTransactionId) {
    dest.value -= 1;

    if full_transaction_id_precedes(*dest, FIRST_NORMAL_FULL_TRANSACTION_ID) {
        return;
    }

    while xid_from_full_transaction_id(*dest).0 < FIRST_NORMAL_TRANSACTION_ID.0 {
        dest.value -= 1;
    }
}

/// Advance a FullTransactionId, stepping over xids that look special as 32-bit.
pub fn full_transaction_id_advance(dest: &mut FullTransactionId) {
    dest.value += 1;

    if full_transaction_id_precedes(*dest, FIRST_NORMAL_FULL_TRANSACTION_ID) {
        return;
    }

    while xid_from_full_transaction_id(*dest).0 < FIRST_NORMAL_TRANSACTION_ID.0 {
        dest.value += 1;
    }
}

/// Back up a transaction ID, handling wraparound.
pub fn transaction_id_retreat(dest: &mut TransactionId) {
    loop {
        dest.0 = dest.0.wrapping_sub(1);
        if dest.0 >= FIRST_NORMAL_TRANSACTION_ID.0 {
            break;
        }
    }
}

/// Compare two XIDs already known to be normal.
pub fn normal_transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool {
    debug_assert!(transaction_id_is_normal(id1) && transaction_id_is_normal(id2));
    (id1.0.wrapping_sub(id2.0) as i32) < 0
}

/// Compare two XIDs already known to be normal.
pub fn normal_transaction_id_follows(id1: TransactionId, id2: TransactionId) -> bool {
    debug_assert!(transaction_id_is_normal(id1) && transaction_id_is_normal(id2));
    (id1.0.wrapping_sub(id2.0) as i32) > 0
}

// ---- OID assignment ranges ----
pub const FIRST_GENBKI_OBJECT_ID: u32 = 10000;
pub const FIRST_UNPINNED_OBJECT_ID: u32 = 12000;
pub const FIRST_NORMAL_OBJECT_ID: u32 = 16384;

/// Shared-memory OID/XID assignment state. (Under single-process this becomes
/// owned heap state guarded by locks; the per-field LWLock split is dropped.)
#[derive(Debug, Clone, Copy)]
pub struct TransamVariablesData {
    // Protected by OidGenLock.
    pub next_oid: Oid,
    pub oid_count: u32,

    // Protected by XidGenLock.
    pub next_xid: FullTransactionId,
    pub oldest_xid: TransactionId,
    pub xid_vac_limit: TransactionId,
    pub xid_warn_limit: TransactionId,
    pub xid_stop_limit: TransactionId,
    pub xid_wrap_limit: TransactionId,
    pub oldest_xid_db: Oid,

    // Protected by CommitTsLock.
    pub oldest_commit_ts_xid: TransactionId,
    pub newest_commit_ts_xid: TransactionId,

    // Protected by ProcArrayLock.
    pub latest_completed_xid: FullTransactionId,

    pub xact_completion_count: u64,

    // Protected by XactTruncationLock.
    pub oldest_clog_xid: TransactionId,
}

// ---- definitions live in transam/{transam,varsup}.c; re-exported here ----
//
// The header declares; the backend modules define (rules s2). The xid
// commit-status helpers and the xid generators became async + threaded through
// `&Arc<SharedState>` (async coloring from the SLRU leaf, design s4); the
// signatures here re-export those NEW shapes. The pure-arithmetic comparators
// stay sync.

// transam/transam.c -- the modulo-2^32 xid comparators (sync).
pub use crate::backend::access::transam::transam::{
    transaction_id_follows as TransactionIdFollows,
    transaction_id_follows_or_equals as TransactionIdFollowsOrEquals,
    transaction_id_latest as TransactionIdLatest,
    transaction_id_precedes as TransactionIdPrecedes,
    transaction_id_precedes_or_equals as TransactionIdPrecedesOrEquals,
};

// transam/transam.c -- xid commit-status access + tree setters (async).
pub use crate::backend::access::transam::transam::{
    transaction_id_abort_tree as TransactionIdAbortTree,
    transaction_id_async_commit_tree as TransactionIdAsyncCommitTree,
    transaction_id_commit_tree as TransactionIdCommitTree,
    transaction_id_did_abort as TransactionIdDidAbort,
    transaction_id_did_commit as TransactionIdDidCommit,
    transaction_id_get_commit_lsn as TransactionIdGetCommitLSN, VariableCache,
};

// transam/varsup.c -- OID/XID generation (async where it Extends clog/subtrans).
pub use crate::backend::access::transam::varsup::{
    varsup_shmem_size as VarsupShmemSize, AdvanceNextFullTransactionIdPastXid, AdvanceOldestClogXid,
    ForceTransactionIdLimitUpdate, GetNewObjectId, GetNewTransactionId, ReadNextFullTransactionId,
    SetTransactionIdLimit, StopGeneratingPinnedObjectIds,
};

/// in transam/xact.c (step 14d): is the current xact a recovery replay xact?
pub fn TransactionStartedDuringRecovery() -> bool {
    // No recovery driver in the foundation; backends are never replay xacts.
    false
}

/// No-op outside assert builds; full check is a debug helper.
pub fn AssertTransactionIdInAllowableRange(_xid: TransactionId) {}

// ---- inline functions (translated in full; FRONTEND-only guard dropped) ----

/// XID part of the next transaction ID.
pub fn ReadNextTransactionId(
    shared: &std::sync::Arc<crate::shared_state::SharedState>,
) -> TransactionId {
    xid_from_full_transaction_id(ReadNextFullTransactionId(shared))
}

/// Return a transaction ID backed up by `amount`, handling wraparound.
pub fn transaction_id_retreated_by(mut xid: TransactionId, amount: u32) -> TransactionId {
    xid.0 = xid.0.wrapping_sub(amount);
    while xid.0 < FIRST_NORMAL_TRANSACTION_ID.0 {
        xid.0 = xid.0.wrapping_sub(1);
    }
    xid
}

/// Return the older of two IDs.
pub fn transaction_id_older(a: TransactionId, b: TransactionId) -> TransactionId {
    if !transaction_id_is_valid(a) {
        return b;
    }
    if !transaction_id_is_valid(b) {
        return a;
    }
    if TransactionIdPrecedes(a, b) {
        a
    } else {
        b
    }
}

/// Return the older of two IDs, assuming both are normal.
pub fn normal_transaction_id_older(a: TransactionId, b: TransactionId) -> TransactionId {
    debug_assert!(transaction_id_is_normal(a));
    debug_assert!(transaction_id_is_normal(b));
    if normal_transaction_id_precedes(a, b) {
        a
    } else {
        b
    }
}

/// Return the newer of two full IDs.
pub fn full_transaction_id_newer(a: FullTransactionId, b: FullTransactionId) -> FullTransactionId {
    if !full_transaction_id_is_valid(a) {
        return b;
    }
    if !full_transaction_id_is_valid(b) {
        return a;
    }
    if full_transaction_id_follows(a, b) {
        a
    } else {
        b
    }
}

/// Compute the FullTransactionId for `xid`, assuming it was between
/// [oldestXid, nextXid] when nextXid was `next_full_xid`.
pub fn full_transaction_id_from_allowable_at(
    next_full_xid: FullTransactionId,
    xid: TransactionId,
) -> FullTransactionId {
    if !transaction_id_is_normal(xid) {
        return full_transaction_id_from_epoch_and_xid(0, xid);
    }

    debug_assert!(TransactionIdPrecedesOrEquals(
        xid,
        xid_from_full_transaction_id(next_full_xid)
    ));

    let mut epoch = epoch_from_full_transaction_id(next_full_xid);
    if xid > xid_from_full_transaction_id(next_full_xid) {
        debug_assert!(epoch != 0);
        epoch -= 1;
    }

    full_transaction_id_from_epoch_and_xid(epoch, xid)
}
