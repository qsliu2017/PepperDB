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

// ---- TransactionId methods ----
//
// NOTE: `<`/`Ord` on TransactionId is RAW numeric ordering (xid sort, BTreeMap
// keys), NOT transaction order. Transaction order is MODULAR with permanent-xid
// special-casing -- use `.precedes()`/`.follows()`, which are non-transitive and
// deliberately not exposed as `Ord`.
impl TransactionId {
    /// Logically < `other` (modulo-2^32 for normal xids; plain unsigned for
    /// permanent xids). transam.c TransactionIdPrecedes.
    #[inline]
    pub fn precedes(self, other: Self) -> bool {
        if !self.is_normal() || !other.is_normal() {
            return self.0 < other.0;
        }
        (self.0.wrapping_sub(other.0) as i32) < 0
    }

    #[inline]
    pub fn precedes_or_equals(self, other: Self) -> bool {
        if !self.is_normal() || !other.is_normal() {
            return self.0 <= other.0;
        }
        (self.0.wrapping_sub(other.0) as i32) <= 0
    }

    #[inline]
    pub fn follows(self, other: Self) -> bool {
        if !self.is_normal() || !other.is_normal() {
            return self.0 > other.0;
        }
        (self.0.wrapping_sub(other.0) as i32) > 0
    }

    #[inline]
    pub fn follows_or_equals(self, other: Self) -> bool {
        if !self.is_normal() || !other.is_normal() {
            return self.0 >= other.0;
        }
        (self.0.wrapping_sub(other.0) as i32) >= 0
    }

    #[inline]
    pub const fn is_valid(self) -> bool {
        self.0 != INVALID_TRANSACTION_ID.0
    }

    #[inline]
    pub const fn is_normal(self) -> bool {
        self.0 >= FIRST_NORMAL_TRANSACTION_ID.0
    }

    /// Advance, handling wraparound (skip the special XIDs).
    #[inline]
    pub fn advance(&mut self) {
        self.0 = self.0.wrapping_add(1);
        if self.0 < FIRST_NORMAL_TRANSACTION_ID.0 {
            *self = FIRST_NORMAL_TRANSACTION_ID;
        }
    }

    /// Back up, handling wraparound.
    #[inline]
    pub fn retreat(&mut self) {
        loop {
            self.0 = self.0.wrapping_sub(1);
            if self.0 >= FIRST_NORMAL_TRANSACTION_ID.0 {
                break;
            }
        }
    }
}

// ---- transaction ID manipulation (deprecated C-named / free-fn shims) ----
// (compare/arith on `.0` so these stay `const fn`; the newtype's derived
// PartialEq/Ord are not const.)
#[deprecated(note = "use xid.is_valid()")]
#[inline]
pub const fn transaction_id_is_valid(xid: TransactionId) -> bool {
    xid.0 != INVALID_TRANSACTION_ID.0
}

#[deprecated(note = "use xid.is_normal()")]
#[inline]
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

// FullTransactionId is monotonic (no wraparound in practice), so `.value` order is
// a true total order -- `<`/`>` are correct. These C-named shims delegate to it.
#[deprecated(note = "use a < b")]
#[inline]
pub const fn full_transaction_id_precedes(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value < b.value
}

#[deprecated(note = "use a <= b")]
#[inline]
pub const fn full_transaction_id_precedes_or_equals(
    a: FullTransactionId,
    b: FullTransactionId,
) -> bool {
    a.value <= b.value
}

#[deprecated(note = "use a > b")]
#[inline]
pub const fn full_transaction_id_follows(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value > b.value
}

#[deprecated(note = "use a >= b")]
#[inline]
pub const fn full_transaction_id_follows_or_equals(
    a: FullTransactionId,
    b: FullTransactionId,
) -> bool {
    a.value >= b.value
}

pub const fn full_transaction_id_is_valid(x: FullTransactionId) -> bool {
    xid_from_full_transaction_id(x).is_valid()
}

pub const INVALID_FULL_TRANSACTION_ID: FullTransactionId =
    full_transaction_id_from_epoch_and_xid(0, INVALID_TRANSACTION_ID);
pub const FIRST_NORMAL_FULL_TRANSACTION_ID: FullTransactionId =
    full_transaction_id_from_epoch_and_xid(0, FIRST_NORMAL_TRANSACTION_ID);

pub const fn full_transaction_id_is_normal(x: FullTransactionId) -> bool {
    x.value >= FIRST_NORMAL_FULL_TRANSACTION_ID.value
}

/// A 64-bit value containing an epoch and a TransactionId. Wrapped in a struct to
/// prevent implicit conversion to/from TransactionId.
///
/// Monotonic (no wraparound in practice), so `<`/`<=`/`>`/`>=` give true transaction
/// order -- unlike `TransactionId`, whose order is modular (use `.precedes()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
#[deprecated(note = "use xid.advance()")]
#[inline]
pub fn transaction_id_advance(dest: &mut TransactionId) {
    dest.advance();
}

/// Retreat a FullTransactionId, stepping over xids that look special as 32-bit.
pub fn full_transaction_id_retreat(dest: &mut FullTransactionId) {
    dest.value -= 1;

    if *dest < FIRST_NORMAL_FULL_TRANSACTION_ID {
        return;
    }

    while xid_from_full_transaction_id(*dest).0 < FIRST_NORMAL_TRANSACTION_ID.0 {
        dest.value -= 1;
    }
}

/// Advance a FullTransactionId, stepping over xids that look special as 32-bit.
pub fn full_transaction_id_advance(dest: &mut FullTransactionId) {
    dest.value += 1;

    if *dest < FIRST_NORMAL_FULL_TRANSACTION_ID {
        return;
    }

    while xid_from_full_transaction_id(*dest).0 < FIRST_NORMAL_TRANSACTION_ID.0 {
        dest.value += 1;
    }
}

/// Back up a transaction ID, handling wraparound.
#[deprecated(note = "use xid.retreat()")]
#[inline]
pub fn transaction_id_retreat(dest: &mut TransactionId) {
    dest.retreat();
}

/// Compare two XIDs already known to be normal.
pub fn normal_transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool {
    debug_assert!(id1.is_normal() && id2.is_normal());
    (id1.0.wrapping_sub(id2.0) as i32) < 0
}

/// Compare two XIDs already known to be normal.
pub fn normal_transaction_id_follows(id1: TransactionId, id2: TransactionId) -> bool {
    debug_assert!(id1.is_normal() && id2.is_normal());
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
// The header declares; the backend modules define (rules s2). The pure-
// arithmetic comparators stay sync and are re-exported under their C names.
//
// The xid commit-status helpers + tree setters now take the narrow subsystem
// handles they need (`&SlruCtl` for clog/subtrans) rather than `&SharedState`,
// and the OID/XID generators are inherent methods on `VariableCache` (R-A). Both
// are reached through the owning type, so they are NOT re-exported under C names
// here -- callers go via `shared.clog()` / `shared.variable_cache().method()`.
// `VariableCache` itself is re-exported as the canonical type name.

// transam/transam.c -- TransactionIdLatest + the VariableCache type.
pub use crate::backend::access::transam::transam::{
    VariableCache, transaction_id_latest as TransactionIdLatest,
};

// Deprecated C-named modulo-2^32 comparators; delegate to the methods.
#[deprecated(note = "use a.precedes(b)")]
#[inline]
pub fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    id1.precedes(id2)
}

#[deprecated(note = "use a.precedes_or_equals(b)")]
#[inline]
pub fn TransactionIdPrecedesOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    id1.precedes_or_equals(id2)
}

#[deprecated(note = "use a.follows(b)")]
#[inline]
pub fn TransactionIdFollows(id1: TransactionId, id2: TransactionId) -> bool {
    id1.follows(id2)
}

#[deprecated(note = "use a.follows_or_equals(b)")]
#[inline]
pub fn TransactionIdFollowsOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    id1.follows_or_equals(id2)
}

// transam/varsup.c -- VarsupShmemSize estimate (free fn, no VariableCache).
pub use crate::backend::access::transam::varsup::varsup_shmem_size as VarsupShmemSize;

/// in transam/xact.c (step 14d): is the current xact a recovery replay xact?
pub fn TransactionStartedDuringRecovery() -> bool {
    // No recovery driver in the foundation; backends are never replay xacts.
    false
}

/// No-op outside assert builds; full check is a debug helper.
pub fn AssertTransactionIdInAllowableRange(_xid: TransactionId) {}

// ---- inline functions (translated in full; FRONTEND-only guard dropped) ----

/// XID part of the next transaction ID. Reads nextXid via the VariableCache.
pub fn ReadNextTransactionId(
    shared: &std::sync::Arc<crate::shared_state::SharedState>,
) -> TransactionId {
    xid_from_full_transaction_id(shared.variable_cache().read_next_full_transaction_id())
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
    if !a.is_valid() {
        return b;
    }
    if !b.is_valid() {
        return a;
    }
    if a.precedes(b) { a } else { b }
}

/// Return the older of two IDs, assuming both are normal.
pub fn normal_transaction_id_older(a: TransactionId, b: TransactionId) -> TransactionId {
    debug_assert!(a.is_normal());
    debug_assert!(b.is_normal());
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
    if a > b { a } else { b }
}

/// Compute the FullTransactionId for `xid`, assuming it was between
/// [oldestXid, nextXid] when nextXid was `next_full_xid`.
pub fn full_transaction_id_from_allowable_at(
    next_full_xid: FullTransactionId,
    xid: TransactionId,
) -> FullTransactionId {
    if !xid.is_normal() {
        return full_transaction_id_from_epoch_and_xid(0, xid);
    }

    debug_assert!(xid.precedes_or_equals(xid_from_full_transaction_id(next_full_xid)));

    let mut epoch = epoch_from_full_transaction_id(next_full_xid);
    if xid > xid_from_full_transaction_id(next_full_xid) {
        debug_assert!(epoch != 0);
        epoch -= 1;
    }

    full_transaction_id_from_epoch_and_xid(epoch, xid)
}
