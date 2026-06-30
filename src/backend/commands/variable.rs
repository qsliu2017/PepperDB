//! PG `src/backend/commands/variable.c` -- the GUC check/assign/show hooks for
//! the "special" configuration variables.
//!
//! Redesigned hook shape (rules.md s10): the C hooks take `(T *newval, void
//! **extra, GucSource)` and return bool; here check hooks are `fn(&mut T,
//! GucSource) -> bool` (the opaque `extra` out-param is dropped -- it carried a
//! malloc'd parse result the assign hook consumed, which the staged subsystems
//! will recompute when they land). Hooks for variables whose backing subsystem
//! is not yet reachable (DateStyle parsing, the timezone database,
//! client_encoding conversion lookup, search_path schema validation) accept the
//! value after light validation, with a STAGED note; the transaction-control
//! hooks are translated faithfully against the real per-task xact state.

use crate::access::xact::{
    XACT_READ_COMMITTED, XACT_READ_UNCOMMITTED, XACT_REPEATABLE_READ, XACT_SERIALIZABLE,
};
use crate::backend::access::transam::xact::{
    xact_iso_level, xact_read_only, IsSubTransaction, IsTransactionState,
};
use crate::backend::utils::time::snapmgr::first_snapshot_set as FirstSnapshotSet;
use crate::utils::guc::GucSource;

// ===========================================================================
// transaction characteristics (faithful)
// ===========================================================================

/// PG `check_transaction_read_only`: forbid going read-write inside a read-only
/// transaction once it is under way. Recovery (`RecoveryInProgress`) is staged.
pub fn check_transaction_read_only(newval: &mut bool, _source: GucSource) -> bool {
    if !*newval && xact_read_only() && IsTransactionState() {
        if IsSubTransaction() {
            return false; // cannot set read-write inside a read-only transaction
        }
        if FirstSnapshotSet() {
            return false; // must be set before any query
        }
    }
    true
}

/// PG `check_transaction_isolation`: the level may only change in a top-level
/// transaction that has not yet taken a snapshot. The hot-standby serializable
/// rejection (`RecoveryInProgress`) is staged.
pub fn check_transaction_isolation(newval: &mut i32, _source: GucSource) -> bool {
    let new_level = *newval;
    if new_level != xact_iso_level() && IsTransactionState() {
        if FirstSnapshotSet() {
            return false; // must be called before any query
        }
        if IsSubTransaction() {
            return false; // must not be called in a subtransaction
        }
    }
    matches!(
        new_level,
        XACT_READ_UNCOMMITTED | XACT_READ_COMMITTED | XACT_REPEATABLE_READ | XACT_SERIALIZABLE
    )
}

/// PG `check_transaction_deferrable`: only settable in a top-level transaction
/// before the first snapshot.
pub fn check_transaction_deferrable(_newval: &mut bool, _source: GucSource) -> bool {
    if IsSubTransaction() {
        return false; // cannot be called within a subtransaction
    }
    if FirstSnapshotSet() {
        return false; // must be called before any query
    }
    true
}

// ===========================================================================
// locale / namespace hooks (validation present, application STAGED)
// ===========================================================================

/// PG `check_datestyle` (namespace `variable.c`): validate the comma-separated
/// style/order tokens. STAGED: applying the parsed style/order to the DateStyle/
/// DateOrder globals (the datetime subsystem is not yet reachable); we accept any
/// recognized token combination.
pub fn check_datestyle(newval: &mut Option<String>, _source: GucSource) -> bool {
    let Some(s) = newval.as_deref() else {
        return true;
    };
    s.split(',').all(|tok| {
        let t = tok.trim();
        t.is_empty()
            || ["iso", "sql", "postgres", "german", "ymd", "mdy", "dmy", "default"]
                .iter()
                .any(|k| t.eq_ignore_ascii_case(k))
    })
}

/// PG `check_timezone`: STAGED -- the IANA timezone database lookup
/// (`pg_tzset`) is not yet translated, so any non-empty zone name is accepted.
pub fn check_timezone(newval: &mut Option<String>, _source: GucSource) -> bool {
    newval.as_deref().is_none_or(|s| !s.trim().is_empty())
}

/// PG `check_client_encoding`: STAGED -- `pg_valid_client_encoding` /
/// `PrepareClientEncoding` are still stubs, so the name is accepted as-is rather
/// than canonicalized.
pub fn check_client_encoding(newval: &mut Option<String>, _source: GucSource) -> bool {
    newval.as_deref().is_none_or(|s| !s.trim().is_empty())
}

/// PG `check_search_path` (catalog `namespace.c`): STAGED -- schema-list
/// splitting + per-schema validation lands with the namespace subsystem; the
/// raw list is accepted here.
pub fn check_search_path(_newval: &mut Option<String>, _source: GucSource) -> bool {
    true
}

/// PG `assign_search_path`: STAGED -- recomputing the active search path
/// (`baseSearchPathValid = false`) lands with the namespace subsystem.
pub fn assign_search_path(_newval: &crate::backend::utils::misc::guc::GucVal) {}
