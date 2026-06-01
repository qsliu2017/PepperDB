//! Implementation of subscription statistics.
//!
//! Faithful translation of `pgstat_subscription.c`. It is kept separate from
//! `pgstat.rs` to enforce the line between the statistics access/storage
//! implementation and the details of individual statistics types.
//!
//! Deviations from upstream PostgreSQL 18.3 are inherited from `pgstat.rs`'s
//! variable-kind machinery: the dshash table is replaced by a process-local
//! entry table, locks are no-ops, and `pgstat_fetch_entry` returns the live
//! shared pointer rather than a snapshot copy. The control flow of every
//! function below mirrors the C original exactly.
//!
//! IDENTIFICATION
//!   src/backend/utils/activity/pgstat_subscription.c

use crate::prelude::*;

use crate::utils::activity::pgstat::{
    pgstat_create_transactional, pgstat_drop_transactional, pgstat_fetch_entry,
    pgstat_get_entry_ref, pgstat_lock_entry, pgstat_prep_pending_entry, pgstat_reset_entry,
    pgstat_unlock_entry, PgStatShared_Common, PgStatShared_Subscription, PgStat_BackendSubEntry,
    PgStat_EntryRef, PgStat_StatSubEntry, TimestampTz, CONFLICT_NUM_TYPES,
    PGSTAT_KIND_SUBSCRIPTION,
};

/// Logical-replication conflict type (nodes/parsenodes.h: ConflictType). The C
/// enum has 7 values; defined locally as a `c_int`.
pub type ConflictType = c_int;

pub const CT_INSERT_EXISTS: ConflictType = 0;
pub const CT_UPDATE_ORIGIN_DIFFERS: ConflictType = 1;
pub const CT_UPDATE_EXISTS: ConflictType = 2;
pub const CT_UPDATE_MISSING: ConflictType = 3;
pub const CT_DELETE_ORIGIN_DIFFERS: ConflictType = 4;
pub const CT_DELETE_MISSING: ConflictType = 5;
pub const CT_MULTIPLE_UNIQUE_CONFLICTS: ConflictType = 6;

/// Report a subscription error.
pub unsafe fn pgstat_report_subscription_error(subid: Oid, is_apply_error: bool) {
    let entry_ref: *mut PgStat_EntryRef =
        pgstat_prep_pending_entry(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid, null_mut());
    let pending = (*entry_ref).pending as *mut PgStat_BackendSubEntry;

    if is_apply_error {
        (*pending).apply_error_count += 1;
    } else {
        (*pending).sync_error_count += 1;
    }
}

/// Report a subscription conflict.
pub unsafe fn pgstat_report_subscription_conflict(subid: Oid, type_: ConflictType) {
    let entry_ref: *mut PgStat_EntryRef =
        pgstat_prep_pending_entry(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid, null_mut());
    let pending = (*entry_ref).pending as *mut PgStat_BackendSubEntry;
    (*pending).conflict_count[type_ as usize] += 1;
}

/// Report creating the subscription.
pub unsafe fn pgstat_create_subscription(subid: Oid) {
    // Ensures that stats are dropped if transaction rolls back
    pgstat_create_transactional(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid);

    // Create and initialize the subscription stats entry
    pgstat_get_entry_ref(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid, true, null_mut());
    pgstat_reset_entry(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid, 0);
}

/// Report dropping the subscription.
///
/// Ensures that stats are dropped if transaction commits.
pub unsafe fn pgstat_drop_subscription(subid: Oid) {
    pgstat_drop_transactional(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid);
}

/// Support function for the SQL-callable pgstat* functions. Returns the
/// collected statistics for one subscription or NULL.
pub unsafe fn pgstat_fetch_stat_subscription(subid: Oid) -> *mut PgStat_StatSubEntry {
    // pgstat_fetch_entry yields the shared blob, which begins with a
    // PgStatShared_Common header; the stats live in its `.stats` field (upstream
    // pgstat_fetch_entry returns the post-header stats pointer directly).
    let sh = pgstat_fetch_entry(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid)
        as *mut PgStatShared_Subscription;
    if sh.is_null() {
        return null_mut();
    }
    &mut (*sh).stats as *mut PgStat_StatSubEntry
}

/// Flush out pending stats for the entry.
///
/// If `nowait` is true and the lock could not be immediately acquired, returns
/// false without flushing the entry. Otherwise returns true.
pub unsafe fn pgstat_subscription_flush_cb(entry_ref: *mut PgStat_EntryRef, nowait: bool) -> bool {
    let localent = (*entry_ref).pending as *mut PgStat_BackendSubEntry;
    let shsubent = (*entry_ref).shared_stats as *mut PgStatShared_Subscription;

    // localent always has non-zero content

    if !pgstat_lock_entry(entry_ref, nowait) {
        return false;
    }

    // SUB_ACC(fld) => shsubent->stats.fld += localent->fld
    (*shsubent).stats.apply_error_count += (*localent).apply_error_count;
    (*shsubent).stats.sync_error_count += (*localent).sync_error_count;
    for i in 0..CONFLICT_NUM_TYPES {
        (*shsubent).stats.conflict_count[i] += (*localent).conflict_count[i];
    }

    pgstat_unlock_entry(entry_ref);
    true
}

pub unsafe fn pgstat_subscription_reset_timestamp_cb(
    header: *mut PgStatShared_Common,
    ts: TimestampTz,
) {
    (*(header as *mut PgStatShared_Subscription))
        .stats
        .stat_reset_timestamp = ts;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // The variable-kind entry table in pgstat.rs is a process-global static, so
    // tests that touch it must be serialized.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn report_error_flushes_to_shared() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            let subid: Oid = 42;

            // Bumping the pending apply_error_count via the public reporter.
            pgstat_report_subscription_error(subid, true);

            // Grab the same entry_ref to drive the flush callback.
            let entry_ref =
                pgstat_prep_pending_entry(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid, null_mut());
            let pending = (*entry_ref).pending as *mut PgStat_BackendSubEntry;
            assert_eq!((*pending).apply_error_count, 1, "pending bumped");

            // Flush moves pending into shared.
            assert!(pgstat_subscription_flush_cb(entry_ref, false));

            // Fetch the shared entry and confirm the count landed.
            let shared = pgstat_fetch_stat_subscription(subid);
            assert!(!shared.is_null(), "shared entry exists");
            assert_eq!((*shared).apply_error_count, 1, "flushed to shared");
        }
    }
}
