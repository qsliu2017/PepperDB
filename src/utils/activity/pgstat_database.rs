//! Implementation of database statistics (port of `pgstat_database.c`).
//!
//! Faithful 1:1 translation of PostgreSQL 18.3's
//! `src/backend/utils/activity/pgstat_database.c`. It publishes per-database
//! cumulative counters (xact commit/rollback, conflicts, temp files, deadlocks,
//! checksum failures, session times, parallel-worker counts, ...) on top of the
//! variable-kind entry-ref machinery in `crate::utils::activity::pgstat`.
//!
//! Deviations from upstream (each noted again inline at the stub site):
//!
//! * GLOBALS / GUCs are PROCESS-LOCAL STATICS. `MyDatabaseId`, `MyBackendType`,
//!   `MyStartTimestamp`, `pgstat_track_counts`, and the `pgStat*Time` /
//!   `pgStatSessionEndCause` accumulators are real backend globals upstream;
//!   here they are `static mut` stand-ins so this file compiles standalone.
//!
//! * GetCurrentTimestamp() / TimestampDifference() come from the pgstat core
//!   stub (returns 0) and a local micro-second subtraction respectively.
//!
//! * pgstat_get_entry_ref_locked() is not yet in the pgstat core, so a thin
//!   local wrapper (get_entry_ref + lock_entry) stands in for it.
//!
//! * The procsignal recovery-conflict reason enum is mirrored as local consts.
//!
//! * pgstat_assert_is_up()/Assert/elog are no-ops here.

use crate::prelude::*;

use crate::utils::activity::pgstat::{
    pgstat_fetch_entry, pgstat_get_entry_ref, pgstat_lock_entry, pgstat_prep_pending_entry,
    pgstat_unlock_entry, GetCurrentTimestamp, PgStatShared_Common, PgStatShared_Database,
    PgStat_Counter, PgStat_EntryRef, PgStat_StatDBEntry, TimestampTz, PGSTAT_KIND_DATABASE,
};

// We deliberately avoid the `libc` crate; pull in `memset` via a local extern.
extern "C" {
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// STUBBED globals / GUCs (real backend globals upstream)
// ---------------------------------------------------------------------------

/// STUB: `MyDatabaseId` is the OID of the database this backend is connected to
/// (miscinit.c). Process-local stand-in defaulting to InvalidOid.
pub static mut MyDatabaseId: Oid = InvalidOid;

/// STUB: `MyStartTimestamp` is the backend's start time (postmaster.c).
pub static mut MyStartTimestamp: TimestampTz = 0;

/// STUB: `MyBackendType` (miscinit.c). B_BACKEND == 1 in the BackendType enum
/// (miscadmin.h); defaulting here so `pgstat_should_report_connstat` returns
/// true and session reporting is exercised.
pub static mut MyBackendType: c_int = B_BACKEND;

/// BackendType ordinal for a regular client backend (miscadmin.h: B_BACKEND).
pub const B_BACKEND: c_int = 1;

/// STUB: `pgstat_track_counts` GUC (default on, guc_tables.c).
pub static mut pgstat_track_counts: bool = true;

// --- Session-end cause (pgstat.h: SessionEndType) -------------------------
//
// STUB: upstream `pgStatSessionEndCause` is set by the backend teardown path.
// The enum ordinals (pgstat.h): NOT_YET=0, NORMAL=1, CLIENT_EOF=2, FATAL=3,
// KILLED=4.
pub const DISCONNECT_NOT_YET: c_int = 0;
pub const DISCONNECT_NORMAL: c_int = 1;
pub const DISCONNECT_CLIENT_EOF: c_int = 2;
pub const DISCONNECT_FATAL: c_int = 3;
pub const DISCONNECT_KILLED: c_int = 4;

/// STUB: `pgStatSessionEndCause` (pgstat_database.c global).
pub static mut pgStatSessionEndCause: c_int = DISCONNECT_NORMAL;

// --- Per-backend time / xact accumulators (pgstat_database.c globals) ------

/// STUB: `pgStatBlockReadTime` (microseconds, accumulated by bufmgr).
pub static mut pgStatBlockReadTime: PgStat_Counter = 0;
/// STUB: `pgStatBlockWriteTime` (microseconds).
pub static mut pgStatBlockWriteTime: PgStat_Counter = 0;
/// STUB: `pgStatActiveTime` (microseconds).
pub static mut pgStatActiveTime: PgStat_Counter = 0;
/// STUB: `pgStatTransactionIdleTime` (microseconds).
pub static mut pgStatTransactionIdleTime: PgStat_Counter = 0;

static mut pgStatXactCommit: c_int = 0;
static mut pgStatXactRollback: c_int = 0;
static mut pgLastSessionReportTime: PgStat_Counter = 0;

// ---------------------------------------------------------------------------
// Recovery-conflict reason enum (storage/procsignal.h) mirrored as consts.
// ---------------------------------------------------------------------------
//
// NOTE: in PG 18.3 LOGICALSLOT precedes BUFFERPIN in the enum, but the switch
// bodies are keyed by name not ordinal, so order does not matter here.
pub const PROCSIG_RECOVERY_CONFLICT_DATABASE: c_int = 1;
pub const PROCSIG_RECOVERY_CONFLICT_TABLESPACE: c_int = 2;
pub const PROCSIG_RECOVERY_CONFLICT_LOCK: c_int = 3;
pub const PROCSIG_RECOVERY_CONFLICT_SNAPSHOT: c_int = 4;
pub const PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT: c_int = 5;
pub const PROCSIG_RECOVERY_CONFLICT_BUFFERPIN: c_int = 6;
pub const PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK: c_int = 7;

// ---------------------------------------------------------------------------
// Local helpers (stubbed core pieces)
// ---------------------------------------------------------------------------

/// `OidIsValid` (c.h): an Oid is valid iff it is not InvalidOid.
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// STUB for `pgstat_get_entry_ref_locked` (pgstat_internal.h): upstream fetches
/// (creating per `create`) the entry-ref then takes its lock. Locking is a
/// no-op in the process-local pgstat subset, so this is get_entry_ref + a
/// (no-op) lock_entry.
unsafe fn pgstat_get_entry_ref_locked(
    kind: u32,
    dboid: Oid,
    objoid: Oid,
    create: bool,
) -> *mut PgStat_EntryRef {
    let entry_ref = pgstat_get_entry_ref(kind, dboid, objoid, create, null_mut());
    pgstat_lock_entry(entry_ref, false);
    entry_ref
}

/// STUB for `TimestampDifference` (utils/timestamp.c): split (stop - start)
/// microseconds into whole seconds and remaining microseconds. Negative diffs
/// clamp to zero, matching upstream behavior.
unsafe fn TimestampDifference(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    secs: *mut c_long,
    microsecs: *mut c_int,
) {
    let diff = stop_time - start_time;
    if diff <= 0 {
        *secs = 0;
        *microsecs = 0;
    } else {
        *secs = (diff / 1_000_000) as c_long;
        *microsecs = (diff % 1_000_000) as c_int;
    }
}

// ---------------------------------------------------------------------------
// Database statistics reporters (pgstat_database.c)
// ---------------------------------------------------------------------------

/// Remove entry for the database being dropped.
#[no_mangle]
pub unsafe fn pgstat_drop_database(databaseid: Oid) {
    crate::utils::activity::pgstat::pgstat_drop_transactional(
        PGSTAT_KIND_DATABASE,
        databaseid,
        InvalidOid,
    );
}

/// Called from autovacuum.c to report startup of an autovacuum process.
/// We are called before InitPostgres is done, so can't rely on MyDatabaseId;
/// the db OID must be passed in, instead.
pub unsafe fn pgstat_report_autovac(dboid: Oid) {
    // Assert(IsUnderPostmaster);  -- stubbed (no-op)

    // End-of-vacuum is reported instantly; report the start the same way for
    // consistency.
    let entry_ref =
        pgstat_get_entry_ref_locked(PGSTAT_KIND_DATABASE, dboid, InvalidOid, false);

    let dbentry = (*entry_ref).shared_stats as *mut PgStatShared_Database;
    (*dbentry).stats.last_autovac_time = GetCurrentTimestamp();

    pgstat_unlock_entry(entry_ref);
}

/// Report a Hot Standby recovery conflict.
pub unsafe fn pgstat_report_recovery_conflict(reason: c_int) {
    // Assert(IsUnderPostmaster);  -- stubbed
    if !pgstat_track_counts {
        return;
    }

    let dbentry = pgstat_prep_database_pending(MyDatabaseId);

    match reason {
        PROCSIG_RECOVERY_CONFLICT_DATABASE => {
            // Since we drop the information about the database as soon as it
            // replicates, there is no point in counting these conflicts.
        }
        PROCSIG_RECOVERY_CONFLICT_TABLESPACE => {
            (*dbentry).conflict_tablespace += 1;
        }
        PROCSIG_RECOVERY_CONFLICT_LOCK => {
            (*dbentry).conflict_lock += 1;
        }
        PROCSIG_RECOVERY_CONFLICT_SNAPSHOT => {
            (*dbentry).conflict_snapshot += 1;
        }
        PROCSIG_RECOVERY_CONFLICT_BUFFERPIN => {
            (*dbentry).conflict_bufferpin += 1;
        }
        PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT => {
            (*dbentry).conflict_logicalslot += 1;
        }
        PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK => {
            (*dbentry).conflict_startup_deadlock += 1;
        }
        _ => {}
    }
}

/// Report a detected deadlock.
pub unsafe fn pgstat_report_deadlock() {
    if !pgstat_track_counts {
        return;
    }

    let dbent = pgstat_prep_database_pending(MyDatabaseId);
    (*dbent).deadlocks += 1;
}

/// Allow this backend to later report checksum failures for dboid, even if in
/// a critical section at the time of the report.
pub unsafe fn pgstat_prepare_report_checksum_failure(dboid: Oid) {
    // Assert(!CritSectionCount);  -- stubbed

    // Just need to ensure this backend has an entry ref for the database.
    pgstat_get_entry_ref(PGSTAT_KIND_DATABASE, dboid, InvalidOid, true, null_mut());
}

/// Report one or more checksum failures.
pub unsafe fn pgstat_report_checksum_failures_in_db(dboid: Oid, failurecount: c_int) {
    if !pgstat_track_counts {
        return;
    }

    // Update the shared stats directly. We pass create=false here, as we want
    // to be sure to not require memory allocations, so this can be called in
    // critical sections.
    let entry_ref =
        pgstat_get_entry_ref(PGSTAT_KIND_DATABASE, dboid, InvalidOid, false, null_mut());

    // Should always have been created by
    // pgstat_prepare_report_checksum_failure(). When not using assertions, we
    // don't want to crash should something have gone wrong, so just return.
    if entry_ref.is_null() {
        // elog(WARNING, ...) -- stubbed
        return;
    }

    pgstat_lock_entry(entry_ref, false);

    let sharedent = (*entry_ref).shared_stats as *mut PgStatShared_Database;
    (*sharedent).stats.checksum_failures += failurecount as PgStat_Counter;
    (*sharedent).stats.last_checksum_failure = GetCurrentTimestamp();

    pgstat_unlock_entry(entry_ref);
}

/// Report creation of temporary file.
pub unsafe fn pgstat_report_tempfile(filesize: usize) {
    if !pgstat_track_counts {
        return;
    }

    let dbent = pgstat_prep_database_pending(MyDatabaseId);
    (*dbent).temp_bytes += filesize as PgStat_Counter;
    (*dbent).temp_files += 1;
}

/// Notify stats system of a new connection.
pub unsafe fn pgstat_report_connect(_dboid: Oid) {
    if !pgstat_should_report_connstat() {
        return;
    }

    pgLastSessionReportTime = MyStartTimestamp;

    let dbentry = pgstat_prep_database_pending(MyDatabaseId);
    (*dbentry).sessions += 1;
}

/// Notify the stats system of a disconnect.
pub unsafe fn pgstat_report_disconnect(_dboid: Oid) {
    if !pgstat_should_report_connstat() {
        return;
    }

    let dbentry = pgstat_prep_database_pending(MyDatabaseId);

    match pgStatSessionEndCause {
        DISCONNECT_NOT_YET | DISCONNECT_NORMAL => {
            // we don't collect these
        }
        DISCONNECT_CLIENT_EOF => {
            (*dbentry).sessions_abandoned += 1;
        }
        DISCONNECT_FATAL => {
            (*dbentry).sessions_fatal += 1;
        }
        DISCONNECT_KILLED => {
            (*dbentry).sessions_killed += 1;
        }
        _ => {}
    }
}

/// Support function for the SQL-callable pgstat* functions. Returns the
/// collected statistics for one database or NULL.
///
/// HEADER-OFFSET RULE: the shared blob is a header-first PgStatShared_Database;
/// `pgstat_fetch_entry` returns the blob START, so we must cast to
/// `*mut PgStatShared_Database` and return `&mut (*sh).stats`, NOT cast the
/// blob start directly to `*mut PgStat_StatDBEntry`.
pub unsafe fn pgstat_fetch_stat_dbentry(dboid: Oid) -> *mut PgStat_StatDBEntry {
    let sh = pgstat_fetch_entry(PGSTAT_KIND_DATABASE, dboid, InvalidOid)
        as *mut PgStatShared_Database;
    if sh.is_null() {
        null_mut()
    } else {
        &mut (*sh).stats as *mut PgStat_StatDBEntry
    }
}

/// Account a transaction commit/abort at end of (sub)xact.
pub unsafe fn AtEOXact_PgStat_Database(isCommit: bool, parallel: bool) {
    // Don't count parallel worker transaction stats
    if !parallel {
        // Count transaction commit or abort. (We use counters, not just bools,
        // in case the reporting message isn't sent right away.)
        if isCommit {
            pgStatXactCommit += 1;
        } else {
            pgStatXactRollback += 1;
        }
    }
}

/// Notify the stats system about parallel worker information.
pub unsafe fn pgstat_update_parallel_workers_stats(
    workers_to_launch: PgStat_Counter,
    workers_launched: PgStat_Counter,
) {
    if !OidIsValid(MyDatabaseId) {
        return;
    }

    let dbentry = pgstat_prep_database_pending(MyDatabaseId);
    (*dbentry).parallel_workers_to_launch += workers_to_launch;
    (*dbentry).parallel_workers_launched += workers_launched;
}

/// Subroutine for pgstat_report_stat(): Handle xact commit/rollback and I/O
/// timings.
pub unsafe fn pgstat_update_dbstats(ts: TimestampTz) {
    // If not connected to a database yet, don't attribute time to "shared
    // state" (InvalidOid is used to track stats for shared relations, etc.).
    if !OidIsValid(MyDatabaseId) {
        return;
    }

    let dbentry = pgstat_prep_database_pending(MyDatabaseId);

    // Accumulate xact commit/rollback and I/O timings to stats entry of the
    // current database.
    (*dbentry).xact_commit += pgStatXactCommit as PgStat_Counter;
    (*dbentry).xact_rollback += pgStatXactRollback as PgStat_Counter;
    (*dbentry).blk_read_time += pgStatBlockReadTime;
    (*dbentry).blk_write_time += pgStatBlockWriteTime;

    if pgstat_should_report_connstat() {
        let mut secs: c_long = 0;
        let mut usecs: c_int = 0;

        // pgLastSessionReportTime is initialized to MyStartTimestamp by
        // pgstat_report_connect().
        TimestampDifference(pgLastSessionReportTime, ts, &mut secs, &mut usecs);
        pgLastSessionReportTime = ts;
        (*dbentry).session_time += (secs as PgStat_Counter) * 1000000 + usecs as PgStat_Counter;
        (*dbentry).active_time += pgStatActiveTime;
        (*dbentry).idle_in_transaction_time += pgStatTransactionIdleTime;
    }

    pgStatXactCommit = 0;
    pgStatXactRollback = 0;
    pgStatBlockReadTime = 0;
    pgStatBlockWriteTime = 0;
    pgStatActiveTime = 0;
    pgStatTransactionIdleTime = 0;
}

/// We report session statistics only for normal backend processes.
pub unsafe fn pgstat_should_report_connstat() -> bool {
    MyBackendType == B_BACKEND
}

/// Find or create a local PgStat_StatDBEntry entry for dboid.
///
/// Thin wrapper: prep the pending entry for (PGSTAT_KIND_DATABASE, dboid,
/// InvalidOid) then return its `pending` blob as a *mut PgStat_StatDBEntry.
pub unsafe fn pgstat_prep_database_pending(dboid: Oid) -> *mut PgStat_StatDBEntry {
    // Assert(!OidIsValid(dboid) || OidIsValid(MyDatabaseId));  -- stubbed

    let entry_ref =
        pgstat_prep_pending_entry(PGSTAT_KIND_DATABASE, dboid, InvalidOid, null_mut());

    (*entry_ref).pending as *mut PgStat_StatDBEntry
}

/// Reset the database's reset timestamp, without resetting the contents of the
/// database stats.
pub unsafe fn pgstat_reset_database_timestamp(_dboid: Oid, ts: TimestampTz) {
    let dbref =
        pgstat_get_entry_ref_locked(PGSTAT_KIND_DATABASE, MyDatabaseId, InvalidOid, false);

    let dbentry = (*dbref).shared_stats as *mut PgStatShared_Database;
    (*dbentry).stats.stat_reset_timestamp = ts;

    pgstat_unlock_entry(dbref);
}

/// Flush out pending stats for the entry.
///
/// If nowait is true and the lock could not be immediately acquired, returns
/// false without flushing the entry. Otherwise returns true.
pub unsafe fn pgstat_database_flush_cb(entry_ref: *mut PgStat_EntryRef, nowait: bool) -> bool {
    let pendingent = (*entry_ref).pending as *mut PgStat_StatDBEntry;
    let sharedent = (*entry_ref).shared_stats as *mut PgStatShared_Database;

    if !pgstat_lock_entry(entry_ref, nowait) {
        return false;
    }

    // PGSTAT_ACCUM_DBCOUNT(item): (sharedent)->stats.item += (pendingent)->item
    macro_rules! PGSTAT_ACCUM_DBCOUNT {
        ($item:ident) => {
            (*sharedent).stats.$item += (*pendingent).$item;
        };
    }

    PGSTAT_ACCUM_DBCOUNT!(xact_commit);
    PGSTAT_ACCUM_DBCOUNT!(xact_rollback);
    PGSTAT_ACCUM_DBCOUNT!(blocks_fetched);
    PGSTAT_ACCUM_DBCOUNT!(blocks_hit);

    PGSTAT_ACCUM_DBCOUNT!(tuples_returned);
    PGSTAT_ACCUM_DBCOUNT!(tuples_fetched);
    PGSTAT_ACCUM_DBCOUNT!(tuples_inserted);
    PGSTAT_ACCUM_DBCOUNT!(tuples_updated);
    PGSTAT_ACCUM_DBCOUNT!(tuples_deleted);

    // last_autovac_time is reported immediately
    // Assert(pendingent->last_autovac_time == 0);  -- stubbed

    PGSTAT_ACCUM_DBCOUNT!(conflict_tablespace);
    PGSTAT_ACCUM_DBCOUNT!(conflict_lock);
    PGSTAT_ACCUM_DBCOUNT!(conflict_snapshot);
    PGSTAT_ACCUM_DBCOUNT!(conflict_logicalslot);
    PGSTAT_ACCUM_DBCOUNT!(conflict_bufferpin);
    PGSTAT_ACCUM_DBCOUNT!(conflict_startup_deadlock);

    PGSTAT_ACCUM_DBCOUNT!(temp_bytes);
    PGSTAT_ACCUM_DBCOUNT!(temp_files);
    PGSTAT_ACCUM_DBCOUNT!(deadlocks);

    // checksum failures are reported immediately
    // Assert(pendingent->checksum_failures == 0);  -- stubbed
    // Assert(pendingent->last_checksum_failure == 0);  -- stubbed

    PGSTAT_ACCUM_DBCOUNT!(blk_read_time);
    PGSTAT_ACCUM_DBCOUNT!(blk_write_time);

    PGSTAT_ACCUM_DBCOUNT!(sessions);
    PGSTAT_ACCUM_DBCOUNT!(session_time);
    PGSTAT_ACCUM_DBCOUNT!(active_time);
    PGSTAT_ACCUM_DBCOUNT!(idle_in_transaction_time);
    PGSTAT_ACCUM_DBCOUNT!(sessions_abandoned);
    PGSTAT_ACCUM_DBCOUNT!(sessions_fatal);
    PGSTAT_ACCUM_DBCOUNT!(sessions_killed);
    PGSTAT_ACCUM_DBCOUNT!(parallel_workers_to_launch);
    PGSTAT_ACCUM_DBCOUNT!(parallel_workers_launched);

    pgstat_unlock_entry(entry_ref);

    memset(
        pendingent as *mut c_void,
        0,
        size_of::<PgStat_StatDBEntry>(),
    );

    true
}

/// Reset-timestamp callback (pgstat.c kind-info dispatch).
pub unsafe fn pgstat_database_reset_timestamp_cb(header: *mut PgStatShared_Common, ts: TimestampTz) {
    (*(header as *mut PgStatShared_Database)).stats.stat_reset_timestamp = ts;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::activity::pgstat::pgstat_get_entry_ref;

    // Serialize tests: the pgstat entry table is a process-global static.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn prep_bump_flush_fetch_roundtrip() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            // Prepare the pending DB entry for dboid=5 and bump two counters.
            let dbentry = pgstat_prep_database_pending(5);
            (*dbentry).deadlocks += 1;
            (*dbentry).xact_commit += 2;

            // Drive get_entry_ref directly to obtain the eref for the flush.
            let eref = pgstat_get_entry_ref(PGSTAT_KIND_DATABASE, 5, InvalidOid, true, null_mut());
            assert!(pgstat_database_flush_cb(eref, false));

            // Shared stats must now reflect the accumulated pending counters.
            let shared = pgstat_fetch_stat_dbentry(5);
            assert!(!shared.is_null());
            assert_eq!((*shared).deadlocks, 1);
            assert_eq!((*shared).xact_commit, 2);

            // Pending must have been memset to zero after the flush.
            assert_eq!((*dbentry).deadlocks, 0);
            assert_eq!((*dbentry).xact_commit, 0);
        }
    }
}
