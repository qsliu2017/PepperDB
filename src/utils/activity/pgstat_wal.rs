//! Translation of postgres/src/backend/utils/activity/pgstat_wal.c
//!
//! Implementation of WAL statistics. Kept separate from pgstat.c to enforce the
//! line between the statistics access / storage implementation and the details
//! about individual types of statistics.
//!
//! The fixed-stats CORE (changecount protocol, shmem control, snapshot, the
//! LWLock no-op stubs) lives in crate::utils::activity::pgstat and is imported
//! here. The WAL-record globals come from crate::executor::instrument:
//!   - `WalUsage` (struct) and `WalUsageAccumDiff` (dst += add - sub) are REAL.
//!   - `pgWalUsage` (the live per-process WAL usage counter that the WAL manager
//!     bumps) is not exported from instrument.rs, and the WAL manager that would
//!     advance it is not ported, so it is STUBBED here as a module-local
//!     `static mut` initialized to zero. TODO: replace with the real ported
//!     pgWalUsage global once the WAL manager exists.
//!
//! Two pgstat infra entry points the upstream report path calls
//! (`pgstat_flush_backend`, `pgstat_flush_io`) are not in the core; they are
//! STUBBED finely below with a TODO.
//!
//! Portions Copyright (c) 2001-2025, PostgreSQL Global Development Group

use crate::prelude::*;
use crate::executor::instrument::{WalUsage, WalUsageAccumDiff};
use crate::utils::activity::pgstat::{
    pgStatLocal, pgstat_shmem, pgstat_snapshot_fixed, LWLockAcquire,
    LWLockInitialize, LWLockRelease, PgStat_WalStats, PgStatShared_Wal, TimestampTz,
    LWTRANCHE_PGSTATS_DATA, LW_EXCLUSIVE, PGSTAT_KIND_WAL,
};

extern "C" {
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// Local stubs for unported pgstat infra / WAL-record globals.
// ---------------------------------------------------------------------------

// LWLock LW_SHARED mode and the conditional-acquire entry point are not in the
// fixed-stats core (the LWLock there is an inert no-op). Mirror them here so the
// translated logic keeps the same shape. STUB: acquisition always succeeds.
const LW_SHARED: c_int = 1;

#[inline]
unsafe fn LWLockConditionalAcquire(lock: *mut crate::utils::activity::pgstat::LWLock, mode: c_int) -> bool {
    LWLockAcquire(lock, mode)
}

// pgstat backend-flush request flags (pgstat.h). Only the WAL/IO bits are used
// from this file. STUB values; the real backend-stats flush path is unported.
const PGSTAT_BACKEND_FLUSH_WAL: u32 = 1 << 0;
const PGSTAT_BACKEND_FLUSH_IO: u32 = 1 << 1;

// STUB: flushes backend-local stats of the given kinds into shmem. Unported;
// returns false (no lock contention). TODO: port pgstat_backend.c.
#[inline]
unsafe fn pgstat_flush_backend(_nowait: bool, _flags: u32) -> bool { crate::utils::activity::pgstat_backend::pgstat_flush_backend(_nowait, _flags) }

// STUB: flushes pending IO stats into shmem. Unported. TODO: port pgstat_io.c.
#[inline]
unsafe fn pgstat_flush_io(_nowait: bool) -> bool {
    false
}

/// WAL usage counters that the WAL manager bumps as records are written.
///
/// STUB: the real `pgWalUsage` is a PGDLLIMPORT global updated by the WAL
/// manager (not ported) and is not exported from instrument.rs. A module-local
/// zero-initialized copy is kept here so the diff arithmetic and the
/// have-pending check remain real. TODO: replace with the real ported global.
static mut pgWalUsage: WalUsage = WalUsage {
    wal_records: 0,
    wal_fpi: 0,
    wal_bytes: 0,
    wal_buffers_full: 0,
};

// ---------------------------------------------------------------------------
// pgstat_wal.c
// ---------------------------------------------------------------------------

/// WAL usage counters saved from pgWalUsage at the previous call to
/// pgstat_report_wal(). Used to calculate how much WAL usage happens between
/// pgstat_report_wal() calls, by subtracting the previous counters from the
/// current ones.
static mut prevWalUsage: WalUsage = WalUsage {
    wal_records: 0,
    wal_fpi: 0,
    wal_bytes: 0,
    wal_buffers_full: 0,
};

/// Calculate how much WAL usage counters have increased and update shared WAL
/// and IO statistics.
///
/// Must be called by processes that generate WAL, that do not call
/// pgstat_report_stat(), like walwriter.
///
/// "force" set to true ensures that the statistics are flushed; note that this
/// needs to acquire the pgstat shmem LWLock, waiting on it. When set to false,
/// the statistics may not be flushed if the lock could not be acquired.
pub unsafe fn pgstat_report_wal(force: bool) {
    // like in pgstat.c, don't wait for lock acquisition when !force
    let nowait = !force;

    // flush wal stats
    let _ = pgstat_wal_flush_cb(nowait);
    pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_WAL);

    // flush IO stats
    pgstat_flush_io(nowait);
    let _ = pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_IO);
}

/// Support function for the SQL-callable pgstat* functions. Returns a pointer to
/// the WAL statistics struct.
pub unsafe fn pgstat_fetch_stat_wal() -> *mut PgStat_WalStats {
    pgstat_snapshot_fixed(PGSTAT_KIND_WAL);

    &mut pgStatLocal.snapshot.wal
}

/// To determine whether WAL usage happened.
#[inline]
unsafe fn pgstat_wal_have_pending() -> bool {
    pgWalUsage.wal_records != prevWalUsage.wal_records
}

/// Calculate how much WAL usage counters have increased by subtracting the
/// previous counters from the current ones.
///
/// If nowait is true, this function returns true if the lock could not be
/// acquired. Otherwise return false.
pub unsafe fn pgstat_wal_flush_cb(nowait: bool) -> bool {
    let stats_shmem: *mut PgStatShared_Wal = &mut (*pgstat_shmem()).wal;
    let mut wal_usage_diff = WalUsage::default();

    // Assert(IsUnderPostmaster || !IsPostmasterEnvironment);
    // Assert(pgStatLocal.shmem != NULL && !pgStatLocal.shmem->is_shutdown);

    // This function can be called even if nothing at all has happened. Avoid
    // taking lock for nothing in that case.
    if !pgstat_wal_have_pending() {
        return false;
    }

    // We don't update the WAL usage portion of the local WalStats elsewhere.
    // Calculate how much WAL usage counters were increased by subtracting the
    // previous counters from the current ones.
    WalUsageAccumDiff(&mut wal_usage_diff, &raw const pgWalUsage, &raw const prevWalUsage);

    if !nowait {
        LWLockAcquire(&mut (*stats_shmem).lock, LW_EXCLUSIVE);
    } else if !LWLockConditionalAcquire(&mut (*stats_shmem).lock, LW_EXCLUSIVE) {
        return true;
    }

    // WALSTAT_ACC(fld, var_to_add): stats.wal_counters.fld += var_to_add.fld
    let wc = &mut (*stats_shmem).stats.wal_counters;
    wc.wal_records += wal_usage_diff.wal_records;
    wc.wal_fpi += wal_usage_diff.wal_fpi;
    wc.wal_bytes += wal_usage_diff.wal_bytes;
    wc.wal_buffers_full += wal_usage_diff.wal_buffers_full;

    LWLockRelease(&mut (*stats_shmem).lock);

    // Save the current counters for the subsequent calculation of WAL usage.
    prevWalUsage = pgWalUsage;

    false
}

pub unsafe fn pgstat_wal_init_backend_cb() {
    // Initialize prevWalUsage with pgWalUsage so that pgstat_wal_flush_cb() can
    // calculate how much pgWalUsage counters are increased by subtracting
    // prevWalUsage from pgWalUsage.
    prevWalUsage = pgWalUsage;
}

pub unsafe extern "C" fn pgstat_wal_init_shmem_cb(stats: *mut c_void) {
    let stats_shmem = stats as *mut PgStatShared_Wal;

    LWLockInitialize(&mut (*stats_shmem).lock, LWTRANCHE_PGSTATS_DATA);
}

pub unsafe fn pgstat_wal_reset_all_cb(ts: TimestampTz) {
    let stats_shmem: *mut PgStatShared_Wal = &mut (*pgstat_shmem()).wal;

    LWLockAcquire(&mut (*stats_shmem).lock, LW_EXCLUSIVE);
    memset(
        &mut (*stats_shmem).stats as *mut _ as *mut c_void,
        0,
        size_of::<PgStat_WalStats>(),
    );
    (*stats_shmem).stats.stat_reset_timestamp = ts;
    LWLockRelease(&mut (*stats_shmem).lock);
}

pub unsafe fn pgstat_wal_snapshot_cb() {
    let stats_shmem: *mut PgStatShared_Wal = &mut (*pgstat_shmem()).wal;

    LWLockAcquire(&mut (*stats_shmem).lock, LW_SHARED);
    core::ptr::copy(
        &(*stats_shmem).stats as *const PgStat_WalStats,
        &mut pgStatLocal.snapshot.wal as *mut PgStat_WalStats,
        1,
    );
    LWLockRelease(&mut (*stats_shmem).lock);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Serialize: these touch the process-global pgStatLocal / PGSTAT_SHMEM and
    // the module-local pgWalUsage/prevWalUsage statics.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn report_wal_bumps_then_fetch_reflects() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            // Reset shmem WAL counters and the local accumulators to a known
            // baseline so the assertion is independent of test ordering.
            let ctl = pgstat_shmem();
            (*ctl).wal.stats.wal_counters.wal_records = 0;
            (*ctl).wal.stats.wal_counters.wal_fpi = 0;
            (*ctl).wal.stats.wal_counters.wal_bytes = 0;
            (*ctl).wal.stats.wal_counters.wal_buffers_full = 0;
            (*ctl).wal.changecount = 0;

            pgWalUsage = WalUsage::default();
            prevWalUsage = pgWalUsage;

            // No pending WAL: flush is a no-op and reports "nothing flushed".
            assert!(!pgstat_wal_flush_cb(false));

            // Simulate WAL activity that the WAL manager would record.
            pgWalUsage.wal_records = 7;
            pgWalUsage.wal_fpi = 3;
            pgWalUsage.wal_bytes = 4096;
            pgWalUsage.wal_buffers_full = 2;

            // force flush (force == true -> nowait == false): commits the diff.
            pgstat_report_wal(true);

            // The shared counters now hold the accumulated diff.
            assert_eq!((*ctl).wal.stats.wal_counters.wal_records, 7);
            assert_eq!((*ctl).wal.stats.wal_counters.wal_fpi, 3);
            assert_eq!((*ctl).wal.stats.wal_counters.wal_bytes, 4096);
            assert_eq!((*ctl).wal.stats.wal_counters.wal_buffers_full, 2);

            // Fetch via the snapshot path and assert it reflects the bump.
            let snap = pgstat_fetch_stat_wal();
            assert_eq!((*snap).wal_counters.wal_records, 7);
            assert_eq!((*snap).wal_counters.wal_fpi, 3);
            assert_eq!((*snap).wal_counters.wal_bytes, 4096);
            assert_eq!((*snap).wal_counters.wal_buffers_full, 2);

            // prevWalUsage advanced to current; a second flush finds nothing.
            assert!(!pgstat_wal_flush_cb(false));
        }
    }

    #[test]
    fn reset_all_clears_and_stamps() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            let ctl = pgstat_shmem();
            (*ctl).wal.stats.wal_counters.wal_records = 99;
            pgstat_wal_reset_all_cb(1234 as TimestampTz);
            assert_eq!((*ctl).wal.stats.wal_counters.wal_records, 0);
            assert_eq!((*ctl).wal.stats.stat_reset_timestamp, 1234 as TimestampTz);
        }
    }
}
