//! Implementation of checkpoint statistics.
//!
//! Faithful 1:1 translation of PostgreSQL 18.3
//! `src/backend/utils/activity/pgstat_checkpointer.c`. This is the fixed-amount
//! CHECKPOINTER stats reporter: a process-local `PendingCheckpointerStats`
//! accumulator is periodically flushed into the changecount-protected shared
//! `stats` block, and SQL-callable fetch obtains a reset-offset-compensated
//! snapshot.
//!
//! Deviations from upstream (all also noted inline):
//!
//! * `pgstat_flush_io(false)` is STUBBED (TODO): the IO stats reporter is not
//!   ported yet. `pgstat_report_checkpointer()` calls it at the end; here it is
//!   a no-op so the checkpointer-specific flush stays REAL.
//!
//! * `pgstat_assert_is_up()` / the `is_shutdown` assertion are omitted (the
//!   pgstat lifecycle asserts are not part of the ported core).
//!
//! * `LWLock` acquire/release and the changecount barriers are no-ops inherited
//!   from the ported core; the structure of the reset protocol and the
//!   changecounted shmem update / snapshot is translated faithfully.

use crate::prelude::*;

use crate::utils::activity::pgstat::{
    pgstat_begin_changecount_write, pgstat_copy_changecounted_stats,
    pgstat_end_changecount_write, pgstat_shmem, pgstat_snapshot_fixed, GetCurrentTimestamp,
    LWLockAcquire, LWLockInitialize, LWLockRelease, PgStatShared_Checkpointer,
    PgStat_CheckpointerStats, TimestampTz, LWTRANCHE_PGSTATS_DATA, LW_EXCLUSIVE,
    PGSTAT_KIND_CHECKPOINTER,
};

// Local memcpy/memset (no `libc` crate). Used for struct-field copies and the
// MemSet/pg_memory_is_all_zeros equivalents below.
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// storage/lwlock.h: LW_SHARED. The core exports only LW_EXCLUSIVE; the stubbed
// LWLockAcquire ignores the mode, so the exact value is immaterial.
const LW_SHARED: c_int = 1;

/// Process-local accumulator for pending checkpointer stats.
///
/// Upstream: `PgStat_CheckpointerStats PendingCheckpointerStats = {0};`
pub static mut PendingCheckpointerStats: PgStat_CheckpointerStats =
    PgStat_CheckpointerStats::zeroed();

/// True iff every byte of `*p` is zero (utils/memutils.h: pg_memory_is_all_zeros).
#[inline]
unsafe fn pg_memory_is_all_zeros(p: *const PgStat_CheckpointerStats) -> bool {
    let bytes = p as *const u8;
    let n = size_of::<PgStat_CheckpointerStats>();
    let mut i = 0usize;
    while i < n {
        if *bytes.add(i) != 0 {
            return false;
        }
        i += 1;
    }
    true
}

/// pgstat_report_checkpointer() - report checkpointer and IO statistics.
pub unsafe fn pgstat_report_checkpointer() {
    let stats_shmem: *mut PgStatShared_Checkpointer = &raw mut (*pgstat_shmem()).checkpointer;

    // Assert(!pgStatLocal.shmem->is_shutdown);  -- lifecycle assert omitted
    // pgstat_assert_is_up();                    -- lifecycle assert omitted

    // This function can be called even if nothing at all has happened. In this
    // case, avoid unnecessarily modifying the stats entry.
    if pg_memory_is_all_zeros(&raw const PendingCheckpointerStats) {
        return;
    }

    pgstat_begin_changecount_write(&raw mut (*stats_shmem).changecount);

    // #define CHECKPOINTER_ACC(fld) stats_shmem->stats.fld += PendingCheckpointerStats.fld
    macro_rules! checkpointer_acc {
        ($fld:ident) => {
            (*stats_shmem).stats.$fld += PendingCheckpointerStats.$fld
        };
    }
    checkpointer_acc!(num_timed);
    checkpointer_acc!(num_requested);
    checkpointer_acc!(num_performed);
    checkpointer_acc!(restartpoints_timed);
    checkpointer_acc!(restartpoints_requested);
    checkpointer_acc!(restartpoints_performed);
    checkpointer_acc!(write_time);
    checkpointer_acc!(sync_time);
    checkpointer_acc!(buffers_written);
    checkpointer_acc!(slru_written);

    pgstat_end_changecount_write(&raw mut (*stats_shmem).changecount);

    // Clear out the statistics buffer, so it can be re-used.
    memset(
        &raw mut PendingCheckpointerStats as *mut c_void,
        0,
        size_of::<PgStat_CheckpointerStats>(),
    );

    // Report IO statistics.
    pgstat_flush_io(false);
}

/// pgstat_fetch_stat_checkpointer() - support function for the SQL-callable
/// pgstat* functions. Returns a pointer to the checkpointer statistics struct.
pub unsafe fn pgstat_fetch_stat_checkpointer() -> *mut PgStat_CheckpointerStats {
    pgstat_snapshot_fixed(PGSTAT_KIND_CHECKPOINTER);

    use crate::utils::activity::pgstat::pgStatLocal;
    &raw mut pgStatLocal.snapshot.checkpointer
}

/// init_shmem callback: initialize the per-kind LWLock.
pub unsafe fn pgstat_checkpointer_init_shmem_cb(stats: *mut c_void) {
    let stats_shmem = stats as *mut PgStatShared_Checkpointer;

    LWLockInitialize(&raw mut (*stats_shmem).lock, LWTRANCHE_PGSTATS_DATA);
}

/// reset_all callback: snapshot the live counters into reset_offset and stamp
/// the reset timestamp. See the reset protocol above PgStatShared_Checkpointer.
pub unsafe fn pgstat_checkpointer_reset_all_cb(ts: TimestampTz) {
    let stats_shmem: *mut PgStatShared_Checkpointer = &raw mut (*pgstat_shmem()).checkpointer;

    LWLockAcquire(&raw mut (*stats_shmem).lock, LW_EXCLUSIVE);
    pgstat_copy_changecounted_stats(
        &raw mut (*stats_shmem).reset_offset as *mut c_void,
        &raw mut (*stats_shmem).stats as *mut c_void,
        size_of::<PgStat_CheckpointerStats>(),
        &raw mut (*stats_shmem).changecount,
    );
    (*stats_shmem).stats.stat_reset_timestamp = ts;
    LWLockRelease(&raw mut (*stats_shmem).lock);
}

/// snapshot callback: copy the live counters into the process-local snapshot
/// and compensate by the reset offsets.
pub unsafe fn pgstat_checkpointer_snapshot_cb() {
    use crate::utils::activity::pgstat::pgStatLocal;

    let stats_shmem: *mut PgStatShared_Checkpointer = &raw mut (*pgstat_shmem()).checkpointer;
    let reset_offset: *mut PgStat_CheckpointerStats = &raw mut (*stats_shmem).reset_offset;
    let mut reset = PgStat_CheckpointerStats::zeroed();

    pgstat_copy_changecounted_stats(
        &raw mut pgStatLocal.snapshot.checkpointer as *mut c_void,
        &raw mut (*stats_shmem).stats as *mut c_void,
        size_of::<PgStat_CheckpointerStats>(),
        &raw mut (*stats_shmem).changecount,
    );

    LWLockAcquire(&raw mut (*stats_shmem).lock, LW_SHARED);
    memcpy(
        &raw mut reset as *mut c_void,
        reset_offset as *const c_void,
        size_of::<PgStat_CheckpointerStats>(),
    );
    LWLockRelease(&raw mut (*stats_shmem).lock);

    // compensate by reset offsets
    // #define CHECKPOINTER_COMP(fld) pgStatLocal.snapshot.checkpointer.fld -= reset.fld;
    macro_rules! checkpointer_comp {
        ($fld:ident) => {
            pgStatLocal.snapshot.checkpointer.$fld -= reset.$fld
        };
    }
    checkpointer_comp!(num_timed);
    checkpointer_comp!(num_requested);
    checkpointer_comp!(num_performed);
    checkpointer_comp!(restartpoints_timed);
    checkpointer_comp!(restartpoints_requested);
    checkpointer_comp!(restartpoints_performed);
    checkpointer_comp!(write_time);
    checkpointer_comp!(sync_time);
    checkpointer_comp!(buffers_written);
    checkpointer_comp!(slru_written);
}

// ---------------------------------------------------------------------------
// STUB: pgstat_flush_io (pgstat_io.c unported)
// ---------------------------------------------------------------------------
//
// TODO: replace with the real IO-stats flush once pgstat_io.c is ported.
// Upstream `pgstat_report_checkpointer()` ends by flushing pending IO stats;
// that path is not available in this subset, so this is an inert placeholder.
#[inline]
unsafe fn pgstat_flush_io(_nowait: bool) {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::activity::pgstat::pgStatLocal;

    // Serialize: these touch the process-global pgStatLocal / PGSTAT_SHMEM.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn report_then_fetch_reflects_bump() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            // Reset shared + pending so the test is self-contained.
            let stats_shmem: *mut PgStatShared_Checkpointer =
                &raw mut (*pgstat_shmem()).checkpointer;
            (*stats_shmem).stats = PgStat_CheckpointerStats::zeroed();
            (*stats_shmem).reset_offset = PgStat_CheckpointerStats::zeroed();
            (*stats_shmem).changecount = 0;
            PendingCheckpointerStats = PgStat_CheckpointerStats::zeroed();
            pgStatLocal.snapshot.checkpointer = PgStat_CheckpointerStats::zeroed();

            // Bump a couple of pending counters and flush into shmem.
            PendingCheckpointerStats.num_timed = 3;
            PendingCheckpointerStats.buffers_written = 17;
            pgstat_report_checkpointer();

            // Pending buffer must have been cleared after the flush.
            assert!(pg_memory_is_all_zeros(&raw const PendingCheckpointerStats));

            // Fetch obtains a snapshot via pgstat_snapshot_fixed; the
            // snapshot_cb-style compensation leaves the (zero) reset offset.
            let snap = pgstat_fetch_stat_checkpointer();
            assert_eq!((*snap).num_timed, 3);
            assert_eq!((*snap).buffers_written, 17);

            // A no-op report (all-zero pending) must not change the shared stats.
            pgstat_report_checkpointer();
            let snap2 = pgstat_fetch_stat_checkpointer();
            assert_eq!((*snap2).num_timed, 3);
            assert_eq!((*snap2).buffers_written, 17);
        }
    }
}
