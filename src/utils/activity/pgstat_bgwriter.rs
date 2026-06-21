//! pgstat_bgwriter.rs - Implementation of bgwriter statistics.
//!
//! 1:1 port of postgres/src/backend/utils/activity/pgstat_bgwriter.c.
//!
//! This file contains the implementation of bgwriter statistics. It is kept
//! separate from pgstat.c to enforce the line between the statistics access /
//! storage implementation and the details about individual types of
//! statistics.
//!
//! The fixed-stats CORE (changecount-protected shmem update, fetch/snapshot,
//! the shared wrappers and stats structs) lives in
//! `crate::utils::activity::pgstat`; this module imports from there.
//!
//! STUB notes:
//! * `pgstat_flush_io(false)` (IO statistics) is not part of the ported core,
//!   so the call at the end of `pgstat_report_bgwriter` is stubbed with a TODO.
//! * Upstream's `Assert(!pgStatLocal.shmem->is_shutdown)` and
//!   `pgstat_assert_is_up()` have no analogue in the trimmed core
//!   (`PgStat_ShmemControl` carries no `is_shutdown`); they become no-ops.
//! * Upstream uses `LW_SHARED` for the reset-offset read in the snapshot cb.
//!   The core only exposes `LW_EXCLUSIVE`; we define a module-local `LW_SHARED`
//!   constant matching upstream's value so the call site stays faithful. The
//!   stubbed `LWLockAcquire` ignores the mode regardless.

use crate::prelude::*;

use crate::utils::activity::pgstat::{
    pgstat_begin_changecount_write, pgstat_copy_changecounted_stats,
    pgstat_end_changecount_write, pgstat_shmem, pgstat_snapshot_fixed, pgStatLocal,
    GetCurrentTimestamp, LWLockAcquire, LWLockInitialize, LWLockRelease, PgStatShared_BgWriter,
    PgStat_BgWriterStats, TimestampTz, LWTRANCHE_PGSTATS_DATA, LW_EXCLUSIVE, PGSTAT_KIND_BGWRITER,
};

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(dst: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

/// LWLock shared-acquire mode. Upstream's enum value for LW_SHARED is 1
/// (LW_EXCLUSIVE is 0). The core only re-exports LW_EXCLUSIVE, so mirror the
/// constant here to keep the snapshot-cb call site faithful.
const LW_SHARED: c_int = 1;

/// Process-local accumulator for pending bgwriter statistics (upstream
/// `PgStat_BgWriterStats PendingBgWriterStats = {0};`). Exposed as a module
/// global so other modules (e.g. the bgwriter main loop) can bump counters.
pub static mut PendingBgWriterStats: PgStat_BgWriterStats = PgStat_BgWriterStats::zeroed();

/// Returns true when every byte of PendingBgWriterStats is zero, mirroring
/// upstream's `pg_memory_is_all_zeros(&PendingBgWriterStats, sizeof(...))`.
unsafe fn pending_is_all_zeros() -> bool {
    let p = core::ptr::addr_of!(PendingBgWriterStats) as *const u8;
    let n = size_of::<PgStat_BgWriterStats>();
    let mut i = 0usize;
    while i < n {
        if *p.add(i) != 0 {
            return false;
        }
        i += 1;
    }
    true
}

/// Report bgwriter and IO statistics.
pub unsafe fn pgstat_report_bgwriter() {
    let stats_shmem: *mut PgStatShared_BgWriter = &mut (*pgstat_shmem()).bgwriter;

    // Assert(!pgStatLocal.shmem->is_shutdown);  -- no is_shutdown in core
    // pgstat_assert_is_up();                    -- no-op in core

    // This function can be called even if nothing at all has happened. In this
    // case, avoid unnecessarily modifying the stats entry.
    if pending_is_all_zeros() {
        return;
    }

    pgstat_begin_changecount_write(&mut (*stats_shmem).changecount);

    (*stats_shmem).stats.buf_written_clean += PendingBgWriterStats.buf_written_clean;
    (*stats_shmem).stats.maxwritten_clean += PendingBgWriterStats.maxwritten_clean;
    (*stats_shmem).stats.buf_alloc += PendingBgWriterStats.buf_alloc;

    pgstat_end_changecount_write(&mut (*stats_shmem).changecount);

    // Clear out the statistics buffer, so it can be re-used.
    memset(
        core::ptr::addr_of_mut!(PendingBgWriterStats) as *mut c_void,
        0,
        size_of::<PgStat_BgWriterStats>(),
    );

    // Report IO statistics.
    // TODO: pgstat_flush_io(false) is not yet ported; IO stats are not flushed.
    pgstat_flush_io(false);
}

/// pgstat_flush_io STUB.
///
/// TODO: port pgstat_io.c. Upstream flushes the process-local pending IO
/// counters into shared memory. Until that module exists this is a no-op.
unsafe fn pgstat_flush_io(_nowait: bool) -> bool {
    false
}

/// Support function for the SQL-callable pgstat* functions. Returns a pointer
/// to the bgwriter statistics struct.
pub unsafe fn pgstat_fetch_stat_bgwriter() -> *mut PgStat_BgWriterStats {
    pgstat_snapshot_fixed(PGSTAT_KIND_BGWRITER);

    &mut pgStatLocal.snapshot.bgwriter
}

pub unsafe extern "C" fn pgstat_bgwriter_init_shmem_cb(stats: *mut c_void) {
    let stats_shmem = stats as *mut PgStatShared_BgWriter;

    LWLockInitialize(&mut (*stats_shmem).lock, LWTRANCHE_PGSTATS_DATA);
}

pub unsafe fn pgstat_bgwriter_reset_all_cb(ts: TimestampTz) {
    let stats_shmem: *mut PgStatShared_BgWriter = &mut (*pgstat_shmem()).bgwriter;

    // see explanation above PgStatShared_BgWriter for the reset protocol
    LWLockAcquire(&mut (*stats_shmem).lock, LW_EXCLUSIVE);
    pgstat_copy_changecounted_stats(
        &mut (*stats_shmem).reset_offset as *mut _ as *mut c_void,
        &mut (*stats_shmem).stats as *mut _ as *mut c_void,
        size_of::<PgStat_BgWriterStats>(),
        &mut (*stats_shmem).changecount,
    );
    (*stats_shmem).stats.stat_reset_timestamp = ts;
    LWLockRelease(&mut (*stats_shmem).lock);
}

pub unsafe fn pgstat_bgwriter_snapshot_cb() {
    let stats_shmem: *mut PgStatShared_BgWriter = &mut (*pgstat_shmem()).bgwriter;
    let reset_offset: *mut PgStat_BgWriterStats = &mut (*stats_shmem).reset_offset;
    let mut reset: PgStat_BgWriterStats = PgStat_BgWriterStats::zeroed();

    pgstat_copy_changecounted_stats(
        &mut pgStatLocal.snapshot.bgwriter as *mut _ as *mut c_void,
        &mut (*stats_shmem).stats as *mut _ as *mut c_void,
        size_of::<PgStat_BgWriterStats>(),
        &mut (*stats_shmem).changecount,
    );

    LWLockAcquire(&mut (*stats_shmem).lock, LW_SHARED);
    memcpy(
        &mut reset as *mut _ as *mut c_void,
        reset_offset as *const c_void,
        size_of::<PgStat_BgWriterStats>(),
    );
    LWLockRelease(&mut (*stats_shmem).lock);

    // compensate by reset offsets
    pgStatLocal.snapshot.bgwriter.buf_written_clean -= reset.buf_written_clean;
    pgStatLocal.snapshot.bgwriter.maxwritten_clean -= reset.maxwritten_clean;
    pgStatLocal.snapshot.bgwriter.buf_alloc -= reset.buf_alloc;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Serialize: these touch the process-global pgStatLocal / PGSTAT_SHMEM.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn report_then_fetch_reflects_bump() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            // Reset shared + pending + snapshot to a known baseline.
            let ctl = pgstat_shmem();
            (*ctl).bgwriter.stats = PgStat_BgWriterStats::zeroed();
            (*ctl).bgwriter.reset_offset = PgStat_BgWriterStats::zeroed();
            (*ctl).bgwriter.changecount = 0;
            PendingBgWriterStats = PgStat_BgWriterStats::zeroed();
            pgStatLocal.snapshot.bgwriter = PgStat_BgWriterStats::zeroed();

            // Bump the process-local pending accumulator.
            PendingBgWriterStats.buf_written_clean = 7;
            PendingBgWriterStats.maxwritten_clean = 3;
            PendingBgWriterStats.buf_alloc = 11;

            // Flush pending -> shared.
            pgstat_report_bgwriter();

            // Pending must be cleared after the flush.
            assert!(pending_is_all_zeros());

            // Fetch snapshots shared into pgStatLocal.snapshot.bgwriter.
            let snap = pgstat_fetch_stat_bgwriter();
            assert_eq!((*snap).buf_written_clean, 7);
            assert_eq!((*snap).maxwritten_clean, 3);
            assert_eq!((*snap).buf_alloc, 11);
        }
    }

    #[test]
    fn report_with_empty_pending_is_noop() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            let ctl = pgstat_shmem();
            (*ctl).bgwriter.stats = PgStat_BgWriterStats::zeroed();
            (*ctl).bgwriter.changecount = 0;
            (*ctl).bgwriter.stats.buf_alloc = 42; // pre-existing shared value
            PendingBgWriterStats = PgStat_BgWriterStats::zeroed();

            let before = (*ctl).bgwriter.changecount;
            pgstat_report_bgwriter(); // pending all-zero -> early return
            // Shared value untouched, changecount unchanged.
            assert_eq!((*ctl).bgwriter.stats.buf_alloc, 42);
            assert_eq!((*ctl).bgwriter.changecount, before);
        }
    }
}
