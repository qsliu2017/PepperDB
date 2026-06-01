// -------------------------------------------------------------------------
//
// pgstat_archiver.rs
//   Implementation of archiver statistics.
//
// This file contains the implementation of archiver statistics. It is kept
// separate from pgstat.rs to enforce the line between the statistics access /
// storage implementation and the details about individual types of
// statistics.
//
// Copyright (c) 2001-2025, PostgreSQL Global Development Group
//
// IDENTIFICATION
//   src/backend/utils/activity/pgstat_archiver.c
// -------------------------------------------------------------------------

use crate::prelude::*;

use crate::utils::activity::pgstat::{
    pgstat_begin_changecount_write, pgstat_copy_changecounted_stats, pgstat_end_changecount_write,
    pgstat_shmem, pgstat_snapshot_fixed, pgStatLocal, GetCurrentTimestamp, LWLockAcquire,
    LWLockInitialize, LWLockRelease, PgStatShared_Archiver, PgStat_ArchiverStats,
    LWTRANCHE_PGSTATS_DATA, LW_EXCLUSIVE, PGSTAT_KIND_ARCHIVER, TimestampTz,
};

// LW_SHARED is not part of the stubbed core lock API (only LW_EXCLUSIVE is);
// define it locally to mirror the C lwlock.h LWLockMode enum value used here.
const LW_SHARED: c_int = 1;

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// Report archiver statistics
pub unsafe fn pgstat_report_archiver(xlog: *const c_char, failed: bool) {
    let stats_shmem: *mut PgStatShared_Archiver = &mut (*pgstat_shmem()).archiver;
    let now = GetCurrentTimestamp();

    pgstat_begin_changecount_write(&mut (*stats_shmem).changecount);

    if failed {
        (*stats_shmem).stats.failed_count += 1;
        memcpy(
            (*stats_shmem).stats.last_failed_wal.as_mut_ptr() as *mut c_void,
            xlog as *const c_void,
            size_of_val(&(*stats_shmem).stats.last_failed_wal),
        );
        (*stats_shmem).stats.last_failed_timestamp = now;
    } else {
        (*stats_shmem).stats.archived_count += 1;
        memcpy(
            (*stats_shmem).stats.last_archived_wal.as_mut_ptr() as *mut c_void,
            xlog as *const c_void,
            size_of_val(&(*stats_shmem).stats.last_archived_wal),
        );
        (*stats_shmem).stats.last_archived_timestamp = now;
    }

    pgstat_end_changecount_write(&mut (*stats_shmem).changecount);
}

// Support function for the SQL-callable pgstat* functions. Returns
// a pointer to the archiver statistics struct.
pub unsafe fn pgstat_fetch_stat_archiver() -> *mut PgStat_ArchiverStats {
    pgstat_snapshot_fixed(PGSTAT_KIND_ARCHIVER);

    &mut pgStatLocal.snapshot.archiver
}

pub unsafe fn pgstat_archiver_init_shmem_cb(stats: *mut c_void) {
    let stats_shmem = stats as *mut PgStatShared_Archiver;

    LWLockInitialize(&mut (*stats_shmem).lock, LWTRANCHE_PGSTATS_DATA);
}

pub unsafe fn pgstat_archiver_reset_all_cb(ts: TimestampTz) {
    let stats_shmem: *mut PgStatShared_Archiver = &mut (*pgstat_shmem()).archiver;

    // see explanation above PgStatShared_Archiver for the reset protocol
    LWLockAcquire(&mut (*stats_shmem).lock, LW_EXCLUSIVE);
    pgstat_copy_changecounted_stats(
        &mut (*stats_shmem).reset_offset as *mut _ as *mut c_void,
        &mut (*stats_shmem).stats as *mut _ as *mut c_void,
        size_of_val(&(*stats_shmem).stats),
        &mut (*stats_shmem).changecount,
    );
    (*stats_shmem).stats.stat_reset_timestamp = ts;
    LWLockRelease(&mut (*stats_shmem).lock);
}

pub unsafe fn pgstat_archiver_snapshot_cb() {
    let stats_shmem: *mut PgStatShared_Archiver = &mut (*pgstat_shmem()).archiver;
    let stat_snap: *mut PgStat_ArchiverStats = &mut pgStatLocal.snapshot.archiver;
    let reset_offset: *mut PgStat_ArchiverStats = &mut (*stats_shmem).reset_offset;
    let mut reset = PgStat_ArchiverStats::zeroed();

    pgstat_copy_changecounted_stats(
        stat_snap as *mut c_void,
        &mut (*stats_shmem).stats as *mut _ as *mut c_void,
        size_of_val(&(*stats_shmem).stats),
        &mut (*stats_shmem).changecount,
    );

    LWLockAcquire(&mut (*stats_shmem).lock, LW_SHARED);
    memcpy(
        &mut reset as *mut _ as *mut c_void,
        reset_offset as *const c_void,
        size_of_val(&(*stats_shmem).stats),
    );
    LWLockRelease(&mut (*stats_shmem).lock);

    // compensate by reset offsets
    if (*stat_snap).archived_count == reset.archived_count {
        (*stat_snap).last_archived_wal[0] = 0;
        (*stat_snap).last_archived_timestamp = 0;
    }
    (*stat_snap).archived_count -= reset.archived_count;

    if (*stat_snap).failed_count == reset.failed_count {
        (*stat_snap).last_failed_wal[0] = 0;
        (*stat_snap).last_failed_timestamp = 0;
    }
    (*stat_snap).failed_count -= reset.failed_count;
}

#[cfg(test)]
mod tests {
    use super::*;

    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_report_and_fetch_archiver() {
        let _guard = LOCK.lock().unwrap();
        unsafe {
            // Capture baseline counts from a fresh snapshot.
            let before = *pgstat_fetch_stat_archiver();

            // Bump the archived counter via the reporter (shmem update).
            let wal: [c_char; 6] = [
                b'0' as c_char,
                b'0' as c_char,
                b'0' as c_char,
                b'0' as c_char,
                b'1' as c_char,
                0,
            ];
            pgstat_report_archiver(wal.as_ptr(), false);

            // Fetch again; the snapshot must reflect the bump.
            let after = *pgstat_fetch_stat_archiver();
            assert_eq!(after.archived_count, before.archived_count + 1);
            assert_eq!(after.last_archived_wal[0], b'0' as c_char);
            assert_eq!(after.last_archived_wal[4], b'1' as c_char);

            // Bump the failed counter too.
            let before_failed = after.failed_count;
            pgstat_report_archiver(wal.as_ptr(), true);
            let after2 = *pgstat_fetch_stat_archiver();
            assert_eq!(after2.failed_count, before_failed + 1);
        }
    }
}
