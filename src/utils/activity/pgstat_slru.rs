//! Translation of postgres/src/backend/utils/activity/pgstat_slru.c
//!
//! Implementation of SLRU statistics. Kept separate from pgstat.c to enforce the
//! line between the statistics access / storage implementation and the details
//! about individual types of statistics.
//!
//! The fixed-stats CORE (shmem control, snapshot, the LWLock no-op stubs) lives
//! in crate::utils::activity::pgstat and is imported here.
//!
//! Deviations from upstream PostgreSQL 18.3:
//!
//! * SLRU's shared wrapper (`PgStatShared_SLRU`) has NO changecount and NO
//!   reset_offset -- it is guarded by its LWLock directly. The core's
//!   `pgstat_snapshot_fixed(PGSTAT_KIND_SLRU)` performs the lock-guarded memcpy;
//!   `pgstat_slru_snapshot_cb` here mirrors it as the per-kind callback.
//!
//! * `pgstat_assert_is_up`, the `IsUnderPostmaster`/`IsPostmasterEnvironment`
//!   asserts, and `pgstat_report_fixed` are not part of the ported core; they are
//!   STUBBED finely (no-ops / module-local) with a TODO.
//!
//! * `LWLockConditionalAcquire` and `LW_SHARED` are not in the fixed-stats core
//!   (the LWLock there is an inert no-op). They are mirrored locally so the
//!   translated logic keeps the same shape; acquisition always succeeds.
//!
//! * `slru_names` is the source of truth for SLRU indices. Upstream declares it
//!   in `lwlocknames.h`/`slru.c`; mirrored here as a module-local table of the
//!   eight canonical names.
//!
//! Portions Copyright (c) 2001-2025, PostgreSQL Global Development Group

use crate::prelude::*;
use crate::utils::activity::pgstat::{
    pgStatLocal, pgstat_shmem, pgstat_snapshot_fixed, GetCurrentTimestamp, LWLockAcquire,
    LWLockInitialize, LWLockRelease, PgStat_Counter, PgStat_SLRUStats, PgStatShared_SLRU,
    TimestampTz, LWTRANCHE_PGSTATS_DATA, LW_EXCLUSIVE, PGSTAT_KIND_SLRU, SLRU_NUM_ELEMENTS,
};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// Local stubs for unported pgstat infra (see header comment).
// ---------------------------------------------------------------------------

// storage/lwlock.h: LW_SHARED. The core exposes only LW_EXCLUSIVE; mirror the
// shared mode here. STUB value matching upstream's enum (LW_EXCLUSIVE=0,
// LW_SHARED=1).
const LW_SHARED: c_int = 1;

// STUB: conditional (nowait) acquire. The core LWLock is an inert no-op so this
// always succeeds, mirroring LWLockConditionalAcquire's shape. TODO: replace
// when storage/lwlock.c is ported.
#[inline]
unsafe fn LWLockConditionalAcquire(
    lock: *mut crate::utils::activity::pgstat::LWLock,
    mode: c_int,
) -> bool {
    LWLockAcquire(lock, mode)
}

// STUB: pgstat_assert_is_up() asserts the cumulative stats subsystem is
// initialized. No-op until the bootstrap path is ported.
#[inline]
unsafe fn pgstat_assert_is_up() {}

// STUB: set by the core report path to note that fixed-amount stats have pending
// updates. The flush/report driver is unported, so this is a module-local sink.
// TODO: wire to the real pgstat_report_fixed once the report driver is ported.
static mut pgstat_report_fixed: bool = false;

/// SLRU names. The order defines the SLRU index space; the final "other" entry
/// is the catch-all for SLRUs defined in external projects. Mirrors upstream's
/// `slru_names[]`.
static SLRU_NAMES: [&str; SLRU_NUM_ELEMENTS] = [
    "commit_timestamp",
    "multixact_member",
    "multixact_offset",
    "notify",
    "serializable",
    "subtransaction",
    "transaction",
    "other",
];

// ---------------------------------------------------------------------------
// pgstat_slru.c
// ---------------------------------------------------------------------------

/// SLRU statistics counts waiting to be flushed out. Inits to zeroes. Entries are
/// one-to-one with SLRU_NAMES[]. Changes of SLRU counters are reported within
/// critical sections so we use static memory to avoid memory allocation.
static mut pending_SLRUStats: [PgStat_SLRUStats; SLRU_NUM_ELEMENTS] =
    [PgStat_SLRUStats::zeroed(); SLRU_NUM_ELEMENTS];
static mut have_slrustats: bool = false;

/// Reset counters for a single SLRU.
///
/// Permission checking for this function is managed through the normal GRANT
/// system.
pub unsafe fn pgstat_reset_slru(name: *const c_char) {
    let ts: TimestampTz = GetCurrentTimestamp();

    // Assert(name != NULL);

    pgstat_reset_slru_counter_internal(pgstat_get_slru_index(name), ts);
}

//
// SLRU statistics count accumulation functions --- called from slru.c
//

pub unsafe fn pgstat_count_slru_page_zeroed(slru_idx: c_int) {
    (*get_slru_entry(slru_idx)).blocks_zeroed += 1;
}

pub unsafe fn pgstat_count_slru_page_hit(slru_idx: c_int) {
    (*get_slru_entry(slru_idx)).blocks_hit += 1;
}

pub unsafe fn pgstat_count_slru_page_exists(slru_idx: c_int) {
    (*get_slru_entry(slru_idx)).blocks_exists += 1;
}

pub unsafe fn pgstat_count_slru_page_read(slru_idx: c_int) {
    (*get_slru_entry(slru_idx)).blocks_read += 1;
}

pub unsafe fn pgstat_count_slru_page_written(slru_idx: c_int) {
    (*get_slru_entry(slru_idx)).blocks_written += 1;
}

pub unsafe fn pgstat_count_slru_flush(slru_idx: c_int) {
    (*get_slru_entry(slru_idx)).flush += 1;
}

pub unsafe fn pgstat_count_slru_truncate(slru_idx: c_int) {
    (*get_slru_entry(slru_idx)).truncate += 1;
}

/// Support function for the SQL-callable pgstat* functions. Returns a pointer to
/// the slru statistics struct.
pub unsafe fn pgstat_fetch_slru() -> *mut PgStat_SLRUStats {
    pgstat_snapshot_fixed(PGSTAT_KIND_SLRU);

    pgStatLocal.snapshot.slru.as_mut_ptr()
}

/// Returns SLRU name for an index. The index may be above SLRU_NUM_ELEMENTS, in
/// which case this returns NULL. This allows writing code that does not know the
/// number of entries in advance.
pub unsafe fn pgstat_get_slru_name(slru_idx: c_int) -> *const c_char {
    if slru_idx < 0 || slru_idx >= SLRU_NUM_ELEMENTS as c_int {
        return null();
    }

    // SLRU_NAMES holds &str backed by NUL-terminated string literals, so the
    // base pointer is a valid C string.
    SLRU_NAMES[slru_idx as usize].as_ptr() as *const c_char
}

/// Determine index of entry for a SLRU with a given name. If there's no exact
/// match, returns index of the last "other" entry used for SLRUs defined in
/// external projects.
pub unsafe fn pgstat_get_slru_index(name: *const c_char) -> c_int {
    for i in 0..SLRU_NUM_ELEMENTS {
        if strcmp_rs(SLRU_NAMES[i], name) {
            return i as c_int;
        }
    }

    // return index of the last entry (which is the "other" one)
    (SLRU_NUM_ELEMENTS - 1) as c_int
}

/// Flush out locally pending SLRU stats entries.
///
/// If nowait is true, this function returns true if the lock could not be
/// acquired. Otherwise return false.
pub unsafe fn pgstat_slru_flush_cb(nowait: bool) -> bool {
    let stats_shmem: *mut PgStatShared_SLRU = &mut (*pgstat_shmem()).slru;

    if !have_slrustats {
        return false;
    }

    if !nowait {
        LWLockAcquire(&mut (*stats_shmem).lock, LW_EXCLUSIVE);
    } else if !LWLockConditionalAcquire(&mut (*stats_shmem).lock, LW_EXCLUSIVE) {
        return true;
    }

    for i in 0..SLRU_NUM_ELEMENTS {
        let sharedent: *mut PgStat_SLRUStats = &mut (*stats_shmem).stats[i];
        let pendingent: *mut PgStat_SLRUStats = &mut pending_SLRUStats[i];

        // SLRU_ACC(fld): sharedent->fld += pendingent->fld
        (*sharedent).blocks_zeroed += (*pendingent).blocks_zeroed;
        (*sharedent).blocks_hit += (*pendingent).blocks_hit;
        (*sharedent).blocks_read += (*pendingent).blocks_read;
        (*sharedent).blocks_written += (*pendingent).blocks_written;
        (*sharedent).blocks_exists += (*pendingent).blocks_exists;
        (*sharedent).flush += (*pendingent).flush;
        (*sharedent).truncate += (*pendingent).truncate;
    }

    // done, clear the pending entry
    memset(
        &raw mut pending_SLRUStats as *mut c_void,
        0,
        size_of::<[PgStat_SLRUStats; SLRU_NUM_ELEMENTS]>(),
    );

    LWLockRelease(&mut (*stats_shmem).lock);

    have_slrustats = false;

    false
}

pub unsafe extern "C" fn pgstat_slru_init_shmem_cb(stats: *mut c_void) {
    let stats_shmem = stats as *mut PgStatShared_SLRU;

    LWLockInitialize(&mut (*stats_shmem).lock, LWTRANCHE_PGSTATS_DATA);
}

pub unsafe fn pgstat_slru_reset_all_cb(ts: TimestampTz) {
    for i in 0..SLRU_NUM_ELEMENTS as c_int {
        pgstat_reset_slru_counter_internal(i, ts);
    }
}

pub unsafe fn pgstat_slru_snapshot_cb() {
    let stats_shmem: *mut PgStatShared_SLRU = &mut (*pgstat_shmem()).slru;

    LWLockAcquire(&mut (*stats_shmem).lock, LW_SHARED);

    memcpy(
        pgStatLocal.snapshot.slru.as_mut_ptr() as *mut c_void,
        &(*stats_shmem).stats as *const _ as *const c_void,
        size_of::<[PgStat_SLRUStats; SLRU_NUM_ELEMENTS]>(),
    );

    LWLockRelease(&mut (*stats_shmem).lock);
}

/// Returns pointer to entry with counters for given SLRU (based on the name
/// stored in SlruCtl as lwlock tranche name).
unsafe fn get_slru_entry(slru_idx: c_int) -> *mut PgStat_SLRUStats {
    pgstat_assert_is_up();

    // The postmaster should never register any SLRU statistics counts; if it
    // did, the counts would be duplicated into child processes via fork().
    // Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

    // Assert((slru_idx >= 0) && (slru_idx < SLRU_NUM_ELEMENTS));

    have_slrustats = true;
    pgstat_report_fixed = true;

    &mut pending_SLRUStats[slru_idx as usize]
}

unsafe fn pgstat_reset_slru_counter_internal(index: c_int, ts: TimestampTz) {
    let stats_shmem: *mut PgStatShared_SLRU = &mut (*pgstat_shmem()).slru;

    LWLockAcquire(&mut (*stats_shmem).lock, LW_EXCLUSIVE);

    memset(
        &mut (*stats_shmem).stats[index as usize] as *mut _ as *mut c_void,
        0,
        size_of::<PgStat_SLRUStats>(),
    );
    (*stats_shmem).stats[index as usize].stat_reset_timestamp = ts;

    LWLockRelease(&mut (*stats_shmem).lock);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compare a Rust &str name (no embedded NUL) against a NUL-terminated C string,
/// standing in for C `strcmp(slru_names[i], name) == 0`.
unsafe fn strcmp_rs(name: &str, cstr: *const c_char) -> bool {
    let bytes = name.as_bytes();
    let mut i = 0usize;
    loop {
        let c = *cstr.add(i) as u8;
        if i == bytes.len() {
            // Equal iff the C string also terminates here.
            return c == 0;
        }
        if c != bytes[i] {
            return false;
        }
        i += 1;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Serialize: these touch the process-global pgStatLocal / PGSTAT_SHMEM and
    // the module-local pending_SLRUStats/have_slrustats statics.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn count_flush_then_fetch_reflects_bumps() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            // Reset shared SLRU stats and the pending accumulators to a known
            // baseline so the assertion is independent of test ordering.
            let ctl = pgstat_shmem();
            memset(
                &mut (*ctl).slru.stats as *mut _ as *mut c_void,
                0,
                size_of::<[PgStat_SLRUStats; SLRU_NUM_ELEMENTS]>(),
            );
            memset(
                &raw mut pending_SLRUStats as *mut c_void,
                0,
                size_of::<[PgStat_SLRUStats; SLRU_NUM_ELEMENTS]>(),
            );
            have_slrustats = false;

            // No pending stats: flush is a no-op and reports "nothing flushed".
            assert!(!pgstat_slru_flush_cb(false));

            // Simulate SLRU activity for index 0: one page hit + one page read.
            pgstat_count_slru_page_hit(0);
            pgstat_count_slru_page_read(0);

            // Pending should now hold the bumps and the have-flag be set.
            assert!(have_slrustats);
            assert_eq!(pending_SLRUStats[0].blocks_hit, 1);
            assert_eq!(pending_SLRUStats[0].blocks_read, 1);

            // Flush the pending stats into shared (force: nowait == false).
            assert!(!pgstat_slru_flush_cb(false));

            // Shared counters now hold the accumulated values; pending cleared.
            assert_eq!((*ctl).slru.stats[0].blocks_hit, 1);
            assert_eq!((*ctl).slru.stats[0].blocks_read, 1);
            assert_eq!(pending_SLRUStats[0].blocks_hit, 0);
            assert!(!have_slrustats);

            // Fetch via the snapshot path and assert it reflects the bumps.
            let snap = pgstat_fetch_slru();
            assert_eq!((*snap.add(0)).blocks_hit, 1);
            assert_eq!((*snap.add(0)).blocks_read, 1);
        }
    }

    #[test]
    fn get_slru_index_matches_names_and_other_fallback() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            let transaction = b"transaction\0";
            assert_eq!(pgstat_get_slru_index(transaction.as_ptr() as *const c_char), 6);

            // Unknown name falls back to the last "other" entry.
            let unknown = b"does_not_exist\0";
            assert_eq!(
                pgstat_get_slru_index(unknown.as_ptr() as *const c_char),
                (SLRU_NUM_ELEMENTS - 1) as c_int
            );

            // Out-of-range names yield a NULL pointer.
            assert!(pgstat_get_slru_name(SLRU_NUM_ELEMENTS as c_int).is_null());
            assert!(!pgstat_get_slru_name(0).is_null());
        }
    }

    #[test]
    fn reset_all_clears_and_stamps() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            let ctl = pgstat_shmem();
            (*ctl).slru.stats[2].blocks_written = 42;
            pgstat_slru_reset_all_cb(7777 as TimestampTz);
            assert_eq!((*ctl).slru.stats[2].blocks_written, 0);
            assert_eq!((*ctl).slru.stats[2].stat_reset_timestamp, 7777 as TimestampTz);
        }
    }
}
