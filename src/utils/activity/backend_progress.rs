// ----------
// backend_progress.rs
//
//	Command progress reporting infrastructure.
//
//	Copyright (c) 2001-2025, PostgreSQL Global Development Group
//
//	src/backend/utils/activity/backend_progress.c
// ----------
//
// Rust translation of PostgreSQL 18.3 backend_progress.c.
//
// DEVIATION: backend_status.c (and its shared-memory PgBackendStatus array)
// is not yet ported. To keep this module self-contained, we provide a MINIMAL
// process-local stand-in for the current backend's status entry (MyBEEntry):
//   - a `static mut MY_BESTATUS: PgBackendStatus` holding only the progress
//     fields actually touched here, plus the changecount, and
//   - `MyBEEntry()` returning a pointer to it.
// When backend_status.c is ported, replace the stand-in with the real entry.
//
// The changecount write protocol mirrors pgstat_internal.h / backend_status.h:
// PGSTAT_BEGIN_WRITE_ACTIVITY bumps st_changecount (now odd) before writing,
// PGSTAT_END_WRITE_ACTIVITY bumps it again (now even) after. Memory barriers
// and the critical-section macros are no-ops in this single-process stand-in.

use core::ffi::c_int;
use core::sync::atomic::{compiler_fence, Ordering};

use crate::c::int64;
use crate::postgres_ext::{InvalidOid, Oid};

// ----------
// ProgressCommandType
//
// Mirrors utils/backend_progress.h. PROGRESS_COMMAND_INVALID is 0.
// ----------
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProgressCommandType {
    PROGRESS_COMMAND_INVALID = 0,
    PROGRESS_COMMAND_VACUUM,
    PROGRESS_COMMAND_ANALYZE,
    PROGRESS_COMMAND_CLUSTER,
    PROGRESS_COMMAND_CREATE_INDEX,
    PROGRESS_COMMAND_BASEBACKUP,
    PROGRESS_COMMAND_COPY,
}

pub use ProgressCommandType::*;

// From utils/backend_progress.h: #define PGSTAT_NUM_PROGRESS_PARAM 20
pub const PGSTAT_NUM_PROGRESS_PARAM: usize = 20;

// ----------
// PgBackendStatus (minimal stand-in)
//
// Only the fields read/written by this module are modeled. The real struct
// in backend_status.h has many more fields. st_progress_command stores a
// ProgressCommandType value; we keep it as c_int to match the C layout where
// the enum is written into an int-typed field.
// ----------
#[repr(C)]
pub struct PgBackendStatus {
    pub st_changecount: u32,
    pub st_progress_command: c_int,
    pub st_progress_command_target: Oid,
    pub st_progress_param: [int64; PGSTAT_NUM_PROGRESS_PARAM],
}

// Process-local stand-in for the current backend's status entry.
static mut MY_BESTATUS: PgBackendStatus = PgBackendStatus {
    st_changecount: 0,
    st_progress_command: ProgressCommandType::PROGRESS_COMMAND_INVALID as c_int,
    st_progress_command_target: InvalidOid,
    st_progress_param: [0; PGSTAT_NUM_PROGRESS_PARAM],
};

// MyBEEntry: pointer to the current backend's status entry.
//
// In real PostgreSQL this is a macro expanding to MyBEEntry (a global pointer
// that may be NULL very early in startup). Here it always points at the
// process-local stand-in, so it is never NULL.
#[inline]
unsafe fn MyBEEntry() -> *mut PgBackendStatus {
    &raw mut MY_BESTATUS
}

// GUC bool pgstat_track_activities. STUB: defaults to true.
static mut pgstat_track_activities: bool = true;

// STUB: pgstat_assert_is_up() - no-op until backend_status.c is ported.
#[inline]
unsafe fn pgstat_assert_is_up() {}

// ----------
// Changecount write protocol (PGSTAT_BEGIN_WRITE_ACTIVITY /
// PGSTAT_END_WRITE_ACTIVITY from backend_status.h).
//
// begin: START_CRIT_SECTION(); st_changecount++; pg_write_barrier();
// end:   pg_write_barrier(); st_changecount++; Assert even; END_CRIT_SECTION();
//
// The critical-section and memory barriers are no-ops in this stand-in; we
// keep a compiler_fence so the changecount bump is not reordered around the
// payload writes, matching the intent of the barriers.
// ----------
#[inline]
unsafe fn PGSTAT_BEGIN_WRITE_ACTIVITY(beentry: *mut PgBackendStatus) {
    // START_CRIT_SECTION(): no-op
    (*beentry).st_changecount = (*beentry).st_changecount.wrapping_add(1);
    // pg_write_barrier(): no-op; compiler fence keeps the order
    compiler_fence(Ordering::Release);
}

#[inline]
unsafe fn PGSTAT_END_WRITE_ACTIVITY(beentry: *mut PgBackendStatus) {
    // pg_write_barrier(): no-op; compiler fence keeps the order
    compiler_fence(Ordering::Release);
    (*beentry).st_changecount = (*beentry).st_changecount.wrapping_add(1);
    debug_assert_eq!((*beentry).st_changecount & 1, 0);
    // END_CRIT_SECTION(): no-op
}

// -----------
// pgstat_progress_start_command() -
//
// Set st_progress_command (and st_progress_command_target) in own backend
// entry.  Also, zero-initialize st_progress_param array.
// -----------
pub unsafe fn pgstat_progress_start_command(cmdtype: ProgressCommandType, relid: Oid) {
    let beentry: *mut PgBackendStatus = MyBEEntry();

    if beentry.is_null() || !pgstat_track_activities {
        return;
    }

    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
    (*beentry).st_progress_command = cmdtype as c_int;
    (*beentry).st_progress_command_target = relid;
    // MemSet(&st_progress_param, 0, sizeof(...))
    (*beentry).st_progress_param = [0; PGSTAT_NUM_PROGRESS_PARAM];
    PGSTAT_END_WRITE_ACTIVITY(beentry);
}

// -----------
// pgstat_progress_update_param() -
//
// Update index'th member in st_progress_param[] of own backend entry.
// -----------
pub unsafe fn pgstat_progress_update_param(index: c_int, val: int64) {
    let beentry: *mut PgBackendStatus = MyBEEntry();

    assert!(index >= 0 && (index as usize) < PGSTAT_NUM_PROGRESS_PARAM);

    if beentry.is_null() || !pgstat_track_activities {
        return;
    }

    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
    (*beentry).st_progress_param[index as usize] = val;
    PGSTAT_END_WRITE_ACTIVITY(beentry);
}

// -----------
// pgstat_progress_incr_param() -
//
// Increment index'th member in st_progress_param[] of own backend entry.
// -----------
pub unsafe fn pgstat_progress_incr_param(index: c_int, incr: int64) {
    let beentry: *mut PgBackendStatus = MyBEEntry();

    assert!(index >= 0 && (index as usize) < PGSTAT_NUM_PROGRESS_PARAM);

    if beentry.is_null() || !pgstat_track_activities {
        return;
    }

    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
    (*beentry).st_progress_param[index as usize] =
        (*beentry).st_progress_param[index as usize].wrapping_add(incr);
    PGSTAT_END_WRITE_ACTIVITY(beentry);
}

// -----------
// pgstat_progress_parallel_incr_param() -
//
// A variant of pgstat_progress_incr_param to allow a worker to poke at
// a leader to do an incremental progress update.
// -----------
pub unsafe fn pgstat_progress_parallel_incr_param(index: c_int, incr: int64) {
    // Parallel workers notify a leader through a PqMsg_Progress message to
    // update progress, passing the progress index and incremented value.
    // Leaders can just call pgstat_progress_incr_param directly.
    //
    // TODO: parallelism is not ported yet (IsParallelWorker / pq_* / atomics).
    // STUB: behave as a leader and increment locally.
    pgstat_progress_incr_param(index, incr);
}

// -----------
// pgstat_progress_update_multi_param() -
//
// Update multiple members in st_progress_param[] of own backend entry.
// This is atomic; readers won't see intermediate states.
// -----------
pub unsafe fn pgstat_progress_update_multi_param(
    nparam: c_int,
    index: *const c_int,
    val: *const int64,
) {
    let beentry: *mut PgBackendStatus = MyBEEntry();

    if beentry.is_null() || !pgstat_track_activities || nparam == 0 {
        return;
    }

    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);

    let mut i: c_int = 0;
    while i < nparam {
        let idx = *index.offset(i as isize);
        assert!(idx >= 0 && (idx as usize) < PGSTAT_NUM_PROGRESS_PARAM);

        (*beentry).st_progress_param[idx as usize] = *val.offset(i as isize);
        i += 1;
    }

    PGSTAT_END_WRITE_ACTIVITY(beentry);
}

// -----------
// pgstat_progress_end_command() -
//
// Reset st_progress_command (and st_progress_command_target) in own backend
// entry.  This signals the end of the command.
// -----------
pub unsafe fn pgstat_progress_end_command() {
    let beentry: *mut PgBackendStatus = MyBEEntry();

    if beentry.is_null() || !pgstat_track_activities {
        return;
    }

    if (*beentry).st_progress_command
        == ProgressCommandType::PROGRESS_COMMAND_INVALID as c_int
    {
        return;
    }

    PGSTAT_BEGIN_WRITE_ACTIVITY(beentry);
    (*beentry).st_progress_command =
        ProgressCommandType::PROGRESS_COMMAND_INVALID as c_int;
    (*beentry).st_progress_command_target = InvalidOid;
    PGSTAT_END_WRITE_ACTIVITY(beentry);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Serialize tests: they all mutate the shared process-local MY_BESTATUS.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    unsafe fn reset() {
        MY_BESTATUS.st_changecount = 0;
        MY_BESTATUS.st_progress_command =
            ProgressCommandType::PROGRESS_COMMAND_INVALID as c_int;
        MY_BESTATUS.st_progress_command_target = InvalidOid;
        MY_BESTATUS.st_progress_param = [0; PGSTAT_NUM_PROGRESS_PARAM];
    }

    #[test]
    fn start_command_sets_command_and_target() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            reset();
            pgstat_progress_start_command(
                ProgressCommandType::PROGRESS_COMMAND_VACUUM,
                42,
            );
            assert_eq!(
                MY_BESTATUS.st_progress_command,
                ProgressCommandType::PROGRESS_COMMAND_VACUUM as c_int
            );
            assert_eq!(MY_BESTATUS.st_progress_command_target, 42);
            // changecount started even, two bumps -> still even.
            assert_eq!(MY_BESTATUS.st_changecount & 1, 0);
        }
    }

    #[test]
    fn update_param_sets_value() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            reset();
            pgstat_progress_update_param(3, 100);
            assert_eq!(MY_BESTATUS.st_progress_param[3], 100);
            assert_eq!(MY_BESTATUS.st_changecount & 1, 0);
        }
    }

    #[test]
    fn incr_param_adds() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            reset();
            pgstat_progress_update_param(5, 10);
            pgstat_progress_incr_param(5, 7);
            assert_eq!(MY_BESTATUS.st_progress_param[5], 17);
            assert_eq!(MY_BESTATUS.st_changecount & 1, 0);
        }
    }

    #[test]
    fn end_command_resets() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            reset();
            pgstat_progress_start_command(
                ProgressCommandType::PROGRESS_COMMAND_ANALYZE,
                7,
            );
            pgstat_progress_end_command();
            assert_eq!(
                MY_BESTATUS.st_progress_command,
                ProgressCommandType::PROGRESS_COMMAND_INVALID as c_int
            );
            assert_eq!(MY_BESTATUS.st_progress_command_target, InvalidOid);
            assert_eq!(MY_BESTATUS.st_changecount & 1, 0);
        }
    }
}
