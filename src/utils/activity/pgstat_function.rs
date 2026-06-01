//! Implementation of function statistics.
//!
//! Faithful translation of `pgstat_function.c`. It is kept separate from
//! `pgstat.rs` to enforce the line between the statistics access/storage
//! implementation and the details of individual statistics types.
//!
//! Deviations from upstream PostgreSQL 18.3:
//!
//! * Inherited from `pgstat.rs`'s variable-kind machinery: the dshash table is
//!   a process-local entry table, locks are no-ops, and `pgstat_fetch_entry`
//!   returns the live shared pointer rather than a snapshot copy.
//!
//! * `PgStat_FunctionCounts.total_time`/`self_time` are stored as `int64`
//!   microseconds rather than `instr_time` (see `pgstat.rs`). The timing math in
//!   `pgstat_init_function_usage`/`pgstat_end_function_usage` is computed in
//!   `instr_time` exactly as upstream and only converted to microseconds when
//!   written into the pending counts -- this keeps the recursion-compensation
//!   arithmetic bit-for-bit faithful while matching our int64 storage.
//!
//! * `MyDatabaseId` is a process-local `static mut Oid` stub (real value comes
//!   from `miscadmin.h`/backend startup, unported).
//!
//! * `pgstat_track_functions` GUC is a `static mut c_int` stub; the
//!   AcceptInvalidationMessages()/SearchSysCacheExists1() dropped-function check
//!   in `pgstat_init_function_usage` is omitted (inval/syscache unported) -- the
//!   `created_entry` branch is reduced to its non-error path.
//!
//! * `find_funcstat_entry` uses `pgstat_get_entry_ref(..., create=false)` and
//!   returns its `pending` pointer in place of upstream's
//!   `pgstat_fetch_pending_entry`.
//!
//! IDENTIFICATION
//!   src/backend/utils/activity/pgstat_function.c

use crate::prelude::*;

use crate::portability::instr_time::{
    instr_time, INSTR_TIME_ADD, INSTR_TIME_GET_MICROSEC, INSTR_TIME_SET_CURRENT, INSTR_TIME_SUBTRACT,
};
use crate::utils::activity::pgstat::{
    pgstat_create_transactional, pgstat_drop_transactional, pgstat_fetch_entry,
    pgstat_get_entry_ref, pgstat_lock_entry, pgstat_prep_pending_entry, pgstat_unlock_entry,
    PgStatShared_Function, PgStat_Counter, PgStat_EntryRef, PgStat_FunctionCounts,
    PgStat_StatFuncEntry, PGSTAT_KIND_FUNCTION,
};
use crate::utils::fmgr::FunctionCallInfo;

// ----------
// track_functions GUC values (utils/guc.h: TRACK_FUNC_*).
// ----------
pub const TRACK_FUNC_OFF: c_int = 0;
pub const TRACK_FUNC_PL: c_int = 1;
pub const TRACK_FUNC_ALL: c_int = 2;

// ----------
// GUC parameters
// ----------
//
// STUB: real value is wired through guc tables; defaults to TRACK_FUNC_OFF.
pub static mut pgstat_track_functions: c_int = TRACK_FUNC_OFF;

/// STUB: real value comes from `miscadmin.h` (set during backend startup).
pub static mut MyDatabaseId: Oid = InvalidOid;

/// Total time charged to functions so far in the current backend. Used to help
/// separate "self" and "other" time charges. (Assumed to initialize to zero.)
static mut total_func_time: instr_time = instr_time { ticks: 0 };

/// Function call usage tracking state (pgstat.h: PgStat_FunctionCallUsage).
///
/// Defined locally per the porting plan. `fs` points at the pending
/// `PgStat_FunctionCounts` for the function being timed.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgStat_FunctionCallUsage {
    pub fs: *mut PgStat_FunctionCounts,
    pub save_f_total_time: instr_time,
    pub save_total: instr_time,
    pub f_start: instr_time,
}

/// Ensure that stats are dropped if transaction aborts.
pub unsafe fn pgstat_create_function(proid: Oid) {
    pgstat_create_transactional(PGSTAT_KIND_FUNCTION, MyDatabaseId, proid);
}

/// Ensure that stats are dropped if transaction commits.
///
/// NB: This is only reliable because `pgstat_init_function_usage()` does some
/// extra work. If other places start emitting function stats they likely need
/// similar logic.
pub unsafe fn pgstat_drop_function(proid: Oid) {
    pgstat_drop_transactional(PGSTAT_KIND_FUNCTION, MyDatabaseId, proid);
}

/// Initialize function call usage data.
/// Called by the executor before invoking a function.
pub unsafe fn pgstat_init_function_usage(
    fcinfo: FunctionCallInfo,
    fcu: *mut PgStat_FunctionCallUsage,
) {
    let flinfo = (*fcinfo).flinfo;

    if pgstat_track_functions <= (*flinfo).fn_stats as c_int {
        /* stats not wanted */
        (*fcu).fs = null_mut();
        return;
    }

    let mut created_entry: bool = false;
    let entry_ref: *mut PgStat_EntryRef = pgstat_prep_pending_entry(
        PGSTAT_KIND_FUNCTION,
        MyDatabaseId,
        (*flinfo).fn_oid,
        &mut created_entry,
    );

    // DEVIATION: upstream here calls AcceptInvalidationMessages() and
    // SearchSysCacheExists1(PROCOID, ...) to detect a function dropped
    // concurrently, calling pgstat_drop_entry() + ereport(ERROR) if so. The
    // inval/syscache subsystems are unported, so that dropped-function guard is
    // omitted; the entry is used as created.
    let _ = created_entry;

    let pending = (*entry_ref).pending as *mut PgStat_FunctionCounts;

    (*fcu).fs = pending;

    /* save stats for this function, later used to compensate for recursion */
    (*fcu).save_f_total_time = instr_time {
        ticks: (*pending).total_time,
    };

    /* save current backend-wide total time */
    (*fcu).save_total = total_func_time;

    /* get clock time as of function start */
    INSTR_TIME_SET_CURRENT(&mut (*fcu).f_start);
}

/// Calculate function call usage and update stat counters.
/// Called by the executor after invoking a function.
///
/// In the case of a set-returning function that runs in value-per-call mode,
/// we will see multiple `pgstat_init_function_usage`/`pgstat_end_function_usage`
/// calls for what the user considers a single call of the function. The
/// `finalize` flag should be true on the last call.
pub unsafe fn pgstat_end_function_usage(fcu: *mut PgStat_FunctionCallUsage, finalize: bool) {
    let fs = (*fcu).fs;
    let mut total: instr_time;
    let mut others: instr_time;
    let mut this_self: instr_time;

    /* stats not wanted? */
    if fs.is_null() {
        return;
    }

    /* total elapsed time in this function call */
    total = instr_time { ticks: 0 };
    INSTR_TIME_SET_CURRENT(&mut total);
    INSTR_TIME_SUBTRACT(&mut total, (*fcu).f_start);

    /* self usage: elapsed minus anything already charged to other calls */
    others = total_func_time;
    INSTR_TIME_SUBTRACT(&mut others, (*fcu).save_total);
    this_self = total;
    INSTR_TIME_SUBTRACT(&mut this_self, others);

    /* update backend-wide total time */
    INSTR_TIME_ADD(&mut total_func_time, this_self);

    /*
     * Compute the new total_time as the total elapsed time added to the
     * pre-call value of total_time. This is necessary to avoid double-counting
     * any time taken by recursive calls of myself. (We do not need any similar
     * kluge for self time, since that already excludes any recursive calls.)
     */
    INSTR_TIME_ADD(&mut total, (*fcu).save_f_total_time);

    /* update counters in function stats table */
    if finalize {
        (*fs).numcalls += 1;
    }
    // DEVIATION: pending stores microseconds (int64). total/self_time arithmetic
    // above is done in instr_time; convert here exactly where the assignment /
    // accumulation happens.
    (*fs).total_time = INSTR_TIME_GET_MICROSEC(total);
    (*fs).self_time += INSTR_TIME_GET_MICROSEC(this_self);
}

/// Flush out pending stats for the entry.
///
/// If `nowait` is true and the lock could not be immediately acquired, returns
/// false without flushing the entry. Otherwise returns true.
pub unsafe fn pgstat_function_flush_cb(entry_ref: *mut PgStat_EntryRef, nowait: bool) -> bool {
    let localent = (*entry_ref).pending as *mut PgStat_FunctionCounts;
    let shfuncent = (*entry_ref).shared_stats as *mut PgStatShared_Function;

    /* localent always has non-zero content */

    if !pgstat_lock_entry(entry_ref, nowait) {
        return false;
    }

    (*shfuncent).stats.numcalls += (*localent).numcalls;
    // DEVIATION: localent.total_time/self_time are already microseconds (int64),
    // so add them directly rather than via INSTR_TIME_GET_MICROSEC().
    (*shfuncent).stats.total_time += (*localent).total_time;
    (*shfuncent).stats.self_time += (*localent).self_time;

    pgstat_unlock_entry(entry_ref);

    true
}

/// Find any existing `PgStat_FunctionCounts` entry for the specified function.
///
/// If no entry, return null, don't create a new one.
///
/// DEVIATION: upstream uses `pgstat_fetch_pending_entry`; here we look up the
/// entry-ref without creating it and return its `pending` pointer.
pub unsafe fn find_funcstat_entry(func_id: Oid) -> *mut PgStat_FunctionCounts {
    let entry_ref =
        pgstat_get_entry_ref(PGSTAT_KIND_FUNCTION, MyDatabaseId, func_id, false, null_mut());

    if !entry_ref.is_null() {
        return (*entry_ref).pending as *mut PgStat_FunctionCounts;
    }
    null_mut()
}

/// Support function for the SQL-callable pgstat* functions. Returns the
/// collected statistics for one function or null.
///
/// HEADER-OFFSET: `pgstat_fetch_entry` returns the START of the shared blob,
/// which is a `PgStatShared_Function` (header first). We must take the address
/// of its embedded `.stats`, not cast the blob start to `PgStat_StatFuncEntry*`.
pub unsafe fn pgstat_fetch_stat_funcentry(func_id: Oid) -> *mut PgStat_StatFuncEntry {
    let sh =
        pgstat_fetch_entry(PGSTAT_KIND_FUNCTION, MyDatabaseId, func_id) as *mut PgStatShared_Function;
    if sh.is_null() {
        null_mut()
    } else {
        &mut (*sh).stats as *mut PgStat_StatFuncEntry
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // The process-local entry table in pgstat.rs is global mutable state shared
    // across all variable-kind tests; serialize to keep entries deterministic.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn flush_cb_lands_in_shared() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            let dboid: Oid = MyDatabaseId;
            let funcoid: Oid = 99;

            // Get a pending function entry for funcoid=99.
            let mut created = false;
            let eref =
                pgstat_prep_pending_entry(PGSTAT_KIND_FUNCTION, dboid, funcoid, &mut created);
            assert!(!eref.is_null());

            // Bump its pending counters directly (already microseconds).
            let pending = (*eref).pending as *mut PgStat_FunctionCounts;
            assert!(!pending.is_null());
            (*pending).numcalls = 3;
            (*pending).total_time = 1500;
            (*pending).self_time = 700;

            // Flush into the shared entry.
            assert!(pgstat_function_flush_cb(eref, false));

            // Fetch the shared stats and assert the values landed (respecting the
            // header offset).
            let sh = pgstat_fetch_stat_funcentry(funcoid);
            assert!(!sh.is_null());
            assert_eq!((*sh).numcalls, 3);
            assert_eq!((*sh).total_time, 1500);
            assert_eq!((*sh).self_time, 700);

            // A second flush accumulates on top of the first.
            assert!(pgstat_function_flush_cb(eref, false));
            let sh2 = pgstat_fetch_stat_funcentry(funcoid);
            assert_eq!((*sh2).numcalls, 6);
            assert_eq!((*sh2).total_time, 3000);
            assert_eq!((*sh2).self_time, 1400);
        }
    }
}
