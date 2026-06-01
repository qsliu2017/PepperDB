//! utils/adt/waitfuncs.c - SQL access to syntheses of multiple contention types.

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::utils::array::{ArrayType, ARR_DATA_PTR, ARR_DIMS, ARR_ELEMTYPE, ARR_NDIM};
use crate::utils::adt::arrayutils::ArrayGetNItems;
use crate::utils::fmgr::FunctionCallInfo;
use crate::storage::predicate_internals::GetSafeSnapshotBlockingPids;
use crate::catalog::pg_type_d::INT4OID;
use crate::{PG_GETARG_INT32, PG_GETARG_DATUM, PG_RETURN_BOOL, DirectFunctionCall1};

// uint32 / int32 come from crate::c::* via the prelude.

// PGPROC is opaque here; the only field we touch is wait_event_info (a uint32).
// The local stub for BackendPidGetProc returns a typed pointer; we read the
// field through an accessor stub since the full PGPROC layout is not ported.
#[allow(non_camel_case_types)]
type PGPROC = std::ffi::c_void;

// UINT32_ACCESS_ONCE(var) == (uint32)(*((volatile uint32 *)&(var)))
#[inline]
unsafe fn UINT32_ACCESS_ONCE(var: *const uint32) -> uint32 {
    core::ptr::read_volatile(var)
}

// ---------------------------------------------------------------------------
// Local stubs for dependencies not yet ported. // TODO: deps not ported
// ---------------------------------------------------------------------------

// storage/proc.h - BackendPidGetProc
unsafe fn BackendPidGetProc(_pid: c_int) -> *mut PGPROC {
    unimplemented!()
}

// Accessor for proc->wait_event_info; PGPROC layout not yet ported.
// TODO: replace with real field access once storage/proc.h PGPROC is ported.
unsafe fn proc_wait_event_info(_proc: *mut PGPROC) -> *const uint32 {
    unimplemented!()
}

// utils/wait_event.h - pgstat_get_wait_event_type
unsafe fn pgstat_get_wait_event_type(_wait_event_info: uint32) -> *const c_char {
    unimplemented!()
}

// utils/array.h - array_contains_nulls
unsafe fn array_contains_nulls(_array: *mut ArrayType) -> bool {
    unimplemented!()
}

// utils/array.h - DatumGetArrayTypeP (detoasts as needed)
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType {
    unimplemented!()
}

// storage/procarray.h fmgr function - pg_blocking_pids
// Used via DirectFunctionCall1, so it must have the fmgr V1 signature.
unsafe fn pg_blocking_pids(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!()
}

/*
 * pg_isolation_test_session_is_blocked - support function for isolationtester
 *
 * Check if specified PID is blocked by any of the PIDs listed in the second
 * argument.  Currently, this looks for blocking caused by waiting for
 * injection points, heavyweight locks, or safe snapshots.  We ignore blockage
 * caused by PIDs not directly under the isolationtester's control, eg
 * autovacuum.
 *
 * This is an undocumented function intended for use by the isolation tester,
 * and may change in future releases as required for testing purposes.
 */
// PG_FUNCTION_INFO_V1(pg_isolation_test_session_is_blocked)
pub unsafe fn pg_isolation_test_session_is_blocked(fcinfo: FunctionCallInfo) -> Datum {
    let blocked_pid: c_int = PG_GETARG_INT32!(fcinfo, 0);
    // PG_GETARG_ARRAYTYPE_P(1) == DatumGetArrayTypeP(PG_GETARG_DATUM(1))
    let interesting_pids_a: *mut ArrayType = DatumGetArrayTypeP(PG_GETARG_DATUM!(fcinfo, 1));
    let proc: *mut PGPROC;
    let wait_event_type: *const c_char;
    let blocking_pids_a: *mut ArrayType;
    let interesting_pids: *mut int32;
    let blocking_pids: *mut int32;
    let num_interesting_pids: c_int;
    let num_blocking_pids: c_int;
    let mut dummy: c_int = 0;

    /* Check if blocked_pid is in an injection point. */
    proc = BackendPidGetProc(blocked_pid);
    if proc.is_null() {
        PG_RETURN_BOOL!(false); /* session gone: definitely unblocked */
    }
    wait_event_type =
        pgstat_get_wait_event_type(UINT32_ACCESS_ONCE(proc_wait_event_info(proc)));
    if !wait_event_type.is_null()
        && libc_strcmp(c_str_InjectionPoint(), wait_event_type) == 0
    {
        PG_RETURN_BOOL!(true);
    }

    /* Validate the passed-in array */
    Assert!(ARR_ELEMTYPE(interesting_pids_a) == INT4OID);
    if array_contains_nulls(interesting_pids_a) {
        elog!(ERROR, "array must not contain nulls");
    }
    interesting_pids = ARR_DATA_PTR(interesting_pids_a) as *mut int32;
    num_interesting_pids = ArrayGetNItems(
        ARR_NDIM(interesting_pids_a),
        ARR_DIMS(interesting_pids_a),
    );

    /*
     * Get the PIDs of all sessions blocking the given session's attempt to
     * acquire heavyweight locks.
     */
    blocking_pids_a =
        DatumGetArrayTypeP(DirectFunctionCall1!(pg_blocking_pids, Int32GetDatum(blocked_pid)));

    Assert!(ARR_ELEMTYPE(blocking_pids_a) == INT4OID);
    Assert!(!array_contains_nulls(blocking_pids_a));
    blocking_pids = ARR_DATA_PTR(blocking_pids_a) as *mut int32;
    num_blocking_pids = ArrayGetNItems(
        ARR_NDIM(blocking_pids_a),
        ARR_DIMS(blocking_pids_a),
    );

    /*
     * Check if any of these are in the list of interesting PIDs, that being
     * the sessions that the isolation tester is running.  We don't use
     * "arrayoverlaps" here, because it would lead to cache lookups and one of
     * our goals is to run quickly with debug_discard_caches > 0.  We expect
     * blocking_pids to be usually empty and otherwise a very small number in
     * isolation tester cases, so make that the outer loop of a naive search
     * for a match.
     */
    let mut i: c_int = 0;
    while i < num_blocking_pids {
        let mut j: c_int = 0;
        while j < num_interesting_pids {
            if *blocking_pids.offset(i as isize) == *interesting_pids.offset(j as isize) {
                PG_RETURN_BOOL!(true);
            }
            j += 1;
        }
        i += 1;
    }

    /*
     * Check if blocked_pid is waiting for a safe snapshot.  We could in
     * theory check the resulting array of blocker PIDs against the
     * interesting PIDs list, but since there is no danger of autovacuum
     * blocking GetSafeSnapshot there seems to be no point in expending cycles
     * on allocating a buffer and searching for overlap; so it's presently
     * sufficient for the isolation tester's purposes to use a single element
     * buffer and check if the number of safe snapshot blockers is non-zero.
     */
    if GetSafeSnapshotBlockingPids(blocked_pid, &mut dummy, 1) > 0 {
        PG_RETURN_BOOL!(true);
    }

    PG_RETURN_BOOL!(false);
}

// ---------------------------------------------------------------------------
// Small helpers for the strcmp("InjectionPoint", ...) comparison.
// strcmp is a libc symbol; keep faithful C semantics over NUL-terminated
// C strings rather than introducing a Rust &str comparison.
// ---------------------------------------------------------------------------

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

#[inline]
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    strcmp(a, b)
}

#[inline]
fn c_str_InjectionPoint() -> *const c_char {
    // "InjectionPoint\0"
    b"InjectionPoint\0".as_ptr() as *const c_char
}
