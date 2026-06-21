//! Translated from postgres/src/backend/access/table/tableamapi.c
//!
//! Support routines for the API for Postgres table access methods.
//!
//! #include mapping:
//!   - access/tableam.h     -> GetTableAmRoutine / GetHeapamTableAmRoutine
//!                             prototypes and the `TableAmRoutine` Node. tableamapi.c
//!                             only ever treats a `TableAmRoutine *` as a Node
//!                             pointer (the `IsA(routine, TableAmRoutine)` check),
//!                             so the giant routine struct is modeled here as an
//!                             opaque forward declaration (see `TableAmRoutine`).
//!   - access/xact.h        -> IsTransactionState() ........................ STUB
//!   - commands/defrem.h    -> get_table_am_oid() ......................... STUB
//!   - miscadmin.h          -> MyDatabaseId ............................... STUB
//!   - utils/guc_hooks.h    -> check_default_table_access_method prototype;
//!                             GucSource / GUC_check_errdetail / PGC_S_TEST ... STUB
//!
//! FULLY REAL:
//!   - GetTableAmRoutine: OidFunctionCall0 the handler, cast the Datum to a
//!     TableAmRoutine pointer, NULL/IsA check.
//!   - check_default_table_access_method: the empty-string and NAMEDATALEN
//!     length validations.
//!
//! STUB:
//!   - GetHeapamTableAmRoutine (the heapam handler / heap routine are not ported).
//!   - The catalog lookup branch of check_default_table_access_method
//!     (get_table_am_oid / IsTransactionState / MyDatabaseId not ported).

use crate::prelude::*;

use crate::nodes::nodes::nodeTag;
use crate::nodes::nodes::NodeTag;
use crate::pg_config::NAMEDATALEN;
use crate::postgres::DatumGetPointer;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::utils::fmgr::OidFunctionCall0Coll;
use crate::c::OidIsValid;

/* ----------------------------------------------------------------
 * TableAmRoutine (access/tableam.h)
 *
 * The real `TableAmRoutine` is a large struct of ~40 function-pointer
 * callbacks beginning with a `NodeTag type` field (it is a proper Node). Inside
 * tableamapi.c it is only ever handled as a Node pointer: we OidFunctionCall the
 * handler, cast the returned Datum to `TableAmRoutine *`, and `IsA`-check the
 * tag. None of the individual callbacks are dereferenced here (the upstream
 * Assert()s on each callback being non-NULL are debug-only sanity checks against
 * the full struct layout). We therefore model it as an opaque forward
 * declaration whose first field is the NodeTag, which is all this file needs.
 * ---------------------------------------------------------------- */

/// Opaque forward declaration of `TableAmRoutine` (access/tableam.h).
///
/// Like a C node, the first machine word is the [`NodeTag`]; the remaining
/// callback fields are intentionally not modeled here.
// TODO(pg-port): replace with the full TableAmRoutine struct (scan_begin,
// scan_end, tuple_insert, relation_size, ... ~40 callbacks) once access/tableam.h
// is ported; then restore the per-callback Assert(routine->cb != NULL) checks in
// GetTableAmRoutine.
#[repr(C)]
pub struct TableAmRoutine {
    /// `NodeTag type;` -- must be `T_TableAmRoutine`.
    pub r#type: NodeTag,
    _private: [u8; 0],
}

/* ----------------------------------------------------------------
 * Unported dependencies (finest-granularity stubs)
 * ---------------------------------------------------------------- */

/// Stub for `GucSource` (utils/guc.h). The value is only compared against
/// `PGC_S_TEST` in the catalog branch, which is itself stubbed out.
// TODO(pg-port): replace with the real GucSource enum once utils/guc is ported.
pub type GucSource = c_int;

/// `PGC_S_TEST` (utils/guc.h): the GUC source used by a test/SET-LOCAL probe.
// TODO(pg-port): real value from the GucSource enum in utils/guc.h.
#[allow(dead_code)]
const PGC_S_TEST: GucSource = 0;

/// `GUC_check_errdetail(...)` (utils/guc.h) stages a detail string onto the
/// in-flight GUC check error. Until guc.c is ported this is a no-op shim that
/// merely formats its argument (mirroring the stack_depth.rs precedent); call
/// sites keep the real format string.
// TODO(pg-port): wire to the real GUC_check_errmsg_string buffer in guc.c.
macro_rules! GUC_check_errdetail {
    ($($arg:tt)*) => {{
        let _detail: std::string::String = format!($($arg)*);
        let _ = _detail;
    }};
}

/// `IsTransactionState()` (access/xact.h): are we inside a transaction able to
/// do catalog access?
// TODO(pg-port): replace with the real xact.c implementation. Stubbed to false
// so the catalog-verification branch is skipped ("accept on faith"), matching
// the documented behavior when not connected to a database.
unsafe fn IsTransactionState() -> bool {
    false
}

/// `MyDatabaseId` (miscadmin.h): the OID of the database we are connected to.
// TODO(pg-port): replace with the real global from globals.c.
#[allow(non_upper_case_globals)]
static mut MyDatabaseId: Oid = InvalidOid;

/// `get_table_am_oid(am_name, missing_ok)` (commands/defrem.h / amcmds.c):
/// look up a table access method's OID by name.
// TODO(pg-port): replace with the real catalog lookup (pg_am scan). Stubbed to
// InvalidOid; the only caller is the stubbed-out catalog branch below.
#[allow(unused_variables)]
unsafe fn get_table_am_oid(am_name: *const c_char, missing_ok: bool) -> Oid { crate::commands::amcmds::get_table_am_oid(am_name, missing_ok) }

/* ----------------------------------------------------------------
 * GetTableAmRoutine
 * ---------------------------------------------------------------- */

/// `GetTableAmRoutine`
///
/// Call the specified access method handler routine to get its
/// `TableAmRoutine` struct, which will be palloc'd in the caller's memory
/// context.
///
/// # Safety
/// `amhandler` must be a valid, registered table-AM handler function OID.
pub unsafe fn GetTableAmRoutine(amhandler: Oid) -> *const TableAmRoutine {
    let datum: Datum = OidFunctionCall0Coll(amhandler, InvalidOid);
    let routine: *const TableAmRoutine = DatumGetPointer(datum) as *const TableAmRoutine;

    if routine.is_null() || nodeTag(routine) != NodeTag::T_TableAmRoutine {
        elog!(
            ERROR,
            "table access method handler {} did not return a TableAmRoutine struct",
            amhandler
        );
        unreachable!();
    }

    // NOTE: upstream asserts here that all ~40 required callbacks are non-NULL
    // (routine->scan_begin, scan_end, ..., scan_sample_next_tuple). Those checks
    // are restored once the full TableAmRoutine struct is ported; the opaque
    // forward declaration used here has no callback fields to inspect.

    routine
}

/* ----------------------------------------------------------------
 * GetHeapamTableAmRoutine
 * ---------------------------------------------------------------- */

/// `GetHeapamTableAmRoutine`
///
/// Get the TableAmRoutine for the heap access method, without going through the
/// handler function.
// STUB: the heapam handler (heapam_methods) and the static heap TableAmRoutine
// live in access/heap/heapam_handler.c, which is not yet ported.
// TODO(pg-port): return &heapam_methods once heapam_handler.c lands.
pub unsafe fn GetHeapamTableAmRoutine() -> *const TableAmRoutine { unimplemented!() }

/* ----------------------------------------------------------------
 * check_default_table_access_method (GUC check hook)
 * ---------------------------------------------------------------- */

/// `check_default_table_access_method` (utils/guc_hooks.h)
///
/// GUC check hook: validate a new value for `default_table_access_method`.
///
/// In C the signature is `bool check_default_table_access_method(char **newval,
/// void **extra, GucSource source)`. We keep `newval`/`extra` as raw pointers
/// (the GUC machinery passes by reference) and `source` typed via the GucSource
/// stub. `newval` points to a NUL-terminated C string (the proposed AM name).
///
/// The empty-string and length validations are REAL; the catalog-verification
/// branch is stubbed (get_table_am_oid / IsTransactionState / MyDatabaseId are
/// not yet ported).
///
/// # Safety
/// `*newval` must be a valid, NUL-terminated C string pointer.
#[allow(unused_variables)]
pub unsafe fn check_default_table_access_method(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    let s: *const c_char = *newval;

    // **newval == '\0' : reject an empty string.
    if *s == 0 {
        GUC_check_errdetail!("\"{}\" cannot be empty.", "default_table_access_method");
        return false;
    }

    // strlen(*newval) >= NAMEDATALEN : reject an over-long name.
    if c_strlen(s) >= NAMEDATALEN {
        GUC_check_errdetail!(
            "\"{}\" is too long (maximum {} characters).",
            "default_table_access_method",
            NAMEDATALEN - 1
        );
        return false;
    }

    // If we aren't inside a transaction, or not connected to a database, we
    // cannot do the catalog access necessary to verify the method. Must accept
    // the value on faith.
    if IsTransactionState() && MyDatabaseId != InvalidOid {
        // STUB: catalog verification of the AM name.
        // TODO(pg-port): once amcmds.c is ported, restore:
        //   if (!OidIsValid(get_table_am_oid(*newval, true))) {
        //       if (source == PGC_S_TEST) { ereport(NOTICE, ...); }
        //       else { GUC_check_errdetail("... does not exist."); return false; }
        //   }
        if !OidIsValid(get_table_am_oid(s, true)) {
            if source == PGC_S_TEST {
                ereport!(
                    NOTICE,
                    errmsg!("table access method \"{:?}\" does not exist", s)
                );
            } else {
                GUC_check_errdetail!("Table access method \"{:?}\" does not exist.", s);
                return false;
            }
        }
    }

    true
}

/// `strlen(3)` over a NUL-terminated C string, returning the count of bytes
/// before the terminator. Kept local to avoid pulling in a libc dependency.
///
/// # Safety
/// `s` must be a valid pointer to a NUL-terminated byte string.
unsafe fn c_strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    // The empty-string validation is the REAL, catalog-free path: a value whose
    // first byte is NUL must be rejected.
    #[test]
    fn rejects_empty_string() {
        let mut buf: [c_char; 1] = [0]; // "" -> first byte is the NUL terminator
        let mut ptr: *mut c_char = buf.as_mut_ptr();
        let newval: *mut *mut c_char = &mut ptr;
        let mut extra_storage: *mut c_void = null_mut();
        let extra: *mut *mut c_void = &mut extra_storage;

        let ok = unsafe { check_default_table_access_method(newval, extra, 0 as GucSource) };
        assert!(!ok, "empty default_table_access_method must be rejected");
    }

    // A short, non-empty name passes the length checks. IsTransactionState() is
    // stubbed to false, so the catalog branch is skipped and the hook accepts.
    #[test]
    fn accepts_short_name() {
        // "heap\0"
        let mut buf: [c_char; 5] = [b'h' as c_char, b'e' as c_char, b'a' as c_char, b'p' as c_char, 0];
        let mut ptr: *mut c_char = buf.as_mut_ptr();
        let newval: *mut *mut c_char = &mut ptr;
        let mut extra_storage: *mut c_void = null_mut();
        let extra: *mut *mut c_void = &mut extra_storage;

        let ok = unsafe { check_default_table_access_method(newval, extra, 0 as GucSource) };
        assert!(ok, "a short non-empty AM name must be accepted on faith");
    }

    // strlen >= NAMEDATALEN must be rejected by the length check.
    #[test]
    fn rejects_too_long_name() {
        // NAMEDATALEN non-NUL bytes followed by a terminator -> strlen == NAMEDATALEN.
        let mut buf: Vec<c_char> = vec![b'a' as c_char; NAMEDATALEN];
        buf.push(0);
        let mut ptr: *mut c_char = buf.as_mut_ptr();
        let newval: *mut *mut c_char = &mut ptr;
        let mut extra_storage: *mut c_void = null_mut();
        let extra: *mut *mut c_void = &mut extra_storage;

        let ok = unsafe { check_default_table_access_method(newval, extra, 0 as GucSource) };
        assert!(!ok, "an over-long AM name must be rejected");
    }
}
