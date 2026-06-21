//! variable.rs
//!   Routines for handling specialized SET variables.
//! Translated 1:1 from postgres/src/backend/commands/variable.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/commands/variable.c

#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;
extern "C" {
    fn isspace(c: c_int) -> c_int;
    fn memcpy(d:*mut c_void,s:*const c_void,n:usize)->*mut c_void;
    fn snprintf(s:*mut c_char,n:usize,f:*const c_char)->c_int;
    fn strcat(d:*mut c_char,s:*const c_char)->*mut c_char;
    fn strchr(s:*const c_char,c:c_int)->*mut c_char;
    fn strcmp(a:*const c_char,b:*const c_char)->c_int;
    fn strcpy(d:*mut c_char,s:*const c_char)->*mut c_char;
    fn strlen(s:*const c_char)->usize;
    fn strtod(s:*const c_char,e:*mut *mut c_char)->f64;
}


// std ffi scalar types are re-exported via prelude (c_char/c_int/c_void/c_long).
use std::ffi::CStr;

/* access/htup_details.h - GETSTRUCT, HeapTupleIsValid, HeapTuple */
use crate::access::htup_details::{HeapTupleIsValid, GETSTRUCT};
/* catalog/pg_authid.h - Form_pg_authid */
use crate::catalog::pg_authid::Form_pg_authid;
/* common/string.h - pg_clean_ascii */
use crate::common::string::pg_clean_ascii;
/* mb/pg_wchar.h - client-encoding helpers */
use crate::mb::pg_wchar::{
    pg_encoding_to_char, pg_valid_client_encoding, GetDatabaseEncodingName, PrepareClientEncoding,
    SetClientEncoding,
};
/* miscadmin.h - DateStyle/DateOrder modes, role/session-auth helpers, AmStartupProcess */
use crate::miscadmin::{
    AmStartupProcess, GetAuthenticatedUserId, GetSessionUserId, GetSessionUserIsSuperuser,
    SetSessionAuthorization, superuser_arg, DATEORDER_DMY, DATEORDER_MDY, DATEORDER_YMD,
    USE_GERMAN_DATES, USE_ISO_DATES, USE_POSTGRES_DATES, USE_SQL_DATES,
};
/* nodes/pg_list.h - List, ListCell, lfirst, list_free */
use crate::nodes::pg_list::{lfirst, list_free, List, ListCell};
/* port/path.h - canonicalize_path */
use crate::port::path::canonicalize_path;
/* port/pgstrcasecmp.h - case-insensitive comparisons */
use crate::port::pgstrcasecmp::{pg_strcasecmp, pg_strncasecmp};
/* postgres.h - Datum constructors */
use crate::postgres::{
    CStringGetDatum, Float8GetDatum, Int32GetDatum, ObjectIdGetDatum, PointerGetDatum,
};
/* postgres_ext.h - Oid, InvalidOid */
use crate::postgres_ext::{InvalidOid, Oid};
/* timegt - pg_tz and timezone lookups */
use crate::pgtime::{
    log_timezone, pg_get_timezone_name, pg_tz, pg_tz_acceptable, pg_tzset, pg_tzset_offset,
    session_timezone,
};
/* utils/acl.h - SysCacheIdentifier AUTHNAME */
use crate::utils::adt::acl::AUTHNAME;
/* utils/adt/date.h - Interval, time-unit constants */
use crate::utils::adt::date::{Interval, SECS_PER_HOUR, USECS_PER_SEC};
/* utils/datetime.h / adt/datetime.c - timezone abbrev table + cache helpers */
use crate::utils::adt::datetime::{
    ClearTimeZoneAbbrevCache, InstallTimeZoneAbbrevs, TimeZoneAbbrevTable,
};
/* utils/fmgrprotos.h - interval_in, setseed via DirectFunctionCall */
use crate::utils::adt::pseudorandomfuncs::setseed;
use crate::utils::adt::timestamp::{interval_in, DatumGetIntervalP};
/* utils/palloc.h - MCXT_ALLOC_NO_OOM */
use crate::utils::palloc::MCXT_ALLOC_NO_OOM;
/* utils/syscache.h - SearchSysCache1, ReleaseSysCache */
use crate::utils::cache::syscache::{ReleaseSysCache, SearchSysCache1};
/* utils/misc/tzparser.h - load_tzoffsets */
use crate::utils::misc::tzparser::load_tzoffsets;
/* DirectFunctionCall macros (utils/fmgr.h) live at crate root via #[macro_export]. */
use crate::{DirectFunctionCall1, DirectFunctionCall3};

/* ----------------------------------------------------------------
 * GUC machinery shims (utils/guc.h, utils/guc_hooks.h)
 *
 * The full GUC core (guc.c) is not yet ported.  We model the pieces this file
 * needs with local stubs so the hook signatures are faithful and call-compatible
 * once guc.c lands.  This mirrors the per-file convention already used across the
 * port (see utils/misc/stack_depth.rs, utils/misc/tzparser.rs).
 * ---------------------------------------------------------------- */

/// Stub for `GucSource` (utils/guc.h).
// TODO(pg-port): replace with the real GucSource enum once utils/guc is ported.
pub type GucSource = c_int;

/// `PGC_S_DEFAULT`, `PGC_S_TEST`, `PGC_S_INTERACTIVE` (utils/guc.h).
// TODO(pg-port): real GucSource enum values live in utils/guc.h.
pub const PGC_S_DEFAULT: GucSource = 0;
pub const PGC_S_TEST: GucSource = 13;
pub const PGC_S_INTERACTIVE: GucSource = 14;

/// `guc_malloc(elevel, size)` (utils/guc.c) - allocates in TopMemoryContext, with
/// failure reported at `elevel` rather than throwing.  Until guc.c is ported we
/// model it as a plain TopMemoryContext allocation that returns NULL on failure.
// TODO(pg-port): real guc_malloc lives in utils/misc/guc.c.
unsafe fn guc_malloc(elevel: c_int, size: Size) -> *mut c_void {
    MemoryContextAllocExtended(TopMemoryContext, size, MCXT_ALLOC_NO_OOM)
}

/// `guc_strdup(elevel, src)` (utils/guc.c).
// TODO(pg-port): real guc_strdup lives in utils/misc/guc.c.
unsafe fn guc_strdup(elevel: c_int, src: *const c_char) -> *mut c_char {
    let len = strlen(src) + 1;
    let result = guc_malloc(elevel, len) as *mut c_char;
    if !result.is_null() {
        memcpy(result as *mut c_void, src as *const c_void, len);
    }
    result
}

/// `guc_free(ptr)` (utils/guc.c).
// TODO(pg-port): real guc_free lives in utils/misc/guc.c.
unsafe fn guc_free(ptr: *mut c_void) {
    if !ptr.is_null() {
        pfree(ptr);
    }
}

/// `GetConfigOptionResetString(name)` (utils/guc.c) - returns the reset value of a
/// string GUC.  Stubbed until guc.c is ported.
// TODO(pg-port): real GetConfigOptionResetString lives in utils/misc/guc.c.
unsafe fn GetConfigOptionResetString(name: *const c_char) -> *const c_char {
    null()
}

/*
 * GUC check-hook error helpers (utils/guc.h are macros around
 * GUC_check_errmsg_string / errdetail / errhint / errcode).  Until guc.c is
 * ported these are no-op shims that merely format their argument, matching the
 * convention in utils/misc/stack_depth.rs and utils/misc/tzparser.rs.
 */
// TODO(pg-port): wire to the real GUC_check_err* buffers in guc.c.
macro_rules! GUC_check_errmsg {
    ($($arg:tt)*) => {{
        let _msg: String = format!($($arg)*);
        let _ = _msg;
    }};
}
macro_rules! GUC_check_errdetail {
    ($($arg:tt)*) => {{
        let _detail: String = format!($($arg)*);
        let _ = _detail;
    }};
}
macro_rules! GUC_check_errhint {
    ($($arg:tt)*) => {{
        let _hint: String = format!($($arg)*);
        let _ = _hint;
    }};
}
macro_rules! GUC_check_errcode {
    ($code:expr) => {{
        let _code: c_int = $code;
        let _ = _code;
    }};
}

/* ----------------------------------------------------------------
 * SQLSTATE error codes (utils/errcodes.h)
 *
 * Stubbed locally (as 0) matching the per-file convention used elsewhere in the
 * port until utils/errcodes is generated.
 * ---------------------------------------------------------------- */
// TODO(pg-port): real ERRCODE_* values live in utils/errcodes.h.
const ERRCODE_ACTIVE_SQL_TRANSACTION: c_int = 0;
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;
const ERRCODE_INVALID_TRANSACTION_STATE: c_int = 0;

/* ----------------------------------------------------------------
 * Transaction / xact state (access/xact.h)
 *
 * Not yet ported; modeled with local stubs.  XactReadOnly/XactIsoLevel are
 * session globals; the predicates report a not-in-transaction state, which is the
 * conservative "allow the change" path in these hooks.
 * ---------------------------------------------------------------- */
// TODO(pg-port): these really live in access/transam/xact.rs.
#[no_mangle]
pub static mut XactReadOnly: bool = false;
#[no_mangle]
pub static mut XactIsoLevel: c_int = 0;
#[no_mangle]
pub static mut FirstSnapshotSet: bool = false;

/// `XACT_SERIALIZABLE` (access/xact.h).
// TODO(pg-port): real value lives in access/transam/xact.rs.
pub const XACT_SERIALIZABLE: c_int = 3;

unsafe fn IsTransactionState() -> bool {
    false
}
unsafe fn IsSubTransaction() -> bool {
    false
}

/* ----------------------------------------------------------------
 * Parallel-worker state (access/parallel.h)
 * ---------------------------------------------------------------- */
// TODO(pg-port): these really live in access/parallel.rs.
pub static mut InitializingParallelWorker: bool = false;

unsafe fn IsParallelWorker() -> bool {
    false
}

/* ----------------------------------------------------------------
 * WAL / recovery state (access/xlog.h, access/xlogprefetcher.h)
 * ---------------------------------------------------------------- */
// TODO(pg-port): RecoveryInProgress lives in access/transam/xlog.rs.
unsafe fn RecoveryInProgress() -> bool {
    false
}
// TODO(pg-port): XLogPrefetchReconfigure lives in access/transam/xlogprefetcher.rs.
unsafe fn XLogPrefetchReconfigure() {}

/* ----------------------------------------------------------------
 * Role / session-authorization catalog helpers (utils/acl.h, miscadmin.h)
 *
 * member_can_set_role / GetCurrentRoleId / SetCurrentRoleId /
 * current_role_is_superuser / role_string are not yet ported; stub them.
 * ---------------------------------------------------------------- */
// TODO(pg-port): member_can_set_role lives in utils/adt/acl.rs.
unsafe fn member_can_set_role(member: Oid, role: Oid) -> bool {
    false
}
// TODO(pg-port): GetCurrentRoleId/SetCurrentRoleId live in utils/init/miscinit.rs.
unsafe fn GetCurrentRoleId() -> Oid {
    InvalidOid
}
unsafe fn SetCurrentRoleId(roleid: Oid, is_superuser: bool) {}
// TODO(pg-port): current_role_is_superuser is a global in utils/init/miscinit.rs.
#[no_mangle]
pub static mut current_role_is_superuser: bool = false;
// TODO(pg-port): role_string is the GUC variable backing SET ROLE (guc_tables.c).
#[no_mangle]
pub static mut role_string: *mut c_char = null_mut();

/* ----------------------------------------------------------------
 * Backend status reporting (utils/backend_status.h)
 * ---------------------------------------------------------------- */
// TODO(pg-port): pgstat_report_appname lives in utils/activity/backend_status.rs.
unsafe fn pgstat_report_appname(appname: *const c_char) {}

/* ----------------------------------------------------------------
 * I/O concurrency / combine-limit GUC backing variables (storage/bufmgr.h,
 * storage/aio.h)
 * ---------------------------------------------------------------- */
// TODO(pg-port): these live in storage/buffer/bufmgr.rs and storage/aio/aio.rs.
#[no_mangle]
pub static mut maintenance_io_concurrency: c_int = 0;
#[no_mangle]
pub static mut io_combine_limit: c_int = 0;
#[no_mangle]
pub static mut io_combine_limit_guc: c_int = 0;
#[no_mangle]
pub static mut io_max_combine_limit: c_int = 0;

/* ----------------------------------------------------------------
 * Octal-mode show-hook GUC backing variables (utils/guc.h,
 * postmaster/syslogger.h, libpq/pqcomm.h)
 * ---------------------------------------------------------------- */
// TODO(pg-port): data_directory_mode lives in utils/init/globals.rs;
// Log_file_mode in postmaster/syslogger.rs; Unix_socket_permissions in libpq.
#[no_mangle]
pub static mut data_directory_mode: c_int = 0o700;
#[no_mangle]
pub static mut Log_file_mode: c_int = 0o600;
#[no_mangle]
pub static mut Unix_socket_permissions: c_int = 0o777;

/*
 * DATESTYLE
 */

/*
 * check_datestyle: GUC check_hook for datestyle
 */
pub unsafe fn check_datestyle(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    let mut newDateStyle: c_int = DateStyle;
    let mut newDateOrder: c_int = DateOrder;
    let mut have_style: bool = false;
    let mut have_order: bool = false;
    let mut ok: bool = true;
    let rawstring: *mut c_char;
    let mut myextra: *mut c_int;
    let result: *mut c_char;
    let mut elemlist: *mut List = null_mut();
    let l: *mut ListCell;

    /* Need a modifiable copy of string */
    rawstring = pstrdup(*newval);

    /* Parse string into list of identifiers */
    if !SplitIdentifierString(rawstring, b',' as c_char, &raw mut elemlist) {
        /* syntax error in list */
        GUC_check_errdetail!("List syntax is invalid.");
        pfree(rawstring as *mut c_void);
        list_free(elemlist);
        return false;
    }

    foreach!(l, elemlist, {
        let tok: *mut c_char = lfirst(current_cell!(l)) as *mut c_char;

        /* Ugh. Somebody ought to write a table driven version -- mjl */

        if pg_strcasecmp(tok, c"ISO".as_ptr()) == 0 {
            if have_style && newDateStyle != USE_ISO_DATES {
                ok = false; /* conflicting styles */
            }
            newDateStyle = USE_ISO_DATES;
            have_style = true;
        } else if pg_strcasecmp(tok, c"SQL".as_ptr()) == 0 {
            if have_style && newDateStyle != USE_SQL_DATES {
                ok = false; /* conflicting styles */
            }
            newDateStyle = USE_SQL_DATES;
            have_style = true;
        } else if pg_strncasecmp(tok, c"POSTGRES".as_ptr(), 8) == 0 {
            if have_style && newDateStyle != USE_POSTGRES_DATES {
                ok = false; /* conflicting styles */
            }
            newDateStyle = USE_POSTGRES_DATES;
            have_style = true;
        } else if pg_strcasecmp(tok, c"GERMAN".as_ptr()) == 0 {
            if have_style && newDateStyle != USE_GERMAN_DATES {
                ok = false; /* conflicting styles */
            }
            newDateStyle = USE_GERMAN_DATES;
            have_style = true;
            /* GERMAN also sets DMY, unless explicitly overridden */
            if !have_order {
                newDateOrder = DATEORDER_DMY;
            }
        } else if pg_strcasecmp(tok, c"YMD".as_ptr()) == 0 {
            if have_order && newDateOrder != DATEORDER_YMD {
                ok = false; /* conflicting orders */
            }
            newDateOrder = DATEORDER_YMD;
            have_order = true;
        } else if pg_strcasecmp(tok, c"DMY".as_ptr()) == 0
            || pg_strncasecmp(tok, c"EURO".as_ptr(), 4) == 0
        {
            if have_order && newDateOrder != DATEORDER_DMY {
                ok = false; /* conflicting orders */
            }
            newDateOrder = DATEORDER_DMY;
            have_order = true;
        } else if pg_strcasecmp(tok, c"MDY".as_ptr()) == 0
            || pg_strcasecmp(tok, c"US".as_ptr()) == 0
            || pg_strncasecmp(tok, c"NONEURO".as_ptr(), 7) == 0
        {
            if have_order && newDateOrder != DATEORDER_MDY {
                ok = false; /* conflicting orders */
            }
            newDateOrder = DATEORDER_MDY;
            have_order = true;
        } else if pg_strcasecmp(tok, c"DEFAULT".as_ptr()) == 0 {
            /*
             * Easiest way to get the current DEFAULT state is to fetch the
             * DEFAULT string from guc.c and recursively parse it.
             *
             * We can't simply "return check_datestyle(...)" because we need
             * to handle constructs like "DEFAULT, ISO".
             */
            let mut subval: *mut c_char;
            let mut subextra: *mut c_void = null_mut();

            subval = guc_strdup(LOG, GetConfigOptionResetString(c"datestyle".as_ptr()));
            if subval.is_null() {
                ok = false;
                break;
            }
            if !check_datestyle(&raw mut subval, &raw mut subextra, source) {
                guc_free(subval as *mut c_void);
                ok = false;
                break;
            }
            myextra = subextra as *mut c_int;
            if !have_style {
                newDateStyle = *myextra.add(0);
            }
            if !have_order {
                newDateOrder = *myextra.add(1);
            }
            guc_free(subval as *mut c_void);
            guc_free(subextra);
        } else {
            GUC_check_errdetail!(
                "Unrecognized key word: \"{}\".",
                CStr::from_ptr(tok).to_string_lossy()
            );
            pfree(rawstring as *mut c_void);
            list_free(elemlist);
            return false;
        }
    });

    pfree(rawstring as *mut c_void);
    list_free(elemlist);

    if !ok {
        GUC_check_errdetail!("Conflicting \"DateStyle\" specifications.");
        return false;
    }

    /*
     * Prepare the canonical string to return.  GUC wants it guc_malloc'd.
     */
    result = guc_malloc(LOG, 32) as *mut c_char;
    if result.is_null() {
        return false;
    }

    match newDateStyle {
        USE_ISO_DATES => {
            strcpy(result, c"ISO".as_ptr());
        }
        USE_SQL_DATES => {
            strcpy(result, c"SQL".as_ptr());
        }
        USE_GERMAN_DATES => {
            strcpy(result, c"German".as_ptr());
        }
        _ => {
            strcpy(result, c"Postgres".as_ptr());
        }
    }
    match newDateOrder {
        DATEORDER_YMD => {
            strcat(result, c", YMD".as_ptr());
        }
        DATEORDER_DMY => {
            strcat(result, c", DMY".as_ptr());
        }
        _ => {
            strcat(result, c", MDY".as_ptr());
        }
    }

    guc_free(*newval as *mut c_void);
    *newval = result;

    /*
     * Set up the "extra" struct actually used by assign_datestyle.
     */
    myextra = guc_malloc(LOG, 2 * std::mem::size_of::<c_int>()) as *mut c_int;
    if myextra.is_null() {
        return false;
    }
    *myextra.add(0) = newDateStyle;
    *myextra.add(1) = newDateOrder;
    *extra = myextra as *mut c_void;

    true
}

/*
 * assign_datestyle: GUC assign_hook for datestyle
 */
pub unsafe fn assign_datestyle(newval: *const c_char, extra: *mut c_void) {
    let myextra: *mut c_int = extra as *mut c_int;

    DateStyle = *myextra.add(0);
    DateOrder = *myextra.add(1);
}

/*
 * TIMEZONE
 */

/*
 * check_timezone: GUC check_hook for timezone
 */
pub unsafe fn check_timezone(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    let mut new_tz: *mut pg_tz;
    let gmtoffset: c_long;
    let mut endptr: *mut c_char = null_mut();
    let hours: f64;

    if pg_strncasecmp(*newval, c"interval".as_ptr(), 8) == 0 {
        /*
         * Support INTERVAL 'foo'.  This is for SQL spec compliance, not
         * because it has any actual real-world usefulness.
         */
        let mut valueptr: *const c_char = *newval;
        let val: *mut c_char;
        let interval: *mut Interval;

        valueptr = valueptr.add(8);
        while isspace(*valueptr as c_int) != 0 {
            valueptr = valueptr.add(1);
        }
        if {
            let c = *valueptr;
            valueptr = valueptr.add(1);
            c
        } != b'\'' as c_char
        {
            return false;
        }
        val = pstrdup(valueptr);
        /* Check and remove trailing quote */
        endptr = strchr(val, b'\'' as c_int);
        if endptr.is_null() || *endptr.add(1) != 0 {
            pfree(val as *mut c_void);
            return false;
        }
        *endptr = 0;

        /*
         * Try to parse it.  XXX an invalid interval format will result in
         * ereport(ERROR), which is not desirable for GUC.  We did what we
         * could to guard against this in flatten_set_variable_args, but a
         * string coming in from postgresql.conf might contain anything.
         */
        interval = DatumGetIntervalP(DirectFunctionCall3!(
            interval_in,
            CStringGetDatum(val),
            ObjectIdGetDatum(InvalidOid),
            Int32GetDatum(-1)
        ));

        pfree(val as *mut c_void);
        if (*interval).month != 0 {
            GUC_check_errdetail!("Cannot specify months in time zone interval.");
            pfree(interval as *mut c_void);
            return false;
        }
        if (*interval).day != 0 {
            GUC_check_errdetail!("Cannot specify days in time zone interval.");
            pfree(interval as *mut c_void);
            return false;
        }

        /* Here we change from SQL to Unix sign convention */
        gmtoffset = -((*interval).time / USECS_PER_SEC) as c_long;
        new_tz = pg_tzset_offset(gmtoffset);

        pfree(interval as *mut c_void);
    } else {
        /*
         * Try it as a numeric number of hours (possibly fractional).
         */
        hours = strtod(*newval, &raw mut endptr);
        if endptr != *newval && *endptr == 0 {
            /* Here we change from SQL to Unix sign convention */
            gmtoffset = (-hours * SECS_PER_HOUR as f64) as c_long;
            new_tz = pg_tzset_offset(gmtoffset);
        } else {
            /*
             * Otherwise assume it is a timezone name, and try to load it.
             */
            new_tz = pg_tzset(*newval);

            if new_tz.is_null() {
                /* Doesn't seem to be any great value in errdetail here */
                return false;
            }

            if !pg_tz_acceptable(new_tz) {
                GUC_check_errmsg!(
                    "time zone \"{}\" appears to use leap seconds",
                    CStr::from_ptr(*newval).to_string_lossy()
                );
                GUC_check_errdetail!("PostgreSQL does not support leap seconds.");
                return false;
            }
        }
    }

    /* Test for failure in pg_tzset_offset, which we assume is out-of-range */
    if new_tz.is_null() {
        GUC_check_errdetail!("UTC timezone offset is out of range.");
        return false;
    }

    /*
     * Pass back data for assign_timezone to use
     */
    *extra = guc_malloc(LOG, std::mem::size_of::<*mut pg_tz>());
    if (*extra).is_null() {
        return false;
    }
    *(*extra as *mut *mut pg_tz) = new_tz;

    true
}

/*
 * assign_timezone: GUC assign_hook for timezone
 */
pub unsafe fn assign_timezone(newval: *const c_char, extra: *mut c_void) {
    session_timezone = *(extra as *mut *mut pg_tz);
    /* datetime.c's cache of timezone abbrevs may now be obsolete */
    ClearTimeZoneAbbrevCache();
}

/*
 * show_timezone: GUC show_hook for timezone
 */
pub unsafe fn show_timezone() -> *const c_char {
    let tzn: *const c_char;

    /* Always show the zone's canonical name */
    tzn = pg_get_timezone_name(session_timezone);

    if !tzn.is_null() {
        return tzn;
    }

    c"unknown".as_ptr()
}

/*
 * LOG_TIMEZONE
 *
 * For log_timezone, we don't support the interval-based methods of setting a
 * zone, which are only there for SQL spec compliance not because they're
 * actually useful.
 */

/*
 * check_log_timezone: GUC check_hook for log_timezone
 */
pub unsafe fn check_log_timezone(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    let new_tz: *mut pg_tz;

    /*
     * Assume it is a timezone name, and try to load it.
     */
    new_tz = pg_tzset(*newval);

    if new_tz.is_null() {
        /* Doesn't seem to be any great value in errdetail here */
        return false;
    }

    if !pg_tz_acceptable(new_tz) {
        GUC_check_errmsg!(
            "time zone \"{}\" appears to use leap seconds",
            CStr::from_ptr(*newval).to_string_lossy()
        );
        GUC_check_errdetail!("PostgreSQL does not support leap seconds.");
        return false;
    }

    /*
     * Pass back data for assign_log_timezone to use
     */
    *extra = guc_malloc(LOG, std::mem::size_of::<*mut pg_tz>());
    if (*extra).is_null() {
        return false;
    }
    *(*extra as *mut *mut pg_tz) = new_tz;

    true
}

/*
 * assign_log_timezone: GUC assign_hook for log_timezone
 */
pub unsafe fn assign_log_timezone(newval: *const c_char, extra: *mut c_void) {
    log_timezone = *(extra as *mut *mut pg_tz);
}

/*
 * show_log_timezone: GUC show_hook for log_timezone
 */
pub unsafe fn show_log_timezone() -> *const c_char {
    let tzn: *const c_char;

    /* Always show the zone's canonical name */
    tzn = pg_get_timezone_name(log_timezone);

    if !tzn.is_null() {
        return tzn;
    }

    c"unknown".as_ptr()
}

/*
 * TIMEZONE_ABBREVIATIONS
 */

/*
 * GUC check_hook for timezone_abbreviations
 */
pub unsafe fn check_timezone_abbreviations(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    /*
     * The boot_val for timezone_abbreviations is NULL.  When we see that we
     * just do nothing.  If the value isn't overridden from the config file
     * then pg_timezone_abbrev_initialize() will eventually replace it with
     * "Default".  This hack has two purposes: to avoid wasting cycles loading
     * values that might soon be overridden from the config file, and to avoid
     * trying to read the timezone abbrev files during InitializeGUCOptions().
     * The latter doesn't work in an EXEC_BACKEND subprocess because
     * my_exec_path hasn't been set yet and so we can't locate PGSHAREDIR.
     */
    if (*newval).is_null() {
        Assert!(source == PGC_S_DEFAULT);
        return true;
    }

    /* OK, load the file and produce a guc_malloc'd TimeZoneAbbrevTable */
    *extra = load_tzoffsets(*newval) as *mut c_void;

    /* tzparser.c returns NULL on failure, reporting via GUC_check_errmsg */
    if (*extra).is_null() {
        return false;
    }

    true
}

/*
 * GUC assign_hook for timezone_abbreviations
 */
pub unsafe fn assign_timezone_abbreviations(newval: *const c_char, extra: *mut c_void) {
    /* Do nothing for the boot_val default of NULL */
    if extra.is_null() {
        return;
    }

    InstallTimeZoneAbbrevs(extra as *mut TimeZoneAbbrevTable);
}

/*
 * SET TRANSACTION READ ONLY and SET TRANSACTION READ WRITE
 *
 * We allow idempotent changes (r/w -> r/w and r/o -> r/o) at any time, and
 * we also always allow changes from read-write to read-only.  However,
 * read-only may be changed to read-write only when in a top-level transaction
 * that has not yet taken an initial snapshot.  Can't do it in a hot standby,
 * either.
 *
 * If we are not in a transaction at all, just allow the change; it means
 * nothing since XactReadOnly will be reset by the next StartTransaction().
 * The IsTransactionState() test protects us against trying to check
 * RecoveryInProgress() in contexts where shared memory is not accessible.
 * (Similarly, if we're restoring state in a parallel worker, just allow
 * the change.)
 */
pub unsafe fn check_transaction_read_only(
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    if *newval == false && XactReadOnly && IsTransactionState() && !InitializingParallelWorker {
        /* Can't go to r/w mode inside a r/o transaction */
        if IsSubTransaction() {
            GUC_check_errcode!(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg!("cannot set transaction read-write mode inside a read-only transaction");
            return false;
        }
        /* Top level transaction can't change to r/w after first snapshot. */
        if FirstSnapshotSet {
            GUC_check_errcode!(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg!("transaction read-write mode must be set before any query");
            return false;
        }
        /* Can't go to r/w mode while recovery is still active */
        if RecoveryInProgress() {
            GUC_check_errcode!(ERRCODE_FEATURE_NOT_SUPPORTED);
            GUC_check_errmsg!("cannot set transaction read-write mode during recovery");
            return false;
        }
    }

    true
}

/*
 * SET TRANSACTION ISOLATION LEVEL
 *
 * We allow idempotent changes at any time, but otherwise this can only be
 * changed in a toplevel transaction that has not yet taken a snapshot.
 *
 * As in check_transaction_read_only, allow it if not inside a transaction,
 * or if restoring state in a parallel worker.
 */
pub unsafe fn check_transaction_isolation(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    let newXactIsoLevel: c_int = *newval;

    if newXactIsoLevel != XactIsoLevel && IsTransactionState() && !InitializingParallelWorker {
        if FirstSnapshotSet {
            GUC_check_errcode!(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg!("SET TRANSACTION ISOLATION LEVEL must be called before any query");
            return false;
        }
        /* We ignore a subtransaction setting it to the existing value. */
        if IsSubTransaction() {
            GUC_check_errcode!(ERRCODE_ACTIVE_SQL_TRANSACTION);
            GUC_check_errmsg!("SET TRANSACTION ISOLATION LEVEL must not be called in a subtransaction");
            return false;
        }
        /* Can't go to serializable mode while recovery is still active */
        if newXactIsoLevel == XACT_SERIALIZABLE && RecoveryInProgress() {
            GUC_check_errcode!(ERRCODE_FEATURE_NOT_SUPPORTED);
            GUC_check_errmsg!("cannot use serializable mode in a hot standby");
            GUC_check_errhint!("You can use REPEATABLE READ instead.");
            return false;
        }
    }

    true
}

/*
 * SET TRANSACTION [NOT] DEFERRABLE
 */

pub unsafe fn check_transaction_deferrable(
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    /* Just accept the value when restoring state in a parallel worker */
    if InitializingParallelWorker {
        return true;
    }

    if IsSubTransaction() {
        GUC_check_errcode!(ERRCODE_ACTIVE_SQL_TRANSACTION);
        GUC_check_errmsg!("SET TRANSACTION [NOT] DEFERRABLE cannot be called within a subtransaction");
        return false;
    }
    if FirstSnapshotSet {
        GUC_check_errcode!(ERRCODE_ACTIVE_SQL_TRANSACTION);
        GUC_check_errmsg!("SET TRANSACTION [NOT] DEFERRABLE must be called before any query");
        return false;
    }

    true
}

/*
 * Random number seed
 *
 * We can't roll back the random sequence on error, and we don't want
 * config file reloads to affect it, so we only want interactive SET SEED
 * commands to set it.  We use the "extra" storage to ensure that rollbacks
 * don't try to do the operation again.
 */

pub unsafe fn check_random_seed(
    newval: *mut f64,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    *extra = guc_malloc(LOG, std::mem::size_of::<c_int>());
    if (*extra).is_null() {
        return false;
    }
    /* Arm the assign only if source of value is an interactive SET */
    *(*extra as *mut c_int) = (source >= PGC_S_INTERACTIVE) as c_int;

    true
}

pub unsafe fn assign_random_seed(newval: f64, extra: *mut c_void) {
    /* We'll do this at most once for any setting of the GUC variable */
    if *(extra as *mut c_int) != 0 {
        DirectFunctionCall1!(setseed, Float8GetDatum(newval));
    }
    *(extra as *mut c_int) = 0;
}

pub unsafe fn show_random_seed() -> *const c_char {
    c"unavailable".as_ptr()
}

/*
 * SET CLIENT_ENCODING
 */

pub unsafe fn check_client_encoding(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    let encoding: c_int;
    let canonical_name: *const c_char;

    /* Look up the encoding by name */
    encoding = pg_valid_client_encoding(*newval);
    if encoding < 0 {
        return false;
    }

    /* Get the canonical name (no aliases, uniform case) */
    canonical_name = pg_encoding_to_char(encoding);

    /*
     * Parallel workers send data to the leader, not the client.  They always
     * send data using the database encoding; therefore, we should never
     * actually change the client encoding in a parallel worker.  However,
     * during parallel worker startup, we want to accept the leader's
     * client_encoding setting so that anyone who looks at the value in the
     * worker sees the same value that they would see in the leader.  A change
     * other than during startup, for example due to a SET clause attached to
     * a function definition, should be rejected, as there is nothing we can
     * do inside the worker to make it take effect.
     */
    if IsParallelWorker() && !InitializingParallelWorker {
        GUC_check_errcode!(ERRCODE_INVALID_TRANSACTION_STATE);
        GUC_check_errdetail!("Cannot change \"client_encoding\" during a parallel operation.");
        return false;
    }

    /*
     * If we are not within a transaction then PrepareClientEncoding will not
     * be able to look up the necessary conversion procs.  If we are still
     * starting up, it will return "OK" anyway, and InitializeClientEncoding
     * will fix things once initialization is far enough along.  After
     * startup, we'll fail.  This would only happen if someone tries to change
     * client_encoding in postgresql.conf and then SIGHUP existing sessions.
     * It seems like a bad idea for client_encoding to change that way anyhow,
     * so we don't go out of our way to support it.
     *
     * In a parallel worker, we might as well skip PrepareClientEncoding since
     * we're not going to use its results.
     *
     * Note: in the postmaster, or any other process that never calls
     * InitializeClientEncoding, PrepareClientEncoding will always succeed,
     * and so will SetClientEncoding; but they won't do anything, which is OK.
     */
    if !IsParallelWorker() && PrepareClientEncoding(encoding) < 0 {
        if IsTransactionState() {
            /* Must be a genuine no-such-conversion problem */
            GUC_check_errcode!(ERRCODE_FEATURE_NOT_SUPPORTED);
            GUC_check_errdetail!(
                "Conversion between {} and {} is not supported.",
                CStr::from_ptr(canonical_name).to_string_lossy(),
                CStr::from_ptr(GetDatabaseEncodingName()).to_string_lossy()
            );
        } else {
            /* Provide a useful complaint */
            GUC_check_errdetail!("Cannot change \"client_encoding\" now.");
        }
        return false;
    }

    /*
     * Replace the user-supplied string with the encoding's canonical name.
     * This gets rid of aliases and case-folding variations.
     *
     * XXX Although canonicalizing seems like a good idea in the abstract, it
     * breaks pre-9.1 JDBC drivers, which expect that if they send "UNICODE"
     * as the client_encoding setting then it will read back the same way. As
     * a workaround, don't replace the string if it's "UNICODE".  Remove that
     * hack when pre-9.1 JDBC drivers are no longer in use.
     */
    if strcmp(*newval, canonical_name) != 0 && strcmp(*newval, c"UNICODE".as_ptr()) != 0
    {
        guc_free(*newval as *mut c_void);
        *newval = guc_strdup(LOG, canonical_name);
        if (*newval).is_null() {
            return false;
        }
    }

    /*
     * Save the encoding's ID in *extra, for use by assign_client_encoding.
     */
    *extra = guc_malloc(LOG, std::mem::size_of::<c_int>());
    if (*extra).is_null() {
        return false;
    }
    *(*extra as *mut c_int) = encoding;

    true
}

pub unsafe fn assign_client_encoding(newval: *const c_char, extra: *mut c_void) {
    let encoding: c_int = *(extra as *mut c_int);

    /*
     * In a parallel worker, we never override the client encoding that was
     * set by ParallelWorkerMain().
     */
    if IsParallelWorker() {
        return;
    }

    /* We do not expect an error if PrepareClientEncoding succeeded */
    if SetClientEncoding(encoding) < 0 {
        elog!(LOG, "SetClientEncoding({}) failed", encoding);
    }
}

/*
 * SET SESSION AUTHORIZATION
 */

#[repr(C)]
struct role_auth_extra {
    /* This is the "extra" state for both SESSION AUTHORIZATION and ROLE */
    roleid: Oid,
    is_superuser: bool,
}

pub unsafe fn check_session_authorization(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    let roleTup: HeapTuple;
    let roleform: Form_pg_authid;
    let roleid: Oid;
    let is_superuser: bool;
    let myextra: *mut role_auth_extra;

    /* Do nothing for the boot_val default of NULL */
    if (*newval).is_null() {
        return true;
    }

    if InitializingParallelWorker {
        /*
         * In parallel worker initialization, we want to copy the leader's
         * state even if it no longer matches the catalogs. ParallelWorkerMain
         * already installed the correct role OID and superuser state.
         */
        roleid = GetSessionUserId();
        is_superuser = GetSessionUserIsSuperuser();
    } else {
        if !IsTransactionState() {
            /*
             * Can't do catalog lookups, so fail.  The result of this is that
             * session_authorization cannot be set in postgresql.conf, which
             * seems like a good thing anyway, so we don't work hard to avoid
             * it.
             */
            return false;
        }

        /*
         * When source == PGC_S_TEST, we don't throw a hard error for a
         * nonexistent user name or insufficient privileges, only a NOTICE.
         * See comments in guc.h.
         */

        /* Look up the username */
        roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(*newval as *const c_void));
        if !HeapTupleIsValid(roleTup) {
            if source == PGC_S_TEST {
                ereport!(
                    NOTICE,
                    errmsg!(
                        "role \"{}\" does not exist",
                        CStr::from_ptr(*newval).to_string_lossy()
                    )
                );
                return true;
            }
            GUC_check_errmsg!(
                "role \"{}\" does not exist",
                CStr::from_ptr(*newval).to_string_lossy()
            );
            return false;
        }

        roleform = GETSTRUCT(roleTup) as Form_pg_authid;
        roleid = (*roleform).oid;
        is_superuser = (*roleform).rolsuper;

        ReleaseSysCache(roleTup);

        /*
         * Only superusers may SET SESSION AUTHORIZATION a role other than
         * itself. Note that in case of multiple SETs in a single session, the
         * original authenticated user's superuserness is what matters.
         */
        if roleid != GetAuthenticatedUserId() && !superuser_arg(GetAuthenticatedUserId()) {
            if source == PGC_S_TEST {
                ereport!(
                    NOTICE,
                    errmsg!(
                        "permission will be denied to set session authorization \"{}\"",
                        CStr::from_ptr(*newval).to_string_lossy()
                    )
                );
                return true;
            }
            GUC_check_errcode!(ERRCODE_INSUFFICIENT_PRIVILEGE);
            GUC_check_errmsg!(
                "permission denied to set session authorization \"{}\"",
                CStr::from_ptr(*newval).to_string_lossy()
            );
            return false;
        }
    }

    /* Set up "extra" struct for assign_session_authorization to use */
    myextra = guc_malloc(LOG, std::mem::size_of::<role_auth_extra>()) as *mut role_auth_extra;
    if myextra.is_null() {
        return false;
    }
    (*myextra).roleid = roleid;
    (*myextra).is_superuser = is_superuser;
    *extra = myextra as *mut c_void;

    true
}

pub unsafe fn assign_session_authorization(newval: *const c_char, extra: *mut c_void) {
    let myextra: *mut role_auth_extra = extra as *mut role_auth_extra;

    /* Do nothing for the boot_val default of NULL */
    if myextra.is_null() {
        return;
    }

    SetSessionAuthorization((*myextra).roleid, (*myextra).is_superuser);
}

/*
 * SET ROLE
 *
 * The SQL spec requires "SET ROLE NONE" to unset the role, so we hardwire
 * a translation of "none" to InvalidOid.  Otherwise this is much like
 * SET SESSION AUTHORIZATION.
 */

pub unsafe fn check_role(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    let roleTup: HeapTuple;
    let roleid: Oid;
    let is_superuser: bool;
    let myextra: *mut role_auth_extra;
    let roleform: Form_pg_authid;

    if strcmp(*newval, c"none".as_ptr()) == 0 {
        /* hardwired translation */
        roleid = InvalidOid;
        is_superuser = false;
    } else if InitializingParallelWorker {
        /*
         * In parallel worker initialization, we want to copy the leader's
         * state even if it no longer matches the catalogs. ParallelWorkerMain
         * already installed the correct role OID and superuser state.
         */
        roleid = GetCurrentRoleId();
        is_superuser = current_role_is_superuser;
    } else {
        if !IsTransactionState() {
            /*
             * Can't do catalog lookups, so fail.  The result of this is that
             * role cannot be set in postgresql.conf, which seems like a good
             * thing anyway, so we don't work hard to avoid it.
             */
            return false;
        }

        /*
         * When source == PGC_S_TEST, we don't throw a hard error for a
         * nonexistent user name or insufficient privileges, only a NOTICE.
         * See comments in guc.h.
         */

        /* Look up the username */
        roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(*newval as *const c_void));
        if !HeapTupleIsValid(roleTup) {
            if source == PGC_S_TEST {
                ereport!(
                    NOTICE,
                    errmsg!(
                        "role \"{}\" does not exist",
                        CStr::from_ptr(*newval).to_string_lossy()
                    )
                );
                return true;
            }
            GUC_check_errmsg!(
                "role \"{}\" does not exist",
                CStr::from_ptr(*newval).to_string_lossy()
            );
            return false;
        }

        roleform = GETSTRUCT(roleTup) as Form_pg_authid;
        roleid = (*roleform).oid;
        is_superuser = (*roleform).rolsuper;

        ReleaseSysCache(roleTup);

        /* Verify that session user is allowed to become this role */
        if !member_can_set_role(GetSessionUserId(), roleid) {
            if source == PGC_S_TEST {
                ereport!(
                    NOTICE,
                    errmsg!(
                        "permission will be denied to set role \"{}\"",
                        CStr::from_ptr(*newval).to_string_lossy()
                    )
                );
                return true;
            }
            GUC_check_errcode!(ERRCODE_INSUFFICIENT_PRIVILEGE);
            GUC_check_errmsg!(
                "permission denied to set role \"{}\"",
                CStr::from_ptr(*newval).to_string_lossy()
            );
            return false;
        }
    }

    /* Set up "extra" struct for assign_role to use */
    myextra = guc_malloc(LOG, std::mem::size_of::<role_auth_extra>()) as *mut role_auth_extra;
    if myextra.is_null() {
        return false;
    }
    (*myextra).roleid = roleid;
    (*myextra).is_superuser = is_superuser;
    *extra = myextra as *mut c_void;

    true
}

pub unsafe fn assign_role(newval: *const c_char, extra: *mut c_void) {
    let myextra: *mut role_auth_extra = extra as *mut role_auth_extra;

    SetCurrentRoleId((*myextra).roleid, (*myextra).is_superuser);
}

pub unsafe fn show_role() -> *const c_char {
    /*
     * Check whether SET ROLE is active; if not return "none".  This is a
     * kluge to deal with the fact that SET SESSION AUTHORIZATION logically
     * resets SET ROLE to NONE, but we cannot set the GUC role variable from
     * assign_session_authorization (because we haven't got enough info to
     * call set_config_option).
     */
    if !OidIsValid(GetCurrentRoleId()) {
        return c"none".as_ptr();
    }

    /* Otherwise we can just use the GUC string */
    if !role_string.is_null() {
        role_string
    } else {
        c"none".as_ptr()
    }
}

/*
 * PATH VARIABLES
 *
 * check_canonical_path is used for log_directory and some other GUCs where
 * all we want to do is canonicalize the represented path name.
 */

pub unsafe fn check_canonical_path(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    /*
     * Since canonicalize_path never enlarges the string, we can just modify
     * newval in-place.  But watch out for NULL, which is the default value
     * for external_pid_file.
     */
    if !(*newval).is_null() {
        canonicalize_path(*newval);
    }
    true
}

/*
 * MISCELLANEOUS
 */

/*
 * GUC check_hook for application_name
 */
pub unsafe fn check_application_name(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    let clean: *mut c_char;
    let ret: *mut c_char;

    /* Only allow clean ASCII chars in the application name */
    clean = pg_clean_ascii(*newval, MCXT_ALLOC_NO_OOM);
    if clean.is_null() {
        return false;
    }

    ret = guc_strdup(LOG, clean);
    if ret.is_null() {
        pfree(clean as *mut c_void);
        return false;
    }

    guc_free(*newval as *mut c_void);

    pfree(clean as *mut c_void);
    *newval = ret;
    true
}

/*
 * GUC assign_hook for application_name
 */
pub unsafe fn assign_application_name(newval: *const c_char, extra: *mut c_void) {
    /* Update the pg_stat_activity view */
    pgstat_report_appname(newval);
}

/*
 * GUC check_hook for cluster_name
 */
pub unsafe fn check_cluster_name(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    let clean: *mut c_char;
    let ret: *mut c_char;

    /* Only allow clean ASCII chars in the cluster name */
    clean = pg_clean_ascii(*newval, MCXT_ALLOC_NO_OOM);
    if clean.is_null() {
        return false;
    }

    ret = guc_strdup(LOG, clean);
    if ret.is_null() {
        pfree(clean as *mut c_void);
        return false;
    }

    guc_free(*newval as *mut c_void);

    pfree(clean as *mut c_void);
    *newval = ret;
    true
}

/*
 * GUC assign_hook for maintenance_io_concurrency
 */
pub unsafe fn assign_maintenance_io_concurrency(newval: c_int, extra: *mut c_void) {
    /*
     * Reconfigure recovery prefetching, because a setting it depends on
     * changed.
     */
    maintenance_io_concurrency = newval;
    if AmStartupProcess() {
        XLogPrefetchReconfigure();
    }
}

/*
 * GUC assign hooks that recompute io_combine_limit whenever
 * io_combine_limit_guc and io_max_combine_limit are changed.  These are needed
 * because the GUC subsystem doesn't support dependencies between GUCs, and
 * they may be assigned in either order.
 */
pub unsafe fn assign_io_max_combine_limit(newval: c_int, extra: *mut c_void) {
    io_combine_limit = Min!(newval, io_combine_limit_guc);
}
pub unsafe fn assign_io_combine_limit(newval: c_int, extra: *mut c_void) {
    io_combine_limit = Min!(io_max_combine_limit, newval);
}

/*
 * These show hooks just exist because we want to show the values in octal.
 */

/*
 * GUC show_hook for data_directory_mode
 */
pub unsafe fn show_data_directory_mode() -> *const c_char {
    static mut buf: [c_char; 12] = [0; 12];

    snprintf(
        buf.as_mut_ptr(),
        std::mem::size_of_val(&buf),
        c"%04o".as_ptr(),
        data_directory_mode,
    );
    buf.as_ptr()
}

/*
 * GUC show_hook for log_file_mode
 */
pub unsafe fn show_log_file_mode() -> *const c_char {
    static mut buf: [c_char; 12] = [0; 12];

    snprintf(
        buf.as_mut_ptr(),
        std::mem::size_of_val(&buf),
        c"%04o".as_ptr(),
        Log_file_mode,
    );
    buf.as_ptr()
}

/*
 * GUC show_hook for unix_socket_permissions
 */
pub unsafe fn show_unix_socket_permissions() -> *const c_char {
    static mut buf: [c_char; 12] = [0; 12];

    snprintf(
        buf.as_mut_ptr(),
        std::mem::size_of_val(&buf),
        c"%04o".as_ptr(),
        Unix_socket_permissions,
    );
    buf.as_ptr()
}

/*
 * These check hooks do nothing more than reject non-default settings
 * in builds that don't support them.
 */

pub unsafe fn check_bonjour(
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    // #ifndef USE_BONJOUR
    if *newval {
        GUC_check_errmsg!("Bonjour is not supported by this build");
        return false;
    }
    // #endif
    true
}

pub unsafe fn check_default_with_oids(
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    if *newval {
        /* check the GUC's definition for an explanation */
        GUC_check_errcode!(ERRCODE_FEATURE_NOT_SUPPORTED);
        GUC_check_errmsg!("tables declared WITH OIDS are not supported");

        return false;
    }

    true
}

pub unsafe fn check_ssl(
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    // #ifndef USE_SSL
    if *newval {
        GUC_check_errmsg!("SSL is not supported by this build");
        return false;
    }
    // #endif
    true
}

/* ----------------------------------------------------------------
 * Local helpers / forward-declared symbols not yet ported
 * ---------------------------------------------------------------- */

/// `Min(a, b)` (c.h).
// TODO(pg-port): real Min macro lives in c.rs.
macro_rules! Min {
    ($a:expr, $b:expr) => {{
        let a = $a;
        let b = $b;
        if a < b {
            a
        } else {
            b
        }
    }};
}
use Min;

/// `SplitIdentifierString(rawstring, separator, namelist)` (utils/adt/varlena.c).
/// Not yet ported in utils/adt/varlena.rs; stub returns failure so callers fall
/// through to their error path.
// TODO(pg-port): real SplitIdentifierString lives in utils/adt/varlena.rs.
unsafe fn SplitIdentifierString(
    rawstring: *mut c_char,
    separator: c_char,
    namelist: *mut *mut List,
) -> bool {
    *namelist = null_mut();
    false
}

/// `HeapTuple` (access/htup.h) - pointer to a HeapTupleData.
// TODO(pg-port): canonical HeapTuple typedef lives in access/htup.rs.
type HeapTuple = crate::access::htup_details::HeapTuple;
