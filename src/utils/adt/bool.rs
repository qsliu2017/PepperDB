//! Translation of postgres/src/backend/utils/adt/bool.c
//!
//! Functions for the built-in type "bool".
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! The .c does:
//!   #include "postgres.h"
//!   #include <ctype.h>
//!   #include "common/hashfn.h"
//!   #include "libpq/pqformat.h"
//!   #include "utils/builtins.h"
//!
//! `postgres.h` -> crate::prelude.  `common/hashfn.h` -> crate::common::hashfn.
//! `<ctype.h>`'s `isspace` is bound directly via `extern "C"` (same convention as
//! parser/scansup.rs), since boolin trims locale-defined whitespace.  The
//! `libpq/pqformat.h` StringInfo serializers (pq_getmsgbyte / pq_begintypsend /
//! pq_sendbyte / pq_endtypsend) are NOT yet translated, so boolrecv/boolsend are
//! stubbed.  `utils/builtins.h`'s `cstring_to_text` (varlena.c) is likewise not
//! yet translated, so booltext is stubbed.  The aggregate support routine
//! `AggCheckCallContext` (executor/nodeAgg.c) is not yet translated, so
//! makeBoolAggState is stubbed.

use crate::prelude::*; // Datum, bool, DatumGetBool/BoolGetDatum, c_char/c_int, palloc, elog!, ereport!, errmsg!
use crate::utils::fmgr::*; // FunctionCallInfo (and the rest of the fmgr.h interface)
// The PG_GETARG_*!/PG_RETURN_*! helpers are #[macro_export] macro_rules! in
// utils/fmgr.rs, so they live at the crate root and must be imported by name
// (a glob `use crate::utils::fmgr::*` does NOT bring exported macros into scope).
use crate::{
    PG_ARGISNULL, PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT64, PG_GETARG_POINTER,
    PG_RETURN_BOOL, PG_RETURN_CSTRING, PG_RETURN_NULL, PG_RETURN_POINTER,
};
use crate::port::pgstrcasecmp::pg_strncasecmp; // boolin/parse_bool use the case-insensitive compare
use crate::common::hashfn::{hash_uint32, hash_uint32_extended}; // common/hashfn.h
use crate::lib::stringinfo::StringInfo; // libpq/pqformat.h passes a StringInfo
use crate::libpq::pqformat::pq_getmsgbyte;
use crate::utils::palloc::{MemoryContext, MemoryContextAlloc}; // utils/palloc.h (agg-context alloc)
use core::ffi::{c_char, c_int, c_void};

/* errcodes.h classification (errcode() shim ignores the value) */
// ERRCODE_INVALID_TEXT_REPRESENTATION from utils/errcodes.h: MAKE_SQLSTATE 22P02.
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 33685634;

// ----------------------------------------------------------------
//   <ctype.h> binding
// ----------------------------------------------------------------
//
// boolin trims leading/trailing whitespace with the locale-aware isspace(),
// exactly as the C does via `isspace((unsigned char) *str)`.  libc's isspace
// takes and returns `int`, operating on a value representable as `unsigned char`.
extern "C" {
    fn isspace(ch: c_int) -> c_int;
}

/*
 * Private strlen for the `*const c_char` C strings handled here (C uses libc's
 * strlen via string.h, included by postgres.h).  Counts bytes up to the NUL.
 *
 * # Safety
 * `s` must point to a valid NUL-terminated C string.
 */
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * Try to interpret value as boolean value.  Valid values are: true,
 * false, yes, no, on, off, 1, 0; as well as unique prefixes thereof.
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 */
//
// # Safety
// `value` must point to a valid NUL-terminated C string; `result`, if non-null,
// must point to a writable `bool`.
pub unsafe fn parse_bool(value: *const c_char, result: *mut bool) -> bool {
    parse_bool_with_len(value, strlen(value), result)
}

//
// # Safety
// `value` must point to at least `len` readable bytes; `result`, if non-null,
// must point to a writable `bool`.
pub unsafe fn parse_bool_with_len(value: *const c_char, len: usize, result: *mut bool) -> bool {
    /* Check the most-used possibilities first. */
    match *value as u8 {
        b't' | b'T' => {
            if pg_strncasecmp(value, c"true".as_ptr(), len) == 0 {
                if !result.is_null() {
                    *result = true;
                }
                return true;
            }
        }
        b'f' | b'F' => {
            if pg_strncasecmp(value, c"false".as_ptr(), len) == 0 {
                if !result.is_null() {
                    *result = false;
                }
                return true;
            }
        }
        b'y' | b'Y' => {
            if pg_strncasecmp(value, c"yes".as_ptr(), len) == 0 {
                if !result.is_null() {
                    *result = true;
                }
                return true;
            }
        }
        b'n' | b'N' => {
            if pg_strncasecmp(value, c"no".as_ptr(), len) == 0 {
                if !result.is_null() {
                    *result = false;
                }
                return true;
            }
        }
        b'o' | b'O' => {
            /* 'o' is not unique enough */
            if pg_strncasecmp(value, c"on".as_ptr(), if len > 2 { len } else { 2 }) == 0 {
                if !result.is_null() {
                    *result = true;
                }
                return true;
            } else if pg_strncasecmp(value, c"off".as_ptr(), if len > 2 { len } else { 2 }) == 0 {
                if !result.is_null() {
                    *result = false;
                }
                return true;
            }
        }
        b'1' => {
            if len == 1 {
                if !result.is_null() {
                    *result = true;
                }
                return true;
            }
        }
        b'0' => {
            if len == 1 {
                if !result.is_null() {
                    *result = false;
                }
                return true;
            }
        }
        _ => {}
    }

    if !result.is_null() {
        *result = false; /* suppress compiler warning */
    }
    false
}

/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/

/*
 *		boolin			- input function for type boolean
 */
pub unsafe fn boolin(fcinfo: FunctionCallInfo) -> Datum {
    let in_str: *const c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let mut str: *const c_char;
    let mut len: usize;
    let mut result: bool = false;

    /*
     * Skip leading and trailing whitespace
     */
    str = in_str;
    while isspace(*str as u8 as c_int) != 0 {
        str = str.add(1);
    }

    len = strlen(str);
    while len > 0 && isspace(*str.add(len - 1) as u8 as c_int) != 0 {
        len -= 1;
    }

    if parse_bool_with_len(str, len, &mut result) {
        PG_RETURN_BOOL!(result);
    }

    /* ereturn(fcinfo->context, (Datum) 0, ...): soft error if escontext is a sink. */
    let escontext = (*fcinfo).context;
    const T_ErrorSaveContext: c_int = 447;
    if !escontext.is_null() && *(escontext as *const c_int) == T_ErrorSaveContext {
        let esc = escontext as *mut crate::nodes::miscnodes::ErrorSaveContext;
        (*esc).error_occurred = true;
        if (*esc).details_wanted {
            let s = format!(
                "invalid input syntax for type {}: \"{}\"",
                "boolean",
                cstring_display(in_str)
            );
            let m = palloc(s.len() + 1) as *mut c_char;
            core::ptr::copy_nonoverlapping(s.as_ptr() as *const c_char, m, s.len());
            *m.add(s.len()) = 0;
            let ed = crate::utils::mmgr::mcxt::palloc0(
                core::mem::size_of::<crate::utils::error::elog_impl::ErrorData>(),
            ) as *mut crate::utils::error::elog_impl::ErrorData;
            (*ed).sqlerrcode = ERRCODE_INVALID_TEXT_REPRESENTATION;
            (*ed).message = m;
            (*esc).error_data = ed as *mut _;
        }
        return 0 as Datum;
    }

    let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
    ereport!(
        ERROR,
        errmsg!(
            "invalid input syntax for type {}: \"{}\"",
            "boolean",
            cstring_display(in_str)
        )
    );
    0 as Datum /* (Datum) 0 */
}

/*
 *		boolout			- converts 1 or 0 to "t" or "f"
 */
pub unsafe fn boolout(fcinfo: FunctionCallInfo) -> Datum {
    let b: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let result: *mut c_char = palloc(2) as *mut c_char;

    *result.add(0) = if b { b't' } else { b'f' } as c_char;
    *result.add(1) = b'\0' as c_char;
    PG_RETURN_CSTRING!(result);
}

/*
 *		boolrecv			- converts external binary format to bool
 *
 * The external representation is one byte.  Any nonzero value is taken
 * as "true".
 */
pub unsafe fn boolrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    let ext: c_int = pq_getmsgbyte(buf);
    PG_RETURN_BOOL!(ext != 0);
}

/*
 *		boolsend			- converts bool to binary format
 */
pub unsafe fn boolsend(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: bool = PG_GETARG_BOOL!(fcinfo, 0);

    // C body:
    //   StringInfoData buf;
    //   pq_begintypsend(&buf);
    //   pq_sendbyte(&buf, arg1 ? 1 : 0);
    //   PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
    // TODO(pg-port): libpq pqformat (pq_begintypsend / pq_sendbyte /
    // pq_endtypsend) not yet translated.
    let _ = arg1;
    unimplemented!("boolsend: libpq/pqformat (pq_sendbyte) not yet translated")
}

/*
 *		booltext			- cast function for bool => text
 *
 * We need this because it's different from the behavior of boolout();
 * this function follows the SQL-spec result (except for producing lower case)
 */
pub unsafe fn booltext(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let str: *const c_char;

    if arg1 {
        str = c"true".as_ptr();
    } else {
        str = c"false".as_ptr();
    }

    // PG_RETURN_TEXT_P(cstring_to_text(str));
    return crate::postgres::PointerGetDatum(
        crate::utils::adt::varlena::cstring_to_text(str) as *const c_void
    );
}


/*****************************************************************************
 *	 PUBLIC ROUTINES														 *
 *****************************************************************************/

pub unsafe fn booleq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let arg2: bool = PG_GETARG_BOOL!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 == arg2);
}

pub unsafe fn boolne(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let arg2: bool = PG_GETARG_BOOL!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 != arg2);
}

pub unsafe fn boollt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let arg2: bool = PG_GETARG_BOOL!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 < arg2);
}

pub unsafe fn boolgt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let arg2: bool = PG_GETARG_BOOL!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 > arg2);
}

pub unsafe fn boolle(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let arg2: bool = PG_GETARG_BOOL!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 <= arg2);
}

pub unsafe fn boolge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let arg2: bool = PG_GETARG_BOOL!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 >= arg2);
}

pub unsafe fn hashbool(fcinfo: FunctionCallInfo) -> Datum {
    // C: return hash_uint32((int32) PG_GETARG_BOOL(0));
    // The int32 cast of the bool then widens to the uint32 parameter.
    hash_uint32(PG_GETARG_BOOL!(fcinfo, 0) as i32 as crate::c::int32 as u32)
}

pub unsafe fn hashboolextended(fcinfo: FunctionCallInfo) -> Datum {
    // C: return hash_uint32_extended((int32) PG_GETARG_BOOL(0), PG_GETARG_INT64(1));
    hash_uint32_extended(
        PG_GETARG_BOOL!(fcinfo, 0) as i32 as crate::c::int32 as u32,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    )
}

/*
 * boolean-and and boolean-or aggregates.
 */

/*
 * Function for standard EVERY aggregate conforming to SQL 2003.
 * The aggregate is also named bool_and for consistency.
 *
 * Note: this is only used in plain aggregate mode, not moving-aggregate mode.
 */
pub unsafe fn booland_statefunc(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_BOOL!(fcinfo, 0) && PG_GETARG_BOOL!(fcinfo, 1));
}

/*
 * Function for standard ANY/SOME aggregate conforming to SQL 2003.
 * The aggregate is named bool_or, because ANY/SOME have parsing conflicts.
 *
 * Note: this is only used in plain aggregate mode, not moving-aggregate mode.
 */
pub unsafe fn boolor_statefunc(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_BOOL!(fcinfo, 0) || PG_GETARG_BOOL!(fcinfo, 1));
}

#[repr(C)]
struct BoolAggState {
    aggcount: crate::c::int64, /* number of non-null values aggregated */
    aggtrue: crate::c::int64,  /* number of values aggregated that are true */
}

unsafe fn makeBoolAggState(fcinfo: FunctionCallInfo) -> *mut BoolAggState {
    let state: *mut BoolAggState;
    let mut agg_context: MemoryContext = std::ptr::null_mut();

    if !AggCheckCallContext(fcinfo, &raw mut agg_context) {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state = MemoryContextAlloc(agg_context, std::mem::size_of::<BoolAggState>())
        as *mut BoolAggState;
    (*state).aggcount = 0;
    (*state).aggtrue = 0;

    state
}

/// STUB: `AggCheckCallContext` (executor/nodeAgg.c).
/// TODO(pg-port): translate executor/nodeAgg.c::AggCheckCallContext.
unsafe fn AggCheckCallContext(
    _fcinfo: FunctionCallInfo,
    _aggcontext: *mut MemoryContext,
) -> bool { crate::executor::nodeAgg::AggCheckCallContext(_fcinfo as _, _aggcontext as _) != 0 }

pub unsafe fn bool_accum(fcinfo: FunctionCallInfo) -> Datum {
    let mut state: *mut BoolAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut BoolAggState
    };

    /* Create the state data on first call */
    if state.is_null() {
        state = makeBoolAggState(fcinfo);
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        (*state).aggcount += 1;
        if PG_GETARG_BOOL!(fcinfo, 1) {
            (*state).aggtrue += 1;
        }
    }

    PG_RETURN_POINTER!(state);
}

pub unsafe fn bool_accum_inv(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut BoolAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut BoolAggState
    };

    /* bool_accum should have created the state data */
    if state.is_null() {
        elog!(ERROR, "bool_accum_inv called with NULL state");
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        (*state).aggcount -= 1;
        if PG_GETARG_BOOL!(fcinfo, 1) {
            (*state).aggtrue -= 1;
        }
    }

    PG_RETURN_POINTER!(state);
}

pub unsafe fn bool_alltrue(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut BoolAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut BoolAggState
    };

    /* if there were no non-null values, return NULL */
    if state.is_null() || (*state).aggcount == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    /* true if all non-null values are true */
    PG_RETURN_BOOL!((*state).aggtrue == (*state).aggcount);
}

pub unsafe fn bool_anytrue(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut BoolAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut BoolAggState
    };

    /* if there were no non-null values, return NULL */
    if state.is_null() || (*state).aggcount == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    /* true if any non-null value is true */
    PG_RETURN_BOOL!((*state).aggtrue > 0);
}

/*
 * Format a `*const c_char` C string for inclusion in an error message via Rust's
 * `{}` formatting (C used the `%s` printf conversion on `in_str`).  Lossily
 * decodes the bytes up to the NUL.
 *
 * # Safety
 * `s` must point to a valid NUL-terminated C string.
 */
unsafe fn cstring_display(s: *const c_char) -> std::string::String {
    let len = strlen(s);
    let bytes = core::slice::from_raw_parts(s as *const u8, len);
    std::string::String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{BoolGetDatum, CStringGetDatum, DatumGetBool, DatumGetCString};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};

    // Drive each SQL function through the real fmgr call path so the
    // fcinfo-threaded PG_GETARG_*!/PG_RETURN_*! macros are exercised end-to-end.
    #[test]
    fn bool_io_and_operators() {
        unsafe {
            // parse_bool: every accepted spelling + a unique prefix + a reject.
            let mut r = false;
            assert!(parse_bool(c"yes".as_ptr(), &mut r) && r);
            assert!(parse_bool(c"off".as_ptr(), &mut r) && !r);
            assert!(parse_bool(c"1".as_ptr(), &mut r) && r);
            assert!(parse_bool(c"0".as_ptr(), &mut r) && !r);
            assert!(parse_bool(c"tru".as_ptr(), &mut r) && r); // unique prefix of "true"
            assert!(!parse_bool(c"xyz".as_ptr(), &mut r)); // not a boolean

            // boolin: trims surrounding whitespace, case-insensitive.
            let d = DirectFunctionCall1Coll(boolin, InvalidOid, CStringGetDatum(c"  TRUE  ".as_ptr()));
            assert!(DatumGetBool(d));
            let d = DirectFunctionCall1Coll(boolin, InvalidOid, CStringGetDatum(c"f".as_ptr()));
            assert!(!DatumGetBool(d));

            // boolout: "t"/"f".
            let s = DatumGetCString(DirectFunctionCall1Coll(boolout, InvalidOid, BoolGetDatum(true)));
            assert_eq!(*s.add(0) as u8, b't');
            assert_eq!(*s.add(1) as u8, 0);
            let s = DatumGetCString(DirectFunctionCall1Coll(boolout, InvalidOid, BoolGetDatum(false)));
            assert_eq!(*s.add(0) as u8, b'f');

            // booleq / boolne / boollt (Rust bool: false < true, mirroring C 0/1).
            let eq = |a, b| DatumGetBool(DirectFunctionCall2Coll(booleq, InvalidOid, BoolGetDatum(a), BoolGetDatum(b)));
            let ne = |a, b| DatumGetBool(DirectFunctionCall2Coll(boolne, InvalidOid, BoolGetDatum(a), BoolGetDatum(b)));
            let lt = |a, b| DatumGetBool(DirectFunctionCall2Coll(boollt, InvalidOid, BoolGetDatum(a), BoolGetDatum(b)));
            assert!(eq(true, true) && eq(false, false) && !eq(true, false));
            assert!(ne(true, false) && !ne(true, true));
            assert!(lt(false, true) && !lt(true, false) && !lt(true, true));
        }
    }
}
