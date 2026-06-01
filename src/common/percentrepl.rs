//! Translation of postgres/src/include/common/percentrepl.h
//!                + postgres/src/common/percentrepl.c
//!
//! Common routines to replace percent placeholders in strings.
//!
//! Note on the variadic API: C declares
//! `replace_percent_placeholders(instr, param_name, letters, ...)` and pulls one
//! `char *` value per letter out of a `va_list`. Rust cannot express C varargs
//! directly (the same situation the stringinfo translation calls out for
//! `appendStringInfo`), so the trailing `...` is replaced by a `values` slice of
//! `*const c_char`: `values[i]` corresponds to `letters[i]`. A NULL value is
//! represented by a null pointer, exactly as a NULL `char *` would be in C, and
//! is treated as if an unsupported placeholder was used.
//!
//! Only the BACKEND (`#ifndef FRONTEND`) error path is translated here; the
//! FRONTEND `pg_log_error`/`exit(1)` path is left as a TODO.

use crate::prelude::*;
use crate::lib::stringinfo::{
    appendStringInfoChar, appendStringInfoString, initStringInfo, StringInfoData,
};

/// `ERRCODE_INVALID_PARAMETER_VALUE`: SQLSTATE class 22 (data exception),
/// subclass 023. Encoded with PostgreSQL's MAKE_SQLSTATE packing of the five
/// SQLSTATE characters into the low 6 bits of each of five bytes. The
/// `errcode()` shim ignores the value, but it is kept for fidelity.
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = (('2' as c_int - '0' as c_int) << 0)
    | (('2' as c_int - '0' as c_int) << 6)
    | (('0' as c_int - '0' as c_int) << 12)
    | (('2' as c_int - '0' as c_int) << 18)
    | (('3' as c_int - '0' as c_int) << 24);

/// `strlen` over a C string (mirrors libc strlen for the port).
///
/// # Safety
/// `s` must point to a valid NUL-terminated C string.
#[inline]
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * replace_percent_placeholders
 *
 * Replace percent-letter placeholders in input string with the supplied
 * values.  For example, to replace %f with foo and %b with bar, call
 *
 * replace_percent_placeholders(instr, "param_name", "bf", bar, foo);
 *
 * The return value is palloc'd.
 *
 * "%%" is replaced by a single "%".
 *
 * This throws an error for an unsupported placeholder or a "%" at the end of
 * the input string.
 *
 * A value may be NULL.  If the corresponding placeholder is found in the
 * input string, it will be treated as if an unsupported placeholder was used.
 * This allows callers to share a "letters" specification but vary the
 * actually supported placeholders at run time.
 *
 * This functions is meant for cases where all the values are readily
 * available or cheap to compute and most invocations will use most values
 * (for example for archive_command).  Also, it requires that all values are
 * strings.  It won't be a good match for things like log prefixes or prompts
 * that use a mix of data types and any invocation will only use a few of the
 * possible values.
 *
 * param_name is the name of the underlying GUC parameter, for error
 * reporting.  At the moment, this function is only used for GUC parameters.
 * If other kinds of uses were added, the error reporting would need to be
 * revised.
 *
 * # Safety
 * `instr`, `param_name`, and `letters` must be valid NUL-terminated C strings.
 * Each non-null entry of `values` must be a valid NUL-terminated C string and
 * remain valid for the duration of the call; `values` must have at least as
 * many entries as `letters` has characters. The returned pointer is palloc'd.
 */
pub unsafe fn replace_percent_placeholders(
    instr: *const c_char,
    param_name: *const c_char,
    letters: *const c_char,
    values: &[*const c_char],
) -> *mut c_char {
    let mut result = StringInfoData {
        data: core::ptr::null_mut(),
        len: 0,
        maxlen: 0,
        cursor: 0,
    };

    initStringInfo(&mut result);

    let mut sp = instr;
    while *sp != 0 {
        if *sp == b'%' as c_char {
            if *sp.add(1) == b'%' as c_char {
                /* Convert %% to a single % */
                sp = sp.add(1);
                appendStringInfoChar(&mut result, *sp);
            } else if *sp.add(1) == b'\0' as c_char {
                /* Incomplete escape sequence, expected a character afterward */
                // TODO(pg-port): FRONTEND path uses pg_log_error/exit(1).
                let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                // errmsg + errdetail folded into the single-message shim.
                elog!(
                    ERROR,
                    "invalid value for parameter \"{}\": \"{}\": String ends unexpectedly after escape character \"%\".",
                    cstr_to_string(param_name),
                    cstr_to_string(instr)
                );
            } else {
                /* Look up placeholder character */
                let mut found = false;

                sp = sp.add(1);

                let mut lp = letters;
                let mut idx = 0usize;
                while *lp != 0 {
                    let val = values[idx];

                    if *sp == *lp {
                        if !val.is_null() {
                            appendStringInfoString(&mut result, val);
                            found = true;
                        }
                        /* If val is NULL, we will report an error. */
                        break;
                    }
                    lp = lp.add(1);
                    idx += 1;
                }
                if !found {
                    /* Unknown placeholder */
                    // TODO(pg-port): FRONTEND path uses pg_log_error/exit(1).
                    let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                    // errmsg + errdetail folded into the single-message shim.
                    elog!(
                        ERROR,
                        "invalid value for parameter \"{}\": \"{}\": String contains unexpected placeholder \"%{}\".",
                        cstr_to_string(param_name),
                        cstr_to_string(instr),
                        (*sp as u8) as char
                    );
                }
            }
        } else {
            appendStringInfoChar(&mut result, *sp);
        }

        sp = sp.add(1);
    }

    result.data
}

/// Render a NUL-terminated C string into a Rust `String` for error messages
/// (lossy on non-UTF-8). Not part of the C source; used only by the error paths.
///
/// # Safety
/// `s` must be a valid NUL-terminated C string.
unsafe fn cstr_to_string(s: *const c_char) -> String {
    let bytes = core::slice::from_raw_parts(s as *const u8, strlen(s));
    String::from_utf8_lossy(bytes).into_owned()
}
