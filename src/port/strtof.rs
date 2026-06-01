//! Translated from PostgreSQL 18.3 `src/port/strtof.c`
//! (declaration in `src/include/port.h`).
//!
//! strtof.c
//!
//! Portions Copyright (c) 2019-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/port/strtof.c
//!
//! Port notes:
//!   - The C source includes <float.h> (FLT_MIN) and <math.h> (isnan/isinf).
//!     FLT_MIN is `f32::MIN_POSITIVE` in Rust; the isnan()/isinf() C macros
//!     become `f32::is_nan()` / `f32::is_infinite()`.
//!   - The C code calls `(strtof)(...)`, where the surrounding parentheses
//!     defeat port.h's `#define strtof(a,b) pg_strtof((a),(b))` macro and thus
//!     invoke the *platform's* strtof().  We bind that platform strtof()
//!     directly via `extern "C"`.
//!   - errno is the per-thread C errno; on macOS/BSD it lives at `*__error()`,
//!     on glibc at `*__errno_location()`.  See the binding below.

#![allow(non_upper_case_globals)]

use crate::prelude::*;

// ----------------------------------------------------------------
//   libc bindings
// ----------------------------------------------------------------
extern "C" {
    // The platform's strtof() / strtod().  In the C source pg_strtof() calls
    // the bare `(strtof)(nptr, &myendptr)` (macro-suppressed) and strtod().
    fn strtof(nptr: *const c_char, endptr: *mut *mut c_char) -> f32;
    fn strtod(nptr: *const c_char, endptr: *mut *mut c_char) -> f64;

    // Per-thread errno location.  macOS/BSD expose it as __error(); glibc uses
    // __errno_location().  Both return `*mut c_int`.
    #[cfg_attr(
        any(target_os = "macos", target_os = "ios", target_vendor = "apple"),
        link_name = "__error"
    )]
    #[cfg_attr(
        not(any(target_os = "macos", target_os = "ios", target_vendor = "apple")),
        link_name = "__errno_location"
    )]
    fn pg_errno_location() -> *mut c_int;
}

/// <errno.h>: ERANGE (Result too large / numerical result out of range).
/// Value 34 on both macOS and Linux.
const ERANGE: c_int = 34;

/// <float.h>: FLT_MIN, the smallest positive normalized `float`.
const FLT_MIN: f32 = f32::MIN_POSITIVE;

/// Read the current C `errno` value.
#[inline]
unsafe fn errno() -> c_int {
    *pg_errno_location()
}

/// Set the current C `errno` value.
#[inline]
unsafe fn set_errno(v: c_int) {
    *pg_errno_location() = v;
}

/*
 * Cygwin has a strtof() which is literally just (float)strtod(), which means
 * we can't avoid the double-rounding problem; but using this wrapper does get
 * us proper over/underflow checks. (Also, if they fix their strtof(), the
 * wrapper doesn't break anything.)
 *
 * Test results on Mingw suggest that it has the same problem, though looking
 * at the code I can't figure out why.
 */
pub unsafe fn pg_strtof(nptr: *const c_char, endptr: *mut *mut c_char) -> f32 {
    let caller_errno: c_int = errno();
    let fresult: f32;
    let mut myendptr: *mut c_char = null_mut();

    set_errno(0);
    fresult = strtof(nptr, &mut myendptr);
    if !endptr.is_null() {
        *endptr = myendptr;
    }
    if errno() != 0 {
        /* On error, just return the error to the caller. */
        return fresult;
    } else if (myendptr == nptr as *mut c_char)
        || fresult.is_nan()
        || ((fresult >= FLT_MIN || fresult <= -FLT_MIN) && !fresult.is_infinite())
    {
        /*
         * If we got nothing parseable, or if we got a non-0 non-subnormal
         * finite value (or NaN) without error, then return that to the caller
         * without error.
         */
        set_errno(caller_errno);
        return fresult;
    } else {
        /*
         * Try again.  errno is already 0 here, and we assume that the endptr
         * won't be any different.
         */
        let dresult: f64 = strtod(nptr, null_mut());

        if errno() != 0 {
            /* On error, just return the error */
            return fresult;
        } else if (dresult == 0.0 && fresult == 0.0)
            || (dresult.is_infinite() && fresult.is_infinite() && (fresult as f64 == dresult))
        {
            /* both values are 0 or infinities of the same sign */
            set_errno(caller_errno);
            return fresult;
        } else if (dresult > 0.0 && dresult <= FLT_MIN as f64 && dresult as f32 != 0.0)
            || (dresult < 0.0 && dresult >= -(FLT_MIN as f64) && dresult as f32 != 0.0)
        {
            /* subnormal but nonzero value */
            set_errno(caller_errno);
            return dresult as f32;
        } else {
            set_errno(ERANGE);
            return fresult;
        }
    }
}
