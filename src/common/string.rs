//! Translation of postgres/src/include/common/string.h
//!                + postgres/src/common/string.c
//!
//! String handling helpers.
//!
//! The .c begins with:
//!     #ifndef FRONTEND
//!     #include "postgres.h"
//!     #else
//!     #include "postgres_fe.h"
//!     #endif
//!     #include "common/string.h"
//! We translate the BACKEND path (the `#ifndef FRONTEND` branch): `pg_clean_ascii`
//! allocates with `palloc_extended` rather than the frontend's `malloc`.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::utils::palloc::palloc_extended;
use core::ffi::{c_char, c_int, c_long};

// ----------------------------------------------------------------
//   Declarations from common/string.h
// ----------------------------------------------------------------

/*
 * typedef struct PromptInterruptContext
 *
 * Carried by the prompt/get-line helpers (pg_get_line, simple_prompt) so a
 * SIGINT can longjmp out of a blocking read.  Those functions live in
 * pg_get_line.c / sprompt.c and are not part of this translation unit, but the
 * struct is declared in string.h, so we provide it here for fidelity.
 *
 *   void           *jmpbuf;   - existing longjmp buffer ("void *" to avoid
 *                               including <setjmp.h> here)
 *   volatile sig_atomic_t *enabled; - flag that enables longjmp-on-interrupt
 *   bool            canceled; - indicates whether cancellation occurred
 *
 * `sig_atomic_t` is `c_int` on the platforms PostgreSQL targets; the field is a
 * pointer to a volatile one, modeled here as a raw pointer to c_int.
 */
#[repr(C)]
pub struct PromptInterruptContext {
    /// To avoid including <setjmp.h> here, jmpbuf is declared "void *"
    pub jmpbuf: *mut core::ffi::c_void,
    /// flag that enables longjmp-on-interrupt
    pub enabled: *mut c_int,
    /// indicates whether cancellation occurred
    pub canceled: bool,
}

// ----------------------------------------------------------------
//   extern "C" / TODO(pg-port) shims
// ----------------------------------------------------------------

// strtoint() below is a thin wrapper over libc strtol(), exactly as in the C.
// strtol writes its stop position through `endptr` and reports overflow by
// setting the C `errno` to ERANGE; strtoint additionally forces ERANGE when the
// long result does not fit in an int.  We bind the libc routine and the
// platform errno cell directly so the errno side effect is preserved 1:1.
//
// TODO(pg-port): once the port has its own errno abstraction, route through it.
extern "C" {
    fn strtol(str: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_long;

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

/// `ERANGE` - result too large.  Value is the same (34) on Linux and the BSD/
/// Darwin family that PostgreSQL targets.
const ERANGE: c_int = 34;

/// Minimal `strlen` over a C string (mirrors libc strlen; matches the bootstrap
/// helper used elsewhere in the port).
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

/// Minimal `strcmp` over two C strings (mirrors libc strcmp).
///
/// # Safety
/// `a` and `b` must point to valid NUL-terminated C strings.
#[inline]
unsafe fn strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let mut i = 0usize;
    loop {
        // Compare as unsigned char, like the C standard library.
        let ca = *a.add(i) as u8;
        let cb = *b.add(i) as u8;
        if ca != cb {
            return ca as c_int - cb as c_int;
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}

// ----------------------------------------------------------------
//   functions in src/common/string.c
// ----------------------------------------------------------------

/*
 * Returns whether the string `str' has the postfix `end'.
 *
 * # Safety
 * `str` and `end` must point to valid NUL-terminated C strings.
 */
pub unsafe fn pg_str_endswith(str: *const c_char, end: *const c_char) -> bool {
    let slen: Size = strlen(str);
    let elen: Size = strlen(end);

    /* can't be a postfix if longer */
    if elen > slen {
        return false;
    }

    /* compare the end of the strings */
    let str = str.add(slen - elen);
    strcmp(str, end) == 0
}

/*
 * strtoint --- just like strtol, but returns int not long
 *
 * # Safety
 * `str` must point to a valid NUL-terminated C string; if `endptr` is non-NULL it
 * must be valid for a pointer write.  Mutates the C `errno` cell exactly as the
 * original (libc `strtol` may set it, and we additionally set ERANGE on int
 * overflow).
 */
pub unsafe fn strtoint(str: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_int {
    let val: c_long;

    val = strtol(str, endptr, base);
    if val != (val as c_int) as c_long {
        *pg_errno_location() = ERANGE;
    }
    val as c_int
}

/*
 * pg_clean_ascii -- Replace any non-ASCII chars with a "\xXX" string
 *
 * Makes a newly allocated copy of the string passed in, which must be
 * '\0'-terminated. In the backend, additional alloc_flags may be provided and
 * will be passed as-is to palloc_extended(); in the frontend, alloc_flags is
 * ignored and the copy is malloc'd.
 *
 * This function exists specifically to deal with filtering out
 * non-ASCII characters in a few places where the client can provide an almost
 * arbitrary string (and it isn't checked to ensure it's a valid username or
 * database name or similar) and we don't want to have control characters or other
 * things ending up in the log file where server admins might end up with a
 * messed up terminal when looking at them.
 *
 * In general, this function should NOT be used- instead, consider how to handle
 * the string without needing to filter out the non-ASCII characters.
 *
 * Ultimately, we'd like to improve the situation to not require replacing all
 * non-ASCII but perform more intelligent filtering which would allow UTF or
 * similar, but it's unclear exactly what we should allow, so stick to ASCII only
 * for now.
 *
 * # Safety
 * `str` must point to a valid NUL-terminated C string.  Returns a freshly
 * palloc'd, NUL-terminated buffer (or NULL if the allocation declined, e.g. with
 * MCXT_ALLOC_NO_OOM).
 */
pub unsafe fn pg_clean_ascii(str: *const c_char, alloc_flags: c_int) -> *mut c_char {
    let dstlen: Size;
    let dst: *mut c_char;
    let mut p: *const c_char;
    let mut i: Size = 0;

    /* Worst case, each byte can become four bytes, plus a null terminator. */
    dstlen = strlen(str) * 4 + 1;

    // #ifdef FRONTEND
    //     dst = malloc(dstlen);
    // TODO(pg-port): frontend build (postgres_fe.h) uses pg_malloc here; this
    // unit translates the backend (#ifndef FRONTEND) path below.
    // #else
    dst = palloc_extended(dstlen, alloc_flags) as *mut c_char;
    // #endif

    if dst.is_null() {
        return core::ptr::null_mut();
    }

    p = str;
    while *p != 0 {
        /* Only allow clean ASCII chars in the string */
        // Note: `char` is signed on the platforms PostgreSQL targets, so the
        // comparison `*p < 32 || *p > 126` is on a signed value (a high-bit
        // byte is negative and thus < 32).  c_char is i8 here, matching that.
        if *p < 32 || *p > 126 {
            Assert!(i < (dstlen - 3));
            /*
             * snprintf(&dst[i], dstlen - i, "\\x%02x", (unsigned char) *p);
             *
             * Emits exactly the 4 bytes  '\' 'x' <hi> <lo>  of the lowercase
             * two-digit hex escape.  dstlen - i always leaves room (asserted
             * above), so no truncation can occur; we write the 4 bytes directly.
             */
            let byte = *p as u8;
            const HEX: &[u8; 16] = b"0123456789abcdef";
            *dst.add(i) = b'\\' as c_char;
            *dst.add(i + 1) = b'x' as c_char;
            *dst.add(i + 2) = HEX[(byte >> 4) as usize] as c_char;
            *dst.add(i + 3) = HEX[(byte & 0x0f) as usize] as c_char;
            i += 4;
        } else {
            Assert!(i < dstlen);
            *dst.add(i) = *p;
            i += 1;
        }
        p = p.add(1);
    }

    Assert!(i < dstlen);
    *dst.add(i) = 0; /* '\0' */
    dst
}

/*
 * pg_is_ascii -- Check if string is made only of ASCII characters
 *
 * # Safety
 * `str` must point to a valid NUL-terminated C string.
 */
pub unsafe fn pg_is_ascii(mut str: *const c_char) -> bool {
    while *str != 0 {
        if IS_HIGHBIT_SET(*str as u8) {
            return false;
        }
        str = str.add(1);
    }
    true
}

/*
 * pg_strip_crlf -- Remove any trailing newline and carriage return
 *
 * Removes any trailing newline and carriage return characters (\r on
 * Windows) in the input string, zero-terminating it.
 *
 * The passed in string must be zero-terminated.  This function returns
 * the new length of the string.
 *
 * # Safety
 * `str` must point to a valid, writable NUL-terminated C string.
 */
pub unsafe fn pg_strip_crlf(str: *mut c_char) -> c_int {
    let mut len: c_int = strlen(str) as c_int;

    while len > 0
        && (*str.add((len - 1) as usize) == b'\n' as c_char
            || *str.add((len - 1) as usize) == b'\r' as c_char)
    {
        len -= 1;
        *str.add(len as usize) = 0; /* '\0' */
    }

    len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endswith() {
        unsafe {
            assert!(pg_str_endswith(c"hello.sql".as_ptr(), c".sql".as_ptr()));
            assert!(!pg_str_endswith(c"hello".as_ptr(), c".sql".as_ptr()));
            assert!(pg_str_endswith(c"x".as_ptr(), c"".as_ptr()));
        }
    }
}
