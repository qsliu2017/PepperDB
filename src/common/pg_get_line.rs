//! Translation of postgres/src/common/pg_get_line.c
//! (declarations in postgres/src/include/common/string.h:
//!     extern char *pg_get_line(FILE *stream, PromptInterruptContext *prompt_ctx);
//!     extern bool pg_get_line_buf(FILE *stream, struct StringInfoData *buf);
//!     extern bool pg_get_line_append(FILE *stream, struct StringInfoData *buf,
//!                                    PromptInterruptContext *prompt_ctx);
//! there is no pg_get_line.h).
//!
//! fgets() with an expansible result buffer.
//!
//! The .c begins with:
//!     #ifndef FRONTEND
//!     #include "postgres.h"
//!     #else
//!     #include "postgres_fe.h"
//!     #endif
//!     #include <setjmp.h>
//!     #include "common/string.h"
//!     #include "lib/stringinfo.h"
//! We translate the BACKEND path (the `#ifndef FRONTEND` branch); palloc/pfree
//! come from the prelude, StringInfo from lib::stringinfo, and
//! PromptInterruptContext from common::string.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/common/pg_get_line.c

use crate::prelude::*;

// lib/stringinfo.h: StringInfo type and the buffer routines used below.
use crate::lib::stringinfo::{
    appendBinaryStringInfo, enlargeStringInfo, initStringInfo, resetStringInfo, StringInfo,
    StringInfoData,
};

// common/string.h: PromptInterruptContext (the longjmp-based cancellation hook).
use crate::common::string::PromptInterruptContext;

// ----------------------------------------------------------------
//   <stdio.h> FILE* bindings
// ----------------------------------------------------------------
//
// pg_get_line is a thin wrapper around the C stdio fgets()/ferror(), so we bind
// those from libc directly.  FILE is an opaque type; we never inspect its
// fields, only pass the pointer through to the stdio routines.

/// Opaque <stdio.h> `FILE`.  Mirrors `typedef struct __sFILE FILE;` etc.; we only
/// ever hold and forward `*mut FILE`, never dereferencing it ourselves.
#[repr(C)]
pub struct FILE {
    _opaque: [u8; 0],
}

extern "C" {
    /// `char *fgets(char *s, int size, FILE *stream);`
    fn fgets(s: *mut c_char, size: c_int, stream: *mut FILE) -> *mut c_char;
    /// `int ferror(FILE *stream);`
    fn ferror(stream: *mut FILE) -> c_int;
}

// ----------------------------------------------------------------
//   <signal.h> / <setjmp.h> cancellation support
// ----------------------------------------------------------------
//
// The original uses sigsetjmp()/siglongjmp() so that a SIGINT handler can
// longjmp back into pg_get_line_append while it is blocked in fgets(), aborting
// the read.  setjmp/longjmp has no safe Rust equivalent: returning a second time
// from a function (as siglongjmp causes) is undefined behavior in Rust, and the
// `sigjmp_buf` layout is platform-specific.  We bind the libc primitive so the
// `prompt_ctx` plumbing (enabled flag, canceled flag, buffer rollback) is
// preserved 1:1, but the actual non-local return path is left to the signal
// machinery the caller installs.
//
// TODO(pg-port): the sigsetjmp() second-return branch cannot be expressed in
// safe/sound Rust.  On the platforms PostgreSQL targets, sigsetjmp is the macro
// __sigsetjmp(env, savemask); we bind that.  When the port grows its own
// query-cancel longjmp abstraction this should route through it instead.
extern "C" {
    /// `int __sigsetjmp(sigjmp_buf env, int savemask);` (what the `sigsetjmp`
    /// macro expands to on glibc and the BSD/Darwin family).  `env` is the
    /// caller's `PromptInterruptContext.jmpbuf`, treated as an opaque buffer.
    fn __sigsetjmp(env: *mut c_void, savemask: c_int) -> c_int;
}

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

/*
 * pg_get_line()
 *
 * This is meant to be equivalent to fgets(), except that instead of
 * reading into a caller-supplied, fixed-size buffer, it reads into
 * a palloc'd (in frontend, really malloc'd) string, which is resized
 * as needed to handle indefinitely long input lines.  The caller is
 * responsible for pfree'ing the result string when appropriate.
 *
 * As with fgets(), returns NULL if there is a read error or if no
 * characters are available before EOF.  The caller can distinguish
 * these cases by checking ferror(stream).
 *
 * Since this is meant to be equivalent to fgets(), the trailing newline
 * (if any) is not stripped.  Callers may wish to apply pg_strip_crlf().
 *
 * Note that while I/O errors are reflected back to the caller to be
 * dealt with, an OOM condition for the palloc'd buffer will not be;
 * there'll be an ereport(ERROR) or exit(1) inside stringinfo.c.
 *
 * Also note that the palloc'd buffer is usually a lot longer than
 * strictly necessary, so it may be inadvisable to use this function
 * to collect lots of long-lived data.  A less memory-hungry option
 * is to use pg_get_line_buf() or pg_get_line_append() in a loop,
 * then pstrdup() each line.
 *
 * prompt_ctx can optionally be provided to allow this function to be
 * canceled via an existing SIGINT signal handler that will longjmp to the
 * specified place only when *(prompt_ctx->enabled) is true.  If canceled,
 * this function returns NULL, and prompt_ctx->canceled is set to true.
 *
 * # Safety
 * `stream` must be a valid open `FILE*`.  `prompt_ctx`, if non-NULL, must point
 * to a valid PromptInterruptContext whose `jmpbuf`/`enabled` are valid.  Returns
 * a palloc'd buffer the caller must pfree, or NULL.
 */
pub unsafe fn pg_get_line(stream: *mut FILE, prompt_ctx: *mut PromptInterruptContext) -> *mut c_char {
    let mut buf: StringInfoData = StringInfoData {
        data: core::ptr::null_mut(),
        len: 0,
        maxlen: 0,
        cursor: 0,
    };

    initStringInfo(&mut buf);

    if !pg_get_line_append(stream, &mut buf, prompt_ctx) {
        /* ensure that free() doesn't mess up errno */
        let save_errno: c_int = *pg_errno_location();

        pfree(buf.data as *mut c_void);
        *pg_errno_location() = save_errno;
        return core::ptr::null_mut();
    }

    buf.data
}

/*
 * pg_get_line_buf()
 *
 * This has similar behavior to pg_get_line(), and thence to fgets(),
 * except that the collected data is returned in a caller-supplied
 * StringInfo buffer.  This is a convenient API for code that just
 * wants to read and process one line at a time, without any artificial
 * limit on line length.
 *
 * Returns true if a line was successfully collected (including the
 * case of a non-newline-terminated line at EOF).  Returns false if
 * there was an I/O error or no data was available before EOF.
 * (Check ferror(stream) to distinguish these cases.)
 *
 * In the false-result case, buf is reset to empty.
 *
 * # Safety
 * `stream` must be a valid open `FILE*`; `buf` must be a writable StringInfo.
 */
pub unsafe fn pg_get_line_buf(stream: *mut FILE, buf: StringInfo) -> bool {
    /* We just need to drop any data from the previous call */
    resetStringInfo(buf);
    pg_get_line_append(stream, buf, core::ptr::null_mut())
}

/*
 * pg_get_line_append()
 *
 * This has similar behavior to pg_get_line(), and thence to fgets(),
 * except that the collected data is appended to whatever is in *buf.
 * This is useful in preference to pg_get_line_buf() if the caller wants
 * to merge some lines together, e.g. to implement backslash continuation.
 *
 * Returns true if a line was successfully collected (including the
 * case of a non-newline-terminated line at EOF).  Returns false if
 * there was an I/O error or no data was available before EOF.
 * (Check ferror(stream) to distinguish these cases.)
 *
 * In the false-result case, the contents of *buf are logically unmodified,
 * though it's possible that the buffer has been resized.
 *
 * prompt_ctx can optionally be provided to allow this function to be
 * canceled via an existing SIGINT signal handler that will longjmp to the
 * specified place only when *(prompt_ctx->enabled) is true.  If canceled,
 * this function returns false, and prompt_ctx->canceled is set to true.
 *
 * # Safety
 * `stream` must be a valid open `FILE*`; `buf` must be a writable StringInfo.
 * `prompt_ctx`, if non-NULL, must point to a valid PromptInterruptContext.
 */
pub unsafe fn pg_get_line_append(
    stream: *mut FILE,
    buf: StringInfo,
    prompt_ctx: *mut PromptInterruptContext,
) -> bool {
    let orig_len: c_int = (*buf).len;

    // if (prompt_ctx && sigsetjmp(*((sigjmp_buf *) prompt_ctx->jmpbuf), 1) != 0)
    //
    // The sigsetjmp() second-return path (reached via siglongjmp from the SIGINT
    // handler) is the cancellation branch.  See the extern "C" note above:
    // a genuine longjmp back into this frame is unsound in Rust, so we keep the
    // call (to arm the jmpbuf identically to the C) but treat only its normal,
    // first return (== 0) here.
    // TODO(pg-port): wire the second-return (canceled) branch through a sound
    //                query-cancel mechanism once the port provides one.
    if !prompt_ctx.is_null() && __sigsetjmp((*prompt_ctx).jmpbuf, 1) != 0 {
        /* Got here with longjmp */
        (*prompt_ctx).canceled = true;
        /* Discard any data we collected before detecting error */
        (*buf).len = orig_len;
        *(*buf).data.add(orig_len as usize) = 0; /* '\0' */
        return false;
    }

    /* Loop until newline or EOF/error */
    loop {
        let res: *mut c_char;

        /* Enable longjmp while waiting for input */
        if !prompt_ctx.is_null() {
            *(*prompt_ctx).enabled = 1; // true
        }

        /* Read some data, appending it to whatever we already have */
        res = fgets(
            (*buf).data.add((*buf).len as usize),
            (*buf).maxlen - (*buf).len,
            stream,
        );

        /* Disable longjmp again, then break if fgets failed */
        if !prompt_ctx.is_null() {
            *(*prompt_ctx).enabled = 0; // false
        }

        if res.is_null() {
            break;
        }

        /* Got data, so update buf->len */
        (*buf).len += strlen((*buf).data.add((*buf).len as usize)) as c_int;

        /* Done if we have collected a newline */
        if (*buf).len > orig_len && *(*buf).data.add(((*buf).len - 1) as usize) == b'\n' as c_char {
            return true;
        }

        /* Make some more room in the buffer, and loop to read more data */
        enlargeStringInfo(buf, 128);
    }

    /* Check for I/O errors and EOF */
    if ferror(stream) != 0 || (*buf).len == orig_len {
        /* Discard any data we collected before detecting error */
        (*buf).len = orig_len;
        *(*buf).data.add(orig_len as usize) = 0; /* '\0' */
        return false;
    }

    /* No newline at EOF, but we did collect some data */
    true
}

// ----------------------------------------------------------------
//   errno access (used by pg_get_line's save/restore of errno)
// ----------------------------------------------------------------
//
// On macOS/BSD errno is `*__error()`; on Linux it is `*__errno_location()`.
// Mirrors the bindings in port/pg_strong_random.rs and common/string.rs.
extern "C" {
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
