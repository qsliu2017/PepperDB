//! Translation of postgres/src/include/common/fe_memutils.h
//!   + postgres/src/common/fe_memutils.c
//!
//! Memory management support for FRONTEND code.
//!
//! These are the client-program counterparts to the backend's palloc family.
//! Whereas the backend palloc (crate::utils::palloc) allocates from a
//! MemoryContext, the frontend versions are thin "safe" wrappers around libc's
//! malloc/realloc/free/strdup that exit(1) on out-of-memory (except
//! pg_malloc_extended with MCXT_ALLOC_NO_OOM).  We bind those libc routines
//! directly via `extern "C"` so allocation behavior matches the C 1:1.
//!
//! This file's role is FRONTEND, so we translate the FRONTEND path: the .c
//! begins with `#ifndef FRONTEND #error ... #endif`, i.e. it is only ever
//! compiled for frontend builds and uses libc malloc, never the backend palloc.
//! There is no backend branch to stub out here.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

// The MCXT_ALLOC_* flags are deliberately named the same as the backend flags
// (see fe_memutils.h); reuse the canonical definitions from utils::palloc so we
// don't duplicate the values.
use crate::utils::palloc::{MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO};

// ----------------------------------------------------------------
//   <stdlib.h> / <string.h> bindings
// ----------------------------------------------------------------
//
// The "safe" allocators wrap the raw C library routines directly so the
// frontend behavior (and ABI, for code that compiles backend files into
// frontend programs) matches the original exactly.
extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn realloc(ptr: *mut c_void, size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
    fn strdup(s: *const c_char) -> *mut c_char;
    fn strnlen(s: *const c_char, maxlen: usize) -> usize;
}

/*
 * Assumed maximum size for allocation requests.
 *
 * We don't enforce this, so the actual maximum is the platform's SIZE_MAX.
 * But it's useful to have it defined in frontend builds, so that common
 * code can check for oversized requests without having frontend-vs-backend
 * differences.  Also, some code relies on MaxAllocSize being no more than
 * INT_MAX/2, so rather than setting this to SIZE_MAX, make it the same as
 * the backend's value.
 */
// NB: the backend value (crate::utils::memutils::MaxAllocSize) is identical and
// already in scope via the prelude; we keep the header's local definition here
// to mirror fe_memutils.h verbatim.
pub const MaxAllocSize: Size = 0x3fffffff as Size; /* 1 gigabyte - 1 */

/// Emit "out of memory" to stderr and exit(EXIT_FAILURE), matching the C's
/// `fprintf(stderr, _("out of memory\n")); exit(EXIT_FAILURE);`.
fn out_of_memory() -> ! {
    eprintln!("out of memory");
    std::process::exit(1);
}

// static inline void *
// pg_malloc_internal(size_t size, int flags)
//
// # Safety
// Returns a libc-`malloc`'d pointer (or NULL with MCXT_ALLOC_NO_OOM); the caller
// owns it and must release it with `pg_free`.
pub unsafe fn pg_malloc_internal(mut size: usize, flags: c_int) -> *mut c_void {
    let tmp: *mut c_void;

    /* Avoid unportable behavior of malloc(0) */
    if size == 0 {
        size = 1;
    }
    tmp = malloc(size);
    if tmp.is_null() {
        if (flags & MCXT_ALLOC_NO_OOM) == 0 {
            out_of_memory();
        }
        return null_mut();
    }

    if (flags & MCXT_ALLOC_ZERO) != 0 {
        MemSet(tmp, 0, size);
    }
    tmp
}

// void *
// pg_malloc(size_t size)
//
// # Safety
// Returns an owned libc allocation; release with `pg_free`.
pub unsafe fn pg_malloc(size: usize) -> *mut c_void {
    pg_malloc_internal(size, 0)
}

// void *
// pg_malloc0(size_t size)
//
// # Safety
// Returns an owned, zero-filled libc allocation; release with `pg_free`.
pub unsafe fn pg_malloc0(size: usize) -> *mut c_void {
    pg_malloc_internal(size, MCXT_ALLOC_ZERO)
}

// void *
// pg_malloc_extended(size_t size, int flags)
//
// # Safety
// Returns an owned libc allocation (or NULL with MCXT_ALLOC_NO_OOM); release
// with `pg_free`.
pub unsafe fn pg_malloc_extended(size: usize, flags: c_int) -> *mut c_void {
    pg_malloc_internal(size, flags)
}

// void *
// pg_realloc(void *ptr, size_t size)
//
// # Safety
// `ptr` must be NULL or a pointer previously returned by this module's
// allocators; the returned pointer supersedes it and must be released with
// `pg_free`.
pub unsafe fn pg_realloc(ptr: *mut c_void, mut size: usize) -> *mut c_void {
    let tmp: *mut c_void;

    /* Avoid unportable behavior of realloc(NULL, 0) */
    if ptr.is_null() && size == 0 {
        size = 1;
    }
    tmp = realloc(ptr, size);
    if tmp.is_null() {
        out_of_memory();
    }
    tmp
}

/*
 * "Safe" wrapper around strdup().
 */
// char *
// pg_strdup(const char *in)
//
// # Safety
// `inp` must be NULL or point to a valid NUL-terminated C string. The returned
// pointer is an owned libc allocation; release with `pg_free`.
pub unsafe fn pg_strdup(inp: *const c_char) -> *mut c_char {
    let tmp: *mut c_char;

    if inp.is_null() {
        eprintln!("cannot duplicate null pointer (internal error)");
        std::process::exit(1);
    }
    tmp = strdup(inp);
    if tmp.is_null() {
        out_of_memory();
    }
    tmp
}

// void
// pg_free(void *ptr)
//
// # Safety
// `ptr` must be NULL or a pointer previously returned by this module's
// allocators, and not already freed.
pub unsafe fn pg_free(ptr: *mut c_void) {
    free(ptr);
}

/*
 * Frontend emulation of backend memory management functions.  Useful for
 * programs that compile backend files.
 */
// void *
// palloc(Size size)
//
// # Safety
// Returns an owned libc allocation; release with `pfree`.
pub unsafe fn palloc(size: Size) -> *mut c_void {
    pg_malloc_internal(size, 0)
}

// void *
// palloc0(Size size)
//
// # Safety
// Returns an owned, zero-filled libc allocation; release with `pfree`.
pub unsafe fn palloc0(size: Size) -> *mut c_void {
    pg_malloc_internal(size, MCXT_ALLOC_ZERO)
}

// void *
// palloc_extended(Size size, int flags)
//
// # Safety
// Returns an owned libc allocation (or NULL with MCXT_ALLOC_NO_OOM); release
// with `pfree`.
pub unsafe fn palloc_extended(size: Size, flags: c_int) -> *mut c_void {
    pg_malloc_internal(size, flags)
}

// void
// pfree(void *pointer)
//
// # Safety
// `pointer` must be NULL or a pointer previously returned by this module's
// allocators, and not already freed.
pub unsafe fn pfree(pointer: *mut c_void) {
    pg_free(pointer);
}

// char *
// pstrdup(const char *in)
//
// # Safety
// `inp` must be NULL or a valid NUL-terminated C string; release the result
// with `pfree`.
pub unsafe fn pstrdup(inp: *const c_char) -> *mut c_char {
    pg_strdup(inp)
}

// char *
// pnstrdup(const char *in, Size size)
//
// # Safety
// `inp` must be NULL or point to a readable buffer of at least `size` bytes (or
// a shorter NUL-terminated string); release the result with `pfree`.
pub unsafe fn pnstrdup(inp: *const c_char, size: Size) -> *mut c_char {
    let tmp: *mut c_char;
    let len: c_int;

    if inp.is_null() {
        eprintln!("cannot duplicate null pointer (internal error)");
        std::process::exit(1);
    }

    len = strnlen(inp, size) as c_int;
    tmp = malloc((len + 1) as usize) as *mut c_char;
    if tmp.is_null() {
        out_of_memory();
    }

    core::ptr::copy_nonoverlapping(inp, tmp, len as usize);
    *tmp.add(len as usize) = b'\0' as c_char;

    tmp
}

// void *
// repalloc(void *pointer, Size size)
//
// # Safety
// `pointer` must be NULL or a pointer previously returned by this module's
// allocators; the result supersedes it and must be released with `pfree`.
pub unsafe fn repalloc(pointer: *mut c_void, size: Size) -> *mut c_void {
    pg_realloc(pointer, size)
}
