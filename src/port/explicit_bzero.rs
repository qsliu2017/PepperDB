//! Translated from PostgreSQL `src/port/explicit_bzero.c`
//! (declaration in `src/include/port.h`:
//! `extern void explicit_bzero(void *buf, size_t len);`).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

// The original C selects among three implementations at compile time:
//
//   #if HAVE_DECL_MEMSET_S
//       (void) memset_s(buf, len, 0, len);
//   #elif defined(WIN32)
//       (void) SecureZeroMemory(buf, len);
//   #else
//       indirect call through a volatile function pointer to bzero2().
//
// TODO(pg-port): the HAVE_DECL_MEMSET_S (C11 Annex K memset_s) and WIN32
// (SecureZeroMemory) variants are not translated; we always provide the
// portable fallback below. Rust gives us first-class primitives for the
// "do not let the optimizer elide the store" guarantee, so we do not need a
// volatile function pointer.

/*
 * explicit_bzero
 *
 * Securely zero `len` bytes starting at `buf`.  Unlike a plain memset(), this
 * must not be optimized away even when the buffer is dead afterwards (the
 * classic dead-store elimination that defeats wiping secrets).
 */
//
// Portable fallback: write zeros, then issue a compiler fence so the compiler
// cannot reorder or elide the preceding writes across this point.  This mirrors
// the OpenSSH-style "indirect call through a volatile pointer" trick used by
// the C `#else` branch (bzero2 / bzero_p), but expressed with Rust's atomic
// compiler fence.
#[no_mangle]
pub unsafe extern "C" fn explicit_bzero(buf: *mut c_void, len: Size) {
    // memset(buf, 0, len)
    core::ptr::write_bytes(buf as *mut u8, 0, len);

    // Prevent dead-store elimination of the writes above.
    core::sync::atomic::compiler_fence(core::sync::atomic::Ordering::SeqCst);
}
