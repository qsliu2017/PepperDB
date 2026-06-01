//! Translation of postgres/src/include/common/cryptohash.h
//!                + postgres/src/common/cryptohash.c
//!
//! cryptohash.c
//!   Fallback implementations for cryptographic hash functions.
//!
//! This is the set of in-core functions used when there are no other
//! alternative options like OpenSSL.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Port notes:
//!   - This is the portable, NON-OpenSSL cryptohash dispatcher. It multiplexes
//!     over PG_MD5/PG_SHA1/PG_SHA224/PG_SHA256/PG_SHA384/PG_SHA512 and delegates
//!     to the already-translated algorithm modules (md5/sha1/sha2). The C file
//!     pulls in md5_int.h / sha1_int.h / sha2_int.h; those internal structs and
//!     the int-header `pg_*_init/update/final` routines now live inside the
//!     md5/sha1/sha2 Rust modules, so we import them directly.
//!   - We translate the BACKEND path (#ifndef FRONTEND): ALLOC -> palloc and
//!     FREE -> pfree. The FRONTEND (malloc/free) branch is marked TODO(pg-port).
//!   - `explicit_bzero` comes from crate::port::explicit_bzero; its signature is
//!     `unsafe extern "C" fn explicit_bzero(buf: *mut c_void, len: Size)`.
//!   - The C `_()` is the gettext translation macro; in the in-core fallback it
//!     behaves as identity, returning a `const char *` to a NUL-terminated
//!     string literal. We reproduce that with static NUL-terminated byte
//!     strings whose pointer is returned as `*const c_char`.

#![allow(clippy::missing_safety_doc)]

use crate::prelude::*;

// #ifndef FRONTEND
// #include "postgres.h"
// #else
// #include "postgres_fe.h"
// #endif
// TODO(pg-port): FRONTEND branch (postgres_fe.h) not ported; backend path active.
//
// #include <sys/param.h>
//
// #include "common/cryptohash.h"
// #include "md5_int.h"
// #include "sha1_int.h"
// #include "sha2_int.h"
use crate::common::md5::*;
use crate::common::sha1::*;
use crate::common::sha2::*;
use crate::port::explicit_bzero::explicit_bzero;

/* ---------------------------------------------------------------------------
 * From the public header src/include/common/cryptohash.h
 * ------------------------------------------------------------------------- */

/* Context Structures for each hash function */
/*
 * typedef enum
 * {
 *     PG_MD5 = 0,
 *     PG_SHA1,
 *     PG_SHA224,
 *     PG_SHA256,
 *     PG_SHA384,
 *     PG_SHA512,
 * } pg_cryptohash_type;
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum pg_cryptohash_type {
    PG_MD5 = 0,
    PG_SHA1,
    PG_SHA224,
    PG_SHA256,
    PG_SHA384,
    PG_SHA512,
}
pub use pg_cryptohash_type::*;

/*
 * The opaque context (typedef struct pg_cryptohash_ctx pg_cryptohash_ctx;) is
 * defined privately below, mirroring the C source where `struct
 * pg_cryptohash_ctx` is opaque in the header and laid out in cryptohash.c.
 *
 * extern pg_cryptohash_ctx *pg_cryptohash_create(pg_cryptohash_type type);
 * extern int  pg_cryptohash_init(pg_cryptohash_ctx *ctx);
 * extern int  pg_cryptohash_update(pg_cryptohash_ctx *ctx, const uint8 *data, size_t len);
 * extern int  pg_cryptohash_final(pg_cryptohash_ctx *ctx, uint8 *dest, size_t len);
 * extern void pg_cryptohash_free(pg_cryptohash_ctx *ctx);
 * extern const char *pg_cryptohash_error(pg_cryptohash_ctx *ctx);
 */

/* ---------------------------------------------------------------------------
 * From the implementation src/common/cryptohash.c
 * ------------------------------------------------------------------------- */

/*
 * In backend, use palloc/pfree to ease the error handling.  In frontend,
 * use malloc to be able to return a failure status back to the caller.
 *
 * #ifndef FRONTEND
 * #define ALLOC(size) palloc(size)
 * #define FREE(ptr) pfree(ptr)
 * #else
 * #define ALLOC(size) malloc(size)
 * #define FREE(ptr) free(ptr)
 * #endif
 *
 * Backend path active.  TODO(pg-port): FRONTEND malloc/free variant not ported.
 */
#[inline]
unsafe fn ALLOC(size: Size) -> *mut c_void {
    palloc(size)
}
#[inline]
unsafe fn FREE(ptr: *mut c_void) {
    pfree(ptr);
}

/* Set of error states */
/*
 * typedef enum pg_cryptohash_errno
 * {
 *     PG_CRYPTOHASH_ERROR_NONE = 0,
 *     PG_CRYPTOHASH_ERROR_DEST_LEN,
 * } pg_cryptohash_errno;
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum pg_cryptohash_errno {
    PG_CRYPTOHASH_ERROR_NONE = 0,
    PG_CRYPTOHASH_ERROR_DEST_LEN,
}
use pg_cryptohash_errno::*;

/*
 * Internal pg_cryptohash_ctx structure
 *
 * struct pg_cryptohash_ctx
 * {
 *     pg_cryptohash_type type;
 *     pg_cryptohash_errno error;
 *
 *     union
 *     {
 *         pg_md5_ctx  md5;
 *         pg_sha1_ctx sha1;
 *         pg_sha224_ctx sha224;
 *         pg_sha256_ctx sha256;
 *         pg_sha384_ctx sha384;
 *         pg_sha512_ctx sha512;
 *     }           data;
 * };
 *
 * pg_sha224_ctx is a typedef of pg_sha256_ctx and pg_sha384_ctx is a typedef of
 * pg_sha512_ctx (see sha2.rs), so the union has three distinct members; we
 * reproduce all six fields faithfully with a #[repr(C)] union.
 */
#[repr(C)]
union pg_cryptohash_ctx_data {
    md5: pg_md5_ctx,
    sha1: pg_sha1_ctx,
    sha224: pg_sha224_ctx,
    sha256: pg_sha256_ctx,
    sha384: pg_sha384_ctx,
    sha512: pg_sha512_ctx,
}

#[repr(C)]
pub struct pg_cryptohash_ctx {
    r#type: pg_cryptohash_type,
    error: pg_cryptohash_errno,
    data: pg_cryptohash_ctx_data,
}

/*
 * pg_cryptohash_create
 *
 * Allocate a hash context.  Returns NULL on failure for an OOM.  The
 * backend issues an error, without returning.
 */
pub unsafe fn pg_cryptohash_create(r#type: pg_cryptohash_type) -> *mut pg_cryptohash_ctx {
    let ctx: *mut pg_cryptohash_ctx;

    /*
     * Note that this always allocates enough space for the largest hash. A
     * smaller allocation would be enough for md5, sha224 and sha256, but the
     * small extra amount of memory does not make it worth complicating this
     * code.
     */
    ctx = ALLOC(core::mem::size_of::<pg_cryptohash_ctx>()) as *mut pg_cryptohash_ctx;
    if ctx.is_null() {
        return core::ptr::null_mut();
    }

    core::ptr::write_bytes(ctx as *mut u8, 0, core::mem::size_of::<pg_cryptohash_ctx>());
    (*ctx).r#type = r#type;
    (*ctx).error = PG_CRYPTOHASH_ERROR_NONE;
    ctx
}

/*
 * pg_cryptohash_init
 *
 * Initialize a hash context.  Returns 0 on success, and -1 on failure.
 */
pub unsafe fn pg_cryptohash_init(ctx: *mut pg_cryptohash_ctx) -> c_int {
    if ctx.is_null() {
        return -1;
    }

    match (*ctx).r#type {
        PG_MD5 => {
            pg_md5_init(&mut (*ctx).data.md5);
        }
        PG_SHA1 => {
            pg_sha1_init(&mut (*ctx).data.sha1);
        }
        PG_SHA224 => {
            pg_sha224_init(&mut (*ctx).data.sha224);
        }
        PG_SHA256 => {
            pg_sha256_init(&mut (*ctx).data.sha256);
        }
        PG_SHA384 => {
            pg_sha384_init(&mut (*ctx).data.sha384);
        }
        PG_SHA512 => {
            pg_sha512_init(&mut (*ctx).data.sha512);
        }
    }

    0
}

/*
 * pg_cryptohash_update
 *
 * Update a hash context.  Returns 0 on success, and -1 on failure.
 */
pub unsafe fn pg_cryptohash_update(
    ctx: *mut pg_cryptohash_ctx,
    data: *const uint8,
    len: Size,
) -> c_int {
    if ctx.is_null() {
        return -1;
    }

    match (*ctx).r#type {
        PG_MD5 => {
            pg_md5_update(&mut (*ctx).data.md5, data, len);
        }
        PG_SHA1 => {
            pg_sha1_update(&mut (*ctx).data.sha1, data, len);
        }
        PG_SHA224 => {
            pg_sha224_update(&mut (*ctx).data.sha224, data, len);
        }
        PG_SHA256 => {
            pg_sha256_update(&mut (*ctx).data.sha256, data, len);
        }
        PG_SHA384 => {
            pg_sha384_update(&mut (*ctx).data.sha384, data, len);
        }
        PG_SHA512 => {
            pg_sha512_update(&mut (*ctx).data.sha512, data, len);
        }
    }

    0
}

/*
 * pg_cryptohash_final
 *
 * Finalize a hash context.  Returns 0 on success, and -1 on failure.
 */
pub unsafe fn pg_cryptohash_final(
    ctx: *mut pg_cryptohash_ctx,
    dest: *mut uint8,
    len: Size,
) -> c_int {
    if ctx.is_null() {
        return -1;
    }

    match (*ctx).r#type {
        PG_MD5 => {
            if len < MD5_DIGEST_LENGTH {
                (*ctx).error = PG_CRYPTOHASH_ERROR_DEST_LEN;
                return -1;
            }
            pg_md5_final(&mut (*ctx).data.md5, dest);
        }
        PG_SHA1 => {
            if len < SHA1_DIGEST_LENGTH {
                (*ctx).error = PG_CRYPTOHASH_ERROR_DEST_LEN;
                return -1;
            }
            pg_sha1_final(&mut (*ctx).data.sha1, dest);
        }
        PG_SHA224 => {
            if len < PG_SHA224_DIGEST_LENGTH {
                (*ctx).error = PG_CRYPTOHASH_ERROR_DEST_LEN;
                return -1;
            }
            pg_sha224_final(&mut (*ctx).data.sha224, dest);
        }
        PG_SHA256 => {
            if len < PG_SHA256_DIGEST_LENGTH {
                (*ctx).error = PG_CRYPTOHASH_ERROR_DEST_LEN;
                return -1;
            }
            pg_sha256_final(&mut (*ctx).data.sha256, dest);
        }
        PG_SHA384 => {
            if len < PG_SHA384_DIGEST_LENGTH {
                (*ctx).error = PG_CRYPTOHASH_ERROR_DEST_LEN;
                return -1;
            }
            pg_sha384_final(&mut (*ctx).data.sha384, dest);
        }
        PG_SHA512 => {
            if len < PG_SHA512_DIGEST_LENGTH {
                (*ctx).error = PG_CRYPTOHASH_ERROR_DEST_LEN;
                return -1;
            }
            pg_sha512_final(&mut (*ctx).data.sha512, dest);
        }
    }

    0
}

/*
 * pg_cryptohash_free
 *
 * Free a hash context.
 */
pub unsafe fn pg_cryptohash_free(ctx: *mut pg_cryptohash_ctx) {
    if ctx.is_null() {
        return;
    }

    explicit_bzero(
        ctx as *mut c_void,
        core::mem::size_of::<pg_cryptohash_ctx>(),
    );
    FREE(ctx as *mut c_void);
}

/*
 * The gettext translation macro `_()`.  In the in-core fallback build (no NLS),
 * `_(x)` is identity and yields a `const char *` to a NUL-terminated literal.
 * We model the message literals as static NUL-terminated byte strings.
 */
const MSG_OUT_OF_MEMORY: &[u8] = b"out of memory\0";
const MSG_SUCCESS: &[u8] = b"success\0";
const MSG_DEST_BUFFER_TOO_SMALL: &[u8] = b"destination buffer too small\0";

/*
 * pg_cryptohash_error
 *
 * Returns a static string providing details about an error that
 * happened during a computation.
 */
pub unsafe fn pg_cryptohash_error(ctx: *const pg_cryptohash_ctx) -> *const c_char {
    /*
     * This implementation would never fail because of an out-of-memory error,
     * except when creating the context.
     */
    if ctx.is_null() {
        return MSG_OUT_OF_MEMORY.as_ptr() as *const c_char; /* _("out of memory") */
    }

    match (*ctx).error {
        PG_CRYPTOHASH_ERROR_NONE => return MSG_SUCCESS.as_ptr() as *const c_char, /* _("success") */
        PG_CRYPTOHASH_ERROR_DEST_LEN => {
            return MSG_DEST_BUFFER_TOO_SMALL.as_ptr() as *const c_char; /* _("destination buffer too small") */
        }
    }

    /*
     * The C ends with an unreachable Assert(false) plus `return _("success");`.
     * The match above is exhaustive, so this tail is dead, but we keep it for
     * fidelity with the original control flow.
     */
    #[allow(unreachable_code)]
    {
        Assert!(false);
        MSG_SUCCESS.as_ptr() as *const c_char
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /* Helper: render a digest slice as lowercase hex. */
    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{:02x}", x)).collect()
    }

    /*
     * Run the full create/init/update/final/free pipeline for a given type and
     * digest length over `input`, returning the digest bytes.
     *
     * Note: pg_cryptohash_create calls palloc; these tests rely on the crate's
     * memory-context allocator being usable in a test harness, mirroring the
     * other backend unit tests.
     */
    unsafe fn digest_of(
        ty: pg_cryptohash_type,
        input: &[u8],
        digest_len: usize,
    ) -> Vec<u8> {
        let ctx = pg_cryptohash_create(ty);
        assert!(!ctx.is_null());
        assert_eq!(pg_cryptohash_init(ctx), 0);
        assert_eq!(
            pg_cryptohash_update(ctx, input.as_ptr(), input.len() as Size),
            0
        );
        let mut out = vec![0u8; digest_len];
        assert_eq!(
            pg_cryptohash_final(ctx, out.as_mut_ptr(), out.len() as Size),
            0
        );
        pg_cryptohash_free(ctx);
        out
    }

    #[test]
    fn dispatch_known_answers() {
        unsafe {
            /* MD5("abc") */
            assert_eq!(
                hex(&digest_of(PG_MD5, b"abc", MD5_DIGEST_LENGTH)),
                "900150983cd24fb0d6963f7d28e17f72"
            );
            /* SHA1("abc") */
            assert_eq!(
                hex(&digest_of(PG_SHA1, b"abc", SHA1_DIGEST_LENGTH)),
                "a9993e364706816aba3e25717850c26c9cd0d89d"
            );
            /* SHA224("abc") */
            assert_eq!(
                hex(&digest_of(PG_SHA224, b"abc", PG_SHA224_DIGEST_LENGTH)),
                "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"
            );
            /* SHA256("abc") */
            assert_eq!(
                hex(&digest_of(PG_SHA256, b"abc", PG_SHA256_DIGEST_LENGTH)),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            );
            /* SHA384("abc") */
            assert_eq!(
                hex(&digest_of(PG_SHA384, b"abc", PG_SHA384_DIGEST_LENGTH)),
                "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"
            );
            /* SHA512("abc") */
            assert_eq!(
                hex(&digest_of(PG_SHA512, b"abc", PG_SHA512_DIGEST_LENGTH)),
                "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
            );
        }
    }

    /* A too-small destination buffer must set the DEST_LEN error and return -1. */
    #[test]
    fn dest_len_error() {
        unsafe {
            let ctx = pg_cryptohash_create(PG_SHA256);
            assert!(!ctx.is_null());
            assert_eq!(pg_cryptohash_init(ctx), 0);
            let msg = b"abc";
            assert_eq!(pg_cryptohash_update(ctx, msg.as_ptr(), msg.len() as Size), 0);
            let mut out = [0u8; 8]; /* shorter than PG_SHA256_DIGEST_LENGTH (32) */
            assert_eq!(
                pg_cryptohash_final(ctx, out.as_mut_ptr(), out.len() as Size),
                -1
            );
            /* error string should be "destination buffer too small" */
            let errp = pg_cryptohash_error(ctx);
            let cs = core::ffi::CStr::from_ptr(errp);
            assert_eq!(cs.to_bytes(), b"destination buffer too small");
            pg_cryptohash_free(ctx);
        }
    }

    /* NULL context behaviors. */
    #[test]
    fn null_context() {
        unsafe {
            assert_eq!(pg_cryptohash_init(core::ptr::null_mut()), -1);
            assert_eq!(
                pg_cryptohash_update(core::ptr::null_mut(), core::ptr::null(), 0),
                -1
            );
            assert_eq!(
                pg_cryptohash_final(core::ptr::null_mut(), core::ptr::null_mut(), 0),
                -1
            );
            /* free(NULL) is a no-op */
            pg_cryptohash_free(core::ptr::null_mut());
            /* error(NULL) is "out of memory" */
            let errp = pg_cryptohash_error(core::ptr::null());
            let cs = core::ffi::CStr::from_ptr(errp);
            assert_eq!(cs.to_bytes(), b"out of memory");
        }
    }
}

#[cfg(test)]
mod dispatch_tests {
    use super::*;

    #[test]
    fn dispatches_sha256() {
        unsafe {
            // SHA-256("abc") through the generic dispatcher must equal the known vector.
            let ctx = pg_cryptohash_create(pg_cryptohash_type::PG_SHA256);
            assert!(!ctx.is_null());
            assert_eq!(pg_cryptohash_init(ctx), 0);
            let msg = b"abc";
            assert_eq!(pg_cryptohash_update(ctx, msg.as_ptr(), msg.len() as Size), 0);
            let mut out = [0u8; 32];
            assert_eq!(pg_cryptohash_final(ctx, out.as_mut_ptr(), out.len() as Size), 0);
            pg_cryptohash_free(ctx);
            let hex: String = out.iter().map(|x| format!("{:02x}", x)).collect();
            assert_eq!(hex, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        }
    }
}
