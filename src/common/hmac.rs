//! Translation of postgres/src/include/common/hmac.h
//!                + postgres/src/common/hmac.c
//!
//! hmac.c
//!   Implements Keyed-Hashing for Message Authentication (HMAC)
//!
//! Fallback implementation of HMAC, as specified in RFC 2104.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Port notes:
//!   - This is the portable, NON-OpenSSL HMAC, built on top of the in-core
//!     cryptohash interface (crate::common::cryptohash). The OpenSSL variant
//!     (hmac_openssl.c) is a separate file and is not part of this module.
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
// #include "common/cryptohash.h"
// #include "common/hmac.h"
// #include "common/md5.h"
// #include "common/sha1.h"
// #include "common/sha2.h"
use crate::common::cryptohash::{
    pg_cryptohash_create, pg_cryptohash_ctx, pg_cryptohash_error, pg_cryptohash_final,
    pg_cryptohash_free, pg_cryptohash_init, pg_cryptohash_type, pg_cryptohash_update,
};
use crate::common::cryptohash::pg_cryptohash_type::*;
use crate::common::md5::{MD5_BLOCK_SIZE, MD5_DIGEST_LENGTH};
use crate::common::sha1::{SHA1_BLOCK_SIZE, SHA1_DIGEST_LENGTH};
use crate::common::sha2::{
    PG_SHA224_BLOCK_LENGTH, PG_SHA224_DIGEST_LENGTH, PG_SHA256_BLOCK_LENGTH,
    PG_SHA256_DIGEST_LENGTH, PG_SHA384_BLOCK_LENGTH, PG_SHA384_DIGEST_LENGTH,
    PG_SHA512_BLOCK_LENGTH, PG_SHA512_DIGEST_LENGTH,
};
use crate::port::explicit_bzero::explicit_bzero;

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
 * typedef enum pg_hmac_errno
 * {
 *     PG_HMAC_ERROR_NONE = 0,
 *     PG_HMAC_ERROR_OOM,
 *     PG_HMAC_ERROR_INTERNAL,
 * } pg_hmac_errno;
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum pg_hmac_errno {
    PG_HMAC_ERROR_NONE = 0,
    PG_HMAC_ERROR_OOM,
    PG_HMAC_ERROR_INTERNAL,
}
use pg_hmac_errno::*;

/* Internal pg_hmac_ctx structure */
/*
 * struct pg_hmac_ctx
 * {
 *     pg_cryptohash_ctx *hash;
 *     pg_cryptohash_type type;
 *     pg_hmac_errno error;
 *     const char *errreason;
 *     int         block_size;
 *     int         digest_size;
 *
 *     uint8       k_ipad[PG_SHA512_BLOCK_LENGTH];
 *     uint8       k_opad[PG_SHA512_BLOCK_LENGTH];
 * };
 */
#[repr(C)]
pub struct pg_hmac_ctx {
    hash: *mut pg_cryptohash_ctx,
    r#type: pg_cryptohash_type,
    error: pg_hmac_errno,
    errreason: *const c_char,
    block_size: c_int,
    digest_size: c_int,

    /*
     * Use the largest block size among supported options.  This wastes some
     * memory but simplifies the allocation logic.
     */
    k_ipad: [uint8; PG_SHA512_BLOCK_LENGTH],
    k_opad: [uint8; PG_SHA512_BLOCK_LENGTH],
}

const HMAC_IPAD: uint8 = 0x36;
const HMAC_OPAD: uint8 = 0x5C;

/*
 * pg_hmac_create
 *
 * Allocate a hash context.  Returns NULL on failure for an OOM.  The
 * backend issues an error, without returning.
 */
pub unsafe fn pg_hmac_create(r#type: pg_cryptohash_type) -> *mut pg_hmac_ctx {
    let ctx: *mut pg_hmac_ctx;

    ctx = ALLOC(core::mem::size_of::<pg_hmac_ctx>()) as *mut pg_hmac_ctx;
    if ctx.is_null() {
        return core::ptr::null_mut();
    }
    core::ptr::write_bytes(ctx as *mut u8, 0, core::mem::size_of::<pg_hmac_ctx>());
    (*ctx).r#type = r#type;
    (*ctx).error = PG_HMAC_ERROR_NONE;
    (*ctx).errreason = core::ptr::null();

    /*
     * Initialize the context data.  This requires to know the digest and
     * block lengths, that depend on the type of hash used.
     */
    match r#type {
        PG_MD5 => {
            (*ctx).digest_size = MD5_DIGEST_LENGTH as c_int;
            (*ctx).block_size = MD5_BLOCK_SIZE as c_int;
        }
        PG_SHA1 => {
            (*ctx).digest_size = SHA1_DIGEST_LENGTH as c_int;
            (*ctx).block_size = SHA1_BLOCK_SIZE as c_int;
        }
        PG_SHA224 => {
            (*ctx).digest_size = PG_SHA224_DIGEST_LENGTH as c_int;
            (*ctx).block_size = PG_SHA224_BLOCK_LENGTH as c_int;
        }
        PG_SHA256 => {
            (*ctx).digest_size = PG_SHA256_DIGEST_LENGTH as c_int;
            (*ctx).block_size = PG_SHA256_BLOCK_LENGTH as c_int;
        }
        PG_SHA384 => {
            (*ctx).digest_size = PG_SHA384_DIGEST_LENGTH as c_int;
            (*ctx).block_size = PG_SHA384_BLOCK_LENGTH as c_int;
        }
        PG_SHA512 => {
            (*ctx).digest_size = PG_SHA512_DIGEST_LENGTH as c_int;
            (*ctx).block_size = PG_SHA512_BLOCK_LENGTH as c_int;
        }
    }

    (*ctx).hash = pg_cryptohash_create(r#type);
    if (*ctx).hash.is_null() {
        explicit_bzero(ctx as *mut c_void, core::mem::size_of::<pg_hmac_ctx>());
        FREE(ctx as *mut c_void);
        return core::ptr::null_mut();
    }

    ctx
}

/*
 * pg_hmac_init
 *
 * Initialize a HMAC context.  Returns 0 on success, -1 on failure.
 */
pub unsafe fn pg_hmac_init(ctx: *mut pg_hmac_ctx, key: *const uint8, len: Size) -> c_int {
    let mut i: c_int; /* C: int i; (loop variable, declared up top) */
    let digest_size: c_int;
    let block_size: c_int;
    let mut shrinkbuf: *mut uint8 = core::ptr::null_mut();

    if ctx.is_null() {
        return -1;
    }

    digest_size = (*ctx).digest_size;
    block_size = (*ctx).block_size;

    core::ptr::write_bytes((*ctx).k_opad.as_mut_ptr(), HMAC_OPAD, (*ctx).block_size as usize);
    core::ptr::write_bytes((*ctx).k_ipad.as_mut_ptr(), HMAC_IPAD, (*ctx).block_size as usize);

    /*
     * If the key is longer than the block size, pass it through the hash once
     * to shrink it down.
     */
    let mut key = key;
    let mut len = len;
    if len > block_size as Size {
        let hash_ctx: *mut pg_cryptohash_ctx;

        /* temporary buffer for one-time shrink */
        shrinkbuf = ALLOC(digest_size as Size) as *mut uint8;
        if shrinkbuf.is_null() {
            (*ctx).error = PG_HMAC_ERROR_OOM;
            return -1;
        }
        core::ptr::write_bytes(shrinkbuf, 0, digest_size as usize);

        hash_ctx = pg_cryptohash_create((*ctx).r#type);
        if hash_ctx.is_null() {
            (*ctx).error = PG_HMAC_ERROR_OOM;
            FREE(shrinkbuf as *mut c_void);
            return -1;
        }

        if pg_cryptohash_init(hash_ctx) < 0
            || pg_cryptohash_update(hash_ctx, key, len) < 0
            || pg_cryptohash_final(hash_ctx, shrinkbuf, digest_size as Size) < 0
        {
            (*ctx).error = PG_HMAC_ERROR_INTERNAL;
            (*ctx).errreason = pg_cryptohash_error(hash_ctx);
            pg_cryptohash_free(hash_ctx);
            FREE(shrinkbuf as *mut c_void);
            return -1;
        }

        key = shrinkbuf;
        len = digest_size as Size;
        pg_cryptohash_free(hash_ctx);
    }

    i = 0;
    while (i as Size) < len {
        (*ctx).k_ipad[i as usize] ^= *key.add(i as usize);
        (*ctx).k_opad[i as usize] ^= *key.add(i as usize);
        i += 1;
    }

    /* tmp = H(K XOR ipad, text) */
    if pg_cryptohash_init((*ctx).hash) < 0
        || pg_cryptohash_update((*ctx).hash, (*ctx).k_ipad.as_ptr(), (*ctx).block_size as Size) < 0
    {
        (*ctx).error = PG_HMAC_ERROR_INTERNAL;
        (*ctx).errreason = pg_cryptohash_error((*ctx).hash);
        if !shrinkbuf.is_null() {
            FREE(shrinkbuf as *mut c_void);
        }
        return -1;
    }

    if !shrinkbuf.is_null() {
        FREE(shrinkbuf as *mut c_void);
    }
    0
}

/*
 * pg_hmac_update
 *
 * Update a HMAC context.  Returns 0 on success, -1 on failure.
 */
pub unsafe fn pg_hmac_update(ctx: *mut pg_hmac_ctx, data: *const uint8, len: Size) -> c_int {
    if ctx.is_null() {
        return -1;
    }

    if pg_cryptohash_update((*ctx).hash, data, len) < 0 {
        (*ctx).error = PG_HMAC_ERROR_INTERNAL;
        (*ctx).errreason = pg_cryptohash_error((*ctx).hash);
        return -1;
    }

    0
}

/*
 * pg_hmac_final
 *
 * Finalize a HMAC context.  Returns 0 on success, -1 on failure.
 */
pub unsafe fn pg_hmac_final(ctx: *mut pg_hmac_ctx, dest: *mut uint8, len: Size) -> c_int {
    let h: *mut uint8;

    if ctx.is_null() {
        return -1;
    }

    h = ALLOC((*ctx).digest_size as Size) as *mut uint8;
    if h.is_null() {
        (*ctx).error = PG_HMAC_ERROR_OOM;
        return -1;
    }
    core::ptr::write_bytes(h, 0, (*ctx).digest_size as usize);

    if pg_cryptohash_final((*ctx).hash, h, (*ctx).digest_size as Size) < 0 {
        (*ctx).error = PG_HMAC_ERROR_INTERNAL;
        (*ctx).errreason = pg_cryptohash_error((*ctx).hash);
        FREE(h as *mut c_void);
        return -1;
    }

    /* H(K XOR opad, tmp) */
    if pg_cryptohash_init((*ctx).hash) < 0
        || pg_cryptohash_update((*ctx).hash, (*ctx).k_opad.as_ptr(), (*ctx).block_size as Size) < 0
        || pg_cryptohash_update((*ctx).hash, h, (*ctx).digest_size as Size) < 0
        || pg_cryptohash_final((*ctx).hash, dest, len) < 0
    {
        (*ctx).error = PG_HMAC_ERROR_INTERNAL;
        (*ctx).errreason = pg_cryptohash_error((*ctx).hash);
        FREE(h as *mut c_void);
        return -1;
    }

    FREE(h as *mut c_void);
    0
}

/*
 * pg_hmac_free
 *
 * Free a HMAC context.
 */
pub unsafe fn pg_hmac_free(ctx: *mut pg_hmac_ctx) {
    if ctx.is_null() {
        return;
    }

    pg_cryptohash_free((*ctx).hash);
    explicit_bzero(ctx as *mut c_void, core::mem::size_of::<pg_hmac_ctx>());
    FREE(ctx as *mut c_void);
}

/*
 * The gettext translation macro `_()`.  In the in-core fallback build (no NLS),
 * `_(x)` is identity and yields a `const char *` to a NUL-terminated literal.
 * We model the message literals as static NUL-terminated byte strings.
 */
const MSG_OUT_OF_MEMORY: &[u8] = b"out of memory\0";
const MSG_SUCCESS: &[u8] = b"success\0";
const MSG_INTERNAL_ERROR: &[u8] = b"internal error\0";

/*
 * pg_hmac_error
 *
 * Returns a static string providing details about an error that happened
 * during a HMAC computation.
 */
pub unsafe fn pg_hmac_error(ctx: *const pg_hmac_ctx) -> *const c_char {
    if ctx.is_null() {
        return MSG_OUT_OF_MEMORY.as_ptr() as *const c_char; /* _("out of memory") */
    }

    /*
     * If a reason is provided, rely on it, else fallback to any error code
     * set.
     */
    if !(*ctx).errreason.is_null() {
        return (*ctx).errreason;
    }

    match (*ctx).error {
        PG_HMAC_ERROR_NONE => return MSG_SUCCESS.as_ptr() as *const c_char, /* _("success") */
        PG_HMAC_ERROR_INTERNAL => return MSG_INTERNAL_ERROR.as_ptr() as *const c_char, /* _("internal error") */
        PG_HMAC_ERROR_OOM => return MSG_OUT_OF_MEMORY.as_ptr() as *const c_char, /* _("out of memory") */
    }

    /*
     * The C ends with an unreachable Assert(false) plus `return _("success");`.
     * The match above is exhaustive, so this tail is dead, but we keep it for
     * fidelity with the original control flow.
     */
    #[allow(unreachable_code)]
    {
        Assert!(false); /* cannot be reached */
        MSG_SUCCESS.as_ptr() as *const c_char
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /* Render a digest slice as lowercase hex. */
    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{:02x}", x)).collect()
    }

    /*
     * Compute HMAC over `key`/`data` for the given type+digest length using the
     * full create/init/update/final/free pipeline, returning the MAC bytes.
     */
    unsafe fn hmac_of(
        ty: pg_cryptohash_type,
        key: &[u8],
        data: &[u8],
        digest_len: usize,
    ) -> Vec<u8> {
        let ctx = pg_hmac_create(ty);
        assert!(!ctx.is_null());
        assert_eq!(pg_hmac_init(ctx, key.as_ptr(), key.len() as Size), 0);
        assert_eq!(pg_hmac_update(ctx, data.as_ptr(), data.len() as Size), 0);
        let mut out = vec![0u8; digest_len];
        assert_eq!(pg_hmac_final(ctx, out.as_mut_ptr(), out.len() as Size), 0);
        pg_hmac_free(ctx);
        out
    }

    /* RFC 4231 / common HMAC test vectors. */
    #[test]
    fn hmac_known_answers() {
        unsafe {
            /* HMAC-MD5(key="", data="") = 74e6f7298a9c2d168935f58c001bad88 */
            assert_eq!(
                hex(&hmac_of(PG_MD5, b"", b"", MD5_DIGEST_LENGTH)),
                "74e6f7298a9c2d168935f58c001bad88"
            );

            /* HMAC-SHA1(key="", data="") = fbdb1d1b18aa6c08324b7d64b71fb76370690e1d */
            assert_eq!(
                hex(&hmac_of(PG_SHA1, b"", b"", SHA1_DIGEST_LENGTH)),
                "fbdb1d1b18aa6c08324b7d64b71fb76370690e1d"
            );

            /* HMAC-SHA256(key="key", data="The quick brown fox jumps over the lazy dog") */
            assert_eq!(
                hex(&hmac_of(
                    PG_SHA256,
                    b"key",
                    b"The quick brown fox jumps over the lazy dog",
                    PG_SHA256_DIGEST_LENGTH
                )),
                "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
            );

            /* HMAC-SHA256(key="", data="") = b613679a0814d9ec772f95d778c35fc5ff1697c493715653c6c712144292c5ad */
            assert_eq!(
                hex(&hmac_of(PG_SHA256, b"", b"", PG_SHA256_DIGEST_LENGTH)),
                "b613679a0814d9ec772f95d778c35fc5ff1697c493715653c6c712144292c5ad"
            );
        }
    }

    /*
     * A key longer than the block size must be hashed down first.  RFC 4231
     * Test Case 6: key = 0xaa * 131, data = "Test Using Larger Than Block-Size
     * Key - Hash Key First", HMAC-SHA256.
     */
    #[test]
    fn hmac_long_key() {
        unsafe {
            let key = [0xaau8; 131];
            let data = b"Test Using Larger Than Block-Size Key - Hash Key First";
            assert_eq!(
                hex(&hmac_of(PG_SHA256, &key, data, PG_SHA256_DIGEST_LENGTH)),
                "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
            );
        }
    }

    /* NULL context behaviors. */
    #[test]
    fn null_context() {
        unsafe {
            assert_eq!(pg_hmac_init(core::ptr::null_mut(), core::ptr::null(), 0), -1);
            assert_eq!(
                pg_hmac_update(core::ptr::null_mut(), core::ptr::null(), 0),
                -1
            );
            assert_eq!(
                pg_hmac_final(core::ptr::null_mut(), core::ptr::null_mut(), 0),
                -1
            );
            /* free(NULL) is a no-op */
            pg_hmac_free(core::ptr::null_mut());
            /* error(NULL) is "out of memory" */
            let errp = pg_hmac_error(core::ptr::null());
            let cs = core::ffi::CStr::from_ptr(errp);
            assert_eq!(cs.to_bytes(), b"out of memory");
        }
    }
}

#[cfg(test)]
mod kat_tests {
    use super::*;
    use crate::common::cryptohash::pg_cryptohash_type;

    #[test]
    fn hmac_sha256_rfc4231_case2() {
        unsafe {
            // RFC 4231 test case 2: key="Jefe", data="what do ya want for nothing?"
            let ctx = pg_hmac_create(pg_cryptohash_type::PG_SHA256);
            assert!(!ctx.is_null());
            let key = b"Jefe";
            assert_eq!(pg_hmac_init(ctx, key.as_ptr(), key.len() as Size), 0);
            let data = b"what do ya want for nothing?";
            assert_eq!(pg_hmac_update(ctx, data.as_ptr(), data.len() as Size), 0);
            let mut out = [0u8; 32];
            assert_eq!(pg_hmac_final(ctx, out.as_mut_ptr(), out.len() as Size), 0);
            pg_hmac_free(ctx);
            let hex: String = out.iter().map(|x| format!("{:02x}", x)).collect();
            assert_eq!(hex, "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
        }
    }
}
