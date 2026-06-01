//! Translation of postgres/src/include/common/scram-common.h
//!                + postgres/src/common/scram-common.c
//!
//! scram-common.c
//!   Shared frontend/backend code for SCRAM authentication
//!
//! This contains the common low-level functions needed in both frontend and
//! backend, for implement the Salted Challenge Response Authentication
//! Mechanism (SCRAM), per IETF's RFC 5802.
//!
//! Portions Copyright (c) 2017-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/common/scram-common.c
//!
//! Port notes:
//!   - We translate the BACKEND path (#ifndef FRONTEND -> postgres.h). The
//!     FRONTEND (postgres_fe.h) include selection, the malloc/free secret
//!     allocation, and the `return NULL`-on-error branches are marked
//!     TODO(pg-port); the backend path uses palloc and elog!(ERROR, ...).
//!   - The PBKDF2 inner loop calls CHECK_FOR_INTERRUPTS(); since miscadmin.h is
//!     not yet ported we model it with a private no-op.
//!   - The C `_()` gettext macro is identity in the in-core fallback; we model
//!     its `const char *` result with static NUL-terminated byte strings, as
//!     done in cryptohash.rs / md5_common.rs.
//!   - `errstr` is `const char **errstr`, i.e. `*mut *const c_char`; we write
//!     through it on error, matching the C out-parameter.

#![allow(clippy::missing_safety_doc)]

use crate::prelude::*;

// #ifndef FRONTEND
// #include "postgres.h"
// #else
// #include "postgres_fe.h"
// #endif
// TODO(pg-port): FRONTEND branch (postgres_fe.h) not ported; backend path active.
//
// #include "common/base64.h"
// #include "common/hmac.h"
// #include "common/scram-common.h"
// #ifndef FRONTEND
// #include "miscadmin.h"
// #endif
// #include "port/pg_bswap.h"
use crate::common::base64::*;
use crate::common::cryptohash::*;
use crate::common::hmac::*;
use crate::common::sha2::*;
use crate::port::pg_bswap::*;

/* ---------------------------------------------------------------------------
 * From the public header src/include/common/scram-common.h
 * ------------------------------------------------------------------------- */

/* Name of SCRAM mechanisms per IANA */
pub const SCRAM_SHA_256_NAME: &[u8] = b"SCRAM-SHA-256\0";
pub const SCRAM_SHA_256_PLUS_NAME: &[u8] = b"SCRAM-SHA-256-PLUS\0"; /* with channel binding */

/* Length of SCRAM keys (client and server) */
pub const SCRAM_SHA_256_KEY_LEN: usize = PG_SHA256_DIGEST_LENGTH;

/*
 * Size of buffers used internally by SCRAM routines, that should be the
 * maximum of SCRAM_SHA_*_KEY_LEN among the hash methods supported.
 */
pub const SCRAM_MAX_KEY_LEN: usize = SCRAM_SHA_256_KEY_LEN;

/*
 * Size of random nonce generated in the authentication exchange.  This
 * is in "raw" number of bytes, the actual nonces sent over the wire are
 * encoded using only ASCII-printable characters.
 */
pub const SCRAM_RAW_NONCE_LEN: c_int = 18;

/*
 * Length of salt when generating new secrets, in bytes.  (It will be stored
 * and sent over the wire encoded in Base64.)  16 bytes is what the example in
 * RFC 7677 uses.
 */
pub const SCRAM_DEFAULT_SALT_LEN: c_int = 16;

/*
 * Default number of iterations when generating secret.  Should be at least
 * 4096 per RFC 7677.
 */
pub const SCRAM_SHA_256_DEFAULT_ITERATIONS: c_int = 4096;

/* ---------------------------------------------------------------------------
 * From the implementation src/common/scram-common.c
 * ------------------------------------------------------------------------- */

/*
 * CHECK_FOR_INTERRUPTS() comes from miscadmin.h (backend only).  That header is
 * not yet ported, so we model the macro as a private no-op.
 */
#[inline]
fn CHECK_FOR_INTERRUPTS() {
    // TODO(pg-port): miscadmin.h interrupts
}

/*
 * Calculate SaltedPassword.
 *
 * The password should already be normalized by SASLprep.  Returns 0 on
 * success, -1 on failure with *errstr pointing to a message about the
 * error details.
 */
pub unsafe fn scram_SaltedPassword(
    password: *const c_char,
    hash_type: pg_cryptohash_type,
    key_length: c_int,
    salt: *const uint8,
    saltlen: c_int,
    iterations: c_int,
    result: *mut uint8,
    errstr: *mut *const c_char,
) -> c_int {
    let password_len: c_int = strlen(password) as c_int;
    let one: uint32 = pg_hton32(1);
    let mut i: c_int;
    let mut j: c_int;
    let mut Ui: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let mut Ui_prev: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let hmac_ctx: *mut pg_hmac_ctx = pg_hmac_create(hash_type);

    if hmac_ctx.is_null() {
        *errstr = pg_hmac_error(core::ptr::null()); /* returns OOM */
        return -1;
    }

    /*
     * Iterate hash calculation of HMAC entry using given salt.  This is
     * essentially PBKDF2 (see RFC2898) with HMAC() as the pseudorandom
     * function.
     */

    /* First iteration */
    if pg_hmac_init(hmac_ctx, password as *const uint8, password_len as Size) < 0
        || pg_hmac_update(hmac_ctx, salt, saltlen as Size) < 0
        || pg_hmac_update(
            hmac_ctx,
            &one as *const uint32 as *const uint8,
            core::mem::size_of::<uint32>(),
        ) < 0
        || pg_hmac_final(hmac_ctx, Ui_prev.as_mut_ptr(), key_length as Size) < 0
    {
        *errstr = pg_hmac_error(hmac_ctx);
        pg_hmac_free(hmac_ctx);
        return -1;
    }

    core::ptr::copy_nonoverlapping(Ui_prev.as_ptr(), result, key_length as usize);

    /* Subsequent iterations */
    i = 1;
    while i < iterations {
        // #ifndef FRONTEND
        /*
         * Make sure that this is interruptible as scram_iterations could be
         * set to a large value.
         */
        CHECK_FOR_INTERRUPTS();
        // #endif

        if pg_hmac_init(hmac_ctx, password as *const uint8, password_len as Size) < 0
            || pg_hmac_update(hmac_ctx, Ui_prev.as_ptr(), key_length as Size) < 0
            || pg_hmac_final(hmac_ctx, Ui.as_mut_ptr(), key_length as Size) < 0
        {
            *errstr = pg_hmac_error(hmac_ctx);
            pg_hmac_free(hmac_ctx);
            return -1;
        }

        j = 0;
        while j < key_length {
            result.add(j as usize)
                .write(*result.add(j as usize) ^ Ui[j as usize]);
            j += 1;
        }
        core::ptr::copy_nonoverlapping(Ui.as_ptr(), Ui_prev.as_mut_ptr(), key_length as usize);

        i += 1;
    }

    pg_hmac_free(hmac_ctx);
    0
}


/*
 * Calculate hash for a NULL-terminated string. (The NULL terminator is
 * not included in the hash).  Returns 0 on success, -1 on failure with *errstr
 * pointing to a message about the error details.
 */
pub unsafe fn scram_H(
    input: *const uint8,
    hash_type: pg_cryptohash_type,
    key_length: c_int,
    result: *mut uint8,
    errstr: *mut *const c_char,
) -> c_int {
    let ctx: *mut pg_cryptohash_ctx;

    ctx = pg_cryptohash_create(hash_type);
    if ctx.is_null() {
        *errstr = pg_cryptohash_error(core::ptr::null()); /* returns OOM */
        return -1;
    }

    if pg_cryptohash_init(ctx) < 0
        || pg_cryptohash_update(ctx, input, key_length as Size) < 0
        || pg_cryptohash_final(ctx, result, key_length as Size) < 0
    {
        *errstr = pg_cryptohash_error(ctx);
        pg_cryptohash_free(ctx);
        return -1;
    }

    pg_cryptohash_free(ctx);
    0
}

/*
 * Calculate ClientKey.  Returns 0 on success, -1 on failure with *errstr
 * pointing to a message about the error details.
 */
pub unsafe fn scram_ClientKey(
    salted_password: *const uint8,
    hash_type: pg_cryptohash_type,
    key_length: c_int,
    result: *mut uint8,
    errstr: *mut *const c_char,
) -> c_int {
    let ctx: *mut pg_hmac_ctx = pg_hmac_create(hash_type);

    if ctx.is_null() {
        *errstr = pg_hmac_error(core::ptr::null()); /* returns OOM */
        return -1;
    }

    if pg_hmac_init(ctx, salted_password, key_length as Size) < 0
        || pg_hmac_update(
            ctx,
            b"Client Key\0".as_ptr() as *const uint8,
            strlen(b"Client Key\0".as_ptr() as *const c_char),
        ) < 0
        || pg_hmac_final(ctx, result, key_length as Size) < 0
    {
        *errstr = pg_hmac_error(ctx);
        pg_hmac_free(ctx);
        return -1;
    }

    pg_hmac_free(ctx);
    0
}

/*
 * Calculate ServerKey.  Returns 0 on success, -1 on failure with *errstr
 * pointing to a message about the error details.
 */
pub unsafe fn scram_ServerKey(
    salted_password: *const uint8,
    hash_type: pg_cryptohash_type,
    key_length: c_int,
    result: *mut uint8,
    errstr: *mut *const c_char,
) -> c_int {
    let ctx: *mut pg_hmac_ctx = pg_hmac_create(hash_type);

    if ctx.is_null() {
        *errstr = pg_hmac_error(core::ptr::null()); /* returns OOM */
        return -1;
    }

    if pg_hmac_init(ctx, salted_password, key_length as Size) < 0
        || pg_hmac_update(
            ctx,
            b"Server Key\0".as_ptr() as *const uint8,
            strlen(b"Server Key\0".as_ptr() as *const c_char),
        ) < 0
        || pg_hmac_final(ctx, result, key_length as Size) < 0
    {
        *errstr = pg_hmac_error(ctx);
        pg_hmac_free(ctx);
        return -1;
    }

    pg_hmac_free(ctx);
    0
}


/*
 * Construct a SCRAM secret, for storing in pg_authid.rolpassword.
 *
 * The password should already have been processed with SASLprep, if necessary!
 *
 * The result is palloc'd or malloc'd, so caller is responsible for freeing it.
 *
 * On error, returns NULL and sets *errstr to point to a message about the
 * error details.
 */
pub unsafe fn scram_build_secret(
    hash_type: pg_cryptohash_type,
    key_length: c_int,
    salt: *const uint8,
    saltlen: c_int,
    iterations: c_int,
    password: *const c_char,
    errstr: *mut *const c_char,
) -> *mut c_char {
    let mut salted_password: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let mut stored_key: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let mut server_key: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let result: *mut c_char;
    let mut p: *mut c_char;
    let maxlen: c_int;
    let encoded_salt_len: c_int;
    let encoded_stored_len: c_int;
    let encoded_server_len: c_int;
    let mut encoded_result: c_int;

    /* Only this hash method is supported currently */
    Assert!(hash_type == PG_SHA256);

    Assert!(iterations > 0);

    /* Calculate StoredKey and ServerKey */
    if scram_SaltedPassword(
        password,
        hash_type,
        key_length,
        salt,
        saltlen,
        iterations,
        salted_password.as_mut_ptr(),
        errstr,
    ) < 0
        || scram_ClientKey(
            salted_password.as_ptr(),
            hash_type,
            key_length,
            stored_key.as_mut_ptr(),
            errstr,
        ) < 0
        || scram_H(
            stored_key.as_ptr(),
            hash_type,
            key_length,
            stored_key.as_mut_ptr(),
            errstr,
        ) < 0
        || scram_ServerKey(
            salted_password.as_ptr(),
            hash_type,
            key_length,
            server_key.as_mut_ptr(),
            errstr,
        ) < 0
    {
        /* errstr is filled already here */
        // #ifdef FRONTEND
        //     return NULL;
        // #else
        // TODO(pg-port): FRONTEND `return NULL;` branch not ported; backend errors.
        elog!(
            ERROR,
            "could not calculate stored key and server key: {}",
            cstr_to_string(*errstr)
        );
        // #endif
    }

    /*----------
     * The format is:
     * SCRAM-SHA-256$<iteration count>:<salt>$<StoredKey>:<ServerKey>
     *----------
     */
    encoded_salt_len = pg_b64_enc_len(saltlen);
    encoded_stored_len = pg_b64_enc_len(key_length);
    encoded_server_len = pg_b64_enc_len(key_length);

    maxlen = strlen(b"SCRAM-SHA-256\0".as_ptr() as *const c_char) as c_int + 1
        + 10 + 1                    /* iteration count */
        + encoded_salt_len + 1      /* Base64-encoded salt */
        + encoded_stored_len + 1    /* Base64-encoded StoredKey */
        + encoded_server_len + 1; /* Base64-encoded ServerKey */

    // #ifdef FRONTEND
    //     result = malloc(maxlen);
    //     if (!result)
    //     {
    //         *errstr = _("out of memory");
    //         return NULL;
    //     }
    // #else
    // TODO(pg-port): FRONTEND malloc/OOM branch not ported; backend uses palloc.
    result = palloc(maxlen as Size) as *mut c_char;
    // #endif

    /*
     * p = result + sprintf(result, "SCRAM-SHA-256$%d:", iterations);
     *
     * sprintf returns the number of bytes written (excluding the NUL); we
     * reproduce it with a small private helper that writes the same prefix.
     */
    p = result.add(sprintf_scram_prefix(result, iterations) as usize);

    /* salt */
    encoded_result = pg_b64_encode(salt, saltlen, p, encoded_salt_len);
    if encoded_result < 0 {
        *errstr = _COULD_NOT_ENCODE_SALT();
        // #ifdef FRONTEND
        //     free(result);
        //     return NULL;
        // #else
        // TODO(pg-port): FRONTEND free/return-NULL branch not ported.
        elog!(ERROR, "{}", cstr_to_string(*errstr));
        // #endif
    }
    p = p.add(encoded_result as usize);
    *p = b'$' as c_char;
    p = p.add(1);

    /* stored key */
    encoded_result = pg_b64_encode(stored_key.as_ptr(), key_length, p, encoded_stored_len);
    if encoded_result < 0 {
        *errstr = _COULD_NOT_ENCODE_STORED_KEY();
        // #ifdef FRONTEND
        //     free(result);
        //     return NULL;
        // #else
        // TODO(pg-port): FRONTEND free/return-NULL branch not ported.
        elog!(ERROR, "{}", cstr_to_string(*errstr));
        // #endif
    }

    p = p.add(encoded_result as usize);
    *p = b':' as c_char;
    p = p.add(1);

    /* server key */
    encoded_result = pg_b64_encode(server_key.as_ptr(), key_length, p, encoded_server_len);
    if encoded_result < 0 {
        *errstr = _COULD_NOT_ENCODE_SERVER_KEY();
        // #ifdef FRONTEND
        //     free(result);
        //     return NULL;
        // #else
        // TODO(pg-port): FRONTEND free/return-NULL branch not ported.
        elog!(ERROR, "{}", cstr_to_string(*errstr));
        // #endif
    }

    p = p.add(encoded_result as usize);
    *p = b'\0' as c_char;
    p = p.add(1);

    Assert!((p.offset_from(result) as c_int) <= maxlen);

    result
}

/*
 * The gettext translation macro `_()`.  In the in-core fallback build (no NLS),
 * `_(x)` is identity and yields a `const char *` to a NUL-terminated literal.
 * We model the message literals as static NUL-terminated byte strings, as done
 * in cryptohash.rs / md5_common.rs.
 */
const MSG_COULD_NOT_ENCODE_SALT: &[u8] = b"could not encode salt\0";
const MSG_COULD_NOT_ENCODE_STORED_KEY: &[u8] = b"could not encode stored key\0";
const MSG_COULD_NOT_ENCODE_SERVER_KEY: &[u8] = b"could not encode server key\0";

#[inline]
fn _COULD_NOT_ENCODE_SALT() -> *const c_char {
    MSG_COULD_NOT_ENCODE_SALT.as_ptr() as *const c_char
}
#[inline]
fn _COULD_NOT_ENCODE_STORED_KEY() -> *const c_char {
    MSG_COULD_NOT_ENCODE_STORED_KEY.as_ptr() as *const c_char
}
#[inline]
fn _COULD_NOT_ENCODE_SERVER_KEY() -> *const c_char {
    MSG_COULD_NOT_ENCODE_SERVER_KEY.as_ptr() as *const c_char
}

/*
 * Reproduce `sprintf(result, "SCRAM-SHA-256$%d:", iterations)`: write the
 * literal "SCRAM-SHA-256$", then the decimal of `iterations`, then ':', and a
 * trailing NUL (as sprintf does).  Returns the number of bytes written,
 * excluding the trailing NUL -- exactly what C sprintf returns.
 *
 * `iterations` is asserted > 0 by the caller, so we only need to format a
 * non-negative decimal.
 */
unsafe fn sprintf_scram_prefix(result: *mut c_char, iterations: c_int) -> c_int {
    let prefix = b"SCRAM-SHA-256$";
    let mut n: c_int = 0;

    /* literal prefix */
    let mut k: usize = 0;
    while k < prefix.len() {
        *result.add(n as usize) = prefix[k] as c_char;
        n += 1;
        k += 1;
    }

    /* decimal of iterations (> 0) */
    let mut digits: [u8; 10] = [0; 10];
    let mut ndig: usize = 0;
    let mut v: u32 = iterations as u32;
    if v == 0 {
        digits[ndig] = b'0';
        ndig += 1;
    } else {
        while v > 0 {
            digits[ndig] = b'0' + (v % 10) as u8;
            ndig += 1;
            v /= 10;
        }
    }
    /* digits were produced in reverse; emit most-significant first */
    let mut d = ndig;
    while d > 0 {
        d -= 1;
        *result.add(n as usize) = digits[d] as c_char;
        n += 1;
    }

    /* trailing ':' */
    *result.add(n as usize) = b':' as c_char;
    n += 1;

    /* sprintf NUL-terminates but does not count the NUL in its return */
    *result.add(n as usize) = b'\0' as c_char;

    n
}

/*
 * Minimal `strlen` over a C string (mirrors libc strlen), as in the sibling
 * modules; the prelude does not export it.
 */
#[inline]
unsafe fn strlen(s: *const c_char) -> Size {
    let mut n: Size = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * Render a NUL-terminated C string for use as the `{}` argument to elog!.  The
 * C source passes `*errstr` directly to elog's `%s`; here we materialize a Rust
 * string view of the same bytes for the formatting shim.
 */
#[inline]
unsafe fn cstr_to_string(s: *const c_char) -> String {
    if s.is_null() {
        return String::new();
    }
    let len = strlen(s);
    let bytes = core::slice::from_raw_parts(s as *const u8, len);
    String::from_utf8_lossy(bytes).into_owned()
}
