//! Translation of postgres/src/common/md5_common.c
//!   (uses postgres/src/include/common/cryptohash.h
//!        + postgres/src/include/common/md5.h)
//!
//! md5_common.c
//!   Routines shared between all MD5 implementations used for encrypted
//!   passwords.
//!
//! Sverre H. Huseby <sverrehu@online.no>
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/common/md5_common.c
//!
//! Port notes:
//!   - We translate the BACKEND path (#ifndef FRONTEND -> postgres.h). The
//!     FRONTEND (postgres_fe.h) include selection is marked TODO(pg-port).
//!   - pg_md5_hash / pg_md5_binary / pg_md5_encrypt build on the already
//!     translated cryptohash dispatcher: pg_cryptohash_* with PG_MD5.
//!   - The MD5_DIGEST_LENGTH constant comes from crate::common::md5 (a `usize`).
//!   - The C `_()` gettext macro is identity in the in-core fallback; we model
//!     its `const char *` result with a static NUL-terminated byte string, as
//!     done in cryptohash.rs.
//!   - `errstr` is `*mut *const c_char`; we set it on error and to NULL on
//!     success, matching the C `const char **errstr` out-parameter.
//!   - C string helpers (strlen/strcpy) are reproduced byte-for-byte against the
//!     raw C pointers; malloc/free are used verbatim as in the C source (the
//!     crypt_buf scratch buffer uses libc-style malloc/free).

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
// #include "common/md5.h"
use crate::common::cryptohash::{
    pg_cryptohash_create, pg_cryptohash_error, pg_cryptohash_final, pg_cryptohash_free,
    pg_cryptohash_init, pg_cryptohash_update, PG_MD5,
};
use crate::common::md5::MD5_DIGEST_LENGTH;

extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
}

/*
 * static void
 * bytesToHex(uint8 b[16], char *s)
 */
unsafe fn bytesToHex(b: *const uint8, s: *mut c_char) {
    static HEX: &[u8; 16] = b"0123456789abcdef";
    let mut q: c_int;
    let mut w: c_int;

    q = 0;
    w = 0;
    while q < 16 {
        *s.offset(w as isize) =
            HEX[((*b.offset(q as isize) >> 4) & 0x0F) as usize] as c_char;
        w += 1;
        *s.offset(w as isize) = HEX[(*b.offset(q as isize) & 0x0F) as usize] as c_char;
        w += 1;
        q += 1;
    }
    *s.offset(w as isize) = b'\0' as c_char;
}

/*
 *	pg_md5_hash
 *
 *	Calculates the MD5 sum of the bytes in a buffer.
 *
 *	SYNOPSIS	  #include "md5.h"
 *				  bool pg_md5_hash(const void *buff, size_t len, char *hexsum,
 *				                   const char **errstr)
 *
 *	INPUT		  buff	  the buffer containing the bytes that you want
 *						  the MD5 sum of.
 *				  len	  number of bytes in the buffer.
 *
 *	OUTPUT		  hexsum  the MD5 sum as a '\0'-terminated string of
 *						  hexadecimal digits.  an MD5 sum is 16 bytes long.
 *						  each byte is represented by two hexadecimal
 *						  characters.  you thus need to provide an array
 *						  of 33 characters, including the trailing '\0'.
 *
 *				  errstr  filled with a constant-string error message
 *						  on failure return; NULL on success.
 *
 *	RETURNS		  false on failure (out of memory for internal buffers
 *				  or MD5 computation failure) or true on success.
 *
 *	STANDARDS	  MD5 is described in RFC 1321.
 *
 *	AUTHOR		  Sverre H. Huseby <sverrehu@online.no>
 *
 */

pub unsafe fn pg_md5_hash(
    buff: *const c_void,
    len: Size,
    hexsum: *mut c_char,
    errstr: *mut *const c_char,
) -> bool {
    let mut sum: [uint8; MD5_DIGEST_LENGTH] = [0; MD5_DIGEST_LENGTH];
    let ctx;

    *errstr = core::ptr::null();
    ctx = pg_cryptohash_create(PG_MD5);
    if ctx.is_null() {
        *errstr = pg_cryptohash_error(core::ptr::null()); /* returns OOM */
        return false;
    }

    if pg_cryptohash_init(ctx) < 0
        || pg_cryptohash_update(ctx, buff as *const uint8, len) < 0
        || pg_cryptohash_final(ctx, sum.as_mut_ptr(), core::mem::size_of_val(&sum)) < 0
    {
        *errstr = pg_cryptohash_error(ctx);
        pg_cryptohash_free(ctx);
        return false;
    }

    bytesToHex(sum.as_ptr(), hexsum);
    pg_cryptohash_free(ctx);
    true
}

/*
 * pg_md5_binary
 *
 * As above, except that the MD5 digest is returned as a binary string
 * (of size MD5_DIGEST_LENGTH) rather than being converted to ASCII hex.
 */
pub unsafe fn pg_md5_binary(
    buff: *const c_void,
    len: Size,
    outbuf: *mut uint8,
    errstr: *mut *const c_char,
) -> bool {
    let ctx;

    *errstr = core::ptr::null();
    ctx = pg_cryptohash_create(PG_MD5);
    if ctx.is_null() {
        *errstr = pg_cryptohash_error(core::ptr::null()); /* returns OOM */
        return false;
    }

    if pg_cryptohash_init(ctx) < 0
        || pg_cryptohash_update(ctx, buff as *const uint8, len) < 0
        || pg_cryptohash_final(ctx, outbuf, MD5_DIGEST_LENGTH) < 0
    {
        *errstr = pg_cryptohash_error(ctx);
        pg_cryptohash_free(ctx);
        return false;
    }

    pg_cryptohash_free(ctx);
    true
}

/*
 * Computes MD5 checksum of "passwd" (a null-terminated string) followed
 * by "salt" (which need not be null-terminated).
 *
 * Output format is "md5" followed by a 32-hex-digit MD5 checksum.
 * Hence, the output buffer "buf" must be at least 36 bytes long.
 *
 * Returns true if okay, false on error with *errstr providing some
 * error context.
 */
pub unsafe fn pg_md5_encrypt(
    passwd: *const c_char,
    salt: *const uint8,
    salt_len: Size,
    buf: *mut c_char,
    errstr: *mut *const c_char,
) -> bool {
    let passwd_len: Size = strlen(passwd);

    /* +1 here is just to avoid risk of unportable malloc(0) */
    let crypt_buf = malloc(passwd_len + salt_len + 1) as *mut c_char;
    let ret: bool;

    if crypt_buf.is_null() {
        *errstr = _OUT_OF_MEMORY(); /* _("out of memory") */
        return false;
    }

    /*
     * Place salt at the end because it may be known by users trying to crack
     * the MD5 output.
     */
    core::ptr::copy_nonoverlapping(passwd as *const u8, crypt_buf as *mut u8, passwd_len);
    core::ptr::copy_nonoverlapping(
        salt as *const u8,
        crypt_buf.add(passwd_len) as *mut u8,
        salt_len,
    );

    strcpy(buf, b"md5\0".as_ptr() as *const c_char);
    ret = pg_md5_hash(
        crypt_buf as *const c_void,
        passwd_len + salt_len,
        buf.add(3),
        errstr,
    );

    free(crypt_buf as *mut c_void);

    ret
}

/*
 * The gettext translation macro `_()`.  In the in-core fallback build (no NLS),
 * `_("out of memory")` is identity and yields a `const char *` to a
 * NUL-terminated literal.  We model the message as a static NUL-terminated byte
 * string, as done in cryptohash.rs.
 */
const MSG_OUT_OF_MEMORY: &[u8] = b"out of memory\0";

#[inline]
fn _OUT_OF_MEMORY() -> *const c_char {
    MSG_OUT_OF_MEMORY.as_ptr() as *const c_char
}

/*
 * strlen / strcpy helpers operating on raw C strings, mirroring the libc
 * routines used in the C source.  These keep byte-for-byte semantics against
 * the NUL-terminated pointers passed in by callers.
 */
#[inline]
unsafe fn strlen(s: *const c_char) -> Size {
    let mut n: Size = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

#[inline]
unsafe fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char {
    let mut i: Size = 0;
    loop {
        let c = *src.add(i);
        *dst.add(i) = c;
        if c == 0 {
            break;
        }
        i += 1;
    }
    dst
}

#[cfg(test)]
mod tests {
    use super::*;

    /* MD5("abc") = 900150983cd24fb0d6963f7d28e17f72 */
    #[test]
    fn md5_hash_abc() {
        unsafe {
            let mut hexsum = [0i8; 33];
            let mut errstr: *const c_char = core::ptr::null();
            let buff = b"abc";
            let ok = pg_md5_hash(
                buff.as_ptr() as *const c_void,
                buff.len() as Size,
                hexsum.as_mut_ptr(),
                &mut errstr,
            );
            assert!(ok);
            assert!(errstr.is_null());
            let cs = core::ffi::CStr::from_ptr(hexsum.as_ptr());
            assert_eq!(cs.to_bytes(), b"900150983cd24fb0d6963f7d28e17f72");
        }
    }

    /* Binary digest of "abc" must match the canonical MD5 vector. */
    #[test]
    fn md5_binary_abc() {
        unsafe {
            let mut out = [0u8; MD5_DIGEST_LENGTH];
            let mut errstr: *const c_char = core::ptr::null();
            let buff = b"abc";
            let ok = pg_md5_binary(
                buff.as_ptr() as *const c_void,
                buff.len() as Size,
                out.as_mut_ptr(),
                &mut errstr,
            );
            assert!(ok);
            assert!(errstr.is_null());
            let hex: String = out.iter().map(|x| format!("{:02x}", x)).collect();
            assert_eq!(hex, "900150983cd24fb0d6963f7d28e17f72");
        }
    }

    /*
     * pg_md5_encrypt("password", "salt") should yield "md5" + MD5hex of
     * "passwordsalt".  Reference: MD5("passwordsalt").
     */
    #[test]
    fn md5_encrypt_known() {
        unsafe {
            let passwd = b"password\0";
            let salt = b"salt";
            let mut buf = [0i8; 36];
            let mut errstr: *const c_char = core::ptr::null();
            let ok = pg_md5_encrypt(
                passwd.as_ptr() as *const c_char,
                salt.as_ptr(),
                salt.len() as Size,
                buf.as_mut_ptr(),
                &mut errstr,
            );
            assert!(ok);
            let cs = core::ffi::CStr::from_ptr(buf.as_ptr());
            /* "md5" + MD5("passwordsalt") */
            let expected = {
                /* compute MD5("passwordsalt") for cross-check */
                let mut hexsum = [0i8; 33];
                let mut e2: *const c_char = core::ptr::null();
                let combined = b"passwordsalt";
                assert!(pg_md5_hash(
                    combined.as_ptr() as *const c_void,
                    combined.len() as Size,
                    hexsum.as_mut_ptr(),
                    &mut e2,
                ));
                let h = core::ffi::CStr::from_ptr(hexsum.as_ptr())
                    .to_str()
                    .unwrap()
                    .to_string();
                format!("md5{}", h)
            };
            assert_eq!(cs.to_str().unwrap(), expected);
        }
    }
}
