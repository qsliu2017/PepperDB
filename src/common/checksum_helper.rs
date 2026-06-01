//! Translation of postgres/src/common/checksum_helper.c
//!                + postgres/src/include/common/checksum_helper.h (the
//!                  pg_checksum_type enum + pg_checksum_raw_context union +
//!                  pg_checksum_context struct + PG_CHECKSUM_MAX_LENGTH)
//!
//! checksum_helper.c
//!   Compute a checksum of any of various types using common routines.
//!
//! A small dispatch wrapper over the CRC-32C and SHA-2 primitives, used by
//! backup manifests etc.
//!
//! Portions Copyright (c) 2016-2025, PostgreSQL Global Development Group
//!
//! Port notes:
//!   - #include "common/checksum_helper.h" pulls in common/cryptohash.h,
//!     common/sha2.h and port/pg_crc32c.h. We dispatch CRC32C to
//!     crate::port::pg_crc32c (INIT_CRC32C/COMP_CRC32C/FIN_CRC32C, pg_crc32c)
//!     and SHA* to crate::common::cryptohash (pg_cryptohash_create/init/update/
//!     final/free over PG_SHA224/256/384/512).
//!   - The C INIT_CRC32C/COMP_CRC32C/FIN_CRC32C are statement macros that
//!     mutate `crc` in place; our Rust equivalents are pure functions that
//!     return the new accumulator, so we assign the result back into the union
//!     field, preserving the exact semantics.
//!   - pg_checksum_raw_context is a C union of { pg_crc32c c_crc32c;
//!     pg_cryptohash_ctx *c_sha2; }. We model it as a #[repr(C)] union.
//!   - The C `_()` gettext macro and the StaticAssertDecl/Assert checks have no
//!     runtime effect in the in-core build; the static asserts are reproduced
//!     as const assertions, and the digest-length facts hold by construction.
//!   - memcpy is bound via core::ptr::copy_nonoverlapping (the C copies the
//!     4-byte CRC into the output buffer).

#![allow(clippy::missing_safety_doc)]

use crate::prelude::*;

// #include "common/cryptohash.h"
use crate::common::cryptohash::{
    pg_cryptohash_create, pg_cryptohash_ctx, pg_cryptohash_final, pg_cryptohash_free,
    pg_cryptohash_init, pg_cryptohash_update,
};
use crate::common::cryptohash::pg_cryptohash_type::{PG_SHA224, PG_SHA256, PG_SHA384, PG_SHA512};
// #include "common/sha2.h"
use crate::common::sha2::{
    PG_SHA224_DIGEST_LENGTH, PG_SHA256_DIGEST_LENGTH, PG_SHA384_DIGEST_LENGTH,
    PG_SHA512_DIGEST_LENGTH,
};
// #include "port/pg_crc32c.h"
use crate::port::pg_crc32c::{pg_crc32c, COMP_CRC32C, FIN_CRC32C, INIT_CRC32C};
// pg_strcasecmp (from port/pgstrcasecmp.c)
use crate::port::pgstrcasecmp::pg_strcasecmp;

/* ---------------------------------------------------------------------------
 * From the public header src/include/common/checksum_helper.h
 * ------------------------------------------------------------------------- */

/*
 * Supported checksum types. It's not necessarily the case that code using
 * these functions needs a cryptographically strong checksum; it may only
 * need to detect accidental modification. That's why we include CRC-32C: it's
 * much faster than any of the other algorithms. On the other hand, we omit
 * MD5 here because any new that does need a cryptographically strong checksum
 * should use something better.
 *
 * typedef enum pg_checksum_type
 * {
 *     CHECKSUM_TYPE_NONE,
 *     CHECKSUM_TYPE_CRC32C,
 *     CHECKSUM_TYPE_SHA224,
 *     CHECKSUM_TYPE_SHA256,
 *     CHECKSUM_TYPE_SHA384,
 *     CHECKSUM_TYPE_SHA512,
 * } pg_checksum_type;
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum pg_checksum_type {
    CHECKSUM_TYPE_NONE,
    CHECKSUM_TYPE_CRC32C,
    CHECKSUM_TYPE_SHA224,
    CHECKSUM_TYPE_SHA256,
    CHECKSUM_TYPE_SHA384,
    CHECKSUM_TYPE_SHA512,
}
pub use pg_checksum_type::*;

/*
 * This is just a union of all applicable context types.
 *
 * typedef union pg_checksum_raw_context
 * {
 *     pg_crc32c   c_crc32c;
 *     pg_cryptohash_ctx *c_sha2;
 * } pg_checksum_raw_context;
 */
#[repr(C)]
pub union pg_checksum_raw_context {
    pub c_crc32c: pg_crc32c,
    pub c_sha2: *mut pg_cryptohash_ctx,
}

/*
 * This structure provides a convenient way to pass the checksum type and the
 * checksum context around together.
 *
 * typedef struct pg_checksum_context
 * {
 *     pg_checksum_type type;
 *     pg_checksum_raw_context raw_context;
 * } pg_checksum_context;
 */
#[repr(C)]
pub struct pg_checksum_context {
    pub r#type: pg_checksum_type,
    pub raw_context: pg_checksum_raw_context,
}

/*
 * This is the longest possible output for any checksum algorithm supported
 * by this file.
 *
 * #define PG_CHECKSUM_MAX_LENGTH PG_SHA512_DIGEST_LENGTH
 */
pub const PG_CHECKSUM_MAX_LENGTH: usize = PG_SHA512_DIGEST_LENGTH;

/* ---------------------------------------------------------------------------
 * From the implementation src/common/checksum_helper.c
 * ------------------------------------------------------------------------- */

/*
 * If 'name' is a recognized checksum type, set *type to the corresponding
 * constant and return true. Otherwise, set *type to CHECKSUM_TYPE_NONE and
 * return false.
 */
pub unsafe fn pg_checksum_parse_type(
    name: *mut c_char,
    r#type: *mut pg_checksum_type,
) -> bool {
    let mut result_type: pg_checksum_type = CHECKSUM_TYPE_NONE;
    let mut result: bool = true;

    if pg_strcasecmp(name, b"none\0".as_ptr() as *const c_char) == 0 {
        result_type = CHECKSUM_TYPE_NONE;
    } else if pg_strcasecmp(name, b"crc32c\0".as_ptr() as *const c_char) == 0 {
        result_type = CHECKSUM_TYPE_CRC32C;
    } else if pg_strcasecmp(name, b"sha224\0".as_ptr() as *const c_char) == 0 {
        result_type = CHECKSUM_TYPE_SHA224;
    } else if pg_strcasecmp(name, b"sha256\0".as_ptr() as *const c_char) == 0 {
        result_type = CHECKSUM_TYPE_SHA256;
    } else if pg_strcasecmp(name, b"sha384\0".as_ptr() as *const c_char) == 0 {
        result_type = CHECKSUM_TYPE_SHA384;
    } else if pg_strcasecmp(name, b"sha512\0".as_ptr() as *const c_char) == 0 {
        result_type = CHECKSUM_TYPE_SHA512;
    } else {
        result = false;
    }

    *r#type = result_type;
    result
}

/*
 * Get the canonical human-readable name corresponding to a checksum type.
 *
 * The C returns a `char *` to a string literal.  We return a *const c_char to
 * a static NUL-terminated byte string with identical contents.
 */
pub unsafe fn pg_checksum_type_name(r#type: pg_checksum_type) -> *const c_char {
    match r#type {
        CHECKSUM_TYPE_NONE => b"NONE\0".as_ptr() as *const c_char,
        CHECKSUM_TYPE_CRC32C => b"CRC32C\0".as_ptr() as *const c_char,
        CHECKSUM_TYPE_SHA224 => b"SHA224\0".as_ptr() as *const c_char,
        CHECKSUM_TYPE_SHA256 => b"SHA256\0".as_ptr() as *const c_char,
        CHECKSUM_TYPE_SHA384 => b"SHA384\0".as_ptr() as *const c_char,
        CHECKSUM_TYPE_SHA512 => b"SHA512\0".as_ptr() as *const c_char,
    }

    // The C has a trailing `Assert(false); return "???";` after the switch,
    // unreachable because the match above is exhaustive.
}

/*
 * Initialize a checksum context for checksums of the given type.
 * Returns 0 for a success, -1 for a failure.
 */
pub unsafe fn pg_checksum_init(
    context: *mut pg_checksum_context,
    r#type: pg_checksum_type,
) -> c_int {
    (*context).r#type = r#type;

    match r#type {
        CHECKSUM_TYPE_NONE => {
            /* do nothing */
        }
        CHECKSUM_TYPE_CRC32C => {
            /* INIT_CRC32C(context->raw_context.c_crc32c); */
            (*context).raw_context.c_crc32c = INIT_CRC32C();
        }
        CHECKSUM_TYPE_SHA224 => {
            (*context).raw_context.c_sha2 = pg_cryptohash_create(PG_SHA224);
            if (*context).raw_context.c_sha2.is_null() {
                return -1;
            }
            if pg_cryptohash_init((*context).raw_context.c_sha2) < 0 {
                pg_cryptohash_free((*context).raw_context.c_sha2);
                return -1;
            }
        }
        CHECKSUM_TYPE_SHA256 => {
            (*context).raw_context.c_sha2 = pg_cryptohash_create(PG_SHA256);
            if (*context).raw_context.c_sha2.is_null() {
                return -1;
            }
            if pg_cryptohash_init((*context).raw_context.c_sha2) < 0 {
                pg_cryptohash_free((*context).raw_context.c_sha2);
                return -1;
            }
        }
        CHECKSUM_TYPE_SHA384 => {
            (*context).raw_context.c_sha2 = pg_cryptohash_create(PG_SHA384);
            if (*context).raw_context.c_sha2.is_null() {
                return -1;
            }
            if pg_cryptohash_init((*context).raw_context.c_sha2) < 0 {
                pg_cryptohash_free((*context).raw_context.c_sha2);
                return -1;
            }
        }
        CHECKSUM_TYPE_SHA512 => {
            (*context).raw_context.c_sha2 = pg_cryptohash_create(PG_SHA512);
            if (*context).raw_context.c_sha2.is_null() {
                return -1;
            }
            if pg_cryptohash_init((*context).raw_context.c_sha2) < 0 {
                pg_cryptohash_free((*context).raw_context.c_sha2);
                return -1;
            }
        }
    }

    0
}

/*
 * Update a checksum context with new data.
 * Returns 0 for a success, -1 for a failure.
 */
pub unsafe fn pg_checksum_update(
    context: *mut pg_checksum_context,
    input: *const uint8,
    len: Size,
) -> c_int {
    match (*context).r#type {
        CHECKSUM_TYPE_NONE => {
            /* do nothing */
        }
        CHECKSUM_TYPE_CRC32C => {
            /* COMP_CRC32C(context->raw_context.c_crc32c, input, len); */
            (*context).raw_context.c_crc32c = COMP_CRC32C(
                (*context).raw_context.c_crc32c,
                input as *const c_void,
                len,
            );
        }
        CHECKSUM_TYPE_SHA224 | CHECKSUM_TYPE_SHA256 | CHECKSUM_TYPE_SHA384
        | CHECKSUM_TYPE_SHA512 => {
            if pg_cryptohash_update((*context).raw_context.c_sha2, input, len) < 0 {
                return -1;
            }
        }
    }

    0
}

/*
 * Finalize a checksum computation and write the result to an output buffer.
 *
 * The caller must ensure that the buffer is at least PG_CHECKSUM_MAX_LENGTH
 * bytes in length. The return value is the number of bytes actually written,
 * or -1 for a failure.
 */
pub unsafe fn pg_checksum_final(
    context: *mut pg_checksum_context,
    output: *mut uint8,
) -> c_int {
    let mut retval: c_int = 0;

    /*
     * StaticAssertDecl(sizeof(pg_crc32c) <= PG_CHECKSUM_MAX_LENGTH, ...);
     * StaticAssertDecl(PG_SHA224_DIGEST_LENGTH <= PG_CHECKSUM_MAX_LENGTH, ...);
     * StaticAssertDecl(PG_SHA256_DIGEST_LENGTH <= PG_CHECKSUM_MAX_LENGTH, ...);
     * StaticAssertDecl(PG_SHA384_DIGEST_LENGTH <= PG_CHECKSUM_MAX_LENGTH, ...);
     * StaticAssertDecl(PG_SHA512_DIGEST_LENGTH <= PG_CHECKSUM_MAX_LENGTH, ...);
     */
    const _: () = assert!(core::mem::size_of::<pg_crc32c>() <= PG_CHECKSUM_MAX_LENGTH);
    const _: () = assert!(PG_SHA224_DIGEST_LENGTH <= PG_CHECKSUM_MAX_LENGTH);
    const _: () = assert!(PG_SHA256_DIGEST_LENGTH <= PG_CHECKSUM_MAX_LENGTH);
    const _: () = assert!(PG_SHA384_DIGEST_LENGTH <= PG_CHECKSUM_MAX_LENGTH);
    const _: () = assert!(PG_SHA512_DIGEST_LENGTH <= PG_CHECKSUM_MAX_LENGTH);

    match (*context).r#type {
        CHECKSUM_TYPE_NONE => {}
        CHECKSUM_TYPE_CRC32C => {
            /* FIN_CRC32C(context->raw_context.c_crc32c); */
            (*context).raw_context.c_crc32c = FIN_CRC32C((*context).raw_context.c_crc32c);
            retval = core::mem::size_of::<pg_crc32c>() as c_int;
            /* memcpy(output, &context->raw_context.c_crc32c, retval); */
            core::ptr::copy_nonoverlapping(
                core::ptr::addr_of!((*context).raw_context.c_crc32c) as *const u8,
                output,
                retval as usize,
            );
        }
        CHECKSUM_TYPE_SHA224 => {
            retval = PG_SHA224_DIGEST_LENGTH as c_int;
            if pg_cryptohash_final((*context).raw_context.c_sha2, output, retval as Size) < 0 {
                return -1;
            }
            pg_cryptohash_free((*context).raw_context.c_sha2);
        }
        CHECKSUM_TYPE_SHA256 => {
            retval = PG_SHA256_DIGEST_LENGTH as c_int;
            if pg_cryptohash_final((*context).raw_context.c_sha2, output, retval as Size) < 0 {
                return -1;
            }
            pg_cryptohash_free((*context).raw_context.c_sha2);
        }
        CHECKSUM_TYPE_SHA384 => {
            retval = PG_SHA384_DIGEST_LENGTH as c_int;
            if pg_cryptohash_final((*context).raw_context.c_sha2, output, retval as Size) < 0 {
                return -1;
            }
            pg_cryptohash_free((*context).raw_context.c_sha2);
        }
        CHECKSUM_TYPE_SHA512 => {
            retval = PG_SHA512_DIGEST_LENGTH as c_int;
            if pg_cryptohash_final((*context).raw_context.c_sha2, output, retval as Size) < 0 {
                return -1;
            }
            pg_cryptohash_free((*context).raw_context.c_sha2);
        }
    }

    Assert!(retval <= PG_CHECKSUM_MAX_LENGTH as c_int);
    retval
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{:02x}", x)).collect()
    }

    /* Run the full init/update/final pipeline over `input`, returning the
     * digest bytes actually written. */
    unsafe fn checksum_of(ty: pg_checksum_type, input: &[u8]) -> Vec<u8> {
        let mut ctx: pg_checksum_context = pg_checksum_context {
            r#type: CHECKSUM_TYPE_NONE,
            raw_context: pg_checksum_raw_context { c_crc32c: 0 },
        };
        assert_eq!(pg_checksum_init(&mut ctx, ty), 0);
        assert_eq!(
            pg_checksum_update(&mut ctx, input.as_ptr(), input.len() as Size),
            0
        );
        let mut out = [0u8; PG_CHECKSUM_MAX_LENGTH];
        let n = pg_checksum_final(&mut ctx, out.as_mut_ptr());
        assert!(n >= 0);
        out[..n as usize].to_vec()
    }

    /* CRC32C checksum of a known input must match port::pg_crc32c directly. */
    #[test]
    fn crc32c_matches_port() {
        unsafe {
            let msg = b"123456789";
            let digest = checksum_of(CHECKSUM_TYPE_CRC32C, msg);
            assert_eq!(digest.len(), 4);

            // Compute the same value straight from the port primitives.
            let mut crc = INIT_CRC32C();
            crc = COMP_CRC32C(crc, msg.as_ptr() as *const c_void, msg.len());
            crc = FIN_CRC32C(crc);
            // checksum_final memcpys the 4-byte crc in host (little-endian) order.
            assert_eq!(digest.as_slice(), &crc.to_ne_bytes()[..]);
            // Standard CRC-32C check value of "123456789" is 0xE3069283.
            assert_eq!(crc, 0xE3069283);
        }
    }

    /* SHA256 of "abc" matches the FIPS 180-2 known-answer test. */
    #[test]
    fn sha256_abc_kat() {
        unsafe {
            let digest = checksum_of(CHECKSUM_TYPE_SHA256, b"abc");
            assert_eq!(digest.len(), PG_SHA256_DIGEST_LENGTH);
            assert_eq!(
                hex(&digest),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            );
        }
    }

    /* SHA224/384/512 known-answer tests for "abc". */
    #[test]
    fn sha_other_kats() {
        unsafe {
            assert_eq!(
                hex(&checksum_of(CHECKSUM_TYPE_SHA224, b"abc")),
                "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"
            );
            assert_eq!(
                hex(&checksum_of(CHECKSUM_TYPE_SHA384, b"abc")),
                "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"
            );
            assert_eq!(
                hex(&checksum_of(CHECKSUM_TYPE_SHA512, b"abc")),
                "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
            );
        }
    }

    /* CHECKSUM_TYPE_NONE writes nothing and returns 0. */
    #[test]
    fn none_writes_zero() {
        unsafe {
            let digest = checksum_of(CHECKSUM_TYPE_NONE, b"whatever");
            assert_eq!(digest.len(), 0);
        }
    }

    /* pg_checksum_type_name returns the canonical names. */
    #[test]
    fn type_names() {
        unsafe {
            let name = |t| {
                let p = pg_checksum_type_name(t);
                core::ffi::CStr::from_ptr(p).to_str().unwrap().to_string()
            };
            assert_eq!(name(CHECKSUM_TYPE_NONE), "NONE");
            assert_eq!(name(CHECKSUM_TYPE_CRC32C), "CRC32C");
            assert_eq!(name(CHECKSUM_TYPE_SHA224), "SHA224");
            assert_eq!(name(CHECKSUM_TYPE_SHA256), "SHA256");
            assert_eq!(name(CHECKSUM_TYPE_SHA384), "SHA384");
            assert_eq!(name(CHECKSUM_TYPE_SHA512), "SHA512");
        }
    }

    /* pg_checksum_parse_type round-trips the recognized names and rejects junk. */
    #[test]
    fn parse_type() {
        unsafe {
            let mut t: pg_checksum_type = CHECKSUM_TYPE_CRC32C;
            assert!(pg_checksum_parse_type(
                b"none\0".as_ptr() as *mut c_char,
                &mut t
            ));
            assert_eq!(t, CHECKSUM_TYPE_NONE);
            assert!(pg_checksum_parse_type(
                b"SHA512\0".as_ptr() as *mut c_char,
                &mut t
            ));
            assert_eq!(t, CHECKSUM_TYPE_SHA512);
            // Unrecognized -> false, *type set to NONE.
            assert!(!pg_checksum_parse_type(
                b"bogus\0".as_ptr() as *mut c_char,
                &mut t
            ));
            assert_eq!(t, CHECKSUM_TYPE_NONE);
        }
    }
}
