//! Translation of postgres/src/backend/utils/adt/cryptohashfuncs.c
//!
//! SQL-callable cryptographic hash functions: md5(text|bytea) -> hex text, and
//! sha224/256/384/512(bytea) -> bytea.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped: common/cryptohash.h -> crate::common::cryptohash,
//! common/md5.h -> crate::common::md5_common (pg_md5_hash), common/sha2.h ->
//! crate::common::sha2 (PG_SHA*_DIGEST_LENGTH), varatt.h -> crate::varatt.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::{pg_detoast_datum_packed, SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};
use crate::PG_GETARG_DATUM;
use crate::c::{bytea, text, uint8};
use crate::common::cryptohash::{
    pg_cryptohash_create, pg_cryptohash_error, pg_cryptohash_final, pg_cryptohash_free,
    pg_cryptohash_init, pg_cryptohash_type, pg_cryptohash_update,
};
use crate::common::md5_common::pg_md5_hash;
use crate::common::sha2::{
    PG_SHA224_DIGEST_LENGTH, PG_SHA256_DIGEST_LENGTH, PG_SHA384_DIGEST_LENGTH,
    PG_SHA512_DIGEST_LENGTH,
};
use crate::postgres::{DatumGetPointer, PointerGetDatum};
use crate::utils::adt::varlena::cstring_to_text;
use core::ffi::{c_char, c_int, c_void};

/* md5.h: the hex-string length of an MD5 hash (not counting the trailing NUL). */
const MD5_HASH_LEN: usize = 32;

/* errcodes.h (errcode() shim ignores the value). */
const ERRCODE_INTERNAL_ERROR: c_int = 0;

/*
 * Create an MD5 hash of a text value and return it as hex string.
 */
pub unsafe fn md5_text(fcinfo: FunctionCallInfo) -> Datum {
    let in_text: *mut text =
        pg_detoast_datum_packed(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut c_void) as *mut text;
    let len: Size;
    let mut hexsum = [0i8; MD5_HASH_LEN + 1];
    let mut errstr: *const c_char = null();

    /* Calculate the length of the buffer using varlena metadata */
    len = VARSIZE_ANY_EXHDR(in_text as *const c_char) as Size;

    /* get the hash result */
    if !pg_md5_hash(VARDATA_ANY(in_text as *const c_char) as *const c_void, len, hexsum.as_mut_ptr(), &mut errstr) {
        let _ = errcode(ERRCODE_INTERNAL_ERROR);
        ereport!(ERROR, errmsg!("could not compute {} hash: {}", "MD5", cstr(errstr)));
    }

    /* convert to text and return it */
    return PointerGetDatum(cstring_to_text(hexsum.as_ptr()) as *const c_void); // PG_RETURN_TEXT_P
}

/*
 * Create an MD5 hash of a bytea value and return it as a hex string.
 */
pub unsafe fn md5_bytea(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut bytea =
        pg_detoast_datum_packed(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut c_void) as *mut bytea;
    let len: Size;
    let mut hexsum = [0i8; MD5_HASH_LEN + 1];
    let mut errstr: *const c_char = null();

    len = VARSIZE_ANY_EXHDR(r#in as *const c_char) as Size;
    if !pg_md5_hash(VARDATA_ANY(r#in as *const c_char) as *const c_void, len, hexsum.as_mut_ptr(), &mut errstr) {
        let _ = errcode(ERRCODE_INTERNAL_ERROR);
        ereport!(ERROR, errmsg!("could not compute {} hash: {}", "MD5", cstr(errstr)));
    }

    return PointerGetDatum(cstring_to_text(hexsum.as_ptr()) as *const c_void); // PG_RETURN_TEXT_P
}

/*
 * Internal routine to compute a cryptohash with the given bytea input.
 *
 * # Safety
 * `input` is a valid (detoasted) bytea.
 */
unsafe fn cryptohash_internal(r#type: pg_cryptohash_type, input: *mut bytea) -> *mut bytea {
    let data: *const uint8;
    let typestr: &str;
    let digest_len: c_int;
    let len: Size;
    let ctx: *mut crate::common::cryptohash::pg_cryptohash_ctx;
    let result: *mut bytea;

    match r#type {
        pg_cryptohash_type::PG_SHA224 => {
            typestr = "SHA224";
            digest_len = PG_SHA224_DIGEST_LENGTH as c_int;
        }
        pg_cryptohash_type::PG_SHA256 => {
            typestr = "SHA256";
            digest_len = PG_SHA256_DIGEST_LENGTH as c_int;
        }
        pg_cryptohash_type::PG_SHA384 => {
            typestr = "SHA384";
            digest_len = PG_SHA384_DIGEST_LENGTH as c_int;
        }
        pg_cryptohash_type::PG_SHA512 => {
            typestr = "SHA512";
            digest_len = PG_SHA512_DIGEST_LENGTH as c_int;
        }
        pg_cryptohash_type::PG_MD5 | pg_cryptohash_type::PG_SHA1 => {
            elog!(ERROR, "unsupported cryptohash type {}", r#type as c_int);
            #[allow(unreachable_code)]
            {
                return null_mut();
            }
        }
    }

    result = palloc0((digest_len + VARHDRSZ) as Size) as *mut bytea;
    len = VARSIZE_ANY_EXHDR(input as *const c_char) as Size;
    data = VARDATA_ANY(input as *const c_char) as *const uint8;

    ctx = pg_cryptohash_create(r#type);
    if pg_cryptohash_init(ctx) < 0 {
        elog!(ERROR, "could not initialize {} context: {}", typestr, cstr(pg_cryptohash_error(ctx)));
    }
    if pg_cryptohash_update(ctx, data, len) < 0 {
        elog!(ERROR, "could not update {} context: {}", typestr, cstr(pg_cryptohash_error(ctx)));
    }
    if pg_cryptohash_final(ctx, VARDATA(result as *const c_char) as *mut uint8, digest_len as Size) < 0 {
        elog!(ERROR, "could not finalize {} context: {}", typestr, cstr(pg_cryptohash_error(ctx)));
    }
    pg_cryptohash_free(ctx);

    SET_VARSIZE(result as *mut c_char, digest_len + VARHDRSZ);

    result
}

/*
 * SHA-2 variants
 */

pub unsafe fn sha224_bytea(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut bytea =
        pg_detoast_datum_packed(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut c_void) as *mut bytea;
    let result = cryptohash_internal(pg_cryptohash_type::PG_SHA224, r#in);
    return PointerGetDatum(result as *const c_void);
}

pub unsafe fn sha256_bytea(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut bytea =
        pg_detoast_datum_packed(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut c_void) as *mut bytea;
    let result = cryptohash_internal(pg_cryptohash_type::PG_SHA256, r#in);
    return PointerGetDatum(result as *const c_void);
}

pub unsafe fn sha384_bytea(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut bytea =
        pg_detoast_datum_packed(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut c_void) as *mut bytea;
    let result = cryptohash_internal(pg_cryptohash_type::PG_SHA384, r#in);
    return PointerGetDatum(result as *const c_void);
}

pub unsafe fn sha512_bytea(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut bytea =
        pg_detoast_datum_packed(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut c_void) as *mut bytea;
    let result = cryptohash_internal(pg_cryptohash_type::PG_SHA512, r#in);
    return PointerGetDatum(result as *const c_void);
}

/*
 * Format a C string for an error message via Rust `{}` (lossy).
 *
 * # Safety
 * `s` is null or a valid NUL-terminated C string.
 */
unsafe fn cstr(s: *const c_char) -> std::string::String {
    if s.is_null() {
        return std::string::String::new();
    }
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    std::string::String::from_utf8_lossy(core::slice::from_raw_parts(s as *const u8, n)).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_ext::InvalidOid;
    use crate::utils::adt::varlena::{cstring_to_text as mk_text, text_to_cstring};
    use crate::utils::fmgr::DirectFunctionCall1Coll;

    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn md5_and_sha_known_vectors() {
        unsafe {
            // md5('abc') = 900150983cd24fb0d6963f7d28e17f72
            let t = mk_text(c"abc".as_ptr());
            let d = DirectFunctionCall1Coll(md5_text, InvalidOid, PointerGetDatum(t as *const c_void));
            let s = text_to_cstring(DatumGetPointer(d) as *const text);
            assert!(cstr_eq(s, "900150983cd24fb0d6963f7d28e17f72"));

            // sha256('abc') = ba7816bf...b410ff61f20015ad (a bytea built from "abc" has the
            // same varlena bytes as the text, so we can pass it to the bytea function).
            let bt = mk_text(c"abc".as_ptr());
            let h = DirectFunctionCall1Coll(sha256_bytea, InvalidOid, PointerGetDatum(bt as *const c_void));
            let hp = DatumGetPointer(h) as *const c_char;
            assert_eq!(VARSIZE_ANY_EXHDR(hp) as usize, 32);
            let data = core::slice::from_raw_parts(VARDATA_ANY(hp) as *const u8, 32);
            assert_eq!(data[0], 0xba);
            assert_eq!(data[1], 0x78);
            assert_eq!(data[31], 0xad);
        }
    }
}
