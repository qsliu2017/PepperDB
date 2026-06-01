//! Translation of postgres/src/backend/access/hash/hashfunc.c
//!
//! Support functions for the hash access method: datatype-specific hash and
//! hash-extended functions stored in pg_amproc.  These support both hash
//! indexes and hash joins (and some are used by catcache / dynahash).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   common/hashfn.h     -> crate::common::hashfn
//!                          (hash_any/hash_any_extended/hash_uint32/hash_uint32_extended)
//!   utils/builtins.h    -> (oidvector validation lives in crate::utils::adt::oid)
//!   utils/float.h       -> crate::utils::adt::float (get_float8_nan); the -0.0/NaN
//!                          normalization itself uses Rust f32::is_nan()/f64::is_nan().
//!   utils/fmgrprotos.h  -> crate::utils::fmgr (the PG_* fmgr macros)
//!   varatt.h            -> crate::varatt (VARDATA_ANY / VARSIZE_ANY_EXHDR / detoast)
//!   <string.h> strlen   -> bound via extern "C".
//!
//! STUBBED / partially translated (dependency not yet ported):
//!   utils/pg_locale.h (pg_newlocale_from_collation / pg_strnxfrm / pg_locale_t):
//!   the NON-deterministic-collation branch of hashtext / hashtextextended cannot be
//!   ported until utils/pg_locale lands.  Mirroring PG's common (deterministic) path,
//!   we hash the raw VARDATA_ANY bytes and leave a TODO(pg-port) for the strxfrm path.
//!   PG_FREE_IF_COPY is a no-op here (the prelude palloc is leak-tolerant and our
//!   detoast of a plain in-line datum is the identity, so there is no copy to free).

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::*;
use crate::{
    PG_GETARG_CHAR, PG_GETARG_DATUM, PG_GETARG_FLOAT4, PG_GETARG_FLOAT8, PG_GETARG_INT16,
    PG_GETARG_INT32, PG_GETARG_INT64, PG_GETARG_NAME, PG_GETARG_OID, PG_GETARG_POINTER,
    PG_GET_COLLATION, PG_RETURN_UINT32, PG_RETURN_UINT64,
};
use crate::c::{float8, int32, oidvector, text, varlena};
// Disambiguate: both crate::varatt and crate::utils::fmgr export pg_detoast_datum_packed via the
// globs above; the varatt one is the real (identity-for-plain) impl. An explicit use wins over globs.
use crate::varatt::pg_detoast_datum_packed;
use crate::common::hashfn::{
    hash_any, hash_any_extended, hash_uint32, hash_uint32_extended,
};
use crate::utils::adt::float::get_float8_nan;
use crate::utils::adt::oid::check_valid_oidvector;
use core::ffi::{c_char, c_int, c_uchar};

extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_INDETERMINATE_COLLATION: c_int = 0;

/*
 * Datatype-specific hash functions.
 *
 * These support both hash indexes and hash joins.
 *
 * NOTE: some of these are also used by catcache operations, without
 * any direct connection to hash indexes.  Also, the common hash_any
 * routine is also used by dynahash tables.
 */

/* Note: this is used for both "char" and boolean datatypes */
pub unsafe fn hashchar(fcinfo: FunctionCallInfo) -> Datum {
    return hash_uint32(PG_GETARG_CHAR!(fcinfo, 0) as int32 as u32);
}

pub unsafe fn hashcharextended(fcinfo: FunctionCallInfo) -> Datum {
    return hash_uint32_extended(
        PG_GETARG_CHAR!(fcinfo, 0) as int32 as u32,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    );
}

pub unsafe fn hashint2(fcinfo: FunctionCallInfo) -> Datum {
    return hash_uint32(PG_GETARG_INT16!(fcinfo, 0) as int32 as u32);
}

pub unsafe fn hashint2extended(fcinfo: FunctionCallInfo) -> Datum {
    return hash_uint32_extended(
        PG_GETARG_INT16!(fcinfo, 0) as int32 as u32,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    );
}

pub unsafe fn hashint4(fcinfo: FunctionCallInfo) -> Datum {
    return hash_uint32(PG_GETARG_INT32!(fcinfo, 0) as u32);
}

pub unsafe fn hashint4extended(fcinfo: FunctionCallInfo) -> Datum {
    return hash_uint32_extended(
        PG_GETARG_INT32!(fcinfo, 0) as u32,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    );
}

pub unsafe fn hashint8(fcinfo: FunctionCallInfo) -> Datum {
    /*
     * The idea here is to produce a hash value compatible with the values
     * produced by hashint4 and hashint2 for logically equal inputs; this is
     * necessary to support cross-type hash joins across these input types.
     * Since all three types are signed, we can xor the high half of the int8
     * value if the sign is positive, or the complement of the high half when
     * the sign is negative.
     */
    let val: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let mut lohalf: u32 = val as u32;
    let hihalf: u32 = (val >> 32) as u32;

    lohalf ^= if val >= 0 { hihalf } else { !hihalf };

    return hash_uint32(lohalf);
}

pub unsafe fn hashint8extended(fcinfo: FunctionCallInfo) -> Datum {
    /* Same approach as hashint8 */
    let val: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let mut lohalf: u32 = val as u32;
    let hihalf: u32 = (val >> 32) as u32;

    lohalf ^= if val >= 0 { hihalf } else { !hihalf };

    return hash_uint32_extended(lohalf, PG_GETARG_INT64!(fcinfo, 1) as u64);
}

pub unsafe fn hashoid(fcinfo: FunctionCallInfo) -> Datum {
    return hash_uint32(PG_GETARG_OID!(fcinfo, 0) as u32);
}

pub unsafe fn hashoidextended(fcinfo: FunctionCallInfo) -> Datum {
    return hash_uint32_extended(
        PG_GETARG_OID!(fcinfo, 0) as u32,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    );
}

pub unsafe fn hashenum(fcinfo: FunctionCallInfo) -> Datum {
    return hash_uint32(PG_GETARG_OID!(fcinfo, 0) as u32);
}

pub unsafe fn hashenumextended(fcinfo: FunctionCallInfo) -> Datum {
    return hash_uint32_extended(
        PG_GETARG_OID!(fcinfo, 0) as u32,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    );
}

pub unsafe fn hashfloat4(fcinfo: FunctionCallInfo) -> Datum {
    let key: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let mut key8: float8;

    /*
     * On IEEE-float machines, minus zero and zero have different bit patterns
     * but should compare as equal.  We must ensure that they have the same
     * hash value, which is most reliably done this way:
     */
    if key == 0 as float4 {
        PG_RETURN_UINT32!(0);
    }

    /*
     * To support cross-type hashing of float8 and float4, we want to return
     * the same hash value hashfloat8 would produce for an equal float8 value.
     * So, widen the value to float8 and hash that.  (We must do this rather
     * than have hashfloat8 try to narrow its value to float4; that could fail
     * on overflow.)
     */
    key8 = key as float8;

    /*
     * Similarly, NaNs can have different bit patterns but they should all
     * compare as equal.  For backwards-compatibility reasons we force them to
     * have the hash value of a standard float8 NaN.  (You'd think we could
     * replace key with a float4 NaN and then widen it; but on some old
     * platforms, that way produces a different bit pattern.)
     */
    if key8.is_nan() {
        key8 = get_float8_nan();
    }

    return hash_any(
        &key8 as *const float8 as *const c_uchar,
        core::mem::size_of::<float8>() as c_int,
    );
}

pub unsafe fn hashfloat4extended(fcinfo: FunctionCallInfo) -> Datum {
    let key: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let seed: u64 = PG_GETARG_INT64!(fcinfo, 1) as u64;
    let mut key8: float8;

    /* Same approach as hashfloat4 */
    if key == 0 as float4 {
        PG_RETURN_UINT64!(seed);
    }
    key8 = key as float8;
    if key8.is_nan() {
        key8 = get_float8_nan();
    }

    return hash_any_extended(
        &key8 as *const float8 as *const c_uchar,
        core::mem::size_of::<float8>() as c_int,
        seed,
    );
}

pub unsafe fn hashfloat8(fcinfo: FunctionCallInfo) -> Datum {
    let mut key: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    /*
     * On IEEE-float machines, minus zero and zero have different bit patterns
     * but should compare as equal.  We must ensure that they have the same
     * hash value, which is most reliably done this way:
     */
    if key == 0 as float8 {
        PG_RETURN_UINT32!(0);
    }

    /*
     * Similarly, NaNs can have different bit patterns but they should all
     * compare as equal.  For backwards-compatibility reasons we force them to
     * have the hash value of a standard NaN.
     */
    if key.is_nan() {
        key = get_float8_nan();
    }

    return hash_any(
        &key as *const float8 as *const c_uchar,
        core::mem::size_of::<float8>() as c_int,
    );
}

pub unsafe fn hashfloat8extended(fcinfo: FunctionCallInfo) -> Datum {
    let mut key: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let seed: u64 = PG_GETARG_INT64!(fcinfo, 1) as u64;

    /* Same approach as hashfloat8 */
    if key == 0 as float8 {
        PG_RETURN_UINT64!(seed);
    }
    if key.is_nan() {
        key = get_float8_nan();
    }

    return hash_any_extended(
        &key as *const float8 as *const c_uchar,
        core::mem::size_of::<float8>() as c_int,
        seed,
    );
}

pub unsafe fn hashoidvector(fcinfo: FunctionCallInfo) -> Datum {
    let key: *mut oidvector = PG_GETARG_POINTER!(fcinfo, 0) as *mut oidvector;

    check_valid_oidvector(key);
    return hash_any(
        (*key).values.as_ptr() as *const c_uchar,
        (*key).dim1 * core::mem::size_of::<Oid>() as c_int,
    );
}

pub unsafe fn hashoidvectorextended(fcinfo: FunctionCallInfo) -> Datum {
    let key: *mut oidvector = PG_GETARG_POINTER!(fcinfo, 0) as *mut oidvector;

    check_valid_oidvector(key);
    return hash_any_extended(
        (*key).values.as_ptr() as *const c_uchar,
        (*key).dim1 * core::mem::size_of::<Oid>() as c_int,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    );
}

pub unsafe fn hashname(fcinfo: FunctionCallInfo) -> Datum {
    let key: *const c_char = NameStr(&*PG_GETARG_NAME!(fcinfo, 0));

    return hash_any(key as *const c_uchar, strlen(key) as c_int);
}

pub unsafe fn hashnameextended(fcinfo: FunctionCallInfo) -> Datum {
    let key: *const c_char = NameStr(&*PG_GETARG_NAME!(fcinfo, 0));

    return hash_any_extended(
        key as *const c_uchar,
        strlen(key) as c_int,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    );
}

pub unsafe fn hashtext(fcinfo: FunctionCallInfo) -> Datum {
    // PG_GETARG_TEXT_PP(0)
    let key: *mut text = pg_detoast_datum_packed(
        crate::postgres::DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut core::ffi::c_void,
    ) as *mut text;
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let result: Datum;

    if collid == 0 {
        let _ = errcode(ERRCODE_INDETERMINATE_COLLATION);
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for string hashing")
        );
    }

    /*
     * mylocale = pg_newlocale_from_collation(collid);
     *
     * For a DETERMINISTIC collation (the common case) PG hashes the raw bytes.
     * The non-deterministic-collation branch (pg_strnxfrm over a palloc'd
     * transform buffer) needs utils/pg_locale, which is not yet ported.
     *
     * TODO(pg-port): utils/pg_locale (pg_newlocale_from_collation / pg_strnxfrm /
     * pg_locale_t.deterministic) not yet translated.  We proceed with the
     * deterministic (raw-byte) hash for all collations.
     */
    result = hash_any(
        VARDATA_ANY(key as *const c_char) as *const c_uchar,
        VARSIZE_ANY_EXHDR(key as *const c_char) as c_int,
    );

    /* PG_FREE_IF_COPY(key, 0): no-op, detoast of an in-line datum is identity. */

    return result;
}

pub unsafe fn hashtextextended(fcinfo: FunctionCallInfo) -> Datum {
    // PG_GETARG_TEXT_PP(0)
    let key: *mut text = pg_detoast_datum_packed(
        crate::postgres::DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut core::ffi::c_void,
    ) as *mut text;
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let result: Datum;

    if collid == 0 {
        let _ = errcode(ERRCODE_INDETERMINATE_COLLATION);
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for string hashing")
        );
    }

    /*
     * mylocale = pg_newlocale_from_collation(collid);
     *
     * TODO(pg-port): see hashtext - the non-deterministic-collation pg_strnxfrm
     * branch needs utils/pg_locale; we take the deterministic byte-hash path.
     */
    result = hash_any_extended(
        VARDATA_ANY(key as *const c_char) as *const c_uchar,
        VARSIZE_ANY_EXHDR(key as *const c_char) as c_int,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    );

    /* PG_FREE_IF_COPY(key, 0): no-op. */

    return result;
}

/*
 * hashvarlena() can be used for any varlena datatype in which there are
 * no non-significant bits, ie, distinct bitpatterns never compare as equal.
 *
 * (However, you need to define an SQL-level wrapper function around it with
 * the concrete input data type; otherwise hashvalidate() won't accept it.
 * Moreover, at least for built-in types, a C-level wrapper function is also
 * recommended; otherwise, the opr_sanity test will get upset.)
 */
pub unsafe fn hashvarlena(fcinfo: FunctionCallInfo) -> Datum {
    // PG_GETARG_VARLENA_PP(0)
    let key: *mut varlena = pg_detoast_datum_packed(
        crate::postgres::DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut core::ffi::c_void,
    ) as *mut varlena;
    let result: Datum;

    result = hash_any(
        VARDATA_ANY(key as *const c_char) as *const c_uchar,
        VARSIZE_ANY_EXHDR(key as *const c_char) as c_int,
    );

    /* Avoid leaking memory for toasted inputs: PG_FREE_IF_COPY(key, 0) - no-op. */

    return result;
}

pub unsafe fn hashvarlenaextended(fcinfo: FunctionCallInfo) -> Datum {
    // PG_GETARG_VARLENA_PP(0)
    let key: *mut varlena = pg_detoast_datum_packed(
        crate::postgres::DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut core::ffi::c_void,
    ) as *mut varlena;
    let result: Datum;

    result = hash_any_extended(
        VARDATA_ANY(key as *const c_char) as *const c_uchar,
        VARSIZE_ANY_EXHDR(key as *const c_char) as c_int,
        PG_GETARG_INT64!(fcinfo, 1) as u64,
    );

    /* PG_FREE_IF_COPY(key, 0): no-op. */

    return result;
}

pub unsafe fn hashbytea(fcinfo: FunctionCallInfo) -> Datum {
    return hashvarlena(fcinfo);
}

pub unsafe fn hashbyteaextended(fcinfo: FunctionCallInfo) -> Datum {
    return hashvarlenaextended(fcinfo);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{
        CStringGetDatum, DatumGetUInt32, DatumGetUInt64, Int32GetDatum, Int64GetDatum,
        PointerGetDatum,
    };
    use crate::postgres_ext::InvalidOid;
    use crate::utils::adt::varlena::cstring_to_text;
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};

    // A non-Invalid collation so the hashtext indeterminate-collation check passes.
    // (PG_GET_COLLATION just reads fcinfo->fncollation; any non-zero Oid works for
    // the deterministic byte-hash path we take.)
    const DEFAULT_COLLATION_OID: Oid = 100;

    #[test]
    fn hashint4_is_deterministic() {
        unsafe {
            let h1 = DatumGetUInt32(DirectFunctionCall1Coll(
                hashint4,
                InvalidOid,
                Int32GetDatum(12345),
            ));
            let h2 = DatumGetUInt32(DirectFunctionCall1Coll(
                hashint4,
                InvalidOid,
                Int32GetDatum(12345),
            ));
            assert_eq!(h1, h2, "same value must hash to same result");

            // different value -> (almost certainly) different hash
            let h3 = DatumGetUInt32(DirectFunctionCall1Coll(
                hashint4,
                InvalidOid,
                Int32GetDatum(12346),
            ));
            assert_ne!(h1, h3);
        }
    }

    #[test]
    fn hashint8_matches_hashint4_and_hashint2_crosstype() {
        unsafe {
            // Logically-equal small positive int hashes identically across int2/4/8,
            // which is the cross-type hash-join contract hashint8 is built to honor.
            let v: i32 = 4242;
            let h4 = DatumGetUInt32(DirectFunctionCall1Coll(
                hashint4,
                InvalidOid,
                Int32GetDatum(v),
            ));
            let h8 = DatumGetUInt32(DirectFunctionCall1Coll(
                hashint8,
                InvalidOid,
                Int64GetDatum(v as i64),
            ));
            let h2 = DatumGetUInt32(DirectFunctionCall1Coll(
                hashint2,
                InvalidOid,
                crate::postgres::Int16GetDatum(v as i16),
            ));
            assert_eq!(h4, h8);
            assert_eq!(h4, h2);

            // determinism of hashint8 itself
            let a = DatumGetUInt32(DirectFunctionCall1Coll(
                hashint8,
                InvalidOid,
                Int64GetDatum(-987654321987i64),
            ));
            let b = DatumGetUInt32(DirectFunctionCall1Coll(
                hashint8,
                InvalidOid,
                Int64GetDatum(-987654321987i64),
            ));
            assert_eq!(a, b);
        }
    }

    #[test]
    fn hashfloat_normalizes_zero_and_nan() {
        unsafe {
            use crate::postgres::{Float4GetDatum, Float8GetDatum};
            // -0.0 and +0.0 hash equal (both return 0 per the C code).
            let pz = DatumGetUInt32(DirectFunctionCall1Coll(
                hashfloat8,
                InvalidOid,
                Float8GetDatum(0.0f64),
            ));
            let nz = DatumGetUInt32(DirectFunctionCall1Coll(
                hashfloat8,
                InvalidOid,
                Float8GetDatum(-0.0f64),
            ));
            assert_eq!(pz, 0);
            assert_eq!(nz, 0);

            // hashfloat4 widens to float8: an equal float4/float8 hash the same.
            let f4 = DatumGetUInt32(DirectFunctionCall1Coll(
                hashfloat4,
                InvalidOid,
                Float4GetDatum(1.5f32),
            ));
            let f8 = DatumGetUInt32(DirectFunctionCall1Coll(
                hashfloat8,
                InvalidOid,
                Float8GetDatum(1.5f64),
            ));
            assert_eq!(f4, f8);

            // All NaNs map to a single canonical NaN -> identical hash.
            let nan_a = DatumGetUInt32(DirectFunctionCall1Coll(
                hashfloat8,
                InvalidOid,
                Float8GetDatum(f64::NAN),
            ));
            let nan_b = DatumGetUInt32(DirectFunctionCall1Coll(
                hashfloat8,
                InvalidOid,
                Float8GetDatum(f64::from_bits(0x7ff8_0000_dead_beef)),
            ));
            assert_eq!(nan_a, nan_b);
        }
    }

    #[test]
    fn hashtext_is_deterministic() {
        unsafe {
            let t1 = cstring_to_text(c"hello world".as_ptr());
            let t2 = cstring_to_text(c"hello world".as_ptr());
            let t3 = cstring_to_text(c"hello worle".as_ptr());

            let h1 = DatumGetUInt32(DirectFunctionCall1Coll(
                hashtext,
                DEFAULT_COLLATION_OID,
                PointerGetDatum(t1 as *const core::ffi::c_void),
            ));
            let h2 = DatumGetUInt32(DirectFunctionCall1Coll(
                hashtext,
                DEFAULT_COLLATION_OID,
                PointerGetDatum(t2 as *const core::ffi::c_void),
            ));
            let h3 = DatumGetUInt32(DirectFunctionCall1Coll(
                hashtext,
                DEFAULT_COLLATION_OID,
                PointerGetDatum(t3 as *const core::ffi::c_void),
            ));
            assert_eq!(h1, h2, "same string -> same hash");
            assert_ne!(h1, h3, "different string -> different hash");

            // extended variant is also deterministic and seed-sensitive.
            let e1 = DatumGetUInt64(DirectFunctionCall2Coll(
                hashtextextended,
                DEFAULT_COLLATION_OID,
                PointerGetDatum(t1 as *const core::ffi::c_void),
                Int64GetDatum(0),
            ));
            let e2 = DatumGetUInt64(DirectFunctionCall2Coll(
                hashtextextended,
                DEFAULT_COLLATION_OID,
                PointerGetDatum(t1 as *const core::ffi::c_void),
                Int64GetDatum(0),
            ));
            let e3 = DatumGetUInt64(DirectFunctionCall2Coll(
                hashtextextended,
                DEFAULT_COLLATION_OID,
                PointerGetDatum(t1 as *const core::ffi::c_void),
                Int64GetDatum(42),
            ));
            assert_eq!(e1, e2);
            assert_ne!(e1, e3, "different seed -> different extended hash");
        }
    }

    #[test]
    #[should_panic]
    fn hashtext_requires_a_collation() {
        unsafe {
            // collation InvalidOid (0) -> indeterminate-collation ERROR (panic under shim).
            let t = cstring_to_text(c"x".as_ptr());
            DirectFunctionCall1Coll(
                hashtext,
                InvalidOid,
                PointerGetDatum(t as *const core::ffi::c_void),
            );
        }
    }
}
