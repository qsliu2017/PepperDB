//! Translation of postgres/src/backend/utils/adt/name.c
//!
//! Functions for the built-in type "name".
//!
//! name replaces char16 and is carefully implemented so that it is a string of
//! physical length NAMEDATALEN. DO NOT use hard-coded constants anywhere -
//! always use NAMEDATALEN as the symbolic constant!   - jolly 8/21/95
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! The .c does:
//!   #include "postgres.h"
//!   #include "catalog/namespace.h"
//!   #include "catalog/pg_collation.h"
//!   #include "catalog/pg_type.h"
//!   #include "libpq/pqformat.h"
//!   #include "mb/pg_wchar.h"
//!   #include "miscadmin.h"
//!   #include "utils/array.h"
//!   #include "utils/builtins.h"
//!   #include "utils/lsyscache.h"
//!   #include "utils/varlena.h"
//!
//! `postgres.h` -> crate::prelude.  `catalog/pg_collation.h`'s C_COLLATION_OID
//! and `catalog/pg_type.h`'s NAMEOID are taken from the ported catalog modules
//! (crate::catalog::pg_known_oids / pg_type_d).  `NameData`/`Name`/`NameStr`
//! come from c.h, already defined in crate::c (re-exported via the prelude).
//!
//! STUBBED dependencies (not yet translated):
//!   - `utils/varlena.h`'s varstr_cmp / varstr_sortsupport (utils/adt/varlena.c
//!     still mid-translation) => local TODO(pg-port) stubs; the non-C-collation
//!     path of namecmp and all of btnamesortsupport route through them.
//!   - `mb/pg_wchar.h`'s pg_mbcliplen is approximated locally (single-byte clip),
//!     matching the conservative stub used in parser/scansup.rs and
//!     utils/mmgr/mcxt.rs (real multibyte clip lives in mb/mbutils.c).

use crate::prelude::*; // Datum, palloc0, pstrdup, NameData/Name/NameStr, c_char/c_int, elog!/ereport!/errmsg!, etc.
use crate::utils::fmgr::*; // FunctionCallInfo, DirectFunctionCall1, SortSupport, ...
// The PG_GETARG_*!/PG_RETURN_*! helpers are #[macro_export] macro_rules! living
// at the crate root; a glob `use crate::utils::fmgr::*` does NOT import them, so
// they must be brought in by name.
use crate::{
    PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_NAME, PG_GETARG_OID, PG_GETARG_POINTER,
    PG_GET_COLLATION, PG_RETURN_BOOL, PG_RETURN_CSTRING, PG_RETURN_INT32, PG_RETURN_NAME,
};
use crate::catalog::pg_known_oids::C_COLLATION_OID; // catalog/pg_collation.h
use crate::catalog::pg_type_d::NAMEOID; // catalog/pg_type.h
use crate::nodes::execnodes::SortSupport; // SortSupport for btnamesortsupport
use crate::nodes::pg_list::{lfirst_oid, linitial_oid, list_free, list_length, List, ListCell, NIL};
use crate::pg_config::NAMEDATALEN;
use crate::{
    current_cell, foreach, DirectFunctionCall1, PG_RETURN_BYTEA_P, PG_RETURN_DATUM,
    PG_RETURN_NULL, PG_RETURN_POINTER, PG_RETURN_VOID,
};
use crate::postgres::CStringGetDatum;
// catalog/namespace.h, utils/lsyscache.h, utils/array.h
use crate::catalog::namespace::fetch_search_path;
use crate::utils::cache::lsyscache::get_namespace_name;
use crate::utils::adt::arrayfuncs::construct_array_builtin;
use crate::utils::array::ArrayType;
// miscadmin.h
use crate::miscadmin::{GetSessionUserId, GetUserId, GetUserNameFromId};
// libpq/pqformat.h
use crate::libpq::pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgtext, pq_sendtext};
use crate::utils::palloc::{palloc, pfree};
use crate::lib::stringinfo::{StringInfo, StringInfoData}; // libpq/pqformat.h passes a StringInfo
use core::ffi::{c_char, c_int};

// libc string/printf routines (string.h / stdio.h, pulled in via postgres.h in
// the C source).  Bound directly via `extern "C"`, the same convention as
// utils/hash/dynahash.rs and common/username.rs.
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: Size) -> c_int;
    fn snprintf(s: *mut c_char, n: Size, format: *const c_char, ...) -> c_int;
}

/*
 * Private strlen for the `*const c_char` C strings handled here (C uses libc's
 * strlen via string.h).  Counts bytes up to the NUL.
 *
 * # Safety
 * `s` must point to a valid NUL-terminated C string.
 */
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * `pg_mbcliplen(mbstr, len, limit)` (mb/pg_wchar.h): longest prefix of `mbstr`
 * (<= `len` bytes) that fits in `limit` bytes without splitting a multibyte
 * char.  Single-byte approximation for now (see header note).
 *
 * # Safety
 * `mbstr` must be valid for `len` bytes.
 */
unsafe fn pg_mbcliplen(_mbstr: *const c_char, len: c_int, limit: c_int) -> c_int {
    // TODO(pg-port): real multibyte-boundary clip via pg_encoding_mbliplen (mbutils.c).
    Min(len, limit)
}

/*
 * `varstr_cmp` (utils/varlena.h): collation-aware comparison of two strings.
 *
 * # Safety
 * `arg1`/`arg2` are readable for `len1`/`len2` bytes.
 */
unsafe fn varstr_cmp(
    arg1: *const c_char,
    len1: c_int,
    arg2: *const c_char,
    len2: c_int,
    collid: Oid,
) -> c_int {
    // TODO(pg-port): varstr_cmp lives in utils/adt/varlena.c (still mid-translation).
    let _ = (arg1, len1, arg2, len2, collid);
    unimplemented!("varstr_cmp: utils/adt/varlena.c not yet translated")
}

/*
 * `varstr_sortsupport` (utils/varlena.h): install the generic string SortSupport.
 *
 * # Safety
 * `ssup` points to a SortSupport node.
 */
unsafe fn varstr_sortsupport(ssup: SortSupport, typid: Oid, collid: Oid) {
    // TODO(pg-port): varstr_sortsupport lives in utils/adt/varlena.c (still mid-translation).
    let _ = (ssup, typid, collid);
    unimplemented!("varstr_sortsupport: utils/adt/varlena.c not yet translated")
}

/*
 * `NameStr(*name)` writable form.  The c.h `NameStr` helper returns a
 * `*const c_char`; for the memcpy destinations here we need a writable
 * `*mut c_char` to the `data` array, which is what C's `NameStr(*result)`
 * (a plain array lvalue) provides.
 *
 * # Safety
 * `name` must point to a live `NameData`.
 */
#[inline]
unsafe fn name_str_mut(name: Name) -> *mut c_char {
    (*name).data.as_mut_ptr()
}

/*****************************************************************************
 *	 USER I/O ROUTINES (none)												 *
 *****************************************************************************/

/*
 *		namein	- converts cstring to internal representation
 *
 *		Note:
 *				[Old] Currently if strlen(s) < NAMEDATALEN, the extra chars are nulls
 *				Now, always NULL terminated
 */
pub unsafe fn namein(fcinfo: FunctionCallInfo) -> Datum {
    let s: *const c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let result: Name;
    let mut len: c_int;

    len = strlen(s) as c_int;

    /* Truncate oversize input */
    if len >= NAMEDATALEN as c_int {
        len = pg_mbcliplen(s, len, NAMEDATALEN as c_int - 1);
    }

    /* We use palloc0 here to ensure result is zero-padded */
    result = palloc0(NAMEDATALEN) as Name;
    memcpy(
        name_str_mut(result) as *mut c_void,
        s as *const c_void,
        len as Size,
    );

    PG_RETURN_NAME!(result);
}

/*
 *		nameout - converts internal representation to cstring
 */
pub unsafe fn nameout(fcinfo: FunctionCallInfo) -> Datum {
    let s: Name = PG_GETARG_NAME!(fcinfo, 0);

    PG_RETURN_CSTRING!(pstrdup(NameStr(&*s)));
}

/*
 *		namerecv			- converts external binary format to name
 */
pub unsafe fn namerecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let result: Name;
    let str: *mut c_char;
    let mut nbytes: c_int = 0;

    str = pq_getmsgtext(buf, (*buf).len - (*buf).cursor, &raw mut nbytes);
    if nbytes >= NAMEDATALEN as c_int {
        ereport!(
            ERROR,
            errmsg!("identifier too long")
        );
        /* C also: errcode(ERRCODE_NAME_TOO_LONG),
         *         errdetail("Identifier must be less than %d characters.",
         *                   NAMEDATALEN) */
    }
    result = palloc0(NAMEDATALEN) as Name;
    memcpy(result as *mut c_void, str as *const c_void, nbytes as Size);
    pfree(str as *mut c_void);
    PG_RETURN_NAME!(result);
}

/*
 *		namesend			- converts name to binary format
 */
pub unsafe fn namesend(fcinfo: FunctionCallInfo) -> Datum {
    let s: Name = PG_GETARG_NAME!(fcinfo, 0);
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&raw mut buf);
    pq_sendtext(&raw mut buf, NameStr(&*s), strlen(NameStr(&*s)) as c_int);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&raw mut buf));
}

/*****************************************************************************
 *	 COMPARISON/SORTING ROUTINES											 *
 *****************************************************************************/

/*
 *		nameeq	- returns 1 iff arguments are equal
 *		namene	- returns 1 iff arguments are not equal
 *		namelt	- returns 1 iff a < b
 *		namele	- returns 1 iff a <= b
 *		namegt	- returns 1 iff a > b
 *		namege	- returns 1 iff a >= b
 *
 * Note that the use of strncmp with NAMEDATALEN limit is mostly historical;
 * strcmp would do as well, because we do not allow NAME values that don't
 * have a '\0' terminator.  Whatever might be past the terminator is not
 * considered relevant to comparisons.
 */
unsafe fn namecmp(arg1: Name, arg2: Name, collid: Oid) -> c_int {
    /* Fast path for common case used in system catalogs */
    if collid == C_COLLATION_OID {
        return strncmp(NameStr(&*arg1), NameStr(&*arg2), NAMEDATALEN as Size);
    }

    /* Else rely on the varstr infrastructure */
    varstr_cmp(
        NameStr(&*arg1),
        strlen(NameStr(&*arg1)) as c_int,
        NameStr(&*arg2),
        strlen(NameStr(&*arg2)) as c_int,
        collid,
    )
}

pub unsafe fn nameeq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Name = PG_GETARG_NAME!(fcinfo, 0);
    let arg2: Name = PG_GETARG_NAME!(fcinfo, 1);

    PG_RETURN_BOOL!(namecmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)) == 0);
}

pub unsafe fn namene(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Name = PG_GETARG_NAME!(fcinfo, 0);
    let arg2: Name = PG_GETARG_NAME!(fcinfo, 1);

    PG_RETURN_BOOL!(namecmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)) != 0);
}

pub unsafe fn namelt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Name = PG_GETARG_NAME!(fcinfo, 0);
    let arg2: Name = PG_GETARG_NAME!(fcinfo, 1);

    PG_RETURN_BOOL!(namecmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)) < 0);
}

pub unsafe fn namele(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Name = PG_GETARG_NAME!(fcinfo, 0);
    let arg2: Name = PG_GETARG_NAME!(fcinfo, 1);

    PG_RETURN_BOOL!(namecmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)) <= 0);
}

pub unsafe fn namegt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Name = PG_GETARG_NAME!(fcinfo, 0);
    let arg2: Name = PG_GETARG_NAME!(fcinfo, 1);

    PG_RETURN_BOOL!(namecmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)) > 0);
}

pub unsafe fn namege(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Name = PG_GETARG_NAME!(fcinfo, 0);
    let arg2: Name = PG_GETARG_NAME!(fcinfo, 1);

    PG_RETURN_BOOL!(namecmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)) >= 0);
}

pub unsafe fn btnamecmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Name = PG_GETARG_NAME!(fcinfo, 0);
    let arg2: Name = PG_GETARG_NAME!(fcinfo, 1);

    PG_RETURN_INT32!(namecmp(arg1, arg2, PG_GET_COLLATION!(fcinfo)));
}

pub unsafe fn btnamesortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup: SortSupport = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;
    let collid: Oid = (*ssup).ssup_collation;
    let oldcontext: MemoryContext;

    oldcontext = MemoryContextSwitchTo((*ssup).ssup_cxt);

    /* Use generic string SortSupport */
    varstr_sortsupport(ssup, NAMEOID, collid);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_VOID!();
}

/*****************************************************************************
 *	 MISCELLANEOUS PUBLIC ROUTINES											 *
 *****************************************************************************/

/*
 * # Safety
 * `name` must point to a live `NameData`; `str` must be a valid NUL-terminated
 * C string.
 */
pub unsafe fn namestrcpy(name: Name, str: *const c_char) {
    /* NB: We need to zero-pad the destination. */
    strncpy(name_str_mut(name), str, NAMEDATALEN);
    *name_str_mut(name).add(NAMEDATALEN - 1) = b'\0' as c_char;
}

/*
 * Private strncpy matching libc semantics (string.h): copies at most `n` bytes
 * from `src` to `dst`, NUL-padding the remainder of the `n` bytes if `src` is
 * shorter (this NUL-padding is exactly why namestrcpy uses it to zero-pad).
 *
 * # Safety
 * `dst` must be writable for `n` bytes; `src` must be a valid C string.
 */
unsafe fn strncpy(dst: *mut c_char, src: *const c_char, n: usize) {
    let mut i: usize = 0;
    while i < n && *src.add(i) != 0 {
        *dst.add(i) = *src.add(i);
        i += 1;
    }
    while i < n {
        *dst.add(i) = 0;
        i += 1;
    }
}

/*
 * Compare a NAME to a C string
 *
 * Assumes C collation always; be careful when using this for
 * anything but equality checks!
 *
 * # Safety
 * `name`, if non-null, must point to a live `NameData`; `str`, if non-null,
 * must be a valid NUL-terminated C string.
 */
pub unsafe fn namestrcmp(name: Name, str: *const c_char) -> c_int {
    if name.is_null() && str.is_null() {
        return 0;
    }
    if name.is_null() {
        return -1; /* NULL < anything */
    }
    if str.is_null() {
        return 1; /* NULL < anything */
    }
    strncmp(NameStr(&*name), str, NAMEDATALEN as Size)
}

/*
 * SQL-functions CURRENT_USER, SESSION_USER
 */
pub unsafe fn current_user(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    PG_RETURN_DATUM!(DirectFunctionCall1!(
        namein,
        CStringGetDatum(GetUserNameFromId(GetUserId(), false))
    ));
}

pub unsafe fn session_user(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    PG_RETURN_DATUM!(DirectFunctionCall1!(
        namein,
        CStringGetDatum(GetUserNameFromId(GetSessionUserId(), false))
    ));
}

/*
 * SQL-functions CURRENT_SCHEMA, CURRENT_SCHEMAS
 */
pub unsafe fn current_schema(fcinfo: FunctionCallInfo) -> Datum {
    let search_path: *mut List = fetch_search_path(false);
    let nspname: *mut c_char;

    if search_path == NIL {
        PG_RETURN_NULL!(fcinfo);
    }
    nspname = get_namespace_name(linitial_oid(search_path));
    list_free(search_path);
    if nspname.is_null() {
        PG_RETURN_NULL!(fcinfo); /* recently-deleted namespace? */
    }
    PG_RETURN_DATUM!(DirectFunctionCall1!(namein, CStringGetDatum(nspname)));
}

pub unsafe fn current_schemas(fcinfo: FunctionCallInfo) -> Datum {
    let search_path: *mut List = fetch_search_path(PG_GETARG_BOOL!(fcinfo, 0));
    let names: *mut Datum;
    let mut i: c_int;
    let array: *mut ArrayType;

    names = palloc(list_length(search_path) as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    i = 0;
    foreach!(l, search_path, {
        let nspname: *mut c_char;

        nspname = get_namespace_name(lfirst_oid(crate::current_cell!(l)));
        if !nspname.is_null()
        /* watch out for deleted namespace */
        {
            *names.add(i as usize) = DirectFunctionCall1!(namein, CStringGetDatum(nspname));
            i += 1;
        }
    });
    list_free(search_path);

    array = construct_array_builtin(names, i, NAMEOID);

    PG_RETURN_POINTER!(array);
}

/*
 * SQL-function nameconcatoid(name, oid) returns name
 *
 * This is used in the information_schema to produce specific_name columns,
 * which are supposed to be unique per schema.  We achieve that (in an ugly
 * way) by appending the object's OID.  The result is the same as
 *		($1::text || '_' || $2::text)::name
 * except that, if it would not fit in NAMEDATALEN, we make it do so by
 * truncating the name input (not the oid).
 */
pub unsafe fn nameconcatoid(fcinfo: FunctionCallInfo) -> Datum {
    let nam: Name = PG_GETARG_NAME!(fcinfo, 0);
    let oid: Oid = PG_GETARG_OID!(fcinfo, 1);
    let result: Name;
    let mut suffix: [c_char; 20] = [0; 20];
    let suflen: c_int;
    let mut namlen: c_int;

    suflen = snprintf(
        suffix.as_mut_ptr(),
        core::mem::size_of_val(&suffix) as Size,
        c"_%u".as_ptr(),
        oid,
    );
    namlen = strlen(NameStr(&*nam)) as c_int;

    /* Truncate oversize input by truncating name part, not suffix */
    if namlen + suflen >= NAMEDATALEN as c_int {
        namlen = pg_mbcliplen(NameStr(&*nam), namlen, NAMEDATALEN as c_int - 1 - suflen);
    }

    /* We use palloc0 here to ensure result is zero-padded */
    result = palloc0(NAMEDATALEN) as Name;
    memcpy(
        name_str_mut(result) as *mut c_void,
        NameStr(&*nam) as *const c_void,
        namlen as Size,
    );
    memcpy(
        name_str_mut(result).add(namlen as usize) as *mut c_void,
        suffix.as_ptr() as *const c_void,
        suflen as Size,
    );

    PG_RETURN_NAME!(result);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{
        CStringGetDatum, DatumGetCString, DatumGetName, ObjectIdGetDatum, PointerGetDatum,
    };
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};

    // Build a heap NameData from a Rust &str (NUL-padded to NAMEDATALEN), as the
    // input/comparison routines expect a pointer to a zero-padded NameData.
    unsafe fn make_name(s: &str) -> Name {
        let n = palloc0(NAMEDATALEN) as Name;
        let bytes = s.as_bytes();
        assert!(bytes.len() < NAMEDATALEN);
        for (i, &b) in bytes.iter().enumerate() {
            (*n).data[i] = b as c_char;
        }
        n
    }

    unsafe fn cstr_eq(p: *const c_char, expect: &str) -> bool {
        let len = strlen(p);
        let bytes = core::slice::from_raw_parts(p as *const u8, len);
        bytes == expect.as_bytes()
    }

    #[test]
    fn name_io_roundtrip_and_truncation() {
        unsafe {
            // namein -> nameout round-trips a short identifier.
            let d = DirectFunctionCall1Coll(namein, InvalidOid, CStringGetDatum(c"foo".as_ptr()));
            let nm = DatumGetName(d);
            // The NameData must be zero-padded: byte 3 onward is NUL.
            assert_eq!((*nm).data[0] as u8, b'f');
            assert_eq!((*nm).data[3] as u8, 0);
            let out = DatumGetCString(DirectFunctionCall1Coll(
                nameout,
                InvalidOid,
                PointerGetDatum(nm as *const c_void),
            ));
            assert!(cstr_eq(out, "foo"));

            // namein truncates oversize input to NAMEDATALEN-1 bytes (here 63).
            let long: std::string::String = "a".repeat(100);
            let cstr = std::ffi::CString::new(long.as_str()).unwrap();
            let d = DirectFunctionCall1Coll(namein, InvalidOid, CStringGetDatum(cstr.as_ptr()));
            let nm = DatumGetName(d);
            assert_eq!(strlen(NameStr(&*nm)), NAMEDATALEN - 1);
            assert_eq!((*nm).data[NAMEDATALEN - 1] as u8, 0);
        }
    }

    #[test]
    fn name_comparisons_c_collation() {
        unsafe {
            let a = make_name("abc");
            let b = make_name("abd");
            let a2 = make_name("abc");

            // Drive the operators through the fmgr path under the C collation
            // (the fast strncmp path of namecmp).
            let eq = |x: Name, y: Name| {
                DatumGetBool(DirectFunctionCall2Coll(
                    nameeq,
                    C_COLLATION_OID,
                    PointerGetDatum(x as *const c_void),
                    PointerGetDatum(y as *const c_void),
                ))
            };
            let lt = |x: Name, y: Name| {
                DatumGetBool(DirectFunctionCall2Coll(
                    namelt,
                    C_COLLATION_OID,
                    PointerGetDatum(x as *const c_void),
                    PointerGetDatum(y as *const c_void),
                ))
            };
            let ge = |x: Name, y: Name| {
                DatumGetBool(DirectFunctionCall2Coll(
                    namege,
                    C_COLLATION_OID,
                    PointerGetDatum(x as *const c_void),
                    PointerGetDatum(y as *const c_void),
                ))
            };

            assert!(eq(a, a2) && !eq(a, b));
            assert!(lt(a, b) && !lt(b, a));
            assert!(ge(b, a) && ge(a, a2) && !ge(a, b));

            // btnamecmp returns sign of the comparison.
            let cmp = |x: Name, y: Name| {
                DatumGetInt32(DirectFunctionCall2Coll(
                    btnamecmp,
                    C_COLLATION_OID,
                    PointerGetDatum(x as *const c_void),
                    PointerGetDatum(y as *const c_void),
                ))
            };
            assert!(cmp(a, b) < 0 && cmp(b, a) > 0 && cmp(a, a2) == 0);
        }
    }

    #[test]
    fn namestrcpy_zero_pads_and_truncates() {
        unsafe {
            let n = palloc0(NAMEDATALEN) as Name;
            // Pre-dirty the buffer to prove zero-padding actually happens.
            for i in 0..NAMEDATALEN {
                (*n).data[i] = b'X' as c_char;
            }
            namestrcpy(n, c"hello".as_ptr());
            assert!(cstr_eq(NameStr(&*n), "hello"));
            // Everything from index 5 must be zero (strncpy NUL-pads the tail).
            for i in 5..NAMEDATALEN {
                assert_eq!((*n).data[i] as u8, 0, "byte {} not zero-padded", i);
            }

            // Oversize source: truncated to NAMEDATALEN-1, terminator forced.
            let long = std::ffi::CString::new("z".repeat(100)).unwrap();
            namestrcpy(n, long.as_ptr());
            assert_eq!(strlen(NameStr(&*n)), NAMEDATALEN - 1);
            assert_eq!((*n).data[NAMEDATALEN - 1] as u8, 0);
        }
    }

    #[test]
    fn namestrcmp_handles_nulls_and_c_collation() {
        unsafe {
            let a = make_name("abc");
            // NULL handling: NULL == NULL, NULL < anything.
            assert_eq!(namestrcmp(null_mut(), null()), 0);
            assert_eq!(namestrcmp(null_mut(), c"x".as_ptr()), -1);
            assert_eq!(namestrcmp(a, null()), 1);
            // Equality / ordering against a C string.
            assert_eq!(namestrcmp(a, c"abc".as_ptr()), 0);
            assert!(namestrcmp(a, c"abd".as_ptr()) < 0);
            assert!(namestrcmp(a, c"abb".as_ptr()) > 0);
        }
    }

    #[test]
    fn nameconcatoid_appends_oid_suffix() {
        unsafe {
            let nam = make_name("foo");
            let d = DirectFunctionCall2Coll(
                nameconcatoid,
                InvalidOid,
                PointerGetDatum(nam as *const c_void),
                ObjectIdGetDatum(42),
            );
            let nm = DatumGetName(d);
            assert!(cstr_eq(NameStr(&*nm), "foo_42"));

            // Oversize name part is truncated so name+suffix fits in NAMEDATALEN-1.
            let big = make_name(&"a".repeat(NAMEDATALEN - 1));
            let d = DirectFunctionCall2Coll(
                nameconcatoid,
                InvalidOid,
                PointerGetDatum(big as *const c_void),
                ObjectIdGetDatum(7),
            );
            let nm = DatumGetName(d);
            let total = strlen(NameStr(&*nm));
            assert!(total <= NAMEDATALEN - 1);
            // The suffix "_7" must be present at the end.
            assert_eq!((*nm).data[total - 2] as u8, b'_');
            assert_eq!((*nm).data[total - 1] as u8, b'7');
        }
    }
}
