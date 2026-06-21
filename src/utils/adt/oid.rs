//! Translation of postgres/src/backend/utils/adt/oid.c
//!
//! Functions for the built-in type Oid ... also oidvector.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! The .c does:
//!   #include "postgres.h"
//!   #include <ctype.h>
//!   #include <limits.h>
//!   #include "catalog/pg_type.h"
//!   #include "common/int.h"
//!   #include "libpq/pqformat.h"
//!   #include "nodes/miscnodes.h"
//!   #include "nodes/value.h"
//!   #include "utils/array.h"
//!   #include "utils/builtins.h"
//!
//! `postgres.h` -> crate::prelude.  `<ctype.h>`'s isspace is bound directly via
//! `extern "C"` (same convention as bool.rs).  `catalog/pg_type.h`'s OIDOID ->
//! crate::catalog::pg_type_d.  `common/int.h`'s pg_cmp_u32 -> crate::common::int.
//! `nodes/value.h`'s Integer/Float nodes + intVal!/castNode! -> crate::nodes.
//! The `oidvector` struct lives in crate::c (c.h).
//!
//! `uint32in_subr` belongs to utils/adt/numutils.c, which IS now ported, so it is
//! imported from crate::utils::adt::numutils (oidin / oidvectorin / oidparse all
//! call it).  In the C, ERANGE / parse failures are reported via ereturn(escontext,
//! ...), which under a soft-error context returns after stashing the error; the
//! current elog shim has no soft-error path, so numutils reports hard ERRORs.
//! Consequently SOFT_ERROR_OCCURRED(escontext) is always false here, and
//! oidvectorin's soft-error check is rendered as a stubbed always-false test
//! (the C `PG_RETURN_NULL()` branch is unreachable until miscnodes/soft errors
//! are ported).
//!
//! REAL (dependencies translated):
//!   * oidin / oidout, oidvectorin / oidvectorout
//!   * oidrecv / oidsend             -- libpq/pqformat (pq_getmsgint /
//!                                      pq_begintypsend / pq_sendint32 / pq_endtypsend)
//!   * buildoidvector / check_valid_oidvector / oidparse / oid_cmp
//!   * oideq/ne/lt/le/gt/ge, oidlarger / oidsmaller
//!
//! Stubbed (dependencies not yet translated):
//!   * oidvectorrecv / oidvectorsend -- utils/array.c (array_recv / array_send),
//!                                      LOCAL_FCINFO + InitFunctionCallInfoData are
//!                                      available, but array_recv/array_send are not.
//!   * oidvectoreq/ne/lt/le/ge/gt    -- depend on btoidvectorcmp (utils/adt/arrayfuncs.c).

use crate::prelude::*; // Datum, palloc/palloc0/repalloc, ereport!/errmsg!/elog!, oidvector, Oid, etc.
use crate::utils::fmgr::*; // FunctionCallInfo (and the rest of the fmgr.h interface)
// The PG_GETARG_*!/PG_RETURN_*! helpers are #[macro_export] macro_rules! in
// utils/fmgr.rs, so they live at the crate root and must be imported by name
// (a glob `use crate::utils::fmgr::*` does NOT bring exported macros into scope).
use crate::{
    PG_GETARG_CSTRING, PG_GETARG_OID, PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_BYTEA_P,
    PG_RETURN_CSTRING, PG_RETURN_OID, PG_RETURN_POINTER,
};
use crate::catalog::pg_type_d::OIDOID; // catalog/pg_type.h
use crate::common::int::pg_cmp_u32; // common/int.h
use crate::utils::adt::arrayfuncs::array_send; // utils/array.c (array_send)
use crate::lib::stringinfo::{StringInfo, StringInfoData}; // libpq/pqformat.h passes a StringInfo
use crate::libpq::pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgint, pq_sendint32}; // libpq/pqformat.h
use crate::nodes::nodes::{nodeTag, Node, NodeTag}; // nodes/value.h, nodes.h
use crate::nodes::value::{Float, Integer}; // nodes/value.h
use crate::utils::adt::numutils::uint32in_subr; // utils/adt/numutils.c
use crate::{castNode, intVal}; // nodes/value.h accessor macros
use core::ffi::{c_char, c_int, c_void};

/* errcodes.h classification (errcode() shim ignores the value) */
// TODO(pg-port): ERRCODE_* from utils/errcodes.h.
const ERRCODE_DATATYPE_MISMATCH: c_int = 0;

/* C: #define OidVectorSize(n)  (offsetof(oidvector, values) + (n) * sizeof(Oid)) */
#[inline]
fn OidVectorSize(n: c_int) -> Size {
    core::mem::offset_of!(oidvector, values) + (n as Size) * core::mem::size_of::<Oid>()
}

// ----------------------------------------------------------------
//   libc bindings (<ctype.h>, <stdio.h>)
// ----------------------------------------------------------------
//
// oidvectorin trims whitespace with the locale-aware isspace().  oidout /
// oidvectorout format an Oid with snprintf("%u", ...).  The strtoul/errno parse
// machinery now lives in the real crate::utils::adt::numutils::uint32in_subr.
extern "C" {
    fn isspace(ch: c_int) -> c_int;

    // C uses snprintf(result, 12, "%u", o) / sprintf(rp, "%u", v).  libc snprintf
    // is variadic; we bind it for the oidout/oidvectorout decimal conversion to
    // match the C byte-for-byte.  (Format string is a literal; args are uint32.)
    fn snprintf(buf: *mut c_char, size: Size, fmt: *const c_char, ...) -> c_int;
}

/*
 * Format a `*const c_char` C string into a Rust String (lossy, up to the NUL).
 * Used only by the test module's `cstr` helper now that uint32in_subr's error
 * formatting moved to numutils.
 *
 * # Safety
 * `s` must point to a valid NUL-terminated C string.
 */
#[cfg(test)]
unsafe fn cstring_display(s: *const c_char) -> std::string::String {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    let bytes = core::slice::from_raw_parts(s as *const u8, n);
    std::string::String::from_utf8_lossy(bytes).into_owned()
}

/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/

pub unsafe fn oidin(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let result: Oid;

    result = uint32in_subr(s, null_mut(), c"oid".as_ptr(), (*fcinfo).context);
    PG_RETURN_OID!(result);
}

pub unsafe fn oidout(fcinfo: FunctionCallInfo) -> Datum {
    let o: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut c_char = palloc(12) as *mut c_char;

    snprintf(result, 12, c"%u".as_ptr(), o);
    PG_RETURN_CSTRING!(result);
}

/*
 *		oidrecv			- converts external binary format to oid
 */
pub unsafe fn oidrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    // C: PG_RETURN_OID((Oid) pq_getmsgint(buf, sizeof(Oid)));
    PG_RETURN_OID!(pq_getmsgint(buf, core::mem::size_of::<Oid>() as c_int) as Oid);
}

/*
 *		oidsend			- converts oid to binary format
 */
pub unsafe fn oidsend(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Oid = PG_GETARG_OID!(fcinfo, 0);

    // C body:
    //   StringInfoData buf;
    //   pq_begintypsend(&buf);
    //   pq_sendint32(&buf, arg1);
    //   PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
    // The C `buf` is a stack local StringInfoData; pq_begintypsend initStringInfo's
    // it (palloc'ing buf.data) and reserves the 4-byte bytea length word.
    let mut buf: StringInfoData = core::mem::zeroed();
    pq_begintypsend(&mut buf);
    pq_sendint32(&mut buf, arg1);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf));
}

/*
 * construct oidvector given a raw array of Oids
 *
 * If oids is NULL then caller must fill values[] afterward
 *
 * # Safety
 * `oids`, if non-null, must point to at least `n` readable Oids.
 */
pub unsafe fn buildoidvector(oids: *const Oid, n: c_int) -> *mut oidvector {
    let result: *mut oidvector;

    result = palloc0(OidVectorSize(n)) as *mut oidvector;

    if n > 0 && !oids.is_null() {
        core::ptr::copy_nonoverlapping(oids, (*result).values.as_mut_ptr(), n as usize);
    }

    /*
     * Attach standard array header.  For historical reasons, we set the index
     * lower bound to 0 not 1.
     */
    crate::varatt::SET_VARSIZE(result as *mut c_char, OidVectorSize(n) as int32);
    (*result).ndim = 1;
    (*result).dataoffset = 0; /* never any nulls */
    (*result).elemtype = OIDOID;
    (*result).dim1 = n;
    (*result).lbound1 = 0;

    result
}

/*
 * validate that an array object meets the restrictions of oidvector
 *
 * We need this because there are pathways by which a general oid[] array can
 * be cast to oidvector, allowing the type's restrictions to be violated.
 * All code that receives an oidvector as a SQL parameter should check this.
 *
 * # Safety
 * `oidArray` must point to a live oidvector.
 */
pub unsafe fn check_valid_oidvector(oidArray: *const oidvector) {
    /*
     * We insist on ndim == 1 and dataoffset == 0 (that is, no nulls) because
     * otherwise the array's layout will not be what calling code expects.  We
     * needn't be picky about the index lower bound though.  Checking elemtype
     * is just paranoia.
     */
    if (*oidArray).ndim != 1 || (*oidArray).dataoffset != 0 || (*oidArray).elemtype != OIDOID {
        let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
        ereport!(ERROR, errmsg!("array is not a valid oidvector"));
    }
}

/*
 *		oidvectorin			- converts "num num ..." to internal form
 */
pub unsafe fn oidvectorin(fcinfo: FunctionCallInfo) -> Datum {
    let mut oidString: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let mut result: *mut oidvector;
    let mut nalloc: c_int;
    let mut n: c_int;

    nalloc = 32; /* arbitrary initial size guess */
    result = palloc0(OidVectorSize(nalloc)) as *mut oidvector;

    n = 0;
    loop {
        while *oidString != 0 && isspace(*oidString as u8 as c_int) != 0 {
            oidString = oidString.add(1);
        }
        if *oidString == 0 {
            break;
        }

        if n >= nalloc {
            nalloc *= 2;
            result = repalloc(result as *mut c_void, OidVectorSize(nalloc)) as *mut oidvector;
        }

        *(*result).values.as_mut_ptr().add(n as usize) =
            uint32in_subr(oidString, &mut oidString, c"oid".as_ptr(), escontext);
        // C: if (SOFT_ERROR_OCCURRED(escontext)) PG_RETURN_NULL();
        // uint32in_subr reports hard ERRORs under the current elog shim (no
        // soft-error path), so this is statically false; the NULL branch is
        // unreachable until nodes/miscnodes.h soft errors are ported.
        // TODO(pg-port): SOFT_ERROR_OCCURRED(escontext) (nodes/miscnodes.h).
        if soft_error_occurred(escontext) {
            crate::PG_RETURN_NULL!(fcinfo);
        }

        n += 1;
    }

    crate::varatt::SET_VARSIZE(result as *mut c_char, OidVectorSize(n) as int32);
    (*result).ndim = 1;
    (*result).dataoffset = 0; /* never any nulls */
    (*result).elemtype = OIDOID;
    (*result).dim1 = n;
    (*result).lbound1 = 0;

    PG_RETURN_POINTER!(result);
}

/*
 * SOFT_ERROR_OCCURRED(escontext) from nodes/miscnodes.h:
 *   ((escontext) != NULL && IsA(escontext, ErrorSaveContext) &&
 *    ((ErrorSaveContext *) (escontext))->error_occurred)
 * The ErrorSaveContext type is an opaque stub (no error_occurred field yet),
 * and the elog shim never produces soft errors, so this is always false.
 * TODO(pg-port): real ErrorSaveContext + IsA(ErrorSaveContext) check.
 */
#[inline]
unsafe fn soft_error_occurred(escontext: *mut Node) -> bool {
    const T_ErrorSaveContext: c_int = 447;
    !escontext.is_null()
        && *(escontext as *const c_int) == T_ErrorSaveContext
        && (*(escontext as *const crate::nodes::miscnodes::ErrorSaveContext)).error_occurred
}

/*
 *		oidvectorout - converts internal form to "num num ..."
 */
pub unsafe fn oidvectorout(fcinfo: FunctionCallInfo) -> Datum {
    let oidArray: *mut oidvector = PG_GETARG_POINTER!(fcinfo, 0) as *mut oidvector;
    let mut num: c_int;
    let nnums: c_int;
    let mut rp: *mut c_char;
    let result: *mut c_char;

    /* validate input before fetching dim1 */
    check_valid_oidvector(oidArray);
    nnums = (*oidArray).dim1;

    /* assumes sign, 10 digits, ' ' */
    result = palloc((nnums as Size) * 12 + 1) as *mut c_char;
    rp = result;
    num = 0;
    while num < nnums {
        if num != 0 {
            *rp = b' ' as c_char;
            rp = rp.add(1);
        }
        // C: sprintf(rp, "%u", oidArray->values[num]);
        // Use snprintf with a generous bound; an Oid is at most 10 digits.
        snprintf(rp, 12, c"%u".as_ptr(), *(*oidArray).values.as_ptr().add(num as usize));
        // C: while (*++rp != '\0') ;  -- advance rp past the digits just written.
        rp = rp.add(1);
        while *rp != 0 {
            rp = rp.add(1);
        }
        num += 1;
    }
    *rp = b'\0' as c_char;
    PG_RETURN_CSTRING!(result);
}

/*
 *		oidvectorrecv			- converts external binary format to oidvector
 */
pub unsafe fn oidvectorrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    // C body (abridged):
    //   LOCAL_FCINFO(locfcinfo, 3);
    //   InitFunctionCallInfoData(*locfcinfo, fcinfo->flinfo, 3, InvalidOid, NULL, NULL);
    //   locfcinfo->args[0] = PointerGetDatum(buf);     /* the StringInfo */
    //   locfcinfo->args[1] = ObjectIdGetDatum(OIDOID); /* element type */
    //   locfcinfo->args[2] = Int32GetDatum(-1);        /* typmod */
    //   result = (oidvector *) DatumGetPointer(array_recv(locfcinfo));
    //   ... sanity checks via ARR_NDIM/ARR_HASNULL/ARR_ELEMTYPE/ARR_LBOUND ...
    //   PG_RETURN_POINTER(result);
    // TODO(pg-port): utils/array.c (array_recv) not yet translated; the
    // ARR_* accessors (utils/array.h) are likewise unavailable.
    let _ = buf;
    unimplemented!("oidvectorrecv: utils/array.c (array_recv) not yet translated")
}

/*
 *		oidvectorsend			- converts oidvector to binary format
 */
pub unsafe fn oidvectorsend(fcinfo: FunctionCallInfo) -> Datum {
    /* We don't do check_valid_oidvector, since array_send won't care */
    array_send(fcinfo)
}

/*
 *		oidparse				- get OID from ICONST/FCONST node
 *
 * # Safety
 * `node` must be a live Integer or Float value node.
 */
pub unsafe fn oidparse(node: *mut Node) -> Oid {
    match nodeTag(node) {
        NodeTag::T_Integer => intVal!(node) as Oid,
        NodeTag::T_Float => {
            /*
             * Values too large for int4 will be represented as Float constants
             * by the lexer.  Accept these if they are valid OID strings.
             */
            uint32in_subr(
                (*castNode!(Float, T_Float, node)).fval,
                null_mut(),
                c"oid".as_ptr(),
                null_mut(),
            )
        }
        other => {
            elog!(ERROR, "unrecognized node type: {}", other as c_int);
            InvalidOid /* keep compiler quiet */
        }
    }
}

/* qsort comparison function for Oids */
//
// C signature: int oid_cmp(const void *p1, const void *p2).  Used as a qsort()
// comparator; kept with the same void-pointer ABI.
//
// # Safety
// `p1` and `p2` must each point to a readable `Oid`.
pub unsafe fn oid_cmp(p1: *const c_void, p2: *const c_void) -> c_int {
    let v1: Oid = *(p1 as *const Oid);
    let v2: Oid = *(p2 as *const Oid);

    pg_cmp_u32(v1, v2)
}

/*****************************************************************************
 *	 PUBLIC ROUTINES														 *
 *****************************************************************************/

pub unsafe fn oideq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Oid = PG_GETARG_OID!(fcinfo, 0);
    let arg2: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 == arg2);
}

pub unsafe fn oidne(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Oid = PG_GETARG_OID!(fcinfo, 0);
    let arg2: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 != arg2);
}

pub unsafe fn oidlt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Oid = PG_GETARG_OID!(fcinfo, 0);
    let arg2: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 < arg2);
}

pub unsafe fn oidle(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Oid = PG_GETARG_OID!(fcinfo, 0);
    let arg2: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 <= arg2);
}

pub unsafe fn oidge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Oid = PG_GETARG_OID!(fcinfo, 0);
    let arg2: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 >= arg2);
}

pub unsafe fn oidgt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Oid = PG_GETARG_OID!(fcinfo, 0);
    let arg2: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 > arg2);
}

pub unsafe fn oidlarger(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Oid = PG_GETARG_OID!(fcinfo, 0);
    let arg2: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_OID!(if arg1 > arg2 { arg1 } else { arg2 });
}

pub unsafe fn oidsmaller(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Oid = PG_GETARG_OID!(fcinfo, 0);
    let arg2: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_OID!(if arg1 < arg2 { arg1 } else { arg2 });
}

pub unsafe fn oidvectoreq(fcinfo: FunctionCallInfo) -> Datum {
    let cmp: int32 = DatumGetInt32(btoidvectorcmp(fcinfo));

    PG_RETURN_BOOL!(cmp == 0);
}

pub unsafe fn oidvectorne(fcinfo: FunctionCallInfo) -> Datum {
    let cmp: int32 = DatumGetInt32(btoidvectorcmp(fcinfo));

    PG_RETURN_BOOL!(cmp != 0);
}

pub unsafe fn oidvectorlt(fcinfo: FunctionCallInfo) -> Datum {
    let cmp: int32 = DatumGetInt32(btoidvectorcmp(fcinfo));

    PG_RETURN_BOOL!(cmp < 0);
}

pub unsafe fn oidvectorle(fcinfo: FunctionCallInfo) -> Datum {
    let cmp: int32 = DatumGetInt32(btoidvectorcmp(fcinfo));

    PG_RETURN_BOOL!(cmp <= 0);
}

pub unsafe fn oidvectorge(fcinfo: FunctionCallInfo) -> Datum {
    let cmp: int32 = DatumGetInt32(btoidvectorcmp(fcinfo));

    PG_RETURN_BOOL!(cmp >= 0);
}

pub unsafe fn oidvectorgt(fcinfo: FunctionCallInfo) -> Datum {
    let cmp: int32 = DatumGetInt32(btoidvectorcmp(fcinfo));

    PG_RETURN_BOOL!(cmp > 0);
}

/*
 * btoidvectorcmp lives in utils/adt/arrayfuncs.c (the btree support routine for
 * the oidvector type).  It is NOT yet translated, so the six oidvector ordering
 * operators above are effectively stubbed through this shim.
 * TODO(pg-port): btoidvectorcmp (utils/adt/arrayfuncs.c) not yet translated.
 */
unsafe fn btoidvectorcmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: *mut oidvector = PG_GETARG_POINTER!(fcinfo, 0) as *mut oidvector;
    let b: *mut oidvector = PG_GETARG_POINTER!(fcinfo, 1) as *mut oidvector;

    /* We arbitrarily choose to sort first by vector length */
    if (*a).dim1 != (*b).dim1 {
        return crate::postgres::Int32GetDatum((*a).dim1 - (*b).dim1);
    }
    let mut i = 0;
    while i < (*a).dim1 {
        let av = *(*a).values.as_ptr().add(i as usize);
        let bv = *(*b).values.as_ptr().add(i as usize);
        if av != bv {
            return crate::postgres::Int32GetDatum(if av < bv { -1 } else { 1 });
        }
        i += 1;
    }
    crate::postgres::Int32GetDatum(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{CStringGetDatum, DatumGetBool, DatumGetCString, ObjectIdGetDatum};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};

    // Read a NUL-terminated C string returned by oidout into a Rust String.
    unsafe fn cstr(p: *mut c_char) -> std::string::String {
        cstring_display(p)
    }

    // Drive the I/O and comparison functions through the real fmgr call path so
    // the fcinfo-threaded PG_GETARG_*!/PG_RETURN_*! macros are exercised
    // end-to-end, exactly as bool.rs does.
    #[test]
    fn oid_io_and_operators() {
        unsafe {
            // oidin: plain decimal, hex (strtoul base 0), and the historical
            // minus-sign acceptance (-1 wraps to 4294967295 = OID_MAX).
            let d = DirectFunctionCall1Coll(oidin, InvalidOid, CStringGetDatum(c"42".as_ptr()));
            assert_eq!(DatumGetObjectId(d), 42);
            let d = DirectFunctionCall1Coll(oidin, InvalidOid, CStringGetDatum(c"0x2a".as_ptr()));
            assert_eq!(DatumGetObjectId(d), 42);
            let d = DirectFunctionCall1Coll(oidin, InvalidOid, CStringGetDatum(c"  7  ".as_ptr()));
            assert_eq!(DatumGetObjectId(d), 7); // trailing whitespace allowed
            let d = DirectFunctionCall1Coll(oidin, InvalidOid, CStringGetDatum(c"-1".as_ptr()));
            assert_eq!(DatumGetObjectId(d), 4294967295);
            let d =
                DirectFunctionCall1Coll(oidin, InvalidOid, CStringGetDatum(c"4294967295".as_ptr()));
            assert_eq!(DatumGetObjectId(d), 4294967295); // OID_MAX

            // oidout: "%u".
            let s = DatumGetCString(DirectFunctionCall1Coll(
                oidout,
                InvalidOid,
                ObjectIdGetDatum(4294967295),
            ));
            assert_eq!(cstr(s), "4294967295");
            let s =
                DatumGetCString(DirectFunctionCall1Coll(oidout, InvalidOid, ObjectIdGetDatum(0)));
            assert_eq!(cstr(s), "0");

            // round-trip oidin(oidout(x)) == x for a few values.
            for &v in &[0u32, 1, 26, 65535, 16777216, 4294967294] {
                let out = DirectFunctionCall1Coll(oidout, InvalidOid, ObjectIdGetDatum(v));
                let back = DirectFunctionCall1Coll(oidin, InvalidOid, out);
                assert_eq!(DatumGetObjectId(back), v);
            }

            // Comparison operators across a < b pair.
            let a = ObjectIdGetDatum(10);
            let b = ObjectIdGetDatum(20);
            let call = |f: PGFunction, x, y| {
                DatumGetBool(DirectFunctionCall2Coll(f, InvalidOid, x, y))
            };
            assert!(call(oideq, a, a) && !call(oideq, a, b));
            assert!(call(oidne, a, b) && !call(oidne, a, a));
            assert!(call(oidlt, a, b) && !call(oidlt, b, a) && !call(oidlt, a, a));
            assert!(call(oidle, a, b) && call(oidle, a, a) && !call(oidle, b, a));
            assert!(call(oidgt, b, a) && !call(oidgt, a, b));
            assert!(call(oidge, b, a) && call(oidge, a, a) && !call(oidge, a, b));

            // oidlarger / oidsmaller.
            assert_eq!(
                DatumGetObjectId(DirectFunctionCall2Coll(oidlarger, InvalidOid, a, b)),
                20
            );
            assert_eq!(
                DatumGetObjectId(DirectFunctionCall2Coll(oidsmaller, InvalidOid, a, b)),
                10
            );

            // oid_cmp comparator returns sign of (a - b).
            let x: Oid = 10;
            let y: Oid = 20;
            assert!(oid_cmp(&x as *const Oid as *const c_void, &y as *const Oid as *const c_void) < 0);
            assert!(oid_cmp(&y as *const Oid as *const c_void, &x as *const Oid as *const c_void) > 0);
            assert_eq!(
                oid_cmp(&x as *const Oid as *const c_void, &x as *const Oid as *const c_void),
                0
            );
        }
    }

    // buildoidvector + check_valid_oidvector + oidvectorout integration.
    #[test]
    fn oidvector_build_and_out() {
        unsafe {
            let oids: [Oid; 4] = [3, 1, 4, 1];
            let v = buildoidvector(oids.as_ptr(), oids.len() as c_int);
            assert_eq!((*v).ndim, 1);
            assert_eq!((*v).dataoffset, 0);
            assert_eq!((*v).elemtype, OIDOID);
            assert_eq!((*v).dim1, 4);
            assert_eq!((*v).lbound1, 0);
            assert_eq!(*(*v).values.as_ptr().add(0), 3);
            assert_eq!(*(*v).values.as_ptr().add(3), 1);

            // check_valid_oidvector accepts a well-formed vector (no panic).
            check_valid_oidvector(v);

            // oidvectorout: space-separated decimals.
            let s = DatumGetCString(DirectFunctionCall1Coll(
                oidvectorout,
                InvalidOid,
                PointerGetDatum(v as *const c_void),
            ));
            assert_eq!(cstr(s), "3 1 4 1");

            // Empty vector -> empty string.
            let empty = buildoidvector(null(), 0);
            let s = DatumGetCString(DirectFunctionCall1Coll(
                oidvectorout,
                InvalidOid,
                PointerGetDatum(empty as *const c_void),
            ));
            assert_eq!(cstr(s), "");
        }
    }

    // oidvectorin round-trips through oidvectorout.
    #[test]
    fn oidvector_in_out_roundtrip() {
        unsafe {
            let d = DirectFunctionCall1Coll(
                oidvectorin,
                InvalidOid,
                CStringGetDatum(c"  11 22   33 ".as_ptr()),
            );
            let v = DatumGetPointer(d) as *mut oidvector;
            assert_eq!((*v).dim1, 3);
            assert_eq!(*(*v).values.as_ptr().add(0), 11);
            assert_eq!(*(*v).values.as_ptr().add(1), 22);
            assert_eq!(*(*v).values.as_ptr().add(2), 33);

            let s = DatumGetCString(DirectFunctionCall1Coll(
                oidvectorout,
                InvalidOid,
                PointerGetDatum(v as *const c_void),
            ));
            assert_eq!(cstr(s), "11 22 33");
        }
    }

    // oidparse from an Integer node and a Float node (large value as string).
    #[test]
    fn oidparse_nodes() {
        unsafe {
            let int_node = crate::nodes::value::makeInteger(26);
            assert_eq!(oidparse(int_node as *mut Node), 26);

            // A value too large for int4 arrives as a Float (string) node.
            let big = c"4000000000".as_ptr() as *mut c_char;
            let f = crate::nodes::value::makeFloat(big);
            assert_eq!(oidparse(f as *mut Node), 4000000000);
        }
    }

    // oidin rejects non-numeric garbage (uint32in_subr reports a hard ERROR,
    // which the elog shim turns into a panic).
    #[test]
    #[should_panic]
    fn oidin_rejects_garbage() {
        unsafe {
            DirectFunctionCall1Coll(oidin, InvalidOid, CStringGetDatum(c"notanumber".as_ptr()));
        }
    }

    // oidin rejects values out of the Oid (uint32) range.
    #[test]
    #[should_panic]
    fn oidin_rejects_overflow() {
        unsafe {
            DirectFunctionCall1Coll(oidin, InvalidOid, CStringGetDatum(c"4294967296".as_ptr()));
        }
    }

    // oidsend -> oidrecv binary round-trip through the real pqformat path.
    #[test]
    fn oidsend_recv_roundtrip() {
        unsafe {
            for &v in &[0u32, 1, 42, 65535, 16777216, 4294967295] {
                // oidsend produces a bytea whose 4-byte payload (after VARHDRSZ)
                // is the big-endian Oid.
                let sent = DirectFunctionCall1Coll(oidsend, InvalidOid, ObjectIdGetDatum(v));
                let ba = DatumGetPointer(sent) as *mut crate::c::bytea;

                // Feed the bytea payload into a StringInfo and read it back with
                // oidrecv, exactly as the wire protocol path would.
                let buf = palloc(core::mem::size_of::<StringInfoData>()) as StringInfo;
                crate::lib::stringinfo::initStringInfo(buf);
                let payload = crate::varatt::VARDATA_ANY(ba as *const c_char) as *const c_char;
                let plen = crate::varatt::VARSIZE_ANY_EXHDR(ba as *const c_char) as c_int;
                crate::lib::stringinfo::appendBinaryStringInfo(buf, payload as *const c_void, plen);

                let got = DirectFunctionCall1Coll(oidrecv, InvalidOid, PointerGetDatum(buf as *const c_void));
                assert_eq!(DatumGetObjectId(got), v);
            }
        }
    }
}
