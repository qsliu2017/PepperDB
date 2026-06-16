//! Translation of postgres/src/backend/utils/adt/int.c
//!
//! Functions for the built-in integer types (except int8).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped: common/int.h -> crate::common::int (pg_{add,sub,mul}_s{16,32,64}_overflow),
//! utils/builtins.h's pg_strtoint*/pg_itoa/pg_ltoa -> crate::utils::adt::numutils,
//! catalog/pg_type.h INT2OID -> crate::catalog::pg_type_d.
//!
//! STUBBED (dependency not yet ported):
//!  - int2recv/int2send/int4recv/int4send: libpq/pqformat (pq_getmsgint/pq_sendint*).
//!  - buildint2vector/check_valid_int2vector/int2vectorin/out/recv/send: the int2vector
//!    array type + utils/array.h (array_recv/array_send/ARR_* / SET_VARSIZE).
//!  - generate_series_int4[_step|_support]: SRF infrastructure (funcapi.h) + planner
//!    support nodes (nodes/supportnodes.h, optimizer/optimizer.h).

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{
    PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_DATUM, PG_GETARG_INT16, PG_GETARG_INT32,
    PG_GETARG_INT64, PG_GETARG_POINTER, PG_NARGS, PG_RETURN_BOOL, PG_RETURN_CSTRING,
    PG_RETURN_INT16, PG_RETURN_INT32, PG_RETURN_NULL, PG_RETURN_POINTER,
};
use crate::c::{int16, int32, int64, int2vector, PG_INT16_MIN, PG_INT32_MIN};
use crate::catalog::pg_type_d::INT2OID;
use crate::common::int::{
    pg_add_s16_overflow, pg_add_s32_overflow, pg_add_s64_overflow, pg_mul_s16_overflow,
    pg_mul_s32_overflow, pg_sub_s16_overflow, pg_sub_s32_overflow,
};
use crate::postgres::{DatumGetInt32, Int32GetDatum, PointerGetDatum};
use crate::postgres_ext::InvalidOid;
use crate::nodes::nodes::Node;
use crate::nodes::supportnodes::SupportRequestRows;
use crate::nodes::primnodes::{Const, FuncExpr};
use crate::nodes::pg_list::{linitial, list_length, lsecond, lthird, List};
use crate::optimizer::util::clauses::estimate_expression_value;
use crate::IsA;
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::libpq::pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgint, pq_sendint16, pq_sendint32};
use crate::utils::adt::numutils::{pg_itoa, pg_ltoa, pg_strtoint16_safe, pg_strtoint32_safe};
use core::ffi::{c_char, c_int, c_void};

// SHRT_MIN/SHRT_MAX from <limits.h>, used by i4toi2 and the int2vector path.
const SHRT_MIN: int32 = i16::MIN as int32;
const SHRT_MAX: int32 = i16::MAX as int32;

// --- shared error reporters (errcode value is ignored by the elog shim) ---
unsafe fn err_integer_out_of_range() {
    ereport!(ERROR, errmsg!("integer out of range"));
}
unsafe fn err_smallint_out_of_range() {
    ereport!(ERROR, errmsg!("smallint out of range"));
}
unsafe fn err_division_by_zero() {
    ereport!(ERROR, errmsg!("division by zero"));
}

/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/

/*
 *		int2in			- converts "num" to short
 */
pub unsafe fn int2in(fcinfo: FunctionCallInfo) -> Datum {
    let num: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);

    PG_RETURN_INT16!(pg_strtoint16_safe(num, (*fcinfo).context));
}

/*
 *		int2out			- converts short to "num"
 */
pub unsafe fn int2out(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let result: *mut c_char = palloc(7) as *mut c_char; /* sign, 5 digits, '\0' */

    pg_itoa(arg1, result);
    PG_RETURN_CSTRING!(result);
}

/*
 *		int2recv			- converts external binary format to int2
 */
pub unsafe fn int2recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    PG_RETURN_INT16!(pq_getmsgint(buf, core::mem::size_of::<int16>() as c_int) as int16);
}

/*
 *		int2send			- converts int2 to binary format
 */
pub unsafe fn int2send(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint16(&mut buf, arg1 as u16);
    return PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void); // PG_RETURN_BYTEA_P
}

/*
 * Confirm that a4 has the properties we expect of an int2vector.
 *
 * We need this because there are pathways by which a general int2[] array can
 * be cast to int2vector, allowing the type's restrictions to be violated.
 * All code that receives an int2vector as a SQL parameter should check this.
 *
 * # Safety
 * `int2_array` must point to a valid int2vector header.
 */
#[allow(dead_code)]
unsafe fn check_valid_int2vector(int2_array: *const int2vector) {
    /*
     * We insist on ndim == 1 and dataoffset == 0 (that is, no nulls) because
     * otherwise the array's layout will not be what calling code expects.  We
     * needn't be picky about the index lower bound though.  Checking elemtype
     * is just paranoia.
     */
    if (*int2_array).ndim != 1
        || (*int2_array).dataoffset != 0
        || (*int2_array).elemtype != INT2OID
    {
        ereport!(ERROR, errmsg!("array is not a valid int2vector"));
        /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
    }
}

/*
 * construct int2vector given a raw array of int2s
 *
 * TODO(pg-port): the int2vector array type + utils/array.h (SET_VARSIZE, the array
 * header fields) are not yet translated.
 *
 * # Safety
 * `int2s` must point to `n` readable int16s (or be null).
 */
pub unsafe fn buildint2vector(int2s: *const int16, n: c_int) -> *mut c_void {
    let _ = (int2s, n);
    unimplemented!("buildint2vector: int2vector type + utils/array.h not yet translated")
}

/*
 *		int2vectorin			- converts "num num ..." to internal form
 */
pub unsafe fn int2vectorin(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("int2vectorin: int2vector type + utils/array.h not yet translated")
}

/*
 *		int2vectorout		- converts internal form to "num num ..."
 */
pub unsafe fn int2vectorout(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("int2vectorout: int2vector type + utils/array.h not yet translated")
}

/*
 *		int2vectorrecv			- converts external binary format to int2vector
 */
pub unsafe fn int2vectorrecv(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("int2vectorrecv: array_recv + int2vector not yet translated")
}

/*
 *		int2vectorsend			- converts int2vector to binary format
 */
pub unsafe fn int2vectorsend(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("int2vectorsend: array_send not yet translated")
}

/*****************************************************************************
 *	 PUBLIC ROUTINES														 *
 *****************************************************************************/

/*
 *		int4in			- converts "num" to int4
 */
pub unsafe fn int4in(fcinfo: FunctionCallInfo) -> Datum {
    let num: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);

    PG_RETURN_INT32!(pg_strtoint32_safe(num, (*fcinfo).context));
}

/*
 *		int4out			- converts int4 to "num"
 */
pub unsafe fn int4out(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let result: *mut c_char = palloc(12) as *mut c_char; /* sign, 10 digits, '\0' */

    pg_ltoa(arg1, result);
    PG_RETURN_CSTRING!(result);
}

/*
 *		int4recv			- converts external binary format to int4
 */
pub unsafe fn int4recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    PG_RETURN_INT32!(pq_getmsgint(buf, core::mem::size_of::<int32>() as c_int) as int32);
}

/*
 *		int4send			- converts int4 to binary format
 */
pub unsafe fn int4send(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint32(&mut buf, arg1 as u32);
    return PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void); // PG_RETURN_BYTEA_P
}

/*
 *		===================
 *		CONVERSION ROUTINES
 *		===================
 */

pub unsafe fn i2toi4(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);

    PG_RETURN_INT32!(arg1 as int32);
}

pub unsafe fn i4toi2(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);

    if arg1 < SHRT_MIN || arg1 > SHRT_MAX {
        err_smallint_out_of_range();
    }

    PG_RETURN_INT16!(arg1 as int16);
}

/* Cast int4 -> bool */
pub unsafe fn int4_bool(fcinfo: FunctionCallInfo) -> Datum {
    if PG_GETARG_INT32!(fcinfo, 0) == 0 {
        PG_RETURN_BOOL!(false);
    } else {
        PG_RETURN_BOOL!(true);
    }
}

/* Cast bool -> int4 */
pub unsafe fn bool_int4(fcinfo: FunctionCallInfo) -> Datum {
    if PG_GETARG_BOOL!(fcinfo, 0) == false {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(1);
    }
}

/*
 *		============================
 *		COMPARISON OPERATOR ROUTINES
 *		============================
 */

pub unsafe fn int4eq(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) == PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int4ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) != PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int4lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) < PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int4le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) <= PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int4gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) > PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int4ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) >= PG_GETARG_INT32!(fcinfo, 1));
}

pub unsafe fn int2eq(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) == PG_GETARG_INT16!(fcinfo, 1));
}
pub unsafe fn int2ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) != PG_GETARG_INT16!(fcinfo, 1));
}
pub unsafe fn int2lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) < PG_GETARG_INT16!(fcinfo, 1));
}
pub unsafe fn int2le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) <= PG_GETARG_INT16!(fcinfo, 1));
}
pub unsafe fn int2gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) > PG_GETARG_INT16!(fcinfo, 1));
}
pub unsafe fn int2ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) >= PG_GETARG_INT16!(fcinfo, 1));
}

pub unsafe fn int24eq(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) as int32 == PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int24ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) as int32 != PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int24lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!((PG_GETARG_INT16!(fcinfo, 0) as int32) < PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int24le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) as int32 <= PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int24gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) as int32 > PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int24ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) as int32 >= PG_GETARG_INT32!(fcinfo, 1));
}

pub unsafe fn int42eq(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) == PG_GETARG_INT16!(fcinfo, 1) as int32);
}
pub unsafe fn int42ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) != PG_GETARG_INT16!(fcinfo, 1) as int32);
}
pub unsafe fn int42lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) < PG_GETARG_INT16!(fcinfo, 1) as int32);
}
pub unsafe fn int42le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) <= PG_GETARG_INT16!(fcinfo, 1) as int32);
}
pub unsafe fn int42gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) > PG_GETARG_INT16!(fcinfo, 1) as int32);
}
pub unsafe fn int42ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) >= PG_GETARG_INT16!(fcinfo, 1) as int32);
}

/*----------------------------------------------------------
 *	in_range functions for int4 and int2 (window framing).
 *---------------------------------------------------------*/

pub unsafe fn in_range_int4_int4(fcinfo: FunctionCallInfo) -> Datum {
    let val: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let base: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mut offset: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let sub: bool = PG_GETARG_BOOL!(fcinfo, 3);
    let less: bool = PG_GETARG_BOOL!(fcinfo, 4);
    let mut sum: int32 = 0;

    if offset < 0 {
        ereport!(
            ERROR,
            errmsg!("invalid preceding or following size in window function")
        );
    }
    if sub {
        offset = -offset; /* cannot overflow */
    }
    if pg_add_s32_overflow(base, offset, &mut sum) {
        PG_RETURN_BOOL!(if sub { !less } else { less });
    }
    if less {
        PG_RETURN_BOOL!(val <= sum);
    } else {
        PG_RETURN_BOOL!(val >= sum);
    }
}

pub unsafe fn in_range_int4_int2(fcinfo: FunctionCallInfo) -> Datum {
    /* Doesn't seem worth duplicating code for, so just invoke int4_int4 */
    DirectFunctionCall5Coll(
        in_range_int4_int4,
        InvalidOid,
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1),
        Int32GetDatum(PG_GETARG_INT16!(fcinfo, 2) as int32),
        PG_GETARG_DATUM!(fcinfo, 3),
        PG_GETARG_DATUM!(fcinfo, 4),
    )
}

pub unsafe fn in_range_int4_int8(fcinfo: FunctionCallInfo) -> Datum {
    /* We must do all the math in int64 */
    let val: int64 = PG_GETARG_INT32!(fcinfo, 0) as int64;
    let base: int64 = PG_GETARG_INT32!(fcinfo, 1) as int64;
    let mut offset: int64 = PG_GETARG_INT64!(fcinfo, 2);
    let sub: bool = PG_GETARG_BOOL!(fcinfo, 3);
    let less: bool = PG_GETARG_BOOL!(fcinfo, 4);
    let mut sum: int64 = 0;

    if offset < 0 {
        ereport!(
            ERROR,
            errmsg!("invalid preceding or following size in window function")
        );
    }
    if sub {
        offset = -offset; /* cannot overflow */
    }
    if pg_add_s64_overflow(base, offset, &mut sum) {
        PG_RETURN_BOOL!(if sub { !less } else { less });
    }
    if less {
        PG_RETURN_BOOL!(val <= sum);
    } else {
        PG_RETURN_BOOL!(val >= sum);
    }
}

pub unsafe fn in_range_int2_int4(fcinfo: FunctionCallInfo) -> Datum {
    /* We must do all the math in int32 */
    let val: int32 = PG_GETARG_INT16!(fcinfo, 0) as int32;
    let base: int32 = PG_GETARG_INT16!(fcinfo, 1) as int32;
    let mut offset: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let sub: bool = PG_GETARG_BOOL!(fcinfo, 3);
    let less: bool = PG_GETARG_BOOL!(fcinfo, 4);
    let mut sum: int32 = 0;

    if offset < 0 {
        ereport!(
            ERROR,
            errmsg!("invalid preceding or following size in window function")
        );
    }
    if sub {
        offset = -offset; /* cannot overflow */
    }
    if pg_add_s32_overflow(base, offset, &mut sum) {
        PG_RETURN_BOOL!(if sub { !less } else { less });
    }
    if less {
        PG_RETURN_BOOL!(val <= sum);
    } else {
        PG_RETURN_BOOL!(val >= sum);
    }
}

pub unsafe fn in_range_int2_int2(fcinfo: FunctionCallInfo) -> Datum {
    /* Doesn't seem worth duplicating code for, so just invoke int2_int4 */
    DirectFunctionCall5Coll(
        in_range_int2_int4,
        InvalidOid,
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1),
        Int32GetDatum(PG_GETARG_INT16!(fcinfo, 2) as int32),
        PG_GETARG_DATUM!(fcinfo, 3),
        PG_GETARG_DATUM!(fcinfo, 4),
    )
}

pub unsafe fn in_range_int2_int8(fcinfo: FunctionCallInfo) -> Datum {
    /* Doesn't seem worth duplicating code for, so just invoke int4_int8 */
    DirectFunctionCall5Coll(
        in_range_int4_int8,
        InvalidOid,
        Int32GetDatum(PG_GETARG_INT16!(fcinfo, 0) as int32),
        Int32GetDatum(PG_GETARG_INT16!(fcinfo, 1) as int32),
        PG_GETARG_DATUM!(fcinfo, 2),
        PG_GETARG_DATUM!(fcinfo, 3),
        PG_GETARG_DATUM!(fcinfo, 4),
    )
}

/*
 *		int[24]pl/mi/mul/div		- arithmetic
 */

pub unsafe fn int4um(fcinfo: FunctionCallInfo) -> Datum {
    let arg: int32 = PG_GETARG_INT32!(fcinfo, 0);

    if arg == PG_INT32_MIN {
        err_integer_out_of_range();
    }
    PG_RETURN_INT32!(arg.wrapping_neg());
}

pub unsafe fn int4up(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT32!(PG_GETARG_INT32!(fcinfo, 0));
}

pub unsafe fn int4pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mut result: int32 = 0;

    if pg_add_s32_overflow(arg1, arg2, &mut result) {
        err_integer_out_of_range();
    }
    PG_RETURN_INT32!(result);
}

pub unsafe fn int4mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mut result: int32 = 0;

    if pg_sub_s32_overflow(arg1, arg2, &mut result) {
        err_integer_out_of_range();
    }
    PG_RETURN_INT32!(result);
}

pub unsafe fn int4mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mut result: int32 = 0;

    if pg_mul_s32_overflow(arg1, arg2, &mut result) {
        err_integer_out_of_range();
    }
    PG_RETURN_INT32!(result);
}

pub unsafe fn int4div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let result: int32;

    if arg2 == 0 {
        err_division_by_zero();
        PG_RETURN_NULL!(fcinfo);
    }

    /* division by -1 is the same as negation (dodges INT_MIN/-1) */
    if arg2 == -1 {
        if arg1 == PG_INT32_MIN {
            err_integer_out_of_range();
        }
        result = arg1.wrapping_neg();
        PG_RETURN_INT32!(result);
    }

    result = arg1.wrapping_div(arg2);
    PG_RETURN_INT32!(result);
}

pub unsafe fn int4inc(fcinfo: FunctionCallInfo) -> Datum {
    let arg: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let mut result: int32 = 0;

    if pg_add_s32_overflow(arg, 1, &mut result) {
        err_integer_out_of_range();
    }
    PG_RETURN_INT32!(result);
}

pub unsafe fn int2um(fcinfo: FunctionCallInfo) -> Datum {
    let arg: int16 = PG_GETARG_INT16!(fcinfo, 0);

    if arg == PG_INT16_MIN {
        err_smallint_out_of_range();
    }
    PG_RETURN_INT16!(arg.wrapping_neg());
}

pub unsafe fn int2up(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT16!(PG_GETARG_INT16!(fcinfo, 0));
}

pub unsafe fn int2pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let mut result: int16 = 0;

    if pg_add_s16_overflow(arg1, arg2, &mut result) {
        err_smallint_out_of_range();
    }
    PG_RETURN_INT16!(result);
}

pub unsafe fn int2mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let mut result: int16 = 0;

    if pg_sub_s16_overflow(arg1, arg2, &mut result) {
        err_smallint_out_of_range();
    }
    PG_RETURN_INT16!(result);
}

pub unsafe fn int2mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let mut result: int16 = 0;

    if pg_mul_s16_overflow(arg1, arg2, &mut result) {
        err_smallint_out_of_range();
    }
    PG_RETURN_INT16!(result);
}

pub unsafe fn int2div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let result: int16;

    if arg2 == 0 {
        err_division_by_zero();
        PG_RETURN_NULL!(fcinfo);
    }

    if arg2 == -1 {
        if arg1 == PG_INT16_MIN {
            err_smallint_out_of_range();
        }
        result = arg1.wrapping_neg();
        PG_RETURN_INT16!(result);
    }

    result = arg1.wrapping_div(arg2);
    PG_RETURN_INT16!(result);
}

pub unsafe fn int24pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mut result: int32 = 0;

    if pg_add_s32_overflow(arg1 as int32, arg2, &mut result) {
        err_integer_out_of_range();
    }
    PG_RETURN_INT32!(result);
}

pub unsafe fn int24mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mut result: int32 = 0;

    if pg_sub_s32_overflow(arg1 as int32, arg2, &mut result) {
        err_integer_out_of_range();
    }
    PG_RETURN_INT32!(result);
}

pub unsafe fn int24mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mut result: int32 = 0;

    if pg_mul_s32_overflow(arg1 as int32, arg2, &mut result) {
        err_integer_out_of_range();
    }
    PG_RETURN_INT32!(result);
}

pub unsafe fn int24div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);

    if arg2 == 0 {
        err_division_by_zero();
        PG_RETURN_NULL!(fcinfo);
    }
    /* No overflow is possible */
    PG_RETURN_INT32!((arg1 as int32).wrapping_div(arg2));
}

pub unsafe fn int42pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let mut result: int32 = 0;

    if pg_add_s32_overflow(arg1, arg2 as int32, &mut result) {
        err_integer_out_of_range();
    }
    PG_RETURN_INT32!(result);
}

pub unsafe fn int42mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let mut result: int32 = 0;

    if pg_sub_s32_overflow(arg1, arg2 as int32, &mut result) {
        err_integer_out_of_range();
    }
    PG_RETURN_INT32!(result);
}

pub unsafe fn int42mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let mut result: int32 = 0;

    if pg_mul_s32_overflow(arg1, arg2 as int32, &mut result) {
        err_integer_out_of_range();
    }
    PG_RETURN_INT32!(result);
}

pub unsafe fn int42div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let result: int32;

    if arg2 == 0 {
        err_division_by_zero();
        PG_RETURN_NULL!(fcinfo);
    }

    if arg2 == -1 {
        if arg1 == PG_INT32_MIN {
            err_integer_out_of_range();
        }
        result = arg1.wrapping_neg();
        PG_RETURN_INT32!(result);
    }

    result = arg1.wrapping_div(arg2 as int32);
    PG_RETURN_INT32!(result);
}

pub unsafe fn int4mod(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);

    if arg2 == 0 {
        err_division_by_zero();
        PG_RETURN_NULL!(fcinfo);
    }
    /* INT_MIN % -1 is well-defined as zero */
    if arg2 == -1 {
        PG_RETURN_INT32!(0);
    }
    /* No overflow is possible */
    PG_RETURN_INT32!(arg1.wrapping_rem(arg2));
}

pub unsafe fn int2mod(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);

    if arg2 == 0 {
        err_division_by_zero();
        PG_RETURN_NULL!(fcinfo);
    }
    if arg2 == -1 {
        PG_RETURN_INT16!(0);
    }
    /* No overflow is possible */
    PG_RETURN_INT16!(arg1.wrapping_rem(arg2));
}

/* int[24]abs() - absolute value */
pub unsafe fn int4abs(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let result: int32;

    if arg1 == PG_INT32_MIN {
        err_integer_out_of_range();
    }
    result = if arg1 < 0 { arg1.wrapping_neg() } else { arg1 };
    PG_RETURN_INT32!(result);
}

pub unsafe fn int2abs(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let result: int16;

    if arg1 == PG_INT16_MIN {
        err_smallint_out_of_range();
    }
    result = if arg1 < 0 { arg1.wrapping_neg() } else { arg1 };
    PG_RETURN_INT16!(result);
}

/*
 * Greatest Common Divisor.  See the C source for the INT_MIN special-casing.
 */
unsafe fn int4gcd_internal(mut arg1: int32, mut arg2: int32) -> int32 {
    let swap: int32;
    let a1: int32;
    let a2: int32;

    /* Put the greater absolute value in arg1 (done in negative space). */
    a1 = if arg1 < 0 { arg1 } else { arg1.wrapping_neg() };
    a2 = if arg2 < 0 { arg2 } else { arg2.wrapping_neg() };
    if a1 > a2 {
        swap = arg1;
        arg1 = arg2;
        arg2 = swap;
    }

    /* Special care needs to be taken with INT_MIN. */
    if arg1 == PG_INT32_MIN {
        if arg2 == 0 || arg2 == PG_INT32_MIN {
            err_integer_out_of_range();
        }
        if arg2 == -1 {
            return 1;
        }
    }

    /* Euclidean algorithm */
    while arg2 != 0 {
        let swap2 = arg2;
        arg2 = arg1.wrapping_rem(arg2);
        arg1 = swap2;
    }

    /* Make sure the result is positive. */
    if arg1 < 0 {
        arg1 = arg1.wrapping_neg();
    }

    arg1
}

pub unsafe fn int4gcd(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);

    let result = int4gcd_internal(arg1, arg2);

    PG_RETURN_INT32!(result);
}

pub unsafe fn int4lcm(fcinfo: FunctionCallInfo) -> Datum {
    let mut arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let gcd: int32;
    let mut result: int32 = 0;

    /* lcm(x, 0) = lcm(0, x) = 0 */
    if arg1 == 0 || arg2 == 0 {
        PG_RETURN_INT32!(0);
    }

    /* lcm(x, y) = abs(x / gcd(x, y) * y) */
    gcd = int4gcd_internal(arg1, arg2);
    arg1 = arg1.wrapping_div(gcd);

    if pg_mul_s32_overflow(arg1, arg2, &mut result) {
        err_integer_out_of_range();
    }

    /* If the result is INT_MIN, it cannot be represented. */
    if result == PG_INT32_MIN {
        err_integer_out_of_range();
    }

    if result < 0 {
        result = result.wrapping_neg();
    }

    PG_RETURN_INT32!(result);
}

pub unsafe fn int2larger(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);

    PG_RETURN_INT16!(if arg1 > arg2 { arg1 } else { arg2 });
}

pub unsafe fn int2smaller(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);

    PG_RETURN_INT16!(if arg1 < arg2 { arg1 } else { arg2 });
}

pub unsafe fn int4larger(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);

    PG_RETURN_INT32!(if arg1 > arg2 { arg1 } else { arg2 });
}

pub unsafe fn int4smaller(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);

    PG_RETURN_INT32!(if arg1 < arg2 { arg1 } else { arg2 });
}

/*
 * Bit-pushing operators.  Shifts use wrapping shifts to mirror C's modulo-width
 * behavior and avoid Rust debug-mode shift-overflow panics.
 */

pub unsafe fn int4and(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT32!(PG_GETARG_INT32!(fcinfo, 0) & PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int4or(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT32!(PG_GETARG_INT32!(fcinfo, 0) | PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int4xor(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT32!(PG_GETARG_INT32!(fcinfo, 0) ^ PG_GETARG_INT32!(fcinfo, 1));
}
pub unsafe fn int4shl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    PG_RETURN_INT32!(arg1.wrapping_shl(arg2 as u32));
}
pub unsafe fn int4shr(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    PG_RETURN_INT32!(arg1.wrapping_shr(arg2 as u32));
}
pub unsafe fn int4not(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT32!(!PG_GETARG_INT32!(fcinfo, 0));
}

pub unsafe fn int2and(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT16!(PG_GETARG_INT16!(fcinfo, 0) & PG_GETARG_INT16!(fcinfo, 1));
}
pub unsafe fn int2or(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT16!(PG_GETARG_INT16!(fcinfo, 0) | PG_GETARG_INT16!(fcinfo, 1));
}
pub unsafe fn int2xor(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT16!(PG_GETARG_INT16!(fcinfo, 0) ^ PG_GETARG_INT16!(fcinfo, 1));
}
pub unsafe fn int2not(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT16!(!PG_GETARG_INT16!(fcinfo, 0));
}

pub unsafe fn int2shl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    // C: (int16)(arg1 << arg2), with arg1 promoted to int before the shift.
    PG_RETURN_INT16!((arg1 as int32).wrapping_shl(arg2 as u32) as int16);
}
pub unsafe fn int2shr(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    PG_RETURN_INT16!((arg1 as int32).wrapping_shr(arg2 as u32) as int16);
}

#[repr(C)]
struct generate_series_fctx {
    current: int32,
    finish: int32,
    step: int32,
}

/*
 * Set-returning-function support (funcapi.h).  funcapi.c is not yet ported, so
 * FuncCallContext and the SRF_* control-flow macros are declared locally with
 * TODO(pg-port) stubs, mirroring the int8.rs precedent.
 */
#[repr(C)]
struct FuncCallContext {
    call_cntr: u64,
    max_calls: u64,
    user_fctx: *mut c_void,
    attinmeta: *mut c_void,
    multi_call_memory_ctx: MemoryContext,
    tuple_desc: *mut c_void,
}

macro_rules! SRF_IS_FIRSTCALL {
    ($fcinfo:expr) => {
        srf_is_firstcall($fcinfo)
    };
}
macro_rules! SRF_FIRSTCALL_INIT {
    ($fcinfo:expr) => {
        srf_firstcall_init($fcinfo)
    };
}
macro_rules! SRF_PERCALL_SETUP {
    ($fcinfo:expr) => {
        srf_percall_setup($fcinfo)
    };
}
macro_rules! SRF_RETURN_NEXT {
    ($fcinfo:expr, $result:expr) => {
        return srf_return_next($fcinfo, $result)
    };
}
macro_rules! SRF_RETURN_DONE {
    ($fcinfo:expr) => {
        return srf_return_done($fcinfo)
    };
}

unsafe fn srf_is_firstcall(_fcinfo: FunctionCallInfo) -> bool {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_firstcall_init(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_percall_setup(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_return_next(_fcinfo: FunctionCallInfo, _result: Datum) -> Datum {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_return_done(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}

/* is_funcclause - nodes/nodeFuncs.h.  Local copy (not exported), per sibling precedent. */
unsafe fn is_funcclause(clause: *const Node) -> bool {
    !clause.is_null() && IsA!(clause, T_FuncExpr)
}

/*
 * non-persistent numeric series generator
 */
pub unsafe fn generate_series_int4(fcinfo: FunctionCallInfo) -> Datum {
    generate_series_step_int4(fcinfo)
}

pub unsafe fn generate_series_step_int4(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let fctx: *mut generate_series_fctx;
    let result: int32;
    let oldcontext: MemoryContext;

    /* stuff done only on the first call of the function */
    if SRF_IS_FIRSTCALL!(fcinfo) {
        let start: int32 = PG_GETARG_INT32!(fcinfo, 0);
        let finish: int32 = PG_GETARG_INT32!(fcinfo, 1);
        let mut step: int32 = 1;

        /* see if we were given an explicit step size */
        if PG_NARGS!(fcinfo) == 3 {
            step = PG_GETARG_INT32!(fcinfo, 2);
        }
        if step == 0 {
            ereport!(ERROR, errmsg!("step size cannot equal zero"));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT!(fcinfo);

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        /* allocate memory for user context */
        let fctx_new: *mut generate_series_fctx =
            palloc(core::mem::size_of::<generate_series_fctx>()) as *mut generate_series_fctx;

        /*
         * Use fctx to keep state from call to call. Seed current with the
         * original start value
         */
        (*fctx_new).current = start;
        (*fctx_new).finish = finish;
        (*fctx_new).step = step;

        (*funcctx).user_fctx = fctx_new as *mut c_void;
        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP!(fcinfo);

    /*
     * get the saved state and use current as the result for this iteration
     */
    fctx = (*funcctx).user_fctx as *mut generate_series_fctx;
    result = (*fctx).current;

    if ((*fctx).step > 0 && (*fctx).current <= (*fctx).finish)
        || ((*fctx).step < 0 && (*fctx).current >= (*fctx).finish)
    {
        /*
         * Increment current in preparation for next iteration. If next-value
         * computation overflows, this is the final result.
         */
        if pg_add_s32_overflow((*fctx).current, (*fctx).step, &mut (*fctx).current) {
            (*fctx).step = 0;
        }

        /* do when there is more left to send */
        SRF_RETURN_NEXT!(fcinfo, Int32GetDatum(result));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE!(fcinfo);
    }
}

/*
 * Planner support function for generate_series(int4, int4 [, int4])
 */
pub unsafe fn generate_series_int4_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let mut ret: *mut Node = null_mut();

    if IsA!(rawreq, T_SupportRequestRows) {
        /* Try to estimate the number of rows returned */
        let req: *mut SupportRequestRows = rawreq as *mut SupportRequestRows;

        if is_funcclause((*req).node) {
            /* be paranoid */
            let args: *mut List = (*((*req).node as *mut FuncExpr)).args;
            let arg1: *mut Node;
            let arg2: *mut Node;
            let arg3: *mut Node;

            /* We can use estimated argument values here */
            arg1 = estimate_expression_value((*req).root, linitial(args) as *mut Node);
            arg2 = estimate_expression_value((*req).root, lsecond(args) as *mut Node);
            if list_length(args) >= 3 {
                arg3 = estimate_expression_value((*req).root, lthird(args) as *mut Node);
            } else {
                arg3 = null_mut();
            }

            /*
             * If any argument is constant NULL, we can safely assume that
             * zero rows are returned.  Otherwise, if they're all non-NULL
             * constants, we can calculate the number of rows that will be
             * returned.  Use double arithmetic to avoid overflow hazards.
             */
            if (IsA!(arg1, T_Const) && (*(arg1 as *mut Const)).constisnull)
                || (IsA!(arg2, T_Const) && (*(arg2 as *mut Const)).constisnull)
                || (!arg3.is_null()
                    && IsA!(arg3, T_Const)
                    && (*(arg3 as *mut Const)).constisnull)
            {
                (*req).rows = 0.0;
                ret = req as *mut Node;
            } else if IsA!(arg1, T_Const)
                && IsA!(arg2, T_Const)
                && (arg3.is_null() || IsA!(arg3, T_Const))
            {
                let start: f64;
                let finish: f64;
                let step: f64;

                start = DatumGetInt32((*(arg1 as *mut Const)).constvalue) as f64;
                finish = DatumGetInt32((*(arg2 as *mut Const)).constvalue) as f64;
                step = if !arg3.is_null() {
                    DatumGetInt32((*(arg3 as *mut Const)).constvalue) as f64
                } else {
                    1.0
                };

                /* This equation works for either sign of step */
                if step != 0.0 {
                    (*req).rows = ((finish - start + step) / step).floor();
                    ret = req as *mut Node;
                }
            }
        }
    }

    PG_RETURN_POINTER!(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{BoolGetDatum, CStringGetDatum, DatumGetBool, DatumGetCString, DatumGetInt16, DatumGetInt32};
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};

    unsafe fn cstr_eq(p: *mut c_char, want: &str) -> bool {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn int4_io_and_arithmetic() {
        unsafe {
            // int4in / int4out round trip
            let d = DirectFunctionCall1Coll(int4in, InvalidOid, CStringGetDatum(c"-12345".as_ptr()));
            assert_eq!(DatumGetInt32(d), -12345);
            let s = DatumGetCString(DirectFunctionCall1Coll(int4out, InvalidOid, Int32GetDatum(67890)));
            assert!(cstr_eq(s, "67890"));
            // int2in clamps via pg_strtoint16; int2out
            let d = DirectFunctionCall1Coll(int2in, InvalidOid, CStringGetDatum(c"32767".as_ptr()));
            assert_eq!(DatumGetInt16(d), 32767);

            // arithmetic with the overflow helpers
            let pl = |a, b| DatumGetInt32(DirectFunctionCall2Coll(int4pl, InvalidOid, Int32GetDatum(a), Int32GetDatum(b)));
            assert_eq!(pl(2, 3), 5);
            let mul = |a, b| DatumGetInt32(DirectFunctionCall2Coll(int4mul, InvalidOid, Int32GetDatum(a), Int32GetDatum(b)));
            assert_eq!(mul(-7, 6), -42);
            let div = |a, b| DatumGetInt32(DirectFunctionCall2Coll(int4div, InvalidOid, Int32GetDatum(a), Int32GetDatum(b)));
            assert_eq!(div(20, -4), -5);
            assert_eq!(div(7, -1), -7); // negation path
            let modf = |a, b| DatumGetInt32(DirectFunctionCall2Coll(int4mod, InvalidOid, Int32GetDatum(a), Int32GetDatum(b)));
            assert_eq!(modf(17, 5), 2);
            assert_eq!(modf(i32::MIN, -1), 0); // well-defined zero

            // gcd / lcm
            let gcd = |a, b| DatumGetInt32(DirectFunctionCall2Coll(int4gcd, InvalidOid, Int32GetDatum(a), Int32GetDatum(b)));
            assert_eq!(gcd(54, 24), 6);
            assert_eq!(gcd(0, 0), 0);
            let lcm = |a, b| DatumGetInt32(DirectFunctionCall2Coll(int4lcm, InvalidOid, Int32GetDatum(a), Int32GetDatum(b)));
            assert_eq!(lcm(4, 6), 12);

            // comparisons + bit ops + shifts
            assert!(DatumGetBool(DirectFunctionCall2Coll(int4lt, InvalidOid, Int32GetDatum(1), Int32GetDatum(2))));
            assert_eq!(DatumGetInt32(DirectFunctionCall2Coll(int4and, InvalidOid, Int32GetDatum(0b1100), Int32GetDatum(0b1010))), 0b1000);
            assert_eq!(DatumGetInt32(DirectFunctionCall2Coll(int4shl, InvalidOid, Int32GetDatum(1), Int32GetDatum(4))), 16);
            assert_eq!(DatumGetInt32(DirectFunctionCall2Coll(int4shr, InvalidOid, Int32GetDatum(-16), Int32GetDatum(2))), -4);

            // bool <-> int4 casts
            assert!(DatumGetBool(DirectFunctionCall1Coll(int4_bool, InvalidOid, Int32GetDatum(5))));
            assert_eq!(DatumGetInt32(DirectFunctionCall1Coll(bool_int4, InvalidOid, BoolGetDatum(true))), 1);
        }
    }

    #[test]
    #[should_panic]
    fn int4div_min_by_neg1_overflows() {
        unsafe {
            // INT_MIN / -1 cannot be represented -> hard ERROR (panic under elog shim)
            DirectFunctionCall2Coll(int4div, InvalidOid, Int32GetDatum(i32::MIN), Int32GetDatum(-1));
        }
    }

    // End-to-end binary wire path: int4send -> bytea -> int4recv round trip.
    #[test]
    fn int4_send_recv_roundtrip() {
        unsafe {
            use crate::postgres::DatumGetPointer;
            use crate::varatt::{VARDATA, VARSIZE};
            let v: int32 = -123_456;

            // send: produces a bytea = [4-byte varlena hdr][4 network-order payload bytes]
            let b = DirectFunctionCall1Coll(int4send, InvalidOid, Int32GetDatum(v));
            let bp = DatumGetPointer(b) as *const c_char;
            assert_eq!(VARSIZE(bp) as usize, VARHDRSZ as usize + 4);

            // recv: point a StringInfo at the 4 payload bytes and read it back
            let mut si: StringInfoData = core::mem::zeroed();
            si.data = VARDATA(bp);
            si.len = 4;
            si.maxlen = 4;
            si.cursor = 0;
            let back = DatumGetInt32(DirectFunctionCall1Coll(
                int4recv,
                InvalidOid,
                PointerGetDatum(&si as *const StringInfoData as *const c_void),
            ));
            assert_eq!(back, v);
        }
    }
}
