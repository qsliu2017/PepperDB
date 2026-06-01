//! Translation of postgres/src/backend/utils/adt/int8.c
//!
//! Internal 64-bit integer operations (the SQL `bigint` type).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped: common/int.h -> crate::common::int (pg_{add,sub,mul}_s64_overflow),
//! utils/builtins.h pg_strtoint64/pg_lltoa -> crate::utils::adt::numutils.  <math.h> rint/rintf
//! bound via extern "C".  int8 is pass-by-value here (USE_FLOAT8_BYVAL), so the int8inc/int8dec
//! pass-by-reference aggregate fast-paths (`#ifndef USE_FLOAT8_BYVAL`) are NOT compiled.
//!
//! STUBBED: int8recv/int8send (libpq/pqformat); int8inc_support + generate_series_int8[_step|
//! _support] (SRF funcapi + planner support nodes).

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{
    PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_FLOAT4, PG_GETARG_FLOAT8, PG_GETARG_INT16,
    PG_GETARG_INT32, PG_GETARG_INT64, PG_GETARG_OID, PG_GETARG_POINTER, PG_RETURN_BOOL,
    PG_RETURN_CSTRING, PG_RETURN_FLOAT4, PG_RETURN_FLOAT8, PG_RETURN_INT16, PG_RETURN_INT32,
    PG_RETURN_INT64, PG_RETURN_NULL, PG_RETURN_OID,
};
use crate::c::{
    float4, float8, int16, int32, int64, PG_INT16_MAX, PG_INT16_MIN, PG_INT32_MAX, PG_INT32_MIN,
    PG_INT64_MIN, PG_UINT32_MAX,
};
use crate::common::int::{pg_add_s64_overflow, pg_mul_s64_overflow, pg_sub_s64_overflow};
use crate::utils::adt::numutils::{pg_lltoa, pg_strtoint64_safe};
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::libpq::pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgint64, pq_sendint64};
use crate::postgres::PointerGetDatum;
use core::ffi::{c_char, c_void};

const MAXINT8LEN: usize = 20;

// <math.h>: rint rounds to nearest integer (ties to even), passing NaN/Inf through.
extern "C" {
    fn rint(x: f64) -> f64;
    fn rintf(x: f32) -> f32;
}

unsafe fn err_bigint_out_of_range() {
    ereport!(ERROR, errmsg!("bigint out of range"));
}
unsafe fn err_division_by_zero() {
    ereport!(ERROR, errmsg!("division by zero"));
}

/*----------------------------------------------------------
 * Formatting and conversion routines.
 *---------------------------------------------------------*/

/* int8in() */
pub unsafe fn int8in(fcinfo: FunctionCallInfo) -> Datum {
    let num: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);

    PG_RETURN_INT64!(pg_strtoint64_safe(num, (*fcinfo).context));
}

/* int8out() */
pub unsafe fn int8out(fcinfo: FunctionCallInfo) -> Datum {
    let val: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let mut buf = [0i8; MAXINT8LEN + 1];
    let result: *mut c_char;
    let len: usize;

    len = (pg_lltoa(val, buf.as_mut_ptr()) + 1) as usize;

    /* length is known, so palloc + memcpy to avoid pstrdup's strlen */
    result = palloc(len) as *mut c_char;
    core::ptr::copy_nonoverlapping(buf.as_ptr(), result, len);
    PG_RETURN_CSTRING!(result);
}

/* int8recv() */
pub unsafe fn int8recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    PG_RETURN_INT64!(pq_getmsgint64(buf));
}

/* int8send() */
pub unsafe fn int8send(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint64(&mut buf, arg1 as u64);
    return PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void); // PG_RETURN_BYTEA_P
}

/*----------------------------------------------------------
 *	Relational operators for int8s, including cross-data-type comparisons.
 *---------------------------------------------------------*/

pub unsafe fn int8eq(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) == PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int8ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) != PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int8lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) < PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int8gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) > PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int8le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) <= PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int8ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) >= PG_GETARG_INT64!(fcinfo, 1));
}

pub unsafe fn int84eq(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) == PG_GETARG_INT32!(fcinfo, 1) as int64);
}
pub unsafe fn int84ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) != PG_GETARG_INT32!(fcinfo, 1) as int64);
}
pub unsafe fn int84lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) < PG_GETARG_INT32!(fcinfo, 1) as int64);
}
pub unsafe fn int84gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) > PG_GETARG_INT32!(fcinfo, 1) as int64);
}
pub unsafe fn int84le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) <= PG_GETARG_INT32!(fcinfo, 1) as int64);
}
pub unsafe fn int84ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) >= PG_GETARG_INT32!(fcinfo, 1) as int64);
}

pub unsafe fn int48eq(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) as int64 == PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int48ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) as int64 != PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int48lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!((PG_GETARG_INT32!(fcinfo, 0) as int64) < PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int48gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) as int64 > PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int48le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) as int64 <= PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int48ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT32!(fcinfo, 0) as int64 >= PG_GETARG_INT64!(fcinfo, 1));
}

pub unsafe fn int82eq(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) == PG_GETARG_INT16!(fcinfo, 1) as int64);
}
pub unsafe fn int82ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) != PG_GETARG_INT16!(fcinfo, 1) as int64);
}
pub unsafe fn int82lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) < PG_GETARG_INT16!(fcinfo, 1) as int64);
}
pub unsafe fn int82gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) > PG_GETARG_INT16!(fcinfo, 1) as int64);
}
pub unsafe fn int82le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) <= PG_GETARG_INT16!(fcinfo, 1) as int64);
}
pub unsafe fn int82ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT64!(fcinfo, 0) >= PG_GETARG_INT16!(fcinfo, 1) as int64);
}

pub unsafe fn int28eq(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) as int64 == PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int28ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) as int64 != PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int28lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!((PG_GETARG_INT16!(fcinfo, 0) as int64) < PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int28gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) as int64 > PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int28le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) as int64 <= PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int28ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(PG_GETARG_INT16!(fcinfo, 0) as int64 >= PG_GETARG_INT64!(fcinfo, 1));
}

/*
 * in_range support function for int8.
 */
pub unsafe fn in_range_int8_int8(fcinfo: FunctionCallInfo) -> Datum {
    let val: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let base: int64 = PG_GETARG_INT64!(fcinfo, 1);
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

/*----------------------------------------------------------
 *	Arithmetic operators on 64-bit integers.
 *---------------------------------------------------------*/

pub unsafe fn int8um(fcinfo: FunctionCallInfo) -> Datum {
    let arg: int64 = PG_GETARG_INT64!(fcinfo, 0);

    if arg == PG_INT64_MIN {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(arg.wrapping_neg());
}

pub unsafe fn int8up(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64!(PG_GETARG_INT64!(fcinfo, 0));
}

pub unsafe fn int8pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_add_s64_overflow(arg1, arg2, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int8mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_sub_s64_overflow(arg1, arg2, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int8mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_mul_s64_overflow(arg1, arg2, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int8div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let result: int64;

    if arg2 == 0 {
        err_division_by_zero();
        PG_RETURN_NULL!(fcinfo);
    }

    /* division by -1 is the same as negation (dodges INT64_MIN/-1) */
    if arg2 == -1 {
        if arg1 == PG_INT64_MIN {
            err_bigint_out_of_range();
        }
        result = arg1.wrapping_neg();
        PG_RETURN_INT64!(result);
    }

    result = arg1.wrapping_div(arg2);
    PG_RETURN_INT64!(result);
}

/* int8abs() */
pub unsafe fn int8abs(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let result: int64;

    if arg1 == PG_INT64_MIN {
        err_bigint_out_of_range();
    }
    result = if arg1 < 0 { arg1.wrapping_neg() } else { arg1 };
    PG_RETURN_INT64!(result);
}

/* int8mod() */
pub unsafe fn int8mod(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);

    if arg2 == 0 {
        err_division_by_zero();
        PG_RETURN_NULL!(fcinfo);
    }
    /* INT64_MIN % -1 is well-defined as zero */
    if arg2 == -1 {
        PG_RETURN_INT64!(0);
    }
    /* No overflow is possible */
    PG_RETURN_INT64!(arg1.wrapping_rem(arg2));
}

/*
 * Greatest Common Divisor.  See the C source for the INT64_MIN special-casing.
 */
unsafe fn int8gcd_internal(mut arg1: int64, mut arg2: int64) -> int64 {
    let swap: int64;
    let a1: int64;
    let a2: int64;

    a1 = if arg1 < 0 { arg1 } else { arg1.wrapping_neg() };
    a2 = if arg2 < 0 { arg2 } else { arg2.wrapping_neg() };
    if a1 > a2 {
        swap = arg1;
        arg1 = arg2;
        arg2 = swap;
    }

    if arg1 == PG_INT64_MIN {
        if arg2 == 0 || arg2 == PG_INT64_MIN {
            err_bigint_out_of_range();
        }
        if arg2 == -1 {
            return 1;
        }
    }

    while arg2 != 0 {
        let swap2 = arg2;
        arg2 = arg1.wrapping_rem(arg2);
        arg1 = swap2;
    }

    if arg1 < 0 {
        arg1 = arg1.wrapping_neg();
    }

    arg1
}

pub unsafe fn int8gcd(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);

    let result = int8gcd_internal(arg1, arg2);

    PG_RETURN_INT64!(result);
}

pub unsafe fn int8lcm(fcinfo: FunctionCallInfo) -> Datum {
    let mut arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let gcd: int64;
    let mut result: int64 = 0;

    if arg1 == 0 || arg2 == 0 {
        PG_RETURN_INT64!(0);
    }

    gcd = int8gcd_internal(arg1, arg2);
    arg1 = arg1.wrapping_div(gcd);

    if pg_mul_s64_overflow(arg1, arg2, &mut result) {
        err_bigint_out_of_range();
    }

    if result == PG_INT64_MIN {
        err_bigint_out_of_range();
    }

    if result < 0 {
        result = result.wrapping_neg();
    }

    PG_RETURN_INT64!(result);
}

pub unsafe fn int8inc(fcinfo: FunctionCallInfo) -> Datum {
    // int8 is pass-by-value (USE_FLOAT8_BYVAL), so only the "dumb way" branch applies.
    let arg: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let mut result: int64 = 0;

    if pg_add_s64_overflow(arg, 1, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int8dec(fcinfo: FunctionCallInfo) -> Datum {
    let arg: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let mut result: int64 = 0;

    if pg_sub_s64_overflow(arg, 1, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

/*
 * Exactly like int8inc/int8dec but used for aggregates that count only
 * non-null values (declared strict, so null checks happen earlier).
 */
pub unsafe fn int8inc_any(fcinfo: FunctionCallInfo) -> Datum {
    int8inc(fcinfo)
}
pub unsafe fn int8inc_float8_float8(fcinfo: FunctionCallInfo) -> Datum {
    int8inc(fcinfo)
}
pub unsafe fn int8dec_any(fcinfo: FunctionCallInfo) -> Datum {
    int8dec(fcinfo)
}

/* int8inc_support - prosupport function for int8inc()/int8inc_any() */
pub unsafe fn int8inc_support(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): planner support nodes (SupportRequestWFuncMonotonic, MonotonicFunction)
    // from nodes/supportnodes.h not yet translated.
    let _ = fcinfo;
    unimplemented!("int8inc_support: planner support nodes not yet translated")
}

pub unsafe fn int8larger(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);

    PG_RETURN_INT64!(if arg1 > arg2 { arg1 } else { arg2 });
}

pub unsafe fn int8smaller(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);

    PG_RETURN_INT64!(if arg1 < arg2 { arg1 } else { arg2 });
}

pub unsafe fn int84pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_add_s64_overflow(arg1, arg2 as int64, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int84mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_sub_s64_overflow(arg1, arg2 as int64, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int84mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_mul_s64_overflow(arg1, arg2 as int64, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int84div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let result: int64;

    if arg2 == 0 {
        err_division_by_zero();
        PG_RETURN_NULL!(fcinfo);
    }

    if arg2 == -1 {
        if arg1 == PG_INT64_MIN {
            err_bigint_out_of_range();
        }
        result = arg1.wrapping_neg();
        PG_RETURN_INT64!(result);
    }

    result = arg1.wrapping_div(arg2 as int64);
    PG_RETURN_INT64!(result);
}

pub unsafe fn int48pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_add_s64_overflow(arg1 as int64, arg2, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int48mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_sub_s64_overflow(arg1 as int64, arg2, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int48mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_mul_s64_overflow(arg1 as int64, arg2, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int48div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);

    if arg2 == 0 {
        err_division_by_zero();
        PG_RETURN_NULL!(fcinfo);
    }
    /* No overflow is possible */
    PG_RETURN_INT64!((arg1 as int64).wrapping_div(arg2));
}

pub unsafe fn int82pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_add_s64_overflow(arg1, arg2 as int64, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int82mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_sub_s64_overflow(arg1, arg2 as int64, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int82mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_mul_s64_overflow(arg1, arg2 as int64, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int82div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let result: int64;

    if arg2 == 0 {
        err_division_by_zero();
        PG_RETURN_NULL!(fcinfo);
    }

    if arg2 == -1 {
        if arg1 == PG_INT64_MIN {
            err_bigint_out_of_range();
        }
        result = arg1.wrapping_neg();
        PG_RETURN_INT64!(result);
    }

    result = arg1.wrapping_div(arg2 as int64);
    PG_RETURN_INT64!(result);
}

pub unsafe fn int28pl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_add_s64_overflow(arg1 as int64, arg2, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int28mi(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_sub_s64_overflow(arg1 as int64, arg2, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int28mul(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let mut result: int64 = 0;

    if pg_mul_s64_overflow(arg1 as int64, arg2, &mut result) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(result);
}

pub unsafe fn int28div(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let arg2: int64 = PG_GETARG_INT64!(fcinfo, 1);

    if arg2 == 0 {
        err_division_by_zero();
        PG_RETURN_NULL!(fcinfo);
    }
    /* No overflow is possible */
    PG_RETURN_INT64!((arg1 as int64).wrapping_div(arg2));
}

/* Bit-pushing operators. */
pub unsafe fn int8and(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64!(PG_GETARG_INT64!(fcinfo, 0) & PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int8or(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64!(PG_GETARG_INT64!(fcinfo, 0) | PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int8xor(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64!(PG_GETARG_INT64!(fcinfo, 0) ^ PG_GETARG_INT64!(fcinfo, 1));
}
pub unsafe fn int8not(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64!(!PG_GETARG_INT64!(fcinfo, 0));
}
pub unsafe fn int8shl(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    PG_RETURN_INT64!(arg1.wrapping_shl(arg2 as u32));
}
pub unsafe fn int8shr(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let arg2: int32 = PG_GETARG_INT32!(fcinfo, 1);
    PG_RETURN_INT64!(arg1.wrapping_shr(arg2 as u32));
}

/*----------------------------------------------------------
 *	Conversion operators.
 *---------------------------------------------------------*/

pub unsafe fn int48(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64!(PG_GETARG_INT32!(fcinfo, 0) as int64);
}

pub unsafe fn int84(fcinfo: FunctionCallInfo) -> Datum {
    let arg: int64 = PG_GETARG_INT64!(fcinfo, 0);

    if arg < PG_INT32_MIN as int64 || arg > PG_INT32_MAX as int64 {
        ereport!(ERROR, errmsg!("integer out of range"));
    }
    PG_RETURN_INT32!(arg as int32);
}

pub unsafe fn int28(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT64!(PG_GETARG_INT16!(fcinfo, 0) as int64);
}

pub unsafe fn int82(fcinfo: FunctionCallInfo) -> Datum {
    let arg: int64 = PG_GETARG_INT64!(fcinfo, 0);

    if arg < PG_INT16_MIN as int64 || arg > PG_INT16_MAX as int64 {
        ereport!(ERROR, errmsg!("smallint out of range"));
    }
    PG_RETURN_INT16!(arg as int16);
}

pub unsafe fn i8tod(fcinfo: FunctionCallInfo) -> Datum {
    let arg: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let result: float8 = arg as float8;

    PG_RETURN_FLOAT8!(result);
}

/* dtoi8() - convert float8 to 8-byte integer */
pub unsafe fn dtoi8(fcinfo: FunctionCallInfo) -> Datum {
    let mut num: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    /* Get rid of any fractional part (rint passes NaN/Inf through). */
    num = rint(num);

    /* Range check: FLOAT8_FITS_IN_INT64 = num in [-2^63, 2^63). */
    if num.is_nan() || !(num >= PG_INT64_MIN as float8 && num < -(PG_INT64_MIN as float8)) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(num as int64);
}

pub unsafe fn i8tof(fcinfo: FunctionCallInfo) -> Datum {
    let arg: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let result: float4 = arg as float4;

    PG_RETURN_FLOAT4!(result);
}

/* ftoi8() - convert float4 to 8-byte integer */
pub unsafe fn ftoi8(fcinfo: FunctionCallInfo) -> Datum {
    let mut num: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);

    num = rintf(num);

    /* Range check: FLOAT4_FITS_IN_INT64 = num in [-2^63, 2^63) in float space. */
    if num.is_nan() || !(num >= PG_INT64_MIN as float4 && num < -(PG_INT64_MIN as float4)) {
        err_bigint_out_of_range();
    }
    PG_RETURN_INT64!(num as int64);
}

pub unsafe fn i8tooid(fcinfo: FunctionCallInfo) -> Datum {
    let arg: int64 = PG_GETARG_INT64!(fcinfo, 0);

    if arg < 0 || arg > PG_UINT32_MAX as int64 {
        ereport!(ERROR, errmsg!("OID out of range"));
    }
    PG_RETURN_OID!(arg as Oid);
}

pub unsafe fn oidtoi8(fcinfo: FunctionCallInfo) -> Datum {
    let arg: Oid = PG_GETARG_OID!(fcinfo, 0);

    PG_RETURN_INT64!(arg as int64);
}

/*
 * non-persistent numeric series generator
 */
pub unsafe fn generate_series_int8(fcinfo: FunctionCallInfo) -> Datum {
    generate_series_step_int8(fcinfo)
}

pub unsafe fn generate_series_step_int8(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): SRF infrastructure (funcapi.h) not yet translated.
    let _ = fcinfo;
    unimplemented!("generate_series_step_int8: SRF infrastructure (funcapi.h) not yet translated")
}

pub unsafe fn generate_series_int8_support(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): planner support nodes + optimizer not yet translated.
    let _ = fcinfo;
    unimplemented!("generate_series_int8_support: planner support nodes not yet translated")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{
        CStringGetDatum, DatumGetBool, DatumGetCString, DatumGetFloat8, DatumGetInt32,
        DatumGetInt64, Float8GetDatum, Int32GetDatum, Int64GetDatum,
    };
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};

    unsafe fn cstr_eq(p: *mut c_char, want: &str) -> bool {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn int8_io_arithmetic_conversions() {
        unsafe {
            // in / out
            let d = DirectFunctionCall1Coll(int8in, InvalidOid, CStringGetDatum(c"-9223372036854775808".as_ptr()));
            assert_eq!(DatumGetInt64(d), i64::MIN);
            let s = DatumGetCString(DirectFunctionCall1Coll(int8out, InvalidOid, Int64GetDatum(9223372036854775807)));
            assert!(cstr_eq(s, "9223372036854775807"));

            // arithmetic
            let pl = |a, b| DatumGetInt64(DirectFunctionCall2Coll(int8pl, InvalidOid, Int64GetDatum(a), Int64GetDatum(b)));
            assert_eq!(pl(5_000_000_000, 1), 5_000_000_001);
            let mul = |a, b| DatumGetInt64(DirectFunctionCall2Coll(int8mul, InvalidOid, Int64GetDatum(a), Int64GetDatum(b)));
            assert_eq!(mul(3_000_000_000, -2), -6_000_000_000);
            let modf = |a, b| DatumGetInt64(DirectFunctionCall2Coll(int8mod, InvalidOid, Int64GetDatum(a), Int64GetDatum(b)));
            assert_eq!(modf(i64::MIN, -1), 0);
            let gcd = |a, b| DatumGetInt64(DirectFunctionCall2Coll(int8gcd, InvalidOid, Int64GetDatum(a), Int64GetDatum(b)));
            assert_eq!(gcd(54_000_000_000, 24_000_000_000), 6_000_000_000);

            // comparisons + bit/shift
            assert!(DatumGetBool(DirectFunctionCall2Coll(int8lt, InvalidOid, Int64GetDatum(1), Int64GetDatum(2))));
            assert_eq!(DatumGetInt64(DirectFunctionCall2Coll(int8shl, InvalidOid, Int64GetDatum(1), Int32GetDatum(40))), 1i64 << 40);

            // conversions: i8tod, dtoi8 (rounds), int84 range
            let dval = DatumGetFloat8(DirectFunctionCall1Coll(i8tod, InvalidOid, Int64GetDatum(123)));
            assert_eq!(dval, 123.0);
            let back = DatumGetInt64(DirectFunctionCall1Coll(dtoi8, InvalidOid, Float8GetDatum(2.5)));
            assert_eq!(back, 2); // rint(2.5) = 2 (ties to even)
            let i4 = DatumGetInt32(DirectFunctionCall1Coll(int84, InvalidOid, Int64GetDatum(100000)));
            assert_eq!(i4, 100000);
        }
    }

    #[test]
    #[should_panic]
    fn int84_out_of_range() {
        unsafe {
            DirectFunctionCall1Coll(int84, InvalidOid, Int64GetDatum(5_000_000_000));
        }
    }
}
