//! numeric.rs
//!   An exact numeric data type for the Postgres database system
//! Translated 1:1 from postgres/src/backend/utils/adt/numeric.c
//!
//! Original coding 1998, Jan Wieck.  Heavily revised 2003, Tom Lane.
//!
//! Many of the algorithmic ideas are borrowed from David M. Smith's "FM"
//! multiple-precision math library, most recently published as Algorithm
//! 786: Multiple-Precision Complex Arithmetic and Functions, ACM
//! Transactions on Mathematical Software, Vol. 24, No. 4, December 1998,
//! pages 359-367.
//!
//! Portions Copyright (c) 1998-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! numeric.rs is the canonical home of Numeric/NumericData/NumericVar/NumericDigit
//! and all the NUMERIC_* macros declared in utils/numeric.h plus the numeric.c
//! internal types.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_parens)]
#![allow(unused_assignments)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{
    PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_FLOAT4, PG_GETARG_FLOAT8, PG_GETARG_INT16,
    PG_GETARG_INT32, PG_GETARG_INT64, PG_GETARG_OID, PG_GETARG_POINTER, PG_RETURN_BOOL,
    PG_RETURN_CSTRING, PG_RETURN_DATUM, PG_RETURN_FLOAT4, PG_RETURN_FLOAT8, PG_RETURN_INT16,
    PG_RETURN_INT32, PG_RETURN_INT64, PG_RETURN_NULL, PG_RETURN_POINTER, PG_RETURN_UINT32,
    PG_RETURN_UINT64, PG_RETURN_VOID, PG_ARGISNULL,
};
use crate::c::{
    float4, float8, int128, int16, int32, int64, uint128, uint16, uint32, uint64, Size,
    PG_INT16_MAX, PG_INT16_MIN, PG_INT32_MAX, PG_INT32_MIN, PG_INT64_MAX, PG_INT64_MIN,
    PG_UINT64_MAX,
};
use crate::catalog::pg_type_d::{FLOAT8OID, INT4OID, NUMERICOID};
use crate::common::hashfn::hash_uint32;
use crate::common::int::{
    pg_add_s64_overflow, pg_add_u64_overflow, pg_mul_s64_overflow, pg_mul_u64_overflow,
    pg_sub_s64_overflow,
};
use crate::lib::stringinfo::{initReadOnlyStringInfo, StringInfo, StringInfoData};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgend, pq_getmsgint, pq_getmsgint64, pq_sendint16,
    pq_sendint32, pq_sendint64,
};
use crate::postgres::{
    CStringGetDatum, DatumGetCString, DatumGetInt32, DatumGetUInt32, DatumGetUInt64, Float8GetDatum,
    Int64GetDatum, PointerGetDatum, UInt32GetDatum, UInt64GetDatum,
};
use crate::{PG_DETOAST_DATUM, PG_DETOAST_DATUM_COPY, PG_GETARG_DATUM};
use crate::{DirectFunctionCall1, DirectFunctionCall2};
use crate::utils::adt::float::{
    get_float4_infinity, get_float4_nan, get_float8_infinity, get_float8_nan,
};
use crate::common::int::{pg_abs_s32, pg_abs_s64};
use crate::common::hashfn::{hash_any, hash_any_extended};
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::nodes::nodes::Node;
use crate::port::pgstrcasecmp::pg_strncasecmp;
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE, VARSIZE_ANY_EXHDR};
use core::ffi::{c_char, c_int, c_void};

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

// fmgr interface helpers from utils/numeric.h.
#[inline]
unsafe fn DatumGetNumeric(X: Datum) -> Numeric {
    PG_DETOAST_DATUM!(X) as Numeric
}
#[inline]
unsafe fn DatumGetNumericCopy(X: Datum) -> Numeric {
    PG_DETOAST_DATUM_COPY!(X) as Numeric
}
#[inline]
fn NumericGetDatum(X: Numeric) -> Datum {
    PointerGetDatum(X as *const c_void)
}

macro_rules! PG_GETARG_NUMERIC {
    ($fcinfo:expr, $n:expr) => {
        DatumGetNumeric($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_RETURN_NUMERIC {
    ($x:expr) => {
        return NumericGetDatum($x)
    };
}
// Local Max/Min mirroring c.h.
macro_rules! Max {
    ($a:expr, $b:expr) => {{
        let a = $a;
        let b = $b;
        if a > b {
            a
        } else {
            b
        }
    }};
}
macro_rules! Min {
    ($a:expr, $b:expr) => {{
        let a = $a;
        let b = $b;
        if a < b {
            a
        } else {
            b
        }
    }};
}

// i64abs() from c.h.
#[inline]
fn i64abs(i: int64) -> int64 {
    i.unsigned_abs() as int64
}

// <math.h> / <stdlib.h> bindings used directly by the numeric engine.
extern "C" {
    fn log10(x: f64) -> f64;
    fn log(x: f64) -> f64;
    fn sqrt(x: f64) -> f64;
    fn fabs(x: f64) -> f64;
    fn strtod(s: *const c_char, endptr: *mut *mut c_char) -> f64;
    fn strlen(s: *const c_char) -> usize;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn isspace(c: c_int) -> c_int;
    fn isdigit(c: c_int) -> c_int;
    fn isxdigit(c: c_int) -> c_int;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

const HUGE_VAL: f64 = f64::INFINITY;
const DBL_DIG: usize = 15;
const FLT_DIG: usize = 6;

// Limits from utils/numeric.h.
const NUMERIC_MAX_PRECISION: c_int = 1000;
const NUMERIC_MIN_SCALE: c_int = -1000;
const NUMERIC_MAX_SCALE: c_int = 1000;
const NUMERIC_MAX_DISPLAY_SCALE: c_int = NUMERIC_MAX_PRECISION;
const NUMERIC_MIN_DISPLAY_SCALE: c_int = 0;
const NUMERIC_MAX_RESULT_SCALE: c_int = NUMERIC_MAX_PRECISION * 2;
const NUMERIC_MIN_SIG_DIGITS: c_int = 16;

// TODO(pg-port): ArrayGetIntegerTypmods lives in utils/array (arrayutils.c).
unsafe fn ArrayGetIntegerTypmods(_arr: *mut c_void, n: *mut c_int) -> *mut int32 {
    *n = 0;
    null_mut()
}

// TODO(pg-port): AggCheckCallContext lives in utils/fmgr (executor/nodeAgg).
unsafe fn AggCheckCallContext(
    _fcinfo: FunctionCallInfo,
    aggcontext: *mut MemoryContext,
) -> c_int {
    if !aggcontext.is_null() {
        *aggcontext = CurrentMemoryContext;
    }
    1
}

// ----------
// Local data types
//
// NBASE = 10000 is the only supported base.
// ----------
const NBASE: c_int = 10000;
const HALF_NBASE: c_int = 5000;
const DEC_DIGITS: c_int = 4; // decimal digits per NBASE digit
const MUL_GUARD_DIGITS: c_int = 2; // these are measured in NBASE digits
const DIV_GUARD_DIGITS: c_int = 4;

/// The type NumericDigit is signed and wide enough to store a digit.
pub type NumericDigit = int16;

const NBASE_SQR: c_int = NBASE * NBASE;

// The Numeric type as stored on disk.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct NumericShort {
    pub n_header: uint16,           // Sign + display scale + weight
    pub n_data: [NumericDigit; 0],  // Digits (FLEXIBLE_ARRAY_MEMBER)
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NumericLong {
    pub n_sign_dscale: uint16,      // Sign + display scale
    pub n_weight: int16,            // Weight of 1st digit
    pub n_data: [NumericDigit; 0],  // Digits (FLEXIBLE_ARRAY_MEMBER)
}

#[repr(C)]
pub union NumericChoice {
    pub n_header: uint16,           // Header word
    pub n_long: NumericLong,   // Long form (4-byte header)
    pub n_short: NumericShort, // Short form (2-byte header)
}

#[repr(C)]
pub struct NumericData {
    pub vl_len_: int32,             // varlena header (do not touch directly!)
    pub choice: NumericChoice,      // choice of format
}

pub type Numeric = *mut NumericData;

// Interpretation of high bits.
const NUMERIC_SIGN_MASK: uint16 = 0xC000;
const NUMERIC_POS: c_int = 0x0000;
const NUMERIC_NEG: c_int = 0x4000;
const NUMERIC_SHORT: uint16 = 0x8000;
const NUMERIC_SPECIAL: uint16 = 0xC000;

#[inline]
unsafe fn NUMERIC_FLAGBITS(n: Numeric) -> uint16 {
    (*n).choice.n_header & NUMERIC_SIGN_MASK
}
#[inline]
unsafe fn NUMERIC_IS_SHORT(n: Numeric) -> bool {
    NUMERIC_FLAGBITS(n) == NUMERIC_SHORT
}
#[inline]
unsafe fn NUMERIC_IS_SPECIAL(n: Numeric) -> bool {
    NUMERIC_FLAGBITS(n) == NUMERIC_SPECIAL
}

const NUMERIC_HDRSZ: Size = (VARHDRSZ as Size)
    + core::mem::size_of::<uint16>()
    + core::mem::size_of::<int16>();
const NUMERIC_HDRSZ_SHORT: Size = (VARHDRSZ as Size) + core::mem::size_of::<uint16>();

#[inline]
unsafe fn NUMERIC_HEADER_IS_SHORT(n: Numeric) -> bool {
    ((*n).choice.n_header & 0x8000) != 0
}
#[inline]
unsafe fn NUMERIC_HEADER_SIZE(n: Numeric) -> Size {
    (VARHDRSZ as Size)
        + core::mem::size_of::<uint16>()
        + (if NUMERIC_HEADER_IS_SHORT(n) {
            0
        } else {
            core::mem::size_of::<int16>()
        })
}

// Definitions for special values (NaN, positive infinity, negative infinity).
const NUMERIC_EXT_SIGN_MASK: uint16 = 0xF000; // high bits plus NaN/Inf flag bits
const NUMERIC_NAN: c_int = 0xC000;
const NUMERIC_PINF: c_int = 0xD000;
const NUMERIC_NINF: c_int = 0xF000;
const NUMERIC_INF_SIGN_MASK: uint16 = 0x2000;

#[inline]
unsafe fn NUMERIC_EXT_FLAGBITS(n: Numeric) -> uint16 {
    (*n).choice.n_header & NUMERIC_EXT_SIGN_MASK
}
#[inline]
unsafe fn NUMERIC_IS_NAN(n: Numeric) -> bool {
    (*n).choice.n_header == NUMERIC_NAN as uint16
}
#[inline]
unsafe fn NUMERIC_IS_PINF(n: Numeric) -> bool {
    (*n).choice.n_header == NUMERIC_PINF as uint16
}
#[inline]
unsafe fn NUMERIC_IS_NINF(n: Numeric) -> bool {
    (*n).choice.n_header == NUMERIC_NINF as uint16
}
#[inline]
unsafe fn NUMERIC_IS_INF(n: Numeric) -> bool {
    ((*n).choice.n_header & !NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF as uint16
}

// Short format definitions.
const NUMERIC_SHORT_SIGN_MASK: uint16 = 0x2000;
const NUMERIC_SHORT_DSCALE_MASK: uint16 = 0x1F80;
const NUMERIC_SHORT_DSCALE_SHIFT: c_int = 7;
const NUMERIC_SHORT_DSCALE_MAX: c_int =
    (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT) as c_int;
const NUMERIC_SHORT_WEIGHT_SIGN_MASK: uint16 = 0x0040;
const NUMERIC_SHORT_WEIGHT_MASK: uint16 = 0x003F;
const NUMERIC_SHORT_WEIGHT_MAX: c_int = NUMERIC_SHORT_WEIGHT_MASK as c_int;
const NUMERIC_SHORT_WEIGHT_MIN: c_int = -(NUMERIC_SHORT_WEIGHT_MASK as c_int + 1);

// Extract sign, display scale, weight.
const NUMERIC_DSCALE_MASK: uint16 = 0x3FFF;
const NUMERIC_DSCALE_MAX: c_int = NUMERIC_DSCALE_MASK as c_int;

#[inline]
unsafe fn NUMERIC_SIGN(n: Numeric) -> c_int {
    if NUMERIC_IS_SHORT(n) {
        if ((*n).choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) != 0 {
            NUMERIC_NEG
        } else {
            NUMERIC_POS
        }
    } else if NUMERIC_IS_SPECIAL(n) {
        NUMERIC_EXT_FLAGBITS(n) as c_int
    } else {
        NUMERIC_FLAGBITS(n) as c_int
    }
}
#[inline]
unsafe fn NUMERIC_DSCALE(n: Numeric) -> c_int {
    if NUMERIC_HEADER_IS_SHORT(n) {
        (((*n).choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT)
            as c_int
    } else {
        ((*n).choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK) as c_int
    }
}
#[inline]
unsafe fn NUMERIC_WEIGHT(n: Numeric) -> c_int {
    if NUMERIC_HEADER_IS_SHORT(n) {
        let h = (*n).choice.n_short.n_header;
        ((if (h & NUMERIC_SHORT_WEIGHT_SIGN_MASK) != 0 {
            !(NUMERIC_SHORT_WEIGHT_MASK as c_int)
        } else {
            0
        }) | (h & NUMERIC_SHORT_WEIGHT_MASK) as c_int)
    } else {
        (*n).choice.n_long.n_weight as c_int
    }
}

// Maximum weight of a stored Numeric value (int16 weight in NumericLong).
const NUMERIC_WEIGHT_MAX: c_int = PG_INT16_MAX as c_int;

// ----------
// NumericVar is the format we use for arithmetic.
// ----------
#[repr(C)]
pub struct NumericVar {
    pub ndigits: c_int,         // # of digits in digits[] - can be 0!
    pub weight: c_int,          // weight of first digit
    pub sign: c_int,            // NUMERIC_POS, _NEG, _NAN, _PINF, or _NINF
    pub dscale: c_int,          // display scale
    pub buf: *mut NumericDigit, // start of palloc'd space for digits[]
    pub digits: *mut NumericDigit, // base-NBASE digits
}

// The preinitialized const NumericVars (const_zero, etc.) are read-only, like
// the `static const NumericVar`s in numeric.c.  They embed raw pointers into
// const digit arrays, which makes NumericVar not Sync by default; assert Sync so
// they can live in `static` items (they are never mutated through the statics).
unsafe impl Sync for NumericVar {}

// ----------
// Data for generate_series
// ----------
#[repr(C)]
struct generate_series_numeric_fctx {
    current: NumericVar,
    stop: NumericVar,
    step: NumericVar,
}

// ----------
// Sort support.
// ----------
#[repr(C)]
struct NumericSortSupport {
    buf: *mut c_void,       // buffer for short varlenas
    input_count: int64,     // number of non-null values seen
    estimating: bool,       // true if estimating cardinality
    abbr_card: hyperLogLogState, // cardinality estimator
}

// ----------
// Fast sum accumulator.
// ----------
#[repr(C)]
pub struct NumericSumAccum {
    ndigits: c_int,
    weight: c_int,
    dscale: c_int,
    num_uncarried: c_int,
    have_carry_space: bool,
    pos_digits: *mut int32,
    neg_digits: *mut int32,
}

// Abbreviated-key representations (SIZEOF_DATUM == 8 on our targets).
#[inline]
fn NumericAbbrevGetDatum(x: int64) -> Datum {
    x as usize as Datum
}
#[inline]
fn DatumGetNumericAbbrev(x: Datum) -> int64 {
    x as int64
}
const NUMERIC_ABBREV_NAN: Datum = PG_INT64_MIN as usize as Datum;
const NUMERIC_ABBREV_PINF: Datum = (-PG_INT64_MAX) as usize as Datum;
const NUMERIC_ABBREV_NINF: Datum = PG_INT64_MAX as usize as Datum;

// ----------
// Some preinitialized constants
// ----------
static const_zero_data: [NumericDigit; 1] = [0];
static const_zero: NumericVar = NumericVar {
    ndigits: 0,
    weight: 0,
    sign: NUMERIC_POS,
    dscale: 0,
    buf: null_mut(),
    digits: const_zero_data.as_ptr() as *mut NumericDigit,
};

static const_one_data: [NumericDigit; 1] = [1];
static const_one: NumericVar = NumericVar {
    ndigits: 1,
    weight: 0,
    sign: NUMERIC_POS,
    dscale: 0,
    buf: null_mut(),
    digits: const_one_data.as_ptr() as *mut NumericDigit,
};

static const_minus_one: NumericVar = NumericVar {
    ndigits: 1,
    weight: 0,
    sign: NUMERIC_NEG,
    dscale: 0,
    buf: null_mut(),
    digits: const_one_data.as_ptr() as *mut NumericDigit,
};

static const_two_data: [NumericDigit; 1] = [2];
static const_two: NumericVar = NumericVar {
    ndigits: 1,
    weight: 0,
    sign: NUMERIC_POS,
    dscale: 0,
    buf: null_mut(),
    digits: const_two_data.as_ptr() as *mut NumericDigit,
};

static const_zero_point_nine_data: [NumericDigit; 1] = [9000];
static const_zero_point_nine: NumericVar = NumericVar {
    ndigits: 1,
    weight: -1,
    sign: NUMERIC_POS,
    dscale: 1,
    buf: null_mut(),
    digits: const_zero_point_nine_data.as_ptr() as *mut NumericDigit,
};

static const_one_point_one_data: [NumericDigit; 2] = [1, 1000];
static const_one_point_one: NumericVar = NumericVar {
    ndigits: 2,
    weight: 0,
    sign: NUMERIC_POS,
    dscale: 1,
    buf: null_mut(),
    digits: const_one_point_one_data.as_ptr() as *mut NumericDigit,
};

static const_nan: NumericVar = NumericVar {
    ndigits: 0,
    weight: 0,
    sign: NUMERIC_NAN,
    dscale: 0,
    buf: null_mut(),
    digits: null_mut(),
};

static const_pinf: NumericVar = NumericVar {
    ndigits: 0,
    weight: 0,
    sign: NUMERIC_PINF,
    dscale: 0,
    buf: null_mut(),
    digits: null_mut(),
};

static const_ninf: NumericVar = NumericVar {
    ndigits: 0,
    weight: 0,
    sign: NUMERIC_NINF,
    dscale: 0,
    buf: null_mut(),
    digits: null_mut(),
};

static round_powers: [c_int; 4] = [0, 1000, 100, 10];

// Const static NumericVars are immutable; the &mut casts below only read.
unsafe fn cvar(v: &'static NumericVar) -> *const NumericVar {
    v as *const NumericVar
}

// ----------
// digit buffer helpers
// ----------
#[inline]
unsafe fn digitbuf_alloc(ndigits: c_int) -> *mut NumericDigit {
    palloc((ndigits as usize) * core::mem::size_of::<NumericDigit>()) as *mut NumericDigit
}
#[inline]
unsafe fn digitbuf_free(buf: *mut NumericDigit) {
    if !buf.is_null() {
        pfree(buf as *mut c_void);
    }
}
#[inline]
unsafe fn init_var(v: *mut NumericVar) {
    memset(v as *mut c_void, 0, core::mem::size_of::<NumericVar>());
}

#[inline]
unsafe fn NUMERIC_DIGITS(num: Numeric) -> *mut NumericDigit {
    if NUMERIC_HEADER_IS_SHORT(num) {
        (*num).choice.n_short.n_data.as_ptr() as *mut NumericDigit
    } else {
        (*num).choice.n_long.n_data.as_ptr() as *mut NumericDigit
    }
}
#[inline]
unsafe fn NUMERIC_NDIGITS(num: Numeric) -> c_int {
    ((VARSIZE(num as *const c_char) as Size - NUMERIC_HEADER_SIZE(num))
        / core::mem::size_of::<NumericDigit>()) as c_int
}
#[inline]
fn NUMERIC_CAN_BE_SHORT(scale: c_int, weight: c_int) -> bool {
    scale <= NUMERIC_SHORT_DSCALE_MAX
        && weight <= NUMERIC_SHORT_WEIGHT_MAX
        && weight >= NUMERIC_SHORT_WEIGHT_MIN
}

// TODO(pg-port): real hyperLogLogState lives in lib/hyperloglog; minimal stub.
#[repr(C)]
struct hyperLogLogState {
    registerWidth: u8,
    nRegisters: usize,
    arrSize: usize,
    hashesArr: *mut u8,
}
// TODO(pg-port): real HLL routines live in lib/hyperloglog.
unsafe fn initHyperLogLog(_cE: *mut hyperLogLogState, _bwidth: u8) {}
unsafe fn addHyperLogLog(_cE: *mut hyperLogLogState, _hash: uint32) {}
unsafe fn estimateHyperLogLog(_cE: *mut hyperLogLogState) -> f64 {
    0.0
}

// TODO(pg-port): trace_sort GUC lives in utils/sortsupport / tuplesort.
static mut trace_sort: bool = false;

// TODO(pg-port): SortSupport / sortsupport.h infrastructure.
type SortSupport = *mut c_void;

// TODO(pg-port): pg_prng_state lives in common/pg_prng.h.
#[repr(C)]
pub struct pg_prng_state {
    s0: uint64,
    s1: uint64,
}
// TODO(pg-port): real pg_prng routines live in common/pg_prng.
unsafe fn pg_prng_uint64_range(_state: *mut pg_prng_state, rmin: uint64, rmax: uint64) -> uint64 {
    rmin.wrapping_add((rmax.wrapping_sub(rmin)) / 2)
}

// TODO(pg-port): XLogRecPtr / pg_lsn lives in access/xlogdefs & utils/pg_lsn.
type XLogRecPtr = uint64;

// ----------------------------------------------------------------------
//
// Input-, output- and rounding-functions
//
// ----------------------------------------------------------------------

/// numeric_in() - Input function for numeric data type
pub unsafe fn numeric_in(fcinfo: FunctionCallInfo) -> Datum {
    let str = PG_GETARG_CSTRING!(fcinfo, 0);
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let escontext = (*fcinfo).context as *mut Node;
    let res: Numeric;
    let mut cp: *const c_char;
    let numstart: *const c_char;
    let mut sign: c_int;

    /* Skip leading spaces */
    cp = str;
    while *cp != 0 {
        if isspace(*cp as u8 as c_int) == 0 {
            break;
        }
        cp = cp.add(1);
    }

    numstart = cp;
    sign = NUMERIC_POS;

    if *cp == b'+' as c_char {
        cp = cp.add(1);
    } else if *cp == b'-' as c_char {
        sign = NUMERIC_NEG;
        cp = cp.add(1);
    }

    if isdigit(*cp as u8 as c_int) == 0 && *cp != b'.' as c_char {
        if pg_strncasecmp(numstart, c"NaN".as_ptr(), 3) == 0 {
            res = make_result(cvar(&const_nan));
            cp = numstart.add(3);
        } else if pg_strncasecmp(cp, c"Infinity".as_ptr(), 8) == 0 {
            res = make_result(if sign == NUMERIC_POS {
                cvar(&const_pinf)
            } else {
                cvar(&const_ninf)
            });
            cp = cp.add(8);
        } else if pg_strncasecmp(cp, c"inf".as_ptr(), 3) == 0 {
            res = make_result(if sign == NUMERIC_POS {
                cvar(&const_pinf)
            } else {
                cvar(&const_ninf)
            });
            cp = cp.add(3);
        } else {
            return numeric_in_invalid_syntax(escontext, str);
        }

        while *cp != 0 {
            if isspace(*cp as u8 as c_int) == 0 {
                return numeric_in_invalid_syntax(escontext, str);
            }
            cp = cp.add(1);
        }

        if !apply_typmod_special(res, typmod, escontext) {
            PG_RETURN_NULL!(fcinfo);
        }
    } else {
        let mut value: NumericVar = core::mem::zeroed();
        let base: c_int;
        let mut have_error: bool = false;

        init_var(&mut value);

        if *cp.add(0) == b'0' as c_char {
            base = match *cp.add(1) as u8 {
                b'x' | b'X' => 16,
                b'o' | b'O' => 8,
                b'b' | b'B' => 2,
                _ => 10,
            };
        } else {
            base = 10;
        }

        if base == 10 {
            if !set_var_from_str(str, cp, &mut value, &mut cp, escontext) {
                PG_RETURN_NULL!(fcinfo);
            }
            value.sign = sign;
        } else {
            if !set_var_from_non_decimal_integer_str(
                str,
                cp.add(2),
                sign,
                base,
                &mut value,
                &mut cp,
                escontext,
            ) {
                PG_RETURN_NULL!(fcinfo);
            }
        }

        while *cp != 0 {
            if isspace(*cp as u8 as c_int) == 0 {
                return numeric_in_invalid_syntax(escontext, str);
            }
            cp = cp.add(1);
        }

        if !apply_typmod(&mut value, typmod, escontext) {
            PG_RETURN_NULL!(fcinfo);
        }

        res = make_result_opt_error(&value, &mut have_error);

        if have_error {
            ereport!(
                ERROR,
                errmsg!("value overflows numeric format")
            );
            return 0;
        }

        free_var(&mut value);
    }

    PG_RETURN_NUMERIC!(res)
}

unsafe fn numeric_in_invalid_syntax(_escontext: *mut Node, str: *const c_char) -> Datum {
    ereport!(
        ERROR,
        errmsg!(
            "invalid input syntax for type {}: \"{}\"",
            "numeric",
            std::ffi::CStr::from_ptr(str).to_string_lossy()
        )
    );
    0
}

/// numeric_out() - Output function for numeric data type
pub unsafe fn numeric_out(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let mut x: NumericVar = core::mem::zeroed();
    let str: *mut c_char;

    if NUMERIC_IS_SPECIAL(num) {
        if NUMERIC_IS_PINF(num) {
            PG_RETURN_CSTRING!(pstrdup(c"Infinity".as_ptr()));
        } else if NUMERIC_IS_NINF(num) {
            PG_RETURN_CSTRING!(pstrdup(c"-Infinity".as_ptr()));
        } else {
            PG_RETURN_CSTRING!(pstrdup(c"NaN".as_ptr()));
        }
    }

    init_var_from_num(num, &mut x);
    str = get_str_from_var(&x);

    PG_RETURN_CSTRING!(str)
}

/// numeric_is_nan() - Is Numeric value a NaN?
pub unsafe fn numeric_is_nan(num: Numeric) -> bool {
    NUMERIC_IS_NAN(num)
}

/// numeric_is_inf() - Is Numeric value an infinity?
pub unsafe fn numeric_is_inf(num: Numeric) -> bool {
    NUMERIC_IS_INF(num)
}

/// numeric_is_integral() - Is Numeric value integral?
unsafe fn numeric_is_integral(num: Numeric) -> bool {
    let mut arg: NumericVar = core::mem::zeroed();

    if NUMERIC_IS_SPECIAL(num) {
        if NUMERIC_IS_NAN(num) {
            return false;
        }
        return true;
    }

    init_var_from_num(num, &mut arg);

    arg.ndigits == 0 || arg.ndigits <= arg.weight + 1
}

/// make_numeric_typmod() - Pack numeric precision and scale into a typmod.
#[inline]
fn make_numeric_typmod(precision: c_int, scale: c_int) -> int32 {
    ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ
}

/// Because of the offset, valid numeric typmods are at least VARHDRSZ
#[inline]
fn is_valid_numeric_typmod(typmod: int32) -> bool {
    typmod >= VARHDRSZ
}

/// numeric_typmod_precision() - Extract the precision from a numeric typmod.
#[inline]
fn numeric_typmod_precision(typmod: int32) -> c_int {
    ((typmod - VARHDRSZ) >> 16) & 0xffff
}

/// numeric_typmod_scale() - Extract the scale from a numeric typmod.
#[inline]
fn numeric_typmod_scale(typmod: int32) -> c_int {
    (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024
}

/// numeric_maximum_size() - Maximum size of a numeric with given typmod.
pub unsafe fn numeric_maximum_size(typmod: int32) -> int32 {
    let precision: c_int;
    let numeric_digits: c_int;

    if !is_valid_numeric_typmod(typmod) {
        return -1;
    }

    precision = numeric_typmod_precision(typmod);

    numeric_digits = (precision + 2 * (DEC_DIGITS - 1)) / DEC_DIGITS;

    (NUMERIC_HDRSZ as c_int) + (numeric_digits * core::mem::size_of::<NumericDigit>() as c_int)
}

/// numeric_out_sci() - Output function for numeric in scientific notation.
pub unsafe fn numeric_out_sci(num: Numeric, scale: c_int) -> *mut c_char {
    let mut x: NumericVar = core::mem::zeroed();
    let str: *mut c_char;

    if NUMERIC_IS_SPECIAL(num) {
        if NUMERIC_IS_PINF(num) {
            return pstrdup(c"Infinity".as_ptr());
        } else if NUMERIC_IS_NINF(num) {
            return pstrdup(c"-Infinity".as_ptr());
        } else {
            return pstrdup(c"NaN".as_ptr());
        }
    }

    init_var_from_num(num, &mut x);
    str = get_str_from_var_sci(&x, scale);

    str
}

/// numeric_normalize() - Output suppressing insignificant trailing zeroes.
pub unsafe fn numeric_normalize(num: Numeric) -> *mut c_char {
    let mut x: NumericVar = core::mem::zeroed();
    let str: *mut c_char;
    let mut last: c_int;

    if NUMERIC_IS_SPECIAL(num) {
        if NUMERIC_IS_PINF(num) {
            return pstrdup(c"Infinity".as_ptr());
        } else if NUMERIC_IS_NINF(num) {
            return pstrdup(c"-Infinity".as_ptr());
        } else {
            return pstrdup(c"NaN".as_ptr());
        }
    }

    init_var_from_num(num, &mut x);
    str = get_str_from_var(&x);

    if !strchr(str, b'.' as c_int).is_null() {
        last = strlen(str) as c_int - 1;
        while *str.add(last as usize) == b'0' as c_char {
            last -= 1;
        }

        if *str.add(last as usize) == b'.' as c_char {
            last -= 1;
        }

        *str.add((last + 1) as usize) = b'\0' as c_char;
    }

    str
}

/// numeric_recv - converts external binary format to numeric
pub unsafe fn numeric_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let mut value: NumericVar = core::mem::zeroed();
    let res: Numeric;
    let len: c_int;
    let mut i: c_int;

    init_var(&mut value);

    len = (pq_getmsgint(buf, core::mem::size_of::<uint16>() as c_int) as uint16) as c_int;

    alloc_var(&mut value, len);

    value.weight = (pq_getmsgint(buf, core::mem::size_of::<int16>() as c_int) as uint16 as int16)
        as c_int;
    /* we allow any int16 for weight --- OK? */

    value.sign = (pq_getmsgint(buf, core::mem::size_of::<uint16>() as c_int) as uint16) as c_int;
    if !(value.sign == NUMERIC_POS
        || value.sign == NUMERIC_NEG
        || value.sign == NUMERIC_NAN
        || value.sign == NUMERIC_PINF
        || value.sign == NUMERIC_NINF)
    {
        ereport!(ERROR, errmsg!("invalid sign in external \"numeric\" value"));
    }

    value.dscale = (pq_getmsgint(buf, core::mem::size_of::<uint16>() as c_int) as uint16) as c_int;
    if (value.dscale & NUMERIC_DSCALE_MASK as c_int) != value.dscale {
        ereport!(ERROR, errmsg!("invalid scale in external \"numeric\" value"));
    }

    i = 0;
    while i < len {
        let d: NumericDigit =
            pq_getmsgint(buf, core::mem::size_of::<NumericDigit>() as c_int) as NumericDigit;

        if (d as c_int) < 0 || (d as c_int) >= NBASE {
            ereport!(ERROR, errmsg!("invalid digit in external \"numeric\" value"));
        }
        *value.digits.add(i as usize) = d;
        i += 1;
    }

    if value.sign == NUMERIC_POS || value.sign == NUMERIC_NEG {
        trunc_var(&mut value, value.dscale);
        apply_typmod(&mut value, typmod, null_mut());
        res = make_result(&value);
    } else {
        res = make_result(&value);
        apply_typmod_special(res, typmod, null_mut());
    }

    free_var(&mut value);

    PG_RETURN_NUMERIC!(res)
}

/// numeric_send - converts numeric to binary format
pub unsafe fn numeric_send(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let mut x: NumericVar = core::mem::zeroed();
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut i: c_int;

    init_var_from_num(num, &mut x);

    pq_begintypsend(&mut buf);

    pq_sendint16(&mut buf, x.ndigits as uint16);
    pq_sendint16(&mut buf, x.weight as uint16);
    pq_sendint16(&mut buf, x.sign as uint16);
    pq_sendint16(&mut buf, x.dscale as uint16);
    i = 0;
    while i < x.ndigits {
        pq_sendint16(&mut buf, *x.digits.add(i as usize) as uint16);
        i += 1;
    }

    PG_RETURN_POINTER!(pq_endtypsend(&mut buf))
}


/// numeric_support() - Planner support function for numeric() length coercion.
/// TODO(pg-port): SupportRequestSimplify / FuncExpr / relabel_to_typmod live in
/// nodes/supportnodes & optimizer; faithful logic retained but uses local stubs.
pub unsafe fn numeric_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let ret: *mut Node = null_mut();

    // TODO(pg-port): IsA(rawreq, SupportRequestSimplify) and the typmod
    // flattening require nodes/supportnodes.h + optimizer.h; not yet ported.
    let _ = rawreq;

    PG_RETURN_POINTER!(ret)
}

/// numeric() - Apply precision and scale of an attribute to a value.
pub unsafe fn numeric(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let new: Numeric;
    let precision: c_int;
    let scale: c_int;
    let ddigits: c_int;
    let maxdigits: c_int;
    let dscale: c_int;
    let mut var: NumericVar = core::mem::zeroed();

    if NUMERIC_IS_SPECIAL(num) {
        apply_typmod_special(num, typmod, null_mut());
        PG_RETURN_NUMERIC!(duplicate_numeric(num));
    }

    if !is_valid_numeric_typmod(typmod) {
        PG_RETURN_NUMERIC!(duplicate_numeric(num));
    }

    precision = numeric_typmod_precision(typmod);
    scale = numeric_typmod_scale(typmod);
    maxdigits = precision - scale;

    dscale = Max!(scale, 0);

    ddigits = (NUMERIC_WEIGHT(num) + 1) * DEC_DIGITS;
    if ddigits <= maxdigits
        && scale >= NUMERIC_DSCALE(num)
        && (NUMERIC_CAN_BE_SHORT(dscale, NUMERIC_WEIGHT(num)) || !NUMERIC_IS_SHORT(num))
    {
        new = duplicate_numeric(num);
        if NUMERIC_IS_SHORT(num) {
            (*new).choice.n_short.n_header =
                ((*num).choice.n_short.n_header & !NUMERIC_SHORT_DSCALE_MASK)
                    | ((dscale << NUMERIC_SHORT_DSCALE_SHIFT) as uint16);
        } else {
            (*new).choice.n_long.n_sign_dscale =
                NUMERIC_SIGN(new) as uint16 | ((dscale as uint16) & NUMERIC_DSCALE_MASK);
        }
        PG_RETURN_NUMERIC!(new);
    }

    init_var(&mut var);

    set_var_from_num(num, &mut var);
    apply_typmod(&mut var, typmod, null_mut());
    new = make_result(&var);

    free_var(&mut var);

    PG_RETURN_NUMERIC!(new)
}

pub unsafe fn numerictypmodin(fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): ArrayType / ArrayGetIntegerTypmods live in utils/array;
    // not yet ported, so this faithfully validates via a stub array reader.
    let ta = PG_GETARG_POINTER!(fcinfo, 0) as *mut c_void;
    let mut n: c_int = 0;
    let tl: *mut int32 = ArrayGetIntegerTypmods(ta, &mut n);
    let typmod: int32;

    if n == 2 {
        if *tl.add(0) < 1 || *tl.add(0) > NUMERIC_MAX_PRECISION {
            ereport!(
                ERROR,
                errmsg!(
                    "NUMERIC precision {} must be between 1 and {}",
                    *tl.add(0),
                    NUMERIC_MAX_PRECISION
                )
            );
        }
        if *tl.add(1) < NUMERIC_MIN_SCALE || *tl.add(1) > NUMERIC_MAX_SCALE {
            ereport!(
                ERROR,
                errmsg!(
                    "NUMERIC scale {} must be between {} and {}",
                    *tl.add(1),
                    NUMERIC_MIN_SCALE,
                    NUMERIC_MAX_SCALE
                )
            );
        }
        typmod = make_numeric_typmod(*tl.add(0), *tl.add(1));
    } else if n == 1 {
        if *tl.add(0) < 1 || *tl.add(0) > NUMERIC_MAX_PRECISION {
            ereport!(
                ERROR,
                errmsg!(
                    "NUMERIC precision {} must be between 1 and {}",
                    *tl.add(0),
                    NUMERIC_MAX_PRECISION
                )
            );
        }
        typmod = make_numeric_typmod(*tl.add(0), 0);
    } else {
        ereport!(ERROR, errmsg!("invalid NUMERIC type modifier"));
        typmod = 0;
    }

    PG_RETURN_INT32!(typmod)
}

pub unsafe fn numerictypmodout(fcinfo: FunctionCallInfo) -> Datum {
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let res: *mut c_char = palloc(64) as *mut c_char;

    if is_valid_numeric_typmod(typmod) {
        snprintf(
            res,
            64,
            c"(%d,%d)".as_ptr(),
            numeric_typmod_precision(typmod),
            numeric_typmod_scale(typmod),
        );
    } else {
        *res = b'\0' as c_char;
    }

    PG_RETURN_CSTRING!(res)
}


// ----------------------------------------------------------------------
//
// Sign manipulation, rounding and the like
//
// ----------------------------------------------------------------------

pub unsafe fn numeric_abs(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let res: Numeric;

    res = duplicate_numeric(num);

    if NUMERIC_IS_SHORT(num) {
        (*res).choice.n_short.n_header =
            (*num).choice.n_short.n_header & !NUMERIC_SHORT_SIGN_MASK;
    } else if NUMERIC_IS_SPECIAL(num) {
        (*res).choice.n_short.n_header =
            (*num).choice.n_short.n_header & !NUMERIC_INF_SIGN_MASK;
    } else {
        (*res).choice.n_long.n_sign_dscale = NUMERIC_POS as uint16 | NUMERIC_DSCALE(num) as uint16;
    }

    PG_RETURN_NUMERIC!(res)
}

pub unsafe fn numeric_uminus(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let res: Numeric;

    res = duplicate_numeric(num);

    if NUMERIC_IS_SPECIAL(num) {
        if !NUMERIC_IS_NAN(num) {
            (*res).choice.n_short.n_header =
                (*num).choice.n_short.n_header ^ NUMERIC_INF_SIGN_MASK;
        }
    } else if NUMERIC_NDIGITS(num) != 0 {
        if NUMERIC_IS_SHORT(num) {
            (*res).choice.n_short.n_header =
                (*num).choice.n_short.n_header ^ NUMERIC_SHORT_SIGN_MASK;
        } else if NUMERIC_SIGN(num) == NUMERIC_POS {
            (*res).choice.n_long.n_sign_dscale =
                NUMERIC_NEG as uint16 | NUMERIC_DSCALE(num) as uint16;
        } else {
            (*res).choice.n_long.n_sign_dscale =
                NUMERIC_POS as uint16 | NUMERIC_DSCALE(num) as uint16;
        }
    }

    PG_RETURN_NUMERIC!(res)
}

pub unsafe fn numeric_uplus(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);

    PG_RETURN_NUMERIC!(duplicate_numeric(num))
}

/// numeric_sign_internal() - sign of finite/infinite num (caller handles NaN).
unsafe fn numeric_sign_internal(num: Numeric) -> c_int {
    if NUMERIC_IS_SPECIAL(num) {
        Assert!(!NUMERIC_IS_NAN(num));
        if NUMERIC_IS_PINF(num) {
            return 1;
        } else {
            return -1;
        }
    } else if NUMERIC_NDIGITS(num) == 0 {
        0
    } else if NUMERIC_SIGN(num) == NUMERIC_NEG {
        -1
    } else {
        1
    }
}

/// numeric_sign()
pub unsafe fn numeric_sign(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);

    if NUMERIC_IS_NAN(num) {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
    }

    match numeric_sign_internal(num) {
        0 => PG_RETURN_NUMERIC!(make_result(cvar(&const_zero))),
        1 => PG_RETURN_NUMERIC!(make_result(cvar(&const_one))),
        -1 => PG_RETURN_NUMERIC!(make_result(cvar(&const_minus_one))),
        _ => {}
    }

    Assert!(false);
    0
}

/// numeric_round() - Round a value to 'scale' digits after the decimal point.
pub unsafe fn numeric_round(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let mut scale: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let res: Numeric;
    let mut arg: NumericVar = core::mem::zeroed();

    if NUMERIC_IS_SPECIAL(num) {
        PG_RETURN_NUMERIC!(duplicate_numeric(num));
    }

    scale = Max!(scale, -(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS - 1);
    scale = Min!(scale, NUMERIC_DSCALE_MAX);

    init_var(&mut arg);
    set_var_from_num(num, &mut arg);

    round_var(&mut arg, scale);

    if scale < 0 {
        arg.dscale = 0;
    }

    res = make_result(&arg);

    free_var(&mut arg);
    PG_RETURN_NUMERIC!(res)
}

/// numeric_trunc() - Truncate a value to 'scale' digits after the decimal point.
pub unsafe fn numeric_trunc(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let mut scale: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let res: Numeric;
    let mut arg: NumericVar = core::mem::zeroed();

    if NUMERIC_IS_SPECIAL(num) {
        PG_RETURN_NUMERIC!(duplicate_numeric(num));
    }

    scale = Max!(scale, -(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS);
    scale = Min!(scale, NUMERIC_DSCALE_MAX);

    init_var(&mut arg);
    set_var_from_num(num, &mut arg);

    trunc_var(&mut arg, scale);

    if scale < 0 {
        arg.dscale = 0;
    }

    res = make_result(&arg);

    free_var(&mut arg);
    PG_RETURN_NUMERIC!(res)
}

/// numeric_ceil()
pub unsafe fn numeric_ceil(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let res: Numeric;
    let mut result: NumericVar = core::mem::zeroed();

    if NUMERIC_IS_SPECIAL(num) {
        PG_RETURN_NUMERIC!(duplicate_numeric(num));
    }

    init_var_from_num(num, &mut result);
    ceil_var(&result, &mut result);

    res = make_result(&result);
    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

/// numeric_floor()
pub unsafe fn numeric_floor(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let res: Numeric;
    let mut result: NumericVar = core::mem::zeroed();

    if NUMERIC_IS_SPECIAL(num) {
        PG_RETURN_NUMERIC!(duplicate_numeric(num));
    }

    init_var_from_num(num, &mut result);
    floor_var(&result, &mut result);

    res = make_result(&result);
    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

/// generate_series_numeric()
/// TODO(pg-port): SRF (funcapi.h SRF_* macros + FuncCallContext) not ported.
pub unsafe fn generate_series_numeric(fcinfo: FunctionCallInfo) -> Datum {
    generate_series_step_numeric(fcinfo)
}

pub unsafe fn generate_series_step_numeric(_fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): set-returning function machinery (funcapi.h SRF_*) not yet
    // ported; faithful body depends on FuncCallContext.
    unimplemented!("generate_series_step_numeric: needs funcapi SRF support")
}

/// generate_series_numeric_support()
pub unsafe fn generate_series_numeric_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let ret: *mut Node = null_mut();
    // TODO(pg-port): SupportRequestRows / estimate_expression_value live in
    // optimizer & nodes/supportnodes; not yet ported.
    let _ = rawreq;
    PG_RETURN_POINTER!(ret)
}


/// width_bucket_numeric() - numeric version of width_bucket().
pub unsafe fn width_bucket_numeric(fcinfo: FunctionCallInfo) -> Datum {
    let operand: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let bound1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let bound2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 2);
    let count: int32 = PG_GETARG_INT32!(fcinfo, 3);
    let mut count_var: NumericVar = core::mem::zeroed();
    let mut result_var: NumericVar = core::mem::zeroed();
    let mut result: int32 = 0;

    if count <= 0 {
        ereport!(ERROR, errmsg!("count must be greater than zero"));
    }

    if NUMERIC_IS_SPECIAL(operand) || NUMERIC_IS_SPECIAL(bound1) || NUMERIC_IS_SPECIAL(bound2) {
        if NUMERIC_IS_NAN(operand) || NUMERIC_IS_NAN(bound1) || NUMERIC_IS_NAN(bound2) {
            ereport!(
                ERROR,
                errmsg!("operand, lower bound, and upper bound cannot be NaN")
            );
        }
        if NUMERIC_IS_INF(bound1) || NUMERIC_IS_INF(bound2) {
            ereport!(ERROR, errmsg!("lower and upper bounds must be finite"));
        }
    }

    init_var(&mut result_var);
    init_var(&mut count_var);

    int64_to_numericvar(count as int64, &mut count_var);

    match cmp_numerics(bound1, bound2) {
        0 => {
            ereport!(ERROR, errmsg!("lower bound cannot equal upper bound"));
        }
        -1 => {
            if cmp_numerics(operand, bound1) < 0 {
                set_var_from_var(cvar(&const_zero), &mut result_var);
            } else if cmp_numerics(operand, bound2) >= 0 {
                add_var(&count_var, cvar(&const_one), &mut result_var);
            } else {
                compute_bucket(operand, bound1, bound2, &count_var, &mut result_var);
            }
        }
        1 => {
            if cmp_numerics(operand, bound1) > 0 {
                set_var_from_var(cvar(&const_zero), &mut result_var);
            } else if cmp_numerics(operand, bound2) <= 0 {
                add_var(&count_var, cvar(&const_one), &mut result_var);
            } else {
                compute_bucket(operand, bound1, bound2, &count_var, &mut result_var);
            }
        }
        _ => {}
    }

    if !numericvar_to_int32(&result_var, &mut result) {
        ereport!(ERROR, errmsg!("integer out of range"));
    }

    free_var(&mut count_var);
    free_var(&mut result_var);

    PG_RETURN_INT32!(result)
}

/// compute_bucket() - determine the correct bucket for an in-range operand.
unsafe fn compute_bucket(
    operand: Numeric,
    bound1: Numeric,
    bound2: Numeric,
    count_var: *const NumericVar,
    result_var: *mut NumericVar,
) {
    let mut bound1_var: NumericVar = core::mem::zeroed();
    let mut bound2_var: NumericVar = core::mem::zeroed();
    let mut operand_var: NumericVar = core::mem::zeroed();

    init_var_from_num(bound1, &mut bound1_var);
    init_var_from_num(bound2, &mut bound2_var);
    init_var_from_num(operand, &mut operand_var);

    sub_var(&operand_var, &bound1_var, &mut operand_var);
    sub_var(&bound2_var, &bound1_var, &mut bound2_var);

    mul_var(
        &operand_var,
        count_var,
        &mut operand_var,
        operand_var.dscale + (*count_var).dscale,
    );
    div_var(&operand_var, &bound2_var, result_var, 0, false, true);
    add_var(result_var, cvar(&const_one), result_var);

    free_var(&mut bound1_var);
    free_var(&mut bound2_var);
    free_var(&mut operand_var);
}


// ----------------------------------------------------------------------
//
// Comparison functions
//
// ----------------------------------------------------------------------

/// Sort support strategy routine.
/// TODO(pg-port): SortSupport / sortsupport.h infrastructure not yet ported.
pub unsafe fn numeric_sortsupport(_fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): full sortsupport (abbreviation) requires utils/sortsupport.h.
    unimplemented!("numeric_sortsupport: needs sortsupport infrastructure")
}

// numeric_abbrev_convert / numeric_abbrev_abort / numeric_fast_cmp /
// numeric_cmp_abbrev / numeric_abbrev_convert_var are part of sortsupport and
// depend on SortSupport; see TODO above.  numeric_abbrev_convert_var's core
// math is translated here for the 64-bit case for reference.
unsafe fn numeric_abbrev_convert_var(var: *const NumericVar, nss: *mut NumericSortSupport) -> Datum {
    let ndigits = (*var).ndigits;
    let weight = (*var).weight;
    let mut result: int64;

    if ndigits == 0 || weight < -44 {
        result = 0;
    } else if weight > 83 {
        result = PG_INT64_MAX;
    } else {
        result = (weight as int64 + 44) << 56;

        match ndigits {
            1 => {
                result |= (*(*var).digits.add(0) as int64) << 42;
            }
            2 => {
                result |= (*(*var).digits.add(1) as int64) << 28;
                result |= (*(*var).digits.add(0) as int64) << 42;
            }
            3 => {
                result |= (*(*var).digits.add(2) as int64) << 14;
                result |= (*(*var).digits.add(1) as int64) << 28;
                result |= (*(*var).digits.add(0) as int64) << 42;
            }
            _ => {
                result |= *(*var).digits.add(3) as int64;
                result |= (*(*var).digits.add(2) as int64) << 14;
                result |= (*(*var).digits.add(1) as int64) << 28;
                result |= (*(*var).digits.add(0) as int64) << 42;
            }
        }
    }

    if (*var).sign == NUMERIC_POS {
        result = -result;
    }

    if (*nss).estimating {
        let tmp: uint32 = (result as uint32) ^ ((result as uint64 >> 32) as uint32);

        addHyperLogLog(&mut (*nss).abbr_card, DatumGetUInt32(hash_uint32(tmp)));
    }

    NumericAbbrevGetDatum(result)
}

pub unsafe fn numeric_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let result: c_int;

    result = cmp_numerics(num1, num2);

    PG_RETURN_INT32!(result)
}

pub unsafe fn numeric_eq(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let result: bool;

    result = cmp_numerics(num1, num2) == 0;

    PG_RETURN_BOOL!(result)
}

pub unsafe fn numeric_ne(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let result: bool;

    result = cmp_numerics(num1, num2) != 0;

    PG_RETURN_BOOL!(result)
}

pub unsafe fn numeric_gt(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let result: bool;

    result = cmp_numerics(num1, num2) > 0;

    PG_RETURN_BOOL!(result)
}

pub unsafe fn numeric_ge(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let result: bool;

    result = cmp_numerics(num1, num2) >= 0;

    PG_RETURN_BOOL!(result)
}

pub unsafe fn numeric_lt(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let result: bool;

    result = cmp_numerics(num1, num2) < 0;

    PG_RETURN_BOOL!(result)
}

pub unsafe fn numeric_le(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let result: bool;

    result = cmp_numerics(num1, num2) <= 0;

    PG_RETURN_BOOL!(result)
}

unsafe fn cmp_numerics(num1: Numeric, num2: Numeric) -> c_int {
    let result: c_int;

    if NUMERIC_IS_SPECIAL(num1) {
        if NUMERIC_IS_NAN(num1) {
            if NUMERIC_IS_NAN(num2) {
                result = 0; /* NAN = NAN */
            } else {
                result = 1; /* NAN > non-NAN */
            }
        } else if NUMERIC_IS_PINF(num1) {
            if NUMERIC_IS_NAN(num2) {
                result = -1; /* PINF < NAN */
            } else if NUMERIC_IS_PINF(num2) {
                result = 0; /* PINF = PINF */
            } else {
                result = 1; /* PINF > anything else */
            }
        } else {
            if NUMERIC_IS_NINF(num2) {
                result = 0; /* NINF = NINF */
            } else {
                result = -1; /* NINF < anything else */
            }
        }
    } else if NUMERIC_IS_SPECIAL(num2) {
        if NUMERIC_IS_NINF(num2) {
            result = 1; /* normal > NINF */
        } else {
            result = -1; /* normal < NAN or PINF */
        }
    } else {
        result = cmp_var_common(
            NUMERIC_DIGITS(num1),
            NUMERIC_NDIGITS(num1),
            NUMERIC_WEIGHT(num1),
            NUMERIC_SIGN(num1),
            NUMERIC_DIGITS(num2),
            NUMERIC_NDIGITS(num2),
            NUMERIC_WEIGHT(num2),
            NUMERIC_SIGN(num2),
        );
    }

    result
}

/// in_range support function for numeric.
pub unsafe fn in_range_numeric_numeric(fcinfo: FunctionCallInfo) -> Datum {
    let val: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let base: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let offset: Numeric = PG_GETARG_NUMERIC!(fcinfo, 2);
    let sub: bool = PG_GETARG_BOOL!(fcinfo, 3);
    let less: bool = PG_GETARG_BOOL!(fcinfo, 4);
    let result: bool;

    if NUMERIC_IS_NAN(offset) || NUMERIC_IS_NINF(offset) || NUMERIC_SIGN(offset) == NUMERIC_NEG {
        ereport!(
            ERROR,
            errmsg!("invalid preceding or following size in window function")
        );
    }

    if NUMERIC_IS_NAN(val) {
        if NUMERIC_IS_NAN(base) {
            result = true; /* NAN = NAN */
        } else {
            result = !less; /* NAN > non-NAN */
        }
    } else if NUMERIC_IS_NAN(base) {
        result = less; /* non-NAN < NAN */
    } else if NUMERIC_IS_SPECIAL(offset) {
        Assert!(NUMERIC_IS_PINF(offset));
        if if sub { NUMERIC_IS_PINF(base) } else { NUMERIC_IS_NINF(base) } {
            result = true;
        } else if sub {
            if less {
                result = NUMERIC_IS_NINF(val);
            } else {
                result = true;
            }
        } else {
            if less {
                result = true;
            } else {
                result = NUMERIC_IS_PINF(val);
            }
        }
    } else if NUMERIC_IS_SPECIAL(val) {
        if NUMERIC_IS_PINF(val) {
            if NUMERIC_IS_PINF(base) {
                result = true; /* PINF = PINF */
            } else {
                result = !less; /* PINF > any other non-NAN */
            }
        } else {
            if NUMERIC_IS_NINF(base) {
                result = true; /* NINF = NINF */
            } else {
                result = less; /* NINF < anything else */
            }
        }
    } else if NUMERIC_IS_SPECIAL(base) {
        if NUMERIC_IS_NINF(base) {
            result = !less; /* normal > NINF */
        } else {
            result = less; /* normal < PINF */
        }
    } else {
        let mut valv: NumericVar = core::mem::zeroed();
        let mut basev: NumericVar = core::mem::zeroed();
        let mut offsetv: NumericVar = core::mem::zeroed();
        let mut sum: NumericVar = core::mem::zeroed();

        init_var_from_num(val, &mut valv);
        init_var_from_num(base, &mut basev);
        init_var_from_num(offset, &mut offsetv);
        init_var(&mut sum);

        if sub {
            sub_var(&basev, &offsetv, &mut sum);
        } else {
            add_var(&basev, &offsetv, &mut sum);
        }

        if less {
            result = cmp_var(&valv, &sum) <= 0;
        } else {
            result = cmp_var(&valv, &sum) >= 0;
        }

        free_var(&mut sum);
    }

    PG_RETURN_BOOL!(result)
}

pub unsafe fn hash_numeric(fcinfo: FunctionCallInfo) -> Datum {
    let key: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let digit_hash: Datum;
    let result: Datum;
    let mut weight: c_int;
    let mut start_offset: c_int;
    let mut end_offset: c_int;
    let mut i: c_int;
    let hash_len: c_int;
    let digits: *mut NumericDigit;

    if NUMERIC_IS_SPECIAL(key) {
        PG_RETURN_UINT32!(0);
    }

    weight = NUMERIC_WEIGHT(key);
    start_offset = 0;
    end_offset = 0;

    digits = NUMERIC_DIGITS(key);
    i = 0;
    while i < NUMERIC_NDIGITS(key) {
        if *digits.add(i as usize) != 0 {
            break;
        }
        start_offset += 1;
        weight -= 1;
        i += 1;
    }

    if NUMERIC_NDIGITS(key) == start_offset {
        PG_RETURN_UINT32!((-1i32) as uint32);
    }

    i = NUMERIC_NDIGITS(key) - 1;
    while i >= 0 {
        if *digits.add(i as usize) != 0 {
            break;
        }
        end_offset += 1;
        i -= 1;
    }

    Assert!(start_offset + end_offset < NUMERIC_NDIGITS(key));

    hash_len = NUMERIC_NDIGITS(key) - start_offset - end_offset;
    digit_hash = hash_any(
        NUMERIC_DIGITS(key).add(start_offset as usize) as *const core::ffi::c_uchar,
        hash_len * core::mem::size_of::<NumericDigit>() as c_int,
    );

    result = digit_hash ^ (weight as Datum);

    PG_RETURN_DATUM!(result)
}

/// hash_numeric_extended()
pub unsafe fn hash_numeric_extended(fcinfo: FunctionCallInfo) -> Datum {
    let key: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let seed: uint64 = PG_GETARG_INT64!(fcinfo, 1) as uint64;
    let digit_hash: Datum;
    let result: Datum;
    let mut weight: c_int;
    let mut start_offset: c_int;
    let mut end_offset: c_int;
    let mut i: c_int;
    let hash_len: c_int;
    let digits: *mut NumericDigit;

    if NUMERIC_IS_SPECIAL(key) {
        PG_RETURN_UINT64!(seed);
    }

    weight = NUMERIC_WEIGHT(key);
    start_offset = 0;
    end_offset = 0;

    digits = NUMERIC_DIGITS(key);
    i = 0;
    while i < NUMERIC_NDIGITS(key) {
        if *digits.add(i as usize) != 0 {
            break;
        }
        start_offset += 1;
        weight -= 1;
        i += 1;
    }

    if NUMERIC_NDIGITS(key) == start_offset {
        PG_RETURN_UINT64!(seed - 1);
    }

    i = NUMERIC_NDIGITS(key) - 1;
    while i >= 0 {
        if *digits.add(i as usize) != 0 {
            break;
        }
        end_offset += 1;
        i -= 1;
    }

    Assert!(start_offset + end_offset < NUMERIC_NDIGITS(key));

    hash_len = NUMERIC_NDIGITS(key) - start_offset - end_offset;
    digit_hash = hash_any_extended(
        NUMERIC_DIGITS(key).add(start_offset as usize) as *const core::ffi::c_uchar,
        hash_len * core::mem::size_of::<NumericDigit>() as c_int,
        seed,
    );

    result = UInt64GetDatum(DatumGetUInt64(digit_hash) ^ (weight as uint64));

    PG_RETURN_DATUM!(result)
}


// ----------------------------------------------------------------------
//
// Basic arithmetic functions
//
// ----------------------------------------------------------------------

/// numeric_add() - Add two numerics
pub unsafe fn numeric_add(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let res: Numeric;

    res = numeric_add_opt_error(num1, num2, null_mut());

    PG_RETURN_NUMERIC!(res)
}

/// numeric_add_opt_error()
pub unsafe fn numeric_add_opt_error(
    num1: Numeric,
    num2: Numeric,
    have_error: *mut bool,
) -> Numeric {
    let mut arg1: NumericVar = core::mem::zeroed();
    let mut arg2: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();
    let res: Numeric;

    if NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2) {
        if NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2) {
            return make_result(cvar(&const_nan));
        }
        if NUMERIC_IS_PINF(num1) {
            if NUMERIC_IS_NINF(num2) {
                return make_result(cvar(&const_nan));
            } else {
                return make_result(cvar(&const_pinf));
            }
        }
        if NUMERIC_IS_NINF(num1) {
            if NUMERIC_IS_PINF(num2) {
                return make_result(cvar(&const_nan));
            } else {
                return make_result(cvar(&const_ninf));
            }
        }
        if NUMERIC_IS_PINF(num2) {
            return make_result(cvar(&const_pinf));
        }
        Assert!(NUMERIC_IS_NINF(num2));
        return make_result(cvar(&const_ninf));
    }

    init_var_from_num(num1, &mut arg1);
    init_var_from_num(num2, &mut arg2);

    init_var(&mut result);
    add_var(&arg1, &arg2, &mut result);

    res = make_result_opt_error(&result, have_error);

    free_var(&mut result);

    res
}

/// numeric_sub() - Subtract one numeric from another
pub unsafe fn numeric_sub(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let res: Numeric;

    res = numeric_sub_opt_error(num1, num2, null_mut());

    PG_RETURN_NUMERIC!(res)
}

/// numeric_sub_opt_error()
pub unsafe fn numeric_sub_opt_error(
    num1: Numeric,
    num2: Numeric,
    have_error: *mut bool,
) -> Numeric {
    let mut arg1: NumericVar = core::mem::zeroed();
    let mut arg2: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();
    let res: Numeric;

    if NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2) {
        if NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2) {
            return make_result(cvar(&const_nan));
        }
        if NUMERIC_IS_PINF(num1) {
            if NUMERIC_IS_PINF(num2) {
                return make_result(cvar(&const_nan));
            } else {
                return make_result(cvar(&const_pinf));
            }
        }
        if NUMERIC_IS_NINF(num1) {
            if NUMERIC_IS_NINF(num2) {
                return make_result(cvar(&const_nan));
            } else {
                return make_result(cvar(&const_ninf));
            }
        }
        if NUMERIC_IS_PINF(num2) {
            return make_result(cvar(&const_ninf));
        }
        Assert!(NUMERIC_IS_NINF(num2));
        return make_result(cvar(&const_pinf));
    }

    init_var_from_num(num1, &mut arg1);
    init_var_from_num(num2, &mut arg2);

    init_var(&mut result);
    sub_var(&arg1, &arg2, &mut result);

    res = make_result_opt_error(&result, have_error);

    free_var(&mut result);

    res
}

/// numeric_mul() - Calculate the product of two numerics
pub unsafe fn numeric_mul(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let res: Numeric;

    res = numeric_mul_opt_error(num1, num2, null_mut());

    PG_RETURN_NUMERIC!(res)
}

/// numeric_mul_opt_error()
pub unsafe fn numeric_mul_opt_error(
    num1: Numeric,
    num2: Numeric,
    have_error: *mut bool,
) -> Numeric {
    let mut arg1: NumericVar = core::mem::zeroed();
    let mut arg2: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();
    let res: Numeric;

    if NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2) {
        if NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2) {
            return make_result(cvar(&const_nan));
        }
        if NUMERIC_IS_PINF(num1) {
            match numeric_sign_internal(num2) {
                0 => return make_result(cvar(&const_nan)),
                1 => return make_result(cvar(&const_pinf)),
                -1 => return make_result(cvar(&const_ninf)),
                _ => {}
            }
            Assert!(false);
        }
        if NUMERIC_IS_NINF(num1) {
            match numeric_sign_internal(num2) {
                0 => return make_result(cvar(&const_nan)),
                1 => return make_result(cvar(&const_ninf)),
                -1 => return make_result(cvar(&const_pinf)),
                _ => {}
            }
            Assert!(false);
        }
        if NUMERIC_IS_PINF(num2) {
            match numeric_sign_internal(num1) {
                0 => return make_result(cvar(&const_nan)),
                1 => return make_result(cvar(&const_pinf)),
                -1 => return make_result(cvar(&const_ninf)),
                _ => {}
            }
            Assert!(false);
        }
        Assert!(NUMERIC_IS_NINF(num2));
        match numeric_sign_internal(num1) {
            0 => return make_result(cvar(&const_nan)),
            1 => return make_result(cvar(&const_ninf)),
            -1 => return make_result(cvar(&const_pinf)),
            _ => {}
        }
        Assert!(false);
    }

    init_var_from_num(num1, &mut arg1);
    init_var_from_num(num2, &mut arg2);

    init_var(&mut result);
    mul_var(&arg1, &arg2, &mut result, arg1.dscale + arg2.dscale);

    if result.dscale > NUMERIC_DSCALE_MAX {
        round_var(&mut result, NUMERIC_DSCALE_MAX);
    }

    res = make_result_opt_error(&result, have_error);

    free_var(&mut result);

    res
}

/// numeric_div() - Divide one numeric into another
pub unsafe fn numeric_div(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let res: Numeric;

    res = numeric_div_opt_error(num1, num2, null_mut());

    PG_RETURN_NUMERIC!(res)
}

/// numeric_div_opt_error()
pub unsafe fn numeric_div_opt_error(
    num1: Numeric,
    num2: Numeric,
    have_error: *mut bool,
) -> Numeric {
    let mut arg1: NumericVar = core::mem::zeroed();
    let mut arg2: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();
    let res: Numeric;
    let rscale: c_int;

    if !have_error.is_null() {
        *have_error = false;
    }

    if NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2) {
        if NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2) {
            return make_result(cvar(&const_nan));
        }
        if NUMERIC_IS_PINF(num1) {
            if NUMERIC_IS_SPECIAL(num2) {
                return make_result(cvar(&const_nan));
            }
            match numeric_sign_internal(num2) {
                0 => {
                    if !have_error.is_null() {
                        *have_error = true;
                        return null_mut();
                    }
                    ereport!(ERROR, errmsg!("division by zero"));
                }
                1 => return make_result(cvar(&const_pinf)),
                -1 => return make_result(cvar(&const_ninf)),
                _ => {}
            }
            Assert!(false);
        }
        if NUMERIC_IS_NINF(num1) {
            if NUMERIC_IS_SPECIAL(num2) {
                return make_result(cvar(&const_nan));
            }
            match numeric_sign_internal(num2) {
                0 => {
                    if !have_error.is_null() {
                        *have_error = true;
                        return null_mut();
                    }
                    ereport!(ERROR, errmsg!("division by zero"));
                }
                1 => return make_result(cvar(&const_ninf)),
                -1 => return make_result(cvar(&const_pinf)),
                _ => {}
            }
            Assert!(false);
        }
        return make_result(cvar(&const_zero));
    }

    init_var_from_num(num1, &mut arg1);
    init_var_from_num(num2, &mut arg2);

    init_var(&mut result);

    rscale = select_div_scale(&arg1, &arg2);

    if !have_error.is_null() && (arg2.ndigits == 0 || *arg2.digits.add(0) == 0) {
        *have_error = true;
        return null_mut();
    }

    div_var(&arg1, &arg2, &mut result, rscale, true, true);

    res = make_result_opt_error(&result, have_error);

    free_var(&mut result);

    res
}

/// numeric_div_trunc() - Divide, truncating the result to an integer
pub unsafe fn numeric_div_trunc(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let mut arg1: NumericVar = core::mem::zeroed();
    let mut arg2: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();
    let res: Numeric;

    if NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2) {
        if NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2) {
            PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
        }
        if NUMERIC_IS_PINF(num1) {
            if NUMERIC_IS_SPECIAL(num2) {
                PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
            }
            match numeric_sign_internal(num2) {
                0 => {
                    ereport!(ERROR, errmsg!("division by zero"));
                }
                1 => PG_RETURN_NUMERIC!(make_result(cvar(&const_pinf))),
                -1 => PG_RETURN_NUMERIC!(make_result(cvar(&const_ninf))),
                _ => {}
            }
            Assert!(false);
        }
        if NUMERIC_IS_NINF(num1) {
            if NUMERIC_IS_SPECIAL(num2) {
                PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
            }
            match numeric_sign_internal(num2) {
                0 => {
                    ereport!(ERROR, errmsg!("division by zero"));
                }
                1 => PG_RETURN_NUMERIC!(make_result(cvar(&const_ninf))),
                -1 => PG_RETURN_NUMERIC!(make_result(cvar(&const_pinf))),
                _ => {}
            }
            Assert!(false);
        }
        PG_RETURN_NUMERIC!(make_result(cvar(&const_zero)));
    }

    init_var_from_num(num1, &mut arg1);
    init_var_from_num(num2, &mut arg2);

    init_var(&mut result);

    div_var(&arg1, &arg2, &mut result, 0, false, true);

    res = make_result(&result);

    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

/// numeric_mod() - Calculate the modulo of two numerics
pub unsafe fn numeric_mod(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let res: Numeric;

    res = numeric_mod_opt_error(num1, num2, null_mut());

    PG_RETURN_NUMERIC!(res)
}

/// numeric_mod_opt_error()
pub unsafe fn numeric_mod_opt_error(
    num1: Numeric,
    num2: Numeric,
    have_error: *mut bool,
) -> Numeric {
    let res: Numeric;
    let mut arg1: NumericVar = core::mem::zeroed();
    let mut arg2: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();

    if !have_error.is_null() {
        *have_error = false;
    }

    if NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2) {
        if NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2) {
            return make_result(cvar(&const_nan));
        }
        if NUMERIC_IS_INF(num1) {
            if numeric_sign_internal(num2) == 0 {
                if !have_error.is_null() {
                    *have_error = true;
                    return null_mut();
                }
                ereport!(ERROR, errmsg!("division by zero"));
            }
            return make_result(cvar(&const_nan));
        }
        return duplicate_numeric(num1);
    }

    init_var_from_num(num1, &mut arg1);
    init_var_from_num(num2, &mut arg2);

    init_var(&mut result);

    if !have_error.is_null() && (arg2.ndigits == 0 || *arg2.digits.add(0) == 0) {
        *have_error = true;
        return null_mut();
    }

    mod_var(&arg1, &arg2, &mut result);

    res = make_result_opt_error(&result, null_mut());

    free_var(&mut result);

    res
}

/// numeric_inc() - Increment a number by one
pub unsafe fn numeric_inc(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let mut arg: NumericVar = core::mem::zeroed();
    let res: Numeric;

    if NUMERIC_IS_SPECIAL(num) {
        PG_RETURN_NUMERIC!(duplicate_numeric(num));
    }

    init_var_from_num(num, &mut arg);

    add_var(&arg, cvar(&const_one), &mut arg);

    res = make_result(&arg);

    free_var(&mut arg);

    PG_RETURN_NUMERIC!(res)
}

/// numeric_smaller() - Return the smaller of two numbers
pub unsafe fn numeric_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);

    if cmp_numerics(num1, num2) < 0 {
        PG_RETURN_NUMERIC!(num1);
    } else {
        PG_RETURN_NUMERIC!(num2);
    }
}

/// numeric_larger() - Return the larger of two numbers
pub unsafe fn numeric_larger(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);

    if cmp_numerics(num1, num2) > 0 {
        PG_RETURN_NUMERIC!(num1);
    } else {
        PG_RETURN_NUMERIC!(num2);
    }
}


// ----------------------------------------------------------------------
//
// Advanced math functions
//
// ----------------------------------------------------------------------

/// numeric_gcd() - greatest common divisor of two numerics
pub unsafe fn numeric_gcd(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let mut arg1: NumericVar = core::mem::zeroed();
    let mut arg2: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();
    let res: Numeric;

    if NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2) {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
    }

    init_var_from_num(num1, &mut arg1);
    init_var_from_num(num2, &mut arg2);

    init_var(&mut result);

    gcd_var(&arg1, &arg2, &mut result);

    res = make_result(&result);

    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

/// numeric_lcm() - least common multiple of two numerics
pub unsafe fn numeric_lcm(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let mut arg1: NumericVar = core::mem::zeroed();
    let mut arg2: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();
    let res: Numeric;

    if NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2) {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
    }

    init_var_from_num(num1, &mut arg1);
    init_var_from_num(num2, &mut arg2);

    init_var(&mut result);

    if arg1.ndigits == 0 || arg2.ndigits == 0 {
        set_var_from_var(cvar(&const_zero), &mut result);
    } else {
        gcd_var(&arg1, &arg2, &mut result);
        div_var(&arg1, &result, &mut result, 0, false, true);
        mul_var(&arg2, &result, &mut result, arg2.dscale);
        result.sign = NUMERIC_POS;
    }

    result.dscale = Max!(arg1.dscale, arg2.dscale);

    res = make_result(&result);

    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

/// numeric_fac() - Compute factorial
pub unsafe fn numeric_fac(fcinfo: FunctionCallInfo) -> Datum {
    let mut num: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let res: Numeric;
    let mut fact: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();

    if num < 0 {
        ereport!(ERROR, errmsg!("factorial of a negative number is undefined"));
    }
    if num <= 1 {
        res = make_result(cvar(&const_one));
        PG_RETURN_NUMERIC!(res);
    }
    if num > 32177 {
        ereport!(ERROR, errmsg!("value overflows numeric format"));
    }

    init_var(&mut fact);
    init_var(&mut result);

    int64_to_numericvar(num, &mut result);

    num -= 1;
    while num > 1 {
        CHECK_FOR_INTERRUPTS();

        int64_to_numericvar(num, &mut fact);

        mul_var(&result, &fact, &mut result, 0);
        num -= 1;
    }

    res = make_result(&result);

    free_var(&mut fact);
    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

/// numeric_sqrt() - Compute the square root of a numeric.
pub unsafe fn numeric_sqrt(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let res: Numeric;
    let mut arg: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();
    let sweight: c_int;
    let mut rscale: c_int;

    if NUMERIC_IS_SPECIAL(num) {
        if NUMERIC_IS_NINF(num) {
            ereport!(ERROR, errmsg!("cannot take square root of a negative number"));
        }
        PG_RETURN_NUMERIC!(duplicate_numeric(num));
    }

    init_var_from_num(num, &mut arg);

    init_var(&mut result);

    /* DEC_DIGITS is even, so the division is exact */
    sweight = arg.weight * DEC_DIGITS / 2 + 1;

    rscale = NUMERIC_MIN_SIG_DIGITS - sweight;
    rscale = Max!(rscale, arg.dscale);
    rscale = Max!(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min!(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    sqrt_var(&arg, &mut result, rscale);

    res = make_result(&result);

    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

/// numeric_exp() - Raise e to the power of x
pub unsafe fn numeric_exp(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let res: Numeric;
    let mut arg: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();
    let mut rscale: c_int;
    let mut val: f64;

    if NUMERIC_IS_SPECIAL(num) {
        if NUMERIC_IS_NINF(num) {
            PG_RETURN_NUMERIC!(make_result(cvar(&const_zero)));
        }
        PG_RETURN_NUMERIC!(duplicate_numeric(num));
    }

    init_var_from_num(num, &mut arg);

    init_var(&mut result);

    val = numericvar_to_double_no_overflow(&arg);

    val *= 0.434294481903252;

    val = Max!(val, -NUMERIC_MAX_RESULT_SCALE as f64);
    val = Min!(val, NUMERIC_MAX_RESULT_SCALE as f64);

    rscale = NUMERIC_MIN_SIG_DIGITS - val as c_int;
    rscale = Max!(rscale, arg.dscale);
    rscale = Max!(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min!(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    exp_var(&arg, &mut result, rscale);

    res = make_result(&result);

    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

/// numeric_ln() - Compute the natural logarithm of x
pub unsafe fn numeric_ln(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let res: Numeric;
    let mut arg: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();
    let ln_dweight: c_int;
    let mut rscale: c_int;

    if NUMERIC_IS_SPECIAL(num) {
        if NUMERIC_IS_NINF(num) {
            ereport!(ERROR, errmsg!("cannot take logarithm of a negative number"));
        }
        PG_RETURN_NUMERIC!(duplicate_numeric(num));
    }

    init_var_from_num(num, &mut arg);
    init_var(&mut result);

    ln_dweight = estimate_ln_dweight(&arg);

    rscale = NUMERIC_MIN_SIG_DIGITS - ln_dweight;
    rscale = Max!(rscale, arg.dscale);
    rscale = Max!(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min!(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    ln_var(&arg, &mut result, rscale);

    res = make_result(&result);

    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

/// numeric_log() - Compute the logarithm of x in a given base
pub unsafe fn numeric_log(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let res: Numeric;
    let mut arg1: NumericVar = core::mem::zeroed();
    let mut arg2: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();

    if NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2) {
        let sign1: c_int;
        let sign2: c_int;

        if NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2) {
            PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
        }
        sign1 = numeric_sign_internal(num1);
        sign2 = numeric_sign_internal(num2);
        if sign1 < 0 || sign2 < 0 {
            ereport!(ERROR, errmsg!("cannot take logarithm of a negative number"));
        }
        if sign1 == 0 || sign2 == 0 {
            ereport!(ERROR, errmsg!("cannot take logarithm of zero"));
        }
        if NUMERIC_IS_PINF(num1) {
            if NUMERIC_IS_PINF(num2) {
                PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
            }
            PG_RETURN_NUMERIC!(make_result(cvar(&const_zero)));
        }
        Assert!(NUMERIC_IS_PINF(num2));
        PG_RETURN_NUMERIC!(make_result(cvar(&const_pinf)));
    }

    init_var_from_num(num1, &mut arg1);
    init_var_from_num(num2, &mut arg2);
    init_var(&mut result);

    log_var(&arg1, &arg2, &mut result);

    res = make_result(&result);

    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

/// numeric_power() - Raise x to the power of y
pub unsafe fn numeric_power(fcinfo: FunctionCallInfo) -> Datum {
    let num1: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let num2: Numeric = PG_GETARG_NUMERIC!(fcinfo, 1);
    let res: Numeric;
    let mut arg1: NumericVar = core::mem::zeroed();
    let mut arg2: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();
    let sign1: c_int;
    let sign2: c_int;

    if NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2) {
        if NUMERIC_IS_NAN(num1) {
            if !NUMERIC_IS_SPECIAL(num2) {
                init_var_from_num(num2, &mut arg2);
                if cmp_var(&arg2, cvar(&const_zero)) == 0 {
                    PG_RETURN_NUMERIC!(make_result(cvar(&const_one)));
                }
            }
            PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
        }
        if NUMERIC_IS_NAN(num2) {
            if !NUMERIC_IS_SPECIAL(num1) {
                init_var_from_num(num1, &mut arg1);
                if cmp_var(&arg1, cvar(&const_one)) == 0 {
                    PG_RETURN_NUMERIC!(make_result(cvar(&const_one)));
                }
            }
            PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
        }
        let s1 = numeric_sign_internal(num1);
        let s2 = numeric_sign_internal(num2);
        if s1 == 0 && s2 < 0 {
            ereport!(ERROR, errmsg!("zero raised to a negative power is undefined"));
        }
        if s1 < 0 && !numeric_is_integral(num2) {
            ereport!(
                ERROR,
                errmsg!("a negative number raised to a non-integer power yields a complex result")
            );
        }

        if !NUMERIC_IS_SPECIAL(num1) {
            init_var_from_num(num1, &mut arg1);
            if cmp_var(&arg1, cvar(&const_one)) == 0 {
                PG_RETURN_NUMERIC!(make_result(cvar(&const_one)));
            }
        }

        if s2 == 0 {
            PG_RETURN_NUMERIC!(make_result(cvar(&const_one)));
        }

        if s1 == 0 && s2 > 0 {
            PG_RETURN_NUMERIC!(make_result(cvar(&const_zero)));
        }

        if NUMERIC_IS_INF(num2) {
            let abs_x_gt_one: bool;

            if NUMERIC_IS_SPECIAL(num1) {
                abs_x_gt_one = true;
            } else {
                init_var_from_num(num1, &mut arg1);
                if cmp_var(&arg1, cvar(&const_minus_one)) == 0 {
                    PG_RETURN_NUMERIC!(make_result(cvar(&const_one)));
                }
                arg1.sign = NUMERIC_POS;
                abs_x_gt_one = cmp_var(&arg1, cvar(&const_one)) > 0;
            }
            if abs_x_gt_one == (s2 > 0) {
                PG_RETURN_NUMERIC!(make_result(cvar(&const_pinf)));
            } else {
                PG_RETURN_NUMERIC!(make_result(cvar(&const_zero)));
            }
        }

        if NUMERIC_IS_PINF(num1) {
            if s2 > 0 {
                PG_RETURN_NUMERIC!(make_result(cvar(&const_pinf)));
            } else {
                PG_RETURN_NUMERIC!(make_result(cvar(&const_zero)));
            }
        }

        Assert!(NUMERIC_IS_NINF(num1));

        if s2 < 0 {
            PG_RETURN_NUMERIC!(make_result(cvar(&const_zero)));
        }

        init_var_from_num(num2, &mut arg2);
        if arg2.ndigits > 0
            && arg2.ndigits == arg2.weight + 1
            && (*arg2.digits.add((arg2.ndigits - 1) as usize) & 1) != 0
        {
            PG_RETURN_NUMERIC!(make_result(cvar(&const_ninf)));
        } else {
            PG_RETURN_NUMERIC!(make_result(cvar(&const_pinf)));
        }
    }

    sign1 = numeric_sign_internal(num1);
    sign2 = numeric_sign_internal(num2);

    if sign1 == 0 && sign2 < 0 {
        ereport!(ERROR, errmsg!("zero raised to a negative power is undefined"));
    }

    init_var(&mut result);
    init_var_from_num(num1, &mut arg1);
    init_var_from_num(num2, &mut arg2);

    power_var(&arg1, &arg2, &mut result);

    res = make_result(&result);

    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

/// numeric_scale() - count of decimal digits in the fractional part
pub unsafe fn numeric_scale(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);

    if NUMERIC_IS_SPECIAL(num) {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_INT32!(NUMERIC_DSCALE(num))
}

/// get_min_scale() - minimum scale for value.
unsafe fn get_min_scale(var: *mut NumericVar) -> c_int {
    let mut min_scale: c_int;
    let mut last_digit_pos: c_int;

    last_digit_pos = (*var).ndigits - 1;
    while last_digit_pos >= 0 && *(*var).digits.add(last_digit_pos as usize) == 0 {
        last_digit_pos -= 1;
    }

    if last_digit_pos >= 0 {
        min_scale = (last_digit_pos - (*var).weight) * DEC_DIGITS;

        if min_scale > 0 {
            let mut last_digit: NumericDigit = *(*var).digits.add(last_digit_pos as usize);

            while last_digit % 10 == 0 {
                min_scale -= 1;
                last_digit /= 10;
            }
        } else {
            min_scale = 0;
        }
    } else {
        min_scale = 0;
    }

    min_scale
}

/// numeric_min_scale()
pub unsafe fn numeric_min_scale(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let mut arg: NumericVar = core::mem::zeroed();
    let min_scale: c_int;

    if NUMERIC_IS_SPECIAL(num) {
        PG_RETURN_NULL!(fcinfo);
    }

    init_var_from_num(num, &mut arg);
    min_scale = get_min_scale(&mut arg);
    free_var(&mut arg);

    PG_RETURN_INT32!(min_scale)
}

/// numeric_trim_scale()
pub unsafe fn numeric_trim_scale(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let res: Numeric;
    let mut result: NumericVar = core::mem::zeroed();

    if NUMERIC_IS_SPECIAL(num) {
        PG_RETURN_NUMERIC!(duplicate_numeric(num));
    }

    init_var_from_num(num, &mut result);
    result.dscale = get_min_scale(&mut result);
    res = make_result(&result);
    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

/// random_numeric() - return a random numeric value in [rmin, rmax].
pub unsafe fn random_numeric(state: *mut pg_prng_state, rmin: Numeric, rmax: Numeric) -> Numeric {
    let mut rmin_var: NumericVar = core::mem::zeroed();
    let mut rmax_var: NumericVar = core::mem::zeroed();
    let mut result: NumericVar = core::mem::zeroed();
    let res: Numeric;

    if NUMERIC_IS_SPECIAL(rmin) {
        if NUMERIC_IS_NAN(rmin) {
            ereport!(ERROR, errmsg!("lower bound cannot be NaN"));
        } else {
            ereport!(ERROR, errmsg!("lower bound cannot be infinity"));
        }
    }
    if NUMERIC_IS_SPECIAL(rmax) {
        if NUMERIC_IS_NAN(rmax) {
            ereport!(ERROR, errmsg!("upper bound cannot be NaN"));
        } else {
            ereport!(ERROR, errmsg!("upper bound cannot be infinity"));
        }
    }

    init_var_from_num(rmin, &mut rmin_var);
    init_var_from_num(rmax, &mut rmax_var);

    init_var(&mut result);

    random_var(state, &rmin_var, &rmax_var, &mut result);

    res = make_result(&result);

    free_var(&mut result);

    res
}


// ----------------------------------------------------------------------
//
// Type conversion functions
//
// ----------------------------------------------------------------------

pub unsafe fn int64_to_numeric(val: int64) -> Numeric {
    let res: Numeric;
    let mut result: NumericVar = core::mem::zeroed();

    init_var(&mut result);

    int64_to_numericvar(val, &mut result);

    res = make_result(&result);

    free_var(&mut result);

    res
}

/// Convert val1/(10**log10val2) to numeric.
pub unsafe fn int64_div_fast_to_numeric(val1: int64, log10val2: c_int) -> Numeric {
    let res: Numeric;
    let mut result: NumericVar = core::mem::zeroed();
    let rscale: c_int;
    let mut w: c_int;
    let mut m: c_int;

    init_var(&mut result);

    rscale = if log10val2 < 0 { 0 } else { log10val2 };

    w = log10val2 / DEC_DIGITS;
    m = log10val2 % DEC_DIGITS;
    if m < 0 {
        m += DEC_DIGITS;
        w -= 1;
    }

    if m > 0 {
        static pow10: [int64; 4] = [1, 10, 100, 1000];

        let factor: int64 = pow10[(DEC_DIGITS - m) as usize];
        let mut new_val1: int64 = 0;

        if pg_mul_s64_overflow(val1, factor, &mut new_val1) {
            /* do the multiplication using 128-bit integers */
            let tmp: int128 = val1 as int128 * factor as int128;

            int128_to_numericvar(tmp, &mut result);
        } else {
            int64_to_numericvar(new_val1, &mut result);
        }

        w += 1;
    } else {
        int64_to_numericvar(val1, &mut result);
    }

    result.weight -= w;
    result.dscale = rscale;

    res = make_result(&result);

    free_var(&mut result);

    res
}

pub unsafe fn int4_numeric(fcinfo: FunctionCallInfo) -> Datum {
    let val: int32 = PG_GETARG_INT32!(fcinfo, 0);

    PG_RETURN_NUMERIC!(int64_to_numeric(val as int64))
}

pub unsafe fn numeric_int4_opt_error(num: Numeric, have_error: *mut bool) -> int32 {
    let mut x: NumericVar = core::mem::zeroed();
    let mut result: int32 = 0;

    if !have_error.is_null() {
        *have_error = false;
    }

    if NUMERIC_IS_SPECIAL(num) {
        if !have_error.is_null() {
            *have_error = true;
            return 0;
        } else {
            if NUMERIC_IS_NAN(num) {
                ereport!(ERROR, errmsg!("cannot convert NaN to {}", "integer"));
            } else {
                ereport!(ERROR, errmsg!("cannot convert infinity to {}", "integer"));
            }
        }
    }

    init_var_from_num(num, &mut x);

    if !numericvar_to_int32(&x, &mut result) {
        if !have_error.is_null() {
            *have_error = true;
            return 0;
        } else {
            ereport!(ERROR, errmsg!("integer out of range"));
        }
    }

    result
}

pub unsafe fn numeric_int4(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);

    PG_RETURN_INT32!(numeric_int4_opt_error(num, null_mut()))
}

/// Given a NumericVar, convert it to an int32.
unsafe fn numericvar_to_int32(var: *const NumericVar, result: *mut int32) -> bool {
    let mut val: int64 = 0;

    if !numericvar_to_int64(var, &mut val) {
        return false;
    }

    if val < PG_INT32_MIN as int64 || val > PG_INT32_MAX as int64 {
        return false;
    }

    *result = val as int32;

    true
}

pub unsafe fn int8_numeric(fcinfo: FunctionCallInfo) -> Datum {
    let val: int64 = PG_GETARG_INT64!(fcinfo, 0);

    PG_RETURN_NUMERIC!(int64_to_numeric(val))
}

pub unsafe fn numeric_int8_opt_error(num: Numeric, have_error: *mut bool) -> int64 {
    let mut x: NumericVar = core::mem::zeroed();
    let mut result: int64 = 0;

    if !have_error.is_null() {
        *have_error = false;
    }

    if NUMERIC_IS_SPECIAL(num) {
        if !have_error.is_null() {
            *have_error = true;
            return 0;
        } else {
            if NUMERIC_IS_NAN(num) {
                ereport!(ERROR, errmsg!("cannot convert NaN to {}", "bigint"));
            } else {
                ereport!(ERROR, errmsg!("cannot convert infinity to {}", "bigint"));
            }
        }
    }

    init_var_from_num(num, &mut x);

    if !numericvar_to_int64(&x, &mut result) {
        if !have_error.is_null() {
            *have_error = true;
            return 0;
        } else {
            ereport!(ERROR, errmsg!("bigint out of range"));
        }
    }

    result
}

pub unsafe fn numeric_int8(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);

    PG_RETURN_INT64!(numeric_int8_opt_error(num, null_mut()))
}

pub unsafe fn int2_numeric(fcinfo: FunctionCallInfo) -> Datum {
    let val: int16 = PG_GETARG_INT16!(fcinfo, 0);

    PG_RETURN_NUMERIC!(int64_to_numeric(val as int64))
}

pub unsafe fn numeric_int2(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let mut x: NumericVar = core::mem::zeroed();
    let mut val: int64 = 0;
    let result: int16;

    if NUMERIC_IS_SPECIAL(num) {
        if NUMERIC_IS_NAN(num) {
            ereport!(ERROR, errmsg!("cannot convert NaN to {}", "smallint"));
        } else {
            ereport!(ERROR, errmsg!("cannot convert infinity to {}", "smallint"));
        }
    }

    init_var_from_num(num, &mut x);

    if !numericvar_to_int64(&x, &mut val) {
        ereport!(ERROR, errmsg!("smallint out of range"));
    }

    if val < PG_INT16_MIN as int64 || val > PG_INT16_MAX as int64 {
        ereport!(ERROR, errmsg!("smallint out of range"));
    }

    result = val as int16;

    PG_RETURN_INT16!(result)
}

pub unsafe fn float8_numeric(fcinfo: FunctionCallInfo) -> Datum {
    let val: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let res: Numeric;
    let mut result: NumericVar = core::mem::zeroed();
    let mut buf: [c_char; DBL_DIG + 100] = [0; DBL_DIG + 100];
    let mut endptr: *const c_char = null();

    if val.is_nan() {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
    }

    if val.is_infinite() {
        if val < 0.0 {
            PG_RETURN_NUMERIC!(make_result(cvar(&const_ninf)));
        } else {
            PG_RETURN_NUMERIC!(make_result(cvar(&const_pinf)));
        }
    }

    snprintf(
        buf.as_mut_ptr(),
        buf.len(),
        c"%.*g".as_ptr(),
        DBL_DIG as c_int,
        val,
    );

    init_var(&mut result);

    set_var_from_str(buf.as_ptr(), buf.as_ptr(), &mut result, &mut endptr, null_mut());

    res = make_result(&result);

    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

pub unsafe fn numeric_float8(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let tmp: *mut c_char;
    let result: Datum;

    if NUMERIC_IS_SPECIAL(num) {
        if NUMERIC_IS_PINF(num) {
            PG_RETURN_FLOAT8!(get_float8_infinity());
        } else if NUMERIC_IS_NINF(num) {
            PG_RETURN_FLOAT8!(-get_float8_infinity());
        } else {
            PG_RETURN_FLOAT8!(get_float8_nan());
        }
    }

    tmp = DatumGetCString(DirectFunctionCall1!(numeric_out, NumericGetDatum(num)));

    result = DirectFunctionCall1!(crate::utils::adt::float::float8in, CStringGetDatum(tmp));

    pfree(tmp as *mut c_void);

    PG_RETURN_DATUM!(result)
}

/// Convert numeric to float8; if out of range, return +/- HUGE_VAL
pub unsafe fn numeric_float8_no_overflow(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let val: f64;

    if NUMERIC_IS_SPECIAL(num) {
        if NUMERIC_IS_PINF(num) {
            val = HUGE_VAL;
        } else if NUMERIC_IS_NINF(num) {
            val = -HUGE_VAL;
        } else {
            val = get_float8_nan();
        }
    } else {
        let mut x: NumericVar = core::mem::zeroed();

        init_var_from_num(num, &mut x);
        val = numericvar_to_double_no_overflow(&x);
    }

    PG_RETURN_FLOAT8!(val)
}

pub unsafe fn float4_numeric(fcinfo: FunctionCallInfo) -> Datum {
    let val: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let res: Numeric;
    let mut result: NumericVar = core::mem::zeroed();
    let mut buf: [c_char; FLT_DIG + 100] = [0; FLT_DIG + 100];
    let mut endptr: *const c_char = null();

    if val.is_nan() {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
    }

    if val.is_infinite() {
        if val < 0.0 {
            PG_RETURN_NUMERIC!(make_result(cvar(&const_ninf)));
        } else {
            PG_RETURN_NUMERIC!(make_result(cvar(&const_pinf)));
        }
    }

    snprintf(
        buf.as_mut_ptr(),
        buf.len(),
        c"%.*g".as_ptr(),
        FLT_DIG as c_int,
        val as f64,
    );

    init_var(&mut result);

    set_var_from_str(buf.as_ptr(), buf.as_ptr(), &mut result, &mut endptr, null_mut());

    res = make_result(&result);

    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

pub unsafe fn numeric_float4(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let tmp: *mut c_char;
    let result: Datum;

    if NUMERIC_IS_SPECIAL(num) {
        if NUMERIC_IS_PINF(num) {
            PG_RETURN_FLOAT4!(get_float4_infinity());
        } else if NUMERIC_IS_NINF(num) {
            PG_RETURN_FLOAT4!(-get_float4_infinity());
        } else {
            PG_RETURN_FLOAT4!(get_float4_nan());
        }
    }

    tmp = DatumGetCString(DirectFunctionCall1!(numeric_out, NumericGetDatum(num)));

    result = DirectFunctionCall1!(crate::utils::adt::float::float4in, CStringGetDatum(tmp));

    pfree(tmp as *mut c_void);

    PG_RETURN_DATUM!(result)
}

pub unsafe fn numeric_pg_lsn(fcinfo: FunctionCallInfo) -> Datum {
    let num: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let mut x: NumericVar = core::mem::zeroed();
    let mut result: XLogRecPtr = 0;

    if NUMERIC_IS_SPECIAL(num) {
        if NUMERIC_IS_NAN(num) {
            ereport!(ERROR, errmsg!("cannot convert NaN to {}", "pg_lsn"));
        } else {
            ereport!(ERROR, errmsg!("cannot convert infinity to {}", "pg_lsn"));
        }
    }

    init_var_from_num(num, &mut x);

    if !numericvar_to_uint64(&x, &mut result as *mut XLogRecPtr as *mut uint64) {
        ereport!(ERROR, errmsg!("pg_lsn out of range"));
    }

    // PG_RETURN_LSN
    return result as usize as Datum;
}


// ----------------------------------------------------------------------
//
// Aggregate functions
//
// ----------------------------------------------------------------------

#[repr(C)]
pub struct NumericAggState {
    calcSumX2: bool,            // if true, calculate sumX2
    agg_context: MemoryContext, // context we're calculating in
    N: int64,                   // count of processed numbers
    sumX: NumericSumAccum,      // sum of processed numbers
    sumX2: NumericSumAccum,     // sum of squares of processed numbers
    maxScale: c_int,            // maximum scale seen so far
    maxScaleCount: int64,       // number of values seen with maximum scale
    NaNcount: int64,            // count of NaN values
    pInfcount: int64,           // count of +Inf values
    nInfcount: int64,           // count of -Inf values
}

#[inline]
unsafe fn NA_TOTAL_COUNT(na: *const NumericAggState) -> int64 {
    (*na).N + (*na).NaNcount + (*na).pInfcount + (*na).nInfcount
}

unsafe fn makeNumericAggState(fcinfo: FunctionCallInfo, calcSumX2: bool) -> *mut NumericAggState {
    let state: *mut NumericAggState;
    let mut agg_context: MemoryContext = null_mut();
    let old_context: MemoryContext;

    if AggCheckCallContext(fcinfo, &mut agg_context) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    old_context = MemoryContextSwitchTo(agg_context);

    state = palloc0(core::mem::size_of::<NumericAggState>()) as *mut NumericAggState;
    (*state).calcSumX2 = calcSumX2;
    (*state).agg_context = agg_context;

    MemoryContextSwitchTo(old_context);

    state
}

unsafe fn makeNumericAggStateCurrentContext(calcSumX2: bool) -> *mut NumericAggState {
    let state: *mut NumericAggState;

    state = palloc0(core::mem::size_of::<NumericAggState>()) as *mut NumericAggState;
    (*state).calcSumX2 = calcSumX2;
    (*state).agg_context = CurrentMemoryContext;

    state
}

/// Accumulate a new input value for numeric aggregate functions.
unsafe fn do_numeric_accum(state: *mut NumericAggState, newval: Numeric) {
    let mut x: NumericVar = core::mem::zeroed();
    let mut x2: NumericVar = core::mem::zeroed();
    let old_context: MemoryContext;

    if NUMERIC_IS_SPECIAL(newval) {
        if NUMERIC_IS_PINF(newval) {
            (*state).pInfcount += 1;
        } else if NUMERIC_IS_NINF(newval) {
            (*state).nInfcount += 1;
        } else {
            (*state).NaNcount += 1;
        }
        return;
    }

    init_var_from_num(newval, &mut x);

    if x.dscale > (*state).maxScale {
        (*state).maxScale = x.dscale;
        (*state).maxScaleCount = 1;
    } else if x.dscale == (*state).maxScale {
        (*state).maxScaleCount += 1;
    }

    if (*state).calcSumX2 {
        init_var(&mut x2);
        mul_var(&x, &x, &mut x2, x.dscale * 2);
    }

    old_context = MemoryContextSwitchTo((*state).agg_context);

    (*state).N += 1;

    accum_sum_add(&mut (*state).sumX, &x);

    if (*state).calcSumX2 {
        accum_sum_add(&mut (*state).sumX2, &x2);
    }

    MemoryContextSwitchTo(old_context);
}

/// Attempt to remove an input value from the aggregated state.
unsafe fn do_numeric_discard(state: *mut NumericAggState, newval: Numeric) -> bool {
    let mut x: NumericVar = core::mem::zeroed();
    let mut x2: NumericVar = core::mem::zeroed();
    let old_context: MemoryContext;

    if NUMERIC_IS_SPECIAL(newval) {
        if NUMERIC_IS_PINF(newval) {
            (*state).pInfcount -= 1;
        } else if NUMERIC_IS_NINF(newval) {
            (*state).nInfcount -= 1;
        } else {
            (*state).NaNcount -= 1;
        }
        return true;
    }

    init_var_from_num(newval, &mut x);

    if x.dscale == (*state).maxScale {
        if (*state).maxScaleCount > 1 || (*state).maxScale == 0 {
            (*state).maxScaleCount -= 1;
        } else if (*state).N == 1 {
            (*state).maxScale = 0;
            (*state).maxScaleCount = 0;
        } else {
            return false;
        }
    }

    if (*state).calcSumX2 {
        init_var(&mut x2);
        mul_var(&x, &x, &mut x2, x.dscale * 2);
    }

    old_context = MemoryContextSwitchTo((*state).agg_context);

    let old_n = (*state).N;
    (*state).N -= 1;
    if old_n > 1 {
        x.sign = if x.sign == NUMERIC_POS { NUMERIC_NEG } else { NUMERIC_POS };
        accum_sum_add(&mut (*state).sumX, &x);

        if (*state).calcSumX2 {
            x2.sign = NUMERIC_NEG;
            accum_sum_add(&mut (*state).sumX2, &x2);
        }
    } else {
        Assert!((*state).N == 0);

        accum_sum_reset(&mut (*state).sumX);
        if (*state).calcSumX2 {
            accum_sum_reset(&mut (*state).sumX2);
        }
    }

    MemoryContextSwitchTo(old_context);

    true
}

pub unsafe fn numeric_accum(fcinfo: FunctionCallInfo) -> Datum {
    let mut state: *mut NumericAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };

    if state.is_null() {
        state = makeNumericAggState(fcinfo, true);
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        do_numeric_accum(state, PG_GETARG_NUMERIC!(fcinfo, 1));
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}

pub unsafe fn numeric_combine(fcinfo: FunctionCallInfo) -> Datum {
    let mut state1: *mut NumericAggState;
    let state2: *mut NumericAggState;
    let mut agg_context: MemoryContext = null_mut();
    let old_context: MemoryContext;

    if AggCheckCallContext(fcinfo, &mut agg_context) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state1 = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };
    state2 = if PG_ARGISNULL!(fcinfo, 1) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 1) as *mut NumericAggState
    };

    if state2.is_null() {
        PG_RETURN_POINTER!(state1 as *mut c_void);
    }

    if state1.is_null() {
        old_context = MemoryContextSwitchTo(agg_context);

        state1 = makeNumericAggStateCurrentContext(true);
        (*state1).N = (*state2).N;
        (*state1).NaNcount = (*state2).NaNcount;
        (*state1).pInfcount = (*state2).pInfcount;
        (*state1).nInfcount = (*state2).nInfcount;
        (*state1).maxScale = (*state2).maxScale;
        (*state1).maxScaleCount = (*state2).maxScaleCount;

        accum_sum_copy(&mut (*state1).sumX, &mut (*state2).sumX);
        accum_sum_copy(&mut (*state1).sumX2, &mut (*state2).sumX2);

        MemoryContextSwitchTo(old_context);

        PG_RETURN_POINTER!(state1 as *mut c_void);
    }

    (*state1).N += (*state2).N;
    (*state1).NaNcount += (*state2).NaNcount;
    (*state1).pInfcount += (*state2).pInfcount;
    (*state1).nInfcount += (*state2).nInfcount;

    if (*state2).N > 0 {
        if (*state2).maxScale > (*state1).maxScale {
            (*state1).maxScale = (*state2).maxScale;
            (*state1).maxScaleCount = (*state2).maxScaleCount;
        } else if (*state2).maxScale == (*state1).maxScale {
            (*state1).maxScaleCount += (*state2).maxScaleCount;
        }

        old_context = MemoryContextSwitchTo(agg_context);

        accum_sum_combine(&mut (*state1).sumX, &mut (*state2).sumX);
        accum_sum_combine(&mut (*state1).sumX2, &mut (*state2).sumX2);

        MemoryContextSwitchTo(old_context);
    }
    PG_RETURN_POINTER!(state1 as *mut c_void)
}

pub unsafe fn numeric_avg_accum(fcinfo: FunctionCallInfo) -> Datum {
    let mut state: *mut NumericAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };

    if state.is_null() {
        state = makeNumericAggState(fcinfo, false);
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        do_numeric_accum(state, PG_GETARG_NUMERIC!(fcinfo, 1));
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}

pub unsafe fn numeric_avg_combine(fcinfo: FunctionCallInfo) -> Datum {
    let mut state1: *mut NumericAggState;
    let state2: *mut NumericAggState;
    let mut agg_context: MemoryContext = null_mut();
    let old_context: MemoryContext;

    if AggCheckCallContext(fcinfo, &mut agg_context) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state1 = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };
    state2 = if PG_ARGISNULL!(fcinfo, 1) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 1) as *mut NumericAggState
    };

    if state2.is_null() {
        PG_RETURN_POINTER!(state1 as *mut c_void);
    }

    if state1.is_null() {
        old_context = MemoryContextSwitchTo(agg_context);

        state1 = makeNumericAggStateCurrentContext(false);
        (*state1).N = (*state2).N;
        (*state1).NaNcount = (*state2).NaNcount;
        (*state1).pInfcount = (*state2).pInfcount;
        (*state1).nInfcount = (*state2).nInfcount;
        (*state1).maxScale = (*state2).maxScale;
        (*state1).maxScaleCount = (*state2).maxScaleCount;

        accum_sum_copy(&mut (*state1).sumX, &mut (*state2).sumX);

        MemoryContextSwitchTo(old_context);

        PG_RETURN_POINTER!(state1 as *mut c_void);
    }

    (*state1).N += (*state2).N;
    (*state1).NaNcount += (*state2).NaNcount;
    (*state1).pInfcount += (*state2).pInfcount;
    (*state1).nInfcount += (*state2).nInfcount;

    if (*state2).N > 0 {
        if (*state2).maxScale > (*state1).maxScale {
            (*state1).maxScale = (*state2).maxScale;
            (*state1).maxScaleCount = (*state2).maxScaleCount;
        } else if (*state2).maxScale == (*state1).maxScale {
            (*state1).maxScaleCount += (*state2).maxScaleCount;
        }

        old_context = MemoryContextSwitchTo(agg_context);

        accum_sum_combine(&mut (*state1).sumX, &mut (*state2).sumX);

        MemoryContextSwitchTo(old_context);
    }
    PG_RETURN_POINTER!(state1 as *mut c_void)
}

pub unsafe fn numeric_avg_serialize(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut NumericAggState;
    let mut buf: StringInfoData = core::mem::zeroed();
    let result: *mut c_void;
    let mut tmp_var: NumericVar = core::mem::zeroed();

    if AggCheckCallContext(fcinfo, null_mut()) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state = PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState;

    init_var(&mut tmp_var);

    pq_begintypsend(&mut buf);

    pq_sendint64(&mut buf, (*state).N as uint64);

    accum_sum_final(&mut (*state).sumX, &mut tmp_var);
    numericvar_serialize(&mut buf, &tmp_var);

    pq_sendint32(&mut buf, (*state).maxScale as uint32);
    pq_sendint64(&mut buf, (*state).maxScaleCount as uint64);
    pq_sendint64(&mut buf, (*state).NaNcount as uint64);
    pq_sendint64(&mut buf, (*state).pInfcount as uint64);
    pq_sendint64(&mut buf, (*state).nInfcount as uint64);

    result = pq_endtypsend(&mut buf) as *mut c_void;

    free_var(&mut tmp_var);

    PG_RETURN_POINTER!(result)
}

pub unsafe fn numeric_avg_deserialize(fcinfo: FunctionCallInfo) -> Datum {
    let sstate: *mut c_char;
    let result: *mut NumericAggState;
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut tmp_var: NumericVar = core::mem::zeroed();

    if AggCheckCallContext(fcinfo, null_mut()) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    sstate = PG_GETARG_POINTER!(fcinfo, 0) as *mut c_char;

    init_var(&mut tmp_var);

    initReadOnlyStringInfo(
        &mut buf,
        VARDATA_ANY(sstate),
        VARSIZE_ANY_EXHDR(sstate) as c_int,
    );

    result = makeNumericAggStateCurrentContext(false);

    (*result).N = pq_getmsgint64(&mut buf);

    numericvar_deserialize(&mut buf, &mut tmp_var);
    accum_sum_add(&mut (*result).sumX, &tmp_var);

    (*result).maxScale = pq_getmsgint(&mut buf, 4) as c_int;
    (*result).maxScaleCount = pq_getmsgint64(&mut buf);
    (*result).NaNcount = pq_getmsgint64(&mut buf);
    (*result).pInfcount = pq_getmsgint64(&mut buf);
    (*result).nInfcount = pq_getmsgint64(&mut buf);

    pq_getmsgend(&mut buf);

    free_var(&mut tmp_var);

    PG_RETURN_POINTER!(result as *mut c_void)
}

pub unsafe fn numeric_serialize(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut NumericAggState;
    let mut buf: StringInfoData = core::mem::zeroed();
    let result: *mut c_void;
    let mut tmp_var: NumericVar = core::mem::zeroed();

    if AggCheckCallContext(fcinfo, null_mut()) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state = PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState;

    init_var(&mut tmp_var);

    pq_begintypsend(&mut buf);

    pq_sendint64(&mut buf, (*state).N as uint64);

    accum_sum_final(&mut (*state).sumX, &mut tmp_var);
    numericvar_serialize(&mut buf, &tmp_var);

    accum_sum_final(&mut (*state).sumX2, &mut tmp_var);
    numericvar_serialize(&mut buf, &tmp_var);

    pq_sendint32(&mut buf, (*state).maxScale as uint32);
    pq_sendint64(&mut buf, (*state).maxScaleCount as uint64);
    pq_sendint64(&mut buf, (*state).NaNcount as uint64);
    pq_sendint64(&mut buf, (*state).pInfcount as uint64);
    pq_sendint64(&mut buf, (*state).nInfcount as uint64);

    result = pq_endtypsend(&mut buf) as *mut c_void;

    free_var(&mut tmp_var);

    PG_RETURN_POINTER!(result)
}

pub unsafe fn numeric_deserialize(fcinfo: FunctionCallInfo) -> Datum {
    let sstate: *mut c_char;
    let result: *mut NumericAggState;
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut tmp_var: NumericVar = core::mem::zeroed();

    if AggCheckCallContext(fcinfo, null_mut()) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    sstate = PG_GETARG_POINTER!(fcinfo, 0) as *mut c_char;

    init_var(&mut tmp_var);

    initReadOnlyStringInfo(
        &mut buf,
        VARDATA_ANY(sstate),
        VARSIZE_ANY_EXHDR(sstate) as c_int,
    );

    result = makeNumericAggStateCurrentContext(false);

    (*result).N = pq_getmsgint64(&mut buf);

    numericvar_deserialize(&mut buf, &mut tmp_var);
    accum_sum_add(&mut (*result).sumX, &tmp_var);

    numericvar_deserialize(&mut buf, &mut tmp_var);
    accum_sum_add(&mut (*result).sumX2, &tmp_var);

    (*result).maxScale = pq_getmsgint(&mut buf, 4) as c_int;
    (*result).maxScaleCount = pq_getmsgint64(&mut buf);
    (*result).NaNcount = pq_getmsgint64(&mut buf);
    (*result).pInfcount = pq_getmsgint64(&mut buf);
    (*result).nInfcount = pq_getmsgint64(&mut buf);

    pq_getmsgend(&mut buf);

    free_var(&mut tmp_var);

    PG_RETURN_POINTER!(result as *mut c_void)
}

pub unsafe fn numeric_accum_inv(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut NumericAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };

    if state.is_null() {
        elog!(ERROR, "numeric_accum_inv called with NULL state");
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        if !do_numeric_discard(state, PG_GETARG_NUMERIC!(fcinfo, 1)) {
            PG_RETURN_NULL!(fcinfo);
        }
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}


// On platforms with 128-bit integer support (always, in Rust) some aggregates
// use a 128-bit integer based transition datatype.

#[repr(C)]
pub struct Int128AggState {
    calcSumX2: bool, // if true, calculate sumX2
    N: int64,        // count of processed numbers
    sumX: int128,    // sum of processed numbers
    sumX2: int128,   // sum of squares of processed numbers
}

unsafe fn makeInt128AggState(fcinfo: FunctionCallInfo, calcSumX2: bool) -> *mut Int128AggState {
    let state: *mut Int128AggState;
    let mut agg_context: MemoryContext = null_mut();
    let old_context: MemoryContext;

    if AggCheckCallContext(fcinfo, &mut agg_context) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    old_context = MemoryContextSwitchTo(agg_context);

    state = palloc0(core::mem::size_of::<Int128AggState>()) as *mut Int128AggState;
    (*state).calcSumX2 = calcSumX2;

    MemoryContextSwitchTo(old_context);

    state
}

unsafe fn makeInt128AggStateCurrentContext(calcSumX2: bool) -> *mut Int128AggState {
    let state: *mut Int128AggState;

    state = palloc0(core::mem::size_of::<Int128AggState>()) as *mut Int128AggState;
    (*state).calcSumX2 = calcSumX2;

    state
}

unsafe fn do_int128_accum(state: *mut Int128AggState, newval: int128) {
    if (*state).calcSumX2 {
        (*state).sumX2 += newval * newval;
    }

    (*state).sumX += newval;
    (*state).N += 1;
}

unsafe fn do_int128_discard(state: *mut Int128AggState, newval: int128) {
    if (*state).calcSumX2 {
        (*state).sumX2 -= newval * newval;
    }

    (*state).sumX -= newval;
    (*state).N -= 1;
}

// PolyNumAggState == Int128AggState on platforms with int128 support.
type PolyNumAggState = Int128AggState;
unsafe fn makePolyNumAggState(fcinfo: FunctionCallInfo, calcSumX2: bool) -> *mut PolyNumAggState {
    makeInt128AggState(fcinfo, calcSumX2)
}
unsafe fn makePolyNumAggStateCurrentContext(calcSumX2: bool) -> *mut PolyNumAggState {
    makeInt128AggStateCurrentContext(calcSumX2)
}

pub unsafe fn int2_accum(fcinfo: FunctionCallInfo) -> Datum {
    let mut state: *mut PolyNumAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };

    if state.is_null() {
        state = makePolyNumAggState(fcinfo, true);
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        do_int128_accum(state, PG_GETARG_INT16!(fcinfo, 1) as int128);
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}

pub unsafe fn int4_accum(fcinfo: FunctionCallInfo) -> Datum {
    let mut state: *mut PolyNumAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };

    if state.is_null() {
        state = makePolyNumAggState(fcinfo, true);
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        do_int128_accum(state, PG_GETARG_INT32!(fcinfo, 1) as int128);
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}

pub unsafe fn int8_accum(fcinfo: FunctionCallInfo) -> Datum {
    let mut state: *mut NumericAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };

    if state.is_null() {
        state = makeNumericAggState(fcinfo, true);
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        do_numeric_accum(state, int64_to_numeric(PG_GETARG_INT64!(fcinfo, 1)));
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}

pub unsafe fn numeric_poly_combine(fcinfo: FunctionCallInfo) -> Datum {
    let mut state1: *mut PolyNumAggState;
    let state2: *mut PolyNumAggState;
    let mut agg_context: MemoryContext = null_mut();
    let old_context: MemoryContext;

    if AggCheckCallContext(fcinfo, &mut agg_context) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state1 = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };
    state2 = if PG_ARGISNULL!(fcinfo, 1) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 1) as *mut PolyNumAggState
    };

    if state2.is_null() {
        PG_RETURN_POINTER!(state1 as *mut c_void);
    }

    if state1.is_null() {
        old_context = MemoryContextSwitchTo(agg_context);

        state1 = makePolyNumAggState(fcinfo, true);
        (*state1).N = (*state2).N;

        (*state1).sumX = (*state2).sumX;
        (*state1).sumX2 = (*state2).sumX2;

        MemoryContextSwitchTo(old_context);

        PG_RETURN_POINTER!(state1 as *mut c_void);
    }

    if (*state2).N > 0 {
        (*state1).N += (*state2).N;

        (*state1).sumX += (*state2).sumX;
        (*state1).sumX2 += (*state2).sumX2;
    }
    PG_RETURN_POINTER!(state1 as *mut c_void)
}

pub unsafe fn numeric_poly_serialize(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut PolyNumAggState;
    let mut buf: StringInfoData = core::mem::zeroed();
    let result: *mut c_void;
    let mut tmp_var: NumericVar = core::mem::zeroed();

    if AggCheckCallContext(fcinfo, null_mut()) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state = PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState;

    init_var(&mut tmp_var);

    pq_begintypsend(&mut buf);

    pq_sendint64(&mut buf, (*state).N as uint64);

    int128_to_numericvar((*state).sumX, &mut tmp_var);
    numericvar_serialize(&mut buf, &tmp_var);

    int128_to_numericvar((*state).sumX2, &mut tmp_var);
    numericvar_serialize(&mut buf, &tmp_var);

    result = pq_endtypsend(&mut buf) as *mut c_void;

    free_var(&mut tmp_var);

    PG_RETURN_POINTER!(result)
}

pub unsafe fn numeric_poly_deserialize(fcinfo: FunctionCallInfo) -> Datum {
    let sstate: *mut c_char;
    let result: *mut PolyNumAggState;
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut tmp_var: NumericVar = core::mem::zeroed();

    if AggCheckCallContext(fcinfo, null_mut()) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    sstate = PG_GETARG_POINTER!(fcinfo, 0) as *mut c_char;

    init_var(&mut tmp_var);

    initReadOnlyStringInfo(
        &mut buf,
        VARDATA_ANY(sstate),
        VARSIZE_ANY_EXHDR(sstate) as c_int,
    );

    result = makePolyNumAggStateCurrentContext(false);

    (*result).N = pq_getmsgint64(&mut buf);

    numericvar_deserialize(&mut buf, &mut tmp_var);
    numericvar_to_int128(&tmp_var, &mut (*result).sumX);

    numericvar_deserialize(&mut buf, &mut tmp_var);
    numericvar_to_int128(&tmp_var, &mut (*result).sumX2);

    pq_getmsgend(&mut buf);

    free_var(&mut tmp_var);

    PG_RETURN_POINTER!(result as *mut c_void)
}

pub unsafe fn int8_avg_accum(fcinfo: FunctionCallInfo) -> Datum {
    let mut state: *mut PolyNumAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };

    if state.is_null() {
        state = makePolyNumAggState(fcinfo, false);
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        do_int128_accum(state, PG_GETARG_INT64!(fcinfo, 1) as int128);
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}

pub unsafe fn int8_avg_combine(fcinfo: FunctionCallInfo) -> Datum {
    let mut state1: *mut PolyNumAggState;
    let state2: *mut PolyNumAggState;
    let mut agg_context: MemoryContext = null_mut();
    let old_context: MemoryContext;

    if AggCheckCallContext(fcinfo, &mut agg_context) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state1 = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };
    state2 = if PG_ARGISNULL!(fcinfo, 1) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 1) as *mut PolyNumAggState
    };

    if state2.is_null() {
        PG_RETURN_POINTER!(state1 as *mut c_void);
    }

    if state1.is_null() {
        old_context = MemoryContextSwitchTo(agg_context);

        state1 = makePolyNumAggState(fcinfo, false);
        (*state1).N = (*state2).N;

        (*state1).sumX = (*state2).sumX;
        MemoryContextSwitchTo(old_context);

        PG_RETURN_POINTER!(state1 as *mut c_void);
    }

    if (*state2).N > 0 {
        (*state1).N += (*state2).N;

        (*state1).sumX += (*state2).sumX;
    }
    PG_RETURN_POINTER!(state1 as *mut c_void)
}

pub unsafe fn int8_avg_serialize(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut PolyNumAggState;
    let mut buf: StringInfoData = core::mem::zeroed();
    let result: *mut c_void;
    let mut tmp_var: NumericVar = core::mem::zeroed();

    if AggCheckCallContext(fcinfo, null_mut()) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state = PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState;

    init_var(&mut tmp_var);

    pq_begintypsend(&mut buf);

    pq_sendint64(&mut buf, (*state).N as uint64);

    int128_to_numericvar((*state).sumX, &mut tmp_var);
    numericvar_serialize(&mut buf, &tmp_var);

    result = pq_endtypsend(&mut buf) as *mut c_void;

    free_var(&mut tmp_var);

    PG_RETURN_POINTER!(result)
}

pub unsafe fn int8_avg_deserialize(fcinfo: FunctionCallInfo) -> Datum {
    let sstate: *mut c_char;
    let result: *mut PolyNumAggState;
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut tmp_var: NumericVar = core::mem::zeroed();

    if AggCheckCallContext(fcinfo, null_mut()) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    sstate = PG_GETARG_POINTER!(fcinfo, 0) as *mut c_char;

    init_var(&mut tmp_var);

    initReadOnlyStringInfo(
        &mut buf,
        VARDATA_ANY(sstate),
        VARSIZE_ANY_EXHDR(sstate) as c_int,
    );

    result = makePolyNumAggStateCurrentContext(false);

    (*result).N = pq_getmsgint64(&mut buf);

    numericvar_deserialize(&mut buf, &mut tmp_var);
    numericvar_to_int128(&tmp_var, &mut (*result).sumX);

    pq_getmsgend(&mut buf);

    free_var(&mut tmp_var);

    PG_RETURN_POINTER!(result as *mut c_void)
}

pub unsafe fn int2_accum_inv(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut PolyNumAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };

    if state.is_null() {
        elog!(ERROR, "int2_accum_inv called with NULL state");
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        do_int128_discard(state, PG_GETARG_INT16!(fcinfo, 1) as int128);
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}

pub unsafe fn int4_accum_inv(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut PolyNumAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };

    if state.is_null() {
        elog!(ERROR, "int4_accum_inv called with NULL state");
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        do_int128_discard(state, PG_GETARG_INT32!(fcinfo, 1) as int128);
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}

pub unsafe fn int8_accum_inv(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut NumericAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };

    if state.is_null() {
        elog!(ERROR, "int8_accum_inv called with NULL state");
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        if !do_numeric_discard(state, int64_to_numeric(PG_GETARG_INT64!(fcinfo, 1))) {
            elog!(ERROR, "do_numeric_discard failed unexpectedly");
        }
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}

pub unsafe fn int8_avg_accum_inv(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut PolyNumAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };

    if state.is_null() {
        elog!(ERROR, "int8_avg_accum_inv called with NULL state");
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        do_int128_discard(state, PG_GETARG_INT64!(fcinfo, 1) as int128);
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}

pub unsafe fn numeric_poly_sum(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut PolyNumAggState;
    let res: Numeric;
    let mut result: NumericVar = core::mem::zeroed();

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };

    if state.is_null() || (*state).N == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    init_var(&mut result);

    int128_to_numericvar((*state).sumX, &mut result);

    res = make_result(&result);

    free_var(&mut result);

    PG_RETURN_NUMERIC!(res)
}

pub unsafe fn numeric_poly_avg(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut PolyNumAggState;
    let mut result: NumericVar = core::mem::zeroed();
    let countd: Datum;
    let sumd: Datum;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };

    if state.is_null() || (*state).N == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    init_var(&mut result);

    int128_to_numericvar((*state).sumX, &mut result);

    countd = NumericGetDatum(int64_to_numeric((*state).N));
    sumd = NumericGetDatum(make_result(&result));

    free_var(&mut result);

    PG_RETURN_DATUM!(DirectFunctionCall2!(numeric_div, sumd, countd))
}

pub unsafe fn numeric_avg(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut NumericAggState;
    let n_datum: Datum;
    let sumX_datum: Datum;
    let mut sumX_var: NumericVar = core::mem::zeroed();

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };

    if state.is_null() || NA_TOTAL_COUNT(state) == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    if (*state).NaNcount > 0 {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
    }

    if (*state).pInfcount > 0 && (*state).nInfcount > 0 {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
    }
    if (*state).pInfcount > 0 {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_pinf)));
    }
    if (*state).nInfcount > 0 {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_ninf)));
    }

    n_datum = NumericGetDatum(int64_to_numeric((*state).N));

    init_var(&mut sumX_var);
    accum_sum_final(&mut (*state).sumX, &mut sumX_var);
    sumX_datum = NumericGetDatum(make_result(&sumX_var));
    free_var(&mut sumX_var);

    PG_RETURN_DATUM!(DirectFunctionCall2!(numeric_div, sumX_datum, n_datum))
}

pub unsafe fn numeric_sum(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut NumericAggState;
    let mut sumX_var: NumericVar = core::mem::zeroed();
    let result: Numeric;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };

    if state.is_null() || NA_TOTAL_COUNT(state) == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    if (*state).NaNcount > 0 {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
    }

    if (*state).pInfcount > 0 && (*state).nInfcount > 0 {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_nan)));
    }
    if (*state).pInfcount > 0 {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_pinf)));
    }
    if (*state).nInfcount > 0 {
        PG_RETURN_NUMERIC!(make_result(cvar(&const_ninf)));
    }

    init_var(&mut sumX_var);
    accum_sum_final(&mut (*state).sumX, &mut sumX_var);
    result = make_result(&sumX_var);
    free_var(&mut sumX_var);

    PG_RETURN_NUMERIC!(result)
}

/// Workhorse routine for the standard deviance and variance aggregates.
unsafe fn numeric_stddev_internal(
    state: *mut NumericAggState,
    variance: bool,
    sample: bool,
    is_null: *mut bool,
) -> Numeric {
    let res: Numeric;
    let mut vn: NumericVar = core::mem::zeroed();
    let mut vsumX: NumericVar = core::mem::zeroed();
    let mut vsumX2: NumericVar = core::mem::zeroed();
    let mut vNminus1: NumericVar = core::mem::zeroed();
    let totCount: int64;
    let mut rscale: c_int;

    if state.is_null() || {
        totCount = NA_TOTAL_COUNT(state);
        totCount == 0
    } {
        *is_null = true;
        return null_mut();
    }

    if sample && totCount <= 1 {
        *is_null = true;
        return null_mut();
    }

    *is_null = false;

    if (*state).NaNcount > 0 || (*state).pInfcount > 0 || (*state).nInfcount > 0 {
        return make_result(cvar(&const_nan));
    }

    init_var(&mut vn);
    init_var(&mut vsumX);
    init_var(&mut vsumX2);

    int64_to_numericvar((*state).N, &mut vn);
    accum_sum_final(&mut (*state).sumX, &mut vsumX);
    accum_sum_final(&mut (*state).sumX2, &mut vsumX2);

    init_var(&mut vNminus1);
    sub_var(&vn, cvar(&const_one), &mut vNminus1);

    rscale = vsumX.dscale * 2;

    mul_var(&vsumX, &vsumX, &mut vsumX, rscale); /* vsumX = sumX * sumX */
    mul_var(&vn, &vsumX2, &mut vsumX2, rscale); /* vsumX2 = N * sumX2 */
    sub_var(&vsumX2, &vsumX, &mut vsumX2); /* N * sumX2 - sumX * sumX */

    if cmp_var(&vsumX2, cvar(&const_zero)) <= 0 {
        res = make_result(cvar(&const_zero));
    } else {
        if sample {
            mul_var(&vn, &vNminus1, &mut vNminus1, 0); /* N * (N - 1) */
        } else {
            mul_var(&vn, &vn, &mut vNminus1, 0); /* N * N */
        }
        rscale = select_div_scale(&vsumX2, &vNminus1);
        div_var(&vsumX2, &vNminus1, &mut vsumX, rscale, true, true); /* variance */
        if !variance {
            sqrt_var(&vsumX, &mut vsumX, rscale); /* stddev */
        }

        res = make_result(&vsumX);
    }

    free_var(&mut vNminus1);
    free_var(&mut vsumX);
    free_var(&mut vsumX2);

    res
}

pub unsafe fn numeric_var_samp(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut NumericAggState;
    let res: Numeric;
    let mut is_null: bool = false;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };

    res = numeric_stddev_internal(state, true, true, &mut is_null);

    if is_null {
        PG_RETURN_NULL!(fcinfo)
    } else {
        PG_RETURN_NUMERIC!(res)
    }
}

pub unsafe fn numeric_stddev_samp(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut NumericAggState;
    let res: Numeric;
    let mut is_null: bool = false;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };

    res = numeric_stddev_internal(state, false, true, &mut is_null);

    if is_null {
        PG_RETURN_NULL!(fcinfo)
    } else {
        PG_RETURN_NUMERIC!(res)
    }
}

pub unsafe fn numeric_var_pop(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut NumericAggState;
    let res: Numeric;
    let mut is_null: bool = false;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };

    res = numeric_stddev_internal(state, true, false, &mut is_null);

    if is_null {
        PG_RETURN_NULL!(fcinfo)
    } else {
        PG_RETURN_NUMERIC!(res)
    }
}

pub unsafe fn numeric_stddev_pop(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut NumericAggState;
    let res: Numeric;
    let mut is_null: bool = false;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut NumericAggState
    };

    res = numeric_stddev_internal(state, false, false, &mut is_null);

    if is_null {
        PG_RETURN_NULL!(fcinfo)
    } else {
        PG_RETURN_NUMERIC!(res)
    }
}

unsafe fn numeric_poly_stddev_internal(
    state: *mut Int128AggState,
    variance: bool,
    sample: bool,
    is_null: *mut bool,
) -> Numeric {
    let mut numstate: NumericAggState = core::mem::zeroed();
    let res: Numeric;

    memset(
        &mut numstate as *mut NumericAggState as *mut c_void,
        0,
        core::mem::size_of::<NumericAggState>(),
    );

    if !state.is_null() {
        let mut tmp_var: NumericVar = core::mem::zeroed();

        numstate.N = (*state).N;

        init_var(&mut tmp_var);

        int128_to_numericvar((*state).sumX, &mut tmp_var);
        accum_sum_add(&mut numstate.sumX, &tmp_var);

        int128_to_numericvar((*state).sumX2, &mut tmp_var);
        accum_sum_add(&mut numstate.sumX2, &tmp_var);

        free_var(&mut tmp_var);
    }

    res = numeric_stddev_internal(&mut numstate, variance, sample, is_null);

    if numstate.sumX.ndigits > 0 {
        pfree(numstate.sumX.pos_digits as *mut c_void);
        pfree(numstate.sumX.neg_digits as *mut c_void);
    }
    if numstate.sumX2.ndigits > 0 {
        pfree(numstate.sumX2.pos_digits as *mut c_void);
        pfree(numstate.sumX2.neg_digits as *mut c_void);
    }

    res
}

pub unsafe fn numeric_poly_var_samp(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut PolyNumAggState;
    let res: Numeric;
    let mut is_null: bool = false;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };

    res = numeric_poly_stddev_internal(state, true, true, &mut is_null);

    if is_null {
        PG_RETURN_NULL!(fcinfo)
    } else {
        PG_RETURN_NUMERIC!(res)
    }
}

pub unsafe fn numeric_poly_stddev_samp(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut PolyNumAggState;
    let res: Numeric;
    let mut is_null: bool = false;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };

    res = numeric_poly_stddev_internal(state, false, true, &mut is_null);

    if is_null {
        PG_RETURN_NULL!(fcinfo)
    } else {
        PG_RETURN_NUMERIC!(res)
    }
}

pub unsafe fn numeric_poly_var_pop(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut PolyNumAggState;
    let res: Numeric;
    let mut is_null: bool = false;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };

    res = numeric_poly_stddev_internal(state, true, false, &mut is_null);

    if is_null {
        PG_RETURN_NULL!(fcinfo)
    } else {
        PG_RETURN_NUMERIC!(res)
    }
}

pub unsafe fn numeric_poly_stddev_pop(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut PolyNumAggState;
    let res: Numeric;
    let mut is_null: bool = false;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut PolyNumAggState
    };

    res = numeric_poly_stddev_internal(state, false, false, &mut is_null);

    if is_null {
        PG_RETURN_NULL!(fcinfo)
    } else {
        PG_RETURN_NUMERIC!(res)
    }
}


// ----------------------------------------------------------------------
//
// SUM transition functions for integer datatypes.
//
// USE_FLOAT8_BYVAL is defined on our targets, so the pass-by-reference
// fast-paths (#ifndef USE_FLOAT8_BYVAL) are NOT compiled.
//
// ----------------------------------------------------------------------

pub unsafe fn int2_sum(fcinfo: FunctionCallInfo) -> Datum {
    let newval: int64;

    if PG_ARGISNULL!(fcinfo, 0) {
        if PG_ARGISNULL!(fcinfo, 1) {
            PG_RETURN_NULL!(fcinfo);
        }
        newval = PG_GETARG_INT16!(fcinfo, 1) as int64;
        PG_RETURN_INT64!(newval);
    }

    {
        let oldsum: int64 = PG_GETARG_INT64!(fcinfo, 0);

        if PG_ARGISNULL!(fcinfo, 1) {
            PG_RETURN_INT64!(oldsum);
        }

        newval = oldsum + PG_GETARG_INT16!(fcinfo, 1) as int64;

        PG_RETURN_INT64!(newval)
    }
}

pub unsafe fn int4_sum(fcinfo: FunctionCallInfo) -> Datum {
    let newval: int64;

    if PG_ARGISNULL!(fcinfo, 0) {
        if PG_ARGISNULL!(fcinfo, 1) {
            PG_RETURN_NULL!(fcinfo);
        }
        newval = PG_GETARG_INT32!(fcinfo, 1) as int64;
        PG_RETURN_INT64!(newval);
    }

    {
        let oldsum: int64 = PG_GETARG_INT64!(fcinfo, 0);

        if PG_ARGISNULL!(fcinfo, 1) {
            PG_RETURN_INT64!(oldsum);
        }

        newval = oldsum + PG_GETARG_INT32!(fcinfo, 1) as int64;

        PG_RETURN_INT64!(newval)
    }
}

/// Note: this function is obsolete, it's no longer used for SUM(int8).
pub unsafe fn int8_sum(fcinfo: FunctionCallInfo) -> Datum {
    let oldsum: Numeric;

    if PG_ARGISNULL!(fcinfo, 0) {
        if PG_ARGISNULL!(fcinfo, 1) {
            PG_RETURN_NULL!(fcinfo);
        }
        PG_RETURN_NUMERIC!(int64_to_numeric(PG_GETARG_INT64!(fcinfo, 1)));
    }

    oldsum = PG_GETARG_NUMERIC!(fcinfo, 0);

    if PG_ARGISNULL!(fcinfo, 1) {
        PG_RETURN_NUMERIC!(oldsum);
    }

    PG_RETURN_DATUM!(DirectFunctionCall2!(
        numeric_add,
        NumericGetDatum(oldsum),
        NumericGetDatum(int64_to_numeric(PG_GETARG_INT64!(fcinfo, 1)))
    ))
}

// ----------------------------------------------------------------------
//
// Routines for avg(int2) and avg(int4), with a two-element int8 array
// transition datatype.
//
// TODO(pg-port): ArrayType / ARR_* macros live in utils/array; the
// int2/int4 avg accumulators and int8_avg/int2int4_sum final functions are
// faithful but reference the array machinery via local stubs.
// ----------------------------------------------------------------------

#[repr(C)]
struct Int8TransTypeData {
    count: int64,
    sum: int64,
}

// TODO(pg-port): utils/array.h ArrayType + ARR_DATA_PTR/ARR_HASNULL/ARR_SIZE.
type ArrayType = *mut c_void;
unsafe fn ARR_HASNULL(_a: ArrayType) -> bool {
    false
}
unsafe fn ARR_SIZE(_a: ArrayType) -> usize {
    0
}
unsafe fn ARR_OVERHEAD_NONULLS(_n: c_int) -> usize {
    0
}
unsafe fn ARR_DATA_PTR(a: ArrayType) -> *mut c_char {
    a as *mut c_char
}

pub unsafe fn int2_avg_accum(fcinfo: FunctionCallInfo) -> Datum {
    let transarray: ArrayType;
    let newval: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let transdata: *mut Int8TransTypeData;

    if AggCheckCallContext(fcinfo, null_mut()) != 0 {
        transarray = PG_GETARG_POINTER!(fcinfo, 0) as ArrayType;
    } else {
        transarray = PG_GETARG_POINTER!(fcinfo, 0) as ArrayType;
    }

    if ARR_HASNULL(transarray)
        || ARR_SIZE(transarray)
            != ARR_OVERHEAD_NONULLS(1) + core::mem::size_of::<Int8TransTypeData>()
    {
        elog!(ERROR, "expected 2-element int8 array");
    }

    transdata = ARR_DATA_PTR(transarray) as *mut Int8TransTypeData;
    (*transdata).count += 1;
    (*transdata).sum += newval as int64;

    PG_RETURN_POINTER!(transarray)
}

pub unsafe fn int4_avg_accum(fcinfo: FunctionCallInfo) -> Datum {
    let transarray: ArrayType;
    let newval: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let transdata: *mut Int8TransTypeData;

    if AggCheckCallContext(fcinfo, null_mut()) != 0 {
        transarray = PG_GETARG_POINTER!(fcinfo, 0) as ArrayType;
    } else {
        transarray = PG_GETARG_POINTER!(fcinfo, 0) as ArrayType;
    }

    if ARR_HASNULL(transarray)
        || ARR_SIZE(transarray)
            != ARR_OVERHEAD_NONULLS(1) + core::mem::size_of::<Int8TransTypeData>()
    {
        elog!(ERROR, "expected 2-element int8 array");
    }

    transdata = ARR_DATA_PTR(transarray) as *mut Int8TransTypeData;
    (*transdata).count += 1;
    (*transdata).sum += newval as int64;

    PG_RETURN_POINTER!(transarray)
}

pub unsafe fn int4_avg_combine(fcinfo: FunctionCallInfo) -> Datum {
    let transarray1: ArrayType;
    let transarray2: ArrayType;
    let state1: *mut Int8TransTypeData;
    let state2: *mut Int8TransTypeData;

    if AggCheckCallContext(fcinfo, null_mut()) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    transarray1 = PG_GETARG_POINTER!(fcinfo, 0) as ArrayType;
    transarray2 = PG_GETARG_POINTER!(fcinfo, 1) as ArrayType;

    if ARR_HASNULL(transarray1)
        || ARR_SIZE(transarray1)
            != ARR_OVERHEAD_NONULLS(1) + core::mem::size_of::<Int8TransTypeData>()
    {
        elog!(ERROR, "expected 2-element int8 array");
    }

    if ARR_HASNULL(transarray2)
        || ARR_SIZE(transarray2)
            != ARR_OVERHEAD_NONULLS(1) + core::mem::size_of::<Int8TransTypeData>()
    {
        elog!(ERROR, "expected 2-element int8 array");
    }

    state1 = ARR_DATA_PTR(transarray1) as *mut Int8TransTypeData;
    state2 = ARR_DATA_PTR(transarray2) as *mut Int8TransTypeData;

    (*state1).count += (*state2).count;
    (*state1).sum += (*state2).sum;

    PG_RETURN_POINTER!(transarray1)
}

pub unsafe fn int2_avg_accum_inv(fcinfo: FunctionCallInfo) -> Datum {
    let transarray: ArrayType;
    let newval: int16 = PG_GETARG_INT16!(fcinfo, 1);
    let transdata: *mut Int8TransTypeData;

    transarray = PG_GETARG_POINTER!(fcinfo, 0) as ArrayType;

    if ARR_HASNULL(transarray)
        || ARR_SIZE(transarray)
            != ARR_OVERHEAD_NONULLS(1) + core::mem::size_of::<Int8TransTypeData>()
    {
        elog!(ERROR, "expected 2-element int8 array");
    }

    transdata = ARR_DATA_PTR(transarray) as *mut Int8TransTypeData;
    (*transdata).count -= 1;
    (*transdata).sum -= newval as int64;

    PG_RETURN_POINTER!(transarray)
}

pub unsafe fn int4_avg_accum_inv(fcinfo: FunctionCallInfo) -> Datum {
    let transarray: ArrayType;
    let newval: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let transdata: *mut Int8TransTypeData;

    transarray = PG_GETARG_POINTER!(fcinfo, 0) as ArrayType;

    if ARR_HASNULL(transarray)
        || ARR_SIZE(transarray)
            != ARR_OVERHEAD_NONULLS(1) + core::mem::size_of::<Int8TransTypeData>()
    {
        elog!(ERROR, "expected 2-element int8 array");
    }

    transdata = ARR_DATA_PTR(transarray) as *mut Int8TransTypeData;
    (*transdata).count -= 1;
    (*transdata).sum -= newval as int64;

    PG_RETURN_POINTER!(transarray)
}

pub unsafe fn int8_avg(fcinfo: FunctionCallInfo) -> Datum {
    let transarray: ArrayType = PG_GETARG_POINTER!(fcinfo, 0) as ArrayType;
    let transdata: *mut Int8TransTypeData;
    let countd: Datum;
    let sumd: Datum;

    if ARR_HASNULL(transarray)
        || ARR_SIZE(transarray)
            != ARR_OVERHEAD_NONULLS(1) + core::mem::size_of::<Int8TransTypeData>()
    {
        elog!(ERROR, "expected 2-element int8 array");
    }
    transdata = ARR_DATA_PTR(transarray) as *mut Int8TransTypeData;

    if (*transdata).count == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    countd = NumericGetDatum(int64_to_numeric((*transdata).count));
    sumd = NumericGetDatum(int64_to_numeric((*transdata).sum));

    PG_RETURN_DATUM!(DirectFunctionCall2!(numeric_div, sumd, countd))
}

/// SUM(int2) and SUM(int4) both return int8.
pub unsafe fn int2int4_sum(fcinfo: FunctionCallInfo) -> Datum {
    let transarray: ArrayType = PG_GETARG_POINTER!(fcinfo, 0) as ArrayType;
    let transdata: *mut Int8TransTypeData;

    if ARR_HASNULL(transarray)
        || ARR_SIZE(transarray)
            != ARR_OVERHEAD_NONULLS(1) + core::mem::size_of::<Int8TransTypeData>()
    {
        elog!(ERROR, "expected 2-element int8 array");
    }
    transdata = ARR_DATA_PTR(transarray) as *mut Int8TransTypeData;

    if (*transdata).count == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    Int64GetDatum((*transdata).sum)
}


// ----------------------------------------------------------------------
//
// Local functions follow
//
// In general, these do not support "special" (NaN or infinity) inputs;
// callers should handle those possibilities first.
//
// ----------------------------------------------------------------------

/// alloc_var() - Allocate a digit buffer of ndigits digits (plus a spare).
unsafe fn alloc_var(var: *mut NumericVar, ndigits: c_int) {
    digitbuf_free((*var).buf);
    (*var).buf = digitbuf_alloc(ndigits + 1);
    *(*var).buf.add(0) = 0; /* spare digit for rounding */
    (*var).digits = (*var).buf.add(1);
    (*var).ndigits = ndigits;
}

/// free_var() - Return the digit buffer of a variable to the free pool
unsafe fn free_var(var: *mut NumericVar) {
    digitbuf_free((*var).buf);
    (*var).buf = null_mut();
    (*var).digits = null_mut();
    (*var).sign = NUMERIC_NAN;
}

/// zero_var() - Set a variable to ZERO.
unsafe fn zero_var(var: *mut NumericVar) {
    digitbuf_free((*var).buf);
    (*var).buf = null_mut();
    (*var).digits = null_mut();
    (*var).ndigits = 0;
    (*var).weight = 0; /* by convention; doesn't really matter */
    (*var).sign = NUMERIC_POS; /* anything but NAN... */
}

/// set_var_from_str() - Parse a string and put the number into a variable
unsafe fn set_var_from_str(
    str: *const c_char,
    cp_in: *const c_char,
    dest: *mut NumericVar,
    endptr: *mut *const c_char,
    escontext: *mut Node,
) -> bool {
    let mut cp = cp_in;
    let mut have_dp = false;
    let mut i: c_int;
    let decdigits: *mut u8;
    let mut sign: c_int = NUMERIC_POS;
    let mut dweight: c_int = -1;
    let ddigits: c_int;
    let mut dscale: c_int = 0;
    let weight: c_int;
    let mut ndigits: c_int;
    let offset: c_int;
    let mut digits: *mut NumericDigit;

    match *cp as u8 {
        b'+' => {
            sign = NUMERIC_POS;
            cp = cp.add(1);
        }
        b'-' => {
            sign = NUMERIC_NEG;
            cp = cp.add(1);
        }
        _ => {}
    }

    if *cp == b'.' as c_char {
        have_dp = true;
        cp = cp.add(1);
    }

    if isdigit(*cp as u8 as c_int) == 0 {
        return svfs_invalid_syntax(escontext, str);
    }

    decdigits = palloc(strlen(cp) + (DEC_DIGITS * 2) as usize) as *mut u8;

    memset(decdigits as *mut c_void, 0, DEC_DIGITS as usize);
    i = DEC_DIGITS;

    while *cp != 0 {
        if isdigit(*cp as u8 as c_int) != 0 {
            *decdigits.add(i as usize) = (*cp as u8) - b'0';
            i += 1;
            cp = cp.add(1);
            if !have_dp {
                dweight += 1;
            } else {
                dscale += 1;
            }
        } else if *cp == b'.' as c_char {
            if have_dp {
                return svfs_invalid_syntax(escontext, str);
            }
            have_dp = true;
            cp = cp.add(1);
            if *cp == b'_' as c_char {
                return svfs_invalid_syntax(escontext, str);
            }
        } else if *cp == b'_' as c_char {
            cp = cp.add(1);
            if isdigit(*cp as u8 as c_int) == 0 {
                return svfs_invalid_syntax(escontext, str);
            }
        } else {
            break;
        }
    }

    ddigits = i - DEC_DIGITS;
    memset(decdigits.add(i as usize) as *mut c_void, 0, (DEC_DIGITS - 1) as usize);

    if *cp == b'e' as c_char || *cp == b'E' as c_char {
        let mut exponent: int64 = 0;
        let mut neg = false;

        cp = cp.add(1);
        if *cp == b'+' as c_char {
            cp = cp.add(1);
        } else if *cp == b'-' as c_char {
            neg = true;
            cp = cp.add(1);
        }

        if isdigit(*cp as u8 as c_int) == 0 {
            pfree(decdigits as *mut c_void);
            return svfs_invalid_syntax(escontext, str);
        }

        while *cp != 0 {
            if isdigit(*cp as u8 as c_int) != 0 {
                exponent = exponent * 10 + ((*cp as u8) - b'0') as int64;
                cp = cp.add(1);
                if exponent > (PG_INT32_MAX / 2) as int64 {
                    pfree(decdigits as *mut c_void);
                    return svfs_out_of_range(escontext, str);
                }
            } else if *cp == b'_' as c_char {
                cp = cp.add(1);
                if isdigit(*cp as u8 as c_int) == 0 {
                    pfree(decdigits as *mut c_void);
                    return svfs_invalid_syntax(escontext, str);
                }
            } else {
                break;
            }
        }

        if neg {
            exponent = -exponent;
        }

        dweight += exponent as c_int;
        dscale -= exponent as c_int;
        if dscale < 0 {
            dscale = 0;
        }
    }

    if dweight >= 0 {
        weight = (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1;
    } else {
        weight = -((-dweight - 1) / DEC_DIGITS + 1);
    }
    offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
    ndigits = (ddigits + offset + DEC_DIGITS - 1) / DEC_DIGITS;

    alloc_var(dest, ndigits);
    (*dest).sign = sign;
    (*dest).weight = weight;
    (*dest).dscale = dscale;

    i = DEC_DIGITS - offset;
    digits = (*dest).digits;

    while ndigits > 0 {
        ndigits -= 1;
        *digits = (((*decdigits.add(i as usize) as c_int * 10
            + *decdigits.add((i + 1) as usize) as c_int)
            * 10
            + *decdigits.add((i + 2) as usize) as c_int)
            * 10
            + *decdigits.add((i + 3) as usize) as c_int) as NumericDigit;
        digits = digits.add(1);
        i += DEC_DIGITS;
    }

    pfree(decdigits as *mut c_void);

    strip_var(dest);

    *endptr = cp;

    true
}

unsafe fn svfs_out_of_range(_escontext: *mut Node, _str: *const c_char) -> bool {
    ereport!(ERROR, errmsg!("value overflows numeric format"));
    false
}
unsafe fn svfs_invalid_syntax(_escontext: *mut Node, str: *const c_char) -> bool {
    ereport!(
        ERROR,
        errmsg!(
            "invalid input syntax for type {}: \"{}\"",
            "numeric",
            std::ffi::CStr::from_ptr(str).to_string_lossy()
        )
    );
    false
}

/// Return the numeric value of a single hex digit.
#[inline]
fn xdigit_value(dig: c_char) -> c_int {
    let d = dig as u8;
    if d >= b'0' && d <= b'9' {
        (d - b'0') as c_int
    } else if d >= b'a' && d <= b'f' {
        (d - b'a' + 10) as c_int
    } else if d >= b'A' && d <= b'F' {
        (d - b'A' + 10) as c_int
    } else {
        -1
    }
}

/// set_var_from_non_decimal_integer_str() - Parse a non-decimal integer string.
unsafe fn set_var_from_non_decimal_integer_str(
    str: *const c_char,
    cp_in: *const c_char,
    sign: c_int,
    base: c_int,
    dest: *mut NumericVar,
    endptr: *mut *const c_char,
    escontext: *mut Node,
) -> bool {
    let mut cp = cp_in;
    let firstdigit = cp;
    let mut tmp: int64;
    let mut mul: int64;
    let mut tmp_var: NumericVar = core::mem::zeroed();

    init_var(&mut tmp_var);

    zero_var(dest);

    tmp = 0;
    mul = 1;

    if base == 16 {
        while *cp != 0 {
            if isxdigit(*cp as u8 as c_int) != 0 {
                if mul > PG_INT64_MAX / 16 {
                    int64_to_numericvar(mul, &mut tmp_var);
                    mul_var(dest, &tmp_var, dest, 0);
                    int64_to_numericvar(tmp, &mut tmp_var);
                    add_var(dest, &tmp_var, dest);

                    if (*dest).weight > NUMERIC_WEIGHT_MAX {
                        return svfs_out_of_range(escontext, str);
                    }

                    tmp = 0;
                    mul = 1;
                }

                tmp = tmp * 16 + xdigit_value(*cp) as int64;
                cp = cp.add(1);
                mul = mul * 16;
            } else if *cp == b'_' as c_char {
                cp = cp.add(1);
                if isxdigit(*cp as u8 as c_int) == 0 {
                    return svfs_invalid_syntax(escontext, str);
                }
            } else {
                break;
            }
        }
    } else if base == 8 {
        while *cp != 0 {
            if *cp >= b'0' as c_char && *cp <= b'7' as c_char {
                if mul > PG_INT64_MAX / 8 {
                    int64_to_numericvar(mul, &mut tmp_var);
                    mul_var(dest, &tmp_var, dest, 0);
                    int64_to_numericvar(tmp, &mut tmp_var);
                    add_var(dest, &tmp_var, dest);

                    if (*dest).weight > NUMERIC_WEIGHT_MAX {
                        return svfs_out_of_range(escontext, str);
                    }

                    tmp = 0;
                    mul = 1;
                }

                tmp = tmp * 8 + ((*cp as u8) - b'0') as int64;
                cp = cp.add(1);
                mul = mul * 8;
            } else if *cp == b'_' as c_char {
                cp = cp.add(1);
                if *cp < b'0' as c_char || *cp > b'7' as c_char {
                    return svfs_invalid_syntax(escontext, str);
                }
            } else {
                break;
            }
        }
    } else if base == 2 {
        while *cp != 0 {
            if *cp >= b'0' as c_char && *cp <= b'1' as c_char {
                if mul > PG_INT64_MAX / 2 {
                    int64_to_numericvar(mul, &mut tmp_var);
                    mul_var(dest, &tmp_var, dest, 0);
                    int64_to_numericvar(tmp, &mut tmp_var);
                    add_var(dest, &tmp_var, dest);

                    if (*dest).weight > NUMERIC_WEIGHT_MAX {
                        return svfs_out_of_range(escontext, str);
                    }

                    tmp = 0;
                    mul = 1;
                }

                tmp = tmp * 2 + ((*cp as u8) - b'0') as int64;
                cp = cp.add(1);
                mul = mul * 2;
            } else if *cp == b'_' as c_char {
                cp = cp.add(1);
                if *cp < b'0' as c_char || *cp > b'1' as c_char {
                    return svfs_invalid_syntax(escontext, str);
                }
            } else {
                break;
            }
        }
    } else {
        return svfs_invalid_syntax(escontext, str);
    }

    if cp == firstdigit {
        return svfs_invalid_syntax(escontext, str);
    }

    int64_to_numericvar(mul, &mut tmp_var);
    mul_var(dest, &tmp_var, dest, 0);
    int64_to_numericvar(tmp, &mut tmp_var);
    add_var(dest, &tmp_var, dest);

    if (*dest).weight > NUMERIC_WEIGHT_MAX {
        return svfs_out_of_range(escontext, str);
    }

    (*dest).sign = sign;

    free_var(&mut tmp_var);

    *endptr = cp;

    true
}

/// set_var_from_num() - Convert the packed db format into a variable
unsafe fn set_var_from_num(num: Numeric, dest: *mut NumericVar) {
    let ndigits: c_int;

    ndigits = NUMERIC_NDIGITS(num);

    alloc_var(dest, ndigits);

    (*dest).weight = NUMERIC_WEIGHT(num);
    (*dest).sign = NUMERIC_SIGN(num);
    (*dest).dscale = NUMERIC_DSCALE(num);

    memcpy(
        (*dest).digits as *mut c_void,
        NUMERIC_DIGITS(num) as *const c_void,
        ndigits as usize * core::mem::size_of::<NumericDigit>(),
    );
}

/// init_var_from_num() - Initialize a variable from packed db format (no copy).
unsafe fn init_var_from_num(num: Numeric, dest: *mut NumericVar) {
    (*dest).ndigits = NUMERIC_NDIGITS(num);
    (*dest).weight = NUMERIC_WEIGHT(num);
    (*dest).sign = NUMERIC_SIGN(num);
    (*dest).dscale = NUMERIC_DSCALE(num);
    (*dest).digits = NUMERIC_DIGITS(num);
    (*dest).buf = null_mut();
}

/// set_var_from_var() - Copy one variable into another
unsafe fn set_var_from_var(value: *const NumericVar, dest: *mut NumericVar) {
    let newbuf: *mut NumericDigit;

    newbuf = digitbuf_alloc((*value).ndigits + 1);
    *newbuf.add(0) = 0; /* spare digit for rounding */
    if (*value).ndigits > 0 {
        memcpy(
            newbuf.add(1) as *mut c_void,
            (*value).digits as *const c_void,
            (*value).ndigits as usize * core::mem::size_of::<NumericDigit>(),
        );
    }

    digitbuf_free((*dest).buf);

    memmove(
        dest as *mut c_void,
        value as *const c_void,
        core::mem::size_of::<NumericVar>(),
    );
    (*dest).buf = newbuf;
    (*dest).digits = newbuf.add(1);
}


/// get_str_from_var() - Convert a var to text representation.
unsafe fn get_str_from_var(var: *const NumericVar) -> *mut c_char {
    let dscale: c_int;
    let str: *mut c_char;
    let mut cp: *mut c_char;
    let endcp: *mut c_char;
    let mut i: c_int;
    let mut d: c_int;
    let mut dig: NumericDigit;
    let mut d1: NumericDigit;

    dscale = (*var).dscale;

    i = ((*var).weight + 1) * DEC_DIGITS;
    if i <= 0 {
        i = 1;
    }

    str = palloc((i + dscale + DEC_DIGITS + 2) as usize) as *mut c_char;
    cp = str;

    if (*var).sign == NUMERIC_NEG {
        *cp = b'-' as c_char;
        cp = cp.add(1);
    }

    if (*var).weight < 0 {
        d = (*var).weight + 1;
        *cp = b'0' as c_char;
        cp = cp.add(1);
    } else {
        d = 0;
        while d <= (*var).weight {
            dig = if d < (*var).ndigits {
                *(*var).digits.add(d as usize)
            } else {
                0
            };
            {
                let mut putit: bool = d > 0;

                d1 = dig / 1000;
                dig -= d1 * 1000;
                putit |= d1 > 0;
                if putit {
                    *cp = (d1 as u8 + b'0') as c_char;
                    cp = cp.add(1);
                }
                d1 = dig / 100;
                dig -= d1 * 100;
                putit |= d1 > 0;
                if putit {
                    *cp = (d1 as u8 + b'0') as c_char;
                    cp = cp.add(1);
                }
                d1 = dig / 10;
                dig -= d1 * 10;
                putit |= d1 > 0;
                if putit {
                    *cp = (d1 as u8 + b'0') as c_char;
                    cp = cp.add(1);
                }
                *cp = (dig as u8 + b'0') as c_char;
                cp = cp.add(1);
            }
            d += 1;
        }
    }

    if dscale > 0 {
        *cp = b'.' as c_char;
        cp = cp.add(1);
        endcp = cp.add(dscale as usize);
        i = 0;
        while i < dscale {
            dig = if d >= 0 && d < (*var).ndigits {
                *(*var).digits.add(d as usize)
            } else {
                0
            };
            d1 = dig / 1000;
            dig -= d1 * 1000;
            *cp = (d1 as u8 + b'0') as c_char;
            cp = cp.add(1);
            d1 = dig / 100;
            dig -= d1 * 100;
            *cp = (d1 as u8 + b'0') as c_char;
            cp = cp.add(1);
            d1 = dig / 10;
            dig -= d1 * 10;
            *cp = (d1 as u8 + b'0') as c_char;
            cp = cp.add(1);
            *cp = (dig as u8 + b'0') as c_char;
            cp = cp.add(1);
            d += 1;
            i += DEC_DIGITS;
        }
        cp = endcp;
    }

    *cp = b'\0' as c_char;
    str
}

/// get_str_from_var_sci() - Convert a var to scientific notation.
unsafe fn get_str_from_var_sci(var: *const NumericVar, rscale_in: c_int) -> *mut c_char {
    let mut rscale = rscale_in;
    let exponent: int32;
    let mut tmp_var: NumericVar = core::mem::zeroed();
    let len: usize;
    let str: *mut c_char;
    let sig_out: *mut c_char;

    if rscale < 0 {
        rscale = 0;
    }

    if (*var).ndigits > 0 {
        exponent = ((*var).weight + 1) * DEC_DIGITS;
        let e = exponent - (DEC_DIGITS - log10(*(*var).digits.add(0) as f64) as c_int);
        let exponent = e;

        init_var(&mut tmp_var);

        power_ten_int(exponent, &mut tmp_var);
        div_var(var, &tmp_var, &mut tmp_var, rscale, true, true);
        sig_out = get_str_from_var(&tmp_var);

        free_var(&mut tmp_var);

        len = strlen(sig_out) + 13;
        str = palloc(len) as *mut c_char;
        snprintf(str, len, c"%se%+03d".as_ptr(), sig_out, exponent);

        pfree(sig_out as *mut c_void);

        return str;
    } else {
        exponent = 0;
    }

    init_var(&mut tmp_var);

    power_ten_int(exponent, &mut tmp_var);
    div_var(var, &tmp_var, &mut tmp_var, rscale, true, true);
    sig_out = get_str_from_var(&tmp_var);

    free_var(&mut tmp_var);

    len = strlen(sig_out) + 13;
    str = palloc(len) as *mut c_char;
    snprintf(str, len, c"%se%+03d".as_ptr(), sig_out, exponent);

    pfree(sig_out as *mut c_void);

    str
}

/// numericvar_serialize - serialize NumericVar to binary format
unsafe fn numericvar_serialize(buf: StringInfo, var: *const NumericVar) {
    let mut i: c_int;

    pq_sendint32(buf, (*var).ndigits as uint32);
    pq_sendint32(buf, (*var).weight as uint32);
    pq_sendint32(buf, (*var).sign as uint32);
    pq_sendint32(buf, (*var).dscale as uint32);
    i = 0;
    while i < (*var).ndigits {
        pq_sendint16(buf, *(*var).digits.add(i as usize) as uint16);
        i += 1;
    }
}

/// numericvar_deserialize - deserialize binary format to NumericVar
unsafe fn numericvar_deserialize(buf: StringInfo, var: *mut NumericVar) {
    let len: c_int;
    let mut i: c_int;

    len = pq_getmsgint(buf, core::mem::size_of::<int32>() as c_int) as c_int;

    alloc_var(var, len);

    (*var).weight = pq_getmsgint(buf, core::mem::size_of::<int32>() as c_int) as c_int;
    (*var).sign = pq_getmsgint(buf, core::mem::size_of::<int32>() as c_int) as c_int;
    (*var).dscale = pq_getmsgint(buf, core::mem::size_of::<int32>() as c_int) as c_int;
    i = 0;
    while i < len {
        *(*var).digits.add(i as usize) =
            pq_getmsgint(buf, core::mem::size_of::<int16>() as c_int) as NumericDigit;
        i += 1;
    }
}

/// duplicate_numeric() - copy a packed-format Numeric (handles NaN/Inf).
unsafe fn duplicate_numeric(num: Numeric) -> Numeric {
    let res: Numeric;

    res = palloc(VARSIZE(num as *const c_char) as usize) as Numeric;
    memcpy(
        res as *mut c_void,
        num as *const c_void,
        VARSIZE(num as *const c_char) as usize,
    );
    res
}

/// make_result_opt_error() - Create the packed db numeric format from a var.
unsafe fn make_result_opt_error(var: *const NumericVar, have_error: *mut bool) -> Numeric {
    let result: Numeric;
    let mut digits: *mut NumericDigit = (*var).digits;
    let mut weight: c_int = (*var).weight;
    let mut sign: c_int = (*var).sign;
    let mut n: c_int;
    let len: Size;

    if !have_error.is_null() {
        *have_error = false;
    }

    if (sign & NUMERIC_SIGN_MASK as c_int) == NUMERIC_SPECIAL as c_int {
        if !(sign == NUMERIC_NAN || sign == NUMERIC_PINF || sign == NUMERIC_NINF) {
            elog!(ERROR, "invalid numeric sign value 0x{:x}", sign);
        }

        result = palloc(NUMERIC_HDRSZ_SHORT) as Numeric;

        SET_VARSIZE(result as *mut c_char, NUMERIC_HDRSZ_SHORT as int32);
        (*result).choice.n_header = sign as uint16;

        return result;
    }

    n = (*var).ndigits;

    /* truncate leading zeroes */
    while n > 0 && *digits == 0 {
        digits = digits.add(1);
        weight -= 1;
        n -= 1;
    }
    /* truncate trailing zeroes */
    while n > 0 && *digits.add((n - 1) as usize) == 0 {
        n -= 1;
    }

    if n == 0 {
        weight = 0;
        sign = NUMERIC_POS;
    }

    if NUMERIC_CAN_BE_SHORT((*var).dscale, weight) {
        len = NUMERIC_HDRSZ_SHORT + n as usize * core::mem::size_of::<NumericDigit>();
        result = palloc(len) as Numeric;
        SET_VARSIZE(result as *mut c_char, len as int32);
        (*result).choice.n_short.n_header = ((if sign == NUMERIC_NEG {
            NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK
        } else {
            NUMERIC_SHORT
        }) | (((*var).dscale << NUMERIC_SHORT_DSCALE_SHIFT) as uint16)
            | (if weight < 0 {
                NUMERIC_SHORT_WEIGHT_SIGN_MASK
            } else {
                0
            })
            | ((weight as uint16) & NUMERIC_SHORT_WEIGHT_MASK));
    } else {
        len = NUMERIC_HDRSZ + n as usize * core::mem::size_of::<NumericDigit>();
        result = palloc(len) as Numeric;
        SET_VARSIZE(result as *mut c_char, len as int32);
        (*result).choice.n_long.n_sign_dscale =
            sign as uint16 | ((*var).dscale as uint16 & NUMERIC_DSCALE_MASK);
        (*result).choice.n_long.n_weight = weight as int16;
    }

    Assert!(NUMERIC_NDIGITS(result) == n);
    if n > 0 {
        memcpy(
            NUMERIC_DIGITS(result) as *mut c_void,
            digits as *const c_void,
            n as usize * core::mem::size_of::<NumericDigit>(),
        );
    }

    if NUMERIC_WEIGHT(result) != weight || NUMERIC_DSCALE(result) != (*var).dscale {
        if !have_error.is_null() {
            *have_error = true;
            return null_mut();
        } else {
            ereport!(ERROR, errmsg!("value overflows numeric format"));
        }
    }

    result
}

/// make_result() - make_result_opt_error() without "have_error".
unsafe fn make_result(var: *const NumericVar) -> Numeric {
    make_result_opt_error(var, null_mut())
}

/// apply_typmod() - Do bounds checking and rounding per typmod.
unsafe fn apply_typmod(var: *mut NumericVar, typmod: int32, escontext: *mut Node) -> bool {
    let precision: c_int;
    let scale: c_int;
    let maxdigits: c_int;
    let mut ddigits: c_int;
    let mut i: c_int;

    if !is_valid_numeric_typmod(typmod) {
        return true;
    }

    precision = numeric_typmod_precision(typmod);
    scale = numeric_typmod_scale(typmod);
    maxdigits = precision - scale;

    round_var(var, scale);

    if (*var).dscale < 0 {
        (*var).dscale = 0;
    }

    ddigits = ((*var).weight + 1) * DEC_DIGITS;
    if ddigits > maxdigits {
        i = 0;
        while i < (*var).ndigits {
            let dig: NumericDigit = *(*var).digits.add(i as usize);

            if dig != 0 {
                if dig < 10 {
                    ddigits -= 3;
                } else if dig < 100 {
                    ddigits -= 2;
                } else if dig < 1000 {
                    ddigits -= 1;
                }
                if ddigits > maxdigits {
                    let _ = escontext;
                    ereport!(ERROR, errmsg!("numeric field overflow"));
                    return false;
                }
                break;
            }
            ddigits -= DEC_DIGITS;
            i += 1;
        }
    }

    true
}

/// apply_typmod_special() - Bounds checking for an Inf or NaN per typmod.
unsafe fn apply_typmod_special(num: Numeric, typmod: int32, escontext: *mut Node) -> bool {
    Assert!(NUMERIC_IS_SPECIAL(num));

    if NUMERIC_IS_NAN(num) {
        return true;
    }

    if !is_valid_numeric_typmod(typmod) {
        return true;
    }

    let _ = escontext;
    ereport!(ERROR, errmsg!("numeric field overflow"));
    false
}

/// Convert numeric to int8, rounding if needed.  false on overflow.
unsafe fn numericvar_to_int64(var: *const NumericVar, result: *mut int64) -> bool {
    let digits: *mut NumericDigit;
    let ndigits: c_int;
    let weight: c_int;
    let mut i: c_int;
    let mut val: int64;
    let neg: bool;
    let mut rounded: NumericVar = core::mem::zeroed();

    init_var(&mut rounded);
    set_var_from_var(var, &mut rounded);
    round_var(&mut rounded, 0);

    strip_var(&mut rounded);
    ndigits = rounded.ndigits;
    if ndigits == 0 {
        *result = 0;
        free_var(&mut rounded);
        return true;
    }

    weight = rounded.weight;
    Assert!(weight >= 0 && ndigits <= weight + 1);

    digits = rounded.digits;
    neg = rounded.sign == NUMERIC_NEG;
    val = -(*digits.add(0) as int64);
    i = 1;
    while i <= weight {
        if pg_mul_s64_overflow(val, NBASE as int64, &mut val) {
            free_var(&mut rounded);
            return false;
        }

        if i < ndigits {
            if pg_sub_s64_overflow(val, *digits.add(i as usize) as int64, &mut val) {
                free_var(&mut rounded);
                return false;
            }
        }
        i += 1;
    }

    free_var(&mut rounded);

    if !neg {
        if val == PG_INT64_MIN {
            return false;
        }
        val = -val;
    }
    *result = val;

    true
}

/// Convert int8 value to numeric.
unsafe fn int64_to_numericvar(val: int64, var: *mut NumericVar) {
    let mut uval: uint64;
    let mut newuval: uint64;
    let mut ptr: *mut NumericDigit;
    let mut ndigits: c_int;

    alloc_var(var, 20 / DEC_DIGITS);
    if val < 0 {
        (*var).sign = NUMERIC_NEG;
        uval = pg_abs_s64(val);
    } else {
        (*var).sign = NUMERIC_POS;
        uval = val as uint64;
    }
    (*var).dscale = 0;
    if val == 0 {
        (*var).ndigits = 0;
        (*var).weight = 0;
        return;
    }
    ptr = (*var).digits.add((*var).ndigits as usize);
    ndigits = 0;
    loop {
        ptr = ptr.sub(1);
        ndigits += 1;
        newuval = uval / NBASE as uint64;
        *ptr = (uval - newuval * NBASE as uint64) as NumericDigit;
        uval = newuval;
        if uval == 0 {
            break;
        }
    }
    (*var).digits = ptr;
    (*var).ndigits = ndigits;
    (*var).weight = ndigits - 1;
}

/// Convert numeric to uint64, rounding if needed.  false on overflow.
unsafe fn numericvar_to_uint64(var: *const NumericVar, result: *mut uint64) -> bool {
    let digits: *mut NumericDigit;
    let ndigits: c_int;
    let weight: c_int;
    let mut i: c_int;
    let mut val: uint64;
    let mut rounded: NumericVar = core::mem::zeroed();

    init_var(&mut rounded);
    set_var_from_var(var, &mut rounded);
    round_var(&mut rounded, 0);

    strip_var(&mut rounded);
    ndigits = rounded.ndigits;
    if ndigits == 0 {
        *result = 0;
        free_var(&mut rounded);
        return true;
    }

    if rounded.sign == NUMERIC_NEG {
        free_var(&mut rounded);
        return false;
    }

    weight = rounded.weight;
    Assert!(weight >= 0 && ndigits <= weight + 1);

    digits = rounded.digits;
    val = *digits.add(0) as uint64;
    i = 1;
    while i <= weight {
        if pg_mul_u64_overflow(val, NBASE as uint64, &mut val) {
            free_var(&mut rounded);
            return false;
        }

        if i < ndigits {
            if pg_add_u64_overflow(val, *digits.add(i as usize) as uint64, &mut val) {
                free_var(&mut rounded);
                return false;
            }
        }
        i += 1;
    }

    free_var(&mut rounded);

    *result = val;

    true
}

/// Convert numeric to int128, rounding if needed.  false on overflow.
unsafe fn numericvar_to_int128(var: *const NumericVar, result: *mut int128) -> bool {
    let digits: *mut NumericDigit;
    let ndigits: c_int;
    let weight: c_int;
    let mut i: c_int;
    let mut val: int128;
    let mut oldval: int128;
    let neg: bool;
    let mut rounded: NumericVar = core::mem::zeroed();

    init_var(&mut rounded);
    set_var_from_var(var, &mut rounded);
    round_var(&mut rounded, 0);

    strip_var(&mut rounded);
    ndigits = rounded.ndigits;
    if ndigits == 0 {
        *result = 0;
        free_var(&mut rounded);
        return true;
    }

    weight = rounded.weight;
    Assert!(weight >= 0 && ndigits <= weight + 1);

    digits = rounded.digits;
    neg = rounded.sign == NUMERIC_NEG;
    val = *digits.add(0) as int128;
    i = 1;
    while i <= weight {
        oldval = val;
        val = val.wrapping_mul(NBASE as int128);
        if i < ndigits {
            val = val.wrapping_add(*digits.add(i as usize) as int128);
        }

        if (val / NBASE as int128) != oldval {
            if !neg || (val.wrapping_neg()) != val || val == 0 || oldval < 0 {
                free_var(&mut rounded);
                return false;
            }
        }
        i += 1;
    }

    free_var(&mut rounded);

    *result = if neg { -val } else { val };
    true
}

/// Convert 128 bit integer to numeric.
unsafe fn int128_to_numericvar(val: int128, var: *mut NumericVar) {
    let mut uval: uint128;
    let mut newuval: uint128;
    let mut ptr: *mut NumericDigit;
    let mut ndigits: c_int;

    alloc_var(var, 40 / DEC_DIGITS);
    if val < 0 {
        (*var).sign = NUMERIC_NEG;
        uval = (val as i128).unsigned_abs();
    } else {
        (*var).sign = NUMERIC_POS;
        uval = val as uint128;
    }
    (*var).dscale = 0;
    if val == 0 {
        (*var).ndigits = 0;
        (*var).weight = 0;
        return;
    }
    ptr = (*var).digits.add((*var).ndigits as usize);
    ndigits = 0;
    loop {
        ptr = ptr.sub(1);
        ndigits += 1;
        newuval = uval / NBASE as uint128;
        *ptr = (uval - newuval * NBASE as uint128) as NumericDigit;
        uval = newuval;
        if uval == 0 {
            break;
        }
    }
    (*var).digits = ptr;
    (*var).ndigits = ndigits;
    (*var).weight = ndigits - 1;
}

/// Convert a NumericVar to float8; if out of range, return +/- HUGE_VAL
unsafe fn numericvar_to_double_no_overflow(var: *const NumericVar) -> f64 {
    let tmp: *mut c_char;
    let val: f64;
    let mut endptr: *mut c_char = null_mut();

    tmp = get_str_from_var(var);

    val = strtod(tmp, &mut endptr);
    if *endptr != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                "double precision",
                std::ffi::CStr::from_ptr(tmp).to_string_lossy()
            )
        );
    }

    pfree(tmp as *mut c_void);

    val
}


/// cmp_var() - Compare two values on variable level.
unsafe fn cmp_var(var1: *const NumericVar, var2: *const NumericVar) -> c_int {
    cmp_var_common(
        (*var1).digits,
        (*var1).ndigits,
        (*var1).weight,
        (*var1).sign,
        (*var2).digits,
        (*var2).ndigits,
        (*var2).weight,
        (*var2).sign,
    )
}

/// cmp_var_common() - Main routine of cmp_var().
unsafe fn cmp_var_common(
    var1digits: *const NumericDigit,
    var1ndigits: c_int,
    var1weight: c_int,
    var1sign: c_int,
    var2digits: *const NumericDigit,
    var2ndigits: c_int,
    var2weight: c_int,
    var2sign: c_int,
) -> c_int {
    if var1ndigits == 0 {
        if var2ndigits == 0 {
            return 0;
        }
        if var2sign == NUMERIC_NEG {
            return 1;
        }
        return -1;
    }
    if var2ndigits == 0 {
        if var1sign == NUMERIC_POS {
            return 1;
        }
        return -1;
    }

    if var1sign == NUMERIC_POS {
        if var2sign == NUMERIC_NEG {
            return 1;
        }
        return cmp_abs_common(
            var1digits, var1ndigits, var1weight, var2digits, var2ndigits, var2weight,
        );
    }

    if var2sign == NUMERIC_POS {
        return -1;
    }

    cmp_abs_common(
        var2digits, var2ndigits, var2weight, var1digits, var1ndigits, var1weight,
    )
}

/// add_var() - Full add functionality on variable level (handling signs).
unsafe fn add_var(var1: *const NumericVar, var2: *const NumericVar, result: *mut NumericVar) {
    if (*var1).sign == NUMERIC_POS {
        if (*var2).sign == NUMERIC_POS {
            add_abs(var1, var2, result);
            (*result).sign = NUMERIC_POS;
        } else {
            match cmp_abs(var1, var2) {
                0 => {
                    zero_var(result);
                    (*result).dscale = Max!((*var1).dscale, (*var2).dscale);
                }
                1 => {
                    sub_abs(var1, var2, result);
                    (*result).sign = NUMERIC_POS;
                }
                -1 => {
                    sub_abs(var2, var1, result);
                    (*result).sign = NUMERIC_NEG;
                }
                _ => {}
            }
        }
    } else {
        if (*var2).sign == NUMERIC_POS {
            match cmp_abs(var1, var2) {
                0 => {
                    zero_var(result);
                    (*result).dscale = Max!((*var1).dscale, (*var2).dscale);
                }
                1 => {
                    sub_abs(var1, var2, result);
                    (*result).sign = NUMERIC_NEG;
                }
                -1 => {
                    sub_abs(var2, var1, result);
                    (*result).sign = NUMERIC_POS;
                }
                _ => {}
            }
        } else {
            add_abs(var1, var2, result);
            (*result).sign = NUMERIC_NEG;
        }
    }
}

/// sub_var() - Full sub functionality on variable level (handling signs).
unsafe fn sub_var(var1: *const NumericVar, var2: *const NumericVar, result: *mut NumericVar) {
    if (*var1).sign == NUMERIC_POS {
        if (*var2).sign == NUMERIC_NEG {
            add_abs(var1, var2, result);
            (*result).sign = NUMERIC_POS;
        } else {
            match cmp_abs(var1, var2) {
                0 => {
                    zero_var(result);
                    (*result).dscale = Max!((*var1).dscale, (*var2).dscale);
                }
                1 => {
                    sub_abs(var1, var2, result);
                    (*result).sign = NUMERIC_POS;
                }
                -1 => {
                    sub_abs(var2, var1, result);
                    (*result).sign = NUMERIC_NEG;
                }
                _ => {}
            }
        }
    } else {
        if (*var2).sign == NUMERIC_NEG {
            match cmp_abs(var1, var2) {
                0 => {
                    zero_var(result);
                    (*result).dscale = Max!((*var1).dscale, (*var2).dscale);
                }
                1 => {
                    sub_abs(var1, var2, result);
                    (*result).sign = NUMERIC_NEG;
                }
                -1 => {
                    sub_abs(var2, var1, result);
                    (*result).sign = NUMERIC_POS;
                }
                _ => {}
            }
        } else {
            add_abs(var1, var2, result);
            (*result).sign = NUMERIC_NEG;
        }
    }
}

/// mul_var() - Multiplication on variable level (rounds to rscale).
unsafe fn mul_var(
    var1_in: *const NumericVar,
    var2_in: *const NumericVar,
    result: *mut NumericVar,
    rscale: c_int,
) {
    let mut var1 = var1_in;
    let mut var2 = var2_in;
    let mut res_ndigits: c_int;
    let mut res_ndigitpairs: c_int;
    let res_sign: c_int;
    let res_weight: c_int;
    let pair_offset: c_int;
    let maxdigits: c_int;
    let maxdigitpairs: c_int;
    let dig: *mut uint64;
    let mut dig_i1_off: *mut uint64;
    let mut maxdig: uint64;
    let mut carry: uint64;
    let mut newdig: uint64;
    let var1ndigits: c_int;
    let var2ndigits: c_int;
    let mut var1ndigitpairs: c_int;
    let mut var2ndigitpairs: c_int;
    let var1digits: *mut NumericDigit;
    let var2digits: *mut NumericDigit;
    let mut var1digitpair: uint32;
    let var2digitpairs: *mut uint32;
    let res_digits: *mut NumericDigit;
    let mut i: c_int;
    let mut i1: c_int;
    let mut i2: c_int;
    let mut i2limit: c_int;

    if (*var1).ndigits > (*var2).ndigits {
        let tmp = var1;
        var1 = var2;
        var2 = tmp;
    }

    var1ndigits = (*var1).ndigits;
    var2ndigits = (*var2).ndigits;
    var1digits = (*var1).digits;
    var2digits = (*var2).digits;

    if var1ndigits == 0 {
        zero_var(result);
        (*result).dscale = rscale;
        return;
    }

    if var1ndigits <= 6 && rscale == (*var1).dscale + (*var2).dscale {
        mul_var_short(var1, var2, result);
        return;
    }

    if (*var1).sign == (*var2).sign {
        res_sign = NUMERIC_POS;
    } else {
        res_sign = NUMERIC_NEG;
    }

    var1ndigitpairs = (var1ndigits + 1) / 2;
    var2ndigitpairs = (var2ndigits + 1) / 2;

    res_ndigits = var1ndigits + var2ndigits;

    res_ndigitpairs = res_ndigits / 2 + 1;

    pair_offset = res_ndigitpairs - var1ndigitpairs - var2ndigitpairs + 1;

    res_weight = (*var1).weight + (*var2).weight + 1 + 2 * res_ndigitpairs
        - res_ndigits
        - (var1ndigits & 1)
        - (var2ndigits & 1);

    maxdigits =
        res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS + MUL_GUARD_DIGITS;
    maxdigitpairs = maxdigits / 2 + 1;

    res_ndigitpairs = Min!(res_ndigitpairs, maxdigitpairs);
    res_ndigits = 2 * res_ndigitpairs;

    if res_ndigitpairs <= pair_offset {
        zero_var(result);
        (*result).dscale = rscale;
        return;
    }
    var1ndigitpairs = Min!(var1ndigitpairs, res_ndigitpairs - pair_offset);
    var2ndigitpairs = Min!(var2ndigitpairs, res_ndigitpairs - pair_offset);

    dig = palloc(
        res_ndigitpairs as usize * core::mem::size_of::<uint64>()
            + var2ndigitpairs as usize * core::mem::size_of::<uint32>(),
    ) as *mut uint64;

    var2digitpairs = dig.add(res_ndigitpairs as usize) as *mut uint32;

    i2 = 0;
    while i2 < var2ndigitpairs - 1 {
        *var2digitpairs.add(i2 as usize) = (*var2digits.add((2 * i2) as usize) as uint32)
            * NBASE as uint32
            + *var2digits.add((2 * i2 + 1) as usize) as uint32;
        i2 += 1;
    }

    if 2 * i2 + 1 < var2ndigits {
        *var2digitpairs.add(i2 as usize) = (*var2digits.add((2 * i2) as usize) as uint32)
            * NBASE as uint32
            + *var2digits.add((2 * i2 + 1) as usize) as uint32;
    } else {
        *var2digitpairs.add(i2 as usize) =
            (*var2digits.add((2 * i2) as usize) as uint32) * NBASE as uint32;
    }

    i1 = var1ndigitpairs - 1;
    if 2 * i1 + 1 < var1ndigits {
        var1digitpair = (*var1digits.add((2 * i1) as usize) as uint32) * NBASE as uint32
            + *var1digits.add((2 * i1 + 1) as usize) as uint32;
    } else {
        var1digitpair = (*var1digits.add((2 * i1) as usize) as uint32) * NBASE as uint32;
    }
    maxdig = var1digitpair as uint64;

    i2limit = Min!(var2ndigitpairs, res_ndigitpairs - i1 - pair_offset);
    dig_i1_off = dig.add((i1 + pair_offset) as usize);

    memset(
        dig as *mut c_void,
        0,
        (i1 + pair_offset) as usize * core::mem::size_of::<uint64>(),
    );
    i2 = 0;
    while i2 < i2limit {
        *dig_i1_off.add(i2 as usize) =
            var1digitpair as uint64 * *var2digitpairs.add(i2 as usize) as uint64;
        i2 += 1;
    }

    i1 -= 1;
    while i1 >= 0 {
        var1digitpair = (*var1digits.add((2 * i1) as usize) as uint32) * NBASE as uint32
            + *var1digits.add((2 * i1 + 1) as usize) as uint32;
        if var1digitpair == 0 {
            i1 -= 1;
            continue;
        }

        maxdig += var1digitpair as uint64;
        if maxdig
            > (PG_UINT64_MAX - PG_UINT64_MAX / NBASE_SQR as uint64) / (NBASE_SQR as uint64 - 1)
        {
            carry = 0;
            i = res_ndigitpairs - 1;
            while i >= 0 {
                newdig = *dig.add(i as usize) + carry;
                if newdig >= NBASE_SQR as uint64 {
                    carry = newdig / NBASE_SQR as uint64;
                    newdig -= carry * NBASE_SQR as uint64;
                } else {
                    carry = 0;
                }
                *dig.add(i as usize) = newdig;
                i -= 1;
            }
            Assert!(carry == 0);
            maxdig = 1 + var1digitpair as uint64;
        }

        i2limit = Min!(var2ndigitpairs, res_ndigitpairs - i1 - pair_offset);
        dig_i1_off = dig.add((i1 + pair_offset) as usize);

        i2 = 0;
        while i2 < i2limit {
            *dig_i1_off.add(i2 as usize) +=
                var1digitpair as uint64 * *var2digitpairs.add(i2 as usize) as uint64;
            i2 += 1;
        }
        i1 -= 1;
    }

    alloc_var(result, res_ndigits);
    res_digits = (*result).digits;
    carry = 0;
    i = res_ndigitpairs - 1;
    while i >= 0 {
        newdig = *dig.add(i as usize) + carry;
        if newdig >= NBASE_SQR as uint64 {
            carry = newdig / NBASE_SQR as uint64;
            newdig -= carry * NBASE_SQR as uint64;
        } else {
            carry = 0;
        }
        *res_digits.add((2 * i + 1) as usize) =
            ((newdig as uint32) % NBASE as uint32) as NumericDigit;
        *res_digits.add((2 * i) as usize) =
            ((newdig as uint32) / NBASE as uint32) as NumericDigit;
        i -= 1;
    }
    Assert!(carry == 0);

    pfree(dig as *mut c_void);

    (*result).weight = res_weight;
    (*result).sign = res_sign;

    round_var(result, rscale);

    strip_var(result);
}


/// mul_var_short() - fast exact product when var1 has 1-6 digits.
unsafe fn mul_var_short(
    var1: *const NumericVar,
    var2: *const NumericVar,
    result: *mut NumericVar,
) {
    let var1ndigits = (*var1).ndigits;
    let var2ndigits = (*var2).ndigits;
    let v1 = (*var1).digits;
    let v2 = (*var2).digits;
    let res_sign: c_int;
    let res_weight: c_int;
    let res_ndigits: c_int;
    let res_buf: *mut NumericDigit;
    let res_digits: *mut NumericDigit;
    let mut carry: uint32 = 0;
    let mut term: uint32;

    Assert!(var1ndigits >= 1);
    Assert!(var1ndigits <= 6);
    Assert!(var2ndigits >= var1ndigits);

    if (*var1).sign == (*var2).sign {
        res_sign = NUMERIC_POS;
    } else {
        res_sign = NUMERIC_NEG;
    }
    res_weight = (*var1).weight + (*var2).weight + 1;
    res_ndigits = var1ndigits + var2ndigits;

    res_buf = digitbuf_alloc(res_ndigits + 1);
    *res_buf.add(0) = 0;
    res_digits = res_buf.add(1);

    // PRODSUMk(v1,i1,v2,i2) helpers.
    let g = |p: *const NumericDigit, i: c_int| -> uint32 { *p.add(i as usize) as uint32 };
    let prodsum1 = |i1: c_int, i2: c_int| -> uint32 { g(v1, i1) * g(v2, i2) };
    let prodsum2 =
        |i1: c_int, i2: c_int| -> uint32 { prodsum1(i1, i2) + g(v1, i1 + 1) * g(v2, i2 - 1) };
    let prodsum3 =
        |i1: c_int, i2: c_int| -> uint32 { prodsum2(i1, i2) + g(v1, i1 + 2) * g(v2, i2 - 2) };
    let prodsum4 =
        |i1: c_int, i2: c_int| -> uint32 { prodsum3(i1, i2) + g(v1, i1 + 3) * g(v2, i2 - 3) };
    let prodsum5 =
        |i1: c_int, i2: c_int| -> uint32 { prodsum4(i1, i2) + g(v1, i1 + 4) * g(v2, i2 - 4) };
    let prodsum6 =
        |i1: c_int, i2: c_int| -> uint32 { prodsum5(i1, i2) + g(v1, i1 + 5) * g(v2, i2 - 5) };

    match var1ndigits {
        1 => {
            let mut i = var2ndigits - 1;
            while i >= 0 {
                term = prodsum1(0, i) + carry;
                *res_digits.add((i + 1) as usize) = (term % NBASE as uint32) as NumericDigit;
                carry = term / NBASE as uint32;
                i -= 1;
            }
            *res_digits.add(0) = carry as NumericDigit;
        }
        2 => {
            term = prodsum1(1, var2ndigits - 1);
            *res_digits.add((res_ndigits - 1) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            let mut i = var2ndigits - 1;
            while i >= 1 {
                term = prodsum2(0, i) + carry;
                *res_digits.add((i + 1) as usize) = (term % NBASE as uint32) as NumericDigit;
                carry = term / NBASE as uint32;
                i -= 1;
            }
        }
        3 => {
            term = prodsum1(2, var2ndigits - 1);
            *res_digits.add((res_ndigits - 1) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            term = prodsum2(1, var2ndigits - 1) + carry;
            *res_digits.add((res_ndigits - 2) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            let mut i = var2ndigits - 1;
            while i >= 2 {
                term = prodsum3(0, i) + carry;
                *res_digits.add((i + 1) as usize) = (term % NBASE as uint32) as NumericDigit;
                carry = term / NBASE as uint32;
                i -= 1;
            }
        }
        4 => {
            term = prodsum1(3, var2ndigits - 1);
            *res_digits.add((res_ndigits - 1) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            term = prodsum2(2, var2ndigits - 1) + carry;
            *res_digits.add((res_ndigits - 2) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            term = prodsum3(1, var2ndigits - 1) + carry;
            *res_digits.add((res_ndigits - 3) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            let mut i = var2ndigits - 1;
            while i >= 3 {
                term = prodsum4(0, i) + carry;
                *res_digits.add((i + 1) as usize) = (term % NBASE as uint32) as NumericDigit;
                carry = term / NBASE as uint32;
                i -= 1;
            }
        }
        5 => {
            term = prodsum1(4, var2ndigits - 1);
            *res_digits.add((res_ndigits - 1) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            term = prodsum2(3, var2ndigits - 1) + carry;
            *res_digits.add((res_ndigits - 2) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            term = prodsum3(2, var2ndigits - 1) + carry;
            *res_digits.add((res_ndigits - 3) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            term = prodsum4(1, var2ndigits - 1) + carry;
            *res_digits.add((res_ndigits - 4) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            let mut i = var2ndigits - 1;
            while i >= 4 {
                term = prodsum5(0, i) + carry;
                *res_digits.add((i + 1) as usize) = (term % NBASE as uint32) as NumericDigit;
                carry = term / NBASE as uint32;
                i -= 1;
            }
        }
        6 => {
            term = prodsum1(5, var2ndigits - 1);
            *res_digits.add((res_ndigits - 1) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            term = prodsum2(4, var2ndigits - 1) + carry;
            *res_digits.add((res_ndigits - 2) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            term = prodsum3(3, var2ndigits - 1) + carry;
            *res_digits.add((res_ndigits - 3) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            term = prodsum4(2, var2ndigits - 1) + carry;
            *res_digits.add((res_ndigits - 4) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            term = prodsum5(1, var2ndigits - 1) + carry;
            *res_digits.add((res_ndigits - 5) as usize) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;

            let mut i = var2ndigits - 1;
            while i >= 5 {
                term = prodsum6(0, i) + carry;
                *res_digits.add((i + 1) as usize) = (term % NBASE as uint32) as NumericDigit;
                carry = term / NBASE as uint32;
                i -= 1;
            }
        }
        _ => {}
    }

    // Most significant var1ndigits result digits.
    match var1ndigits {
        6 => {
            term = prodsum5(0, 4) + carry;
            *res_digits.add(5) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;
            term = prodsum4(0, 3) + carry;
            *res_digits.add(4) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;
            term = prodsum3(0, 2) + carry;
            *res_digits.add(3) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;
            term = prodsum2(0, 1) + carry;
            *res_digits.add(2) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;
            term = prodsum1(0, 0) + carry;
            *res_digits.add(1) = (term % NBASE as uint32) as NumericDigit;
            *res_digits.add(0) = (term / NBASE as uint32) as NumericDigit;
        }
        5 => {
            term = prodsum4(0, 3) + carry;
            *res_digits.add(4) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;
            term = prodsum3(0, 2) + carry;
            *res_digits.add(3) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;
            term = prodsum2(0, 1) + carry;
            *res_digits.add(2) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;
            term = prodsum1(0, 0) + carry;
            *res_digits.add(1) = (term % NBASE as uint32) as NumericDigit;
            *res_digits.add(0) = (term / NBASE as uint32) as NumericDigit;
        }
        4 => {
            term = prodsum3(0, 2) + carry;
            *res_digits.add(3) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;
            term = prodsum2(0, 1) + carry;
            *res_digits.add(2) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;
            term = prodsum1(0, 0) + carry;
            *res_digits.add(1) = (term % NBASE as uint32) as NumericDigit;
            *res_digits.add(0) = (term / NBASE as uint32) as NumericDigit;
        }
        3 => {
            term = prodsum2(0, 1) + carry;
            *res_digits.add(2) = (term % NBASE as uint32) as NumericDigit;
            carry = term / NBASE as uint32;
            term = prodsum1(0, 0) + carry;
            *res_digits.add(1) = (term % NBASE as uint32) as NumericDigit;
            *res_digits.add(0) = (term / NBASE as uint32) as NumericDigit;
        }
        2 => {
            term = prodsum1(0, 0) + carry;
            *res_digits.add(1) = (term % NBASE as uint32) as NumericDigit;
            *res_digits.add(0) = (term / NBASE as uint32) as NumericDigit;
        }
        _ => {}
    }

    digitbuf_free((*result).buf);
    (*result).ndigits = res_ndigits;
    (*result).buf = res_buf;
    (*result).digits = res_digits;
    (*result).weight = res_weight;
    (*result).sign = res_sign;
    (*result).dscale = (*var1).dscale + (*var2).dscale;

    strip_var(result);
}


/// div_var() - Compute the quotient var1 / var2 to rscale fractional digits.
unsafe fn div_var(
    var1: *const NumericVar,
    var2: *const NumericVar,
    result: *mut NumericVar,
    rscale: c_int,
    round: bool,
    mut exact: bool,
) {
    let var1ndigits = (*var1).ndigits;
    let var2ndigits = (*var2).ndigits;
    let res_sign: c_int;
    let res_weight: c_int;
    let mut res_ndigits: c_int;
    let mut var1ndigitpairs: c_int;
    let mut var2ndigitpairs: c_int;
    let res_ndigitpairs: c_int;
    let div_ndigitpairs: c_int;
    let dividend: *mut int64;
    let divisor: *mut int32;
    let fdivisor: f64;
    let fdivisorinverse: f64;
    let mut fdividend: f64;
    let mut fquotient: f64;
    let mut maxdiv: int64;
    let mut qi: c_int;
    let mut qdigit: int32;
    let mut carry: int64;
    let mut newdig: int64;
    let remainder: *mut int64;
    let res_digits: *mut NumericDigit;
    let mut i: c_int;

    if var2ndigits == 0 || *(*var2).digits.add(0) == 0 {
        ereport!(ERROR, errmsg!("division by zero"));
    }

    if var2ndigits <= 2 {
        let mut idivisor: c_int;
        let mut idivisor_weight: c_int;

        idivisor = *(*var2).digits.add(0) as c_int;
        idivisor_weight = (*var2).weight;
        if var2ndigits == 2 {
            idivisor = idivisor * NBASE + *(*var2).digits.add(1) as c_int;
            idivisor_weight -= 1;
        }
        if (*var2).sign == NUMERIC_NEG {
            idivisor = -idivisor;
        }

        div_var_int(var1, idivisor, idivisor_weight, result, rscale, round);
        return;
    }
    if var2ndigits <= 4 {
        let mut idivisor: int64;
        let mut idivisor_weight: c_int;

        idivisor = *(*var2).digits.add(0) as int64;
        idivisor_weight = (*var2).weight;
        i = 1;
        while i < var2ndigits {
            idivisor = idivisor * NBASE as int64 + *(*var2).digits.add(i as usize) as int64;
            idivisor_weight -= 1;
            i += 1;
        }
        if (*var2).sign == NUMERIC_NEG {
            idivisor = -idivisor;
        }

        div_var_int64(var1, idivisor, idivisor_weight, result, rscale, round);
        return;
    }

    if var1ndigits == 0 {
        zero_var(result);
        (*result).dscale = rscale;
        return;
    }

    if var2ndigits <= 2 * (DIV_GUARD_DIGITS + 2) {
        exact = true;
    }

    if (*var1).sign == (*var2).sign {
        res_sign = NUMERIC_POS;
    } else {
        res_sign = NUMERIC_NEG;
    }
    res_weight = (*var1).weight - (*var2).weight + 1;
    res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    res_ndigits = Max!(res_ndigits, 1);
    if round {
        res_ndigits += 1;
    }
    if !exact {
        res_ndigits += DIV_GUARD_DIGITS;
    }

    var1ndigitpairs = (var1ndigits + 1) / 2;
    var2ndigitpairs = (var2ndigits + 1) / 2;
    res_ndigitpairs = (res_ndigits + 1) / 2;
    res_ndigits = 2 * res_ndigitpairs;

    if exact {
        div_ndigitpairs = res_ndigitpairs + var2ndigitpairs;
        var1ndigitpairs = Min!(var1ndigitpairs, div_ndigitpairs);
    } else {
        div_ndigitpairs = res_ndigitpairs;
        var1ndigitpairs = Min!(var1ndigitpairs, div_ndigitpairs);
        var2ndigitpairs = Min!(var2ndigitpairs, div_ndigitpairs);
    }

    dividend = palloc(
        (div_ndigitpairs + 1) as usize * core::mem::size_of::<int64>()
            + var2ndigitpairs as usize * core::mem::size_of::<int32>(),
    ) as *mut int64;
    divisor = dividend.add((div_ndigitpairs + 1) as usize) as *mut int32;

    i = 0;
    while i < var1ndigitpairs - 1 {
        *dividend.add(i as usize) = (*(*var1).digits.add((2 * i) as usize) as int64)
            * NBASE as int64
            + *(*var1).digits.add((2 * i + 1) as usize) as int64;
        i += 1;
    }

    if 2 * i + 1 < var1ndigits {
        *dividend.add(i as usize) = (*(*var1).digits.add((2 * i) as usize) as int64)
            * NBASE as int64
            + *(*var1).digits.add((2 * i + 1) as usize) as int64;
    } else {
        *dividend.add(i as usize) =
            (*(*var1).digits.add((2 * i) as usize) as int64) * NBASE as int64;
    }

    memset(
        dividend.add((i + 1) as usize) as *mut c_void,
        0,
        (div_ndigitpairs - i) as usize * core::mem::size_of::<int64>(),
    );

    i = 0;
    while i < var2ndigitpairs - 1 {
        *divisor.add(i as usize) = (*(*var2).digits.add((2 * i) as usize) as int32) * NBASE
            + *(*var2).digits.add((2 * i + 1) as usize) as int32;
        i += 1;
    }

    if 2 * i + 1 < var2ndigits {
        *divisor.add(i as usize) = (*(*var2).digits.add((2 * i) as usize) as int32) * NBASE
            + *(*var2).digits.add((2 * i + 1) as usize) as int32;
    } else {
        *divisor.add(i as usize) = (*(*var2).digits.add((2 * i) as usize) as int32) * NBASE;
    }

    fdivisor = *divisor.add(0) as f64 * NBASE_SQR as f64;
    let fdivisor = if var2ndigitpairs > 1 {
        fdivisor + *divisor.add(1) as f64
    } else {
        fdivisor
    };
    fdivisorinverse = 1.0 / fdivisor;

    maxdiv = 1;

    qi = 0;
    while qi < res_ndigitpairs {
        fdividend = *dividend.add(qi as usize) as f64 * NBASE_SQR as f64;
        fdividend += *dividend.add((qi + 1) as usize) as f64;

        fquotient = fdividend * fdivisorinverse;
        qdigit = if fquotient >= 0.0 {
            fquotient as int32
        } else {
            (fquotient as int32) - 1
        };

        if qdigit != 0 {
            maxdiv += i64abs(qdigit as int64);
            if maxdiv
                > (PG_INT64_MAX - PG_INT64_MAX / NBASE_SQR as int64 - 1) / (NBASE_SQR as int64 - 1)
            {
                carry = 0;
                i = Min!(qi + var2ndigitpairs - 2, div_ndigitpairs - 1);
                while i > qi {
                    newdig = *dividend.add(i as usize) + carry;
                    if newdig < 0 {
                        carry = -((-newdig - 1) / NBASE_SQR as int64) - 1;
                        newdig -= carry * NBASE_SQR as int64;
                    } else if newdig >= NBASE_SQR as int64 {
                        carry = newdig / NBASE_SQR as int64;
                        newdig -= carry * NBASE_SQR as int64;
                    } else {
                        carry = 0;
                    }
                    *dividend.add(i as usize) = newdig;
                    i -= 1;
                }
                *dividend.add(qi as usize) += carry;

                maxdiv = 1;

                fdividend = *dividend.add(qi as usize) as f64 * NBASE_SQR as f64;
                fdividend += *dividend.add((qi + 1) as usize) as f64;
                fquotient = fdividend * fdivisorinverse;
                qdigit = if fquotient >= 0.0 {
                    fquotient as int32
                } else {
                    (fquotient as int32) - 1
                };

                maxdiv += i64abs(qdigit as int64);
            }

            if qdigit != 0 {
                let istop = Min!(var2ndigitpairs, div_ndigitpairs - qi);
                let dividend_qi = dividend.add(qi as usize);

                i = 0;
                while i < istop {
                    *dividend_qi.add(i as usize) -=
                        qdigit as int64 * *divisor.add(i as usize) as int64;
                    i += 1;
                }
            }
        }

        *dividend.add((qi + 1) as usize) += *dividend.add(qi as usize) * NBASE_SQR as int64;

        *dividend.add(qi as usize) = qdigit as int64;
        qi += 1;
    }

    if exact {
        remainder = dividend.add(qi as usize);
        carry = 0;
        i = var2ndigitpairs - 2;
        while i >= 0 {
            newdig = *remainder.add(i as usize) + carry;
            if newdig < 0 {
                carry = -((-newdig - 1) / NBASE_SQR as int64) - 1;
                newdig -= carry * NBASE_SQR as int64;
            } else if newdig >= NBASE_SQR as int64 {
                carry = newdig / NBASE_SQR as int64;
                newdig -= carry * NBASE_SQR as int64;
            } else {
                carry = 0;
            }
            *remainder.add((i + 1) as usize) = newdig;
            i -= 1;
        }
        *remainder.add(0) = carry;

        if *remainder.add(0) < 0 {
            loop {
                carry = 0;
                i = var2ndigitpairs - 1;
                while i > 0 {
                    newdig =
                        *remainder.add(i as usize) + *divisor.add(i as usize) as int64 + carry;
                    if newdig >= NBASE_SQR as int64 {
                        *remainder.add(i as usize) = newdig - NBASE_SQR as int64;
                        carry = 1;
                    } else {
                        *remainder.add(i as usize) = newdig;
                        carry = 0;
                    }
                    i -= 1;
                }
                *remainder.add(0) += *divisor.add(0) as int64 + carry;

                *dividend.add((qi - 1) as usize) -= 1;

                if *remainder.add(0) >= 0 {
                    break;
                }
            }
        } else {
            loop {
                let mut less: bool = false;

                i = 0;
                while i < var2ndigitpairs {
                    if *remainder.add(i as usize) < *divisor.add(i as usize) as int64 {
                        less = true;
                        break;
                    }
                    if *remainder.add(i as usize) > *divisor.add(i as usize) as int64 {
                        break;
                    }
                    i += 1;
                }
                if less {
                    break;
                }

                carry = 0;
                i = var2ndigitpairs - 1;
                while i > 0 {
                    newdig =
                        *remainder.add(i as usize) - *divisor.add(i as usize) as int64 + carry;
                    if newdig < 0 {
                        *remainder.add(i as usize) = newdig + NBASE_SQR as int64;
                        carry = -1;
                    } else {
                        *remainder.add(i as usize) = newdig;
                        carry = 0;
                    }
                    i -= 1;
                }
                *remainder.add(0) = *remainder.add(0) - *divisor.add(0) as int64 + carry;

                *dividend.add((qi - 1) as usize) += 1;
            }
        }
    }

    alloc_var(result, res_ndigits);
    res_digits = (*result).digits;
    carry = 0;
    i = res_ndigitpairs - 1;
    while i >= 0 {
        newdig = *dividend.add(i as usize) + carry;
        if newdig < 0 {
            carry = -((-newdig - 1) / NBASE_SQR as int64) - 1;
            newdig -= carry * NBASE_SQR as int64;
        } else if newdig >= NBASE_SQR as int64 {
            carry = newdig / NBASE_SQR as int64;
            newdig -= carry * NBASE_SQR as int64;
        } else {
            carry = 0;
        }
        *res_digits.add((2 * i + 1) as usize) =
            ((newdig as uint32) % NBASE as uint32) as NumericDigit;
        *res_digits.add((2 * i) as usize) =
            ((newdig as uint32) / NBASE as uint32) as NumericDigit;
        i -= 1;
    }
    Assert!(carry == 0);

    pfree(dividend as *mut c_void);

    (*result).weight = res_weight;
    (*result).sign = res_sign;

    if round {
        round_var(result, rscale);
    } else {
        trunc_var(result, rscale);
    }

    strip_var(result);
}

/// div_var_int() - Divide a numeric variable by a 32-bit integer with weight.
unsafe fn div_var_int(
    var: *const NumericVar,
    ival: c_int,
    ival_weight: c_int,
    result: *mut NumericVar,
    rscale: c_int,
    round: bool,
) {
    let var_digits = (*var).digits;
    let var_ndigits = (*var).ndigits;
    let res_sign: c_int;
    let res_weight: c_int;
    let mut res_ndigits: c_int;
    let res_buf: *mut NumericDigit;
    let res_digits: *mut NumericDigit;
    let divisor: uint32;
    let mut i: c_int;

    if ival == 0 {
        ereport!(ERROR, errmsg!("division by zero"));
    }

    if var_ndigits == 0 {
        zero_var(result);
        (*result).dscale = rscale;
        return;
    }

    if (*var).sign == NUMERIC_POS {
        res_sign = if ival > 0 { NUMERIC_POS } else { NUMERIC_NEG };
    } else {
        res_sign = if ival > 0 { NUMERIC_NEG } else { NUMERIC_POS };
    }
    res_weight = (*var).weight - ival_weight;
    res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    res_ndigits = Max!(res_ndigits, 1);
    if round {
        res_ndigits += 1;
    }

    res_buf = digitbuf_alloc(res_ndigits + 1);
    *res_buf.add(0) = 0;
    res_digits = res_buf.add(1);

    divisor = (ival as int64).unsigned_abs() as uint32;

    if divisor <= u32::MAX / NBASE as uint32 {
        let mut carry: uint32 = 0;

        i = 0;
        while i < res_ndigits {
            carry = carry * NBASE as uint32
                + (if i < var_ndigits {
                    *var_digits.add(i as usize) as uint32
                } else {
                    0
                });
            *res_digits.add(i as usize) = (carry / divisor) as NumericDigit;
            carry = carry % divisor;
            i += 1;
        }
    } else {
        let mut carry: uint64 = 0;

        i = 0;
        while i < res_ndigits {
            carry = carry * NBASE as uint64
                + (if i < var_ndigits {
                    *var_digits.add(i as usize) as uint64
                } else {
                    0
                });
            *res_digits.add(i as usize) = (carry / divisor as uint64) as NumericDigit;
            carry = carry % divisor as uint64;
            i += 1;
        }
    }

    digitbuf_free((*result).buf);
    (*result).ndigits = res_ndigits;
    (*result).buf = res_buf;
    (*result).digits = res_digits;
    (*result).weight = res_weight;
    (*result).sign = res_sign;

    if round {
        round_var(result, rscale);
    } else {
        trunc_var(result, rscale);
    }

    strip_var(result);
}

/// div_var_int64() - Divide a numeric variable by a 64-bit integer with weight.
unsafe fn div_var_int64(
    var: *const NumericVar,
    ival: int64,
    ival_weight: c_int,
    result: *mut NumericVar,
    rscale: c_int,
    round: bool,
) {
    let var_digits = (*var).digits;
    let var_ndigits = (*var).ndigits;
    let res_sign: c_int;
    let res_weight: c_int;
    let mut res_ndigits: c_int;
    let res_buf: *mut NumericDigit;
    let res_digits: *mut NumericDigit;
    let divisor: uint64;
    let mut i: c_int;

    if ival == 0 {
        ereport!(ERROR, errmsg!("division by zero"));
    }

    if var_ndigits == 0 {
        zero_var(result);
        (*result).dscale = rscale;
        return;
    }

    if (*var).sign == NUMERIC_POS {
        res_sign = if ival > 0 { NUMERIC_POS } else { NUMERIC_NEG };
    } else {
        res_sign = if ival > 0 { NUMERIC_NEG } else { NUMERIC_POS };
    }
    res_weight = (*var).weight - ival_weight;
    res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    res_ndigits = Max!(res_ndigits, 1);
    if round {
        res_ndigits += 1;
    }

    res_buf = digitbuf_alloc(res_ndigits + 1);
    *res_buf.add(0) = 0;
    res_digits = res_buf.add(1);

    divisor = i64abs(ival) as uint64;

    if divisor <= PG_UINT64_MAX / NBASE as uint64 {
        let mut carry: uint64 = 0;

        i = 0;
        while i < res_ndigits {
            carry = carry * NBASE as uint64
                + (if i < var_ndigits {
                    *var_digits.add(i as usize) as uint64
                } else {
                    0
                });
            *res_digits.add(i as usize) = (carry / divisor) as NumericDigit;
            carry = carry % divisor;
            i += 1;
        }
    } else {
        let mut carry: uint128 = 0;

        i = 0;
        while i < res_ndigits {
            carry = carry * NBASE as uint128
                + (if i < var_ndigits {
                    *var_digits.add(i as usize) as uint128
                } else {
                    0
                });
            *res_digits.add(i as usize) = (carry / divisor as uint128) as NumericDigit;
            carry = carry % divisor as uint128;
            i += 1;
        }
    }

    digitbuf_free((*result).buf);
    (*result).ndigits = res_ndigits;
    (*result).buf = res_buf;
    (*result).digits = res_digits;
    (*result).weight = res_weight;
    (*result).sign = res_sign;

    if round {
        round_var(result, rscale);
    } else {
        trunc_var(result, rscale);
    }

    strip_var(result);
}

/// select_div_scale() - Default scale selection for division.
unsafe fn select_div_scale(var1: *const NumericVar, var2: *const NumericVar) -> c_int {
    let mut weight1: c_int;
    let mut weight2: c_int;
    let mut qweight: c_int;
    let mut i: c_int;
    let mut firstdigit1: NumericDigit;
    let mut firstdigit2: NumericDigit;
    let mut rscale: c_int;

    weight1 = 0;
    firstdigit1 = 0;
    i = 0;
    while i < (*var1).ndigits {
        firstdigit1 = *(*var1).digits.add(i as usize);
        if firstdigit1 != 0 {
            weight1 = (*var1).weight - i;
            break;
        }
        i += 1;
    }

    weight2 = 0;
    firstdigit2 = 0;
    i = 0;
    while i < (*var2).ndigits {
        firstdigit2 = *(*var2).digits.add(i as usize);
        if firstdigit2 != 0 {
            weight2 = (*var2).weight - i;
            break;
        }
        i += 1;
    }

    qweight = weight1 - weight2;
    if firstdigit1 <= firstdigit2 {
        qweight -= 1;
    }

    rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
    rscale = Max!(rscale, (*var1).dscale);
    rscale = Max!(rscale, (*var2).dscale);
    rscale = Max!(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min!(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    rscale
}

/// mod_var() - Calculate the modulo of two numerics at variable level.
unsafe fn mod_var(var1: *const NumericVar, var2: *const NumericVar, result: *mut NumericVar) {
    let mut tmp: NumericVar = core::mem::zeroed();

    init_var(&mut tmp);

    div_var(var1, var2, &mut tmp, 0, false, true);

    mul_var(var2, &tmp, &mut tmp, (*var2).dscale);

    sub_var(var1, &tmp, result);

    free_var(&mut tmp);
}

/// div_mod_var() - truncated integer quotient and numeric remainder.
unsafe fn div_mod_var(
    var1: *const NumericVar,
    var2: *const NumericVar,
    quot: *mut NumericVar,
    rem: *mut NumericVar,
) {
    let mut q: NumericVar = core::mem::zeroed();
    let mut r: NumericVar = core::mem::zeroed();

    init_var(&mut q);
    init_var(&mut r);

    div_var(var1, var2, &mut q, 0, false, false);

    mul_var(var2, &q, &mut r, (*var2).dscale);
    sub_var(var1, &r, &mut r);

    while r.ndigits != 0 && r.sign != (*var1).sign {
        if (*var1).sign == (*var2).sign {
            sub_var(&q, cvar(&const_one), &mut q);
            add_var(&r, var2, &mut r);
        } else {
            add_var(&q, cvar(&const_one), &mut q);
            sub_var(&r, var2, &mut r);
        }
    }

    while cmp_abs(&r, var2) >= 0 {
        if (*var1).sign == (*var2).sign {
            add_var(&q, cvar(&const_one), &mut q);
            sub_var(&r, var2, &mut r);
        } else {
            sub_var(&q, cvar(&const_one), &mut q);
            add_var(&r, var2, &mut r);
        }
    }

    set_var_from_var(&q, quot);
    set_var_from_var(&r, rem);

    free_var(&mut q);
    free_var(&mut r);
}

/// ceil_var() - smallest integer >= argument.
unsafe fn ceil_var(var: *const NumericVar, result: *mut NumericVar) {
    let mut tmp: NumericVar = core::mem::zeroed();

    init_var(&mut tmp);
    set_var_from_var(var, &mut tmp);

    trunc_var(&mut tmp, 0);

    if (*var).sign == NUMERIC_POS && cmp_var(var, &tmp) != 0 {
        add_var(&tmp, cvar(&const_one), &mut tmp);
    }

    set_var_from_var(&tmp, result);
    free_var(&mut tmp);
}

/// floor_var() - largest integer <= argument.
unsafe fn floor_var(var: *const NumericVar, result: *mut NumericVar) {
    let mut tmp: NumericVar = core::mem::zeroed();

    init_var(&mut tmp);
    set_var_from_var(var, &mut tmp);

    trunc_var(&mut tmp, 0);

    if (*var).sign == NUMERIC_NEG && cmp_var(var, &tmp) != 0 {
        sub_var(&tmp, cvar(&const_one), &mut tmp);
    }

    set_var_from_var(&tmp, result);
    free_var(&mut tmp);
}

/// gcd_var() - greatest common divisor of two numerics at variable level.
unsafe fn gcd_var(var1_in: *const NumericVar, var2_in: *const NumericVar, result: *mut NumericVar) {
    let mut var1 = var1_in;
    let mut var2 = var2_in;
    let res_dscale: c_int;
    let cmp: c_int;
    let mut tmp_arg: NumericVar = core::mem::zeroed();
    let mut modv: NumericVar = core::mem::zeroed();

    res_dscale = Max!((*var1).dscale, (*var2).dscale);

    cmp = cmp_abs(var1, var2);
    if cmp < 0 {
        let tmp = var1;
        var1 = var2;
        var2 = tmp;
    }

    if cmp == 0 || (*var2).ndigits == 0 {
        set_var_from_var(var1, result);
        (*result).sign = NUMERIC_POS;
        (*result).dscale = res_dscale;
        return;
    }

    init_var(&mut tmp_arg);
    init_var(&mut modv);

    set_var_from_var(var1, &mut tmp_arg);
    set_var_from_var(var2, result);

    loop {
        CHECK_FOR_INTERRUPTS();

        mod_var(&tmp_arg, result, &mut modv);
        if modv.ndigits == 0 {
            break;
        }
        set_var_from_var(result, &mut tmp_arg);
        set_var_from_var(&modv, result);
    }
    (*result).sign = NUMERIC_POS;
    (*result).dscale = res_dscale;

    free_var(&mut tmp_arg);
    free_var(&mut modv);
}


/// sqrt_var() - Compute the square root using Karatsuba Square Root.
unsafe fn sqrt_var(arg: *const NumericVar, result: *mut NumericVar, rscale: c_int) {
    let stat: c_int;
    let res_weight: c_int;
    let mut res_ndigits: c_int;
    let mut src_ndigits: c_int;
    let mut step: c_int;
    let mut ndigits: [c_int; 32] = [0; 32];
    let mut blen: c_int;
    let mut arg_int64: int64;
    let mut src_idx: c_int;
    let mut s_int64: int64;
    let mut r_int64: int64;
    let mut s_var: NumericVar = core::mem::zeroed();
    let mut r_var: NumericVar = core::mem::zeroed();
    let mut a0_var: NumericVar = core::mem::zeroed();
    let mut a1_var: NumericVar = core::mem::zeroed();
    let mut q_var: NumericVar = core::mem::zeroed();
    let mut u_var: NumericVar = core::mem::zeroed();

    stat = cmp_var(arg, cvar(&const_zero));
    if stat == 0 {
        zero_var(result);
        (*result).dscale = rscale;
        return;
    }

    if stat < 0 {
        ereport!(ERROR, errmsg!("cannot take square root of a negative number"));
    }

    init_var(&mut s_var);
    init_var(&mut r_var);
    init_var(&mut a0_var);
    init_var(&mut a1_var);
    init_var(&mut q_var);
    init_var(&mut u_var);

    if (*arg).weight >= 0 {
        res_weight = (*arg).weight / 2;
    } else {
        res_weight = -((-(*arg).weight - 1) / 2 + 1);
    }

    if rscale + 1 >= 0 {
        res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS) / DEC_DIGITS;
    } else {
        res_ndigits = res_weight + 1 - (-rscale - 1) / DEC_DIGITS;
    }
    res_ndigits = Max!(res_ndigits, 1);

    src_ndigits = (*arg).weight + 1 + (res_ndigits - res_weight - 1) * 2;
    src_ndigits = Max!(src_ndigits, 1);

    step = 0;
    loop {
        ndigits[step as usize] = src_ndigits;
        if !(ndigits[step as usize] > 4) {
            break;
        }
        blen = src_ndigits / 4;
        if blen * 4 == src_ndigits && (*(*arg).digits.add(0) as c_int) < NBASE / 4 {
            blen -= 1;
        }
        src_ndigits -= 2 * blen;
        step += 1;
    }

    arg_int64 = *(*arg).digits.add(0) as int64;
    src_idx = 1;
    while src_idx < src_ndigits {
        arg_int64 *= NBASE as int64;
        if src_idx < (*arg).ndigits {
            arg_int64 += *(*arg).digits.add(src_idx as usize) as int64;
        }
        src_idx += 1;
    }

    s_int64 = sqrt(arg_int64 as f64) as int64;
    r_int64 = arg_int64 - s_int64 * s_int64;

    while r_int64 < 0 || r_int64 > 2 * s_int64 {
        s_int64 = (s_int64 + arg_int64 / s_int64) / 2;
        r_int64 = arg_int64 - s_int64 * s_int64;
    }

    step -= 1;
    while step >= 0 && {
        src_ndigits = ndigits[step as usize];
        src_ndigits <= 8
    } {
        let b: c_int;
        let mut a0: c_int;
        let mut a1: c_int;
        let mut i: c_int;
        let numer: int64;
        let denom: int64;
        let q: int64;
        let u: int64;
        let mut bb: c_int = 1;

        blen = (src_ndigits - src_idx) / 2;

        a0 = 0;
        a1 = 0;

        i = 0;
        while i < blen {
            bb *= NBASE;
            a1 *= NBASE;
            if src_idx < (*arg).ndigits {
                a1 += *(*arg).digits.add(src_idx as usize) as c_int;
            }
            i += 1;
            src_idx += 1;
        }
        b = bb;

        i = 0;
        while i < blen {
            a0 *= NBASE;
            if src_idx < (*arg).ndigits {
                a0 += *(*arg).digits.add(src_idx as usize) as c_int;
            }
            i += 1;
            src_idx += 1;
        }

        numer = r_int64 * b as int64 + a1 as int64;
        denom = 2 * s_int64;
        q = numer / denom;
        u = numer - q * denom;

        s_int64 = s_int64 * b as int64 + q;
        r_int64 = u * b as int64 + a0 as int64 - q * q;

        if r_int64 < 0 {
            r_int64 += s_int64;
            s_int64 -= 1;
            r_int64 += s_int64;
        }

        Assert!(src_idx == src_ndigits);
        step -= 1;
    }

    // int128 path
    if step >= 0 {
        let mut s_int128: int128;
        let mut r_int128: int128;

        s_int128 = s_int64 as int128;
        r_int128 = r_int64 as int128;

        while step >= 0 && {
            src_ndigits = ndigits[step as usize];
            src_ndigits <= 16
        } {
            let b: int64;
            let mut a0: int64;
            let mut a1: int64;
            let mut i: int64;
            let numer: int128;
            let denom: int128;
            let q: int128;
            let u: int128;
            let mut bb: int64 = 1;

            blen = (src_ndigits - src_idx) / 2;

            a0 = 0;
            a1 = 0;

            i = 0;
            while i < blen as int64 {
                bb *= NBASE as int64;
                a1 *= NBASE as int64;
                if src_idx < (*arg).ndigits {
                    a1 += *(*arg).digits.add(src_idx as usize) as int64;
                }
                i += 1;
                src_idx += 1;
            }
            b = bb;

            i = 0;
            while i < blen as int64 {
                a0 *= NBASE as int64;
                if src_idx < (*arg).ndigits {
                    a0 += *(*arg).digits.add(src_idx as usize) as int64;
                }
                i += 1;
                src_idx += 1;
            }

            numer = r_int128 * b as int128 + a1 as int128;
            denom = 2 * s_int128;
            q = numer / denom;
            u = numer - q * denom;

            s_int128 = s_int128 * b as int128 + q;
            r_int128 = u * b as int128 + a0 as int128 - q * q;

            if r_int128 < 0 {
                r_int128 += s_int128;
                s_int128 -= 1;
                r_int128 += s_int128;
            }

            Assert!(src_idx == src_ndigits);
            step -= 1;
        }

        int128_to_numericvar(s_int128, &mut s_var);
        if step >= 0 {
            int128_to_numericvar(r_int128, &mut r_var);
        }
    } else {
        int64_to_numericvar(s_int64, &mut s_var);
    }

    while step >= 0 {
        let mut tmp_len: c_int;

        src_ndigits = ndigits[step as usize];
        blen = (src_ndigits - src_idx) / 2;

        if src_idx < (*arg).ndigits {
            tmp_len = Min!(blen, (*arg).ndigits - src_idx);
            alloc_var(&mut a1_var, tmp_len);
            memcpy(
                a1_var.digits as *mut c_void,
                (*arg).digits.add(src_idx as usize) as *const c_void,
                tmp_len as usize * core::mem::size_of::<NumericDigit>(),
            );
            a1_var.weight = blen - 1;
            a1_var.sign = NUMERIC_POS;
            a1_var.dscale = 0;
            strip_var(&mut a1_var);
        } else {
            zero_var(&mut a1_var);
            a1_var.dscale = 0;
        }
        src_idx += blen;

        if src_idx < (*arg).ndigits {
            tmp_len = Min!(blen, (*arg).ndigits - src_idx);
            alloc_var(&mut a0_var, tmp_len);
            memcpy(
                a0_var.digits as *mut c_void,
                (*arg).digits.add(src_idx as usize) as *const c_void,
                tmp_len as usize * core::mem::size_of::<NumericDigit>(),
            );
            a0_var.weight = blen - 1;
            a0_var.sign = NUMERIC_POS;
            a0_var.dscale = 0;
            strip_var(&mut a0_var);
        } else {
            zero_var(&mut a0_var);
            a0_var.dscale = 0;
        }
        src_idx += blen;

        set_var_from_var(&r_var, &mut q_var);
        q_var.weight += blen;
        add_var(&q_var, &a1_var, &mut q_var);
        add_var(&s_var, &s_var, &mut u_var);
        div_mod_var(&q_var, &u_var, &mut q_var, &mut u_var);

        s_var.weight += blen;
        add_var(&s_var, &q_var, &mut s_var);

        u_var.weight += blen;
        add_var(&u_var, &a0_var, &mut u_var);
        mul_var(&q_var, &q_var, &mut q_var, 0);

        if step > 0 {
            sub_var(&u_var, &q_var, &mut r_var);
            if r_var.sign == NUMERIC_NEG {
                add_var(&r_var, &s_var, &mut r_var);
                sub_var(&s_var, cvar(&const_one), &mut s_var);
                add_var(&r_var, &s_var, &mut r_var);
            }
        } else {
            if cmp_var(&u_var, &q_var) < 0 {
                sub_var(&s_var, cvar(&const_one), &mut s_var);
            }
        }

        Assert!(src_idx == src_ndigits);
        step -= 1;
    }

    set_var_from_var(&s_var, result);
    (*result).weight = res_weight;
    (*result).sign = NUMERIC_POS;

    round_var(result, rscale);

    strip_var(result);

    free_var(&mut s_var);
    free_var(&mut r_var);
    free_var(&mut a0_var);
    free_var(&mut a1_var);
    free_var(&mut q_var);
    free_var(&mut u_var);
}

/// exp_var() - Raise e to the power of x.
unsafe fn exp_var(arg: *const NumericVar, result: *mut NumericVar, rscale: c_int) {
    let mut x: NumericVar = core::mem::zeroed();
    let mut elem: NumericVar = core::mem::zeroed();
    let mut ni: c_int;
    let mut val: f64;
    let dweight: c_int;
    let mut ndiv2: c_int;
    let sig_digits: c_int;
    let mut local_rscale: c_int;

    init_var(&mut x);
    init_var(&mut elem);

    set_var_from_var(arg, &mut x);

    val = numericvar_to_double_no_overflow(&x);

    if fabs(val) >= (NUMERIC_MAX_RESULT_SCALE * 3) as f64 {
        if val > 0.0 {
            ereport!(ERROR, errmsg!("value overflows numeric format"));
        }
        zero_var(result);
        (*result).dscale = rscale;
        return;
    }

    dweight = (val * 0.434294481903252) as c_int;

    if fabs(val) > 0.01 {
        ndiv2 = 1;
        val /= 2.0;

        while fabs(val) > 0.01 {
            ndiv2 += 1;
            val /= 2.0;
        }

        local_rscale = x.dscale + ndiv2;
        div_var_int(&x, 1 << ndiv2, 0, &mut x, local_rscale, true);
    } else {
        ndiv2 = 0;
    }

    let sig_digits_tmp = 1 + dweight + rscale + (ndiv2 as f64 * 0.301029995663981) as c_int;
    let sig_digits = Max!(sig_digits_tmp, 0) + 8;
    let _ = sig_digits;

    local_rscale = sig_digits - 1;

    add_var(cvar(&const_one), &x, result);

    mul_var(&x, &x, &mut elem, local_rscale);
    ni = 2;
    div_var_int(&elem, ni, 0, &mut elem, local_rscale, true);

    while elem.ndigits != 0 {
        add_var(result, &elem, result);

        mul_var(&elem, &x, &mut elem, local_rscale);
        ni += 1;
        div_var_int(&elem, ni, 0, &mut elem, local_rscale, true);
    }

    while ndiv2 > 0 {
        ndiv2 -= 1;
        local_rscale = sig_digits - (*result).weight * 2 * DEC_DIGITS;
        local_rscale = Max!(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);
        mul_var(result, result, result, local_rscale);
    }

    round_var(result, rscale);

    free_var(&mut x);
    free_var(&mut elem);
}

/// estimate_ln_dweight() - estimate log10(abs(ln(var))).
unsafe fn estimate_ln_dweight(var: *const NumericVar) -> c_int {
    let ln_dweight: c_int;

    if (*var).sign != NUMERIC_POS {
        return 0;
    }

    if cmp_var(var, cvar(&const_zero_point_nine)) >= 0
        && cmp_var(var, cvar(&const_one_point_one)) <= 0
    {
        let mut x: NumericVar = core::mem::zeroed();

        init_var(&mut x);
        sub_var(var, cvar(&const_one), &mut x);

        if x.ndigits > 0 {
            ln_dweight = x.weight * DEC_DIGITS + log10(*x.digits.add(0) as f64) as c_int;
        } else {
            ln_dweight = 0;
        }

        free_var(&mut x);
    } else {
        if (*var).ndigits > 0 {
            let mut digits: c_int;
            let mut dweight: c_int;
            let ln_var: f64;

            digits = *(*var).digits.add(0) as c_int;
            dweight = (*var).weight * DEC_DIGITS;

            if (*var).ndigits > 1 {
                digits = digits * NBASE + *(*var).digits.add(1) as c_int;
                dweight -= DEC_DIGITS;
            }

            ln_var = log(digits as f64) + dweight as f64 * 2.302585092994046;
            ln_dweight = log10(fabs(ln_var)) as c_int;
        } else {
            ln_dweight = 0;
        }
    }

    ln_dweight
}

/// ln_var() - Compute the natural log of x.
unsafe fn ln_var(arg: *const NumericVar, result: *mut NumericVar, rscale: c_int) {
    let mut x: NumericVar = core::mem::zeroed();
    let mut xx: NumericVar = core::mem::zeroed();
    let mut ni: c_int;
    let mut elem: NumericVar = core::mem::zeroed();
    let mut fact: NumericVar = core::mem::zeroed();
    let mut nsqrt: c_int;
    let mut local_rscale: c_int;
    let cmp: c_int;

    cmp = cmp_var(arg, cvar(&const_zero));
    if cmp == 0 {
        ereport!(ERROR, errmsg!("cannot take logarithm of zero"));
    } else if cmp < 0 {
        ereport!(ERROR, errmsg!("cannot take logarithm of a negative number"));
    }

    init_var(&mut x);
    init_var(&mut xx);
    init_var(&mut elem);
    init_var(&mut fact);

    set_var_from_var(arg, &mut x);
    set_var_from_var(cvar(&const_two), &mut fact);

    nsqrt = 0;
    while cmp_var(&x, cvar(&const_zero_point_nine)) <= 0 {
        local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
        sqrt_var(&x, &mut x, local_rscale);
        mul_var(&fact, cvar(&const_two), &mut fact, 0);
        nsqrt += 1;
    }
    while cmp_var(&x, cvar(&const_one_point_one)) >= 0 {
        local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
        sqrt_var(&x, &mut x, local_rscale);
        mul_var(&fact, cvar(&const_two), &mut fact, 0);
        nsqrt += 1;
    }

    local_rscale = rscale + ((nsqrt + 1) as f64 * 0.301029995663981) as c_int + 8;

    sub_var(&x, cvar(&const_one), result);
    add_var(&x, cvar(&const_one), &mut elem);
    div_var(result, &elem, result, local_rscale, true, false);
    set_var_from_var(result, &mut xx);
    mul_var(result, result, &mut x, local_rscale);

    ni = 1;

    loop {
        ni += 2;
        mul_var(&xx, &x, &mut xx, local_rscale);
        div_var_int(&xx, ni, 0, &mut elem, local_rscale, true);

        if elem.ndigits == 0 {
            break;
        }

        add_var(result, &elem, result);

        if elem.weight < ((*result).weight - local_rscale * 2 / DEC_DIGITS) {
            break;
        }
    }

    mul_var(result, &fact, result, rscale);

    free_var(&mut x);
    free_var(&mut xx);
    free_var(&mut elem);
    free_var(&mut fact);
}

/// log_var() - Compute the logarithm of num in a given base.
unsafe fn log_var(base: *const NumericVar, num: *const NumericVar, result: *mut NumericVar) {
    let mut ln_base: NumericVar = core::mem::zeroed();
    let mut ln_num: NumericVar = core::mem::zeroed();
    let ln_base_dweight: c_int;
    let ln_num_dweight: c_int;
    let result_dweight: c_int;
    let mut rscale: c_int;
    let mut ln_base_rscale: c_int;
    let mut ln_num_rscale: c_int;

    init_var(&mut ln_base);
    init_var(&mut ln_num);

    ln_base_dweight = estimate_ln_dweight(base);
    ln_num_dweight = estimate_ln_dweight(num);
    result_dweight = ln_num_dweight - ln_base_dweight;

    rscale = NUMERIC_MIN_SIG_DIGITS - result_dweight;
    rscale = Max!(rscale, (*base).dscale);
    rscale = Max!(rscale, (*num).dscale);
    rscale = Max!(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min!(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    ln_base_rscale = rscale + result_dweight - ln_base_dweight + 8;
    ln_base_rscale = Max!(ln_base_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    ln_num_rscale = rscale + result_dweight - ln_num_dweight + 8;
    ln_num_rscale = Max!(ln_num_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    ln_var(base, &mut ln_base, ln_base_rscale);
    ln_var(num, &mut ln_num, ln_num_rscale);

    div_var(&ln_num, &ln_base, result, rscale, true, false);

    free_var(&mut ln_num);
    free_var(&mut ln_base);
}


/// power_var() - Raise base to the power of exp.
unsafe fn power_var(base_in: *const NumericVar, exp: *const NumericVar, result: *mut NumericVar) {
    let mut base = base_in;
    let res_sign: c_int;
    let mut abs_base: NumericVar = core::mem::zeroed();
    let mut ln_base: NumericVar = core::mem::zeroed();
    let mut ln_num: NumericVar = core::mem::zeroed();
    let ln_dweight: c_int;
    let mut rscale: c_int;
    let sig_digits: c_int;
    let mut local_rscale: c_int;
    let mut val: f64;

    if (*exp).ndigits == 0 || (*exp).ndigits <= (*exp).weight + 1 {
        let mut expval64: int64 = 0;

        if numericvar_to_int64(exp, &mut expval64) {
            if expval64 >= PG_INT32_MIN as int64 && expval64 <= PG_INT32_MAX as int64 {
                power_var_int(base, expval64 as c_int, (*exp).dscale, result);
                return;
            }
        }
    }

    if cmp_var(base, cvar(&const_zero)) == 0 {
        set_var_from_var(cvar(&const_zero), result);
        (*result).dscale = NUMERIC_MIN_SIG_DIGITS;
        return;
    }

    init_var(&mut abs_base);
    init_var(&mut ln_base);
    init_var(&mut ln_num);

    if (*base).sign == NUMERIC_NEG {
        if (*exp).ndigits > 0 && (*exp).ndigits > (*exp).weight + 1 {
            ereport!(
                ERROR,
                errmsg!("a negative number raised to a non-integer power yields a complex result")
            );
        }

        if (*exp).ndigits > 0
            && (*exp).ndigits == (*exp).weight + 1
            && (*(*exp).digits.add(((*exp).ndigits - 1) as usize) & 1) != 0
        {
            res_sign = NUMERIC_NEG;
        } else {
            res_sign = NUMERIC_POS;
        }

        set_var_from_var(base, &mut abs_base);
        abs_base.sign = NUMERIC_POS;
        base = &abs_base;
    } else {
        res_sign = NUMERIC_POS;
    }

    ln_dweight = estimate_ln_dweight(base);

    local_rscale = 8 - ln_dweight;
    local_rscale = Max!(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    ln_var(base, &mut ln_base, local_rscale);

    mul_var(&ln_base, exp, &mut ln_num, local_rscale);

    val = numericvar_to_double_no_overflow(&ln_num);

    if fabs(val) > NUMERIC_MAX_RESULT_SCALE as f64 * 3.01 {
        if val > 0.0 {
            ereport!(ERROR, errmsg!("value overflows numeric format"));
        }
        zero_var(result);
        (*result).dscale = NUMERIC_MAX_DISPLAY_SCALE;
        return;
    }

    val *= 0.434294481903252;

    rscale = NUMERIC_MIN_SIG_DIGITS - val as c_int;
    rscale = Max!(rscale, (*base).dscale);
    rscale = Max!(rscale, (*exp).dscale);
    rscale = Max!(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min!(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    sig_digits = rscale + val as c_int;
    let sig_digits = Max!(sig_digits, 0);

    local_rscale = sig_digits - ln_dweight + 8;
    local_rscale = Max!(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    ln_var(base, &mut ln_base, local_rscale);

    mul_var(&ln_base, exp, &mut ln_num, local_rscale);

    exp_var(&ln_num, result, rscale);

    if res_sign == NUMERIC_NEG && (*result).ndigits > 0 {
        (*result).sign = NUMERIC_NEG;
    }

    free_var(&mut ln_num);
    free_var(&mut ln_base);
    free_var(&mut abs_base);
}

/// power_var_int() - Raise base to an integer power.
unsafe fn power_var_int(
    base: *const NumericVar,
    exp: c_int,
    exp_dscale: c_int,
    result: *mut NumericVar,
) {
    let mut f: f64;
    let mut p: c_int;
    let mut i: c_int;
    let mut rscale: c_int;
    let mut sig_digits: c_int;
    let mut mask: c_uint;
    let mut neg: bool;
    let mut base_prod: NumericVar = core::mem::zeroed();
    let mut local_rscale: c_int;

    if (*base).ndigits != 0 {
        f = *(*base).digits.add(0) as f64;
        p = (*base).weight * DEC_DIGITS;

        i = 1;
        while i < (*base).ndigits && i * DEC_DIGITS < 16 {
            f = f * NBASE as f64 + *(*base).digits.add(i as usize) as f64;
            p -= DEC_DIGITS;
            i += 1;
        }

        f = exp as f64 * (log10(f) + p as f64);
    } else {
        f = 0.0;
    }

    if f > ((NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS) as f64 {
        ereport!(ERROR, errmsg!("value overflows numeric format"));
    }
    if f + 1.0 < (-NUMERIC_MAX_DISPLAY_SCALE) as f64 {
        zero_var(result);
        (*result).dscale = NUMERIC_MAX_DISPLAY_SCALE;
        return;
    }

    rscale = NUMERIC_MIN_SIG_DIGITS - f as c_int;
    rscale = Max!(rscale, (*base).dscale);
    rscale = Max!(rscale, exp_dscale);
    rscale = Max!(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min!(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    match exp {
        0 => {
            set_var_from_var(cvar(&const_one), result);
            (*result).dscale = rscale;
            return;
        }
        1 => {
            set_var_from_var(base, result);
            round_var(result, rscale);
            return;
        }
        -1 => {
            div_var(cvar(&const_one), base, result, rscale, true, true);
            return;
        }
        2 => {
            mul_var(base, base, result, rscale);
            return;
        }
        _ => {}
    }

    if (*base).ndigits == 0 {
        if exp < 0 {
            ereport!(ERROR, errmsg!("division by zero"));
        }
        zero_var(result);
        (*result).dscale = rscale;
        return;
    }

    sig_digits = 1 + rscale + f as c_int;

    sig_digits += log(fabs(exp as f64)) as c_int + 8;

    neg = exp < 0;
    mask = pg_abs_s32(exp);

    init_var(&mut base_prod);
    set_var_from_var(base, &mut base_prod);

    if mask & 1 != 0 {
        set_var_from_var(base, result);
    } else {
        set_var_from_var(cvar(&const_one), result);
    }

    loop {
        mask >>= 1;
        if !(mask > 0) {
            break;
        }
        local_rscale = sig_digits - 2 * base_prod.weight * DEC_DIGITS;
        local_rscale = Min!(local_rscale, 2 * base_prod.dscale);
        local_rscale = Max!(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

        mul_var(&base_prod, &base_prod, &mut base_prod, local_rscale);

        if mask & 1 != 0 {
            local_rscale = sig_digits - (base_prod.weight + (*result).weight) * DEC_DIGITS;
            local_rscale = Min!(local_rscale, base_prod.dscale + (*result).dscale);
            local_rscale = Max!(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

            mul_var(&base_prod, result, result, local_rscale);
        }

        if base_prod.weight > NUMERIC_WEIGHT_MAX || (*result).weight > NUMERIC_WEIGHT_MAX {
            if !neg {
                ereport!(ERROR, errmsg!("value overflows numeric format"));
            }
            zero_var(result);
            neg = false;
            break;
        }
    }

    free_var(&mut base_prod);

    if neg {
        div_var(cvar(&const_one), result, result, rscale, true, false);
    } else {
        round_var(result, rscale);
    }
}

/// power_ten_int() - Raise ten to the power of exp (no overflow/rounding).
unsafe fn power_ten_int(exp_in: c_int, result: *mut NumericVar) {
    let mut exp = exp_in;
    set_var_from_var(cvar(&const_one), result);

    (*result).dscale = if exp < 0 { -exp } else { 0 };

    if exp >= 0 {
        (*result).weight = exp / DEC_DIGITS;
    } else {
        (*result).weight = (exp + 1) / DEC_DIGITS - 1;
    }

    exp -= (*result).weight * DEC_DIGITS;

    while exp > 0 {
        exp -= 1;
        *(*result).digits.add(0) *= 10;
    }
}

/// random_var() - return a random value in the range [rmin, rmax].
unsafe fn random_var(
    state: *mut pg_prng_state,
    rmin: *const NumericVar,
    rmax: *const NumericVar,
    result: *mut NumericVar,
) {
    let rscale: c_int;
    let mut rlen: NumericVar = core::mem::zeroed();
    let res_ndigits: c_int;
    let n: c_int;
    let mut pow10: c_int;
    let mut i: c_int;
    let mut rlen64: uint64;
    let mut rlen64_ndigits: c_int;

    rscale = Max!((*rmin).dscale, (*rmax).dscale);

    init_var(&mut rlen);
    sub_var(rmax, rmin, &mut rlen);

    if rlen.sign == NUMERIC_NEG {
        ereport!(
            ERROR,
            errmsg!("lower bound must be less than or equal to upper bound")
        );
    }

    if rlen.ndigits == 0 {
        set_var_from_var(rmin, result);
        (*result).dscale = rscale;
        free_var(&mut rlen);
        return;
    }

    res_ndigits = rlen.weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;

    n = ((rscale + DEC_DIGITS - 1) / DEC_DIGITS) * DEC_DIGITS - rscale;
    pow10 = 1;
    i = 0;
    while i < n {
        pow10 *= 10;
        i += 1;
    }

    rlen64 = *rlen.digits.add(0) as uint64;
    rlen64_ndigits = 1;
    while rlen64_ndigits < res_ndigits && rlen64_ndigits < 4 {
        rlen64 *= NBASE as uint64;
        if rlen64_ndigits < rlen.ndigits {
            rlen64 += *rlen.digits.add(rlen64_ndigits as usize) as uint64;
        }
        rlen64_ndigits += 1;
    }

    loop {
        let res_digits: *mut NumericDigit;
        let mut rand: uint64;
        let mut whole_ndigits: c_int;

        alloc_var(result, res_ndigits);
        (*result).sign = NUMERIC_POS;
        (*result).weight = rlen.weight;
        (*result).dscale = rscale;
        res_digits = (*result).digits;

        if rlen64_ndigits == res_ndigits && pow10 != 1 {
            rand = pg_prng_uint64_range(state, 0, rlen64 / pow10 as uint64) * pow10 as uint64;
        } else {
            rand = pg_prng_uint64_range(state, 0, rlen64);
        }

        i = rlen64_ndigits - 1;
        while i >= 0 {
            *res_digits.add(i as usize) = (rand % NBASE as uint64) as NumericDigit;
            rand = rand / NBASE as uint64;
            i -= 1;
        }

        whole_ndigits = res_ndigits;
        if pow10 != 1 {
            whole_ndigits -= 1;
        }

        i = rlen64_ndigits;
        while i < whole_ndigits - 3 {
            rand = pg_prng_uint64_range(
                state,
                0,
                NBASE as uint64 * NBASE as uint64 * NBASE as uint64 * NBASE as uint64 - 1,
            );
            *res_digits.add(i as usize) = (rand % NBASE as uint64) as NumericDigit;
            i += 1;
            rand = rand / NBASE as uint64;
            *res_digits.add(i as usize) = (rand % NBASE as uint64) as NumericDigit;
            i += 1;
            rand = rand / NBASE as uint64;
            *res_digits.add(i as usize) = (rand % NBASE as uint64) as NumericDigit;
            i += 1;
            rand = rand / NBASE as uint64;
            *res_digits.add(i as usize) = rand as NumericDigit;
            i += 1;
        }

        while i < whole_ndigits {
            rand = pg_prng_uint64_range(state, 0, NBASE as uint64 - 1);
            *res_digits.add(i as usize) = rand as NumericDigit;
            i += 1;
        }

        if i < res_ndigits {
            rand = pg_prng_uint64_range(state, 0, NBASE as uint64 / pow10 as uint64 - 1)
                * pow10 as uint64;
            *res_digits.add(i as usize) = rand as NumericDigit;
        }

        strip_var(result);

        if !(cmp_var(result, &rlen) > 0) {
            break;
        }
    }

    add_var(result, rmin, result);

    free_var(&mut rlen);
}


// ----------------------------------------------------------------------
//
// Following are the lowest level functions that operate unsigned
// on the variable level
//
// ----------------------------------------------------------------------

/// cmp_abs() - Compare the absolute values of var1 and var2.
unsafe fn cmp_abs(var1: *const NumericVar, var2: *const NumericVar) -> c_int {
    cmp_abs_common(
        (*var1).digits,
        (*var1).ndigits,
        (*var1).weight,
        (*var2).digits,
        (*var2).ndigits,
        (*var2).weight,
    )
}

/// cmp_abs_common() - Main routine of cmp_abs().
unsafe fn cmp_abs_common(
    var1digits: *const NumericDigit,
    var1ndigits: c_int,
    mut var1weight: c_int,
    var2digits: *const NumericDigit,
    var2ndigits: c_int,
    mut var2weight: c_int,
) -> c_int {
    let mut i1: c_int = 0;
    let mut i2: c_int = 0;

    while var1weight > var2weight && i1 < var1ndigits {
        if *var1digits.add(i1 as usize) != 0 {
            return 1;
        }
        i1 += 1;
        var1weight -= 1;
    }
    while var2weight > var1weight && i2 < var2ndigits {
        if *var2digits.add(i2 as usize) != 0 {
            return -1;
        }
        i2 += 1;
        var2weight -= 1;
    }

    if var1weight == var2weight {
        while i1 < var1ndigits && i2 < var2ndigits {
            let stat: c_int = *var1digits.add(i1 as usize) as c_int
                - *var2digits.add(i2 as usize) as c_int;
            i1 += 1;
            i2 += 1;

            if stat != 0 {
                if stat > 0 {
                    return 1;
                }
                return -1;
            }
        }
    }

    while i1 < var1ndigits {
        if *var1digits.add(i1 as usize) != 0 {
            return 1;
        }
        i1 += 1;
    }
    while i2 < var2ndigits {
        if *var2digits.add(i2 as usize) != 0 {
            return -1;
        }
        i2 += 1;
    }

    0
}

/// add_abs() - Add the absolute values of two variables into result.
unsafe fn add_abs(var1: *const NumericVar, var2: *const NumericVar, result: *mut NumericVar) {
    let res_buf: *mut NumericDigit;
    let res_digits: *mut NumericDigit;
    let mut res_ndigits: c_int;
    let res_weight: c_int;
    let res_rscale: c_int;
    let rscale1: c_int;
    let rscale2: c_int;
    let res_dscale: c_int;
    let mut i: c_int;
    let mut i1: c_int;
    let mut i2: c_int;
    let mut carry: c_int = 0;

    let var1ndigits = (*var1).ndigits;
    let var2ndigits = (*var2).ndigits;
    let var1digits = (*var1).digits;
    let var2digits = (*var2).digits;

    res_weight = Max!((*var1).weight, (*var2).weight) + 1;

    res_dscale = Max!((*var1).dscale, (*var2).dscale);

    rscale1 = (*var1).ndigits - (*var1).weight - 1;
    rscale2 = (*var2).ndigits - (*var2).weight - 1;
    res_rscale = Max!(rscale1, rscale2);

    res_ndigits = res_rscale + res_weight + 1;
    if res_ndigits <= 0 {
        res_ndigits = 1;
    }

    res_buf = digitbuf_alloc(res_ndigits + 1);
    *res_buf.add(0) = 0;
    res_digits = res_buf.add(1);

    i1 = res_rscale + (*var1).weight + 1;
    i2 = res_rscale + (*var2).weight + 1;
    i = res_ndigits - 1;
    while i >= 0 {
        i1 -= 1;
        i2 -= 1;
        if i1 >= 0 && i1 < var1ndigits {
            carry += *var1digits.add(i1 as usize) as c_int;
        }
        if i2 >= 0 && i2 < var2ndigits {
            carry += *var2digits.add(i2 as usize) as c_int;
        }

        if carry >= NBASE {
            *res_digits.add(i as usize) = (carry - NBASE) as NumericDigit;
            carry = 1;
        } else {
            *res_digits.add(i as usize) = carry as NumericDigit;
            carry = 0;
        }
        i -= 1;
    }

    Assert!(carry == 0);

    digitbuf_free((*result).buf);
    (*result).ndigits = res_ndigits;
    (*result).buf = res_buf;
    (*result).digits = res_digits;
    (*result).weight = res_weight;
    (*result).dscale = res_dscale;

    strip_var(result);
}

/// sub_abs() - Subtract abs(var2) from abs(var1); requires abs(var1) >= abs(var2).
unsafe fn sub_abs(var1: *const NumericVar, var2: *const NumericVar, result: *mut NumericVar) {
    let res_buf: *mut NumericDigit;
    let res_digits: *mut NumericDigit;
    let mut res_ndigits: c_int;
    let res_weight: c_int;
    let res_rscale: c_int;
    let rscale1: c_int;
    let rscale2: c_int;
    let res_dscale: c_int;
    let mut i: c_int;
    let mut i1: c_int;
    let mut i2: c_int;
    let mut borrow: c_int = 0;

    let var1ndigits = (*var1).ndigits;
    let var2ndigits = (*var2).ndigits;
    let var1digits = (*var1).digits;
    let var2digits = (*var2).digits;

    res_weight = (*var1).weight;

    res_dscale = Max!((*var1).dscale, (*var2).dscale);

    rscale1 = (*var1).ndigits - (*var1).weight - 1;
    rscale2 = (*var2).ndigits - (*var2).weight - 1;
    res_rscale = Max!(rscale1, rscale2);

    res_ndigits = res_rscale + res_weight + 1;
    if res_ndigits <= 0 {
        res_ndigits = 1;
    }

    res_buf = digitbuf_alloc(res_ndigits + 1);
    *res_buf.add(0) = 0;
    res_digits = res_buf.add(1);

    i1 = res_rscale + (*var1).weight + 1;
    i2 = res_rscale + (*var2).weight + 1;
    i = res_ndigits - 1;
    while i >= 0 {
        i1 -= 1;
        i2 -= 1;
        if i1 >= 0 && i1 < var1ndigits {
            borrow += *var1digits.add(i1 as usize) as c_int;
        }
        if i2 >= 0 && i2 < var2ndigits {
            borrow -= *var2digits.add(i2 as usize) as c_int;
        }

        if borrow < 0 {
            *res_digits.add(i as usize) = (borrow + NBASE) as NumericDigit;
            borrow = -1;
        } else {
            *res_digits.add(i as usize) = borrow as NumericDigit;
            borrow = 0;
        }
        i -= 1;
    }

    Assert!(borrow == 0);

    digitbuf_free((*result).buf);
    (*result).ndigits = res_ndigits;
    (*result).buf = res_buf;
    (*result).digits = res_digits;
    (*result).weight = res_weight;
    (*result).dscale = res_dscale;

    strip_var(result);
}

/// round_var - Round to no more than rscale decimal digits after the point.
unsafe fn round_var(var: *mut NumericVar, rscale: c_int) {
    let digits: *mut NumericDigit = (*var).digits;
    let mut di: c_int;
    let mut ndigits: c_int;
    let mut carry: c_int;

    (*var).dscale = rscale;

    di = ((*var).weight + 1) * DEC_DIGITS + rscale;

    if di < 0 {
        (*var).ndigits = 0;
        (*var).weight = 0;
        (*var).sign = NUMERIC_POS;
    } else {
        ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

        di %= DEC_DIGITS;

        if ndigits < (*var).ndigits || (ndigits == (*var).ndigits && di > 0) {
            (*var).ndigits = ndigits;

            if di == 0 {
                carry = if *digits.add(ndigits as usize) as c_int >= HALF_NBASE {
                    1
                } else {
                    0
                };
            } else {
                let extra: c_int;
                let mut pow10: c_int;

                pow10 = round_powers[di as usize];
                ndigits -= 1;
                extra = *digits.add(ndigits as usize) as c_int % pow10;
                *digits.add(ndigits as usize) -= extra as NumericDigit;
                carry = 0;
                if extra >= pow10 / 2 {
                    pow10 += *digits.add(ndigits as usize) as c_int;
                    if pow10 >= NBASE {
                        pow10 -= NBASE;
                        carry = 1;
                    }
                    *digits.add(ndigits as usize) = pow10 as NumericDigit;
                }
            }

            while carry != 0 {
                ndigits -= 1;
                carry += *digits.add(ndigits as usize) as c_int;
                if carry >= NBASE {
                    *digits.add(ndigits as usize) = (carry - NBASE) as NumericDigit;
                    carry = 1;
                } else {
                    *digits.add(ndigits as usize) = carry as NumericDigit;
                    carry = 0;
                }
            }

            if ndigits < 0 {
                Assert!(ndigits == -1);
                Assert!((*var).digits > (*var).buf);
                (*var).digits = (*var).digits.sub(1);
                (*var).ndigits += 1;
                (*var).weight += 1;
            }
        }
    }
}

/// trunc_var - Truncate (towards zero) at rscale decimal digits.
unsafe fn trunc_var(var: *mut NumericVar, rscale: c_int) {
    let mut di: c_int;
    let mut ndigits: c_int;

    (*var).dscale = rscale;

    di = ((*var).weight + 1) * DEC_DIGITS + rscale;

    if di <= 0 {
        (*var).ndigits = 0;
        (*var).weight = 0;
        (*var).sign = NUMERIC_POS;
    } else {
        ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

        if ndigits <= (*var).ndigits {
            (*var).ndigits = ndigits;

            di %= DEC_DIGITS;

            if di > 0 {
                let digits: *mut NumericDigit = (*var).digits;
                let extra: c_int;
                let pow10: c_int;

                pow10 = round_powers[di as usize];
                ndigits -= 1;
                extra = *digits.add(ndigits as usize) as c_int % pow10;
                *digits.add(ndigits as usize) -= extra as NumericDigit;
            }
        }
    }
}

/// strip_var - Strip any leading and trailing zeroes.
unsafe fn strip_var(var: *mut NumericVar) {
    let mut digits: *mut NumericDigit = (*var).digits;
    let mut ndigits: c_int = (*var).ndigits;

    while ndigits > 0 && *digits == 0 {
        digits = digits.add(1);
        (*var).weight -= 1;
        ndigits -= 1;
    }

    while ndigits > 0 && *digits.add((ndigits - 1) as usize) == 0 {
        ndigits -= 1;
    }

    if ndigits == 0 {
        (*var).sign = NUMERIC_POS;
        (*var).weight = 0;
    }

    (*var).digits = digits;
    (*var).ndigits = ndigits;
}


// ----------------------------------------------------------------------
//
// Fast sum accumulator functions
//
// ----------------------------------------------------------------------

/// Reset the accumulator's value to zero.
unsafe fn accum_sum_reset(accum: *mut NumericSumAccum) {
    let mut i: c_int;

    (*accum).dscale = 0;
    i = 0;
    while i < (*accum).ndigits {
        *(*accum).pos_digits.add(i as usize) = 0;
        *(*accum).neg_digits.add(i as usize) = 0;
        i += 1;
    }
}

/// Accumulate a new value.
unsafe fn accum_sum_add(accum: *mut NumericSumAccum, val: *const NumericVar) {
    let accum_digits: *mut int32;
    let mut i: c_int;
    let mut val_i: c_int;
    let val_ndigits: c_int;
    let val_digits: *mut NumericDigit;

    if (*accum).num_uncarried == NBASE - 1 {
        accum_sum_carry(accum);
    }

    accum_sum_rescale(accum, val);

    if (*val).sign == NUMERIC_POS {
        accum_digits = (*accum).pos_digits;
    } else {
        accum_digits = (*accum).neg_digits;
    }

    val_ndigits = (*val).ndigits;
    val_digits = (*val).digits;

    i = (*accum).weight - (*val).weight;
    val_i = 0;
    while val_i < val_ndigits {
        *accum_digits.add(i as usize) += *val_digits.add(val_i as usize) as int32;
        i += 1;
        val_i += 1;
    }

    (*accum).num_uncarried += 1;
}

/// Propagate carries.
unsafe fn accum_sum_carry(accum: *mut NumericSumAccum) {
    let mut i: c_int;
    let ndigits: c_int;
    let mut dig: *mut int32;
    let mut carry: int32;
    let mut newdig: int32 = 0;

    if (*accum).num_uncarried == 0 {
        return;
    }

    Assert!(*(*accum).pos_digits.add(0) == 0 && *(*accum).neg_digits.add(0) == 0);

    ndigits = (*accum).ndigits;

    dig = (*accum).pos_digits;
    carry = 0;
    i = ndigits - 1;
    while i >= 0 {
        newdig = *dig.add(i as usize) + carry;
        if newdig >= NBASE {
            carry = newdig / NBASE;
            newdig -= carry * NBASE;
        } else {
            carry = 0;
        }
        *dig.add(i as usize) = newdig;
        i -= 1;
    }
    if newdig > 0 {
        (*accum).have_carry_space = false;
    }

    dig = (*accum).neg_digits;
    carry = 0;
    i = ndigits - 1;
    while i >= 0 {
        newdig = *dig.add(i as usize) + carry;
        if newdig >= NBASE {
            carry = newdig / NBASE;
            newdig -= carry * NBASE;
        } else {
            carry = 0;
        }
        *dig.add(i as usize) = newdig;
        i -= 1;
    }
    if newdig > 0 {
        (*accum).have_carry_space = false;
    }

    (*accum).num_uncarried = 0;
}

/// Re-scale accumulator to accommodate new value.
unsafe fn accum_sum_rescale(accum: *mut NumericSumAccum, val: *const NumericVar) {
    let old_weight = (*accum).weight;
    let old_ndigits = (*accum).ndigits;
    let mut accum_ndigits: c_int;
    let mut accum_weight: c_int;
    let accum_rscale: c_int;
    let val_rscale: c_int;

    accum_weight = old_weight;
    accum_ndigits = old_ndigits;

    if (*val).weight >= accum_weight {
        accum_weight = (*val).weight + 1;
        accum_ndigits = accum_ndigits + (accum_weight - old_weight);
    } else if !(*accum).have_carry_space {
        accum_weight += 1;
        accum_ndigits += 1;
    }

    accum_rscale = accum_ndigits - accum_weight - 1;
    val_rscale = (*val).ndigits - (*val).weight - 1;
    if val_rscale > accum_rscale {
        accum_ndigits = accum_ndigits + (val_rscale - accum_rscale);
    }

    if accum_ndigits != old_ndigits || accum_weight != old_weight {
        let new_pos_digits: *mut int32;
        let new_neg_digits: *mut int32;
        let weightdiff: c_int;

        weightdiff = accum_weight - old_weight;

        new_pos_digits = palloc0(accum_ndigits as usize * core::mem::size_of::<int32>()) as *mut int32;
        new_neg_digits = palloc0(accum_ndigits as usize * core::mem::size_of::<int32>()) as *mut int32;

        if !(*accum).pos_digits.is_null() {
            memcpy(
                new_pos_digits.add(weightdiff as usize) as *mut c_void,
                (*accum).pos_digits as *const c_void,
                old_ndigits as usize * core::mem::size_of::<int32>(),
            );
            pfree((*accum).pos_digits as *mut c_void);

            memcpy(
                new_neg_digits.add(weightdiff as usize) as *mut c_void,
                (*accum).neg_digits as *const c_void,
                old_ndigits as usize * core::mem::size_of::<int32>(),
            );
            pfree((*accum).neg_digits as *mut c_void);
        }

        (*accum).pos_digits = new_pos_digits;
        (*accum).neg_digits = new_neg_digits;

        (*accum).weight = accum_weight;
        (*accum).ndigits = accum_ndigits;

        Assert!(*(*accum).pos_digits.add(0) == 0 && *(*accum).neg_digits.add(0) == 0);
        (*accum).have_carry_space = true;
    }

    if (*val).dscale > (*accum).dscale {
        (*accum).dscale = (*val).dscale;
    }
}

/// Return the current value of the accumulator.
unsafe fn accum_sum_final(accum: *mut NumericSumAccum, result: *mut NumericVar) {
    let mut i: c_int;
    let mut pos_var: NumericVar = core::mem::zeroed();
    let mut neg_var: NumericVar = core::mem::zeroed();

    if (*accum).ndigits == 0 {
        set_var_from_var(cvar(&const_zero), result);
        return;
    }

    accum_sum_carry(accum);

    init_var(&mut pos_var);
    init_var(&mut neg_var);

    pos_var.ndigits = (*accum).ndigits;
    neg_var.ndigits = (*accum).ndigits;
    pos_var.weight = (*accum).weight;
    neg_var.weight = (*accum).weight;
    pos_var.dscale = (*accum).dscale;
    neg_var.dscale = (*accum).dscale;
    pos_var.sign = NUMERIC_POS;
    neg_var.sign = NUMERIC_NEG;

    pos_var.buf = digitbuf_alloc((*accum).ndigits);
    pos_var.digits = pos_var.buf;
    neg_var.buf = digitbuf_alloc((*accum).ndigits);
    neg_var.digits = neg_var.buf;

    i = 0;
    while i < (*accum).ndigits {
        Assert!(*(*accum).pos_digits.add(i as usize) < NBASE);
        *pos_var.digits.add(i as usize) = *(*accum).pos_digits.add(i as usize) as int16;

        Assert!(*(*accum).neg_digits.add(i as usize) < NBASE);
        *neg_var.digits.add(i as usize) = *(*accum).neg_digits.add(i as usize) as int16;
        i += 1;
    }

    add_var(&pos_var, &neg_var, result);

    strip_var(result);
}

/// Copy an accumulator's state.
unsafe fn accum_sum_copy(dst: *mut NumericSumAccum, src: *mut NumericSumAccum) {
    (*dst).pos_digits = palloc((*src).ndigits as usize * core::mem::size_of::<int32>()) as *mut int32;
    (*dst).neg_digits = palloc((*src).ndigits as usize * core::mem::size_of::<int32>()) as *mut int32;

    memcpy(
        (*dst).pos_digits as *mut c_void,
        (*src).pos_digits as *const c_void,
        (*src).ndigits as usize * core::mem::size_of::<int32>(),
    );
    memcpy(
        (*dst).neg_digits as *mut c_void,
        (*src).neg_digits as *const c_void,
        (*src).ndigits as usize * core::mem::size_of::<int32>(),
    );
    (*dst).num_uncarried = (*src).num_uncarried;
    (*dst).ndigits = (*src).ndigits;
    (*dst).weight = (*src).weight;
    (*dst).dscale = (*src).dscale;
}

/// Add the current value of 'accum2' into 'accum'.
unsafe fn accum_sum_combine(accum: *mut NumericSumAccum, accum2: *mut NumericSumAccum) {
    let mut tmp_var: NumericVar = core::mem::zeroed();

    init_var(&mut tmp_var);

    accum_sum_final(accum2, &mut tmp_var);
    accum_sum_add(accum, &tmp_var);

    free_var(&mut tmp_var);
}
