//! Functions for the built-in integer types int2 (smallint) and int4
//! (integer). Translated from src/backend/utils/adt/int.c.
//!
//! Covers the user I/O routines (in/out/recv/send), the int2vector type, the
//! int2<->int4 and bool casts, every comparison operator (same-width and the
//! int24/int42 cross-width forms), the `in_range` window-frame helpers, the
//! arithmetic operators with PG-exact overflow handling (pl/mi/mul/div/mod/
//! abs/um/up/inc, gcd/lcm), larger/smaller, the bit-pushing operators, and the
//! generate_series(int4) set-returning function + its planner support.
//!
//! Each C `Datum fn(PG_FUNCTION_ARGS)` becomes a `PGFunction`-typed Rust fn
//! `fn(&mut FunctionCallInfoBaseData) -> Datum`; arguments are read with the
//! `PG_GETARG_*` helpers (deref of `fcinfo.args[n]`) and results returned via
//! `PG_RETURN_*` (the `XGetDatum` conversions in `postgres.rs`). Overflow is
//! detected with the `common::int` checked helpers (the C `pg_*_overflow`
//! out-param + bool folded to `Option`), and the out-of-range / divide-by-zero
//! / bad-syntax paths raise through `ereport!` per the error model.
//!
//! Subsystems int.c reaches that are not yet translated are called through their
//! existing `unimplemented!()` stubs (rules.md s4): the array machinery
//! (`array_recv`/`array_send`) behind int2vector recv/send, the SRF multi-call
//! context behind generate_series, the binary wire `StringInfo`/`MsgReader`
//! behind recv/send, and the planner support-node introspection. For M1 only
//! the int4/int2 text I/O + arithmetic + comparison paths need to actually run.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "intentional C width arithmetic: int.c does explicit int16/int32 casts \
              and accumulates conversions in wider unsigned types (the value-cast \
              family is an allowed port-inherent lint per rules.md s11)"
)]

use crate::c::{
    int2vector, PG_INT16_MAX, PG_INT16_MIN, PG_INT32_MAX, PG_INT32_MIN, PG_INT64_MAX, PG_INT64_MIN,
};
use crate::common::int::{
    pg_add_s16_overflow, pg_add_s32_overflow, pg_add_s64_overflow, pg_mul_s16_overflow,
    pg_mul_s32_overflow, pg_neg_u16_overflow, pg_neg_u32_overflow, pg_sub_s16_overflow,
    pg_sub_s32_overflow,
};
use crate::{ereport, ereturn};
use crate::fmgr::{FunctionCallInfoBaseData, PG_ARGISNULL, PG_NARGS};
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::nodes::nodes::Node;
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, Datum, DatumGetBool, DatumGetCString, DatumGetInt16,
    DatumGetInt32, DatumGetInt64, Int16GetDatum, Int32GetDatum, Int64GetDatum,
};
use crate::utils::elog::ERROR;
use crate::utils::errcodes::{
    ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

// ---------------------------------------------------------------------------
// PG_GETARG_* / PG_RETURN_* accessors.
//
// In C these are macros over `fcinfo`; here they are tiny inline helpers over
// `FunctionCallInfoBaseData`. The DirectFunctionCallN / FunctionCallN contract
// guarantees the args are populated, so a plain index is correct.
// ---------------------------------------------------------------------------

#[inline]
fn pg_getarg_int16(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i16 {
    DatumGetInt16(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_int32(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i32 {
    DatumGetInt32(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_int64(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i64 {
    DatumGetInt64(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_bool(fcinfo: &FunctionCallInfoBaseData, n: usize) -> bool {
    DatumGetBool(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_datum(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Datum {
    fcinfo.args[n].value
}

/// PG `PG_GETARG_CSTRING(n)`: the argument as a borrowed UTF-8 string.
///
/// The arg is a `Datum` holding a `*mut i8` to a NUL-terminated C string (set by
/// `InputFunctionCall` from a Rust `&str`'s bytes). We borrow it back as `&str`
/// for the duration of the call.
#[inline]
fn pg_getarg_cstring(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = DatumGetCString(fcinfo.args[n].value);
    // SAFETY: an input function's cstring argument is a NUL-terminated C string
    // that outlives the call (InputFunctionCall keeps the source alive).
    let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
    cstr.to_string_lossy().into_owned()
}

/// The call's soft-error context (`fcinfo->context` cast to `ErrorSaveContext`),
/// or `None` when the caller did not supply one (the hard-error path). Shared by
/// the type input functions that use `ereturn` (int2in/int4in).
#[inline]
fn fcinfo_escontext(fcinfo: &mut FunctionCallInfoBaseData) -> Option<&mut ErrorSaveContext> {
    match fcinfo.context.as_deref_mut() {
        Some(Node::ErrorSaveContext(e)) => Some(e),
        _ => None,
    }
}

/// PG `PG_RETURN_CSTRING(s)`: hand back an owned C string as a `Datum`.
///
/// C returns a palloc'd `char *`; the caller (`OutputFunctionCall`) reads it as
/// a C string and copies it out. We leak a `CString` (no MemoryContext to free
/// it into yet) so the pointer stays valid until that copy happens.
#[inline]
fn pg_return_cstring(s: &str) -> Datum {
    let c = std::ffi::CString::new(s).unwrap_or_else(|_| {
        // An int output never contains an interior NUL; keep the prefix.
        let bytes: Vec<u8> = s.bytes().take_while(|&b| b != 0).collect();
        std::ffi::CString::new(bytes).unwrap_or_default()
    });
    CStringGetDatum(c.into_raw())
}

// ---------------------------------------------------------------------------
// Integer text-conversion helpers.
//
// int.c delegates input parsing to pg_strtoint{16,32}_safe and output formatting
// to pg_itoa / pg_ltoa (numutils.c). numutils.c is its own leaf file, not yet
// translated; its builtins.h declarations are still `unimplemented!()` stubs we
// must not edit here. So we provide the small conversions int.c needs as private
// file-local helpers that reproduce numutils.c's accepted syntax (optional sign,
// decimal/hex/octal/binary with 0x/0o/0b prefixes, single underscores between
// digits, surrounding whitespace) and PG's out-of-range / invalid-syntax errors.
//
// TODO(numutils): delete these and call crate::utils::builtins::{pg_strtoint16,
// pg_strtoint32, pg_ltoa, pg_itoa} once numutils.c is translated.
// ---------------------------------------------------------------------------

/// PG numutils `pg_ltoa`: decimal string for a signed 32-bit integer.
fn ltoa(value: i32) -> String {
    value.to_string()
}

/// PG numutils `pg_itoa`: `pg_ltoa((int32) i, a)` -- decimal string for int16.
fn itoa(value: i16) -> String {
    ltoa(i32::from(value))
}

/// Outcome of integer string parsing, mirroring numutils.c's two error labels.
enum ParseErr {
    OutOfRange,
    InvalidSyntax,
}

/// Parse `s` per numutils.c into an unsigned magnitude + sign, base-aware.
/// Returns the magnitude (as u64, wide enough for any of int16/int32/int64) and
/// whether it was negated; the caller range-checks against its own type.
/// `int_min` is the target family's minimum, setting PG's per-digit overflow
/// guard `tmp > -(PG_INTnn_MIN / base)`.
fn parse_int_str(s: &str, int_min: i64) -> Result<(u64, bool), ParseErr> {
    let bytes = s.as_bytes();
    let mut i = 0;
    let len = bytes.len();

    // skip leading spaces
    while i < len && bytes[i].is_ascii_whitespace() {
        i += 1;
    }

    // sign
    let mut neg = false;
    if i < len && bytes[i] == b'-' {
        neg = true;
        i += 1;
    } else if i < len && bytes[i] == b'+' {
        i += 1;
    }

    // base detection
    let (base, mut j): (u64, usize) = if i + 1 < len
        && bytes[i] == b'0'
        && matches!(bytes[i + 1], b'x' | b'X')
    {
        (16, i + 2)
    } else if i + 1 < len && bytes[i] == b'0' && matches!(bytes[i + 1], b'o' | b'O') {
        (8, i + 2)
    } else if i + 1 < len && bytes[i] == b'0' && matches!(bytes[i + 1], b'b' | b'B') {
        (2, i + 2)
    } else {
        (10, i)
    };

    let firstdigit = j;
    let mut tmp: u64 = 0;
    // PG's overflow guard threshold: tmp > -(MIN/base). We use a u64 accumulator
    // so the caller's final, type-exact range check sees the full magnitude.
    let ceiling = (-(int_min / base as i64)) as u64;

    while j < len {
        let c = bytes[j];
        let digit = match (base, c) {
            (16, b'a'..=b'f') => u64::from(c - b'a') + 10,
            (16, b'A'..=b'F') => u64::from(c - b'A') + 10,
            (16 | 10, b'0'..=b'9') | (8, b'0'..=b'7') | (2, b'0'..=b'1') => u64::from(c - b'0'),
            (_, b'_') => {
                // decimal: underscore may not be first (non-decimal bases allow it
                // right after the 0x/0o/0b prefix); it must be followed by a digit.
                if base == 10 && j == firstdigit {
                    return Err(ParseErr::InvalidSyntax);
                }
                j += 1;
                if j >= len || !is_base_digit(bytes[j], base) {
                    return Err(ParseErr::InvalidSyntax);
                }
                continue;
            }
            _ => break,
        };
        j += 1;
        if tmp > ceiling {
            return Err(ParseErr::OutOfRange);
        }
        tmp = tmp * base + digit;
    }

    // require at least one digit
    if j == firstdigit {
        return Err(ParseErr::InvalidSyntax);
    }

    // trailing whitespace only
    while j < len && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    if j != len {
        return Err(ParseErr::InvalidSyntax);
    }

    Ok((tmp, neg))
}

/// True iff `c` is a valid digit in `base` (for the underscore look-ahead).
fn is_base_digit(c: u8, base: u64) -> bool {
    match base {
        16 => c.is_ascii_hexdigit(),
        10 => c.is_ascii_digit(),
        8 => (b'0'..=b'7').contains(&c),
        2 => matches!(c, b'0' | b'1'),
        _ => false,
    }
}

/// PG numutils `pg_strtoint16_safe`: parse a string to int16. On error the report
/// is routed through `escontext` -- soft (records + returns `None`) if the caller
/// supplied an `ErrorSaveContext`, else hard (ereport ERROR, diverges). C's
/// out-param + bool folds to `Option`.
fn strtoint16_safe(s: &str, escontext: Option<&mut ErrorSaveContext>) -> Option<i16> {
    match parse_int_str(s, i64::from(PG_INT32_MIN)) {
        Ok((tmp, neg)) => {
            if neg {
                match pg_neg_u16_overflow(tmp as u16) {
                    Some(r) if u64::from(tmp as u16) == tmp => Some(r),
                    _ => out_of_range(s, "smallint", escontext),
                }
            } else if tmp <= PG_INT16_MAX as u64 {
                Some(tmp as i16)
            } else {
                out_of_range(s, "smallint", escontext)
            }
        }
        Err(ParseErr::OutOfRange) => out_of_range(s, "smallint", escontext),
        Err(ParseErr::InvalidSyntax) => invalid_syntax(s, "smallint", escontext),
    }
}

/// PG numutils `pg_strtoint32_safe`: like [`strtoint16_safe`] but for int32.
fn strtoint32_safe(s: &str, escontext: Option<&mut ErrorSaveContext>) -> Option<i32> {
    match parse_int_str(s, i64::from(PG_INT32_MIN)) {
        Ok((tmp, neg)) => {
            if neg {
                match pg_neg_u32_overflow(tmp as u32) {
                    Some(r) if u64::from(tmp as u32) == tmp => Some(r),
                    _ => out_of_range(s, "integer", escontext),
                }
            } else if tmp <= PG_INT32_MAX as u64 {
                Some(tmp as i32)
            } else {
                out_of_range(s, "integer", escontext)
            }
        }
        Err(ParseErr::OutOfRange) => out_of_range(s, "integer", escontext),
        Err(ParseErr::InvalidSyntax) => invalid_syntax(s, "integer", escontext),
    }
}

/// PG numutils `pg_strtoint64_safe`: like [`strtoint16_safe`] but for int64.
/// `pub` because the parser's `make_const` uses it for oversize integer literals
/// (PG parse_node.c calls pg_strtoint64_safe on every T_Float literal first).
pub fn strtoint64_safe(s: &str, escontext: Option<&mut ErrorSaveContext>) -> Option<i64> {
    match parse_int_str(s, PG_INT64_MIN) {
        Ok((tmp, neg)) => {
            if neg {
                // pg_neg_u64_overflow: -tmp fits iff tmp <= 2^63 (|INT64_MIN|).
                if tmp <= (1u64 << 63) {
                    Some(tmp.wrapping_neg() as i64)
                } else {
                    out_of_range(s, "bigint", escontext)
                }
            } else if tmp <= PG_INT64_MAX as u64 {
                Some(tmp as i64)
            } else {
                out_of_range(s, "bigint", escontext)
            }
        }
        Err(ParseErr::OutOfRange) => out_of_range(s, "bigint", escontext),
        Err(ParseErr::InvalidSyntax) => invalid_syntax(s, "bigint", escontext),
    }
}

/// numutils.c `out_of_range:` label -- `ereturn(escontext, 0, ...)`. Generic over
/// the caller's result width (`Option<T>`): the soft path returns `None`, the hard
/// path raises ERROR.
fn out_of_range<T>(s: &str, typname: &str, escontext: Option<&mut ErrorSaveContext>) -> Option<T> {
    let (sv, tv) = (s.to_owned(), typname.to_owned());
    ereturn!(escontext, None, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg(format!("value \"{sv}\" is out of range for type {tv}"));
    });
}

/// numutils.c `invalid_syntax:` label -- `ereturn(escontext, 0, ...)`.
fn invalid_syntax<T>(s: &str, typname: &str, escontext: Option<&mut ErrorSaveContext>) -> Option<T> {
    let (sv, tv) = (s.to_owned(), typname.to_owned());
    ereturn!(escontext, None, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .errmsg(format!("invalid input syntax for type {tv}: \"{sv}\""));
    });
}

/// Raise the standard "integer out of range" / "smallint out of range" error.
fn integer_out_of_range() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg("integer out of range");
    });
    unreachable!()
}
fn smallint_out_of_range() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg("smallint out of range");
    });
    unreachable!()
}
fn division_by_zero() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_DIVISION_BY_ZERO).errmsg("division by zero");
    });
    unreachable!()
}

// ===========================================================================
//   USER I/O ROUTINES
// ===========================================================================

/// PG `int2in`: converts "num" to int16. Routes any error through the call's
/// soft-error context (`fcinfo->context`) via `pg_strtoint16_safe`. On a soft
/// error the returned datum is garbage (0, the ereturn dummy); the caller checks
/// the context before trusting it.
pub fn int2in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_cstring(fcinfo, 0);
    let esc = fcinfo_escontext(fcinfo);
    Int16GetDatum(strtoint16_safe(&num, esc).unwrap_or(0))
}

/// PG `int2out`: converts int16 to "num".
pub fn int2out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    pg_return_cstring(&itoa(arg1))
}

/// PG `int2recv`: converts external binary format to int2.
pub fn int2recv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("int2recv needs the binary wire StringInfo (pq_getmsgint) path")
}

/// PG `int2send`: converts int2 to binary format.
pub fn int2send(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("int2send needs pq_begintypsend/pq_endtypsend bytea boxing")
}

/// PG `int2vectorin`: converts "num num ..." to internal int2vector form.
/// Routes bad-element errors through the call's soft-error context. Each element
/// is parsed with C `strtol(s, &endp, 10)` semantics (optional sign + decimal
/// digits); the error messages carry the REMAINING input from the bad element on,
/// exactly as C's `%s` of the unadvanced pointer does.
pub fn int2vectorin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    use crate::pg_config_manual::FUNC_MAX_ARGS;

    let s = pg_getarg_cstring(fcinfo, 0);
    let esc = fcinfo_escontext(fcinfo);
    let bytes = s.as_bytes();
    let mut i = 0usize;
    let mut values: Vec<i16> = Vec::new();

    for _ in 0..FUNC_MAX_ARGS {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let start = i;
        let mut j = i;
        if bytes[j] == b'-' || bytes[j] == b'+' {
            j += 1;
        }
        let digits_start = j;
        // Saturating accumulate: any i64-saturated value is out of int16 range,
        // matching strtol's ERANGE.
        let mut l: i64 = 0;
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            l = l.saturating_mul(10).saturating_add(i64::from(bytes[j] - b'0'));
            j += 1;
        }
        if j == digits_start {
            // strtol consumed nothing (endp == input).
            let rest = s[start..].to_owned();
            ereturn!(esc, Datum(0), |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                    .errmsg(format!("invalid input syntax for type smallint: \"{rest}\""));
            });
        }
        if bytes[start] == b'-' {
            l = -l;
        }
        if l < i64::from(PG_INT16_MIN) || l > i64::from(PG_INT16_MAX) {
            let rest = s[start..].to_owned();
            ereturn!(esc, Datum(0), |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                    .errmsg(format!("value \"{rest}\" is out of range for type smallint"));
            });
        }
        values.push(l as i16);
        i = j;
    }
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i < bytes.len() {
        ereturn!(esc, Datum(0), |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .errmsg("int2vector has too many elements".to_owned());
        });
    }

    crate::postgres::PointerGetDatum(crate::utils::builtins::buildint2vector(&values).cast::<u8>())
}

/// PG `int2vectorout`: converts int2vector internal form to "num num ...".
pub fn int2vectorout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = fcinfo.args[0].value.0 as *const int2vector;
    // SAFETY: `p` is a live int2vector for the duration of the call and `dim1`
    // elements follow the header at `values`.
    let s = unsafe {
        let n = (*p).dim1 as usize;
        let vptr = std::ptr::addr_of!((*p).values).cast::<i16>();
        let mut parts: Vec<String> = Vec::with_capacity(n);
        for k in 0..n {
            parts.push((*vptr.add(k)).to_string());
        }
        parts.join(" ")
    };
    pg_return_cstring(&s)
}

/// PG `int2vectorrecv`: converts external binary format to int2vector.
pub fn int2vectorrecv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("int2vectorrecv needs array_recv")
}

/// PG `int2vectorsend`: converts int2vector to binary format.
pub fn int2vectorsend(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("int2vectorsend needs array_send")
}

/// PG `int4in`: converts "num" to int32. Routes errors through the call's
/// soft-error context (`fcinfo->context`) via `pg_strtoint32_safe`.
pub fn int4in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_cstring(fcinfo, 0);
    let esc = fcinfo_escontext(fcinfo);
    Int32GetDatum(strtoint32_safe(&num, esc).unwrap_or(0))
}

/// PG `int4out`: converts int32 to "num".
pub fn int4out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    pg_return_cstring(&ltoa(arg1))
}

/// PG `int4recv`: converts external binary format to int4.
pub fn int4recv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("int4recv needs the binary wire StringInfo (pq_getmsgint) path")
}

/// PG `int4send`: converts int4 to binary format.
pub fn int4send(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("int4send needs pq_begintypsend/pq_endtypsend bytea boxing")
}

// ===========================================================================
//   CONVERSION ROUTINES
// ===========================================================================

/// PG `i2toi4`: widen int16 to int32.
pub fn i2toi4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    Int32GetDatum(i32::from(arg1))
}

/// PG `i4toi2`: narrow int32 to int16, raising on overflow.
pub fn i4toi2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    if arg1 < i32::from(PG_INT16_MIN) || arg1 > i32::from(PG_INT16_MAX) {
        smallint_out_of_range();
    }
    Int16GetDatum(arg1 as i16)
}

/// PG `int4_bool`: cast int4 -> bool.
pub fn int4_bool(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_int32(fcinfo, 0) != 0)
}

/// PG `bool_int4`: cast bool -> int4.
pub fn bool_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(i32::from(pg_getarg_bool(fcinfo, 0)))
}

// ===========================================================================
//   COMPARISON OPERATOR ROUTINES
// ===========================================================================

macro_rules! cmp_op {
    ($name:ident, $geta:ident, $getb:ident, $op:tt) => {
        #[doc = concat!("PG `", stringify!($name), "`.")]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let arg1 = $geta(fcinfo, 0);
            let arg2 = $getb(fcinfo, 1);
            BoolGetDatum(i64::from(arg1) $op i64::from(arg2))
        }
    };
}

cmp_op!(int4eq, pg_getarg_int32, pg_getarg_int32, ==);
cmp_op!(int4ne, pg_getarg_int32, pg_getarg_int32, !=);
cmp_op!(int4lt, pg_getarg_int32, pg_getarg_int32, <);
cmp_op!(int4le, pg_getarg_int32, pg_getarg_int32, <=);
cmp_op!(int4gt, pg_getarg_int32, pg_getarg_int32, >);
cmp_op!(int4ge, pg_getarg_int32, pg_getarg_int32, >=);

cmp_op!(int2eq, pg_getarg_int16, pg_getarg_int16, ==);
cmp_op!(int2ne, pg_getarg_int16, pg_getarg_int16, !=);
cmp_op!(int2lt, pg_getarg_int16, pg_getarg_int16, <);
cmp_op!(int2le, pg_getarg_int16, pg_getarg_int16, <=);
cmp_op!(int2gt, pg_getarg_int16, pg_getarg_int16, >);
cmp_op!(int2ge, pg_getarg_int16, pg_getarg_int16, >=);

cmp_op!(int24eq, pg_getarg_int16, pg_getarg_int32, ==);
cmp_op!(int24ne, pg_getarg_int16, pg_getarg_int32, !=);
cmp_op!(int24lt, pg_getarg_int16, pg_getarg_int32, <);
cmp_op!(int24le, pg_getarg_int16, pg_getarg_int32, <=);
cmp_op!(int24gt, pg_getarg_int16, pg_getarg_int32, >);
cmp_op!(int24ge, pg_getarg_int16, pg_getarg_int32, >=);

cmp_op!(int42eq, pg_getarg_int32, pg_getarg_int16, ==);
cmp_op!(int42ne, pg_getarg_int32, pg_getarg_int16, !=);
cmp_op!(int42lt, pg_getarg_int32, pg_getarg_int16, <);
cmp_op!(int42le, pg_getarg_int32, pg_getarg_int16, <=);
cmp_op!(int42gt, pg_getarg_int32, pg_getarg_int16, >);
cmp_op!(int42ge, pg_getarg_int32, pg_getarg_int16, >=);

// int8.c comparisons: same-width and the int84/int48 cross-width forms
// (count(*) = 1 is int8 = int4; ORDER BY count(*) sorts by int8lt).
cmp_op!(int8eq, pg_getarg_int64, pg_getarg_int64, ==);
cmp_op!(int8ne, pg_getarg_int64, pg_getarg_int64, !=);
cmp_op!(int8lt, pg_getarg_int64, pg_getarg_int64, <);
cmp_op!(int8le, pg_getarg_int64, pg_getarg_int64, <=);
cmp_op!(int8gt, pg_getarg_int64, pg_getarg_int64, >);
cmp_op!(int8ge, pg_getarg_int64, pg_getarg_int64, >=);

cmp_op!(int84eq, pg_getarg_int64, pg_getarg_int32, ==);
cmp_op!(int84ne, pg_getarg_int64, pg_getarg_int32, !=);
cmp_op!(int84lt, pg_getarg_int64, pg_getarg_int32, <);
cmp_op!(int84le, pg_getarg_int64, pg_getarg_int32, <=);
cmp_op!(int84gt, pg_getarg_int64, pg_getarg_int32, >);
cmp_op!(int84ge, pg_getarg_int64, pg_getarg_int32, >=);

cmp_op!(int48eq, pg_getarg_int32, pg_getarg_int64, ==);
cmp_op!(int48ne, pg_getarg_int32, pg_getarg_int64, !=);
cmp_op!(int48lt, pg_getarg_int32, pg_getarg_int64, <);
cmp_op!(int48le, pg_getarg_int32, pg_getarg_int64, <=);
cmp_op!(int48gt, pg_getarg_int32, pg_getarg_int64, >);
cmp_op!(int48ge, pg_getarg_int32, pg_getarg_int64, >=);

// ---------------------------------------------------------------------------
//   in_range functions for int4 and int2, including cross-type comparisons.
// ---------------------------------------------------------------------------

/// PG `in_range_int4_int4`.
pub fn in_range_int4_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = pg_getarg_int32(fcinfo, 0);
    let base = pg_getarg_int32(fcinfo, 1);
    let mut offset = pg_getarg_int32(fcinfo, 2);
    let sub = pg_getarg_bool(fcinfo, 3);
    let less = pg_getarg_bool(fcinfo, 4);

    if offset < 0 {
        invalid_preceding_or_following();
    }
    if sub {
        offset = -offset; // cannot overflow
    }
    let Some(sum) = pg_add_s32_overflow(base, offset) else {
        return BoolGetDatum(if sub { !less } else { less });
    };
    BoolGetDatum(if less { val <= sum } else { val >= sum })
}

/// PG `in_range_int4_int2`: invoke int4_int4 with the offset widened.
pub fn in_range_int4_int2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut inner = remap_args(
        fcinfo,
        5,
        &[
            (0, pg_getarg_datum(fcinfo, 0)),
            (1, pg_getarg_datum(fcinfo, 1)),
            (2, Int32GetDatum(i32::from(pg_getarg_int16(fcinfo, 2)))),
            (3, pg_getarg_datum(fcinfo, 3)),
            (4, pg_getarg_datum(fcinfo, 4)),
        ],
    );
    in_range_int4_int4(&mut inner)
}

/// PG `in_range_int4_int8`: all math in int64.
pub fn in_range_int4_int8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = i64::from(pg_getarg_int32(fcinfo, 0));
    let base = i64::from(pg_getarg_int32(fcinfo, 1));
    let mut offset = pg_getarg_int64(fcinfo, 2);
    let sub = pg_getarg_bool(fcinfo, 3);
    let less = pg_getarg_bool(fcinfo, 4);

    if offset < 0 {
        invalid_preceding_or_following();
    }
    if sub {
        offset = -offset;
    }
    let Some(sum) = pg_add_s64_overflow(base, offset) else {
        return BoolGetDatum(if sub { !less } else { less });
    };
    BoolGetDatum(if less { val <= sum } else { val >= sum })
}

/// PG `in_range_int2_int4`: all math in int32.
pub fn in_range_int2_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = i32::from(pg_getarg_int16(fcinfo, 0));
    let base = i32::from(pg_getarg_int16(fcinfo, 1));
    let mut offset = pg_getarg_int32(fcinfo, 2);
    let sub = pg_getarg_bool(fcinfo, 3);
    let less = pg_getarg_bool(fcinfo, 4);

    if offset < 0 {
        invalid_preceding_or_following();
    }
    if sub {
        offset = -offset;
    }
    let Some(sum) = pg_add_s32_overflow(base, offset) else {
        return BoolGetDatum(if sub { !less } else { less });
    };
    BoolGetDatum(if less { val <= sum } else { val >= sum })
}

/// PG `in_range_int2_int2`: invoke int2_int4 with the offset widened.
pub fn in_range_int2_int2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut inner = remap_args(
        fcinfo,
        5,
        &[
            (0, pg_getarg_datum(fcinfo, 0)),
            (1, pg_getarg_datum(fcinfo, 1)),
            (2, Int32GetDatum(i32::from(pg_getarg_int16(fcinfo, 2)))),
            (3, pg_getarg_datum(fcinfo, 3)),
            (4, pg_getarg_datum(fcinfo, 4)),
        ],
    );
    in_range_int2_int4(&mut inner)
}

/// PG `in_range_int2_int8`: invoke int4_int8 with the val/base widened.
pub fn in_range_int2_int8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut inner = remap_args(
        fcinfo,
        5,
        &[
            (0, Int32GetDatum(i32::from(pg_getarg_int16(fcinfo, 0)))),
            (1, Int32GetDatum(i32::from(pg_getarg_int16(fcinfo, 1)))),
            (2, pg_getarg_datum(fcinfo, 2)),
            (3, pg_getarg_datum(fcinfo, 3)),
            (4, pg_getarg_datum(fcinfo, 4)),
        ],
    );
    in_range_int4_int8(&mut inner)
}

fn invalid_preceding_or_following() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE)
            .errmsg("invalid preceding or following size in window function");
    });
    unreachable!()
}

/// Build a fresh fcinfo with `nargs` slots, then set the given (index, value)
/// args non-null. Models int.c's `DirectFunctionCall5(...)` re-dispatch where a
/// cross-type in_range delegates to its same-width sibling with a widened arg.
fn remap_args(
    src: &FunctionCallInfoBaseData,
    nargs: i16,
    args: &[(usize, Datum)],
) -> FunctionCallInfoBaseData {
    let mut fcinfo = FunctionCallInfoBaseData {
        flinfo: None,
        context: None,
        resultinfo: None,
        fncollation: src.fncollation,
        isnull: false,
        nargs,
        args: vec![
            crate::postgres::NullableDatum { value: Datum(0), isnull: true };
            nargs as usize
        ],
    };
    for &(n, v) in args {
        fcinfo.args[n].value = v;
        fcinfo.args[n].isnull = false;
    }
    fcinfo
}

// ===========================================================================
//   ARITHMETIC OPERATORS
// ===========================================================================

/// PG `int4um`: unary minus.
pub fn int4um(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg = pg_getarg_int32(fcinfo, 0);
    if arg == PG_INT32_MIN {
        integer_out_of_range();
    }
    Int32GetDatum(-arg)
}

/// PG `int4up`: unary plus (identity).
pub fn int4up(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(pg_getarg_int32(fcinfo, 0))
}

/// PG `int4pl`: arg1 + arg2.
pub fn int4pl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    let result = pg_add_s32_overflow(arg1, arg2).unwrap_or_else(|| integer_out_of_range());
    Int32GetDatum(result)
}

/// PG `int4mi`: arg1 - arg2.
pub fn int4mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    let result = pg_sub_s32_overflow(arg1, arg2).unwrap_or_else(|| integer_out_of_range());
    Int32GetDatum(result)
}

/// PG `int4mul`: arg1 * arg2.
pub fn int4mul(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    let result = pg_mul_s32_overflow(arg1, arg2).unwrap_or_else(|| integer_out_of_range());
    Int32GetDatum(result)
}

/// PG `int4div`: arg1 / arg2, handling div-by-zero and INT_MIN / -1.
pub fn int4div(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);

    if arg2 == 0 {
        division_by_zero();
    }
    if arg2 == -1 {
        if arg1 == PG_INT32_MIN {
            integer_out_of_range();
        }
        return Int32GetDatum(-arg1);
    }
    Int32GetDatum(arg1 / arg2)
}

/// PG `int4inc`: arg + 1 (the count(*) increment).
pub fn int4inc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg = pg_getarg_int32(fcinfo, 0);
    let result = pg_add_s32_overflow(arg, 1).unwrap_or_else(|| integer_out_of_range());
    Int32GetDatum(result)
}

/// PG `int2um`: unary minus.
pub fn int2um(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg = pg_getarg_int16(fcinfo, 0);
    if arg == PG_INT16_MIN {
        smallint_out_of_range();
    }
    Int16GetDatum(-arg)
}

/// PG `int2up`: unary plus (identity).
pub fn int2up(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int16GetDatum(pg_getarg_int16(fcinfo, 0))
}

/// PG `int2pl`: arg1 + arg2.
pub fn int2pl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int16(fcinfo, 1);
    let result = pg_add_s16_overflow(arg1, arg2).unwrap_or_else(|| smallint_out_of_range());
    Int16GetDatum(result)
}

/// PG `int2mi`: arg1 - arg2.
pub fn int2mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int16(fcinfo, 1);
    let result = pg_sub_s16_overflow(arg1, arg2).unwrap_or_else(|| smallint_out_of_range());
    Int16GetDatum(result)
}

/// PG `int2mul`: arg1 * arg2.
pub fn int2mul(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int16(fcinfo, 1);
    let result = pg_mul_s16_overflow(arg1, arg2).unwrap_or_else(|| smallint_out_of_range());
    Int16GetDatum(result)
}

/// PG `int2div`: arg1 / arg2, handling div-by-zero and SHRT_MIN / -1.
pub fn int2div(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int16(fcinfo, 1);

    if arg2 == 0 {
        division_by_zero();
    }
    if arg2 == -1 {
        if arg1 == PG_INT16_MIN {
            smallint_out_of_range();
        }
        return Int16GetDatum(-arg1);
    }
    Int16GetDatum(arg1 / arg2)
}

/// PG `int24pl`: int16 + int32.
pub fn int24pl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    let result =
        pg_add_s32_overflow(i32::from(arg1), arg2).unwrap_or_else(|| integer_out_of_range());
    Int32GetDatum(result)
}

/// PG `int24mi`: int16 - int32.
pub fn int24mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    let result =
        pg_sub_s32_overflow(i32::from(arg1), arg2).unwrap_or_else(|| integer_out_of_range());
    Int32GetDatum(result)
}

/// PG `int24mul`: int16 * int32.
pub fn int24mul(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    let result =
        pg_mul_s32_overflow(i32::from(arg1), arg2).unwrap_or_else(|| integer_out_of_range());
    Int32GetDatum(result)
}

/// PG `int24div`: int16 / int32 (no overflow possible; div-by-zero checked).
pub fn int24div(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    if arg2 == 0 {
        division_by_zero();
    }
    Int32GetDatum(i32::from(arg1) / arg2)
}

/// PG `int42pl`: int32 + int16.
pub fn int42pl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int16(fcinfo, 1);
    let result =
        pg_add_s32_overflow(arg1, i32::from(arg2)).unwrap_or_else(|| integer_out_of_range());
    Int32GetDatum(result)
}

/// PG `int42mi`: int32 - int16.
pub fn int42mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int16(fcinfo, 1);
    let result =
        pg_sub_s32_overflow(arg1, i32::from(arg2)).unwrap_or_else(|| integer_out_of_range());
    Int32GetDatum(result)
}

/// PG `int42mul`: int32 * int16.
pub fn int42mul(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int16(fcinfo, 1);
    let result =
        pg_mul_s32_overflow(arg1, i32::from(arg2)).unwrap_or_else(|| integer_out_of_range());
    Int32GetDatum(result)
}

/// PG `int42div`: int32 / int16, handling div-by-zero and INT_MIN / -1.
pub fn int42div(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int16(fcinfo, 1);

    if arg2 == 0 {
        division_by_zero();
    }
    if arg2 == -1 {
        if arg1 == PG_INT32_MIN {
            integer_out_of_range();
        }
        return Int32GetDatum(-arg1);
    }
    Int32GetDatum(arg1 / i32::from(arg2))
}

/// PG `int4mod`: arg1 % arg2.
pub fn int4mod(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);

    if arg2 == 0 {
        division_by_zero();
    }
    // INT_MIN % -1 is well-defined as 0; sidestep the FPE some machines raise.
    if arg2 == -1 {
        return Int32GetDatum(0);
    }
    Int32GetDatum(arg1 % arg2)
}

/// PG `int2mod`: arg1 % arg2.
pub fn int2mod(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int16(fcinfo, 1);

    if arg2 == 0 {
        division_by_zero();
    }
    if arg2 == -1 {
        return Int16GetDatum(0);
    }
    Int16GetDatum(arg1 % arg2)
}

/// PG `int4abs`: absolute value, raising on INT_MIN.
pub fn int4abs(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    if arg1 == PG_INT32_MIN {
        integer_out_of_range();
    }
    // The INT_MIN case is guarded above, so abs() cannot overflow.
    Int32GetDatum(arg1.abs())
}

/// PG `int2abs`: absolute value, raising on SHRT_MIN.
pub fn int2abs(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    if arg1 == PG_INT16_MIN {
        smallint_out_of_range();
    }
    // The INT_MIN case is guarded above, so abs() cannot overflow.
    Int16GetDatum(arg1.abs())
}

/// PG `int4gcd_internal`: Euclidean GCD with INT_MIN guards.
fn int4gcd_internal(mut arg1: i32, mut arg2: i32) -> i32 {
    // Put the greater absolute value in arg1 (computed in negative space so
    // INT_MIN is representable). wrapping_neg on a non-negative i32 never wraps;
    // INT_MIN is never reached here because it is already negative.
    let a1 = if arg1 < 0 { arg1 } else { arg1.wrapping_neg() };
    let a2 = if arg2 < 0 { arg2 } else { arg2.wrapping_neg() };
    if a1 > a2 {
        std::mem::swap(&mut arg1, &mut arg2);
    }

    if arg1 == PG_INT32_MIN {
        if arg2 == 0 || arg2 == PG_INT32_MIN {
            integer_out_of_range();
        }
        // gcd(INT_MIN, -1) = 1; dodge the INT_MIN % -1 FPE.
        if arg2 == -1 {
            return 1;
        }
    }

    while arg2 != 0 {
        let swap = arg2;
        arg2 = arg1 % arg2;
        arg1 = swap;
    }

    if arg1 < 0 {
        arg1 = -arg1;
    }
    arg1
}

/// PG `int4gcd`: greatest common divisor.
pub fn int4gcd(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    Int32GetDatum(int4gcd_internal(arg1, arg2))
}

/// PG `int4lcm`: least common multiple.
pub fn int4lcm(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);

    if arg1 == 0 || arg2 == 0 {
        return Int32GetDatum(0);
    }
    let gcd = int4gcd_internal(arg1, arg2);
    arg1 /= gcd;

    let mut result = pg_mul_s32_overflow(arg1, arg2).unwrap_or_else(|| integer_out_of_range());
    if result == PG_INT32_MIN {
        integer_out_of_range();
    }
    if result < 0 {
        result = -result;
    }
    Int32GetDatum(result)
}

/// PG `int2larger`: max of two int16.
pub fn int2larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int16(fcinfo, 1);
    Int16GetDatum(if arg1 > arg2 { arg1 } else { arg2 })
}

/// PG `int2smaller`: min of two int16.
pub fn int2smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int16(fcinfo, 1);
    Int16GetDatum(if arg1 < arg2 { arg1 } else { arg2 })
}

/// PG `int4larger`: max of two int32.
pub fn int4larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    Int32GetDatum(if arg1 > arg2 { arg1 } else { arg2 })
}

/// PG `int4smaller`: min of two int32.
pub fn int4smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    Int32GetDatum(if arg1 < arg2 { arg1 } else { arg2 })
}

// ===========================================================================
//   INT8 (bigint) -- the subset count/sum/min/max need (int8.c, partial).
//   int8 is pass-by-value here (USE_FLOAT8_BYVAL), so the AggCheckCallContext
//   in-place transition path is ifdef-ed out: the by-value branch is the only
//   one, and the transition value flows by value through fmgr.
// ===========================================================================

/// PG `int8in`: parse a bigint text representation. Routes errors through the
/// call's soft-error context (`fcinfo->context`) via `pg_strtoint64_safe`.
pub fn int8in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_cstring(fcinfo, 0);
    let esc = fcinfo_escontext(fcinfo);
    Int64GetDatum(strtoint64_safe(&num, esc).unwrap_or(0))
}

/// PG `int8out`: bigint -> its decimal text.
pub fn int8out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = pg_getarg_int64(fcinfo, 0);
    pg_return_cstring(&val.to_string())
}

/// PG `int48`: widen int32 to int64.
pub fn int48(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int64GetDatum(i64::from(pg_getarg_int32(fcinfo, 0)))
}

/// PG `int84`: narrow int64 to int32, raising on overflow.
pub fn int84(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg = pg_getarg_int64(fcinfo, 0);
    if arg < i64::from(PG_INT32_MIN) || arg > i64::from(PG_INT32_MAX) {
        integer_out_of_range();
    }
    Int32GetDatum(arg as i32)
}

/// PG `int28`: widen int16 to int64.
pub fn int28(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int64GetDatum(i64::from(pg_getarg_int16(fcinfo, 0)))
}

/// PG `int82`: narrow int64 to int16, raising on overflow.
pub fn int82(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg = pg_getarg_int64(fcinfo, 0);
    if arg < i64::from(PG_INT16_MIN) || arg > i64::from(PG_INT16_MAX) {
        smallint_out_of_range();
    }
    Int16GetDatum(arg as i16)
}

/// PG `int8inc`: increment the running count (COUNT(*) transition fn). With int8
/// pass-by-value the in-place aggregate path is ifdef-ed out; this is the plain
/// `arg + 1` form.
pub fn int8inc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg = pg_getarg_int64(fcinfo, 0);
    let result = pg_add_s64_overflow(arg, 1).unwrap_or_else(|| bigint_out_of_range());
    Int64GetDatum(result)
}

/// PG `int8inc_any`: COUNT(expr) transition fn -- ignores arg 1 (the counted
/// value, already known non-null by the caller) and increments the count in arg 0.
pub fn int8inc_any(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    int8inc(fcinfo)
}

/// PG `int8larger`: max of two int64.
pub fn int8larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int64(fcinfo, 0);
    let arg2 = pg_getarg_int64(fcinfo, 1);
    Int64GetDatum(if arg1 > arg2 { arg1 } else { arg2 })
}

/// PG `int8smaller`: min of two int64.
pub fn int8smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int64(fcinfo, 0);
    let arg2 = pg_getarg_int64(fcinfo, 1);
    Int64GetDatum(if arg1 < arg2 { arg1 } else { arg2 })
}

/// PG `int8pl`: int64 + int64 (the count/sum combine fn).
pub fn int8pl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int64(fcinfo, 0);
    let arg2 = pg_getarg_int64(fcinfo, 1);
    Int64GetDatum(pg_add_s64_overflow(arg1, arg2).unwrap_or_else(|| bigint_out_of_range()))
}

/// PG `int4_sum`: SUM(int4) transition fn -- accumulate an int4 into an int8
/// running sum. NOT strict on the state: a NULL state seeds from the first
/// non-null input (PG marks it strict only on the input, handled by the caller
/// passing non-null inputs; we replicate the explicit null checks).
pub fn int4_sum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    if PG_ARGISNULL(fcinfo, 0) {
        if PG_ARGISNULL(fcinfo, 1) {
            fcinfo.isnull = true;
            return Datum(0);
        }
        return Int64GetDatum(i64::from(pg_getarg_int32(fcinfo, 1)));
    }
    let oldsum = pg_getarg_int64(fcinfo, 0);
    if PG_ARGISNULL(fcinfo, 1) {
        return Int64GetDatum(oldsum);
    }
    Int64GetDatum(oldsum + i64::from(pg_getarg_int32(fcinfo, 1)))
}

/// PG `int2_sum`: SUM(int2) transition fn -- like int4_sum over int16 input.
pub fn int2_sum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    if PG_ARGISNULL(fcinfo, 0) {
        if PG_ARGISNULL(fcinfo, 1) {
            fcinfo.isnull = true;
            return Datum(0);
        }
        return Int64GetDatum(i64::from(pg_getarg_int16(fcinfo, 1)));
    }
    let oldsum = pg_getarg_int64(fcinfo, 0);
    if PG_ARGISNULL(fcinfo, 1) {
        return Int64GetDatum(oldsum);
    }
    Int64GetDatum(oldsum + i64::from(pg_getarg_int16(fcinfo, 1)))
}

/// bigint out of range error (PG int8.c's overflow ereport).
fn bigint_out_of_range() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE).errmsg("bigint out of range");
    });
    unreachable!("ereport!(ERROR) raises")
}

// ===========================================================================
//   BIT-PUSHING OPERATORS
// ===========================================================================

/// PG `int4and`: arg1 & arg2.
pub fn int4and(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(pg_getarg_int32(fcinfo, 0) & pg_getarg_int32(fcinfo, 1))
}
/// PG `int4or`: arg1 | arg2.
pub fn int4or(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(pg_getarg_int32(fcinfo, 0) | pg_getarg_int32(fcinfo, 1))
}
/// PG `int4xor`: arg1 ^ arg2.
pub fn int4xor(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(pg_getarg_int32(fcinfo, 0) ^ pg_getarg_int32(fcinfo, 1))
}
/// PG `int4shl`: arg1 << arg2. C `int << int` with an out-of-range count is UB
/// that real hardware masks to the low bits; `wrapping_shl` is that masking,
/// made defined.
pub fn int4shl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    Int32GetDatum(arg1.wrapping_shl(arg2 as u32))
}
/// PG `int4shr`: arg1 >> arg2 (arithmetic shift; see int4shl on the count).
pub fn int4shr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    Int32GetDatum(arg1.wrapping_shr(arg2 as u32))
}
/// PG `int4not`: ~arg1.
pub fn int4not(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(!pg_getarg_int32(fcinfo, 0))
}

/// PG `int2and`: arg1 & arg2.
pub fn int2and(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int16GetDatum(pg_getarg_int16(fcinfo, 0) & pg_getarg_int16(fcinfo, 1))
}
/// PG `int2or`: arg1 | arg2.
pub fn int2or(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int16GetDatum(pg_getarg_int16(fcinfo, 0) | pg_getarg_int16(fcinfo, 1))
}
/// PG `int2xor`: arg1 ^ arg2.
pub fn int2xor(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int16GetDatum(pg_getarg_int16(fcinfo, 0) ^ pg_getarg_int16(fcinfo, 1))
}
/// PG `int2not`: ~arg1.
pub fn int2not(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int16GetDatum(!pg_getarg_int16(fcinfo, 0))
}
/// PG `int2shl`: arg1 << arg2. C promotes int16 to int (32-bit) before the
/// shift and truncates the result back to int16.
pub fn int2shl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    Int16GetDatum(i32::from(arg1).wrapping_shl(arg2 as u32) as i16)
}
/// PG `int2shr`: arg1 >> arg2 (arithmetic shift; int16 promoted to int first).
pub fn int2shr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int16(fcinfo, 0);
    let arg2 = pg_getarg_int32(fcinfo, 1);
    Int16GetDatum(i32::from(arg1).wrapping_shr(arg2 as u32) as i16)
}

// ===========================================================================
//   non-persistent numeric series generator
// ===========================================================================

/// Cross-call state for the integer generate_series SRFs (C `generate_series_fctx`).
struct GenerateSeriesFctx<T> {
    current: T,
    finish: T,
    step: T,
}

/// PG `generate_series_int4`: generate_series with implicit step 1.
pub fn generate_series_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    generate_series_step_int4(fcinfo)
}

/// PG `generate_series_step_int4`: the SRF generate_series(int4, int4 [, int4]).
/// ValuePerCall: the first call reads (start, finish, step) into the cross-call
/// [`crate::funcapi::FuncCallContext`] `user_fctx`; each call returns the current
/// value and advances, ending when the series is exhausted.
pub fn generate_series_step_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    use crate::funcapi::{SRF_FIRSTCALL_INIT, SRF_IS_FIRSTCALL, SRF_PERCALL_SETUP, SRF_RETURN_DONE, SRF_RETURN_NEXT};

    if SRF_IS_FIRSTCALL(fcinfo) {
        let start = pg_getarg_int32(fcinfo, 0);
        let finish = pg_getarg_int32(fcinfo, 1);
        let step = if PG_NARGS(fcinfo) == 3 { pg_getarg_int32(fcinfo, 2) } else { 1 };
        if step == 0 {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_INVALID_PARAMETER_VALUE).errmsg("step size cannot equal zero");
            });
        }
        let funcctx = SRF_FIRSTCALL_INIT(fcinfo);
        funcctx.user_fctx = Some(Box::new(GenerateSeriesFctx { current: start, finish, step }));
    }

    let funcctx = SRF_PERCALL_SETUP(fcinfo);
    let fctx = funcctx
        .user_fctx
        .as_mut()
        .and_then(|b| b.downcast_mut::<GenerateSeriesFctx<i32>>())
        .unwrap_or_else(|| unreachable!("generate_series_step_int4: user_fctx set on first call"));
    let result = fctx.current;

    if (fctx.step > 0 && fctx.current <= fctx.finish) || (fctx.step < 0 && fctx.current >= fctx.finish) {
        // Increment; if the next-value computation overflows, this is the final result.
        match pg_add_s32_overflow(fctx.current, fctx.step) {
            Some(next) => fctx.current = next,
            None => fctx.step = 0,
        }
        SRF_RETURN_NEXT(fcinfo, Int32GetDatum(result))
    } else {
        SRF_RETURN_DONE(fcinfo)
    }
}

/// PG `generate_series_int8`: generate_series with implicit step 1.
pub fn generate_series_int8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    generate_series_step_int8(fcinfo)
}

/// PG `generate_series_step_int8`: the SRF generate_series(int8, int8 [, int8]).
pub fn generate_series_step_int8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    use crate::funcapi::{SRF_FIRSTCALL_INIT, SRF_IS_FIRSTCALL, SRF_PERCALL_SETUP, SRF_RETURN_DONE, SRF_RETURN_NEXT};

    if SRF_IS_FIRSTCALL(fcinfo) {
        let start = pg_getarg_int64(fcinfo, 0);
        let finish = pg_getarg_int64(fcinfo, 1);
        let step = if PG_NARGS(fcinfo) == 3 { pg_getarg_int64(fcinfo, 2) } else { 1 };
        if step == 0 {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_INVALID_PARAMETER_VALUE).errmsg("step size cannot equal zero");
            });
        }
        let funcctx = SRF_FIRSTCALL_INIT(fcinfo);
        funcctx.user_fctx = Some(Box::new(GenerateSeriesFctx { current: start, finish, step }));
    }

    let funcctx = SRF_PERCALL_SETUP(fcinfo);
    let fctx = funcctx
        .user_fctx
        .as_mut()
        .and_then(|b| b.downcast_mut::<GenerateSeriesFctx<i64>>())
        .unwrap_or_else(|| unreachable!("generate_series_step_int8: user_fctx set on first call"));
    let result = fctx.current;

    if (fctx.step > 0 && fctx.current <= fctx.finish) || (fctx.step < 0 && fctx.current >= fctx.finish) {
        match pg_add_s64_overflow(fctx.current, fctx.step) {
            Some(next) => fctx.current = next,
            None => fctx.step = 0,
        }
        SRF_RETURN_NEXT(fcinfo, Int64GetDatum(result))
    } else {
        SRF_RETURN_DONE(fcinfo)
    }
}

/// PG `int8gcd_internal`: gcd(arg1, arg2) with INT64_MIN special-casing (int8.c).
#[allow(
    clippy::suboptimal_flops,
    reason = "1:1 port: int8.c computes -abs() in NEGATIVE space so INT64_MIN doesn't overflow; `-x.abs()` would panic on INT64_MIN"
)]
fn int8gcd_internal(mut arg1: i64, mut arg2: i64) -> i64 {
    // Put the greater absolute value in arg1, working in negative space to handle
    // INT64_MIN.
    let a1 = if arg1 < 0 { arg1 } else { -arg1 };
    let a2 = if arg2 < 0 { arg2 } else { -arg2 };
    if a1 > a2 {
        std::mem::swap(&mut arg1, &mut arg2);
    }

    if arg1 == PG_INT64_MIN {
        if arg2 == 0 || arg2 == PG_INT64_MIN {
            bigint_out_of_range();
        }
        // gcd(INT64_MIN, -1) = 1 (guard the INT64_MIN % -1 trap).
        if arg2 == -1 {
            return 1;
        }
    }

    // Iterate: arg1 = gcd so far, arg2 = remainder.
    while arg2 != 0 {
        let rem = arg1 % arg2;
        arg1 = arg2;
        arg2 = rem;
    }

    // Absolute value of the result (arg1 may be negative here).
    if arg1 < 0 {
        arg1 = arg1.checked_neg().unwrap_or_else(|| bigint_out_of_range());
    }
    arg1
}

/// PG `int8gcd`: greatest common divisor of two bigints.
pub fn int8gcd(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int64(fcinfo, 0);
    let arg2 = pg_getarg_int64(fcinfo, 1);
    Int64GetDatum(int8gcd_internal(arg1, arg2))
}

/// PG `int8lcm`: least common multiple of two bigints.
pub fn int8lcm(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int64(fcinfo, 0);
    let arg2 = pg_getarg_int64(fcinfo, 1);

    // lcm(x, 0) = lcm(0, x) = 0.
    if arg1 == 0 || arg2 == 0 {
        return Int64GetDatum(0);
    }

    // lcm(x, y) = abs(x / gcd(x, y) * y).
    let gcd = int8gcd_internal(arg1, arg2);
    let a1 = arg1 / gcd;
    let mut result = crate::common::int::pg_mul_s64_overflow(a1, arg2).unwrap_or_else(|| bigint_out_of_range());
    if result == PG_INT64_MIN {
        bigint_out_of_range();
    }
    if result < 0 {
        result = -result;
    }
    Int64GetDatum(result)
}

/// PG `generate_series_int4_support`: planner support estimating result rows.
///
/// Reaches the support-node introspection (`IsA`, `estimate_expression_value`,
/// `is_funcclause`) which is not yet translated; calls those stubs (rules.md s4).
pub fn generate_series_int4_support(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("generate_series_int4_support needs support-node introspection (nodeFuncs)")
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::fmgr::FunctionCallInfoBaseData;
    use crate::postgres::NullableDatum;
    use std::panic::catch_unwind;

    /// Build an fcinfo with the given non-null Datum args.
    fn fc(args: &[Datum]) -> FunctionCallInfoBaseData {
        FunctionCallInfoBaseData {
            flinfo: None,
            context: None,
            resultinfo: None,
            fncollation: crate::postgres_ext::InvalidOid,
            isnull: false,
            nargs: args.len() as i16,
            args: args
                .iter()
                .map(|&value| NullableDatum { value, isnull: false })
                .collect(),
        }
    }

    /// Make a cstring Datum (leaked, like a palloc'd arg) for int*in tests.
    fn cstr_datum(s: &str) -> Datum {
        let c = std::ffi::CString::new(s).unwrap();
        CStringGetDatum(c.into_raw())
    }

    fn out_to_string(d: Datum) -> String {
        let p = DatumGetCString(d);
        let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
        cstr.to_string_lossy().into_owned()
    }

    #[test]
    fn int4_in_out_roundtrip() {
        for s in ["1", "0", "-1", "2147483647", "-2147483648", "42"] {
            let mut in_fc = fc(&[cstr_datum(s)]);
            let d = int4in(&mut in_fc);
            let mut out_fc = fc(&[d]);
            let back = out_to_string(int4out(&mut out_fc));
            assert_eq!(back, s, "roundtrip of {s}");
        }
    }

    #[test]
    fn int4_in_boundaries_and_bases() {
        let cases = [
            ("2147483647", 2_147_483_647i32),
            ("-2147483648", -2_147_483_648i32),
            ("  17  ", 17),
            ("+5", 5),
            ("0x10", 16),
            ("0o17", 15),
            ("0b101", 5),
            ("1_000", 1000),
        ];
        for (s, want) in cases {
            let mut f = fc(&[cstr_datum(s)]);
            assert_eq!(DatumGetInt32(int4in(&mut f)), want, "parse {s}");
        }
    }

    #[test]
    fn int4_in_overflow_and_syntax_error() {
        for bad in ["2147483648", "-2147483649", "99999999999"] {
            let s = bad.to_owned();
            let r = catch_unwind(move || {
                let mut f = fc(&[cstr_datum(&s)]);
                int4in(&mut f)
            });
            assert!(r.is_err(), "{bad} should overflow");
        }
        for bad in ["", "abc", "1.5", "12x", "_5", "5_"] {
            let s = bad.to_owned();
            let r = catch_unwind(move || {
                let mut f = fc(&[cstr_datum(&s)]);
                int4in(&mut f)
            });
            assert!(r.is_err(), "{bad} should be invalid syntax");
        }
    }

    #[test]
    fn underscore_after_nondecimal_prefix() {
        // Non-decimal bases allow an underscore right after the 0x/0o/0b prefix
        // (numutils.c only forbids a leading underscore in decimal).
        for (s, want) in [("0b_10_0101", 37i32), ("0xE_FF", 0xEFF), ("0o2_73", 0o273)] {
            let mut f = fc(&[cstr_datum(s)]);
            assert_eq!(DatumGetInt32(int4in(&mut f)), want, "parse {s}");
        }
        for bad in ["_100", "100_", "10__000", "0b_", "0x_"] {
            let s = bad.to_owned();
            let r = catch_unwind(move || {
                let mut f = fc(&[cstr_datum(&s)]);
                int4in(&mut f)
            });
            assert!(r.is_err(), "{bad} should be invalid syntax");
        }
    }

    #[test]
    fn strtoint64_and_int8in() {
        assert_eq!(strtoint64_safe("9223372036854775807", None), Some(i64::MAX));
        assert_eq!(strtoint64_safe("-9223372036854775808", None), Some(i64::MIN));
        assert_eq!(strtoint64_safe("0x7FFF_FFFF_FFFF_FFFF", None), Some(i64::MAX));
        assert!(catch_unwind(|| strtoint64_safe("9223372036854775808", None)).is_err());
        assert!(catch_unwind(|| strtoint64_safe("12abc", None)).is_err());
        let mut f = fc(&[cstr_datum("4567890123456789")]);
        assert_eq!(DatumGetInt64(int8in(&mut f)), 4_567_890_123_456_789);
    }

    #[test]
    fn int8_int4_int2_conversions() {
        let mut f = fc(&[Int32GetDatum(-7)]);
        assert_eq!(DatumGetInt64(int48(&mut f)), -7);
        let mut f = fc(&[Int64GetDatum(123)]);
        assert_eq!(DatumGetInt32(int84(&mut f)), 123);
        assert!(catch_unwind(|| {
            let mut f = fc(&[Int64GetDatum(i64::from(PG_INT32_MAX) + 1)]);
            int84(&mut f)
        })
        .is_err());
        let mut f = fc(&[Int16GetDatum(-3)]);
        assert_eq!(DatumGetInt64(int28(&mut f)), -3);
        assert!(catch_unwind(|| {
            let mut f = fc(&[Int64GetDatum(40000)]);
            int82(&mut f)
        })
        .is_err());
    }

    #[test]
    fn int2vector_in_out() {
        let mut f = fc(&[cstr_datum(" 1 3  5 ")]);
        let d = int2vectorin(&mut f);
        let mut out_fc = fc(&[d]);
        assert_eq!(out_to_string(int2vectorout(&mut out_fc)), "1 3 5");
        // Bad element -> invalid syntax carrying the remaining text.
        let payload = catch_unwind(|| {
            let mut f = fc(&[cstr_datum("1 asdf")]);
            int2vectorin(&mut f)
        })
        .expect_err("must raise");
        let edata = payload
            .downcast_ref::<crate::utils::elog::ErrorData>()
            .expect("payload is ErrorData");
        assert_eq!(
            edata.message.as_deref(),
            Some(r#"invalid input syntax for type smallint: "asdf""#)
        );
        // Out of int16 range.
        assert!(catch_unwind(|| {
            let mut f = fc(&[cstr_datum("50000")]);
            int2vectorin(&mut f)
        })
        .is_err());
    }

    #[test]
    fn int2_in_out_roundtrip_and_overflow() {
        for s in ["1", "-1", "32767", "-32768"] {
            let mut in_fc = fc(&[cstr_datum(s)]);
            let d = int2in(&mut in_fc);
            let mut out_fc = fc(&[d]);
            assert_eq!(out_to_string(int2out(&mut out_fc)), s);
        }
        let r = catch_unwind(|| {
            let mut f = fc(&[cstr_datum("32768")]);
            int2in(&mut f)
        });
        assert!(r.is_err());
    }

    #[test]
    fn int4_arithmetic_and_overflow() {
        let mut f = fc(&[Int32GetDatum(2), Int32GetDatum(3)]);
        assert_eq!(DatumGetInt32(int4pl(&mut f)), 5);
        let mut f = fc(&[Int32GetDatum(10), Int32GetDatum(3)]);
        assert_eq!(DatumGetInt32(int4mi(&mut f)), 7);
        let mut f = fc(&[Int32GetDatum(6), Int32GetDatum(7)]);
        assert_eq!(DatumGetInt32(int4mul(&mut f)), 42);
        let mut f = fc(&[Int32GetDatum(20), Int32GetDatum(6)]);
        assert_eq!(DatumGetInt32(int4div(&mut f)), 3);
        let mut f = fc(&[Int32GetDatum(20), Int32GetDatum(6)]);
        assert_eq!(DatumGetInt32(int4mod(&mut f)), 2);

        // overflow paths raise
        assert!(catch_unwind(|| {
            let mut f = fc(&[Int32GetDatum(PG_INT32_MAX), Int32GetDatum(1)]);
            int4pl(&mut f)
        })
        .is_err());
        assert!(catch_unwind(|| {
            let mut f = fc(&[Int32GetDatum(PG_INT32_MAX), Int32GetDatum(2)]);
            int4mul(&mut f)
        })
        .is_err());
        // division by zero raises
        assert!(catch_unwind(|| {
            let mut f = fc(&[Int32GetDatum(1), Int32GetDatum(0)]);
            int4div(&mut f)
        })
        .is_err());
        // INT_MIN / -1 raises
        assert!(catch_unwind(|| {
            let mut f = fc(&[Int32GetDatum(PG_INT32_MIN), Int32GetDatum(-1)]);
            int4div(&mut f)
        })
        .is_err());
        // INT_MIN % -1 == 0 (no FPE)
        let mut f = fc(&[Int32GetDatum(PG_INT32_MIN), Int32GetDatum(-1)]);
        assert_eq!(DatumGetInt32(int4mod(&mut f)), 0);
    }

    #[test]
    fn comparisons() {
        let mut f = fc(&[Int32GetDatum(1), Int32GetDatum(1)]);
        assert!(DatumGetBool(int4eq(&mut f)));
        let mut f = fc(&[Int32GetDatum(1), Int32GetDatum(2)]);
        assert!(DatumGetBool(int4lt(&mut f)));
        assert!(DatumGetBool(int4ne(&mut f)));
        let mut f = fc(&[Int32GetDatum(2), Int32GetDatum(1)]);
        assert!(DatumGetBool(int4gt(&mut f)));
        // cross-width
        let mut f = fc(&[Int16GetDatum(5), Int32GetDatum(5)]);
        assert!(DatumGetBool(int24eq(&mut f)));
        let mut f = fc(&[Int32GetDatum(5), Int16GetDatum(6)]);
        assert!(DatumGetBool(int42lt(&mut f)));
        // int2
        let mut f = fc(&[Int16GetDatum(3), Int16GetDatum(3)]);
        assert!(DatumGetBool(int2ge(&mut f)));
    }

    #[test]
    fn int8_gcd_lcm() {
        let mut f = fc(&[Int64GetDatum(1071), Int64GetDatum(462)]);
        assert_eq!(DatumGetInt64(int8gcd(&mut f)), 21);
        let mut f = fc(&[Int64GetDatum(6), Int64GetDatum(4)]);
        assert_eq!(DatumGetInt64(int8lcm(&mut f)), 12);
        // lcm(x, 0) = 0.
        let mut f = fc(&[Int64GetDatum(5), Int64GetDatum(0)]);
        assert_eq!(DatumGetInt64(int8lcm(&mut f)), 0);
        // gcd(INT64_MIN, 0) overflows.
        assert!(catch_unwind(|| {
            let mut f = fc(&[Int64GetDatum(PG_INT64_MIN), Int64GetDatum(0)]);
            int8gcd(&mut f)
        })
        .is_err());
        // gcd(INT64_MIN, -1) = 1 (no FPE).
        let mut f = fc(&[Int64GetDatum(PG_INT64_MIN), Int64GetDatum(-1)]);
        assert_eq!(DatumGetInt64(int8gcd(&mut f)), 1);
    }

    #[test]
    fn gcd_lcm_abs() {
        let mut f = fc(&[Int32GetDatum(12), Int32GetDatum(18)]);
        assert_eq!(DatumGetInt32(int4gcd(&mut f)), 6);
        let mut f = fc(&[Int32GetDatum(4), Int32GetDatum(6)]);
        assert_eq!(DatumGetInt32(int4lcm(&mut f)), 12);
        let mut f = fc(&[Int32GetDatum(-5)]);
        assert_eq!(DatumGetInt32(int4abs(&mut f)), 5);
        assert!(catch_unwind(|| {
            let mut f = fc(&[Int32GetDatum(PG_INT32_MIN)]);
            int4abs(&mut f)
        })
        .is_err());
    }

    #[test]
    fn bit_ops_and_shifts() {
        let mut f = fc(&[Int32GetDatum(0b1100), Int32GetDatum(0b1010)]);
        assert_eq!(DatumGetInt32(int4and(&mut f)), 0b1000);
        let mut f = fc(&[Int32GetDatum(0b1100), Int32GetDatum(0b1010)]);
        assert_eq!(DatumGetInt32(int4or(&mut f)), 0b1110);
        let mut f = fc(&[Int32GetDatum(1), Int32GetDatum(4)]);
        assert_eq!(DatumGetInt32(int4shl(&mut f)), 16);
        let mut f = fc(&[Int32GetDatum(-16), Int32GetDatum(2)]);
        assert_eq!(DatumGetInt32(int4shr(&mut f)), -4);
        // int2 shift promotes to int then truncates; large counts don't panic.
        let mut f = fc(&[Int16GetDatum(1), Int32GetDatum(20)]);
        assert_eq!(DatumGetInt16(int2shl(&mut f)), 0);
        let mut f = fc(&[Int16GetDatum(1), Int32GetDatum(8)]);
        assert_eq!(DatumGetInt16(int2shl(&mut f)), 256);
    }

    /// int4out resolves through the generated fmgr table to a bound function and
    /// OutputFunctionCall yields "1".
    #[test]
    fn fmgr_table_binds_int4out() {
        use crate::utils::fmgrtab::{fmgr_builtins, FmgrBuiltin};
        let int4out_entry: Option<&FmgrBuiltin> =
            fmgr_builtins.iter().find(|b| b.func_name == "int4out");
        let entry = int4out_entry.expect("int4out present in builtin table");
        let func = entry.func.expect("int4out is bound to a Rust fn");

        // Invoke through OutputFunctionCall on a flinfo carrying that fn_addr.
        let mut flinfo = crate::fmgr::FmgrInfo {
            fn_addr: Some(func),
            oid: entry.foid,
            nargs: entry.nargs,
            strict: entry.strict,
            retset: entry.retset,
            stats: 0,
            extra: 0,
            mcxt: (),
            expr: None,
        };
        let s = crate::fmgr::OutputFunctionCall(&mut flinfo, Int32GetDatum(1));
        assert_eq!(s, "1");
    }

    /// int4in also resolves and parses "1" through the table-bound fn.
    #[test]
    fn fmgr_table_binds_int4in() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "int4in")
            .expect("int4in present");
        let func = entry.func.expect("int4in bound");
        let mut f = fc(&[cstr_datum("1")]);
        assert_eq!(DatumGetInt32(func(&mut f)), 1);
    }
}
