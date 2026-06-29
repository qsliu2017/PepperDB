//! Arbitrary-precision exact numeric type `numeric`. Translated from
//! src/backend/utils/adt/numeric.c (one of PG's largest files; the core is
//! translated completely, the transcendental/scientific surface is staged --
//! see the per-fn notes).
//!
//! FULLY TRANSLATED (the M3-and-beyond arithmetic surface):
//!   - representation: [`NumericVar`] (owned base-NBASE digit array with weight,
//!     sign, dscale) and the on-disk varlena packing (`make_result` /
//!     `set_var_from_num` / `init_var_from_num`), faithful to PG's short/long
//!     header + digit encoding;
//!   - I/O: `numeric_in`/`numeric_out`/`numeric_out_sci` (decimal, non-decimal
//!     integer prefixes 0x/0o/0b, scientific notation, NaN/Infinity, dscale);
//!   - arithmetic: `numeric_add`/`sub`/`mul`/`div`/`div_trunc`/`mod`/`abs`/
//!     `uminus`/`uplus`/`inc`, the kernels add_var/sub_var/mul_var/div_var/
//!     mod_var/round_var/trunc_var/cmp_var/cmp_abs/add_abs/sub_abs/strip_var,
//!     ceil/floor/round/trunc/sign;
//!   - comparison: `numeric_cmp`/`eq`/`ne`/`lt`/`le`/`gt`/`ge` (+ NaN/Inf order);
//!   - casts: int2/int4/int8 <-> numeric, float4/float8 <-> numeric, numeric()
//!     typmod application, numeric_int4/int8_opt_error;
//!   - smaller/larger, gcd/lcm, scale/min_scale/trim_scale.
//!
//! STAGED (`unimplemented!()`; not reachable until far-later milestones, the
//! full-file principle is relaxed here for size -- rules.md s4):
//!   - the transcendental/scientific functions: numeric_sqrt, numeric_exp,
//!     numeric_ln, numeric_log, numeric_power, numeric_fac;
//!   - numeric_random (needs the prng-state arg marshalling);
//!   - the aggregate accumulators numeric_accum/numeric_avg_accum/numeric_sum/
//!     numeric_avg/stddev/variance/combine/serialize and the poly_* variants:
//!     these reach the executor aggregate context (AggCheckCallContext +
//!     MemoryContextAlloc to materialize the NumericAggState pointer-through-
//!     Datum) which is not translated (same precedent as bool.c's bool_accum);
//!   - numeric_recv/numeric_send: stage on the binary-wire StringInfo/MsgReader,
//!     not yet marshalled (same as int4recv/int4send);
//!   - numeric_sortsupport / abbreviation, numeric_support (planner), the
//!     to_char/to_number/cash/pg_lsn conversions, generate_series(numeric).
//!
//! VARLENA / DIGIT BUFFER: a packed on-disk numeric is a 4-byte-header varlena
//! built by `make_varlena` (a leaked owned `Box<[u8]>`, exactly like varlena.rs,
//! since palloc is still a stub) carrying PG's NumericShort/NumericLong header
//! and the base-NBASE digits. In-memory arithmetic uses [`NumericVar`] whose
//! digits are an owned `Vec<i16>` (no `*mut`/spare-digit pointer trick; the
//! carry-out-the-top case is handled by prepending to the Vec), so the whole
//! type is alignment-sound and `Send`.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    reason = "faithful C width arithmetic: numeric.c does explicit int16/int32/\
              int64 casts, base-NBASE digit truncation, and float estimation in \
              long division (the value-cast family is an allowed port-inherent \
              lint per rules.md s11)"
)]
#![allow(
    clippy::similar_names,
    reason = "numeric.c names its operands var1/var2, indices i1/i2, digits d0/d1, \
              and fcinfo args num1/num2/arg1/arg2; preserving these faithful C \
              identifiers trips the nursery similar_names heuristic (port-inherent)"
)]

use crate::c::{
    PG_INT16_MAX, PG_INT16_MIN, PG_INT32_MAX, PG_INT32_MIN, PG_INT64_MAX, PG_INT64_MIN,
    PG_UINT64_MAX, VARHDRSZ,
};
use crate::common::int::{
    pg_abs_s64, pg_add_u64_overflow, pg_mul_s64_overflow, pg_mul_u64_overflow, pg_sub_s64_overflow,
};
use crate::ereport;
use crate::fmgr::FunctionCallInfoBaseData;
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, Datum, DatumGetCString, DatumGetFloat4, DatumGetFloat8,
    DatumGetInt16, DatumGetInt32, DatumGetInt64, DatumGetPointer, Float4GetDatum, Float8GetDatum,
    Int16GetDatum, Int32GetDatum, Int64GetDatum, PointerGetDatum,
};
use crate::utils::elog::ERROR;
use crate::utils::errcodes::{
    ERRCODE_DIVISION_BY_ZERO, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use crate::varatt::{SET_VARSIZE, VARDATA, VARSIZE_ANY};

// ---------------------------------------------------------------------------
// Representation constants (numeric.c). NBASE=10000, DEC_DIGITS=4.
// ---------------------------------------------------------------------------

pub type NumericDigit = i16;

const NBASE: i32 = 10000;
const HALF_NBASE: i32 = 5000;
const DEC_DIGITS: usize = 4;
const MUL_GUARD_DIGITS: i32 = 2;
const DIV_GUARD_DIGITS: i32 = 4;
const NBASE_SQR: u64 = (NBASE as u64) * (NBASE as u64);

// Sign / flag bits.
const NUMERIC_SIGN_MASK: u16 = 0xC000;
const NUMERIC_POS: i32 = 0x0000;
const NUMERIC_NEG: i32 = 0x4000;
const NUMERIC_SHORT: u16 = 0x8000;
const NUMERIC_SPECIAL: u16 = 0xC000;

const NUMERIC_NAN: i32 = 0xC000;
const NUMERIC_PINF: i32 = 0xD000;
const NUMERIC_NINF: i32 = 0xF000;
const NUMERIC_EXT_SIGN_MASK: u16 = 0xF000;
const NUMERIC_INF_SIGN_MASK: u16 = 0x2000;

const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
const NUMERIC_SHORT_DSCALE_MASK: u16 = 0x1F80;
const NUMERIC_SHORT_DSCALE_SHIFT: u16 = 7;
const NUMERIC_SHORT_DSCALE_MAX: i32 = (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT) as i32;
const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
const NUMERIC_SHORT_WEIGHT_MASK: u16 = 0x003F;
const NUMERIC_SHORT_WEIGHT_MAX: i32 = NUMERIC_SHORT_WEIGHT_MASK as i32;
const NUMERIC_SHORT_WEIGHT_MIN: i32 = -(NUMERIC_SHORT_WEIGHT_MASK as i32 + 1);

const NUMERIC_DSCALE_MASK: u16 = 0x3FFF;
const NUMERIC_DSCALE_MAX: i32 = NUMERIC_DSCALE_MASK as i32;
const NUMERIC_WEIGHT_MAX: i32 = PG_INT16_MAX as i32;

const NUMERIC_HDRSZ: usize = VARHDRSZ as usize + 2 + 2; // varlena + uint16 + int16
const NUMERIC_HDRSZ_SHORT: usize = VARHDRSZ as usize + 2;

// Internal scale limits (also in utils/numeric.h header stub).
const NUMERIC_MIN_SIG_DIGITS: i32 = 16;
const NUMERIC_MIN_DISPLAY_SCALE: i32 = 0;
const NUMERIC_MAX_PRECISION: i32 = 1000;
const NUMERIC_MAX_DISPLAY_SCALE: i32 = NUMERIC_MAX_PRECISION;

const ROUND_POWERS: [i32; 4] = [0, 1000, 100, 10];

// ---------------------------------------------------------------------------
// NumericVar: the in-memory arithmetic format. Owned, Send, alignment-sound.
//
// PG keeps a separate palloc'd `buf`/`digits` with a leading spare digit for
// carry-out-the-top. We use an owned `Vec<i16>` instead; the spare-digit cases
// (round_var carry, mul/add carry) are handled by `digits.insert(0, ...)`.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct NumericVar {
    weight: i32,
    sign: i32, // NUMERIC_POS/_NEG/_NAN/_PINF/_NINF
    dscale: i32,
    digits: Vec<NumericDigit>,
}

impl NumericVar {
    fn new() -> Self {
        Self { weight: 0, sign: NUMERIC_POS, dscale: 0, digits: Vec::new() }
    }
    fn ndigits(&self) -> i32 {
        self.digits.len() as i32
    }
    fn zero(&mut self) {
        self.digits.clear();
        self.weight = 0;
        self.sign = NUMERIC_POS;
    }
}

const fn const_var(weight: i32, sign: i32, dscale: i32) -> NumericVar {
    NumericVar { weight, sign, dscale, digits: Vec::new() }
}

fn const_zero() -> NumericVar {
    const_var(0, NUMERIC_POS, 0)
}
fn const_one() -> NumericVar {
    NumericVar { weight: 0, sign: NUMERIC_POS, dscale: 0, digits: vec![1] }
}
fn const_minus_one() -> NumericVar {
    NumericVar { weight: 0, sign: NUMERIC_NEG, dscale: 0, digits: vec![1] }
}
fn const_nan() -> NumericVar {
    const_var(0, NUMERIC_NAN, 0)
}
fn const_pinf() -> NumericVar {
    const_var(0, NUMERIC_PINF, 0)
}
fn const_ninf() -> NumericVar {
    const_var(0, NUMERIC_NINF, 0)
}

// ---------------------------------------------------------------------------
// Packed (on-disk) numeric: a varlena pointer carried in a byref Datum.
// We provide typed read accessors over the raw bytes and a builder.
// ---------------------------------------------------------------------------

/// A borrowed view over a packed numeric's bytes (post-varlena-header).
struct PackedNumeric {
    bytes: Vec<u8>, // full varlena (incl. 4-byte header)
}

impl PackedNumeric {
    /// The first header word (n_header / n_short.n_header / n_long.n_sign_dscale).
    fn header(&self) -> u16 {
        u16::from_ne_bytes([self.bytes[VARHDRSZ as usize], self.bytes[VARHDRSZ as usize + 1]])
    }
    fn flagbits(&self) -> u16 {
        self.header() & NUMERIC_SIGN_MASK
    }
    fn is_short(&self) -> bool {
        self.flagbits() == NUMERIC_SHORT
    }
    fn is_special(&self) -> bool {
        self.flagbits() == NUMERIC_SPECIAL
    }
    fn header_is_short(&self) -> bool {
        (self.header() & 0x8000) != 0
    }
    fn is_nan(&self) -> bool {
        self.header() == NUMERIC_NAN as u16
    }
    fn is_pinf(&self) -> bool {
        self.header() == NUMERIC_PINF as u16
    }
    fn is_ninf(&self) -> bool {
        self.header() == NUMERIC_NINF as u16
    }
    fn is_inf(&self) -> bool {
        (self.header() & !NUMERIC_INF_SIGN_MASK) == NUMERIC_PINF as u16
    }
    fn header_size(&self) -> usize {
        if self.header_is_short() {
            NUMERIC_HDRSZ_SHORT
        } else {
            NUMERIC_HDRSZ
        }
    }
    fn ndigits(&self) -> i32 {
        ((self.bytes.len() - self.header_size()) / 2) as i32
    }
    fn weight(&self) -> i32 {
        if self.header_is_short() {
            let h = self.header();
            let sign = if h & NUMERIC_SHORT_WEIGHT_SIGN_MASK != 0 {
                !i32::from(NUMERIC_SHORT_WEIGHT_MASK)
            } else {
                0
            };
            sign | i32::from(h & NUMERIC_SHORT_WEIGHT_MASK)
        } else {
            i32::from(i16::from_ne_bytes([
                self.bytes[VARHDRSZ as usize + 2],
                self.bytes[VARHDRSZ as usize + 3],
            ]))
        }
    }
    fn sign(&self) -> i32 {
        if self.is_short() {
            if self.header() & NUMERIC_SHORT_SIGN_MASK != 0 {
                NUMERIC_NEG
            } else {
                NUMERIC_POS
            }
        } else if self.is_special() {
            i32::from(self.header() & NUMERIC_EXT_SIGN_MASK)
        } else {
            i32::from(self.flagbits())
        }
    }
    fn dscale(&self) -> i32 {
        if self.header_is_short() {
            i32::from((self.header() & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT)
        } else {
            i32::from(self.header() & NUMERIC_DSCALE_MASK)
        }
    }
    fn digit(&self, i: usize) -> NumericDigit {
        let off = self.header_size() + 2 * i;
        i16::from_ne_bytes([self.bytes[off], self.bytes[off + 1]])
    }
    fn digits(&self) -> Vec<NumericDigit> {
        (0..self.ndigits() as usize).map(|i| self.digit(i)).collect()
    }
}

/// `PG_GETARG_NUMERIC(n)`: read the packed numeric bytes from a byref Datum.
fn pg_getarg_numeric(fcinfo: &FunctionCallInfoBaseData, n: usize) -> PackedNumeric {
    let p = DatumGetPointer(fcinfo.args[n].value);
    // SAFETY: a numeric argument is a non-toasted varlena that outlives the call.
    let total = unsafe { VARSIZE_ANY(p) };
    let bytes = unsafe { core::slice::from_raw_parts(p, total) }.to_vec();
    PackedNumeric { bytes }
}

/// `PG_RETURN_NUMERIC(n)`: hand back a packed numeric as a byref Datum.
fn pg_return_numeric(n: PackedNumeric) -> Datum {
    let total = n.bytes.len();
    let mut buf = n.bytes.into_boxed_slice();
    let ptr = buf.as_mut_ptr();
    // SAFETY: header already laid out by make_result; this just re-asserts the
    // varlena length over the leaked buffer.
    unsafe {
        SET_VARSIZE(ptr, total as u32);
    }
    PointerGetDatum(Box::leak(buf).as_ptr())
}

fn pg_getarg_cstring(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = DatumGetCString(fcinfo.args[n].value);
    // SAFETY: an input function's cstring argument outlives the call.
    let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
    cstr.to_string_lossy().into_owned()
}

fn pg_return_cstring(s: &str) -> Datum {
    let bytes: Vec<u8> = s.bytes().take_while(|&b| b != 0).collect();
    let c = std::ffi::CString::new(bytes).unwrap_or_default();
    CStringGetDatum(c.into_raw())
}

// ---------------------------------------------------------------------------
// Error raisers.
// ---------------------------------------------------------------------------

fn overflow_error() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg("value overflows numeric format");
    });
    unreachable!()
}
fn invalid_syntax(s: &str) -> ! {
    let sv = s.to_owned();
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .errmsg(format!("invalid input syntax for type numeric: \"{sv}\""));
    });
    unreachable!()
}
fn division_by_zero() -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_DIVISION_BY_ZERO).errmsg("division by zero");
    });
    unreachable!()
}
fn int_out_of_range(typname: &str) -> ! {
    let t = typname.to_owned();
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg(format!("{t} out of range"));
    });
    unreachable!()
}
fn cannot_convert_special(special: &str, typname: &str) -> ! {
    let (s, t) = (special.to_owned(), typname.to_owned());
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot convert {s} to {t}"));
    });
    unreachable!()
}

// ===========================================================================
//   Conversion: packed <-> NumericVar  (set_var_from_num / make_result)
// ===========================================================================

/// PG `init_var_from_num` + `set_var_from_num` (we always own a Vec copy).
fn var_from_num(num: &PackedNumeric) -> NumericVar {
    NumericVar {
        weight: num.weight(),
        sign: num.sign(),
        dscale: num.dscale(),
        digits: num.digits(),
    }
}

/// PG `make_result_opt_error`: pack a NumericVar into on-disk form.
/// On overflow of the int16 weight/dscale fields, raises (have_error=NULL path).
fn make_result(var: &NumericVar) -> PackedNumeric {
    let mut sign = var.sign;
    if (sign as u16 & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL {
        debug_assert!(sign == NUMERIC_NAN || sign == NUMERIC_PINF || sign == NUMERIC_NINF);
        let mut bytes = vec![0u8; NUMERIC_HDRSZ_SHORT];
        let h = (sign as u16).to_ne_bytes();
        bytes[VARHDRSZ as usize] = h[0];
        bytes[VARHDRSZ as usize + 1] = h[1];
        let p = PackedNumeric { bytes };
        return p;
    }

    let mut digits = var.digits.as_slice();
    let mut weight = var.weight;
    let mut n = var.ndigits() as usize;

    // truncate leading zeroes
    let mut lead = 0;
    while lead < n && digits[lead] == 0 {
        lead += 1;
        weight -= 1;
    }
    digits = &digits[lead..];
    n -= lead;
    // truncate trailing zeroes
    while n > 0 && digits[n - 1] == 0 {
        n -= 1;
    }
    if n == 0 {
        weight = 0;
        sign = NUMERIC_POS;
    }
    let digits = &digits[..n];

    let dscale = var.dscale;
    let can_be_short = dscale <= NUMERIC_SHORT_DSCALE_MAX
        && (NUMERIC_SHORT_WEIGHT_MIN..=NUMERIC_SHORT_WEIGHT_MAX).contains(&weight);

    let (header_size, mut bytes);
    if can_be_short {
        header_size = NUMERIC_HDRSZ_SHORT;
        bytes = vec![0u8; NUMERIC_HDRSZ_SHORT + n * 2];
        let h = (if sign == NUMERIC_NEG {
            NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK
        } else {
            NUMERIC_SHORT
        }) | ((dscale as u16) << NUMERIC_SHORT_DSCALE_SHIFT)
            | (if weight < 0 { NUMERIC_SHORT_WEIGHT_SIGN_MASK } else { 0 })
            | ((weight as u16) & NUMERIC_SHORT_WEIGHT_MASK);
        let hb = h.to_ne_bytes();
        bytes[VARHDRSZ as usize] = hb[0];
        bytes[VARHDRSZ as usize + 1] = hb[1];
    } else {
        header_size = NUMERIC_HDRSZ;
        bytes = vec![0u8; NUMERIC_HDRSZ + n * 2];
        let sign_dscale = (sign as u16) | ((dscale as u16) & NUMERIC_DSCALE_MASK);
        let sdb = sign_dscale.to_ne_bytes();
        bytes[VARHDRSZ as usize] = sdb[0];
        bytes[VARHDRSZ as usize + 1] = sdb[1];
        let wb = (weight as i16).to_ne_bytes();
        bytes[VARHDRSZ as usize + 2] = wb[0];
        bytes[VARHDRSZ as usize + 3] = wb[1];
    }
    for (i, &d) in digits.iter().enumerate() {
        let db = d.to_ne_bytes();
        bytes[header_size + 2 * i] = db[0];
        bytes[header_size + 2 * i + 1] = db[1];
    }

    let res = PackedNumeric { bytes };
    // Check for overflow of int16 fields.
    if res.weight() != weight || res.dscale() != dscale {
        overflow_error();
    }
    res
}

fn duplicate_numeric(num: &PackedNumeric) -> PackedNumeric {
    PackedNumeric { bytes: num.bytes.clone() }
}

// ===========================================================================
//   USER I/O ROUTINES
// ===========================================================================

/// PG `numeric_in`: input function for numeric.
#[allow(
    clippy::too_many_lines,
    reason = "faithful port of numeric.c's numeric_in: sign/special/base-prefix \
              detection + the decimal and non-decimal parse dispatch are a single \
              function in C"
)]
pub fn numeric_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str = pg_getarg_cstring(fcinfo, 0);
    let typmod = if fcinfo.nargs >= 3 {
        DatumGetInt32(fcinfo.args[2].value)
    } else {
        -1
    };
    let bytes = str.as_bytes();
    let mut cp = 0;
    while cp < bytes.len() && bytes[cp].is_ascii_whitespace() {
        cp += 1;
    }
    let numstart = cp;
    let mut sign = NUMERIC_POS;
    if cp < bytes.len() && bytes[cp] == b'+' {
        cp += 1;
    } else if cp < bytes.len() && bytes[cp] == b'-' {
        sign = NUMERIC_NEG;
        cp += 1;
    }

    let at_digit_or_dot =
        cp < bytes.len() && (bytes[cp].is_ascii_digit() || bytes[cp] == b'.');

    if !at_digit_or_dot {
        // NaN / Infinity, else syntax error.
        let rest = &str[numstart..];
        let res;
        let consumed;
        let inf = if sign == NUMERIC_POS { const_pinf() } else { const_ninf() };
        if rest.len() >= 3 && rest[..3].eq_ignore_ascii_case("NaN") && cp == numstart {
            // NaN must not have a sign: cp==numstart means no sign consumed.
            res = make_result(&const_nan());
            consumed = numstart + 3;
        } else if str[cp..].len() >= 8 && str[cp..][..8].eq_ignore_ascii_case("Infinity") {
            res = make_result(&inf);
            consumed = cp + 8;
        } else if str[cp..].len() >= 3 && str[cp..][..3].eq_ignore_ascii_case("inf") {
            res = make_result(&inf);
            consumed = cp + 3;
        } else {
            invalid_syntax(&str);
        }
        // trailing junk check
        if bytes[consumed..].iter().any(|b| !b.is_ascii_whitespace()) {
            invalid_syntax(&str);
        }
        apply_typmod_special(&res, typmod);
        return pg_return_numeric(res);
    }

    // Normal value: detect base prefix.
    let base = if cp + 1 < bytes.len() && bytes[cp] == b'0' {
        match bytes[cp + 1] {
            b'x' | b'X' => 16,
            b'o' | b'O' => 8,
            b'b' | b'B' => 2,
            _ => 10,
        }
    } else {
        10
    };

    let mut value;
    if base == 10 {
        let (v, endp) = set_var_from_str(&str, cp);
        value = v;
        value.sign = sign;
        // trailing junk
        if bytes[endp..].iter().any(|b| !b.is_ascii_whitespace()) {
            invalid_syntax(&str);
        }
    } else {
        let (v, endp) = set_var_from_non_decimal_integer_str(&str, cp + 2, sign, base);
        value = v;
        if bytes[endp..].iter().any(|b| !b.is_ascii_whitespace()) {
            invalid_syntax(&str);
        }
    }
    apply_typmod(&mut value, typmod);
    pg_return_numeric(make_result(&value))
}

/// PG `numeric_out`: output function for numeric.
pub fn numeric_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        let s = if num.is_pinf() {
            "Infinity"
        } else if num.is_ninf() {
            "-Infinity"
        } else {
            "NaN"
        };
        return pg_return_cstring(s);
    }
    let x = var_from_num(&num);
    pg_return_cstring(&get_str_from_var(&x))
}

/// PG `numeric_recv`: binary input. Stages on the wire StringInfo/MsgReader
/// (same as int4recv); not reachable until binary protocol input lands.
pub fn numeric_recv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numeric_recv needs the binary wire StringInfo (pq_getmsgint) path")
}

/// PG `numeric_send`: binary output. Stages on pq_begintypsend/pq_endtypsend.
pub fn numeric_send(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numeric_send needs pq_begintypsend/pq_endtypsend bytea boxing")
}

// ---------------------------------------------------------------------------
// numeric.h utility entry points (callable from Rust; not fmgr-bound).
// ---------------------------------------------------------------------------

/// PG `numeric_is_nan`.
#[must_use]
pub fn numeric_is_nan_bytes(bytes: &[u8]) -> bool {
    PackedNumeric { bytes: bytes.to_vec() }.is_nan()
}

/// PG `int64_to_numeric`: pack an int64 (returns the on-disk bytes).
#[must_use]
pub fn int64_to_numeric_bytes(val: i64) -> Vec<u8> {
    let mut v = NumericVar::new();
    int64_to_numericvar(val, &mut v);
    make_result(&v).bytes
}

// ===========================================================================
//   set_var_from_str / get_str_from_var
// ===========================================================================

/// PG `set_var_from_str`: parse a base-10 decimal/scientific number. Returns the
/// parsed var and the end+1 byte offset. Raises on bad syntax / overflow.
#[allow(
    clippy::too_many_lines,
    reason = "faithful port of numeric.c's set_var_from_str: digit scan, exponent \
              handling, and the decimal->base-NBASE conversion are one function in C"
)]
fn set_var_from_str(str: &str, start: usize) -> (NumericVar, usize) {
    let bytes = str.as_bytes();
    let mut cp = start;
    let mut have_dp = false;
    let mut sign = NUMERIC_POS;
    let mut dweight: i32 = -1;
    let mut dscale: i32 = 0;

    match bytes.get(cp) {
        Some(b'+') => cp += 1,
        Some(b'-') => {
            sign = NUMERIC_NEG;
            cp += 1;
        }
        _ => {}
    }
    if bytes.get(cp) == Some(&b'.') {
        have_dp = true;
        cp += 1;
    }
    if !bytes.get(cp).is_some_and(u8::is_ascii_digit) {
        invalid_syntax(str);
    }

    // decdigits: leading DEC_DIGITS zero pad, then the parsed decimal digits.
    let mut decdigits: Vec<u8> = vec![0; DEC_DIGITS];

    while cp < bytes.len() {
        let c = bytes[cp];
        if c.is_ascii_digit() {
            decdigits.push(c - b'0');
            cp += 1;
            if have_dp {
                dscale += 1;
            } else {
                dweight += 1;
            }
        } else if c == b'.' {
            if have_dp {
                invalid_syntax(str);
            }
            have_dp = true;
            cp += 1;
            if bytes.get(cp) == Some(&b'_') {
                invalid_syntax(str);
            }
        } else if c == b'_' {
            cp += 1;
            if !bytes.get(cp).is_some_and(u8::is_ascii_digit) {
                invalid_syntax(str);
            }
        } else {
            break;
        }
    }
    let ddigits = decdigits.len() - DEC_DIGITS;
    // trailing pad
    decdigits.extend(std::iter::repeat_n(0u8, DEC_DIGITS - 1));

    // Exponent.
    if matches!(bytes.get(cp), Some(b'e' | b'E')) {
        let mut exponent: i64 = 0;
        let mut neg = false;
        cp += 1;
        match bytes.get(cp) {
            Some(b'+') => cp += 1,
            Some(b'-') => {
                neg = true;
                cp += 1;
            }
            _ => {}
        }
        if !bytes.get(cp).is_some_and(u8::is_ascii_digit) {
            invalid_syntax(str);
        }
        while cp < bytes.len() {
            let c = bytes[cp];
            if c.is_ascii_digit() {
                exponent = exponent * 10 + i64::from(c - b'0');
                if exponent > i64::from(PG_INT32_MAX) / 2 {
                    overflow_error();
                }
                cp += 1;
            } else if c == b'_' {
                cp += 1;
                if !bytes.get(cp).is_some_and(u8::is_ascii_digit) {
                    invalid_syntax(str);
                }
            } else {
                break;
            }
        }
        if neg {
            exponent = -exponent;
        }
        dweight += exponent as i32;
        dscale -= exponent as i32;
        if dscale < 0 {
            dscale = 0;
        }
    }

    // Convert decimal to base NBASE.
    let weight: i32 = if dweight >= 0 {
        (dweight + 1 + DEC_DIGITS as i32 - 1) / DEC_DIGITS as i32 - 1
    } else {
        -((-dweight - 1) / DEC_DIGITS as i32 + 1)
    };
    let offset = (weight + 1) * DEC_DIGITS as i32 - (dweight + 1);
    let ndigits = (ddigits as i32 + offset + DEC_DIGITS as i32 - 1) / DEC_DIGITS as i32;

    let mut digits = vec![0i16; ndigits.max(0) as usize];
    let mut i = (DEC_DIGITS as i32 - offset) as usize;
    for slot in &mut digits {
        let d0 = i32::from(decdigits[i]);
        let d1 = i32::from(decdigits[i + 1]);
        let d2 = i32::from(decdigits[i + 2]);
        let d3 = i32::from(decdigits[i + 3]);
        *slot = (((d0 * 10 + d1) * 10 + d2) * 10 + d3) as i16;
        i += DEC_DIGITS;
    }

    let mut var = NumericVar { weight, sign, dscale, digits };
    strip_var(&mut var);
    (var, cp)
}

/// PG `set_var_from_non_decimal_integer_str`: parse 0x/0o/0b integers.
fn set_var_from_non_decimal_integer_str(
    str: &str,
    start: usize,
    sign: i32,
    base: i64,
) -> (NumericVar, usize) {
    let bytes = str.as_bytes();
    let mut cp = start;
    let firstdigit = cp;
    let mut dest = NumericVar::new();
    let mut tmp: i64 = 0;
    let mut mul: i64 = 1;

    let in_base = |c: u8| -> Option<i64> {
        match base {
            16 => (c as char).to_digit(16).map(i64::from),
            8 => {
                if (b'0'..=b'7').contains(&c) {
                    Some(i64::from(c - b'0'))
                } else {
                    None
                }
            }
            2 => {
                if matches!(c, b'0' | b'1') {
                    Some(i64::from(c - b'0'))
                } else {
                    None
                }
            }
            _ => None,
        }
    };

    while cp < bytes.len() {
        let c = bytes[cp];
        if let Some(d) = in_base(c) {
            if mul > PG_INT64_MAX / base {
                flush_nondecimal_group(&mut dest, mul, tmp);
                if dest.weight > NUMERIC_WEIGHT_MAX {
                    overflow_error();
                }
                tmp = 0;
                mul = 1;
            }
            tmp = tmp * base + d;
            mul *= base;
            cp += 1;
        } else if c == b'_' {
            cp += 1;
            if bytes.get(cp).copied().and_then(in_base).is_none() {
                invalid_syntax(str);
            }
        } else {
            break;
        }
    }
    if cp == firstdigit {
        invalid_syntax(str);
    }
    flush_nondecimal_group(&mut dest, mul, tmp);
    if dest.weight > NUMERIC_WEIGHT_MAX {
        overflow_error();
    }
    dest.sign = sign;
    (dest, cp)
}

fn flush_nondecimal_group(dest: &mut NumericVar, mul: i64, tmp: i64) {
    let mut tv = NumericVar::new();
    int64_to_numericvar(mul, &mut tv);
    let mut prod = NumericVar::new();
    mul_var(dest, &tv, &mut prod, 0);
    *dest = prod;
    int64_to_numericvar(tmp, &mut tv);
    let mut sum = NumericVar::new();
    add_var(dest, &tv, &mut sum);
    *dest = sum;
}

/// PG `get_str_from_var`: render a var to its decimal text (numeric_out guts).
fn get_str_from_var(var: &NumericVar) -> String {
    let dscale = var.dscale;
    let mut out: Vec<u8> = Vec::new();
    if var.sign == NUMERIC_NEG {
        out.push(b'-');
    }
    let ndigits = var.ndigits();

    // Digits before the decimal point.
    let mut d: i32;
    if var.weight < 0 {
        d = var.weight + 1;
        out.push(b'0');
    } else {
        d = 0;
        while d <= var.weight {
            let mut dig = i32::from(if d < ndigits { var.digits[d as usize] } else { 0 });
            let mut putit = d > 0;
            let mut d1 = dig / 1000;
            dig -= d1 * 1000;
            putit |= d1 > 0;
            if putit {
                out.push(d1 as u8 + b'0');
            }
            d1 = dig / 100;
            dig -= d1 * 100;
            putit |= d1 > 0;
            if putit {
                out.push(d1 as u8 + b'0');
            }
            d1 = dig / 10;
            dig -= d1 * 10;
            putit |= d1 > 0;
            if putit {
                out.push(d1 as u8 + b'0');
            }
            out.push(dig as u8 + b'0');
            d += 1;
        }
    }

    // Digits after the decimal point.
    if dscale > 0 {
        out.push(b'.');
        let start_len = out.len();
        let mut i = 0;
        while i < dscale {
            let mut dig = i32::from(if d >= 0 && d < ndigits { var.digits[d as usize] } else { 0 });
            let d1 = dig / 1000;
            dig -= d1 * 1000;
            out.push(d1 as u8 + b'0');
            let d1 = dig / 100;
            dig -= d1 * 100;
            out.push(d1 as u8 + b'0');
            let d1 = dig / 10;
            dig -= d1 * 10;
            out.push(d1 as u8 + b'0');
            out.push(dig as u8 + b'0');
            d += 1;
            i += DEC_DIGITS as i32;
        }
        out.truncate(start_len + dscale as usize);
    }

    // out is pure ASCII.
    String::from_utf8(out).unwrap_or_default()
}

// ===========================================================================
//   typmod application
// ===========================================================================

fn is_valid_numeric_typmod(typmod: i32) -> bool {
    typmod >= VARHDRSZ
}
fn numeric_typmod_precision(typmod: i32) -> i32 {
    ((typmod - VARHDRSZ) >> 16) & 0xffff
}
fn numeric_typmod_scale(typmod: i32) -> i32 {
    (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024
}

/// PG `apply_typmod`: round to scale, range-check against precision.
fn apply_typmod(var: &mut NumericVar, typmod: i32) {
    if !is_valid_numeric_typmod(typmod) {
        return;
    }
    let precision = numeric_typmod_precision(typmod);
    let scale = numeric_typmod_scale(typmod);
    let maxdigits = precision - scale;

    round_var(var, scale);
    if var.dscale < 0 {
        var.dscale = 0;
    }

    let mut ddigits = (var.weight + 1) * DEC_DIGITS as i32;
    if ddigits > maxdigits {
        for i in 0..var.ndigits() as usize {
            let dig = var.digits[i];
            if dig != 0 {
                if dig < 10 {
                    ddigits -= 3;
                } else if dig < 100 {
                    ddigits -= 2;
                } else if dig < 1000 {
                    ddigits -= 1;
                }
                if ddigits > maxdigits {
                    field_overflow(precision, scale, maxdigits);
                }
                break;
            }
            ddigits -= DEC_DIGITS as i32;
        }
    }
}

fn field_overflow(precision: i32, scale: i32, maxdigits: i32) -> ! {
    let (p, s, m) = (precision, scale, maxdigits);
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        let detail = if m != 0 {
            format!("A field with precision {p}, scale {s} must round to an absolute value less than 10^{m}.")
        } else {
            format!("A field with precision {p}, scale {s} must round to an absolute value less than 1.")
        };
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg("numeric field overflow")
            .errdetail(detail);
    });
    unreachable!()
}

/// PG `apply_typmod_special`: bounds-check an Inf/NaN. NaN always allowed; Inf
/// rejected under any typmod restriction.
fn apply_typmod_special(num: &PackedNumeric, typmod: i32) {
    if num.is_nan() {
        return;
    }
    if !is_valid_numeric_typmod(typmod) {
        return;
    }
    let precision = numeric_typmod_precision(typmod);
    let scale = numeric_typmod_scale(typmod);
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        let detail = format!(
            "A field with precision {precision}, scale {scale} cannot hold an infinite value."
        );
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg("numeric field overflow")
            .errdetail(detail);
    });
}

/// PG `numeric`: apply precision/scale typmod before storage.
pub fn numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    let typmod = DatumGetInt32(fcinfo.args[1].value);

    if num.is_special() {
        apply_typmod_special(&num, typmod);
        return pg_return_numeric(duplicate_numeric(&num));
    }
    if !is_valid_numeric_typmod(typmod) {
        return pg_return_numeric(duplicate_numeric(&num));
    }
    let mut var = var_from_num(&num);
    apply_typmod(&mut var, typmod);
    pg_return_numeric(make_result(&var))
}

// ===========================================================================
//   Sign manipulation, rounding
// ===========================================================================

/// PG `numeric_abs`.
pub fn numeric_abs(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        // -Inf -> Inf, NaN unaffected
        let v = if num.is_inf() { const_pinf() } else { const_nan() };
        return pg_return_numeric(make_result(&v));
    }
    let mut var = var_from_num(&num);
    var.sign = NUMERIC_POS;
    pg_return_numeric(make_result(&var))
}

/// PG `numeric_uminus`: unary minus.
pub fn numeric_uminus(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        let v = if num.is_nan() {
            const_nan()
        } else if num.is_pinf() {
            const_ninf()
        } else {
            const_pinf()
        };
        return pg_return_numeric(make_result(&v));
    }
    let mut var = var_from_num(&num);
    if var.ndigits() != 0 {
        var.sign = if var.sign == NUMERIC_POS { NUMERIC_NEG } else { NUMERIC_POS };
    }
    pg_return_numeric(make_result(&var))
}

/// PG `numeric_uplus`: identity.
pub fn numeric_uplus(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    pg_return_numeric(duplicate_numeric(&num))
}

/// PG `numeric_sign_internal`.
fn numeric_sign_internal(num: &PackedNumeric) -> i32 {
    if num.is_special() {
        if num.is_pinf() {
            1
        } else {
            -1
        }
    } else if num.ndigits() == 0 {
        0
    } else if num.sign() == NUMERIC_NEG {
        -1
    } else {
        1
    }
}

/// PG `numeric_sign`.
pub fn numeric_sign(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_nan() {
        return pg_return_numeric(make_result(&const_nan()));
    }
    let v = match numeric_sign_internal(&num) {
        0 => const_zero(),
        1 => const_one(),
        _ => const_minus_one(),
    };
    pg_return_numeric(make_result(&v))
}

/// PG `numeric_round`.
pub fn numeric_round(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    let mut scale = DatumGetInt32(fcinfo.args[1].value);
    if num.is_special() {
        return pg_return_numeric(duplicate_numeric(&num));
    }
    scale = scale.max(-(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS as i32 - 1);
    scale = scale.min(NUMERIC_DSCALE_MAX);
    let mut arg = var_from_num(&num);
    round_var(&mut arg, scale);
    if scale < 0 {
        arg.dscale = 0;
    }
    pg_return_numeric(make_result(&arg))
}

/// PG `numeric_trunc`.
pub fn numeric_trunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    let mut scale = DatumGetInt32(fcinfo.args[1].value);
    if num.is_special() {
        return pg_return_numeric(duplicate_numeric(&num));
    }
    scale = scale.max(-(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS as i32);
    scale = scale.min(NUMERIC_DSCALE_MAX);
    let mut arg = var_from_num(&num);
    trunc_var(&mut arg, scale);
    if scale < 0 {
        arg.dscale = 0;
    }
    pg_return_numeric(make_result(&arg))
}

/// PG `numeric_ceil`.
pub fn numeric_ceil(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        return pg_return_numeric(duplicate_numeric(&num));
    }
    let arg = var_from_num(&num);
    let mut result = NumericVar::new();
    ceil_var(&arg, &mut result);
    pg_return_numeric(make_result(&result))
}

/// PG `numeric_floor`.
pub fn numeric_floor(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        return pg_return_numeric(duplicate_numeric(&num));
    }
    let arg = var_from_num(&num);
    let mut result = NumericVar::new();
    floor_var(&arg, &mut result);
    pg_return_numeric(make_result(&result))
}

// ===========================================================================
//   Comparison
// ===========================================================================

fn cmp_numerics(num1: &PackedNumeric, num2: &PackedNumeric) -> i32 {
    if num1.is_special() {
        if num1.is_nan() {
            return i32::from(!num2.is_nan());
        } else if num1.is_pinf() {
            return if num2.is_nan() {
                -1
            } else { i32::from(!num2.is_pinf()) };
        }
        // num1 is NINF
        return if num2.is_ninf() { 0 } else { -1 };
    }
    if num2.is_special() {
        return if num2.is_ninf() { 1 } else { -1 };
    }
    cmp_var_common(
        &num1.digits(),
        num1.ndigits(),
        num1.weight(),
        num1.sign(),
        &num2.digits(),
        num2.ndigits(),
        num2.weight(),
        num2.sign(),
    )
}

macro_rules! cmp_op {
    ($name:ident, $op:tt) => {
        #[doc = concat!("PG `", stringify!($name), "`.")]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let num1 = pg_getarg_numeric(fcinfo, 0);
            let num2 = pg_getarg_numeric(fcinfo, 1);
            BoolGetDatum(cmp_numerics(&num1, &num2) $op 0)
        }
    };
}
cmp_op!(numeric_eq, ==);
cmp_op!(numeric_ne, !=);
cmp_op!(numeric_gt, >);
cmp_op!(numeric_ge, >=);
cmp_op!(numeric_lt, <);
cmp_op!(numeric_le, <=);

/// PG `numeric_cmp`: the btree 3-way comparison (`btnumericcmp` shares this).
pub fn numeric_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num1 = pg_getarg_numeric(fcinfo, 0);
    let num2 = pg_getarg_numeric(fcinfo, 1);
    Int32GetDatum(cmp_numerics(&num1, &num2))
}

/// PG `numeric_smaller`.
pub fn numeric_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num1 = pg_getarg_numeric(fcinfo, 0);
    let num2 = pg_getarg_numeric(fcinfo, 1);
    if cmp_numerics(&num1, &num2) < 0 {
        pg_return_numeric(num1)
    } else {
        pg_return_numeric(num2)
    }
}

/// PG `numeric_larger`.
pub fn numeric_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num1 = pg_getarg_numeric(fcinfo, 0);
    let num2 = pg_getarg_numeric(fcinfo, 1);
    if cmp_numerics(&num1, &num2) > 0 {
        pg_return_numeric(num1)
    } else {
        pg_return_numeric(num2)
    }
}

// ===========================================================================
//   Arithmetic operators
// ===========================================================================

/// Common special-value handling for binary ops. Returns Some(result var) if
/// at least one operand is special.
fn special_binop(
    num1: &PackedNumeric,
    num2: &PackedNumeric,
    op: BinOp,
) -> Option<NumericVar> {
    if !(num1.is_special() || num2.is_special()) {
        return None;
    }
    if num1.is_nan() || num2.is_nan() {
        return Some(const_nan());
    }
    Some(match op {
        BinOp::Add => {
            if num1.is_pinf() {
                if num2.is_ninf() {
                    const_nan()
                } else {
                    const_pinf()
                }
            } else if num1.is_ninf() {
                if num2.is_pinf() {
                    const_nan()
                } else {
                    const_ninf()
                }
            } else if num2.is_pinf() {
                const_pinf()
            } else {
                const_ninf()
            }
        }
        BinOp::Sub => {
            if num1.is_pinf() {
                if num2.is_pinf() {
                    const_nan()
                } else {
                    const_pinf()
                }
            } else if num1.is_ninf() {
                if num2.is_ninf() {
                    const_nan()
                } else {
                    const_ninf()
                }
            } else if num2.is_pinf() {
                const_ninf()
            } else {
                const_pinf()
            }
        }
        BinOp::Mul => {
            let inf_times = |other: &PackedNumeric, pinf: bool| match numeric_sign_internal(other) {
                0 => const_nan(),
                1 => if pinf { const_pinf() } else { const_ninf() },
                _ => if pinf { const_ninf() } else { const_pinf() },
            };
            if num1.is_pinf() {
                inf_times(num2, true)
            } else if num1.is_ninf() {
                inf_times(num2, false)
            } else if num2.is_pinf() {
                inf_times(num1, true)
            } else {
                inf_times(num1, false)
            }
        }
        BinOp::Div => special_div(num1, num2),
    })
}

#[derive(Clone, Copy)]
enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
}

fn special_div(num1: &PackedNumeric, num2: &PackedNumeric) -> NumericVar {
    if num1.is_pinf() {
        if num2.is_special() {
            return const_nan();
        }
        return match numeric_sign_internal(num2) {
            0 => division_by_zero(),
            1 => const_pinf(),
            _ => const_ninf(),
        };
    }
    if num1.is_ninf() {
        if num2.is_special() {
            return const_nan();
        }
        return match numeric_sign_internal(num2) {
            0 => division_by_zero(),
            1 => const_ninf(),
            _ => const_pinf(),
        };
    }
    // num1 finite, num2 special (Inf): result 0
    const_zero()
}

/// PG `numeric_add`.
pub fn numeric_add(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num1 = pg_getarg_numeric(fcinfo, 0);
    let num2 = pg_getarg_numeric(fcinfo, 1);
    if let Some(v) = special_binop(&num1, &num2, BinOp::Add) {
        return pg_return_numeric(make_result(&v));
    }
    let arg1 = var_from_num(&num1);
    let arg2 = var_from_num(&num2);
    let mut result = NumericVar::new();
    add_var(&arg1, &arg2, &mut result);
    pg_return_numeric(make_result(&result))
}

/// PG `numeric_sub`.
pub fn numeric_sub(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num1 = pg_getarg_numeric(fcinfo, 0);
    let num2 = pg_getarg_numeric(fcinfo, 1);
    if let Some(v) = special_binop(&num1, &num2, BinOp::Sub) {
        return pg_return_numeric(make_result(&v));
    }
    let arg1 = var_from_num(&num1);
    let arg2 = var_from_num(&num2);
    let mut result = NumericVar::new();
    sub_var(&arg1, &arg2, &mut result);
    pg_return_numeric(make_result(&result))
}

/// PG `numeric_mul`.
pub fn numeric_mul(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num1 = pg_getarg_numeric(fcinfo, 0);
    let num2 = pg_getarg_numeric(fcinfo, 1);
    if let Some(v) = special_binop(&num1, &num2, BinOp::Mul) {
        return pg_return_numeric(make_result(&v));
    }
    let arg1 = var_from_num(&num1);
    let arg2 = var_from_num(&num2);
    let mut result = NumericVar::new();
    mul_var(&arg1, &arg2, &mut result, arg1.dscale + arg2.dscale);
    if result.dscale > NUMERIC_DSCALE_MAX {
        round_var(&mut result, NUMERIC_DSCALE_MAX);
    }
    pg_return_numeric(make_result(&result))
}

/// PG `numeric_div`.
pub fn numeric_div(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num1 = pg_getarg_numeric(fcinfo, 0);
    let num2 = pg_getarg_numeric(fcinfo, 1);
    if let Some(v) = special_binop(&num1, &num2, BinOp::Div) {
        return pg_return_numeric(make_result(&v));
    }
    let arg1 = var_from_num(&num1);
    let arg2 = var_from_num(&num2);
    let rscale = select_div_scale(&arg1, &arg2);
    let mut result = NumericVar::new();
    div_var(&arg1, &arg2, &mut result, rscale, true);
    pg_return_numeric(make_result(&result))
}

/// PG `numeric_div_trunc`.
pub fn numeric_div_trunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num1 = pg_getarg_numeric(fcinfo, 0);
    let num2 = pg_getarg_numeric(fcinfo, 1);
    if let Some(v) = special_binop(&num1, &num2, BinOp::Div) {
        return pg_return_numeric(make_result(&v));
    }
    let arg1 = var_from_num(&num1);
    let arg2 = var_from_num(&num2);
    let mut result = NumericVar::new();
    div_var(&arg1, &arg2, &mut result, 0, false);
    pg_return_numeric(make_result(&result))
}

/// PG `numeric_mod`.
pub fn numeric_mod(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num1 = pg_getarg_numeric(fcinfo, 0);
    let num2 = pg_getarg_numeric(fcinfo, 1);
    if num1.is_special() || num2.is_special() {
        if num1.is_nan() || num2.is_nan() {
            return pg_return_numeric(make_result(&const_nan()));
        }
        if num1.is_inf() {
            if numeric_sign_internal(&num2) == 0 {
                division_by_zero();
            }
            return pg_return_numeric(make_result(&const_nan()));
        }
        return pg_return_numeric(duplicate_numeric(&num1));
    }
    let arg1 = var_from_num(&num1);
    let arg2 = var_from_num(&num2);
    let mut result = NumericVar::new();
    mod_var(&arg1, &arg2, &mut result);
    pg_return_numeric(make_result(&result))
}

/// PG `numeric_inc`.
pub fn numeric_inc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        return pg_return_numeric(duplicate_numeric(&num));
    }
    let arg = var_from_num(&num);
    let mut result = NumericVar::new();
    add_var(&arg, &const_one(), &mut result);
    pg_return_numeric(make_result(&result))
}

/// PG `numeric_gcd`.
pub fn numeric_gcd(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num1 = pg_getarg_numeric(fcinfo, 0);
    let num2 = pg_getarg_numeric(fcinfo, 1);
    if num1.is_special() || num2.is_special() {
        if num1.is_nan() || num2.is_nan() {
            return pg_return_numeric(make_result(&const_nan()));
        }
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                .errmsg("cannot take gcd of infinity");
        });
        unreachable!()
    }
    let arg1 = var_from_num(&num1);
    let arg2 = var_from_num(&num2);
    let mut result = NumericVar::new();
    gcd_var(&arg1, &arg2, &mut result);
    pg_return_numeric(make_result(&result))
}

/// PG `numeric_lcm`.
pub fn numeric_lcm(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num1 = pg_getarg_numeric(fcinfo, 0);
    let num2 = pg_getarg_numeric(fcinfo, 1);
    if num1.is_special() || num2.is_special() {
        if num1.is_nan() || num2.is_nan() {
            return pg_return_numeric(make_result(&const_nan()));
        }
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                .errmsg("cannot take lcm of infinity");
        });
        unreachable!()
    }
    let arg1 = var_from_num(&num1);
    let arg2 = var_from_num(&num2);
    // lcm(x,y) = abs(x / gcd(x,y) * y); gcd==0 -> 0
    let mut g = NumericVar::new();
    gcd_var(&arg1, &arg2, &mut g);
    let result = if g.ndigits() == 0 {
        const_zero()
    } else {
        let mut q = NumericVar::new();
        div_var(&arg1, &g, &mut q, select_div_scale(&arg1, &g), true);
        let mut prod = NumericVar::new();
        mul_var(&q, &arg2, &mut prod, q.dscale + arg2.dscale);
        prod.sign = NUMERIC_POS;
        let res_dscale = arg1.dscale.max(arg2.dscale);
        prod.dscale = prod.dscale.max(res_dscale);
        prod
    };
    pg_return_numeric(make_result(&result))
}

/// PG `numeric_scale`: the dscale of the value, as numeric.
pub fn numeric_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        return pg_return_numeric(make_result(&const_nan()));
    }
    pg_return_numeric(PackedNumeric { bytes: int64_to_numeric_bytes(i64::from(num.dscale())) })
}

/// PG `numeric_min_scale`: minimum scale needed to represent the value exactly.
pub fn numeric_min_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        return pg_return_numeric(make_result(&const_nan()));
    }
    let arg = var_from_num(&num);
    let min = get_min_scale(&arg);
    pg_return_numeric(PackedNumeric { bytes: int64_to_numeric_bytes(i64::from(min)) })
}

/// PG `numeric_trim_scale`: drop trailing zero fractional digits.
pub fn numeric_trim_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        return pg_return_numeric(duplicate_numeric(&num));
    }
    let mut arg = var_from_num(&num);
    arg.dscale = get_min_scale(&arg);
    pg_return_numeric(make_result(&arg))
}

/// PG `get_min_scale`: number of fractional decimal digits actually present.
fn get_min_scale(var: &NumericVar) -> i32 {
    let ndigits = var.ndigits();
    let min_scale = (ndigits - (var.weight + 1)) * DEC_DIGITS as i32;
    if min_scale <= 0 {
        return 0;
    }
    // Reduce by trailing zeroes in the last NBASE digit.
    let mut last = i32::from(var.digits[(ndigits - 1) as usize]);
    let mut zeroes = 0;
    while zeroes < DEC_DIGITS as i32 && last % 10 == 0 && last != 0 {
        last /= 10;
        zeroes += 1;
    }
    if last == 0 {
        zeroes = DEC_DIGITS as i32;
    }
    (min_scale - zeroes).max(0)
}

// ===========================================================================
//   Casts: int / float
// ===========================================================================

/// PG `int4_numeric`.
pub fn int4_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = DatumGetInt32(fcinfo.args[0].value);
    pg_return_numeric(PackedNumeric { bytes: int64_to_numeric_bytes(i64::from(val)) })
}

/// PG `int8_numeric`.
pub fn int8_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = DatumGetInt64(fcinfo.args[0].value);
    pg_return_numeric(PackedNumeric { bytes: int64_to_numeric_bytes(val) })
}

/// PG `int2_numeric`.
pub fn int2_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = DatumGetInt16(fcinfo.args[0].value);
    pg_return_numeric(PackedNumeric { bytes: int64_to_numeric_bytes(i64::from(val)) })
}

/// PG `numeric_int4_opt_error`: numeric -> int32, raising on out-of-range/special.
fn numeric_to_int32(num: &PackedNumeric) -> i32 {
    if num.is_special() {
        cannot_convert_special(if num.is_nan() { "NaN" } else { "infinity" }, "integer");
    }
    let x = var_from_num(num);
    match numericvar_to_int64(&x) {
        Some(v) if v >= i64::from(PG_INT32_MIN) && v <= i64::from(PG_INT32_MAX) => v as i32,
        _ => int_out_of_range("integer"),
    }
}

/// PG `numeric_int4`.
pub fn numeric_int4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    Int32GetDatum(numeric_to_int32(&num))
}

/// PG `numeric_int8`.
pub fn numeric_int8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        cannot_convert_special(if num.is_nan() { "NaN" } else { "infinity" }, "bigint");
    }
    let x = var_from_num(&num);
    numericvar_to_int64(&x).map_or_else(|| int_out_of_range("bigint"), Int64GetDatum)
}

/// PG `numeric_int2`.
pub fn numeric_int2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        cannot_convert_special(if num.is_nan() { "NaN" } else { "infinity" }, "smallint");
    }
    let x = var_from_num(&num);
    match numericvar_to_int64(&x) {
        Some(v) if v >= i64::from(PG_INT16_MIN) && v <= i64::from(PG_INT16_MAX) => {
            Int16GetDatum(v as i16)
        }
        _ => int_out_of_range("smallint"),
    }
}

/// PG `float8_numeric`.
pub fn float8_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = DatumGetFloat8(fcinfo.args[0].value);
    if val.is_nan() {
        return pg_return_numeric(make_result(&const_nan()));
    }
    if val.is_infinite() {
        let v = if val < 0.0 { const_ninf() } else { const_pinf() };
        return pg_return_numeric(make_result(&v));
    }
    // PG: snprintf "%.*g" with DBL_DIG=15, then set_var_from_str.
    let buf = format_g(val, 15);
    let (result, _) = set_var_from_str(&buf, 0);
    pg_return_numeric(make_result(&result))
}

/// PG `float4_numeric`.
pub fn float4_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = DatumGetFloat4(fcinfo.args[0].value);
    if val.is_nan() {
        return pg_return_numeric(make_result(&const_nan()));
    }
    if val.is_infinite() {
        let v = if val < 0.0 { const_ninf() } else { const_pinf() };
        return pg_return_numeric(make_result(&v));
    }
    // PG: snprintf "%.*g" with FLT_DIG=6.
    let buf = format_g(f64::from(val), 6);
    let (result, _) = set_var_from_str(&buf, 0);
    pg_return_numeric(make_result(&result))
}

/// PG `numeric_float8`.
pub fn numeric_float8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        let v = if num.is_pinf() {
            f64::INFINITY
        } else if num.is_ninf() {
            f64::NEG_INFINITY
        } else {
            f64::NAN
        };
        return Float8GetDatum(v);
    }
    let x = var_from_num(&num);
    Float8GetDatum(numericvar_to_double(&x))
}

/// PG `numeric_float4`.
pub fn numeric_float4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        let v = if num.is_pinf() {
            f32::INFINITY
        } else if num.is_ninf() {
            f32::NEG_INFINITY
        } else {
            f32::NAN
        };
        return Float4GetDatum(v);
    }
    let x = var_from_num(&num);
    Float4GetDatum(numericvar_to_double(&x) as f32)
}

/// PG `numeric_float8_no_overflow` (internal): saturating numeric -> float8.
pub fn numeric_float8_no_overflow(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let num = pg_getarg_numeric(fcinfo, 0);
    if num.is_special() {
        let v = if num.is_pinf() {
            f64::INFINITY
        } else if num.is_ninf() {
            f64::NEG_INFINITY
        } else {
            f64::NAN
        };
        return Float8GetDatum(v);
    }
    let x = var_from_num(&num);
    Float8GetDatum(numericvar_to_double(&x))
}

/// Format a float like C `snprintf("%.*g", prec, val)` (shortest of e/f form).
fn format_g(val: f64, prec: usize) -> String {
    if val == 0.0 {
        return "0".to_owned();
    }
    let prec = prec.max(1);
    let exp = val.abs().log10().floor() as i32;
    // %g uses scientific if exp < -4 or exp >= prec.
    if exp < -4 || exp >= prec as i32 {
        let s = format!("{:.*e}", prec - 1, val);
        trim_g_exp(&s)
    } else {
        let decimals = (prec as i32 - 1 - exp).max(0) as usize;
        let s = format!("{val:.decimals$}");
        trim_g_frac(&s)
    }
}

fn trim_g_frac(s: &str) -> String {
    if s.contains('.') {
        let t = s.trim_end_matches('0');
        t.trim_end_matches('.').to_owned()
    } else {
        s.to_owned()
    }
}

fn trim_g_exp(s: &str) -> String {
    // Rust emits e.g. "1.5e2"; C %g emits "1.5e+02". Normalize mantissa zeroes
    // and exponent sign/padding to feed set_var_from_str (which accepts e+NN).
    let (mantissa, exp) = s.split_once('e').unwrap_or((s, "0"));
    let mantissa = trim_g_frac(mantissa);
    let exp_val: i32 = exp.parse().unwrap_or(0);
    format!("{mantissa}e{exp_val:+03}")
}

// ===========================================================================
//   Variable-level kernels
// ===========================================================================

/// PG `int64_to_numericvar`.
fn int64_to_numericvar(val: i64, var: &mut NumericVar) {
    var.dscale = 0;
    if val == 0 {
        var.digits.clear();
        var.weight = 0;
        var.sign = NUMERIC_POS;
        return;
    }
    let mut uval = if val < 0 {
        var.sign = NUMERIC_NEG;
        pg_abs_s64(val)
    } else {
        var.sign = NUMERIC_POS;
        val as u64
    };
    // Build least-significant first, then reverse.
    let mut rev: Vec<NumericDigit> = Vec::new();
    while uval != 0 {
        let newuval = uval / NBASE as u64;
        rev.push((uval - newuval * NBASE as u64) as i16);
        uval = newuval;
    }
    rev.reverse();
    var.weight = rev.len() as i32 - 1;
    var.digits = rev;
}

/// PG `numericvar_to_int64`: round to nearest integer; None on overflow.
fn numericvar_to_int64(var: &NumericVar) -> Option<i64> {
    let mut rounded = var.clone();
    round_var(&mut rounded, 0);
    strip_var(&mut rounded);
    let ndigits = rounded.ndigits();
    if ndigits == 0 {
        return Some(0);
    }
    let weight = rounded.weight;
    let neg = rounded.sign == NUMERIC_NEG;
    let mut val: i64 = -i64::from(rounded.digits[0]);
    for i in 1..=weight {
        val = pg_mul_s64_overflow(val, i64::from(NBASE))?;
        if i < ndigits {
            val = pg_sub_s64_overflow(val, i64::from(rounded.digits[i as usize]))?;
        }
    }
    if !neg {
        if val == PG_INT64_MIN {
            return None;
        }
        val = -val;
    }
    Some(val)
}

/// PG `numericvar_to_double_no_overflow`: go through the decimal string.
fn numericvar_to_double(var: &NumericVar) -> f64 {
    get_str_from_var(var).parse::<f64>().unwrap_or(0.0)
}

/// PG `cmp_var_common`.
#[allow(clippy::too_many_arguments, reason = "faithful port of cmp_var_common's signature")]
fn cmp_var_common(
    var1digits: &[NumericDigit],
    var1ndigits: i32,
    var1weight: i32,
    var1sign: i32,
    var2digits: &[NumericDigit],
    var2ndigits: i32,
    var2weight: i32,
    var2sign: i32,
) -> i32 {
    if var1ndigits == 0 {
        if var2ndigits == 0 {
            return 0;
        }
        return if var2sign == NUMERIC_NEG { 1 } else { -1 };
    }
    if var2ndigits == 0 {
        return if var1sign == NUMERIC_POS { 1 } else { -1 };
    }
    if var1sign == NUMERIC_POS {
        if var2sign == NUMERIC_NEG {
            return 1;
        }
        return cmp_abs_common(var1digits, var1ndigits, var1weight, var2digits, var2ndigits, var2weight);
    }
    if var2sign == NUMERIC_POS {
        return -1;
    }
    cmp_abs_common(var2digits, var2ndigits, var2weight, var1digits, var1ndigits, var1weight)
}

fn cmp_var(var1: &NumericVar, var2: &NumericVar) -> i32 {
    cmp_var_common(
        &var1.digits,
        var1.ndigits(),
        var1.weight,
        var1.sign,
        &var2.digits,
        var2.ndigits(),
        var2.weight,
        var2.sign,
    )
}

/// PG `cmp_abs_common`.
fn cmp_abs_common(
    var1digits: &[NumericDigit],
    var1ndigits: i32,
    mut var1weight: i32,
    var2digits: &[NumericDigit],
    var2ndigits: i32,
    mut var2weight: i32,
) -> i32 {
    let mut i1 = 0i32;
    let mut i2 = 0i32;
    while var1weight > var2weight && i1 < var1ndigits {
        if var1digits[i1 as usize] != 0 {
            return 1;
        }
        i1 += 1;
        var1weight -= 1;
    }
    while var2weight > var1weight && i2 < var2ndigits {
        if var2digits[i2 as usize] != 0 {
            return -1;
        }
        i2 += 1;
        var2weight -= 1;
    }
    if var1weight == var2weight {
        while i1 < var1ndigits && i2 < var2ndigits {
            let stat = var1digits[i1 as usize] - var2digits[i2 as usize];
            i1 += 1;
            i2 += 1;
            if stat != 0 {
                return if stat > 0 { 1 } else { -1 };
            }
        }
    }
    while i1 < var1ndigits {
        if var1digits[i1 as usize] != 0 {
            return 1;
        }
        i1 += 1;
    }
    while i2 < var2ndigits {
        if var2digits[i2 as usize] != 0 {
            return -1;
        }
        i2 += 1;
    }
    0
}

fn cmp_abs(var1: &NumericVar, var2: &NumericVar) -> i32 {
    cmp_abs_common(&var1.digits, var1.ndigits(), var1.weight, &var2.digits, var2.ndigits(), var2.weight)
}

/// PG `add_var`: signed addition.
fn add_var(var1: &NumericVar, var2: &NumericVar, result: &mut NumericVar) {
    if var1.sign == NUMERIC_POS {
        if var2.sign == NUMERIC_POS {
            add_abs(var1, var2, result);
            result.sign = NUMERIC_POS;
        } else {
            match cmp_abs(var1, var2) {
                0 => {
                    result.zero();
                    result.dscale = var1.dscale.max(var2.dscale);
                }
                1 => {
                    sub_abs(var1, var2, result);
                    result.sign = NUMERIC_POS;
                }
                _ => {
                    sub_abs(var2, var1, result);
                    result.sign = NUMERIC_NEG;
                }
            }
        }
    } else if var2.sign == NUMERIC_POS {
        match cmp_abs(var1, var2) {
            0 => {
                result.zero();
                result.dscale = var1.dscale.max(var2.dscale);
            }
            1 => {
                sub_abs(var1, var2, result);
                result.sign = NUMERIC_NEG;
            }
            _ => {
                sub_abs(var2, var1, result);
                result.sign = NUMERIC_POS;
            }
        }
    } else {
        add_abs(var1, var2, result);
        result.sign = NUMERIC_NEG;
    }
}

/// PG `sub_var`: signed subtraction.
fn sub_var(var1: &NumericVar, var2: &NumericVar, result: &mut NumericVar) {
    if var1.sign == NUMERIC_POS {
        if var2.sign == NUMERIC_NEG {
            add_abs(var1, var2, result);
            result.sign = NUMERIC_POS;
        } else {
            match cmp_abs(var1, var2) {
                0 => {
                    result.zero();
                    result.dscale = var1.dscale.max(var2.dscale);
                }
                1 => {
                    sub_abs(var1, var2, result);
                    result.sign = NUMERIC_POS;
                }
                _ => {
                    sub_abs(var2, var1, result);
                    result.sign = NUMERIC_NEG;
                }
            }
        }
    } else if var2.sign == NUMERIC_NEG {
        match cmp_abs(var1, var2) {
            0 => {
                result.zero();
                result.dscale = var1.dscale.max(var2.dscale);
            }
            1 => {
                sub_abs(var1, var2, result);
                result.sign = NUMERIC_NEG;
            }
            _ => {
                sub_abs(var2, var1, result);
                result.sign = NUMERIC_POS;
            }
        }
    } else {
        add_abs(var1, var2, result);
        result.sign = NUMERIC_NEG;
    }
}

/// PG `add_abs`: add absolute values.
fn add_abs(var1: &NumericVar, var2: &NumericVar, result: &mut NumericVar) {
    let var1ndigits = var1.ndigits();
    let var2ndigits = var2.ndigits();
    let res_weight = var1.weight.max(var2.weight) + 1;
    let res_dscale = var1.dscale.max(var2.dscale);
    let rscale1 = var1ndigits - var1.weight - 1;
    let rscale2 = var2ndigits - var2.weight - 1;
    let res_rscale = rscale1.max(rscale2);
    let mut res_ndigits = res_rscale + res_weight + 1;
    if res_ndigits <= 0 {
        res_ndigits = 1;
    }
    let mut res_digits = vec![0i16; res_ndigits as usize];
    let mut carry = 0i32;
    let mut i1 = res_rscale + var1.weight + 1;
    let mut i2 = res_rscale + var2.weight + 1;
    for i in (0..res_ndigits).rev() {
        i1 -= 1;
        i2 -= 1;
        if i1 >= 0 && i1 < var1ndigits {
            carry += i32::from(var1.digits[i1 as usize]);
        }
        if i2 >= 0 && i2 < var2ndigits {
            carry += i32::from(var2.digits[i2 as usize]);
        }
        if carry >= NBASE {
            res_digits[i as usize] = (carry - NBASE) as i16;
            carry = 1;
        } else {
            res_digits[i as usize] = carry as i16;
            carry = 0;
        }
    }
    debug_assert_eq!(carry, 0);
    result.digits = res_digits;
    result.weight = res_weight;
    result.dscale = res_dscale;
    strip_var(result);
}

/// PG `sub_abs`: ABS(var1) - ABS(var2), requires ABS(var1) >= ABS(var2).
fn sub_abs(var1: &NumericVar, var2: &NumericVar, result: &mut NumericVar) {
    let var1ndigits = var1.ndigits();
    let var2ndigits = var2.ndigits();
    let res_weight = var1.weight;
    let res_dscale = var1.dscale.max(var2.dscale);
    let rscale1 = var1ndigits - var1.weight - 1;
    let rscale2 = var2ndigits - var2.weight - 1;
    let res_rscale = rscale1.max(rscale2);
    let mut res_ndigits = res_rscale + res_weight + 1;
    if res_ndigits <= 0 {
        res_ndigits = 1;
    }
    let mut res_digits = vec![0i16; res_ndigits as usize];
    let mut borrow = 0i32;
    let mut i1 = res_rscale + var1.weight + 1;
    let mut i2 = res_rscale + var2.weight + 1;
    for i in (0..res_ndigits).rev() {
        i1 -= 1;
        i2 -= 1;
        if i1 >= 0 && i1 < var1ndigits {
            borrow += i32::from(var1.digits[i1 as usize]);
        }
        if i2 >= 0 && i2 < var2ndigits {
            borrow -= i32::from(var2.digits[i2 as usize]);
        }
        if borrow < 0 {
            res_digits[i as usize] = (borrow + NBASE) as i16;
            borrow = -1;
        } else {
            res_digits[i as usize] = borrow as i16;
            borrow = 0;
        }
    }
    debug_assert_eq!(borrow, 0);
    result.digits = res_digits;
    result.weight = res_weight;
    result.dscale = res_dscale;
    strip_var(result);
}

/// PG `mul_var`: product to rscale fractional digits.
///
/// This is the classic O(N^2) schoolbook base-NBASE long multiplication (the
/// algorithm PG used through v17). PG 18 added a base-NBASE^2 digit-pair variant
/// for speed, but it computes the identical rounded result; we keep the simpler,
/// obviously-correct form for the port (no unsafe pointer-packed accumulator).
fn mul_var(var1: &NumericVar, var2: &NumericVar, result: &mut NumericVar, rscale: i32) {
    let var1ndigits = var1.ndigits();
    let var2ndigits = var2.ndigits();
    if var1ndigits == 0 || var2ndigits == 0 {
        result.zero();
        result.dscale = rscale;
        return;
    }
    let res_sign = if var1.sign == var2.sign { NUMERIC_POS } else { NUMERIC_NEG };
    // dig[k] (k = i1 + i2) carries weight (var1.weight - i1) + (var2.weight - i2)
    // = var1.weight + var2.weight - k, so the most significant product digit
    // dig[0] has weight var1.weight + var2.weight. A carry out of dig[0] (handled
    // below) bumps the weight by one.
    let res_weight = var1.weight + var2.weight;

    // Truncate the computation to what rscale needs, plus guard digits. The
    // exact result has res_exact_ndigits digits; we never need more than
    // res_weight + 1 (integer part) + ceil(rscale/DEC_DIGITS) + guard.
    let res_exact = var1ndigits + var2ndigits;
    let maxdigits = res_weight + 1 + (rscale + DEC_DIGITS as i32 - 1) / DEC_DIGITS as i32
        + MUL_GUARD_DIGITS;
    let res_ndigits = res_exact.min(maxdigits.max(1));
    if res_ndigits <= 0 {
        result.zero();
        result.dscale = rscale;
        return;
    }

    // Schoolbook accumulation in an i64 array `dig[0..res_ndigits]`, where
    // dig[k] accumulates products contributing to result digit k. With NBASE
    // 10000, each partial product < 10^8 and there are at most ~min(ndigits)
    // terms per column, all far within i64.
    let mut dig = vec![0i64; res_ndigits as usize];
    for i1 in 0..var1ndigits {
        let v1 = i64::from(var1.digits[i1 as usize]);
        if v1 == 0 {
            continue;
        }
        for i2 in 0..var2ndigits {
            let k = i1 + i2; // result digit index for this partial product
            if k >= res_ndigits {
                continue; // truncated away (beyond what rscale needs)
            }
            dig[k as usize] += v1 * i64::from(var2.digits[i2 as usize]);
        }
    }
    // Single carry-normalization pass, low to high.
    let mut carry: i64 = 0;
    let mut res_digits = vec![0i16; res_ndigits as usize];
    for i in (0..res_ndigits as usize).rev() {
        let v = dig[i] + carry;
        carry = v / i64::from(NBASE);
        res_digits[i] = (v - carry * i64::from(NBASE)) as i16;
    }
    let mut weight = res_weight;
    if carry != 0 {
        // carry out the top digit: prepend it
        res_digits.insert(0, carry as i16);
        weight += 1;
    }

    result.digits = res_digits;
    result.weight = weight;
    result.sign = res_sign;
    round_var(result, rscale);
    strip_var(result);
}

/// PG `div_var`: var1 / var2 to rscale fractional digits.
///
/// Classic base-NBASE long division (the pre-v18 algorithm; PG 18's base-NBASE^2
/// variant computes the identical rounded result). `round` selects round vs
/// truncate at the rscale'th digit.
fn div_var(var1: &NumericVar, var2: &NumericVar, result: &mut NumericVar, rscale: i32, round: bool) {
    let var1ndigits = var1.ndigits();
    let var2ndigits = var2.ndigits();
    if var2ndigits == 0 || var2.digits[0] == 0 {
        division_by_zero();
    }
    if var1ndigits == 0 {
        result.zero();
        result.dscale = rscale;
        return;
    }
    let res_sign = if var1.sign == var2.sign { NUMERIC_POS } else { NUMERIC_NEG };
    // Quotient digit 0 has weight res_weight; emit res_ndigits of them.
    let res_weight = var1.weight - var2.weight;
    let mut res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS as i32 - 1) / DEC_DIGITS as i32;
    res_ndigits = res_ndigits.max(1);
    if round {
        res_ndigits += 1; // one extra to round correctly
    }
    res_ndigits += DIV_GUARD_DIGITS;

    // Working remainder, base NBASE, big-endian in `rem`. `rem` is the running
    // remainder; at step qi we look at the prefix of length var2ndigits+1.
    //
    // Build the dividend digit stream: var1's digits, then zeros for the
    // fractional/guard positions we still need to produce.
    let n2 = var2ndigits as usize;
    let divisor: Vec<i64> = var2.digits.iter().map(|&d| i64::from(d)).collect();

    // Stream of dividend digits long enough to emit res_ndigits quotient digits.
    let dividend_len = (res_ndigits + var2ndigits) as usize;
    let mut stream = vec![0i64; dividend_len];
    let copy = (var1ndigits as usize).min(dividend_len);
    for (slot, &d) in stream.iter_mut().zip(&var1.digits[..copy]) {
        *slot = i64::from(d);
    }

    // rem holds the current remainder window (length n2+1, big-endian, rem[0]
    // most significant), with the divisor's n2 digits aligned under rem[1..=n2].
    // Pre-load the top n2 dividend digits so quotient[0] has weight res_weight;
    // then each step emits a digit and shifts in stream[n2 + qi].
    let mut rem = vec![0i64; n2 + 1];
    rem[1..=n2].copy_from_slice(&stream[..n2]);
    let mut quotient = Vec::with_capacity(res_ndigits as usize);

    for qi in 0..res_ndigits as usize {
        // Binary search the quotient digit q in [0, NBASE).
        let mut lo = 0i64;
        let mut hi = i64::from(NBASE) - 1;
        let mut q = 0i64;
        while lo <= hi {
            let mid = i64::midpoint(lo, hi);
            if cmp_scaled_le(&divisor, mid, &rem) {
                q = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        // rem -= q * divisor (aligned so divisor[n2-1] is rem[n2]).
        if q != 0 {
            let mut borrow = 0i64;
            let mut carry = 0i64;
            for k in (0..n2).rev() {
                let prod = q * divisor[k] + carry;
                carry = prod / i64::from(NBASE);
                let sub = prod % i64::from(NBASE);
                let pos = k + 1; // rem index aligned under divisor[k]
                let mut v = rem[pos] - sub - borrow;
                if v < 0 {
                    v += i64::from(NBASE);
                    borrow = 1;
                } else {
                    borrow = 0;
                }
                rem[pos] = v;
            }
            rem[0] -= carry + borrow;
            debug_assert!(rem[0] >= 0);
        }
        quotient.push(q as i16);

        // Shift the remainder window left one digit and bring down the next
        // dividend digit (stream[n2 + qi]); zeros once var1 is exhausted.
        rem.remove(0);
        let next = stream.get(n2 + qi).copied().unwrap_or(0);
        rem.push(next);
    }

    result.digits = quotient;
    result.weight = res_weight;
    result.sign = res_sign;
    if round {
        round_var(result, rscale);
    } else {
        trunc_var(result, rscale);
    }
    strip_var(result);
}

/// True iff `q * divisor` (big-endian, aligned under rem[1..]) is <= `rem`.
/// `rem` has length divisor.len()+1; its first element is the high carry slot.
fn cmp_scaled_le(divisor: &[i64], q: i64, rem: &[i64]) -> bool {
    let n = divisor.len();
    // Compute q*divisor into a big-endian buffer of length n+1, then compare.
    let mut prod = vec![0i64; n + 1];
    let mut carry = 0i64;
    for k in (0..n).rev() {
        let v = q * divisor[k] + carry;
        carry = v / i64::from(NBASE);
        prod[k + 1] = v % i64::from(NBASE);
    }
    prod[0] = carry;
    // prod <= rem ?
    for i in 0..=n {
        if prod[i] != rem[i] {
            return prod[i] < rem[i];
        }
    }
    true
}

/// PG `select_div_scale`.
fn select_div_scale(var1: &NumericVar, var2: &NumericVar) -> i32 {
    let mut weight1 = 0;
    let mut firstdigit1 = 0i16;
    for i in 0..var1.ndigits() {
        firstdigit1 = var1.digits[i as usize];
        if firstdigit1 != 0 {
            weight1 = var1.weight - i;
            break;
        }
    }
    let mut weight2 = 0;
    let mut firstdigit2 = 0i16;
    for i in 0..var2.ndigits() {
        firstdigit2 = var2.digits[i as usize];
        if firstdigit2 != 0 {
            weight2 = var2.weight - i;
            break;
        }
    }
    let mut qweight = weight1 - weight2;
    if firstdigit1 <= firstdigit2 {
        qweight -= 1;
    }
    let mut rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS as i32;
    rscale = rscale.max(var1.dscale);
    rscale = rscale.max(var2.dscale);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);
    rscale
}

/// PG `mod_var`: mod(x,y) = x - trunc(x/y)*y.
fn mod_var(var1: &NumericVar, var2: &NumericVar, result: &mut NumericVar) {
    let mut tmp = NumericVar::new();
    div_var(var1, var2, &mut tmp, 0, false);
    let mut prod = NumericVar::new();
    mul_var(var2, &tmp, &mut prod, var2.dscale);
    sub_var(var1, &prod, result);
}

/// PG `ceil_var`.
fn ceil_var(var: &NumericVar, result: &mut NumericVar) {
    let mut tmp = var.clone();
    trunc_var(&mut tmp, 0);
    if var.sign == NUMERIC_POS && cmp_var(var, &tmp) != 0 {
        let mut t2 = NumericVar::new();
        add_var(&tmp, &const_one(), &mut t2);
        tmp = t2;
    }
    *result = tmp;
}

/// PG `floor_var`.
fn floor_var(var: &NumericVar, result: &mut NumericVar) {
    let mut tmp = var.clone();
    trunc_var(&mut tmp, 0);
    if var.sign == NUMERIC_NEG && cmp_var(var, &tmp) != 0 {
        let mut t2 = NumericVar::new();
        sub_var(&tmp, &const_one(), &mut t2);
        tmp = t2;
    }
    *result = tmp;
}

/// PG `gcd_var`: Euclidean GCD; result dscale = max(input dscales).
fn gcd_var(var1: &NumericVar, var2: &NumericVar, result: &mut NumericVar) {
    let res_dscale = var1.dscale.max(var2.dscale);
    let mut a = var1.clone();
    let mut b = var2.clone();
    a.sign = NUMERIC_POS;
    b.sign = NUMERIC_POS;
    if cmp_abs(&a, &b) < 0 {
        std::mem::swap(&mut a, &mut b);
    }
    while b.ndigits() != 0 {
        let mut m = NumericVar::new();
        mod_var(&a, &b, &mut m);
        a = b;
        b = m;
        a.sign = NUMERIC_POS;
        b.sign = NUMERIC_POS;
    }
    a.dscale = res_dscale;
    *result = a;
}

/// PG `round_var`: round to rscale decimal digits after the point.
fn round_var(var: &mut NumericVar, rscale: i32) {
    var.dscale = rscale;
    let di = (var.weight + 1) * DEC_DIGITS as i32 + rscale;
    if di < 0 {
        var.digits.clear();
        var.weight = 0;
        var.sign = NUMERIC_POS;
        return;
    }
    let mut ndigits = (di + DEC_DIGITS as i32 - 1) / DEC_DIGITS as i32;
    let di_mod = di % DEC_DIGITS as i32;

    if ndigits < var.ndigits() || (ndigits == var.ndigits() && di_mod > 0) {
        let mut carry;
        if di_mod == 0 {
            // Rounding falls on a whole-NBASE-digit boundary; the first dropped
            // digit (at index ndigits) decides the carry. Read it before trunc.
            let next = if (ndigits as usize) < var.digits.len() {
                i32::from(var.digits[ndigits as usize])
            } else {
                0
            };
            var.digits.truncate(ndigits as usize);
            carry = i32::from(next >= HALF_NBASE);
        } else {
            var.digits.truncate(ndigits as usize);
            let pow10 = ROUND_POWERS[di_mod as usize];
            ndigits -= 1;
            let extra = i32::from(var.digits[ndigits as usize]) % pow10;
            var.digits[ndigits as usize] -= extra as i16;
            carry = 0;
            if extra >= pow10 / 2 {
                let mut p = pow10 + i32::from(var.digits[ndigits as usize]);
                if p >= NBASE {
                    p -= NBASE;
                    carry = 1;
                }
                var.digits[ndigits as usize] = p as i16;
            }
        }
        // propagate carry
        while carry != 0 {
            if ndigits == 0 {
                // carry out the top: prepend a digit
                var.digits.insert(0, carry as i16);
                var.weight += 1;
                break;
            }
            ndigits -= 1;
            carry += i32::from(var.digits[ndigits as usize]);
            if carry >= NBASE {
                var.digits[ndigits as usize] = (carry - NBASE) as i16;
                carry = 1;
            } else {
                var.digits[ndigits as usize] = carry as i16;
                carry = 0;
            }
        }
    }
}

/// PG `trunc_var`: truncate toward zero at rscale decimal digits.
fn trunc_var(var: &mut NumericVar, rscale: i32) {
    var.dscale = rscale;
    let di = (var.weight + 1) * DEC_DIGITS as i32 + rscale;
    if di <= 0 {
        var.digits.clear();
        var.weight = 0;
        var.sign = NUMERIC_POS;
        return;
    }
    let mut ndigits = (di + DEC_DIGITS as i32 - 1) / DEC_DIGITS as i32;
    if ndigits <= var.ndigits() {
        var.digits.truncate(ndigits as usize);
        let di_mod = di % DEC_DIGITS as i32;
        if di_mod > 0 {
            let pow10 = ROUND_POWERS[di_mod as usize];
            ndigits -= 1;
            let extra = i32::from(var.digits[ndigits as usize]) % pow10;
            var.digits[ndigits as usize] -= extra as i16;
        }
    }
}

/// PG `strip_var`: strip leading/trailing zero digits, normalize zero.
fn strip_var(var: &mut NumericVar) {
    let mut lead = 0;
    while lead < var.digits.len() && var.digits[lead] == 0 {
        lead += 1;
        var.weight -= 1;
    }
    if lead > 0 {
        var.digits.drain(0..lead);
    }
    while var.digits.last() == Some(&0) {
        var.digits.pop();
    }
    if var.digits.is_empty() {
        var.sign = NUMERIC_POS;
        var.weight = 0;
    }
}

// ===========================================================================
//   numeric_out_sci (numeric.h)
// ===========================================================================

/// PG `numeric_out_sci`: scientific-notation rendering (callable from Rust).
#[must_use]
pub fn numeric_out_sci_bytes(num_bytes: &[u8], scale: i32) -> String {
    let num = PackedNumeric { bytes: num_bytes.to_vec() };
    if num.is_special() {
        return if num.is_pinf() {
            "Infinity".to_owned()
        } else if num.is_ninf() {
            "-Infinity".to_owned()
        } else {
            "NaN".to_owned()
        };
    }
    let x = var_from_num(&num);
    get_str_from_var_sci(&x, scale.max(0))
}

fn get_str_from_var_sci(var: &NumericVar, rscale: i32) -> String {
    let exponent: i32 = if var.ndigits() > 0 {
        let mut e = (var.weight + 1) * DEC_DIGITS as i32;
        e -= DEC_DIGITS as i32 - (f64::from(var.digits[0]).log10() as i32);
        e
    } else {
        0
    };
    // significand = var / 10^exponent, rounded to rscale.
    let mut ten = NumericVar::new();
    power_ten_int(exponent, &mut ten);
    let mut sig = NumericVar::new();
    div_var(var, &ten, &mut sig, rscale, true);
    let sig_out = get_str_from_var(&sig);
    format!("{sig_out}e{exponent:+03}")
}

/// PG `power_ten_int`: result = 10^exp (used by numeric_out_sci).
fn power_ten_int(exp: i32, result: &mut NumericVar) {
    if exp == 0 {
        *result = const_one();
        return;
    }
    let mut e = exp;
    let neg = e < 0;
    if neg {
        e = -e;
    }
    let mut ten = NumericVar::new();
    int64_to_numericvar(10, &mut ten);
    let mut acc = const_one();
    for _ in 0..e {
        let mut prod = NumericVar::new();
        mul_var(&acc, &ten, &mut prod, 0);
        acc = prod;
    }
    if neg {
        let mut inv = NumericVar::new();
        div_var(&const_one(), &acc, &mut inv, exp.unsigned_abs() as i32 + NUMERIC_MIN_SIG_DIGITS, true);
        *result = inv;
    } else {
        *result = acc;
    }
}

// ===========================================================================
//   STAGED: transcendental / scientific functions (rules.md s4)
// ===========================================================================

/// PG `numeric_sqrt`: STAGED -- square root (transcendental kernel sqrt_var).
pub fn numeric_sqrt(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numeric_sqrt: deferred -- transcendental, not yet reachable")
}
/// PG `numeric_exp`: STAGED -- e^x (transcendental kernel exp_var).
pub fn numeric_exp(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numeric_exp: deferred -- transcendental, not yet reachable")
}
/// PG `numeric_ln`: STAGED -- natural log (transcendental kernel ln_var).
pub fn numeric_ln(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numeric_ln: deferred -- transcendental, not yet reachable")
}
/// PG `numeric_log`: STAGED -- log_b(x) (transcendental kernel log_var).
pub fn numeric_log(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numeric_log: deferred -- transcendental, not yet reachable")
}
/// PG `numeric_power`: STAGED -- x^y (transcendental kernel power_var).
pub fn numeric_power(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numeric_power: deferred -- transcendental, not yet reachable")
}
/// PG `numeric_fac`: STAGED -- factorial.
pub fn numeric_fac(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numeric_fac: deferred -- transcendental, not yet reachable")
}
/// PG `numeric_random`: STAGED -- needs the prng-state arg marshalling.
pub fn numeric_random(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numeric_random: deferred -- needs pg_prng state marshalling")
}

// ===========================================================================
//   STAGED: aggregate accumulators (reach the executor agg-context; rules.md s4)
// ===========================================================================

macro_rules! agg_stub {
    ($name:ident, $what:expr) => {
        #[doc = concat!("PG `", stringify!($name), "`: STAGED -- needs the aggregate context.")]
        pub fn $name(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            unimplemented!(concat!(stringify!($name), ": needs AggCheckCallContext + MemoryContextAlloc (", $what, ")"))
        }
    };
}
agg_stub!(numeric_accum, "NumericAggState sumX/sumX2");
agg_stub!(numeric_accum_inv, "NumericAggState moving-aggregate");
agg_stub!(numeric_combine, "NumericAggState combine");
agg_stub!(numeric_avg_accum, "NumericAggState sumX");
agg_stub!(numeric_avg_combine, "NumericAggState combine");
agg_stub!(numeric_avg_serialize, "NumericAggState serialize");
agg_stub!(numeric_avg_deserialize, "NumericAggState deserialize");
agg_stub!(numeric_serialize, "NumericAggState serialize");
agg_stub!(numeric_deserialize, "NumericAggState deserialize");
agg_stub!(numeric_avg, "NumericAggState final");
agg_stub!(numeric_sum, "NumericAggState final");
agg_stub!(numeric_var_samp, "NumericAggState variance");
agg_stub!(numeric_var_pop, "NumericAggState variance");
agg_stub!(numeric_stddev_samp, "NumericAggState stddev");
agg_stub!(numeric_stddev_pop, "NumericAggState stddev");
agg_stub!(numeric_poly_combine, "PolyNumAggState combine");
agg_stub!(numeric_poly_serialize, "PolyNumAggState serialize");
agg_stub!(numeric_poly_deserialize, "PolyNumAggState deserialize");
agg_stub!(numeric_poly_sum, "PolyNumAggState final");
agg_stub!(numeric_poly_avg, "PolyNumAggState final");
agg_stub!(numeric_poly_var_samp, "PolyNumAggState variance");
agg_stub!(numeric_poly_var_pop, "PolyNumAggState variance");
agg_stub!(numeric_poly_stddev_samp, "PolyNumAggState stddev");
agg_stub!(numeric_poly_stddev_pop, "PolyNumAggState stddev");

// ===========================================================================
//   STAGED: sortsupport / planner support / misc conversions
// ===========================================================================

/// PG `numeric_sortsupport`: STAGED -- abbreviated-key sortsupport.
pub fn numeric_sortsupport(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numeric_sortsupport: needs SortSupport + abbreviation (deferred)")
}
/// PG `numeric_support`: STAGED -- planner length-coercion support node.
pub fn numeric_support(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numeric_support: needs support-node introspection (deferred)")
}
/// PG `numerictypmodin`: STAGED -- needs ArrayType typmod parsing.
pub fn numerictypmodin(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numerictypmodin: needs the array typmod machinery (deferred)")
}
/// PG `numerictypmodout`: STAGED -- typmod -> "(p,s)" cstring.
pub fn numerictypmodout(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numerictypmodout: deferred")
}
/// PG `numeric_pg_lsn`: STAGED -- numeric -> pg_lsn.
pub fn numeric_pg_lsn(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("numeric_pg_lsn: deferred")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::NullableDatum;
    use std::panic::catch_unwind;

    fn fc(args: &[Datum]) -> FunctionCallInfoBaseData {
        FunctionCallInfoBaseData {
            flinfo: None,
            context: None,
            resultinfo: None,
            fncollation: crate::postgres_ext::InvalidOid,
            isnull: false,
            nargs: args.len() as i16,
            args: args.iter().map(|&value| NullableDatum { value, isnull: false }).collect(),
        }
    }
    fn cstr_datum(s: &str) -> Datum {
        let c = std::ffi::CString::new(s).unwrap();
        CStringGetDatum(c.into_raw())
    }
    fn out_to_string(d: Datum) -> String {
        let p = DatumGetCString(d);
        let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
        cstr.to_string_lossy().into_owned()
    }
    /// Parse a literal into a numeric Datum (no typmod).
    fn num(s: &str) -> Datum {
        let mut f = fc(&[cstr_datum(s)]);
        numeric_in(&mut f)
    }
    /// numeric Datum -> output string.
    fn num_out(d: Datum) -> String {
        let mut f = fc(&[d]);
        out_to_string(numeric_out(&mut f))
    }

    #[test]
    fn in_out_roundtrip() {
        for s in [
            "0", "1", "-1", "42", "3.14", "-3.14", "100.00", "0.001", "-0.5",
            "12345678901234567890", "0.10", "1.000",
        ] {
            assert_eq!(num_out(num(s)), s, "roundtrip {s}");
        }
    }

    #[test]
    fn in_out_specials_and_sci() {
        assert_eq!(num_out(num("NaN")), "NaN");
        assert_eq!(num_out(num("Infinity")), "Infinity");
        assert_eq!(num_out(num("-Infinity")), "-Infinity");
        assert_eq!(num_out(num("inf")), "Infinity");
        // scientific input
        assert_eq!(num_out(num("1.5e2")), "150");
        assert_eq!(num_out(num("1.5e-2")), "0.015");
        assert_eq!(num_out(num("100e-2")), "1.00");
        // leading/trailing zeros and dscale preservation
        assert_eq!(num_out(num("007")), "7");
        assert_eq!(num_out(num("1.500")), "1.500");
        // hex/oct/bin
        assert_eq!(num_out(num("0x10")), "16");
        assert_eq!(num_out(num("0o17")), "15");
        assert_eq!(num_out(num("0b101")), "5");
    }

    fn binop(f: fn(&mut FunctionCallInfoBaseData) -> Datum, a: &str, b: &str) -> String {
        let mut fci = fc(&[num(a), num(b)]);
        num_out(f(&mut fci))
    }

    #[test]
    fn arithmetic_scale() {
        assert_eq!(binop(numeric_add, "1.5", "2.25"), "3.75");
        assert_eq!(binop(numeric_sub, "5.00", "1.5"), "3.50");
        assert_eq!(binop(numeric_mul, "1.5", "2.25"), "3.375");
        assert_eq!(binop(numeric_mul, "2", "3"), "6");
        assert_eq!(binop(numeric_add, "-1", "1"), "0");
        // 10/3 to the default selected scale.
        let q = binop(numeric_div, "10", "3");
        assert!(q.starts_with("3.3333333333333333"), "10/3 = {q}");
        // div_trunc and mod
        assert_eq!(binop(numeric_div_trunc, "10", "3"), "3");
        assert_eq!(binop(numeric_mod, "10", "3"), "1");
        assert_eq!(binop(numeric_mod, "10.5", "3"), "1.5");
        assert_eq!(binop(numeric_div, "1", "4"), "0.25000000000000000000");
        // multi-NBASE-digit divisor long-division paths
        assert_eq!(binop(numeric_div_trunc, "100000000", "123457"), "809");
        assert_eq!(binop(numeric_mul, "12345.678", "1000"), "12345678.000");
        assert_eq!(binop(numeric_mul, "99999999", "99999999"), "9999999800000001");
        let s = binop(numeric_div, "22", "7");
        assert!(s.starts_with("3.142857142857"), "22/7 = {s}");
        let s = binop(numeric_div, "1", "7");
        assert!(s.starts_with("0.142857142857"), "1/7 = {s}");
        // exact big multiply then exact divide back
        assert_eq!(binop(numeric_div_trunc, "9999999800000001", "99999999"), "99999999");
    }

    #[test]
    fn division_by_zero_raises() {
        assert!(catch_unwind(|| {
            let mut f = fc(&[num("1"), num("0")]);
            numeric_div(&mut f)
        })
        .is_err());
    }

    #[test]
    fn comparison_across_scales() {
        let cmp = |a: &str, b: &str| {
            let mut f = fc(&[num(a), num(b)]);
            DatumGetInt32(numeric_cmp(&mut f))
        };
        assert_eq!(cmp("1.0", "1.00"), 0);
        assert_eq!(cmp("2.5", "2.49"), 1);
        assert_eq!(cmp("-1", "1"), -1);
        let eq = |a: &str, b: &str| {
            let mut f = fc(&[num(a), num(b)]);
            crate::postgres::DatumGetBool(numeric_eq(&mut f))
        };
        assert!(eq("1.0", "1.00"));
        let gt = |a: &str, b: &str| {
            let mut f = fc(&[num(a), num(b)]);
            crate::postgres::DatumGetBool(numeric_gt(&mut f))
        };
        assert!(gt("2.5", "2.49"));
        // NaN sorts greater than everything.
        assert_eq!(cmp("NaN", "1"), 1);
        assert_eq!(cmp("Infinity", "1"), 1);
        assert_eq!(cmp("-Infinity", "1"), -1);
    }

    #[test]
    fn int_casts_roundtrip() {
        // int4 -> numeric -> int4
        let mut f = fc(&[Int32GetDatum(-12345)]);
        let n = int4_numeric(&mut f);
        assert_eq!(num_out(n), "-12345");
        let mut f = fc(&[n]);
        assert_eq!(DatumGetInt32(numeric_int4(&mut f)), -12345);
        // int8
        let mut f = fc(&[Int64GetDatum(9_000_000_000)]);
        let n = int8_numeric(&mut f);
        let mut f = fc(&[n]);
        assert_eq!(DatumGetInt64(numeric_int8(&mut f)), 9_000_000_000);
        // rounding numeric -> int4
        let mut f = fc(&[num("3.7")]);
        assert_eq!(DatumGetInt32(numeric_int4(&mut f)), 4);
        let mut f = fc(&[num("-3.7")]);
        assert_eq!(DatumGetInt32(numeric_int4(&mut f)), -4);
        // out of range
        assert!(catch_unwind(|| {
            let mut f = fc(&[num("99999999999")]);
            numeric_int4(&mut f)
        })
        .is_err());
    }

    #[test]
    fn float_casts_roundtrip() {
        let mut f = fc(&[Float8GetDatum(1.5)]);
        let n = float8_numeric(&mut f);
        assert_eq!(num_out(n), "1.5");
        let mut f = fc(&[num("2.5")]);
        assert!((DatumGetFloat8(numeric_float8(&mut f)) - 2.5).abs() < 1e-12);
        let mut f = fc(&[Float4GetDatum(0.5f32)]);
        let n = float4_numeric(&mut f);
        assert_eq!(num_out(n), "0.5");
    }

    #[test]
    fn round_trunc_ceil_floor_sign() {
        let r = |a: &str, s: i32| {
            let mut f = fc(&[num(a), Int32GetDatum(s)]);
            num_out(numeric_round(&mut f))
        };
        assert_eq!(r("3.14159", 2), "3.14");
        assert_eq!(r("2.5", 0), "3");
        assert_eq!(r("-2.5", 0), "-3");
        let t = |a: &str, s: i32| {
            let mut f = fc(&[num(a), Int32GetDatum(s)]);
            num_out(numeric_trunc(&mut f))
        };
        assert_eq!(t("3.99", 1), "3.9");
        assert_eq!(t("-3.99", 1), "-3.9");
        let unary = |f: fn(&mut FunctionCallInfoBaseData) -> Datum, a: &str| {
            let mut fci = fc(&[num(a)]);
            num_out(f(&mut fci))
        };
        assert_eq!(unary(numeric_ceil, "1.1"), "2");
        assert_eq!(unary(numeric_ceil, "-1.1"), "-1");
        assert_eq!(unary(numeric_floor, "1.9"), "1");
        assert_eq!(unary(numeric_floor, "-1.1"), "-2");
        assert_eq!(unary(numeric_sign, "-5"), "-1");
        assert_eq!(unary(numeric_sign, "0"), "0");
        assert_eq!(unary(numeric_abs, "-3.5"), "3.5");
        assert_eq!(unary(numeric_uminus, "3.5"), "-3.5");
        assert_eq!(unary(numeric_inc, "1.5"), "2.5");
    }

    /// The on-disk varlena bytes for the integer 1: short header, one digit.
    #[test]
    fn ondisk_encoding_of_one() {
        let mut f = fc(&[cstr_datum("1")]);
        let d = numeric_in(&mut f);
        let p = DatumGetPointer(d);
        let total = unsafe { VARSIZE_ANY(p) };
        let bytes = unsafe { core::slice::from_raw_parts(p, total) };
        // varlena header (4B) + short numeric header (2B) + one digit (2B) = 8.
        assert_eq!(total, 8, "numeric 1 is 8 bytes on disk");
        let header = u16::from_ne_bytes([bytes[4], bytes[5]]);
        // NUMERIC_SHORT, positive, dscale 0, weight 0.
        assert_eq!(header & NUMERIC_SIGN_MASK, NUMERIC_SHORT);
        assert_eq!(header & NUMERIC_SHORT_SIGN_MASK, 0); // positive
        assert_eq!((header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT, 0);
        assert_eq!(header & NUMERIC_SHORT_WEIGHT_MASK, 0); // weight 0
        let digit = i16::from_ne_bytes([bytes[6], bytes[7]]);
        assert_eq!(digit, 1);
    }

    /// numeric_out resolves through the generated fmgr table to a bound fn.
    #[test]
    fn fmgr_table_binds_numeric_out() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "numeric_out")
            .expect("numeric_out present in builtin table");
        let func = entry.func.expect("numeric_out bound to a Rust fn");
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
        let s = crate::fmgr::OutputFunctionCall(&mut flinfo, num("42"));
        assert_eq!(s, "42");
    }
}
