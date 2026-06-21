//! Translation of postgres/src/backend/utils/adt/cash.c
//!                (+ postgres/src/include/utils/cash.h merged in)
//!
//! The "money" type: stored and handled as a 64-bit integer (Cash = int64),
//! holding values up to $92,233,720,368,547,758.07.
//!
//! Written by D'Arcy J.M. Cain.
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//!
//! `#include`s mapped: common/int.h -> crate::common::int (pg_{add,sub,mul}_s64_overflow,
//! pg_abs_s64); libpq/pqformat.h -> crate::libpq::pqformat (pq_getmsgint64/pq_sendint64,
//! pq_begintypsend/pq_endtypsend); utils/builtins.h cstring_to_text_with_len -> crate::utils::adt::varlena,
//! pg_toupper -> crate::port::pgstrcasecmp; utils/float.h float8_mul/float8_div -> crate::utils::adt::float;
//! lib/stringinfo.h -> crate::lib::stringinfo (initStringInfo/appendStringInfo*).  <math.h> rint bound
//! via extern "C".  int8mul (for int4_cash/int8_cash) -> crate::utils::adt::int8.
//!
//! LOCALE STUB: utils/pg_locale.h PGLC_localeconv() is not ported.  We substitute a fixed default
//! `lconv` matching the C/POSIX monetary locale that PostgreSQL falls back to when libc gives no
//! monetary data: frac_digits=2, mon_decimal_point=".", mon_thousands_sep=",", currency_symbol="$",
//! positive_sign="", negative_sign="-", and the p/n sign-position fields used by cash_out().  The
//! parser/formatter logic is otherwise self-contained.
//! TODO(pg-port): wire real locale data once pg_locale.c (PGLC_localeconv) is translated.
//!
//! cash_numeric/numeric_cash use utils/numeric.h (int64_to_numeric, numeric_round/div/mul/int8)
//! from crate::utils::adt::numeric.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::*;
use crate::{
    appendStringInfo, PG_GETARG_CSTRING, PG_GETARG_DATUM, PG_GETARG_FLOAT4, PG_GETARG_FLOAT8,
    PG_GETARG_INT16, PG_GETARG_INT32, PG_GETARG_INT64, PG_GETARG_POINTER, PG_RETURN_BOOL,
    PG_RETURN_BYTEA_P, PG_RETURN_CSTRING, PG_RETURN_FLOAT8, PG_RETURN_INT32, PG_RETURN_TEXT_P,
};
use crate::c::{float4, float8, int16, int32, int64, text, PG_INT64_MIN};
use crate::common::int::{pg_abs_s64, pg_add_s64_overflow, pg_mul_s64_overflow, pg_sub_s64_overflow};
use crate::lib::stringinfo::{appendStringInfoString, initStringInfo, StringInfo, StringInfoData};
use crate::libpq::pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgint64, pq_sendint64};
use crate::port::pgstrcasecmp::pg_toupper;
use crate::postgres::{DatumGetInt64, Int32GetDatum, Int64GetDatum, PointerGetDatum};
use crate::utils::adt::float::{float8_div, float8_mul};
use crate::utils::adt::int8::int8mul;
use crate::utils::adt::numeric::{
    int64_to_numeric, numeric_div, numeric_int8, numeric_mul, numeric_round, Numeric,
};
use crate::utils::adt::varlena::cstring_to_text_with_len;
use core::ffi::{c_char, c_int, c_void};

// ---- utils/cash.h ----
/* the "money" datatype is stored and handled as a 64-bit integer */
pub type Cash = int64;

/* Cash is pass-by-reference if and only if int64 is (it isn't here) */
#[inline]
pub unsafe fn DatumGetCash(x: Datum) -> Cash {
    DatumGetInt64(x)
}
#[inline]
pub fn CashGetDatum(x: Cash) -> Datum {
    Int64GetDatum(x)
}
// PG_GETARG_CASH(n) == DatumGetCash(PG_GETARG_DATUM!(fcinfo, n))
// PG_RETURN_CASH(x) == return CashGetDatum(x)

// <math.h>: rint rounds to nearest integer (ties to even), passing NaN/Inf through.
extern "C" {
    fn rint(x: f64) -> f64;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE: c_int = 0;
const ERRCODE_DIVISION_BY_ZERO: c_int = 0;
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;

// utils/float.h: FLOAT8_FITS_IN_INT64(x) == x is in [-2^63, 2^63) (excludes NaN).
#[inline]
fn FLOAT8_FITS_IN_INT64(x: float8) -> bool {
    x >= PG_INT64_MIN as float8 && x < -(PG_INT64_MIN as float8)
}

// limits.h CHAR_MAX is not needed: the C code deliberately range-checks frac_digits rather
// than testing for CHAR_MAX (see comments in cash_in).

/*
 * Default monetary locale (the C/POSIX fallback PostgreSQL uses when libc supplies
 * no monetary data).  Stands in for `struct lconv` from PGLC_localeconv().
 * TODO(pg-port): replace with real PGLC_localeconv() once pg_locale.c is translated.
 */
struct DefaultLconv {
    frac_digits: c_int,
    mon_decimal_point: &'static [u8], // NUL-terminated
    mon_thousands_sep: &'static [u8], // NUL-terminated
    mon_grouping: &'static [u8],      // first byte is the group size (3), then NUL
    currency_symbol: &'static [u8],   // NUL-terminated
    positive_sign: &'static [u8],     // NUL-terminated (empty in C locale)
    negative_sign: &'static [u8],     // NUL-terminated
    p_cs_precedes: c_char,
    n_cs_precedes: c_char,
    p_sep_by_space: c_char,
    n_sep_by_space: c_char,
    p_sign_posn: c_char,
    n_sign_posn: c_char,
}

const DEFAULT_LCONV: DefaultLconv = DefaultLconv {
    frac_digits: 2,
    mon_decimal_point: b".\0",
    mon_thousands_sep: b",\0",
    mon_grouping: b"\x03\0",
    currency_symbol: b"$\0",
    positive_sign: b"\0",
    negative_sign: b"-\0",
    p_cs_precedes: 1,
    n_cs_precedes: 1,
    p_sep_by_space: 0,
    n_sep_by_space: 0,
    p_sign_posn: 1,
    n_sign_posn: 1,
};

/*************************************************************************
 * Private routines
 ************************************************************************/

/*
 * # Safety
 * `buf` must be a valid initialized StringInfo.
 */
unsafe fn append_num_word(buf: StringInfo, value: Cash) {
    const SMALL: [&[u8]; 28] = [
        b"zero", b"one", b"two", b"three", b"four", b"five", b"six", b"seven",
        b"eight", b"nine", b"ten", b"eleven", b"twelve", b"thirteen", b"fourteen",
        b"fifteen", b"sixteen", b"seventeen", b"eighteen", b"nineteen", b"twenty",
        b"thirty", b"forty", b"fifty", b"sixty", b"seventy", b"eighty", b"ninety",
    ];
    // big = small + 18 (i.e. big[i] == SMALL[18 + i]).
    let big = |i: usize| SMALL[18 + i];
    let tu = (value % 100) as i64;

    /* deal with the simple cases first */
    if value <= 20 {
        append_str(buf, SMALL[value as usize]);
        return;
    }

    /* is it an even multiple of 100? */
    if tu == 0 {
        appendStringInfo!(buf, "{} hundred", as_str(SMALL[(value / 100) as usize]));
        return;
    }

    /* more than 99? */
    if value > 99 {
        /* is it an even multiple of 10 other than 10? */
        if value % 10 == 0 && tu > 10 {
            appendStringInfo!(
                buf,
                "{} hundred {}",
                as_str(SMALL[(value / 100) as usize]),
                as_str(big((tu / 10) as usize))
            );
        } else if tu < 20 {
            appendStringInfo!(
                buf,
                "{} hundred and {}",
                as_str(SMALL[(value / 100) as usize]),
                as_str(SMALL[tu as usize])
            );
        } else {
            appendStringInfo!(
                buf,
                "{} hundred {} {}",
                as_str(SMALL[(value / 100) as usize]),
                as_str(big((tu / 10) as usize)),
                as_str(SMALL[(tu % 10) as usize])
            );
        }
    } else {
        /* is it an even multiple of 10 other than 10? */
        if value % 10 == 0 && tu > 10 {
            append_str(buf, big((tu / 10) as usize));
        } else if tu < 20 {
            append_str(buf, SMALL[tu as usize]);
        } else {
            appendStringInfo!(
                buf,
                "{} {}",
                as_str(big((tu / 10) as usize)),
                as_str(SMALL[(tu % 10) as usize])
            );
        }
    }
}

#[inline]
unsafe fn cash_pl_cash(c1: Cash, c2: Cash) -> Cash {
    let mut res: Cash = 0;

    if pg_add_s64_overflow(c1, c2, &mut res) {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("money out of range"));
    }

    res
}

#[inline]
unsafe fn cash_mi_cash(c1: Cash, c2: Cash) -> Cash {
    let mut res: Cash = 0;

    if pg_sub_s64_overflow(c1, c2, &mut res) {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("money out of range"));
    }

    res
}

#[inline]
unsafe fn cash_mul_float8(c: Cash, f: float8) -> Cash {
    let res: float8 = rint(float8_mul(c as float8, f));

    if res.is_nan() || !FLOAT8_FITS_IN_INT64(res) {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("money out of range"));
    }

    res as Cash
}

#[inline]
unsafe fn cash_div_float8(c: Cash, f: float8) -> Cash {
    let res: float8 = rint(float8_div(c as float8, f));

    if res.is_nan() || !FLOAT8_FITS_IN_INT64(res) {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("money out of range"));
    }

    res as Cash
}

#[inline]
unsafe fn cash_mul_int64(c: Cash, i: int64) -> Cash {
    let mut res: Cash = 0;

    if pg_mul_s64_overflow(c, i, &mut res) {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("money out of range"));
    }

    res
}

#[inline]
unsafe fn cash_div_int64(c: Cash, i: int64) -> Cash {
    if i == 0 {
        let _ = errcode(ERRCODE_DIVISION_BY_ZERO);
        ereport!(ERROR, errmsg!("division by zero"));
    }

    c / i
}

/*
 * Report an input error through the real errsave mechanism so
 * pg_input_is_valid / pg_input_error_info see a populated ErrorSaveContext;
 * for a null/non-ErrorSaveContext this raises a hard ERROR.
 */
#[inline]
unsafe fn cash_input_soft_error(escontext: *mut crate::nodes::nodes::Node, errcode_val: c_int, msg: String) {
    if crate::utils::error::elog_impl::errsave_start(escontext, core::ptr::null()) {
        crate::utils::error::elog_impl::errcode_impl(errcode_val);
        if let Ok(c) = std::ffi::CString::new(msg) {
            crate::utils::error::elog_impl::errmsg_c(c.as_ptr());
        }
        crate::utils::error::elog_impl::errsave_finish(
            escontext, c"cash.rs".as_ptr(), 0, c"cash_in".as_ptr(),
        );
    }
}

/* cash_in()
 * Convert a string to a cash data type.
 * Format is [$]###[,]###[.##]
 * Examples: 123.45 $123.45 $123,456.78
 *
 */
pub unsafe fn cash_in(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext = (*fcinfo).context;
    let result: Cash;
    let mut value: Cash = 0;
    let mut dec: Cash = 0;
    let mut sgn: Cash = 1;
    let mut seen_dot: bool = false;
    let mut s: *const c_char = str;
    let mut fpoint: c_int;
    let dsymbol: c_char;
    let ssymbol: *const c_char;
    let psymbol: *const c_char;
    let nsymbol: *const c_char;
    let csymbol: *const c_char;
    let lconvert = &DEFAULT_LCONV; // PGLC_localeconv()

    /*
     * frac_digits will be CHAR_MAX in some locales, notably C.  However, just
     * testing for == CHAR_MAX is risky, because of compilers like gcc that
     * "helpfully" let you alter the platform-standard definition of whether
     * char is signed or not.  If we are so unfortunate as to get compiled
     * with a nonstandard -fsigned-char or -funsigned-char switch, then our
     * idea of CHAR_MAX will not agree with libc's. The safest course is not
     * to test for CHAR_MAX at all, but to impose a range check for plausible
     * frac_digits values.
     */
    fpoint = lconvert.frac_digits;
    if fpoint < 0 || fpoint > 10 {
        fpoint = 2; /* best guess in this case, I think */
    }

    /* we restrict dsymbol to be a single byte, but not the other symbols */
    if lconvert.mon_decimal_point[0] != b'\0' && lconvert.mon_decimal_point[1] == b'\0' {
        dsymbol = lconvert.mon_decimal_point[0] as c_char;
    } else {
        dsymbol = b'.' as c_char;
    }
    if lconvert.mon_thousands_sep[0] != b'\0' {
        ssymbol = lconvert.mon_thousands_sep.as_ptr() as *const c_char;
    } else {
        /* ssymbol should not equal dsymbol */
        ssymbol = if dsymbol != b',' as c_char {
            b",\0".as_ptr() as *const c_char
        } else {
            b".\0".as_ptr() as *const c_char
        };
    }
    csymbol = if lconvert.currency_symbol[0] != b'\0' {
        lconvert.currency_symbol.as_ptr() as *const c_char
    } else {
        b"$\0".as_ptr() as *const c_char
    };
    psymbol = if lconvert.positive_sign[0] != b'\0' {
        lconvert.positive_sign.as_ptr() as *const c_char
    } else {
        b"+\0".as_ptr() as *const c_char
    };
    nsymbol = if lconvert.negative_sign[0] != b'\0' {
        lconvert.negative_sign.as_ptr() as *const c_char
    } else {
        b"-\0".as_ptr() as *const c_char
    };

    /* we need to add all sorts of checking here.  For now just */
    /* strip all leading whitespace and any leading currency symbol */
    while isspace(*s as u8 as c_int) != 0 {
        s = s.add(1);
    }
    if strncmp(s, csymbol, strlen(csymbol)) == 0 {
        s = s.add(strlen(csymbol));
    }
    while isspace(*s as u8 as c_int) != 0 {
        s = s.add(1);
    }

    /* a leading minus or paren signifies a negative number */
    /* again, better heuristics needed */
    /* XXX - doesn't properly check for balanced parens - djmc */
    if strncmp(s, nsymbol, strlen(nsymbol)) == 0 {
        sgn = -1;
        s = s.add(strlen(nsymbol));
    } else if *s == b'(' as c_char {
        sgn = -1;
        s = s.add(1);
    } else if strncmp(s, psymbol, strlen(psymbol)) == 0 {
        s = s.add(strlen(psymbol));
    }

    /* allow whitespace and currency symbol after the sign, too */
    while isspace(*s as u8 as c_int) != 0 {
        s = s.add(1);
    }
    if strncmp(s, csymbol, strlen(csymbol)) == 0 {
        s = s.add(strlen(csymbol));
    }
    while isspace(*s as u8 as c_int) != 0 {
        s = s.add(1);
    }

    /*
     * We accumulate the absolute amount in "value" and then apply the sign at
     * the end.  (The sign can appear before or after the digits, so it would
     * be more complicated to do otherwise.)  Because of the larger range of
     * negative signed integers, we build "value" in the negative and then
     * flip the sign at the end, catching most-negative-number overflow if
     * necessary.
     */

    while *s != 0 {
        /*
         * We look for digits as long as we have found less than the required
         * number of decimal places.
         */
        if isdigit(*s as u8 as c_int) != 0 && (!seen_dot || dec < fpoint as Cash) {
            let digit: int64 = (*s as u8 - b'0') as int64;

            if pg_mul_s64_overflow(value, 10, &mut value)
                || pg_sub_s64_overflow(value, digit, &mut value)
            {
                // ereturn(escontext, ...) -> hard ERROR for now
                cash_input_soft_error(escontext as *mut crate::nodes::nodes::Node, 50331778,
                format!("value \"{}\" is out of range for type money", std::ffi::CStr::from_ptr(str).to_string_lossy()));
            return 0 as Datum;
            }

            if seen_dot {
                dec += 1;
            }
        }
        /* decimal point? then start counting fractions... */
        else if *s == dsymbol && !seen_dot {
            seen_dot = true;
        }
        /* ignore if "thousands" separator, else we're done */
        else if strncmp(s, ssymbol, strlen(ssymbol)) == 0 {
            s = s.add(strlen(ssymbol) - 1);
        } else {
            break;
        }

        s = s.add(1);
    }

    /* round off if there's another digit */
    if isdigit(*s as u8 as c_int) != 0 && *s as u8 >= b'5' {
        /* remember we build the value in the negative */
        if pg_sub_s64_overflow(value, 1, &mut value) {
            cash_input_soft_error(escontext as *mut crate::nodes::nodes::Node, 50331778,
                format!("value \"{}\" is out of range for type money", std::ffi::CStr::from_ptr(str).to_string_lossy()));
            return 0 as Datum;
        }
    }

    /* adjust for less than required decimal places */
    while dec < fpoint as Cash {
        if pg_mul_s64_overflow(value, 10, &mut value) {
            cash_input_soft_error(escontext as *mut crate::nodes::nodes::Node, 50331778,
                format!("value \"{}\" is out of range for type money", std::ffi::CStr::from_ptr(str).to_string_lossy()));
            return 0 as Datum;
        }
        dec += 1;
    }

    /*
     * should only be trailing digits followed by whitespace, right paren,
     * trailing sign, and/or trailing currency symbol
     */
    while isdigit(*s as u8 as c_int) != 0 {
        s = s.add(1);
    }

    while *s != 0 {
        if isspace(*s as u8 as c_int) != 0 || *s == b')' as c_char {
            s = s.add(1);
        } else if strncmp(s, nsymbol, strlen(nsymbol)) == 0 {
            sgn = -1;
            s = s.add(strlen(nsymbol));
        } else if strncmp(s, psymbol, strlen(psymbol)) == 0 {
            s = s.add(strlen(psymbol));
        } else if strncmp(s, csymbol, strlen(csymbol)) == 0 {
            s = s.add(strlen(csymbol));
        } else {
            cash_input_soft_error(escontext as *mut crate::nodes::nodes::Node, 33685634,
                format!("invalid input syntax for type money: \"{}\"", std::ffi::CStr::from_ptr(str).to_string_lossy()));
            return 0 as Datum;
        }
    }

    /*
     * If the value is supposed to be positive, flip the sign, but check for
     * the most negative number.
     */
    if sgn > 0 {
        if value == PG_INT64_MIN {
            cash_input_soft_error(escontext as *mut crate::nodes::nodes::Node, 50331778,
                format!("value \"{}\" is out of range for type money", std::ffi::CStr::from_ptr(str).to_string_lossy()));
            return 0 as Datum;
        }
        result = -value;
    } else {
        result = value;
    }

    return CashGetDatum(result); // PG_RETURN_CASH
}

/* cash_out()
 * Function to convert cash to a dollars and cents representation, using
 * the lc_monetary locale's formatting.
 */
pub unsafe fn cash_out(fcinfo: FunctionCallInfo) -> Datum {
    let value: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_CASH
    let mut uvalue: u64;
    let result: *mut c_char;
    let mut buf = [0u8; 128];
    let mut bufptr: usize; // index into buf[]
    let mut digit_pos: c_int;
    let mut points: c_int;
    let mut mon_group: c_int;
    let dsymbol: c_char;
    let ssymbol: &[u8]; // NUL-terminated bytes (the separator, without the NUL counted)
    let csymbol: &[u8];
    let signsymbol: &[u8];
    let sign_posn: c_char;
    let cs_precedes: c_char;
    let sep_by_space: c_char;
    let lconvert = &DEFAULT_LCONV; // PGLC_localeconv()

    /* see comments about frac_digits in cash_in() */
    points = lconvert.frac_digits;
    if points < 0 || points > 10 {
        points = 2; /* best guess in this case, I think */
    }

    /*
     * As with frac_digits, must apply a range check to mon_grouping to avoid
     * being fooled by variant CHAR_MAX values.
     */
    mon_group = lconvert.mon_grouping[0] as c_char as c_int;
    if mon_group <= 0 || mon_group > 6 {
        mon_group = 3;
    }

    /* we restrict dsymbol to be a single byte, but not the other symbols */
    if lconvert.mon_decimal_point[0] != b'\0' && lconvert.mon_decimal_point[1] == b'\0' {
        dsymbol = lconvert.mon_decimal_point[0] as c_char;
    } else {
        dsymbol = b'.' as c_char;
    }
    if lconvert.mon_thousands_sep[0] != b'\0' {
        ssymbol = nul_term_slice(lconvert.mon_thousands_sep);
    } else {
        /* ssymbol should not equal dsymbol */
        ssymbol = if dsymbol != b',' as c_char { b"," } else { b"." };
    }
    csymbol = if lconvert.currency_symbol[0] != b'\0' {
        nul_term_slice(lconvert.currency_symbol)
    } else {
        b"$"
    };

    if value < 0 {
        /* set up formatting data */
        signsymbol = if lconvert.negative_sign[0] != b'\0' {
            nul_term_slice(lconvert.negative_sign)
        } else {
            b"-"
        };
        sign_posn = lconvert.n_sign_posn;
        cs_precedes = lconvert.n_cs_precedes;
        sep_by_space = lconvert.n_sep_by_space;
    } else {
        signsymbol = nul_term_slice(lconvert.positive_sign);
        sign_posn = lconvert.p_sign_posn;
        cs_precedes = lconvert.p_cs_precedes;
        sep_by_space = lconvert.p_sep_by_space;
    }

    /* make the amount positive for digit-reconstruction loop */
    uvalue = pg_abs_s64(value);

    /* we build the digits+decimal-point+sep string right-to-left in buf[] */
    bufptr = buf.len() - 1;
    buf[bufptr] = b'\0';

    /*
     * Generate digits till there are no non-zero digits left and we emitted
     * at least one to the left of the decimal point.  digit_pos is the
     * current digit position, with zero as the digit just left of the decimal
     * point, increasing to the right.
     */
    digit_pos = points;
    loop {
        if points != 0 && digit_pos == 0 {
            /* insert decimal point, but not if value cannot be fractional */
            bufptr -= 1;
            buf[bufptr] = dsymbol as u8;
        } else if digit_pos < 0 && (digit_pos % mon_group) == 0 {
            /* insert thousands sep, but only to left of radix point */
            bufptr -= ssymbol.len();
            buf[bufptr..bufptr + ssymbol.len()].copy_from_slice(ssymbol);
        }

        bufptr -= 1;
        buf[bufptr] = (uvalue % 10) as u8 + b'0';
        uvalue /= 10;
        digit_pos -= 1;

        if !(uvalue != 0 || digit_pos >= 0) {
            break;
        }
    }

    let body = &buf[bufptr..buf.len() - 1]; // the digits string (without trailing NUL)

    /*----------
     * Now, attach currency symbol and sign symbol in the correct order.
     *
     * The POSIX spec defines these values controlling this code:
     *
     * p/n_sign_posn:
     *	0	Parentheses enclose the quantity and the currency_symbol.
     *	1	The sign string precedes the quantity and the currency_symbol.
     *	2	The sign string succeeds the quantity and the currency_symbol.
     *	3	The sign string precedes the currency_symbol.
     *	4	The sign string succeeds the currency_symbol.
     *
     * p/n_cs_precedes: 0 means currency symbol after value, else before it.
     *
     * p/n_sep_by_space:
     *	0	No <space> separates the currency symbol and value.
     *	1	If the currency symbol and sign string are adjacent, a <space>
     *		separates them from the value; otherwise, a <space> separates
     *		the currency symbol from the value.
     *	2	If the currency symbol and sign string are adjacent, a <space>
     *		separates them; otherwise, a <space> separates the sign string
     *		from the value.
     *----------
     */
    let cs = bstr(csymbol);
    let bp = bstr(body);
    let ss = bstr(signsymbol);
    let sp1 = if sep_by_space == 1 { " " } else { "" };
    let sp2 = if sep_by_space == 2 { " " } else { "" };
    let formatted: std::string::String = match sign_posn {
        0 => {
            if cs_precedes != 0 {
                format!("({}{}{})", cs, sp1, bp)
            } else {
                format!("({}{}{})", bp, sp1, cs)
            }
        }
        2 => {
            if cs_precedes != 0 {
                format!("{}{}{}{}{}", cs, sp1, bp, sp2, ss)
            } else {
                format!("{}{}{}{}{}", bp, sp1, cs, sp2, ss)
            }
        }
        3 => {
            if cs_precedes != 0 {
                format!("{}{}{}{}{}", ss, sp2, cs, sp1, bp)
            } else {
                format!("{}{}{}{}{}", bp, sp1, ss, sp2, cs)
            }
        }
        4 => {
            if cs_precedes != 0 {
                format!("{}{}{}{}{}", cs, sp2, ss, sp1, bp)
            } else {
                format!("{}{}{}{}{}", bp, sp1, cs, sp2, ss)
            }
        }
        /* case 1 and default */
        _ => {
            if cs_precedes != 0 {
                format!("{}{}{}{}{}", ss, sp2, cs, sp1, bp)
            } else {
                format!("{}{}{}{}{}", ss, sp2, bp, sp1, cs)
            }
        }
    };

    result = cstring_dup(&formatted); // psprintf(...)
    PG_RETURN_CSTRING!(result);
}

/*
 *		cash_recv			- converts external binary format to cash
 */
pub unsafe fn cash_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    return CashGetDatum(pq_getmsgint64(buf) as Cash); // PG_RETURN_CASH
}

/*
 *		cash_send			- converts cash to binary format
 */
pub unsafe fn cash_send(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_CASH
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint64(&mut buf, arg1 as u64);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf));
}

/*
 * Comparison functions
 */

pub unsafe fn cash_eq(fcinfo: FunctionCallInfo) -> Datum {
    let c1: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let c2: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(c1 == c2);
}

pub unsafe fn cash_ne(fcinfo: FunctionCallInfo) -> Datum {
    let c1: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let c2: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(c1 != c2);
}

pub unsafe fn cash_lt(fcinfo: FunctionCallInfo) -> Datum {
    let c1: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let c2: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(c1 < c2);
}

pub unsafe fn cash_le(fcinfo: FunctionCallInfo) -> Datum {
    let c1: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let c2: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(c1 <= c2);
}

pub unsafe fn cash_gt(fcinfo: FunctionCallInfo) -> Datum {
    let c1: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let c2: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(c1 > c2);
}

pub unsafe fn cash_ge(fcinfo: FunctionCallInfo) -> Datum {
    let c1: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let c2: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    PG_RETURN_BOOL!(c1 >= c2);
}

pub unsafe fn cash_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let c1: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let c2: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    if c1 > c2 {
        PG_RETURN_INT32!(1);
    } else if c1 == c2 {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(-1);
    }
}

/* cash_pl()
 * Add two cash values.
 */
pub unsafe fn cash_pl(fcinfo: FunctionCallInfo) -> Datum {
    let c1: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let c2: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    return CashGetDatum(cash_pl_cash(c1, c2)); // PG_RETURN_CASH
}

/* cash_mi()
 * Subtract two cash values.
 */
pub unsafe fn cash_mi(fcinfo: FunctionCallInfo) -> Datum {
    let c1: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let c2: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    return CashGetDatum(cash_mi_cash(c1, c2)); // PG_RETURN_CASH
}

/* cash_div_cash()
 * Divide cash by cash, returning float8.
 */
pub unsafe fn cash_div_cash(fcinfo: FunctionCallInfo) -> Datum {
    let dividend: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let divisor: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));
    let quotient: float8;

    if divisor == 0 {
        let _ = errcode(ERRCODE_DIVISION_BY_ZERO);
        ereport!(ERROR, errmsg!("division by zero"));
    }

    quotient = dividend as float8 / divisor as float8;
    PG_RETURN_FLOAT8!(quotient);
}

/* cash_mul_flt8()
 * Multiply cash by float8.
 */
pub unsafe fn cash_mul_flt8(fcinfo: FunctionCallInfo) -> Datum {
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let f: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    return CashGetDatum(cash_mul_float8(c, f)); // PG_RETURN_CASH
}

/* flt8_mul_cash()
 * Multiply float8 by cash.
 */
pub unsafe fn flt8_mul_cash(fcinfo: FunctionCallInfo) -> Datum {
    let f: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    return CashGetDatum(cash_mul_float8(c, f)); // PG_RETURN_CASH
}

/* cash_div_flt8()
 * Divide cash by float8.
 */
pub unsafe fn cash_div_flt8(fcinfo: FunctionCallInfo) -> Datum {
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let f: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);

    return CashGetDatum(cash_div_float8(c, f)); // PG_RETURN_CASH
}

/* cash_mul_flt4()
 * Multiply cash by float4.
 */
pub unsafe fn cash_mul_flt4(fcinfo: FunctionCallInfo) -> Datum {
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let f: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    return CashGetDatum(cash_mul_float8(c, f as float8)); // PG_RETURN_CASH
}

/* flt4_mul_cash()
 * Multiply float4 by cash.
 */
pub unsafe fn flt4_mul_cash(fcinfo: FunctionCallInfo) -> Datum {
    let f: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    return CashGetDatum(cash_mul_float8(c, f as float8)); // PG_RETURN_CASH
}

/* cash_div_flt4()
 * Divide cash by float4.
 *
 */
pub unsafe fn cash_div_flt4(fcinfo: FunctionCallInfo) -> Datum {
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let f: float4 = PG_GETARG_FLOAT4!(fcinfo, 1);

    return CashGetDatum(cash_div_float8(c, f as float8)); // PG_RETURN_CASH
}

/* cash_mul_int8()
 * Multiply cash by int8.
 */
pub unsafe fn cash_mul_int8(fcinfo: FunctionCallInfo) -> Datum {
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let i: int64 = PG_GETARG_INT64!(fcinfo, 1);

    return CashGetDatum(cash_mul_int64(c, i)); // PG_RETURN_CASH
}

/* int8_mul_cash()
 * Multiply int8 by cash.
 */
pub unsafe fn int8_mul_cash(fcinfo: FunctionCallInfo) -> Datum {
    let i: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    return CashGetDatum(cash_mul_int64(c, i)); // PG_RETURN_CASH
}

/* cash_div_int8()
 * Divide cash by 8-byte integer.
 */
pub unsafe fn cash_div_int8(fcinfo: FunctionCallInfo) -> Datum {
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let i: int64 = PG_GETARG_INT64!(fcinfo, 1);

    return CashGetDatum(cash_div_int64(c, i)); // PG_RETURN_CASH
}

/* cash_mul_int4()
 * Multiply cash by int4.
 */
pub unsafe fn cash_mul_int4(fcinfo: FunctionCallInfo) -> Datum {
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let i: int32 = PG_GETARG_INT32!(fcinfo, 1);

    return CashGetDatum(cash_mul_int64(c, i as int64)); // PG_RETURN_CASH
}

/* int4_mul_cash()
 * Multiply int4 by cash.
 */
pub unsafe fn int4_mul_cash(fcinfo: FunctionCallInfo) -> Datum {
    let i: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    return CashGetDatum(cash_mul_int64(c, i as int64)); // PG_RETURN_CASH
}

/* cash_div_int4()
 * Divide cash by 4-byte integer.
 *
 */
pub unsafe fn cash_div_int4(fcinfo: FunctionCallInfo) -> Datum {
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let i: int32 = PG_GETARG_INT32!(fcinfo, 1);

    return CashGetDatum(cash_div_int64(c, i as int64)); // PG_RETURN_CASH
}

/* cash_mul_int2()
 * Multiply cash by int2.
 */
pub unsafe fn cash_mul_int2(fcinfo: FunctionCallInfo) -> Datum {
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let s: int16 = PG_GETARG_INT16!(fcinfo, 1);

    return CashGetDatum(cash_mul_int64(c, s as int64)); // PG_RETURN_CASH
}

/* int2_mul_cash()
 * Multiply int2 by cash.
 */
pub unsafe fn int2_mul_cash(fcinfo: FunctionCallInfo) -> Datum {
    let s: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));

    return CashGetDatum(cash_mul_int64(c, s as int64)); // PG_RETURN_CASH
}

/* cash_div_int2()
 * Divide cash by int2.
 *
 */
pub unsafe fn cash_div_int2(fcinfo: FunctionCallInfo) -> Datum {
    let c: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let s: int16 = PG_GETARG_INT16!(fcinfo, 1);

    return CashGetDatum(cash_div_int64(c, s as int64)); // PG_RETURN_CASH
}

/* cashlarger()
 * Return larger of two cash values.
 */
pub unsafe fn cashlarger(fcinfo: FunctionCallInfo) -> Datum {
    let c1: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let c2: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));
    let result: Cash;

    result = if c1 > c2 { c1 } else { c2 };

    return CashGetDatum(result); // PG_RETURN_CASH
}

/* cashsmaller()
 * Return smaller of two cash values.
 */
pub unsafe fn cashsmaller(fcinfo: FunctionCallInfo) -> Datum {
    let c1: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0));
    let c2: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 1));
    let result: Cash;

    result = if c1 < c2 { c1 } else { c2 };

    return CashGetDatum(result); // PG_RETURN_CASH
}

/* cash_words()
 * This converts an int4 as well but to a representation using words
 * Obviously way North American centric - sorry
 */
pub unsafe fn cash_words(fcinfo: FunctionCallInfo) -> Datum {
    let mut value: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_CASH
    let val: u64;
    let mut buf: StringInfoData = core::mem::zeroed();
    let res: *mut text;
    let dollars: Cash;
    let m0: Cash;
    let m1: Cash;
    let m2: Cash;
    let m3: Cash;
    let m4: Cash;
    let m5: Cash;
    let m6: Cash;

    initStringInfo(&mut buf);

    /* work with positive numbers */
    if value < 0 {
        value = -value;
        appendStringInfoString(&mut buf, b"minus \0".as_ptr() as *const c_char);
    }

    /* Now treat as unsigned, to avoid trouble at INT_MIN */
    val = value as u64;

    dollars = (val / 100) as Cash;
    m0 = (val % 100) as Cash; /* cents */
    m1 = ((val / 100) % 1000) as Cash; /* hundreds */
    m2 = ((val / 100000) % 1000) as Cash; /* thousands */
    m3 = ((val / 100000000) % 1000) as Cash; /* millions */
    m4 = ((val / 100000000000) % 1000) as Cash; /* billions */
    m5 = ((val / 100000000000000) % 1000) as Cash; /* trillions */
    m6 = ((val / 100000000000000000) % 1000) as Cash; /* quadrillions */

    if m6 != 0 {
        append_num_word(&mut buf, m6);
        appendStringInfoString(&mut buf, b" quadrillion \0".as_ptr() as *const c_char);
    }

    if m5 != 0 {
        append_num_word(&mut buf, m5);
        appendStringInfoString(&mut buf, b" trillion \0".as_ptr() as *const c_char);
    }

    if m4 != 0 {
        append_num_word(&mut buf, m4);
        appendStringInfoString(&mut buf, b" billion \0".as_ptr() as *const c_char);
    }

    if m3 != 0 {
        append_num_word(&mut buf, m3);
        appendStringInfoString(&mut buf, b" million \0".as_ptr() as *const c_char);
    }

    if m2 != 0 {
        append_num_word(&mut buf, m2);
        appendStringInfoString(&mut buf, b" thousand \0".as_ptr() as *const c_char);
    }

    if m1 != 0 {
        append_num_word(&mut buf, m1);
    }

    if dollars == 0 {
        appendStringInfoString(&mut buf, b"zero\0".as_ptr() as *const c_char);
    }

    appendStringInfoString(
        &mut buf,
        if dollars == 1 {
            b" dollar and \0".as_ptr() as *const c_char
        } else {
            b" dollars and \0".as_ptr() as *const c_char
        },
    );
    append_num_word(&mut buf, m0);
    appendStringInfoString(
        &mut buf,
        if m0 == 1 {
            b" cent\0".as_ptr() as *const c_char
        } else {
            b" cents\0".as_ptr() as *const c_char
        },
    );

    /* capitalize output */
    *buf.data = pg_toupper(*buf.data as u8) as c_char;

    /* return as text datum */
    res = cstring_to_text_with_len(buf.data, buf.len);
    pfree(buf.data as *mut c_void);
    PG_RETURN_TEXT_P!(res);
}

/* cash_numeric()
 * Convert cash to numeric.
 */
pub unsafe fn cash_numeric(fcinfo: FunctionCallInfo) -> Datum {
    let money: Cash = DatumGetCash(PG_GETARG_DATUM!(fcinfo, 0)); // PG_GETARG_CASH
    let mut result: Datum;
    let mut fpoint: c_int;
    let lconvert = &DEFAULT_LCONV; // PGLC_localeconv()

    /* see comments about frac_digits in cash_in() */
    fpoint = lconvert.frac_digits;
    if fpoint < 0 || fpoint > 10 {
        fpoint = 2;
    }

    /* convert the integral money value to numeric */
    result = NumericGetDatum(int64_to_numeric(money));

    /* scale appropriately, if needed */
    if fpoint > 0 {
        let mut scale: int64;
        let mut i: c_int;
        let mut numeric_scale: Datum;
        let quotient: Datum;

        /* compute required scale factor */
        scale = 1;
        i = 0;
        while i < fpoint {
            scale *= 10;
            i += 1;
        }
        numeric_scale = NumericGetDatum(int64_to_numeric(scale));

        /*
         * Given integral inputs approaching INT64_MAX, select_div_scale()
         * might choose a result scale of zero, causing loss of fractional
         * digits in the quotient.  We can ensure an exact result by setting
         * the dscale of either input to be at least as large as the desired
         * result scale.  numeric_round() will do that for us.
         */
        numeric_scale = DirectFunctionCall2Coll(
            numeric_round,
            InvalidOid,
            numeric_scale,
            Int32GetDatum(fpoint),
        );

        /* Now we can safely divide ... */
        quotient = DirectFunctionCall2Coll(numeric_div, InvalidOid, result, numeric_scale);

        /* ... and forcibly round to exactly the intended number of digits */
        result = DirectFunctionCall2Coll(
            numeric_round,
            InvalidOid,
            quotient,
            Int32GetDatum(fpoint),
        );
    }

    return result; // PG_RETURN_DATUM
}

/* numeric_cash()
 * Convert numeric to cash.
 */
pub unsafe fn numeric_cash(fcinfo: FunctionCallInfo) -> Datum {
    let mut amount: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let result: Cash;
    let mut fpoint: c_int;
    let mut scale: int64;
    let mut i: c_int;
    let numeric_scale: Datum;
    let lconvert = &DEFAULT_LCONV; // PGLC_localeconv()

    /* see comments about frac_digits in cash_in() */
    fpoint = lconvert.frac_digits;
    if fpoint < 0 || fpoint > 10 {
        fpoint = 2;
    }

    /* compute required scale factor */
    scale = 1;
    i = 0;
    while i < fpoint {
        scale *= 10;
        i += 1;
    }

    /* multiply the input amount by scale factor */
    numeric_scale = NumericGetDatum(int64_to_numeric(scale));
    amount = DirectFunctionCall2Coll(numeric_mul, InvalidOid, amount, numeric_scale);

    /* note that numeric_int8 will round to nearest integer for us */
    result = DatumGetInt64(DirectFunctionCall1Coll(numeric_int8, InvalidOid, amount));

    return CashGetDatum(result); // PG_RETURN_CASH
}

/* int4_cash()
 * Convert int4 (int) to cash
 */
pub unsafe fn int4_cash(fcinfo: FunctionCallInfo) -> Datum {
    let amount: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let result: Cash;
    let mut fpoint: c_int;
    let mut scale: int64;
    let mut i: c_int;
    let lconvert = &DEFAULT_LCONV; // PGLC_localeconv()

    /* see comments about frac_digits in cash_in() */
    fpoint = lconvert.frac_digits;
    if fpoint < 0 || fpoint > 10 {
        fpoint = 2;
    }

    /* compute required scale factor */
    scale = 1;
    i = 0;
    while i < fpoint {
        scale *= 10;
        i += 1;
    }

    /* compute amount * scale, checking for overflow */
    result = DatumGetInt64(DirectFunctionCall2Coll(
        int8mul,
        InvalidOid,
        Int64GetDatum(amount as int64),
        Int64GetDatum(scale),
    ));

    return CashGetDatum(result); // PG_RETURN_CASH
}

/* int8_cash()
 * Convert int8 (bigint) to cash
 */
pub unsafe fn int8_cash(fcinfo: FunctionCallInfo) -> Datum {
    let amount: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let result: Cash;
    let mut fpoint: c_int;
    let mut scale: int64;
    let mut i: c_int;
    let lconvert = &DEFAULT_LCONV; // PGLC_localeconv()

    /* see comments about frac_digits in cash_in() */
    fpoint = lconvert.frac_digits;
    if fpoint < 0 || fpoint > 10 {
        fpoint = 2;
    }

    /* compute required scale factor */
    scale = 1;
    i = 0;
    while i < fpoint {
        scale *= 10;
        i += 1;
    }

    /* compute amount * scale, checking for overflow */
    result = DatumGetInt64(DirectFunctionCall2Coll(
        int8mul,
        InvalidOid,
        Int64GetDatum(amount),
        Int64GetDatum(scale),
    ));

    return CashGetDatum(result); // PG_RETURN_CASH
}

// ===== libc bindings + small helpers (mirroring uuid.rs/numutils.rs style) =====

extern "C" {
    fn isspace(ch: c_int) -> c_int;
    fn isdigit(ch: c_int) -> c_int;
    fn strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int;
}

/* strlen over a NUL-terminated C string. */
#[inline]
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/* Length of a NUL-terminated &[u8] literal as a slice without the trailing NUL. */
#[inline]
fn nul_term_slice(b: &'static [u8]) -> &'static [u8] {
    let mut n = 0usize;
    while n < b.len() && b[n] != 0 {
        n += 1;
    }
    &b[..n]
}

/* &[u8] -> &str for the C-locale ASCII symbols used in formatting. */
#[inline]
fn bstr(b: &[u8]) -> std::borrow::Cow<'_, str> {
    std::string::String::from_utf8_lossy(b)
}

/* Append a non-NUL-terminated byte slice (ASCII word) to a StringInfo. */
#[inline]
unsafe fn append_str(buf: StringInfo, b: &[u8]) {
    crate::lib::stringinfo::appendBinaryStringInfo(buf, b.as_ptr() as *const c_void, b.len() as c_int);
}

/* &[u8] word -> &str (the small[]/big[] entries are pure ASCII). */
#[inline]
fn as_str(b: &[u8]) -> &str {
    // SAFETY: all entries in SMALL are ASCII literals.
    std::str::from_utf8(b).unwrap()
}

/* psprintf replacement: palloc a NUL-terminated copy of a Rust string. */
#[inline]
unsafe fn cstring_dup(s: &str) -> *mut c_char {
    let n = s.len();
    let p = palloc(n + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(s.as_ptr(), p as *mut u8, n);
    *p.add(n) = 0;
    p
}

/* numeric.h NumericGetDatum (the numeric.rs copy is private). */
#[inline]
unsafe fn NumericGetDatum(x: Numeric) -> Datum {
    PointerGetDatum(x as *const c_void)
}

/* Format a C string for an error message via Rust `{}` (lossy). */
unsafe fn cstr(s: *const c_char) -> std::string::String {
    let n = strlen(s);
    let bytes = core::slice::from_raw_parts(s as *const u8, n);
    std::string::String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{
        CStringGetDatum, DatumGetBool, DatumGetCString, DatumGetFloat8, DatumGetInt32, Float8GetDatum,
    };
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};

    unsafe fn cstr_eq(p: *mut c_char, want: &str) -> bool {
        let n = strlen(p);
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    unsafe fn text_eq(d: Datum, want: &str) -> bool {
        let t = DatumGetPointer(d) as *const c_char;
        let p = VARDATA(t);
        let n = (VARSIZE(t) as usize) - VARHDRSZ as usize;
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn cash_io_arith_words() {
        unsafe {
            // cash_in: "$123,456.78" -> 12345678 (scaled by frac_digits=2)
            let d = DirectFunctionCall1Coll(
                cash_in,
                InvalidOid,
                CStringGetDatum(c"$123,456.78".as_ptr()),
            );
            assert_eq!(DatumGetCash(d), 12345678);

            // cash_out round trip -> "$123,456.78"
            let s = DatumGetCString(DirectFunctionCall1Coll(cash_out, InvalidOid, d));
            assert!(cstr_eq(s, "$123,456.78"));

            // negative + parser leniency: "($1.50)" parsed as -150
            let neg = DirectFunctionCall1Coll(cash_in, InvalidOid, CStringGetDatum(c"($1.50)".as_ptr()));
            assert_eq!(DatumGetCash(neg), -150);
            let sn = DatumGetCString(DirectFunctionCall1Coll(cash_out, InvalidOid, neg));
            assert!(cstr_eq(sn, "-$1.50"));

            // rounding: extra fractional digit >= 5 rounds away from zero
            let r = DirectFunctionCall1Coll(cash_in, InvalidOid, CStringGetDatum(c"1.005".as_ptr()));
            assert_eq!(DatumGetCash(r), 101); // 1.00 then round up -> 101

            // comparisons + cmp
            assert!(DatumGetBool(DirectFunctionCall2Coll(cash_lt, InvalidOid, neg, d)));
            assert_eq!(DatumGetInt32(DirectFunctionCall2Coll(cash_cmp, InvalidOid, d, neg)), 1);
            assert!(DatumGetBool(DirectFunctionCall2Coll(cash_eq, InvalidOid, d, d)));

            // pl / mi
            let a = CashGetDatum(100);
            let b = CashGetDatum(25);
            assert_eq!(DatumGetCash(DirectFunctionCall2Coll(cash_pl, InvalidOid, a, b)), 125);
            assert_eq!(DatumGetCash(DirectFunctionCall2Coll(cash_mi, InvalidOid, a, b)), 75);

            // mul/div by scalars (rint rounding)
            assert_eq!(
                DatumGetCash(DirectFunctionCall2Coll(cash_mul_flt8, InvalidOid, CashGetDatum(100), Float8GetDatum(2.5))),
                250
            );
            assert_eq!(
                DatumGetCash(DirectFunctionCall2Coll(cash_div_int4, InvalidOid, CashGetDatum(100), Int32GetDatum(4))),
                25
            );
            assert_eq!(
                DatumGetCash(DirectFunctionCall2Coll(int2_mul_cash, InvalidOid, crate::postgres::Int16GetDatum(3), CashGetDatum(100))),
                300
            );

            // cash_div_cash -> float8
            let q = DatumGetFloat8(DirectFunctionCall2Coll(cash_div_cash, InvalidOid, CashGetDatum(1000), CashGetDatum(250)));
            assert!((q - 4.0).abs() < 1e-9);

            // int8_cash / int4_cash: scale by 100
            assert_eq!(DatumGetCash(DirectFunctionCall1Coll(int8_cash, InvalidOid, Int64GetDatum(5))), 500);
            assert_eq!(DatumGetCash(DirectFunctionCall1Coll(int4_cash, InvalidOid, Int32GetDatum(7))), 700);

            // cash_words: 1.05 -> "One dollar and five cents"
            let w = DirectFunctionCall1Coll(cash_words, InvalidOid, CashGetDatum(105));
            assert!(text_eq(w, "One dollar and five cents"));
            // zero dollars, multiple cents
            let w2 = DirectFunctionCall1Coll(cash_words, InvalidOid, CashGetDatum(23));
            assert!(text_eq(w2, "Zero dollars and twenty three cents"));
        }
    }

    #[test]
    #[should_panic]
    fn cash_div_int4_by_zero_errors() {
        unsafe {
            DirectFunctionCall2Coll(cash_div_int4, InvalidOid, CashGetDatum(100), Int32GetDatum(0));
        }
    }
}
