//! Functions for the built-in type `"char"` (a single byte; not to be confused
//! with bpchar, the SQL `CHAR(n)` type). Translated from
//! src/backend/utils/adt/char.c.
//!
//! Covers the user I/O routines (in/out/recv/send), every comparison operator
//! (eq/ne/lt/le/gt/ge), the "char"<->int4 casts (chartoi4/i4tochar), and the
//! "char"<->text casts (char_text/text_char).
//!
//! NOTE (from char.c): comparisons are done as though `char` is unsigned
//! (uint8); conversions to/from integer are done as though `char` is signed
//! (int8). We reproduce that asymmetry faithfully.
//!
//! recv/send reach the binary wire `MsgReader`/`StringInfo`, which is not yet
//! translated; they call those stubs (rules.md s4). The core in/out/cmp/cast
//! paths are complete.

use crate::c::{text, IS_HIGHBIT_SET};
use crate::ereport;
use crate::fmgr::FunctionCallInfoBaseData;
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, CharGetDatum, Datum, DatumGetCString, DatumGetChar,
    DatumGetInt32, Int32GetDatum, PointerGetDatum,
};
use crate::utils::elog::ERROR;
use crate::utils::errcodes::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;

const SCHAR_MIN: i32 = -128;
const SCHAR_MAX: i32 = 127;

/// ISOCTAL(c): true if `c` is an octal digit.
#[inline]
const fn is_octal(c: u8) -> bool {
    c.is_ascii_digit() && c <= b'7'
}
/// TOOCTAL(c): octal digit value to ASCII byte.
#[inline]
const fn to_octal(c: u8) -> u8 {
    c + b'0'
}
/// FROMOCTAL(c): ASCII octal digit to its numeric value.
#[inline]
const fn from_octal(c: u8) -> u8 {
    c - b'0'
}

// ---------------------------------------------------------------------------
// PG_GETARG_* / PG_RETURN_* accessors (see int.rs for the contract).
// ---------------------------------------------------------------------------

#[inline]
fn pg_getarg_char(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i8 {
    DatumGetChar(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_int32(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i32 {
    DatumGetInt32(fcinfo.args[n].value)
}

#[inline]
fn pg_getarg_cstring(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Vec<u8> {
    let p = DatumGetCString(fcinfo.args[n].value);
    // SAFETY: an input function's cstring argument is a NUL-terminated C string
    // that outlives the call.
    let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
    cstr.to_bytes().to_vec()
}

/// PG_RETURN_CSTRING for a byte buffer (the value may be a raw high byte).
#[inline]
fn pg_return_cstring_bytes(bytes: &[u8]) -> Datum {
    let c = std::ffi::CString::new(bytes).unwrap_or_default();
    CStringGetDatum(c.into_raw())
}

// ===========================================================================
//   USER I/O ROUTINES
// ===========================================================================

/// PG `charin`: converts "x" to 'x'.
///
/// Accepts the formats charout produces. Multibyte input not in `\ooo` form
/// takes its first byte (a backwards-compatibility provision). The zero-length
/// input yields '\0'.
pub fn charin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ch = pg_getarg_cstring(fcinfo, 0);
    if ch.len() == 4
        && ch[0] == b'\\'
        && is_octal(ch[1])
        && is_octal(ch[2])
        && is_octal(ch[3])
    {
        let v = (from_octal(ch[1]) << 6)
            .wrapping_add(from_octal(ch[2]) << 3)
            .wrapping_add(from_octal(ch[3]));
        return CharGetDatum(v as i8);
    }
    // Right thing for a zero-length input string.
    CharGetDatum(ch.first().copied().unwrap_or(0) as i8)
}

/// PG `charout`: converts 'x' to "x".
///
/// Output formats: 0x00 -> empty string; 0x01..0x7F -> single ASCII byte;
/// 0x80..0xFF -> `\ooo` (backslash and 3 octal digits, matching bytea escape).
pub fn charout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ch = pg_getarg_char(fcinfo, 0) as u8;
    if IS_HIGHBIT_SET(ch) {
        let out = [
            b'\\',
            to_octal(ch >> 6),
            to_octal((ch >> 3) & 0o7),
            to_octal(ch & 0o7),
        ];
        pg_return_cstring_bytes(&out)
    } else {
        // Acceptable for 0x00 as well: an empty cstring.
        let one = [ch];
        pg_return_cstring_bytes(if ch == 0 { &[] } else { &one[..] })
    }
}

/// PG `charrecv`: converts external binary format to char (one raw byte).
pub fn charrecv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("charrecv needs the binary wire StringInfo (pq_getmsgbyte) path")
}

/// PG `charsend`: converts char to binary format.
pub fn charsend(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("charsend needs pq_begintypsend/pq_endtypsend bytea boxing")
}

// ===========================================================================
//   PUBLIC ROUTINES
// ===========================================================================

/// PG `chareq`.
pub fn chareq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_char(fcinfo, 0) == pg_getarg_char(fcinfo, 1))
}
/// PG `charne`.
pub fn charne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_char(fcinfo, 0) != pg_getarg_char(fcinfo, 1))
}
/// PG `charlt`: compares as unsigned (uint8).
pub fn charlt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum((pg_getarg_char(fcinfo, 0) as u8) < (pg_getarg_char(fcinfo, 1) as u8))
}
/// PG `charle`: compares as unsigned (uint8).
pub fn charle(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum((pg_getarg_char(fcinfo, 0) as u8) <= (pg_getarg_char(fcinfo, 1) as u8))
}
/// PG `chargt`: compares as unsigned (uint8).
pub fn chargt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum((pg_getarg_char(fcinfo, 0) as u8) > (pg_getarg_char(fcinfo, 1) as u8))
}
/// PG `charge`: compares as unsigned (uint8).
pub fn charge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum((pg_getarg_char(fcinfo, 0) as u8) >= (pg_getarg_char(fcinfo, 1) as u8))
}

/// PG `chartoi4`: cast "char" -> int4 (as signed int8).
pub fn chartoi4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(i32::from(pg_getarg_char(fcinfo, 0)))
}

/// PG `i4tochar`: cast int4 -> "char" (as signed int8), raising out of range.
pub fn i4tochar(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_int32(fcinfo, 0);
    if !(SCHAR_MIN..=SCHAR_MAX).contains(&arg1) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                .errmsg("\"char\" out of range");
        });
    }
    CharGetDatum(arg1 as i8)
}

/// PG `text_char`: cast text -> "char".
///
/// Conversion rules match `charin`, but the empty-string case yields '\0'.
pub fn text_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = fcinfo.args[0].value.0 as *mut u8;
    // SAFETY: arg is a valid non-toasted text varlena alive for the call.
    let ch = unsafe { varlena_bytes(p) };
    let result: u8 = if ch.len() == 4
        && ch[0] == b'\\'
        && is_octal(ch[1])
        && is_octal(ch[2])
        && is_octal(ch[3])
    {
        (from_octal(ch[1]) << 6)
            .wrapping_add(from_octal(ch[2]) << 3)
            .wrapping_add(from_octal(ch[3]))
    } else if !ch.is_empty() {
        ch[0]
    } else {
        b'\0'
    };
    CharGetDatum(result as i8)
}

/// PG `char_text`: cast "char" -> text.
///
/// Conversion rules match `charout`, being honest about 0x00 -> empty string.
pub fn char_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_char(fcinfo, 0) as u8;
    let bytes: Vec<u8> = if IS_HIGHBIT_SET(arg1) {
        vec![
            b'\\',
            to_octal(arg1 >> 6),
            to_octal((arg1 >> 3) & 0o7),
            to_octal(arg1 & 0o7),
        ]
    } else if arg1 != 0 {
        vec![arg1]
    } else {
        Vec::new()
    };
    let t = crate::backend::utils::adt::varlena::cstring_to_text_with_len(
        &String::from_utf8_lossy(&bytes),
        bytes.len() as i32,
    );
    PointerGetDatum(t.cast::<u8>())
}

/// Borrow a non-toasted varlena's payload bytes (see varlena.rs).
///
/// SAFETY: `p` must point to a valid 4-byte-or-short-header varlena that
/// outlives the borrow.
unsafe fn varlena_bytes<'a>(p: *mut u8) -> &'a [u8] {
    let len = crate::varatt::VARSIZE_ANY_EXHDR(p);
    core::slice::from_raw_parts(crate::varatt::VARDATA_ANY(p), len)
}

const _: fn() = || {
    // `text` is the documented payload type of char_text/text_char.
    let _ = core::mem::size_of::<text>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{DatumGetBool, NullableDatum};
    use std::panic::catch_unwind;

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

    fn cstr_datum_bytes(bytes: &[u8]) -> Datum {
        let c = std::ffi::CString::new(bytes).unwrap();
        CStringGetDatum(c.into_raw())
    }

    fn out_to_bytes(d: Datum) -> Vec<u8> {
        let p = DatumGetCString(d);
        let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
        cstr.to_bytes().to_vec()
    }

    #[test]
    fn char_in_out_single_byte() {
        // Ordinary ASCII roundtrips as a single byte.
        let mut f = fc(&[cstr_datum_bytes(b"A")]);
        let d = charin(&mut f);
        assert_eq!(DatumGetChar(d), b'A' as i8);
        let mut f = fc(&[d]);
        assert_eq!(out_to_bytes(charout(&mut f)), b"A");
    }

    #[test]
    fn char_in_out_zero_byte() {
        // 0x00 charin from empty string; charout of 0 is the empty string.
        let mut f = fc(&[cstr_datum_bytes(b"")]);
        let d = charin(&mut f);
        assert_eq!(DatumGetChar(d), 0);
        let mut f = fc(&[CharGetDatum(0)]);
        assert_eq!(out_to_bytes(charout(&mut f)), b"");
    }

    #[test]
    fn char_in_out_high_byte_octal() {
        // 0x80..0xFF: charout produces \ooo; charin parses it back.
        for v in [0x80u8, 0xFF, 0xA5, 0xC0] {
            let mut f = fc(&[CharGetDatum(v as i8)]);
            let printed = out_to_bytes(charout(&mut f));
            assert_eq!(printed[0], b'\\');
            assert_eq!(printed.len(), 4);
            let mut f = fc(&[cstr_datum_bytes(&printed)]);
            assert_eq!(DatumGetChar(charin(&mut f)) as u8, v, "roundtrip {v:#x}");
        }
    }

    #[test]
    fn char_to_int4_and_back() {
        // signed conversion: 0xFF as "char" is -1 as int4.
        let mut f = fc(&[CharGetDatum(-1)]);
        assert_eq!(DatumGetInt32(chartoi4(&mut f)), -1);
        let mut f = fc(&[Int32GetDatum(-1)]);
        assert_eq!(DatumGetChar(i4tochar(&mut f)), -1);
        let mut f = fc(&[Int32GetDatum(127)]);
        assert_eq!(DatumGetChar(i4tochar(&mut f)), 127);
        // out of [-128,127] raises.
        assert!(catch_unwind(|| {
            let mut f = fc(&[Int32GetDatum(128)]);
            i4tochar(&mut f)
        })
        .is_err());
        assert!(catch_unwind(|| {
            let mut f = fc(&[Int32GetDatum(-129)]);
            i4tochar(&mut f)
        })
        .is_err());
    }

    #[test]
    fn char_comparisons_unsigned() {
        // 0x80 (-128 signed) compares GREATER than 0x01 because cmp is unsigned.
        let mut f = fc(&[CharGetDatum(-128), CharGetDatum(1)]);
        assert!(DatumGetBool(chargt(&mut f)));
        let mut f = fc(&[CharGetDatum(1), CharGetDatum(1)]);
        assert!(DatumGetBool(chareq(&mut f)));
        let mut f = fc(&[CharGetDatum(1), CharGetDatum(2)]);
        assert!(DatumGetBool(charlt(&mut f)));
        assert!(DatumGetBool(charne(&mut f)));
    }

    /// charout resolves through the generated fmgr table to a bound function.
    #[test]
    fn fmgr_table_binds_charout() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "charout")
            .expect("charout present");
        let func = entry.func.expect("charout bound");
        let mut f = fc(&[CharGetDatum(b'Z' as i8)]);
        assert_eq!(out_to_bytes(func(&mut f)), b"Z");
    }
}
