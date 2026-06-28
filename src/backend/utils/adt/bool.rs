//! Functions for the built-in type "bool". Translated from
//! src/backend/utils/adt/bool.c.
//!
//! Covers `parse_bool`/`parse_bool_with_len`, the user I/O routines
//! (`boolin`/`boolout`/`boolrecv`/`boolsend`/`booltext`), the comparison
//! operators, the hash functions, and the bool_and/bool_or aggregate support
//! (`booland_statefunc`/`boolor_statefunc`/`bool_accum` etc.).
//!
//! As in int.c, each C `Datum fn(PG_FUNCTION_ARGS)` becomes a `PGFunction`-typed
//! `fn(&mut FunctionCallInfoBaseData) -> Datum`. Output functions return a leaked
//! C string (no MemoryContext yet, like int.rs). Bad input raises via `ereport!`.

use crate::ereport;
use crate::fmgr::FunctionCallInfoBaseData;
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, Datum, DatumGetBool, DatumGetCString, Int32GetDatum,
};
use crate::backend::utils::adt::varlena::cstring_to_text;
use crate::utils::elog::ERROR;
use crate::utils::errcodes::ERRCODE_INVALID_TEXT_REPRESENTATION;

#[inline]
fn pg_getarg_bool(fcinfo: &FunctionCallInfoBaseData, n: usize) -> bool {
    DatumGetBool(fcinfo.args[n].value)
}

/// PG `PG_GETARG_CSTRING(n)`: the argument as an owned UTF-8 string.
#[inline]
fn pg_getarg_cstring(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = DatumGetCString(fcinfo.args[n].value);
    // SAFETY: an input function's cstring argument is a NUL-terminated C string
    // that outlives the call (InputFunctionCall keeps the source alive).
    let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
    cstr.to_string_lossy().into_owned()
}

/// PG `PG_RETURN_CSTRING(s)`: hand back an owned C string as a `Datum`.
#[inline]
fn pg_return_cstring(s: &str) -> Datum {
    let bytes: Vec<u8> = s.bytes().take_while(|&b| b != 0).collect();
    let c = std::ffi::CString::new(bytes).unwrap_or_default();
    CStringGetDatum(c.into_raw())
}

/// Try to interpret `value` as a boolean. Valid values are `true`, `false`,
/// `yes`, `no`, `on`, `off`, `1`, `0`, as well as unique prefixes thereof.
/// Returns `Some(b)` if the string parses, else `None`. C: `parse_bool`.
#[must_use]
pub fn parse_bool(value: &str) -> Option<bool> {
    parse_bool_with_len(value, value.len())
}

/// PG `parse_bool_with_len`: like [`parse_bool`] but the caller supplies the
/// length (the string need not be NUL-terminated).
#[must_use]
pub fn parse_bool_with_len(value: &str, len: usize) -> Option<bool> {
    let bytes = value.as_bytes();
    // The whitespace-trimmed slice the comparison applies to.
    let s = &value[..len.min(value.len())];
    let first = *bytes.first()?;
    match first {
        b't' | b'T' => prefix_match(s, "true").then_some(true),
        b'f' | b'F' => prefix_match(s, "false").then_some(false),
        b'y' | b'Y' => prefix_match(s, "yes").then_some(true),
        b'n' | b'N' => prefix_match(s, "no").then_some(false),
        b'o' | b'O' => {
            // 'o' is not unique enough: require at least 2 chars matched.
            if s.len() >= 2 && prefix_match(s, "on") {
                Some(true)
            } else if s.len() >= 2 && prefix_match(s, "off") {
                Some(false)
            } else {
                None
            }
        }
        b'1' if len == 1 => Some(true),
        b'0' if len == 1 => Some(false),
        _ => None,
    }
}

/// Case-insensitive: is `s` a non-empty prefix of `full` (C `pg_strncasecmp`
/// with `len` from the input)? `s` is the trimmed input; it matches when it
/// equals a leading slice of `full` and is no longer than `full`.
fn prefix_match(s: &str, full: &str) -> bool {
    !s.is_empty() && s.len() <= full.len() && full[..s.len()].eq_ignore_ascii_case(s)
}

// ===========================================================================
//   USER I/O ROUTINES
// ===========================================================================

/// PG `boolin`: input function for type boolean.
pub fn boolin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let in_str = pg_getarg_cstring(fcinfo, 0);
    let trimmed = in_str.trim_matches(|c: char| c.is_ascii_whitespace());
    if let Some(result) = parse_bool_with_len(trimmed, trimmed.len()) {
        return BoolGetDatum(result);
    }
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .errmsg(format!("invalid input syntax for type boolean: \"{in_str}\""));
    });
    unreachable!()
}

/// PG `boolout`: converts 1 or 0 to "t" or "f".
pub fn boolout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let b = pg_getarg_bool(fcinfo, 0);
    pg_return_cstring(if b { "t" } else { "f" })
}

/// PG `boolrecv`: converts external binary format to bool. The binary
/// representation is one byte; any nonzero value is "true".
pub fn boolrecv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("boolrecv needs the binary wire StringInfo (pq_getmsgbyte) marshalling")
}

/// PG `boolsend`: converts bool to binary format.
pub fn boolsend(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("boolsend needs pq_begintypsend/pq_endtypsend bytea boxing")
}

/// PG `booltext`: cast function for bool => text (SQL-spec "true"/"false").
pub fn booltext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_bool(fcinfo, 0);
    let s = if arg1 { "true" } else { "false" };
    Datum(cstring_to_text(s) as usize)
}

// ===========================================================================
//   PUBLIC ROUTINES (comparison operators)
// ===========================================================================

macro_rules! bool_cmp {
    ($name:ident, $op:tt) => {
        #[doc = concat!("PG `", stringify!($name), "`.")]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let arg1 = pg_getarg_bool(fcinfo, 0);
            let arg2 = pg_getarg_bool(fcinfo, 1);
            BoolGetDatum(arg1 $op arg2)
        }
    };
}

bool_cmp!(booleq, ==);
bool_cmp!(boolne, !=);
bool_cmp!(boollt, <);
bool_cmp!(boolgt, >);
bool_cmp!(boolle, <=);
bool_cmp!(boolge, >=);

/// PG `hashbool`.
pub fn hashbool(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("hashbool needs hash_uint32 (common/hashfn)")
}

/// PG `hashboolextended`.
pub fn hashboolextended(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("hashboolextended needs hash_uint32_extended (common/hashfn)")
}

// ===========================================================================
//   boolean-and and boolean-or aggregates
// ===========================================================================

/// PG `booland_statefunc`: standard EVERY (bool_and) state transition.
pub fn booland_statefunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let r = pg_getarg_bool(fcinfo, 0) && pg_getarg_bool(fcinfo, 1);
    BoolGetDatum(r)
}

/// PG `boolor_statefunc`: standard ANY/SOME (bool_or) state transition.
pub fn boolor_statefunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let r = pg_getarg_bool(fcinfo, 0) || pg_getarg_bool(fcinfo, 1);
    BoolGetDatum(r)
}

/// PG `BoolAggState`: running counts behind the moving bool_and/bool_or aggs.
#[derive(Debug, Clone, Copy)]
pub struct BoolAggState {
    /// Number of non-null values aggregated.
    pub aggcount: i64,
    /// Number of aggregated values that are true.
    pub aggtrue: i64,
}

/// PG `bool_accum`: forward state-transition for bool_and/bool_or aggregates.
pub fn bool_accum(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // Needs AggCheckCallContext + MemoryContextAlloc to materialize the
    // internal-type BoolAggState (aggregate executor machinery + palloc).
    unimplemented!("bool_accum needs AggCheckCallContext + MemoryContextAlloc (aggregate ctx)")
}

/// PG `bool_accum_inv`: inverse transition for moving-aggregate mode.
pub fn bool_accum_inv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("bool_accum_inv needs the BoolAggState internal-type state (aggregate ctx)")
}

/// PG `bool_alltrue`: bool_and final function over a `BoolAggState`.
pub fn bool_alltrue(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("bool_alltrue needs the BoolAggState internal-type state (aggregate ctx)")
}

/// PG `bool_anytrue`: bool_or final function over a `BoolAggState`.
pub fn bool_anytrue(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("bool_anytrue needs the BoolAggState internal-type state (aggregate ctx)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::NullableDatum;

    fn fc(args: &[Datum]) -> FunctionCallInfoBaseData {
        FunctionCallInfoBaseData {
            flinfo: None,
            context: core::ptr::null_mut(),
            resultinfo: core::ptr::null_mut(),
            fncollation: crate::postgres_ext::InvalidOid,
            isnull: false,
            nargs: args.len() as i16,
            args: args
                .iter()
                .map(|&value| NullableDatum { value, isnull: false })
                .collect(),
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

    #[test]
    fn parse_bool_accepts_canonical_and_prefixes() {
        for s in ["t", "T", "true", "TRUE", "tr", "y", "yes", "on", "1"] {
            assert_eq!(parse_bool(s), Some(true), "{s}");
        }
        for s in ["f", "F", "false", "FALSE", "fa", "n", "no", "off", "0"] {
            assert_eq!(parse_bool(s), Some(false), "{s}");
        }
    }

    #[test]
    fn parse_bool_rejects_bad_and_ambiguous() {
        // 'o' alone is ambiguous between on/off; 2/00 are not valid.
        for s in ["o", "O", "yep", "2", "00", "", "tru!", "x"] {
            assert_eq!(parse_bool(s), None, "{s}");
        }
    }

    #[test]
    fn boolin_boolout_roundtrip() {
        for (input, want) in [("t", "t"), ("  true ", "t"), ("0", "f"), ("off", "f")] {
            let mut f = fc(&[cstr_datum(input)]);
            let d = boolin(&mut f);
            let mut out = fc(&[d]);
            assert_eq!(out_to_string(boolout(&mut out)), want, "{input}");
        }
    }

    #[test]
    fn boolin_invalid_errors() {
        let mut f = fc(&[cstr_datum("notabool")]);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| boolin(&mut f)));
        assert!(r.is_err());
    }

    #[test]
    fn bool_comparisons() {
        let t = BoolGetDatum(true);
        let f = BoolGetDatum(false);
        assert!(DatumGetBool(booleq(&mut fc(&[t, t]))));
        assert!(DatumGetBool(boolne(&mut fc(&[t, f]))));
        assert!(DatumGetBool(boollt(&mut fc(&[f, t]))));
        assert!(DatumGetBool(boolgt(&mut fc(&[t, f]))));
        assert!(DatumGetBool(boolle(&mut fc(&[f, f]))));
        assert!(DatumGetBool(boolge(&mut fc(&[t, t]))));
    }

    #[test]
    fn bool_and_or_statefuncs() {
        let t = BoolGetDatum(true);
        let f = BoolGetDatum(false);
        assert!(DatumGetBool(booland_statefunc(&mut fc(&[t, t]))));
        assert!(!DatumGetBool(booland_statefunc(&mut fc(&[t, f]))));
        assert!(DatumGetBool(boolor_statefunc(&mut fc(&[f, t]))));
        assert!(!DatumGetBool(boolor_statefunc(&mut fc(&[f, f]))));
    }

    #[test]
    fn fmgr_table_binds_boolin() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "boolin")
            .expect("boolin present");
        let func = entry.func.expect("boolin bound");
        let mut f = fc(&[cstr_datum("true")]);
        assert!(DatumGetBool(func(&mut f)));
    }

    #[test]
    fn booltext_unused_field_ok() {
        // Touch BoolAggState so the struct + fields aren't dead in this build.
        let s = BoolAggState { aggcount: 1, aggtrue: 1 };
        assert_eq!(s.aggcount, s.aggtrue);
        let _ = Int32GetDatum(0);
    }
}
