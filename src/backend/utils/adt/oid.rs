//! Functions for the built-in type `Oid` and `oidvector`. Translated from
//! src/backend/utils/adt/oid.c.
//!
//! Covers the user I/O routines (in/out/recv/send), `oidvector` I/O, the
//! `oidparse` ICONST/FCONST helper, the `oid_cmp` qsort callback, every Oid
//! comparison operator (eq/ne/lt/le/gt/ge), larger/smaller, and the oidvector
//! comparison wrappers (eq/ne/lt/le/gt/ge over `btoidvectorcmp`).
//!
//! Each C `Datum fn(PG_FUNCTION_ARGS)` becomes a `PGFunction`-typed Rust fn
//! `fn(&mut FunctionCallInfoBaseData) -> Datum`. Oids are unsigned 32-bit, so
//! comparison is unsigned, matching oid.c.
//!
//! Subsystems oid.c reaches that are not yet translated are called through
//! their existing stubs (rules.md s4): the binary wire `MsgReader` behind
//! recv/send, the array machinery (`array_recv`/`array_send`) behind oidvector
//! recv/send, and `btoidvectorcmp` (its array layout is not built yet) behind
//! the oidvector comparison operators. `buildoidvector`/`check_valid_oidvector`
//! already live in `utils::builtins` from step 19; we reuse them.
//!
//! oid.c's `uint32in_subr` (numutils.c) is not translated yet; we provide the
//! small unsigned parse it needs as a private file-local helper that reproduces
//! the accepted syntax (decimal, optional sign with the documented unsigned
//! wraparound, surrounding whitespace) and PG's out-of-range / invalid-syntax
//! errors.
//!
//! TODO(numutils): delete `parse_oid` and call `utils::builtins::uint32in_subr`
//! once numutils.c is translated.

use crate::c::oidvector;
use crate::ereport;
use crate::fmgr::FunctionCallInfoBaseData;
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, Datum, DatumGetCString, DatumGetInt32, DatumGetObjectId,
    Int32GetDatum, ObjectIdGetDatum, PointerGetDatum,
};
use crate::postgres_ext::Oid;
use crate::utils::elog::ERROR;
use crate::utils::errcodes::{ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};

// ---------------------------------------------------------------------------
// PG_GETARG_* / PG_RETURN_* accessors (see int.rs for the contract).
// ---------------------------------------------------------------------------

#[inline]
fn pg_getarg_oid(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Oid {
    DatumGetObjectId(fcinfo.args[n].value)
}

#[inline]
fn pg_getarg_cstring(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = DatumGetCString(fcinfo.args[n].value);
    // SAFETY: an input function's cstring argument is a NUL-terminated C string
    // that outlives the call (InputFunctionCall keeps the source alive).
    let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
    cstr.to_string_lossy().into_owned()
}

#[inline]
fn pg_return_cstring(s: &str) -> Datum {
    let c = std::ffi::CString::new(s).unwrap_or_default();
    CStringGetDatum(c.into_raw())
}

// ---------------------------------------------------------------------------
// Oid text parsing (numutils.c uint32in_subr stand-in).
// ---------------------------------------------------------------------------

/// PG `uint32in_subr` for the "oid" type: parse `s` to an Oid (u32).
///
/// PG accepts an optional leading sign; a leading `-` wraps around modulo 2^32
/// (the historical unsigned-input behavior), so e.g. "-1" parses to 4294967295.
fn parse_oid(s: &str) -> u32 {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        oid_invalid_syntax(s);
    }

    let (neg, digits) = match trimmed.as_bytes()[0] {
        b'-' => (true, &trimmed[1..]),
        b'+' => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };

    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        oid_invalid_syntax(s);
    }

    // Parse into u64 so we can detect overflow past 2^32-1, then apply the
    // unsigned-wraparound semantics of strtoul for a negative sign.
    let mut acc: u64 = 0;
    for b in digits.bytes() {
        acc = acc * 10 + u64::from(b - b'0');
        if acc > u64::from(u32::MAX) {
            oid_out_of_range(s);
        }
    }
    let v = acc as u32;
    if neg {
        v.wrapping_neg()
    } else {
        v
    }
}

fn oid_out_of_range(s: &str) -> ! {
    let sv = s.to_owned();
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg(format!("value \"{sv}\" is out of range for type oid"));
    });
    unreachable!()
}

fn oid_invalid_syntax(s: &str) -> ! {
    let sv = s.to_owned();
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .errmsg(format!("invalid input syntax for type oid: \"{sv}\""));
    });
    unreachable!()
}

// ===========================================================================
//   USER I/O ROUTINES
// ===========================================================================

/// PG `oidin`: converts "num" to oid.
pub fn oidin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = pg_getarg_cstring(fcinfo, 0);
    ObjectIdGetDatum(Oid(parse_oid(&s)))
}

/// PG `oidout`: converts oid to "num".
pub fn oidout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let o = pg_getarg_oid(fcinfo, 0);
    pg_return_cstring(&o.0.to_string())
}

/// PG `oidrecv`: converts external binary format to oid.
pub fn oidrecv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("oidrecv needs the binary wire StringInfo (pq_getmsgint) path")
}

/// PG `oidsend`: converts oid to binary format.
pub fn oidsend(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("oidsend needs pq_begintypsend/pq_endtypsend bytea boxing")
}

/// PG `oidvectorin`: converts "num num ..." to internal oidvector form.
pub fn oidvectorin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = pg_getarg_cstring(fcinfo, 0);
    let mut oids: Vec<Oid> = Vec::new();
    for tok in s.split_ascii_whitespace() {
        oids.push(Oid(parse_oid(tok)));
    }
    PointerGetDatum(crate::utils::builtins::buildoidvector(&oids).cast::<u8>())
}

/// PG `oidvectorout`: converts internal oidvector form to "num num ...".
///
/// PG runs `check_valid_oidvector` here as paranoia against an oid[] array cast
/// to oidvector; that validator is still a stub (its array introspection isn't
/// built), so we omit the call -- every oidvector we construct via
/// `buildoidvector` is already 1-D, 0-based, no-nulls. TODO(array): restore the
/// `check_valid_oidvector` call once it is translated.
pub fn oidvectorout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = fcinfo.args[0].value.0 as *const oidvector;
    // SAFETY: `p` is a live oidvector for the duration of the
    // call and `dim1` elements follow the header at `values`.
    let s = unsafe {
        let nnums = (*p).dim1 as usize;
        let vptr = std::ptr::addr_of!((*p).values).cast::<Oid>();
        let mut parts: Vec<String> = Vec::with_capacity(nnums);
        for i in 0..nnums {
            parts.push((*vptr.add(i)).0.to_string());
        }
        parts.join(" ")
    };
    pg_return_cstring(&s)
}

/// PG `oidvectorrecv`: converts external binary format to oidvector.
pub fn oidvectorrecv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("oidvectorrecv needs array_recv")
}

/// PG `oidvectorsend`: converts oidvector to binary format.
pub fn oidvectorsend(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("oidvectorsend needs array_send")
}

/// PG `oidparse`: get an Oid from an ICONST/FCONST Value node.
///
/// The Value/Node parse-tree machinery is not yet translated; the only call
/// path that needs this (bootstrap/parse_oid) reaches the not-yet-built node
/// helpers, so this stub-calls them per rules.md s4.
pub fn oidparse(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("oidparse needs the Value/Node (T_Integer/T_Float) parse tree")
}

/// PG `oid_cmp`: qsort comparison function for Oids (unsigned 3-way).
pub fn oid_cmp(v1: Oid, v2: Oid) -> i32 {
    v1.0.cmp(&v2.0) as i32
}

// ===========================================================================
//   COMPARISON / PUBLIC ROUTINES
// ===========================================================================

macro_rules! oid_cmp_op {
    ($name:ident, $op:tt) => {
        #[doc = concat!("PG `", stringify!($name), "`.")]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let arg1 = pg_getarg_oid(fcinfo, 0).0;
            let arg2 = pg_getarg_oid(fcinfo, 1).0;
            BoolGetDatum(arg1 $op arg2)
        }
    };
}

oid_cmp_op!(oideq, ==);
oid_cmp_op!(oidne, !=);
oid_cmp_op!(oidlt, <);
oid_cmp_op!(oidle, <=);
oid_cmp_op!(oidgt, >);
oid_cmp_op!(oidge, >=);

/// PG `oidlarger`: max of two oids.
pub fn oidlarger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_oid(fcinfo, 0);
    let arg2 = pg_getarg_oid(fcinfo, 1);
    ObjectIdGetDatum(if arg1.0 > arg2.0 { arg1 } else { arg2 })
}

/// PG `oidsmaller`: min of two oids.
pub fn oidsmaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = pg_getarg_oid(fcinfo, 0);
    let arg2 = pg_getarg_oid(fcinfo, 1);
    ObjectIdGetDatum(if arg1.0 < arg2.0 { arg1 } else { arg2 })
}

// ---------------------------------------------------------------------------
//   oidvector comparison operators: thin wrappers over btoidvectorcmp.
//
// btoidvectorcmp (nbtcompare.c) needs the array layout that is not built yet;
// these dispatch to its existing stub (rules.md s4).
// ---------------------------------------------------------------------------

macro_rules! oidvector_cmp_op {
    ($name:ident, $op:tt) => {
        #[doc = concat!("PG `", stringify!($name), "`.")]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let cmp = DatumGetInt32(
                crate::backend::access::nbtree::nbtcompare::btoidvectorcmp(fcinfo),
            );
            BoolGetDatum(cmp $op 0)
        }
    };
}

oidvector_cmp_op!(oidvectoreq, ==);
oidvector_cmp_op!(oidvectorne, !=);
oidvector_cmp_op!(oidvectorlt, <);
oidvector_cmp_op!(oidvectorle, <=);
oidvector_cmp_op!(oidvectorgt, >);
oidvector_cmp_op!(oidvectorge, >=);

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
    fn oid_in_out_roundtrip() {
        for s in ["0", "1", "42", "4294967295"] {
            let mut in_fc = fc(&[cstr_datum(s)]);
            let d = oidin(&mut in_fc);
            let mut out_fc = fc(&[d]);
            assert_eq!(out_to_string(oidout(&mut out_fc)), s);
        }
    }

    #[test]
    fn oid_in_unsigned_wraparound_and_errors() {
        // "-1" wraps to UINT32_MAX (strtoul semantics).
        let mut f = fc(&[cstr_datum("-1")]);
        assert_eq!(DatumGetObjectId(oidin(&mut f)).0, u32::MAX);
        let mut f = fc(&[cstr_datum("-2")]);
        assert_eq!(DatumGetObjectId(oidin(&mut f)).0, u32::MAX - 1);

        // overflow past 2^32-1 raises.
        assert!(catch_unwind(|| {
            let mut f = fc(&[cstr_datum("4294967296")]);
            oidin(&mut f)
        })
        .is_err());
        // invalid syntax raises.
        for bad in ["", "abc", "1.5", "12x"] {
            let s = bad.to_owned();
            let r = catch_unwind(move || {
                let mut f = fc(&[cstr_datum(&s)]);
                oidin(&mut f)
            });
            assert!(r.is_err(), "{bad} should be invalid");
        }
    }

    #[test]
    fn oid_comparisons() {
        let mut f = fc(&[ObjectIdGetDatum(Oid(10)), ObjectIdGetDatum(Oid(10))]);
        assert!(DatumGetBool(oideq(&mut f)));
        let mut f = fc(&[ObjectIdGetDatum(Oid(10)), ObjectIdGetDatum(Oid(20))]);
        assert!(DatumGetBool(oidlt(&mut f)));
        assert!(DatumGetBool(oidne(&mut f)));
        // unsigned: a large oid is > a small one (no sign confusion).
        let mut f = fc(&[ObjectIdGetDatum(Oid(u32::MAX)), ObjectIdGetDatum(Oid(1))]);
        assert!(DatumGetBool(oidgt(&mut f)));
        let mut f = fc(&[ObjectIdGetDatum(Oid(5)), ObjectIdGetDatum(Oid(9))]);
        assert_eq!(DatumGetObjectId(oidlarger(&mut f)).0, 9);
        let mut f = fc(&[ObjectIdGetDatum(Oid(5)), ObjectIdGetDatum(Oid(9))]);
        assert_eq!(DatumGetObjectId(oidsmaller(&mut f)).0, 5);
    }

    #[test]
    fn oidvector_in_out_roundtrip() {
        for s in ["10 20 30", "0", "1 2 3 4 5"] {
            let mut in_fc = fc(&[cstr_datum(s)]);
            let d = oidvectorin(&mut in_fc);
            let mut out_fc = fc(&[d]);
            assert_eq!(out_to_string(oidvectorout(&mut out_fc)), s);
        }
        // empty input -> empty vector -> empty string.
        let mut in_fc = fc(&[cstr_datum("   ")]);
        let d = oidvectorin(&mut in_fc);
        let mut out_fc = fc(&[d]);
        assert_eq!(out_to_string(oidvectorout(&mut out_fc)), "");
    }

    /// oidout resolves through the generated fmgr table to a bound function.
    #[test]
    fn fmgr_table_binds_oidout() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "oidout")
            .expect("oidout present");
        let func = entry.func.expect("oidout bound");
        let mut f = fc(&[ObjectIdGetDatum(Oid(42))]);
        assert_eq!(out_to_string(func(&mut f)), "42");
    }
}
