//! Comparison functions for the btree access method. Translated from
//! src/backend/access/nbtree/nbtcompare.c.
//!
//! These functions are stored in pg_amproc. For each btree operator class they
//! compute `compare(a, b)` returning `< 0`, `0`, or `> 0` (always an int32).
//! Each C `Datum fn(PG_FUNCTION_ARGS)` becomes a `PGFunction`-typed Rust fn
//! `fn(&mut FunctionCallInfoBaseData) -> Datum` (read args with `args[n]`,
//! return via `Int32GetDatum`). The sort-support fast comparators
//! (`btint2fastcmp`, ...) and the skip-support inc/dec callbacks are file-local
//! statics installed into the SortSupport / SkipSupport callback slots.
//!
//! The skip/sort-support entry points (`bt*sortsupport`, `bt*skipsupport`) take
//! a pointer/handle argument that the executor passes via `args[0]`; the
//! preprocessing machinery that builds those is not yet exercised in M2, so they
//! install the callbacks and return VOID (matching C).

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    reason = "intentional C width arithmetic: nbtcompare.c does explicit int16/int32/uint8 \
              casts and subtractions in wider types (the value-cast family is an allowed \
              port-inherent lint per rules.md s11)"
)]

use crate::c::{PG_INT16_MAX, PG_INT16_MIN, PG_INT32_MAX, PG_INT32_MIN, PG_INT64_MAX, PG_INT64_MIN};
use crate::fmgr::FunctionCallInfoBaseData;
use crate::postgres::{
    Datum, DatumGetInt16, DatumGetInt32, DatumGetInt64, DatumGetObjectId, Int16GetDatum,
    Int32GetDatum, Int64GetDatum, ObjectIdGetDatum, UInt8GetDatum,
};
use crate::postgres_ext::{Oid, OID_MAX};

// STRESS_SORT_INT_MIN is not defined: A_LESS_THAN_B = -1, A_GREATER_THAN_B = +1.
const A_LESS_THAN_B: i32 = -1;
const A_GREATER_THAN_B: i32 = 1;

/// Three-way compare returning the customary -1/0/+1 (the C `if a>b .. else if`
/// ladder, written as a single ordering match to satisfy `comparison_chain`).
#[inline]
fn cmp3<T: Ord + Copy>(a: T, b: T) -> i32 {
    match a.cmp(&b) {
        core::cmp::Ordering::Greater => A_GREATER_THAN_B,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Less => A_LESS_THAN_B,
    }
}

// ---------------------------------------------------------------------------
// PG_GETARG helpers (mirror the int.c convention: deref of `args[n]`).
// ---------------------------------------------------------------------------

#[inline]
fn arg_i16(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i16 {
    DatumGetInt16(fcinfo.args[n].value)
}
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i32 {
    DatumGetInt32(fcinfo.args[n].value)
}
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i64 {
    DatumGetInt64(fcinfo.args[n].value)
}
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Oid {
    DatumGetObjectId(fcinfo.args[n].value)
}
/// PG `PG_GETARG_CHAR`: a `"char"` value is a single signed byte in the Datum.
#[inline]
fn arg_char(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i8 {
    fcinfo.args[n].value.0 as i8
}

// ===========================================================================
// bool
// ===========================================================================

/// PG `btboolcmp`.
pub fn btboolcmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = i32::from(fcinfo.args[0].value.0 as u8 != 0);
    let b = i32::from(fcinfo.args[1].value.0 as u8 != 0);
    Int32GetDatum(a - b)
}

// ===========================================================================
// int2 (int16)
// ===========================================================================

/// PG `btint2cmp`.
pub fn btint2cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_i16(fcinfo, 0);
    let b = arg_i16(fcinfo, 1);
    Int32GetDatum(i32::from(a) - i32::from(b))
}

/// PG `btint2fastcmp` (SortSupport comparator).
fn btint2fastcmp(x: Datum, y: Datum, _ssup: &crate::utils::sortsupport::SortSupportData) -> i32 {
    i32::from(DatumGetInt16(x)) - i32::from(DatumGetInt16(y))
}

/// PG `btint2sortsupport`.
pub fn btint2sortsupport(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ssup = ssup_ptr(fcinfo);
    // SAFETY: args[0] holds a valid SortSupportData pointer (sortsupport entry).
    let ssup = unsafe { &mut *ssup };
    ssup.comparator = Some(btint2fastcmp);
    Datum(0)
}

fn int2_decrement(_rel: Option<&crate::utils::rel::RelationData>, existing: Datum) -> (Datum, bool) {
    let v = DatumGetInt16(existing);
    if v == PG_INT16_MIN {
        return (Datum(0), true);
    }
    (Int16GetDatum(v - 1), false)
}
fn int2_increment(_rel: Option<&crate::utils::rel::RelationData>, existing: Datum) -> (Datum, bool) {
    let v = DatumGetInt16(existing);
    if v == PG_INT16_MAX {
        return (Datum(0), true);
    }
    (Int16GetDatum(v + 1), false)
}

/// PG `btint2skipsupport`.
pub fn btint2skipsupport(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let sksup = sksup_ptr(fcinfo);
    // SAFETY: args[0] holds a valid SkipSupportData pointer (skipsupport entry).
    let sksup = unsafe { &mut *sksup };
    sksup.decrement = int2_decrement;
    sksup.increment = int2_increment;
    sksup.low_elem = Int16GetDatum(PG_INT16_MIN);
    sksup.high_elem = Int16GetDatum(PG_INT16_MAX);
    Datum(0)
}

// ===========================================================================
// int4 (int32)
// ===========================================================================

/// PG `btint4cmp`.
pub fn btint4cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(cmp3(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}

/// PG `btint4sortsupport`.
pub fn btint4sortsupport(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ssup = ssup_ptr(fcinfo);
    // SAFETY: args[0] holds a valid SortSupportData pointer (sortsupport entry).
    let ssup = unsafe { &mut *ssup };
    ssup.comparator = Some(ssup_datum_int32_cmp);
    Datum(0)
}

fn int4_decrement(_rel: Option<&crate::utils::rel::RelationData>, existing: Datum) -> (Datum, bool) {
    let v = DatumGetInt32(existing);
    if v == PG_INT32_MIN {
        return (Datum(0), true);
    }
    (Int32GetDatum(v - 1), false)
}
fn int4_increment(_rel: Option<&crate::utils::rel::RelationData>, existing: Datum) -> (Datum, bool) {
    let v = DatumGetInt32(existing);
    if v == PG_INT32_MAX {
        return (Datum(0), true);
    }
    (Int32GetDatum(v + 1), false)
}

/// PG `btint4skipsupport`.
pub fn btint4skipsupport(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let sksup = sksup_ptr(fcinfo);
    // SAFETY: args[0] holds a valid SkipSupportData pointer (skipsupport entry).
    let sksup = unsafe { &mut *sksup };
    sksup.decrement = int4_decrement;
    sksup.increment = int4_increment;
    sksup.low_elem = Int32GetDatum(PG_INT32_MIN);
    sksup.high_elem = Int32GetDatum(PG_INT32_MAX);
    Datum(0)
}

// ===========================================================================
// int8 (int64)
// ===========================================================================

/// PG `btint8cmp`.
pub fn btint8cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(cmp3(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}

/// PG `btint8sortsupport`. On 64-bit Datum, uses the signed-datum comparator.
pub fn btint8sortsupport(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ssup = ssup_ptr(fcinfo);
    // SAFETY: args[0] holds a valid SortSupportData pointer (sortsupport entry).
    let ssup = unsafe { &mut *ssup };
    ssup.comparator = Some(ssup_datum_signed_cmp);
    Datum(0)
}

fn int8_decrement(_rel: Option<&crate::utils::rel::RelationData>, existing: Datum) -> (Datum, bool) {
    let v = DatumGetInt64(existing);
    if v == PG_INT64_MIN {
        return (Datum(0), true);
    }
    (Int64GetDatum(v - 1), false)
}
fn int8_increment(_rel: Option<&crate::utils::rel::RelationData>, existing: Datum) -> (Datum, bool) {
    let v = DatumGetInt64(existing);
    if v == PG_INT64_MAX {
        return (Datum(0), true);
    }
    (Int64GetDatum(v + 1), false)
}

/// PG `btint8skipsupport`.
pub fn btint8skipsupport(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let sksup = sksup_ptr(fcinfo);
    // SAFETY: args[0] holds a valid SkipSupportData pointer (skipsupport entry).
    let sksup = unsafe { &mut *sksup };
    sksup.decrement = int8_decrement;
    sksup.increment = int8_increment;
    sksup.low_elem = Int64GetDatum(PG_INT64_MIN);
    sksup.high_elem = Int64GetDatum(PG_INT64_MAX);
    Datum(0)
}

// ===========================================================================
// cross-width int comparisons
// ===========================================================================

macro_rules! cross_cmp {
    ($name:ident, $get_a:ident, $get_b:ident) => {
        /// PG cross-width btree comparison.
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            Int32GetDatum(cmp3($get_a(fcinfo, 0) as i64, $get_b(fcinfo, 1) as i64))
        }
    };
}
cross_cmp!(btint48cmp, arg_i32, arg_i64);
cross_cmp!(btint84cmp, arg_i64, arg_i32);
cross_cmp!(btint24cmp, arg_i16, arg_i32);
cross_cmp!(btint42cmp, arg_i32, arg_i16);
cross_cmp!(btint28cmp, arg_i16, arg_i64);
cross_cmp!(btint82cmp, arg_i64, arg_i16);

// ===========================================================================
// oid
// ===========================================================================

/// PG `btoidcmp`.
pub fn btoidcmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(cmp3(arg_oid(fcinfo, 0).0, arg_oid(fcinfo, 1).0))
}

fn btoidfastcmp(x: Datum, y: Datum, _ssup: &crate::utils::sortsupport::SortSupportData) -> i32 {
    cmp3(DatumGetObjectId(x).0, DatumGetObjectId(y).0)
}

/// PG `btoidsortsupport`.
pub fn btoidsortsupport(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ssup = ssup_ptr(fcinfo);
    // SAFETY: args[0] holds a valid SortSupportData pointer (sortsupport entry).
    let ssup = unsafe { &mut *ssup };
    ssup.comparator = Some(btoidfastcmp);
    Datum(0)
}

fn oid_decrement(_rel: Option<&crate::utils::rel::RelationData>, existing: Datum) -> (Datum, bool) {
    let v = DatumGetObjectId(existing);
    if v.0 == 0 {
        return (Datum(0), true);
    }
    (ObjectIdGetDatum(Oid(v.0 - 1)), false)
}
fn oid_increment(_rel: Option<&crate::utils::rel::RelationData>, existing: Datum) -> (Datum, bool) {
    let v = DatumGetObjectId(existing);
    if v.0 == OID_MAX.0 {
        return (Datum(0), true);
    }
    (ObjectIdGetDatum(Oid(v.0 + 1)), false)
}

/// PG `btoidskipsupport`.
pub fn btoidskipsupport(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let sksup = sksup_ptr(fcinfo);
    // SAFETY: args[0] holds a valid SkipSupportData pointer (skipsupport entry).
    let sksup = unsafe { &mut *sksup };
    sksup.decrement = oid_decrement;
    sksup.increment = oid_increment;
    sksup.low_elem = ObjectIdGetDatum(Oid(0));
    sksup.high_elem = ObjectIdGetDatum(OID_MAX);
    Datum(0)
}

/// PG `btoidvectorcmp`. The oidvector array machinery is not yet translated; the
/// pointer argument resolves through the not-yet-built array path.
pub fn btoidvectorcmp(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!()
}

// ===========================================================================
// char ("char", a single byte)
// ===========================================================================

/// PG `btcharcmp`. Chars compare as unsigned.
pub fn btcharcmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_char(fcinfo, 0) as u8;
    let b = arg_char(fcinfo, 1) as u8;
    Int32GetDatum(i32::from(a) - i32::from(b))
}

fn char_decrement(_rel: Option<&crate::utils::rel::RelationData>, existing: Datum) -> (Datum, bool) {
    let v = existing.0 as u8;
    if v == 0 {
        return (Datum(0), true);
    }
    (UInt8GetDatum(v - 1), false)
}
fn char_increment(_rel: Option<&crate::utils::rel::RelationData>, existing: Datum) -> (Datum, bool) {
    let v = existing.0 as u8;
    if v == u8::MAX {
        return (Datum(0), true);
    }
    (UInt8GetDatum(v + 1), false)
}

/// PG `btcharskipsupport`.
pub fn btcharskipsupport(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let sksup = sksup_ptr(fcinfo);
    // SAFETY: args[0] holds a valid SkipSupportData pointer (skipsupport entry).
    let sksup = unsafe { &mut *sksup };
    sksup.decrement = char_decrement;
    sksup.increment = char_increment;
    sksup.low_elem = UInt8GetDatum(0);
    sksup.high_elem = UInt8GetDatum(u8::MAX);
    Datum(0)
}

// ===========================================================================
// argument-pointer helpers + the generic datum comparators
// ===========================================================================

/// The SortSupport handle passed in `args[0]` (a `*mut SortSupportData`). The
/// caller derefs under its own `&mut fcinfo`, so no `&mut`-from-`&` laundering.
fn ssup_ptr(fcinfo: &FunctionCallInfoBaseData) -> *mut crate::utils::sortsupport::SortSupportData {
    fcinfo.args[0].value.0 as *mut crate::utils::sortsupport::SortSupportData
}

/// The SkipSupport handle passed in `args[0]` (a `*mut SkipSupportData`).
fn sksup_ptr(fcinfo: &FunctionCallInfoBaseData) -> *mut crate::utils::skipsupport::SkipSupportData {
    fcinfo.args[0].value.0 as *mut crate::utils::skipsupport::SkipSupportData
}

/// PG `ssup_datum_int32_cmp` (sortsupport.h): three-way compare of int32 datums.
pub fn ssup_datum_int32_cmp(
    x: Datum,
    y: Datum,
    _ssup: &crate::utils::sortsupport::SortSupportData,
) -> i32 {
    cmp3(DatumGetInt32(x), DatumGetInt32(y))
}

/// PG `ssup_datum_signed_cmp` (sortsupport.h): three-way compare of int64 datums.
pub fn ssup_datum_signed_cmp(
    x: Datum,
    y: Datum,
    _ssup: &crate::utils::sortsupport::SortSupportData,
) -> i32 {
    cmp3(DatumGetInt64(x), DatumGetInt64(y))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fmgr::FunctionCallInfoBaseData;
    use crate::postgres::NullableDatum;

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

    fn fc2(a: Datum, b: Datum) -> FunctionCallInfoBaseData {
        fc(&[a, b])
    }

    #[test]
    fn int4_cmp_total_order() {
        let lt = btint4cmp(&mut fc2(Int32GetDatum(1), Int32GetDatum(2)));
        let eq = btint4cmp(&mut fc2(Int32GetDatum(7), Int32GetDatum(7)));
        let gt = btint4cmp(&mut fc2(Int32GetDatum(9), Int32GetDatum(2)));
        assert_eq!(DatumGetInt32(lt), -1);
        assert_eq!(DatumGetInt32(eq), 0);
        assert_eq!(DatumGetInt32(gt), 1);
    }

    #[test]
    fn int4_cmp_no_overflow() {
        // a - b would overflow if naively subtracted, but cmp must be correct.
        let r = btint4cmp(&mut fc2(Int32GetDatum(PG_INT32_MAX), Int32GetDatum(PG_INT32_MIN)));
        assert_eq!(DatumGetInt32(r), 1);
        let r = btint4cmp(&mut fc2(Int32GetDatum(PG_INT32_MIN), Int32GetDatum(PG_INT32_MAX)));
        assert_eq!(DatumGetInt32(r), -1);
    }

    #[test]
    fn int2_cmp() {
        let r = btint2cmp(&mut fc2(Int16GetDatum(-5), Int16GetDatum(5)));
        assert!(DatumGetInt32(r) < 0);
    }

    #[test]
    fn int8_cmp_no_overflow() {
        let r = btint8cmp(&mut fc2(Int64GetDatum(PG_INT64_MAX), Int64GetDatum(PG_INT64_MIN)));
        assert_eq!(DatumGetInt32(r), 1);
    }

    #[test]
    fn cross_width_48() {
        let r = btint48cmp(&mut fc2(Int32GetDatum(3), Int64GetDatum(3)));
        assert_eq!(DatumGetInt32(r), 0);
        let r = btint48cmp(&mut fc2(Int32GetDatum(3), Int64GetDatum(4)));
        assert_eq!(DatumGetInt32(r), -1);
    }

    #[test]
    fn oid_cmp() {
        let r = btoidcmp(&mut fc2(ObjectIdGetDatum(Oid(10)), ObjectIdGetDatum(Oid(20))));
        assert_eq!(DatumGetInt32(r), -1);
    }

    #[test]
    fn char_cmp_unsigned() {
        // 0x80 as signed char is negative but must sort above 0x01 (unsigned).
        let r = btcharcmp(&mut fc2(Datum(0x80), Datum(0x01)));
        assert!(DatumGetInt32(r) > 0);
    }

    #[test]
    fn bool_cmp() {
        let r = btboolcmp(&mut fc2(Datum(1), Datum(0)));
        assert_eq!(DatumGetInt32(r), 1);
    }

    #[test]
    fn int4_skipsupport_installs_and_bounds() {
        let mut sksup = crate::utils::skipsupport::SkipSupportData {
            low_elem: Datum(0),
            high_elem: Datum(0),
            decrement: int4_decrement,
            increment: int4_increment,
        };
        let mut f = fc(&[Datum(std::ptr::from_mut(&mut sksup) as usize)]);
        btint4skipsupport(&mut f);
        assert_eq!(DatumGetInt32(sksup.low_elem), PG_INT32_MIN);
        assert_eq!(DatumGetInt32(sksup.high_elem), PG_INT32_MAX);
        let (d, of) = (sksup.increment)(None, Int32GetDatum(5));
        assert!(!of);
        assert_eq!(DatumGetInt32(d), 6);
        let (_d, of) = (sksup.increment)(None, Int32GetDatum(PG_INT32_MAX));
        assert!(of);
    }
}
