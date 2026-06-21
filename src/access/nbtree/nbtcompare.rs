//! Comparison functions for the btree access method.
//!
//! IMPL:   postgres/src/backend/access/nbtree/nbtcompare.c
//!
//! #include mapping:
//!   "postgres.h"             -> use crate::prelude::*
//!   <limits.h>               -> UCHAR_MAX inlined as a const below
//!   "utils/builtins.h"       -> check_valid_oidvector (crate::utils::adt::oid)
//!   "utils/fmgrprotos.h"     -> (fmgr V1 prototypes; not needed in Rust)
//!   "utils/skipsupport.h"    -> SkipSupport (NOT PORTED -> skipsupport variants stubbed)
//!   "utils/sortsupport.h"    -> SortSupport / SortSupportData (crate::utils::sort::sortsupport)
//!
//! These functions are stored in pg_amproc.  For each btree operator class they
//! compute compare(a, b): <0 if a<b, =0 if a==b, >0 if a>b.  The result is always
//! int32 regardless of input width.
//!
//! Status:
//!   * btboolcmp / btint2cmp / btint4cmp / btint8cmp / cross-width int cmps /
//!     btoidcmp / btoidvectorcmp / btcharcmp                       -- FULLY REAL
//!   * btint2sortsupport / btint4sortsupport / btint8sortsupport /
//!     btoidsortsupport / btcharsortsupport (set ssup->comparator) -- FULLY REAL
//!   * btboolskipsupport / btint2skipsupport / btint4skipsupport /
//!     btint8skipsupport / btoidskipsupport / btcharskipsupport    -- STUBBED
//!     (need utils/skipsupport PrepareSkipSupport / SkipSupport, not yet ported)

use crate::prelude::*; // Datum, c-types, DatumGet*/...GetDatum, ereport!/errmsg!
use crate::postgres::{
    BoolGetDatum, CharGetDatum, DatumGetBool, DatumGetChar, DatumGetInt16, DatumGetInt32,
    DatumGetInt64, DatumGetObjectId, DatumGetPointer, Int16GetDatum, Int32GetDatum, Int64GetDatum,
    ObjectIdGetDatum,
};
use crate::common::int::{pg_cmp_s16, pg_cmp_s32, pg_cmp_s64, pg_cmp_u32}; // common/int.h
use crate::utils::adt::oid::check_valid_oidvector; // utils/adt/oid.c (utils/builtins.h)
use crate::utils::adt::skipsupport::{Relation, SkipSupport}; // utils/skipsupport.h (now ported)
use crate::utils::fmgr::FunctionCallInfo; // fmgr.h
use crate::utils::sort::sortsupport::SortSupport; // utils/sortsupport.h

// ---------------------------------------------------------------------------
// skip-support increment/decrement callbacks (one pair per type), matching
// SkipSupportIncDec = fn(Relation, Datum, *mut bool) -> Datum.  On under/
// overflow each sets the flag and returns an undefined Datum (0).
// ---------------------------------------------------------------------------
unsafe extern "C" fn bool_decrement(_rel: Relation, existing: Datum, underflow: *mut bool) -> Datum {
    if DatumGetBool(existing) {
        *underflow = false;
        BoolGetDatum(false)
    } else {
        *underflow = true;
        0
    }
}
unsafe extern "C" fn bool_increment(_rel: Relation, existing: Datum, overflow: *mut bool) -> Datum {
    if !DatumGetBool(existing) {
        *overflow = false;
        BoolGetDatum(true)
    } else {
        *overflow = true;
        0
    }
}
unsafe extern "C" fn int2_decrement(_rel: Relation, existing: Datum, underflow: *mut bool) -> Datum {
    let v = DatumGetInt16(existing);
    if v == PG_INT16_MIN {
        *underflow = true;
        return 0;
    }
    *underflow = false;
    Int16GetDatum(v - 1)
}
unsafe extern "C" fn int2_increment(_rel: Relation, existing: Datum, overflow: *mut bool) -> Datum {
    let v = DatumGetInt16(existing);
    if v == PG_INT16_MAX {
        *overflow = true;
        return 0;
    }
    *overflow = false;
    Int16GetDatum(v + 1)
}
unsafe extern "C" fn int4_decrement(_rel: Relation, existing: Datum, underflow: *mut bool) -> Datum {
    let v = DatumGetInt32(existing);
    if v == PG_INT32_MIN {
        *underflow = true;
        return 0;
    }
    *underflow = false;
    Int32GetDatum(v - 1)
}
unsafe extern "C" fn int4_increment(_rel: Relation, existing: Datum, overflow: *mut bool) -> Datum {
    let v = DatumGetInt32(existing);
    if v == PG_INT32_MAX {
        *overflow = true;
        return 0;
    }
    *overflow = false;
    Int32GetDatum(v + 1)
}
unsafe extern "C" fn int8_decrement(_rel: Relation, existing: Datum, underflow: *mut bool) -> Datum {
    let v = DatumGetInt64(existing);
    if v == PG_INT64_MIN {
        *underflow = true;
        return 0;
    }
    *underflow = false;
    Int64GetDatum(v - 1)
}
unsafe extern "C" fn int8_increment(_rel: Relation, existing: Datum, overflow: *mut bool) -> Datum {
    let v = DatumGetInt64(existing);
    if v == PG_INT64_MAX {
        *overflow = true;
        return 0;
    }
    *overflow = false;
    Int64GetDatum(v + 1)
}
unsafe extern "C" fn oid_decrement(_rel: Relation, existing: Datum, underflow: *mut bool) -> Datum {
    let v = DatumGetObjectId(existing);
    if v == 0 {
        *underflow = true;
        return 0;
    }
    *underflow = false;
    ObjectIdGetDatum(v - 1)
}
unsafe extern "C" fn oid_increment(_rel: Relation, existing: Datum, overflow: *mut bool) -> Datum {
    let v = DatumGetObjectId(existing);
    if v == u32::MAX {
        *overflow = true;
        return 0;
    }
    *overflow = false;
    ObjectIdGetDatum(v + 1)
}
unsafe extern "C" fn char_decrement(_rel: Relation, existing: Datum, underflow: *mut bool) -> Datum {
    let v = DatumGetChar(existing) as u8;
    if v == 0 {
        *underflow = true;
        return 0;
    }
    *underflow = false;
    CharGetDatum((v - 1) as core::ffi::c_char)
}
unsafe extern "C" fn char_increment(_rel: Relation, existing: Datum, overflow: *mut bool) -> Datum {
    let v = DatumGetChar(existing) as u8;
    if v == UCHAR_MAX {
        *overflow = true;
        return 0;
    }
    *overflow = false;
    CharGetDatum((v + 1) as core::ffi::c_char)
}
use crate::{
    PG_GETARG_BOOL, PG_GETARG_CHAR, PG_GETARG_INT16, PG_GETARG_INT32, PG_GETARG_INT64,
    PG_GETARG_OID, PG_GETARG_POINTER, PG_RETURN_INT32, PG_RETURN_VOID,
};
use core::ffi::c_int;

/*
 * C builds this file optionally with STRESS_SORT_INT_MIN, which makes the
 * "<"/">" results INT_MIN / INT_MAX.  For production (and here) they are -1/+1.
 */
const A_LESS_THAN_B: int32 = -1;
const A_GREATER_THAN_B: int32 = 1;

// <limits.h> UCHAR_MAX (unsigned char is 8-bit on all supported platforms).
const UCHAR_MAX: uint8 = 255;

// ---------------------------------------------------------------------------
// bool
// ---------------------------------------------------------------------------

pub unsafe fn btboolcmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: bool = PG_GETARG_BOOL!(fcinfo, 0);
    let b: bool = PG_GETARG_BOOL!(fcinfo, 1);

    PG_RETURN_INT32!(a as int32 - b as int32);
}

pub unsafe fn btboolskipsupport(fcinfo: FunctionCallInfo) -> Datum {
    let sksup: SkipSupport = PG_GETARG_POINTER!(fcinfo, 0) as SkipSupport;
    (*sksup).decrement = Some(bool_decrement);
    (*sksup).increment = Some(bool_increment);
    (*sksup).low_elem = BoolGetDatum(false);
    (*sksup).high_elem = BoolGetDatum(true);
    PG_RETURN_VOID!()
}

// ---------------------------------------------------------------------------
// int2 (int16)
// ---------------------------------------------------------------------------

pub unsafe fn btint2cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let b: int16 = PG_GETARG_INT16!(fcinfo, 1);

    PG_RETURN_INT32!(a as int32 - b as int32);
}

/// Fast inline comparator wired into SortSupport by btint2sortsupport.
pub unsafe fn btint2fastcmp(x: Datum, y: Datum, _ssup: SortSupport) -> c_int {
    let a: int16 = DatumGetInt16(x);
    let b: int16 = DatumGetInt16(y);

    pg_cmp_s16(a, b)
}

pub unsafe fn btint2sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup: SortSupport = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;

    (*ssup).comparator = Some(btint2fastcmp);
    PG_RETURN_VOID!();
}

pub unsafe fn btint2skipsupport(fcinfo: FunctionCallInfo) -> Datum {
    let sksup: SkipSupport = PG_GETARG_POINTER!(fcinfo, 0) as SkipSupport;
    (*sksup).decrement = Some(int2_decrement);
    (*sksup).increment = Some(int2_increment);
    (*sksup).low_elem = Int16GetDatum(PG_INT16_MIN);
    (*sksup).high_elem = Int16GetDatum(PG_INT16_MAX);
    PG_RETURN_VOID!()
}

// ---------------------------------------------------------------------------
// int4 (int32)
// ---------------------------------------------------------------------------

pub unsafe fn btint4cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let b: int32 = PG_GETARG_INT32!(fcinfo, 1);

    if a > b {
        PG_RETURN_INT32!(A_GREATER_THAN_B);
    } else if a == b {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(A_LESS_THAN_B);
    }
}

/// Authoritative int32 comparator wired into SortSupport by btint4sortsupport.
/// (C uses ssup_datum_int32_cmp from sortsupport.h; equivalent to pg_cmp_s32.)
pub unsafe fn btint4fastcmp(x: Datum, y: Datum, _ssup: SortSupport) -> c_int {
    let a: int32 = DatumGetInt32(x);
    let b: int32 = DatumGetInt32(y);

    pg_cmp_s32(a, b)
}

pub unsafe fn btint4sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup: SortSupport = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;

    (*ssup).comparator = Some(btint4fastcmp);
    PG_RETURN_VOID!();
}

pub unsafe fn btint4skipsupport(fcinfo: FunctionCallInfo) -> Datum {
    let sksup: SkipSupport = PG_GETARG_POINTER!(fcinfo, 0) as SkipSupport;
    (*sksup).decrement = Some(int4_decrement);
    (*sksup).increment = Some(int4_increment);
    (*sksup).low_elem = Int32GetDatum(PG_INT32_MIN);
    (*sksup).high_elem = Int32GetDatum(PG_INT32_MAX);
    PG_RETURN_VOID!()
}

// ---------------------------------------------------------------------------
// int8 (int64)
// ---------------------------------------------------------------------------

pub unsafe fn btint8cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let b: int64 = PG_GETARG_INT64!(fcinfo, 1);

    if a > b {
        PG_RETURN_INT32!(A_GREATER_THAN_B);
    } else if a == b {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(A_LESS_THAN_B);
    }
}

/// Authoritative int64 comparator wired into SortSupport by btint8sortsupport.
/// On a 64-bit Datum platform C uses ssup_datum_signed_cmp; on 32-bit it uses
/// btint8fastcmp.  Both are a signed 64-bit 3-way compare == pg_cmp_s64.
pub unsafe fn btint8fastcmp(x: Datum, y: Datum, _ssup: SortSupport) -> c_int {
    let a: int64 = DatumGetInt64(x);
    let b: int64 = DatumGetInt64(y);

    pg_cmp_s64(a, b)
}

pub unsafe fn btint8sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup: SortSupport = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;

    (*ssup).comparator = Some(btint8fastcmp);
    PG_RETURN_VOID!();
}

pub unsafe fn btint8skipsupport(fcinfo: FunctionCallInfo) -> Datum {
    let sksup: SkipSupport = PG_GETARG_POINTER!(fcinfo, 0) as SkipSupport;
    (*sksup).decrement = Some(int8_decrement);
    (*sksup).increment = Some(int8_increment);
    (*sksup).low_elem = Int64GetDatum(PG_INT64_MIN);
    (*sksup).high_elem = Int64GetDatum(PG_INT64_MAX);
    PG_RETURN_VOID!()
}

// ---------------------------------------------------------------------------
// cross-width integer comparisons
// ---------------------------------------------------------------------------

pub unsafe fn btint48cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let b: int64 = PG_GETARG_INT64!(fcinfo, 1);

    if a as int64 > b {
        PG_RETURN_INT32!(A_GREATER_THAN_B);
    } else if a as int64 == b {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(A_LESS_THAN_B);
    }
}

pub unsafe fn btint84cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let b: int32 = PG_GETARG_INT32!(fcinfo, 1);

    if a > b as int64 {
        PG_RETURN_INT32!(A_GREATER_THAN_B);
    } else if a == b as int64 {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(A_LESS_THAN_B);
    }
}

pub unsafe fn btint24cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let b: int32 = PG_GETARG_INT32!(fcinfo, 1);

    if a as int32 > b {
        PG_RETURN_INT32!(A_GREATER_THAN_B);
    } else if a as int32 == b {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(A_LESS_THAN_B);
    }
}

pub unsafe fn btint42cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let b: int16 = PG_GETARG_INT16!(fcinfo, 1);

    if a > b as int32 {
        PG_RETURN_INT32!(A_GREATER_THAN_B);
    } else if a == b as int32 {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(A_LESS_THAN_B);
    }
}

pub unsafe fn btint28cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: int16 = PG_GETARG_INT16!(fcinfo, 0);
    let b: int64 = PG_GETARG_INT64!(fcinfo, 1);

    if a as int64 > b {
        PG_RETURN_INT32!(A_GREATER_THAN_B);
    } else if a as int64 == b {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(A_LESS_THAN_B);
    }
}

pub unsafe fn btint82cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let b: int16 = PG_GETARG_INT16!(fcinfo, 1);

    if a > b as int64 {
        PG_RETURN_INT32!(A_GREATER_THAN_B);
    } else if a == b as int64 {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(A_LESS_THAN_B);
    }
}

// ---------------------------------------------------------------------------
// oid
// ---------------------------------------------------------------------------

pub unsafe fn btoidcmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: Oid = PG_GETARG_OID!(fcinfo, 0);
    let b: Oid = PG_GETARG_OID!(fcinfo, 1);

    if a > b {
        PG_RETURN_INT32!(A_GREATER_THAN_B);
    } else if a == b {
        PG_RETURN_INT32!(0);
    } else {
        PG_RETURN_INT32!(A_LESS_THAN_B);
    }
}

/// Fast inline comparator wired into SortSupport by btoidsortsupport.
pub unsafe fn btoidfastcmp(x: Datum, y: Datum, _ssup: SortSupport) -> c_int {
    let a: Oid = DatumGetObjectId(x);
    let b: Oid = DatumGetObjectId(y);

    pg_cmp_u32(a, b)
}

pub unsafe fn btoidsortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup: SortSupport = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;

    (*ssup).comparator = Some(btoidfastcmp);
    PG_RETURN_VOID!();
}

pub unsafe fn btoidskipsupport(fcinfo: FunctionCallInfo) -> Datum {
    let sksup: SkipSupport = PG_GETARG_POINTER!(fcinfo, 0) as SkipSupport;
    (*sksup).decrement = Some(oid_decrement);
    (*sksup).increment = Some(oid_increment);
    (*sksup).low_elem = ObjectIdGetDatum(InvalidOid); // InvalidOid == 0
    (*sksup).high_elem = ObjectIdGetDatum(u32::MAX); // OID_MAX
    PG_RETURN_VOID!()
}

// ---------------------------------------------------------------------------
// oidvector
// ---------------------------------------------------------------------------

pub unsafe fn btoidvectorcmp(fcinfo: FunctionCallInfo) -> Datum {
    let a = PG_GETARG_POINTER!(fcinfo, 0) as *mut oidvector;
    let b = PG_GETARG_POINTER!(fcinfo, 1) as *mut oidvector;

    check_valid_oidvector(a);
    check_valid_oidvector(b);

    /* We arbitrarily choose to sort first by vector length */
    if (*a).dim1 != (*b).dim1 {
        PG_RETURN_INT32!((*a).dim1 - (*b).dim1);
    }

    let aval = (*a).values.as_ptr();
    let bval = (*b).values.as_ptr();
    let mut i: c_int = 0;
    while i < (*a).dim1 {
        let av = *aval.add(i as usize);
        let bv = *bval.add(i as usize);
        if av != bv {
            if av > bv {
                PG_RETURN_INT32!(A_GREATER_THAN_B);
            } else {
                PG_RETURN_INT32!(A_LESS_THAN_B);
            }
        }
        i += 1;
    }
    PG_RETURN_INT32!(0);
}

// ---------------------------------------------------------------------------
// char ("char" / single-byte)
// ---------------------------------------------------------------------------

pub unsafe fn btcharcmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: c_char = PG_GETARG_CHAR!(fcinfo, 0);
    let b: c_char = PG_GETARG_CHAR!(fcinfo, 1);

    /* Be careful to compare chars as unsigned */
    PG_RETURN_INT32!((a as uint8) as int32 - (b as uint8) as int32);
}

/// Fast inline comparator wired into SortSupport by btcharsortsupport.
/// btcharcmp compares chars as unsigned, so the comparator must too.
pub unsafe fn btcharfastcmp(x: Datum, y: Datum, _ssup: SortSupport) -> c_int {
    let a: uint8 = DatumGetChar(x) as uint8;
    let b: uint8 = DatumGetChar(y) as uint8;

    pg_cmp_u32(a as uint32, b as uint32)
}

pub unsafe fn btcharsortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup: SortSupport = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;

    (*ssup).comparator = Some(btcharfastcmp);
    PG_RETURN_VOID!();
}

pub unsafe fn btcharskipsupport(fcinfo: FunctionCallInfo) -> Datum {
    let sksup: SkipSupport = PG_GETARG_POINTER!(fcinfo, 0) as SkipSupport;
    (*sksup).decrement = Some(char_decrement);
    (*sksup).increment = Some(char_increment);
    /* btcharcmp compares chars as unsigned: low byte 0x00, high byte 0xFF. */
    (*sksup).low_elem = CharGetDatum(0);
    (*sksup).high_elem = CharGetDatum(UCHAR_MAX as core::ffi::c_char);
    PG_RETURN_VOID!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::Int32GetDatum;
    use crate::DirectFunctionCall2;
    use core::ptr::null_mut;

    /// Call a btree cmp via the fmgr V1 path, with int32 args, return the int32.
    pub unsafe fn call_int4cmp(a: int32, b: int32) -> int32 {
        DatumGetInt32(DirectFunctionCall2!(
            btint4cmp,
            Int32GetDatum(a),
            Int32GetDatum(b)
        ))
    }

    #[test]
    fn btint4cmp_three_way() {
        unsafe {
            assert!((call_int4cmp(1, 2) as i32) < 0);
            assert_eq!(call_int4cmp(2, 2), 0);
            assert!((call_int4cmp(3, 2) as i32) > 0);
        }
    }

    #[test]
    fn btint4_fastcmp_matches_ordering() {
        unsafe {
            // The SortSupport fast comparator must agree with btint4cmp's order.
            let s = null_mut::<crate::utils::sort::sortsupport::SortSupportData>();
            assert!((btint4fastcmp(Int32GetDatum(1), Int32GetDatum(2), s) as i32) < 0);
            assert_eq!(btint4fastcmp(Int32GetDatum(2), Int32GetDatum(2), s) as i32, 0);
            assert!((btint4fastcmp(Int32GetDatum(3), Int32GetDatum(2), s) as i32) > 0);
        }
    }

    #[test]
    fn btcharcmp_unsigned() {
        unsafe {
            // 0x80 (128 unsigned) must sort ABOVE 0x7f (127), not below.
            let s = null_mut::<crate::utils::sort::sortsupport::SortSupportData>();
            assert!((btcharfastcmp(0x80 as Datum, 0x7f as Datum, s) as i32) > 0);
            assert!((btcharfastcmp(0x01 as Datum, 0x80 as Datum, s) as i32) < 0);
        }
    }

    // The skipsupport variants fill a SkipSupportData with the type's bounds +
    // inc/dec callbacks; exercise the int4 one end-to-end.
    #[test]
    fn btint4skipsupport_fills_and_steps() {
        use crate::utils::adt::skipsupport::SkipSupportData;
        use crate::DirectFunctionCall1;
        unsafe {
            let mut sksup = SkipSupportData {
                low_elem: 0,
                high_elem: 0,
                decrement: None,
                increment: None,
            };
            // The SkipSupport pointer is passed as fmgr arg 0.
            DirectFunctionCall1!(
                btint4skipsupport,
                (&mut sksup as *mut SkipSupportData) as Datum
            );
            assert_eq!(DatumGetInt32(sksup.low_elem), PG_INT32_MIN);
            assert_eq!(DatumGetInt32(sksup.high_elem), PG_INT32_MAX);

            let mut overflow = false;
            let next = (sksup.increment.unwrap())(null_mut(), Int32GetDatum(5), &mut overflow);
            assert!(!overflow);
            assert_eq!(DatumGetInt32(next), 6);

            // increment at INT32_MAX overflows
            (sksup.increment.unwrap())(null_mut(), Int32GetDatum(PG_INT32_MAX), &mut overflow);
            assert!(overflow);
        }
    }
}
