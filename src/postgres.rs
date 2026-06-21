//! Translation of postgres/src/include/postgres.h
//!
//! Primary include file for PostgreSQL backend `.c` files. Defines the `Datum`
//! type (the universal pass-by-value/pass-by-reference value carrier) and the
//! Get/Set conversion helpers between Datum and concrete C types.
//!
//! This build uses USE_FLOAT8_BYVAL (64-bit), so int64/float8 are pass-by-value.

use crate::c::*;
use crate::pg_config::USE_FLOAT8_BYVAL;
use crate::postgres_ext::Oid;
use core::ffi::{c_char, c_void};

// ----------------------------------------------------------------
//              Section 1: Datum type + support functions
// ----------------------------------------------------------------

/// A Datum holds either a pass-by-value value or a pointer to a pass-by-reference
/// value. `sizeof(Datum) == sizeof(void *)`.
pub type Datum = usize; // uintptr_t

/// A Datum together with its nullness.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct NullableDatum {
    pub value: Datum,
    pub isnull: bool,
}

pub const FIELDNO_NULLABLE_DATUM_DATUM: usize = 0;
pub const FIELDNO_NULLABLE_DATUM_ISNULL: usize = 1;

/// `DatumGetBool`: any nonzero value is true.
#[inline]
#[no_mangle]
pub fn DatumGetBool(X: Datum) -> bool {
    X != 0
}

/// `BoolGetDatum`.
#[inline]
pub fn BoolGetDatum(X: bool) -> Datum {
    if X {
        1
    } else {
        0
    }
}

#[inline]
pub fn DatumGetChar(X: Datum) -> c_char {
    X as c_char
}
#[inline]
pub fn CharGetDatum(X: c_char) -> Datum {
    X as Datum
}

#[inline]
pub fn Int8GetDatum(X: int8) -> Datum {
    X as Datum
}
#[inline]
pub fn DatumGetUInt8(X: Datum) -> uint8 {
    X as uint8
}
#[inline]
pub fn UInt8GetDatum(X: uint8) -> Datum {
    X as Datum
}

#[inline]
pub fn DatumGetInt16(X: Datum) -> int16 {
    X as int16
}
#[inline]
pub fn Int16GetDatum(X: int16) -> Datum {
    X as Datum
}
#[inline]
pub fn DatumGetUInt16(X: Datum) -> uint16 {
    X as uint16
}
#[inline]
pub fn UInt16GetDatum(X: uint16) -> Datum {
    X as Datum
}

#[inline]
pub fn DatumGetInt32(X: Datum) -> int32 {
    X as int32
}
#[inline]
pub fn Int32GetDatum(X: int32) -> Datum {
    X as Datum
}
#[inline]
pub fn DatumGetUInt32(X: Datum) -> uint32 {
    X as uint32
}
#[inline]
pub fn UInt32GetDatum(X: uint32) -> Datum {
    X as Datum
}

#[inline]
pub fn DatumGetObjectId(X: Datum) -> Oid {
    X as Oid
}
#[inline]
pub fn ObjectIdGetDatum(X: Oid) -> Datum {
    X as Datum
}

#[inline]
pub fn DatumGetTransactionId(X: Datum) -> TransactionId {
    X as TransactionId
}
#[inline]
pub fn TransactionIdGetDatum(X: TransactionId) -> Datum {
    X as Datum
}
#[inline]
pub fn MultiXactIdGetDatum(X: MultiXactId) -> Datum {
    X as Datum
}

#[inline]
pub fn DatumGetCommandId(X: Datum) -> CommandId {
    X as CommandId
}
#[inline]
pub fn CommandIdGetDatum(X: CommandId) -> Datum {
    X as Datum
}

/// `DatumGetPointer`.
#[inline]
pub fn DatumGetPointer(X: Datum) -> Pointer {
    X as Pointer
}
/// `PointerGetDatum`.
#[inline]
pub fn PointerGetDatum(X: *const c_void) -> Datum {
    X as Datum
}

/// `NameGetDatum`. Returns datum representation for a name.
#[inline]
pub fn NameGetDatum(X: *const crate::c::NameData) -> Datum {
    CStringGetDatum(X as *const c_char)
}

/// `DatumGetCString`.
#[inline]
pub fn DatumGetCString(X: Datum) -> *mut c_char {
    DatumGetPointer(X) as *mut c_char
}
/// `CStringGetDatum`.
#[inline]
pub fn CStringGetDatum(X: *const c_char) -> Datum {
    PointerGetDatum(X as *const c_void)
}

/// `DatumGetName`.
#[inline]
pub fn DatumGetName(X: Datum) -> Name {
    DatumGetPointer(X) as Name
}

/// `DatumGetInt64`. With USE_FLOAT8_BYVAL this is pass-by-value.
///
/// # Safety
/// In the (unused here) pass-by-reference build, `X` must point to an `int64`.
#[inline]
pub unsafe fn DatumGetInt64(X: Datum) -> int64 {
    if USE_FLOAT8_BYVAL {
        X as int64
    } else {
        *(DatumGetPointer(X) as *const int64)
    }
}

/// `Int64GetDatum`. Pass-by-value with USE_FLOAT8_BYVAL.
#[inline]
pub fn Int64GetDatum(X: int64) -> Datum {
    X as Datum
}

/// `DatumGetUInt64`.
///
/// # Safety
/// See [`DatumGetInt64`].
#[inline]
pub unsafe fn DatumGetUInt64(X: Datum) -> uint64 {
    if USE_FLOAT8_BYVAL {
        X as uint64
    } else {
        *(DatumGetPointer(X) as *const uint64)
    }
}

/// `UInt64GetDatum`.
#[inline]
pub fn UInt64GetDatum(X: uint64) -> Datum {
    Int64GetDatum(X as int64)
}

/// `DatumGetFloat4`: reinterpret the low 32 bits of the Datum as a float4.
#[inline]
pub fn DatumGetFloat4(X: Datum) -> float4 {
    f32::from_bits(DatumGetInt32(X) as u32)
}

/// `Float4GetDatum`.
#[inline]
pub fn Float4GetDatum(X: float4) -> Datum {
    Int32GetDatum(X.to_bits() as int32)
}

/// `DatumGetFloat8`.
///
/// # Safety
/// See [`DatumGetInt64`].
#[inline]
#[no_mangle]
pub unsafe fn DatumGetFloat8(X: Datum) -> float8 {
    if USE_FLOAT8_BYVAL {
        f64::from_bits(DatumGetInt64(X) as u64)
    } else {
        *(DatumGetPointer(X) as *const float8)
    }
}

/// `Float8GetDatum`.
#[inline]
pub fn Float8GetDatum(X: float8) -> Datum {
    Int64GetDatum(X.to_bits() as int64)
}

// ----------------------------------------------------------------
//              Section 2: miscellaneous
// ----------------------------------------------------------------
// NON_EXEC_STATIC and EXEC_BACKEND machinery have no Rust analogue and are omitted.
