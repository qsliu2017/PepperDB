//! Translated from PostgreSQL src/include/postgres.h
//!
//! NOTE: postgres.h sits at topological level 4, but `Datum`/`NullableDatum` are
//! ambient foundational types that earlier-level headers (fmgr.h, varatt.h, ...)
//! use without an #include. They are seeded here per translation-rules.md
//! (`Datum` = newtype over `usize`, pointer-width). The level-4 pass extends this
//! module; it must not redefine these.

/// `Datum` = `uintptr_t`: a pointer-width tagged value. NOT `u64` - it aliases
/// pointers. Per-type interpretation is up to the caller (see fmgr `DatumGetX`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct Datum(pub usize);

impl Datum {
    pub const fn from_bool(b: bool) -> Self {
        Datum(b as usize)
    }
    pub const fn get_bool(self) -> bool {
        self.0 != 0
    }
}

/// A `Datum` paired with its nullness, for places that need both together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NullableDatum {
    pub value: Datum,
    pub isnull: bool,
}

use crate::c::{CommandId, MultiXactId, Pointer, TransactionId, float4, float8};
use crate::postgres_ext::Oid;

/// `SIZEOF_DATUM == SIZEOF_VOID_P` = 8 on our 64-bit targets.
pub const SIZEOF_DATUM: usize = core::mem::size_of::<usize>();

// --- Section 1: Datum conversions (USE_FLOAT8_BYVAL path, 64-bit target) ---

/// Any nonzero value is true.
pub const fn DatumGetBool(x: Datum) -> bool {
    x.0 != 0
}
pub const fn BoolGetDatum(x: bool) -> Datum {
    Datum(if x { 1 } else { 0 })
}

/// C `char` is signed on our targets.
pub const fn DatumGetChar(x: Datum) -> i8 {
    x.0 as i8
}
pub const fn CharGetDatum(x: i8) -> Datum {
    Datum(x as usize)
}

pub const fn Int8GetDatum(x: i8) -> Datum {
    Datum(x as usize)
}
pub const fn DatumGetUInt8(x: Datum) -> u8 {
    x.0 as u8
}
pub const fn UInt8GetDatum(x: u8) -> Datum {
    Datum(x as usize)
}

pub const fn DatumGetInt16(x: Datum) -> i16 {
    x.0 as i16
}
pub const fn Int16GetDatum(x: i16) -> Datum {
    Datum(x as usize)
}
pub const fn DatumGetUInt16(x: Datum) -> u16 {
    x.0 as u16
}
pub const fn UInt16GetDatum(x: u16) -> Datum {
    Datum(x as usize)
}

pub const fn DatumGetInt32(x: Datum) -> i32 {
    x.0 as i32
}
pub const fn Int32GetDatum(x: i32) -> Datum {
    Datum(x as usize)
}
pub const fn DatumGetUInt32(x: Datum) -> u32 {
    x.0 as u32
}
pub const fn UInt32GetDatum(x: u32) -> Datum {
    Datum(x as usize)
}

pub const fn DatumGetObjectId(x: Datum) -> Oid {
    Oid(x.0 as u32)
}
pub const fn ObjectIdGetDatum(x: Oid) -> Datum {
    Datum(x.0 as usize)
}

pub const fn DatumGetTransactionId(x: Datum) -> TransactionId {
    TransactionId(x.0 as u32)
}
pub const fn TransactionIdGetDatum(x: TransactionId) -> Datum {
    Datum(x.0 as usize)
}

/// MultiXactId is a TransactionId alias; only the GetDatum side exists in C.
pub const fn MultiXactIdGetDatum(x: MultiXactId) -> Datum {
    Datum(x.0 as usize)
}

pub const fn DatumGetCommandId(x: Datum) -> CommandId {
    CommandId(x.0 as u32)
}
pub const fn CommandIdGetDatum(x: CommandId) -> Datum {
    Datum(x.0 as usize)
}

// USE_FLOAT8_BYVAL: int64/uint64/float8 are pass-by-value on 64-bit targets.
pub const fn DatumGetInt64(x: Datum) -> i64 {
    x.0 as i64
}
pub const fn Int64GetDatum(x: i64) -> Datum {
    Datum(x as usize)
}
pub const fn DatumGetUInt64(x: Datum) -> u64 {
    x.0 as u64
}
pub const fn UInt64GetDatum(x: u64) -> Datum {
    Datum(x as usize)
}

// --- Pointer-bearing Datums (pass-by-reference) ---

pub fn DatumGetPointer(x: Datum) -> Pointer {
    x.0 as Pointer // TODO(ptr): ownership not encoded in signature
}
pub fn PointerGetDatum(x: *const u8) -> Datum {
    Datum(x as usize) // TODO(ptr)
}

/// C string (NUL-terminated). Returned as a raw pointer at this boundary.
pub fn DatumGetCString(x: Datum) -> *mut i8 {
    x.0 as *mut i8 // TODO(ptr)
}
pub fn CStringGetDatum(x: *const i8) -> Datum {
    Datum(x as usize) // TODO(ptr)
}

pub fn DatumGetName(x: Datum) -> crate::c::Name {
    x.0 as crate::c::Name // TODO(ptr)
}
/// C: `NameGetDatum` returns the address of the embedded name buffer.
pub fn NameGetDatum(x: &crate::c::NameData) -> Datum {
    Datum(x.data.as_ptr() as usize) // TODO(ptr): caller ensures lifetime
}

// --- Float <-> Datum: go through the integer bit pattern (FLOAT8PASSBYVAL) ---

pub const fn DatumGetFloat4(x: Datum) -> float4 {
    f32::from_bits(DatumGetInt32(x) as u32)
}
pub const fn Float4GetDatum(x: float4) -> Datum {
    Int32GetDatum(x.to_bits() as i32)
}
pub const fn DatumGetFloat8(x: Datum) -> float8 {
    f64::from_bits(DatumGetInt64(x) as u64)
}
pub const fn Float8GetDatum(x: float8) -> Datum {
    Int64GetDatum(x.to_bits() as i64)
}

// --- Section 2: miscellaneous ---
// NON_EXEC_STATIC / EXEC_BACKEND: no Rust analog (single-process model). Omitted.
