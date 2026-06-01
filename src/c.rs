//! Translation of postgres/src/include/c.h
//!
//! Fundamental C definitions, included (transitively) by every PostgreSQL source
//! file. This module provides the historical scalar type aliases, the IsValid/
//! alignment/assertion macros, and a handful of widely-used helper types.
//!
//! Compiler-characteristic macros from Section 1 of c.h (attribute wrappers like
//! `pg_attribute_printf`, `pg_noreturn`, `likely`/`unlikely`) are either no-ops in
//! Rust or expressed directly in the language, so most are omitted; the few with
//! semantic meaning are provided below.

use crate::pg_config::*;
use crate::postgres_ext::Oid;
use core::ffi::{c_char, c_int, c_uint, c_void};

// re-export the raw C scalar ffi types used throughout the port
pub use core::ffi::{c_char as cchar, c_int as cint, c_void as cvoid};

// ----------------------------------------------------------------
//              Section 1: compiler characteristics
// ----------------------------------------------------------------

/// `likely(x)` branch hint. Rust has `core::intrinsics::likely` only on nightly,
/// so on stable this is the identity, matching c.h's non-GCC fallback.
#[inline(always)]
pub fn likely(x: bool) -> bool {
    x
}

/// `unlikely(x)` branch hint (identity on stable Rust).
#[inline(always)]
pub fn unlikely(x: bool) -> bool {
    x
}

/// Generic function pointer (c.h `pg_funcptr_t`).
pub type pg_funcptr_t = Option<unsafe extern "C" fn()>;

/// `FLEXIBLE_ARRAY_MEMBER`: a C trailing flexible array. In Rust we represent the
/// member as a zero-length array `[T; FLEXIBLE_ARRAY_MEMBER]` placed last in a
/// `#[repr(C)]` struct, and index past it with raw pointers.
pub const FLEXIBLE_ARRAY_MEMBER: usize = 0;

// ----------------------------------------------------------------
//              Section 3: standard system types
// ----------------------------------------------------------------

/// Pointer: variable holding the address of any memory resident object.
pub type Pointer = *mut c_char;

// Historical names for the <stdint.h> types.
pub type int8 = i8;
pub type int16 = i16;
pub type int32 = i32;
pub type int64 = i64;
pub type uint8 = u8;
pub type uint16 = u16;
pub type uint32 = u32;
pub type uint64 = u64;

// bitsN: unit of bitwise operation, AT LEAST N bits in size.
pub type bits8 = uint8;
pub type bits16 = uint16;
pub type bits32 = uint32;

// 128-bit integers (HAVE_INT128 on platforms with __int128; Rust has them natively).
pub type int128 = i128;
pub type uint128 = u128;
pub const HAVE_INT128: bool = true;

/// `INT64CONST(x)` / `UINT64CONST(x)`: 64-bit integer literals (identity in Rust).
#[inline(always)]
pub const fn INT64CONST(x: i64) -> i64 {
    x
}
#[inline(always)]
pub const fn UINT64CONST(x: u64) -> u64 {
    x
}

// Historical names for limits in <stdint.h>.
pub const PG_INT8_MIN: int8 = int8::MIN;
pub const PG_INT8_MAX: int8 = int8::MAX;
pub const PG_UINT8_MAX: uint8 = uint8::MAX;
pub const PG_INT16_MIN: int16 = int16::MIN;
pub const PG_INT16_MAX: int16 = int16::MAX;
pub const PG_UINT16_MAX: uint16 = uint16::MAX;
pub const PG_INT32_MIN: int32 = int32::MIN;
pub const PG_INT32_MAX: int32 = int32::MAX;
pub const PG_UINT32_MAX: uint32 = uint32::MAX;
pub const PG_INT64_MIN: int64 = int64::MIN;
pub const PG_INT64_MAX: int64 = int64::MAX;
pub const PG_UINT64_MAX: uint64 = uint64::MAX;

/// Size: size of any memory resident object, as returned by `sizeof`.
pub type Size = usize;

/// Index into any memory resident array (non-negative).
pub type Index = c_uint;

/// Offset into any memory resident array (may be negative).
pub type Offset = c_int;

// Common Postgres datatype names (as used in the catalogs).
pub type float4 = f32;
pub type float8 = f64;

pub const FLOAT8PASSBYVAL: bool = USE_FLOAT8_BYVAL;

// regproc / RegProcedure / xact id family
pub type regproc = Oid;
pub type RegProcedure = regproc;
pub type TransactionId = uint32;
pub type LocalTransactionId = uint32;
pub type SubTransactionId = uint32;

pub const InvalidSubTransactionId: SubTransactionId = 0;
pub const TopSubTransactionId: SubTransactionId = 1;

pub type MultiXactId = TransactionId;
pub type MultiXactOffset = uint32;
pub type CommandId = uint32;

pub const FirstCommandId: CommandId = 0;
pub const InvalidCommandId: CommandId = !0u32;

/// Variable-length datatypes share the `struct varlena` header.
/// Do not touch `vl_len_` directly; use the VAR* helpers.
#[repr(C)]
pub struct varlena {
    /// Do not touch this field directly!
    pub vl_len_: [c_char; 4],
    /// Data content begins here (FLEXIBLE_ARRAY_MEMBER).
    pub vl_dat: [c_char; FLEXIBLE_ARRAY_MEMBER],
}

/// `VARHDRSZ`: size of the varlena header (an int32).
pub const VARHDRSZ: int32 = core::mem::size_of::<int32>() as int32;

pub type bytea = varlena;
pub type text = varlena;
/// blank-padded char, ie SQL char(n)
pub type BpChar = varlena;
/// var-length char, ie SQL varchar(n)
pub type VarChar = varlena;

/// int2vector: a 1-D array of int16 laid out like ArrayType (header must match).
#[repr(C)]
pub struct int2vector {
    pub vl_len_: int32,
    pub ndim: c_int,
    pub dataoffset: int32,
    pub elemtype: Oid,
    pub dim1: c_int,
    pub lbound1: c_int,
    pub values: [int16; FLEXIBLE_ARRAY_MEMBER],
}

/// oidvector: a 1-D array of Oid laid out like ArrayType (header must match).
#[repr(C)]
pub struct oidvector {
    pub vl_len_: int32,
    pub ndim: c_int,
    pub dataoffset: int32,
    pub elemtype: Oid,
    pub dim1: c_int,
    pub lbound1: c_int,
    pub values: [Oid; FLEXIBLE_ARRAY_MEMBER],
}

/// Representation of a Name: a C string null-padded to exactly NAMEDATALEN bytes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct nameData {
    pub data: [c_char; NAMEDATALEN],
}
pub type NameData = nameData;
pub type Name = *mut NameData;

/// `NameStr(name)`: the `data` field of a NameData as a `*const c_char`.
///
/// # Safety
/// `name` must point to a live `NameData`.
#[inline]
pub unsafe fn NameStr(name: &NameData) -> *const c_char {
    name.data.as_ptr()
}

// ----------------------------------------------------------------
//              Section 4: IsValid macros for system types
// ----------------------------------------------------------------

/// `BoolIsValid` - a Rust `bool` is always valid; kept for call-site fidelity.
#[inline(always)]
pub fn BoolIsValid(_boolean: bool) -> bool {
    true
}

/// `PointerIsValid(p)`: true iff pointer is non-NULL.
#[inline(always)]
pub fn PointerIsValid<T>(pointer: *const T) -> bool {
    !pointer.is_null()
}

/// `PointerIsAligned(pointer, type)`: pointer is aligned for `type`.
#[inline(always)]
pub fn PointerIsAligned<P, Ty>(pointer: *const P) -> bool {
    (pointer as usize) % core::mem::size_of::<Ty>() == 0
}

/// `OffsetToPointer(base, offset)`.
///
/// # Safety
/// `base + offset` must remain within the same allocation.
#[inline(always)]
pub unsafe fn OffsetToPointer(base: *mut c_void, offset: usize) -> *mut c_void {
    (base as *mut c_char).add(offset) as *mut c_void
}

/// `OidIsValid(objectId)`: an Oid is valid iff it isn't InvalidOid.
#[inline(always)]
pub fn OidIsValid(objectId: Oid) -> bool {
    objectId != crate::postgres_ext::InvalidOid
}

/// `RegProcedureIsValid(p)`.
#[inline(always)]
pub fn RegProcedureIsValid(p: RegProcedure) -> bool {
    OidIsValid(p)
}

// ----------------------------------------------------------------
//              Section 5: lengthof, alignment
// ----------------------------------------------------------------

/// `lengthof(array)`: number of elements in a fixed-size array.
#[macro_export]
macro_rules! lengthof {
    ($array:expr) => {
        ($array).len()
    };
}

/// `TYPEALIGN(ALIGNVAL, LEN)`: round LEN up to a multiple of ALIGNVAL (a power of 2).
#[inline(always)]
pub const fn TYPEALIGN(alignval: usize, len: usize) -> usize {
    (len + (alignval - 1)) & !(alignval - 1)
}
#[inline(always)]
pub const fn SHORTALIGN(len: usize) -> usize {
    TYPEALIGN(ALIGNOF_SHORT, len)
}
#[inline(always)]
pub const fn INTALIGN(len: usize) -> usize {
    TYPEALIGN(ALIGNOF_INT, len)
}
#[inline(always)]
pub const fn LONGALIGN(len: usize) -> usize {
    TYPEALIGN(ALIGNOF_LONG, len)
}
#[inline(always)]
pub const fn DOUBLEALIGN(len: usize) -> usize {
    TYPEALIGN(ALIGNOF_DOUBLE, len)
}
#[inline(always)]
pub const fn MAXALIGN(len: usize) -> usize {
    TYPEALIGN(MAXIMUM_ALIGNOF, len)
}
#[inline(always)]
pub const fn BUFFERALIGN(len: usize) -> usize {
    TYPEALIGN(ALIGNOF_BUFFER, len)
}
#[inline(always)]
pub const fn CACHELINEALIGN(len: usize) -> usize {
    TYPEALIGN(PG_CACHE_LINE_SIZE, len)
}

#[inline(always)]
pub const fn TYPEALIGN_DOWN(alignval: usize, len: usize) -> usize {
    len & !(alignval - 1)
}
#[inline(always)]
pub const fn SHORTALIGN_DOWN(len: usize) -> usize {
    TYPEALIGN_DOWN(ALIGNOF_SHORT, len)
}
#[inline(always)]
pub const fn INTALIGN_DOWN(len: usize) -> usize {
    TYPEALIGN_DOWN(ALIGNOF_INT, len)
}
#[inline(always)]
pub const fn LONGALIGN_DOWN(len: usize) -> usize {
    TYPEALIGN_DOWN(ALIGNOF_LONG, len)
}
#[inline(always)]
pub const fn DOUBLEALIGN_DOWN(len: usize) -> usize {
    TYPEALIGN_DOWN(ALIGNOF_DOUBLE, len)
}
#[inline(always)]
pub const fn MAXALIGN_DOWN(len: usize) -> usize {
    TYPEALIGN_DOWN(MAXIMUM_ALIGNOF, len)
}

// ----------------------------------------------------------------
//              Section 6: assertions
// ----------------------------------------------------------------

/// `Assert(condition)`: in USE_ASSERT_CHECKING builds, abort if the condition is
/// false; otherwise a no-op. Rust's `debug_assertions` cfg plays the role of
/// USE_ASSERT_CHECKING, so this forwards to `debug_assert!`.
#[macro_export]
macro_rules! Assert {
    ($cond:expr $(,)?) => {
        debug_assert!($cond)
    };
    ($cond:expr, $($arg:tt)+) => {
        debug_assert!($cond, $($arg)+)
    };
}

/// `AssertMacro(condition)`: expression-form Assert. Evaluates to `()`.
#[macro_export]
macro_rules! AssertMacro {
    ($cond:expr) => {{
        debug_assert!($cond);
    }};
}

/// `ExceptionalCondition`: backend hook invoked by a failed Assert. The real
/// backend logs and aborts; the `Assert!` macro above uses `debug_assert!`
/// directly, so this is provided for callers that reference it explicitly.
///
/// # Panics
/// Always (it never returns), like the C `pg_noreturn` declaration.
pub fn ExceptionalCondition(conditionName: &str, fileName: &str, lineNumber: c_int) -> ! {
    panic!(
        "TRAP: failed Assert(\"{}\"), File: \"{}\", Line: {}",
        conditionName, fileName, lineNumber
    );
}

// ----------------------------------------------------------------
//              Section 7: widely useful macros
// ----------------------------------------------------------------

/// `Max(x, y)`. Unlike the C macro, arguments are evaluated once.
#[inline(always)]
pub fn Max<T: PartialOrd>(x: T, y: T) -> T {
    if x > y {
        x
    } else {
        y
    }
}

/// `Min(x, y)`. Unlike the C macro, arguments are evaluated once.
#[inline(always)]
pub fn Min<T: PartialOrd>(x: T, y: T) -> T {
    if x < y {
        x
    } else {
        y
    }
}

/// `INVERT_COMPARE_RESULT(var)`: flip the sign of a qsort-style comparison result,
/// avoiding the INT_MIN trap. Returns the new value.
#[inline(always)]
pub fn INVERT_COMPARE_RESULT(var: c_int) -> c_int {
    if var < 0 {
        1
    } else {
        -var
    }
}

// msb for char
pub const HIGHBIT: u8 = 0x80;
/// `IS_HIGHBIT_SET(ch)`
#[inline(always)]
pub fn IS_HIGHBIT_SET(ch: u8) -> bool {
    ch & HIGHBIT != 0
}

/// `SQL_STR_DOUBLE(ch, escape_backslash)`: does `ch` need doubling in a SQL string?
#[inline(always)]
pub fn SQL_STR_DOUBLE(ch: u8, escape_backslash: bool) -> bool {
    ch == b'\'' || (ch == b'\\' && escape_backslash)
}

pub const ESCAPE_STRING_SYNTAX: u8 = b'E';

pub const STATUS_OK: c_int = 0;
pub const STATUS_ERROR: c_int = -1;
pub const STATUS_EOF: c_int = -2;

// ----------------------------------------------------------------
//              Section 9: system-specific hacks
// ----------------------------------------------------------------

// Non-Windows: text and binary file open flags are identical.
pub const PG_BINARY: c_int = 0;
pub const PG_BINARY_A: &str = "a";
pub const PG_BINARY_R: &str = "r";
pub const PG_BINARY_W: &str = "w";

/// `MemSet(start, val, len)`: like memset but optimized for zeroing word-aligned
/// memory. The port uses Rust's `write_bytes`, which the compiler lowers
/// efficiently; the alignment fast-path is therefore unnecessary.
///
/// # Safety
/// `start` must be valid for writes of `len` bytes.
#[inline]
pub unsafe fn MemSet(start: *mut c_void, val: c_int, len: Size) {
    core::ptr::write_bytes(start as *mut u8, val as u8, len);
}
