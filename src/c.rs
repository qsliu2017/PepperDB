//! Translated from PostgreSQL src/include/c.h

use crate::pg_config::{
    ALIGNOF_DOUBLE, ALIGNOF_INT, ALIGNOF_LONG, ALIGNOF_SHORT, MAXIMUM_ALIGNOF,
};
use crate::postgres_ext::Oid;

// Re-export NAMEDATALEN so dependents can import it from `crate::c` (canonical path).
pub use crate::pg_config_manual::NAMEDATALEN;

// === Section 1: compiler characteristics ===

/// Generic function pointer (C: `typedef void (*pg_funcptr_t)(void)`).
pub type pg_funcptr_t = unsafe extern "C" fn();

// pg_attribute_* / pg_noreturn / likely / unlikely: compiler hints with no Rust
// equivalent. pg_noreturn maps to a `-> !` return type at the call site.
// CppAsString/CppConcat/VA_ARGS_NARGS: C preprocessor plumbing, dropped.

// === Section 3: standard system types ===

/// C: `typedef char *Pointer`. Used for byte-pointer arithmetic.
pub type Pointer = *mut u8;

// Historical names for <stdint.h> types map straight to Rust primitives:
// int8->i8, int16->i16, int32->i32, int64->i64, uint8->u8, ... (no aliases).

/// Unit of bitwise operation, at least 8 bits.
pub type bits8 = u8;
/// Unit of bitwise operation, at least 16 bits.
pub type bits16 = u16;
/// Unit of bitwise operation, at least 32 bits.
pub type bits32 = u32;

pub const PG_INT8_MIN: i8 = i8::MIN;
pub const PG_INT8_MAX: i8 = i8::MAX;
pub const PG_UINT8_MAX: u8 = u8::MAX;
pub const PG_INT16_MIN: i16 = i16::MIN;
pub const PG_INT16_MAX: i16 = i16::MAX;
pub const PG_UINT16_MAX: u16 = u16::MAX;
pub const PG_INT32_MIN: i32 = i32::MIN;
pub const PG_INT32_MAX: i32 = i32::MAX;
pub const PG_UINT32_MAX: u32 = u32::MAX;
pub const PG_INT64_MIN: i64 = i64::MIN;
pub const PG_INT64_MAX: i64 = i64::MAX;
pub const PG_UINT64_MAX: u64 = u64::MAX;

/// 128-bit integers (target compilers support these natively).
pub type int128 = i128;
pub type uint128 = u128;

/// Size of any memory-resident object (C: `typedef size_t Size`).
pub type Size = usize;
/// Index into any memory-resident array (C: `typedef unsigned int Index`).
pub type Index = usize;
/// Offset into any memory-resident array; may be negative (C: `signed int Offset`).
pub type Offset = isize;

/// Common Postgres datatype name for `float` (catalog name `float4`).
pub type float4 = f32;
/// Common Postgres datatype name for `double` (catalog name `float8`).
pub type float8 = f64;

pub const FLOAT8PASSBYVAL: bool = true;

/// regproc is the catalog type name; `RegProcedure` is preferred in code.
pub type regproc = Oid;
pub type RegProcedure = regproc;

// Identifier domains are newtypes (not aliases) so they cannot implicitly convert
// to/from `u32` or each other; access the raw value via `.0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct TransactionId(pub u32);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct LocalTransactionId(pub u32);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct SubTransactionId(pub u32);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct MultiXactOffset(pub u32);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct CommandId(pub u32);

pub const InvalidSubTransactionId: SubTransactionId = SubTransactionId(0);
pub const TopSubTransactionId: SubTransactionId = SubTransactionId(1);

/// MultiXactId must be equivalent to TransactionId, to fit in xmax; it is the
/// SAME newtype (an alias to a newtype, so still no implicit `u32` conversion).
pub type MultiXactId = TransactionId;

pub const FirstCommandId: CommandId = CommandId(0);
pub const InvalidCommandId: CommandId = CommandId(u32::MAX);

// TransactionId well-known values (from transam.h, but conventionally needed
// alongside the c.h typedef; canonical export list places them here).
pub const InvalidTransactionId: TransactionId = TransactionId(0);
pub const BootstrapTransactionId: TransactionId = TransactionId(1);
pub const FrozenTransactionId: TransactionId = TransactionId(2);
pub const FirstNormalTransactionId: TransactionId = TransactionId(3);

pub const InvalidMultiXactId: MultiXactId = TransactionId(0);
pub const FirstMultiXactId: MultiXactId = TransactionId(1);

/// Variable-length datatype header (on-disk). The 4-byte length prefix is
/// followed by the data bytes as a flexible array member.
#[repr(C)]
pub struct varlena {
    /// Do not touch directly; use the VARSIZE/VARDATA accessors.
    pub vl_len_: [u8; 4],
    /// Data content (flexible array member); access via `dat()`.
    pub dat: [u8; 0],
}

impl varlena {
    /// Pointer to the data bytes following the 4-byte length header.
    /// SAFETY: `self` must point into a varlena buffer of its recorded length.
    pub fn dat(&self) -> *const u8 {
        self.dat.as_ptr()
    }
}

pub const VARHDRSZ: i32 = core::mem::size_of::<i32>() as i32;

/// SQL `bytea`: just a varlena header and data bytes.
pub type bytea = varlena;
/// SQL `text`: just a varlena header and data bytes.
pub type text = varlena;
/// Blank-padded char, SQL `char(n)`.
pub type BpChar = varlena;
/// Variable-length char, SQL `varchar(n)`.
pub type VarChar = varlena;

/// int2vector: physically an array, kept as a distinct catalog type (on-disk).
#[repr(C)]
pub struct int2vector {
    pub vl_len_: i32,
    pub ndim: i32,
    pub dataoffset: i32,
    pub elemtype: Oid,
    pub dim1: i32,
    pub lbound1: i32,
    pub values: [i16; 0],
}

/// oidvector: physically an array, kept as a distinct catalog type (on-disk).
#[repr(C)]
pub struct oidvector {
    pub vl_len_: i32,
    pub ndim: i32,
    pub dataoffset: i32,
    pub elemtype: Oid,
    pub dim1: i32,
    pub lbound1: i32,
    pub values: [Oid; 0],
}

/// A Name: a C string null-padded to exactly NAMEDATALEN bytes (on-disk).
#[repr(C)]
pub struct NameData {
    pub data: [u8; NAMEDATALEN],
}
const _: () = assert!(core::mem::size_of::<NameData>() == 64);

/// C: `typedef NameData *Name`.
pub type Name = *mut NameData; // TODO(ptr)

/// C: `#define NameStr(name) ((name).data)`.
pub fn NameStr(name: &NameData) -> &[u8] {
    &name.data
}

// === Section 4: IsValid macros for system types ===

#[allow(clippy::overly_complex_bool_expr, reason = "mirrors C macro; always-true is intentional type-check stub")]
pub fn BoolIsValid(boolean: bool) -> bool {
    !boolean || boolean
}

pub fn PointerIsValid<T>(pointer: *const T) -> bool {
    !pointer.is_null()
}

pub fn OidIsValid(object_id: Oid) -> bool {
    object_id != crate::postgres_ext::InvalidOid
}

pub fn RegProcedureIsValid(p: RegProcedure) -> bool {
    OidIsValid(p)
}

// === Section 5: lengthof, alignment ===

/// Round `len` up to a multiple of `alignval` (must be a power of two).
pub const fn TYPEALIGN(alignval: usize, len: usize) -> usize {
    (len + (alignval - 1)) & !(alignval - 1)
}

pub const fn SHORTALIGN(len: usize) -> usize {
    TYPEALIGN(ALIGNOF_SHORT, len)
}
pub const fn INTALIGN(len: usize) -> usize {
    TYPEALIGN(ALIGNOF_INT, len)
}
pub const fn LONGALIGN(len: usize) -> usize {
    TYPEALIGN(ALIGNOF_LONG, len)
}
pub const fn DOUBLEALIGN(len: usize) -> usize {
    TYPEALIGN(ALIGNOF_DOUBLE, len)
}
pub const fn MAXALIGN(len: usize) -> usize {
    TYPEALIGN(MAXIMUM_ALIGNOF, len)
}

/// Round `len` down to a multiple of `alignval` (must be a power of two).
pub const fn TYPEALIGN_DOWN(alignval: usize, len: usize) -> usize {
    len & !(alignval - 1)
}

pub const fn SHORTALIGN_DOWN(len: usize) -> usize {
    TYPEALIGN_DOWN(ALIGNOF_SHORT, len)
}
pub const fn INTALIGN_DOWN(len: usize) -> usize {
    TYPEALIGN_DOWN(ALIGNOF_INT, len)
}
pub const fn LONGALIGN_DOWN(len: usize) -> usize {
    TYPEALIGN_DOWN(ALIGNOF_LONG, len)
}
pub const fn DOUBLEALIGN_DOWN(len: usize) -> usize {
    TYPEALIGN_DOWN(ALIGNOF_DOUBLE, len)
}
pub const fn MAXALIGN_DOWN(len: usize) -> usize {
    TYPEALIGN_DOWN(MAXIMUM_ALIGNOF, len)
}

// === Section 6: assertions ===

/// C: `Assert(condition)` -> debug-only check.
#[inline]
pub fn Assert(condition: bool) {
    debug_assert!(condition);
}

/// C: `AssertMacro(condition)` -> debug-only check usable in expressions.
#[inline]
pub fn AssertMacro(condition: bool) {
    debug_assert!(condition);
}

// StaticAssertDecl/StaticAssertStmt/StaticAssertExpr -> `const _: () = assert!(...)`
// at the use site. AssertVariableIsOfType -> Rust's type system handles this.

/// C: `pg_noreturn extern void ExceptionalCondition(...)`. Handles a failed
/// Assert(): reports on stderr and aborts. Intentionally bypasses elog() to
/// minimize infrastructure needed to report an assertion failure.
pub use crate::backend::utils::error::assert::ExceptionalCondition;

// === Section 7: widely useful macros ===

pub fn Max<T: PartialOrd>(x: T, y: T) -> T {
    if x > y {
        x
    } else {
        y
    }
}

pub fn Min<T: PartialOrd>(x: T, y: T) -> T {
    if x < y {
        x
    } else {
        y
    }
}

// MemSet/MemSetAligned: zeroing optimizations -> use `*x = Default::default()` or
// `slice.fill(0)` in Rust. Dropped.

pub fn FLOAT4_FITS_IN_INT16(num: f32) -> bool {
    num >= f32::from(PG_INT16_MIN) && num < -f32::from(PG_INT16_MIN)
}
pub fn FLOAT4_FITS_IN_INT32(num: f32) -> bool {
    num >= (PG_INT32_MIN as f32) && num < -(PG_INT32_MIN as f32)
}
pub fn FLOAT4_FITS_IN_INT64(num: f32) -> bool {
    num >= (PG_INT64_MIN as f32) && num < -(PG_INT64_MIN as f32)
}
pub fn FLOAT8_FITS_IN_INT16(num: f64) -> bool {
    num >= f64::from(PG_INT16_MIN) && num < -f64::from(PG_INT16_MIN)
}
pub fn FLOAT8_FITS_IN_INT32(num: f64) -> bool {
    num >= f64::from(PG_INT32_MIN) && num < -f64::from(PG_INT32_MIN)
}
pub fn FLOAT8_FITS_IN_INT64(num: f64) -> bool {
    num >= (PG_INT64_MIN as f64) && num < -(PG_INT64_MIN as f64)
}

// === Section 8: random stuff ===

/// Invert a qsort-style comparison result, safe against `INT_MIN`.
pub fn INVERT_COMPARE_RESULT(var: i32) -> i32 {
    if var < 0 {
        1
    } else {
        -var
    }
}

pub const HIGHBIT: u8 = 0x80;

pub fn IS_HIGHBIT_SET(ch: u8) -> bool {
    ch & HIGHBIT != 0
}

pub fn SQL_STR_DOUBLE(ch: u8, escape_backslash: bool) -> bool {
    ch == b'\'' || (ch == b'\\' && escape_backslash)
}

pub const ESCAPE_STRING_SYNTAX: u8 = b'E';

pub const STATUS_OK: i32 = 0;
pub const STATUS_ERROR: i32 = -1;
pub const STATUS_EOF: i32 = -2;

// PGAlignedBlock / PGIOAlignedBlock / PGAlignedXLogBlock: alignment-forcing page
// buffers. In Rust, allocate `[u8; BLCKSZ]` with an explicit `#[repr(align(N))]`
// wrapper where I/O alignment is required (deferred to the buffer layer).

// === Section 9: system-specific hacks ===

/// On non-Windows targets `PG_BINARY` is 0 (no special open mode).
pub const PG_BINARY: i32 = 0;
pub const PG_BINARY_A: &str = "a";
pub const PG_BINARY_R: &str = "r";
pub const PG_BINARY_W: &str = "w";

// gettext/_/PG_TEXTDOMAIN, unconstify/unvolatize, strtoi64/i64abs, PGDLLIMPORT/
// EXPORT, SIGNAL_ARGS, sigsetjmp: C/NLS/platform plumbing replaced by Rust std.
