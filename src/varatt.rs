//! Translated from PostgreSQL src/include/varatt.h
//!
//! ON-DISK: variable-length datatype (TOAST) layouts. Target is 64-bit
//! little-endian only, so only the `!WORDS_BIGENDIAN` branch is translated. The
//! varlena headers are bit-packed length+flag words, so per the rules they are
//! accessed through accessor functions over raw `*const u8`/`*mut u8` (the C
//! `PTR`) rather than as `#[repr(C)]` bitfields. The fixed TOAST-pointer structs
//! keep `#[repr(C)]` with layout asserts.

use crate::postgres_ext::Oid;

/// Traditional out-of-line TOAST pointer. No padding (compared via memcmp).
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct varatt_external {
    pub va_rawsize: i32,    // original data size (includes header)
    pub va_extinfo: u32,    // external saved size (w/o header) + compression method
    pub va_valueid: Oid,    // unique ID of value within TOAST table
    pub va_toastrelid: Oid, // RelID of TOAST table containing it
}
const _: () = assert!(core::mem::size_of::<varatt_external>() == 16);

// "Saved size" portion of va_extinfo; the two high bits select compression.
pub const VARLENA_EXTSIZE_BITS: u32 = 30;
pub const VARLENA_EXTSIZE_MASK: u32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;

/// In-memory ("indirect") TOAST pointer.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct varatt_indirect {
    pub pointer: *mut varlena, // pointer to in-memory varlena
}

/// Forward reference: real definition in utils/expandeddatum.h.
// TODO(struct-forward): repoint to crate::utils::expandeddatum::ExpandedObjectHeader in Phase 2
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::expandeddatum in Phase 2")]
#[repr(C)]
pub struct ExpandedObjectHeader {
    _private: [u8; 0],
}

/// Expanded ("in-memory, type-specific format") TOAST pointer.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[allow(deprecated)]
pub struct varatt_expanded {
    pub eohptr: *mut ExpandedObjectHeader,
}

/// The varlena type itself (opaque header; the bytes live in a buffer).
#[repr(C)]
pub struct varlena {
    _private: [u8; 0],
}

/// Type tag for the various sorts of TOAST pointer datums.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum vartag_external {
    VARTAG_INDIRECT = 1,
    VARTAG_EXPANDED_RO = 2,
    VARTAG_EXPANDED_RW = 3,
    VARTAG_ONDISK = 18,
}
pub use vartag_external::*;

/// This test relies on the specific tag values above.
#[inline]
pub const fn VARTAG_IS_EXPANDED(tag: u8) -> bool {
    (tag & !1) == vartag_external::VARTAG_EXPANDED_RO as u8
}

/// Size of a TOAST pointer's payload by tag.
#[inline]
pub fn VARTAG_SIZE(tag: u8) -> usize {
    if tag == vartag_external::VARTAG_INDIRECT as u8 {
        core::mem::size_of::<varatt_indirect>()
    } else if VARTAG_IS_EXPANDED(tag) {
        core::mem::size_of::<varatt_expanded>()
    } else if tag == vartag_external::VARTAG_ONDISK as u8 {
        core::mem::size_of::<varatt_external>()
    } else {
        0
    }
}

// Header-byte offsets within the varlena buffer (little-endian target).
// va_header is the first byte/word; va_data follows the fixed header.

/// VARHDRSZ: size of a normal 4-byte varlena header.
pub const VARHDRSZ: usize = core::mem::size_of::<u32>();
/// offsetof(varattrib_1b_e, va_data): 1-byte header + 1-byte tag.
pub const VARHDRSZ_EXTERNAL: usize = 2;
/// offsetof(varattrib_4b, va_compressed.va_data): two uint32 words.
pub const VARHDRSZ_COMPRESSED: usize = 2 * core::mem::size_of::<u32>();
/// offsetof(varattrib_1b, va_data): single 1-byte header.
pub const VARHDRSZ_SHORT: usize = 1;

pub const VARATT_SHORT_MAX: usize = 0x7F;

// --- internal endian-dependent accessors (little-endian only) --------------
// All take the raw datum pointer (`PTR`) the C macros operate on.

/// SAFETY: `ptr` points at the start of a valid varlena.
#[inline]
pub unsafe fn VARATT_IS_4B(ptr: *const u8) -> bool {
    (*ptr & 0x01) == 0x00
}
/// SAFETY: see `VARATT_IS_4B`.
#[inline]
pub unsafe fn VARATT_IS_4B_U(ptr: *const u8) -> bool {
    (*ptr & 0x03) == 0x00
}
/// SAFETY: see `VARATT_IS_4B`.
#[inline]
pub unsafe fn VARATT_IS_4B_C(ptr: *const u8) -> bool {
    (*ptr & 0x03) == 0x02
}
/// SAFETY: see `VARATT_IS_4B`.
#[inline]
pub unsafe fn VARATT_IS_1B(ptr: *const u8) -> bool {
    (*ptr & 0x01) == 0x01
}
/// SAFETY: see `VARATT_IS_4B`.
#[inline]
pub unsafe fn VARATT_IS_1B_E(ptr: *const u8) -> bool {
    *ptr == 0x01
}
/// SAFETY: see `VARATT_IS_4B`.
#[inline]
pub unsafe fn VARATT_NOT_PAD_BYTE(ptr: *const u8) -> bool {
    *ptr != 0
}

/// VARSIZE_4B: only valid on known-aligned data.
/// SAFETY: `ptr` points at an aligned 4-byte-header varlena.
#[inline]
pub unsafe fn VARSIZE_4B(ptr: *const u8) -> u32 {
    let hdr = (ptr as *const u32).read_unaligned();
    (hdr >> 2) & 0x3FFFFFFF
}
/// SAFETY: see `VARATT_IS_4B`.
#[inline]
pub unsafe fn VARSIZE_1B(ptr: *const u8) -> u32 {
    ((*ptr >> 1) & 0x7F) as u32
}
/// SAFETY: `ptr` points at a 1-byte-header external (toast) varlena.
#[inline]
pub unsafe fn VARTAG_1B_E(ptr: *const u8) -> u8 {
    *ptr.add(1)
}

/// SAFETY: `ptr` points at writable storage for a 4-byte-header varlena.
#[inline]
pub unsafe fn SET_VARSIZE_4B(ptr: *mut u8, len: u32) {
    (ptr as *mut u32).write_unaligned(len << 2);
}
/// SAFETY: see `SET_VARSIZE_4B`.
#[inline]
pub unsafe fn SET_VARSIZE_4B_C(ptr: *mut u8, len: u32) {
    (ptr as *mut u32).write_unaligned((len << 2) | 0x02);
}
/// SAFETY: `ptr` points at writable storage for a 1-byte-header varlena.
#[inline]
pub unsafe fn SET_VARSIZE_1B(ptr: *mut u8, len: u8) {
    *ptr = (len << 1) | 0x01;
}
/// SAFETY: `ptr` points at writable storage for a 1-byte external varlena.
#[inline]
pub unsafe fn SET_VARTAG_1B_E(ptr: *mut u8, tag: u8) {
    *ptr = 0x01;
    *ptr.add(1) = tag;
}

/// VARDATA_4B: data after a normal 4-byte header.
/// SAFETY: `ptr` points at a 4-byte-header varlena.
#[inline]
pub unsafe fn VARDATA_4B(ptr: *mut u8) -> *mut u8 {
    ptr.add(VARHDRSZ)
}
/// SAFETY: `ptr` points at a compressed 4-byte-header varlena.
#[inline]
pub unsafe fn VARDATA_4B_C(ptr: *mut u8) -> *mut u8 {
    ptr.add(VARHDRSZ_COMPRESSED)
}
/// SAFETY: `ptr` points at a 1-byte-header varlena.
#[inline]
pub unsafe fn VARDATA_1B(ptr: *mut u8) -> *mut u8 {
    ptr.add(VARHDRSZ_SHORT)
}
/// SAFETY: `ptr` points at a 1-byte external varlena.
#[inline]
pub unsafe fn VARDATA_1B_E(ptr: *mut u8) -> *mut u8 {
    ptr.add(VARHDRSZ_EXTERNAL)
}

// --- externally visible TOAST accessors ------------------------------------

/// SAFETY: `ptr` points at an aligned 4-byte-header varlena.
#[inline]
pub unsafe fn VARDATA(ptr: *mut u8) -> *mut u8 {
    VARDATA_4B(ptr)
}
/// SAFETY: `ptr` points at an aligned 4-byte-header varlena.
#[inline]
pub unsafe fn VARSIZE(ptr: *const u8) -> u32 {
    VARSIZE_4B(ptr)
}

/// SAFETY: `ptr` points at a 1-byte-header varlena.
#[inline]
pub unsafe fn VARSIZE_SHORT(ptr: *const u8) -> u32 {
    VARSIZE_1B(ptr)
}
/// SAFETY: `ptr` points at a 1-byte-header varlena.
#[inline]
pub unsafe fn VARDATA_SHORT(ptr: *mut u8) -> *mut u8 {
    VARDATA_1B(ptr)
}

/// SAFETY: `ptr` points at a 1-byte external varlena.
#[inline]
pub unsafe fn VARTAG_EXTERNAL(ptr: *const u8) -> u8 {
    VARTAG_1B_E(ptr)
}
/// SAFETY: see `VARTAG_EXTERNAL`.
#[inline]
pub unsafe fn VARSIZE_EXTERNAL(ptr: *const u8) -> usize {
    VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(ptr))
}
/// SAFETY: see `VARTAG_EXTERNAL`.
#[inline]
pub unsafe fn VARDATA_EXTERNAL(ptr: *mut u8) -> *mut u8 {
    VARDATA_1B_E(ptr)
}

/// SAFETY: `ptr` points at a valid varlena.
#[inline]
pub unsafe fn VARATT_IS_COMPRESSED(ptr: *const u8) -> bool {
    VARATT_IS_4B_C(ptr)
}
/// SAFETY: see `VARATT_IS_COMPRESSED`.
#[inline]
pub unsafe fn VARATT_IS_EXTERNAL(ptr: *const u8) -> bool {
    VARATT_IS_1B_E(ptr)
}
/// SAFETY: see `VARATT_IS_COMPRESSED`.
#[inline]
pub unsafe fn VARATT_IS_EXTERNAL_ONDISK(ptr: *const u8) -> bool {
    VARATT_IS_EXTERNAL(ptr) && VARTAG_EXTERNAL(ptr) == vartag_external::VARTAG_ONDISK as u8
}
/// SAFETY: see `VARATT_IS_COMPRESSED`.
#[inline]
pub unsafe fn VARATT_IS_EXTERNAL_INDIRECT(ptr: *const u8) -> bool {
    VARATT_IS_EXTERNAL(ptr) && VARTAG_EXTERNAL(ptr) == vartag_external::VARTAG_INDIRECT as u8
}
/// SAFETY: see `VARATT_IS_COMPRESSED`.
#[inline]
pub unsafe fn VARATT_IS_EXTERNAL_EXPANDED_RO(ptr: *const u8) -> bool {
    VARATT_IS_EXTERNAL(ptr) && VARTAG_EXTERNAL(ptr) == vartag_external::VARTAG_EXPANDED_RO as u8
}
/// SAFETY: see `VARATT_IS_COMPRESSED`.
#[inline]
pub unsafe fn VARATT_IS_EXTERNAL_EXPANDED_RW(ptr: *const u8) -> bool {
    VARATT_IS_EXTERNAL(ptr) && VARTAG_EXTERNAL(ptr) == vartag_external::VARTAG_EXPANDED_RW as u8
}
/// SAFETY: see `VARATT_IS_COMPRESSED`.
#[inline]
pub unsafe fn VARATT_IS_EXTERNAL_EXPANDED(ptr: *const u8) -> bool {
    VARATT_IS_EXTERNAL(ptr) && VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(ptr))
}
/// SAFETY: see `VARATT_IS_COMPRESSED`.
#[inline]
pub unsafe fn VARATT_IS_EXTERNAL_NON_EXPANDED(ptr: *const u8) -> bool {
    VARATT_IS_EXTERNAL(ptr) && !VARTAG_IS_EXPANDED(VARTAG_EXTERNAL(ptr))
}
/// SAFETY: see `VARATT_IS_COMPRESSED`.
#[inline]
pub unsafe fn VARATT_IS_SHORT(ptr: *const u8) -> bool {
    VARATT_IS_1B(ptr)
}
/// SAFETY: see `VARATT_IS_COMPRESSED`.
#[inline]
pub unsafe fn VARATT_IS_EXTENDED(ptr: *const u8) -> bool {
    !VARATT_IS_4B_U(ptr)
}

/// VARATT_CAN_MAKE_SHORT: whether a 4-byte uncompressed datum fits a short header.
/// SAFETY: `ptr` points at an aligned 4-byte-header varlena.
#[inline]
pub unsafe fn VARATT_CAN_MAKE_SHORT(ptr: *const u8) -> bool {
    VARATT_IS_4B_U(ptr)
        && (VARSIZE(ptr) as usize - VARHDRSZ + VARHDRSZ_SHORT) <= VARATT_SHORT_MAX
}
/// SAFETY: see `VARATT_CAN_MAKE_SHORT`.
#[inline]
pub unsafe fn VARATT_CONVERTED_SHORT_SIZE(ptr: *const u8) -> usize {
    VARSIZE(ptr) as usize - VARHDRSZ + VARHDRSZ_SHORT
}

/// SAFETY: `ptr` points at writable aligned storage.
#[inline]
pub unsafe fn SET_VARSIZE(ptr: *mut u8, len: u32) {
    SET_VARSIZE_4B(ptr, len)
}
/// SAFETY: `ptr` points at writable storage.
#[inline]
pub unsafe fn SET_VARSIZE_SHORT(ptr: *mut u8, len: u8) {
    SET_VARSIZE_1B(ptr, len)
}
/// SAFETY: `ptr` points at writable aligned storage.
#[inline]
pub unsafe fn SET_VARSIZE_COMPRESSED(ptr: *mut u8, len: u32) {
    SET_VARSIZE_4B_C(ptr, len)
}
/// SAFETY: `ptr` points at writable storage for an external varlena.
#[inline]
pub unsafe fn SET_VARTAG_EXTERNAL(ptr: *mut u8, tag: u8) {
    SET_VARTAG_1B_E(ptr, tag)
}

/// VARSIZE_ANY: total size of any varlena form.
/// SAFETY: `ptr` points at a valid varlena.
#[inline]
pub unsafe fn VARSIZE_ANY(ptr: *const u8) -> usize {
    if VARATT_IS_1B_E(ptr) {
        VARSIZE_EXTERNAL(ptr)
    } else if VARATT_IS_1B(ptr) {
        VARSIZE_1B(ptr) as usize
    } else {
        VARSIZE_4B(ptr) as usize
    }
}

/// VARSIZE_ANY_EXHDR: payload size of any varlena form (excluding header).
/// SAFETY: `ptr` points at a valid varlena.
#[inline]
pub unsafe fn VARSIZE_ANY_EXHDR(ptr: *const u8) -> usize {
    if VARATT_IS_1B_E(ptr) {
        VARSIZE_EXTERNAL(ptr) - VARHDRSZ_EXTERNAL
    } else if VARATT_IS_1B(ptr) {
        VARSIZE_1B(ptr) as usize - VARHDRSZ_SHORT
    } else {
        VARSIZE_4B(ptr) as usize - VARHDRSZ
    }
}

/// VARDATA_ANY: payload pointer (may be unaligned; not for external/compressed).
/// SAFETY: `ptr` points at a non-external, non-compressed varlena.
#[inline]
pub unsafe fn VARDATA_ANY(ptr: *mut u8) -> *mut u8 {
    if VARATT_IS_1B(ptr) {
        VARDATA_1B(ptr)
    } else {
        VARDATA_4B(ptr)
    }
}

/// Decompressed size of a compressed-in-line datum.
/// SAFETY: `ptr` points at a compressed 4-byte-header varlena.
#[inline]
pub unsafe fn VARDATA_COMPRESSED_GET_EXTSIZE(ptr: *const u8) -> u32 {
    let tcinfo = (ptr.add(VARHDRSZ) as *const u32).read_unaligned();
    tcinfo & VARLENA_EXTSIZE_MASK
}
/// SAFETY: see `VARDATA_COMPRESSED_GET_EXTSIZE`.
#[inline]
pub unsafe fn VARDATA_COMPRESSED_GET_COMPRESS_METHOD(ptr: *const u8) -> u32 {
    let tcinfo = (ptr.add(VARHDRSZ) as *const u32).read_unaligned();
    tcinfo >> VARLENA_EXTSIZE_BITS
}

/// Saved external size of a TOAST pointer (argument is a `varatt_external`).
#[inline]
pub fn VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer: varatt_external) -> u32 {
    toast_pointer.va_extinfo & VARLENA_EXTSIZE_MASK
}
/// Compression method of a TOAST pointer.
#[inline]
pub fn VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer: varatt_external) -> u32 {
    toast_pointer.va_extinfo >> VARLENA_EXTSIZE_BITS
}

/// Pack saved size and compression method into a TOAST pointer.
#[inline]
pub fn VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(
    toast_pointer: &mut varatt_external,
    len: u32,
    cm: u32,
) {
    toast_pointer.va_extinfo = len | (cm << VARLENA_EXTSIZE_BITS);
}

/// Whether an externally-stored value is compressed.
#[inline]
pub fn VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer: varatt_external) -> bool {
    let rawsize = toast_pointer.va_rawsize;
    VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) < (rawsize as u32).wrapping_sub(VARHDRSZ as u32)
}
