//! Translation of postgres/src/include/varatt.h
//!
//! On-disk/in-memory layout of variable-length (varlena) datatypes and the VAR*
//! access macros.  The `struct varlena` / `text` / `bytea` typedefs and `VARHDRSZ`
//! itself live in c.rs (from c.h); this module adds the header-manipulation layer.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Only the LITTLE-ENDIAN macro variants are translated (the build targets x86_64/
//! aarch64).  The full TOAST machinery (varatt_external / compression / expanded
//! objects) is represented structurally but the size/format accessors for external
//! and compressed-in-line datums are largely TODO(pg-port) until access/common/
//! detoast.c + toast_internals land.  The macros are rendered as small `unsafe fn`s
//! taking the datum start pointer (the C macros' `PTR`).

use crate::c::{int32, uint32, uint8};
use crate::prelude::*;
use core::ffi::{c_char, c_void};

#[cfg(target_endian = "big")]
compile_error!("varatt.rs currently implements only the little-endian VAR* macro layout");

// VARHDRSZ (= 4) and `struct varlena`/`text`/`bytea` come from crate::c (c.h).
pub use crate::c::VARHDRSZ;

/* offsetof(varattrib_1b, va_data) and offsetof(varattrib_1b_e, va_data) */
pub const VARHDRSZ_SHORT: int32 = 1;
pub const VARHDRSZ_EXTERNAL: int32 = 2;

/// `varattrib_1b` - the short (1-byte header) varlena form.
#[repr(C)]
pub struct varattrib_1b {
    pub va_header: uint8,
    pub va_data: [c_char; FLEXIBLE_ARRAY_MEMBER],
}

/// `varattrib_1b_e` - a TOAST pointer (1-byte header tagged datum).
#[repr(C)]
pub struct varattrib_1b_e {
    pub va_header: uint8, /* Always 0x80 or 0x01 */
    pub va_tag: uint8,    /* Type of datum */
    pub va_data: [c_char; FLEXIBLE_ARRAY_MEMBER],
}

// ----------------------------------------------------------------
//   Endian-dependent internal macros (little-endian only)
// ----------------------------------------------------------------

#[inline]
unsafe fn header_byte(ptr: *const c_char) -> u8 {
    *(ptr as *const u8)
}

/// VARATT_IS_4B: a 4-byte-header (aligned) datum.
#[inline]
pub unsafe fn VARATT_IS_4B(ptr: *const c_char) -> bool {
    header_byte(ptr) & 0x01 == 0x00
}
/// VARATT_IS_4B_U: 4-byte header, uncompressed.
#[inline]
pub unsafe fn VARATT_IS_4B_U(ptr: *const c_char) -> bool {
    header_byte(ptr) & 0x03 == 0x00
}
/// VARATT_IS_4B_C: 4-byte header, compressed-in-line.
#[inline]
pub unsafe fn VARATT_IS_4B_C(ptr: *const c_char) -> bool {
    header_byte(ptr) & 0x03 == 0x02
}
/// VARATT_IS_1B: a 1-byte-header (unaligned, short) datum.
#[inline]
pub unsafe fn VARATT_IS_1B(ptr: *const c_char) -> bool {
    header_byte(ptr) & 0x01 == 0x01
}
/// VARATT_IS_1B_E: a TOAST pointer (external).
#[inline]
pub unsafe fn VARATT_IS_1B_E(ptr: *const c_char) -> bool {
    header_byte(ptr) == 0x01
}
/// VARATT_NOT_PAD_BYTE.
#[inline]
pub unsafe fn VARATT_NOT_PAD_BYTE(ptr: *const c_char) -> bool {
    header_byte(ptr) != 0
}

/// VARSIZE_4B - total size incl. header (only on known-aligned data).
#[inline]
pub unsafe fn VARSIZE_4B(ptr: *const c_char) -> uint32 {
    (core::ptr::read_unaligned(ptr as *const uint32) >> 2) & 0x3FFF_FFFF
}
/// VARSIZE_1B - total size incl. the 1-byte header.
#[inline]
pub unsafe fn VARSIZE_1B(ptr: *const c_char) -> uint32 {
    ((header_byte(ptr) >> 1) & 0x7F) as uint32
}
/// VARTAG_1B_E - the TOAST-pointer tag.
#[inline]
pub unsafe fn VARTAG_1B_E(ptr: *const c_char) -> uint8 {
    (*(ptr as *const varattrib_1b_e)).va_tag
}

/// SET_VARSIZE_4B.
#[inline]
pub unsafe fn SET_VARSIZE_4B(ptr: *mut c_char, len: int32) {
    core::ptr::write_unaligned(ptr as *mut uint32, (len as uint32) << 2);
}
/// SET_VARSIZE_4B_C (compressed-in-line).
#[inline]
pub unsafe fn SET_VARSIZE_4B_C(ptr: *mut c_char, len: int32) {
    core::ptr::write_unaligned(ptr as *mut uint32, ((len as uint32) << 2) | 0x02);
}
/// SET_VARSIZE_1B (short header).
#[inline]
pub unsafe fn SET_VARSIZE_1B(ptr: *mut c_char, len: int32) {
    *(ptr as *mut uint8) = ((len as uint8) << 1) | 0x01;
}

/// VARDATA_4B - pointer to the data of a 4-byte-header datum (offset 4).
#[inline]
pub unsafe fn VARDATA_4B(ptr: *const c_char) -> *mut c_char {
    ptr.add(VARHDRSZ as usize) as *mut c_char
}
/// VARDATA_1B - pointer to the data of a 1-byte-header datum (offset 1).
#[inline]
pub unsafe fn VARDATA_1B(ptr: *const c_char) -> *mut c_char {
    ptr.add(VARHDRSZ_SHORT as usize) as *mut c_char
}
/// VARDATA_1B_E - data of a TOAST pointer (offset 2).
#[inline]
pub unsafe fn VARDATA_1B_E(ptr: *const c_char) -> *mut c_char {
    ptr.add(VARHDRSZ_EXTERNAL as usize) as *mut c_char
}

// ----------------------------------------------------------------
//   Externally visible macros
// ----------------------------------------------------------------

/// VARDATA(PTR) - data pointer for an assembled (4B) datum.
#[inline]
pub unsafe fn VARDATA(ptr: *const c_char) -> *mut c_char {
    VARDATA_4B(ptr)
}
/// VARSIZE(PTR) - total size of an assembled (4B) datum.
#[inline]
pub unsafe fn VARSIZE(ptr: *const c_char) -> uint32 {
    VARSIZE_4B(ptr)
}
/// SET_VARSIZE(PTR, len).
#[inline]
pub unsafe fn SET_VARSIZE(ptr: *mut c_char, len: int32) {
    SET_VARSIZE_4B(ptr, len);
}
/// SET_VARSIZE_SHORT(PTR, len).
#[inline]
pub unsafe fn SET_VARSIZE_SHORT(ptr: *mut c_char, len: int32) {
    SET_VARSIZE_1B(ptr, len);
}
/// SET_VARSIZE_COMPRESSED(PTR, len): set a 4-byte compressed-in-line header.
#[inline]
pub unsafe fn SET_VARSIZE_COMPRESSED(ptr: *mut c_char, len: int32) {
    SET_VARSIZE_4B_C(ptr, len);
}

#[inline]
pub unsafe fn VARATT_IS_COMPRESSED(ptr: *const c_char) -> bool {
    VARATT_IS_4B_C(ptr)
}
#[inline]
pub unsafe fn VARATT_IS_EXTERNAL(ptr: *const c_char) -> bool {
    VARATT_IS_1B_E(ptr)
}

/* vartag_external values (the TOAST-pointer kinds). */
pub const VARTAG_INDIRECT: u8 = 1;
pub const VARTAG_EXPANDED_RO: u8 = 2;
pub const VARTAG_EXPANDED_RW: u8 = 3;
pub const VARTAG_ONDISK: u8 = 18;

#[inline]
pub fn VARTAG_IS_EXPANDED(tag: u8) -> bool {
    tag == VARTAG_EXPANDED_RO || tag == VARTAG_EXPANDED_RW
}
#[inline]
pub unsafe fn VARATT_IS_EXTERNAL_EXPANDED(ptr: *const c_char) -> bool {
    VARATT_IS_EXTERNAL(ptr) && VARTAG_IS_EXPANDED(VARTAG_1B_E(ptr))
}
#[inline]
pub unsafe fn VARATT_IS_EXTERNAL_EXPANDED_RW(ptr: *const c_char) -> bool {
    VARATT_IS_EXTERNAL(ptr) && VARTAG_1B_E(ptr) == VARTAG_EXPANDED_RW
}
#[inline]
pub unsafe fn VARATT_IS_SHORT(ptr: *const c_char) -> bool {
    VARATT_IS_1B(ptr)
}
#[inline]
pub unsafe fn VARATT_IS_EXTENDED(ptr: *const c_char) -> bool {
    !VARATT_IS_4B_U(ptr)
}

/// VARSIZE_ANY - total datum size for any in-line form.
///
/// External (TOAST-pointer) datums require the VARTAG_SIZE table (toast_internals),
/// not yet ported -> TODO(pg-port).
#[inline]
pub unsafe fn VARSIZE_ANY(ptr: *const c_char) -> uint32 {
    if VARATT_IS_1B_E(ptr) {
        unimplemented!("VARSIZE_ANY on external TOAST pointer: VARTAG_SIZE not yet translated")
    } else if VARATT_IS_1B(ptr) {
        VARSIZE_1B(ptr)
    } else {
        VARSIZE_4B(ptr)
    }
}

/// VARSIZE_ANY_EXHDR - data size (excluding header) for any in-line form.
#[inline]
pub unsafe fn VARSIZE_ANY_EXHDR(ptr: *const c_char) -> uint32 {
    if VARATT_IS_1B_E(ptr) {
        unimplemented!("VARSIZE_ANY_EXHDR on external TOAST pointer: VARTAG_SIZE not yet translated")
    } else if VARATT_IS_1B(ptr) {
        VARSIZE_1B(ptr) - VARHDRSZ_SHORT as uint32
    } else {
        VARSIZE_4B(ptr) - VARHDRSZ as uint32
    }
}

/// VARDATA_ANY - data pointer for any in-line (non-external, non-compressed) form.
/// (Will not work on an external or compressed-in-line Datum; may be unaligned.)
#[inline]
pub unsafe fn VARDATA_ANY(ptr: *const c_char) -> *mut c_char {
    if VARATT_IS_1B(ptr) {
        VARDATA_1B(ptr)
    } else {
        VARDATA_4B(ptr)
    }
}

/*
 * pg_detoast_datum_packed (access/common/detoast.c): for the common in-memory case
 * of a plain (4B-uncompressed or short-1B) datum this is the identity.  Compressed
 * or external datums require the TOAST fetch/decompress path, not yet translated.
 *
 * # Safety
 * `datum` points to a valid varlena.
 */
pub unsafe fn pg_detoast_datum_packed(datum: *mut c_void) -> *mut c_void {
    crate::access::common::detoast::pg_detoast_datum_packed(datum as _) as _
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varlena_header_roundtrip() {
        unsafe {
            // Build a 4-byte-header datum holding 5 bytes of payload.
            let total = VARHDRSZ as usize + 5;
            let p = palloc(total) as *mut c_char;
            SET_VARSIZE(p, total as int32);
            let data = VARDATA(p);
            for i in 0..5 {
                *data.add(i) = (b'a' + i as u8) as c_char;
            }

            assert!(VARATT_IS_4B(p));
            assert!(VARATT_IS_4B_U(p));
            assert!(!VARATT_IS_1B(p));
            assert!(!VARATT_IS_COMPRESSED(p));
            assert!(!VARATT_IS_EXTERNAL(p));
            assert!(!VARATT_IS_EXTENDED(p));
            assert_eq!(VARSIZE(p) as usize, total);
            assert_eq!(VARSIZE_ANY(p) as usize, total);
            assert_eq!(VARSIZE_ANY_EXHDR(p) as usize, 5);
            // VARDATA points 4 bytes in; payload intact
            assert_eq!(VARDATA_ANY(p), data);
            let slice = core::slice::from_raw_parts(VARDATA_ANY(p) as *const u8, 5);
            assert_eq!(slice, b"abcde");
            pfree(p as *mut c_void);

            // Short (1-byte header) datum holding 3 bytes.
            let q = palloc(VARHDRSZ_SHORT as usize + 3) as *mut c_char;
            SET_VARSIZE_SHORT(q, (VARHDRSZ_SHORT as usize + 3) as int32);
            assert!(VARATT_IS_1B(q));
            assert!(VARATT_IS_SHORT(q));
            assert!(!VARATT_IS_4B(q));
            assert!(VARATT_IS_EXTENDED(q));
            assert_eq!(VARSIZE_1B(q) as usize, VARHDRSZ_SHORT as usize + 3);
            assert_eq!(VARSIZE_ANY_EXHDR(q) as usize, 3);
            assert_eq!(VARDATA_ANY(q), VARDATA_1B(q));
            pfree(q as *mut c_void);
        }
    }
}
