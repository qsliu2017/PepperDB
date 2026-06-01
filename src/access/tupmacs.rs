//! Translation of postgres/src/include/access/tupmacs.h
//!
//! Tuple macros used by both index tuples and heap tuples: null-bitmap test,
//! the byval/byref datum fetch/store helpers, and the attribute alignment /
//! length-advance helpers used when forming and deforming tuples.
//!
//! The C macros that take a `cur_offset` (a `uintptr_t` in C, sometimes a
//! `char *`) are rendered here as inline fns over `usize` offsets, which is how
//! heaptuple.c / indextuple.c use them.  The `attptr` arguments stay raw
//! pointers (`*const c_char`).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::varatt::{VARATT_IS_SHORT, VARATT_NOT_PAD_BYTE, VARSIZE_ANY};
use crate::c::{int16, int32, bits8, TYPEALIGN, INTALIGN, SHORTALIGN, DOUBLEALIGN};
use crate::postgres::{
    CharGetDatum, Int16GetDatum, Int32GetDatum, PointerGetDatum, DatumGetPointer,
    DatumGetChar, DatumGetInt16, DatumGetInt32,
};
use core::ffi::{c_char, c_int, c_void};

extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

// TYPALIGN_* come from catalog/pg_type_d.h (tupmacs.h #includes it for these).
// Defined module-locally for now (also locally defined in access/common/tupdesc.rs);
// TODO(pg-port): centralize the TYPALIGN_* consts in catalog/pg_type_d.rs.
pub const TYPALIGN_CHAR: c_char = b'c' as c_char;
pub const TYPALIGN_SHORT: c_char = b's' as c_char;
pub const TYPALIGN_INT: c_char = b'i' as c_char;
pub const TYPALIGN_DOUBLE: c_char = b'd' as c_char;

/*
 * Check a tuple's null bitmap to determine whether the attribute is null.
 * Note that a 0 in the null bitmap indicates a null, while 1 indicates
 * non-null.
 *
 * # Safety
 * `bits` must point to a null bitmap with at least (att >> 3) + 1 valid bytes.
 */
#[inline]
pub unsafe fn att_isnull(att: c_int, bits: *const bits8) -> bool {
    (*bits.add((att >> 3) as usize) & (1u8 << (att & 0x07))) == 0
}

/*
 * Given an attbyval and an attlen and a pointer into a tuple's data area,
 * return the correct value or pointer as a Datum.  If attbyval is false we
 * return the pointer unchanged; otherwise we fetch the value and extend it to
 * Datum form.  T must already be properly aligned.
 *
 * # Safety
 * For the byval cases, `T` must point to at least `attlen` readable, properly
 * aligned bytes.
 */
#[inline]
pub unsafe fn fetch_att(T: *const c_void, attbyval: bool, attlen: c_int) -> Datum {
    if attbyval {
        match attlen {
            1 => CharGetDatum(*(T as *const c_char)),
            2 => Int16GetDatum(*(T as *const int16)),
            4 => Int32GetDatum(*(T as *const int32)),
            // SIZEOF_DATUM == 8
            8 => *(T as *const Datum),
            _ => {
                ereport!(ERROR, errmsg!("unsupported byval length: {}", attlen));
                0
            }
        }
    } else {
        PointerGetDatum(T)
    }
}

/*
 * att_align_datum aligns the given offset as needed for a datum of alignment
 * requirement attalign and typlen attlen.  attdatum is only accessed when
 * dealing with a varlena type (to skip alignment of a short-header datum).
 */
#[inline]
pub unsafe fn att_align_datum(
    cur_offset: usize,
    attalign: c_char,
    attlen: c_int,
    attdatum: Datum,
) -> usize {
    if attlen == -1 && VARATT_IS_SHORT(DatumGetPointer(attdatum) as *const c_char) {
        cur_offset
    } else {
        att_align_nominal(cur_offset, attalign)
    }
}

/*
 * Similar to att_align_datum, but accepts a number of bytes (typically
 * CompactAttribute.attalignby) to align the Datum by.
 */
#[inline]
pub unsafe fn att_datum_alignby(
    cur_offset: usize,
    attalignby: u8,
    attlen: c_int,
    attdatum: Datum,
) -> usize {
    if attlen == -1 && VARATT_IS_SHORT(DatumGetPointer(attdatum) as *const c_char) {
        cur_offset
    } else {
        TYPEALIGN(attalignby as usize, cur_offset)
    }
}

/*
 * att_align_pointer performs the same calculation as att_align_datum, but is
 * used when walking a tuple: attptr is the current actual data pointer; for a
 * varlena field we "peek" to see if we're at a pad byte or a 1-byte-header
 * datum (a zero byte must be pad or the first byte of an aligned 4-byte length
 * word, so we can align; a non-zero byte needs no alignment).
 */
#[inline]
pub unsafe fn att_align_pointer(
    cur_offset: usize,
    attalign: c_char,
    attlen: c_int,
    attptr: *const c_char,
) -> usize {
    if attlen == -1 && VARATT_NOT_PAD_BYTE(attptr) {
        cur_offset
    } else {
        att_align_nominal(cur_offset, attalign)
    }
}

/*
 * Similar to att_align_pointer, but accepts a number of bytes to align by.
 */
#[inline]
pub unsafe fn att_pointer_alignby(
    cur_offset: usize,
    attalignby: u8,
    attlen: c_int,
    attptr: *const c_char,
) -> usize {
    if attlen == -1 && VARATT_NOT_PAD_BYTE(attptr) {
        cur_offset
    } else {
        TYPEALIGN(attalignby as usize, cur_offset)
    }
}

/*
 * att_align_nominal aligns the given offset as needed for a datum of alignment
 * requirement attalign, ignoring any consideration of packed varlena datums.
 * The attalign cases are tested in (hopefully) their frequency of occurrence.
 */
#[inline]
pub fn att_align_nominal(cur_offset: usize, attalign: c_char) -> usize {
    if attalign == TYPALIGN_INT {
        INTALIGN(cur_offset)
    } else if attalign == TYPALIGN_CHAR {
        cur_offset
    } else if attalign == TYPALIGN_DOUBLE {
        DOUBLEALIGN(cur_offset)
    } else {
        Assert!(attalign == TYPALIGN_SHORT);
        SHORTALIGN(cur_offset)
    }
}

/*
 * Similar to att_align_nominal, but accepts a number of bytes to align by.
 */
#[inline]
pub fn att_nominal_alignby(cur_offset: usize, attalignby: u8) -> usize {
    TYPEALIGN(attalignby as usize, cur_offset)
}

/*
 * att_addlength_datum increments the given offset by the space needed for the
 * given Datum.  attdatum is only accessed for a variable-length attribute.
 */
#[inline]
pub unsafe fn att_addlength_datum(cur_offset: usize, attlen: c_int, attdatum: Datum) -> usize {
    att_addlength_pointer(cur_offset, attlen, DatumGetPointer(attdatum) as *const c_char)
}

/*
 * att_addlength_pointer performs the same calculation as att_addlength_datum,
 * but is used when walking a tuple: attptr points to the field within the tuple.
 */
#[inline]
pub unsafe fn att_addlength_pointer(cur_offset: usize, attlen: c_int, attptr: *const c_char) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        cur_offset + VARSIZE_ANY(attptr) as usize
    } else {
        Assert!(attlen == -2);
        cur_offset + (strlen(attptr) + 1)
    }
}

/*
 * store_att_byval is a partial inverse of fetch_att: store a given byval Datum
 * value into a tuple data area at the specified address.
 *
 * # Safety
 * `T` must point to at least `attlen` writable, properly aligned bytes.
 */
#[inline]
pub unsafe fn store_att_byval(T: *mut c_void, newdatum: Datum, attlen: c_int) {
    match attlen {
        1 => *(T as *mut c_char) = DatumGetChar(newdatum),
        2 => *(T as *mut int16) = DatumGetInt16(newdatum),
        4 => *(T as *mut int32) = DatumGetInt32(newdatum),
        // SIZEOF_DATUM == 8
        8 => *(T as *mut Datum) = newdatum,
        _ => ereport!(ERROR, errmsg!("unsupported byval length: {}", attlen)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_bitmap_and_align() {
        unsafe {
            // bitmap: attr 0 non-null (bit set), attr 1 null (bit clear), attr 2 set.
            let bits: [bits8; 1] = [0b0000_0101];
            assert!(!att_isnull(0, bits.as_ptr())); // bit 0 set -> not null
            assert!(att_isnull(1, bits.as_ptr())); // bit 1 clear -> null
            assert!(!att_isnull(2, bits.as_ptr())); // bit 2 set -> not null
            assert!(att_isnull(3, bits.as_ptr())); // bit 3 clear -> null
        }
        // alignment: INT aligns to 4, DOUBLE to 8, SHORT to 2, CHAR is a no-op.
        assert_eq!(att_align_nominal(5, TYPALIGN_INT), 8);
        assert_eq!(att_align_nominal(5, TYPALIGN_DOUBLE), 8);
        assert_eq!(att_align_nominal(5, TYPALIGN_SHORT), 6);
        assert_eq!(att_align_nominal(5, TYPALIGN_CHAR), 5);
        assert_eq!(att_nominal_alignby(5, 4), 8);
    }

    #[test]
    fn byval_roundtrip() {
        unsafe {
            // store then fetch a 4-byte int through the byval path.
            let mut slot: i32 = 0;
            store_att_byval(
                &mut slot as *mut i32 as *mut c_void,
                Int32GetDatum(0x1234_5678),
                4,
            );
            assert_eq!(slot, 0x1234_5678);
            let d = fetch_att(&slot as *const i32 as *const c_void, true, 4);
            assert_eq!(DatumGetInt32(d), 0x1234_5678);
        }
    }

    #[test]
    fn addlength_fixed() {
        unsafe {
            // fixed-length attr: offset advances by attlen.
            assert_eq!(att_addlength_pointer(4, 8, core::ptr::null()), 12);
        }
    }
}
