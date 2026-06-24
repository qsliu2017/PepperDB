//! Translated from PostgreSQL src/include/utils/arrayaccess.h

use crate::access::tupmacs::{att_addlength_pointer, att_align_nominal, fetch_att};
use crate::postgres::Datum;
use crate::utils::array::AnyArrayType;

/// State for element-by-element iteration over a flat or expanded array.
///
/// `datumptr.is_none()` distinguishes flat from expanded: when set, we read the
/// already-deconstructed Datum/isnull arrays; otherwise we walk the flat data
/// area with a nulls bitmap. Raw pointers mirror the C struct's view into the
/// array buffer. TODO(ptr): provenance is the source array, kept alive by caller.
pub struct ArrayIter {
    // Fields used when we have an expanded array.
    pub datumptr: *mut Datum, // None == null pointer
    pub isnullptr: *mut bool,
    // Fields used when we have a flat array.
    pub dataptr: *mut u8,    // current spot in the data area
    pub bitmapptr: *mut u8,  // current byte of nulls bitmap, or null
    pub bitmask: i32,        // mask for current bit in nulls bitmap
}

/// array_iter_setup: prepare to iterate over `a`.
///
/// SAFETY: `a` must reference a valid flat/expanded array for the iterator's life.
/// TODO(ptr): ARR_DATA_PTR/ARR_NULLBITMAP/expanded-header access not yet ported in
/// crate::utils::array; wiring deferred to Phase 2.
pub unsafe fn array_iter_setup(_a: &AnyArrayType) -> ArrayIter {
    unimplemented!()
}

/// array_iter_next: fetch element `i` (zero-origin) into (datum, isnull).
///
/// Elements can only be fetched sequentially despite the explicit index.
/// SAFETY: `it` must come from array_iter_setup on a still-live array.
pub unsafe fn array_iter_next(
    it: &mut ArrayIter,
    i: usize,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
) -> (Datum, bool) {
    if !it.datumptr.is_null() {
        let ret = unsafe { *it.datumptr.add(i) };
        let isnull = if it.isnullptr.is_null() {
            false
        } else {
            unsafe { *it.isnullptr.add(i) }
        };
        (ret, isnull)
    } else {
        let (ret, isnull);
        if !it.bitmapptr.is_null() && (unsafe { *it.bitmapptr } as i32 & it.bitmask) == 0 {
            isnull = true;
            ret = Datum(0);
        } else {
            isnull = false;
            ret = unsafe { fetch_att(it.dataptr, elmbyval, elmlen) };
            // Advance dataptr by element length, then re-align, via offset helpers.
            let len = unsafe { att_addlength_pointer(0, elmlen, it.dataptr) };
            it.dataptr = unsafe { it.dataptr.add(len) };
            let pad = att_align_nominal(it.dataptr as usize, elmalign) - it.dataptr as usize;
            it.dataptr = unsafe { it.dataptr.add(pad) };
        }
        it.bitmask <<= 1;
        if it.bitmask == 0x100 {
            if !it.bitmapptr.is_null() {
                it.bitmapptr = unsafe { it.bitmapptr.add(1) };
            }
            it.bitmask = 1;
        }
        (ret, isnull)
    }
}
