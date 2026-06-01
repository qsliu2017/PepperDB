//! utils/arrayaccess.h - element-by-element access to Postgres arrays.

use std::ffi::{c_char, c_int, c_void};

use crate::access::tupmacs::{att_addlength_pointer, att_align_nominal, fetch_att};
use crate::c::bits8;
use crate::postgres::Datum;
use crate::utils::adt::array_expanded::AnyArrayType;
use crate::utils::adt::expandeddatum::VARATT_IS_EXPANDED_HEADER;
use crate::utils::array::{ArrayType, ARR_DATA_PTR, ARR_NULLBITMAP};

/*
 * Functions for iterating through elements of a flat or expanded array.
 * These require a state struct "array_iter iter".
 *
 * Use "array_iter_setup(&iter, arrayptr);" to prepare to iterate, and
 * "datumvar = array_iter_next(&iter, &isnullvar, index, ...);" to fetch
 * the next element into datumvar/isnullvar.
 * "index" must be the zero-origin element number; we make caller provide
 * this since caller is generally counting the elements anyway.  Despite
 * that, these functions can only fetch elements sequentially.
 */

#[repr(C)]
pub struct array_iter {
    /* datumptr being NULL or not tells if we have flat or expanded array */

    /* Fields used when we have an expanded array */
    /// Pointer to Datum array
    pub datumptr: *mut Datum,
    /// Pointer to isnull array
    pub isnullptr: *mut bool,

    /* Fields used when we have a flat array */
    /// Current spot in the data area
    pub dataptr: *mut c_char,
    /// Current byte of the nulls bitmap, or NULL
    pub bitmapptr: *mut bits8,
    /// mask for current bit in nulls bitmap
    pub bitmask: c_int,
}

#[inline]
pub unsafe fn array_iter_setup(it: *mut array_iter, a: *mut AnyArrayType) {
    if VARATT_IS_EXPANDED_HEADER(a as *const c_void) {
        // The `xpn` union variant is an ExpandedArrayHeader at offset 0; read its
        // fields via a struct pointer to avoid the implicit_unsafe_autorefs lint
        // that fires on union-member access through a raw-pointer deref.
        let xpn = a as *mut crate::utils::adt::array_expanded::ExpandedArrayHeader;
        let dvalues = (*xpn).dvalues;
        let dnulls = (*xpn).dnulls;
        let fvalue = (*xpn).fvalue;
        if !dvalues.is_null() {
            (*it).datumptr = dvalues;
            (*it).isnullptr = dnulls;
            /* we must fill all fields to prevent compiler warnings */
            (*it).dataptr = core::ptr::null_mut();
            (*it).bitmapptr = core::ptr::null_mut();
        } else {
            /* Work with flat array embedded in the expanded datum */
            (*it).datumptr = core::ptr::null_mut();
            (*it).isnullptr = core::ptr::null_mut();
            (*it).dataptr = ARR_DATA_PTR(fvalue);
            (*it).bitmapptr = ARR_NULLBITMAP(fvalue);
        }
    } else {
        (*it).datumptr = core::ptr::null_mut();
        (*it).isnullptr = core::ptr::null_mut();
        (*it).dataptr = ARR_DATA_PTR(a as *const ArrayType);
        (*it).bitmapptr = ARR_NULLBITMAP(a as *const ArrayType);
    }
    (*it).bitmask = 1;
}

#[inline]
pub unsafe fn array_iter_next(
    it: *mut array_iter,
    isnull: *mut bool,
    i: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
) -> Datum {
    let ret: Datum;

    if !(*it).datumptr.is_null() {
        ret = *(*it).datumptr.add(i as usize);
        *isnull = if !(*it).isnullptr.is_null() {
            *(*it).isnullptr.add(i as usize)
        } else {
            false
        };
    } else {
        if !(*it).bitmapptr.is_null() && (*(*it).bitmapptr & (*it).bitmask as bits8) == 0 {
            *isnull = true;
            ret = 0 as Datum;
        } else {
            *isnull = false;
            ret = fetch_att((*it).dataptr as *const c_void, elmbyval, elmlen);
            (*it).dataptr = att_addlength_pointer(
                (*it).dataptr as usize,
                elmlen,
                (*it).dataptr as *const c_char,
            ) as *mut c_char;
            (*it).dataptr = att_align_nominal((*it).dataptr as usize, elmalign) as *mut c_char;
        }
        (*it).bitmask <<= 1;
        if (*it).bitmask == 0x100 {
            if !(*it).bitmapptr.is_null() {
                (*it).bitmapptr = (*it).bitmapptr.add(1);
            }
            (*it).bitmask = 1;
        }
    }

    ret
}
