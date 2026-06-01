//! Translation of postgres/src/include/utils/array.h (the ArrayType layout + ARR_*
//! accessor macros + size limits).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Only the standard (non-expanded) array representation is modeled here.  The
//! array I/O / construct / deconstruct functions live in utils/adt/arrayfuncs.c
//! (not yet translated); the AnyArrayType / expanded-array (VARATT_IS_EXPANDED_HEADER)
//! macros need utils/expandeddatum.h and are TODO.

use crate::c::{int32, MAXALIGN};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::varatt::VARSIZE;
use crate::utils::memutils::MaxAllocSize;
use core::ffi::{c_char, c_int};

/// The maximum number of array elements we allow (bounds ArrayGetNItems).
pub const MaxArraySize: usize = MaxAllocSize / core::mem::size_of::<Datum>();

/*
 * Arrays are varlena objects, so must meet the varlena convention that the
 * first int32 of the object contains the total object size in bytes.  Be sure
 * to use VARSIZE() and SET_VARSIZE() to access it, though!
 *
 * Header layout (struct fields are followed by ndim dim ints, ndim lbound
 * ints, an optional null bitmap, MAXALIGN padding, then the element data).
 */
#[repr(C)]
pub struct ArrayType {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub ndim: c_int,    /* # of dimensions */
    pub dataoffset: int32, /* offset to data, or 0 if no bitmap */
    pub elemtype: Oid,  /* element type OID */
}

#[inline]
pub unsafe fn ARR_SIZE(a: *const ArrayType) -> u32 {
    VARSIZE(a as *const c_char)
}
#[inline]
pub unsafe fn ARR_NDIM(a: *const ArrayType) -> c_int {
    (*a).ndim
}
#[inline]
pub unsafe fn ARR_HASNULL(a: *const ArrayType) -> bool {
    (*a).dataoffset != 0
}
#[inline]
pub unsafe fn ARR_ELEMTYPE(a: *const ArrayType) -> Oid {
    (*a).elemtype
}

/// ARR_DIMS(a): the dimensions array (ndim ints right after the header).
#[inline]
pub unsafe fn ARR_DIMS(a: *const ArrayType) -> *mut c_int {
    (a as *const c_char).add(core::mem::size_of::<ArrayType>()) as *mut c_int
}
/// ARR_LBOUND(a): the lower-bounds array (ndim ints after ARR_DIMS).
#[inline]
pub unsafe fn ARR_LBOUND(a: *const ArrayType) -> *mut c_int {
    (a as *const c_char)
        .add(core::mem::size_of::<ArrayType>() + core::mem::size_of::<c_int>() * ARR_NDIM(a) as usize)
        as *mut c_int
}
/// ARR_NULLBITMAP(a): the null bitmap, or NULL if the array has no nulls.
#[inline]
pub unsafe fn ARR_NULLBITMAP(a: *const ArrayType) -> *mut u8 {
    if ARR_HASNULL(a) {
        (a as *const c_char).add(
            core::mem::size_of::<ArrayType>() + 2 * core::mem::size_of::<c_int>() * ARR_NDIM(a) as usize,
        ) as *mut u8
    } else {
        core::ptr::null_mut()
    }
}

/// ARR_OVERHEAD_NONULLS(ndims): total header size for a no-nulls array.
#[inline]
pub fn ARR_OVERHEAD_NONULLS(ndims: c_int) -> usize {
    MAXALIGN(core::mem::size_of::<ArrayType>() + 2 * core::mem::size_of::<c_int>() * ndims as usize)
}
/// ARR_OVERHEAD_WITHNULLS(ndims, nitems): header size including a null bitmap.
#[inline]
pub fn ARR_OVERHEAD_WITHNULLS(ndims: c_int, nitems: c_int) -> usize {
    MAXALIGN(
        core::mem::size_of::<ArrayType>()
            + 2 * core::mem::size_of::<c_int>() * ndims as usize
            + (nitems as usize + 7) / 8,
    )
}

#[inline]
pub unsafe fn ARR_DATA_OFFSET(a: *const ArrayType) -> usize {
    if ARR_HASNULL(a) {
        (*a).dataoffset as usize
    } else {
        ARR_OVERHEAD_NONULLS(ARR_NDIM(a))
    }
}
/// ARR_DATA_PTR(a): pointer to the actual element data.
#[inline]
pub unsafe fn ARR_DATA_PTR(a: *const ArrayType) -> *mut c_char {
    (a as *const c_char).add(ARR_DATA_OFFSET(a)) as *mut c_char
}
