//! Translation of postgres/src/backend/utils/adt/arrayutils.c
//!
//! Internal "utility" routines for the array datatype: subscript/offset math,
//! element-count + bounds validation, and slice stepping.  These are pure
//! arithmetic over caller-supplied dimension arrays (no catalog).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ArrayGetIntegerTypmods is STUBBED: it needs array_contains_nulls +
//! deconstruct_array_builtin (utils/adt/arrayfuncs.c, not yet translated).

use crate::prelude::*;
use crate::c::int32;
use crate::common::int::pg_add_s32_overflow;
use crate::utils::array::{ArrayType, MaxArraySize, ARR_ELEMTYPE, ARR_NDIM};
use crate::nodes::nodes::Node;
use core::ffi::c_int;

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;
const ERRCODE_ARRAY_ELEMENT_ERROR: c_int = 0;
const ERRCODE_ARRAY_SUBSCRIPT_ERROR: c_int = 0;
const ERRCODE_NULL_VALUE_NOT_ALLOWED: c_int = 0;

/*
 * Convert subscript list into linear element number (from 0).
 *
 * # Safety
 * `dim`, `lb`, `indx` point to at least `n` readable ints.
 */
pub unsafe fn ArrayGetOffset(
    n: c_int,
    dim: *const c_int,
    lb: *const c_int,
    indx: *const c_int,
) -> c_int {
    let mut scale: c_int = 1;
    let mut offset: c_int = 0;
    let mut i = n - 1;
    while i >= 0 {
        offset += (*indx.add(i as usize) - *lb.add(i as usize)) * scale;
        scale *= *dim.add(i as usize);
        i -= 1;
    }
    offset
}

/*
 * Convert array dimensions into number of elements (with overflow checking).
 *
 * # Safety
 * `dims` points to at least `ndim` readable ints.
 */
pub unsafe fn ArrayGetNItems(ndim: c_int, dims: *const c_int) -> c_int {
    ArrayGetNItemsSafe(ndim, dims, null_mut())
}

/// As above, reporting into an ErrorSaveContext instead of throwing; -1 on error.
/// (The soft-error path is not yet supported, so errors are hard ERROR.)
///
/// # Safety
/// `dims` points to at least `ndim` readable ints.
pub unsafe fn ArrayGetNItemsSafe(ndim: c_int, dims: *const c_int, escontext: *mut Node) -> c_int {
    let mut ret: int32;
    let _ = escontext;

    if ndim <= 0 {
        return 0;
    }
    ret = 1;
    let mut i = 0;
    while i < ndim {
        /* A negative dimension implies that UB-LB overflowed ... */
        if *dims.add(i as usize) < 0 {
            array_size_error();
            return -1;
        }

        let prod: i64 = (ret as i64) * (*dims.add(i as usize) as i64);
        ret = prod as int32;
        if (ret as i64) != prod {
            array_size_error();
            return -1;
        }
        i += 1;
    }
    Assert!(ret >= 0);
    if (ret as usize) > MaxArraySize {
        array_size_error();
        return -1;
    }
    ret as c_int
}

unsafe fn array_size_error() {
    let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    ereport!(
        ERROR,
        errmsg!("array size exceeds the maximum allowed ({})", MaxArraySize as c_int)
    );
}

/*
 * Verify sanity of proposed lower-bound values for an array.
 *
 * # Safety
 * `dims`, `lb` point to at least `ndim` readable ints.
 */
pub unsafe fn ArrayCheckBounds(ndim: c_int, dims: *const c_int, lb: *const c_int) {
    let _ = ArrayCheckBoundsSafe(ndim, dims, lb, null_mut());
}

/// # Safety
/// As ArrayCheckBounds.
pub unsafe fn ArrayCheckBoundsSafe(
    ndim: c_int,
    dims: *const c_int,
    lb: *const c_int,
    escontext: *mut Node,
) -> bool {
    let _ = escontext;
    let mut i = 0;
    while i < ndim {
        let mut sum: int32 = 0;
        if pg_add_s32_overflow(*dims.add(i as usize), *lb.add(i as usize), &mut sum) {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(
                ERROR,
                errmsg!("array lower bound is too large: {}", *lb.add(i as usize))
            );
            return false;
        }
        i += 1;
    }
    true
}

/*
 * Compute ranges (sub-array dimensions) for an array slice.
 *
 * # Safety
 * `span`, `st`, `endp` point to at least `n` ints (`span` writable).
 */
pub unsafe fn mda_get_range(n: c_int, span: *mut c_int, st: *const c_int, endp: *const c_int) {
    let mut i = 0;
    while i < n {
        *span.add(i as usize) = *endp.add(i as usize) - *st.add(i as usize) + 1;
        i += 1;
    }
}

/*
 * Compute products of array dimensions (scale factors for subscripts).
 *
 * # Safety
 * `range`, `prod` point to at least `n` ints (`prod` writable).
 */
pub unsafe fn mda_get_prod(n: c_int, range: *const c_int, prod: *mut c_int) {
    *prod.add((n - 1) as usize) = 1;
    let mut i = n - 2;
    while i >= 0 {
        *prod.add(i as usize) = *prod.add((i + 1) as usize) * *range.add((i + 1) as usize);
        i -= 1;
    }
}

/*
 * Compute offset distances to step through a sub-array within an array.
 *
 * # Safety
 * `dist`, `prod`, `span` point to at least `n` ints (`dist` writable).
 */
pub unsafe fn mda_get_offset_values(n: c_int, dist: *mut c_int, prod: *const c_int, span: *const c_int) {
    *dist.add((n - 1) as usize) = 0;
    let mut j = n - 2;
    while j >= 0 {
        *dist.add(j as usize) = *prod.add(j as usize) - 1;
        let mut i = j + 1;
        while i < n {
            *dist.add(j as usize) -= (*span.add(i as usize) - 1) * *prod.add(i as usize);
            i += 1;
        }
        j -= 1;
    }
}

/*
 * Generate the lexicographically next n-tuple in `curr` (each elem < span[i]).
 * Returns -1 if no next tuple, else the advanced dimension (0..n-1).
 *
 * # Safety
 * `curr`, `span` point to at least `n` ints (`curr` writable).
 */
pub unsafe fn mda_next_tuple(n: c_int, curr: *mut c_int, span: *const c_int) -> c_int {
    if n <= 0 {
        return -1;
    }

    *curr.add((n - 1) as usize) = (*curr.add((n - 1) as usize) + 1) % *span.add((n - 1) as usize);
    let mut i = n - 1;
    while i != 0 && *curr.add(i as usize) == 0 {
        *curr.add((i - 1) as usize) = (*curr.add((i - 1) as usize) + 1) % *span.add((i - 1) as usize);
        i -= 1;
    }

    if i != 0 {
        return i;
    }
    if *curr.add(0) != 0 {
        return 0;
    }

    -1
}

/*
 * ArrayGetIntegerTypmods: verify the argument is a 1-D cstring array and return
 * its contents as integers.  [STUBBED]
 *
 * # Safety
 * `arr` is a valid ArrayType; `n` is writable.
 */
pub unsafe fn ArrayGetIntegerTypmods(arr: *mut ArrayType, n: *mut c_int) -> *mut int32 {
    // Validations that don't need arrayfuncs are kept:
    if ARR_ELEMTYPE(arr) != crate::catalog::pg_type_d::CSTRINGOID {
        let _ = errcode(ERRCODE_ARRAY_ELEMENT_ERROR);
        ereport!(ERROR, errmsg!("typmod array must be type cstring[]"));
    }
    if ARR_NDIM(arr) != 1 {
        let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
        ereport!(ERROR, errmsg!("typmod array must be one-dimensional"));
    }
    if crate::utils::adt::arrayfuncs::array_contains_nulls(arr) {
        let _ = errcode(ERRCODE_ARRAY_ELEMENT_ERROR);
        ereport!(ERROR, errmsg!("typmod array must not contain nulls"));
    }

    // cstring: typlen=-2, typbyval=false, typalign='c'
    let mut elem_values: *mut crate::postgres::Datum = core::ptr::null_mut();
    crate::utils::adt::arrayfuncs::deconstruct_array(
        arr,
        crate::catalog::pg_type_d::CSTRINGOID,
        -2,
        false,
        b'c' as c_char,
        &mut elem_values,
        core::ptr::null_mut(),
        n,
    );

    let cnt = *n;
    let result = crate::utils::palloc::palloc(cnt as usize * core::mem::size_of::<int32>()) as *mut int32;
    for i in 0..cnt {
        *result.add(i as usize) =
            crate::utils::builtins::pg_strtoint32(crate::postgres::DatumGetCString(*elem_values.add(i as usize)));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dimension_math() {
        unsafe {
            // 2-D array, dims [3,4], lbound [1,1]; element (2,3) -> linear offset.
            let dim = [3i32, 4];
            let lb = [1i32, 1];
            let indx = [2i32, 3];
            // offset = (indx0-lb0)*dim1 + (indx1-lb1) = (1)*4 + 2 = 6
            assert_eq!(ArrayGetOffset(2, dim.as_ptr(), lb.as_ptr(), indx.as_ptr()), 6);

            // NItems = product of dims
            assert_eq!(ArrayGetNItems(2, dim.as_ptr()), 12);
            assert_eq!(ArrayGetNItems(0, dim.as_ptr()), 0);

            // bounds ok
            assert!(ArrayCheckBoundsSafe(2, dim.as_ptr(), lb.as_ptr(), null_mut()));

            // mda_get_prod for dims [3,4]: prod = [4,1]
            let mut prod = [0i32; 2];
            mda_get_prod(2, dim.as_ptr(), prod.as_mut_ptr());
            assert_eq!(prod, [4, 1]);

            // mda_get_range: st=[0,0], end=[2,3] -> span=[3,4]
            let mut span = [0i32; 2];
            let st = [0i32, 0];
            let endp = [2i32, 3];
            mda_get_range(2, span.as_mut_ptr(), st.as_ptr(), endp.as_ptr());
            assert_eq!(span, [3, 4]);

            // mda_next_tuple steps the odometer; from [0,3] (span [3,4]) advances dim 0
            let mut curr = [0i32, 3];
            let r = mda_next_tuple(2, curr.as_mut_ptr(), span.as_ptr());
            assert_eq!(r, 0);
            assert_eq!(curr, [1, 0]);
        }
    }

    #[test]
    #[should_panic]
    fn nitems_overflow_errors() {
        unsafe {
            // dims whose product overflows int32 -> ERROR
            let dim = [100000i32, 100000];
            ArrayGetNItems(2, dim.as_ptr());
        }
    }
}
