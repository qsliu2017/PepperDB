//! Translation of postgres/src/include/lib/qunique.h
//! (#include mapping: this header has no #includes of its own beyond the
//! ambient postgres.h that the C compilation unit provides; it only uses
//! `size_t` and `memcpy`).
//!
//! Two `static inline` functions that remove ADJACENT duplicate elements from
//! an already-sorted raw array in place, returning the new (reduced) element
//! count.  Modeled like crate::port::qsort: the array is a raw `*mut c_void`
//! viewed as `elements` records of `width` bytes, and the comparator is a
//! function pointer (two-arg for `qunique`, three-arg with a trailing user
//! `arg` for `qunique_arg`, mirroring qsort_arg()).
//!
//! Portions Copyright (c) 2019-2025, PostgreSQL Global Development Group

use core::ffi::{c_int, c_void};

/// qunique comparator: int (*)(const void *a, const void *b).
pub type qunique_comparator = unsafe fn(*const c_void, *const c_void) -> c_int;
/// qunique_arg comparator: int (*)(const void *a, const void *b, void *arg).
pub type qunique_arg_comparator = unsafe fn(*const c_void, *const c_void, *mut c_void) -> c_int;

/// Remove duplicates from a pre-sorted array, according to a user-supplied
/// comparator.  Usually the array should have been sorted with qsort() using
/// the same arguments.  Return the new size.
///
/// `array`: base pointer; `elements`: record count; `width`: bytes per record.
#[inline]
pub unsafe fn qunique(
    array: *mut c_void,
    elements: usize,
    width: usize,
    compare: qunique_comparator,
) -> usize {
    let bytes = array as *mut u8;

    if elements <= 1 {
        return elements;
    }

    let mut j: usize = 0;
    let mut i: usize = 1;
    while i < elements {
        // compare(bytes + i*width, bytes + j*width) != 0 && ++j != i
        if compare(
            bytes.add(i * width) as *const c_void,
            bytes.add(j * width) as *const c_void,
        ) != 0
        {
            j += 1;
            if j != i {
                // memcpy(bytes + j*width, bytes + i*width, width); j < i so
                // source/destination never overlap (C uses memcpy here).
                core::ptr::copy_nonoverlapping(
                    bytes.add(i * width),
                    bytes.add(j * width),
                    width,
                );
            }
        }
        i += 1;
    }

    j + 1
}

/// Like qunique(), but takes a comparator with an extra user data argument
/// which is passed through, for compatibility with qsort_arg().
#[inline]
#[no_mangle]
pub unsafe fn qunique_arg(
    array: *mut c_void,
    elements: usize,
    width: usize,
    compare: qunique_arg_comparator,
    arg: *mut c_void,
) -> usize {
    let bytes = array as *mut u8;

    if elements <= 1 {
        return elements;
    }

    let mut j: usize = 0;
    let mut i: usize = 1;
    while i < elements {
        if compare(
            bytes.add(i * width) as *const c_void,
            bytes.add(j * width) as *const c_void,
            arg,
        ) != 0
        {
            j += 1;
            if j != i {
                core::ptr::copy_nonoverlapping(
                    bytes.add(i * width),
                    bytes.add(j * width),
                    width,
                );
            }
        }
        i += 1;
    }

    j + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn cmp_i32(a: *const c_void, b: *const c_void) -> c_int {
        let av = *(a as *const i32);
        let bv = *(b as *const i32);
        (av > bv) as c_int - (av < bv) as c_int
    }

    // Reverse comparator that also consults a user-supplied multiplier `arg`,
    // exercising the argument pass-through.  Result sign is still 0-vs-nonzero
    // for dedup purposes, but flipping direction proves `arg` reaches the fn.
    unsafe fn cmp_i32_arg(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
        let mult = *(arg as *const i32);
        let av = *(a as *const i32);
        let bv = *(b as *const i32);
        mult * ((av > bv) as c_int - (av < bv) as c_int)
    }

    #[test]
    fn dedups_sorted_array() {
        let mut a: [i32; 7] = [1, 1, 2, 3, 3, 3, 4];
        let n = unsafe {
            qunique(
                a.as_mut_ptr() as *mut c_void,
                a.len(),
                core::mem::size_of::<i32>(),
                cmp_i32,
            )
        };
        assert_eq!(n, 4);
        assert_eq!(&a[..n], &[1, 2, 3, 4]);
    }

    #[test]
    fn empty_returns_zero() {
        let mut a: [i32; 0] = [];
        let n = unsafe {
            qunique(
                a.as_mut_ptr() as *mut c_void,
                a.len(),
                core::mem::size_of::<i32>(),
                cmp_i32,
            )
        };
        assert_eq!(n, 0);
    }

    #[test]
    fn single_element_unchanged() {
        let mut a: [i32; 1] = [42];
        let n = unsafe {
            qunique(
                a.as_mut_ptr() as *mut c_void,
                a.len(),
                core::mem::size_of::<i32>(),
                cmp_i32,
            )
        };
        assert_eq!(n, 1);
        assert_eq!(a[0], 42);
    }

    #[test]
    fn arg_variant_dedups_reverse_sorted() {
        // Reverse-sorted input deduped with a reverse comparator (mult = -1).
        let mut a: [i32; 6] = [4, 4, 3, 2, 2, 1];
        let mut mult: i32 = -1;
        let n = unsafe {
            qunique_arg(
                a.as_mut_ptr() as *mut c_void,
                a.len(),
                core::mem::size_of::<i32>(),
                cmp_i32_arg,
                &mut mult as *mut i32 as *mut c_void,
            )
        };
        assert_eq!(n, 4);
        assert_eq!(&a[..n], &[4, 3, 2, 1]);
    }
}
