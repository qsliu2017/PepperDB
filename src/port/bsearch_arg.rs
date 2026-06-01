//! Translation of postgres/src/port/bsearch_arg.c
//!
//! bsearch_arg.c: bsearch variant with a user-supplied pointer
//! (declaration lives in port.h; only the function defined here is provided)
//!
//! Copyright (c) 2021-2025, PostgreSQL Global Development Group
//! Copyright (c) 1990 Regents of the University of California.
//! All rights reserved.

use crate::prelude::*;

/// Type of the comparator callback passed to [`bsearch_arg`].
///
/// Mirrors the C `int (*compar)(const void *, const void *, void *)`.
pub type bsearch_arg_compar =
    unsafe extern "C" fn(*const c_void, *const c_void, *mut c_void) -> c_int;

/*
 * Perform a binary search.
 *
 * The code below is a bit sneaky.  After a comparison fails, we
 * divide the work in half by moving either left or right. If lim
 * is odd, moving left simply involves halving lim: e.g., when lim
 * is 5 we look at item 2, so we change lim to 2 so that we will
 * look at items 0 & 1.  If lim is even, the same applies.  If lim
 * is odd, moving right again involves halving lim, this time moving
 * the base up one item past p: e.g., when lim is 5 we change base
 * to item 3 and make lim 2 so that we will look at items 3 and 4.
 * If lim is even, however, we have to shrink it by one before
 * halving: e.g., when lim is 4, we still looked at item 2, so we
 * have to make lim 3, then halve, obtaining 1, so that we will only
 * look at item 3.
 */
/// # Safety
/// `base0` must point to an array of `nmemb` elements each of `size` bytes,
/// `key` must be valid for the comparator, and `compar` must not read past the
/// element bounds. Returns a pointer into the array, or NULL if not found.
pub unsafe fn bsearch_arg(
    key: *const c_void,
    base0: *const c_void,
    nmemb: Size,
    size: Size,
    compar: bsearch_arg_compar,
    arg: *mut c_void,
) -> *mut c_void {
    let mut base = base0 as *const c_char;
    let mut lim: Size;
    let mut cmp: c_int;
    let mut p: *const c_void;

    // for (lim = nmemb; lim != 0; lim >>= 1)
    lim = nmemb;
    while lim != 0 {
        p = base.add((lim >> 1).wrapping_mul(size)) as *const c_void;
        cmp = compar(key, p, arg);
        if cmp == 0 {
            return p as *mut c_void;
        }
        if cmp > 0 {
            /* key > p: move right */
            base = (p as *const c_char).add(size);
            lim -= 1;
        } /* else move left */
        lim >>= 1;
    }
    core::ptr::null_mut()
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe extern "C" fn cmp_i32(a: *const c_void, b: *const c_void, _arg: *mut c_void) -> c_int {
        let av = *(a as *const i32);
        let bv = *(b as *const i32);
        (av - bv).signum()
    }

    #[test]
    fn finds_and_misses() {
        unsafe {
            let arr: [i32; 6] = [1, 3, 5, 7, 9, 11];
            let size = core::mem::size_of::<i32>();
            for (i, want) in arr.iter().enumerate() {
                let key = *want;
                let found = bsearch_arg(
                    &key as *const i32 as *const c_void,
                    arr.as_ptr() as *const c_void,
                    arr.len(),
                    size,
                    cmp_i32,
                    core::ptr::null_mut(),
                );
                assert!(!found.is_null());
                assert_eq!(*(found as *const i32), *want);
                // returned pointer should be the i-th element
                assert_eq!(found as usize, arr.as_ptr().add(i) as usize);
            }
            // misses
            for miss in [0i32, 2, 4, 12] {
                let found = bsearch_arg(
                    &miss as *const i32 as *const c_void,
                    arr.as_ptr() as *const c_void,
                    arr.len(),
                    size,
                    cmp_i32,
                    core::ptr::null_mut(),
                );
                assert!(found.is_null());
            }
            // empty array
            let found = bsearch_arg(
                &0i32 as *const i32 as *const c_void,
                arr.as_ptr() as *const c_void,
                0,
                size,
                cmp_i32,
                core::ptr::null_mut(),
            );
            assert!(found.is_null());
        }
    }
}
