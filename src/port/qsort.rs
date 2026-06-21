//! Translation of postgres/src/port/qsort.c + postgres/src/port/qsort_arg.c,
//! which are both instantiations of the lib/sort_template.h macro template
//! (ST_ELEMENT_TYPE_VOID + ST_COMPARE_RUNTIME_POINTER).
//!
//! Like simplehash, sort_template.h is a C macro template; we port the ST_SORT
//! algorithm once over a raw byte buffer (base + element_size) and expose the
//! two void-element instantiations PostgreSQL builds: `pg_qsort` (comparator
//! takes two element pointers) and `qsort_arg` (comparator also takes a void*
//! arg).  The algorithm - Bentley/McIlroy introsort with a presorted-input fast
//! path, insertion sort for n<7, median-of-3 (median-of-medians for n>40),
//! 3-way partition, recurse-smaller/iterate-larger - is faithful to the C.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use core::ffi::{c_int, c_void};

/// pg_qsort comparator: int (*)(const void *a, const void *b).
pub type qsort_comparator = unsafe fn(*const c_void, *const c_void) -> c_int;
/// qsort_arg comparator: int (*)(const void *a, const void *b, void *arg).
pub type qsort_arg_comparator = unsafe fn(*const c_void, *const c_void, *mut c_void) -> c_int;

/// Swap `n` bytes between the elements at `a` and `b` (DO_SWAPN over uint8).
#[inline]
unsafe fn swapn(a: *mut u8, b: *mut u8, n: usize) {
    for i in 0..n {
        let pa = a.add(i);
        let pb = b.add(i);
        let tmp = *pa;
        *pa = *pb;
        *pb = tmp;
    }
}

/// Median of three (ST_MED3), returning the pointer to the median element.
#[inline]
unsafe fn med3(
    a: *mut u8,
    b: *mut u8,
    c: *mut u8,
    cmp: &dyn Fn(*const u8, *const u8) -> c_int,
) -> *mut u8 {
    if cmp(a, b) < 0 {
        if cmp(b, c) < 0 {
            b
        } else if cmp(a, c) < 0 {
            c
        } else {
            a
        }
    } else if cmp(b, c) > 0 {
        b
    } else if cmp(a, c) < 0 {
        a
    } else {
        c
    }
}

/// The ST_SORT body: sort `n` elements of `size` bytes starting at `base`.
unsafe fn st_sort(
    mut a: *mut u8,
    mut n: usize,
    size: usize,
    cmp: &dyn Fn(*const u8, *const u8) -> c_int,
) {
    'loop_: loop {
        // Insertion sort for small inputs.
        if n < 7 {
            let mut pm = a.add(size);
            while (pm as usize) < (a as usize) + n * size {
                let mut pl = pm;
                while (pl as usize) > (a as usize) && cmp(pl.sub(size), pl) > 0 {
                    swapn(pl, pl.sub(size), size);
                    pl = pl.sub(size);
                }
                pm = pm.add(size);
            }
            return;
        }

        // Presorted-input fast path.
        let mut presorted = true;
        {
            let mut pm = a.add(size);
            while (pm as usize) < (a as usize) + n * size {
                if cmp(pm.sub(size), pm) > 0 {
                    presorted = false;
                    break;
                }
                pm = pm.add(size);
            }
        }
        if presorted {
            return;
        }

        // Choose a pivot (median of three / median of medians).
        let mut pm = a.add((n / 2) * size);
        if n > 7 {
            let mut pl = a;
            let mut pn = a.add((n - 1) * size);
            if n > 40 {
                let d = (n / 8) * size;
                pl = med3(pl, pl.add(d), pl.add(2 * d), cmp);
                pm = med3(pm.sub(d), pm, pm.add(d), cmp);
                pn = med3(pn.sub(2 * d), pn.sub(d), pn, cmp);
            }
            pm = med3(pl, pm, pn, cmp);
        }
        swapn(a, pm, size);

        // 3-way partition (Bentley/McIlroy).
        let mut pa = a.add(size);
        let mut pb = pa;
        let mut pc = a.add((n - 1) * size);
        let mut pd = pc;
        loop {
            let mut r;
            while pb as usize <= pc as usize && {
                r = cmp(pb, a);
                r <= 0
            } {
                if r == 0 {
                    swapn(pa, pb, size);
                    pa = pa.add(size);
                }
                pb = pb.add(size);
            }
            while pb as usize <= pc as usize && {
                r = cmp(pc, a);
                r >= 0
            } {
                if r == 0 {
                    swapn(pc, pd, size);
                    pd = pd.sub(size);
                }
                pc = pc.sub(size);
            }
            if pb as usize > pc as usize {
                break;
            }
            swapn(pb, pc, size);
            pb = pb.add(size);
            pc = pc.sub(size);
        }

        let pn = a.add(n * size);
        // Move the equal-key elements (at the two ends) back to the middle.
        let mut d1 = core::cmp::min((pa as usize) - (a as usize), (pb as usize) - (pa as usize));
        swapn(a, pb.sub(d1), d1);
        d1 = core::cmp::min(
            (pd as usize) - (pc as usize),
            (pn as usize) - (pd as usize) - size,
        );
        swapn(pb, pn.sub(d1), d1);

        let d1 = (pb as usize) - (pa as usize);
        let d2 = (pd as usize) - (pc as usize);
        if d1 <= d2 {
            // Recurse on the (smaller) left partition, iterate on the right.
            if d1 > size {
                st_sort(a, d1 / size, size, cmp);
            }
            if d2 > size {
                a = pn.sub(d2);
                n = d2 / size;
                continue 'loop_;
            }
        } else {
            // Recurse on the (smaller) right partition, iterate on the left.
            if d2 > size {
                st_sort(pn.sub(d2), d2 / size, size, cmp);
            }
            if d1 > size {
                n = d1 / size;
                continue 'loop_;
            }
        }
        return;
    }
}

/*
 * pg_qsort - sort `n` elements of `element_size` bytes at `base` using `cmp`.
 *
 * # Safety
 * `base` must point to `n * element_size` valid, writable bytes; `cmp` must
 * impose a total order.
 */
pub unsafe fn pg_qsort(
    base: *mut c_void,
    n: usize,
    element_size: usize,
    cmp: qsort_comparator,
) {
    let closure = |a: *const u8, b: *const u8| cmp(a as *const c_void, b as *const c_void);
    st_sort(base as *mut u8, n, element_size, &closure);
}

/*
 * qsort_arg - like pg_qsort, but `cmp` also receives the caller's `arg`.
 *
 * # Safety
 * As pg_qsort; `arg` is passed opaquely to every `cmp` call.
 */
#[no_mangle]
pub unsafe fn qsort_arg(
    base: *mut c_void,
    n: usize,
    element_size: usize,
    cmp: qsort_arg_comparator,
    arg: *mut c_void,
) {
    let closure = |a: *const u8, b: *const u8| cmp(a as *const c_void, b as *const c_void, arg);
    st_sort(base as *mut u8, n, element_size, &closure);
}

/*
 * pg_qsort_strcmp - a pg_qsort comparator for arrays of `char *` (port/qsort.c).
 *
 * # Safety
 * `a`/`b` point to `*const c_char` elements.
 */
pub unsafe fn pg_qsort_strcmp(a: *const c_void, b: *const c_void) -> c_int {
    extern "C" {
        fn strcmp(a: *const core::ffi::c_char, b: *const core::ffi::c_char) -> c_int;
    }
    strcmp(
        *(a as *const *const core::ffi::c_char),
        *(b as *const *const core::ffi::c_char),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn cmp_i32(a: *const c_void, b: *const c_void) -> c_int {
        let x = *(a as *const i32);
        let y = *(b as *const i32);
        (x > y) as c_int - (x < y) as c_int
    }
    unsafe fn cmp_i32_desc(a: *const c_void, b: *const c_void, _arg: *mut c_void) -> c_int {
        let x = *(a as *const i32);
        let y = *(b as *const i32);
        (y > x) as c_int - (y < x) as c_int
    }

    #[test]
    fn sorts_correctly() {
        unsafe {
            // A non-trivial mix incl. duplicates, reverse runs, and presorted prefix.
            let mut v: Vec<i32> = vec![
                5, 3, 8, 1, 9, 2, 7, 0, 6, 4, 3, 3, 100, -5, 50, 50, 50, 1, 1, 1, 42, 17,
            ];
            let n = v.len();
            pg_qsort(v.as_mut_ptr() as *mut c_void, n, 4, cmp_i32);
            let mut want = v.clone();
            want.sort();
            assert_eq!(v, want);

            // Descending via qsort_arg.
            let mut w: Vec<i32> = (0..500).rev().chain(0..500).collect();
            let m = w.len();
            qsort_arg(w.as_mut_ptr() as *mut c_void, m, 4, cmp_i32_desc, core::ptr::null_mut());
            let mut want2 = w.clone();
            want2.sort_by(|a, b| b.cmp(a));
            assert_eq!(w, want2);

            // Already-sorted (presorted fast path) and reverse, larger n (>40 path).
            let mut s: Vec<i32> = (0..1000).collect();
            pg_qsort(s.as_mut_ptr() as *mut c_void, 1000, 4, cmp_i32);
            assert!(s.windows(2).all(|p| p[0] <= p[1]));
            let mut r: Vec<i32> = (0..1000).rev().collect();
            pg_qsort(r.as_mut_ptr() as *mut c_void, 1000, 4, cmp_i32);
            assert_eq!(r, (0..1000).collect::<Vec<_>>());

            // Empty + single-element are no-ops.
            pg_qsort(core::ptr::null_mut(), 0, 4, cmp_i32);
            let mut one = [7i32];
            pg_qsort(one.as_mut_ptr() as *mut c_void, 1, 4, cmp_i32);
            assert_eq!(one, [7]);
        }
    }
}
