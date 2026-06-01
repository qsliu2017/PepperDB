//! Translation of postgres/src/backend/utils/sort/qsort_interruptible.c
//!
//! This is an instantiation of the lib/sort_template.h macro template identical
//! to port/qsort.c's `qsort_arg`, but with ST_CHECK_FOR_INTERRUPTS defined so a
//! very large sort can be cancelled.  The sort algorithm itself is already ported
//! once as a generic in crate::port::qsort, so this just re-exposes the
//! interruptible entry point.  CHECK_FOR_INTERRUPTS is a no-op here until the
//! signal/interrupt machinery (miscadmin.h) is ported.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::port::qsort::qsort_arg_comparator;
use core::ffi::c_void;

/*
 * qsort_interruptible - like qsort_arg, but interruptible during very large
 * sorts.
 *
 * # Safety
 * As crate::port::qsort::qsort_arg.
 *
 * TODO(pg-port): inject CHECK_FOR_INTERRUPTS() once miscadmin.h's interrupt
 * machinery is ported; the algorithm is shared with port::qsort.
 */
pub unsafe fn qsort_interruptible(
    base: *mut c_void,
    n: usize,
    element_size: usize,
    cmp: qsort_arg_comparator,
    arg: *mut c_void,
) {
    crate::port::qsort::qsort_arg(base, n, element_size, cmp, arg);
}
