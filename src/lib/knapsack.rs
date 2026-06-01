//! Translation of postgres/src/include/lib/knapsack.h
//!                + postgres/src/backend/lib/knapsack.c
//!
//! Knapsack problem solver (0/1 knapsack via dynamic programming).
//!
//! Given input vectors of integral item weights (>= 0) and values (double >= 0),
//! compute the set of items producing the greatest total value without exceeding
//! a specified total weight; each item is included at most once. Weight-0 items
//! are always included. Performance is pseudo-polynomial, O(nW).
//!
//! Copyright (c) 2017-2025, PostgreSQL Global Development Group

use crate::nodes::bitmapset::{
    bms_add_member, bms_copy, bms_del_member, bms_make_singleton, bms_replace_members, Bitmapset,
};
use crate::prelude::*;
use crate::AllocSetContextCreate;
use core::ffi::c_int;

/*
 * DiscreteKnapsack
 *
 * The item_values input is optional; if omitted (NULL), all the items are
 * assumed to have value 1.
 *
 * Returns a Bitmapset of the 0..(n-1) indexes of the items chosen for
 * inclusion in the solution.
 *
 * This uses the usual dynamic-programming algorithm, adapted to reuse the
 * memory on each pass (by working from larger weights to smaller).  At the
 * start of pass number i, the values[w] array contains the largest value
 * computed with total weight <= w, using only items with indices < i; and
 * sets[w] contains the bitmap of items actually used for that value.  (The
 * bitmapsets are all pre-initialized with an unused high bit so that memory
 * allocation is done only once.)
 *
 * # Safety
 * `item_weights` must point to `num_items` ints; `item_values`, if non-NULL,
 * to `num_items` doubles.
 */
pub unsafe fn DiscreteKnapsack(
    max_weight: c_int,
    num_items: c_int,
    item_weights: *mut c_int,
    item_values: *mut f64,
) -> *mut Bitmapset {
    let local_ctx = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"Knapsack".as_ptr(),
        ALLOCSET_SMALL_SIZES
    );
    let oldctx = MemoryContextSwitchTo(local_ctx);
    let values: *mut f64;
    let sets: *mut *mut Bitmapset;
    let result: *mut Bitmapset;
    let mut i: c_int;
    let mut j: c_int;

    Assert!(max_weight >= 0);
    Assert!(num_items > 0 && !item_weights.is_null());

    values = palloc((1 + max_weight) as Size * core::mem::size_of::<f64>()) as *mut f64;
    sets = palloc((1 + max_weight) as Size * core::mem::size_of::<*mut Bitmapset>())
        as *mut *mut Bitmapset;

    i = 0;
    while i <= max_weight {
        *values.offset(i as isize) = 0.0;
        *sets.offset(i as isize) = bms_make_singleton(num_items);
        i += 1;
    }

    i = 0;
    while i < num_items {
        let iw = *item_weights.offset(i as isize);
        let iv = if !item_values.is_null() {
            *item_values.offset(i as isize)
        } else {
            1.0
        };

        j = max_weight;
        while j >= iw {
            let ow = j - iw;

            if *values.offset(j as isize) <= *values.offset(ow as isize) + iv {
                /* copy sets[ow] to sets[j] without realloc */
                if j != ow {
                    *sets.offset(j as isize) = bms_replace_members(
                        *sets.offset(j as isize),
                        *sets.offset(ow as isize) as *const Bitmapset,
                    );
                }

                *sets.offset(j as isize) = bms_add_member(*sets.offset(j as isize), i);

                *values.offset(j as isize) = *values.offset(ow as isize) + iv;
            }

            j -= 1;
        }

        i += 1;
    }

    MemoryContextSwitchTo(oldctx);

    result = bms_del_member(
        bms_copy(*sets.offset(max_weight as isize) as *const Bitmapset),
        num_items,
    );

    MemoryContextDelete(local_ctx);

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::bitmapset::bms_is_member;

    #[test]
    fn solves_small_instance() {
        unsafe {
            // weights/values; capacity 5. Optimal pick is items {0,1}: weight 5, value 7.
            let mut w: [c_int; 4] = [2, 3, 4, 5];
            let mut v: [f64; 4] = [3.0, 4.0, 5.0, 6.0];
            let r = DiscreteKnapsack(5, 4, w.as_mut_ptr(), v.as_mut_ptr());
            assert!(bms_is_member(0, r));
            assert!(bms_is_member(1, r));
            assert!(!bms_is_member(2, r));
            assert!(!bms_is_member(3, r));
        }
    }
}
