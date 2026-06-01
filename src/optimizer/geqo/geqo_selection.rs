//! src/backend/optimizer/geqo/geqo_selection.c
//!
//! Linear selection scheme for the genetic query optimizer (adopted from
//! D. Whitley's Genitor algorithm).
//!
//! #include mapping:
//!   - "postgres.h"                  -> `use crate::prelude::*;`
//!   - <math.h>                      -> f64::sqrt (intrinsic; no import needed)
//!   - "optimizer/geqo_copy.h"       -> `geqo_copy` + `Chromosome`, imported
//!                                      from the sibling geqo_copy module.
//!   - "optimizer/geqo_random.h"     -> `geqo_rand`, imported from geqo_random.
//!   - "optimizer/geqo_selection.h"  -> just declares geqo_selection; nothing to
//!                                      merge beyond the prototype.
//!
//! `Pool` (from optimizer/geqo_gene.h) is normally provided by the geqo_pool
//! module. That module is not yet ported, so a minimal `Pool` mirroring the C
//! struct is defined locally below.
//! TODO: drop this local `Pool` and import it from
//! `crate::optimizer::geqo::geqo_pool` once that module lands.
//!
//! This is a FULLY REAL 1:1 translation of the C source, including the exact
//! sqrt-based linear-bias index formula.

use core::ffi::c_double;

use crate::prelude::*;

use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo::geqo_copy::{geqo_copy, Chromosome};
use crate::optimizer::geqo::geqo_random::geqo_rand;

/// Pool (from optimizer/geqo_gene.h).
///
/// A population of `size` chromosomes (`data`), each a tour of `string_length`
/// genes.
///
/// TODO: this is a minimal local stand-in; replace with the canonical `Pool`
/// from the geqo_pool module once it is ported.
#[repr(C)]
pub struct Pool {
    pub data: *mut Chromosome,
    pub size: c_int,
    pub string_length: c_int,
}

/// geqo_selection
///
/// According to bias described by input parameters, first and second genes are
/// selected from the pool and copied into `momma` / `daddy`.
///
/// # Safety
/// `root`, `momma`, `daddy`, and `pool` must be valid; `pool->data` must point
/// to at least `pool->size` chromosomes, each with a `string` array of at least
/// `pool->string_length` genes.
pub unsafe fn geqo_selection(
    root: *mut PlannerInfo,
    momma: *mut Chromosome,
    daddy: *mut Chromosome,
    pool: *mut Pool,
    bias: c_double,
) {
    let first: c_int;
    let mut second: c_int;

    first = linear_rand(root, (*pool).size, bias);
    second = linear_rand(root, (*pool).size, bias);

    // Ensure we have selected different genes, except if pool size is only one,
    // when we can't.
    if (*pool).size > 1 {
        while first == second {
            second = linear_rand(root, (*pool).size, bias);
        }
    }

    geqo_copy(
        root,
        momma,
        (*pool).data.offset(first as isize),
        (*pool).string_length,
    );
    geqo_copy(
        root,
        daddy,
        (*pool).data.offset(second as isize),
        (*pool).string_length,
    );
}

/// linear_rand
///
/// Generates a random integer between 0 and `pool_size` (exclusive) using the
/// input linear bias.
///
/// `bias` is the y-intercept of the linear distribution.
///
/// probability distribution function is: f(x) = bias - 2(bias - 1)x
///        bias = (prob of first rule) / (prob of middle rule)
///
/// # Safety
/// `root` must be a valid PlannerInfo with a seeded GEQO RNG (geqo_rand reaches
/// `root->join_search_private->random_state`).
unsafe fn linear_rand(root: *mut PlannerInfo, pool_size: c_int, bias: c_double) -> c_int {
    let mut index: c_double; /* index between 0 and pool_size */
    let max: c_double = pool_size as c_double;

    // geqo_rand() is not supposed to return 1.0, but if it does then we will
    // get exactly max from this equation, whereas we need 0 <= index < max.
    // Also it seems possible that roundoff error might deliver values slightly
    // outside the range; in particular avoid passing a value slightly less than
    // 0 to sqrt().  If we get a bad value just try again.
    loop {
        let mut sqrtval: c_double;

        sqrtval = (bias * bias) - 4.0 * (bias - 1.0) * geqo_rand(root);
        if sqrtval > 0.0 {
            sqrtval = sqrtval.sqrt();
        }
        index = max * (bias - sqrtval) / 2.0 / (bias - 1.0);

        if !(index < 0.0 || index >= max) {
            break;
        }
    }

    index as c_int
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::pg_prng::{pg_prng_fseed, pg_prng_state};
    use crate::optimizer::geqo::geqo_random::GeqoPrivateData;
    use crate::optimizer::geqo::geqo_recombination::Gene;

    // Build a PlannerInfo whose join_search_private holds a deterministically
    // seeded GeqoPrivateData, so geqo_rand() (via the sibling module) is
    // reproducible. PlannerInfo has ~82 fields, so build it zeroed via palloc0
    // and only set join_search_private (the single field the GEQO RNG reaches),
    // matching the geqo_random module's access path.
    unsafe fn seeded_root(seed: f64) -> *mut PlannerInfo {
        let private = palloc0(core::mem::size_of::<GeqoPrivateData>()) as *mut GeqoPrivateData;
        pg_prng_fseed(&mut (*private).random_state as *mut pg_prng_state, seed);
        let root = palloc0(core::mem::size_of::<PlannerInfo>()) as *mut PlannerInfo;
        (*root).join_search_private = private as *mut c_void;
        root
    }

    // linear_rand must always return an index within [0, pool_size - 1], for a
    // range of pool sizes and over many draws, at both bias endpoints
    // (MIN_GEQO_SELECTION_BIAS = 1.5 and MAX_GEQO_SELECTION_BIAS = 2.0).
    #[test]
    fn linear_rand_stays_in_range() {
        unsafe {
            for &bias in &[1.5_f64, 2.0_f64] {
                for &pool_size in &[1i32, 2, 5, 16, 100] {
                    let root = seeded_root(0.5);
                    for _ in 0..10_000 {
                        let idx = linear_rand(root, pool_size, bias as c_double);
                        assert!(
                            idx >= 0 && idx < pool_size,
                            "linear_rand returned {} out of range [0, {}) (bias {})",
                            idx,
                            pool_size,
                            bias
                        );
                    }
                }
            }
        }
    }

    // The linear bias must skew selection toward low indices: with bias > 1 the
    // mean index over many draws should be below the midpoint (pool_size / 2).
    #[test]
    fn linear_rand_is_biased_toward_low_indices() {
        unsafe {
            let pool_size: c_int = 100;
            let root = seeded_root(0.123);
            let draws = 50_000;
            let mut sum: f64 = 0.0;
            for _ in 0..draws {
                sum += linear_rand(root, pool_size, 2.0 as c_double) as f64;
            }
            let mean = sum / draws as f64;
            assert!(
                mean < (pool_size as f64) / 2.0,
                "mean index {} not biased below midpoint {}",
                mean,
                (pool_size as f64) / 2.0
            );
        }
    }

    // geqo_selection must copy two valid pool members into momma and daddy.
    // With pool size > 1 it must also pick two distinct members; we verify the
    // copies are real (worth + full string array) against the pool contents.
    #[test]
    fn geqo_selection_copies_two_valid_members() {
        unsafe {
            let pool_size: c_int = 6;
            let string_length: c_int = 4;

            // Build a pool of distinct chromosomes: chromosome k has worth k and
            // a string filled with k*10 + position.
            let data =
                palloc((pool_size as usize) * core::mem::size_of::<Chromosome>()) as *mut Chromosome;
            for k in 0..pool_size {
                let arr =
                    palloc((string_length as usize) * core::mem::size_of::<Gene>()) as *mut Gene;
                for p in 0..string_length {
                    *arr.offset(p as isize) = (k * 10 + p) as Gene;
                }
                *data.offset(k as isize) = Chromosome {
                    string: arr,
                    worth: k as c_double,
                };
            }
            let mut pool = Pool {
                data,
                size: pool_size,
                string_length,
            };

            // Destination chromosomes with their own backing arrays.
            let momma_arr =
                palloc((string_length as usize) * core::mem::size_of::<Gene>()) as *mut Gene;
            let daddy_arr =
                palloc((string_length as usize) * core::mem::size_of::<Gene>()) as *mut Gene;
            let mut momma = Chromosome {
                string: momma_arr,
                worth: -1.0 as c_double,
            };
            let mut daddy = Chromosome {
                string: daddy_arr,
                worth: -1.0 as c_double,
            };

            let root = seeded_root(0.777);
            geqo_selection(
                root,
                &mut momma as *mut Chromosome,
                &mut daddy as *mut Chromosome,
                &mut pool as *mut Pool,
                2.0 as c_double,
            );

            // Each destination must equal exactly one pool member (its worth
            // identifies which), and the string array must match that member.
            for (label, dst) in [("momma", &momma), ("daddy", &daddy)] {
                let k = dst.worth as c_int;
                assert!(
                    k >= 0 && k < pool_size,
                    "{} worth {} does not match any pool member",
                    label,
                    dst.worth
                );
                for p in 0..string_length {
                    assert_eq!(
                        *dst.string.offset(p as isize),
                        (k * 10 + p) as Gene,
                        "{} string[{}] mismatch for member {}",
                        label,
                        p,
                        k
                    );
                }
            }

            // With pool size > 1, the two selected members must be distinct.
            assert_ne!(
                momma.worth as c_int, daddy.worth as c_int,
                "momma and daddy must be different pool members when size > 1"
            );
        }
    }

    // With a singleton pool, geqo_selection cannot pick different members; both
    // momma and daddy must copy the sole chromosome (the size > 1 dedup loop is
    // skipped).
    #[test]
    fn geqo_selection_singleton_pool() {
        unsafe {
            let string_length: c_int = 3;
            let arr = palloc((string_length as usize) * core::mem::size_of::<Gene>()) as *mut Gene;
            for p in 0..string_length {
                *arr.offset(p as isize) = (p + 7) as Gene;
            }
            let data = palloc(core::mem::size_of::<Chromosome>()) as *mut Chromosome;
            *data = Chromosome {
                string: arr,
                worth: 99.0 as c_double,
            };
            let mut pool = Pool {
                data,
                size: 1,
                string_length,
            };

            let momma_arr =
                palloc((string_length as usize) * core::mem::size_of::<Gene>()) as *mut Gene;
            let daddy_arr =
                palloc((string_length as usize) * core::mem::size_of::<Gene>()) as *mut Gene;
            let mut momma = Chromosome {
                string: momma_arr,
                worth: 0.0 as c_double,
            };
            let mut daddy = Chromosome {
                string: daddy_arr,
                worth: 0.0 as c_double,
            };

            let root = seeded_root(0.5);
            geqo_selection(
                root,
                &mut momma as *mut Chromosome,
                &mut daddy as *mut Chromosome,
                &mut pool as *mut Pool,
                2.0 as c_double,
            );

            assert_eq!(momma.worth as c_int, 99);
            assert_eq!(daddy.worth as c_int, 99);
            for p in 0..string_length {
                assert_eq!(*momma.string.offset(p as isize), (p + 7) as Gene);
                assert_eq!(*daddy.string.offset(p as isize), (p + 7) as Gene);
            }
        }
    }
}
