//! src/backend/optimizer/geqo/geqo_mutation.c
//!
//! TSP mutation routines.
//!
//! Contributed by Martin Utesch (Institute of Automatic Control, University of
//! Mining and Technology, Freiberg, Germany), adopted from Genitor
//! (Copyright (c) 1990 Darrell L. Whitley, Colorado State University).
//!
//! #include mapping:
//!   - Gene from optimizer/geqo_gene.h            -> `type Gene = c_int` below
//!   - geqo_randint from optimizer/geqo_random.h  -> imported from the sibling
//!     crate::optimizer::geqo::geqo_random when present; modeled locally here
//!     since that unit is not yet ported (see note below).
//!   - PlannerInfo (only forwarded to geqo_randint) -> opaque, see below.
//!
//! The original C guards this file behind `#if defined(CX)` ("currently used
//! only in CX mode"); that compile-time selector is not modeled - the function
//! is always available in Rust. The swap logic is a FULLY REAL 1:1 translation.

use crate::prelude::*;
use core::ffi::c_double;

/// Genome representation (optimizer/geqo_gene.h). "we presume that int instead
/// of Relid is o.k. for Gene; so don't change it!"
pub type Gene = c_int;

/// Planner RNG state (optimizer/geqo.h: GeqoPrivateData). In PostgreSQL the
/// planner reaches its geqo RNG through `root->join_search_private`, which for
/// the geqo path points at a `GeqoPrivateData` holding a `pg_prng_state`.
///
/// `geqo_random.c`/`geqo_random.h` are not yet ported, so we model the minimal
/// shape needed to drive `geqo_randint` deterministically for tests. When the
/// real unit lands this should be replaced with the imported type and the
/// sibling `geqo_rand`/`geqo_randint`.
#[repr(C)]
pub struct GeqoPrivateData {
    /// PRNG state used by geqo_rand (geqo_random.c).
    pub random_state: crate::common::pg_prng::pg_prng_state,
}

/// PlannerInfo is opaque here: the only thing `geqo_mutation` does with `root`
/// is forward it to `geqo_randint`, which reads the geqo RNG state. We model
/// just enough to reach that state.
#[repr(C)]
pub struct PlannerInfo {
    /// Mirrors `root->join_search_private` (a `void *` in C); for the geqo path
    /// it points at a `GeqoPrivateData`.
    pub join_search_private: *mut c_void,
}

/// geqo_rand (optimizer/geqo_random.h): returns a double in [0.0, 1.0).
///
/// Local model of the sibling `crate::optimizer::geqo::geqo_random::geqo_rand`
/// (not yet ported). Reads the `GeqoPrivateData` PRNG reached via
/// `root->join_search_private`.
///
/// # Safety
/// `root` must be a valid pointer whose `join_search_private` points at a live
/// `GeqoPrivateData`.
unsafe fn geqo_rand(root: *mut PlannerInfo) -> c_double {
    let private = (*root).join_search_private as *mut GeqoPrivateData;
    crate::common::pg_prng::pg_prng_double(&raw mut (*private).random_state)
}

/// geqo_randint (optimizer/geqo_random.h):
///   `((int) floor(geqo_rand(root) * (((upper) - (lower)) + 0.999999)) + (lower))`
/// i.e. a pseudo-random integer in the inclusive range [lower, upper].
///
/// Local model of the sibling
/// `crate::optimizer::geqo::geqo_random::geqo_randint` (not yet ported).
///
/// # Safety
/// See `geqo_rand`.
unsafe fn geqo_randint(root: *mut PlannerInfo, upper: c_int, lower: c_int) -> c_int {
    (geqo_rand(root) * (((upper - lower) as c_double) + 0.999999)).floor() as c_int + lower
}

/// geqo_mutation
///
/// 1:1 translation of geqo_mutation() in geqo_mutation.c. Performs a random
/// number of random pair-swaps within `tour`, leaving it a permutation of its
/// original cities (the multiset of genes is unchanged).
///
/// # Safety
/// `tour` must point to at least `num_gene` readable/writable `Gene`s. `root`
/// is forwarded to `geqo_randint` (see its safety contract).
pub unsafe fn geqo_mutation(root: *mut PlannerInfo, tour: *mut Gene, num_gene: c_int) {
    let mut swap1: c_int;
    let mut swap2: c_int;
    let mut num_swaps: c_int = geqo_randint(root, num_gene / 3, 0);
    let mut temp: Gene;

    while num_swaps > 0 {
        swap1 = geqo_randint(root, num_gene - 1, 0);
        swap2 = geqo_randint(root, num_gene - 1, 0);

        while swap1 == swap2 {
            swap2 = geqo_randint(root, num_gene - 1, 0);
        }

        temp = *tour.offset(swap1 as isize);
        *tour.offset(swap1 as isize) = *tour.offset(swap2 as isize);
        *tour.offset(swap2 as isize) = temp;

        num_swaps -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // geqo_mutation reads/writes a process-local stack tour and a stack
    // GeqoPrivateData, so no shared global state to serialize here.

    unsafe fn make_root(seed: u64) -> (Box<GeqoPrivateData>, Box<PlannerInfo>) {
        let mut private = Box::new(GeqoPrivateData {
            random_state: core::mem::zeroed(),
        });
        crate::common::pg_prng::pg_prng_seed(&raw mut private.random_state, seed);
        let root = Box::new(PlannerInfo {
            join_search_private: &mut *private as *mut GeqoPrivateData as *mut c_void,
        });
        (private, root)
    }

    fn sorted(v: &[Gene]) -> Vec<Gene> {
        let mut c = v.to_vec();
        c.sort_unstable();
        c
    }

    #[test]
    fn keeps_tour_a_valid_permutation() {
        unsafe {
            // Run across several seeds and several tour lengths to exercise
            // num_swaps == 0 and num_swaps > 0 paths.
            for &seed in &[1u64, 2, 7, 42, 12345, 0xDEAD_BEEF] {
                for &n in &[1i32, 2, 3, 5, 10, 33] {
                    let original: Vec<Gene> = (1..=n).collect();
                    let mut tour = original.clone();

                    let (_private, mut root) = make_root(seed);
                    geqo_mutation(&mut *root as *mut PlannerInfo, tour.as_mut_ptr(), n);

                    // Same multiset of cities (a valid permutation).
                    assert_eq!(
                        sorted(&tour),
                        sorted(&original),
                        "mutation changed the multiset (seed={seed}, n={n})"
                    );
                    // Length unchanged.
                    assert_eq!(tour.len(), original.len());
                }
            }
        }
    }

    #[test]
    fn swaps_are_in_bounds_and_self_swap_avoided() {
        // With num_gene < 3, num_gene/3 == 0 so num_swaps == 0: tour must be
        // returned untouched (degenerate but valid permutation).
        unsafe {
            for &n in &[1i32, 2] {
                let original: Vec<Gene> = (1..=n).collect();
                let mut tour = original.clone();
                let (_private, mut root) = make_root(99);
                geqo_mutation(&mut *root as *mut PlannerInfo, tour.as_mut_ptr(), n);
                assert_eq!(tour, original, "tiny tour (n={n}) should be unchanged");
            }
        }
    }
}
