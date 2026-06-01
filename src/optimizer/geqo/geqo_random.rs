//------------------------------------------------------------------------
//
// geqo_random.rs
//    random number generator
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
//
// src/backend/optimizer/geqo/geqo_random.c
//   (+ MERGED src/include/optimizer/geqo_random.h)
//
//------------------------------------------------------------------------
//
// contributed by:
//   Martin Utesch    * Institute of Automatic Control
//                    = University of Mining and Technology
//   utesch@aut.tu-freiberg.de  * Freiberg, Germany
//
// -- parts of this are adapted from D. Whitley's Genitor algorithm --
//
//------------------------------------------------------------------------

use crate::prelude::*;

use crate::common::pg_prng::{pg_prng_double, pg_prng_fseed, pg_prng_state, pg_prng_uint64_range};
use crate::nodes::pathnodes::PlannerInfo;

// ---------------------------------------------------------------------------
// GeqoPrivateData (from optimizer/geqo.h)
//
// Private state for a GEQO run --- accessible via root->join_search_private.
// The `initial_rels` field is part of the C struct but geqo_random only ever
// touches `random_state`; we keep both for layout fidelity with geqo.h.
// `List` is opaque here (declared in nodes/pg_list.rs); we only need the pointer.
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct GeqoPrivateData {
    pub initial_rels: *mut crate::nodes::pg_list::List, // the base relations we are joining
    pub random_state: pg_prng_state,                    // PRNG state
}

// Helper to reach the GEQO private state from the planner root, mirroring
// the C cast `(GeqoPrivateData *) root->join_search_private`.
#[inline]
unsafe fn geqo_private(root: *mut PlannerInfo) -> *mut GeqoPrivateData {
    (*root).join_search_private as *mut GeqoPrivateData
}

// geqo_set_seed: seed the run's PRNG from a double in [0, 1].
pub unsafe fn geqo_set_seed(root: *mut PlannerInfo, seed: f64) {
    let private: *mut GeqoPrivateData = geqo_private(root);

    pg_prng_fseed(&mut (*private).random_state, seed);
}

// geqo_rand returns a random float value in the range [0.0, 1.0).
pub unsafe fn geqo_rand(root: *mut PlannerInfo) -> f64 {
    let private: *mut GeqoPrivateData = geqo_private(root);

    pg_prng_double(&mut (*private).random_state)
}

// geqo_randint returns integer value between lower and upper inclusive.
pub unsafe fn geqo_randint(root: *mut PlannerInfo, upper: c_int, lower: c_int) -> c_int {
    let private: *mut GeqoPrivateData = geqo_private(root);

    // In current usage, "lower" is never negative so we can just use
    // pg_prng_uint64_range directly.  This yields lower + floor(geqo_rand() *
    // (upper - lower + 1)), i.e. an inclusive integer in [lower, upper], exactly
    // as the geqo_random.h macro specified.
    pg_prng_uint64_range(
        &mut (*private).random_state,
        lower as uint64,
        upper as uint64,
    ) as c_int
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::pg_prng::pg_prng_seed;

    // Build a PlannerInfo whose join_search_private points at a seeded
    // GeqoPrivateData, run a closure with `root`, then tear it down.
    unsafe fn with_seeded_root<F: FnOnce(*mut PlannerInfo)>(seed: uint64, f: F) {
        let mut priv_data = GeqoPrivateData {
            initial_rels: null_mut(),
            random_state: pg_prng_state { s0: 0, s1: 0 },
        };
        pg_prng_seed(&mut priv_data.random_state, seed);

        // We only need a PlannerInfo big enough to carry join_search_private.
        // Zero-initialize the whole struct, then plant the private pointer.
        let mut root: PlannerInfo = std::mem::zeroed();
        root.join_search_private = (&mut priv_data as *mut GeqoPrivateData) as *mut c_void;

        f(&mut root as *mut PlannerInfo);
    }

    #[test]
    fn geqo_rand_in_unit_interval() {
        unsafe {
            with_seeded_root(0x1234_5678_9abc_def0, |root| {
                for _ in 0..10_000 {
                    let r = geqo_rand(root);
                    assert!(r >= 0.0 && r < 1.0, "geqo_rand out of [0,1): {r}");
                }
            });
        }
    }

    #[test]
    fn geqo_randint_within_inclusive_range() {
        unsafe {
            with_seeded_root(0xdead_beef_cafe_babe, |root| {
                let lo: c_int = 3;
                let hi: c_int = 17;
                let mut saw_lo = false;
                let mut saw_hi = false;
                for _ in 0..50_000 {
                    let v = geqo_randint(root, hi, lo);
                    assert!(v >= lo && v <= hi, "geqo_randint {v} not in [{lo},{hi}]");
                    if v == lo {
                        saw_lo = true;
                    }
                    if v == hi {
                        saw_hi = true;
                    }
                }
                // Over 50k draws across a 15-wide range, both endpoints must appear.
                assert!(saw_lo, "lower bound never produced");
                assert!(saw_hi, "upper bound never produced");
            });
        }
    }

    #[test]
    fn geqo_randint_singleton_range() {
        unsafe {
            with_seeded_root(42, |root| {
                // upper == lower must always return that single value.
                for _ in 0..1000 {
                    assert_eq!(geqo_randint(root, 9, 9), 9);
                }
            });
        }
    }
}
