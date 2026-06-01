//! src/backend/optimizer/geqo/geqo_recombination.c
//!
//! Misc recombination procedures for the genetic query optimizer.
//! Parts adapted from D. Whitley's Genitor algorithm.
//!
//! #include mapping:
//!   - "postgres.h"                          -> `use crate::prelude::*;`
//!   - "optimizer/geqo_random.h"             -> geqo_randint (sibling module
//!                                              crate::optimizer::geqo::geqo_random)
//!   - "optimizer/geqo_recombination.h"      -> the Edge / City struct decls and
//!                                              the alloc prototypes are MERGED in
//!                                              below (this is their defining .c)
//!   - Gene from optimizer/geqo_gene.h       -> `type Gene = c_int` below
//!   - PlannerInfo (only passed through to geqo_randint) -> opaque alias to the
//!     sibling's PlannerInfo so `root` threads straight through.
//!
//! init_tour() is a FULLY REAL 1:1 translation of the C source (inside-out
//! Fisher-Yates shuffle). The C source guards alloc_city_table/free_city_table
//! behind `#if defined(CX) || defined(PX) || defined(OX1) || defined(OX2)`;
//! that compile-time selector is not modeled - the functions are always
//! available in Rust. (alloc_edge_table/free_edge_table live in geqo_pool.c /
//! the ERX crossover file in PostgreSQL, NOT in this .c, so they are only
//! declared in the header and are not defined here.)

use crate::prelude::*;

// geqo_randint lives in the sibling geqo_random module in this batch.
// Real signature/semantics: geqo_randint(root, upper, lower) returns an integer
// in the inclusive range [lower, upper] drawn from root's PRNG state.
use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo::geqo_random::geqo_randint;

/// Genome representation (optimizer/geqo_gene.h). "we presume that int instead
/// of Relid is o.k. for Gene; so don't change it!"
pub type Gene = c_int;

/// Edge recombination crossover [ERX] table entry
/// (from optimizer/geqo_recombination.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Edge {
    /// list of edges
    pub edge_list: [Gene; 4],
    pub total_edges: c_int,
    pub unused_edges: c_int,
}

/// City table entry, used by the CX / PX / OX1 / OX2 crossover methods
/// (from optimizer/geqo_recombination.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct City {
    pub tour2_position: c_int,
    pub tour1_position: c_int,
    pub used: c_int,
    pub select_list: c_int,
}

// indicator for gene from dad / mom (optimizer/geqo_recombination.h).
/// indicator for gene from dad
pub const DAD: c_int = 1;
/// indicator for gene from mom
pub const MOM: c_int = 0;

/// init_tour
///
/// Randomly generates a legal "traveling salesman" tour (i.e. where each point
/// is visited only once).
///
/// We fill the `tour[]` array with a random permutation of the numbers
/// 1 .. num_gene in one pass using the "inside-out" variant of the
/// Fisher-Yates shuffle algorithm. Notionally, we append each new value to the
/// array and then swap it with a randomly-chosen array element (possibly
/// including itself, else we fail to generate permutations with the last city
/// last). The swap step is optimized by combining it with the insertion.
///
/// # Safety
/// `tour` must point to at least `num_gene` writable `Gene`s. `root` is only
/// forwarded to geqo_randint.
pub unsafe fn init_tour(root: *mut PlannerInfo, tour: *mut Gene, num_gene: c_int) {
    let mut j: c_int;

    if num_gene > 0 {
        *tour.offset(0) = 1 as Gene;
    }

    let mut i: c_int = 1;
    while i < num_gene {
        j = geqo_randint(root, i, 0);
        // i != j check avoids fetching uninitialized array element
        if i != j {
            *tour.offset(i as isize) = *tour.offset(j as isize);
        }
        *tour.offset(j as isize) = (i + 1) as Gene;
        i += 1;
    }
}

/// alloc_city_table
///
/// Allocate memory for city table.
///
/// palloc one extra location so that nodes numbered 1..n can be indexed
/// directly; index 0 will not be used.
///
/// # Safety
/// Returned pointer must eventually be released with `free_city_table`. `root`
/// is unused (kept for signature fidelity with the C source).
pub unsafe fn alloc_city_table(_root: *mut PlannerInfo, num_gene: c_int) -> *mut City {
    let city_table =
        palloc(((num_gene + 1) as usize) * core::mem::size_of::<City>()) as *mut City;

    city_table
}

/// free_city_table
///
/// Deallocate memory of city table.
///
/// # Safety
/// `city_table` must have come from `alloc_city_table`. `root` is unused.
pub unsafe fn free_city_table(_root: *mut PlannerInfo, city_table: *mut City) {
    pfree(city_table as *mut c_void);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::pg_prng::{pg_prng_fseed, pg_prng_state};
    use crate::optimizer::geqo::geqo_random::GeqoPrivateData;

    // Build a PlannerInfo whose join_search_private holds a deterministically
    // seeded GeqoPrivateData, so geqo_randint() (via the sibling module) is
    // reproducible. The exact construction must match how the sibling
    // geqo_random module reaches root->join_search_private->random_state.
    // PlannerInfo has ~82 fields, so build it zeroed via palloc0 and only set
    // join_search_private (the single field the GEQO RNG reaches), matching the
    // sibling geqo_random module's access path.
    unsafe fn seeded_root(seed: f64) -> *mut PlannerInfo {
        let private = palloc0(core::mem::size_of::<GeqoPrivateData>()) as *mut GeqoPrivateData;
        pg_prng_fseed(&mut (*private).random_state as *mut pg_prng_state, seed);
        let root = palloc0(core::mem::size_of::<PlannerInfo>()) as *mut PlannerInfo;
        (*root).join_search_private = private as *mut c_void;
        root
    }

    // init_tour must emit a valid permutation of 1..=num_gene: every city
    // exactly once, no out-of-range / duplicate / missing values.
    #[test]
    fn init_tour_produces_valid_permutation() {
        unsafe {
            for &num_gene in &[1i32, 2, 5, 16, 64] {
                let root = seeded_root(0.5);

                let mut tour: Vec<Gene> = vec![0; num_gene as usize];
                init_tour(
                    root,
                    tour.as_mut_ptr(),
                    num_gene,
                );

                let mut seen = vec![0u32; (num_gene + 1) as usize];
                for &g in &tour {
                    assert!(
                        g >= 1 && g <= num_gene,
                        "gene {} out of range 1..={}",
                        g,
                        num_gene
                    );
                    seen[g as usize] += 1;
                }
                for v in 1..=num_gene {
                    assert_eq!(
                        seen[v as usize], 1,
                        "gene {} appeared {} times (expected exactly once)",
                        v, seen[v as usize]
                    );
                }
            }
        }
    }

    // Same seed -> same tour (the RNG is deterministic).
    #[test]
    fn init_tour_is_deterministic_for_fixed_seed() {
        unsafe {
            let num_gene: c_int = 32;

            let r1 = seeded_root(0.25);
            let mut t1: Vec<Gene> = vec![0; num_gene as usize];
            init_tour(r1, t1.as_mut_ptr(), num_gene);

            let r2 = seeded_root(0.25);
            let mut t2: Vec<Gene> = vec![0; num_gene as usize];
            init_tour(r2, t2.as_mut_ptr(), num_gene);

            assert_eq!(t1, t2);
        }
    }

    // alloc/free round-trip for the city table; one extra slot so indices
    // 1..=num_gene are directly addressable.
    #[test]
    fn city_table_alloc_indexable_1_to_n() {
        unsafe {
            let num_gene: c_int = 10;
            let ct = alloc_city_table(core::ptr::null_mut(), num_gene);
            for i in 1..=num_gene {
                (*ct.offset(i as isize)).used = i;
            }
            for i in 1..=num_gene {
                assert_eq!((*ct.offset(i as isize)).used, i);
            }
            free_city_table(core::ptr::null_mut(), ct);
        }
    }
}
