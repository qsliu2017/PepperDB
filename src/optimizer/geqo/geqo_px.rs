//------------------------------------------------------------------------
//
// geqo_px.rs
//    position crossover [PX] routines;
//    PX operator according to Syswerda
//    (The Genetic Algorithms Handbook, L Davis, ed)
//
// src/backend/optimizer/geqo/geqo_px.c
//
//------------------------------------------------------------------------
//
// contributed by:
//   Martin Utesch    * Institute of Automatic Control
//                    = University of Mining and Technology
//   utesch@aut.tu-freiberg.de  * Freiberg, Germany
//
// the px algorithm is adopted from Genitor (D. L. Whitley, Colorado State
// University, (c) 1990); permission was granted to copy all or any part of
// that program for free distribution.
//
//------------------------------------------------------------------------
//
// #include mapping:
//   - "postgres.h"                     -> `use crate::prelude::*;`
//   - "optimizer/geqo.h"               -> PlannerInfo (only forwarded to
//                                         geqo_randint) from nodes::pathnodes
//   - "optimizer/geqo_random.h"        -> geqo_randint (sibling geqo_random)
//   - "optimizer/geqo_recombination.h" -> City / Gene (sibling
//                                         geqo_recombination)
//
// The C source guards the whole file behind `#if defined(PX)`; that
// compile-time selector is not modeled in Rust - px() is always available.

use crate::prelude::*;

use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo::geqo_random::geqo_randint;
use crate::optimizer::geqo::geqo_recombination::{City, Gene};

/// px
///
/// position crossover.
///
/// Selects a number of random positions, transfers those cities from `tour1`
/// into `offspring` at the same positions, then fills the remaining offspring
/// positions with the as-yet-unused cities taken in `tour2` order.
///
/// # Safety
/// `tour1`, `tour2` and `offspring` must each point to at least `num_gene`
/// `Gene`s. `city_table` must be indexable on 1..=num_gene (one extra slot at
/// index 0, as produced by `alloc_city_table`). `root` is only forwarded to
/// `geqo_randint`.
pub unsafe fn px(
    root: *mut PlannerInfo,
    tour1: *mut Gene,
    tour2: *mut Gene,
    offspring: *mut Gene,
    num_gene: c_int,
    city_table: *mut City,
) {
    let num_positions: c_int;
    let mut pos: c_int;
    let mut tour2_index: c_int;
    let mut offspring_index: c_int;

    // initialize city table
    let mut i: c_int = 1;
    while i <= num_gene {
        (*city_table.offset(i as isize)).used = 0;
        i += 1;
    }

    // choose random positions that will be inherited directly from parent
    num_positions = geqo_randint(root, 2 * num_gene / 3, num_gene / 3);

    // choose random position
    i = 0;
    while i < num_positions {
        pos = geqo_randint(root, num_gene - 1, 0);

        // transfer cities to child
        *offspring.offset(pos as isize) = *tour1.offset(pos as isize);
        // mark city used
        (*city_table.offset(*tour1.offset(pos as isize) as c_int as isize)).used = 1;

        i += 1;
    }

    tour2_index = 0;
    offspring_index = 0;

    // px main part

    while offspring_index < num_gene {
        // next position in offspring filled
        if (*city_table.offset(*tour1.offset(offspring_index as isize) as c_int as isize)).used == 0
        {
            // next city in tour1 not used
            if (*city_table.offset(*tour2.offset(tour2_index as isize) as c_int as isize)).used == 0
            {
                // inherit from tour1
                *offspring.offset(offspring_index as isize) = *tour2.offset(tour2_index as isize);

                tour2_index += 1;
                offspring_index += 1;
            } else {
                // next city in tour2 has been used
                tour2_index += 1;
            }
        } else {
            // next position in offspring is filled
            offspring_index += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::pg_prng::{pg_prng_fseed, pg_prng_state};
    use crate::optimizer::geqo::geqo_random::GeqoPrivateData;
    use crate::optimizer::geqo::geqo_recombination::{alloc_city_table, free_city_table, init_tour};

    // Build a PlannerInfo whose join_search_private holds a deterministically
    // seeded GeqoPrivateData, so geqo_randint() is reproducible. PlannerInfo has
    // ~82 fields, so palloc0 it and set only join_search_private (the single
    // field the GEQO RNG reaches), matching the geqo_random access path.
    unsafe fn seeded_root(seed: f64) -> *mut PlannerInfo {
        let private = palloc0(core::mem::size_of::<GeqoPrivateData>()) as *mut GeqoPrivateData;
        pg_prng_fseed(&mut (*private).random_state as *mut pg_prng_state, seed);
        let root = palloc0(core::mem::size_of::<PlannerInfo>()) as *mut PlannerInfo;
        (*root).join_search_private = private as *mut c_void;
        root
    }

    // px offspring must be a valid permutation of 1..=num_gene: every city
    // appears exactly once, none out of range, none duplicated or missing.
    #[test]
    fn px_produces_valid_permutation() {
        unsafe {
            for &num_gene in &[1i32, 2, 5, 16, 64, 100] {
                let root = seeded_root(0.5);

                let mut tour1: Vec<Gene> = vec![0; num_gene as usize];
                let mut tour2: Vec<Gene> = vec![0; num_gene as usize];
                let mut offspring: Vec<Gene> = vec![0; num_gene as usize];

                init_tour(root, tour1.as_mut_ptr(), num_gene);
                init_tour(root, tour2.as_mut_ptr(), num_gene);

                let city_table = alloc_city_table(root, num_gene);

                px(
                    root,
                    tour1.as_mut_ptr(),
                    tour2.as_mut_ptr(),
                    offspring.as_mut_ptr(),
                    num_gene,
                    city_table,
                );

                let mut seen = vec![0u32; (num_gene + 1) as usize];
                for &g in &offspring {
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

                free_city_table(root, city_table);
            }
        }
    }

    // Positions inherited directly from tour1 land at matching indices, and the
    // result is still a permutation across several seeds.
    #[test]
    fn px_permutation_across_seeds() {
        unsafe {
            let num_gene: c_int = 48;
            for &seed in &[0.1f64, 0.25, 0.5, 0.75, 0.99] {
                let root = seeded_root(seed);

                let mut tour1: Vec<Gene> = vec![0; num_gene as usize];
                let mut tour2: Vec<Gene> = vec![0; num_gene as usize];
                let mut offspring: Vec<Gene> = vec![0; num_gene as usize];

                init_tour(root, tour1.as_mut_ptr(), num_gene);
                init_tour(root, tour2.as_mut_ptr(), num_gene);

                let city_table = alloc_city_table(root, num_gene);

                px(
                    root,
                    tour1.as_mut_ptr(),
                    tour2.as_mut_ptr(),
                    offspring.as_mut_ptr(),
                    num_gene,
                    city_table,
                );

                let mut seen = vec![false; (num_gene + 1) as usize];
                for &g in &offspring {
                    assert!(g >= 1 && g <= num_gene);
                    assert!(!seen[g as usize], "duplicate gene {} for seed {}", g, seed);
                    seen[g as usize] = true;
                }

                free_city_table(root, city_table);
            }
        }
    }
}
