//! src/backend/optimizer/geqo/geqo_ox2.c
//!
//! Order crossover [OX] routines; OX2 operator according to Syswerda
//! (The Genetic Algorithms Handbook, ed L Davis).
//!
//! The ox algorithm is adopted from Genitor (Copyright (c) 1990, Darrell L.
//! Whitley, Computer Science Department, Colorado State University).
//!
//! #include mapping:
//!   - "postgres.h"                     -> `use crate::prelude::*;`
//!   - "optimizer/geqo.h"               -> PlannerInfo is reached only to thread
//!                                         through to geqo_randint.
//!   - "optimizer/geqo_random.h"        -> geqo_randint (sibling geqo_random)
//!   - "optimizer/geqo_recombination.h" -> Gene / City (sibling
//!                                         geqo_recombination)
//!
//! The C source body is guarded by `#if defined(OX2)`; that compile-time
//! selector is not modeled - the function is always available in Rust.

use crate::prelude::*;

use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo::geqo_random::geqo_randint;
use crate::optimizer::geqo::geqo_recombination::{City, Gene};

/// ox2
///
/// Position crossover.
///
/// Selects a random subset of positions from `tour1`, then imposes the order in
/// which those cities appear in tour1 onto the slots of `tour2` that those
/// cities occupy, leaving every other slot inherited directly from tour2.
///
/// # Safety
/// `tour1`, `tour2`, and `offspring` must each point to at least `num_gene`
/// `Gene`s (offspring writable). `city_table` must have `num_gene + 1` entries
/// (index 0 unused for the 1..=num_gene addressing, matching alloc_city_table).
/// `root` is only forwarded to geqo_randint.
pub unsafe fn ox2(
    root: *mut PlannerInfo,
    tour1: *mut Gene,
    tour2: *mut Gene,
    offspring: *mut Gene,
    num_gene: c_int,
    city_table: *mut City,
) {
    let mut k: c_int;
    let mut j: c_int;
    let mut count: c_int;
    let mut pos: c_int;
    let mut select: c_int;
    let num_positions: c_int;

    /* initialize city table */
    k = 1;
    while k <= num_gene {
        (*city_table.offset(k as isize)).used = 0;
        (*city_table.offset((k - 1) as isize)).select_list = -1;
        k += 1;
    }

    /* determine the number of positions to be inherited from tour1  */
    num_positions = geqo_randint(root, 2 * num_gene / 3, num_gene / 3);

    /* make a list of selected cities */
    k = 0;
    while k < num_positions {
        pos = geqo_randint(root, num_gene - 1, 0);
        (*city_table.offset(pos as isize)).select_list = *tour1.offset(pos as isize) as c_int;
        (*city_table.offset(*tour1.offset(pos as isize) as isize)).used = 1; /* mark used */
        k += 1;
    }

    count = 0;
    k = 0;

    /* consolidate the select list to adjacent positions */
    while count < num_positions {
        if (*city_table.offset(k as isize)).select_list == -1 {
            j = k + 1;
            while ((*city_table.offset(j as isize)).select_list == -1) && (j < num_gene) {
                j += 1;
            }

            (*city_table.offset(k as isize)).select_list =
                (*city_table.offset(j as isize)).select_list;
            (*city_table.offset(j as isize)).select_list = -1;
            count += 1;
        } else {
            count += 1;
        }
        k += 1;
    }

    select = 0;

    k = 0;
    while k < num_gene {
        if (*city_table.offset(*tour2.offset(k as isize) as isize)).used != 0 {
            *offspring.offset(k as isize) =
                (*city_table.offset(select as isize)).select_list as Gene;
            select += 1; /* next city in  the select list   */
        } else {
            /* city isn't used yet, so inherit from tour2 */
            *offspring.offset(k as isize) = *tour2.offset(k as isize);
        }
        k += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::pg_prng::{pg_prng_fseed, pg_prng_state};
    use crate::optimizer::geqo::geqo_random::GeqoPrivateData;
    use crate::optimizer::geqo::geqo_recombination::{alloc_city_table, free_city_table, init_tour};

    // Build a PlannerInfo whose join_search_private holds a deterministically
    // seeded GeqoPrivateData, so geqo_randint() is reproducible. PlannerInfo
    // has ~82 fields, so build it zeroed via palloc0 and only set
    // join_search_private (the single field the GEQO RNG reaches), matching the
    // sibling geqo_random module's access path.
    unsafe fn seeded_root(seed: f64) -> *mut PlannerInfo {
        let private = palloc0(core::mem::size_of::<GeqoPrivateData>()) as *mut GeqoPrivateData;
        pg_prng_fseed(&mut (*private).random_state as *mut pg_prng_state, seed);
        let root = palloc0(core::mem::size_of::<PlannerInfo>()) as *mut PlannerInfo;
        (*root).join_search_private = private as *mut c_void;
        root
    }

    // ox2 offspring must be a valid permutation of 1..=num_gene: every city
    // exactly once, no out-of-range / duplicate / missing values.
    #[test]
    fn ox2_produces_valid_permutation() {
        unsafe {
            for &num_gene in &[4i32, 5, 8, 16, 32, 64] {
                for &seed in &[0.1f64, 0.5, 0.9] {
                    let root = seeded_root(seed);

                    let mut tour1: Vec<Gene> = vec![0; num_gene as usize];
                    let mut tour2: Vec<Gene> = vec![0; num_gene as usize];
                    let mut offspring: Vec<Gene> = vec![0; num_gene as usize];

                    init_tour(root, tour1.as_mut_ptr(), num_gene);
                    init_tour(root, tour2.as_mut_ptr(), num_gene);

                    let city_table = alloc_city_table(root, num_gene);

                    ox2(
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
                            "gene {} out of range 1..={} (num_gene={}, seed={})",
                            g,
                            num_gene,
                            num_gene,
                            seed
                        );
                        seen[g as usize] += 1;
                    }
                    for v in 1..=num_gene {
                        assert_eq!(
                            seen[v as usize], 1,
                            "gene {} appeared {} times (expected once) num_gene={} seed={}",
                            v, seen[v as usize], num_gene, seed
                        );
                    }

                    free_city_table(root, city_table);
                }
            }
        }
    }

    // Same seed and parents -> same offspring (RNG is deterministic).
    #[test]
    fn ox2_is_deterministic_for_fixed_seed() {
        unsafe {
            let num_gene: c_int = 24;

            let run = || -> Vec<Gene> {
                let root = seeded_root(0.42);
                let mut tour1: Vec<Gene> = vec![0; num_gene as usize];
                let mut tour2: Vec<Gene> = vec![0; num_gene as usize];
                let mut offspring: Vec<Gene> = vec![0; num_gene as usize];
                init_tour(root, tour1.as_mut_ptr(), num_gene);
                init_tour(root, tour2.as_mut_ptr(), num_gene);
                let city_table = alloc_city_table(root, num_gene);
                ox2(
                    root,
                    tour1.as_mut_ptr(),
                    tour2.as_mut_ptr(),
                    offspring.as_mut_ptr(),
                    num_gene,
                    city_table,
                );
                free_city_table(root, city_table);
                offspring
            };

            assert_eq!(run(), run());
        }
    }
}
