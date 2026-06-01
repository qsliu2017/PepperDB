//------------------------------------------------------------------------
//
// geqo_ox1.rs
//    order crossover [OX] routines;
//    OX1 operator according to Davis
//    (Proc Int'l Joint Conf on AI)
//
// src/backend/optimizer/geqo/geqo_ox1.c
//   (+ MERGED decls from optimizer/geqo_random.h and
//      optimizer/geqo_recombination.h, which are imported from their
//      sibling defining modules rather than redeclared here)
//
//------------------------------------------------------------------------
//
// contributed by:
//   Martin Utesch    * Institute of Automatic Control
//                    = University of Mining and Technology
//   utesch@aut.tu-freiberg.de  * Freiberg, Germany
//
// the ox algorithm is adopted from Genitor:
//   Copyright (c) 1990  Darrell L. Whitley
//   Computer Science Department, Colorado State University
//
//------------------------------------------------------------------------
//
// The C source body is guarded by `#if defined(OX1)`; that compile-time
// selector is not modeled - the function is always available in Rust.
//
// #include mapping:
//   - "postgres.h"                     -> `use crate::prelude::*;`
//   - "optimizer/geqo.h"               -> PlannerInfo (only forwarded to
//                                         geqo_randint) from nodes::pathnodes
//   - "optimizer/geqo_random.h"        -> geqo_randint (sibling geqo_random)
//   - "optimizer/geqo_recombination.h" -> City / Gene (sibling
//                                         geqo_recombination)
//
//------------------------------------------------------------------------

use crate::prelude::*;

use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo::geqo_random::geqo_randint;
use crate::optimizer::geqo::geqo_recombination::{City, Gene};

/// ox1
///
/// position crossover.
///
/// Copies a randomly-chosen contiguous slice [left, right] from `tour1` into
/// `offspring` at the same positions, marking those cities used in
/// `city_table`. The remaining offspring positions (walking circularly from
/// just past `right`) are filled in `tour2` order, skipping any city already
/// used. The result is a valid permutation of `tour1` (equivalently of
/// 1..=num_gene when the inputs are such permutations).
///
/// # Safety
/// `tour1`, `tour2`, `offspring` must each point to at least `num_gene`
/// `Gene`s; `city_table` must point to at least `num_gene + 1` `City`s (it is
/// indexed 1..=num_gene). `root` is only forwarded to geqo_randint.
pub unsafe fn ox1(
    root: *mut PlannerInfo,
    tour1: *mut Gene,
    tour2: *mut Gene,
    offspring: *mut Gene,
    num_gene: c_int,
    city_table: *mut City,
) {
    let mut left: c_int;
    let mut right: c_int;
    let mut k: c_int;
    let mut p: c_int;

    // initialize city table
    k = 1;
    while k <= num_gene {
        (*city_table.offset(k as isize)).used = 0;
        k += 1;
    }

    // select portion to copy from tour1
    left = geqo_randint(root, num_gene - 1, 0);
    right = geqo_randint(root, num_gene - 1, 0);

    if left > right {
        let temp: c_int = left;
        left = right;
        right = temp;
    }

    // copy portion from tour1 to offspring
    k = left;
    while k <= right {
        *offspring.offset(k as isize) = *tour1.offset(k as isize);
        (*city_table.offset(*tour1.offset(k as isize) as isize)).used = 1;
        k += 1;
    }

    k = (right + 1) % num_gene; // index into offspring
    p = k; // index into tour2

    // copy stuff from tour2 to offspring
    while k != left {
        if (*city_table.offset(*tour2.offset(p as isize) as isize)).used == 0 {
            *offspring.offset(k as isize) = *tour2.offset(p as isize);
            k = (k + 1) % num_gene;
            (*city_table.offset(*tour2.offset(p as isize) as isize)).used = 1;
        }
        p = (p + 1) % num_gene; // increment tour2-index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::pg_prng::{pg_prng_fseed, pg_prng_state};
    use crate::optimizer::geqo::geqo_random::GeqoPrivateData;

    // Build a PlannerInfo whose join_search_private holds a deterministically
    // seeded GeqoPrivateData so geqo_randint() is reproducible. PlannerInfo has
    // ~82 fields, so build it zeroed via palloc0 and set only join_search_private
    // (the single field the GEQO RNG reaches), matching the sibling
    // geqo_random module's access path.
    unsafe fn seeded_root(seed: f64) -> *mut PlannerInfo {
        let private = palloc0(core::mem::size_of::<GeqoPrivateData>()) as *mut GeqoPrivateData;
        pg_prng_fseed(&mut (*private).random_state as *mut pg_prng_state, seed);
        let root = palloc0(core::mem::size_of::<PlannerInfo>()) as *mut PlannerInfo;
        (*root).join_search_private = private as *mut c_void;
        root
    }

    // Allocate a zeroed city_table of num_gene + 1 entries (1-indexed by ox1).
    unsafe fn city_table(num_gene: c_int) -> *mut City {
        palloc0((num_gene as usize + 1) * core::mem::size_of::<City>()) as *mut City
    }

    // ox1 offspring must be a valid permutation of 1..=num_gene: every city in
    // range, each exactly once. tour1/tour2 are themselves permutations so the
    // copied slice plus the tour2-ordered fill must reconstruct a full tour.
    #[test]
    fn ox1_produces_valid_permutation() {
        unsafe {
            for &num_gene in &[1i32, 2, 5, 16, 64] {
                let root = seeded_root(0.5);

                // tour1 = 1..=num_gene ; tour2 = reverse, two distinct
                // permutations of the same city set.
                let tour1: Vec<Gene> = (1..=num_gene).collect();
                let tour2: Vec<Gene> = (1..=num_gene).rev().collect();
                let mut offspring: Vec<Gene> = vec![0; num_gene as usize];
                let ctab = city_table(num_gene);

                let mut t1 = tour1.clone();
                let mut t2 = tour2.clone();
                ox1(
                    root,
                    t1.as_mut_ptr(),
                    t2.as_mut_ptr(),
                    offspring.as_mut_ptr(),
                    num_gene,
                    ctab,
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
                        "gene {} appeared {} times (expected exactly once) for num_gene={}",
                        v, seen[v as usize], num_gene
                    );
                }
            }
        }
    }

    // Same seed + same parents -> same offspring (the RNG is deterministic).
    #[test]
    fn ox1_is_deterministic_for_fixed_seed() {
        unsafe {
            let num_gene: c_int = 32;
            let tour1: Vec<Gene> = (1..=num_gene).collect();
            let tour2: Vec<Gene> = (1..=num_gene).rev().collect();

            let r1 = seeded_root(0.25);
            let mut a1 = tour1.clone();
            let mut b1 = tour2.clone();
            let mut o1: Vec<Gene> = vec![0; num_gene as usize];
            ox1(r1, a1.as_mut_ptr(), b1.as_mut_ptr(), o1.as_mut_ptr(), num_gene, city_table(num_gene));

            let r2 = seeded_root(0.25);
            let mut a2 = tour1.clone();
            let mut b2 = tour2.clone();
            let mut o2: Vec<Gene> = vec![0; num_gene as usize];
            ox1(r2, a2.as_mut_ptr(), b2.as_mut_ptr(), o2.as_mut_ptr(), num_gene, city_table(num_gene));

            assert_eq!(o1, o2);
        }
    }
}
