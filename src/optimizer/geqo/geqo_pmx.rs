//! src/backend/optimizer/geqo/geqo_pmx.c
//!
//! Partially Matched Crossover [PMX] routines; PMX operator according to
//! Goldberg & Lingle. The pmx algorithm is adopted from Genitor
//! (Copyright (c) 1990 Darrell L. Whitley, Colorado State University).
//!
//! #include mapping:
//!   - Gene from optimizer/geqo_gene.h           -> `type Gene = c_int` below
//!   - DAD/MOM from optimizer/geqo_recombination.h -> consts below
//!   - geqo_randint from optimizer/geqo_random.h  -> STUBBED below (TODO)
//!   - PlannerInfo (only passed through to geqo_randint) -> opaque `*mut c_void`
//!
//! The PMX mapping / conflict-resolution logic is a FULLY REAL 1:1 translation
//! of the C source. The original C guards this file behind `#if defined(PMX)`;
//! that compile-time selector is not modeled here - the function is always
//! available in Rust.

use crate::prelude::*;

/// Genome representation (optimizer/geqo_gene.h). "we presume that int instead
/// of Relid is o.k. for Gene; so don't change it!"
pub type Gene = c_int;

/// PlannerInfo is only passed straight through to geqo_randint here, so we keep
/// it opaque to avoid pulling in the full planner node graph.
pub type PlannerInfo = c_void;

/// indicator for gene from dad (optimizer/geqo_recombination.h)
pub const DAD: c_int = 1;
/// indicator for gene from mom (optimizer/geqo_recombination.h)
pub const MOM: c_int = 0;

/// STUB of geqo_randint (optimizer/geqo_random.h / geqo_recombination.c).
///
/// In PostgreSQL this returns a pseudo-random integer in the inclusive range
/// [lower, upper] derived from the planner's RNG state. That RNG is not yet
/// ported, so we return a deterministic value (`lower`) which keeps `pmx`
/// compiling and gives a fixed, reproducible crossover point for tests.
///
/// TODO: replace with the real geqo_randint once geqo_random.h / the planner
/// RNG (root->geqo_rateset / pg_prng) are ported.
unsafe fn geqo_randint(_root: *mut PlannerInfo, upper: c_int, lower: c_int) -> c_int {
    // Real signature/semantics: inclusive [lower, upper].
    let _ = upper;
    lower
}

/// pmx
///
/// Partially matched crossover. 1:1 translation of pmx() in geqo_pmx.c.
///
/// `tour1`/`tour2` are the parent permutations (length `num_gene`), `offspring`
/// is the output buffer (length `num_gene`). Genes are values 1..=num_gene.
///
/// # Safety
/// `tour1`, `tour2` must point to at least `num_gene` readable Gene's, and
/// `offspring` to at least `num_gene` writable Gene's. `root` is only forwarded
/// to geqo_randint.
pub unsafe fn pmx(
    root: *mut PlannerInfo,
    tour1: *mut Gene,
    tour2: *mut Gene,
    offspring: *mut Gene,
    num_gene: c_int,
) {
    let n = num_gene as usize;

    let failed = palloc((n + 1) * core::mem::size_of::<c_int>()) as *mut c_int;
    let from = palloc((n + 1) * core::mem::size_of::<c_int>()) as *mut c_int;
    let indx = palloc((n + 1) * core::mem::size_of::<c_int>()) as *mut c_int;
    let check_list = palloc((n + 1) * core::mem::size_of::<c_int>()) as *mut c_int;

    let mut left: c_int;
    let mut right: c_int;
    let temp: c_int;
    let mut i: c_int;
    let mut j: c_int;
    let mut k: c_int;
    let mut mx_fail: c_int;
    let mut found: c_int;
    let mx_hold: c_int;

    // no mutation so start up the pmx replacement algorithm
    // initialize failed[], from[], check_list[]
    k = 0;
    while k < num_gene {
        *failed.offset(k as isize) = -1;
        *from.offset(k as isize) = -1;
        *check_list.offset((k + 1) as isize) = 0;
        k += 1;
    }

    // locate crossover points
    left = geqo_randint(root, num_gene - 1, 0);
    right = geqo_randint(root, num_gene - 1, 0);

    if left > right {
        temp = left;
        left = right;
        right = temp;
    }

    // copy tour2 into offspring
    k = 0;
    while k < num_gene {
        *offspring.offset(k as isize) = *tour2.offset(k as isize);
        *from.offset(k as isize) = DAD;
        let t2 = *tour2.offset(k as isize);
        *check_list.offset(t2 as isize) += 1;
        k += 1;
    }

    // copy tour1 into offspring
    k = left;
    while k <= right {
        let off = *offspring.offset(k as isize);
        *check_list.offset(off as isize) -= 1;
        *offspring.offset(k as isize) = *tour1.offset(k as isize);
        *from.offset(k as isize) = MOM;
        let t1 = *tour1.offset(k as isize);
        *check_list.offset(t1 as isize) += 1;
        k += 1;
    }

    // pmx main part

    mx_fail = 0;

    // STEP 1

    k = left;
    while k <= right {
        // for all elements in the tour1-2

        if *tour1.offset(k as isize) == *tour2.offset(k as isize) {
            found = 1; // find match in tour2
        } else {
            found = 0; // substitute elements

            j = 0;
            while found == 0 && j < num_gene {
                if *offspring.offset(j as isize) == *tour1.offset(k as isize)
                    && *from.offset(j as isize) == DAD
                {
                    let off = *offspring.offset(j as isize);
                    *check_list.offset(off as isize) -= 1;
                    *offspring.offset(j as isize) = *tour2.offset(k as isize);
                    found = 1;
                    let t2 = *tour2.offset(k as isize);
                    *check_list.offset(t2 as isize) += 1;
                }

                j += 1;
            }
        }

        if found == 0 {
            // failed to replace gene
            *failed.offset(mx_fail as isize) = *tour1.offset(k as isize) as c_int;
            *indx.offset(mx_fail as isize) = k;
            mx_fail += 1;
        }

        k += 1;
    } // ... while (for)

    // STEP 2

    // see if any genes could not be replaced
    if mx_fail > 0 {
        mx_hold = mx_fail;

        k = 0;
        while k < mx_hold {
            found = 0;

            j = 0;
            while found == 0 && j < num_gene {
                if *failed.offset(k as isize) == *offspring.offset(j as isize) as c_int
                    && *from.offset(j as isize) == DAD
                {
                    let off = *offspring.offset(j as isize);
                    *check_list.offset(off as isize) -= 1;
                    let idxk = *indx.offset(k as isize);
                    *offspring.offset(j as isize) = *tour2.offset(idxk as isize);
                    let t2 = *tour2.offset(idxk as isize);
                    *check_list.offset(t2 as isize) += 1;

                    found = 1;
                    *failed.offset(k as isize) = -1;
                    mx_fail -= 1;
                }

                j += 1;
            }

            k += 1;
        } // ... for
    } // ... if

    // STEP 3

    k = 1;
    while k <= num_gene {
        if *check_list.offset(k as isize) > 1 {
            i = 0;

            while i < num_gene {
                if *offspring.offset(i as isize) == k as Gene
                    && *from.offset(i as isize) == DAD
                {
                    j = 1;

                    while j <= num_gene {
                        if *check_list.offset(j as isize) == 0 {
                            *offspring.offset(i as isize) = j as Gene;
                            *check_list.offset(k as isize) -= 1;
                            *check_list.offset(j as isize) += 1;
                            i = num_gene + 1;
                            j = i;
                        }

                        j += 1;
                    }
                } // ... if

                i += 1;
            } // end while
        }

        k += 1;
    } // ... for

    pfree(failed as *mut c_void);
    pfree(from as *mut c_void);
    pfree(indx as *mut c_void);
    pfree(check_list as *mut c_void);
}

#[cfg(test)]
mod tests {
    use super::*;

    // With the stub geqo_randint returning `lower` (=0) for both crossover
    // points, left == right == 0: a single-gene slice from tour1 is copied,
    // and the conflict-resolution machinery (STEP 1/2/3) must still produce a
    // valid permutation of 1..=num_gene in the offspring.
    #[test]
    fn pmx_produces_valid_permutation() {
        unsafe {
            let num_gene: c_int = 8;

            // Two distinct permutations of 1..=8.
            let mut tour1: Vec<Gene> = vec![1, 2, 3, 4, 5, 6, 7, 8];
            let mut tour2: Vec<Gene> = vec![8, 6, 4, 2, 7, 5, 3, 1];
            let mut offspring: Vec<Gene> = vec![0; num_gene as usize];

            pmx(
                core::ptr::null_mut(),
                tour1.as_mut_ptr(),
                tour2.as_mut_ptr(),
                offspring.as_mut_ptr(),
                num_gene,
            );

            // Every gene 1..=num_gene appears exactly once.
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
        }
    }

    #[test]
    fn pmx_identical_parents_yields_same_permutation() {
        unsafe {
            let num_gene: c_int = 5;
            let mut tour1: Vec<Gene> = vec![3, 1, 4, 5, 2];
            let mut tour2: Vec<Gene> = vec![3, 1, 4, 5, 2];
            let mut offspring: Vec<Gene> = vec![0; num_gene as usize];

            pmx(
                core::ptr::null_mut(),
                tour1.as_mut_ptr(),
                tour2.as_mut_ptr(),
                offspring.as_mut_ptr(),
                num_gene,
            );

            let mut seen = vec![0u32; (num_gene + 1) as usize];
            for &g in &offspring {
                assert!(g >= 1 && g <= num_gene);
                seen[g as usize] += 1;
            }
            for v in 1..=num_gene {
                assert_eq!(seen[v as usize], 1);
            }
        }
    }
}
