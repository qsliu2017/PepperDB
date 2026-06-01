//! src/backend/optimizer/geqo/geqo_cx.c
//!
//! Cycle Crossover [CX] routines; CX operator according to Oliver et al
//! (Proc 2nd Int'l Conf on GA's). The cx algorithm is adopted from Genitor
//! (Copyright (c) 1990 Darrell L. Whitley, Colorado State University).
//!
//! #include mapping:
//!   - Gene from optimizer/geqo_gene.h            -> `type Gene = c_int` below
//!   - City from optimizer/geqo_recombination.h   -> `struct City` below
//!   - geqo_randint from optimizer/geqo_random.h  -> STUBBED below (TODO)
//!   - PlannerInfo (only passed through to geqo_randint) -> opaque `*mut c_void`
//!
//! The cycle-following / fill logic is a FULLY REAL 1:1 translation of the C
//! source. The original C guards this file behind `#if defined(CX)`; that
//! compile-time selector is not modeled here - the function is always
//! available in Rust.

use crate::prelude::*;

/// Genome representation (optimizer/geqo_gene.h). "we presume that int instead
/// of Relid is o.k. for Gene; so don't change it!"
pub type Gene = c_int;

/// PlannerInfo is only passed straight through to geqo_randint here, so we keep
/// it opaque to avoid pulling in the full planner node graph.
pub type PlannerInfo = c_void;

/// City table entry (optimizer/geqo_recombination.h).
///
/// Defined locally because geqo_recombination.rs (which owns the canonical
/// `City` / `alloc_city_table`) is not yet ported. This is a 1:1 mirror of the
/// C `typedef struct City`. When geqo_recombination.rs lands, this should be
/// replaced by an import of that struct.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct City {
    pub tour2_position: c_int,
    pub tour1_position: c_int,
    pub used: c_int,
    pub select_list: c_int,
}

/// STUB of geqo_randint (optimizer/geqo_random.h / geqo_recombination.c).
///
/// In PostgreSQL this returns a pseudo-random integer in the inclusive range
/// [lower, upper] derived from the planner's RNG state. That RNG is not yet
/// ported, so we return a deterministic value (`lower`) which keeps `cx`
/// compiling and gives a fixed, reproducible cycle start for tests.
///
/// TODO: replace with the real geqo_randint once geqo_random.h / the planner
/// RNG (root->geqo_rateset / pg_prng) are ported.
unsafe fn geqo_randint(_root: *mut PlannerInfo, upper: c_int, lower: c_int) -> c_int {
    // Real signature/semantics: inclusive [lower, upper].
    let _ = upper;
    lower
}

/// cx
///
/// Cycle crossover. 1:1 translation of cx() in geqo_cx.c.
///
/// Builds `offspring` by following cycles between the two parent tours `tour1`
/// and `tour2` (each a permutation of 1..=num_gene). `city_table` is scratch
/// space of length `num_gene + 1` (1-based indexing on gene values).
///
/// Returns `num_diffs`: the number of positions where tour1 differs from the
/// produced offspring, but only when a complete tour could not be formed by
/// STEP 1 + STEP 2; otherwise 0. (This mirrors the C return value exactly.)
///
/// # Safety
/// `tour1`, `tour2` must point to at least `num_gene` readable Gene's;
/// `offspring` to at least `num_gene` writable Gene's; `city_table` to at least
/// `num_gene + 1` City entries. `root` is only forwarded to geqo_randint.
pub unsafe fn cx(
    root: *mut PlannerInfo,
    tour1: *mut Gene,
    tour2: *mut Gene,
    offspring: *mut Gene,
    num_gene: c_int,
    city_table: *mut City,
) -> c_int {
    let mut i: c_int;
    let start_pos: c_int;
    let mut curr_pos: c_int;
    let mut count: c_int = 0;
    let mut num_diffs: c_int = 0;

    // initialize city table
    i = 1;
    while i <= num_gene {
        (*city_table.offset(i as isize)).used = 0;
        let t2 = *tour2.offset((i - 1) as isize);
        (*city_table.offset(t2 as isize)).tour2_position = i - 1;
        let t1 = *tour1.offset((i - 1) as isize);
        (*city_table.offset(t1 as isize)).tour1_position = i - 1;
        i += 1;
    }

    // choose random cycle starting position
    start_pos = geqo_randint(root, num_gene - 1, 0);

    // child inherits first city
    *offspring.offset(start_pos as isize) = *tour1.offset(start_pos as isize);

    // begin cycle with tour1
    curr_pos = start_pos;
    let t1_start = *tour1.offset(start_pos as isize);
    (*city_table.offset(t1_start as isize)).used = 1;

    count += 1;

    // cx main part

    // STEP 1

    while *tour2.offset(curr_pos as isize) != *tour1.offset(start_pos as isize) {
        let t2_curr = *tour2.offset(curr_pos as isize);
        (*city_table.offset(t2_curr as isize)).used = 1;
        curr_pos = (*city_table.offset(t2_curr as isize)).tour1_position;
        *offspring.offset(curr_pos as isize) = *tour1.offset(curr_pos as isize);
        count += 1;
    }

    // STEP 2

    // failed to create a complete tour
    if count < num_gene {
        i = 1;
        while i <= num_gene {
            if (*city_table.offset(i as isize)).used == 0 {
                let pos = (*city_table.offset(i as isize)).tour2_position;
                *offspring.offset(pos as isize) = *tour2.offset(pos as isize);
                count += 1;
            }
            i += 1;
        }
    }

    // STEP 3

    // still failed to create a complete tour
    if count < num_gene {
        // count the number of differences between mom and offspring
        i = 0;
        while i < num_gene {
            if *tour1.offset(i as isize) != *offspring.offset(i as isize) {
                num_diffs += 1;
            }
            i += 1;
        }
    }

    num_diffs
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal seeded PRNG standing in for the not-yet-ported planner RNG /
    /// GeqoPrivateData. xorshift64* gives reproducible cycle starts so the test
    /// exercises real cycle-following from a non-zero `start_pos` as well.
    struct SeededRng {
        state: u64,
    }

    impl SeededRng {
        fn new(seed: u64) -> Self {
            SeededRng {
                state: seed | 1, // avoid the all-zero fixed point
            }
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.state;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.state = x;
            x.wrapping_mul(0x2545F4914F6CDD1D)
        }
        /// inclusive [lower, upper], matching geqo_randint semantics
        fn randint(&mut self, upper: c_int, lower: c_int) -> c_int {
            let span = (upper - lower + 1) as u64;
            lower + (self.next_u64() % span) as c_int
        }
    }

    /// Like `cx`, but drives the starting position from a caller-supplied seed
    /// instead of the stub geqo_randint. The body is otherwise identical to
    /// `cx`, so this validates the real cycle-following logic across many start
    /// positions. (When the real RNG is ported, `cx` itself covers this.)
    unsafe fn cx_seeded(
        tour1: *mut Gene,
        tour2: *mut Gene,
        offspring: *mut Gene,
        num_gene: c_int,
        city_table: *mut City,
        rng: &mut SeededRng,
    ) -> c_int {
        let mut i: c_int;
        let start_pos: c_int;
        let mut curr_pos: c_int;
        let mut count: c_int = 0;
        let mut num_diffs: c_int = 0;

        i = 1;
        while i <= num_gene {
            (*city_table.offset(i as isize)).used = 0;
            let t2 = *tour2.offset((i - 1) as isize);
            (*city_table.offset(t2 as isize)).tour2_position = i - 1;
            let t1 = *tour1.offset((i - 1) as isize);
            (*city_table.offset(t1 as isize)).tour1_position = i - 1;
            i += 1;
        }

        start_pos = rng.randint(num_gene - 1, 0);

        *offspring.offset(start_pos as isize) = *tour1.offset(start_pos as isize);
        curr_pos = start_pos;
        let t1_start = *tour1.offset(start_pos as isize);
        (*city_table.offset(t1_start as isize)).used = 1;
        count += 1;

        while *tour2.offset(curr_pos as isize) != *tour1.offset(start_pos as isize) {
            let t2_curr = *tour2.offset(curr_pos as isize);
            (*city_table.offset(t2_curr as isize)).used = 1;
            curr_pos = (*city_table.offset(t2_curr as isize)).tour1_position;
            *offspring.offset(curr_pos as isize) = *tour1.offset(curr_pos as isize);
            count += 1;
        }

        if count < num_gene {
            i = 1;
            while i <= num_gene {
                if (*city_table.offset(i as isize)).used == 0 {
                    let pos = (*city_table.offset(i as isize)).tour2_position;
                    *offspring.offset(pos as isize) = *tour2.offset(pos as isize);
                    count += 1;
                }
                i += 1;
            }
        }

        if count < num_gene {
            i = 0;
            while i < num_gene {
                if *tour1.offset(i as isize) != *offspring.offset(i as isize) {
                    num_diffs += 1;
                }
                i += 1;
            }
        }

        num_diffs
    }

    fn assert_valid_permutation(offspring: &[Gene], num_gene: c_int) {
        let mut seen = vec![0u32; (num_gene + 1) as usize];
        for &g in offspring {
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

    /// With the stub geqo_randint (start_pos = 0), cx must still produce a valid
    /// permutation of 1..=num_gene for two hand-built parent permutations.
    #[test]
    fn cx_produces_valid_permutation() {
        unsafe {
            let num_gene: c_int = 8;

            let mut tour1: Vec<Gene> = vec![1, 2, 3, 4, 5, 6, 7, 8];
            let mut tour2: Vec<Gene> = vec![8, 6, 4, 2, 7, 5, 3, 1];
            let mut offspring: Vec<Gene> = vec![0; num_gene as usize];
            // city_table is 1-based: needs num_gene + 1 entries.
            let mut city_table: Vec<City> = vec![City::default(); (num_gene + 1) as usize];

            cx(
                core::ptr::null_mut(),
                tour1.as_mut_ptr(),
                tour2.as_mut_ptr(),
                offspring.as_mut_ptr(),
                num_gene,
                city_table.as_mut_ptr(),
            );

            assert_valid_permutation(&offspring, num_gene);
        }
    }

    /// Drive the cycle start from a seeded RNG over many start positions; every
    /// resulting offspring must be a valid permutation of 1..=num_gene.
    #[test]
    fn cx_seeded_starts_always_valid_permutation() {
        unsafe {
            let num_gene: c_int = 8;
            let tour1_src: Vec<Gene> = vec![1, 2, 3, 4, 5, 6, 7, 8];
            let tour2_src: Vec<Gene> = vec![8, 6, 4, 2, 7, 5, 3, 1];

            let mut rng = SeededRng::new(0x1234_5678_9ABC_DEF0);

            for _ in 0..200 {
                let mut tour1 = tour1_src.clone();
                let mut tour2 = tour2_src.clone();
                let mut offspring: Vec<Gene> = vec![0; num_gene as usize];
                let mut city_table: Vec<City> =
                    vec![City::default(); (num_gene + 1) as usize];

                cx_seeded(
                    tour1.as_mut_ptr(),
                    tour2.as_mut_ptr(),
                    offspring.as_mut_ptr(),
                    num_gene,
                    city_table.as_mut_ptr(),
                    &mut rng,
                );

                assert_valid_permutation(&offspring, num_gene);
            }
        }
    }

    /// Identical parents: the whole tour is one trivial cycle, so the offspring
    /// must equal tour1 exactly and num_diffs must be 0.
    #[test]
    fn cx_identical_parents_copies_tour1() {
        unsafe {
            let num_gene: c_int = 5;
            let mut tour1: Vec<Gene> = vec![3, 1, 4, 5, 2];
            let mut tour2: Vec<Gene> = vec![3, 1, 4, 5, 2];
            let mut offspring: Vec<Gene> = vec![0; num_gene as usize];
            let mut city_table: Vec<City> = vec![City::default(); (num_gene + 1) as usize];

            let num_diffs = cx(
                core::ptr::null_mut(),
                tour1.as_mut_ptr(),
                tour2.as_mut_ptr(),
                offspring.as_mut_ptr(),
                num_gene,
                city_table.as_mut_ptr(),
            );

            assert_valid_permutation(&offspring, num_gene);
            assert_eq!(offspring, tour1, "identical parents must copy tour1");
            assert_eq!(num_diffs, 0, "complete tour from STEP 1 => num_diffs 0");
        }
    }
}
