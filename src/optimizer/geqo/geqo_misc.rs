//------------------------------------------------------------------------
//
// geqo_misc.rs
//    misc. printout and debug stuff
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
//
// src/backend/optimizer/geqo/geqo_misc.c
//   (+ MERGED the prototypes from src/include/optimizer/geqo_misc.h)
//
//------------------------------------------------------------------------
//
// contributed by:
//   Martin Utesch    * Institute of Automatic Control
//                    = University of Mining and Technology
//   utesch@aut.tu-freiberg.de  * Freiberg, Germany
//
//------------------------------------------------------------------------
//
// #include mapping:
//   - "postgres.h"                     -> `use crate::prelude::*;`
//   - "optimizer/geqo_misc.h"          -> the print_* prototypes; MERGED here.
//   - Pool (optimizer/geqo_gene.h)     -> sibling geqo_pool module.
//   - Edge / Gene                      -> sibling geqo_recombination module.
//   - Chromosome                       -> sibling geqo_copy module (reached
//                                         through Pool::data; not named directly
//                                         here, but its `string` / `worth`
//                                         fields are what we read).
//   - PlannerInfo                      -> not needed by this file (none of the
//                                         debug routines take a root).
//
// In C, every routine here is wrapped in `#ifdef GEQO_DEBUG` and the FILE*
// printout helpers are conditionally compiled. PepperDB allows dead code, so we
// port them as REAL, always-available functions. The file prints are mirrored
// 1:1 onto a C `FILE *` via libc::fprintf / fflush so the on-the-wire output is
// byte-identical to the C source; only avg_pool carries returnable logic.

use core::ffi::c_double;

use crate::prelude::*;

use crate::optimizer::geqo::geqo_pool::Pool;
use crate::optimizer::geqo::geqo_recombination::Edge;

// FILE is opaque here; we only ever pass the pointer straight back to
// fprintf/fflush, mirroring the C source's `FILE *fp` parameter.
#[allow(non_camel_case_types)]
pub enum FILE {}

extern "C" {
    fn fprintf(stream: *mut FILE, format: *const c_char, ...) -> c_int;
    fn fflush(stream: *mut FILE) -> c_int;
}

// avg_pool
//
// Average fitness (`worth`) over the whole pool.
//
// # Safety
// `pool` must be a valid pointer whose `data` points to at least `size`
// Chromosomes.
unsafe fn avg_pool(pool: *mut Pool) -> c_double {
    let mut cumulative: c_double = 0.0;

    if (*pool).size <= 0 {
        elog!(ERROR, "pool_size is zero");
    }

    // Since the pool may contain multiple occurrences of DBL_MAX, divide by
    // pool->size before summing, not after, to avoid overflow.  This loses a
    // little in speed and accuracy, but this routine is only used for debug
    // printouts, so we don't care that much.
    let size = (*pool).size;
    let mut i: c_int = 0;
    while i < size {
        cumulative += (*(*pool).data.offset(i as isize)).worth / size as c_double;
        i += 1;
    }

    cumulative
}

// print_pool
//
// # Safety
// `fp` must be a valid open `FILE *`; `pool` must be valid with `data` pointing
// to at least `size` Chromosomes, each whose `string` holds at least
// `string_length` Genes.
pub unsafe fn print_pool(fp: *mut FILE, pool: *mut Pool, mut start: c_int, mut stop: c_int) {
    // be extra careful that start and stop are valid inputs

    if start < 0 {
        start = 0;
    }
    if stop > (*pool).size {
        stop = (*pool).size;
    }

    if start + stop > (*pool).size {
        start = 0;
        stop = (*pool).size;
    }

    let mut i: c_int = start;
    while i < stop {
        fprintf(fp, b"%d)\t\0".as_ptr() as *const c_char, i);
        let chromo = (*pool).data.offset(i as isize);
        let mut j: c_int = 0;
        while j < (*pool).string_length {
            fprintf(
                fp,
                b"%d \0".as_ptr() as *const c_char,
                *(*chromo).string.offset(j as isize),
            );
            j += 1;
        }
        fprintf(fp, b"%g\n\0".as_ptr() as *const c_char, (*chromo).worth);
        i += 1;
    }

    fflush(fp);
}

// print_gen
//
//   printout for chromosome: best, worst, mean, average
//
// # Safety
// As for `print_pool` (minus the per-gene access); `fp` and `pool` must be
// valid.
pub unsafe fn print_gen(fp: *mut FILE, pool: *mut Pool, generation: c_int) {
    // Get index to lowest ranking gene in population.
    // Use 2nd to last since last is buffer.
    let lowest: c_int = if (*pool).size > 1 {
        (*pool).size - 2
    } else {
        0
    };

    fprintf(
        fp,
        b"%5d | Best: %g  Worst: %g  Mean: %g  Avg: %g\n\0".as_ptr() as *const c_char,
        generation,
        (*(*pool).data.offset(0)).worth,
        (*(*pool).data.offset(lowest as isize)).worth,
        (*(*pool).data.offset(((*pool).size / 2) as isize)).worth,
        avg_pool(pool),
    );

    fflush(fp);
}

// print_edge_table
//
// # Safety
// `fp` must be a valid open `FILE *`; `edge_table` must point to at least
// `num_gene + 1` Edges (the table is 1-indexed in the loop below, matching C).
pub unsafe fn print_edge_table(fp: *mut FILE, edge_table: *mut Edge, num_gene: c_int) {
    fprintf(fp, b"\nEDGE TABLE\n\0".as_ptr() as *const c_char);

    let mut i: c_int = 1;
    while i <= num_gene {
        fprintf(fp, b"%d :\0".as_ptr() as *const c_char, i);
        let entry = edge_table.offset(i as isize);
        let mut j: c_int = 0;
        while j < (*entry).unused_edges {
            fprintf(
                fp,
                b" %d\0".as_ptr() as *const c_char,
                (*entry).edge_list[j as usize],
            );
            j += 1;
        }
        fprintf(fp, b"\n\0".as_ptr() as *const c_char);
        i += 1;
    }

    fprintf(fp, b"\n\0".as_ptr() as *const c_char);

    fflush(fp);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::geqo::geqo_copy::Chromosome;
    use crate::optimizer::geqo::geqo_recombination::Gene;

    // Build a tiny Pool of 2 chromosomes with known `worth` and assert avg_pool
    // returns their average. Mirrors the C avg_pool's Assert(size > 0) by only
    // exercising a size > 0 pool. Each chromosome gets a 1-gene `string`
    // (string_length == 1) so the struct is self-consistent, though avg_pool
    // never reads the tour.
    #[test]
    fn avg_pool_returns_mean_worth() {
        let mut g0: Gene = 0;
        let mut g1: Gene = 0;

        let mut chromos: [Chromosome; 2] = [
            Chromosome {
                string: &mut g0 as *mut Gene,
                worth: 10.0,
            },
            Chromosome {
                string: &mut g1 as *mut Gene,
                worth: 30.0,
            },
        ];

        let mut pool = Pool {
            data: chromos.as_mut_ptr(),
            size: 2,
            string_length: 1,
        };

        let avg = unsafe { avg_pool(&mut pool as *mut Pool) };
        // (10 + 30) / 2 == 20. avg_pool divides each worth by size first.
        assert!((avg - 20.0).abs() < 1e-9, "expected 20.0, got {}", avg);
    }

    #[test]
    fn avg_pool_single_chromosome() {
        let mut g: Gene = 0;
        let mut chromos = [Chromosome {
            string: &mut g as *mut Gene,
            worth: 42.5,
        }];
        let mut pool = Pool {
            data: chromos.as_mut_ptr(),
            size: 1,
            string_length: 1,
        };
        let avg = unsafe { avg_pool(&mut pool as *mut Pool) };
        assert!((avg - 42.5).abs() < 1e-9, "expected 42.5, got {}", avg);
    }
}
