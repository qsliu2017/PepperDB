//------------------------------------------------------------------------
//
// geqo_pool.rs
//    Genetic Algorithm (GA) pool stuff
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
//
// src/backend/optimizer/geqo/geqo_pool.c
//   (+ MERGED the Pool struct decl from src/include/optimizer/geqo_gene.h;
//      the Chromosome / Gene decls live in sibling modules, imported below)
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
//
// #include mapping:
//   - "postgres.h"                          -> `use crate::prelude::*;`
//   - <float.h> (DBL_MAX)                   -> `f64::MAX`
//   - "optimizer/geqo_copy.h"               -> Chromosome + geqo_copy from the
//                                              sibling geqo_copy module.
//   - "optimizer/geqo_pool.h"               -> the function prototypes; the Pool
//                                              struct it pulls in (via geqo.h ->
//                                              geqo_gene.h) is MERGED in below.
//   - "optimizer/geqo_recombination.h"      -> init_tour + Gene from the sibling
//                                              geqo_recombination module.
//   - PlannerInfo                           -> sibling nodes::pathnodes module.
//
// alloc_pool / free_pool / sort_pool / compare / alloc_chromo / free_chromo /
// spread_chromo are FULLY REAL 1:1 translations of the C source. random_init_pool
// is real too, except that geqo_eval (the cost evaluator, in geqo_eval.c which is
// NOT yet ported) is STUBBED below; see its TODO.

use core::ffi::c_double;

use crate::prelude::*;

use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo::geqo_copy::{geqo_copy, Chromosome};
use crate::optimizer::geqo::geqo_recombination::{init_tour, Gene};

// ---------------------------------------------------------------------------
// Pool (from optimizer/geqo_gene.h)
//
// A Pool is a flat vector of `size` Chromosomes (`data`), each of whose tours
// is `string_length` genes long (with one extra allocated slot, matching the C
// `(string_length + 1)` allocations).
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct Pool {
    pub data: *mut Chromosome,
    pub size: c_int,
    pub string_length: c_int,
}

// geqo_eval (STUB)
//
// TODO: real geqo_eval lives in src/backend/optimizer/geqo/geqo_eval.c, which
// is NOT yet ported. The real function builds a join RelOptInfo from the tour
// and returns its total cost (or DBL_MAX for an invalid/impossible plan). Until
// that is ported, we return a deterministic placeholder cost: the sum of the
// tour's genes as an f64. This is finite (< DBL_MAX) so random_init_pool will
// accept every tour, and it is order-sensitive enough for sort_pool /
// spread_chromo to have meaningful worths to order by in tests.
//
// # Safety
// `tour` must point to at least `num_gene` readable `Gene`s. `root` is unused.
unsafe fn geqo_eval(_root: *mut PlannerInfo, tour: *mut Gene, num_gene: c_int) -> c_double {
    let mut sum: c_double = 0.0;
    let mut i: c_int = 0;
    while i < num_gene {
        sum += *tour.offset(i as isize) as c_double;
        i += 1;
    }
    sum
}

// alloc_pool
//    allocates memory for GA pool
//
// # Safety
// Returned pointer must eventually be released with `free_pool`. `root` is only
// passed through (never dereferenced), matching the C source.
pub unsafe fn alloc_pool(
    _root: *mut PlannerInfo,
    pool_size: c_int,
    string_length: c_int,
) -> *mut Pool {
    // pool
    let new_pool = palloc(core::mem::size_of::<Pool>()) as *mut Pool;
    (*new_pool).size = pool_size;
    (*new_pool).string_length = string_length;

    // all chromosome
    (*new_pool).data =
        palloc((pool_size as usize) * core::mem::size_of::<Chromosome>()) as *mut Chromosome;

    // all gene
    let chromo = (*new_pool).data; // vector of all chromos
    let mut i: c_int = 0;
    while i < pool_size {
        (*chromo.offset(i as isize)).string =
            palloc(((string_length + 1) as usize) * core::mem::size_of::<Gene>()) as *mut Gene;
        i += 1;
    }

    new_pool
}

// free_pool
//    deallocates memory for GA pool
//
// # Safety
// `pool` must have come from `alloc_pool`. `root` is unused.
pub unsafe fn free_pool(_root: *mut PlannerInfo, pool: *mut Pool) {
    // all gene
    let chromo = (*pool).data; // vector of all chromos
    let mut i: c_int = 0;
    while i < (*pool).size {
        pfree((*chromo.offset(i as isize)).string as *mut c_void);
        i += 1;
    }

    // all chromosome
    pfree((*pool).data as *mut c_void);

    // pool
    pfree(pool as *mut c_void);
}

// random_init_pool
//    initialize genetic pool
//
// # Safety
// `pool` must have come from `alloc_pool`. `root` is forwarded to init_tour /
// geqo_eval.
pub unsafe fn random_init_pool(root: *mut PlannerInfo, pool: *mut Pool) {
    let chromo = (*pool).data;
    let mut bad: c_int = 0;

    // We immediately discard any invalid individuals (those that geqo_eval
    // returns DBL_MAX for), thereby not wasting pool space on them.
    //
    // If we fail to make any valid individuals after 10000 tries, give up;
    // this probably means something is broken, and we shouldn't just let
    // ourselves get stuck in an infinite loop.
    let mut i: c_int = 0;
    while i < (*pool).size {
        init_tour(root, (*chromo.offset(i as isize)).string, (*pool).string_length);
        (*(*pool).data.offset(i as isize)).worth = geqo_eval(
            root,
            (*chromo.offset(i as isize)).string,
            (*pool).string_length,
        );
        if (*(*pool).data.offset(i as isize)).worth < f64::MAX {
            i += 1;
        } else {
            bad += 1;
            if i == 0 && bad >= 10000 {
                elog!(ERROR, "geqo failed to make a valid plan");
                unreachable!();
            }
        }
    }

    // (GEQO_DEBUG block from the C source is compiled out by default.)
}

// sort_pool
//   sorts input pool according to worth, from smallest to largest
//
//   maybe you have to change compare() for different ordering ...
//
// # Safety
// `pool` must have come from `alloc_pool`. `root` is unused.
pub unsafe fn sort_pool(_root: *mut PlannerInfo, pool: *mut Pool) {
    // qsort(pool->data, pool->size, sizeof(Chromosome), compare)
    let slice = core::slice::from_raw_parts_mut((*pool).data, (*pool).size as usize);
    slice.sort_by(compare);
}

// compare
//   qsort comparison function for sort_pool
fn compare(chromo1: &Chromosome, chromo2: &Chromosome) -> core::cmp::Ordering {
    if chromo1.worth == chromo2.worth {
        core::cmp::Ordering::Equal
    } else if chromo1.worth > chromo2.worth {
        core::cmp::Ordering::Greater
    } else {
        core::cmp::Ordering::Less
    }
}

// alloc_chromo
//    allocates a chromosome and string space
//
// # Safety
// Returned pointer must eventually be released with `free_chromo`. `root` is
// unused.
pub unsafe fn alloc_chromo(_root: *mut PlannerInfo, string_length: c_int) -> *mut Chromosome {
    let chromo = palloc(core::mem::size_of::<Chromosome>()) as *mut Chromosome;
    (*chromo).string =
        palloc(((string_length + 1) as usize) * core::mem::size_of::<Gene>()) as *mut Gene;

    chromo
}

// free_chromo
//    deallocates a chromosome and string space
//
// # Safety
// `chromo` must have come from `alloc_chromo`. `root` is unused.
pub unsafe fn free_chromo(_root: *mut PlannerInfo, chromo: *mut Chromosome) {
    pfree((*chromo).string as *mut c_void);
    pfree(chromo as *mut c_void);
}

// spread_chromo
//   inserts a new chromosome into the pool, displacing worst gene in pool
//   assumes best->worst = smallest->largest
//
// # Safety
// `chromo` and `pool` must be valid; `pool` must have come from `alloc_pool`
// and be sorted ascending by worth. `root` is forwarded to geqo_copy.
pub unsafe fn spread_chromo(root: *mut PlannerInfo, chromo: *mut Chromosome, pool: *mut Pool) {
    let mut top: c_int;
    let mut mid: c_int;
    let mut bot: c_int;
    let mut index: c_int;
    // Rust won't let us field-assign an uninitialized Copy struct piecemeal;
    // initialize both fully (they are overwritten before use below).
    let mut swap_chromo = Chromosome {
        string: null_mut(),
        worth: 0.0,
    };
    let mut tmp_chromo = Chromosome {
        string: null_mut(),
        worth: 0.0,
    };

    // new chromo is so bad we can't use it
    if (*chromo).worth > (*(*pool).data.offset(((*pool).size - 1) as isize)).worth {
        return;
    }

    // do a binary search to find the index of the new chromo

    top = 0;
    mid = (*pool).size / 2;
    bot = (*pool).size - 1;
    index = -1;

    while index == -1 {
        // these 4 cases find a new location

        if (*chromo).worth <= (*(*pool).data.offset(top as isize)).worth {
            index = top;
        } else if (*chromo).worth == (*(*pool).data.offset(mid as isize)).worth {
            index = mid;
        } else if (*chromo).worth == (*(*pool).data.offset(bot as isize)).worth {
            index = bot;
        } else if bot - top <= 1 {
            index = bot;
        }
        // these 2 cases move the search indices since a new location has not
        // yet been found.
        else if (*chromo).worth < (*(*pool).data.offset(mid as isize)).worth {
            bot = mid;
            mid = top + ((bot - top) / 2);
        } else {
            // (chromo->worth > pool->data[mid].worth)
            top = mid;
            mid = top + ((bot - top) / 2);
        }
    } // ... while

    // now we have index for chromo

    // move every gene from index on down one position to make room for chromo

    // copy new gene into pool storage; always replace worst gene in pool

    geqo_copy(
        root,
        (*pool).data.offset(((*pool).size - 1) as isize),
        chromo,
        (*pool).string_length,
    );

    swap_chromo.string = (*(*pool).data.offset(((*pool).size - 1) as isize)).string;
    swap_chromo.worth = (*(*pool).data.offset(((*pool).size - 1) as isize)).worth;

    let mut i: c_int = index;
    while i < (*pool).size {
        tmp_chromo.string = (*(*pool).data.offset(i as isize)).string;
        tmp_chromo.worth = (*(*pool).data.offset(i as isize)).worth;

        (*(*pool).data.offset(i as isize)).string = swap_chromo.string;
        (*(*pool).data.offset(i as isize)).worth = swap_chromo.worth;

        swap_chromo.string = tmp_chromo.string;
        swap_chromo.worth = tmp_chromo.worth;

        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // alloc_pool / free_pool round-trip: the Pool's scalar fields are set, the
    // data vector and each per-chromosome string array are allocated (writable),
    // and free_pool releases them all without error.
    #[test]
    fn alloc_pool_free_pool_round_trip() {
        unsafe {
            let pool_size: c_int = 4;
            let string_length: c_int = 6;

            let pool = alloc_pool(core::ptr::null_mut(), pool_size, string_length);

            assert_eq!((*pool).size, pool_size);
            assert_eq!((*pool).string_length, string_length);
            assert!(!(*pool).data.is_null());

            // Every chromosome has a usable string array of at least
            // (string_length + 1) genes; write through the whole range to prove
            // it is allocated and distinct per chromosome.
            for c in 0..pool_size {
                let s = (*(*pool).data.offset(c as isize)).string;
                assert!(!s.is_null());
                for g in 0..=string_length {
                    *s.offset(g as isize) = (c * 100 + g) as Gene;
                }
            }
            for c in 0..pool_size {
                let s = (*(*pool).data.offset(c as isize)).string;
                for g in 0..=string_length {
                    assert_eq!(*s.offset(g as isize), (c * 100 + g) as Gene);
                }
            }

            free_pool(core::ptr::null_mut(), pool);
        }
    }

    // Helper: build a Pool of `worths.len()` chromosomes (already sorted
    // ascending by worth), each with a string array filled with a sentinel so
    // that geqo_copy inside spread_chromo has real genes to move around.
    unsafe fn build_pool(worths: &[c_double], string_length: c_int) -> *mut Pool {
        let pool = alloc_pool(core::ptr::null_mut(), worths.len() as c_int, string_length);
        for (i, &w) in worths.iter().enumerate() {
            (*(*pool).data.offset(i as isize)).worth = w;
            let s = (*(*pool).data.offset(i as isize)).string;
            for g in 0..=string_length {
                // distinct, worth-derived contents
                *s.offset(g as isize) = (w as c_int) * 10 + g;
            }
        }
        pool
    }

    unsafe fn is_sorted_ascending(pool: *mut Pool) -> bool {
        for i in 1..(*pool).size {
            if (*(*pool).data.offset((i - 1) as isize)).worth
                > (*(*pool).data.offset(i as isize)).worth
            {
                return false;
            }
        }
        true
    }

    // spread_chromo must keep the pool sorted ascending by worth after inserting
    // a new chromosome, displacing the worst entry. We hand-build a small sorted
    // pool and insert a value that belongs in the middle.
    #[test]
    fn spread_chromo_keeps_pool_sorted() {
        unsafe {
            let string_length: c_int = 3;
            // sorted ascending: 10, 20, 30, 40, 50
            let pool = build_pool(&[10.0, 20.0, 30.0, 40.0, 50.0], string_length);

            // new chromosome with worth 25 -> belongs between 20 and 30.
            let chromo = alloc_chromo(core::ptr::null_mut(), string_length);
            (*chromo).worth = 25.0;
            for g in 0..=string_length {
                *(*chromo).string.offset(g as isize) = 250 + g;
            }

            spread_chromo(core::ptr::null_mut(), chromo, pool);

            assert!(is_sorted_ascending(pool), "pool not sorted after insert");

            // The worst (50.0) must have been displaced; 25.0 must now be
            // present.
            let mut worths = Vec::new();
            for i in 0..(*pool).size {
                worths.push((*(*pool).data.offset(i as isize)).worth);
            }
            assert_eq!(worths, vec![10.0, 20.0, 25.0, 30.0, 40.0]);

            free_chromo(core::ptr::null_mut(), chromo);
            free_pool(core::ptr::null_mut(), pool);
        }
    }

    // A chromosome worse than the current worst must be rejected (pool
    // unchanged).
    #[test]
    fn spread_chromo_rejects_too_bad() {
        unsafe {
            let string_length: c_int = 2;
            let pool = build_pool(&[10.0, 20.0, 30.0], string_length);

            let chromo = alloc_chromo(core::ptr::null_mut(), string_length);
            (*chromo).worth = 999.0; // worse than worst (30.0)
            for g in 0..=string_length {
                *(*chromo).string.offset(g as isize) = g;
            }

            spread_chromo(core::ptr::null_mut(), chromo, pool);

            let mut worths = Vec::new();
            for i in 0..(*pool).size {
                worths.push((*(*pool).data.offset(i as isize)).worth);
            }
            assert_eq!(worths, vec![10.0, 20.0, 30.0]);

            free_chromo(core::ptr::null_mut(), chromo);
            free_pool(core::ptr::null_mut(), pool);
        }
    }

    // Inserting the new best (smaller than every existing worth) puts it at the
    // front and keeps the pool sorted.
    #[test]
    fn spread_chromo_new_best_goes_to_front() {
        unsafe {
            let string_length: c_int = 2;
            let pool = build_pool(&[10.0, 20.0, 30.0, 40.0], string_length);

            let chromo = alloc_chromo(core::ptr::null_mut(), string_length);
            (*chromo).worth = 5.0;
            for g in 0..=string_length {
                *(*chromo).string.offset(g as isize) = 50 + g;
            }

            spread_chromo(core::ptr::null_mut(), chromo, pool);

            assert!(is_sorted_ascending(pool));
            assert_eq!((*(*pool).data.offset(0)).worth, 5.0);

            let mut worths = Vec::new();
            for i in 0..(*pool).size {
                worths.push((*(*pool).data.offset(i as isize)).worth);
            }
            assert_eq!(worths, vec![5.0, 10.0, 20.0, 30.0]);

            free_chromo(core::ptr::null_mut(), chromo);
            free_pool(core::ptr::null_mut(), pool);
        }
    }

    // sort_pool sorts an out-of-order pool ascending by worth.
    #[test]
    fn sort_pool_orders_ascending() {
        unsafe {
            let string_length: c_int = 2;
            let pool = alloc_pool(core::ptr::null_mut(), 5, string_length);
            let unsorted = [30.0, 10.0, 50.0, 20.0, 40.0];
            for (i, &w) in unsorted.iter().enumerate() {
                (*(*pool).data.offset(i as isize)).worth = w;
            }

            sort_pool(core::ptr::null_mut(), pool);

            assert!(is_sorted_ascending(pool));
            let mut worths = Vec::new();
            for i in 0..(*pool).size {
                worths.push((*(*pool).data.offset(i as isize)).worth);
            }
            assert_eq!(worths, vec![10.0, 20.0, 30.0, 40.0, 50.0]);

            free_pool(core::ptr::null_mut(), pool);
        }
    }
}
