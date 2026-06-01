//------------------------------------------------------------------------
//
// geqo_erx.rs
//    edge recombination crossover [ER]
//
// src/backend/optimizer/geqo/geqo_erx.c
//   (+ MERGED Edge / alloc_edge_table / free_edge_table / gimme_edge_table /
//    gimme_tour prototypes from optimizer/geqo_recombination.h)
//
//------------------------------------------------------------------------
//
// contributed by:
//   Martin Utesch    * Institute of Automatic Control
//                    = University of Mining and Technology
//   utesch@aut.tu-freiberg.de  * Freiberg, Germany
//
// the edge recombination algorithm is adopted from Genitor:
//   Copyright (c) 1990 Darrell L. Whitley, Computer Science Department,
//   Colorado State University. Permission is hereby granted to copy all or
//   any part of this program for free distribution.
//
//------------------------------------------------------------------------
//
// #include mapping:
//   - "postgres.h"                      -> `use crate::prelude::*;`
//   - "optimizer/geqo.h"                -> PlannerInfo (opaque pass-through)
//   - "optimizer/geqo_random.h"         -> geqo_randint (sibling module)
//   - "optimizer/geqo_recombination.h"  -> Edge / Gene struct decls (imported
//                                          from the sibling geqo_recombination
//                                          module, their defining .c)
//
// The whole C file is guarded by `#if defined(ERX)`. That compile-time
// selector is not modeled here: PostgreSQL's default crossover is ERX, so
// these routines are unconditionally available in Rust.
//
// FULLY REAL 1:1 translation of the pure-array ERX algorithm. The
// edge_list[4] / total_edges / unused_edges accounting (especially the
// swap-with-last deletion in remove_gene and the negative "shared edge"
// encoding) is preserved exactly as in the C source.
//------------------------------------------------------------------------

use crate::prelude::*;

use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo::geqo_random::geqo_randint;
use crate::optimizer::geqo::geqo_recombination::{Edge, Gene};

// alloc_edge_table
//
//   allocate memory for edge table
pub unsafe fn alloc_edge_table(_root: *mut PlannerInfo, num_gene: c_int) -> *mut Edge {
    // palloc one extra location so that nodes numbered 1..n can be indexed
    // directly; 0 will not be used
    let edge_table = palloc(((num_gene + 1) as usize) * core::mem::size_of::<Edge>()) as *mut Edge;

    edge_table
}

// free_edge_table
//
//   deallocate memory of edge table
pub unsafe fn free_edge_table(_root: *mut PlannerInfo, edge_table: *mut Edge) {
    pfree(edge_table as *mut c_void);
}

// gimme_edge_table
//
//   fills a data structure which represents the set of explicit edges between
//   points in the (2) input genes
//
//   assumes circular tours and bidirectional edges
//
//   gimme_edge() will set "shared" edges to negative values
//
//   returns average number edges/city in range 2.0 - 4.0
//   where 2.0=homogeneous; 4.0=diverse
pub unsafe fn gimme_edge_table(
    root: *mut PlannerInfo,
    tour1: *mut Gene,
    tour2: *mut Gene,
    num_gene: c_int,
    edge_table: *mut Edge,
) -> f32 {
    let mut edge_total: c_int; // total number of unique edges in two genes

    // at first clear the edge table's old data
    let mut i: c_int = 1;
    while i <= num_gene {
        (*edge_table.offset(i as isize)).total_edges = 0;
        (*edge_table.offset(i as isize)).unused_edges = 0;
        i += 1;
    }

    // fill edge table with new data

    edge_total = 0;

    let mut index1: c_int = 0;
    while index1 < num_gene {
        // presume the tour is circular, i.e. 1->2, 2->3, 3->1 this operation
        // maps n back to 1
        let index2: c_int = (index1 + 1) % num_gene;

        // edges are bidirectional, i.e. 1->2 is same as 2->1 call gimme_edge
        // twice per edge

        edge_total += gimme_edge(
            root,
            *tour1.offset(index1 as isize),
            *tour1.offset(index2 as isize),
            edge_table,
        );
        gimme_edge(
            root,
            *tour1.offset(index2 as isize),
            *tour1.offset(index1 as isize),
            edge_table,
        );

        edge_total += gimme_edge(
            root,
            *tour2.offset(index1 as isize),
            *tour2.offset(index2 as isize),
            edge_table,
        );
        gimme_edge(
            root,
            *tour2.offset(index2 as isize),
            *tour2.offset(index1 as isize),
            edge_table,
        );

        index1 += 1;
    }

    // return average number of edges per index
    ((edge_total * 2) as f32) / (num_gene as f32)
}

// gimme_edge
//
//   registers edge from city1 to city2 in input edge table
//
//   no assumptions about directionality are made; therefore it is up to the
//   calling routine to call gimme_edge twice to make a bi-directional edge
//   between city1 and city2; uni-directional edges are possible as well (just
//   call gimme_edge once with the direction from city1 to city2)
//
//   returns 1 if edge was not already registered and was just added;
//           0 if edge was already registered and edge_table is unchanged
unsafe fn gimme_edge(
    _root: *mut PlannerInfo,
    gene1: Gene,
    gene2: Gene,
    edge_table: *mut Edge,
) -> c_int {
    let city1: c_int = gene1 as c_int;
    let city2: c_int = gene2 as c_int;

    // check whether edge city1->city2 already exists
    let edges: c_int = (*edge_table.offset(city1 as isize)).total_edges;

    let mut i: c_int = 0;
    while i < edges {
        if (*edge_table.offset(city1 as isize)).edge_list[i as usize].abs() as Gene == city2 {
            // mark shared edges as negative
            (*edge_table.offset(city1 as isize)).edge_list[i as usize] = 0 - city2;

            return 0;
        }
        i += 1;
    }

    // add city1->city2;
    (*edge_table.offset(city1 as isize)).edge_list[edges as usize] = city2;

    // increment the number of edges from city1
    (*edge_table.offset(city1 as isize)).total_edges += 1;
    (*edge_table.offset(city1 as isize)).unused_edges += 1;

    1
}

// gimme_tour
//
//   creates a new tour using edges from the edge table. priority is given to
//   "shared" edges (i.e. edges which all parent genes possess and are marked
//   as negative in the edge table.)
pub unsafe fn gimme_tour(
    root: *mut PlannerInfo,
    edge_table: *mut Edge,
    new_gene: *mut Gene,
    num_gene: c_int,
) -> c_int {
    let mut edge_failures: c_int = 0;

    // choose int between 1 and num_gene
    *new_gene.offset(0) = geqo_randint(root, num_gene, 1) as Gene;

    let mut i: c_int = 1;
    while i < num_gene {
        // as each point is entered into the tour, remove it from the edge
        // table
        let prev: Gene = *new_gene.offset((i - 1) as isize);
        remove_gene(root, prev, *edge_table.offset(prev as isize), edge_table);

        // find destination for the newly entered point
        if (*edge_table.offset(prev as isize)).unused_edges > 0 {
            *new_gene.offset(i as isize) =
                gimme_gene(root, *edge_table.offset(prev as isize), edge_table);
        } else {
            // cope with fault
            edge_failures += 1;

            *new_gene.offset(i as isize) =
                edge_failure(root, new_gene, i - 1, edge_table, num_gene);
        }

        // mark this node as incorporated
        let prev2: Gene = *new_gene.offset((i - 1) as isize);
        (*edge_table.offset(prev2 as isize)).unused_edges = -1;

        i += 1;
    }

    edge_failures
}

// remove_gene
//
//   removes input gene from edge_table. input edge is used to identify
//   deletion locations within edge table.
unsafe fn remove_gene(_root: *mut PlannerInfo, gene: Gene, edge: Edge, edge_table: *mut Edge) {
    // do for every gene known to have an edge to input gene (i.e. in edge_list
    // for input edge)
    let mut i: c_int = 0;
    while i < edge.unused_edges {
        let possess_edge: c_int = edge.edge_list[i as usize].abs();
        let genes_remaining: c_int = (*edge_table.offset(possess_edge as isize)).unused_edges;

        // find the input gene in all edge_lists and delete it
        let mut j: c_int = 0;
        while j < genes_remaining {
            if (*edge_table.offset(possess_edge as isize)).edge_list[j as usize].abs() as Gene
                == gene
            {
                (*edge_table.offset(possess_edge as isize)).unused_edges -= 1;

                (*edge_table.offset(possess_edge as isize)).edge_list[j as usize] =
                    (*edge_table.offset(possess_edge as isize)).edge_list
                        [(genes_remaining - 1) as usize];

                break;
            }
            j += 1;
        }
        i += 1;
    }
}

// gimme_gene
//
//   priority is given to "shared" edges (i.e. edges which both genes possess)
unsafe fn gimme_gene(root: *mut PlannerInfo, edge: Edge, edge_table: *mut Edge) -> Gene {
    let mut friend: Gene;
    // no point has edges to more than 4 other points thus, this contrived
    // minimum will be replaced
    let mut minimum_edges: c_int = 5;
    let mut minimum_count: c_int = -1;

    // consider candidate destination points in edge list
    let mut i: c_int = 0;
    while i < edge.unused_edges {
        friend = edge.edge_list[i as usize] as Gene;

        // give priority to shared edges that are negative; so return 'em
        //
        // negative values are caught here so we need not worry about
        // converting to absolute values
        if friend < 0 {
            return friend.abs() as Gene;
        }

        // give priority to candidates with fewest remaining unused edges; find
        // out what the minimum number of unused edges is (minimum_edges); if
        // there is more than one candidate with the minimum number of unused
        // edges keep count of this number (minimum_count);
        //
        // The test for minimum_count can probably be removed at some point but
        // comments should probably indicate exactly why it is guaranteed that
        // the test will always succeed the first time around. If it can fail
        // then the code is in error
        if (*edge_table.offset(friend as isize)).unused_edges < minimum_edges {
            minimum_edges = (*edge_table.offset(friend as isize)).unused_edges;
            minimum_count = 1;
        } else if minimum_count == -1 {
            elog!(ERROR, "minimum_count not set");
            unreachable!();
        } else if (*edge_table.offset(friend as isize)).unused_edges == minimum_edges {
            minimum_count += 1;
        }

        i += 1;
    }

    // random decision of the possible candidates to use
    let rand_decision: c_int = geqo_randint(root, minimum_count - 1, 0);

    let mut i: c_int = 0;
    while i < edge.unused_edges {
        friend = edge.edge_list[i as usize] as Gene;

        // return the chosen candidate point
        if (*edge_table.offset(friend as isize)).unused_edges == minimum_edges {
            minimum_count -= 1;

            if minimum_count == rand_decision {
                return friend;
            }
        }
        i += 1;
    }

    // ... should never be reached
    elog!(ERROR, "neither shared nor minimum number nor random edge found");
    unreachable!();
}

// edge_failure
//
//   routine for handling edge failure
unsafe fn edge_failure(
    root: *mut PlannerInfo,
    gene: *mut Gene,
    index: c_int,
    edge_table: *mut Edge,
    num_gene: c_int,
) -> Gene {
    let fail_gene: Gene = *gene.offset(index as isize);
    let mut remaining_edges: c_int = 0;
    let mut four_count: c_int = 0;
    let rand_decision: c_int;

    // how many edges remain? how many gene with four total (initial) edges
    // remain?
    let mut i: c_int = 1;
    while i <= num_gene {
        if ((*edge_table.offset(i as isize)).unused_edges != -1) && (i != fail_gene as c_int) {
            remaining_edges += 1;

            if (*edge_table.offset(i as isize)).total_edges == 4 {
                four_count += 1;
            }
        }
        i += 1;
    }

    // random decision of the gene with remaining edges and whose total_edges
    // == 4
    if four_count != 0 {
        rand_decision = geqo_randint(root, four_count - 1, 0);

        let mut i: c_int = 1;
        while i <= num_gene {
            if (i as Gene) != fail_gene
                && (*edge_table.offset(i as isize)).unused_edges != -1
                && (*edge_table.offset(i as isize)).total_edges == 4
            {
                four_count -= 1;

                if rand_decision == four_count {
                    return i as Gene;
                }
            }
            i += 1;
        }

        elog!(LOG, "no edge found via random decision and total_edges == 4");
    } else if remaining_edges != 0 {
        // random decision of the gene with remaining edges
        rand_decision = geqo_randint(root, remaining_edges - 1, 0);

        let mut i: c_int = 1;
        while i <= num_gene {
            if (i as Gene) != fail_gene && (*edge_table.offset(i as isize)).unused_edges != -1 {
                remaining_edges -= 1;

                if rand_decision == remaining_edges {
                    return i as Gene;
                }
            }
            i += 1;
        }

        elog!(LOG, "no edge found via random decision with remaining edges");
    }
    // edge table seems to be empty; this happens sometimes on the last point
    // due to the fact that the first point is removed from the table even
    // though only one of its edges has been determined
    else {
        // occurs only at the last point in the tour; simply look for the point
        // which is not yet used
        let mut i: c_int = 1;
        while i <= num_gene {
            if (*edge_table.offset(i as isize)).unused_edges >= 0 {
                return i as Gene;
            }
            i += 1;
        }

        elog!(LOG, "no edge found via looking for the last unused point");
    }

    // ... should never be reached
    elog!(ERROR, "no edge found");
    unreachable!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::pg_prng::{pg_prng_fseed, pg_prng_state};
    use crate::optimizer::geqo::geqo_random::GeqoPrivateData;

    // Build a PlannerInfo whose join_search_private holds a deterministically
    // seeded GeqoPrivateData so geqo_randint() is reproducible. PlannerInfo has
    // ~82 fields, so palloc0 it and only set join_search_private (the single
    // field the GEQO RNG reaches), matching geqo_random's access path.
    unsafe fn seeded_root(seed: f64) -> *mut PlannerInfo {
        let private = palloc0(core::mem::size_of::<GeqoPrivateData>()) as *mut GeqoPrivateData;
        pg_prng_fseed(&mut (*private).random_state as *mut pg_prng_state, seed);
        let root = palloc0(core::mem::size_of::<PlannerInfo>()) as *mut PlannerInfo;
        (*root).join_search_private = private as *mut c_void;
        root
    }

    // gimme_edge_table must build a symmetric adjacency table: if city a lists
    // b among its edges (ignoring the shared-edge negative sign), then city b
    // must list a as well. Also every total_edges must equal unused_edges right
    // after the table is built (nothing consumed yet) and lie in 2..=4.
    #[test]
    fn edge_table_is_symmetric() {
        unsafe {
            let root = seeded_root(0.5);
            let num_gene: c_int = 6;

            // two distinct parent permutations of 1..=6
            let mut tour1: Vec<Gene> = vec![1, 2, 3, 4, 5, 6];
            let mut tour2: Vec<Gene> = vec![2, 4, 6, 1, 3, 5];

            let edge_table = alloc_edge_table(root, num_gene);

            let avg = gimme_edge_table(
                root,
                tour1.as_mut_ptr(),
                tour2.as_mut_ptr(),
                num_gene,
                edge_table,
            );

            // average edges/city is in [2.0, 4.0]
            assert!(avg >= 2.0 && avg <= 4.0, "avg out of range: {}", avg);

            // build the undirected neighbor sets from the table (abs strips the
            // shared-edge sign) and confirm symmetry
            for a in 1..=num_gene {
                let e = *edge_table.offset(a as isize);
                assert_eq!(
                    e.total_edges, e.unused_edges,
                    "fresh table: total != unused for city {}",
                    a
                );
                assert!(
                    e.total_edges >= 2 && e.total_edges <= 4,
                    "city {} has {} edges",
                    a,
                    e.total_edges
                );
                for k in 0..e.total_edges {
                    let b = e.edge_list[k as usize].abs();
                    // b must list a back among its edges
                    let eb = *edge_table.offset(b as isize);
                    let mut found = false;
                    for m in 0..eb.total_edges {
                        if eb.edge_list[m as usize].abs() == a {
                            found = true;
                            break;
                        }
                    }
                    assert!(found, "edge {}->{} not mirrored", a, b);
                }
            }

            free_edge_table(root, edge_table);
        }
    }

    // The full ERX (gimme_tour over a freshly built edge table) must yield a
    // valid permutation of 1..=num_gene from two hand-built parents: every city
    // appears exactly once, none out of range, none missing.
    #[test]
    fn erx_produces_valid_permutation() {
        unsafe {
            for &seed in &[0.1f64, 0.37, 0.5, 0.83] {
                let root = seeded_root(seed);
                let num_gene: c_int = 8;

                let mut tour1: Vec<Gene> = vec![1, 2, 3, 4, 5, 6, 7, 8];
                let mut tour2: Vec<Gene> = vec![3, 1, 4, 8, 2, 7, 5, 6];

                let edge_table = alloc_edge_table(root, num_gene);
                gimme_edge_table(
                    root,
                    tour1.as_mut_ptr(),
                    tour2.as_mut_ptr(),
                    num_gene,
                    edge_table,
                );

                let mut offspring: Vec<Gene> = vec![0; num_gene as usize];
                gimme_tour(root, edge_table, offspring.as_mut_ptr(), num_gene);

                let mut seen = vec![0u32; (num_gene + 1) as usize];
                for &g in &offspring {
                    assert!(
                        g >= 1 && g <= num_gene,
                        "city {} out of range (seed {})",
                        g,
                        seed
                    );
                    seen[g as usize] += 1;
                }
                for c in 1..=num_gene {
                    assert_eq!(
                        seen[c as usize], 1,
                        "city {} appears {} times (seed {})",
                        c, seen[c as usize], seed
                    );
                }

                free_edge_table(root, edge_table);
            }
        }
    }
}
