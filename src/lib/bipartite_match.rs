//! Translation of postgres/src/include/lib/bipartite_match.h
//!                + postgres/src/backend/lib/bipartite_match.c
//!
//! Hopcroft-Karp maximum cardinality algorithm for bipartite graphs.
//!
//! This implementation is based on pseudocode found at:
//!
//! https://en.wikipedia.org/w/index.php?title=Hopcroft%E2%80%93Karp_algorithm&oldid=593898016
//!
//! Copyright (c) 2015-2025, PostgreSQL Global Development Group

use crate::prelude::*;
use core::ffi::{c_int, c_void};

/*
 * Given a bipartite graph consisting of nodes U numbered 1..nU, nodes V
 * numbered 1..nV, and an adjacency map of undirected edges in the form
 * adjacency[u] = [k, v1, v2, v3, ... vk], we wish to find a "maximum
 * cardinality matching", which is defined as follows: a matching is a subset
 * of the original edges such that no node has more than one edge, and a
 * matching has maximum cardinality if there exists no other matching with a
 * greater number of edges.
 *
 * This matching has various applications in graph theory, but the motivating
 * example here is Dilworth's theorem: a partially-ordered set can be divided
 * into the minimum number of chains (i.e. subsets X where x1 < x2 < x3 ...) by
 * a bipartite graph construction. This gives us a polynomial-time solution to
 * the problem of planning a collection of grouping sets with the provably
 * minimal number of sort operations.
 */
#[repr(C)]
pub struct BipartiteMatchState {
    /* inputs: */
    pub u_size: c_int,        /* size of U */
    pub v_size: c_int,        /* size of V */
    pub adjacency: *mut *mut i16, /* adjacency[u] = [k, v1,v2,v3,...,vk] */
    /* outputs: */
    pub matching: c_int,      /* number of edges in matching */
    pub pair_uv: *mut i16,    /* pair_uv[u] -> v */
    pub pair_vu: *mut i16,    /* pair_vu[v] -> u */
    /* private state for matching algorithm: */
    pub distance: *mut i16,   /* distance[u] */
    pub queue: *mut i16,      /* queue storage for breadth search */
}

/*
 * The distances computed in hk_breadth_search can easily be seen to never
 * exceed u_size.  Since we restrict u_size to be less than SHRT_MAX, we
 * can therefore use SHRT_MAX as the "infinity" distance needed as a marker.
 */
const HK_INFINITY: i16 = i16::MAX; /* SHRT_MAX */

// SHRT_MAX from <limits.h>, used for the u_size/v_size range checks below.
const SHRT_MAX: c_int = i16::MAX as c_int;

// TODO(pg-port): real signal/interrupt handling (miscadmin.h)
#[inline]
fn CHECK_FOR_INTERRUPTS() {}

// TODO(pg-port): real stack-depth check (miscadmin.h). The C code calls
// check_stack_depth() to guard the recursion in hk_depth_search; here it is a
// no-op until interrupt/stack infrastructure exists.
#[inline]
fn check_stack_depth() {}

/*
 * Given the size of U and V, where each is indexed 1..size, and an adjacency
 * list, perform the matching and return the resulting state.
 */
pub unsafe fn BipartiteMatch(
    u_size: c_int,
    v_size: c_int,
    adjacency: *mut *mut i16,
) -> *mut BipartiteMatchState {
    let state = palloc(core::mem::size_of::<BipartiteMatchState>()) as *mut BipartiteMatchState;

    if u_size < 0 || u_size >= SHRT_MAX || v_size < 0 || v_size >= SHRT_MAX {
        elog!(ERROR, "invalid set size for BipartiteMatch");
    }

    (*state).u_size = u_size;
    (*state).v_size = v_size;
    (*state).adjacency = adjacency;
    (*state).matching = 0;
    (*state).pair_uv =
        palloc0((u_size as Size + 1) * core::mem::size_of::<i16>()) as *mut i16;
    (*state).pair_vu =
        palloc0((v_size as Size + 1) * core::mem::size_of::<i16>()) as *mut i16;
    (*state).distance =
        palloc((u_size as Size + 1) * core::mem::size_of::<i16>()) as *mut i16;
    (*state).queue =
        palloc((u_size as Size + 2) * core::mem::size_of::<i16>()) as *mut i16;

    while hk_breadth_search(state) {
        let mut u: c_int = 1;
        while u <= u_size {
            if *(*state).pair_uv.offset(u as isize) == 0 {
                if hk_depth_search(state, u) {
                    (*state).matching += 1;
                }
            }
            u += 1;
        }

        CHECK_FOR_INTERRUPTS(); /* just in case */
    }

    state
}

/*
 * Free a state returned by BipartiteMatch, except for the original adjacency
 * list, which is owned by the caller. This only frees memory, so it's optional.
 */
pub unsafe fn BipartiteMatchFree(state: *mut BipartiteMatchState) {
    /* adjacency matrix is treated as owned by the caller */
    pfree((*state).pair_uv as *mut c_void);
    pfree((*state).pair_vu as *mut c_void);
    pfree((*state).distance as *mut c_void);
    pfree((*state).queue as *mut c_void);
    pfree(state as *mut c_void);
}

/*
 * Perform the breadth-first search step of H-K matching.
 * Returns true if successful.
 */
unsafe fn hk_breadth_search(state: *mut BipartiteMatchState) -> bool {
    let usize_: c_int = (*state).u_size;
    let queue = (*state).queue;
    let distance = (*state).distance;
    let mut qhead: c_int = 0; /* we never enqueue any node more than once */
    let mut qtail: c_int = 0; /* so don't have to worry about wrapping */
    let mut u: c_int;

    *distance.offset(0) = HK_INFINITY;

    u = 1;
    while u <= usize_ {
        if *(*state).pair_uv.offset(u as isize) == 0 {
            *distance.offset(u as isize) = 0;
            *queue.offset(qhead as isize) = u as i16;
            qhead += 1;
        } else {
            *distance.offset(u as isize) = HK_INFINITY;
        }
        u += 1;
    }

    while qtail < qhead {
        u = *queue.offset(qtail as isize) as c_int;
        qtail += 1;

        if *distance.offset(u as isize) < *distance.offset(0) {
            let u_adj = *(*state).adjacency.offset(u as isize);
            let mut i: c_int = if !u_adj.is_null() {
                *u_adj.offset(0) as c_int
            } else {
                0
            };

            while i > 0 {
                let u_next: c_int =
                    *(*state).pair_vu.offset(*u_adj.offset(i as isize) as isize) as c_int;

                if *distance.offset(u_next as isize) == HK_INFINITY {
                    *distance.offset(u_next as isize) = 1 + *distance.offset(u as isize);
                    Assert!(qhead < usize_ + 2);
                    *queue.offset(qhead as isize) = u_next as i16;
                    qhead += 1;
                }
                i -= 1;
            }
        }
    }

    *distance.offset(0) != HK_INFINITY
}

/*
 * Perform the depth-first search step of H-K matching.
 * Returns true if successful.
 */
unsafe fn hk_depth_search(state: *mut BipartiteMatchState, u: c_int) -> bool {
    let distance = (*state).distance;
    let pair_uv = (*state).pair_uv;
    let pair_vu = (*state).pair_vu;
    let u_adj = *(*state).adjacency.offset(u as isize);
    let mut i: c_int = if !u_adj.is_null() {
        *u_adj.offset(0) as c_int
    } else {
        0
    };
    let nextdist: i16;

    if u == 0 {
        return true;
    }
    if *distance.offset(u as isize) == HK_INFINITY {
        return false;
    }
    nextdist = *distance.offset(u as isize) + 1;

    check_stack_depth();

    while i > 0 {
        let v: c_int = *u_adj.offset(i as isize) as c_int;

        if *distance.offset(*pair_vu.offset(v as isize) as isize) == nextdist {
            if hk_depth_search(state, *pair_vu.offset(v as isize) as c_int) {
                *pair_vu.offset(v as isize) = u as i16;
                *pair_uv.offset(u as isize) = v as i16;
                return true;
            }
        }
        i -= 1;
    }

    *distance.offset(u as isize) = HK_INFINITY;
    false
}
