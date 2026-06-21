//! Translation of postgres/src/backend/utils/misc/sampling.c
//!                + postgres/src/include/utils/sampling.h
//!
//! Relation block sampling routines.
//!
//! Block-level sampling uses Algorithm S from Knuth 3.4.2 (used when the total
//! number of blocks is known in advance).  Reservoir sampling uses Algorithm Z
//! from Vitter, "Random sampling with a reservoir", ACM TOMS 11(1), 1985.
//! Both are used by ANALYZE.  This is a FULLY REAL translation over the ported
//! xoroshiro128** PRNG.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! #include mapping:
//!   - "postgres.h"            -> crate::prelude::* (REAL)
//!   - <math.h>                -> log/exp/floor via extern "C" libm (REAL)
//!   - "utils/sampling.h"      -> structs/typedefs MERGED below (REAL)
//!       - "common/pg_prng.h"  -> crate::common::pg_prng::* (REAL)
//!       - "storage/block.h"   -> crate::storage::block::BlockNumber (REAL)

use crate::prelude::*;

use crate::common::pg_prng::{
    pg_global_prng_state, pg_prng_double, pg_prng_seed, pg_prng_state, pg_prng_uint32,
};
use crate::storage::block::BlockNumber;

// <math.h>: bind the three functions actually used by the Vitter math.  log/exp
// drive the W/S threshold computation; floor truncates X toward -inf.  Binding
// libm directly (rather than f64 methods) keeps the arithmetic bit-identical to
// the C source, which matters for sample correctness.
extern "C" {
    fn log(x: f64) -> f64;
    fn exp(x: f64) -> f64;
    fn floor(x: f64) -> f64;
}

// --- merged from utils/sampling.h ---------------------------------------------

/* Random generator for sampling code */
// (functions declared below)

/* Block sampling methods */

/// Data structure for Algorithm S from Knuth 3.4.2.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BlockSamplerData {
    pub N: BlockNumber, /* number of blocks, known in advance */
    pub n: c_int,       /* desired sample size */
    pub t: BlockNumber, /* current block number */
    pub m: c_int,       /* blocks selected so far */
    pub randstate: pg_prng_state, /* random generator state */
}

pub type BlockSampler = *mut BlockSamplerData;

/* Reservoir sampling methods */

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReservoirStateData {
    pub W: f64,
    pub randstate: pg_prng_state, /* random generator state */
}

pub type ReservoirState = *mut ReservoirStateData;

/// Random generator state type alias used by the sampling code.
pub type SamplerRandomState = pg_prng_state;

// --- sampling.c ---------------------------------------------------------------

/*
 * BlockSampler_Init -- prepare for random sampling of blocknumbers
 *
 * BlockSampler provides algorithm for block level sampling of a relation
 * as discussed on pgsql-hackers 2004-04-02 (subject "Large DB")
 * It selects a random sample of samplesize blocks out of
 * the nblocks blocks in the table. If the table has less than
 * samplesize blocks, all blocks are selected.
 *
 * Since we know the total number of blocks in advance, we can use the
 * straightforward Algorithm S from Knuth 3.4.2, rather than Vitter's
 * algorithm.
 *
 * Returns the number of blocks that BlockSampler_Next will return.
 */
pub unsafe fn BlockSampler_Init(
    bs: BlockSampler,
    nblocks: BlockNumber,
    samplesize: c_int,
    randseed: uint32,
) -> BlockNumber {
    (*bs).N = nblocks; /* measured table size */

    /*
     * If we decide to reduce samplesize for tables that have less or not much
     * more than samplesize blocks, here is the place to do it.
     */
    (*bs).n = samplesize;
    (*bs).t = 0; /* blocks scanned so far */
    (*bs).m = 0; /* blocks selected so far */

    sampler_random_init_state(randseed, &mut (*bs).randstate);

    // C: Min(bs->n, bs->N) with bs->n int and bs->N BlockNumber; the macro
    // result is used as BlockNumber.  n is a non-negative sample size, so the
    // cast to BlockNumber is exact.
    Min((*bs).n as BlockNumber, (*bs).N)
}

#[no_mangle]
pub unsafe fn BlockSampler_HasMore(bs: BlockSampler) -> bool {
    ((*bs).t < (*bs).N) && ((*bs).m < (*bs).n)
}

#[no_mangle]
pub unsafe fn BlockSampler_Next(bs: BlockSampler) -> BlockNumber {
    let mut K: BlockNumber = (*bs).N - (*bs).t; /* remaining blocks */
    let k: c_int = (*bs).n - (*bs).m; /* blocks still to sample */
    let mut p: f64; /* probability to skip block */
    let V: f64; /* random */

    Assert!(BlockSampler_HasMore(bs)); /* hence K > 0 and k > 0 */

    if (k as BlockNumber) >= K {
        /* need all the rest */
        (*bs).m += 1;
        let r = (*bs).t;
        (*bs).t += 1;
        return r;
    }

    /*----------
     * It is not obvious that this code matches Knuth's Algorithm S.
     * Knuth says to skip the current block with probability 1 - k/K.
     * If we are to skip, we should advance t (hence decrease K), and
     * repeat the same probabilistic test for the next block.  The naive
     * implementation thus requires a sampler_random_fract() call for each
     * block number.  But we can reduce this to one sampler_random_fract()
     * call per selected block, by noting that each time the while-test
     * succeeds, we can reinterpret V as a uniform random number in the range
     * 0 to p. Therefore, instead of choosing a new V, we just adjust p to be
     * the appropriate fraction of its former value, and our next loop
     * makes the appropriate probabilistic test.
     *
     * We have initially K > k > 0.  If the loop reduces K to equal k,
     * the next while-test must fail since p will become exactly zero
     * (we assume there will not be roundoff error in the division).
     * (Note: Knuth suggests a "<=" loop condition, but we use "<" just
     * to be doubly sure about roundoff error.)  Therefore K cannot become
     * less than k, which means that we cannot fail to select enough blocks.
     *----------
     */
    V = sampler_random_fract(&mut (*bs).randstate);
    p = 1.0 - (k as f64) / (K as f64);
    while V < p {
        /* skip */
        (*bs).t += 1;
        K -= 1; /* keep K == N - t */

        /* adjust p to be new cutoff point in reduced range */
        p *= 1.0 - (k as f64) / (K as f64);
    }

    /* select */
    (*bs).m += 1;
    let r = (*bs).t;
    (*bs).t += 1;
    r
}

/*
 * These two routines embody Algorithm Z from "Random sampling with a
 * reservoir" by Jeffrey S. Vitter, in ACM Trans. Math. Softw. 11, 1
 * (Mar. 1985), Pages 37-57.  Vitter describes his algorithm in terms
 * of the count S of records to skip before processing another record.
 * It is computed primarily based on t, the number of records already read.
 * The only extra state needed between calls is W, a random state variable.
 *
 * reservoir_init_selection_state computes the initial W value.
 *
 * Given that we've already read t records (t >= n), reservoir_get_next_S
 * determines the number of records to skip before the next record is
 * processed.
 */
pub unsafe fn reservoir_init_selection_state(rs: ReservoirState, n: c_int) {
    /*
     * Reservoir sampling is not used anywhere where it would need to return
     * repeatable results so we can initialize it randomly.
     */
    sampler_random_init_state(
        pg_prng_uint32(&mut pg_global_prng_state),
        &mut (*rs).randstate,
    );

    /* Initial value of W (for use when Algorithm Z is first applied) */
    (*rs).W = exp(-log(sampler_random_fract(&mut (*rs).randstate)) / n as f64);
}

#[no_mangle]
pub unsafe fn reservoir_get_next_S(rs: ReservoirState, mut t: f64, n: c_int) -> f64 {
    let S: f64;

    /* The magic constant here is T from Vitter's paper */
    if t <= (22.0 * n as f64) {
        /* Process records using Algorithm X until t is large enough */
        let V: f64;
        let mut quot: f64;

        V = sampler_random_fract(&mut (*rs).randstate); /* Generate V */
        let mut s: f64 = 0.0;
        t += 1.0;
        /* Note: "num" in Vitter's code is always equal to t - n */
        quot = (t - n as f64) / t;
        /* Find min S satisfying (4.1) */
        while quot > V {
            s += 1.0;
            t += 1.0;
            quot *= (t - n as f64) / t;
        }
        S = s;
    } else {
        /* Now apply Algorithm Z */
        let mut W: f64 = (*rs).W;
        let term: f64 = t - n as f64 + 1.0;

        loop {
            let numer_lim: f64;
            let denom_init: f64;
            let U: f64;
            let X: f64;
            let lhs: f64;
            let rhs: f64;
            let mut y: f64;
            let tmp: f64;

            /* Generate U and X */
            U = sampler_random_fract(&mut (*rs).randstate);
            X = t * (W - 1.0);
            let s_try = floor(X); /* S is tentatively set to floor(X) */
            /* Test if U <= h(S)/cg(X) in the manner of (6.3) */
            tmp = (t + 1.0) / term;
            lhs = exp(log(((U * tmp * tmp) * (term + s_try)) / (t + X)) / n as f64);
            rhs = (((t + X) / (term + s_try)) * term) / t;
            if lhs <= rhs {
                W = rhs / lhs;
                S = s_try;
                break;
            }
            /* Test if U <= f(S)/cg(X) */
            y = (((U * (t + 1.0)) / term) * (t + s_try + 1.0)) / (t + X);
            if (n as f64) < s_try {
                denom_init = t;
                numer_lim = term + s_try;
            } else {
                denom_init = t - n as f64 + s_try;
                numer_lim = t + 1.0;
            }
            let mut denom = denom_init;
            let mut numer = t + s_try;
            while numer >= numer_lim {
                y *= numer / denom;
                denom -= 1.0;
                numer -= 1.0;
            }
            W = exp(-log(sampler_random_fract(&mut (*rs).randstate)) / n as f64); /* Generate W in advance */
            if exp(log(y) / n as f64) <= (t + X) / t {
                S = s_try;
                break;
            }
        }
        (*rs).W = W;
    }
    S
}

/*----------
 * Random number generator used by sampling
 *----------
 */
pub unsafe fn sampler_random_init_state(seed: uint32, randstate: *mut pg_prng_state) {
    pg_prng_seed(randstate, seed as uint64);
}

/* Select a random value R uniformly distributed in (0 - 1) */
#[no_mangle]
pub unsafe fn sampler_random_fract(randstate: *mut pg_prng_state) -> f64 {
    let mut res: f64;

    /* pg_prng_double returns a value in [0.0 - 1.0), so we must reject 0.0 */
    loop {
        res = pg_prng_double(randstate);
        if !crate::c::unlikely(res == 0.0) {
            break;
        }
    }
    res
}

/*
 * Backwards-compatible API for block sampling
 *
 * This code is now deprecated, but since it's still in use by many FDWs,
 * we should keep it for awhile at least.  The functionality is the same as
 * sampler_random_fract/reservoir_init_selection_state/reservoir_get_next_S,
 * except that a common random state is used across all callers.
 */
// C: static ReservoirStateData oldrs; static bool oldrs_initialized = false;
static mut oldrs: ReservoirStateData = ReservoirStateData {
    W: 0.0,
    randstate: pg_prng_state { s0: 0, s1: 0 },
};
static mut oldrs_initialized: bool = false;

pub unsafe fn anl_random_fract() -> f64 {
    /* initialize if first time through */
    if crate::c::unlikely(!oldrs_initialized) {
        sampler_random_init_state(
            pg_prng_uint32(&mut pg_global_prng_state),
            &mut oldrs.randstate,
        );
        oldrs_initialized = true;
    }

    /* and compute a random fraction */
    sampler_random_fract(&mut oldrs.randstate)
}

pub unsafe fn anl_init_selection_state(n: c_int) -> f64 {
    /* initialize if first time through */
    if crate::c::unlikely(!oldrs_initialized) {
        sampler_random_init_state(
            pg_prng_uint32(&mut pg_global_prng_state),
            &mut oldrs.randstate,
        );
        oldrs_initialized = true;
    }

    /* Initial value of W (for use when Algorithm Z is first applied) */
    exp(-log(sampler_random_fract(&mut oldrs.randstate)) / n as f64)
}

pub unsafe fn anl_get_next_S(t: f64, n: c_int, stateptr: *mut f64) -> f64 {
    let result: f64;

    oldrs.W = *stateptr;
    result = reservoir_get_next_S(&mut oldrs, t, n);
    *stateptr = oldrs.W;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_bs() -> BlockSamplerData {
        BlockSamplerData {
            N: 0,
            n: 0,
            t: 0,
            m: 0,
            randstate: pg_prng_state { s0: 0, s1: 0 },
        }
    }

    fn new_rs() -> ReservoirStateData {
        ReservoirStateData {
            W: 0.0,
            randstate: pg_prng_state { s0: 0, s1: 0 },
        }
    }

    #[test]
    fn block_sampler_yields_exactly_n_distinct_in_range() {
        unsafe {
            let mut bs = new_bs();
            let ret = BlockSampler_Init(&mut bs, 100, 10, 12345);
            assert_eq!(ret, 10); // Min(10, 100)

            let mut out: Vec<BlockNumber> = Vec::new();
            while BlockSampler_HasMore(&mut bs) {
                out.push(BlockSampler_Next(&mut bs));
            }
            assert_eq!(out.len(), 10);
            // all in [0, 100)
            for &b in &out {
                assert!(b < 100);
            }
            // strictly increasing => distinct (Algorithm S walks t upward)
            for w in out.windows(2) {
                assert!(w[0] < w[1], "expected increasing/distinct, got {:?}", out);
            }
        }
    }

    #[test]
    fn block_sampler_small_table_selects_all() {
        unsafe {
            let mut bs = new_bs();
            // n > N: must return all N blocks 0..N
            let ret = BlockSampler_Init(&mut bs, 5, 10, 999);
            assert_eq!(ret, 5);
            let mut out: Vec<BlockNumber> = Vec::new();
            while BlockSampler_HasMore(&mut bs) {
                out.push(BlockSampler_Next(&mut bs));
            }
            assert_eq!(out, vec![0, 1, 2, 3, 4]);
        }
    }

    #[test]
    fn sampler_random_fract_in_open_unit_interval() {
        unsafe {
            let mut st = pg_prng_state { s0: 0, s1: 0 };
            pg_prng_seed(&mut st, 42);
            for _ in 0..10_000 {
                let r = sampler_random_fract(&mut st);
                assert!(r > 0.0 && r < 1.0, "fract out of (0,1): {}", r);
            }
        }
    }

    #[test]
    fn reservoir_get_next_s_nonnegative_skip() {
        unsafe {
            let mut rs = new_rs();
            reservoir_init_selection_state(&mut rs, 100);
            assert!(rs.W.is_finite() && rs.W > 0.0);

            // Algorithm X branch (t small relative to n).
            for t in [100.0_f64, 200.0, 500.0, 1000.0] {
                let s = reservoir_get_next_S(&mut rs, t, 100);
                assert!(s >= 0.0, "S negative in Algorithm X: {}", s);
                assert!(s.is_finite());
            }

            // Algorithm Z branch (t > 22*n).
            for t in [3000.0_f64, 10_000.0, 100_000.0] {
                let s = reservoir_get_next_S(&mut rs, t, 100);
                assert!(s >= 0.0, "S negative in Algorithm Z: {}", s);
                assert!(s.is_finite());
            }
        }
    }

    #[test]
    fn anl_old_api_roundtrips() {
        unsafe {
            let f = anl_random_fract();
            assert!(f > 0.0 && f < 1.0);
            let mut w = anl_init_selection_state(50);
            assert!(w.is_finite() && w > 0.0);
            let s = anl_get_next_S(50.0, 50, &mut w);
            assert!(s >= 0.0 && s.is_finite());
        }
    }
}
