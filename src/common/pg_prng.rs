//! Translation of postgres/src/include/common/pg_prng.h
//!                + postgres/src/common/pg_prng.c
//!
//! Pseudo-Random Number Generator
//!
//! We use Blackman and Vigna's xoroshiro128** 1.0 algorithm
//! to have a small, fast PRNG suitable for generating reasonably
//! good-quality 64-bit data.  This should not be considered
//! cryptographically strong, however.
//!
//! About these generators: https://prng.di.unimi.it/
//! See also https://en.wikipedia.org/wiki/List_of_random_number_generators
//!
//! Copyright (c) 2021-2025, PostgreSQL Global Development Group
//!
//! Translator's note on integer arithmetic: the xoroshiro128** scrambler and
//! the splitmix64 seeder rely on two's-complement wrapping on overflow, which
//! is the defined behavior in C but a panic in Rust debug builds.  Every
//! multiply/add that can overflow therefore uses `wrapping_mul` /
//! `wrapping_add`, and the 64-bit rotate uses `u64::rotate_left`.  The XOR and
//! shift state updates cannot overflow, so they are left as ordinary operators.

use crate::prelude::*;
use core::ffi::c_int;

// pg_leftmost_one_pos64 is used by pg_prng_uint64_range to size the rejection
// mask; import it from the already-translated bitutils module.
use crate::port::pg_bitutils::pg_leftmost_one_pos64;

/* X/Open (XSI) requires <math.h> to provide M_PI, but core POSIX does not */
// In C this is `#define M_PI 3.14159265358979323846`; here we use Rust's
// std::f64::consts::PI, which carries the same value.
const M_PI: f64 = core::f64::consts::PI;

/*
 * State vector for PRNG generation.  Callers should treat this as an
 * opaque typedef, but we expose its definition to allow it to be
 * embedded in other structs.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct pg_prng_state {
    pub s0: uint64,
    pub s1: uint64,
}

/*
 * Callers not needing local PRNG series may use this global state vector,
 * after initializing it with one of the pg_prng_...seed functions.
 */
/* process-wide state vector */
// C: `pg_prng_state pg_global_prng_state;` -- a zero-initialized global.
pub static mut pg_global_prng_state: pg_prng_state = pg_prng_state { s0: 0, s1: 0 };

/*
 * 64-bit rotate left
 */
#[inline]
fn rotl(x: uint64, bits: c_int) -> uint64 {
    // C: (x << bits) | (x >> (64 - bits)).  Rust's rotate_left has identical
    // semantics for 0 < bits < 64 (the only values used here) and avoids the
    // undefined `x >> 64` shift when bits == 0.
    x.rotate_left(bits as u32)
}

/*
 * The basic xoroshiro128** algorithm.
 * Generates and returns a 64-bit uniformly distributed number,
 * updating the state vector for next time.
 *
 * Note: the state vector must not be all-zeroes, as that is a fixed point.
 */
unsafe fn xoroshiro128ss(state: *mut pg_prng_state) -> uint64 {
    let s0: uint64 = (*state).s0;
    let sx: uint64 = (*state).s1 ^ s0;
    // val = rotl(s0 * 5, 7) * 9 -- both multiplies must wrap.
    let val: uint64 = rotl(s0.wrapping_mul(5), 7).wrapping_mul(9);

    /* update state */
    (*state).s0 = rotl(s0, 24) ^ sx ^ (sx << 16);
    (*state).s1 = rotl(sx, 37);

    val
}

/*
 * We use this generator just to fill the xoroshiro128** state vector
 * from a 64-bit seed.
 */
unsafe fn splitmix64(state: *mut uint64) -> uint64 {
    /* state update */
    // *state += 0x9E3779B97f4A7C15 (wrapping)
    *state = (*state).wrapping_add(0x9E3779B97f4A7C15);
    let mut val: uint64 = *state;

    /* value extraction */
    val = (val ^ (val >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    val = (val ^ (val >> 27)).wrapping_mul(0x94D049BB133111EB);

    val ^ (val >> 31)
}

/*
 * Initialize the PRNG state from a 64-bit integer,
 * taking care that we don't produce all-zeroes.
 */
pub unsafe fn pg_prng_seed(state: *mut pg_prng_state, seed: uint64) {
    let mut seed: uint64 = seed;
    (*state).s0 = splitmix64(&mut seed);
    (*state).s1 = splitmix64(&mut seed);
    /* Let's just make sure we didn't get all-zeroes */
    let _ = pg_prng_seed_check(state);
}

/*
 * Initialize the PRNG state from a double in the range [-1.0, 1.0],
 * taking care that we don't produce all-zeroes.
 */
pub unsafe fn pg_prng_fseed(state: *mut pg_prng_state, fseed: f64) {
    /* Assume there's about 52 mantissa bits; the sign contributes too. */
    // C: int64 seed = ((double)((UINT64CONST(1) << 52) - 1)) * fseed;
    let seed: int64 = (((1u64 << 52) - 1) as f64 * fseed) as int64;

    pg_prng_seed(state, seed as uint64);
}

/*
 * Validate a PRNG seed value.
 */
pub unsafe fn pg_prng_seed_check(state: *mut pg_prng_state) -> bool {
    /*
     * If the seeding mechanism chanced to produce all-zeroes, insert
     * something nonzero.  Anything would do; use Knuth's LCG parameters.
     */
    if (*state).s0 == 0 && (*state).s1 == 0 {
        (*state).s0 = 0x5851F42D4C957F2D;
        (*state).s1 = 0x14057B7EF767814F;
    }

    /* As a convenience for the pg_prng_strong_seed macro, return true */
    true
}

/*
 * Select a random uint64 uniformly from the range [0, PG_UINT64_MAX].
 */
pub unsafe fn pg_prng_uint64(state: *mut pg_prng_state) -> uint64 {
    xoroshiro128ss(state)
}

/*
 * Select a random uint64 uniformly from the range [rmin, rmax].
 * If the range is empty, rmin is always produced.
 */
pub unsafe fn pg_prng_uint64_range(state: *mut pg_prng_state, rmin: uint64, rmax: uint64) -> uint64 {
    let val: uint64;

    if rmax > rmin {
        /*
         * Use bitmask rejection method to generate an offset in 0..range.
         * Each generated val is less than twice "range", so on average we
         * should not have to iterate more than twice.
         */
        let range: uint64 = rmax - rmin;
        let rshift: uint32 = 63 - pg_leftmost_one_pos64(range) as uint32;

        let mut v: uint64;
        loop {
            v = xoroshiro128ss(state) >> rshift;
            if v <= range {
                break;
            }
        }
        val = v;
    } else {
        val = 0;
    }

    // rmin + val: the result is guaranteed to be <= rmax, so this cannot
    // overflow, but use wrapping_add to mirror C's defined unsigned arithmetic.
    rmin.wrapping_add(val)
}

/*
 * Select a random int64 uniformly from the range [PG_INT64_MIN, PG_INT64_MAX].
 */
pub unsafe fn pg_prng_int64(state: *mut pg_prng_state) -> int64 {
    xoroshiro128ss(state) as int64
}

/*
 * Select a random int64 uniformly from the range [0, PG_INT64_MAX].
 */
pub unsafe fn pg_prng_int64p(state: *mut pg_prng_state) -> int64 {
    (xoroshiro128ss(state) & 0x7FFFFFFFFFFFFFFF) as int64
}

/*
 * Select a random int64 uniformly from the range [rmin, rmax].
 * If the range is empty, rmin is always produced.
 */
pub unsafe fn pg_prng_int64_range(state: *mut pg_prng_state, rmin: int64, rmax: int64) -> int64 {
    let val: int64;

    if rmax > rmin {
        /*
         * Use pg_prng_uint64_range().  Can't simply pass it rmin and rmax,
         * since (uint64) rmin will be larger than (uint64) rmax if rmin < 0.
         */
        // (uint64) rmin + pg_prng_uint64_range(state, 0, (uint64)rmax - (uint64)rmin)
        let uval: uint64 = (rmin as uint64).wrapping_add(pg_prng_uint64_range(
            state,
            0,
            (rmax as uint64).wrapping_sub(rmin as uint64),
        ));

        /*
         * Safely convert back to int64, avoiding implementation-defined
         * behavior for values larger than PG_INT64_MAX.  Modern compilers
         * will reduce this to a simple assignment.
         */
        if uval > PG_INT64_MAX as uint64 {
            val = (uval.wrapping_sub(PG_INT64_MIN as uint64) as int64).wrapping_add(PG_INT64_MIN);
        } else {
            val = uval as int64;
        }
    } else {
        val = rmin;
    }

    val
}

/*
 * Select a random uint32 uniformly from the range [0, PG_UINT32_MAX].
 */
pub unsafe fn pg_prng_uint32(state: *mut pg_prng_state) -> uint32 {
    /*
     * Although xoroshiro128** is not known to have any weaknesses in
     * randomness of low-order bits, we prefer to use the upper bits of its
     * result here and below.
     */
    let v: uint64 = xoroshiro128ss(state);

    (v >> 32) as uint32
}

/*
 * Select a random int32 uniformly from the range [PG_INT32_MIN, PG_INT32_MAX].
 */
pub unsafe fn pg_prng_int32(state: *mut pg_prng_state) -> int32 {
    let v: uint64 = xoroshiro128ss(state);

    (v >> 32) as int32
}

/*
 * Select a random int32 uniformly from the range [0, PG_INT32_MAX].
 */
pub unsafe fn pg_prng_int32p(state: *mut pg_prng_state) -> int32 {
    let v: uint64 = xoroshiro128ss(state);

    (v >> 33) as int32
}

/*
 * Select a random double uniformly from the range [0.0, 1.0).
 *
 * Note: if you want a result in the range (0.0, 1.0], the standard way
 * to get that is "1.0 - pg_prng_double(state)".
 */
pub unsafe fn pg_prng_double(state: *mut pg_prng_state) -> f64 {
    let v: uint64 = xoroshiro128ss(state);

    /*
     * As above, assume there's 52 mantissa bits in a double.  This result
     * could round to 1.0 if double's precision is less than that; but we
     * assume IEEE float arithmetic elsewhere in Postgres, so this seems OK.
     */
    // ldexp(x, e) == x * 2^e; here e = -52.
    ((v >> (64 - 52)) as f64) * 2f64.powi(-52)
}

/*
 * Select a random double from the normal distribution with
 * mean = 0.0 and stddev = 1.0.
 *
 * To get a result from a different normal distribution use
 *   STDDEV * pg_prng_double_normal() + MEAN
 *
 * Uses https://en.wikipedia.org/wiki/Box%E2%80%93Muller_transform
 */
pub unsafe fn pg_prng_double_normal(state: *mut pg_prng_state) -> f64 {
    let u1: f64;
    let u2: f64;
    let z0: f64;

    /*
     * pg_prng_double generates [0, 1), but for the basic version of the
     * Box-Muller transform the two uniformly distributed random numbers are
     * expected to be in (0, 1]; in particular we'd better not compute log(0).
     */
    u1 = 1.0 - pg_prng_double(state);
    u2 = 1.0 - pg_prng_double(state);

    /* Apply Box-Muller transform to get one normal-valued output */
    z0 = (-2.0 * u1.ln()).sqrt() * (2.0 * M_PI * u2).sin();
    z0
}

/*
 * Select a random boolean value.
 */
pub unsafe fn pg_prng_bool(state: *mut pg_prng_state) -> bool {
    let v: uint64 = xoroshiro128ss(state);

    (v >> 63) != 0
}
