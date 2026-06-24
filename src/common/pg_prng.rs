//! Translated from PostgreSQL src/include/common/pg_prng.h
// xoroshiro128** PRNG state and API. Bodies stubbed (impl in pg_prng.c port).

/// State vector for PRNG generation (treat as opaque; exposed for embedding).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PgPrngState {
    pub s0: u64,
    pub s1: u64,
}

/// Global state vector for callers not needing a local PRNG series.
pub static mut PG_GLOBAL_PRNG_STATE: PgPrngState = PgPrngState { s0: 0, s1: 0 };

pub fn pg_prng_seed(state: &mut PgPrngState, seed: u64) {
    let _ = (state, seed);
    unimplemented!()
}

pub fn pg_prng_fseed(state: &mut PgPrngState, fseed: f64) {
    let _ = (state, fseed);
    unimplemented!()
}

/// Returns false if the state is all-zeroes (invalid seed).
pub fn pg_prng_seed_check(state: &mut PgPrngState) -> bool {
    let _ = state;
    unimplemented!()
}

/// Seed from a strong-random source; false means caller must seed otherwise.
/// In C this is a macro so the pg_strong_random() call stays in the caller.
pub fn pg_prng_strong_seed(state: &mut PgPrngState) -> bool {
    let _ = state;
    unimplemented!()
}

pub fn pg_prng_uint64(state: &mut PgPrngState) -> u64 {
    let _ = state;
    unimplemented!()
}

pub fn pg_prng_uint64_range(state: &mut PgPrngState, rmin: u64, rmax: u64) -> u64 {
    let _ = (state, rmin, rmax);
    unimplemented!()
}

pub fn pg_prng_int64(state: &mut PgPrngState) -> i64 {
    let _ = state;
    unimplemented!()
}

pub fn pg_prng_int64p(state: &mut PgPrngState) -> i64 {
    let _ = state;
    unimplemented!()
}

pub fn pg_prng_int64_range(state: &mut PgPrngState, rmin: i64, rmax: i64) -> i64 {
    let _ = (state, rmin, rmax);
    unimplemented!()
}

pub fn pg_prng_uint32(state: &mut PgPrngState) -> u32 {
    let _ = state;
    unimplemented!()
}

pub fn pg_prng_int32(state: &mut PgPrngState) -> i32 {
    let _ = state;
    unimplemented!()
}

pub fn pg_prng_int32p(state: &mut PgPrngState) -> i32 {
    let _ = state;
    unimplemented!()
}

pub fn pg_prng_double(state: &mut PgPrngState) -> f64 {
    let _ = state;
    unimplemented!()
}

pub fn pg_prng_double_normal(state: &mut PgPrngState) -> f64 {
    let _ = state;
    unimplemented!()
}

pub fn pg_prng_bool(state: &mut PgPrngState) -> bool {
    let _ = state;
    unimplemented!()
}
