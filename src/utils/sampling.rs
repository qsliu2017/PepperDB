//! Translated from PostgreSQL src/include/utils/sampling.h

use crate::common::pg_prng::PgPrngState;
use crate::storage::block::BlockNumber;

// Random generator for sampling code.
pub fn sampler_random_init_state(seed: u32, randstate: &mut PgPrngState) {
    unimplemented!()
}
pub fn sampler_random_fract(randstate: &mut PgPrngState) -> f64 {
    unimplemented!()
}

/// Block sampling: Algorithm S from Knuth 3.4.2.
pub struct BlockSamplerData {
    pub N: BlockNumber, // number of blocks, known in advance
    pub n: i32,         // desired sample size
    pub t: BlockNumber, // current block number
    pub m: i32,         // blocks selected so far
    pub randstate: PgPrngState,
}
pub type BlockSampler = *mut BlockSamplerData; // TODO(ptr)

pub fn BlockSampler_Init(
    bs: &mut BlockSamplerData,
    nblocks: BlockNumber,
    samplesize: i32,
    randseed: u32,
) -> BlockNumber {
    unimplemented!()
}
pub fn BlockSampler_HasMore(bs: &mut BlockSamplerData) -> bool {
    unimplemented!()
}
pub fn BlockSampler_Next(bs: &mut BlockSamplerData) -> BlockNumber {
    unimplemented!()
}

/// Reservoir sampling state.
pub struct ReservoirStateData {
    pub W: f64,
    pub randstate: PgPrngState,
}
pub type ReservoirState = *mut ReservoirStateData; // TODO(ptr)

pub fn reservoir_init_selection_state(rs: &mut ReservoirStateData, n: i32) {
    unimplemented!()
}
pub fn reservoir_get_next_S(rs: &mut ReservoirStateData, t: f64, n: i32) -> f64 {
    unimplemented!()
}

// Old API, still used by assorted FDWs.
pub fn anl_random_fract() -> f64 {
    unimplemented!()
}
pub fn anl_init_selection_state(n: i32) -> f64 {
    unimplemented!()
}
/// `double *stateptr` in/out-param kept as &mut.
pub fn anl_get_next_S(t: f64, n: i32, stateptr: &mut f64) -> f64 {
    unimplemented!()
}
