//! Translated from PostgreSQL src/include/common/pg_lzcompress.h
// Builtin LZ compressor. Output is on-disk (TOAST) - keep the algorithm bit-exact.

/// Buffer size required by `pglz_compress` (4 bytes overrun allowance).
pub const fn pglz_max_output(dlen: i32) -> i32 {
    dlen + 4
}

/// Values controlling the compression algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PglzStrategy {
    /// Minimum input data size to consider compression.
    pub min_input_size: i32,
    /// Maximum input data size to consider compression.
    pub max_input_size: i32,
    /// Minimum compression rate (0-99%) to require.
    pub min_comp_rate: i32,
    /// Abandon if no compressible data within the first this-many bytes.
    pub first_success_by: i32,
    /// Initial GOOD match size when starting history lookup.
    pub match_size_good: i32,
    /// Percentage by which match_size_good drops after each history check.
    pub match_size_drop: i32,
}

/// Recommended default strategy for TOAST.
pub static PGLZ_STRATEGY_DEFAULT: PglzStrategy = PglzStrategy {
    min_input_size: 32,
    max_input_size: i32::MAX,
    min_comp_rate: 25,
    first_success_by: 1024,
    match_size_good: 128,
    match_size_drop: 10,
};

/// Try to compress inputs of any length.
pub static PGLZ_STRATEGY_ALWAYS: PglzStrategy = PglzStrategy {
    min_input_size: 0,
    max_input_size: i32::MAX,
    min_comp_rate: 0,
    first_success_by: i32::MAX,
    match_size_good: 128,
    match_size_drop: 6,
};

/// Compress `source` into `dest`; Ok holds the compressed length.
pub fn pglz_compress(source: &[u8], dest: &mut [u8], strategy: &PglzStrategy) -> Result<usize, ()> {
    let _ = (source, dest, strategy);
    unimplemented!()
}

/// Decompress `source` into `dest` (`rawsize` expected); Ok holds the output length.
pub fn pglz_decompress(
    source: &[u8],
    dest: &mut [u8],
    rawsize: i32,
    check_complete: bool,
) -> Result<usize, ()> {
    let _ = (source, dest, rawsize, check_complete);
    unimplemented!()
}

/// Largest compressed size still worth decompressing given the totals.
pub fn pglz_maximum_compressed_size(rawsize: i32, total_compressed_size: i32) -> i32 {
    let _ = (rawsize, total_compressed_size);
    unimplemented!()
}
