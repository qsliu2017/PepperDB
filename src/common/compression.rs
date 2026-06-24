//! Translated from PostgreSQL src/include/common/compression.h
//! Shared definitions for compression methods and specifications.

use bitflags::bitflags;

/// Compression algorithm. Values are persisted (e.g. pg_dump), so order is fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum PgCompressAlgorithm {
    None = 0,
    Gzip,
    Lz4,
    Zstd,
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PgCompressionOption: u32 {
        const WORKERS       = 1 << 0;
        const LONG_DISTANCE = 1 << 1;
    }
}

/// Parsed compression specification.
pub struct PgCompressSpecification {
    pub algorithm: PgCompressAlgorithm,
    pub options: PgCompressionOption,
    pub level: i32,
    pub workers: i32,
    pub long_distance: bool,
    /// None if parsing was OK, else the error message.
    pub parse_error: Option<String>,
}

/// Split an option string into algorithm and detail parts.
pub fn parse_compress_options(option: &str) -> (String, Option<String>) {
    let _ = option;
    unimplemented!()
}

/// Parse an algorithm name; None if unrecognized.
pub fn parse_compress_algorithm(name: &str) -> Option<PgCompressAlgorithm> {
    let _ = name;
    unimplemented!()
}

/// Name string for an algorithm.
pub fn get_compress_algorithm_name(algorithm: PgCompressAlgorithm) -> &'static str {
    let _ = algorithm;
    unimplemented!()
}

/// Parse a full specification into `result`.
pub fn parse_compress_specification(
    algorithm: PgCompressAlgorithm,
    specification: Option<&str>,
) -> PgCompressSpecification {
    let _ = (algorithm, specification);
    unimplemented!()
}

/// Validate a specification; None if valid, else the error message.
pub fn validate_compress_specification(spec: &PgCompressSpecification) -> Option<String> {
    let _ = spec;
    unimplemented!()
}
