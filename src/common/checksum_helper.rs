//! Translated from PostgreSQL src/include/common/checksum_helper.h
//
// Compute a checksum of various types using common routines. In-memory helper
// types (not on-disk): the C union of context types -> a Rust enum.

use crate::common::cryptohash::PgCryptohashCtx;
use crate::common::sha2::PG_SHA512_DIGEST_LENGTH;
use crate::port::pg_crc32c::pg_crc32c;

/// Supported checksum types. CRC-32C is included for speed (accidental-change
/// detection); MD5 is deliberately omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum pg_checksum_type {
    CHECKSUM_TYPE_NONE,
    CHECKSUM_TYPE_CRC32C,
    CHECKSUM_TYPE_SHA224,
    CHECKSUM_TYPE_SHA256,
    CHECKSUM_TYPE_SHA384,
    CHECKSUM_TYPE_SHA512,
}

/// C union pg_checksum_raw_context (CRC-32C value or a cryptohash context).
/// A union of distinct context types -> a tagged enum.
pub enum pg_checksum_raw_context {
    Crc32c(pg_crc32c),
    Sha2(*mut PgCryptohashCtx), // TODO(ptr): own the context (Box) once impl lands
}

/// Carries the checksum type and its context together.
pub struct pg_checksum_context {
    pub type_: pg_checksum_type,
    pub raw_context: pg_checksum_raw_context,
}

/// Longest possible digest for any supported algorithm.
pub const PG_CHECKSUM_MAX_LENGTH: usize = PG_SHA512_DIGEST_LENGTH;

/// `bool pg_checksum_parse_type(char *name, pg_checksum_type *)` -> Option (the
/// out-param holds the parsed type; false means unrecognized name).
pub fn pg_checksum_parse_type(_name: &str) -> Option<pg_checksum_type> {
    unimplemented!()
}

pub fn pg_checksum_type_name(_type: pg_checksum_type) -> &'static str {
    unimplemented!()
}

/// C functions return an int status (0 ok, -1 error) -> Result.
pub fn pg_checksum_init(_context: &mut pg_checksum_context, _type: pg_checksum_type) -> Result<(), ()> {
    unimplemented!()
}

pub fn pg_checksum_update(_context: &mut pg_checksum_context, _input: &[u8]) -> Result<(), ()> {
    unimplemented!()
}

/// Writes the digest to `output` and returns its length on success.
pub fn pg_checksum_final(_context: &mut pg_checksum_context, _output: &mut [u8]) -> Result<usize, ()> {
    unimplemented!()
}
