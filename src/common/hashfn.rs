//! Translated from PostgreSQL src/include/common/hashfn.h
//! COMPAT-SENSITIVE: these hashes route rows in hash indexes and hash
//! partitioning, so the bit values must stay identical to upstream. The inline
//! functions are translated in full; the externs are stubbed.

use crate::postgres::Datum;

/// Rotate the high and low 32 bits separately (extended-hash seed=0 compat).
pub const fn rotate_high_and_low_32bits(v: u64) -> u64 {
    ((v << 1) & 0xfffffffefffffffe) | ((v >> 31) & 0x100000001)
}

pub fn hash_bytes(k: &[u8]) -> u32 {
    let _ = k;
    unimplemented!()
}

pub fn hash_bytes_extended(k: &[u8], seed: u64) -> u64 {
    let _ = (k, seed);
    unimplemented!()
}

pub fn hash_bytes_uint32(k: u32) -> u32 {
    let _ = k;
    unimplemented!()
}

pub fn hash_bytes_uint32_extended(k: u32, seed: u64) -> u64 {
    let _ = (k, seed);
    unimplemented!()
}

// Backend-only inline wrappers (return a Datum).

pub fn hash_any(k: &[u8]) -> Datum {
    Datum(hash_bytes(k) as usize)
}

pub fn hash_any_extended(k: &[u8], seed: u64) -> Datum {
    Datum(hash_bytes_extended(k, seed) as usize)
}

pub fn hash_uint32(k: u32) -> Datum {
    Datum(hash_bytes_uint32(k) as usize)
}

pub fn hash_uint32_extended(k: u32, seed: u64) -> Datum {
    Datum(hash_bytes_uint32_extended(k, seed) as usize)
}

pub fn string_hash(key: &[u8], keysize: usize) -> u32 {
    let _ = (key, keysize);
    unimplemented!()
}

pub fn tag_hash(key: &[u8], keysize: usize) -> u32 {
    let _ = (key, keysize);
    unimplemented!()
}

pub fn uint32_hash(key: &[u8], keysize: usize) -> u32 {
    let _ = (key, keysize);
    unimplemented!()
}

/// `oid_hash` is an alias for `uint32_hash` (to be removed upstream eventually).
pub use self::uint32_hash as oid_hash;

/// Combine two 32-bit hash values with decent bit mixing (boost-style).
pub const fn hash_combine(mut a: u32, b: u32) -> u32 {
    a ^= b
        .wrapping_add(0x9e3779b9)
        .wrapping_add(a << 6)
        .wrapping_add(a >> 2);
    a
}

/// Combine two 64-bit hash values with good bit mixing.
pub const fn hash_combine64(mut a: u64, b: u64) -> u64 {
    a ^= b
        .wrapping_add(0x49a0f4dd15e5a8e3)
        .wrapping_add(a << 54)
        .wrapping_add(a >> 7);
    a
}

/// Inline murmur hash of a 32-bit integer.
pub const fn murmurhash32(data: u32) -> u32 {
    let mut h = data;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;
    h
}

/// 64-bit murmur hash variant.
pub const fn murmurhash64(data: u64) -> u64 {
    let mut h = data;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
}
