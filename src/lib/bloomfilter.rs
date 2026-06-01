//! Translation of postgres/src/include/lib/bloomfilter.h
//!                + postgres/src/backend/lib/bloomfilter.c
//!
//! Space-efficient set membership testing
//!
//! A Bloom filter is a probabilistic data structure that is used to test an
//! element's membership of a set.  False positives are possible, but false
//! negatives are not; a test of membership of the set returns either "possibly
//! in set" or "definitely not in set".  This is typically very space efficient,
//! which can be a decisive advantage.
//!
//! Elements can be added to the set, but not removed.  The more elements that
//! are added, the larger the probability of false positives.  Caller must hint
//! an estimated total size of the set when the Bloom filter is initialized.
//! This is used to balance the use of memory against the final false positive
//! rate.
//!
//! The implementation is well suited to data synchronization problems between
//! unordered sets, especially where predictable performance is important and
//! some false positives are acceptable.  It's also well suited to cache
//! filtering problems where a relatively small and/or low cardinality set is
//! fingerprinted, especially when many subsequent membership tests end up
//! indicating that values of interest are not present.  That should save the
//! caller many authoritative lookups, such as expensive probes of a much larger
//! on-disk structure.
//!
//! Copyright (c) 2018-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!	  src/backend/lib/bloomfilter.c

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_uchar};

use crate::common::hashfn::hash_any_extended;
use crate::port::pg_bitutils::pg_popcount;

// TODO(pg-port): BITS_PER_BYTE is defined as 8 in
// postgres/src/include/pg_config_manual.h, which is not part of the translated
// prelude.  Mirror the sibling lib/hyperloglog.rs convention and define it
// locally.  It is a uint64 here because every use site in this file multiplies
// or divides a uint64 bitset size by it.
const BITS_PER_BYTE: uint64 = 8;

const MAX_HASH_FUNCS: usize = 10;

#[repr(C)]
pub struct bloom_filter {
    /* K hash functions are used, seeded by caller's seed */
    pub k_hash_funcs: c_int,
    pub seed: uint64,
    /* m is bitset size, in bits.  Must be a power of two <= 2^32.  */
    pub m: uint64,
    // unsigned char bitset[FLEXIBLE_ARRAY_MEMBER];
    //
    // A trailing flexible array member.  Represented as a zero-length array
    // placed last in the struct (the c.rs convention).  The actual storage is
    // allocated by bloom_create via palloc0 of
    //   offset_of!(bloom_filter, bitset) + bitset_bytes
    // and indexed through a raw byte pointer derived from the struct pointer.
    pub bitset: [c_uchar; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * Create Bloom filter in caller's memory context.  We aim for a false positive
 * rate of between 1% and 2% when bitset size is not constrained by memory
 * availability.
 *
 * total_elems is an estimate of the final size of the set.  It should be
 * approximately correct, but the implementation can cope well with it being
 * off by perhaps a factor of five or more.  See "Bloom Filters in
 * Probabilistic Verification" (Dillinger & Manolios, 2004) for details of why
 * this is the case.
 *
 * bloom_work_mem is sized in KB, in line with the general work_mem convention.
 * This determines the size of the underlying bitset (trivial bookkeeping space
 * isn't counted).  The bitset is always sized as a power of two number of
 * bits, and the largest possible bitset is 512MB (2^32 bits).  The
 * implementation allocates only enough memory to target its standard false
 * positive rate, using a simple formula with caller's total_elems estimate as
 * an input.  The bitset might be as small as 1MB, even when bloom_work_mem is
 * much higher.
 *
 * The Bloom filter is seeded using a value provided by the caller.  Using a
 * distinct seed value on every call makes it unlikely that the same false
 * positives will reoccur when the same set is fingerprinted a second time.
 * Callers that don't care about this pass a constant as their seed, typically
 * 0.  Callers can also use a pseudo-random seed, eg from pg_prng_uint64().
 */
pub unsafe fn bloom_create(
    total_elems: int64,
    bloom_work_mem: c_int,
    seed: uint64,
) -> *mut bloom_filter {
    let filter: *mut bloom_filter;
    let bloom_power: c_int;
    let mut bitset_bytes: uint64;
    let bitset_bits: uint64;

    /*
     * Aim for two bytes per element; this is sufficient to get a false
     * positive rate below 1%, independent of the size of the bitset or total
     * number of elements.  Also, if rounding down the size of the bitset to
     * the next lowest power of two turns out to be a significant drop, the
     * false positive rate still won't exceed 2% in almost all cases.
     */
    // C: Min(bloom_work_mem * UINT64CONST(1024), total_elems * 2)
    // bloom_work_mem is promoted to uint64 by the * 1024 and total_elems
    // (int64) is reinterpreted as uint64 to match the Min operand types; both
    // products wrap on overflow as in C.
    bitset_bytes = Min(
        (bloom_work_mem as uint64).wrapping_mul(UINT64CONST(1024)),
        (total_elems as uint64).wrapping_mul(2),
    );
    bitset_bytes = Max(1024 * 1024, bitset_bytes);

    /*
     * Size in bits should be the highest power of two <= target.  bitset_bits
     * is uint64 because PG_UINT32_MAX is 2^32 - 1, not 2^32
     */
    bloom_power = my_bloom_power(bitset_bytes.wrapping_mul(BITS_PER_BYTE));
    bitset_bits = UINT64CONST(1) << bloom_power;
    bitset_bytes = bitset_bits / BITS_PER_BYTE;

    /* Allocate bloom filter with unset bitset */
    // palloc0 of offsetof(bloom_filter, bitset) + sizeof(unsigned char) *
    // bitset_bytes.  size_of::<c_uchar>() == 1, so the bitset contributes
    // bitset_bytes bytes.
    filter = palloc0(
        core::mem::offset_of!(bloom_filter, bitset)
            + core::mem::size_of::<c_uchar>() * bitset_bytes as Size,
    ) as *mut bloom_filter;
    (*filter).k_hash_funcs = optimal_k(bitset_bits, total_elems);
    (*filter).seed = seed;
    (*filter).m = bitset_bits;

    filter
}

/*
 * Free Bloom filter
 */
pub unsafe fn bloom_free(filter: *mut bloom_filter) {
    pfree(filter as *mut core::ffi::c_void);
}

/*
 * Add element to Bloom filter
 */
pub unsafe fn bloom_add_element(filter: *mut bloom_filter, elem: *mut c_uchar, len: Size) {
    let mut hashes: [uint32; MAX_HASH_FUNCS] = [0; MAX_HASH_FUNCS];

    k_hashes(filter, hashes.as_mut_ptr(), elem, len);

    /* Map a bit-wise address to a byte-wise address + bit offset */
    // filter->bitset is the flexible array member; index it through a raw byte
    // pointer derived from the struct pointer.
    let bitset = bloom_filter_bitset_mut(filter);
    let mut i: c_int = 0;
    while i < (*filter).k_hash_funcs {
        let h = hashes[i as usize];
        let byte = bitset.add((h >> 3) as usize);
        *byte |= (1u32 << (h & 7)) as c_uchar;
        i += 1;
    }
}

/*
 * Test if Bloom filter definitely lacks element.
 *
 * Returns true if the element is definitely not in the set of elements
 * observed by bloom_add_element().  Otherwise, returns false, indicating that
 * element is probably present in set.
 */
pub unsafe fn bloom_lacks_element(filter: *mut bloom_filter, elem: *mut c_uchar, len: Size) -> bool {
    let mut hashes: [uint32; MAX_HASH_FUNCS] = [0; MAX_HASH_FUNCS];

    k_hashes(filter, hashes.as_mut_ptr(), elem, len);

    /* Map a bit-wise address to a byte-wise address + bit offset */
    let bitset = bloom_filter_bitset_mut(filter);
    let mut i: c_int = 0;
    while i < (*filter).k_hash_funcs {
        let h = hashes[i as usize];
        let byte = *bitset.add((h >> 3) as usize);
        if (byte & (1u32 << (h & 7)) as c_uchar) == 0 {
            return true;
        }
        i += 1;
    }

    false
}

/*
 * What proportion of bits are currently set?
 *
 * Returns proportion, expressed as a multiplier of filter size.  That should
 * generally be close to 0.5, even when we have more than enough memory to
 * ensure a false positive rate within target 1% to 2% band, since more hash
 * functions are used as more memory is available per element.
 *
 * This is the only instrumentation that is low overhead enough to appear in
 * debug traces.  When debugging Bloom filter code, it's likely to be far more
 * interesting to directly test the false positive rate.
 */
pub unsafe fn bloom_prop_bits_set(filter: *mut bloom_filter) -> f64 {
    let bitset_bytes: c_int = ((*filter).m / BITS_PER_BYTE) as c_int;
    let bits_set: uint64 = pg_popcount(bloom_filter_bitset_mut(filter) as *const c_char, bitset_bytes);

    bits_set as f64 / (*filter).m as f64
}

/*
 * Return a raw pointer to the start of the flexible array member `bitset`.
 *
 * TODO(pg-port): the C code names `filter->bitset` directly; here the zero-
 * length array carries no usable storage, so we recover the address of the
 * trailing flexible array from the struct base plus its byte offset.
 */
#[inline]
unsafe fn bloom_filter_bitset_mut(filter: *mut bloom_filter) -> *mut c_uchar {
    (filter as *mut u8).add(core::mem::offset_of!(bloom_filter, bitset)) as *mut c_uchar
}

/*
 * Which element in the sequence of powers of two is less than or equal to
 * target_bitset_bits?
 *
 * Value returned here must be generally safe as the basis for actual bitset
 * size.
 *
 * Bitset is never allowed to exceed 2 ^ 32 bits (512MB).  This is sufficient
 * for the needs of all current callers, and allows us to use 32-bit hash
 * functions.  It also makes it easy to stay under the MaxAllocSize restriction
 * (caller needs to leave room for non-bitset fields that appear before
 * flexible array member, so a 1GB bitset would use an allocation that just
 * exceeds MaxAllocSize).
 */
fn my_bloom_power(target_bitset_bits: uint64) -> c_int {
    let mut target_bitset_bits = target_bitset_bits;
    let mut bloom_power: c_int = -1;

    while target_bitset_bits > 0 && bloom_power < 32 {
        bloom_power += 1;
        target_bitset_bits >>= 1;
    }

    bloom_power
}

/*
 * Determine optimal number of hash functions based on size of filter in bits,
 * and projected total number of elements.  The optimal number is the number
 * that minimizes the false positive rate.
 */
fn optimal_k(bitset_bits: uint64, total_elems: int64) -> c_int {
    // C: int k = rint(log(2.0) * bitset_bits / total_elems);
    // rint() rounds to nearest with ties-to-even (the default FP rounding
    // mode); Rust's round_ties_even() matches that exactly (f64::round() would
    // instead round halves away from zero).
    let k: c_int =
        (2.0f64.ln() * bitset_bits as f64 / total_elems as f64).round_ties_even() as c_int;

    Max(1, Min(k, MAX_HASH_FUNCS as c_int))
}

/*
 * Generate k hash values for element.
 *
 * Caller passes array, which is filled-in with k values determined by hashing
 * caller's element.
 *
 * Only 2 real independent hash functions are actually used to support an
 * interface of up to MAX_HASH_FUNCS hash functions; enhanced double hashing is
 * used to make this work.  The main reason we prefer enhanced double hashing
 * to classic double hashing is that the latter has an issue with collisions
 * when using power of two sized bitsets.  See Dillinger & Manolios for full
 * details.
 */
unsafe fn k_hashes(filter: *mut bloom_filter, hashes: *mut uint32, elem: *mut c_uchar, len: Size) {
    let hash: uint64;
    let mut x: uint32;
    let mut y: uint32;
    let m: uint64;

    /* Use 64-bit hashing to get two independent 32-bit hashes */
    // hash_any_extended takes (k: *const c_uchar, keylen: c_int, seed: uint64)
    // and returns Datum; DatumGetUInt64 recovers the uint64 hash.
    hash = DatumGetUInt64(hash_any_extended(elem as *const c_uchar, len as c_int, (*filter).seed));
    x = hash as uint32;
    y = (hash >> 32) as uint32;
    m = (*filter).m;

    x = mod_m(x, m);
    y = mod_m(y, m);

    /* Accumulate hashes */
    *hashes.add(0) = x;
    let mut i: c_int = 1;
    while i < (*filter).k_hash_funcs {
        // x = mod_m(x + y, m); y = mod_m(y + i, m);
        // The additions are uint32 (the mod_m argument type) and wrap on
        // overflow as in C.
        x = mod_m(x.wrapping_add(y), m);
        y = mod_m(y.wrapping_add(i as uint32), m);

        *hashes.add(i as usize) = x;
        i += 1;
    }
}

/*
 * Calculate "val MOD m" inexpensively.
 *
 * Assumes that m (which is bitset size) is a power of two.
 *
 * Using a power of two number of bits for bitset size allows us to use bitwise
 * AND operations to calculate the modulo of a hash value.  It's also a simple
 * way of avoiding the modulo bias effect.
 */
#[inline]
fn mod_m(val: uint32, m: uint64) -> uint32 {
    Assert!(m <= PG_UINT32_MAX as uint64 + UINT64CONST(1));
    Assert!(((m - 1) & m) == 0);

    // val & (m - 1): m fits in 33 bits (<= 2^32) but (m - 1) fits in 32 bits,
    // so the truncation to uint32 is exact and matches C's `val & (m - 1)`
    // where the uint32 val is promoted, AND'd, then the result is uint32.
    val & ((m - 1) as uint32)
}
