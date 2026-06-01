//! Translation of postgres/src/include/common/hashfn.h
//!                + postgres/src/common/hashfn.c
//!
//! Generic hashing functions, and hash functions for use in dynahash.c
//! hashtables.
//!
//! NOTES (from hashfn.c):
//!   It is expected that every bit of a hash function's 32-bit result is
//!   as random as every other; failure to ensure this is likely to lead
//!   to poor performance of hash tables.  In most cases a hash
//!   function should use hash_bytes() or its variant hash_bytes_uint32(),
//!   or the wrappers hash_any() and hash_uint32 defined in hashfn.h.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};

use crate::port::pg_bitutils::pg_rotate_left32;

/*
 * Rotate the high 32 bits and the low 32 bits separately.  The standard
 * hash function sometimes rotates the low 32 bits by one bit when
 * combining elements.  We want extended hash functions to be compatible with
 * that algorithm when the seed is 0, so we can't just do a normal rotation.
 * This works, though.
 *
 * #define ROTATE_HIGH_AND_LOW_32BITS(v) ...
 *
 * Translated as a private const fn.  Note: this macro is defined in the
 * header but is not referenced by any code translated here; it is kept for
 * fidelity with the original source.  Arithmetic is on uint64; the shifts and
 * masks cannot overflow, but we use wrapping shifts for safety against the
 * crate's debug overflow checks.
 */
#[allow(dead_code)]
const fn ROTATE_HIGH_AND_LOW_32BITS(v: uint64) -> uint64 {
    (((v << 1) & 0xfffffffefffffffe_u64) | ((v >> 31) & 0x100000001_u64))
}

/* Get a bit mask of the bits set in non-uint32 aligned addresses */
const UINT32_ALIGN_MASK: usize = core::mem::size_of::<uint32>() - 1;

/*
 * #define rot(x,k) pg_rotate_left32(x, k)
 *
 * pg_rotate_left32 takes (word: uint32, n: c_int); the bit mixing below never
 * passes k >= 32, so this matches the C semantics exactly.
 */
#[inline(always)]
fn rot(x: uint32, k: c_int) -> uint32 {
    pg_rotate_left32(x, k)
}

/*----------
 * mix -- mix 3 32-bit values reversibly.
 *
 * This is reversible, so any information in (a,b,c) before mix() is
 * still in (a,b,c) after mix().
 *
 * (See the original comment in hashfn.c for the full discussion of the
 * mixing properties.)  Implemented as a macro operating on three uint32
 * lvalues.  ALL arithmetic uses wrapping ops because the C `+`/`-` wrap on
 * overflow whereas Rust would otherwise panic in debug builds; hash
 * correctness depends on exact two's-complement wrapping.
 *----------
 */
macro_rules! mix {
    ($a:expr, $b:expr, $c:expr) => {{
        $a = $a.wrapping_sub($c);
        $a ^= rot($c, 4);
        $c = $c.wrapping_add($b);
        $b = $b.wrapping_sub($a);
        $b ^= rot($a, 6);
        $a = $a.wrapping_add($c);
        $c = $c.wrapping_sub($b);
        $c ^= rot($b, 8);
        $b = $b.wrapping_add($a);
        $a = $a.wrapping_sub($c);
        $a ^= rot($c, 16);
        $c = $c.wrapping_add($b);
        $b = $b.wrapping_sub($a);
        $b ^= rot($a, 19);
        $a = $a.wrapping_add($c);
        $c = $c.wrapping_sub($b);
        $c ^= rot($b, 4);
        $b = $b.wrapping_add($a);
    }};
}

/*----------
 * final -- final mixing of 3 32-bit values (a,b,c) into c
 *
 * Pairs of (a,b,c) values differing in only a few bits will usually
 * produce values of c that look totally different.  (See the original
 * comment in hashfn.c for the full discussion.)
 *
 * Implemented as a macro operating on three uint32 lvalues.  All arithmetic
 * uses wrapping ops (see the note on mix! above).  `final` is a reserved-ish
 * word in some contexts, so we name the macro `r#final`.
 *----------
 */
macro_rules! r#final {
    ($a:expr, $b:expr, $c:expr) => {{
        $c ^= $b;
        $c = $c.wrapping_sub(rot($b, 14));
        $a ^= $c;
        $a = $a.wrapping_sub(rot($c, 11));
        $b ^= $a;
        $b = $b.wrapping_sub(rot($a, 25));
        $c ^= $b;
        $c = $c.wrapping_sub(rot($b, 16));
        $a ^= $c;
        $a = $a.wrapping_sub(rot($c, 4));
        $b ^= $a;
        $b = $b.wrapping_sub(rot($a, 14));
        $c ^= $b;
        $c = $c.wrapping_sub(rot($b, 24));
    }};
}

/*
 * hash_bytes() -- hash a variable-length key into a 32-bit value
 *		k		: the key (the unaligned variable-length array of bytes)
 *		len		: the length of the key, counting by bytes
 *
 * Returns a uint32 value.  Every bit of the key affects every bit of
 * the return value.  Every 1-bit and 2-bit delta achieves avalanche.
 * About 6*len+35 instructions. The best hash table sizes are powers
 * of 2.  There is no need to do mod a prime (mod is sooo slow!).
 * If you need less than 32 bits, use a bitmask.
 *
 * This procedure must never throw elog(ERROR); the ResourceOwner code
 * relies on this not to fail.
 *
 * Note: we could easily change this function to return a 64-bit hash value
 * by using the final values of both b and c.  b is perhaps a little less
 * well mixed than c, however.
 *
 * This build is little-endian (WORDS_BIGENDIAN unset), so we only translate
 * the !WORDS_BIGENDIAN code paths.  Word-wide fetches are reproduced by
 * reading four bytes and assembling them little-endian, which is value-
 * identical to the C `const uint32 *` fetch on this platform while avoiding
 * any unaligned-load undefined behavior in Rust.
 */
pub unsafe fn hash_bytes(k: *const core::ffi::c_uchar, keylen: c_int) -> uint32 {
    let mut a: uint32;
    let mut b: uint32;
    let mut c: uint32;
    let mut len: uint32;

    /* Set up the internal state */
    len = keylen as uint32;
    a = 0x9e3779b9_u32
        .wrapping_add(len)
        .wrapping_add(3923095);
    b = a;
    c = a;

    let mut k = k;

    /* If the source pointer is word-aligned, we use word-wide fetches */
    if ((k as usize) & UINT32_ALIGN_MASK) == 0 {
        /* Code path for aligned source data */
        let mut ka = k as *const uint32;

        /* handle most of the key */
        while len >= 12 {
            a = a.wrapping_add(read_word_le(ka, 0));
            b = b.wrapping_add(read_word_le(ka, 1));
            c = c.wrapping_add(read_word_le(ka, 2));
            mix!(a, b, c);
            ka = ka.add(3);
            len -= 12;
        }

        /* handle the last 11 bytes */
        k = ka as *const core::ffi::c_uchar;
        /* !WORDS_BIGENDIAN */
        match len {
            11 => {
                c = c.wrapping_add((*k.add(10) as uint32) << 24);
                c = c.wrapping_add((*k.add(9) as uint32) << 16);
                c = c.wrapping_add((*k.add(8) as uint32) << 8);
                /* the lowest byte of c is reserved for the length */
                b = b.wrapping_add(read_word_le(ka, 1));
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            10 => {
                c = c.wrapping_add((*k.add(9) as uint32) << 16);
                c = c.wrapping_add((*k.add(8) as uint32) << 8);
                b = b.wrapping_add(read_word_le(ka, 1));
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            9 => {
                c = c.wrapping_add((*k.add(8) as uint32) << 8);
                b = b.wrapping_add(read_word_le(ka, 1));
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            8 => {
                /* the lowest byte of c is reserved for the length */
                b = b.wrapping_add(read_word_le(ka, 1));
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            7 => {
                b = b.wrapping_add((*k.add(6) as uint32) << 16);
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            6 => {
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            5 => {
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            4 => {
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            3 => {
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            2 => {
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            1 => {
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            _ => { /* case 0: nothing left to add */ }
        }
    } else {
        /* Code path for non-aligned source data */

        /* handle most of the key */
        while len >= 12 {
            /* !WORDS_BIGENDIAN */
            a = a.wrapping_add(
                (*k.add(0) as uint32)
                    .wrapping_add((*k.add(1) as uint32) << 8)
                    .wrapping_add((*k.add(2) as uint32) << 16)
                    .wrapping_add((*k.add(3) as uint32) << 24),
            );
            b = b.wrapping_add(
                (*k.add(4) as uint32)
                    .wrapping_add((*k.add(5) as uint32) << 8)
                    .wrapping_add((*k.add(6) as uint32) << 16)
                    .wrapping_add((*k.add(7) as uint32) << 24),
            );
            c = c.wrapping_add(
                (*k.add(8) as uint32)
                    .wrapping_add((*k.add(9) as uint32) << 8)
                    .wrapping_add((*k.add(10) as uint32) << 16)
                    .wrapping_add((*k.add(11) as uint32) << 24),
            );
            mix!(a, b, c);
            k = k.add(12);
            len -= 12;
        }

        /* handle the last 11 bytes */
        /* !WORDS_BIGENDIAN */
        match len {
            11 => {
                c = c.wrapping_add((*k.add(10) as uint32) << 24);
                c = c.wrapping_add((*k.add(9) as uint32) << 16);
                c = c.wrapping_add((*k.add(8) as uint32) << 8);
                /* the lowest byte of c is reserved for the length */
                b = b.wrapping_add((*k.add(7) as uint32) << 24);
                b = b.wrapping_add((*k.add(6) as uint32) << 16);
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            10 => {
                c = c.wrapping_add((*k.add(9) as uint32) << 16);
                c = c.wrapping_add((*k.add(8) as uint32) << 8);
                b = b.wrapping_add((*k.add(7) as uint32) << 24);
                b = b.wrapping_add((*k.add(6) as uint32) << 16);
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            9 => {
                c = c.wrapping_add((*k.add(8) as uint32) << 8);
                b = b.wrapping_add((*k.add(7) as uint32) << 24);
                b = b.wrapping_add((*k.add(6) as uint32) << 16);
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            8 => {
                /* the lowest byte of c is reserved for the length */
                b = b.wrapping_add((*k.add(7) as uint32) << 24);
                b = b.wrapping_add((*k.add(6) as uint32) << 16);
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            7 => {
                b = b.wrapping_add((*k.add(6) as uint32) << 16);
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            6 => {
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            5 => {
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            4 => {
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            3 => {
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            2 => {
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            1 => {
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            _ => { /* case 0: nothing left to add */ }
        }
    }

    r#final!(a, b, c);

    /* report the result */
    c
}

/*
 * read_word_le() -- read the i-th 32-bit word from an aligned uint32 pointer,
 * assembling it little-endian.
 *
 * TODO(pg-port): the C code does a direct `ka[i]` fetch through a
 * `const uint32 *`.  On this little-endian build that is value-identical to
 * reading four bytes and reassembling with `from_le_bytes`.  We read the bytes
 * to avoid relying on the pointer actually being suitably aligned for a
 * `read()` of uint32 (the alignment check only guarantees 4-byte alignment for
 * the *base* pointer, which is exactly what uint32 needs, but going through
 * bytes is robust regardless).
 */
#[inline(always)]
unsafe fn read_word_le(ka: *const uint32, i: usize) -> uint32 {
    let p = (ka as *const u8).add(i * 4);
    let bytes = [*p, *p.add(1), *p.add(2), *p.add(3)];
    u32::from_le_bytes(bytes)
}

/*
 * hash_bytes_extended() -- hash into a 64-bit value, using an optional seed
 *		k		: the key (the unaligned variable-length array of bytes)
 *		len		: the length of the key, counting by bytes
 *		seed	: a 64-bit seed (0 means no seed)
 *
 * Returns a uint64 value.  Otherwise similar to hash_bytes.
 *
 * Only the !WORDS_BIGENDIAN paths are translated (little-endian build).
 */
pub unsafe fn hash_bytes_extended(
    k: *const core::ffi::c_uchar,
    keylen: c_int,
    seed: uint64,
) -> uint64 {
    let mut a: uint32;
    let mut b: uint32;
    let mut c: uint32;
    let mut len: uint32;

    /* Set up the internal state */
    len = keylen as uint32;
    a = 0x9e3779b9_u32
        .wrapping_add(len)
        .wrapping_add(3923095);
    b = a;
    c = a;

    /* If the seed is non-zero, use it to perturb the internal state. */
    if seed != 0 {
        /*
         * In essence, the seed is treated as part of the data being hashed,
         * but for simplicity, we pretend that it's padded with four bytes of
         * zeroes so that the seed constitutes a 12-byte chunk.
         */
        a = a.wrapping_add((seed >> 32) as uint32);
        b = b.wrapping_add(seed as uint32);
        mix!(a, b, c);
    }

    let mut k = k;

    /* If the source pointer is word-aligned, we use word-wide fetches */
    if ((k as usize) & UINT32_ALIGN_MASK) == 0 {
        /* Code path for aligned source data */
        let mut ka = k as *const uint32;

        /* handle most of the key */
        while len >= 12 {
            a = a.wrapping_add(read_word_le(ka, 0));
            b = b.wrapping_add(read_word_le(ka, 1));
            c = c.wrapping_add(read_word_le(ka, 2));
            mix!(a, b, c);
            ka = ka.add(3);
            len -= 12;
        }

        /* handle the last 11 bytes */
        k = ka as *const core::ffi::c_uchar;
        /* !WORDS_BIGENDIAN */
        match len {
            11 => {
                c = c.wrapping_add((*k.add(10) as uint32) << 24);
                c = c.wrapping_add((*k.add(9) as uint32) << 16);
                c = c.wrapping_add((*k.add(8) as uint32) << 8);
                /* the lowest byte of c is reserved for the length */
                b = b.wrapping_add(read_word_le(ka, 1));
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            10 => {
                c = c.wrapping_add((*k.add(9) as uint32) << 16);
                c = c.wrapping_add((*k.add(8) as uint32) << 8);
                b = b.wrapping_add(read_word_le(ka, 1));
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            9 => {
                c = c.wrapping_add((*k.add(8) as uint32) << 8);
                b = b.wrapping_add(read_word_le(ka, 1));
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            8 => {
                /* the lowest byte of c is reserved for the length */
                b = b.wrapping_add(read_word_le(ka, 1));
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            7 => {
                b = b.wrapping_add((*k.add(6) as uint32) << 16);
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            6 => {
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            5 => {
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            4 => {
                a = a.wrapping_add(read_word_le(ka, 0));
            }
            3 => {
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            2 => {
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            1 => {
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            _ => { /* case 0: nothing left to add */ }
        }
    } else {
        /* Code path for non-aligned source data */

        /* handle most of the key */
        while len >= 12 {
            /* !WORDS_BIGENDIAN */
            a = a.wrapping_add(
                (*k.add(0) as uint32)
                    .wrapping_add((*k.add(1) as uint32) << 8)
                    .wrapping_add((*k.add(2) as uint32) << 16)
                    .wrapping_add((*k.add(3) as uint32) << 24),
            );
            b = b.wrapping_add(
                (*k.add(4) as uint32)
                    .wrapping_add((*k.add(5) as uint32) << 8)
                    .wrapping_add((*k.add(6) as uint32) << 16)
                    .wrapping_add((*k.add(7) as uint32) << 24),
            );
            c = c.wrapping_add(
                (*k.add(8) as uint32)
                    .wrapping_add((*k.add(9) as uint32) << 8)
                    .wrapping_add((*k.add(10) as uint32) << 16)
                    .wrapping_add((*k.add(11) as uint32) << 24),
            );
            mix!(a, b, c);
            k = k.add(12);
            len -= 12;
        }

        /* handle the last 11 bytes */
        /* !WORDS_BIGENDIAN */
        match len {
            11 => {
                c = c.wrapping_add((*k.add(10) as uint32) << 24);
                c = c.wrapping_add((*k.add(9) as uint32) << 16);
                c = c.wrapping_add((*k.add(8) as uint32) << 8);
                /* the lowest byte of c is reserved for the length */
                b = b.wrapping_add((*k.add(7) as uint32) << 24);
                b = b.wrapping_add((*k.add(6) as uint32) << 16);
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            10 => {
                c = c.wrapping_add((*k.add(9) as uint32) << 16);
                c = c.wrapping_add((*k.add(8) as uint32) << 8);
                b = b.wrapping_add((*k.add(7) as uint32) << 24);
                b = b.wrapping_add((*k.add(6) as uint32) << 16);
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            9 => {
                c = c.wrapping_add((*k.add(8) as uint32) << 8);
                b = b.wrapping_add((*k.add(7) as uint32) << 24);
                b = b.wrapping_add((*k.add(6) as uint32) << 16);
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            8 => {
                /* the lowest byte of c is reserved for the length */
                b = b.wrapping_add((*k.add(7) as uint32) << 24);
                b = b.wrapping_add((*k.add(6) as uint32) << 16);
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            7 => {
                b = b.wrapping_add((*k.add(6) as uint32) << 16);
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            6 => {
                b = b.wrapping_add((*k.add(5) as uint32) << 8);
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            5 => {
                b = b.wrapping_add(*k.add(4) as uint32);
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            4 => {
                a = a.wrapping_add((*k.add(3) as uint32) << 24);
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            3 => {
                a = a.wrapping_add((*k.add(2) as uint32) << 16);
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            2 => {
                a = a.wrapping_add((*k.add(1) as uint32) << 8);
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            1 => {
                a = a.wrapping_add(*k.add(0) as uint32);
            }
            _ => { /* case 0: nothing left to add */ }
        }
    }

    r#final!(a, b, c);

    /* report the result */
    ((b as uint64) << 32) | (c as uint64)
}

/*
 * hash_bytes_uint32() -- hash a 32-bit value to a 32-bit value
 *
 * This has the same result as
 *		hash_bytes(&k, sizeof(uint32))
 * but is faster and doesn't force the caller to store k into memory.
 */
pub fn hash_bytes_uint32(k: uint32) -> uint32 {
    let mut a: uint32;
    let mut b: uint32;
    let mut c: uint32;

    a = 0x9e3779b9_u32
        .wrapping_add(core::mem::size_of::<uint32>() as uint32)
        .wrapping_add(3923095);
    b = a;
    c = a;
    a = a.wrapping_add(k);

    r#final!(a, b, c);

    /* report the result */
    c
}

/*
 * hash_bytes_uint32_extended() -- hash 32-bit value to 64-bit value, with seed
 *
 * Like hash_bytes_uint32, this is a convenience function.
 */
pub fn hash_bytes_uint32_extended(k: uint32, seed: uint64) -> uint64 {
    let mut a: uint32;
    let mut b: uint32;
    let mut c: uint32;

    a = 0x9e3779b9_u32
        .wrapping_add(core::mem::size_of::<uint32>() as uint32)
        .wrapping_add(3923095);
    b = a;
    c = a;

    if seed != 0 {
        a = a.wrapping_add((seed >> 32) as uint32);
        b = b.wrapping_add(seed as uint32);
        mix!(a, b, c);
    }

    a = a.wrapping_add(k);

    r#final!(a, b, c);

    /* report the result */
    ((b as uint64) << 32) | (c as uint64)
}

/* ----------------------------------------------------------------
 * The following items come from common/hashfn.h.  In C they are
 * `static inline` functions (and `#define oid_hash uint32_hash`);
 * here they become `pub` Rust functions.
 * ----------------------------------------------------------------
 */

/*
 * hash_any() / hash_any_extended() / hash_uint32() / hash_uint32_extended()
 *
 * In C these live behind `#ifndef FRONTEND` and return Datum via
 * UInt32GetDatum / UInt64GetDatum.  We translate the backend (non-FRONTEND)
 * variants.  They deref the key pointer, so they are `unsafe`.
 */
#[inline]
pub unsafe fn hash_any(k: *const core::ffi::c_uchar, keylen: c_int) -> Datum {
    UInt32GetDatum(hash_bytes(k, keylen))
}

#[inline]
pub unsafe fn hash_any_extended(
    k: *const core::ffi::c_uchar,
    keylen: c_int,
    seed: uint64,
) -> Datum {
    UInt64GetDatum(hash_bytes_extended(k, keylen, seed))
}

#[inline]
pub fn hash_uint32(k: uint32) -> Datum {
    UInt32GetDatum(hash_bytes_uint32(k))
}

#[inline]
pub fn hash_uint32_extended(k: uint32, seed: uint64) -> Datum {
    UInt64GetDatum(hash_bytes_uint32_extended(k, seed))
}

/*
 * Combine two 32-bit hash values, resulting in another hash value, with
 * decent bit mixing.
 *
 * Similar to boost's hash_combine().
 */
#[inline]
pub fn hash_combine(mut a: uint32, b: uint32) -> uint32 {
    a ^= b
        .wrapping_add(0x9e3779b9)
        .wrapping_add(a << 6)
        .wrapping_add(a >> 2);
    a
}

/*
 * Combine two 64-bit hash values, resulting in another hash value, using the
 * same kind of technique as hash_combine().  Testing shows that this also
 * produces good bit mixing.
 */
#[inline]
pub fn hash_combine64(mut a: uint64, b: uint64) -> uint64 {
    /* 0x49a0f4dd15e5a8e3 is 64bit random data */
    a ^= b
        .wrapping_add(0x49a0f4dd15e5a8e3_u64)
        .wrapping_add(a << 54)
        .wrapping_add(a >> 7);
    a
}

/*
 * Simple inline murmur hash implementation hashing a 32 bit integer, for
 * performance.
 */
#[inline]
pub fn murmurhash32(data: uint32) -> uint32 {
    let mut h: uint32 = data;

    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;
    h
}

/* 64-bit variant */
#[inline]
pub fn murmurhash64(data: uint64) -> uint64 {
    let mut h: uint64 = data;

    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd_u64);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53_u64);
    h ^= h >> 33;

    h
}

/* ----------------------------------------------------------------
 * Hash functions for use in dynahash.c hashtables (from hashfn.c).
 * ----------------------------------------------------------------
 */

/*
 * string_hash: hash function for keys that are NUL-terminated strings.
 *
 * NOTE: this is the default hash function if none is specified.
 */
pub unsafe fn string_hash(key: *const c_void, keysize: Size) -> uint32 {
    /*
     * If the string exceeds keysize-1 bytes, we want to hash only that many,
     * because when it is copied into the hash table it will be truncated at
     * that length.
     */
    let mut s_len: Size = strlen_local(key as *const c_char);

    s_len = Min(s_len, keysize - 1);
    hash_bytes(key as *const core::ffi::c_uchar, s_len as c_int)
}

/*
 * strlen() over a NUL-terminated C string.
 *
 * TODO(pg-port): the prelude does not currently re-export libc `strlen`; this
 * private helper reproduces it so the module stays independently compilable.
 */
#[inline]
unsafe fn strlen_local(s: *const c_char) -> Size {
    let mut n: Size = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * tag_hash: hash function for fixed-size tag values
 */
pub unsafe fn tag_hash(key: *const c_void, keysize: Size) -> uint32 {
    hash_bytes(key as *const core::ffi::c_uchar, keysize as c_int)
}

/*
 * uint32_hash: hash function for keys that are uint32 or int32
 *
 * (tag_hash works for this case too, but is slower)
 */
pub unsafe fn uint32_hash(key: *const c_void, keysize: Size) -> uint32 {
    Assert!(keysize == core::mem::size_of::<uint32>());
    hash_bytes_uint32(*(key as *const uint32))
}

/*
 * #define oid_hash uint32_hash	/* Remove me eventually */
 */
pub use uint32_hash as oid_hash;
