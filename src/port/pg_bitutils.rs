//! Translation of postgres/src/include/port/pg_bitutils.h
//!                + postgres/src/port/pg_bitutils.c
//!
//! Miscellaneous functions for bit-wise operations.
//!
//! The original .c file uses runtime CPU dispatch (cpuid / inline asm,
//! AVX-512 / POPCNT, AArch64 Neon/SVE) to choose an optimized popcount
//! implementation.  We do NOT translate any of the intrinsics or runtime
//! dispatch paths here; instead we translate the *portable* fallback that
//! PostgreSQL compiles when no special instructions are available
//! (the `!defined(TRY_POPCNT_X86_64) && !defined(POPCNT_AARCH64)` branch).
//!
//! Where the C header relies on compiler builtins (`__builtin_clz`,
//! `__builtin_ctz`, `__builtin_popcount`) we use Rust's equivalent intrinsic
//! methods (`u32::leading_zeros`, `trailing_zeros`, `count_ones`, ...), which
//! lower to the same hardware instructions.  The byte lookup tables are copied
//! verbatim from the .c so that callers wanting an explicit table-driven path
//! still have them available.

use crate::prelude::*;
use core::ffi::{c_char, c_int};

// TODO(pg-port): runtime SIMD popcount dispatch (cpuid/POPCNT/AVX-512/Neon/SVE).
// PostgreSQL selects between hand-rolled, hardware-POPCNT, AVX-512 and AArch64
// Neon/SVE implementations at run time via function pointers.  We translate only
// the portable, always-correct fallback and rely on Rust intrinsics for speed.

/*
 * Array giving the position of the left-most set bit for each possible
 * byte value.  We count the right-most position as the 0th bit, and the
 * left-most the 7th bit.  The 0th entry of the array should not be used.
 *
 * Note: this is not used by the functions in pg_bitutils.h when
 * HAVE__BUILTIN_CLZ is defined, but we provide it anyway, so that
 * extensions possibly compiled with a different compiler can use it.
 */
pub const pg_leftmost_one_pos: [uint8; 256] = [
    0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
    4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
    5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
    5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
];

/*
 * Array giving the position of the right-most set bit for each possible
 * byte value.  We count the right-most position as the 0th bit, and the
 * left-most the 7th bit.  The 0th entry of the array should not be used.
 *
 * Note: this is not used by the functions in pg_bitutils.h when
 * HAVE__BUILTIN_CTZ is defined, but we provide it anyway, so that
 * extensions possibly compiled with a different compiler can use it.
 */
pub const pg_rightmost_one_pos: [uint8; 256] = [
    0, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
    4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
];

/*
 * Array giving the number of 1-bits in each possible byte value.
 *
 * Note: we export this for use by functions in which explicit use
 * of the popcount functions seems unlikely to be a win.
 */
pub const pg_number_of_ones: [uint8; 256] = [
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
];

/*
 * pg_leftmost_one_pos32
 *		Returns the position of the most significant set bit in "word",
 *		measured from the least significant bit.  word must not be 0.
 */
#[inline]
pub fn pg_leftmost_one_pos32(word: uint32) -> c_int {
    // HAVE__BUILTIN_CLZ path: 31 - __builtin_clz(word).
    Assert!(word != 0);

    (31 - word.leading_zeros()) as c_int
}

/*
 * pg_leftmost_one_pos64
 *		As above, but for a 64-bit word.
 */
#[inline]
pub fn pg_leftmost_one_pos64(word: uint64) -> c_int {
    // HAVE__BUILTIN_CLZ path: 63 - __builtin_clzl(word).
    Assert!(word != 0);

    (63 - word.leading_zeros()) as c_int
}

/*
 * pg_rightmost_one_pos32
 *		Returns the position of the least significant set bit in "word",
 *		measured from the least significant bit.  word must not be 0.
 */
#[inline]
pub fn pg_rightmost_one_pos32(word: uint32) -> c_int {
    // HAVE__BUILTIN_CTZ path: __builtin_ctz(word).
    Assert!(word != 0);

    word.trailing_zeros() as c_int
}

/*
 * pg_rightmost_one_pos64
 *		As above, but for a 64-bit word.
 */
#[inline]
pub fn pg_rightmost_one_pos64(word: uint64) -> c_int {
    // HAVE__BUILTIN_CTZ path: __builtin_ctzl(word).
    Assert!(word != 0);

    word.trailing_zeros() as c_int
}

/*
 * pg_nextpower2_32
 *		Returns the next higher power of 2 above 'num', or 'num' if it's
 *		already a power of 2.
 *
 * 'num' mustn't be 0 or be above PG_UINT32_MAX / 2 + 1.
 */
#[inline]
pub fn pg_nextpower2_32(num: uint32) -> uint32 {
    Assert!(num > 0 && num <= PG_UINT32_MAX / 2 + 1);

    /*
     * A power 2 number has only 1 bit set.  Subtracting 1 from such a number
     * will turn on all previous bits resulting in no common bits being set
     * between num and num-1.
     */
    if (num & (num - 1)) == 0 {
        return num; /* already power 2 */
    }

    (1 as uint32) << (pg_leftmost_one_pos32(num) + 1)
}

/*
 * pg_nextpower2_64
 *		Returns the next higher power of 2 above 'num', or 'num' if it's
 *		already a power of 2.
 *
 * 'num' mustn't be 0 or be above PG_UINT64_MAX / 2  + 1.
 */
#[inline]
pub fn pg_nextpower2_64(num: uint64) -> uint64 {
    Assert!(num > 0 && num <= PG_UINT64_MAX / 2 + 1);

    /*
     * A power 2 number has only 1 bit set.  Subtracting 1 from such a number
     * will turn on all previous bits resulting in no common bits being set
     * between num and num-1.
     */
    if (num & (num - 1)) == 0 {
        return num; /* already power 2 */
    }

    (1 as uint64) << (pg_leftmost_one_pos64(num) + 1)
}

/*
 * pg_prevpower2_32
 *		Returns the next lower power of 2 below 'num', or 'num' if it's
 *		already a power of 2.
 *
 * 'num' mustn't be 0.
 */
#[inline]
pub fn pg_prevpower2_32(num: uint32) -> uint32 {
    (1 as uint32) << pg_leftmost_one_pos32(num)
}

/*
 * pg_prevpower2_64
 *		Returns the next lower power of 2 below 'num', or 'num' if it's
 *		already a power of 2.
 *
 * 'num' mustn't be 0.
 */
#[inline]
pub fn pg_prevpower2_64(num: uint64) -> uint64 {
    (1 as uint64) << pg_leftmost_one_pos64(num)
}

/*
 * pg_ceil_log2_32
 *		Returns equivalent of ceil(log2(num))
 */
#[inline]
pub fn pg_ceil_log2_32(num: uint32) -> uint32 {
    if num < 2 {
        0
    } else {
        (pg_leftmost_one_pos32(num - 1) + 1) as uint32
    }
}

/*
 * pg_ceil_log2_64
 *		Returns equivalent of ceil(log2(num))
 */
#[inline]
pub fn pg_ceil_log2_64(num: uint64) -> uint64 {
    if num < 2 {
        0
    } else {
        (pg_leftmost_one_pos64(num - 1) + 1) as uint64
    }
}

/*
 * pg_popcount32_slow
 *		Return the number of 1 bits set in word
 *
 * (HAVE__BUILTIN_POPCOUNT path -> Rust's count_ones intrinsic.)
 */
#[inline]
fn pg_popcount32_slow(word: uint32) -> c_int {
    word.count_ones() as c_int
}

/*
 * pg_popcount64_slow
 *		Return the number of 1 bits set in word
 *
 * (HAVE__BUILTIN_POPCOUNT path -> Rust's count_ones intrinsic.)
 */
#[inline]
fn pg_popcount64_slow(word: uint64) -> c_int {
    word.count_ones() as c_int
}

/*
 * pg_popcount_slow
 *		Returns the number of 1-bits in buf
 *
 * This is the SIZEOF_VOID_P >= 8 (64-bit) portable path: process the buffer in
 * aligned 64-bit chunks, then mop up the trailing bytes via pg_number_of_ones.
 */
unsafe fn pg_popcount_slow(mut buf: *const c_char, mut bytes: c_int) -> uint64 {
    let mut popcnt: uint64 = 0;

    /* Process in 64-bit chunks if the buffer is aligned. */
    if buf as usize == TYPEALIGN(8, buf as usize) {
        let mut words = buf as *const uint64;

        while bytes >= 8 {
            popcnt += pg_popcount64_slow(*words) as uint64;
            words = words.add(1);
            bytes -= 8;
        }

        buf = words as *const c_char;
    }

    /* Process any remaining bytes */
    while bytes != 0 {
        bytes -= 1;
        popcnt += pg_number_of_ones[*buf as u8 as usize] as uint64;
        buf = buf.add(1);
    }

    popcnt
}

/*
 * pg_popcount_masked_slow
 *		Returns the number of 1-bits in buf after applying the mask to each byte
 */
unsafe fn pg_popcount_masked_slow(mut buf: *const c_char, mut bytes: c_int, mask: bits8) -> uint64 {
    let mut popcnt: uint64 = 0;

    /* Process in 64-bit chunks if the buffer is aligned */
    let maskv: uint64 = !UINT64CONST(0) / 0xFF * mask as uint64;

    if buf as usize == TYPEALIGN(8, buf as usize) {
        let mut words = buf as *const uint64;

        while bytes >= 8 {
            popcnt += pg_popcount64_slow(*words & maskv) as uint64;
            words = words.add(1);
            bytes -= 8;
        }

        buf = words as *const c_char;
    }

    /* Process any remaining bytes */
    while bytes != 0 {
        bytes -= 1;
        popcnt += pg_number_of_ones[(*buf as u8 & mask) as usize] as uint64;
        buf = buf.add(1);
    }

    popcnt
}

/*
 * When special CPU instructions are not available, there's no point in using
 * function pointers to vary the implementation between the fast and slow
 * method.  We instead just make these actual external functions.  The compiler
 * should be able to inline the slow versions here.
 */
pub fn pg_popcount32(word: uint32) -> c_int {
    pg_popcount32_slow(word)
}

pub fn pg_popcount64(word: uint64) -> c_int {
    pg_popcount64_slow(word)
}

/*
 * pg_popcount_optimized
 *		Returns the number of 1-bits in buf
 */
pub unsafe fn pg_popcount_optimized(buf: *const c_char, bytes: c_int) -> uint64 {
    pg_popcount_slow(buf, bytes)
}

/*
 * pg_popcount_masked_optimized
 *		Returns the number of 1-bits in buf after applying the mask to each byte
 */
pub unsafe fn pg_popcount_masked_optimized(buf: *const c_char, bytes: c_int, mask: bits8) -> uint64 {
    pg_popcount_masked_slow(buf, bytes, mask)
}

/*
 * Returns the number of 1-bits in buf.
 *
 * If there aren't many bytes to process, the function call overhead of the
 * optimized versions isn't worth taking, so we inline a loop that consults
 * pg_number_of_ones in that case.  If there are many bytes to process, we
 * accept the function call overhead because the optimized versions are likely
 * to be faster.
 */
#[inline]
pub unsafe fn pg_popcount(mut buf: *const c_char, mut bytes: c_int) -> uint64 {
    /*
     * We set the threshold to the point at which we'll first use special
     * instructions in the optimized version.  (SIZEOF_VOID_P >= 8 path.)
     */
    let threshold: c_int = 8;

    if bytes < threshold {
        let mut popcnt: uint64 = 0;

        while bytes != 0 {
            bytes -= 1;
            popcnt += pg_number_of_ones[*buf as u8 as usize] as uint64;
            buf = buf.add(1);
        }
        return popcnt;
    }

    pg_popcount_optimized(buf, bytes)
}

/*
 * Returns the number of 1-bits in buf after applying the mask to each byte.
 *
 * Similar to pg_popcount(), we only take on the function pointer overhead when
 * it's likely to be faster.
 */
#[inline]
pub unsafe fn pg_popcount_masked(mut buf: *const c_char, mut bytes: c_int, mask: bits8) -> uint64 {
    /*
     * We set the threshold to the point at which we'll first use special
     * instructions in the optimized version.  (SIZEOF_VOID_P >= 8 path.)
     */
    let threshold: c_int = 8;

    if bytes < threshold {
        let mut popcnt: uint64 = 0;

        while bytes != 0 {
            bytes -= 1;
            popcnt += pg_number_of_ones[(*buf as u8 & mask) as usize] as uint64;
            buf = buf.add(1);
        }
        return popcnt;
    }

    pg_popcount_masked_optimized(buf, bytes, mask)
}

/*
 * Rotate the bits of "word" to the right/left by n bits.
 */
#[inline]
pub fn pg_rotate_right32(word: uint32, n: c_int) -> uint32 {
    (word >> n) | (word << (32 - n))
}

#[inline]
pub fn pg_rotate_left32(word: uint32, n: c_int) -> uint32 {
    (word << n) | (word >> (32 - n))
}

/* size_t variants of the above, as required */

// SIZEOF_SIZE_T == 8 on our 64-bit target, so the size_t variants alias the
// 64-bit functions (the #else branch of the C header).
#[inline]
pub fn pg_leftmost_one_pos_size_t(word: uint64) -> c_int {
    pg_leftmost_one_pos64(word)
}

#[inline]
pub fn pg_nextpower2_size_t(num: uint64) -> uint64 {
    pg_nextpower2_64(num)
}

#[inline]
pub fn pg_prevpower2_size_t(num: uint64) -> uint64 {
    pg_prevpower2_64(num)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_tables_consistent() {
        // Spot-check a few table entries against the intrinsic-based helpers.
        assert_eq!(pg_number_of_ones[0xFF], 8);
        assert_eq!(pg_number_of_ones[0x00], 0);
        assert_eq!(pg_leftmost_one_pos[0x80], 7);
        assert_eq!(pg_leftmost_one_pos[0x01], 0);
        assert_eq!(pg_rightmost_one_pos[0x80], 7);
        assert_eq!(pg_rightmost_one_pos[0x06], 1);
    }

    #[test]
    fn bit_positions() {
        assert_eq!(pg_leftmost_one_pos32(1), 0);
        assert_eq!(pg_leftmost_one_pos32(0x8000_0000), 31);
        assert_eq!(pg_leftmost_one_pos64(0x8000_0000_0000_0000), 63);
        assert_eq!(pg_rightmost_one_pos32(0x8000_0000), 31);
        assert_eq!(pg_rightmost_one_pos32(0b1100), 2);
        assert_eq!(pg_rightmost_one_pos64(0b1000), 3);
    }

    #[test]
    fn popcount_scalar() {
        assert_eq!(pg_popcount32(0xFFFF_FFFF), 32);
        assert_eq!(pg_popcount64(0xFFFF_FFFF_FFFF_FFFF), 64);
        assert_eq!(pg_popcount32(0), 0);
    }

    #[test]
    fn powers_of_two() {
        assert_eq!(pg_nextpower2_32(5), 8);
        assert_eq!(pg_nextpower2_32(8), 8);
        assert_eq!(pg_prevpower2_32(5), 4);
        assert_eq!(pg_ceil_log2_32(8), 3);
        assert_eq!(pg_ceil_log2_32(9), 4);
        assert_eq!(pg_ceil_log2_64(1), 0);
    }

    #[test]
    fn popcount_buffer() {
        let data: [u8; 16] = [0xFF; 16];
        unsafe {
            assert_eq!(pg_popcount(data.as_ptr() as *const c_char, 16), 128);
            // short path (< 8 bytes)
            assert_eq!(pg_popcount(data.as_ptr() as *const c_char, 3), 24);
            // masked: only low nibble of each byte
            assert_eq!(
                pg_popcount_masked(data.as_ptr() as *const c_char, 16, 0x0F),
                64
            );
        }
    }
}
