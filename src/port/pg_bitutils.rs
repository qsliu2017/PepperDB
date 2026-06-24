//! Translated from PostgreSQL src/include/port/pg_bitutils.h

// Bit-wise utility functions. PG wraps clz/ctz/popcount builtins; Rust core
// has them as integer intrinsics, so the C byte lookup tables are unneeded.

/// Position of the most significant set bit, from the LSB. word must not be 0.
pub fn pg_leftmost_one_pos32(word: u32) -> i32 {
    debug_assert!(word != 0);
    (31 - word.leading_zeros()) as i32
}

/// As above, for a 64-bit word.
pub fn pg_leftmost_one_pos64(word: u64) -> i32 {
    debug_assert!(word != 0);
    (63 - word.leading_zeros()) as i32
}

/// Position of the least significant set bit, from the LSB. word must not be 0.
pub fn pg_rightmost_one_pos32(word: u32) -> i32 {
    debug_assert!(word != 0);
    word.trailing_zeros() as i32
}

/// As above, for a 64-bit word.
pub fn pg_rightmost_one_pos64(word: u64) -> i32 {
    debug_assert!(word != 0);
    word.trailing_zeros() as i32
}

/// Next higher power of 2 >= num (num itself if already a power of 2).
pub fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num > 0 && num <= u32::MAX / 2 + 1);
    if num & (num - 1) == 0 {
        return num;
    }
    1u32 << (pg_leftmost_one_pos32(num) + 1)
}

/// Next higher power of 2 >= num (num itself if already a power of 2).
pub fn pg_nextpower2_64(num: u64) -> u64 {
    debug_assert!(num > 0 && num <= u64::MAX / 2 + 1);
    if num & (num - 1) == 0 {
        return num;
    }
    1u64 << (pg_leftmost_one_pos64(num) + 1)
}

/// Next lower power of 2 <= num. num must not be 0.
pub fn pg_prevpower2_32(num: u32) -> u32 {
    1u32 << pg_leftmost_one_pos32(num)
}

/// Next lower power of 2 <= num. num must not be 0.
pub fn pg_prevpower2_64(num: u64) -> u64 {
    1u64 << pg_leftmost_one_pos64(num)
}

/// ceil(log2(num)).
pub fn pg_ceil_log2_32(num: u32) -> u32 {
    if num < 2 {
        0
    } else {
        (pg_leftmost_one_pos32(num - 1) + 1) as u32
    }
}

/// ceil(log2(num)).
pub fn pg_ceil_log2_64(num: u64) -> u64 {
    if num < 2 {
        0
    } else {
        (pg_leftmost_one_pos64(num - 1) + 1) as u64
    }
}

/// Number of 1-bits in word.
pub fn pg_popcount32(word: u32) -> i32 {
    word.count_ones() as i32
}

/// Number of 1-bits in word.
pub fn pg_popcount64(word: u64) -> i32 {
    word.count_ones() as i32
}

/// Number of 1-bits in buf.
pub fn pg_popcount(buf: &[u8]) -> u64 {
    buf.iter().map(|b| b.count_ones() as u64).sum()
}

/// Number of 1-bits in buf after applying mask to each byte.
pub fn pg_popcount_masked(buf: &[u8], mask: u8) -> u64 {
    buf.iter().map(|b| (b & mask).count_ones() as u64).sum()
}

/// Rotate the bits of word to the right by n bits.
pub const fn pg_rotate_right32(word: u32, n: u32) -> u32 {
    word.rotate_right(n)
}

/// Rotate the bits of word to the left by n bits.
pub const fn pg_rotate_left32(word: u32, n: u32) -> u32 {
    word.rotate_left(n)
}

// SIZEOF_SIZE_T == 8 on target -> size_t variants alias the 64-bit ones.
pub fn pg_leftmost_one_pos_size_t(word: u64) -> i32 {
    pg_leftmost_one_pos64(word)
}

pub fn pg_nextpower2_size_t(num: u64) -> u64 {
    pg_nextpower2_64(num)
}

pub fn pg_prevpower2_size_t(num: u64) -> u64 {
    pg_prevpower2_64(num)
}
