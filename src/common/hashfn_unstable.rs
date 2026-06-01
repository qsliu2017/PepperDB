//! common/hashfn_unstable.h - building blocks for fast inlineable (unstable) hash functions
//!
//! fasthash is a modification of code taken from
//! https://code.google.com/archive/p/fast-hash/source/default/source
//! under the terms of the MIT license. The functions here are NOT guaranteed
//! to be stable between versions and must not be used in indexes or other
//! on-disk structures. See hashfn.h if stability is needed.

use crate::c::{uint32, uint64, Size};

/// staging area for chunks of input plus running hash
#[repr(C)]
#[derive(Clone, Copy)]
pub struct fasthash_state {
    /// staging area for chunks of input
    pub accum: uint64,
    pub hash: uint64,
}

/// #define FH_SIZEOF_ACCUM sizeof(uint64)
pub const FH_SIZEOF_ACCUM: Size = core::mem::size_of::<uint64>();

/// Initialize the hash state. 'seed' can be zero.
#[inline]
pub unsafe fn fasthash_init(hs: *mut fasthash_state, seed: uint64) {
    core::ptr::write_bytes(hs as *mut u8, 0, core::mem::size_of::<fasthash_state>());
    (*hs).hash = seed ^ 0x880355f21e6d1965;
}

/// both the finalizer and part of the combining step
#[inline]
pub unsafe fn fasthash_mix(mut h: uint64, tweak: uint64) -> uint64 {
    h ^= (h >> 23).wrapping_add(tweak);
    h = h.wrapping_mul(0x2127599bf4325c37);
    h ^= h >> 47;
    h
}

/// combine one chunk of input into the hash
#[inline]
pub unsafe fn fasthash_combine(hs: *mut fasthash_state) {
    (*hs).hash ^= fasthash_mix((*hs).accum, 0);
    (*hs).hash = (*hs).hash.wrapping_mul(0x880355f21e6d1965);
}

/// accumulate up to 8 bytes of input and combine it into the hash
#[inline]
pub unsafe fn fasthash_accum(hs: *mut fasthash_state, k: *const core::ffi::c_char, len: Size) {
    // Assert(len <= FH_SIZEOF_ACCUM)
    debug_assert!(len <= FH_SIZEOF_ACCUM);
    (*hs).accum = 0;

    // For consistency, bytewise loads must match the platform's endianness.
    #[cfg(target_endian = "big")]
    {
        match len {
            8 => {
                core::ptr::copy_nonoverlapping(k as *const u8, &mut (*hs).accum as *mut uint64 as *mut u8, 8);
            }
            7 => {
                (*hs).accum |= (*k.add(6) as uint64) << 8;
                (*hs).accum |= (*k.add(5) as uint64) << 16;
                (*hs).accum |= (*k.add(4) as uint64) << 24;
                let mut lower_four: uint32 = 0;
                core::ptr::copy_nonoverlapping(k as *const u8, &mut lower_four as *mut uint32 as *mut u8, core::mem::size_of::<uint32>());
                (*hs).accum |= (lower_four as uint64) << 32;
            }
            6 => {
                (*hs).accum |= (*k.add(5) as uint64) << 16;
                (*hs).accum |= (*k.add(4) as uint64) << 24;
                let mut lower_four: uint32 = 0;
                core::ptr::copy_nonoverlapping(k as *const u8, &mut lower_four as *mut uint32 as *mut u8, core::mem::size_of::<uint32>());
                (*hs).accum |= (lower_four as uint64) << 32;
            }
            5 => {
                (*hs).accum |= (*k.add(4) as uint64) << 24;
                let mut lower_four: uint32 = 0;
                core::ptr::copy_nonoverlapping(k as *const u8, &mut lower_four as *mut uint32 as *mut u8, core::mem::size_of::<uint32>());
                (*hs).accum |= (lower_four as uint64) << 32;
            }
            4 => {
                let mut lower_four: uint32 = 0;
                core::ptr::copy_nonoverlapping(k as *const u8, &mut lower_four as *mut uint32 as *mut u8, core::mem::size_of::<uint32>());
                (*hs).accum |= (lower_four as uint64) << 32;
            }
            3 => {
                (*hs).accum |= (*k.add(2) as uint64) << 40;
                (*hs).accum |= (*k.add(1) as uint64) << 48;
                (*hs).accum |= (*k.add(0) as uint64) << 56;
            }
            2 => {
                (*hs).accum |= (*k.add(1) as uint64) << 48;
                (*hs).accum |= (*k.add(0) as uint64) << 56;
            }
            1 => {
                (*hs).accum |= (*k.add(0) as uint64) << 56;
            }
            0 => return,
            _ => {}
        }
    }
    #[cfg(not(target_endian = "big"))]
    {
        match len {
            8 => {
                core::ptr::copy_nonoverlapping(k as *const u8, &mut (*hs).accum as *mut uint64 as *mut u8, 8);
            }
            7 => {
                (*hs).accum |= (*k.add(6) as uint64) << 48;
                (*hs).accum |= (*k.add(5) as uint64) << 40;
                (*hs).accum |= (*k.add(4) as uint64) << 32;
                let mut lower_four: uint32 = 0;
                core::ptr::copy_nonoverlapping(k as *const u8, &mut lower_four as *mut uint32 as *mut u8, core::mem::size_of::<uint32>());
                (*hs).accum |= lower_four as uint64;
            }
            6 => {
                (*hs).accum |= (*k.add(5) as uint64) << 40;
                (*hs).accum |= (*k.add(4) as uint64) << 32;
                let mut lower_four: uint32 = 0;
                core::ptr::copy_nonoverlapping(k as *const u8, &mut lower_four as *mut uint32 as *mut u8, core::mem::size_of::<uint32>());
                (*hs).accum |= lower_four as uint64;
            }
            5 => {
                (*hs).accum |= (*k.add(4) as uint64) << 32;
                let mut lower_four: uint32 = 0;
                core::ptr::copy_nonoverlapping(k as *const u8, &mut lower_four as *mut uint32 as *mut u8, core::mem::size_of::<uint32>());
                (*hs).accum |= lower_four as uint64;
            }
            4 => {
                let mut lower_four: uint32 = 0;
                core::ptr::copy_nonoverlapping(k as *const u8, &mut lower_four as *mut uint32 as *mut u8, core::mem::size_of::<uint32>());
                (*hs).accum |= lower_four as uint64;
            }
            3 => {
                (*hs).accum |= (*k.add(2) as uint64) << 16;
                (*hs).accum |= (*k.add(1) as uint64) << 8;
                (*hs).accum |= *k.add(0) as uint64;
            }
            2 => {
                (*hs).accum |= (*k.add(1) as uint64) << 8;
                (*hs).accum |= *k.add(0) as uint64;
            }
            1 => {
                (*hs).accum |= *k.add(0) as uint64;
            }
            0 => return,
            _ => {}
        }
    }

    fasthash_combine(hs);
}

/// Set high bit in lowest byte where the input is zero, from:
/// https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
///
/// #define haszero64(v) (((v) - 0x01..01) & ~(v) & 0x80..80)
#[inline]
pub fn haszero64(v: uint64) -> uint64 {
    (v.wrapping_sub(0x0101010101010101)) & !v & 0x8080808080808080
}

/// all-purpose workhorse for fasthash_accum_cstring
#[inline]
pub unsafe fn fasthash_accum_cstring_unaligned(
    hs: *mut fasthash_state,
    mut str_: *const core::ffi::c_char,
) -> Size {
    let start: *const core::ffi::c_char = str_;

    while *str_ != 0 {
        let mut chunk_len: Size = 0;

        while chunk_len < FH_SIZEOF_ACCUM && *str_.add(chunk_len) != 0 {
            chunk_len += 1;
        }

        fasthash_accum(hs, str_, chunk_len);
        str_ = str_.add(chunk_len);
    }

    str_.offset_from(start) as Size
}

/// specialized workhorse for fasthash_accum_cstring
///
/// With an aligned pointer, we consume the string a word at a time. Loading the
/// word containing the NUL terminator cannot segfault since allocation
/// boundaries are suitably aligned. The C code annotates this with
/// pg_attribute_no_sanitize_address(); the Rust equivalent attribute is omitted.
#[inline]
pub unsafe fn fasthash_accum_cstring_aligned(
    hs: *mut fasthash_state,
    mut str_: *const core::ffi::c_char,
) -> Size {
    let start: *const core::ffi::c_char = str_;
    let remainder: Size;
    let zero_byte_low: uint64;

    // Assert(PointerIsAligned(start, uint64))
    debug_assert!((start as usize) % core::mem::align_of::<uint64>() == 0);

    // For every chunk of input, check for zero bytes before mixing into the
    // hash. The chunk with zeros must contain the NUL terminator.
    loop {
        let chunk: uint64 = *(str_ as *const uint64);

        let z = haszero64(chunk);
        if z != 0 {
            zero_byte_low = z;
            break;
        }

        (*hs).accum = chunk;
        fasthash_combine(hs);
        str_ = str_.add(FH_SIZEOF_ACCUM);
    }
    let _ = zero_byte_low;

    // mix in remaining bytes
    remainder = fasthash_accum_cstring_unaligned(hs, str_);
    str_ = str_.add(remainder);

    str_.offset_from(start) as Size
}

/// Mix 'str' into the hash state and return the length of the string.
#[inline]
pub unsafe fn fasthash_accum_cstring(
    hs: *mut fasthash_state,
    str_: *const core::ffi::c_char,
) -> Size {
    // #if SIZEOF_VOID_P >= 8
    #[cfg(target_pointer_width = "64")]
    {
        // USE_ASSERT_CHECKING path (len_check / hs_check) is debug-only in C and
        // omitted here; the aligned/unaligned results are asserted equal there.
        if (str_ as usize) % core::mem::align_of::<uint64>() == 0 {
            let len = fasthash_accum_cstring_aligned(hs, str_);
            return len;
        }
    }

    // It's not worth it to try to make the word-at-a-time optimization work on
    // 32-bit platforms.
    fasthash_accum_cstring_unaligned(hs, str_)
}

/// The finalizer.
///
/// 'tweak' is intended to be the input length when the caller doesn't know the
/// length ahead of time (such as for NUL-terminated strings), otherwise zero.
#[inline]
pub unsafe fn fasthash_final64(hs: *mut fasthash_state, tweak: uint64) -> uint64 {
    fasthash_mix((*hs).hash, tweak)
}

/// Reduce a 64-bit hash to a 32-bit hash.
///
/// This optional step provides a bit more additional mixing compared to just
/// taking the lower 32-bits.
#[inline]
pub fn fasthash_reduce32(h: uint64) -> uint32 {
    // Convert the 64-bit hashcode to Fermat residue, which retains information
    // from both the higher and lower parts of hashcode.
    h.wrapping_sub(h >> 32) as uint32
}

/// finalize and reduce
#[inline]
pub unsafe fn fasthash_final32(hs: *mut fasthash_state, tweak: uint64) -> uint32 {
    fasthash_reduce32(fasthash_final64(hs, tweak))
}

/// The original fasthash64 function, re-implemented using the incremental
/// interface. Returns a 64-bit hashcode. 'len' controls not only how many bytes
/// to hash, but also modifies the internal seed. 'seed' can be zero.
#[inline]
pub unsafe fn fasthash64(mut k: *const core::ffi::c_char, mut len: Size, seed: uint64) -> uint64 {
    let mut hs: fasthash_state = fasthash_state { accum: 0, hash: 0 };

    fasthash_init(&mut hs, 0);

    // re-initialize the seed according to input length
    hs.hash = seed ^ (len as uint64).wrapping_mul(0x880355f21e6d1965);

    while len >= FH_SIZEOF_ACCUM {
        fasthash_accum(&mut hs, k, FH_SIZEOF_ACCUM);
        k = k.add(FH_SIZEOF_ACCUM);
        len -= FH_SIZEOF_ACCUM;
    }

    fasthash_accum(&mut hs, k, len);
    fasthash_final64(&mut hs, 0)
}

/// like fasthash64, but returns a 32-bit hashcode
#[inline]
pub unsafe fn fasthash32(k: *const core::ffi::c_char, len: Size, seed: uint64) -> uint32 {
    fasthash_reduce32(fasthash64(k, len, seed))
}

/// Convenience function for hashing NUL-terminated strings
#[inline]
pub unsafe fn hash_string(s: *const core::ffi::c_char) -> uint32 {
    let mut hs: fasthash_state = fasthash_state { accum: 0, hash: 0 };

    fasthash_init(&mut hs, 0);

    // Combine string into the hash and save the length for tweaking the final mix.
    let s_len = fasthash_accum_cstring(&mut hs, s);

    fasthash_final32(&mut hs, s_len as uint64)
}
