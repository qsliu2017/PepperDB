//! Translated from PostgreSQL src/include/common/hashfn_unstable.h
//! fasthash building blocks. NOT stable across versions/platforms - never use in
//! indexes or on-disk structures (see hashfn.rs for stable hashes). Inline funcs
//! translated in full; little-endian only (the WORDS_BIGENDIAN path is dropped).

/// Incremental fasthash state.
#[derive(Clone, Copy)]
pub struct FasthashState {
    /// Staging area for chunks of input.
    pub accum: u64,
    pub hash: u64,
}

pub const FH_SIZEOF_ACCUM: usize = core::mem::size_of::<u64>();

/// Initialize the hash state. `seed` can be zero.
pub fn fasthash_init(seed: u64) -> FasthashState {
    FasthashState {
        accum: 0,
        hash: seed ^ 0x880355f21e6d1965,
    }
}

/// Both the finalizer and part of the combining step.
pub const fn fasthash_mix(mut h: u64, tweak: u64) -> u64 {
    h ^= (h >> 23).wrapping_add(tweak);
    h = h.wrapping_mul(0x2127599bf4325c37);
    h ^= h >> 47;
    h
}

/// Combine the current `accum` chunk into the hash.
pub fn fasthash_combine(hs: &mut FasthashState) {
    hs.hash ^= fasthash_mix(hs.accum, 0);
    hs.hash = hs.hash.wrapping_mul(0x880355f21e6d1965);
}

/// Accumulate up to 8 bytes of input and combine them (little-endian load).
pub fn fasthash_accum(hs: &mut FasthashState, k: &[u8]) {
    let len = k.len();
    debug_assert!(len <= FH_SIZEOF_ACCUM);
    hs.accum = 0;

    match len {
        8 => hs.accum = u64::from_le_bytes(k[..8].try_into().unwrap()),
        7 => {
            hs.accum |= u64::from(k[6]) << 48;
            hs.accum |= u64::from(k[5]) << 40;
            hs.accum |= u64::from(k[4]) << 32;
            hs.accum |= u64::from(u32::from_le_bytes(k[..4].try_into().unwrap()));
        }
        6 => {
            hs.accum |= u64::from(k[5]) << 40;
            hs.accum |= u64::from(k[4]) << 32;
            hs.accum |= u64::from(u32::from_le_bytes(k[..4].try_into().unwrap()));
        }
        5 => {
            hs.accum |= u64::from(k[4]) << 32;
            hs.accum |= u64::from(u32::from_le_bytes(k[..4].try_into().unwrap()));
        }
        4 => hs.accum |= u64::from(u32::from_le_bytes(k[..4].try_into().unwrap())),
        3 => {
            hs.accum |= u64::from(k[2]) << 16;
            hs.accum |= u64::from(k[1]) << 8;
            hs.accum |= u64::from(k[0]);
        }
        2 => {
            hs.accum |= u64::from(k[1]) << 8;
            hs.accum |= u64::from(k[0]);
        }
        1 => hs.accum |= u64::from(k[0]),
        0 => return,
        _ => unreachable!(),
    }

    fasthash_combine(hs);
}

/// Set high bit in the lowest byte where the input byte is zero.
pub const fn haszero64(v: u64) -> u64 {
    (v.wrapping_sub(0x0101010101010101)) & !v & 0x8080808080808080
}

/// Workhorse for hashing a NUL-terminated string, byte chunk at a time.
/// Returns the string length (bytes consumed before the NUL).
pub fn fasthash_accum_cstring_unaligned(hs: &mut FasthashState, str: &[u8]) -> usize {
    let mut pos = 0;
    while pos < str.len() && str[pos] != 0 {
        let mut chunk_len = 0;
        while chunk_len < FH_SIZEOF_ACCUM
            && pos + chunk_len < str.len()
            && str[pos + chunk_len] != 0
        {
            chunk_len += 1;
        }
        fasthash_accum(hs, &str[pos..pos + chunk_len]);
        pos += chunk_len;
    }
    pos
}

/// Word-at-a-time variant; equivalent to the unaligned one over a byte slice.
pub fn fasthash_accum_cstring_aligned(hs: &mut FasthashState, str: &[u8]) -> usize {
    let mut pos = 0;
    while pos + FH_SIZEOF_ACCUM <= str.len() {
        let chunk = u64::from_le_bytes(str[pos..pos + FH_SIZEOF_ACCUM].try_into().unwrap());
        if haszero64(chunk) != 0 {
            break;
        }
        hs.accum = chunk;
        fasthash_combine(hs);
        pos += FH_SIZEOF_ACCUM;
    }
    pos + fasthash_accum_cstring_unaligned(hs, &str[pos..])
}

/// Mix `str` into the hash state and return its length.
pub fn fasthash_accum_cstring(hs: &mut FasthashState, str: &[u8]) -> usize {
    fasthash_accum_cstring_aligned(hs, str)
}

/// The finalizer. `tweak` is the input length for unknown-length inputs, else 0.
pub const fn fasthash_final64(hs: &FasthashState, tweak: u64) -> u64 {
    fasthash_mix(hs.hash, tweak)
}

/// Reduce a 64-bit hash to a 32-bit hash with extra mixing.
pub const fn fasthash_reduce32(h: u64) -> u32 {
    (h.wrapping_sub(h >> 32)) as u32
}

/// Finalize and reduce to 32 bits.
pub const fn fasthash_final32(hs: &FasthashState, tweak: u64) -> u32 {
    fasthash_reduce32(fasthash_final64(hs, tweak))
}

/// The original fasthash64, via the incremental interface. `seed` can be zero.
pub fn fasthash64(k: &[u8], seed: u64) -> u64 {
    let mut hs = fasthash_init(0);
    let len = k.len();
    hs.hash = seed ^ (len as u64).wrapping_mul(0x880355f21e6d1965);

    let mut pos = 0;
    while len - pos >= FH_SIZEOF_ACCUM {
        fasthash_accum(&mut hs, &k[pos..pos + FH_SIZEOF_ACCUM]);
        pos += FH_SIZEOF_ACCUM;
    }
    fasthash_accum(&mut hs, &k[pos..]);
    fasthash_final64(&hs, 0)
}

/// Like fasthash64 but returns a 32-bit hashcode.
pub fn fasthash32(k: &[u8], seed: u64) -> u32 {
    fasthash_reduce32(fasthash64(k, seed))
}

/// Hash a NUL-terminated string (the NUL terminator, if present, ends it).
pub fn hash_string(s: &[u8]) -> u32 {
    let mut hs = fasthash_init(0);
    let s_len = fasthash_accum_cstring(&mut hs, s);
    fasthash_final32(&hs, s_len as u64)
}
