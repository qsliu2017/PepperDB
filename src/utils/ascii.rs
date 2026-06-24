//! Translated from PostgreSQL src/include/utils/ascii.h

// port/simd.h is a tombstone (-> std/core); the SIMD/no-SIMD chunked C code in
// is_valid_ascii becomes a scalar byte loop here (the len%chunk Assert drops).

pub fn ascii_safe_strlcpy(dest: &mut [u8], src: &[u8], destsiz: usize) {
    unimplemented!()
}

/// Verify a chunk of bytes for valid ASCII.
/// Returns false if the input contains any zero byte or any high-bit byte.
#[inline]
pub fn is_valid_ascii(s: &[u8]) -> bool {
    s.iter().all(|&b| b != 0 && (b & 0x80) == 0)
}
