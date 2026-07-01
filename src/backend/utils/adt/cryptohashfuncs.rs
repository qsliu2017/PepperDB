//! Cryptographic hash functions. Translated from
//! src/backend/utils/adt/cryptohashfuncs.c, plus the MD5 core (common/md5.c ->
//! md5_common.c) and SHA-2 core (common/sha2.c) folded in as pure-Rust
//! implementations so no external crate is pulled in.
//!
//! Exposes the SQL builtins `md5(text)` / `md5(bytea)` (hex text results) and
//! `sha224`/`sha256`/`sha384`/`sha512` (raw-digest bytea results). The digest
//! primitives (`md5`, `sha256`, ...) are faithful RFC 1321 / FIPS 180-4
//! implementations verified against the standard test vectors in the unit
//! tests below.
//!
//! VARLENA I/O mirrors varlena.rs: arguments are read through the
//! `VARSIZE_ANY_EXHDR`/`VARDATA_ANY` accessors, and results are leaked 4-byte
//! header varlenas (text via `cstring_to_text`, bytea via [`make_bytea`]).

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "faithful port arithmetic: MD5/SHA process bytes as fixed-width \
              words with C-style wrapping and cast between byte/word widths (the \
              value-cast family is an allowed port-inherent lint per rules.md s11)"
)]
#![allow(
    clippy::many_single_char_names,
    reason = "MD5/SHA round working variables (a,b,c,d,e,f,g,h,...) mirror the \
              canonical RFC 1321 / FIPS 180-4 notation; renaming obscures the \
              algorithm"
)]

use crate::c::VARHDRSZ;
use crate::fmgr::FunctionCallInfoBaseData;
use crate::postgres::{Datum, DatumGetPointer, PointerGetDatum};
use crate::backend::utils::adt::varlena::cstring_to_text;
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};

// SHA-2 digest lengths (common/sha2.h).
const PG_SHA224_DIGEST_LENGTH: usize = 28;
const PG_SHA256_DIGEST_LENGTH: usize = 32;
const PG_SHA384_DIGEST_LENGTH: usize = 48;
const PG_SHA512_DIGEST_LENGTH: usize = 64;

// ---------------------------------------------------------------------------
// Varlena helpers (mirrors varlena.rs; kept private to this module).
// ---------------------------------------------------------------------------

/// Borrow the payload bytes of a non-toasted varlena argument.
///
/// SAFETY: `p` must point at a valid, non-external/non-compressed varlena that
/// outlives the returned slice.
unsafe fn varlena_bytes<'a>(p: *mut u8) -> &'a [u8] {
    let len = VARSIZE_ANY_EXHDR(p);
    core::slice::from_raw_parts(VARDATA_ANY(p), len)
}

/// Build a leaked 4-byte-header bytea from raw `src` bytes (bytea is a plain
/// varlena of raw bytes; same layout as text). TODO(memory-context): reclaim
/// via the per-call context when it lands, replacing the leak.
fn make_bytea(src: &[u8]) -> *mut u8 {
    let total = src.len() + VARHDRSZ as usize;
    let mut buf = vec![0u8; total].into_boxed_slice();
    let ptr = buf.as_mut_ptr();
    // SAFETY: `ptr` heads a freshly-allocated `total`-byte buffer; the header
    // write touches the first 4 bytes and VARDATA the following `src.len()`.
    unsafe {
        SET_VARSIZE(ptr, total as u32);
        if !src.is_empty() {
            core::ptr::copy_nonoverlapping(src.as_ptr(), VARDATA(ptr), src.len());
        }
    }
    Box::leak(buf).as_mut_ptr()
}

/// `PG_GETARG_TEXT_PP(n)` / `PG_GETARG_BYTEA_PP(n)`: the argument varlena ptr.
#[inline]
fn pg_getarg_varlena(fcinfo: &FunctionCallInfoBaseData, n: usize) -> *mut u8 {
    DatumGetPointer(fcinfo.args[n].value)
}

/// Lowercase-hex encode `bytes` into a `String` (C: `bytesToHex`).
fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(char::from(HEX[(b >> 4) as usize]));
        out.push(char::from(HEX[(b & 0x0f) as usize]));
    }
    out
}

// ===========================================================================
//   SQL builtins
// ===========================================================================

/// PG `md5_text`: MD5 of a text value, returned as a 32-char hex text.
pub fn md5_text(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: the arg is a valid non-toasted text varlena.
    let data = unsafe { varlena_bytes(p) };
    let hex = bytes_to_hex(&md5(data));
    PointerGetDatum(cstring_to_text(&hex).cast::<u8>())
}

/// PG `md5_bytea`: MD5 of a bytea value, returned as a 32-char hex text.
pub fn md5_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: the arg is a valid non-toasted bytea varlena.
    let data = unsafe { varlena_bytes(p) };
    let hex = bytes_to_hex(&md5(data));
    PointerGetDatum(cstring_to_text(&hex).cast::<u8>())
}

/// PG `sha224_bytea`: SHA-224 of a bytea, returned as raw digest bytea.
pub fn sha224_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: the arg is a valid non-toasted bytea varlena.
    let data = unsafe { varlena_bytes(p) };
    PointerGetDatum(make_bytea(&sha224(data)))
}

/// PG `sha256_bytea`: SHA-256 of a bytea, returned as raw digest bytea.
pub fn sha256_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: the arg is a valid non-toasted bytea varlena.
    let data = unsafe { varlena_bytes(p) };
    PointerGetDatum(make_bytea(&sha256(data)))
}

/// PG `sha384_bytea`: SHA-384 of a bytea, returned as raw digest bytea.
pub fn sha384_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: the arg is a valid non-toasted bytea varlena.
    let data = unsafe { varlena_bytes(p) };
    PointerGetDatum(make_bytea(&sha384(data)))
}

/// PG `sha512_bytea`: SHA-512 of a bytea, returned as raw digest bytea.
pub fn sha512_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let p = pg_getarg_varlena(fcinfo, 0);
    // SAFETY: the arg is a valid non-toasted bytea varlena.
    let data = unsafe { varlena_bytes(p) };
    PointerGetDatum(make_bytea(&sha512(data)))
}

// ===========================================================================
//   MD5 (RFC 1321)
// ===========================================================================

const MD5_S: [u32; 64] = [
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9,
    14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10, 15,
    21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
];

const MD5_K: [u32; 64] = [
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
    0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
    0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
    0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
    0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
];

/// RFC 1321 MD5. Returns the 16-byte digest.
fn md5(input: &[u8]) -> [u8; 16] {
    let mut a0: u32 = 0x67452301;
    let mut b0: u32 = 0xefcdab89;
    let mut c0: u32 = 0x98badcfe;
    let mut d0: u32 = 0x10325476;

    let bit_len = (input.len() as u64).wrapping_mul(8);
    let mut msg = input.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());

    for chunk in msg.chunks_exact(64) {
        let mut m = [0u32; 16];
        for (i, word) in m.iter_mut().enumerate() {
            *word = u32::from_le_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }

        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | (!b & d), i),
                16..=31 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let tmp = d;
            d = c;
            c = b;
            let sum = a
                .wrapping_add(f)
                .wrapping_add(MD5_K[i])
                .wrapping_add(m[g]);
            b = b.wrapping_add(sum.rotate_left(MD5_S[i]));
            a = tmp;
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }

    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&a0.to_le_bytes());
    out[4..8].copy_from_slice(&b0.to_le_bytes());
    out[8..12].copy_from_slice(&c0.to_le_bytes());
    out[12..16].copy_from_slice(&d0.to_le_bytes());
    out
}

// ===========================================================================
//   SHA-256 / SHA-224 (FIPS 180-4)
// ===========================================================================

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// SHA-256 core over the eight 32-bit state words. Processes 64-byte blocks.
fn sha256_core(mut h: [u32; 8], input: &[u8]) -> [u32; 8] {
    let bit_len = (input.len() as u64).wrapping_mul(8);
    let mut msg = input.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, word) in w.iter_mut().take(16).enumerate() {
            *word = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(SHA256_K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    h
}

fn sha256(input: &[u8]) -> [u8; PG_SHA256_DIGEST_LENGTH] {
    let h = sha256_core(
        [
            0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
            0x5be0cd19,
        ],
        input,
    );
    let mut out = [0u8; PG_SHA256_DIGEST_LENGTH];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

fn sha224(input: &[u8]) -> [u8; PG_SHA224_DIGEST_LENGTH] {
    let h = sha256_core(
        [
            0xc1059ed8, 0x367cd507, 0x3070dd17, 0xf70e5939, 0xffc00b31, 0x68581511, 0x64f98fa7,
            0xbefa4fa4,
        ],
        input,
    );
    let mut out = [0u8; PG_SHA224_DIGEST_LENGTH];
    for (i, word) in h.iter().take(7).enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

// ===========================================================================
//   SHA-512 / SHA-384 (FIPS 180-4)
// ===========================================================================

const SHA512_K: [u64; 80] = [
    0x428a2f98d728ae22, 0x7137449123ef65cd, 0xb5c0fbcfec4d3b2f, 0xe9b5dba58189dbbc,
    0x3956c25bf348b538, 0x59f111f1b605d019, 0x923f82a4af194f9b, 0xab1c5ed5da6d8118,
    0xd807aa98a3030242, 0x12835b0145706fbe, 0x243185be4ee4b28c, 0x550c7dc3d5ffb4e2,
    0x72be5d74f27b896f, 0x80deb1fe3b1696b1, 0x9bdc06a725c71235, 0xc19bf174cf692694,
    0xe49b69c19ef14ad2, 0xefbe4786384f25e3, 0x0fc19dc68b8cd5b5, 0x240ca1cc77ac9c65,
    0x2de92c6f592b0275, 0x4a7484aa6ea6e483, 0x5cb0a9dcbd41fbd4, 0x76f988da831153b5,
    0x983e5152ee66dfab, 0xa831c66d2db43210, 0xb00327c898fb213f, 0xbf597fc7beef0ee4,
    0xc6e00bf33da88fc2, 0xd5a79147930aa725, 0x06ca6351e003826f, 0x142929670a0e6e70,
    0x27b70a8546d22ffc, 0x2e1b21385c26c926, 0x4d2c6dfc5ac42aed, 0x53380d139d95b3df,
    0x650a73548baf63de, 0x766a0abb3c77b2a8, 0x81c2c92e47edaee6, 0x92722c851482353b,
    0xa2bfe8a14cf10364, 0xa81a664bbc423001, 0xc24b8b70d0f89791, 0xc76c51a30654be30,
    0xd192e819d6ef5218, 0xd69906245565a910, 0xf40e35855771202a, 0x106aa07032bbd1b8,
    0x19a4c116b8d2d0c8, 0x1e376c085141ab53, 0x2748774cdf8eeb99, 0x34b0bcb5e19b48a8,
    0x391c0cb3c5c95a63, 0x4ed8aa4ae3418acb, 0x5b9cca4f7763e373, 0x682e6ff3d6b2b8a3,
    0x748f82ee5defb2fc, 0x78a5636f43172f60, 0x84c87814a1f0ab72, 0x8cc702081a6439ec,
    0x90befffa23631e28, 0xa4506cebde82bde9, 0xbef9a3f7b2c67915, 0xc67178f2e372532b,
    0xca273eceea26619c, 0xd186b8c721c0c207, 0xeada7dd6cde0eb1e, 0xf57d4f7fee6ed178,
    0x06f067aa72176fba, 0x0a637dc5a2c898a6, 0x113f9804bef90dae, 0x1b710b35131c471b,
    0x28db77f523047d84, 0x32caab7b40c72493, 0x3c9ebe0a15c9bebc, 0x431d67c49c100d4c,
    0x4cc5d4becb3e42b6, 0x597f299cfc657e2a, 0x5fcb6fab3ad6faec, 0x6c44198c4a475817,
];

/// SHA-512 core over the eight 64-bit state words. Processes 128-byte blocks.
fn sha512_core(mut h: [u64; 8], input: &[u8]) -> [u64; 8] {
    // 128-bit message length; inputs here are far below 2^64 bytes so the high
    // 64 bits are always zero.
    let bit_len = (input.len() as u128).wrapping_mul(8);
    let mut msg = input.to_vec();
    msg.push(0x80);
    while msg.len() % 128 != 112 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.chunks_exact(128) {
        let mut w = [0u64; 80];
        for (i, word) in w.iter_mut().take(16).enumerate() {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&chunk[i * 8..i * 8 + 8]);
            *word = u64::from_be_bytes(bytes);
        }
        for i in 16..80 {
            let s0 = w[i - 15].rotate_right(1) ^ w[i - 15].rotate_right(8) ^ (w[i - 15] >> 7);
            let s1 = w[i - 2].rotate_right(19) ^ w[i - 2].rotate_right(61) ^ (w[i - 2] >> 6);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..80 {
            let s1 = e.rotate_right(14) ^ e.rotate_right(18) ^ e.rotate_right(41);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(SHA512_K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(28) ^ a.rotate_right(34) ^ a.rotate_right(39);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    h
}

fn sha512(input: &[u8]) -> [u8; PG_SHA512_DIGEST_LENGTH] {
    let h = sha512_core(
        [
            0x6a09e667f3bcc908, 0xbb67ae8584caa73b, 0x3c6ef372fe94f82b, 0xa54ff53a5f1d36f1,
            0x510e527fade682d1, 0x9b05688c2b3e6c1f, 0x1f83d9abfb41bd6b, 0x5be0cd19137e2179,
        ],
        input,
    );
    let mut out = [0u8; PG_SHA512_DIGEST_LENGTH];
    for (i, word) in h.iter().enumerate() {
        out[i * 8..i * 8 + 8].copy_from_slice(&word.to_be_bytes());
    }
    out
}

fn sha384(input: &[u8]) -> [u8; PG_SHA384_DIGEST_LENGTH] {
    let h = sha512_core(
        [
            0xcbbb9d5dc1059ed8, 0x629a292a367cd507, 0x9159015a3070dd17, 0x152fecd8f70e5939,
            0x67332667ffc00b31, 0x8eb44a8768581511, 0xdb0c2e0d64f98fa7, 0x47b5481dbefa4fa4,
        ],
        input,
    );
    let mut out = [0u8; PG_SHA384_DIGEST_LENGTH];
    for (i, word) in h.iter().take(6).enumerate() {
        out[i * 8..i * 8 + 8].copy_from_slice(&word.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::genbki::C_COLLATION_OID;
    use crate::postgres::{DatumGetCString, NullableDatum};

    fn fc(args: &[Datum]) -> FunctionCallInfoBaseData {
        FunctionCallInfoBaseData {
            flinfo: None,
            context: None,
            resultinfo: None,
            fncollation: C_COLLATION_OID,
            isnull: false,
            nargs: args.len() as i16,
            args: args
                .iter()
                .map(|&value| NullableDatum { value, isnull: false })
                .collect(),
        }
    }

    fn text_datum(s: &str) -> Datum {
        PointerGetDatum(cstring_to_text(s).cast::<u8>())
    }

    fn out_hex_text(d: Datum) -> String {
        // md5_* returns a text hex; read the varlena payload back.
        let p = DatumGetPointer(d);
        // SAFETY: freshly built text varlena we own.
        let bytes = unsafe { varlena_bytes(p) };
        String::from_utf8_lossy(bytes).into_owned()
    }

    fn out_bytea_hex(d: Datum) -> String {
        let p = DatumGetPointer(d);
        // SAFETY: freshly built bytea varlena we own.
        let bytes = unsafe { varlena_bytes(p) };
        bytes_to_hex(bytes)
    }

    #[test]
    fn md5_rfc_vectors() {
        assert_eq!(bytes_to_hex(&md5(b"")), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(bytes_to_hex(&md5(b"abc")), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(
            bytes_to_hex(&md5(b"message digest")),
            "f96b697d7cb7938d525a2f31aaf161d0"
        );
        assert_eq!(
            bytes_to_hex(&md5(b"abcdefghijklmnopqrstuvwxyz")),
            "c3fcd3d76192e4007dfb496cca67e13b"
        );
    }

    #[test]
    fn sha_nist_vectors() {
        // Empty-input FIPS 180-4 / NIST vectors.
        assert_eq!(
            bytes_to_hex(&sha224(b"")),
            "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"
        );
        assert_eq!(
            bytes_to_hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            bytes_to_hex(&sha384(b"")),
            "38b060a751ac96384cd9327eb1b1e36a21fdb71114be0743\
             4c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b"
        );
        assert_eq!(
            bytes_to_hex(&sha512(b"")),
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce\
             47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        );
        // "abc" vectors.
        assert_eq!(
            bytes_to_hex(&sha224(b"abc")),
            "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"
        );
        assert_eq!(
            bytes_to_hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            bytes_to_hex(&sha384(b"abc")),
            "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded163\
             1a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"
        );
        assert_eq!(
            bytes_to_hex(&sha512(b"abc")),
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a\
             2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
        );
    }

    #[test]
    fn md5_text_and_bytea_builtin() {
        assert_eq!(
            out_hex_text(md5_text(&mut fc(&[text_datum("abc")]))),
            "900150983cd24fb0d6963f7d28e17f72"
        );
        assert_eq!(
            out_hex_text(md5_bytea(&mut fc(&[text_datum("")]))),
            "d41d8cd98f00b204e9800998ecf8427e"
        );
    }

    #[test]
    fn sha_builtins_return_bytea() {
        assert_eq!(
            out_bytea_hex(sha256_bytea(&mut fc(&[text_datum("abc")]))),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            out_bytea_hex(sha224_bytea(&mut fc(&[text_datum("abc")]))),
            "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"
        );
        assert_eq!(
            out_bytea_hex(sha384_bytea(&mut fc(&[text_datum("abc")]))),
            "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded163\
             1a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"
        );
        assert_eq!(
            out_bytea_hex(sha512_bytea(&mut fc(&[text_datum("abc")]))),
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a\
             2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
        );
    }

    #[test]
    fn fmgr_table_binds_crypto() {
        use crate::utils::fmgrtab::fmgr_builtins;
        for name in [
            "md5_text",
            "md5_bytea",
            "sha224_bytea",
            "sha256_bytea",
            "sha384_bytea",
            "sha512_bytea",
        ] {
            let entry = fmgr_builtins
                .iter()
                .find(|b| b.func_name == name)
                .unwrap_or_else(|| panic!("{name} present"));
            assert!(entry.func.is_some(), "{name} bound");
        }
        // Resolve md5_text through the table and check its output.
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "md5_text")
            .unwrap_or_else(|| panic!("md5_text present"));
        let func = entry.func.unwrap_or_else(|| panic!("md5_text bound"));
        let mut f = fc(&[text_datum("abc")]);
        assert_eq!(out_hex_text(func(&mut f)), "900150983cd24fb0d6963f7d28e17f72");
    }
}
