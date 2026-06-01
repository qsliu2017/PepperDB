//! Translation of postgres/src/include/common/sha2.h
//!                + postgres/src/common/sha2_int.h
//!                + postgres/src/common/sha2.c
//!
//! SHA functions for SHA-224, SHA-256, SHA-384 and SHA-512.
//!
//! This includes the fallback implementation for SHA2 cryptographic
//! hashes (the portable in-tree version, NOT the OpenSSL one).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Original author: Aaron D. Gifford <me@aarongifford.com>
//! Copyright (c) 2000-2001, Aaron D. Gifford. All rights reserved.
//!
//! Port notes:
//!   - We translate the BACKEND, NON-OpenSSL fallback path.
//!   - We translate the NON-`WORDS_BIGENDIAN` path (little-endian host), which
//!     is the conventional build target.  SHA itself is big-endian; the host
//!     byte-order conversion macros (REVERSE32/REVERSE64) convert the 64-bit
//!     bitcount and the final state words between host and SHA byte order.
//!   - The SHA2_UNROLL_TRANSFORM variant is not used (it is gated behind a
//!     macro that is not defined here); we translate the rolled transform.
//!   - All round arithmetic uses wrapping_add (uint32/uint64 overflow is
//!     expected) and rotate_right for the rotation primitives.

use crate::prelude::*;

/*** SHA224/256/384/512 Various Length Definitions ***********************/
/* (from common/sha2.h) */
pub const PG_SHA224_BLOCK_LENGTH: usize = 64;
pub const PG_SHA224_DIGEST_LENGTH: usize = 28;
pub const PG_SHA224_DIGEST_STRING_LENGTH: usize = PG_SHA224_DIGEST_LENGTH * 2 + 1;
pub const PG_SHA256_BLOCK_LENGTH: usize = 64;
pub const PG_SHA256_DIGEST_LENGTH: usize = 32;
pub const PG_SHA256_DIGEST_STRING_LENGTH: usize = PG_SHA256_DIGEST_LENGTH * 2 + 1;
pub const PG_SHA384_BLOCK_LENGTH: usize = 128;
pub const PG_SHA384_DIGEST_LENGTH: usize = 48;
pub const PG_SHA384_DIGEST_STRING_LENGTH: usize = PG_SHA384_DIGEST_LENGTH * 2 + 1;
pub const PG_SHA512_BLOCK_LENGTH: usize = 128;
pub const PG_SHA512_DIGEST_LENGTH: usize = 64;
pub const PG_SHA512_DIGEST_STRING_LENGTH: usize = PG_SHA512_DIGEST_LENGTH * 2 + 1;

/* Context structures (from common/sha2_int.h) */
// Copy: POD state; lets these compose into the cryptohash.c union faithfully.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct pg_sha256_ctx {
    pub state: [uint32; 8],
    pub bitcount: uint64,
    pub buffer: [uint8; PG_SHA256_BLOCK_LENGTH],
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct pg_sha512_ctx {
    pub state: [uint64; 8],
    pub bitcount: [uint64; 2],
    pub buffer: [uint8; PG_SHA512_BLOCK_LENGTH],
}
/* typedef struct pg_sha256_ctx pg_sha224_ctx; */
pub type pg_sha224_ctx = pg_sha256_ctx;
/* typedef struct pg_sha512_ctx pg_sha384_ctx; */
pub type pg_sha384_ctx = pg_sha512_ctx;

/*** SHA-256/384/512 Various Length Definitions ***********************/
const PG_SHA256_SHORT_BLOCK_LENGTH: usize = PG_SHA256_BLOCK_LENGTH - 8;
#[allow(dead_code)]
const PG_SHA384_SHORT_BLOCK_LENGTH: usize = PG_SHA384_BLOCK_LENGTH - 16;
const PG_SHA512_SHORT_BLOCK_LENGTH: usize = PG_SHA512_BLOCK_LENGTH - 16;

/*** ENDIAN REVERSAL MACROS *******************************************/
/*
 * These are only used on a non-bigendian host (the conventional build).
 * They reverse the byte order of a 32- or 64-bit word.
 *
 * #define REVERSE32(w,x) ...
 * #define REVERSE64(w,x) ...
 */
#[inline(always)]
fn REVERSE32(w: uint32) -> uint32 {
    let mut tmp: uint32 = w;
    tmp = (tmp >> 16) | (tmp << 16);
    ((tmp & 0xff00ff00) >> 8) | ((tmp & 0x00ff00ff) << 8)
}

#[inline(always)]
fn REVERSE64(w: uint64) -> uint64 {
    let mut tmp: uint64 = w;
    tmp = (tmp >> 32) | (tmp << 32);
    tmp = ((tmp & 0xff00ff00ff00ff00) >> 8) | ((tmp & 0x00ff00ff00ff00ff) << 8);
    ((tmp & 0xffff0000ffff0000) >> 16) | ((tmp & 0x0000ffff0000ffff) << 16)
}

/*
 * Macro for incrementally adding the unsigned 64-bit integer n to the
 * unsigned 128-bit integer (represented using a two-element array of
 * 64-bit words):
 *
 * #define ADDINC128(w,n) ...
 *
 * Arithmetic on the 64-bit words wraps (carry detection mirrors the C, which
 * relies on the unsigned wraparound), so we use wrapping_add explicitly.
 */
#[inline(always)]
fn ADDINC128(w: &mut [uint64; 2], n: uint64) {
    w[0] = w[0].wrapping_add(n);
    if w[0] < n {
        w[1] = w[1].wrapping_add(1);
    }
}

/*** THE SIX LOGICAL FUNCTIONS ****************************************/
/*
 * Bit shifting and rotation (used by the six SHA-XYZ logical functions:
 *
 *	 NOTE:	The naming of R and S appears backwards here (R is a SHIFT and
 *	 S is a ROTATION) because the SHA-256/384/512 description document
 *	 (see http://www.iwar.org.uk/comsec/resources/cipher/sha256-384-512.pdf)
 *	 uses this same "backwards" definition.
 */
/* Shift-right (used in SHA-256, SHA-384, and SHA-512): */
/* #define R(b,x) ((x) >> (b)) */
#[inline(always)]
fn R32(b: u32, x: uint32) -> uint32 {
    x >> b
}
#[inline(always)]
fn R64(b: u32, x: uint64) -> uint64 {
    x >> b
}
/* 32-bit Rotate-right (used in SHA-256): */
/* #define S32(b,x) (((x) >> (b)) | ((x) << (32 - (b)))) */
#[inline(always)]
fn S32(b: u32, x: uint32) -> uint32 {
    x.rotate_right(b)
}
/* 64-bit Rotate-right (used in SHA-384 and SHA-512): */
/* #define S64(b,x) (((x) >> (b)) | ((x) << (64 - (b)))) */
#[inline(always)]
fn S64(b: u32, x: uint64) -> uint64 {
    x.rotate_right(b)
}

/* Two of six logical functions used in SHA-256, SHA-384, and SHA-512: */
/* #define Ch(x,y,z) (((x) & (y)) ^ ((~(x)) & (z))) */
#[inline(always)]
fn Ch32(x: uint32, y: uint32, z: uint32) -> uint32 {
    (x & y) ^ ((!x) & z)
}
#[inline(always)]
fn Ch64(x: uint64, y: uint64, z: uint64) -> uint64 {
    (x & y) ^ ((!x) & z)
}
/* #define Maj(x,y,z) (((x) & (y)) ^ ((x) & (z)) ^ ((y) & (z))) */
#[inline(always)]
fn Maj32(x: uint32, y: uint32, z: uint32) -> uint32 {
    (x & y) ^ (x & z) ^ (y & z)
}
#[inline(always)]
fn Maj64(x: uint64, y: uint64, z: uint64) -> uint64 {
    (x & y) ^ (x & z) ^ (y & z)
}

/* Four of six logical functions used in SHA-256: */
/* #define Sigma0_256(x) (S32(2, (x)) ^ S32(13, (x)) ^ S32(22, (x))) */
#[inline(always)]
fn Sigma0_256(x: uint32) -> uint32 {
    S32(2, x) ^ S32(13, x) ^ S32(22, x)
}
/* #define Sigma1_256(x) (S32(6, (x)) ^ S32(11, (x)) ^ S32(25, (x))) */
#[inline(always)]
fn Sigma1_256(x: uint32) -> uint32 {
    S32(6, x) ^ S32(11, x) ^ S32(25, x)
}
/* #define sigma0_256(x) (S32(7, (x)) ^ S32(18, (x)) ^ R(3, (x))) */
#[inline(always)]
fn sigma0_256(x: uint32) -> uint32 {
    S32(7, x) ^ S32(18, x) ^ R32(3, x)
}
/* #define sigma1_256(x) (S32(17, (x)) ^ S32(19, (x)) ^ R(10, (x))) */
#[inline(always)]
fn sigma1_256(x: uint32) -> uint32 {
    S32(17, x) ^ S32(19, x) ^ R32(10, x)
}

/* Four of six logical functions used in SHA-384 and SHA-512: */
/* #define Sigma0_512(x) (S64(28, (x)) ^ S64(34, (x)) ^ S64(39, (x))) */
#[inline(always)]
fn Sigma0_512(x: uint64) -> uint64 {
    S64(28, x) ^ S64(34, x) ^ S64(39, x)
}
/* #define Sigma1_512(x) (S64(14, (x)) ^ S64(18, (x)) ^ S64(41, (x))) */
#[inline(always)]
fn Sigma1_512(x: uint64) -> uint64 {
    S64(14, x) ^ S64(18, x) ^ S64(41, x)
}
/* #define sigma0_512(x) (S64( 1, (x)) ^ S64( 8, (x)) ^ R( 7, (x))) */
#[inline(always)]
fn sigma0_512(x: uint64) -> uint64 {
    S64(1, x) ^ S64(8, x) ^ R64(7, x)
}
/* #define sigma1_512(x) (S64(19, (x)) ^ S64(61, (x)) ^ R( 6, (x))) */
#[inline(always)]
fn sigma1_512(x: uint64) -> uint64 {
    S64(19, x) ^ S64(61, x) ^ R64(6, x)
}

/*** SHA-XYZ INITIAL HASH VALUES AND CONSTANTS ************************/
/* Hash constant words K for SHA-256: */
static K256: [uint32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
    0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
    0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
    0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
    0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
    0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/* Initial hash value H for SHA-224: */
static sha224_initial_hash_value: [uint32; 8] = [
    0xc1059ed8,
    0x367cd507,
    0x3070dd17,
    0xf70e5939,
    0xffc00b31,
    0x68581511,
    0x64f98fa7,
    0xbefa4fa4,
];

/* Initial hash value H for SHA-256: */
static sha256_initial_hash_value: [uint32; 8] = [
    0x6a09e667,
    0xbb67ae85,
    0x3c6ef372,
    0xa54ff53a,
    0x510e527f,
    0x9b05688c,
    0x1f83d9ab,
    0x5be0cd19,
];

/* Hash constant words K for SHA-384 and SHA-512: */
static K512: [uint64; 80] = [
    0x428a2f98d728ae22, 0x7137449123ef65cd,
    0xb5c0fbcfec4d3b2f, 0xe9b5dba58189dbbc,
    0x3956c25bf348b538, 0x59f111f1b605d019,
    0x923f82a4af194f9b, 0xab1c5ed5da6d8118,
    0xd807aa98a3030242, 0x12835b0145706fbe,
    0x243185be4ee4b28c, 0x550c7dc3d5ffb4e2,
    0x72be5d74f27b896f, 0x80deb1fe3b1696b1,
    0x9bdc06a725c71235, 0xc19bf174cf692694,
    0xe49b69c19ef14ad2, 0xefbe4786384f25e3,
    0x0fc19dc68b8cd5b5, 0x240ca1cc77ac9c65,
    0x2de92c6f592b0275, 0x4a7484aa6ea6e483,
    0x5cb0a9dcbd41fbd4, 0x76f988da831153b5,
    0x983e5152ee66dfab, 0xa831c66d2db43210,
    0xb00327c898fb213f, 0xbf597fc7beef0ee4,
    0xc6e00bf33da88fc2, 0xd5a79147930aa725,
    0x06ca6351e003826f, 0x142929670a0e6e70,
    0x27b70a8546d22ffc, 0x2e1b21385c26c926,
    0x4d2c6dfc5ac42aed, 0x53380d139d95b3df,
    0x650a73548baf63de, 0x766a0abb3c77b2a8,
    0x81c2c92e47edaee6, 0x92722c851482353b,
    0xa2bfe8a14cf10364, 0xa81a664bbc423001,
    0xc24b8b70d0f89791, 0xc76c51a30654be30,
    0xd192e819d6ef5218, 0xd69906245565a910,
    0xf40e35855771202a, 0x106aa07032bbd1b8,
    0x19a4c116b8d2d0c8, 0x1e376c085141ab53,
    0x2748774cdf8eeb99, 0x34b0bcb5e19b48a8,
    0x391c0cb3c5c95a63, 0x4ed8aa4ae3418acb,
    0x5b9cca4f7763e373, 0x682e6ff3d6b2b8a3,
    0x748f82ee5defb2fc, 0x78a5636f43172f60,
    0x84c87814a1f0ab72, 0x8cc702081a6439ec,
    0x90befffa23631e28, 0xa4506cebde82bde9,
    0xbef9a3f7b2c67915, 0xc67178f2e372532b,
    0xca273eceea26619c, 0xd186b8c721c0c207,
    0xeada7dd6cde0eb1e, 0xf57d4f7fee6ed178,
    0x06f067aa72176fba, 0x0a637dc5a2c898a6,
    0x113f9804bef90dae, 0x1b710b35131c471b,
    0x28db77f523047d84, 0x32caab7b40c72493,
    0x3c9ebe0a15c9bebc, 0x431d67c49c100d4c,
    0x4cc5d4becb3e42b6, 0x597f299cfc657e2a,
    0x5fcb6fab3ad6faec, 0x6c44198c4a475817,
];

/* Initial hash value H for SHA-384 */
static sha384_initial_hash_value: [uint64; 8] = [
    0xcbbb9d5dc1059ed8,
    0x629a292a367cd507,
    0x9159015a3070dd17,
    0x152fecd8f70e5939,
    0x67332667ffc00b31,
    0x8eb44a8768581511,
    0xdb0c2e0d64f98fa7,
    0x47b5481dbefa4fa4,
];

/* Initial hash value H for SHA-512 */
static sha512_initial_hash_value: [uint64; 8] = [
    0x6a09e667f3bcc908,
    0xbb67ae8584caa73b,
    0x3c6ef372fe94f82b,
    0xa54ff53a5f1d36f1,
    0x510e527fade682d1,
    0x9b05688c2b3e6c1f,
    0x1f83d9abfb41bd6b,
    0x5be0cd19137e2179,
];

/*** SHA-256: *********************************************************/
pub unsafe fn pg_sha256_init(context: *mut pg_sha256_ctx) {
    if context.is_null() {
        return;
    }
    /* memcpy(context->state, sha256_initial_hash_value, PG_SHA256_DIGEST_LENGTH); */
    core::ptr::copy_nonoverlapping(
        sha256_initial_hash_value.as_ptr() as *const u8,
        (*context).state.as_mut_ptr() as *mut u8,
        PG_SHA256_DIGEST_LENGTH,
    );
    /* memset(context->buffer, 0, PG_SHA256_BLOCK_LENGTH); */
    core::ptr::write_bytes((*context).buffer.as_mut_ptr(), 0, PG_SHA256_BLOCK_LENGTH);
    (*context).bitcount = 0;
}

/* (SHA2_UNROLL_TRANSFORM variant omitted; we use the rolled transform.) */
unsafe fn SHA256_Transform(context: *mut pg_sha256_ctx, mut data: *const uint8) {
    let mut a: uint32;
    let mut b: uint32;
    let mut c: uint32;
    let mut d: uint32;
    let mut e: uint32;
    let mut f: uint32;
    let mut g: uint32;
    let mut h: uint32;
    let mut s0: uint32;
    let mut s1: uint32;
    let mut T1: uint32;
    let mut T2: uint32;
    /* W256 = (uint32 *) context->buffer; */
    let W256: *mut uint32 = (*context).buffer.as_mut_ptr() as *mut uint32;
    let mut j: c_int;

    /* Initialize registers with the prev. intermediate value */
    a = (*context).state[0];
    b = (*context).state[1];
    c = (*context).state[2];
    d = (*context).state[3];
    e = (*context).state[4];
    f = (*context).state[5];
    g = (*context).state[6];
    h = (*context).state[7];

    j = 0;
    loop {
        /*
         * W256[j] = (uint32) data[3] | ((uint32) data[2] << 8) |
         *     ((uint32) data[1] << 16) | ((uint32) data[0] << 24);
         */
        *W256.add(j as usize) = (*data.add(3) as uint32)
            | ((*data.add(2) as uint32) << 8)
            | ((*data.add(1) as uint32) << 16)
            | ((*data.add(0) as uint32) << 24);
        data = data.add(4);
        /* Apply the SHA-256 compression function to update a..h */
        T1 = h
            .wrapping_add(Sigma1_256(e))
            .wrapping_add(Ch32(e, f, g))
            .wrapping_add(K256[j as usize])
            .wrapping_add(*W256.add(j as usize));
        T2 = Sigma0_256(a).wrapping_add(Maj32(a, b, c));
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(T1);
        d = c;
        c = b;
        b = a;
        a = T1.wrapping_add(T2);

        j += 1;
        if j >= 16 {
            break;
        }
    }

    loop {
        /* Part of the message block expansion: */
        s0 = *W256.add(((j + 1) & 0x0f) as usize);
        s0 = sigma0_256(s0);
        s1 = *W256.add(((j + 14) & 0x0f) as usize);
        s1 = sigma1_256(s1);

        /* Apply the SHA-256 compression function to update a..h */
        /*
         * T1 = h + Sigma1_256(e) + Ch(e, f, g) + K256[j] +
         *     (W256[j & 0x0f] += s1 + W256[(j + 9) & 0x0f] + s0);
         */
        let w_idx = (j & 0x0f) as usize;
        let w_new = (*W256.add(w_idx))
            .wrapping_add(s1)
            .wrapping_add(*W256.add(((j + 9) & 0x0f) as usize))
            .wrapping_add(s0);
        *W256.add(w_idx) = w_new;
        T1 = h
            .wrapping_add(Sigma1_256(e))
            .wrapping_add(Ch32(e, f, g))
            .wrapping_add(K256[j as usize])
            .wrapping_add(w_new);
        T2 = Sigma0_256(a).wrapping_add(Maj32(a, b, c));
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(T1);
        d = c;
        c = b;
        b = a;
        a = T1.wrapping_add(T2);

        j += 1;
        if j >= 64 {
            break;
        }
    }

    /* Compute the current intermediate hash value */
    (*context).state[0] = (*context).state[0].wrapping_add(a);
    (*context).state[1] = (*context).state[1].wrapping_add(b);
    (*context).state[2] = (*context).state[2].wrapping_add(c);
    (*context).state[3] = (*context).state[3].wrapping_add(d);
    (*context).state[4] = (*context).state[4].wrapping_add(e);
    (*context).state[5] = (*context).state[5].wrapping_add(f);
    (*context).state[6] = (*context).state[6].wrapping_add(g);
    (*context).state[7] = (*context).state[7].wrapping_add(h);

    /* Clean up */
    a = 0;
    b = a;
    c = b;
    d = c;
    e = d;
    f = e;
    g = f;
    h = g;
    T1 = h;
    T2 = T1;
    let _ = (a, b, c, d, e, f, g, h, T1, T2, s0, s1);
}

pub unsafe fn pg_sha256_update(context: *mut pg_sha256_ctx, mut data: *const uint8, mut len: usize) {
    let freespace: usize;
    let usedspace: usize;

    /* Calling with no data is valid (we do nothing) */
    if len == 0 {
        return;
    }

    usedspace = (((*context).bitcount >> 3) % PG_SHA256_BLOCK_LENGTH as uint64) as usize;
    if usedspace > 0 {
        /* Calculate how much free space is available in the buffer */
        freespace = PG_SHA256_BLOCK_LENGTH - usedspace;

        if len >= freespace {
            /* Fill the buffer completely and process it */
            core::ptr::copy_nonoverlapping(
                data,
                (*context).buffer.as_mut_ptr().add(usedspace),
                freespace,
            );
            (*context).bitcount += (freespace << 3) as uint64;
            len -= freespace;
            data = data.add(freespace);
            SHA256_Transform(context, (*context).buffer.as_ptr());
        } else {
            /* The buffer is not yet full */
            core::ptr::copy_nonoverlapping(
                data,
                (*context).buffer.as_mut_ptr().add(usedspace),
                len,
            );
            (*context).bitcount += (len << 3) as uint64;
            /* Clean up: */
            return;
        }
    }
    while len >= PG_SHA256_BLOCK_LENGTH {
        /* Process as many complete blocks as we can */
        SHA256_Transform(context, data);
        (*context).bitcount += (PG_SHA256_BLOCK_LENGTH << 3) as uint64;
        len -= PG_SHA256_BLOCK_LENGTH;
        data = data.add(PG_SHA256_BLOCK_LENGTH);
    }
    if len > 0 {
        /* There's left-overs, so save 'em */
        core::ptr::copy_nonoverlapping(data, (*context).buffer.as_mut_ptr(), len);
        (*context).bitcount += (len << 3) as uint64;
    }
    /* Clean up: */
}

unsafe fn SHA256_Last(context: *mut pg_sha256_ctx) {
    let mut usedspace: c_uint;

    usedspace = (((*context).bitcount >> 3) % PG_SHA256_BLOCK_LENGTH as uint64) as c_uint;
    /* Convert FROM host byte order */
    (*context).bitcount = REVERSE64((*context).bitcount);
    if usedspace > 0 {
        /* Begin padding with a 1 bit: */
        (*context).buffer[usedspace as usize] = 0x80;
        usedspace += 1;

        if (usedspace as usize) <= PG_SHA256_SHORT_BLOCK_LENGTH {
            /* Set-up for the last transform: */
            core::ptr::write_bytes(
                (*context).buffer.as_mut_ptr().add(usedspace as usize),
                0,
                PG_SHA256_SHORT_BLOCK_LENGTH - usedspace as usize,
            );
        } else {
            if (usedspace as usize) < PG_SHA256_BLOCK_LENGTH {
                core::ptr::write_bytes(
                    (*context).buffer.as_mut_ptr().add(usedspace as usize),
                    0,
                    PG_SHA256_BLOCK_LENGTH - usedspace as usize,
                );
            }
            /* Do second-to-last transform: */
            SHA256_Transform(context, (*context).buffer.as_ptr());

            /* And set-up for the last transform: */
            core::ptr::write_bytes(
                (*context).buffer.as_mut_ptr(),
                0,
                PG_SHA256_SHORT_BLOCK_LENGTH,
            );
        }
    } else {
        /* Set-up for the last transform: */
        core::ptr::write_bytes(
            (*context).buffer.as_mut_ptr(),
            0,
            PG_SHA256_SHORT_BLOCK_LENGTH,
        );

        /* Begin padding with a 1 bit: */
        (*context).buffer[0] = 0x80;
    }
    /* Set the bit count: */
    /* *(uint64 *) &context->buffer[PG_SHA256_SHORT_BLOCK_LENGTH] = context->bitcount; */
    core::ptr::write_unaligned(
        (*context).buffer.as_mut_ptr().add(PG_SHA256_SHORT_BLOCK_LENGTH) as *mut uint64,
        (*context).bitcount,
    );

    /* Final transform: */
    SHA256_Transform(context, (*context).buffer.as_ptr());
}

pub unsafe fn pg_sha256_final(context: *mut pg_sha256_ctx, digest: *mut uint8) {
    /* If no digest buffer is passed, we don't bother doing this: */
    if !digest.is_null() {
        SHA256_Last(context);

        {
            /* Convert TO host byte order */
            let mut j: c_int = 0;
            while j < 8 {
                (*context).state[j as usize] = REVERSE32((*context).state[j as usize]);
                j += 1;
            }
        }
        core::ptr::copy_nonoverlapping(
            (*context).state.as_ptr() as *const u8,
            digest,
            PG_SHA256_DIGEST_LENGTH,
        );
    }

    /* Clean up state data: */
    core::ptr::write_bytes(context as *mut u8, 0, core::mem::size_of::<pg_sha256_ctx>());
}

/*** SHA-512: *********************************************************/
pub unsafe fn pg_sha512_init(context: *mut pg_sha512_ctx) {
    if context.is_null() {
        return;
    }
    /* memcpy(context->state, sha512_initial_hash_value, PG_SHA512_DIGEST_LENGTH); */
    core::ptr::copy_nonoverlapping(
        sha512_initial_hash_value.as_ptr() as *const u8,
        (*context).state.as_mut_ptr() as *mut u8,
        PG_SHA512_DIGEST_LENGTH,
    );
    /* memset(context->buffer, 0, PG_SHA512_BLOCK_LENGTH); */
    core::ptr::write_bytes((*context).buffer.as_mut_ptr(), 0, PG_SHA512_BLOCK_LENGTH);
    (*context).bitcount[0] = 0;
    (*context).bitcount[1] = 0;
}

/* (SHA2_UNROLL_TRANSFORM variant omitted; we use the rolled transform.) */
unsafe fn SHA512_Transform(context: *mut pg_sha512_ctx, mut data: *const uint8) {
    let mut a: uint64;
    let mut b: uint64;
    let mut c: uint64;
    let mut d: uint64;
    let mut e: uint64;
    let mut f: uint64;
    let mut g: uint64;
    let mut h: uint64;
    let mut s0: uint64;
    let mut s1: uint64;
    let mut T1: uint64;
    let mut T2: uint64;
    let W512: *mut uint64 = (*context).buffer.as_mut_ptr() as *mut uint64;
    let mut j: c_int;

    /* Initialize registers with the prev. intermediate value */
    a = (*context).state[0];
    b = (*context).state[1];
    c = (*context).state[2];
    d = (*context).state[3];
    e = (*context).state[4];
    f = (*context).state[5];
    g = (*context).state[6];
    h = (*context).state[7];

    j = 0;
    loop {
        /*
         * W512[j] = (uint64) data[7] | ((uint64) data[6] << 8) |
         *     ((uint64) data[5] << 16) | ((uint64) data[4] << 24) |
         *     ((uint64) data[3] << 32) | ((uint64) data[2] << 40) |
         *     ((uint64) data[1] << 48) | ((uint64) data[0] << 56);
         */
        *W512.add(j as usize) = (*data.add(7) as uint64)
            | ((*data.add(6) as uint64) << 8)
            | ((*data.add(5) as uint64) << 16)
            | ((*data.add(4) as uint64) << 24)
            | ((*data.add(3) as uint64) << 32)
            | ((*data.add(2) as uint64) << 40)
            | ((*data.add(1) as uint64) << 48)
            | ((*data.add(0) as uint64) << 56);
        data = data.add(8);
        /* Apply the SHA-512 compression function to update a..h */
        T1 = h
            .wrapping_add(Sigma1_512(e))
            .wrapping_add(Ch64(e, f, g))
            .wrapping_add(K512[j as usize])
            .wrapping_add(*W512.add(j as usize));
        T2 = Sigma0_512(a).wrapping_add(Maj64(a, b, c));
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(T1);
        d = c;
        c = b;
        b = a;
        a = T1.wrapping_add(T2);

        j += 1;
        if j >= 16 {
            break;
        }
    }

    loop {
        /* Part of the message block expansion: */
        s0 = *W512.add(((j + 1) & 0x0f) as usize);
        s0 = sigma0_512(s0);
        s1 = *W512.add(((j + 14) & 0x0f) as usize);
        s1 = sigma1_512(s1);

        /* Apply the SHA-512 compression function to update a..h */
        /*
         * T1 = h + Sigma1_512(e) + Ch(e, f, g) + K512[j] +
         *     (W512[j & 0x0f] += s1 + W512[(j + 9) & 0x0f] + s0);
         */
        let w_idx = (j & 0x0f) as usize;
        let w_new = (*W512.add(w_idx))
            .wrapping_add(s1)
            .wrapping_add(*W512.add(((j + 9) & 0x0f) as usize))
            .wrapping_add(s0);
        *W512.add(w_idx) = w_new;
        T1 = h
            .wrapping_add(Sigma1_512(e))
            .wrapping_add(Ch64(e, f, g))
            .wrapping_add(K512[j as usize])
            .wrapping_add(w_new);
        T2 = Sigma0_512(a).wrapping_add(Maj64(a, b, c));
        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(T1);
        d = c;
        c = b;
        b = a;
        a = T1.wrapping_add(T2);

        j += 1;
        if j >= 80 {
            break;
        }
    }

    /* Compute the current intermediate hash value */
    (*context).state[0] = (*context).state[0].wrapping_add(a);
    (*context).state[1] = (*context).state[1].wrapping_add(b);
    (*context).state[2] = (*context).state[2].wrapping_add(c);
    (*context).state[3] = (*context).state[3].wrapping_add(d);
    (*context).state[4] = (*context).state[4].wrapping_add(e);
    (*context).state[5] = (*context).state[5].wrapping_add(f);
    (*context).state[6] = (*context).state[6].wrapping_add(g);
    (*context).state[7] = (*context).state[7].wrapping_add(h);

    /* Clean up */
    a = 0;
    b = a;
    c = b;
    d = c;
    e = d;
    f = e;
    g = f;
    h = g;
    T1 = h;
    T2 = T1;
    let _ = (a, b, c, d, e, f, g, h, T1, T2, s0, s1);
}

pub unsafe fn pg_sha512_update(context: *mut pg_sha512_ctx, mut data: *const uint8, mut len: usize) {
    let freespace: usize;
    let usedspace: usize;

    /* Calling with no data is valid (we do nothing) */
    if len == 0 {
        return;
    }

    usedspace = (((*context).bitcount[0] >> 3) % PG_SHA512_BLOCK_LENGTH as uint64) as usize;
    if usedspace > 0 {
        /* Calculate how much free space is available in the buffer */
        freespace = PG_SHA512_BLOCK_LENGTH - usedspace;

        if len >= freespace {
            /* Fill the buffer completely and process it */
            core::ptr::copy_nonoverlapping(
                data,
                (*context).buffer.as_mut_ptr().add(usedspace),
                freespace,
            );
            ADDINC128(&mut (*context).bitcount, (freespace << 3) as uint64);
            len -= freespace;
            data = data.add(freespace);
            SHA512_Transform(context, (*context).buffer.as_ptr());
        } else {
            /* The buffer is not yet full */
            core::ptr::copy_nonoverlapping(
                data,
                (*context).buffer.as_mut_ptr().add(usedspace),
                len,
            );
            ADDINC128(&mut (*context).bitcount, (len << 3) as uint64);
            /* Clean up: */
            return;
        }
    }
    while len >= PG_SHA512_BLOCK_LENGTH {
        /* Process as many complete blocks as we can */
        SHA512_Transform(context, data);
        ADDINC128(&mut (*context).bitcount, (PG_SHA512_BLOCK_LENGTH << 3) as uint64);
        len -= PG_SHA512_BLOCK_LENGTH;
        data = data.add(PG_SHA512_BLOCK_LENGTH);
    }
    if len > 0 {
        /* There's left-overs, so save 'em */
        core::ptr::copy_nonoverlapping(data, (*context).buffer.as_mut_ptr(), len);
        ADDINC128(&mut (*context).bitcount, (len << 3) as uint64);
    }
    /* Clean up: */
}

unsafe fn SHA512_Last(context: *mut pg_sha512_ctx) {
    let mut usedspace: c_uint;

    usedspace = (((*context).bitcount[0] >> 3) % PG_SHA512_BLOCK_LENGTH as uint64) as c_uint;
    /* Convert FROM host byte order */
    (*context).bitcount[0] = REVERSE64((*context).bitcount[0]);
    (*context).bitcount[1] = REVERSE64((*context).bitcount[1]);
    if usedspace > 0 {
        /* Begin padding with a 1 bit: */
        (*context).buffer[usedspace as usize] = 0x80;
        usedspace += 1;

        if (usedspace as usize) <= PG_SHA512_SHORT_BLOCK_LENGTH {
            /* Set-up for the last transform: */
            core::ptr::write_bytes(
                (*context).buffer.as_mut_ptr().add(usedspace as usize),
                0,
                PG_SHA512_SHORT_BLOCK_LENGTH - usedspace as usize,
            );
        } else {
            if (usedspace as usize) < PG_SHA512_BLOCK_LENGTH {
                core::ptr::write_bytes(
                    (*context).buffer.as_mut_ptr().add(usedspace as usize),
                    0,
                    PG_SHA512_BLOCK_LENGTH - usedspace as usize,
                );
            }
            /* Do second-to-last transform: */
            SHA512_Transform(context, (*context).buffer.as_ptr());

            /* And set-up for the last transform: */
            core::ptr::write_bytes(
                (*context).buffer.as_mut_ptr(),
                0,
                PG_SHA512_BLOCK_LENGTH - 2,
            );
        }
    } else {
        /* Prepare for final transform: */
        core::ptr::write_bytes(
            (*context).buffer.as_mut_ptr(),
            0,
            PG_SHA512_SHORT_BLOCK_LENGTH,
        );

        /* Begin padding with a 1 bit: */
        (*context).buffer[0] = 0x80;
    }
    /* Store the length of input data (in bits): */
    /* *(uint64 *) &context->buffer[PG_SHA512_SHORT_BLOCK_LENGTH] = context->bitcount[1]; */
    core::ptr::write_unaligned(
        (*context).buffer.as_mut_ptr().add(PG_SHA512_SHORT_BLOCK_LENGTH) as *mut uint64,
        (*context).bitcount[1],
    );
    /* *(uint64 *) &context->buffer[PG_SHA512_SHORT_BLOCK_LENGTH + 8] = context->bitcount[0]; */
    core::ptr::write_unaligned(
        (*context).buffer.as_mut_ptr().add(PG_SHA512_SHORT_BLOCK_LENGTH + 8) as *mut uint64,
        (*context).bitcount[0],
    );

    /* Final transform: */
    SHA512_Transform(context, (*context).buffer.as_ptr());
}

pub unsafe fn pg_sha512_final(context: *mut pg_sha512_ctx, digest: *mut uint8) {
    /* If no digest buffer is passed, we don't bother doing this: */
    if !digest.is_null() {
        SHA512_Last(context);

        /* Save the hash data for output: */
        {
            /* Convert TO host byte order */
            let mut j: c_int = 0;
            while j < 8 {
                (*context).state[j as usize] = REVERSE64((*context).state[j as usize]);
                j += 1;
            }
        }
        core::ptr::copy_nonoverlapping(
            (*context).state.as_ptr() as *const u8,
            digest,
            PG_SHA512_DIGEST_LENGTH,
        );
    }

    /* Zero out state data */
    core::ptr::write_bytes(context as *mut u8, 0, core::mem::size_of::<pg_sha512_ctx>());
}

/*** SHA-384: *********************************************************/
pub unsafe fn pg_sha384_init(context: *mut pg_sha384_ctx) {
    if context.is_null() {
        return;
    }
    /* memcpy(context->state, sha384_initial_hash_value, PG_SHA512_DIGEST_LENGTH); */
    core::ptr::copy_nonoverlapping(
        sha384_initial_hash_value.as_ptr() as *const u8,
        (*context).state.as_mut_ptr() as *mut u8,
        PG_SHA512_DIGEST_LENGTH,
    );
    /* memset(context->buffer, 0, PG_SHA384_BLOCK_LENGTH); */
    core::ptr::write_bytes((*context).buffer.as_mut_ptr(), 0, PG_SHA384_BLOCK_LENGTH);
    (*context).bitcount[0] = 0;
    (*context).bitcount[1] = 0;
}

pub unsafe fn pg_sha384_update(context: *mut pg_sha384_ctx, data: *const uint8, len: usize) {
    pg_sha512_update(context as *mut pg_sha512_ctx, data, len);
}

pub unsafe fn pg_sha384_final(context: *mut pg_sha384_ctx, digest: *mut uint8) {
    /* If no digest buffer is passed, we don't bother doing this: */
    if !digest.is_null() {
        SHA512_Last(context as *mut pg_sha512_ctx);

        /* Save the hash data for output: */
        {
            /* Convert TO host byte order */
            let mut j: c_int = 0;
            while j < 6 {
                (*context).state[j as usize] = REVERSE64((*context).state[j as usize]);
                j += 1;
            }
        }
        core::ptr::copy_nonoverlapping(
            (*context).state.as_ptr() as *const u8,
            digest,
            PG_SHA384_DIGEST_LENGTH,
        );
    }

    /* Zero out state data */
    core::ptr::write_bytes(context as *mut u8, 0, core::mem::size_of::<pg_sha384_ctx>());
}

/*** SHA-224: *********************************************************/
pub unsafe fn pg_sha224_init(context: *mut pg_sha224_ctx) {
    if context.is_null() {
        return;
    }
    /* memcpy(context->state, sha224_initial_hash_value, PG_SHA256_DIGEST_LENGTH); */
    core::ptr::copy_nonoverlapping(
        sha224_initial_hash_value.as_ptr() as *const u8,
        (*context).state.as_mut_ptr() as *mut u8,
        PG_SHA256_DIGEST_LENGTH,
    );
    /* memset(context->buffer, 0, PG_SHA256_BLOCK_LENGTH); */
    core::ptr::write_bytes((*context).buffer.as_mut_ptr(), 0, PG_SHA256_BLOCK_LENGTH);
    (*context).bitcount = 0;
}

pub unsafe fn pg_sha224_update(context: *mut pg_sha224_ctx, data: *const uint8, len: usize) {
    pg_sha256_update(context as *mut pg_sha256_ctx, data, len);
}

pub unsafe fn pg_sha224_final(context: *mut pg_sha224_ctx, digest: *mut uint8) {
    /* If no digest buffer is passed, we don't bother doing this: */
    if !digest.is_null() {
        SHA256_Last(context);

        {
            /* Convert TO host byte order */
            let mut j: c_int = 0;
            while j < 8 {
                (*context).state[j as usize] = REVERSE32((*context).state[j as usize]);
                j += 1;
            }
        }
        core::ptr::copy_nonoverlapping(
            (*context).state.as_ptr() as *const u8,
            digest,
            PG_SHA224_DIGEST_LENGTH,
        );
    }

    /* Clean up state data: */
    core::ptr::write_bytes(context as *mut u8, 0, core::mem::size_of::<pg_sha224_ctx>());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{:02x}", x)).collect()
    }

    #[test]
    fn sha256_known_answer() {
        unsafe {
            let mut ctx: pg_sha256_ctx = core::mem::zeroed();
            pg_sha256_init(&mut ctx);
            let msg = b"abc";
            pg_sha256_update(&mut ctx, msg.as_ptr(), msg.len());
            let mut out = [0u8; PG_SHA256_DIGEST_LENGTH];
            pg_sha256_final(&mut ctx, out.as_mut_ptr());
            assert_eq!(
                hex(&out),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            );
        }
    }
}
