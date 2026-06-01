//! Translation of postgres/src/common/f2s.c
//!
//! Ryu floating-point output for single precision (float32 -> shortest decimal).
//!
//! #include mapping:
//!   - "common/shortest_dec.h": only FLOAT_SHORTEST_DECIMAL_LEN (=16) is used; inlined below.
//!   - "digit_table.h"        : the DIGIT_TABLE[200] table; inlined below (verbatim).
//!   - "ryu_common.h"         : the helpers f2s.c references (pow5bits, log10Pow2, log10Pow5,
//!                              copy_special_str, float_to_bits) plus STRICTLY_SHORTEST;
//!                              inlined below as private fns/const.
//!   - "common/d2s_intrinsics.h": f2s.c references none of its helpers directly (the float
//!                              path computes its mulShift inline), so nothing is taken.
//!
//! This is a modification of code taken from github.com/ulfjack/ryu under the
//! terms of the Boost license. Copyright 2018 Ulf Adams; Portions Copyright (c)
//! 2018-2025, PostgreSQL Global Development Group.
//!
//! PUBLIC entry points (used by float4out):
//!   float_to_shortest_decimal_bufn / float_to_shortest_decimal_buf / float_to_shortest_decimal.
//!
//! Pure integer arithmetic over u32/u64. C integer arithmetic wraps; matching
//! wrapping ops are used where the C relies on modular/truncating behavior. The
//! mulShift 64-bit multiply of the float mantissa is exact (no overflow by
//! construction). We translate the 64-bit-platform (non RYU_32_BIT_PLATFORM)
//! path of mulShift, which is the one selected on 64-bit targets.

#![allow(non_snake_case, non_upper_case_globals)]

use crate::prelude::*;

// --- from common/shortest_dec.h ---

/// Maximum length of a shortest-decimal float string (incl. NUL).
const FLOAT_SHORTEST_DECIMAL_LEN: c_int = 16;

// --- from c.h ---
const PG_UINT32_MAX: u64 = u32::MAX as u64;

// --- libc, for the memcpy/memmove/memset the C uses ---
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(dest: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// --- from digit_table.h (verbatim) ---
//
// A table of all two-digit numbers, used to speed up decimal digit generation
// by copying pairs of digits into the final output.
static DIGIT_TABLE: [c_char; 200] = [
    b'0' as c_char, b'0' as c_char, b'0' as c_char, b'1' as c_char, b'0' as c_char, b'2' as c_char, b'0' as c_char, b'3' as c_char, b'0' as c_char, b'4' as c_char, b'0' as c_char, b'5' as c_char, b'0' as c_char, b'6' as c_char, b'0' as c_char, b'7' as c_char, b'0' as c_char, b'8' as c_char, b'0' as c_char, b'9' as c_char,
    b'1' as c_char, b'0' as c_char, b'1' as c_char, b'1' as c_char, b'1' as c_char, b'2' as c_char, b'1' as c_char, b'3' as c_char, b'1' as c_char, b'4' as c_char, b'1' as c_char, b'5' as c_char, b'1' as c_char, b'6' as c_char, b'1' as c_char, b'7' as c_char, b'1' as c_char, b'8' as c_char, b'1' as c_char, b'9' as c_char,
    b'2' as c_char, b'0' as c_char, b'2' as c_char, b'1' as c_char, b'2' as c_char, b'2' as c_char, b'2' as c_char, b'3' as c_char, b'2' as c_char, b'4' as c_char, b'2' as c_char, b'5' as c_char, b'2' as c_char, b'6' as c_char, b'2' as c_char, b'7' as c_char, b'2' as c_char, b'8' as c_char, b'2' as c_char, b'9' as c_char,
    b'3' as c_char, b'0' as c_char, b'3' as c_char, b'1' as c_char, b'3' as c_char, b'2' as c_char, b'3' as c_char, b'3' as c_char, b'3' as c_char, b'4' as c_char, b'3' as c_char, b'5' as c_char, b'3' as c_char, b'6' as c_char, b'3' as c_char, b'7' as c_char, b'3' as c_char, b'8' as c_char, b'3' as c_char, b'9' as c_char,
    b'4' as c_char, b'0' as c_char, b'4' as c_char, b'1' as c_char, b'4' as c_char, b'2' as c_char, b'4' as c_char, b'3' as c_char, b'4' as c_char, b'4' as c_char, b'4' as c_char, b'5' as c_char, b'4' as c_char, b'6' as c_char, b'4' as c_char, b'7' as c_char, b'4' as c_char, b'8' as c_char, b'4' as c_char, b'9' as c_char,
    b'5' as c_char, b'0' as c_char, b'5' as c_char, b'1' as c_char, b'5' as c_char, b'2' as c_char, b'5' as c_char, b'3' as c_char, b'5' as c_char, b'4' as c_char, b'5' as c_char, b'5' as c_char, b'5' as c_char, b'6' as c_char, b'5' as c_char, b'7' as c_char, b'5' as c_char, b'8' as c_char, b'5' as c_char, b'9' as c_char,
    b'6' as c_char, b'0' as c_char, b'6' as c_char, b'1' as c_char, b'6' as c_char, b'2' as c_char, b'6' as c_char, b'3' as c_char, b'6' as c_char, b'4' as c_char, b'6' as c_char, b'5' as c_char, b'6' as c_char, b'6' as c_char, b'6' as c_char, b'7' as c_char, b'6' as c_char, b'8' as c_char, b'6' as c_char, b'9' as c_char,
    b'7' as c_char, b'0' as c_char, b'7' as c_char, b'1' as c_char, b'7' as c_char, b'2' as c_char, b'7' as c_char, b'3' as c_char, b'7' as c_char, b'4' as c_char, b'7' as c_char, b'5' as c_char, b'7' as c_char, b'6' as c_char, b'7' as c_char, b'7' as c_char, b'7' as c_char, b'8' as c_char, b'7' as c_char, b'9' as c_char,
    b'8' as c_char, b'0' as c_char, b'8' as c_char, b'1' as c_char, b'8' as c_char, b'2' as c_char, b'8' as c_char, b'3' as c_char, b'8' as c_char, b'4' as c_char, b'8' as c_char, b'5' as c_char, b'8' as c_char, b'6' as c_char, b'8' as c_char, b'7' as c_char, b'8' as c_char, b'8' as c_char, b'8' as c_char, b'9' as c_char,
    b'9' as c_char, b'0' as c_char, b'9' as c_char, b'1' as c_char, b'9' as c_char, b'2' as c_char, b'9' as c_char, b'3' as c_char, b'9' as c_char, b'4' as c_char, b'9' as c_char, b'5' as c_char, b'9' as c_char, b'6' as c_char, b'9' as c_char, b'7' as c_char, b'9' as c_char, b'8' as c_char, b'9' as c_char, b'9' as c_char,
];

// --- from ryu_common.h ---

/// Upstream Ryu always emits the shortest; PG sets this to 0 to avoid outputting
/// the exact midpoint between two representable floats.
const STRICTLY_SHORTEST: bool = false;

/// Returns e == 0 ? 1 : ceil(log_2(5^e)).
#[inline]
fn pow5bits(e: int32) -> uint32 {
    Assert!(e >= 0);
    Assert!(e <= 3528);
    (((e as uint32).wrapping_mul(1217359)) >> 19).wrapping_add(1)
}

/// Returns floor(log_10(2^e)).
#[inline]
fn log10Pow2(e: int32) -> int32 {
    Assert!(e >= 0);
    Assert!(e <= 1650);
    (((e as uint32).wrapping_mul(78913)) >> 18) as int32
}

/// Returns floor(log_10(5^e)).
#[inline]
fn log10Pow5(e: int32) -> int32 {
    Assert!(e >= 0);
    Assert!(e <= 2620);
    (((e as uint32).wrapping_mul(732923)) >> 20) as int32
}

#[inline]
unsafe fn copy_special_str(result: *mut c_char, sign: bool, exponent: bool, mantissa: bool) -> c_int {
    if mantissa {
        memcpy(result as *mut c_void, b"NaN".as_ptr() as *const c_void, 3);
        return 3;
    }
    if sign {
        *result = b'-' as c_char;
    }
    let s = sign as usize;
    if exponent {
        memcpy(result.add(s) as *mut c_void, b"Infinity".as_ptr() as *const c_void, 8);
        return (s + 8) as c_int;
    }
    *result.add(s) = b'0' as c_char;
    (s + 1) as c_int
}

#[inline]
fn float_to_bits(f: f32) -> uint32 {
    // memcpy(&bits, &f, sizeof(float)); -- a pure bit reinterpretation.
    f.to_bits()
}

// --- f2s.c proper ---

const FLOAT_MANTISSA_BITS: u32 = 23;
const FLOAT_EXPONENT_BITS: u32 = 8;
const FLOAT_BIAS: int32 = 127;

/*
 * This table is generated (by the upstream) by PrintFloatLookupTable,
 * and modified (by us) to add UINT64CONST.
 */
const FLOAT_POW5_INV_BITCOUNT: int32 = 59;
static FLOAT_POW5_INV_SPLIT: [uint64; 31] = [
    576460752303423489, 461168601842738791, 368934881474191033, 295147905179352826,
    472236648286964522, 377789318629571618, 302231454903657294, 483570327845851670,
    386856262276681336, 309485009821345069, 495176015714152110, 396140812571321688,
    316912650057057351, 507060240091291761, 405648192073033409, 324518553658426727,
    519229685853482763, 415383748682786211, 332306998946228969, 531691198313966350,
    425352958651173080, 340282366920938464, 544451787073501542, 435561429658801234,
    348449143727040987, 557518629963265579, 446014903970612463, 356811923176489971,
    570899077082383953, 456719261665907162, 365375409332725730,
];
const FLOAT_POW5_BITCOUNT: int32 = 61;
static FLOAT_POW5_SPLIT: [uint64; 47] = [
    1152921504606846976, 1441151880758558720, 1801439850948198400, 2251799813685248000,
    1407374883553280000, 1759218604441600000, 2199023255552000000, 1374389534720000000,
    1717986918400000000, 2147483648000000000, 1342177280000000000, 1677721600000000000,
    2097152000000000000, 1310720000000000000, 1638400000000000000, 2048000000000000000,
    1280000000000000000, 1600000000000000000, 2000000000000000000, 1250000000000000000,
    1562500000000000000, 1953125000000000000, 1220703125000000000, 1525878906250000000,
    1907348632812500000, 1192092895507812500, 1490116119384765625, 1862645149230957031,
    1164153218269348144, 1455191522836685180, 1818989403545856475, 2273736754432320594,
    1421085471520200371, 1776356839400250464, 2220446049250313080, 1387778780781445675,
    1734723475976807094, 2168404344971008868, 1355252715606880542, 1694065894508600678,
    2117582368135750847, 1323488980084844279, 1654361225106055349, 2067951531382569187,
    1292469707114105741, 1615587133892632177, 2019483917365790221,
];

#[inline]
fn pow5Factor(mut value: uint32) -> uint32 {
    let mut count: uint32 = 0;
    loop {
        Assert!(value != 0);
        let q = value / 5;
        let r = value % 5;
        if r != 0 {
            break;
        }
        value = q;
        count += 1;
    }
    count
}

/// Returns true if value is divisible by 5^p.
#[inline]
fn multipleOfPowerOf5(value: uint32, p: uint32) -> bool {
    pow5Factor(value) >= p
}

/// Returns true if value is divisible by 2^p.
#[inline]
fn multipleOfPowerOf2(value: uint32, p: uint32) -> bool {
    /* return __builtin_ctz(value) >= p; */
    (value & ((1u32 << p).wrapping_sub(1))) == 0
}

/*
 * It seems to be slightly faster to avoid uint128_t here, although the
 * generated code for uint128_t looks slightly nicer. (64-bit-platform path.)
 */
#[inline]
fn mulShift(m: uint32, factor: uint64, shift: int32) -> uint32 {
    let factorLo = factor as uint32;
    let factorHi = (factor >> 32) as uint32;
    let bits0: uint64 = (m as uint64) * (factorLo as uint64);
    let bits1: uint64 = (m as uint64) * (factorHi as uint64);

    Assert!(shift > 32);

    let sum: uint64 = (bits0 >> 32) + bits1;
    let shiftedSum: uint64 = sum >> (shift - 32);
    Assert!(shiftedSum <= PG_UINT32_MAX);
    shiftedSum as uint32
}

#[inline]
fn mulPow5InvDivPow2(m: uint32, q: uint32, j: int32) -> uint32 {
    mulShift(m, FLOAT_POW5_INV_SPLIT[q as usize], j)
}

#[inline]
fn mulPow5divPow2(m: uint32, i: uint32, j: int32) -> uint32 {
    mulShift(m, FLOAT_POW5_SPLIT[i as usize], j)
}

#[inline]
fn decimalLength(v: uint32) -> uint32 {
    /* Function precondition: v is not a 10-digit number. */
    /* (9 digits are sufficient for round-tripping.) */
    Assert!(v < 1000000000);
    if v >= 100000000 {
        return 9;
    }
    if v >= 10000000 {
        return 8;
    }
    if v >= 1000000 {
        return 7;
    }
    if v >= 100000 {
        return 6;
    }
    if v >= 10000 {
        return 5;
    }
    if v >= 1000 {
        return 4;
    }
    if v >= 100 {
        return 3;
    }
    if v >= 10 {
        return 2;
    }
    1
}

/// A floating decimal representing m * 10^e.
#[derive(Clone, Copy)]
struct floating_decimal_32 {
    mantissa: uint32,
    exponent: int32,
}

fn f2d(ieeeMantissa: uint32, ieeeExponent: uint32) -> floating_decimal_32 {
    let e2: int32;
    let m2: uint32;

    if ieeeExponent == 0 {
        /* We subtract 2 so that the bounds computation has 2 additional bits. */
        e2 = 1 - FLOAT_BIAS - (FLOAT_MANTISSA_BITS as int32) - 2;
        m2 = ieeeMantissa;
    } else {
        e2 = (ieeeExponent as int32) - FLOAT_BIAS - (FLOAT_MANTISSA_BITS as int32) - 2;
        m2 = (1u32 << FLOAT_MANTISSA_BITS) | ieeeMantissa;
    }

    let acceptBounds: bool = if STRICTLY_SHORTEST {
        (m2 & 1) == 0
    } else {
        false
    };

    /* Step 2: Determine the interval of legal decimal representations. */
    let mv: uint32 = 4u32.wrapping_mul(m2);
    let mp: uint32 = 4u32.wrapping_mul(m2).wrapping_add(2);

    /* Implicit bool -> int conversion. True is 1, false is 0. */
    let mmShift: uint32 = (ieeeMantissa != 0 || ieeeExponent <= 1) as uint32;
    let mm: uint32 = 4u32.wrapping_mul(m2).wrapping_sub(1).wrapping_sub(mmShift);

    /* Step 3: Convert to a decimal power base using 64-bit arithmetic. */
    let mut vr: uint32;
    let mut vp: uint32;
    let mut vm: uint32;
    let e10: int32;
    let mut vmIsTrailingZeros = false;
    let mut vrIsTrailingZeros = false;
    let mut lastRemovedDigit: uint8 = 0;

    if e2 >= 0 {
        let q: uint32 = log10Pow2(e2) as uint32;
        e10 = q as int32;

        let k: int32 = FLOAT_POW5_INV_BITCOUNT + (pow5bits(q as int32) as int32) - 1;
        let i: int32 = -e2 + (q as int32) + k;

        vr = mulPow5InvDivPow2(mv, q, i);
        vp = mulPow5InvDivPow2(mp, q, i);
        vm = mulPow5InvDivPow2(mm, q, i);

        if q != 0 && (vp - 1) / 10 <= vm / 10 {
            /*
             * We need to know one removed digit even if we are not going to
             * loop below.
             */
            let l: int32 = FLOAT_POW5_INV_BITCOUNT + (pow5bits((q - 1) as int32) as int32) - 1;
            lastRemovedDigit =
                (mulPow5InvDivPow2(mv, q - 1, -e2 + (q as int32) - 1 + l) % 10) as uint8;
        }
        if q <= 9 {
            /*
             * The largest power of 5 that fits in 24 bits is 5^10, but q <= 9
             * seems to be safe as well. Only one of mp, mv, and mm can be a
             * multiple of 5, if any.
             */
            if mv % 5 == 0 {
                vrIsTrailingZeros = multipleOfPowerOf5(mv, q);
            } else if acceptBounds {
                vmIsTrailingZeros = multipleOfPowerOf5(mm, q);
            } else {
                vp -= multipleOfPowerOf5(mp, q) as uint32;
            }
        }
    } else {
        let q: uint32 = log10Pow5(-e2) as uint32;
        e10 = (q as int32) + e2;

        let i: int32 = -e2 - (q as int32);
        let k: int32 = (pow5bits(i) as int32) - FLOAT_POW5_BITCOUNT;
        let mut j: int32 = (q as int32) - k;

        vr = mulPow5divPow2(mv, i as uint32, j);
        vp = mulPow5divPow2(mp, i as uint32, j);
        vm = mulPow5divPow2(mm, i as uint32, j);

        if q != 0 && (vp - 1) / 10 <= vm / 10 {
            j = (q as int32) - 1 - ((pow5bits(i + 1) as int32) - FLOAT_POW5_BITCOUNT);
            lastRemovedDigit = (mulPow5divPow2(mv, (i + 1) as uint32, j) % 10) as uint8;
        }
        if q <= 1 {
            /*
             * {vr,vp,vm} is trailing zeros if {mv,mp,mm} has at least q
             * trailing 0 bits.
             */
            /* mv = 4 * m2, so it always has at least two trailing 0 bits. */
            vrIsTrailingZeros = true;
            if acceptBounds {
                /*
                 * mm = mv - 1 - mmShift, so it has 1 trailing 0 bit iff
                 * mmShift == 1.
                 */
                vmIsTrailingZeros = mmShift == 1;
            } else {
                /*
                 * mp = mv + 2, so it always has at least one trailing 0 bit.
                 */
                vp -= 1;
            }
        } else if q < 31 {
            /* TODO(ulfjack):Use a tighter bound here. */
            vrIsTrailingZeros = multipleOfPowerOf2(mv, q - 1);
        }
    }

    /*
     * Step 4: Find the shortest decimal representation in the interval of
     * legal representations.
     */
    let mut removed: uint32 = 0;
    let output: uint32;

    if vmIsTrailingZeros || vrIsTrailingZeros {
        /* General case, which happens rarely (~4.0%). */
        while vp / 10 > vm / 10 {
            vmIsTrailingZeros &= vm - (vm / 10) * 10 == 0;
            vrIsTrailingZeros &= lastRemovedDigit == 0;
            lastRemovedDigit = (vr % 10) as uint8;
            vr /= 10;
            vp /= 10;
            vm /= 10;
            removed += 1;
        }
        if vmIsTrailingZeros {
            while vm % 10 == 0 {
                vrIsTrailingZeros &= lastRemovedDigit == 0;
                lastRemovedDigit = (vr % 10) as uint8;
                vr /= 10;
                vp /= 10;
                vm /= 10;
                removed += 1;
            }
        }

        if vrIsTrailingZeros && lastRemovedDigit == 5 && vr % 2 == 0 {
            /* Round even if the exact number is .....50..0. */
            lastRemovedDigit = 4;
        }

        /*
         * We need to take vr + 1 if vr is outside bounds or we need to round
         * up.
         */
        output = vr
            + (((vr == vm && (!acceptBounds || !vmIsTrailingZeros)) || lastRemovedDigit >= 5)
                as uint32);
    } else {
        /*
         * Specialized for the common case (~96.0%).
         */
        while vp / 10 > vm / 10 {
            lastRemovedDigit = (vr % 10) as uint8;
            vr /= 10;
            vp /= 10;
            vm /= 10;
            removed += 1;
        }

        /*
         * We need to take vr + 1 if vr is outside bounds or we need to round
         * up.
         */
        output = vr + ((vr == vm || lastRemovedDigit >= 5) as uint32);
    }

    let exp: int32 = e10 + (removed as int32);

    floating_decimal_32 {
        exponent: exp,
        mantissa: output,
    }
}

unsafe fn to_chars_f(v: floating_decimal_32, olength: uint32, result: *mut c_char) -> c_int {
    /* Step 5: Print the decimal representation. */
    let mut index: int32 = 0;

    let mut output: uint32 = v.mantissa;
    let exp: int32 = v.exponent;

    /*----
     * On entry, mantissa * 10^exp is the result to be output.
     * Caller has already done the - sign if needed.
     *
     * We want to insert the point somewhere depending on the output length
     * and exponent, which might mean adding zeros:
     *
     *            exp  | format
     *            1+   |  ddddddddd000000
     *            0    |  ddddddddd
     *  -1 .. -len+1   |  dddddddd.d to d.ddddddddd
     *  -len ...       |  0.ddddddddd to 0.000dddddd
     */
    let mut i: uint32 = 0;
    let nexp: int32 = exp + (olength as int32);

    if nexp <= 0 {
        /* -nexp is number of 0s to add after '.' */
        Assert!(nexp >= -3);
        /* 0.000ddddd */
        index = 2 - nexp;
        /* copy 8 bytes rather than 5 to let compiler optimize */
        memcpy(result as *mut c_void, b"0.000000".as_ptr() as *const c_void, 8);
    } else if exp < 0 {
        /*
         * dddd.dddd; leave space at the start and move the '.' in after
         */
        index = 1;
    } else {
        /*
         * Pre-fill with zeros. No more than 6 output digits in this form.
         */
        Assert!(exp < 6 && exp + (olength as int32) <= 6);
        memset(result as *mut c_void, b'0' as c_int, 8);
    }

    while output >= 10000 {
        let c: uint32 = output - 10000 * (output / 10000);
        let c0: uint32 = (c % 100) << 1;
        let c1: uint32 = (c / 100) << 1;

        output /= 10000;

        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 2) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(c0 as usize) as *const c_void,
            2,
        );
        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 4) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(c1 as usize) as *const c_void,
            2,
        );
        i += 4;
    }
    if output >= 100 {
        let c: uint32 = (output % 100) << 1;
        output /= 100;
        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 2) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(c as usize) as *const c_void,
            2,
        );
        i += 2;
    }
    if output >= 10 {
        let c: uint32 = output << 1;
        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 2) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(c as usize) as *const c_void,
            2,
        );
    } else {
        *result.offset(index as isize) = (b'0' + output as u8) as c_char;
    }

    if index == 1 {
        /*
         * nexp is 1..6 here, representing the number of digits before the
         * point.
         */
        Assert!(nexp < 7);
        /* gcc only seems to want to optimize memmove for small 2^n */
        if nexp & 4 != 0 {
            memmove(
                result.offset((index - 1) as isize) as *mut c_void,
                result.offset(index as isize) as *const c_void,
                4,
            );
            index += 4;
        }
        if nexp & 2 != 0 {
            memmove(
                result.offset((index - 1) as isize) as *mut c_void,
                result.offset(index as isize) as *const c_void,
                2,
            );
            index += 2;
        }
        if nexp & 1 != 0 {
            *result.offset((index - 1) as isize) = *result.offset(index as isize);
        }
        *result.offset(nexp as isize) = b'.' as c_char;
        index = (olength as int32) + 1;
    } else if exp >= 0 {
        /* we supplied the trailing zeros earlier, now just set the length. */
        index = (olength as int32) + exp;
    } else {
        index = (olength as int32) + (2 - nexp);
    }

    index
}

unsafe fn to_chars(v: floating_decimal_32, sign: bool, result: *mut c_char) -> c_int {
    /* Step 5: Print the decimal representation. */
    let mut index: int32 = 0;

    let mut output: uint32 = v.mantissa;
    let mut olength: uint32 = decimalLength(output);
    let mut exp: int32 = v.exponent + (olength as int32) - 1;

    if sign {
        *result.offset(index as isize) = b'-' as c_char;
        index += 1;
    }

    /*
     * The thresholds for fixed-point output are chosen to match printf
     * defaults.
     */
    if exp >= -4 && exp < 6 {
        return to_chars_f(v, olength, result.offset(index as isize)) + (sign as c_int);
    }

    /*
     * If v.exponent is exactly 0, we might have reached here via the small
     * integer fast path, in which case v.mantissa might contain trailing
     * (decimal) zeros. For scientific notation we need to move these zeros
     * into the exponent.
     */
    if v.exponent == 0 {
        while (output & 1) == 0 {
            let q: uint32 = output / 10;
            let r: uint32 = output - 10 * q;
            if r != 0 {
                break;
            }
            output = q;
            olength -= 1;
        }
    }

    /*----
     * Print the decimal digits.
     */
    let mut i: uint32 = 0;

    while output >= 10000 {
        let c: uint32 = output - 10000 * (output / 10000);
        let c0: uint32 = (c % 100) << 1;
        let c1: uint32 = (c / 100) << 1;

        output /= 10000;

        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 1) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(c0 as usize) as *const c_void,
            2,
        );
        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 3) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(c1 as usize) as *const c_void,
            2,
        );
        i += 4;
    }
    if output >= 100 {
        let c: uint32 = (output % 100) << 1;
        output /= 100;
        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 1) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(c as usize) as *const c_void,
            2,
        );
        i += 2;
    }
    if output >= 10 {
        let c: uint32 = output << 1;
        /*
         * We can't use memcpy here: the decimal dot goes between these two
         * digits.
         */
        *result.offset((index + (olength as int32) - (i as int32)) as isize) =
            DIGIT_TABLE[(c + 1) as usize];
        *result.offset(index as isize) = DIGIT_TABLE[c as usize];
    } else {
        *result.offset(index as isize) = (b'0' + output as u8) as c_char;
    }

    /* Print decimal point if needed. */
    if olength > 1 {
        *result.offset((index + 1) as isize) = b'.' as c_char;
        index += (olength as int32) + 1;
    } else {
        index += 1;
    }

    /* Print the exponent. */
    *result.offset(index as isize) = b'e' as c_char;
    index += 1;
    if exp < 0 {
        *result.offset(index as isize) = b'-' as c_char;
        index += 1;
        exp = -exp;
    } else {
        *result.offset(index as isize) = b'+' as c_char;
        index += 1;
    }

    memcpy(
        result.offset(index as isize) as *mut c_void,
        DIGIT_TABLE.as_ptr().add((2 * exp) as usize) as *const c_void,
        2,
    );
    index += 2;

    index
}

fn f2d_small_int(
    ieeeMantissa: uint32,
    ieeeExponent: uint32,
    v: &mut floating_decimal_32,
) -> bool {
    let e2: int32 = (ieeeExponent as int32) - FLOAT_BIAS - (FLOAT_MANTISSA_BITS as int32);

    /*
     * Avoid using multiple "return false;" here since it tends to provoke the
     * compiler into inlining multiple copies of f2d, which is undesirable.
     */
    if e2 >= -(FLOAT_MANTISSA_BITS as int32) && e2 <= 0 {
        /*----
         * Since 2^23 <= m2 < 2^24 and 0 <= -e2 <= 23: 1 <= f < 2^24.
         */
        let mask: uint32 = (1u32 << (-e2)) - 1;
        let fraction: uint32 = ieeeMantissa & mask;

        if fraction == 0 {
            /*----
             * f is an integer in the range [1, 2^24).
             */
            let m2: uint32 = (1u32 << FLOAT_MANTISSA_BITS) | ieeeMantissa;
            v.mantissa = m2 >> (-e2);
            v.exponent = 0;
            return true;
        }
    }

    false
}

/// Store the shortest decimal representation of the given float as an
/// UNTERMINATED string in the caller's supplied buffer (which must be at least
/// FLOAT_SHORTEST_DECIMAL_LEN-1 bytes long). Returns the number of bytes stored.
#[no_mangle]
pub unsafe extern "C" fn float_to_shortest_decimal_bufn(f: f32, result: *mut c_char) -> c_int {
    /*
     * Step 1: Decode the floating-point number, and unify normalized and
     * subnormal cases.
     */
    let bits: uint32 = float_to_bits(f);

    /* Decode bits into sign, mantissa, and exponent. */
    let ieeeSign: bool = ((bits >> (FLOAT_MANTISSA_BITS + FLOAT_EXPONENT_BITS)) & 1) != 0;
    let ieeeMantissa: uint32 = bits & ((1u32 << FLOAT_MANTISSA_BITS) - 1);
    let ieeeExponent: uint32 = (bits >> FLOAT_MANTISSA_BITS) & ((1u32 << FLOAT_EXPONENT_BITS) - 1);

    /* Case distinction; exit early for the easy cases. */
    if ieeeExponent == ((1u32 << FLOAT_EXPONENT_BITS) - 1u32)
        || (ieeeExponent == 0 && ieeeMantissa == 0)
    {
        return copy_special_str(result, ieeeSign, ieeeExponent != 0, ieeeMantissa != 0);
    }

    let mut v = floating_decimal_32 {
        mantissa: 0,
        exponent: 0,
    };
    let isSmallInt: bool = f2d_small_int(ieeeMantissa, ieeeExponent, &mut v);

    if !isSmallInt {
        v = f2d(ieeeMantissa, ieeeExponent);
    }

    to_chars(v, ieeeSign, result)
}

/// Store the shortest decimal representation of the given float as a
/// null-terminated string in the caller's supplied buffer (which must be at
/// least FLOAT_SHORTEST_DECIMAL_LEN bytes long). Returns the string length.
#[no_mangle]
pub unsafe extern "C" fn float_to_shortest_decimal_buf(f: f32, result: *mut c_char) -> c_int {
    let index: c_int = float_to_shortest_decimal_bufn(f, result);

    /* Terminate the string. */
    Assert!(index < FLOAT_SHORTEST_DECIMAL_LEN);
    *result.offset(index as isize) = b'\0' as c_char;
    index
}

/// Return the shortest decimal representation as a null-terminated palloc'd
/// string. Caller is responsible for freeing the result.
#[no_mangle]
pub unsafe extern "C" fn float_to_shortest_decimal(f: f32) -> *mut c_char {
    let result: *mut c_char = palloc(FLOAT_SHORTEST_DECIMAL_LEN as Size) as *mut c_char;
    float_to_shortest_decimal_buf(f, result);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: run the real algorithm into a buffer and return the produced String.
    fn shortest(f: f32) -> String {
        let mut buf = [0u8; 32];
        let len = unsafe { float_to_shortest_decimal_buf(f, buf.as_mut_ptr() as *mut c_char) };
        assert!(len >= 0 && (len as usize) < buf.len());
        String::from_utf8(buf[..len as usize].to_vec()).unwrap()
    }

    #[test]
    fn test_basic_values() {
        assert_eq!(shortest(1.5), "1.5");
        assert_eq!(shortest(0.0), "0");
        assert_eq!(shortest(3.0), "3");
    }

    #[test]
    fn test_special_values() {
        assert_eq!(shortest(f32::INFINITY), "Infinity");
        assert_eq!(shortest(f32::NEG_INFINITY), "-Infinity");
        assert_eq!(shortest(f32::NAN), "NaN");
        assert_eq!(shortest(-0.0), "-0");
    }

    #[test]
    fn test_scientific_and_fixed() {
        // 3.0e9 is outside the fixed-point window -> scientific notation.
        assert_eq!(shortest(3.0e9), "3e+09");
        // 1e-7 is below the fixed-point window -> scientific notation.
        assert_eq!(shortest(1e-7), "1e-07");
    }

    #[test]
    fn test_negative_and_fraction() {
        assert_eq!(shortest(-1.5), "-1.5");
        assert_eq!(shortest(0.5), "0.5");
        assert_eq!(shortest(0.001), "0.001");
        assert_eq!(shortest(100.0), "100");
    }

    #[test]
    fn test_round_trip_parseable() {
        // The output must parse back to the exact same float bits.
        let cases: [f32; 10] = [
            1.5, 3.0, 0.1, 123.456, 1e10, 1e-10, 12345.678, 0.0001234,
            3.4028235e38_f32, // ~FLT_MAX
            1.1754944e-38_f32, // ~smallest normal
        ];
        for &c in &cases {
            let s = shortest(c);
            let parsed: f32 = s.parse().unwrap_or_else(|_| panic!("parse {s}"));
            assert_eq!(parsed.to_bits(), c.to_bits(), "round-trip {c} -> {s}");
        }
    }

    #[test]
    fn test_helpers() {
        assert_eq!(pow5bits(0), 1);
        assert_eq!(log10Pow2(0), 0);
        assert_eq!(log10Pow5(0), 0);
        assert_eq!(decimalLength(0), 1);
        assert_eq!(decimalLength(999999999), 9);
        assert!(multipleOfPowerOf2(8, 3));
        assert!(!multipleOfPowerOf2(8, 4));
        assert!(multipleOfPowerOf5(25, 2));
        assert!(!multipleOfPowerOf5(25, 3));
        assert_eq!(pow5Factor(125), 3);
    }
}
