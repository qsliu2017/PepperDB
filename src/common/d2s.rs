//! Translation of postgres/src/common/d2s.c
//!
//! Ryu floating-point output for double precision (f64 -> shortest decimal).
//!
//! #include mapping:
//!   - "common/shortest_dec.h" : only DOUBLE_SHORTEST_DECIMAL_LEN (=25) is used; inlined below.
//!   - "digit_table.h"         : the DIGIT_TABLE[200] table; inlined below (verbatim).
//!   - "d2s_full_table.h"      : the two big POW5 split tables. These are ALREADY GENERATED and
//!                               imported from crate::common::d2s_full_table (DOUBLE_POW5_INV_SPLIT,
//!                               DOUBLE_POW5_SPLIT), each `pub static NAME: [[u64; 2]; N]`.
//!   - "common/d2s_intrinsics.h": div5/div10/div100/div1e8 and mulShift/mulShiftAll (the
//!                               HAVE_INT128 path, which is the one selected on 64-bit targets).
//!   - "ryu_common.h"          : the helpers d2s.c references (pow5bits, log10Pow2, log10Pow5,
//!                               copy_special_str, double_to_bits) plus STRICTLY_SHORTEST;
//!                               inlined below as private fns/const (shared with f2s.rs).
//!
//! This is a modification of code taken from github.com/ulfjack/ryu under the
//! terms of the Boost license. Copyright 2018 Ulf Adams; Portions Copyright (c)
//! 2018-2025, PostgreSQL Global Development Group.
//!
//! PUBLIC entry points (used by float8out):
//!   double_to_shortest_decimal_bufn / double_to_shortest_decimal_buf / double_to_shortest_decimal.
//!
//! Pure integer arithmetic over u32/u64/u128. C integer arithmetic wraps; matching
//! wrapping ops are used where the C relies on modular/truncating behavior (the
//! m/mp/mm/vr computations). The mulShift 64x128 product is EXACT via u128 (no
//! overflow by construction). We translate the HAVE_INT128 path of the intrinsics,
//! which is the one selected on 64-bit targets.

#![allow(non_snake_case, non_upper_case_globals)]

use crate::common::d2s_full_table::{DOUBLE_POW5_INV_SPLIT, DOUBLE_POW5_SPLIT};
use crate::prelude::*;

// --- from common/shortest_dec.h ---

/// Maximum length of a shortest-decimal double string (incl. NUL).
const DOUBLE_SHORTEST_DECIMAL_LEN: c_int = 25;

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
fn double_to_bits(d: f64) -> uint64 {
    // memcpy(&bits, &d, sizeof(double)); -- a pure bit reinterpretation.
    d.to_bits()
}

// --- from common/d2s_intrinsics.h (HAVE_INT128 / RYU_32_BIT_PLATFORM=off path) ---

#[inline]
fn div5(x: uint64) -> uint64 {
    x / 5
}

#[inline]
fn div10(x: uint64) -> uint64 {
    x / 10
}

#[inline]
fn div100(x: uint64) -> uint64 {
    x / 100
}

#[inline]
fn div1e8(x: uint64) -> uint64 {
    x / 100000000
}

/*
 * Best case: use 128-bit type.
 *
 * The 64x128-bit product is exact (no overflow): m has at most 55 significant
 * bits and each mul[] limb at most 64, so each partial product fits in u128.
 */
#[inline]
fn mulShift(m: uint64, mul: &[uint64; 2], j: int32) -> uint64 {
    let b0: u128 = (m as u128) * (mul[0] as u128);
    let b2: u128 = (m as u128) * (mul[1] as u128);

    (((b0 >> 64) + b2) >> (j - 64)) as uint64
}

#[inline]
fn mulShiftAll(
    m: uint64,
    mul: &[uint64; 2],
    j: int32,
    vp: &mut uint64,
    vm: &mut uint64,
    mmShift: uint32,
) -> uint64 {
    *vp = mulShift(4u64.wrapping_mul(m).wrapping_add(2), mul, j);
    *vm = mulShift(
        4u64.wrapping_mul(m).wrapping_sub(1).wrapping_sub(mmShift as uint64),
        mul,
        j,
    );
    mulShift(4u64.wrapping_mul(m), mul, j)
}

// --- d2s.c proper ---

const DOUBLE_MANTISSA_BITS: u32 = 52;
const DOUBLE_EXPONENT_BITS: u32 = 11;
const DOUBLE_BIAS: int32 = 1023;

const DOUBLE_POW5_INV_BITCOUNT: int32 = 122;
const DOUBLE_POW5_BITCOUNT: int32 = 121;

#[inline]
fn pow5Factor(mut value: uint64) -> uint32 {
    let mut count: uint32 = 0;
    loop {
        Assert!(value != 0);
        let q = div5(value);
        let r = (value.wrapping_sub(5u64.wrapping_mul(q))) as uint32;

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
fn multipleOfPowerOf5(value: uint64, p: uint32) -> bool {
    pow5Factor(value) >= p
}

/// Returns true if value is divisible by 2^p.
#[inline]
fn multipleOfPowerOf2(value: uint64, p: uint32) -> bool {
    /* return __builtin_ctzll(value) >= p; */
    (value & ((1u64 << p).wrapping_sub(1))) == 0
}

#[inline]
fn decimalLength(v: uint64) -> uint32 {
    /* This is slightly faster than a loop. */
    /* The average output length is 16.38 digits, so we check high-to-low. */
    /* Function precondition: v is not an 18, 19, or 20-digit number. */
    /* (17 digits are sufficient for round-tripping.) */
    Assert!(v < 100000000000000000u64);
    if v >= 10000000000000000u64 {
        return 17;
    }
    if v >= 1000000000000000u64 {
        return 16;
    }
    if v >= 100000000000000u64 {
        return 15;
    }
    if v >= 10000000000000u64 {
        return 14;
    }
    if v >= 1000000000000u64 {
        return 13;
    }
    if v >= 100000000000u64 {
        return 12;
    }
    if v >= 10000000000u64 {
        return 11;
    }
    if v >= 1000000000u64 {
        return 10;
    }
    if v >= 100000000u64 {
        return 9;
    }
    if v >= 10000000u64 {
        return 8;
    }
    if v >= 1000000u64 {
        return 7;
    }
    if v >= 100000u64 {
        return 6;
    }
    if v >= 10000u64 {
        return 5;
    }
    if v >= 1000u64 {
        return 4;
    }
    if v >= 100u64 {
        return 3;
    }
    if v >= 10u64 {
        return 2;
    }
    1
}

/// A floating decimal representing m * 10^e.
#[derive(Clone, Copy)]
struct floating_decimal_64 {
    mantissa: uint64,
    exponent: int32,
}

fn d2d(ieeeMantissa: uint64, ieeeExponent: uint32) -> floating_decimal_64 {
    let e2: int32;
    let m2: uint64;

    if ieeeExponent == 0 {
        /* We subtract 2 so that the bounds computation has 2 additional bits. */
        e2 = 1 - DOUBLE_BIAS - (DOUBLE_MANTISSA_BITS as int32) - 2;
        m2 = ieeeMantissa;
    } else {
        e2 = (ieeeExponent as int32) - DOUBLE_BIAS - (DOUBLE_MANTISSA_BITS as int32) - 2;
        m2 = (1u64 << DOUBLE_MANTISSA_BITS) | ieeeMantissa;
    }

    let acceptBounds: bool = if STRICTLY_SHORTEST {
        (m2 & 1) == 0
    } else {
        false
    };

    /* Step 2: Determine the interval of legal decimal representations. */
    let mv: uint64 = 4u64.wrapping_mul(m2);

    /* Implicit bool -> int conversion. True is 1, false is 0. */
    let mmShift: uint32 = (ieeeMantissa != 0 || ieeeExponent <= 1) as uint32;

    /* We would compute mp and mm like this: */
    /* uint64 mp = 4 * m2 + 2; */
    /* uint64 mm = mv - 1 - mmShift; */

    /* Step 3: Convert to a decimal power base using 128-bit arithmetic. */
    let mut vr: uint64;
    let mut vp: uint64 = 0;
    let mut vm: uint64 = 0;
    let e10: int32;
    let mut vmIsTrailingZeros = false;
    let mut vrIsTrailingZeros = false;

    if e2 >= 0 {
        /*
         * I tried special-casing q == 0, but there was no effect on
         * performance.
         *
         * This expr is slightly faster than max(0, log10Pow2(e2) - 1).
         */
        let q: uint32 = (log10Pow2(e2) - (e2 > 3) as int32) as uint32;
        let k: int32 = DOUBLE_POW5_INV_BITCOUNT + (pow5bits(q as int32) as int32) - 1;
        let i: int32 = -e2 + (q as int32) + k;

        e10 = q as int32;

        vr = mulShiftAll(m2, &DOUBLE_POW5_INV_SPLIT[q as usize], i, &mut vp, &mut vm, mmShift);

        if q <= 21 {
            /*
             * This should use q <= 22, but I think 21 is also safe. Smaller
             * values may still be safe, but it's more difficult to reason
             * about them.
             *
             * Only one of mp, mv, and mm can be a multiple of 5, if any.
             */
            let mvMod5: uint32 = (mv.wrapping_sub(5u64.wrapping_mul(div5(mv)))) as uint32;

            if mvMod5 == 0 {
                vrIsTrailingZeros = multipleOfPowerOf5(mv, q);
            } else if acceptBounds {
                /*----
                 * Same as min(e2 + (~mm & 1), pow5Factor(mm)) >= q
                 * <=> e2 + (~mm & 1) >= q && pow5Factor(mm) >= q
                 * <=> true && pow5Factor(mm) >= q, since e2 >= q.
                 *----
                 */
                vmIsTrailingZeros =
                    multipleOfPowerOf5(mv.wrapping_sub(1).wrapping_sub(mmShift as uint64), q);
            } else {
                /* Same as min(e2 + 1, pow5Factor(mp)) >= q. */
                vp -= multipleOfPowerOf5(mv.wrapping_add(2), q) as uint64;
            }
        }
    } else {
        /*
         * This expression is slightly faster than max(0, log10Pow5(-e2) - 1).
         */
        let q: uint32 = (log10Pow5(-e2) - (-e2 > 1) as int32) as uint32;
        let i: int32 = -e2 - (q as int32);
        let k: int32 = (pow5bits(i) as int32) - DOUBLE_POW5_BITCOUNT;
        let j: int32 = (q as int32) - k;

        e10 = (q as int32) + e2;

        vr = mulShiftAll(m2, &DOUBLE_POW5_SPLIT[i as usize], j, &mut vp, &mut vm, mmShift);

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
        } else if q < 63 {
            /* TODO(ulfjack):Use a tighter bound here. */
            /*
             * We need to compute min(ntz(mv), pow5Factor(mv) - e2) >= q - 1
             * <=> ntz(mv) >= q - 1 && pow5Factor(mv) - e2 >= q - 1
             * <=> ntz(mv) >= q - 1 (e2 is negative and -e2 >= q)
             * <=> (mv & ((1 << (q - 1)) - 1)) == 0
             *
             * We also need to make sure that the left shift does not overflow.
             */
            vrIsTrailingZeros = multipleOfPowerOf2(mv, q - 1);
        }
    }

    /*
     * Step 4: Find the shortest decimal representation in the interval of
     * legal representations.
     */
    let mut removed: uint32 = 0;
    let mut lastRemovedDigit: uint8 = 0;
    let output: uint64;

    /* On average, we remove ~2 digits. */
    if vmIsTrailingZeros || vrIsTrailingZeros {
        /* General case, which happens rarely (~0.7%). */
        loop {
            let vpDiv10: uint64 = div10(vp);
            let vmDiv10: uint64 = div10(vm);

            if vpDiv10 <= vmDiv10 {
                break;
            }

            let vmMod10: uint32 = (vm.wrapping_sub(10u64.wrapping_mul(vmDiv10))) as uint32;
            let vrDiv10: uint64 = div10(vr);
            let vrMod10: uint32 = (vr.wrapping_sub(10u64.wrapping_mul(vrDiv10))) as uint32;

            vmIsTrailingZeros &= vmMod10 == 0;
            vrIsTrailingZeros &= lastRemovedDigit == 0;
            lastRemovedDigit = vrMod10 as uint8;
            vr = vrDiv10;
            vp = vpDiv10;
            vm = vmDiv10;
            removed += 1;
        }

        if vmIsTrailingZeros {
            loop {
                let vmDiv10: uint64 = div10(vm);
                let vmMod10: uint32 = (vm.wrapping_sub(10u64.wrapping_mul(vmDiv10))) as uint32;

                if vmMod10 != 0 {
                    break;
                }

                let vpDiv10: uint64 = div10(vp);
                let vrDiv10: uint64 = div10(vr);
                let vrMod10: uint32 = (vr.wrapping_sub(10u64.wrapping_mul(vrDiv10))) as uint32;

                vrIsTrailingZeros &= lastRemovedDigit == 0;
                lastRemovedDigit = vrMod10 as uint8;
                vr = vrDiv10;
                vp = vpDiv10;
                vm = vmDiv10;
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
                as uint64);
    } else {
        /*
         * Specialized for the common case (~99.3%). Percentages below are
         * relative to this.
         */
        let mut roundUp = false;
        let vpDiv100: uint64 = div100(vp);
        let vmDiv100: uint64 = div100(vm);

        if vpDiv100 > vmDiv100 {
            /* Optimization:remove two digits at a time(~86.2 %). */
            let vrDiv100: uint64 = div100(vr);
            let vrMod100: uint32 = (vr.wrapping_sub(100u64.wrapping_mul(vrDiv100))) as uint32;

            roundUp = vrMod100 >= 50;
            vr = vrDiv100;
            vp = vpDiv100;
            vm = vmDiv100;
            removed += 2;
        }

        /*----
         * Loop iterations below (approximately), without optimization above:
         *
         * 0: 0.03%, 1: 13.8%, 2: 70.6%, 3: 14.0%, 4: 1.40%, 5: 0.14%, 6+: 0.02%
         *
         * Loop iterations below (approximately), with optimization above:
         *
         * 0: 70.6%, 1: 27.8%, 2: 1.40%, 3: 0.14%, 4+: 0.02%
         *----
         */
        loop {
            let vpDiv10: uint64 = div10(vp);
            let vmDiv10: uint64 = div10(vm);

            if vpDiv10 <= vmDiv10 {
                break;
            }

            let vrDiv10: uint64 = div10(vr);
            let vrMod10: uint32 = (vr.wrapping_sub(10u64.wrapping_mul(vrDiv10))) as uint32;

            roundUp = vrMod10 >= 5;
            vr = vrDiv10;
            vp = vpDiv10;
            vm = vmDiv10;
            removed += 1;
        }

        /*
         * We need to take vr + 1 if vr is outside bounds or we need to round
         * up.
         */
        output = vr + ((vr == vm || roundUp) as uint64);
    }

    let exp: int32 = e10 + (removed as int32);

    floating_decimal_64 {
        exponent: exp,
        mantissa: output,
    }
}

unsafe fn to_chars_df(v: floating_decimal_64, olength: uint32, result: *mut c_char) -> c_int {
    /* Step 5: Print the decimal representation. */
    let mut index: int32 = 0;

    let mut output: uint64 = v.mantissa;
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
        /* won't need more than this many 0s */
        memcpy(result as *mut c_void, b"0.000000".as_ptr() as *const c_void, 8);
    } else if exp < 0 {
        /*
         * dddd.dddd; leave space at the start and move the '.' in after
         */
        index = 1;
    } else {
        /*
         * We can save some code later by pre-filling with zeros. We know that
         * there can be no more than 16 output digits in this form, otherwise
         * we would not choose fixed-point output.
         */
        Assert!(exp < 16 && exp + (olength as int32) <= 16);
        memset(result as *mut c_void, b'0' as c_int, 16);
    }

    /*
     * We prefer 32-bit operations, even on 64-bit platforms. We have at most
     * 17 digits, and uint32 can store 9 digits. If output doesn't fit into
     * uint32, we cut off 8 digits, so the rest will fit into uint32.
     */
    if (output >> 32) != 0 {
        /* Expensive 64-bit division. */
        let q: uint64 = div1e8(output);
        let mut output2: uint32 = (output.wrapping_sub(100000000u64.wrapping_mul(q))) as uint32;
        let c: uint32 = output2 % 10000;

        output = q;
        output2 /= 10000;

        let d: uint32 = output2 % 10000;
        let c0: uint32 = (c % 100) << 1;
        let c1: uint32 = (c / 100) << 1;
        let d0: uint32 = (d % 100) << 1;
        let d1: uint32 = (d / 100) << 1;

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
        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 6) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(d0 as usize) as *const c_void,
            2,
        );
        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 8) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(d1 as usize) as *const c_void,
            2,
        );
        i += 8;
    }

    let mut output2: uint32 = output as uint32;

    while output2 >= 10000 {
        let c: uint32 = output2 - 10000 * (output2 / 10000);
        let c0: uint32 = (c % 100) << 1;
        let c1: uint32 = (c / 100) << 1;

        output2 /= 10000;
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
    if output2 >= 100 {
        let c: uint32 = (output2 % 100) << 1;

        output2 /= 100;
        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 2) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(c as usize) as *const c_void,
            2,
        );
        i += 2;
    }
    if output2 >= 10 {
        let c: uint32 = output2 << 1;

        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 2) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(c as usize) as *const c_void,
            2,
        );
    } else {
        *result.offset(index as isize) = (b'0' + output2 as u8) as c_char;
    }

    if index == 1 {
        /*
         * nexp is 1..15 here, representing the number of digits before the
         * point. A value of 16 is not possible because we switch to
         * scientific notation when the display exponent reaches 15.
         */
        Assert!(nexp < 16);
        /* gcc only seems to want to optimize memmove for small 2^n */
        if nexp & 8 != 0 {
            memmove(
                result.offset((index - 1) as isize) as *mut c_void,
                result.offset(index as isize) as *const c_void,
                8,
            );
            index += 8;
        }
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

unsafe fn to_chars(v: floating_decimal_64, sign: bool, result: *mut c_char) -> c_int {
    /* Step 5: Print the decimal representation. */
    let mut index: int32 = 0;

    let mut output: uint64 = v.mantissa;
    let mut olength: uint32 = decimalLength(output);
    let mut exp: int32 = v.exponent + (olength as int32) - 1;

    if sign {
        *result.offset(index as isize) = b'-' as c_char;
        index += 1;
    }

    /*
     * The thresholds for fixed-point output are chosen to match printf
     * defaults. Beware that both the code of to_chars_df and the value of
     * DOUBLE_SHORTEST_DECIMAL_LEN are sensitive to these thresholds.
     */
    if exp >= -4 && exp < 15 {
        return to_chars_df(v, olength, result.offset(index as isize)) + (sign as c_int);
    }

    /*
     * If v.exponent is exactly 0, we might have reached here via the small
     * integer fast path, in which case v.mantissa might contain trailing
     * (decimal) zeros. For scientific notation we need to move these zeros
     * into the exponent. (For fixed point this doesn't matter, which is why
     * we do this here rather than above.)
     *
     * Since we already calculated the display exponent (exp) above based on
     * the old decimal length, that value does not change here. Instead, we
     * just reduce the display length for each digit removed.
     *
     * If we didn't get here via the fast path, the raw exponent will not
     * usually be 0, and there will be no trailing zeros, so we pay no more
     * than one div10/multiply extra cost. We claw back half of that by
     * checking for divisibility by 2 before dividing by 10.
     */
    if v.exponent == 0 {
        while (output & 1) == 0 {
            let q: uint64 = div10(output);
            let r: uint32 = (output.wrapping_sub(10u64.wrapping_mul(q))) as uint32;

            if r != 0 {
                break;
            }
            output = q;
            olength -= 1;
        }
    }

    /*----
     * Print the decimal digits.
     *
     * The following code is equivalent to:
     *
     * for (uint32 i = 0; i < olength - 1; ++i) {
     *   const uint32 c = output % 10; output /= 10;
     *   result[index + olength - i] = (char) ('0' + c);
     * }
     * result[index] = '0' + output % 10;
     *----
     */

    let mut i: uint32 = 0;

    /*
     * We prefer 32-bit operations, even on 64-bit platforms. We have at most
     * 17 digits, and uint32 can store 9 digits. If output doesn't fit into
     * uint32, we cut off 8 digits, so the rest will fit into uint32.
     */
    if (output >> 32) != 0 {
        /* Expensive 64-bit division. */
        let q: uint64 = div1e8(output);
        let mut output2: uint32 = (output.wrapping_sub(100000000u64.wrapping_mul(q))) as uint32;

        output = q;

        let c: uint32 = output2 % 10000;

        output2 /= 10000;

        let d: uint32 = output2 % 10000;
        let c0: uint32 = (c % 100) << 1;
        let c1: uint32 = (c / 100) << 1;
        let d0: uint32 = (d % 100) << 1;
        let d1: uint32 = (d / 100) << 1;

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
        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 5) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(d0 as usize) as *const c_void,
            2,
        );
        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 7) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(d1 as usize) as *const c_void,
            2,
        );
        i += 8;
    }

    let mut output2: uint32 = output as uint32;

    while output2 >= 10000 {
        let c: uint32 = output2 - 10000 * (output2 / 10000);

        output2 /= 10000;

        let c0: uint32 = (c % 100) << 1;
        let c1: uint32 = (c / 100) << 1;

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
    if output2 >= 100 {
        let c: uint32 = (output2 % 100) << 1;

        output2 /= 100;
        memcpy(
            result.offset((index + (olength as int32) - (i as int32) - 1) as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add(c as usize) as *const c_void,
            2,
        );
        i += 2;
    }
    if output2 >= 10 {
        let c: uint32 = output2 << 1;

        /*
         * We can't use memcpy here: the decimal dot goes between these two
         * digits.
         */
        *result.offset((index + (olength as int32) - (i as int32)) as isize) =
            DIGIT_TABLE[(c + 1) as usize];
        *result.offset(index as isize) = DIGIT_TABLE[c as usize];
    } else {
        *result.offset(index as isize) = (b'0' + output2 as u8) as c_char;
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

    if exp >= 100 {
        let c: int32 = exp % 10;

        memcpy(
            result.offset(index as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add((2 * (exp / 10)) as usize) as *const c_void,
            2,
        );
        *result.offset((index + 2) as isize) = (b'0' + c as u8) as c_char;
        index += 3;
    } else {
        memcpy(
            result.offset(index as isize) as *mut c_void,
            DIGIT_TABLE.as_ptr().add((2 * exp) as usize) as *const c_void,
            2,
        );
        index += 2;
    }

    index
}

fn d2d_small_int(
    ieeeMantissa: uint64,
    ieeeExponent: uint32,
    v: &mut floating_decimal_64,
) -> bool {
    let e2: int32 = (ieeeExponent as int32) - DOUBLE_BIAS - (DOUBLE_MANTISSA_BITS as int32);

    /*
     * Avoid using multiple "return false;" here since it tends to provoke the
     * compiler into inlining multiple copies of d2d, which is undesirable.
     */
    if e2 >= -(DOUBLE_MANTISSA_BITS as int32) && e2 <= 0 {
        /*----
         * Since 2^52 <= m2 < 2^53 and 0 <= -e2 <= 52:
         *   1 <= f = m2 / 2^-e2 < 2^53.
         *
         * Test if the lower -e2 bits of the significand are 0, i.e. whether
         * the fraction is 0.
         */
        let mask: uint64 = (1u64 << (-e2)) - 1;
        let fraction: uint64 = ieeeMantissa & mask;

        if fraction == 0 {
            /*----
             * f is an integer in the range [1, 2^53).
             * Note: mantissa might contain trailing (decimal) 0's.
             * Note: since 2^53 < 10^16, there is no need to adjust
             * decimalLength().
             */
            let m2: uint64 = (1u64 << DOUBLE_MANTISSA_BITS) | ieeeMantissa;

            v.mantissa = m2 >> (-e2);
            v.exponent = 0;
            return true;
        }
    }

    false
}

/// Store the shortest decimal representation of the given double as an
/// UNTERMINATED string in the caller's supplied buffer (which must be at least
/// DOUBLE_SHORTEST_DECIMAL_LEN-1 bytes long). Returns the number of bytes stored.
#[no_mangle]
pub unsafe extern "C" fn double_to_shortest_decimal_bufn(f: f64, result: *mut c_char) -> c_int {
    /*
     * Step 1: Decode the floating-point number, and unify normalized and
     * subnormal cases.
     */
    let bits: uint64 = double_to_bits(f);

    /* Decode bits into sign, mantissa, and exponent. */
    let ieeeSign: bool = ((bits >> (DOUBLE_MANTISSA_BITS + DOUBLE_EXPONENT_BITS)) & 1) != 0;
    let ieeeMantissa: uint64 = bits & ((1u64 << DOUBLE_MANTISSA_BITS) - 1);
    let ieeeExponent: uint32 =
        ((bits >> DOUBLE_MANTISSA_BITS) & ((1u64 << DOUBLE_EXPONENT_BITS) - 1)) as uint32;

    /* Case distinction; exit early for the easy cases. */
    if ieeeExponent == ((1u32 << DOUBLE_EXPONENT_BITS) - 1u32)
        || (ieeeExponent == 0 && ieeeMantissa == 0)
    {
        return copy_special_str(result, ieeeSign, ieeeExponent != 0, ieeeMantissa != 0);
    }

    let mut v = floating_decimal_64 {
        mantissa: 0,
        exponent: 0,
    };
    let isSmallInt: bool = d2d_small_int(ieeeMantissa, ieeeExponent, &mut v);

    if !isSmallInt {
        v = d2d(ieeeMantissa, ieeeExponent);
    }

    to_chars(v, ieeeSign, result)
}

/// Store the shortest decimal representation of the given double as a
/// null-terminated string in the caller's supplied buffer (which must be at
/// least DOUBLE_SHORTEST_DECIMAL_LEN bytes long). Returns the string length.
#[no_mangle]
pub unsafe extern "C" fn double_to_shortest_decimal_buf(f: f64, result: *mut c_char) -> c_int {
    let index: c_int = double_to_shortest_decimal_bufn(f, result);

    /* Terminate the string. */
    Assert!(index < DOUBLE_SHORTEST_DECIMAL_LEN);
    *result.offset(index as isize) = b'\0' as c_char;
    index
}

/// Return the shortest decimal representation as a null-terminated palloc'd
/// string. Caller is responsible for freeing the result.
#[no_mangle]
pub unsafe extern "C" fn double_to_shortest_decimal(f: f64) -> *mut c_char {
    let result: *mut c_char = palloc(DOUBLE_SHORTEST_DECIMAL_LEN as Size) as *mut c_char;
    double_to_shortest_decimal_buf(f, result);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: run the real algorithm into a buffer and return the produced String.
    fn shortest(f: f64) -> String {
        let mut buf = [0u8; 32];
        let len = unsafe { double_to_shortest_decimal_buf(f, buf.as_mut_ptr() as *mut c_char) };
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
        assert_eq!(shortest(f64::INFINITY), "Infinity");
        assert_eq!(shortest(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(shortest(f64::NAN), "NaN");
        assert_eq!(shortest(-0.0), "-0");
    }

    #[test]
    fn test_scientific_and_fixed() {
        // 1e300 is far outside the fixed-point window -> scientific notation.
        assert_eq!(shortest(1e300), "1e+300");
        // 1e-7 is below the fixed-point window -> scientific notation.
        assert_eq!(shortest(1e-7), "1e-07");
        // Within the window -> fixed point.
        assert_eq!(shortest(100.0), "100");
        assert_eq!(shortest(0.001), "0.001");
    }

    #[test]
    fn test_negative_and_fraction() {
        assert_eq!(shortest(-1.5), "-1.5");
        assert_eq!(shortest(0.5), "0.5");
    }

    #[test]
    fn test_round_trip_parseable() {
        // The output must parse back to the exact same double bits.
        let cases: [f64; 10] = [
            1.5,
            3.0,
            0.1,
            1.0 / 3.0,
            123.456,
            1e10,
            1e-10,
            12345.6789,
            f64::MAX,             // near DBL_MAX
            f64::MIN_POSITIVE,    // smallest normal
        ];
        for &c in &cases {
            let s = shortest(c);
            let parsed: f64 = s.parse().unwrap_or_else(|_| panic!("parse {s}"));
            assert_eq!(parsed.to_bits(), c.to_bits(), "round-trip {c} -> {s}");
        }
    }

    #[test]
    fn test_helpers() {
        assert_eq!(pow5bits(0), 1);
        assert_eq!(log10Pow2(0), 0);
        assert_eq!(log10Pow5(0), 0);
        assert_eq!(decimalLength(0), 1);
        assert_eq!(decimalLength(99999999999999999u64), 17);
        assert!(multipleOfPowerOf2(8, 3));
        assert!(!multipleOfPowerOf2(8, 4));
        assert!(multipleOfPowerOf5(25, 2));
        assert!(!multipleOfPowerOf5(25, 3));
        assert_eq!(pow5Factor(125), 3);
        assert_eq!(div5(50), 10);
        assert_eq!(div1e8(100000000), 1);
    }
}
