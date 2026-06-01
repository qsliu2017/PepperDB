//! Translation of postgres/src/include/lib/hyperloglog.h
//!                + postgres/src/backend/lib/hyperloglog.c
//!
//! hyperloglog.h / hyperloglog.c
//!
//! A simple HyperLogLog cardinality estimator implementation
//!
//! Portions Copyright (c) 2014-2025, PostgreSQL Global Development Group
//!
//! Based on Hideaki Ohno's C++ implementation.  The copyright terms of Ohno's
//! original version (the MIT license) follow.
//!
//! Copyright (c) 2013 Hideaki Ohno <hide.o.j55{at}gmail.com>
//!
//! Permission is hereby granted, free of charge, to any person obtaining a copy
//! of this software and associated documentation files (the 'Software'), to
//! deal in the Software without restriction, including without limitation the
//! rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
//! sell copies of the Software, and to permit persons to whom the Software is
//! furnished to do so, subject to the following conditions:
//!
//! The above copyright notice and this permission notice shall be included in
//! all copies or substantial portions of the Software.
//!
//! THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//! IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//! FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//! AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//! LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
//! FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
//! IN THE SOFTWARE.

use crate::prelude::*;
use crate::port::pg_bitutils::pg_leftmost_one_pos32;
use core::ffi::c_int;

/*
 * HyperLogLog is an approximate technique for computing the number of distinct
 * entries in a set.  Importantly, it does this by using a fixed amount of
 * memory.  See the 2007 paper "HyperLogLog: the analysis of a near-optimal
 * cardinality estimation algorithm" for more.
 *
 * hyperLogLogState
 *
 *		registerWidth		register width, in bits ("k")
 *		nRegisters			number of registers
 *		alphaMM				alpha * m ^ 2 (see initHyperLogLog())
 *		hashesArr			array of hashes
 *		arrSize				size of hashesArr
 */
#[repr(C)]
pub struct hyperLogLogState {
    pub registerWidth: uint8,
    pub nRegisters: Size,
    pub alphaMM: f64,
    pub hashesArr: *mut uint8,
    pub arrSize: Size,
}

// POW_2_32 / NEG_POW_2_32 from hyperloglog.c
const POW_2_32: f64 = 4294967296.0;
const NEG_POW_2_32: f64 = -4294967296.0;

// TODO(pg-port): BITS_PER_BYTE is defined as 8 in postgres/src/include/pg_config_manual.h
// and is not yet present in the prelude; define it privately here to keep the module
// independently compilable.
const BITS_PER_BYTE: uint32 = 8;

/*
 * Initialize HyperLogLog track state, by bit width
 *
 * bwidth is bit width (so register size will be 2 to the power of bwidth).
 * Must be between 4 and 16 inclusive.
 */
pub unsafe fn initHyperLogLog(cState: *mut hyperLogLogState, bwidth: uint8) {
    let alpha: f64;

    if bwidth < 4 || bwidth > 16 {
        elog!(ERROR, "bit width must be between 4 and 16 inclusive");
    }

    (*cState).registerWidth = bwidth;
    (*cState).nRegisters = (1 as Size) << bwidth;
    // sizeof(uint8) == 1, so arrSize = nRegisters + 1
    (*cState).arrSize = core::mem::size_of::<uint8>() * (*cState).nRegisters + 1;

    /*
     * Initialize hashes array to zero, not negative infinity, per discussion
     * of the coupon collector problem in the HyperLogLog paper
     */
    (*cState).hashesArr = palloc0((*cState).arrSize) as *mut uint8;

    /*
     * "alpha" is a value that for each possible number of registers (m) is
     * used to correct a systematic multiplicative bias present in m ^ 2 Z (Z
     * is "the indicator function" through which we finally compute E,
     * estimated cardinality).
     */
    match (*cState).nRegisters {
        16 => {
            alpha = 0.673;
        }
        32 => {
            alpha = 0.697;
        }
        64 => {
            alpha = 0.709;
        }
        _ => {
            alpha = 0.7213 / (1.0 + 1.079 / (*cState).nRegisters as f64);
        }
    }

    /*
     * Precalculate alpha m ^ 2, later used to generate "raw" HyperLogLog
     * estimate E
     */
    (*cState).alphaMM =
        alpha * (*cState).nRegisters as f64 * (*cState).nRegisters as f64;
}

/*
 * Initialize HyperLogLog track state, by error rate
 *
 * Instead of specifying bwidth (number of bits used for addressing the
 * register), this method allows sizing the counter for particular error
 * rate using a simple formula from the paper:
 *
 *	 e = 1.04 / sqrt(m)
 *
 * where 'm' is the number of registers, i.e. (2^bwidth). The method
 * finds the lowest bwidth with 'e' below the requested error rate, and
 * then uses it to initialize the counter.
 *
 * As bwidth has to be between 4 and 16, the worst possible error rate
 * is between ~25% (bwidth=4) and 0.4% (bwidth=16).
 */
pub unsafe fn initHyperLogLogError(cState: *mut hyperLogLogState, error: f64) {
    let mut bwidth: uint8 = 4;

    while bwidth < 16 {
        let m: f64 = ((1 as Size) << bwidth) as f64;

        if 1.04 / m.sqrt() < error {
            break;
        }
        bwidth += 1;
    }

    initHyperLogLog(cState, bwidth);
}

/*
 * Free HyperLogLog track state
 *
 * Releases allocated resources, but not the state itself (in case it's not
 * allocated by palloc).
 */
pub unsafe fn freeHyperLogLog(cState: *mut hyperLogLogState) {
    Assert!(!(*cState).hashesArr.is_null());
    pfree((*cState).hashesArr as *mut core::ffi::c_void);
}

/*
 * Adds element to the estimator, from caller-supplied hash.
 *
 * It is critical that the hash value passed be an actual hash value, typically
 * generated using hash_any().  The algorithm relies on a specific bit-pattern
 * observable in conjunction with stochastic averaging.  There must be a
 * uniform distribution of bits in hash values for each distinct original value
 * observed.
 */
pub unsafe fn addHyperLogLog(cState: *mut hyperLogLogState, hash: uint32) {
    let count: uint8;
    let index: uint32;

    /* Use the first "k" (registerWidth) bits as a zero based index */
    index = hash >> (BITS_PER_BYTE * core::mem::size_of::<uint32>() as uint32
        - (*cState).registerWidth as uint32);

    /* Compute the rank of the remaining 32 - "k" (registerWidth) bits */
    // hash << registerWidth: registerWidth is 4..=16 (< 32), so the shift is
    // in range; use wrapping_shl to mirror C's two's-complement shift behavior.
    count = rho(
        hash.wrapping_shl((*cState).registerWidth as uint32),
        (BITS_PER_BYTE * core::mem::size_of::<uint32>() as uint32
            - (*cState).registerWidth as uint32) as uint8,
    );

    let slot = (*cState).hashesArr.add(index as usize);
    *slot = Max(count, *slot);
}

/*
 * Estimates cardinality, based on elements added so far
 */
pub unsafe fn estimateHyperLogLog(cState: *mut hyperLogLogState) -> f64 {
    let mut result: f64;
    let mut sum: f64 = 0.0;
    let mut i: Size;

    i = 0;
    while i < (*cState).nRegisters {
        sum += 1.0 / 2.0f64.powf(*(*cState).hashesArr.add(i) as f64);
        i += 1;
    }

    /* result set to "raw" HyperLogLog estimate (E in the HyperLogLog paper) */
    result = (*cState).alphaMM / sum;

    if result <= (5.0 / 2.0) * (*cState).nRegisters as f64 {
        /* Small range correction */
        let mut zero_count: c_int = 0;

        i = 0;
        while i < (*cState).nRegisters {
            if *(*cState).hashesArr.add(i) == 0 {
                zero_count += 1;
            }
            i += 1;
        }

        if zero_count != 0 {
            result = (*cState).nRegisters as f64
                * ((*cState).nRegisters as f64 / zero_count as f64).ln();
        }
    } else if result > (1.0 / 30.0) * POW_2_32 {
        /* Large range correction */
        result = NEG_POW_2_32 * (1.0 - (result / POW_2_32)).ln();
    }

    result
}

/*
 * Worker for addHyperLogLog().
 *
 * Calculates the position of the first set bit in first b bits of x argument
 * starting from the first, reading from most significant to least significant
 * bits.
 *
 * Example (when considering fist 10 bits of x):
 *
 * rho(x = 0b1000000000)   returns 1
 * rho(x = 0b0010000000)   returns 3
 * rho(x = 0b0000000000)   returns b + 1
 *
 * "The binary address determined by the first b bits of x"
 *
 * Return value "j" used to index bit pattern to watch.
 */
#[inline]
fn rho(x: uint32, b: uint8) -> uint8 {
    let j: uint8;

    if x == 0 {
        return b + 1;
    }

    j = 32 - pg_leftmost_one_pos32(x) as uint8;

    if j > b {
        return b + 1;
    }

    j
}
