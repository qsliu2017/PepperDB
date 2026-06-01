//! Translation of postgres/src/backend/utils/adt/pseudorandomfuncs.c
//!
//! Functions giving SQL access to a pseudorandom number generator.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   postgres.h               -> crate::prelude::*
//!   <math.h>                 -> isnan -> Rust f64::is_nan
//!   common/pg_prng.h         -> crate::common::pg_prng (pg_prng_state + pg_prng_* fns)
//!   miscadmin.h              -> NOT ported (MyProcPid); only used in the strong-seed
//!                               fallback, which is stubbed (see initialize_prng)
//!   utils/fmgrprotos.h       -> crate::utils::fmgr (PG_GETARG_*/PG_RETURN_* macros)
//!   utils/numeric.h          -> NOT ported (Numeric / PG_GETARG_NUMERIC / random_numeric)
//!                               => numeric_random stubbed
//!   utils/timestamp.h        -> NOT ported (TimestampTz / GetCurrentTimestamp); only used
//!                               in the strong-seed fallback, which is stubbed
//!
//! TRANSLATED FULLY: initialize_prng (strong-seed path via pg_strong_random),
//! setseed, drandom, drandom_normal, int4random, int8random.
//! STUBBED: numeric_random (numeric.c not yet ported); the time/PID seed fallback
//! inside initialize_prng (GetCurrentTimestamp / MyProcPid not ported).

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::{
    PG_GETARG_FLOAT8, PG_GETARG_INT32, PG_GETARG_INT64, PG_RETURN_FLOAT8, PG_RETURN_INT32,
    PG_RETURN_INT64, PG_RETURN_VOID,
};
use crate::c::{float8, int32, int64};
use crate::common::pg_prng::{
    pg_prng_double, pg_prng_double_normal, pg_prng_fseed, pg_prng_int64_range, pg_prng_seed,
    pg_prng_seed_check, pg_prng_state,
};
use crate::port::pg_strong_random::pg_strong_random;

// errcodes.h is not yet ported as a constants module; like the sibling adt
// units (varchar.rs etc.) we define the one ERRCODE we need locally. errcode()
// is a shim that ignores its argument, so the value is informational only.
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

/* Shared PRNG state used by all the random functions */
// C: `static pg_prng_state prng_state;` -- a zero-initialized file-static. We
// mirror it as a module-private `static mut`. pg_prng_state is { s0, s1 }.
static mut prng_state: pg_prng_state = pg_prng_state { s0: 0, s1: 0 };
// C: `static bool prng_seed_set = false;`
static mut prng_seed_set: bool = false;

/*
 * initialize_prng() -
 *
 *	Initialize (seed) the PRNG, if not done yet in this process.
 */
unsafe fn initialize_prng() {
    if !prng_seed_set {
        /*
         * If possible, seed the PRNG using high-quality random bits. Should
         * that fail for some reason, we fall back on a lower-quality seed
         * based on current time and PID.
         */
        // C calls pg_prng_strong_seed(&prng_state), a macro defined as
        //   (pg_strong_random((void *) &(state), sizeof(pg_prng_state)) ?
        //        pg_prng_seed_check(&(state)) : false)
        // pg_prng_strong_seed is not present in the ported pg_prng module, so we
        // inline that macro here: fill the whole pg_prng_state with strong random
        // bytes, then run the all-zeroes check.
        let strong: bool = pg_strong_random(
            &raw mut prng_state as *mut c_void,
            core::mem::size_of::<pg_prng_state>(),
        ) && pg_prng_seed_check(&raw mut prng_state);

        if !strong {
            // TODO(pg-port): faithful fallback is
            //     TimestampTz now = GetCurrentTimestamp();
            //     uint64 iseed = (uint64) now ^ ((uint64) MyProcPid << 32);
            //     pg_prng_seed(&prng_state, iseed);
            // but GetCurrentTimestamp() (utils/timestamp.h) and MyProcPid
            // (miscadmin.h) are not yet ported. Use a FIXED seed so behavior is
            // deterministic until those deps land.
            let iseed: uint64 = 0;
            pg_prng_seed(&raw mut prng_state, iseed);
        }
        prng_seed_set = true;
    }
}

/*
 * setseed() -
 *
 *	Seed the PRNG from a specified value in the range [-1.0, 1.0].
 */
pub unsafe fn setseed(fcinfo: FunctionCallInfo) -> Datum {
    let seed: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);

    if seed < -1.0 || seed > 1.0 || seed.is_nan() {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!(
                "setseed parameter {} is out of allowed range [-1,1]",
                seed
            )
        );
    }

    pg_prng_fseed(&raw mut prng_state, seed);
    prng_seed_set = true;

    PG_RETURN_VOID!()
}

/*
 * drandom() -
 *
 *	Returns a random number chosen uniformly in the range [0.0, 1.0).
 */
pub unsafe fn drandom(fcinfo: FunctionCallInfo) -> Datum {
    let result: float8;

    initialize_prng();

    /* pg_prng_double produces desired result range [0.0, 1.0) */
    result = pg_prng_double(&raw mut prng_state);

    PG_RETURN_FLOAT8!(result)
}

/*
 * drandom_normal() -
 *
 *	Returns a random number from a normal distribution.
 */
pub unsafe fn drandom_normal(fcinfo: FunctionCallInfo) -> Datum {
    let mean: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let stddev: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);
    let result: float8;
    let z: float8;

    initialize_prng();

    /* Get random value from standard normal(mean = 0.0, stddev = 1.0) */
    z = pg_prng_double_normal(&raw mut prng_state);
    /* Transform the normal standard variable (z) */
    /* using the target normal distribution parameters */
    result = (stddev * z) + mean;

    PG_RETURN_FLOAT8!(result)
}

/*
 * int4random() -
 *
 *	Returns a random 32-bit integer chosen uniformly in the specified range.
 */
pub unsafe fn int4random(fcinfo: FunctionCallInfo) -> Datum {
    let rmin: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let rmax: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let result: int32;

    if rmin > rmax {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!("lower bound must be less than or equal to upper bound")
        );
    }

    initialize_prng();

    result = pg_prng_int64_range(&raw mut prng_state, rmin as int64, rmax as int64) as int32;

    PG_RETURN_INT32!(result)
}

/*
 * int8random() -
 *
 *	Returns a random 64-bit integer chosen uniformly in the specified range.
 */
pub unsafe fn int8random(fcinfo: FunctionCallInfo) -> Datum {
    let rmin: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let rmax: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let result: int64;

    if rmin > rmax {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!("lower bound must be less than or equal to upper bound")
        );
    }

    initialize_prng();

    result = pg_prng_int64_range(&raw mut prng_state, rmin, rmax);

    PG_RETURN_INT64!(result)
}

/*
 * numeric_random() -
 *
 *	Returns a random numeric value chosen uniformly in the specified range.
 */
// TODO(pg-port): needs utils/numeric (Numeric type, PG_GETARG_NUMERIC,
// PG_RETURN_NUMERIC, random_numeric). numeric.c is not yet ported.
//
// Original C body:
//   Numeric  rmin = PG_GETARG_NUMERIC(0);
//   Numeric  rmax = PG_GETARG_NUMERIC(1);
//   Numeric  result;
//
//   initialize_prng();
//
//   result = random_numeric(&prng_state, rmin, rmax);
//
//   PG_RETURN_NUMERIC(result);
pub unsafe fn numeric_random(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Exercise the real PRNG logic directly against the file-static state.
    // The fmgr entry points need a FunctionCallInfo we cannot synthesize here,
    // so we drive the same underlying calls the SQL functions make.

    #[test]
    fn setseed_then_drandom_in_unit_range() {
        unsafe {
            // setseed(0.5): seed the shared state deterministically.
            pg_prng_fseed(&raw mut prng_state, 0.5);
            prng_seed_set = true;

            // drandom() => pg_prng_double in [0.0, 1.0).
            for _ in 0..1000 {
                let r = pg_prng_double(&raw mut prng_state);
                assert!(r >= 0.0 && r < 1.0, "drandom out of range: {}", r);
            }
        }
    }

    #[test]
    fn int4random_singleton_range_is_fixed() {
        unsafe {
            pg_prng_fseed(&raw mut prng_state, -0.25);
            prng_seed_set = true;
            // int4random(5, 5) must always be 5 (empty range -> rmin).
            for _ in 0..100 {
                let v = pg_prng_int64_range(&raw mut prng_state, 5, 5) as int32;
                assert_eq!(v, 5);
            }
        }
    }

    #[test]
    fn int8random_stays_within_bounds() {
        unsafe {
            pg_prng_fseed(&raw mut prng_state, 0.0);
            prng_seed_set = true;
            let (lo, hi): (int64, int64) = (-1000, 1000);
            for _ in 0..1000 {
                let v = pg_prng_int64_range(&raw mut prng_state, lo, hi);
                assert!(v >= lo && v <= hi, "int8random out of range: {}", v);
            }
        }
    }

    #[test]
    fn initialize_prng_sets_flag_and_nonzero_state() {
        unsafe {
            // Force re-seed via the real initializer (strong-random path on
            // platforms with /dev/urandom; fixed-seed fallback otherwise).
            prng_seed_set = false;
            initialize_prng();
            assert!(prng_seed_set);
            // pg_prng_seed_check guarantees the state is never all-zeroes.
            assert!(prng_state.s0 != 0 || prng_state.s1 != 0);
        }
    }
}
