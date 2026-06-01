//! Selectivity routines registered in the operator catalog in the
//! "oprrest" and "oprjoin" attributes.
//!
//! Source: postgres/src/backend/utils/adt/geo_selfuncs.c
//! #include "postgres.h"            -> crate::prelude::*
//! #include "utils/fmgrprotos.h"    -> crate::utils::fmgr (FunctionCallInfo)
//!
//! XXX These are totally bogus.  Perhaps someone will make them do
//! something reasonable, someday.
//!
//! Selectivity functions for geometric operators.  These are bogus -- unless
//! we know the actual key distribution in the index, we can't make a good
//! prediction of the selectivity of these operators.  Each body simply
//! returns a fixed float8 constant; the arguments are ignored.

use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;
use crate::PG_RETURN_FLOAT8;

/*
 * Selectivity for operators that depend on area, such as "overlap".
 */

pub unsafe fn areasel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(0.005);
}

pub unsafe fn areajoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(0.005);
}

/*
 *	positionsel
 *
 * How likely is a box to be strictly left of (right of, above, below)
 * a given box?
 */

pub unsafe fn positionsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(0.1);
}

pub unsafe fn positionjoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(0.1);
}

/*
 *	contsel -- How likely is a box to contain (be contained by) a given box?
 *
 * This is a tighter constraint than "overlap", so produce a smaller
 * estimate than areasel does.
 */

pub unsafe fn contsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(0.001);
}

pub unsafe fn contjoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(0.001);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Args are unused by every function, so a null fcinfo is safe to pass.
    // We decode the returned Datum back to f64 (float8) and compare to the
    // expected constant.
    unsafe fn call_float8(f: unsafe fn(FunctionCallInfo) -> Datum) -> f64 {
        let d: Datum = f(null_mut());
        crate::postgres::DatumGetFloat8(d)
    }

    #[test]
    fn test_areasel_constant() {
        unsafe {
            assert_eq!(call_float8(areasel), 0.005);
            assert_eq!(call_float8(areajoinsel), 0.005);
        }
    }

    #[test]
    fn test_positionsel_constant() {
        unsafe {
            assert_eq!(call_float8(positionsel), 0.1);
            assert_eq!(call_float8(positionjoinsel), 0.1);
        }
    }

    #[test]
    fn test_contsel_constant() {
        unsafe {
            assert_eq!(call_float8(contsel), 0.001);
            assert_eq!(call_float8(contjoinsel), 0.001);
        }
    }
}
