//! multirangetypes_selfuncs.rs
//!   Functions for selectivity estimation of multirange operators
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/multirangetypes_selfuncs.c
//!
//! Estimates are based on histograms of lower and upper bounds, and the
//! fraction of empty multiranges.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/multirangetypes_selfuncs.c
#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::utils::cache::typcache::TypeCacheEntry;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{IsA, PG_GETARG_INT32, PG_GETARG_OID, PG_GETARG_POINTER, PG_RETURN_FLOAT8};

use std::ffi::{c_char, c_int, c_void};

use crate::c::{float4, float8, int32};
use crate::postgres_ext::Oid;

use crate::nodes::nodes::{Node, Selectivity};
use crate::nodes::primnodes::Const;

use crate::utils::adt::float::get_float8_infinity;
use crate::utils::fmgr::{FmgrInfo, FunctionCall2Coll};

/* multirange/range op OIDs (catalog/pg_operator.h via pg_known_oids.rs) */
use crate::catalog::pg_known_oids::{
    OID_MULTIRANGE_CONTAINS_ELEM_OP, OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP,
    OID_MULTIRANGE_CONTAINS_RANGE_OP, OID_MULTIRANGE_ELEM_CONTAINED_OP,
    OID_MULTIRANGE_GREATER_EQUAL_OP, OID_MULTIRANGE_GREATER_OP, OID_MULTIRANGE_LEFT_MULTIRANGE_OP,
    OID_MULTIRANGE_LEFT_RANGE_OP, OID_MULTIRANGE_LESS_EQUAL_OP, OID_MULTIRANGE_LESS_OP,
    OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP, OID_MULTIRANGE_OVERLAPS_LEFT_MULTIRANGE_OP,
    OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP, OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP,
    OID_MULTIRANGE_OVERLAPS_RANGE_OP, OID_MULTIRANGE_OVERLAPS_RIGHT_MULTIRANGE_OP,
    OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP, OID_MULTIRANGE_RANGE_CONTAINED_OP,
    OID_MULTIRANGE_RIGHT_MULTIRANGE_OP, OID_MULTIRANGE_RIGHT_RANGE_OP,
    OID_RANGE_CONTAINS_MULTIRANGE_OP, OID_RANGE_LEFT_MULTIRANGE_OP,
    OID_RANGE_MULTIRANGE_CONTAINED_OP, OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP,
    OID_RANGE_OVERLAPS_MULTIRANGE_OP, OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP,
    OID_RANGE_RIGHT_MULTIRANGE_OP,
};

/* pg_statistic kinds (catalog/pg_statistic.h) */
use crate::catalog::pg_statistic::{
    STATISTIC_KIND_BOUNDS_HISTOGRAM, STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
};

/* selfuncs.h / examine-variable machinery (utils/adt/selfuncs.rs) */
use crate::utils::adt::selfuncs::{
    get_restriction_variable, statistic_proc_security_check, AttStatsSlot, VariableStatData,
};

/* rangetypes.h (utils/adt/rangetypes.rs) */
use crate::utils::adt::rangetypes::{
    range_cmp_bounds, range_deserialize, range_serialize, DatumGetRangeTypeP, RangeBound, RangeType,
};

/* multirangetypes.h (utils/adt/multirangetypes.rs) */
use crate::utils::adt::multirangetypes::{
    make_multirange, multirange_get_bounds, multirange_get_typcache, MultirangeType,
};

use crate::access::htup_details::{HeapTupleIsValid, GETSTRUCT};
use crate::catalog::pg_statistic::Form_pg_statistic;
use crate::postgres::DatumGetFloat8;

// ---------------------------------------------------------------------------
// Local helper macros and stubs for symbols whose homes are not yet ported,
// or that are not path-importable from their home module.
// ---------------------------------------------------------------------------

/*
 * Clamp a computed probability estimate to valid range.  Argument must be a
 * float variable.  (selfuncs.h CLAMP_PROBABILITY; the version in selfuncs.rs
 * is not exported, so a local copy is provided.)
 */
macro_rules! CLAMP_PROBABILITY {
    ($p:expr) => {{
        if $p < 0.0 {
            $p = 0.0;
        } else if $p > 1.0 {
            $p = 1.0;
        }
    }};
}

/* c.h Max/Min, working for both integers and floats. */
macro_rules! Max {
    ($a:expr, $b:expr) => {{
        let a = $a;
        let b = $b;
        if a > b {
            a
        } else {
            b
        }
    }};
}
macro_rules! Min {
    ($a:expr, $b:expr) => {{
        let a = $a;
        let b = $b;
        if a < b {
            a
        } else {
            b
        }
    }};
}

/* DEFAULT_MULTIRANGE_INEQ_SEL / DEFAULT_INEQ_SEL (selfuncs.h, not exported). */
// TODO(pg-port): real DEFAULT_MULTIRANGE_INEQ_SEL lives in utils/adt/selfuncs.rs (selfuncs.h)
const DEFAULT_MULTIRANGE_INEQ_SEL: f64 = 0.005;
// TODO(pg-port): real DEFAULT_INEQ_SEL lives in utils/adt/selfuncs.rs (selfuncs.h)
const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;

/* ATTSTATSSLOT_* flags (lsyscache.h, private in selfuncs.rs). */
// TODO(pg-port): real ATTSTATSSLOT_* live in utils/cache/lsyscache.rs
const ATTSTATSSLOT_VALUES: c_int = 0x01;
// TODO(pg-port): real ATTSTATSSLOT_* live in utils/cache/lsyscache.rs
const ATTSTATSSLOT_NUMBERS: c_int = 0x02;

/*
 * get_attstatsslot / free_attstatsslot (lsyscache.h).  selfuncs.rs has private
 * stubs; provide local TODO stubs here.
 */
// TODO(pg-port): real get_attstatsslot lives in utils/cache/lsyscache.rs
unsafe fn get_attstatsslot(
    sslot: *mut AttStatsSlot,
    statstuple: *mut c_void,
    reqkind: int16,
    reqop: Oid,
    flags: c_int,
) -> bool { crate::utils::cache::lsyscache::get_attstatsslot(sslot as _, statstuple as _, reqkind as _, reqop as _, flags as _) }
// TODO(pg-port): real free_attstatsslot lives in utils/cache/lsyscache.rs
unsafe fn free_attstatsslot(sslot: *mut AttStatsSlot) { crate::utils::cache::lsyscache::free_attstatsslot(sslot as _) }

/* get_commutator (lsyscache.h). */
// TODO(pg-port): real get_commutator lives in utils/cache/lsyscache.rs
unsafe fn get_commutator(opno: Oid) -> Oid { crate::utils::cache::lsyscache::get_commutator(opno as _) as _ }

/*
 * MultirangeIsEmpty / DatumGetMultirangeTypeP (utils/multirangetypes.h).  These
 * are private (non-pub) in multirangetypes.rs, so local copies are provided.
 */
// TODO(pg-port): real MultirangeIsEmpty lives in utils/adt/multirangetypes.rs
unsafe fn MultirangeIsEmpty(mr: *const MultirangeType) -> bool {
    (*mr).rangeCount == 0
}
// TODO(pg-port): real DatumGetMultirangeTypeP lives in utils/adt/multirangetypes.rs
unsafe fn DatumGetMultirangeTypeP(X: Datum) -> *mut MultirangeType {
    crate::PG_DETOAST_DATUM!(X) as *mut MultirangeType
}

/*
 * ReleaseVariableStats: free vardata.statsTuple if valid.  (selfuncs.h; the
 * macro in selfuncs.rs is not exported, so a local copy is provided.)
 */
macro_rules! ReleaseVariableStats {
    ($vardata:expr) => {{
        if HeapTupleIsValid($vardata.statsTuple) {
            if let Some(f) = $vardata.freefunc {
                f($vardata.statsTuple);
            }
        }
    }};
}

/*
 * Returns a default selectivity estimate for given operator, when we don't
 * have statistics or cannot use them for some reason.
 */
unsafe fn default_multirange_selectivity(operator: Oid) -> f64 {
    match operator {
        OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP
        | OID_MULTIRANGE_OVERLAPS_RANGE_OP
        | OID_RANGE_OVERLAPS_MULTIRANGE_OP => 0.01,

        OID_RANGE_CONTAINS_MULTIRANGE_OP
        | OID_RANGE_MULTIRANGE_CONTAINED_OP
        | OID_MULTIRANGE_CONTAINS_RANGE_OP
        | OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP
        | OID_MULTIRANGE_RANGE_CONTAINED_OP
        | OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP => 0.005,

        OID_MULTIRANGE_CONTAINS_ELEM_OP | OID_MULTIRANGE_ELEM_CONTAINED_OP =>
        /*
         * "multirange @> elem" is more or less identical to a scalar
         * inequality "A >= b AND A <= c".
         */
        {
            DEFAULT_MULTIRANGE_INEQ_SEL
        }

        OID_MULTIRANGE_LESS_OP
        | OID_MULTIRANGE_LESS_EQUAL_OP
        | OID_MULTIRANGE_GREATER_OP
        | OID_MULTIRANGE_GREATER_EQUAL_OP
        | OID_MULTIRANGE_LEFT_RANGE_OP
        | OID_MULTIRANGE_LEFT_MULTIRANGE_OP
        | OID_RANGE_LEFT_MULTIRANGE_OP
        | OID_MULTIRANGE_RIGHT_RANGE_OP
        | OID_MULTIRANGE_RIGHT_MULTIRANGE_OP
        | OID_RANGE_RIGHT_MULTIRANGE_OP
        | OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP
        | OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP
        | OID_MULTIRANGE_OVERLAPS_LEFT_MULTIRANGE_OP
        | OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP
        | OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP
        | OID_MULTIRANGE_OVERLAPS_RIGHT_MULTIRANGE_OP =>
        /* these are similar to regular scalar inequalities */
        {
            DEFAULT_INEQ_SEL
        }

        _ =>
        /*
         * all multirange operators should be handled above, but just in
         * case
         */
        {
            0.01
        }
    }
}

/*
 * multirangesel -- restriction selectivity for multirange operators
 */
pub unsafe fn multirangesel(fcinfo: FunctionCallInfo) -> Datum {
    let root = PG_GETARG_POINTER!(fcinfo, 0) as *mut PlannerInfo;
    let mut operator = PG_GETARG_OID!(fcinfo, 1);
    let args = PG_GETARG_POINTER!(fcinfo, 2) as *mut List;
    let varRelid = PG_GETARG_INT32!(fcinfo, 3);
    let mut vardata: VariableStatData = std::mem::zeroed();
    let mut other: *mut Node = null_mut();
    let mut varonleft: bool = false;
    let selec: Selectivity;
    let mut typcache: *mut TypeCacheEntry = null_mut();
    let mut constmultirange: *mut MultirangeType = null_mut();
    let mut constrange: *mut RangeType = null_mut();

    /*
     * If expression is not (variable op something) or (something op
     * variable), then punt and return a default estimate.
     */
    if !get_restriction_variable(
        root as *mut crate::utils::adt::selfuncs::PlannerInfo,
        args,
        varRelid,
        &raw mut vardata,
        &raw mut other,
        &raw mut varonleft,
    ) {
        PG_RETURN_FLOAT8!(default_multirange_selectivity(operator));
    }

    /*
     * Can't do anything useful if the something is not a constant, either.
     */
    if !IsA!(other, T_Const) {
        ReleaseVariableStats!(vardata);
        PG_RETURN_FLOAT8!(default_multirange_selectivity(operator));
    }

    /*
     * All the multirange operators are strict, so we can cope with a NULL
     * constant right away.
     */
    if (*(other as *mut Const)).constisnull {
        ReleaseVariableStats!(vardata);
        PG_RETURN_FLOAT8!(0.0);
    }

    /*
     * If var is on the right, commute the operator, so that we can assume the
     * var is on the left in what follows.
     */
    if !varonleft {
        /* we have other Op var, commute to make var Op other */
        operator = get_commutator(operator);
        if operator == InvalidOid {
            /* Use default selectivity (should we raise an error instead?) */
            ReleaseVariableStats!(vardata);
            PG_RETURN_FLOAT8!(default_multirange_selectivity(operator));
        }
    }

    /*
     * OK, there's a Var and a Const we're dealing with here.  We need the
     * Const to be of same multirange type as the column, else we can't do
     * anything useful. (Such cases will likely fail at runtime, but here we'd
     * rather just return a default estimate.)
     *
     * If the operator is "multirange @> element", the constant should be of
     * the element type of the multirange column. Convert it to a multirange
     * that includes only that single point, so that we don't need special
     * handling for that in what follows.
     */
    if operator == OID_MULTIRANGE_CONTAINS_ELEM_OP {
        typcache = multirange_get_typcache(fcinfo, vardata.vartype);

        if (*(other as *mut Const)).consttype == (*(*(*typcache).rngtype).rngelemtype).type_id {
            let mut lower: RangeBound = std::mem::zeroed();
            let mut upper: RangeBound = std::mem::zeroed();

            lower.inclusive = true;
            lower.val = (*(other as *mut Const)).constvalue;
            lower.infinite = false;
            lower.lower = true;
            upper.inclusive = true;
            upper.val = (*(other as *mut Const)).constvalue;
            upper.infinite = false;
            upper.lower = false;
            constrange = range_serialize(
                (*typcache).rngtype,
                &raw mut lower,
                &raw mut upper,
                false,
                null_mut(),
            );
            constmultirange = make_multirange(
                (*typcache).type_id,
                (*typcache).rngtype,
                1,
                &raw mut constrange,
            );
        }
    } else if operator == OID_RANGE_MULTIRANGE_CONTAINED_OP
        || operator == OID_MULTIRANGE_CONTAINS_RANGE_OP
        || operator == OID_MULTIRANGE_OVERLAPS_RANGE_OP
        || operator == OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP
        || operator == OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP
        || operator == OID_MULTIRANGE_LEFT_RANGE_OP
        || operator == OID_MULTIRANGE_RIGHT_RANGE_OP
    {
        /*
         * Promote a range in "multirange OP range" just like we do an element
         * in "multirange OP element".
         */
        typcache = multirange_get_typcache(fcinfo, vardata.vartype);
        if (*(other as *mut Const)).consttype == (*(*typcache).rngtype).type_id {
            constrange = DatumGetRangeTypeP((*(other as *mut Const)).constvalue);
            constmultirange = make_multirange(
                (*typcache).type_id,
                (*typcache).rngtype,
                1,
                &raw mut constrange,
            );
        }
    } else if operator == OID_RANGE_OVERLAPS_MULTIRANGE_OP
        || operator == OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP
        || operator == OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP
        || operator == OID_RANGE_LEFT_MULTIRANGE_OP
        || operator == OID_RANGE_RIGHT_MULTIRANGE_OP
        || operator == OID_RANGE_CONTAINS_MULTIRANGE_OP
        || operator == OID_MULTIRANGE_ELEM_CONTAINED_OP
        || operator == OID_MULTIRANGE_RANGE_CONTAINED_OP
    {
        /*
         * Here, the Var is the elem/range, not the multirange.  For now we
         * just punt and return the default estimate.  In future we could
         * disassemble the multirange constant to do something more
         * intelligent.
         */
    } else if (*(other as *mut Const)).consttype == vardata.vartype {
        /* Both sides are the same multirange type */
        typcache = multirange_get_typcache(fcinfo, vardata.vartype);

        constmultirange = DatumGetMultirangeTypeP((*(other as *mut Const)).constvalue);
    }

    /*
     * If we got a valid constant on one side of the operator, proceed to
     * estimate using statistics. Otherwise punt and return a default constant
     * estimate.  Note that calc_multirangesel need not handle
     * OID_MULTIRANGE_*_CONTAINED_OP.
     */
    if !constmultirange.is_null() {
        selec = calc_multirangesel(typcache, &raw mut vardata, constmultirange, operator);
    } else {
        selec = default_multirange_selectivity(operator);
    }

    ReleaseVariableStats!(vardata);

    let mut selec = selec;
    CLAMP_PROBABILITY!(selec);

    PG_RETURN_FLOAT8!(selec as float8);
}

unsafe fn calc_multirangesel(
    typcache: *mut TypeCacheEntry,
    vardata: *mut VariableStatData,
    constval: *const MultirangeType,
    operator: Oid,
) -> f64 {
    let hist_selec: f64;
    let mut selec: f64;
    let empty_frac: float4;
    let null_frac: float4;

    /*
     * First look up the fraction of NULLs and empty multiranges from
     * pg_statistic.
     */
    if HeapTupleIsValid((*vardata).statsTuple) {
        let stats: Form_pg_statistic;
        let mut sslot: AttStatsSlot = std::mem::zeroed();

        stats = GETSTRUCT((*vardata).statsTuple) as Form_pg_statistic;
        null_frac = (*stats).stanullfrac;

        /* Try to get fraction of empty multiranges */
        if get_attstatsslot(
            &raw mut sslot,
            (*vardata).statsTuple as *mut c_void,
            STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_NUMBERS,
        ) {
            if sslot.nnumbers != 1 {
                elog!(ERROR, "invalid empty fraction statistic"); /* shouldn't happen */
            }
            empty_frac = *sslot.numbers.add(0);
            free_attstatsslot(&raw mut sslot);
        } else {
            /* No empty fraction statistic. Assume no empty ranges. */
            empty_frac = 0.0;
        }
    } else {
        /*
         * No stats are available. Follow through the calculations below
         * anyway, assuming no NULLs and no empty multiranges. This still
         * allows us to give a better-than-nothing estimate based on whether
         * the constant is an empty multirange or not.
         */
        null_frac = 0.0;
        empty_frac = 0.0;
    }

    if MultirangeIsEmpty(constval) {
        /*
         * An empty multirange matches all multiranges, all empty multiranges,
         * or nothing, depending on the operator
         */
        match operator {
            /* these return false if either argument is empty */
            OID_MULTIRANGE_OVERLAPS_RANGE_OP
            | OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP
            | OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP
            | OID_MULTIRANGE_OVERLAPS_LEFT_MULTIRANGE_OP
            | OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP
            | OID_MULTIRANGE_OVERLAPS_RIGHT_MULTIRANGE_OP
            | OID_MULTIRANGE_LEFT_RANGE_OP
            | OID_MULTIRANGE_LEFT_MULTIRANGE_OP
            | OID_MULTIRANGE_RIGHT_RANGE_OP
            | OID_MULTIRANGE_RIGHT_MULTIRANGE_OP
            /* nothing is less than an empty multirange */
            | OID_MULTIRANGE_LESS_OP => {
                selec = 0.0;
            }

            /*
             * only empty multiranges can be contained by an empty
             * multirange
             */
            OID_RANGE_MULTIRANGE_CONTAINED_OP
            | OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP
            /* only empty ranges are <= an empty multirange */
            | OID_MULTIRANGE_LESS_EQUAL_OP => {
                selec = empty_frac as f64;
            }

            /* everything contains an empty multirange */
            OID_MULTIRANGE_CONTAINS_RANGE_OP
            | OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP
            /* everything is >= an empty multirange */
            | OID_MULTIRANGE_GREATER_EQUAL_OP => {
                selec = 1.0;
            }

            /* all non-empty multiranges are > an empty multirange */
            OID_MULTIRANGE_GREATER_OP => {
                selec = 1.0 - empty_frac as f64;
            }

            /* an element cannot be empty */
            OID_MULTIRANGE_CONTAINS_ELEM_OP
            /* filtered out by multirangesel() */
            | OID_RANGE_OVERLAPS_MULTIRANGE_OP
            | OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP
            | OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP
            | OID_RANGE_LEFT_MULTIRANGE_OP
            | OID_RANGE_RIGHT_MULTIRANGE_OP
            | OID_RANGE_CONTAINS_MULTIRANGE_OP
            | OID_MULTIRANGE_ELEM_CONTAINED_OP
            | OID_MULTIRANGE_RANGE_CONTAINED_OP
            | _ => {
                elog!(ERROR, "unexpected operator {}", operator);
                selec = 0.0; /* keep compiler quiet */
            }
        }
    } else {
        /*
         * Calculate selectivity using bound histograms. If that fails for
         * some reason, e.g no histogram in pg_statistic, use the default
         * constant estimate for the fraction of non-empty values. This is
         * still somewhat better than just returning the default estimate,
         * because this still takes into account the fraction of empty and
         * NULL tuples, if we had statistics for them.
         */
        let mut hist_selec = calc_hist_selectivity(typcache, vardata, constval, operator);
        if hist_selec < 0.0 {
            hist_selec = default_multirange_selectivity(operator);
        }

        /*
         * Now merge the results for the empty multiranges and histogram
         * calculations, realizing that the histogram covers only the
         * non-null, non-empty values.
         */
        if operator == OID_RANGE_MULTIRANGE_CONTAINED_OP
            || operator == OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP
        {
            /* empty is contained by anything non-empty */
            selec = (1.0 - empty_frac as f64) * hist_selec + empty_frac as f64;
        } else {
            /* with any other operator, empty Op non-empty matches nothing */
            selec = (1.0 - empty_frac as f64) * hist_selec;
        }
    }

    /* all multirange operators are strict */
    selec *= 1.0 - null_frac as f64;

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY!(selec);

    selec
}

/*
 * Calculate multirange operator selectivity using histograms of multirange bounds.
 *
 * This estimate is for the portion of values that are not empty and not
 * NULL.
 */
unsafe fn calc_hist_selectivity(
    typcache: *mut TypeCacheEntry,
    vardata: *mut VariableStatData,
    constval: *const MultirangeType,
    operator: Oid,
) -> f64 {
    let rng_typcache: *mut TypeCacheEntry = (*typcache).rngtype;
    let mut hslot: AttStatsSlot = std::mem::zeroed();
    let mut lslot: AttStatsSlot = std::mem::zeroed();
    let nhist: c_int;
    let hist_lower: *mut RangeBound;
    let hist_upper: *mut RangeBound;
    let mut i: c_int;
    let mut const_lower: RangeBound = std::mem::zeroed();
    let mut const_upper: RangeBound = std::mem::zeroed();
    let mut tmp: RangeBound = std::mem::zeroed();
    let hist_selec: f64;

    /* Can't use the histogram with insecure multirange support functions */
    if !statistic_proc_security_check(vardata, (*rng_typcache).rng_cmp_proc_finfo.fn_oid) {
        return -1.0;
    }
    if OidIsValid((*rng_typcache).rng_subdiff_finfo.fn_oid)
        && !statistic_proc_security_check(vardata, (*rng_typcache).rng_subdiff_finfo.fn_oid)
    {
        return -1.0;
    }

    /* Try to get histogram of ranges */
    if !(HeapTupleIsValid((*vardata).statsTuple)
        && get_attstatsslot(
            &raw mut hslot,
            (*vardata).statsTuple as *mut c_void,
            STATISTIC_KIND_BOUNDS_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        ))
    {
        return -1.0;
    }

    /* check that it's a histogram, not just a dummy entry */
    if hslot.nvalues < 2 {
        free_attstatsslot(&raw mut hslot);
        return -1.0;
    }

    /*
     * Convert histogram of ranges into histograms of its lower and upper
     * bounds.
     */
    nhist = hslot.nvalues;
    hist_lower = palloc(std::mem::size_of::<RangeBound>() * nhist as usize) as *mut RangeBound;
    hist_upper = palloc(std::mem::size_of::<RangeBound>() * nhist as usize) as *mut RangeBound;
    i = 0;
    while i < nhist {
        let mut empty: bool = false;

        range_deserialize(
            rng_typcache,
            DatumGetRangeTypeP(*hslot.values.add(i as usize)),
            hist_lower.add(i as usize),
            hist_upper.add(i as usize),
            &raw mut empty,
        );
        /* The histogram should not contain any empty ranges */
        if empty {
            elog!(ERROR, "bounds histogram contains an empty range");
        }
        i += 1;
    }

    /* @> and @< also need a histogram of range lengths */
    if operator == OID_MULTIRANGE_CONTAINS_RANGE_OP
        || operator == OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP
        || operator == OID_MULTIRANGE_RANGE_CONTAINED_OP
        || operator == OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP
    {
        if !(HeapTupleIsValid((*vardata).statsTuple)
            && get_attstatsslot(
                &raw mut lslot,
                (*vardata).statsTuple as *mut c_void,
                STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
                InvalidOid,
                ATTSTATSSLOT_VALUES,
            ))
        {
            free_attstatsslot(&raw mut hslot);
            return -1.0;
        }

        /* check that it's a histogram, not just a dummy entry */
        if lslot.nvalues < 2 {
            free_attstatsslot(&raw mut lslot);
            free_attstatsslot(&raw mut hslot);
            return -1.0;
        }
    } else {
        std::ptr::write_bytes(&raw mut lslot, 0, 1);
    }

    /* Extract the bounds of the constant value. */
    Assert!((*constval).rangeCount > 0);
    multirange_get_bounds(
        rng_typcache,
        constval,
        0,
        &raw mut const_lower,
        &raw mut tmp,
    );
    multirange_get_bounds(
        rng_typcache,
        constval,
        (*constval).rangeCount - 1,
        &raw mut tmp,
        &raw mut const_upper,
    );

    /*
     * Calculate selectivity comparing the lower or upper bound of the
     * constant with the histogram of lower or upper bounds.
     */
    match operator {
        OID_MULTIRANGE_LESS_OP => {
            /*
             * The regular b-tree comparison operators (<, <=, >, >=) compare
             * the lower bounds first, and the upper bounds for values with
             * equal lower bounds. Estimate that by comparing the lower bounds
             * only. This gives a fairly accurate estimate assuming there
             * aren't many rows with a lower bound equal to the constant's
             * lower bound.
             */
            hist_selec = calc_hist_selectivity_scalar(
                rng_typcache,
                &raw const const_lower,
                hist_lower,
                nhist,
                false,
            );
        }

        OID_MULTIRANGE_LESS_EQUAL_OP => {
            hist_selec = calc_hist_selectivity_scalar(
                rng_typcache,
                &raw const const_lower,
                hist_lower,
                nhist,
                true,
            );
        }

        OID_MULTIRANGE_GREATER_OP => {
            hist_selec = 1.0
                - calc_hist_selectivity_scalar(
                    rng_typcache,
                    &raw const const_lower,
                    hist_lower,
                    nhist,
                    false,
                );
        }

        OID_MULTIRANGE_GREATER_EQUAL_OP => {
            hist_selec = 1.0
                - calc_hist_selectivity_scalar(
                    rng_typcache,
                    &raw const const_lower,
                    hist_lower,
                    nhist,
                    true,
                );
        }

        OID_MULTIRANGE_LEFT_RANGE_OP | OID_MULTIRANGE_LEFT_MULTIRANGE_OP => {
            /* var << const when upper(var) < lower(const) */
            hist_selec = calc_hist_selectivity_scalar(
                rng_typcache,
                &raw const const_lower,
                hist_upper,
                nhist,
                false,
            );
        }

        OID_MULTIRANGE_RIGHT_RANGE_OP | OID_MULTIRANGE_RIGHT_MULTIRANGE_OP => {
            /* var >> const when lower(var) > upper(const) */
            hist_selec = 1.0
                - calc_hist_selectivity_scalar(
                    rng_typcache,
                    &raw const const_upper,
                    hist_lower,
                    nhist,
                    true,
                );
        }

        OID_MULTIRANGE_OVERLAPS_RIGHT_RANGE_OP | OID_MULTIRANGE_OVERLAPS_RIGHT_MULTIRANGE_OP => {
            /* compare lower bounds */
            hist_selec = 1.0
                - calc_hist_selectivity_scalar(
                    rng_typcache,
                    &raw const const_lower,
                    hist_lower,
                    nhist,
                    false,
                );
        }

        OID_MULTIRANGE_OVERLAPS_LEFT_RANGE_OP | OID_MULTIRANGE_OVERLAPS_LEFT_MULTIRANGE_OP => {
            /* compare upper bounds */
            hist_selec = calc_hist_selectivity_scalar(
                rng_typcache,
                &raw const const_upper,
                hist_upper,
                nhist,
                true,
            );
        }

        OID_MULTIRANGE_OVERLAPS_RANGE_OP
        | OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP
        | OID_MULTIRANGE_CONTAINS_ELEM_OP => {
            /*
             * A && B <=> NOT (A << B OR A >> B).
             *
             * Since A << B and A >> B are mutually exclusive events we can
             * sum their probabilities to find probability of (A << B OR A >>
             * B).
             *
             * "multirange @> elem" is equivalent to "multirange &&
             * {[elem,elem]}". The caller already constructed the singular
             * range from the element constant, so just treat it the same as
             * &&.
             */
            let mut hs = calc_hist_selectivity_scalar(
                rng_typcache,
                &raw const const_lower,
                hist_upper,
                nhist,
                false,
            );
            hs += 1.0
                - calc_hist_selectivity_scalar(
                    rng_typcache,
                    &raw const const_upper,
                    hist_lower,
                    nhist,
                    true,
                );
            hist_selec = 1.0 - hs;
        }

        OID_MULTIRANGE_CONTAINS_RANGE_OP | OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP => {
            hist_selec = calc_hist_selectivity_contains(
                rng_typcache,
                &raw const const_lower,
                &raw const const_upper,
                hist_lower,
                nhist,
                lslot.values,
                lslot.nvalues,
            );
        }

        OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP | OID_RANGE_MULTIRANGE_CONTAINED_OP => {
            if const_lower.infinite {
                /*
                 * Lower bound no longer matters. Just estimate the fraction
                 * with an upper bound <= const upper bound
                 */
                hist_selec = calc_hist_selectivity_scalar(
                    rng_typcache,
                    &raw const const_upper,
                    hist_upper,
                    nhist,
                    true,
                );
            } else if const_upper.infinite {
                hist_selec = 1.0
                    - calc_hist_selectivity_scalar(
                        rng_typcache,
                        &raw const const_lower,
                        hist_lower,
                        nhist,
                        false,
                    );
            } else {
                hist_selec = calc_hist_selectivity_contained(
                    rng_typcache,
                    &raw const const_lower,
                    &raw mut const_upper,
                    hist_lower,
                    nhist,
                    lslot.values,
                    lslot.nvalues,
                );
            }
        }

        /* filtered out by multirangesel() */
        OID_RANGE_OVERLAPS_MULTIRANGE_OP
        | OID_RANGE_OVERLAPS_LEFT_MULTIRANGE_OP
        | OID_RANGE_OVERLAPS_RIGHT_MULTIRANGE_OP
        | OID_RANGE_LEFT_MULTIRANGE_OP
        | OID_RANGE_RIGHT_MULTIRANGE_OP
        | OID_RANGE_CONTAINS_MULTIRANGE_OP
        | OID_MULTIRANGE_ELEM_CONTAINED_OP
        | OID_MULTIRANGE_RANGE_CONTAINED_OP
        | _ => {
            elog!(ERROR, "unknown multirange operator {}", operator);
            hist_selec = -1.0; /* keep compiler quiet */
        }
    }

    free_attstatsslot(&raw mut lslot);
    free_attstatsslot(&raw mut hslot);

    hist_selec
}

/*
 * Look up the fraction of values less than (or equal, if 'equal' argument
 * is true) a given const in a histogram of range bounds.
 */
unsafe fn calc_hist_selectivity_scalar(
    typcache: *mut TypeCacheEntry,
    constbound: *const RangeBound,
    hist: *const RangeBound,
    hist_nvalues: c_int,
    equal: bool,
) -> f64 {
    let mut selec: Selectivity;
    let index: c_int;

    /*
     * Find the histogram bin the given constant falls into. Estimate
     * selectivity as the number of preceding whole bins.
     */
    index = rbound_bsearch(typcache, constbound, hist, hist_nvalues, equal);
    selec = (Max!(index, 0)) as Selectivity / (hist_nvalues - 1) as Selectivity;

    /* Adjust using linear interpolation within the bin */
    if index >= 0 && index < hist_nvalues - 1 {
        selec += get_position(
            typcache,
            constbound,
            hist.add(index as usize),
            hist.add((index + 1) as usize),
        ) / (hist_nvalues - 1) as Selectivity;
    }

    selec
}

/*
 * Binary search on an array of range bounds. Returns greatest index of range
 * bound in array which is less(less or equal) than given range bound. If all
 * range bounds in array are greater or equal(greater) than given range bound,
 * return -1. When "equal" flag is set conditions in brackets are used.
 *
 * This function is used in scalar operator selectivity estimation. Another
 * goal of this function is to find a histogram bin where to stop
 * interpolation of portion of bounds which are less than or equal to given bound.
 */
unsafe fn rbound_bsearch(
    typcache: *mut TypeCacheEntry,
    value: *const RangeBound,
    hist: *const RangeBound,
    hist_length: c_int,
    equal: bool,
) -> c_int {
    let mut lower: c_int = -1;
    let mut upper: c_int = hist_length - 1;
    let mut cmp: c_int;
    let mut middle: c_int;

    while lower < upper {
        middle = (lower + upper + 1) / 2;
        cmp = range_cmp_bounds(typcache, hist.add(middle as usize), value);

        if cmp < 0 || (equal && cmp == 0) {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }
    lower
}

/*
 * Binary search on length histogram. Returns greatest index of range length in
 * histogram which is less than (less than or equal) the given length value. If
 * all lengths in the histogram are greater than (greater than or equal) the
 * given length, returns -1.
 */
unsafe fn length_hist_bsearch(
    length_hist_values: *mut Datum,
    length_hist_nvalues: c_int,
    value: f64,
    equal: bool,
) -> c_int {
    let mut lower: c_int = -1;
    let mut upper: c_int = length_hist_nvalues - 1;
    let mut middle: c_int;

    while lower < upper {
        let middleval: f64;

        middle = (lower + upper + 1) / 2;

        middleval = DatumGetFloat8(*length_hist_values.add(middle as usize));
        if middleval < value || (equal && middleval <= value) {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }
    lower
}

/*
 * Get relative position of value in histogram bin in [0,1] range.
 */
unsafe fn get_position(
    typcache: *mut TypeCacheEntry,
    value: *const RangeBound,
    hist1: *const RangeBound,
    hist2: *const RangeBound,
) -> float8 {
    let has_subdiff: bool = OidIsValid((*typcache).rng_subdiff_finfo.fn_oid);
    let mut position: float8;

    if !(*hist1).infinite && !(*hist2).infinite {
        let bin_width: float8;

        /*
         * Both bounds are finite. Assuming the subtype's comparison function
         * works sanely, the value must be finite, too, because it lies
         * somewhere between the bounds.  If it doesn't, arbitrarily return
         * 0.5.
         */
        if (*value).infinite {
            return 0.5;
        }

        /* Can't interpolate without subdiff function */
        if !has_subdiff {
            return 0.5;
        }

        /* Calculate relative position using subdiff function. */
        bin_width = DatumGetFloat8(FunctionCall2Coll(
            &raw mut (*typcache).rng_subdiff_finfo,
            (*typcache).rng_collation,
            (*hist2).val,
            (*hist1).val,
        ));
        if bin_width.is_nan() || bin_width <= 0.0 {
            return 0.5; /* punt for NaN or zero-width bin */
        }

        position = DatumGetFloat8(FunctionCall2Coll(
            &raw mut (*typcache).rng_subdiff_finfo,
            (*typcache).rng_collation,
            (*value).val,
            (*hist1).val,
        )) / bin_width;

        if position.is_nan() {
            return 0.5; /* punt for NaN from subdiff, Inf/Inf, etc */
        }

        /* Relative position must be in [0,1] range */
        position = Max!(position, 0.0);
        position = Min!(position, 1.0);
        position
    } else if (*hist1).infinite && !(*hist2).infinite {
        /*
         * Lower bin boundary is -infinite, upper is finite. If the value is
         * -infinite, return 0.0 to indicate it's equal to the lower bound.
         * Otherwise return 1.0 to indicate it's infinitely far from the lower
         * bound.
         */
        if (*value).infinite && (*value).lower {
            0.0
        } else {
            1.0
        }
    } else if !(*hist1).infinite && (*hist2).infinite {
        /* same as above, but in reverse */
        if (*value).infinite && !(*value).lower {
            1.0
        } else {
            0.0
        }
    } else {
        /*
         * If both bin boundaries are infinite, they should be equal to each
         * other, and the value should also be infinite and equal to both
         * bounds. (But don't Assert that, to avoid crashing if a user creates
         * a datatype with a broken comparison function).
         *
         * Assume the value to lie in the middle of the infinite bounds.
         */
        0.5
    }
}

/*
 * Get relative position of value in a length histogram bin in [0,1] range.
 */
unsafe fn get_len_position(value: f64, hist1: f64, hist2: f64) -> f64 {
    if !hist1.is_infinite() && !hist2.is_infinite() {
        /*
         * Both bounds are finite. The value should be finite too, because it
         * lies somewhere between the bounds. If it doesn't, just return
         * something.
         */
        if value.is_infinite() {
            return 0.5;
        }

        1.0 - (hist2 - value) / (hist2 - hist1)
    } else if hist1.is_infinite() && !hist2.is_infinite() {
        /*
         * Lower bin boundary is -infinite, upper is finite. Return 1.0 to
         * indicate the value is infinitely far from the lower bound.
         */
        1.0
    } else if hist1.is_infinite() && hist2.is_infinite() {
        /* same as above, but in reverse */
        0.0
    } else {
        /*
         * If both bin boundaries are infinite, they should be equal to each
         * other, and the value should also be infinite and equal to both
         * bounds. (But don't Assert that, to avoid crashing unnecessarily if
         * the caller messes up)
         *
         * Assume the value to lie in the middle of the infinite bounds.
         */
        0.5
    }
}

/*
 * Measure distance between two range bounds.
 */
unsafe fn get_distance(
    typcache: *mut TypeCacheEntry,
    bound1: *const RangeBound,
    bound2: *const RangeBound,
) -> float8 {
    let has_subdiff: bool = OidIsValid((*typcache).rng_subdiff_finfo.fn_oid);

    if !(*bound1).infinite && !(*bound2).infinite {
        /*
         * Neither bound is infinite, use subdiff function or return default
         * value of 1.0 if no subdiff is available.
         */
        if has_subdiff {
            let res: float8;

            res = DatumGetFloat8(FunctionCall2Coll(
                &raw mut (*typcache).rng_subdiff_finfo,
                (*typcache).rng_collation,
                (*bound2).val,
                (*bound1).val,
            ));
            /* Reject possible NaN result, also negative result */
            if res.is_nan() || res < 0.0 {
                1.0
            } else {
                res
            }
        } else {
            1.0
        }
    } else if (*bound1).infinite && (*bound2).infinite {
        /* Both bounds are infinite */
        if (*bound1).lower == (*bound2).lower {
            0.0
        } else {
            get_float8_infinity()
        }
    } else {
        /* One bound is infinite, the other is not */
        get_float8_infinity()
    }
}

/*
 * Calculate the average of function P(x), in the interval [length1, length2],
 * where P(x) is the fraction of tuples with length < x (or length <= x if
 * 'equal' is true).
 */
unsafe fn calc_length_hist_frac(
    length_hist_values: *mut Datum,
    length_hist_nvalues: c_int,
    length1: f64,
    length2: f64,
    equal: bool,
) -> f64 {
    let frac: f64;
    let mut a: f64;
    let mut b: f64;
    let mut pa: f64;
    let mut pb: f64;
    let mut pos: f64;
    let mut i: c_int;
    let mut area: f64;

    Assert!(length2 >= length1);

    if length2 < 0.0 {
        return 0.0; /* shouldn't happen, but doesn't hurt to check */
    }

    /* All lengths in the table are <= infinite. */
    if length2.is_infinite() && equal {
        return 1.0;
    }

    /*----------
     * The average of a function between A and B can be calculated by the
     * formula:
     *
     *			B
     *	  1		/
     * -------	| P(x)dx
     *	B - A	/
     *			A
     *
     * The geometrical interpretation of the integral is the area under the
     * graph of P(x). P(x) is defined by the length histogram. We calculate
     * the area in a piecewise fashion, iterating through the length histogram
     * bins. Each bin is a trapezoid:
     *
     *		 P(x2)
     *		  /|
     *		 / |
     * P(x1)/  |
     *	   |   |
     *	   |   |
     *	---+---+--
     *	   x1  x2
     *
     * where x1 and x2 are the boundaries of the current histogram, and P(x1)
     * and P(x1) are the cumulative fraction of tuples at the boundaries.
     *
     * The area of each trapezoid is 1/2 * (P(x2) + P(x1)) * (x2 - x1)
     *
     * The first bin contains the lower bound passed by the caller, so we
     * use linear interpolation between the previous and next histogram bin
     * boundary to calculate P(x1). Likewise for the last bin: we use linear
     * interpolation to calculate P(x2). For the bins in between, x1 and x2
     * lie on histogram bin boundaries, so P(x1) and P(x2) are simply:
     * P(x1) =	  (bin index) / (number of bins)
     * P(x2) = (bin index + 1 / (number of bins)
     */

    /* First bin, the one that contains lower bound */
    i = length_hist_bsearch(length_hist_values, length_hist_nvalues, length1, equal);
    if i >= length_hist_nvalues - 1 {
        return 1.0;
    }

    if i < 0 {
        i = 0;
        pos = 0.0;
    } else {
        /* interpolate length1's position in the bin */
        pos = get_len_position(
            length1,
            DatumGetFloat8(*length_hist_values.add(i as usize)),
            DatumGetFloat8(*length_hist_values.add((i + 1) as usize)),
        );
    }
    pb = (i as f64 + pos) / (length_hist_nvalues - 1) as f64;
    b = length1;

    /*
     * In the degenerate case that length1 == length2, simply return
     * P(length1). This is not merely an optimization: if length1 == length2,
     * we'd divide by zero later on.
     */
    if length2 == length1 {
        return pb;
    }

    /*
     * Loop through all the bins, until we hit the last bin, the one that
     * contains the upper bound. (if lower and upper bounds are in the same
     * bin, this falls out immediately)
     */
    area = 0.0;
    while i < length_hist_nvalues - 1 {
        let bin_upper: f64 = DatumGetFloat8(*length_hist_values.add((i + 1) as usize));

        /* check if we've reached the last bin */
        if !(bin_upper < length2 || (equal && bin_upper <= length2)) {
            break;
        }

        /* the upper bound of previous bin is the lower bound of this bin */
        a = b;
        pa = pb;

        b = bin_upper;
        pb = i as f64 / (length_hist_nvalues - 1) as f64;

        /*
         * Add the area of this trapezoid to the total. The point of the
         * if-check is to avoid NaN, in the corner case that PA == PB == 0,
         * and B - A == Inf. The area of a zero-height trapezoid (PA == PB ==
         * 0) is zero, regardless of the width (B - A).
         */
        if pa > 0.0 || pb > 0.0 {
            area += 0.5 * (pb + pa) * (b - a);
        }

        i += 1;
    }

    /* Last bin */
    a = b;
    pa = pb;

    b = length2; /* last bin ends at the query upper bound */
    if i >= length_hist_nvalues - 1 {
        pos = 0.0;
    } else {
        if DatumGetFloat8(*length_hist_values.add(i as usize))
            == DatumGetFloat8(*length_hist_values.add((i + 1) as usize))
        {
            pos = 0.0;
        } else {
            pos = get_len_position(
                length2,
                DatumGetFloat8(*length_hist_values.add(i as usize)),
                DatumGetFloat8(*length_hist_values.add((i + 1) as usize)),
            );
        }
    }
    pb = (i as f64 + pos) / (length_hist_nvalues - 1) as f64;

    if pa > 0.0 || pb > 0.0 {
        area += 0.5 * (pb + pa) * (b - a);
    }

    /*
     * Ok, we have calculated the area, ie. the integral. Divide by width to
     * get the requested average.
     *
     * Avoid NaN arising from infinite / infinite. This happens at least if
     * length2 is infinite. It's not clear what the correct value would be in
     * that case, so 0.5 seems as good as any value.
     */
    if area.is_infinite() && length2.is_infinite() {
        frac = 0.5;
    } else {
        frac = area / (length2 - length1);
    }

    frac
}

/*
 * Calculate selectivity of "var <@ const" operator, ie. estimate the fraction
 * of multiranges that fall within the constant lower and upper bounds. This uses
 * the histograms of range lower bounds and range lengths, on the assumption
 * that the range lengths are independent of the lower bounds.
 *
 * The caller has already checked that constant lower and upper bounds are
 * finite.
 */
unsafe fn calc_hist_selectivity_contained(
    typcache: *mut TypeCacheEntry,
    lower: *const RangeBound,
    upper: *mut RangeBound,
    hist_lower: *const RangeBound,
    hist_nvalues: c_int,
    length_hist_values: *mut Datum,
    length_hist_nvalues: c_int,
) -> f64 {
    let mut i: c_int;
    let mut upper_index: c_int;
    let mut prev_dist: float8;
    let mut bin_width: f64;
    let upper_bin_width: f64;
    let mut sum_frac: f64;

    /*
     * Begin by finding the bin containing the upper bound, in the lower bound
     * histogram. Any range with a lower bound > constant upper bound can't
     * match, ie. there are no matches in bins greater than upper_index.
     */
    (*upper).inclusive = !(*upper).inclusive;
    (*upper).lower = true;
    upper_index = rbound_bsearch(typcache, upper, hist_lower, hist_nvalues, false);

    /*
     * If the upper bound value is below the histogram's lower limit, there
     * are no matches.
     */
    if upper_index < 0 {
        return 0.0;
    }

    /*
     * If the upper bound value is at or beyond the histogram's upper limit,
     * start our loop at the last actual bin, as though the upper bound were
     * within that bin; get_position will clamp its result to 1.0 anyway.
     * (This corresponds to assuming that the data population above the
     * histogram's upper limit is empty, exactly like what we just assumed for
     * the lower limit.)
     */
    upper_index = Min!(upper_index, hist_nvalues - 2);

    /*
     * Calculate upper_bin_width, ie. the fraction of the (upper_index,
     * upper_index + 1) bin which is greater than upper bound of query range
     * using linear interpolation of subdiff function.
     */
    upper_bin_width = get_position(
        typcache,
        upper,
        hist_lower.add(upper_index as usize),
        hist_lower.add((upper_index + 1) as usize),
    );

    /*
     * In the loop, dist and prev_dist are the distance of the "current" bin's
     * lower and upper bounds from the constant upper bound.
     *
     * bin_width represents the width of the current bin. Normally it is 1.0,
     * meaning a full width bin, but can be less in the corner cases: start
     * and end of the loop. We start with bin_width = upper_bin_width, because
     * we begin at the bin containing the upper bound.
     */
    prev_dist = 0.0;
    bin_width = upper_bin_width;

    sum_frac = 0.0;
    i = upper_index;
    while i >= 0 {
        let dist: f64;
        let length_hist_frac: f64;
        let mut final_bin: bool = false;

        /*
         * dist -- distance from upper bound of query range to lower bound of
         * the current bin in the lower bound histogram. Or to the lower bound
         * of the constant range, if this is the final bin, containing the
         * constant lower bound.
         */
        if range_cmp_bounds(typcache, hist_lower.add(i as usize), lower) < 0 {
            dist = get_distance(typcache, lower, upper);

            /*
             * Subtract from bin_width the portion of this bin that we want to
             * ignore.
             */
            bin_width -= get_position(
                typcache,
                lower,
                hist_lower.add(i as usize),
                hist_lower.add((i + 1) as usize),
            );
            if bin_width < 0.0 {
                bin_width = 0.0;
            }
            final_bin = true;
        } else {
            dist = get_distance(typcache, hist_lower.add(i as usize), upper);
        }

        /*
         * Estimate the fraction of tuples in this bin that are narrow enough
         * to not exceed the distance to the upper bound of the query range.
         */
        length_hist_frac = calc_length_hist_frac(
            length_hist_values,
            length_hist_nvalues,
            prev_dist,
            dist,
            true,
        );

        /*
         * Add the fraction of tuples in this bin, with a suitable length, to
         * the total.
         */
        sum_frac += length_hist_frac * bin_width / (hist_nvalues - 1) as f64;

        if final_bin {
            break;
        }

        bin_width = 1.0;
        prev_dist = dist;

        i -= 1;
    }

    sum_frac
}

/*
 * Calculate selectivity of "var @> const" operator, ie. estimate the fraction
 * of multiranges that contain the constant lower and upper bounds. This uses
 * the histograms of range lower bounds and range lengths, on the assumption
 * that the range lengths are independent of the lower bounds.
 */
unsafe fn calc_hist_selectivity_contains(
    typcache: *mut TypeCacheEntry,
    lower: *const RangeBound,
    upper: *const RangeBound,
    hist_lower: *const RangeBound,
    hist_nvalues: c_int,
    length_hist_values: *mut Datum,
    length_hist_nvalues: c_int,
) -> f64 {
    let mut i: c_int;
    let mut lower_index: c_int;
    let mut bin_width: f64;
    let lower_bin_width: f64;
    let mut sum_frac: f64;
    let mut prev_dist: float8;

    /* Find the bin containing the lower bound of query range. */
    lower_index = rbound_bsearch(typcache, lower, hist_lower, hist_nvalues, true);

    /*
     * If the lower bound value is below the histogram's lower limit, there
     * are no matches.
     */
    if lower_index < 0 {
        return 0.0;
    }

    /*
     * If the lower bound value is at or beyond the histogram's upper limit,
     * start our loop at the last actual bin, as though the upper bound were
     * within that bin; get_position will clamp its result to 1.0 anyway.
     * (This corresponds to assuming that the data population above the
     * histogram's upper limit is empty, exactly like what we just assumed for
     * the lower limit.)
     */
    lower_index = Min!(lower_index, hist_nvalues - 2);

    /*
     * Calculate lower_bin_width, ie. the fraction of the of (lower_index,
     * lower_index + 1) bin which is greater than lower bound of query range
     * using linear interpolation of subdiff function.
     */
    lower_bin_width = get_position(
        typcache,
        lower,
        hist_lower.add(lower_index as usize),
        hist_lower.add((lower_index + 1) as usize),
    );

    /*
     * Loop through all the lower bound bins, smaller than the query lower
     * bound. In the loop, dist and prev_dist are the distance of the
     * "current" bin's lower and upper bounds from the constant upper bound.
     * We begin from query lower bound, and walk backwards, so the first bin's
     * upper bound is the query lower bound, and its distance to the query
     * upper bound is the length of the query range.
     *
     * bin_width represents the width of the current bin. Normally it is 1.0,
     * meaning a full width bin, except for the first bin, which is only
     * counted up to the constant lower bound.
     */
    prev_dist = get_distance(typcache, lower, upper);
    sum_frac = 0.0;
    bin_width = lower_bin_width;
    i = lower_index;
    while i >= 0 {
        let dist: float8;
        let length_hist_frac: f64;

        /*
         * dist -- distance from upper bound of query range to current value
         * of lower bound histogram or lower bound of query range (if we've
         * reach it).
         */
        dist = get_distance(typcache, hist_lower.add(i as usize), upper);

        /*
         * Get average fraction of length histogram which covers intervals
         * longer than (or equal to) distance to upper bound of query range.
         */
        length_hist_frac = 1.0
            - calc_length_hist_frac(
                length_hist_values,
                length_hist_nvalues,
                prev_dist,
                dist,
                false,
            );

        sum_frac += length_hist_frac * bin_width / (hist_nvalues - 1) as f64;

        bin_width = 1.0;
        prev_dist = dist;

        i -= 1;
    }

    sum_frac
}

// ---------------------------------------------------------------------------
// Imports that are only referenced indirectly above (kept here to document
// their home modules and avoid accidental unused-import churn).
// ---------------------------------------------------------------------------
use crate::c::int16;
use crate::nodes::pathnodes::PlannerInfo;
use crate::nodes::pg_list::List;
