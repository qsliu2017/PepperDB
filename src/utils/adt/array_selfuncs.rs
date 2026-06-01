//! array_selfuncs.rs
//!   Functions for selectivity estimation of array operators
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/array_selfuncs.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/array_selfuncs.c

// #include "postgres.h"
use crate::prelude::*;

// #include <math.h> -> libm via f64 methods (exp/sqrt)

// Core node/fmgr types used throughout.
use crate::nodes::nodes::{Node, Selectivity};
use crate::nodes::pg_list::List;
use crate::nodes::primnodes::Const;
use crate::utils::fmgr::{FmgrInfo, FunctionCallInfo};

// catalog/pg_statistic.h
use crate::catalog::pg_statistic::Form_pg_statistic;

// catalog/pg_operator.h: array operator OIDs
use crate::catalog::pg_known_oids::{
    OID_ARRAY_CONTAINED_OP, OID_ARRAY_CONTAINS_OP, OID_ARRAY_OVERLAP_OP,
};

// access/htup_details.h
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};

// #[macro_export] macros live at the crate root.
use crate::{IsA, PG_GETARG_INT32, PG_GETARG_OID, PG_GETARG_POINTER, PG_RETURN_FLOAT8};

use std::ffi::{c_int, c_void};

/* Default selectivity constant for "@>" and "<@" operators */
const DEFAULT_CONTAIN_SEL: f64 = 0.005;

/* Default selectivity constant for "&&" operator */
const DEFAULT_OVERLAP_SEL: f64 = 0.01;

/* Default selectivity for given operator */
#[inline]
fn DEFAULT_SEL(operator: Oid) -> f64 {
    if operator == OID_ARRAY_OVERLAP_OP {
        DEFAULT_OVERLAP_SEL
    } else {
        DEFAULT_CONTAIN_SEL
    }
}

/*
 * CLAMP_PROBABILITY (from selfuncs.h): force a probability estimate into [0,1].
 * selfuncs.h is not yet ported, so inline the macro here.
 */
#[inline]
fn CLAMP_PROBABILITY(p: &mut Selectivity) {
    if *p < 0.0 {
        *p = 0.0;
    } else if *p > 1.0 {
        *p = 1.0;
    }
}

/*
 * scalararraysel_containment
 *		Estimate selectivity of ScalarArrayOpExpr via array containment.
 *
 * If we have const =/<> ANY/ALL (array_var) then we can estimate the
 * selectivity as though this were an array containment operator,
 * array_var op ARRAY[const].
 *
 * scalararraysel() has already verified that the ScalarArrayOpExpr's operator
 * is the array element type's default equality or inequality operator, and
 * has aggressively simplified both inputs to constants.
 *
 * Returns selectivity (0..1), or -1 if we fail to estimate selectivity.
 */
pub unsafe fn scalararraysel_containment(
    root: *mut PlannerInfo,
    leftop: *mut Node,
    rightop: *mut Node,
    elemtype: Oid,
    isEquality: bool,
    mut useOr: bool,
    varRelid: c_int,
) -> Selectivity {
    let mut selec: Selectivity;
    let mut vardata: VariableStatData = std::mem::zeroed();
    let constval: Datum;
    let typentry: *mut TypeCacheEntry;
    let cmpfunc: *mut FmgrInfo;

    /*
     * rightop must be a variable, else punt.
     */
    examine_variable(root, rightop, varRelid, &mut vardata);
    if vardata.rel.is_null() {
        ReleaseVariableStats(&mut vardata);
        return -1.0;
    }

    /*
     * leftop must be a constant, else punt.
     */
    if !IsA!(leftop, T_Const) {
        ReleaseVariableStats(&mut vardata);
        return -1.0;
    }
    if (*(leftop as *mut Const)).constisnull {
        /* qual can't succeed if null on left */
        ReleaseVariableStats(&mut vardata);
        return 0.0 as Selectivity;
    }
    constval = (*(leftop as *mut Const)).constvalue;

    /* Get element type's default comparison function */
    typentry = lookup_type_cache(elemtype, TYPECACHE_CMP_PROC_FINFO);
    if !OidIsValid((*typentry).cmp_proc_finfo.fn_oid) {
        ReleaseVariableStats(&mut vardata);
        return -1.0;
    }
    cmpfunc = &mut (*typentry).cmp_proc_finfo;

    /*
     * If the operator is <>, swap ANY/ALL, then invert the result later.
     */
    if !isEquality {
        useOr = !useOr;
    }

    /* Get array element stats for var, if available */
    if HeapTupleIsValid(vardata.statsTuple)
        && statistic_proc_security_check(&mut vardata, (*cmpfunc).fn_oid)
    {
        let stats: Form_pg_statistic;
        let mut sslot: AttStatsSlot = std::mem::zeroed();
        let mut hslot: AttStatsSlot = std::mem::zeroed();

        stats = GETSTRUCT(vardata.statsTuple) as Form_pg_statistic;

        /* MCELEM will be an array of same type as element */
        if get_attstatsslot(
            &mut sslot,
            vardata.statsTuple,
            STATISTIC_KIND_MCELEM,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        ) {
            /* For ALL case, also get histogram of distinct-element counts */
            if useOr
                || !get_attstatsslot(
                    &mut hslot,
                    vardata.statsTuple,
                    STATISTIC_KIND_DECHIST,
                    InvalidOid,
                    ATTSTATSSLOT_NUMBERS,
                )
            {
                std::ptr::write_bytes(&mut hslot as *mut AttStatsSlot, 0, 1);
            }

            /*
             * For = ANY, estimate as var @> ARRAY[const].
             *
             * For = ALL, estimate as var <@ ARRAY[const].
             */
            if useOr {
                selec = mcelem_array_contain_overlap_selec(
                    sslot.values,
                    sslot.nvalues,
                    sslot.numbers,
                    sslot.nnumbers,
                    &constval as *const Datum as *mut Datum,
                    1,
                    OID_ARRAY_CONTAINS_OP,
                    typentry,
                );
            } else {
                selec = mcelem_array_contained_selec(
                    sslot.values,
                    sslot.nvalues,
                    sslot.numbers,
                    sslot.nnumbers,
                    &constval as *const Datum as *mut Datum,
                    1,
                    hslot.numbers,
                    hslot.nnumbers,
                    OID_ARRAY_CONTAINED_OP,
                    typentry,
                );
            }

            free_attstatsslot(&mut hslot);
            free_attstatsslot(&mut sslot);
        } else {
            /* No most-common-elements info, so do without */
            if useOr {
                selec = mcelem_array_contain_overlap_selec(
                    null_mut(),
                    0,
                    null_mut(),
                    0,
                    &constval as *const Datum as *mut Datum,
                    1,
                    OID_ARRAY_CONTAINS_OP,
                    typentry,
                );
            } else {
                selec = mcelem_array_contained_selec(
                    null_mut(),
                    0,
                    null_mut(),
                    0,
                    &constval as *const Datum as *mut Datum,
                    1,
                    null_mut(),
                    0,
                    OID_ARRAY_CONTAINED_OP,
                    typentry,
                );
            }
        }

        /*
         * MCE stats count only non-null rows, so adjust for null rows.
         */
        selec *= 1.0 - (*stats).stanullfrac as f64;
    } else {
        /* No stats at all, so do without */
        if useOr {
            selec = mcelem_array_contain_overlap_selec(
                null_mut(),
                0,
                null_mut(),
                0,
                &constval as *const Datum as *mut Datum,
                1,
                OID_ARRAY_CONTAINS_OP,
                typentry,
            );
        } else {
            selec = mcelem_array_contained_selec(
                null_mut(),
                0,
                null_mut(),
                0,
                &constval as *const Datum as *mut Datum,
                1,
                null_mut(),
                0,
                OID_ARRAY_CONTAINED_OP,
                typentry,
            );
        }
        /* we assume no nulls here, so no stanullfrac correction */
    }

    ReleaseVariableStats(&mut vardata);

    /*
     * If the operator is <>, invert the results.
     */
    if !isEquality {
        selec = 1.0 - selec;
    }

    CLAMP_PROBABILITY(&mut selec);

    selec
}

/*
 * arraycontsel -- restriction selectivity for array @>, &&, <@ operators
 */
pub unsafe fn arraycontsel(fcinfo: FunctionCallInfo) -> Datum {
    let root = PG_GETARG_POINTER!(fcinfo, 0) as *mut PlannerInfo;
    let mut operator = PG_GETARG_OID!(fcinfo, 1);
    let args = PG_GETARG_POINTER!(fcinfo, 2) as *mut List;
    let varRelid = PG_GETARG_INT32!(fcinfo, 3);
    let mut vardata: VariableStatData = std::mem::zeroed();
    let mut other: *mut Node = null_mut();
    let mut varonleft: bool = false;
    let selec: Selectivity;
    let element_typeid: Oid;

    /*
     * If expression is not (variable op something) or (something op
     * variable), then punt and return a default estimate.
     */
    if !get_restriction_variable(
        root,
        args,
        varRelid,
        &mut vardata,
        &mut other,
        &mut varonleft,
    ) {
        PG_RETURN_FLOAT8!(DEFAULT_SEL(operator));
    }

    /*
     * Can't do anything useful if the something is not a constant, either.
     */
    if !IsA!(other, T_Const) {
        ReleaseVariableStats(&mut vardata);
        PG_RETURN_FLOAT8!(DEFAULT_SEL(operator));
    }

    /*
     * The "&&", "@>" and "<@" operators are strict, so we can cope with a
     * NULL constant right away.
     */
    if (*(other as *mut Const)).constisnull {
        ReleaseVariableStats(&mut vardata);
        PG_RETURN_FLOAT8!(0.0);
    }

    /*
     * If var is on the right, commute the operator, so that we can assume the
     * var is on the left in what follows.
     */
    if !varonleft {
        if operator == OID_ARRAY_CONTAINS_OP {
            operator = OID_ARRAY_CONTAINED_OP;
        } else if operator == OID_ARRAY_CONTAINED_OP {
            operator = OID_ARRAY_CONTAINS_OP;
        }
    }

    /*
     * OK, there's a Var and a Const we're dealing with here.  We need the
     * Const to be an array with same element type as column, else we can't do
     * anything useful.  (Such cases will likely fail at runtime, but here
     * we'd rather just return a default estimate.)
     */
    element_typeid = get_base_element_type((*(other as *mut Const)).consttype);
    if element_typeid != InvalidOid && element_typeid == get_base_element_type(vardata.vartype) {
        selec = calc_arraycontsel(
            &mut vardata,
            (*(other as *mut Const)).constvalue,
            element_typeid,
            operator,
        );
    } else {
        selec = DEFAULT_SEL(operator);
    }

    ReleaseVariableStats(&mut vardata);

    let mut selec = selec;
    CLAMP_PROBABILITY(&mut selec);

    PG_RETURN_FLOAT8!(selec as f64)
}

/*
 * arraycontjoinsel -- join selectivity for array @>, &&, <@ operators
 */
pub unsafe fn arraycontjoinsel(fcinfo: FunctionCallInfo) -> Datum {
    /* For the moment this is just a stub */
    let operator = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_FLOAT8!(DEFAULT_SEL(operator))
}

/*
 * Calculate selectivity for "arraycolumn @> const", "arraycolumn && const"
 * or "arraycolumn <@ const" based on the statistics
 *
 * This function is mainly responsible for extracting the pg_statistic data
 * to be used; we then pass the problem on to mcelem_array_selec().
 */
unsafe fn calc_arraycontsel(
    vardata: *mut VariableStatData,
    constval: Datum,
    elemtype: Oid,
    operator: Oid,
) -> Selectivity {
    let mut selec: Selectivity;
    let typentry: *mut TypeCacheEntry;
    let cmpfunc: *mut FmgrInfo;
    let array: *mut ArrayType;

    /* Get element type's default comparison function */
    typentry = lookup_type_cache(elemtype, TYPECACHE_CMP_PROC_FINFO);
    if !OidIsValid((*typentry).cmp_proc_finfo.fn_oid) {
        return DEFAULT_SEL(operator);
    }
    cmpfunc = &mut (*typentry).cmp_proc_finfo;

    /*
     * The caller made sure the const is an array with same element type, so
     * get it now
     */
    array = DatumGetArrayTypeP(constval);

    if HeapTupleIsValid((*vardata).statsTuple)
        && statistic_proc_security_check(vardata, (*cmpfunc).fn_oid)
    {
        let stats: Form_pg_statistic;
        let mut sslot: AttStatsSlot = std::mem::zeroed();
        let mut hslot: AttStatsSlot = std::mem::zeroed();

        stats = GETSTRUCT((*vardata).statsTuple) as Form_pg_statistic;

        /* MCELEM will be an array of same type as column */
        if get_attstatsslot(
            &mut sslot,
            (*vardata).statsTuple,
            STATISTIC_KIND_MCELEM,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        ) {
            /*
             * For "array <@ const" case we also need histogram of distinct
             * element counts.
             */
            if operator != OID_ARRAY_CONTAINED_OP
                || !get_attstatsslot(
                    &mut hslot,
                    (*vardata).statsTuple,
                    STATISTIC_KIND_DECHIST,
                    InvalidOid,
                    ATTSTATSSLOT_NUMBERS,
                )
            {
                std::ptr::write_bytes(&mut hslot as *mut AttStatsSlot, 0, 1);
            }

            /* Use the most-common-elements slot for the array Var. */
            selec = mcelem_array_selec(
                array,
                typentry,
                sslot.values,
                sslot.nvalues,
                sslot.numbers,
                sslot.nnumbers,
                hslot.numbers,
                hslot.nnumbers,
                operator,
            );

            free_attstatsslot(&mut hslot);
            free_attstatsslot(&mut sslot);
        } else {
            /* No most-common-elements info, so do without */
            selec = mcelem_array_selec(
                array,
                typentry,
                null_mut(),
                0,
                null_mut(),
                0,
                null_mut(),
                0,
                operator,
            );
        }

        /*
         * MCE stats count only non-null rows, so adjust for null rows.
         */
        selec *= 1.0 - (*stats).stanullfrac as f64;
    } else {
        /* No stats at all, so do without */
        selec = mcelem_array_selec(
            array,
            typentry,
            null_mut(),
            0,
            null_mut(),
            0,
            null_mut(),
            0,
            operator,
        );
        /* we assume no nulls here, so no stanullfrac correction */
    }

    /* If constant was toasted, release the copy we made */
    if PointerGetDatum(array as *const c_void) != constval {
        pfree(array as *mut c_void);
    }

    selec
}

/*
 * Array selectivity estimation based on most common elements statistics
 *
 * This function just deconstructs and sorts the array constant's contents,
 * and then passes the problem on to mcelem_array_contain_overlap_selec or
 * mcelem_array_contained_selec depending on the operator.
 */
unsafe fn mcelem_array_selec(
    array: *mut ArrayType,
    typentry: *mut TypeCacheEntry,
    mcelem: *mut Datum,
    nmcelem: c_int,
    numbers: *mut float4,
    nnumbers: c_int,
    hist: *mut float4,
    nhist: c_int,
    operator: Oid,
) -> Selectivity {
    let selec: Selectivity;
    let mut num_elems: c_int = 0;
    let mut elem_values: *mut Datum = null_mut();
    let mut elem_nulls: *mut bool = null_mut();
    let mut null_present: bool;
    let mut nonnull_nitems: c_int;
    let mut i: c_int;

    /*
     * Prepare constant array data for sorting.  Sorting lets us find unique
     * elements and efficiently merge with the MCELEM array.
     */
    deconstruct_array(
        array,
        (*typentry).type_id,
        (*typentry).typlen as c_int,
        (*typentry).typbyval,
        (*typentry).typalign,
        &mut elem_values,
        &mut elem_nulls,
        &mut num_elems,
    );

    /* Collapse out any null elements */
    nonnull_nitems = 0;
    null_present = false;
    i = 0;
    while i < num_elems {
        if *elem_nulls.offset(i as isize) {
            null_present = true;
        } else {
            *elem_values.offset(nonnull_nitems as isize) = *elem_values.offset(i as isize);
            nonnull_nitems += 1;
        }
        i += 1;
    }

    /*
     * Query "column @> '{anything, null}'" matches nothing.  For the other
     * two operators, presence of a null in the constant can be ignored.
     */
    if null_present && operator == OID_ARRAY_CONTAINS_OP {
        pfree(elem_values as *mut c_void);
        pfree(elem_nulls as *mut c_void);
        return 0.0 as Selectivity;
    }

    /* Sort extracted elements using their default comparison function. */
    qsort_arg(
        elem_values as *mut c_void,
        nonnull_nitems as Size,
        std::mem::size_of::<Datum>() as Size,
        Some(element_compare),
        typentry as *mut c_void,
    );

    /* Separate cases according to operator */
    if operator == OID_ARRAY_CONTAINS_OP || operator == OID_ARRAY_OVERLAP_OP {
        selec = mcelem_array_contain_overlap_selec(
            mcelem,
            nmcelem,
            numbers,
            nnumbers,
            elem_values,
            nonnull_nitems,
            operator,
            typentry,
        );
    } else if operator == OID_ARRAY_CONTAINED_OP {
        selec = mcelem_array_contained_selec(
            mcelem,
            nmcelem,
            numbers,
            nnumbers,
            elem_values,
            nonnull_nitems,
            hist,
            nhist,
            operator,
            typentry,
        );
    } else {
        elog!(
            ERROR,
            "arraycontsel called for unrecognized operator {}",
            operator
        );
        selec = 0.0; /* keep compiler quiet */
    }

    pfree(elem_values as *mut c_void);
    pfree(elem_nulls as *mut c_void);
    selec
}

/*
 * Estimate selectivity of "column @> const" and "column && const" based on
 * most common element statistics.  This estimation assumes element
 * occurrences are independent.
 *
 * mcelem (of length nmcelem) and numbers (of length nnumbers) are from
 * the array column's MCELEM statistics slot, or are NULL/0 if stats are
 * not available.  array_data (of length nitems) is the constant's elements.
 *
 * Both the mcelem and array_data arrays are assumed presorted according
 * to the element type's cmpfunc.  Null elements are not present.
 *
 * TODO: this estimate probably could be improved by using the distinct
 * elements count histogram.  For example, excepting the special case of
 * "column @> '{}'", we can multiply the calculated selectivity by the
 * fraction of nonempty arrays in the column.
 */
unsafe fn mcelem_array_contain_overlap_selec(
    mcelem: *mut Datum,
    nmcelem: c_int,
    mut numbers: *mut float4,
    mut nnumbers: c_int,
    array_data: *mut Datum,
    nitems: c_int,
    operator: Oid,
    typentry: *mut TypeCacheEntry,
) -> Selectivity {
    let mut selec: Selectivity;
    let mut elem_selec: Selectivity;
    let mut mcelem_index: c_int;
    let mut i: c_int;
    let use_bsearch: bool;
    let minfreq: float4;

    /*
     * There should be three more Numbers than Values, because the last three
     * cells should hold minimal and maximal frequency among the non-null
     * elements, and then the frequency of null elements.  Ignore the Numbers
     * if not right.
     */
    if nnumbers != nmcelem + 3 {
        numbers = null_mut();
        nnumbers = 0;
    }

    if !numbers.is_null() {
        /* Grab the lowest observed frequency */
        minfreq = *numbers.offset(nmcelem as isize);
    } else {
        /* Without statistics make some default assumptions */
        minfreq = 2.0 * DEFAULT_CONTAIN_SEL as float4;
    }

    /* Decide whether it is faster to use binary search or not. */
    if nitems * floor_log2(nmcelem as u32) < nmcelem + nitems {
        use_bsearch = true;
    } else {
        use_bsearch = false;
    }

    if operator == OID_ARRAY_CONTAINS_OP {
        /*
         * Initial selectivity for "column @> const" query is 1.0, and it will
         * be decreased with each element of constant array.
         */
        selec = 1.0;
    } else {
        /*
         * Initial selectivity for "column && const" query is 0.0, and it will
         * be increased with each element of constant array.
         */
        selec = 0.0;
    }

    /* Scan mcelem and array in parallel. */
    mcelem_index = 0;
    i = 0;
    while i < nitems {
        let mut match_: bool = false;

        /* Ignore any duplicates in the array data. */
        if i > 0
            && element_compare(
                array_data.offset((i - 1) as isize) as *const c_void,
                array_data.offset(i as isize) as *const c_void,
                typentry as *mut c_void,
            ) == 0
        {
            i += 1;
            continue;
        }

        /* Find the smallest MCELEM >= this array item. */
        if use_bsearch {
            match_ = find_next_mcelem(
                mcelem,
                nmcelem,
                *array_data.offset(i as isize),
                &mut mcelem_index,
                typentry,
            );
        } else {
            while mcelem_index < nmcelem {
                let cmp = element_compare(
                    mcelem.offset(mcelem_index as isize) as *const c_void,
                    array_data.offset(i as isize) as *const c_void,
                    typentry as *mut c_void,
                );

                if cmp < 0 {
                    mcelem_index += 1;
                } else {
                    if cmp == 0 {
                        match_ = true; /* mcelem is found */
                    }
                    break;
                }
            }
        }

        if match_ && !numbers.is_null() {
            /* MCELEM matches the array item; use its frequency. */
            elem_selec = *numbers.offset(mcelem_index as isize) as Selectivity;
            mcelem_index += 1;
        } else {
            /*
             * The element is not in MCELEM.  Punt, but assume that the
             * selectivity cannot be more than minfreq / 2.
             */
            elem_selec = f64::min(DEFAULT_CONTAIN_SEL, (minfreq / 2.0) as f64);
        }

        /*
         * Update overall selectivity using the current element's selectivity
         * and an assumption of element occurrence independence.
         */
        if operator == OID_ARRAY_CONTAINS_OP {
            selec *= elem_selec;
        } else {
            selec = selec + elem_selec - selec * elem_selec;
        }

        /* Clamp intermediate results to stay sane despite roundoff error */
        CLAMP_PROBABILITY(&mut selec);

        i += 1;
    }

    selec
}

/*
 * Estimate selectivity of "column <@ const" based on most common element
 * statistics.
 *
 * mcelem (of length nmcelem) and numbers (of length nnumbers) are from
 * the array column's MCELEM statistics slot, or are NULL/0 if stats are
 * not available.  array_data (of length nitems) is the constant's elements.
 * hist (of length nhist) is from the array column's DECHIST statistics slot,
 * or is NULL/0 if those stats are not available.
 *
 * Both the mcelem and array_data arrays are assumed presorted according
 * to the element type's cmpfunc.  Null elements are not present.
 *
 * Independent element occurrence would imply a particular distribution of
 * distinct element counts among matching rows.  Real data usually falsifies
 * that assumption.  For example, in a set of 11-element integer arrays having
 * elements in the range [0..10], element occurrences are typically not
 * independent.  If they were, a sufficiently-large set would include all
 * distinct element counts 0 through 11.  We correct for this using the
 * histogram of distinct element counts.
 *
 * In the "column @> const" and "column && const" cases, we usually have a
 * "const" with low number of elements (otherwise we have selectivity close
 * to 0 or 1 respectively).  That's why the effect of dependence related
 * to distinct element count distribution is negligible there.  In the
 * "column <@ const" case, number of elements is usually high (otherwise we
 * have selectivity close to 0).  That's why we should do a correction with
 * the array distinct element count distribution here.
 *
 * Using the histogram of distinct element counts produces a different
 * distribution law than independent occurrences of elements.  This
 * distribution law can be described as follows:
 *
 * P(o1, o2, ..., on) = f1^o1 * (1 - f1)^(1 - o1) * f2^o2 *
 *	  (1 - f2)^(1 - o2) * ... * fn^on * (1 - fn)^(1 - on) * hist[m] / ind[m]
 *
 * where:
 * o1, o2, ..., on - occurrences of elements 1, 2, ..., n
 *		(1 - occurrence, 0 - no occurrence) in row
 * f1, f2, ..., fn - frequencies of elements 1, 2, ..., n
 *		(scalar values in [0..1]) according to collected statistics
 * m = o1 + o2 + ... + on = total number of distinct elements in row
 * hist[m] - histogram data for occurrence of m elements.
 * ind[m] - probability of m occurrences from n events assuming their
 *	  probabilities to be equal to frequencies of array elements.
 *
 * ind[m] = sum(f1^o1 * (1 - f1)^(1 - o1) * f2^o2 * (1 - f2)^(1 - o2) *
 * ... * fn^on * (1 - fn)^(1 - on), o1, o2, ..., on) | o1 + o2 + .. on = m
 */
unsafe fn mcelem_array_contained_selec(
    mcelem: *mut Datum,
    nmcelem: c_int,
    numbers: *mut float4,
    nnumbers: c_int,
    array_data: *mut Datum,
    nitems: c_int,
    hist: *mut float4,
    nhist: c_int,
    _operator: Oid,
    typentry: *mut TypeCacheEntry,
) -> Selectivity {
    let mut mcelem_index: c_int;
    let mut i: c_int;
    let mut unique_nitems: c_int = 0;
    let mut selec: f32;
    let minfreq: f32;
    let nullelem_freq: f32;
    let dist: *mut f32;
    let mcelem_dist: *mut f32;
    let hist_part: *mut f32;
    let avg_count: f32;
    let mut mult: f32;
    let mut rest: f32;
    let elem_selec: *mut f32;

    /*
     * There should be three more Numbers than Values in the MCELEM slot,
     * because the last three cells should hold minimal and maximal frequency
     * among the non-null elements, and then the frequency of null elements.
     * Punt if not right, because we can't do much without the element freqs.
     */
    if numbers.is_null() || nnumbers != nmcelem + 3 {
        return DEFAULT_CONTAIN_SEL;
    }

    /* Can't do much without a count histogram, either */
    if hist.is_null() || nhist < 3 {
        return DEFAULT_CONTAIN_SEL;
    }

    /*
     * Grab some of the summary statistics that compute_array_stats() stores:
     * lowest frequency, frequency of null elements, and average distinct
     * element count.
     */
    minfreq = *numbers.offset(nmcelem as isize);
    nullelem_freq = *numbers.offset((nmcelem + 2) as isize);
    avg_count = *hist.offset((nhist - 1) as isize);

    /*
     * "rest" will be the sum of the frequencies of all elements not
     * represented in MCELEM.  The average distinct element count is the sum
     * of the frequencies of *all* elements.  Begin with that; we will proceed
     * to subtract the MCELEM frequencies.
     */
    rest = avg_count;

    /*
     * mult is a multiplier representing estimate of probability that each
     * mcelem that is not present in constant doesn't occur.
     */
    mult = 1.0f32;

    /*
     * elem_selec is array of estimated frequencies for elements in the
     * constant.
     */
    elem_selec = palloc(std::mem::size_of::<f32>() * nitems as usize) as *mut f32;

    /* Scan mcelem and array in parallel. */
    mcelem_index = 0;
    i = 0;
    while i < nitems {
        let mut match_: bool = false;

        /* Ignore any duplicates in the array data. */
        if i > 0
            && element_compare(
                array_data.offset((i - 1) as isize) as *const c_void,
                array_data.offset(i as isize) as *const c_void,
                typentry as *mut c_void,
            ) == 0
        {
            i += 1;
            continue;
        }

        /*
         * Iterate over MCELEM until we find an entry greater than or equal to
         * this element of the constant.  Update "rest" and "mult" for mcelem
         * entries skipped over.
         */
        while mcelem_index < nmcelem {
            let cmp = element_compare(
                mcelem.offset(mcelem_index as isize) as *const c_void,
                array_data.offset(i as isize) as *const c_void,
                typentry as *mut c_void,
            );

            if cmp < 0 {
                mult *= 1.0f32 - *numbers.offset(mcelem_index as isize);
                rest -= *numbers.offset(mcelem_index as isize);
                mcelem_index += 1;
            } else {
                if cmp == 0 {
                    match_ = true; /* mcelem is found */
                }
                break;
            }
        }

        if match_ {
            /* MCELEM matches the array item. */
            *elem_selec.offset(unique_nitems as isize) = *numbers.offset(mcelem_index as isize);
            /* "rest" is decremented for all mcelems, matched or not */
            rest -= *numbers.offset(mcelem_index as isize);
            mcelem_index += 1;
        } else {
            /*
             * The element is not in MCELEM.  Punt, but assume that the
             * selectivity cannot be more than minfreq / 2.
             */
            *elem_selec.offset(unique_nitems as isize) =
                f32::min(DEFAULT_CONTAIN_SEL as f32, minfreq / 2.0);
        }

        unique_nitems += 1;

        i += 1;
    }

    /*
     * If we handled all constant elements without exhausting the MCELEM
     * array, finish walking it to complete calculation of "rest" and "mult".
     */
    while mcelem_index < nmcelem {
        mult *= 1.0f32 - *numbers.offset(mcelem_index as isize);
        rest -= *numbers.offset(mcelem_index as isize);
        mcelem_index += 1;
    }

    /*
     * The presence of many distinct rare elements materially decreases
     * selectivity.  Use the Poisson distribution to estimate the probability
     * of a column value having zero occurrences of such elements.  See above
     * for the definition of "rest".
     */
    mult *= (-rest).exp();

    /*----------
     * Using the distinct element count histogram requires
     *		O(unique_nitems * (nmcelem + unique_nitems))
     * operations.  Beyond a certain computational cost threshold, it's
     * reasonable to sacrifice accuracy for decreased planning time.  We limit
     * the number of operations to EFFORT * nmcelem; since nmcelem is limited
     * by the column's statistics target, the work done is user-controllable.
     *
     * If the number of operations would be too large, we can reduce it
     * without losing all accuracy by reducing unique_nitems and considering
     * only the most-common elements of the constant array.  To make the
     * results exactly match what we would have gotten with only those
     * elements to start with, we'd have to remove any discarded elements'
     * frequencies from "mult", but since this is only an approximation
     * anyway, we don't bother with that.  Therefore it's sufficient to qsort
     * elem_selec[] and take the largest elements.  (They will no longer match
     * up with the elements of array_data[], but we don't care.)
     *----------
     */
    const EFFORT: c_int = 100;

    if (nmcelem + unique_nitems) > 0
        && unique_nitems > EFFORT * nmcelem / (nmcelem + unique_nitems)
    {
        /*
         * Use the quadratic formula to solve for largest allowable N.  We
         * have A = 1, B = nmcelem, C = - EFFORT * nmcelem.
         */
        let b = nmcelem as f64;
        let n: c_int;

        n = ((((b * b + 4.0 * EFFORT as f64 * b).sqrt()) - b) / 2.0) as c_int;

        /* Sort, then take just the first n elements */
        qsort(
            elem_selec as *mut c_void,
            unique_nitems as Size,
            std::mem::size_of::<f32>() as Size,
            Some(float_compare_desc),
        );
        unique_nitems = n;
    }

    /*
     * Calculate probabilities of each distinct element count for both mcelems
     * and constant elements.  At this point, assume independent element
     * occurrence.
     */
    dist = calc_distr(elem_selec, unique_nitems, unique_nitems, 0.0f32);
    mcelem_dist = calc_distr(numbers, nmcelem, unique_nitems, rest);

    /* ignore hist[nhist-1], which is the average not a histogram member */
    hist_part = calc_hist(hist, nhist - 1, unique_nitems);

    selec = 0.0f32;
    i = 0;
    while i <= unique_nitems {
        /*
         * mult * dist[i] / mcelem_dist[i] gives us probability of qual
         * matching from assumption of independent element occurrence with the
         * condition that distinct element count = i.
         */
        if *mcelem_dist.offset(i as isize) > 0.0 {
            selec += *hist_part.offset(i as isize) * mult * *dist.offset(i as isize)
                / *mcelem_dist.offset(i as isize);
        }
        i += 1;
    }

    pfree(dist as *mut c_void);
    pfree(mcelem_dist as *mut c_void);
    pfree(hist_part as *mut c_void);
    pfree(elem_selec as *mut c_void);

    /* Take into account occurrence of NULL element. */
    selec *= 1.0f32 - nullelem_freq;

    let mut selec_d = selec as Selectivity;
    CLAMP_PROBABILITY(&mut selec_d);

    selec_d
}

/*
 * Calculate the first n distinct element count probabilities from a
 * histogram of distinct element counts.
 *
 * Returns a palloc'd array of n+1 entries, with array[k] being the
 * probability of element count k, k in [0..n].
 *
 * We assume that a histogram box with bounds a and b gives 1 / ((b - a + 1) *
 * (nhist - 1)) probability to each value in (a,b) and an additional half of
 * that to a and b themselves.
 */
unsafe fn calc_hist(hist: *const float4, nhist: c_int, n: c_int) -> *mut f32 {
    let hist_part: *mut f32;
    let mut k: c_int;
    let mut i: c_int = 0;
    let mut prev_interval: f32 = 0.0;
    let mut next_interval: f32;
    let frac: f32;

    hist_part = palloc((n + 1) as usize * std::mem::size_of::<f32>()) as *mut f32;

    /*
     * frac is a probability contribution for each interval between histogram
     * values.  We have nhist - 1 intervals, so contribution of each one will
     * be 1 / (nhist - 1).
     */
    frac = 1.0f32 / ((nhist - 1) as f32);

    k = 0;
    while k <= n {
        let mut count: c_int = 0;

        /*
         * Count the histogram boundaries equal to k.  (Although the histogram
         * should theoretically contain only exact integers, entries are
         * floats so there could be roundoff error in large values.  Treat any
         * fractional value as equal to the next larger k.)
         */
        while i < nhist && *hist.offset(i as isize) <= k as f32 {
            count += 1;
            i += 1;
        }

        if count > 0 {
            /* k is an exact bound for at least one histogram box. */
            let mut val: f32;

            /* Find length between current histogram value and the next one */
            if i < nhist {
                next_interval = *hist.offset(i as isize) - *hist.offset((i - 1) as isize);
            } else {
                next_interval = 0.0;
            }

            /*
             * count - 1 histogram boxes contain k exclusively.  They
             * contribute a total of (count - 1) * frac probability.  Also
             * factor in the partial histogram boxes on either side.
             */
            val = (count - 1) as f32;
            if next_interval > 0.0 {
                val += 0.5f32 / next_interval;
            }
            if prev_interval > 0.0 {
                val += 0.5f32 / prev_interval;
            }
            *hist_part.offset(k as isize) = frac * val;

            prev_interval = next_interval;
        } else {
            /* k does not appear as an exact histogram bound. */
            if prev_interval > 0.0 {
                *hist_part.offset(k as isize) = frac / prev_interval;
            } else {
                *hist_part.offset(k as isize) = 0.0f32;
            }
        }

        k += 1;
    }

    hist_part
}

/*
 * Consider n independent events with probabilities p[].  This function
 * calculates probabilities of exact k of events occurrence for k in [0..m].
 * Returns a palloc'd array of size m+1.
 *
 * "rest" is the sum of the probabilities of all low-probability events not
 * included in p.
 *
 * Imagine matrix M of size (n + 1) x (m + 1).  Element M[i,j] denotes the
 * probability that exactly j of first i events occur.  Obviously M[0,0] = 1.
 * For any constant j, each increment of i increases the probability iff the
 * event occurs.  So, by the law of total probability:
 *	M[i,j] = M[i - 1, j] * (1 - p[i]) + M[i - 1, j - 1] * p[i]
 *		for i > 0, j > 0.
 *	M[i,0] = M[i - 1, 0] * (1 - p[i]) for i > 0.
 */
unsafe fn calc_distr(p: *const f32, n: c_int, m: c_int, rest: f32) -> *mut f32 {
    let mut row: *mut f32;
    let mut prev_row: *mut f32;
    let mut tmp: *mut f32;
    let mut i: c_int;
    let mut j: c_int;

    /*
     * Since we return only the last row of the matrix and need only the
     * current and previous row for calculations, allocate two rows.
     */
    row = palloc((m + 1) as usize * std::mem::size_of::<f32>()) as *mut f32;
    prev_row = palloc((m + 1) as usize * std::mem::size_of::<f32>()) as *mut f32;

    /* M[0,0] = 1 */
    *row.offset(0) = 1.0f32;
    i = 1;
    while i <= n {
        let t = *p.offset((i - 1) as isize);

        /* Swap rows */
        tmp = row;
        row = prev_row;
        prev_row = tmp;

        /* Calculate next row */
        j = 0;
        while j <= i && j <= m {
            let mut val: f32 = 0.0f32;

            if j < i {
                val += *prev_row.offset(j as isize) * (1.0f32 - t);
            }
            if j > 0 {
                val += *prev_row.offset((j - 1) as isize) * t;
            }
            *row.offset(j as isize) = val;

            j += 1;
        }

        i += 1;
    }

    /*
     * The presence of many distinct rare (not in "p") elements materially
     * decreases selectivity.  Model their collective occurrence with the
     * Poisson distribution.
     */
    if rest > DEFAULT_CONTAIN_SEL as f32 {
        let mut t: f32;

        /* Swap rows */
        tmp = row;
        row = prev_row;
        prev_row = tmp;

        i = 0;
        while i <= m {
            *row.offset(i as isize) = 0.0f32;
            i += 1;
        }

        /* Value of Poisson distribution for 0 occurrences */
        t = (-rest).exp();

        /*
         * Calculate convolution of previously computed distribution and the
         * Poisson distribution.
         */
        i = 0;
        while i <= m {
            j = 0;
            while j <= m - i {
                *row.offset((j + i) as isize) += *prev_row.offset(j as isize) * t;
                j += 1;
            }

            /* Get Poisson distribution value for (i + 1) occurrences */
            t *= rest / ((i + 1) as f32);
            i += 1;
        }
    }

    pfree(prev_row as *mut c_void);
    row
}

/* Fast function for floor value of 2 based logarithm calculation. */
fn floor_log2(mut n: u32) -> c_int {
    let mut logval: c_int = 0;

    if n == 0 {
        return -1;
    }
    if n >= (1 << 16) {
        n >>= 16;
        logval += 16;
    }
    if n >= (1 << 8) {
        n >>= 8;
        logval += 8;
    }
    if n >= (1 << 4) {
        n >>= 4;
        logval += 4;
    }
    if n >= (1 << 2) {
        n >>= 2;
        logval += 2;
    }
    if n >= (1 << 1) {
        logval += 1;
    }
    logval
}

/*
 * find_next_mcelem binary-searches a most common elements array, starting
 * from *index, for the first member >= value.  It saves the position of the
 * match into *index and returns true if it's an exact match.  (Note: we
 * assume the mcelem elements are distinct so there can't be more than one
 * exact match.)
 */
unsafe fn find_next_mcelem(
    mcelem: *mut Datum,
    nmcelem: c_int,
    value: Datum,
    index: *mut c_int,
    typentry: *mut TypeCacheEntry,
) -> bool {
    let mut l: c_int = *index;
    let mut r: c_int = nmcelem - 1;
    let mut i: c_int;
    let mut res: c_int;

    while l <= r {
        i = (l + r) / 2;
        res = element_compare(
            mcelem.offset(i as isize) as *const c_void,
            &value as *const Datum as *const c_void,
            typentry as *mut c_void,
        );
        if res == 0 {
            *index = i;
            return true;
        } else if res < 0 {
            l = i + 1;
        } else {
            r = i - 1;
        }
    }
    *index = l;
    false
}

/*
 * Comparison function for elements.
 *
 * We use the element type's default btree opclass, and its default collation
 * if the type is collation-sensitive.
 *
 * XXX consider using SortSupport infrastructure
 */
unsafe extern "C" fn element_compare(
    key1: *const c_void,
    key2: *const c_void,
    arg: *mut c_void,
) -> c_int {
    let d1: Datum = *(key1 as *const Datum);
    let d2: Datum = *(key2 as *const Datum);
    let typentry: *mut TypeCacheEntry = arg as *mut TypeCacheEntry;
    let cmpfunc: *mut FmgrInfo = &mut (*typentry).cmp_proc_finfo;
    let c: Datum;

    c = FunctionCall2Coll(cmpfunc, (*typentry).typcollation, d1, d2);
    DatumGetInt32(c)
}

/*
 * Comparison function for sorting floats into descending order.
 */
unsafe extern "C" fn float_compare_desc(key1: *const c_void, key2: *const c_void) -> c_int {
    let d1: f32 = *(key1 as *const f32);
    let d2: f32 = *(key2 as *const f32);

    if d1 > d2 {
        -1
    } else if d1 < d2 {
        1
    } else {
        0
    }
}

/* ---- Local stubs for unported dependencies ---- */
//
// utils/selfuncs.c, utils/lsyscache.c, utils/typcache.c, utils/array.c and the
// catalog/pg_statistic slot-kind helpers are NOT yet ported, so stub the
// symbols this file needs.  Once those modules land, replace these with the
// real `use` paths noted in each comment.

/* catalog/pg_statistic.h: STATISTIC_KIND_MCELEM / STATISTIC_KIND_DECHIST */
const STATISTIC_KIND_MCELEM: int16 = 4;
const STATISTIC_KIND_DECHIST: int16 = 5;

/* utils/lsyscache.h: get_attstatsslot() flags */
const ATTSTATSSLOT_VALUES: c_int = 0x01;
const ATTSTATSSLOT_NUMBERS: c_int = 0x02;

/* utils/typcache.h: lookup_type_cache() flags */
const TYPECACHE_CMP_PROC_FINFO: c_int = 0x00800;

/* nodes/pathnodes.h: PlannerInfo (opaque here) */
// TODO(pg-port): real PlannerInfo lives in nodes/pathnodes.rs
#[repr(C)]
pub struct PlannerInfo {
    _private: [u8; 0],
}

/* nodes/pathnodes.h: RelOptInfo (opaque here) */
// TODO(pg-port): real RelOptInfo lives in nodes/pathnodes.rs
#[repr(C)]
pub struct RelOptInfo {
    _private: [u8; 0],
}

/*
 * utils/selfuncs.h: VariableStatData.  Only the fields used here are stubbed;
 * the real definition lives in utils/selfuncs.h once ported.
 */
// TODO(pg-port): real VariableStatData lives in utils/adt/selfuncs.rs
#[repr(C)]
pub struct VariableStatData {
    pub rel: *mut RelOptInfo,
    pub statsTuple: HeapTuple,
    pub vartype: Oid,
    // ... other selfuncs.h fields elided until utils/selfuncs is ported
}

/*
 * utils/lsyscache.h: AttStatsSlot.  Only the fields used here are stubbed.
 */
// TODO(pg-port): real AttStatsSlot lives in utils/adt/selfuncs.rs (lsyscache)
#[repr(C)]
pub struct AttStatsSlot {
    pub values: *mut Datum,
    pub nvalues: c_int,
    pub numbers: *mut float4,
    pub nnumbers: c_int,
    // ... kind/valuetype/values_arr/numbers_arr elided until lsyscache is ported
}

/*
 * utils/typcache.h: TypeCacheEntry.  Only the fields used here are stubbed.
 */
// TODO(pg-port): real TypeCacheEntry lives in utils/cache/typcache.rs
#[repr(C)]
pub struct TypeCacheEntry {
    pub type_id: Oid,
    pub typlen: int16,
    pub typbyval: bool,
    pub typalign: c_char,
    pub typcollation: Oid,
    pub cmp_proc_finfo: FmgrInfo,
}

/* utils/array.h: ArrayType (opaque here) */
// TODO(pg-port): real ArrayType lives in utils/adt/array.rs
#[repr(C)]
pub struct ArrayType {
    _private: [u8; 0],
}

/* utils/selfuncs.c: examine_variable() */
unsafe fn examine_variable(
    _root: *mut PlannerInfo,
    _node: *mut Node,
    _varRelid: c_int,
    _vardata: *mut VariableStatData,
) {
    unimplemented!() /* TODO(pg-port): utils/adt/selfuncs.rs */
}

/* utils/selfuncs.c: get_restriction_variable() */
unsafe fn get_restriction_variable(
    _root: *mut PlannerInfo,
    _args: *mut List,
    _varRelid: c_int,
    _vardata: *mut VariableStatData,
    _other: *mut *mut Node,
    _varonleft: *mut bool,
) -> bool {
    unimplemented!() /* TODO(pg-port): utils/adt/selfuncs.rs */
}

/* utils/selfuncs.h: ReleaseVariableStats() */
unsafe fn ReleaseVariableStats(_vardata: *mut VariableStatData) {
    unimplemented!() /* TODO(pg-port): utils/adt/selfuncs.rs */
}

/* utils/selfuncs.c: statistic_proc_security_check() */
unsafe fn statistic_proc_security_check(_vardata: *mut VariableStatData, _func_oid: Oid) -> bool {
    unimplemented!() /* TODO(pg-port): utils/adt/selfuncs.rs */
}

/* utils/lsyscache.c: get_attstatsslot() */
unsafe fn get_attstatsslot(
    _sslot: *mut AttStatsSlot,
    _statstuple: HeapTuple,
    _reqkind: int16,
    _reqop: Oid,
    _flags: c_int,
) -> bool {
    unimplemented!() /* TODO(pg-port): utils/cache/lsyscache.rs */
}

/* utils/lsyscache.c: free_attstatsslot() */
unsafe fn free_attstatsslot(_sslot: *mut AttStatsSlot) {
    unimplemented!() /* TODO(pg-port): utils/cache/lsyscache.rs */
}

/* utils/lsyscache.c: get_base_element_type() */
unsafe fn get_base_element_type(_typid: Oid) -> Oid {
    unimplemented!() /* TODO(pg-port): utils/cache/lsyscache.rs */
}

/* utils/cache/typcache.c: lookup_type_cache() */
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!() /* TODO(pg-port): utils/cache/typcache.rs */
}

/* utils/array.c: DatumGetArrayTypeP() */
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType {
    unimplemented!() /* TODO(pg-port): utils/adt/array.rs */
}

/* utils/array.c: deconstruct_array() */
unsafe fn deconstruct_array(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elmlen: c_int,
    _elmbyval: bool,
    _elmalign: c_char,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
    unimplemented!() /* TODO(pg-port): utils/adt/array.rs */
}

/* fmgr.c: FunctionCall2Coll() */
unsafe fn FunctionCall2Coll(
    _flinfo: *mut FmgrInfo,
    _collation: Oid,
    _arg1: Datum,
    _arg2: Datum,
) -> Datum {
    unimplemented!() /* TODO(pg-port): utils/fmgr.rs */
}

/* lib/qsort_arg.c / port: qsort_arg() and qsort() */
type qsort_arg_comparator =
    Option<unsafe extern "C" fn(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int>;
type qsort_comparator = Option<unsafe extern "C" fn(a: *const c_void, b: *const c_void) -> c_int>;

unsafe fn qsort_arg(
    _base: *mut c_void,
    _nel: Size,
    _elsize: Size,
    _cmp: qsort_arg_comparator,
    _arg: *mut c_void,
) {
    unimplemented!() /* TODO(pg-port): port/qsort_arg */
}

unsafe fn qsort(_base: *mut c_void, _nel: Size, _elsize: Size, _cmp: qsort_comparator) {
    unimplemented!() /* TODO(pg-port): libc qsort */
}
