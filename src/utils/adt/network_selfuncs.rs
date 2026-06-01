//! src/backend/utils/adt/network_selfuncs.c
//!
//! Functions for selectivity estimation of inet/cidr operators
//!
//! This module provides estimators for the subnet inclusion and overlap
//! operators.  Estimates are based on null fraction, most common values,
//! and histogram of inet/cidr columns.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

// #[macro_export] macros live at the crate root.
use crate::{
    FunctionCall2, IsA, PG_GETARG_INT32, PG_GETARG_OID, PG_GETARG_POINTER, PG_RETURN_FLOAT8,
};
// Core node/fmgr types used throughout.
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::List;
use crate::nodes::primnodes::Const;
use crate::utils::fmgr::{FmgrInfo, FunctionCallInfo};

use std::ffi::c_int;

// Default selectivity for the inet overlap operator
const DEFAULT_OVERLAP_SEL: f64 = 0.01;

// Default selectivity for the various inclusion operators
const DEFAULT_INCLUSION_SEL: f64 = 0.005;

// Default selectivity for specified operator
#[inline]
fn DEFAULT_SEL(operator: Oid) -> f64 {
    if operator == OID_INET_OVERLAP_OP {
        DEFAULT_OVERLAP_SEL
    } else {
        DEFAULT_INCLUSION_SEL
    }
}

// Maximum number of items to consider in join selectivity calculations
const MAX_CONSIDERED_ELEMS: c_int = 1024;

/*
 * Selectivity estimation for the subnet inclusion/overlap operators
 */
#[no_mangle]
pub unsafe extern "C" fn networksel(fcinfo: FunctionCallInfo) -> Datum {
    let root = PG_GETARG_POINTER!(fcinfo, 0) as *mut PlannerInfo;
    let operator = PG_GETARG_OID!(fcinfo, 1);
    let args = PG_GETARG_POINTER!(fcinfo, 2) as *mut List;
    let varRelid = PG_GETARG_INT32!(fcinfo, 3);
    let opr_codenum: c_int;
    let mut vardata: VariableStatData = std::mem::zeroed();
    let mut other: *mut Node = std::ptr::null_mut();
    let mut varonleft: bool = false;
    let selec: Selectivity;
    let mcv_selec: Selectivity;
    let non_mcv_selec: Selectivity;
    let constvalue: Datum;
    let stats: Form_pg_statistic;
    let mut hslot: AttStatsSlot = std::mem::zeroed();
    let mut sumcommon: f64 = 0.0;
    let nullfrac: f64;
    let mut proc: FmgrInfo = std::mem::zeroed();

    /*
     * Before all else, verify that the operator is one of the ones supported
     * by this function, which in turn proves that the input datatypes are
     * what we expect.  Otherwise, attaching this selectivity function to some
     * unexpected operator could cause trouble.
     */
    opr_codenum = inet_opr_codenum(operator);

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
        ReleaseVariableStats(vardata);
        PG_RETURN_FLOAT8!(DEFAULT_SEL(operator));
    }

    /* All of the operators handled here are strict. */
    if (*(other as *mut Const)).constisnull {
        ReleaseVariableStats(vardata);
        PG_RETURN_FLOAT8!(0.0);
    }
    constvalue = (*(other as *mut Const)).constvalue;

    /* Otherwise, we need stats in order to produce a non-default estimate. */
    if !HeapTupleIsValid(vardata.statsTuple) {
        ReleaseVariableStats(vardata);
        PG_RETURN_FLOAT8!(DEFAULT_SEL(operator));
    }

    stats = GETSTRUCT(vardata.statsTuple) as Form_pg_statistic;
    nullfrac = (*stats).stanullfrac as f64;

    /*
     * If we have most-common-values info, add up the fractions of the MCV
     * entries that satisfy MCV OP CONST.  These fractions contribute directly
     * to the result selectivity.  Also add up the total fraction represented
     * by MCV entries.
     */
    fmgr_info(get_opcode(operator), &mut proc);
    mcv_selec = mcv_selectivity(
        &mut vardata,
        &mut proc,
        InvalidOid,
        constvalue,
        varonleft,
        &mut sumcommon,
    );

    /*
     * If we have a histogram, use it to estimate the proportion of the
     * non-MCV population that satisfies the clause.  If we don't, apply the
     * default selectivity to that population.
     */
    if get_attstatsslot(
        &mut hslot,
        vardata.statsTuple,
        STATISTIC_KIND_HISTOGRAM,
        InvalidOid,
        ATTSTATSSLOT_VALUES,
    ) {
        let h_codenum: c_int;

        /* Commute if needed, so we can consider histogram to be on the left */
        h_codenum = if varonleft { opr_codenum } else { -opr_codenum };
        non_mcv_selec = inet_hist_value_sel(hslot.values, hslot.nvalues, constvalue, h_codenum);

        free_attstatsslot(&mut hslot);
    } else {
        non_mcv_selec = DEFAULT_SEL(operator);
    }

    /* Combine selectivities for MCV and non-MCV populations */
    selec = mcv_selec + (1.0 - nullfrac - sumcommon) * non_mcv_selec;

    /* Result should be in range, but make sure... */
    let mut selec = selec;
    CLAMP_PROBABILITY(&mut selec);

    ReleaseVariableStats(vardata);

    PG_RETURN_FLOAT8!(selec)
}

/*
 * Join selectivity estimation for the subnet inclusion/overlap operators
 *
 * This function has the same structure as eqjoinsel() in selfuncs.c.
 *
 * Throughout networkjoinsel and its subroutines, we have a performance issue
 * in that the amount of work to be done is O(N^2) in the length of the MCV
 * and histogram arrays.  To keep the runtime from getting out of hand when
 * large statistics targets have been set, we arbitrarily limit the number of
 * values considered to 1024 (MAX_CONSIDERED_ELEMS).  For the MCV arrays, this
 * is easy: just consider at most the first N elements.  (Since the MCVs are
 * sorted by decreasing frequency, this correctly gets us the first N MCVs.)
 * For the histogram arrays, we decimate; that is consider only every k'th
 * element, where k is chosen so that no more than MAX_CONSIDERED_ELEMS
 * elements are considered.  This should still give us a good random sample of
 * the non-MCV population.  Decimation is done on-the-fly in the loops that
 * iterate over the histogram arrays.
 */
#[no_mangle]
pub unsafe extern "C" fn networkjoinsel(fcinfo: FunctionCallInfo) -> Datum {
    let root = PG_GETARG_POINTER!(fcinfo, 0) as *mut PlannerInfo;
    let operator = PG_GETARG_OID!(fcinfo, 1);
    let args = PG_GETARG_POINTER!(fcinfo, 2) as *mut List;
    let sjinfo = PG_GETARG_POINTER!(fcinfo, 4) as *mut SpecialJoinInfo;
    let mut selec: f64;
    let opr_codenum: c_int;
    let mut vardata1: VariableStatData = std::mem::zeroed();
    let mut vardata2: VariableStatData = std::mem::zeroed();
    let mut join_is_reversed: bool = false;

    /*
     * Before all else, verify that the operator is one of the ones supported
     * by this function, which in turn proves that the input datatypes are
     * what we expect.  Otherwise, attaching this selectivity function to some
     * unexpected operator could cause trouble.
     */
    opr_codenum = inet_opr_codenum(operator);

    get_join_variables(
        root,
        args,
        sjinfo,
        &mut vardata1,
        &mut vardata2,
        &mut join_is_reversed,
    );

    match (*sjinfo).jointype {
        JOIN_INNER | JOIN_LEFT | JOIN_FULL => {
            /*
             * Selectivity for left/full join is not exactly the same as inner
             * join, but we neglect the difference, as eqjoinsel does.
             */
            selec = networkjoinsel_inner(operator, opr_codenum, &mut vardata1, &mut vardata2);
        }
        JOIN_SEMI | JOIN_ANTI => {
            /* Here, it's important that we pass the outer var on the left. */
            if !join_is_reversed {
                selec = networkjoinsel_semi(operator, opr_codenum, &mut vardata1, &mut vardata2);
            } else {
                selec = networkjoinsel_semi(
                    get_commutator(operator),
                    -opr_codenum,
                    &mut vardata2,
                    &mut vardata1,
                );
            }
        }
        _ => {
            /* other values not expected here */
            elog!(ERROR, "unrecognized join type: {}", (*sjinfo).jointype as c_int);
            #[allow(unreachable_code)]
            {
                selec = 0.0; /* keep compiler quiet */
            }
        }
    }

    ReleaseVariableStats(vardata1);
    ReleaseVariableStats(vardata2);

    CLAMP_PROBABILITY(&mut selec);

    PG_RETURN_FLOAT8!(selec)
}

/*
 * Inner join selectivity estimation for subnet inclusion/overlap operators
 *
 * Calculates MCV vs MCV, MCV vs histogram and histogram vs histogram
 * selectivity for join using the subnet inclusion operators.  Unlike the
 * join selectivity function for the equality operator, eqjoinsel_inner(),
 * one to one matching of the values is not enough.  Network inclusion
 * operators are likely to match many to many, so we must check all pairs.
 * (Note: it might be possible to exploit understanding of the histogram's
 * btree ordering to reduce the work needed, but we don't currently try.)
 * Also, MCV vs histogram selectivity is not neglected as in eqjoinsel_inner().
 */
unsafe fn networkjoinsel_inner(
    operator: Oid,
    opr_codenum: c_int,
    vardata1: *mut VariableStatData,
    vardata2: *mut VariableStatData,
) -> Selectivity {
    let mut stats: Form_pg_statistic;
    let mut nullfrac1: f64 = 0.0;
    let mut nullfrac2: f64 = 0.0;
    let mut selec: Selectivity = 0.0;
    let mut sumcommon1: Selectivity = 0.0;
    let mut sumcommon2: Selectivity = 0.0;
    let mut mcv1_exists: bool = false;
    let mut mcv2_exists: bool = false;
    let mut hist1_exists: bool = false;
    let mut hist2_exists: bool = false;
    let mut mcv1_length: c_int = 0;
    let mut mcv2_length: c_int = 0;
    let mut mcv1_slot: AttStatsSlot = std::mem::zeroed();
    let mut mcv2_slot: AttStatsSlot = std::mem::zeroed();
    let mut hist1_slot: AttStatsSlot = std::mem::zeroed();
    let mut hist2_slot: AttStatsSlot = std::mem::zeroed();

    if HeapTupleIsValid((*vardata1).statsTuple) {
        stats = GETSTRUCT((*vardata1).statsTuple) as Form_pg_statistic;
        nullfrac1 = (*stats).stanullfrac as f64;

        mcv1_exists = get_attstatsslot(
            &mut mcv1_slot,
            (*vardata1).statsTuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        );
        hist1_exists = get_attstatsslot(
            &mut hist1_slot,
            (*vardata1).statsTuple,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        );
        /* Arbitrarily limit number of MCVs considered */
        mcv1_length = Min(mcv1_slot.nvalues, MAX_CONSIDERED_ELEMS);
        if mcv1_exists {
            sumcommon1 = mcv_population(mcv1_slot.numbers, mcv1_length);
        }
    } else {
        std::ptr::write_bytes(&mut mcv1_slot as *mut AttStatsSlot, 0, 1);
        std::ptr::write_bytes(&mut hist1_slot as *mut AttStatsSlot, 0, 1);
    }

    if HeapTupleIsValid((*vardata2).statsTuple) {
        stats = GETSTRUCT((*vardata2).statsTuple) as Form_pg_statistic;
        nullfrac2 = (*stats).stanullfrac as f64;

        mcv2_exists = get_attstatsslot(
            &mut mcv2_slot,
            (*vardata2).statsTuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        );
        hist2_exists = get_attstatsslot(
            &mut hist2_slot,
            (*vardata2).statsTuple,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        );
        /* Arbitrarily limit number of MCVs considered */
        mcv2_length = Min(mcv2_slot.nvalues, MAX_CONSIDERED_ELEMS);
        if mcv2_exists {
            sumcommon2 = mcv_population(mcv2_slot.numbers, mcv2_length);
        }
    } else {
        std::ptr::write_bytes(&mut mcv2_slot as *mut AttStatsSlot, 0, 1);
        std::ptr::write_bytes(&mut hist2_slot as *mut AttStatsSlot, 0, 1);
    }

    /*
     * Calculate selectivity for MCV vs MCV matches.
     */
    if mcv1_exists && mcv2_exists {
        selec += inet_mcv_join_sel(
            mcv1_slot.values,
            mcv1_slot.numbers,
            mcv1_length,
            mcv2_slot.values,
            mcv2_slot.numbers,
            mcv2_length,
            operator,
        );
    }

    /*
     * Add in selectivities for MCV vs histogram matches, scaling according to
     * the fractions of the populations represented by the histograms. Note
     * that the second case needs to commute the operator.
     */
    if mcv1_exists && hist2_exists {
        selec += (1.0 - nullfrac2 - sumcommon2)
            * inet_mcv_hist_sel(
                mcv1_slot.values,
                mcv1_slot.numbers,
                mcv1_length,
                hist2_slot.values,
                hist2_slot.nvalues,
                opr_codenum,
            );
    }
    if mcv2_exists && hist1_exists {
        selec += (1.0 - nullfrac1 - sumcommon1)
            * inet_mcv_hist_sel(
                mcv2_slot.values,
                mcv2_slot.numbers,
                mcv2_length,
                hist1_slot.values,
                hist1_slot.nvalues,
                -opr_codenum,
            );
    }

    /*
     * Add in selectivity for histogram vs histogram matches, again scaling
     * appropriately.
     */
    if hist1_exists && hist2_exists {
        selec += (1.0 - nullfrac1 - sumcommon1)
            * (1.0 - nullfrac2 - sumcommon2)
            * inet_hist_inclusion_join_sel(
                hist1_slot.values,
                hist1_slot.nvalues,
                hist2_slot.values,
                hist2_slot.nvalues,
                opr_codenum,
            );
    }

    /*
     * If useful statistics are not available then use the default estimate.
     * We can apply null fractions if known, though.
     */
    if (!mcv1_exists && !hist1_exists) || (!mcv2_exists && !hist2_exists) {
        selec = (1.0 - nullfrac1) * (1.0 - nullfrac2) * DEFAULT_SEL(operator);
    }

    /* Release stats. */
    free_attstatsslot(&mut mcv1_slot);
    free_attstatsslot(&mut mcv2_slot);
    free_attstatsslot(&mut hist1_slot);
    free_attstatsslot(&mut hist2_slot);

    selec
}

/*
 * Semi join selectivity estimation for subnet inclusion/overlap operators
 *
 * Calculates MCV vs MCV, MCV vs histogram, histogram vs MCV, and histogram vs
 * histogram selectivity for semi/anti join cases.
 */
unsafe fn networkjoinsel_semi(
    operator: Oid,
    opr_codenum: c_int,
    vardata1: *mut VariableStatData,
    vardata2: *mut VariableStatData,
) -> Selectivity {
    let mut stats: Form_pg_statistic;
    let mut selec: Selectivity = 0.0;
    let mut sumcommon1: Selectivity = 0.0;
    let mut sumcommon2: Selectivity = 0.0;
    let mut nullfrac1: f64 = 0.0;
    let mut nullfrac2: f64 = 0.0;
    let mut hist2_weight: f64 = 0.0;
    let mut mcv1_exists: bool = false;
    let mut mcv2_exists: bool = false;
    let mut hist1_exists: bool = false;
    let mut hist2_exists: bool = false;
    let mut proc: FmgrInfo = std::mem::zeroed();
    let mut i: c_int;
    let mut mcv1_length: c_int = 0;
    let mut mcv2_length: c_int = 0;
    let mut mcv1_slot: AttStatsSlot = std::mem::zeroed();
    let mut mcv2_slot: AttStatsSlot = std::mem::zeroed();
    let mut hist1_slot: AttStatsSlot = std::mem::zeroed();
    let mut hist2_slot: AttStatsSlot = std::mem::zeroed();

    if HeapTupleIsValid((*vardata1).statsTuple) {
        stats = GETSTRUCT((*vardata1).statsTuple) as Form_pg_statistic;
        nullfrac1 = (*stats).stanullfrac as f64;

        mcv1_exists = get_attstatsslot(
            &mut mcv1_slot,
            (*vardata1).statsTuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        );
        hist1_exists = get_attstatsslot(
            &mut hist1_slot,
            (*vardata1).statsTuple,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        );
        /* Arbitrarily limit number of MCVs considered */
        mcv1_length = Min(mcv1_slot.nvalues, MAX_CONSIDERED_ELEMS);
        if mcv1_exists {
            sumcommon1 = mcv_population(mcv1_slot.numbers, mcv1_length);
        }
    } else {
        std::ptr::write_bytes(&mut mcv1_slot as *mut AttStatsSlot, 0, 1);
        std::ptr::write_bytes(&mut hist1_slot as *mut AttStatsSlot, 0, 1);
    }

    if HeapTupleIsValid((*vardata2).statsTuple) {
        stats = GETSTRUCT((*vardata2).statsTuple) as Form_pg_statistic;
        nullfrac2 = (*stats).stanullfrac as f64;

        mcv2_exists = get_attstatsslot(
            &mut mcv2_slot,
            (*vardata2).statsTuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        );
        hist2_exists = get_attstatsslot(
            &mut hist2_slot,
            (*vardata2).statsTuple,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        );
        /* Arbitrarily limit number of MCVs considered */
        mcv2_length = Min(mcv2_slot.nvalues, MAX_CONSIDERED_ELEMS);
        if mcv2_exists {
            sumcommon2 = mcv_population(mcv2_slot.numbers, mcv2_length);
        }
    } else {
        std::ptr::write_bytes(&mut mcv2_slot as *mut AttStatsSlot, 0, 1);
        std::ptr::write_bytes(&mut hist2_slot as *mut AttStatsSlot, 0, 1);
    }

    fmgr_info(get_opcode(operator), &mut proc);

    /* Estimate number of input rows represented by RHS histogram. */
    if hist2_exists && !(*vardata2).rel.is_null() {
        hist2_weight = (1.0 - nullfrac2 - sumcommon2) * (*(*vardata2).rel).rows;
    }

    /*
     * Consider each element of the LHS MCV list, matching it to whatever RHS
     * stats we have.  Scale according to the known frequency of the MCV.
     */
    if mcv1_exists && (mcv2_exists || hist2_exists) {
        i = 0;
        while i < mcv1_length {
            selec += *mcv1_slot.numbers.offset(i as isize) as f64
                * inet_semi_join_sel(
                    *mcv1_slot.values.offset(i as isize),
                    mcv2_exists,
                    mcv2_slot.values,
                    mcv2_length,
                    hist2_exists,
                    hist2_slot.values,
                    hist2_slot.nvalues,
                    hist2_weight,
                    &mut proc,
                    opr_codenum,
                );
            i += 1;
        }
    }

    /*
     * Consider each element of the LHS histogram, except for the first and
     * last elements, which we exclude on the grounds that they're outliers
     * and thus not very representative.  Scale on the assumption that each
     * such histogram element represents an equal share of the LHS histogram
     * population (which is a bit bogus, because the members of its bucket may
     * not all act the same with respect to the join clause, but it's hard to
     * do better).
     *
     * If there are too many histogram elements, decimate to limit runtime.
     */
    if hist1_exists && hist1_slot.nvalues > 2 && (mcv2_exists || hist2_exists) {
        let mut hist_selec_sum: f64 = 0.0;
        let k: c_int;
        let mut n: c_int;

        k = (hist1_slot.nvalues - 3) / MAX_CONSIDERED_ELEMS + 1;

        n = 0;
        i = 1;
        while i < hist1_slot.nvalues - 1 {
            hist_selec_sum += inet_semi_join_sel(
                *hist1_slot.values.offset(i as isize),
                mcv2_exists,
                mcv2_slot.values,
                mcv2_length,
                hist2_exists,
                hist2_slot.values,
                hist2_slot.nvalues,
                hist2_weight,
                &mut proc,
                opr_codenum,
            );
            n += 1;
            i += k;
        }

        selec += (1.0 - nullfrac1 - sumcommon1) * hist_selec_sum / n as f64;
    }

    /*
     * If useful statistics are not available then use the default estimate.
     * We can apply null fractions if known, though.
     */
    if (!mcv1_exists && !hist1_exists) || (!mcv2_exists && !hist2_exists) {
        selec = (1.0 - nullfrac1) * (1.0 - nullfrac2) * DEFAULT_SEL(operator);
    }

    /* Release stats. */
    free_attstatsslot(&mut mcv1_slot);
    free_attstatsslot(&mut mcv2_slot);
    free_attstatsslot(&mut hist1_slot);
    free_attstatsslot(&mut hist2_slot);

    selec
}

/*
 * Compute the fraction of a relation's population that is represented
 * by the MCV list.
 */
unsafe fn mcv_population(mcv_numbers: *mut f32, mcv_nvalues: c_int) -> Selectivity {
    let mut sumcommon: Selectivity = 0.0;
    let mut i: c_int;

    i = 0;
    while i < mcv_nvalues {
        sumcommon += *mcv_numbers.offset(i as isize) as f64;
        i += 1;
    }

    sumcommon
}

/*
 * Inet histogram vs single value selectivity estimation
 *
 * Estimate the fraction of the histogram population that satisfies
 * "value OPR CONST".  (The result needs to be scaled to reflect the
 * proportion of the total population represented by the histogram.)
 *
 * The histogram is originally for the inet btree comparison operators.
 * Only the common bits of the network part and the length of the network part
 * (masklen) are interesting for the subnet inclusion operators.  Fortunately,
 * btree comparison treats the network part as the major sort key.  Even so,
 * the length of the network part would not really be significant in the
 * histogram.  This would lead to big mistakes for data sets with uneven
 * masklen distribution.  To reduce this problem, comparisons with the left
 * and the right sides of the buckets are used together.
 *
 * Histogram bucket matches are calculated in two forms.  If the constant
 * matches both bucket endpoints the bucket is considered as fully matched.
 * The second form is to match the bucket partially; we recognize this when
 * the constant matches just one endpoint, or the two endpoints fall on
 * opposite sides of the constant.  (Note that when the constant matches an
 * interior histogram element, it gets credit for partial matches to the
 * buckets on both sides, while a match to a histogram endpoint gets credit
 * for only one partial match.  This is desirable.)
 *
 * The divider in the partial bucket match is imagined as the distance
 * between the decisive bits and the common bits of the addresses.  It will
 * be used as a power of two as it is the natural scale for the IP network
 * inclusion.  This partial bucket match divider calculation is an empirical
 * formula and subject to change with more experiment.
 *
 * For a partial match, we try to calculate dividers for both of the
 * boundaries.  If the address family of a boundary value does not match the
 * constant or comparison of the length of the network parts is not correct
 * for the operator, the divider for that boundary will not be taken into
 * account.  If both of the dividers are valid, the greater one will be used
 * to minimize the mistake in buckets that have disparate masklens.  This
 * calculation is unfair when dividers can be calculated for both of the
 * boundaries but they are far from each other; but it is not a common
 * situation as the boundaries are expected to share most of their significant
 * bits of their masklens.  The mistake would be greater, if we would use the
 * minimum instead of the maximum, and we don't know a sensible way to combine
 * them.
 *
 * For partial match in buckets that have different address families on the
 * left and right sides, only the boundary with the same address family is
 * taken into consideration.  This can cause more mistakes for these buckets
 * if the masklens of their boundaries are also disparate.  But this can only
 * happen in one bucket, since only two address families exist.  It seems a
 * better option than not considering these buckets at all.
 */
unsafe fn inet_hist_value_sel(
    values: *mut Datum,
    nvalues: c_int,
    constvalue: Datum,
    opr_codenum: c_int,
) -> Selectivity {
    let mut r#match: Selectivity = 0.0;
    let query: *mut inet;
    let mut left: *mut inet;
    let mut right: *mut inet;
    let mut i: c_int;
    let k: c_int;
    let mut n: c_int;
    let mut left_order: c_int;
    let mut right_order: c_int;
    let mut left_divider: c_int;
    let mut right_divider: c_int;

    /* guard against zero-divide below */
    if nvalues <= 1 {
        return 0.0;
    }

    /* if there are too many histogram elements, decimate to limit runtime */
    k = (nvalues - 2) / MAX_CONSIDERED_ELEMS + 1;

    query = DatumGetInetPP(constvalue);

    /* "left" is the left boundary value of the current bucket ... */
    left = DatumGetInetPP(*values.offset(0));
    left_order = inet_inclusion_cmp(left, query, opr_codenum);

    n = 0;
    i = k;
    while i < nvalues {
        /* ... and "right" is the right boundary value */
        right = DatumGetInetPP(*values.offset(i as isize));
        right_order = inet_inclusion_cmp(right, query, opr_codenum);

        if left_order == 0 && right_order == 0 {
            /* The whole bucket matches, since both endpoints do. */
            r#match += 1.0;
        } else if (left_order <= 0 && right_order >= 0) || (left_order >= 0 && right_order <= 0) {
            /* Partial bucket match. */
            left_divider = inet_hist_match_divider(left, query, opr_codenum);
            right_divider = inet_hist_match_divider(right, query, opr_codenum);

            if left_divider >= 0 || right_divider >= 0 {
                r#match += 1.0 / 2.0f64.powf(Max(left_divider, right_divider) as f64);
            }
        }

        /* Shift the variables. */
        left = right;
        left_order = right_order;

        /* Count the number of buckets considered. */
        n += 1;

        i += k;
    }

    r#match / n as f64
}

/*
 * Inet MCV vs MCV join selectivity estimation
 *
 * We simply add up the fractions of the populations that satisfy the clause.
 * The result is exact and does not need to be scaled further.
 */
unsafe fn inet_mcv_join_sel(
    mcv1_values: *mut Datum,
    mcv1_numbers: *mut f32,
    mcv1_nvalues: c_int,
    mcv2_values: *mut Datum,
    mcv2_numbers: *mut f32,
    mcv2_nvalues: c_int,
    operator: Oid,
) -> Selectivity {
    let mut selec: Selectivity = 0.0;
    let mut proc: FmgrInfo = std::mem::zeroed();
    let mut i: c_int;
    let mut j: c_int;

    fmgr_info(get_opcode(operator), &mut proc);

    i = 0;
    while i < mcv1_nvalues {
        j = 0;
        while j < mcv2_nvalues {
            if DatumGetBool(FunctionCall2!(
                &mut proc,
                *mcv1_values.offset(i as isize),
                *mcv2_values.offset(j as isize)
            )) {
                selec += *mcv1_numbers.offset(i as isize) as f64
                    * *mcv2_numbers.offset(j as isize) as f64;
            }
            j += 1;
        }
        i += 1;
    }
    selec
}

/*
 * Inet MCV vs histogram join selectivity estimation
 *
 * For each MCV on the lefthand side, estimate the fraction of the righthand's
 * histogram population that satisfies the join clause, and add those up,
 * scaling by the MCV's frequency.  The result still needs to be scaled
 * according to the fraction of the righthand's population represented by
 * the histogram.
 */
unsafe fn inet_mcv_hist_sel(
    mcv_values: *mut Datum,
    mcv_numbers: *mut f32,
    mcv_nvalues: c_int,
    hist_values: *mut Datum,
    hist_nvalues: c_int,
    opr_codenum: c_int,
) -> Selectivity {
    let mut selec: Selectivity = 0.0;
    let mut i: c_int;

    /*
     * We'll call inet_hist_value_selec with the histogram on the left, so we
     * must commute the operator.
     */
    let opr_codenum = -opr_codenum;

    i = 0;
    while i < mcv_nvalues {
        selec += *mcv_numbers.offset(i as isize) as f64
            * inet_hist_value_sel(
                hist_values,
                hist_nvalues,
                *mcv_values.offset(i as isize),
                opr_codenum,
            );
        i += 1;
    }
    selec
}

/*
 * Inet histogram vs histogram join selectivity estimation
 *
 * Here, we take all values listed in the second histogram (except for the
 * first and last elements, which are excluded on the grounds of possibly
 * not being very representative) and treat them as a uniform sample of
 * the non-MCV population for that relation.  For each one, we apply
 * inet_hist_value_selec to see what fraction of the first histogram
 * it matches.
 *
 * We could alternatively do this the other way around using the operator's
 * commutator.  XXX would it be worthwhile to do it both ways and take the
 * average?  That would at least avoid non-commutative estimation results.
 */
unsafe fn inet_hist_inclusion_join_sel(
    hist1_values: *mut Datum,
    hist1_nvalues: c_int,
    hist2_values: *mut Datum,
    hist2_nvalues: c_int,
    opr_codenum: c_int,
) -> Selectivity {
    let mut r#match: f64 = 0.0;
    let mut i: c_int;
    let k: c_int;
    let mut n: c_int;

    if hist2_nvalues <= 2 {
        return 0.0; /* no interior histogram elements */
    }

    /* if there are too many histogram elements, decimate to limit runtime */
    k = (hist2_nvalues - 3) / MAX_CONSIDERED_ELEMS + 1;

    n = 0;
    i = 1;
    while i < hist2_nvalues - 1 {
        r#match += inet_hist_value_sel(
            hist1_values,
            hist1_nvalues,
            *hist2_values.offset(i as isize),
            opr_codenum,
        );
        n += 1;
        i += k;
    }

    r#match / n as f64
}

/*
 * Inet semi join selectivity estimation for one value
 *
 * The function calculates the probability that there is at least one row
 * in the RHS table that satisfies the "lhs_value op column" condition.
 * It is used in semi join estimation to check a sample from the left hand
 * side table.
 *
 * The MCV and histogram from the right hand side table should be provided as
 * arguments with the lhs_value from the left hand side table for the join.
 * hist_weight is the total number of rows represented by the histogram.
 * For example, if the table has 1000 rows, and 10% of the rows are in the MCV
 * list, and another 10% are NULLs, hist_weight would be 800.
 *
 * First, the lhs_value will be matched to the most common values.  If it
 * matches any of them, 1.0 will be returned, because then there is surely
 * a match.
 *
 * Otherwise, the histogram will be used to estimate the number of rows in
 * the second table that match the condition.  If the estimate is greater
 * than 1.0, 1.0 will be returned, because it means there is a greater chance
 * that the lhs_value will match more than one row in the table.  If it is
 * between 0.0 and 1.0, it will be returned as the probability.
 */
unsafe fn inet_semi_join_sel(
    lhs_value: Datum,
    mcv_exists: bool,
    mcv_values: *mut Datum,
    mcv_nvalues: c_int,
    hist_exists: bool,
    hist_values: *mut Datum,
    hist_nvalues: c_int,
    hist_weight: f64,
    proc: *mut FmgrInfo,
    opr_codenum: c_int,
) -> Selectivity {
    if mcv_exists {
        let mut i: c_int;

        i = 0;
        while i < mcv_nvalues {
            if DatumGetBool(FunctionCall2!(
                proc,
                lhs_value,
                *mcv_values.offset(i as isize)
            )) {
                return 1.0;
            }
            i += 1;
        }
    }

    if hist_exists && hist_weight > 0.0 {
        let hist_selec: Selectivity;

        /* Commute operator, since we're passing lhs_value on the right */
        hist_selec = inet_hist_value_sel(hist_values, hist_nvalues, lhs_value, -opr_codenum);

        if hist_selec > 0.0 {
            return Min(1.0, hist_weight * hist_selec);
        }
    }

    0.0
}

/*
 * Assign useful code numbers for the subnet inclusion/overlap operators
 *
 * This will throw an error if the operator is not one of the ones we
 * support in networksel() and networkjoinsel().
 *
 * Only inet_masklen_inclusion_cmp() and inet_hist_match_divider() depend
 * on the exact codes assigned here; but many other places in this file
 * know that they can negate a code to obtain the code for the commutator
 * operator.
 */
unsafe fn inet_opr_codenum(operator: Oid) -> c_int {
    match operator {
        OID_INET_SUP_OP => -2,
        OID_INET_SUPEQ_OP => -1,
        OID_INET_OVERLAP_OP => 0,
        OID_INET_SUBEQ_OP => 1,
        OID_INET_SUB_OP => 2,
        _ => {
            elog!(
                ERROR,
                "unrecognized operator {} for inet selectivity",
                operator
            );
            #[allow(unreachable_code)]
            0 /* unreached, but keep compiler quiet */
        }
    }
}

/*
 * Comparison function for the subnet inclusion/overlap operators
 *
 * If the comparison is okay for the specified inclusion operator, the return
 * value will be 0.  Otherwise the return value will be less than or greater
 * than 0 as appropriate for the operator.
 *
 * Comparison is compatible with the basic comparison function for the inet
 * type.  See network_cmp_internal() in network.c for the original.  Basic
 * comparison operators are implemented with the network_cmp_internal()
 * function.  It is possible to implement the subnet inclusion operators with
 * this function.
 *
 * Comparison is first on the common bits of the network part, then on the
 * length of the network part (masklen) as in the network_cmp_internal()
 * function.  Only the first part is in this function.  The second part is
 * separated to another function for reusability.  The difference between the
 * second part and the original network_cmp_internal() is that the inclusion
 * operator is considered while comparing the lengths of the network parts.
 * See the inet_masklen_inclusion_cmp() function below.
 */
unsafe fn inet_inclusion_cmp(left: *mut inet, right: *mut inet, opr_codenum: c_int) -> c_int {
    if ip_family(left) == ip_family(right) {
        let order: c_int;

        order = bitncmp(
            ip_addr(left),
            ip_addr(right),
            Min(ip_bits(left) as c_int, ip_bits(right) as c_int),
        );
        if order != 0 {
            return order;
        }

        return inet_masklen_inclusion_cmp(left, right, opr_codenum);
    }

    ip_family(left) - ip_family(right)
}

/*
 * Masklen comparison function for the subnet inclusion/overlap operators
 *
 * Compares the lengths of the network parts of the inputs.  If the comparison
 * is okay for the specified inclusion operator, the return value will be 0.
 * Otherwise the return value will be less than or greater than 0 as
 * appropriate for the operator.
 */
unsafe fn inet_masklen_inclusion_cmp(left: *mut inet, right: *mut inet, opr_codenum: c_int) -> c_int {
    let order: c_int;

    order = ip_bits(left) as c_int - ip_bits(right) as c_int;

    /*
     * Return 0 if the operator would accept this combination of masklens.
     * Note that opr_codenum zero (overlaps) will accept all cases.
     */
    if (order > 0 && opr_codenum >= 0)
        || (order == 0 && opr_codenum >= -1 && opr_codenum <= 1)
        || (order < 0 && opr_codenum <= 0)
    {
        return 0;
    }

    /*
     * Otherwise, return a negative value for sup/supeq (notionally, the RHS
     * needs to have a larger masklen than it has, which would make it sort
     * later), or a positive value for sub/subeq (vice versa).
     */
    opr_codenum
}

/*
 * Inet histogram partial match divider calculation
 *
 * First the families and the lengths of the network parts are compared using
 * the subnet inclusion operator.  If those are acceptable for the operator,
 * the divider will be calculated using the masklens and the common bits of
 * the addresses.  -1 will be returned if it cannot be calculated.
 *
 * See commentary for inet_hist_value_sel() for some rationale for this.
 */
unsafe fn inet_hist_match_divider(boundary: *mut inet, query: *mut inet, opr_codenum: c_int) -> c_int {
    if ip_family(boundary) == ip_family(query)
        && inet_masklen_inclusion_cmp(boundary, query, opr_codenum) == 0
    {
        let min_bits: c_int;
        let decisive_bits: c_int;

        min_bits = Min(ip_bits(boundary) as c_int, ip_bits(query) as c_int);

        /*
         * Set decisive_bits to the masklen of the one that should contain the
         * other according to the operator.
         */
        if opr_codenum < 0 {
            decisive_bits = ip_bits(boundary) as c_int;
        } else if opr_codenum > 0 {
            decisive_bits = ip_bits(query) as c_int;
        } else {
            decisive_bits = min_bits;
        }

        /*
         * Now return the number of non-common decisive bits.  (This will be
         * zero if the boundary and query in fact match, else positive.)
         */
        if min_bits > 0 {
            return decisive_bits - bitncommon(ip_addr(boundary), ip_addr(query), min_bits);
        }
        return decisive_bits;
    }

    -1
}

// ---------------------------------------------------------------------------
// Local stubs for unported dependencies
// ---------------------------------------------------------------------------

// Selectivity type (double) from utils/selfuncs.h
#[allow(non_camel_case_types)]
type Selectivity = f64;

// inet type from utils/inet.h
#[repr(C)]
pub struct inet {
    _opaque: [u8; 0],
}

// Operator OIDs from catalog/pg_operator.h
const OID_INET_SUP_OP: Oid = 932;
const OID_INET_SUPEQ_OP: Oid = 933;
const OID_INET_OVERLAP_OP: Oid = 934;
const OID_INET_SUBEQ_OP: Oid = 931;
const OID_INET_SUB_OP: Oid = 930;

// Statistic kinds from catalog/pg_statistic.h
const STATISTIC_KIND_MCV: c_int = 1;
const STATISTIC_KIND_HISTOGRAM: c_int = 2;

// AttStatsSlot flags from utils/lsyscache.h
const ATTSTATSSLOT_VALUES: c_int = 0x01;
const ATTSTATSSLOT_NUMBERS: c_int = 0x02;

// JoinType values from nodes/nodes.h
const JOIN_INNER: c_int = 0;
const JOIN_LEFT: c_int = 1;
const JOIN_FULL: c_int = 2;
const JOIN_SEMI: c_int = 4;
const JOIN_ANTI: c_int = 5;

#[repr(C)]
pub struct PlannerInfo {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct RelOptInfo {
    pub rows: f64,
}

#[repr(C)]
pub struct SpecialJoinInfo {
    pub jointype: c_int,
}

#[repr(C)]
pub struct VariableStatData {
    pub statsTuple: HeapTuple,
    pub rel: *mut RelOptInfo,
}

#[allow(non_camel_case_types)]
pub type HeapTuple = *mut std::ffi::c_void;

#[repr(C)]
pub struct AttStatsSlot {
    pub values: *mut Datum,
    pub nvalues: c_int,
    pub numbers: *mut f32,
    pub nnumbers: c_int,
}

#[allow(non_camel_case_types)]
pub type Form_pg_statistic = *mut FormData_pg_statistic;

#[repr(C)]
pub struct FormData_pg_statistic {
    pub stanullfrac: f32,
}

unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}

unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: access/htup_details.h
}

unsafe fn CLAMP_PROBABILITY(p: *mut f64) {
    if *p < 0.0 {
        *p = 0.0;
    } else if *p > 1.0 {
        *p = 1.0;
    }
}

unsafe fn ReleaseVariableStats(_vardata: VariableStatData) {
    unimplemented!() // TODO: utils/selfuncs.h
}

unsafe fn get_restriction_variable(
    _root: *mut PlannerInfo,
    _args: *mut List,
    _varRelid: c_int,
    _vardata: *mut VariableStatData,
    _other: *mut *mut Node,
    _varonleft: *mut bool,
) -> bool {
    unimplemented!() // TODO: utils/selfuncs.c
}

unsafe fn get_join_variables(
    _root: *mut PlannerInfo,
    _args: *mut List,
    _sjinfo: *mut SpecialJoinInfo,
    _vardata1: *mut VariableStatData,
    _vardata2: *mut VariableStatData,
    _join_is_reversed: *mut bool,
) {
    unimplemented!() // TODO: utils/selfuncs.c
}

unsafe fn mcv_selectivity(
    _vardata: *mut VariableStatData,
    _opproc: *mut FmgrInfo,
    _collation: Oid,
    _constval: Datum,
    _varonleft: bool,
    _sumcommonp: *mut f64,
) -> Selectivity {
    unimplemented!() // TODO: utils/selfuncs.c
}

unsafe fn get_attstatsslot(
    _sslot: *mut AttStatsSlot,
    _statstuple: HeapTuple,
    _reqkind: c_int,
    _reqop: Oid,
    _flags: c_int,
) -> bool {
    unimplemented!() // TODO: utils/lsyscache.c
}

unsafe fn free_attstatsslot(_sslot: *mut AttStatsSlot) {
    unimplemented!() // TODO: utils/lsyscache.c
}

unsafe fn get_opcode(_operator: Oid) -> Oid {
    unimplemented!() // TODO: utils/lsyscache.c
}

unsafe fn get_commutator(_operator: Oid) -> Oid {
    unimplemented!() // TODO: utils/lsyscache.c
}

unsafe fn fmgr_info(_functionId: Oid, _finfo: *mut FmgrInfo) {
    unimplemented!() // TODO: utils/fmgr.c
}

unsafe fn DatumGetInetPP(_datum: Datum) -> *mut inet {
    unimplemented!() // TODO: utils/inet.h
}

unsafe fn ip_family(_inetptr: *mut inet) -> c_int {
    unimplemented!() // TODO: utils/inet.h
}

unsafe fn ip_bits(_inetptr: *mut inet) -> std::ffi::c_uchar {
    unimplemented!() // TODO: utils/inet.h
}

unsafe fn ip_addr(_inetptr: *mut inet) -> *mut std::ffi::c_uchar {
    unimplemented!() // TODO: utils/inet.h
}

unsafe fn bitncmp(_l: *mut std::ffi::c_uchar, _r: *mut std::ffi::c_uchar, _n: c_int) -> c_int {
    unimplemented!() // TODO: utils/adt/network.c
}

unsafe fn bitncommon(_l: *mut std::ffi::c_uchar, _r: *mut std::ffi::c_uchar, _n: c_int) -> c_int {
    unimplemented!() // TODO: utils/adt/network.c
}
