//! src/backend/statistics/mcv.c
//!
//! POSTGRES multivariate MCV lists
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/statistics/mcv.c

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::access::attnum::AttrNumber;
use crate::access::common::heaptuple::heap_form_tuple;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::HeapTuple;
use crate::access::tupmacs::{fetch_att, store_att_byval};
use crate::c::{bytea, text, uint16, uint32, MAXALIGN, VARHDRSZ};
use crate::catalog::pg_statistic_ext::STATS_EXT_MCV;
use crate::catalog::pg_type_d::{BOOLOID, TEXTOID};
use crate::nodes::bitmapset::{bms_member_index, bms_num_members, Bitmapset};
use crate::nodes::equalfuncs::equal;
use crate::nodes::nodeFuncs::exprCollation;
use crate::nodes::nodes::{JoinType, Node, Selectivity};
use crate::nodes::parsenodes::RangeTblEntry;
use crate::nodes::pathnodes::{
    PlannerInfo, RelOptInfo, SpecialJoinInfo, StatisticExtInfo,
};
use crate::nodes::pg_list::{lfirst, List, NIL};
use crate::nodes::primnodes::{
    BoolExpr, Const, NullTest, NullTestType, OpExpr, ScalarArrayOpExpr, Var,
};
use crate::optimizer::util::clauses::is_opclause;
use crate::port::bsearch_arg::bsearch_arg;
use crate::postgres::{
    BoolGetDatum, Datum, DatumGetBool, DatumGetCString, DatumGetPointer, Float8GetDatum,
    Int32GetDatum, ObjectIdGetDatum, PointerGetDatum,
};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::statistics::extended_stats::{
    build_sorted_items, compare_datums_simple, compare_scalars_simple, examine_opclause_args,
    multi_sort_add_dimension, multi_sort_compare, multi_sort_init, VacAttrStats,
};
use crate::statistics::extended_stats_internal::{
    DimensionInfo, MultiSortSupport, MultiSortSupportData, SortItem, StatsBuildData,
};
use crate::statistics::statistics::{
    MCVItem, MCVList, STATS_MAX_DIMENSIONS, STATS_MCVLIST_MAX_ITEMS, STATS_MCV_MAGIC,
    STATS_MCV_TYPE_BASIC,
};
use crate::utils::array::{ArrayType, ARR_ELEMTYPE};
use crate::utils::adt::arrayfuncs::{
    accumArrayResult, deconstruct_array, makeArrayResult, ArrayBuildState,
};
use crate::utils::adt::varlena::{byteaout, cstring_to_text};
use crate::utils::cache::lsyscache::{get_opcode, get_typlenbyvalalign, getTypeOutputInfo};
use crate::utils::cache::typcache::{lookup_type_cache, TYPECACHE_LT_OPR};
use crate::utils::fmgr::{fmgr_info, FmgrInfo, FunctionCallInfo, FunctionCall2Coll};
use crate::utils::sort::qsort_interruptible::qsort_interruptible;
use crate::utils::sort::sortsupport::{
    ApplySortComparator, PrepareSortSupportFromOrderingOp, SortSupport, SortSupportData,
};
use crate::varatt::{
    SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY, VARSIZE_ANY_EXHDR,
};
use crate::{
    elog, ereport, errmsg, foreach, list_make1, Assert, CLAMP_PROBABILITY, FunctionCall1,
    IsA, PG_DETOAST_DATUM, PG_GETARG_BYTEA_P, PG_RETURN_VOID,
};

// ---------------------------------------------------------------------------
// Stubs for dependencies in not-yet-ported .c files.
// ---------------------------------------------------------------------------

// utils/adt/varlena.h: byteasend() binary output routine (not yet ported).
unsafe fn byteasend(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("TODO(pg-port): byteasend (utils/adt/varlena.c not ported)")
}

// utils/cache/syscache.h: STATEXTDATASTXOID syscache id.
// TODO(pg-port): replace with the real constant once syscache.h is ported.
const STATEXTDATASTXOID: c_int = 62;

// catalog/pg_statistic_ext_data.h: column number of stxdmcv.
// TODO(pg-port): replace once pg_statistic_ext_data.h is ported.
const Anum_pg_statistic_ext_data_stxdmcv: c_int = 0;

// funcapi.h: TypeFuncClass (only TYPEFUNC_COMPOSITE used here).
const TYPEFUNC_COMPOSITE: c_int = 1;

// utils/fmgr.h: ERRCODE used for error reporting; folded into a comment below.

// funcapi.h: cross-call persistence context for set-returning functions.
// TODO(pg-port): replace with the real FuncCallContext once funcapi.c is ported.
#[repr(C)]
pub struct AttInMetadata {
    pub tupdesc: TupleDesc,
}

#[repr(C)]
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub max_calls: u64,
    pub user_fctx: *mut c_void,
    pub attinmeta: *mut AttInMetadata,
    pub multi_call_memory_ctx: MemoryContext,
    pub tuple_desc: TupleDesc,
}

// SRF_IS_FIRSTCALL()
unsafe fn SRF_IS_FIRSTCALL(_fcinfo: FunctionCallInfo) -> bool {
    unimplemented!("TODO(pg-port): SRF_IS_FIRSTCALL (utils/fmgr/funcapi.c not ported)")
}

// SRF_FIRSTCALL_INIT()
unsafe fn SRF_FIRSTCALL_INIT(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!("TODO(pg-port): SRF_FIRSTCALL_INIT (utils/fmgr/funcapi.c not ported)")
}

// SRF_PERCALL_SETUP()
unsafe fn SRF_PERCALL_SETUP(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!("TODO(pg-port): SRF_PERCALL_SETUP (utils/fmgr/funcapi.c not ported)")
}

// SRF_RETURN_NEXT(funcctx, result)
unsafe fn SRF_RETURN_NEXT(_funcctx: *mut FuncCallContext, _result: Datum) -> Datum {
    unimplemented!("TODO(pg-port): SRF_RETURN_NEXT (utils/fmgr/funcapi.c not ported)")
}

// SRF_RETURN_DONE(funcctx)
unsafe fn SRF_RETURN_DONE(_funcctx: *mut FuncCallContext) -> Datum {
    unimplemented!("TODO(pg-port): SRF_RETURN_DONE (utils/fmgr/funcapi.c not ported)")
}

// get_call_result_type(fcinfo, resultTypeId, resultTupleDesc)
unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> c_int {
    unimplemented!("TODO(pg-port): get_call_result_type (utils/fmgr/funcapi.c not ported)")
}

// funcapi.h: BlessTupleDesc()
unsafe fn BlessTupleDesc(_tupdesc: TupleDesc) -> TupleDesc {
    unimplemented!("TODO(pg-port): BlessTupleDesc (utils/fmgr/funcapi.c not ported)")
}

// funcapi.h: TupleDescGetAttInMetadata()
unsafe fn TupleDescGetAttInMetadata(_tupdesc: TupleDesc) -> *mut AttInMetadata {
    unimplemented!("TODO(pg-port): TupleDescGetAttInMetadata (utils/fmgr/funcapi.c not ported)")
}

// funcapi.h: HeapTupleGetDatum(tuple)
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!("TODO(pg-port): HeapTupleGetDatum (funcapi.h not ported)")
}

// utils/syscache.h: SearchSysCache2()
unsafe fn SearchSysCache2(_cache_id: c_int, _key1: Datum, _key2: Datum) -> HeapTuple {
    unimplemented!("TODO(pg-port): SearchSysCache2 (utils/cache/syscache.c not ported)")
}

// utils/syscache.h: SysCacheGetAttr()
unsafe fn SysCacheGetAttr(
    _cache_id: c_int,
    _tup: HeapTuple,
    _attribute_number: c_int,
    _is_null: *mut bool,
) -> Datum {
    unimplemented!("TODO(pg-port): SysCacheGetAttr (utils/cache/syscache.c not ported)")
}

// utils/syscache.h: ReleaseSysCache()
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!("TODO(pg-port): ReleaseSysCache (utils/cache/syscache.c not ported)")
}

// access/htup.h: HeapTupleIsValid(tuple)
#[inline]
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}

// libc strlen, used for cstring length computations.
unsafe fn strlen(s: *const c_char) -> Size {
    let mut n: Size = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

// postgres.h: BoolGetDatum already imported; DatumGetByteaP via fmgr macro.
unsafe fn DatumGetByteaP(d: Datum) -> *mut bytea {
    crate::DatumGetByteaP!(d)
}

// utils/array.h: DatumGetArrayTypeP()
// TODO(pg-port): replace once utils/adt/arrayfuncs.c exposes the detoasting form.
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType {
    PG_DETOAST_DATUM!(d) as *mut ArrayType
}

// is_orclause/is_andclause/is_notclause (nodes/nodeFuncs.h-style helpers).
// TODO(pg-port): replace once these inline helpers are ported.
unsafe fn is_orclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && (*(clause as *const Node)).r#type == crate::nodes::nodes::NodeTag::T_BoolExpr
        && (*(clause as *const BoolExpr)).boolop == crate::nodes::primnodes::BoolExprType::OR_EXPR
}
unsafe fn is_andclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && (*(clause as *const Node)).r#type == crate::nodes::nodes::NodeTag::T_BoolExpr
        && (*(clause as *const BoolExpr)).boolop == crate::nodes::primnodes::BoolExprType::AND_EXPR
}
unsafe fn is_notclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && (*(clause as *const Node)).r#type == crate::nodes::nodes::NodeTag::T_BoolExpr
        && (*(clause as *const BoolExpr)).boolop == crate::nodes::primnodes::BoolExprType::NOT_EXPR
}

// nodes/pg_list.h: list_length()
unsafe fn list_length(l: *const List) -> c_int {
    if l.is_null() {
        0
    } else {
        (*l).length
    }
}

// extern "C" trampolines so the imported comparators (declared as plain
// `unsafe fn` in extended_stats.rs) can be passed to bsearch_arg, which expects
// an `unsafe extern "C" fn` pointer.
unsafe extern "C" fn multi_sort_compare_c(
    a: *const c_void,
    b: *const c_void,
    arg: *mut c_void,
) -> c_int {
    multi_sort_compare(a, b, arg)
}
unsafe extern "C" fn compare_scalars_simple_c(
    a: *const c_void,
    b: *const c_void,
    arg: *mut c_void,
) -> c_int {
    compare_scalars_simple(a, b, arg)
}

/*
 * Computes size of a serialized MCV item, depending on the number of
 * dimensions (columns) the statistic is defined on. The datum values are
 * stored in a separate array (deduplicated, to minimize the size), and
 * so the serialized items only store uint16 indexes into that array.
 *
 * Each serialized item stores (in this order):
 *
 * - indexes to values	  (ndim * sizeof(uint16))
 * - null flags			  (ndim * sizeof(bool))
 * - frequency			  (sizeof(double))
 * - base_frequency		  (sizeof(double))
 *
 * There is no alignment padding within an MCV item.
 * So in total each MCV item requires this many bytes:
 *
 *	 ndim * (sizeof(uint16) + sizeof(bool)) + 2 * sizeof(double)
 */
#[inline]
const fn ITEM_SIZE(ndims: usize) -> usize {
    ndims * (core::mem::size_of::<uint16>() + core::mem::size_of::<bool>())
        + 2 * core::mem::size_of::<f64>()
}

/*
 * Used to compute size of serialized MCV list representation.
 */
const MinSizeOfMCVList: usize = VARHDRSZ as usize
    + core::mem::size_of::<uint32>() * 3
    + core::mem::size_of::<AttrNumber>();

/*
 * Size of the serialized MCV list, excluding the space needed for
 * deduplicated per-dimension values. The macro is meant to be used
 * when it's not yet safe to access the serialized info about amount
 * of data for each column.
 */
#[inline]
const fn SizeOfMCVList(ndims: usize, nitems: usize) -> usize {
    (MinSizeOfMCVList + core::mem::size_of::<Oid>() * ndims)
        + (ndims * core::mem::size_of::<DimensionInfo>())
        + (nitems * ITEM_SIZE(ndims))
}

/*
 * Compute new value for bitmap item, considering whether it's used for
 * clauses connected by AND/OR.
 */
#[inline]
fn RESULT_MERGE(value: bool, is_or: bool, r#match: bool) -> bool {
    if is_or {
        value || r#match
    } else {
        value && r#match
    }
}

/*
 * When processing a list of clauses, the bitmap item may get set to a value
 * such that additional clauses can't change it. For example, when processing
 * a list of clauses connected to AND, as soon as the item gets set to 'false'
 * then it'll remain like that. Similarly clauses connected by OR and 'true'.
 *
 * Returns true when the value in the bitmap can't change no matter how the
 * remaining clauses are evaluated.
 */
#[inline]
fn RESULT_IS_FINAL(value: bool, is_or: bool) -> bool {
    if is_or {
        value
    } else {
        !value
    }
}

/*
 * get_mincount_for_mcv_list
 * 		Determine the minimum number of times a value needs to appear in
 * 		the sample for it to be included in the MCV list.
 *
 * We want to keep only values that appear sufficiently often in the
 * sample that it is reasonable to extrapolate their sample frequencies to
 * the entire table.  We do this by placing an upper bound on the relative
 * standard error of the sample frequency, so that any estimates the
 * planner generates from the MCV statistics can be expected to be
 * reasonably accurate.
 *
 * Since we are sampling without replacement, the sample frequency of a
 * particular value is described by a hypergeometric distribution.  A
 * common rule of thumb when estimating errors in this situation is to
 * require at least 10 instances of the value in the sample, in which case
 * the distribution can be approximated by a normal distribution, and
 * standard error analysis techniques can be applied.  Given a sample size
 * of n, a population size of N, and a sample frequency of p=cnt/n, the
 * standard error of the proportion p is given by
 *		SE = sqrt(p*(1-p)/n) * sqrt((N-n)/(N-1))
 * where the second term is the finite population correction.  To get
 * reasonably accurate planner estimates, we impose an upper bound on the
 * relative standard error of 20% -- i.e., SE/p < 0.2.  This 20% relative
 * error bound is fairly arbitrary, but has been found empirically to work
 * well.  Rearranging this formula gives a lower bound on the number of
 * instances of the value seen:
 *		cnt > n*(N-n) / (N-n+0.04*n*(N-1))
 * This bound is at most 25, and approaches 0 as n approaches 0 or N. The
 * case where n approaches 0 cannot happen in practice, since the sample
 * size is at least 300.  The case where n approaches N corresponds to
 * sampling the whole table, in which case it is reasonable to keep
 * the whole MCV list (have no lower bound), so it makes sense to apply
 * this formula for all inputs, even though the above derivation is
 * technically only valid when the right hand side is at least around 10.
 *
 * An alternative way to look at this formula is as follows -- assume that
 * the number of instances of the value seen scales up to the entire
 * table, so that the population count is K=N*cnt/n. Then the distribution
 * in the sample is a hypergeometric distribution parameterised by N, n
 * and K, and the bound above is mathematically equivalent to demanding
 * that the standard deviation of that distribution is less than 20% of
 * its mean.  Thus the relative errors in any planner estimates produced
 * from the MCV statistics are likely to be not too large.
 */
unsafe fn get_mincount_for_mcv_list(samplerows: c_int, totalrows: f64) -> f64 {
    let n: f64 = samplerows as f64;
    let N: f64 = totalrows;
    let numer: f64;
    let denom: f64;

    numer = n * (N - n);
    denom = N - n + 0.04 * n * (N - 1.0);

    /* Guard against division by zero (possible if n = N = 1) */
    if denom == 0.0 {
        return 0.0;
    }

    numer / denom
}

/*
 * Builds MCV list from the set of sampled rows.
 *
 * The algorithm is quite simple:
 *
 *	   (1) sort the data (default collation, '<' for the data type)
 *
 *	   (2) count distinct groups, decide how many to keep
 *
 *	   (3) build the MCV list using the threshold determined in (2)
 *
 *	   (4) remove rows represented by the MCV from the sample
 *
 */
pub unsafe fn statext_mcv_build(
    data: *mut StatsBuildData,
    totalrows: f64,
    stattarget: c_int,
) -> *mut MCVList {
    let mut i: c_int;
    let numattrs: c_int;
    let numrows: c_int;
    let ngroups: c_int;
    let mut nitems: c_int;
    let mincount: f64;
    let items: *mut SortItem;
    let groups: *mut SortItem;
    let mut mcvlist: *mut MCVList = core::ptr::null_mut();
    let mss: MultiSortSupport;

    /* comparator for all the columns */
    mss = build_mss(data);

    /* sort the rows */
    let mut nitems_out: c_int = 0;
    items = build_sorted_items(
        data,
        &mut nitems_out,
        mss,
        (*data).nattnums,
        (*data).attnums,
    );
    nitems = nitems_out;

    if items.is_null() {
        return core::ptr::null_mut();
    }

    /* for convenience */
    numattrs = (*data).nattnums;
    numrows = (*data).numrows;

    /* transform the sorted rows into groups (sorted by frequency) */
    let mut ngroups_out: c_int = 0;
    groups = build_distinct_groups(nitems, items, mss, &mut ngroups_out);
    ngroups = ngroups_out;

    /*
     * The maximum number of MCV items to store, based on the statistics
     * target we computed for the statistics object (from the target set for
     * the object itself, attributes and the system default). In any case, we
     * can't keep more groups than we have available.
     */
    nitems = stattarget;
    if nitems > ngroups {
        nitems = ngroups;
    }

    /*
     * Decide how many items to keep in the MCV list. We can't use the same
     * algorithm as per-column MCV lists, because that only considers the
     * actual group frequency - but we're primarily interested in how the
     * actual frequency differs from the base frequency (product of simple
     * per-column frequencies, as if the columns were independent).
     *
     * Using the same algorithm might exclude items that are close to the
     * "average" frequency of the sample. But that does not say whether the
     * observed frequency is close to the base frequency or not. We also need
     * to consider unexpectedly uncommon items (again, compared to the base
     * frequency), and the single-column algorithm does not have to.
     *
     * We simply decide how many items to keep by computing the minimum count
     * using get_mincount_for_mcv_list() and then keep all items that seem to
     * be more common than that.
     */
    mincount = get_mincount_for_mcv_list(numrows, totalrows);

    /*
     * Walk the groups until we find the first group with a count below the
     * mincount threshold (the index of that group is the number of groups we
     * want to keep).
     */
    i = 0;
    while i < nitems {
        if ((*groups.offset(i as isize)).count as f64) < mincount {
            nitems = i;
            break;
        }
        i += 1;
    }

    /*
     * At this point, we know the number of items for the MCV list. There
     * might be none (for uniform distribution with many groups), and in that
     * case, there will be no MCV list. Otherwise, construct the MCV list.
     */
    if nitems > 0 {
        let mut j: c_int;
        let mut key: SortItem = core::mem::zeroed();
        let tmp: MultiSortSupport;

        /* frequencies for values in each attribute */
        let freqs: *mut *mut SortItem;
        let nfreqs: *mut c_int;

        /* used to search values */
        tmp = palloc(
            core::mem::offset_of!(MultiSortSupportData, ssup)
                + core::mem::size_of::<SortSupportData>(),
        ) as MultiSortSupport;

        /* compute frequencies for values in each column */
        nfreqs = palloc0(core::mem::size_of::<c_int>() * numattrs as usize) as *mut c_int;
        freqs = build_column_frequencies(groups, ngroups, mss, nfreqs);

        /*
         * Allocate the MCV list structure, set the global parameters.
         */
        mcvlist = palloc0(
            core::mem::offset_of!(MCVList, items)
                + core::mem::size_of::<MCVItem>() * nitems as usize,
        ) as *mut MCVList;

        (*mcvlist).magic = STATS_MCV_MAGIC;
        (*mcvlist).r#type = STATS_MCV_TYPE_BASIC;
        (*mcvlist).ndimensions = numattrs as AttrNumber;
        (*mcvlist).nitems = nitems as uint32;

        /* store info about data type OIDs */
        i = 0;
        while i < numattrs {
            let stat = *((*data).stats as *mut *mut VacAttrStats).offset(i as isize);
            (*mcvlist).types[i as usize] = (*stat).attrtypid;
            i += 1;
        }

        /* Copy the first chunk of groups into the result. */
        i = 0;
        while i < nitems {
            /* just point to the proper place in the list */
            let item: *mut MCVItem = (*mcvlist).items.as_mut_ptr().offset(i as isize);

            (*item).values =
                palloc(core::mem::size_of::<Datum>() * numattrs as usize) as *mut Datum;
            (*item).isnull =
                palloc(core::mem::size_of::<bool>() * numattrs as usize) as *mut bool;

            /* copy values for the group */
            core::ptr::copy_nonoverlapping(
                (*groups.offset(i as isize)).values,
                (*item).values,
                numattrs as usize,
            );
            core::ptr::copy_nonoverlapping(
                (*groups.offset(i as isize)).isnull,
                (*item).isnull,
                numattrs as usize,
            );

            /* groups should be sorted by frequency in descending order */
            Assert!(
                (i == 0)
                    || ((*groups.offset((i - 1) as isize)).count
                        >= (*groups.offset(i as isize)).count)
            );

            /* group frequency */
            (*item).frequency = (*groups.offset(i as isize)).count as f64 / numrows as f64;

            /* base frequency, if the attributes were independent */
            (*item).base_frequency = 1.0;
            j = 0;
            while j < numattrs {
                let freq: *mut SortItem;

                /* single dimension */
                (*tmp).ndims = 1;
                *(*tmp).ssup.as_mut_ptr().offset(0) = *(*mss).ssup.as_ptr().offset(j as isize);

                /* fill search key */
                key.values = &mut (*(*groups.offset(i as isize)).values.offset(j as isize));
                key.isnull = &mut (*(*groups.offset(i as isize)).isnull.offset(j as isize));

                freq = bsearch_arg(
                    &key as *const SortItem as *const c_void,
                    *freqs.offset(j as isize) as *const c_void,
                    *nfreqs.offset(j as isize) as Size,
                    core::mem::size_of::<SortItem>() as Size,
                    multi_sort_compare_c,
                    tmp as *mut c_void,
                ) as *mut SortItem;

                (*item).base_frequency *= (*freq).count as f64 / numrows as f64;
                j += 1;
            }
            i += 1;
        }

        pfree(nfreqs as *mut c_void);
        pfree(freqs as *mut c_void);
    }

    pfree(items as *mut c_void);
    pfree(groups as *mut c_void);

    mcvlist
}

/*
 * build_mss
 *		Build a MultiSortSupport for the given StatsBuildData.
 */
unsafe fn build_mss(data: *mut StatsBuildData) -> MultiSortSupport {
    let mut i: c_int;
    let numattrs: c_int = (*data).nattnums;

    /* Sort by multiple columns (using array of SortSupport) */
    let mss: MultiSortSupport = multi_sort_init(numattrs);

    /* prepare the sort functions for all the attributes */
    i = 0;
    while i < numattrs {
        let colstat = *((*data).stats as *mut *mut VacAttrStats).offset(i as isize);
        let r#type;

        r#type = lookup_type_cache((*colstat).attrtypid, TYPECACHE_LT_OPR);
        if (*r#type).lt_opr == InvalidOid
        /* shouldn't happen */
        {
            elog!(
                ERROR,
                "cache lookup failed for ordering operator for type {}",
                (*colstat).attrtypid
            );
        }

        multi_sort_add_dimension(mss, i, (*r#type).lt_opr, (*colstat).attrcollid);
        i += 1;
    }

    mss
}

/*
 * count_distinct_groups
 *		Count distinct combinations of SortItems in the array.
 *
 * The array is assumed to be sorted according to the MultiSortSupport.
 */
unsafe fn count_distinct_groups(
    numrows: c_int,
    items: *mut SortItem,
    mss: MultiSortSupport,
) -> c_int {
    let mut i: c_int;
    let mut ndistinct: c_int;

    ndistinct = 1;
    i = 1;
    while i < numrows {
        /* make sure the array really is sorted */
        Assert!(
            multi_sort_compare(
                items.offset(i as isize) as *const c_void,
                items.offset((i - 1) as isize) as *const c_void,
                mss as *mut c_void,
            ) >= 0
        );

        if multi_sort_compare(
            items.offset(i as isize) as *const c_void,
            items.offset((i - 1) as isize) as *const c_void,
            mss as *mut c_void,
        ) != 0
        {
            ndistinct += 1;
        }
        i += 1;
    }

    ndistinct
}

/*
 * compare_sort_item_count
 *		Comparator for sorting items by count (frequencies) in descending
 *		order.
 */
unsafe extern "C" fn compare_sort_item_count(
    a: *const c_void,
    b: *const c_void,
    _arg: *mut c_void,
) -> c_int {
    let ia: *mut SortItem = a as *mut SortItem;
    let ib: *mut SortItem = b as *mut SortItem;

    if (*ia).count == (*ib).count {
        0
    } else if (*ia).count > (*ib).count {
        -1
    } else {
        1
    }
}

/*
 * build_distinct_groups
 *		Build an array of SortItems for distinct groups and counts matching
 *		items.
 *
 * The 'items' array is assumed to be sorted.
 */
unsafe fn build_distinct_groups(
    numrows: c_int,
    items: *mut SortItem,
    mss: MultiSortSupport,
    ndistinct: *mut c_int,
) -> *mut SortItem {
    let mut i: c_int;
    let mut j: c_int;
    let ngroups: c_int = count_distinct_groups(numrows, items, mss);

    let groups: *mut SortItem =
        palloc(ngroups as usize * core::mem::size_of::<SortItem>()) as *mut SortItem;

    j = 0;
    *groups.offset(0) = *items.offset(0);
    (*groups.offset(0)).count = 1;

    i = 1;
    while i < numrows {
        /* Assume sorted in ascending order. */
        Assert!(
            multi_sort_compare(
                items.offset(i as isize) as *const c_void,
                items.offset((i - 1) as isize) as *const c_void,
                mss as *mut c_void,
            ) >= 0
        );

        /* New distinct group detected. */
        if multi_sort_compare(
            items.offset(i as isize) as *const c_void,
            items.offset((i - 1) as isize) as *const c_void,
            mss as *mut c_void,
        ) != 0
        {
            j += 1;
            *groups.offset(j as isize) = *items.offset(i as isize);
            (*groups.offset(j as isize)).count = 0;
        }

        (*groups.offset(j as isize)).count += 1;
        i += 1;
    }

    /* ensure we filled the expected number of distinct groups */
    Assert!(j + 1 == ngroups);

    /* Sort the distinct groups by frequency (in descending order). */
    qsort_interruptible(
        groups as *mut c_void,
        ngroups as usize,
        core::mem::size_of::<SortItem>(),
        compare_sort_item_count,
        core::ptr::null_mut(),
    );

    *ndistinct = ngroups;
    groups
}

/* compare sort items (single dimension) */
unsafe extern "C" fn sort_item_compare(
    a: *const c_void,
    b: *const c_void,
    arg: *mut c_void,
) -> c_int {
    let ssup: SortSupport = arg as SortSupport;
    let ia: *mut SortItem = a as *mut SortItem;
    let ib: *mut SortItem = b as *mut SortItem;

    ApplySortComparator(
        *(*ia).values.offset(0),
        *(*ia).isnull.offset(0),
        *(*ib).values.offset(0),
        *(*ib).isnull.offset(0),
        ssup,
    )
}

/*
 * build_column_frequencies
 *		Compute frequencies of values in each column.
 *
 * This returns an array of SortItems for each attribute the MCV is built
 * on, with a frequency (number of occurrences) for each value. This is
 * then used to compute "base" frequency of MCV items.
 *
 * All the memory is allocated in a single chunk, so that a single pfree
 * is enough to release it. We do not allocate space for values/isnull
 * arrays in the SortItems, because we can simply point into the input
 * groups directly.
 */
unsafe fn build_column_frequencies(
    groups: *mut SortItem,
    ngroups: c_int,
    mss: MultiSortSupport,
    ncounts: *mut c_int,
) -> *mut *mut SortItem {
    let mut i: c_int;
    let mut dim: c_int;
    let result: *mut *mut SortItem;
    let mut ptr: *mut c_char;

    Assert!(!groups.is_null());
    Assert!(!ncounts.is_null());

    /* allocate arrays for all columns as a single chunk */
    ptr = palloc(
        MAXALIGN(core::mem::size_of::<*mut SortItem>() * (*mss).ndims as usize)
            + (*mss).ndims as usize
                * MAXALIGN(core::mem::size_of::<SortItem>() * ngroups as usize),
    ) as *mut c_char;

    /* initial array of pointers */
    result = ptr as *mut *mut SortItem;
    ptr = ptr.add(MAXALIGN(
        core::mem::size_of::<*mut SortItem>() * (*mss).ndims as usize,
    ));

    dim = 0;
    while dim < (*mss).ndims {
        let ssup: SortSupport = &mut (*(*mss).ssup.as_mut_ptr().offset(dim as isize));

        /* array of values for a single column */
        *result.offset(dim as isize) = ptr as *mut SortItem;
        ptr = ptr.add(MAXALIGN(core::mem::size_of::<SortItem>() * ngroups as usize));

        /* extract data for the dimension */
        i = 0;
        while i < ngroups {
            let rd: *mut SortItem = *result.offset(dim as isize);
            /* point into the input groups */
            (*rd.offset(i as isize)).values =
                &mut (*(*groups.offset(i as isize)).values.offset(dim as isize));
            (*rd.offset(i as isize)).isnull =
                &mut (*(*groups.offset(i as isize)).isnull.offset(dim as isize));
            (*rd.offset(i as isize)).count = (*groups.offset(i as isize)).count;
            i += 1;
        }

        /* sort the values, deduplicate */
        qsort_interruptible(
            *result.offset(dim as isize) as *mut c_void,
            ngroups as usize,
            core::mem::size_of::<SortItem>(),
            sort_item_compare,
            ssup as *mut c_void,
        );

        /*
         * Identify distinct values, compute frequency (there might be
         * multiple MCV items containing this value, so we need to sum counts
         * from all of them.
         */
        let rd: *mut SortItem = *result.offset(dim as isize);
        *ncounts.offset(dim as isize) = 1;
        i = 1;
        while i < ngroups {
            if sort_item_compare(
                rd.offset((i - 1) as isize) as *const c_void,
                rd.offset(i as isize) as *const c_void,
                ssup as *mut c_void,
            ) == 0
            {
                (*rd.offset((*ncounts.offset(dim as isize) - 1) as isize)).count +=
                    (*rd.offset(i as isize)).count;
                i += 1;
                continue;
            }

            *rd.offset(*ncounts.offset(dim as isize) as isize) = *rd.offset(i as isize);

            *ncounts.offset(dim as isize) += 1;
            i += 1;
        }
        dim += 1;
    }

    result
}

/*
 * statext_mcv_load
 *		Load the MCV list for the indicated pg_statistic_ext_data tuple.
 */
pub unsafe fn statext_mcv_load(mvoid: Oid, inh: bool) -> *mut MCVList {
    let result: *mut MCVList;
    let mut isnull: bool = false;
    let mcvlist: Datum;
    let htup: HeapTuple = SearchSysCache2(
        STATEXTDATASTXOID,
        ObjectIdGetDatum(mvoid),
        BoolGetDatum(inh),
    );

    if !HeapTupleIsValid(htup) {
        elog!(ERROR, "cache lookup failed for statistics object {}", mvoid);
    }

    mcvlist = SysCacheGetAttr(
        STATEXTDATASTXOID,
        htup,
        Anum_pg_statistic_ext_data_stxdmcv,
        &mut isnull,
    );

    if isnull {
        elog!(
            ERROR,
            "requested statistics kind \"{}\" is not yet built for statistics object {}",
            STATS_EXT_MCV,
            mvoid
        );
    }

    result = statext_mcv_deserialize(DatumGetByteaP(mcvlist));

    ReleaseSysCache(htup);

    result
}

/*
 * statext_mcv_serialize
 *		Serialize MCV list into a pg_mcv_list value.
 *
 * The MCV items may include values of various data types, and it's reasonable
 * to expect redundancy (values for a given attribute, repeated for multiple
 * MCV list items). So we deduplicate the values into arrays, and then replace
 * the values by indexes into those arrays.
 *
 * The overall structure of the serialized representation looks like this:
 *
 * +---------------+----------------+---------------------+-------+
 * | header fields | dimension info | deduplicated values | items |
 * +---------------+----------------+---------------------+-------+
 *
 * Where dimension info stores information about the type of the K-th
 * attribute (e.g. typlen, typbyval and length of deduplicated values).
 * Deduplicated values store deduplicated values for each attribute.  And
 * items store the actual MCV list items, with values replaced by indexes into
 * the arrays.
 *
 * When serializing the items, we use uint16 indexes. The number of MCV items
 * is limited by the statistics target (which is capped to 10k at the moment).
 * We might increase this to 65k and still fit into uint16, so there's a bit of
 * slack. Furthermore, this limit is on the number of distinct values per column,
 * and we usually have few of those (and various combinations of them for the
 * those MCV list). So uint16 seems fine for now.
 *
 * We don't really expect the serialization to save as much space as for
 * histograms, as we are not doing any bucket splits (which is the source
 * of high redundancy in histograms).
 *
 * TODO: Consider packing boolean flags (NULL) for each item into a single char
 * (or a longer type) instead of using an array of bool items.
 */
pub unsafe fn statext_mcv_serialize(
    mcvlist: *mut MCVList,
    stats: *mut *mut VacAttrStats,
) -> *mut bytea {
    let mut i: c_int;
    let mut dim: c_int;
    let ndims: c_int = (*mcvlist).ndimensions as c_int;

    let ssup: SortSupport;
    let info: *mut DimensionInfo;

    let mut total_length: Size;

    /* serialized items (indexes into arrays, etc.) */
    let raw: *mut bytea;
    let mut ptr: *mut c_char;
    let endptr: *mut c_char; // PG_USED_FOR_ASSERTS_ONLY

    /* values per dimension (and number of non-NULL values) */
    let values: *mut *mut Datum =
        palloc0(core::mem::size_of::<*mut Datum>() * ndims as usize) as *mut *mut Datum;
    let counts: *mut c_int =
        palloc0(core::mem::size_of::<c_int>() * ndims as usize) as *mut c_int;

    /*
     * We'll include some rudimentary information about the attribute types
     * (length, by-val flag), so that we don't have to look them up while
     * deserializing the MCV list (we already have the type OID in the
     * header).  This is safe because when changing the type of the attribute
     * the statistics gets dropped automatically.  We need to store the info
     * about the arrays of deduplicated values anyway.
     */
    info = palloc0(core::mem::size_of::<DimensionInfo>() * ndims as usize) as *mut DimensionInfo;

    /* sort support data for all attributes included in the MCV list */
    ssup = palloc0(core::mem::size_of::<SortSupportData>() * ndims as usize) as SortSupport;

    /* collect and deduplicate values for each dimension (attribute) */
    dim = 0;
    while dim < ndims {
        let mut ndistinct: c_int;
        let typentry;

        /*
         * Lookup the LT operator (can't get it from stats extra_data, as we
         * don't know how to interpret that - scalar vs. array etc.).
         */
        typentry = lookup_type_cache((**stats.offset(dim as isize)).attrtypid, TYPECACHE_LT_OPR);

        /* copy important info about the data type (length, by-value) */
        (*info.offset(dim as isize)).typlen =
            (*(**stats.offset(dim as isize)).attrtype).typlen as c_int;
        (*info.offset(dim as isize)).typbyval =
            (*(**stats.offset(dim as isize)).attrtype).typbyval;

        /* allocate space for values in the attribute and collect them */
        *values.offset(dim as isize) =
            palloc0(core::mem::size_of::<Datum>() * (*mcvlist).nitems as usize) as *mut Datum;

        i = 0;
        while i < (*mcvlist).nitems as c_int {
            /* skip NULL values - we don't need to deduplicate those */
            if *(*(*mcvlist).items.as_ptr().offset(i as isize)).isnull.offset(dim as isize) {
                i += 1;
                continue;
            }

            /* append the value at the end */
            *(*values.offset(dim as isize)).offset(*counts.offset(dim as isize) as isize) =
                *(*(*mcvlist).items.as_ptr().offset(i as isize)).values.offset(dim as isize);
            *counts.offset(dim as isize) += 1;
            i += 1;
        }

        /* if there are just NULL values in this dimension, we're done */
        if *counts.offset(dim as isize) == 0 {
            dim += 1;
            continue;
        }

        /* sort and deduplicate the data */
        (*ssup.offset(dim as isize)).ssup_cxt = CurrentMemoryContext;
        (*ssup.offset(dim as isize)).ssup_collation = (**stats.offset(dim as isize)).attrcollid;
        (*ssup.offset(dim as isize)).ssup_nulls_first = false;

        PrepareSortSupportFromOrderingOp((*typentry).lt_opr, &mut (*ssup.offset(dim as isize)));

        qsort_interruptible(
            *values.offset(dim as isize) as *mut c_void,
            *counts.offset(dim as isize) as usize,
            core::mem::size_of::<Datum>(),
            compare_scalars_simple,
            &mut (*ssup.offset(dim as isize)) as *mut SortSupportData as *mut c_void,
        );

        /*
         * Walk through the array and eliminate duplicate values, but keep the
         * ordering (so that we can do a binary search later). We know there's
         * at least one item as (counts[dim] != 0), so we can skip the first
         * element.
         */
        ndistinct = 1; /* number of distinct values */
        i = 1;
        while i < *counts.offset(dim as isize) {
            /* expect sorted array */
            Assert!(
                compare_datums_simple(
                    *(*values.offset(dim as isize)).offset((i - 1) as isize),
                    *(*values.offset(dim as isize)).offset(i as isize),
                    &mut (*ssup.offset(dim as isize)),
                ) <= 0
            );

            /* if the value is the same as the previous one, we can skip it */
            if compare_datums_simple(
                *(*values.offset(dim as isize)).offset((i - 1) as isize),
                *(*values.offset(dim as isize)).offset(i as isize),
                &mut (*ssup.offset(dim as isize)),
            ) == 0
            {
                i += 1;
                continue;
            }

            *(*values.offset(dim as isize)).offset(ndistinct as isize) =
                *(*values.offset(dim as isize)).offset(i as isize);
            ndistinct += 1;
            i += 1;
        }

        /* we must not exceed PG_UINT16_MAX, as we use uint16 indexes */
        Assert!(ndistinct <= uint16::MAX as c_int);

        /*
         * Store additional info about the attribute - number of deduplicated
         * values, and also size of the serialized data. For fixed-length data
         * types this is trivial to compute, for varwidth types we need to
         * actually walk the array and sum the sizes.
         */
        (*info.offset(dim as isize)).nvalues = ndistinct;

        if (*info.offset(dim as isize)).typbyval {
            /* by-value data types */
            (*info.offset(dim as isize)).nbytes =
                (*info.offset(dim as isize)).nvalues * (*info.offset(dim as isize)).typlen;

            /*
             * We copy the data into the MCV item during deserialization, so
             * we don't need to allocate any extra space.
             */
            (*info.offset(dim as isize)).nbytes_aligned = 0;
        } else if (*info.offset(dim as isize)).typlen > 0 {
            /* fixed-length by-ref */

            /*
             * We don't care about alignment in the serialized data, so we
             * pack the data as much as possible. But we also track how much
             * data will be needed after deserialization, and in that case we
             * need to account for alignment of each item.
             *
             * Note: As the items are fixed-length, we could easily compute
             * this during deserialization, but we do it here anyway.
             */
            (*info.offset(dim as isize)).nbytes =
                (*info.offset(dim as isize)).nvalues * (*info.offset(dim as isize)).typlen;
            (*info.offset(dim as isize)).nbytes_aligned = (*info.offset(dim as isize)).nvalues
                * MAXALIGN((*info.offset(dim as isize)).typlen as usize) as c_int;
        } else if (*info.offset(dim as isize)).typlen == -1 {
            /* varlena */
            (*info.offset(dim as isize)).nbytes = 0;
            (*info.offset(dim as isize)).nbytes_aligned = 0;
            i = 0;
            while i < (*info.offset(dim as isize)).nvalues {
                let len: Size;

                /*
                 * For varlena values, we detoast the values and store the
                 * length and data separately. We don't bother with alignment
                 * here, which means that during deserialization we need to
                 * copy the fields and only access the copies.
                 */
                *(*values.offset(dim as isize)).offset(i as isize) = PointerGetDatum(
                    PG_DETOAST_DATUM!(*(*values.offset(dim as isize)).offset(i as isize)),
                );

                /* serialized length (uint32 length + data) */
                len = VARSIZE_ANY_EXHDR(DatumGetPointer(
                    *(*values.offset(dim as isize)).offset(i as isize),
                )) as Size;
                (*info.offset(dim as isize)).nbytes += core::mem::size_of::<uint32>() as c_int; /* length */
                (*info.offset(dim as isize)).nbytes += len as c_int; /* value (no header) */

                /*
                 * During deserialization we'll build regular varlena values
                 * with full headers, and we need to align them properly.
                 */
                (*info.offset(dim as isize)).nbytes_aligned +=
                    MAXALIGN(VARHDRSZ as usize + len) as c_int;
                i += 1;
            }
        } else if (*info.offset(dim as isize)).typlen == -2 {
            /* cstring */
            (*info.offset(dim as isize)).nbytes = 0;
            (*info.offset(dim as isize)).nbytes_aligned = 0;
            i = 0;
            while i < (*info.offset(dim as isize)).nvalues {
                let len: Size;

                /*
                 * cstring is handled similar to varlena - first we store the
                 * length as uint32 and then the data. We don't care about
                 * alignment, which means that during deserialization we need
                 * to copy the fields and only access the copies.
                 */

                /* c-strings include terminator, so +1 byte */
                len = strlen(DatumGetCString(
                    *(*values.offset(dim as isize)).offset(i as isize),
                )) + 1;
                (*info.offset(dim as isize)).nbytes += core::mem::size_of::<uint32>() as c_int; /* length */
                (*info.offset(dim as isize)).nbytes += len as c_int; /* value */

                /* space needed for properly aligned deserialized copies */
                (*info.offset(dim as isize)).nbytes_aligned += MAXALIGN(len) as c_int;
                i += 1;
            }
        }

        /* we know (count>0) so there must be some data */
        Assert!((*info.offset(dim as isize)).nbytes > 0);
        dim += 1;
    }

    /*
     * Now we can finally compute how much space we'll actually need for the
     * whole serialized MCV list (varlena header, MCV header, dimension info
     * for each attribute, deduplicated values and items).
     */
    total_length = (3 * core::mem::size_of::<uint32>()) /* magic + type + nitems */
        + core::mem::size_of::<AttrNumber>() /* ndimensions */
        + (ndims as usize * core::mem::size_of::<Oid>()); /* attribute types */

    /* dimension info */
    total_length += ndims as usize * core::mem::size_of::<DimensionInfo>();

    /* add space for the arrays of deduplicated values */
    i = 0;
    while i < ndims {
        total_length += (*info.offset(i as isize)).nbytes as usize;
        i += 1;
    }

    /*
     * And finally account for the items (those are fixed-length, thanks to
     * replacing values with uint16 indexes into the deduplicated arrays).
     */
    total_length += (*mcvlist).nitems as usize * ITEM_SIZE(dim as usize);

    /*
     * Allocate space for the whole serialized MCV list (we'll skip bytes, so
     * we set them to zero to make the result more compressible).
     */
    raw = palloc0(VARHDRSZ as usize + total_length) as *mut bytea;
    SET_VARSIZE(raw as *mut c_char, (VARHDRSZ as usize + total_length) as int32);

    ptr = VARDATA(raw as *mut c_char);
    endptr = ptr.add(total_length);

    /* copy the MCV list header fields, one by one */
    core::ptr::copy_nonoverlapping(
        &(*mcvlist).magic as *const uint32 as *const c_char,
        ptr,
        core::mem::size_of::<uint32>(),
    );
    ptr = ptr.add(core::mem::size_of::<uint32>());

    core::ptr::copy_nonoverlapping(
        &(*mcvlist).r#type as *const uint32 as *const c_char,
        ptr,
        core::mem::size_of::<uint32>(),
    );
    ptr = ptr.add(core::mem::size_of::<uint32>());

    core::ptr::copy_nonoverlapping(
        &(*mcvlist).nitems as *const uint32 as *const c_char,
        ptr,
        core::mem::size_of::<uint32>(),
    );
    ptr = ptr.add(core::mem::size_of::<uint32>());

    core::ptr::copy_nonoverlapping(
        &(*mcvlist).ndimensions as *const AttrNumber as *const c_char,
        ptr,
        core::mem::size_of::<AttrNumber>(),
    );
    ptr = ptr.add(core::mem::size_of::<AttrNumber>());

    core::ptr::copy_nonoverlapping(
        (*mcvlist).types.as_ptr() as *const c_char,
        ptr,
        core::mem::size_of::<Oid>() * ndims as usize,
    );
    ptr = ptr.add(core::mem::size_of::<Oid>() * ndims as usize);

    /* store information about the attributes (data amounts, ...) */
    core::ptr::copy_nonoverlapping(
        info as *const c_char,
        ptr,
        core::mem::size_of::<DimensionInfo>() * ndims as usize,
    );
    ptr = ptr.add(core::mem::size_of::<DimensionInfo>() * ndims as usize);

    /* Copy the deduplicated values for all attributes to the output. */
    dim = 0;
    while dim < ndims {
        /* remember the starting point for Asserts later */
        let start: *mut c_char = ptr; // PG_USED_FOR_ASSERTS_ONLY
        let _ = start;

        i = 0;
        while i < (*info.offset(dim as isize)).nvalues {
            let value: Datum = *(*values.offset(dim as isize)).offset(i as isize);

            if (*info.offset(dim as isize)).typbyval {
                /* passed by value */
                let mut tmp: Datum = 0;

                /*
                 * For byval types, we need to copy just the significant bytes
                 * - we can't use memcpy directly, as that assumes
                 * little-endian behavior.  store_att_byval does almost what
                 * we need, but it requires a properly aligned buffer - the
                 * output buffer does not guarantee that. So we simply use a
                 * local Datum variable (which guarantees proper alignment),
                 * and then copy the value from it.
                 */
                store_att_byval(
                    &mut tmp as *mut Datum as *mut c_void,
                    value,
                    (*info.offset(dim as isize)).typlen,
                );

                core::ptr::copy_nonoverlapping(
                    &tmp as *const Datum as *const c_char,
                    ptr,
                    (*info.offset(dim as isize)).typlen as usize,
                );
                ptr = ptr.add((*info.offset(dim as isize)).typlen as usize);
            } else if (*info.offset(dim as isize)).typlen > 0 {
                /* passed by reference */
                /* no special alignment needed, treated as char array */
                core::ptr::copy_nonoverlapping(
                    DatumGetPointer(value),
                    ptr,
                    (*info.offset(dim as isize)).typlen as usize,
                );
                ptr = ptr.add((*info.offset(dim as isize)).typlen as usize);
            } else if (*info.offset(dim as isize)).typlen == -1 {
                /* varlena */
                let len: uint32 = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

                /* copy the length */
                core::ptr::copy_nonoverlapping(
                    &len as *const uint32 as *const c_char,
                    ptr,
                    core::mem::size_of::<uint32>(),
                );
                ptr = ptr.add(core::mem::size_of::<uint32>());

                /* data from the varlena value (without the header) */
                core::ptr::copy_nonoverlapping(
                    VARDATA_ANY(DatumGetPointer(value)),
                    ptr,
                    len as usize,
                );
                ptr = ptr.add(len as usize);
            } else if (*info.offset(dim as isize)).typlen == -2 {
                /* cstring */
                let len: uint32 = strlen(DatumGetCString(value)) as uint32 + 1;

                /* copy the length */
                core::ptr::copy_nonoverlapping(
                    &len as *const uint32 as *const c_char,
                    ptr,
                    core::mem::size_of::<uint32>(),
                );
                ptr = ptr.add(core::mem::size_of::<uint32>());

                /* value */
                core::ptr::copy_nonoverlapping(DatumGetCString(value), ptr, len as usize);
                ptr = ptr.add(len as usize);
            }

            /* no underflows or overflows */
            Assert!(
                (ptr as usize > start as usize)
                    && ((ptr as usize - start as usize)
                        <= (*info.offset(dim as isize)).nbytes as usize)
            );
            i += 1;
        }

        /* we should get exactly nbytes of data for this dimension */
        Assert!((ptr as usize - start as usize) == (*info.offset(dim as isize)).nbytes as usize);
        dim += 1;
    }

    /* Serialize the items, with uint16 indexes instead of the values. */
    i = 0;
    while i < (*mcvlist).nitems as c_int {
        let mcvitem: *mut MCVItem = (*mcvlist).items.as_mut_ptr().offset(i as isize);

        /* don't write beyond the allocated space */
        Assert!(ptr as usize <= (endptr as usize - ITEM_SIZE(dim as usize)));

        /* copy NULL and frequency flags into the serialized MCV */
        core::ptr::copy_nonoverlapping(
            (*mcvitem).isnull as *const c_char,
            ptr,
            core::mem::size_of::<bool>() * ndims as usize,
        );
        ptr = ptr.add(core::mem::size_of::<bool>() * ndims as usize);

        core::ptr::copy_nonoverlapping(
            &(*mcvitem).frequency as *const f64 as *const c_char,
            ptr,
            core::mem::size_of::<f64>(),
        );
        ptr = ptr.add(core::mem::size_of::<f64>());

        core::ptr::copy_nonoverlapping(
            &(*mcvitem).base_frequency as *const f64 as *const c_char,
            ptr,
            core::mem::size_of::<f64>(),
        );
        ptr = ptr.add(core::mem::size_of::<f64>());

        /* store the indexes last */
        dim = 0;
        while dim < ndims {
            let mut index: uint16 = 0;
            let value: *mut Datum;

            /* do the lookup only for non-NULL values */
            if !*(*mcvitem).isnull.offset(dim as isize) {
                value = bsearch_arg(
                    (*mcvitem).values.offset(dim as isize) as *const c_void,
                    *values.offset(dim as isize) as *const c_void,
                    (*info.offset(dim as isize)).nvalues as Size,
                    core::mem::size_of::<Datum>() as Size,
                    compare_scalars_simple_c,
                    &mut (*ssup.offset(dim as isize)) as *mut SortSupportData as *mut c_void,
                ) as *mut Datum;

                Assert!(!value.is_null()); /* serialization or deduplication error */

                /* compute index within the deduplicated array */
                index = (value.offset_from(*values.offset(dim as isize))) as uint16;

                /* check the index is within expected bounds */
                Assert!((index as c_int) < (*info.offset(dim as isize)).nvalues);
            }

            /* copy the index into the serialized MCV */
            core::ptr::copy_nonoverlapping(
                &index as *const uint16 as *const c_char,
                ptr,
                core::mem::size_of::<uint16>(),
            );
            ptr = ptr.add(core::mem::size_of::<uint16>());
            dim += 1;
        }

        /* make sure we don't overflow the allocated value */
        Assert!(ptr as usize <= endptr as usize);
        i += 1;
    }

    /* at this point we expect to match the total_length exactly */
    Assert!(ptr as usize == endptr as usize);

    pfree(values as *mut c_void);
    pfree(counts as *mut c_void);

    raw
}

/*
 * statext_mcv_deserialize
 *		Reads serialized MCV list into MCVList structure.
 *
 * All the memory needed by the MCV list is allocated as a single chunk, so
 * it's possible to simply pfree() it at once.
 */
pub unsafe fn statext_mcv_deserialize(data: *mut bytea) -> *mut MCVList {
    let mut dim: c_int;
    let mut i: c_int;
    let mut expected_size: Size;
    let mut mcvlist: *mut MCVList;
    let raw: *mut c_char;
    let mut ptr: *mut c_char;
    let endptr: *mut c_char; // PG_USED_FOR_ASSERTS_ONLY

    let ndims: c_int;
    let nitems: c_int;
    let info: *mut DimensionInfo;

    /* local allocation buffer (used only for deserialization) */
    let map: *mut *mut Datum;

    /* MCV list */
    let mcvlen: Size;

    /* buffer used for the result */
    let mut datalen: Size;
    let mut dataptr: *mut c_char;
    let mut valuesptr: *mut c_char;
    let mut isnullptr: *mut c_char;

    if data.is_null() {
        return core::ptr::null_mut();
    }

    /*
     * We can't possibly deserialize a MCV list if there's not even a complete
     * header. We need an explicit formula here, because we serialize the
     * header fields one by one, so we need to ignore struct alignment.
     */
    if (VARSIZE_ANY(data as *const c_char) as Size) < MinSizeOfMCVList {
        elog!(
            ERROR,
            "invalid MCV size {} (expected at least {})",
            VARSIZE_ANY(data as *const c_char),
            MinSizeOfMCVList
        );
    }

    /* read the MCV list header */
    mcvlist = palloc0(core::mem::offset_of!(MCVList, items)) as *mut MCVList;

    /* pointer to the data part (skip the varlena header) */
    raw = data as *mut c_char;
    ptr = VARDATA_ANY(raw);
    endptr = (raw as *mut c_char).add(VARSIZE_ANY(data as *const c_char) as usize);

    /* get the header and perform further sanity checks */
    core::ptr::copy_nonoverlapping(
        ptr,
        &mut (*mcvlist).magic as *mut uint32 as *mut c_char,
        core::mem::size_of::<uint32>(),
    );
    ptr = ptr.add(core::mem::size_of::<uint32>());

    core::ptr::copy_nonoverlapping(
        ptr,
        &mut (*mcvlist).r#type as *mut uint32 as *mut c_char,
        core::mem::size_of::<uint32>(),
    );
    ptr = ptr.add(core::mem::size_of::<uint32>());

    core::ptr::copy_nonoverlapping(
        ptr,
        &mut (*mcvlist).nitems as *mut uint32 as *mut c_char,
        core::mem::size_of::<uint32>(),
    );
    ptr = ptr.add(core::mem::size_of::<uint32>());

    core::ptr::copy_nonoverlapping(
        ptr,
        &mut (*mcvlist).ndimensions as *mut AttrNumber as *mut c_char,
        core::mem::size_of::<AttrNumber>(),
    );
    ptr = ptr.add(core::mem::size_of::<AttrNumber>());

    if (*mcvlist).magic != STATS_MCV_MAGIC {
        elog!(
            ERROR,
            "invalid MCV magic {} (expected {})",
            (*mcvlist).magic,
            STATS_MCV_MAGIC
        );
    }

    if (*mcvlist).r#type != STATS_MCV_TYPE_BASIC {
        elog!(
            ERROR,
            "invalid MCV type {} (expected {})",
            (*mcvlist).r#type,
            STATS_MCV_TYPE_BASIC
        );
    }

    if (*mcvlist).ndimensions == 0 {
        elog!(ERROR, "invalid zero-length dimension array in MCVList");
    } else if ((*mcvlist).ndimensions as c_int > STATS_MAX_DIMENSIONS as c_int)
        || ((*mcvlist).ndimensions < 0)
    {
        elog!(
            ERROR,
            "invalid length ({}) dimension array in MCVList",
            (*mcvlist).ndimensions
        );
    }

    if (*mcvlist).nitems == 0 {
        elog!(ERROR, "invalid zero-length item array in MCVList");
    } else if (*mcvlist).nitems as c_int > STATS_MCVLIST_MAX_ITEMS {
        elog!(
            ERROR,
            "invalid length ({}) item array in MCVList",
            (*mcvlist).nitems
        );
    }

    nitems = (*mcvlist).nitems as c_int;
    ndims = (*mcvlist).ndimensions as c_int;

    /*
     * Check amount of data including DimensionInfo for all dimensions and
     * also the serialized items (including uint16 indexes). Also, walk
     * through the dimension information and add it to the sum.
     */
    expected_size = SizeOfMCVList(ndims as usize, nitems as usize);

    /*
     * Check that we have at least the dimension and info records, along with
     * the items. We don't know the size of the serialized values yet. We need
     * to do this check first, before accessing the dimension info.
     */
    if (VARSIZE_ANY(data as *const c_char) as Size) < expected_size {
        elog!(
            ERROR,
            "invalid MCV size {} (expected {})",
            VARSIZE_ANY(data as *const c_char),
            expected_size
        );
    }

    /* Now copy the array of type Oids. */
    core::ptr::copy_nonoverlapping(
        ptr,
        (*mcvlist).types.as_mut_ptr() as *mut c_char,
        core::mem::size_of::<Oid>() * ndims as usize,
    );
    ptr = ptr.add(core::mem::size_of::<Oid>() * ndims as usize);

    /* Now it's safe to access the dimension info. */
    info = palloc(ndims as usize * core::mem::size_of::<DimensionInfo>()) as *mut DimensionInfo;

    core::ptr::copy_nonoverlapping(
        ptr,
        info as *mut c_char,
        ndims as usize * core::mem::size_of::<DimensionInfo>(),
    );
    ptr = ptr.add(ndims as usize * core::mem::size_of::<DimensionInfo>());

    /* account for the value arrays */
    dim = 0;
    while dim < ndims {
        /*
         * XXX I wonder if we can/should rely on asserts here. Maybe those
         * checks should be done every time?
         */
        Assert!((*info.offset(dim as isize)).nvalues >= 0);
        Assert!((*info.offset(dim as isize)).nbytes >= 0);

        expected_size += (*info.offset(dim as isize)).nbytes as usize;
        dim += 1;
    }

    /*
     * Now we know the total expected MCV size, including all the pieces
     * (header, dimension info. items and deduplicated data). So do the final
     * check on size.
     */
    if VARSIZE_ANY(data as *const c_char) as Size != expected_size {
        elog!(
            ERROR,
            "invalid MCV size {} (expected {})",
            VARSIZE_ANY(data as *const c_char),
            expected_size
        );
    }

    /*
     * We need an array of Datum values for each dimension, so that we can
     * easily translate the uint16 indexes later. We also need a top-level
     * array of pointers to those per-dimension arrays.
     *
     * While allocating the arrays for dimensions, compute how much space we
     * need for a copy of the by-ref data, as we can't simply point to the
     * original values (it might go away).
     */
    datalen = 0; /* space for by-ref data */
    map = palloc(ndims as usize * core::mem::size_of::<*mut Datum>()) as *mut *mut Datum;

    dim = 0;
    while dim < ndims {
        *map.offset(dim as isize) =
            palloc(core::mem::size_of::<Datum>() * (*info.offset(dim as isize)).nvalues as usize)
                as *mut Datum;

        /* space needed for a copy of data for by-ref types */
        datalen += (*info.offset(dim as isize)).nbytes_aligned as usize;
        dim += 1;
    }

    /*
     * Now resize the MCV list so that the allocation includes all the data.
     *
     * Allocate space for a copy of the data, as we can't simply reference the
     * serialized data - it's not aligned properly, and it may disappear while
     * we're still using the MCV list, e.g. due to catcache release.
     *
     * We do care about alignment here, because we will allocate all the
     * pieces at once, but then use pointers to different parts.
     */
    mcvlen = MAXALIGN(
        core::mem::offset_of!(MCVList, items)
            + (core::mem::size_of::<MCVItem>() * nitems as usize),
    );

    /* arrays of values and isnull flags for all MCV items */
    let mut mcvlen = mcvlen;
    mcvlen += nitems as usize * MAXALIGN(core::mem::size_of::<Datum>() * ndims as usize);
    mcvlen += nitems as usize * MAXALIGN(core::mem::size_of::<bool>() * ndims as usize);

    /* we don't quite need to align this, but it makes some asserts easier */
    mcvlen += MAXALIGN(datalen);

    /* now resize the deserialized MCV list, and compute pointers to parts */
    mcvlist = repalloc(mcvlist as *mut c_void, mcvlen) as *mut MCVList;

    /* pointer to the beginning of values/isnull arrays */
    valuesptr = (mcvlist as *mut c_char).add(MAXALIGN(
        core::mem::offset_of!(MCVList, items)
            + (core::mem::size_of::<MCVItem>() * nitems as usize),
    ));

    isnullptr = valuesptr.add(nitems as usize * MAXALIGN(core::mem::size_of::<Datum>() * ndims as usize));

    dataptr = isnullptr.add(nitems as usize * MAXALIGN(core::mem::size_of::<bool>() * ndims as usize));

    /*
     * Build mapping (index => value) for translating the serialized data into
     * the in-memory representation.
     */
    dim = 0;
    while dim < ndims {
        /* remember start position in the input array */
        let start: *mut c_char = ptr; // PG_USED_FOR_ASSERTS_ONLY
        let _ = start;

        if (*info.offset(dim as isize)).typbyval {
            /* for by-val types we simply copy data into the mapping */
            i = 0;
            while i < (*info.offset(dim as isize)).nvalues {
                let mut v: Datum = 0;

                core::ptr::copy_nonoverlapping(
                    ptr,
                    &mut v as *mut Datum as *mut c_char,
                    (*info.offset(dim as isize)).typlen as usize,
                );
                ptr = ptr.add((*info.offset(dim as isize)).typlen as usize);

                *(*map.offset(dim as isize)).offset(i as isize) =
                    fetch_att(&v as *const Datum as *const c_void, true, (*info.offset(dim as isize)).typlen);

                /* no under/overflow of input array */
                Assert!(ptr as usize <= (start as usize + (*info.offset(dim as isize)).nbytes as usize));
                i += 1;
            }
        } else {
            /* for by-ref types we need to also make a copy of the data */

            /* passed by reference, but fixed length (name, tid, ...) */
            if (*info.offset(dim as isize)).typlen > 0 {
                i = 0;
                while i < (*info.offset(dim as isize)).nvalues {
                    core::ptr::copy_nonoverlapping(
                        ptr,
                        dataptr,
                        (*info.offset(dim as isize)).typlen as usize,
                    );
                    ptr = ptr.add((*info.offset(dim as isize)).typlen as usize);

                    /* just point into the array */
                    *(*map.offset(dim as isize)).offset(i as isize) =
                        PointerGetDatum(dataptr as *const c_void);
                    dataptr = dataptr.add(MAXALIGN((*info.offset(dim as isize)).typlen as usize));
                    i += 1;
                }
            } else if (*info.offset(dim as isize)).typlen == -1 {
                /* varlena */
                i = 0;
                while i < (*info.offset(dim as isize)).nvalues {
                    let mut len: uint32 = 0;

                    /* read the uint32 length */
                    core::ptr::copy_nonoverlapping(
                        ptr,
                        &mut len as *mut uint32 as *mut c_char,
                        core::mem::size_of::<uint32>(),
                    );
                    ptr = ptr.add(core::mem::size_of::<uint32>());

                    /* the length is data-only */
                    SET_VARSIZE(dataptr, (len + VARHDRSZ as uint32) as int32);
                    core::ptr::copy_nonoverlapping(ptr, VARDATA(dataptr), len as usize);
                    ptr = ptr.add(len as usize);

                    /* just point into the array */
                    *(*map.offset(dim as isize)).offset(i as isize) =
                        PointerGetDatum(dataptr as *const c_void);

                    /* skip to place of the next deserialized value */
                    dataptr = dataptr.add(MAXALIGN(len as usize + VARHDRSZ as usize));
                    i += 1;
                }
            } else if (*info.offset(dim as isize)).typlen == -2 {
                /* cstring */
                i = 0;
                while i < (*info.offset(dim as isize)).nvalues {
                    let mut len: uint32 = 0;

                    core::ptr::copy_nonoverlapping(
                        ptr,
                        &mut len as *mut uint32 as *mut c_char,
                        core::mem::size_of::<uint32>(),
                    );
                    ptr = ptr.add(core::mem::size_of::<uint32>());

                    core::ptr::copy_nonoverlapping(ptr, dataptr, len as usize);
                    ptr = ptr.add(len as usize);

                    /* just point into the array */
                    *(*map.offset(dim as isize)).offset(i as isize) =
                        PointerGetDatum(dataptr as *const c_void);
                    dataptr = dataptr.add(MAXALIGN(len as usize));
                    i += 1;
                }
            }

            /* no under/overflow of input array */
            Assert!(ptr as usize <= (start as usize + (*info.offset(dim as isize)).nbytes as usize));

            /* no overflow of the output mcv value */
            Assert!(dataptr as usize <= ((mcvlist as *mut c_char as usize) + mcvlen));
        }

        /* check we consumed input data for this dimension exactly */
        Assert!(ptr as usize == (start as usize + (*info.offset(dim as isize)).nbytes as usize));
        dim += 1;
    }

    /* we should have also filled the MCV list exactly */
    Assert!(dataptr as usize == ((mcvlist as *mut c_char as usize) + mcvlen));

    /* deserialize the MCV items and translate the indexes to Datums */
    i = 0;
    while i < nitems {
        let item: *mut MCVItem = (*mcvlist).items.as_mut_ptr().offset(i as isize);

        (*item).values = valuesptr as *mut Datum;
        valuesptr = valuesptr.add(MAXALIGN(core::mem::size_of::<Datum>() * ndims as usize));

        (*item).isnull = isnullptr as *mut bool;
        isnullptr = isnullptr.add(MAXALIGN(core::mem::size_of::<bool>() * ndims as usize));

        core::ptr::copy_nonoverlapping(
            ptr,
            (*item).isnull as *mut c_char,
            core::mem::size_of::<bool>() * ndims as usize,
        );
        ptr = ptr.add(core::mem::size_of::<bool>() * ndims as usize);

        core::ptr::copy_nonoverlapping(
            ptr,
            &mut (*item).frequency as *mut f64 as *mut c_char,
            core::mem::size_of::<f64>(),
        );
        ptr = ptr.add(core::mem::size_of::<f64>());

        core::ptr::copy_nonoverlapping(
            ptr,
            &mut (*item).base_frequency as *mut f64 as *mut c_char,
            core::mem::size_of::<f64>(),
        );
        ptr = ptr.add(core::mem::size_of::<f64>());

        /* finally translate the indexes (for non-NULL only) */
        dim = 0;
        while dim < ndims {
            let mut index: uint16 = 0;

            core::ptr::copy_nonoverlapping(
                ptr,
                &mut index as *mut uint16 as *mut c_char,
                core::mem::size_of::<uint16>(),
            );
            ptr = ptr.add(core::mem::size_of::<uint16>());

            if *(*item).isnull.offset(dim as isize) {
                dim += 1;
                continue;
            }

            *(*item).values.offset(dim as isize) =
                *(*map.offset(dim as isize)).offset(index as isize);
            dim += 1;
        }

        /* check we're not overflowing the input */
        Assert!(ptr as usize <= endptr as usize);
        i += 1;
    }

    /* check that we processed all the data */
    Assert!(ptr as usize == endptr as usize);

    /* release the buffers used for mapping */
    dim = 0;
    while dim < ndims {
        pfree(*map.offset(dim as isize) as *mut c_void);
        dim += 1;
    }

    pfree(map as *mut c_void);

    mcvlist
}

/*
 * SRF with details about buckets of a histogram:
 *
 * - item ID (0...nitems)
 * - values (string array)
 * - nulls only (boolean array)
 * - frequency (double precision)
 * - base_frequency (double precision)
 *
 * The input is the OID of the statistics, and there are no rows returned if
 * the statistics contains no histogram.
 */
pub unsafe fn pg_stats_ext_mcvlist_items(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;

    /* stuff done only on the first call of the function */
    if SRF_IS_FIRSTCALL(fcinfo) {
        let oldcontext: MemoryContext;
        let mcvlist: *mut MCVList;
        let mut tupdesc: TupleDesc = core::ptr::null_mut();

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT(fcinfo);

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        mcvlist = statext_mcv_deserialize(PG_GETARG_BYTEA_P!(fcinfo, 0));

        (*funcctx).user_fctx = mcvlist as *mut c_void;

        /* total number of tuples to be returned */
        (*funcctx).max_calls = 0;
        if !(*funcctx).user_fctx.is_null() {
            (*funcctx).max_calls = (*mcvlist).nitems as u64;
        }

        /* Build a tuple descriptor for our result type */
        if get_call_result_type(fcinfo, core::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
            ereport!(
                ERROR,
                errmsg!(
                    "function returning record called in context that cannot accept type record"
                )
            );
            // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        }
        tupdesc = BlessTupleDesc(tupdesc);

        /*
         * generate attribute metadata needed later to produce tuples from raw
         * C strings
         */
        (*funcctx).attinmeta = TupleDescGetAttInMetadata(tupdesc);

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP(fcinfo);

    if (*funcctx).call_cntr < (*funcctx).max_calls
    /* do when there is more left to send */
    {
        let mut values: [Datum; 5] = [0; 5];
        let mut nulls: [bool; 5] = [false; 5];
        let tuple: HeapTuple;
        let result: Datum;
        let mut astate_values: *mut ArrayBuildState = core::ptr::null_mut();
        let mut astate_nulls: *mut ArrayBuildState = core::ptr::null_mut();

        let mut i: c_int;
        let mcvlist: *mut MCVList;
        let item: *mut MCVItem;

        mcvlist = (*funcctx).user_fctx as *mut MCVList;

        Assert!((*funcctx).call_cntr < (*mcvlist).nitems as u64);

        item = (*mcvlist).items.as_mut_ptr().offset((*funcctx).call_cntr as isize);

        i = 0;
        while i < (*mcvlist).ndimensions as c_int {
            astate_nulls = accumArrayResult(
                astate_nulls,
                BoolGetDatum(*(*item).isnull.offset(i as isize)),
                false,
                BOOLOID,
                CurrentMemoryContext,
            );

            if !*(*item).isnull.offset(i as isize) {
                let mut isvarlena: bool = false;
                let mut outfunc: Oid = 0;
                let mut fmgrinfo: FmgrInfo = core::mem::zeroed();
                let val: Datum;
                let txt: *mut text;

                /* lookup output func for the type */
                getTypeOutputInfo((*mcvlist).types[i as usize], &mut outfunc, &mut isvarlena);
                fmgr_info(outfunc, &mut fmgrinfo);

                val = FunctionCall1!(&mut fmgrinfo, *(*item).values.offset(i as isize));
                txt = cstring_to_text(DatumGetPointer(val));

                astate_values = accumArrayResult(
                    astate_values,
                    PointerGetDatum(txt as *const c_void),
                    false,
                    TEXTOID,
                    CurrentMemoryContext,
                );
            } else {
                astate_values = accumArrayResult(
                    astate_values,
                    0 as Datum,
                    true,
                    TEXTOID,
                    CurrentMemoryContext,
                );
            }
            i += 1;
        }

        values[0] = Int32GetDatum((*funcctx).call_cntr as int32);
        values[1] = makeArrayResult(astate_values, CurrentMemoryContext);
        values[2] = makeArrayResult(astate_nulls, CurrentMemoryContext);
        values[3] = Float8GetDatum((*item).frequency);
        values[4] = Float8GetDatum((*item).base_frequency);

        /* no NULLs in the tuple */
        core::ptr::write_bytes(nulls.as_mut_ptr(), 0, nulls.len());

        /* build a tuple */
        tuple = heap_form_tuple(
            (*(*funcctx).attinmeta).tupdesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        /* make the tuple into a datum */
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result)
    } else
    /* do when there is no more left */
    {
        SRF_RETURN_DONE(funcctx)
    }
}

/*
 * pg_mcv_list_in		- input routine for type pg_mcv_list.
 *
 * pg_mcv_list is real enough to be a table column, but it has no operations
 * of its own, and disallows input too
 */
pub unsafe fn pg_mcv_list_in(fcinfo: FunctionCallInfo) -> Datum {
    /*
     * pg_mcv_list stores the data in binary form and parsing text input is
     * not needed, so disallow this.
     */
    ereport!(
        ERROR,
        errmsg!("cannot accept a value of type {}", "pg_mcv_list")
    );
    // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)

    #[allow(unreachable_code)]
    PG_RETURN_VOID!() /* keep compiler quiet */
}

/*
 * pg_mcv_list_out		- output routine for type pg_mcv_list.
 *
 * MCV lists are serialized into a bytea value, so we simply call byteaout()
 * to serialize the value into text. But it'd be nice to serialize that into
 * a meaningful representation (e.g. for inspection by people).
 *
 * XXX This should probably return something meaningful, similar to what
 * pg_dependencies_out does. Not sure how to deal with the deduplicated
 * values, though - do we want to expand that or not?
 */
pub unsafe fn pg_mcv_list_out(fcinfo: FunctionCallInfo) -> Datum {
    byteaout(fcinfo)
}

/*
 * pg_mcv_list_recv		- binary input routine for type pg_mcv_list.
 */
pub unsafe fn pg_mcv_list_recv(_fcinfo: FunctionCallInfo) -> Datum {
    ereport!(
        ERROR,
        errmsg!("cannot accept a value of type {}", "pg_mcv_list")
    );
    // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)

    #[allow(unreachable_code)]
    PG_RETURN_VOID!() /* keep compiler quiet */
}

/*
 * pg_mcv_list_send		- binary output routine for type pg_mcv_list.
 *
 * MCV lists are serialized in a bytea value (although the type is named
 * differently), so let's just send that.
 */
pub unsafe fn pg_mcv_list_send(fcinfo: FunctionCallInfo) -> Datum {
    byteasend(fcinfo)
}

/*
 * match the attribute/expression to a dimension of the statistic
 *
 * Returns the zero-based index of the matching statistics dimension.
 * Optionally determines the collation.
 */
unsafe fn mcv_match_expression(
    expr: *mut Node,
    keys: *mut Bitmapset,
    exprs: *mut List,
    collid: *mut Oid,
) -> c_int {
    let mut idx: c_int;

    if IsA!(expr, Var) {
        /* simple Var, so just lookup using varattno */
        let var: *mut Var = expr as *mut Var;

        if !collid.is_null() {
            *collid = (*var).varcollid;
        }

        idx = bms_member_index(keys, (*var).varattno as c_int);

        if idx < 0 {
            elog!(ERROR, "variable not found in statistics object");
        }
    } else {
        /* expression - lookup in stats expressions */

        if !collid.is_null() {
            *collid = exprCollation(expr);
        }

        /* expressions are stored after the simple columns */
        idx = bms_num_members(keys);
        let mut found: bool = false;
        foreach!(lc, exprs, {
            let stat_expr: *mut Node = lfirst(current_cell!(lc)) as *mut Node;

            if equal(expr as *const c_void, stat_expr as *const c_void) {
                found = true;
                break;
            }

            idx += 1;
        });

        if !found {
            elog!(ERROR, "expression not found in statistics object");
        }
    }

    idx
}

/*
 * mcv_get_match_bitmap
 *	Evaluate clauses using the MCV list, and update the match bitmap.
 *
 * A match bitmap keeps match/mismatch status for each MCV item, and we
 * update it based on additional clauses. We also use it to skip items
 * that can't possibly match (e.g. item marked as "mismatch" can't change
 * to "match" when evaluating AND clause list).
 *
 * The function also returns a flag indicating whether there was an
 * equality condition for all attributes, the minimum frequency in the MCV
 * list, and a total MCV frequency (sum of frequencies for all items).
 *
 * XXX Currently the match bitmap uses a bool for each MCV item, which is
 * somewhat wasteful as we could do with just a single bit, thus reducing
 * the size to ~1/8. It would also allow us to combine bitmaps simply using
 * & and |, which should be faster than min/max. The bitmaps are fairly
 * small, though (thanks to the cap on the MCV list size).
 */
unsafe fn mcv_get_match_bitmap(
    root: *mut PlannerInfo,
    clauses: *mut List,
    keys: *mut Bitmapset,
    exprs: *mut List,
    mcvlist: *mut MCVList,
    is_or: bool,
) -> *mut bool {
    let matches: *mut bool;

    /* The bitmap may be partially built. */
    Assert!(clauses != NIL);
    Assert!(!mcvlist.is_null());
    Assert!((*mcvlist).nitems > 0);
    Assert!((*mcvlist).nitems as c_int <= STATS_MCVLIST_MAX_ITEMS);

    matches = palloc(core::mem::size_of::<bool>() * (*mcvlist).nitems as usize) as *mut bool;
    core::ptr::write_bytes(matches, (!is_or) as u8, (*mcvlist).nitems as usize);

    /*
     * Loop through the list of clauses, and for each of them evaluate all the
     * MCV items not yet eliminated by the preceding clauses.
     */
    foreach!(l, clauses, {
        let mut clause: *mut Node = lfirst(current_cell!(l)) as *mut Node;

        /* if it's a RestrictInfo, then extract the clause */
        if IsA!(clause, RestrictInfo) {
            clause = (*(clause as *mut crate::nodes::pathnodes::RestrictInfo)).clause as *mut Node;
        }

        /*
         * Handle the various types of clauses - OpClause, NullTest and
         * AND/OR/NOT
         */
        if is_opclause(clause as *const c_void) {
            let expr: *mut OpExpr = clause as *mut OpExpr;
            let mut opproc: FmgrInfo = core::mem::zeroed();

            /* valid only after examine_opclause_args returns true */
            let mut clause_expr: *mut Node = core::ptr::null_mut();
            let mut cst: *mut Const = core::ptr::null_mut();
            let mut expronleft: bool = false;
            let idx: c_int;
            let mut collid: Oid = 0;

            fmgr_info(get_opcode((*expr).opno), &mut opproc);

            /* extract the var/expr and const from the expression */
            if !examine_opclause_args((*expr).args, &mut clause_expr, &mut cst, &mut expronleft) {
                elog!(ERROR, "incompatible clause");
            }

            /* match the attribute/expression to a dimension of the statistic */
            idx = mcv_match_expression(clause_expr, keys, exprs, &mut collid);

            /*
             * Walk through the MCV items and evaluate the current clause. We
             * can skip items that were already ruled out, and terminate if
             * there are no remaining MCV items that might possibly match.
             */
            let mut i: c_int = 0;
            while i < (*mcvlist).nitems as c_int {
                let mut r#match: bool = true;
                let item: *mut MCVItem = (*mcvlist).items.as_mut_ptr().offset(i as isize);

                Assert!(idx >= 0);

                /*
                 * When the MCV item or the Const value is NULL we can treat
                 * this as a mismatch. We must not call the operator because
                 * of strictness.
                 */
                if *(*item).isnull.offset(idx as isize) || (*cst).constisnull {
                    *matches.offset(i as isize) =
                        RESULT_MERGE(*matches.offset(i as isize), is_or, false);
                    i += 1;
                    continue;
                }

                /*
                 * Skip MCV items that can't change result in the bitmap. Once
                 * the value gets false for AND-lists, or true for OR-lists,
                 * we don't need to look at more clauses.
                 */
                if RESULT_IS_FINAL(*matches.offset(i as isize), is_or) {
                    i += 1;
                    continue;
                }

                /*
                 * First check whether the constant is below the lower
                 * boundary (in that case we can skip the bucket, because
                 * there's no overlap).
                 *
                 * We don't store collations used to build the statistics, but
                 * we can use the collation for the attribute itself, as
                 * stored in varcollid. We do reset the statistics after a
                 * type change (including collation change), so this is OK.
                 * For expressions, we use the collation extracted from the
                 * expression itself.
                 */
                if expronleft {
                    r#match = DatumGetBool(FunctionCall2Coll(
                        &mut opproc,
                        collid,
                        *(*item).values.offset(idx as isize),
                        (*cst).constvalue,
                    ));
                } else {
                    r#match = DatumGetBool(FunctionCall2Coll(
                        &mut opproc,
                        collid,
                        (*cst).constvalue,
                        *(*item).values.offset(idx as isize),
                    ));
                }

                /* update the match bitmap with the result */
                *matches.offset(i as isize) =
                    RESULT_MERGE(*matches.offset(i as isize), is_or, r#match);
                i += 1;
            }
        } else if IsA!(clause, ScalarArrayOpExpr) {
            let expr: *mut ScalarArrayOpExpr = clause as *mut ScalarArrayOpExpr;
            let mut opproc: FmgrInfo = core::mem::zeroed();

            /* valid only after examine_opclause_args returns true */
            let mut clause_expr: *mut Node = core::ptr::null_mut();
            let mut cst: *mut Const = core::ptr::null_mut();
            let mut expronleft: bool = false;
            let mut collid: Oid = 0;
            let idx: c_int;

            /* array evaluation */
            let arrayval: *mut ArrayType;
            let mut elmlen: int16 = 0;
            let mut elmbyval: bool = false;
            let mut elmalign: c_char = 0;
            let mut num_elems: c_int = 0;
            let mut elem_values: *mut Datum = core::ptr::null_mut();
            let mut elem_nulls: *mut bool = core::ptr::null_mut();

            fmgr_info(get_opcode((*expr).opno), &mut opproc);

            /* extract the var/expr and const from the expression */
            if !examine_opclause_args((*expr).args, &mut clause_expr, &mut cst, &mut expronleft) {
                elog!(ERROR, "incompatible clause");
            }

            /* We expect Var on left */
            if !expronleft {
                elog!(ERROR, "incompatible clause");
            }

            /*
             * Deconstruct the array constant, unless it's NULL (we'll cover
             * that case below)
             */
            if !(*cst).constisnull {
                arrayval = DatumGetArrayTypeP((*cst).constvalue);
                get_typlenbyvalalign(
                    ARR_ELEMTYPE(arrayval),
                    &mut elmlen,
                    &mut elmbyval,
                    &mut elmalign,
                );
                deconstruct_array(
                    arrayval,
                    ARR_ELEMTYPE(arrayval),
                    elmlen as c_int,
                    elmbyval,
                    elmalign,
                    &mut elem_values,
                    &mut elem_nulls,
                    &mut num_elems,
                );
            }

            /* match the attribute/expression to a dimension of the statistic */
            idx = mcv_match_expression(clause_expr, keys, exprs, &mut collid);

            /*
             * Walk through the MCV items and evaluate the current clause. We
             * can skip items that were already ruled out, and terminate if
             * there are no remaining MCV items that might possibly match.
             */
            let mut i: c_int = 0;
            while i < (*mcvlist).nitems as c_int {
                let mut j: c_int;
                let mut r#match: bool = !(*expr).useOr;
                let item: *mut MCVItem = (*mcvlist).items.as_mut_ptr().offset(i as isize);

                /*
                 * When the MCV item or the Const value is NULL we can treat
                 * this as a mismatch. We must not call the operator because
                 * of strictness.
                 */
                if *(*item).isnull.offset(idx as isize) || (*cst).constisnull {
                    *matches.offset(i as isize) =
                        RESULT_MERGE(*matches.offset(i as isize), is_or, false);
                    i += 1;
                    continue;
                }

                /*
                 * Skip MCV items that can't change result in the bitmap. Once
                 * the value gets false for AND-lists, or true for OR-lists,
                 * we don't need to look at more clauses.
                 */
                if RESULT_IS_FINAL(*matches.offset(i as isize), is_or) {
                    i += 1;
                    continue;
                }

                j = 0;
                while j < num_elems {
                    let elem_value: Datum = *elem_values.offset(j as isize);
                    let elem_isnull: bool = *elem_nulls.offset(j as isize);
                    let elem_match: bool;

                    /* NULL values always evaluate as not matching. */
                    if elem_isnull {
                        r#match = RESULT_MERGE(r#match, (*expr).useOr, false);
                        j += 1;
                        continue;
                    }

                    /*
                     * Stop evaluating the array elements once we reach a
                     * matching value that can't change - ALL() is the same as
                     * AND-list, ANY() is the same as OR-list.
                     */
                    if RESULT_IS_FINAL(r#match, (*expr).useOr) {
                        break;
                    }

                    elem_match = DatumGetBool(FunctionCall2Coll(
                        &mut opproc,
                        collid,
                        *(*item).values.offset(idx as isize),
                        elem_value,
                    ));

                    r#match = RESULT_MERGE(r#match, (*expr).useOr, elem_match);
                    j += 1;
                }

                /* update the match bitmap with the result */
                *matches.offset(i as isize) =
                    RESULT_MERGE(*matches.offset(i as isize), is_or, r#match);
                i += 1;
            }
        } else if IsA!(clause, NullTest) {
            let expr: *mut NullTest = clause as *mut NullTest;
            let clause_expr: *mut Node = (*expr).arg as *mut Node;

            /* match the attribute/expression to a dimension of the statistic */
            let idx: c_int = mcv_match_expression(clause_expr, keys, exprs, core::ptr::null_mut());

            /*
             * Walk through the MCV items and evaluate the current clause. We
             * can skip items that were already ruled out, and terminate if
             * there are no remaining MCV items that might possibly match.
             */
            let mut i: c_int = 0;
            while i < (*mcvlist).nitems as c_int {
                let mut r#match: bool = false; /* assume mismatch */
                let item: *mut MCVItem = (*mcvlist).items.as_mut_ptr().offset(i as isize);

                /* if the clause mismatches the MCV item, update the bitmap */
                match (*expr).nulltesttype {
                    NullTestType::IS_NULL => {
                        r#match = if *(*item).isnull.offset(idx as isize) {
                            true
                        } else {
                            r#match
                        };
                    }
                    NullTestType::IS_NOT_NULL => {
                        r#match = if !*(*item).isnull.offset(idx as isize) {
                            true
                        } else {
                            r#match
                        };
                    }
                }

                /* now, update the match bitmap, depending on OR/AND type */
                *matches.offset(i as isize) =
                    RESULT_MERGE(*matches.offset(i as isize), is_or, r#match);
                i += 1;
            }
        } else if is_orclause(clause as *const c_void) || is_andclause(clause as *const c_void) {
            /* AND/OR clause, with all subclauses being compatible */

            let mut i: c_int;
            let bool_clause: *mut BoolExpr = clause as *mut BoolExpr;
            let bool_clauses: *mut List = (*bool_clause).args;

            /* match/mismatch bitmap for each MCV item */
            let bool_matches: *mut bool;

            Assert!(bool_clauses != NIL);
            Assert!(list_length(bool_clauses) >= 2);

            /* build the match bitmap for the OR-clauses */
            bool_matches = mcv_get_match_bitmap(
                root,
                bool_clauses,
                keys,
                exprs,
                mcvlist,
                is_orclause(clause as *const c_void),
            );

            /*
             * Merge the bitmap produced by mcv_get_match_bitmap into the
             * current one. We need to consider if we're evaluating AND or OR
             * condition when merging the results.
             */
            i = 0;
            while i < (*mcvlist).nitems as c_int {
                *matches.offset(i as isize) = RESULT_MERGE(
                    *matches.offset(i as isize),
                    is_or,
                    *bool_matches.offset(i as isize),
                );
                i += 1;
            }

            pfree(bool_matches as *mut c_void);
        } else if is_notclause(clause as *const c_void) {
            /* NOT clause, with all subclauses compatible */

            let mut i: c_int;
            let not_clause: *mut BoolExpr = clause as *mut BoolExpr;
            let not_args: *mut List = (*not_clause).args;

            /* match/mismatch bitmap for each MCV item */
            let not_matches: *mut bool;

            Assert!(not_args != NIL);
            Assert!(list_length(not_args) == 1);

            /* build the match bitmap for the NOT-clause */
            not_matches = mcv_get_match_bitmap(root, not_args, keys, exprs, mcvlist, false);

            /*
             * Merge the bitmap produced by mcv_get_match_bitmap into the
             * current one. We're handling a NOT clause, so invert the result
             * before merging it into the global bitmap.
             */
            i = 0;
            while i < (*mcvlist).nitems as c_int {
                *matches.offset(i as isize) = RESULT_MERGE(
                    *matches.offset(i as isize),
                    is_or,
                    !*not_matches.offset(i as isize),
                );
                i += 1;
            }

            pfree(not_matches as *mut c_void);
        } else if IsA!(clause, Var) {
            /* Var (has to be a boolean Var, possibly from below NOT) */

            let var: *mut Var = clause as *mut Var;

            /* match the attribute to a dimension of the statistic */
            let idx: c_int = bms_member_index(keys, (*var).varattno as c_int);

            Assert!((*var).vartype == BOOLOID);

            /*
             * Walk through the MCV items and evaluate the current clause. We
             * can skip items that were already ruled out, and terminate if
             * there are no remaining MCV items that might possibly match.
             */
            let mut i: c_int = 0;
            while i < (*mcvlist).nitems as c_int {
                let item: *mut MCVItem = (*mcvlist).items.as_mut_ptr().offset(i as isize);
                let mut r#match: bool = false;

                /* if the item is NULL, it's a mismatch */
                if !*(*item).isnull.offset(idx as isize)
                    && DatumGetBool(*(*item).values.offset(idx as isize))
                {
                    r#match = true;
                }

                /* update the result bitmap */
                *matches.offset(i as isize) =
                    RESULT_MERGE(*matches.offset(i as isize), is_or, r#match);
                i += 1;
            }
        } else {
            /* Otherwise, it must be a bare boolean-returning expression */
            let idx: c_int;

            /* match the expression to a dimension of the statistic */
            idx = mcv_match_expression(clause, keys, exprs, core::ptr::null_mut());

            /*
             * Walk through the MCV items and evaluate the current clause. We
             * can skip items that were already ruled out, and terminate if
             * there are no remaining MCV items that might possibly match.
             */
            let mut i: c_int = 0;
            while i < (*mcvlist).nitems as c_int {
                let r#match: bool;
                let item: *mut MCVItem = (*mcvlist).items.as_mut_ptr().offset(i as isize);

                /* "match" just means it's bool TRUE */
                r#match = !*(*item).isnull.offset(idx as isize)
                    && DatumGetBool(*(*item).values.offset(idx as isize));

                /* now, update the match bitmap, depending on OR/AND type */
                *matches.offset(i as isize) =
                    RESULT_MERGE(*matches.offset(i as isize), is_or, r#match);
                i += 1;
            }
        }
    });

    matches
}

/*
 * mcv_combine_selectivities
 * 		Combine per-column and multi-column MCV selectivity estimates.
 *
 * simple_sel is a "simple" selectivity estimate (produced without using any
 * extended statistics, essentially assuming independence of columns/clauses).
 *
 * mcv_sel and mcv_basesel are sums of the frequencies and base frequencies of
 * all matching MCV items.  The difference (mcv_sel - mcv_basesel) is then
 * essentially interpreted as a correction to be added to simple_sel, as
 * described below.
 *
 * mcv_totalsel is the sum of the frequencies of all MCV items (not just the
 * matching ones).  This is used as an upper bound on the portion of the
 * selectivity estimates not covered by the MCV statistics.
 *
 * Note: While simple and base selectivities are defined in a quite similar
 * way, the values are computed differently and are not therefore equal. The
 * simple selectivity is computed as a product of per-clause estimates, while
 * the base selectivity is computed by adding up base frequencies of matching
 * items of the multi-column MCV list. So the values may differ for two main
 * reasons - (a) the MCV list may not cover 100% of the data and (b) some of
 * the MCV items did not match the estimated clauses.
 *
 * As both (a) and (b) reduce the base selectivity value, it generally holds
 * that (simple_sel >= mcv_basesel). If the MCV list covers all the data, the
 * values may be equal.
 *
 * So, other_sel = (simple_sel - mcv_basesel) is an estimate for the part not
 * covered by the MCV list, and (mcv_sel - mcv_basesel) may be seen as a
 * correction for the part covered by the MCV list. Those two statements are
 * actually equivalent.
 */
pub unsafe fn mcv_combine_selectivities(
    simple_sel: Selectivity,
    mcv_sel: Selectivity,
    mcv_basesel: Selectivity,
    mcv_totalsel: Selectivity,
) -> Selectivity {
    let mut other_sel: Selectivity;
    let mut sel: Selectivity;

    /* estimated selectivity of values not covered by MCV matches */
    other_sel = simple_sel - mcv_basesel;
    CLAMP_PROBABILITY!(other_sel);

    /* this non-MCV selectivity cannot exceed 1 - mcv_totalsel */
    if other_sel > 1.0 - mcv_totalsel {
        other_sel = 1.0 - mcv_totalsel;
    }

    /* overall selectivity is the sum of the MCV and non-MCV parts */
    sel = mcv_sel + other_sel;
    CLAMP_PROBABILITY!(sel);

    sel
}

/*
 * mcv_clauselist_selectivity
 *		Use MCV statistics to estimate the selectivity of an implicitly-ANDed
 *		list of clauses.
 *
 * This determines which MCV items match every clause in the list and returns
 * the sum of the frequencies of those items.
 *
 * In addition, it returns the sum of the base frequencies of each of those
 * items (that is the sum of the selectivities that each item would have if
 * the columns were independent of one another), and the total selectivity of
 * all the MCV items (not just the matching ones).  These are expected to be
 * used together with a "simple" selectivity estimate (one based only on
 * per-column statistics) to produce an overall selectivity estimate that
 * makes use of both per-column and multi-column statistics --- see
 * mcv_combine_selectivities().
 */
pub unsafe fn mcv_clauselist_selectivity(
    root: *mut PlannerInfo,
    stat: *mut StatisticExtInfo,
    clauses: *mut List,
    _varRelid: c_int,
    _jointype: JoinType,
    _sjinfo: *mut SpecialJoinInfo,
    rel: *mut RelOptInfo,
    basesel: *mut Selectivity,
    totalsel: *mut Selectivity,
) -> Selectivity {
    let mut i: c_int;
    let mcv: *mut MCVList;
    let mut s: Selectivity = 0.0;
    let rte: *mut RangeTblEntry = *(*root).simple_rte_array.offset((*rel).relid as isize);

    /* match/mismatch bitmap for each MCV item */
    let matches: *mut bool;

    /* load the MCV list stored in the statistics object */
    mcv = statext_mcv_load((*stat).statOid, (*rte).inh);

    /* build a match bitmap for the clauses */
    matches = mcv_get_match_bitmap(root, clauses, (*stat).keys, (*stat).exprs, mcv, false);

    /* sum frequencies for all the matching MCV items */
    *basesel = 0.0;
    *totalsel = 0.0;
    i = 0;
    while i < (*mcv).nitems as c_int {
        *totalsel += (*(*mcv).items.as_ptr().offset(i as isize)).frequency;

        if *matches.offset(i as isize) != false {
            *basesel += (*(*mcv).items.as_ptr().offset(i as isize)).base_frequency;
            s += (*(*mcv).items.as_ptr().offset(i as isize)).frequency;
        }
        i += 1;
    }

    s
}

/*
 * mcv_clause_selectivity_or
 *		Use MCV statistics to estimate the selectivity of a clause that
 *		appears in an ORed list of clauses.
 *
 * As with mcv_clauselist_selectivity() this determines which MCV items match
 * the clause and returns both the sum of the frequencies and the sum of the
 * base frequencies of those items, as well as the sum of the frequencies of
 * all MCV items (not just the matching ones) so that this information can be
 * used by mcv_combine_selectivities() to produce a selectivity estimate that
 * makes use of both per-column and multi-column statistics.
 *
 * Additionally, we return information to help compute the overall selectivity
 * of the ORed list of clauses assumed to contain this clause.  This function
 * is intended to be called for each clause in the ORed list of clauses,
 * allowing the overall selectivity to be computed using the following
 * algorithm:
 *
 * Suppose P[n] = P(C[1] OR C[2] OR ... OR C[n]) is the combined selectivity
 * of the first n clauses in the list.  Then the combined selectivity taking
 * into account the next clause C[n+1] can be written as
 *
 *		P[n+1] = P[n] + P(C[n+1]) - P((C[1] OR ... OR C[n]) AND C[n+1])
 *
 * The final term above represents the overlap between the clauses examined so
 * far and the (n+1)'th clause.  To estimate its selectivity, we track the
 * match bitmap for the ORed list of clauses examined so far and examine its
 * intersection with the match bitmap for the (n+1)'th clause.
 *
 * We then also return the sums of the MCV item frequencies and base
 * frequencies for the match bitmap intersection corresponding to the overlap
 * term above, so that they can be combined with a simple selectivity estimate
 * for that term.
 *
 * The parameter "or_matches" is an in/out parameter tracking the match bitmap
 * for the clauses examined so far.  The caller is expected to set it to NULL
 * the first time it calls this function.
 */
pub unsafe fn mcv_clause_selectivity_or(
    root: *mut PlannerInfo,
    stat: *mut StatisticExtInfo,
    mcv: *mut MCVList,
    clause: *mut Node,
    or_matches: *mut *mut bool,
    basesel: *mut Selectivity,
    overlap_mcvsel: *mut Selectivity,
    overlap_basesel: *mut Selectivity,
    totalsel: *mut Selectivity,
) -> Selectivity {
    let mut s: Selectivity = 0.0;
    let new_matches: *mut bool;
    let mut i: c_int;

    /* build the OR-matches bitmap, if not built already */
    if (*or_matches).is_null() {
        *or_matches = palloc0(core::mem::size_of::<bool>() * (*mcv).nitems as usize) as *mut bool;
    }

    /* build the match bitmap for the new clause */
    new_matches = mcv_get_match_bitmap(
        root,
        list_make1!(clause as *mut c_void),
        (*stat).keys,
        (*stat).exprs,
        mcv,
        false,
    );

    /*
     * Sum the frequencies for all the MCV items matching this clause and also
     * those matching the overlap between this clause and any of the preceding
     * clauses as described above.
     */
    *basesel = 0.0;
    *overlap_mcvsel = 0.0;
    *overlap_basesel = 0.0;
    *totalsel = 0.0;
    i = 0;
    while i < (*mcv).nitems as c_int {
        *totalsel += (*(*mcv).items.as_ptr().offset(i as isize)).frequency;

        if *new_matches.offset(i as isize) {
            s += (*(*mcv).items.as_ptr().offset(i as isize)).frequency;
            *basesel += (*(*mcv).items.as_ptr().offset(i as isize)).base_frequency;

            if *(*or_matches).offset(i as isize) {
                *overlap_mcvsel += (*(*mcv).items.as_ptr().offset(i as isize)).frequency;
                *overlap_basesel += (*(*mcv).items.as_ptr().offset(i as isize)).base_frequency;
            }
        }

        /* update the OR-matches bitmap for the next clause */
        *(*or_matches).offset(i as isize) =
            *(*or_matches).offset(i as isize) || *new_matches.offset(i as isize);
        i += 1;
    }

    pfree(new_matches as *mut c_void);

    s
}
