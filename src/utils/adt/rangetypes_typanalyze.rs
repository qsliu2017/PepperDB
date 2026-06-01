//! src/backend/utils/adt/rangetypes_typanalyze.c
//!
//! Functions for gathering statistics from range columns
//!
//! For a range type column, histograms of lower and upper bounds, and
//! the fraction of NULL and empty ranges are collected.
//!
//! Both histograms have the same length, and they are combined into a
//! single array of ranges. This has the same shape as the histogram that
//! std_typanalyze would collect, but the values are different. Each range
//! in the array is a valid range, even though the lower and upper bounds
//! come from different tuples. In theory, the standard scalar selectivity
//! functions could be used with the combined histogram.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/rangetypes_typanalyze.c

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::utils::fmgr::{FunctionCall2Coll, FunctionCallInfo};
use crate::utils::sort::qsort_interruptible::qsort_interruptible;
use crate::utils::adt::float::get_float8_infinity;
use crate::varatt::VARSIZE_ANY;
use crate::catalog::pg_statistic::{
    STATISTIC_KIND_BOUNDS_HISTOGRAM, STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
};
use crate::catalog::pg_type::{TYPTYPE_MULTIRANGE, TYPTYPE_RANGE};
use crate::catalog::pg_type_d::FLOAT8OID;
use crate::catalog::pg_known_oids::Float8LessOperator;
use crate::{PG_GETARG_POINTER, PG_RETURN_BOOL};

// ---------------------------------------------------------------------------
// Local stubs for values not yet ported.
// ---------------------------------------------------------------------------

// GUC default_statistics_target (commands/analyze.c / utils/misc/guc_tables.c).
static mut default_statistics_target: c_int = 100; // TODO: utils/misc/guc.c

// ---------------------------------------------------------------------------
// Local stub types for dependencies not yet ported.
// ---------------------------------------------------------------------------

type VacAttrStats = c_void; // TODO: commands/vacuum.h
type AnalyzeAttrFetchFunc =
    unsafe extern "C" fn(stats: *mut VacAttrStats, rownum: c_int, isNull: *mut bool) -> Datum;
type TypeCacheEntry = c_void; // TODO: utils/typcache.h
type RangeType = c_void; // TODO: utils/rangetypes.h
type MultirangeType = c_void; // TODO: utils/multirangetypes.h
type FmgrInfo = c_void; // TODO: fmgr.h

#[repr(C)]
pub struct RangeBound {
    pub val: Datum,
    pub lbound: bool,
    pub inclusive: bool,
    pub infinite: bool,
} // TODO: utils/rangetypes.h

/*
 * Comparison function for sorting float8s, used for range lengths.
 */
unsafe extern "C" fn float8_qsort_cmp(
    a1: *const c_void,
    a2: *const c_void,
    _arg: *mut c_void,
) -> c_int {
    let f1 = a1 as *const float8;
    let f2 = a2 as *const float8;

    if *f1 < *f2 {
        -1
    } else if *f1 == *f2 {
        0
    } else {
        1
    }
}

/*
 * Comparison function for sorting RangeBounds.
 */
unsafe extern "C" fn range_bound_qsort_cmp(
    a1: *const c_void,
    a2: *const c_void,
    arg: *mut c_void,
) -> c_int {
    let b1 = a1 as *mut RangeBound;
    let b2 = a2 as *mut RangeBound;
    let typcache = arg as *mut TypeCacheEntry;

    range_cmp_bounds(typcache, b1, b2)
}

/*
 * range_typanalyze -- typanalyze function for range columns
 */
#[no_mangle]
pub unsafe extern "C" fn range_typanalyze(fcinfo: FunctionCallInfo) -> Datum {
    let stats = PG_GETARG_POINTER!(fcinfo, 0) as *mut VacAttrStats;
    let typcache: *mut TypeCacheEntry;

    /* Get information about range type; note column might be a domain */
    typcache = range_get_typcache(fcinfo, getBaseType((*stats).attrtypid));

    if (*stats).attstattarget < 0 {
        (*stats).attstattarget = default_statistics_target;
    }

    (*stats).compute_stats = compute_range_stats as *mut c_void;
    (*stats).extra_data = typcache as *mut c_void;
    /* same as in std_typanalyze */
    (*stats).minrows = 300 * (*stats).attstattarget;

    PG_RETURN_BOOL!(true)
}

/*
 * multirange_typanalyze -- typanalyze function for multirange columns
 *
 * We do the same analysis as for ranges, but on the smallest range that
 * completely includes the multirange.
 */
#[no_mangle]
pub unsafe extern "C" fn multirange_typanalyze(fcinfo: FunctionCallInfo) -> Datum {
    let stats = PG_GETARG_POINTER!(fcinfo, 0) as *mut VacAttrStats;
    let typcache: *mut TypeCacheEntry;

    /* Get information about multirange type; note column might be a domain */
    typcache = multirange_get_typcache(fcinfo, getBaseType((*stats).attrtypid));

    if (*stats).attstattarget < 0 {
        (*stats).attstattarget = default_statistics_target;
    }

    (*stats).compute_stats = compute_range_stats as *mut c_void;
    (*stats).extra_data = typcache as *mut c_void;
    /* same as in std_typanalyze */
    (*stats).minrows = 300 * (*stats).attstattarget;

    PG_RETURN_BOOL!(true)
}

/*
 * compute_range_stats() -- compute statistics for a range column
 */
unsafe extern "C" fn compute_range_stats(
    stats: *mut VacAttrStats,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: c_int,
    _totalrows: f64,
) {
    let mut typcache = (*stats).extra_data as *mut TypeCacheEntry;
    let mut mltrng_typcache: *mut TypeCacheEntry = std::ptr::null_mut();
    let has_subdiff: bool;
    let mut null_cnt: c_int = 0;
    let mut non_null_cnt: c_int = 0;
    let mut non_empty_cnt: c_int = 0;
    let mut empty_cnt: c_int = 0;
    let mut range_no: c_int;
    let mut slot_idx: c_int;
    let num_bins: c_int = (*stats).attstattarget;
    let mut num_hist: c_int;
    let lengths: *mut float8;
    let lowers: *mut RangeBound;
    let uppers: *mut RangeBound;
    let mut total_width: f64 = 0.0;

    if (*typcache).typtype == TYPTYPE_MULTIRANGE {
        mltrng_typcache = typcache;
        typcache = (*typcache).rngtype;
    } else {
        Assert((*typcache).typtype == TYPTYPE_RANGE);
    }
    has_subdiff = OidIsValid((*typcache).rng_subdiff_finfo.fn_oid);

    /* Allocate memory to hold range bounds and lengths of the sample ranges. */
    lowers = palloc(std::mem::size_of::<RangeBound>() * samplerows as usize) as *mut RangeBound;
    uppers = palloc(std::mem::size_of::<RangeBound>() * samplerows as usize) as *mut RangeBound;
    lengths = palloc(std::mem::size_of::<float8>() * samplerows as usize) as *mut float8;

    /* Loop over the sample ranges. */
    range_no = 0;
    while range_no < samplerows {
        let value: Datum;
        let mut isnull: bool = false;
        let mut empty: bool = false;
        let multirange: *mut MultirangeType;
        let range: *mut RangeType;
        let mut lower: RangeBound = std::mem::zeroed();
        let mut upper: RangeBound = std::mem::zeroed();
        let length: float8;

        vacuum_delay_point(true);

        value = fetchfunc(stats, range_no, &mut isnull);
        if isnull {
            /* range is null, just count that */
            null_cnt += 1;
            range_no += 1;
            continue;
        }

        /*
         * XXX: should we ignore wide values, like std_typanalyze does, to
         * avoid bloating the statistics table?
         */
        total_width += VARSIZE_ANY(DatumGetPointer(value)) as f64;

        /* Get range and deserialize it for further analysis. */
        if !mltrng_typcache.is_null() {
            /* Treat multiranges like a big range without gaps. */
            multirange = DatumGetMultirangeTypeP(value);
            if !MultirangeIsEmpty(multirange) {
                let mut tmp: RangeBound = std::mem::zeroed();

                multirange_get_bounds(typcache, multirange, 0, &mut lower, &mut tmp);
                multirange_get_bounds(
                    typcache,
                    multirange,
                    (*multirange).rangeCount - 1,
                    &mut tmp,
                    &mut upper,
                );
                empty = false;
            } else {
                empty = true;
            }
        } else {
            range = DatumGetRangeTypeP(value);
            range_deserialize(typcache, range, &mut lower, &mut upper, &mut empty);
        }

        if !empty {
            /* Remember bounds and length for further usage in histograms */
            *lowers.offset(non_empty_cnt as isize) = lower;
            *uppers.offset(non_empty_cnt as isize) = upper;

            if lower.infinite || upper.infinite {
                /* Length of any kind of an infinite range is infinite */
                length = get_float8_infinity();
            } else if has_subdiff {
                /*
                 * For an ordinary range, use subdiff function between upper
                 * and lower bound values.
                 */
                length = DatumGetFloat8(FunctionCall2Coll(
                    &mut (*typcache).rng_subdiff_finfo,
                    (*typcache).rng_collation,
                    upper.val,
                    lower.val,
                ));
            } else {
                /* Use default value of 1.0 if no subdiff is available. */
                length = 1.0;
            }
            *lengths.offset(non_empty_cnt as isize) = length;

            non_empty_cnt += 1;
        } else {
            empty_cnt += 1;
        }

        non_null_cnt += 1;
        range_no += 1;
    }

    slot_idx = 0;

    /* We can only compute real stats if we found some non-null values. */
    if non_null_cnt > 0 {
        let bound_hist_values: *mut Datum;
        let mut length_hist_values: *mut Datum;
        let mut pos: c_int;
        let mut posfrac: c_int;
        let mut delta: c_int;
        let mut deltafrac: c_int;
        let mut i: c_int;
        let old_cxt: MemoryContext;
        let emptyfrac: *mut f32;

        (*stats).stats_valid = true;
        /* Do the simple null-frac and width stats */
        (*stats).stanullfrac = null_cnt as f64 / samplerows as f64;
        (*stats).stawidth = (total_width / non_null_cnt as f64) as c_int;

        /* Estimate that non-null values are unique */
        (*stats).stadistinct = -1.0 * (1.0 - (*stats).stanullfrac);

        /* Must copy the target values into anl_context */
        old_cxt = MemoryContextSwitchTo((*stats).anl_context);

        /*
         * Generate a bounds histogram slot entry if there are at least two
         * values.
         */
        if non_empty_cnt >= 2 {
            /* Sort bound values */
            qsort_interruptible(
                lowers as *mut c_void,
                non_empty_cnt as Size,
                std::mem::size_of::<RangeBound>() as Size,
                range_bound_qsort_cmp,
                typcache as *mut c_void,
            );
            qsort_interruptible(
                uppers as *mut c_void,
                non_empty_cnt as Size,
                std::mem::size_of::<RangeBound>() as Size,
                range_bound_qsort_cmp,
                typcache as *mut c_void,
            );

            num_hist = non_empty_cnt;
            if num_hist > num_bins {
                num_hist = num_bins + 1;
            }

            bound_hist_values =
                palloc(num_hist as usize * std::mem::size_of::<Datum>()) as *mut Datum;

            /*
             * The object of this loop is to construct ranges from first and
             * last entries in lowers[] and uppers[] along with evenly-spaced
             * values in between. So the i'th value is a range of lowers[(i *
             * (nvals - 1)) / (num_hist - 1)] and uppers[(i * (nvals - 1)) /
             * (num_hist - 1)]. But computing that subscript directly risks
             * integer overflow when the stats target is more than a couple
             * thousand.  Instead we add (nvals - 1) / (num_hist - 1) to pos
             * at each step, tracking the integral and fractional parts of the
             * sum separately.
             */
            delta = (non_empty_cnt - 1) / (num_hist - 1);
            deltafrac = (non_empty_cnt - 1) % (num_hist - 1);
            pos = 0;
            posfrac = 0;

            i = 0;
            while i < num_hist {
                *bound_hist_values.offset(i as isize) = PointerGetDatum(range_serialize(
                    typcache,
                    lowers.offset(pos as isize),
                    uppers.offset(pos as isize),
                    false,
                    std::ptr::null_mut(),
                ) as *const c_void);
                pos += delta;
                posfrac += deltafrac;
                if posfrac >= (num_hist - 1) {
                    /* fractional part exceeds 1, carry to integer part */
                    pos += 1;
                    posfrac -= num_hist - 1;
                }
                i += 1;
            }

            (*stats).stakind[slot_idx as usize] = STATISTIC_KIND_BOUNDS_HISTOGRAM as i16;
            (*stats).stavalues[slot_idx as usize] = bound_hist_values;
            (*stats).numvalues[slot_idx as usize] = num_hist;

            /* Store ranges even if we're analyzing a multirange column */
            (*stats).statypid[slot_idx as usize] = (*typcache).type_id;
            (*stats).statyplen[slot_idx as usize] = (*typcache).typlen;
            (*stats).statypbyval[slot_idx as usize] = (*typcache).typbyval;
            (*stats).statypalign[slot_idx as usize] = (*typcache).typalign;

            slot_idx += 1;
        }

        /*
         * Generate a length histogram slot entry if there are at least two
         * values.
         */
        if non_empty_cnt >= 2 {
            /*
             * Ascending sort of range lengths for further filling of
             * histogram
             */
            qsort_interruptible(
                lengths as *mut c_void,
                non_empty_cnt as Size,
                std::mem::size_of::<float8>() as Size,
                float8_qsort_cmp,
                std::ptr::null_mut(),
            );

            num_hist = non_empty_cnt;
            if num_hist > num_bins {
                num_hist = num_bins + 1;
            }

            length_hist_values =
                palloc(num_hist as usize * std::mem::size_of::<Datum>()) as *mut Datum;

            /*
             * The object of this loop is to copy the first and last lengths[]
             * entries along with evenly-spaced values in between. So the i'th
             * value is lengths[(i * (nvals - 1)) / (num_hist - 1)]. But
             * computing that subscript directly risks integer overflow when
             * the stats target is more than a couple thousand.  Instead we
             * add (nvals - 1) / (num_hist - 1) to pos at each step, tracking
             * the integral and fractional parts of the sum separately.
             */
            delta = (non_empty_cnt - 1) / (num_hist - 1);
            deltafrac = (non_empty_cnt - 1) % (num_hist - 1);
            pos = 0;
            posfrac = 0;

            i = 0;
            while i < num_hist {
                *length_hist_values.offset(i as isize) =
                    Float8GetDatum(*lengths.offset(pos as isize));
                pos += delta;
                posfrac += deltafrac;
                if posfrac >= (num_hist - 1) {
                    /* fractional part exceeds 1, carry to integer part */
                    pos += 1;
                    posfrac -= num_hist - 1;
                }
                i += 1;
            }
        } else {
            /*
             * Even when we don't create the histogram, store an empty array
             * to mean "no histogram". We can't just leave stavalues NULL,
             * because get_attstatsslot() errors if you ask for stavalues, and
             * it's NULL. We'll still store the empty fraction in stanumbers.
             */
            length_hist_values = palloc(0) as *mut Datum;
            num_hist = 0;
        }
        (*stats).staop[slot_idx as usize] = Float8LessOperator;
        (*stats).stacoll[slot_idx as usize] = InvalidOid;
        (*stats).stavalues[slot_idx as usize] = length_hist_values;
        (*stats).numvalues[slot_idx as usize] = num_hist;
        (*stats).statypid[slot_idx as usize] = FLOAT8OID;
        (*stats).statyplen[slot_idx as usize] = std::mem::size_of::<float8>() as i16;
        (*stats).statypbyval[slot_idx as usize] = FLOAT8PASSBYVAL;
        (*stats).statypalign[slot_idx as usize] = b'd' as c_char;

        /* Store the fraction of empty ranges */
        emptyfrac = palloc(std::mem::size_of::<f32>()) as *mut f32;
        *emptyfrac = (empty_cnt as f64 / non_null_cnt as f64) as f32;
        (*stats).stanumbers[slot_idx as usize] = emptyfrac;
        (*stats).numnumbers[slot_idx as usize] = 1;

        (*stats).stakind[slot_idx as usize] = STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM as i16;
        slot_idx += 1;
        let _ = slot_idx;

        MemoryContextSwitchTo(old_cxt);
    } else if null_cnt > 0 {
        /* We found only nulls; assume the column is entirely null */
        (*stats).stats_valid = true;
        (*stats).stanullfrac = 1.0;
        (*stats).stawidth = 0; /* "unknown" */
        (*stats).stadistinct = 0.0; /* "unknown" */
    }

    /*
     * We don't need to bother cleaning up any of our temporary palloc's. The
     * hashtable should also go away, as it used a child memory context.
     */
}

// ---------------------------------------------------------------------------
// Local stubs for unported helper functions.
// ---------------------------------------------------------------------------

unsafe fn range_cmp_bounds(
    _typcache: *mut TypeCacheEntry,
    _b1: *mut RangeBound,
    _b2: *mut RangeBound,
) -> c_int {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_get_typcache(
    _fcinfo: FunctionCallInfo,
    _rngtypid: Oid,
) -> *mut TypeCacheEntry {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn multirange_get_typcache(
    _fcinfo: FunctionCallInfo,
    _mltrngtypid: Oid,
) -> *mut TypeCacheEntry {
    unimplemented!() // TODO: utils/adt/multirangetypes.c
}

unsafe fn getBaseType(_typid: Oid) -> Oid {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}

unsafe fn vacuum_delay_point(_is_analyze: bool) {
    unimplemented!() // TODO: commands/vacuum.c
}

unsafe fn DatumGetMultirangeTypeP(_d: Datum) -> *mut MultirangeType {
    unimplemented!() // TODO: utils/adt/multirangetypes.c
}

unsafe fn MultirangeIsEmpty(_mr: *mut MultirangeType) -> bool {
    unimplemented!() // TODO: utils/multirangetypes.h
}

unsafe fn multirange_get_bounds(
    _typcache: *mut TypeCacheEntry,
    _mr: *mut MultirangeType,
    _i: u32,
    _lower: *mut RangeBound,
    _upper: *mut RangeBound,
) {
    unimplemented!() // TODO: utils/adt/multirangetypes.c
}

unsafe fn DatumGetRangeTypeP(_d: Datum) -> *mut RangeType {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_deserialize(
    _typcache: *mut TypeCacheEntry,
    _range: *mut RangeType,
    _lower: *mut RangeBound,
    _upper: *mut RangeBound,
    _empty: *mut bool,
) {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_serialize(
    _typcache: *mut TypeCacheEntry,
    _lower: *mut RangeBound,
    _upper: *mut RangeBound,
    _empty: bool,
    _escontext: *mut c_void,
) -> *mut RangeType {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

