//! gistproc.rs
//!   Support procedures for GiSTs over 2-D objects (boxes, polygons, circles,
//!   points).
//!
//! This gives R-tree behavior, with Guttman's poly-time split algorithm.
//!
//! Translated 1:1 from postgres/src/backend/access/gist/gistproc.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!			src/backend/access/gist/gistproc.c

use crate::prelude::*;
use crate::{
    DirectFunctionCall2, DirectFunctionCall5, PG_GETARG_BOX_P, PG_GETARG_CIRCLE_P,
    PG_GETARG_DATUM, PG_GETARG_POINT_P, PG_GETARG_POINTER, PG_GETARG_POLYGON_P, PG_GETARG_UINT16,
    PG_RETURN_BOOL, PG_RETURN_FLOAT8, PG_RETURN_POINTER, PG_RETURN_VOID,
};

use crate::access::gist::gist_private::{GISTENTRY, GIST_SPLITVEC};
use crate::access::gist::gistutil::GistEntryVector;
use crate::storage::bufpage::{Page, PageGetSpecialPointer};
use crate::utils::rel::Relation;
use crate::access::stratnum::{
    StrategyNumber, RTAboveStrategyNumber, RTBelowStrategyNumber, RTContainedByStrategyNumber,
    RTContainsStrategyNumber, RTLeftStrategyNumber, RTOldAboveStrategyNumber,
    RTOldBelowStrategyNumber, RTOverAboveStrategyNumber, RTOverBelowStrategyNumber,
    RTOverLeftStrategyNumber, RTOverRightStrategyNumber, RTOverlapStrategyNumber,
    RTRightStrategyNumber, RTSameStrategyNumber,
};
use crate::port::qsort::pg_qsort;
use crate::postgres::{
    DatumGetBool, DatumGetFloat8, Int16GetDatum, PointerGetDatum,
};
use crate::storage::off::{FirstOffsetNumber, OffsetNumber, OffsetNumberNext};
use crate::utils::adt::float::{
    float4_div, float8_cmp_internal, float8_div, float8_eq, float8_ge, float8_gt, float8_le,
    float8_lt, float8_max, float8_mi, float8_min, float8_mul, float8_pl, get_float8_infinity,
};
use crate::utils::geo_decls::{
    BoxPGetDatum, CirclePGetDatum, DatumGetBoxP, DatumGetCircleP, DatumGetPointP,
    DatumGetPolygonP, FPeq, FPge, FPgt, FPle, FPlt, PointPGetDatum, PolygonPGetDatum, BOX, Point,
};
use crate::utils::adt::geo_ops::{
    box_above, box_below, box_contain, box_contained, box_left, box_overabove, box_overbelow,
    box_overlap, box_overleft, box_overright, box_right, box_same, circle_contain_pt,
    point_distance, poly_contain_pt,
};
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::sort::sortsupport::SortSupport;
use crate::utils::sort::tuplesort::ssup_datum_unsigned_cmp;

use core::ffi::c_int;

// ===========================================================================
// access/gist.h stub.  GIST_LEAF(entry) consults the leaf flag of the page the
// entry lives on; gistutil.rs keeps GistPageIsLeaf private, so re-derive here.
// TODO(pg-port): dedup once access/gist.h is ported.
// ===========================================================================

/* #define GIST_LEAF(entry) (GistPageIsLeaf((entry)->page)) */
#[inline]
unsafe fn GIST_LEAF(entry: *const GISTENTRY) -> bool {
    // GistPageIsLeaf(page) == (GistPageGetOpaque(page)->flags & F_LEAF)
    const F_LEAF: u16 = 1 << 0;
    let page = (*entry).page;
    let opaque =
        PageGetSpecialPointer(page) as *const crate::access::gist::gistutil::GISTPageOpaqueData;
    ((*opaque).flags & F_LEAF) != 0
}

/* #define gistentryinit(e, k, r, pg, o, l) ... -- initialize a GISTENTRY */
/* (the gistutil.rs copy is private; redefine locally) */
#[inline]
unsafe fn gistentryinit(
    e: *mut GISTENTRY,
    k: Datum,
    r: Relation,
    pg: Page,
    o: OffsetNumber,
    l: bool,
) {
    (*e).key = k;
    (*e).rel = r;
    (*e).page = pg;
    (*e).offset = o;
    (*e).leafkey = l;
}

// ssup_datum_unsigned_cmp lives in utils/sort/tuplesort.rs in C; re-exported via
// sortsupport above.

/* Minimum accepted ratio of split */
const LIMIT_RATIO: f64 = 0.3;

/**************************************************
 * Box ops
 **************************************************/

/*
 * Calculates union of two boxes, a and b. The result is stored in *n.
 */
unsafe fn rt_box_union(n: *mut BOX, a: *const BOX, b: *const BOX) {
    (*n).high.x = float8_max((*a).high.x, (*b).high.x);
    (*n).high.y = float8_max((*a).high.y, (*b).high.y);
    (*n).low.x = float8_min((*a).low.x, (*b).low.x);
    (*n).low.y = float8_min((*a).low.y, (*b).low.y);
}

/*
 * Size of a BOX for penalty-calculation purposes.
 * The result can be +Infinity, but not NaN.
 */
unsafe fn size_box(r#box: *const BOX) -> float8 {
    /*
     * Check for zero-width cases.  Note that we define the size of a zero-
     * by-infinity box as zero.  It's important to special-case this somehow,
     * as naively multiplying infinity by zero will produce NaN.
     *
     * The less-than cases should not happen, but if they do, say "zero".
     */
    if float8_le((*r#box).high.x, (*r#box).low.x) || float8_le((*r#box).high.y, (*r#box).low.y) {
        return 0.0;
    }

    /*
     * We treat NaN as larger than +Infinity, so any distance involving a NaN
     * and a non-NaN is infinite.  Note the previous check eliminated the
     * possibility that the low fields are NaNs.
     */
    if (*r#box).high.x.is_nan() || (*r#box).high.y.is_nan() {
        return get_float8_infinity();
    }
    float8_mul(
        float8_mi((*r#box).high.x, (*r#box).low.x),
        float8_mi((*r#box).high.y, (*r#box).low.y),
    )
}

/*
 * Return amount by which the union of the two boxes is larger than
 * the original BOX's area.  The result can be +Infinity, but not NaN.
 */
unsafe fn box_penalty(original: *const BOX, new: *const BOX) -> float8 {
    let mut unionbox: BOX = core::mem::zeroed();

    rt_box_union(&mut unionbox, original, new);
    float8_mi(size_box(&unionbox), size_box(original))
}

/*
 * The GiST Consistent method for boxes
 *
 * Should return false if for all data items x below entry,
 * the predicate x op query must be false, where op is the oper
 * corresponding to strategy in the pg_amop table.
 */
pub unsafe fn gist_box_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let query = PG_GETARG_BOX_P!(fcinfo, 1);
    let strategy = PG_GETARG_UINT16!(fcinfo, 2) as StrategyNumber;

    /* Oid		subtype = PG_GETARG_OID(3); */
    let recheck = PG_GETARG_POINTER!(fcinfo, 4) as *mut bool;

    /* All cases served by this function are exact */
    *recheck = false;

    if DatumGetBoxP((*entry).key).is_null() || query.is_null() {
        PG_RETURN_BOOL!(false);
    }

    /*
     * if entry is not leaf, use rtree_internal_consistent, else use
     * gist_box_leaf_consistent
     */
    if GIST_LEAF(entry) {
        PG_RETURN_BOOL!(gist_box_leaf_consistent(
            DatumGetBoxP((*entry).key),
            query,
            strategy
        ));
    } else {
        PG_RETURN_BOOL!(rtree_internal_consistent(
            DatumGetBoxP((*entry).key),
            query,
            strategy
        ));
    }
}

/*
 * Increase BOX b to include addon.
 */
unsafe fn adjustBox(b: *mut BOX, addon: *const BOX) {
    if float8_lt((*b).high.x, (*addon).high.x) {
        (*b).high.x = (*addon).high.x;
    }
    if float8_gt((*b).low.x, (*addon).low.x) {
        (*b).low.x = (*addon).low.x;
    }
    if float8_lt((*b).high.y, (*addon).high.y) {
        (*b).high.y = (*addon).high.y;
    }
    if float8_gt((*b).low.y, (*addon).low.y) {
        (*b).low.y = (*addon).low.y;
    }
}

/*
 * The GiST Union method for boxes
 *
 * returns the minimal bounding box that encloses all the entries in entryvec
 */
pub unsafe fn gist_box_union(fcinfo: FunctionCallInfo) -> Datum {
    let entryvec = PG_GETARG_POINTER!(fcinfo, 0) as *mut GistEntryVector;
    let sizep = PG_GETARG_POINTER!(fcinfo, 1) as *mut c_int;
    let numranges: c_int;
    let mut i: c_int;
    let mut cur: *mut BOX;
    let pageunion: *mut BOX;

    numranges = (*entryvec).n;
    pageunion = palloc(core::mem::size_of::<BOX>()) as *mut BOX;
    cur = DatumGetBoxP((*(*entryvec).vector.as_ptr().add(0)).key);
    core::ptr::copy_nonoverlapping(cur as *const u8, pageunion as *mut u8, core::mem::size_of::<BOX>());

    i = 1;
    while i < numranges {
        cur = DatumGetBoxP((*(*entryvec).vector.as_ptr().add(i as usize)).key);
        adjustBox(pageunion, cur);
        i += 1;
    }
    *sizep = core::mem::size_of::<BOX>() as c_int;

    PG_RETURN_POINTER!(pageunion);
}

/*
 * We store boxes as boxes in GiST indexes, so we do not need
 * compress, decompress, or fetch functions.
 */

/*
 * The GiST Penalty method for boxes (also used for points)
 *
 * As in the R-tree paper, we use change in area as our penalty metric
 */
pub unsafe fn gist_box_penalty(fcinfo: FunctionCallInfo) -> Datum {
    let origentry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let newentry = PG_GETARG_POINTER!(fcinfo, 1) as *mut GISTENTRY;
    let result = PG_GETARG_POINTER!(fcinfo, 2) as *mut f32;
    let origbox = DatumGetBoxP((*origentry).key);
    let newbox = DatumGetBoxP((*newentry).key);

    *result = box_penalty(origbox, newbox) as f32;
    PG_RETURN_POINTER!(result);
}

/*
 * Trivial split: half of entries will be placed on one page
 * and another half - to another
 */
unsafe fn fallbackSplit(entryvec: *mut GistEntryVector, v: *mut GIST_SPLITVEC) {
    let mut i: OffsetNumber;
    let maxoff: OffsetNumber;
    let mut unionL: *mut BOX = core::ptr::null_mut();
    let mut unionR: *mut BOX = core::ptr::null_mut();
    let nbytes: c_int;

    maxoff = ((*entryvec).n - 1) as OffsetNumber;

    nbytes = (maxoff as c_int + 2) * core::mem::size_of::<OffsetNumber>() as c_int;
    (*v).spl_left = palloc(nbytes as Size) as *mut OffsetNumber;
    (*v).spl_right = palloc(nbytes as Size) as *mut OffsetNumber;
    (*v).spl_nleft = 0;
    (*v).spl_nright = 0;

    i = FirstOffsetNumber;
    while i <= maxoff {
        let cur = DatumGetBoxP((*(*entryvec).vector.as_ptr().add(i as usize)).key);

        if i as c_int <= (maxoff as c_int - FirstOffsetNumber as c_int + 1) / 2 {
            *(*v).spl_left.add((*v).spl_nleft as usize) = i;
            if unionL.is_null() {
                unionL = palloc(core::mem::size_of::<BOX>()) as *mut BOX;
                *unionL = *cur;
            } else {
                adjustBox(unionL, cur);
            }

            (*v).spl_nleft += 1;
        } else {
            *(*v).spl_right.add((*v).spl_nright as usize) = i;
            if unionR.is_null() {
                unionR = palloc(core::mem::size_of::<BOX>()) as *mut BOX;
                *unionR = *cur;
            } else {
                adjustBox(unionR, cur);
            }

            (*v).spl_nright += 1;
        }
        i = OffsetNumberNext(i);
    }

    (*v).spl_ldatum = BoxPGetDatum(unionL);
    (*v).spl_rdatum = BoxPGetDatum(unionR);
}

/*
 * Represents information about an entry that can be placed to either group
 * without affecting overlap over selected axis ("common entry").
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct CommonEntry {
    /* Index of entry in the initial array */
    index: c_int,
    /* Delta between penalties of entry insertion into different groups */
    delta: float8,
}

/*
 * Context for g_box_consider_split. Contains information about currently
 * selected split and some general information.
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct ConsiderSplitContext {
    entriesCount: c_int, /* total number of entries being split */
    boundingBox: BOX,    /* minimum bounding box across all entries */

    /* Information about currently selected split follows */
    first: bool, /* true if no split was selected yet */

    leftUpper: float8,  /* upper bound of left interval */
    rightLower: float8, /* lower bound of right interval */

    ratio: f32,
    overlap: f32,
    dim: c_int,    /* axis of this split */
    range: float8, /* width of general MBR projection to the
                    * selected axis */
}

/*
 * Interval represents projection of box to axis.
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct SplitInterval {
    lower: float8,
    upper: float8,
}

/*
 * Interval comparison function by lower bound of the interval;
 */
unsafe fn interval_cmp_lower(i1: *const c_void, i2: *const c_void) -> c_int {
    let lower1 = (*(i1 as *const SplitInterval)).lower;
    let lower2 = (*(i2 as *const SplitInterval)).lower;

    float8_cmp_internal(lower1, lower2)
}

/*
 * Interval comparison function by upper bound of the interval;
 */
unsafe fn interval_cmp_upper(i1: *const c_void, i2: *const c_void) -> c_int {
    let upper1 = (*(i1 as *const SplitInterval)).upper;
    let upper2 = (*(i2 as *const SplitInterval)).upper;

    float8_cmp_internal(upper1, upper2)
}

/*
 * Replace negative (or NaN) value with zero.
 */
#[inline]
fn non_negative(val: f32) -> f32 {
    if val >= 0.0f32 {
        val
    } else {
        0.0f32
    }
}

/*
 * Consider replacement of currently selected split with the better one.
 */
#[inline]
unsafe fn g_box_consider_split(
    context: *mut ConsiderSplitContext,
    dimNum: c_int,
    rightLower: float8,
    minLeftCount: c_int,
    leftUpper: float8,
    maxLeftCount: c_int,
) {
    let leftCount: c_int;
    let rightCount: c_int;
    let ratio: f32;
    let overlap: f32;
    let mut range: float8 = 0.0;

    /*
     * Calculate entries distribution ratio assuming most uniform distribution
     * of common entries.
     */
    if minLeftCount >= ((*context).entriesCount + 1) / 2 {
        leftCount = minLeftCount;
    } else {
        if maxLeftCount <= (*context).entriesCount / 2 {
            leftCount = maxLeftCount;
        } else {
            leftCount = (*context).entriesCount / 2;
        }
    }
    rightCount = (*context).entriesCount - leftCount;

    /*
     * Ratio of split - quotient between size of lesser group and total
     * entries count.
     */
    ratio = float4_div(Min(leftCount, rightCount) as f32, (*context).entriesCount as f32);

    if ratio as f64 > LIMIT_RATIO {
        let mut selectthis = false;

        /*
         * The ratio is acceptable, so compare current split with previously
         * selected one. Between splits of one dimension we search for minimal
         * overlap (allowing negative values) and minimal ration (between same
         * overlaps. We switch dimension if find less overlap (non-negative)
         * or less range with same overlap.
         */
        if dimNum == 0 {
            range = float8_mi((*context).boundingBox.high.x, (*context).boundingBox.low.x);
        } else {
            range = float8_mi((*context).boundingBox.high.y, (*context).boundingBox.low.y);
        }

        overlap = float8_div(float8_mi(leftUpper, rightLower), range) as f32;

        /* If there is no previous selection, select this */
        if (*context).first {
            selectthis = true;
        } else if (*context).dim == dimNum {
            /*
             * Within the same dimension, choose the new split if it has a
             * smaller overlap, or same overlap but better ratio.
             */
            if overlap < (*context).overlap
                || (overlap == (*context).overlap && ratio > (*context).ratio)
            {
                selectthis = true;
            }
        } else {
            /*
             * Across dimensions, choose the new split if it has a smaller
             * *non-negative* overlap, or same *non-negative* overlap but
             * bigger range. This condition differs from the one described in
             * the article. On the datasets where leaf MBRs don't overlap
             * themselves, non-overlapping splits (i.e. splits which have zero
             * *non-negative* overlap) are frequently possible. In this case
             * splits tends to be along one dimension, because most distant
             * non-overlapping splits (i.e. having lowest negative overlap)
             * appears to be in the same dimension as in the previous split.
             * Therefore MBRs appear to be very prolonged along another
             * dimension, which leads to bad search performance. Using range
             * as the second split criteria makes MBRs more quadratic. Using
             * *non-negative* overlap instead of overlap as the first split
             * criteria gives to range criteria a chance to matter, because
             * non-overlapping splits are equivalent in this criteria.
             */
            if non_negative(overlap) < non_negative((*context).overlap)
                || (range > (*context).range
                    && non_negative(overlap) <= non_negative((*context).overlap))
            {
                selectthis = true;
            }
        }

        if selectthis {
            /* save information about selected split */
            (*context).first = false;
            (*context).ratio = ratio;
            (*context).range = range;
            (*context).overlap = overlap;
            (*context).rightLower = rightLower;
            (*context).leftUpper = leftUpper;
            (*context).dim = dimNum;
        }
    }
}

/*
 * Compare common entries by their deltas.
 */
unsafe fn common_entry_cmp(i1: *const c_void, i2: *const c_void) -> c_int {
    let delta1 = (*(i1 as *const CommonEntry)).delta;
    let delta2 = (*(i2 as *const CommonEntry)).delta;

    float8_cmp_internal(delta1, delta2)
}

/*
 * --------------------------------------------------------------------------
 * Double sorting split algorithm. This is used for both boxes and points.
 *
 * The algorithm finds split of boxes by considering splits along each axis.
 * Each entry is first projected as an interval on the X-axis, and different
 * ways to split the intervals into two groups are considered, trying to
 * minimize the overlap of the groups. Then the same is repeated for the
 * Y-axis, and the overall best split is chosen. The quality of a split is
 * determined by overlap along that axis and some other criteria (see
 * g_box_consider_split).
 *
 * After that, all the entries are divided into three groups:
 *
 * 1) Entries which should be placed to the left group
 * 2) Entries which should be placed to the right group
 * 3) "Common entries" which can be placed to any of groups without affecting
 *	  of overlap along selected axis.
 *
 * The common entries are distributed by minimizing penalty.
 *
 * For details see:
 * "A new double sorting-based node splitting algorithm for R-tree", A. Korotkov
 * http://syrcose.ispras.ru/2011/files/SYRCoSE2011_Proceedings.pdf#page=36
 * --------------------------------------------------------------------------
 */
pub unsafe fn gist_box_picksplit(fcinfo: FunctionCallInfo) -> Datum {
    let entryvec = PG_GETARG_POINTER!(fcinfo, 0) as *mut GistEntryVector;
    let v = PG_GETARG_POINTER!(fcinfo, 1) as *mut GIST_SPLITVEC;
    let mut i: OffsetNumber;
    let maxoff: OffsetNumber;
    let mut context: ConsiderSplitContext = core::mem::zeroed();
    let mut r#box: *mut BOX;
    let leftBox: *mut BOX;
    let rightBox: *mut BOX;
    let mut dim: c_int;
    let mut commonEntriesCount: c_int;
    let intervalsLower: *mut SplitInterval;
    let intervalsUpper: *mut SplitInterval;
    let commonEntries: *mut CommonEntry;
    let nentries: c_int;

    // memset(&context, 0, sizeof(ConsiderSplitContext));
    context = core::mem::zeroed();

    maxoff = ((*entryvec).n - 1) as OffsetNumber;
    context.entriesCount = maxoff as c_int - FirstOffsetNumber as c_int + 1;
    nentries = context.entriesCount;

    /* Allocate arrays for intervals along axes */
    intervalsLower =
        palloc(nentries as usize * core::mem::size_of::<SplitInterval>()) as *mut SplitInterval;
    intervalsUpper =
        palloc(nentries as usize * core::mem::size_of::<SplitInterval>()) as *mut SplitInterval;

    /*
     * Calculate the overall minimum bounding box over all the entries.
     */
    i = FirstOffsetNumber;
    while i <= maxoff {
        r#box = DatumGetBoxP((*(*entryvec).vector.as_ptr().add(i as usize)).key);
        if i == FirstOffsetNumber {
            context.boundingBox = *r#box;
        } else {
            adjustBox(&mut context.boundingBox, r#box);
        }
        i = OffsetNumberNext(i);
    }

    /*
     * Iterate over axes for optimal split searching.
     */
    context.first = true; /* nothing selected yet */
    dim = 0;
    while dim < 2 {
        let mut leftUpper: float8;
        let mut rightLower: float8;
        let mut i1: c_int;
        let mut i2: c_int;

        /* Project each entry as an interval on the selected axis. */
        i = FirstOffsetNumber;
        while i <= maxoff {
            r#box = DatumGetBoxP((*(*entryvec).vector.as_ptr().add(i as usize)).key);
            if dim == 0 {
                (*intervalsLower.add((i - FirstOffsetNumber) as usize)).lower = (*r#box).low.x;
                (*intervalsLower.add((i - FirstOffsetNumber) as usize)).upper = (*r#box).high.x;
            } else {
                (*intervalsLower.add((i - FirstOffsetNumber) as usize)).lower = (*r#box).low.y;
                (*intervalsLower.add((i - FirstOffsetNumber) as usize)).upper = (*r#box).high.y;
            }
            i = OffsetNumberNext(i);
        }

        /*
         * Make two arrays of intervals: one sorted by lower bound and another
         * sorted by upper bound.
         */
        core::ptr::copy_nonoverlapping(
            intervalsLower as *const u8,
            intervalsUpper as *mut u8,
            core::mem::size_of::<SplitInterval>() * nentries as usize,
        );
        pg_qsort(
            intervalsLower as *mut c_void,
            nentries as usize,
            core::mem::size_of::<SplitInterval>(),
            interval_cmp_lower,
        );
        pg_qsort(
            intervalsUpper as *mut c_void,
            nentries as usize,
            core::mem::size_of::<SplitInterval>(),
            interval_cmp_upper,
        );

        /*----
         * The goal is to form a left and right interval, so that every entry
         * interval is contained by either left or right interval (or both).
         *
         * For example, with the intervals (0,1), (1,3), (2,3), (2,4):
         *
         * 0 1 2 3 4
         * +-+
         *	 +---+
         *	   +-+
         *	   +---+
         *
         * The left and right intervals are of the form (0,a) and (b,4).
         * We first consider splits where b is the lower bound of an entry.
         * We iterate through all entries, and for each b, calculate the
         * smallest possible a. Then we consider splits where a is the
         * upper bound of an entry, and for each a, calculate the greatest
         * possible b.
         *
         * In the above example, the first loop would consider splits:
         * b=0: (0,1)-(0,4)
         * b=1: (0,1)-(1,4)
         * b=2: (0,3)-(2,4)
         *
         * And the second loop:
         * a=1: (0,1)-(1,4)
         * a=3: (0,3)-(2,4)
         * a=4: (0,4)-(2,4)
         */

        /*
         * Iterate over lower bound of right group, finding smallest possible
         * upper bound of left group.
         */
        i1 = 0;
        i2 = 0;
        rightLower = (*intervalsLower.add(i1 as usize)).lower;
        leftUpper = (*intervalsUpper.add(i2 as usize)).lower;
        loop {
            /*
             * Find next lower bound of right group.
             */
            while i1 < nentries
                && float8_eq(rightLower, (*intervalsLower.add(i1 as usize)).lower)
            {
                if float8_lt(leftUpper, (*intervalsLower.add(i1 as usize)).upper) {
                    leftUpper = (*intervalsLower.add(i1 as usize)).upper;
                }
                i1 += 1;
            }
            if i1 >= nentries {
                break;
            }
            rightLower = (*intervalsLower.add(i1 as usize)).lower;

            /*
             * Find count of intervals which anyway should be placed to the
             * left group.
             */
            while i2 < nentries && float8_le((*intervalsUpper.add(i2 as usize)).upper, leftUpper) {
                i2 += 1;
            }

            /*
             * Consider found split.
             */
            g_box_consider_split(&mut context, dim, rightLower, i1, leftUpper, i2);
        }

        /*
         * Iterate over upper bound of left group finding greatest possible
         * lower bound of right group.
         */
        i1 = nentries - 1;
        i2 = nentries - 1;
        rightLower = (*intervalsLower.add(i1 as usize)).upper;
        leftUpper = (*intervalsUpper.add(i2 as usize)).upper;
        loop {
            /*
             * Find next upper bound of left group.
             */
            while i2 >= 0 && float8_eq(leftUpper, (*intervalsUpper.add(i2 as usize)).upper) {
                if float8_gt(rightLower, (*intervalsUpper.add(i2 as usize)).lower) {
                    rightLower = (*intervalsUpper.add(i2 as usize)).lower;
                }
                i2 -= 1;
            }
            if i2 < 0 {
                break;
            }
            leftUpper = (*intervalsUpper.add(i2 as usize)).upper;

            /*
             * Find count of intervals which anyway should be placed to the
             * right group.
             */
            while i1 >= 0 && float8_ge((*intervalsLower.add(i1 as usize)).lower, rightLower) {
                i1 -= 1;
            }

            /*
             * Consider found split.
             */
            g_box_consider_split(&mut context, dim, rightLower, i1 + 1, leftUpper, i2 + 1);
        }
        dim += 1;
    }

    /*
     * If we failed to find any acceptable splits, use trivial split.
     */
    if context.first {
        fallbackSplit(entryvec, v);
        PG_RETURN_POINTER!(v);
    }

    /*
     * Ok, we have now selected the split across one axis.
     *
     * While considering the splits, we already determined that there will be
     * enough entries in both groups to reach the desired ratio, but we did
     * not memorize which entries go to which group. So determine that now.
     */

    /* Allocate vectors for results */
    (*v).spl_left =
        palloc(nentries as usize * core::mem::size_of::<OffsetNumber>()) as *mut OffsetNumber;
    (*v).spl_right =
        palloc(nentries as usize * core::mem::size_of::<OffsetNumber>()) as *mut OffsetNumber;
    (*v).spl_nleft = 0;
    (*v).spl_nright = 0;

    /* Allocate bounding boxes of left and right groups */
    leftBox = palloc0(core::mem::size_of::<BOX>()) as *mut BOX;
    rightBox = palloc0(core::mem::size_of::<BOX>()) as *mut BOX;

    /*
     * Allocate an array for "common entries" - entries which can be placed to
     * either group without affecting overlap along selected axis.
     */
    commonEntriesCount = 0;
    commonEntries =
        palloc(nentries as usize * core::mem::size_of::<CommonEntry>()) as *mut CommonEntry;

    /* Helper macros to place an entry in the left or right group */
    // #define PLACE_LEFT(box, off)
    macro_rules! PLACE_LEFT {
        ($box:expr, $off:expr) => {{
            if (*v).spl_nleft > 0 {
                adjustBox(leftBox, $box);
            } else {
                *leftBox = *($box);
            }
            *(*v).spl_left.add((*v).spl_nleft as usize) = $off;
            (*v).spl_nleft += 1;
        }};
    }

    // #define PLACE_RIGHT(box, off)
    macro_rules! PLACE_RIGHT {
        ($box:expr, $off:expr) => {{
            if (*v).spl_nright > 0 {
                adjustBox(rightBox, $box);
            } else {
                *rightBox = *($box);
            }
            *(*v).spl_right.add((*v).spl_nright as usize) = $off;
            (*v).spl_nright += 1;
        }};
    }

    /*
     * Distribute entries which can be distributed unambiguously, and collect
     * common entries.
     */
    i = FirstOffsetNumber;
    while i <= maxoff {
        let lower: float8;
        let upper: float8;

        /*
         * Get upper and lower bounds along selected axis.
         */
        r#box = DatumGetBoxP((*(*entryvec).vector.as_ptr().add(i as usize)).key);
        if context.dim == 0 {
            lower = (*r#box).low.x;
            upper = (*r#box).high.x;
        } else {
            lower = (*r#box).low.y;
            upper = (*r#box).high.y;
        }

        if float8_le(upper, context.leftUpper) {
            /* Fits to the left group */
            if float8_ge(lower, context.rightLower) {
                /* Fits also to the right group, so "common entry" */
                (*commonEntries.add(commonEntriesCount as usize)).index = i as c_int;
                commonEntriesCount += 1;
            } else {
                /* Doesn't fit to the right group, so join to the left group */
                PLACE_LEFT!(r#box, i);
            }
        } else {
            /*
             * Each entry should fit on either left or right group. Since this
             * entry didn't fit on the left group, it better fit in the right
             * group.
             */
            Assert!(float8_ge(lower, context.rightLower));

            /* Doesn't fit to the left group, so join to the right group */
            PLACE_RIGHT!(r#box, i);
        }
        i = OffsetNumberNext(i);
    }

    /*
     * Distribute "common entries", if any.
     */
    if commonEntriesCount > 0 {
        /*
         * Calculate minimum number of entries that must be placed in both
         * groups, to reach LIMIT_RATIO.
         */
        let m: c_int = (LIMIT_RATIO * nentries as f64).ceil() as c_int;

        /*
         * Calculate delta between penalties of join "common entries" to
         * different groups.
         */
        let mut j: c_int = 0;
        while j < commonEntriesCount {
            r#box = DatumGetBoxP(
                (*(*entryvec)
                    .vector
                    .as_ptr()
                    .add((*commonEntries.add(j as usize)).index as usize))
                .key,
            );
            (*commonEntries.add(j as usize)).delta =
                float8_mi(box_penalty(leftBox, r#box), box_penalty(rightBox, r#box)).abs();
            j += 1;
        }

        /*
         * Sort "common entries" by calculated deltas in order to distribute
         * the most ambiguous entries first.
         */
        pg_qsort(
            commonEntries as *mut c_void,
            commonEntriesCount as usize,
            core::mem::size_of::<CommonEntry>(),
            common_entry_cmp,
        );

        /*
         * Distribute "common entries" between groups.
         */
        let mut k: c_int = 0;
        while k < commonEntriesCount {
            r#box = DatumGetBoxP(
                (*(*entryvec)
                    .vector
                    .as_ptr()
                    .add((*commonEntries.add(k as usize)).index as usize))
                .key,
            );

            /*
             * Check if we have to place this entry in either group to achieve
             * LIMIT_RATIO.
             */
            if (*v).spl_nleft + (commonEntriesCount - k) <= m {
                PLACE_LEFT!(r#box, (*commonEntries.add(k as usize)).index as OffsetNumber);
            } else if (*v).spl_nright + (commonEntriesCount - k) <= m {
                PLACE_RIGHT!(r#box, (*commonEntries.add(k as usize)).index as OffsetNumber);
            } else {
                /* Otherwise select the group by minimal penalty */
                if box_penalty(leftBox, r#box) < box_penalty(rightBox, r#box) {
                    PLACE_LEFT!(r#box, (*commonEntries.add(k as usize)).index as OffsetNumber);
                } else {
                    PLACE_RIGHT!(r#box, (*commonEntries.add(k as usize)).index as OffsetNumber);
                }
            }
            k += 1;
        }
    }

    (*v).spl_ldatum = PointerGetDatum(leftBox as *const c_void);
    (*v).spl_rdatum = PointerGetDatum(rightBox as *const c_void);
    PG_RETURN_POINTER!(v);
}

/*
 * Equality method
 *
 * This is used for boxes, points, circles, and polygons, all of which store
 * boxes as GiST index entries.
 *
 * Returns true only when boxes are exactly the same.  We can't use fuzzy
 * comparisons here without breaking index consistency; therefore, this isn't
 * equivalent to box_same().
 */
pub unsafe fn gist_box_same(fcinfo: FunctionCallInfo) -> Datum {
    let b1 = PG_GETARG_BOX_P!(fcinfo, 0);
    let b2 = PG_GETARG_BOX_P!(fcinfo, 1);
    let result = PG_GETARG_POINTER!(fcinfo, 2) as *mut bool;

    if !b1.is_null() && !b2.is_null() {
        *result = float8_eq((*b1).low.x, (*b2).low.x)
            && float8_eq((*b1).low.y, (*b2).low.y)
            && float8_eq((*b1).high.x, (*b2).high.x)
            && float8_eq((*b1).high.y, (*b2).high.y);
    } else {
        *result = b1.is_null() && b2.is_null();
    }
    PG_RETURN_POINTER!(result);
}

/*
 * Leaf-level consistency for boxes: just apply the query operator
 */
unsafe fn gist_box_leaf_consistent(key: *mut BOX, query: *mut BOX, strategy: StrategyNumber) -> bool {
    let retval: bool;

    match strategy {
        RTLeftStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_left,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTOverLeftStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_overleft,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTOverlapStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_overlap,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTOverRightStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_overright,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTRightStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_right,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTSameStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_same,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTContainsStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_contain,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTContainedByStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_contained,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTOverBelowStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_overbelow,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTBelowStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_below,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTAboveStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_above,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTOverAboveStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_overabove,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        _ => {
            elog!(ERROR, "unrecognized strategy number: {}", strategy as c_int);
            #[allow(unreachable_code)]
            {
                retval = false; /* keep compiler quiet */
            }
        }
    }
    retval
}

/*****************************************
 * Common rtree functions (for boxes, polygons, and circles)
 *****************************************/

/*
 * Internal-page consistency for all these types
 *
 * We can use the same function since all types use bounding boxes as the
 * internal-page representation.
 */
unsafe fn rtree_internal_consistent(
    key: *mut BOX,
    query: *mut BOX,
    strategy: StrategyNumber,
) -> bool {
    let retval: bool;

    match strategy {
        RTLeftStrategyNumber => {
            retval = !DatumGetBool(DirectFunctionCall2!(
                box_overright,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTOverLeftStrategyNumber => {
            retval = !DatumGetBool(DirectFunctionCall2!(
                box_right,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTOverlapStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_overlap,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTOverRightStrategyNumber => {
            retval = !DatumGetBool(DirectFunctionCall2!(
                box_left,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTRightStrategyNumber => {
            retval = !DatumGetBool(DirectFunctionCall2!(
                box_overleft,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTSameStrategyNumber | RTContainsStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_contain,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTContainedByStrategyNumber => {
            retval = DatumGetBool(DirectFunctionCall2!(
                box_overlap,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTOverBelowStrategyNumber => {
            retval = !DatumGetBool(DirectFunctionCall2!(
                box_above,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTBelowStrategyNumber => {
            retval = !DatumGetBool(DirectFunctionCall2!(
                box_overabove,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTAboveStrategyNumber => {
            retval = !DatumGetBool(DirectFunctionCall2!(
                box_overbelow,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        RTOverAboveStrategyNumber => {
            retval = !DatumGetBool(DirectFunctionCall2!(
                box_below,
                PointerGetDatum(key as *const c_void),
                PointerGetDatum(query as *const c_void)
            ));
        }
        _ => {
            elog!(ERROR, "unrecognized strategy number: {}", strategy as c_int);
            #[allow(unreachable_code)]
            {
                retval = false; /* keep compiler quiet */
            }
        }
    }
    retval
}

/**************************************************
 * Polygon ops
 **************************************************/

/*
 * GiST compress for polygons: represent a polygon by its bounding box
 */
pub unsafe fn gist_poly_compress(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let retval: *mut GISTENTRY;

    if (*entry).leafkey {
        let r#in = DatumGetPolygonP((*entry).key);
        let r: *mut BOX;

        r = palloc(core::mem::size_of::<BOX>()) as *mut BOX;
        core::ptr::copy_nonoverlapping(
            &(*r#in).boundbox as *const BOX as *const u8,
            r as *mut u8,
            core::mem::size_of::<BOX>(),
        );

        retval = palloc(core::mem::size_of::<GISTENTRY>()) as *mut GISTENTRY;
        gistentryinit(
            retval,
            PointerGetDatum(r as *const c_void),
            (*entry).rel,
            (*entry).page,
            (*entry).offset,
            false,
        );
    } else {
        retval = entry;
    }
    PG_RETURN_POINTER!(retval);
}

/*
 * The GiST Consistent method for polygons
 */
pub unsafe fn gist_poly_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let query = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let strategy = PG_GETARG_UINT16!(fcinfo, 2) as StrategyNumber;

    /* Oid		subtype = PG_GETARG_OID(3); */
    let recheck = PG_GETARG_POINTER!(fcinfo, 4) as *mut bool;
    let result: bool;

    /* All cases served by this function are inexact */
    *recheck = true;

    if DatumGetBoxP((*entry).key).is_null() || query.is_null() {
        PG_RETURN_BOOL!(false);
    }

    /*
     * Since the operators require recheck anyway, we can just use
     * rtree_internal_consistent even at leaf nodes.  (This works in part
     * because the index entries are bounding boxes not polygons.)
     */
    result = rtree_internal_consistent(
        DatumGetBoxP((*entry).key),
        &mut (*query).boundbox,
        strategy,
    );

    /* Avoid memory leak if supplied poly is toasted */
    // C also: PG_FREE_IF_COPY(query, 1);

    PG_RETURN_BOOL!(result);
}

/**************************************************
 * Circle ops
 **************************************************/

/*
 * GiST compress for circles: represent a circle by its bounding box
 */
pub unsafe fn gist_circle_compress(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let retval: *mut GISTENTRY;

    if (*entry).leafkey {
        let r#in = DatumGetCircleP((*entry).key);
        let r: *mut BOX;

        r = palloc(core::mem::size_of::<BOX>()) as *mut BOX;
        (*r).high.x = float8_pl((*r#in).center.x, (*r#in).radius);
        (*r).low.x = float8_mi((*r#in).center.x, (*r#in).radius);
        (*r).high.y = float8_pl((*r#in).center.y, (*r#in).radius);
        (*r).low.y = float8_mi((*r#in).center.y, (*r#in).radius);

        retval = palloc(core::mem::size_of::<GISTENTRY>()) as *mut GISTENTRY;
        gistentryinit(
            retval,
            PointerGetDatum(r as *const c_void),
            (*entry).rel,
            (*entry).page,
            (*entry).offset,
            false,
        );
    } else {
        retval = entry;
    }
    PG_RETURN_POINTER!(retval);
}

/*
 * The GiST Consistent method for circles
 */
pub unsafe fn gist_circle_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let query = PG_GETARG_CIRCLE_P!(fcinfo, 1);
    let strategy = PG_GETARG_UINT16!(fcinfo, 2) as StrategyNumber;

    /* Oid		subtype = PG_GETARG_OID(3); */
    let recheck = PG_GETARG_POINTER!(fcinfo, 4) as *mut bool;
    let mut bbox: BOX = core::mem::zeroed();
    let result: bool;

    /* All cases served by this function are inexact */
    *recheck = true;

    if DatumGetBoxP((*entry).key).is_null() || query.is_null() {
        PG_RETURN_BOOL!(false);
    }

    /*
     * Since the operators require recheck anyway, we can just use
     * rtree_internal_consistent even at leaf nodes.  (This works in part
     * because the index entries are bounding boxes not circles.)
     */
    bbox.high.x = float8_pl((*query).center.x, (*query).radius);
    bbox.low.x = float8_mi((*query).center.x, (*query).radius);
    bbox.high.y = float8_pl((*query).center.y, (*query).radius);
    bbox.low.y = float8_mi((*query).center.y, (*query).radius);

    result = rtree_internal_consistent(DatumGetBoxP((*entry).key), &mut bbox, strategy);

    PG_RETURN_BOOL!(result);
}

/**************************************************
 * Point ops
 **************************************************/

pub unsafe fn gist_point_compress(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;

    if (*entry).leafkey {
        /* Point, actually */
        let r#box = palloc(core::mem::size_of::<BOX>()) as *mut BOX;
        let point = DatumGetPointP((*entry).key);
        let retval = palloc(core::mem::size_of::<GISTENTRY>()) as *mut GISTENTRY;

        (*r#box).high = *point;
        (*r#box).low = *point;

        gistentryinit(
            retval,
            BoxPGetDatum(r#box),
            (*entry).rel,
            (*entry).page,
            (*entry).offset,
            false,
        );

        PG_RETURN_POINTER!(retval);
    }

    PG_RETURN_POINTER!(entry);
}

/*
 * GiST Fetch method for point
 *
 * Get point coordinates from its bounding box coordinates and form new
 * gistentry.
 */
pub unsafe fn gist_point_fetch(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let r#in = DatumGetBoxP((*entry).key);
    let r: *mut Point;
    let retval: *mut GISTENTRY;

    retval = palloc(core::mem::size_of::<GISTENTRY>()) as *mut GISTENTRY;

    r = palloc(core::mem::size_of::<Point>()) as *mut Point;
    (*r).x = (*r#in).high.x;
    (*r).y = (*r#in).high.y;
    gistentryinit(
        retval,
        PointerGetDatum(r as *const c_void),
        (*entry).rel,
        (*entry).page,
        (*entry).offset,
        false,
    );

    PG_RETURN_POINTER!(retval);
}

// #define point_point_distance(p1,p2)
//     DatumGetFloat8(DirectFunctionCall2(point_distance,
//                                        PointPGetDatum(p1), PointPGetDatum(p2)))
#[inline]
unsafe fn point_point_distance(p1: *const Point, p2: *const Point) -> float8 {
    DatumGetFloat8(DirectFunctionCall2!(
        point_distance,
        PointPGetDatum(p1),
        PointPGetDatum(p2)
    ))
}

unsafe fn computeDistance(isLeaf: bool, r#box: *mut BOX, point: *mut Point) -> float8 {
    let mut result: float8 = 0.0;

    if isLeaf {
        /* simple point to point distance */
        result = point_point_distance(point, &(*r#box).low);
    } else if (*point).x <= (*r#box).high.x
        && (*point).x >= (*r#box).low.x
        && (*point).y <= (*r#box).high.y
        && (*point).y >= (*r#box).low.y
    {
        /* point inside the box */
        result = 0.0;
    } else if (*point).x <= (*r#box).high.x && (*point).x >= (*r#box).low.x {
        /* point is over or below box */
        Assert!((*r#box).low.y <= (*r#box).high.y);
        if (*point).y > (*r#box).high.y {
            result = float8_mi((*point).y, (*r#box).high.y);
        } else if (*point).y < (*r#box).low.y {
            result = float8_mi((*r#box).low.y, (*point).y);
        } else {
            elog!(ERROR, "inconsistent point values");
        }
    } else if (*point).y <= (*r#box).high.y && (*point).y >= (*r#box).low.y {
        /* point is to left or right of box */
        Assert!((*r#box).low.x <= (*r#box).high.x);
        if (*point).x > (*r#box).high.x {
            result = float8_mi((*point).x, (*r#box).high.x);
        } else if (*point).x < (*r#box).low.x {
            result = float8_mi((*r#box).low.x, (*point).x);
        } else {
            elog!(ERROR, "inconsistent point values");
        }
    } else {
        /* closest point will be a vertex */
        let mut p: Point = core::mem::zeroed();
        let mut subresult: float8;

        result = point_point_distance(point, &(*r#box).low);

        subresult = point_point_distance(point, &(*r#box).high);
        if result > subresult {
            result = subresult;
        }

        p.x = (*r#box).low.x;
        p.y = (*r#box).high.y;
        subresult = point_point_distance(point, &p);
        if result > subresult {
            result = subresult;
        }

        p.x = (*r#box).high.x;
        p.y = (*r#box).low.y;
        subresult = point_point_distance(point, &p);
        if result > subresult {
            result = subresult;
        }
    }

    result
}

unsafe fn gist_point_consistent_internal(
    strategy: StrategyNumber,
    isLeaf: bool,
    key: *mut BOX,
    query: *mut Point,
) -> bool {
    let mut result: bool = false;

    match strategy {
        RTLeftStrategyNumber => {
            result = FPlt((*key).low.x, (*query).x);
        }
        RTRightStrategyNumber => {
            result = FPgt((*key).high.x, (*query).x);
        }
        RTAboveStrategyNumber => {
            result = FPgt((*key).high.y, (*query).y);
        }
        RTBelowStrategyNumber => {
            result = FPlt((*key).low.y, (*query).y);
        }
        RTSameStrategyNumber => {
            if isLeaf {
                /* key.high must equal key.low, so we can disregard it */
                result = FPeq((*key).low.x, (*query).x) && FPeq((*key).low.y, (*query).y);
            } else {
                result = FPle((*query).x, (*key).high.x)
                    && FPge((*query).x, (*key).low.x)
                    && FPle((*query).y, (*key).high.y)
                    && FPge((*query).y, (*key).low.y);
            }
        }
        _ => {
            elog!(ERROR, "unrecognized strategy number: {}", strategy as c_int);
            #[allow(unreachable_code)]
            {
                result = false; /* keep compiler quiet */
            }
        }
    }

    result
}

const GeoStrategyNumberOffset: c_int = 20;
const PointStrategyNumberGroup: c_int = 0;
const BoxStrategyNumberGroup: c_int = 1;
const PolygonStrategyNumberGroup: c_int = 2;
const CircleStrategyNumberGroup: c_int = 3;

pub unsafe fn gist_point_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let mut strategy = PG_GETARG_UINT16!(fcinfo, 2) as StrategyNumber;
    let recheck = PG_GETARG_POINTER!(fcinfo, 4) as *mut bool;
    let mut result: bool;
    let strategyGroup: StrategyNumber;

    /*
     * We have to remap these strategy numbers to get this klugy
     * classification logic to work.
     */
    if strategy == RTOldBelowStrategyNumber {
        strategy = RTBelowStrategyNumber;
    } else if strategy == RTOldAboveStrategyNumber {
        strategy = RTAboveStrategyNumber;
    }

    strategyGroup = strategy / GeoStrategyNumberOffset as StrategyNumber;
    match strategyGroup as c_int {
        PointStrategyNumberGroup => {
            result = gist_point_consistent_internal(
                strategy % GeoStrategyNumberOffset as StrategyNumber,
                GIST_LEAF(entry),
                DatumGetBoxP((*entry).key),
                PG_GETARG_POINT_P!(fcinfo, 1),
            );
            *recheck = false;
        }
        BoxStrategyNumberGroup => {
            /*
             * The only operator in this group is point <@ box (on_pb), so
             * we needn't examine strategy again.
             *
             * For historical reasons, on_pb uses exact rather than fuzzy
             * comparisons.  We could use box_overlap when at an internal
             * page, but that would lead to possibly visiting child pages
             * uselessly, because box_overlap uses fuzzy comparisons.
             * Instead we write a non-fuzzy overlap test.  The same code
             * will also serve for leaf-page tests, since leaf keys have
             * high == low.
             */
            let query: *mut BOX;
            let key: *mut BOX;

            query = PG_GETARG_BOX_P!(fcinfo, 1);
            key = DatumGetBoxP((*entry).key);

            result = (*key).high.x >= (*query).low.x
                && (*key).low.x <= (*query).high.x
                && (*key).high.y >= (*query).low.y
                && (*key).low.y <= (*query).high.y;
            *recheck = false;
        }
        PolygonStrategyNumberGroup => {
            let query = PG_GETARG_POLYGON_P!(fcinfo, 1);

            result = DatumGetBool(DirectFunctionCall5!(
                gist_poly_consistent,
                PointerGetDatum(entry as *const c_void),
                PolygonPGetDatum(query),
                Int16GetDatum(RTOverlapStrategyNumber as i16),
                0,
                PointerGetDatum(recheck as *const c_void)
            ));

            if GIST_LEAF(entry) && result {
                /*
                 * We are on leaf page and quick check shows overlapping
                 * of polygon's bounding box and point
                 */
                let r#box = DatumGetBoxP((*entry).key);

                Assert!((*r#box).high.x == (*r#box).low.x && (*r#box).high.y == (*r#box).low.y);
                result = DatumGetBool(DirectFunctionCall2!(
                    poly_contain_pt,
                    PolygonPGetDatum(query),
                    PointPGetDatum(&(*r#box).high)
                ));
                *recheck = false;
            }
        }
        CircleStrategyNumberGroup => {
            let query = PG_GETARG_CIRCLE_P!(fcinfo, 1);

            result = DatumGetBool(DirectFunctionCall5!(
                gist_circle_consistent,
                PointerGetDatum(entry as *const c_void),
                CirclePGetDatum(query),
                Int16GetDatum(RTOverlapStrategyNumber as i16),
                0,
                PointerGetDatum(recheck as *const c_void)
            ));

            if GIST_LEAF(entry) && result {
                /*
                 * We are on leaf page and quick check shows overlapping
                 * of polygon's bounding box and point
                 */
                let r#box = DatumGetBoxP((*entry).key);

                Assert!((*r#box).high.x == (*r#box).low.x && (*r#box).high.y == (*r#box).low.y);
                result = DatumGetBool(DirectFunctionCall2!(
                    circle_contain_pt,
                    CirclePGetDatum(query),
                    PointPGetDatum(&(*r#box).high)
                ));
                *recheck = false;
            }
        }
        _ => {
            elog!(ERROR, "unrecognized strategy number: {}", strategy as c_int);
            #[allow(unreachable_code)]
            {
                result = false; /* keep compiler quiet */
            }
        }
    }

    PG_RETURN_BOOL!(result);
}

pub unsafe fn gist_point_distance(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let strategy = PG_GETARG_UINT16!(fcinfo, 2) as StrategyNumber;
    let distance: float8;
    let strategyGroup: StrategyNumber = strategy / GeoStrategyNumberOffset as StrategyNumber;

    match strategyGroup as c_int {
        PointStrategyNumberGroup => {
            distance = computeDistance(
                GIST_LEAF(entry),
                DatumGetBoxP((*entry).key),
                PG_GETARG_POINT_P!(fcinfo, 1),
            );
        }
        _ => {
            elog!(ERROR, "unrecognized strategy number: {}", strategy as c_int);
            #[allow(unreachable_code)]
            {
                distance = 0.0; /* keep compiler quiet */
            }
        }
    }

    PG_RETURN_FLOAT8!(distance);
}

unsafe fn gist_bbox_distance(
    entry: *mut GISTENTRY,
    query: Datum,
    strategy: StrategyNumber,
) -> float8 {
    let distance: float8;
    let strategyGroup: StrategyNumber = strategy / GeoStrategyNumberOffset as StrategyNumber;

    match strategyGroup as c_int {
        PointStrategyNumberGroup => {
            distance = computeDistance(false, DatumGetBoxP((*entry).key), DatumGetPointP(query));
        }
        _ => {
            elog!(ERROR, "unrecognized strategy number: {}", strategy as c_int);
            #[allow(unreachable_code)]
            {
                distance = 0.0; /* keep compiler quiet */
            }
        }
    }

    distance
}

pub unsafe fn gist_box_distance(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let query = PG_GETARG_DATUM!(fcinfo, 1);
    let strategy = PG_GETARG_UINT16!(fcinfo, 2) as StrategyNumber;

    /* Oid subtype = PG_GETARG_OID(3); */
    /* bool	   *recheck = (bool *) PG_GETARG_POINTER(4); */
    let distance: float8;

    distance = gist_bbox_distance(entry, query, strategy);

    PG_RETURN_FLOAT8!(distance);
}

/*
 * The inexact GiST distance methods for geometric types that store bounding
 * boxes.
 *
 * Compute lossy distance from point to index entries.  The result is inexact
 * because index entries are bounding boxes, not the exact shapes of the
 * indexed geometric types.  We use distance from point to MBR of index entry.
 * This is a lower bound estimate of distance from point to indexed geometric
 * type.
 */
pub unsafe fn gist_circle_distance(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let query = PG_GETARG_DATUM!(fcinfo, 1);
    let strategy = PG_GETARG_UINT16!(fcinfo, 2) as StrategyNumber;

    /* Oid subtype = PG_GETARG_OID(3); */
    let recheck = PG_GETARG_POINTER!(fcinfo, 4) as *mut bool;
    let distance: float8;

    distance = gist_bbox_distance(entry, query, strategy);
    *recheck = true;

    PG_RETURN_FLOAT8!(distance);
}

pub unsafe fn gist_poly_distance(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let query = PG_GETARG_DATUM!(fcinfo, 1);
    let strategy = PG_GETARG_UINT16!(fcinfo, 2) as StrategyNumber;

    /* Oid subtype = PG_GETARG_OID(3); */
    let recheck = PG_GETARG_POINTER!(fcinfo, 4) as *mut bool;
    let distance: float8;

    distance = gist_bbox_distance(entry, query, strategy);
    *recheck = true;

    PG_RETURN_FLOAT8!(distance);
}

/*
 * Z-order routines for fast index build
 */

/*
 * Compute Z-value of a point
 *
 * Z-order (also known as Morton Code) maps a two-dimensional point to a
 * single integer, in a way that preserves locality. Points that are close in
 * the two-dimensional space are mapped to integer that are not far from each
 * other. We do that by interleaving the bits in the X and Y components.
 *
 * Morton Code is normally defined only for integers, but the X and Y values
 * of a point are floating point. We expect floats to be in IEEE format.
 */
unsafe fn point_zorder_internal(x: float4, y: float4) -> uint64 {
    let ix = ieee_float32_to_uint32(x);
    let iy = ieee_float32_to_uint32(y);

    /* Interleave the bits */
    part_bits32_by2(ix) | (part_bits32_by2(iy) << 1)
}

/* Interleave 32 bits with zeroes */
unsafe fn part_bits32_by2(x: uint32) -> uint64 {
    let mut n: uint64 = x as uint64;

    n = (n | (n << 16)) & UINT64CONST(0x0000FFFF0000FFFF);
    n = (n | (n << 8)) & UINT64CONST(0x00FF00FF00FF00FF);
    n = (n | (n << 4)) & UINT64CONST(0x0F0F0F0F0F0F0F0F);
    n = (n | (n << 2)) & UINT64CONST(0x3333333333333333);
    n = (n | (n << 1)) & UINT64CONST(0x5555555555555555);

    n
}

/*
 * Convert a 32-bit IEEE float to uint32 in a way that preserves the ordering
 */
unsafe fn ieee_float32_to_uint32(f: f32) -> uint32 {
    /*----
     *
     * IEEE 754 floating point format
     * ------------------------------
     *
     * IEEE 754 floating point numbers have this format:
     *
     *   exponent (8 bits)
     *   |
     * s eeeeeeee mmmmmmmmmmmmmmmmmmmmmmm
     * |          |
     * sign       mantissa (23 bits)
     *
     * Infinity has all bits in the exponent set and the mantissa is all
     * zeros. Negative infinity is the same but with the sign bit set.
     *
     * NaNs are represented with all bits in the exponent set, and the least
     * significant bit in the mantissa also set. The rest of the mantissa bits
     * can be used to distinguish different kinds of NaNs.
     *
     * The IEEE format has the nice property that when you take the bit
     * representation and interpret it as an integer, the order is preserved,
     * except for the sign. That holds for the +-Infinity values too.
     *
     * Mapping to uint32
     * -----------------
     *
     * In order to have a smooth transition from negative to positive numbers,
     * we map floats to unsigned integers like this:
     *
     * x < 0 to range 0-7FFFFFFF
     * x = 0 to value 8000000 (both positive and negative zero)
     * x > 0 to range 8000001-FFFFFFFF
     *
     * We don't care to distinguish different kind of NaNs, so they are all
     * mapped to the same arbitrary value, FFFFFFFF. Because of the IEEE bit
     * representation of NaNs, there aren't any non-NaN values that would be
     * mapped to FFFFFFFF. In fact, there is a range of unused values on both
     * ends of the uint32 space.
     */
    if f.is_nan() {
        return 0xFFFFFFFF;
    } else {
        // union { float f; uint32 i; } u;
        let mut i: uint32 = f.to_bits();

        /* Check the sign bit */
        if (i & 0x80000000) != 0 {
            /*
             * Map the negative value to range 0-7FFFFFFF. This flips the sign
             * bit to 0 in the same instruction.
             */
            Assert!(f <= 0.0); /* can be -0 */
            i ^= 0xFFFFFFFF;
        } else {
            /* Map the positive value (or 0) to range 80000000-FFFFFFFF */
            i |= 0x80000000;
        }

        return i;
    }
}

/*
 * Compare the Z-order of points
 */
unsafe fn gist_bbox_zorder_cmp(a: Datum, b: Datum, _ssup: SortSupport) -> c_int {
    let p1: *mut Point = &mut (*DatumGetBoxP(a)).low;
    let p2: *mut Point = &mut (*DatumGetBoxP(b)).low;
    let z1: uint64;
    let z2: uint64;

    /*
     * Do a quick check for equality first. It's not clear if this is worth it
     * in general, but certainly is when used as tie-breaker with abbreviated
     * keys,
     */
    if (*p1).x == (*p2).x && (*p1).y == (*p2).y {
        return 0;
    }

    z1 = point_zorder_internal((*p1).x as float4, (*p1).y as float4);
    z2 = point_zorder_internal((*p2).x as float4, (*p2).y as float4);
    if z1 > z2 {
        1
    } else if z1 < z2 {
        -1
    } else {
        0
    }
}

/*
 * Abbreviated version of Z-order comparison
 *
 * The abbreviated format is a Z-order value computed from the two 32-bit
 * floats. If SIZEOF_DATUM == 8, the 64-bit Z-order value fits fully in the
 * abbreviated Datum, otherwise use its most significant bits.
 */
unsafe fn gist_bbox_zorder_abbrev_convert(original: Datum, _ssup: SortSupport) -> Datum {
    let p: *mut Point = &mut (*DatumGetBoxP(original)).low;
    let z: uint64;

    z = point_zorder_internal((*p).x as float4, (*p).y as float4);

    // #if SIZEOF_DATUM == 8
    if core::mem::size_of::<Datum>() == 8 {
        z as Datum
    } else {
        // #else
        (z >> 32) as Datum
    }
}

/*
 * We never consider aborting the abbreviation.
 *
 * On 64-bit systems, the abbreviation is not lossy so it is always
 * worthwhile. (Perhaps it's not on 32-bit systems, but we don't bother
 * with logic to decide.)
 */
unsafe fn gist_bbox_zorder_abbrev_abort(_memtupcount: c_int, _ssup: SortSupport) -> bool {
    false
}

/*
 * Sort support routine for fast GiST index build by sorting.
 */
pub unsafe fn gist_point_sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;

    if (*ssup).abbreviate {
        (*ssup).comparator = Some(ssup_datum_unsigned_cmp);
        (*ssup).abbrev_converter = Some(gist_bbox_zorder_abbrev_convert);
        (*ssup).abbrev_abort = Some(gist_bbox_zorder_abbrev_abort);
        (*ssup).abbrev_full_comparator = Some(gist_bbox_zorder_cmp);
    } else {
        (*ssup).comparator = Some(gist_bbox_zorder_cmp);
    }
    PG_RETURN_VOID!();
}
