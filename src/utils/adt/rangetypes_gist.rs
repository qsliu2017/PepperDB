//! Translation of postgres/src/backend/utils/adt/rangetypes_gist.c
//!
//! GiST support for range types.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!			src/backend/utils/adt/rangetypes_gist.c
//!
//! #include mapping:
//!   postgres.h            -> crate::prelude::*
//!   access/gist.h         -> NOT PORTED.  GISTENTRY / GistEntryVector /
//!                            GIST_SPLITVEC / gistentryinit / GIST_LEAF are
//!                            defined LOCALLY below, mirroring the minimal
//!                            definitions in network_gist.rs / tsquery_gist.rs.
//!                            TODO(pg-port): replace with crate::access::gist.
//!   access/stratnum.h     -> crate::access::stratnum (RT* strategy consts)
//!   utils/datum.h         -> crate::utils::adt::datum::datumCopy
//!   utils/float.h         -> crate::utils::adt::float (get_float4_infinity)
//!   utils/fmgrprotos.h    -> the fmgr-callable entry points are defined here
//!   utils/multirangetypes.h -> crate::utils::adt::multirangetypes
//!   utils/rangetypes.h    -> crate::utils::adt::rangetypes

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use crate::prelude::*;

use crate::access::stratnum::{
    RTContainedByStrategyNumber, RTContainsElemStrategyNumber, RTContainsStrategyNumber,
    RTEqualStrategyNumber, RTLeftStrategyNumber, RTOverLeftStrategyNumber,
    RTOverRightStrategyNumber, RTOverlapStrategyNumber, RTRightStrategyNumber, RTSameStrategyNumber,
    StrategyNumber,
};
use crate::catalog::pg_type_d::{ANYMULTIRANGEOID, ANYRANGEOID};
use crate::port::qsort::{pg_qsort, qsort_arg};
use crate::storage::off::{FirstOffsetNumber, OffsetNumber, OffsetNumberNext};
use crate::utils::adt::datum::datumCopy;
use crate::utils::adt::float::get_float4_infinity;
use crate::utils::adt::multirangetypes::{
    multirange_contains_range_internal, multirange_get_bounds, multirange_get_typcache,
    multirange_get_union_range, range_adjacent_multirange_internal,
    range_after_multirange_internal, range_before_multirange_internal,
    range_contains_multirange_internal, range_overlaps_multirange_internal,
    range_overleft_multirange_internal, range_overright_multirange_internal, MultirangeType,
};
use crate::utils::adt::rangetypes::{
    make_range, range_adjacent_internal, range_after_internal, range_before_internal,
    range_cmp_bounds, range_contained_by_internal, range_contains_elem_internal,
    range_contains_internal, range_deserialize, range_eq_internal, range_get_flags,
    range_get_typcache, range_overlaps_internal, range_overleft_internal, range_overright_internal,
    range_set_contain_empty, DatumGetRangeTypeP, RangeBound, RangeType, RangeIsEmpty,
    RangeIsOrContainsEmpty, RangeTypeGetOid, RangeTypePGetDatum, RANGE_CONTAIN_EMPTY, RANGE_EMPTY,
    RANGE_LB_INF, RANGE_UB_INF,
};
use crate::utils::cache::typcache::TypeCacheEntry;
use crate::utils::fmgr::{FunctionCall2Coll, FunctionCallInfo};

use crate::{
    PG_GETARG_DATUM, PG_GETARG_OID, PG_GETARG_POINTER, PG_GETARG_RANGE_P, PG_GETARG_UINT16,
    PG_RETURN_BOOL, PG_RETURN_POINTER, PG_RETURN_RANGE_P,
};

extern "C" {
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn abs(j: c_int) -> c_int;
}

// ================================================================
//   utils/rangetypes.h:  PG_RETURN_RANGE_P / PG_GETARG_RANGE_P live in
//   rangetypes.rs but are not #[macro_export]ed; local copies here.
// ================================================================

/* PG_GETARG_RANGE_P / PG_RETURN_RANGE_P (range-specific fmgr macros) */
macro_rules! PG_GETARG_RANGE_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetRangeTypeP(crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_RETURN_RANGE_P {
    ($x:expr) => {
        return RangeTypePGetDatum($x)
    };
}

/* multirangetypes.h: fmgr functions for multirange type objects */
#[inline]
unsafe fn DatumGetMultirangeTypeP(X: Datum) -> *mut MultirangeType {
    crate::PG_DETOAST_DATUM!(X) as *mut MultirangeType
}
#[inline]
unsafe fn MultirangeTypeGetOid(mr: *const MultirangeType) -> Oid {
    (*mr).multirangetypid
}
#[inline]
unsafe fn MultirangeIsEmpty(mr: *const MultirangeType) -> bool {
    (*mr).rangeCount == 0
}

// ================================================================
//   access/gist.h  --  NOT yet ported in its own module.
//   GISTENTRY / GistEntryVector / GIST_SPLITVEC / gistentryinit / GIST_LEAF
//   are defined LOCALLY here, mirroring network_gist.rs / tsquery_gist.rs.
//   TODO(pg-port): replace with the real crate::access::gist once ported.
// ================================================================

/*
 * struct GISTENTRY (access/gist.h).  `rel`/`page` (Relation/Page) are opaque.
 */
#[repr(C)]
pub struct GISTENTRY {
    pub key: Datum,
    pub rel: *mut c_void,
    pub page: *mut c_void,
    pub offset: OffsetNumber,
    pub leafkey: bool,
}

/*
 * struct GIST_SPLITVEC (access/gist.h): the split vector returned by PickSplit.
 */
#[repr(C)]
pub struct GIST_SPLITVEC {
    pub spl_left: *mut OffsetNumber,
    pub spl_nleft: c_int,
    pub spl_ldatum: Datum,
    pub spl_ldatum_exists: bool,

    pub spl_right: *mut OffsetNumber,
    pub spl_nright: c_int,
    pub spl_rdatum: Datum,
    pub spl_rdatum_exists: bool,
}

/*
 * struct GistEntryVector (access/gist.h): vector of GISTENTRY with a leading
 * count.  `vector` is a C flexible-array member; modeled as a zero-length array
 * we index past the end of (matching the on-heap layout).
 */
#[repr(C)]
pub struct GistEntryVector {
    pub n: int32,
    pub vector: [GISTENTRY; 0],
}

impl GistEntryVector {
    /* &entryvec->vector[pos] */
    #[inline]
    unsafe fn entry(&self, pos: usize) -> *mut GISTENTRY {
        (self.vector.as_ptr() as *mut GISTENTRY).add(pos)
    }
}

/* #define gistentryinit(e, k, r, pg, o, l) ...  -- initialize a GISTENTRY. */
#[inline]
unsafe fn gistentryinit(
    e: *mut GISTENTRY,
    k: Datum,
    r: *mut c_void,
    pg: *mut c_void,
    o: OffsetNumber,
    l: bool,
) {
    (*e).key = k;
    (*e).rel = r;
    (*e).page = pg;
    (*e).offset = o;
    (*e).leafkey = l;
}

/*
 * #define GIST_LEAF(entry) (GistPageIsLeaf((entry)->page))
 *
 * STUB: needs the real GiST page layout (GistPageGetOpaque -> flags & F_LEAF)
 * from the unported storage/bufpage + access/gist page opaque area.
 */
#[inline]
unsafe fn GIST_LEAF(_entry: *const GISTENTRY) -> bool {
    // TODO(pg-port): GistPageIsLeaf((entry)->page) once the GiST page layout is ported.
    unimplemented!("GIST_LEAF requires the unported GiST page layout")
}

// ================================================================
//   Range class properties used to segregate different classes of ranges in
//   GiST.  Each unique combination of properties is a class.  CLS_EMPTY cannot
//   be combined with anything else.
// ================================================================

const CLS_NORMAL: usize = 0; /* Ordinary finite range (no bits set) */
const CLS_LOWER_INF: usize = 1; /* Lower bound is infinity */
const CLS_UPPER_INF: usize = 2; /* Upper bound is infinity */
const CLS_CONTAIN_EMPTY: usize = 4; /* Contains underlying empty ranges */
const CLS_EMPTY: usize = 8; /* Special class for empty ranges */

const CLS_COUNT: usize = 9; /* # of classes; includes all combinations of
                             * properties. CLS_EMPTY doesn't combine with
                             * anything else, so it's only 2^3 + 1. */

/*
 * Minimum accepted ratio of split for items of the same class.  If the items
 * are of different classes, we will separate along those lines regardless of
 * the ratio.
 */
const LIMIT_RATIO: f64 = 0.3;

/* Constants for fixed penalty values */
const INFINITE_BOUND_PENALTY: f32 = 2.0;
const CONTAIN_EMPTY_PENALTY: f32 = 1.0;
const DEFAULT_SUBTYPE_DIFF_PENALTY: f32 = 1.0;

// Operator strategy numbers used in the GiST and SP-GiST range opclasses
// (utils/rangetypes.h).
const RANGESTRAT_BEFORE: StrategyNumber = RTLeftStrategyNumber;
const RANGESTRAT_OVERLEFT: StrategyNumber = RTOverLeftStrategyNumber;
const RANGESTRAT_OVERLAPS: StrategyNumber = RTOverlapStrategyNumber;
const RANGESTRAT_OVERRIGHT: StrategyNumber = RTOverRightStrategyNumber;
const RANGESTRAT_AFTER: StrategyNumber = RTRightStrategyNumber;
const RANGESTRAT_ADJACENT: StrategyNumber = RTSameStrategyNumber;
const RANGESTRAT_CONTAINS: StrategyNumber = RTContainsStrategyNumber;
const RANGESTRAT_CONTAINED_BY: StrategyNumber = RTContainedByStrategyNumber;
const RANGESTRAT_CONTAINS_ELEM: StrategyNumber = RTContainsElemStrategyNumber;
const RANGESTRAT_EQ: StrategyNumber = RTEqualStrategyNumber;

/*
 * Per-item data for range_gist_single_sorting_split.
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct SingleBoundSortItem {
    index: c_int,
    bound: RangeBound,
}

/* place on left or right side of split? */
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
enum SplitLR {
    SPLIT_LEFT = 0, /* makes initialization to SPLIT_LEFT easier */
    SPLIT_RIGHT,
}
use SplitLR::*;

/*
 * Context for range_gist_consider_split.
 */
#[repr(C)]
struct ConsiderSplitContext {
    typcache: *mut TypeCacheEntry, /* typcache for range type */
    has_subtype_diff: bool,        /* does it have subtype_diff? */
    entries_count: c_int,          /* total number of entries being split */

    /* Information about currently selected split follows */
    first: bool, /* true if no split was selected yet */

    left_upper: *mut RangeBound,  /* upper bound of left interval */
    right_lower: *mut RangeBound, /* lower bound of right interval */

    ratio: f32,        /* split ratio */
    overlap: f32,      /* overlap between left and right predicate */
    common_left: c_int, /* # common entries destined for each side */
    common_right: c_int,
}

/*
 * Bounds extracted from a non-empty range, for use in
 * range_gist_double_sorting_split.
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct NonEmptyRange {
    lower: RangeBound,
    upper: RangeBound,
}

/*
 * Represents information about an entry that can be placed in either group
 * without affecting overlap over selected axis ("common entry").
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct CommonEntry {
    /* Index of entry in the initial array */
    index: c_int,
    /* Delta between closeness of range to each of the two groups */
    delta: f64,
}

/*
 * Helper macros to place an entry in the left or right group during split.
 * Note direct access to variables v, typcache, left_range, right_range.
 *
 * #define PLACE_LEFT(range, off) ...
 * #define PLACE_RIGHT(range, off) ...
 */
macro_rules! PLACE_LEFT {
    ($v:expr, $typcache:expr, $left_range:expr, $range:expr, $off:expr) => {{
        if (*$v).spl_nleft > 0 {
            $left_range = range_super_union($typcache, $left_range, $range);
        } else {
            $left_range = $range;
        }
        *(*$v).spl_left.add((*$v).spl_nleft as usize) = $off;
        (*$v).spl_nleft += 1;
    }};
}
macro_rules! PLACE_RIGHT {
    ($v:expr, $typcache:expr, $right_range:expr, $range:expr, $off:expr) => {{
        if (*$v).spl_nright > 0 {
            $right_range = range_super_union($typcache, $right_range, $range);
        } else {
            $right_range = $range;
        }
        *(*$v).spl_right.add((*$v).spl_nright as usize) = $off;
        (*$v).spl_nright += 1;
    }};
}

/* Copy a RangeType datum (hardwires typbyval and typlen for ranges...) */
#[inline]
unsafe fn rangeCopy(r: *const RangeType) -> *mut RangeType {
    datumCopy(PointerGetDatum(r as *const c_void), false, -1) as *mut RangeType
}

/* GiST query consistency check */
pub unsafe fn range_gist_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let query: Datum = PG_GETARG_DATUM!(fcinfo, 1);
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 2) as StrategyNumber;
    let result: bool;
    let subtype: Oid = PG_GETARG_OID!(fcinfo, 3);
    let recheck = PG_GETARG_POINTER!(fcinfo, 4) as *mut bool;
    let key: *mut RangeType = DatumGetRangeTypeP((*entry).key);
    let typcache: *mut TypeCacheEntry;

    /* All operators served by this function are exact */
    *recheck = false;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(key));

    /*
     * Perform consistent checking using function corresponding to key type
     * (leaf or internal) and query subtype (range, multirange, or element).
     * Note that invalid subtype means that query type matches key type
     * (range).
     */
    if GIST_LEAF(entry) {
        if !OidIsValid(subtype) || subtype == ANYRANGEOID {
            result =
                range_gist_consistent_leaf_range(typcache, strategy, key, DatumGetRangeTypeP(query));
        } else if subtype == ANYMULTIRANGEOID {
            result = range_gist_consistent_leaf_multirange(
                typcache,
                strategy,
                key,
                DatumGetMultirangeTypeP(query),
            );
        } else {
            result = range_gist_consistent_leaf_element(typcache, strategy, key, query);
        }
    } else {
        if !OidIsValid(subtype) || subtype == ANYRANGEOID {
            result =
                range_gist_consistent_int_range(typcache, strategy, key, DatumGetRangeTypeP(query));
        } else if subtype == ANYMULTIRANGEOID {
            result = range_gist_consistent_int_multirange(
                typcache,
                strategy,
                key,
                DatumGetMultirangeTypeP(query),
            );
        } else {
            result = range_gist_consistent_int_element(typcache, strategy, key, query);
        }
    }
    PG_RETURN_BOOL!(result)
}

/*
 * GiST compress method for multiranges: multirange is approximated as union
 * range with no gaps.
 */
pub unsafe fn multirange_gist_compress(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;

    if (*entry).leafkey {
        let mr: *mut MultirangeType = DatumGetMultirangeTypeP((*entry).key);
        let r: *mut RangeType;
        let typcache: *mut TypeCacheEntry;
        let retval = palloc(core::mem::size_of::<GISTENTRY>()) as *mut GISTENTRY;

        typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));
        r = multirange_get_union_range((*typcache).rngtype, mr);

        gistentryinit(
            retval,
            RangeTypePGetDatum(r),
            (*entry).rel,
            (*entry).page,
            (*entry).offset,
            false,
        );

        PG_RETURN_POINTER!(retval)
    }

    PG_RETURN_POINTER!(entry)
}

/* GiST query consistency check for multiranges */
pub unsafe fn multirange_gist_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let query: Datum = PG_GETARG_DATUM!(fcinfo, 1);
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 2) as StrategyNumber;
    let result: bool;
    let subtype: Oid = PG_GETARG_OID!(fcinfo, 3);
    let recheck = PG_GETARG_POINTER!(fcinfo, 4) as *mut bool;
    let key: *mut RangeType = DatumGetRangeTypeP((*entry).key);
    let typcache: *mut TypeCacheEntry;

    /*
     * All operators served by this function are inexact because multirange is
     * approximated by union range with no gaps.
     */
    *recheck = true;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(key));

    /*
     * Perform consistent checking using function corresponding to key type
     * (leaf or internal) and query subtype (range, multirange, or element).
     * Note that invalid subtype means that query type matches key type
     * (multirange).
     */
    if GIST_LEAF(entry) {
        if !OidIsValid(subtype) || subtype == ANYMULTIRANGEOID {
            result = range_gist_consistent_leaf_multirange(
                typcache,
                strategy,
                key,
                DatumGetMultirangeTypeP(query),
            );
        } else if subtype == ANYRANGEOID {
            result =
                range_gist_consistent_leaf_range(typcache, strategy, key, DatumGetRangeTypeP(query));
        } else {
            result = range_gist_consistent_leaf_element(typcache, strategy, key, query);
        }
    } else {
        if !OidIsValid(subtype) || subtype == ANYMULTIRANGEOID {
            result = range_gist_consistent_int_multirange(
                typcache,
                strategy,
                key,
                DatumGetMultirangeTypeP(query),
            );
        } else if subtype == ANYRANGEOID {
            result =
                range_gist_consistent_int_range(typcache, strategy, key, DatumGetRangeTypeP(query));
        } else {
            result = range_gist_consistent_int_element(typcache, strategy, key, query);
        }
    }
    PG_RETURN_BOOL!(result)
}

/* form union range */
pub unsafe fn range_gist_union(fcinfo: FunctionCallInfo) -> Datum {
    let entryvec = PG_GETARG_POINTER!(fcinfo, 0) as *mut GistEntryVector;
    let ent = (*entryvec).vector.as_ptr() as *mut GISTENTRY;
    let mut result_range: *mut RangeType;
    let typcache: *mut TypeCacheEntry;
    let mut i: c_int;

    result_range = DatumGetRangeTypeP((*ent.add(0)).key);

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(result_range));

    i = 1;
    while i < (*entryvec).n {
        result_range =
            range_super_union(typcache, result_range, DatumGetRangeTypeP((*ent.add(i as usize)).key));
        i += 1;
    }

    PG_RETURN_RANGE_P!(result_range)
}

/*
 * We store ranges as ranges in GiST indexes, so we do not need
 * compress, decompress, or fetch functions.  Note this implies a limit
 * on the size of range values that can be indexed.
 */

/*
 * GiST page split penalty function.
 *
 * The penalty function has the following goals (in order from most to least
 * important):
 * - Keep normal ranges separate
 * - Avoid broadening the class of the original predicate
 * - Avoid broadening (as determined by subtype_diff) the original predicate
 * - Favor adding ranges to narrower original predicates
 */
pub unsafe fn range_gist_penalty(fcinfo: FunctionCallInfo) -> Datum {
    let origentry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let newentry = PG_GETARG_POINTER!(fcinfo, 1) as *mut GISTENTRY;
    let penalty = PG_GETARG_POINTER!(fcinfo, 2) as *mut f32;
    let orig: *mut RangeType = DatumGetRangeTypeP((*origentry).key);
    let new: *mut RangeType = DatumGetRangeTypeP((*newentry).key);
    let typcache: *mut TypeCacheEntry;
    let has_subtype_diff: bool;
    let mut orig_lower: RangeBound = core::mem::zeroed();
    let mut new_lower: RangeBound = core::mem::zeroed();
    let mut orig_upper: RangeBound = core::mem::zeroed();
    let mut new_upper: RangeBound = core::mem::zeroed();
    let mut orig_empty: bool = false;
    let mut new_empty: bool = false;

    if RangeTypeGetOid(orig) != RangeTypeGetOid(new) {
        elog!(ERROR, "range types do not match");
    }

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(orig));

    has_subtype_diff = OidIsValid((*typcache).rng_subdiff_finfo.fn_oid);

    range_deserialize(typcache, orig, &mut orig_lower, &mut orig_upper, &mut orig_empty);
    range_deserialize(typcache, new, &mut new_lower, &mut new_upper, &mut new_empty);

    /*
     * Distinct branches for handling distinct classes of ranges.  Note that
     * penalty values only need to be commensurate within the same class of
     * new range.
     */
    if new_empty {
        /* Handle insertion of empty range */
        if orig_empty {
            /*
             * The best case is to insert it to empty original range.
             * Insertion here means no broadening of original range. Also
             * original range is the most narrow.
             */
            *penalty = 0.0;
        } else if RangeIsOrContainsEmpty(orig) {
            /*
             * The second case is to insert empty range into range which
             * contains at least one underlying empty range.  There is still
             * no broadening of original range, but original range is not as
             * narrow as possible.
             */
            *penalty = CONTAIN_EMPTY_PENALTY;
        } else if orig_lower.infinite && orig_upper.infinite {
            /*
             * Original range requires broadening.  (-inf; +inf) is most far
             * from normal range in this case.
             */
            *penalty = 2.0 * CONTAIN_EMPTY_PENALTY;
        } else if orig_lower.infinite || orig_upper.infinite {
            /*
             * (-inf, x) or (x, +inf) original ranges are closer to normal
             * ranges, so it's worse to mix it with empty ranges.
             */
            *penalty = 3.0 * CONTAIN_EMPTY_PENALTY;
        } else {
            /*
             * The least preferred case is broadening of normal range.
             */
            *penalty = 4.0 * CONTAIN_EMPTY_PENALTY;
        }
    } else if new_lower.infinite && new_upper.infinite {
        /* Handle insertion of (-inf, +inf) range */
        if orig_lower.infinite && orig_upper.infinite {
            /*
             * Best case is inserting to (-inf, +inf) original range.
             */
            *penalty = 0.0;
        } else if orig_lower.infinite || orig_upper.infinite {
            /*
             * When original range is (-inf, x) or (x, +inf) it requires
             * broadening of original range (extension of one bound to
             * infinity).
             */
            *penalty = INFINITE_BOUND_PENALTY;
        } else {
            /*
             * Insertion to normal original range is least preferred.
             */
            *penalty = 2.0 * INFINITE_BOUND_PENALTY;
        }

        if RangeIsOrContainsEmpty(orig) {
            /*
             * Original range is narrower when it doesn't contain empty
             * ranges. Add additional penalty otherwise.
             */
            *penalty += CONTAIN_EMPTY_PENALTY;
        }
    } else if new_lower.infinite {
        /* Handle insertion of (-inf, x) range */
        if !orig_empty && orig_lower.infinite {
            if orig_upper.infinite {
                /*
                 * (-inf, +inf) range won't be extended by insertion of (-inf,
                 * x) range. It's a less desirable case than insertion to
                 * (-inf, y) original range without extension, because in that
                 * case original range is narrower. But we can't express that
                 * in single float value.
                 */
                *penalty = 0.0;
            } else {
                if range_cmp_bounds(typcache, &new_upper, &orig_upper) > 0 {
                    /*
                     * Get extension of original range using subtype_diff. Use
                     * constant if subtype_diff unavailable.
                     */
                    if has_subtype_diff {
                        *penalty =
                            call_subtype_diff(typcache, new_upper.val, orig_upper.val) as f32;
                    } else {
                        *penalty = DEFAULT_SUBTYPE_DIFF_PENALTY;
                    }
                } else {
                    /* No extension of original range */
                    *penalty = 0.0;
                }
            }
        } else {
            /*
             * If lower bound of original range is not -inf, then extension of
             * it is infinity.
             */
            *penalty = get_float4_infinity();
        }
    } else if new_upper.infinite {
        /* Handle insertion of (x, +inf) range */
        if !orig_empty && orig_upper.infinite {
            if orig_lower.infinite {
                /*
                 * (-inf, +inf) range won't be extended by insertion of (x,
                 * +inf) range. It's a less desirable case than insertion to
                 * (y, +inf) original range without extension, because in that
                 * case original range is narrower. But we can't express that
                 * in single float value.
                 */
                *penalty = 0.0;
            } else {
                if range_cmp_bounds(typcache, &new_lower, &orig_lower) < 0 {
                    /*
                     * Get extension of original range using subtype_diff. Use
                     * constant if subtype_diff unavailable.
                     */
                    if has_subtype_diff {
                        *penalty =
                            call_subtype_diff(typcache, orig_lower.val, new_lower.val) as f32;
                    } else {
                        *penalty = DEFAULT_SUBTYPE_DIFF_PENALTY;
                    }
                } else {
                    /* No extension of original range */
                    *penalty = 0.0;
                }
            }
        } else {
            /*
             * If upper bound of original range is not +inf, then extension of
             * it is infinity.
             */
            *penalty = get_float4_infinity();
        }
    } else {
        /* Handle insertion of normal (non-empty, non-infinite) range */
        if orig_empty || orig_lower.infinite || orig_upper.infinite {
            /*
             * Avoid mixing normal ranges with infinite and empty ranges.
             */
            *penalty = get_float4_infinity();
        } else {
            /*
             * Calculate extension of original range by calling subtype_diff.
             * Use constant if subtype_diff unavailable.
             */
            let mut diff: f64 = 0.0;

            if range_cmp_bounds(typcache, &new_lower, &orig_lower) < 0 {
                if has_subtype_diff {
                    diff += call_subtype_diff(typcache, orig_lower.val, new_lower.val);
                } else {
                    diff += DEFAULT_SUBTYPE_DIFF_PENALTY as f64;
                }
            }
            if range_cmp_bounds(typcache, &new_upper, &orig_upper) > 0 {
                if has_subtype_diff {
                    diff += call_subtype_diff(typcache, new_upper.val, orig_upper.val);
                } else {
                    diff += DEFAULT_SUBTYPE_DIFF_PENALTY as f64;
                }
            }
            *penalty = diff as f32;
        }
    }

    PG_RETURN_POINTER!(penalty)
}

/*
 * The GiST PickSplit method for ranges
 *
 * Primarily, we try to segregate ranges of different classes.  If splitting
 * ranges of the same class, use the appropriate split method for that class.
 */
pub unsafe fn range_gist_picksplit(fcinfo: FunctionCallInfo) -> Datum {
    let entryvec = PG_GETARG_POINTER!(fcinfo, 0) as *mut GistEntryVector;
    let v = PG_GETARG_POINTER!(fcinfo, 1) as *mut GIST_SPLITVEC;
    let typcache: *mut TypeCacheEntry;
    let mut i: OffsetNumber;
    let pred_left: *mut RangeType;
    let nbytes: c_int;
    let maxoff: OffsetNumber;
    let mut count_in_classes: [c_int; CLS_COUNT] = [0; CLS_COUNT];
    let mut j: c_int;
    let mut non_empty_classes_count: c_int = 0;
    let mut biggest_class: c_int = -1;
    let mut biggest_class_count: c_int = 0;
    let total_count: c_int;

    /* use first item to look up range type's info */
    pred_left = DatumGetRangeTypeP((*(*entryvec).entry(FirstOffsetNumber as usize)).key);
    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(pred_left));

    maxoff = ((*entryvec).n - 1) as OffsetNumber;
    nbytes = (maxoff as c_int + 1) * core::mem::size_of::<OffsetNumber>() as c_int;
    (*v).spl_left = palloc(nbytes as usize) as *mut OffsetNumber;
    (*v).spl_right = palloc(nbytes as usize) as *mut OffsetNumber;

    /*
     * Get count distribution of range classes.
     */
    memset(
        count_in_classes.as_mut_ptr() as *mut c_void,
        0,
        core::mem::size_of::<[c_int; CLS_COUNT]>(),
    );
    i = FirstOffsetNumber;
    while i <= maxoff {
        let range: *mut RangeType = DatumGetRangeTypeP((*(*entryvec).entry(i as usize)).key);

        count_in_classes[get_gist_range_class(range) as usize] += 1;
        i = OffsetNumberNext(i);
    }

    /*
     * Count non-empty classes and find biggest class.
     */
    total_count = maxoff as c_int;
    j = 0;
    while j < CLS_COUNT as c_int {
        if count_in_classes[j as usize] > 0 {
            if count_in_classes[j as usize] > biggest_class_count {
                biggest_class_count = count_in_classes[j as usize];
                biggest_class = j;
            }
            non_empty_classes_count += 1;
        }
        j += 1;
    }

    Assert!(non_empty_classes_count > 0);

    if non_empty_classes_count == 1 {
        /* One non-empty class, so split inside class */
        if (biggest_class & !(CLS_CONTAIN_EMPTY as c_int)) == CLS_NORMAL as c_int {
            /* double sorting split for normal ranges */
            range_gist_double_sorting_split(typcache, entryvec, v);
        } else if (biggest_class & !(CLS_CONTAIN_EMPTY as c_int)) == CLS_LOWER_INF as c_int {
            /* upper bound sorting split for (-inf, x) ranges */
            range_gist_single_sorting_split(typcache, entryvec, v, true);
        } else if (biggest_class & !(CLS_CONTAIN_EMPTY as c_int)) == CLS_UPPER_INF as c_int {
            /* lower bound sorting split for (x, +inf) ranges */
            range_gist_single_sorting_split(typcache, entryvec, v, false);
        } else {
            /* trivial split for all (-inf, +inf) or all empty ranges */
            range_gist_fallback_split(typcache, entryvec, v);
        }
    } else {
        /*
         * Class based split.
         *
         * To which side of the split should each class go?  Initialize them
         * all to go to the left side.
         */
        let mut classes_groups: [SplitLR; CLS_COUNT] = [SPLIT_LEFT; CLS_COUNT];

        memset(
            classes_groups.as_mut_ptr() as *mut c_void,
            0,
            core::mem::size_of::<[SplitLR; CLS_COUNT]>(),
        );

        if count_in_classes[CLS_NORMAL] > 0 {
            /* separate normal ranges if any */
            classes_groups[CLS_NORMAL] = SPLIT_RIGHT;
        } else {
            /*----------
             * Try to split classes in one of two ways:
             *	1) containing infinities - not containing infinities
             *	2) containing empty - not containing empty
             *
             * Select the way which balances the ranges between left and right
             * the best. If split in these ways is not possible, there are at
             * most 3 classes, so just separate biggest class.
             *----------
             */
            let inf_count: c_int;
            let non_inf_count: c_int;
            let empty_count: c_int;
            let non_empty_count: c_int;

            non_inf_count = count_in_classes[CLS_NORMAL]
                + count_in_classes[CLS_CONTAIN_EMPTY]
                + count_in_classes[CLS_EMPTY];
            inf_count = total_count - non_inf_count;

            non_empty_count = count_in_classes[CLS_NORMAL]
                + count_in_classes[CLS_LOWER_INF]
                + count_in_classes[CLS_UPPER_INF]
                + count_in_classes[CLS_LOWER_INF | CLS_UPPER_INF];
            empty_count = total_count - non_empty_count;

            if inf_count > 0
                && non_inf_count > 0
                && (abs(inf_count - non_inf_count) <= abs(empty_count - non_empty_count))
            {
                classes_groups[CLS_NORMAL] = SPLIT_RIGHT;
                classes_groups[CLS_CONTAIN_EMPTY] = SPLIT_RIGHT;
                classes_groups[CLS_EMPTY] = SPLIT_RIGHT;
            } else if empty_count > 0 && non_empty_count > 0 {
                classes_groups[CLS_NORMAL] = SPLIT_RIGHT;
                classes_groups[CLS_LOWER_INF] = SPLIT_RIGHT;
                classes_groups[CLS_UPPER_INF] = SPLIT_RIGHT;
                classes_groups[CLS_LOWER_INF | CLS_UPPER_INF] = SPLIT_RIGHT;
            } else {
                /*
                 * Either total_count == emptyCount or total_count ==
                 * infCount.
                 */
                classes_groups[biggest_class as usize] = SPLIT_RIGHT;
            }
        }

        range_gist_class_split(typcache, entryvec, v, classes_groups.as_mut_ptr());
    }

    PG_RETURN_POINTER!(v)
}

/* equality comparator for GiST */
pub unsafe fn range_gist_same(fcinfo: FunctionCallInfo) -> Datum {
    let r1: *mut RangeType = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2: *mut RangeType = PG_GETARG_RANGE_P!(fcinfo, 1);
    let result = PG_GETARG_POINTER!(fcinfo, 2) as *mut bool;

    /*
     * range_eq will ignore the RANGE_CONTAIN_EMPTY flag, so we have to check
     * that for ourselves.  More generally, if the entries have been properly
     * normalized, then unequal flags bytes must mean unequal ranges ... so
     * let's just test all the flag bits at once.
     */
    if range_get_flags(r1) != range_get_flags(r2) {
        *result = false;
    } else {
        let typcache: *mut TypeCacheEntry;

        typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

        *result = range_eq_internal(typcache, r1, r2);
    }

    PG_RETURN_POINTER!(result)
}

/*
 *----------------------------------------------------------
 * STATIC FUNCTIONS
 *----------------------------------------------------------
 */

/*
 * Return the smallest range that contains r1 and r2
 *
 * This differs from regular range_union in two critical ways:
 * 1. It won't throw an error for non-adjacent r1 and r2, but just absorb
 * the intervening values into the result range.
 * 2. We track whether any empty range has been union'd into the result,
 * so that contained_by searches can be indexed.  Note that this means
 * that *all* unions formed within the GiST index must go through here.
 */
unsafe fn range_super_union(
    typcache: *mut TypeCacheEntry,
    r1: *mut RangeType,
    r2: *mut RangeType,
) -> *mut RangeType {
    let result: *mut RangeType;
    let mut lower1: RangeBound = core::mem::zeroed();
    let mut lower2: RangeBound = core::mem::zeroed();
    let mut upper1: RangeBound = core::mem::zeroed();
    let mut upper2: RangeBound = core::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;
    let flags1: c_char;
    let flags2: c_char;
    let result_lower: *mut RangeBound;
    let result_upper: *mut RangeBound;
    let mut r1 = r1;
    let mut r2 = r2;

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);
    flags1 = range_get_flags(r1);
    flags2 = range_get_flags(r2);

    if empty1 {
        /* We can return r2 as-is if it already is or contains empty */
        if (flags2 & (RANGE_EMPTY | RANGE_CONTAIN_EMPTY)) != 0 {
            return r2;
        }
        /* Else we'd better copy it (modify-in-place isn't safe) */
        r2 = rangeCopy(r2);
        range_set_contain_empty(r2);
        return r2;
    }
    if empty2 {
        /* We can return r1 as-is if it already is or contains empty */
        if (flags1 & (RANGE_EMPTY | RANGE_CONTAIN_EMPTY)) != 0 {
            return r1;
        }
        /* Else we'd better copy it (modify-in-place isn't safe) */
        r1 = rangeCopy(r1);
        range_set_contain_empty(r1);
        return r1;
    }

    if range_cmp_bounds(typcache, &lower1, &lower2) <= 0 {
        result_lower = &mut lower1;
    } else {
        result_lower = &mut lower2;
    }

    if range_cmp_bounds(typcache, &upper1, &upper2) >= 0 {
        result_upper = &mut upper1;
    } else {
        result_upper = &mut upper2;
    }

    /* optimization to avoid constructing a new range */
    if result_lower == &mut lower1
        && result_upper == &mut upper1
        && ((flags1 & RANGE_CONTAIN_EMPTY) != 0 || (flags2 & RANGE_CONTAIN_EMPTY) == 0)
    {
        return r1;
    }
    if result_lower == &mut lower2
        && result_upper == &mut upper2
        && ((flags2 & RANGE_CONTAIN_EMPTY) != 0 || (flags1 & RANGE_CONTAIN_EMPTY) == 0)
    {
        return r2;
    }

    result = make_range(typcache, result_lower, result_upper, false, null_mut());

    if (flags1 & RANGE_CONTAIN_EMPTY) != 0 || (flags2 & RANGE_CONTAIN_EMPTY) != 0 {
        range_set_contain_empty(result);
    }

    result
}

unsafe fn multirange_union_range_equal(
    typcache: *mut TypeCacheEntry,
    r: *const RangeType,
    mr: *const MultirangeType,
) -> bool {
    let mut lower1: RangeBound = core::mem::zeroed();
    let mut upper1: RangeBound = core::mem::zeroed();
    let mut lower2: RangeBound = core::mem::zeroed();
    let mut upper2: RangeBound = core::mem::zeroed();
    let mut tmp: RangeBound = core::mem::zeroed();
    let mut empty: bool = false;

    if RangeIsEmpty(r) || MultirangeIsEmpty(mr) {
        return RangeIsEmpty(r) && MultirangeIsEmpty(mr);
    }

    range_deserialize(typcache, r, &mut lower1, &mut upper1, &mut empty);
    Assert!(!empty);
    multirange_get_bounds((*typcache).rngtype, mr, 0, &mut lower2, &mut tmp);
    multirange_get_bounds(
        (*typcache).rngtype,
        mr,
        (*mr).rangeCount - 1,
        &mut tmp,
        &mut upper2,
    );

    range_cmp_bounds(typcache, &lower1, &lower2) == 0
        && range_cmp_bounds(typcache, &upper1, &upper2) == 0
}

/*
 * GiST consistent test on an index internal page with range query
 */
unsafe fn range_gist_consistent_int_range(
    typcache: *mut TypeCacheEntry,
    strategy: StrategyNumber,
    key: *const RangeType,
    query: *const RangeType,
) -> bool {
    match strategy {
        RANGESTRAT_BEFORE => {
            if RangeIsEmpty(key) || RangeIsEmpty(query) {
                return false;
            }
            !range_overright_internal(typcache, key, query)
        }
        RANGESTRAT_OVERLEFT => {
            if RangeIsEmpty(key) || RangeIsEmpty(query) {
                return false;
            }
            !range_after_internal(typcache, key, query)
        }
        RANGESTRAT_OVERLAPS => range_overlaps_internal(typcache, key, query),
        RANGESTRAT_OVERRIGHT => {
            if RangeIsEmpty(key) || RangeIsEmpty(query) {
                return false;
            }
            !range_before_internal(typcache, key, query)
        }
        RANGESTRAT_AFTER => {
            if RangeIsEmpty(key) || RangeIsEmpty(query) {
                return false;
            }
            !range_overleft_internal(typcache, key, query)
        }
        RANGESTRAT_ADJACENT => {
            if RangeIsEmpty(key) || RangeIsEmpty(query) {
                return false;
            }
            if range_adjacent_internal(typcache, key, query) {
                return true;
            }
            range_overlaps_internal(typcache, key, query)
        }
        RANGESTRAT_CONTAINS => range_contains_internal(typcache, key, query),
        RANGESTRAT_CONTAINED_BY => {
            /*
             * Empty ranges are contained by anything, so if key is or
             * contains any empty ranges, we must descend into it.  Otherwise,
             * descend only if key overlaps the query.
             */
            if RangeIsOrContainsEmpty(key) {
                return true;
            }
            range_overlaps_internal(typcache, key, query)
        }
        RANGESTRAT_EQ => {
            /*
             * If query is empty, descend only if the key is or contains any
             * empty ranges.  Otherwise, descend if key contains query.
             */
            if RangeIsEmpty(query) {
                return RangeIsOrContainsEmpty(key);
            }
            range_contains_internal(typcache, key, query)
        }
        _ => {
            elog!(ERROR, "unrecognized range strategy: {}", strategy);
            false /* keep compiler quiet */
        }
    }
}

/*
 * GiST consistent test on an index internal page with multirange query
 */
unsafe fn range_gist_consistent_int_multirange(
    typcache: *mut TypeCacheEntry,
    strategy: StrategyNumber,
    key: *const RangeType,
    query: *const MultirangeType,
) -> bool {
    match strategy {
        RANGESTRAT_BEFORE => {
            if RangeIsEmpty(key) || MultirangeIsEmpty(query) {
                return false;
            }
            !range_overright_multirange_internal(typcache, key, query)
        }
        RANGESTRAT_OVERLEFT => {
            if RangeIsEmpty(key) || MultirangeIsEmpty(query) {
                return false;
            }
            !range_after_multirange_internal(typcache, key, query)
        }
        RANGESTRAT_OVERLAPS => range_overlaps_multirange_internal(typcache, key, query),
        RANGESTRAT_OVERRIGHT => {
            if RangeIsEmpty(key) || MultirangeIsEmpty(query) {
                return false;
            }
            !range_before_multirange_internal(typcache, key, query)
        }
        RANGESTRAT_AFTER => {
            if RangeIsEmpty(key) || MultirangeIsEmpty(query) {
                return false;
            }
            !range_overleft_multirange_internal(typcache, key, query)
        }
        RANGESTRAT_ADJACENT => {
            if RangeIsEmpty(key) || MultirangeIsEmpty(query) {
                return false;
            }
            if range_adjacent_multirange_internal(typcache, key, query) {
                return true;
            }
            range_overlaps_multirange_internal(typcache, key, query)
        }
        RANGESTRAT_CONTAINS => range_contains_multirange_internal(typcache, key, query),
        RANGESTRAT_CONTAINED_BY => {
            /*
             * Empty ranges are contained by anything, so if key is or
             * contains any empty ranges, we must descend into it.  Otherwise,
             * descend only if key overlaps the query.
             */
            if RangeIsOrContainsEmpty(key) {
                return true;
            }
            range_overlaps_multirange_internal(typcache, key, query)
        }
        RANGESTRAT_EQ => {
            /*
             * If query is empty, descend only if the key is or contains any
             * empty ranges.  Otherwise, descend if key contains query.
             */
            if MultirangeIsEmpty(query) {
                return RangeIsOrContainsEmpty(key);
            }
            range_contains_multirange_internal(typcache, key, query)
        }
        _ => {
            elog!(ERROR, "unrecognized range strategy: {}", strategy);
            false /* keep compiler quiet */
        }
    }
}

/*
 * GiST consistent test on an index internal page with element query
 */
unsafe fn range_gist_consistent_int_element(
    typcache: *mut TypeCacheEntry,
    strategy: StrategyNumber,
    key: *const RangeType,
    query: Datum,
) -> bool {
    match strategy {
        RANGESTRAT_CONTAINS_ELEM => range_contains_elem_internal(typcache, key, query),
        _ => {
            elog!(ERROR, "unrecognized range strategy: {}", strategy);
            false /* keep compiler quiet */
        }
    }
}

/*
 * GiST consistent test on an index leaf page with range query
 */
unsafe fn range_gist_consistent_leaf_range(
    typcache: *mut TypeCacheEntry,
    strategy: StrategyNumber,
    key: *const RangeType,
    query: *const RangeType,
) -> bool {
    match strategy {
        RANGESTRAT_BEFORE => range_before_internal(typcache, key, query),
        RANGESTRAT_OVERLEFT => range_overleft_internal(typcache, key, query),
        RANGESTRAT_OVERLAPS => range_overlaps_internal(typcache, key, query),
        RANGESTRAT_OVERRIGHT => range_overright_internal(typcache, key, query),
        RANGESTRAT_AFTER => range_after_internal(typcache, key, query),
        RANGESTRAT_ADJACENT => range_adjacent_internal(typcache, key, query),
        RANGESTRAT_CONTAINS => range_contains_internal(typcache, key, query),
        RANGESTRAT_CONTAINED_BY => range_contained_by_internal(typcache, key, query),
        RANGESTRAT_EQ => range_eq_internal(typcache, key, query),
        _ => {
            elog!(ERROR, "unrecognized range strategy: {}", strategy);
            false /* keep compiler quiet */
        }
    }
}

/*
 * GiST consistent test on an index leaf page with multirange query
 */
unsafe fn range_gist_consistent_leaf_multirange(
    typcache: *mut TypeCacheEntry,
    strategy: StrategyNumber,
    key: *const RangeType,
    query: *const MultirangeType,
) -> bool {
    match strategy {
        RANGESTRAT_BEFORE => range_before_multirange_internal(typcache, key, query),
        RANGESTRAT_OVERLEFT => range_overleft_multirange_internal(typcache, key, query),
        RANGESTRAT_OVERLAPS => range_overlaps_multirange_internal(typcache, key, query),
        RANGESTRAT_OVERRIGHT => range_overright_multirange_internal(typcache, key, query),
        RANGESTRAT_AFTER => range_after_multirange_internal(typcache, key, query),
        RANGESTRAT_ADJACENT => range_adjacent_multirange_internal(typcache, key, query),
        RANGESTRAT_CONTAINS => range_contains_multirange_internal(typcache, key, query),
        RANGESTRAT_CONTAINED_BY => multirange_contains_range_internal(typcache, query, key),
        RANGESTRAT_EQ => multirange_union_range_equal(typcache, key, query),
        _ => {
            elog!(ERROR, "unrecognized range strategy: {}", strategy);
            false /* keep compiler quiet */
        }
    }
}

/*
 * GiST consistent test on an index leaf page with element query
 */
unsafe fn range_gist_consistent_leaf_element(
    typcache: *mut TypeCacheEntry,
    strategy: StrategyNumber,
    key: *const RangeType,
    query: Datum,
) -> bool {
    match strategy {
        RANGESTRAT_CONTAINS_ELEM => range_contains_elem_internal(typcache, key, query),
        _ => {
            elog!(ERROR, "unrecognized range strategy: {}", strategy);
            false /* keep compiler quiet */
        }
    }
}

/*
 * Trivial split: half of entries will be placed on one page
 * and the other half on the other page.
 */
unsafe fn range_gist_fallback_split(
    typcache: *mut TypeCacheEntry,
    entryvec: *mut GistEntryVector,
    v: *mut GIST_SPLITVEC,
) {
    let mut left_range: *mut RangeType = null_mut();
    let mut right_range: *mut RangeType = null_mut();
    let mut i: OffsetNumber;
    let maxoff: OffsetNumber;
    let split_idx: OffsetNumber;

    maxoff = ((*entryvec).n - 1) as OffsetNumber;
    /* Split entries before this to left page, after to right: */
    split_idx = (maxoff - FirstOffsetNumber) / 2 + FirstOffsetNumber;

    (*v).spl_nleft = 0;
    (*v).spl_nright = 0;
    i = FirstOffsetNumber;
    while i <= maxoff {
        let range: *mut RangeType = DatumGetRangeTypeP((*(*entryvec).entry(i as usize)).key);

        if i < split_idx {
            PLACE_LEFT!(v, typcache, left_range, range, i);
        } else {
            PLACE_RIGHT!(v, typcache, right_range, range, i);
        }
        i += 1;
    }

    (*v).spl_ldatum = RangeTypePGetDatum(left_range);
    (*v).spl_rdatum = RangeTypePGetDatum(right_range);
}

/*
 * Split based on classes of ranges.
 *
 * See get_gist_range_class for class definitions.
 * classes_groups is an array of length CLS_COUNT indicating the side of the
 * split to which each class should go.
 */
unsafe fn range_gist_class_split(
    typcache: *mut TypeCacheEntry,
    entryvec: *mut GistEntryVector,
    v: *mut GIST_SPLITVEC,
    classes_groups: *mut SplitLR,
) {
    let mut left_range: *mut RangeType = null_mut();
    let mut right_range: *mut RangeType = null_mut();
    let mut i: OffsetNumber;
    let maxoff: OffsetNumber;

    maxoff = ((*entryvec).n - 1) as OffsetNumber;

    (*v).spl_nleft = 0;
    (*v).spl_nright = 0;
    i = FirstOffsetNumber;
    while i <= maxoff {
        let range: *mut RangeType = DatumGetRangeTypeP((*(*entryvec).entry(i as usize)).key);
        let class: c_int;

        /* Get class of range */
        class = get_gist_range_class(range);

        /* Place range to appropriate page */
        if *classes_groups.add(class as usize) == SPLIT_LEFT {
            PLACE_LEFT!(v, typcache, left_range, range, i);
        } else {
            Assert!(*classes_groups.add(class as usize) == SPLIT_RIGHT);
            PLACE_RIGHT!(v, typcache, right_range, range, i);
        }
        i = OffsetNumberNext(i);
    }

    (*v).spl_ldatum = RangeTypePGetDatum(left_range);
    (*v).spl_rdatum = RangeTypePGetDatum(right_range);
}

/*
 * Sorting based split. First half of entries according to the sort will be
 * placed to one page, and second half of entries will be placed to other
 * page. use_upper_bound parameter indicates whether to use upper or lower
 * bound for sorting.
 */
unsafe fn range_gist_single_sorting_split(
    typcache: *mut TypeCacheEntry,
    entryvec: *mut GistEntryVector,
    v: *mut GIST_SPLITVEC,
    use_upper_bound: bool,
) {
    let sort_items: *mut SingleBoundSortItem;
    let mut left_range: *mut RangeType = null_mut();
    let mut right_range: *mut RangeType = null_mut();
    let mut i: OffsetNumber;
    let maxoff: OffsetNumber;
    let split_idx: OffsetNumber;

    maxoff = ((*entryvec).n - 1) as OffsetNumber;

    sort_items = palloc(maxoff as usize * core::mem::size_of::<SingleBoundSortItem>())
        as *mut SingleBoundSortItem;

    /*
     * Prepare auxiliary array and sort the values.
     */
    i = FirstOffsetNumber;
    while i <= maxoff {
        let range: *mut RangeType = DatumGetRangeTypeP((*(*entryvec).entry(i as usize)).key);
        let mut bound2: RangeBound = core::mem::zeroed();
        let mut empty: bool = false;

        (*sort_items.add((i - 1) as usize)).index = i as c_int;
        /* Put appropriate bound into array */
        if use_upper_bound {
            range_deserialize(
                typcache,
                range,
                &mut bound2,
                &mut (*sort_items.add((i - 1) as usize)).bound,
                &mut empty,
            );
        } else {
            range_deserialize(
                typcache,
                range,
                &mut (*sort_items.add((i - 1) as usize)).bound,
                &mut bound2,
                &mut empty,
            );
        }
        Assert!(!empty);
        i = OffsetNumberNext(i);
    }

    qsort_arg(
        sort_items as *mut c_void,
        maxoff as usize,
        core::mem::size_of::<SingleBoundSortItem>(),
        single_bound_cmp,
        typcache as *mut c_void,
    );

    split_idx = maxoff / 2;

    (*v).spl_nleft = 0;
    (*v).spl_nright = 0;

    i = 0;
    while i < maxoff {
        let idx: c_int = (*sort_items.add(i as usize)).index;
        let range: *mut RangeType = DatumGetRangeTypeP((*(*entryvec).entry(idx as usize)).key);

        if i < split_idx {
            PLACE_LEFT!(v, typcache, left_range, range, idx as OffsetNumber);
        } else {
            PLACE_RIGHT!(v, typcache, right_range, range, idx as OffsetNumber);
        }
        i += 1;
    }

    (*v).spl_ldatum = RangeTypePGetDatum(left_range);
    (*v).spl_rdatum = RangeTypePGetDatum(right_range);
}

/*
 * Double sorting split algorithm.
 *
 * The algorithm considers dividing ranges into two groups. The first (left)
 * group contains general left bound. The second (right) group contains
 * general right bound. The challenge is to find upper bound of left group
 * and lower bound of right group so that overlap of groups is minimal and
 * ratio of distribution is acceptable. Algorithm finds for each lower bound of
 * right group minimal upper bound of left group, and for each upper bound of
 * left group maximal lower bound of right group. For each found pair
 * range_gist_consider_split considers replacement of currently selected
 * split with the new one.
 *
 * After that, all the entries are divided into three groups:
 * 1) Entries which should be placed to the left group
 * 2) Entries which should be placed to the right group
 * 3) "Common entries" which can be placed to either group without affecting
 *	  amount of overlap.
 *
 * The common ranges are distributed by difference of distance from lower
 * bound of common range to lower bound of right group and distance from upper
 * bound of common range to upper bound of left group.
 *
 * For details see:
 * "A new double sorting-based node splitting algorithm for R-tree",
 * A. Korotkov
 * http://syrcose.ispras.ru/2011/files/SYRCoSE2011_Proceedings.pdf#page=36
 */
unsafe fn range_gist_double_sorting_split(
    typcache: *mut TypeCacheEntry,
    entryvec: *mut GistEntryVector,
    v: *mut GIST_SPLITVEC,
) {
    let mut context: ConsiderSplitContext = core::mem::zeroed();
    let mut i: OffsetNumber;
    let maxoff: OffsetNumber;
    let mut left_range: *mut RangeType = null_mut();
    let mut right_range: *mut RangeType = null_mut();
    let mut common_entries_count: c_int;
    let by_lower: *mut NonEmptyRange;
    let by_upper: *mut NonEmptyRange;
    let common_entries: *mut CommonEntry;
    let nentries: c_int;
    let mut i1: c_int;
    let mut i2: c_int;
    let mut right_lower: *mut RangeBound;
    let mut left_upper: *mut RangeBound;

    memset(
        &mut context as *mut ConsiderSplitContext as *mut c_void,
        0,
        core::mem::size_of::<ConsiderSplitContext>(),
    );
    context.typcache = typcache;
    context.has_subtype_diff = OidIsValid((*typcache).rng_subdiff_finfo.fn_oid);

    maxoff = ((*entryvec).n - 1) as OffsetNumber;
    nentries = maxoff as c_int - FirstOffsetNumber as c_int + 1;
    context.entries_count = nentries;
    context.first = true;

    /* Allocate arrays for sorted range bounds */
    by_lower = palloc(nentries as usize * core::mem::size_of::<NonEmptyRange>())
        as *mut NonEmptyRange;
    by_upper = palloc(nentries as usize * core::mem::size_of::<NonEmptyRange>())
        as *mut NonEmptyRange;

    /* Fill arrays of bounds */
    i = FirstOffsetNumber;
    while i <= maxoff {
        let range: *mut RangeType = DatumGetRangeTypeP((*(*entryvec).entry(i as usize)).key);
        let mut empty: bool = false;

        range_deserialize(
            typcache,
            range,
            &mut (*by_lower.add((i - FirstOffsetNumber) as usize)).lower,
            &mut (*by_lower.add((i - FirstOffsetNumber) as usize)).upper,
            &mut empty,
        );
        Assert!(!empty);
        i = OffsetNumberNext(i);
    }

    /*
     * Make two arrays of range bounds: one sorted by lower bound and another
     * sorted by upper bound.
     */
    memcpy(
        by_upper as *mut c_void,
        by_lower as *const c_void,
        nentries as usize * core::mem::size_of::<NonEmptyRange>(),
    );
    qsort_arg(
        by_lower as *mut c_void,
        nentries as usize,
        core::mem::size_of::<NonEmptyRange>(),
        interval_cmp_lower,
        typcache as *mut c_void,
    );
    qsort_arg(
        by_upper as *mut c_void,
        nentries as usize,
        core::mem::size_of::<NonEmptyRange>(),
        interval_cmp_upper,
        typcache as *mut c_void,
    );

    /*----------
     * The goal is to form a left and right range, so that every entry
     * range is contained by either left or right interval (or both).
     *
     * For example, with the ranges (0,1), (1,3), (2,3), (2,4):
     *
     * 0 1 2 3 4
     * +-+
     *	 +---+
     *	   +-+
     *	   +---+
     *
     * The left and right ranges are of the form (0,a) and (b,4).
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
     *----------
     */

    /*
     * Iterate over lower bound of right group, finding smallest possible
     * upper bound of left group.
     */
    i1 = 0;
    i2 = 0;
    right_lower = &mut (*by_lower.add(i1 as usize)).lower;
    left_upper = &mut (*by_upper.add(i2 as usize)).lower;
    loop {
        /*
         * Find next lower bound of right group.
         */
        while i1 < nentries
            && range_cmp_bounds(typcache, right_lower, &(*by_lower.add(i1 as usize)).lower) == 0
        {
            if range_cmp_bounds(typcache, &(*by_lower.add(i1 as usize)).upper, left_upper) > 0 {
                left_upper = &mut (*by_lower.add(i1 as usize)).upper;
            }
            i1 += 1;
        }
        if i1 >= nentries {
            break;
        }
        right_lower = &mut (*by_lower.add(i1 as usize)).lower;

        /*
         * Find count of ranges which anyway should be placed to the left
         * group.
         */
        while i2 < nentries
            && range_cmp_bounds(typcache, &(*by_upper.add(i2 as usize)).upper, left_upper) <= 0
        {
            i2 += 1;
        }

        /*
         * Consider found split to see if it's better than what we had.
         */
        range_gist_consider_split(&mut context, right_lower, i1, left_upper, i2);
    }

    /*
     * Iterate over upper bound of left group finding greatest possible lower
     * bound of right group.
     */
    i1 = nentries - 1;
    i2 = nentries - 1;
    right_lower = &mut (*by_lower.add(i1 as usize)).upper;
    left_upper = &mut (*by_upper.add(i2 as usize)).upper;
    loop {
        /*
         * Find next upper bound of left group.
         */
        while i2 >= 0
            && range_cmp_bounds(typcache, left_upper, &(*by_upper.add(i2 as usize)).upper) == 0
        {
            if range_cmp_bounds(typcache, &(*by_upper.add(i2 as usize)).lower, right_lower) < 0 {
                right_lower = &mut (*by_upper.add(i2 as usize)).lower;
            }
            i2 -= 1;
        }
        if i2 < 0 {
            break;
        }
        left_upper = &mut (*by_upper.add(i2 as usize)).upper;

        /*
         * Find count of intervals which anyway should be placed to the right
         * group.
         */
        while i1 >= 0
            && range_cmp_bounds(typcache, &(*by_lower.add(i1 as usize)).lower, right_lower) >= 0
        {
            i1 -= 1;
        }

        /*
         * Consider found split to see if it's better than what we had.
         */
        range_gist_consider_split(&mut context, right_lower, i1 + 1, left_upper, i2 + 1);
    }

    /*
     * If we failed to find any acceptable splits, use trivial split.
     */
    if context.first {
        range_gist_fallback_split(typcache, entryvec, v);
        return;
    }

    /*
     * Ok, we have now selected bounds of the groups. Now we have to
     * distribute entries themselves. At first we distribute entries which can
     * be placed unambiguously and collect "common entries" to array.
     */

    /* Allocate vectors for results */
    (*v).spl_left = palloc(nentries as usize * core::mem::size_of::<OffsetNumber>())
        as *mut OffsetNumber;
    (*v).spl_right = palloc(nentries as usize * core::mem::size_of::<OffsetNumber>())
        as *mut OffsetNumber;
    (*v).spl_nleft = 0;
    (*v).spl_nright = 0;

    /*
     * Allocate an array for "common entries" - entries which can be placed to
     * either group without affecting overlap along selected axis.
     */
    common_entries_count = 0;
    common_entries =
        palloc(nentries as usize * core::mem::size_of::<CommonEntry>()) as *mut CommonEntry;

    /*
     * Distribute entries which can be distributed unambiguously, and collect
     * common entries.
     */
    i = FirstOffsetNumber;
    while i <= maxoff {
        let range: *mut RangeType;
        let mut lower: RangeBound = core::mem::zeroed();
        let mut upper: RangeBound = core::mem::zeroed();
        let mut empty: bool = false;

        /*
         * Get upper and lower bounds along selected axis.
         */
        range = DatumGetRangeTypeP((*(*entryvec).entry(i as usize)).key);

        range_deserialize(typcache, range, &mut lower, &mut upper, &mut empty);

        if range_cmp_bounds(typcache, &upper, context.left_upper) <= 0 {
            /* Fits in the left group */
            if range_cmp_bounds(typcache, &lower, context.right_lower) >= 0 {
                /* Fits also in the right group, so "common entry" */
                (*common_entries.add(common_entries_count as usize)).index = i as c_int;
                if context.has_subtype_diff {
                    /*
                     * delta = (lower - context.right_lower) -
                     * (context.left_upper - upper)
                     */
                    (*common_entries.add(common_entries_count as usize)).delta = call_subtype_diff(
                        typcache,
                        lower.val,
                        (*context.right_lower).val,
                    ) - call_subtype_diff(
                        typcache,
                        (*context.left_upper).val,
                        upper.val,
                    );
                } else {
                    /* Without subtype_diff, take all deltas as zero */
                    (*common_entries.add(common_entries_count as usize)).delta = 0.0;
                }
                common_entries_count += 1;
            } else {
                /* Doesn't fit to the right group, so join to the left group */
                PLACE_LEFT!(v, typcache, left_range, range, i);
            }
        } else {
            /*
             * Each entry should fit on either left or right group. Since this
             * entry didn't fit in the left group, it better fit in the right
             * group.
             */
            Assert!(range_cmp_bounds(typcache, &lower, context.right_lower) >= 0);
            PLACE_RIGHT!(v, typcache, right_range, range, i);
        }
        i = OffsetNumberNext(i);
    }

    /*
     * Distribute "common entries", if any.
     */
    if common_entries_count > 0 {
        /*
         * Sort "common entries" by calculated deltas in order to distribute
         * the most ambiguous entries first.
         */
        pg_qsort(
            common_entries as *mut c_void,
            common_entries_count as usize,
            core::mem::size_of::<CommonEntry>(),
            common_entry_cmp,
        );

        /*
         * Distribute "common entries" between groups according to sorting.
         */
        i = 0;
        while (i as c_int) < common_entries_count {
            let range: *mut RangeType;
            let idx: c_int = (*common_entries.add(i as usize)).index;

            range = DatumGetRangeTypeP((*(*entryvec).entry(idx as usize)).key);

            /*
             * Check if we have to place this entry in either group to achieve
             * LIMIT_RATIO.
             */
            if (i as c_int) < context.common_left {
                PLACE_LEFT!(v, typcache, left_range, range, idx as OffsetNumber);
            } else {
                PLACE_RIGHT!(v, typcache, right_range, range, idx as OffsetNumber);
            }
            i += 1;
        }
    }

    (*v).spl_ldatum = PointerGetDatum(left_range as *const c_void);
    (*v).spl_rdatum = PointerGetDatum(right_range as *const c_void);
}

/*
 * Consider replacement of currently selected split with a better one
 * during range_gist_double_sorting_split.
 */
unsafe fn range_gist_consider_split(
    context: *mut ConsiderSplitContext,
    right_lower: *mut RangeBound,
    min_left_count: c_int,
    left_upper: *mut RangeBound,
    max_left_count: c_int,
) {
    let left_count: c_int;
    let right_count: c_int;
    let ratio: f32;
    let overlap: f32;

    /*
     * Calculate entries distribution ratio assuming most uniform distribution
     * of common entries.
     */
    if min_left_count >= ((*context).entries_count + 1) / 2 {
        left_count = min_left_count;
    } else if max_left_count <= (*context).entries_count / 2 {
        left_count = max_left_count;
    } else {
        left_count = (*context).entries_count / 2;
    }
    right_count = (*context).entries_count - left_count;

    /*
     * Ratio of split: quotient between size of smaller group and total
     * entries count.  This is necessarily 0.5 or less; if it's less than
     * LIMIT_RATIO then we will never accept the new split.
     */
    ratio = (core::cmp::min(left_count, right_count) as f32) / ((*context).entries_count as f32);

    if ratio as f64 > LIMIT_RATIO {
        let mut selectthis: bool = false;

        /*
         * The ratio is acceptable, so compare current split with previously
         * selected one. We search for minimal overlap (allowing negative
         * values) and minimal ratio secondarily.  If subtype_diff is
         * available, it's used for overlap measure.  Without subtype_diff we
         * use number of "common entries" as an overlap measure.
         */
        if (*context).has_subtype_diff {
            overlap = call_subtype_diff(
                (*context).typcache,
                (*left_upper).val,
                (*right_lower).val,
            ) as f32;
        } else {
            overlap = (max_left_count - min_left_count) as f32;
        }

        /* If there is no previous selection, select this split */
        if (*context).first {
            selectthis = true;
        } else {
            /*
             * Choose the new split if it has a smaller overlap, or same
             * overlap but better ratio.
             */
            if overlap < (*context).overlap
                || (overlap == (*context).overlap && ratio > (*context).ratio)
            {
                selectthis = true;
            }
        }

        if selectthis {
            /* save information about selected split */
            (*context).first = false;
            (*context).ratio = ratio;
            (*context).overlap = overlap;
            (*context).right_lower = right_lower;
            (*context).left_upper = left_upper;
            (*context).common_left = max_left_count - left_count;
            (*context).common_right = left_count - min_left_count;
        }
    }
}

/*
 * Find class number for range.
 *
 * The class number is a valid combination of the properties of the
 * range.  Note: the highest possible number is 8, because CLS_EMPTY
 * can't be combined with anything else.
 */
unsafe fn get_gist_range_class(range: *mut RangeType) -> c_int {
    let class_number: c_int;
    let flags: c_char;

    flags = range_get_flags(range);
    if (flags & RANGE_EMPTY) != 0 {
        class_number = CLS_EMPTY as c_int;
    } else {
        let mut cn: c_int = 0;
        if (flags & RANGE_LB_INF) != 0 {
            cn |= CLS_LOWER_INF as c_int;
        }
        if (flags & RANGE_UB_INF) != 0 {
            cn |= CLS_UPPER_INF as c_int;
        }
        if (flags & RANGE_CONTAIN_EMPTY) != 0 {
            cn |= CLS_CONTAIN_EMPTY as c_int;
        }
        class_number = cn;
    }
    class_number
}

/*
 * Comparison function for range_gist_single_sorting_split.
 */
unsafe fn single_bound_cmp(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    let i1 = a as *const SingleBoundSortItem;
    let i2 = b as *const SingleBoundSortItem;
    let typcache = arg as *mut TypeCacheEntry;

    range_cmp_bounds(typcache, &(*i1).bound, &(*i2).bound)
}

/*
 * Compare NonEmptyRanges by lower bound.
 */
unsafe fn interval_cmp_lower(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    let i1 = a as *const NonEmptyRange;
    let i2 = b as *const NonEmptyRange;
    let typcache = arg as *mut TypeCacheEntry;

    range_cmp_bounds(typcache, &(*i1).lower, &(*i2).lower)
}

/*
 * Compare NonEmptyRanges by upper bound.
 */
unsafe fn interval_cmp_upper(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    let i1 = a as *const NonEmptyRange;
    let i2 = b as *const NonEmptyRange;
    let typcache = arg as *mut TypeCacheEntry;

    range_cmp_bounds(typcache, &(*i1).upper, &(*i2).upper)
}

/*
 * Compare CommonEntrys by their deltas.
 */
unsafe fn common_entry_cmp(i1: *const c_void, i2: *const c_void) -> c_int {
    let delta1: f64 = (*(i1 as *const CommonEntry)).delta;
    let delta2: f64 = (*(i2 as *const CommonEntry)).delta;

    if delta1 < delta2 {
        -1
    } else if delta1 > delta2 {
        1
    } else {
        0
    }
}

/*
 * Convenience function to invoke type-specific subtype_diff function.
 * Caller must have already checked that there is one for the range type.
 */
unsafe fn call_subtype_diff(typcache: *mut TypeCacheEntry, val1: Datum, val2: Datum) -> f64 {
    let value: f64;

    value = DatumGetFloat8(FunctionCall2Coll(
        &mut (*typcache).rng_subdiff_finfo,
        (*typcache).rng_collation,
        val1,
        val2,
    ));
    /* Cope with buggy subtype_diff function by returning zero */
    if value >= 0.0 {
        return value;
    }
    0.0
}
