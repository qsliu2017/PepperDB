//! Translation of postgres/src/backend/utils/adt/rangetypes_spgist.c
//!
//! Implementation of quad tree over ranges mapped to 2d-points for SP-GiST.
//!
//! Quad tree is a data structure similar to a binary tree, but is adapted to
//! 2d data. Each inner node of a quad tree contains a point (centroid) which
//! divides the 2d-space into 4 quadrants. Each quadrant is associated with a
//! child node.
//!
//! Ranges are mapped to 2d-points so that the lower bound is one dimension,
//! and the upper bound is another. By convention, we visualize the lower bound
//! to be the horizontal axis, and upper bound the vertical axis.
//!
//! One quirk with this mapping is the handling of empty ranges. An empty range
//! doesn't have lower and upper bounds, so it cannot be mapped to 2d space in
//! a straightforward way. To cope with that, the root node can have a 5th
//! quadrant, which is reserved for empty ranges. Furthermore, there can be
//! inner nodes in the tree with no centroid. They contain only two child nodes,
//! one for empty ranges and another for non-empty ones. Such a node can appear
//! as the root node, or in the tree under the 5th child of the root node (in
//! which case it will only contain empty nodes).
//!
//! The SP-GiST picksplit function uses medians along both axes as the centroid.
//! This implementation only uses the comparison function of the range element
//! datatype, therefore it works for any range type.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!			src/backend/utils/adt/rangetypes_spgist.c

use crate::prelude::*;

use crate::access::common::scankey::StrategyNumber;
use crate::access::spgist::spgist::{
    spgConfigOut, spgChooseIn, spgChooseOut, spgInnerConsistentIn, spgInnerConsistentOut,
    spgLeafConsistentIn, spgLeafConsistentOut, spgMatchNode, spgPickSplitIn, spgPickSplitOut,
};
use crate::access::stratnum::{
    RTContainedByStrategyNumber, RTContainsElemStrategyNumber, RTContainsStrategyNumber,
    RTEqualStrategyNumber, RTLeftStrategyNumber, RTOverLeftStrategyNumber,
    RTOverRightStrategyNumber, RTOverlapStrategyNumber, RTRightStrategyNumber, RTSameStrategyNumber,
};
use crate::port::qsort::qsort_arg;
use crate::utils::adt::datum::datumCopy;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_VOID};

// ---------------------------------------------------------------------------
// Local stub types for dependencies not yet ported.
// ---------------------------------------------------------------------------

type TypeCacheEntry = c_void; // TODO: utils/typcache.h
type RangeType = c_void; // TODO: utils/rangetypes.h

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeBound {
    pub val: Datum,       /* the bound value, if any */
    pub infinite: bool,   /* bound is +/- infinity */
    pub inclusive: bool,  /* bound is inclusive (vs exclusive) */
    pub lower: bool,      /* this is the lower (vs upper) bound */
} // TODO: utils/rangetypes.h

// Operator strategy numbers used in the GiST and SP-GiST range opclasses
// (utils/rangetypes.h). Numbers are chosen to match up operator names with
// existing usages.
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

// Type OIDs (catalog/pg_type.h), stubbed as local consts.
const ANYRANGEOID: Oid = 3831;
const VOIDOID: Oid = 2278;

/*
 * SP-GiST 'config' interface function.
 */
pub unsafe fn spg_range_quad_config(fcinfo: FunctionCallInfo) -> Datum {
    /* spgConfigIn *cfgin = (spgConfigIn *) PG_GETARG_POINTER(0); */
    let cfg = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgConfigOut;

    (*cfg).prefixType = ANYRANGEOID;
    (*cfg).labelType = VOIDOID; /* we don't need node labels */
    (*cfg).canReturnData = true;
    (*cfg).longValuesOK = false;
    PG_RETURN_VOID!()
}

/*----------
 * Determine which quadrant a 2d-mapped range falls into, relative to the
 * centroid.
 *
 * Quadrants are numbered like this:
 *
 *	 4	|  1
 *	----+----
 *	 3	|  2
 *
 * Where the lower bound of range is the horizontal axis and upper bound the
 * vertical axis.
 *
 * Ranges on one of the axes are taken to lie in the quadrant with higher value
 * along perpendicular axis. That is, a value on the horizontal axis is taken
 * to belong to quadrant 1 or 4, and a value on the vertical axis is taken to
 * belong to quadrant 1 or 2. A range equal to centroid is taken to lie in
 * quadrant 1.
 *
 * Empty ranges are taken to lie in the special quadrant 5.
 *----------
 */
unsafe fn getQuadrant(
    typcache: *mut TypeCacheEntry,
    centroid: *const RangeType,
    tst: *const RangeType,
) -> int16 {
    let mut centroidLower: RangeBound = std::mem::zeroed();
    let mut centroidUpper: RangeBound = std::mem::zeroed();
    let mut centroidEmpty: bool = false;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    range_deserialize(
        typcache,
        centroid,
        &mut centroidLower,
        &mut centroidUpper,
        &mut centroidEmpty,
    );
    range_deserialize(typcache, tst, &mut lower, &mut upper, &mut empty);

    if empty {
        return 5;
    }

    if range_cmp_bounds(typcache, &lower, &centroidLower) >= 0 {
        if range_cmp_bounds(typcache, &upper, &centroidUpper) >= 0 {
            1
        } else {
            2
        }
    } else {
        if range_cmp_bounds(typcache, &upper, &centroidUpper) >= 0 {
            4
        } else {
            3
        }
    }
}

/*
 * Choose SP-GiST function: choose path for addition of new range.
 */
pub unsafe fn spg_range_quad_choose(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgChooseIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgChooseOut;
    let inRange: *mut RangeType = DatumGetRangeTypeP((*in_).datum);
    let centroid: *mut RangeType;
    let quadrant: int16;
    let typcache: *mut TypeCacheEntry;

    if (*in_).allTheSame {
        (*out).resultType = spgMatchNode;
        /* nodeN will be set by core */
        (*out).result.matchNode.levelAdd = 0;
        (*out).result.matchNode.restDatum = RangeTypePGetDatum(inRange);
        PG_RETURN_VOID!();
    }

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(inRange));

    /*
     * A node with no centroid divides ranges purely on whether they're empty
     * or not. All empty ranges go to child node 0, all non-empty ranges go to
     * node 1.
     */
    if !(*in_).hasPrefix {
        (*out).resultType = spgMatchNode;
        if RangeIsEmpty(inRange) {
            (*out).result.matchNode.nodeN = 0;
        } else {
            (*out).result.matchNode.nodeN = 1;
        }
        (*out).result.matchNode.levelAdd = 1;
        (*out).result.matchNode.restDatum = RangeTypePGetDatum(inRange);
        PG_RETURN_VOID!();
    }

    centroid = DatumGetRangeTypeP((*in_).prefixDatum);
    quadrant = getQuadrant(typcache, centroid, inRange);

    Assert!(quadrant as c_int <= (*in_).nNodes);

    /* Select node matching to quadrant number */
    (*out).resultType = spgMatchNode;
    (*out).result.matchNode.nodeN = (quadrant - 1) as c_int;
    (*out).result.matchNode.levelAdd = 1;
    (*out).result.matchNode.restDatum = RangeTypePGetDatum(inRange);

    PG_RETURN_VOID!()
}

/*
 * Bound comparison for sorting.
 */
unsafe fn bound_cmp(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    let ba = a as *mut RangeBound;
    let bb = b as *mut RangeBound;
    let typcache = arg as *mut TypeCacheEntry;

    range_cmp_bounds(typcache, ba, bb)
}

/*
 * Picksplit SP-GiST function: split ranges into nodes. Select "centroid"
 * range and distribute ranges according to quadrants.
 */
pub unsafe fn spg_range_quad_picksplit(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgPickSplitIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgPickSplitOut;
    let mut i: c_int;
    let mut j: c_int;
    let nonEmptyCount: c_int;
    let centroid: *mut RangeType;
    let mut empty: bool = false;
    let typcache: *mut TypeCacheEntry;

    /* Use the median values of lower and upper bounds as the centroid range */
    let lowerBounds: *mut RangeBound;
    let upperBounds: *mut RangeBound;

    typcache = range_get_typcache(
        fcinfo,
        RangeTypeGetOid(DatumGetRangeTypeP(*(*in_).datums.offset(0))),
    );

    /* Allocate memory for bounds */
    lowerBounds = palloc(std::mem::size_of::<RangeBound>() * (*in_).nTuples as usize)
        as *mut RangeBound;
    upperBounds = palloc(std::mem::size_of::<RangeBound>() * (*in_).nTuples as usize)
        as *mut RangeBound;
    j = 0;

    /* Deserialize bounds of ranges, count non-empty ranges */
    i = 0;
    while i < (*in_).nTuples {
        range_deserialize(
            typcache,
            DatumGetRangeTypeP(*(*in_).datums.offset(i as isize)),
            lowerBounds.offset(j as isize),
            upperBounds.offset(j as isize),
            &mut empty,
        );
        if !empty {
            j += 1;
        }
        i += 1;
    }
    nonEmptyCount = j;

    /*
     * All the ranges are empty. The best we can do is to construct an inner
     * node with no centroid, and put all ranges into node 0. If non-empty
     * ranges are added later, they will be routed to node 1.
     */
    if nonEmptyCount == 0 {
        (*out).nNodes = 2;
        (*out).hasPrefix = false;
        /* Prefix is empty */
        (*out).prefixDatum = PointerGetDatum(null());
        (*out).nodeLabels = null_mut();

        (*out).mapTuplesToNodes =
            palloc(std::mem::size_of::<c_int>() * (*in_).nTuples as usize) as *mut c_int;
        (*out).leafTupleDatums =
            palloc(std::mem::size_of::<Datum>() * (*in_).nTuples as usize) as *mut Datum;

        /* Place all ranges into node 0 */
        i = 0;
        while i < (*in_).nTuples {
            let range: *mut RangeType = DatumGetRangeTypeP(*(*in_).datums.offset(i as isize));

            *(*out).leafTupleDatums.offset(i as isize) = RangeTypePGetDatum(range);
            *(*out).mapTuplesToNodes.offset(i as isize) = 0;
            i += 1;
        }
        PG_RETURN_VOID!();
    }

    /* Sort range bounds in order to find medians */
    qsort_arg(
        lowerBounds as *mut c_void,
        nonEmptyCount as usize,
        std::mem::size_of::<RangeBound>(),
        bound_cmp,
        typcache as *mut c_void,
    );
    qsort_arg(
        upperBounds as *mut c_void,
        nonEmptyCount as usize,
        std::mem::size_of::<RangeBound>(),
        bound_cmp,
        typcache as *mut c_void,
    );

    /* Construct "centroid" range from medians of lower and upper bounds */
    centroid = range_serialize(
        typcache,
        lowerBounds.offset((nonEmptyCount / 2) as isize),
        upperBounds.offset((nonEmptyCount / 2) as isize),
        false,
        null_mut(),
    );
    (*out).hasPrefix = true;
    (*out).prefixDatum = RangeTypePGetDatum(centroid);

    /* Create node for empty ranges only if it is a root node */
    (*out).nNodes = if (*in_).level == 0 { 5 } else { 4 };
    (*out).nodeLabels = null_mut(); /* we don't need node labels */

    (*out).mapTuplesToNodes =
        palloc(std::mem::size_of::<c_int>() * (*in_).nTuples as usize) as *mut c_int;
    (*out).leafTupleDatums =
        palloc(std::mem::size_of::<Datum>() * (*in_).nTuples as usize) as *mut Datum;

    /*
     * Assign ranges to corresponding nodes according to quadrants relative to
     * "centroid" range.
     */
    i = 0;
    while i < (*in_).nTuples {
        let range: *mut RangeType = DatumGetRangeTypeP(*(*in_).datums.offset(i as isize));
        let quadrant: int16 = getQuadrant(typcache, centroid, range);

        *(*out).leafTupleDatums.offset(i as isize) = RangeTypePGetDatum(range);
        *(*out).mapTuplesToNodes.offset(i as isize) = (quadrant - 1) as c_int;
        i += 1;
    }

    PG_RETURN_VOID!()
}

/*
 * SP-GiST consistent function for inner nodes: check which nodes are
 * consistent with given set of queries.
 */
pub unsafe fn spg_range_quad_inner_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgInnerConsistentIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgInnerConsistentOut;
    let mut which: c_int;
    let mut i: c_int;
    let oldCtx: MemoryContext;

    /*
     * For adjacent search we need also previous centroid (if any) to improve
     * the precision of the consistent check. In this case needPrevious flag
     * is set and centroid is passed into traversalValue.
     */
    let mut needPrevious: bool = false;

    if (*in_).allTheSame {
        /* Report that all nodes should be visited */
        (*out).nNodes = (*in_).nNodes;
        (*out).nodeNumbers =
            palloc(std::mem::size_of::<c_int>() * (*in_).nNodes as usize) as *mut c_int;
        i = 0;
        while i < (*in_).nNodes {
            *(*out).nodeNumbers.offset(i as isize) = i;
            i += 1;
        }
        PG_RETURN_VOID!();
    }

    if !(*in_).hasPrefix {
        /*
         * No centroid on this inner node. Such a node has two child nodes,
         * the first for empty ranges, and the second for non-empty ones.
         */
        Assert!((*in_).nNodes == 2);

        /*
         * Nth bit of which variable means that (N - 1)th node should be
         * visited. Initially all bits are set. Bits of nodes which should be
         * skipped will be unset.
         */
        which = (1 << 1) | (1 << 2);
        i = 0;
        while i < (*in_).nkeys {
            let strategy: StrategyNumber = (*(*in_).scankeys.offset(i as isize)).sk_strategy;
            let empty: bool;

            /*
             * The only strategy when second argument of operator is not range
             * is RANGESTRAT_CONTAINS_ELEM.
             */
            if strategy != RANGESTRAT_CONTAINS_ELEM {
                empty =
                    RangeIsEmpty(DatumGetRangeTypeP((*(*in_).scankeys.offset(i as isize)).sk_argument));
            } else {
                empty = false;
            }

            match strategy {
                _ if strategy == RANGESTRAT_BEFORE
                    || strategy == RANGESTRAT_OVERLEFT
                    || strategy == RANGESTRAT_OVERLAPS
                    || strategy == RANGESTRAT_OVERRIGHT
                    || strategy == RANGESTRAT_AFTER
                    || strategy == RANGESTRAT_ADJACENT =>
                {
                    /* These strategies return false if any argument is empty */
                    if empty {
                        which = 0;
                    } else {
                        which &= 1 << 2;
                    }
                }

                _ if strategy == RANGESTRAT_CONTAINS => {
                    /*
                     * All ranges contain an empty range. Only non-empty
                     * ranges can contain a non-empty range.
                     */
                    if !empty {
                        which &= 1 << 2;
                    }
                }

                _ if strategy == RANGESTRAT_CONTAINED_BY => {
                    /*
                     * Only an empty range is contained by an empty range.
                     * Both empty and non-empty ranges can be contained by a
                     * non-empty range.
                     */
                    if empty {
                        which &= 1 << 1;
                    }
                }

                _ if strategy == RANGESTRAT_CONTAINS_ELEM => {
                    which &= 1 << 2;
                }

                _ if strategy == RANGESTRAT_EQ => {
                    if empty {
                        which &= 1 << 1;
                    } else {
                        which &= 1 << 2;
                    }
                }

                _ => {
                    elog!(ERROR, "unrecognized range strategy: {}", strategy);
                }
            }
            if which == 0 {
                break; /* no need to consider remaining conditions */
            }
            i += 1;
        }
    } else {
        let mut centroidLower: RangeBound = std::mem::zeroed();
        let mut centroidUpper: RangeBound = std::mem::zeroed();
        let mut centroidEmpty: bool = false;
        let typcache: *mut TypeCacheEntry;
        let centroid: *mut RangeType;

        /* This node has a centroid. Fetch it. */
        centroid = DatumGetRangeTypeP((*in_).prefixDatum);
        typcache = range_get_typcache(fcinfo, RangeTypeGetOid(centroid));
        range_deserialize(
            typcache,
            centroid,
            &mut centroidLower,
            &mut centroidUpper,
            &mut centroidEmpty,
        );

        Assert!((*in_).nNodes == 4 || (*in_).nNodes == 5);

        /*
         * Nth bit of which variable means that (N - 1)th node (Nth quadrant)
         * should be visited. Initially all bits are set. Bits of nodes which
         * can be skipped will be unset.
         */
        which = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 5);

        i = 0;
        while i < (*in_).nkeys {
            let mut strategy: StrategyNumber;
            let mut lower: RangeBound = std::mem::zeroed();
            let mut upper: RangeBound = std::mem::zeroed();
            let mut empty: bool = false;
            let range: *mut RangeType;

            let mut prevCentroid: *mut RangeType = null_mut();
            let mut prevLower: RangeBound = std::mem::zeroed();
            let mut prevUpper: RangeBound = std::mem::zeroed();
            let mut prevEmpty: bool = false;

            /* Restrictions on range bounds according to scan strategy */
            let mut minLower: *mut RangeBound = null_mut();
            let mut maxLower: *mut RangeBound = null_mut();
            let mut minUpper: *mut RangeBound = null_mut();
            let mut maxUpper: *mut RangeBound = null_mut();

            /* Are the restrictions on range bounds inclusive? */
            let mut inclusive: bool = true;
            let mut strictEmpty: bool = true;
            let mut cmp: c_int;
            let which1: c_int;
            let which2: c_int;

            strategy = (*(*in_).scankeys.offset(i as isize)).sk_strategy;

            /*
             * RANGESTRAT_CONTAINS_ELEM is just like RANGESTRAT_CONTAINS, but
             * the argument is a single element. Expand the single element to
             * a range containing only the element, and treat it like
             * RANGESTRAT_CONTAINS.
             */
            if strategy == RANGESTRAT_CONTAINS_ELEM {
                lower.inclusive = true;
                lower.infinite = false;
                lower.lower = true;
                lower.val = (*(*in_).scankeys.offset(i as isize)).sk_argument;

                upper.inclusive = true;
                upper.infinite = false;
                upper.lower = false;
                upper.val = (*(*in_).scankeys.offset(i as isize)).sk_argument;

                empty = false;

                strategy = RANGESTRAT_CONTAINS;

                range = null_mut();
            } else {
                range = DatumGetRangeTypeP((*(*in_).scankeys.offset(i as isize)).sk_argument);
                range_deserialize(typcache, range, &mut lower, &mut upper, &mut empty);
            }

            /*
             * Most strategies are handled by forming a bounding box from the
             * search key, defined by a minLower, maxLower, minUpper,
             * maxUpper. Some modify 'which' directly, to specify exactly
             * which quadrants need to be visited.
             *
             * For most strategies, nothing matches an empty search key, and
             * an empty range never matches a non-empty key. If a strategy
             * does not behave like that wrt. empty ranges, set strictEmpty to
             * false.
             */
            match strategy {
                _ if strategy == RANGESTRAT_BEFORE => {
                    /*
                     * Range A is before range B if upper bound of A is lower
                     * than lower bound of B.
                     */
                    maxUpper = &mut lower;
                    inclusive = false;
                }

                _ if strategy == RANGESTRAT_OVERLEFT => {
                    /*
                     * Range A is overleft to range B if upper bound of A is
                     * less than or equal to upper bound of B.
                     */
                    maxUpper = &mut upper;
                }

                _ if strategy == RANGESTRAT_OVERLAPS => {
                    /*
                     * Non-empty ranges overlap, if lower bound of each range
                     * is lower or equal to upper bound of the other range.
                     */
                    maxLower = &mut upper;
                    minUpper = &mut lower;
                }

                _ if strategy == RANGESTRAT_OVERRIGHT => {
                    /*
                     * Range A is overright to range B if lower bound of A is
                     * greater than or equal to lower bound of B.
                     */
                    minLower = &mut lower;
                }

                _ if strategy == RANGESTRAT_AFTER => {
                    /*
                     * Range A is after range B if lower bound of A is greater
                     * than upper bound of B.
                     */
                    minLower = &mut upper;
                    inclusive = false;
                }

                _ if strategy == RANGESTRAT_ADJACENT => {
                    if empty {
                        /* Skip to strictEmpty check. */
                    } else {
                        /*
                         * Previously selected quadrant could exclude possibility
                         * for lower or upper bounds to be adjacent. Deserialize
                         * previous centroid range if present for checking this.
                         */
                        if !(*in_).traversalValue.is_null() {
                            prevCentroid = (*in_).traversalValue as *mut RangeType;
                            range_deserialize(
                                typcache,
                                prevCentroid,
                                &mut prevLower,
                                &mut prevUpper,
                                &mut prevEmpty,
                            );
                        }

                        /*
                         * For a range's upper bound to be adjacent to the
                         * argument's lower bound, it will be found along the line
                         * adjacent to (and just below) Y=lower. Therefore, if the
                         * argument's lower bound is less than the centroid's
                         * upper bound, the line falls in quadrants 2 and 3; if
                         * greater, the line falls in quadrants 1 and 4. (see
                         * adjacent_cmp_bounds for description of edge cases).
                         */
                        cmp = adjacent_inner_consistent(
                            typcache,
                            &lower,
                            &centroidUpper,
                            if !prevCentroid.is_null() {
                                &prevUpper
                            } else {
                                null()
                            },
                        );
                        if cmp > 0 {
                            which1 = (1 << 1) | (1 << 4);
                        } else if cmp < 0 {
                            which1 = (1 << 2) | (1 << 3);
                        } else {
                            which1 = 0;
                        }

                        /*
                         * Also search for ranges's adjacent to argument's upper
                         * bound. They will be found along the line adjacent to
                         * (and just right of) X=upper, which falls in quadrants 3
                         * and 4, or 1 and 2.
                         */
                        cmp = adjacent_inner_consistent(
                            typcache,
                            &upper,
                            &centroidLower,
                            if !prevCentroid.is_null() {
                                &prevLower
                            } else {
                                null()
                            },
                        );
                        if cmp > 0 {
                            which2 = (1 << 1) | (1 << 2);
                        } else if cmp < 0 {
                            which2 = (1 << 3) | (1 << 4);
                        } else {
                            which2 = 0;
                        }

                        /* We must chase down ranges adjacent to either bound. */
                        which &= which1 | which2;

                        needPrevious = true;
                    }
                }

                _ if strategy == RANGESTRAT_CONTAINS => {
                    /*
                     * Non-empty range A contains non-empty range B if lower
                     * bound of A is lower or equal to lower bound of range B
                     * and upper bound of range A is greater than or equal to
                     * upper bound of range A.
                     *
                     * All non-empty ranges contain an empty range.
                     */
                    strictEmpty = false;
                    if !empty {
                        which &= (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);
                        maxLower = &mut lower;
                        minUpper = &mut upper;
                    }
                }

                _ if strategy == RANGESTRAT_CONTAINED_BY => {
                    /* The opposite of contains. */
                    strictEmpty = false;
                    if empty {
                        /* An empty range is only contained by an empty range */
                        which &= 1 << 5;
                    } else {
                        minLower = &mut lower;
                        maxUpper = &mut upper;
                    }
                }

                _ if strategy == RANGESTRAT_EQ => {
                    /*
                     * Equal range can be only in the same quadrant where
                     * argument would be placed to.
                     */
                    strictEmpty = false;
                    which &= 1 << getQuadrant(typcache, centroid, range);
                }

                _ => {
                    elog!(ERROR, "unrecognized range strategy: {}", strategy);
                }
            }

            if strictEmpty {
                if empty {
                    /* Scan key is empty, no branches are satisfying */
                    which = 0;
                    break;
                } else {
                    /* Shouldn't visit tree branch with empty ranges */
                    which &= (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);
                }
            }

            /*
             * Using the bounding box, see which quadrants we have to descend
             * into.
             */
            if !minLower.is_null() {
                /*
                 * If the centroid's lower bound is less than or equal to the
                 * minimum lower bound, anything in the 3rd and 4th quadrants
                 * will have an even smaller lower bound, and thus can't
                 * match.
                 */
                if range_cmp_bounds(typcache, &centroidLower, minLower) <= 0 {
                    which &= (1 << 1) | (1 << 2) | (1 << 5);
                }
            }
            if !maxLower.is_null() {
                /*
                 * If the centroid's lower bound is greater than the maximum
                 * lower bound, anything in the 1st and 2nd quadrants will
                 * also have a greater than or equal lower bound, and thus
                 * can't match. If the centroid's lower bound is equal to the
                 * maximum lower bound, we can still exclude the 1st and 2nd
                 * quadrants if we're looking for a value strictly greater
                 * than the maximum.
                 */

                cmp = range_cmp_bounds(typcache, &centroidLower, maxLower);
                if cmp > 0 || (!inclusive && cmp == 0) {
                    which &= (1 << 3) | (1 << 4) | (1 << 5);
                }
            }
            if !minUpper.is_null() {
                /*
                 * If the centroid's upper bound is less than or equal to the
                 * minimum upper bound, anything in the 2nd and 3rd quadrants
                 * will have an even smaller upper bound, and thus can't
                 * match.
                 */
                if range_cmp_bounds(typcache, &centroidUpper, minUpper) <= 0 {
                    which &= (1 << 1) | (1 << 4) | (1 << 5);
                }
            }
            if !maxUpper.is_null() {
                /*
                 * If the centroid's upper bound is greater than the maximum
                 * upper bound, anything in the 1st and 4th quadrants will
                 * also have a greater than or equal upper bound, and thus
                 * can't match. If the centroid's upper bound is equal to the
                 * maximum upper bound, we can still exclude the 1st and 4th
                 * quadrants if we're looking for a value strictly greater
                 * than the maximum.
                 */

                cmp = range_cmp_bounds(typcache, &centroidUpper, maxUpper);
                if cmp > 0 || (!inclusive && cmp == 0) {
                    which &= (1 << 2) | (1 << 3) | (1 << 5);
                }
            }

            if which == 0 {
                break; /* no need to consider remaining conditions */
            }
            i += 1;
        }
    }

    /* We must descend into the quadrant(s) identified by 'which' */
    (*out).nodeNumbers =
        palloc(std::mem::size_of::<c_int>() * (*in_).nNodes as usize) as *mut c_int;
    if needPrevious {
        (*out).traversalValues =
            palloc(std::mem::size_of::<*mut c_void>() * (*in_).nNodes as usize)
                as *mut *mut c_void;
    }
    (*out).nNodes = 0;

    /*
     * Elements of traversalValues should be allocated in
     * traversalMemoryContext
     */
    oldCtx = MemoryContextSwitchTo((*in_).traversalMemoryContext as MemoryContext);

    i = 1;
    while i <= (*in_).nNodes {
        if (which & (1 << i)) != 0 {
            /* Save previous prefix if needed */
            if needPrevious {
                let previousCentroid: Datum;

                /*
                 * We know, that in->prefixDatum in this place is varlena,
                 * because it's range
                 */
                previousCentroid = datumCopy((*in_).prefixDatum, false, -1);
                *(*out).traversalValues.offset((*out).nNodes as isize) =
                    previousCentroid as *mut c_void;
            }
            *(*out).nodeNumbers.offset((*out).nNodes as isize) = i - 1;
            (*out).nNodes += 1;
        }
        i += 1;
    }

    MemoryContextSwitchTo(oldCtx);

    PG_RETURN_VOID!()
}

/*
 * adjacent_cmp_bounds
 *
 * Given an argument and centroid bound, this function determines if any
 * bounds that are adjacent to the argument are smaller than, or greater than
 * or equal to centroid. For brevity, we call the arg < centroid "left", and
 * arg >= centroid case "right". This corresponds to how the quadrants are
 * arranged, if you imagine that "left" is equivalent to "down" and "right"
 * is equivalent to "up".
 *
 * For the "left" case, returns -1, and for the "right" case, returns 1.
 */
unsafe fn adjacent_cmp_bounds(
    typcache: *mut TypeCacheEntry,
    arg: *const RangeBound,
    centroid: *const RangeBound,
) -> c_int {
    let cmp: c_int;

    Assert!((*arg).lower != (*centroid).lower);

    cmp = range_cmp_bounds(typcache, arg, centroid);

    if (*centroid).lower {
        /*------
         * The argument is an upper bound, we are searching for adjacent lower
         * bounds. A matching adjacent lower bound must be *larger* than the
         * argument, but only just.
         *
         * The following table illustrates the desired result with a fixed
         * argument bound, and different centroids. The CMP column shows
         * the value of 'cmp' variable, and ADJ shows whether the argument
         * and centroid are adjacent, per bounds_adjacent(). (N) means we
         * don't need to check for that case, because it's implied by CMP.
         * With the argument range [..., 500), the adjacent range we're
         * searching for is [500, ...):
         *
         *	ARGUMENT   CENTROID		CMP   ADJ
         *	[..., 500) [498, ...)	 >	  (N)	[500, ...) is to the right
         *	[..., 500) [499, ...)	 =	  (N)	[500, ...) is to the right
         *	[..., 500) [500, ...)	 <	   Y	[500, ...) is to the right
         *	[..., 500) [501, ...)	 <	   N	[500, ...) is to the left
         *
         * So, we must search left when the argument is smaller than, and not
         * adjacent, to the centroid. Otherwise search right.
         *------
         */
        if cmp < 0 && !bounds_adjacent(typcache, *arg, *centroid) {
            -1
        } else {
            1
        }
    } else {
        /*------
         * The argument is a lower bound, we are searching for adjacent upper
         * bounds. A matching adjacent upper bound must be *smaller* than the
         * argument, but only just.
         *
         *	ARGUMENT   CENTROID		CMP   ADJ
         *	[500, ...) [..., 499)	 >	  (N)	[..., 500) is to the right
         *	[500, ...) [..., 500)	 >	  (Y)	[..., 500) is to the right
         *	[500, ...) [..., 501)	 =	  (N)	[..., 500) is to the left
         *	[500, ...) [..., 502)	 <	  (N)	[..., 500) is to the left
         *
         * We must search left when the argument is smaller than or equal to
         * the centroid. Otherwise search right. We don't need to check
         * whether the argument is adjacent with the centroid, because it
         * doesn't matter.
         *------
         */
        if cmp <= 0 {
            -1
        } else {
            1
        }
    }
}

/*----------
 * adjacent_inner_consistent
 *
 * Like adjacent_cmp_bounds, but also takes into account the previous
 * level's centroid. We might've traversed left (or right) at the previous
 * node, in search for ranges adjacent to the other bound, even though we
 * already ruled out the possibility for any matches in that direction for
 * this bound. By comparing the argument with the previous centroid, and
 * the previous centroid with the current centroid, we can determine which
 * direction we should've moved in at previous level, and which direction we
 * actually moved.
 *
 * If there can be any matches to the left, returns -1. If to the right,
 * returns 1. If there can be no matches below this centroid, because we
 * already ruled them out at the previous level, returns 0.
 *
 * XXX: Comparing just the previous and current level isn't foolproof; we
 * might still search some branches unnecessarily. For example, imagine that
 * we are searching for value 15, and we traverse the following centroids
 * (only considering one bound for the moment):
 *
 * Level 1: 20
 * Level 2: 50
 * Level 3: 25
 *
 * At this point, previous centroid is 50, current centroid is 25, and the
 * target value is to the left. But because we already moved right from
 * centroid 20 to 50 in the first level, there cannot be any values < 20 in
 * the current branch. But we don't know that just by looking at the previous
 * and current centroid, so we traverse left, unnecessarily. The reason we are
 * down this branch is that we're searching for matches with the *other*
 * bound. If we kept track of which bound we are searching for explicitly,
 * instead of deducing that from the previous and current centroid, we could
 * avoid some unnecessary work.
 *----------
 */
unsafe fn adjacent_inner_consistent(
    typcache: *mut TypeCacheEntry,
    arg: *const RangeBound,
    centroid: *const RangeBound,
    prev: *const RangeBound,
) -> c_int {
    if !prev.is_null() {
        let prevcmp: c_int;
        let cmp: c_int;

        /*
         * Which direction were we supposed to traverse at previous level,
         * left or right?
         */
        prevcmp = adjacent_cmp_bounds(typcache, arg, prev);

        /* and which direction did we actually go? */
        cmp = range_cmp_bounds(typcache, centroid, prev);

        /* if the two don't agree, there's nothing to see here */
        if (prevcmp < 0 && cmp >= 0) || (prevcmp > 0 && cmp < 0) {
            return 0;
        }
    }

    adjacent_cmp_bounds(typcache, arg, centroid)
}

/*
 * SP-GiST consistent function for leaf nodes: check leaf value against query
 * using corresponding function.
 */
pub unsafe fn spg_range_quad_leaf_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgLeafConsistentIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgLeafConsistentOut;
    let leafRange: *mut RangeType = DatumGetRangeTypeP((*in_).leafDatum);
    let typcache: *mut TypeCacheEntry;
    let mut res: bool;
    let mut i: c_int;

    /* all tests are exact */
    (*out).recheck = false;

    /* leafDatum is what it is... */
    (*out).leafValue = (*in_).leafDatum;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(leafRange));

    /* Perform the required comparison(s) */
    res = true;
    i = 0;
    while i < (*in_).nkeys {
        let keyDatum: Datum = (*(*in_).scankeys.offset(i as isize)).sk_argument;

        /* Call the function corresponding to the scan strategy */
        let strategy = (*(*in_).scankeys.offset(i as isize)).sk_strategy;
        if strategy == RANGESTRAT_BEFORE {
            res = range_before_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
        } else if strategy == RANGESTRAT_OVERLEFT {
            res = range_overleft_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
        } else if strategy == RANGESTRAT_OVERLAPS {
            res = range_overlaps_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
        } else if strategy == RANGESTRAT_OVERRIGHT {
            res = range_overright_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
        } else if strategy == RANGESTRAT_AFTER {
            res = range_after_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
        } else if strategy == RANGESTRAT_ADJACENT {
            res = range_adjacent_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
        } else if strategy == RANGESTRAT_CONTAINS {
            res = range_contains_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
        } else if strategy == RANGESTRAT_CONTAINED_BY {
            res = range_contained_by_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
        } else if strategy == RANGESTRAT_CONTAINS_ELEM {
            res = range_contains_elem_internal(typcache, leafRange, keyDatum);
        } else if strategy == RANGESTRAT_EQ {
            res = range_eq_internal(typcache, leafRange, DatumGetRangeTypeP(keyDatum));
        } else {
            elog!(ERROR, "unrecognized range strategy: {}", strategy);
        }

        /*
         * If leaf datum doesn't match to a query key, no need to check
         * subsequent keys.
         */
        if !res {
            break;
        }
        i += 1;
    }

    PG_RETURN_BOOL!(res)
}

// ---------------------------------------------------------------------------
// Local stubs for unported helper functions (utils/adt/rangetypes.c,
// utils/rangetypes.h, utils/typcache.h).
// ---------------------------------------------------------------------------

unsafe fn range_deserialize(
    _typcache: *mut TypeCacheEntry,
    _range: *const RangeType,
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

unsafe fn range_cmp_bounds(
    _typcache: *mut TypeCacheEntry,
    _b1: *const RangeBound,
    _b2: *const RangeBound,
) -> c_int {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn bounds_adjacent(
    _typcache: *mut TypeCacheEntry,
    _b1: RangeBound,
    _b2: RangeBound,
) -> bool {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_get_typcache(_fcinfo: FunctionCallInfo, _rngtypid: Oid) -> *mut TypeCacheEntry {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn DatumGetRangeTypeP(_d: Datum) -> *mut RangeType {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn RangeTypePGetDatum(_r: *const RangeType) -> Datum {
    unimplemented!() // TODO: utils/rangetypes.h
}

unsafe fn RangeTypeGetOid(_r: *const RangeType) -> Oid {
    unimplemented!() // TODO: utils/rangetypes.h
}

unsafe fn RangeIsEmpty(_r: *const RangeType) -> bool {
    unimplemented!() // TODO: utils/rangetypes.h
}

unsafe fn range_before_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_overleft_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_overlaps_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_overright_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_after_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_adjacent_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_contains_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_contained_by_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_contains_elem_internal(
    _typcache: *mut TypeCacheEntry,
    _r: *const RangeType,
    _val: Datum,
) -> bool {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}

unsafe fn range_eq_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO: utils/adt/rangetypes.c
}
