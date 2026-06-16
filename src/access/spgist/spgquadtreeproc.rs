//! Translation of postgres/src/backend/access/spgist/spgquadtreeproc.c
//!
//! Implementation of quad tree over points for SP-GiST.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * `Point` / `BOX` and the geo accessors `DatumGetPointP` / `DatumGetBoxP`,
//!   plus `box_copy` and `spg_key_orderbys_distances`, are imported from
//!   crate::access::spgist::spgproc (geo_decls.h itself is NOT ported -- spgproc
//!   defines the minimal mirrors).  We import them rather than redefine so the
//!   Point/BOX types match exactly (no TYPE MISMATCH).
//!
//! * The spgConfigIn/Out, spgChooseIn/Out, spgPickSplitIn/Out,
//!   spgInnerConsistentIn/Out, spgLeafConsistentIn/Out structs come from
//!   access/spgist.h (NOT ported).  We define MINIMAL #[repr(C)] mirrors below
//!   containing ONLY the fields these functions touch.  spgChooseOut and
//!   spgInnerConsistentOut have tagged-union "result" members in C; we mirror
//!   just the touched arms.
//!
//! * The SPTEST geometric predicate operators (point_above/below/left/right/
//!   horiz/vert/eq and box_contain_pt) come from geo_ops.c (NOT ported).  Since
//!   getQuadrant is the REAL testable core, the predicates are implemented
//!   directly here as inline coordinate comparisons matching geo_ops.c exactly
//!   (e.g. point_above is p1.y > p2.y, point_horiz is p1.y == p2.y, etc).
//!
//! * Type OIDs POINTOID / VOIDOID come from catalog/pg_type.h; stubbed as local
//!   consts.  get_float8_infinity comes from utils/float.h.
//!
//! * Heavy SP-GiST framework alloc bits (out->nodeNumbers, out->levelAdds,
//!   distances, traversalValues) are kept real where they are simple palloc
//!   loops; MemoryContextSwitchTo is stubbed (no-op identity) since the memory
//!   context machinery is not ported.

use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_VOID};
use crate::access::common::scankey::ScanKey;
use crate::access::spgist::spgproc::{
    box_copy, spg_key_orderbys_distances, DatumGetBoxP, DatumGetPointP, Point, BOX,
};
use crate::access::stratnum::{
    RTAboveStrategyNumber, RTBelowStrategyNumber, RTContainedByStrategyNumber, RTLeftStrategyNumber,
    RTOldAboveStrategyNumber, RTOldBelowStrategyNumber, RTRightStrategyNumber, RTSameStrategyNumber,
};
use core::ffi::c_double;

// ===========================================================================
// Stubbed type OIDs (catalog/pg_type.h).
// ===========================================================================

const POINTOID: Oid = 600;
const VOIDOID: Oid = 2278;

// ===========================================================================
// Stubbed utils/float.h helpers.
// ===========================================================================

/// `get_float8_infinity()` from utils/float.h.
#[inline]
fn get_float8_infinity() -> c_double {
    c_double::INFINITY
}

// ===========================================================================
// MemoryContext stub (memory context machinery not ported).
// ===========================================================================

type MemoryContext = *mut c_void;

/// `MemoryContextSwitchTo` stub: returns the passed context, switches nothing.
#[inline]
unsafe fn MemoryContextSwitchTo(context: MemoryContext) -> MemoryContext {
    context
}

// ===========================================================================
// Minimal access/spgist.h struct mirrors (only the touched fields).
// ===========================================================================

/// `spgConfigOut` (touched fields only).
#[repr(C)]
pub struct spgConfigOut {
    pub prefixType: Oid,
    pub labelType: Oid,
    pub canReturnData: bool,
    pub longValuesOK: bool,
}

/// Result-type tags for spgChooseOut / framework.
const spgMatchNode: c_int = 1;

/// matchNode arm of spgChooseOut.result union.
#[repr(C)]
pub struct spgMatchNodeOut {
    pub nodeN: c_int,
    pub levelAdd: c_int,
    pub restDatum: Datum,
}

/// `spgChooseIn` (touched fields only).
#[repr(C)]
pub struct spgChooseIn {
    pub datum: Datum,
    pub allTheSame: bool,
    pub hasPrefix: bool,
    pub prefixDatum: Datum,
    pub nNodes: c_int,
}

/// `spgChooseOut` -- we mirror only the matchNode arm of its result union.
#[repr(C)]
pub struct spgChooseOut {
    pub resultType: c_int,
    pub result: spgMatchNodeOut,
}

/// `spgPickSplitIn` (touched fields only).
#[repr(C)]
pub struct spgPickSplitIn {
    pub nTuples: c_int,
    pub datums: *mut Datum,
}

/// `spgPickSplitOut` (touched fields only).
#[repr(C)]
pub struct spgPickSplitOut {
    pub hasPrefix: bool,
    pub prefixDatum: Datum,
    pub nNodes: c_int,
    pub nodeLabels: *mut Datum,
    pub mapTuplesToNodes: *mut c_int,
    pub leafTupleDatums: *mut Datum,
}

/// `spgInnerConsistentIn` (touched fields only).
#[repr(C)]
pub struct spgInnerConsistentIn {
    pub scankeys: ScanKey,
    pub nkeys: c_int,
    pub orderbys: ScanKey,
    pub norderbys: c_int,
    pub hasPrefix: bool,
    pub prefixDatum: Datum,
    pub allTheSame: bool,
    pub nNodes: c_int,
    pub level: c_int,
    pub traversalValue: *mut c_void,
    pub traversalMemoryContext: MemoryContext,
}

/// `spgInnerConsistentOut` (touched fields only).
#[repr(C)]
pub struct spgInnerConsistentOut {
    pub nNodes: c_int,
    pub nodeNumbers: *mut c_int,
    pub levelAdds: *mut c_int,
    pub distances: *mut *mut c_double,
    pub traversalValues: *mut *mut c_void,
}

/// `spgLeafConsistentIn` (touched fields only).
#[repr(C)]
pub struct spgLeafConsistentIn {
    pub scankeys: ScanKey,
    pub nkeys: c_int,
    pub orderbys: ScanKey,
    pub norderbys: c_int,
    pub leafDatum: Datum,
}

/// `spgLeafConsistentOut` (touched fields only).
#[repr(C)]
pub struct spgLeafConsistentOut {
    pub leafValue: Datum,
    pub recheck: bool,
    pub distances: *mut c_double,
}

// ===========================================================================
// SPTEST geometric predicates (geo_ops.c operators, implemented inline).
//
// SPTEST(f, x, y) == DatumGetBool(DirectFunctionCall2(f, x, y)).  Each operator
// is a simple coordinate comparison matching geo_ops.c.
// ===========================================================================

#[inline]
unsafe fn point_above(p1: *mut Point, p2: *mut Point) -> bool {
    (*p1).y > (*p2).y
}
#[inline]
unsafe fn point_below(p1: *mut Point, p2: *mut Point) -> bool {
    (*p1).y < (*p2).y
}
#[inline]
unsafe fn point_right(p1: *mut Point, p2: *mut Point) -> bool {
    (*p1).x > (*p2).x
}
#[inline]
unsafe fn point_left(p1: *mut Point, p2: *mut Point) -> bool {
    (*p1).x < (*p2).x
}
/// point_horiz: points are on the same horizontal line (equal y).
#[inline]
unsafe fn point_horiz(p1: *mut Point, p2: *mut Point) -> bool {
    (*p1).y == (*p2).y
}
/// point_vert: points are on the same vertical line (equal x).
#[inline]
unsafe fn point_vert(p1: *mut Point, p2: *mut Point) -> bool {
    (*p1).x == (*p2).x
}
#[inline]
unsafe fn point_eq(p1: *mut Point, p2: *mut Point) -> bool {
    (*p1).x == (*p2).x && (*p1).y == (*p2).y
}
/// box_contain_pt(box, pt): is point within (closed) box?
#[inline]
unsafe fn box_contain_pt(b: *mut BOX, p: *mut Point) -> bool {
    (*p).x <= (*b).high.x
        && (*p).x >= (*b).low.x
        && (*p).y <= (*b).high.y
        && (*p).y >= (*b).low.y
}

// ===========================================================================
// spgquadtreeproc.c
// ===========================================================================

/// `Datum spg_quad_config(PG_FUNCTION_ARGS)`
pub unsafe fn spg_quad_config(fcinfo: FunctionCallInfo) -> Datum {
    // spgConfigIn *cfgin = (spgConfigIn *) PG_GETARG_POINTER(0);
    let cfg = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgConfigOut;

    (*cfg).prefixType = POINTOID;
    (*cfg).labelType = VOIDOID; // we don't need node labels
    (*cfg).canReturnData = true;
    (*cfg).longValuesOK = false;
    PG_RETURN_VOID!();
}

/*
 * Determine which quadrant a point falls into, relative to the centroid.
 *
 * Quadrants are identified like this:
 *
 *	 4	|  1
 *	----+-----
 *	 3	|  2
 *
 * Points on one of the axes are taken to lie in the lowest-numbered
 * adjacent quadrant.
 */
/// `static int16 getQuadrant(Point *centroid, Point *tst)`
unsafe fn getQuadrant(centroid: *mut Point, tst: *mut Point) -> i16 {
    if (point_above(tst, centroid) || point_horiz(tst, centroid))
        && (point_right(tst, centroid) || point_vert(tst, centroid))
    {
        return 1;
    }

    if point_below(tst, centroid) && (point_right(tst, centroid) || point_vert(tst, centroid)) {
        return 2;
    }

    if (point_below(tst, centroid) || point_horiz(tst, centroid)) && point_left(tst, centroid) {
        return 3;
    }

    if point_above(tst, centroid) && point_left(tst, centroid) {
        return 4;
    }

    elog!(ERROR, "getQuadrant: impossible case");
    #[allow(unreachable_code)]
    {
        unreachable!()
    }
}

/// `static BOX *getQuadrantArea(BOX *bbox, Point *centroid, int quadrant)`
///
/// Returns bounding box of a given quadrant inside given bounding box.
unsafe fn getQuadrantArea(bbox: *mut BOX, centroid: *mut Point, quadrant: c_int) -> *mut BOX {
    let result = palloc(core::mem::size_of::<BOX>()) as *mut BOX;

    match quadrant {
        1 => {
            (*result).high = (*bbox).high;
            (*result).low = *centroid;
        }
        2 => {
            (*result).high.x = (*bbox).high.x;
            (*result).high.y = (*centroid).y;
            (*result).low.x = (*centroid).x;
            (*result).low.y = (*bbox).low.y;
        }
        3 => {
            (*result).high = *centroid;
            (*result).low = (*bbox).low;
        }
        4 => {
            (*result).high.x = (*centroid).x;
            (*result).high.y = (*bbox).high.y;
            (*result).low.x = (*bbox).low.x;
            (*result).low.y = (*centroid).y;
        }
        _ => {}
    }

    result
}

/// `Datum spg_quad_choose(PG_FUNCTION_ARGS)`
pub unsafe fn spg_quad_choose(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgChooseIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgChooseOut;
    let inPoint = DatumGetPointP((*in_).datum);

    if (*in_).allTheSame {
        (*out).resultType = spgMatchNode;
        // nodeN will be set by core
        (*out).result.levelAdd = 0;
        (*out).result.restDatum = PointerGetDatum(inPoint as *const c_void);
        PG_RETURN_VOID!();
    }

    assert!((*in_).hasPrefix);
    let centroid = DatumGetPointP((*in_).prefixDatum);

    assert!((*in_).nNodes == 4);

    (*out).resultType = spgMatchNode;
    (*out).result.nodeN = (getQuadrant(centroid, inPoint) - 1) as c_int;
    (*out).result.levelAdd = 0;
    (*out).result.restDatum = PointerGetDatum(inPoint as *const c_void);

    PG_RETURN_VOID!();
}

// ===========================================================================
// USE_MEDIAN qsort comparators (C: #ifdef USE_MEDIAN).  USE_MEDIAN is never
// defined in a default build, so these mirror the dead #ifdef branch via
// #[cfg(any())] (always-false cfg).  Translated 1:1 from the C comparators.
// ===========================================================================

/// `static int x_cmp(const void *a, const void *b, void *arg)`
#[cfg(any())]
unsafe fn x_cmp(a: *const c_void, b: *const c_void, _arg: *mut c_void) -> c_int {
    let pa = *(a as *const *mut Point);
    let pb = *(b as *const *mut Point);

    if (*pa).x == (*pb).x {
        return 0;
    }
    if (*pa).x > (*pb).x {
        1
    } else {
        -1
    }
}

/// `static int y_cmp(const void *a, const void *b, void *arg)`
#[cfg(any())]
unsafe fn y_cmp(a: *const c_void, b: *const c_void, _arg: *mut c_void) -> c_int {
    let pa = *(a as *const *mut Point);
    let pb = *(b as *const *mut Point);

    if (*pa).y == (*pb).y {
        return 0;
    }
    if (*pa).y > (*pb).y {
        1
    } else {
        -1
    }
}

/// `Datum spg_quad_picksplit(PG_FUNCTION_ARGS)`
pub unsafe fn spg_quad_picksplit(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgPickSplitIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgPickSplitOut;

    // Use the average values of x and y as the centroid point (non-USE_MEDIAN).
    let centroid = palloc0(core::mem::size_of::<Point>()) as *mut Point;

    let mut i: c_int = 0;
    while i < (*in_).nTuples {
        let p = DatumGetPointP(*(*in_).datums.add(i as usize));
        (*centroid).x += (*p).x;
        (*centroid).y += (*p).y;
        i += 1;
    }

    (*centroid).x /= (*in_).nTuples as c_double;
    (*centroid).y /= (*in_).nTuples as c_double;

    (*out).hasPrefix = true;
    (*out).prefixDatum = PointerGetDatum(centroid as *const c_void);

    (*out).nNodes = 4;
    (*out).nodeLabels = null_mut(); // we don't need node labels

    (*out).mapTuplesToNodes =
        palloc((*in_).nTuples as Size * core::mem::size_of::<c_int>()) as *mut c_int;
    (*out).leafTupleDatums =
        palloc((*in_).nTuples as Size * core::mem::size_of::<Datum>()) as *mut Datum;

    i = 0;
    while i < (*in_).nTuples {
        let p = DatumGetPointP(*(*in_).datums.add(i as usize));
        let quadrant = (getQuadrant(centroid, p) - 1) as c_int;

        *(*out).leafTupleDatums.add(i as usize) = PointerGetDatum(p as *const c_void);
        *(*out).mapTuplesToNodes.add(i as usize) = quadrant;
        i += 1;
    }

    PG_RETURN_VOID!();
}

/// `Datum spg_quad_inner_consistent(PG_FUNCTION_ARGS)`
pub unsafe fn spg_quad_inner_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgInnerConsistentIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgInnerConsistentOut;
    let mut infbbox = BOX {
        high: Point { x: 0.0, y: 0.0 },
        low: Point { x: 0.0, y: 0.0 },
    };
    let mut bbox: *mut BOX = null_mut();
    let mut which: c_int;
    let mut i: c_int;

    assert!((*in_).hasPrefix);
    let centroid = DatumGetPointP((*in_).prefixDatum);

    /*
     * When ordering scan keys are specified, we've to calculate distance for
     * them.  In order to do that, we need calculate bounding boxes for all
     * children nodes.  Calculation of those bounding boxes on non-zero level
     * require knowledge of bounding box of upper node.  So, we save bounding
     * boxes to traversalValues.
     */
    if (*in_).norderbys > 0 {
        (*out).distances =
            palloc((*in_).nNodes as Size * core::mem::size_of::<*mut c_double>())
                as *mut *mut c_double;
        (*out).traversalValues =
            palloc((*in_).nNodes as Size * core::mem::size_of::<*mut c_void>())
                as *mut *mut c_void;

        if (*in_).level == 0 {
            let inf = get_float8_infinity();

            infbbox.high.x = inf;
            infbbox.high.y = inf;
            infbbox.low.x = -inf;
            infbbox.low.y = -inf;
            bbox = &mut infbbox;
        } else {
            bbox = (*in_).traversalValue as *mut BOX;
            assert!(!bbox.is_null());
        }
    }

    if (*in_).allTheSame {
        // Report that all nodes should be visited
        (*out).nNodes = (*in_).nNodes;
        (*out).nodeNumbers =
            palloc((*in_).nNodes as Size * core::mem::size_of::<c_int>()) as *mut c_int;
        i = 0;
        while i < (*in_).nNodes {
            *(*out).nodeNumbers.add(i as usize) = i;

            if (*in_).norderbys > 0 {
                let oldCtx = MemoryContextSwitchTo((*in_).traversalMemoryContext);

                // Use parent quadrant box as traversalValue
                let quadrant = box_copy(bbox);

                MemoryContextSwitchTo(oldCtx);

                *(*out).traversalValues.add(i as usize) = quadrant as *mut c_void;
                *(*out).distances.add(i as usize) = spg_key_orderbys_distances(
                    PointerGetDatum(quadrant as *const c_void),
                    false,
                    (*in_).orderbys,
                    (*in_).norderbys,
                );
            }
            i += 1;
        }
        PG_RETURN_VOID!();
    }

    assert!((*in_).nNodes == 4);

    // "which" is a bitmask of quadrants that satisfy all constraints
    which = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);

    i = 0;
    while i < (*in_).nkeys {
        let sk = (*in_).scankeys.add(i as usize);
        let query = DatumGetPointP((*sk).sk_argument);

        match (*sk).sk_strategy {
            x if x == RTLeftStrategyNumber => {
                if point_right(centroid, query) {
                    which &= (1 << 3) | (1 << 4);
                }
            }
            x if x == RTRightStrategyNumber => {
                if point_left(centroid, query) {
                    which &= (1 << 1) | (1 << 2);
                }
            }
            x if x == RTSameStrategyNumber => {
                which &= 1 << getQuadrant(centroid, query);
            }
            x if x == RTBelowStrategyNumber || x == RTOldBelowStrategyNumber => {
                if point_above(centroid, query) {
                    which &= (1 << 2) | (1 << 3);
                }
            }
            x if x == RTAboveStrategyNumber || x == RTOldAboveStrategyNumber => {
                if point_below(centroid, query) {
                    which &= (1 << 1) | (1 << 4);
                }
            }
            x if x == RTContainedByStrategyNumber => {
                /*
                 * For this operator, the query is a box not a point.  We
                 * cheat to the extent of assuming that DatumGetPointP won't
                 * do anything that would be bad for a pointer-to-box.
                 */
                let boxQuery = DatumGetBoxP((*sk).sk_argument);

                if box_contain_pt(boxQuery, centroid) {
                    // centroid is in box, so all quadrants are OK
                } else {
                    // identify quadrant(s) containing all corners of box
                    let mut p: Point;
                    let mut r: c_int = 0;

                    p = (*boxQuery).low;
                    r |= 1 << getQuadrant(centroid, &mut p);
                    p.y = (*boxQuery).high.y;
                    r |= 1 << getQuadrant(centroid, &mut p);
                    p = (*boxQuery).high;
                    r |= 1 << getQuadrant(centroid, &mut p);
                    p.x = (*boxQuery).low.x;
                    r |= 1 << getQuadrant(centroid, &mut p);

                    which &= r;
                }
            }
            other => {
                elog!(ERROR, "unrecognized strategy number: {}", other);
            }
        }

        if which == 0 {
            break; // no need to consider remaining conditions
        }
        i += 1;
    }

    (*out).levelAdds = palloc(4 * core::mem::size_of::<c_int>()) as *mut c_int;
    i = 0;
    while i < 4 {
        *(*out).levelAdds.add(i as usize) = 1;
        i += 1;
    }

    // We must descend into the quadrant(s) identified by which
    (*out).nodeNumbers = palloc(4 * core::mem::size_of::<c_int>()) as *mut c_int;
    (*out).nNodes = 0;

    i = 1;
    while i <= 4 {
        if which & (1 << i) != 0 {
            *(*out).nodeNumbers.add((*out).nNodes as usize) = i - 1;

            if (*in_).norderbys > 0 {
                let oldCtx = MemoryContextSwitchTo((*in_).traversalMemoryContext);
                let quadrant = getQuadrantArea(bbox, centroid, i);

                MemoryContextSwitchTo(oldCtx);

                *(*out).traversalValues.add((*out).nNodes as usize) = quadrant as *mut c_void;

                *(*out).distances.add((*out).nNodes as usize) = spg_key_orderbys_distances(
                    PointerGetDatum(quadrant as *const c_void),
                    false,
                    (*in_).orderbys,
                    (*in_).norderbys,
                );
            }

            (*out).nNodes += 1;
        }
        i += 1;
    }

    PG_RETURN_VOID!();
}

/// `Datum spg_quad_leaf_consistent(PG_FUNCTION_ARGS)`
pub unsafe fn spg_quad_leaf_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgLeafConsistentIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgLeafConsistentOut;
    let datum = DatumGetPointP((*in_).leafDatum);
    let mut res: bool;

    // all tests are exact
    (*out).recheck = false;

    // leafDatum is what it is...
    (*out).leafValue = (*in_).leafDatum;

    // Perform the required comparison(s)
    res = true;
    let mut i: c_int = 0;
    while i < (*in_).nkeys {
        let sk = (*in_).scankeys.add(i as usize);
        let query = DatumGetPointP((*sk).sk_argument);

        match (*sk).sk_strategy {
            x if x == RTLeftStrategyNumber => {
                res = point_left(datum, query);
            }
            x if x == RTRightStrategyNumber => {
                res = point_right(datum, query);
            }
            x if x == RTSameStrategyNumber => {
                res = point_eq(datum, query);
            }
            x if x == RTBelowStrategyNumber || x == RTOldBelowStrategyNumber => {
                res = point_below(datum, query);
            }
            x if x == RTAboveStrategyNumber || x == RTOldAboveStrategyNumber => {
                res = point_above(datum, query);
            }
            x if x == RTContainedByStrategyNumber => {
                /*
                 * For this operator, the query is a box not a point.  We
                 * cheat to the extent of assuming that DatumGetPointP won't
                 * do anything that would be bad for a pointer-to-box.
                 */
                // SPTEST(box_contain_pt, query, datum): query is the box.
                res = box_contain_pt(query as *mut BOX, datum);
            }
            other => {
                elog!(ERROR, "unrecognized strategy number: {}", other);
            }
        }

        if !res {
            break;
        }
        i += 1;
    }

    if res && (*in_).norderbys > 0 {
        // ok, it passes -> let's compute the distances
        (*out).distances = spg_key_orderbys_distances(
            (*in_).leafDatum,
            true,
            (*in_).orderbys,
            (*in_).norderbys,
        );
    }

    PG_RETURN_BOOL!(res);
}

// ===========================================================================
// Tests: getQuadrant is the REAL testable core.  Quadrant layout:
//
//   4 | 1
//   --+--
//   3 | 2
//
// Points on an axis fall into the lowest-numbered adjacent quadrant.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn q(cx: f64, cy: f64, px: f64, py: f64) -> i16 {
        let mut c = Point { x: cx, y: cy };
        let mut p = Point { x: px, y: py };
        unsafe { getQuadrant(&mut c, &mut p) }
    }

    #[test]
    fn quadrant_strictly_offset_points() {
        // centroid at origin.
        // Q1: upper-right (above & right).
        assert_eq!(q(0.0, 0.0, 1.0, 1.0), 1);
        // Q2: lower-right (below & right).
        assert_eq!(q(0.0, 0.0, 1.0, -1.0), 2);
        // Q3: lower-left (below & left).
        assert_eq!(q(0.0, 0.0, -1.0, -1.0), 3);
        // Q4: upper-left (above & left).
        assert_eq!(q(0.0, 0.0, -1.0, 1.0), 4);
    }

    #[test]
    fn quadrant_on_axes_lowest_adjacent() {
        // On +x axis (right, same y): above-or-horiz && right-or-vert -> Q1.
        assert_eq!(q(0.0, 0.0, 5.0, 0.0), 1);
        // On +y axis (above, same x): above && right-or-vert(vert true) -> Q1.
        assert_eq!(q(0.0, 0.0, 0.0, 5.0), 1);
        // On -x axis (left, same y): below-or-horiz(horiz) && left -> Q3.
        assert_eq!(q(0.0, 0.0, -5.0, 0.0), 3);
        // On -y axis (below, same x): below && right-or-vert(vert) -> Q2.
        assert_eq!(q(0.0, 0.0, 0.0, -5.0), 2);
    }

    #[test]
    fn quadrant_relative_to_nonzero_centroid() {
        // centroid (10, 20).
        assert_eq!(q(10.0, 20.0, 11.0, 21.0), 1); // up-right
        assert_eq!(q(10.0, 20.0, 11.0, 19.0), 2); // down-right
        assert_eq!(q(10.0, 20.0, 9.0, 19.0), 3); // down-left
        assert_eq!(q(10.0, 20.0, 9.0, 21.0), 4); // up-left
    }

    #[test]
    fn box_contain_pt_inclusive_edges() {
        let b = BOX {
            high: Point { x: 10.0, y: 10.0 },
            low: Point { x: 0.0, y: 0.0 },
        };
        let mut bb = b;
        let mut inside = Point { x: 5.0, y: 5.0 };
        let mut edge = Point { x: 0.0, y: 10.0 };
        let mut outside = Point { x: 11.0, y: 5.0 };
        unsafe {
            assert!(box_contain_pt(&mut bb, &mut inside));
            assert!(box_contain_pt(&mut bb, &mut edge));
            assert!(!box_contain_pt(&mut bb, &mut outside));
        }
    }
}
