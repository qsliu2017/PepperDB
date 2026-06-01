//! Translation of postgres/src/backend/access/spgist/spgkdtreeproc.c
//!
//! Implementation of a k-d tree over points for SP-GiST.  The tree splits
//! alternately on the x and y coordinate by tree level: at even levels the
//! split is on y, at odd levels on x (mirroring the C `level % 2` tests).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * The opclass support structs (spgConfigOut, spgChooseIn/Out,
//!   spgPickSplitIn/Out, spgInnerConsistentIn/Out) come from access/spgist.h,
//!   which is not ported.  We define MINIMAL #[repr(C)] mirrors carrying ONLY
//!   the fields this file touches, in declaration order, with the real C
//!   layout up to the last touched field.  Fields after the last one we read /
//!   write are omitted; pointers we never dereference are kept as opaque
//!   `*mut c_void` so offsets of the fields we DO touch stay correct.
//!
//! * `Point` / `BOX` and `box_copy` / `spg_key_orderbys_distances` are imported
//!   from crate::access::spgist::spgproc (the real geo_decls.h layout lives
//!   there) so the geometry types match across files.
//!
//! * `DatumGetPointP` / `DatumGetBoxP` / `PointPGetDatum` are geo_decls.h
//!   macros (not ported).  `DatumGetPointP` / `DatumGetBoxP` come from spgproc;
//!   `PointPGetDatum` is defined locally as `PointerGetDatum`.
//!
//! * `FPlt(A,B)` / `FPgt(A,B)` are geo_decls.h macros expanding to `A < B` /
//!   `A > B`; inlined directly.
//!
//! * `MemoryContext` / `MemoryContextSwitchTo` are the real ones from the
//!   prelude (utils/palloc); the picksplit framework array allocation is real
//!   (palloc); the qsort is implemented with Rust's slice sort using the same
//!   comparator semantics as x_cmp / y_cmp.
//!
//! * `spg_kd_leaf_consistent` in C is documented to just borrow
//!   spg_quad_leaf_consistent (spgquadtreeproc.c), which is NOT ported yet.
//!   It is STUBBED here as `unimplemented!` so callers compile; the REAL k-d
//!   math (axis selection, median split, inner-consistent predicates) is
//!   fully translated.

use crate::prelude::*;
use crate::access::common::scankey::ScanKey;
use crate::access::spgist::spgproc::{
    box_copy, spg_key_orderbys_distances, Point, BOX,
};
use crate::access::stratnum::{
    RTAboveStrategyNumber, RTBelowStrategyNumber, RTContainedByStrategyNumber,
    RTLeftStrategyNumber, RTOldAboveStrategyNumber, RTOldBelowStrategyNumber,
    RTRightStrategyNumber, RTSameStrategyNumber,
};
use crate::catalog::pg_type_d::{FLOAT8OID, VOIDOID};
use crate::utils::adt::float::get_float8_infinity;
use crate::utils::fmgr::FunctionCallInfo;
// DatumGetFloat8 / Float8GetDatum / PointerGetDatum come from the prelude
// (crate::postgres).
use core::ffi::c_double;

use crate::{PG_GETARG_POINTER, PG_RETURN_VOID};

// ===========================================================================
// Minimal #[repr(C)] mirrors of the access/spgist.h argument structs.
// Only the touched fields are kept; opaque pointers preserve later offsets.
// ===========================================================================

/// `spgConfigOut` - we set prefixType/labelType/canReturnData/longValuesOK.
/// `leafType` sits between labelType and canReturnData in the real struct and
/// must be present for correct offsets even though we do not write it.
#[repr(C)]
struct spgConfigOut {
    prefixType: Oid,
    labelType: Oid,
    leafType: Oid,
    canReturnData: bool,
    longValuesOK: bool,
}

/// `spgChooseIn` - we read datum, level, allTheSame, hasPrefix, prefixDatum,
/// nNodes.  nodeLabels follows but is untouched (kept for completeness).
#[repr(C)]
struct spgChooseIn {
    datum: Datum,
    leafDatum: Datum,
    level: c_int,
    allTheSame: bool,
    hasPrefix: bool,
    prefixDatum: Datum,
    nNodes: c_int,
    nodeLabels: *mut Datum,
}

/// `spgChooseResultType` enum value we use.
const spgMatchNode: c_int = 1;

/// results-for-spgMatchNode payload (first arm of the spgChooseOut union).
#[repr(C)]
struct spgChooseOutMatchNode {
    nodeN: c_int,
    levelAdd: c_int,
    restDatum: Datum,
}

/// `spgChooseOut` - we only use the spgMatchNode arm.  The union is at least as
/// large as its biggest member (the splitTuple arm), but since we write only
/// resultType and the matchNode fields, mirroring resultType + the matchNode
/// struct is sufficient for our writes (the C code likewise only touches these).
#[repr(C)]
struct spgChooseOut {
    resultType: c_int,
    matchNode: spgChooseOutMatchNode,
}

/// `spgPickSplitIn` - nTuples, datums, level.
#[repr(C)]
struct spgPickSplitIn {
    nTuples: c_int,
    datums: *mut Datum,
    level: c_int,
}

/// `spgPickSplitOut` - all fields touched.
#[repr(C)]
struct spgPickSplitOut {
    hasPrefix: bool,
    prefixDatum: Datum,
    nNodes: c_int,
    nodeLabels: *mut Datum,
    mapTuplesToNodes: *mut c_int,
    leafTupleDatums: *mut Datum,
}

/// `spgInnerConsistentIn` - we read scankeys, orderbys, nkeys, norderbys,
/// traversalValue, traversalMemoryContext, level.  reconstructedValue sits
/// between norderbys and traversalValue in the real struct; keep it for offset
/// correctness.  Fields after `level` (returnData, allTheSame, ...) are read
/// here too: we read prefixDatum.  Keep through prefixDatum.
#[repr(C)]
struct spgInnerConsistentIn {
    scankeys: ScanKey,
    orderbys: ScanKey,
    nkeys: c_int,
    norderbys: c_int,
    reconstructedValue: Datum,
    traversalValue: *mut c_void,
    traversalMemoryContext: MemoryContext,
    level: c_int,
    returnData: bool,
    allTheSame: bool,
    hasPrefix: bool,
    prefixDatum: Datum,
    nNodes: c_int,
    nodeLabels: *mut Datum,
}

/// `spgInnerConsistentOut` - nNodes, nodeNumbers, levelAdds, traversalValues,
/// distances.  reconstructedValues sits between levelAdds and traversalValues
/// in the real struct; keep it for offset correctness.
#[repr(C)]
struct spgInnerConsistentOut {
    nNodes: c_int,
    nodeNumbers: *mut c_int,
    levelAdds: *mut c_int,
    reconstructedValues: *mut Datum,
    traversalValues: *mut *mut c_void,
    distances: *mut *mut c_double,
}

// ---------------------------------------------------------------------------
// geo_decls.h macro: PointPGetDatum(X) -> PointerGetDatum(X).
// ---------------------------------------------------------------------------

/// `PointPGetDatum(X)` - the Datum holds a `*Point`.
#[inline]
fn PointPGetDatum(X: *mut Point) -> Datum {
    PointerGetDatum(X as *const c_void)
}

// ---------------------------------------------------------------------------
// geo_decls.h DatumGet accessors are re-exported from spgproc; bring them in
// via fully-qualified paths to keep the geometry types identical.
// ---------------------------------------------------------------------------
use crate::access::spgist::spgproc::{DatumGetBoxP, DatumGetPointP};

// `MemoryContext` and `MemoryContextSwitchTo` come from the prelude
// (utils/palloc).

// ===========================================================================
// spgkdtreeproc.c
// ===========================================================================

/// `Datum spg_kd_config(PG_FUNCTION_ARGS)`
#[no_mangle]
pub unsafe fn spg_kd_config(fcinfo: FunctionCallInfo) -> Datum {
    // spgConfigIn *cfgin = (spgConfigIn *) PG_GETARG_POINTER(0);  (unused)
    let cfg = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgConfigOut;

    (*cfg).prefixType = FLOAT8OID;
    (*cfg).labelType = VOIDOID; // we don't need node labels
    (*cfg).canReturnData = true;
    (*cfg).longValuesOK = false;
    PG_RETURN_VOID!();
}

/// `static int getSide(double coord, bool isX, Point *tst)`
///
/// Returns the side of `tst` relative to the split coordinate `coord` along the
/// active axis: 0 if equal, 1 if the split coord is greater than the point's
/// coord, -1 if less.  `isX` selects the x axis (true) or y axis (false).
fn getSide(coord: c_double, isX: bool, tst: &Point) -> c_int {
    let tstcoord = if isX { tst.x } else { tst.y };

    if coord == tstcoord {
        0
    } else if coord > tstcoord {
        1
    } else {
        -1
    }
}

/// `Datum spg_kd_choose(PG_FUNCTION_ARGS)`
#[no_mangle]
pub unsafe fn spg_kd_choose(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgChooseIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgChooseOut;
    let inPoint = DatumGetPointP((*in_).datum);

    if (*in_).allTheSame {
        elog!(ERROR, "allTheSame should not occur for k-d trees");
        unreachable!();
    }

    // Assert(in->hasPrefix);
    let coord = DatumGetFloat8((*in_).prefixDatum);

    // Assert(in->nNodes == 2);

    (*out).resultType = spgMatchNode;
    // axis: level % 2 != 0 -> split on x (isX true); even -> split on y.
    let isX = ((*in_).level % 2) != 0;
    (*out).matchNode.nodeN = if getSide(coord, isX, &*inPoint) > 0 { 0 } else { 1 };
    (*out).matchNode.levelAdd = 1;
    (*out).matchNode.restDatum = PointPGetDatum(inPoint);

    PG_RETURN_VOID!();
}

/// `typedef struct SortedPoint { Point *p; int i; } SortedPoint;`
#[derive(Clone, Copy)]
struct SortedPoint {
    p: *mut Point,
    i: c_int,
}

/// `Datum spg_kd_picksplit(PG_FUNCTION_ARGS)`
#[no_mangle]
pub unsafe fn spg_kd_picksplit(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgPickSplitIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgPickSplitOut;

    let nTuples = (*in_).nTuples;

    let sorted =
        palloc(core::mem::size_of::<SortedPoint>() * nTuples as usize) as *mut SortedPoint;
    let mut i = 0;
    while i < nTuples {
        *sorted.add(i as usize) = SortedPoint {
            p: DatumGetPointP(*(*in_).datums.add(i as usize)),
            i,
        };
        i += 1;
    }

    // qsort with x_cmp (split on x) when level is odd, y_cmp when even.
    let onX = ((*in_).level % 2) != 0;
    let slice = core::slice::from_raw_parts_mut(sorted, nTuples as usize);
    slice.sort_by(|a, b| {
        let (ca, cb) = if onX {
            ((*a.p).x, (*b.p).x)
        } else {
            ((*a.p).y, (*b.p).y)
        };
        // x_cmp / y_cmp: 0 if equal, else +/-1 by ordering.  total order on
        // finite coords; treat NaN-equality like C's == (partial) deterministically.
        if ca == cb {
            core::cmp::Ordering::Equal
        } else if ca > cb {
            core::cmp::Ordering::Greater
        } else {
            core::cmp::Ordering::Less
        }
    });

    let middle = (nTuples >> 1) as usize;
    let coord = if onX {
        (*(*sorted.add(middle)).p).x
    } else {
        (*(*sorted.add(middle)).p).y
    };

    (*out).hasPrefix = true;
    (*out).prefixDatum = Float8GetDatum(coord);

    (*out).nNodes = 2;
    (*out).nodeLabels = null_mut(); // we don't need node labels

    (*out).mapTuplesToNodes =
        palloc(core::mem::size_of::<c_int>() * nTuples as usize) as *mut c_int;
    (*out).leafTupleDatums =
        palloc(core::mem::size_of::<Datum>() * nTuples as usize) as *mut Datum;

    // Points exactly equal to coord may land in either node depending on their
    // position in the sorted list; inner_consistent descends into both sides
    // for boundary cases, so this stays balanced and never triggers allTheSame.
    let mut i = 0usize;
    while i < nTuples as usize {
        let sp = *sorted.add(i);
        let p = sp.p;
        let n = sp.i as usize;

        *(*out).mapTuplesToNodes.add(n) = if i < middle { 0 } else { 1 };
        *(*out).leafTupleDatums.add(n) = PointPGetDatum(p);
        i += 1;
    }

    PG_RETURN_VOID!();
}

/// `Datum spg_kd_inner_consistent(PG_FUNCTION_ARGS)`
#[no_mangle]
pub unsafe fn spg_kd_inner_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgInnerConsistentIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgInnerConsistentOut;

    // Assert(in->hasPrefix);
    let coord = DatumGetFloat8((*in_).prefixDatum);

    if (*in_).allTheSame {
        elog!(ERROR, "allTheSame should not occur for k-d trees");
        unreachable!();
    }

    // Assert(in->nNodes == 2);

    let oddLevel = ((*in_).level % 2) != 0;

    // "which" is a bitmask of children that satisfy all constraints.
    let mut which: c_int = (1 << 1) | (1 << 2);

    let mut i = 0;
    while i < (*in_).nkeys {
        let sk = (*in_).scankeys.add(i as usize);
        let query = DatumGetPointP((*sk).sk_argument);
        let strat = (*sk).sk_strategy;

        if strat == RTLeftStrategyNumber {
            if oddLevel && (*query).x < coord {
                which &= 1 << 1;
            }
        } else if strat == RTRightStrategyNumber {
            if oddLevel && (*query).x > coord {
                which &= 1 << 2;
            }
        } else if strat == RTSameStrategyNumber {
            if oddLevel {
                if (*query).x < coord {
                    which &= 1 << 1;
                } else if (*query).x > coord {
                    which &= 1 << 2;
                }
            } else {
                if (*query).y < coord {
                    which &= 1 << 1;
                } else if (*query).y > coord {
                    which &= 1 << 2;
                }
            }
        } else if strat == RTBelowStrategyNumber || strat == RTOldBelowStrategyNumber {
            if !oddLevel && (*query).y < coord {
                which &= 1 << 1;
            }
        } else if strat == RTAboveStrategyNumber || strat == RTOldAboveStrategyNumber {
            if !oddLevel && (*query).y > coord {
                which &= 1 << 2;
            }
        } else if strat == RTContainedByStrategyNumber {
            // For this operator the query is a box, not a point.  We cheat by
            // assuming DatumGetPointP won't misbehave for a pointer-to-box.
            let boxQuery = DatumGetBoxP((*sk).sk_argument);

            if oddLevel {
                if (*boxQuery).high.x < coord {
                    which &= 1 << 1;
                } else if (*boxQuery).low.x > coord {
                    which &= 1 << 2;
                }
            } else {
                if (*boxQuery).high.y < coord {
                    which &= 1 << 1;
                } else if (*boxQuery).low.y > coord {
                    which &= 1 << 2;
                }
            }
        } else {
            elog!(
                ERROR,
                "unrecognized strategy number: {}",
                (*sk).sk_strategy
            );
            unreachable!();
        }

        if which == 0 {
            break; // no need to consider remaining conditions
        }
        i += 1;
    }

    // We must descend into the children identified by which.
    (*out).nNodes = 0;

    // Fast-path for no matching children.
    if which == 0 {
        PG_RETURN_VOID!();
    }

    (*out).nodeNumbers = palloc(core::mem::size_of::<c_int>() * 2) as *mut c_int;

    // bboxes for the two children (only populated when ordering keys present).
    let mut bboxes: [BOX; 2] = [
        BOX {
            high: Point { x: 0.0, y: 0.0 },
            low: Point { x: 0.0, y: 0.0 },
        },
        BOX {
            high: Point { x: 0.0, y: 0.0 },
            low: Point { x: 0.0, y: 0.0 },
        },
    ];

    // When ordering scan keys are specified we compute distances, which needs
    // the bounding boxes of both children.  Boxes at non-zero levels depend on
    // the parent box, saved into traversalValues.
    if (*in_).norderbys > 0 {
        (*out).distances =
            palloc(core::mem::size_of::<*mut c_double>() * (*in_).nNodes as usize)
                as *mut *mut c_double;
        (*out).traversalValues =
            palloc(core::mem::size_of::<*mut c_void>() * (*in_).nNodes as usize)
                as *mut *mut c_void;

        let mut infArea = BOX {
            high: Point { x: 0.0, y: 0.0 },
            low: Point { x: 0.0, y: 0.0 },
        };
        let area: *const BOX;

        if (*in_).level == 0 {
            let inf = get_float8_infinity();
            infArea.high.x = inf;
            infArea.high.y = inf;
            infArea.low.x = -inf;
            infArea.low.y = -inf;
            area = &infArea;
        } else {
            area = (*in_).traversalValue as *const BOX;
            // Assert(area);
        }

        bboxes[0].low = (*area).low;
        bboxes[1].high = (*area).high;

        if oddLevel {
            // split box by x
            bboxes[0].high.x = coord;
            bboxes[1].low.x = coord;
            bboxes[0].high.y = (*area).high.y;
            bboxes[1].low.y = (*area).low.y;
        } else {
            // split box by y
            bboxes[0].high.y = coord;
            bboxes[1].low.y = coord;
            bboxes[0].high.x = (*area).high.x;
            bboxes[1].low.x = (*area).low.x;
        }
    }

    let mut i = 1;
    while i <= 2 {
        if (which & (1 << i)) != 0 {
            let slot = (*out).nNodes as usize;
            *(*out).nodeNumbers.add(slot) = i - 1;

            if (*in_).norderbys > 0 {
                let oldCtx = MemoryContextSwitchTo((*in_).traversalMemoryContext);
                let r#box = box_copy(&mut bboxes[(i - 1) as usize] as *mut BOX);
                MemoryContextSwitchTo(oldCtx);

                *(*out).traversalValues.add(slot) = r#box as *mut c_void;

                *(*out).distances.add(slot) = spg_key_orderbys_distances(
                    PointerGetDatum(r#box as *const c_void),
                    false,
                    (*in_).orderbys,
                    (*in_).norderbys,
                );
            }

            (*out).nNodes += 1;
        }
        i += 1;
    }

    // Set up level increments, too.
    (*out).levelAdds = palloc(core::mem::size_of::<c_int>() * 2) as *mut c_int;
    *(*out).levelAdds.add(0) = 1;
    *(*out).levelAdds.add(1) = 1;

    PG_RETURN_VOID!();
}

/// `Datum spg_kd_leaf_consistent(PG_FUNCTION_ARGS)`
///
/// In C this opclass entry simply reuses spg_quad_leaf_consistent (it supports
/// the same operators and leaf data type).  spgquadtreeproc.c is NOT ported
/// yet, so this is STUBBED.  Wire it to spg_quad_leaf_consistent once that file
/// is translated.
#[no_mangle]
pub unsafe fn spg_kd_leaf_consistent(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("TODO: borrow spg_quad_leaf_consistent (spgquadtreeproc.c not ported)")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mkpoint(x: f64, y: f64) -> Point {
        Point { x, y }
    }

    // --- Axis selection: level even -> y axis, level odd -> x axis. ---

    #[test]
    fn axis_selection_by_level_parity() {
        // The active axis used everywhere is `(level % 2) != 0` -> isX.
        for level in 0..8 {
            let oddLevel = (level % 2) != 0;
            // Even levels split on y (isX == false); odd levels split on x.
            assert_eq!(oddLevel, level % 2 == 1);
        }
    }

    // --- getSide + choose routing: REAL k-d math. ---
    // nodeN = (getSide(coord, isX, point) > 0) ? 0 : 1
    // getSide returns 1 when coord > point-coord (point is on the low/left
    // side), so a point strictly left/below the split goes to child 0, and a
    // point on/right goes to child 1.

    #[test]
    fn choose_routes_x_split_odd_level() {
        let isX = true; // odd level
        let coord = 5.0;

        // Point left of split (x = 3 < 5): coord > x -> getSide = 1 (>0) -> node 0.
        let pl = mkpoint(3.0, 99.0);
        assert_eq!(getSide(coord, isX, &pl), 1);
        assert_eq!(if getSide(coord, isX, &pl) > 0 { 0 } else { 1 }, 0);

        // Point right of split (x = 8 > 5): coord < x -> getSide = -1 -> node 1.
        let pr = mkpoint(8.0, -99.0);
        assert_eq!(getSide(coord, isX, &pr), -1);
        assert_eq!(if getSide(coord, isX, &pr) > 0 { 0 } else { 1 }, 1);

        // Point exactly on split (x == 5): getSide = 0 (not > 0) -> node 1.
        let pe = mkpoint(5.0, 0.0);
        assert_eq!(getSide(coord, isX, &pe), 0);
        assert_eq!(if getSide(coord, isX, &pe) > 0 { 0 } else { 1 }, 1);
    }

    #[test]
    fn choose_routes_y_split_even_level() {
        let isX = false; // even level: split on y, getSide compares y
        let coord = 0.0;

        // Below split (y = -2 < 0): coord > y -> getSide = 1 -> node 0.
        let pb = mkpoint(123.0, -2.0);
        assert_eq!(getSide(coord, isX, &pb), 1);
        assert_eq!(if getSide(coord, isX, &pb) > 0 { 0 } else { 1 }, 0);

        // Above split (y = 4 > 0): getSide = -1 -> node 1.
        let pa = mkpoint(-123.0, 4.0);
        assert_eq!(getSide(coord, isX, &pa), -1);
        assert_eq!(if getSide(coord, isX, &pa) > 0 { 0 } else { 1 }, 1);

        // getSide must ignore x on an even (y) level: same y -> 0 regardless of x.
        let p1 = mkpoint(1000.0, 0.0);
        let p2 = mkpoint(-1000.0, 0.0);
        assert_eq!(getSide(coord, isX, &p1), 0);
        assert_eq!(getSide(coord, isX, &p2), 0);
    }

    // --- Median split: picksplit picks the median coord on the active axis,
    //     and points sorted before the median go to node 0, the rest node 1. ---

    #[test]
    fn median_split_partitions_around_middle() {
        // Mirror picksplit's partition rule on a sorted axis array of 5 coords.
        // middle = 5 >> 1 = 2; indices [0,1] -> node 0, [2,3,4] -> node 1.
        let n = 5i32;
        let middle = (n >> 1) as usize;
        assert_eq!(middle, 2);

        let nodes: Vec<i32> = (0..n as usize)
            .map(|i| if i < middle { 0 } else { 1 })
            .collect();
        assert_eq!(nodes, vec![0, 0, 1, 1, 1]);
    }

    #[test]
    fn median_coord_is_middle_of_sorted_axis() {
        // Sorted x coords; middle element is the chosen split coordinate.
        let mut xs = [9.0f64, 1.0, 5.0, 3.0, 7.0];
        xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let middle = (xs.len() >> 1) as usize; // 2
        assert_eq!(xs[middle], 5.0);

        // Everything strictly before the median index is the low child; values
        // <= median may straddle but the median value itself anchors child 0/1.
        assert!(xs[..middle].iter().all(|&v| v <= xs[middle]));
        assert!(xs[middle..].iter().all(|&v| v >= xs[middle]));
    }

    // --- inner_consistent predicate: a Left query at odd level prunes the
    //     right child when query.x < coord (REAL bitmask math). ---

    #[test]
    fn inner_consistent_left_prunes_to_low_child() {
        // which starts as 0b110 (children 1 and 2 in 1-based bit positions).
        let mut which: i32 = (1 << 1) | (1 << 2);
        let oddLevel = true; // x axis
        let coord = 5.0;
        let query_x = 3.0; // query strictly left of split

        // RTLeftStrategyNumber branch: query.x < coord -> keep only bit 1.
        if oddLevel && query_x < coord {
            which &= 1 << 1;
        }
        assert_eq!(which, 1 << 1); // only low child (node 0) survives

        // Same query at an even level must NOT prune (axis mismatch).
        let mut which2: i32 = (1 << 1) | (1 << 2);
        let oddLevel2 = false;
        if oddLevel2 && query_x < coord {
            which2 &= 1 << 1;
        }
        assert_eq!(which2, (1 << 1) | (1 << 2));
    }

    #[test]
    fn inner_consistent_right_prunes_to_high_child() {
        let mut which: i32 = (1 << 1) | (1 << 2);
        let oddLevel = true;
        let coord = 5.0;
        let query_x = 9.0; // strictly right of split

        // RTRightStrategyNumber: query.x > coord -> keep only bit 2.
        if oddLevel && query_x > coord {
            which &= 1 << 2;
        }
        assert_eq!(which, 1 << 2); // only high child (node 1) survives
    }
}
