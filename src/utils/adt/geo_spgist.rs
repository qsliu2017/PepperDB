//! Translation of postgres/src/backend/utils/adt/geo_spgist.c
//!
//! SP-GiST implementation of 4-dimensional quad tree over boxes.
//!
//! This module provides an SP-GiST implementation for boxes using a quad tree
//! analogy in 4-dimensional space.  2D boxes are treated as points in 4D space
//! (low.x, high.x, low.y, high.y); the tree splits into 16 quadrants.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * `Point` / `BOX` and the geo accessors `DatumGetBoxP` / `DatumGetPointP`,
//!   plus `spg_key_orderbys_distances`, are imported from
//!   crate::access::spgist::spgproc (geo_decls.h itself is NOT ported -- spgproc
//!   defines the minimal mirrors).  We import them rather than redefine so the
//!   Point/BOX types match exactly.
//!
//! * The spgConfigOut, spgChooseIn/Out, spgPickSplitIn/Out,
//!   spgInnerConsistentIn/Out, spgLeafConsistentIn/Out structs come from
//!   access/spgist.h (NOT ported).  We define MINIMAL #[repr(C)] mirrors below
//!   containing ONLY the fields these functions touch -- mirroring the field
//!   sets used by the already-ported spgquadtreeproc.rs, plus the extra fields
//!   this opclass touches (spgConfigOut.leafType, spgPickSplitOut.mapTuplesToNodes,
//!   ScanKey.sk_subtype, spgLeafConsistentOut.recheckDistances / returnData).
//!
//! * The leaf-consistent box predicates (box_overlap/contain/contained/same/
//!   left/right/above/below/over*) dispatch via DirectFunctionCall2 in C, into
//!   geo_ops.c (NOT ported).  Following the precedent of spgquadtreeproc.rs
//!   (which inlines box_contain_pt), each predicate is implemented here as the
//!   exact coordinate comparison from geo_ops.c.  This keeps the REAL strategy
//!   math testable without the catalog/fmgr dispatch.
//!
//! * `FPlt/FPle/FPgt/FPge` are geo_decls.h macros.  In PG18 EPSILON is 0.0, so
//!   they are plain `< <= > >=` comparisons (implemented inline below).
//!
//! * `POLYGON` (geo_decls.h) is NOT ported; a minimal mirror with `boundbox`
//!   is defined locally for the polygon scankey / compress paths.
//!
//! * Type OIDs BOXOID / VOIDOID / POLYGONOID come from catalog/pg_type.h;
//!   stubbed as local consts.  `F_DIST_POLYP` (fmgroids.h) stubbed as a const.
//!
//! * MemoryContextSwitchTo is stubbed (identity, no-op): the inner-consistent
//!   traversal-value allocations still go through palloc.

use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_DATUM, PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_VOID};
use crate::access::common::scankey::ScanKey;
use crate::access::spgist::spgproc::{
    spg_key_orderbys_distances, DatumGetBoxP, DatumGetPointP, Point, BOX,
};
use crate::access::stratnum::{
    RTAboveStrategyNumber, RTBelowStrategyNumber, RTContainedByStrategyNumber,
    RTContainsStrategyNumber, RTLeftStrategyNumber, RTOverAboveStrategyNumber,
    RTOverBelowStrategyNumber, RTOverLeftStrategyNumber, RTOverRightStrategyNumber,
    RTOverlapStrategyNumber, RTRightStrategyNumber, RTSameStrategyNumber,
};
use crate::port::qsort::pg_qsort;
use core::ffi::{c_double, c_void};

// ===========================================================================
// Stubbed type OIDs (catalog/pg_type.h) and fmgroids.h.
// ===========================================================================

const BOXOID: Oid = 603;
const VOIDOID: Oid = 2278;
const POLYGONOID: Oid = 604;

/// `F_DIST_POLYP` from fmgroids.h (OID of the point <-> polygon distance fn).
const F_DIST_POLYP: Oid = 3275;

type StrategyNumber = u16;
type float8 = c_double;

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
// Minimal geo_decls.h POLYGON mirror (only the boundbox field is touched).
// ===========================================================================

/// `POLYGON` from geo_decls.h (only the bounding box is used here).  In C the
/// full struct also carries vl_len_, npts and the point array; we only need the
/// `boundbox` for the scankey / compress paths.
#[repr(C)]
pub struct POLYGON {
    pub vl_len_: i32,
    pub npts: i32,
    pub boundbox: BOX,
    // p[] flexible array omitted.
}

/// `DatumGetPolygonP(X)` -- the Datum holds a `*POLYGON`.
#[inline]
unsafe fn DatumGetPolygonP(X: Datum) -> *mut POLYGON {
    DatumGetPointer(X) as *mut POLYGON
}

/// `BoxPGetDatum(X)` -- a BOX pointer carried as a Datum.
#[inline]
fn BoxPGetDatum(b: *mut BOX) -> Datum {
    PointerGetDatum(b as *const c_void)
}

// ===========================================================================
// geo_decls.h FP* comparison macros (EPSILON == 0.0 in PG18 -> plain cmp).
// ===========================================================================

#[inline]
fn FPlt(a: float8, b: float8) -> bool {
    a < b
}
#[inline]
fn FPle(a: float8, b: float8) -> bool {
    a <= b
}
#[inline]
fn FPgt(a: float8, b: float8) -> bool {
    a > b
}
#[inline]
fn FPge(a: float8, b: float8) -> bool {
    a >= b
}

// ===========================================================================
// utils/float.h: get_float8_infinity / HYPOT.
// ===========================================================================

#[inline]
fn get_float8_infinity() -> float8 {
    c_double::INFINITY
}

#[inline]
fn HYPOT(dx: float8, dy: float8) -> float8 {
    dx.hypot(dy)
}

// ===========================================================================
// Minimal access/spgist.h struct mirrors (only the touched fields).
// ===========================================================================

/// `spgConfigOut` (touched fields only).
#[repr(C)]
pub struct spgConfigOut {
    pub prefixType: Oid,
    pub labelType: Oid,
    pub leafType: Oid,
    pub canReturnData: bool,
    pub longValuesOK: bool,
}

/// Result-type tag for spgChooseOut / framework.
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
    pub leafDatum: Datum,
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
    pub returnData: bool,
    pub leafDatum: Datum,
}

/// `spgLeafConsistentOut` (touched fields only).
#[repr(C)]
pub struct spgLeafConsistentOut {
    pub leafValue: Datum,
    pub recheck: bool,
    pub recheckDistances: bool,
    pub distances: *mut c_double,
}

// ===========================================================================
// 4D quad-tree supporting structures.
// ===========================================================================

#[repr(C)]
#[derive(Clone, Copy)]
struct Range {
    low: float8,
    high: float8,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RangeBox {
    left: Range,
    right: Range,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RectBox {
    range_box_x: RangeBox,
    range_box_y: RangeBox,
}

/// Comparator for qsort over float8.
///
/// `static int compareDoubles(const void *a, const void *b)`
unsafe fn compareDoubles(a: *const c_void, b: *const c_void) -> c_int {
    let x = *(a as *const float8);
    let y = *(b as *const float8);

    if x == y {
        return 0;
    }
    if x > y {
        1
    } else {
        -1
    }
}

/// Calculate the quadrant.
///
/// The quadrant is an 8-bit unsigned integer with 4 least bits in use.  All 4
/// bits are set by comparing a corner of the box, making 16 quadrants total.
///
/// `static uint8 getQuadrant(BOX *centroid, BOX *inBox)`
unsafe fn getQuadrant(centroid: *mut BOX, inBox: *mut BOX) -> u8 {
    let mut quadrant: u8 = 0;

    if (*inBox).low.x > (*centroid).low.x {
        quadrant |= 0x8;
    }
    if (*inBox).high.x > (*centroid).high.x {
        quadrant |= 0x4;
    }
    if (*inBox).low.y > (*centroid).low.y {
        quadrant |= 0x2;
    }
    if (*inBox).high.y > (*centroid).high.y {
        quadrant |= 0x1;
    }

    quadrant
}

/// Get RangeBox using BOX.
///
/// `static RangeBox *getRangeBox(BOX *box)`
unsafe fn getRangeBox(r#box: *mut BOX) -> *mut RangeBox {
    let range_box = palloc(core::mem::size_of::<RangeBox>()) as *mut RangeBox;

    (*range_box).left.low = (*r#box).low.x;
    (*range_box).left.high = (*r#box).high.x;

    (*range_box).right.low = (*r#box).low.y;
    (*range_box).right.high = (*r#box).high.y;

    range_box
}

/// Initialize the traversal value to cover the whole 4D space.
///
/// `static RectBox *initRectBox(void)`
unsafe fn initRectBox() -> *mut RectBox {
    let rect_box = palloc(core::mem::size_of::<RectBox>()) as *mut RectBox;
    let infinity = get_float8_infinity();

    (*rect_box).range_box_x.left.low = -infinity;
    (*rect_box).range_box_x.left.high = infinity;

    (*rect_box).range_box_x.right.low = -infinity;
    (*rect_box).range_box_x.right.high = infinity;

    (*rect_box).range_box_y.left.low = -infinity;
    (*rect_box).range_box_y.left.high = infinity;

    (*rect_box).range_box_y.right.low = -infinity;
    (*rect_box).range_box_y.right.high = infinity;

    rect_box
}

/// Calculate the next traversal value, using centroid and quadrant.
///
/// `static RectBox *nextRectBox(RectBox *rect_box, RangeBox *centroid, uint8 quadrant)`
unsafe fn nextRectBox(
    rect_box: *mut RectBox,
    centroid: *mut RangeBox,
    quadrant: u8,
) -> *mut RectBox {
    let next_rect_box = palloc(core::mem::size_of::<RectBox>()) as *mut RectBox;

    *next_rect_box = *rect_box;

    if quadrant & 0x8 != 0 {
        (*next_rect_box).range_box_x.left.low = (*centroid).left.low;
    } else {
        (*next_rect_box).range_box_x.left.high = (*centroid).left.low;
    }

    if quadrant & 0x4 != 0 {
        (*next_rect_box).range_box_x.right.low = (*centroid).left.high;
    } else {
        (*next_rect_box).range_box_x.right.high = (*centroid).left.high;
    }

    if quadrant & 0x2 != 0 {
        (*next_rect_box).range_box_y.left.low = (*centroid).right.low;
    } else {
        (*next_rect_box).range_box_y.left.high = (*centroid).right.low;
    }

    if quadrant & 0x1 != 0 {
        (*next_rect_box).range_box_y.right.low = (*centroid).right.high;
    } else {
        (*next_rect_box).range_box_y.right.high = (*centroid).right.high;
    }

    next_rect_box
}

/* Can any range from range_box overlap with this argument? */
unsafe fn overlap2D(range_box: *mut RangeBox, query: *mut Range) -> bool {
    FPge((*range_box).right.high, (*query).low) && FPle((*range_box).left.low, (*query).high)
}

/* Can any rectangle from rect_box overlap with this argument? */
unsafe fn overlap4D(rect_box: *mut RectBox, query: *mut RangeBox) -> bool {
    overlap2D(&mut (*rect_box).range_box_x, &mut (*query).left)
        && overlap2D(&mut (*rect_box).range_box_y, &mut (*query).right)
}

/* Can any range from range_box contain this argument? */
unsafe fn contain2D(range_box: *mut RangeBox, query: *mut Range) -> bool {
    FPge((*range_box).right.high, (*query).high) && FPle((*range_box).left.low, (*query).low)
}

/* Can any rectangle from rect_box contain this argument? */
unsafe fn contain4D(rect_box: *mut RectBox, query: *mut RangeBox) -> bool {
    contain2D(&mut (*rect_box).range_box_x, &mut (*query).left)
        && contain2D(&mut (*rect_box).range_box_y, &mut (*query).right)
}

/* Can any range from range_box be contained by this argument? */
unsafe fn contained2D(range_box: *mut RangeBox, query: *mut Range) -> bool {
    FPle((*range_box).left.low, (*query).high)
        && FPge((*range_box).left.high, (*query).low)
        && FPle((*range_box).right.low, (*query).high)
        && FPge((*range_box).right.high, (*query).low)
}

/* Can any rectangle from rect_box be contained by this argument? */
unsafe fn contained4D(rect_box: *mut RectBox, query: *mut RangeBox) -> bool {
    contained2D(&mut (*rect_box).range_box_x, &mut (*query).left)
        && contained2D(&mut (*rect_box).range_box_y, &mut (*query).right)
}

/* Can any range from range_box to be lower than this argument? */
unsafe fn lower2D(range_box: *mut RangeBox, query: *mut Range) -> bool {
    FPlt((*range_box).left.low, (*query).low) && FPlt((*range_box).right.low, (*query).low)
}

/* Can any range from range_box not extend to the right side of the query? */
unsafe fn overLower2D(range_box: *mut RangeBox, query: *mut Range) -> bool {
    FPle((*range_box).left.low, (*query).high) && FPle((*range_box).right.low, (*query).high)
}

/* Can any range from range_box to be higher than this argument? */
unsafe fn higher2D(range_box: *mut RangeBox, query: *mut Range) -> bool {
    FPgt((*range_box).left.high, (*query).high) && FPgt((*range_box).right.high, (*query).high)
}

/* Can any range from range_box not extend to the left side of the query? */
unsafe fn overHigher2D(range_box: *mut RangeBox, query: *mut Range) -> bool {
    FPge((*range_box).left.high, (*query).low) && FPge((*range_box).right.high, (*query).low)
}

/* Can any rectangle from rect_box be left of this argument? */
unsafe fn left4D(rect_box: *mut RectBox, query: *mut RangeBox) -> bool {
    lower2D(&mut (*rect_box).range_box_x, &mut (*query).left)
}

/* Can any rectangle from rect_box not extend to the right of this argument? */
unsafe fn overLeft4D(rect_box: *mut RectBox, query: *mut RangeBox) -> bool {
    overLower2D(&mut (*rect_box).range_box_x, &mut (*query).left)
}

/* Can any rectangle from rect_box be right of this argument? */
unsafe fn right4D(rect_box: *mut RectBox, query: *mut RangeBox) -> bool {
    higher2D(&mut (*rect_box).range_box_x, &mut (*query).left)
}

/* Can any rectangle from rect_box not extend to the left of this argument? */
unsafe fn overRight4D(rect_box: *mut RectBox, query: *mut RangeBox) -> bool {
    overHigher2D(&mut (*rect_box).range_box_x, &mut (*query).left)
}

/* Can any rectangle from rect_box be below of this argument? */
unsafe fn below4D(rect_box: *mut RectBox, query: *mut RangeBox) -> bool {
    lower2D(&mut (*rect_box).range_box_y, &mut (*query).right)
}

/* Can any rectangle from rect_box not extend above this argument? */
unsafe fn overBelow4D(rect_box: *mut RectBox, query: *mut RangeBox) -> bool {
    overLower2D(&mut (*rect_box).range_box_y, &mut (*query).right)
}

/* Can any rectangle from rect_box be above of this argument? */
unsafe fn above4D(rect_box: *mut RectBox, query: *mut RangeBox) -> bool {
    higher2D(&mut (*rect_box).range_box_y, &mut (*query).right)
}

/* Can any rectangle from rect_box not extend below of this argument? */
unsafe fn overAbove4D(rect_box: *mut RectBox, query: *mut RangeBox) -> bool {
    overHigher2D(&mut (*rect_box).range_box_y, &mut (*query).right)
}

/// Lower bound for the distance between point and rect_box.
///
/// `static double pointToRectBoxDistance(Point *point, RectBox *rect_box)`
unsafe fn pointToRectBoxDistance(point: *mut Point, rect_box: *mut RectBox) -> c_double {
    let dx: c_double;
    let dy: c_double;

    if (*point).x < (*rect_box).range_box_x.left.low {
        dx = (*rect_box).range_box_x.left.low - (*point).x;
    } else if (*point).x > (*rect_box).range_box_x.right.high {
        dx = (*point).x - (*rect_box).range_box_x.right.high;
    } else {
        dx = 0.0;
    }

    if (*point).y < (*rect_box).range_box_y.left.low {
        dy = (*rect_box).range_box_y.left.low - (*point).y;
    } else if (*point).y > (*rect_box).range_box_y.right.high {
        dy = (*point).y - (*rect_box).range_box_y.right.high;
    } else {
        dy = 0.0;
    }

    HYPOT(dx, dy)
}

// ===========================================================================
// geo_ops.c box predicates (NOT ported) -- inlined exact coordinate cmps.
//
// In C the leaf-consistent path dispatches these through DirectFunctionCall2;
// here we apply the same comparisons directly.  `box1` is the leaf, `box2` the
// query, matching DirectFunctionCall2(box_X, leaf, query).
// ===========================================================================

unsafe fn box_ov(box1: *mut BOX, box2: *mut BOX) -> bool {
    FPle((*box1).low.x, (*box2).high.x)
        && FPle((*box2).low.x, (*box1).high.x)
        && FPle((*box1).low.y, (*box2).high.y)
        && FPle((*box2).low.y, (*box1).high.y)
}

unsafe fn box_overlap(box1: *mut BOX, box2: *mut BOX) -> bool {
    box_ov(box1, box2)
}

/// Is the second box in the first box or on its border?
unsafe fn box_contain_box(contains_box: *mut BOX, contained_box: *mut BOX) -> bool {
    FPge((*contains_box).high.x, (*contained_box).high.x)
        && FPle((*contains_box).low.x, (*contained_box).low.x)
        && FPge((*contains_box).high.y, (*contained_box).high.y)
        && FPle((*contains_box).low.y, (*contained_box).low.y)
}

unsafe fn box_contain(box1: *mut BOX, box2: *mut BOX) -> bool {
    box_contain_box(box1, box2)
}

unsafe fn box_contained(box1: *mut BOX, box2: *mut BOX) -> bool {
    box_contain_box(box2, box1)
}

unsafe fn box_same(box1: *mut BOX, box2: *mut BOX) -> bool {
    (*box1).high.x == (*box2).high.x
        && (*box1).high.y == (*box2).high.y
        && (*box1).low.x == (*box2).low.x
        && (*box1).low.y == (*box2).low.y
}

unsafe fn box_left(box1: *mut BOX, box2: *mut BOX) -> bool {
    FPlt((*box1).high.x, (*box2).low.x)
}

unsafe fn box_overleft(box1: *mut BOX, box2: *mut BOX) -> bool {
    FPle((*box1).high.x, (*box2).high.x)
}

unsafe fn box_right(box1: *mut BOX, box2: *mut BOX) -> bool {
    FPgt((*box1).low.x, (*box2).high.x)
}

unsafe fn box_overright(box1: *mut BOX, box2: *mut BOX) -> bool {
    FPge((*box1).low.x, (*box2).low.x)
}

unsafe fn box_below(box1: *mut BOX, box2: *mut BOX) -> bool {
    FPlt((*box1).high.y, (*box2).low.y)
}

unsafe fn box_overbelow(box1: *mut BOX, box2: *mut BOX) -> bool {
    FPle((*box1).high.y, (*box2).high.y)
}

unsafe fn box_above(box1: *mut BOX, box2: *mut BOX) -> bool {
    FPgt((*box1).low.y, (*box2).high.y)
}

unsafe fn box_overabove(box1: *mut BOX, box2: *mut BOX) -> bool {
    FPge((*box1).low.y, (*box2).low.y)
}

// ===========================================================================
// SP-GiST opclass support functions.
// ===========================================================================

/// SP-GiST config function.
///
/// `Datum spg_box_quad_config(PG_FUNCTION_ARGS)`
pub unsafe fn spg_box_quad_config(fcinfo: FunctionCallInfo) -> Datum {
    let cfg = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgConfigOut;

    (*cfg).prefixType = BOXOID;
    (*cfg).labelType = VOIDOID; // We don't need node labels.
    (*cfg).canReturnData = true;
    (*cfg).longValuesOK = false;

    PG_RETURN_VOID!()
}

/// SP-GiST choose function.
///
/// `Datum spg_box_quad_choose(PG_FUNCTION_ARGS)`
pub unsafe fn spg_box_quad_choose(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgChooseIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgChooseOut;
    let centroid = DatumGetBoxP((*in_).prefixDatum);
    let r#box = DatumGetBoxP((*in_).leafDatum);

    (*out).resultType = spgMatchNode;
    (*out).result.restDatum = BoxPGetDatum(r#box);

    // nodeN will be set by core, when allTheSame.
    if !(*in_).allTheSame {
        (*out).result.nodeN = getQuadrant(centroid, r#box) as c_int;
    }

    PG_RETURN_VOID!()
}

/// SP-GiST pick-split function.
///
/// Splits a list of boxes into quadrants by choosing a central 4D point as the
/// median of the coordinates of the boxes.
///
/// `Datum spg_box_quad_picksplit(PG_FUNCTION_ARGS)`
pub unsafe fn spg_box_quad_picksplit(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgPickSplitIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgPickSplitOut;

    let nt = (*in_).nTuples as usize;
    let lowXs = palloc(core::mem::size_of::<float8>() * nt) as *mut float8;
    let highXs = palloc(core::mem::size_of::<float8>() * nt) as *mut float8;
    let lowYs = palloc(core::mem::size_of::<float8>() * nt) as *mut float8;
    let highYs = palloc(core::mem::size_of::<float8>() * nt) as *mut float8;

    // Calculate median of all 4D coordinates.
    let mut i = 0;
    while i < (*in_).nTuples {
        let r#box = DatumGetBoxP(*(*in_).datums.add(i as usize));

        *lowXs.add(i as usize) = (*r#box).low.x;
        *highXs.add(i as usize) = (*r#box).high.x;
        *lowYs.add(i as usize) = (*r#box).low.y;
        *highYs.add(i as usize) = (*r#box).high.y;
        i += 1;
    }

    pg_qsort(lowXs as *mut c_void, nt, core::mem::size_of::<float8>(), compareDoubles);
    pg_qsort(highXs as *mut c_void, nt, core::mem::size_of::<float8>(), compareDoubles);
    pg_qsort(lowYs as *mut c_void, nt, core::mem::size_of::<float8>(), compareDoubles);
    pg_qsort(highYs as *mut c_void, nt, core::mem::size_of::<float8>(), compareDoubles);

    let median = (*in_).nTuples / 2;

    let centroid = palloc(core::mem::size_of::<BOX>()) as *mut BOX;

    (*centroid).low.x = *lowXs.add(median as usize);
    (*centroid).high.x = *highXs.add(median as usize);
    (*centroid).low.y = *lowYs.add(median as usize);
    (*centroid).high.y = *highYs.add(median as usize);

    // Fill the output.
    (*out).hasPrefix = true;
    (*out).prefixDatum = BoxPGetDatum(centroid);

    (*out).nNodes = 16;
    (*out).nodeLabels = core::ptr::null_mut(); // We don't need node labels.

    (*out).mapTuplesToNodes = palloc(core::mem::size_of::<c_int>() * nt) as *mut c_int;
    (*out).leafTupleDatums = palloc(core::mem::size_of::<Datum>() * nt) as *mut Datum;

    // Assign ranges to corresponding nodes according to quadrants relative to
    // the "centroid" range.
    let mut i = 0;
    while i < (*in_).nTuples {
        let r#box = DatumGetBoxP(*(*in_).datums.add(i as usize));
        let quadrant = getQuadrant(centroid, r#box);

        *(*out).leafTupleDatums.add(i as usize) = BoxPGetDatum(r#box);
        *(*out).mapTuplesToNodes.add(i as usize) = quadrant as c_int;
        i += 1;
    }

    PG_RETURN_VOID!()
}

/// Check if result of consistent method based on bounding box is exact.
///
/// `static bool is_bounding_box_test_exact(StrategyNumber strategy)`
fn is_bounding_box_test_exact(strategy: StrategyNumber) -> bool {
    matches!(
        strategy,
        RTLeftStrategyNumber
            | RTOverLeftStrategyNumber
            | RTOverRightStrategyNumber
            | RTRightStrategyNumber
            | RTOverBelowStrategyNumber
            | RTBelowStrategyNumber
            | RTAboveStrategyNumber
            | RTOverAboveStrategyNumber
    )
}

/// Get bounding box for ScanKey.
///
/// `static BOX *spg_box_quad_get_scankey_bbox(ScanKey sk, bool *recheck)`
unsafe fn spg_box_quad_get_scankey_bbox(sk: ScanKey, recheck: *mut bool) -> *mut BOX {
    match (*sk).sk_subtype {
        BOXOID => DatumGetBoxP((*sk).sk_argument),

        POLYGONOID => {
            if !recheck.is_null() && !is_bounding_box_test_exact((*sk).sk_strategy) {
                *recheck = true;
            }
            &mut (*DatumGetPolygonP((*sk).sk_argument)).boundbox
        }

        other => {
            elog!(ERROR, "unrecognized scankey subtype: {}", other);
                unreachable!()
        }
    }
}

/// SP-GiST inner consistent function.
///
/// `Datum spg_box_quad_inner_consistent(PG_FUNCTION_ARGS)`
pub unsafe fn spg_box_quad_inner_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgInnerConsistentIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgInnerConsistentOut;

    // We are saving the traversal value or initialize it an unbounded one, if
    // we have just begun to walk the tree.
    let rect_box: *mut RectBox = if !(*in_).traversalValue.is_null() {
        (*in_).traversalValue as *mut RectBox
    } else {
        initRectBox()
    };

    if (*in_).allTheSame {
        // Report that all nodes should be visited.
        (*out).nNodes = (*in_).nNodes;
        (*out).nodeNumbers =
            palloc(core::mem::size_of::<c_int>() * (*in_).nNodes as usize) as *mut c_int;
        let mut i = 0;
        while i < (*in_).nNodes {
            *(*out).nodeNumbers.add(i as usize) = i;
            i += 1;
        }

        if (*in_).norderbys > 0 && (*in_).nNodes > 0 {
            let distances =
                palloc(core::mem::size_of::<c_double>() * (*in_).norderbys as usize)
                    as *mut c_double;

            let mut j = 0;
            while j < (*in_).norderbys {
                let pt = DatumGetPointP((*(*in_).orderbys.add(j as usize)).sk_argument);
                *distances.add(j as usize) = pointToRectBoxDistance(pt, rect_box);
                j += 1;
            }

            (*out).distances =
                palloc(core::mem::size_of::<*mut c_double>() * (*in_).nNodes as usize)
                    as *mut *mut c_double;
            *(*out).distances.add(0) = distances;

            let mut i = 1;
            while i < (*in_).nNodes {
                let d = palloc(core::mem::size_of::<c_double>() * (*in_).norderbys as usize)
                    as *mut c_double;
                core::ptr::copy_nonoverlapping(
                    distances,
                    d,
                    (*in_).norderbys as usize,
                );
                *(*out).distances.add(i as usize) = d;
                i += 1;
            }
        }

        PG_RETURN_VOID!()
    }

    // Cast the prefix and queries to RangeBoxes for ease of operations.
    let centroid = getRangeBox(DatumGetBoxP((*in_).prefixDatum));
    let queries = palloc((*in_).nkeys as usize * core::mem::size_of::<*mut RangeBox>())
        as *mut *mut RangeBox;
    let mut i = 0;
    while i < (*in_).nkeys {
        let r#box = spg_box_quad_get_scankey_bbox(
            (*in_).scankeys.add(i as usize),
            core::ptr::null_mut(),
        );
        *queries.add(i as usize) = getRangeBox(r#box);
        i += 1;
    }

    // Allocate enough memory for nodes.
    (*out).nNodes = 0;
    (*out).nodeNumbers =
        palloc(core::mem::size_of::<c_int>() * (*in_).nNodes as usize) as *mut c_int;
    (*out).traversalValues =
        palloc(core::mem::size_of::<*mut c_void>() * (*in_).nNodes as usize) as *mut *mut c_void;
    if (*in_).norderbys > 0 {
        (*out).distances =
            palloc(core::mem::size_of::<*mut c_double>() * (*in_).nNodes as usize)
                as *mut *mut c_double;
    }

    // Switch context for new traversal-value allocations (stubbed identity).
    let old_ctx = MemoryContextSwitchTo((*in_).traversalMemoryContext);

    let mut quadrant: u8 = 0;
    while (quadrant as c_int) < (*in_).nNodes {
        let next_rect_box = nextRectBox(rect_box, centroid, quadrant);
        let mut flag = true;

        let mut i = 0;
        while i < (*in_).nkeys {
            let strategy = (*(*in_).scankeys.add(i as usize)).sk_strategy;
            let q = *queries.add(i as usize);

            flag = match strategy {
                RTOverlapStrategyNumber => overlap4D(next_rect_box, q),
                RTContainsStrategyNumber => contain4D(next_rect_box, q),
                RTSameStrategyNumber | RTContainedByStrategyNumber => {
                    contained4D(next_rect_box, q)
                }
                RTLeftStrategyNumber => left4D(next_rect_box, q),
                RTOverLeftStrategyNumber => overLeft4D(next_rect_box, q),
                RTRightStrategyNumber => right4D(next_rect_box, q),
                RTOverRightStrategyNumber => overRight4D(next_rect_box, q),
                RTAboveStrategyNumber => above4D(next_rect_box, q),
                RTOverAboveStrategyNumber => overAbove4D(next_rect_box, q),
                RTBelowStrategyNumber => below4D(next_rect_box, q),
                RTOverBelowStrategyNumber => overBelow4D(next_rect_box, q),
                other => {
                    elog!(ERROR, "unrecognized strategy: {}", other);
                    unreachable!()
                }
            };

            // If any check is failed, we have found our answer.
            if !flag {
                break;
            }
            i += 1;
        }

        if flag {
            *(*out).traversalValues.add((*out).nNodes as usize) = next_rect_box as *mut c_void;
            *(*out).nodeNumbers.add((*out).nNodes as usize) = quadrant as c_int;

            if (*in_).norderbys > 0 {
                let distances =
                    palloc(core::mem::size_of::<c_double>() * (*in_).norderbys as usize)
                        as *mut c_double;
                *(*out).distances.add((*out).nNodes as usize) = distances;

                let mut j = 0;
                while j < (*in_).norderbys {
                    let pt = DatumGetPointP((*(*in_).orderbys.add(j as usize)).sk_argument);
                    *distances.add(j as usize) = pointToRectBoxDistance(pt, next_rect_box);
                    j += 1;
                }
            }

            (*out).nNodes += 1;
        } else {
            // If this node is not selected, we don't need to keep the next
            // traversal value in the memory context.
            pfree(next_rect_box as *mut c_void);
        }

        quadrant += 1;
    }

    // Switch back.
    MemoryContextSwitchTo(old_ctx);

    PG_RETURN_VOID!()
}

/// SP-GiST leaf consistent function.
///
/// `Datum spg_box_quad_leaf_consistent(PG_FUNCTION_ARGS)`
pub unsafe fn spg_box_quad_leaf_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgLeafConsistentIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgLeafConsistentOut;
    let leaf = (*in_).leafDatum;
    let mut flag = true;

    // All tests are exact.
    (*out).recheck = false;

    // Don't return leafValue unless told to.
    if (*in_).returnData {
        (*out).leafValue = leaf;
    }

    let leaf_box = DatumGetBoxP(leaf);

    // Perform the required comparison(s).
    let mut i = 0;
    while i < (*in_).nkeys {
        let strategy = (*(*in_).scankeys.add(i as usize)).sk_strategy;
        let r#box = spg_box_quad_get_scankey_bbox(
            (*in_).scankeys.add(i as usize),
            &mut (*out).recheck,
        );

        flag = match strategy {
            RTOverlapStrategyNumber => box_overlap(leaf_box, r#box),
            RTContainsStrategyNumber => box_contain(leaf_box, r#box),
            RTContainedByStrategyNumber => box_contained(leaf_box, r#box),
            RTSameStrategyNumber => box_same(leaf_box, r#box),
            RTLeftStrategyNumber => box_left(leaf_box, r#box),
            RTOverLeftStrategyNumber => box_overleft(leaf_box, r#box),
            RTRightStrategyNumber => box_right(leaf_box, r#box),
            RTOverRightStrategyNumber => box_overright(leaf_box, r#box),
            RTAboveStrategyNumber => box_above(leaf_box, r#box),
            RTOverAboveStrategyNumber => box_overabove(leaf_box, r#box),
            RTBelowStrategyNumber => box_below(leaf_box, r#box),
            RTOverBelowStrategyNumber => box_overbelow(leaf_box, r#box),
            other => {
                elog!(ERROR, "unrecognized strategy: {}", other);
                    unreachable!()
            }
        };

        // If any check is failed, we have found our answer.
        if !flag {
            break;
        }
        i += 1;
    }

    if flag && (*in_).norderbys > 0 {
        let distfnoid = (*(*in_).orderbys.add(0)).sk_func.fn_oid;

        (*out).distances =
            spg_key_orderbys_distances(leaf, false, (*in_).orderbys, (*in_).norderbys);

        // Recheck is necessary when computing distance to polygon.
        (*out).recheckDistances = distfnoid == F_DIST_POLYP;
    }

    PG_RETURN_BOOL!(flag)
}

/// SP-GiST config function for 2-D types that are lossy represented by their
/// bounding boxes.
///
/// `Datum spg_bbox_quad_config(PG_FUNCTION_ARGS)`
pub unsafe fn spg_bbox_quad_config(fcinfo: FunctionCallInfo) -> Datum {
    let cfg = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgConfigOut;

    (*cfg).prefixType = BOXOID; // A type represented by its bounding box.
    (*cfg).labelType = VOIDOID; // We don't need node labels.
    (*cfg).leafType = BOXOID;
    (*cfg).canReturnData = false;
    (*cfg).longValuesOK = false;

    PG_RETURN_VOID!()
}

/// SP-GiST compress function for polygons.
///
/// `Datum spg_poly_quad_compress(PG_FUNCTION_ARGS)`
pub unsafe fn spg_poly_quad_compress(fcinfo: FunctionCallInfo) -> Datum {
    // PG_GETARG_POLYGON_P(0): detoast not needed (mirror is plain).
    let polygon = DatumGetPolygonP(PG_GETARG_DATUM!(fcinfo, 0));

    let r#box = palloc(core::mem::size_of::<BOX>()) as *mut BOX;
    *r#box = (*polygon).boundbox;

    // PG_RETURN_BOX_P(box).
    BoxPGetDatum(r#box)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mkbox(lx: f64, ly: f64, hx: f64, hy: f64) -> BOX {
        BOX {
            high: Point { x: hx, y: hy },
            low: Point { x: lx, y: ly },
        }
    }

    // ----- getQuadrant: classify a box vs a centroid into one of 16 quads ----

    #[test]
    fn quadrant_all_bits_set_when_box_strictly_greater() {
        // inBox every corner strictly greater than centroid -> 0xF.
        let mut centroid = mkbox(0.0, 0.0, 0.0, 0.0);
        let mut inbox = mkbox(1.0, 1.0, 1.0, 1.0);
        unsafe {
            assert_eq!(getQuadrant(&mut centroid, &mut inbox), 0xF);
        }
    }

    #[test]
    fn quadrant_zero_when_box_not_greater() {
        // inBox every corner <= centroid -> 0x0.
        let mut centroid = mkbox(5.0, 5.0, 5.0, 5.0);
        let mut inbox = mkbox(5.0, 5.0, 5.0, 5.0); // equal -> not strictly greater
        unsafe {
            assert_eq!(getQuadrant(&mut centroid, &mut inbox), 0x0);
        }
        let mut lower = mkbox(1.0, 1.0, 2.0, 2.0);
        unsafe {
            assert_eq!(getQuadrant(&mut centroid, &mut lower), 0x0);
        }
    }

    #[test]
    fn quadrant_individual_bits() {
        // Centroid at origin box.  Set exactly one corner greater each time.
        let mut centroid = mkbox(0.0, 0.0, 0.0, 0.0);

        // low.x > centroid.low.x => 0x8
        let mut b = mkbox(1.0, 0.0, 0.0, 0.0);
        unsafe {
            assert_eq!(getQuadrant(&mut centroid, &mut b), 0x8);
        }
        // high.x > centroid.high.x => 0x4
        let mut b = mkbox(0.0, 0.0, 1.0, 0.0);
        unsafe {
            assert_eq!(getQuadrant(&mut centroid, &mut b), 0x4);
        }
        // low.y > centroid.low.y => 0x2
        let mut b = mkbox(0.0, 1.0, 0.0, 0.0);
        unsafe {
            assert_eq!(getQuadrant(&mut centroid, &mut b), 0x2);
        }
        // high.y > centroid.high.y => 0x1
        let mut b = mkbox(0.0, 0.0, 0.0, 1.0);
        unsafe {
            assert_eq!(getQuadrant(&mut centroid, &mut b), 0x1);
        }
    }

    #[test]
    fn quadrant_centroid_vs_itself_is_zero_idempotent() {
        // Idempotence: classifying the centroid box relative to itself uses
        // strict-greater, so no corner is greater -> quadrant 0, stably.
        let mut centroid = mkbox(2.0, 3.0, 7.0, 9.0);
        let mut copy = centroid;
        unsafe {
            let q1 = getQuadrant(&mut centroid, &mut copy);
            let q2 = getQuadrant(&mut centroid, &mut copy);
            assert_eq!(q1, 0);
            assert_eq!(q1, q2);
        }
    }

    // ----- leaf predicates agree with direct geometric checks ---------------

    #[test]
    fn leaf_overlap_agrees_with_box_ov() {
        // Two overlapping boxes.
        let mut leaf = mkbox(0.0, 0.0, 5.0, 5.0);
        let mut query = mkbox(3.0, 3.0, 8.0, 8.0);
        unsafe {
            assert!(box_overlap(&mut leaf, &mut query));
        }
        // Disjoint boxes.
        let mut query2 = mkbox(10.0, 10.0, 12.0, 12.0);
        unsafe {
            assert!(!box_overlap(&mut leaf, &mut query2));
        }
    }

    #[test]
    fn leaf_contain_and_contained() {
        let mut big = mkbox(0.0, 0.0, 10.0, 10.0);
        let mut small = mkbox(2.0, 2.0, 4.0, 4.0);
        unsafe {
            // big contains small
            assert!(box_contain(&mut big, &mut small));
            // small contained by big
            assert!(box_contained(&mut small, &mut big));
            // small does NOT contain big
            assert!(!box_contain(&mut small, &mut big));
        }
    }

    #[test]
    fn leaf_left_right_above_below() {
        let mut a = mkbox(0.0, 0.0, 1.0, 1.0);
        let mut b = mkbox(5.0, 5.0, 6.0, 6.0);
        unsafe {
            // a is strictly left of b: a.high.x (1) < b.low.x (5)
            assert!(box_left(&mut a, &mut b));
            assert!(!box_right(&mut a, &mut b));
            // b is strictly right of a: b.low.x (5) > a.high.x (1)
            assert!(box_right(&mut b, &mut a));
            // a is strictly below b: a.high.y (1) < b.low.y (5)
            assert!(box_below(&mut a, &mut b));
            assert!(box_above(&mut b, &mut a));
        }
    }

    #[test]
    fn leaf_same_is_exact() {
        let mut a = mkbox(1.0, 2.0, 3.0, 4.0);
        let mut b = mkbox(1.0, 2.0, 3.0, 4.0);
        let mut c = mkbox(1.0, 2.0, 3.0, 4.5);
        unsafe {
            assert!(box_same(&mut a, &mut b));
            assert!(!box_same(&mut a, &mut c));
        }
    }

    // ----- 4D rect-box consistency: an unbounded RectBox accepts everything --

    #[test]
    fn initrectbox_overlaps_any_query() {
        unsafe {
            let rb = initRectBox();
            let mut q = mkbox(1.0, 2.0, 3.0, 4.0);
            let query = getRangeBox(&mut q);
            // Unbounded rect box can overlap / contain / be contained by anything.
            assert!(overlap4D(rb, query));
            assert!(contain4D(rb, query));
            assert!(contained4D(rb, query));
        }
    }

    #[test]
    fn nextrectbox_narrows_bounds_by_quadrant() {
        // Round-trip-ish: starting unbounded, narrow with quadrant 0xF, the
        // result must still overlap the centroid box itself.
        unsafe {
            let rb = initRectBox();
            let mut centroid = mkbox(0.0, 0.0, 10.0, 10.0);
            let crange = getRangeBox(&mut centroid);
            let nrb = nextRectBox(rb, crange, 0xF);

            // For quadrant 0xF, all four "low" sides were tightened to the
            // centroid coordinates; bounds remain a superset that still
            // overlaps the centroid box.
            let query = getRangeBox(&mut centroid);
            assert!(overlap4D(nrb, query));

            // Stability: building it twice yields equal field values.
            let nrb2 = nextRectBox(rb, crange, 0xF);
            assert_eq!((*nrb).range_box_x.left.low, (*nrb2).range_box_x.left.low);
            assert_eq!((*nrb).range_box_y.right.low, (*nrb2).range_box_y.right.low);
        }
    }

    #[test]
    fn point_to_rectbox_distance_zero_inside() {
        unsafe {
            let rb = initRectBox(); // unbounded -> any point is "inside"
            let mut p = Point { x: 3.0, y: 4.0 };
            assert_eq!(pointToRectBoxDistance(&mut p, rb), 0.0);
        }
    }
}
