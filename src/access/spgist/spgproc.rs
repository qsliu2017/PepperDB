//! Translation of postgres/src/backend/access/spgist/spgproc.c
//!
//! Common supporting procedures for SP-GiST opclasses (geometric distance).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * `Point` and `BOX` come from utils/geo_decls.h, which is NOT ported.  Their
//!   real geo_decls.h layout is defined LOCALLY here as minimal #[repr(C)]
//!   structs: `Point { x, y }` and `BOX { high, low }` (both Point).
//!
//! * `DatumGetPointP` / `DatumGetBoxP` / `PointPGetDatum` are geo_decls.h macros
//!   (not ported).  The key Datum holds a `*BOX` or `*Point`, so these are
//!   defined LOCALLY as `DatumGetPointer(d) as *mut T`.
//!
//! * `point_point_distance(p1,p2)` expands to a `DirectFunctionCall2` of
//!   `point_distance` (geo_ops.c, NOT ported).  It is STUBBED below as
//!   `point_distance_stub` returning the Euclidean distance directly; only the
//!   leaf-key path uses it, and the REAL geometry to verify here is
//!   `point_box_distance`.
//!
//! * `isnan` / `HYPOT` / `Min`/`Max`/`fabs` -> Rust `f64::is_nan`/`hypot`.

use crate::prelude::*;
use crate::access::common::scankey::ScanKey;
use crate::utils::adt::float::get_float8_nan;
use core::ffi::c_double;

// ===========================================================================
// Minimal geo_decls.h types (utils/geo_decls.h is NOT ported).
// These mirror the real PostgreSQL geometric type layout.
// ===========================================================================

/// `Point` from geo_decls.h.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Point {
    pub x: c_double,
    pub y: c_double,
}

/// `BOX` from geo_decls.h.  `high` is the upper-right corner, `low` the
/// lower-left, after normalization.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BOX {
    pub high: Point,
    pub low: Point,
}

// ---------------------------------------------------------------------------
// geo_decls.h Datum <-> geometry pointer accessors (macros in C).
// The key Datum holds a pointer to a palloc'd BOX or Point.
// ---------------------------------------------------------------------------

/// `DatumGetPointP(X)` - the Datum holds a `*Point`.
#[inline]
pub fn DatumGetPointP(X: Datum) -> *mut Point {
    DatumGetPointer(X) as *mut Point
}

/// `DatumGetBoxP(X)` - the Datum holds a `*BOX`.
#[inline]
pub fn DatumGetBoxP(X: Datum) -> *mut BOX {
    DatumGetPointer(X) as *mut BOX
}

// ---------------------------------------------------------------------------
// point_point_distance macro -> DirectFunctionCall2(point_distance, ...).
//
// geo_ops.c's `point_distance` is NOT ported; STUB it with the direct
// Euclidean distance (which is exactly what point_distance computes).
// ---------------------------------------------------------------------------

/// STUB for geo_ops.c `point_distance` (not ported): Euclidean distance
/// between two points.
unsafe fn point_distance_stub(p1: *mut Point, p2: *mut Point) -> c_double {
    let dx = (*p1).x - (*p2).x;
    let dy = (*p1).y - (*p2).y;
    dx.hypot(dy)
}

/// `point_point_distance(p1, p2)` macro.
#[inline]
unsafe fn point_point_distance(p1: *mut Point, p2: *mut Point) -> c_double {
    point_distance_stub(p1, p2)
}

// ===========================================================================
// spgproc.c
// ===========================================================================

/// Point-box distance in the assumption that box is aligned by axis.
///
/// `static double point_box_distance(Point *point, BOX *box)`
fn point_box_distance(point: &Point, r#box: &BOX) -> c_double {
    let dx: c_double;
    let dy: c_double;

    if point.x.is_nan()
        || r#box.low.x.is_nan()
        || point.y.is_nan()
        || r#box.low.y.is_nan()
    {
        return get_float8_nan();
    }

    if point.x < r#box.low.x {
        dx = r#box.low.x - point.x;
    } else if point.x > r#box.high.x {
        dx = point.x - r#box.high.x;
    } else {
        dx = 0.0;
    }

    if point.y < r#box.low.y {
        dy = r#box.low.y - point.y;
    } else if point.y > r#box.high.y {
        dy = point.y - r#box.high.y;
    } else {
        dy = 0.0;
    }

    // HYPOT(dx, dy)
    dx.hypot(dy)
}

/// Returns distances from given key to array of ordering scan keys.  Leaf key
/// is expected to be point, non-leaf key is expected to be box.  Scan key
/// arguments are expected to be points.
///
/// `double *spg_key_orderbys_distances(Datum key, bool isLeaf,
///                                     ScanKey orderbys, int norderbys)`
pub unsafe fn spg_key_orderbys_distances(
    key: Datum,
    isLeaf: bool,
    mut orderbys: ScanKey,
    norderbys: c_int,
) -> *mut c_double {
    let distances =
        palloc(norderbys as Size * core::mem::size_of::<c_double>()) as *mut c_double;
    let mut distance = distances;

    let mut sk_num = 0;
    while sk_num < norderbys {
        let point = DatumGetPointP((*orderbys).sk_argument);

        *distance = if isLeaf {
            point_point_distance(point, DatumGetPointP(key))
        } else {
            // point_box_distance(point, DatumGetBoxP(key))
            point_box_distance(&*point, &*DatumGetBoxP(key))
        };

        sk_num += 1;
        orderbys = orderbys.add(1);
        distance = distance.add(1);
    }

    distances
}

/// `BOX *box_copy(BOX *orig)`
pub unsafe fn box_copy(orig: *mut BOX) -> *mut BOX {
    let result = palloc(core::mem::size_of::<BOX>()) as *mut BOX;

    *result = *orig;
    result
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

    #[test]
    fn point_inside_box_distance_is_zero() {
        // Box [0,0]..[10,10], point (5,5) strictly inside -> distance 0.
        let b = mkbox(0.0, 0.0, 10.0, 10.0);
        let p = Point { x: 5.0, y: 5.0 };
        assert_eq!(point_box_distance(&p, &b), 0.0);
    }

    #[test]
    fn point_diagonal_offset_distance_is_hypot() {
        // Box [0,0]..[10,10], point (13,14): dx = 13-10 = 3, dy = 14-10 = 4.
        // Expected sqrt(3^2 + 4^2) = 5.
        let b = mkbox(0.0, 0.0, 10.0, 10.0);
        let p = Point { x: 13.0, y: 14.0 };
        let d = point_box_distance(&p, &b);
        assert!((d - 5.0).abs() < 1e-12, "got {d}");

        // Lower-left diagonal offset: point (-3,-4) -> dx=3, dy=4 -> 5.
        let p2 = Point { x: -3.0, y: -4.0 };
        let d2 = point_box_distance(&p2, &b);
        assert!((d2 - 5.0).abs() < 1e-12, "got {d2}");
    }

    #[test]
    fn nan_coordinate_yields_nan() {
        let b = mkbox(0.0, 0.0, 10.0, 10.0);
        let p = Point { x: f64::NAN, y: 5.0 };
        assert!(point_box_distance(&p, &b).is_nan());
    }
}
