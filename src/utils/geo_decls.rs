//! utils/geo_decls.h - Declarations for various 2D constructs (points, boxes, paths, etc).

use core::ffi::c_void;

use crate::c::{float8, int32, FLEXIBLE_ARRAY_MEMBER};
use crate::postgres::{Datum, DatumGetPointer, PointerGetDatum};

// ---------------------------------------------------------------------
// Useful floating point utilities and constants.
//
// "Fuzzy" floating-point comparisons: values within EPSILON of each other
// are considered equal.  Not NaN-aware (returns false for NaN inputs).
// ---------------------------------------------------------------------

pub const EPSILON: f64 = 1.0E-06;

// #define FPzero(A) (fabs(A) <= EPSILON)
#[inline]
pub fn FPzero(A: f64) -> bool {
    A.abs() <= EPSILON
}

#[inline]
pub fn FPeq(A: f64, B: f64) -> bool {
    A == B || (A - B).abs() <= EPSILON
}

#[inline]
pub fn FPne(A: f64, B: f64) -> bool {
    A != B && (A - B).abs() > EPSILON
}

#[inline]
pub fn FPlt(A: f64, B: f64) -> bool {
    A + EPSILON < B
}

#[inline]
pub fn FPle(A: f64, B: f64) -> bool {
    A <= B + EPSILON
}

#[inline]
pub fn FPgt(A: f64, B: f64) -> bool {
    A > B + EPSILON
}

#[inline]
pub fn FPge(A: f64, B: f64) -> bool {
    A + EPSILON >= B
}

// #define HYPOT(A, B) pg_hypot(A, B)
#[inline]
pub unsafe fn HYPOT(A: float8, B: float8) -> float8 {
    pg_hypot(A, B)
}

// ---------------------------------------------------------------------
// Point - (x,y)
// ---------------------------------------------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Point {
    pub x: float8,
    pub y: float8,
}

// ---------------------------------------------------------------------
// LSEG - A straight line, specified by endpoints.
// ---------------------------------------------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LSEG {
    pub p: [Point; 2],
}

// ---------------------------------------------------------------------
// PATH - Specified by vertex points.
// ---------------------------------------------------------------------
#[repr(C)]
pub struct PATH {
    pub vl_len_: int32, // varlena header (do not touch directly!)
    pub npts: int32,
    pub closed: int32, // is this a closed polygon?
    pub dummy: int32,  // padding to make it double align
    pub p: [Point; FLEXIBLE_ARRAY_MEMBER],
}

// ---------------------------------------------------------------------
// LINE - Specified by its general equation (Ax+By+C=0).
// ---------------------------------------------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LINE {
    pub A: float8,
    pub B: float8,
    pub C: float8,
}

// ---------------------------------------------------------------------
// BOX - Specified by two corner points, which are
//       sorted to save calculation time later.
// ---------------------------------------------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BOX {
    pub high: Point, // corner POINTs
    pub low: Point,
}

// ---------------------------------------------------------------------
// POLYGON - Specified by an array of doubles defining the points,
//      keeping the number of points and the bounding box for
//      speed purposes.
// ---------------------------------------------------------------------
#[repr(C)]
pub struct POLYGON {
    pub vl_len_: int32, // varlena header (do not touch directly!)
    pub npts: int32,
    pub boundbox: BOX,
    pub p: [Point; FLEXIBLE_ARRAY_MEMBER],
}

// ---------------------------------------------------------------------
// CIRCLE - Specified by a center point and radius.
// ---------------------------------------------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CIRCLE {
    pub center: Point,
    pub radius: float8,
}

// ---------------------------------------------------------------------
// fmgr interface functions
//
// Path and Polygon are toastable varlena types, the others are just
// fixed-size pass-by-reference types.
// ---------------------------------------------------------------------

#[inline]
pub unsafe fn DatumGetPointP(X: Datum) -> *mut Point {
    DatumGetPointer(X) as *mut Point
}

#[inline]
pub unsafe fn PointPGetDatum(X: *const Point) -> Datum {
    PointerGetDatum(X as *const c_void)
}

// #define PG_GETARG_POINT_P(n) DatumGetPointP(PG_GETARG_DATUM(n))
#[macro_export]
macro_rules! PG_GETARG_POINT_P {
    ($fcinfo:expr, $n:expr) => {
        $crate::utils::geo_decls::DatumGetPointP($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
// #define PG_RETURN_POINT_P(x) return PointPGetDatum(x)
#[macro_export]
macro_rules! PG_RETURN_POINT_P {
    ($x:expr) => {
        return $crate::utils::geo_decls::PointPGetDatum($x)
    };
}

#[inline]
pub unsafe fn DatumGetLsegP(X: Datum) -> *mut LSEG {
    DatumGetPointer(X) as *mut LSEG
}

#[inline]
pub unsafe fn LsegPGetDatum(X: *const LSEG) -> Datum {
    PointerGetDatum(X as *const c_void)
}

// #define PG_GETARG_LSEG_P(n) DatumGetLsegP(PG_GETARG_DATUM(n))
#[macro_export]
macro_rules! PG_GETARG_LSEG_P {
    ($fcinfo:expr, $n:expr) => {
        $crate::utils::geo_decls::DatumGetLsegP($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
// #define PG_RETURN_LSEG_P(x) return LsegPGetDatum(x)
#[macro_export]
macro_rules! PG_RETURN_LSEG_P {
    ($x:expr) => {
        return $crate::utils::geo_decls::LsegPGetDatum($x)
    };
}

#[inline]
pub unsafe fn DatumGetPathP(X: Datum) -> *mut PATH {
    crate::PG_DETOAST_DATUM!(X) as *mut PATH
}

#[inline]
pub unsafe fn DatumGetPathPCopy(X: Datum) -> *mut PATH {
    crate::PG_DETOAST_DATUM_COPY!(X) as *mut PATH
}

#[inline]
pub unsafe fn PathPGetDatum(X: *const PATH) -> Datum {
    PointerGetDatum(X as *const c_void)
}

// #define PG_GETARG_PATH_P(n) DatumGetPathP(PG_GETARG_DATUM(n))
#[macro_export]
macro_rules! PG_GETARG_PATH_P {
    ($fcinfo:expr, $n:expr) => {
        $crate::utils::geo_decls::DatumGetPathP($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
// #define PG_GETARG_PATH_P_COPY(n) DatumGetPathPCopy(PG_GETARG_DATUM(n))
#[macro_export]
macro_rules! PG_GETARG_PATH_P_COPY {
    ($fcinfo:expr, $n:expr) => {
        $crate::utils::geo_decls::DatumGetPathPCopy($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
// #define PG_RETURN_PATH_P(x) return PathPGetDatum(x)
#[macro_export]
macro_rules! PG_RETURN_PATH_P {
    ($x:expr) => {
        return $crate::utils::geo_decls::PathPGetDatum($x)
    };
}

#[inline]
pub unsafe fn DatumGetLineP(X: Datum) -> *mut LINE {
    DatumGetPointer(X) as *mut LINE
}

#[inline]
pub unsafe fn LinePGetDatum(X: *const LINE) -> Datum {
    PointerGetDatum(X as *const c_void)
}

// #define PG_GETARG_LINE_P(n) DatumGetLineP(PG_GETARG_DATUM(n))
#[macro_export]
macro_rules! PG_GETARG_LINE_P {
    ($fcinfo:expr, $n:expr) => {
        $crate::utils::geo_decls::DatumGetLineP($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
// #define PG_RETURN_LINE_P(x) return LinePGetDatum(x)
#[macro_export]
macro_rules! PG_RETURN_LINE_P {
    ($x:expr) => {
        return $crate::utils::geo_decls::LinePGetDatum($x)
    };
}

#[inline]
pub unsafe fn DatumGetBoxP(X: Datum) -> *mut BOX {
    DatumGetPointer(X) as *mut BOX
}

#[inline]
pub unsafe fn BoxPGetDatum(X: *const BOX) -> Datum {
    PointerGetDatum(X as *const c_void)
}

// #define PG_GETARG_BOX_P(n) DatumGetBoxP(PG_GETARG_DATUM(n))
#[macro_export]
macro_rules! PG_GETARG_BOX_P {
    ($fcinfo:expr, $n:expr) => {
        $crate::utils::geo_decls::DatumGetBoxP($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
// #define PG_RETURN_BOX_P(x) return BoxPGetDatum(x)
#[macro_export]
macro_rules! PG_RETURN_BOX_P {
    ($x:expr) => {
        return $crate::utils::geo_decls::BoxPGetDatum($x)
    };
}

#[inline]
pub unsafe fn DatumGetPolygonP(X: Datum) -> *mut POLYGON {
    crate::PG_DETOAST_DATUM!(X) as *mut POLYGON
}

#[inline]
pub unsafe fn DatumGetPolygonPCopy(X: Datum) -> *mut POLYGON {
    crate::PG_DETOAST_DATUM_COPY!(X) as *mut POLYGON
}

#[inline]
pub unsafe fn PolygonPGetDatum(X: *const POLYGON) -> Datum {
    PointerGetDatum(X as *const c_void)
}

// #define PG_GETARG_POLYGON_P(n) DatumGetPolygonP(PG_GETARG_DATUM(n))
#[macro_export]
macro_rules! PG_GETARG_POLYGON_P {
    ($fcinfo:expr, $n:expr) => {
        $crate::utils::geo_decls::DatumGetPolygonP($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
// #define PG_GETARG_POLYGON_P_COPY(n) DatumGetPolygonPCopy(PG_GETARG_DATUM(n))
#[macro_export]
macro_rules! PG_GETARG_POLYGON_P_COPY {
    ($fcinfo:expr, $n:expr) => {
        $crate::utils::geo_decls::DatumGetPolygonPCopy($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
// #define PG_RETURN_POLYGON_P(x) return PolygonPGetDatum(x)
#[macro_export]
macro_rules! PG_RETURN_POLYGON_P {
    ($x:expr) => {
        return $crate::utils::geo_decls::PolygonPGetDatum($x)
    };
}

#[inline]
pub unsafe fn DatumGetCircleP(X: Datum) -> *mut CIRCLE {
    DatumGetPointer(X) as *mut CIRCLE
}

#[inline]
pub unsafe fn CirclePGetDatum(X: *const CIRCLE) -> Datum {
    PointerGetDatum(X as *const c_void)
}

// #define PG_GETARG_CIRCLE_P(n) DatumGetCircleP(PG_GETARG_DATUM(n))
#[macro_export]
macro_rules! PG_GETARG_CIRCLE_P {
    ($fcinfo:expr, $n:expr) => {
        $crate::utils::geo_decls::DatumGetCircleP($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
// #define PG_RETURN_CIRCLE_P(x) return CirclePGetDatum(x)
#[macro_export]
macro_rules! PG_RETURN_CIRCLE_P {
    ($x:expr) => {
        return $crate::utils::geo_decls::CirclePGetDatum($x)
    };
}

// ---------------------------------------------------------------------
// in geo_ops.c
// ---------------------------------------------------------------------

pub unsafe fn pg_hypot(x: float8, y: float8) -> float8 {
    unimplemented!()
}
