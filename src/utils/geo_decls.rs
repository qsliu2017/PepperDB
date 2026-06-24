//! Translated from PostgreSQL src/include/utils/geo_decls.h
//!
//! 2D geometric types. Point/Lseg/Line/Box/Circle are fixed-size
//! pass-by-reference on-disk types; PATH/POLYGON are toastable varlenas.

use crate::postgres::Datum;

// "Fuzzy" floating-point comparisons: within EPSILON are equal.
pub const EPSILON: f64 = 1.0e-06;

#[inline]
pub fn FPzero(a: f64) -> bool {
    a.abs() <= EPSILON
}
#[inline]
pub fn FPeq(a: f64, b: f64) -> bool {
    a == b || (a - b).abs() <= EPSILON
}
#[inline]
pub fn FPne(a: f64, b: f64) -> bool {
    a != b && (a - b).abs() > EPSILON
}
#[inline]
pub fn FPlt(a: f64, b: f64) -> bool {
    a + EPSILON < b
}
#[inline]
pub fn FPle(a: f64, b: f64) -> bool {
    a <= b + EPSILON
}
#[inline]
pub fn FPgt(a: f64, b: f64) -> bool {
    a > b + EPSILON
}
#[inline]
pub fn FPge(a: f64, b: f64) -> bool {
    a + EPSILON >= b
}

#[inline]
pub fn HYPOT(a: f64, b: f64) -> f64 {
    pg_hypot(a, b)
}

/// Point - (x, y). On-disk fixed-size pass-by-reference.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}
const _: () = assert!(core::mem::size_of::<Point>() == 16);

/// LSEG - straight line specified by its two endpoints.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct LSEG {
    pub p: [Point; 2],
}
const _: () = assert!(core::mem::size_of::<LSEG>() == 32);

/// PATH - vertex points. On-disk varlena (trailing Point FAM).
#[repr(C)]
pub struct PATH {
    pub vl_len_: i32, // varlena header (do not touch directly)
    pub npts: i32,
    pub closed: i32, // is this a closed polygon?
    pub dummy: i32,  // padding to make it double-align
    // Point p[FLEXIBLE_ARRAY_MEMBER] follows; access via a safe slice accessor.
}
const _: () = assert!(core::mem::size_of::<PATH>() == 16);

/// LINE - general equation Ax + By + C = 0.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct LINE {
    pub A: f64,
    pub B: f64,
    pub C: f64,
}
const _: () = assert!(core::mem::size_of::<LINE>() == 24);

/// BOX - two corner points (sorted high/low to save later calculation).
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BOX {
    pub high: Point,
    pub low: Point,
}
const _: () = assert!(core::mem::size_of::<BOX>() == 32);

/// POLYGON - points plus a cached bounding box. On-disk varlena (Point FAM).
#[repr(C)]
pub struct POLYGON {
    pub vl_len_: i32, // varlena header (do not touch directly)
    pub npts: i32,
    pub boundbox: BOX,
    // Point p[FLEXIBLE_ARRAY_MEMBER] follows.
}
const _: () = assert!(core::mem::size_of::<POLYGON>() == 40);

/// CIRCLE - center point and radius.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CIRCLE {
    pub center: Point,
    pub radius: f64,
}
const _: () = assert!(core::mem::size_of::<CIRCLE>() == 24);

// fmgr interface. Fixed-size types use plain pointer reinterpretation; PATH and
// POLYGON detoast.
#[inline]
pub fn DatumGetPointP(x: Datum) -> *mut Point {
    x.0 as *mut Point // TODO(ptr)
}
#[inline]
pub fn PointPGetDatum(x: &Point) -> Datum {
    Datum(x as *const Point as usize)
}
#[inline]
pub fn DatumGetLsegP(x: Datum) -> *mut LSEG {
    x.0 as *mut LSEG // TODO(ptr)
}
#[inline]
pub fn LsegPGetDatum(x: &LSEG) -> Datum {
    Datum(x as *const LSEG as usize)
}
#[inline]
pub fn DatumGetPathP(x: Datum) -> *mut PATH {
    unimplemented!() // PG_DETOAST_DATUM; TODO(ptr)
}
#[inline]
pub fn DatumGetPathPCopy(x: Datum) -> *mut PATH {
    unimplemented!() // PG_DETOAST_DATUM_COPY; TODO(ptr)
}
#[inline]
pub fn PathPGetDatum(x: &PATH) -> Datum {
    Datum(x as *const PATH as usize)
}
#[inline]
pub fn DatumGetLineP(x: Datum) -> *mut LINE {
    x.0 as *mut LINE // TODO(ptr)
}
#[inline]
pub fn LinePGetDatum(x: &LINE) -> Datum {
    Datum(x as *const LINE as usize)
}
#[inline]
pub fn DatumGetBoxP(x: Datum) -> *mut BOX {
    x.0 as *mut BOX // TODO(ptr)
}
#[inline]
pub fn BoxPGetDatum(x: &BOX) -> Datum {
    Datum(x as *const BOX as usize)
}
#[inline]
pub fn DatumGetPolygonP(x: Datum) -> *mut POLYGON {
    unimplemented!() // PG_DETOAST_DATUM; TODO(ptr)
}
#[inline]
pub fn DatumGetPolygonPCopy(x: Datum) -> *mut POLYGON {
    unimplemented!() // PG_DETOAST_DATUM_COPY; TODO(ptr)
}
#[inline]
pub fn PolygonPGetDatum(x: &POLYGON) -> Datum {
    Datum(x as *const POLYGON as usize)
}
#[inline]
pub fn DatumGetCircleP(x: Datum) -> *mut CIRCLE {
    x.0 as *mut CIRCLE // TODO(ptr)
}
#[inline]
pub fn CirclePGetDatum(x: &CIRCLE) -> Datum {
    Datum(x as *const CIRCLE as usize)
}

// in geo_ops.c
pub fn pg_hypot(x: f64, y: f64) -> f64 {
    unimplemented!()
}
