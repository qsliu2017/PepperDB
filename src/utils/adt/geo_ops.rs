//! geo_ops.rs
//!   2D geometric operations
//! Translated 1:1 from postgres/src/backend/utils/adt/geo_ops.c
//!
//! This module implements the geometric functions and operators.  The
//! geometric types are (from simple to more complicated):
//!
//! - point
//! - line
//! - line segment
//! - box
//! - circle
//! - polygon
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/geo_ops.c
//!
//! IMPORT NOTES (see crate-level porting conventions):
//!  - geometric types (Point/LSEG/BOX/LINE/PATH/POLYGON/CIRCLE) + FP macros +
//!    PG_GETARG_*/PG_RETURN_* type accessors live in crate::utils::geo_decls.
//!  - float8 arithmetic helpers (float8_pl/mi/mul/div, float8_min/max,
//!    get_float8_infinity/get_float8_nan, float8_eq/lt) live in
//!    crate::utils::adt::float.
//!  - pg_hypot lives in *this* file (declared as a stub in geo_decls.h).
//!  - isnan/isinf -> Rust f64::is_nan()/is_infinite().

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_parens)]
#![allow(unused_assignments)]

use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{
    PG_FREE_IF_COPY, appendStringInfo,
    PG_GETARG_BOX_P,
    PG_GETARG_CIRCLE_P,
    PG_GETARG_CSTRING,
    PG_GETARG_FLOAT8,
    PG_GETARG_INT32,
    PG_GETARG_LINE_P,
    PG_GETARG_LSEG_P,
    PG_GETARG_PATH_P,
    PG_GETARG_PATH_P_COPY,
    PG_GETARG_POINTER,
    PG_GETARG_POINT_P,
    PG_GETARG_POLYGON_P,
    PG_RETURN_BOOL,
    PG_RETURN_BOX_P,
    PG_RETURN_BYTEA_P,
    PG_RETURN_CIRCLE_P,
    PG_RETURN_CSTRING,
    PG_RETURN_FLOAT8,
    PG_RETURN_INT32,
    PG_RETURN_LINE_P,
    PG_RETURN_LSEG_P,
    PG_RETURN_NULL,
    PG_RETURN_PATH_P,
    PG_RETURN_POINT_P,
    PG_RETURN_POLYGON_P,
};


use std::ffi::c_void;

use crate::c::{bytea, float8, int32, text};
use crate::lib::stringinfo::{
    appendBinaryStringInfo, appendStringInfoChar, initStringInfo, StringInfo, StringInfoData,
};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgbyte, pq_getmsgfloat8, pq_getmsgint, pq_sendint8 as pq_sendbyte,
    pq_sendfloat8, pq_sendint32,
};
use crate::miscadmin::{check_stack_depth, CHECK_FOR_INTERRUPTS};
use crate::nodes::nodes::Node;
use crate::utils::adt::float::{
    float8_div, float8_eq, float8_gt, float8_lt, float8_max, float8_min, float8_mi, float8_mul,
    float8_pl, float8in_internal, float8out_internal, get_float8_infinity, get_float8_nan,
};
use crate::utils::geo_decls::{
    FPeq, FPge, FPgt, FPle, FPlt, FPne, FPzero, BOX, CIRCLE, LINE, LSEG, PATH, POLYGON,
};
use crate::utils::geo_decls::{
    BoxPGetDatum, CirclePGetDatum, DatumGetBoxP, DatumGetCircleP, DatumGetLineP, DatumGetLsegP,
    DatumGetPathP, DatumGetPathPCopy, DatumGetPointP, DatumGetPolygonP, LinePGetDatum,
    LsegPGetDatum, PathPGetDatum, Point, PointPGetDatum, PolygonPGetDatum, HYPOT,
};
use crate::varatt::SET_VARSIZE;

// ---------------------------------------------------------------------------
// <math.h> bindings used by this module.  isnan/isinf map to Rust methods.
// ---------------------------------------------------------------------------
extern "C" {
    fn sqrt(x: f64) -> f64;
    fn fabs(x: f64) -> f64;
    fn cos(x: f64) -> f64;
    fn sin(x: f64) -> f64;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strrchr(s: *const c_char, c: c_int) -> *mut c_char;
}

// X/Open (XSI) requires <math.h> to provide M_PI, but core POSIX does not.
// TODO(pg-port): M_PI is defined privately in crate::utils::adt::float.
const M_PI: f64 = 3.14159265358979323846;

// INT_MAX from <limits.h>.
const INT_MAX: int32 = i32::MAX;

// float_overflow_error/float_underflow_error live in crate::utils::adt::float
// but are not re-exported there; mirror them locally for pg_hypot().
// TODO(pg-port): real float_overflow_error lives in crate::utils::adt::float.
unsafe fn float_overflow_error() {
    ereport!(
        ERROR,
        errmsg!("value out of range: overflow")
    );
    unreachable!()
}
// TODO(pg-port): real float_underflow_error lives in crate::utils::adt::float.
unsafe fn float_underflow_error() {
    ereport!(
        ERROR,
        errmsg!("value out of range: underflow")
    );
    unreachable!()
}

// psprintf has no ported home yet; minimal local shim used by line_out().
// TODO(pg-port): real psprintf lives in crate::utils::palloc (mcxt/psprintf.c).
unsafe fn psprintf_line(astr: *mut c_char, bstr: *mut c_char, cstr: *mut c_char) -> *mut c_char {
    let s = format!(
        "{}{}{}{}{}{}{}",
        LDELIM_L as u8 as char,
        std::ffi::CStr::from_ptr(astr).to_string_lossy(),
        DELIM as u8 as char,
        std::ffi::CStr::from_ptr(bstr).to_string_lossy(),
        DELIM as u8 as char,
        std::ffi::CStr::from_ptr(cstr).to_string_lossy(),
        RDELIM_L as u8 as char,
    );
    let len = s.len();
    let p = palloc(len + 1) as *mut c_char;
    std::ptr::copy_nonoverlapping(s.as_ptr() as *const c_char, p, len);
    *p.add(len) = 0;
    p
}

// errcode constants used in ereturn/ereport sites (dropped per porting convention).
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;
const ERRCODE_INVALID_BINARY_REPRESENTATION: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;

/*
 * ereturn(escontext, dummy, (...)) mirrors the local pattern used by other
 * adt/ modules: the elog shim emits at ERROR level (errcode/errdetail/errhint
 * dropped per porting convention) and returns the dummy.  Defined textually
 * before first use since macro_rules! is not hoisted.
 */
macro_rules! ereturn {
    ($escontext:expr, $dummy:expr, $($arg:tt)*) => {{
        let __msg: String = { $($arg)* };
        soft_error_record($escontext as *mut Node, &__msg);
        #[allow(unreachable_code)]
        return $dummy;
    }};
}

/*
 * soft_error_record: report a (soft or hard) input error through the real
 * errsave mechanism so pg_input_is_valid / pg_input_error_info see a populated
 * ErrorSaveContext.  errsave_start sets error_occurred and (when details are
 * wanted) opens a real error stack frame; for a null/non-ErrorSaveContext it
 * punts to errstart(ERROR) and errsave_finish then raises a hard ERROR.
 * All geo input failures are 22P02 (invalid_text_representation).
 */
#[inline]
unsafe fn soft_error_record(escontext: *mut Node, msg: &str) {
    const ERRCODE_INVALID_TEXT_REPRESENTATION_REAL: c_int = 33685634; /* 22P02 */
    if crate::utils::error::elog_impl::errsave_start(escontext, core::ptr::null()) {
        crate::utils::error::elog_impl::errcode_impl(ERRCODE_INVALID_TEXT_REPRESENTATION_REAL);
        if let Ok(c) = std::ffi::CString::new(msg) {
            crate::utils::error::elog_impl::errmsg_c(c.as_ptr());
        }
        crate::utils::error::elog_impl::errsave_finish(
            escontext, c"geo_ops.rs".as_ptr(), 0, c"geo_ops".as_ptr(),
        );
    }
}

/*
 * SOFT_ERROR_FLAG: if `escontext` is a real ErrorSaveContext, record that a soft
 * error occurred and return true (caller returns its dummy without raising);
 * otherwise return false so the caller raises a hard ERROR.
 */
#[inline]
unsafe fn SOFT_ERROR_FLAG(escontext: *mut Node) -> bool {
    const T_ErrorSaveContext: c_int = 447;
    if !escontext.is_null() && *(escontext as *const c_int) == T_ErrorSaveContext {
        (*(escontext as *mut crate::nodes::miscnodes::ErrorSaveContext)).error_occurred = true;
        return true;
    }
    false
}

/*
 * SOFT_ERROR_OCCURRED(escontext): true once a soft error has been recorded.
 */
#[inline]
unsafe fn SOFT_ERROR_OCCURRED(escontext: *mut Node) -> bool {
    const T_ErrorSaveContext: c_int = 447;
    !escontext.is_null()
        && *(escontext as *const c_int) == T_ErrorSaveContext
        && (*(escontext as *const crate::nodes::miscnodes::ErrorSaveContext)).error_occurred
}

/*
 * * Type constructors have this form:
 *   void type_construct(Type *result, ...);
 *
 * * Operators commonly have signatures such as
 *   void type1_operator_type2(Type *result, Type1 *obj1, Type2 *obj2);
 *
 * Common operators are:
 * * Intersection point:
 *   bool type1_interpt_type2(Point *result, Type1 *obj1, Type2 *obj2);
 *      Return whether the two objects intersect. If *result is not NULL,
 *      it is set to the intersection point.
 *
 * * Containment:
 *   bool type1_contain_type2(Type1 *obj1, Type2 *obj2);
 *      Return whether obj1 contains obj2.
 *   bool type1_contain_type2(Type1 *contains_obj, Type1 *contained_obj);
 *      Return whether obj1 contains obj2 (used when types are the same)
 *
 * * Distance of closest point in or on obj1 to obj2:
 *   float8 type1_closept_type2(Point *result, Type1 *obj1, Type2 *obj2);
 *      Returns the shortest distance between two objects.  If *result is not
 *      NULL, it is set to the closest point in or on obj1 to obj2.
 *
 * These functions may be used to implement multiple SQL-level operators.  For
 * example, determining whether two lines are parallel is done by checking
 * whether they don't intersect.
 */

/*
 * Internal routines
 */

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum path_delim {
    PATH_NONE,
    PATH_OPEN,
    PATH_CLOSED,
}
use path_delim::*;

/*
 * Delimiters for input and output strings.
 * LDELIM, RDELIM, and DELIM are left, right, and separator delimiters, respectively.
 * LDELIM_EP, RDELIM_EP are left and right delimiters for paths with endpoints.
 */

const LDELIM: c_char = b'(' as c_char;
const RDELIM: c_char = b')' as c_char;
const DELIM: c_char = b',' as c_char;
const LDELIM_EP: c_char = b'[' as c_char;
const RDELIM_EP: c_char = b']' as c_char;
const LDELIM_C: c_char = b'<' as c_char;
const RDELIM_C: c_char = b'>' as c_char;
const LDELIM_L: c_char = b'{' as c_char;
const RDELIM_L: c_char = b'}' as c_char;

// isspace((unsigned char) c) from <ctype.h>.
#[inline]
unsafe fn isspace(c: c_char) -> bool {
    matches!(c as u8, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

// offsetof(PATH, p) / offsetof(POLYGON, p): header bytes before the FAM array.
#[inline]
fn offsetof_path_p() -> usize {
    // vl_len_ + npts + closed + dummy = 4 * int32
    4 * std::mem::size_of::<int32>()
}
#[inline]
fn offsetof_polygon_p() -> usize {
    // vl_len_ + npts + boundbox(BOX)
    2 * std::mem::size_of::<int32>() + std::mem::size_of::<BOX>()
}

/*
 * Geometric data types are composed of points.
 * This code tries to support a common format throughout the data types,
 *  to allow for more predictable usage and data type conversion.
 * The fundamental unit is the point. Other units are line segments,
 *  open paths, boxes, closed paths, and polygons (which should be considered
 *  non-intersecting closed paths).
 *
 * Data representation is as follows:
 *  point:              (x,y)
 *  line segment:       [(x1,y1),(x2,y2)]
 *  box:                (x1,y1),(x2,y2)
 *  open path:          [(x1,y1),...,(xn,yn)]
 *  closed path:        ((x1,y1),...,(xn,yn))
 *  polygon:            ((x1,y1),...,(xn,yn))
 *
 * For boxes, the points are opposite corners with the first point at the top right.
 * For closed paths and polygons, the points should be reordered to allow
 *  fast and correct equality comparisons.
 *
 * XXX perhaps points in complex shapes should be reordered internally
 *  to allow faster internal operations, but should keep track of input order
 *  and restore that order for text output - tgl 97/01/16
 */

unsafe fn single_decode(
    num: *mut c_char,
    x: *mut float8,
    endptr_p: *mut *mut c_char,
    type_name: *const c_char,
    orig_string: *const c_char,
    escontext: *mut Node,
) -> bool {
    *x = float8in_internal(num, endptr_p, type_name, orig_string, escontext);
    return !SOFT_ERROR_OCCURRED(escontext);
} /* single_decode() */

unsafe fn single_encode(x: float8, str: StringInfo) {
    let xstr: *mut c_char = float8out_internal(x);

    appendStringInfoString(str, xstr);
    pfree(xstr as *mut c_void);
} /* single_encode() */

unsafe fn pair_decode(
    mut str: *mut c_char,
    x: *mut float8,
    y: *mut float8,
    endptr_p: *mut *mut c_char,
    type_name: *const c_char,
    orig_string: *const c_char,
    escontext: *mut Node,
) -> bool {
    let has_delim: bool;

    while isspace(*str) {
        str = str.add(1);
    }
    has_delim = *str == LDELIM;
    if has_delim {
        str = str.add(1);
    }

    if !single_decode(str, x, &raw mut str, type_name, orig_string, escontext) {
        return false;
    }

    if {
        let c = *str;
        str = str.add(1);
        c
    } != DELIM
    {
        return pair_decode_fail(type_name, orig_string, escontext);
    }

    if !single_decode(str, y, &raw mut str, type_name, orig_string, escontext) {
        return false;
    }

    if has_delim {
        if {
            let c = *str;
            str = str.add(1);
            c
        } != RDELIM
        {
            return pair_decode_fail(type_name, orig_string, escontext);
        }
        while isspace(*str) {
            str = str.add(1);
        }
    }

    /* report stopping point if wanted, else complain if not end of string */
    if !endptr_p.is_null() {
        *endptr_p = str;
    } else if *str != 0 {
        return pair_decode_fail(type_name, orig_string, escontext);
    }
    return true;
}

unsafe fn pair_decode_fail(
    type_name: *const c_char,
    orig_string: *const c_char,
    escontext: *mut Node,
) -> bool {
    ereturn!(
        escontext,
        false,
        errmsg!(
            "invalid input syntax for type {}: \"{}\"",
            std::ffi::CStr::from_ptr(type_name).to_string_lossy(),
            std::ffi::CStr::from_ptr(orig_string).to_string_lossy()
        )
    );
}

unsafe fn pair_encode(x: float8, y: float8, str: StringInfo) {
    let xstr: *mut c_char = float8out_internal(x);
    let ystr: *mut c_char = float8out_internal(y);

    appendStringInfo!(
        str,
        "{},{}",
        std::ffi::CStr::from_ptr(xstr).to_string_lossy(),
        std::ffi::CStr::from_ptr(ystr).to_string_lossy()
    );
    pfree(xstr as *mut c_void);
    pfree(ystr as *mut c_void);
}

unsafe fn path_decode(
    mut str: *mut c_char,
    opentype: bool,
    npts: c_int,
    mut p: *mut Point,
    isopen: *mut bool,
    endptr_p: *mut *mut c_char,
    type_name: *const c_char,
    orig_string: *const c_char,
    escontext: *mut Node,
) -> bool {
    let mut depth: c_int = 0;
    let cp: *mut c_char;
    let mut i: c_int;

    while isspace(*str) {
        str = str.add(1);
    }
    *isopen = *str == LDELIM_EP;
    if *isopen {
        /* no open delimiter allowed? */
        if !opentype {
            return path_decode_fail(type_name, orig_string, escontext);
        }
        depth += 1;
        str = str.add(1);
    } else if *str == LDELIM {
        cp = str.add(1);
        let mut cpw = cp;
        while isspace(*cpw) {
            cpw = cpw.add(1);
        }
        if *cpw == LDELIM {
            depth += 1;
            str = cpw;
        } else if strrchr(str, LDELIM as c_int) == str {
            depth += 1;
            str = cpw;
        }
    }

    i = 0;
    while i < npts {
        if !pair_decode(
            str,
            &raw mut (*p).x,
            &raw mut (*p).y,
            &raw mut str,
            type_name,
            orig_string,
            escontext,
        ) {
            return false;
        }
        if *str == DELIM {
            str = str.add(1);
        }
        p = p.add(1);
        i += 1;
    }

    while depth > 0 {
        if *str == RDELIM || (*str == RDELIM_EP && *isopen && depth == 1) {
            depth -= 1;
            str = str.add(1);
            while isspace(*str) {
                str = str.add(1);
            }
        } else {
            return path_decode_fail(type_name, orig_string, escontext);
        }
    }

    /* report stopping point if wanted, else complain if not end of string */
    if !endptr_p.is_null() {
        *endptr_p = str;
    } else if *str != 0 {
        return path_decode_fail(type_name, orig_string, escontext);
    }
    return true;
} /* path_decode() */

unsafe fn path_decode_fail(
    type_name: *const c_char,
    orig_string: *const c_char,
    escontext: *mut Node,
) -> bool {
    ereturn!(
        escontext,
        false,
        errmsg!(
            "invalid input syntax for type {}: \"{}\"",
            std::ffi::CStr::from_ptr(type_name).to_string_lossy(),
            std::ffi::CStr::from_ptr(orig_string).to_string_lossy()
        )
    );
}

unsafe fn path_encode(path_delim: path_delim, npts: c_int, mut pt: *mut Point) -> *mut c_char {
    let mut str: StringInfoData = std::mem::zeroed();
    let mut i: c_int;

    initStringInfo(&raw mut str);

    match path_delim {
        PATH_CLOSED => {
            appendStringInfoChar(&raw mut str, LDELIM);
        }
        PATH_OPEN => {
            appendStringInfoChar(&raw mut str, LDELIM_EP);
        }
        PATH_NONE => {}
    }

    i = 0;
    while i < npts {
        if i > 0 {
            appendStringInfoChar(&raw mut str, DELIM);
        }
        appendStringInfoChar(&raw mut str, LDELIM);
        pair_encode((*pt).x, (*pt).y, &raw mut str);
        appendStringInfoChar(&raw mut str, RDELIM);
        pt = pt.add(1);
        i += 1;
    }

    match path_delim {
        PATH_CLOSED => {
            appendStringInfoChar(&raw mut str, RDELIM);
        }
        PATH_OPEN => {
            appendStringInfoChar(&raw mut str, RDELIM_EP);
        }
        PATH_NONE => {}
    }

    return str.data;
} /* path_encode() */

// appendStringInfoString: append a C string (mirrors lib/stringinfo).
unsafe fn appendStringInfoString(str: StringInfo, s: *const c_char) {
    crate::lib::stringinfo::appendStringInfoString(str, s);
}

/*-------------------------------------------------------------
 * pair_count - count the number of points
 * allow the following notation:
 * '((1,2),(3,4))'
 * '(1,3,2,4)'
 * require an odd number of delim characters in the string
 *-------------------------------------------------------------*/
unsafe fn pair_count(mut s: *mut c_char, delim: c_char) -> c_int {
    let mut ndelim: c_int = 0;

    loop {
        s = strchr(s, delim as c_int);
        if s.is_null() {
            break;
        }
        ndelim += 1;
        s = s.add(1);
    }
    return if (ndelim % 2) != 0 {
        (ndelim + 1) / 2
    } else {
        -1
    };
}

/***********************************************************************
 **
 **     Routines for two-dimensional boxes.
 **
 ***********************************************************************/

/*----------------------------------------------------------
 * Formatting and conversion routines.
 *---------------------------------------------------------*/

/*      box_in  -       convert a string to internal form.
 *
 *      External format: (two corners of box)
 *              "(f8, f8), (f8, f8)"
 *              also supports the older style "(f8, f8, f8, f8)"
 */
pub unsafe fn box_in(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let box_: *mut BOX = palloc(std::mem::size_of::<BOX>()) as *mut BOX;
    let mut isopen: bool = false;
    let x: float8;
    let y: float8;

    if !path_decode(
        str,
        false,
        2,
        &raw mut (*box_).high,
        &raw mut isopen,
        null_mut(),
        c"box".as_ptr(),
        str,
        escontext,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }

    /* reorder corners if necessary... */
    if float8_lt((*box_).high.x, (*box_).low.x) {
        x = (*box_).high.x;
        (*box_).high.x = (*box_).low.x;
        (*box_).low.x = x;
    }
    if float8_lt((*box_).high.y, (*box_).low.y) {
        y = (*box_).high.y;
        (*box_).high.y = (*box_).low.y;
        (*box_).low.y = y;
    }

    PG_RETURN_BOX_P!(box_);
}

/*      box_out -       convert a box to external form.
 */
pub unsafe fn box_out(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);

    PG_RETURN_CSTRING!(path_encode(PATH_NONE, 2, &raw mut (*box_).high));
}

/*
 *      box_recv            - converts external binary format to box
 */
pub unsafe fn box_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let box_: *mut BOX;
    let x: float8;
    let y: float8;

    box_ = palloc(std::mem::size_of::<BOX>()) as *mut BOX;

    (*box_).high.x = pq_getmsgfloat8(buf);
    (*box_).high.y = pq_getmsgfloat8(buf);
    (*box_).low.x = pq_getmsgfloat8(buf);
    (*box_).low.y = pq_getmsgfloat8(buf);

    /* reorder corners if necessary... */
    if float8_lt((*box_).high.x, (*box_).low.x) {
        x = (*box_).high.x;
        (*box_).high.x = (*box_).low.x;
        (*box_).low.x = x;
    }
    if float8_lt((*box_).high.y, (*box_).low.y) {
        y = (*box_).high.y;
        (*box_).high.y = (*box_).low.y;
        (*box_).low.y = y;
    }

    PG_RETURN_BOX_P!(box_);
}

/*
 *      box_send            - converts box to binary format
 */
pub unsafe fn box_send(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_begintypsend(&raw mut buf);
    pq_sendfloat8(&raw mut buf, (*box_).high.x);
    pq_sendfloat8(&raw mut buf, (*box_).high.y);
    pq_sendfloat8(&raw mut buf, (*box_).low.x);
    pq_sendfloat8(&raw mut buf, (*box_).low.y);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&raw mut buf));
}

/*      box_construct   -       fill in a new box.
 */
#[inline]
unsafe fn box_construct(result: *mut BOX, pt1: *mut Point, pt2: *mut Point) {
    if float8_gt((*pt1).x, (*pt2).x) {
        (*result).high.x = (*pt1).x;
        (*result).low.x = (*pt2).x;
    } else {
        (*result).high.x = (*pt2).x;
        (*result).low.x = (*pt1).x;
    }
    if float8_gt((*pt1).y, (*pt2).y) {
        (*result).high.y = (*pt1).y;
        (*result).low.y = (*pt2).y;
    } else {
        (*result).high.y = (*pt2).y;
        (*result).low.y = (*pt1).y;
    }
}

/*----------------------------------------------------------
 *  Relational operators for BOXes.
 *      <, >, <=, >=, and == are based on box area.
 *---------------------------------------------------------*/

/*      box_same        -       are two boxes identical?
 */
pub unsafe fn box_same(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(
        point_eq_point(&raw mut (*box1).high, &raw mut (*box2).high)
            && point_eq_point(&raw mut (*box1).low, &raw mut (*box2).low)
    );
}

/*      box_overlap     -       does box1 overlap box2?
 */
pub unsafe fn box_overlap(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(box_ov(box1, box2));
}

unsafe fn box_ov(box1: *mut BOX, box2: *mut BOX) -> bool {
    return FPle((*box1).low.x, (*box2).high.x)
        && FPle((*box2).low.x, (*box1).high.x)
        && FPle((*box1).low.y, (*box2).high.y)
        && FPle((*box2).low.y, (*box1).high.y);
}

/*      box_left        -       is box1 strictly left of box2?
 */
pub unsafe fn box_left(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPlt((*box1).high.x, (*box2).low.x));
}

/*      box_overleft    -       is the right edge of box1 at or left of
 *                              the right edge of box2?
 */
pub unsafe fn box_overleft(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPle((*box1).high.x, (*box2).high.x));
}

/*      box_right       -       is box1 strictly right of box2?
 */
pub unsafe fn box_right(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPgt((*box1).low.x, (*box2).high.x));
}

/*      box_overright   -       is the left edge of box1 at or right of
 *                              the left edge of box2?
 */
pub unsafe fn box_overright(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPge((*box1).low.x, (*box2).low.x));
}

/*      box_below       -       is box1 strictly below box2?
 */
pub unsafe fn box_below(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPlt((*box1).high.y, (*box2).low.y));
}

/*      box_overbelow   -       is the upper edge of box1 at or below
 *                              the upper edge of box2?
 */
pub unsafe fn box_overbelow(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPle((*box1).high.y, (*box2).high.y));
}

/*      box_above       -       is box1 strictly above box2?
 */
pub unsafe fn box_above(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPgt((*box1).low.y, (*box2).high.y));
}

/*      box_overabove   -       is the lower edge of box1 at or above
 *                              the lower edge of box2?
 */
pub unsafe fn box_overabove(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPge((*box1).low.y, (*box2).low.y));
}

/*      box_contained   -       is box1 contained by box2?
 */
pub unsafe fn box_contained(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(box_contain_box(box2, box1));
}

/*      box_contain     -       does box1 contain box2?
 */
pub unsafe fn box_contain(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(box_contain_box(box1, box2));
}

/*
 * Check whether the second box is in the first box or on its border
 */
unsafe fn box_contain_box(contains_box: *mut BOX, contained_box: *mut BOX) -> bool {
    return FPge((*contains_box).high.x, (*contained_box).high.x)
        && FPle((*contains_box).low.x, (*contained_box).low.x)
        && FPge((*contains_box).high.y, (*contained_box).high.y)
        && FPle((*contains_box).low.y, (*contained_box).low.y);
}

/*      box_positionop  -
 *              is box1 entirely {above,below} box2?
 *
 * box_below_eq and box_above_eq are obsolete versions that (probably
 * erroneously) accept the equal-boundaries case.  Since these are not
 * in sync with the box_left and box_right code, they are deprecated and
 * not supported in the PG 8.1 rtree operator class extension.
 */
pub unsafe fn box_below_eq(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPle((*box1).high.y, (*box2).low.y));
}

pub unsafe fn box_above_eq(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPge((*box1).low.y, (*box2).high.y));
}

/*      box_relop       -       is area(box1) relop area(box2), within
 *                              our accuracy constraint?
 */
pub unsafe fn box_lt(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPlt(box_ar(box1), box_ar(box2)));
}

pub unsafe fn box_gt(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPgt(box_ar(box1), box_ar(box2)));
}

pub unsafe fn box_eq(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPeq(box_ar(box1), box_ar(box2)));
}

pub unsafe fn box_le(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPle(box_ar(box1), box_ar(box2)));
}

pub unsafe fn box_ge(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPge(box_ar(box1), box_ar(box2)));
}

/*----------------------------------------------------------
 *  "Arithmetic" operators on boxes.
 *---------------------------------------------------------*/

/*      box_area        -       returns the area of the box.
 */
pub unsafe fn box_area(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);

    PG_RETURN_FLOAT8!(box_ar(box_));
}

/*      box_width       -       returns the width of the box
 *                                (horizontal magnitude).
 */
pub unsafe fn box_width(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);

    PG_RETURN_FLOAT8!(box_wd(box_));
}

/*      box_height      -       returns the height of the box
 *                                (vertical magnitude).
 */
pub unsafe fn box_height(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);

    PG_RETURN_FLOAT8!(box_ht(box_));
}

/*      box_distance    -       returns the distance between the
 *                                center points of two boxes.
 */
pub unsafe fn box_distance(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);
    let mut a: Point = std::mem::zeroed();
    let mut b: Point = std::mem::zeroed();

    box_cn(&raw mut a, box1);
    box_cn(&raw mut b, box2);

    PG_RETURN_FLOAT8!(point_dt(&raw mut a, &raw mut b));
}

/*      box_center      -       returns the center point of the box.
 */
pub unsafe fn box_center(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let result: *mut Point = palloc(std::mem::size_of::<Point>()) as *mut Point;

    box_cn(result, box_);

    PG_RETURN_POINT_P!(result);
}

/*      box_ar  -       returns the area of the box.
 */
unsafe fn box_ar(box_: *mut BOX) -> float8 {
    return float8_mul(box_wd(box_), box_ht(box_));
}

/*      box_cn  -       stores the centerpoint of the box into *center.
 */
unsafe fn box_cn(center: *mut Point, box_: *mut BOX) {
    (*center).x = float8_div(float8_pl((*box_).high.x, (*box_).low.x), 2.0);
    (*center).y = float8_div(float8_pl((*box_).high.y, (*box_).low.y), 2.0);
}

/*      box_wd  -       returns the width (length) of the box
 *                                (horizontal magnitude).
 */
unsafe fn box_wd(box_: *mut BOX) -> float8 {
    return float8_mi((*box_).high.x, (*box_).low.x);
}

/*      box_ht  -       returns the height of the box
 *                                (vertical magnitude).
 */
unsafe fn box_ht(box_: *mut BOX) -> float8 {
    return float8_mi((*box_).high.y, (*box_).low.y);
}

/*----------------------------------------------------------
 *  Funky operations.
 *---------------------------------------------------------*/

/*      box_intersect   -
 *              returns the overlapping portion of two boxes,
 *                or NULL if they do not intersect.
 */
pub unsafe fn box_intersect(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);
    let result: *mut BOX;

    if !box_ov(box1, box2) {
        PG_RETURN_NULL!(fcinfo);
    }

    result = palloc(std::mem::size_of::<BOX>()) as *mut BOX;

    (*result).high.x = float8_min((*box1).high.x, (*box2).high.x);
    (*result).low.x = float8_max((*box1).low.x, (*box2).low.x);
    (*result).high.y = float8_min((*box1).high.y, (*box2).high.y);
    (*result).low.y = float8_max((*box1).low.y, (*box2).low.y);

    PG_RETURN_BOX_P!(result);
}

/*      box_diagonal    -
 *              returns a line segment which happens to be the
 *                positive-slope diagonal of "box".
 */
pub unsafe fn box_diagonal(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let result: *mut LSEG = palloc(std::mem::size_of::<LSEG>()) as *mut LSEG;

    statlseg_construct(result, &raw mut (*box_).high, &raw mut (*box_).low);

    PG_RETURN_LSEG_P!(result);
}

/***********************************************************************
 **
 **     Routines for 2D lines.
 **
 ***********************************************************************/

unsafe fn line_decode(
    mut s: *mut c_char,
    str: *const c_char,
    line: *mut LINE,
    escontext: *mut Node,
) -> bool {
    /* s was already advanced over leading '{' */
    if !single_decode(s, &raw mut (*line).A, &raw mut s, c"line".as_ptr(), str, escontext) {
        return false;
    }
    if {
        let c = *s;
        s = s.add(1);
        c
    } != DELIM
    {
        return line_decode_fail(str, escontext);
    }
    if !single_decode(s, &raw mut (*line).B, &raw mut s, c"line".as_ptr(), str, escontext) {
        return false;
    }
    if {
        let c = *s;
        s = s.add(1);
        c
    } != DELIM
    {
        return line_decode_fail(str, escontext);
    }
    if !single_decode(s, &raw mut (*line).C, &raw mut s, c"line".as_ptr(), str, escontext) {
        return false;
    }
    if {
        let c = *s;
        s = s.add(1);
        c
    } != RDELIM_L
    {
        return line_decode_fail(str, escontext);
    }
    while isspace(*s) {
        s = s.add(1);
    }
    if *s != 0 {
        return line_decode_fail(str, escontext);
    }
    return true;
}

unsafe fn line_decode_fail(str: *const c_char, escontext: *mut Node) -> bool {
    ereturn!(
        escontext,
        false,
        errmsg!(
            "invalid input syntax for type {}: \"{}\"",
            "line",
            std::ffi::CStr::from_ptr(str).to_string_lossy()
        )
    );
}

pub unsafe fn line_in(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let line: *mut LINE = palloc(std::mem::size_of::<LINE>()) as *mut LINE;
    let mut lseg: LSEG = std::mem::zeroed();
    let mut isopen: bool = false;
    let mut s: *mut c_char;

    s = str;
    while isspace(*s) {
        s = s.add(1);
    }
    if *s == LDELIM_L {
        if !line_decode(s.add(1), str, line, escontext) {
            PG_RETURN_NULL!(fcinfo);
        }
        if FPzero((*line).A) && FPzero((*line).B) {
            ereturn!(
                escontext,
                0 as Datum,
                errmsg!("invalid line specification: A and B cannot both be zero")
            );
        }
    } else {
        if !path_decode(
            s,
            true,
            2,
            &raw mut lseg.p[0],
            &raw mut isopen,
            null_mut(),
            c"line".as_ptr(),
            str,
            escontext,
        ) {
            PG_RETURN_NULL!(fcinfo);
        }
        if point_eq_point(&raw mut lseg.p[0], &raw mut lseg.p[1]) {
            ereturn!(
                escontext,
                0 as Datum,
                errmsg!("invalid line specification: must be two distinct points")
            );
        }

        /*
         * XXX lseg_sl() and line_construct() can throw overflow/underflow
         * errors.  Eventually we should allow those to be soft, but the
         * notational pain seems to outweigh the value for now.
         */
        line_construct(line, &raw mut lseg.p[0], lseg_sl(&raw mut lseg));
    }

    PG_RETURN_LINE_P!(line);
}

pub unsafe fn line_out(fcinfo: FunctionCallInfo) -> Datum {
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);
    let astr: *mut c_char = float8out_internal((*line).A);
    let bstr: *mut c_char = float8out_internal((*line).B);
    let cstr: *mut c_char = float8out_internal((*line).C);

    PG_RETURN_CSTRING!(psprintf_line(astr, bstr, cstr));
}

/*
 *      line_recv           - converts external binary format to line
 */
pub unsafe fn line_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let line: *mut LINE;

    line = palloc(std::mem::size_of::<LINE>()) as *mut LINE;

    (*line).A = pq_getmsgfloat8(buf);
    (*line).B = pq_getmsgfloat8(buf);
    (*line).C = pq_getmsgfloat8(buf);

    if FPzero((*line).A) && FPzero((*line).B) {
        ereport!(
            ERROR,
            errmsg!("invalid line specification: A and B cannot both be zero")
        );
    }

    PG_RETURN_LINE_P!(line);
}

/*
 *      line_send           - converts line to binary format
 */
pub unsafe fn line_send(fcinfo: FunctionCallInfo) -> Datum {
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_begintypsend(&raw mut buf);
    pq_sendfloat8(&raw mut buf, (*line).A);
    pq_sendfloat8(&raw mut buf, (*line).B);
    pq_sendfloat8(&raw mut buf, (*line).C);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&raw mut buf));
}

/*----------------------------------------------------------
 *  Conversion routines from one line formula to internal.
 *      Internal form:  Ax+By+C=0
 *---------------------------------------------------------*/

/*
 * Fill already-allocated LINE struct from the point and the slope
 */
#[inline]
unsafe fn line_construct(result: *mut LINE, pt: *mut Point, m: float8) {
    if m.is_infinite() {
        /* vertical - use "x = C" */
        (*result).A = -1.0;
        (*result).B = 0.0;
        (*result).C = (*pt).x;
    } else if m == 0.0 {
        /* horizontal - use "y = C" */
        (*result).A = 0.0;
        (*result).B = -1.0;
        (*result).C = (*pt).y;
    } else {
        /* use "mx - y + yinter = 0" */
        (*result).A = m;
        (*result).B = -1.0;
        (*result).C = float8_mi((*pt).y, float8_mul(m, (*pt).x));
        /* on some platforms, the preceding expression tends to produce -0 */
        if (*result).C == 0.0 {
            (*result).C = 0.0;
        }
    }
}

/* line_construct_pp()
 * two points
 */
pub unsafe fn line_construct_pp(fcinfo: FunctionCallInfo) -> Datum {
    let pt1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let pt2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut LINE = palloc(std::mem::size_of::<LINE>()) as *mut LINE;

    if point_eq_point(pt1, pt2) {
        ereport!(
            ERROR,
            errmsg!("invalid line specification: must be two distinct points")
        );
    }

    line_construct(result, pt1, point_sl(pt1, pt2));

    PG_RETURN_LINE_P!(result);
}

/*----------------------------------------------------------
 *  Relative position routines.
 *---------------------------------------------------------*/

pub unsafe fn line_intersect(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);
    let l2: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(line_interpt_line(null_mut(), l1, l2));
}

pub unsafe fn line_parallel(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);
    let l2: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(!line_interpt_line(null_mut(), l1, l2));
}

pub unsafe fn line_perp(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);
    let l2: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 1);

    if FPzero((*l1).A) {
        PG_RETURN_BOOL!(FPzero((*l2).B));
    }
    if FPzero((*l2).A) {
        PG_RETURN_BOOL!(FPzero((*l1).B));
    }
    if FPzero((*l1).B) {
        PG_RETURN_BOOL!(FPzero((*l2).A));
    }
    if FPzero((*l2).B) {
        PG_RETURN_BOOL!(FPzero((*l1).A));
    }

    PG_RETURN_BOOL!(FPeq(
        float8_div(
            float8_mul((*l1).A, (*l2).A),
            float8_mul((*l1).B, (*l2).B)
        ),
        -1.0
    ));
}

pub unsafe fn line_vertical(fcinfo: FunctionCallInfo) -> Datum {
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);

    PG_RETURN_BOOL!(FPzero((*line).B));
}

pub unsafe fn line_horizontal(fcinfo: FunctionCallInfo) -> Datum {
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);

    PG_RETURN_BOOL!(FPzero((*line).A));
}

/*
 * Check whether the two lines are the same
 */
pub unsafe fn line_eq(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);
    let l2: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 1);
    let ratio: float8;

    /* If any NaNs are involved, insist on exact equality */
    if (*l1).A.is_nan()
        || (*l1).B.is_nan()
        || (*l1).C.is_nan()
        || (*l2).A.is_nan()
        || (*l2).B.is_nan()
        || (*l2).C.is_nan()
    {
        PG_RETURN_BOOL!(
            float8_eq((*l1).A, (*l2).A)
                && float8_eq((*l1).B, (*l2).B)
                && float8_eq((*l1).C, (*l2).C)
        );
    }

    /* Otherwise, lines whose parameters are proportional are the same */
    if !FPzero((*l2).A) {
        ratio = float8_div((*l1).A, (*l2).A);
    } else if !FPzero((*l2).B) {
        ratio = float8_div((*l1).B, (*l2).B);
    } else if !FPzero((*l2).C) {
        ratio = float8_div((*l1).C, (*l2).C);
    } else {
        ratio = 1.0;
    }

    PG_RETURN_BOOL!(
        FPeq((*l1).A, float8_mul(ratio, (*l2).A))
            && FPeq((*l1).B, float8_mul(ratio, (*l2).B))
            && FPeq((*l1).C, float8_mul(ratio, (*l2).C))
    );
}

/*----------------------------------------------------------
 *  Line arithmetic routines.
 *---------------------------------------------------------*/

/*
 * Return slope of the line
 */
#[inline]
unsafe fn line_sl(line: *mut LINE) -> float8 {
    if FPzero((*line).A) {
        return 0.0;
    }
    if FPzero((*line).B) {
        return get_float8_infinity();
    }
    return float8_div((*line).A, -(*line).B);
}

/*
 * Return inverse slope of the line
 */
#[inline]
unsafe fn line_invsl(line: *mut LINE) -> float8 {
    if FPzero((*line).A) {
        return get_float8_infinity();
    }
    if FPzero((*line).B) {
        return 0.0;
    }
    return float8_div((*line).B, (*line).A);
}

/* line_distance()
 * Distance between two lines.
 */
pub unsafe fn line_distance(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);
    let l2: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 1);
    let ratio: float8;

    if line_interpt_line(null_mut(), l1, l2) {
        /* intersecting? */
        PG_RETURN_FLOAT8!(0.0);
    }

    if !FPzero((*l1).A) && !(*l1).A.is_nan() && !FPzero((*l2).A) && !(*l2).A.is_nan() {
        ratio = float8_div((*l1).A, (*l2).A);
    } else if !FPzero((*l1).B) && !(*l1).B.is_nan() && !FPzero((*l2).B) && !(*l2).B.is_nan() {
        ratio = float8_div((*l1).B, (*l2).B);
    } else {
        ratio = 1.0;
    }

    PG_RETURN_FLOAT8!(float8_div(
        fabs(float8_mi((*l1).C, float8_mul(ratio, (*l2).C))),
        HYPOT((*l1).A, (*l1).B)
    ));
}

/* line_interpt()
 * Point where two lines l1, l2 intersect (if any)
 */
pub unsafe fn line_interpt(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);
    let l2: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 1);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    if !line_interpt_line(result, l1, l2) {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_POINT_P!(result);
}

/*
 * Internal version of line_interpt
 *
 * Return whether two lines intersect. If *result is not NULL, it is set to
 * the intersection point.
 *
 * NOTE: If the lines are identical then we will find they are parallel
 * and report "no intersection".  This is a little weird, but since
 * there's no *unique* intersection, maybe it's appropriate behavior.
 *
 * If the lines have NaN constants, we will return true, and the intersection
 * point would have NaN coordinates.  We shouldn't return false in this case
 * because that would mean the lines are parallel.
 */
unsafe fn line_interpt_line(result: *mut Point, l1: *mut LINE, l2: *mut LINE) -> bool {
    let mut x: float8;
    let mut y: float8;

    if !FPzero((*l1).B) {
        if FPeq((*l2).A, float8_mul((*l1).A, float8_div((*l2).B, (*l1).B))) {
            return false;
        }

        x = float8_div(
            float8_mi(float8_mul((*l1).B, (*l2).C), float8_mul((*l2).B, (*l1).C)),
            float8_mi(float8_mul((*l1).A, (*l2).B), float8_mul((*l2).A, (*l1).B)),
        );
        y = float8_div(-float8_pl(float8_mul((*l1).A, x), (*l1).C), (*l1).B);
    } else if !FPzero((*l2).B) {
        if FPeq((*l1).A, float8_mul((*l2).A, float8_div((*l1).B, (*l2).B))) {
            return false;
        }

        x = float8_div(
            float8_mi(float8_mul((*l2).B, (*l1).C), float8_mul((*l1).B, (*l2).C)),
            float8_mi(float8_mul((*l2).A, (*l1).B), float8_mul((*l1).A, (*l2).B)),
        );
        y = float8_div(-float8_pl(float8_mul((*l2).A, x), (*l2).C), (*l2).B);
    } else {
        return false;
    }

    /* On some platforms, the preceding expressions tend to produce -0. */
    if x == 0.0 {
        x = 0.0;
    }
    if y == 0.0 {
        y = 0.0;
    }

    if !result.is_null() {
        point_construct(result, x, y);
    }

    return true;
}

/***********************************************************************
 **
 **     Routines for 2D paths (sequences of line segments, also
 **             called `polylines').
 **
 **             This is not a general package for geometric paths,
 **             which of course include polygons; the emphasis here
 **             is on (for example) usefulness in wire layout.
 **
 ***********************************************************************/

/*----------------------------------------------------------
 *  String to path / path to string conversion.
 *      External format:
 *              "((xcoord, ycoord),... )"
 *              "[(xcoord, ycoord),... ]"
 *              "(xcoord, ycoord),... "
 *              "[xcoord, ycoord,... ]"
 *      Also support older format:
 *              "(closed, npts, xcoord, ycoord,... )"
 *---------------------------------------------------------*/

pub unsafe fn path_area(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let mut area: float8 = 0.0;
    let mut i: c_int;
    let mut j: c_int;

    if (*path).closed == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    i = 0;
    while i < (*path).npts {
        j = (i + 1) % (*path).npts;
        area = float8_pl(
            area,
            float8_mul(
                (*path).p.as_ptr().add(i as usize).read().x,
                (*path).p.as_ptr().add(j as usize).read().y,
            ),
        );
        area = float8_mi(
            area,
            float8_mul(
                (*path).p.as_ptr().add(i as usize).read().y,
                (*path).p.as_ptr().add(j as usize).read().x,
            ),
        );
        i += 1;
    }

    PG_RETURN_FLOAT8!(float8_div(fabs(area), 2.0));
}

pub unsafe fn path_in(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let path: *mut PATH;
    let mut isopen: bool = false;
    let mut s: *mut c_char;
    let npts: c_int;
    let size: c_int;
    let base_size: c_int;
    let mut depth: c_int = 0;

    npts = pair_count(str, b',' as c_char);
    if npts <= 0 {
        ereturn!(
            escontext,
            0 as Datum,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                "path",
                std::ffi::CStr::from_ptr(str).to_string_lossy()
            )
        );
    }

    s = str;
    while isspace(*s) {
        s = s.add(1);
    }

    /* skip single leading paren */
    if *s == LDELIM && strrchr(s, LDELIM as c_int) == s {
        s = s.add(1);
        depth += 1;
    }

    base_size = (std::mem::size_of::<Point>() as c_int) * npts;
    size = offsetof_path_p() as c_int + base_size;

    /* Check for integer overflow */
    if base_size / npts != std::mem::size_of::<Point>() as c_int || size <= base_size {
        ereturn!(
            escontext,
            0 as Datum,
            errmsg!("too many points requested")
        );
    }

    path = palloc(size as Size) as *mut PATH;

    SET_VARSIZE(path as *mut c_char, size);
    (*path).npts = npts;

    if !path_decode(
        s,
        true,
        npts,
        (*path).p.as_mut_ptr(),
        &raw mut isopen,
        &raw mut s,
        c"path".as_ptr(),
        str,
        escontext,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }

    if depth >= 1 {
        if {
            let c = *s;
            s = s.add(1);
            c
        } != RDELIM
        {
            ereturn!(
                escontext,
                0 as Datum,
                errmsg!(
                    "invalid input syntax for type {}: \"{}\"",
                    "path",
                    std::ffi::CStr::from_ptr(str).to_string_lossy()
                )
            );
        }
        while isspace(*s) {
            s = s.add(1);
        }
    }
    if *s != 0 {
        ereturn!(
            escontext,
            0 as Datum,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                "path",
                std::ffi::CStr::from_ptr(str).to_string_lossy()
            )
        );
    }

    (*path).closed = (!isopen) as int32;
    /* prevent instability in unused pad bytes */
    (*path).dummy = 0;

    PG_RETURN_PATH_P!(path);
}

pub unsafe fn path_out(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);

    PG_RETURN_CSTRING!(path_encode(
        if (*path).closed != 0 {
            PATH_CLOSED
        } else {
            PATH_OPEN
        },
        (*path).npts,
        (*path).p.as_mut_ptr()
    ));
}

/*
 *      path_recv           - converts external binary format to path
 *
 * External representation is closed flag (a boolean byte), int32 number
 * of points, and the points.
 */
pub unsafe fn path_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let path: *mut PATH;
    let closed: c_int;
    let npts: int32;
    let mut i: int32;
    let size: c_int;

    closed = pq_getmsgbyte(buf);
    npts = pq_getmsgint(buf, std::mem::size_of::<int32>() as c_int) as int32;
    if npts <= 0
        || npts
            >= ((INT_MAX - offsetof_path_p() as int32) / std::mem::size_of::<Point>() as int32)
    {
        ereport!(
            ERROR,
            errmsg!("invalid number of points in external \"path\" value")
        );
    }

    size = offsetof_path_p() as c_int + std::mem::size_of::<Point>() as c_int * npts;
    path = palloc(size as Size) as *mut PATH;

    SET_VARSIZE(path as *mut c_char, size);
    (*path).npts = npts;
    (*path).closed = if closed != 0 { 1 } else { 0 };
    /* prevent instability in unused pad bytes */
    (*path).dummy = 0;

    i = 0;
    while i < npts {
        (*(*path).p.as_mut_ptr().add(i as usize)).x = pq_getmsgfloat8(buf);
        (*(*path).p.as_mut_ptr().add(i as usize)).y = pq_getmsgfloat8(buf);
        i += 1;
    }

    PG_RETURN_PATH_P!(path);
}

/*
 *      path_send           - converts path to binary format
 */
pub unsafe fn path_send(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();
    let mut i: int32;

    pq_begintypsend(&raw mut buf);
    pq_sendbyte(&raw mut buf, if (*path).closed != 0 { 1 } else { 0 });
    pq_sendint32(&raw mut buf, (*path).npts as u32);
    i = 0;
    while i < (*path).npts {
        pq_sendfloat8(&raw mut buf, (*(*path).p.as_ptr().add(i as usize)).x);
        pq_sendfloat8(&raw mut buf, (*(*path).p.as_ptr().add(i as usize)).y);
        i += 1;
    }
    PG_RETURN_BYTEA_P!(pq_endtypsend(&raw mut buf));
}

/*----------------------------------------------------------
 *  Relational operators.
 *      These are based on the path cardinality,
 *      as stupid as that sounds.
 *
 *      Better relops and access methods coming soon.
 *---------------------------------------------------------*/

pub unsafe fn path_n_lt(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let p2: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 1);

    PG_RETURN_BOOL!((*p1).npts < (*p2).npts);
}

pub unsafe fn path_n_gt(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let p2: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 1);

    PG_RETURN_BOOL!((*p1).npts > (*p2).npts);
}

pub unsafe fn path_n_eq(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let p2: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 1);

    PG_RETURN_BOOL!((*p1).npts == (*p2).npts);
}

pub unsafe fn path_n_le(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let p2: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 1);

    PG_RETURN_BOOL!((*p1).npts <= (*p2).npts);
}

pub unsafe fn path_n_ge(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let p2: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 1);

    PG_RETURN_BOOL!((*p1).npts >= (*p2).npts);
}

/*----------------------------------------------------------
 * Conversion operators.
 *---------------------------------------------------------*/

pub unsafe fn path_isclosed(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);

    PG_RETURN_BOOL!((*path).closed != 0);
}

pub unsafe fn path_isopen(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);

    PG_RETURN_BOOL!((*path).closed == 0);
}

pub unsafe fn path_npoints(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);

    PG_RETURN_INT32!((*path).npts);
}

pub unsafe fn path_close(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P_COPY!(fcinfo, 0);

    (*path).closed = true as int32;

    PG_RETURN_PATH_P!(path);
}

pub unsafe fn path_open(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P_COPY!(fcinfo, 0);

    (*path).closed = false as int32;

    PG_RETURN_PATH_P!(path);
}

/* path_inter -
 *      Does p1 intersect p2 at any point?
 *      Use bounding boxes for a quick (O(n)) check, then do a
 *      O(n^2) iterative edge check.
 */
pub unsafe fn path_inter(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let p2: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 1);
    let mut b1: BOX = std::mem::zeroed();
    let mut b2: BOX = std::mem::zeroed();
    let mut i: c_int;
    let mut j: c_int;
    let mut seg1: LSEG = std::mem::zeroed();
    let mut seg2: LSEG = std::mem::zeroed();

    Assert!((*p1).npts > 0 && (*p2).npts > 0);

    b1.high.x = (*(*p1).p.as_ptr().add(0)).x;
    b1.low.x = b1.high.x;
    b1.high.y = (*(*p1).p.as_ptr().add(0)).y;
    b1.low.y = b1.high.y;
    i = 1;
    while i < (*p1).npts {
        b1.high.x = float8_max((*(*p1).p.as_ptr().add(i as usize)).x, b1.high.x);
        b1.high.y = float8_max((*(*p1).p.as_ptr().add(i as usize)).y, b1.high.y);
        b1.low.x = float8_min((*(*p1).p.as_ptr().add(i as usize)).x, b1.low.x);
        b1.low.y = float8_min((*(*p1).p.as_ptr().add(i as usize)).y, b1.low.y);
        i += 1;
    }
    b2.high.x = (*(*p2).p.as_ptr().add(0)).x;
    b2.low.x = b2.high.x;
    b2.high.y = (*(*p2).p.as_ptr().add(0)).y;
    b2.low.y = b2.high.y;
    i = 1;
    while i < (*p2).npts {
        b2.high.x = float8_max((*(*p2).p.as_ptr().add(i as usize)).x, b2.high.x);
        b2.high.y = float8_max((*(*p2).p.as_ptr().add(i as usize)).y, b2.high.y);
        b2.low.x = float8_min((*(*p2).p.as_ptr().add(i as usize)).x, b2.low.x);
        b2.low.y = float8_min((*(*p2).p.as_ptr().add(i as usize)).y, b2.low.y);
        i += 1;
    }
    if !box_ov(&raw mut b1, &raw mut b2) {
        PG_RETURN_BOOL!(false);
    }

    /* pairwise check lseg intersections */
    i = 0;
    while i < (*p1).npts {
        let iprev: c_int;

        if i > 0 {
            iprev = i - 1;
        } else {
            if (*p1).closed == 0 {
                i += 1;
                continue;
            }
            iprev = (*p1).npts - 1; /* include the closure segment */
        }

        j = 0;
        while j < (*p2).npts {
            let jprev: c_int;

            if j > 0 {
                jprev = j - 1;
            } else {
                if (*p2).closed == 0 {
                    j += 1;
                    continue;
                }
                jprev = (*p2).npts - 1; /* include the closure segment */
            }

            statlseg_construct(
                &raw mut seg1,
                (*p1).p.as_mut_ptr().add(iprev as usize),
                (*p1).p.as_mut_ptr().add(i as usize),
            );
            statlseg_construct(
                &raw mut seg2,
                (*p2).p.as_mut_ptr().add(jprev as usize),
                (*p2).p.as_mut_ptr().add(j as usize),
            );
            if lseg_interpt_lseg(null_mut(), &raw mut seg1, &raw mut seg2) {
                PG_RETURN_BOOL!(true);
            }
            j += 1;
        }
        i += 1;
    }

    /* if we dropped through, no two segs intersected */
    PG_RETURN_BOOL!(false);
}

/* path_distance()
 * This essentially does a cartesian product of the lsegs in the
 *  two paths, and finds the min distance between any two lsegs
 */
pub unsafe fn path_distance(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let p2: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 1);
    let mut min: float8 = 0.0; /* initialize to keep compiler quiet */
    let mut have_min: bool = false;
    let mut tmp: float8;
    let mut i: c_int;
    let mut j: c_int;
    let mut seg1: LSEG = std::mem::zeroed();
    let mut seg2: LSEG = std::mem::zeroed();

    i = 0;
    while i < (*p1).npts {
        let iprev: c_int;

        if i > 0 {
            iprev = i - 1;
        } else {
            if (*p1).closed == 0 {
                i += 1;
                continue;
            }
            iprev = (*p1).npts - 1; /* include the closure segment */
        }

        j = 0;
        while j < (*p2).npts {
            let jprev: c_int;

            if j > 0 {
                jprev = j - 1;
            } else {
                if (*p2).closed == 0 {
                    j += 1;
                    continue;
                }
                jprev = (*p2).npts - 1; /* include the closure segment */
            }

            statlseg_construct(
                &raw mut seg1,
                (*p1).p.as_mut_ptr().add(iprev as usize),
                (*p1).p.as_mut_ptr().add(i as usize),
            );
            statlseg_construct(
                &raw mut seg2,
                (*p2).p.as_mut_ptr().add(jprev as usize),
                (*p2).p.as_mut_ptr().add(j as usize),
            );

            tmp = lseg_closept_lseg(null_mut(), &raw mut seg1, &raw mut seg2);
            if !have_min || float8_lt(tmp, min) {
                min = tmp;
                have_min = true;
            }
            j += 1;
        }
        i += 1;
    }

    if !have_min {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_FLOAT8!(min);
}

/*----------------------------------------------------------
 *  "Arithmetic" operations.
 *---------------------------------------------------------*/

pub unsafe fn path_length(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let mut result: float8 = 0.0;
    let mut i: c_int;

    i = 0;
    while i < (*path).npts {
        let iprev: c_int;

        if i > 0 {
            iprev = i - 1;
        } else {
            if (*path).closed == 0 {
                i += 1;
                continue;
            }
            iprev = (*path).npts - 1; /* include the closure segment */
        }

        result = float8_pl(
            result,
            point_dt(
                (*path).p.as_mut_ptr().add(iprev as usize),
                (*path).p.as_mut_ptr().add(i as usize),
            ),
        );
        i += 1;
    }

    PG_RETURN_FLOAT8!(result);
}

/***********************************************************************
 **
 **     Routines for 2D points.
 **
 ***********************************************************************/

/*----------------------------------------------------------
 *  String to point, point to string conversion.
 *      External format:
 *              "(x,y)"
 *              "x,y"
 *---------------------------------------------------------*/

pub unsafe fn point_in(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let point: *mut Point = palloc(std::mem::size_of::<Point>()) as *mut Point;

    /* Ignore failure from pair_decode, since our return value won't matter */
    pair_decode(
        str,
        &raw mut (*point).x,
        &raw mut (*point).y,
        null_mut(),
        c"point".as_ptr(),
        str,
        (*fcinfo).context,
    );
    PG_RETURN_POINT_P!(point);
}

pub unsafe fn point_out(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);

    PG_RETURN_CSTRING!(path_encode(PATH_NONE, 1, pt));
}

/*
 *      point_recv          - converts external binary format to point
 */
pub unsafe fn point_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let point: *mut Point;

    point = palloc(std::mem::size_of::<Point>()) as *mut Point;
    (*point).x = pq_getmsgfloat8(buf);
    (*point).y = pq_getmsgfloat8(buf);
    PG_RETURN_POINT_P!(point);
}

/*
 *      point_send          - converts point to binary format
 */
pub unsafe fn point_send(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_begintypsend(&raw mut buf);
    pq_sendfloat8(&raw mut buf, (*pt).x);
    pq_sendfloat8(&raw mut buf, (*pt).y);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&raw mut buf));
}

/*
 * Initialize a point
 */
#[inline]
unsafe fn point_construct(result: *mut Point, x: float8, y: float8) {
    (*result).x = x;
    (*result).y = y;
}

/*----------------------------------------------------------
 *  Relational operators for Points.
 *      Since we do have a sense of coordinates being
 *      "equal" to a given accuracy (point_vert, point_horiz),
 *      the other ops must preserve that sense.  This means
 *      that results may, strictly speaking, be a lie (unless
 *      EPSILON = 0.0).
 *---------------------------------------------------------*/

pub unsafe fn point_left(fcinfo: FunctionCallInfo) -> Datum {
    let pt1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let pt2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPlt((*pt1).x, (*pt2).x));
}

pub unsafe fn point_right(fcinfo: FunctionCallInfo) -> Datum {
    let pt1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let pt2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPgt((*pt1).x, (*pt2).x));
}

pub unsafe fn point_above(fcinfo: FunctionCallInfo) -> Datum {
    let pt1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let pt2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPgt((*pt1).y, (*pt2).y));
}

pub unsafe fn point_below(fcinfo: FunctionCallInfo) -> Datum {
    let pt1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let pt2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPlt((*pt1).y, (*pt2).y));
}

pub unsafe fn point_vert(fcinfo: FunctionCallInfo) -> Datum {
    let pt1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let pt2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPeq((*pt1).x, (*pt2).x));
}

pub unsafe fn point_horiz(fcinfo: FunctionCallInfo) -> Datum {
    let pt1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let pt2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPeq((*pt1).y, (*pt2).y));
}

pub unsafe fn point_eq(fcinfo: FunctionCallInfo) -> Datum {
    let pt1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let pt2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(point_eq_point(pt1, pt2));
}

pub unsafe fn point_ne(fcinfo: FunctionCallInfo) -> Datum {
    let pt1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let pt2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(!point_eq_point(pt1, pt2));
}

/*
 * Check whether the two points are the same
 */
#[inline]
unsafe fn point_eq_point(pt1: *mut Point, pt2: *mut Point) -> bool {
    /* If any NaNs are involved, insist on exact equality */
    if (*pt1).x.is_nan() || (*pt1).y.is_nan() || (*pt2).x.is_nan() || (*pt2).y.is_nan() {
        return float8_eq((*pt1).x, (*pt2).x) && float8_eq((*pt1).y, (*pt2).y);
    }

    return FPeq((*pt1).x, (*pt2).x) && FPeq((*pt1).y, (*pt2).y);
}

/*----------------------------------------------------------
 *  "Arithmetic" operators on points.
 *---------------------------------------------------------*/

#[no_mangle]
pub unsafe fn point_distance(fcinfo: FunctionCallInfo) -> Datum {
    let pt1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let pt2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(point_dt(pt1, pt2));
}

#[inline]
unsafe fn point_dt(pt1: *mut Point, pt2: *mut Point) -> float8 {
    return HYPOT(
        float8_mi((*pt1).x, (*pt2).x),
        float8_mi((*pt1).y, (*pt2).y),
    );
}

pub unsafe fn point_slope(fcinfo: FunctionCallInfo) -> Datum {
    let pt1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let pt2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(point_sl(pt1, pt2));
}

/*
 * Return slope of two points
 *
 * Note that this function returns Inf when the points are the same.
 */
#[inline]
unsafe fn point_sl(pt1: *mut Point, pt2: *mut Point) -> float8 {
    if FPeq((*pt1).x, (*pt2).x) {
        return get_float8_infinity();
    }
    if FPeq((*pt1).y, (*pt2).y) {
        return 0.0;
    }
    return float8_div(
        float8_mi((*pt1).y, (*pt2).y),
        float8_mi((*pt1).x, (*pt2).x),
    );
}

/*
 * Return inverse slope of two points
 *
 * Note that this function returns 0.0 when the points are the same.
 */
#[inline]
unsafe fn point_invsl(pt1: *mut Point, pt2: *mut Point) -> float8 {
    if FPeq((*pt1).x, (*pt2).x) {
        return 0.0;
    }
    if FPeq((*pt1).y, (*pt2).y) {
        return get_float8_infinity();
    }
    return float8_div(
        float8_mi((*pt1).x, (*pt2).x),
        float8_mi((*pt2).y, (*pt1).y),
    );
}

/***********************************************************************
 **
 **     Routines for 2D line segments.
 **
 ***********************************************************************/

/*----------------------------------------------------------
 *  String to lseg, lseg to string conversion.
 *      External forms: "[(x1, y1), (x2, y2)]"
 *                      "(x1, y1), (x2, y2)"
 *                      "x1, y1, x2, y2"
 *      closed form ok  "((x1, y1), (x2, y2))"
 *      (old form)      "(x1, y1, x2, y2)"
 *---------------------------------------------------------*/

pub unsafe fn lseg_in(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let lseg: *mut LSEG = palloc(std::mem::size_of::<LSEG>()) as *mut LSEG;
    let mut isopen: bool = false;

    if !path_decode(
        str,
        true,
        2,
        &raw mut (*lseg).p[0],
        &raw mut isopen,
        null_mut(),
        c"lseg".as_ptr(),
        str,
        escontext,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_LSEG_P!(lseg);
}

pub unsafe fn lseg_out(fcinfo: FunctionCallInfo) -> Datum {
    let ls: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);

    PG_RETURN_CSTRING!(path_encode(PATH_OPEN, 2, &raw mut (*ls).p[0]));
}

/*
 *      lseg_recv           - converts external binary format to lseg
 */
pub unsafe fn lseg_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let lseg: *mut LSEG;

    lseg = palloc(std::mem::size_of::<LSEG>()) as *mut LSEG;

    (*lseg).p[0].x = pq_getmsgfloat8(buf);
    (*lseg).p[0].y = pq_getmsgfloat8(buf);
    (*lseg).p[1].x = pq_getmsgfloat8(buf);
    (*lseg).p[1].y = pq_getmsgfloat8(buf);

    PG_RETURN_LSEG_P!(lseg);
}

/*
 *      lseg_send           - converts lseg to binary format
 */
pub unsafe fn lseg_send(fcinfo: FunctionCallInfo) -> Datum {
    let ls: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_begintypsend(&raw mut buf);
    pq_sendfloat8(&raw mut buf, (*ls).p[0].x);
    pq_sendfloat8(&raw mut buf, (*ls).p[0].y);
    pq_sendfloat8(&raw mut buf, (*ls).p[1].x);
    pq_sendfloat8(&raw mut buf, (*ls).p[1].y);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&raw mut buf));
}

/* lseg_construct -
 *      form a LSEG from two Points.
 */
pub unsafe fn lseg_construct(fcinfo: FunctionCallInfo) -> Datum {
    let pt1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let pt2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut LSEG = palloc(std::mem::size_of::<LSEG>()) as *mut LSEG;

    statlseg_construct(result, pt1, pt2);

    PG_RETURN_LSEG_P!(result);
}

/* like lseg_construct, but assume space already allocated */
#[inline]
unsafe fn statlseg_construct(lseg: *mut LSEG, pt1: *mut Point, pt2: *mut Point) {
    (*lseg).p[0].x = (*pt1).x;
    (*lseg).p[0].y = (*pt1).y;
    (*lseg).p[1].x = (*pt2).x;
    (*lseg).p[1].y = (*pt2).y;
}

/*
 * Return slope of the line segment
 */
#[inline]
unsafe fn lseg_sl(lseg: *mut LSEG) -> float8 {
    return point_sl(&raw mut (*lseg).p[0], &raw mut (*lseg).p[1]);
}

/*
 * Return inverse slope of the line segment
 */
#[inline]
unsafe fn lseg_invsl(lseg: *mut LSEG) -> float8 {
    return point_invsl(&raw mut (*lseg).p[0], &raw mut (*lseg).p[1]);
}

pub unsafe fn lseg_length(fcinfo: FunctionCallInfo) -> Datum {
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);

    PG_RETURN_FLOAT8!(point_dt(&raw mut (*lseg).p[0], &raw mut (*lseg).p[1]));
}

/*----------------------------------------------------------
 *  Relative position routines.
 *---------------------------------------------------------*/

/*
 **  find intersection of the two lines, and see if it falls on
 **  both segments.
 */
#[no_mangle]
pub unsafe fn lseg_intersect(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let l2: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_BOOL!(lseg_interpt_lseg(null_mut(), l1, l2));
}

pub unsafe fn lseg_parallel(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let l2: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPeq(lseg_sl(l1), lseg_sl(l2)));
}

/*
 * Determine if two line segments are perpendicular.
 */
pub unsafe fn lseg_perp(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let l2: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPeq(lseg_sl(l1), lseg_invsl(l2)));
}

pub unsafe fn lseg_vertical(fcinfo: FunctionCallInfo) -> Datum {
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);

    PG_RETURN_BOOL!(FPeq((*lseg).p[0].x, (*lseg).p[1].x));
}

pub unsafe fn lseg_horizontal(fcinfo: FunctionCallInfo) -> Datum {
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);

    PG_RETURN_BOOL!(FPeq((*lseg).p[0].y, (*lseg).p[1].y));
}

pub unsafe fn lseg_eq(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let l2: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_BOOL!(
        point_eq_point(&raw mut (*l1).p[0], &raw mut (*l2).p[0])
            && point_eq_point(&raw mut (*l1).p[1], &raw mut (*l2).p[1])
    );
}

pub unsafe fn lseg_ne(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let l2: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_BOOL!(
        !point_eq_point(&raw mut (*l1).p[0], &raw mut (*l2).p[0])
            || !point_eq_point(&raw mut (*l1).p[1], &raw mut (*l2).p[1])
    );
}

pub unsafe fn lseg_lt(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let l2: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPlt(
        point_dt(&raw mut (*l1).p[0], &raw mut (*l1).p[1]),
        point_dt(&raw mut (*l2).p[0], &raw mut (*l2).p[1])
    ));
}

pub unsafe fn lseg_le(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let l2: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPle(
        point_dt(&raw mut (*l1).p[0], &raw mut (*l1).p[1]),
        point_dt(&raw mut (*l2).p[0], &raw mut (*l2).p[1])
    ));
}

pub unsafe fn lseg_gt(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let l2: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPgt(
        point_dt(&raw mut (*l1).p[0], &raw mut (*l1).p[1]),
        point_dt(&raw mut (*l2).p[0], &raw mut (*l2).p[1])
    ));
}

pub unsafe fn lseg_ge(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let l2: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPge(
        point_dt(&raw mut (*l1).p[0], &raw mut (*l1).p[1]),
        point_dt(&raw mut (*l2).p[0], &raw mut (*l2).p[1])
    ));
}

/*----------------------------------------------------------
 *  Line arithmetic routines.
 *---------------------------------------------------------*/

/* lseg_distance -
 *      If two segments don't intersect, then the closest
 *      point will be from one of the endpoints to the other
 *      segment.
 */
pub unsafe fn lseg_distance(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let l2: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(lseg_closept_lseg(null_mut(), l1, l2));
}

pub unsafe fn lseg_center(fcinfo: FunctionCallInfo) -> Datum {
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    (*result).x = float8_div(float8_pl((*lseg).p[0].x, (*lseg).p[1].x), 2.0);
    (*result).y = float8_div(float8_pl((*lseg).p[0].y, (*lseg).p[1].y), 2.0);

    PG_RETURN_POINT_P!(result);
}

/*
 * Return whether the two segments intersect. If *result is not NULL,
 * it is set to the intersection point.
 *
 * This function is almost perfectly symmetric, even though it doesn't look
 * like it.  See lseg_interpt_line() for the other half of it.
 */
unsafe fn lseg_interpt_lseg(result: *mut Point, l1: *mut LSEG, l2: *mut LSEG) -> bool {
    let mut interpt: Point = std::mem::zeroed();
    let mut tmp: LINE = std::mem::zeroed();

    line_construct(&raw mut tmp, &raw mut (*l2).p[0], lseg_sl(l2));
    if !lseg_interpt_line(&raw mut interpt, l1, &raw mut tmp) {
        return false;
    }

    /*
     * If the line intersection point isn't within l2, there is no valid
     * segment intersection point at all.
     */
    if !lseg_contain_point(l2, &raw mut interpt) {
        return false;
    }

    if !result.is_null() {
        *result = interpt;
    }

    return true;
}

#[no_mangle]
pub unsafe fn lseg_interpt(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let l2: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    if !lseg_interpt_lseg(result, l1, l2) {
        PG_RETURN_NULL!(fcinfo);
    }
    PG_RETURN_POINT_P!(result);
}

/***********************************************************************
 **
 **     Routines for position comparisons of differently-typed
 **             2D objects.
 **
 ***********************************************************************/

/*---------------------------------------------------------------------
 *      dist_
 *              Minimum distance from one object to another.
 *-------------------------------------------------------------------*/

/*
 * Distance from a point to a line
 */
pub unsafe fn dist_pl(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(line_closept_point(null_mut(), line, pt));
}

/*
 * Distance from a line to a point
 */
pub unsafe fn dist_lp(fcinfo: FunctionCallInfo) -> Datum {
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(line_closept_point(null_mut(), line, pt));
}

/*
 * Distance from a point to a lseg
 */
pub unsafe fn dist_ps(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(lseg_closept_point(null_mut(), lseg, pt));
}

/*
 * Distance from a lseg to a point
 */
pub unsafe fn dist_sp(fcinfo: FunctionCallInfo) -> Datum {
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(lseg_closept_point(null_mut(), lseg, pt));
}

unsafe fn dist_ppath_internal(pt: *mut Point, path: *mut PATH) -> float8 {
    let mut result: float8 = 0.0; /* keep compiler quiet */
    let mut have_min: bool = false;
    let tmp: float8;
    let mut i: c_int;
    let mut lseg: LSEG = std::mem::zeroed();

    Assert!((*path).npts > 0);

    /*
     * The distance from a point to a path is the smallest distance from the
     * point to any of its constituent segments.
     */
    i = 0;
    while i < (*path).npts {
        let iprev: c_int;

        if i > 0 {
            iprev = i - 1;
        } else {
            if (*path).closed == 0 {
                i += 1;
                continue;
            }
            iprev = (*path).npts - 1; /* Include the closure segment */
        }

        statlseg_construct(
            &raw mut lseg,
            (*path).p.as_mut_ptr().add(iprev as usize),
            (*path).p.as_mut_ptr().add(i as usize),
        );
        let tmp = lseg_closept_point(null_mut(), &raw mut lseg, pt);
        if !have_min || float8_lt(tmp, result) {
            result = tmp;
            have_min = true;
        }
        i += 1;
    }
    let _ = tmp;

    return result;
}

/*
 * Distance from a point to a path
 */
pub unsafe fn dist_ppath(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let path: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(dist_ppath_internal(pt, path));
}

/*
 * Distance from a path to a point
 */
pub unsafe fn dist_pathp(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(dist_ppath_internal(pt, path));
}

/*
 * Distance from a point to a box
 */
pub unsafe fn dist_pb(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(box_closept_point(null_mut(), box_, pt));
}

/*
 * Distance from a box to a point
 */
pub unsafe fn dist_bp(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(box_closept_point(null_mut(), box_, pt));
}

/*
 * Distance from a lseg to a line
 */
pub unsafe fn dist_sl(fcinfo: FunctionCallInfo) -> Datum {
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(lseg_closept_line(null_mut(), lseg, line));
}

/*
 * Distance from a line to a lseg
 */
pub unsafe fn dist_ls(fcinfo: FunctionCallInfo) -> Datum {
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(lseg_closept_line(null_mut(), lseg, line));
}

/*
 * Distance from a lseg to a box
 */
pub unsafe fn dist_sb(fcinfo: FunctionCallInfo) -> Datum {
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(box_closept_lseg(null_mut(), box_, lseg));
}

/*
 * Distance from a box to a lseg
 */
pub unsafe fn dist_bs(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(box_closept_lseg(null_mut(), box_, lseg));
}

unsafe fn dist_cpoly_internal(circle: *mut CIRCLE, poly: *mut POLYGON) -> float8 {
    let mut result: float8;

    /* calculate distance to center, and subtract radius */
    result = float8_mi(
        dist_ppoly_internal(&raw mut (*circle).center, poly),
        (*circle).radius,
    );
    if result < 0.0 {
        result = 0.0;
    }

    return result;
}

/*
 * Distance from a circle to a polygon
 */
pub unsafe fn dist_cpoly(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(dist_cpoly_internal(circle, poly));
}

/*
 * Distance from a polygon to a circle
 */
pub unsafe fn dist_polyc(fcinfo: FunctionCallInfo) -> Datum {
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(dist_cpoly_internal(circle, poly));
}

/*
 * Distance from a point to a polygon
 */
pub unsafe fn dist_ppoly(fcinfo: FunctionCallInfo) -> Datum {
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(dist_ppoly_internal(point, poly));
}

pub unsafe fn dist_polyp(fcinfo: FunctionCallInfo) -> Datum {
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_FLOAT8!(dist_ppoly_internal(point, poly));
}

unsafe fn dist_ppoly_internal(pt: *mut Point, poly: *mut POLYGON) -> float8 {
    let mut result: float8;
    let d: float8;
    let mut i: c_int;
    let mut seg: LSEG = std::mem::zeroed();

    if point_inside(pt, (*poly).npts, (*poly).p.as_mut_ptr()) != 0 {
        return 0.0;
    }

    /* initialize distance with segment between first and last points */
    seg.p[0].x = (*(*poly).p.as_ptr().add(0)).x;
    seg.p[0].y = (*(*poly).p.as_ptr().add(0)).y;
    seg.p[1].x = (*(*poly).p.as_ptr().add(((*poly).npts - 1) as usize)).x;
    seg.p[1].y = (*(*poly).p.as_ptr().add(((*poly).npts - 1) as usize)).y;
    result = lseg_closept_point(null_mut(), &raw mut seg, pt);

    /* check distances for other segments */
    i = 0;
    while i < (*poly).npts - 1 {
        seg.p[0].x = (*(*poly).p.as_ptr().add(i as usize)).x;
        seg.p[0].y = (*(*poly).p.as_ptr().add(i as usize)).y;
        seg.p[1].x = (*(*poly).p.as_ptr().add((i + 1) as usize)).x;
        seg.p[1].y = (*(*poly).p.as_ptr().add((i + 1) as usize)).y;
        let d = lseg_closept_point(null_mut(), &raw mut seg, pt);
        if float8_lt(d, result) {
            result = d;
        }
        i += 1;
    }
    let _ = d;

    return result;
}

/*---------------------------------------------------------------------
 *      interpt_
 *              Intersection point of objects.
 *              We choose to ignore the "point" of intersection between
 *                lines and boxes, since there are typically two.
 *-------------------------------------------------------------------*/

/*
 * Return whether the line segment intersect with the line. If *result is not
 * NULL, it is set to the intersection point.
 */
unsafe fn lseg_interpt_line(result: *mut Point, lseg: *mut LSEG, line: *mut LINE) -> bool {
    let mut interpt: Point = std::mem::zeroed();
    let mut tmp: LINE = std::mem::zeroed();

    /*
     * First, we promote the line segment to a line, because we know how to
     * find the intersection point of two lines.  If they don't have an
     * intersection point, we are done.
     */
    line_construct(&raw mut tmp, &raw mut (*lseg).p[0], lseg_sl(lseg));
    if !line_interpt_line(&raw mut interpt, &raw mut tmp, line) {
        return false;
    }

    /*
     * Then, we check whether the intersection point is actually on the line
     * segment.
     */
    if !lseg_contain_point(lseg, &raw mut interpt) {
        return false;
    }
    if !result.is_null() {
        /*
         * If there is an intersection, then check explicitly for matching
         * endpoints since there may be rounding effects with annoying LSB
         * residue.
         */
        if point_eq_point(&raw mut (*lseg).p[0], &raw mut interpt) {
            *result = (*lseg).p[0];
        } else if point_eq_point(&raw mut (*lseg).p[1], &raw mut interpt) {
            *result = (*lseg).p[1];
        } else {
            *result = interpt;
        }
    }

    return true;
}

/*---------------------------------------------------------------------
 *      close_
 *              Point of closest proximity between objects.
 *-------------------------------------------------------------------*/

/*
 * If *result is not NULL, it is set to the intersection point of a
 * perpendicular of the line through the point.  Returns the distance
 * of those two points.
 */
unsafe fn line_closept_point(result: *mut Point, line: *mut LINE, point: *mut Point) -> float8 {
    let mut closept: Point = std::mem::zeroed();
    let mut tmp: LINE = std::mem::zeroed();

    /*
     * We drop a perpendicular to find the intersection point.  Ordinarily we
     * should always find it, but that can fail in the presence of NaN
     * coordinates, and perhaps even from simple roundoff issues.
     */
    line_construct(&raw mut tmp, point, line_invsl(line));
    if !line_interpt_line(&raw mut closept, &raw mut tmp, line) {
        if !result.is_null() {
            *result = *point;
        }

        return get_float8_nan();
    }

    if !result.is_null() {
        *result = closept;
    }

    return point_dt(&raw mut closept, point);
}

pub unsafe fn close_pl(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 1);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    if line_closept_point(result, line, pt).is_nan() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_POINT_P!(result);
}

/*
 * Closest point on line segment to specified point.
 *
 * If *result is not NULL, set it to the closest point on the line segment
 * to the point.  Returns the distance of the two points.
 */
unsafe fn lseg_closept_point(result: *mut Point, lseg: *mut LSEG, pt: *mut Point) -> float8 {
    let mut closept: Point = std::mem::zeroed();
    let mut tmp: LINE = std::mem::zeroed();

    /*
     * To find the closest point, we draw a perpendicular line from the point
     * to the line segment.
     */
    line_construct(
        &raw mut tmp,
        pt,
        point_invsl(&raw mut (*lseg).p[0], &raw mut (*lseg).p[1]),
    );
    lseg_closept_line(&raw mut closept, lseg, &raw mut tmp);

    if !result.is_null() {
        *result = closept;
    }

    return point_dt(&raw mut closept, pt);
}

pub unsafe fn close_ps(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    if lseg_closept_point(result, lseg, pt).is_nan() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_POINT_P!(result);
}

/*
 * Closest point on line segment to line segment
 */
unsafe fn lseg_closept_lseg(result: *mut Point, on_lseg: *mut LSEG, to_lseg: *mut LSEG) -> float8 {
    let mut point: Point = std::mem::zeroed();
    let mut dist: float8;
    let d: float8;

    /* First, we handle the case when the line segments are intersecting. */
    if lseg_interpt_lseg(result, on_lseg, to_lseg) {
        return 0.0;
    }

    /*
     * Then, we find the closest points from the endpoints of the second line
     * segment, and keep the closest one.
     */
    dist = lseg_closept_point(result, on_lseg, &raw mut (*to_lseg).p[0]);
    let d = lseg_closept_point(&raw mut point, on_lseg, &raw mut (*to_lseg).p[1]);
    if float8_lt(d, dist) {
        dist = d;
        if !result.is_null() {
            *result = point;
        }
    }

    /* The closest point can still be one of the endpoints, so we test them. */
    let d = lseg_closept_point(null_mut(), to_lseg, &raw mut (*on_lseg).p[0]);
    if float8_lt(d, dist) {
        dist = d;
        if !result.is_null() {
            *result = (*on_lseg).p[0];
        }
    }
    let d = lseg_closept_point(null_mut(), to_lseg, &raw mut (*on_lseg).p[1]);
    if float8_lt(d, dist) {
        dist = d;
        if !result.is_null() {
            *result = (*on_lseg).p[1];
        }
    }
    let _ = d;

    return dist;
}

pub unsafe fn close_lseg(fcinfo: FunctionCallInfo) -> Datum {
    let l1: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let l2: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);
    let result: *mut Point;

    if lseg_sl(l1) == lseg_sl(l2) {
        PG_RETURN_NULL!(fcinfo);
    }

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    if lseg_closept_lseg(result, l2, l1).is_nan() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_POINT_P!(result);
}

/*
 * Closest point on or in box to specified point.
 *
 * If *result is not NULL, set it to the closest point on the box to the
 * given point, and return the distance of the two points.
 */
unsafe fn box_closept_point(result: *mut Point, box_: *mut BOX, pt: *mut Point) -> float8 {
    let mut dist: float8;
    let d: float8;
    let mut point: Point = std::mem::zeroed();
    let mut closept: Point = std::mem::zeroed();
    let mut lseg: LSEG = std::mem::zeroed();

    if box_contain_point(box_, pt) {
        if !result.is_null() {
            *result = *pt;
        }

        return 0.0;
    }

    /* pairwise check lseg distances */
    point.x = (*box_).low.x;
    point.y = (*box_).high.y;
    statlseg_construct(&raw mut lseg, &raw mut (*box_).low, &raw mut point);
    dist = lseg_closept_point(result, &raw mut lseg, pt);

    statlseg_construct(&raw mut lseg, &raw mut (*box_).high, &raw mut point);
    let d = lseg_closept_point(&raw mut closept, &raw mut lseg, pt);
    if float8_lt(d, dist) {
        dist = d;
        if !result.is_null() {
            *result = closept;
        }
    }

    point.x = (*box_).high.x;
    point.y = (*box_).low.y;
    statlseg_construct(&raw mut lseg, &raw mut (*box_).low, &raw mut point);
    let d = lseg_closept_point(&raw mut closept, &raw mut lseg, pt);
    if float8_lt(d, dist) {
        dist = d;
        if !result.is_null() {
            *result = closept;
        }
    }

    statlseg_construct(&raw mut lseg, &raw mut (*box_).high, &raw mut point);
    let d = lseg_closept_point(&raw mut closept, &raw mut lseg, pt);
    if float8_lt(d, dist) {
        dist = d;
        if !result.is_null() {
            *result = closept;
        }
    }
    let _ = d;

    return dist;
}

pub unsafe fn close_pb(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    if box_closept_point(result, box_, pt).is_nan() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_POINT_P!(result);
}

/*
 * Closest point on line segment to line.
 *
 * Return the distance between the line and the closest point of the line
 * segment to the line.  If *result is not NULL, set it to that point.
 *
 * NOTE: When the lines are parallel, endpoints of one of the line segment
 * are FPeq(), in presence of NaN or Infinite coordinates, or perhaps =
 * even because of simple roundoff issues, there may not be a single closest
 * point.  We are likely to set the result to the second endpoint in these
 * cases.
 */
unsafe fn lseg_closept_line(result: *mut Point, lseg: *mut LSEG, line: *mut LINE) -> float8 {
    let dist1: float8;
    let dist2: float8;

    if lseg_interpt_line(result, lseg, line) {
        return 0.0;
    }

    dist1 = line_closept_point(null_mut(), line, &raw mut (*lseg).p[0]);
    dist2 = line_closept_point(null_mut(), line, &raw mut (*lseg).p[1]);

    if dist1 < dist2 {
        if !result.is_null() {
            *result = (*lseg).p[0];
        }

        return dist1;
    } else {
        if !result.is_null() {
            *result = (*lseg).p[1];
        }

        return dist2;
    }
}

pub unsafe fn close_ls(fcinfo: FunctionCallInfo) -> Datum {
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);
    let result: *mut Point;

    if lseg_sl(lseg) == line_sl(line) {
        PG_RETURN_NULL!(fcinfo);
    }

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    if lseg_closept_line(result, lseg, line).is_nan() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_POINT_P!(result);
}

/*
 * Closest point on or in box to line segment.
 *
 * Returns the distance between the closest point on or in the box to
 * the line segment.  If *result is not NULL, it is set to that point.
 */
unsafe fn box_closept_lseg(result: *mut Point, box_: *mut BOX, lseg: *mut LSEG) -> float8 {
    let mut dist: float8;
    let d: float8;
    let mut point: Point = std::mem::zeroed();
    let mut closept: Point = std::mem::zeroed();
    let mut bseg: LSEG = std::mem::zeroed();

    if box_interpt_lseg(result, box_, lseg) {
        return 0.0;
    }

    /* pairwise check lseg distances */
    point.x = (*box_).low.x;
    point.y = (*box_).high.y;
    statlseg_construct(&raw mut bseg, &raw mut (*box_).low, &raw mut point);
    dist = lseg_closept_lseg(result, &raw mut bseg, lseg);

    statlseg_construct(&raw mut bseg, &raw mut (*box_).high, &raw mut point);
    let d = lseg_closept_lseg(&raw mut closept, &raw mut bseg, lseg);
    if float8_lt(d, dist) {
        dist = d;
        if !result.is_null() {
            *result = closept;
        }
    }

    point.x = (*box_).high.x;
    point.y = (*box_).low.y;
    statlseg_construct(&raw mut bseg, &raw mut (*box_).low, &raw mut point);
    let d = lseg_closept_lseg(&raw mut closept, &raw mut bseg, lseg);
    if float8_lt(d, dist) {
        dist = d;
        if !result.is_null() {
            *result = closept;
        }
    }

    statlseg_construct(&raw mut bseg, &raw mut (*box_).high, &raw mut point);
    let d = lseg_closept_lseg(&raw mut closept, &raw mut bseg, lseg);
    if float8_lt(d, dist) {
        dist = d;
        if !result.is_null() {
            *result = closept;
        }
    }
    let _ = d;

    return dist;
}

pub unsafe fn close_sb(fcinfo: FunctionCallInfo) -> Datum {
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    if box_closept_lseg(result, box_, lseg).is_nan() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_POINT_P!(result);
}

/*---------------------------------------------------------------------
 *      on_
 *              Whether one object lies completely within another.
 *-------------------------------------------------------------------*/

/*
 *      Does the point satisfy the equation?
 */
unsafe fn line_contain_point(line: *mut LINE, point: *mut Point) -> bool {
    return FPzero(float8_pl(
        float8_pl(
            float8_mul((*line).A, (*point).x),
            float8_mul((*line).B, (*point).y),
        ),
        (*line).C,
    ));
}

pub unsafe fn on_pl(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(line_contain_point(line, pt));
}

/*
 *      Determine colinearity by detecting a triangle inequality.
 * This algorithm seems to behave nicely even with lsb residues - tgl 1997-07-09
 */
unsafe fn lseg_contain_point(lseg: *mut LSEG, pt: *mut Point) -> bool {
    return FPeq(
        point_dt(pt, &raw mut (*lseg).p[0]) + point_dt(pt, &raw mut (*lseg).p[1]),
        point_dt(&raw mut (*lseg).p[0], &raw mut (*lseg).p[1]),
    );
}

pub unsafe fn on_ps(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 1);

    PG_RETURN_BOOL!(lseg_contain_point(lseg, pt));
}

/*
 * Check whether the point is in the box or on its border
 */
unsafe fn box_contain_point(box_: *mut BOX, point: *mut Point) -> bool {
    return (*box_).high.x >= (*point).x
        && (*box_).low.x <= (*point).x
        && (*box_).high.y >= (*point).y
        && (*box_).low.y <= (*point).y;
}

pub unsafe fn on_pb(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(box_contain_point(box_, pt));
}

pub unsafe fn box_contain_pt(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(box_contain_point(box_, pt));
}

/* on_ppath -
 *      Whether a point lies within (on) a polyline.
 *      If open, we have to (groan) check each segment.
 * (uses same algorithm as for point intersecting segment - tgl 1997-07-09)
 *      If closed, we use the old O(n) ray method for point-in-polygon.
 *              The ray is horizontal, from pt out to the right.
 *              Each segment that crosses the ray counts as an
 *              intersection; note that an endpoint or edge may touch
 *              but not cross.
 *              (we can do p-in-p in lg(n), but it takes preprocessing)
 */
pub unsafe fn on_ppath(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let path: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 1);
    let mut i: c_int;
    let n: c_int;
    let mut a: float8;
    let mut b: float8;

    /*-- OPEN --*/
    if (*path).closed == 0 {
        n = (*path).npts - 1;
        a = point_dt(pt, (*path).p.as_mut_ptr().add(0));
        i = 0;
        while i < n {
            b = point_dt(pt, (*path).p.as_mut_ptr().add((i + 1) as usize));
            if FPeq(
                float8_pl(a, b),
                point_dt(
                    (*path).p.as_mut_ptr().add(i as usize),
                    (*path).p.as_mut_ptr().add((i + 1) as usize),
                ),
            ) {
                PG_RETURN_BOOL!(true);
            }
            a = b;
            i += 1;
        }
        PG_RETURN_BOOL!(false);
    }

    /*-- CLOSED --*/
    PG_RETURN_BOOL!(point_inside(pt, (*path).npts, (*path).p.as_mut_ptr()) != 0);
}

/*
 * Check whether the line segment is on the line or close enough
 *
 * It is, if both of its points are on the line or close enough.
 */
pub unsafe fn on_sl(fcinfo: FunctionCallInfo) -> Datum {
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(
        line_contain_point(line, &raw mut (*lseg).p[0])
            && line_contain_point(line, &raw mut (*lseg).p[1])
    );
}

/*
 * Check whether the line segment is in the box or on its border
 *
 * It is, if both of its points are in the box or on its border.
 */
unsafe fn box_contain_lseg(box_: *mut BOX, lseg: *mut LSEG) -> bool {
    return box_contain_point(box_, &raw mut (*lseg).p[0])
        && box_contain_point(box_, &raw mut (*lseg).p[1]);
}

pub unsafe fn on_sb(fcinfo: FunctionCallInfo) -> Datum {
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(box_contain_lseg(box_, lseg));
}

/*---------------------------------------------------------------------
 *      inter_
 *              Whether one object intersects another.
 *-------------------------------------------------------------------*/

pub unsafe fn inter_sl(fcinfo: FunctionCallInfo) -> Datum {
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(lseg_interpt_line(null_mut(), lseg, line));
}

/*
 * Do line segment and box intersect?
 *
 * Segment completely inside box counts as intersection.
 * If you want only segments crossing box boundaries,
 *  try converting box to path first.
 *
 * This function also sets the *result to the closest point on the line
 * segment to the center of the box when they overlap and the result is
 * not NULL.  It is somewhat arbitrary, but maybe the best we can do as
 * there are typically two points they intersect.
 *
 * Optimize for non-intersection by checking for box intersection first.
 * - thomas 1998-01-30
 */
unsafe fn box_interpt_lseg(result: *mut Point, box_: *mut BOX, lseg: *mut LSEG) -> bool {
    let mut lbox: BOX = std::mem::zeroed();
    let mut bseg: LSEG = std::mem::zeroed();
    let mut point: Point = std::mem::zeroed();

    lbox.low.x = float8_min((*lseg).p[0].x, (*lseg).p[1].x);
    lbox.low.y = float8_min((*lseg).p[0].y, (*lseg).p[1].y);
    lbox.high.x = float8_max((*lseg).p[0].x, (*lseg).p[1].x);
    lbox.high.y = float8_max((*lseg).p[0].y, (*lseg).p[1].y);

    /* nothing close to overlap? then not going to intersect */
    if !box_ov(&raw mut lbox, box_) {
        return false;
    }

    if !result.is_null() {
        box_cn(&raw mut point, box_);
        lseg_closept_point(result, lseg, &raw mut point);
    }

    /* an endpoint of segment is inside box? then clearly intersects */
    if box_contain_point(box_, &raw mut (*lseg).p[0])
        || box_contain_point(box_, &raw mut (*lseg).p[1])
    {
        return true;
    }

    /* pairwise check lseg intersections */
    point.x = (*box_).low.x;
    point.y = (*box_).high.y;
    statlseg_construct(&raw mut bseg, &raw mut (*box_).low, &raw mut point);
    if lseg_interpt_lseg(null_mut(), &raw mut bseg, lseg) {
        return true;
    }

    statlseg_construct(&raw mut bseg, &raw mut (*box_).high, &raw mut point);
    if lseg_interpt_lseg(null_mut(), &raw mut bseg, lseg) {
        return true;
    }

    point.x = (*box_).high.x;
    point.y = (*box_).low.y;
    statlseg_construct(&raw mut bseg, &raw mut (*box_).low, &raw mut point);
    if lseg_interpt_lseg(null_mut(), &raw mut bseg, lseg) {
        return true;
    }

    statlseg_construct(&raw mut bseg, &raw mut (*box_).high, &raw mut point);
    if lseg_interpt_lseg(null_mut(), &raw mut bseg, lseg) {
        return true;
    }

    /* if we dropped through, no two segs intersected */
    return false;
}

pub unsafe fn inter_sb(fcinfo: FunctionCallInfo) -> Datum {
    let lseg: *mut LSEG = PG_GETARG_LSEG_P!(fcinfo, 0);
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);

    PG_RETURN_BOOL!(box_interpt_lseg(null_mut(), box_, lseg));
}

/* inter_lb()
 * Do line and box intersect?
 */
pub unsafe fn inter_lb(fcinfo: FunctionCallInfo) -> Datum {
    let line: *mut LINE = PG_GETARG_LINE_P!(fcinfo, 0);
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);
    let mut bseg: LSEG = std::mem::zeroed();
    let mut p1: Point = std::mem::zeroed();
    let mut p2: Point = std::mem::zeroed();

    /* pairwise check lseg intersections */
    p1.x = (*box_).low.x;
    p1.y = (*box_).low.y;
    p2.x = (*box_).low.x;
    p2.y = (*box_).high.y;
    statlseg_construct(&raw mut bseg, &raw mut p1, &raw mut p2);
    if lseg_interpt_line(null_mut(), &raw mut bseg, line) {
        PG_RETURN_BOOL!(true);
    }
    p1.x = (*box_).high.x;
    p1.y = (*box_).high.y;
    statlseg_construct(&raw mut bseg, &raw mut p1, &raw mut p2);
    if lseg_interpt_line(null_mut(), &raw mut bseg, line) {
        PG_RETURN_BOOL!(true);
    }
    p2.x = (*box_).high.x;
    p2.y = (*box_).low.y;
    statlseg_construct(&raw mut bseg, &raw mut p1, &raw mut p2);
    if lseg_interpt_line(null_mut(), &raw mut bseg, line) {
        PG_RETURN_BOOL!(true);
    }
    p1.x = (*box_).low.x;
    p1.y = (*box_).low.y;
    statlseg_construct(&raw mut bseg, &raw mut p1, &raw mut p2);
    if lseg_interpt_line(null_mut(), &raw mut bseg, line) {
        PG_RETURN_BOOL!(true);
    }

    /* if we dropped through, no intersection */
    PG_RETURN_BOOL!(false);
}

/*------------------------------------------------------------------
 * The following routines define a data type and operator class for
 * POLYGONS .... Part of which (the polygon's bounding box) is built on
 * top of the BOX data type.
 *
 * make_bound_box - create the bounding box for the input polygon
 *------------------------------------------------------------------*/

/*---------------------------------------------------------------------
 * Make the smallest bounding box for the given polygon.
 *---------------------------------------------------------------------*/
unsafe fn make_bound_box(poly: *mut POLYGON) {
    let mut i: c_int;
    let mut x1: float8;
    let mut y1: float8;
    let mut x2: float8;
    let mut y2: float8;

    Assert!((*poly).npts > 0);

    x1 = (*(*poly).p.as_ptr().add(0)).x;
    x2 = x1;
    y1 = (*(*poly).p.as_ptr().add(0)).y;
    y2 = y1;
    i = 1;
    while i < (*poly).npts {
        if float8_lt((*(*poly).p.as_ptr().add(i as usize)).x, x1) {
            x1 = (*(*poly).p.as_ptr().add(i as usize)).x;
        }
        if float8_gt((*(*poly).p.as_ptr().add(i as usize)).x, x2) {
            x2 = (*(*poly).p.as_ptr().add(i as usize)).x;
        }
        if float8_lt((*(*poly).p.as_ptr().add(i as usize)).y, y1) {
            y1 = (*(*poly).p.as_ptr().add(i as usize)).y;
        }
        if float8_gt((*(*poly).p.as_ptr().add(i as usize)).y, y2) {
            y2 = (*(*poly).p.as_ptr().add(i as usize)).y;
        }
        i += 1;
    }

    (*poly).boundbox.low.x = x1;
    (*poly).boundbox.high.x = x2;
    (*poly).boundbox.low.y = y1;
    (*poly).boundbox.high.y = y2;
}

/*------------------------------------------------------------------
 * poly_in - read in the polygon from a string specification
 *
 *      External format:
 *              "((x0,y0),...,(xn,yn))"
 *              "x0,y0,...,xn,yn"
 *              also supports the older style "(x1,...,xn,y1,...yn)"
 *------------------------------------------------------------------*/
pub unsafe fn poly_in(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let poly: *mut POLYGON;
    let npts: c_int;
    let size: c_int;
    let base_size: c_int;
    let mut isopen: bool = false;

    npts = pair_count(str, b',' as c_char);
    if npts <= 0 {
        ereturn!(
            escontext,
            0 as Datum,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                "polygon",
                std::ffi::CStr::from_ptr(str).to_string_lossy()
            )
        );
    }

    base_size = std::mem::size_of::<Point>() as c_int * npts;
    size = offsetof_polygon_p() as c_int + base_size;

    /* Check for integer overflow */
    if base_size / npts != std::mem::size_of::<Point>() as c_int || size <= base_size {
        ereturn!(
            escontext,
            0 as Datum,
            errmsg!("too many points requested")
        );
    }

    poly = palloc0(size as Size) as *mut POLYGON; /* zero any holes */

    SET_VARSIZE(poly as *mut c_char, size);
    (*poly).npts = npts;

    if !path_decode(
        str,
        false,
        npts,
        (*poly).p.as_mut_ptr(),
        &raw mut isopen,
        null_mut(),
        c"polygon".as_ptr(),
        str,
        escontext,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }

    make_bound_box(poly);

    PG_RETURN_POLYGON_P!(poly);
}

/*---------------------------------------------------------------
 * poly_out - convert internal POLYGON representation to the
 *            character string format "((f8,f8),...,(f8,f8))"
 *---------------------------------------------------------------*/
pub unsafe fn poly_out(fcinfo: FunctionCallInfo) -> Datum {
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);

    PG_RETURN_CSTRING!(path_encode(PATH_CLOSED, (*poly).npts, (*poly).p.as_mut_ptr()));
}

/*
 *      poly_recv           - converts external binary format to polygon
 *
 * External representation is int32 number of points, and the points.
 * We recompute the bounding box on read, instead of trusting it to
 * be valid.  (Checking it would take just as long, so may as well
 * omit it from external representation.)
 */
pub unsafe fn poly_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let poly: *mut POLYGON;
    let npts: int32;
    let mut i: int32;
    let size: c_int;

    npts = pq_getmsgint(buf, std::mem::size_of::<int32>() as c_int) as int32;
    if npts <= 0
        || npts
            >= ((INT_MAX - offsetof_polygon_p() as int32) / std::mem::size_of::<Point>() as int32)
    {
        ereport!(
            ERROR,
            errmsg!("invalid number of points in external \"polygon\" value")
        );
    }

    size = offsetof_polygon_p() as c_int + std::mem::size_of::<Point>() as c_int * npts;
    poly = palloc0(size as Size) as *mut POLYGON; /* zero any holes */

    SET_VARSIZE(poly as *mut c_char, size);
    (*poly).npts = npts;

    i = 0;
    while i < npts {
        (*(*poly).p.as_mut_ptr().add(i as usize)).x = pq_getmsgfloat8(buf);
        (*(*poly).p.as_mut_ptr().add(i as usize)).y = pq_getmsgfloat8(buf);
        i += 1;
    }

    make_bound_box(poly);

    PG_RETURN_POLYGON_P!(poly);
}

/*
 *      poly_send           - converts polygon to binary format
 */
pub unsafe fn poly_send(fcinfo: FunctionCallInfo) -> Datum {
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();
    let mut i: int32;

    pq_begintypsend(&raw mut buf);
    pq_sendint32(&raw mut buf, (*poly).npts as u32);
    i = 0;
    while i < (*poly).npts {
        pq_sendfloat8(&raw mut buf, (*(*poly).p.as_ptr().add(i as usize)).x);
        pq_sendfloat8(&raw mut buf, (*(*poly).p.as_ptr().add(i as usize)).y);
        i += 1;
    }
    PG_RETURN_BYTEA_P!(pq_endtypsend(&raw mut buf));
}

/*-------------------------------------------------------
 * Is polygon A strictly left of polygon B? i.e. is
 * the right most point of A left of the left most point
 * of B?
 *-------------------------------------------------------*/
pub unsafe fn poly_left(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let result: bool;

    result = (*polya).boundbox.high.x < (*polyb).boundbox.low.x;

    /*
     * Avoid leaking memory for toasted inputs ... needed for rtree indexes
     */
    PG_FREE_IF_COPY!(fcinfo, polya, 0);
    PG_FREE_IF_COPY!(fcinfo, polyb, 1);

    PG_RETURN_BOOL!(result);
}

/*-------------------------------------------------------
 * Is polygon A overlapping or left of polygon B? i.e. is
 * the right most point of A at or left of the right most point
 * of B?
 *-------------------------------------------------------*/
pub unsafe fn poly_overleft(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let result: bool;

    result = (*polya).boundbox.high.x <= (*polyb).boundbox.high.x;

    /*
     * Avoid leaking memory for toasted inputs ... needed for rtree indexes
     */
    PG_FREE_IF_COPY!(fcinfo, polya, 0);
    PG_FREE_IF_COPY!(fcinfo, polyb, 1);

    PG_RETURN_BOOL!(result);
}

/*-------------------------------------------------------
 * Is polygon A strictly right of polygon B? i.e. is
 * the left most point of A right of the right most point
 * of B?
 *-------------------------------------------------------*/
pub unsafe fn poly_right(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let result: bool;

    result = (*polya).boundbox.low.x > (*polyb).boundbox.high.x;

    /*
     * Avoid leaking memory for toasted inputs ... needed for rtree indexes
     */
    PG_FREE_IF_COPY!(fcinfo, polya, 0);
    PG_FREE_IF_COPY!(fcinfo, polyb, 1);

    PG_RETURN_BOOL!(result);
}

/*-------------------------------------------------------
 * Is polygon A overlapping or right of polygon B? i.e. is
 * the left most point of A at or right of the left most point
 * of B?
 *-------------------------------------------------------*/
pub unsafe fn poly_overright(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let result: bool;

    result = (*polya).boundbox.low.x >= (*polyb).boundbox.low.x;

    /*
     * Avoid leaking memory for toasted inputs ... needed for rtree indexes
     */
    PG_FREE_IF_COPY!(fcinfo, polya, 0);
    PG_FREE_IF_COPY!(fcinfo, polyb, 1);

    PG_RETURN_BOOL!(result);
}

/*-------------------------------------------------------
 * Is polygon A strictly below polygon B? i.e. is
 * the upper most point of A below the lower most point
 * of B?
 *-------------------------------------------------------*/
pub unsafe fn poly_below(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let result: bool;

    result = (*polya).boundbox.high.y < (*polyb).boundbox.low.y;

    /*
     * Avoid leaking memory for toasted inputs ... needed for rtree indexes
     */
    PG_FREE_IF_COPY!(fcinfo, polya, 0);
    PG_FREE_IF_COPY!(fcinfo, polyb, 1);

    PG_RETURN_BOOL!(result);
}

/*-------------------------------------------------------
 * Is polygon A overlapping or below polygon B? i.e. is
 * the upper most point of A at or below the upper most point
 * of B?
 *-------------------------------------------------------*/
pub unsafe fn poly_overbelow(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let result: bool;

    result = (*polya).boundbox.high.y <= (*polyb).boundbox.high.y;

    /*
     * Avoid leaking memory for toasted inputs ... needed for rtree indexes
     */
    PG_FREE_IF_COPY!(fcinfo, polya, 0);
    PG_FREE_IF_COPY!(fcinfo, polyb, 1);

    PG_RETURN_BOOL!(result);
}

/*-------------------------------------------------------
 * Is polygon A strictly above polygon B? i.e. is
 * the lower most point of A above the upper most point
 * of B?
 *-------------------------------------------------------*/
pub unsafe fn poly_above(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let result: bool;

    result = (*polya).boundbox.low.y > (*polyb).boundbox.high.y;

    /*
     * Avoid leaking memory for toasted inputs ... needed for rtree indexes
     */
    PG_FREE_IF_COPY!(fcinfo, polya, 0);
    PG_FREE_IF_COPY!(fcinfo, polyb, 1);

    PG_RETURN_BOOL!(result);
}

/*-------------------------------------------------------
 * Is polygon A overlapping or above polygon B? i.e. is
 * the lower most point of A at or above the lower most point
 * of B?
 *-------------------------------------------------------*/
pub unsafe fn poly_overabove(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let result: bool;

    result = (*polya).boundbox.low.y >= (*polyb).boundbox.low.y;

    /*
     * Avoid leaking memory for toasted inputs ... needed for rtree indexes
     */
    PG_FREE_IF_COPY!(fcinfo, polya, 0);
    PG_FREE_IF_COPY!(fcinfo, polyb, 1);

    PG_RETURN_BOOL!(result);
}

/*-------------------------------------------------------
 * Is polygon A the same as polygon B? i.e. are all the
 * points the same?
 * Check all points for matches in both forward and reverse
 *  direction since polygons are non-directional and are
 *  closed shapes.
 *-------------------------------------------------------*/
pub unsafe fn poly_same(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let result: bool;

    if (*polya).npts != (*polyb).npts {
        result = false;
    } else {
        result = plist_same((*polya).npts, (*polya).p.as_mut_ptr(), (*polyb).p.as_mut_ptr());
    }

    /*
     * Avoid leaking memory for toasted inputs ... needed for rtree indexes
     */
    PG_FREE_IF_COPY!(fcinfo, polya, 0);
    PG_FREE_IF_COPY!(fcinfo, polyb, 1);

    PG_RETURN_BOOL!(result);
}

/*-----------------------------------------------------------------
 * Determine if polygon A overlaps polygon B
 *-----------------------------------------------------------------*/
unsafe fn poly_overlap_internal(polya: *mut POLYGON, polyb: *mut POLYGON) -> bool {
    let mut result: bool;

    Assert!((*polya).npts > 0 && (*polyb).npts > 0);

    /* Quick check by bounding box */
    result = box_ov(&raw mut (*polya).boundbox, &raw mut (*polyb).boundbox);

    /*
     * Brute-force algorithm - try to find intersected edges, if so then
     * polygons are overlapped else check is one polygon inside other or not
     * by testing single point of them.
     */
    if result {
        let mut ia: c_int;
        let mut ib: c_int;
        let mut sa: LSEG = std::mem::zeroed();
        let mut sb: LSEG = std::mem::zeroed();

        /* Init first of polya's edge with last point */
        sa.p[0] = (*(*polya).p.as_ptr().add(((*polya).npts - 1) as usize)).clone();
        result = false;

        ia = 0;
        while ia < (*polya).npts && !result {
            /* Second point of polya's edge is a current one */
            sa.p[1] = (*(*polya).p.as_ptr().add(ia as usize)).clone();

            /* Init first of polyb's edge with last point */
            sb.p[0] = (*(*polyb).p.as_ptr().add(((*polyb).npts - 1) as usize)).clone();

            ib = 0;
            while ib < (*polyb).npts && !result {
                sb.p[1] = (*(*polyb).p.as_ptr().add(ib as usize)).clone();
                result = lseg_interpt_lseg(null_mut(), &raw mut sa, &raw mut sb);
                sb.p[0] = sb.p[1];
                ib += 1;
            }

            /*
             * move current endpoint to the first point of next edge
             */
            sa.p[0] = sa.p[1];
            ia += 1;
        }

        if !result {
            result = point_inside((*polya).p.as_mut_ptr(), (*polyb).npts, (*polyb).p.as_mut_ptr())
                != 0
                || point_inside((*polyb).p.as_mut_ptr(), (*polya).npts, (*polya).p.as_mut_ptr())
                    != 0;
        }
    }

    return result;
}

pub unsafe fn poly_overlap(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let result: bool;

    result = poly_overlap_internal(polya, polyb);

    /*
     * Avoid leaking memory for toasted inputs ... needed for rtree indexes
     */
    PG_FREE_IF_COPY!(fcinfo, polya, 0);
    PG_FREE_IF_COPY!(fcinfo, polyb, 1);

    PG_RETURN_BOOL!(result);
}

/*
 * Tests special kind of segment for in/out of polygon.
 * Special kind means:
 *  - point a should be on segment s
 *  - segment (a,b) should not be contained by s
 * Returns true if:
 *  - segment (a,b) is collinear to s and (a,b) is in polygon
 *  - segment (a,b) s not collinear to s. Note: that doesn't
 *    mean that segment is in polygon!
 */
unsafe fn touched_lseg_inside_poly(
    a: *mut Point,
    b: *mut Point,
    s: *mut LSEG,
    poly: *mut POLYGON,
    start: c_int,
) -> bool {
    /* point a is on s, b is not */
    let mut t: LSEG = std::mem::zeroed();

    t.p[0] = *a;
    t.p[1] = *b;

    if point_eq_point(a, (*s).p.as_mut_ptr()) {
        if lseg_contain_point(&raw mut t, (*s).p.as_mut_ptr().add(1)) {
            return lseg_inside_poly(b, (*s).p.as_mut_ptr().add(1), poly, start);
        }
    } else if point_eq_point(a, (*s).p.as_mut_ptr().add(1)) {
        if lseg_contain_point(&raw mut t, (*s).p.as_mut_ptr()) {
            return lseg_inside_poly(b, (*s).p.as_mut_ptr(), poly, start);
        }
    } else if lseg_contain_point(&raw mut t, (*s).p.as_mut_ptr()) {
        return lseg_inside_poly(b, (*s).p.as_mut_ptr(), poly, start);
    } else if lseg_contain_point(&raw mut t, (*s).p.as_mut_ptr().add(1)) {
        return lseg_inside_poly(b, (*s).p.as_mut_ptr().add(1), poly, start);
    }

    return true; /* may be not true, but that will check later */
}

/*
 * Returns true if segment (a,b) is in polygon, option
 * start is used for optimization - function checks
 * polygon's edges starting from start
 */
unsafe fn lseg_inside_poly(a: *mut Point, b: *mut Point, poly: *mut POLYGON, start: c_int) -> bool {
    let mut s: LSEG = std::mem::zeroed();
    let mut t: LSEG = std::mem::zeroed();
    let mut i: c_int;
    let mut res: bool = true;
    let mut intersection: bool = false;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    t.p[0] = *a;
    t.p[1] = *b;
    s.p[0] = (*(*poly).p.as_ptr().add(if start == 0 {
        ((*poly).npts - 1) as usize
    } else {
        (start - 1) as usize
    }))
    .clone();

    i = start;
    while i < (*poly).npts && res {
        let mut interpt: Point = std::mem::zeroed();

        CHECK_FOR_INTERRUPTS();

        s.p[1] = (*(*poly).p.as_ptr().add(i as usize)).clone();

        if lseg_contain_point(&raw mut s, t.p.as_mut_ptr()) {
            if lseg_contain_point(&raw mut s, t.p.as_mut_ptr().add(1)) {
                return true; /* t is contained by s */
            }

            /* Y-cross */
            res = touched_lseg_inside_poly(
                t.p.as_mut_ptr(),
                t.p.as_mut_ptr().add(1),
                &raw mut s,
                poly,
                i + 1,
            );
        } else if lseg_contain_point(&raw mut s, t.p.as_mut_ptr().add(1)) {
            /* Y-cross */
            res = touched_lseg_inside_poly(
                t.p.as_mut_ptr().add(1),
                t.p.as_mut_ptr(),
                &raw mut s,
                poly,
                i + 1,
            );
        } else if lseg_interpt_lseg(&raw mut interpt, &raw mut t, &raw mut s) {
            /*
             * segments are X-crossing, go to check each subsegment
             */

            intersection = true;
            res = lseg_inside_poly(t.p.as_mut_ptr(), &raw mut interpt, poly, i + 1);
            if res {
                res = lseg_inside_poly(t.p.as_mut_ptr().add(1), &raw mut interpt, poly, i + 1);
            }
        }

        s.p[0] = s.p[1];
        i += 1;
    }

    if res && !intersection {
        let mut p: Point = std::mem::zeroed();

        /*
         * if X-intersection wasn't found, then check central point of tested
         * segment. In opposite case we already check all subsegments
         */
        p.x = float8_div(float8_pl(t.p[0].x, t.p[1].x), 2.0);
        p.y = float8_div(float8_pl(t.p[0].y, t.p[1].y), 2.0);

        res = point_inside(&raw mut p, (*poly).npts, (*poly).p.as_mut_ptr()) != 0;
    }

    return res;
}

/*
 * Check whether the first polygon contains the second
 */
unsafe fn poly_contain_poly(contains_poly: *mut POLYGON, contained_poly: *mut POLYGON) -> bool {
    let mut i: c_int;
    let mut s: LSEG = std::mem::zeroed();

    Assert!((*contains_poly).npts > 0 && (*contained_poly).npts > 0);

    /*
     * Quick check to see if contained's bounding box is contained in
     * contains' bb.
     */
    if !box_contain_box(&raw mut (*contains_poly).boundbox, &raw mut (*contained_poly).boundbox) {
        return false;
    }

    s.p[0] = (*(*contained_poly)
        .p
        .as_ptr()
        .add(((*contained_poly).npts - 1) as usize))
    .clone();

    i = 0;
    while i < (*contained_poly).npts {
        s.p[1] = (*(*contained_poly).p.as_ptr().add(i as usize)).clone();
        if !lseg_inside_poly(s.p.as_mut_ptr(), s.p.as_mut_ptr().add(1), contains_poly, 0) {
            return false;
        }
        s.p[0] = s.p[1];
        i += 1;
    }

    return true;
}

pub unsafe fn poly_contain(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let result: bool;

    result = poly_contain_poly(polya, polyb);

    /*
     * Avoid leaking memory for toasted inputs ... needed for rtree indexes
     */
    PG_FREE_IF_COPY!(fcinfo, polya, 0);
    PG_FREE_IF_COPY!(fcinfo, polyb, 1);

    PG_RETURN_BOOL!(result);
}

/*-----------------------------------------------------------------
 * Determine if polygon A is contained by polygon B
 *-----------------------------------------------------------------*/
pub unsafe fn poly_contained(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let result: bool;

    /* Just switch the arguments and pass it off to poly_contain */
    result = poly_contain_poly(polyb, polya);

    /*
     * Avoid leaking memory for toasted inputs ... needed for rtree indexes
     */
    PG_FREE_IF_COPY!(fcinfo, polya, 0);
    PG_FREE_IF_COPY!(fcinfo, polyb, 1);

    PG_RETURN_BOOL!(result);
}

pub unsafe fn poly_contain_pt(fcinfo: FunctionCallInfo) -> Datum {
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let p: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(point_inside(p, (*poly).npts, (*poly).p.as_mut_ptr()) != 0);
}

pub unsafe fn pt_contained_poly(fcinfo: FunctionCallInfo) -> Datum {
    let p: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);

    PG_RETURN_BOOL!(point_inside(p, (*poly).npts, (*poly).p.as_mut_ptr()) != 0);
}

pub unsafe fn poly_distance(fcinfo: FunctionCallInfo) -> Datum {
    let polya: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let polyb: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 1);
    let mut min: float8 = 0.0; /* initialize to keep compiler quiet */
    let mut have_min: bool = false;
    let mut tmp: float8;
    let mut i: c_int;
    let mut j: c_int;
    let mut seg1: LSEG = std::mem::zeroed();
    let mut seg2: LSEG = std::mem::zeroed();

    /*
     * Distance is zero if polygons overlap.  We must check this because the
     * path distance will not give the right answer if one poly is entirely
     * within the other.
     */
    if poly_overlap_internal(polya, polyb) {
        PG_RETURN_FLOAT8!(0.0);
    }

    /*
     * When they don't overlap, the distance calculation is identical to that
     * for closed paths (i.e., we needn't care about the fact that polygons
     * include their contained areas).  See path_distance().
     */
    i = 0;
    while i < (*polya).npts {
        let iprev: c_int;

        if i > 0 {
            iprev = i - 1;
        } else {
            iprev = (*polya).npts - 1;
        }

        j = 0;
        while j < (*polyb).npts {
            let jprev: c_int;

            if j > 0 {
                jprev = j - 1;
            } else {
                jprev = (*polyb).npts - 1;
            }

            statlseg_construct(
                &raw mut seg1,
                (*polya).p.as_mut_ptr().add(iprev as usize),
                (*polya).p.as_mut_ptr().add(i as usize),
            );
            statlseg_construct(
                &raw mut seg2,
                (*polyb).p.as_mut_ptr().add(jprev as usize),
                (*polyb).p.as_mut_ptr().add(j as usize),
            );

            tmp = lseg_closept_lseg(null_mut(), &raw mut seg1, &raw mut seg2);
            if !have_min || float8_lt(tmp, min) {
                min = tmp;
                have_min = true;
            }
            j += 1;
        }
        i += 1;
    }

    if !have_min {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_FLOAT8!(min);
}

/***********************************************************************
 **
 **     Routines for 2D points.
 **
 ***********************************************************************/

pub unsafe fn construct_point(fcinfo: FunctionCallInfo) -> Datum {
    let x: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let y: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    point_construct(result, x, y);

    PG_RETURN_POINT_P!(result);
}

#[inline]
unsafe fn point_add_point(result: *mut Point, pt1: *mut Point, pt2: *mut Point) {
    point_construct(
        result,
        float8_pl((*pt1).x, (*pt2).x),
        float8_pl((*pt1).y, (*pt2).y),
    );
}

pub unsafe fn point_add(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let p2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    point_add_point(result, p1, p2);

    PG_RETURN_POINT_P!(result);
}

#[inline]
unsafe fn point_sub_point(result: *mut Point, pt1: *mut Point, pt2: *mut Point) {
    point_construct(
        result,
        float8_mi((*pt1).x, (*pt2).x),
        float8_mi((*pt1).y, (*pt2).y),
    );
}

pub unsafe fn point_sub(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let p2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    point_sub_point(result, p1, p2);

    PG_RETURN_POINT_P!(result);
}

#[inline]
unsafe fn point_mul_point(result: *mut Point, pt1: *mut Point, pt2: *mut Point) {
    point_construct(
        result,
        float8_mi(
            float8_mul((*pt1).x, (*pt2).x),
            float8_mul((*pt1).y, (*pt2).y),
        ),
        float8_pl(
            float8_mul((*pt1).x, (*pt2).y),
            float8_mul((*pt1).y, (*pt2).x),
        ),
    );
}

pub unsafe fn point_mul(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let p2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    point_mul_point(result, p1, p2);

    PG_RETURN_POINT_P!(result);
}

#[inline]
unsafe fn point_div_point(result: *mut Point, pt1: *mut Point, pt2: *mut Point) {
    let div: float8;

    div = float8_pl(
        float8_mul((*pt2).x, (*pt2).x),
        float8_mul((*pt2).y, (*pt2).y),
    );

    point_construct(
        result,
        float8_div(
            float8_pl(
                float8_mul((*pt1).x, (*pt2).x),
                float8_mul((*pt1).y, (*pt2).y),
            ),
            div,
        ),
        float8_div(
            float8_mi(
                float8_mul((*pt1).y, (*pt2).x),
                float8_mul((*pt1).x, (*pt2).y),
            ),
            div,
        ),
    );
}

pub unsafe fn point_div(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let p2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    point_div_point(result, p1, p2);

    PG_RETURN_POINT_P!(result);
}

/***********************************************************************
 **
 **     Routines for 2D boxes.
 **
 ***********************************************************************/

pub unsafe fn points_box(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let p2: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut BOX;

    result = palloc(std::mem::size_of::<BOX>()) as *mut BOX;

    box_construct(result, p1, p2);

    PG_RETURN_BOX_P!(result);
}

pub unsafe fn box_add(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let p: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut BOX;

    result = palloc(std::mem::size_of::<BOX>()) as *mut BOX;

    point_add_point(&raw mut (*result).high, &raw mut (*box_).high, p);
    point_add_point(&raw mut (*result).low, &raw mut (*box_).low, p);

    PG_RETURN_BOX_P!(result);
}

pub unsafe fn box_sub(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let p: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut BOX;

    result = palloc(std::mem::size_of::<BOX>()) as *mut BOX;

    point_sub_point(&raw mut (*result).high, &raw mut (*box_).high, p);
    point_sub_point(&raw mut (*result).low, &raw mut (*box_).low, p);

    PG_RETURN_BOX_P!(result);
}

pub unsafe fn box_mul(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let p: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut BOX;
    let mut high: Point = std::mem::zeroed();
    let mut low: Point = std::mem::zeroed();

    result = palloc(std::mem::size_of::<BOX>()) as *mut BOX;

    point_mul_point(&raw mut high, &raw mut (*box_).high, p);
    point_mul_point(&raw mut low, &raw mut (*box_).low, p);

    box_construct(result, &raw mut high, &raw mut low);

    PG_RETURN_BOX_P!(result);
}

pub unsafe fn box_div(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let p: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut BOX;
    let mut high: Point = std::mem::zeroed();
    let mut low: Point = std::mem::zeroed();

    result = palloc(std::mem::size_of::<BOX>()) as *mut BOX;

    point_div_point(&raw mut high, &raw mut (*box_).high, p);
    point_div_point(&raw mut low, &raw mut (*box_).low, p);

    box_construct(result, &raw mut high, &raw mut low);

    PG_RETURN_BOX_P!(result);
}

/*
 * Convert point to empty box
 */
pub unsafe fn point_box(fcinfo: FunctionCallInfo) -> Datum {
    let pt: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let box_: *mut BOX;

    box_ = palloc(std::mem::size_of::<BOX>()) as *mut BOX;

    (*box_).high.x = (*pt).x;
    (*box_).low.x = (*pt).x;
    (*box_).high.y = (*pt).y;
    (*box_).low.y = (*pt).y;

    PG_RETURN_BOX_P!(box_);
}

/*
 * Smallest bounding box that includes both of the given boxes
 */
pub unsafe fn boxes_bound_box(fcinfo: FunctionCallInfo) -> Datum {
    let box1: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let box2: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 1);
    let container: *mut BOX;

    container = palloc(std::mem::size_of::<BOX>()) as *mut BOX;

    (*container).high.x = float8_max((*box1).high.x, (*box2).high.x);
    (*container).low.x = float8_min((*box1).low.x, (*box2).low.x);
    (*container).high.y = float8_max((*box1).high.y, (*box2).high.y);
    (*container).low.y = float8_min((*box1).low.y, (*box2).low.y);

    PG_RETURN_BOX_P!(container);
}

/***********************************************************************
 **
 **     Routines for 2D paths.
 **
 ***********************************************************************/

/* path_add()
 * Concatenate two paths (only if they are both open).
 */
pub unsafe fn path_add(fcinfo: FunctionCallInfo) -> Datum {
    let p1: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let p2: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 1);
    let result: *mut PATH;
    let size: c_int;
    let base_size: c_int;
    let mut i: c_int;

    if (*p1).closed != 0 || (*p2).closed != 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    base_size = std::mem::size_of::<Point>() as c_int * ((*p1).npts + (*p2).npts);
    size = offsetof_path_p() as c_int + base_size;

    /* Check for integer overflow */
    if base_size / std::mem::size_of::<Point>() as c_int != ((*p1).npts + (*p2).npts)
        || size <= base_size
    {
        ereport!(ERROR, errmsg!("too many points requested"));
    }

    result = palloc(size as Size) as *mut PATH;

    SET_VARSIZE(result as *mut c_char, size);
    (*result).npts = (*p1).npts + (*p2).npts;
    (*result).closed = (*p1).closed;
    /* prevent instability in unused pad bytes */
    (*result).dummy = 0;

    i = 0;
    while i < (*p1).npts {
        (*(*result).p.as_mut_ptr().add(i as usize)).x = (*(*p1).p.as_ptr().add(i as usize)).x;
        (*(*result).p.as_mut_ptr().add(i as usize)).y = (*(*p1).p.as_ptr().add(i as usize)).y;
        i += 1;
    }
    i = 0;
    while i < (*p2).npts {
        (*(*result).p.as_mut_ptr().add((i + (*p1).npts) as usize)).x =
            (*(*p2).p.as_ptr().add(i as usize)).x;
        (*(*result).p.as_mut_ptr().add((i + (*p1).npts) as usize)).y =
            (*(*p2).p.as_ptr().add(i as usize)).y;
        i += 1;
    }

    PG_RETURN_PATH_P!(result);
}

/* path_add_pt()
 * Translation operators.
 */
pub unsafe fn path_add_pt(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P_COPY!(fcinfo, 0);
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let mut i: c_int;

    i = 0;
    while i < (*path).npts {
        point_add_point(
            (*path).p.as_mut_ptr().add(i as usize),
            (*path).p.as_mut_ptr().add(i as usize),
            point,
        );
        i += 1;
    }

    PG_RETURN_PATH_P!(path);
}

pub unsafe fn path_sub_pt(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P_COPY!(fcinfo, 0);
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let mut i: c_int;

    i = 0;
    while i < (*path).npts {
        point_sub_point(
            (*path).p.as_mut_ptr().add(i as usize),
            (*path).p.as_mut_ptr().add(i as usize),
            point,
        );
        i += 1;
    }

    PG_RETURN_PATH_P!(path);
}

/* path_mul_pt()
 * Rotation and scaling operators.
 */
pub unsafe fn path_mul_pt(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P_COPY!(fcinfo, 0);
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let mut i: c_int;

    i = 0;
    while i < (*path).npts {
        point_mul_point(
            (*path).p.as_mut_ptr().add(i as usize),
            (*path).p.as_mut_ptr().add(i as usize),
            point,
        );
        i += 1;
    }

    PG_RETURN_PATH_P!(path);
}

pub unsafe fn path_div_pt(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P_COPY!(fcinfo, 0);
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let mut i: c_int;

    i = 0;
    while i < (*path).npts {
        point_div_point(
            (*path).p.as_mut_ptr().add(i as usize),
            (*path).p.as_mut_ptr().add(i as usize),
            point,
        );
        i += 1;
    }

    PG_RETURN_PATH_P!(path);
}

pub unsafe fn path_poly(fcinfo: FunctionCallInfo) -> Datum {
    let path: *mut PATH = PG_GETARG_PATH_P!(fcinfo, 0);
    let poly: *mut POLYGON;
    let size: c_int;
    let mut i: c_int;

    /* This is not very consistent --- other similar cases return NULL ... */
    if (*path).closed == 0 {
        ereport!(
            ERROR,
            errmsg!("open path cannot be converted to polygon")
        );
    }

    /*
     * Never overflows: the old size fit in MaxAllocSize, and the new size is
     * just a small constant larger.
     */
    size = offsetof_polygon_p() as c_int + std::mem::size_of::<Point>() as c_int * (*path).npts;
    poly = palloc(size as Size) as *mut POLYGON;

    SET_VARSIZE(poly as *mut c_char, size);
    (*poly).npts = (*path).npts;

    i = 0;
    while i < (*path).npts {
        (*(*poly).p.as_mut_ptr().add(i as usize)).x = (*(*path).p.as_ptr().add(i as usize)).x;
        (*(*poly).p.as_mut_ptr().add(i as usize)).y = (*(*path).p.as_ptr().add(i as usize)).y;
        i += 1;
    }

    make_bound_box(poly);

    PG_RETURN_POLYGON_P!(poly);
}

/***********************************************************************
 **
 **     Routines for 2D polygons.
 **
 ***********************************************************************/

pub unsafe fn poly_npoints(fcinfo: FunctionCallInfo) -> Datum {
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);

    PG_RETURN_INT32!((*poly).npts);
}

pub unsafe fn poly_center(fcinfo: FunctionCallInfo) -> Datum {
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let result: *mut Point;
    let mut circle: CIRCLE = std::mem::zeroed();

    result = palloc(std::mem::size_of::<Point>()) as *mut Point;

    poly_to_circle(&raw mut circle, poly);
    *result = circle.center;

    PG_RETURN_POINT_P!(result);
}

pub unsafe fn poly_box(fcinfo: FunctionCallInfo) -> Datum {
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let box_: *mut BOX;

    box_ = palloc(std::mem::size_of::<BOX>()) as *mut BOX;
    *box_ = (*poly).boundbox;

    PG_RETURN_BOX_P!(box_);
}

/* box_poly()
 * Convert a box to a polygon.
 */
pub unsafe fn box_poly(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let poly: *mut POLYGON;
    let size: c_int;

    /* map four corners of the box to a polygon */
    size = offsetof_polygon_p() as c_int + std::mem::size_of::<Point>() as c_int * 4;
    poly = palloc(size as Size) as *mut POLYGON;

    SET_VARSIZE(poly as *mut c_char, size);
    (*poly).npts = 4;

    (*(*poly).p.as_mut_ptr().add(0)).x = (*box_).low.x;
    (*(*poly).p.as_mut_ptr().add(0)).y = (*box_).low.y;
    (*(*poly).p.as_mut_ptr().add(1)).x = (*box_).low.x;
    (*(*poly).p.as_mut_ptr().add(1)).y = (*box_).high.y;
    (*(*poly).p.as_mut_ptr().add(2)).x = (*box_).high.x;
    (*(*poly).p.as_mut_ptr().add(2)).y = (*box_).high.y;
    (*(*poly).p.as_mut_ptr().add(3)).x = (*box_).high.x;
    (*(*poly).p.as_mut_ptr().add(3)).y = (*box_).low.y;

    box_construct(&raw mut (*poly).boundbox, &raw mut (*box_).high, &raw mut (*box_).low);

    PG_RETURN_POLYGON_P!(poly);
}

pub unsafe fn poly_path(fcinfo: FunctionCallInfo) -> Datum {
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let path: *mut PATH;
    let size: c_int;
    let mut i: c_int;

    /*
     * Never overflows: the old size fit in MaxAllocSize, and the new size is
     * smaller by a small constant.
     */
    size = offsetof_path_p() as c_int + std::mem::size_of::<Point>() as c_int * (*poly).npts;
    path = palloc(size as Size) as *mut PATH;

    SET_VARSIZE(path as *mut c_char, size);
    (*path).npts = (*poly).npts;
    (*path).closed = true as int32;
    /* prevent instability in unused pad bytes */
    (*path).dummy = 0;

    i = 0;
    while i < (*poly).npts {
        (*(*path).p.as_mut_ptr().add(i as usize)).x = (*(*poly).p.as_ptr().add(i as usize)).x;
        (*(*path).p.as_mut_ptr().add(i as usize)).y = (*(*poly).p.as_ptr().add(i as usize)).y;
        i += 1;
    }

    PG_RETURN_PATH_P!(path);
}

/***********************************************************************
 **
 **		Routines for circles.
 **
 ***********************************************************************/

/*----------------------------------------------------------
 * Formatting and conversion routines.
 *---------------------------------------------------------*/

/*		circle_in		-		convert a string to internal form.
 *
 *		External format: (center and radius of circle)
 *				"<(f8,f8),f8>"
 *				also supports quick entry style "f8,f8,f8"
 */
pub unsafe fn circle_in(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let circle: *mut CIRCLE = palloc(std::mem::size_of::<CIRCLE>() as Size) as *mut CIRCLE;
    let mut s: *mut c_char;
    let cp: *mut c_char;
    let mut depth: c_int = 0;

    s = str;
    while isspace(*s) {
        s = s.add(1);
    }
    if *s == LDELIM_C {
        depth += 1;
        s = s.add(1);
    } else if *s == LDELIM {
        /* If there are two left parens, consume the first one */
        cp = s.add(1);
        let mut cpw = cp;
        while isspace(*cpw) {
            cpw = cpw.add(1);
        }
        if *cpw == LDELIM {
            depth += 1;
            s = cpw;
        }
    }

    /* pair_decode will consume parens around the pair, if any */
    if !pair_decode(
        s,
        &raw mut (*circle).center.x,
        &raw mut (*circle).center.y,
        &raw mut s,
        c"circle".as_ptr(),
        str,
        escontext,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }

    if *s == DELIM {
        s = s.add(1);
    }

    if !single_decode(s, &raw mut (*circle).radius, &raw mut s, c"circle".as_ptr(), str, escontext) {
        PG_RETURN_NULL!(fcinfo);
    }

    /* We have to accept NaN. */
    if (*circle).radius < 0.0 {
        ereturn!(
            escontext,
            0 as Datum,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                "circle",
                std::ffi::CStr::from_ptr(str).to_string_lossy()
            )
        );
    }

    while depth > 0 {
        if (*s == RDELIM) || ((*s == RDELIM_C) && (depth == 1)) {
            depth -= 1;
            s = s.add(1);
            while isspace(*s) {
                s = s.add(1);
            }
        } else {
            ereturn!(
                escontext,
                0 as Datum,
                errmsg!(
                    "invalid input syntax for type {}: \"{}\"",
                    "circle",
                    std::ffi::CStr::from_ptr(str).to_string_lossy()
                )
            );
        }
    }

    if *s != 0 {
        ereturn!(
            escontext,
            0 as Datum,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                "circle",
                std::ffi::CStr::from_ptr(str).to_string_lossy()
            )
        );
    }

    PG_RETURN_CIRCLE_P!(circle);
}

/*		circle_out		-		convert a circle to external form.
 */
pub unsafe fn circle_out(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let mut str: StringInfoData = std::mem::zeroed();

    initStringInfo(&raw mut str);

    appendStringInfoChar(&raw mut str, LDELIM_C);
    appendStringInfoChar(&raw mut str, LDELIM);
    pair_encode((*circle).center.x, (*circle).center.y, &raw mut str);
    appendStringInfoChar(&raw mut str, RDELIM);
    appendStringInfoChar(&raw mut str, DELIM);
    single_encode((*circle).radius, &raw mut str);
    appendStringInfoChar(&raw mut str, RDELIM_C);

    PG_RETURN_CSTRING!(str.data);
}

/*
 *		circle_recv			- converts external binary format to circle
 */
pub unsafe fn circle_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let circle: *mut CIRCLE;

    circle = palloc(std::mem::size_of::<CIRCLE>() as Size) as *mut CIRCLE;

    (*circle).center.x = pq_getmsgfloat8(buf);
    (*circle).center.y = pq_getmsgfloat8(buf);
    (*circle).radius = pq_getmsgfloat8(buf);

    /* We have to accept NaN. */
    if (*circle).radius < 0.0 {
        ereport!(
            ERROR,
            errmsg!("invalid radius in external \"circle\" value")
        );
    }

    PG_RETURN_CIRCLE_P!(circle);
}

/*
 *		circle_send			- converts circle to binary format
 */
pub unsafe fn circle_send(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_begintypsend(&raw mut buf);
    pq_sendfloat8(&raw mut buf, (*circle).center.x);
    pq_sendfloat8(&raw mut buf, (*circle).center.y);
    pq_sendfloat8(&raw mut buf, (*circle).radius);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&raw mut buf));
}


/*----------------------------------------------------------
 *	Relational operators for CIRCLEs.
 *		<, >, <=, >=, and == are based on circle area.
 *---------------------------------------------------------*/

/*		circles identical?
 *
 * We consider NaNs values to be equal to each other to let those circles
 * to be found.
 */
pub unsafe fn circle_same(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(
        (((*circle1).radius.is_nan() && (*circle2).radius.is_nan())
            || FPeq((*circle1).radius, (*circle2).radius))
            && point_eq_point(&raw mut (*circle1).center, &raw mut (*circle2).center)
    );
}

/*		circle_overlap	-		does circle1 overlap circle2?
 */
pub unsafe fn circle_overlap(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPle(
        point_dt(&raw mut (*circle1).center, &raw mut (*circle2).center),
        float8_pl((*circle1).radius, (*circle2).radius)
    ));
}

/*		circle_overleft -		is the right edge of circle1 at or left of
 *								the right edge of circle2?
 */
pub unsafe fn circle_overleft(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPle(
        float8_pl((*circle1).center.x, (*circle1).radius),
        float8_pl((*circle2).center.x, (*circle2).radius)
    ));
}

/*		circle_left		-		is circle1 strictly left of circle2?
 */
pub unsafe fn circle_left(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPlt(
        float8_pl((*circle1).center.x, (*circle1).radius),
        float8_mi((*circle2).center.x, (*circle2).radius)
    ));
}

/*		circle_right	-		is circle1 strictly right of circle2?
 */
pub unsafe fn circle_right(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPgt(
        float8_mi((*circle1).center.x, (*circle1).radius),
        float8_pl((*circle2).center.x, (*circle2).radius)
    ));
}

/*		circle_overright	-	is the left edge of circle1 at or right of
 *								the left edge of circle2?
 */
pub unsafe fn circle_overright(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPge(
        float8_mi((*circle1).center.x, (*circle1).radius),
        float8_mi((*circle2).center.x, (*circle2).radius)
    ));
}

/*		circle_contained		-		is circle1 contained by circle2?
 */
pub unsafe fn circle_contained(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPle(
        point_dt(&raw mut (*circle1).center, &raw mut (*circle2).center),
        float8_mi((*circle2).radius, (*circle1).radius)
    ));
}

/*		circle_contain	-		does circle1 contain circle2?
 */
pub unsafe fn circle_contain(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPle(
        point_dt(&raw mut (*circle1).center, &raw mut (*circle2).center),
        float8_mi((*circle1).radius, (*circle2).radius)
    ));
}


/*		circle_below		-		is circle1 strictly below circle2?
 */
pub unsafe fn circle_below(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPlt(
        float8_pl((*circle1).center.y, (*circle1).radius),
        float8_mi((*circle2).center.y, (*circle2).radius)
    ));
}

/*		circle_above	-		is circle1 strictly above circle2?
 */
pub unsafe fn circle_above(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPgt(
        float8_mi((*circle1).center.y, (*circle1).radius),
        float8_pl((*circle2).center.y, (*circle2).radius)
    ));
}

/*		circle_overbelow -		is the upper edge of circle1 at or below
 *								the upper edge of circle2?
 */
pub unsafe fn circle_overbelow(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPle(
        float8_pl((*circle1).center.y, (*circle1).radius),
        float8_pl((*circle2).center.y, (*circle2).radius)
    ));
}

/*		circle_overabove	-	is the lower edge of circle1 at or above
 *								the lower edge of circle2?
 */
pub unsafe fn circle_overabove(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPge(
        float8_mi((*circle1).center.y, (*circle1).radius),
        float8_mi((*circle2).center.y, (*circle2).radius)
    ));
}


/*		circle_relop	-		is area(circle1) relop area(circle2), within
 *								our accuracy constraint?
 */
pub unsafe fn circle_eq(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPeq(circle_ar(circle1), circle_ar(circle2)));
}

pub unsafe fn circle_ne(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPne(circle_ar(circle1), circle_ar(circle2)));
}

pub unsafe fn circle_lt(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPlt(circle_ar(circle1), circle_ar(circle2)));
}

pub unsafe fn circle_gt(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPgt(circle_ar(circle1), circle_ar(circle2)));
}

pub unsafe fn circle_le(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPle(circle_ar(circle1), circle_ar(circle2)));
}

pub unsafe fn circle_ge(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);

    PG_RETURN_BOOL!(FPge(circle_ar(circle1), circle_ar(circle2)));
}


/*----------------------------------------------------------
 *	"Arithmetic" operators on circles.
 *---------------------------------------------------------*/

/* circle_add_pt()
 * Translation operator.
 */
pub unsafe fn circle_add_pt(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut CIRCLE;

    result = palloc(std::mem::size_of::<CIRCLE>() as Size) as *mut CIRCLE;

    point_add_point(&raw mut (*result).center, &raw mut (*circle).center, point);
    (*result).radius = (*circle).radius;

    PG_RETURN_CIRCLE_P!(result);
}

pub unsafe fn circle_sub_pt(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut CIRCLE;

    result = palloc(std::mem::size_of::<CIRCLE>() as Size) as *mut CIRCLE;

    point_sub_point(&raw mut (*result).center, &raw mut (*circle).center, point);
    (*result).radius = (*circle).radius;

    PG_RETURN_CIRCLE_P!(result);
}


/* circle_mul_pt()
 * Rotation and scaling operators.
 */
pub unsafe fn circle_mul_pt(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut CIRCLE;

    result = palloc(std::mem::size_of::<CIRCLE>() as Size) as *mut CIRCLE;

    point_mul_point(&raw mut (*result).center, &raw mut (*circle).center, point);
    (*result).radius = float8_mul((*circle).radius, HYPOT((*point).x, (*point).y));

    PG_RETURN_CIRCLE_P!(result);
}

pub unsafe fn circle_div_pt(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let result: *mut CIRCLE;

    result = palloc(std::mem::size_of::<CIRCLE>() as Size) as *mut CIRCLE;

    point_div_point(&raw mut (*result).center, &raw mut (*circle).center, point);
    (*result).radius = float8_div((*circle).radius, HYPOT((*point).x, (*point).y));

    PG_RETURN_CIRCLE_P!(result);
}


/*		circle_area		-		returns the area of the circle.
 */
pub unsafe fn circle_area(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);

    PG_RETURN_FLOAT8!(circle_ar(circle));
}


/*		circle_diameter -		returns the diameter of the circle.
 */
pub unsafe fn circle_diameter(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);

    PG_RETURN_FLOAT8!(float8_mul((*circle).radius, 2.0));
}


/*		circle_radius	-		returns the radius of the circle.
 */
pub unsafe fn circle_radius(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);

    PG_RETURN_FLOAT8!((*circle).radius);
}


/*		circle_distance -		returns the distance between
 *								  two circles.
 */
pub unsafe fn circle_distance(fcinfo: FunctionCallInfo) -> Datum {
    let circle1: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let circle2: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);
    let mut result: float8;

    result = float8_mi(
        point_dt(&raw mut (*circle1).center, &raw mut (*circle2).center),
        float8_pl((*circle1).radius, (*circle2).radius),
    );
    if result < 0.0 {
        result = 0.0;
    }

    PG_RETURN_FLOAT8!(result);
}


pub unsafe fn circle_contain_pt(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let d: float8;

    d = point_dt(&raw mut (*circle).center, point);
    PG_RETURN_BOOL!(d <= (*circle).radius);
}


pub unsafe fn pt_contained_circle(fcinfo: FunctionCallInfo) -> Datum {
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);
    let d: float8;

    d = point_dt(&raw mut (*circle).center, point);
    PG_RETURN_BOOL!(d <= (*circle).radius);
}


/*		dist_pc -		returns the distance between
 *						  a point and a circle.
 */
pub unsafe fn dist_pc(fcinfo: FunctionCallInfo) -> Datum {
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);
    let mut result: float8;

    result = float8_mi(point_dt(point, &raw mut (*circle).center), (*circle).radius);
    if result < 0.0 {
        result = 0.0;
    }

    PG_RETURN_FLOAT8!(result);
}

/*
 * Distance from a circle to a point
 */
pub unsafe fn dist_cpoint(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let point: *mut Point = PG_GETARG_POINT_P!(fcinfo, 1);
    let mut result: float8;

    result = float8_mi(point_dt(point, &raw mut (*circle).center), (*circle).radius);
    if result < 0.0 {
        result = 0.0;
    }

    PG_RETURN_FLOAT8!(result);
}

/*		circle_center	-		returns the center point of the circle.
 */
pub unsafe fn circle_center(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let result: *mut Point;

    result = palloc(std::mem::size_of::<Point>() as Size) as *mut Point;
    (*result).x = (*circle).center.x;
    (*result).y = (*circle).center.y;

    PG_RETURN_POINT_P!(result);
}


/*		circle_ar		-		returns the area of the circle.
 */
unsafe fn circle_ar(circle: *mut CIRCLE) -> float8 {
    return float8_mul(float8_mul((*circle).radius, (*circle).radius), M_PI);
}


/*----------------------------------------------------------
 *	Conversion operators.
 *---------------------------------------------------------*/

pub unsafe fn cr_circle(fcinfo: FunctionCallInfo) -> Datum {
    let center: *mut Point = PG_GETARG_POINT_P!(fcinfo, 0);
    let radius: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);
    let result: *mut CIRCLE;

    result = palloc(std::mem::size_of::<CIRCLE>() as Size) as *mut CIRCLE;

    (*result).center.x = (*center).x;
    (*result).center.y = (*center).y;
    (*result).radius = radius;

    PG_RETURN_CIRCLE_P!(result);
}

pub unsafe fn circle_box(fcinfo: FunctionCallInfo) -> Datum {
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 0);
    let box_: *mut BOX;
    let delta: float8;

    box_ = palloc(std::mem::size_of::<BOX>() as Size) as *mut BOX;

    delta = float8_div((*circle).radius, sqrt(2.0));

    (*box_).high.x = float8_pl((*circle).center.x, delta);
    (*box_).low.x = float8_mi((*circle).center.x, delta);
    (*box_).high.y = float8_pl((*circle).center.y, delta);
    (*box_).low.y = float8_mi((*circle).center.y, delta);

    PG_RETURN_BOX_P!(box_);
}

/* box_circle()
 * Convert a box to a circle.
 */
pub unsafe fn box_circle(fcinfo: FunctionCallInfo) -> Datum {
    let box_: *mut BOX = PG_GETARG_BOX_P!(fcinfo, 0);
    let circle: *mut CIRCLE;

    circle = palloc(std::mem::size_of::<CIRCLE>() as Size) as *mut CIRCLE;

    (*circle).center.x = float8_div(float8_pl((*box_).high.x, (*box_).low.x), 2.0);
    (*circle).center.y = float8_div(float8_pl((*box_).high.y, (*box_).low.y), 2.0);

    (*circle).radius = point_dt(&raw mut (*circle).center, &raw mut (*box_).high);

    PG_RETURN_CIRCLE_P!(circle);
}


pub unsafe fn circle_poly(fcinfo: FunctionCallInfo) -> Datum {
    let npts: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let circle: *mut CIRCLE = PG_GETARG_CIRCLE_P!(fcinfo, 1);
    let poly: *mut POLYGON;
    let base_size: c_int;
    let size: c_int;
    let mut i: c_int;
    let mut angle: float8;
    let anglestep: float8;

    if FPzero((*circle).radius) {
        ereport!(
            ERROR,
            errmsg!("cannot convert circle with radius zero to polygon")
        );
    }

    if npts < 2 {
        ereport!(
            ERROR,
            errmsg!("must request at least 2 points")
        );
    }

    base_size = std::mem::size_of::<Point>() as c_int * npts;
    size = offsetof_polygon_p() as c_int + base_size;

    /* Check for integer overflow */
    if base_size / npts != std::mem::size_of::<Point>() as c_int || size <= base_size {
        ereport!(
            ERROR,
            errmsg!("too many points requested")
        );
    }

    poly = palloc0(size as Size) as *mut POLYGON; /* zero any holes */
    SET_VARSIZE(poly as *mut c_char, size);
    (*poly).npts = npts;

    anglestep = float8_div(2.0 * M_PI, npts as float8);

    i = 0;
    while i < npts {
        angle = float8_mul(anglestep, i as float8);

        (*(*poly).p.as_mut_ptr().add(i as usize)).x =
            float8_mi((*circle).center.x, float8_mul((*circle).radius, cos(angle)));
        (*(*poly).p.as_mut_ptr().add(i as usize)).y =
            float8_pl((*circle).center.y, float8_mul((*circle).radius, sin(angle)));
        i += 1;
    }

    make_bound_box(poly);

    PG_RETURN_POLYGON_P!(poly);
}

/*
 * Convert polygon to circle
 *
 * The result must be preallocated.
 *
 * XXX This algorithm should use weighted means of line segments
 *	rather than straight average values of points - tgl 97/01/21.
 */
unsafe fn poly_to_circle(result: *mut CIRCLE, poly: *mut POLYGON) {
    let mut i: c_int;

    Assert!((*poly).npts > 0);

    (*result).center.x = 0.0;
    (*result).center.y = 0.0;
    (*result).radius = 0.0;

    i = 0;
    while i < (*poly).npts {
        point_add_point(
            &raw mut (*result).center,
            &raw mut (*result).center,
            (*poly).p.as_mut_ptr().add(i as usize),
        );
        i += 1;
    }
    (*result).center.x = float8_div((*result).center.x, (*poly).npts as float8);
    (*result).center.y = float8_div((*result).center.y, (*poly).npts as float8);

    i = 0;
    while i < (*poly).npts {
        (*result).radius = float8_pl(
            (*result).radius,
            point_dt((*poly).p.as_mut_ptr().add(i as usize), &raw mut (*result).center),
        );
        i += 1;
    }
    (*result).radius = float8_div((*result).radius, (*poly).npts as float8);
}

pub unsafe fn poly_circle(fcinfo: FunctionCallInfo) -> Datum {
    let poly: *mut POLYGON = PG_GETARG_POLYGON_P!(fcinfo, 0);
    let result: *mut CIRCLE;

    result = palloc(std::mem::size_of::<CIRCLE>() as Size) as *mut CIRCLE;

    poly_to_circle(result, poly);

    PG_RETURN_CIRCLE_P!(result);
}


/***********************************************************************
 **
 **		Private routines for multiple types.
 **
 ***********************************************************************/

/*
 *	Test to see if the point is inside the polygon, returns 1/0, or 2 if
 *	the point is on the polygon.
 *	Code adapted but not copied from integer-based routines in WN: A
 *	Server for the HTTP
 *	version 1.15.1, file wn/image.c
 *	http://hopf.math.northwestern.edu/index.html
 *	Description of algorithm:  http://www.linuxjournal.com/article/2197
 *							   http://www.linuxjournal.com/article/2029
 */

const POINT_ON_POLYGON: c_int = INT_MAX;

unsafe fn point_inside(p: *mut Point, npts: c_int, plist: *mut Point) -> c_int {
    let x0: float8;
    let y0: float8;
    let mut prev_x: float8;
    let mut prev_y: float8;
    let mut i: c_int = 0;
    let mut x: float8;
    let mut y: float8;
    let mut cross: c_int;
    let mut total_cross: c_int = 0;

    Assert!(npts > 0);

    /* compute first polygon point relative to single point */
    x0 = float8_mi((*plist.add(0)).x, (*p).x);
    y0 = float8_mi((*plist.add(0)).y, (*p).y);

    prev_x = x0;
    prev_y = y0;
    /* loop over polygon points and aggregate total_cross */
    i = 1;
    while i < npts {
        /* compute next polygon point relative to single point */
        x = float8_mi((*plist.add(i as usize)).x, (*p).x);
        y = float8_mi((*plist.add(i as usize)).y, (*p).y);

        /* compute previous to current point crossing */
        cross = lseg_crossing(x, y, prev_x, prev_y);
        if cross == POINT_ON_POLYGON {
            return 2;
        }
        total_cross += cross;

        prev_x = x;
        prev_y = y;
        i += 1;
    }

    /* now do the first point */
    cross = lseg_crossing(x0, y0, prev_x, prev_y);
    if cross == POINT_ON_POLYGON {
        return 2;
    }
    total_cross += cross;

    if total_cross != 0 {
        return 1;
    }
    return 0;
}


/* lseg_crossing()
 * Returns +/-2 if line segment crosses the positive X-axis in a +/- direction.
 * Returns +/-1 if one point is on the positive X-axis.
 * Returns 0 if both points are on the positive X-axis, or there is no crossing.
 * Returns POINT_ON_POLYGON if the segment contains (0,0).
 * Wow, that is one confusing API, but it is used above, and when summed,
 * can tell is if a point is in a polygon.
 */

unsafe fn lseg_crossing(x: float8, y: float8, prev_x: float8, prev_y: float8) -> c_int {
    let z: float8;
    let y_sign: c_int;

    if FPzero(y) {
        /* y == 0, on X axis */
        if FPzero(x) {
            /* (x,y) is (0,0)? */
            return POINT_ON_POLYGON;
        } else if FPgt(x, 0.0) {
            /* x > 0 */
            if FPzero(prev_y) {
                /* y and prev_y are zero */
                /* prev_x > 0? */
                return if FPgt(prev_x, 0.0) { 0 } else { POINT_ON_POLYGON };
            }
            return if FPlt(prev_y, 0.0) { 1 } else { -1 };
        } else {
            /* x < 0, x not on positive X axis */
            if FPzero(prev_y) {
                /* prev_x < 0? */
                return if FPlt(prev_x, 0.0) { 0 } else { POINT_ON_POLYGON };
            }
            return 0;
        }
    } else {
        /* y != 0 */
        /* compute y crossing direction from previous point */
        y_sign = if FPgt(y, 0.0) { 1 } else { -1 };

        if FPzero(prev_y) {
            /* previous point was on X axis, so new point is either off or on */
            return if FPlt(prev_x, 0.0) { 0 } else { y_sign };
        } else if (y_sign < 0 && FPlt(prev_y, 0.0)) || (y_sign > 0 && FPgt(prev_y, 0.0)) {
            /* both above or below X axis */
            return 0; /* same sign */
        } else {
            /* y and prev_y cross X-axis */
            if FPge(x, 0.0) && FPgt(prev_x, 0.0) {
                /* both non-negative so cross positive X-axis */
                return 2 * y_sign;
            }
            if FPlt(x, 0.0) && FPle(prev_x, 0.0) {
                /* both non-positive so do not cross positive X-axis */
                return 0;
            }

            /* x and y cross axes, see URL above point_inside() */
            z = float8_mi(
                float8_mul(float8_mi(x, prev_x), y),
                float8_mul(float8_mi(y, prev_y), x),
            );
            if FPzero(z) {
                return POINT_ON_POLYGON;
            }
            if (y_sign < 0 && FPlt(z, 0.0)) || (y_sign > 0 && FPgt(z, 0.0)) {
                return 0;
            }
            return 2 * y_sign;
        }
    }
}


unsafe fn plist_same(npts: c_int, p1: *mut Point, p2: *mut Point) -> bool {
    let mut i: c_int;
    let mut ii: c_int;
    let mut j: c_int;

    /* find match for first point */
    i = 0;
    while i < npts {
        if point_eq_point(p2.add(i as usize), p1.add(0)) {
            /* match found? then look forward through remaining points */
            ii = 1;
            j = i + 1;
            while ii < npts {
                if j >= npts {
                    j = 0;
                }
                if !point_eq_point(p2.add(j as usize), p1.add(ii as usize)) {
                    break;
                }
                ii += 1;
                j += 1;
            }
            if ii == npts {
                return true;
            }

            /* match not found forwards? then look backwards */
            ii = 1;
            j = i - 1;
            while ii < npts {
                if j < 0 {
                    j = npts - 1;
                }
                if !point_eq_point(p2.add(j as usize), p1.add(ii as usize)) {
                    break;
                }
                ii += 1;
                j -= 1;
            }
            if ii == npts {
                return true;
            }
        }
        i += 1;
    }

    return false;
}


/*-------------------------------------------------------------------------
 * Determine the hypotenuse.
 *
 * If required, x and y are swapped to make x the larger number. The
 * traditional formula of x^2+y^2 is rearranged to factor x outside the
 * sqrt. This allows computation of the hypotenuse for significantly
 * larger values, and with a higher precision than when using the naive
 * formula.  In particular, this cannot overflow unless the final result
 * would be out-of-range.
 *
 * sqrt( x^2 + y^2 ) = sqrt( x^2( 1 + y^2/x^2) )
 *					 = x * sqrt( 1 + y^2/x^2 )
 *					 = x * sqrt( 1 + y/x * y/x )
 *
 * It is expected that this routine will eventually be replaced with the
 * C99 hypot() function.
 *
 * This implementation conforms to IEEE Std 1003.1 and GLIBC, in that the
 * case of hypot(inf,nan) results in INF, and not NAN.
 *-----------------------------------------------------------------------
 */
pub unsafe fn pg_hypot(mut x: float8, mut y: float8) -> float8 {
    let yx: float8;
    let result: float8;

    /* Handle INF and NaN properly */
    if x.is_infinite() || y.is_infinite() {
        return get_float8_infinity();
    }

    if x.is_nan() || y.is_nan() {
        return get_float8_nan();
    }

    /* Else, drop any minus signs */
    x = fabs(x);
    y = fabs(y);

    /* Swap x and y if needed to make x the larger one */
    if x < y {
        let temp: float8 = x;

        x = y;
        y = temp;
    }

    /*
     * If y is zero, the hypotenuse is x.  This test saves a few cycles in
     * such cases, but more importantly it also protects against
     * divide-by-zero errors, since now x >= y.
     */
    if y == 0.0 {
        return x;
    }

    /* Determine the hypotenuse */
    yx = y / x;
    result = x * sqrt(1.0 + (yx * yx));

    if result.is_infinite() {
        float_overflow_error();
    }
    if result == 0.0 {
        float_underflow_error();
    }

    return result;
}
