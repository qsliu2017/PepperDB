//! jsonpath.rs
//!   Input/output and supporting routines for jsonpath
//!
//! jsonpath expression is a chain of path items.  First path item is $, $var,
//! literal or arithmetic expression.  Subsequent path items are accessors
//! (.key, .*, [subscripts], [*]), filters (? (predicate)) and methods (.type(),
//! .size() etc).
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/jsonpath.c
//!
//! Copyright (c) 2019-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/jsonpath.c

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(unused_variables)]
#![allow(unused_assignments)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unreachable_code)]
#![allow(unreachable_patterns)]
#![allow(clippy::all)]

use crate::prelude::*;
use crate::{PG_GETARG_CSTRING, PG_GETARG_POINTER, PG_GETARG_DATUM,
    PG_RETURN_CSTRING, PG_RETURN_BYTEA_P};
use core::ffi::{c_char, c_int, c_void, CStr};
use core::ptr;

use crate::postgres_ext::{Oid, InvalidOid};
use crate::c::{int32, uint32, Size, INTALIGN, PG_UINT32_MAX};
use crate::postgres::{Datum, PointerGetDatum, DatumGetPointer, DatumGetCString};
use crate::varatt::{VARSIZE, SET_VARSIZE};

use crate::utils::adt::numeric::{Numeric, NumericData, NumericGetDatum, numeric_out};

use crate::utils::adt::jsonb_gin::{
    JsonPath, JsonPathItem, JsonPathItemType, JsonPathItemContent,
    JsonPathItemArgs, JsonPathItemArrayElems, JsonPathItemArray,
    JsonPathItemAnyBounds, JsonPathItemValue, JsonPathItemLikeRegex,
    JSONPATH_LAX,
};
pub use crate::utils::adt::jsonb_gin::JsonPathItemType::*;

use crate::utils::adt::json::escape_json_with_len;
use crate::utils::adt::formatting::datetime_format_has_tz;

use crate::lib::stringinfo::{
    StringInfo, StringInfoData,
    initStringInfo, enlargeStringInfo, appendStringInfoChar, appendStringInfoString,
    appendStringInfoSpaces, appendBinaryStringInfo,
};
use crate::{appendStringInfo, appendStringInfoCharMacro};
use crate::libpq::pqformat::{
    pq_getmsgint, pq_getmsgtext, pq_begintypsend, pq_sendint8, pq_sendtext,
    pq_endtypsend,
};
use crate::utils::mmgr::mcxt::pfree;

use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::value::String as PgString;
use crate::nodes::pg_list::{List, ListCell, lfirst};
use crate::nodes::nodeFuncs::exprType;
use crate::{DirectFunctionCall1, forboth, lfirst_node};

use crate::catalog::pg_type_d::{DATEOID, TIMEOID, TIMESTAMPOID, TIMETZOID, TIMESTAMPTZOID};

use crate::miscadmin::{check_stack_depth, CHECK_FOR_INTERRUPTS};

// ---------------------------------------------------------------------------
// Constants and macros from utils/jsonpath.h
// ---------------------------------------------------------------------------

pub const JSONPATH_VERSION: uint32 = 0x01;

// JSONPATH_HDRSZ = offsetof(JsonPath, data); vl_len_ (int32) + header (uint32)
pub const JSONPATH_HDRSZ: c_int = core::mem::offset_of!(JsonPath, data) as c_int;

pub const JSP_REGEX_ICASE: uint32 = 0x01; /* i flag, case insensitive */
pub const JSP_REGEX_DOTALL: uint32 = 0x02; /* s flag, dot matches newline */
pub const JSP_REGEX_MLINE: uint32 = 0x04; /* m flag, ^/$ match at newlines */
pub const JSP_REGEX_WSPACE: uint32 = 0x08; /* x flag, ignore whitespace in pattern */
pub const JSP_REGEX_QUOTE: uint32 = 0x10; /* q flag, no special characters */

// jspHasNext(jsp) ((jsp)->nextPos > 0)
#[inline]
pub unsafe fn jspHasNext(jsp: *mut JsonPathItem) -> bool {
    (*jsp).nextPos > 0
}

/* PG_GETARG_JSONPATH_P(x): DatumGetJsonPathP(PG_GETARG_DATUM(x)) (jsonpath.h) */
macro_rules! PG_GETARG_JSONPATH_P {
    ($fcinfo:expr, $n:expr) => {
        // TODO(pg-port): real DatumGetJsonPathP detoasts the varlena.
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut JsonPath
    };
}

/* PG_RETURN_JSONPATH_P(p): PG_RETURN_POINTER(p) (jsonpath.h) */
macro_rules! PG_RETURN_JSONPATH_P {
    ($x:expr) => {
        return PointerGetDatum($x as *const c_void)
    };
}

// ---------------------------------------------------------------------------
// Parsing support data structures (utils/jsonpath.h)
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct JsonPathParseItem {
    pub type_: JsonPathItemType,
    pub next: *mut JsonPathParseItem, /* next in path */
    pub value: JsonPathParseItemValue,
}

#[repr(C)]
pub union JsonPathParseItemValue {
    pub args: JsonPathParseItemArgs,
    pub arg: *mut JsonPathParseItem,
    pub array: JsonPathParseItemArray,
    pub anybounds: JsonPathParseItemAnyBounds,
    pub like_regex: JsonPathParseItemLikeRegex,
    pub numeric: Numeric,
    pub boolean: bool,
    pub string: JsonPathParseItemString,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathParseItemArgs {
    pub left: *mut JsonPathParseItem,
    pub right: *mut JsonPathParseItem,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathParseItemArrayElem {
    pub from: *mut JsonPathParseItem,
    pub to: *mut JsonPathParseItem,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathParseItemArray {
    pub nelems: c_int,
    pub elems: *mut JsonPathParseItemArrayElem,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathParseItemAnyBounds {
    pub first: uint32,
    pub last: uint32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathParseItemLikeRegex {
    pub expr: *mut JsonPathParseItem,
    pub pattern: *mut c_char, /* could not be not null-terminated */
    pub patternlen: uint32,
    pub flags: uint32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathParseItemString {
    pub len: uint32,
    pub val: *mut c_char, /* could not be not null-terminated */
}

#[repr(C)]
pub struct JsonPathParseResult {
    pub expr: *mut JsonPathParseItem,
    pub lax: bool,
}

// ---------------------------------------------------------------------------
// Local ereturn / SOFT_ERROR shim (escontext soft-error path; elog shim
// ignores escontext, matching sibling jsonfuncs.rs).
// ---------------------------------------------------------------------------
macro_rules! ereturn {
    ($escontext:expr, $dummy:expr, $($arg:tt)*) => {{
        let _ = &$escontext;
        $crate::ereport!(ERROR, $($arg)*);
        return $dummy;
    }};
}
macro_rules! SOFT_ERROR_OCCURRED {
    ($escontext:expr) => {{
        // TODO(pg-port): real SOFT_ERROR_OCCURRED checks ErrorSaveContext->error_occurred.
        let _ = &$escontext;
        false
    }};
}

// ---------------------------------------------------------------------------
// Dependencies that live in OTHER .c files, not yet ported.  Stubbed with
// TODO(pg-port) bodies.
// ---------------------------------------------------------------------------

// parsejsonpath(): jsonpath_scan.l / jsonpath_gram.y (Bison/flex generated).
unsafe fn parsejsonpath(str_: *const c_char, len: c_int,
                        escontext: *mut Node) -> *mut JsonPathParseResult {
    // TODO(pg-port): real parser lives in jsonpath_scan.l + jsonpath_gram.y.
    let _ = (str_, len, escontext);
    ptr::null_mut()
}

/**************************** INPUT/OUTPUT ********************************/

/*
 * jsonpath type input function
 */
pub unsafe fn jsonpath_in(fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum {
    let in_: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let len: c_int = libc::strlen(in_) as c_int;

    return jsonPathFromCstring(in_, len, (*fcinfo).context);
}

/*
 * jsonpath type recv function
 *
 * The type is sent as text in binary mode, so this is almost the same
 * as the input function, but it's prefixed with a version number so we
 * can change the binary format sent in future if necessary. For now,
 * only version 1 is supported.
 */
pub unsafe fn jsonpath_recv(fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let version: c_int = pq_getmsgint(buf, 1) as c_int;
    let mut str: *mut c_char;
    let mut nbytes: c_int = 0;

    if version as uint32 == JSONPATH_VERSION {
        str = pq_getmsgtext(buf, (*buf).len - (*buf).cursor, &mut nbytes);
    } else {
        elog!(ERROR, "unsupported jsonpath version number: {}", version);
        str = ptr::null_mut();
    }

    return jsonPathFromCstring(str, nbytes, ptr::null_mut());
}

/*
 * jsonpath type output function
 */
pub unsafe fn jsonpath_out(fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum {
    let in_: *mut JsonPath = PG_GETARG_JSONPATH_P!(fcinfo, 0);

    PG_RETURN_CSTRING!(jsonPathToCstring(ptr::null_mut(), in_,
                                         VARSIZE(in_ as *const c_char) as c_int));
}

/*
 * jsonpath type send function
 *
 * Just send jsonpath as a version number, then a string of text
 */
pub unsafe fn jsonpath_send(fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum {
    let in_: *mut JsonPath = PG_GETARG_JSONPATH_P!(fcinfo, 0);
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut jtext: StringInfoData = core::mem::zeroed();
    let version: c_int = JSONPATH_VERSION as c_int;

    initStringInfo(&mut jtext);
    jsonPathToCstring(&mut jtext, in_, VARSIZE(in_ as *const c_char) as c_int);

    pq_begintypsend(&mut buf);
    pq_sendint8(&mut buf, version as u8);
    pq_sendtext(&mut buf, jtext.data, jtext.len);
    pfree(jtext.data as *mut c_void);

    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf));
}

/*
 * Converts C-string to a jsonpath value.
 *
 * Uses jsonpath parser to turn string into an AST, then
 * flattenJsonPathParseItem() does second pass turning AST into binary
 * representation of jsonpath.
 */
unsafe fn jsonPathFromCstring(in_: *mut c_char, len: c_int,
                              escontext: *mut Node) -> Datum {
    let jsonpath: *mut JsonPathParseResult = parsejsonpath(in_, len, escontext);
    let res: *mut JsonPath;
    let mut buf: StringInfoData = core::mem::zeroed();

    if SOFT_ERROR_OCCURRED!(escontext) {
        return 0 as Datum;
    }

    if jsonpath.is_null() {
        ereturn!(escontext, 0 as Datum,
            errmsg!("invalid input syntax for type {}: \"{}\"", "jsonpath",
                CStr::from_ptr(in_).to_string_lossy()));
        // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
    }

    initStringInfo(&mut buf);
    enlargeStringInfo(&mut buf, 4 * len /* estimation */);

    appendStringInfoSpaces(&mut buf, JSONPATH_HDRSZ);

    if !flattenJsonPathParseItem(&mut buf, ptr::null_mut(), escontext,
                                 (*jsonpath).expr, 0, false) {
        return 0 as Datum;
    }

    res = buf.data as *mut JsonPath;
    SET_VARSIZE(res as *mut c_char, buf.len);
    (*res).header = JSONPATH_VERSION;
    if (*jsonpath).lax {
        (*res).header |= JSONPATH_LAX;
    }

    PG_RETURN_JSONPATH_P!(res);
}

/*
 * Converts jsonpath value to a C-string.
 *
 * If 'out' argument is non-null, the resulting C-string is stored inside the
 * StringBuffer.  The resulting string is always returned.
 */
unsafe fn jsonPathToCstring(mut out: StringInfo, in_: *mut JsonPath,
                            estimated_len: c_int) -> *mut c_char {
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut v: JsonPathItem = core::mem::zeroed();

    if out.is_null() {
        out = &mut buf;
        initStringInfo(out);
    }
    enlargeStringInfo(out, estimated_len);

    if ((*in_).header & JSONPATH_LAX) == 0 {
        appendStringInfoString(out, c"strict ".as_ptr());
    }

    jspInit(&mut v, in_);
    printJsonPathItem(out, &mut v, false, true);

    return (*out).data;
}

/*
 * Recursive function converting given jsonpath parse item and all its
 * children into a binary representation.
 */
unsafe fn flattenJsonPathParseItem(buf: StringInfo, result: *mut c_int,
                                   escontext: *mut Node,
                                   item: *mut JsonPathParseItem,
                                   nestingLevel: c_int,
                                   insideArraySubscript: bool) -> bool {
    /* position from beginning of jsonpath data */
    let pos: int32 = (*buf).len - JSONPATH_HDRSZ;
    let mut chld: int32 = 0;
    let next: int32;
    let mut argNestingLevel: c_int = 0;

    check_stack_depth();
    CHECK_FOR_INTERRUPTS();

    appendStringInfoChar(buf, (*item).type_ as i32 as c_char);

    /*
     * We align buffer to int32 because a series of int32 values often goes
     * after the header, and we want to read them directly by dereferencing
     * int32 pointer (see jspInitByBuffer()).
     */
    alignStringInfoInt(buf);

    /*
     * Reserve space for next item pointer.  Actual value will be recorded
     * later, after next and children items processing.
     */
    next = reserveSpaceForItemPointer(buf);

    match (*item).type_ {
        jpiString | jpiVariable | jpiKey => {
            appendBinaryStringInfo(buf,
                &(*item).value.string.len as *const uint32 as *const c_void,
                core::mem::size_of_val(&(*item).value.string.len) as c_int);
            appendBinaryStringInfo(buf, (*item).value.string.val as *const c_void,
                (*item).value.string.len as c_int);
            appendStringInfoChar(buf, b'\0' as c_char);
        }
        jpiNumeric => {
            appendBinaryStringInfo(buf, (*item).value.numeric as *const c_void,
                VARSIZE((*item).value.numeric as *const c_char) as c_int);
        }
        jpiBool => {
            appendBinaryStringInfo(buf,
                &(*item).value.boolean as *const bool as *const c_void,
                core::mem::size_of_val(&(*item).value.boolean) as c_int);
        }
        jpiAnd | jpiOr | jpiEqual | jpiNotEqual | jpiLess | jpiGreater
        | jpiLessOrEqual | jpiGreaterOrEqual | jpiAdd | jpiSub | jpiMul
        | jpiDiv | jpiMod | jpiStartsWith | jpiDecimal => {
            /*
             * First, reserve place for left/right arg's positions, then
             * record both args and sets actual position in reserved
             * places.
             */
            let left: int32 = reserveSpaceForItemPointer(buf);
            let right: int32 = reserveSpaceForItemPointer(buf);

            if (*item).value.args.left.is_null() {
                chld = pos;
            } else if !flattenJsonPathParseItem(buf, &mut chld, escontext,
                                                (*item).value.args.left,
                                                nestingLevel + argNestingLevel,
                                                insideArraySubscript) {
                return false;
            }
            *((*buf).data.add(left as usize) as *mut int32) = chld - pos;

            if (*item).value.args.right.is_null() {
                chld = pos;
            } else if !flattenJsonPathParseItem(buf, &mut chld, escontext,
                                                (*item).value.args.right,
                                                nestingLevel + argNestingLevel,
                                                insideArraySubscript) {
                return false;
            }
            *((*buf).data.add(right as usize) as *mut int32) = chld - pos;
        }
        jpiLikeRegex => {
            let offs: int32;

            appendBinaryStringInfo(buf,
                &(*item).value.like_regex.flags as *const uint32 as *const c_void,
                core::mem::size_of_val(&(*item).value.like_regex.flags) as c_int);
            offs = reserveSpaceForItemPointer(buf);
            appendBinaryStringInfo(buf,
                &(*item).value.like_regex.patternlen as *const uint32 as *const c_void,
                core::mem::size_of_val(&(*item).value.like_regex.patternlen) as c_int);
            appendBinaryStringInfo(buf, (*item).value.like_regex.pattern as *const c_void,
                (*item).value.like_regex.patternlen as c_int);
            appendStringInfoChar(buf, b'\0' as c_char);

            if !flattenJsonPathParseItem(buf, &mut chld, escontext,
                                         (*item).value.like_regex.expr,
                                         nestingLevel,
                                         insideArraySubscript) {
                return false;
            }
            *((*buf).data.add(offs as usize) as *mut int32) = chld - pos;
        }
        jpiFilter | jpiIsUnknown | jpiNot | jpiPlus | jpiMinus | jpiExists
        | jpiDatetime | jpiTime | jpiTimeTz | jpiTimestamp | jpiTimestampTz => {
            if (*item).type_ == jpiFilter {
                argNestingLevel += 1;
            }

            let arg: int32 = reserveSpaceForItemPointer(buf);

            if (*item).value.arg.is_null() {
                chld = pos;
            } else if !flattenJsonPathParseItem(buf, &mut chld, escontext,
                                                (*item).value.arg,
                                                nestingLevel + argNestingLevel,
                                                insideArraySubscript) {
                return false;
            }
            *((*buf).data.add(arg as usize) as *mut int32) = chld - pos;
        }
        jpiNull => {}
        jpiRoot => {}
        jpiAnyArray | jpiAnyKey => {}
        jpiCurrent => {
            if nestingLevel <= 0 {
                ereturn!(escontext, false,
                    errmsg!("@ is not allowed in root expressions"));
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
            }
        }
        jpiLast => {
            if !insideArraySubscript {
                ereturn!(escontext, false,
                    errmsg!("LAST is allowed only in array subscripts"));
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
            }
        }
        jpiIndexArray => {
            let nelems: int32 = (*item).value.array.nelems;
            let offset: c_int;
            let mut i: c_int;

            appendBinaryStringInfo(buf, &nelems as *const int32 as *const c_void,
                core::mem::size_of_val(&nelems) as c_int);

            offset = (*buf).len;

            appendStringInfoSpaces(buf,
                (core::mem::size_of::<int32>() * 2) as c_int * nelems);

            i = 0;
            while i < nelems {
                let ppos: *mut int32;
                let mut topos: int32;
                let mut frompos: int32 = 0;

                if !flattenJsonPathParseItem(buf, &mut frompos, escontext,
                        (*(*item).value.array.elems.add(i as usize)).from,
                        nestingLevel, true) {
                    return false;
                }
                frompos -= pos;

                if !(*(*item).value.array.elems.add(i as usize)).to.is_null() {
                    topos = 0;
                    if !flattenJsonPathParseItem(buf, &mut topos, escontext,
                            (*(*item).value.array.elems.add(i as usize)).to,
                            nestingLevel, true) {
                        return false;
                    }
                    topos -= pos;
                } else {
                    topos = 0;
                }

                ppos = &mut (*buf).data[..]
                    .as_mut_ptr()
                    .add((offset + i * 2 * core::mem::size_of::<int32>() as c_int) as usize)
                    as *mut _ as *mut int32;
                let _ = ppos;
                let ppos: *mut int32 = (*buf).data
                    .add((offset + i * 2 * core::mem::size_of::<int32>() as c_int) as usize)
                    as *mut int32;

                *ppos.add(0) = frompos;
                *ppos.add(1) = topos;

                i += 1;
            }
        }
        jpiAny => {
            appendBinaryStringInfo(buf,
                &(*item).value.anybounds.first as *const uint32 as *const c_void,
                core::mem::size_of_val(&(*item).value.anybounds.first) as c_int);
            appendBinaryStringInfo(buf,
                &(*item).value.anybounds.last as *const uint32 as *const c_void,
                core::mem::size_of_val(&(*item).value.anybounds.last) as c_int);
        }
        jpiType | jpiSize | jpiAbs | jpiFloor | jpiCeiling | jpiDouble
        | jpiKeyValue | jpiBigint | jpiBoolean | jpiDate | jpiInteger
        | jpiNumber | jpiStringFunc => {}
        _ => {
            elog!(ERROR, "unrecognized jsonpath item type: {}", (*item).type_ as c_int);
        }
    }

    if !(*item).next.is_null() {
        if !flattenJsonPathParseItem(buf, &mut chld, escontext,
                                     (*item).next, nestingLevel,
                                     insideArraySubscript) {
            return false;
        }
        chld -= pos;
        *((*buf).data.add(next as usize) as *mut int32) = chld;
    }

    if !result.is_null() {
        *result = pos;
    }
    true
}

/*
 * Align StringInfo to int by adding zero padding bytes
 */
unsafe fn alignStringInfoInt(buf: StringInfo) {
    match INTALIGN((*buf).len as usize) as c_int - (*buf).len {
        3 => {
            appendStringInfoCharMacro(buf, 0);
            /* FALLTHROUGH */
            appendStringInfoCharMacro(buf, 0);
            appendStringInfoCharMacro(buf, 0);
        }
        2 => {
            appendStringInfoCharMacro(buf, 0);
            /* FALLTHROUGH */
            appendStringInfoCharMacro(buf, 0);
        }
        1 => {
            appendStringInfoCharMacro(buf, 0);
            /* FALLTHROUGH */
        }
        _ => {}
    }
}

/*
 * Reserve space for int32 JsonPathItem pointer.  Now zero pointer is written,
 * actual value will be recorded at '(int32 *) &buf->data[pos]' later.
 */
unsafe fn reserveSpaceForItemPointer(buf: StringInfo) -> int32 {
    let pos: int32 = (*buf).len;
    let ptr_: int32 = 0;

    appendBinaryStringInfo(buf, &ptr_ as *const int32 as *const c_void,
        core::mem::size_of_val(&ptr_) as c_int);

    return pos;
}

/*
 * Prints text representation of given jsonpath item and all its children.
 */
unsafe fn printJsonPathItem(buf: StringInfo, v: *mut JsonPathItem, inKey: bool,
                            printBracketes: bool) {
    let mut elem: JsonPathItem = core::mem::zeroed();
    let mut i: c_int;
    let mut len: int32 = 0;
    let mut str: *mut c_char;

    check_stack_depth();
    CHECK_FOR_INTERRUPTS();

    match (*v).type_ {
        jpiNull => {
            appendStringInfoString(buf, c"null".as_ptr());
        }
        jpiString => {
            str = jspGetString(v, &mut len);
            escape_json_with_len(buf, str, len);
        }
        jpiNumeric => {
            if jspHasNext(v) {
                appendStringInfoChar(buf, b'(' as c_char);
            }
            appendStringInfoString(buf,
                DatumGetCString(DirectFunctionCall1!(numeric_out,
                    NumericGetDatum(jspGetNumeric(v)))));
            if jspHasNext(v) {
                appendStringInfoChar(buf, b')' as c_char);
            }
        }
        jpiBool => {
            if jspGetBool(v) {
                appendStringInfoString(buf, c"true".as_ptr());
            } else {
                appendStringInfoString(buf, c"false".as_ptr());
            }
        }
        jpiAnd | jpiOr | jpiEqual | jpiNotEqual | jpiLess | jpiGreater
        | jpiLessOrEqual | jpiGreaterOrEqual | jpiAdd | jpiSub | jpiMul
        | jpiDiv | jpiMod | jpiStartsWith => {
            if printBracketes {
                appendStringInfoChar(buf, b'(' as c_char);
            }
            jspGetLeftArg(v, &mut elem);
            printJsonPathItem(buf, &mut elem, false,
                operationPriority(elem.type_) <= operationPriority((*v).type_));
            appendStringInfoChar(buf, b' ' as c_char);
            appendStringInfoString(buf, jspOperationName((*v).type_));
            appendStringInfoChar(buf, b' ' as c_char);
            jspGetRightArg(v, &mut elem);
            printJsonPathItem(buf, &mut elem, false,
                operationPriority(elem.type_) <= operationPriority((*v).type_));
            if printBracketes {
                appendStringInfoChar(buf, b')' as c_char);
            }
        }
        jpiNot => {
            appendStringInfoString(buf, c"!(".as_ptr());
            jspGetArg(v, &mut elem);
            printJsonPathItem(buf, &mut elem, false, false);
            appendStringInfoChar(buf, b')' as c_char);
        }
        jpiIsUnknown => {
            appendStringInfoChar(buf, b'(' as c_char);
            jspGetArg(v, &mut elem);
            printJsonPathItem(buf, &mut elem, false, false);
            appendStringInfoString(buf, c") is unknown".as_ptr());
        }
        jpiPlus | jpiMinus => {
            if printBracketes {
                appendStringInfoChar(buf, b'(' as c_char);
            }
            appendStringInfoChar(buf,
                if (*v).type_ == jpiPlus { b'+' as c_char } else { b'-' as c_char });
            jspGetArg(v, &mut elem);
            printJsonPathItem(buf, &mut elem, false,
                operationPriority(elem.type_) <= operationPriority((*v).type_));
            if printBracketes {
                appendStringInfoChar(buf, b')' as c_char);
            }
        }
        jpiAnyArray => {
            appendStringInfoString(buf, c"[*]".as_ptr());
        }
        jpiAnyKey => {
            if inKey {
                appendStringInfoChar(buf, b'.' as c_char);
            }
            appendStringInfoChar(buf, b'*' as c_char);
        }
        jpiIndexArray => {
            appendStringInfoChar(buf, b'[' as c_char);
            i = 0;
            while i < (*v).content.array.nelems {
                let mut from: JsonPathItem = core::mem::zeroed();
                let mut to: JsonPathItem = core::mem::zeroed();
                let range: bool = jspGetArraySubscript(v, &mut from, &mut to, i);

                if i != 0 {
                    appendStringInfoChar(buf, b',' as c_char);
                }

                printJsonPathItem(buf, &mut from, false, false);

                if range {
                    appendStringInfoString(buf, c" to ".as_ptr());
                    printJsonPathItem(buf, &mut to, false, false);
                }

                i += 1;
            }
            appendStringInfoChar(buf, b']' as c_char);
        }
        jpiAny => {
            if inKey {
                appendStringInfoChar(buf, b'.' as c_char);
            }

            if (*v).content.anybounds.first == 0 &&
                (*v).content.anybounds.last == PG_UINT32_MAX {
                appendStringInfoString(buf, c"**".as_ptr());
            } else if (*v).content.anybounds.first == (*v).content.anybounds.last {
                if (*v).content.anybounds.first == PG_UINT32_MAX {
                    appendStringInfoString(buf, c"**{last}".as_ptr());
                } else {
                    appendStringInfo!(buf, "**{{{}}}",
                        (*v).content.anybounds.first);
                }
            } else if (*v).content.anybounds.first == PG_UINT32_MAX {
                appendStringInfo!(buf, "**{{last to {}}}",
                    (*v).content.anybounds.last);
            } else if (*v).content.anybounds.last == PG_UINT32_MAX {
                appendStringInfo!(buf, "**{{{} to last}}",
                    (*v).content.anybounds.first);
            } else {
                appendStringInfo!(buf, "**{{{} to {}}}",
                    (*v).content.anybounds.first,
                    (*v).content.anybounds.last);
            }
        }
        jpiKey => {
            if inKey {
                appendStringInfoChar(buf, b'.' as c_char);
            }
            str = jspGetString(v, &mut len);
            escape_json_with_len(buf, str, len);
        }
        jpiCurrent => {
            assert!(!inKey);
            appendStringInfoChar(buf, b'@' as c_char);
        }
        jpiRoot => {
            assert!(!inKey);
            appendStringInfoChar(buf, b'$' as c_char);
        }
        jpiVariable => {
            appendStringInfoChar(buf, b'$' as c_char);
            str = jspGetString(v, &mut len);
            escape_json_with_len(buf, str, len);
        }
        jpiFilter => {
            appendStringInfoString(buf, c"?(".as_ptr());
            jspGetArg(v, &mut elem);
            printJsonPathItem(buf, &mut elem, false, false);
            appendStringInfoChar(buf, b')' as c_char);
        }
        jpiExists => {
            appendStringInfoString(buf, c"exists (".as_ptr());
            jspGetArg(v, &mut elem);
            printJsonPathItem(buf, &mut elem, false, false);
            appendStringInfoChar(buf, b')' as c_char);
        }
        jpiType => {
            appendStringInfoString(buf, c".type()".as_ptr());
        }
        jpiSize => {
            appendStringInfoString(buf, c".size()".as_ptr());
        }
        jpiAbs => {
            appendStringInfoString(buf, c".abs()".as_ptr());
        }
        jpiFloor => {
            appendStringInfoString(buf, c".floor()".as_ptr());
        }
        jpiCeiling => {
            appendStringInfoString(buf, c".ceiling()".as_ptr());
        }
        jpiDouble => {
            appendStringInfoString(buf, c".double()".as_ptr());
        }
        jpiDatetime => {
            appendStringInfoString(buf, c".datetime(".as_ptr());
            if (*v).content.arg != 0 {
                jspGetArg(v, &mut elem);
                printJsonPathItem(buf, &mut elem, false, false);
            }
            appendStringInfoChar(buf, b')' as c_char);
        }
        jpiKeyValue => {
            appendStringInfoString(buf, c".keyvalue()".as_ptr());
        }
        jpiLast => {
            appendStringInfoString(buf, c"last".as_ptr());
        }
        jpiLikeRegex => {
            if printBracketes {
                appendStringInfoChar(buf, b'(' as c_char);
            }

            jspInitByBuffer(&mut elem, (*v).base, (*v).content.like_regex.expr);
            printJsonPathItem(buf, &mut elem, false,
                operationPriority(elem.type_) <= operationPriority((*v).type_));

            appendStringInfoString(buf, c" like_regex ".as_ptr());

            escape_json_with_len(buf,
                (*v).content.like_regex.pattern,
                (*v).content.like_regex.patternlen);

            if (*v).content.like_regex.flags != 0 {
                appendStringInfoString(buf, c" flag \"".as_ptr());

                if (*v).content.like_regex.flags & JSP_REGEX_ICASE != 0 {
                    appendStringInfoChar(buf, b'i' as c_char);
                }
                if (*v).content.like_regex.flags & JSP_REGEX_DOTALL != 0 {
                    appendStringInfoChar(buf, b's' as c_char);
                }
                if (*v).content.like_regex.flags & JSP_REGEX_MLINE != 0 {
                    appendStringInfoChar(buf, b'm' as c_char);
                }
                if (*v).content.like_regex.flags & JSP_REGEX_WSPACE != 0 {
                    appendStringInfoChar(buf, b'x' as c_char);
                }
                if (*v).content.like_regex.flags & JSP_REGEX_QUOTE != 0 {
                    appendStringInfoChar(buf, b'q' as c_char);
                }

                appendStringInfoChar(buf, b'"' as c_char);
            }

            if printBracketes {
                appendStringInfoChar(buf, b')' as c_char);
            }
        }
        jpiBigint => {
            appendStringInfoString(buf, c".bigint()".as_ptr());
        }
        jpiBoolean => {
            appendStringInfoString(buf, c".boolean()".as_ptr());
        }
        jpiDate => {
            appendStringInfoString(buf, c".date()".as_ptr());
        }
        jpiDecimal => {
            appendStringInfoString(buf, c".decimal(".as_ptr());
            if (*v).content.args.left != 0 {
                jspGetLeftArg(v, &mut elem);
                printJsonPathItem(buf, &mut elem, false, false);
            }
            if (*v).content.args.right != 0 {
                appendStringInfoChar(buf, b',' as c_char);
                jspGetRightArg(v, &mut elem);
                printJsonPathItem(buf, &mut elem, false, false);
            }
            appendStringInfoChar(buf, b')' as c_char);
        }
        jpiInteger => {
            appendStringInfoString(buf, c".integer()".as_ptr());
        }
        jpiNumber => {
            appendStringInfoString(buf, c".number()".as_ptr());
        }
        jpiStringFunc => {
            appendStringInfoString(buf, c".string()".as_ptr());
        }
        jpiTime => {
            appendStringInfoString(buf, c".time(".as_ptr());
            if (*v).content.arg != 0 {
                jspGetArg(v, &mut elem);
                printJsonPathItem(buf, &mut elem, false, false);
            }
            appendStringInfoChar(buf, b')' as c_char);
        }
        jpiTimeTz => {
            appendStringInfoString(buf, c".time_tz(".as_ptr());
            if (*v).content.arg != 0 {
                jspGetArg(v, &mut elem);
                printJsonPathItem(buf, &mut elem, false, false);
            }
            appendStringInfoChar(buf, b')' as c_char);
        }
        jpiTimestamp => {
            appendStringInfoString(buf, c".timestamp(".as_ptr());
            if (*v).content.arg != 0 {
                jspGetArg(v, &mut elem);
                printJsonPathItem(buf, &mut elem, false, false);
            }
            appendStringInfoChar(buf, b')' as c_char);
        }
        jpiTimestampTz => {
            appendStringInfoString(buf, c".timestamp_tz(".as_ptr());
            if (*v).content.arg != 0 {
                jspGetArg(v, &mut elem);
                printJsonPathItem(buf, &mut elem, false, false);
            }
            appendStringInfoChar(buf, b')' as c_char);
        }
        _ => {
            elog!(ERROR, "unrecognized jsonpath item type: {}", (*v).type_ as c_int);
        }
    }

    if jspGetNext(v, &mut elem) {
        printJsonPathItem(buf, &mut elem, true, true);
    }
}

pub unsafe fn jspOperationName(type_: JsonPathItemType) -> *const c_char {
    match type_ {
        jpiAnd => c"&&".as_ptr(),
        jpiOr => c"||".as_ptr(),
        jpiEqual => c"==".as_ptr(),
        jpiNotEqual => c"!=".as_ptr(),
        jpiLess => c"<".as_ptr(),
        jpiGreater => c">".as_ptr(),
        jpiLessOrEqual => c"<=".as_ptr(),
        jpiGreaterOrEqual => c">=".as_ptr(),
        jpiAdd | jpiPlus => c"+".as_ptr(),
        jpiSub | jpiMinus => c"-".as_ptr(),
        jpiMul => c"*".as_ptr(),
        jpiDiv => c"/".as_ptr(),
        jpiMod => c"%".as_ptr(),
        jpiType => c"type".as_ptr(),
        jpiSize => c"size".as_ptr(),
        jpiAbs => c"abs".as_ptr(),
        jpiFloor => c"floor".as_ptr(),
        jpiCeiling => c"ceiling".as_ptr(),
        jpiDouble => c"double".as_ptr(),
        jpiDatetime => c"datetime".as_ptr(),
        jpiKeyValue => c"keyvalue".as_ptr(),
        jpiStartsWith => c"starts with".as_ptr(),
        jpiLikeRegex => c"like_regex".as_ptr(),
        jpiBigint => c"bigint".as_ptr(),
        jpiBoolean => c"boolean".as_ptr(),
        jpiDate => c"date".as_ptr(),
        jpiDecimal => c"decimal".as_ptr(),
        jpiInteger => c"integer".as_ptr(),
        jpiNumber => c"number".as_ptr(),
        jpiStringFunc => c"string".as_ptr(),
        jpiTime => c"time".as_ptr(),
        jpiTimeTz => c"time_tz".as_ptr(),
        jpiTimestamp => c"timestamp".as_ptr(),
        jpiTimestampTz => c"timestamp_tz".as_ptr(),
        _ => {
            elog!(ERROR, "unrecognized jsonpath item type: {}", type_ as c_int);
            ptr::null()
        }
    }
}

unsafe fn operationPriority(op: JsonPathItemType) -> c_int {
    match op {
        jpiOr => 0,
        jpiAnd => 1,
        jpiEqual | jpiNotEqual | jpiLess | jpiGreater | jpiLessOrEqual
        | jpiGreaterOrEqual | jpiStartsWith => 2,
        jpiAdd | jpiSub => 3,
        jpiMul | jpiDiv | jpiMod => 4,
        jpiPlus | jpiMinus => 5,
        _ => 6,
    }
}

/******************* Support functions for JsonPath *************************/

/*
 * Support macros to read stored values
 */

macro_rules! read_byte {
    ($v:expr, $b:expr, $p:expr) => {{
        $v = core::mem::transmute::<u32, JsonPathItemType>(
            *(($b).add($p as usize) as *const u8) as u32);
        $p += 1;
    }};
}

macro_rules! read_int32 {
    ($v:expr, $b:expr, $p:expr) => {{
        $v = *(($b).add($p as usize) as *const uint32) as _;
        $p += core::mem::size_of::<int32>() as int32;
    }};
}

macro_rules! read_int32_n {
    ($v:expr, $b:expr, $p:expr, $n:expr) => {{
        $v = ($b).add($p as usize) as *mut _;
        $p += core::mem::size_of::<int32>() as int32 * ($n);
    }};
}

/*
 * Read root node and fill root node representation
 */
pub unsafe fn jspInit(v: *mut JsonPathItem, js: *mut JsonPath) {
    assert!(((*js).header & !JSONPATH_LAX) == JSONPATH_VERSION);
    jspInitByBuffer(v, (*js).data.as_mut_ptr(), 0);
}

/*
 * Read node from buffer and fill its representation
 */
pub unsafe fn jspInitByBuffer(v: *mut JsonPathItem, base: *mut c_char, mut pos: int32) {
    (*v).base = base.add(pos as usize);

    read_byte!((*v).type_, base, pos);
    pos = (INTALIGN(base.add(pos as usize) as usize) as isize
        - base as isize) as int32;
    read_int32!((*v).nextPos, base, pos);

    match (*v).type_ {
        jpiNull | jpiRoot | jpiCurrent | jpiAnyArray | jpiAnyKey | jpiType
        | jpiSize | jpiAbs | jpiFloor | jpiCeiling | jpiDouble | jpiKeyValue
        | jpiLast | jpiBigint | jpiBoolean | jpiDate | jpiInteger | jpiNumber
        | jpiStringFunc => {}
        jpiString | jpiKey | jpiVariable => {
            read_int32!((*v).content.value.datalen, base, pos);
            /* FALLTHROUGH */
            (*v).content.value.data = base.add(pos as usize);
        }
        jpiNumeric | jpiBool => {
            (*v).content.value.data = base.add(pos as usize);
        }
        jpiAnd | jpiOr | jpiEqual | jpiNotEqual | jpiLess | jpiGreater
        | jpiLessOrEqual | jpiGreaterOrEqual | jpiAdd | jpiSub | jpiMul
        | jpiDiv | jpiMod | jpiStartsWith | jpiDecimal => {
            read_int32!((*v).content.args.left, base, pos);
            read_int32!((*v).content.args.right, base, pos);
        }
        jpiNot | jpiIsUnknown | jpiExists | jpiPlus | jpiMinus | jpiFilter
        | jpiDatetime | jpiTime | jpiTimeTz | jpiTimestamp | jpiTimestampTz => {
            read_int32!((*v).content.arg, base, pos);
        }
        jpiIndexArray => {
            read_int32!((*v).content.array.nelems, base, pos);
            read_int32_n!((*v).content.array.elems, base, pos,
                (*v).content.array.nelems * 2);
        }
        jpiAny => {
            read_int32!((*v).content.anybounds.first, base, pos);
            read_int32!((*v).content.anybounds.last, base, pos);
        }
        jpiLikeRegex => {
            read_int32!((*v).content.like_regex.flags, base, pos);
            read_int32!((*v).content.like_regex.expr, base, pos);
            read_int32!((*v).content.like_regex.patternlen, base, pos);
            (*v).content.like_regex.pattern = base.add(pos as usize);
        }
        _ => {
            elog!(ERROR, "unrecognized jsonpath item type: {}", (*v).type_ as c_int);
        }
    }
}

pub unsafe fn jspGetArg(v: *mut JsonPathItem, a: *mut JsonPathItem) {
    assert!((*v).type_ == jpiNot ||
        (*v).type_ == jpiIsUnknown ||
        (*v).type_ == jpiPlus ||
        (*v).type_ == jpiMinus ||
        (*v).type_ == jpiFilter ||
        (*v).type_ == jpiExists ||
        (*v).type_ == jpiDatetime ||
        (*v).type_ == jpiTime ||
        (*v).type_ == jpiTimeTz ||
        (*v).type_ == jpiTimestamp ||
        (*v).type_ == jpiTimestampTz);

    jspInitByBuffer(a, (*v).base, (*v).content.arg);
}

pub unsafe fn jspGetNext(v: *mut JsonPathItem, a: *mut JsonPathItem) -> bool {
    if jspHasNext(v) {
        assert!((*v).type_ == jpiNull ||
            (*v).type_ == jpiString ||
            (*v).type_ == jpiNumeric ||
            (*v).type_ == jpiBool ||
            (*v).type_ == jpiAnd ||
            (*v).type_ == jpiOr ||
            (*v).type_ == jpiNot ||
            (*v).type_ == jpiIsUnknown ||
            (*v).type_ == jpiEqual ||
            (*v).type_ == jpiNotEqual ||
            (*v).type_ == jpiLess ||
            (*v).type_ == jpiGreater ||
            (*v).type_ == jpiLessOrEqual ||
            (*v).type_ == jpiGreaterOrEqual ||
            (*v).type_ == jpiAdd ||
            (*v).type_ == jpiSub ||
            (*v).type_ == jpiMul ||
            (*v).type_ == jpiDiv ||
            (*v).type_ == jpiMod ||
            (*v).type_ == jpiPlus ||
            (*v).type_ == jpiMinus ||
            (*v).type_ == jpiAnyArray ||
            (*v).type_ == jpiAnyKey ||
            (*v).type_ == jpiIndexArray ||
            (*v).type_ == jpiAny ||
            (*v).type_ == jpiKey ||
            (*v).type_ == jpiCurrent ||
            (*v).type_ == jpiRoot ||
            (*v).type_ == jpiVariable ||
            (*v).type_ == jpiFilter ||
            (*v).type_ == jpiExists ||
            (*v).type_ == jpiType ||
            (*v).type_ == jpiSize ||
            (*v).type_ == jpiAbs ||
            (*v).type_ == jpiFloor ||
            (*v).type_ == jpiCeiling ||
            (*v).type_ == jpiDouble ||
            (*v).type_ == jpiDatetime ||
            (*v).type_ == jpiKeyValue ||
            (*v).type_ == jpiLast ||
            (*v).type_ == jpiStartsWith ||
            (*v).type_ == jpiLikeRegex ||
            (*v).type_ == jpiBigint ||
            (*v).type_ == jpiBoolean ||
            (*v).type_ == jpiDate ||
            (*v).type_ == jpiDecimal ||
            (*v).type_ == jpiInteger ||
            (*v).type_ == jpiNumber ||
            (*v).type_ == jpiStringFunc ||
            (*v).type_ == jpiTime ||
            (*v).type_ == jpiTimeTz ||
            (*v).type_ == jpiTimestamp ||
            (*v).type_ == jpiTimestampTz);

        if !a.is_null() {
            jspInitByBuffer(a, (*v).base, (*v).nextPos);
        }
        return true;
    }

    return false;
}

pub unsafe fn jspGetLeftArg(v: *mut JsonPathItem, a: *mut JsonPathItem) {
    assert!((*v).type_ == jpiAnd ||
        (*v).type_ == jpiOr ||
        (*v).type_ == jpiEqual ||
        (*v).type_ == jpiNotEqual ||
        (*v).type_ == jpiLess ||
        (*v).type_ == jpiGreater ||
        (*v).type_ == jpiLessOrEqual ||
        (*v).type_ == jpiGreaterOrEqual ||
        (*v).type_ == jpiAdd ||
        (*v).type_ == jpiSub ||
        (*v).type_ == jpiMul ||
        (*v).type_ == jpiDiv ||
        (*v).type_ == jpiMod ||
        (*v).type_ == jpiStartsWith ||
        (*v).type_ == jpiDecimal);

    jspInitByBuffer(a, (*v).base, (*v).content.args.left);
}

pub unsafe fn jspGetRightArg(v: *mut JsonPathItem, a: *mut JsonPathItem) {
    assert!((*v).type_ == jpiAnd ||
        (*v).type_ == jpiOr ||
        (*v).type_ == jpiEqual ||
        (*v).type_ == jpiNotEqual ||
        (*v).type_ == jpiLess ||
        (*v).type_ == jpiGreater ||
        (*v).type_ == jpiLessOrEqual ||
        (*v).type_ == jpiGreaterOrEqual ||
        (*v).type_ == jpiAdd ||
        (*v).type_ == jpiSub ||
        (*v).type_ == jpiMul ||
        (*v).type_ == jpiDiv ||
        (*v).type_ == jpiMod ||
        (*v).type_ == jpiStartsWith ||
        (*v).type_ == jpiDecimal);

    jspInitByBuffer(a, (*v).base, (*v).content.args.right);
}

pub unsafe fn jspGetBool(v: *mut JsonPathItem) -> bool {
    assert!((*v).type_ == jpiBool);

    return *(*v).content.value.data != 0;
}

pub unsafe fn jspGetNumeric(v: *mut JsonPathItem) -> Numeric {
    assert!((*v).type_ == jpiNumeric);

    return (*v).content.value.data as Numeric;
}

pub unsafe fn jspGetString(v: *mut JsonPathItem, len: *mut int32) -> *mut c_char {
    assert!((*v).type_ == jpiKey ||
        (*v).type_ == jpiString ||
        (*v).type_ == jpiVariable);

    if !len.is_null() {
        *len = (*v).content.value.datalen;
    }
    return (*v).content.value.data;
}

pub unsafe fn jspGetArraySubscript(v: *mut JsonPathItem, from: *mut JsonPathItem,
                                   to: *mut JsonPathItem, i: c_int) -> bool {
    assert!((*v).type_ == jpiIndexArray);

    jspInitByBuffer(from, (*v).base, (*(*v).content.array.elems.add(i as usize)).from);

    if (*(*v).content.array.elems.add(i as usize)).to == 0 {
        return false;
    }

    jspInitByBuffer(to, (*v).base, (*(*v).content.array.elems.add(i as usize)).to);

    return true;
}

/* SQL/JSON datatype status: */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum JsonPathDatatypeStatus {
    jpdsNonDateTime,        /* null, bool, numeric, string, array, object */
    jpdsUnknownDateTime,    /* unknown datetime type */
    jpdsDateTimeZoned,      /* timetz, timestamptz */
    jpdsDateTimeNonZoned,   /* time, timestamp, date */
}
use JsonPathDatatypeStatus::*;

/* Context for jspIsMutableWalker() */
#[repr(C)]
struct JsonPathMutableContext {
    varnames: *mut List,                    /* list of variable names */
    varexprs: *mut List,                    /* list of variable expressions */
    current: JsonPathDatatypeStatus,        /* status of @ item */
    lax: bool,                              /* jsonpath is lax or strict */
    mutable_: bool,                         /* resulting mutability status */
}

/*
 * Function to check whether jsonpath expression is mutable to be used in the
 * planner function contain_mutable_functions().
 */
pub unsafe fn jspIsMutable(path: *mut JsonPath, varnames: *mut List,
                           varexprs: *mut List) -> bool {
    let mut cxt: JsonPathMutableContext = core::mem::zeroed();
    let mut jpi: JsonPathItem = core::mem::zeroed();

    cxt.varnames = varnames;
    cxt.varexprs = varexprs;
    cxt.current = jpdsNonDateTime;
    cxt.lax = ((*path).header & JSONPATH_LAX) != 0;
    cxt.mutable_ = false;

    jspInit(&mut jpi, path);
    jspIsMutableWalker(&mut jpi, &mut cxt);

    return cxt.mutable_;
}

/*
 * Recursive walker for jspIsMutable()
 */
unsafe fn jspIsMutableWalker(mut jpi: *mut JsonPathItem,
                             cxt: *mut JsonPathMutableContext)
                             -> JsonPathDatatypeStatus {
    let mut next: JsonPathItem = core::mem::zeroed();
    let mut status: JsonPathDatatypeStatus = jpdsNonDateTime;

    while !(*cxt).mutable_ {
        let mut arg: JsonPathItem = core::mem::zeroed();
        let leftStatus: JsonPathDatatypeStatus;
        let rightStatus: JsonPathDatatypeStatus;

        match (*jpi).type_ {
            jpiRoot => {
                assert!(status == jpdsNonDateTime);
            }

            jpiCurrent => {
                assert!(status == jpdsNonDateTime);
                status = (*cxt).current;
            }

            jpiFilter => {
                let prevStatus: JsonPathDatatypeStatus = (*cxt).current;

                (*cxt).current = status;
                jspGetArg(jpi, &mut arg);
                jspIsMutableWalker(&mut arg, cxt);

                (*cxt).current = prevStatus;
            }

            jpiVariable => {
                let mut len: int32 = 0;
                let name: *const c_char = jspGetString(jpi, &mut len);
                let mut lc1: *mut ListCell;
                let mut lc2: *mut ListCell;

                assert!(status == jpdsNonDateTime);

                forboth!(lc1, (*cxt).varnames, lc2, (*cxt).varexprs, {
                    let varname: *mut PgString = lfirst_node!(PgString, T_String, lc1);
                    let varexpr: *mut Node = lfirst(lc2) as *mut Node;

                    if libc::strncmp((*varname).sval, name, len as libc::size_t) != 0 {
                        continue;
                    }

                    match exprType(varexpr) {
                        DATEOID | TIMEOID | TIMESTAMPOID => {
                            status = jpdsDateTimeNonZoned;
                        }

                        TIMETZOID | TIMESTAMPTZOID => {
                            status = jpdsDateTimeZoned;
                        }

                        _ => {
                            status = jpdsNonDateTime;
                        }
                    }

                    break;
                });
            }

            jpiEqual | jpiNotEqual | jpiLess | jpiGreater | jpiLessOrEqual
            | jpiGreaterOrEqual => {
                assert!(status == jpdsNonDateTime);
                jspGetLeftArg(jpi, &mut arg);
                leftStatus = jspIsMutableWalker(&mut arg, cxt);

                jspGetRightArg(jpi, &mut arg);
                rightStatus = jspIsMutableWalker(&mut arg, cxt);

                /*
                 * Comparison of datetime type with different timezone status
                 * is mutable.
                 */
                if leftStatus != jpdsNonDateTime &&
                    rightStatus != jpdsNonDateTime &&
                    (leftStatus == jpdsUnknownDateTime ||
                     rightStatus == jpdsUnknownDateTime ||
                     leftStatus != rightStatus) {
                    (*cxt).mutable_ = true;
                }
            }

            jpiNot | jpiIsUnknown | jpiExists | jpiPlus | jpiMinus => {
                assert!(status == jpdsNonDateTime);
                jspGetArg(jpi, &mut arg);
                jspIsMutableWalker(&mut arg, cxt);
            }

            jpiAnd | jpiOr | jpiAdd | jpiSub | jpiMul | jpiDiv | jpiMod
            | jpiStartsWith => {
                assert!(status == jpdsNonDateTime);
                jspGetLeftArg(jpi, &mut arg);
                jspIsMutableWalker(&mut arg, cxt);
                jspGetRightArg(jpi, &mut arg);
                jspIsMutableWalker(&mut arg, cxt);
            }

            jpiIndexArray => {
                let mut i: c_int = 0;
                while i < (*jpi).content.array.nelems {
                    let mut from: JsonPathItem = core::mem::zeroed();
                    let mut to: JsonPathItem = core::mem::zeroed();

                    if jspGetArraySubscript(jpi, &mut from, &mut to, i) {
                        jspIsMutableWalker(&mut to, cxt);
                    }

                    jspIsMutableWalker(&mut from, cxt);

                    i += 1;
                }
                /* FALLTHROUGH */
                if !(*cxt).lax {
                    status = jpdsNonDateTime;
                }
            }

            jpiAnyArray => {
                if !(*cxt).lax {
                    status = jpdsNonDateTime;
                }
            }

            jpiAny => {
                if (*jpi).content.anybounds.first > 0 {
                    status = jpdsNonDateTime;
                }
            }

            jpiDatetime => {
                if (*jpi).content.arg != 0 {
                    let template: *mut c_char;

                    jspGetArg(jpi, &mut arg);
                    if arg.type_ != jpiString {
                        status = jpdsNonDateTime;
                        break;  /* there will be runtime error */
                    }

                    template = jspGetString(&mut arg, ptr::null_mut());
                    if datetime_format_has_tz(template) {
                        status = jpdsDateTimeZoned;
                    } else {
                        status = jpdsDateTimeNonZoned;
                    }
                } else {
                    status = jpdsUnknownDateTime;
                }
            }

            jpiLikeRegex => {
                assert!(status == jpdsNonDateTime);
                jspInitByBuffer(&mut arg, (*jpi).base, (*jpi).content.like_regex.expr);
                jspIsMutableWalker(&mut arg, cxt);
            }

            /* literals */
            jpiNull | jpiString | jpiNumeric | jpiBool
            /* accessors */
            | jpiKey | jpiAnyKey
            /* special items */
            | jpiSubscript | jpiLast
            /* item methods */
            | jpiType | jpiSize | jpiAbs | jpiFloor | jpiCeiling | jpiDouble
            | jpiKeyValue | jpiBigint | jpiBoolean | jpiDecimal | jpiInteger
            | jpiNumber | jpiStringFunc => {
                status = jpdsNonDateTime;
            }

            jpiTime | jpiDate | jpiTimestamp => {
                status = jpdsDateTimeNonZoned;
                (*cxt).mutable_ = true;
            }

            jpiTimeTz | jpiTimestampTz => {
                status = jpdsDateTimeNonZoned;
                (*cxt).mutable_ = true;
            }

            _ => {}
        }

        if !jspGetNext(jpi, &mut next) {
            break;
        }

        jpi = &mut next;
    }

    return status;
}
