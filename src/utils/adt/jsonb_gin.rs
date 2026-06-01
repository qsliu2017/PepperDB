//! jsonb_gin.rs
//!   GIN support functions for jsonb
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/jsonb_gin.c
//!
//! Copyright (c) 2014-2025, PostgreSQL Global Development Group
//!
//! We provide two opclasses for jsonb indexing: jsonb_ops and jsonb_path_ops.
//! For their description see json.sgml and comments in jsonb.h.
//!
//! The operators support, among the others, "jsonb @? jsonpath" and
//! "jsonb @@ jsonpath".  Expressions containing these operators are easily
//! expressed through each other.
//!
//!	jb @? 'path' <=> jb @@ 'EXISTS(path)'
//!	jb @@ 'expr' <=> jb @? '$ ? (expr)'
//!
//! Thus, we're going to consider only @@ operator, while regarding @? operator
//! the same is true for jb @@ 'EXISTS(path)'.
//!
//! Result of jsonpath query extraction is a tree, which leaf nodes are index
//! entries and non-leaf nodes are AND/OR logical expressions.  Basically we
//! extract following statements out of jsonpath:
//!
//!	1) "accessors_chain = const",
//!	2) "EXISTS(accessors_chain)".
//!
//! Accessors chain may consist of .key, [*] and [index] accessors.  jsonb_ops
//! additionally supports .* and .**.
//!
//! For now, both jsonb_ops and jsonb_path_ops supports only statements of
//! the 1st find.  jsonb_ops might also support statements of the 2nd kind,
//! but given we have no statistics keys extracted from accessors chain
//! are likely non-selective.  Therefore, we choose to not confuse optimizer
//! and skip statements of the 2nd kind altogether.  In future versions that
//! might be changed.
//!
//! In jsonb_ops statement of the 1st kind is split into expression of AND'ed
//! keys and const.  Sometimes const might be interpreted as both value or key
//! in jsonb_ops.  Then statement of 1st kind is decomposed into the expression
//! below.
//!
//!	key1 AND key2 AND ... AND keyN AND (const_as_value OR const_as_key)
//!
//! jsonb_path_ops transforms each statement of the 1st kind into single hash
//! entry below.
//!
//!	HASH(key1, key2, ... , keyN, const)
//!
//! Despite statements of the 2nd kind are not supported by both jsonb_ops and
//! jsonb_path_ops, EXISTS(path) expressions might be still supported,
//! when statements of 1st kind could be extracted out of their filters.
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/jsonb_gin.c

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use crate::prelude::*; // postgres.h

// access/gin.h
use crate::access::gin::gin::{
    GinTernaryValue, GIN_FALSE, GIN_MAYBE, GIN_SEARCH_MODE_ALL, GIN_TRUE,
};
// access/stratnum.h
use crate::access::stratnum::StrategyNumber;
// catalog/pg_collation.h
use crate::catalog::pg_known_oids::C_COLLATION_OID;
// catalog/pg_type.h
use crate::catalog::pg_type_d::TEXTOID;
// common/hashfn.h
use crate::common::hashfn::hash_any;
// utils/jsonb.h (jsonb_util.c)
use crate::utils::adt::jsonb_util::{
    jbvBool, jbvNull, jbvNumeric, jbvString, Jsonb, JsonbHashScalarValue, JsonbIterator,
    JsonbIteratorInit, JsonbIteratorNext, JsonbIteratorToken, JsonbValue, Numeric, WJB_BEGIN_ARRAY,
    WJB_BEGIN_OBJECT, WJB_DONE, WJB_ELEM, WJB_END_ARRAY, WJB_END_OBJECT, WJB_KEY, WJB_VALUE,
};
// utils/adt/numeric.c
use crate::utils::adt::numeric::numeric_normalize;
// utils/adt/arrayfuncs.c
use crate::utils::adt::arrayfuncs::deconstruct_array_builtin;
// utils/array.h
use crate::utils::array::ArrayType;
// nodes/pg_list.h
use crate::nodes::pg_list::{lappend, linitial, list_length, lfirst, List, NIL};
// miscadmin.h
use crate::utils::misc::stack_depth::check_stack_depth;
// varlena / varatt
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};

use crate::utils::fmgr::{FunctionCallInfo, PGFunction};
use crate::utils::elog::ERROR;
use crate::{
    elog, foreach, current_cell, DirectFunctionCall2, PG_GETARG_DATUM, PG_GETARG_INT32,
    PG_GETARG_POINTER, PG_GETARG_TEXT_PP, PG_GETARG_UINT16, PG_RETURN_BOOL,
    PG_RETURN_GIN_TERNARY_VALUE, PG_RETURN_INT32, PG_RETURN_POINTER,
};

// libc bindings (string.h / stdio.h, via postgres.h).
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strlen(s: *const c_char) -> usize;
    fn snprintf(s: *mut c_char, n: usize, format: *const c_char, ...) -> c_int;
}

// ---------------------------------------------------------------------------
// TODO(pg-port): varstr_cmp lives in utils/adt/varlena.c (not yet ported).
// ---------------------------------------------------------------------------
extern "C" {
    fn varstr_cmp(arg1: *const c_char, len1: c_int, arg2: *const c_char, len2: c_int, collid: Oid)
        -> int32;
}

// ---------------------------------------------------------------------------
// TODO(pg-port): GIN strategy numbers for jsonb live in utils/jsonb.h, whose
// Rust home (crate::utils::adt::jsonb) does not yet exist.  Defined here
// verbatim from utils/jsonb.h until that module exists.
// ---------------------------------------------------------------------------
const JsonbContainsStrategyNumber: StrategyNumber = 7;
const JsonbExistsStrategyNumber: StrategyNumber = 9;
const JsonbExistsAnyStrategyNumber: StrategyNumber = 10;
const JsonbExistsAllStrategyNumber: StrategyNumber = 11;
const JsonbJsonpathExistsStrategyNumber: StrategyNumber = 15;
const JsonbJsonpathPredicateStrategyNumber: StrategyNumber = 16;

// ---------------------------------------------------------------------------
// TODO(pg-port): jsonb_ops GIN key flag bits live in utils/jsonb.h (see above).
// Defined here verbatim from utils/jsonb.h.
// ---------------------------------------------------------------------------
const JGINFLAG_KEY: c_char = 0x01; /* key (or string array element) */
const JGINFLAG_NULL: c_char = 0x02; /* null value */
const JGINFLAG_BOOL: c_char = 0x03; /* boolean value */
const JGINFLAG_NUM: c_char = 0x04; /* numeric value */
const JGINFLAG_STR: c_char = 0x05; /* string value (if not an array element) */
const JGINFLAG_HASHED: c_char = 0x10; /* OR'd into flag if value was hashed */
const JGIN_MAXLENGTH: c_int = 125; /* max length of text part before hashing */

const VARHDRSZ: usize = core::mem::size_of::<int32>();

// ---------------------------------------------------------------------------
// TODO(pg-port): JB_ROOT_COUNT macro lives in utils/jsonb.h (see above).
// ---------------------------------------------------------------------------
const JB_CMASK: uint32 = 0x0FFFFFFF; /* mask for count field */

#[inline]
unsafe fn JB_ROOT_COUNT(jbp: *mut Jsonb) -> c_int {
    (if VARSIZE_ANY_EXHDR(jbp as *const c_char) != 0 {
        *(VARDATA(jbp as *const c_char) as *mut uint32) & JB_CMASK
    } else {
        0
    }) as c_int
}

// ---------------------------------------------------------------------------
// TODO(pg-port): JsonPath / JsonPathItem types and jsp* accessors live in
// utils/jsonpath.h (utils/adt/jsonpath.c), which is NOT yet ported.  They are
// defined here verbatim from utils/jsonpath.h so this file can translate 1:1.
// When jsonpath gets its own module, these should move there and be imported.
// ---------------------------------------------------------------------------

pub const JSONPATH_LAX: uint32 = 0x80000000;

/* On-disk jsonpath datum */
#[repr(C)]
pub struct JsonPath {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub header: uint32, /* version and flags (see below) */
    pub data: [c_char; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/*
 * All node's type of jsonpath expression.  Values for the scalar types match
 * the corresponding jbvType values (jsonb.h).
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JsonPathItemType {
    jpiNull = 0x0,    /* NULL literal */
    jpiString = 0x1,  /* string literal */
    jpiNumeric = 0x2, /* numeric literal */
    jpiBool = 0x3,    /* boolean literal: TRUE or FALSE */
    jpiAnd,           /* predicate && predicate */
    jpiOr,            /* predicate || predicate */
    jpiNot,           /* ! predicate */
    jpiIsUnknown,     /* (predicate) IS UNKNOWN */
    jpiEqual,         /* expr == expr */
    jpiNotEqual,      /* expr != expr */
    jpiLess,          /* expr < expr */
    jpiGreater,       /* expr > expr */
    jpiLessOrEqual,   /* expr <= expr */
    jpiGreaterOrEqual, /* expr >= expr */
    jpiAdd,           /* expr + expr */
    jpiSub,           /* expr - expr */
    jpiMul,           /* expr * expr */
    jpiDiv,           /* expr / expr */
    jpiMod,           /* expr % expr */
    jpiPlus,          /* + expr */
    jpiMinus,         /* - expr */
    jpiAnyArray,      /* [*] */
    jpiAnyKey,        /* .* */
    jpiIndexArray,    /* [subscript, ...] */
    jpiAny,           /* .** */
    jpiKey,           /* .key */
    jpiCurrent,       /* @ */
    jpiRoot,          /* $ */
    jpiVariable,      /* $variable */
    jpiFilter,        /* ? (predicate) */
    jpiExists,        /* EXISTS (expr) predicate */
    jpiType,          /* .type() item method */
    jpiSize,          /* .size() item method */
    jpiAbs,           /* .abs() item method */
    jpiFloor,         /* .floor() item method */
    jpiCeiling,       /* .ceiling() item method */
    jpiDouble,        /* .double() item method */
    jpiDatetime,      /* .datetime() item method */
    jpiKeyValue,      /* .keyvalue() item method */
    jpiSubscript,     /* array subscript: 'expr' or 'expr TO expr' */
    jpiLast,          /* LAST array subscript */
    jpiStartsWith,    /* STARTS WITH predicate */
    jpiLikeRegex,     /* LIKE_REGEX predicate */
    jpiBigint,        /* .bigint() item method */
    jpiBoolean,       /* .boolean() item method */
    jpiDate,          /* .date() item method */
    jpiDecimal,       /* .decimal() item method */
    jpiInteger,       /* .integer() item method */
    jpiNumber,        /* .number() item method */
    jpiStringFunc,    /* .string() item method */
    jpiTime,          /* .time() item method */
    jpiTimeTz,        /* .time_tz() item method */
    jpiTimestamp,     /* .timestamp() item method */
    jpiTimestampTz,   /* .timestamp_tz() item method */
}
pub use JsonPathItemType::*;

/* jspIsScalar(type): (type) >= jpiNull && (type) <= jpiBool */
#[inline]
fn jspIsScalar(t: JsonPathItemType) -> bool {
    t >= jpiNull && t <= jpiBool
}

/*
 * Binary representation of a jsonpath item.  Unlike many other expression
 * representations the first/main node is not an operation but the left operand
 * of the expression.
 */
#[repr(C)]
pub struct JsonPathItem {
    pub type_: JsonPathItemType,
    /* position from base to next node */
    pub nextPos: int32,
    /*
     * pointer into JsonPath value to current node, all positions of current
     * are relative to this base
     */
    pub base: *mut c_char,
    pub content: JsonPathItemContent,
}

#[repr(C)]
pub union JsonPathItemContent {
    pub args: JsonPathItemArgs,
    pub arg: int32,
    pub array: JsonPathItemArray,
    pub anybounds: JsonPathItemAnyBounds,
    pub value: JsonPathItemValue,
    pub like_regex: JsonPathItemLikeRegex,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathItemArgs {
    pub left: int32,
    pub right: int32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathItemArrayElems {
    pub from: int32,
    pub to: int32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathItemArray {
    pub nelems: int32,
    pub elems: *mut JsonPathItemArrayElems,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathItemAnyBounds {
    pub first: uint32,
    pub last: uint32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathItemValue {
    pub data: *mut c_char, /* for bool, numeric and string/key */
    pub datalen: int32,    /* filled only for string/key */
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathItemLikeRegex {
    pub expr: int32,
    pub pattern: *mut c_char,
    pub patternlen: int32,
    pub flags: uint32,
}

extern "C" {
    fn jspInit(v: *mut JsonPathItem, js: *mut JsonPath);
    fn jspGetNext(v: *mut JsonPathItem, a: *mut JsonPathItem) -> bool;
    fn jspGetArg(v: *mut JsonPathItem, a: *mut JsonPathItem);
    fn jspGetLeftArg(v: *mut JsonPathItem, a: *mut JsonPathItem);
    fn jspGetRightArg(v: *mut JsonPathItem, a: *mut JsonPathItem);
    fn jspGetString(v: *mut JsonPathItem, len: *mut int32) -> *mut c_char;
}

/* PG_GETARG_JSONPATH_P(x): DatumGetJsonPathP(PG_GETARG_DATUM(x)) (jsonpath.h) */
macro_rules! PG_GETARG_JSONPATH_P {
    ($fcinfo:expr, $n:expr) => {
        // TODO(pg-port): real DatumGetJsonPathP detoasts; jsonpath.c not ported.
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut JsonPath
    };
}

/* PG_GETARG_JSONB_P(x): DatumGetJsonbP(PG_GETARG_DATUM(x)) (jsonb.h) */
macro_rules! PG_GETARG_JSONB_P {
    ($fcinfo:expr, $n:expr) => {
        // TODO(pg-port): real DatumGetJsonbP detoasts; jsonb.h not ported.
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut Jsonb
    };
}

/* PG_GETARG_ARRAYTYPE_P(n): DatumGetArrayTypeP(PG_GETARG_DATUM(n)) (array.h) */
macro_rules! PG_GETARG_ARRAYTYPE_P {
    ($fcinfo:expr, $n:expr) => {
        // TODO(pg-port): real DatumGetArrayTypeP detoasts; helper not yet public.
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut ArrayType
    };
}

#[repr(C)]
struct PathHashStack {
    hash: uint32,
    parent: *mut PathHashStack,
}

/* Buffer for GIN entries */
#[repr(C)]
struct GinEntries {
    buf: *mut Datum,
    count: c_int,
    allocated: c_int,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum JsonPathGinNodeType {
    JSP_GIN_OR,
    JSP_GIN_AND,
    JSP_GIN_ENTRY,
}
use JsonPathGinNodeType::*;

/* Node in jsonpath expression tree */
#[repr(C)]
struct JsonPathGinNode {
    type_: JsonPathGinNodeType,
    val: JsonPathGinNodeVal,
    args: [*mut JsonPathGinNode; 0], /* FLEXIBLE_ARRAY_MEMBER, valid for OR and AND nodes */
}

#[repr(C)]
union JsonPathGinNodeVal {
    nargs: c_int,      /* valid for OR and AND nodes */
    entryIndex: c_int, /* index in GinEntries array, valid for ENTRY nodes after entries output */
    entryDatum: Datum, /* path hash or key name/scalar, valid for ENTRY nodes before entries output */
}

/*
 * jsonb_ops entry extracted from jsonpath item.  Corresponding path item
 * may be: '.key', '.*', '.**', '[index]' or '[*]'.
 * Entry type is stored in 'type' field.
 */
#[repr(C)]
struct JsonPathGinPathItem {
    parent: *mut JsonPathGinPathItem,
    keyName: Datum, /* key name (for '.key' path item) or NULL */
    type_: JsonPathItemType, /* type of jsonpath item */
}

/* GIN representation of the extracted json path */
#[repr(C)]
#[derive(Clone, Copy)]
union JsonPathGinPath {
    items: *mut JsonPathGinPathItem, /* list of path items (jsonb_ops) */
    hash: uint32,                    /* hash of the path (jsonb_path_ops) */
}

/* Callback, which stores information about path item into JsonPathGinPath */
type JsonPathGinAddPathItemFunc =
    unsafe fn(path: *mut JsonPathGinPath, jsp: *mut JsonPathItem) -> bool;

/*
 * Callback, which extracts set of nodes from statement of 1st kind
 * (scalar != NULL) or statement of 2nd kind (scalar == NULL).
 */
type JsonPathGinExtractNodesFunc = unsafe fn(
    cxt: *mut JsonPathGinContext,
    path: JsonPathGinPath,
    scalar: *mut JsonbValue,
    nodes: *mut List,
) -> *mut List;

/* Context for jsonpath entries extraction */
#[repr(C)]
struct JsonPathGinContext {
    add_path_item: JsonPathGinAddPathItemFunc,
    extract_nodes: JsonPathGinExtractNodesFunc,
    lax: bool,
}

/* Initialize GinEntries struct */
unsafe fn init_gin_entries(entries: *mut GinEntries, preallocated: c_int) {
    (*entries).allocated = preallocated;
    (*entries).buf = if preallocated != 0 {
        palloc(core::mem::size_of::<Datum>() * preallocated as usize) as *mut Datum
    } else {
        null_mut()
    };
    (*entries).count = 0;
}

/* Add new entry to GinEntries */
unsafe fn add_gin_entry(entries: *mut GinEntries, entry: Datum) -> c_int {
    let id: c_int = (*entries).count;

    if (*entries).count >= (*entries).allocated {
        if (*entries).allocated != 0 {
            (*entries).allocated *= 2;
            (*entries).buf = repalloc(
                (*entries).buf as *mut c_void,
                core::mem::size_of::<Datum>() * (*entries).allocated as usize,
            ) as *mut Datum;
        } else {
            (*entries).allocated = 8;
            (*entries).buf =
                palloc(core::mem::size_of::<Datum>() * (*entries).allocated as usize) as *mut Datum;
        }
    }

    *(*entries).buf.add((*entries).count as usize) = entry;
    (*entries).count += 1;

    id
}

/*
 *
 * jsonb_ops GIN opclass support functions
 *
 */

pub unsafe fn gin_compare_jsonb(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let arg2: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: int32;
    let a1p: *mut c_char;
    let a2p: *mut c_char;
    let len1: c_int;
    let len2: c_int;

    a1p = VARDATA_ANY(arg1 as *const c_char);
    a2p = VARDATA_ANY(arg2 as *const c_char);

    len1 = VARSIZE_ANY_EXHDR(arg1 as *const c_char) as c_int;
    len2 = VARSIZE_ANY_EXHDR(arg2 as *const c_char) as c_int;

    /* Compare text as bttextcmp does, but always using C collation */
    result = varstr_cmp(a1p, len1, a2p, len2, C_COLLATION_OID);

    // PG_FREE_IF_COPY(arg1, 0); PG_FREE_IF_COPY(arg2, 1);  -- no-ops here

    PG_RETURN_INT32!(result)
}

pub unsafe fn gin_extract_jsonb(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let nentries: *mut int32 = PG_GETARG_POINTER!(fcinfo, 1) as *mut int32;
    let total: c_int = JB_ROOT_COUNT(jb);
    let mut it: *mut JsonbIterator;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut r: JsonbIteratorToken;
    let mut entries: GinEntries = core::mem::zeroed();

    /* If the root level is empty, we certainly have no keys */
    if total == 0 {
        *nentries = 0;
        PG_RETURN_POINTER!(null_mut::<c_void>());
    }

    /* Otherwise, use 2 * root count as initial estimate of result size */
    init_gin_entries(&mut entries, 2 * total);

    it = JsonbIteratorInit(&mut (*jb).root);

    loop {
        r = JsonbIteratorNext(&mut it, &mut v, false);
        if r == WJB_DONE {
            break;
        }
        match r {
            WJB_KEY => {
                add_gin_entry(&mut entries, make_scalar_key(&v, true));
            }
            WJB_ELEM => {
                /* Pretend string array elements are keys, see jsonb.h */
                add_gin_entry(&mut entries, make_scalar_key(&v, v.type_ == jbvString));
            }
            WJB_VALUE => {
                add_gin_entry(&mut entries, make_scalar_key(&v, false));
            }
            _ => {
                /* we can ignore structural items */
            }
        }
    }

    *nentries = entries.count;

    PG_RETURN_POINTER!(entries.buf)
}

/* Append JsonPathGinPathItem to JsonPathGinPath (jsonb_ops) */
unsafe fn jsonb_ops__add_path_item(path: *mut JsonPathGinPath, jsp: *mut JsonPathItem) -> bool {
    let pentry: *mut JsonPathGinPathItem;
    let keyName: Datum;

    match (*jsp).type_ {
        jpiRoot => {
            (*path).items = null_mut(); /* reset path */
            return true;
        }

        jpiKey => {
            let mut len: c_int = 0;
            let key: *mut c_char = jspGetString(jsp, &mut len);

            keyName = make_text_key(JGINFLAG_KEY, key, len);
        }

        jpiAny | jpiAnyKey | jpiAnyArray | jpiIndexArray => {
            keyName = PointerGetDatum(null());
        }

        _ => {
            /* other path items like item methods are not supported */
            return false;
        }
    }

    pentry = palloc(core::mem::size_of::<JsonPathGinPathItem>()) as *mut JsonPathGinPathItem;

    (*pentry).type_ = (*jsp).type_;
    (*pentry).keyName = keyName;
    (*pentry).parent = (*path).items;

    (*path).items = pentry;

    true
}

/* Combine existing path hash with next key hash (jsonb_path_ops) */
unsafe fn jsonb_path_ops__add_path_item(
    path: *mut JsonPathGinPath,
    jsp: *mut JsonPathItem,
) -> bool {
    match (*jsp).type_ {
        jpiRoot => {
            (*path).hash = 0; /* reset path hash */
            true
        }

        jpiKey => {
            let mut jbv: JsonbValue = core::mem::zeroed();

            jbv.type_ = jbvString;
            jbv.val.string.val = jspGetString(jsp, &mut jbv.val.string.len);

            JsonbHashScalarValue(&jbv, &mut (*path).hash);
            true
        }

        jpiIndexArray | jpiAnyArray => true, /* path hash is unchanged */

        _ => {
            /* other items (wildcard paths, item methods) are not supported */
            false
        }
    }
}

unsafe fn make_jsp_entry_node(entry: Datum) -> *mut JsonPathGinNode {
    let node: *mut JsonPathGinNode =
        palloc(core::mem::offset_of!(JsonPathGinNode, args)) as *mut JsonPathGinNode;

    (*node).type_ = JSP_GIN_ENTRY;
    (*node).val.entryDatum = entry;

    node
}

unsafe fn make_jsp_entry_node_scalar(scalar: *mut JsonbValue, iskey: bool) -> *mut JsonPathGinNode {
    make_jsp_entry_node(make_scalar_key(scalar, iskey))
}

unsafe fn make_jsp_expr_node(type_: JsonPathGinNodeType, nargs: c_int) -> *mut JsonPathGinNode {
    let node: *mut JsonPathGinNode = palloc(
        core::mem::offset_of!(JsonPathGinNode, args)
            + core::mem::size_of::<*mut JsonPathGinNode>() * nargs as usize,
    ) as *mut JsonPathGinNode;

    (*node).type_ = type_;
    (*node).val.nargs = nargs;

    node
}

unsafe fn make_jsp_expr_node_args(
    type_: JsonPathGinNodeType,
    args: *mut List,
) -> *mut JsonPathGinNode {
    let node: *mut JsonPathGinNode = make_jsp_expr_node(type_, list_length(args));
    let mut i: c_int = 0;

    foreach!(lc, args, {
        *(*node).args.as_mut_ptr().add(i as usize) =
            lfirst(current_cell!(lc)) as *mut JsonPathGinNode;
        i += 1;
    });

    node
}

unsafe fn make_jsp_expr_node_binary(
    type_: JsonPathGinNodeType,
    arg1: *mut JsonPathGinNode,
    arg2: *mut JsonPathGinNode,
) -> *mut JsonPathGinNode {
    let node: *mut JsonPathGinNode = make_jsp_expr_node(type_, 2);

    *(*node).args.as_mut_ptr().add(0) = arg1;
    *(*node).args.as_mut_ptr().add(1) = arg2;

    node
}

/* Append a list of nodes from the jsonpath (jsonb_ops). */
unsafe fn jsonb_ops__extract_nodes(
    cxt: *mut JsonPathGinContext,
    path: JsonPathGinPath,
    scalar: *mut JsonbValue,
    mut nodes: *mut List,
) -> *mut List {
    let mut pentry: *mut JsonPathGinPathItem;

    if !scalar.is_null() {
        let node: *mut JsonPathGinNode;

        /*
         * Append path entry nodes only if scalar is provided.  See header
         * comment for details.
         */
        pentry = path.items;
        while !pentry.is_null() {
            if (*pentry).type_ == jpiKey {
                /* only keys are indexed */
                nodes = lappend(nodes, make_jsp_entry_node((*pentry).keyName) as *mut c_void);
            }
            pentry = (*pentry).parent;
        }

        /* Append scalar node for equality queries. */
        if (*scalar).type_ == jbvString {
            let last: *mut JsonPathGinPathItem = path.items;
            let key_entry: GinTernaryValue;

            /*
             * Assuming that jsonb_ops interprets string array elements as
             * keys, we may extract key or non-key entry or even both.  In the
             * latter case we create OR-node.  It is possible in lax mode
             * where arrays are automatically unwrapped, or in strict mode for
             * jpiAny items.
             */

            if (*cxt).lax {
                key_entry = GIN_MAYBE;
            } else if last.is_null() {
                /* root ($) */
                key_entry = GIN_FALSE;
            } else if (*last).type_ == jpiAnyArray || (*last).type_ == jpiIndexArray {
                key_entry = GIN_TRUE;
            } else if (*last).type_ == jpiAny {
                key_entry = GIN_MAYBE;
            } else {
                key_entry = GIN_FALSE;
            }

            if key_entry == GIN_MAYBE {
                let n1: *mut JsonPathGinNode = make_jsp_entry_node_scalar(scalar, true);
                let n2: *mut JsonPathGinNode = make_jsp_entry_node_scalar(scalar, false);

                node = make_jsp_expr_node_binary(JSP_GIN_OR, n1, n2);
            } else {
                node = make_jsp_entry_node_scalar(scalar, key_entry == GIN_TRUE);
            }
        } else {
            node = make_jsp_entry_node_scalar(scalar, false);
        }

        nodes = lappend(nodes, node as *mut c_void);
    }

    nodes
}

/* Append a list of nodes from the jsonpath (jsonb_path_ops). */
unsafe fn jsonb_path_ops__extract_nodes(
    _cxt: *mut JsonPathGinContext,
    path: JsonPathGinPath,
    scalar: *mut JsonbValue,
    nodes: *mut List,
) -> *mut List {
    if !scalar.is_null() {
        /* append path hash node for equality queries */
        let mut hash: uint32 = path.hash;

        JsonbHashScalarValue(scalar, &mut hash);

        lappend(
            nodes,
            make_jsp_entry_node(UInt32GetDatum(hash)) as *mut c_void,
        )
    } else {
        /* jsonb_path_ops doesn't support EXISTS queries => nothing to append */
        nodes
    }
}

/*
 * Extract a list of expression nodes that need to be AND-ed by the caller.
 * Extracted expression is 'path == scalar' if 'scalar' is non-NULL, and
 * 'EXISTS(path)' otherwise.
 */
unsafe fn extract_jsp_path_expr_nodes(
    cxt: *mut JsonPathGinContext,
    mut path: JsonPathGinPath,
    mut jsp: *mut JsonPathItem,
    scalar: *mut JsonbValue,
) -> *mut List {
    let mut next: JsonPathItem = core::mem::zeroed();
    let mut nodes: *mut List = NIL;

    loop {
        match (*jsp).type_ {
            jpiCurrent => {}

            jpiFilter => {
                let mut arg: JsonPathItem = core::mem::zeroed();
                let filter: *mut JsonPathGinNode;

                jspGetArg(jsp, &mut arg);

                filter = extract_jsp_bool_expr(cxt, path, &mut arg, false);

                if !filter.is_null() {
                    nodes = lappend(nodes, filter as *mut c_void);
                }
            }

            _ => {
                if !((*cxt).add_path_item)(&mut path, jsp) {
                    /*
                     * Path is not supported by the index opclass, return only
                     * the extracted filter nodes.
                     */
                    return nodes;
                }
            }
        }

        if !jspGetNext(jsp, &mut next) {
            break;
        }

        jsp = &mut next;
    }

    /*
     * Append nodes from the path expression itself to the already extracted
     * list of filter nodes.
     */
    ((*cxt).extract_nodes)(cxt, path, scalar, nodes)
}

/*
 * Extract an expression node from one of following jsonpath path expressions:
 *   EXISTS(jsp)    (when 'scalar' is NULL)
 *   jsp == scalar  (when 'scalar' is not NULL).
 *
 * The current path (@) is passed in 'path'.
 */
unsafe fn extract_jsp_path_expr(
    cxt: *mut JsonPathGinContext,
    path: JsonPathGinPath,
    jsp: *mut JsonPathItem,
    scalar: *mut JsonbValue,
) -> *mut JsonPathGinNode {
    /* extract a list of nodes to be AND-ed */
    let nodes: *mut List = extract_jsp_path_expr_nodes(cxt, path, jsp, scalar);

    if nodes == NIL {
        /* no nodes were extracted => full scan is needed for this path */
        return null_mut();
    }

    if list_length(nodes) == 1 {
        return linitial(nodes) as *mut JsonPathGinNode; /* avoid extra AND-node */
    }

    /* construct AND-node for path with filters */
    make_jsp_expr_node_args(JSP_GIN_AND, nodes)
}

/* Recursively extract nodes from the boolean jsonpath expression. */
unsafe fn extract_jsp_bool_expr(
    cxt: *mut JsonPathGinContext,
    path: JsonPathGinPath,
    jsp: *mut JsonPathItem,
    not: bool,
) -> *mut JsonPathGinNode {
    check_stack_depth();

    match (*jsp).type_ {
        jpiAnd | jpiOr => {
            /* expr && expr  /  expr || expr */
            let mut arg: JsonPathItem = core::mem::zeroed();
            let larg: *mut JsonPathGinNode;
            let rarg: *mut JsonPathGinNode;
            let type_: JsonPathGinNodeType;

            jspGetLeftArg(jsp, &mut arg);
            larg = extract_jsp_bool_expr(cxt, path, &mut arg, not);

            jspGetRightArg(jsp, &mut arg);
            rarg = extract_jsp_bool_expr(cxt, path, &mut arg, not);

            if larg.is_null() || rarg.is_null() {
                if (*jsp).type_ == jpiOr {
                    return null_mut();
                }

                return if !larg.is_null() { larg } else { rarg };
            }

            type_ = if not ^ ((*jsp).type_ == jpiAnd) {
                JSP_GIN_AND
            } else {
                JSP_GIN_OR
            };

            make_jsp_expr_node_binary(type_, larg, rarg)
        }

        jpiNot => {
            /* !expr */
            let mut arg: JsonPathItem = core::mem::zeroed();

            jspGetArg(jsp, &mut arg);

            /* extract child expression inverting 'not' flag */
            extract_jsp_bool_expr(cxt, path, &mut arg, !not)
        }

        jpiExists => {
            /* EXISTS(path) */
            let mut arg: JsonPathItem = core::mem::zeroed();

            if not {
                return null_mut(); /* NOT EXISTS is not supported */
            }

            jspGetArg(jsp, &mut arg);

            extract_jsp_path_expr(cxt, path, &mut arg, null_mut())
        }

        jpiNotEqual => {
            /*
             * 'not' == true case is not supported here because '!(path !=
             * scalar)' is not equivalent to 'path == scalar' in the general
             * case because of sequence comparison semantics: 'path == scalar'
             * === 'EXISTS (path, @ == scalar)', '!(path != scalar)' ===
             * 'FOR_ALL(path, @ == scalar)'. So, we should translate '!(path
             * != scalar)' into GIN query 'path == scalar || EMPTY(path)', but
             * 'EMPTY(path)' queries are not supported by the both jsonb
             * opclasses.  However in strict mode we could omit 'EMPTY(path)'
             * part if the path can return exactly one item (it does not
             * contain wildcard accessors or item methods like .keyvalue()
             * etc.).
             */
            null_mut()
        }

        jpiEqual => {
            /* path == scalar */
            let mut left_item: JsonPathItem = core::mem::zeroed();
            let mut right_item: JsonPathItem = core::mem::zeroed();
            let path_item: *mut JsonPathItem;
            let scalar_item: *mut JsonPathItem;
            let mut scalar: JsonbValue = core::mem::zeroed();

            if not {
                return null_mut();
            }

            jspGetLeftArg(jsp, &mut left_item);
            jspGetRightArg(jsp, &mut right_item);

            if jspIsScalar(left_item.type_) {
                scalar_item = &mut left_item;
                path_item = &mut right_item;
            } else if jspIsScalar(right_item.type_) {
                scalar_item = &mut right_item;
                path_item = &mut left_item;
            } else {
                return null_mut(); /* at least one operand should be a scalar */
            }

            match (*scalar_item).type_ {
                jpiNull => {
                    scalar.type_ = jbvNull;
                }
                jpiBool => {
                    scalar.type_ = jbvBool;
                    scalar.val.boolean = *(*scalar_item).content.value.data != 0;
                }
                jpiNumeric => {
                    scalar.type_ = jbvNumeric;
                    scalar.val.numeric = (*scalar_item).content.value.data as Numeric;
                }
                jpiString => {
                    scalar.type_ = jbvString;
                    scalar.val.string.val = (*scalar_item).content.value.data;
                    scalar.val.string.len = (*scalar_item).content.value.datalen;
                }
                _ => {
                    elog!(
                        ERROR,
                        "invalid scalar jsonpath item type: {}",
                        (*scalar_item).type_ as c_int
                    );
                    return null_mut();
                }
            }

            extract_jsp_path_expr(cxt, path, path_item, &mut scalar)
        }

        _ => {
            null_mut() /* not a boolean expression */
        }
    }
}

/* Recursively emit all GIN entries found in the node tree */
unsafe fn emit_jsp_gin_entries(node: *mut JsonPathGinNode, entries: *mut GinEntries) {
    check_stack_depth();

    match (*node).type_ {
        JSP_GIN_ENTRY => {
            /* replace datum with its index in the array */
            (*node).val.entryIndex = add_gin_entry(entries, (*node).val.entryDatum);
        }

        JSP_GIN_OR | JSP_GIN_AND => {
            let mut i: c_int = 0;
            while i < (*node).val.nargs {
                emit_jsp_gin_entries(*(*node).args.as_ptr().add(i as usize), entries);
                i += 1;
            }
        }
    }
}

/*
 * Recursively extract GIN entries from jsonpath query.
 * Root expression node is put into (*extra_data)[0].
 */
#[allow(invalid_value)]
unsafe fn extract_jsp_query(
    jp: *mut JsonPath,
    strat: StrategyNumber,
    pathOps: bool,
    nentries: *mut int32,
    extra_data: *mut *mut Pointer,
) -> *mut Datum {
    let mut cxt: JsonPathGinContext = core::mem::zeroed();
    let mut root: JsonPathItem = core::mem::zeroed();
    let node: *mut JsonPathGinNode;
    let path: JsonPathGinPath = JsonPathGinPath { items: null_mut() };
    let mut entries: GinEntries = core::mem::zeroed();

    cxt.lax = ((*jp).header & JSONPATH_LAX) != 0;

    if pathOps {
        cxt.add_path_item = jsonb_path_ops__add_path_item;
        cxt.extract_nodes = jsonb_path_ops__extract_nodes;
    } else {
        cxt.add_path_item = jsonb_ops__add_path_item;
        cxt.extract_nodes = jsonb_ops__extract_nodes;
    }

    jspInit(&mut root, jp);

    node = if strat == JsonbJsonpathExistsStrategyNumber {
        extract_jsp_path_expr(&mut cxt, path, &mut root, null_mut())
    } else {
        extract_jsp_bool_expr(&mut cxt, path, &mut root, false)
    };

    if node.is_null() {
        *nentries = 0;
        return null_mut();
    }

    emit_jsp_gin_entries(node, &mut entries);

    *nentries = entries.count;
    if *nentries == 0 {
        return null_mut();
    }

    *extra_data = palloc0(core::mem::size_of::<Pointer>() * entries.count as usize) as *mut Pointer;
    **extra_data = node as Pointer;

    entries.buf
}

/*
 * Recursively execute jsonpath expression.
 * 'check' is a bool[] or a GinTernaryValue[] depending on 'ternary' flag.
 */
unsafe fn execute_jsp_gin_node(
    node: *mut JsonPathGinNode,
    check: *mut c_void,
    ternary: bool,
) -> GinTernaryValue {
    let mut res: GinTernaryValue;
    let v: GinTernaryValue;
    let mut i: c_int;

    match (*node).type_ {
        JSP_GIN_AND => {
            res = GIN_TRUE;
            i = 0;
            while i < (*node).val.nargs {
                let vv =
                    execute_jsp_gin_node(*(*node).args.as_ptr().add(i as usize), check, ternary);
                if vv == GIN_FALSE {
                    return GIN_FALSE;
                } else if vv == GIN_MAYBE {
                    res = GIN_MAYBE;
                }
                i += 1;
            }
            res
        }

        JSP_GIN_OR => {
            res = GIN_FALSE;
            i = 0;
            while i < (*node).val.nargs {
                let vv =
                    execute_jsp_gin_node(*(*node).args.as_ptr().add(i as usize), check, ternary);
                if vv == GIN_TRUE {
                    return GIN_TRUE;
                } else if vv == GIN_MAYBE {
                    res = GIN_MAYBE;
                }
                i += 1;
            }
            res
        }

        JSP_GIN_ENTRY => {
            let index: c_int = (*node).val.entryIndex;

            if ternary {
                v = *(check as *mut GinTernaryValue).add(index as usize);
                v
            } else {
                if *(check as *mut bool).add(index as usize) {
                    GIN_TRUE
                } else {
                    GIN_FALSE
                }
            }
        }
    }
}

pub unsafe fn gin_extract_jsonb_query(fcinfo: FunctionCallInfo) -> Datum {
    let nentries: *mut int32 = PG_GETARG_POINTER!(fcinfo, 1) as *mut int32;
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 2);
    let searchMode: *mut int32 = PG_GETARG_POINTER!(fcinfo, 6) as *mut int32;
    let entries: *mut Datum;

    if strategy == JsonbContainsStrategyNumber {
        /* Query is a jsonb, so just apply gin_extract_jsonb... */
        entries = DatumGetPointer(DirectFunctionCall2!(
            gin_extract_jsonb as PGFunction,
            PG_GETARG_DATUM!(fcinfo, 0),
            PointerGetDatum(nentries as *const c_void)
        )) as *mut Datum;
        /* ...although "contains {}" requires a full index scan */
        if *nentries == 0 {
            *searchMode = GIN_SEARCH_MODE_ALL;
        }
    } else if strategy == JsonbExistsStrategyNumber {
        /* Query is a text string, which we treat as a key */
        let query: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);

        *nentries = 1;
        entries = palloc(core::mem::size_of::<Datum>()) as *mut Datum;
        *entries.add(0) = make_text_key(
            JGINFLAG_KEY,
            VARDATA_ANY(query as *const c_char),
            VARSIZE_ANY_EXHDR(query as *const c_char) as c_int,
        );
    } else if strategy == JsonbExistsAnyStrategyNumber || strategy == JsonbExistsAllStrategyNumber {
        /* Query is a text array; each element is treated as a key */
        let query: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
        let mut key_datums: *mut Datum = null_mut();
        let mut key_nulls: *mut bool = null_mut();
        let mut key_count: c_int = 0;
        let mut i: c_int;
        let mut j: c_int;

        deconstruct_array_builtin(
            query,
            TEXTOID,
            &mut key_datums,
            &mut key_nulls,
            &mut key_count,
        );

        entries = palloc(core::mem::size_of::<Datum>() * key_count as usize) as *mut Datum;

        i = 0;
        j = 0;
        while i < key_count {
            /* Nulls in the array are ignored */
            if *key_nulls.add(i as usize) {
                i += 1;
                continue;
            }
            /* We rely on the array elements not being toasted */
            let kd = *key_datums.add(i as usize);
            *entries.add(j as usize) = make_text_key(
                JGINFLAG_KEY,
                VARDATA_ANY(DatumGetPointer(kd)),
                VARSIZE_ANY_EXHDR(DatumGetPointer(kd)) as c_int,
            );
            j += 1;
            i += 1;
        }

        *nentries = j;
        /* ExistsAll with no keys should match everything */
        if j == 0 && strategy == JsonbExistsAllStrategyNumber {
            *searchMode = GIN_SEARCH_MODE_ALL;
        }
    } else if strategy == JsonbJsonpathPredicateStrategyNumber
        || strategy == JsonbJsonpathExistsStrategyNumber
    {
        let jp: *mut JsonPath = PG_GETARG_JSONPATH_P!(fcinfo, 0);
        let extra_data: *mut *mut Pointer = PG_GETARG_POINTER!(fcinfo, 4) as *mut *mut Pointer;

        entries = extract_jsp_query(jp, strategy, false, nentries, extra_data);

        if entries.is_null() {
            *searchMode = GIN_SEARCH_MODE_ALL;
        }
    } else {
        elog!(ERROR, "unrecognized strategy number: {}", strategy);
        entries = null_mut(); /* keep compiler quiet */
    }

    PG_RETURN_POINTER!(entries)
}

pub unsafe fn gin_consistent_jsonb(fcinfo: FunctionCallInfo) -> Datum {
    let check: *mut bool = PG_GETARG_POINTER!(fcinfo, 0) as *mut bool;
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 1);

    /* Jsonb	   *query = PG_GETARG_JSONB_P(2); */
    let nkeys: int32 = PG_GETARG_INT32!(fcinfo, 3);

    let extra_data: *mut Pointer = PG_GETARG_POINTER!(fcinfo, 4) as *mut Pointer;
    let recheck: *mut bool = PG_GETARG_POINTER!(fcinfo, 5) as *mut bool;
    let mut res: bool = true;
    let mut i: int32;

    if strategy == JsonbContainsStrategyNumber {
        /*
         * We must always recheck, since we can't tell from the index whether
         * the positions of the matched items match the structure of the query
         * object.  (Even if we could, we'd also have to worry about hashed
         * keys and the index's failure to distinguish keys from string array
         * elements.)  However, the tuple certainly doesn't match unless it
         * contains all the query keys.
         */
        *recheck = true;
        i = 0;
        while i < nkeys {
            if !*check.add(i as usize) {
                res = false;
                break;
            }
            i += 1;
        }
    } else if strategy == JsonbExistsStrategyNumber {
        /*
         * Although the key is certainly present in the index, we must recheck
         * because (1) the key might be hashed, and (2) the index match might
         * be for a key that's not at top level of the JSON object.  For (1),
         * we could look at the query key to see if it's hashed and not
         * recheck if not, but the index lacks enough info to tell about (2).
         */
        *recheck = true;
        res = true;
    } else if strategy == JsonbExistsAnyStrategyNumber {
        /* As for plain exists, we must recheck */
        *recheck = true;
        res = true;
    } else if strategy == JsonbExistsAllStrategyNumber {
        /* As for plain exists, we must recheck */
        *recheck = true;
        /* ... but unless all the keys are present, we can say "false" */
        i = 0;
        while i < nkeys {
            if !*check.add(i as usize) {
                res = false;
                break;
            }
            i += 1;
        }
    } else if strategy == JsonbJsonpathPredicateStrategyNumber
        || strategy == JsonbJsonpathExistsStrategyNumber
    {
        *recheck = true;

        if nkeys > 0 {
            Assert!(!extra_data.is_null() && !(*extra_data).is_null());
            res = execute_jsp_gin_node(
                *extra_data as *mut JsonPathGinNode,
                check as *mut c_void,
                false,
            ) != GIN_FALSE;
        }
    } else {
        elog!(ERROR, "unrecognized strategy number: {}", strategy);
    }

    PG_RETURN_BOOL!(res)
}

pub unsafe fn gin_triconsistent_jsonb(fcinfo: FunctionCallInfo) -> Datum {
    let check: *mut GinTernaryValue = PG_GETARG_POINTER!(fcinfo, 0) as *mut GinTernaryValue;
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 1);

    /* Jsonb	   *query = PG_GETARG_JSONB_P(2); */
    let nkeys: int32 = PG_GETARG_INT32!(fcinfo, 3);
    let extra_data: *mut Pointer = PG_GETARG_POINTER!(fcinfo, 4) as *mut Pointer;
    let mut res: GinTernaryValue = GIN_MAYBE;
    let mut i: int32;

    /*
     * Note that we never return GIN_TRUE, only GIN_MAYBE or GIN_FALSE; this
     * corresponds to always forcing recheck in the regular consistent
     * function, for the reasons listed there.
     */
    if strategy == JsonbContainsStrategyNumber || strategy == JsonbExistsAllStrategyNumber {
        /* All extracted keys must be present */
        i = 0;
        while i < nkeys {
            if *check.add(i as usize) == GIN_FALSE {
                res = GIN_FALSE;
                break;
            }
            i += 1;
        }
    } else if strategy == JsonbExistsStrategyNumber || strategy == JsonbExistsAnyStrategyNumber {
        /* At least one extracted key must be present */
        res = GIN_FALSE;
        i = 0;
        while i < nkeys {
            if *check.add(i as usize) == GIN_TRUE || *check.add(i as usize) == GIN_MAYBE {
                res = GIN_MAYBE;
                break;
            }
            i += 1;
        }
    } else if strategy == JsonbJsonpathPredicateStrategyNumber
        || strategy == JsonbJsonpathExistsStrategyNumber
    {
        if nkeys > 0 {
            Assert!(!extra_data.is_null() && !(*extra_data).is_null());
            res = execute_jsp_gin_node(
                *extra_data as *mut JsonPathGinNode,
                check as *mut c_void,
                true,
            );

            /* Should always recheck the result */
            if res == GIN_TRUE {
                res = GIN_MAYBE;
            }
        }
    } else {
        elog!(ERROR, "unrecognized strategy number: {}", strategy);
    }

    PG_RETURN_GIN_TERNARY_VALUE!(res)
}

/*
 *
 * jsonb_path_ops GIN opclass support functions
 *
 * In a jsonb_path_ops index, the GIN keys are uint32 hashes, one per JSON
 * value; but the JSON key(s) leading to each value are also included in its
 * hash computation.  This means we can only support containment queries,
 * but the index can distinguish, for example, {"foo": 42} from {"bar": 42}
 * since different hashes will be generated.
 *
 */

pub unsafe fn gin_extract_jsonb_path(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let nentries: *mut int32 = PG_GETARG_POINTER!(fcinfo, 1) as *mut int32;
    let total: c_int = JB_ROOT_COUNT(jb);
    let mut it: *mut JsonbIterator;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut r: JsonbIteratorToken;
    let mut tail: PathHashStack = core::mem::zeroed();
    let mut stack: *mut PathHashStack;
    let mut entries: GinEntries = core::mem::zeroed();

    /* If the root level is empty, we certainly have no keys */
    if total == 0 {
        *nentries = 0;
        PG_RETURN_POINTER!(null_mut::<c_void>());
    }

    /* Otherwise, use 2 * root count as initial estimate of result size */
    init_gin_entries(&mut entries, 2 * total);

    /* We keep a stack of partial hashes corresponding to parent key levels */
    tail.parent = null_mut();
    tail.hash = 0;
    stack = &mut tail;

    it = JsonbIteratorInit(&mut (*jb).root);

    loop {
        r = JsonbIteratorNext(&mut it, &mut v, false);
        if r == WJB_DONE {
            break;
        }
        let parent: *mut PathHashStack;

        match r {
            WJB_BEGIN_ARRAY | WJB_BEGIN_OBJECT => {
                /* Push a stack level for this object */
                parent = stack;
                stack = palloc(core::mem::size_of::<PathHashStack>()) as *mut PathHashStack;

                /*
                 * We pass forward hashes from outer nesting levels so that
                 * the hashes for nested values will include outer keys as
                 * well as their own keys.
                 *
                 * Nesting an array within another array will not alter
                 * innermost scalar element hash values, but that seems
                 * inconsequential.
                 */
                (*stack).hash = (*parent).hash;
                (*stack).parent = parent;
            }
            WJB_KEY => {
                /* mix this key into the current outer hash */
                JsonbHashScalarValue(&v, &mut (*stack).hash);
                /* hash is now ready to incorporate the value */
            }
            WJB_ELEM | WJB_VALUE => {
                /* mix the element or value's hash into the prepared hash */
                JsonbHashScalarValue(&v, &mut (*stack).hash);
                /* and emit an index entry */
                add_gin_entry(&mut entries, UInt32GetDatum((*stack).hash));
                /* reset hash for next key, value, or sub-object */
                (*stack).hash = (*(*stack).parent).hash;
            }
            WJB_END_ARRAY | WJB_END_OBJECT => {
                /* Pop the stack */
                parent = (*stack).parent;
                pfree(stack as *mut c_void);
                stack = parent;
                /* reset hash for next key, value, or sub-object */
                if !(*stack).parent.is_null() {
                    (*stack).hash = (*(*stack).parent).hash;
                } else {
                    (*stack).hash = 0;
                }
            }
            _ => {
                elog!(ERROR, "invalid JsonbIteratorNext rc: {}", r as c_int);
            }
        }
    }

    *nentries = entries.count;

    PG_RETURN_POINTER!(entries.buf)
}

pub unsafe fn gin_extract_jsonb_query_path(fcinfo: FunctionCallInfo) -> Datum {
    let nentries: *mut int32 = PG_GETARG_POINTER!(fcinfo, 1) as *mut int32;
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 2);
    let searchMode: *mut int32 = PG_GETARG_POINTER!(fcinfo, 6) as *mut int32;
    let entries: *mut Datum;

    if strategy == JsonbContainsStrategyNumber {
        /* Query is a jsonb, so just apply gin_extract_jsonb_path ... */
        entries = DatumGetPointer(DirectFunctionCall2!(
            gin_extract_jsonb_path as PGFunction,
            PG_GETARG_DATUM!(fcinfo, 0),
            PointerGetDatum(nentries as *const c_void)
        )) as *mut Datum;

        /* ... although "contains {}" requires a full index scan */
        if *nentries == 0 {
            *searchMode = GIN_SEARCH_MODE_ALL;
        }
    } else if strategy == JsonbJsonpathPredicateStrategyNumber
        || strategy == JsonbJsonpathExistsStrategyNumber
    {
        let jp: *mut JsonPath = PG_GETARG_JSONPATH_P!(fcinfo, 0);
        let extra_data: *mut *mut Pointer = PG_GETARG_POINTER!(fcinfo, 4) as *mut *mut Pointer;

        entries = extract_jsp_query(jp, strategy, true, nentries, extra_data);

        if entries.is_null() {
            *searchMode = GIN_SEARCH_MODE_ALL;
        }
    } else {
        elog!(ERROR, "unrecognized strategy number: {}", strategy);
        entries = null_mut();
    }

    PG_RETURN_POINTER!(entries)
}

pub unsafe fn gin_consistent_jsonb_path(fcinfo: FunctionCallInfo) -> Datum {
    let check: *mut bool = PG_GETARG_POINTER!(fcinfo, 0) as *mut bool;
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 1);

    /* Jsonb	   *query = PG_GETARG_JSONB_P(2); */
    let nkeys: int32 = PG_GETARG_INT32!(fcinfo, 3);
    let extra_data: *mut Pointer = PG_GETARG_POINTER!(fcinfo, 4) as *mut Pointer;
    let recheck: *mut bool = PG_GETARG_POINTER!(fcinfo, 5) as *mut bool;
    let mut res: bool = true;
    let mut i: int32;

    if strategy == JsonbContainsStrategyNumber {
        /*
         * jsonb_path_ops is necessarily lossy, not only because of hash
         * collisions but also because it doesn't preserve complete
         * information about the structure of the JSON object.  Besides, there
         * are some special rules around the containment of raw scalars in
         * arrays that are not handled here.  So we must always recheck a
         * match.  However, if not all of the keys are present, the tuple
         * certainly doesn't match.
         */
        *recheck = true;
        i = 0;
        while i < nkeys {
            if !*check.add(i as usize) {
                res = false;
                break;
            }
            i += 1;
        }
    } else if strategy == JsonbJsonpathPredicateStrategyNumber
        || strategy == JsonbJsonpathExistsStrategyNumber
    {
        *recheck = true;

        if nkeys > 0 {
            Assert!(!extra_data.is_null() && !(*extra_data).is_null());
            res = execute_jsp_gin_node(
                *extra_data as *mut JsonPathGinNode,
                check as *mut c_void,
                false,
            ) != GIN_FALSE;
        }
    } else {
        elog!(ERROR, "unrecognized strategy number: {}", strategy);
    }

    PG_RETURN_BOOL!(res)
}

pub unsafe fn gin_triconsistent_jsonb_path(fcinfo: FunctionCallInfo) -> Datum {
    let check: *mut GinTernaryValue = PG_GETARG_POINTER!(fcinfo, 0) as *mut GinTernaryValue;
    let strategy: StrategyNumber = PG_GETARG_UINT16!(fcinfo, 1);

    /* Jsonb	   *query = PG_GETARG_JSONB_P(2); */
    let nkeys: int32 = PG_GETARG_INT32!(fcinfo, 3);
    let extra_data: *mut Pointer = PG_GETARG_POINTER!(fcinfo, 4) as *mut Pointer;
    let mut res: GinTernaryValue = GIN_MAYBE;
    let mut i: int32;

    if strategy == JsonbContainsStrategyNumber {
        /*
         * Note that we never return GIN_TRUE, only GIN_MAYBE or GIN_FALSE;
         * this corresponds to always forcing recheck in the regular
         * consistent function, for the reasons listed there.
         */
        i = 0;
        while i < nkeys {
            if *check.add(i as usize) == GIN_FALSE {
                res = GIN_FALSE;
                break;
            }
            i += 1;
        }
    } else if strategy == JsonbJsonpathPredicateStrategyNumber
        || strategy == JsonbJsonpathExistsStrategyNumber
    {
        if nkeys > 0 {
            Assert!(!extra_data.is_null() && !(*extra_data).is_null());
            res = execute_jsp_gin_node(
                *extra_data as *mut JsonPathGinNode,
                check as *mut c_void,
                true,
            );

            /* Should always recheck the result */
            if res == GIN_TRUE {
                res = GIN_MAYBE;
            }
        }
    } else {
        elog!(ERROR, "unrecognized strategy number: {}", strategy);
    }

    PG_RETURN_GIN_TERNARY_VALUE!(res)
}

/*
 * Construct a jsonb_ops GIN key from a flag byte and a textual representation
 * (which need not be null-terminated).  This function is responsible
 * for hashing overlength text representations; it will add the
 * JGINFLAG_HASHED bit to the flag value if it does that.
 */
unsafe fn make_text_key(mut flag: c_char, mut str: *const c_char, mut len: c_int) -> Datum {
    let item: *mut text;
    let mut hashbuf: [c_char; 10] = [0; 10];

    if len > JGIN_MAXLENGTH {
        let hashval: uint32;

        hashval = DatumGetUInt32(hash_any(str as *const c_uchar, len));
        snprintf(
            hashbuf.as_mut_ptr(),
            core::mem::size_of_val(&hashbuf),
            c"%08x".as_ptr(),
            hashval,
        );
        str = hashbuf.as_ptr();
        len = 8;
        flag |= JGINFLAG_HASHED;
    }

    /*
     * Now build the text Datum.  For simplicity we build a 4-byte-header
     * varlena text Datum here, but we expect it will get converted to short
     * header format when stored in the index.
     */
    item = palloc(VARHDRSZ + len as usize + 1) as *mut text;
    SET_VARSIZE(item as *mut c_char, (VARHDRSZ + len as usize + 1) as int32);

    *VARDATA(item as *const c_char) = flag;

    memcpy(
        VARDATA(item as *const c_char).add(1) as *mut c_void,
        str as *const c_void,
        len as usize,
    );

    PointerGetDatum(item as *const c_void)
}

/*
 * Create a textual representation of a JsonbValue that will serve as a GIN
 * key in a jsonb_ops index.  is_key is true if the JsonbValue is a key,
 * or if it is a string array element (since we pretend those are keys,
 * see jsonb.h).
 */
unsafe fn make_scalar_key(scalarVal: *const JsonbValue, is_key: bool) -> Datum {
    let item: Datum;
    let cstr: *mut c_char;

    match (*scalarVal).type_ {
        jbvNull => {
            Assert!(!is_key);
            item = make_text_key(JGINFLAG_NULL, c"".as_ptr(), 0);
        }
        jbvBool => {
            Assert!(!is_key);
            item = make_text_key(
                JGINFLAG_BOOL,
                if (*scalarVal).val.boolean {
                    c"t".as_ptr()
                } else {
                    c"f".as_ptr()
                },
                1,
            );
        }
        jbvNumeric => {
            Assert!(!is_key);

            /*
             * A normalized textual representation, free of trailing zeroes,
             * is required so that numerically equal values will produce equal
             * strings.
             *
             * It isn't ideal that numerics are stored in a relatively bulky
             * textual format.  However, it's a notationally convenient way of
             * storing a "union" type in the GIN B-Tree, and indexing Jsonb
             * strings takes precedence.
             */
            cstr = numeric_normalize((*scalarVal).val.numeric as crate::utils::adt::numeric::Numeric);
            item = make_text_key(JGINFLAG_NUM, cstr, strlen(cstr) as c_int);
            pfree(cstr as *mut c_void);
        }
        jbvString => {
            item = make_text_key(
                if is_key { JGINFLAG_KEY } else { JGINFLAG_STR },
                (*scalarVal).val.string.val,
                (*scalarVal).val.string.len,
            );
        }
        _ => {
            elog!(
                ERROR,
                "unrecognized jsonb scalar type: {}",
                (*scalarVal).type_ as c_int
            );
            item = 0; /* keep compiler quiet */
        }
    }

    item
}
