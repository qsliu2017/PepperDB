//! jsonb_util.rs
//!   converting between Jsonb and JsonbValues, and iterating.
//! Translated 1:1 from postgres/src/backend/utils/adt/jsonb_util.c
//!
//! Copyright (c) 2014-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/jsonb_util.c

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use crate::prelude::*; // postgres.h
use crate::c::{int32, uint32, uint64, Size, INTALIGN};
use crate::postgres_ext::Oid;
use core::ffi::{c_char, c_int, c_void};

use crate::common::hashfn::{hash_any, hash_any_extended};
use crate::lib::stringinfo::{enlargeStringInfo, initStringInfo, StringInfo, StringInfoData};
use crate::port::pg_bitutils::pg_rotate_left32;
use crate::port::port_api::qsort_arg;
use crate::utils::misc::stack_depth::check_stack_depth;
use crate::utils::adt::json::JsonEncodeDateTime;
use crate::varatt::{SET_VARSIZE, VARDATA, VARSIZE, VARSIZE_ANY};

use crate::{DirectFunctionCall1, DirectFunctionCall2, Assert, ereport, errmsg, elog};
use crate::utils::elog::ERROR;

// libc bindings (string.h, via postgres.h).
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int;
    fn strlen(s: *const c_char) -> usize;
}

// ---------------------------------------------------------------------------
// TODO(pg-port): the jsonb types below normally live in utils/jsonb.h, whose
// Rust home (crate::utils::adt::jsonb) does not yet exist.  They are defined
// here verbatim from utils/jsonb.h so that this file can be translated 1:1.
// When jsonb.h gets its own module, these definitions should move there and be
// imported instead.
// ---------------------------------------------------------------------------

/* Tokens used when sequentially processing a jsonb value */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JsonbIteratorToken {
    WJB_DONE,
    WJB_KEY,
    WJB_VALUE,
    WJB_ELEM,
    WJB_BEGIN_ARRAY,
    WJB_END_ARRAY,
    WJB_BEGIN_OBJECT,
    WJB_END_OBJECT,
}
pub use JsonbIteratorToken::*;

/*
 * JEntry format.
 */
pub type JEntry = uint32;

pub const JENTRY_OFFLENMASK: uint32 = 0x0FFFFFFF;
pub const JENTRY_TYPEMASK: uint32 = 0x70000000;
pub const JENTRY_HAS_OFF: uint32 = 0x80000000;

/* values stored in the type bits */
pub const JENTRY_ISSTRING: uint32 = 0x00000000;
pub const JENTRY_ISNUMERIC: uint32 = 0x10000000;
pub const JENTRY_ISBOOL_FALSE: uint32 = 0x20000000;
pub const JENTRY_ISBOOL_TRUE: uint32 = 0x30000000;
pub const JENTRY_ISNULL: uint32 = 0x40000000;
pub const JENTRY_ISCONTAINER: uint32 = 0x50000000; /* array or object */

/* Access macros.  Note possible multiple evaluations */
#[inline]
pub fn JBE_OFFLENFLD(je_: JEntry) -> uint32 {
    je_ & JENTRY_OFFLENMASK
}
#[inline]
pub fn JBE_HAS_OFF(je_: JEntry) -> bool {
    (je_ & JENTRY_HAS_OFF) != 0
}
#[inline]
pub fn JBE_ISSTRING(je_: JEntry) -> bool {
    (je_ & JENTRY_TYPEMASK) == JENTRY_ISSTRING
}
#[inline]
pub fn JBE_ISNUMERIC(je_: JEntry) -> bool {
    (je_ & JENTRY_TYPEMASK) == JENTRY_ISNUMERIC
}
#[inline]
pub fn JBE_ISCONTAINER(je_: JEntry) -> bool {
    (je_ & JENTRY_TYPEMASK) == JENTRY_ISCONTAINER
}
#[inline]
pub fn JBE_ISNULL(je_: JEntry) -> bool {
    (je_ & JENTRY_TYPEMASK) == JENTRY_ISNULL
}
#[inline]
pub fn JBE_ISBOOL_TRUE(je_: JEntry) -> bool {
    (je_ & JENTRY_TYPEMASK) == JENTRY_ISBOOL_TRUE
}
#[inline]
pub fn JBE_ISBOOL_FALSE(je_: JEntry) -> bool {
    (je_ & JENTRY_TYPEMASK) == JENTRY_ISBOOL_FALSE
}

/* Macro for advancing an offset variable to the next JEntry */
#[inline]
pub fn JBE_ADVANCE_OFFSET(offset: &mut uint32, je: JEntry) {
    let je_: JEntry = je;
    if JBE_HAS_OFF(je_) {
        *offset = JBE_OFFLENFLD(je_);
    } else {
        *offset += JBE_OFFLENFLD(je_);
    }
}

/*
 * We store an offset, not a length, every JB_OFFSET_STRIDE children.
 */
pub const JB_OFFSET_STRIDE: c_int = 32;

/*
 * A jsonb array or object node, within a Jsonb Datum.
 */
#[repr(C)]
pub struct JsonbContainer {
    pub header: uint32, /* number of elements or key/value pairs, and flags */
    pub children: [JEntry; 0], /* FLEXIBLE_ARRAY_MEMBER */
                        /* the data for each child node follows. */
}

/* flags for the header-field in JsonbContainer */
pub const JB_CMASK: uint32 = 0x0FFFFFFF; /* mask for count field */
pub const JB_FSCALAR: uint32 = 0x10000000; /* flag bits */
pub const JB_FOBJECT: uint32 = 0x20000000;
pub const JB_FARRAY: uint32 = 0x40000000;

/* convenience macros for accessing a JsonbContainer struct */
#[inline]
pub unsafe fn JsonContainerSize(jc: *const JsonbContainer) -> uint32 {
    (*jc).header & JB_CMASK
}
#[inline]
pub unsafe fn JsonContainerIsScalar(jc: *const JsonbContainer) -> bool {
    ((*jc).header & JB_FSCALAR) != 0
}
#[inline]
pub unsafe fn JsonContainerIsObject(jc: *const JsonbContainer) -> bool {
    ((*jc).header & JB_FOBJECT) != 0
}
#[inline]
pub unsafe fn JsonContainerIsArray(jc: *const JsonbContainer) -> bool {
    ((*jc).header & JB_FARRAY) != 0
}

/* The top-level on-disk format for a jsonb datum. */
#[repr(C)]
pub struct Jsonb {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub root: JsonbContainer,
}

/*
 * enum jbvType
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum jbvType {
    /* Scalar types */
    jbvNull = 0x0,
    jbvString = 0x1,
    jbvNumeric = 0x2,
    jbvBool = 0x3,
    /* Composite types */
    jbvArray = 0x10,
    jbvObject = 0x11,
    /* Binary (i.e. struct Jsonb) jbvArray/jbvObject */
    jbvBinary = 0x12,
    /* Virtual types. */
    jbvDatetime = 0x20,
}
pub use jbvType::*;

/* TODO(pg-port): real Numeric lives in utils/numeric.h (utils/adt/numeric.rs,
 * not yet ported).  Numeric is an opaque varlena pointer; stub the type. */
#[repr(C)]
pub struct NumericData {
    _opaque: [u8; 0],
}
pub type Numeric = *mut NumericData;

/*
 * JsonbValue:	In-memory representation of Jsonb.
 */
#[repr(C)]
pub struct JsonbValue {
    pub type_: jbvType, /* Influences sort order */
    pub val: JsonbValueUnion,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union JsonbValueUnion {
    pub numeric: Numeric,
    pub boolean: bool,
    pub string: JsonbValueString, /* String primitive type */
    pub array: JsonbValueArray,   /* Array container type */
    pub object: JsonbValueObject, /* Associative container type */
    pub binary: JsonbValueBinary, /* Array or object, in on-disk format */
    pub datetime: JsonbValueDatetime,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonbValueString {
    pub len: c_int,
    pub val: *mut c_char, /* Not necessarily null-terminated */
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonbValueArray {
    pub nElems: c_int,
    pub elems: *mut JsonbValue,
    pub rawScalar: bool, /* Top-level "raw scalar" array? */
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonbValueObject {
    pub nPairs: c_int, /* 1 pair, 2 elements */
    pub pairs: *mut JsonbPair,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonbValueBinary {
    pub len: c_int,
    pub data: *mut JsonbContainer,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonbValueDatetime {
    pub value: Datum,
    pub typid: Oid,
    pub typmod: int32,
    pub tz: c_int, /* Numeric time zone, in seconds, for TimestampTz */
}

#[inline]
pub unsafe fn IsAJsonbScalar(jsonbval: *const JsonbValue) -> bool {
    ((*jsonbval).type_ >= jbvNull && (*jsonbval).type_ <= jbvBool)
        || (*jsonbval).type_ == jbvDatetime
}

/*
 * Key/value pair within an Object.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonbPair {
    pub key: JsonbValue,   /* Must be a jbvString */
    pub value: JsonbValue, /* May be of any type */
    pub order: uint32,     /* Pair's index in original sequence */
}

/* Make JsonbValue copyable since the C code does struct assignment of it. */
impl Clone for JsonbValue {
    fn clone(&self) -> Self {
        *self
    }
}
impl Copy for JsonbValue {}

/* Conversion state used when parsing Jsonb from text, or for type coercion */
#[repr(C)]
pub struct JsonbParseState {
    pub contVal: JsonbValue,
    pub size: Size,
    pub next: *mut JsonbParseState,
    pub unique_keys: bool, /* Check object key uniqueness */
    pub skip_nulls: bool,  /* Skip null object fields */
}

/*
 * JsonbIterator holds details of the type for each iteration.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum JsonbIterState {
    JBI_ARRAY_START,
    JBI_ARRAY_ELEM,
    JBI_OBJECT_START,
    JBI_OBJECT_KEY,
    JBI_OBJECT_VALUE,
}
pub use JsonbIterState::*;

#[repr(C)]
pub struct JsonbIterator {
    /* Container being iterated */
    pub container: *mut JsonbContainer,
    pub nElems: uint32, /* Number of elements in children array (will be nPairs
                         * for objects) */
    pub isScalar: bool,    /* Pseudo-array scalar value? */
    pub children: *mut JEntry, /* JEntrys for child nodes */
    /* Data proper.  This points to the beginning of the variable-length data */
    pub dataProper: *mut c_char,

    /* Current item in buffer (up to nElems) */
    pub curIndex: c_int,

    /* Data offset corresponding to current item */
    pub curDataOffset: uint32,

    /*
     * If the container is an object, we want to return keys and values
     * alternately; so curDataOffset points to the current key, and
     * curValueOffset points to the current value.
     */
    pub curValueOffset: uint32,

    /* Private state */
    pub state: JsonbIterState,

    pub parent: *mut JsonbIterator,
}

const VARHDRSZ: usize = core::mem::size_of::<int32>();

/* TODO(pg-port): ERRCODE_* live in utils/errcodes.h (not yet ported); the
 * errcode() shim ignores the code, so these are placeholders for fidelity. */
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;
const ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE: c_int = 0;

/* TODO(pg-port): DEFAULT_COLLATION_OID lives in catalog/pg_collation.h. */
use crate::catalog::pg_known_oids::DEFAULT_COLLATION_OID;

/* TODO(pg-port): the following numeric helpers, varstr_cmp, and the Datum
 * conversion macros below have no Rust home yet (utils/adt/numeric.rs,
 * utils/adt/varlena.rs varstr_cmp).  They are stubbed minimally so that this
 * file is self-contained and faithfully reproduces the original logic. */
#[inline]
unsafe fn NumericGetDatum(X: Numeric) -> Datum {
    PointerGetDatum(X as *const c_void)
}

/* TODO(pg-port): real numeric_eq lives in utils/adt/numeric.c (not yet ported).
 * Declared with the PGFunction signature so it can be handed to
 * DirectFunctionCall*. */
unsafe fn numeric_eq(_fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum { crate::utils::adt::numeric::numeric_eq(_fcinfo) as _ }
/* TODO(pg-port): real numeric_cmp lives in utils/adt/numeric.c (not yet ported). */
unsafe fn numeric_cmp(_fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum { crate::utils::adt::numeric::numeric_cmp(_fcinfo) as _ }
/* TODO(pg-port): real hash_numeric lives in utils/adt/numeric.c (not yet ported). */
unsafe fn hash_numeric(_fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum { crate::utils::adt::numeric::hash_numeric(_fcinfo) as _ }
/* TODO(pg-port): real hash_numeric_extended lives in utils/adt/numeric.c. */
unsafe fn hash_numeric_extended(_fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum { crate::utils::adt::numeric::hash_numeric_extended(_fcinfo) as _ }

/* TODO(pg-port): real varstr_cmp lives in utils/adt/varlena.c (not yet ported). */
unsafe fn varstr_cmp(
    _arg1: *const c_char,
    _len1: c_int,
    _arg2: *const c_char,
    _len2: c_int,
    _collid: Oid,
) -> c_int { crate::utils::adt::varlena::varstr_cmp(_arg1 as _, _len1 as _, _arg2 as _, _len2 as _, _collid as _) as _ }

/* hashcharextended is ported in access/hash/hashfunc.rs */
use crate::access::hash::hashfunc::hashcharextended;

/*
 * ROTATE_HIGH_AND_LOW_32BITS, from common/hashfn.h.
 */
#[inline]
const fn ROTATE_HIGH_AND_LOW_32BITS(v: uint64) -> uint64 {
    ((v << 1) & 0xfffffffefffffffe_u64) | ((v >> 31) & 0x100000001_u64)
}

/*
 * Maximum number of elements in an array (or key/value pairs in an object).
 * This is limited by two things: the size of the JEntry array must fit
 * in MaxAllocSize, and the number of elements (or pairs) must fit in the bits
 * reserved for that in the JsonbContainer.header field.
 *
 * (The total size of an array's or object's elements is also limited by
 * JENTRY_OFFLENMASK, but we're not concerned about that here.)
 */
#[inline]
fn JSONB_MAX_ELEMS() -> Size {
    crate::c::Min(
        MaxAllocSize / core::mem::size_of::<JsonbValue>(),
        JB_CMASK as Size,
    )
}
#[inline]
fn JSONB_MAX_PAIRS() -> Size {
    crate::c::Min(
        MaxAllocSize / core::mem::size_of::<JsonbPair>(),
        JB_CMASK as Size,
    )
}

pub unsafe fn JsonbToJsonbValue(jsonb: *mut Jsonb, val: *mut JsonbValue) {
    (*val).type_ = jbvBinary;
    (*val).val.binary.data = &raw mut (*jsonb).root;
    (*val).val.binary.len = VARSIZE(jsonb as *const c_char) as c_int - VARHDRSZ as c_int;
}

/*
 * Turn an in-memory JsonbValue into a Jsonb for on-disk storage.
 *
 * Generally we find it more convenient to directly iterate through the Jsonb
 * representation and only really convert nested scalar values.
 * JsonbIteratorNext() does this, so that clients of the iteration code don't
 * have to directly deal with the binary representation (JsonbDeepContains() is
 * a notable exception, although all exceptions are internal to this module).
 * In general, functions that accept a JsonbValue argument are concerned with
 * the manipulation of scalar values, or simple containers of scalar values,
 * where it would be inconvenient to deal with a great amount of other state.
 */
pub unsafe fn JsonbValueToJsonb(val: *mut JsonbValue) -> *mut Jsonb {
    let out: *mut Jsonb;

    if IsAJsonbScalar(val) {
        /* Scalar value */
        let mut pstate: *mut JsonbParseState = std::ptr::null_mut();
        let res: *mut JsonbValue;
        let mut scalarArray: JsonbValue = core::mem::zeroed();

        scalarArray.type_ = jbvArray;
        scalarArray.val.array.rawScalar = true;
        scalarArray.val.array.nElems = 1;

        pushJsonbValue(&mut pstate, WJB_BEGIN_ARRAY, &mut scalarArray);
        pushJsonbValue(&mut pstate, WJB_ELEM, val);
        res = pushJsonbValue(&mut pstate, WJB_END_ARRAY, std::ptr::null_mut());

        out = convertToJsonb(res);
    } else if (*val).type_ == jbvObject || (*val).type_ == jbvArray {
        out = convertToJsonb(val);
    } else {
        Assert!((*val).type_ == jbvBinary);
        out = palloc(VARHDRSZ + (*val).val.binary.len as usize) as *mut Jsonb;
        SET_VARSIZE(
            out as *mut c_char,
            (VARHDRSZ as c_int) + (*val).val.binary.len,
        );
        memcpy(
            VARDATA(out as *const c_char) as *mut c_void,
            (*val).val.binary.data as *const c_void,
            (*val).val.binary.len as usize,
        );
    }

    out
}

/*
 * Get the offset of the variable-length portion of a Jsonb node within
 * the variable-length-data part of its container.  The node is identified
 * by index within the container's JEntry array.
 */
pub unsafe fn getJsonbOffset(jc: *const JsonbContainer, index: c_int) -> uint32 {
    let mut offset: uint32 = 0;
    let mut i: c_int;

    /*
     * Start offset of this entry is equal to the end offset of the previous
     * entry.  Walk backwards to the most recent entry stored as an end
     * offset, returning that offset plus any lengths in between.
     */
    i = index - 1;
    while i >= 0 {
        let child = *(&raw const (*jc).children as *const JEntry).add(i as usize);
        offset += JBE_OFFLENFLD(child);
        if JBE_HAS_OFF(child) {
            break;
        }
        i -= 1;
    }

    offset
}

/*
 * Get the length of the variable-length portion of a Jsonb node.
 * The node is identified by index within the container's JEntry array.
 */
pub unsafe fn getJsonbLength(jc: *const JsonbContainer, index: c_int) -> uint32 {
    let off: uint32;
    let len: uint32;

    let child = *(&raw const (*jc).children as *const JEntry).add(index as usize);

    /*
     * If the length is stored directly in the JEntry, just return it.
     * Otherwise, get the begin offset of the entry, and subtract that from
     * the stored end+1 offset.
     */
    if JBE_HAS_OFF(child) {
        off = getJsonbOffset(jc, index);
        len = JBE_OFFLENFLD(child) - off;
    } else {
        len = JBE_OFFLENFLD(child);
    }

    len
}

/*
 * BT comparator worker function.  Returns an integer less than, equal to, or
 * greater than zero, indicating whether a is less than, equal to, or greater
 * than b.  Consistent with the requirements for a B-Tree operator class
 *
 * Strings are compared lexically, in contrast with other places where we use a
 * much simpler comparator logic for searching through Strings.  Since this is
 * called from B-Tree support function 1, we're careful about not leaking
 * memory here.
 */
pub unsafe fn compareJsonbContainers(a: *mut JsonbContainer, b: *mut JsonbContainer) -> c_int {
    let mut ita: *mut JsonbIterator;
    let mut itb: *mut JsonbIterator;
    let mut res: c_int = 0;

    ita = JsonbIteratorInit(a);
    itb = JsonbIteratorInit(b);

    loop {
        let mut va: JsonbValue = core::mem::zeroed();
        let mut vb: JsonbValue = core::mem::zeroed();
        let ra: JsonbIteratorToken;
        let rb: JsonbIteratorToken;

        ra = JsonbIteratorNext(&mut ita, &mut va, false);
        rb = JsonbIteratorNext(&mut itb, &mut vb, false);

        if ra == rb {
            if ra == WJB_DONE {
                /* Decisively equal */
                break;
            }

            if ra == WJB_END_ARRAY || ra == WJB_END_OBJECT {
                /*
                 * There is no array or object to compare at this stage of
                 * processing.  jbvArray/jbvObject values are compared
                 * initially, at the WJB_BEGIN_ARRAY and WJB_BEGIN_OBJECT
                 * tokens.
                 */
                continue;
            }

            if va.type_ == vb.type_ {
                match va.type_ {
                    jbvString | jbvNull | jbvNumeric | jbvBool => {
                        res = compareJsonbScalarValue(&mut va, &mut vb);
                    }
                    jbvArray => {
                        /*
                         * This could be a "raw scalar" pseudo array.  That's
                         * a special case here though, since we still want the
                         * general type-based comparisons to apply, and as far
                         * as we're concerned a pseudo array is just a scalar.
                         */
                        if va.val.array.rawScalar != vb.val.array.rawScalar {
                            res = if va.val.array.rawScalar { -1 } else { 1 };
                        }

                        /*
                         * There should be an "else" here, to prevent us from
                         * overriding the above, but we can't change the sort
                         * order now, so there is a mild anomaly that an empty
                         * top level array sorts less than null.
                         */
                        if va.val.array.nElems != vb.val.array.nElems {
                            res = if va.val.array.nElems > vb.val.array.nElems {
                                1
                            } else {
                                -1
                            };
                        }
                    }
                    jbvObject => {
                        if va.val.object.nPairs != vb.val.object.nPairs {
                            res = if va.val.object.nPairs > vb.val.object.nPairs {
                                1
                            } else {
                                -1
                            };
                        }
                    }
                    jbvBinary => {
                        elog!(ERROR, "unexpected jbvBinary value");
                    }
                    jbvDatetime => {
                        elog!(ERROR, "unexpected jbvDatetime value");
                    }
                }
            } else {
                /* Type-defined order */
                res = if va.type_ > vb.type_ { 1 } else { -1 };
            }
        } else {
            /*
             * If the two values were of the same container type, then there'd
             * have been a chance to observe the variation in the number of
             * elements/pairs (when processing WJB_BEGIN_OBJECT, say). They're
             * either two heterogeneously-typed containers, or a container and
             * some scalar type.
             *
             * We don't have to consider the WJB_END_ARRAY and WJB_END_OBJECT
             * cases here, because we would have seen the corresponding
             * WJB_BEGIN_ARRAY and WJB_BEGIN_OBJECT tokens first, and
             * concluded that they don't match.
             */
            Assert!(ra != WJB_END_ARRAY && ra != WJB_END_OBJECT);
            Assert!(rb != WJB_END_ARRAY && rb != WJB_END_OBJECT);

            Assert!(va.type_ != vb.type_);
            Assert!(va.type_ != jbvBinary);
            Assert!(vb.type_ != jbvBinary);
            /* Type-defined order */
            res = if va.type_ > vb.type_ { 1 } else { -1 };
        }

        if res != 0 {
            break;
        }
    }

    while !ita.is_null() {
        let i = (*ita).parent;

        pfree(ita as *mut c_void);
        ita = i;
    }
    while !itb.is_null() {
        let i = (*itb).parent;

        pfree(itb as *mut c_void);
        itb = i;
    }

    res
}

/*
 * Find value in object (i.e. the "value" part of some key/value pair in an
 * object), or find a matching element if we're looking through an array.  Do
 * so on the basis of equality of the object keys only, or alternatively
 * element values only, with a caller-supplied value "key".  The "flags"
 * argument allows the caller to specify which container types are of interest.
 *
 * This exported utility function exists to facilitate various cases concerned
 * with "containment".  If asked to look through an object, the caller had
 * better pass a Jsonb String, because their keys can only be strings.
 * Otherwise, for an array, any type of JsonbValue will do.
 *
 * In order to proceed with the search, it is necessary for callers to have
 * both specified an interest in exactly one particular container type with an
 * appropriate flag, as well as having the pointed-to Jsonb container be of
 * one of those same container types at the top level. (Actually, we just do
 * whichever makes sense to save callers the trouble of figuring it out - at
 * most one can make sense, because the container either points to an array
 * (possibly a "raw scalar" pseudo array) or an object.)
 *
 * Note that we can return a jbvBinary JsonbValue if this is called on an
 * object, but we never do so on an array.  If the caller asks to look through
 * a container type that is not of the type pointed to by the container,
 * immediately fall through and return NULL.  If we cannot find the value,
 * return NULL.  Otherwise, return palloc()'d copy of value.
 */
pub unsafe fn findJsonbValueFromContainer(
    container: *mut JsonbContainer,
    flags: uint32,
    key: *mut JsonbValue,
) -> *mut JsonbValue {
    let children: *mut JEntry = &raw mut (*container).children as *mut JEntry;
    let count: c_int = JsonContainerSize(container) as c_int;

    Assert!((flags & !(JB_FARRAY | JB_FOBJECT)) == 0);

    /* Quick out without a palloc cycle if object/array is empty */
    if count <= 0 {
        return std::ptr::null_mut();
    }

    if (flags & JB_FARRAY) != 0 && JsonContainerIsArray(container) {
        let result: *mut JsonbValue = palloc(core::mem::size_of::<JsonbValue>()) as *mut JsonbValue;
        let base_addr: *mut c_char = children.add(count as usize) as *mut c_char;
        let mut offset: uint32 = 0;
        let mut i: c_int = 0;

        while i < count {
            fillJsonbValue(container, i, base_addr, offset, result);

            if (*key).type_ == (*result).type_ {
                if equalsJsonbScalarValue(key, result) {
                    return result;
                }
            }

            JBE_ADVANCE_OFFSET(&mut offset, *children.add(i as usize));
            i += 1;
        }

        pfree(result as *mut c_void);
    } else if (flags & JB_FOBJECT) != 0 && JsonContainerIsObject(container) {
        /* Object key passed by caller must be a string */
        Assert!((*key).type_ == jbvString);

        return getKeyJsonValueFromContainer(
            container,
            (*key).val.string.val,
            (*key).val.string.len,
            std::ptr::null_mut(),
        );
    }

    /* Not found */
    std::ptr::null_mut()
}

/*
 * Find value by key in Jsonb object and fetch it into 'res', which is also
 * returned.
 *
 * 'res' can be passed in as NULL, in which case it's newly palloc'ed here.
 */
pub unsafe fn getKeyJsonValueFromContainer(
    container: *mut JsonbContainer,
    keyVal: *const c_char,
    keyLen: c_int,
    mut res: *mut JsonbValue,
) -> *mut JsonbValue {
    let children: *mut JEntry = &raw mut (*container).children as *mut JEntry;
    let count: c_int = JsonContainerSize(container) as c_int;
    let baseAddr: *mut c_char;
    let mut stopLow: uint32;
    let mut stopHigh: uint32;

    Assert!(JsonContainerIsObject(container));

    /* Quick out without a palloc cycle if object is empty */
    if count <= 0 {
        return std::ptr::null_mut();
    }

    /*
     * Binary search the container. Since we know this is an object, account
     * for *Pairs* of Jentrys
     */
    baseAddr = children.add((count * 2) as usize) as *mut c_char;
    stopLow = 0;
    stopHigh = count as uint32;
    while stopLow < stopHigh {
        let stopMiddle: uint32;
        let difference: c_int;
        let candidateVal: *const c_char;
        let candidateLen: c_int;

        stopMiddle = stopLow + (stopHigh - stopLow) / 2;

        candidateVal = baseAddr.add(getJsonbOffset(container, stopMiddle as c_int) as usize);
        candidateLen = getJsonbLength(container, stopMiddle as c_int) as c_int;

        difference = lengthCompareJsonbString(candidateVal, candidateLen, keyVal, keyLen);

        if difference == 0 {
            /* Found our key, return corresponding value */
            let index: c_int = stopMiddle as c_int + count;

            if res.is_null() {
                res = palloc(core::mem::size_of::<JsonbValue>()) as *mut JsonbValue;
            }

            fillJsonbValue(
                container,
                index,
                baseAddr,
                getJsonbOffset(container, index),
                res,
            );

            return res;
        } else {
            if difference < 0 {
                stopLow = stopMiddle + 1;
            } else {
                stopHigh = stopMiddle;
            }
        }
    }

    /* Not found */
    std::ptr::null_mut()
}

/*
 * Get i-th value of a Jsonb array.
 *
 * Returns palloc()'d copy of the value, or NULL if it does not exist.
 */
pub unsafe fn getIthJsonbValueFromContainer(
    container: *mut JsonbContainer,
    i: uint32,
) -> *mut JsonbValue {
    let result: *mut JsonbValue;
    let base_addr: *mut c_char;
    let nelements: uint32;

    if !JsonContainerIsArray(container) {
        elog!(ERROR, "not a jsonb array");
    }

    nelements = JsonContainerSize(container);
    let children: *mut JEntry = &raw mut (*container).children as *mut JEntry;
    base_addr = children.add(nelements as usize) as *mut c_char;

    if i >= nelements {
        return std::ptr::null_mut();
    }

    result = palloc(core::mem::size_of::<JsonbValue>()) as *mut JsonbValue;

    fillJsonbValue(
        container,
        i as c_int,
        base_addr,
        getJsonbOffset(container, i as c_int),
        result,
    );

    result
}

/*
 * A helper function to fill in a JsonbValue to represent an element of an
 * array, or a key or value of an object.
 *
 * The node's JEntry is at container->children[index], and its variable-length
 * data is at base_addr + offset.  We make the caller determine the offset
 * since in many cases the caller can amortize that work across multiple
 * children.  When it can't, it can just call getJsonbOffset().
 *
 * A nested array or object will be returned as jbvBinary, ie. it won't be
 * expanded.
 */
unsafe fn fillJsonbValue(
    container: *mut JsonbContainer,
    index: c_int,
    base_addr: *mut c_char,
    offset: uint32,
    result: *mut JsonbValue,
) {
    let children: *mut JEntry = &raw mut (*container).children as *mut JEntry;
    let entry: JEntry = *children.add(index as usize);

    if JBE_ISNULL(entry) {
        (*result).type_ = jbvNull;
    } else if JBE_ISSTRING(entry) {
        (*result).type_ = jbvString;
        (*result).val.string.val = base_addr.add(offset as usize);
        (*result).val.string.len = getJsonbLength(container, index) as c_int;
        Assert!((*result).val.string.len >= 0);
    } else if JBE_ISNUMERIC(entry) {
        (*result).type_ = jbvNumeric;
        (*result).val.numeric = base_addr.add(INTALIGN(offset as usize)) as Numeric;
    } else if JBE_ISBOOL_TRUE(entry) {
        (*result).type_ = jbvBool;
        (*result).val.boolean = true;
    } else if JBE_ISBOOL_FALSE(entry) {
        (*result).type_ = jbvBool;
        (*result).val.boolean = false;
    } else {
        Assert!(JBE_ISCONTAINER(entry));
        (*result).type_ = jbvBinary;
        /* Remove alignment padding from data pointer and length */
        (*result).val.binary.data = base_addr.add(INTALIGN(offset as usize)) as *mut JsonbContainer;
        (*result).val.binary.len =
            getJsonbLength(container, index) as c_int - (INTALIGN(offset as usize) - offset as usize) as c_int;
    }
}

/*
 * Push JsonbValue into JsonbParseState.
 *
 * Used when parsing JSON tokens to form Jsonb, or when converting an in-memory
 * JsonbValue to a Jsonb.
 *
 * Initial state of *JsonbParseState is NULL, since it'll be allocated here
 * originally (caller will get JsonbParseState back by reference).
 *
 * Only sequential tokens pertaining to non-container types should pass a
 * JsonbValue.  There is one exception -- WJB_BEGIN_ARRAY callers may pass a
 * "raw scalar" pseudo array to append it - the actual scalar should be passed
 * next and it will be added as the only member of the array.
 *
 * Values of type jbvBinary, which are rolled up arrays and objects,
 * are unpacked before being added to the result.
 */
pub unsafe fn pushJsonbValue(
    pstate: *mut *mut JsonbParseState,
    seq: JsonbIteratorToken,
    jbval: *mut JsonbValue,
) -> *mut JsonbValue {
    let mut it: *mut JsonbIterator;
    let mut res: *mut JsonbValue = std::ptr::null_mut();
    let mut v: JsonbValue = core::mem::zeroed();
    let mut tok: JsonbIteratorToken;
    let mut i: c_int;

    if !jbval.is_null()
        && (seq == WJB_ELEM || seq == WJB_VALUE)
        && (*jbval).type_ == jbvObject
    {
        pushJsonbValue(pstate, WJB_BEGIN_OBJECT, std::ptr::null_mut());
        i = 0;
        while i < (*jbval).val.object.nPairs {
            pushJsonbValue(pstate, WJB_KEY, &mut (*(*jbval).val.object.pairs.add(i as usize)).key);
            pushJsonbValue(
                pstate,
                WJB_VALUE,
                &mut (*(*jbval).val.object.pairs.add(i as usize)).value,
            );
            i += 1;
        }

        return pushJsonbValue(pstate, WJB_END_OBJECT, std::ptr::null_mut());
    }

    if !jbval.is_null()
        && (seq == WJB_ELEM || seq == WJB_VALUE)
        && (*jbval).type_ == jbvArray
    {
        pushJsonbValue(pstate, WJB_BEGIN_ARRAY, std::ptr::null_mut());
        i = 0;
        while i < (*jbval).val.array.nElems {
            pushJsonbValue(pstate, WJB_ELEM, &mut *(*jbval).val.array.elems.add(i as usize));
            i += 1;
        }

        return pushJsonbValue(pstate, WJB_END_ARRAY, std::ptr::null_mut());
    }

    if jbval.is_null()
        || (seq != WJB_ELEM && seq != WJB_VALUE)
        || (*jbval).type_ != jbvBinary
    {
        /* drop through */
        return pushJsonbValueScalar(pstate, seq, jbval);
    }

    /* unpack the binary and add each piece to the pstate */
    it = JsonbIteratorInit((*jbval).val.binary.data);

    if ((*(*jbval).val.binary.data).header & JB_FSCALAR) != 0 && !(*pstate).is_null() {
        tok = JsonbIteratorNext(&mut it, &mut v, true);
        Assert!(tok == WJB_BEGIN_ARRAY);
        Assert!(v.type_ == jbvArray && v.val.array.rawScalar);

        tok = JsonbIteratorNext(&mut it, &mut v, true);
        Assert!(tok == WJB_ELEM);

        res = pushJsonbValueScalar(pstate, seq, &mut v);

        tok = JsonbIteratorNext(&mut it, &mut v, true);
        Assert!(tok == WJB_END_ARRAY);
        Assert!(it.is_null());

        return res;
    }

    loop {
        tok = JsonbIteratorNext(&mut it, &mut v, false);
        if tok == WJB_DONE {
            break;
        }
        let arg = if tok < WJB_BEGIN_ARRAY
            || (tok == WJB_BEGIN_ARRAY && v.val.array.rawScalar)
        {
            &mut v as *mut JsonbValue
        } else {
            std::ptr::null_mut()
        };
        res = pushJsonbValueScalar(pstate, tok, arg);
    }

    res
}

/*
 * Do the actual pushing, with only scalar or pseudo-scalar-array values
 * accepted.
 */
unsafe fn pushJsonbValueScalar(
    pstate: *mut *mut JsonbParseState,
    seq: JsonbIteratorToken,
    scalarVal: *mut JsonbValue,
) -> *mut JsonbValue {
    let mut result: *mut JsonbValue = std::ptr::null_mut();

    match seq {
        WJB_BEGIN_ARRAY => {
            Assert!(scalarVal.is_null() || (*scalarVal).val.array.rawScalar);
            *pstate = pushState(pstate);
            result = &raw mut (**pstate).contVal;
            (**pstate).contVal.type_ = jbvArray;
            (**pstate).contVal.val.array.nElems = 0;
            (**pstate).contVal.val.array.rawScalar =
                !scalarVal.is_null() && (*scalarVal).val.array.rawScalar;
            if !scalarVal.is_null() && (*scalarVal).val.array.nElems > 0 {
                /* Assume that this array is still really a scalar */
                Assert!((*scalarVal).type_ == jbvArray);
                (**pstate).size = (*scalarVal).val.array.nElems as Size;
            } else {
                (**pstate).size = 4;
            }
            (**pstate).contVal.val.array.elems =
                palloc(core::mem::size_of::<JsonbValue>() * (**pstate).size) as *mut JsonbValue;
        }
        WJB_BEGIN_OBJECT => {
            Assert!(scalarVal.is_null());
            *pstate = pushState(pstate);
            result = &raw mut (**pstate).contVal;
            (**pstate).contVal.type_ = jbvObject;
            (**pstate).contVal.val.object.nPairs = 0;
            (**pstate).size = 4;
            (**pstate).contVal.val.object.pairs =
                palloc(core::mem::size_of::<JsonbPair>() * (**pstate).size) as *mut JsonbPair;
        }
        WJB_KEY => {
            Assert!((*scalarVal).type_ == jbvString);
            appendKey(*pstate, scalarVal);
        }
        WJB_VALUE => {
            Assert!(IsAJsonbScalar(scalarVal));
            appendValue(*pstate, scalarVal);
        }
        WJB_ELEM => {
            Assert!(IsAJsonbScalar(scalarVal));
            appendElement(*pstate, scalarVal);
        }
        WJB_END_OBJECT => {
            uniqueifyJsonbObject(
                &raw mut (**pstate).contVal,
                (**pstate).unique_keys,
                (**pstate).skip_nulls,
            );
            /* fall through! */
            result = endArrayOrObject(pstate);
        }
        WJB_END_ARRAY => {
            /* Steps here common to WJB_END_OBJECT case */
            Assert!(scalarVal.is_null());
            result = endArrayOrObject(pstate);
        }
        _ => {
            elog!(ERROR, "unrecognized jsonb sequential processing token");
        }
    }

    result
}

/*
 * Shared tail of WJB_END_OBJECT / WJB_END_ARRAY in pushJsonbValueScalar().
 * Pop stack and push current array/object as value in parent array/object.
 */
unsafe fn endArrayOrObject(pstate: *mut *mut JsonbParseState) -> *mut JsonbValue {
    let result: *mut JsonbValue = &raw mut (**pstate).contVal;

    /*
     * Pop stack and push current array/object as value in parent
     * array/object
     */
    *pstate = (**pstate).next;
    if !(*pstate).is_null() {
        match (**pstate).contVal.type_ {
            jbvArray => {
                appendElement(*pstate, result);
            }
            jbvObject => {
                appendValue(*pstate, result);
            }
            _ => {
                elog!(ERROR, "invalid jsonb container type");
            }
        }
    }

    result
}

/*
 * pushJsonbValue() worker:  Iteration-like forming of Jsonb
 */
unsafe fn pushState(pstate: *mut *mut JsonbParseState) -> *mut JsonbParseState {
    let ns: *mut JsonbParseState =
        palloc(core::mem::size_of::<JsonbParseState>()) as *mut JsonbParseState;

    (*ns).next = *pstate;
    (*ns).unique_keys = false;
    (*ns).skip_nulls = false;

    ns
}

/*
 * pushJsonbValue() worker:  Append a pair key to state when generating a Jsonb
 */
unsafe fn appendKey(pstate: *mut JsonbParseState, string: *mut JsonbValue) {
    let object: *mut JsonbValue = &raw mut (*pstate).contVal;

    Assert!((*object).type_ == jbvObject);
    Assert!((*string).type_ == jbvString);

    if (*object).val.object.nPairs as Size >= JSONB_MAX_PAIRS() {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(
            ERROR,
            errmsg!(
                "number of jsonb object pairs exceeds the maximum allowed ({})",
                JSONB_MAX_PAIRS()
            )
        );
    }

    if (*object).val.object.nPairs as Size >= (*pstate).size {
        (*pstate).size *= 2;
        (*object).val.object.pairs = repalloc(
            (*object).val.object.pairs as *mut c_void,
            core::mem::size_of::<JsonbPair>() * (*pstate).size,
        ) as *mut JsonbPair;
    }

    let n = (*object).val.object.nPairs as usize;
    (*(*object).val.object.pairs.add(n)).key = *string;
    (*(*object).val.object.pairs.add(n)).order = (*object).val.object.nPairs as uint32;
}

/*
 * pushJsonbValue() worker:  Append a pair value to state when generating a
 * Jsonb
 */
unsafe fn appendValue(pstate: *mut JsonbParseState, scalarVal: *mut JsonbValue) {
    let object: *mut JsonbValue = &raw mut (*pstate).contVal;

    Assert!((*object).type_ == jbvObject);

    let n = (*object).val.object.nPairs as usize;
    (*(*object).val.object.pairs.add(n)).value = *scalarVal;
    (*object).val.object.nPairs += 1;
}

/*
 * pushJsonbValue() worker:  Append an element to state when generating a Jsonb
 */
unsafe fn appendElement(pstate: *mut JsonbParseState, scalarVal: *mut JsonbValue) {
    let array: *mut JsonbValue = &raw mut (*pstate).contVal;

    Assert!((*array).type_ == jbvArray);

    if (*array).val.array.nElems as Size >= JSONB_MAX_ELEMS() {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(
            ERROR,
            errmsg!(
                "number of jsonb array elements exceeds the maximum allowed ({})",
                JSONB_MAX_ELEMS()
            )
        );
    }

    if (*array).val.array.nElems as Size >= (*pstate).size {
        (*pstate).size *= 2;
        (*array).val.array.elems = repalloc(
            (*array).val.array.elems as *mut c_void,
            core::mem::size_of::<JsonbValue>() * (*pstate).size,
        ) as *mut JsonbValue;
    }

    let n = (*array).val.array.nElems as usize;
    *(*array).val.array.elems.add(n) = *scalarVal;
    (*array).val.array.nElems += 1;
}

/*
 * Given a JsonbContainer, expand to JsonbIterator to iterate over items
 * fully expanded to in-memory representation for manipulation.
 *
 * See JsonbIteratorNext() for notes on memory management.
 */
pub unsafe fn JsonbIteratorInit(container: *mut JsonbContainer) -> *mut JsonbIterator {
    iteratorFromContainer(container, std::ptr::null_mut())
}

/*
 * Get next JsonbValue while iterating
 *
 * Caller should initially pass their own, original iterator.  They may get
 * back a child iterator palloc()'d here instead.  The function can be relied
 * on to free those child iterators, lest the memory allocated for highly
 * nested objects become unreasonable, but only if callers don't end iteration
 * early (by breaking upon having found something in a search, for example).
 *
 * Callers in such a scenario, that are particularly sensitive to leaking
 * memory in a long-lived context may walk the ancestral tree from the final
 * iterator we left them with to its oldest ancestor, pfree()ing as they go.
 * They do not have to free any other memory previously allocated for iterators
 * but not accessible as direct ancestors of the iterator they're last passed
 * back.
 *
 * Returns "Jsonb sequential processing" token value.  Iterator "state"
 * reflects the current stage of the process in a less granular fashion, and is
 * mostly used here to track things internally with respect to particular
 * iterators.
 *
 * Clients of this function should not have to handle any jbvBinary values
 * (since recursive calls will deal with this), provided skipNested is false.
 * It is our job to expand the jbvBinary representation without bothering them
 * with it.  However, clients should not take it upon themselves to touch array
 * or Object element/pair buffers, since their element/pair pointers are
 * garbage.
 *
 * *val is not meaningful when the result is WJB_DONE, WJB_END_ARRAY or
 * WJB_END_OBJECT.  However, we set val->type = jbvNull in those cases,
 * so that callers may assume that val->type is always well-defined.
 */
pub unsafe fn JsonbIteratorNext(
    it: *mut *mut JsonbIterator,
    val: *mut JsonbValue,
    skipNested: bool,
) -> JsonbIteratorToken {
    if (*it).is_null() {
        (*val).type_ = jbvNull;
        return WJB_DONE;
    }

    /*
     * When stepping into a nested container, we jump back here to start
     * processing the child. We will not recurse further in one call, because
     * processing the child will always begin in JBI_ARRAY_START or
     * JBI_OBJECT_START state.
     */
    loop {
        // recurse:
        match (**it).state {
            JBI_ARRAY_START => {
                /* Set v to array on first array call */
                (*val).type_ = jbvArray;
                (*val).val.array.nElems = (**it).nElems as c_int;

                /*
                 * v->val.array.elems is not actually set, because we aren't doing
                 * a full conversion
                 */
                (*val).val.array.rawScalar = (**it).isScalar;
                (**it).curIndex = 0;
                (**it).curDataOffset = 0;
                (**it).curValueOffset = 0; /* not actually used */
                /* Set state for next call */
                (**it).state = JBI_ARRAY_ELEM;
                return WJB_BEGIN_ARRAY;
            }

            JBI_ARRAY_ELEM => {
                if (**it).curIndex as uint32 >= (**it).nElems {
                    /*
                     * All elements within array already processed.  Report this
                     * to caller, and give it back original parent iterator (which
                     * independently tracks iteration progress at its level of
                     * nesting).
                     */
                    *it = freeAndGetParent(*it);
                    (*val).type_ = jbvNull;
                    return WJB_END_ARRAY;
                }

                fillJsonbValue(
                    (**it).container,
                    (**it).curIndex,
                    (**it).dataProper,
                    (**it).curDataOffset,
                    val,
                );

                let mut off = (**it).curDataOffset;
                JBE_ADVANCE_OFFSET(&mut off, *(**it).children.add((**it).curIndex as usize));
                (**it).curDataOffset = off;
                (**it).curIndex += 1;

                if !IsAJsonbScalar(val) && !skipNested {
                    /* Recurse into container. */
                    *it = iteratorFromContainer((*val).val.binary.data, *it);
                    continue; // goto recurse;
                } else {
                    /*
                     * Scalar item in array, or a container and caller didn't want
                     * us to recurse into it.
                     */
                    return WJB_ELEM;
                }
            }

            JBI_OBJECT_START => {
                /* Set v to object on first object call */
                (*val).type_ = jbvObject;
                (*val).val.object.nPairs = (**it).nElems as c_int;

                /*
                 * v->val.object.pairs is not actually set, because we aren't
                 * doing a full conversion
                 */
                (**it).curIndex = 0;
                (**it).curDataOffset = 0;
                (**it).curValueOffset = getJsonbOffset((**it).container, (**it).nElems as c_int);
                /* Set state for next call */
                (**it).state = JBI_OBJECT_KEY;
                return WJB_BEGIN_OBJECT;
            }

            JBI_OBJECT_KEY => {
                if (**it).curIndex as uint32 >= (**it).nElems {
                    /*
                     * All pairs within object already processed.  Report this to
                     * caller, and give it back original containing iterator
                     * (which independently tracks iteration progress at its level
                     * of nesting).
                     */
                    *it = freeAndGetParent(*it);
                    (*val).type_ = jbvNull;
                    return WJB_END_OBJECT;
                } else {
                    /* Return key of a key/value pair.  */
                    fillJsonbValue(
                        (**it).container,
                        (**it).curIndex,
                        (**it).dataProper,
                        (**it).curDataOffset,
                        val,
                    );
                    if (*val).type_ != jbvString {
                        elog!(ERROR, "unexpected jsonb type as object key");
                    }

                    /* Set state for next call */
                    (**it).state = JBI_OBJECT_VALUE;
                    return WJB_KEY;
                }
            }

            JBI_OBJECT_VALUE => {
                /* Set state for next call */
                (**it).state = JBI_OBJECT_KEY;

                fillJsonbValue(
                    (**it).container,
                    (**it).curIndex + (**it).nElems as c_int,
                    (**it).dataProper,
                    (**it).curValueOffset,
                    val,
                );

                let mut off1 = (**it).curDataOffset;
                JBE_ADVANCE_OFFSET(&mut off1, *(**it).children.add((**it).curIndex as usize));
                (**it).curDataOffset = off1;
                let mut off2 = (**it).curValueOffset;
                JBE_ADVANCE_OFFSET(
                    &mut off2,
                    *(**it)
                        .children
                        .add((**it).curIndex as usize + (**it).nElems as usize),
                );
                (**it).curValueOffset = off2;
                (**it).curIndex += 1;

                /*
                 * Value may be a container, in which case we recurse with new,
                 * child iterator (unless the caller asked not to, by passing
                 * skipNested).
                 */
                if !IsAJsonbScalar(val) && !skipNested {
                    *it = iteratorFromContainer((*val).val.binary.data, *it);
                    continue; // goto recurse;
                } else {
                    return WJB_VALUE;
                }
            }
        }
    }

    // unreachable in C path (the match is exhaustive and every arm returns or
    // continues), so the trailing elog/return below the switch is not needed.
}

/*
 * Initialize an iterator for iterating all elements in a container.
 */
unsafe fn iteratorFromContainer(
    container: *mut JsonbContainer,
    parent: *mut JsonbIterator,
) -> *mut JsonbIterator {
    let it: *mut JsonbIterator;

    it = palloc0(core::mem::size_of::<JsonbIterator>()) as *mut JsonbIterator;
    (*it).container = container;
    (*it).parent = parent;
    (*it).nElems = JsonContainerSize(container);

    /* Array starts just after header */
    (*it).children = &raw mut (*container).children as *mut JEntry;

    match (*container).header & (JB_FARRAY | JB_FOBJECT) {
        JB_FARRAY => {
            (*it).dataProper = ((*it).children as *mut c_char)
                .add((*it).nElems as usize * core::mem::size_of::<JEntry>());
            (*it).isScalar = JsonContainerIsScalar(container);
            /* This is either a "raw scalar", or an array */
            Assert!(!(*it).isScalar || (*it).nElems == 1);

            (*it).state = JBI_ARRAY_START;
        }

        JB_FOBJECT => {
            (*it).dataProper = ((*it).children as *mut c_char)
                .add((*it).nElems as usize * core::mem::size_of::<JEntry>() * 2);
            (*it).state = JBI_OBJECT_START;
        }

        _ => {
            elog!(ERROR, "unknown type of jsonb container");
        }
    }

    it
}

/*
 * JsonbIteratorNext() worker:	Return parent, while freeing memory for current
 * iterator
 */
unsafe fn freeAndGetParent(it: *mut JsonbIterator) -> *mut JsonbIterator {
    let v: *mut JsonbIterator = (*it).parent;

    pfree(it as *mut c_void);
    v
}

/*
 * Worker for "contains" operator's function
 *
 * Formally speaking, containment is top-down, unordered subtree isomorphism.
 *
 * Takes iterators that belong to some container type.  These iterators
 * "belong" to those values in the sense that they've just been initialized in
 * respect of them by the caller (perhaps in a nested fashion).
 *
 * "val" is lhs Jsonb, and mContained is rhs Jsonb when called from top level.
 * We determine if mContained is contained within val.
 */
pub unsafe fn JsonbDeepContains(
    val: *mut *mut JsonbIterator,
    mContained: *mut *mut JsonbIterator,
) -> bool {
    let mut vval: JsonbValue = core::mem::zeroed();
    let mut vcontained: JsonbValue = core::mem::zeroed();
    let rval: JsonbIteratorToken;
    let mut rcont: JsonbIteratorToken;

    /*
     * Guard against stack overflow due to overly complex Jsonb.
     *
     * Functions called here independently take this precaution, but that
     * might not be sufficient since this is also a recursive function.
     */
    check_stack_depth();

    rval = JsonbIteratorNext(val, &mut vval, false);
    rcont = JsonbIteratorNext(mContained, &mut vcontained, false);

    if rval != rcont {
        /*
         * The differing return values can immediately be taken as indicating
         * two differing container types at this nesting level, which is
         * sufficient reason to give up entirely (but it should be the case
         * that they're both some container type).
         */
        Assert!(rval == WJB_BEGIN_OBJECT || rval == WJB_BEGIN_ARRAY);
        Assert!(rcont == WJB_BEGIN_OBJECT || rcont == WJB_BEGIN_ARRAY);
        return false;
    } else if rcont == WJB_BEGIN_OBJECT {
        Assert!(vval.type_ == jbvObject);
        Assert!(vcontained.type_ == jbvObject);

        /*
         * If the lhs has fewer pairs than the rhs, it can't possibly contain
         * the rhs.  (This conclusion is safe only because we de-duplicate
         * keys in all Jsonb objects; thus there can be no corresponding
         * optimization in the array case.)  The case probably won't arise
         * often, but since it's such a cheap check we may as well make it.
         */
        if vval.val.object.nPairs < vcontained.val.object.nPairs {
            return false;
        }

        /* Work through rhs "is it contained within?" object */
        loop {
            let lhsVal: *mut JsonbValue; /* lhsVal is from pair in lhs object */
            let mut lhsValBuf: JsonbValue = core::mem::zeroed();

            rcont = JsonbIteratorNext(mContained, &mut vcontained, false);

            /*
             * When we get through caller's rhs "is it contained within?"
             * object without failing to find one of its values, it's
             * contained.
             */
            if rcont == WJB_END_OBJECT {
                return true;
            }

            Assert!(rcont == WJB_KEY);
            Assert!(vcontained.type_ == jbvString);

            /* First, find value by key... */
            lhsVal = getKeyJsonValueFromContainer(
                (**val).container,
                vcontained.val.string.val,
                vcontained.val.string.len,
                &mut lhsValBuf,
            );
            if lhsVal.is_null() {
                return false;
            }

            /*
             * ...at this stage it is apparent that there is at least a key
             * match for this rhs pair.
             */
            rcont = JsonbIteratorNext(mContained, &mut vcontained, true);

            Assert!(rcont == WJB_VALUE);

            /*
             * Compare rhs pair's value with lhs pair's value just found using
             * key
             */
            if (*lhsVal).type_ != vcontained.type_ {
                return false;
            } else if IsAJsonbScalar(lhsVal) {
                if !equalsJsonbScalarValue(lhsVal, &mut vcontained) {
                    return false;
                }
            } else {
                /* Nested container value (object or array) */
                let mut nestval: *mut JsonbIterator;
                let mut nestContained: *mut JsonbIterator;

                Assert!((*lhsVal).type_ == jbvBinary);
                Assert!(vcontained.type_ == jbvBinary);

                nestval = JsonbIteratorInit((*lhsVal).val.binary.data);
                nestContained = JsonbIteratorInit(vcontained.val.binary.data);

                /*
                 * Match "value" side of rhs datum object's pair recursively.
                 * It's a nested structure.
                 *
                 * Note that nesting still has to "match up" at the right
                 * nesting sub-levels.  However, there need only be zero or
                 * more matching pairs (or elements) at each nesting level
                 * (provided the *rhs* pairs/elements *all* match on each
                 * level), which enables searching nested structures for a
                 * single String or other primitive type sub-datum quite
                 * effectively (provided the user constructed the rhs nested
                 * structure such that we "know where to look").
                 *
                 * In other words, the mapping of container nodes in the rhs
                 * "vcontained" Jsonb to internal nodes on the lhs is
                 * injective, and parent-child edges on the rhs must be mapped
                 * to parent-child edges on the lhs to satisfy the condition
                 * of containment (plus of course the mapped nodes must be
                 * equal).
                 */
                if !JsonbDeepContains(&mut nestval, &mut nestContained) {
                    return false;
                }
            }
        }
    } else if rcont == WJB_BEGIN_ARRAY {
        let mut lhsConts: *mut JsonbValue = std::ptr::null_mut();
        let mut nLhsElems: uint32 = vval.val.array.nElems as uint32;

        Assert!(vval.type_ == jbvArray);
        Assert!(vcontained.type_ == jbvArray);

        /*
         * Handle distinction between "raw scalar" pseudo arrays, and real
         * arrays.
         *
         * A raw scalar may contain another raw scalar, and an array may
         * contain a raw scalar, but a raw scalar may not contain an array. We
         * don't do something like this for the object case, since objects can
         * only contain pairs, never raw scalars (a pair is represented by an
         * rhs object argument with a single contained pair).
         */
        if vval.val.array.rawScalar && !vcontained.val.array.rawScalar {
            return false;
        }

        /* Work through rhs "is it contained within?" array */
        loop {
            rcont = JsonbIteratorNext(mContained, &mut vcontained, true);

            /*
             * When we get through caller's rhs "is it contained within?"
             * array without failing to find one of its values, it's
             * contained.
             */
            if rcont == WJB_END_ARRAY {
                return true;
            }

            Assert!(rcont == WJB_ELEM);

            if IsAJsonbScalar(&mut vcontained) {
                if findJsonbValueFromContainer((**val).container, JB_FARRAY, &mut vcontained)
                    .is_null()
                {
                    return false;
                }
            } else {
                let mut i: uint32;

                /*
                 * If this is first container found in rhs array (at this
                 * depth), initialize temp lhs array of containers
                 */
                if lhsConts.is_null() {
                    let mut j: uint32 = 0;

                    /* Make room for all possible values */
                    lhsConts = palloc(core::mem::size_of::<JsonbValue>() * nLhsElems as usize)
                        as *mut JsonbValue;

                    i = 0;
                    while i < nLhsElems {
                        /* Store all lhs elements in temp array */
                        rcont = JsonbIteratorNext(val, &mut vval, true);
                        Assert!(rcont == WJB_ELEM);

                        if vval.type_ == jbvBinary {
                            *lhsConts.add(j as usize) = vval;
                            j += 1;
                        }
                        i += 1;
                    }

                    /* No container elements in temp array, so give up now */
                    if j == 0 {
                        return false;
                    }

                    /* We may have only partially filled array */
                    nLhsElems = j;
                }

                /* XXX: Nested array containment is O(N^2) */
                i = 0;
                while i < nLhsElems {
                    /* Nested container value (object or array) */
                    let mut nestval: *mut JsonbIterator;
                    let mut nestContained: *mut JsonbIterator;
                    let contains: bool;

                    nestval = JsonbIteratorInit((*lhsConts.add(i as usize)).val.binary.data);
                    nestContained = JsonbIteratorInit(vcontained.val.binary.data);

                    contains = JsonbDeepContains(&mut nestval, &mut nestContained);

                    if !nestval.is_null() {
                        pfree(nestval as *mut c_void);
                    }
                    if !nestContained.is_null() {
                        pfree(nestContained as *mut c_void);
                    }
                    if contains {
                        break;
                    }
                    i += 1;
                }

                /*
                 * Report rhs container value is not contained if couldn't
                 * match rhs container to *some* lhs cont
                 */
                if i == nLhsElems {
                    return false;
                }
            }
        }
    } else {
        elog!(ERROR, "invalid jsonb container type");
    }

    #[allow(unreachable_code)]
    {
        elog!(ERROR, "unexpectedly fell off end of jsonb container");
        false
    }
}

/*
 * Hash a JsonbValue scalar value, mixing the hash value into an existing
 * hash provided by the caller.
 *
 * Some callers may wish to independently XOR in JB_FOBJECT and JB_FARRAY
 * flags.
 */
pub unsafe fn JsonbHashScalarValue(scalarVal: *const JsonbValue, hash: *mut uint32) {
    let tmp: uint32;

    /* Compute hash value for scalarVal */
    match (*scalarVal).type_ {
        jbvNull => {
            tmp = 0x01;
        }
        jbvString => {
            tmp = DatumGetUInt32(hash_any(
                (*scalarVal).val.string.val as *const core::ffi::c_uchar,
                (*scalarVal).val.string.len,
            ));
        }
        jbvNumeric => {
            /* Must hash equal numerics to equal hash codes */
            tmp = DatumGetUInt32(DirectFunctionCall1!(
                hash_numeric,
                NumericGetDatum((*scalarVal).val.numeric)
            ));
        }
        jbvBool => {
            tmp = if (*scalarVal).val.boolean { 0x02 } else { 0x04 };
        }
        _ => {
            elog!(ERROR, "invalid jsonb scalar type");
            #[allow(unreachable_code)]
            {
                tmp = 0; /* keep compiler quiet */
            }
        }
    }

    /*
     * Combine hash values of successive keys, values and elements by rotating
     * the previous value left 1 bit, then XOR'ing in the new
     * key/value/element's hash value.
     */
    *hash = pg_rotate_left32(*hash, 1);
    *hash ^= tmp;
}

/*
 * Hash a value to a 64-bit value, with a seed. Otherwise, similar to
 * JsonbHashScalarValue.
 */
pub unsafe fn JsonbHashScalarValueExtended(
    scalarVal: *const JsonbValue,
    hash: *mut uint64,
    seed: uint64,
) {
    let tmp: uint64;

    match (*scalarVal).type_ {
        jbvNull => {
            tmp = seed + 0x01;
        }
        jbvString => {
            tmp = DatumGetUInt64(hash_any_extended(
                (*scalarVal).val.string.val as *const core::ffi::c_uchar,
                (*scalarVal).val.string.len,
                seed,
            ));
        }
        jbvNumeric => {
            tmp = DatumGetUInt64(DirectFunctionCall2!(
                hash_numeric_extended,
                NumericGetDatum((*scalarVal).val.numeric),
                UInt64GetDatum(seed)
            ));
        }
        jbvBool => {
            if seed != 0 {
                tmp = DatumGetUInt64(DirectFunctionCall2!(
                    hashcharextended,
                    BoolGetDatum((*scalarVal).val.boolean),
                    UInt64GetDatum(seed)
                ));
            } else {
                tmp = if (*scalarVal).val.boolean { 0x02 } else { 0x04 };
            }
        }
        _ => {
            elog!(ERROR, "invalid jsonb scalar type");
            #[allow(unreachable_code)]
            {
                tmp = 0;
            }
        }
    }

    *hash = ROTATE_HIGH_AND_LOW_32BITS(*hash);
    *hash ^= tmp;
}

/*
 * Are two scalar JsonbValues of the same type a and b equal?
 */
unsafe fn equalsJsonbScalarValue(a: *mut JsonbValue, b: *mut JsonbValue) -> bool {
    if (*a).type_ == (*b).type_ {
        match (*a).type_ {
            jbvNull => {
                return true;
            }
            jbvString => {
                return lengthCompareJsonbStringValue(
                    a as *const c_void,
                    b as *const c_void,
                ) == 0;
            }
            jbvNumeric => {
                return DatumGetBool(DirectFunctionCall2!(
                    numeric_eq,
                    PointerGetDatum((*a).val.numeric as *const c_void),
                    PointerGetDatum((*b).val.numeric as *const c_void)
                ));
            }
            jbvBool => {
                return (*a).val.boolean == (*b).val.boolean;
            }
            _ => {
                elog!(ERROR, "invalid jsonb scalar type");
            }
        }
    }
    elog!(ERROR, "jsonb scalar type mismatch");
    #[allow(unreachable_code)]
    {
        false
    }
}

/*
 * Compare two scalar JsonbValues, returning -1, 0, or 1.
 *
 * Strings are compared using the default collation.  Used by B-tree
 * operators, where a lexical sort order is generally expected.
 */
unsafe fn compareJsonbScalarValue(a: *mut JsonbValue, b: *mut JsonbValue) -> c_int {
    if (*a).type_ == (*b).type_ {
        match (*a).type_ {
            jbvNull => {
                return 0;
            }
            jbvString => {
                return varstr_cmp(
                    (*a).val.string.val,
                    (*a).val.string.len,
                    (*b).val.string.val,
                    (*b).val.string.len,
                    DEFAULT_COLLATION_OID,
                );
            }
            jbvNumeric => {
                return DatumGetInt32(DirectFunctionCall2!(
                    numeric_cmp,
                    PointerGetDatum((*a).val.numeric as *const c_void),
                    PointerGetDatum((*b).val.numeric as *const c_void)
                ));
            }
            jbvBool => {
                if (*a).val.boolean == (*b).val.boolean {
                    return 0;
                } else if (*a).val.boolean & !(*b).val.boolean {
                    return 1;
                } else {
                    return -1;
                }
            }
            _ => {
                elog!(ERROR, "invalid jsonb scalar type");
            }
        }
    }
    elog!(ERROR, "jsonb scalar type mismatch");
    #[allow(unreachable_code)]
    {
        -1
    }
}

/*
 * Functions for manipulating the resizable buffer used by convertJsonb and
 * its subroutines.
 */

/*
 * Reserve 'len' bytes, at the end of the buffer, enlarging it if necessary.
 * Returns the offset to the reserved area. The caller is expected to fill
 * the reserved area later with copyToBuffer().
 */
unsafe fn reserveFromBuffer(buffer: StringInfo, len: c_int) -> c_int {
    let offset: c_int;

    /* Make more room if needed */
    enlargeStringInfo(buffer, len);

    /* remember current offset */
    offset = (*buffer).len;

    /* reserve the space */
    (*buffer).len += len;

    /*
     * Keep a trailing null in place, even though it's not useful for us; it
     * seems best to preserve the invariants of StringInfos.
     */
    *(*buffer).data.add((*buffer).len as usize) = b'\0' as c_char;

    offset
}

/*
 * Copy 'len' bytes to a previously reserved area in buffer.
 */
unsafe fn copyToBuffer(buffer: StringInfo, offset: c_int, data: *const c_void, len: c_int) {
    memcpy(
        (*buffer).data.add(offset as usize) as *mut c_void,
        data,
        len as usize,
    );
}

/*
 * A shorthand for reserveFromBuffer + copyToBuffer.
 */
unsafe fn appendToBuffer(buffer: StringInfo, data: *const c_void, len: c_int) {
    let offset: c_int;

    offset = reserveFromBuffer(buffer, len);
    copyToBuffer(buffer, offset, data, len);
}

/*
 * Append padding, so that the length of the StringInfo is int-aligned.
 * Returns the number of padding bytes appended.
 */
unsafe fn padBufferToInt(buffer: StringInfo) -> i16 {
    let padlen: c_int;
    let mut p: c_int;
    let offset: c_int;

    padlen = INTALIGN((*buffer).len as usize) as c_int - (*buffer).len;

    offset = reserveFromBuffer(buffer, padlen);

    /* padlen must be small, so this is probably faster than a memset */
    p = 0;
    while p < padlen {
        *(*buffer).data.add((offset + p) as usize) = b'\0' as c_char;
        p += 1;
    }

    padlen as i16
}

/*
 * Given a JsonbValue, convert to Jsonb. The result is palloc'd.
 */
unsafe fn convertToJsonb(val: *mut JsonbValue) -> *mut Jsonb {
    let mut buffer: StringInfoData = core::mem::zeroed();
    let mut jentry: JEntry = 0;
    let res: *mut Jsonb;

    /* Should not already have binary representation */
    Assert!((*val).type_ != jbvBinary);

    /* Allocate an output buffer. It will be enlarged as needed */
    initStringInfo(&mut buffer);

    /* Make room for the varlena header */
    reserveFromBuffer(&mut buffer, VARHDRSZ as c_int);

    convertJsonbValue(&mut buffer, &mut jentry, val, 0);

    /*
     * Note: the JEntry of the root is discarded. Therefore the root
     * JsonbContainer struct must contain enough information to tell what kind
     * of value it is.
     */

    res = buffer.data as *mut Jsonb;

    SET_VARSIZE(res as *mut c_char, buffer.len);

    res
}

/*
 * Subroutine of convertJsonb: serialize a single JsonbValue into buffer.
 *
 * The JEntry header for this node is returned in *header.  It is filled in
 * with the length of this value and appropriate type bits.  If we wish to
 * store an end offset rather than a length, it is the caller's responsibility
 * to adjust for that.
 *
 * If the value is an array or an object, this recurses. 'level' is only used
 * for debugging purposes.
 */
unsafe fn convertJsonbValue(buffer: StringInfo, header: *mut JEntry, val: *mut JsonbValue, level: c_int) {
    check_stack_depth();

    if val.is_null() {
        return;
    }

    /*
     * A JsonbValue passed as val should never have a type of jbvBinary, and
     * neither should any of its sub-components. Those values will be produced
     * by convertJsonbArray and convertJsonbObject, the results of which will
     * not be passed back to this function as an argument.
     */

    if IsAJsonbScalar(val) {
        convertJsonbScalar(buffer, header, val);
    } else if (*val).type_ == jbvArray {
        convertJsonbArray(buffer, header, val, level);
    } else if (*val).type_ == jbvObject {
        convertJsonbObject(buffer, header, val, level);
    } else {
        elog!(ERROR, "unknown type of jsonb container to convert");
    }
}

unsafe fn convertJsonbArray(buffer: StringInfo, header: *mut JEntry, val: *mut JsonbValue, level: c_int) {
    let base_offset: c_int;
    let mut jentry_offset: c_int;
    let mut i: c_int;
    let mut totallen: c_int;
    let mut containerhead: uint32;
    let nElems: c_int = (*val).val.array.nElems;

    /* Remember where in the buffer this array starts. */
    base_offset = (*buffer).len;

    /* Align to 4-byte boundary (any padding counts as part of my data) */
    padBufferToInt(buffer);

    /*
     * Construct the header Jentry and store it in the beginning of the
     * variable-length payload.
     */
    containerhead = nElems as uint32 | JB_FARRAY;
    if (*val).val.array.rawScalar {
        Assert!(nElems == 1);
        Assert!(level == 0);
        containerhead |= JB_FSCALAR;
    }

    appendToBuffer(
        buffer,
        &containerhead as *const uint32 as *const c_void,
        core::mem::size_of::<uint32>() as c_int,
    );

    /* Reserve space for the JEntries of the elements. */
    jentry_offset = reserveFromBuffer(buffer, core::mem::size_of::<JEntry>() as c_int * nElems);

    totallen = 0;
    i = 0;
    while i < nElems {
        let elem: *mut JsonbValue = &raw mut *(*val).val.array.elems.add(i as usize);
        let len: c_int;
        let mut meta: JEntry = 0;

        /*
         * Convert element, producing a JEntry and appending its
         * variable-length data to buffer
         */
        convertJsonbValue(buffer, &mut meta, elem, level + 1);

        len = JBE_OFFLENFLD(meta) as c_int;
        totallen += len;

        /*
         * Bail out if total variable-length data exceeds what will fit in a
         * JEntry length field.  We check this in each iteration, not just
         * once at the end, to forestall possible integer overflow.
         */
        if totallen > JENTRY_OFFLENMASK as c_int {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(
                ERROR,
                errmsg!(
                    "total size of jsonb array elements exceeds the maximum of {} bytes",
                    JENTRY_OFFLENMASK
                )
            );
        }

        /*
         * Convert each JB_OFFSET_STRIDE'th length to an offset.
         */
        if (i % JB_OFFSET_STRIDE) == 0 {
            meta = (meta & JENTRY_TYPEMASK) | totallen as uint32 | JENTRY_HAS_OFF;
        }

        copyToBuffer(
            buffer,
            jentry_offset,
            &meta as *const JEntry as *const c_void,
            core::mem::size_of::<JEntry>() as c_int,
        );
        jentry_offset += core::mem::size_of::<JEntry>() as c_int;
        i += 1;
    }

    /* Total data size is everything we've appended to buffer */
    totallen = (*buffer).len - base_offset;

    /* Check length again, since we didn't include the metadata above */
    if totallen > JENTRY_OFFLENMASK as c_int {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(
            ERROR,
            errmsg!(
                "total size of jsonb array elements exceeds the maximum of {} bytes",
                JENTRY_OFFLENMASK
            )
        );
    }

    /* Initialize the header of this node in the container's JEntry array */
    *header = JENTRY_ISCONTAINER | totallen as uint32;
}

unsafe fn convertJsonbObject(buffer: StringInfo, header: *mut JEntry, val: *mut JsonbValue, level: c_int) {
    let base_offset: c_int;
    let mut jentry_offset: c_int;
    let mut i: c_int;
    let mut totallen: c_int;
    let containerheader: uint32;
    let nPairs: c_int = (*val).val.object.nPairs;

    /* Remember where in the buffer this object starts. */
    base_offset = (*buffer).len;

    /* Align to 4-byte boundary (any padding counts as part of my data) */
    padBufferToInt(buffer);

    /*
     * Construct the header Jentry and store it in the beginning of the
     * variable-length payload.
     */
    containerheader = nPairs as uint32 | JB_FOBJECT;
    appendToBuffer(
        buffer,
        &containerheader as *const uint32 as *const c_void,
        core::mem::size_of::<uint32>() as c_int,
    );

    /* Reserve space for the JEntries of the keys and values. */
    jentry_offset = reserveFromBuffer(buffer, core::mem::size_of::<JEntry>() as c_int * nPairs * 2);

    /*
     * Iterate over the keys, then over the values, since that is the ordering
     * we want in the on-disk representation.
     */
    totallen = 0;
    i = 0;
    while i < nPairs {
        let pair: *mut JsonbPair = &raw mut *(*val).val.object.pairs.add(i as usize);
        let len: c_int;
        let mut meta: JEntry = 0;

        /*
         * Convert key, producing a JEntry and appending its variable-length
         * data to buffer
         */
        convertJsonbScalar(buffer, &mut meta, &raw mut (*pair).key);

        len = JBE_OFFLENFLD(meta) as c_int;
        totallen += len;

        /*
         * Bail out if total variable-length data exceeds what will fit in a
         * JEntry length field.  We check this in each iteration, not just
         * once at the end, to forestall possible integer overflow.
         */
        if totallen > JENTRY_OFFLENMASK as c_int {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(
                ERROR,
                errmsg!(
                    "total size of jsonb object elements exceeds the maximum of {} bytes",
                    JENTRY_OFFLENMASK
                )
            );
        }

        /*
         * Convert each JB_OFFSET_STRIDE'th length to an offset.
         */
        if (i % JB_OFFSET_STRIDE) == 0 {
            meta = (meta & JENTRY_TYPEMASK) | totallen as uint32 | JENTRY_HAS_OFF;
        }

        copyToBuffer(
            buffer,
            jentry_offset,
            &meta as *const JEntry as *const c_void,
            core::mem::size_of::<JEntry>() as c_int,
        );
        jentry_offset += core::mem::size_of::<JEntry>() as c_int;
        i += 1;
    }
    i = 0;
    while i < nPairs {
        let pair: *mut JsonbPair = &raw mut *(*val).val.object.pairs.add(i as usize);
        let len: c_int;
        let mut meta: JEntry = 0;

        /*
         * Convert value, producing a JEntry and appending its variable-length
         * data to buffer
         */
        convertJsonbValue(buffer, &mut meta, &raw mut (*pair).value, level + 1);

        len = JBE_OFFLENFLD(meta) as c_int;
        totallen += len;

        /*
         * Bail out if total variable-length data exceeds what will fit in a
         * JEntry length field.  We check this in each iteration, not just
         * once at the end, to forestall possible integer overflow.
         */
        if totallen > JENTRY_OFFLENMASK as c_int {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(
                ERROR,
                errmsg!(
                    "total size of jsonb object elements exceeds the maximum of {} bytes",
                    JENTRY_OFFLENMASK
                )
            );
        }

        /*
         * Convert each JB_OFFSET_STRIDE'th length to an offset.
         */
        if ((i + nPairs) % JB_OFFSET_STRIDE) == 0 {
            meta = (meta & JENTRY_TYPEMASK) | totallen as uint32 | JENTRY_HAS_OFF;
        }

        copyToBuffer(
            buffer,
            jentry_offset,
            &meta as *const JEntry as *const c_void,
            core::mem::size_of::<JEntry>() as c_int,
        );
        jentry_offset += core::mem::size_of::<JEntry>() as c_int;
        i += 1;
    }

    /* Total data size is everything we've appended to buffer */
    totallen = (*buffer).len - base_offset;

    /* Check length again, since we didn't include the metadata above */
    if totallen > JENTRY_OFFLENMASK as c_int {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(
            ERROR,
            errmsg!(
                "total size of jsonb object elements exceeds the maximum of {} bytes",
                JENTRY_OFFLENMASK
            )
        );
    }

    /* Initialize the header of this node in the container's JEntry array */
    *header = JENTRY_ISCONTAINER | totallen as uint32;
}

unsafe fn convertJsonbScalar(buffer: StringInfo, header: *mut JEntry, scalarVal: *mut JsonbValue) {
    let numlen: c_int;
    let padlen: i16;

    match (*scalarVal).type_ {
        jbvNull => {
            *header = JENTRY_ISNULL;
        }

        jbvString => {
            appendToBuffer(
                buffer,
                (*scalarVal).val.string.val as *const c_void,
                (*scalarVal).val.string.len,
            );

            *header = (*scalarVal).val.string.len as uint32;
        }

        jbvNumeric => {
            numlen = VARSIZE_ANY((*scalarVal).val.numeric as *const c_char) as c_int;
            padlen = padBufferToInt(buffer);

            appendToBuffer(buffer, (*scalarVal).val.numeric as *const c_void, numlen);

            *header = JENTRY_ISNUMERIC | (padlen as c_int + numlen) as uint32;
        }

        jbvBool => {
            *header = if (*scalarVal).val.boolean {
                JENTRY_ISBOOL_TRUE
            } else {
                JENTRY_ISBOOL_FALSE
            };
        }

        jbvDatetime => {
            let mut buf: [c_char; MAXDATELEN as usize + 1] = [0; MAXDATELEN as usize + 1];
            let len: usize;

            JsonEncodeDateTime(
                buf.as_mut_ptr(),
                (*scalarVal).val.datetime.value,
                (*scalarVal).val.datetime.typid,
                &raw const (*scalarVal).val.datetime.tz,
            );
            len = strlen(buf.as_ptr());
            appendToBuffer(buffer, buf.as_ptr() as *const c_void, len as c_int);

            *header = len as uint32;
        }

        _ => {
            elog!(ERROR, "invalid jsonb scalar type");
        }
    }
}

/* TODO(pg-port): MAXDATELEN lives in utils/datetime.h (not yet ported). */
const MAXDATELEN: c_int = 128;

/*
 * Compare two jbvString JsonbValue values, a and b.
 *
 * This is a special qsort() comparator used to sort strings in certain
 * internal contexts where it is sufficient to have a well-defined sort order.
 * In particular, object pair keys are sorted according to this criteria to
 * facilitate cheap binary searches where we don't care about lexical sort
 * order.
 *
 * a and b are first sorted based on their length.  If a tie-breaker is
 * required, only then do we consider string binary equality.
 */
unsafe fn lengthCompareJsonbStringValue(a: *const c_void, b: *const c_void) -> c_int {
    let va: *const JsonbValue = a as *const JsonbValue;
    let vb: *const JsonbValue = b as *const JsonbValue;

    Assert!((*va).type_ == jbvString);
    Assert!((*vb).type_ == jbvString);

    lengthCompareJsonbString(
        (*va).val.string.val,
        (*va).val.string.len,
        (*vb).val.string.val,
        (*vb).val.string.len,
    )
}

/*
 * Subroutine for lengthCompareJsonbStringValue
 *
 * This is also useful separately to implement binary search on
 * JsonbContainers.
 */
unsafe fn lengthCompareJsonbString(
    val1: *const c_char,
    len1: c_int,
    val2: *const c_char,
    len2: c_int,
) -> c_int {
    if len1 == len2 {
        memcmp(val1 as *const c_void, val2 as *const c_void, len1 as usize)
    } else {
        if len1 > len2 {
            1
        } else {
            -1
        }
    }
}

/*
 * qsort_arg() comparator to compare JsonbPair values.
 *
 * Third argument 'binequal' may point to a bool. If it's set, *binequal is set
 * to true iff a and b have full binary equality, since some callers have an
 * interest in whether the two values are equal or merely equivalent.
 *
 * N.B: String comparisons here are "length-wise"
 *
 * Pairs with equals keys are ordered such that the order field is respected.
 */
unsafe extern "C" fn lengthCompareJsonbPair(
    a: *const c_void,
    b: *const c_void,
    binequal: *mut c_void,
) -> c_int {
    let pa: *const JsonbPair = a as *const JsonbPair;
    let pb: *const JsonbPair = b as *const JsonbPair;
    let mut res: c_int;

    res = lengthCompareJsonbStringValue(
        &raw const (*pa).key as *const c_void,
        &raw const (*pb).key as *const c_void,
    );
    if res == 0 && !binequal.is_null() {
        *(binequal as *mut bool) = true;
    }

    /*
     * Guarantee keeping order of equal pair.  Unique algorithm will prefer
     * first element as value.
     */
    if res == 0 {
        res = if (*pa).order > (*pb).order { -1 } else { 1 };
    }

    res
}

/*
 * Sort and unique-ify pairs in JsonbValue object
 */
unsafe fn uniqueifyJsonbObject(object: *mut JsonbValue, unique_keys: bool, skip_nulls: bool) {
    let mut hasNonUniq: bool = false;

    Assert!((*object).type_ == jbvObject);

    if (*object).val.object.nPairs > 1 {
        qsort_arg(
            (*object).val.object.pairs as *mut c_void,
            (*object).val.object.nPairs as Size,
            core::mem::size_of::<JsonbPair>() as Size,
            lengthCompareJsonbPair,
            &mut hasNonUniq as *mut bool as *mut c_void,
        );
    }

    if hasNonUniq && unique_keys {
        let _ = errcode(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE);
        ereport!(ERROR, errmsg!("duplicate JSON object key value"));
    }

    if hasNonUniq || skip_nulls {
        let mut ptr: *mut JsonbPair;
        let mut res: *mut JsonbPair;

        while skip_nulls
            && (*object).val.object.nPairs > 0
            && (*(*object).val.object.pairs).value.type_ == jbvNull
        {
            /* If skip_nulls is true, remove leading items with null */
            (*object).val.object.pairs = (*object).val.object.pairs.add(1);
            (*object).val.object.nPairs -= 1;
        }

        if (*object).val.object.nPairs > 0 {
            ptr = (*object).val.object.pairs.add(1);
            res = (*object).val.object.pairs;

            while ptr.offset_from((*object).val.object.pairs) < (*object).val.object.nPairs as isize {
                /* Avoid copying over duplicate or null */
                if lengthCompareJsonbStringValue(ptr as *const c_void, res as *const c_void) != 0
                    && (!skip_nulls || (*ptr).value.type_ != jbvNull)
                {
                    res = res.add(1);
                    if ptr != res {
                        memcpy(
                            res as *mut c_void,
                            ptr as *const c_void,
                            core::mem::size_of::<JsonbPair>(),
                        );
                    }
                }
                ptr = ptr.add(1);
            }

            (*object).val.object.nPairs =
                (res.offset_from((*object).val.object.pairs) + 1) as c_int;
        }
    }
}
