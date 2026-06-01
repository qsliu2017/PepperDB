//! rangetypes.rs
//!   I/O functions, operators, and support functions for range types.
//!
//! The stored (serialized) format of a range value is:
//!
//!	4 bytes: varlena header
//!	4 bytes: range type's OID
//!	Lower boundary value, if any, aligned according to subtype's typalign
//!	Upper boundary value, if any, aligned according to subtype's typalign
//!	1 byte for flags
//!
//! This representation is chosen to avoid needing any padding before the
//! lower boundary value, even when it requires double alignment.  We can
//! expect that the varlena header is presented to us on a suitably aligned
//! boundary (possibly after detoasting), and then the lower boundary is too.
//! Note that this means we can't work with a packed (short varlena header)
//! value; we must detoast it first.
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/rangetypes.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/utils/adt/rangetypes.c

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use crate::prelude::*;

use std::ffi::c_int as _c_int; // (kept explicit; c_int already in prelude)

use crate::c::{bytea, int16, int32, int64, text, uint32, uint64, Pointer, Size};
use crate::postgres_ext::Oid;

use crate::access::stratnum::{
    BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber, BTLessEqualStrategyNumber,
    BTLessStrategyNumber,
};
use crate::access::tupmacs::{
    att_addlength_datum, att_addlength_pointer, att_align_datum, att_align_nominal,
    att_align_pointer, fetch_att, store_att_byval,
};
use crate::catalog::pg_type_d::BOOLOID;
use crate::lib::stringinfo::{
    appendBinaryStringInfo, appendStringInfoChar, appendStringInfoString, initStringInfo,
    makeStringInfo, StringInfo, StringInfoData,
};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgbyte, pq_getmsgbytes, pq_getmsgend, pq_getmsgint,
    pq_sendbytes, pq_sendint32, pq_sendint8,
};
use crate::miscadmin::check_stack_depth;
use crate::nodes::makefuncs::{makeBoolConst, makeConst, make_andclause, make_opclause};
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{linitial, list_length, lsecond};
use crate::nodes::primnodes::{Const, Expr, FuncExpr};
use crate::nodes::supportnodes::SupportRequestSimplify;
use crate::optimizer::cost::cost_qual_eval_node;
use crate::optimizer::optimizer::{contain_volatile_functions, cpu_operator_cost, PlannerInfo};
use crate::common::hashfn::{hash_uint32, hash_uint32_extended};
use crate::port::pg_bitutils::pg_rotate_left32;
use crate::utils::adt::date::{
    DateADTGetDatum, DatumGetDateADT, DATE_NOT_FINITE, IS_VALID_DATE, Timestamp, USECS_PER_SEC,
};
use crate::utils::adt::date::DateADT;
use crate::utils::adt::varlena::text_to_cstring;
use crate::utils::fmgr::{
    fmgr_info_cxt, get_fn_expr_argtype, get_fn_expr_rettype, FmgrInfo, FunctionCall1Coll,
    FunctionCall2Coll, FunctionCallInfo, InputFunctionCallSafe, OutputFunctionCall,
    ReceiveFunctionCall, SendFunctionCall,
};
use crate::utils::sort::sortsupport::{SortSupport, SortSupportData};
use crate::varatt::{
    SET_VARSIZE, SET_VARSIZE_SHORT, VARATT_IS_4B_U, VARATT_IS_EXTERNAL, VARATT_IS_SHORT,
    VARDATA, VARHDRSZ, VARHDRSZ_SHORT, VARSIZE, VARSIZE_1B,
};

use crate::{
    list_make2, makeNode, Assert, DirectFunctionCall1, DirectFunctionCall2, IsA,
    InitFunctionCallInfoData, FunctionCallInvoke, LOCAL_FCINFO,
    PG_ARGISNULL, PG_DETOAST_DATUM, PG_DETOAST_DATUM_COPY, PG_DETOAST_DATUM_PACKED, PG_FREE_IF_COPY,
    PG_GETARG_DATUM, PG_GETARG_FLOAT8, PG_GETARG_INT32, PG_GETARG_INT64, PG_GETARG_OID,
    PG_GETARG_POINTER, PG_GETARG_TEXT_PP, PG_RETURN_BOOL, PG_RETURN_BYTEA_P, SOFT_ERROR_OCCURRED,
    PG_RETURN_CSTRING, PG_RETURN_DATUM, PG_RETURN_FLOAT8, PG_RETURN_INT32, PG_RETURN_NULL,
    PG_RETURN_POINTER, PG_RETURN_UINT64, PG_RETURN_VOID,
};

use crate::port::pgstrcasecmp::pg_strncasecmp;

// ---- varatt.h short-header helpers (not in crate::varatt) + small shims ----
const VARATT_SHORT_MAX: uint32 = 0x7F;
#[inline]
const fn ROTATE_HIGH_AND_LOW_32BITS(v: uint64) -> uint64 {
    ((v << 1) & 0xfffffffefffffffe) | ((v >> 31) & 0x100000001)
}
#[inline]
unsafe fn VARSIZE_SHORT(ptr: *const c_char) -> uint32 {
    VARSIZE_1B(ptr)
}
#[inline]
unsafe fn VARATT_CAN_MAKE_SHORT(ptr: *const c_char) -> bool {
    VARATT_IS_4B_U(ptr)
        && (VARSIZE(ptr) as uint32 - VARHDRSZ as uint32 + VARHDRSZ_SHORT as uint32)
            <= VARATT_SHORT_MAX
}
#[inline]
unsafe fn VARATT_CONVERTED_SHORT_SIZE(ptr: *const c_char) -> uint32 {
    VARSIZE(ptr) as uint32 - VARHDRSZ as uint32 + VARHDRSZ_SHORT as uint32
}
// pq_sendbyte(buf, int): crate::libpq::pqformat only has pq_sendint8(buf, uint8).
#[inline]
unsafe fn pq_sendbyte(buf: StringInfo, byt: c_int) {
    pq_sendint8(buf, byt as uint8);
}


// PG_GETARG_TIMESTAMP: macro lives in date.rs (not #[macro_export]); local copy.
macro_rules! PG_GETARG_TIMESTAMP {
    ($fcinfo:expr, $n:expr) => {
        crate::PG_GETARG_DATUM!($fcinfo, $n) as i64 as crate::utils::adt::date::Timestamp
    };
}

extern "C" {
    fn isspace(ch: c_int) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn pfree_c(p: *mut c_void); // unused placeholder (pfree comes from prelude)
}

// ---------------------------------------------------------------------------
// Range type declarations (merged from src/include/utils/rangetypes.h).
// ---------------------------------------------------------------------------

/*
 * Ranges are varlena objects, so must meet the varlena convention that
 * the first int32 of the object contains the total object size in bytes.
 * Be sure to use VARSIZE() and SET_VARSIZE() to access it, though!
 */
#[repr(C)]
pub struct RangeType {
    pub vl_len_: int32,    /* varlena header (do not touch directly!) */
    pub rangetypid: Oid,   /* range type's own OID */
    /* Following the OID are zero to two bound values, then a flags byte */
}

pub const RANGE_EMPTY_LITERAL: &[u8] = b"empty\0";

/* Use this macro in preference to fetching rangetypid field directly */
#[inline]
pub unsafe fn RangeTypeGetOid(r: *const RangeType) -> Oid {
    (*r).rangetypid
}

/* A range's flags byte contains these bits: */
pub const RANGE_EMPTY: c_char = 0x01; /* range is empty */
pub const RANGE_LB_INC: c_char = 0x02; /* lower bound is inclusive */
pub const RANGE_UB_INC: c_char = 0x04; /* upper bound is inclusive */
pub const RANGE_LB_INF: c_char = 0x08; /* lower bound is -infinity */
pub const RANGE_UB_INF: c_char = 0x10; /* upper bound is +infinity */
pub const RANGE_LB_NULL: c_char = 0x20; /* lower bound is null (NOT USED) */
pub const RANGE_UB_NULL: c_char = 0x40; /* upper bound is null (NOT USED) */
pub const RANGE_CONTAIN_EMPTY: c_char = 0x80u8 as c_char; /* GiST internal-page entry whose
                                                          * subtree contains some empty ranges */

#[inline]
pub fn RANGE_HAS_LBOUND(flags: c_char) -> bool {
    (flags & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF)) == 0
}

#[inline]
pub fn RANGE_HAS_UBOUND(flags: c_char) -> bool {
    (flags & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF)) == 0
}

#[inline]
pub unsafe fn RangeIsEmpty(r: *const RangeType) -> bool {
    (range_get_flags(r) & RANGE_EMPTY) != 0
}

#[inline]
pub unsafe fn RangeIsOrContainsEmpty(r: *const RangeType) -> bool {
    (range_get_flags(r) & (RANGE_EMPTY | RANGE_CONTAIN_EMPTY)) != 0
}

/* Internal representation of either bound of a range (not what's on disk) */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeBound {
    pub val: Datum,      /* the bound value, if any */
    pub infinite: bool,  /* bound is +/- infinity */
    pub inclusive: bool, /* bound is inclusive (vs exclusive) */
    pub lower: bool,     /* this is the lower (vs upper) bound */
}

/*
 * fmgr functions for range type objects
 */
#[inline]
pub unsafe fn DatumGetRangeTypeP(X: Datum) -> *mut RangeType {
    PG_DETOAST_DATUM!(X) as *mut RangeType
}

#[inline]
pub unsafe fn DatumGetRangeTypePCopy(X: Datum) -> *mut RangeType {
    PG_DETOAST_DATUM_COPY!(X) as *mut RangeType
}

#[inline]
pub unsafe fn RangeTypePGetDatum(X: *const RangeType) -> Datum {
    PointerGetDatum(X as *const c_void)
}

/* PG_GETARG_RANGE_P / PG_RETURN_RANGE_P (range-specific fmgr macros) */
macro_rules! PG_GETARG_RANGE_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetRangeTypeP(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_RANGE_P_COPY {
    ($fcinfo:expr, $n:expr) => {
        DatumGetRangeTypePCopy(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_RETURN_RANGE_P {
    ($x:expr) => {
        return RangeTypePGetDatum($x)
    };
}

// ---------------------------------------------------------------------------
// errcodes.h: classification codes.  The elog shim ignores these.
// TODO(pg-port): ERRCODE_* live in utils/errcodes.h.
// ---------------------------------------------------------------------------
const ERRCODE_DATA_EXCEPTION: c_int = 0;
const ERRCODE_SYNTAX_ERROR: c_int = 0;
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;
const ERRCODE_UNDEFINED_FUNCTION: c_int = 0;
const ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE: c_int = 0;
const ERRCODE_DATETIME_VALUE_OUT_OF_RANGE: c_int = 0;

/*
 * ereturn(escontext, dummy, (...)) mirrors crate::utils::adt::rowtypes's local
 * pattern: the elog shim emits at ERROR level (errcode/errdetail/errhint dropped
 * per porting convention) and returns the dummy.  Defined textually before first
 * use since macro_rules! is not hoisted.
 */
macro_rules! ereturn {
    ($escontext:expr, $dummy:expr, $($arg:tt)*) => {{
        let _ = &$escontext;
        crate::utils::elog::emit_log(ERROR, &$($arg)*, file!(), line!());
        return $dummy;
    }};
}

// ---------------------------------------------------------------------------
// Local stubs for symbols whose home modules are not yet ported.
// ---------------------------------------------------------------------------

/*
 * TypeCacheEntry and the TYPECACHE_* flags live in utils/typcache.h, which is
 * not yet ported.  Define a faithful local copy here so range functions can
 * access the fields they need (the actual layout matches typcache.h).
 * TODO(pg-port): real TypeCacheEntry lives in src/utils/cache/typcache.rs.
 */
pub use crate::utils::cache::typcache::TypeCacheEntry;

/* Bit flags to indicate which fields a given caller needs to have set */
pub const TYPECACHE_RANGE_INFO: c_int = 0x00800;
pub const TYPECACHE_HASH_PROC_FINFO: c_int = 0x00080;
pub const TYPECACHE_HASH_EXTENDED_PROC_FINFO: c_int = 0x08000;

/*
 * fn_extra cache entry for one of the range I/O functions
 */
#[repr(C)]
pub struct RangeIOData {
    pub typcache: *mut TypeCacheEntry, /* range type's typcache entry */
    pub typioproc: FmgrInfo,           /* element type's I/O function */
    pub typioparam: Oid,               /* element type's I/O parameter */
}

/*
 * IOFuncSelector (fmgr.h).  TODO(pg-port): real enum lives in include/fmgr.h.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum IOFuncSelector {
    IOFunc_input = 0,
    IOFunc_output,
    IOFunc_receive,
    IOFunc_send,
}
use IOFuncSelector::*;

/* lookup_type_cache: TODO(pg-port): real fn lives in utils/cache/typcache.rs. */
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!() /* TODO: utils/typcache.c */
}

/* get_type_io_data: TODO(pg-port): real fn lives in utils/cache/lsyscache.rs. */
#[allow(clippy::too_many_arguments)]
unsafe fn get_type_io_data(
    _typid: Oid,
    _which_func: IOFuncSelector,
    _typlen: *mut int16,
    _typbyval: *mut bool,
    _typalign: *mut c_char,
    _typdelim: *mut c_char,
    _typioparam: *mut Oid,
    _func: *mut Oid,
) {
    unimplemented!() /* TODO: utils/cache/lsyscache.c */
}

/* get_opfamily_member: TODO(pg-port): real fn lives in utils/cache/lsyscache.rs. */
unsafe fn get_opfamily_member(
    _opfamily: Oid,
    _lefttype: Oid,
    _righttype: Oid,
    _strategy: int16,
) -> Oid {
    unimplemented!() /* TODO: utils/cache/lsyscache.c */
}

/* type_is_range: TODO(pg-port): real fn lives in utils/cache/lsyscache.rs. */
unsafe fn type_is_range(_typid: Oid) -> bool {
    unimplemented!() /* TODO: utils/cache/lsyscache.c */
}

/* format_type_be: TODO(pg-port): real fn lives in utils/adt/format_type.rs. */
unsafe fn format_type_be(_type_oid: Oid) -> *mut c_char {
    unimplemented!() /* TODO: utils/adt/format_type.c */
}

/* AggCheckCallContext: TODO(pg-port): real fn lives in executor/execAggregates. */
unsafe fn AggCheckCallContext(_fcinfo: FunctionCallInfo, _aggcontext: *mut MemoryContext) -> c_int {
    unimplemented!() /* TODO: fmgr/funcapi.c */
}

/* contain_subplans: TODO(pg-port): real fn lives in optimizer/util/clauses.rs. */
unsafe fn contain_subplans(_clause: *mut Node) -> bool {
    unimplemented!() /* TODO: optimizer/util/clauses.c */
}

/* copyObject: TODO(pg-port): real recursive copy lives in nodes/copyfuncs.rs. */
unsafe fn copyObject<T>(obj: *mut T) -> *mut T {
    // TODO(pg-port): shallow stub until copyfuncs.c is wired.
    obj
}

/* numeric_sub / numeric_float8: TODO(pg-port): real fns live in utils/adt/numeric.rs. */
unsafe fn numeric_sub(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() /* TODO: utils/adt/numeric.c */
}
unsafe fn numeric_float8(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() /* TODO: utils/adt/numeric.c */
}

/* TYPSTORAGE_PLAIN / TYPALIGN_CHAR (catalog/pg_type.h, c.h). */
const TYPSTORAGE_PLAIN: c_char = b'p' as c_char;
const TYPALIGN_CHAR: c_char = b'c' as c_char;

/*
 *----------------------------------------------------------
 * I/O FUNCTIONS
 *----------------------------------------------------------
 */

pub unsafe fn range_in(fcinfo: FunctionCallInfo) -> Datum {
    let input_str = crate::PG_GETARG_CSTRING!(fcinfo, 0);
    let rngtypoid = PG_GETARG_OID!(fcinfo, 1);
    let typmod = PG_GETARG_INT32!(fcinfo, 2);
    let escontext = (*fcinfo).context;
    let range: *mut RangeType;
    let cache: *mut RangeIOData;
    let mut flags: c_char = 0;
    let mut lbound_str: *mut c_char = null_mut();
    let mut ubound_str: *mut c_char = null_mut();
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();

    check_stack_depth(); /* recurses when subtype is a range type */

    cache = get_range_io_data(fcinfo, rngtypoid, IOFunc_input);

    /* parse */
    if !range_parse(
        input_str,
        &mut flags,
        &mut lbound_str,
        &mut ubound_str,
        escontext,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }

    /* call element type's input function */
    if RANGE_HAS_LBOUND(flags) {
        if !InputFunctionCallSafe(
            &mut (*cache).typioproc,
            lbound_str,
            (*cache).typioparam,
            typmod,
            escontext,
            &mut lower.val,
        ) {
            PG_RETURN_NULL!(fcinfo);
        }
    }
    if RANGE_HAS_UBOUND(flags) {
        if !InputFunctionCallSafe(
            &mut (*cache).typioproc,
            ubound_str,
            (*cache).typioparam,
            typmod,
            escontext,
            &mut upper.val,
        ) {
            PG_RETURN_NULL!(fcinfo);
        }
    }

    lower.infinite = (flags & RANGE_LB_INF) != 0;
    lower.inclusive = (flags & RANGE_LB_INC) != 0;
    lower.lower = true;
    upper.infinite = (flags & RANGE_UB_INF) != 0;
    upper.inclusive = (flags & RANGE_UB_INC) != 0;
    upper.lower = false;

    /* serialize and canonicalize */
    range = make_range(
        (*cache).typcache,
        &mut lower,
        &mut upper,
        (flags & RANGE_EMPTY) != 0,
        escontext,
    );

    PG_RETURN_RANGE_P!(range);
}

pub unsafe fn range_out(fcinfo: FunctionCallInfo) -> Datum {
    let range = PG_GETARG_RANGE_P!(fcinfo, 0);
    let output_str: *mut c_char;
    let cache: *mut RangeIOData;
    let flags: c_char;
    let mut lbound_str: *mut c_char = null_mut();
    let mut ubound_str: *mut c_char = null_mut();
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    check_stack_depth(); /* recurses when subtype is a range type */

    cache = get_range_io_data(fcinfo, RangeTypeGetOid(range), IOFunc_output);

    /* deserialize */
    range_deserialize((*cache).typcache, range, &mut lower, &mut upper, &mut empty);
    flags = range_get_flags(range);

    /* call element type's output function */
    if RANGE_HAS_LBOUND(flags) {
        lbound_str = OutputFunctionCall(&mut (*cache).typioproc, lower.val);
    }
    if RANGE_HAS_UBOUND(flags) {
        ubound_str = OutputFunctionCall(&mut (*cache).typioproc, upper.val);
    }

    /* construct result string */
    output_str = range_deparse(flags, lbound_str, ubound_str);

    PG_RETURN_CSTRING!(output_str);
}

/*
 * Binary representation: The first byte is the flags, then the lower bound
 * (if present), then the upper bound (if present).  Each bound is represented
 * by a 4-byte length header and the binary representation of that bound (as
 * returned by a call to the send function for the subtype).
 */

pub unsafe fn range_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let rngtypoid = PG_GETARG_OID!(fcinfo, 1);
    let typmod = PG_GETARG_INT32!(fcinfo, 2);
    let range: *mut RangeType;
    let cache: *mut RangeIOData;
    let mut flags: c_char;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();

    check_stack_depth(); /* recurses when subtype is a range type */

    cache = get_range_io_data(fcinfo, rngtypoid, IOFunc_receive);

    /* receive the flags... */
    flags = (pq_getmsgbyte(buf) as c_uchar) as c_char;

    /*
     * Mask out any unsupported flags, particularly RANGE_xB_NULL which would
     * confuse following tests.  Note that range_serialize will take care of
     * cleaning up any inconsistencies in the remaining flags.
     */
    flags &= RANGE_EMPTY | RANGE_LB_INC | RANGE_LB_INF | RANGE_UB_INC | RANGE_UB_INF;

    /* receive the bounds ... */
    if RANGE_HAS_LBOUND(flags) {
        let bound_len = pq_getmsgint(buf, 4);
        let bound_data = pq_getmsgbytes(buf, bound_len as c_int);
        let mut bound_buf: StringInfoData = std::mem::zeroed();

        initStringInfo(&mut bound_buf);
        appendBinaryStringInfo(&mut bound_buf, bound_data as *const c_void, bound_len as c_int);

        lower.val = ReceiveFunctionCall(
            &mut (*cache).typioproc,
            &mut bound_buf,
            (*cache).typioparam,
            typmod,
        );
        pfree(bound_buf.data as *mut c_void);
    } else {
        lower.val = 0 as Datum;
    }

    if RANGE_HAS_UBOUND(flags) {
        let bound_len = pq_getmsgint(buf, 4);
        let bound_data = pq_getmsgbytes(buf, bound_len as c_int);
        let mut bound_buf: StringInfoData = std::mem::zeroed();

        initStringInfo(&mut bound_buf);
        appendBinaryStringInfo(&mut bound_buf, bound_data as *const c_void, bound_len as c_int);

        upper.val = ReceiveFunctionCall(
            &mut (*cache).typioproc,
            &mut bound_buf,
            (*cache).typioparam,
            typmod,
        );
        pfree(bound_buf.data as *mut c_void);
    } else {
        upper.val = 0 as Datum;
    }

    pq_getmsgend(buf);

    /* finish constructing RangeBound representation */
    lower.infinite = (flags & RANGE_LB_INF) != 0;
    lower.inclusive = (flags & RANGE_LB_INC) != 0;
    lower.lower = true;
    upper.infinite = (flags & RANGE_UB_INF) != 0;
    upper.inclusive = (flags & RANGE_UB_INC) != 0;
    upper.lower = false;

    /* serialize and canonicalize */
    range = make_range(
        (*cache).typcache,
        &mut lower,
        &mut upper,
        (flags & RANGE_EMPTY) != 0,
        null_mut(),
    );

    PG_RETURN_RANGE_P!(range);
}

pub unsafe fn range_send(fcinfo: FunctionCallInfo) -> Datum {
    let range = PG_GETARG_RANGE_P!(fcinfo, 0);
    let buf = makeStringInfo();
    let cache: *mut RangeIOData;
    let flags: c_char;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    check_stack_depth(); /* recurses when subtype is a range type */

    cache = get_range_io_data(fcinfo, RangeTypeGetOid(range), IOFunc_send);

    /* deserialize */
    range_deserialize((*cache).typcache, range, &mut lower, &mut upper, &mut empty);
    flags = range_get_flags(range);

    /* construct output */
    pq_begintypsend(buf);

    pq_sendbyte(buf, flags as c_int);

    if RANGE_HAS_LBOUND(flags) {
        let bound = PointerGetDatum(SendFunctionCall(&mut (*cache).typioproc, lower.val)
            as *const c_void);
        let bound_len = VARSIZE(DatumGetPointer(bound) as *const c_char) - VARHDRSZ as uint32;
        let bound_data = VARDATA(DatumGetPointer(bound) as *const c_char);

        pq_sendint32(buf, bound_len);
        pq_sendbytes(buf, bound_data as *const c_void, bound_len as c_int);
    }

    if RANGE_HAS_UBOUND(flags) {
        let bound = PointerGetDatum(SendFunctionCall(&mut (*cache).typioproc, upper.val)
            as *const c_void);
        let bound_len = VARSIZE(DatumGetPointer(bound) as *const c_char) - VARHDRSZ as uint32;
        let bound_data = VARDATA(DatumGetPointer(bound) as *const c_char);

        pq_sendint32(buf, bound_len);
        pq_sendbytes(buf, bound_data as *const c_void, bound_len as c_int);
    }

    PG_RETURN_BYTEA_P!(pq_endtypsend(buf));
}

/*
 * get_range_io_data: get cached information needed for range type I/O
 *
 * The range I/O functions need a bit more cached info than other range
 * functions, so they store a RangeIOData struct in fn_extra, not just a
 * pointer to a type cache entry.
 */
unsafe fn get_range_io_data(
    fcinfo: FunctionCallInfo,
    rngtypid: Oid,
    func: IOFuncSelector,
) -> *mut RangeIOData {
    let mut cache = (*(*fcinfo).flinfo).fn_extra as *mut RangeIOData;

    if cache.is_null() || (*(*cache).typcache).type_id != rngtypid {
        let mut typlen: int16 = 0;
        let mut typbyval: bool = false;
        let mut typalign: c_char = 0;
        let mut typdelim: c_char = 0;
        let mut typiofunc: Oid = 0;

        cache = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            std::mem::size_of::<RangeIOData>(),
        ) as *mut RangeIOData;
        (*cache).typcache = lookup_type_cache(rngtypid, TYPECACHE_RANGE_INFO);
        if (*(*cache).typcache).rngelemtype.is_null() {
            elog!(ERROR, "type {} is not a range type", rngtypid);
        }

        /* get_type_io_data does more than we need, but is convenient */
        get_type_io_data(
            (*(*(*cache).typcache).rngelemtype).type_id,
            func,
            &mut typlen,
            &mut typbyval,
            &mut typalign,
            &mut typdelim,
            &mut (*cache).typioparam,
            &mut typiofunc,
        );

        if !OidIsValid(typiofunc) {
            /* this could only happen for receive or send */
            if func == IOFunc_receive {
                ereport!(
                    ERROR,
                    errmsg!(
                        "no binary input function available for type {}",
                        std::ffi::CStr::from_ptr(format_type_be(
                            (*(*(*cache).typcache).rngelemtype).type_id
                        ))
                        .to_string_lossy()
                    )
                );
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "no binary output function available for type {}",
                        std::ffi::CStr::from_ptr(format_type_be(
                            (*(*(*cache).typcache).rngelemtype).type_id
                        ))
                        .to_string_lossy()
                    )
                );
            }
        }
        fmgr_info_cxt(typiofunc, &mut (*cache).typioproc, (*(*fcinfo).flinfo).fn_mcxt);

        (*(*fcinfo).flinfo).fn_extra = cache as *mut c_void;
    }

    cache
}

/*
 *----------------------------------------------------------
 * GENERIC FUNCTIONS
 *----------------------------------------------------------
 */

/* Construct standard-form range value from two arguments */
pub unsafe fn range_constructor2(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = PG_GETARG_DATUM!(fcinfo, 0);
    let arg2 = PG_GETARG_DATUM!(fcinfo, 1);
    let rngtypid = get_fn_expr_rettype((*fcinfo).flinfo);
    let range: *mut RangeType;
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();

    typcache = range_get_typcache(fcinfo, rngtypid);

    lower.val = if PG_ARGISNULL!(fcinfo, 0) { 0 as Datum } else { arg1 };
    lower.infinite = PG_ARGISNULL!(fcinfo, 0);
    lower.inclusive = true;
    lower.lower = true;

    upper.val = if PG_ARGISNULL!(fcinfo, 1) { 0 as Datum } else { arg2 };
    upper.infinite = PG_ARGISNULL!(fcinfo, 1);
    upper.inclusive = false;
    upper.lower = false;

    range = make_range(typcache, &mut lower, &mut upper, false, null_mut());

    PG_RETURN_RANGE_P!(range);
}

/* Construct general range value from three arguments */
pub unsafe fn range_constructor3(fcinfo: FunctionCallInfo) -> Datum {
    let arg1 = PG_GETARG_DATUM!(fcinfo, 0);
    let arg2 = PG_GETARG_DATUM!(fcinfo, 1);
    let rngtypid = get_fn_expr_rettype((*fcinfo).flinfo);
    let range: *mut RangeType;
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let flags: c_char;

    typcache = range_get_typcache(fcinfo, rngtypid);

    if PG_ARGISNULL!(fcinfo, 2) {
        ereport!(
            ERROR,
            errmsg!("range constructor flags argument must not be null")
        );
    }

    flags = range_parse_flags(text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 2) as *const text));

    lower.val = if PG_ARGISNULL!(fcinfo, 0) { 0 as Datum } else { arg1 };
    lower.infinite = PG_ARGISNULL!(fcinfo, 0);
    lower.inclusive = (flags & RANGE_LB_INC) != 0;
    lower.lower = true;

    upper.val = if PG_ARGISNULL!(fcinfo, 1) { 0 as Datum } else { arg2 };
    upper.infinite = PG_ARGISNULL!(fcinfo, 1);
    upper.inclusive = (flags & RANGE_UB_INC) != 0;
    upper.lower = false;

    range = make_range(typcache, &mut lower, &mut upper, false, null_mut());

    PG_RETURN_RANGE_P!(range);
}

/* range -> subtype functions */

/* extract lower bound value */
pub unsafe fn range_lower(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    range_deserialize(typcache, r1, &mut lower, &mut upper, &mut empty);

    /* Return NULL if there's no finite lower bound */
    if empty || lower.infinite {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_DATUM!(lower.val);
}

/* extract upper bound value */
pub unsafe fn range_upper(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    range_deserialize(typcache, r1, &mut lower, &mut upper, &mut empty);

    /* Return NULL if there's no finite upper bound */
    if empty || upper.infinite {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_DATUM!(upper.val);
}

/* range -> bool functions */

/* is range empty? */
pub unsafe fn range_empty(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let flags = range_get_flags(r1);

    PG_RETURN_BOOL!((flags & RANGE_EMPTY) != 0);
}

/* is lower bound inclusive? */
pub unsafe fn range_lower_inc(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let flags = range_get_flags(r1);

    PG_RETURN_BOOL!((flags & RANGE_LB_INC) != 0);
}

/* is upper bound inclusive? */
pub unsafe fn range_upper_inc(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let flags = range_get_flags(r1);

    PG_RETURN_BOOL!((flags & RANGE_UB_INC) != 0);
}

/* is lower bound infinite? */
pub unsafe fn range_lower_inf(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let flags = range_get_flags(r1);

    PG_RETURN_BOOL!((flags & RANGE_LB_INF) != 0);
}

/* is upper bound infinite? */
pub unsafe fn range_upper_inf(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let flags = range_get_flags(r1);

    PG_RETURN_BOOL!((flags & RANGE_UB_INF) != 0);
}

/* range, element -> bool functions */

/* contains? */
pub unsafe fn range_contains_elem(fcinfo: FunctionCallInfo) -> Datum {
    let r = PG_GETARG_RANGE_P!(fcinfo, 0);
    let val = PG_GETARG_DATUM!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r));

    PG_RETURN_BOOL!(range_contains_elem_internal(typcache, r, val));
}

/* contained by? */
pub unsafe fn elem_contained_by_range(fcinfo: FunctionCallInfo) -> Datum {
    let val = PG_GETARG_DATUM!(fcinfo, 0);
    let r = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r));

    PG_RETURN_BOOL!(range_contains_elem_internal(typcache, r, val));
}

/* range, range -> bool functions */

/* equality (internal version) */
pub unsafe fn range_eq_internal(
    typcache: *mut TypeCacheEntry,
    r1: *const RangeType,
    r2: *const RangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;

    /* Different types should be prevented by ANYRANGE matching rules */
    if RangeTypeGetOid(r1) != RangeTypeGetOid(r2) {
        elog!(ERROR, "range types do not match");
    }

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    if empty1 && empty2 {
        return true;
    }
    if empty1 != empty2 {
        return false;
    }

    if range_cmp_bounds(typcache, &lower1, &lower2) != 0 {
        return false;
    }

    if range_cmp_bounds(typcache, &upper1, &upper2) != 0 {
        return false;
    }

    true
}

/* equality */
pub unsafe fn range_eq(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_BOOL!(range_eq_internal(typcache, r1, r2));
}

/* inequality (internal version) */
pub unsafe fn range_ne_internal(
    typcache: *mut TypeCacheEntry,
    r1: *const RangeType,
    r2: *const RangeType,
) -> bool {
    !range_eq_internal(typcache, r1, r2)
}

/* inequality */
pub unsafe fn range_ne(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_BOOL!(range_ne_internal(typcache, r1, r2));
}

/* contains? */
pub unsafe fn range_contains(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_BOOL!(range_contains_internal(typcache, r1, r2));
}

/* contained by? */
pub unsafe fn range_contained_by(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_BOOL!(range_contained_by_internal(typcache, r1, r2));
}

/* strictly left of? (internal version) */
pub unsafe fn range_before_internal(
    typcache: *mut TypeCacheEntry,
    r1: *const RangeType,
    r2: *const RangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;

    /* Different types should be prevented by ANYRANGE matching rules */
    if RangeTypeGetOid(r1) != RangeTypeGetOid(r2) {
        elog!(ERROR, "range types do not match");
    }

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    /* An empty range is neither before nor after any other range */
    if empty1 || empty2 {
        return false;
    }

    range_cmp_bounds(typcache, &upper1, &lower2) < 0
}

/* strictly left of? */
pub unsafe fn range_before(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_BOOL!(range_before_internal(typcache, r1, r2));
}

/* strictly right of? (internal version) */
pub unsafe fn range_after_internal(
    typcache: *mut TypeCacheEntry,
    r1: *const RangeType,
    r2: *const RangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;

    /* Different types should be prevented by ANYRANGE matching rules */
    if RangeTypeGetOid(r1) != RangeTypeGetOid(r2) {
        elog!(ERROR, "range types do not match");
    }

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    /* An empty range is neither before nor after any other range */
    if empty1 || empty2 {
        return false;
    }

    range_cmp_bounds(typcache, &lower1, &upper2) > 0
}

/* strictly right of? */
pub unsafe fn range_after(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_BOOL!(range_after_internal(typcache, r1, r2));
}

/*
 * Check if two bounds A and B are "adjacent", where A is an upper bound and B
 * is a lower bound. For the bounds to be adjacent, each subtype value must
 * satisfy strictly one of the bounds: there are no values which satisfy both
 * bounds (i.e. less than A and greater than B); and there are no values which
 * satisfy neither bound (i.e. greater than A and less than B).
 *
 * For discrete ranges, we rely on the canonicalization function to see if A..B
 * normalizes to empty. (If there is no canonicalization function, it's
 * impossible for such a range to normalize to empty, so we needn't bother to
 * try.)
 *
 * If A == B, the ranges are adjacent only if the bounds have different
 * inclusive flags (i.e., exactly one of the ranges includes the common
 * boundary point).
 *
 * And if A > B then the ranges are not adjacent in this order.
 */
pub unsafe fn bounds_adjacent(
    typcache: *mut TypeCacheEntry,
    mut boundA: RangeBound,
    mut boundB: RangeBound,
) -> bool {
    let cmp: c_int;

    Assert!(!boundA.lower && boundB.lower);

    cmp = range_cmp_bound_values(typcache, &boundA, &boundB);
    if cmp < 0 {
        let r: *mut RangeType;

        /*
         * Bounds do not overlap; see if there are points in between.
         */

        /* in a continuous subtype, there are assumed to be points between */
        if !OidIsValid((*typcache).rng_canonical_finfo.fn_oid) {
            return false;
        }

        /*
         * The bounds are of a discrete range type; so make a range A..B and
         * see if it's empty.
         */

        /* flip the inclusion flags */
        boundA.inclusive = !boundA.inclusive;
        boundB.inclusive = !boundB.inclusive;
        /* change upper/lower labels to avoid Assert failures */
        boundA.lower = true;
        boundB.lower = false;
        r = make_range(typcache, &mut boundA, &mut boundB, false, null_mut());
        RangeIsEmpty(r)
    } else if cmp == 0 {
        boundA.inclusive != boundB.inclusive
    } else {
        false /* bounds overlap */
    }
}

/* adjacent to (but not overlapping)? (internal version) */
pub unsafe fn range_adjacent_internal(
    typcache: *mut TypeCacheEntry,
    r1: *const RangeType,
    r2: *const RangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;

    /* Different types should be prevented by ANYRANGE matching rules */
    if RangeTypeGetOid(r1) != RangeTypeGetOid(r2) {
        elog!(ERROR, "range types do not match");
    }

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    /* An empty range is not adjacent to any other range */
    if empty1 || empty2 {
        return false;
    }

    /*
     * Given two ranges A..B and C..D, the ranges are adjacent if and only if
     * B is adjacent to C, or D is adjacent to A.
     */
    bounds_adjacent(typcache, upper1, lower2) || bounds_adjacent(typcache, upper2, lower1)
}

/* adjacent to (but not overlapping)? */
pub unsafe fn range_adjacent(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_BOOL!(range_adjacent_internal(typcache, r1, r2));
}

/* overlaps? (internal version) */
pub unsafe fn range_overlaps_internal(
    typcache: *mut TypeCacheEntry,
    r1: *const RangeType,
    r2: *const RangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;

    /* Different types should be prevented by ANYRANGE matching rules */
    if RangeTypeGetOid(r1) != RangeTypeGetOid(r2) {
        elog!(ERROR, "range types do not match");
    }

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    /* An empty range does not overlap any other range */
    if empty1 || empty2 {
        return false;
    }

    if range_cmp_bounds(typcache, &lower1, &lower2) >= 0
        && range_cmp_bounds(typcache, &lower1, &upper2) <= 0
    {
        return true;
    }

    if range_cmp_bounds(typcache, &lower2, &lower1) >= 0
        && range_cmp_bounds(typcache, &lower2, &upper1) <= 0
    {
        return true;
    }

    false
}

/* overlaps? */
pub unsafe fn range_overlaps(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_BOOL!(range_overlaps_internal(typcache, r1, r2));
}

/* does not extend to right of? (internal version) */
pub unsafe fn range_overleft_internal(
    typcache: *mut TypeCacheEntry,
    r1: *const RangeType,
    r2: *const RangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;

    /* Different types should be prevented by ANYRANGE matching rules */
    if RangeTypeGetOid(r1) != RangeTypeGetOid(r2) {
        elog!(ERROR, "range types do not match");
    }

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    /* An empty range is neither before nor after any other range */
    if empty1 || empty2 {
        return false;
    }

    if range_cmp_bounds(typcache, &upper1, &upper2) <= 0 {
        return true;
    }

    false
}

/* does not extend to right of? */
pub unsafe fn range_overleft(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_BOOL!(range_overleft_internal(typcache, r1, r2));
}

/* does not extend to left of? (internal version) */
pub unsafe fn range_overright_internal(
    typcache: *mut TypeCacheEntry,
    r1: *const RangeType,
    r2: *const RangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;

    /* Different types should be prevented by ANYRANGE matching rules */
    if RangeTypeGetOid(r1) != RangeTypeGetOid(r2) {
        elog!(ERROR, "range types do not match");
    }

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    /* An empty range is neither before nor after any other range */
    if empty1 || empty2 {
        return false;
    }

    if range_cmp_bounds(typcache, &lower1, &lower2) >= 0 {
        return true;
    }

    false
}

/* does not extend to left of? */
pub unsafe fn range_overright(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_BOOL!(range_overright_internal(typcache, r1, r2));
}

/* range, range -> range functions */

/* set difference */
pub unsafe fn range_minus(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let ret: *mut RangeType;
    let typcache: *mut TypeCacheEntry;

    /* Different types should be prevented by ANYRANGE matching rules */
    if RangeTypeGetOid(r1) != RangeTypeGetOid(r2) {
        elog!(ERROR, "range types do not match");
    }

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    ret = range_minus_internal(typcache, r1, r2);
    if !ret.is_null() {
        PG_RETURN_RANGE_P!(ret);
    } else {
        PG_RETURN_NULL!(fcinfo);
    }
}

pub unsafe fn range_minus_internal(
    typcache: *mut TypeCacheEntry,
    r1: *mut RangeType,
    r2: *mut RangeType,
) -> *mut RangeType {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;
    let cmp_l1l2: c_int;
    let cmp_l1u2: c_int;
    let cmp_u1l2: c_int;
    let cmp_u1u2: c_int;

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    /* if either is empty, r1 is the correct answer */
    if empty1 || empty2 {
        return r1;
    }

    cmp_l1l2 = range_cmp_bounds(typcache, &lower1, &lower2);
    cmp_l1u2 = range_cmp_bounds(typcache, &lower1, &upper2);
    cmp_u1l2 = range_cmp_bounds(typcache, &upper1, &lower2);
    cmp_u1u2 = range_cmp_bounds(typcache, &upper1, &upper2);

    if cmp_l1l2 < 0 && cmp_u1u2 > 0 {
        ereport!(
            ERROR,
            errmsg!("result of range difference would not be contiguous")
        );
    }

    if cmp_l1u2 > 0 || cmp_u1l2 < 0 {
        return r1;
    }

    if cmp_l1l2 >= 0 && cmp_u1u2 <= 0 {
        return make_empty_range(typcache);
    }

    if cmp_l1l2 <= 0 && cmp_u1l2 >= 0 && cmp_u1u2 <= 0 {
        lower2.inclusive = !lower2.inclusive;
        lower2.lower = false; /* it will become the upper bound */
        return make_range(typcache, &mut lower1, &mut lower2, false, null_mut());
    }

    if cmp_l1l2 >= 0 && cmp_u1u2 >= 0 && cmp_l1u2 <= 0 {
        upper2.inclusive = !upper2.inclusive;
        upper2.lower = true; /* it will become the lower bound */
        return make_range(typcache, &mut upper2, &mut upper1, false, null_mut());
    }

    elog!(ERROR, "unexpected case in range_minus");
    #[allow(unreachable_code)]
    null_mut()
}

/*
 * Set union.  If strict is true, it is an error that the two input ranges
 * are not adjacent or overlapping.
 */
pub unsafe fn range_union_internal(
    typcache: *mut TypeCacheEntry,
    r1: *mut RangeType,
    r2: *mut RangeType,
    strict: bool,
) -> *mut RangeType {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;
    let result_lower: *mut RangeBound;
    let result_upper: *mut RangeBound;

    /* Different types should be prevented by ANYRANGE matching rules */
    if RangeTypeGetOid(r1) != RangeTypeGetOid(r2) {
        elog!(ERROR, "range types do not match");
    }

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    /* if either is empty, the other is the correct answer */
    if empty1 {
        return r2;
    }
    if empty2 {
        return r1;
    }

    if strict
        && !DatumGetBool(range_overlaps_internal(typcache, r1, r2) as Datum)
        && !DatumGetBool(range_adjacent_internal(typcache, r1, r2) as Datum)
    {
        ereport!(
            ERROR,
            errmsg!("result of range union would not be contiguous")
        );
    }

    if range_cmp_bounds(typcache, &lower1, &lower2) < 0 {
        result_lower = &mut lower1;
    } else {
        result_lower = &mut lower2;
    }

    if range_cmp_bounds(typcache, &upper1, &upper2) > 0 {
        result_upper = &mut upper1;
    } else {
        result_upper = &mut upper2;
    }

    make_range(typcache, result_lower, result_upper, false, null_mut())
}

pub unsafe fn range_union(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_RANGE_P!(range_union_internal(typcache, r1, r2, true));
}

/*
 * range merge: like set union, except also allow and account for non-adjacent
 * input ranges.
 */
pub unsafe fn range_merge(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_RANGE_P!(range_union_internal(typcache, r1, r2, false));
}

/* set intersection */
pub unsafe fn range_intersect(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    /* Different types should be prevented by ANYRANGE matching rules */
    if RangeTypeGetOid(r1) != RangeTypeGetOid(r2) {
        elog!(ERROR, "range types do not match");
    }

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    PG_RETURN_RANGE_P!(range_intersect_internal(typcache, r1, r2));
}

pub unsafe fn range_intersect_internal(
    typcache: *mut TypeCacheEntry,
    r1: *const RangeType,
    r2: *const RangeType,
) -> *mut RangeType {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;
    let result_lower: *mut RangeBound;
    let result_upper: *mut RangeBound;

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    if empty1 || empty2 || !range_overlaps_internal(typcache, r1, r2) {
        return make_empty_range(typcache);
    }

    if range_cmp_bounds(typcache, &lower1, &lower2) >= 0 {
        result_lower = &mut lower1;
    } else {
        result_lower = &mut lower2;
    }

    if range_cmp_bounds(typcache, &upper1, &upper2) <= 0 {
        result_upper = &mut upper1;
    } else {
        result_upper = &mut upper2;
    }

    make_range(typcache, result_lower, result_upper, false, null_mut())
}

/* range, range -> range, range functions */

/*
 * range_split_internal - if r2 intersects the middle of r1, leaving non-empty
 * ranges on both sides, then return true and set output1 and output2 to the
 * results of r1 - r2 (in order). Otherwise return false and don't set output1
 * or output2. Neither input range should be empty.
 */
pub unsafe fn range_split_internal(
    typcache: *mut TypeCacheEntry,
    r1: *const RangeType,
    r2: *const RangeType,
    output1: *mut *mut RangeType,
    output2: *mut *mut RangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    if range_cmp_bounds(typcache, &lower1, &lower2) < 0
        && range_cmp_bounds(typcache, &upper1, &upper2) > 0
    {
        /*
         * Need to invert inclusive/exclusive for the lower2 and upper2
         * points. They can't be infinite though. We're allowed to overwrite
         * these RangeBounds since they only exist locally.
         */
        lower2.inclusive = !lower2.inclusive;
        lower2.lower = false;
        upper2.inclusive = !upper2.inclusive;
        upper2.lower = true;

        *output1 = make_range(typcache, &mut lower1, &mut lower2, false, null_mut());
        *output2 = make_range(typcache, &mut upper2, &mut upper1, false, null_mut());
        return true;
    }

    false
}

/* range -> range aggregate functions */

pub unsafe fn range_intersect_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let mut aggContext: MemoryContext = null_mut();
    let rngtypoid: Oid;
    let typcache: *mut TypeCacheEntry;
    let mut result: *mut RangeType;
    let current: *mut RangeType;

    if AggCheckCallContext(fcinfo, &mut aggContext) == 0 {
        elog!(
            ERROR,
            "range_intersect_agg_transfn called in non-aggregate context"
        );
    }

    rngtypoid = get_fn_expr_argtype((*fcinfo).flinfo, 1);
    if !type_is_range(rngtypoid) {
        elog!(ERROR, "range_intersect_agg must be called with a range");
    }

    typcache = range_get_typcache(fcinfo, rngtypoid);

    /* strictness ensures these are non-null */
    result = PG_GETARG_RANGE_P!(fcinfo, 0);
    current = PG_GETARG_RANGE_P!(fcinfo, 1);

    result = range_intersect_internal(typcache, result, current);
    PG_RETURN_RANGE_P!(result);
}

/* Btree support */

/* btree comparator */
pub unsafe fn range_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let r1 = PG_GETARG_RANGE_P!(fcinfo, 0);
    let r2 = PG_GETARG_RANGE_P!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;
    let mut cmp: c_int;

    check_stack_depth(); /* recurses when subtype is a range type */

    /* Different types should be prevented by ANYRANGE matching rules */
    if RangeTypeGetOid(r1) != RangeTypeGetOid(r2) {
        elog!(ERROR, "range types do not match");
    }

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    /* For b-tree use, empty ranges sort before all else */
    if empty1 && empty2 {
        cmp = 0;
    } else if empty1 {
        cmp = -1;
    } else if empty2 {
        cmp = 1;
    } else {
        cmp = range_cmp_bounds(typcache, &lower1, &lower2);
        if cmp == 0 {
            cmp = range_cmp_bounds(typcache, &upper1, &upper2);
        }
    }

    PG_FREE_IF_COPY!(fcinfo, r1, 0);
    PG_FREE_IF_COPY!(fcinfo, r2, 1);

    PG_RETURN_INT32!(cmp);
}

/* Sort support strategy routine */
pub unsafe fn range_sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;

    (*ssup).comparator = Some(range_fast_cmp);
    (*ssup).ssup_extra = null_mut();

    PG_RETURN_VOID!();
}

/* like range_cmp, but uses the new sortsupport interface */
unsafe fn range_fast_cmp(a: Datum, b: Datum, ssup: SortSupport) -> c_int {
    let range_a = DatumGetRangeTypeP(a);
    let range_b = DatumGetRangeTypeP(b);
    let typcache: *mut TypeCacheEntry;
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;
    let mut cmp: c_int;

    /* cache the range info between calls */
    if (*ssup).ssup_extra.is_null() {
        Assert!(RangeTypeGetOid(range_a) == RangeTypeGetOid(range_b));
        (*ssup).ssup_extra =
            lookup_type_cache(RangeTypeGetOid(range_a), TYPECACHE_RANGE_INFO) as *mut c_void;
    }
    typcache = (*ssup).ssup_extra as *mut TypeCacheEntry;

    range_deserialize(typcache, range_a, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, range_b, &mut lower2, &mut upper2, &mut empty2);

    /* For b-tree use, empty ranges sort before all else */
    if empty1 && empty2 {
        cmp = 0;
    } else if empty1 {
        cmp = -1;
    } else if empty2 {
        cmp = 1;
    } else {
        cmp = range_cmp_bounds(typcache, &lower1, &lower2);
        if cmp == 0 {
            cmp = range_cmp_bounds(typcache, &upper1, &upper2);
        }
    }

    if (range_a as Datum) != a {
        pfree(range_a as *mut c_void);
    }
    if (range_b as Datum) != b {
        pfree(range_b as *mut c_void);
    }

    cmp
}

/* inequality operators using the range_cmp function */
pub unsafe fn range_lt(fcinfo: FunctionCallInfo) -> Datum {
    let cmp = range_cmp(fcinfo) as int32;

    PG_RETURN_BOOL!(cmp < 0);
}

pub unsafe fn range_le(fcinfo: FunctionCallInfo) -> Datum {
    let cmp = range_cmp(fcinfo) as int32;

    PG_RETURN_BOOL!(cmp <= 0);
}

pub unsafe fn range_ge(fcinfo: FunctionCallInfo) -> Datum {
    let cmp = range_cmp(fcinfo) as int32;

    PG_RETURN_BOOL!(cmp >= 0);
}

pub unsafe fn range_gt(fcinfo: FunctionCallInfo) -> Datum {
    let cmp = range_cmp(fcinfo) as int32;

    PG_RETURN_BOOL!(cmp > 0);
}

/* Hash support */

/* hash a range value */
pub unsafe fn hash_range(fcinfo: FunctionCallInfo) -> Datum {
    let r = PG_GETARG_RANGE_P!(fcinfo, 0);
    let mut result: uint32;
    let typcache: *mut TypeCacheEntry;
    let mut scache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;
    let flags: c_char;
    let lower_hash: uint32;
    let upper_hash: uint32;

    check_stack_depth(); /* recurses when subtype is a range type */

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r));

    /* deserialize */
    range_deserialize(typcache, r, &mut lower, &mut upper, &mut empty);
    flags = range_get_flags(r);

    /*
     * Look up the element type's hash function, if not done already.
     */
    scache = (*typcache).rngelemtype;
    if !OidIsValid((*scache).hash_proc_finfo.fn_oid) {
        scache = lookup_type_cache((*scache).type_id, TYPECACHE_HASH_PROC_FINFO);
        if !OidIsValid((*scache).hash_proc_finfo.fn_oid) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not identify a hash function for type {}",
                    std::ffi::CStr::from_ptr(format_type_be((*scache).type_id))
                        .to_string_lossy()
                )
            );
        }
    }

    /*
     * Apply the hash function to each bound.
     */
    if RANGE_HAS_LBOUND(flags) {
        lower_hash = DatumGetUInt32(FunctionCall1Coll(
            &mut (*scache).hash_proc_finfo,
            (*typcache).rng_collation,
            lower.val,
        ));
    } else {
        lower_hash = 0;
    }

    if RANGE_HAS_UBOUND(flags) {
        upper_hash = DatumGetUInt32(FunctionCall1Coll(
            &mut (*scache).hash_proc_finfo,
            (*typcache).rng_collation,
            upper.val,
        ));
    } else {
        upper_hash = 0;
    }

    /* Merge hashes of flags and bounds */
    result = DatumGetUInt32(hash_uint32((flags as c_uchar) as uint32));
    result ^= lower_hash;
    result = pg_rotate_left32(result, 1);
    result ^= upper_hash;

    PG_RETURN_INT32!(result as int32);
}

/*
 * Returns 64-bit value by hashing a value to a 64-bit value, with a seed.
 * Otherwise, similar to hash_range.
 */
pub unsafe fn hash_range_extended(fcinfo: FunctionCallInfo) -> Datum {
    let r = PG_GETARG_RANGE_P!(fcinfo, 0);
    let seed = PG_GETARG_DATUM!(fcinfo, 1);
    let mut result: uint64;
    let typcache: *mut TypeCacheEntry;
    let mut scache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;
    let flags: c_char;
    let lower_hash: uint64;
    let upper_hash: uint64;

    check_stack_depth();

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r));

    range_deserialize(typcache, r, &mut lower, &mut upper, &mut empty);
    flags = range_get_flags(r);

    scache = (*typcache).rngelemtype;
    if !OidIsValid((*scache).hash_extended_proc_finfo.fn_oid) {
        scache = lookup_type_cache((*scache).type_id, TYPECACHE_HASH_EXTENDED_PROC_FINFO);
        if !OidIsValid((*scache).hash_extended_proc_finfo.fn_oid) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not identify a hash function for type {}",
                    std::ffi::CStr::from_ptr(format_type_be((*scache).type_id))
                        .to_string_lossy()
                )
            );
        }
    }

    if RANGE_HAS_LBOUND(flags) {
        lower_hash = DatumGetUInt64(FunctionCall2Coll(
            &mut (*scache).hash_extended_proc_finfo,
            (*typcache).rng_collation,
            lower.val,
            seed,
        ));
    } else {
        lower_hash = 0;
    }

    if RANGE_HAS_UBOUND(flags) {
        upper_hash = DatumGetUInt64(FunctionCall2Coll(
            &mut (*scache).hash_extended_proc_finfo,
            (*typcache).rng_collation,
            upper.val,
            seed,
        ));
    } else {
        upper_hash = 0;
    }

    /* Merge hashes of flags and bounds */
    result = DatumGetUInt64(hash_uint32_extended(
        (flags as c_uchar) as uint32,
        DatumGetInt64(seed) as uint64,
    ));
    result ^= lower_hash;
    result = ROTATE_HIGH_AND_LOW_32BITS(result);
    result ^= upper_hash;

    PG_RETURN_UINT64!(result);
}

/*
 *----------------------------------------------------------
 * CANONICAL FUNCTIONS
 *
 *	 Functions for specific built-in range types.
 *----------------------------------------------------------
 */

pub unsafe fn int4range_canonical(fcinfo: FunctionCallInfo) -> Datum {
    let r = PG_GETARG_RANGE_P!(fcinfo, 0);
    let escontext = (*fcinfo).context;
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r));

    range_deserialize(typcache, r, &mut lower, &mut upper, &mut empty);

    if empty {
        PG_RETURN_RANGE_P!(r);
    }

    if !lower.infinite && !lower.inclusive {
        let bnd = DatumGetInt32(lower.val);

        /* Handle possible overflow manually */
        if bnd == PG_INT32_MAX {
            ereturn!(escontext, 0 as Datum, errmsg!("integer out of range"));
        }
        lower.val = Int32GetDatum(bnd + 1);
        lower.inclusive = true;
    }

    if !upper.infinite && upper.inclusive {
        let bnd = DatumGetInt32(upper.val);

        /* Handle possible overflow manually */
        if bnd == PG_INT32_MAX {
            ereturn!(escontext, 0 as Datum, errmsg!("integer out of range"));
        }
        upper.val = Int32GetDatum(bnd + 1);
        upper.inclusive = false;
    }

    PG_RETURN_RANGE_P!(range_serialize(typcache, &mut lower, &mut upper, false, escontext));
}

pub unsafe fn int8range_canonical(fcinfo: FunctionCallInfo) -> Datum {
    let r = PG_GETARG_RANGE_P!(fcinfo, 0);
    let escontext = (*fcinfo).context;
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r));

    range_deserialize(typcache, r, &mut lower, &mut upper, &mut empty);

    if empty {
        PG_RETURN_RANGE_P!(r);
    }

    if !lower.infinite && !lower.inclusive {
        let bnd = DatumGetInt64(lower.val);

        /* Handle possible overflow manually */
        if bnd == PG_INT64_MAX {
            ereturn!(escontext, 0 as Datum, errmsg!("bigint out of range"));
        }
        lower.val = Int64GetDatum(bnd + 1);
        lower.inclusive = true;
    }

    if !upper.infinite && upper.inclusive {
        let bnd = DatumGetInt64(upper.val);

        /* Handle possible overflow manually */
        if bnd == PG_INT64_MAX {
            ereturn!(escontext, 0 as Datum, errmsg!("bigint out of range"));
        }
        upper.val = Int64GetDatum(bnd + 1);
        upper.inclusive = false;
    }

    PG_RETURN_RANGE_P!(range_serialize(typcache, &mut lower, &mut upper, false, escontext));
}

pub unsafe fn daterange_canonical(fcinfo: FunctionCallInfo) -> Datum {
    let r = PG_GETARG_RANGE_P!(fcinfo, 0);
    let escontext = (*fcinfo).context;
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r));

    range_deserialize(typcache, r, &mut lower, &mut upper, &mut empty);

    if empty {
        PG_RETURN_RANGE_P!(r);
    }

    if !lower.infinite && !DATE_NOT_FINITE(DatumGetDateADT(lower.val)) && !lower.inclusive {
        let mut bnd: DateADT = DatumGetDateADT(lower.val);

        /* Check for overflow -- note we already eliminated PG_INT32_MAX */
        bnd += 1;
        if !IS_VALID_DATE(bnd) {
            ereturn!(escontext, 0 as Datum, errmsg!("date out of range"));
        }
        lower.val = DateADTGetDatum(bnd);
        lower.inclusive = true;
    }

    if !upper.infinite && !DATE_NOT_FINITE(DatumGetDateADT(upper.val)) && upper.inclusive {
        let mut bnd: DateADT = DatumGetDateADT(upper.val);

        /* Check for overflow -- note we already eliminated PG_INT32_MAX */
        bnd += 1;
        if !IS_VALID_DATE(bnd) {
            ereturn!(escontext, 0 as Datum, errmsg!("date out of range"));
        }
        upper.val = DateADTGetDatum(bnd);
        upper.inclusive = false;
    }

    PG_RETURN_RANGE_P!(range_serialize(typcache, &mut lower, &mut upper, false, escontext));
}

/*
 *----------------------------------------------------------
 * SUBTYPE_DIFF FUNCTIONS
 *
 * Functions for specific built-in range types.
 *
 * Note that subtype_diff does return the difference, not the absolute value
 * of the difference, and it must take care to avoid overflow.
 * (numrange_subdiff is at some risk there ...)
 *----------------------------------------------------------
 */

pub unsafe fn int4range_subdiff(fcinfo: FunctionCallInfo) -> Datum {
    let v1 = PG_GETARG_INT32!(fcinfo, 0);
    let v2 = PG_GETARG_INT32!(fcinfo, 1);

    PG_RETURN_FLOAT8!(v1 as float8 - v2 as float8);
}

pub unsafe fn int8range_subdiff(fcinfo: FunctionCallInfo) -> Datum {
    let v1 = PG_GETARG_INT64!(fcinfo, 0);
    let v2 = PG_GETARG_INT64!(fcinfo, 1);

    PG_RETURN_FLOAT8!(v1 as float8 - v2 as float8);
}

pub unsafe fn numrange_subdiff(fcinfo: FunctionCallInfo) -> Datum {
    let v1 = PG_GETARG_DATUM!(fcinfo, 0);
    let v2 = PG_GETARG_DATUM!(fcinfo, 1);
    let numresult: Datum;
    let floatresult: float8;

    numresult = DirectFunctionCall2!(numeric_sub, v1, v2);

    floatresult = DatumGetFloat8(DirectFunctionCall1!(numeric_float8, numresult));

    PG_RETURN_FLOAT8!(floatresult);
}

pub unsafe fn daterange_subdiff(fcinfo: FunctionCallInfo) -> Datum {
    let v1 = PG_GETARG_INT32!(fcinfo, 0);
    let v2 = PG_GETARG_INT32!(fcinfo, 1);

    PG_RETURN_FLOAT8!(v1 as float8 - v2 as float8);
}

pub unsafe fn tsrange_subdiff(fcinfo: FunctionCallInfo) -> Datum {
    let v1: Timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let v2: Timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 1);
    let result: float8;

    result = (v1 as float8 - v2 as float8) / USECS_PER_SEC as float8;
    PG_RETURN_FLOAT8!(result);
}

pub unsafe fn tstzrange_subdiff(fcinfo: FunctionCallInfo) -> Datum {
    let v1: Timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let v2: Timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 1);
    let result: float8;

    result = (v1 as float8 - v2 as float8) / USECS_PER_SEC as float8;
    PG_RETURN_FLOAT8!(result);
}

/*
 *----------------------------------------------------------
 * SUPPORT FUNCTIONS
 *
 *	 These functions aren't in pg_proc, but are useful for
 *	 defining new generic range functions in C.
 *----------------------------------------------------------
 */

/*
 * range_get_typcache: get cached information about a range type
 *
 * This is for use by range-related functions that follow the convention
 * of using the fn_extra field as a pointer to the type cache entry for
 * the range type.  Functions that need to cache more information than
 * that must fend for themselves.
 */
pub unsafe fn range_get_typcache(fcinfo: FunctionCallInfo, rngtypid: Oid) -> *mut TypeCacheEntry {
    let mut typcache = (*(*fcinfo).flinfo).fn_extra as *mut TypeCacheEntry;

    if typcache.is_null() || (*typcache).type_id != rngtypid {
        typcache = lookup_type_cache(rngtypid, TYPECACHE_RANGE_INFO);
        if (*typcache).rngelemtype.is_null() {
            elog!(ERROR, "type {} is not a range type", rngtypid);
        }
        (*(*fcinfo).flinfo).fn_extra = typcache as *mut c_void;
    }

    typcache
}

/*
 * range_serialize: construct a range value from bounds and empty-flag
 *
 * This does not force canonicalization of the range value.  In most cases,
 * external callers should only be canonicalization functions.  Note that
 * we perform some datatype-independent canonicalization checks anyway.
 */
pub unsafe fn range_serialize(
    typcache: *mut TypeCacheEntry,
    lower: *mut RangeBound,
    upper: *mut RangeBound,
    empty: bool,
    escontext: *mut Node,
) -> *mut RangeType {
    let range: *mut RangeType;
    let cmp: c_int;
    let mut msize: Size;
    let mut ptr: Pointer;
    let typlen: int16;
    let typbyval: bool;
    let typalign: c_char;
    let typstorage: c_char;
    let mut flags: c_char = 0;

    /*
     * Verify range is not invalid on its face, and construct flags value,
     * preventing any non-canonical combinations such as infinite+inclusive.
     */
    Assert!((*lower).lower);
    Assert!(!(*upper).lower);

    if empty {
        flags |= RANGE_EMPTY;
    } else {
        cmp = range_cmp_bound_values(typcache, lower, upper);

        /* error check: if lower bound value is above upper, it's wrong */
        if cmp > 0 {
            ereturn!(
                escontext,
                null_mut(),
                errmsg!("range lower bound must be less than or equal to range upper bound")
            );
        }

        /* if bounds are equal, and not both inclusive, range is empty */
        if cmp == 0 && !((*lower).inclusive && (*upper).inclusive) {
            flags |= RANGE_EMPTY;
        } else {
            /* infinite boundaries are never inclusive */
            if (*lower).infinite {
                flags |= RANGE_LB_INF;
            } else if (*lower).inclusive {
                flags |= RANGE_LB_INC;
            }
            if (*upper).infinite {
                flags |= RANGE_UB_INF;
            } else if (*upper).inclusive {
                flags |= RANGE_UB_INC;
            }
        }
    }

    /* Fetch information about range's element type */
    typlen = (*(*typcache).rngelemtype).typlen;
    typbyval = (*(*typcache).rngelemtype).typbyval;
    typalign = (*(*typcache).rngelemtype).typalign;
    typstorage = (*(*typcache).rngelemtype).typstorage;

    /* Count space for varlena header and range type's OID */
    msize = std::mem::size_of::<RangeType>();
    Assert!(msize == MAXALIGN(msize));

    /* Count space for bounds */
    if RANGE_HAS_LBOUND(flags) {
        /*
         * Make sure item to be inserted is not toasted.  It is essential that
         * we not insert an out-of-line toast value pointer into a range
         * object, for the same reasons that arrays and records can't contain
         * them.  It would work to store a compressed-in-line value, but we
         * prefer to decompress and then let compression be applied to the
         * whole range object if necessary.  But, unlike arrays, we do allow
         * short-header varlena objects to stay as-is.
         */
        if typlen == -1 {
            (*lower).val = PointerGetDatum(PG_DETOAST_DATUM_PACKED!((*lower).val) as *const c_void);
        }

        msize = datum_compute_size(msize, (*lower).val, typbyval, typalign, typlen, typstorage);
    }

    if RANGE_HAS_UBOUND(flags) {
        /* Make sure item to be inserted is not toasted */
        if typlen == -1 {
            (*upper).val = PointerGetDatum(PG_DETOAST_DATUM_PACKED!((*upper).val) as *const c_void);
        }

        msize = datum_compute_size(msize, (*upper).val, typbyval, typalign, typlen, typstorage);
    }

    /* Add space for flag byte */
    msize += std::mem::size_of::<c_char>();

    /* Note: zero-fill is required here, just as in heap tuples */
    range = palloc0(msize) as *mut RangeType;
    SET_VARSIZE(range as *mut c_char, msize as int32);

    /* Now fill in the datum */
    (*range).rangetypid = (*typcache).type_id;

    ptr = range.add(1) as Pointer;

    if RANGE_HAS_LBOUND(flags) {
        Assert!((*lower).lower);
        ptr = datum_write(ptr, (*lower).val, typbyval, typalign, typlen, typstorage);
    }

    if RANGE_HAS_UBOUND(flags) {
        Assert!(!(*upper).lower);
        ptr = datum_write(ptr, (*upper).val, typbyval, typalign, typlen, typstorage);
    }

    *(ptr as *mut c_char) = flags;

    range
}

/*
 * range_deserialize: deconstruct a range value
 *
 * NB: the given range object must be fully detoasted; it cannot have a
 * short varlena header.
 *
 * Note that if the element type is pass-by-reference, the datums in the
 * RangeBound structs will be pointers into the given range object.
 */
pub unsafe fn range_deserialize(
    typcache: *mut TypeCacheEntry,
    range: *const RangeType,
    lower: *mut RangeBound,
    upper: *mut RangeBound,
    empty: *mut bool,
) {
    let flags: c_char;
    let typlen: int16;
    let typbyval: bool;
    let typalign: c_char;
    let base: Pointer;
    let mut off: usize;
    let lbound: Datum;
    let ubound: Datum;

    /* assert caller passed the right typcache entry */
    Assert!(RangeTypeGetOid(range) == (*typcache).type_id);

    /* fetch the flag byte from datum's last byte */
    flags = *((range as *const c_char).add(VARSIZE(range as *const c_char) as usize - 1));

    /* fetch information about range's element type */
    typlen = (*(*typcache).rngelemtype).typlen;
    typbyval = (*(*typcache).rngelemtype).typbyval;
    typalign = (*(*typcache).rngelemtype).typalign;

    /* initialize data pointer just after the range OID */
    base = (range as *mut RangeType).add(1) as Pointer;
    off = 0;

    /* fetch lower bound, if any */
    if RANGE_HAS_LBOUND(flags) {
        /* att_align_pointer cannot be necessary here */
        lbound = fetch_att(base.add(off) as *const c_void, typbyval, typlen as c_int);
        off = att_addlength_pointer(off, typlen as c_int, base.add(off));
    } else {
        lbound = 0 as Datum;
    }

    /* fetch upper bound, if any */
    if RANGE_HAS_UBOUND(flags) {
        off = att_align_pointer(off, typalign, typlen as c_int, base.add(off));
        ubound = fetch_att(base.add(off) as *const c_void, typbyval, typlen as c_int);
        /* no need for att_addlength_pointer */
    } else {
        ubound = 0 as Datum;
    }

    /* emit results */

    *empty = (flags & RANGE_EMPTY) != 0;

    (*lower).val = lbound;
    (*lower).infinite = (flags & RANGE_LB_INF) != 0;
    (*lower).inclusive = (flags & RANGE_LB_INC) != 0;
    (*lower).lower = true;

    (*upper).val = ubound;
    (*upper).infinite = (flags & RANGE_UB_INF) != 0;
    (*upper).inclusive = (flags & RANGE_UB_INC) != 0;
    (*upper).lower = false;
}

/*
 * range_get_flags: just get the flags from a RangeType value.
 *
 * This is frequently useful in places that only need the flags and not
 * the full results of range_deserialize.
 */
pub unsafe fn range_get_flags(range: *const RangeType) -> c_char {
    /* fetch the flag byte from datum's last byte */
    *((range as *const c_char).add(VARSIZE(range as *const c_char) as usize - 1))
}

/*
 * range_set_contain_empty: set the RANGE_CONTAIN_EMPTY bit in the value.
 *
 * This is only needed in GiST operations, so we don't include a provision
 * for setting it in range_serialize; rather, this function must be applied
 * afterwards.
 */
pub unsafe fn range_set_contain_empty(range: *mut RangeType) {
    let flagsp: *mut c_char;

    /* flag byte is datum's last byte */
    flagsp = (range as *mut c_char).add(VARSIZE(range as *const c_char) as usize - 1);

    *flagsp |= RANGE_CONTAIN_EMPTY;
}

/*
 * This both serializes and canonicalizes (if applicable) the range.
 * This should be used by most callers.
 */
pub unsafe fn make_range(
    typcache: *mut TypeCacheEntry,
    lower: *mut RangeBound,
    upper: *mut RangeBound,
    empty: bool,
    escontext: *mut Node,
) -> *mut RangeType {
    let mut range: *mut RangeType;

    range = range_serialize(typcache, lower, upper, empty, escontext);

    if SOFT_ERROR_OCCURRED!(escontext) {
        return null_mut();
    }

    /* no need to call canonical on empty ranges ... */
    if OidIsValid((*typcache).rng_canonical_finfo.fn_oid) && !RangeIsEmpty(range) {
        /* Do this the hard way so that we can pass escontext */
        LOCAL_FCINFO!(fcinfo, 1);
        let result: Datum;

        InitFunctionCallInfoData!(
            fcinfo,
            &mut (*typcache).rng_canonical_finfo,
            1,
            InvalidOid,
            escontext,
            null_mut()
        );

        (*fcinfo).args.as_mut_ptr().add(0).write(crate::postgres::NullableDatum {
            value: RangeTypePGetDatum(range),
            isnull: false,
        });

        result = FunctionCallInvoke!(fcinfo);

        if SOFT_ERROR_OCCURRED!(escontext) {
            return null_mut();
        }

        /* Should not get a null result if there was no error */
        if (*fcinfo).isnull {
            elog!(
                ERROR,
                "function {} returned NULL",
                (*typcache).rng_canonical_finfo.fn_oid
            );
        }

        range = DatumGetRangeTypeP(result);
    }

    range
}

/*
 * Compare two range boundary points, returning <0, 0, or >0 according to
 * whether b1 is less than, equal to, or greater than b2.
 *
 * The boundaries can be any combination of upper and lower; so it's useful
 * for a variety of operators.
 *
 * The simple case is when b1 and b2 are both finite and inclusive, in which
 * case the result is just a comparison of the values held in b1 and b2.
 *
 * If a bound is exclusive, then we need to know whether it's a lower bound,
 * in which case we treat the boundary point as "just greater than" the held
 * value; or an upper bound, in which case we treat the boundary point as
 * "just less than" the held value.
 *
 * If a bound is infinite, it represents minus infinity (less than every other
 * point) if it's a lower bound; or plus infinity (greater than every other
 * point) if it's an upper bound.
 *
 * There is only one case where two boundaries compare equal but are not
 * identical: when both bounds are inclusive and hold the same finite value,
 * but one is an upper bound and the other a lower bound.
 */
pub unsafe fn range_cmp_bounds(
    typcache: *mut TypeCacheEntry,
    b1: *const RangeBound,
    b2: *const RangeBound,
) -> c_int {
    let result: int32;

    /*
     * First, handle cases involving infinity, which don't require invoking
     * the comparison proc.
     */
    if (*b1).infinite && (*b2).infinite {
        /*
         * Both are infinity, so they are equal unless one is lower and the
         * other not.
         */
        if (*b1).lower == (*b2).lower {
            return 0;
        } else {
            return if (*b1).lower { -1 } else { 1 };
        }
    } else if (*b1).infinite {
        return if (*b1).lower { -1 } else { 1 };
    } else if (*b2).infinite {
        return if (*b2).lower { 1 } else { -1 };
    }

    /*
     * Both boundaries are finite, so compare the held values.
     */
    result = DatumGetInt32(FunctionCall2Coll(
        &mut (*typcache).rng_cmp_proc_finfo,
        (*typcache).rng_collation,
        (*b1).val,
        (*b2).val,
    ));

    /*
     * If the comparison is anything other than equal, we're done. If they
     * compare equal though, we still have to consider whether the boundaries
     * are inclusive or exclusive.
     */
    if result == 0 {
        if !(*b1).inclusive && !(*b2).inclusive {
            /* both are exclusive */
            if (*b1).lower == (*b2).lower {
                return 0;
            } else {
                return if (*b1).lower { 1 } else { -1 };
            }
        } else if !(*b1).inclusive {
            return if (*b1).lower { 1 } else { -1 };
        } else if !(*b2).inclusive {
            return if (*b2).lower { -1 } else { 1 };
        } else {
            /*
             * Both are inclusive and the values held are equal, so they are
             * equal regardless of whether they are upper or lower boundaries,
             * or a mix.
             */
            return 0;
        }
    }

    result
}

/*
 * Compare two range boundary point values, returning <0, 0, or >0 according
 * to whether b1 is less than, equal to, or greater than b2.
 *
 * This is similar to but simpler than range_cmp_bounds().  We just compare
 * the values held in b1 and b2, ignoring inclusive/exclusive flags.  The
 * lower/upper flags only matter for infinities, where they tell us if the
 * infinity is plus or minus.
 */
pub unsafe fn range_cmp_bound_values(
    typcache: *mut TypeCacheEntry,
    b1: *const RangeBound,
    b2: *const RangeBound,
) -> c_int {
    /*
     * First, handle cases involving infinity, which don't require invoking
     * the comparison proc.
     */
    if (*b1).infinite && (*b2).infinite {
        /*
         * Both are infinity, so they are equal unless one is lower and the
         * other not.
         */
        if (*b1).lower == (*b2).lower {
            return 0;
        } else {
            return if (*b1).lower { -1 } else { 1 };
        }
    } else if (*b1).infinite {
        return if (*b1).lower { -1 } else { 1 };
    } else if (*b2).infinite {
        return if (*b2).lower { 1 } else { -1 };
    }

    /*
     * Both boundaries are finite, so compare the held values.
     */
    DatumGetInt32(FunctionCall2Coll(
        &mut (*typcache).rng_cmp_proc_finfo,
        (*typcache).rng_collation,
        (*b1).val,
        (*b2).val,
    ))
}

/*
 * qsort callback for sorting ranges.
 *
 * Two empty ranges compare equal; an empty range sorts to the left of any
 * non-empty range.  Two non-empty ranges are sorted by lower bound first
 * and by upper bound next.
 */
pub unsafe fn range_compare(key1: *const c_void, key2: *const c_void, arg: *mut c_void) -> c_int {
    let r1 = *(key1 as *const *mut RangeType);
    let r2 = *(key2 as *const *mut RangeType);
    let typcache = arg as *mut TypeCacheEntry;
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut empty2: bool = false;
    let mut cmp: c_int;

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    if empty1 && empty2 {
        cmp = 0;
    } else if empty1 {
        cmp = -1;
    } else if empty2 {
        cmp = 1;
    } else {
        cmp = range_cmp_bounds(typcache, &lower1, &lower2);
        if cmp == 0 {
            cmp = range_cmp_bounds(typcache, &upper1, &upper2);
        }
    }

    cmp
}

/*
 * Build an empty range value of the type indicated by the typcache entry.
 */
pub unsafe fn make_empty_range(typcache: *mut TypeCacheEntry) -> *mut RangeType {
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();

    lower.val = 0 as Datum;
    lower.infinite = false;
    lower.inclusive = false;
    lower.lower = true;

    upper.val = 0 as Datum;
    upper.infinite = false;
    upper.inclusive = false;
    upper.lower = false;

    make_range(typcache, &mut lower, &mut upper, true, null_mut())
}

/*
 * Planner support function for elem_contained_by_range (<@ operator).
 */
pub unsafe fn elem_contained_by_range_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let mut ret: *mut Node = null_mut();

    if IsA!(rawreq, T_SupportRequestSimplify) {
        let req = rawreq as *mut SupportRequestSimplify;
        let fexpr = (*req).fcall;
        let leftop: *mut Expr;
        let rightop: *mut Expr;

        Assert!(list_length((*fexpr).args) == 2);
        leftop = linitial((*fexpr).args) as *mut Expr;
        rightop = lsecond((*fexpr).args) as *mut Expr;

        ret = find_simplified_clause((*req).root, rightop, leftop);
    }

    PG_RETURN_POINTER!(ret);
}

/*
 * Planner support function for range_contains_elem (@> operator).
 */
pub unsafe fn range_contains_elem_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let mut ret: *mut Node = null_mut();

    if IsA!(rawreq, T_SupportRequestSimplify) {
        let req = rawreq as *mut SupportRequestSimplify;
        let fexpr = (*req).fcall;
        let leftop: *mut Expr;
        let rightop: *mut Expr;

        Assert!(list_length((*fexpr).args) == 2);
        leftop = linitial((*fexpr).args) as *mut Expr;
        rightop = lsecond((*fexpr).args) as *mut Expr;

        ret = find_simplified_clause((*req).root, leftop, rightop);
    }

    PG_RETURN_POINTER!(ret);
}

/*
 *----------------------------------------------------------
 * STATIC FUNCTIONS
 *----------------------------------------------------------
 */

/*
 * Given a string representing the flags for the range type, return the flags
 * represented as a char.
 */
unsafe fn range_parse_flags(flags_str: *const c_char) -> c_char {
    let mut flags: c_char = 0;

    if *flags_str.add(0) == b'\0' as c_char
        || *flags_str.add(1) == b'\0' as c_char
        || *flags_str.add(2) != b'\0' as c_char
    {
        ereport!(ERROR, errmsg!("invalid range bound flags"));
    }

    match *flags_str.add(0) as u8 {
        b'[' => {
            flags |= RANGE_LB_INC;
        }
        b'(' => {}
        _ => {
            ereport!(ERROR, errmsg!("invalid range bound flags"));
        }
    }

    match *flags_str.add(1) as u8 {
        b']' => {
            flags |= RANGE_UB_INC;
        }
        b')' => {}
        _ => {
            ereport!(ERROR, errmsg!("invalid range bound flags"));
        }
    }

    flags
}

/*
 * Parse range input.
 *
 * Input parameters:
 *	string: input string to be parsed
 * Output parameters:
 *	*flags: receives flags bitmask
 *	*lbound_str: receives palloc'd lower bound string, or NULL if none
 *	*ubound_str: receives palloc'd upper bound string, or NULL if none
 *
 * This is modeled somewhat after record_in in rowtypes.c.
 * The input syntax is:
 *	<range>   := EMPTY
 *			   | <lb-inc> <string>, <string> <ub-inc>
 *	<lb-inc>  := '[' | '('
 *	<ub-inc>  := ']' | ')'
 *
 * Whitespace before or after <range> is ignored.  Whitespace within a <string>
 * is taken literally and becomes part of the input string for that bound.
 *
 * A <string> of length zero is taken as "infinite" (i.e. no bound), unless it
 * is surrounded by double-quotes, in which case it is the literal empty
 * string.
 *
 * Within a <string>, special characters (such as comma, parenthesis, or
 * brackets) can be enclosed in double-quotes or escaped with backslash. Within
 * double-quotes, a double-quote can be escaped with double-quote or backslash.
 *
 * Returns true on success, false on failure (but failures will return only if
 * escontext is an ErrorSaveContext).
 */
unsafe fn range_parse(
    string: *const c_char,
    flags: *mut c_char,
    lbound_str: *mut *mut c_char,
    ubound_str: *mut *mut c_char,
    escontext: *mut Node,
) -> bool {
    let mut ptr = string;
    let mut infinite: bool = false;

    *flags = 0;

    /* consume whitespace */
    while *ptr != b'\0' as c_char && isspace(*ptr as c_uchar as c_int) != 0 {
        ptr = ptr.add(1);
    }

    /* check for empty range */
    if pg_strncasecmp(
        ptr,
        RANGE_EMPTY_LITERAL.as_ptr() as *const c_char,
        strlen(RANGE_EMPTY_LITERAL.as_ptr() as *const c_char),
    ) == 0
    {
        *flags = RANGE_EMPTY;
        *lbound_str = null_mut();
        *ubound_str = null_mut();

        ptr = ptr.add(strlen(RANGE_EMPTY_LITERAL.as_ptr() as *const c_char));

        /* the rest should be whitespace */
        while *ptr != b'\0' as c_char && isspace(*ptr as c_uchar as c_int) != 0 {
            ptr = ptr.add(1);
        }

        /* should have consumed everything */
        if *ptr != b'\0' as c_char {
            ereturn!(
                escontext,
                false,
                errmsg!(
                    "malformed range literal: \"{}\"",
                    std::ffi::CStr::from_ptr(string).to_string_lossy()
                )
            );
        }

        return true;
    }

    if *ptr == b'[' as c_char {
        *flags |= RANGE_LB_INC;
        ptr = ptr.add(1);
    } else if *ptr == b'(' as c_char {
        ptr = ptr.add(1);
    } else {
        ereturn!(
            escontext,
            false,
            errmsg!(
                "malformed range literal: \"{}\"",
                std::ffi::CStr::from_ptr(string).to_string_lossy()
            )
        );
    }

    let p = range_parse_bound(string, ptr, lbound_str, &mut infinite, escontext);
    if p.is_null() {
        return false;
    }
    ptr = p;
    if infinite {
        *flags |= RANGE_LB_INF;
    }

    if *ptr == b',' as c_char {
        ptr = ptr.add(1);
    } else {
        ereturn!(
            escontext,
            false,
            errmsg!(
                "malformed range literal: \"{}\"",
                std::ffi::CStr::from_ptr(string).to_string_lossy()
            )
        );
    }

    let p = range_parse_bound(string, ptr, ubound_str, &mut infinite, escontext);
    if p.is_null() {
        return false;
    }
    ptr = p;
    if infinite {
        *flags |= RANGE_UB_INF;
    }

    if *ptr == b']' as c_char {
        *flags |= RANGE_UB_INC;
        ptr = ptr.add(1);
    } else if *ptr == b')' as c_char {
        ptr = ptr.add(1);
    } else {
        /* must be a comma */
        ereturn!(
            escontext,
            false,
            errmsg!(
                "malformed range literal: \"{}\"",
                std::ffi::CStr::from_ptr(string).to_string_lossy()
            )
        );
    }

    /* consume whitespace */
    while *ptr != b'\0' as c_char && isspace(*ptr as c_uchar as c_int) != 0 {
        ptr = ptr.add(1);
    }

    if *ptr != b'\0' as c_char {
        ereturn!(
            escontext,
            false,
            errmsg!(
                "malformed range literal: \"{}\"",
                std::ffi::CStr::from_ptr(string).to_string_lossy()
            )
        );
    }

    true
}

/*
 * Helper for range_parse: parse and de-quote one bound string.
 *
 * We scan until finding comma, right parenthesis, or right bracket.
 *
 * Input parameters:
 *	string: entire input string (used only for error reports)
 *	ptr: where to start parsing bound
 * Output parameters:
 *	*bound_str: receives palloc'd bound string, or NULL if none
 *	*infinite: set true if no bound, else false
 *
 * The return value is the scan ptr, advanced past the bound string.
 * However, if escontext is an ErrorSaveContext, we return NULL on failure.
 */
unsafe fn range_parse_bound(
    string: *const c_char,
    mut ptr: *const c_char,
    bound_str: *mut *mut c_char,
    infinite: *mut bool,
    escontext: *mut Node,
) -> *const c_char {
    let mut buf: StringInfoData = std::mem::zeroed();

    /* Check for null: completely empty input means null */
    if *ptr == b',' as c_char || *ptr == b')' as c_char || *ptr == b']' as c_char {
        *bound_str = null_mut();
        *infinite = true;
    } else {
        /* Extract string for this bound */
        let mut inquote: bool = false;

        initStringInfo(&mut buf);
        while inquote
            || !(*ptr == b',' as c_char || *ptr == b')' as c_char || *ptr == b']' as c_char)
        {
            let ch = *ptr;
            ptr = ptr.add(1);

            if ch == b'\0' as c_char {
                ereturn!(
                    escontext,
                    null(),
                    errmsg!(
                        "malformed range literal: \"{}\"",
                        std::ffi::CStr::from_ptr(string).to_string_lossy()
                    )
                );
            }
            if ch == b'\\' as c_char {
                if *ptr == b'\0' as c_char {
                    ereturn!(
                        escontext,
                        null(),
                        errmsg!(
                            "malformed range literal: \"{}\"",
                            std::ffi::CStr::from_ptr(string).to_string_lossy()
                        )
                    );
                }
                appendStringInfoChar(&mut buf, *ptr);
                ptr = ptr.add(1);
            } else if ch == b'"' as c_char {
                if !inquote {
                    inquote = true;
                } else if *ptr == b'"' as c_char {
                    /* doubled quote within quote sequence */
                    appendStringInfoChar(&mut buf, *ptr);
                    ptr = ptr.add(1);
                } else {
                    inquote = false;
                }
            } else {
                appendStringInfoChar(&mut buf, ch);
            }
        }

        *bound_str = buf.data;
        *infinite = false;
    }

    ptr
}

/*
 * Convert a deserialized range value to text form
 *
 * Inputs are the flags byte, and the two bound values already converted to
 * text (but not yet quoted).  If no bound value, pass NULL.
 *
 * Result is a palloc'd string
 */
unsafe fn range_deparse(flags: c_char, lbound_str: *const c_char, ubound_str: *const c_char) -> *mut c_char {
    let mut buf: StringInfoData = std::mem::zeroed();

    if (flags & RANGE_EMPTY) != 0 {
        return pstrdup(RANGE_EMPTY_LITERAL.as_ptr() as *const c_char);
    }

    initStringInfo(&mut buf);

    appendStringInfoChar(&mut buf, if (flags & RANGE_LB_INC) != 0 { b'[' as c_char } else { b'(' as c_char });

    if RANGE_HAS_LBOUND(flags) {
        appendStringInfoString(&mut buf, range_bound_escape(lbound_str));
    }

    appendStringInfoChar(&mut buf, b',' as c_char);

    if RANGE_HAS_UBOUND(flags) {
        appendStringInfoString(&mut buf, range_bound_escape(ubound_str));
    }

    appendStringInfoChar(&mut buf, if (flags & RANGE_UB_INC) != 0 { b']' as c_char } else { b')' as c_char });

    buf.data
}

/*
 * Helper for range_deparse: quote a bound value as needed
 *
 * Result is a palloc'd string
 */
unsafe fn range_bound_escape(value: *const c_char) -> *mut c_char {
    let mut nq: bool;
    let mut ptr: *const c_char;
    let mut buf: StringInfoData = std::mem::zeroed();

    initStringInfo(&mut buf);

    /* Detect whether we need double quotes for this value */
    nq = *value.add(0) == b'\0' as c_char; /* force quotes for empty string */
    ptr = value;
    while *ptr != 0 {
        let ch = *ptr;

        if ch == b'"' as c_char
            || ch == b'\\' as c_char
            || ch == b'(' as c_char
            || ch == b')' as c_char
            || ch == b'[' as c_char
            || ch == b']' as c_char
            || ch == b',' as c_char
            || isspace(ch as c_uchar as c_int) != 0
        {
            nq = true;
            break;
        }
        ptr = ptr.add(1);
    }

    /* And emit the string */
    if nq {
        appendStringInfoChar(&mut buf, b'"' as c_char);
    }
    ptr = value;
    while *ptr != 0 {
        let ch = *ptr;

        if ch == b'"' as c_char || ch == b'\\' as c_char {
            appendStringInfoChar(&mut buf, ch);
        }
        appendStringInfoChar(&mut buf, ch);
        ptr = ptr.add(1);
    }
    if nq {
        appendStringInfoChar(&mut buf, b'"' as c_char);
    }

    buf.data
}

/*
 * Test whether range r1 contains range r2.
 *
 * Caller has already checked that they are the same range type, and looked up
 * the necessary typcache entry.
 */
pub unsafe fn range_contains_internal(
    typcache: *mut TypeCacheEntry,
    r1: *const RangeType,
    r2: *const RangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut empty1: bool = false;
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty2: bool = false;

    /* Different types should be prevented by ANYRANGE matching rules */
    if RangeTypeGetOid(r1) != RangeTypeGetOid(r2) {
        elog!(ERROR, "range types do not match");
    }

    range_deserialize(typcache, r1, &mut lower1, &mut upper1, &mut empty1);
    range_deserialize(typcache, r2, &mut lower2, &mut upper2, &mut empty2);

    /* If either range is empty, the answer is easy */
    if empty2 {
        return true;
    } else if empty1 {
        return false;
    }

    /* Else we must have lower1 <= lower2 and upper1 >= upper2 */
    if range_cmp_bounds(typcache, &lower1, &lower2) > 0 {
        return false;
    }
    if range_cmp_bounds(typcache, &upper1, &upper2) < 0 {
        return false;
    }

    true
}

pub unsafe fn range_contained_by_internal(
    typcache: *mut TypeCacheEntry,
    r1: *const RangeType,
    r2: *const RangeType,
) -> bool {
    range_contains_internal(typcache, r2, r1)
}

/*
 * Test whether range r contains a specific element value.
 */
pub unsafe fn range_contains_elem_internal(
    typcache: *mut TypeCacheEntry,
    r: *const RangeType,
    val: Datum,
) -> bool {
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;
    let mut cmp: int32;

    range_deserialize(typcache, r, &mut lower, &mut upper, &mut empty);

    if empty {
        return false;
    }

    if !lower.infinite {
        cmp = DatumGetInt32(FunctionCall2Coll(
            &mut (*typcache).rng_cmp_proc_finfo,
            (*typcache).rng_collation,
            lower.val,
            val,
        ));
        if cmp > 0 {
            return false;
        }
        if cmp == 0 && !lower.inclusive {
            return false;
        }
    }

    if !upper.infinite {
        cmp = DatumGetInt32(FunctionCall2Coll(
            &mut (*typcache).rng_cmp_proc_finfo,
            (*typcache).rng_collation,
            upper.val,
            val,
        ));
        if cmp < 0 {
            return false;
        }
        if cmp == 0 && !upper.inclusive {
            return false;
        }
    }

    true
}

/*
 * datum_compute_size() and datum_write() are used to insert the bound
 * values into a range object.  They are modeled after heaptuple.c's
 * heap_compute_data_size() and heap_fill_tuple(), but we need not handle
 * null values here.  TYPE_IS_PACKABLE must test the same conditions as
 * heaptuple.c's ATT_IS_PACKABLE macro.  See the comments there for more
 * details.
 */

/* Does datatype allow packing into the 1-byte-header varlena format? */
#[inline]
fn TYPE_IS_PACKABLE(typlen: int16, typstorage: c_char) -> bool {
    typlen == -1 && typstorage != TYPSTORAGE_PLAIN
}

/*
 * Increment data_length by the space needed by the datum, including any
 * preceding alignment padding.
 */
unsafe fn datum_compute_size(
    mut data_length: Size,
    val: Datum,
    _typbyval: bool,
    typalign: c_char,
    typlen: int16,
    typstorage: c_char,
) -> Size {
    if TYPE_IS_PACKABLE(typlen, typstorage)
        && VARATT_CAN_MAKE_SHORT(DatumGetPointer(val) as *const c_char)
    {
        /*
         * we're anticipating converting to a short varlena header, so adjust
         * length and don't count any alignment
         */
        data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val) as *const c_char) as Size;
    } else {
        data_length = att_align_datum(data_length, typalign, typlen as c_int, val);
        data_length = att_addlength_datum(data_length, typlen as c_int, val);
    }

    data_length
}

/*
 * Write the given datum beginning at ptr (after advancing to correct
 * alignment, if needed).  Return the pointer incremented by space used.
 */
unsafe fn datum_write(
    mut ptr: Pointer,
    datum: Datum,
    typbyval: bool,
    typalign: c_char,
    typlen: int16,
    typstorage: c_char,
) -> Pointer {
    let data_length: Size;

    if typbyval {
        /* pass-by-value */
        ptr = att_nominal_align(ptr, typalign);
        store_att_byval(ptr as *mut c_void, datum, typlen as c_int);
        data_length = typlen as Size;
    } else if typlen == -1 {
        /* varlena */
        let val = DatumGetPointer(datum) as *mut c_char;

        if VARATT_IS_EXTERNAL(val) {
            /*
             * Throw error, because we must never put a toast pointer inside a
             * range object.  Caller should have detoasted it.
             */
            elog!(ERROR, "cannot store a toast pointer inside a range");
            #[allow(unreachable_code)]
            {
                data_length = 0; /* keep compiler quiet */
            }
        } else if VARATT_IS_SHORT(val) {
            /* no alignment for short varlenas */
            data_length = VARSIZE_SHORT(val) as Size;
            memcpy(ptr as *mut c_void, val as *const c_void, data_length);
        } else if TYPE_IS_PACKABLE(typlen, typstorage) && VARATT_CAN_MAKE_SHORT(val) {
            /* convert to short varlena -- no alignment */
            data_length = VARATT_CONVERTED_SHORT_SIZE(val) as Size;
            SET_VARSIZE_SHORT(ptr, data_length as int32);
            memcpy(
                ptr.add(1) as *mut c_void,
                VARDATA(val) as *const c_void,
                data_length - 1,
            );
        } else {
            /* full 4-byte header varlena */
            ptr = att_nominal_align(ptr, typalign);
            data_length = VARSIZE(val) as Size;
            memcpy(ptr as *mut c_void, val as *const c_void, data_length);
        }
    } else if typlen == -2 {
        /* cstring ... never needs alignment */
        Assert!(typalign == TYPALIGN_CHAR);
        data_length = strlen(DatumGetCString(datum)) + 1;
        memcpy(
            ptr as *mut c_void,
            DatumGetPointer(datum) as *const c_void,
            data_length,
        );
    } else {
        /* fixed-length pass-by-reference */
        ptr = att_nominal_align(ptr, typalign);
        Assert!(typlen > 0);
        data_length = typlen as Size;
        memcpy(
            ptr as *mut c_void,
            DatumGetPointer(datum) as *const c_void,
            data_length,
        );
    }

    ptr = ptr.add(data_length);

    ptr
}

/*
 * att_align_nominal as applied to a raw pointer (the C source uses
 * att_align_nominal(ptr, typalign) where the macro operates on the integer
 * value of the pointer).  Implement that here by rounding the pointer address.
 */
#[inline]
unsafe fn att_nominal_align(ptr: Pointer, typalign: c_char) -> Pointer {
    let aligned = att_align_nominal(ptr as usize, typalign);
    aligned as Pointer
}

/*
 * Common code for the elem_contained_by_range and range_contains_elem
 * support functions.  The caller has extracted the function argument
 * expressions, and swapped them if necessary to pass the range first.
 *
 * Returns a simplified replacement expression, or NULL if we can't simplify.
 */
unsafe fn find_simplified_clause(
    root: *mut crate::nodes::pathnodes::PlannerInfo,
    rangeExpr: *mut Expr,
    mut elemExpr: *mut Expr,
) -> *mut Node {
    let range: *mut RangeType;
    let rangetypcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    /* can't do anything unless the range is a non-null constant */
    if !IsA!(rangeExpr, T_Const) || (*(rangeExpr as *mut Const)).constisnull {
        return null_mut();
    }
    range = DatumGetRangeTypeP((*(rangeExpr as *mut Const)).constvalue);

    rangetypcache = lookup_type_cache(RangeTypeGetOid(range), TYPECACHE_RANGE_INFO);
    if (*rangetypcache).rngelemtype.is_null() {
        elog!(ERROR, "type {} is not a range type", RangeTypeGetOid(range));
    }

    range_deserialize(rangetypcache, range, &mut lower, &mut upper, &mut empty);

    if empty {
        /* if the range is empty, then there can be no matches */
        makeBoolConst(false, false)
    } else if lower.infinite && upper.infinite {
        /* the range has infinite bounds, so it matches everything */
        makeBoolConst(true, false)
    } else {
        /* at least one bound is available, we have something to work with */
        let elemTypcache = (*rangetypcache).rngelemtype;
        let opfamily = (*rangetypcache).rng_opfamily;
        let rng_collation = (*rangetypcache).rng_collation;
        let mut lowerExpr: *mut Expr = null_mut();
        let mut upperExpr: *mut Expr = null_mut();

        if !lower.infinite && !upper.infinite {
            /*
             * When both bounds are present, we have a problem: the
             * "simplified" clause would need to evaluate the elemExpr twice.
             * That's definitely not okay if the elemExpr is volatile, and
             * it's also unattractive if the elemExpr is expensive.
             */
            let mut eval_cost: crate::nodes::pathnodes::QualCost = std::mem::zeroed();

            if contain_volatile_functions(elemExpr as *mut Node) {
                return null_mut();
            }

            /*
             * We define "expensive" as "contains any subplan or more than 10
             * operators".  Note that the subplan search has to be done
             * explicitly, since cost_qual_eval() will barf on unplanned
             * subselects.
             */
            if contain_subplans(elemExpr as *mut Node) {
                return null_mut();
            }
            cost_qual_eval_node(&mut eval_cost, elemExpr as *mut Node, root);
            if eval_cost.startup + eval_cost.per_tuple > 10.0 * cpu_operator_cost {
                return null_mut();
            }
        }

        /* Okay, try to build boundary comparison expressions */
        if !lower.infinite {
            lowerExpr = build_bound_expr(
                elemExpr,
                lower.val,
                true,
                lower.inclusive,
                elemTypcache,
                opfamily,
                rng_collation,
            );
            if lowerExpr.is_null() {
                return null_mut();
            }
        }

        if !upper.infinite {
            /* Copy the elemExpr if we need two copies */
            if !lower.infinite {
                elemExpr = copyObject(elemExpr);
            }
            upperExpr = build_bound_expr(
                elemExpr,
                upper.val,
                false,
                upper.inclusive,
                elemTypcache,
                opfamily,
                rng_collation,
            );
            if upperExpr.is_null() {
                return null_mut();
            }
        }

        if !lowerExpr.is_null() && !upperExpr.is_null() {
            make_andclause(list_make2!(lowerExpr, upperExpr)) as *mut Node
        } else if !lowerExpr.is_null() {
            lowerExpr as *mut Node
        } else if !upperExpr.is_null() {
            upperExpr as *mut Node
        } else {
            Assert!(false);
            null_mut()
        }
    }
}

/*
 * Helper function for find_simplified_clause().
 *
 * Build the expression (elemExpr Operator val), where the operator is
 * the appropriate member of the given opfamily depending on
 * isLowerBound and isInclusive.  typeCache is the typcache entry for
 * the "val" value (presently, this will be the same type as elemExpr).
 * rng_collation is the collation to use in the comparison.
 *
 * Return NULL on failure (if, for some reason, we can't find the operator).
 */
unsafe fn build_bound_expr(
    elemExpr: *mut Expr,
    val: Datum,
    isLowerBound: bool,
    isInclusive: bool,
    typeCache: *mut TypeCacheEntry,
    opfamily: Oid,
    rng_collation: Oid,
) -> *mut Expr {
    let elemType = (*typeCache).type_id;
    let elemTypeLen = (*typeCache).typlen;
    let elemByValue = (*typeCache).typbyval;
    let elemCollation = (*typeCache).typcollation;
    let strategy: int16;
    let oproid: Oid;
    let constExpr: *mut Expr;

    /* Identify the comparison operator to use */
    if isLowerBound {
        strategy = if isInclusive {
            BTGreaterEqualStrategyNumber as int16
        } else {
            BTGreaterStrategyNumber as int16
        };
    } else {
        strategy = if isInclusive {
            BTLessEqualStrategyNumber as int16
        } else {
            BTLessStrategyNumber as int16
        };
    }

    /*
     * We could use exprType(elemExpr) here, if it ever becomes possible that
     * elemExpr is not the exact same type as the range elements.
     */
    oproid = get_opfamily_member(opfamily, elemType, elemType, strategy);

    /* We don't really expect failure here, but just in case ... */
    if !OidIsValid(oproid) {
        return null_mut();
    }

    /* OK, convert "val" to a full-fledged Const node, and make the OpExpr */
    constExpr = makeConst(
        elemType,
        -1,
        elemCollation,
        elemTypeLen as c_int,
        val,
        false,
        elemByValue,
    ) as *mut Expr;

    make_opclause(
        oproid,
        BOOLOID,
        false,
        elemExpr,
        constExpr,
        InvalidOid,
        rng_collation,
    )
}
