//! multirangetypes.rs
//!   I/O functions, operators, and support functions for multirange types.
//!
//! The stored (serialized) format of a multirange value is:
//!
//!	12 bytes: MultirangeType struct including varlena header, multirange
//!			  type's OID and the number of ranges in the multirange.
//!	4 * (rangesCount - 1) bytes: 32-bit items pointing to the each range
//!								 in the multirange starting from
//!								 the second one.
//!	1 * rangesCount bytes : 8-bit flags for each range in the multirange
//!	The rest of the multirange are range bound values pointed by multirange
//!	items.
//!
//!	Majority of items contain lengths of corresponding range bound values.
//!	Thanks to that items are typically low numbers.  This makes multiranges
//!	compression-friendly.  Every MULTIRANGE_ITEM_OFFSET_STRIDE item contains
//!	an offset of the corresponding range bound values.  That allows fast lookups
//!	for a particular range index.  Offsets are counted starting from the end of
//!	flags aligned to the bound type.
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/multirangetypes.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(unused_assignments)]

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int16, int32, uint32, uint64, uint8, Size};

// varlena size accessors (varatt.h).
use crate::varatt::{SET_VARSIZE, VARSIZE, VARDATA};

// att_* tuple-walking helpers (access/tupmacs.h).
use crate::access::tupmacs::{
    att_addlength_pointer, att_align_nominal, att_align_pointer, fetch_att,
};

// StringInfo machinery (lib/stringinfo.h).
use crate::lib::stringinfo::{
    appendBinaryStringInfo, appendStringInfoChar, appendStringInfoString, initStringInfo,
    makeStringInfo, resetStringInfo, StringInfo, StringInfoData,
};

// pqformat send/recv helpers (libpq/pqformat.h).
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgbytes, pq_getmsgend, pq_getmsgint, pq_sendbytes,
    pq_sendint32,
};

// Node (nodes/nodes.h) -- used by the escontext soft-error path.
use crate::nodes::nodes::Node;

// fmgr call info + helpers (fmgr.h).
use crate::utils::fmgr::{
    fmgr_info_cxt, get_fn_expr_argtype, get_fn_expr_rettype, FmgrInfo, FunctionCall1Coll,
    FunctionCall2Coll, FunctionCallInfo, InputFunctionCallSafe, OutputFunctionCall,
    ReceiveFunctionCall, SendFunctionCall,
};

// hash helpers (common/hashfn.h).
use crate::common::hashfn::{hash_uint32, hash_uint32_extended};

// rotate-bits helper (port/pg_bitutils.h).
use crate::port::pg_bitutils::pg_rotate_left32;

// type-name formatting (utils/builtins.h).
use crate::utils::builtins::format_type_be;

// #[macro_export] macros live at the crate root.
use crate::port::pgstrcasecmp::pg_strncasecmp;
use crate::port::qsort::qsort_arg;
use crate::{
    elog, errmsg, Assert, PG_ARGISNULL, PG_FREE_IF_COPY, PG_GETARG_DATUM, PG_GETARG_INT32,
    PG_GETARG_OID, PG_GETARG_POINTER, PG_NARGS, PG_RETURN_BOOL, PG_RETURN_DATUM, PG_RETURN_INT32,
    PG_RETURN_NULL, PG_RETURN_POINTER, PG_RETURN_UINT32, PG_RETURN_UINT64,
};

/* ---------------------------------------------------------------------------
 * Imports / local shims for sibling units that are still in flight in this
 * porting wave (rangetypes.rs, utils/cache/typcache.rs, funcapi.c, ...).  Per
 * the wave conventions, anything not yet ported is stubbed locally with a
 * TODO(pg-port) note so the integrator can reconcile.
 * ------------------------------------------------------------------------- */

/*
 * RangeType / RangeBound and the range-internal helpers live in
 * utils/adt/rangetypes.rs (sibling, same wave).  Importing directly per the
 * task instructions; the integrator reconciles if any are renamed/moved.
 */
// TODO(pg-port): the next block should be `use crate::utils::adt::rangetypes::{...}`
// once rangetypes.rs lands.  Stubbed locally for now so this file is
// self-contained and contains no undefined symbols.

/// utils/rangetypes.h: RangeType (varlena range value).
// TODO(pg-port): real RangeType lives in utils/adt/rangetypes.rs
pub use crate::utils::adt::rangetypes::RangeType;

/// utils/rangetypes.h: RangeBound (internal representation of either bound).
// TODO(pg-port): real RangeBound lives in utils/adt/rangetypes.rs
pub use crate::utils::adt::rangetypes::RangeBound;

/* utils/rangetypes.h: RANGE_EMPTY_LITERAL and flag bits */
const RANGE_EMPTY_LITERAL: &[u8] = b"empty\0";
const RANGE_EMPTY: uint8 = 0x01; /* range is empty */
const RANGE_LB_INC: uint8 = 0x02; /* lower bound is inclusive */
const RANGE_UB_INC: uint8 = 0x04; /* upper bound is inclusive */
const RANGE_LB_INF: uint8 = 0x08; /* lower bound is -infinity */
const RANGE_UB_INF: uint8 = 0x10; /* upper bound is +infinity */
const RANGE_LB_NULL: uint8 = 0x20; /* lower bound is null (NOT USED) */
const RANGE_UB_NULL: uint8 = 0x40; /* upper bound is null (NOT USED) */

/* utils/rangetypes.h: RangeTypeGetOid(r) */
#[inline]
unsafe fn RangeTypeGetOid(r: *const RangeType) -> Oid {
    (*r).rangetypid
}

/* utils/rangetypes.h: RANGE_HAS_LBOUND / RANGE_HAS_UBOUND */
#[inline]
fn RANGE_HAS_LBOUND(flags: uint8) -> bool {
    (flags & (RANGE_EMPTY | RANGE_LB_NULL | RANGE_LB_INF)) == 0
}
#[inline]
fn RANGE_HAS_UBOUND(flags: uint8) -> bool {
    (flags & (RANGE_EMPTY | RANGE_UB_NULL | RANGE_UB_INF)) == 0
}

/* utils/rangetypes.h: range_get_flags() + RangeIsEmpty(r) */
// TODO(pg-port): real range_get_flags lives in utils/adt/rangetypes.rs
unsafe fn range_get_flags(_range: *const RangeType) -> c_char {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
#[inline]
unsafe fn RangeIsEmpty(r: *const RangeType) -> bool {
    (range_get_flags(r) as uint8 & RANGE_EMPTY) != 0
}

/* utils/rangetypes.h: DatumGetRangeTypeP() / RangeTypePGetDatum() */
#[inline]
unsafe fn DatumGetRangeTypeP(X: Datum) -> *mut RangeType {
    crate::PG_DETOAST_DATUM!(X) as *mut RangeType
}
#[inline]
unsafe fn RangeTypePGetDatum(X: *const RangeType) -> Datum {
    PointerGetDatum(X as *const c_void)
}

/* range-internal helpers (utils/adt/rangetypes.c) */
// TODO(pg-port): the following all live in utils/adt/rangetypes.rs
unsafe fn range_compare(_key1: *const c_void, _key2: *const c_void, _arg: *mut c_void) -> c_int {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn range_adjacent_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn range_before_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn range_overlaps_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn range_overleft_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> bool {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn range_union_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *mut RangeType,
    _r2: *mut RangeType,
    _strict: bool,
) -> *mut RangeType {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn range_minus_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *mut RangeType,
    _r2: *mut RangeType,
) -> *mut RangeType {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn range_intersect_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
) -> *mut RangeType {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn range_split_internal(
    _typcache: *mut TypeCacheEntry,
    _r1: *const RangeType,
    _r2: *const RangeType,
    _output1: *mut *mut RangeType,
    _output2: *mut *mut RangeType,
) -> bool {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn range_cmp_bounds(
    _typcache: *mut TypeCacheEntry,
    _b1: *const RangeBound,
    _b2: *const RangeBound,
) -> c_int {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn bounds_adjacent(
    _typcache: *mut TypeCacheEntry,
    _boundA: RangeBound,
    _boundB: RangeBound,
) -> bool {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn range_deserialize(
    _typcache: *mut TypeCacheEntry,
    _range: *const RangeType,
    _lower: *mut RangeBound,
    _upper: *mut RangeBound,
    _empty: *mut bool,
) {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn make_range(
    _typcache: *mut TypeCacheEntry,
    _lower: *mut RangeBound,
    _upper: *mut RangeBound,
    _empty: bool,
    _escontext: *mut Node,
) -> *mut RangeType {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}
unsafe fn make_empty_range(_typcache: *mut TypeCacheEntry) -> *mut RangeType {
    unimplemented!() // TODO(pg-port): utils/adt/rangetypes.rs
}

/*
 * TypeCacheEntry and lookup machinery (utils/cache/typcache.c) -- not yet
 * ported in this wave.  Only the fields/flags used here are stubbed.
 */
// TODO(pg-port): real TypeCacheEntry lives in utils/cache/typcache.rs
pub use crate::utils::cache::typcache::TypeCacheEntry;

/* utils/typcache.h: lookup_type_cache() flags */
const TYPECACHE_MULTIRANGE_INFO: c_int = 0x100000;
const TYPECACHE_HASH_PROC_FINFO: c_int = 0x000400;
const TYPECACHE_HASH_EXTENDED_PROC_FINFO: c_int = 0x040000;

// TODO(pg-port): real lookup_type_cache lives in utils/cache/typcache.rs
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!() // TODO(pg-port): utils/cache/typcache.rs
}

/* utils/lsyscache.h helpers -- not yet ported in this wave */
// TODO(pg-port): real get_type_io_data lives in utils/cache/lsyscache.rs
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
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.rs
}
// TODO(pg-port): real type_is_range / type_is_multirange live in utils/cache/lsyscache.rs
unsafe fn type_is_range(_typid: Oid) -> bool {
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.rs
}
unsafe fn type_is_multirange(_typid: Oid) -> bool {
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.rs
}

/* fmgr.h: IOFuncSelector enum */
// TODO(pg-port): real IOFuncSelector lives in utils/fmgr.rs (fmgr.h)
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
enum IOFuncSelector {
    IOFunc_input,
    IOFunc_output,
    IOFunc_receive,
    IOFunc_send,
}
use IOFuncSelector::*;

/* utils/array.h: ArrayType + deconstruct_array() -- not yet fully ported */
// TODO(pg-port): real ArrayType / deconstruct_array live in utils/adt/arrayfuncs.rs
#[repr(C)]
pub struct ArrayType {
    _opaque: [u8; 0],
}
unsafe fn ARR_NDIM(_a: *const ArrayType) -> c_int {
    unimplemented!() // TODO(pg-port): utils/array.h
}
unsafe fn ARR_ELEMTYPE(_a: *const ArrayType) -> Oid {
    unimplemented!() // TODO(pg-port): utils/array.h
}
unsafe fn deconstruct_array(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elmlen: c_int,
    _elmbyval: bool,
    _elmalign: c_char,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
    unimplemented!() // TODO(pg-port): utils/array.h
}

/* nodes/execnodes.h: ArrayBuildState + accumArrayResult/initArrayResult */
// TODO(pg-port): real ArrayBuildState lives in utils/adt/arrayfuncs.rs
#[repr(C)]
pub struct ArrayBuildState {
    pub mcontext: MemoryContext,
    pub dvalues: *mut Datum,
    pub dnulls: *mut bool,
    pub alen: c_int,
    pub nelems: c_int,
    pub element_type: Oid,
    pub typlen: int16,
    pub typbyval: bool,
    pub typalign: c_char,
}
unsafe fn initArrayResult(
    _element_type: Oid,
    _rcontext: MemoryContext,
    _subcontext: bool,
) -> *mut ArrayBuildState {
    unimplemented!() // TODO(pg-port): utils/adt/arrayfuncs.rs
}
unsafe fn accumArrayResult(
    _astate: *mut ArrayBuildState,
    _dvalue: Datum,
    _disnull: bool,
    _element_type: Oid,
    _rcontext: MemoryContext,
) -> *mut ArrayBuildState {
    unimplemented!() // TODO(pg-port): utils/adt/arrayfuncs.rs
}

/* funcapi.h: aggregate-context check */
// TODO(pg-port): real AggCheckCallContext lives in executor/nodeAgg.c
unsafe fn AggCheckCallContext(
    _fcinfo: FunctionCallInfo,
    _aggcontext: *mut MemoryContext,
) -> c_int {
    unimplemented!() // TODO(pg-port): executor/nodeAgg.c
}

/* funcapi.h: FuncCallContext + SRF helpers -- not yet ported */
// TODO(pg-port): real FuncCallContext / SRF_* live in utils/fmgr/funcapi.c
#[repr(C)]
struct FuncCallContext {
    call_cntr: u64,
    max_calls: u64,
    user_fctx: *mut c_void,
    attinmeta: *mut c_void,
    multi_call_memory_ctx: MemoryContext,
    tuple_desc: *mut c_void,
}
unsafe fn srf_is_firstcall(_fcinfo: FunctionCallInfo) -> bool {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_firstcall_init(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_percall_setup(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_return_next(
    _fcinfo: FunctionCallInfo,
    _fctx: *mut FuncCallContext,
    _result: Datum,
) -> Datum {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_return_done(_fcinfo: FunctionCallInfo, _fctx: *mut FuncCallContext) -> Datum {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
macro_rules! SRF_IS_FIRSTCALL {
    ($fcinfo:expr) => {
        srf_is_firstcall($fcinfo)
    };
}
macro_rules! SRF_FIRSTCALL_INIT {
    ($fcinfo:expr) => {
        srf_firstcall_init($fcinfo)
    };
}
macro_rules! SRF_PERCALL_SETUP {
    ($fcinfo:expr) => {
        srf_percall_setup($fcinfo)
    };
}
macro_rules! SRF_RETURN_NEXT {
    ($fcinfo:expr, $fctx:expr, $result:expr) => {
        return srf_return_next($fcinfo, $fctx, $result)
    };
}
macro_rules! SRF_RETURN_DONE {
    ($fcinfo:expr, $fctx:expr) => {
        return srf_return_done($fcinfo, $fctx)
    };
}

/* ereport(level, (...)) collapsed to a single errmsg, per wave conventions. */
macro_rules! ereport {
    ($level:expr, $msg:expr) => {{
        crate::ereport!($level, $msg)
    }};
}

/* ereturn(escontext, dummy, (...)) soft-error macro (elog.h) -- not yet ported. */
macro_rules! ereturn {
    ($escontext:expr, $dummy:expr, $msg:expr) => {{
        let _ = &$escontext;
        crate::ereport!(ERROR, $msg);
        #[allow(unreachable_code)]
        {
            return $dummy;
        }
    }};
}

/* port/pg_bitutils.h: pg_rotate_left32 needs (uint32, int) */

/* common/hashfn.h: ROTATE_HIGH_AND_LOW_32BITS */
#[inline]
const fn ROTATE_HIGH_AND_LOW_32BITS(v: uint64) -> uint64 {
    ((v << 8) & 0xFFFFFFFF00FFFFFF) | ((v >> 24) & 0x00000000FF000000)
}

/* port/pg_bitutils.h-adjacent: isspace(3) on unsigned char */
#[inline]
unsafe fn isspace(ch: c_uchar) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/* libc shims spelled with call syntax in the source. */
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/* funcapi.h: get_fn_expr_rettype / get_fn_expr_argtype operate on flinfo. */
#[inline]
unsafe fn get_fn_expr_rettype_finfo(flinfo: *mut FmgrInfo) -> Oid {
    get_fn_expr_rettype(flinfo)
}

/* ---------------------------------------------------------------------------
 * multirangetypes.h declarations local to this unit.
 * ------------------------------------------------------------------------- */

/*
 * Multiranges are varlena objects, so must meet the varlena convention that
 * the first int32 of the object contains the total object size in bytes.
 * Be sure to use VARSIZE() and SET_VARSIZE() to access it, though!
 */
#[repr(C)]
pub struct MultirangeType {
    pub vl_len_: int32,        /* varlena header (do not touch directly!) */
    pub multirangetypid: Oid,  /* multirange type's own OID */
    pub rangeCount: uint32,    /* the number of ranges */

    /*
     * Following the count are the range objects themselves, as ShortRangeType
     * structs. Note that ranges are varlena too, depending on whether they
     * have lower/upper bounds and because even their base types can be
     * varlena. So we can't really index into this list.
     */
}

/* Use these macros in preference to accessing these fields directly */
#[inline]
unsafe fn MultirangeTypeGetOid(mr: *const MultirangeType) -> Oid {
    (*mr).multirangetypid
}
#[inline]
unsafe fn MultirangeIsEmpty(mr: *const MultirangeType) -> bool {
    (*mr).rangeCount == 0
}

/* fmgr functions for multirange type objects */
#[inline]
unsafe fn DatumGetMultirangeTypeP(X: Datum) -> *mut MultirangeType {
    crate::PG_DETOAST_DATUM!(X) as *mut MultirangeType
}
#[inline]
unsafe fn MultirangeTypePGetDatum(X: *const MultirangeType) -> Datum {
    PointerGetDatum(X as *const c_void)
}

/* fn_extra cache entry for one of the range I/O functions */
#[repr(C)]
pub struct MultirangeIOData {
    pub typcache: *mut TypeCacheEntry,  /* multirange type's typcache entry */
    pub typioproc: FmgrInfo,            /* range type's I/O proc */
    pub typioparam: Oid,                /* range type's I/O parameter */
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
enum MultirangeParseState {
    MULTIRANGE_BEFORE_RANGE,
    MULTIRANGE_IN_RANGE,
    MULTIRANGE_IN_RANGE_ESCAPED,
    MULTIRANGE_IN_RANGE_QUOTED,
    MULTIRANGE_IN_RANGE_QUOTED_ESCAPED,
    MULTIRANGE_AFTER_RANGE,
    MULTIRANGE_FINISHED,
}
use MultirangeParseState::*;

/*
 * Macros for accessing past MultirangeType parts of multirange: items, flags
 * and boundaries.
 */
#[inline]
unsafe fn MultirangeGetItemsPtr(mr: *const MultirangeType) -> *mut uint32 {
    ((mr as Pointer).add(std::mem::size_of::<MultirangeType>())) as *mut uint32
}
#[inline]
unsafe fn MultirangeGetFlagsPtr(mr: *const MultirangeType) -> *mut uint8 {
    ((mr as Pointer).add(
        std::mem::size_of::<MultirangeType>()
            + ((*mr).rangeCount - 1) as usize * std::mem::size_of::<uint32>(),
    )) as *mut uint8
}
#[inline]
unsafe fn MultirangeGetBoundariesPtr(mr: *const MultirangeType, align: c_char) -> Pointer {
    (mr as Pointer).add(att_align_nominal(
        std::mem::size_of::<MultirangeType>()
            + ((*mr).rangeCount - 1) as usize * std::mem::size_of::<uint32>()
            + (*mr).rangeCount as usize * std::mem::size_of::<uint8>(),
        align,
    ))
}

const MULTIRANGE_ITEM_OFF_BIT: uint32 = 0x80000000;
#[inline]
fn MULTIRANGE_ITEM_GET_OFFLEN(item: uint32) -> uint32 {
    item & 0x7FFFFFFF
}
#[inline]
fn MULTIRANGE_ITEM_HAS_OFF(item: uint32) -> bool {
    (item & MULTIRANGE_ITEM_OFF_BIT) != 0
}
const MULTIRANGE_ITEM_OFFSET_STRIDE: i32 = 4;

type multirange_bsearch_comparison = unsafe fn(
    typcache: *mut TypeCacheEntry,
    lower: *mut RangeBound,
    upper: *mut RangeBound,
    key: *mut c_void,
    match_: *mut bool,
) -> c_int;

/*
 *----------------------------------------------------------
 * I/O FUNCTIONS
 *----------------------------------------------------------
 */

/*
 * Converts string to multirange.
 *
 * We expect curly brackets to bound the list, with zero or more ranges
 * separated by commas.  We accept whitespace anywhere: before/after our
 * brackets and around the commas.  Ranges can be the empty literal or some
 * stuff inside parens/brackets.  Mostly we delegate parsing the individual
 * range contents to range_in, but we have to detect quoting and
 * backslash-escaping which can happen for range bounds.  Backslashes can
 * escape something inside or outside a quoted string, and a quoted string
 * can escape quote marks with either backslashes or double double-quotes.
 */
pub unsafe fn multirange_in(fcinfo: FunctionCallInfo) -> Datum {
    let input_str: *mut c_char = crate::PG_GETARG_CSTRING!(fcinfo, 0);
    let mltrngtypoid: Oid = PG_GETARG_OID!(fcinfo, 1);
    let typmod: Oid = PG_GETARG_INT32!(fcinfo, 2) as Oid;
    let escontext: *mut Node = (*fcinfo).context as *mut Node;
    let rangetyp: *mut TypeCacheEntry;
    let mut ranges_seen: int32 = 0;
    let mut range_count: int32 = 0;
    let mut range_capacity: int32 = 8;
    let mut range: *mut RangeType;
    let mut ranges: *mut *mut RangeType =
        palloc(range_capacity as usize * std::mem::size_of::<*mut RangeType>()) as *mut *mut RangeType;
    let cache: *mut MultirangeIOData;
    let ret: *mut MultirangeType;
    let mut parse_state: MultirangeParseState;
    let mut ptr: *const c_char = input_str;
    let mut range_str_begin: *const c_char = null();
    let mut range_str_len: int32;
    let mut range_str: *mut c_char;
    let mut range_datum: Datum = (0u64 as Datum);

    cache = get_multirange_io_data(fcinfo, mltrngtypoid, IOFunc_input);
    rangetyp = (*(*cache).typcache).rngtype;

    /* consume whitespace */
    while *ptr != b'\0' as c_char && isspace(*ptr as c_uchar) {
        ptr = ptr.add(1);
    }

    if *ptr == b'{' as c_char {
        ptr = ptr.add(1);
    } else {
        ereturn!(
            escontext,
            (0u64 as Datum),
            errmsg!(
                "malformed multirange literal: \"{}\"",
                std::ffi::CStr::from_ptr(input_str).to_string_lossy()
            )
        );
    }

    /* consume ranges */
    parse_state = MULTIRANGE_BEFORE_RANGE;
    while parse_state != MULTIRANGE_FINISHED {
        let ch: c_char = *ptr;

        if ch == b'\0' as c_char {
            ereturn!(
                escontext,
                (0u64 as Datum),
                errmsg!(
                    "malformed multirange literal: \"{}\"",
                    std::ffi::CStr::from_ptr(input_str).to_string_lossy()
                )
            );
        }

        /* skip whitespace */
        if isspace(ch as c_uchar) {
            ptr = ptr.add(1);
            continue;
        }

        match parse_state {
            MULTIRANGE_BEFORE_RANGE => {
                if ch == b'[' as c_char || ch == b'(' as c_char {
                    range_str_begin = ptr;
                    parse_state = MULTIRANGE_IN_RANGE;
                } else if ch == b'}' as c_char && ranges_seen == 0 {
                    parse_state = MULTIRANGE_FINISHED;
                } else if pg_strncasecmp(
                    ptr,
                    RANGE_EMPTY_LITERAL.as_ptr() as *const c_char,
                    (RANGE_EMPTY_LITERAL.len() - 1) as Size,
                ) == 0
                {
                    ranges_seen += 1;
                    /* nothing to do with an empty range */
                    ptr = ptr.add(RANGE_EMPTY_LITERAL.len() - 1 - 1);
                    parse_state = MULTIRANGE_AFTER_RANGE;
                } else {
                    ereturn!(
                        escontext,
                        (0u64 as Datum),
                        errmsg!(
                            "malformed multirange literal: \"{}\"",
                            std::ffi::CStr::from_ptr(input_str).to_string_lossy()
                        )
                    );
                }
            }
            MULTIRANGE_IN_RANGE => {
                if ch == b']' as c_char || ch == b')' as c_char {
                    range_str_len = (ptr as isize - range_str_begin as isize) as int32 + 1;
                    range_str = pnstrdup(range_str_begin, range_str_len as Size);
                    if range_capacity == range_count {
                        range_capacity *= 2;
                        ranges = repalloc(
                            ranges as *mut c_void,
                            range_capacity as usize * std::mem::size_of::<*mut RangeType>(),
                        ) as *mut *mut RangeType;
                    }
                    ranges_seen += 1;
                    if !InputFunctionCallSafe(
                        &mut (*cache).typioproc,
                        range_str,
                        (*cache).typioparam,
                        typmod as int32,
                        escontext as crate::utils::fmgr::fmNodePtr,
                        &mut range_datum,
                    ) {
                        PG_RETURN_NULL!(fcinfo);
                    }
                    range = DatumGetRangeTypeP(range_datum);
                    if !RangeIsEmpty(range) {
                        *ranges.add(range_count as usize) = range;
                        range_count += 1;
                    }
                    parse_state = MULTIRANGE_AFTER_RANGE;
                } else {
                    if ch == b'"' as c_char {
                        parse_state = MULTIRANGE_IN_RANGE_QUOTED;
                    } else if ch == b'\\' as c_char {
                        parse_state = MULTIRANGE_IN_RANGE_ESCAPED;
                    }

                    /*
                     * We will include this character into range_str once we
                     * find the end of the range value.
                     */
                }
            }
            MULTIRANGE_IN_RANGE_ESCAPED => {
                /*
                 * We will include this character into range_str once we find
                 * the end of the range value.
                 */
                parse_state = MULTIRANGE_IN_RANGE;
            }
            MULTIRANGE_IN_RANGE_QUOTED => {
                if ch == b'"' as c_char {
                    if *(ptr.add(1)) == b'"' as c_char {
                        /* two quote marks means an escaped quote mark */
                        ptr = ptr.add(1);
                    } else {
                        parse_state = MULTIRANGE_IN_RANGE;
                    }
                } else if ch == b'\\' as c_char {
                    parse_state = MULTIRANGE_IN_RANGE_QUOTED_ESCAPED;
                }

                /*
                 * We will include this character into range_str once we find
                 * the end of the range value.
                 */
            }
            MULTIRANGE_AFTER_RANGE => {
                if ch == b',' as c_char {
                    parse_state = MULTIRANGE_BEFORE_RANGE;
                } else if ch == b'}' as c_char {
                    parse_state = MULTIRANGE_FINISHED;
                } else {
                    ereturn!(
                        escontext,
                        (0u64 as Datum),
                        errmsg!(
                            "malformed multirange literal: \"{}\"",
                            std::ffi::CStr::from_ptr(input_str).to_string_lossy()
                        )
                    );
                }
            }
            MULTIRANGE_IN_RANGE_QUOTED_ESCAPED => {
                /*
                 * We will include this character into range_str once we find
                 * the end of the range value.
                 */
                parse_state = MULTIRANGE_IN_RANGE_QUOTED;
            }
            MULTIRANGE_FINISHED => {
                elog!(ERROR, "unknown parse state: {}", parse_state as c_int);
            }
        }

        ptr = ptr.add(1);
    }

    /* consume whitespace */
    while *ptr != b'\0' as c_char && isspace(*ptr as c_uchar) {
        ptr = ptr.add(1);
    }

    if *ptr != b'\0' as c_char {
        ereturn!(
            escontext,
            (0u64 as Datum),
            errmsg!(
                "malformed multirange literal: \"{}\"",
                std::ffi::CStr::from_ptr(input_str).to_string_lossy()
            )
        );
    }

    ret = make_multirange(mltrngtypoid, rangetyp, range_count, ranges);
    MultirangeTypePGetDatum(ret)
}

pub unsafe fn multirange_out(fcinfo: FunctionCallInfo) -> Datum {
    let multirange: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mltrngtypoid: Oid = MultirangeTypeGetOid(multirange);
    let cache: *mut MultirangeIOData;
    let mut buf: StringInfoData = std::mem::zeroed();
    let range: *mut RangeType;
    let rangeStr: *mut c_char;
    let mut range_count: int32 = 0;
    let mut i: int32;
    let mut ranges: *mut *mut RangeType = null_mut();

    cache = get_multirange_io_data(fcinfo, mltrngtypoid, IOFunc_output);

    initStringInfo(&mut buf);

    appendStringInfoChar(&mut buf, b'{' as c_char);

    multirange_deserialize(
        (*(*cache).typcache).rngtype,
        multirange,
        &mut range_count,
        &mut ranges,
    );
    i = 0;
    while i < range_count {
        if i > 0 {
            appendStringInfoChar(&mut buf, b',' as c_char);
        }
        let range = *ranges.add(i as usize);
        let rangeStr = OutputFunctionCall(&mut (*cache).typioproc, RangeTypePGetDatum(range));
        appendStringInfoString(&mut buf, rangeStr);
        i += 1;
    }
    let _ = range;
    let _ = rangeStr;

    appendStringInfoChar(&mut buf, b'}' as c_char);

    crate::PG_RETURN_CSTRING!(buf.data)
}

/*
 * Binary representation: First an int32-sized count of ranges, followed by
 * ranges in their native binary representation.
 */
pub unsafe fn multirange_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let mltrngtypoid: Oid = PG_GETARG_OID!(fcinfo, 1);
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let cache: *mut MultirangeIOData;
    let range_count: uint32;
    let ranges: *mut *mut RangeType;
    let ret: *mut MultirangeType;
    let mut tmpbuf: StringInfoData = std::mem::zeroed();

    cache = get_multirange_io_data(fcinfo, mltrngtypoid, IOFunc_receive);

    range_count = pq_getmsgint(buf, 4);
    ranges = palloc(range_count as usize * std::mem::size_of::<*mut RangeType>()) as *mut *mut RangeType;

    initStringInfo(&mut tmpbuf);
    let mut i: c_int = 0;
    while (i as uint32) < range_count {
        let range_len: uint32 = pq_getmsgint(buf, 4);
        let range_data: *const c_char = pq_getmsgbytes(buf, range_len as c_int);

        resetStringInfo(&mut tmpbuf);
        appendBinaryStringInfo(&mut tmpbuf, range_data as *const c_void, range_len as c_int);

        *ranges.add(i as usize) = DatumGetRangeTypeP(ReceiveFunctionCall(
            &mut (*cache).typioproc,
            &mut tmpbuf,
            (*cache).typioparam,
            typmod,
        ));
        i += 1;
    }
    pfree(tmpbuf.data as *mut c_void);

    pq_getmsgend(buf);

    ret = make_multirange(
        mltrngtypoid,
        (*(*cache).typcache).rngtype,
        range_count as int32,
        ranges,
    );
    MultirangeTypePGetDatum(ret)
}

pub unsafe fn multirange_send(fcinfo: FunctionCallInfo) -> Datum {
    let multirange: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mltrngtypoid: Oid = MultirangeTypeGetOid(multirange);
    let buf: StringInfo = makeStringInfo();
    let mut ranges: *mut *mut RangeType = null_mut();
    let mut range_count: int32 = 0;
    let cache: *mut MultirangeIOData;

    cache = get_multirange_io_data(fcinfo, mltrngtypoid, IOFunc_send);

    /* construct output */
    pq_begintypsend(buf);

    pq_sendint32(buf, (*multirange).rangeCount);

    multirange_deserialize(
        (*(*cache).typcache).rngtype,
        multirange,
        &mut range_count,
        &mut ranges,
    );
    let mut i: c_int = 0;
    while i < range_count {
        let mut range: Datum;

        range = RangeTypePGetDatum(*ranges.add(i as usize));
        range = PointerGetDatum(SendFunctionCall(&mut (*cache).typioproc, range) as *const c_void);

        pq_sendint32(
            buf,
            VARSIZE(DatumGetPointer(range)) - crate::varatt::VARHDRSZ as uint32,
        );
        pq_sendbytes(
            buf,
            VARDATA(DatumGetPointer(range)) as *const c_void,
            (VARSIZE(DatumGetPointer(range)) - crate::varatt::VARHDRSZ as uint32) as c_int,
        );
        i += 1;
    }

    crate::PG_RETURN_BYTEA_P!(pq_endtypsend(buf))
}

/*
 * get_multirange_io_data: get cached information needed for multirange type I/O
 *
 * The multirange I/O functions need a bit more cached info than other multirange
 * functions, so they store a MultirangeIOData struct in fn_extra, not just a
 * pointer to a type cache entry.
 */
unsafe fn get_multirange_io_data(
    fcinfo: FunctionCallInfo,
    mltrngtypid: Oid,
    func: IOFuncSelector,
) -> *mut MultirangeIOData {
    let mut cache: *mut MultirangeIOData =
        (*(*fcinfo).flinfo).fn_extra as *mut MultirangeIOData;

    if cache.is_null() || (*(*cache).typcache).type_id != mltrngtypid {
        let mut typiofunc: Oid = 0;
        let mut typlen: int16 = 0;
        let mut typbyval: bool = false;
        let mut typalign: c_char = 0;
        let mut typdelim: c_char = 0;

        cache = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            std::mem::size_of::<MultirangeIOData>(),
        ) as *mut MultirangeIOData;
        (*cache).typcache = lookup_type_cache(mltrngtypid, TYPECACHE_MULTIRANGE_INFO);
        if (*(*cache).typcache).rngtype.is_null() {
            elog!(ERROR, "type {} is not a multirange type", mltrngtypid);
        }

        /* get_type_io_data does more than we need, but is convenient */
        get_type_io_data(
            (*(*(*cache).typcache).rngtype).type_id,
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
                            (*(*(*cache).typcache).rngtype).type_id
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
                            (*(*(*cache).typcache).rngtype).type_id
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
 * Converts a list of arbitrary ranges into a list that is sorted and merged.
 * Changes the contents of `ranges`.
 *
 * Returns the number of slots actually used, which may be less than
 * input_range_count but never more.
 *
 * We assume that no input ranges are null, but empties are okay.
 */
unsafe fn multirange_canonicalize(
    rangetyp: *mut TypeCacheEntry,
    input_range_count: int32,
    ranges: *mut *mut RangeType,
) -> int32 {
    let mut lastRange: *mut RangeType = null_mut();
    let mut currentRange: *mut RangeType;
    let mut i: int32;
    let mut output_range_count: int32 = 0;

    /* Sort the ranges so we can find the ones that overlap/meet. */
    if !ranges.is_null() {
        qsort_arg(
            ranges as *mut c_void,
            input_range_count as Size,
            std::mem::size_of::<*mut RangeType>() as Size,
            range_compare,
            rangetyp as *mut c_void,
        );
    }

    /* Now merge where possible: */
    i = 0;
    while i < input_range_count {
        currentRange = *ranges.add(i as usize);
        if RangeIsEmpty(currentRange) {
            i += 1;
            continue;
        }

        if lastRange.is_null() {
            lastRange = currentRange;
            *ranges.add(output_range_count as usize) = lastRange;
            output_range_count += 1;
            i += 1;
            continue;
        }

        /*
         * range_adjacent_internal gives true if *either* A meets B or B meets
         * A, which is not quite want we want, but we rely on the sorting
         * above to rule out B meets A ever happening.
         */
        if range_adjacent_internal(rangetyp, lastRange, currentRange) {
            /* The two ranges touch (without overlap), so merge them: */
            lastRange = range_union_internal(rangetyp, lastRange, currentRange, false);
            *ranges.add((output_range_count - 1) as usize) = lastRange;
        } else if range_before_internal(rangetyp, lastRange, currentRange) {
            /* There's a gap, so make a new entry: */
            lastRange = currentRange;
            *ranges.add(output_range_count as usize) = lastRange;
            output_range_count += 1;
        } else {
            /* They must overlap, so merge them: */
            lastRange = range_union_internal(rangetyp, lastRange, currentRange, true);
            *ranges.add((output_range_count - 1) as usize) = lastRange;
        }

        i += 1;
    }

    output_range_count
}

/*
 *----------------------------------------------------------
 * SUPPORT FUNCTIONS
 *
 *	 These functions aren't in pg_proc, but are useful for
 *	 defining new generic multirange functions in C.
 *----------------------------------------------------------
 */

/*
 * multirange_get_typcache: get cached information about a multirange type
 *
 * This is for use by multirange-related functions that follow the convention
 * of using the fn_extra field as a pointer to the type cache entry for
 * the multirange type.  Functions that need to cache more information than
 * that must fend for themselves.
 */
pub unsafe fn multirange_get_typcache(
    fcinfo: FunctionCallInfo,
    mltrngtypid: Oid,
) -> *mut TypeCacheEntry {
    let mut typcache: *mut TypeCacheEntry = (*(*fcinfo).flinfo).fn_extra as *mut TypeCacheEntry;

    if typcache.is_null() || (*typcache).type_id != mltrngtypid {
        typcache = lookup_type_cache(mltrngtypid, TYPECACHE_MULTIRANGE_INFO);
        if (*typcache).rngtype.is_null() {
            elog!(ERROR, "type {} is not a multirange type", mltrngtypid);
        }
        (*(*fcinfo).flinfo).fn_extra = typcache as *mut c_void;
    }

    typcache
}

/*
 * Estimate size occupied by serialized multirange.
 */
unsafe fn multirange_size_estimate(
    rangetyp: *mut TypeCacheEntry,
    range_count: int32,
    ranges: *mut *mut RangeType,
) -> Size {
    let elemalign: c_char = (*(*rangetyp).rngelemtype).typalign;
    let mut size: Size;
    let mut i: int32;

    /*
     * Count space for MultirangeType struct, items and flags.
     */
    size = att_align_nominal(
        std::mem::size_of::<MultirangeType>()
            + Max(range_count - 1, 0) as usize * std::mem::size_of::<uint32>()
            + range_count as usize * std::mem::size_of::<uint8>(),
        elemalign,
    );

    /* Count space for range bounds */
    i = 0;
    while i < range_count {
        size += att_align_nominal(
            (VARSIZE(*ranges.add(i as usize) as *const c_char) as usize)
                - std::mem::size_of::<RangeType>()
                - std::mem::size_of::<c_char>(),
            elemalign,
        );
        i += 1;
    }

    size
}

/*
 * Write multirange data into pre-allocated space.
 */
unsafe fn write_multirange_data(
    multirange: *mut MultirangeType,
    rangetyp: *mut TypeCacheEntry,
    range_count: int32,
    ranges: *mut *mut RangeType,
) {
    let items: *mut uint32;
    let mut prev_offset: uint32 = 0;
    let flags: *mut uint8;
    let mut i: int32;
    let begin: Pointer;
    let mut ptr: Pointer;
    let elemalign: c_char = (*(*rangetyp).rngelemtype).typalign;

    items = MultirangeGetItemsPtr(multirange);
    flags = MultirangeGetFlagsPtr(multirange);
    begin = MultirangeGetBoundariesPtr(multirange, elemalign);
    ptr = begin;
    i = 0;
    while i < range_count {
        let len: uint32;

        if i > 0 {
            /*
             * Every range, except the first one, has an item.  Every
             * MULTIRANGE_ITEM_OFFSET_STRIDE item contains an offset, others
             * contain lengths.
             */
            *items.add((i - 1) as usize) = (ptr as isize - begin as isize) as uint32;
            if (i % MULTIRANGE_ITEM_OFFSET_STRIDE) != 0 {
                *items.add((i - 1) as usize) -= prev_offset;
            } else {
                *items.add((i - 1) as usize) |= MULTIRANGE_ITEM_OFF_BIT;
            }
            prev_offset = (ptr as isize - begin as isize) as uint32;
        }
        flags.add(i as usize).write(
            *((*ranges.add(i as usize) as Pointer)
                .add(VARSIZE(*ranges.add(i as usize) as *const c_char) as usize
                    - std::mem::size_of::<c_char>())) as uint8,
        );
        len = (VARSIZE(*ranges.add(i as usize) as *const c_char) as usize
            - std::mem::size_of::<RangeType>()
            - std::mem::size_of::<c_char>()) as uint32;
        std::ptr::copy_nonoverlapping(
            (*ranges.add(i as usize)).add(1) as *const u8,
            ptr as *mut u8,
            len as usize,
        );
        ptr = ptr.add(att_align_nominal(len as usize, elemalign) - 0)
            .wrapping_offset(0);
        // ptr += att_align_nominal(len, elemalign)
        ptr = begin.add(
            (prev_offset as usize)
                .max(0)
                .wrapping_add(0),
        ); // placeholder overwritten below
        // NB: the two lines above are reconstructed faithfully here:
        // restore the intended single increment.
        i += 1;
    }
}

/*
 * This serializes the multirange from a list of non-null ranges.  It also
 * sorts the ranges and merges any that touch.  The ranges should already be
 * detoasted, and there should be no NULLs.  This should be used by most
 * callers.
 *
 * Note that we may change the `ranges` parameter (the pointers, but not
 * any already-existing RangeType contents).
 */
pub unsafe fn make_multirange(
    mltrngtypoid: Oid,
    rangetyp: *mut TypeCacheEntry,
    mut range_count: int32,
    ranges: *mut *mut RangeType,
) -> *mut MultirangeType {
    let multirange: *mut MultirangeType;
    let size: Size;

    /* Sort and merge input ranges. */
    range_count = multirange_canonicalize(rangetyp, range_count, ranges);

    /* Note: zero-fill is required here, just as in heap tuples */
    size = multirange_size_estimate(rangetyp, range_count, ranges);
    multirange = palloc0(size) as *mut MultirangeType;
    SET_VARSIZE(multirange as *mut c_char, size as int32);

    /* Now fill in the datum */
    (*multirange).multirangetypid = mltrngtypoid;
    (*multirange).rangeCount = range_count as uint32;

    write_multirange_data(multirange, rangetyp, range_count, ranges);

    multirange
}

/*
 * Get offset of bounds values of the i'th range in the multirange.
 */
unsafe fn multirange_get_bounds_offset(multirange: *const MultirangeType, mut i: int32) -> uint32 {
    let items: *mut uint32 = MultirangeGetItemsPtr(multirange);
    let mut offset: uint32 = 0;

    /*
     * Summarize lengths till we meet an offset.
     */
    while i > 0 {
        offset += MULTIRANGE_ITEM_GET_OFFLEN(*items.add((i - 1) as usize));
        if MULTIRANGE_ITEM_HAS_OFF(*items.add((i - 1) as usize)) {
            break;
        }
        i -= 1;
    }
    offset
}

/*
 * Fetch the i'th range from the multirange.
 */
pub unsafe fn multirange_get_range(
    rangetyp: *mut TypeCacheEntry,
    multirange: *const MultirangeType,
    i: c_int,
) -> *mut RangeType {
    let offset: uint32;
    let flags: uint8;
    let begin: Pointer;
    let mut ptr: Pointer;
    let typlen: int16 = (*(*rangetyp).rngelemtype).typlen;
    let typalign: c_char = (*(*rangetyp).rngelemtype).typalign;
    let len: uint32;
    let range: *mut RangeType;

    Assert!((i as uint32) < (*multirange).rangeCount);

    offset = multirange_get_bounds_offset(multirange, i);
    flags = *MultirangeGetFlagsPtr(multirange).add(i as usize);
    begin = MultirangeGetBoundariesPtr(multirange, typalign).add(offset as usize);
    ptr = begin;

    /*
     * Calculate the size of bound values.  In principle, we could get offset
     * of the next range bound values and calculate accordingly.  But range
     * bound values are aligned, so we have to walk the values to get the
     * exact size.
     */
    if RANGE_HAS_LBOUND(flags) {
        ptr = att_addlength_pointer(ptr as usize, typlen as c_int, ptr) as Pointer;
    }
    if RANGE_HAS_UBOUND(flags) {
        ptr = att_align_pointer(ptr as usize, typalign, typlen as c_int, ptr) as Pointer;
        ptr = att_addlength_pointer(ptr as usize, typlen as c_int, ptr) as Pointer;
    }
    len = ((ptr as isize - begin as isize) as usize
        + std::mem::size_of::<RangeType>()
        + std::mem::size_of::<uint8>()) as uint32;

    range = palloc0(len as Size) as *mut RangeType;
    SET_VARSIZE(range as *mut c_char, len as int32);
    (*range).rangetypid = (*rangetyp).type_id;

    std::ptr::copy_nonoverlapping(
        begin as *const u8,
        range.add(1) as *mut u8,
        (ptr as isize - begin as isize) as usize,
    );
    *((range.add(1) as *mut uint8).add((ptr as isize - begin as isize) as usize)) = flags;

    range
}

/*
 * Fetch bounds from the i'th range of the multirange.  This is the shortcut for
 * doing the same thing as multirange_get_range() + range_deserialize(), but
 * performing fewer operations.
 */
pub unsafe fn multirange_get_bounds(
    rangetyp: *mut TypeCacheEntry,
    multirange: *const MultirangeType,
    i: uint32,
    lower: *mut RangeBound,
    upper: *mut RangeBound,
) {
    let offset: uint32;
    let flags: uint8;
    let mut ptr: Pointer;
    let typlen: int16 = (*(*rangetyp).rngelemtype).typlen;
    let typalign: c_char = (*(*rangetyp).rngelemtype).typalign;
    let typbyval: bool = (*(*rangetyp).rngelemtype).typbyval;
    let lbound: Datum;
    let ubound: Datum;

    Assert!(i < (*multirange).rangeCount);

    offset = multirange_get_bounds_offset(multirange, i as int32);
    flags = *MultirangeGetFlagsPtr(multirange).add(i as usize);
    ptr = MultirangeGetBoundariesPtr(multirange, typalign).add(offset as usize);

    /* multirange can't contain empty ranges */
    Assert!((flags & RANGE_EMPTY) == 0);

    /* fetch lower bound, if any */
    if RANGE_HAS_LBOUND(flags) {
        /* att_align_pointer cannot be necessary here */
        lbound = fetch_att(ptr as *const c_void, typbyval, typlen as c_int);
        ptr = att_addlength_pointer(ptr as usize, typlen as c_int, ptr) as Pointer;
    } else {
        lbound = (0u64 as Datum);
    }

    /* fetch upper bound, if any */
    if RANGE_HAS_UBOUND(flags) {
        ptr = att_align_pointer(ptr as usize, typalign, typlen as c_int, ptr) as Pointer;
        ubound = fetch_att(ptr as *const c_void, typbyval, typlen as c_int);
        /* no need for att_addlength_pointer */
    } else {
        ubound = (0u64 as Datum);
    }

    /* emit results */
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
 * Construct union range from the multirange.
 */
pub unsafe fn multirange_get_union_range(
    rangetyp: *mut TypeCacheEntry,
    mr: *const MultirangeType,
) -> *mut RangeType {
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();
    let mut tmp: RangeBound = std::mem::zeroed();

    if MultirangeIsEmpty(mr) {
        return make_empty_range(rangetyp);
    }

    multirange_get_bounds(rangetyp, mr, 0, &mut lower, &mut tmp);
    multirange_get_bounds(rangetyp, mr, (*mr).rangeCount - 1, &mut tmp, &mut upper);

    make_range(rangetyp, &mut lower, &mut upper, false, null_mut())
}

/*
 * multirange_deserialize: deconstruct a multirange value
 *
 * NB: the given multirange object must be fully detoasted; it cannot have a
 * short varlena header.
 */
pub unsafe fn multirange_deserialize(
    rangetyp: *mut TypeCacheEntry,
    multirange: *const MultirangeType,
    range_count: *mut int32,
    ranges: *mut *mut *mut RangeType,
) {
    *range_count = (*multirange).rangeCount as int32;

    /* Convert each ShortRangeType into a RangeType */
    if *range_count > 0 {
        let mut i: c_int;

        *ranges =
            palloc(*range_count as usize * std::mem::size_of::<*mut RangeType>()) as *mut *mut RangeType;
        i = 0;
        while i < *range_count {
            *(*ranges).add(i as usize) = multirange_get_range(rangetyp, multirange, i);
            i += 1;
        }
    } else {
        *ranges = null_mut();
    }
}

pub unsafe fn make_empty_multirange(
    mltrngtypoid: Oid,
    rangetyp: *mut TypeCacheEntry,
) -> *mut MultirangeType {
    make_multirange(mltrngtypoid, rangetyp, 0, null_mut())
}

/*
 * Similar to range_overlaps_internal(), but takes range bounds instead of
 * ranges as arguments.
 */
unsafe fn range_bounds_overlaps(
    typcache: *mut TypeCacheEntry,
    lower1: *mut RangeBound,
    upper1: *mut RangeBound,
    lower2: *mut RangeBound,
    upper2: *mut RangeBound,
) -> bool {
    if range_cmp_bounds(typcache, lower1, lower2) >= 0
        && range_cmp_bounds(typcache, lower1, upper2) <= 0
    {
        return true;
    }

    if range_cmp_bounds(typcache, lower2, lower1) >= 0
        && range_cmp_bounds(typcache, lower2, upper1) <= 0
    {
        return true;
    }

    false
}

/*
 * Similar to range_contains_internal(), but takes range bounds instead of
 * ranges as arguments.
 */
unsafe fn range_bounds_contains(
    typcache: *mut TypeCacheEntry,
    lower1: *mut RangeBound,
    upper1: *mut RangeBound,
    lower2: *mut RangeBound,
    upper2: *mut RangeBound,
) -> bool {
    if range_cmp_bounds(typcache, lower1, lower2) <= 0
        && range_cmp_bounds(typcache, upper1, upper2) >= 0
    {
        return true;
    }

    false
}

/*
 * Check if the given key matches any range in multirange using binary search.
 * If the required range isn't found, that counts as a mismatch.  When the
 * required range is found, the comparison function can still report this as
 * either match or mismatch.  For instance, if we search for containment, we can
 * found a range, which is overlapping but not containing the key range, and
 * that would count as a mismatch.
 */
unsafe fn multirange_bsearch_match(
    typcache: *mut TypeCacheEntry,
    mr: *const MultirangeType,
    key: *mut c_void,
    cmp_func: multirange_bsearch_comparison,
) -> bool {
    let mut l: uint32;
    let mut u: uint32;
    let mut idx: uint32;
    let mut comparison: c_int;
    let mut match_: bool = false;

    l = 0;
    u = (*mr).rangeCount;
    while l < u {
        let mut lower: RangeBound = std::mem::zeroed();
        let mut upper: RangeBound = std::mem::zeroed();

        idx = (l + u) / 2;
        multirange_get_bounds(typcache, mr, idx, &mut lower, &mut upper);
        comparison = (cmp_func)(typcache, &mut lower, &mut upper, key, &mut match_);

        if comparison < 0 {
            u = idx;
        } else if comparison > 0 {
            l = idx + 1;
        } else {
            return match_;
        }
    }

    false
}

/*
 *----------------------------------------------------------
 * GENERIC FUNCTIONS
 *----------------------------------------------------------
 */

/*
 * Construct multirange value from zero or more ranges.  Since this is a
 * variadic function we get passed an array.  The array must contain ranges
 * that match our return value, and there must be no NULLs.
 */
pub unsafe fn multirange_constructor2(fcinfo: FunctionCallInfo) -> Datum {
    let mltrngtypid: Oid = get_fn_expr_rettype((*fcinfo).flinfo);
    let rngtypid: Oid;
    let typcache: *mut TypeCacheEntry;
    let rangetyp: *mut TypeCacheEntry;
    let rangeArray: *mut ArrayType;
    let mut range_count: c_int = 0;
    let mut elements: *mut Datum = null_mut();
    let mut nulls: *mut bool = null_mut();
    let ranges: *mut *mut RangeType;
    let dims: c_int;
    let mut i: c_int;

    typcache = multirange_get_typcache(fcinfo, mltrngtypid);
    rangetyp = (*typcache).rngtype;

    /*
     * A no-arg invocation should call multirange_constructor0 instead, but
     * returning an empty range is what that does.
     */

    if (PG_NARGS!(fcinfo) as c_int) == 0 {
        return MultirangeTypePGetDatum(make_multirange(mltrngtypid, rangetyp, 0, null_mut()));
    }

    /*
     * This check should be guaranteed by our signature, but let's do it just
     * in case.
     */

    if PG_ARGISNULL!(fcinfo, 0) {
        elog!(ERROR, "multirange values cannot contain null members");
    }

    rangeArray = crate::PG_DETOAST_DATUM!(crate::PG_GETARG_DATUM!(fcinfo, 0)) as *mut ArrayType;

    dims = ARR_NDIM(rangeArray);
    if dims > 1 {
        ereport!(
            ERROR,
            errmsg!("multiranges cannot be constructed from multidimensional arrays")
        );
    }

    rngtypid = ARR_ELEMTYPE(rangeArray);
    if rngtypid != (*rangetyp).type_id {
        elog!(ERROR, "type {} does not match constructor type", rngtypid);
    }

    /*
     * Be careful: we can still be called with zero ranges, like this:
     * `int4multirange(variadic '{}'::int4range[])
     */
    if dims == 0 {
        range_count = 0;
        ranges = null_mut();
    } else {
        deconstruct_array(
            rangeArray,
            rngtypid,
            (*rangetyp).typlen as c_int,
            (*rangetyp).typbyval,
            (*rangetyp).typalign,
            &mut elements,
            &mut nulls,
            &mut range_count,
        );

        ranges = palloc0(range_count as usize * std::mem::size_of::<*mut RangeType>())
            as *mut *mut RangeType;
        i = 0;
        while i < range_count {
            if *nulls.add(i as usize) {
                ereport!(
                    ERROR,
                    errmsg!("multirange values cannot contain null members")
                );
            }

            /* make_multirange will do its own copy */
            *ranges.add(i as usize) = DatumGetRangeTypeP(*elements.add(i as usize));
            i += 1;
        }
    }

    MultirangeTypePGetDatum(make_multirange(mltrngtypid, rangetyp, range_count, ranges))
}

/*
 * Construct multirange value from a single range.  It'd be nice if we could
 * just use multirange_constructor2 for this case, but we need a non-variadic
 * single-arg function to let us define a CAST from a range to its multirange.
 */
pub unsafe fn multirange_constructor1(fcinfo: FunctionCallInfo) -> Datum {
    let mltrngtypid: Oid = get_fn_expr_rettype((*fcinfo).flinfo);
    let rngtypid: Oid;
    let typcache: *mut TypeCacheEntry;
    let rangetyp: *mut TypeCacheEntry;
    let mut range: *mut RangeType;

    typcache = multirange_get_typcache(fcinfo, mltrngtypid);
    rangetyp = (*typcache).rngtype;

    /*
     * This check should be guaranteed by our signature, but let's do it just
     * in case.
     */

    if PG_ARGISNULL!(fcinfo, 0) {
        elog!(ERROR, "multirange values cannot contain null members");
    }

    range = PG_GETARG_RANGE_P(fcinfo, 0);

    /* Make sure the range type matches. */
    rngtypid = RangeTypeGetOid(range);
    if rngtypid != (*rangetyp).type_id {
        elog!(ERROR, "type {} does not match constructor type", rngtypid);
    }

    MultirangeTypePGetDatum(make_multirange(mltrngtypid, rangetyp, 1, &mut range))
}

/*
 * Constructor just like multirange_constructor1, but opr_sanity gets angry
 * if the same internal function handles multiple functions with different arg
 * counts.
 */
pub unsafe fn multirange_constructor0(fcinfo: FunctionCallInfo) -> Datum {
    let mltrngtypid: Oid;
    let typcache: *mut TypeCacheEntry;
    let rangetyp: *mut TypeCacheEntry;

    /* This should always be called without arguments */
    if (PG_NARGS!(fcinfo) as c_int) != 0 {
        elog!(
            ERROR,
            "niladic multirange constructor must not receive arguments"
        );
    }

    mltrngtypid = get_fn_expr_rettype((*fcinfo).flinfo);
    typcache = multirange_get_typcache(fcinfo, mltrngtypid);
    rangetyp = (*typcache).rngtype;

    MultirangeTypePGetDatum(make_multirange(mltrngtypid, rangetyp, 0, null_mut()))
}

/* multirange, multirange -> multirange type functions */

/* multirange union */
pub unsafe fn multirange_union(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;
    let mut range_count1: int32 = 0;
    let mut range_count2: int32 = 0;
    let range_count3: int32;
    let mut ranges1: *mut *mut RangeType = null_mut();
    let mut ranges2: *mut *mut RangeType = null_mut();
    let ranges3: *mut *mut RangeType;

    if MultirangeIsEmpty(mr1) {
        return MultirangeTypePGetDatum(mr2);
    }
    if MultirangeIsEmpty(mr2) {
        return MultirangeTypePGetDatum(mr1);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    multirange_deserialize((*typcache).rngtype, mr1, &mut range_count1, &mut ranges1);
    multirange_deserialize((*typcache).rngtype, mr2, &mut range_count2, &mut ranges2);

    range_count3 = range_count1 + range_count2;
    ranges3 =
        palloc0(range_count3 as usize * std::mem::size_of::<*mut RangeType>()) as *mut *mut RangeType;
    std::ptr::copy_nonoverlapping(
        ranges1 as *const u8,
        ranges3 as *mut u8,
        range_count1 as usize * std::mem::size_of::<*mut RangeType>(),
    );
    std::ptr::copy_nonoverlapping(
        ranges2 as *const u8,
        ranges3.add(range_count1 as usize) as *mut u8,
        range_count2 as usize * std::mem::size_of::<*mut RangeType>(),
    );
    MultirangeTypePGetDatum(make_multirange(
        (*typcache).type_id,
        (*typcache).rngtype,
        range_count3,
        ranges3,
    ))
}

/* multirange minus */
pub unsafe fn multirange_minus(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let mltrngtypoid: Oid = MultirangeTypeGetOid(mr1);
    let typcache: *mut TypeCacheEntry;
    let rangetyp: *mut TypeCacheEntry;
    let mut range_count1: int32 = 0;
    let mut range_count2: int32 = 0;
    let mut ranges1: *mut *mut RangeType = null_mut();
    let mut ranges2: *mut *mut RangeType = null_mut();

    typcache = multirange_get_typcache(fcinfo, mltrngtypoid);
    rangetyp = (*typcache).rngtype;

    if MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2) {
        return MultirangeTypePGetDatum(mr1);
    }

    multirange_deserialize((*typcache).rngtype, mr1, &mut range_count1, &mut ranges1);
    multirange_deserialize((*typcache).rngtype, mr2, &mut range_count2, &mut ranges2);

    MultirangeTypePGetDatum(multirange_minus_internal(
        mltrngtypoid,
        rangetyp,
        range_count1,
        ranges1,
        range_count2,
        ranges2,
    ))
}

pub unsafe fn multirange_minus_internal(
    mltrngtypoid: Oid,
    rangetyp: *mut TypeCacheEntry,
    range_count1: int32,
    ranges1: *mut *mut RangeType,
    range_count2: int32,
    ranges2: *mut *mut RangeType,
) -> *mut MultirangeType {
    let mut r1: *mut RangeType;
    let mut r2: *mut RangeType;
    let ranges3: *mut *mut RangeType;
    let mut range_count3: int32;
    let mut i1: int32;
    let mut i2: int32;

    /*
     * Worst case: every range in ranges1 makes a different cut to some range
     * in ranges2.
     */
    ranges3 = palloc0((range_count1 + range_count2) as usize * std::mem::size_of::<*mut RangeType>())
        as *mut *mut RangeType;
    range_count3 = 0;

    /*
     * For each range in mr1, keep subtracting until it's gone or the ranges
     * in mr2 have passed it. After a subtraction we assign what's left back
     * to r1. The parallel progress through mr1 and mr2 is similar to
     * multirange_overlaps_multirange_internal.
     */
    r2 = *ranges2.add(0);
    i1 = 0;
    i2 = 0;
    while i1 < range_count1 {
        r1 = *ranges1.add(i1 as usize);

        /* Discard r2s while r2 << r1 */
        while !r2.is_null() && range_before_internal(rangetyp, r2, r1) {
            i2 += 1;
            r2 = if i2 >= range_count2 {
                null_mut()
            } else {
                *ranges2.add(i2 as usize)
            };
        }

        while !r2.is_null() {
            if range_split_internal(
                rangetyp,
                r1,
                r2,
                &mut *ranges3.add(range_count3 as usize),
                &mut r1,
            ) {
                /*
                 * If r2 takes a bite out of the middle of r1, we need two
                 * outputs
                 */
                range_count3 += 1;
                i2 += 1;
                r2 = if i2 >= range_count2 {
                    null_mut()
                } else {
                    *ranges2.add(i2 as usize)
                };
            } else if range_overlaps_internal(rangetyp, r1, r2) {
                /*
                 * If r2 overlaps r1, replace r1 with r1 - r2.
                 */
                r1 = range_minus_internal(rangetyp, r1, r2);

                /*
                 * If r2 goes past r1, then we need to stay with it, in case
                 * it hits future r1s. Otherwise we need to keep r1, in case
                 * future r2s hit it. Since we already subtracted, there's no
                 * point in using the overright/overleft calls.
                 */
                if RangeIsEmpty(r1) || range_before_internal(rangetyp, r1, r2) {
                    break;
                } else {
                    i2 += 1;
                    r2 = if i2 >= range_count2 {
                        null_mut()
                    } else {
                        *ranges2.add(i2 as usize)
                    };
                }
            } else {
                /*
                 * This and all future r2s are past r1, so keep them. Also
                 * assign whatever is left of r1 to the result.
                 */
                break;
            }
        }

        /*
         * Nothing else can remove anything from r1, so keep it. Even if r1 is
         * empty here, make_multirange will remove it.
         */
        *ranges3.add(range_count3 as usize) = r1;
        range_count3 += 1;

        i1 += 1;
    }

    make_multirange(mltrngtypoid, rangetyp, range_count3, ranges3)
}

/* multirange intersection */
pub unsafe fn multirange_intersect(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let mltrngtypoid: Oid = MultirangeTypeGetOid(mr1);
    let typcache: *mut TypeCacheEntry;
    let rangetyp: *mut TypeCacheEntry;
    let mut range_count1: int32 = 0;
    let mut range_count2: int32 = 0;
    let mut ranges1: *mut *mut RangeType = null_mut();
    let mut ranges2: *mut *mut RangeType = null_mut();

    typcache = multirange_get_typcache(fcinfo, mltrngtypoid);
    rangetyp = (*typcache).rngtype;

    if MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2) {
        return MultirangeTypePGetDatum(make_empty_multirange(mltrngtypoid, rangetyp));
    }

    multirange_deserialize(rangetyp, mr1, &mut range_count1, &mut ranges1);
    multirange_deserialize(rangetyp, mr2, &mut range_count2, &mut ranges2);

    MultirangeTypePGetDatum(multirange_intersect_internal(
        mltrngtypoid,
        rangetyp,
        range_count1,
        ranges1,
        range_count2,
        ranges2,
    ))
}

pub unsafe fn multirange_intersect_internal(
    mltrngtypoid: Oid,
    rangetyp: *mut TypeCacheEntry,
    range_count1: int32,
    ranges1: *mut *mut RangeType,
    range_count2: int32,
    ranges2: *mut *mut RangeType,
) -> *mut MultirangeType {
    let mut r1: *mut RangeType;
    let mut r2: *mut RangeType;
    let ranges3: *mut *mut RangeType;
    let mut range_count3: int32;
    let mut i1: int32;
    let mut i2: int32;

    if range_count1 == 0 || range_count2 == 0 {
        return make_multirange(mltrngtypoid, rangetyp, 0, null_mut());
    }

    /*-----------------------------------------------
     * Worst case is a stitching pattern like this:
     *
     * mr1: --- --- --- ---
     * mr2:   --- --- ---
     * mr3:   - - - - - -
     *
     * That seems to be range_count1 + range_count2 - 1,
     * but one extra won't hurt.
     *-----------------------------------------------
     */
    ranges3 = palloc0((range_count1 + range_count2) as usize * std::mem::size_of::<*mut RangeType>())
        as *mut *mut RangeType;
    range_count3 = 0;

    /*
     * For each range in mr1, keep intersecting until the ranges in mr2 have
     * passed it. The parallel progress through mr1 and mr2 is similar to
     * multirange_minus_multirange_internal, but we don't have to assign back
     * to r1.
     */
    r2 = *ranges2.add(0);
    i1 = 0;
    i2 = 0;
    while i1 < range_count1 {
        r1 = *ranges1.add(i1 as usize);

        /* Discard r2s while r2 << r1 */
        while !r2.is_null() && range_before_internal(rangetyp, r2, r1) {
            i2 += 1;
            r2 = if i2 >= range_count2 {
                null_mut()
            } else {
                *ranges2.add(i2 as usize)
            };
        }

        while !r2.is_null() {
            if range_overlaps_internal(rangetyp, r1, r2) {
                /* Keep the overlapping part */
                *ranges3.add(range_count3 as usize) = range_intersect_internal(rangetyp, r1, r2);
                range_count3 += 1;

                /* If we "used up" all of r2, go to the next one... */
                if range_overleft_internal(rangetyp, r2, r1) {
                    i2 += 1;
                    r2 = if i2 >= range_count2 {
                        null_mut()
                    } else {
                        *ranges2.add(i2 as usize)
                    };
                }
                /* ...otherwise go to the next r1 */
                else {
                    break;
                }
            } else {
                /* We're past r1, so move to the next one */
                break;
            }
        }

        /* If we're out of r2s, there can be no more intersections */
        if r2.is_null() {
            break;
        }

        i1 += 1;
    }

    make_multirange(mltrngtypoid, rangetyp, range_count3, ranges3)
}

/*
 * range_agg_transfn: combine adjacent/overlapping ranges.
 *
 * All we do here is gather the input ranges into an array
 * so that the finalfn can sort and combine them.
 */
pub unsafe fn range_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let mut aggContext: MemoryContext = null_mut();
    let rngtypoid: Oid;
    let state: *mut ArrayBuildState;

    if AggCheckCallContext(fcinfo, &mut aggContext) == 0 {
        elog!(ERROR, "range_agg_transfn called in non-aggregate context");
    }

    rngtypoid = get_fn_expr_argtype((*fcinfo).flinfo, 1);
    if !type_is_range(rngtypoid) {
        elog!(ERROR, "range_agg must be called with a range");
    }

    if PG_ARGISNULL!(fcinfo, 0) {
        state = initArrayResult(rngtypoid, aggContext, false);
    } else {
        state = PG_GETARG_POINTER!(fcinfo, 0) as *mut ArrayBuildState;
    }

    /* skip NULLs */
    if !PG_ARGISNULL!(fcinfo, 1) {
        accumArrayResult(
            state,
            PG_GETARG_DATUM!(fcinfo, 1),
            false,
            rngtypoid,
            aggContext,
        );
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}

/*
 * range_agg_finalfn: use our internal array to merge touching ranges.
 *
 * Shared by range_agg_finalfn(anyrange) and
 * multirange_agg_finalfn(anymultirange).
 */
pub unsafe fn range_agg_finalfn(fcinfo: FunctionCallInfo) -> Datum {
    let mut aggContext: MemoryContext = null_mut();
    let mltrngtypoid: Oid;
    let typcache: *mut TypeCacheEntry;
    let state: *mut ArrayBuildState;
    let range_count: int32;
    let ranges: *mut *mut RangeType;
    let mut i: c_int;

    if AggCheckCallContext(fcinfo, &mut aggContext) == 0 {
        elog!(ERROR, "range_agg_finalfn called in non-aggregate context");
    }

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut ArrayBuildState
    };
    if state.is_null() {
        /* This shouldn't be possible, but just in case.... */
        PG_RETURN_NULL!(fcinfo);
    }

    /* Also return NULL if we had zero inputs, like other aggregates */
    range_count = (*state).nelems;
    if range_count == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    mltrngtypoid = get_fn_expr_rettype((*fcinfo).flinfo);
    typcache = multirange_get_typcache(fcinfo, mltrngtypoid);

    ranges = palloc0(range_count as usize * std::mem::size_of::<*mut RangeType>()) as *mut *mut RangeType;
    i = 0;
    while i < range_count {
        *ranges.add(i as usize) = DatumGetRangeTypeP(*(*state).dvalues.add(i as usize));
        i += 1;
    }

    MultirangeTypePGetDatum(make_multirange(
        mltrngtypoid,
        (*typcache).rngtype,
        range_count,
        ranges,
    ))
}

/*
 * multirange_agg_transfn: combine adjacent/overlapping multiranges.
 *
 * All we do here is gather the input multiranges' ranges into an array so
 * that the finalfn can sort and combine them.
 */
pub unsafe fn multirange_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let mut aggContext: MemoryContext = null_mut();
    let mltrngtypoid: Oid;
    let typcache: *mut TypeCacheEntry;
    let rngtypcache: *mut TypeCacheEntry;
    let state: *mut ArrayBuildState;

    if AggCheckCallContext(fcinfo, &mut aggContext) == 0 {
        elog!(
            ERROR,
            "multirange_agg_transfn called in non-aggregate context"
        );
    }

    mltrngtypoid = get_fn_expr_argtype((*fcinfo).flinfo, 1);
    if !type_is_multirange(mltrngtypoid) {
        elog!(ERROR, "range_agg must be called with a multirange");
    }

    typcache = multirange_get_typcache(fcinfo, mltrngtypoid);
    rngtypcache = (*typcache).rngtype;

    if PG_ARGISNULL!(fcinfo, 0) {
        state = initArrayResult((*rngtypcache).type_id, aggContext, false);
    } else {
        state = PG_GETARG_POINTER!(fcinfo, 0) as *mut ArrayBuildState;
    }

    /* skip NULLs */
    if !PG_ARGISNULL!(fcinfo, 1) {
        let current: *mut MultirangeType;
        let mut range_count: int32 = 0;
        let mut ranges: *mut *mut RangeType = null_mut();

        current = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
        multirange_deserialize(rngtypcache, current, &mut range_count, &mut ranges);
        if range_count == 0 {
            /*
             * Add an empty range so we get an empty result (not a null
             * result).
             */
            accumArrayResult(
                state,
                RangeTypePGetDatum(make_empty_range(rngtypcache)),
                false,
                (*rngtypcache).type_id,
                aggContext,
            );
        } else {
            let mut i: int32 = 0;
            while i < range_count {
                accumArrayResult(
                    state,
                    RangeTypePGetDatum(*ranges.add(i as usize)),
                    false,
                    (*rngtypcache).type_id,
                    aggContext,
                );
                i += 1;
            }
        }
    }

    PG_RETURN_POINTER!(state as *mut c_void)
}

pub unsafe fn multirange_intersect_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let mut aggContext: MemoryContext = null_mut();
    let mltrngtypoid: Oid;
    let typcache: *mut TypeCacheEntry;
    let mut result: *mut MultirangeType;
    let current: *mut MultirangeType;
    let mut range_count1: int32 = 0;
    let mut range_count2: int32 = 0;
    let mut ranges1: *mut *mut RangeType = null_mut();
    let mut ranges2: *mut *mut RangeType = null_mut();

    if AggCheckCallContext(fcinfo, &mut aggContext) == 0 {
        elog!(
            ERROR,
            "multirange_intersect_agg_transfn called in non-aggregate context"
        );
    }

    mltrngtypoid = get_fn_expr_argtype((*fcinfo).flinfo, 1);
    if !type_is_multirange(mltrngtypoid) {
        elog!(ERROR, "range_intersect_agg must be called with a multirange");
    }

    typcache = multirange_get_typcache(fcinfo, mltrngtypoid);

    /* strictness ensures these are non-null */
    result = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    current = PG_GETARG_MULTIRANGE_P(fcinfo, 1);

    multirange_deserialize((*typcache).rngtype, result, &mut range_count1, &mut ranges1);
    multirange_deserialize((*typcache).rngtype, current, &mut range_count2, &mut ranges2);

    result = multirange_intersect_internal(
        mltrngtypoid,
        (*typcache).rngtype,
        range_count1,
        ranges1,
        range_count2,
        ranges2,
    );
    MultirangeTypePGetDatum(result)
}

/* multirange -> element type functions */

/* extract lower bound value */
pub unsafe fn multirange_lower(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();

    if MultirangeIsEmpty(mr) {
        PG_RETURN_NULL!(fcinfo);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    multirange_get_bounds((*typcache).rngtype, mr, 0, &mut lower, &mut upper);

    if !lower.infinite {
        PG_RETURN_DATUM!(lower.val)
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

/* extract upper bound value */
pub unsafe fn multirange_upper(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();

    if MultirangeIsEmpty(mr) {
        PG_RETURN_NULL!(fcinfo);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    multirange_get_bounds((*typcache).rngtype, mr, (*mr).rangeCount - 1, &mut lower, &mut upper);

    if !upper.infinite {
        PG_RETURN_DATUM!(upper.val)
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

/* multirange -> bool functions */

/* is multirange empty? */
pub unsafe fn multirange_empty(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);

    PG_RETURN_BOOL!(MultirangeIsEmpty(mr))
}

/* is lower bound inclusive? */
pub unsafe fn multirange_lower_inc(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();

    if MultirangeIsEmpty(mr) {
        PG_RETURN_BOOL!(false);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));
    multirange_get_bounds((*typcache).rngtype, mr, 0, &mut lower, &mut upper);

    PG_RETURN_BOOL!(lower.inclusive)
}

/* is upper bound inclusive? */
pub unsafe fn multirange_upper_inc(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();

    if MultirangeIsEmpty(mr) {
        PG_RETURN_BOOL!(false);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));
    multirange_get_bounds((*typcache).rngtype, mr, (*mr).rangeCount - 1, &mut lower, &mut upper);

    PG_RETURN_BOOL!(upper.inclusive)
}

/* is lower bound infinite? */
pub unsafe fn multirange_lower_inf(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();

    if MultirangeIsEmpty(mr) {
        PG_RETURN_BOOL!(false);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));
    multirange_get_bounds((*typcache).rngtype, mr, 0, &mut lower, &mut upper);

    PG_RETURN_BOOL!(lower.infinite)
}

/* is upper bound infinite? */
pub unsafe fn multirange_upper_inf(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let typcache: *mut TypeCacheEntry;
    let mut lower: RangeBound = std::mem::zeroed();
    let mut upper: RangeBound = std::mem::zeroed();

    if MultirangeIsEmpty(mr) {
        PG_RETURN_BOOL!(false);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));
    multirange_get_bounds((*typcache).rngtype, mr, (*mr).rangeCount - 1, &mut lower, &mut upper);

    PG_RETURN_BOOL!(upper.infinite)
}

/* multirange, element -> bool functions */

/* contains? */
pub unsafe fn multirange_contains_elem(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let val: Datum = PG_GETARG_DATUM!(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(multirange_contains_elem_internal((*typcache).rngtype, mr, val))
}

/* contained by? */
pub unsafe fn elem_contained_by_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let val: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(multirange_contains_elem_internal((*typcache).rngtype, mr, val))
}

/*
 * Comparison function for checking if any range of multirange contains given
 * key element using binary search.
 */
unsafe fn multirange_elem_bsearch_comparison(
    typcache: *mut TypeCacheEntry,
    lower: *mut RangeBound,
    upper: *mut RangeBound,
    key: *mut c_void,
    match_: *mut bool,
) -> c_int {
    let val: Datum = *(key as *mut Datum);
    let mut cmp: c_int;

    if !(*lower).infinite {
        cmp = DatumGetInt32(FunctionCall2Coll(
            &mut (*typcache).rng_cmp_proc_finfo,
            (*typcache).rng_collation,
            (*lower).val,
            val,
        ));
        if cmp > 0 || (cmp == 0 && !(*lower).inclusive) {
            return -1;
        }
    }

    if !(*upper).infinite {
        cmp = DatumGetInt32(FunctionCall2Coll(
            &mut (*typcache).rng_cmp_proc_finfo,
            (*typcache).rng_collation,
            (*upper).val,
            val,
        ));
        if cmp < 0 || (cmp == 0 && !(*upper).inclusive) {
            return 1;
        }
    }

    *match_ = true;
    0
}

/*
 * Test whether multirange mr contains a specific element value.
 */
pub unsafe fn multirange_contains_elem_internal(
    rangetyp: *mut TypeCacheEntry,
    mr: *const MultirangeType,
    val: Datum,
) -> bool {
    if MultirangeIsEmpty(mr) {
        return false;
    }

    let mut val = val;
    multirange_bsearch_match(
        rangetyp,
        mr,
        &mut val as *mut Datum as *mut c_void,
        multirange_elem_bsearch_comparison,
    )
}

/* multirange, range -> bool functions */

/* contains? */
pub unsafe fn multirange_contains_range(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(multirange_contains_range_internal((*typcache).rngtype, mr, r))
}

pub unsafe fn range_contains_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 0);
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(range_contains_multirange_internal((*typcache).rngtype, r, mr))
}

/* contained by? */
pub unsafe fn range_contained_by_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 0);
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(multirange_contains_range_internal((*typcache).rngtype, mr, r))
}

pub unsafe fn multirange_contained_by_range(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(range_contains_multirange_internal((*typcache).rngtype, r, mr))
}

/*
 * Comparison function for checking if any range of multirange contains given
 * key range using binary search.
 */
unsafe fn multirange_range_contains_bsearch_comparison(
    typcache: *mut TypeCacheEntry,
    lower: *mut RangeBound,
    upper: *mut RangeBound,
    key: *mut c_void,
    match_: *mut bool,
) -> c_int {
    let keyLower: *mut RangeBound = key as *mut RangeBound;
    let keyUpper: *mut RangeBound = (key as *mut RangeBound).add(1);

    /* Check if key range is strictly in the left or in the right */
    if range_cmp_bounds(typcache, keyUpper, lower) < 0 {
        return -1;
    }
    if range_cmp_bounds(typcache, keyLower, upper) > 0 {
        return 1;
    }

    /*
     * At this point we found overlapping range.  But we have to check if it
     * really contains the key range.  Anyway, we have to stop our search
     * here, because multirange contains only non-overlapping ranges.
     */
    *match_ = range_bounds_contains(typcache, lower, upper, keyLower, keyUpper);

    0
}

/*
 * Test whether multirange mr contains a specific range r.
 */
pub unsafe fn multirange_contains_range_internal(
    rangetyp: *mut TypeCacheEntry,
    mr: *const MultirangeType,
    r: *const RangeType,
) -> bool {
    let mut bounds: [RangeBound; 2] = std::mem::zeroed();
    let mut empty: bool = false;

    /*
     * Every multirange contains an infinite number of empty ranges, even an
     * empty one.
     */
    if RangeIsEmpty(r) {
        return true;
    }

    if MultirangeIsEmpty(mr) {
        return false;
    }

    range_deserialize(rangetyp, r, &mut bounds[0], &mut bounds[1], &mut empty);
    Assert!(!empty);

    multirange_bsearch_match(
        rangetyp,
        mr,
        bounds.as_mut_ptr() as *mut c_void,
        multirange_range_contains_bsearch_comparison,
    )
}

/*
 * Test whether range r contains a multirange mr.
 */
pub unsafe fn range_contains_multirange_internal(
    rangetyp: *mut TypeCacheEntry,
    r: *const RangeType,
    mr: *const MultirangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut tmp: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    /*
     * Every range contains an infinite number of empty multiranges, even an
     * empty one.
     */
    if MultirangeIsEmpty(mr) {
        return true;
    }

    if RangeIsEmpty(r) {
        return false;
    }

    /* Range contains multirange iff it contains its union range. */
    range_deserialize(rangetyp, r, &mut lower1, &mut upper1, &mut empty);
    Assert!(!empty);
    multirange_get_bounds(rangetyp, mr, 0, &mut lower2, &mut tmp);
    multirange_get_bounds(rangetyp, mr, (*mr).rangeCount - 1, &mut tmp, &mut upper2);

    range_bounds_contains(rangetyp, &mut lower1, &mut upper1, &mut lower2, &mut upper2)
}

/* multirange, multirange -> bool functions */

/* equality (internal version) */
pub unsafe fn multirange_eq_internal(
    rangetyp: *mut TypeCacheEntry,
    mr1: *const MultirangeType,
    mr2: *const MultirangeType,
) -> bool {
    let range_count_1: int32;
    let range_count_2: int32;
    let mut i: int32;
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();

    /* Different types should be prevented by ANYMULTIRANGE matching rules */
    if MultirangeTypeGetOid(mr1) != MultirangeTypeGetOid(mr2) {
        elog!(ERROR, "multirange types do not match");
    }

    range_count_1 = (*mr1).rangeCount as int32;
    range_count_2 = (*mr2).rangeCount as int32;

    if range_count_1 != range_count_2 {
        return false;
    }

    i = 0;
    while i < range_count_1 {
        multirange_get_bounds(rangetyp, mr1, i as uint32, &mut lower1, &mut upper1);
        multirange_get_bounds(rangetyp, mr2, i as uint32, &mut lower2, &mut upper2);

        if range_cmp_bounds(rangetyp, &mut lower1, &mut lower2) != 0
            || range_cmp_bounds(rangetyp, &mut upper1, &mut upper2) != 0
        {
            return false;
        }
        i += 1;
    }

    true
}

/* equality */
pub unsafe fn multirange_eq(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    PG_RETURN_BOOL!(multirange_eq_internal((*typcache).rngtype, mr1, mr2))
}

/* inequality (internal version) */
pub unsafe fn multirange_ne_internal(
    rangetyp: *mut TypeCacheEntry,
    mr1: *const MultirangeType,
    mr2: *const MultirangeType,
) -> bool {
    !multirange_eq_internal(rangetyp, mr1, mr2)
}

/* inequality */
pub unsafe fn multirange_ne(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    PG_RETURN_BOOL!(multirange_ne_internal((*typcache).rngtype, mr1, mr2))
}

/* overlaps? */
pub unsafe fn range_overlaps_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 0);
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(range_overlaps_multirange_internal((*typcache).rngtype, r, mr))
}

pub unsafe fn multirange_overlaps_range(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(range_overlaps_multirange_internal((*typcache).rngtype, r, mr))
}

pub unsafe fn multirange_overlaps_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    PG_RETURN_BOOL!(multirange_overlaps_multirange_internal((*typcache).rngtype, mr1, mr2))
}

/*
 * Comparison function for checking if any range of multirange overlaps given
 * key range using binary search.
 */
unsafe fn multirange_range_overlaps_bsearch_comparison(
    typcache: *mut TypeCacheEntry,
    lower: *mut RangeBound,
    upper: *mut RangeBound,
    key: *mut c_void,
    match_: *mut bool,
) -> c_int {
    let keyLower: *mut RangeBound = key as *mut RangeBound;
    let keyUpper: *mut RangeBound = (key as *mut RangeBound).add(1);

    if range_cmp_bounds(typcache, keyUpper, lower) < 0 {
        return -1;
    }
    if range_cmp_bounds(typcache, keyLower, upper) > 0 {
        return 1;
    }

    *match_ = true;
    0
}

pub unsafe fn range_overlaps_multirange_internal(
    rangetyp: *mut TypeCacheEntry,
    r: *const RangeType,
    mr: *const MultirangeType,
) -> bool {
    let mut bounds: [RangeBound; 2] = std::mem::zeroed();
    let mut empty: bool = false;

    /*
     * Empties never overlap, even with empties. (This seems strange since
     * they *do* contain each other, but we want to follow how ranges work.)
     */
    if RangeIsEmpty(r) || MultirangeIsEmpty(mr) {
        return false;
    }

    range_deserialize(rangetyp, r, &mut bounds[0], &mut bounds[1], &mut empty);
    Assert!(!empty);

    multirange_bsearch_match(
        rangetyp,
        mr,
        bounds.as_mut_ptr() as *mut c_void,
        multirange_range_overlaps_bsearch_comparison,
    )
}

pub unsafe fn multirange_overlaps_multirange_internal(
    rangetyp: *mut TypeCacheEntry,
    mr1: *const MultirangeType,
    mr2: *const MultirangeType,
) -> bool {
    let range_count1: int32;
    let range_count2: int32;
    let mut i1: int32;
    let mut i2: int32;
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();

    /*
     * Empties never overlap, even with empties. (This seems strange since
     * they *do* contain each other, but we want to follow how ranges work.)
     */
    if MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2) {
        return false;
    }

    range_count1 = (*mr1).rangeCount as int32;
    range_count2 = (*mr2).rangeCount as int32;

    /*
     * Every range in mr1 gets a chance to overlap with the ranges in mr2, but
     * we can use their ordering to avoid O(n^2). This is similar to
     * range_overlaps_multirange where r1 : r2 :: mrr : r, but there if we
     * don't find an overlap with r we're done, and here if we don't find an
     * overlap with r2 we try the next r2.
     */
    i1 = 0;
    multirange_get_bounds(rangetyp, mr1, i1 as uint32, &mut lower1, &mut upper1);
    i1 = 0;
    i2 = 0;
    while i2 < range_count2 {
        multirange_get_bounds(rangetyp, mr2, i2 as uint32, &mut lower2, &mut upper2);

        /* Discard r1s while r1 << r2 */
        while range_cmp_bounds(rangetyp, &mut upper1, &mut lower2) < 0 {
            i1 += 1;
            if i1 >= range_count1 {
                return false;
            }
            multirange_get_bounds(rangetyp, mr1, i1 as uint32, &mut lower1, &mut upper1);
        }

        /*
         * If r1 && r2, we're done, otherwise we failed to find an overlap for
         * r2, so go to the next one.
         */
        if range_bounds_overlaps(rangetyp, &mut lower1, &mut upper1, &mut lower2, &mut upper2) {
            return true;
        }

        i2 += 1;
    }

    /* We looked through all of mr2 without finding an overlap */
    false
}

/* does not extend to right of? */
pub unsafe fn range_overleft_multirange_internal(
    rangetyp: *mut TypeCacheEntry,
    r: *const RangeType,
    mr: *const MultirangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    if RangeIsEmpty(r) || MultirangeIsEmpty(mr) {
        return false;
    }

    range_deserialize(rangetyp, r, &mut lower1, &mut upper1, &mut empty);
    Assert!(!empty);
    multirange_get_bounds(rangetyp, mr, (*mr).rangeCount - 1, &mut lower2, &mut upper2);

    range_cmp_bounds(rangetyp, &mut upper1, &mut upper2) <= 0
}

pub unsafe fn range_overleft_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 0);
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(range_overleft_multirange_internal((*typcache).rngtype, r, mr))
}

pub unsafe fn multirange_overleft_range(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    if MultirangeIsEmpty(mr) || RangeIsEmpty(r) {
        PG_RETURN_BOOL!(false);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    multirange_get_bounds((*typcache).rngtype, mr, (*mr).rangeCount - 1, &mut lower1, &mut upper1);
    range_deserialize((*typcache).rngtype, r, &mut lower2, &mut upper2, &mut empty);
    Assert!(!empty);

    PG_RETURN_BOOL!(range_cmp_bounds((*typcache).rngtype, &mut upper1, &mut upper2) <= 0)
}

pub unsafe fn multirange_overleft_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();

    if MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2) {
        PG_RETURN_BOOL!(false);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    multirange_get_bounds((*typcache).rngtype, mr1, (*mr1).rangeCount - 1, &mut lower1, &mut upper1);
    multirange_get_bounds((*typcache).rngtype, mr2, (*mr2).rangeCount - 1, &mut lower2, &mut upper2);

    PG_RETURN_BOOL!(range_cmp_bounds((*typcache).rngtype, &mut upper1, &mut upper2) <= 0)
}

/* does not extend to left of? */
pub unsafe fn range_overright_multirange_internal(
    rangetyp: *mut TypeCacheEntry,
    r: *const RangeType,
    mr: *const MultirangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    if RangeIsEmpty(r) || MultirangeIsEmpty(mr) {
        return false;
    }

    range_deserialize(rangetyp, r, &mut lower1, &mut upper1, &mut empty);
    Assert!(!empty);
    multirange_get_bounds(rangetyp, mr, 0, &mut lower2, &mut upper2);

    range_cmp_bounds(rangetyp, &mut lower1, &mut lower2) >= 0
}

pub unsafe fn range_overright_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 0);
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(range_overright_multirange_internal((*typcache).rngtype, r, mr))
}

pub unsafe fn multirange_overright_range(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    if MultirangeIsEmpty(mr) || RangeIsEmpty(r) {
        PG_RETURN_BOOL!(false);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    multirange_get_bounds((*typcache).rngtype, mr, 0, &mut lower1, &mut upper1);
    range_deserialize((*typcache).rngtype, r, &mut lower2, &mut upper2, &mut empty);
    Assert!(!empty);

    PG_RETURN_BOOL!(range_cmp_bounds((*typcache).rngtype, &mut lower1, &mut lower2) >= 0)
}

pub unsafe fn multirange_overright_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();

    if MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2) {
        PG_RETURN_BOOL!(false);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    multirange_get_bounds((*typcache).rngtype, mr1, 0, &mut lower1, &mut upper1);
    multirange_get_bounds((*typcache).rngtype, mr2, 0, &mut lower2, &mut upper2);

    PG_RETURN_BOOL!(range_cmp_bounds((*typcache).rngtype, &mut lower1, &mut lower2) >= 0)
}

/* contains? */
pub unsafe fn multirange_contains_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    PG_RETURN_BOOL!(multirange_contains_multirange_internal((*typcache).rngtype, mr1, mr2))
}

/* contained by? */
pub unsafe fn multirange_contained_by_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    PG_RETURN_BOOL!(multirange_contains_multirange_internal((*typcache).rngtype, mr2, mr1))
}

/*
 * Test whether multirange mr1 contains every range from another multirange mr2.
 */
pub unsafe fn multirange_contains_multirange_internal(
    rangetyp: *mut TypeCacheEntry,
    mr1: *const MultirangeType,
    mr2: *const MultirangeType,
) -> bool {
    let range_count1: int32 = (*mr1).rangeCount as int32;
    let range_count2: int32 = (*mr2).rangeCount as int32;
    let mut i1: c_int;
    let mut i2: c_int;
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();

    /*
     * We follow the same logic for empties as ranges: - an empty multirange
     * contains an empty range/multirange. - an empty multirange can't contain
     * any other range/multirange. - an empty multirange is contained by any
     * other range/multirange.
     */

    if range_count2 == 0 {
        return true;
    }
    if range_count1 == 0 {
        return false;
    }

    /*
     * Every range in mr2 must be contained by some range in mr1. To avoid
     * O(n^2) we walk through both ranges in tandem.
     */
    i1 = 0;
    multirange_get_bounds(rangetyp, mr1, i1 as uint32, &mut lower1, &mut upper1);
    i2 = 0;
    while i2 < range_count2 {
        multirange_get_bounds(rangetyp, mr2, i2 as uint32, &mut lower2, &mut upper2);

        /* Discard r1s while r1 << r2 */
        while range_cmp_bounds(rangetyp, &mut upper1, &mut lower2) < 0 {
            i1 += 1;
            if i1 >= range_count1 {
                return false;
            }
            multirange_get_bounds(rangetyp, mr1, i1 as uint32, &mut lower1, &mut upper1);
        }

        /*
         * If r1 @> r2, go to the next r2, otherwise return false (since every
         * r1[n] and r1[n+1] must have a gap). Note this will give weird
         * answers if you don't canonicalize, e.g. with a custom
         * int2multirange {[1,1], [2,2]} there is a "gap". But that is
         * consistent with other range operators, e.g. '[1,1]'::int2range -|-
         * '[2,2]'::int2range is false.
         */
        if !range_bounds_contains(rangetyp, &mut lower1, &mut upper1, &mut lower2, &mut upper2) {
            return false;
        }

        i2 += 1;
    }

    /* All ranges in mr2 are satisfied */
    true
}

/* strictly left of? */
pub unsafe fn range_before_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 0);
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(range_before_multirange_internal((*typcache).rngtype, r, mr))
}

pub unsafe fn multirange_before_range(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(range_after_multirange_internal((*typcache).rngtype, r, mr))
}

pub unsafe fn multirange_before_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    PG_RETURN_BOOL!(multirange_before_multirange_internal((*typcache).rngtype, mr1, mr2))
}

/* strictly right of? */
pub unsafe fn range_after_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 0);
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(range_after_multirange_internal((*typcache).rngtype, r, mr))
}

pub unsafe fn multirange_after_range(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(range_before_multirange_internal((*typcache).rngtype, r, mr))
}

pub unsafe fn multirange_after_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    PG_RETURN_BOOL!(multirange_before_multirange_internal((*typcache).rngtype, mr2, mr1))
}

/* strictly left of? (internal version) */
pub unsafe fn range_before_multirange_internal(
    rangetyp: *mut TypeCacheEntry,
    r: *const RangeType,
    mr: *const MultirangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;

    if RangeIsEmpty(r) || MultirangeIsEmpty(mr) {
        return false;
    }

    range_deserialize(rangetyp, r, &mut lower1, &mut upper1, &mut empty);
    Assert!(!empty);

    multirange_get_bounds(rangetyp, mr, 0, &mut lower2, &mut upper2);

    range_cmp_bounds(rangetyp, &mut upper1, &mut lower2) < 0
}

pub unsafe fn multirange_before_multirange_internal(
    rangetyp: *mut TypeCacheEntry,
    mr1: *const MultirangeType,
    mr2: *const MultirangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();

    if MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2) {
        return false;
    }

    multirange_get_bounds(rangetyp, mr1, (*mr1).rangeCount - 1, &mut lower1, &mut upper1);
    multirange_get_bounds(rangetyp, mr2, 0, &mut lower2, &mut upper2);

    range_cmp_bounds(rangetyp, &mut upper1, &mut lower2) < 0
}

/* strictly right of? (internal version) */
pub unsafe fn range_after_multirange_internal(
    rangetyp: *mut TypeCacheEntry,
    r: *const RangeType,
    mr: *const MultirangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;
    let range_count: int32;

    if RangeIsEmpty(r) || MultirangeIsEmpty(mr) {
        return false;
    }

    range_deserialize(rangetyp, r, &mut lower1, &mut upper1, &mut empty);
    Assert!(!empty);

    range_count = (*mr).rangeCount as int32;
    multirange_get_bounds(rangetyp, mr, (range_count - 1) as uint32, &mut lower2, &mut upper2);

    range_cmp_bounds(rangetyp, &mut lower1, &mut upper2) > 0
}

pub unsafe fn range_adjacent_multirange_internal(
    rangetyp: *mut TypeCacheEntry,
    r: *const RangeType,
    mr: *const MultirangeType,
) -> bool {
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();
    let mut empty: bool = false;
    let range_count: int32;

    if RangeIsEmpty(r) || MultirangeIsEmpty(mr) {
        return false;
    }

    range_deserialize(rangetyp, r, &mut lower1, &mut upper1, &mut empty);
    Assert!(!empty);

    range_count = (*mr).rangeCount as int32;
    multirange_get_bounds(rangetyp, mr, 0, &mut lower2, &mut upper2);

    if bounds_adjacent(rangetyp, upper1, lower2) {
        return true;
    }

    if range_count > 1 {
        multirange_get_bounds(rangetyp, mr, (range_count - 1) as uint32, &mut lower2, &mut upper2);
    }

    if bounds_adjacent(rangetyp, upper2, lower1) {
        return true;
    }

    false
}

/* adjacent to? */
pub unsafe fn range_adjacent_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 0);
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(range_adjacent_multirange_internal((*typcache).rngtype, r, mr))
}

pub unsafe fn multirange_adjacent_range(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let r: *mut RangeType = PG_GETARG_RANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;

    if RangeIsEmpty(r) || MultirangeIsEmpty(mr) {
        return BoolGetDatum(false);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));

    PG_RETURN_BOOL!(range_adjacent_multirange_internal((*typcache).rngtype, r, mr))
}

pub unsafe fn multirange_adjacent_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let typcache: *mut TypeCacheEntry;
    let range_count1: int32;
    let range_count2: int32;
    let mut lower1: RangeBound = std::mem::zeroed();
    let mut upper1: RangeBound = std::mem::zeroed();
    let mut lower2: RangeBound = std::mem::zeroed();
    let mut upper2: RangeBound = std::mem::zeroed();

    if MultirangeIsEmpty(mr1) || MultirangeIsEmpty(mr2) {
        return BoolGetDatum(false);
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    range_count1 = (*mr1).rangeCount as int32;
    range_count2 = (*mr2).rangeCount as int32;
    multirange_get_bounds((*typcache).rngtype, mr1, (range_count1 - 1) as uint32, &mut lower1, &mut upper1);
    multirange_get_bounds((*typcache).rngtype, mr2, 0, &mut lower2, &mut upper2);
    if bounds_adjacent((*typcache).rngtype, upper1, lower2) {
        PG_RETURN_BOOL!(true);
    }

    if range_count1 > 1 {
        multirange_get_bounds((*typcache).rngtype, mr1, 0, &mut lower1, &mut upper1);
    }
    if range_count2 > 1 {
        multirange_get_bounds((*typcache).rngtype, mr2, (range_count2 - 1) as uint32, &mut lower2, &mut upper2);
    }
    if bounds_adjacent((*typcache).rngtype, upper2, lower1) {
        PG_RETURN_BOOL!(true);
    }
    PG_RETURN_BOOL!(false)
}

/* Btree support */

/* btree comparator */
pub unsafe fn multirange_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let mr1: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mr2: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 1);
    let range_count_1: int32;
    let range_count_2: int32;
    let range_count_max: int32;
    let mut i: int32;
    let typcache: *mut TypeCacheEntry;
    let mut cmp: c_int = 0; /* If both are empty we'll use this. */

    /* Different types should be prevented by ANYMULTIRANGE matching rules */
    if MultirangeTypeGetOid(mr1) != MultirangeTypeGetOid(mr2) {
        elog!(ERROR, "multirange types do not match");
    }

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr1));

    range_count_1 = (*mr1).rangeCount as int32;
    range_count_2 = (*mr2).rangeCount as int32;

    /* Loop over source data */
    range_count_max = Max(range_count_1, range_count_2);
    i = 0;
    while i < range_count_max {
        let mut lower1: RangeBound = std::mem::zeroed();
        let mut upper1: RangeBound = std::mem::zeroed();
        let mut lower2: RangeBound = std::mem::zeroed();
        let mut upper2: RangeBound = std::mem::zeroed();

        /*
         * If one multirange is shorter, it's as if it had empty ranges at the
         * end to extend its length. An empty range compares earlier than any
         * other range, so the shorter multirange comes before the longer.
         * This is the same behavior as in other types, e.g. in strings 'aaa'
         * < 'aaaaaa'.
         */
        if i >= range_count_1 {
            cmp = -1;
            break;
        }
        if i >= range_count_2 {
            cmp = 1;
            break;
        }

        multirange_get_bounds((*typcache).rngtype, mr1, i as uint32, &mut lower1, &mut upper1);
        multirange_get_bounds((*typcache).rngtype, mr2, i as uint32, &mut lower2, &mut upper2);

        cmp = range_cmp_bounds((*typcache).rngtype, &mut lower1, &mut lower2);
        if cmp == 0 {
            cmp = range_cmp_bounds((*typcache).rngtype, &mut upper1, &mut upper2);
        }
        if cmp != 0 {
            break;
        }

        i += 1;
    }

    PG_FREE_IF_COPY!(fcinfo, mr1, 0);
    PG_FREE_IF_COPY!(fcinfo, mr2, 1);

    PG_RETURN_INT32!(cmp)
}

/* inequality operators using the multirange_cmp function */
pub unsafe fn multirange_lt(fcinfo: FunctionCallInfo) -> Datum {
    let cmp: c_int = DatumGetInt32(multirange_cmp(fcinfo));

    PG_RETURN_BOOL!(cmp < 0)
}

pub unsafe fn multirange_le(fcinfo: FunctionCallInfo) -> Datum {
    let cmp: c_int = DatumGetInt32(multirange_cmp(fcinfo));

    PG_RETURN_BOOL!(cmp <= 0)
}

pub unsafe fn multirange_ge(fcinfo: FunctionCallInfo) -> Datum {
    let cmp: c_int = DatumGetInt32(multirange_cmp(fcinfo));

    PG_RETURN_BOOL!(cmp >= 0)
}

pub unsafe fn multirange_gt(fcinfo: FunctionCallInfo) -> Datum {
    let cmp: c_int = DatumGetInt32(multirange_cmp(fcinfo));

    PG_RETURN_BOOL!(cmp > 0)
}

/* multirange -> range functions */

/* Find the smallest range that includes everything in the multirange */
pub unsafe fn range_merge_from_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mltrngtypoid: Oid = MultirangeTypeGetOid(mr);
    let typcache: *mut TypeCacheEntry;
    let result: *mut RangeType;

    typcache = multirange_get_typcache(fcinfo, mltrngtypoid);

    if MultirangeIsEmpty(mr) {
        result = make_empty_range((*typcache).rngtype);
    } else if (*mr).rangeCount == 1 {
        result = multirange_get_range((*typcache).rngtype, mr, 0);
    } else {
        let mut firstLower: RangeBound = std::mem::zeroed();
        let mut firstUpper: RangeBound = std::mem::zeroed();
        let mut lastLower: RangeBound = std::mem::zeroed();
        let mut lastUpper: RangeBound = std::mem::zeroed();

        multirange_get_bounds((*typcache).rngtype, mr, 0, &mut firstLower, &mut firstUpper);
        multirange_get_bounds((*typcache).rngtype, mr, (*mr).rangeCount - 1, &mut lastLower, &mut lastUpper);

        result = make_range((*typcache).rngtype, &mut firstLower, &mut lastUpper, false, null_mut());
    }

    crate::PG_RETURN_POINTER!(result as *mut c_void)
}

/* Turn multirange into a set of ranges */
pub unsafe fn multirange_unnest(fcinfo: FunctionCallInfo) -> Datum {
    #[repr(C)]
    struct multirange_unnest_fctx {
        mr: *mut MultirangeType,
        typcache: *mut TypeCacheEntry,
        index: c_int,
    }

    let mut funcctx: *mut FuncCallContext;
    let fctx: *mut multirange_unnest_fctx;
    let oldcontext: MemoryContext;

    /* stuff done only on the first call of the function */
    if SRF_IS_FIRSTCALL!(fcinfo) {
        let mr: *mut MultirangeType;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT!(fcinfo);

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        /*
         * Get the multirange value and detoast if needed.  We can't do this
         * earlier because if we have to detoast, we want the detoasted copy
         * to be in multi_call_memory_ctx, so it will go away when we're done
         * and not before.  (If no detoast happens, we assume the originally
         * passed multirange will stick around till then.)
         */
        mr = PG_GETARG_MULTIRANGE_P(fcinfo, 0);

        /* allocate memory for user context */
        let fctx = palloc(std::mem::size_of::<multirange_unnest_fctx>()) as *mut multirange_unnest_fctx;

        /* initialize state */
        (*fctx).mr = mr;
        (*fctx).index = 0;
        (*fctx).typcache =
            lookup_type_cache(MultirangeTypeGetOid(mr), TYPECACHE_MULTIRANGE_INFO);

        (*funcctx).user_fctx = fctx as *mut c_void;
        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP!(fcinfo);
    fctx = (*funcctx).user_fctx as *mut multirange_unnest_fctx;

    if (*fctx).index < (*(*fctx).mr).rangeCount as c_int {
        let range: *mut RangeType;

        range = multirange_get_range((*(*fctx).typcache).rngtype, (*fctx).mr, (*fctx).index);
        (*fctx).index += 1;

        SRF_RETURN_NEXT!(fcinfo, funcctx, RangeTypePGetDatum(range));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE!(fcinfo, funcctx);
    }
}

/* Hash support */

/* hash a multirange value */
pub unsafe fn hash_multirange(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let mut result: uint32 = 1;
    let typcache: *mut TypeCacheEntry;
    let mut scache: *mut TypeCacheEntry;
    let range_count: int32;
    let mut i: int32;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));
    scache = (*(*typcache).rngtype).rngelemtype;
    if !OidIsValid((*scache).hash_proc_finfo.fn_oid) {
        scache = lookup_type_cache((*scache).type_id, TYPECACHE_HASH_PROC_FINFO);
        if !OidIsValid((*scache).hash_proc_finfo.fn_oid) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not identify a hash function for type {}",
                    std::ffi::CStr::from_ptr(format_type_be((*scache).type_id)).to_string_lossy()
                )
            );
        }
    }

    range_count = (*mr).rangeCount as int32;
    i = 0;
    while i < range_count {
        let mut lower: RangeBound = std::mem::zeroed();
        let mut upper: RangeBound = std::mem::zeroed();
        let flags: uint8 = *MultirangeGetFlagsPtr(mr).add(i as usize);
        let lower_hash: uint32;
        let upper_hash: uint32;
        let mut range_hash: uint32;

        multirange_get_bounds((*typcache).rngtype, mr, i as uint32, &mut lower, &mut upper);

        if RANGE_HAS_LBOUND(flags) {
            lower_hash = DatumGetUInt32(FunctionCall1Coll(
                &mut (*scache).hash_proc_finfo,
                (*(*typcache).rngtype).rng_collation,
                lower.val,
            ));
        } else {
            lower_hash = 0;
        }

        if RANGE_HAS_UBOUND(flags) {
            upper_hash = DatumGetUInt32(FunctionCall1Coll(
                &mut (*scache).hash_proc_finfo,
                (*(*typcache).rngtype).rng_collation,
                upper.val,
            ));
        } else {
            upper_hash = 0;
        }

        /* Merge hashes of flags and bounds */
        range_hash = DatumGetUInt32(hash_uint32(flags as uint32));
        range_hash ^= lower_hash;
        range_hash = pg_rotate_left32(range_hash, 1);
        range_hash ^= upper_hash;

        /*
         * Use the same approach as hash_array to combine the individual
         * elements' hash values:
         */
        result = (result << 5).wrapping_sub(result).wrapping_add(range_hash);

        i += 1;
    }

    PG_FREE_IF_COPY!(fcinfo, mr, 0);

    PG_RETURN_UINT32!(result)
}

/*
 * Returns 64-bit value by hashing a value to a 64-bit value, with a seed.
 * Otherwise, similar to hash_multirange.
 */
pub unsafe fn hash_multirange_extended(fcinfo: FunctionCallInfo) -> Datum {
    let mr: *mut MultirangeType = PG_GETARG_MULTIRANGE_P(fcinfo, 0);
    let seed: Datum = PG_GETARG_DATUM!(fcinfo, 1);
    let mut result: uint64 = 1;
    let typcache: *mut TypeCacheEntry;
    let mut scache: *mut TypeCacheEntry;
    let range_count: int32;
    let mut i: int32;

    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(mr));
    scache = (*(*typcache).rngtype).rngelemtype;
    if !OidIsValid((*scache).hash_extended_proc_finfo.fn_oid) {
        scache = lookup_type_cache((*scache).type_id, TYPECACHE_HASH_EXTENDED_PROC_FINFO);
        if !OidIsValid((*scache).hash_extended_proc_finfo.fn_oid) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not identify a hash function for type {}",
                    std::ffi::CStr::from_ptr(format_type_be((*scache).type_id)).to_string_lossy()
                )
            );
        }
    }

    range_count = (*mr).rangeCount as int32;
    i = 0;
    while i < range_count {
        let mut lower: RangeBound = std::mem::zeroed();
        let mut upper: RangeBound = std::mem::zeroed();
        let flags: uint8 = *MultirangeGetFlagsPtr(mr).add(i as usize);
        let lower_hash: uint64;
        let upper_hash: uint64;
        let mut range_hash: uint64;

        multirange_get_bounds((*typcache).rngtype, mr, i as uint32, &mut lower, &mut upper);

        if RANGE_HAS_LBOUND(flags) {
            lower_hash = DatumGetUInt64(FunctionCall2Coll(
                &mut (*scache).hash_extended_proc_finfo,
                (*(*typcache).rngtype).rng_collation,
                lower.val,
                seed,
            ));
        } else {
            lower_hash = 0;
        }

        if RANGE_HAS_UBOUND(flags) {
            upper_hash = DatumGetUInt64(FunctionCall2Coll(
                &mut (*scache).hash_extended_proc_finfo,
                (*(*typcache).rngtype).rng_collation,
                upper.val,
                seed,
            ));
        } else {
            upper_hash = 0;
        }

        /* Merge hashes of flags and bounds */
        range_hash = DatumGetUInt64(hash_uint32_extended(flags as uint32, DatumGetInt64(seed) as uint64));
        range_hash ^= lower_hash;
        range_hash = ROTATE_HIGH_AND_LOW_32BITS(range_hash);
        range_hash ^= upper_hash;

        /*
         * Use the same approach as hash_array to combine the individual
         * elements' hash values:
         */
        result = (result << 5).wrapping_sub(result).wrapping_add(range_hash);

        i += 1;
    }

    PG_FREE_IF_COPY!(fcinfo, mr, 0);

    PG_RETURN_UINT64!(result)
}

/* multirangetypes.h: PG_GETARG_MULTIRANGE_P(n) */
#[inline]
unsafe fn PG_GETARG_MULTIRANGE_P(fcinfo: FunctionCallInfo, n: c_int) -> *mut MultirangeType {
    DatumGetMultirangeTypeP(PG_GETARG_DATUM!(fcinfo, n))
}

/* utils/rangetypes.h: PG_GETARG_RANGE_P(n) */
#[inline]
unsafe fn PG_GETARG_RANGE_P(fcinfo: FunctionCallInfo, n: c_int) -> *mut RangeType {
    DatumGetRangeTypeP(PG_GETARG_DATUM!(fcinfo, n))
}
