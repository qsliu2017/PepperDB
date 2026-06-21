//! array_userfuncs.rs
//!   Misc user-visible array support functions
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/array_userfuncs.c
//!
//! Portions Copyright (c) 2003-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/array_userfuncs.c

// #include "postgres.h"
use crate::prelude::*;

// catalog/pg_operator_d.h, catalog/pg_type.h
use crate::catalog::pg_type_d::INT4OID;
use crate::catalog::pg_known_oids::{ARRAY_GT_OP, ARRAY_LT_OP};

// common/int.h
use crate::common::int::{pg_add_s32_overflow, pg_sub_s32_overflow};

// common/pg_prng.h
use crate::common::pg_prng::{pg_global_prng_state, pg_prng_uint64_range};

// libpq/pqformat.h
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgbyte, pq_getmsgbytes, pq_getmsgend, pq_getmsgint,
    pq_getmsgint64, pq_sendbytes, pq_sendint16, pq_sendint32, pq_sendint64, pq_sendint8,
};

// nodes/supportnodes.h
use crate::nodes::nodes::{nodeTag, Node};
use crate::nodes::pg_list::{linitial, lsecond, List};
use crate::nodes::primnodes::{Param, ParamKind};
use crate::nodes::supportnodes::SupportRequestModifyInPlace;

// port/pg_bitutils.h
use crate::port::pg_bitutils::pg_nextpower2_32;

// utils/array.h
use crate::utils::array::{
    ArrayType, ARR_DATA_OFFSET, ARR_DATA_PTR, ARR_DIMS, ARR_ELEMTYPE, ARR_HASNULL, ARR_LBOUND,
    ARR_NDIM, ARR_NULLBITMAP, ARR_OVERHEAD_NONULLS, ARR_OVERHEAD_WITHNULLS, ARR_SIZE,
};
use crate::utils::adt::array_expanded::{ArrayMetaState, ExpandedArrayHeader};
use crate::utils::adt::arrayutils::{ArrayCheckBounds, ArrayGetNItems};

// utils/builtins.h
use crate::utils::builtins::format_type_be;

// utils/datum.h
use crate::utils::adt::datum::datumCopy;

// utils/expandeddatum.h
use crate::utils::adt::expandeddatum::EOHPGetRWDatum;

// utils/fmgr.h
use crate::utils::fmgr::{
    fmgr_info_cxt, get_fn_expr_argtype, FmgrInfo, FunctionCall2Coll, FunctionCallInfo,
    ReceiveFunctionCall, SendFunctionCall,
};

// utils/init/globals.h: work_mem
use crate::utils::init::globals::work_mem;

// lib/stringinfo.h
use crate::lib::stringinfo::{initReadOnlyStringInfo, StringInfoData};

// varatt.h
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARHDRSZ, VARSIZE, VARSIZE_ANY_EXHDR};

// #[macro_export] macros live at the crate root.  (DatumGetBool, Int32GetDatum,
// OidIsValid are plain fns from postgres.h/c.h, already in scope via prelude.)
use crate::{
    IsA, PG_ARGISNULL, PG_FREE_IF_COPY, PG_GETARG_BOOL, PG_GETARG_DATUM, PG_GETARG_INT32,
    PG_GETARG_POINTER, PG_GET_COLLATION, PG_NARGS, PG_RETURN_INT32, PG_RETURN_NULL,
    PG_RETURN_POINTER,
};

use crate::c::{bits8, bytea, int16, int32, uint32};
use std::ffi::{c_char, c_int, c_void};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ----------------------------------------------------------------------------
//   Local macros not yet at the crate root.
// ----------------------------------------------------------------------------

// PG_GETARG_ARRAYTYPE_P(n): DatumGetArrayTypeP(PG_GETARG_DATUM(fcinfo, n)).
macro_rules! PG_GETARG_ARRAYTYPE_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetArrayTypeP(PG_GETARG_DATUM!($fcinfo, $n))
    };
}

// PG_RETURN_ARRAYTYPE_P(x): return PointerGetDatum(x).
macro_rules! PG_RETURN_ARRAYTYPE_P {
    ($x:expr) => {
        return PointerGetDatum($x as *const c_void)
    };
}

// PG_RETURN_BYTEA_P(x): return PointerGetDatum(x).
macro_rules! PG_RETURN_BYTEA_P {
    ($x:expr) => {
        return PointerGetDatum($x as *const c_void)
    };
}

// PG_RETURN_DATUM(x): return x.
macro_rules! PG_RETURN_DATUM {
    ($x:expr) => {
        return ($x)
    };
}

// PG_GETARG_BYTEA_PP(n): detoast as packed bytea.  The crate-root macro lives in
// fmgr; re-declare locally for ergonomics.
macro_rules! PG_GETARG_BYTEA_PP {
    ($fcinfo:expr, $n:expr) => {
        crate::PG_GETARG_BYTEA_PP!($fcinfo, $n)
    };
}

// ----------------------------------------------------------------------------
//   STUBBED dependencies (not yet ported)
// ----------------------------------------------------------------------------

/// STUB: `DatumGetArrayTypeP` (fmgr.h / arrayfuncs.c) - detoasts a Datum into a
/// flat ArrayType pointer.  The crate-root macro is not yet available.
/// TODO(pg-port): real DatumGetArrayTypeP lives in utils/adt/arrayfuncs.rs.
#[inline]
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType {
    DatumGetPointer(d) as *mut ArrayType
}

/// STUB: `IsA(node, SupportRequestModifyInPlace)` - the NodeTag enum variant
/// `T_SupportRequestModifyInPlace` is not yet present in nodes/nodes.rs, so the
/// crate-root `IsA!` macro can't name it.  Match the request struct's leading
/// `type_` field against the (not-yet-added) tag.  Conservatively returns false
/// until the tag exists, which makes the planner support function a no-op (a
/// semantically valid outcome for SupportRequestModifyInPlace).
/// TODO(pg-port): add NodeTag::T_SupportRequestModifyInPlace to nodes/nodes.rs
/// and replace this with `IsA!(node, T_SupportRequestModifyInPlace)`.
#[inline]
unsafe fn isa_support_request_modify_in_place(node: *mut Node) -> bool {
    let _ = nodeTag(node);
    false
}

/// STUB: `Tuplesortstate` (utils/sort/tuplesort.c) - opaque sort state.
/// TODO(pg-port): real Tuplesortstate lives in utils/sort/tuplesort.rs.
#[repr(C)]
pub struct Tuplesortstate {
    _private: [u8; 0],
}

/// STUB: TUPLESORT_NONE (utils/tuplesort.h) sort option flag.
/// TODO(pg-port): utils/sort/tuplesort.rs.
const TUPLESORT_NONE: c_int = 0;

/// STUB: `TypeCacheEntry` (utils/typcache.h) - cached per-type catalog info.
/// Only the fields used in this file are modeled.
/// TODO(pg-port): real TypeCacheEntry lives in utils/cache/typcache.rs.
#[repr(C)]
pub struct TypeCacheEntry {
    pub type_id: Oid,
    pub typlen: int16,
    pub typbyval: bool,
    pub typalign: c_char,
    pub typarray: Oid,
    pub lt_opr: Oid,
    pub gt_opr: Oid,
    pub eq_opr_finfo: FmgrInfo,
}

/* utils/typcache.h: lookup_type_cache() flags */
const TYPECACHE_EQ_OPR_FINFO: c_int = 0x00400;
const TYPECACHE_LT_OPR: c_int = 0x00001;
const TYPECACHE_GT_OPR: c_int = 0x00002;

/// STUB: `lookup_type_cache` (utils/cache/typcache.c).
/// TODO(pg-port): translate utils/cache/typcache.c::lookup_type_cache.
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!("lookup_type_cache (typcache.c) not yet ported");
}

/// STUB: `ArrayIterator` (utils/array.h) - opaque iterator over array elements.
/// TODO(pg-port): real ArrayIterator lives in utils/adt/arrayfuncs.rs.
pub type ArrayIterator = *mut c_void;

/// STUB: `ArrayBuildState` (utils/array.h).
/// TODO(pg-port): real ArrayBuildState lives in utils/adt/arrayfuncs.rs.
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

/// STUB: `ArrayBuildStateArr` (utils/array.h).
/// TODO(pg-port): real ArrayBuildStateArr lives in utils/adt/arrayfuncs.rs.
#[repr(C)]
pub struct ArrayBuildStateArr {
    pub mcontext: MemoryContext,
    pub data: *mut c_char,
    pub nullbitmap: *mut bits8,
    pub abytes: c_int,
    pub nbytes: c_int,
    pub aitems: c_int,
    pub nitems: c_int,
    pub ndims: c_int,
    pub dims: [c_int; MAXDIM as usize],
    pub lbs: [c_int; MAXDIM as usize],
    pub array_type: Oid,
    pub element_type: Oid,
}

/// STUB: `ArrayBuildStateAny` (utils/array.h) - opaque "either" build state.
/// TODO(pg-port): real ArrayBuildStateAny lives in utils/adt/arrayfuncs.rs.
#[repr(C)]
pub struct ArrayBuildStateAny {
    _private: [u8; 0],
}

/* utils/array.h: MAXDIM */
const MAXDIM: c_int = 6;

/// STUB: `construct_empty_array` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::construct_empty_array.
unsafe fn construct_empty_array(_elmtype: Oid) -> *mut ArrayType {
    unimplemented!("construct_empty_array (arrayfuncs.c) not yet ported");
}

/// STUB: `construct_md_array` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::construct_md_array.
unsafe fn construct_md_array(
    _elems: *mut Datum,
    _nulls: *mut bool,
    _ndims: c_int,
    _dims: *mut c_int,
    _lbs: *mut c_int,
    _elmtype: Oid,
    _elmlen: c_int,
    _elmbyval: bool,
    _elmalign: c_char,
) -> *mut ArrayType {
    unimplemented!("construct_md_array (arrayfuncs.c) not yet ported");
}

/// STUB: `deconstruct_array` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::deconstruct_array.
unsafe fn deconstruct_array(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elmlen: c_int,
    _elmbyval: bool,
    _elmalign: c_char,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) { crate::utils::adt::arrayfuncs::deconstruct_array(_array as _, _elmtype as _, _elmlen as _, _elmbyval, _elmalign as _, _elemsp as _, _nullsp as _, _nelemsp as _) }

/// STUB: `array_set_element` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::array_set_element.
unsafe fn array_set_element(
    _arraydatum: Datum,
    _nSubscripts: c_int,
    _indx: *mut c_int,
    _dataValue: Datum,
    _isNull: bool,
    _arraytyplen: c_int,
    _elmlen: c_int,
    _elmbyval: bool,
    _elmalign: c_char,
) -> Datum { crate::utils::adt::arrayfuncs::array_set_element(_arraydatum as _, _nSubscripts as _, _indx as _, _dataValue as _, _isNull, _arraytyplen as _, _elmlen as _, _elmbyval, _elmalign as _) as _ }

/// STUB: `array_contains_nulls` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::array_contains_nulls.
unsafe fn array_contains_nulls(_array: *mut ArrayType) -> bool { crate::utils::adt::arrayfuncs::array_contains_nulls(_array as _) }

/// STUB: `array_bitmap_copy` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::array_bitmap_copy.
unsafe fn array_bitmap_copy(
    _destbitmap: *mut bits8,
    _destoffset: c_int,
    _srcbitmap: *const bits8,
    _srcoffset: c_int,
    _nitems: c_int,
) { crate::utils::adt::arrayfuncs::array_bitmap_copy(_destbitmap as _, _destoffset as _, _srcbitmap as _, _srcoffset as _, _nitems as _) }

/// STUB: `array_create_iterator` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::array_create_iterator.
unsafe fn array_create_iterator(
    _arr: *mut ArrayType,
    _slice_ndim: c_int,
    _mstate: *mut ArrayMetaState,
) -> ArrayIterator { crate::utils::adt::arrayfuncs::array_create_iterator(_arr as _, _slice_ndim as _, _mstate as _) as _ }

/// STUB: `array_iterate` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::array_iterate.
unsafe fn array_iterate(_iterator: ArrayIterator, _value: *mut Datum, _isnull: *mut bool) -> bool { crate::utils::adt::arrayfuncs::array_iterate(_iterator as _, _value as _, _isnull as _) }

/// STUB: `array_free_iterator` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::array_free_iterator.
unsafe fn array_free_iterator(_iterator: ArrayIterator) { crate::utils::adt::arrayfuncs::array_free_iterator(_iterator as _) }

/// STUB: `construct_empty_expanded_array` (utils/adt/array_expanded.c).
/// TODO(pg-port): translate utils/adt/array_expanded.c::construct_empty_expanded_array.
unsafe fn construct_empty_expanded_array(
    _element_type: Oid,
    _parentcontext: MemoryContext,
    _metacache: *mut ArrayMetaState,
) -> *mut ExpandedArrayHeader { crate::utils::adt::arrayfuncs::construct_empty_expanded_array(_element_type as _, _parentcontext as _, _metacache as _) as _ }

/// STUB: `PG_GETARG_EXPANDED_ARRAYX` (utils/array.h).
/// TODO(pg-port): translate utils/adt/array_expanded.c::DatumGetExpandedArrayX.
unsafe fn PG_GETARG_EXPANDED_ARRAYX(
    _fcinfo: FunctionCallInfo,
    _argno: c_int,
    _metacache: *mut ArrayMetaState,
) -> *mut ExpandedArrayHeader {
    unimplemented!("PG_GETARG_EXPANDED_ARRAYX (array_expanded.c) not yet ported");
}

/// STUB: `AggCheckCallContext` (executor/nodeAgg.c).
/// TODO(pg-port): translate executor/nodeAgg.c::AggCheckCallContext.
unsafe fn AggCheckCallContext(_fcinfo: FunctionCallInfo, _aggcontext: *mut MemoryContext) -> c_int {
    unimplemented!("AggCheckCallContext (nodeAgg.c) not yet ported");
}

/// STUB: `get_element_type` (utils/cache/lsyscache.c).
/// TODO(pg-port): translate utils/cache/lsyscache.c::get_element_type.
unsafe fn get_element_type(_typid: Oid) -> Oid { crate::utils::cache::lsyscache::get_element_type(_typid as _) as _ }

/// STUB: `get_typlenbyvalalign` (utils/cache/lsyscache.c).
/// TODO(pg-port): translate utils/cache/lsyscache.c::get_typlenbyvalalign.
unsafe fn get_typlenbyvalalign(
    _typid: Oid,
    _typlen: *mut int16,
    _typbyval: *mut bool,
    _typalign: *mut c_char,
) { crate::utils::cache::lsyscache::get_typlenbyvalalign(_typid as _, _typlen as _, _typbyval as _, _typalign as _) }

/// STUB: `getTypeBinaryOutputInfo` (utils/cache/lsyscache.c).
/// TODO(pg-port): translate utils/cache/lsyscache.c::getTypeBinaryOutputInfo.
unsafe fn getTypeBinaryOutputInfo(_type: Oid, _typSend: *mut Oid, _typIsVarlena: *mut bool) {
    unimplemented!("getTypeBinaryOutputInfo (lsyscache.c) not yet ported");
}

/// STUB: `getTypeBinaryInputInfo` (utils/cache/lsyscache.c).
/// TODO(pg-port): translate utils/cache/lsyscache.c::getTypeBinaryInputInfo.
unsafe fn getTypeBinaryInputInfo(_type: Oid, _typReceive: *mut Oid, _typIOParam: *mut Oid) {
    unimplemented!("getTypeBinaryInputInfo (lsyscache.c) not yet ported");
}

/// STUB: `initArrayResult` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::initArrayResult.
unsafe fn initArrayResult(
    _element_type: Oid,
    _rcontext: MemoryContext,
    _subcontext: bool,
) -> *mut ArrayBuildState { crate::utils::adt::arrayfuncs::initArrayResult(_element_type as _, _rcontext as _, _subcontext) as _ }

/// STUB: `initArrayResultWithSize` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::initArrayResultWithSize.
unsafe fn initArrayResultWithSize(
    _element_type: Oid,
    _rcontext: MemoryContext,
    _subcontext: bool,
    _initsize: c_int,
) -> *mut ArrayBuildState { crate::utils::adt::arrayfuncs::initArrayResultWithSize(_element_type as _, _rcontext as _, _subcontext, _initsize as _) as _ }

/// STUB: `initArrayResultArr` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::initArrayResultArr.
unsafe fn initArrayResultArr(
    _array_type: Oid,
    _element_type: Oid,
    _rcontext: MemoryContext,
    _subcontext: bool,
) -> *mut ArrayBuildStateArr { crate::utils::adt::arrayfuncs::initArrayResultArr(_array_type as _, _element_type as _, _rcontext as _, _subcontext) as _ }

/// STUB: `accumArrayResult` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::accumArrayResult.
unsafe fn accumArrayResult(
    _astate: *mut ArrayBuildState,
    _dvalue: Datum,
    _disnull: bool,
    _element_type: Oid,
    _rcontext: MemoryContext,
) -> *mut ArrayBuildState { crate::utils::adt::arrayfuncs::accumArrayResult(_astate as _, _dvalue as _, _disnull, _element_type as _, _rcontext as _) as _ }

/// STUB: `accumArrayResultArr` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::accumArrayResultArr.
unsafe fn accumArrayResultArr(
    _astate: *mut ArrayBuildStateArr,
    _dvalue: Datum,
    _disnull: bool,
    _array_type: Oid,
    _rcontext: MemoryContext,
) -> *mut ArrayBuildStateArr { crate::utils::adt::arrayfuncs::accumArrayResultArr(_astate as _, _dvalue as _, _disnull, _array_type as _, _rcontext as _) as _ }

/// STUB: `accumArrayResultAny` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::accumArrayResultAny.
unsafe fn accumArrayResultAny(
    _astate: *mut ArrayBuildStateAny,
    _dvalue: Datum,
    _disnull: bool,
    _input_type: Oid,
    _rcontext: MemoryContext,
) -> *mut ArrayBuildStateAny { crate::utils::adt::arrayfuncs::accumArrayResultAny(_astate as _, _dvalue as _, _disnull, _input_type as _, _rcontext as _) as _ }

/// STUB: `makeArrayResult` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::makeArrayResult.
unsafe fn makeArrayResult(_astate: *mut ArrayBuildState, _rcontext: MemoryContext) -> Datum { crate::utils::adt::arrayfuncs::makeArrayResult(_astate as _, _rcontext as _) as _ }

/// STUB: `makeMdArrayResult` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::makeMdArrayResult.
unsafe fn makeMdArrayResult(
    _astate: *mut ArrayBuildState,
    _ndims: c_int,
    _dims: *mut c_int,
    _lbs: *mut c_int,
    _rcontext: MemoryContext,
    _release: bool,
) -> Datum { crate::utils::adt::arrayfuncs::makeMdArrayResult(_astate as _, _ndims as _, _dims as _, _lbs as _, _rcontext as _, _release) as _ }

/// STUB: `makeArrayResultArr` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::makeArrayResultArr.
unsafe fn makeArrayResultArr(
    _astate: *mut ArrayBuildStateArr,
    _rcontext: MemoryContext,
    _release: bool,
) -> Datum { crate::utils::adt::arrayfuncs::makeArrayResultArr(_astate as _, _rcontext as _, _release) as _ }

/// STUB: `makeArrayResultAny` (utils/adt/arrayfuncs.c).
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::makeArrayResultAny.
unsafe fn makeArrayResultAny(
    _astate: *mut ArrayBuildStateAny,
    _rcontext: MemoryContext,
    _release: bool,
) -> Datum { crate::utils::adt::arrayfuncs::makeArrayResultAny(_astate as _, _rcontext as _, _release) as _ }

/// STUB: `tuplesort_begin_datum` (utils/sort/tuplesort.c).
/// TODO(pg-port): translate utils/sort/tuplesortvariants.c::tuplesort_begin_datum.
unsafe fn tuplesort_begin_datum(
    _datumType: Oid,
    _sortOperator: Oid,
    _sortCollation: Oid,
    _nullsFirstFlag: bool,
    _workMem: c_int,
    _coordinate: *mut c_void,
    _sortopt: c_int,
) -> *mut Tuplesortstate { crate::utils::sort::tuplesortvariants::tuplesort_begin_datum(_datumType as _, _sortOperator as _, _sortCollation as _, _nullsFirstFlag, _workMem as _, _coordinate as _, _sortopt as _) as _ }

/// STUB: `tuplesort_putdatum` (utils/sort/tuplesort.c).
unsafe fn tuplesort_putdatum(_state: *mut Tuplesortstate, _val: Datum, _isNull: bool) { crate::utils::sort::tuplesortvariants::tuplesort_putdatum(_state as _, _val as _, _isNull) }

/// STUB: `tuplesort_performsort` (utils/sort/tuplesort.c).
unsafe fn tuplesort_performsort(_state: *mut Tuplesortstate) {
    unimplemented!("tuplesort_performsort (tuplesort.c) not yet ported");
}

/// STUB: `tuplesort_getdatum` (utils/sort/tuplesort.c).
unsafe fn tuplesort_getdatum(
    _state: *mut Tuplesortstate,
    _forward: bool,
    _copy: bool,
    _val: *mut Datum,
    _isNull: *mut bool,
    _abbrev: *mut Datum,
) -> bool { crate::utils::sort::tuplesortvariants::tuplesort_getdatum(_state as _, _forward, _copy, _val as _, _isNull as _, _abbrev as _) }

/// STUB: `tuplesort_end` (utils/sort/tuplesort.c).
unsafe fn tuplesort_end(_state: *mut Tuplesortstate) {
    unimplemented!("tuplesort_end (tuplesort.c) not yet ported");
}

/* ------------------------------------------------------------------------- */

/*
 * SerialIOData
 *		Used for caching element-type data in array_agg_serialize
 */
#[repr(C)]
pub struct SerialIOData {
    pub typsend: FmgrInfo,
}

/*
 * DeserialIOData
 *		Used for caching element-type data in array_agg_deserialize
 */
#[repr(C)]
pub struct DeserialIOData {
    pub typreceive: FmgrInfo,
    pub typioparam: Oid,
}

/*
 * ArraySortCachedInfo
 *		Used for caching catalog data in array_sort
 */
#[repr(C)]
pub struct ArraySortCachedInfo {
    pub array_meta: ArrayMetaState, /* metadata for array_create_iterator */
    pub elem_lt_opr: Oid,           /* "<" operator for element type */
    pub elem_gt_opr: Oid,           /* ">" operator for element type */
    pub array_type: Oid,            /* pg_type OID of array type */
}

/*
 * fetch_array_arg_replace_nulls
 *
 * Fetch an array-valued argument in expanded form; if it's null, construct an
 * empty array value of the proper data type.  Also cache basic element type
 * information in fn_extra.
 *
 * Caution: if the input is a read/write pointer, this returns the input
 * argument; so callers must be sure that their changes are "safe", that is
 * they cannot leave the array in a corrupt state.
 *
 * If we're being called as an aggregate function, make sure any newly-made
 * expanded array is allocated in the aggregate state context, so as to save
 * copying operations.
 */
unsafe fn fetch_array_arg_replace_nulls(
    fcinfo: FunctionCallInfo,
    argno: c_int,
) -> *mut ExpandedArrayHeader {
    let eah: *mut ExpandedArrayHeader;
    let element_type: Oid;
    let mut my_extra: *mut ArrayMetaState;
    let mut resultcxt: MemoryContext = CurrentMemoryContext;

    /* If first time through, create datatype cache struct */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
    if my_extra.is_null() {
        my_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::size_of::<ArrayMetaState>(),
        ) as *mut ArrayMetaState;
        (*my_extra).element_type = InvalidOid;
        (*(*fcinfo).flinfo).fn_extra = my_extra as *mut c_void;
    }

    /* Figure out which context we want the result in */
    if AggCheckCallContext(fcinfo, &raw mut resultcxt) == 0 {
        resultcxt = CurrentMemoryContext;
    }

    /* Now collect the array value */
    if !PG_ARGISNULL!(fcinfo, argno) {
        let oldcxt = MemoryContextSwitchTo(resultcxt);

        eah = PG_GETARG_EXPANDED_ARRAYX(fcinfo, argno, my_extra);
        MemoryContextSwitchTo(oldcxt);
    } else {
        /* We have to look up the array type and element type */
        let arr_typeid: Oid = get_fn_expr_argtype((*fcinfo).flinfo, argno);

        if !OidIsValid(arr_typeid) {
            ereport!(
                ERROR,
                errmsg!("could not determine input data type")
            );
        }
        element_type = get_element_type(arr_typeid);
        if !OidIsValid(element_type) {
            ereport!(
                ERROR,
                errmsg!("input data type is not an array")
            );
        }

        eah = construct_empty_expanded_array(element_type, resultcxt, my_extra);
    }

    eah
}

/*-----------------------------------------------------------------------------
 * array_append :
 *		push an element onto the end of a one-dimensional array
 *----------------------------------------------------------------------------
 */
pub unsafe fn array_append(fcinfo: FunctionCallInfo) -> Datum {
    let eah: *mut ExpandedArrayHeader;
    let newelem: Datum;
    let isNull: bool;
    let result: Datum;
    let dimv: *mut c_int;
    let lb: *mut c_int;
    let indx: c_int;
    let my_extra: *mut ArrayMetaState;

    eah = fetch_array_arg_replace_nulls(fcinfo, 0);
    isNull = PG_ARGISNULL!(fcinfo, 1);
    if isNull {
        newelem = 0 as Datum;
    } else {
        newelem = PG_GETARG_DATUM!(fcinfo, 1);
    }

    if (*eah).ndims == 1 {
        /* append newelem */
        lb = (*eah).lbound;
        dimv = (*eah).dims;

        /* index of added elem is at lb[0] + (dimv[0] - 1) + 1 */
        let mut indx_tmp: c_int = 0;
        if pg_add_s32_overflow(*lb.add(0), *dimv.add(0), &raw mut indx_tmp) {
            ereport!(
                ERROR,
                errmsg!("integer out of range")
            );
        }
        indx = indx_tmp;
    } else if (*eah).ndims == 0 {
        indx = 1;
    } else {
        ereport!(
            ERROR,
            errmsg!("argument must be empty or one-dimensional array")
        );
        indx = 0; /* unreachable; keeps initialization happy */
    }

    /* Perform element insertion */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;

    let mut indx_arr = indx;
    result = array_set_element(
        EOHPGetRWDatum(&raw const (*eah).hdr),
        1,
        &raw mut indx_arr,
        newelem,
        isNull,
        -1,
        (*my_extra).typlen as c_int,
        (*my_extra).typbyval,
        (*my_extra).typalign,
    );

    PG_RETURN_DATUM!(result);
}

/*
 * array_append_support()
 *
 * Planner support function for array_append()
 */
pub unsafe fn array_append_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let mut ret: *mut Node = std::ptr::null_mut();

    if isa_support_request_modify_in_place(rawreq) {
        /*
         * We can optimize in-place appends if the function's array argument
         * is the array being assigned to.  We don't need to worry about array
         * references within the other argument.
         */
        let req = rawreq as *mut SupportRequestModifyInPlace;
        let arg = linitial((*req).args) as *mut Param;

        if !arg.is_null()
            && IsA!(arg, T_Param)
            && (*arg).paramkind == ParamKind::PARAM_EXTERN
            && (*arg).paramid == (*req).paramid
        {
            ret = arg as *mut Node;
        }
    }

    PG_RETURN_POINTER!(ret);
}

/*-----------------------------------------------------------------------------
 * array_prepend :
 *		push an element onto the front of a one-dimensional array
 *----------------------------------------------------------------------------
 */
pub unsafe fn array_prepend(fcinfo: FunctionCallInfo) -> Datum {
    let eah: *mut ExpandedArrayHeader;
    let newelem: Datum;
    let isNull: bool;
    let result: Datum;
    let lb: *mut c_int;
    let indx: c_int;
    let lb0: c_int;
    let my_extra: *mut ArrayMetaState;

    isNull = PG_ARGISNULL!(fcinfo, 0);
    if isNull {
        newelem = 0 as Datum;
    } else {
        newelem = PG_GETARG_DATUM!(fcinfo, 0);
    }
    eah = fetch_array_arg_replace_nulls(fcinfo, 1);

    if (*eah).ndims == 1 {
        /* prepend newelem */
        lb = (*eah).lbound;
        lb0 = *lb.add(0);

        let mut indx_tmp: c_int = 0;
        if pg_sub_s32_overflow(lb0, 1, &raw mut indx_tmp) {
            ereport!(
                ERROR,
                errmsg!("integer out of range")
            );
        }
        indx = indx_tmp;
    } else if (*eah).ndims == 0 {
        indx = 1;
        lb0 = 1;
    } else {
        ereport!(
            ERROR,
            errmsg!("argument must be empty or one-dimensional array")
        );
        indx = 0; /* unreachable */
        lb0 = 0;
    }

    /* Perform element insertion */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;

    let mut indx_arr = indx;
    result = array_set_element(
        EOHPGetRWDatum(&raw const (*eah).hdr),
        1,
        &raw mut indx_arr,
        newelem,
        isNull,
        -1,
        (*my_extra).typlen as c_int,
        (*my_extra).typbyval,
        (*my_extra).typalign,
    );

    /* Readjust result's LB to match the input's, as expected for prepend */
    Assert!(result == EOHPGetRWDatum(&raw const (*eah).hdr));
    if (*eah).ndims == 1 {
        /* This is ok whether we've deconstructed or not */
        *(*eah).lbound.add(0) = lb0;
    }

    PG_RETURN_DATUM!(result);
}

/*
 * array_prepend_support()
 *
 * Planner support function for array_prepend()
 */
pub unsafe fn array_prepend_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let mut ret: *mut Node = std::ptr::null_mut();

    if isa_support_request_modify_in_place(rawreq) {
        /*
         * We can optimize in-place prepends if the function's array argument
         * is the array being assigned to.  We don't need to worry about array
         * references within the other argument.
         */
        let req = rawreq as *mut SupportRequestModifyInPlace;
        let arg = lsecond((*req).args) as *mut Param;

        if !arg.is_null()
            && IsA!(arg, T_Param)
            && (*arg).paramkind == ParamKind::PARAM_EXTERN
            && (*arg).paramid == (*req).paramid
        {
            ret = arg as *mut Node;
        }
    }

    PG_RETURN_POINTER!(ret);
}

/*-----------------------------------------------------------------------------
 * array_cat :
 *		concatenate two nD arrays to form an nD array, or
 *		push an (n-1)D array onto the end of an nD array
 *----------------------------------------------------------------------------
 */
pub unsafe fn array_cat(fcinfo: FunctionCallInfo) -> Datum {
    let v1: *mut ArrayType;
    let v2: *mut ArrayType;
    let result: *mut ArrayType;
    let dims: *mut c_int;
    let lbs: *mut c_int;
    let ndims: c_int;
    let nitems: c_int;
    let ndatabytes: c_int;
    let nbytes: c_int;
    let dims1: *mut c_int;
    let lbs1: *mut c_int;
    let ndims1: c_int;
    let nitems1: c_int;
    let ndatabytes1: c_int;
    let dims2: *mut c_int;
    let lbs2: *mut c_int;
    let ndims2: c_int;
    let nitems2: c_int;
    let ndatabytes2: c_int;
    let mut i: c_int;
    let dat1: *mut c_char;
    let dat2: *mut c_char;
    let bitmap1: *mut bits8;
    let bitmap2: *mut bits8;
    let element_type: Oid;
    let element_type1: Oid;
    let element_type2: Oid;
    let dataoffset: int32;

    /* Concatenating a null array is a no-op, just return the other input */
    if PG_ARGISNULL!(fcinfo, 0) {
        if PG_ARGISNULL!(fcinfo, 1) {
            PG_RETURN_NULL!(fcinfo);
        }
        let result0 = PG_GETARG_ARRAYTYPE_P!(fcinfo, 1);
        PG_RETURN_ARRAYTYPE_P!(result0);
    }
    if PG_ARGISNULL!(fcinfo, 1) {
        let result0 = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
        PG_RETURN_ARRAYTYPE_P!(result0);
    }

    v1 = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    v2 = PG_GETARG_ARRAYTYPE_P!(fcinfo, 1);

    element_type1 = ARR_ELEMTYPE(v1);
    element_type2 = ARR_ELEMTYPE(v2);

    /* Check we have matching element types */
    if element_type1 != element_type2 {
        ereport!(
            ERROR,
            errmsg!("cannot concatenate incompatible arrays")
        );
    }

    /* OK, use it */
    element_type = element_type1;

    /*----------
     * We must have one of the following combinations of inputs:
     * 1) one empty array, and one non-empty array
     * 2) both arrays empty
     * 3) two arrays with ndims1 == ndims2
     * 4) ndims1 == ndims2 - 1
     * 5) ndims1 == ndims2 + 1
     *----------
     */
    ndims1 = ARR_NDIM(v1);
    ndims2 = ARR_NDIM(v2);

    /*
     * short circuit - if one input array is empty, and the other is not, we
     * return the non-empty one as the result
     *
     * if both are empty, return the first one
     */
    if ndims1 == 0 && ndims2 > 0 {
        PG_RETURN_ARRAYTYPE_P!(v2);
    }

    if ndims2 == 0 {
        PG_RETURN_ARRAYTYPE_P!(v1);
    }

    /* the rest fall under rule 3, 4, or 5 */
    if ndims1 != ndims2 && ndims1 != ndims2 - 1 && ndims1 != ndims2 + 1 {
        ereport!(
            ERROR,
            errmsg!("cannot concatenate incompatible arrays")
        );
    }

    /* get argument array details */
    lbs1 = ARR_LBOUND(v1);
    lbs2 = ARR_LBOUND(v2);
    dims1 = ARR_DIMS(v1);
    dims2 = ARR_DIMS(v2);
    dat1 = ARR_DATA_PTR(v1);
    dat2 = ARR_DATA_PTR(v2);
    bitmap1 = ARR_NULLBITMAP(v1);
    bitmap2 = ARR_NULLBITMAP(v2);
    nitems1 = ArrayGetNItems(ndims1, dims1);
    nitems2 = ArrayGetNItems(ndims2, dims2);
    ndatabytes1 = ARR_SIZE(v1) as c_int - ARR_DATA_OFFSET(v1) as c_int;
    ndatabytes2 = ARR_SIZE(v2) as c_int - ARR_DATA_OFFSET(v2) as c_int;

    if ndims1 == ndims2 {
        /*
         * resulting array is made up of the elements (possibly arrays
         * themselves) of the input argument arrays
         */
        ndims = ndims1;
        dims = palloc(ndims as usize * core::mem::size_of::<c_int>()) as *mut c_int;
        lbs = palloc(ndims as usize * core::mem::size_of::<c_int>()) as *mut c_int;

        *dims.add(0) = *dims1.add(0) + *dims2.add(0);
        *lbs.add(0) = *lbs1.add(0);

        i = 1;
        while i < ndims {
            if *dims1.add(i as usize) != *dims2.add(i as usize)
                || *lbs1.add(i as usize) != *lbs2.add(i as usize)
            {
                ereport!(
                    ERROR,
                    errmsg!("cannot concatenate incompatible arrays")
                );
            }

            *dims.add(i as usize) = *dims1.add(i as usize);
            *lbs.add(i as usize) = *lbs1.add(i as usize);
            i += 1;
        }
    } else if ndims1 == ndims2 - 1 {
        /*
         * resulting array has the second argument as the outer array, with
         * the first argument inserted at the front of the outer dimension
         */
        ndims = ndims2;
        dims = palloc(ndims as usize * core::mem::size_of::<c_int>()) as *mut c_int;
        lbs = palloc(ndims as usize * core::mem::size_of::<c_int>()) as *mut c_int;
        memcpy(
            dims as *mut c_void,
            dims2 as *const c_void,
            ndims as usize * core::mem::size_of::<c_int>(),
        );
        memcpy(
            lbs as *mut c_void,
            lbs2 as *const c_void,
            ndims as usize * core::mem::size_of::<c_int>(),
        );

        /* increment number of elements in outer array */
        *dims.add(0) += 1;

        /* make sure the added element matches our existing elements */
        i = 0;
        while i < ndims1 {
            if *dims1.add(i as usize) != *dims.add(i as usize + 1)
                || *lbs1.add(i as usize) != *lbs.add(i as usize + 1)
            {
                ereport!(
                    ERROR,
                    errmsg!("cannot concatenate incompatible arrays")
                );
            }
            i += 1;
        }
    } else {
        /*
         * (ndims1 == ndims2 + 1)
         *
         * resulting array has the first argument as the outer array, with the
         * second argument appended to the end of the outer dimension
         */
        ndims = ndims1;
        dims = palloc(ndims as usize * core::mem::size_of::<c_int>()) as *mut c_int;
        lbs = palloc(ndims as usize * core::mem::size_of::<c_int>()) as *mut c_int;
        memcpy(
            dims as *mut c_void,
            dims1 as *const c_void,
            ndims as usize * core::mem::size_of::<c_int>(),
        );
        memcpy(
            lbs as *mut c_void,
            lbs1 as *const c_void,
            ndims as usize * core::mem::size_of::<c_int>(),
        );

        /* increment number of elements in outer array */
        *dims.add(0) += 1;

        /* make sure the added element matches our existing elements */
        i = 0;
        while i < ndims2 {
            if *dims2.add(i as usize) != *dims.add(i as usize + 1)
                || *lbs2.add(i as usize) != *lbs.add(i as usize + 1)
            {
                ereport!(
                    ERROR,
                    errmsg!("cannot concatenate incompatible arrays")
                );
            }
            i += 1;
        }
    }

    /* Do this mainly for overflow checking */
    nitems = ArrayGetNItems(ndims, dims);
    ArrayCheckBounds(ndims, dims, lbs);

    /* build the result array */
    ndatabytes = ndatabytes1 + ndatabytes2;
    if ARR_HASNULL(v1) || ARR_HASNULL(v2) {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nitems) as int32;
        nbytes = ndatabytes + dataoffset;
    } else {
        dataoffset = 0; /* marker for no null bitmap */
        nbytes = ndatabytes + ARR_OVERHEAD_NONULLS(ndims) as int32;
    }
    result = palloc0(nbytes as usize) as *mut ArrayType;
    SET_VARSIZE(result as *mut c_char, nbytes as int32);
    (*result).ndim = ndims;
    (*result).dataoffset = dataoffset;
    (*result).elemtype = element_type;
    memcpy(
        ARR_DIMS(result) as *mut c_void,
        dims as *const c_void,
        ndims as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        ARR_LBOUND(result) as *mut c_void,
        lbs as *const c_void,
        ndims as usize * core::mem::size_of::<c_int>(),
    );
    /* data area is arg1 then arg2 */
    memcpy(
        ARR_DATA_PTR(result) as *mut c_void,
        dat1 as *const c_void,
        ndatabytes1 as usize,
    );
    memcpy(
        ARR_DATA_PTR(result).add(ndatabytes1 as usize) as *mut c_void,
        dat2 as *const c_void,
        ndatabytes2 as usize,
    );
    /* handle the null bitmap if needed */
    if ARR_HASNULL(result) {
        array_bitmap_copy(ARR_NULLBITMAP(result), 0, bitmap1, 0, nitems1);
        array_bitmap_copy(ARR_NULLBITMAP(result), nitems1, bitmap2, 0, nitems2);
    }

    PG_RETURN_ARRAYTYPE_P!(result);
}

/*
 * ARRAY_AGG(anynonarray) aggregate function
 */
pub unsafe fn array_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let arg1_typeid: Oid = get_fn_expr_argtype((*fcinfo).flinfo, 1);
    let mut aggcontext: MemoryContext = std::ptr::null_mut();
    let mut state: *mut ArrayBuildState;
    let elem: Datum;

    if arg1_typeid == InvalidOid {
        ereport!(
            ERROR,
            errmsg!("could not determine input data type")
        );
    }

    /*
     * Note: we do not need a run-time check about whether arg1_typeid is a
     * valid array element type, because the parser would have verified that
     * while resolving the input/result types of this polymorphic aggregate.
     */

    if AggCheckCallContext(fcinfo, &raw mut aggcontext) == 0 {
        /* cannot be called directly because of internal-type argument */
        elog!(ERROR, "array_agg_transfn called in non-aggregate context");
    }

    if PG_ARGISNULL!(fcinfo, 0) {
        state = initArrayResult(arg1_typeid, aggcontext, false);
    } else {
        state = PG_GETARG_POINTER!(fcinfo, 0) as *mut ArrayBuildState;
    }

    elem = if PG_ARGISNULL!(fcinfo, 1) {
        0 as Datum
    } else {
        PG_GETARG_DATUM!(fcinfo, 1)
    };

    state = accumArrayResult(
        state,
        elem,
        PG_ARGISNULL!(fcinfo, 1),
        arg1_typeid,
        aggcontext,
    );

    /*
     * The transition type for array_agg() is declared to be "internal", which
     * is a pass-by-value type the same size as a pointer.  So we can safely
     * pass the ArrayBuildState pointer through nodeAgg.c's machinations.
     */
    PG_RETURN_POINTER!(state);
}

pub unsafe fn array_agg_combine(fcinfo: FunctionCallInfo) -> Datum {
    let mut state1: *mut ArrayBuildState;
    let state2: *mut ArrayBuildState;
    let mut agg_context: MemoryContext = std::ptr::null_mut();
    let old_context: MemoryContext;

    if AggCheckCallContext(fcinfo, &raw mut agg_context) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state1 = if PG_ARGISNULL!(fcinfo, 0) {
        std::ptr::null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut ArrayBuildState
    };
    state2 = if PG_ARGISNULL!(fcinfo, 1) {
        std::ptr::null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 1) as *mut ArrayBuildState
    };

    if state2.is_null() {
        /*
         * NULL state2 is easy, just return state1, which we know is already
         * in the agg_context
         */
        if state1.is_null() {
            PG_RETURN_NULL!(fcinfo);
        }
        PG_RETURN_POINTER!(state1);
    }

    if state1.is_null() {
        /* We must copy state2's data into the agg_context */
        state1 = initArrayResultWithSize(
            (*state2).element_type,
            agg_context,
            false,
            (*state2).alen,
        );

        old_context = MemoryContextSwitchTo(agg_context);

        let mut i: c_int = 0;
        while i < (*state2).nelems {
            if !*(*state2).dnulls.add(i as usize) {
                *(*state1).dvalues.add(i as usize) = datumCopy(
                    *(*state2).dvalues.add(i as usize),
                    (*state1).typbyval,
                    (*state1).typlen as c_int,
                );
            } else {
                *(*state1).dvalues.add(i as usize) = 0 as Datum;
            }
            i += 1;
        }

        MemoryContextSwitchTo(old_context);

        memcpy(
            (*state1).dnulls as *mut c_void,
            (*state2).dnulls as *const c_void,
            core::mem::size_of::<bool>() * (*state2).nelems as usize,
        );

        (*state1).nelems = (*state2).nelems;

        PG_RETURN_POINTER!(state1);
    } else if (*state2).nelems > 0 {
        /* We only need to combine the two states if state2 has any elements */
        let reqsize: c_int = (*state1).nelems + (*state2).nelems;
        let oldContext = MemoryContextSwitchTo((*state1).mcontext);

        Assert!((*state1).element_type == (*state2).element_type);

        /* Enlarge state1 arrays if needed */
        if (*state1).alen < reqsize {
            /* Use a power of 2 size rather than allocating just reqsize */
            (*state1).alen = pg_nextpower2_32(reqsize as uint32) as c_int;
            (*state1).dvalues = repalloc(
                (*state1).dvalues as *mut c_void,
                (*state1).alen as usize * core::mem::size_of::<Datum>(),
            ) as *mut Datum;
            (*state1).dnulls = repalloc(
                (*state1).dnulls as *mut c_void,
                (*state1).alen as usize * core::mem::size_of::<bool>(),
            ) as *mut bool;
        }

        /* Copy in the state2 elements to the end of the state1 arrays */
        let mut i: c_int = 0;
        while i < (*state2).nelems {
            if !*(*state2).dnulls.add(i as usize) {
                *(*state1).dvalues.add((i + (*state1).nelems) as usize) = datumCopy(
                    *(*state2).dvalues.add(i as usize),
                    (*state1).typbyval,
                    (*state1).typlen as c_int,
                );
            } else {
                *(*state1).dvalues.add((i + (*state1).nelems) as usize) = 0 as Datum;
            }
            i += 1;
        }

        memcpy(
            (*state1).dnulls.add((*state1).nelems as usize) as *mut c_void,
            (*state2).dnulls as *const c_void,
            core::mem::size_of::<bool>() * (*state2).nelems as usize,
        );

        (*state1).nelems = reqsize;

        MemoryContextSwitchTo(oldContext);
    }

    PG_RETURN_POINTER!(state1);
}

/*
 * array_agg_serialize
 *		Serialize ArrayBuildState into bytea.
 */
pub unsafe fn array_agg_serialize(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut ArrayBuildState;
    let mut buf: StringInfoData = core::mem::zeroed();
    let result: *mut bytea;

    /* cannot be called directly because of internal-type argument */
    Assert!(AggCheckCallContext(fcinfo, std::ptr::null_mut()) != 0);

    state = PG_GETARG_POINTER!(fcinfo, 0) as *mut ArrayBuildState;

    pq_begintypsend(&raw mut buf);

    /*
     * element_type. Putting this first is more convenient in deserialization
     */
    pq_sendint32(&raw mut buf, (*state).element_type as uint32);

    /*
     * nelems -- send first so we know how large to make the dvalues and
     * dnulls array during deserialization.
     */
    pq_sendint64(&raw mut buf, (*state).nelems as crate::c::uint64);

    /* alen can be decided during deserialization */

    /* typlen */
    pq_sendint16(&raw mut buf, (*state).typlen as crate::c::uint16);

    /* typbyval */
    pq_sendint8(&raw mut buf, (*state).typbyval as crate::c::uint8);

    /* typalign */
    pq_sendint8(&raw mut buf, (*state).typalign as crate::c::uint8);

    /* dnulls */
    pq_sendbytes(
        &raw mut buf,
        (*state).dnulls as *const c_void,
        (core::mem::size_of::<bool>() * (*state).nelems as usize) as c_int,
    );

    /*
     * dvalues.  By agreement with array_agg_deserialize, when the element
     * type is byval, we just transmit the Datum array as-is, including any
     * null elements.  For by-ref types, we must invoke the element type's
     * send function, and we skip null elements (which is why the nulls flags
     * must be sent first).
     */
    if (*state).typbyval {
        pq_sendbytes(
            &raw mut buf,
            (*state).dvalues as *const c_void,
            (core::mem::size_of::<Datum>() * (*state).nelems as usize) as c_int,
        );
    } else {
        let mut iodata: *mut SerialIOData;
        let mut i: c_int;

        /* Avoid repeat catalog lookups for typsend function */
        iodata = (*(*fcinfo).flinfo).fn_extra as *mut SerialIOData;
        if iodata.is_null() {
            let mut typsend: Oid = InvalidOid;
            let mut typisvarlena: bool = false;

            iodata = MemoryContextAlloc(
                (*(*fcinfo).flinfo).fn_mcxt,
                core::mem::size_of::<SerialIOData>(),
            ) as *mut SerialIOData;
            getTypeBinaryOutputInfo(
                (*state).element_type,
                &raw mut typsend,
                &raw mut typisvarlena,
            );
            fmgr_info_cxt(typsend, &raw mut (*iodata).typsend, (*(*fcinfo).flinfo).fn_mcxt);
            (*(*fcinfo).flinfo).fn_extra = iodata as *mut c_void;
        }

        i = 0;
        while i < (*state).nelems {
            let outputbytes: *mut bytea;

            if *(*state).dnulls.add(i as usize) {
                i += 1;
                continue;
            }
            outputbytes = SendFunctionCall(&raw mut (*iodata).typsend, *(*state).dvalues.add(i as usize));
            pq_sendint32(
                &raw mut buf,
                (VARSIZE(outputbytes as *const c_char) as c_int - VARHDRSZ) as uint32,
            );
            pq_sendbytes(
                &raw mut buf,
                VARDATA(outputbytes as *const c_char) as *const c_void,
                VARSIZE(outputbytes as *const c_char) as c_int - VARHDRSZ,
            );
            i += 1;
        }
    }

    result = pq_endtypsend(&raw mut buf);

    PG_RETURN_BYTEA_P!(result);
}

pub unsafe fn array_agg_deserialize(fcinfo: FunctionCallInfo) -> Datum {
    let sstate: *mut bytea;
    let result: *mut ArrayBuildState;
    let mut buf: StringInfoData = core::mem::zeroed();
    let element_type: Oid;
    let nelems: crate::c::int64;
    let mut temp: *const c_char;

    if AggCheckCallContext(fcinfo, std::ptr::null_mut()) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    sstate = PG_GETARG_BYTEA_PP!(fcinfo, 0);

    /*
     * Initialize a StringInfo so that we can "receive" it using the standard
     * recv-function infrastructure.
     */
    initReadOnlyStringInfo(
        &raw mut buf,
        VARDATA_ANY(sstate as *const c_char),
        VARSIZE_ANY_EXHDR(sstate as *const c_char) as c_int,
    );

    /* element_type */
    element_type = pq_getmsgint(&raw mut buf, 4) as Oid;

    /* nelems */
    nelems = pq_getmsgint64(&raw mut buf);

    /* Create output ArrayBuildState with the needed number of elements */
    result = initArrayResultWithSize(element_type, CurrentMemoryContext, false, nelems as c_int);
    (*result).nelems = nelems as c_int;

    /* typlen */
    (*result).typlen = pq_getmsgint(&raw mut buf, 2) as int16;

    /* typbyval */
    (*result).typbyval = pq_getmsgbyte(&raw mut buf) != 0;

    /* typalign */
    (*result).typalign = pq_getmsgbyte(&raw mut buf) as c_char;

    /* dnulls */
    temp = pq_getmsgbytes(&raw mut buf, (core::mem::size_of::<bool>() as crate::c::int64 * nelems) as c_int);
    memcpy(
        (*result).dnulls as *mut c_void,
        temp as *const c_void,
        core::mem::size_of::<bool>() * nelems as usize,
    );

    /* dvalues --- see comment in array_agg_serialize */
    if (*result).typbyval {
        temp = pq_getmsgbytes(
            &raw mut buf,
            (core::mem::size_of::<Datum>() as crate::c::int64 * nelems) as c_int,
        );
        memcpy(
            (*result).dvalues as *mut c_void,
            temp as *const c_void,
            core::mem::size_of::<Datum>() * nelems as usize,
        );
    } else {
        let mut iodata: *mut DeserialIOData;

        /* Avoid repeat catalog lookups for typreceive function */
        iodata = (*(*fcinfo).flinfo).fn_extra as *mut DeserialIOData;
        if iodata.is_null() {
            let mut typreceive: Oid = InvalidOid;

            iodata = MemoryContextAlloc(
                (*(*fcinfo).flinfo).fn_mcxt,
                core::mem::size_of::<DeserialIOData>(),
            ) as *mut DeserialIOData;
            getTypeBinaryInputInfo(element_type, &raw mut typreceive, &raw mut (*iodata).typioparam);
            fmgr_info_cxt(typreceive, &raw mut (*iodata).typreceive, (*(*fcinfo).flinfo).fn_mcxt);
            (*(*fcinfo).flinfo).fn_extra = iodata as *mut c_void;
        }

        let mut i: crate::c::int64 = 0;
        while i < nelems {
            let itemlen: c_int;
            let mut elem_buf: StringInfoData = core::mem::zeroed();

            if *(*result).dnulls.add(i as usize) {
                *(*result).dvalues.add(i as usize) = 0 as Datum;
                i += 1;
                continue;
            }

            itemlen = pq_getmsgint(&raw mut buf, 4) as c_int;
            if itemlen < 0 || itemlen > (buf.len - buf.cursor) {
                ereport!(
                    ERROR,
                    errmsg!("insufficient data left in message")
                );
            }

            /*
             * Rather than copying data around, we just initialize a
             * StringInfo pointing to the correct portion of the message
             * buffer.
             */
            initReadOnlyStringInfo(
                &raw mut elem_buf,
                buf.data.add(buf.cursor as usize),
                itemlen,
            );

            buf.cursor += itemlen;

            /* Now call the element's receiveproc */
            *(*result).dvalues.add(i as usize) = ReceiveFunctionCall(
                &raw mut (*iodata).typreceive,
                &raw mut elem_buf,
                (*iodata).typioparam,
                -1,
            );
            i += 1;
        }
    }

    pq_getmsgend(&raw mut buf);

    PG_RETURN_POINTER!(result);
}

pub unsafe fn array_agg_finalfn(fcinfo: FunctionCallInfo) -> Datum {
    let result: Datum;
    let state: *mut ArrayBuildState;
    let mut dims: [c_int; 1] = [0; 1];
    let mut lbs: [c_int; 1] = [0; 1];

    /* cannot be called directly because of internal-type argument */
    Assert!(AggCheckCallContext(fcinfo, std::ptr::null_mut()) != 0);

    state = if PG_ARGISNULL!(fcinfo, 0) {
        std::ptr::null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut ArrayBuildState
    };

    if state.is_null() {
        PG_RETURN_NULL!(fcinfo); /* returns null iff no input values */
    }

    dims[0] = (*state).nelems;
    lbs[0] = 1;

    /*
     * Make the result.  We cannot release the ArrayBuildState because
     * sometimes aggregate final functions are re-executed.  Rather, it is
     * nodeAgg.c's responsibility to reset the aggcontext when it's safe to do
     * so.
     */
    result = makeMdArrayResult(
        state,
        1,
        dims.as_mut_ptr(),
        lbs.as_mut_ptr(),
        CurrentMemoryContext,
        false,
    );

    PG_RETURN_DATUM!(result);
}

/*
 * ARRAY_AGG(anyarray) aggregate function
 */
pub unsafe fn array_agg_array_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let arg1_typeid: Oid = get_fn_expr_argtype((*fcinfo).flinfo, 1);
    let mut aggcontext: MemoryContext = std::ptr::null_mut();
    let mut state: *mut ArrayBuildStateArr;

    if arg1_typeid == InvalidOid {
        ereport!(
            ERROR,
            errmsg!("could not determine input data type")
        );
    }

    /*
     * Note: we do not need a run-time check about whether arg1_typeid is a
     * valid array type, because the parser would have verified that while
     * resolving the input/result types of this polymorphic aggregate.
     */

    if AggCheckCallContext(fcinfo, &raw mut aggcontext) == 0 {
        /* cannot be called directly because of internal-type argument */
        elog!(
            ERROR,
            "array_agg_array_transfn called in non-aggregate context"
        );
    }

    if PG_ARGISNULL!(fcinfo, 0) {
        state = initArrayResultArr(arg1_typeid, InvalidOid, aggcontext, false);
    } else {
        state = PG_GETARG_POINTER!(fcinfo, 0) as *mut ArrayBuildStateArr;
    }

    state = accumArrayResultArr(
        state,
        PG_GETARG_DATUM!(fcinfo, 1),
        PG_ARGISNULL!(fcinfo, 1),
        arg1_typeid,
        aggcontext,
    );

    /*
     * The transition type for array_agg() is declared to be "internal", which
     * is a pass-by-value type the same size as a pointer.  So we can safely
     * pass the ArrayBuildStateArr pointer through nodeAgg.c's machinations.
     */
    PG_RETURN_POINTER!(state);
}

pub unsafe fn array_agg_array_combine(fcinfo: FunctionCallInfo) -> Datum {
    let mut state1: *mut ArrayBuildStateArr;
    let state2: *mut ArrayBuildStateArr;
    let mut agg_context: MemoryContext = std::ptr::null_mut();
    let old_context: MemoryContext;

    if AggCheckCallContext(fcinfo, &raw mut agg_context) == 0 {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state1 = if PG_ARGISNULL!(fcinfo, 0) {
        std::ptr::null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut ArrayBuildStateArr
    };
    state2 = if PG_ARGISNULL!(fcinfo, 1) {
        std::ptr::null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 1) as *mut ArrayBuildStateArr
    };

    if state2.is_null() {
        /*
         * NULL state2 is easy, just return state1, which we know is already
         * in the agg_context
         */
        if state1.is_null() {
            PG_RETURN_NULL!(fcinfo);
        }
        PG_RETURN_POINTER!(state1);
    }

    if state1.is_null() {
        /* We must copy state2's data into the agg_context */
        old_context = MemoryContextSwitchTo(agg_context);

        state1 = initArrayResultArr((*state2).array_type, InvalidOid, agg_context, false);

        (*state1).abytes = (*state2).abytes;
        (*state1).data = palloc((*state1).abytes as usize) as *mut c_char;

        if !(*state2).nullbitmap.is_null() {
            let size: c_int = ((*state2).aitems + 7) / 8;

            (*state1).nullbitmap = palloc(size as usize) as *mut bits8;
            memcpy(
                (*state1).nullbitmap as *mut c_void,
                (*state2).nullbitmap as *const c_void,
                size as usize,
            );
        }

        memcpy(
            (*state1).data as *mut c_void,
            (*state2).data as *const c_void,
            (*state2).nbytes as usize,
        );
        (*state1).nbytes = (*state2).nbytes;
        (*state1).aitems = (*state2).aitems;
        (*state1).nitems = (*state2).nitems;
        (*state1).ndims = (*state2).ndims;
        memcpy(
            (*state1).dims.as_mut_ptr() as *mut c_void,
            (*state2).dims.as_ptr() as *const c_void,
            core::mem::size_of_val(&(*state2).dims),
        );
        memcpy(
            (*state1).lbs.as_mut_ptr() as *mut c_void,
            (*state2).lbs.as_ptr() as *const c_void,
            core::mem::size_of_val(&(*state2).lbs),
        );
        (*state1).array_type = (*state2).array_type;
        (*state1).element_type = (*state2).element_type;

        MemoryContextSwitchTo(old_context);

        PG_RETURN_POINTER!(state1);
    }
    /* We only need to combine the two states if state2 has any items */
    else if (*state2).nitems > 0 {
        let oldContext: MemoryContext;
        let reqsize: c_int = (*state1).nbytes + (*state2).nbytes;
        let mut i: c_int;

        /*
         * Check the states are compatible with each other.  Ensure we use the
         * same error messages that are listed in accumArrayResultArr so that
         * the same error is shown as would have been if we'd not used the
         * combine function for the aggregation.
         */
        if (*state1).ndims != (*state2).ndims {
            ereport!(
                ERROR,
                errmsg!("cannot accumulate arrays of different dimensionality")
            );
        }

        /* Check dimensions match ignoring the first dimension. */
        i = 1;
        while i < (*state1).ndims {
            if (*state1).dims[i as usize] != (*state2).dims[i as usize]
                || (*state1).lbs[i as usize] != (*state2).lbs[i as usize]
            {
                ereport!(
                    ERROR,
                    errmsg!("cannot accumulate arrays of different dimensionality")
                );
            }
            i += 1;
        }

        oldContext = MemoryContextSwitchTo((*state1).mcontext);

        /*
         * If there's not enough space in state1 then we'll need to reallocate
         * more.
         */
        if (*state1).abytes < reqsize {
            /* use a power of 2 size rather than allocating just reqsize */
            (*state1).abytes = pg_nextpower2_32(reqsize as uint32) as c_int;
            (*state1).data =
                repalloc((*state1).data as *mut c_void, (*state1).abytes as usize) as *mut c_char;
        }

        if !(*state2).nullbitmap.is_null() {
            let newnitems: c_int = (*state1).nitems + (*state2).nitems;

            if (*state1).nullbitmap.is_null() {
                /*
                 * First input with nulls; we must retrospectively handle any
                 * previous inputs by marking all their items non-null.
                 */
                (*state1).aitems = pg_nextpower2_32(Max(256, newnitems + 1) as uint32) as c_int;
                (*state1).nullbitmap =
                    palloc((((*state1).aitems + 7) / 8) as usize) as *mut bits8;
                array_bitmap_copy(
                    (*state1).nullbitmap,
                    0,
                    std::ptr::null(),
                    0,
                    (*state1).nitems,
                );
            } else if newnitems > (*state1).aitems {
                let newaitems: c_int = (*state1).aitems + (*state2).aitems;

                (*state1).aitems = pg_nextpower2_32(newaitems as uint32) as c_int;
                (*state1).nullbitmap = repalloc(
                    (*state1).nullbitmap as *mut c_void,
                    (((*state1).aitems + 7) / 8) as usize,
                ) as *mut bits8;
            }
            array_bitmap_copy(
                (*state1).nullbitmap,
                (*state1).nitems,
                (*state2).nullbitmap,
                0,
                (*state2).nitems,
            );
        }

        memcpy(
            (*state1).data.add((*state1).nbytes as usize) as *mut c_void,
            (*state2).data as *const c_void,
            (*state2).nbytes as usize,
        );
        (*state1).nbytes += (*state2).nbytes;
        (*state1).nitems += (*state2).nitems;

        (*state1).dims[0] += (*state2).dims[0];
        /* remaining dims already match, per test above */

        Assert!((*state1).array_type == (*state2).array_type);
        Assert!((*state1).element_type == (*state2).element_type);

        MemoryContextSwitchTo(oldContext);
    }

    PG_RETURN_POINTER!(state1);
}

/*
 * array_agg_array_serialize
 *		Serialize ArrayBuildStateArr into bytea.
 */
pub unsafe fn array_agg_array_serialize(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut ArrayBuildStateArr;
    let mut buf: StringInfoData = core::mem::zeroed();
    let result: *mut bytea;

    /* cannot be called directly because of internal-type argument */
    Assert!(AggCheckCallContext(fcinfo, std::ptr::null_mut()) != 0);

    state = PG_GETARG_POINTER!(fcinfo, 0) as *mut ArrayBuildStateArr;

    pq_begintypsend(&raw mut buf);

    /*
     * element_type. Putting this first is more convenient in deserialization
     * so that we can init the new state sooner.
     */
    pq_sendint32(&raw mut buf, (*state).element_type as uint32);

    /* array_type */
    pq_sendint32(&raw mut buf, (*state).array_type as uint32);

    /* nbytes */
    pq_sendint32(&raw mut buf, (*state).nbytes as uint32);

    /* data */
    pq_sendbytes(&raw mut buf, (*state).data as *const c_void, (*state).nbytes);

    /* abytes */
    pq_sendint32(&raw mut buf, (*state).abytes as uint32);

    /* aitems */
    pq_sendint32(&raw mut buf, (*state).aitems as uint32);

    /* nullbitmap */
    if !(*state).nullbitmap.is_null() {
        Assert!((*state).aitems > 0);
        pq_sendbytes(
            &raw mut buf,
            (*state).nullbitmap as *const c_void,
            ((*state).aitems + 7) / 8,
        );
    }

    /* nitems */
    pq_sendint32(&raw mut buf, (*state).nitems as uint32);

    /* ndims */
    pq_sendint32(&raw mut buf, (*state).ndims as uint32);

    /* dims: XXX should we just send ndims elements? */
    pq_sendbytes(
        &raw mut buf,
        (*state).dims.as_ptr() as *const c_void,
        core::mem::size_of_val(&(*state).dims) as c_int,
    );

    /* lbs */
    pq_sendbytes(
        &raw mut buf,
        (*state).lbs.as_ptr() as *const c_void,
        core::mem::size_of_val(&(*state).lbs) as c_int,
    );

    result = pq_endtypsend(&raw mut buf);

    PG_RETURN_BYTEA_P!(result);
}

pub unsafe fn array_agg_array_deserialize(fcinfo: FunctionCallInfo) -> Datum {
    let sstate: *mut bytea;
    let result: *mut ArrayBuildStateArr;
    let mut buf: StringInfoData = core::mem::zeroed();
    let element_type: Oid;
    let array_type: Oid;
    let nbytes: c_int;
    let mut temp: *const c_char;

    /* cannot be called directly because of internal-type argument */
    Assert!(AggCheckCallContext(fcinfo, std::ptr::null_mut()) != 0);

    sstate = PG_GETARG_BYTEA_PP!(fcinfo, 0);

    /*
     * Initialize a StringInfo so that we can "receive" it using the standard
     * recv-function infrastructure.
     */
    initReadOnlyStringInfo(
        &raw mut buf,
        VARDATA_ANY(sstate as *const c_char),
        VARSIZE_ANY_EXHDR(sstate as *const c_char) as c_int,
    );

    /* element_type */
    element_type = pq_getmsgint(&raw mut buf, 4) as Oid;

    /* array_type */
    array_type = pq_getmsgint(&raw mut buf, 4) as Oid;

    /* nbytes */
    nbytes = pq_getmsgint(&raw mut buf, 4) as c_int;

    result = initArrayResultArr(array_type, element_type, CurrentMemoryContext, false);

    (*result).abytes = 1024;
    while (*result).abytes < nbytes {
        (*result).abytes *= 2;
    }

    (*result).data = palloc((*result).abytes as usize) as *mut c_char;

    /* data */
    temp = pq_getmsgbytes(&raw mut buf, nbytes);
    memcpy(
        (*result).data as *mut c_void,
        temp as *const c_void,
        nbytes as usize,
    );
    (*result).nbytes = nbytes;

    /* abytes */
    (*result).abytes = pq_getmsgint(&raw mut buf, 4) as c_int;

    /* aitems: might be 0 */
    (*result).aitems = pq_getmsgint(&raw mut buf, 4) as c_int;

    /* nullbitmap */
    if (*result).aitems > 0 {
        let size: c_int = ((*result).aitems + 7) / 8;

        (*result).nullbitmap = palloc(size as usize) as *mut bits8;
        temp = pq_getmsgbytes(&raw mut buf, size);
        memcpy(
            (*result).nullbitmap as *mut c_void,
            temp as *const c_void,
            size as usize,
        );
    } else {
        (*result).nullbitmap = std::ptr::null_mut();
    }

    /* nitems */
    (*result).nitems = pq_getmsgint(&raw mut buf, 4) as c_int;

    /* ndims */
    (*result).ndims = pq_getmsgint(&raw mut buf, 4) as c_int;

    /* dims */
    temp = pq_getmsgbytes(&raw mut buf, core::mem::size_of_val(&(*result).dims) as c_int);
    memcpy(
        (*result).dims.as_mut_ptr() as *mut c_void,
        temp as *const c_void,
        core::mem::size_of_val(&(*result).dims),
    );

    /* lbs */
    temp = pq_getmsgbytes(&raw mut buf, core::mem::size_of_val(&(*result).lbs) as c_int);
    memcpy(
        (*result).lbs.as_mut_ptr() as *mut c_void,
        temp as *const c_void,
        core::mem::size_of_val(&(*result).lbs),
    );

    pq_getmsgend(&raw mut buf);

    PG_RETURN_POINTER!(result);
}

pub unsafe fn array_agg_array_finalfn(fcinfo: FunctionCallInfo) -> Datum {
    let result: Datum;
    let state: *mut ArrayBuildStateArr;

    /* cannot be called directly because of internal-type argument */
    Assert!(AggCheckCallContext(fcinfo, std::ptr::null_mut()) != 0);

    state = if PG_ARGISNULL!(fcinfo, 0) {
        std::ptr::null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut ArrayBuildStateArr
    };

    if state.is_null() {
        PG_RETURN_NULL!(fcinfo); /* returns null iff no input values */
    }

    /*
     * Make the result.  We cannot release the ArrayBuildStateArr because
     * sometimes aggregate final functions are re-executed.  Rather, it is
     * nodeAgg.c's responsibility to reset the aggcontext when it's safe to do
     * so.
     */
    result = makeArrayResultArr(state, CurrentMemoryContext, false);

    PG_RETURN_DATUM!(result);
}

/*-----------------------------------------------------------------------------
 * array_position, array_position_start :
 *			return the offset of a value in an array.
 *
 * IS NOT DISTINCT FROM semantics are used for comparisons.  Return NULL when
 * the value is not found.
 *-----------------------------------------------------------------------------
 */
pub unsafe fn array_position(fcinfo: FunctionCallInfo) -> Datum {
    array_position_common(fcinfo)
}

pub unsafe fn array_position_start(fcinfo: FunctionCallInfo) -> Datum {
    array_position_common(fcinfo)
}

/*
 * array_position_common
 *		Common code for array_position and array_position_start
 *
 * These are separate wrappers for the sake of opr_sanity regression test.
 * They are not strict so we have to test for null inputs explicitly.
 */
unsafe fn array_position_common(fcinfo: FunctionCallInfo) -> Datum {
    let array: *mut ArrayType;
    let collation: Oid = PG_GET_COLLATION!(fcinfo);
    let element_type: Oid;
    let searched_element: Datum;
    let mut value: Datum = 0 as Datum;
    let mut isnull: bool = false;
    let mut position: c_int;
    let position_min: c_int;
    let mut found: bool = false;
    let typentry: *mut TypeCacheEntry;
    let mut my_extra: *mut ArrayMetaState;
    let null_search: bool;
    let array_iterator: ArrayIterator;

    if PG_ARGISNULL!(fcinfo, 0) {
        PG_RETURN_NULL!(fcinfo);
    }

    array = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);

    /*
     * We refuse to search for elements in multi-dimensional arrays, since we
     * have no good way to report the element's location in the array.
     */
    if ARR_NDIM(array) > 1 {
        ereport!(
            ERROR,
            errmsg!("searching for elements in multidimensional arrays is not supported")
        );
    }

    /* Searching in an empty array is well-defined, though: it always fails */
    if ARR_NDIM(array) < 1 {
        PG_RETURN_NULL!(fcinfo);
    }

    if PG_ARGISNULL!(fcinfo, 1) {
        /* fast return when the array doesn't have nulls */
        if !array_contains_nulls(array) {
            PG_RETURN_NULL!(fcinfo);
        }
        searched_element = 0 as Datum;
        null_search = true;
    } else {
        searched_element = PG_GETARG_DATUM!(fcinfo, 1);
        null_search = false;
    }

    element_type = ARR_ELEMTYPE(array);
    position = *ARR_LBOUND(array).add(0) - 1;

    /* figure out where to start */
    if PG_NARGS!(fcinfo) as c_int == 3 {
        if PG_ARGISNULL!(fcinfo, 2) {
            ereport!(
                ERROR,
                errmsg!("initial position must not be null")
            );
        }

        position_min = PG_GETARG_INT32!(fcinfo, 2);
    } else {
        position_min = *ARR_LBOUND(array).add(0);
    }

    /*
     * We arrange to look up type info for array_create_iterator only once per
     * series of calls, assuming the element type doesn't change underneath
     * us.
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
    if my_extra.is_null() {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::size_of::<ArrayMetaState>(),
        ) as *mut c_void;
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
        (*my_extra).element_type = !element_type;
    }

    if (*my_extra).element_type != element_type {
        get_typlenbyvalalign(
            element_type,
            &raw mut (*my_extra).typlen,
            &raw mut (*my_extra).typbyval,
            &raw mut (*my_extra).typalign,
        );

        typentry = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);

        if !OidIsValid((*typentry).eq_opr_finfo.fn_oid) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not identify an equality operator for type {}",
                    std::ffi::CStr::from_ptr(format_type_be(element_type)).to_string_lossy()
                )
            );
        }

        (*my_extra).element_type = element_type;
        fmgr_info_cxt(
            (*typentry).eq_opr_finfo.fn_oid,
            &raw mut (*my_extra).proc,
            (*(*fcinfo).flinfo).fn_mcxt,
        );
    }

    /* Examine each array element until we find a match. */
    array_iterator = array_create_iterator(array, 0, my_extra);
    while array_iterate(array_iterator, &raw mut value, &raw mut isnull) {
        position += 1;

        /* skip initial elements if caller requested so */
        if position < position_min {
            continue;
        }

        /*
         * Can't look at the array element's value if it's null; but if we
         * search for null, we have a hit and are done.
         */
        if isnull || null_search {
            if isnull && null_search {
                found = true;
                break;
            } else {
                continue;
            }
        }

        /* not nulls, so run the operator */
        if DatumGetBool(FunctionCall2Coll(
            &raw mut (*my_extra).proc,
            collation,
            searched_element,
            value,
        )) {
            found = true;
            break;
        }
    }

    array_free_iterator(array_iterator);

    /* Avoid leaking memory when handed toasted input */
    PG_FREE_IF_COPY!(fcinfo, array, 0);

    if !found {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_INT32!(position);
}

/*-----------------------------------------------------------------------------
 * array_positions :
 *			return an array of positions of a value in an array.
 *
 * IS NOT DISTINCT FROM semantics are used for comparisons.  Returns NULL when
 * the input array is NULL.  When the value is not found in the array, returns
 * an empty array.
 *
 * This is not strict so we have to test for null inputs explicitly.
 *-----------------------------------------------------------------------------
 */
pub unsafe fn array_positions(fcinfo: FunctionCallInfo) -> Datum {
    let array: *mut ArrayType;
    let collation: Oid = PG_GET_COLLATION!(fcinfo);
    let element_type: Oid;
    let searched_element: Datum;
    let mut value: Datum = 0 as Datum;
    let mut isnull: bool = false;
    let mut position: c_int;
    let typentry: *mut TypeCacheEntry;
    let mut my_extra: *mut ArrayMetaState;
    let null_search: bool;
    let array_iterator: ArrayIterator;
    let mut astate: *mut ArrayBuildState;

    if PG_ARGISNULL!(fcinfo, 0) {
        PG_RETURN_NULL!(fcinfo);
    }

    array = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);

    /*
     * We refuse to search for elements in multi-dimensional arrays, since we
     * have no good way to report the element's location in the array.
     */
    if ARR_NDIM(array) > 1 {
        ereport!(
            ERROR,
            errmsg!("searching for elements in multidimensional arrays is not supported")
        );
    }

    astate = initArrayResult(INT4OID, CurrentMemoryContext, false);

    /* Searching in an empty array is well-defined, though: it always fails */
    if ARR_NDIM(array) < 1 {
        PG_RETURN_DATUM!(makeArrayResult(astate, CurrentMemoryContext));
    }

    if PG_ARGISNULL!(fcinfo, 1) {
        /* fast return when the array doesn't have nulls */
        if !array_contains_nulls(array) {
            PG_RETURN_DATUM!(makeArrayResult(astate, CurrentMemoryContext));
        }
        searched_element = 0 as Datum;
        null_search = true;
    } else {
        searched_element = PG_GETARG_DATUM!(fcinfo, 1);
        null_search = false;
    }

    element_type = ARR_ELEMTYPE(array);
    position = *ARR_LBOUND(array).add(0) - 1;

    /*
     * We arrange to look up type info for array_create_iterator only once per
     * series of calls, assuming the element type doesn't change underneath
     * us.
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
    if my_extra.is_null() {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::size_of::<ArrayMetaState>(),
        ) as *mut c_void;
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
        (*my_extra).element_type = !element_type;
    }

    if (*my_extra).element_type != element_type {
        get_typlenbyvalalign(
            element_type,
            &raw mut (*my_extra).typlen,
            &raw mut (*my_extra).typbyval,
            &raw mut (*my_extra).typalign,
        );

        typentry = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);

        if !OidIsValid((*typentry).eq_opr_finfo.fn_oid) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not identify an equality operator for type {}",
                    std::ffi::CStr::from_ptr(format_type_be(element_type)).to_string_lossy()
                )
            );
        }

        (*my_extra).element_type = element_type;
        fmgr_info_cxt(
            (*typentry).eq_opr_finfo.fn_oid,
            &raw mut (*my_extra).proc,
            (*(*fcinfo).flinfo).fn_mcxt,
        );
    }

    /*
     * Accumulate each array position iff the element matches the given
     * element.
     */
    array_iterator = array_create_iterator(array, 0, my_extra);
    while array_iterate(array_iterator, &raw mut value, &raw mut isnull) {
        position += 1;

        /*
         * Can't look at the array element's value if it's null; but if we
         * search for null, we have a hit.
         */
        if isnull || null_search {
            if isnull && null_search {
                astate = accumArrayResult(
                    astate,
                    Int32GetDatum(position),
                    false,
                    INT4OID,
                    CurrentMemoryContext,
                );
            }

            continue;
        }

        /* not nulls, so run the operator */
        if DatumGetBool(FunctionCall2Coll(
            &raw mut (*my_extra).proc,
            collation,
            searched_element,
            value,
        )) {
            astate = accumArrayResult(
                astate,
                Int32GetDatum(position),
                false,
                INT4OID,
                CurrentMemoryContext,
            );
        }
    }

    array_free_iterator(array_iterator);

    /* Avoid leaking memory when handed toasted input */
    PG_FREE_IF_COPY!(fcinfo, array, 0);

    PG_RETURN_DATUM!(makeArrayResult(astate, CurrentMemoryContext));
}

/*
 * array_shuffle_n
 *		Return a copy of array with n randomly chosen items.
 *
 * The number of items must not exceed the size of the first dimension of the
 * array.  We preserve the first dimension's lower bound if keep_lb,
 * else it's set to 1.  Lower-order dimensions are preserved in any case.
 *
 * NOTE: it would be cleaner to look up the elmlen/elmbval/elmalign info
 * from the system catalogs, given only the elmtyp. However, the caller is
 * in a better position to cache this info across multiple calls.
 */
unsafe fn array_shuffle_n(
    array: *mut ArrayType,
    n: c_int,
    keep_lb: bool,
    elmtyp: Oid,
    typentry: *mut TypeCacheEntry,
) -> *mut ArrayType {
    let result: *mut ArrayType;
    let ndim: c_int;
    let dims: *mut c_int;
    let lbs: *mut c_int;
    let mut nelm: c_int = 0;
    let nitem: c_int;
    let mut rdims: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut rlbs: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let elmlen: int16;
    let elmbyval: bool;
    let elmalign: c_char;
    let mut elms: *mut Datum = std::ptr::null_mut();
    let mut ielms: *mut Datum;
    let mut nuls: *mut bool = std::ptr::null_mut();
    let mut inuls: *mut bool;

    ndim = ARR_NDIM(array);
    dims = ARR_DIMS(array);
    lbs = ARR_LBOUND(array);

    elmlen = (*typentry).typlen;
    elmbyval = (*typentry).typbyval;
    elmalign = (*typentry).typalign;

    /* If the target array is empty, exit fast */
    if ndim < 1 || *dims.add(0) < 1 || n < 1 {
        return construct_empty_array(elmtyp);
    }

    deconstruct_array(
        array,
        elmtyp,
        elmlen as c_int,
        elmbyval,
        elmalign,
        &raw mut elms,
        &raw mut nuls,
        &raw mut nelm,
    );

    nitem = *dims.add(0); /* total number of items */
    nelm /= nitem; /* number of elements per item */

    Assert!(n <= nitem); /* else it's caller error */

    /*
     * Shuffle array using Fisher-Yates algorithm.  Scan the array and swap
     * current item (nelm datums starting at ielms) with a randomly chosen
     * later item (nelm datums starting at jelms) in each iteration.  We can
     * stop once we've done n iterations; then first n items are the result.
     */
    ielms = elms;
    inuls = nuls;
    for i in 0..n {
        let j: c_int = pg_prng_uint64_range(
            &raw mut pg_global_prng_state,
            i as crate::c::uint64,
            (nitem - 1) as crate::c::uint64,
        ) as c_int
            * nelm;
        let mut jelms: *mut Datum = elms.add(j as usize);
        let mut jnuls: *mut bool = nuls.add(j as usize);

        /* Swap i'th and j'th items; advance ielms/inuls to next item */
        for _k in 0..nelm {
            let elm: Datum = *ielms;
            let nul: bool = *inuls;

            *ielms = *jelms;
            ielms = ielms.add(1);
            *inuls = *jnuls;
            inuls = inuls.add(1);
            *jelms = elm;
            jelms = jelms.add(1);
            *jnuls = nul;
            jnuls = jnuls.add(1);
        }
    }

    /* Set up dimensions of the result */
    memcpy(
        rdims.as_mut_ptr() as *mut c_void,
        dims as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        rlbs.as_mut_ptr() as *mut c_void,
        lbs as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    rdims[0] = n;
    if !keep_lb {
        rlbs[0] = 1;
    }

    result = construct_md_array(
        elms,
        nuls,
        ndim,
        rdims.as_mut_ptr(),
        rlbs.as_mut_ptr(),
        elmtyp,
        elmlen as c_int,
        elmbyval,
        elmalign,
    );

    pfree(elms as *mut c_void);
    pfree(nuls as *mut c_void);

    result
}

/*
 * array_shuffle
 *
 * Returns an array with the same dimensions as the input array, with its
 * first-dimension elements in random order.
 */
pub unsafe fn array_shuffle(fcinfo: FunctionCallInfo) -> Datum {
    let array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    let result: *mut ArrayType;
    let elmtyp: Oid;
    let mut typentry: *mut TypeCacheEntry;

    /*
     * There is no point in shuffling empty arrays or arrays with less than
     * two items.
     */
    if ARR_NDIM(array) < 1 || *ARR_DIMS(array).add(0) < 2 {
        PG_RETURN_ARRAYTYPE_P!(array);
    }

    elmtyp = ARR_ELEMTYPE(array);
    typentry = (*(*fcinfo).flinfo).fn_extra as *mut TypeCacheEntry;
    if typentry.is_null() || (*typentry).type_id != elmtyp {
        typentry = lookup_type_cache(elmtyp, 0);
        (*(*fcinfo).flinfo).fn_extra = typentry as *mut c_void;
    }

    result = array_shuffle_n(array, *ARR_DIMS(array).add(0), true, elmtyp, typentry);

    PG_RETURN_ARRAYTYPE_P!(result);
}

/*
 * array_sample
 *
 * Returns an array of n randomly chosen first-dimension elements
 * from the input array.
 */
pub unsafe fn array_sample(fcinfo: FunctionCallInfo) -> Datum {
    let array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    let n: c_int = PG_GETARG_INT32!(fcinfo, 1);
    let result: *mut ArrayType;
    let elmtyp: Oid;
    let mut typentry: *mut TypeCacheEntry;
    let nitem: c_int;

    nitem = if ARR_NDIM(array) < 1 {
        0
    } else {
        *ARR_DIMS(array).add(0)
    };

    if n < 0 || n > nitem {
        ereport!(
            ERROR,
            errmsg!("sample size must be between 0 and {}", nitem)
        );
    }

    elmtyp = ARR_ELEMTYPE(array);
    typentry = (*(*fcinfo).flinfo).fn_extra as *mut TypeCacheEntry;
    if typentry.is_null() || (*typentry).type_id != elmtyp {
        typentry = lookup_type_cache(elmtyp, 0);
        (*(*fcinfo).flinfo).fn_extra = typentry as *mut c_void;
    }

    result = array_shuffle_n(array, n, false, elmtyp, typentry);

    PG_RETURN_ARRAYTYPE_P!(result);
}

/*
 * array_reverse_n
 *		Return a copy of array with reversed items.
 *
 * NOTE: it would be cleaner to look up the elmlen/elmbval/elmalign info
 * from the system catalogs, given only the elmtyp. However, the caller is
 * in a better position to cache this info across multiple calls.
 */
unsafe fn array_reverse_n(
    array: *mut ArrayType,
    elmtyp: Oid,
    typentry: *mut TypeCacheEntry,
) -> *mut ArrayType {
    let result: *mut ArrayType;
    let ndim: c_int;
    let dims: *mut c_int;
    let lbs: *mut c_int;
    let mut nelm: c_int = 0;
    let nitem: c_int;
    let mut rdims: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut rlbs: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let elmlen: int16;
    let elmbyval: bool;
    let elmalign: c_char;
    let mut elms: *mut Datum = std::ptr::null_mut();
    let mut ielms: *mut Datum;
    let mut nuls: *mut bool = std::ptr::null_mut();
    let mut inuls: *mut bool;

    ndim = ARR_NDIM(array);
    dims = ARR_DIMS(array);
    lbs = ARR_LBOUND(array);

    elmlen = (*typentry).typlen;
    elmbyval = (*typentry).typbyval;
    elmalign = (*typentry).typalign;

    deconstruct_array(
        array,
        elmtyp,
        elmlen as c_int,
        elmbyval,
        elmalign,
        &raw mut elms,
        &raw mut nuls,
        &raw mut nelm,
    );

    nitem = *dims.add(0); /* total number of items */
    nelm /= nitem; /* number of elements per item */

    /* Reverse the array */
    ielms = elms;
    inuls = nuls;
    for i in 0..(nitem / 2) {
        let j: c_int = (nitem - i - 1) * nelm;
        let mut jelms: *mut Datum = elms.add(j as usize);
        let mut jnuls: *mut bool = nuls.add(j as usize);

        /* Swap i'th and j'th items; advance ielms/inuls to next item */
        for _k in 0..nelm {
            let elm: Datum = *ielms;
            let nul: bool = *inuls;

            *ielms = *jelms;
            ielms = ielms.add(1);
            *inuls = *jnuls;
            inuls = inuls.add(1);
            *jelms = elm;
            jelms = jelms.add(1);
            *jnuls = nul;
            jnuls = jnuls.add(1);
        }
    }

    /* Set up dimensions of the result */
    memcpy(
        rdims.as_mut_ptr() as *mut c_void,
        dims as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        rlbs.as_mut_ptr() as *mut c_void,
        lbs as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    rdims[0] = nitem;

    result = construct_md_array(
        elms,
        nuls,
        ndim,
        rdims.as_mut_ptr(),
        rlbs.as_mut_ptr(),
        elmtyp,
        elmlen as c_int,
        elmbyval,
        elmalign,
    );

    pfree(elms as *mut c_void);
    pfree(nuls as *mut c_void);

    result
}

/*
 * array_reverse
 *
 * Returns an array with the same dimensions as the input array, with its
 * first-dimension elements in reverse order.
 */
pub unsafe fn array_reverse(fcinfo: FunctionCallInfo) -> Datum {
    let array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    let result: *mut ArrayType;
    let elmtyp: Oid;
    let mut typentry: *mut TypeCacheEntry;

    /*
     * There is no point in reversing empty arrays or arrays with less than
     * two items.
     */
    if ARR_NDIM(array) < 1 || *ARR_DIMS(array).add(0) < 2 {
        PG_RETURN_ARRAYTYPE_P!(array);
    }

    elmtyp = ARR_ELEMTYPE(array);
    typentry = (*(*fcinfo).flinfo).fn_extra as *mut TypeCacheEntry;
    if typentry.is_null() || (*typentry).type_id != elmtyp {
        typentry = lookup_type_cache(elmtyp, 0);
        (*(*fcinfo).flinfo).fn_extra = typentry as *mut c_void;
    }

    result = array_reverse_n(array, elmtyp, typentry);

    PG_RETURN_ARRAYTYPE_P!(result);
}

/*
 * array_sort
 *
 * Sorts the first dimension of the array.
 */
unsafe fn array_sort_internal(
    array: *mut ArrayType,
    descending: bool,
    nulls_first: bool,
    fcinfo: FunctionCallInfo,
) -> *mut ArrayType {
    let newarray: *mut ArrayType;
    let collation: Oid = PG_GET_COLLATION!(fcinfo);
    let ndim: c_int;
    let dims: *mut c_int;
    let lbs: *mut c_int;
    let mut cache_info: *mut ArraySortCachedInfo;
    let elmtyp: Oid;
    let sort_typ: Oid;
    let sort_opr: Oid;
    let tuplesortstate: *mut Tuplesortstate;
    let array_iterator: ArrayIterator;
    let mut value: Datum = 0 as Datum;
    let mut isnull: bool = false;
    let mut astate: *mut ArrayBuildStateAny = std::ptr::null_mut();

    ndim = ARR_NDIM(array);
    dims = ARR_DIMS(array);
    lbs = ARR_LBOUND(array);

    /* Quick exit if we don't need to sort */
    if ndim < 1 || *dims.add(0) < 2 {
        return array;
    }

    /* Set up cache area if we didn't already */
    cache_info = (*(*fcinfo).flinfo).fn_extra as *mut ArraySortCachedInfo;
    if cache_info.is_null() {
        cache_info = MemoryContextAllocZero(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::size_of::<ArraySortCachedInfo>(),
        ) as *mut ArraySortCachedInfo;
        (*(*fcinfo).flinfo).fn_extra = cache_info as *mut c_void;
    }

    /* Fetch and cache required data if we don't have it */
    elmtyp = ARR_ELEMTYPE(array);
    if elmtyp != (*cache_info).array_meta.element_type {
        let typentry: *mut TypeCacheEntry;

        typentry = lookup_type_cache(elmtyp, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
        (*cache_info).array_meta.element_type = elmtyp;
        (*cache_info).array_meta.typlen = (*typentry).typlen;
        (*cache_info).array_meta.typbyval = (*typentry).typbyval;
        (*cache_info).array_meta.typalign = (*typentry).typalign;
        (*cache_info).elem_lt_opr = (*typentry).lt_opr;
        (*cache_info).elem_gt_opr = (*typentry).gt_opr;
        (*cache_info).array_type = (*typentry).typarray;
    }

    /* Identify the sort operator to use */
    if ndim == 1 {
        /* Need to sort the element type */
        sort_typ = elmtyp;
        sort_opr = if descending {
            (*cache_info).elem_gt_opr
        } else {
            (*cache_info).elem_lt_opr
        };
    } else {
        /* Otherwise we're sorting arrays */
        sort_typ = (*cache_info).array_type;
        if !OidIsValid(sort_typ) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not find array type for data type {}",
                    std::ffi::CStr::from_ptr(format_type_be(elmtyp)).to_string_lossy()
                )
            );
        }
        /* We know what operators to use for arrays */
        sort_opr = if descending { ARRAY_GT_OP } else { ARRAY_LT_OP };
    }

    /*
     * Fail if we don't know how to sort.  The error message is chosen to
     * match what array_lt()/array_gt() will say in the multidimensional case.
     */
    if !OidIsValid(sort_opr) {
        ereport!(
            ERROR,
            errmsg!(
                "could not identify a comparison function for type {}",
                std::ffi::CStr::from_ptr(format_type_be(elmtyp)).to_string_lossy()
            )
        );
    }

    /* Put the things to be sorted (elements or sub-arrays) into a tuplesort */
    tuplesortstate = tuplesort_begin_datum(
        sort_typ,
        sort_opr,
        collation,
        nulls_first,
        work_mem,
        std::ptr::null_mut(),
        TUPLESORT_NONE,
    );

    array_iterator = array_create_iterator(array, ndim - 1, &raw mut (*cache_info).array_meta);
    while array_iterate(array_iterator, &raw mut value, &raw mut isnull) {
        tuplesort_putdatum(tuplesortstate, value, isnull);
    }
    array_free_iterator(array_iterator);

    /* Do the sort */
    tuplesort_performsort(tuplesortstate);

    /* Extract results into a new array */
    while tuplesort_getdatum(
        tuplesortstate,
        true,
        false,
        &raw mut value,
        &raw mut isnull,
        std::ptr::null_mut(),
    ) {
        astate = accumArrayResultAny(astate, value, isnull, sort_typ, CurrentMemoryContext);
    }
    tuplesort_end(tuplesortstate);

    newarray = DatumGetArrayTypeP(makeArrayResultAny(astate, CurrentMemoryContext, true));

    /* Adjust lower bound to match the input */
    *ARR_LBOUND(newarray).add(0) = *lbs.add(0);

    newarray
}

pub unsafe fn array_sort(fcinfo: FunctionCallInfo) -> Datum {
    let array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);

    PG_RETURN_ARRAYTYPE_P!(array_sort_internal(array, false, false, fcinfo));
}

pub unsafe fn array_sort_order(fcinfo: FunctionCallInfo) -> Datum {
    let array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    let descending: bool = PG_GETARG_BOOL!(fcinfo, 1);

    PG_RETURN_ARRAYTYPE_P!(array_sort_internal(array, descending, descending, fcinfo));
}

pub unsafe fn array_sort_order_nulls_first(fcinfo: FunctionCallInfo) -> Datum {
    let array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    let descending: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let nulls_first: bool = PG_GETARG_BOOL!(fcinfo, 2);

    PG_RETURN_ARRAYTYPE_P!(array_sort_internal(array, descending, nulls_first, fcinfo));
}
