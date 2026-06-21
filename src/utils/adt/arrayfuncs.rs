//! arrayfuncs.rs
//!   Support functions for arrays.
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/arrayfuncs.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/arrayfuncs.c

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_assignments)]
#![allow(unused_variables)]
#![deny(dangerous_implicit_autorefs)]

// #include "postgres.h"
use crate::prelude::*;
use crate::nodes::nodes::Node;
use crate::c::Pointer;

// catalog/pg_type.h
use crate::catalog::pg_type_d::{
    CHAROID, CSTRINGOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID, NAMEOID, OIDOID,
    RECORDOID, REGTYPEOID, TEXTOID, TIDOID, XIDOID,
};
// catalog/catalog.h (FirstGenbkiObjectId)
use crate::catalog::catalog::FirstGenbkiObjectId;

// pg_config_manual.h
use crate::pg_config::NAMEDATALEN;
// storage/itemptr.h
use crate::storage::itemptr::ItemPointerData;

// common/int.h
use crate::common::int::{pg_add_s32_overflow, pg_sub_s32_overflow};

// libpq/pqformat.h
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgint, pq_sendbytes, pq_sendint32,
};

// nodes/supportnodes.h
use crate::nodes::supportnodes::SupportRequestRows;
// nodes/primnodes.h
use crate::nodes::primnodes::FuncExpr;
// nodes/pg_list.h
use crate::nodes::pg_list::{linitial, List};
// nodes/execnodes.h
use crate::nodes::execnodes::{ExprContext, ExprState};

// optimizer/optimizer.h
use crate::optimizer::optimizer::estimate_expression_value;

// parser/scansup.h
use crate::parser::scansup::scanner_isspace;

// port/pg_bitutils.h
use crate::port::pg_bitutils::pg_nextpower2_32;
// port/pgstrcasecmp
use crate::port::pgstrcasecmp::pg_strcasecmp;

// lib/stringinfo.h
use crate::lib::stringinfo::{
    appendStringInfoChar, initReadOnlyStringInfo, initStringInfo, resetStringInfo, StringInfo,
    StringInfoData,
};

// access/tupmacs.h
use crate::access::tupmacs::{
    att_addlength_datum, att_addlength_pointer, att_align_nominal, fetch_att, store_att_byval,
};

// utils/array.h
use crate::utils::array::{
    ArrayType, ARR_DATA_OFFSET, ARR_DATA_PTR, ARR_DIMS, ARR_ELEMTYPE, ARR_HASNULL, ARR_LBOUND,
    ARR_NDIM, ARR_NULLBITMAP, ARR_OVERHEAD_NONULLS, ARR_OVERHEAD_WITHNULLS, ARR_SIZE, MaxArraySize,
};
// utils/array.h: shared metadata + expanded-array structs live in array_expanded.
use crate::utils::adt::array_expanded::{
    deconstruct_expanded_array, expand_array, AnyArrayType, ArrayMetaState, DatumGetAnyArrayP,
    DatumGetExpandedArray, ExpandedArrayHeader,
};
// utils/arrayaccess.h
use crate::utils::arrayaccess::{array_iter, array_iter_next, array_iter_setup};
// utils/adt/arrayutils.c
use crate::utils::adt::arrayutils::{
    mda_get_offset_values, mda_get_prod, mda_get_range, mda_next_tuple, ArrayCheckBounds,
    ArrayGetNItems, ArrayGetOffset,
};

// utils/builtins.h
use crate::utils::builtins::{cstring_to_text, format_type_be, format_type_extended};

// utils/datum.h
use crate::utils::adt::datum::datumCopy;

// utils/expandeddatum.h
use crate::utils::adt::expandeddatum::{
    DatumGetEOHP, EOHPGetRWDatum, VARATT_IS_EXPANDED_HEADER,
};

// utils/fmgr.h / fmgr.c
use crate::utils::fmgr::{
    fmgr_info, fmgr_info_cxt, get_fn_expr_argtype, FmgrInfo, FunctionCallInfo, OutputFunctionCall,
    ReceiveFunctionCall, SendFunctionCall,
};

// utils/memutils.h
use crate::utils::memutils::AllocSizeIsValid;

// varatt.h
use crate::varatt::{SET_VARSIZE, VARDATA, VARSIZE};

use crate::c::{bits8, bytea, float8, int16, int32, uint32, uint64, Size};
use crate::postgres::{
    DatumGetBool, DatumGetFloat8, DatumGetInt32, DatumGetInt64, DatumGetPointer, DatumGetUInt32,
    DatumGetUInt64, Int32GetDatum, Int64GetDatum, NullableDatum, PointerGetDatum,
};

use crate::{
    DirectFunctionCall2, FunctionCallInvoke, InitFunctionCallInfoData, IsA, LOCAL_FCINFO,
    PG_ARGISNULL, PG_FREE_IF_COPY, PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_DATUM,
    PG_GETARG_INT32, PG_GETARG_INT64, PG_GETARG_OID, PG_GETARG_POINTER, PG_GET_COLLATION,
    PG_NARGS, PG_RETURN_BOOL, PG_RETURN_BYTEA_P, PG_RETURN_CSTRING, PG_RETURN_DATUM,
    PG_RETURN_INT32, PG_RETURN_NULL, PG_RETURN_POINTER, PG_RETURN_TEXT_P, PG_RETURN_UINT32,
    PG_RETURN_UINT64,
};

use core::ffi::{c_char, c_int, c_void};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strcpy(dest: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strtol(nptr: *const c_char, endptr: *mut *mut c_char, base: c_int) -> i64;
    fn sprintf(s: *mut c_char, format: *const c_char, ...) -> c_int;
    fn isdigit(c: c_int) -> c_int;
    fn isnan(x: f64) -> c_int;
    static mut errno: c_int;
}

// ----------------------------------------------------------------------------
//   Local fmgr macros (DirectFunctionCall2 needs a DatumGetArrayTypeP helper).
// ----------------------------------------------------------------------------

// DatumGetArrayTypeP(X): ((ArrayType *) PG_DETOAST_DATUM(X)) (utils/array.h)
#[inline]
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType {
    crate::PG_DETOAST_DATUM!(d) as *mut ArrayType
}

// PG_GETARG_ARRAYTYPE_P(n): DatumGetArrayTypeP(PG_GETARG_DATUM(n)) (utils/array.h)
macro_rules! PG_GETARG_ARRAYTYPE_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetArrayTypeP(PG_GETARG_DATUM!($fcinfo, $n))
    };
}

// PG_RETURN_ARRAYTYPE_P(x): PG_RETURN_POINTER(x) (utils/array.h)
macro_rules! PG_RETURN_ARRAYTYPE_P {
    ($x:expr) => {
        PG_RETURN_POINTER!($x as *const c_void)
    };
}

// PG_GETARG_ANY_ARRAY_P(n): DatumGetAnyArrayP(PG_GETARG_DATUM(n)) (utils/array.h)
macro_rules! PG_GETARG_ANY_ARRAY_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetAnyArrayP(PG_GETARG_DATUM!($fcinfo, $n))
    };
}

// ----------------------------------------------------------------------------
//   AARR_* macros for AnyArrayType inputs (utils/array.h). Beware multiple
//   references!  Read union fields through explicit struct pointers to avoid the
//   dangerous_implicit_autorefs lint.
// ----------------------------------------------------------------------------

#[inline]
unsafe fn xpn_ptr(a: *const AnyArrayType) -> *const ExpandedArrayHeader {
    a as *const ExpandedArrayHeader
}

/// AARR_NDIM(a)
#[inline]
unsafe fn AARR_NDIM(a: *const AnyArrayType) -> c_int {
    if VARATT_IS_EXPANDED_HEADER(a as *const c_void) {
        (*xpn_ptr(a)).ndims
    } else {
        ARR_NDIM(a as *const ArrayType)
    }
}

/// AARR_HASNULL(a)
#[inline]
unsafe fn AARR_HASNULL(a: *const AnyArrayType) -> bool {
    if VARATT_IS_EXPANDED_HEADER(a as *const c_void) {
        let xpn = xpn_ptr(a);
        if !(*xpn).dvalues.is_null() {
            !(*xpn).dnulls.is_null()
        } else {
            ARR_HASNULL((*xpn).fvalue)
        }
    } else {
        ARR_HASNULL(a as *const ArrayType)
    }
}

/// AARR_ELEMTYPE(a)
#[inline]
unsafe fn AARR_ELEMTYPE(a: *const AnyArrayType) -> Oid {
    if VARATT_IS_EXPANDED_HEADER(a as *const c_void) {
        (*xpn_ptr(a)).element_type
    } else {
        ARR_ELEMTYPE(a as *const ArrayType)
    }
}

/// AARR_DIMS(a)
#[inline]
unsafe fn AARR_DIMS(a: *const AnyArrayType) -> *mut c_int {
    if VARATT_IS_EXPANDED_HEADER(a as *const c_void) {
        (*xpn_ptr(a)).dims
    } else {
        ARR_DIMS(a as *const ArrayType)
    }
}

/// AARR_LBOUND(a)
#[inline]
unsafe fn AARR_LBOUND(a: *const AnyArrayType) -> *mut c_int {
    if VARATT_IS_EXPANDED_HEADER(a as *const c_void) {
        (*xpn_ptr(a)).lbound
    } else {
        ARR_LBOUND(a as *const ArrayType)
    }
}

// ----------------------------------------------------------------------------
//   GUC parameter
// ----------------------------------------------------------------------------

#[no_mangle]
pub static mut Array_nulls: bool = true;

// ----------------------------------------------------------------------------
//   Local definitions
// ----------------------------------------------------------------------------

const MAXDIM: c_int = 6;

const ASSGN: &[u8] = b"=\0";

const INT_MAX: c_int = i32::MAX;
const PG_INT32_MAX: i64 = i32::MAX as i64;
const PG_INT32_MIN: i64 = i32::MIN as i64;
const ERANGE: c_int = 34;

// AARR_FREE_IF_COPY(array,n): if (!VARATT_IS_EXPANDED_HEADER(array)) PG_FREE_IF_COPY(array, n)
macro_rules! AARR_FREE_IF_COPY {
    ($fcinfo:expr, $array:expr, $n:expr) => {
        if !VARATT_IS_EXPANDED_HEADER($array as *const c_void) {
            PG_FREE_IF_COPY!($fcinfo, $array as *const c_void, $n);
        }
    };
}

/// ReadArrayToken return type.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
enum ArrayToken {
    ATOK_LEVEL_START,
    ATOK_LEVEL_END,
    ATOK_DELIM,
    ATOK_ELEM,
    ATOK_ELEM_NULL,
    ATOK_ERROR,
}
use ArrayToken::*;

/// Working state for array_iterate().
#[repr(C)]
pub struct ArrayIteratorData {
    /* basic info about the array, set up during array_create_iterator() */
    arr: *mut ArrayType,        /* array we're iterating through */
    nullbitmap: *mut bits8,     /* its null bitmap, if any */
    nitems: c_int,              /* total number of elements in array */
    typlen: int16,              /* element type's length */
    typbyval: bool,             /* element type's byval property */
    typalign: c_char,           /* element type's align property */

    /* information about the requested slice size */
    slice_ndim: c_int,          /* slice dimension, or 0 if not slicing */
    slice_len: c_int,           /* number of elements per slice */
    slice_dims: *mut c_int,     /* slice dims array */
    slice_lbound: *mut c_int,   /* slice lbound array */
    slice_values: *mut Datum,   /* workspace of length slice_len */
    slice_nulls: *mut bool,     /* workspace of length slice_len */

    /* current position information, updated on each iteration */
    data_ptr: *mut c_char,      /* our current position in the array */
    current_item: c_int,        /* the item # we're at in the array */
}

/// ArrayIteratorData is private in arrayfuncs.c (utils/array.h: typedef ... *ArrayIterator).
pub type ArrayIterator = *mut ArrayIteratorData;

// ----------------------------------------------------------------------------
//   ArrayBuildState* structs (utils/array.h; private working state lives here).
// ----------------------------------------------------------------------------

/// working state for accumArrayResult() and friends.
/// note that the input must be scalars (legal array elements).
#[repr(C)]
pub struct ArrayBuildState {
    pub mcontext: MemoryContext, /* where all the temp stuff is kept */
    pub dvalues: *mut Datum,     /* array of accumulated Datums */
    pub dnulls: *mut bool,       /* array of is-null flags for Datums */
    pub alen: c_int,             /* allocated length of above arrays */
    pub nelems: c_int,           /* number of valid entries in above arrays */
    pub element_type: Oid,       /* data type of the Datums */
    pub typlen: int16,           /* needed info about datatype */
    pub typbyval: bool,
    pub typalign: c_char,
    pub private_cxt: bool, /* use private memory context */
}

/// working state for accumArrayResultArr() and friends.
/// note that the input must be arrays, and the same array type is returned.
#[repr(C)]
pub struct ArrayBuildStateArr {
    pub mcontext: MemoryContext, /* where all the temp stuff is kept */
    pub data: *mut c_char,       /* accumulated data */
    pub nullbitmap: *mut bits8,  /* bitmap of is-null flags, or NULL if none */
    pub abytes: c_int,           /* allocated length of "data" */
    pub nbytes: c_int,           /* number of bytes used so far */
    pub aitems: c_int,           /* allocated length of bitmap (in elements) */
    pub nitems: c_int,           /* total number of elements in result */
    pub ndims: c_int,            /* current dimensions of result */
    pub dims: [c_int; MAXDIM as usize],
    pub lbs: [c_int; MAXDIM as usize],
    pub array_type: Oid,   /* data type of the arrays */
    pub element_type: Oid, /* data type of the array elements */
    pub private_cxt: bool, /* use private memory context */
}

/// working state for accumArrayResultAny() and friends.
/// these functions handle both cases.
#[repr(C)]
pub struct ArrayBuildStateAny {
    /* Exactly one of these is not NULL: */
    pub scalarstate: *mut ArrayBuildState,
    pub arraystate: *mut ArrayBuildStateArr,
}

/// private state needed by array_map (here because caller must provide it).
#[repr(C)]
pub struct ArrayMapState {
    pub inp_extra: ArrayMetaState,
    pub ret_extra: ArrayMetaState,
}

// ----------------------------------------------------------------------------
//   STUBBED dependencies (not yet ported).
// ----------------------------------------------------------------------------

/// STUB: IOFuncSelector (fmgr.h). TODO(pg-port): real enum lives in include/fmgr.h.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum IOFuncSelector {
    IOFunc_input = 0,
    IOFunc_output,
    IOFunc_receive,
    IOFunc_send,
}
use IOFuncSelector::*;

/// STUB: get_type_io_data (utils/cache/lsyscache.c).
/// TODO(pg-port): real fn lives in utils/cache/lsyscache.rs.
unsafe fn get_type_io_data(
    typid: Oid,
    which_func: IOFuncSelector,
    typlen: *mut int16,
    typbyval: *mut bool,
    typalign: *mut c_char,
    typdelim: *mut c_char,
    typioparam: *mut Oid,
    func: *mut Oid,
) {
    crate::utils::cache::lsyscache::get_type_io_data(
        typid, core::mem::transmute(which_func), typlen, typbyval,
        typalign, typdelim, typioparam, func,
    );
    return;
    #[allow(unreachable_code)]
    unimplemented!("get_type_io_data (lsyscache.c) not yet ported");
}

/// STUB: get_typlenbyvalalign (utils/cache/lsyscache.c).
/// TODO(pg-port): real fn lives in utils/cache/lsyscache.rs.
unsafe fn get_typlenbyvalalign(
    _typid: Oid,
    _typlen: *mut int16,
    _typbyval: *mut bool,
    _typalign: *mut c_char,
) { crate::utils::cache::lsyscache::get_typlenbyvalalign(_typid as _, _typlen as _, _typbyval as _, _typalign as _) }

/// STUB: get_element_type (utils/cache/lsyscache.c).
/// TODO(pg-port): real fn lives in utils/cache/lsyscache.rs.
unsafe fn get_element_type(_typid: Oid) -> Oid { crate::utils::cache::lsyscache::get_element_type(_typid as _) as _ }

/// STUB: get_array_type (utils/cache/lsyscache.c).
/// TODO(pg-port): real fn lives in utils/cache/lsyscache.rs.
unsafe fn get_array_type(_typid: Oid) -> Oid { crate::utils::cache::lsyscache::get_array_type(_typid as _) as _ }

/// STUB: InputFunctionCallSafe (fmgr.c).
/// TODO(pg-port): use the real crate::utils::fmgr::InputFunctionCallSafe once its
/// signature matches; declared here to keep array_in self-contained.
unsafe fn InputFunctionCallSafe(
    _flinfo: *mut FmgrInfo,
    _str: *mut c_char,
    _typioparam: Oid,
    _typmod: int32,
    _escontext: *mut Node,
    _result: *mut Datum,
) -> bool { crate::utils::fmgr::InputFunctionCallSafe(_flinfo as _, _str as _, _typioparam as _, _typmod as _, _escontext as _, _result as _) }

/// STUB: estimate_array_length (optimizer/util/clauses.c).
/// TODO(pg-port): real fn lives in optimizer/util/clauses.rs.
unsafe fn estimate_array_length(_root: *mut c_void, _arrayexpr: *mut Node) -> f64 { crate::utils::adt::selfuncs::estimate_array_length(_root as _, _arrayexpr as _) as _ }

/// STUB: is_funcclause (nodes/nodeFuncs.h).
/// TODO(pg-port): real fn lives in nodes/nodeFuncs.rs.
unsafe fn is_funcclause(_clause: *const Node) -> bool {
    unimplemented!("is_funcclause (nodeFuncs.c) not yet ported");
}

/// STUB: ExecEvalExpr (executor/executor.h) - thin wrapper around the executor's
/// expression evaluator. TODO(pg-port): use crate::executor::executor::ExecEvalExpr.
unsafe fn ExecEvalExpr(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isNull: *mut bool,
) -> Datum {
    crate::executor::executor::ExecEvalExpr(state, econtext, isNull)
}

// ---- typcache.h stubs (not yet ported) -------------------------------------

/// STUB: TypeCacheEntry (utils/typcache.h).
/// TODO(pg-port): real struct lives in utils/cache/typcache.rs.
#[repr(C)]
pub struct TypeCacheEntry {
    pub type_id: Oid,
    pub typlen: int16,
    pub typbyval: bool,
    pub typalign: c_char,
    pub eq_opr_finfo: FmgrInfo,
    pub cmp_proc_finfo: FmgrInfo,
    pub hash_proc_finfo: FmgrInfo,
    pub hash_extended_proc_finfo: FmgrInfo,
}

/// STUB: lookup_type_cache (utils/cache/typcache.c).
/// TODO(pg-port): real fn lives in utils/cache/typcache.rs.
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!("lookup_type_cache (typcache.c) not yet ported");
}

/* typcache flags (utils/typcache.h) */
const TYPECACHE_EQ_OPR_FINFO: c_int = 0x00080;
const TYPECACHE_CMP_PROC_FINFO: c_int = 0x00800;
const TYPECACHE_HASH_PROC_FINFO: c_int = 0x01000;
const TYPECACHE_HASH_EXTENDED_PROC_FINFO: c_int = 0x10000;

/* utils/fmgroids.h */
const F_HASH_RECORD: Oid = 0; // TODO(pg-port): real OID from utils/fmgroids.h.

// ---- funcapi.h SRF stubs (not yet ported) ----------------------------------

/// STUB: FuncCallContext (funcapi.h).
/// TODO(pg-port): real struct lives in utils/funcapi.rs.
#[repr(C)]
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub max_calls: u64,
    pub user_fctx: *mut c_void,
    pub attinmeta: *mut c_void,
    pub multi_call_memory_ctx: MemoryContext,
    pub tuple_desc: *mut c_void,
}

unsafe fn srf_is_firstcall(_fcinfo: FunctionCallInfo) -> bool {
    unimplemented!("SRF_IS_FIRSTCALL (funcapi.h) not yet ported");
}
unsafe fn srf_firstcall_init(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!("SRF_FIRSTCALL_INIT (funcapi.h) not yet ported");
}
unsafe fn srf_percall_setup(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!("SRF_PERCALL_SETUP (funcapi.h) not yet ported");
}
unsafe fn srf_return_next(_funcctx: *mut FuncCallContext, _result: Datum) -> Datum {
    unimplemented!("SRF_RETURN_NEXT (funcapi.h) not yet ported");
}
unsafe fn srf_return_done(_funcctx: *mut FuncCallContext) -> Datum {
    unimplemented!("SRF_RETURN_DONE (funcapi.h) not yet ported");
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
    ($funcctx:expr, $result:expr) => {
        return srf_return_next($funcctx, $result)
    };
}
macro_rules! SRF_RETURN_DONE {
    ($funcctx:expr) => {
        return srf_return_done($funcctx)
    };
}

// CopyArrayEls / array_bitmap_copy are public helpers; declared with the rest.

/*
 * array_in :
 *		  converts an array from the external format in "string" to
 *		  its internal format.
 *
 * return value :
 *		  the internal representation of the input array
 */
pub unsafe fn array_in(fcinfo: FunctionCallInfo) -> Datum {
    let string = PG_GETARG_CSTRING!(fcinfo, 0); /* external form */
    let element_type = PG_GETARG_OID!(fcinfo, 1); /* type of an array element */
    let typmod = PG_GETARG_INT32!(fcinfo, 2); /* typmod for array elements */
    let escontext = (*fcinfo).context;
    let typlen: c_int;
    let typbyval: bool;
    let typalign: c_char;
    let typdelim: c_char;
    let typioparam: Oid;
    let mut p: *mut c_char;
    let mut nitems: c_int = 0;
    let mut values: *mut Datum = null_mut();
    let mut nulls: *mut bool = null_mut();
    let hasnulls: bool;
    let mut nbytes: int32;
    let dataoffset: int32;
    let retval: *mut ArrayType;
    let mut ndim: c_int = 0;
    let mut dim: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut lBound: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut my_extra: *mut ArrayMetaState;

    /*
     * We arrange to look up info about element type, including its input
     * conversion proc, only once per series of calls, assuming the element
     * type doesn't change underneath us.
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
    if my_extra.is_null() {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::size_of::<ArrayMetaState>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
        (*my_extra).element_type = !element_type;
    }

    if (*my_extra).element_type != element_type {
        /*
         * Get info about element type, including its input conversion proc
         */
        get_type_io_data(
            element_type,
            IOFunc_input,
            &raw mut (*my_extra).typlen,
            &raw mut (*my_extra).typbyval,
            &raw mut (*my_extra).typalign,
            &raw mut (*my_extra).typdelim,
            &raw mut (*my_extra).typioparam,
            &raw mut (*my_extra).typiofunc,
        );
        fmgr_info_cxt(
            (*my_extra).typiofunc,
            &raw mut (*my_extra).proc,
            (*(*fcinfo).flinfo).fn_mcxt,
        );
        (*my_extra).element_type = element_type;
    }
    typlen = (*my_extra).typlen as c_int;
    typbyval = (*my_extra).typbyval;
    typalign = (*my_extra).typalign;
    typdelim = (*my_extra).typdelim;
    typioparam = (*my_extra).typioparam;

    /*
     * Initialize dim[] and lBound[] for ReadArrayStr, in case there is no
     * explicit dimension info.  (If there is, ReadArrayDimensions will
     * overwrite this.)
     */
    for i in 0..MAXDIM as usize {
        dim[i] = -1; /* indicates "not yet known" */
        lBound[i] = 1; /* default lower bound */
    }

    /*
     * Start processing the input string.
     *
     * If the input string starts with dimension info, read and use that.
     * Otherwise, we'll determine the dimensions during ReadArrayStr.
     */
    p = string;
    if !ReadArrayDimensions(
        &raw mut p,
        &raw mut ndim,
        dim.as_mut_ptr(),
        lBound.as_mut_ptr(),
        string,
        escontext,
    ) {
        return 0 as Datum;
    }

    if ndim == 0 {
        /* No array dimensions, so next character should be a left brace */
        if *p != b'{' as c_char {
            ereport!(
                ERROR,
                errmsg!(
                    "malformed array literal: \"{}\"",
                    std::ffi::CStr::from_ptr(string).to_string_lossy()
                )
            );
            return 0 as Datum;
        }
    } else {
        /* If array dimensions are given, expect '=' operator */
        if strncmp(p, ASSGN.as_ptr() as *const c_char, strlen(ASSGN.as_ptr() as *const c_char))
            != 0
        {
            ereport!(
                ERROR,
                errmsg!(
                    "malformed array literal: \"{}\"",
                    std::ffi::CStr::from_ptr(string).to_string_lossy()
                )
            );
            return 0 as Datum;
        }
        p = p.add(strlen(ASSGN.as_ptr() as *const c_char));
        /* Allow whitespace after it */
        while scanner_isspace(*p) {
            p = p.add(1);
        }

        if *p != b'{' as c_char {
            ereport!(
                ERROR,
                errmsg!(
                    "malformed array literal: \"{}\"",
                    std::ffi::CStr::from_ptr(string).to_string_lossy()
                )
            );
            return 0 as Datum;
        }
    }

    /* Parse the value part, in the curly braces: { ... } */
    if !ReadArrayStr(
        &raw mut p,
        &raw mut (*my_extra).proc,
        typioparam,
        typmod,
        typdelim,
        typlen,
        typbyval,
        typalign,
        &raw mut ndim,
        dim.as_mut_ptr(),
        &raw mut nitems,
        &raw mut values,
        &raw mut nulls,
        string,
        escontext,
    ) {
        return 0 as Datum;
    }

    /* only whitespace is allowed after the closing brace */
    while *p != 0 {
        let ch = *p;
        p = p.add(1);
        if !scanner_isspace(ch) {
            ereport!(
                ERROR,
                errmsg!(
                    "malformed array literal: \"{}\"",
                    std::ffi::CStr::from_ptr(string).to_string_lossy()
                )
            );
            return 0 as Datum;
        }
    }

    /* Empty array? */
    if nitems == 0 {
        PG_RETURN_ARRAYTYPE_P!(construct_empty_array(element_type));
    }

    /*
     * Check for nulls, compute total data space needed
     */
    hasnulls = {
        let mut hasnulls = false;
        nbytes = 0;
        for i in 0..nitems as usize {
            if *nulls.add(i) {
                hasnulls = true;
            } else {
                /* let's just make sure data is not toasted */
                if typlen == -1 {
                    *values.add(i) =
                        PointerGetDatum(crate::PG_DETOAST_DATUM!(*values.add(i)) as *const c_void);
                }
                nbytes = att_addlength_datum(nbytes as usize, typlen, *values.add(i)) as int32;
                nbytes = att_align_nominal(nbytes as usize, typalign) as int32;
                /* check for overflow of total request */
                if !AllocSizeIsValid(nbytes as Size) {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "array size exceeds the maximum allowed ({})",
                            MaxAllocSize as c_int
                        )
                    );
                    return 0 as Datum;
                }
            }
        }
        hasnulls
    };
    if hasnulls {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndim, nitems) as int32;
        nbytes += dataoffset;
    } else {
        dataoffset = 0; /* marker for no null bitmap */
        nbytes += ARR_OVERHEAD_NONULLS(ndim) as int32;
    }

    /*
     * Construct the final array datum
     */
    retval = palloc0(nbytes as usize) as *mut ArrayType;
    SET_VARSIZE(retval as *mut c_char, nbytes);
    (*retval).ndim = ndim;
    (*retval).dataoffset = dataoffset;

    /*
     * This comes from the array's pg_type.typelem (which points to the base
     * data type's pg_type.oid) and stores system oids in user tables. This
     * oid must be preserved by binary upgrades.
     */
    (*retval).elemtype = element_type;
    memcpy(
        ARR_DIMS(retval) as *mut c_void,
        dim.as_ptr() as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        ARR_LBOUND(retval) as *mut c_void,
        lBound.as_ptr() as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );

    CopyArrayEls(
        retval, values, nulls, nitems, typlen, typbyval, typalign, true,
    );

    pfree(values as *mut c_void);
    pfree(nulls as *mut c_void);

    PG_RETURN_ARRAYTYPE_P!(retval);
}

/*
 * ReadArrayDimensions
 *	 parses the array dimensions part of the input and converts the values
 *	 to internal format.
 *
 * On entry, *srcptr points to the string to parse. It is advanced to point
 * after whitespace (if any) and dimension info (if any).
 *
 * *ndim_p, dim[], and lBound[] are output variables. They are filled with the
 * number of dimensions (<= MAXDIM), the lengths of each dimension, and the
 * lower subscript bounds, respectively.  If no dimension info appears,
 * *ndim_p will be set to zero, and dim[] and lBound[] are unchanged.
 *
 * 'origStr' is the original input string, used only in error messages.
 * If *escontext points to an ErrorSaveContext, details of any error are
 * reported there.
 *
 * Result:
 *	true for success, false for failure (if escontext is provided).
 *
 * Note that dim[] and lBound[] are allocated by the caller, and must have
 * MAXDIM elements.
 */
unsafe fn ReadArrayDimensions(
    srcptr: *mut *mut c_char,
    ndim_p: *mut c_int,
    dim: *mut c_int,
    lBound: *mut c_int,
    origStr: *const c_char,
    escontext: *mut Node,
) -> bool {
    let mut p = *srcptr;
    let mut ndim: c_int;

    /*
     * Dimension info takes the form of one or more [n] or [m:n] items.  This
     * loop iterates once per dimension item.
     */
    ndim = 0;
    loop {
        let mut q: *mut c_char;
        let mut ub: c_int = 0;
        let mut i: c_int = 0;

        /*
         * Note: we currently allow whitespace between, but not within,
         * dimension items.
         */
        while scanner_isspace(*p) {
            p = p.add(1);
        }
        if *p != b'[' as c_char {
            break; /* no more dimension items */
        }
        p = p.add(1);
        if ndim >= MAXDIM {
            ereport!(
                ERROR,
                errmsg!(
                    "number of array dimensions exceeds the maximum allowed ({})",
                    MAXDIM
                )
            );
            return false;
        }

        q = p;
        if !ReadDimensionInt(&raw mut p, &raw mut i, origStr, escontext) {
            return false;
        }
        if p == q {
            /* no digits? */
            ereport!(
                ERROR,
                errmsg!(
                    "malformed array literal: \"{}\"",
                    std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                )
            );
            return false;
        }

        if *p == b':' as c_char {
            /* [m:n] format */
            *lBound.add(ndim as usize) = i;
            p = p.add(1);
            q = p;
            if !ReadDimensionInt(&raw mut p, &raw mut ub, origStr, escontext) {
                return false;
            }
            if p == q {
                /* no digits? */
                ereport!(
                    ERROR,
                    errmsg!(
                        "malformed array literal: \"{}\"",
                        std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                    )
                );
                return false;
            }
        } else {
            /* [n] format */
            *lBound.add(ndim as usize) = 1;
            ub = i;
        }
        if *p != b']' as c_char {
            ereport!(
                ERROR,
                errmsg!(
                    "malformed array literal: \"{}\"",
                    std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                )
            );
            return false;
        }
        p = p.add(1);

        /*
         * Note: we could accept ub = lb-1 to represent a zero-length
         * dimension.  However, that would result in an empty array, for which
         * we don't keep any dimension data, so that e.g. [1:0] and [101:100]
         * would be equivalent.  Given the lack of field demand, there seems
         * little point in allowing such cases.
         */
        if ub < *lBound.add(ndim as usize) {
            ereport!(ERROR, errmsg!("upper bound cannot be less than lower bound"));
            return false;
        }

        /* Upper bound of INT_MAX must be disallowed, cf ArrayCheckBounds() */
        if ub == INT_MAX {
            ereport!(ERROR, errmsg!("array upper bound is too large: {}", ub));
            return false;
        }

        /* Compute "ub - lBound[ndim] + 1", detecting overflow */
        if pg_sub_s32_overflow(ub, *lBound.add(ndim as usize), &raw mut ub)
            || pg_add_s32_overflow(ub, 1, &raw mut ub)
        {
            ereport!(
                ERROR,
                errmsg!(
                    "array size exceeds the maximum allowed ({})",
                    MaxArraySize as c_int
                )
            );
            return false;
        }

        *dim.add(ndim as usize) = ub;
        ndim += 1;
    }

    *srcptr = p;
    *ndim_p = ndim;
    true
}

/*
 * ReadDimensionInt
 *	 parse an integer, for the array dimensions
 *
 * On entry, *srcptr points to the string to parse. It is advanced past the
 * digits of the integer. If there are no digits, returns true and leaves
 * *srcptr unchanged.
 *
 * Result:
 *	true for success, false for failure (if escontext is provided).
 *  On success, the parsed integer is returned in *result.
 */
unsafe fn ReadDimensionInt(
    srcptr: *mut *mut c_char,
    result: *mut c_int,
    origStr: *const c_char,
    escontext: *mut Node,
) -> bool {
    let p = *srcptr;
    let l: i64;

    /* don't accept leading whitespace */
    if isdigit(*p as c_uchar as c_int) == 0 && *p != b'-' as c_char && *p != b'+' as c_char {
        *result = 0;
        return true;
    }

    errno = 0;
    l = strtol(p, srcptr, 10);

    if errno == ERANGE || l > PG_INT32_MAX || l < PG_INT32_MIN {
        ereport!(ERROR, errmsg!("array bound is out of integer range"));
        return false;
    }

    *result = l as c_int;
    true
}

/*
 * ReadArrayStr :
 *	 parses the array string pointed to by *srcptr and converts the values
 *	 to internal format.  Determines the array dimensions as it goes.
 *
 * On entry, *srcptr points to the string to parse (it must point to a '{').
 * On successful return, it is advanced to point past the closing '}'.
 *
 * If dimensions were specified explicitly, they are passed in *ndim_p and
 * dim[].  This function will check that the array values match the specified
 * dimensions.  If dimensions were not given, caller must pass *ndim_p == 0
 * and initialize all elements of dim[] to -1.  Then this function will
 * deduce the dimensions from the structure of the input and store them in
 * *ndim_p and the dim[] array.
 *
 * Element type information:
 *	inputproc: type-specific input procedure for element datatype.
 *	typioparam, typmod: auxiliary values to pass to inputproc.
 *	typdelim: the value delimiter (type-specific).
 *	typlen, typbyval, typalign: storage parameters of element datatype.
 *
 * Outputs:
 *  *ndim_p, dim: dimensions deduced from the input structure.
 *  *nitems_p: total number of elements.
 *	*values_p[]: palloc'd array, filled with converted data values.
 *	*nulls_p[]: palloc'd array, filled with is-null markers.
 *
 * 'origStr' is the original input string, used only in error messages.
 * If *escontext points to an ErrorSaveContext, details of any error are
 * reported there.
 *
 * Result:
 *	true for success, false for failure (if escontext is provided).
 */
unsafe fn ReadArrayStr(
    srcptr: *mut *mut c_char,
    inputproc: *mut FmgrInfo,
    typioparam: Oid,
    typmod: int32,
    typdelim: c_char,
    typlen: c_int,
    typbyval: bool,
    typalign: c_char,
    ndim_p: *mut c_int,
    dim: *mut c_int,
    nitems_p: *mut c_int,
    values_p: *mut *mut Datum,
    nulls_p: *mut *mut bool,
    origStr: *const c_char,
    escontext: *mut Node,
) -> bool {
    let mut ndim = *ndim_p;
    let dimensions_specified = ndim != 0;
    let mut maxitems: c_int;
    let mut values: *mut Datum;
    let mut nulls: *mut bool;
    let mut elembuf = core::mem::MaybeUninit::<StringInfoData>::uninit();
    let mut nest_level: c_int;
    let mut nitems: c_int;
    let mut ndim_frozen: bool;
    let mut expect_delim: bool;
    let mut nelems: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];

    /* Allocate some starting output workspace; we'll enlarge as needed */
    maxitems = 16;
    values = palloc(maxitems as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls = palloc(maxitems as usize * core::mem::size_of::<bool>()) as *mut bool;

    /* Allocate workspace to hold (string representation of) one element */
    initStringInfo(elembuf.as_mut_ptr());
    let elembuf = elembuf.as_mut_ptr();

    /* Loop below assumes first token is ATOK_LEVEL_START */
    Assert!(**srcptr == b'{' as c_char);

    /* Parse tokens until we reach the matching right brace */
    nest_level = 0;
    nitems = 0;
    ndim_frozen = dimensions_specified;
    expect_delim = false;

    let dimension_error = |ndim_p: *mut c_int| -> bool {
        if dimensions_specified {
            ereport!(
                ERROR,
                errmsg!(
                    "malformed array literal: \"{}\"",
                    std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                )
            );
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "malformed array literal: \"{}\"",
                    std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                )
            );
        }
        let _ = ndim_p;
        false
    };

    loop {
        let tok = ReadArrayToken(srcptr, elembuf, typdelim, origStr, escontext);

        match tok {
            ATOK_LEVEL_START => {
                /* Can't write left brace where delim is expected */
                if expect_delim {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "malformed array literal: \"{}\"",
                            std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                        )
                    );
                    return false;
                }

                /* Initialize element counting in the new level */
                if nest_level >= MAXDIM {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "number of array dimensions exceeds the maximum allowed ({})",
                            MAXDIM
                        )
                    );
                    return false;
                }

                nelems[nest_level as usize] = 0;
                nest_level += 1;
                if nest_level > ndim {
                    /* Can't increase ndim once it's frozen */
                    if ndim_frozen {
                        return dimension_error(ndim_p);
                    }
                    ndim = nest_level;
                }
            }

            ATOK_LEVEL_END => {
                /* Can't get here with nest_level == 0 */
                Assert!(nest_level > 0);

                /*
                 * We allow a right brace to terminate an empty sub-array,
                 * otherwise it must occur where we expect a delimiter.
                 */
                if nelems[(nest_level - 1) as usize] > 0 && !expect_delim {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "malformed array literal: \"{}\"",
                            std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                        )
                    );
                    return false;
                }
                nest_level -= 1;
                /* Nested sub-arrays count as elements of outer level */
                if nest_level > 0 {
                    nelems[(nest_level - 1) as usize] += 1;
                }

                /*
                 * Note: if we had dimensionality info, then dim[nest_level]
                 * is initially non-negative, and we'll check each sub-array's
                 * length against that.
                 */
                if *dim.add(nest_level as usize) < 0 {
                    /* Save length of first sub-array of this level */
                    *dim.add(nest_level as usize) = nelems[nest_level as usize];
                } else if nelems[nest_level as usize] != *dim.add(nest_level as usize) {
                    /* Subsequent sub-arrays must have same length */
                    return dimension_error(ndim_p);
                }

                /*
                 * Must have a delim or another right brace following, unless
                 * we have reached nest_level 0, where this won't matter.
                 */
                expect_delim = true;
            }

            ATOK_DELIM => {
                if !expect_delim {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "malformed array literal: \"{}\"",
                            std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                        )
                    );
                    return false;
                }
                expect_delim = false;
            }

            ATOK_ELEM | ATOK_ELEM_NULL => {
                /* Can't get here with nest_level == 0 */
                Assert!(nest_level > 0);

                /* Disallow consecutive ELEM tokens */
                if expect_delim {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "malformed array literal: \"{}\"",
                            std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                        )
                    );
                    return false;
                }

                /* Enlarge the values/nulls arrays if needed */
                if nitems >= maxitems {
                    if maxitems as Size >= MaxArraySize {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "array size exceeds the maximum allowed ({})",
                                MaxArraySize as c_int
                            )
                        );
                        return false;
                    }
                    maxitems = core::cmp::min((maxitems * 2) as Size, MaxArraySize) as c_int;
                    values = repalloc(
                        values as *mut c_void,
                        maxitems as usize * core::mem::size_of::<Datum>(),
                    ) as *mut Datum;
                    nulls = repalloc(
                        nulls as *mut c_void,
                        maxitems as usize * core::mem::size_of::<bool>(),
                    ) as *mut bool;
                }

                /* Read the element's value, or check that NULL is allowed */
                if !InputFunctionCallSafe(
                    inputproc,
                    if tok == ATOK_ELEM_NULL {
                        null_mut()
                    } else {
                        (*elembuf).data
                    },
                    typioparam,
                    typmod,
                    escontext,
                    values.add(nitems as usize),
                ) {
                    return false;
                }
                *nulls.add(nitems as usize) = tok == ATOK_ELEM_NULL;
                nitems += 1;

                /*
                 * Once we have found an element, the number of dimensions can
                 * no longer increase, and subsequent elements must all be at
                 * the same nesting depth.
                 */
                ndim_frozen = true;
                if nest_level != ndim {
                    return dimension_error(ndim_p);
                }
                /* Count the new element */
                nelems[(nest_level - 1) as usize] += 1;

                /* Must have a delim or a right brace following */
                expect_delim = true;
            }

            ATOK_ERROR => {
                return false;
            }
        }

        if nest_level <= 0 {
            break;
        }
    }

    /* Clean up and return results */
    pfree((*elembuf).data as *mut c_void);

    *ndim_p = ndim;
    *nitems_p = nitems;
    *values_p = values;
    *nulls_p = nulls;
    true
}

/*
 * ReadArrayToken
 *	 read one token from an array value string
 *
 * Starts scanning from *srcptr.  On non-error return, *srcptr is
 * advanced past the token.
 *
 * If the token is ATOK_ELEM, the de-escaped string is returned in elembuf.
 */
unsafe fn ReadArrayToken(
    srcptr: *mut *mut c_char,
    elembuf: StringInfo,
    typdelim: c_char,
    origStr: *const c_char,
    escontext: *mut Node,
) -> ArrayToken {
    let mut p = *srcptr;

    resetStringInfo(elembuf);

    /* Identify token type.  Loop advances over leading whitespace. */
    'ident: loop {
        match *p as u8 {
            0 => {
                /* ending_error */
                ereport!(
                    ERROR,
                    errmsg!(
                        "malformed array literal: \"{}\"",
                        std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                    )
                );
                return ATOK_ERROR;
            }
            b'{' => {
                *srcptr = p.add(1);
                return ATOK_LEVEL_START;
            }
            b'}' => {
                *srcptr = p.add(1);
                return ATOK_LEVEL_END;
            }
            b'"' => {
                p = p.add(1);
                break 'ident;
            }
            _ => {
                if *p == typdelim {
                    *srcptr = p.add(1);
                    return ATOK_DELIM;
                }
                if scanner_isspace(*p) {
                    p = p.add(1);
                    continue;
                }
                /* goto unquoted_element */
                return read_unquoted_element(srcptr, elembuf, p, typdelim, origStr, escontext);
            }
        }
    }

    /* quoted_element: */
    loop {
        match *p as u8 {
            0 => {
                ereport!(
                    ERROR,
                    errmsg!(
                        "malformed array literal: \"{}\"",
                        std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                    )
                );
                return ATOK_ERROR;
            }
            b'\\' => {
                /* Skip backslash, copy next character as-is. */
                p = p.add(1);
                if *p == 0 {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "malformed array literal: \"{}\"",
                            std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                        )
                    );
                    return ATOK_ERROR;
                }
                appendStringInfoChar(elembuf, *p);
                p = p.add(1);
            }
            b'"' => {
                /*
                 * If next non-whitespace isn't typdelim or a brace, complain
                 * about incorrect quoting.
                 */
                loop {
                    p = p.add(1);
                    if *p == 0 {
                        break;
                    }
                    if *p == typdelim || *p == b'}' as c_char || *p == b'{' as c_char {
                        *srcptr = p;
                        return ATOK_ELEM;
                    }
                    if !scanner_isspace(*p) {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "malformed array literal: \"{}\"",
                                std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                            )
                        );
                        return ATOK_ERROR;
                    }
                }
                ereport!(
                    ERROR,
                    errmsg!(
                        "malformed array literal: \"{}\"",
                        std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                    )
                );
                return ATOK_ERROR;
            }
            _ => {
                appendStringInfoChar(elembuf, *p);
                p = p.add(1);
            }
        }
    }
}

/*
 * unquoted_element handling for ReadArrayToken.
 *
 * We don't include trailing whitespace in the result.  dstlen tracks how
 * much of the output string is known to not be trailing whitespace.
 */
unsafe fn read_unquoted_element(
    srcptr: *mut *mut c_char,
    elembuf: StringInfo,
    mut p: *mut c_char,
    typdelim: c_char,
    origStr: *const c_char,
    escontext: *mut Node,
) -> ArrayToken {
    let mut dstlen: c_int = 0;
    let mut has_escapes: bool = false;

    loop {
        match *p as u8 {
            0 => {
                ereport!(
                    ERROR,
                    errmsg!(
                        "malformed array literal: \"{}\"",
                        std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                    )
                );
                return ATOK_ERROR;
            }
            b'{' => {
                ereport!(
                    ERROR,
                    errmsg!(
                        "malformed array literal: \"{}\"",
                        std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                    )
                );
                return ATOK_ERROR;
            }
            b'"' => {
                /* Must double-quote all or none of an element. */
                ereport!(
                    ERROR,
                    errmsg!(
                        "malformed array literal: \"{}\"",
                        std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                    )
                );
                return ATOK_ERROR;
            }
            b'\\' => {
                /* Skip backslash, copy next character as-is. */
                p = p.add(1);
                if *p == 0 {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "malformed array literal: \"{}\"",
                            std::ffi::CStr::from_ptr(origStr).to_string_lossy()
                        )
                    );
                    return ATOK_ERROR;
                }
                appendStringInfoChar(elembuf, *p);
                p = p.add(1);
                dstlen = (*elembuf).len; /* treat it as non-whitespace */
                has_escapes = true;
            }
            _ => {
                /* End of elem? */
                if *p == typdelim || *p == b'}' as c_char {
                    /* hack: truncate the output string to dstlen */
                    *(*elembuf).data.add(dstlen as usize) = 0;
                    (*elembuf).len = dstlen;
                    *srcptr = p;
                    /* Check if it's unquoted "NULL" */
                    if Array_nulls
                        && !has_escapes
                        && pg_strcasecmp((*elembuf).data, c"NULL".as_ptr()) == 0
                    {
                        return ATOK_ELEM_NULL;
                    } else {
                        return ATOK_ELEM;
                    }
                }
                appendStringInfoChar(elembuf, *p);
                if !scanner_isspace(*p) {
                    dstlen = (*elembuf).len;
                }
                p = p.add(1);
            }
        }
    }
}

/*
 * Copy data into an array object from a temporary array of Datums.
 *
 * array: array object (with header fields already filled in)
 * values: array of Datums to be copied
 * nulls: array of is-null flags (can be NULL if no nulls)
 * nitems: number of Datums to be copied
 * typbyval, typlen, typalign: info about element datatype
 * freedata: if true and element type is pass-by-ref, pfree data values
 * referenced by Datums after copying them.
 *
 * If the input data is of varlena type, the caller must have ensured that
 * the values are not toasted.  (Doing it here doesn't work since the
 * caller has already allocated space for the array...)
 */
pub unsafe fn CopyArrayEls(
    array: *mut ArrayType,
    values: *mut Datum,
    nulls: *mut bool,
    nitems: c_int,
    typlen: c_int,
    typbyval: bool,
    typalign: c_char,
    mut freedata: bool,
) {
    let mut p = ARR_DATA_PTR(array);
    let mut bitmap = ARR_NULLBITMAP(array);
    let mut bitval: c_int = 0;
    let mut bitmask: c_int = 1;

    if typbyval {
        freedata = false;
    }

    for i in 0..nitems as usize {
        if !nulls.is_null() && *nulls.add(i) {
            if bitmap.is_null() {
                /* shouldn't happen */
                elog!(ERROR, "null array element where not supported");
            }
            /* bitmap bit stays 0 */
        } else {
            bitval |= bitmask;
            p = p.add(ArrayCastAndSet(*values.add(i), typlen, typbyval, typalign, p) as usize);
            if freedata {
                pfree(DatumGetPointer(*values.add(i)) as *mut c_void);
            }
        }
        if !bitmap.is_null() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                *bitmap = bitval as bits8;
                bitmap = bitmap.add(1);
                bitval = 0;
                bitmask = 1;
            }
        }
    }

    if !bitmap.is_null() && bitmask != 1 {
        *bitmap = bitval as bits8;
    }
}

/*
 * array_out :
 *		   takes the internal representation of an array and returns a string
 *		  containing the array in its external format.
 */
pub unsafe fn array_out(fcinfo: FunctionCallInfo) -> Datum {
    let v = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let element_type = AARR_ELEMTYPE(v);
    let typlen: c_int;
    let typbyval: bool;
    let typalign: c_char;
    let typdelim: c_char;
    let mut p: *mut c_char;
    let mut tmp: *mut c_char;
    let retval: *mut c_char;
    let values: *mut *mut c_char;
    /*
     * 33 per dim since we assume 15 digits per number + ':' +'[]'
     *
     * +2 allows for assignment operator + trailing null
     */
    let mut dims_str: [c_char; (MAXDIM as usize * 33) + 2] = [0; (MAXDIM as usize * 33) + 2];
    let needquotes: *mut bool;
    let mut needdims = false;
    let mut overall_length: usize;
    let nitems: c_int;
    let mut i: c_int;
    let mut j: c_int;
    let mut k: c_int;
    let mut indx: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let ndim: c_int;
    let dims: *mut c_int;
    let lb: *mut c_int;
    let mut iter = core::mem::MaybeUninit::<array_iter>::uninit();
    let mut my_extra: *mut ArrayMetaState;

    /*
     * We arrange to look up info about element type, including its output
     * conversion proc, only once per series of calls, assuming the element
     * type doesn't change underneath us.
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
    if my_extra.is_null() {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::size_of::<ArrayMetaState>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
        (*my_extra).element_type = !element_type;
    }

    if (*my_extra).element_type != element_type {
        /*
         * Get info about element type, including its output conversion proc
         */
        get_type_io_data(
            element_type,
            IOFunc_output,
            &raw mut (*my_extra).typlen,
            &raw mut (*my_extra).typbyval,
            &raw mut (*my_extra).typalign,
            &raw mut (*my_extra).typdelim,
            &raw mut (*my_extra).typioparam,
            &raw mut (*my_extra).typiofunc,
        );
        fmgr_info_cxt(
            (*my_extra).typiofunc,
            &raw mut (*my_extra).proc,
            (*(*fcinfo).flinfo).fn_mcxt,
        );
        (*my_extra).element_type = element_type;
    }
    typlen = (*my_extra).typlen as c_int;
    typbyval = (*my_extra).typbyval;
    typalign = (*my_extra).typalign;
    typdelim = (*my_extra).typdelim;

    ndim = AARR_NDIM(v);
    dims = AARR_DIMS(v);
    lb = AARR_LBOUND(v);
    nitems = ArrayGetNItems(ndim, dims);

    if nitems == 0 {
        let retval = pstrdup(c"{}".as_ptr());
        PG_RETURN_CSTRING!(retval);
    }

    /*
     * we will need to add explicit dimensions if any dimension has a lower
     * bound other than one
     */
    i = 0;
    while i < ndim {
        if *lb.add(i as usize) != 1 {
            needdims = true;
            break;
        }
        i += 1;
    }

    /*
     * Convert all values to string form, count total space needed (including
     * any overhead such as escaping backslashes), and detect whether each
     * item needs double quotes.
     */
    values = palloc(nitems as usize * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
    needquotes = palloc(nitems as usize * core::mem::size_of::<bool>()) as *mut bool;
    overall_length = 0;

    array_iter_setup(iter.as_mut_ptr(), v);
    let iter = iter.as_mut_ptr();

    i = 0;
    while i < nitems {
        let itemvalue: Datum;
        let mut isnull = false;
        let mut needquote: bool;

        /* Get source element, checking for NULL */
        itemvalue = array_iter_next(iter, &raw mut isnull, i, typlen, typbyval, typalign);

        if isnull {
            *values.add(i as usize) = pstrdup(c"NULL".as_ptr());
            overall_length += 4;
            needquote = false;
        } else {
            *values.add(i as usize) = OutputFunctionCall(&raw mut (*my_extra).proc, itemvalue);

            /* count data plus backslashes; detect chars needing quotes */
            if *(*values.add(i as usize)) == 0 {
                needquote = true; /* force quotes for empty string */
            } else if pg_strcasecmp(*values.add(i as usize), c"NULL".as_ptr()) == 0 {
                needquote = true; /* force quotes for literal NULL */
            } else {
                needquote = false;
            }

            tmp = *values.add(i as usize);
            while *tmp != 0 {
                let ch = *tmp;

                overall_length += 1;
                if ch == b'"' as c_char || ch == b'\\' as c_char {
                    needquote = true;
                    overall_length += 1;
                } else if ch == b'{' as c_char
                    || ch == b'}' as c_char
                    || ch == typdelim
                    || scanner_isspace(ch)
                {
                    needquote = true;
                }
                tmp = tmp.add(1);
            }
        }

        *needquotes.add(i as usize) = needquote;

        /* Count the pair of double quotes, if needed */
        if needquote {
            overall_length += 2;
        }
        /* and the comma (or other typdelim delimiter) */
        overall_length += 1;
        i += 1;
    }

    /*
     * The very last array element doesn't have a typdelim delimiter after it,
     * but that's OK; that space is needed for the trailing '\0'.
     *
     * Now count total number of curly brace pairs in output string.
     */
    i = 0;
    j = 0;
    k = 1;
    while i < ndim {
        j += k;
        k *= *dims.add(i as usize);
        i += 1;
    }
    overall_length += (2 * j) as usize;

    /* Format explicit dimensions if required */
    dims_str[0] = 0;
    if needdims {
        let mut ptr = dims_str.as_mut_ptr();

        i = 0;
        while i < ndim {
            sprintf(
                ptr,
                c"[%d:%d]".as_ptr(),
                *lb.add(i as usize),
                *lb.add(i as usize) + *dims.add(i as usize) - 1,
            );
            ptr = ptr.add(strlen(ptr));
            i += 1;
        }
        *ptr = *(ASSGN.as_ptr() as *const c_char);
        ptr = ptr.add(1);
        *ptr = 0;
        overall_length += ptr.offset_from(dims_str.as_ptr()) as usize;
    }

    /* Now construct the output string */
    retval = palloc(overall_length) as *mut c_char;
    p = retval;

    // APPENDSTR(str): (strcpy(p, (str)), p += strlen(p))
    macro_rules! APPENDSTR {
        ($str:expr) => {{
            strcpy(p, $str);
            p = p.add(strlen(p));
        }};
    }
    // APPENDCHAR(ch): (*p++ = (ch), *p = '\0')
    macro_rules! APPENDCHAR {
        ($ch:expr) => {{
            *p = $ch;
            p = p.add(1);
            *p = 0;
        }};
    }

    if needdims {
        APPENDSTR!(dims_str.as_ptr());
    }
    APPENDCHAR!(b'{' as c_char);
    i = 0;
    while i < ndim {
        indx[i as usize] = 0;
        i += 1;
    }
    j = 0;
    k = 0;
    loop {
        i = j;
        while i < ndim - 1 {
            APPENDCHAR!(b'{' as c_char);
            i += 1;
        }

        if *needquotes.add(k as usize) {
            APPENDCHAR!(b'"' as c_char);
            tmp = *values.add(k as usize);
            while *tmp != 0 {
                let ch = *tmp;

                if ch == b'"' as c_char || ch == b'\\' as c_char {
                    *p = b'\\' as c_char;
                    p = p.add(1);
                }
                *p = ch;
                p = p.add(1);
                tmp = tmp.add(1);
            }
            *p = 0;
            APPENDCHAR!(b'"' as c_char);
        } else {
            APPENDSTR!(*values.add(k as usize));
        }
        pfree(*values.add(k as usize) as *mut c_void);
        k += 1;

        i = ndim - 1;
        while i >= 0 {
            indx[i as usize] += 1;
            if indx[i as usize] < *dims.add(i as usize) {
                APPENDCHAR!(typdelim);
                break;
            } else {
                indx[i as usize] = 0;
                APPENDCHAR!(b'}' as c_char);
            }
            i -= 1;
        }
        j = i;
        if j == -1 {
            break;
        }
    }

    /* Assert that we calculated the string length accurately */
    Assert!(overall_length == (p.offset_from(retval) + 1) as usize);

    pfree(values as *mut c_void);
    pfree(needquotes as *mut c_void);

    PG_RETURN_CSTRING!(retval);
}

/*
 * array_recv :
 *		  converts an array from the external binary format to
 *		  its internal format.
 *
 * return value :
 *		  the internal representation of the input array
 */
pub unsafe fn array_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let spec_element_type = PG_GETARG_OID!(fcinfo, 1); /* type of an array element */
    let typmod = PG_GETARG_INT32!(fcinfo, 2); /* typmod for array elements */
    let mut element_type: Oid;
    let typlen: c_int;
    let typbyval: bool;
    let typalign: c_char;
    let typioparam: Oid;
    let nitems: c_int;
    let dataPtr: *mut Datum;
    let nullsPtr: *mut bool;
    let mut hasnulls = false;
    let mut nbytes: int32 = 0;
    let dataoffset: int32;
    let retval: *mut ArrayType;
    let ndim: c_int;
    let flags: c_int;
    let mut dim: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut lBound: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut my_extra: *mut ArrayMetaState;

    /* Get the array header information */
    ndim = pq_getmsgint(buf, 4) as c_int;
    if ndim < 0 {
        /* we do allow zero-dimension arrays */
        ereport!(ERROR, errmsg!("invalid number of dimensions: {}", ndim));
    }
    if ndim > MAXDIM {
        ereport!(
            ERROR,
            errmsg!(
                "number of array dimensions ({}) exceeds the maximum allowed ({})",
                ndim,
                MAXDIM
            )
        );
    }

    flags = pq_getmsgint(buf, 4) as c_int;
    if flags != 0 && flags != 1 {
        ereport!(ERROR, errmsg!("invalid array flags"));
    }

    /* Check element type recorded in the data */
    element_type = pq_getmsgint(buf, core::mem::size_of::<Oid>() as c_int) as Oid;

    /*
     * From a security standpoint, it doesn't matter whether the input's
     * element type matches what we expect ... [see arrayfuncs.c]
     */
    if element_type != spec_element_type {
        if element_type < FirstGenbkiObjectId && spec_element_type < FirstGenbkiObjectId {
            ereport!(
                ERROR,
                errmsg!(
                    "binary data has array element type {} ({}) instead of expected {} ({})",
                    element_type,
                    std::ffi::CStr::from_ptr(format_type_extended(
                        element_type,
                        -1,
                        crate::utils::builtins::FORMAT_TYPE_ALLOW_INVALID as crate::c::bits16
                    ))
                    .to_string_lossy(),
                    spec_element_type,
                    std::ffi::CStr::from_ptr(format_type_extended(
                        spec_element_type,
                        -1,
                        crate::utils::builtins::FORMAT_TYPE_ALLOW_INVALID as crate::c::bits16
                    ))
                    .to_string_lossy()
                )
            );
        }
        element_type = spec_element_type;
    }

    for i in 0..ndim as usize {
        dim[i] = pq_getmsgint(buf, 4) as c_int;
        lBound[i] = pq_getmsgint(buf, 4) as c_int;
    }

    /* This checks for overflow of array dimensions */
    nitems = ArrayGetNItems(ndim, dim.as_ptr());
    ArrayCheckBounds(ndim, dim.as_ptr(), lBound.as_ptr());

    /*
     * We arrange to look up info about element type, including its receive
     * conversion proc, only once per series of calls ...
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
    if my_extra.is_null() {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::size_of::<ArrayMetaState>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
        (*my_extra).element_type = !element_type;
    }

    if (*my_extra).element_type != element_type {
        /* Get info about element type, including its receive proc */
        get_type_io_data(
            element_type,
            IOFunc_receive,
            &raw mut (*my_extra).typlen,
            &raw mut (*my_extra).typbyval,
            &raw mut (*my_extra).typalign,
            &raw mut (*my_extra).typdelim,
            &raw mut (*my_extra).typioparam,
            &raw mut (*my_extra).typiofunc,
        );
        if !OidIsValid((*my_extra).typiofunc) {
            ereport!(
                ERROR,
                errmsg!(
                    "no binary input function available for type {}",
                    std::ffi::CStr::from_ptr(format_type_be(element_type)).to_string_lossy()
                )
            );
        }
        fmgr_info_cxt(
            (*my_extra).typiofunc,
            &raw mut (*my_extra).proc,
            (*(*fcinfo).flinfo).fn_mcxt,
        );
        (*my_extra).element_type = element_type;
    }

    if nitems == 0 {
        /* Return empty array ... but not till we've validated element_type */
        PG_RETURN_ARRAYTYPE_P!(construct_empty_array(element_type));
    }

    typlen = (*my_extra).typlen as c_int;
    typbyval = (*my_extra).typbyval;
    typalign = (*my_extra).typalign;
    typioparam = (*my_extra).typioparam;

    dataPtr = palloc(nitems as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nullsPtr = palloc(nitems as usize * core::mem::size_of::<bool>()) as *mut bool;
    ReadArrayBinary(
        buf,
        nitems,
        &raw mut (*my_extra).proc,
        typioparam,
        typmod,
        typlen,
        typbyval,
        typalign,
        dataPtr,
        nullsPtr,
        &raw mut hasnulls,
        &raw mut nbytes,
    );
    if hasnulls {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndim, nitems) as int32;
        nbytes += dataoffset;
    } else {
        dataoffset = 0; /* marker for no null bitmap */
        nbytes += ARR_OVERHEAD_NONULLS(ndim) as int32;
    }
    retval = palloc0(nbytes as usize) as *mut ArrayType;
    SET_VARSIZE(retval as *mut c_char, nbytes);
    (*retval).ndim = ndim;
    (*retval).dataoffset = dataoffset;
    (*retval).elemtype = element_type;
    memcpy(
        ARR_DIMS(retval) as *mut c_void,
        dim.as_ptr() as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        ARR_LBOUND(retval) as *mut c_void,
        lBound.as_ptr() as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );

    CopyArrayEls(
        retval, dataPtr, nullsPtr, nitems, typlen, typbyval, typalign, true,
    );

    pfree(dataPtr as *mut c_void);
    pfree(nullsPtr as *mut c_void);

    PG_RETURN_ARRAYTYPE_P!(retval);
}

/*
 * ReadArrayBinary:
 *	 collect the data elements of an array being read in binary style.
 */
unsafe fn ReadArrayBinary(
    buf: StringInfo,
    nitems: c_int,
    receiveproc: *mut FmgrInfo,
    typioparam: Oid,
    typmod: int32,
    typlen: c_int,
    typbyval: bool,
    typalign: c_char,
    values: *mut Datum,
    nulls: *mut bool,
    hasnulls: *mut bool,
    nbytes: *mut int32,
) {
    let mut hasnull: bool;
    let mut totbytes: int32;

    for i in 0..nitems as usize {
        let itemlen: c_int;
        let mut elem_buf = core::mem::MaybeUninit::<StringInfoData>::uninit();

        /* Get and check the item length */
        itemlen = pq_getmsgint(buf, 4) as c_int;
        if itemlen < -1 || itemlen > ((*buf).len - (*buf).cursor) {
            ereport!(ERROR, errmsg!("insufficient data left in message"));
        }

        if itemlen == -1 {
            /* -1 length means NULL */
            *values.add(i) = ReceiveFunctionCall(receiveproc, null_mut(), typioparam, typmod);
            *nulls.add(i) = true;
            continue;
        }

        /*
         * Rather than copying data around, we just initialize a StringInfo
         * pointing to the correct portion of the message buffer.
         */
        initReadOnlyStringInfo(
            elem_buf.as_mut_ptr(),
            (*buf).data.add((*buf).cursor as usize),
            itemlen,
        );
        let elem_buf = elem_buf.as_mut_ptr();

        (*buf).cursor += itemlen;

        /* Now call the element's receiveproc */
        *values.add(i) = ReceiveFunctionCall(receiveproc, elem_buf, typioparam, typmod);
        *nulls.add(i) = false;

        /* Trouble if it didn't eat the whole buffer */
        if (*elem_buf).cursor != itemlen {
            ereport!(
                ERROR,
                errmsg!("improper binary format in array element {}", i + 1)
            );
        }
    }

    /*
     * Check for nulls, compute total data space needed
     */
    hasnull = false;
    totbytes = 0;
    for i in 0..nitems as usize {
        if *nulls.add(i) {
            hasnull = true;
        } else {
            /* let's just make sure data is not toasted */
            if typlen == -1 {
                *values.add(i) =
                    PointerGetDatum(crate::PG_DETOAST_DATUM!(*values.add(i)) as *const c_void);
            }
            totbytes = att_addlength_datum(totbytes as usize, typlen, *values.add(i)) as int32;
            totbytes = att_align_nominal(totbytes as usize, typalign) as int32;
            /* check for overflow of total request */
            if !AllocSizeIsValid(totbytes as Size) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "array size exceeds the maximum allowed ({})",
                        MaxAllocSize as c_int
                    )
                );
            }
        }
    }
    *hasnulls = hasnull;
    *nbytes = totbytes;
}

/*
 * array_send :
 *		  takes the internal representation of an array and returns a bytea
 *		  containing the array in its external binary format.
 */
pub unsafe fn array_send(fcinfo: FunctionCallInfo) -> Datum {
    let v = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let element_type = AARR_ELEMTYPE(v);
    let typlen: c_int;
    let typbyval: bool;
    let typalign: c_char;
    let nitems: c_int;
    let mut i: c_int;
    let ndim: c_int;
    let dim: *mut c_int;
    let lb: *mut c_int;
    let mut buf = core::mem::MaybeUninit::<StringInfoData>::uninit();
    let mut iter = core::mem::MaybeUninit::<array_iter>::uninit();
    let mut my_extra: *mut ArrayMetaState;

    /*
     * We arrange to look up info about element type, including its send
     * conversion proc, only once per series of calls ...
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
    if my_extra.is_null() {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::size_of::<ArrayMetaState>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
        (*my_extra).element_type = !element_type;
    }

    if (*my_extra).element_type != element_type {
        /* Get info about element type, including its send proc */
        get_type_io_data(
            element_type,
            IOFunc_send,
            &raw mut (*my_extra).typlen,
            &raw mut (*my_extra).typbyval,
            &raw mut (*my_extra).typalign,
            &raw mut (*my_extra).typdelim,
            &raw mut (*my_extra).typioparam,
            &raw mut (*my_extra).typiofunc,
        );
        if !OidIsValid((*my_extra).typiofunc) {
            ereport!(
                ERROR,
                errmsg!(
                    "no binary output function available for type {}",
                    std::ffi::CStr::from_ptr(format_type_be(element_type)).to_string_lossy()
                )
            );
        }
        fmgr_info_cxt(
            (*my_extra).typiofunc,
            &raw mut (*my_extra).proc,
            (*(*fcinfo).flinfo).fn_mcxt,
        );
        (*my_extra).element_type = element_type;
    }
    typlen = (*my_extra).typlen as c_int;
    typbyval = (*my_extra).typbyval;
    typalign = (*my_extra).typalign;

    ndim = AARR_NDIM(v);
    dim = AARR_DIMS(v);
    lb = AARR_LBOUND(v);
    nitems = ArrayGetNItems(ndim, dim);

    pq_begintypsend(buf.as_mut_ptr());
    let buf = buf.as_mut_ptr();

    /* Send the array header information */
    pq_sendint32(buf, ndim as uint32);
    pq_sendint32(buf, if AARR_HASNULL(v) { 1 } else { 0 });
    pq_sendint32(buf, element_type);
    i = 0;
    while i < ndim {
        pq_sendint32(buf, *dim.add(i as usize) as uint32);
        pq_sendint32(buf, *lb.add(i as usize) as uint32);
        i += 1;
    }

    /* Send the array elements using the element's own sendproc */
    array_iter_setup(iter.as_mut_ptr(), v);
    let iter = iter.as_mut_ptr();

    i = 0;
    while i < nitems {
        let itemvalue: Datum;
        let mut isnull = false;

        /* Get source element, checking for NULL */
        itemvalue = array_iter_next(iter, &raw mut isnull, i, typlen, typbyval, typalign);

        if isnull {
            /* -1 length means a NULL */
            pq_sendint32(buf, -1i32 as uint32);
        } else {
            let outputbytes: *mut bytea;

            outputbytes = SendFunctionCall(&raw mut (*my_extra).proc, itemvalue);
            pq_sendint32(
                buf,
                (VARSIZE(outputbytes as *const c_char) as int32 - crate::c::VARHDRSZ) as uint32,
            );
            pq_sendbytes(
                buf,
                VARDATA(outputbytes as *const c_char) as *const c_void,
                VARSIZE(outputbytes as *const c_char) as int32 - crate::c::VARHDRSZ,
            );
            pfree(outputbytes as *mut c_void);
        }
        i += 1;
    }

    PG_RETURN_BYTEA_P!(pq_endtypsend(buf));
}

/*
 * array_ndims :
 *		  returns the number of dimensions of the array pointed to by "v"
 */
pub unsafe fn array_ndims(fcinfo: FunctionCallInfo) -> Datum {
    let v = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);

    /* Sanity check: does it look like an array at all? */
    if AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_INT32!(AARR_NDIM(v));
}

/*
 * array_dims :
 *		  returns the dimensions of the array pointed to by "v", as a "text"
 */
pub unsafe fn array_dims(fcinfo: FunctionCallInfo) -> Datum {
    let v = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let mut p: *mut c_char;
    let mut i: c_int;
    let dimv: *mut c_int;
    let lb: *mut c_int;

    /*
     * 33 since we assume 15 digits per number + ':' +'[]'
     *
     * +1 for trailing null
     */
    let mut buf: [c_char; MAXDIM as usize * 33 + 1] = [0; MAXDIM as usize * 33 + 1];

    /* Sanity check: does it look like an array at all? */
    if AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM {
        PG_RETURN_NULL!(fcinfo);
    }

    dimv = AARR_DIMS(v);
    lb = AARR_LBOUND(v);

    p = buf.as_mut_ptr();
    i = 0;
    while i < AARR_NDIM(v) {
        sprintf(
            p,
            c"[%d:%d]".as_ptr(),
            *lb.add(i as usize),
            *dimv.add(i as usize) + *lb.add(i as usize) - 1,
        );
        p = p.add(strlen(p));
        i += 1;
    }

    PG_RETURN_TEXT_P!(cstring_to_text(buf.as_ptr()));
}

/*
 * array_lower :
 *		returns the lower dimension, of the DIM requested, for
 *		the array pointed to by "v", as an int4
 */
pub unsafe fn array_lower(fcinfo: FunctionCallInfo) -> Datum {
    let v = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let reqdim = PG_GETARG_INT32!(fcinfo, 1);
    let lb: *mut c_int;
    let result: c_int;

    /* Sanity check: does it look like an array at all? */
    if AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM {
        PG_RETURN_NULL!(fcinfo);
    }

    /* Sanity check: was the requested dim valid */
    if reqdim <= 0 || reqdim > AARR_NDIM(v) {
        PG_RETURN_NULL!(fcinfo);
    }

    lb = AARR_LBOUND(v);
    result = *lb.add((reqdim - 1) as usize);

    PG_RETURN_INT32!(result);
}

/*
 * array_upper :
 *		returns the upper dimension, of the DIM requested, for
 *		the array pointed to by "v", as an int4
 */
pub unsafe fn array_upper(fcinfo: FunctionCallInfo) -> Datum {
    let v = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let reqdim = PG_GETARG_INT32!(fcinfo, 1);
    let dimv: *mut c_int;
    let lb: *mut c_int;
    let result: c_int;

    /* Sanity check: does it look like an array at all? */
    if AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM {
        PG_RETURN_NULL!(fcinfo);
    }

    /* Sanity check: was the requested dim valid */
    if reqdim <= 0 || reqdim > AARR_NDIM(v) {
        PG_RETURN_NULL!(fcinfo);
    }

    lb = AARR_LBOUND(v);
    dimv = AARR_DIMS(v);

    result = *dimv.add((reqdim - 1) as usize) + *lb.add((reqdim - 1) as usize) - 1;

    PG_RETURN_INT32!(result);
}

/*
 * array_length :
 *		returns the length, of the dimension requested, for
 *		the array pointed to by "v", as an int4
 */
pub unsafe fn array_length(fcinfo: FunctionCallInfo) -> Datum {
    let v = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let reqdim = PG_GETARG_INT32!(fcinfo, 1);
    let dimv: *mut c_int;
    let result: c_int;

    /* Sanity check: does it look like an array at all? */
    if AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM {
        PG_RETURN_NULL!(fcinfo);
    }

    /* Sanity check: was the requested dim valid */
    if reqdim <= 0 || reqdim > AARR_NDIM(v) {
        PG_RETURN_NULL!(fcinfo);
    }

    dimv = AARR_DIMS(v);

    result = *dimv.add((reqdim - 1) as usize);

    PG_RETURN_INT32!(result);
}

/*
 * array_cardinality:
 *		returns the total number of elements in an array
 */
pub unsafe fn array_cardinality(fcinfo: FunctionCallInfo) -> Datum {
    let v = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);

    PG_RETURN_INT32!(ArrayGetNItems(AARR_NDIM(v), AARR_DIMS(v)));
}

/*
 * array_get_element :
 *	  This routine takes an array datum and a subscript array and returns
 *	  the referenced item as a Datum.  [see arrayfuncs.c]
 */
pub unsafe fn array_get_element(
    arraydatum: Datum,
    nSubscripts: c_int,
    indx: *mut c_int,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
    isNull: *mut bool,
) -> Datum {
    let ndim: c_int;
    let dim: *mut c_int;
    let lb: *mut c_int;
    let offset: c_int;
    let mut fixedDim: [c_int; 1] = [0; 1];
    let mut fixedLb: [c_int; 1] = [0; 1];
    let arraydataptr: *mut c_char;
    let retptr: *mut c_char;
    let arraynullsptr: *mut bits8;

    if arraytyplen > 0 {
        /*
         * fixed-length arrays -- these are assumed to be 1-d, 0-based
         */
        ndim = 1;
        fixedDim[0] = arraytyplen / elmlen;
        fixedLb[0] = 0;
        dim = fixedDim.as_mut_ptr();
        lb = fixedLb.as_mut_ptr();
        arraydataptr = DatumGetPointer(arraydatum) as *mut c_char;
        arraynullsptr = null_mut();
    } else if VARATT_IS_EXPANDED_HEADER(DatumGetPointer(arraydatum) as *const c_void) {
        /* expanded array: let's do this in a separate function */
        return array_get_element_expanded(
            arraydatum,
            nSubscripts,
            indx,
            arraytyplen,
            elmlen,
            elmbyval,
            elmalign,
            isNull,
        );
    } else {
        /* detoast array if necessary, producing normal varlena input */
        let array = DatumGetArrayTypeP(arraydatum);

        ndim = ARR_NDIM(array);
        dim = ARR_DIMS(array);
        lb = ARR_LBOUND(array);
        arraydataptr = ARR_DATA_PTR(array);
        arraynullsptr = ARR_NULLBITMAP(array);
    }

    /*
     * Return NULL for invalid subscript
     */
    if ndim != nSubscripts || ndim <= 0 || ndim > MAXDIM {
        *isNull = true;
        return 0 as Datum;
    }
    for i in 0..ndim as usize {
        if *indx.add(i) < *lb.add(i) || *indx.add(i) >= (*dim.add(i) + *lb.add(i)) {
            *isNull = true;
            return 0 as Datum;
        }
    }

    /*
     * Calculate the element number
     */
    offset = ArrayGetOffset(nSubscripts, dim, lb, indx);

    /*
     * Check for NULL array element
     */
    if array_get_isnull(arraynullsptr, offset) {
        *isNull = true;
        return 0 as Datum;
    }

    /*
     * OK, get the element
     */
    *isNull = false;
    retptr = array_seek(
        arraydataptr,
        0,
        arraynullsptr,
        offset,
        elmlen,
        elmbyval,
        elmalign,
    );
    ArrayCast(retptr, elmbyval, elmlen)
}

/*
 * Implementation of array_get_element() for an expanded array
 */
unsafe fn array_get_element_expanded(
    arraydatum: Datum,
    nSubscripts: c_int,
    indx: *mut c_int,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
    isNull: *mut bool,
) -> Datum {
    let eah: *mut ExpandedArrayHeader;
    let ndim: c_int;
    let dim: *mut c_int;
    let lb: *mut c_int;
    let offset: c_int;
    let dvalues: *mut Datum;
    let dnulls: *mut bool;

    eah = DatumGetEOHP(arraydatum) as *mut ExpandedArrayHeader;
    Assert!((*eah).ea_magic == crate::utils::adt::array_expanded::EA_MAGIC);

    /* sanity-check caller's info against object */
    Assert!(arraytyplen == -1);
    Assert!(elmlen == (*eah).typlen as c_int);
    Assert!(elmbyval == (*eah).typbyval);
    Assert!(elmalign == (*eah).typalign);

    ndim = (*eah).ndims;
    dim = (*eah).dims;
    lb = (*eah).lbound;

    /*
     * Return NULL for invalid subscript
     */
    if ndim != nSubscripts || ndim <= 0 || ndim > MAXDIM {
        *isNull = true;
        return 0 as Datum;
    }
    for i in 0..ndim as usize {
        if *indx.add(i) < *lb.add(i) || *indx.add(i) >= (*dim.add(i) + *lb.add(i)) {
            *isNull = true;
            return 0 as Datum;
        }
    }

    /*
     * Calculate the element number
     */
    offset = ArrayGetOffset(nSubscripts, dim, lb, indx);

    /*
     * Deconstruct array if we didn't already. ...
     */
    deconstruct_expanded_array(eah);

    dvalues = (*eah).dvalues;
    dnulls = (*eah).dnulls;

    /*
     * Check for NULL array element
     */
    if !dnulls.is_null() && *dnulls.add(offset as usize) {
        *isNull = true;
        return 0 as Datum;
    }

    /*
     * OK, get the element. ...
     */
    *isNull = false;
    *dvalues.add(offset as usize)
}

/*
 * array_get_slice :
 *		   This routine takes an array and a range of indices (upperIndx and
 *		   lowerIndx), creates a new array structure for the referred elements
 *		   and returns a pointer to it.  [see arrayfuncs.c]
 */
pub unsafe fn array_get_slice(
    arraydatum: Datum,
    nSubscripts: c_int,
    upperIndx: *mut c_int,
    lowerIndx: *mut c_int,
    upperProvided: *mut bool,
    lowerProvided: *mut bool,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
) -> Datum {
    let mut array: *mut ArrayType = null_mut();
    let newarray: *mut ArrayType;
    let mut i: c_int;
    let ndim: c_int;
    let dim: *mut c_int;
    let lb: *mut c_int;
    let newlb: *mut c_int;
    let mut fixedDim: [c_int; 1] = [0; 1];
    let mut fixedLb: [c_int; 1] = [0; 1];
    let elemtype: Oid;
    let arraydataptr: *mut c_char;
    let arraynullsptr: *mut bits8;
    let dataoffset: int32;
    let mut bytes: c_int;
    let mut span: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];

    if arraytyplen > 0 {
        /*
         * fixed-length arrays -- currently, cannot slice these ...
         */
        ereport!(ERROR, errmsg!("slices of fixed-length arrays not implemented"));

        /*
         * fixed-length arrays -- these are assumed to be 1-d, 0-based
         */
        ndim = 1;
        fixedDim[0] = arraytyplen / elmlen;
        fixedLb[0] = 0;
        dim = fixedDim.as_mut_ptr();
        lb = fixedLb.as_mut_ptr();
        elemtype = InvalidOid; /* XXX */
        arraydataptr = DatumGetPointer(arraydatum) as *mut c_char;
        arraynullsptr = null_mut();
    } else {
        /* detoast input array if necessary */
        array = DatumGetArrayTypeP(arraydatum);

        ndim = ARR_NDIM(array);
        dim = ARR_DIMS(array);
        lb = ARR_LBOUND(array);
        elemtype = ARR_ELEMTYPE(array);
        arraydataptr = ARR_DATA_PTR(array);
        arraynullsptr = ARR_NULLBITMAP(array);
    }

    /*
     * Check provided subscripts. ...
     */
    if ndim < nSubscripts || ndim <= 0 || ndim > MAXDIM {
        return PointerGetDatum(construct_empty_array(elemtype) as *const c_void);
    }

    i = 0;
    while i < nSubscripts {
        let iu = i as usize;
        if !*lowerProvided.add(iu) || *lowerIndx.add(iu) < *lb.add(iu) {
            *lowerIndx.add(iu) = *lb.add(iu);
        }
        if !*upperProvided.add(iu) || *upperIndx.add(iu) >= (*dim.add(iu) + *lb.add(iu)) {
            *upperIndx.add(iu) = *dim.add(iu) + *lb.add(iu) - 1;
        }
        if *lowerIndx.add(iu) > *upperIndx.add(iu) {
            return PointerGetDatum(construct_empty_array(elemtype) as *const c_void);
        }
        i += 1;
    }
    /* fill any missing subscript positions with full array range */
    while i < ndim {
        let iu = i as usize;
        *lowerIndx.add(iu) = *lb.add(iu);
        *upperIndx.add(iu) = *dim.add(iu) + *lb.add(iu) - 1;
        if *lowerIndx.add(iu) > *upperIndx.add(iu) {
            return PointerGetDatum(construct_empty_array(elemtype) as *const c_void);
        }
        i += 1;
    }

    mda_get_range(ndim, span.as_mut_ptr(), lowerIndx, upperIndx);

    bytes = array_slice_size(
        arraydataptr,
        arraynullsptr,
        ndim,
        dim,
        lb,
        lowerIndx,
        upperIndx,
        elmlen,
        elmbyval,
        elmalign,
    );

    /*
     * Currently, we put a null bitmap in the result if the source has one ...
     */
    if !arraynullsptr.is_null() {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndim, ArrayGetNItems(ndim, span.as_ptr())) as int32;
        bytes += dataoffset;
    } else {
        dataoffset = 0; /* marker for no null bitmap */
        bytes += ARR_OVERHEAD_NONULLS(ndim) as c_int;
    }

    newarray = palloc0(bytes as usize) as *mut ArrayType;
    SET_VARSIZE(newarray as *mut c_char, bytes);
    (*newarray).ndim = ndim;
    (*newarray).dataoffset = dataoffset;
    (*newarray).elemtype = elemtype;
    memcpy(
        ARR_DIMS(newarray) as *mut c_void,
        span.as_ptr() as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );

    /*
     * Lower bounds of the new array are set to 1. ...
     */
    newlb = ARR_LBOUND(newarray);
    i = 0;
    while i < ndim {
        *newlb.add(i as usize) = 1;
        i += 1;
    }

    array_extract_slice(
        newarray,
        ndim,
        dim,
        lb,
        arraydataptr,
        arraynullsptr,
        lowerIndx,
        upperIndx,
        elmlen,
        elmbyval,
        elmalign,
    );

    let _ = array;
    PointerGetDatum(newarray as *const c_void)
}

/*
 * array_set_element :
 *		  This routine sets the value of one array element (specified by
 *		  a subscript array) to a new value specified by "dataValue".
 *		  [see arrayfuncs.c]
 */
pub unsafe fn array_set_element(
    arraydatum: Datum,
    nSubscripts: c_int,
    indx: *mut c_int,
    mut dataValue: Datum,
    isNull: bool,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
) -> Datum {
    let array: *mut ArrayType;
    let newarray: *mut ArrayType;
    let mut i: c_int;
    let ndim: c_int;
    let mut dim: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut lb: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut offset: c_int = 0;
    let elt_ptr: *mut c_char;
    let mut newhasnulls: bool;
    let oldnullbitmap: *mut bits8;
    let oldnitems: c_int;
    let newnitems: c_int;
    let olddatasize: c_int;
    let newsize: c_int;
    let mut olditemlen: c_int = 0;
    let newitemlen: c_int;
    let overheadlen: c_int;
    let oldoverheadlen: c_int;
    let mut addedbefore: c_int;
    let mut addedafter: c_int;
    let mut lenbefore: c_int = 0;
    let mut lenafter: c_int = 0;

    if arraytyplen > 0 {
        /*
         * fixed-length arrays -- these are assumed to be 1-d, 0-based. We
         * cannot extend them, either.
         */
        let resultarray: *mut c_char;

        if nSubscripts != 1 {
            ereport!(ERROR, errmsg!("wrong number of array subscripts"));
        }

        if *indx.add(0) < 0 || *indx.add(0) >= arraytyplen / elmlen {
            ereport!(ERROR, errmsg!("array subscript out of range"));
        }

        if isNull {
            ereport!(
                ERROR,
                errmsg!("cannot assign null value to an element of a fixed-length array")
            );
        }

        resultarray = palloc(arraytyplen as usize) as *mut c_char;
        memcpy(
            resultarray as *mut c_void,
            DatumGetPointer(arraydatum) as *const c_void,
            arraytyplen as usize,
        );
        elt_ptr = resultarray.add((*indx.add(0) * elmlen) as usize);
        ArrayCastAndSet(dataValue, elmlen, elmbyval, elmalign, elt_ptr);
        return PointerGetDatum(resultarray as *const c_void);
    }

    if nSubscripts <= 0 || nSubscripts > MAXDIM {
        ereport!(ERROR, errmsg!("wrong number of array subscripts"));
    }

    /* make sure item to be inserted is not toasted */
    if elmlen == -1 && !isNull {
        dataValue = PointerGetDatum(crate::PG_DETOAST_DATUM!(dataValue) as *const c_void);
    }

    if VARATT_IS_EXPANDED_HEADER(DatumGetPointer(arraydatum) as *const c_void) {
        /* expanded array: let's do this in a separate function */
        return array_set_element_expanded(
            arraydatum,
            nSubscripts,
            indx,
            dataValue,
            isNull,
            arraytyplen,
            elmlen,
            elmbyval,
            elmalign,
        );
    }

    /* detoast input array if necessary */
    array = DatumGetArrayTypeP(arraydatum);

    ndim = ARR_NDIM(array);

    /*
     * if number of dims is zero, i.e. an empty array, create an array with
     * nSubscripts dimensions ...
     */
    if ndim == 0 {
        let elmtype = ARR_ELEMTYPE(array);

        for i in 0..nSubscripts as usize {
            dim[i] = 1;
            lb[i] = *indx.add(i);
        }

        let mut dataValue_l = dataValue;
        let mut isNull_l = isNull;
        return PointerGetDatum(construct_md_array(
            &raw mut dataValue_l,
            &raw mut isNull_l,
            nSubscripts,
            dim.as_mut_ptr(),
            lb.as_mut_ptr(),
            elmtype,
            elmlen,
            elmbyval,
            elmalign,
        ) as *const c_void);
    }

    if ndim != nSubscripts {
        ereport!(ERROR, errmsg!("wrong number of array subscripts"));
    }

    /* copy dim/lb since we may modify them */
    memcpy(
        dim.as_mut_ptr() as *mut c_void,
        ARR_DIMS(array) as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        lb.as_mut_ptr() as *mut c_void,
        ARR_LBOUND(array) as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );

    newhasnulls = ARR_HASNULL(array) || isNull;
    addedbefore = 0;
    addedafter = 0;

    /*
     * Check subscripts. ...
     */
    if ndim == 1 {
        if *indx.add(0) < lb[0] {
            if pg_sub_s32_overflow(lb[0], *indx.add(0), &raw mut addedbefore)
                || pg_add_s32_overflow(dim[0], addedbefore, &raw mut dim[0])
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "array size exceeds the maximum allowed ({})",
                        MaxArraySize as c_int
                    )
                );
            }
            lb[0] = *indx.add(0);
            if addedbefore > 1 {
                newhasnulls = true; /* will insert nulls */
            }
        }
        if *indx.add(0) >= (dim[0] + lb[0]) {
            if pg_sub_s32_overflow(*indx.add(0), dim[0] + lb[0], &raw mut addedafter)
                || pg_add_s32_overflow(addedafter, 1, &raw mut addedafter)
                || pg_add_s32_overflow(dim[0], addedafter, &raw mut dim[0])
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "array size exceeds the maximum allowed ({})",
                        MaxArraySize as c_int
                    )
                );
            }
            if addedafter > 1 {
                newhasnulls = true; /* will insert nulls */
            }
        }
    } else {
        /*
         * XXX currently we do not support extending multi-dimensional arrays
         * during assignment
         */
        for i in 0..ndim as usize {
            if *indx.add(i) < lb[i] || *indx.add(i) >= (dim[i] + lb[i]) {
                ereport!(ERROR, errmsg!("array subscript out of range"));
            }
        }
    }

    /* This checks for overflow of the array dimensions */
    newnitems = ArrayGetNItems(ndim, dim.as_ptr());
    ArrayCheckBounds(ndim, dim.as_ptr(), lb.as_ptr());

    /*
     * Compute sizes of items and areas to copy
     */
    if newhasnulls {
        overheadlen = ARR_OVERHEAD_WITHNULLS(ndim, newnitems) as c_int;
    } else {
        overheadlen = ARR_OVERHEAD_NONULLS(ndim) as c_int;
    }
    oldnitems = ArrayGetNItems(ndim, ARR_DIMS(array));
    oldnullbitmap = ARR_NULLBITMAP(array);
    oldoverheadlen = ARR_DATA_OFFSET(array) as c_int;
    olddatasize = ARR_SIZE(array) as c_int - oldoverheadlen;
    if addedbefore != 0 {
        offset = 0;
        lenbefore = 0;
        olditemlen = 0;
        lenafter = olddatasize;
    } else if addedafter != 0 {
        offset = oldnitems;
        lenbefore = olddatasize;
        olditemlen = 0;
        lenafter = 0;
    } else {
        offset = ArrayGetOffset(nSubscripts, dim.as_ptr(), lb.as_ptr(), indx);
        elt_ptr = array_seek(
            ARR_DATA_PTR(array),
            0,
            oldnullbitmap,
            offset,
            elmlen,
            elmbyval,
            elmalign,
        );
        lenbefore = elt_ptr.offset_from(ARR_DATA_PTR(array)) as c_int;
        if array_get_isnull(oldnullbitmap, offset) {
            olditemlen = 0;
        } else {
            olditemlen = att_addlength_pointer(0, elmlen, elt_ptr) as c_int;
            olditemlen = att_align_nominal(olditemlen as usize, elmalign) as c_int;
        }
        lenafter = olddatasize - lenbefore - olditemlen;
    }

    let newitemlen_v: c_int;
    if isNull {
        newitemlen_v = 0;
    } else {
        let mut nl = att_addlength_datum(0, elmlen, dataValue) as c_int;
        nl = att_align_nominal(nl as usize, elmalign) as c_int;
        newitemlen_v = nl;
    }
    newitemlen = newitemlen_v;

    newsize = overheadlen + lenbefore + newitemlen + lenafter;

    /*
     * OK, create the new array and fill in header/dimensions
     */
    newarray = palloc0(newsize as usize) as *mut ArrayType;
    SET_VARSIZE(newarray as *mut c_char, newsize);
    (*newarray).ndim = ndim;
    (*newarray).dataoffset = if newhasnulls { overheadlen } else { 0 };
    (*newarray).elemtype = ARR_ELEMTYPE(array);
    memcpy(
        ARR_DIMS(newarray) as *mut c_void,
        dim.as_ptr() as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        ARR_LBOUND(newarray) as *mut c_void,
        lb.as_ptr() as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );

    /*
     * Fill in data
     */
    memcpy(
        (newarray as *mut c_char).add(overheadlen as usize) as *mut c_void,
        (array as *mut c_char).add(oldoverheadlen as usize) as *const c_void,
        lenbefore as usize,
    );
    if !isNull {
        ArrayCastAndSet(
            dataValue,
            elmlen,
            elmbyval,
            elmalign,
            (newarray as *mut c_char).add((overheadlen + lenbefore) as usize),
        );
    }
    memcpy(
        (newarray as *mut c_char).add((overheadlen + lenbefore + newitemlen) as usize)
            as *mut c_void,
        (array as *mut c_char).add((oldoverheadlen + lenbefore + olditemlen) as usize)
            as *const c_void,
        lenafter as usize,
    );

    /*
     * Fill in nulls bitmap if needed ...
     */
    if newhasnulls {
        let newnullbitmap = ARR_NULLBITMAP(newarray);

        /* palloc0 above already marked any inserted positions as nulls */
        /* Fix the inserted value */
        if addedafter != 0 {
            array_set_isnull(newnullbitmap, newnitems - 1, isNull);
        } else {
            array_set_isnull(newnullbitmap, offset, isNull);
        }
        /* Fix the copied range(s) */
        if addedbefore != 0 {
            array_bitmap_copy(newnullbitmap, addedbefore, oldnullbitmap, 0, oldnitems);
        } else {
            array_bitmap_copy(newnullbitmap, 0, oldnullbitmap, 0, offset);
            if addedafter == 0 {
                array_bitmap_copy(
                    newnullbitmap,
                    offset + 1,
                    oldnullbitmap,
                    offset + 1,
                    oldnitems - offset - 1,
                );
            }
        }
    }

    PointerGetDatum(newarray as *const c_void)
}

/*
 * Implementation of array_set_element() for an expanded array
 *
 * Note: as with any operation on a read/write expanded object, we must
 * take pains not to leave the object in a corrupt state if we fail partway
 * through.
 */
unsafe fn array_set_element_expanded(
    arraydatum: Datum,
    nSubscripts: c_int,
    indx: *mut c_int,
    mut dataValue: Datum,
    isNull: bool,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
) -> Datum {
    let eah: *mut ExpandedArrayHeader;
    let mut dvalues: *mut Datum;
    let mut dnulls: *mut bool;
    let mut i: c_int;
    let mut ndim: c_int;
    let mut dim: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut lb: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let offset: c_int;
    let mut dimschanged: bool;
    let mut newhasnulls: bool;
    let mut addedbefore: c_int;
    let mut addedafter: c_int;
    let oldValue: *mut c_char;

    /* Convert to R/W object if not so already */
    eah = DatumGetExpandedArray(arraydatum);

    /* Sanity-check caller's info against object; we don't use it otherwise */
    Assert!(arraytyplen == -1);
    Assert!(elmlen == (*eah).typlen as c_int);
    Assert!(elmbyval == (*eah).typbyval);
    Assert!(elmalign == (*eah).typalign);

    /*
     * Copy dimension info into local storage. ...
     */
    ndim = (*eah).ndims;
    Assert!(ndim >= 0 && ndim <= MAXDIM);
    memcpy(
        dim.as_mut_ptr() as *mut c_void,
        (*eah).dims as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        lb.as_mut_ptr() as *mut c_void,
        (*eah).lbound as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    dimschanged = false;

    /*
     * if number of dims is zero ... create an array with nSubscripts dimensions
     */
    if ndim == 0 {
        Assert!(nSubscripts > 0 && nSubscripts <= MAXDIM);
        (*eah).dims = MemoryContextAllocZero(
            (*eah).hdr.eoh_context as crate::utils::palloc::MemoryContext,
            nSubscripts as usize * core::mem::size_of::<c_int>(),
        ) as *mut c_int;
        (*eah).lbound = MemoryContextAllocZero(
            (*eah).hdr.eoh_context as crate::utils::palloc::MemoryContext,
            nSubscripts as usize * core::mem::size_of::<c_int>(),
        ) as *mut c_int;

        /* Update local copies of dimension info */
        ndim = nSubscripts;
        for i in 0..nSubscripts as usize {
            dim[i] = 0;
            lb[i] = *indx.add(i);
        }
        dimschanged = true;
    } else if ndim != nSubscripts {
        ereport!(ERROR, errmsg!("wrong number of array subscripts"));
    }

    /*
     * Deconstruct array if we didn't already. ...
     */
    deconstruct_expanded_array(eah);

    /*
     * Copy new element into array's context, if needed ...
     */
    if !(*eah).typbyval && !isNull {
        let oldcxt = MemoryContextSwitchTo((*eah).hdr.eoh_context as crate::utils::palloc::MemoryContext);

        dataValue = datumCopy(dataValue, false, (*eah).typlen as c_int);
        MemoryContextSwitchTo(oldcxt);
    }

    dvalues = (*eah).dvalues;
    dnulls = (*eah).dnulls;

    newhasnulls = !dnulls.is_null() || isNull;
    addedbefore = 0;
    addedafter = 0;

    /*
     * Check subscripts (this logic must match array_set_element). ...
     */
    if ndim == 1 {
        if *indx.add(0) < lb[0] {
            if pg_sub_s32_overflow(lb[0], *indx.add(0), &raw mut addedbefore)
                || pg_add_s32_overflow(dim[0], addedbefore, &raw mut dim[0])
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "array size exceeds the maximum allowed ({})",
                        MaxArraySize as c_int
                    )
                );
            }
            lb[0] = *indx.add(0);
            dimschanged = true;
            if addedbefore > 1 {
                newhasnulls = true; /* will insert nulls */
            }
        }
        if *indx.add(0) >= (dim[0] + lb[0]) {
            if pg_sub_s32_overflow(*indx.add(0), dim[0] + lb[0], &raw mut addedafter)
                || pg_add_s32_overflow(addedafter, 1, &raw mut addedafter)
                || pg_add_s32_overflow(dim[0], addedafter, &raw mut dim[0])
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "array size exceeds the maximum allowed ({})",
                        MaxArraySize as c_int
                    )
                );
            }
            dimschanged = true;
            if addedafter > 1 {
                newhasnulls = true; /* will insert nulls */
            }
        }
    } else {
        /*
         * XXX currently we do not support extending multi-dimensional arrays
         * during assignment
         */
        for i in 0..ndim as usize {
            if *indx.add(i) < lb[i] || *indx.add(i) >= (dim[i] + lb[i]) {
                ereport!(ERROR, errmsg!("array subscript out of range"));
            }
        }
    }

    /* Check for overflow of the array dimensions */
    if dimschanged {
        ArrayGetNItems(ndim, dim.as_ptr());
        ArrayCheckBounds(ndim, dim.as_ptr(), lb.as_ptr());
    }

    /* Now we can calculate linear offset of target item in array */
    offset = ArrayGetOffset(nSubscripts, dim.as_ptr(), lb.as_ptr(), indx);

    /* Physically enlarge existing dvalues/dnulls arrays if needed */
    if dim[0] > (*eah).dvalueslen {
        /* We want some extra space if we're enlarging */
        let mut newlen = dim[0] + dim[0] / 8;

        newlen = core::cmp::max(newlen, dim[0]); /* integer overflow guard */
        dvalues = repalloc(
            dvalues as *mut c_void,
            newlen as usize * core::mem::size_of::<Datum>(),
        ) as *mut Datum;
        (*eah).dvalues = dvalues;
        if !dnulls.is_null() {
            dnulls = repalloc(
                dnulls as *mut c_void,
                newlen as usize * core::mem::size_of::<bool>(),
            ) as *mut bool;
            (*eah).dnulls = dnulls;
        }
        (*eah).dvalueslen = newlen;
    }

    /*
     * If we need a nulls bitmap and don't already have one, create it ...
     */
    if newhasnulls && dnulls.is_null() {
        dnulls = MemoryContextAllocZero(
            (*eah).hdr.eoh_context as crate::utils::palloc::MemoryContext,
            (*eah).dvalueslen as usize * core::mem::size_of::<bool>(),
        ) as *mut bool;
        (*eah).dnulls = dnulls;
    }

    /*
     * We now have all the needed space allocated ...
     */

    /* Flattened value will no longer represent array accurately */
    (*eah).fvalue = null_mut();
    /* And we don't know the flattened size either */
    (*eah).flat_size = 0;

    /* Update dimensionality info if needed */
    if dimschanged {
        (*eah).ndims = ndim;
        memcpy(
            (*eah).dims as *mut c_void,
            dim.as_ptr() as *const c_void,
            ndim as usize * core::mem::size_of::<c_int>(),
        );
        memcpy(
            (*eah).lbound as *mut c_void,
            lb.as_ptr() as *const c_void,
            ndim as usize * core::mem::size_of::<c_int>(),
        );
    }

    /* Reposition items if needed, and fill addedbefore items with nulls */
    if addedbefore > 0 {
        memmove(
            dvalues.add(addedbefore as usize) as *mut c_void,
            dvalues as *const c_void,
            (*eah).nelems as usize * core::mem::size_of::<Datum>(),
        );
        i = 0;
        while i < addedbefore {
            *dvalues.add(i as usize) = 0 as Datum;
            i += 1;
        }
        if !dnulls.is_null() {
            memmove(
                dnulls.add(addedbefore as usize) as *mut c_void,
                dnulls as *const c_void,
                (*eah).nelems as usize * core::mem::size_of::<bool>(),
            );
            i = 0;
            while i < addedbefore {
                *dnulls.add(i as usize) = true;
                i += 1;
            }
        }
        (*eah).nelems += addedbefore;
    }

    /* fill addedafter items with nulls */
    if addedafter > 0 {
        i = 0;
        while i < addedafter {
            *dvalues.add(((*eah).nelems + i) as usize) = 0 as Datum;
            i += 1;
        }
        if !dnulls.is_null() {
            i = 0;
            while i < addedafter {
                *dnulls.add(((*eah).nelems + i) as usize) = true;
                i += 1;
            }
        }
        (*eah).nelems += addedafter;
    }

    /* Grab old element value for pfree'ing, if needed. */
    if !(*eah).typbyval && (dnulls.is_null() || !*dnulls.add(offset as usize)) {
        oldValue = DatumGetPointer(*dvalues.add(offset as usize)) as *mut c_char;
    } else {
        oldValue = null_mut();
    }

    /* And finally we can insert the new element. */
    *dvalues.add(offset as usize) = dataValue;
    if !dnulls.is_null() {
        *dnulls.add(offset as usize) = isNull;
    }

    /*
     * Free old element if needed ...
     */
    if !oldValue.is_null() {
        /* Don't try to pfree a part of the original flat array */
        if oldValue < (*eah).fstartptr || oldValue >= (*eah).fendptr {
            pfree(oldValue as *mut c_void);
        }
    }

    /* Done, return standard TOAST pointer for object */
    EOHPGetRWDatum(&raw const (*eah).hdr)
}

/*
 * array_set_slice :
 *		  This routine sets the value of a range of array locations (specified
 *		  by upper and lower subscript values) to new values passed as
 *		  another array.  [see arrayfuncs.c]
 */
pub unsafe fn array_set_slice(
    arraydatum: Datum,
    nSubscripts: c_int,
    upperIndx: *mut c_int,
    lowerIndx: *mut c_int,
    upperProvided: *mut bool,
    lowerProvided: *mut bool,
    srcArrayDatum: Datum,
    isNull: bool,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
) -> Datum {
    let array: *mut ArrayType;
    let srcArray: *mut ArrayType;
    let newarray: *mut ArrayType;
    let mut i: c_int;
    let ndim: c_int;
    let mut dim: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut lb: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut span: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut newhasnulls: bool;
    let nitems: c_int;
    let nsrcitems: c_int;
    let olddatasize: c_int;
    let newsize: c_int;
    let mut olditemsize: c_int = 0;
    let newitemsize: c_int;
    let overheadlen: c_int;
    let oldoverheadlen: c_int;
    let mut addedbefore: c_int;
    let mut addedafter: c_int;
    let mut lenbefore: c_int = 0;
    let mut lenafter: c_int = 0;
    let mut itemsbefore: c_int = 0;
    let mut itemsafter: c_int = 0;
    let mut nolditems: c_int = 0;

    /* Currently, assignment from a NULL source array is a no-op */
    if isNull {
        return arraydatum;
    }

    if arraytyplen > 0 {
        /*
         * fixed-length arrays -- not got round to doing this...
         */
        ereport!(
            ERROR,
            errmsg!("updates on slices of fixed-length arrays not implemented")
        );
    }

    /* detoast arrays if necessary */
    array = DatumGetArrayTypeP(arraydatum);
    srcArray = DatumGetArrayTypeP(srcArrayDatum);

    /* note: we assume srcArray contains no toasted elements */

    ndim = ARR_NDIM(array);

    /*
     * if number of dims is zero ... create an array with nSubscripts dimensions
     */
    if ndim == 0 {
        let mut dvalues: *mut Datum = null_mut();
        let mut dnulls: *mut bool = null_mut();
        let mut nelems: c_int = 0;
        let elmtype = ARR_ELEMTYPE(array);

        deconstruct_array(
            srcArray,
            elmtype,
            elmlen,
            elmbyval,
            elmalign,
            &raw mut dvalues,
            &raw mut dnulls,
            &raw mut nelems,
        );

        for i in 0..nSubscripts as usize {
            if !*upperProvided.add(i) || !*lowerProvided.add(i) {
                ereport!(
                    ERROR,
                    errmsg!("array slice subscript must provide both boundaries")
                );
            }

            /* compute "upperIndx[i] - lowerIndx[i] + 1", detecting overflow */
            if pg_sub_s32_overflow(*upperIndx.add(i), *lowerIndx.add(i), &raw mut dim[i])
                || pg_add_s32_overflow(dim[i], 1, &raw mut dim[i])
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "array size exceeds the maximum allowed ({})",
                        MaxArraySize as c_int
                    )
                );
            }

            lb[i] = *lowerIndx.add(i);
        }

        /* complain if too few source items; we ignore extras, however */
        if nelems < ArrayGetNItems(nSubscripts, dim.as_ptr()) {
            ereport!(ERROR, errmsg!("source array too small"));
        }

        return PointerGetDatum(construct_md_array(
            dvalues,
            dnulls,
            nSubscripts,
            dim.as_mut_ptr(),
            lb.as_mut_ptr(),
            elmtype,
            elmlen,
            elmbyval,
            elmalign,
        ) as *const c_void);
    }

    if ndim < nSubscripts || ndim <= 0 || ndim > MAXDIM {
        ereport!(ERROR, errmsg!("wrong number of array subscripts"));
    }

    /* copy dim/lb since we may modify them */
    memcpy(
        dim.as_mut_ptr() as *mut c_void,
        ARR_DIMS(array) as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        lb.as_mut_ptr() as *mut c_void,
        ARR_LBOUND(array) as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );

    newhasnulls = ARR_HASNULL(array) || ARR_HASNULL(srcArray);
    addedbefore = 0;
    addedafter = 0;

    /*
     * Check subscripts. ...
     */
    if ndim == 1 {
        Assert!(nSubscripts == 1);
        if !*lowerProvided.add(0) {
            *lowerIndx.add(0) = lb[0];
        }
        if !*upperProvided.add(0) {
            *upperIndx.add(0) = dim[0] + lb[0] - 1;
        }
        if *lowerIndx.add(0) > *upperIndx.add(0) {
            ereport!(ERROR, errmsg!("upper bound cannot be less than lower bound"));
        }
        if *lowerIndx.add(0) < lb[0] {
            if pg_sub_s32_overflow(lb[0], *lowerIndx.add(0), &raw mut addedbefore)
                || pg_add_s32_overflow(dim[0], addedbefore, &raw mut dim[0])
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "array size exceeds the maximum allowed ({})",
                        MaxArraySize as c_int
                    )
                );
            }
            lb[0] = *lowerIndx.add(0);
            if addedbefore > 1 {
                newhasnulls = true; /* will insert nulls */
            }
        }
        if *upperIndx.add(0) >= (dim[0] + lb[0]) {
            if pg_sub_s32_overflow(*upperIndx.add(0), dim[0] + lb[0], &raw mut addedafter)
                || pg_add_s32_overflow(addedafter, 1, &raw mut addedafter)
                || pg_add_s32_overflow(dim[0], addedafter, &raw mut dim[0])
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "array size exceeds the maximum allowed ({})",
                        MaxArraySize as c_int
                    )
                );
            }
            if addedafter > 1 {
                newhasnulls = true; /* will insert nulls */
            }
        }
    } else {
        /*
         * XXX currently we do not support extending multi-dimensional arrays
         * during assignment
         */
        i = 0;
        while i < nSubscripts {
            let iu = i as usize;
            if !*lowerProvided.add(iu) {
                *lowerIndx.add(iu) = lb[iu];
            }
            if !*upperProvided.add(iu) {
                *upperIndx.add(iu) = dim[iu] + lb[iu] - 1;
            }
            if *lowerIndx.add(iu) > *upperIndx.add(iu) {
                ereport!(ERROR, errmsg!("upper bound cannot be less than lower bound"));
            }
            if *lowerIndx.add(iu) < lb[iu] || *upperIndx.add(iu) >= (dim[iu] + lb[iu]) {
                ereport!(ERROR, errmsg!("array subscript out of range"));
            }
            i += 1;
        }
        /* fill any missing subscript positions with full array range */
        while i < ndim {
            let iu = i as usize;
            *lowerIndx.add(iu) = lb[iu];
            *upperIndx.add(iu) = dim[iu] + lb[iu] - 1;
            if *lowerIndx.add(iu) > *upperIndx.add(iu) {
                ereport!(ERROR, errmsg!("upper bound cannot be less than lower bound"));
            }
            i += 1;
        }
    }

    /* Do this mainly to check for overflow */
    nitems = ArrayGetNItems(ndim, dim.as_ptr());
    ArrayCheckBounds(ndim, dim.as_ptr(), lb.as_ptr());

    /*
     * Make sure source array has enough entries. ...
     */
    mda_get_range(ndim, span.as_mut_ptr(), lowerIndx, upperIndx);
    nsrcitems = ArrayGetNItems(ndim, span.as_ptr());
    if nsrcitems > ArrayGetNItems(ARR_NDIM(srcArray), ARR_DIMS(srcArray)) {
        ereport!(ERROR, errmsg!("source array too small"));
    }

    /*
     * Compute space occupied by new entries ...
     */
    if newhasnulls {
        overheadlen = ARR_OVERHEAD_WITHNULLS(ndim, nitems) as c_int;
    } else {
        overheadlen = ARR_OVERHEAD_NONULLS(ndim) as c_int;
    }
    newitemsize = array_nelems_size(
        ARR_DATA_PTR(srcArray),
        0,
        ARR_NULLBITMAP(srcArray),
        nsrcitems,
        elmlen,
        elmbyval,
        elmalign,
    );
    oldoverheadlen = ARR_DATA_OFFSET(array) as c_int;
    olddatasize = ARR_SIZE(array) as c_int - oldoverheadlen;
    if ndim > 1 {
        /*
         * here we do not need to cope with extension of the array ...
         */
        olditemsize = array_slice_size(
            ARR_DATA_PTR(array),
            ARR_NULLBITMAP(array),
            ndim,
            dim.as_mut_ptr(),
            lb.as_mut_ptr(),
            lowerIndx,
            upperIndx,
            elmlen,
            elmbyval,
            elmalign,
        );
        lenbefore = 0;
        lenafter = 0; /* keep compiler quiet */
        itemsbefore = 0;
        itemsafter = 0;
        nolditems = 0;
    } else {
        /*
         * here we must allow for possibility of slice larger than orig array ...
         */
        let oldlb = *ARR_LBOUND(array).add(0);
        let oldub = oldlb + *ARR_DIMS(array).add(0) - 1;
        let slicelb = core::cmp::max(oldlb, *lowerIndx.add(0));
        let sliceub = core::cmp::min(oldub, *upperIndx.add(0));
        let oldarraydata = ARR_DATA_PTR(array);
        let oldarraybitmap = ARR_NULLBITMAP(array);

        /* count/size of old array entries that will go before the slice */
        itemsbefore = core::cmp::min(slicelb, oldub + 1) - oldlb;
        lenbefore = array_nelems_size(
            oldarraydata,
            0,
            oldarraybitmap,
            itemsbefore,
            elmlen,
            elmbyval,
            elmalign,
        );
        /* count/size of old array entries that will be replaced by slice */
        if slicelb > sliceub {
            nolditems = 0;
            olditemsize = 0;
        } else {
            nolditems = sliceub - slicelb + 1;
            olditemsize = array_nelems_size(
                oldarraydata.add(lenbefore as usize),
                itemsbefore,
                oldarraybitmap,
                nolditems,
                elmlen,
                elmbyval,
                elmalign,
            );
        }
        /* count/size of old array entries that will go after the slice */
        itemsafter = oldub + 1 - core::cmp::max(sliceub + 1, oldlb);
        lenafter = olddatasize - lenbefore - olditemsize;
    }

    newsize = overheadlen + olddatasize - olditemsize + newitemsize;

    newarray = palloc0(newsize as usize) as *mut ArrayType;
    SET_VARSIZE(newarray as *mut c_char, newsize);
    (*newarray).ndim = ndim;
    (*newarray).dataoffset = if newhasnulls { overheadlen } else { 0 };
    (*newarray).elemtype = ARR_ELEMTYPE(array);
    memcpy(
        ARR_DIMS(newarray) as *mut c_void,
        dim.as_ptr() as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        ARR_LBOUND(newarray) as *mut c_void,
        lb.as_ptr() as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );

    if ndim > 1 {
        /*
         * here we do not need to cope with extension of the array ...
         */
        array_insert_slice(
            newarray,
            array,
            srcArray,
            ndim,
            dim.as_mut_ptr(),
            lb.as_mut_ptr(),
            lowerIndx,
            upperIndx,
            elmlen,
            elmbyval,
            elmalign,
        );
    } else {
        /* fill in data */
        memcpy(
            (newarray as *mut c_char).add(overheadlen as usize) as *mut c_void,
            (array as *mut c_char).add(oldoverheadlen as usize) as *const c_void,
            lenbefore as usize,
        );
        memcpy(
            (newarray as *mut c_char).add((overheadlen + lenbefore) as usize) as *mut c_void,
            ARR_DATA_PTR(srcArray) as *const c_void,
            newitemsize as usize,
        );
        memcpy(
            (newarray as *mut c_char).add((overheadlen + lenbefore + newitemsize) as usize)
                as *mut c_void,
            (array as *mut c_char).add((oldoverheadlen + lenbefore + olditemsize) as usize)
                as *const c_void,
            lenafter as usize,
        );
        /* fill in nulls bitmap if needed */
        if newhasnulls {
            let newnullbitmap = ARR_NULLBITMAP(newarray);
            let oldnullbitmap = ARR_NULLBITMAP(array);

            /* palloc0 above already marked any inserted positions as nulls */
            array_bitmap_copy(newnullbitmap, addedbefore, oldnullbitmap, 0, itemsbefore);
            array_bitmap_copy(
                newnullbitmap,
                *lowerIndx.add(0) - lb[0],
                ARR_NULLBITMAP(srcArray),
                0,
                nsrcitems,
            );
            array_bitmap_copy(
                newnullbitmap,
                addedbefore + itemsbefore + nolditems,
                oldnullbitmap,
                itemsbefore + nolditems,
                itemsafter,
            );
        }
    }

    PointerGetDatum(newarray as *const c_void)
}

/*
 * array_ref : backwards compatibility wrapper for array_get_element
 */
pub unsafe fn array_ref(
    array: *mut ArrayType,
    nSubscripts: c_int,
    indx: *mut c_int,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
    isNull: *mut bool,
) -> Datum {
    array_get_element(
        PointerGetDatum(array as *const c_void),
        nSubscripts,
        indx,
        arraytyplen,
        elmlen,
        elmbyval,
        elmalign,
        isNull,
    )
}

/*
 * array_set : backwards compatibility wrapper for array_set_element
 */
pub unsafe fn array_set(
    array: *mut ArrayType,
    nSubscripts: c_int,
    indx: *mut c_int,
    dataValue: Datum,
    isNull: bool,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
) -> *mut ArrayType {
    DatumGetArrayTypeP(array_set_element(
        PointerGetDatum(array as *const c_void),
        nSubscripts,
        indx,
        dataValue,
        isNull,
        arraytyplen,
        elmlen,
        elmbyval,
        elmalign,
    ))
}

/*
 * array_map()
 *
 * Map an array through an arbitrary expression.  [see arrayfuncs.c]
 */
pub unsafe fn array_map(
    arrayd: Datum,
    exprstate: *mut ExprState,
    econtext: *mut ExprContext,
    retType: Oid,
    amstate: *mut ArrayMapState,
) -> Datum {
    let v = DatumGetAnyArrayP(arrayd);
    let result: *mut ArrayType;
    let values: *mut Datum;
    let nulls: *mut bool;
    let dim: *mut c_int;
    let ndim: c_int;
    let nitems: c_int;
    let mut i: c_int;
    let mut nbytes: int32 = 0;
    let dataoffset: int32;
    let mut hasnulls: bool;
    let inpType: Oid;
    let inp_typlen: c_int;
    let inp_typbyval: bool;
    let inp_typalign: c_char;
    let typlen: c_int;
    let typbyval: bool;
    let typalign: c_char;
    let mut iter = core::mem::MaybeUninit::<array_iter>::uninit();
    let inp_extra: *mut ArrayMetaState;
    let ret_extra: *mut ArrayMetaState;
    let transform_source: *mut Datum = (*exprstate).innermost_caseval;
    let transform_source_isnull: *mut bool = (*exprstate).innermost_casenull;

    inpType = AARR_ELEMTYPE(v);
    ndim = AARR_NDIM(v);
    dim = AARR_DIMS(v);
    nitems = ArrayGetNItems(ndim, dim);

    /* Check for empty array */
    if nitems <= 0 {
        /* Return empty array */
        return PointerGetDatum(construct_empty_array(retType) as *const c_void);
    }

    /*
     * We arrange to look up info about input and return element types only
     * once per series of calls ...
     */
    inp_extra = &raw mut (*amstate).inp_extra;
    ret_extra = &raw mut (*amstate).ret_extra;

    if (*inp_extra).element_type != inpType {
        get_typlenbyvalalign(
            inpType,
            &raw mut (*inp_extra).typlen,
            &raw mut (*inp_extra).typbyval,
            &raw mut (*inp_extra).typalign,
        );
        (*inp_extra).element_type = inpType;
    }
    inp_typlen = (*inp_extra).typlen as c_int;
    inp_typbyval = (*inp_extra).typbyval;
    inp_typalign = (*inp_extra).typalign;

    if (*ret_extra).element_type != retType {
        get_typlenbyvalalign(
            retType,
            &raw mut (*ret_extra).typlen,
            &raw mut (*ret_extra).typbyval,
            &raw mut (*ret_extra).typalign,
        );
        (*ret_extra).element_type = retType;
    }
    typlen = (*ret_extra).typlen as c_int;
    typbyval = (*ret_extra).typbyval;
    typalign = (*ret_extra).typalign;

    /* Allocate temporary arrays for new values */
    values = palloc(nitems as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls = palloc(nitems as usize * core::mem::size_of::<bool>()) as *mut bool;

    /* Loop over source data */
    array_iter_setup(iter.as_mut_ptr(), v);
    let iter = iter.as_mut_ptr();
    hasnulls = false;

    i = 0;
    while i < nitems {
        /* Get source element, checking for NULL */
        *transform_source = array_iter_next(
            iter,
            transform_source_isnull,
            i,
            inp_typlen,
            inp_typbyval,
            inp_typalign,
        );

        /* Apply the given expression to source element */
        *values.add(i as usize) = ExecEvalExpr(exprstate, econtext, nulls.add(i as usize));

        if *nulls.add(i as usize) {
            hasnulls = true;
        } else {
            /* Ensure data is not toasted */
            if typlen == -1 {
                *values.add(i as usize) = PointerGetDatum(
                    crate::PG_DETOAST_DATUM!(*values.add(i as usize)) as *const c_void,
                );
            }
            /* Update total result size */
            nbytes = att_addlength_datum(nbytes as usize, typlen, *values.add(i as usize)) as int32;
            nbytes = att_align_nominal(nbytes as usize, typalign) as int32;
            /* check for overflow of total request */
            if !AllocSizeIsValid(nbytes as Size) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "array size exceeds the maximum allowed ({})",
                        MaxAllocSize as c_int
                    )
                );
            }
        }
        i += 1;
    }

    /* Allocate and fill the result array */
    if hasnulls {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndim, nitems) as int32;
        nbytes += dataoffset;
    } else {
        dataoffset = 0; /* marker for no null bitmap */
        nbytes += ARR_OVERHEAD_NONULLS(ndim) as int32;
    }
    result = palloc0(nbytes as usize) as *mut ArrayType;
    SET_VARSIZE(result as *mut c_char, nbytes);
    (*result).ndim = ndim;
    (*result).dataoffset = dataoffset;
    (*result).elemtype = retType;
    memcpy(
        ARR_DIMS(result) as *mut c_void,
        AARR_DIMS(v) as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        ARR_LBOUND(result) as *mut c_void,
        AARR_LBOUND(v) as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );

    CopyArrayEls(
        result, values, nulls, nitems, typlen, typbyval, typalign, false,
    );

    /*
     * Note: do not risk trying to pfree the results of the called expression
     */
    pfree(values as *mut c_void);
    pfree(nulls as *mut c_void);

    PointerGetDatum(result as *const c_void)
}

/*
 * construct_array	--- simple method for constructing an array object
 */
pub unsafe fn construct_array(
    elems: *mut Datum,
    nelems: c_int,
    elmtype: Oid,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
) -> *mut ArrayType {
    let mut dims: [c_int; 1] = [0; 1];
    let mut lbs: [c_int; 1] = [0; 1];

    dims[0] = nelems;
    lbs[0] = 1;

    construct_md_array(
        elems,
        null_mut(),
        1,
        dims.as_mut_ptr(),
        lbs.as_mut_ptr(),
        elmtype,
        elmlen,
        elmbyval,
        elmalign,
    )
}

/*
 * Like construct_array(), where elmtype must be a built-in type, and
 * elmlen/elmbyval/elmalign is looked up from hardcoded data.
 */
#[no_mangle]
pub unsafe fn construct_array_builtin(elems: *mut Datum, nelems: c_int, elmtype: Oid) -> *mut ArrayType {
    use crate::catalog::pg_type::{TYPALIGN_CHAR, TYPALIGN_DOUBLE, TYPALIGN_INT, TYPALIGN_SHORT};
    let elmlen: c_int;
    let elmbyval: bool;
    let elmalign: c_char;

    match elmtype {
        CHAROID => {
            elmlen = 1;
            elmbyval = true;
            elmalign = TYPALIGN_CHAR;
        }
        CSTRINGOID => {
            elmlen = -2;
            elmbyval = false;
            elmalign = TYPALIGN_CHAR;
        }
        FLOAT4OID => {
            elmlen = core::mem::size_of::<crate::c::float4>() as c_int;
            elmbyval = true;
            elmalign = TYPALIGN_INT;
        }
        FLOAT8OID => {
            elmlen = core::mem::size_of::<float8>() as c_int;
            elmbyval = FLOAT8PASSBYVAL;
            elmalign = TYPALIGN_DOUBLE;
        }
        INT2OID => {
            elmlen = core::mem::size_of::<int16>() as c_int;
            elmbyval = true;
            elmalign = TYPALIGN_SHORT;
        }
        INT4OID => {
            elmlen = core::mem::size_of::<int32>() as c_int;
            elmbyval = true;
            elmalign = TYPALIGN_INT;
        }
        INT8OID => {
            elmlen = core::mem::size_of::<crate::c::int64>() as c_int;
            elmbyval = FLOAT8PASSBYVAL;
            elmalign = TYPALIGN_DOUBLE;
        }
        NAMEOID => {
            elmlen = NAMEDATALEN as c_int;
            elmbyval = false;
            elmalign = TYPALIGN_CHAR;
        }
        OIDOID | REGTYPEOID => {
            elmlen = core::mem::size_of::<Oid>() as c_int;
            elmbyval = true;
            elmalign = TYPALIGN_INT;
        }
        TEXTOID => {
            elmlen = -1;
            elmbyval = false;
            elmalign = TYPALIGN_INT;
        }
        TIDOID => {
            elmlen = core::mem::size_of::<ItemPointerData>() as c_int;
            elmbyval = false;
            elmalign = TYPALIGN_SHORT;
        }
        XIDOID => {
            elmlen = core::mem::size_of::<TransactionId>() as c_int;
            elmbyval = true;
            elmalign = TYPALIGN_INT;
        }
        _ => {
            elog!(
                ERROR,
                "type {} not supported by construct_array_builtin()",
                elmtype
            );
            /* keep compiler quiet */
            elmlen = 0;
            elmbyval = false;
            elmalign = 0;
        }
    }

    construct_array(elems, nelems, elmtype, elmlen, elmbyval, elmalign)
}

/*
 * construct_md_array	--- simple method for constructing an array object
 *							with arbitrary dimensions and possible NULLs
 */
#[no_mangle]
pub unsafe fn construct_md_array(
    elems: *mut Datum,
    nulls: *mut bool,
    ndims: c_int,
    dims: *mut c_int,
    lbs: *mut c_int,
    elmtype: Oid,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
) -> *mut ArrayType {
    let result: *mut ArrayType;
    let mut hasnulls: bool;
    let mut nbytes: int32;
    let dataoffset: int32;
    let nelems: c_int;

    if ndims < 0 {
        /* we do allow zero-dimension arrays */
        ereport!(ERROR, errmsg!("invalid number of dimensions: {}", ndims));
    }
    if ndims > MAXDIM {
        ereport!(
            ERROR,
            errmsg!(
                "number of array dimensions ({}) exceeds the maximum allowed ({})",
                ndims,
                MAXDIM
            )
        );
    }

    /* This checks for overflow of the array dimensions */
    nelems = ArrayGetNItems(ndims, dims);
    ArrayCheckBounds(ndims, dims, lbs);

    /* if ndims <= 0 or any dims[i] == 0, return empty array */
    if nelems <= 0 {
        return construct_empty_array(elmtype);
    }

    /* compute required space */
    nbytes = 0;
    hasnulls = false;
    for i in 0..nelems as usize {
        if !nulls.is_null() && *nulls.add(i) {
            hasnulls = true;
            continue;
        }
        /* make sure data is not toasted */
        if elmlen == -1 {
            *elems.add(i) =
                PointerGetDatum(crate::PG_DETOAST_DATUM!(*elems.add(i)) as *const c_void);
        }
        nbytes = att_addlength_datum(nbytes as usize, elmlen, *elems.add(i)) as int32;
        nbytes = att_align_nominal(nbytes as usize, elmalign) as int32;
        /* check for overflow of total request */
        if !AllocSizeIsValid(nbytes as Size) {
            ereport!(
                ERROR,
                errmsg!(
                    "array size exceeds the maximum allowed ({})",
                    MaxAllocSize as c_int
                )
            );
        }
    }

    /* Allocate and initialize result array */
    if hasnulls {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nelems) as int32;
        nbytes += dataoffset;
    } else {
        dataoffset = 0; /* marker for no null bitmap */
        nbytes += ARR_OVERHEAD_NONULLS(ndims) as int32;
    }
    result = palloc0(nbytes as usize) as *mut ArrayType;
    SET_VARSIZE(result as *mut c_char, nbytes);
    (*result).ndim = ndims;
    (*result).dataoffset = dataoffset;
    (*result).elemtype = elmtype;
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

    CopyArrayEls(
        result, elems, nulls, nelems, elmlen, elmbyval, elmalign, false,
    );

    result
}

/*
 * construct_empty_array	--- make a zero-dimensional array of given type
 */
pub unsafe fn construct_empty_array(elmtype: Oid) -> *mut ArrayType {
    let result: *mut ArrayType;

    result = palloc0(core::mem::size_of::<ArrayType>()) as *mut ArrayType;
    SET_VARSIZE(result as *mut c_char, core::mem::size_of::<ArrayType>() as int32);
    (*result).ndim = 0;
    (*result).dataoffset = 0;
    (*result).elemtype = elmtype;
    result
}

/*
 * construct_empty_expanded_array: make an empty expanded array
 * given only type information.  (metacache can be NULL if not needed.)
 */
pub unsafe fn construct_empty_expanded_array(
    element_type: Oid,
    parentcontext: MemoryContext,
    metacache: *mut ArrayMetaState,
) -> *mut ExpandedArrayHeader {
    let array = construct_empty_array(element_type);
    let d: Datum;

    d = expand_array(
        PointerGetDatum(array as *const c_void),
        parentcontext,
        metacache,
    );
    pfree(array as *mut c_void);
    DatumGetEOHP(d) as *mut ExpandedArrayHeader
}

/*
 * deconstruct_array  --- simple method for extracting data from an array
 *
 * [see arrayfuncs.c]
 */
pub unsafe fn deconstruct_array(
    array: *mut ArrayType,
    elmtype: Oid,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
    elemsp: *mut *mut Datum,
    nullsp: *mut *mut bool,
    nelemsp: *mut c_int,
) {
    let elems: *mut Datum;
    let nulls: *mut bool;
    let nelems: c_int;
    let mut p: *mut c_char;
    let mut bitmap: *mut bits8;
    let mut bitmask: c_int;

    Assert!(ARR_ELEMTYPE(array) == elmtype);

    nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    elems = palloc(nelems as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    *elemsp = elems;
    if !nullsp.is_null() {
        nulls = palloc0(nelems as usize * core::mem::size_of::<bool>()) as *mut bool;
        *nullsp = nulls;
    } else {
        nulls = null_mut();
    }
    *nelemsp = nelems;

    p = ARR_DATA_PTR(array);
    bitmap = ARR_NULLBITMAP(array);
    bitmask = 1;

    for i in 0..nelems as usize {
        /* Get source element, checking for NULL */
        if !bitmap.is_null() && (*bitmap & bitmask as bits8) == 0 {
            *elems.add(i) = 0 as Datum;
            if !nulls.is_null() {
                *nulls.add(i) = true;
            } else {
                ereport!(
                    ERROR,
                    errmsg!("null array element not allowed in this context")
                );
            }
        } else {
            *elems.add(i) = fetch_att(p as *const c_void, elmbyval, elmlen);
            p = att_addlength_pointer(p as usize, elmlen, p) as *mut c_char;
            p = att_align_nominal(p as usize, elmalign) as *mut c_char;
        }

        /* advance bitmap pointer if any */
        if !bitmap.is_null() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                bitmap = bitmap.add(1);
                bitmask = 1;
            }
        }
    }
}

/*
 * Like deconstruct_array(), where elmtype must be a built-in type, and
 * elmlen/elmbyval/elmalign is looked up from hardcoded data.
 */
#[no_mangle]
pub unsafe fn deconstruct_array_builtin(
    array: *mut ArrayType,
    elmtype: Oid,
    elemsp: *mut *mut Datum,
    nullsp: *mut *mut bool,
    nelemsp: *mut c_int,
) {
    use crate::catalog::pg_type::{TYPALIGN_CHAR, TYPALIGN_DOUBLE, TYPALIGN_INT, TYPALIGN_SHORT};
    let elmlen: c_int;
    let elmbyval: bool;
    let elmalign: c_char;

    match elmtype {
        CHAROID => {
            elmlen = 1;
            elmbyval = true;
            elmalign = TYPALIGN_CHAR;
        }
        CSTRINGOID => {
            elmlen = -2;
            elmbyval = false;
            elmalign = TYPALIGN_CHAR;
        }
        FLOAT8OID => {
            elmlen = core::mem::size_of::<float8>() as c_int;
            elmbyval = FLOAT8PASSBYVAL;
            elmalign = TYPALIGN_DOUBLE;
        }
        INT2OID => {
            elmlen = core::mem::size_of::<int16>() as c_int;
            elmbyval = true;
            elmalign = TYPALIGN_SHORT;
        }
        INT4OID => {
            elmlen = core::mem::size_of::<int32>() as c_int;
            elmbyval = true;
            elmalign = TYPALIGN_INT;
        }
        OIDOID => {
            elmlen = core::mem::size_of::<Oid>() as c_int;
            elmbyval = true;
            elmalign = TYPALIGN_INT;
        }
        TEXTOID => {
            elmlen = -1;
            elmbyval = false;
            elmalign = TYPALIGN_INT;
        }
        TIDOID => {
            elmlen = core::mem::size_of::<ItemPointerData>() as c_int;
            elmbyval = false;
            elmalign = TYPALIGN_SHORT;
        }
        _ => {
            elog!(
                ERROR,
                "type {} not supported by deconstruct_array_builtin()",
                elmtype
            );
            /* keep compiler quiet */
            elmlen = 0;
            elmbyval = false;
            elmalign = 0;
        }
    }

    deconstruct_array(
        array, elmtype, elmlen, elmbyval, elmalign, elemsp, nullsp, nelemsp,
    );
}

/*
 * array_contains_nulls --- detect whether an array has any null elements
 *
 * This gives an accurate answer, whereas testing ARR_HASNULL only tells
 * if the array *might* contain a null.
 */
pub unsafe fn array_contains_nulls(array: *mut ArrayType) -> bool {
    let mut nelems: c_int;
    let mut bitmap: *mut bits8;
    let mut bitmask: c_int;

    /* Easy answer if there's no null bitmap */
    if !ARR_HASNULL(array) {
        return false;
    }

    nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

    bitmap = ARR_NULLBITMAP(array);

    /* check whole bytes of the bitmap byte-at-a-time */
    while nelems >= 8 {
        if *bitmap != 0xFF {
            return true;
        }
        bitmap = bitmap.add(1);
        nelems -= 8;
    }

    /* check last partial byte */
    bitmask = 1;
    while nelems > 0 {
        if (*bitmap & bitmask as bits8) == 0 {
            return true;
        }
        bitmask <<= 1;
        nelems -= 1;
    }

    false
}

/*
 * array_eq :
 *		  compares two arrays for equality
 * result :
 *		  returns true if the arrays are equal, false otherwise.
 *
 * Note: we do not use array_cmp here, since equality may be meaningful in
 * datatypes that don't have a total ordering (and hence no btree support).
 */
pub unsafe fn array_eq(fcinfo: FunctionCallInfo) -> Datum {
    LOCAL_FCINFO!(locfcinfo, 2);
    let array1 = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let array2 = PG_GETARG_ANY_ARRAY_P!(fcinfo, 1);
    let collation = PG_GET_COLLATION!(fcinfo);
    let ndims1 = AARR_NDIM(array1);
    let ndims2 = AARR_NDIM(array2);
    let dims1 = AARR_DIMS(array1);
    let dims2 = AARR_DIMS(array2);
    let lbs1 = AARR_LBOUND(array1);
    let lbs2 = AARR_LBOUND(array2);
    let element_type = AARR_ELEMTYPE(array1);
    let mut result = true;
    let nitems: c_int;
    let typentry: *mut TypeCacheEntry;
    let typlen: c_int;
    let typbyval: bool;
    let typalign: c_char;
    let mut it1: array_iter = core::mem::zeroed();
    let mut it2: array_iter = core::mem::zeroed();
    let mut i: c_int;

    if element_type != AARR_ELEMTYPE(array2) {
        ereport!(
            ERROR,
            errmsg!("cannot compare arrays of different element types")
        );
    }

    /* fast path if the arrays do not have the same dimensionality */
    if ndims1 != ndims2
        || memcmp(
            dims1 as *const c_void,
            dims2 as *const c_void,
            (ndims1 as usize) * core::mem::size_of::<c_int>(),
        ) != 0
        || memcmp(
            lbs1 as *const c_void,
            lbs2 as *const c_void,
            (ndims1 as usize) * core::mem::size_of::<c_int>(),
        ) != 0
    {
        result = false;
    } else {
        /*
         * We arrange to look up the equality function only once per series of
         * calls, assuming the element type doesn't change underneath us.  The
         * typcache is used so that we have no memory leakage when being used
         * as an index support function.
         */
        let mut typentry_l = (*(*fcinfo).flinfo).fn_extra as *mut TypeCacheEntry;
        if typentry_l.is_null() || (*typentry_l).type_id != element_type {
            typentry_l = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);
            if !OidIsValid((*typentry_l).eq_opr_finfo.fn_oid) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not identify an equality operator for type {}",
                        std::ffi::CStr::from_ptr(format_type_be(element_type)).to_string_lossy()
                    )
                );
            }
            (*(*fcinfo).flinfo).fn_extra = typentry_l as *mut c_void;
        }
        typentry = typentry_l;
        typlen = (*typentry).typlen as c_int;
        typbyval = (*typentry).typbyval;
        typalign = (*typentry).typalign;

        /*
         * apply the operator to each pair of array elements.
         */
        InitFunctionCallInfoData!(
            locfcinfo,
            &raw mut (*typentry).eq_opr_finfo,
            2,
            collation,
            null_mut(),
            null_mut()
        );

        /* Loop over source data */
        nitems = ArrayGetNItems(ndims1, dims1);
        array_iter_setup(&raw mut it1, array1);
        array_iter_setup(&raw mut it2, array2);

        i = 0;
        while i < nitems {
            let elt1: Datum;
            let elt2: Datum;
            let mut isnull1: bool = false;
            let mut isnull2: bool = false;
            let oprresult: bool;

            /* Get elements, checking for NULL */
            elt1 = array_iter_next(&raw mut it1, &raw mut isnull1, i, typlen, typbyval, typalign);
            elt2 = array_iter_next(&raw mut it2, &raw mut isnull2, i, typlen, typbyval, typalign);

            /*
             * We consider two NULLs equal; NULL and not-NULL are unequal.
             */
            if isnull1 && isnull2 {
                i += 1;
                continue;
            }
            if isnull1 || isnull2 {
                result = false;
                break;
            }

            /*
             * Apply the operator to the element pair; treat NULL as false
             */
            (*(*locfcinfo).args.as_mut_ptr().add(0)).value = elt1;
            (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
            (*(*locfcinfo).args.as_mut_ptr().add(1)).value = elt2;
            (*(*locfcinfo).args.as_mut_ptr().add(1)).isnull = false;
            (*locfcinfo).isnull = false;
            oprresult = DatumGetBool(FunctionCallInvoke!(locfcinfo));
            if (*locfcinfo).isnull || !oprresult {
                result = false;
                break;
            }
            i += 1;
        }
    }

    /* Avoid leaking memory when handed toasted input. */
    AARR_FREE_IF_COPY!(fcinfo, array1, 0);
    AARR_FREE_IF_COPY!(fcinfo, array2, 1);

    PG_RETURN_BOOL!(result)
}

/*-----------------------------------------------------------------------------
 * array-array bool operators:
 *		Given two arrays, iterate comparison operators
 *		over the array. Uses logic similar to text comparison
 *		functions, except element-by-element instead of
 *		character-by-character.
 *----------------------------------------------------------------------------
 */

pub unsafe fn array_ne(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(!DatumGetBool(array_eq(fcinfo)))
}

pub unsafe fn array_lt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(array_cmp(fcinfo) < 0)
}

pub unsafe fn array_gt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(array_cmp(fcinfo) > 0)
}

pub unsafe fn array_le(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(array_cmp(fcinfo) <= 0)
}

pub unsafe fn array_ge(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(array_cmp(fcinfo) >= 0)
}

pub unsafe fn btarraycmp(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_INT32!(array_cmp(fcinfo))
}

/*
 * array_cmp()
 * Internal comparison function for arrays.
 *
 * Returns -1, 0 or 1
 */
unsafe fn array_cmp(fcinfo: FunctionCallInfo) -> c_int {
    LOCAL_FCINFO!(locfcinfo, 2);
    let array1 = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let array2 = PG_GETARG_ANY_ARRAY_P!(fcinfo, 1);
    let collation = PG_GET_COLLATION!(fcinfo);
    let ndims1 = AARR_NDIM(array1);
    let ndims2 = AARR_NDIM(array2);
    let dims1 = AARR_DIMS(array1);
    let dims2 = AARR_DIMS(array2);
    let nitems1 = ArrayGetNItems(ndims1, dims1);
    let nitems2 = ArrayGetNItems(ndims2, dims2);
    let element_type = AARR_ELEMTYPE(array1);
    let mut result: c_int = 0;
    let typentry: *mut TypeCacheEntry;
    let typlen: c_int;
    let typbyval: bool;
    let typalign: c_char;
    let min_nitems: c_int;
    let mut it1: array_iter = core::mem::zeroed();
    let mut it2: array_iter = core::mem::zeroed();
    let mut i: c_int;

    if element_type != AARR_ELEMTYPE(array2) {
        ereport!(
            ERROR,
            errmsg!("cannot compare arrays of different element types")
        );
    }

    /*
     * We arrange to look up the comparison function only once per series of
     * calls, assuming the element type doesn't change underneath us. The
     * typcache is used so that we have no memory leakage when being used as
     * an index support function.
     */
    let mut typentry_l = (*(*fcinfo).flinfo).fn_extra as *mut TypeCacheEntry;
    if typentry_l.is_null() || (*typentry_l).type_id != element_type {
        typentry_l = lookup_type_cache(element_type, TYPECACHE_CMP_PROC_FINFO);
        if !OidIsValid((*typentry_l).cmp_proc_finfo.fn_oid) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not identify a comparison function for type {}",
                    std::ffi::CStr::from_ptr(format_type_be(element_type)).to_string_lossy()
                )
            );
        }
        (*(*fcinfo).flinfo).fn_extra = typentry_l as *mut c_void;
    }
    typentry = typentry_l;
    typlen = (*typentry).typlen as c_int;
    typbyval = (*typentry).typbyval;
    typalign = (*typentry).typalign;

    /*
     * apply the operator to each pair of array elements.
     */
    InitFunctionCallInfoData!(
        locfcinfo,
        &raw mut (*typentry).cmp_proc_finfo,
        2,
        collation,
        null_mut(),
        null_mut()
    );

    /* Loop over source data */
    min_nitems = Min(nitems1, nitems2);
    array_iter_setup(&raw mut it1, array1);
    array_iter_setup(&raw mut it2, array2);

    i = 0;
    while i < min_nitems {
        let elt1: Datum;
        let elt2: Datum;
        let mut isnull1: bool = false;
        let mut isnull2: bool = false;
        let cmpresult: int32;

        /* Get elements, checking for NULL */
        elt1 = array_iter_next(&raw mut it1, &raw mut isnull1, i, typlen, typbyval, typalign);
        elt2 = array_iter_next(&raw mut it2, &raw mut isnull2, i, typlen, typbyval, typalign);

        /*
         * We consider two NULLs equal; NULL > not-NULL.
         */
        if isnull1 && isnull2 {
            i += 1;
            continue;
        }
        if isnull1 {
            /* arg1 is greater than arg2 */
            result = 1;
            break;
        }
        if isnull2 {
            /* arg1 is less than arg2 */
            result = -1;
            break;
        }

        /* Compare the pair of elements */
        (*(*locfcinfo).args.as_mut_ptr().add(0)).value = elt1;
        (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
        (*(*locfcinfo).args.as_mut_ptr().add(1)).value = elt2;
        (*(*locfcinfo).args.as_mut_ptr().add(1)).isnull = false;
        cmpresult = DatumGetInt32(FunctionCallInvoke!(locfcinfo));

        /* We don't expect comparison support functions to return null */
        Assert!(!(*locfcinfo).isnull);

        if cmpresult == 0 {
            i += 1;
            continue; /* equal */
        }

        if cmpresult < 0 {
            /* arg1 is less than arg2 */
            result = -1;
            break;
        } else {
            /* arg1 is greater than arg2 */
            result = 1;
            break;
        }
    }

    /*
     * If arrays contain same data (up to end of shorter one), apply
     * additional rules to sort by dimensionality.  The relative significance
     * of the different bits of information is historical; mainly we just care
     * that we don't say "equal" for arrays of different dimensionality.
     */
    if result == 0 {
        if nitems1 != nitems2 {
            result = if nitems1 < nitems2 { -1 } else { 1 };
        } else if ndims1 != ndims2 {
            result = if ndims1 < ndims2 { -1 } else { 1 };
        } else {
            i = 0;
            while i < ndims1 {
                if *dims1.add(i as usize) != *dims2.add(i as usize) {
                    result = if *dims1.add(i as usize) < *dims2.add(i as usize) {
                        -1
                    } else {
                        1
                    };
                    break;
                }
                i += 1;
            }
            if result == 0 {
                let lbound1 = AARR_LBOUND(array1);
                let lbound2 = AARR_LBOUND(array2);

                i = 0;
                while i < ndims1 {
                    if *lbound1.add(i as usize) != *lbound2.add(i as usize) {
                        result = if *lbound1.add(i as usize) < *lbound2.add(i as usize) {
                            -1
                        } else {
                            1
                        };
                        break;
                    }
                    i += 1;
                }
            }
        }
    }

    /* Avoid leaking memory when handed toasted input. */
    AARR_FREE_IF_COPY!(fcinfo, array1, 0);
    AARR_FREE_IF_COPY!(fcinfo, array2, 1);

    result
}

/*-----------------------------------------------------------------------------
 * array hashing
 *		Hash the elements and combine the results.
 *----------------------------------------------------------------------------
 */

pub unsafe fn hash_array(fcinfo: FunctionCallInfo) -> Datum {
    LOCAL_FCINFO!(locfcinfo, 1);
    let array = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let ndims = AARR_NDIM(array);
    let dims = AARR_DIMS(array);
    let element_type = AARR_ELEMTYPE(array);
    let mut result: uint32 = 1;
    let nitems: c_int;
    let mut typentry: *mut TypeCacheEntry;
    let typlen: c_int;
    let typbyval: bool;
    let typalign: c_char;
    let mut i: c_int;
    let mut iter: array_iter = core::mem::zeroed();

    /*
     * We arrange to look up the hash function only once per series of calls,
     * assuming the element type doesn't change underneath us.  The typcache
     * is used so that we have no memory leakage when being used as an index
     * support function.
     */
    typentry = (*(*fcinfo).flinfo).fn_extra as *mut TypeCacheEntry;
    if typentry.is_null() || (*typentry).type_id != element_type {
        typentry = lookup_type_cache(element_type, TYPECACHE_HASH_PROC_FINFO);
        if !OidIsValid((*typentry).hash_proc_finfo.fn_oid) && element_type != RECORDOID {
            ereport!(
                ERROR,
                errmsg!(
                    "could not identify a hash function for type {}",
                    std::ffi::CStr::from_ptr(format_type_be(element_type)).to_string_lossy()
                )
            );
        }

        /*
         * The type cache doesn't believe that record is hashable (see
         * cache_record_field_properties()), but since we're here, we're
         * committed to hashing, so we can assume it does.  Worst case, if any
         * components of the record don't support hashing, we will fail at
         * execution.
         */
        if element_type == RECORDOID {
            let oldcontext: MemoryContext;
            let record_typentry: *mut TypeCacheEntry;

            oldcontext = MemoryContextSwitchTo((*(*fcinfo).flinfo).fn_mcxt);

            /*
             * Make fake type cache entry structure.  Note that we can't just
             * modify typentry, since that points directly into the type
             * cache.
             */
            record_typentry = palloc0(core::mem::size_of::<TypeCacheEntry>()) as *mut TypeCacheEntry;
            (*record_typentry).type_id = element_type;

            /* fill in what we need below */
            (*record_typentry).typlen = (*typentry).typlen;
            (*record_typentry).typbyval = (*typentry).typbyval;
            (*record_typentry).typalign = (*typentry).typalign;
            fmgr_info(F_HASH_RECORD, &raw mut (*record_typentry).hash_proc_finfo);

            MemoryContextSwitchTo(oldcontext);

            typentry = record_typentry;
        }

        (*(*fcinfo).flinfo).fn_extra = typentry as *mut c_void;
    }

    typlen = (*typentry).typlen as c_int;
    typbyval = (*typentry).typbyval;
    typalign = (*typentry).typalign;

    /*
     * apply the hash function to each array element.
     */
    InitFunctionCallInfoData!(
        locfcinfo,
        &raw mut (*typentry).hash_proc_finfo,
        1,
        PG_GET_COLLATION!(fcinfo),
        null_mut(),
        null_mut()
    );

    /* Loop over source data */
    nitems = ArrayGetNItems(ndims, dims);
    array_iter_setup(&raw mut iter, array);

    i = 0;
    while i < nitems {
        let elt: Datum;
        let mut isnull: bool = false;
        let elthash: uint32;

        /* Get element, checking for NULL */
        elt = array_iter_next(&raw mut iter, &raw mut isnull, i, typlen, typbyval, typalign);

        if isnull {
            /* Treat nulls as having hashvalue 0 */
            elthash = 0;
        } else {
            /* Apply the hash function */
            (*(*locfcinfo).args.as_mut_ptr().add(0)).value = elt;
            (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
            elthash = DatumGetUInt32(FunctionCallInvoke!(locfcinfo));
            /* We don't expect hash functions to return null */
            Assert!(!(*locfcinfo).isnull);
        }

        /*
         * Combine hash values of successive elements by multiplying the
         * current value by 31 and adding on the new element's hash value.
         *
         * The result is a sum in which each element's hash value is
         * multiplied by a different power of 31. This is modulo 2^32
         * arithmetic, and the powers of 31 modulo 2^32 form a cyclic group of
         * order 2^27. So for arrays of up to 2^27 elements, each element's
         * hash value is multiplied by a different (odd) number, resulting in
         * a good mixing of all the elements' hash values.
         */
        result = (result << 5).wrapping_sub(result).wrapping_add(elthash);
        i += 1;
    }

    /* Avoid leaking memory when handed toasted input. */
    AARR_FREE_IF_COPY!(fcinfo, array, 0);

    PG_RETURN_UINT32!(result)
}

/*
 * Returns 64-bit value by hashing a value to a 64-bit value, with a seed.
 * Otherwise, similar to hash_array.
 */
pub unsafe fn hash_array_extended(fcinfo: FunctionCallInfo) -> Datum {
    LOCAL_FCINFO!(locfcinfo, 2);
    let array = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let seed: uint64 = PG_GETARG_INT64!(fcinfo, 1) as uint64;
    let ndims = AARR_NDIM(array);
    let dims = AARR_DIMS(array);
    let element_type = AARR_ELEMTYPE(array);
    let mut result: uint64 = 1;
    let nitems: c_int;
    let mut typentry: *mut TypeCacheEntry;
    let typlen: c_int;
    let typbyval: bool;
    let typalign: c_char;
    let mut i: c_int;
    let mut iter: array_iter = core::mem::zeroed();

    typentry = (*(*fcinfo).flinfo).fn_extra as *mut TypeCacheEntry;
    if typentry.is_null() || (*typentry).type_id != element_type {
        typentry = lookup_type_cache(element_type, TYPECACHE_HASH_EXTENDED_PROC_FINFO);
        if !OidIsValid((*typentry).hash_extended_proc_finfo.fn_oid) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not identify an extended hash function for type {}",
                    std::ffi::CStr::from_ptr(format_type_be(element_type)).to_string_lossy()
                )
            );
        }
        (*(*fcinfo).flinfo).fn_extra = typentry as *mut c_void;
    }
    typlen = (*typentry).typlen as c_int;
    typbyval = (*typentry).typbyval;
    typalign = (*typentry).typalign;

    InitFunctionCallInfoData!(
        locfcinfo,
        &raw mut (*typentry).hash_extended_proc_finfo,
        2,
        PG_GET_COLLATION!(fcinfo),
        null_mut(),
        null_mut()
    );

    /* Loop over source data */
    nitems = ArrayGetNItems(ndims, dims);
    array_iter_setup(&raw mut iter, array);

    i = 0;
    while i < nitems {
        let elt: Datum;
        let mut isnull: bool = false;
        let elthash: uint64;

        /* Get element, checking for NULL */
        elt = array_iter_next(&raw mut iter, &raw mut isnull, i, typlen, typbyval, typalign);

        if isnull {
            elthash = 0;
        } else {
            /* Apply the hash function */
            (*(*locfcinfo).args.as_mut_ptr().add(0)).value = elt;
            (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
            (*(*locfcinfo).args.as_mut_ptr().add(1)).value = Int64GetDatum(seed as i64);
            (*(*locfcinfo).args.as_mut_ptr().add(1)).isnull = false;
            elthash = DatumGetUInt64(FunctionCallInvoke!(locfcinfo));
            /* We don't expect hash functions to return null */
            Assert!(!(*locfcinfo).isnull);
        }

        result = (result << 5).wrapping_sub(result).wrapping_add(elthash);
        i += 1;
    }

    AARR_FREE_IF_COPY!(fcinfo, array, 0);

    PG_RETURN_UINT64!(result)
}

/*-----------------------------------------------------------------------------
 * array overlap/containment comparisons
 *		These use the same methods of comparing array elements as array_eq.
 *		We consider only the elements of the arrays, ignoring dimensionality.
 *----------------------------------------------------------------------------
 */

/*
 * array_contain_compare :
 *		  compares two arrays for overlap/containment
 *
 * When matchall is true, return true if all members of array1 are in array2.
 * When matchall is false, return true if any members of array1 are in array2.
 */
unsafe fn array_contain_compare(
    array1: *mut AnyArrayType,
    array2: *mut AnyArrayType,
    collation: Oid,
    matchall: bool,
    fn_extra: *mut *mut c_void,
) -> bool {
    LOCAL_FCINFO!(locfcinfo, 2);
    let mut result = matchall;
    let element_type = AARR_ELEMTYPE(array1);
    let typentry: *mut TypeCacheEntry;
    let nelems1: c_int;
    let mut values2: *mut Datum = null_mut();
    let mut nulls2: *mut bool = null_mut();
    let mut nelems2: c_int = 0;
    let typlen: c_int;
    let typbyval: bool;
    let typalign: c_char;
    let mut i: c_int;
    let mut j: c_int;
    let mut it1: array_iter = core::mem::zeroed();

    if element_type != AARR_ELEMTYPE(array2) {
        ereport!(
            ERROR,
            errmsg!("cannot compare arrays of different element types")
        );
    }

    /*
     * We arrange to look up the equality function only once per series of
     * calls, assuming the element type doesn't change underneath us.  The
     * typcache is used so that we have no memory leakage when being used as
     * an index support function.
     */
    let mut typentry_l = *fn_extra as *mut TypeCacheEntry;
    if typentry_l.is_null() || (*typentry_l).type_id != element_type {
        typentry_l = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);
        if !OidIsValid((*typentry_l).eq_opr_finfo.fn_oid) {
            ereport!(
                ERROR,
                errmsg!(
                    "could not identify an equality operator for type {}",
                    std::ffi::CStr::from_ptr(format_type_be(element_type)).to_string_lossy()
                )
            );
        }
        *fn_extra = typentry_l as *mut c_void;
    }
    typentry = typentry_l;
    typlen = (*typentry).typlen as c_int;
    typbyval = (*typentry).typbyval;
    typalign = (*typentry).typalign;

    /*
     * Since we probably will need to scan array2 multiple times, it's
     * worthwhile to use deconstruct_array on it.  We scan array1 the hard way
     * however, since we very likely won't need to look at all of it.
     */
    if VARATT_IS_EXPANDED_HEADER(array2 as *const c_void) {
        /* This should be safe even if input is read-only */
        let xpn2 = xpn_ptr(array2) as *mut ExpandedArrayHeader;
        deconstruct_expanded_array(xpn2);
        values2 = (*xpn2).dvalues;
        nulls2 = (*xpn2).dnulls;
        nelems2 = (*xpn2).nelems;
    } else {
        deconstruct_array(
            array2 as *mut ArrayType,
            element_type,
            typlen,
            typbyval,
            typalign,
            &raw mut values2,
            &raw mut nulls2,
            &raw mut nelems2,
        );
    }

    /*
     * Apply the comparison operator to each pair of array elements.
     */
    InitFunctionCallInfoData!(
        locfcinfo,
        &raw mut (*typentry).eq_opr_finfo,
        2,
        collation,
        null_mut(),
        null_mut()
    );

    /* Loop over source data */
    nelems1 = ArrayGetNItems(AARR_NDIM(array1), AARR_DIMS(array1));
    array_iter_setup(&raw mut it1, array1);

    i = 0;
    'outer: while i < nelems1 {
        let elt1: Datum;
        let mut isnull1: bool = false;

        /* Get element, checking for NULL */
        elt1 = array_iter_next(&raw mut it1, &raw mut isnull1, i, typlen, typbyval, typalign);

        /*
         * We assume that the comparison operator is strict, so a NULL can't
         * match anything.  XXX this diverges from the "NULL=NULL" behavior of
         * array_eq, should we act like that?
         */
        if isnull1 {
            if matchall {
                result = false;
                break;
            }
            i += 1;
            continue;
        }

        j = 0;
        while j < nelems2 {
            let elt2: Datum = *values2.add(j as usize);
            let isnull2: bool = if !nulls2.is_null() {
                *nulls2.add(j as usize)
            } else {
                false
            };
            let oprresult: bool;

            if isnull2 {
                j += 1;
                continue; /* can't match */
            }

            /*
             * Apply the operator to the element pair; treat NULL as false
             */
            (*(*locfcinfo).args.as_mut_ptr().add(0)).value = elt1;
            (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
            (*(*locfcinfo).args.as_mut_ptr().add(1)).value = elt2;
            (*(*locfcinfo).args.as_mut_ptr().add(1)).isnull = false;
            (*locfcinfo).isnull = false;
            oprresult = DatumGetBool(FunctionCallInvoke!(locfcinfo));
            if !(*locfcinfo).isnull && oprresult {
                break;
            }
            j += 1;
        }

        if j < nelems2 {
            /* found a match for elt1 */
            if !matchall {
                result = true;
                break 'outer;
            }
        } else {
            /* no match for elt1 */
            if matchall {
                result = false;
                break 'outer;
            }
        }
        i += 1;
    }

    result
}

pub unsafe fn arrayoverlap(fcinfo: FunctionCallInfo) -> Datum {
    let array1 = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let array2 = PG_GETARG_ANY_ARRAY_P!(fcinfo, 1);
    let collation = PG_GET_COLLATION!(fcinfo);
    let result: bool;

    result = array_contain_compare(
        array1,
        array2,
        collation,
        false,
        &raw mut (*(*fcinfo).flinfo).fn_extra,
    );

    /* Avoid leaking memory when handed toasted input. */
    AARR_FREE_IF_COPY!(fcinfo, array1, 0);
    AARR_FREE_IF_COPY!(fcinfo, array2, 1);

    PG_RETURN_BOOL!(result)
}

pub unsafe fn arraycontains(fcinfo: FunctionCallInfo) -> Datum {
    let array1 = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let array2 = PG_GETARG_ANY_ARRAY_P!(fcinfo, 1);
    let collation = PG_GET_COLLATION!(fcinfo);
    let result: bool;

    result = array_contain_compare(
        array2,
        array1,
        collation,
        true,
        &raw mut (*(*fcinfo).flinfo).fn_extra,
    );

    /* Avoid leaking memory when handed toasted input. */
    AARR_FREE_IF_COPY!(fcinfo, array1, 0);
    AARR_FREE_IF_COPY!(fcinfo, array2, 1);

    PG_RETURN_BOOL!(result)
}

pub unsafe fn arraycontained(fcinfo: FunctionCallInfo) -> Datum {
    let array1 = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
    let array2 = PG_GETARG_ANY_ARRAY_P!(fcinfo, 1);
    let collation = PG_GET_COLLATION!(fcinfo);
    let result: bool;

    result = array_contain_compare(
        array1,
        array2,
        collation,
        true,
        &raw mut (*(*fcinfo).flinfo).fn_extra,
    );

    /* Avoid leaking memory when handed toasted input. */
    AARR_FREE_IF_COPY!(fcinfo, array1, 0);
    AARR_FREE_IF_COPY!(fcinfo, array2, 1);

    PG_RETURN_BOOL!(result)
}

/*-----------------------------------------------------------------------------
 * Array iteration functions
 *		These functions are used to iterate efficiently through arrays
 *-----------------------------------------------------------------------------
 */

/*
 * array_create_iterator --- set up to iterate through an array
 *
 * If slice_ndim is zero, we will iterate element-by-element; the returned
 * datums are of the array's element type.
 *
 * If slice_ndim is 1..ARR_NDIM(arr), we will iterate by slices: the
 * returned datums are of the same array type as 'arr', but of size
 * equal to the rightmost N dimensions of 'arr'.
 *
 * The passed-in array must remain valid for the lifetime of the iterator.
 */
#[no_mangle]
pub unsafe fn array_create_iterator(
    arr: *mut ArrayType,
    slice_ndim: c_int,
    mstate: *mut ArrayMetaState,
) -> ArrayIterator {
    let iterator: ArrayIterator =
        palloc0(core::mem::size_of::<ArrayIteratorData>()) as ArrayIterator;

    /*
     * Sanity-check inputs --- caller should have got this right already
     */
    Assert!(PointerIsValid(arr));
    if slice_ndim < 0 || slice_ndim > ARR_NDIM(arr) {
        elog!(ERROR, "invalid arguments to array_create_iterator");
    }

    /*
     * Remember basic info about the array and its element type
     */
    (*iterator).arr = arr;
    (*iterator).nullbitmap = ARR_NULLBITMAP(arr);
    (*iterator).nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

    if !mstate.is_null() {
        Assert!((*mstate).element_type == ARR_ELEMTYPE(arr));

        (*iterator).typlen = (*mstate).typlen;
        (*iterator).typbyval = (*mstate).typbyval;
        (*iterator).typalign = (*mstate).typalign;
    } else {
        get_typlenbyvalalign(
            ARR_ELEMTYPE(arr),
            &raw mut (*iterator).typlen,
            &raw mut (*iterator).typbyval,
            &raw mut (*iterator).typalign,
        );
    }

    /*
     * Remember the slicing parameters.
     */
    (*iterator).slice_ndim = slice_ndim;

    if slice_ndim > 0 {
        /*
         * Get pointers into the array's dims and lbound arrays to represent
         * the dims/lbound arrays of a slice.  These are the same as the
         * rightmost N dimensions of the array.
         */
        (*iterator).slice_dims = ARR_DIMS(arr).add((ARR_NDIM(arr) - slice_ndim) as usize);
        (*iterator).slice_lbound = ARR_LBOUND(arr).add((ARR_NDIM(arr) - slice_ndim) as usize);

        /*
         * Compute number of elements in a slice.
         */
        (*iterator).slice_len = ArrayGetNItems(slice_ndim, (*iterator).slice_dims);

        /*
         * Create workspace for building sub-arrays.
         */
        (*iterator).slice_values =
            palloc((*iterator).slice_len as usize * core::mem::size_of::<Datum>()) as *mut Datum;
        (*iterator).slice_nulls =
            palloc((*iterator).slice_len as usize * core::mem::size_of::<bool>()) as *mut bool;
    }

    /*
     * Initialize our data pointer and linear element number.  These will
     * advance through the array during array_iterate().
     */
    (*iterator).data_ptr = ARR_DATA_PTR(arr);
    (*iterator).current_item = 0;

    iterator
}

/*
 * Iterate through the array referenced by 'iterator'.
 *
 * As long as there is another element (or slice), return it into
 * *value / *isnull, and return true.  Return false when no more data.
 */
#[no_mangle]
pub unsafe fn array_iterate(iterator: ArrayIterator, value: *mut Datum, isnull: *mut bool) -> bool {
    /* Done if we have reached the end of the array */
    if (*iterator).current_item >= (*iterator).nitems {
        return false;
    }

    if (*iterator).slice_ndim == 0 {
        /*
         * Scalar case: return one element.
         */
        let cur = (*iterator).current_item;
        (*iterator).current_item += 1;
        if array_get_isnull((*iterator).nullbitmap, cur) {
            *isnull = true;
            *value = 0 as Datum;
        } else {
            /* non-NULL, so fetch the individual Datum to return */
            let mut p = (*iterator).data_ptr;

            *isnull = false;
            *value = fetch_att(p as *const c_void, (*iterator).typbyval, (*iterator).typlen as c_int);

            /* Move our data pointer forward to the next element */
            p = att_addlength_pointer(p as usize, (*iterator).typlen as c_int, p) as *mut c_char;
            p = att_align_nominal(p as usize, (*iterator).typalign) as *mut c_char;
            (*iterator).data_ptr = p;
        }
    } else {
        /*
         * Slice case: build and return an array of the requested size.
         */
        let result: *mut ArrayType;
        let values = (*iterator).slice_values;
        let nulls = (*iterator).slice_nulls;
        let mut p = (*iterator).data_ptr;
        let mut i: c_int;

        i = 0;
        while i < (*iterator).slice_len {
            let cur = (*iterator).current_item;
            (*iterator).current_item += 1;
            if array_get_isnull((*iterator).nullbitmap, cur) {
                *nulls.add(i as usize) = true;
                *values.add(i as usize) = 0 as Datum;
            } else {
                *nulls.add(i as usize) = false;
                *values.add(i as usize) =
                    fetch_att(p as *const c_void, (*iterator).typbyval, (*iterator).typlen as c_int);

                /* Move our data pointer forward to the next element */
                p = att_addlength_pointer(p as usize, (*iterator).typlen as c_int, p) as *mut c_char;
                p = att_align_nominal(p as usize, (*iterator).typalign) as *mut c_char;
            }
            i += 1;
        }

        (*iterator).data_ptr = p;

        result = construct_md_array(
            values,
            nulls,
            (*iterator).slice_ndim,
            (*iterator).slice_dims,
            (*iterator).slice_lbound,
            ARR_ELEMTYPE((*iterator).arr),
            (*iterator).typlen as c_int,
            (*iterator).typbyval,
            (*iterator).typalign,
        );

        *isnull = false;
        *value = PointerGetDatum(result as *const c_void);
    }

    true
}

/*
 * Release an ArrayIterator data structure
 */
pub unsafe fn array_free_iterator(iterator: ArrayIterator) {
    if (*iterator).slice_ndim > 0 {
        pfree((*iterator).slice_values as *mut c_void);
        pfree((*iterator).slice_nulls as *mut c_void);
    }
    pfree(iterator as *mut c_void);
}

/***************************************************************************/
/******************|		  Support  Routines			  |*****************/
/***************************************************************************/

/*
 * Check whether a specific array element is NULL
 *
 * nullbitmap: pointer to array's null bitmap (NULL if none)
 * offset: 0-based linear element number of array element
 */
unsafe fn array_get_isnull(nullbitmap: *const bits8, offset: c_int) -> bool {
    if nullbitmap.is_null() {
        return false; /* assume not null */
    }
    if *nullbitmap.add((offset / 8) as usize) & (1 << (offset % 8)) as bits8 != 0 {
        return false; /* not null */
    }
    true
}

/*
 * Set a specific array element's null-bitmap entry
 *
 * nullbitmap: pointer to array's null bitmap (mustn't be NULL)
 * offset: 0-based linear element number of array element
 * isNull: null status to set
 */
unsafe fn array_set_isnull(mut nullbitmap: *mut bits8, offset: c_int, isNull: bool) {
    let bitmask: c_int;

    nullbitmap = nullbitmap.add((offset / 8) as usize);
    bitmask = 1 << (offset % 8);
    if isNull {
        *nullbitmap &= !bitmask as bits8;
    } else {
        *nullbitmap |= bitmask as bits8;
    }
}

/*
 * Fetch array element at pointer, converted correctly to a Datum
 *
 * Caller must have handled case of NULL element
 */
unsafe fn ArrayCast(value: *mut c_char, byval: bool, len: c_int) -> Datum {
    fetch_att(value as *const c_void, byval, len)
}

/*
 * Copy datum to *dest and return total space used (including align padding)
 *
 * Caller must have handled case of NULL element
 */
pub unsafe fn ArrayCastAndSet(
    src: Datum,
    typlen: c_int,
    typbyval: bool,
    typalign: c_char,
    dest: *mut c_char,
) -> c_int {
    let mut inc: c_int;

    if typlen > 0 {
        if typbyval {
            store_att_byval(dest as *mut c_void, src, typlen);
        } else {
            memmove(
                dest as *mut c_void,
                DatumGetPointer(src) as *const c_void,
                typlen as usize,
            );
        }
        inc = att_align_nominal(typlen as usize, typalign) as c_int;
    } else {
        Assert!(!typbyval);
        inc = att_addlength_datum(0, typlen, src) as c_int;
        memmove(
            dest as *mut c_void,
            DatumGetPointer(src) as *const c_void,
            inc as usize,
        );
        inc = att_align_nominal(inc as usize, typalign) as c_int;
    }

    inc
}

/*
 * Advance ptr over nitems array elements
 *
 * ptr: starting location in array
 * offset: 0-based linear element number of first element (the one at *ptr)
 * nullbitmap: start of array's null bitmap, or NULL if none
 * nitems: number of array elements to advance over (>= 0)
 * typlen, typbyval, typalign: storage parameters of array element datatype
 *
 * It is caller's responsibility to ensure that nitems is within range
 */
unsafe fn array_seek(
    mut ptr: *mut c_char,
    offset: c_int,
    mut nullbitmap: *mut bits8,
    nitems: c_int,
    typlen: c_int,
    typbyval: bool,
    typalign: c_char,
) -> *mut c_char {
    let mut bitmask: c_int;
    let mut i: c_int;

    /* easy if fixed-size elements and no NULLs */
    if typlen > 0 && nullbitmap.is_null() {
        return ptr.add(nitems as usize * (att_align_nominal(typlen as usize, typalign) as Size));
    }

    /* seems worth having separate loops for NULL and no-NULLs cases */
    if !nullbitmap.is_null() {
        nullbitmap = nullbitmap.add((offset / 8) as usize);
        bitmask = 1 << (offset % 8);

        i = 0;
        while i < nitems {
            if *nullbitmap & bitmask as bits8 != 0 {
                ptr = att_addlength_pointer(ptr as usize, typlen, ptr) as *mut c_char;
                ptr = att_align_nominal(ptr as usize, typalign) as *mut c_char;
            }
            bitmask <<= 1;
            if bitmask == 0x100 {
                nullbitmap = nullbitmap.add(1);
                bitmask = 1;
            }
            i += 1;
        }
    } else {
        i = 0;
        while i < nitems {
            ptr = att_addlength_pointer(ptr as usize, typlen, ptr) as *mut c_char;
            ptr = att_align_nominal(ptr as usize, typalign) as *mut c_char;
            i += 1;
        }
    }
    ptr
}

/*
 * Compute total size of the nitems array elements starting at *ptr
 *
 * Parameters same as for array_seek
 */
unsafe fn array_nelems_size(
    ptr: *mut c_char,
    offset: c_int,
    nullbitmap: *mut bits8,
    nitems: c_int,
    typlen: c_int,
    typbyval: bool,
    typalign: c_char,
) -> c_int {
    array_seek(ptr, offset, nullbitmap, nitems, typlen, typbyval, typalign).offset_from(ptr) as c_int
}

/*
 * Copy nitems array elements from srcptr to destptr
 *
 * destptr: starting destination location (must be enough room!)
 * nitems: number of array elements to copy (>= 0)
 * srcptr: starting location in source array
 * offset: 0-based linear element number of first element (the one at *srcptr)
 * nullbitmap: start of source array's null bitmap, or NULL if none
 * typlen, typbyval, typalign: storage parameters of array element datatype
 *
 * Returns number of bytes copied
 *
 * NB: this does not take care of setting up the destination's null bitmap!
 */
unsafe fn array_copy(
    destptr: *mut c_char,
    nitems: c_int,
    srcptr: *mut c_char,
    offset: c_int,
    nullbitmap: *mut bits8,
    typlen: c_int,
    typbyval: bool,
    typalign: c_char,
) -> c_int {
    let numbytes: c_int;

    numbytes = array_nelems_size(
        srcptr, offset, nullbitmap, nitems, typlen, typbyval, typalign,
    );
    memcpy(
        destptr as *mut c_void,
        srcptr as *const c_void,
        numbytes as usize,
    );
    numbytes
}

/*
 * Copy nitems null-bitmap bits from source to destination
 *
 * destbitmap: start of destination array's null bitmap (mustn't be NULL)
 * destoffset: 0-based linear element number of first dest element
 * srcbitmap: start of source array's null bitmap, or NULL if none
 * srcoffset: 0-based linear element number of first source element
 * nitems: number of bits to copy (>= 0)
 *
 * If srcbitmap is NULL then we assume the source is all-non-NULL and
 * fill 1's into the destination bitmap.  Note that only the specified
 * bits in the destination map are changed, not any before or after.
 *
 * Note: this could certainly be optimized using standard bitblt methods.
 * However, it's not clear that the typical Postgres array has enough elements
 * to make it worth worrying too much.  For the moment, KISS.
 */
pub unsafe fn array_bitmap_copy(
    mut destbitmap: *mut bits8,
    destoffset: c_int,
    mut srcbitmap: *const bits8,
    srcoffset: c_int,
    mut nitems: c_int,
) {
    let mut destbitmask: c_int;
    let mut destbitval: c_int;
    let mut srcbitmask: c_int;
    let mut srcbitval: c_int;

    Assert!(!destbitmap.is_null());
    if nitems <= 0 {
        return; /* don't risk fetch off end of memory */
    }
    destbitmap = destbitmap.add((destoffset / 8) as usize);
    destbitmask = 1 << (destoffset % 8);
    destbitval = *destbitmap as c_int;
    if !srcbitmap.is_null() {
        srcbitmap = srcbitmap.add((srcoffset / 8) as usize);
        srcbitmask = 1 << (srcoffset % 8);
        srcbitval = *srcbitmap as c_int;
        loop {
            let old = nitems;
            nitems -= 1;
            if old <= 0 {
                break;
            }
            if srcbitval & srcbitmask != 0 {
                destbitval |= destbitmask;
            } else {
                destbitval &= !destbitmask;
            }
            destbitmask <<= 1;
            if destbitmask == 0x100 {
                *destbitmap = destbitval as bits8;
                destbitmap = destbitmap.add(1);
                destbitmask = 1;
                if nitems > 0 {
                    destbitval = *destbitmap as c_int;
                }
            }
            srcbitmask <<= 1;
            if srcbitmask == 0x100 {
                srcbitmap = srcbitmap.add(1);
                srcbitmask = 1;
                if nitems > 0 {
                    srcbitval = *srcbitmap as c_int;
                }
            }
        }
        if destbitmask != 1 {
            *destbitmap = destbitval as bits8;
        }
    } else {
        loop {
            let old = nitems;
            nitems -= 1;
            if old <= 0 {
                break;
            }
            destbitval |= destbitmask;
            destbitmask <<= 1;
            if destbitmask == 0x100 {
                *destbitmap = destbitval as bits8;
                destbitmap = destbitmap.add(1);
                destbitmask = 1;
                if nitems > 0 {
                    destbitval = *destbitmap as c_int;
                }
            }
        }
        if destbitmask != 1 {
            *destbitmap = destbitval as bits8;
        }
    }
}

/*
 * Compute space needed for a slice of an array
 *
 * We assume the caller has verified that the slice coordinates are valid.
 */
unsafe fn array_slice_size(
    arraydataptr: *mut c_char,
    arraynullsptr: *mut bits8,
    ndim: c_int,
    dim: *mut c_int,
    lb: *mut c_int,
    st: *mut c_int,
    endp: *mut c_int,
    typlen: c_int,
    typbyval: bool,
    typalign: c_char,
) -> c_int {
    let mut src_offset: c_int;
    let mut span: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut prod: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut dist: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut indx: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut ptr: *mut c_char;
    let mut i: c_int;
    let mut j: c_int;
    let mut inc: c_int;
    let mut count: c_int = 0;

    mda_get_range(ndim, span.as_mut_ptr(), st, endp);

    /* Pretty easy for fixed element length without nulls ... */
    if typlen > 0 && arraynullsptr.is_null() {
        return ArrayGetNItems(ndim, span.as_mut_ptr()) * att_align_nominal(typlen as usize, typalign) as c_int;
    }

    /* Else gotta do it the hard way */
    src_offset = ArrayGetOffset(ndim, dim, lb, st);
    ptr = array_seek(
        arraydataptr,
        0,
        arraynullsptr,
        src_offset,
        typlen,
        typbyval,
        typalign,
    );
    mda_get_prod(ndim, dim, prod.as_mut_ptr());
    mda_get_offset_values(ndim, dist.as_mut_ptr(), prod.as_mut_ptr(), span.as_mut_ptr());
    i = 0;
    while i < ndim {
        indx[i as usize] = 0;
        i += 1;
    }
    j = ndim - 1;
    loop {
        if dist[j as usize] != 0 {
            ptr = array_seek(
                ptr,
                src_offset,
                arraynullsptr,
                dist[j as usize],
                typlen,
                typbyval,
                typalign,
            );
            src_offset += dist[j as usize];
        }
        if !array_get_isnull(arraynullsptr, src_offset) {
            inc = att_addlength_pointer(0, typlen, ptr) as c_int;
            inc = att_align_nominal(inc as usize, typalign) as c_int;
            ptr = ptr.add(inc as usize);
            count += inc;
        }
        src_offset += 1;
        j = mda_next_tuple(ndim, indx.as_mut_ptr(), span.as_mut_ptr());
        if j == -1 {
            break;
        }
    }
    count
}

/*
 * Extract a slice of an array into consecutive elements in the destination
 * array.
 *
 * We assume the caller has verified that the slice coordinates are valid,
 * allocated enough storage for the result, and initialized the header
 * of the new array.
 */
unsafe fn array_extract_slice(
    newarray: *mut ArrayType,
    ndim: c_int,
    dim: *mut c_int,
    lb: *mut c_int,
    arraydataptr: *mut c_char,
    arraynullsptr: *mut bits8,
    st: *mut c_int,
    endp: *mut c_int,
    typlen: c_int,
    typbyval: bool,
    typalign: c_char,
) {
    let mut destdataptr = ARR_DATA_PTR(newarray);
    let destnullsptr = ARR_NULLBITMAP(newarray);
    let mut srcdataptr: *mut c_char;
    let mut src_offset: c_int;
    let mut dest_offset: c_int;
    let mut prod: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut span: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut dist: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut indx: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut i: c_int;
    let mut j: c_int;
    let mut inc: c_int;

    src_offset = ArrayGetOffset(ndim, dim, lb, st);
    srcdataptr = array_seek(
        arraydataptr,
        0,
        arraynullsptr,
        src_offset,
        typlen,
        typbyval,
        typalign,
    );
    mda_get_prod(ndim, dim, prod.as_mut_ptr());
    mda_get_range(ndim, span.as_mut_ptr(), st, endp);
    mda_get_offset_values(ndim, dist.as_mut_ptr(), prod.as_mut_ptr(), span.as_mut_ptr());
    i = 0;
    while i < ndim {
        indx[i as usize] = 0;
        i += 1;
    }
    dest_offset = 0;
    j = ndim - 1;
    loop {
        if dist[j as usize] != 0 {
            /* skip unwanted elements */
            srcdataptr = array_seek(
                srcdataptr,
                src_offset,
                arraynullsptr,
                dist[j as usize],
                typlen,
                typbyval,
                typalign,
            );
            src_offset += dist[j as usize];
        }
        inc = array_copy(
            destdataptr,
            1,
            srcdataptr,
            src_offset,
            arraynullsptr,
            typlen,
            typbyval,
            typalign,
        );
        if !destnullsptr.is_null() {
            array_bitmap_copy(destnullsptr, dest_offset, arraynullsptr, src_offset, 1);
        }
        destdataptr = destdataptr.add(inc as usize);
        srcdataptr = srcdataptr.add(inc as usize);
        src_offset += 1;
        dest_offset += 1;
        j = mda_next_tuple(ndim, indx.as_mut_ptr(), span.as_mut_ptr());
        if j == -1 {
            break;
        }
    }
}

/*
 * Insert a slice into an array.
 *
 * ndim/dim[]/lb[] are dimensions of the original array.  A new array with
 * those same dimensions is to be constructed.  destArray must already
 * have been allocated and its header initialized.
 *
 * st[]/endp[] identify the slice to be replaced.  Elements within the slice
 * volume are taken from consecutive elements of the srcArray; elements
 * outside it are copied from origArray.
 *
 * We assume the caller has verified that the slice coordinates are valid.
 */
unsafe fn array_insert_slice(
    destArray: *mut ArrayType,
    origArray: *mut ArrayType,
    srcArray: *mut ArrayType,
    ndim: c_int,
    dim: *mut c_int,
    lb: *mut c_int,
    st: *mut c_int,
    endp: *mut c_int,
    typlen: c_int,
    typbyval: bool,
    typalign: c_char,
) {
    let mut destPtr = ARR_DATA_PTR(destArray);
    let mut origPtr = ARR_DATA_PTR(origArray);
    let mut srcPtr = ARR_DATA_PTR(srcArray);
    let destBitmap = ARR_NULLBITMAP(destArray);
    let origBitmap = ARR_NULLBITMAP(origArray);
    let srcBitmap = ARR_NULLBITMAP(srcArray);
    let orignitems = ArrayGetNItems(ARR_NDIM(origArray), ARR_DIMS(origArray));
    let mut dest_offset: c_int;
    let mut orig_offset: c_int;
    let mut src_offset: c_int;
    let mut prod: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut span: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut dist: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut indx: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut i: c_int;
    let mut j: c_int;
    let mut inc: c_int;

    dest_offset = ArrayGetOffset(ndim, dim, lb, st);
    /* copy items before the slice start */
    inc = array_copy(
        destPtr, dest_offset, origPtr, 0, origBitmap, typlen, typbyval, typalign,
    );
    destPtr = destPtr.add(inc as usize);
    origPtr = origPtr.add(inc as usize);
    if !destBitmap.is_null() {
        array_bitmap_copy(destBitmap, 0, origBitmap, 0, dest_offset);
    }
    orig_offset = dest_offset;
    mda_get_prod(ndim, dim, prod.as_mut_ptr());
    mda_get_range(ndim, span.as_mut_ptr(), st, endp);
    mda_get_offset_values(ndim, dist.as_mut_ptr(), prod.as_mut_ptr(), span.as_mut_ptr());
    i = 0;
    while i < ndim {
        indx[i as usize] = 0;
        i += 1;
    }
    src_offset = 0;
    j = ndim - 1;
    loop {
        /* Copy/advance over elements between here and next part of slice */
        if dist[j as usize] != 0 {
            inc = array_copy(
                destPtr,
                dist[j as usize],
                origPtr,
                orig_offset,
                origBitmap,
                typlen,
                typbyval,
                typalign,
            );
            destPtr = destPtr.add(inc as usize);
            origPtr = origPtr.add(inc as usize);
            if !destBitmap.is_null() {
                array_bitmap_copy(
                    destBitmap,
                    dest_offset,
                    origBitmap,
                    orig_offset,
                    dist[j as usize],
                );
            }
            dest_offset += dist[j as usize];
            orig_offset += dist[j as usize];
        }
        /* Copy new element at this slice position */
        inc = array_copy(
            destPtr, 1, srcPtr, src_offset, srcBitmap, typlen, typbyval, typalign,
        );
        if !destBitmap.is_null() {
            array_bitmap_copy(destBitmap, dest_offset, srcBitmap, src_offset, 1);
        }
        destPtr = destPtr.add(inc as usize);
        srcPtr = srcPtr.add(inc as usize);
        dest_offset += 1;
        src_offset += 1;
        /* Advance over old element at this slice position */
        origPtr = array_seek(
            origPtr, orig_offset, origBitmap, 1, typlen, typbyval, typalign,
        );
        orig_offset += 1;
        j = mda_next_tuple(ndim, indx.as_mut_ptr(), span.as_mut_ptr());
        if j == -1 {
            break;
        }
    }

    /* don't miss any data at the end */
    array_copy(
        destPtr,
        orignitems - orig_offset,
        origPtr,
        orig_offset,
        origBitmap,
        typlen,
        typbyval,
        typalign,
    );
    if !destBitmap.is_null() {
        array_bitmap_copy(
            destBitmap,
            dest_offset,
            origBitmap,
            orig_offset,
            orignitems - orig_offset,
        );
    }
}

/*
 * initArrayResult - initialize an empty ArrayBuildState
 *
 *	element_type is the array element type (must be a valid array element type)
 *	rcontext is where to keep working state
 *	subcontext is a flag determining whether to use a separate memory context
 *
 * Note: there are two common schemes for using accumArrayResult().
 * In the older scheme, you start with a NULL ArrayBuildState pointer, and
 * call accumArrayResult once per element.  In this scheme you end up with
 * a NULL pointer if there were no elements, which you need to special-case.
 * In the newer scheme, call initArrayResult and then call accumArrayResult
 * once per element.  In this scheme you always end with a non-NULL pointer
 * that you can pass to makeArrayResult; you get an empty array if there
 * were no elements.  This is preferred if an empty array is what you want.
 *
 * It's possible to choose whether to create a separate memory context for the
 * array build state, or whether to allocate it directly within rcontext.
 *
 * When there are many concurrent small states (e.g. array_agg() using hash
 * aggregation of many small groups), using a separate memory context for each
 * one may result in severe memory bloat. In such cases, use the same memory
 * context to initialize all such array build states, and pass
 * subcontext=false.
 *
 * In cases when the array build states have different lifetimes, using a
 * single memory context is impractical. Instead, pass subcontext=true so that
 * the array build states can be freed individually.
 */
pub unsafe fn initArrayResult(
    element_type: Oid,
    rcontext: MemoryContext,
    subcontext: bool,
) -> *mut ArrayBuildState {
    /*
     * When using a subcontext, we can afford to start with a somewhat larger
     * initial array size.  Without subcontexts, we'd better hope that most of
     * the states stay small ...
     */
    initArrayResultWithSize(
        element_type,
        rcontext,
        subcontext,
        if subcontext { 64 } else { 8 },
    )
}

/*
 * initArrayResultWithSize
 *		As initArrayResult, but allow the initial size of the allocated arrays
 *		to be specified.
 */
pub unsafe fn initArrayResultWithSize(
    element_type: Oid,
    rcontext: MemoryContext,
    subcontext: bool,
    initsize: c_int,
) -> *mut ArrayBuildState {
    let astate: *mut ArrayBuildState;
    let mut arr_context: MemoryContext = rcontext;

    /* Make a temporary context to hold all the junk */
    if subcontext {
        arr_context = AllocSetContextCreate!(
            rcontext,
            c"accumArrayResult".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        );
    }

    astate =
        MemoryContextAlloc(arr_context, core::mem::size_of::<ArrayBuildState>()) as *mut ArrayBuildState;
    (*astate).mcontext = arr_context;
    (*astate).private_cxt = subcontext;
    (*astate).alen = initsize;
    (*astate).dvalues =
        MemoryContextAlloc(arr_context, (*astate).alen as usize * core::mem::size_of::<Datum>())
            as *mut Datum;
    (*astate).dnulls =
        MemoryContextAlloc(arr_context, (*astate).alen as usize * core::mem::size_of::<bool>())
            as *mut bool;
    (*astate).nelems = 0;
    (*astate).element_type = element_type;
    get_typlenbyvalalign(
        element_type,
        &raw mut (*astate).typlen,
        &raw mut (*astate).typbyval,
        &raw mut (*astate).typalign,
    );

    astate
}

/*
 * accumArrayResult - accumulate one (more) Datum for an array result
 *
 *	astate is working state (can be NULL on first call)
 *	dvalue/disnull represent the new Datum to append to the array
 *	element_type is the Datum's type (must be a valid array element type)
 *	rcontext is where to keep working state
 */
pub unsafe fn accumArrayResult(
    mut astate: *mut ArrayBuildState,
    mut dvalue: Datum,
    disnull: bool,
    element_type: Oid,
    rcontext: MemoryContext,
) -> *mut ArrayBuildState {
    let oldcontext: MemoryContext;

    if astate.is_null() {
        /* First time through --- initialize */
        astate = initArrayResult(element_type, rcontext, true);
    } else {
        Assert!((*astate).element_type == element_type);
    }

    oldcontext = MemoryContextSwitchTo((*astate).mcontext);

    /* enlarge dvalues[]/dnulls[] if needed */
    if (*astate).nelems >= (*astate).alen {
        (*astate).alen *= 2;
        /* give an array-related error if we go past MaxAllocSize */
        if !AllocSizeIsValid((*astate).alen as usize * core::mem::size_of::<Datum>()) {
            ereport!(
                ERROR,
                errmsg!(
                    "array size exceeds the maximum allowed ({})",
                    MaxAllocSize as c_int
                )
            );
        }
        (*astate).dvalues = repalloc(
            (*astate).dvalues as *mut c_void,
            (*astate).alen as usize * core::mem::size_of::<Datum>(),
        ) as *mut Datum;
        (*astate).dnulls = repalloc(
            (*astate).dnulls as *mut c_void,
            (*astate).alen as usize * core::mem::size_of::<bool>(),
        ) as *mut bool;
    }

    /*
     * Ensure pass-by-ref stuff is copied into mcontext; and detoast it too if
     * it's varlena.  (You might think that detoasting is not needed here
     * because construct_md_array can detoast the array elements later.
     * However, we must not let construct_md_array modify the ArrayBuildState
     * because that would mean array_agg_finalfn damages its input, which is
     * verboten.  Also, this way frequently saves one copying step.)
     */
    if !disnull && !(*astate).typbyval {
        if (*astate).typlen == -1 {
            dvalue = PointerGetDatum(crate::PG_DETOAST_DATUM_COPY!(dvalue) as *const c_void);
        } else {
            dvalue = datumCopy(dvalue, (*astate).typbyval, (*astate).typlen as c_int);
        }
    }

    *(*astate).dvalues.add((*astate).nelems as usize) = dvalue;
    *(*astate).dnulls.add((*astate).nelems as usize) = disnull;
    (*astate).nelems += 1;

    MemoryContextSwitchTo(oldcontext);

    astate
}

/*
 * makeArrayResult - produce 1-D final result of accumArrayResult
 *
 * Note: only releases astate if it was initialized within a separate memory
 * context (i.e. using subcontext=true when calling initArrayResult).
 *
 *	astate is working state (must not be NULL)
 *	rcontext is where to construct result
 */
pub unsafe fn makeArrayResult(astate: *mut ArrayBuildState, rcontext: MemoryContext) -> Datum {
    let ndims: c_int;
    let mut dims: [c_int; 1] = [0; 1];
    let mut lbs: [c_int; 1] = [0; 1];

    /* If no elements were presented, we want to create an empty array */
    ndims = if (*astate).nelems > 0 { 1 } else { 0 };
    dims[0] = (*astate).nelems;
    lbs[0] = 1;

    makeMdArrayResult(
        astate,
        ndims,
        dims.as_mut_ptr(),
        lbs.as_mut_ptr(),
        rcontext,
        (*astate).private_cxt,
    )
}

/*
 * makeMdArrayResult - produce multi-D final result of accumArrayResult
 *
 * beware: no check that specified dimensions match the number of values
 * accumulated.
 *
 * Note: if the astate was not initialized within a separate memory context
 * (that is, initArrayResult was called with subcontext=false), then using
 * release=true is illegal. Instead, release astate along with the rest of its
 * context when appropriate.
 *
 *	astate is working state (must not be NULL)
 *	rcontext is where to construct result
 *	release is true if okay to release working state
 */
pub unsafe fn makeMdArrayResult(
    astate: *mut ArrayBuildState,
    ndims: c_int,
    dims: *mut c_int,
    lbs: *mut c_int,
    rcontext: MemoryContext,
    release: bool,
) -> Datum {
    let result: *mut ArrayType;
    let oldcontext: MemoryContext;

    /* Build the final array result in rcontext */
    oldcontext = MemoryContextSwitchTo(rcontext);

    result = construct_md_array(
        (*astate).dvalues,
        (*astate).dnulls,
        ndims,
        dims,
        lbs,
        (*astate).element_type,
        (*astate).typlen as c_int,
        (*astate).typbyval,
        (*astate).typalign,
    );

    MemoryContextSwitchTo(oldcontext);

    /* Clean up all the junk */
    if release {
        Assert!((*astate).private_cxt);
        MemoryContextDelete((*astate).mcontext);
    }

    PointerGetDatum(result as *const c_void)
}

/*
 * The following three functions provide essentially the same API as
 * initArrayResult/accumArrayResult/makeArrayResult, but instead of accepting
 * inputs that are array elements, they accept inputs that are arrays and
 * produce an output array having N+1 dimensions.  The inputs must all have
 * identical dimensionality as well as element type.
 */

/*
 * initArrayResultArr - initialize an empty ArrayBuildStateArr
 *
 *	array_type is the array type (must be a valid varlena array type)
 *	element_type is the type of the array's elements (lookup if InvalidOid)
 *	rcontext is where to keep working state
 *	subcontext is a flag determining whether to use a separate memory context
 */
pub unsafe fn initArrayResultArr(
    array_type: Oid,
    mut element_type: Oid,
    rcontext: MemoryContext,
    subcontext: bool,
) -> *mut ArrayBuildStateArr {
    let astate: *mut ArrayBuildStateArr;
    let mut arr_context: MemoryContext = rcontext; /* by default use the parent ctx */

    /* Lookup element type, unless element_type already provided */
    if !OidIsValid(element_type) {
        element_type = get_element_type(array_type);

        if !OidIsValid(element_type) {
            ereport!(
                ERROR,
                errmsg!(
                    "data type {} is not an array type",
                    std::ffi::CStr::from_ptr(format_type_be(array_type)).to_string_lossy()
                )
            );
        }
    }

    /* Make a temporary context to hold all the junk */
    if subcontext {
        arr_context = AllocSetContextCreate!(
            rcontext,
            c"accumArrayResultArr".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        );
    }

    /* Note we initialize all fields to zero */
    astate = MemoryContextAllocZero(arr_context, core::mem::size_of::<ArrayBuildStateArr>())
        as *mut ArrayBuildStateArr;
    (*astate).mcontext = arr_context;
    (*astate).private_cxt = subcontext;

    /* Save relevant datatype information */
    (*astate).array_type = array_type;
    (*astate).element_type = element_type;

    astate
}

/*
 * accumArrayResultArr - accumulate one (more) sub-array for an array result
 *
 *	astate is working state (can be NULL on first call)
 *	dvalue/disnull represent the new sub-array to append to the array
 *	array_type is the array type (must be a valid varlena array type)
 *	rcontext is where to keep working state
 */
pub unsafe fn accumArrayResultArr(
    mut astate: *mut ArrayBuildStateArr,
    dvalue: Datum,
    disnull: bool,
    array_type: Oid,
    rcontext: MemoryContext,
) -> *mut ArrayBuildStateArr {
    let arg: *mut ArrayType;
    let oldcontext: MemoryContext;
    let dims: *mut c_int;
    let lbs: *mut c_int;
    let ndims: c_int;
    let nitems: c_int;
    let ndatabytes: c_int;
    let data: *mut c_char;
    let mut i: c_int;

    /*
     * We disallow accumulating null subarrays.  Another plausible definition
     * is to ignore them, but callers that want that can just skip calling
     * this function.
     */
    if disnull {
        ereport!(ERROR, errmsg!("cannot accumulate null arrays"));
    }

    /* Detoast input array in caller's context */
    arg = DatumGetArrayTypeP(dvalue);

    if astate.is_null() {
        astate = initArrayResultArr(array_type, InvalidOid, rcontext, true);
    } else {
        Assert!((*astate).array_type == array_type);
    }

    oldcontext = MemoryContextSwitchTo((*astate).mcontext);

    /* Collect this input's dimensions */
    ndims = ARR_NDIM(arg);
    dims = ARR_DIMS(arg);
    lbs = ARR_LBOUND(arg);
    data = ARR_DATA_PTR(arg);
    nitems = ArrayGetNItems(ndims, dims);
    ndatabytes = (ARR_SIZE(arg) as c_int) - (ARR_DATA_OFFSET(arg) as c_int);

    if (*astate).ndims == 0 {
        /* First input; check/save the dimensionality info */

        /* Should we allow empty inputs and just produce an empty output? */
        if ndims == 0 {
            ereport!(ERROR, errmsg!("cannot accumulate empty arrays"));
        }
        if ndims + 1 > MAXDIM {
            ereport!(
                ERROR,
                errmsg!(
                    "number of array dimensions ({}) exceeds the maximum allowed ({})",
                    ndims + 1,
                    MAXDIM
                )
            );
        }

        /*
         * The output array will have n+1 dimensions, with the ones after the
         * first matching the input's dimensions.
         */
        (*astate).ndims = ndims + 1;
        (*astate).dims[0] = 0;
        memcpy(
            (&raw mut (*astate).dims[1]) as *mut c_void,
            dims as *const c_void,
            ndims as usize * core::mem::size_of::<c_int>(),
        );
        (*astate).lbs[0] = 1;
        memcpy(
            (&raw mut (*astate).lbs[1]) as *mut c_void,
            lbs as *const c_void,
            ndims as usize * core::mem::size_of::<c_int>(),
        );

        /* Allocate at least enough data space for this item */
        (*astate).abytes = pg_nextpower2_32(Max(1024, ndatabytes + 1) as uint32) as c_int;
        (*astate).data = palloc((*astate).abytes as usize) as *mut c_char;
    } else {
        /* Second or later input: must match first input's dimensionality */
        if (*astate).ndims != ndims + 1 {
            ereport!(
                ERROR,
                errmsg!("cannot accumulate arrays of different dimensionality")
            );
        }
        i = 0;
        while i < ndims {
            if (*astate).dims[(i + 1) as usize] != *dims.add(i as usize)
                || (*astate).lbs[(i + 1) as usize] != *lbs.add(i as usize)
            {
                ereport!(
                    ERROR,
                    errmsg!("cannot accumulate arrays of different dimensionality")
                );
            }
            i += 1;
        }

        /* Enlarge data space if needed */
        if (*astate).nbytes + ndatabytes > (*astate).abytes {
            (*astate).abytes = Max((*astate).abytes * 2, (*astate).nbytes + ndatabytes);
            (*astate).data =
                repalloc((*astate).data as *mut c_void, (*astate).abytes as usize) as *mut c_char;
        }
    }

    /*
     * Copy the data portion of the sub-array.  Note we assume that the
     * advertised data length of the sub-array is properly aligned.  We do not
     * have to worry about detoasting elements since whatever's in the
     * sub-array should be OK already.
     */
    memcpy(
        (*astate).data.add((*astate).nbytes as usize) as *mut c_void,
        data as *const c_void,
        ndatabytes as usize,
    );
    (*astate).nbytes += ndatabytes;

    /* Deal with null bitmap if needed */
    if !(*astate).nullbitmap.is_null() || ARR_HASNULL(arg) {
        let newnitems = (*astate).nitems + nitems;

        if (*astate).nullbitmap.is_null() {
            /*
             * First input with nulls; we must retrospectively handle any
             * previous inputs by marking all their items non-null.
             */
            (*astate).aitems = pg_nextpower2_32(Max(256, newnitems + 1) as uint32) as c_int;
            (*astate).nullbitmap = palloc((((*astate).aitems + 7) / 8) as usize) as *mut bits8;
            array_bitmap_copy((*astate).nullbitmap, 0, null(), 0, (*astate).nitems);
        } else if newnitems > (*astate).aitems {
            (*astate).aitems = Max((*astate).aitems * 2, newnitems);
            (*astate).nullbitmap = repalloc(
                (*astate).nullbitmap as *mut c_void,
                (((*astate).aitems + 7) / 8) as usize,
            ) as *mut bits8;
        }
        array_bitmap_copy(
            (*astate).nullbitmap,
            (*astate).nitems,
            ARR_NULLBITMAP(arg),
            0,
            nitems,
        );
    }

    (*astate).nitems += nitems;
    (*astate).dims[0] += 1;

    MemoryContextSwitchTo(oldcontext);

    /* Release detoasted copy if any */
    if arg as Pointer != DatumGetPointer(dvalue) {
        pfree(arg as *mut c_void);
    }

    astate
}

/*
 * makeArrayResultArr - produce N+1-D final result of accumArrayResultArr
 *
 *	astate is working state (must not be NULL)
 *	rcontext is where to construct result
 *	release is true if okay to release working state
 */
pub unsafe fn makeArrayResultArr(
    astate: *mut ArrayBuildStateArr,
    rcontext: MemoryContext,
    release: bool,
) -> Datum {
    let result: *mut ArrayType;
    let oldcontext: MemoryContext;

    /* Build the final array result in rcontext */
    oldcontext = MemoryContextSwitchTo(rcontext);

    if (*astate).ndims == 0 {
        /* No inputs, return empty array */
        result = construct_empty_array((*astate).element_type);
    } else {
        let dataoffset: c_int;
        let mut nbytes: c_int;

        /* Check for overflow of the array dimensions */
        ArrayGetNItems((*astate).ndims, (*astate).dims.as_mut_ptr());
        ArrayCheckBounds(
            (*astate).ndims,
            (*astate).dims.as_mut_ptr(),
            (*astate).lbs.as_mut_ptr(),
        );

        /* Compute required space */
        nbytes = (*astate).nbytes;
        if !(*astate).nullbitmap.is_null() {
            dataoffset = ARR_OVERHEAD_WITHNULLS((*astate).ndims, (*astate).nitems) as c_int;
            nbytes += dataoffset;
        } else {
            dataoffset = 0;
            nbytes += ARR_OVERHEAD_NONULLS((*astate).ndims) as c_int;
        }

        result = palloc0(nbytes as usize) as *mut ArrayType;
        SET_VARSIZE(result as *mut c_char, nbytes);
        (*result).ndim = (*astate).ndims;
        (*result).dataoffset = dataoffset;
        (*result).elemtype = (*astate).element_type;

        memcpy(
            ARR_DIMS(result) as *mut c_void,
            (*astate).dims.as_ptr() as *const c_void,
            (*astate).ndims as usize * core::mem::size_of::<c_int>(),
        );
        memcpy(
            ARR_LBOUND(result) as *mut c_void,
            (*astate).lbs.as_ptr() as *const c_void,
            (*astate).ndims as usize * core::mem::size_of::<c_int>(),
        );
        memcpy(
            ARR_DATA_PTR(result) as *mut c_void,
            (*astate).data as *const c_void,
            (*astate).nbytes as usize,
        );

        if !(*astate).nullbitmap.is_null() {
            array_bitmap_copy(
                ARR_NULLBITMAP(result),
                0,
                (*astate).nullbitmap,
                0,
                (*astate).nitems,
            );
        }
    }

    MemoryContextSwitchTo(oldcontext);

    /* Clean up all the junk */
    if release {
        Assert!((*astate).private_cxt);
        MemoryContextDelete((*astate).mcontext);
    }

    PointerGetDatum(result as *const c_void)
}

/*
 * The following three functions provide essentially the same API as
 * initArrayResult/accumArrayResult/makeArrayResult, but can accept either
 * scalar or array inputs, invoking the appropriate set of functions above.
 */

/*
 * initArrayResultAny - initialize an empty ArrayBuildStateAny
 *
 *	input_type is the input datatype (either element or array type)
 *	rcontext is where to keep working state
 *	subcontext is a flag determining whether to use a separate memory context
 */
pub unsafe fn initArrayResultAny(
    input_type: Oid,
    rcontext: MemoryContext,
    subcontext: bool,
) -> *mut ArrayBuildStateAny {
    let astate: *mut ArrayBuildStateAny;

    /*
     * int2vector and oidvector will satisfy both get_element_type and
     * get_array_type.  We prefer to treat them as scalars, to be consistent
     * with get_promoted_array_type.  Hence, check get_array_type not
     * get_element_type.
     */
    if !OidIsValid(get_array_type(input_type)) {
        /* Array case */
        let arraystate: *mut ArrayBuildStateArr;

        arraystate = initArrayResultArr(input_type, InvalidOid, rcontext, subcontext);
        astate = MemoryContextAlloc((*arraystate).mcontext, core::mem::size_of::<ArrayBuildStateAny>())
            as *mut ArrayBuildStateAny;
        (*astate).scalarstate = null_mut();
        (*astate).arraystate = arraystate;
    } else {
        /* Scalar case */
        let scalarstate: *mut ArrayBuildState;

        scalarstate = initArrayResult(input_type, rcontext, subcontext);
        astate = MemoryContextAlloc((*scalarstate).mcontext, core::mem::size_of::<ArrayBuildStateAny>())
            as *mut ArrayBuildStateAny;
        (*astate).scalarstate = scalarstate;
        (*astate).arraystate = null_mut();
    }

    astate
}

/*
 * accumArrayResultAny - accumulate one (more) input for an array result
 *
 *	astate is working state (can be NULL on first call)
 *	dvalue/disnull represent the new input to append to the array
 *	input_type is the input datatype (either element or array type)
 *	rcontext is where to keep working state
 */
pub unsafe fn accumArrayResultAny(
    mut astate: *mut ArrayBuildStateAny,
    dvalue: Datum,
    disnull: bool,
    input_type: Oid,
    rcontext: MemoryContext,
) -> *mut ArrayBuildStateAny {
    if astate.is_null() {
        astate = initArrayResultAny(input_type, rcontext, true);
    }

    if !(*astate).scalarstate.is_null() {
        accumArrayResult((*astate).scalarstate, dvalue, disnull, input_type, rcontext);
    } else {
        accumArrayResultArr((*astate).arraystate, dvalue, disnull, input_type, rcontext);
    }

    astate
}

/*
 * makeArrayResultAny - produce final result of accumArrayResultAny
 *
 *	astate is working state (must not be NULL)
 *	rcontext is where to construct result
 *	release is true if okay to release working state
 */
pub unsafe fn makeArrayResultAny(
    astate: *mut ArrayBuildStateAny,
    rcontext: MemoryContext,
    release: bool,
) -> Datum {
    let result: Datum;

    if !(*astate).scalarstate.is_null() {
        /* Must use makeMdArrayResult to support "release" parameter */
        let ndims: c_int;
        let mut dims: [c_int; 1] = [0; 1];
        let mut lbs: [c_int; 1] = [0; 1];

        /* If no elements were presented, we want to create an empty array */
        ndims = if (*(*astate).scalarstate).nelems > 0 {
            1
        } else {
            0
        };
        dims[0] = (*(*astate).scalarstate).nelems;
        lbs[0] = 1;

        result = makeMdArrayResult(
            (*astate).scalarstate,
            ndims,
            dims.as_mut_ptr(),
            lbs.as_mut_ptr(),
            rcontext,
            release,
        );
    } else {
        result = makeArrayResultArr((*astate).arraystate, rcontext, release);
    }
    result
}

pub unsafe fn array_larger(fcinfo: FunctionCallInfo) -> Datum {
    if array_cmp(fcinfo) > 0 {
        PG_RETURN_DATUM!(PG_GETARG_DATUM!(fcinfo, 0))
    } else {
        PG_RETURN_DATUM!(PG_GETARG_DATUM!(fcinfo, 1))
    }
}

pub unsafe fn array_smaller(fcinfo: FunctionCallInfo) -> Datum {
    if array_cmp(fcinfo) < 0 {
        PG_RETURN_DATUM!(PG_GETARG_DATUM!(fcinfo, 0))
    } else {
        PG_RETURN_DATUM!(PG_GETARG_DATUM!(fcinfo, 1))
    }
}

#[repr(C)]
struct generate_subscripts_fctx {
    lower: int32,
    upper: int32,
    reverse: bool,
}

/*
 * generate_subscripts(array anyarray, dim int [, reverse bool])
 *		Returns all subscripts of the array for any dimension
 */
pub unsafe fn generate_subscripts(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let oldcontext: MemoryContext;
    let fctx: *mut generate_subscripts_fctx;

    /* stuff done only on the first call of the function */
    if SRF_IS_FIRSTCALL!(fcinfo) {
        let v = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);
        let reqdim = PG_GETARG_INT32!(fcinfo, 1);
        let lb: *mut c_int;
        let dimv: *mut c_int;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT!(fcinfo);

        /* Sanity check: does it look like an array at all? */
        if AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM {
            SRF_RETURN_DONE!(funcctx);
        }

        /* Sanity check: was the requested dim valid */
        if reqdim <= 0 || reqdim > AARR_NDIM(v) {
            SRF_RETURN_DONE!(funcctx);
        }

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);
        let fctx_l =
            palloc(core::mem::size_of::<generate_subscripts_fctx>()) as *mut generate_subscripts_fctx;

        lb = AARR_LBOUND(v);
        dimv = AARR_DIMS(v);

        (*fctx_l).lower = *lb.add((reqdim - 1) as usize);
        (*fctx_l).upper = *dimv.add((reqdim - 1) as usize) + *lb.add((reqdim - 1) as usize) - 1;
        (*fctx_l).reverse = if (PG_NARGS!(fcinfo) as c_int) < 3 {
            false
        } else {
            PG_GETARG_BOOL!(fcinfo, 2)
        };

        (*funcctx).user_fctx = fctx_l as *mut c_void;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP!(fcinfo);

    fctx = (*funcctx).user_fctx as *mut generate_subscripts_fctx;

    if (*fctx).lower <= (*fctx).upper {
        if !(*fctx).reverse {
            let v = (*fctx).lower;
            (*fctx).lower += 1;
            SRF_RETURN_NEXT!(funcctx, Int32GetDatum(v));
        } else {
            let v = (*fctx).upper;
            (*fctx).upper -= 1;
            SRF_RETURN_NEXT!(funcctx, Int32GetDatum(v));
        }
    } else {
        /* done when there are no more elements left */
        SRF_RETURN_DONE!(funcctx);
    }
}

/*
 * generate_subscripts_nodir
 *		Implements the 2-argument version of generate_subscripts
 */
pub unsafe fn generate_subscripts_nodir(fcinfo: FunctionCallInfo) -> Datum {
    /* just call the other one -- it can handle both cases */
    generate_subscripts(fcinfo)
}

/*
 * array_fill_with_lower_bounds
 *		Create and fill array with defined lower bounds.
 */
pub unsafe fn array_fill_with_lower_bounds(fcinfo: FunctionCallInfo) -> Datum {
    let dims: *mut ArrayType;
    let lbs: *mut ArrayType;
    let result: *mut ArrayType;
    let elmtype: Oid;
    let value: Datum;
    let isnull: bool;

    if PG_ARGISNULL!(fcinfo, 1) || PG_ARGISNULL!(fcinfo, 2) {
        ereport!(
            ERROR,
            errmsg!("dimension array or low bound array cannot be null")
        );
    }

    dims = PG_GETARG_ARRAYTYPE_P!(fcinfo, 1);
    lbs = PG_GETARG_ARRAYTYPE_P!(fcinfo, 2);

    if !PG_ARGISNULL!(fcinfo, 0) {
        value = PG_GETARG_DATUM!(fcinfo, 0);
        isnull = false;
    } else {
        value = 0;
        isnull = true;
    }

    elmtype = get_fn_expr_argtype((*fcinfo).flinfo, 0);
    if !OidIsValid(elmtype) {
        elog!(ERROR, "could not determine data type of input");
    }

    result = array_fill_internal(dims, lbs, value, isnull, elmtype, fcinfo);
    PG_RETURN_ARRAYTYPE_P!(result)
}

/*
 * array_fill
 *		Create and fill array with default lower bounds.
 */
pub unsafe fn array_fill(fcinfo: FunctionCallInfo) -> Datum {
    let dims: *mut ArrayType;
    let result: *mut ArrayType;
    let elmtype: Oid;
    let value: Datum;
    let isnull: bool;

    if PG_ARGISNULL!(fcinfo, 1) {
        ereport!(
            ERROR,
            errmsg!("dimension array or low bound array cannot be null")
        );
    }

    dims = PG_GETARG_ARRAYTYPE_P!(fcinfo, 1);

    if !PG_ARGISNULL!(fcinfo, 0) {
        value = PG_GETARG_DATUM!(fcinfo, 0);
        isnull = false;
    } else {
        value = 0;
        isnull = true;
    }

    elmtype = get_fn_expr_argtype((*fcinfo).flinfo, 0);
    if !OidIsValid(elmtype) {
        elog!(ERROR, "could not determine data type of input");
    }

    result = array_fill_internal(dims, null_mut(), value, isnull, elmtype, fcinfo);
    PG_RETURN_ARRAYTYPE_P!(result)
}

unsafe fn create_array_envelope(
    ndims: c_int,
    dimv: *mut c_int,
    lbsv: *mut c_int,
    nbytes: c_int,
    elmtype: Oid,
    dataoffset: c_int,
) -> *mut ArrayType {
    let result: *mut ArrayType;

    result = palloc0(nbytes as usize) as *mut ArrayType;
    SET_VARSIZE(result as *mut c_char, nbytes);
    (*result).ndim = ndims;
    (*result).dataoffset = dataoffset;
    (*result).elemtype = elmtype;
    memcpy(
        ARR_DIMS(result) as *mut c_void,
        dimv as *const c_void,
        ndims as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        ARR_LBOUND(result) as *mut c_void,
        lbsv as *const c_void,
        ndims as usize * core::mem::size_of::<c_int>(),
    );

    result
}

unsafe fn array_fill_internal(
    dims: *mut ArrayType,
    lbs: *mut ArrayType,
    mut value: Datum,
    isnull: bool,
    elmtype: Oid,
    fcinfo: FunctionCallInfo,
) -> *mut ArrayType {
    let result: *mut ArrayType;
    let dimv: *mut c_int;
    let lbsv: *mut c_int;
    let ndims: c_int;
    let nitems: c_int;
    let mut deflbs: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let elmlen: int16;
    let elmbyval: bool;
    let elmalign: c_char;
    let mut my_extra: *mut ArrayMetaState;

    /*
     * Params checks
     */
    if ARR_NDIM(dims) > 1 {
        ereport!(ERROR, errmsg!("wrong number of array subscripts"));
    }

    if array_contains_nulls(dims) {
        ereport!(ERROR, errmsg!("dimension values cannot be null"));
    }

    dimv = ARR_DATA_PTR(dims) as *mut c_int;
    ndims = if ARR_NDIM(dims) > 0 {
        *ARR_DIMS(dims).add(0)
    } else {
        0
    };

    if ndims < 0 {
        /* we do allow zero-dimension arrays */
        ereport!(ERROR, errmsg!("invalid number of dimensions: {}", ndims));
    }
    if ndims > MAXDIM {
        ereport!(
            ERROR,
            errmsg!(
                "number of array dimensions ({}) exceeds the maximum allowed ({})",
                ndims,
                MAXDIM
            )
        );
    }

    if !lbs.is_null() {
        if ARR_NDIM(lbs) > 1 {
            ereport!(ERROR, errmsg!("wrong number of array subscripts"));
        }

        if array_contains_nulls(lbs) {
            ereport!(ERROR, errmsg!("dimension values cannot be null"));
        }

        if ndims != (if ARR_NDIM(lbs) > 0 { *ARR_DIMS(lbs).add(0) } else { 0 }) {
            ereport!(ERROR, errmsg!("wrong number of array subscripts"));
        }

        lbsv = ARR_DATA_PTR(lbs) as *mut c_int;
    } else {
        let mut i: c_int = 0;

        while i < MAXDIM {
            deflbs[i as usize] = 1;
            i += 1;
        }

        lbsv = deflbs.as_mut_ptr();
    }

    /* This checks for overflow of the array dimensions */
    nitems = ArrayGetNItems(ndims, dimv);
    ArrayCheckBounds(ndims, dimv, lbsv);

    /* fast track for empty array */
    if nitems <= 0 {
        return construct_empty_array(elmtype);
    }

    /*
     * We arrange to look up info about element type only once per series of
     * calls, assuming the element type doesn't change underneath us.
     */
    my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
    if my_extra.is_null() {
        (*(*fcinfo).flinfo).fn_extra = MemoryContextAlloc(
            (*(*fcinfo).flinfo).fn_mcxt,
            core::mem::size_of::<ArrayMetaState>(),
        );
        my_extra = (*(*fcinfo).flinfo).fn_extra as *mut ArrayMetaState;
        (*my_extra).element_type = InvalidOid;
    }

    if (*my_extra).element_type != elmtype {
        /* Get info about element type */
        get_typlenbyvalalign(
            elmtype,
            &raw mut (*my_extra).typlen,
            &raw mut (*my_extra).typbyval,
            &raw mut (*my_extra).typalign,
        );
        (*my_extra).element_type = elmtype;
    }

    elmlen = (*my_extra).typlen;
    elmbyval = (*my_extra).typbyval;
    elmalign = (*my_extra).typalign;

    /* compute required space */
    if !isnull {
        let mut i: c_int;
        let mut p: *mut c_char;
        let mut nbytes: c_int;
        let mut totbytes: c_int;

        /* make sure data is not toasted */
        if elmlen == -1 {
            value = PointerGetDatum(crate::PG_DETOAST_DATUM!(value) as *const c_void);
        }

        nbytes = att_addlength_datum(0, elmlen as c_int, value) as c_int;
        nbytes = att_align_nominal(nbytes as usize, elmalign) as c_int;
        Assert!(nbytes > 0);

        totbytes = nbytes * nitems;

        /* check for overflow of multiplication or total request */
        if totbytes / nbytes != nitems || !AllocSizeIsValid(totbytes as usize) {
            ereport!(
                ERROR,
                errmsg!(
                    "array size exceeds the maximum allowed ({})",
                    MaxAllocSize as c_int
                )
            );
        }

        /*
         * This addition can't overflow, but it might cause us to go past
         * MaxAllocSize.  We leave it to palloc to complain in that case.
         */
        totbytes += ARR_OVERHEAD_NONULLS(ndims) as c_int;

        result = create_array_envelope(ndims, dimv, lbsv, totbytes, elmtype, 0);

        p = ARR_DATA_PTR(result);
        i = 0;
        while i < nitems {
            p = p.add(ArrayCastAndSet(value, elmlen as c_int, elmbyval, elmalign, p) as usize);
            i += 1;
        }
    } else {
        let nbytes: c_int;
        let dataoffset: c_int;

        dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nitems) as c_int;
        nbytes = dataoffset;

        result = create_array_envelope(ndims, dimv, lbsv, nbytes, elmtype, dataoffset);

        /* create_array_envelope already zeroed the bitmap, so we're done */
    }

    result
}

/*
 * UNNEST
 */
pub unsafe fn array_unnest(fcinfo: FunctionCallInfo) -> Datum {
    #[repr(C)]
    struct array_unnest_fctx {
        iter: array_iter,
        nextelem: c_int,
        numelems: c_int,
        elmlen: int16,
        elmbyval: bool,
        elmalign: c_char,
    }

    let mut funcctx: *mut FuncCallContext;
    let fctx: *mut array_unnest_fctx;
    let oldcontext: MemoryContext;

    /* stuff done only on the first call of the function */
    if SRF_IS_FIRSTCALL!(fcinfo) {
        let arr: *mut AnyArrayType;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT!(fcinfo);

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        /*
         * Get the array value and detoast if needed.  We can't do this
         * earlier because if we have to detoast, we want the detoasted copy
         * to be in multi_call_memory_ctx, so it will go away when we're done
         * and not before.  (If no detoast happens, we assume the originally
         * passed array will stick around till then.)
         */
        arr = PG_GETARG_ANY_ARRAY_P!(fcinfo, 0);

        /* allocate memory for user context */
        let fctx_l = palloc(core::mem::size_of::<array_unnest_fctx>()) as *mut array_unnest_fctx;

        /* initialize state */
        array_iter_setup(&raw mut (*fctx_l).iter, arr);
        (*fctx_l).nextelem = 0;
        (*fctx_l).numelems = ArrayGetNItems(AARR_NDIM(arr), AARR_DIMS(arr));

        if VARATT_IS_EXPANDED_HEADER(arr as *const c_void) {
            /* we can just grab the type data from expanded array */
            let xpn = xpn_ptr(arr);
            (*fctx_l).elmlen = (*xpn).typlen;
            (*fctx_l).elmbyval = (*xpn).typbyval;
            (*fctx_l).elmalign = (*xpn).typalign;
        } else {
            get_typlenbyvalalign(
                AARR_ELEMTYPE(arr),
                &raw mut (*fctx_l).elmlen,
                &raw mut (*fctx_l).elmbyval,
                &raw mut (*fctx_l).elmalign,
            );
        }

        (*funcctx).user_fctx = fctx_l as *mut c_void;
        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP!(fcinfo);
    fctx = (*funcctx).user_fctx as *mut array_unnest_fctx;

    if (*fctx).nextelem < (*fctx).numelems {
        let offset = (*fctx).nextelem;
        (*fctx).nextelem += 1;
        let elem: Datum;

        elem = array_iter_next(
            &raw mut (*fctx).iter,
            &raw mut (*fcinfo).isnull,
            offset,
            (*fctx).elmlen as c_int,
            (*fctx).elmbyval,
            (*fctx).elmalign,
        );

        SRF_RETURN_NEXT!(funcctx, elem);
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE!(funcctx);
    }
}

/*
 * Planner support function for array_unnest(anyarray)
 *
 * Note: this is now also used for information_schema._pg_expandarray(),
 * which is simply a wrapper around array_unnest().
 */
pub unsafe fn array_unnest_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let mut ret: *mut Node = null_mut();

    if IsA!(rawreq, T_SupportRequestRows) {
        /* Try to estimate the number of rows returned */
        let req = rawreq as *mut SupportRequestRows;

        if is_funcclause((*req).node) {
            /* be paranoid */
            let args: *mut List = (*((*req).node as *mut FuncExpr)).args;
            let arg1: *mut Node;

            /* We can use estimated argument values here */
            arg1 = estimate_expression_value((*req).root as *mut crate::optimizer::optimizer::PlannerInfo, linitial(args) as *mut Node);

            (*req).rows = estimate_array_length((*req).root as *mut c_void, arg1);
            ret = req as *mut Node;
        }
    }

    PG_RETURN_POINTER!(ret as *const c_void)
}

/*
 * array_replace/array_remove support
 *
 * Find all array entries matching (not distinct from) search/search_isnull,
 * and delete them if remove is true, else replace them with
 * replace/replace_isnull.  Comparisons are done using the specified
 * collation.  fcinfo is passed only for caching purposes.
 */
unsafe fn array_replace_internal(
    array: *mut ArrayType,
    mut search: Datum,
    search_isnull: bool,
    mut replace: Datum,
    replace_isnull: bool,
    remove: bool,
    collation: Oid,
    fcinfo: FunctionCallInfo,
) -> *mut ArrayType {
    LOCAL_FCINFO!(locfcinfo, 2);
    let result: *mut ArrayType;
    let element_type: Oid;
    let values: *mut Datum;
    let nulls: *mut bool;
    let dim: *mut c_int;
    let ndim: c_int;
    let nitems: c_int;
    let mut nresult: c_int;
    let mut i: c_int;
    let mut nbytes: int32 = 0;
    let dataoffset: int32;
    let mut hasnulls: bool;
    let typlen: c_int;
    let typbyval: bool;
    let typalign: c_char;
    let mut arraydataptr: *mut c_char;
    let mut bitmap: *mut bits8;
    let mut bitmask: c_int;
    let mut changed: bool = false;
    let mut typentry: *mut TypeCacheEntry;

    element_type = ARR_ELEMTYPE(array);
    ndim = ARR_NDIM(array);
    dim = ARR_DIMS(array);
    nitems = ArrayGetNItems(ndim, dim);

    /* Return input array unmodified if it is empty */
    if nitems <= 0 {
        return array;
    }

    /*
     * We can't remove elements from multi-dimensional arrays, since the
     * result might not be rectangular.
     */
    if remove && ndim > 1 {
        ereport!(
            ERROR,
            errmsg!("removing elements from multidimensional arrays is not supported")
        );
    }

    /*
     * We arrange to look up the equality function only once per series of
     * calls, assuming the element type doesn't change underneath us.
     */
    typentry = (*(*fcinfo).flinfo).fn_extra as *mut TypeCacheEntry;
    if typentry.is_null() || (*typentry).type_id != element_type {
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
        (*(*fcinfo).flinfo).fn_extra = typentry as *mut c_void;
    }
    typlen = (*typentry).typlen as c_int;
    typbyval = (*typentry).typbyval;
    typalign = (*typentry).typalign;

    /*
     * Detoast values if they are toasted.  The replacement value must be
     * detoasted for insertion into the result array, while detoasting the
     * search value only once saves cycles.
     */
    if typlen == -1 {
        if !search_isnull {
            search = PointerGetDatum(crate::PG_DETOAST_DATUM!(search) as *const c_void);
        }
        if !replace_isnull {
            replace = PointerGetDatum(crate::PG_DETOAST_DATUM!(replace) as *const c_void);
        }
    }

    /* Prepare to apply the comparison operator */
    InitFunctionCallInfoData!(
        locfcinfo,
        &raw mut (*typentry).eq_opr_finfo,
        2,
        collation,
        null_mut(),
        null_mut()
    );

    /* Allocate temporary arrays for new values */
    values = palloc(nitems as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    nulls = palloc(nitems as usize * core::mem::size_of::<bool>()) as *mut bool;

    /* Loop over source data */
    arraydataptr = ARR_DATA_PTR(array);
    bitmap = ARR_NULLBITMAP(array);
    bitmask = 1;
    hasnulls = false;
    nresult = 0;

    i = 0;
    while i < nitems {
        let elt: Datum;
        let mut isNull: bool;
        let oprresult: bool;
        let mut skip: bool = false;

        /* Get source element, checking for NULL */
        if !bitmap.is_null() && (*bitmap & bitmask as bits8) == 0 {
            isNull = true;
            /* If searching for NULL, we have a match */
            if search_isnull {
                if remove {
                    skip = true;
                    changed = true;
                } else if !replace_isnull {
                    *values.add(nresult as usize) = replace;
                    isNull = false;
                    changed = true;
                }
            }
        } else {
            isNull = false;
            elt = fetch_att(arraydataptr as *const c_void, typbyval, typlen);
            arraydataptr = att_addlength_datum(arraydataptr as usize, typlen, elt) as *mut c_char;
            arraydataptr = att_align_nominal(arraydataptr as usize, typalign) as *mut c_char;

            if search_isnull {
                /* no match possible, keep element */
                *values.add(nresult as usize) = elt;
            } else {
                /*
                 * Apply the operator to the element pair; treat NULL as false
                 */
                (*(*locfcinfo).args.as_mut_ptr().add(0)).value = elt;
                (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
                (*(*locfcinfo).args.as_mut_ptr().add(1)).value = search;
                (*(*locfcinfo).args.as_mut_ptr().add(1)).isnull = false;
                (*locfcinfo).isnull = false;
                oprresult = DatumGetBool(FunctionCallInvoke!(locfcinfo));
                if (*locfcinfo).isnull || !oprresult {
                    /* no match, keep element */
                    *values.add(nresult as usize) = elt;
                } else {
                    /* match, so replace or delete */
                    changed = true;
                    if remove {
                        skip = true;
                    } else {
                        *values.add(nresult as usize) = replace;
                        isNull = replace_isnull;
                    }
                }
            }
        }

        if !skip {
            *nulls.add(nresult as usize) = isNull;
            if isNull {
                hasnulls = true;
            } else {
                /* Update total result size */
                nbytes = att_addlength_datum(nbytes as usize, typlen, *values.add(nresult as usize)) as int32;
                nbytes = att_align_nominal(nbytes as usize, typalign) as int32;
                /* check for overflow of total request */
                if !AllocSizeIsValid(nbytes as usize) {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "array size exceeds the maximum allowed ({})",
                            MaxAllocSize as c_int
                        )
                    );
                }
            }
            nresult += 1;
        }

        /* advance bitmap pointer if any */
        if !bitmap.is_null() {
            bitmask <<= 1;
            if bitmask == 0x100 {
                bitmap = bitmap.add(1);
                bitmask = 1;
            }
        }
        i += 1;
    }

    /*
     * If not changed just return the original array
     */
    if !changed {
        pfree(values as *mut c_void);
        pfree(nulls as *mut c_void);
        return array;
    }

    /* If all elements were removed return an empty array */
    if nresult == 0 {
        pfree(values as *mut c_void);
        pfree(nulls as *mut c_void);
        return construct_empty_array(element_type);
    }

    /* Allocate and initialize the result array */
    if hasnulls {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndim, nresult) as int32;
        nbytes += dataoffset;
    } else {
        dataoffset = 0; /* marker for no null bitmap */
        nbytes += ARR_OVERHEAD_NONULLS(ndim) as int32;
    }
    result = palloc0(nbytes as usize) as *mut ArrayType;
    SET_VARSIZE(result as *mut c_char, nbytes);
    (*result).ndim = ndim;
    (*result).dataoffset = dataoffset;
    (*result).elemtype = element_type;
    memcpy(
        ARR_DIMS(result) as *mut c_void,
        ARR_DIMS(array) as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        ARR_LBOUND(result) as *mut c_void,
        ARR_LBOUND(array) as *const c_void,
        ndim as usize * core::mem::size_of::<c_int>(),
    );

    if remove {
        /* Adjust the result length */
        *ARR_DIMS(result).add(0) = nresult;
    }

    /* Insert data into result array */
    CopyArrayEls(
        result, values, nulls, nresult, typlen, typbyval, typalign, false,
    );

    pfree(values as *mut c_void);
    pfree(nulls as *mut c_void);

    result
}

/*
 * Remove any occurrences of an element from an array
 *
 * If used on a multi-dimensional array this will raise an error.
 */
pub unsafe fn array_remove(fcinfo: FunctionCallInfo) -> Datum {
    let mut array: *mut ArrayType;
    let search = PG_GETARG_DATUM!(fcinfo, 1);
    let search_isnull = PG_ARGISNULL!(fcinfo, 1);

    if PG_ARGISNULL!(fcinfo, 0) {
        PG_RETURN_NULL!(fcinfo);
    }
    array = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);

    array = array_replace_internal(
        array,
        search,
        search_isnull,
        0 as Datum,
        true,
        true,
        PG_GET_COLLATION!(fcinfo),
        fcinfo,
    );
    PG_RETURN_ARRAYTYPE_P!(array)
}

/*
 * Replace any occurrences of an element in an array
 */
pub unsafe fn array_replace(fcinfo: FunctionCallInfo) -> Datum {
    let mut array: *mut ArrayType;
    let search = PG_GETARG_DATUM!(fcinfo, 1);
    let search_isnull = PG_ARGISNULL!(fcinfo, 1);
    let replace = PG_GETARG_DATUM!(fcinfo, 2);
    let replace_isnull = PG_ARGISNULL!(fcinfo, 2);

    if PG_ARGISNULL!(fcinfo, 0) {
        PG_RETURN_NULL!(fcinfo);
    }
    array = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);

    array = array_replace_internal(
        array,
        search,
        search_isnull,
        replace,
        replace_isnull,
        false,
        PG_GET_COLLATION!(fcinfo),
        fcinfo,
    );
    PG_RETURN_ARRAYTYPE_P!(array)
}

/*
 * Implements width_bucket(anyelement, anyarray).
 *
 * 'thresholds' is an array containing lower bound values for each bucket;
 * these must be sorted from smallest to largest, or bogus results will be
 * produced.  If N thresholds are supplied, the output is from 0 to N:
 * 0 is for inputs < first threshold, N is for inputs >= last threshold.
 */
pub unsafe fn width_bucket_array(fcinfo: FunctionCallInfo) -> Datum {
    let operand = PG_GETARG_DATUM!(fcinfo, 0);
    let thresholds = PG_GETARG_ARRAYTYPE_P!(fcinfo, 1);
    let collation = PG_GET_COLLATION!(fcinfo);
    let element_type = ARR_ELEMTYPE(thresholds);
    let result: c_int;

    /* Check input */
    if ARR_NDIM(thresholds) > 1 {
        ereport!(ERROR, errmsg!("thresholds must be one-dimensional array"));
    }

    if array_contains_nulls(thresholds) {
        ereport!(ERROR, errmsg!("thresholds array must not contain NULLs"));
    }

    /* We have a dedicated implementation for float8 data */
    if element_type == FLOAT8OID {
        result = width_bucket_array_float8(operand, thresholds);
    } else {
        let mut typentry: *mut TypeCacheEntry;

        /* Cache information about the input type */
        typentry = (*(*fcinfo).flinfo).fn_extra as *mut TypeCacheEntry;
        if typentry.is_null() || (*typentry).type_id != element_type {
            typentry = lookup_type_cache(element_type, TYPECACHE_CMP_PROC_FINFO);
            if !OidIsValid((*typentry).cmp_proc_finfo.fn_oid) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not identify a comparison function for type {}",
                        std::ffi::CStr::from_ptr(format_type_be(element_type)).to_string_lossy()
                    )
                );
            }
            (*(*fcinfo).flinfo).fn_extra = typentry as *mut c_void;
        }

        /*
         * We have separate implementation paths for fixed- and variable-width
         * types, since indexing the array is a lot cheaper in the first case.
         */
        if (*typentry).typlen > 0 {
            result = width_bucket_array_fixed(operand, thresholds, collation, typentry);
        } else {
            result = width_bucket_array_variable(operand, thresholds, collation, typentry);
        }
    }

    /* Avoid leaking memory when handed toasted input. */
    PG_FREE_IF_COPY!(fcinfo, thresholds, 1);

    PG_RETURN_INT32!(result)
}

/*
 * width_bucket_array for float8 data.
 */
unsafe fn width_bucket_array_float8(operand: Datum, thresholds: *mut ArrayType) -> c_int {
    let op: float8 = DatumGetFloat8(operand);
    let thresholds_data: *mut float8;
    let mut left: c_int;
    let mut right: c_int;

    /*
     * Since we know the array contains no NULLs, we can just index it
     * directly.
     */
    thresholds_data = ARR_DATA_PTR(thresholds) as *mut float8;

    left = 0;
    right = ArrayGetNItems(ARR_NDIM(thresholds), ARR_DIMS(thresholds));

    /*
     * If the probe value is a NaN, it's greater than or equal to all possible
     * threshold values (including other NaNs), so we need not search.  Note
     * that this would give the same result as searching even if the array
     * contains multiple NaNs (as long as they're correctly sorted), since the
     * loop logic will find the rightmost of multiple equal threshold values.
     */
    if isnan(op) != 0 {
        return right;
    }

    /* Find the bucket */
    while left < right {
        let mid = (left + right) / 2;

        if isnan(*thresholds_data.add(mid as usize)) != 0 || op < *thresholds_data.add(mid as usize)
        {
            right = mid;
        } else {
            left = mid + 1;
        }
    }

    left
}

/*
 * width_bucket_array for generic fixed-width data types.
 */
unsafe fn width_bucket_array_fixed(
    operand: Datum,
    thresholds: *mut ArrayType,
    collation: Oid,
    typentry: *mut TypeCacheEntry,
) -> c_int {
    LOCAL_FCINFO!(locfcinfo, 2);
    let thresholds_data: *mut c_char;
    let typlen: c_int = (*typentry).typlen as c_int;
    let typbyval: bool = (*typentry).typbyval;
    let mut left: c_int;
    let mut right: c_int;

    /*
     * Since we know the array contains no NULLs, we can just index it
     * directly.
     */
    thresholds_data = ARR_DATA_PTR(thresholds);

    InitFunctionCallInfoData!(
        locfcinfo,
        &raw mut (*typentry).cmp_proc_finfo,
        2,
        collation,
        null_mut(),
        null_mut()
    );

    /* Find the bucket */
    left = 0;
    right = ArrayGetNItems(ARR_NDIM(thresholds), ARR_DIMS(thresholds));
    while left < right {
        let mid = (left + right) / 2;
        let ptr: *mut c_char;
        let cmpresult: int32;

        ptr = thresholds_data.add((mid * typlen) as usize);

        (*(*locfcinfo).args.as_mut_ptr().add(0)).value = operand;
        (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
        (*(*locfcinfo).args.as_mut_ptr().add(1)).value = fetch_att(ptr as *const c_void, typbyval, typlen);
        (*(*locfcinfo).args.as_mut_ptr().add(1)).isnull = false;

        cmpresult = DatumGetInt32(FunctionCallInvoke!(locfcinfo));

        /* We don't expect comparison support functions to return null */
        Assert!(!(*locfcinfo).isnull);

        if cmpresult < 0 {
            right = mid;
        } else {
            left = mid + 1;
        }
    }

    left
}

/*
 * width_bucket_array for generic variable-width data types.
 */
unsafe fn width_bucket_array_variable(
    operand: Datum,
    thresholds: *mut ArrayType,
    collation: Oid,
    typentry: *mut TypeCacheEntry,
) -> c_int {
    LOCAL_FCINFO!(locfcinfo, 2);
    let mut thresholds_data: *mut c_char;
    let typlen: c_int = (*typentry).typlen as c_int;
    let typbyval: bool = (*typentry).typbyval;
    let typalign: c_char = (*typentry).typalign;
    let mut left: c_int;
    let mut right: c_int;

    thresholds_data = ARR_DATA_PTR(thresholds);

    InitFunctionCallInfoData!(
        locfcinfo,
        &raw mut (*typentry).cmp_proc_finfo,
        2,
        collation,
        null_mut(),
        null_mut()
    );

    /* Find the bucket */
    left = 0;
    right = ArrayGetNItems(ARR_NDIM(thresholds), ARR_DIMS(thresholds));
    while left < right {
        let mid = (left + right) / 2;
        let mut ptr: *mut c_char;
        let mut i: c_int;
        let cmpresult: int32;

        /* Locate mid'th array element by advancing from left element */
        ptr = thresholds_data;
        i = left;
        while i < mid {
            ptr = att_addlength_pointer(ptr as usize, typlen, ptr) as *mut c_char;
            ptr = att_align_nominal(ptr as usize, typalign) as *mut c_char;
            i += 1;
        }

        (*(*locfcinfo).args.as_mut_ptr().add(0)).value = operand;
        (*(*locfcinfo).args.as_mut_ptr().add(0)).isnull = false;
        (*(*locfcinfo).args.as_mut_ptr().add(1)).value = fetch_att(ptr as *const c_void, typbyval, typlen);
        (*(*locfcinfo).args.as_mut_ptr().add(1)).isnull = false;

        cmpresult = DatumGetInt32(FunctionCallInvoke!(locfcinfo));

        /* We don't expect comparison support functions to return null */
        Assert!(!(*locfcinfo).isnull);

        if cmpresult < 0 {
            right = mid;
        } else {
            left = mid + 1;

            /*
             * Move the thresholds pointer to match new "left" index, so we
             * don't have to seek over those elements again.  This trick
             * ensures we do only O(N) array indexing work, not O(N^2).
             */
            ptr = att_addlength_pointer(ptr as usize, typlen, ptr) as *mut c_char;
            thresholds_data = att_align_nominal(ptr as usize, typalign) as *mut c_char;
        }
    }

    left
}

/*
 * Trim the last N elements from an array by building an appropriate slice.
 * Only the first dimension is trimmed.
 */
pub unsafe fn trim_array(fcinfo: FunctionCallInfo) -> Datum {
    let v = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    let n = PG_GETARG_INT32!(fcinfo, 1);
    let array_length = if ARR_NDIM(v) > 0 {
        *ARR_DIMS(v).add(0)
    } else {
        0
    };
    let mut elmlen: int16 = 0;
    let mut elmbyval: bool = false;
    let mut elmalign: c_char = 0;
    let mut lower: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut upper: [c_int; MAXDIM as usize] = [0; MAXDIM as usize];
    let mut lowerProvided: [bool; MAXDIM as usize] = [false; MAXDIM as usize];
    let mut upperProvided: [bool; MAXDIM as usize] = [false; MAXDIM as usize];
    let result: Datum;

    /* Per spec, throw an error if out of bounds */
    if n < 0 || n > array_length {
        ereport!(
            ERROR,
            errmsg!(
                "number of elements to trim must be between 0 and {}",
                array_length
            )
        );
    }

    /* Set all the bounds as unprovided except the first upper bound */
    memset(
        lowerProvided.as_mut_ptr() as *mut c_void,
        0,
        core::mem::size_of_val(&lowerProvided),
    );
    memset(
        upperProvided.as_mut_ptr() as *mut c_void,
        0,
        core::mem::size_of_val(&upperProvided),
    );
    if ARR_NDIM(v) > 0 {
        upper[0] = *ARR_LBOUND(v).add(0) + array_length - n - 1;
        upperProvided[0] = true;
    }

    /* Fetch the needed information about the element type */
    get_typlenbyvalalign(
        ARR_ELEMTYPE(v),
        &raw mut elmlen,
        &raw mut elmbyval,
        &raw mut elmalign,
    );

    /* Get the slice */
    result = array_get_slice(
        PointerGetDatum(v as *const c_void),
        1,
        upper.as_mut_ptr(),
        lower.as_mut_ptr(),
        upperProvided.as_mut_ptr(),
        lowerProvided.as_mut_ptr(),
        -1,
        elmlen as c_int,
        elmbyval,
        elmalign,
    );

    PG_RETURN_DATUM!(result)
}
