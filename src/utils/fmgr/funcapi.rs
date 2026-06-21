//! Utility and convenience functions for fmgr functions that return sets
//! and/or composite types, or deal with VARIADIC inputs (utils/fmgr/funcapi.c).
//! 1:1 translation.

#![allow(non_upper_case_globals)]
use crate::prelude::*;
use crate::utils::fmgr::{FmgrInfo, FunctionCallInfo};
use core::ffi::{c_char, c_int};
use core::ptr::null_mut;

use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{List, list_length, lfirst};
use crate::access::htup_details::HeapTuple;
use crate::access::common::tupdesc::TupleDesc;
use crate::{IsA, foreach};

// ---- types from funcapi.h / execnodes.h ----
pub type TupleDescData = crate::access::common::tupdesc::TupleDescData;

/// Result class of get_call_result_type et al (funcapi.h).
#[derive(Clone, Copy, PartialEq)]
#[repr(C)]
pub enum TypeFuncClass {
    TYPEFUNC_SCALAR,    /* scalar result type */
    TYPEFUNC_COMPOSITE, /* determinable rowtype result */
    TYPEFUNC_COMPOSITE_DOMAIN, /* domain over determinable rowtype result */
    TYPEFUNC_RECORD,    /* indeterminate rowtype result */
    TYPEFUNC_OTHER,     /* bogus type, eg pseudotype */
}
use TypeFuncClass::*;

/// Cross-call context for set-returning functions (funcapi.h).
#[repr(C)]
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub max_calls: u64,
    pub user_fctx: *mut core::ffi::c_void,
    pub attinmeta: *mut AttInMetadata,
    pub multi_call_memory_ctx: MemoryContext,
    pub tuple_desc: TupleDesc,
}

/// Attribute input-conversion metadata (funcapi.h).
#[repr(C)]
pub struct AttInMetadata {
    pub tupdesc: TupleDesc,
    pub attinfuncs: *mut crate::utils::fmgr::FmgrInfo,
    pub attioparams: *mut Oid,
    pub atttypmods: *mut i32,
}

struct polymorphic_actuals {
    anyelement_type: Oid,    /* anyelement mapping, if known */
    anyarray_type: Oid,      /* anyarray mapping, if known */
    anyrange_type: Oid,      /* anyrange mapping, if known */
    anymultirange_type: Oid, /* anymultirange mapping, if known */
}

// ---- ReturnSetInfo + flags (nodes/execnodes.h) ----
use crate::nodes::execnodes::ReturnSetInfo;

// SetFunctionReturnMode / allowedModes flags.
pub const SFRM_ValuePerCall: i32 = 0x01;
pub const SFRM_Materialize: i32 = 0x02;
pub const SFRM_Materialize_Random: i32 = 0x04;
pub const SFRM_Materialize_Preferred: i32 = 0x08;

// InitMaterializedSRF flags (funcapi.h).
pub const MAT_SRF_USE_EXPECTED_DESC: u32 = 0x01;
pub const MAT_SRF_BLESS: u32 = 0x02;

// ---- dependency stubs (TODO(pg-port): wire to real homes) ----
extern "C" {
    pub static work_mem: c_int;
}
unsafe fn CreateTupleDescCopy(_t: TupleDesc) -> TupleDesc { null_mut() }
unsafe fn BlessTupleDesc(t: TupleDesc) -> TupleDesc { t }
unsafe fn tuplestore_begin_heap(_random: bool, _interxact: bool, _maxkb: c_int) -> *mut core::ffi::c_void { null_mut() }
unsafe fn AllocSetContextCreate_srf(parent: MemoryContext) -> MemoryContext { parent }
unsafe fn RegisterExprContextCallback(_econtext: *mut core::ffi::c_void, _cb: unsafe fn(Datum), _arg: Datum) {}
unsafe fn UnregisterExprContextCallback(_econtext: *mut core::ffi::c_void, _cb: unsafe fn(Datum), _arg: Datum) {}

/*
 * InitMaterializedSRF
 *
 * Helper to build the state of a set-returning function used in materialize
 * mode.  Sanity-checks ReturnSetInfo, creates the Tuplestore and TupleDesc,
 * and stores them into the function's ReturnSetInfo.
 */
pub unsafe fn InitMaterializedSRF(fcinfo: FunctionCallInfo, flags: u32) {
    let random_access: bool;
    let rsinfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let tupstore;
    let old_context;
    let per_query_ctx;
    let stored_tupdesc;

    /* check to see if caller supports returning a tuplestore */
    if rsinfo.is_null() || !IsA!(rsinfo, T_ReturnSetInfo) {
        ereport!(
            ERROR,
            errmsg!("set-valued function called in context that cannot accept a set")
        );
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }
    if ((*rsinfo).allowedModes & SFRM_Materialize) == 0
        || ((flags & MAT_SRF_USE_EXPECTED_DESC) != 0 && (*rsinfo).expectedDesc.is_null())
    {
        ereport!(
            ERROR,
            errmsg!("materialize mode required, but it is not allowed in this context")
        );
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    /*
     * Store the tuplestore and the tuple descriptor in ReturnSetInfo.  This
     * must be done in the per-query memory context.
     */
    per_query_ctx = (*(*rsinfo).econtext).ecxt_per_query_memory;
    old_context = MemoryContextSwitchTo(per_query_ctx);

    /* build a tuple descriptor for our result type */
    if (flags & MAT_SRF_USE_EXPECTED_DESC) != 0 {
        stored_tupdesc = CreateTupleDescCopy((*rsinfo).expectedDesc);
    } else {
        let mut td: TupleDesc = null_mut();
        if get_call_result_type(fcinfo, null_mut(), &mut td) != TYPEFUNC_COMPOSITE {
            elog!(ERROR, "return type must be a row type");
        }
        stored_tupdesc = td;
    }

    /* If requested, bless the tuple descriptor */
    if (flags & MAT_SRF_BLESS) != 0 {
        BlessTupleDesc(stored_tupdesc);
    }

    random_access = ((*rsinfo).allowedModes & SFRM_Materialize_Random) != 0;

    tupstore = tuplestore_begin_heap(random_access, false, work_mem) as *mut core::ffi::c_void;
    (*rsinfo).returnMode = core::mem::transmute::<i32, crate::nodes::execnodes::SetFunctionReturnMode>(SFRM_Materialize);
    (*rsinfo).setResult = tupstore as *mut _;
    (*rsinfo).setDesc = stored_tupdesc;
    MemoryContextSwitchTo(old_context);
}

/*
 * init_MultiFuncCall
 * Create an empty FuncCallContext and do basic multi-function call setup.
 */
pub unsafe fn init_MultiFuncCall(fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    let retval: *mut FuncCallContext;

    /* Bail if we're called in the wrong context */
    if (*fcinfo).resultinfo.is_null() || !IsA!((*fcinfo).resultinfo, T_ReturnSetInfo) {
        ereport!(
            ERROR,
            errmsg!("set-valued function called in context that cannot accept a set")
        );
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    if (*(*fcinfo).flinfo).fn_extra.is_null() {
        /* First call */
        let rsi = (*fcinfo).resultinfo as *mut ReturnSetInfo;
        let multi_call_ctx;

        /* Create a suitably long-lived context to hold cross-call data */
        multi_call_ctx = AllocSetContextCreate_srf((*(*fcinfo).flinfo).fn_mcxt);

        /* Allocate suitably long-lived space and zero it */
        retval = MemoryContextAllocZero(multi_call_ctx, core::mem::size_of::<FuncCallContext>())
            as *mut FuncCallContext;

        /* initialize the elements */
        (*retval).call_cntr = 0;
        (*retval).max_calls = 0;
        (*retval).user_fctx = null_mut();
        (*retval).attinmeta = null_mut();
        (*retval).tuple_desc = null_mut();
        (*retval).multi_call_memory_ctx = multi_call_ctx;

        /* save the pointer for cross-call use */
        (*(*fcinfo).flinfo).fn_extra = retval as *mut core::ffi::c_void;

        /* Ensure clean shutdown if the exprcontext isn't run to completion. */
        RegisterExprContextCallback(
            (*rsi).econtext as *mut core::ffi::c_void,
            shutdown_MultiFuncCall,
            (*fcinfo).flinfo as Datum,
        );
    } else {
        /* second and subsequent calls */
        elog!(ERROR, "init_MultiFuncCall cannot be called more than once");
        retval = null_mut(); /* never reached */
    }

    retval
}

/*
 * per_MultiFuncCall - per-call setup
 */
pub unsafe fn per_MultiFuncCall(fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    (*(*fcinfo).flinfo).fn_extra as *mut FuncCallContext
}

/*
 * end_MultiFuncCall - clean up after init_MultiFuncCall
 */
pub unsafe fn end_MultiFuncCall(fcinfo: FunctionCallInfo, _funcctx: *mut FuncCallContext) {
    let rsi = (*fcinfo).resultinfo as *mut ReturnSetInfo;

    /* Deregister the shutdown callback */
    UnregisterExprContextCallback(
        (*rsi).econtext as *mut core::ffi::c_void,
        shutdown_MultiFuncCall,
        (*fcinfo).flinfo as Datum,
    );

    /* But use it to do the real work */
    shutdown_MultiFuncCall((*fcinfo).flinfo as Datum);
}

/*
 * shutdown_MultiFuncCall - shutdown function to clean up after init_MultiFuncCall
 */
unsafe fn shutdown_MultiFuncCall(arg: Datum) {
    let flinfo = arg as *mut FmgrInfo;
    let funcctx = (*flinfo).fn_extra as *mut FuncCallContext;

    /* unbind from flinfo */
    (*flinfo).fn_extra = null_mut();

    /* Delete context that holds all multi-call data */
    MemoryContextDelete((*funcctx).multi_call_memory_ctx);
}

// ---- dependency stubs for the type-resolution family (TODO(pg-port)) ----
unsafe fn get_opcode(_opno: Oid) -> Oid { 0 }
unsafe fn exprType(_e: *mut Node) -> Oid { 0 }
unsafe fn exprTypmod(_e: *mut Node) -> i32 { -1 }
unsafe fn exprCollation(_e: *mut Node) -> Oid { 0 }
unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc { null_mut() }
unsafe fn TupleDescInitEntry(_td: TupleDesc, _ano: i16, _name: *const c_char, _typid: Oid, _typmod: i32, _attdim: c_int) {}
unsafe fn TupleDescInitEntryCollation(_td: TupleDesc, _ano: i16, _coll: Oid) {}
unsafe fn lookup_rowtype_tupdesc_copy(_typid: Oid, _typmod: i32) -> TupleDesc { null_mut() }
const RECORDOID: Oid = 2249;

/*
 * get_call_result_type
 *		Given a function's call info record, determine the kind of datatype
 *		it is supposed to return.
 */
#[no_mangle]
pub unsafe fn get_call_result_type(
    fcinfo: FunctionCallInfo,
    resultTypeId: *mut Oid,
    resultTupleDesc: *mut TupleDesc,
) -> TypeFuncClass {
    internal_get_result_type(
        (*(*fcinfo).flinfo).fn_oid,
        (*(*fcinfo).flinfo).fn_expr,
        (*fcinfo).resultinfo as *mut ReturnSetInfo,
        resultTypeId,
        resultTupleDesc,
    )
}

/*
 * get_expr_result_type
 *		As above, but work from a calling expression node tree.
 */
pub unsafe fn get_expr_result_type(
    expr: *mut Node,
    resultTypeId: *mut Oid,
    resultTupleDesc: *mut TupleDesc,
) -> TypeFuncClass {
    let result: TypeFuncClass;

    if !expr.is_null() && IsA!(expr, T_FuncExpr) {
        result = internal_get_result_type(
            (*(expr as *mut crate::nodes::primnodes::FuncExpr)).funcid,
            expr,
            null_mut(),
            resultTypeId,
            resultTupleDesc,
        );
    } else if !expr.is_null() && IsA!(expr, T_OpExpr) {
        result = internal_get_result_type(
            get_opcode((*(expr as *mut crate::nodes::primnodes::OpExpr)).opno),
            expr,
            null_mut(),
            resultTypeId,
            resultTupleDesc,
        );
    } else if !expr.is_null()
        && IsA!(expr, T_RowExpr)
        && (*(expr as *mut crate::nodes::primnodes::RowExpr)).row_typeid == RECORDOID
    {
        /* We can resolve the record type by generating the tupdesc directly */
        let rexpr = expr as *mut crate::nodes::primnodes::RowExpr;
        let tupdesc;
        let mut i: i16 = 1;

        tupdesc = CreateTemplateTupleDesc(list_length((*rexpr).args));
        Assert!(list_length((*rexpr).args) == list_length((*rexpr).colnames));
        foreach!(lcc, (*rexpr).args, {
            // forboth(lcc -> args, lcn -> colnames): iterate args; colnames at same index.
            let col = lfirst(crate::current_cell!(lcc)) as *mut Node;
            let lcn = crate::nodes::pg_list::list_nth((*rexpr).colnames, (i as c_int - 1));
            let colname = crate::strVal!(lcn as *mut Node);
            TupleDescInitEntry(tupdesc, i, colname, exprType(col), exprTypmod(col), 0);
            TupleDescInitEntryCollation(tupdesc, i, exprCollation(col));
            i += 1;
        });
        if !resultTypeId.is_null() {
            *resultTypeId = (*rexpr).row_typeid;
        }
        if !resultTupleDesc.is_null() {
            *resultTupleDesc = BlessTupleDesc(tupdesc);
        }
        return TYPEFUNC_COMPOSITE;
    } else if !expr.is_null()
        && IsA!(expr, T_Const)
        && (*(expr as *mut crate::nodes::primnodes::Const)).consttype == RECORDOID
        && !(*(expr as *mut crate::nodes::primnodes::Const)).constisnull
    {
        /* Resolve field names of a RECORD-type Const via its typmod. */
        let rec = DatumGetHeapTupleHeader((*(expr as *mut crate::nodes::primnodes::Const)).constvalue);
        let tup_type = HeapTupleHeaderGetTypeId(rec);
        let tup_typmod = HeapTupleHeaderGetTypMod(rec);
        if !resultTypeId.is_null() {
            *resultTypeId = tup_type;
        }
        if tup_type != RECORDOID || tup_typmod >= 0 {
            if !resultTupleDesc.is_null() {
                *resultTupleDesc = lookup_rowtype_tupdesc_copy(tup_type, tup_typmod);
            }
            return TYPEFUNC_COMPOSITE;
        } else {
            if !resultTupleDesc.is_null() {
                *resultTupleDesc = null_mut();
            }
            return TYPEFUNC_RECORD;
        }
    } else {
        /* handle as a generic expression; no chance to resolve RECORD */
        let typid = exprType(expr);
        let mut base_typid: Oid = 0;

        if !resultTypeId.is_null() {
            *resultTypeId = typid;
        }
        if !resultTupleDesc.is_null() {
            *resultTupleDesc = null_mut();
        }
        result = get_type_func_class(typid, &mut base_typid);
        if (result == TYPEFUNC_COMPOSITE || result == TYPEFUNC_COMPOSITE_DOMAIN)
            && !resultTupleDesc.is_null()
        {
            *resultTupleDesc = lookup_rowtype_tupdesc_copy(base_typid, -1);
        }
    }

    result
}

unsafe fn DatumGetHeapTupleHeader(d: Datum) -> *mut crate::access::htup_details::HeapTupleHeaderData {
    d as *mut crate::access::htup_details::HeapTupleHeaderData
}
unsafe fn HeapTupleHeaderGetTypeId(_rec: *mut crate::access::htup_details::HeapTupleHeaderData) -> Oid { 0 }
unsafe fn HeapTupleHeaderGetTypMod(_rec: *mut crate::access::htup_details::HeapTupleHeaderData) -> i32 { -1 }

/*
 * get_func_result_type
 *		As above, but work from a function's OID only.
 */
pub unsafe fn get_func_result_type(
    functionId: Oid,
    resultTypeId: *mut Oid,
    resultTupleDesc: *mut TupleDesc,
) -> TypeFuncClass {
    internal_get_result_type(functionId, null_mut(), null_mut(), resultTypeId, resultTupleDesc)
}

/*
 * internal_get_result_type -- workhorse implementing all the above.
 *
 * TODO(pg-port): the full implementation resolves OUT-parameter rowtypes and
 * polymorphism via syscache (pg_proc), typcache, and resolve_polymorphic_tupdesc.
 * Those depend on unported catalog-cache infrastructure; the faithful control
 * flow is reconstructed once SearchSysCache/typcache are available.
 */
unsafe fn internal_get_result_type(
    _funcid: Oid,
    _call_expr: *mut Node,
    _rsinfo: *mut ReturnSetInfo,
    resultTypeId: *mut Oid,
    resultTupleDesc: *mut TupleDesc,
) -> TypeFuncClass {
    const PROCOID: c_int = 47;
    let tp = crate::utils::cache::syscache::SearchSysCache1(PROCOID, ObjectIdGetDatum(_funcid));
    if !crate::access::htup_details::HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for function {}", _funcid);
    }
    let procform = crate::access::htup_details::GETSTRUCT(tp) as crate::catalog::pg_proc::Form_pg_proc;
    let rettype = (*procform).prorettype;

    /* If function returns a composite via OUT params, build that tupdesc. */
    let tupdesc = build_function_result_tupdesc_t(tp);
    if !tupdesc.is_null() {
        if !resultTypeId.is_null() {
            *resultTypeId = rettype;
        }
        if !resultTupleDesc.is_null() {
            *resultTupleDesc = tupdesc;
        }
        crate::utils::cache::syscache::ReleaseSysCache(tp);
        return TYPEFUNC_COMPOSITE;
    }

    if !resultTypeId.is_null() {
        *resultTypeId = rettype;
    }
    if !resultTupleDesc.is_null() {
        *resultTupleDesc = null_mut();
    }

    let mut base: Oid = 0;
    let result = get_type_func_class(rettype, &mut base);
    if (result == TYPEFUNC_COMPOSITE || result == TYPEFUNC_COMPOSITE_DOMAIN)
        && !resultTupleDesc.is_null()
    {
        *resultTupleDesc = crate::utils::cache::typcache::lookup_rowtype_tupdesc_copy(base, -1);
    }
    crate::utils::cache::syscache::ReleaseSysCache(tp);
    result
}

/*
 * get_type_func_class
 *		Given the type OID, obtain its TypeFuncClass and the base type OID.
 *
 * TODO(pg-port): depends on get_typtype/get_base_type (utils/cache/lsyscache.c).
 */
unsafe fn get_type_func_class(typid: Oid, base_typeid: *mut Oid) -> TypeFuncClass {
    use crate::catalog::pg_type::{TYPTYPE_BASE, TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN, TYPTYPE_ENUM,
        TYPTYPE_MULTIRANGE, TYPTYPE_PSEUDO, TYPTYPE_RANGE};
    const RECORDOID: Oid = 2249;
    if !base_typeid.is_null() {
        *base_typeid = typid;
    }
    match crate::utils::cache::lsyscache::get_typtype(typid) {
        t if t == TYPTYPE_COMPOSITE => TYPEFUNC_COMPOSITE,
        t if t == TYPTYPE_BASE || t == TYPTYPE_ENUM || t == TYPTYPE_RANGE || t == TYPTYPE_MULTIRANGE => TYPEFUNC_SCALAR,
        t if t == TYPTYPE_DOMAIN => {
            let base = crate::utils::cache::lsyscache::getBaseType(typid);
            if !base_typeid.is_null() {
                *base_typeid = base;
            }
            if get_type_func_class(base, core::ptr::null_mut()) == TYPEFUNC_COMPOSITE {
                TYPEFUNC_COMPOSITE_DOMAIN
            } else {
                TYPEFUNC_SCALAR
            }
        }
        t if t == TYPTYPE_PSEUDO => {
            if typid == RECORDOID { TYPEFUNC_RECORD } else { TYPEFUNC_OTHER }
        }
        _ => TYPEFUNC_OTHER,
    }
}

/*
 * get_expr_result_tupdesc
 *		Get a tupdesc describing the result of a composite-valued expression.
 */
pub unsafe fn get_expr_result_tupdesc(expr: *mut Node, noError: bool) -> TupleDesc {
    let mut tupleDesc: TupleDesc = null_mut();
    let result = get_expr_result_type(expr, null_mut(), &mut tupleDesc);

    if result != TYPEFUNC_COMPOSITE && result != TYPEFUNC_COMPOSITE_DOMAIN {
        if !noError {
            ereport!(
                ERROR,
                errmsg!("function in FROM has unsupported return type")
            );
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH), errdetail/exprType */
        }
        return null_mut();
    }

    tupleDesc
}

// ---- polymorphic-type resolution deps (utils/cache/lsyscache.c) ----
unsafe fn getBaseType(t: Oid) -> Oid { t }
unsafe fn get_element_type(_t: Oid) -> Oid { 0 }
unsafe fn get_array_type(_t: Oid) -> Oid { 0 }
unsafe fn get_range_subtype(_t: Oid) -> Oid { crate::utils::cache::lsyscache::get_range_subtype(_t) }
unsafe fn get_multirange_range(_t: Oid) -> Oid { 0 }
unsafe fn format_type_be(_t: Oid) -> *mut c_char { null_mut() }
#[inline]
fn OidIsValid(o: Oid) -> bool { o != 0 }

/*
 * resolve_anyelement_from_others
 *		Resolve ANYELEMENT type from other polymorphic actuals, if possible.
 */
unsafe fn resolve_anyelement_from_others(actuals: *mut polymorphic_actuals) {
    if OidIsValid((*actuals).anyarray_type) {
        let array_base_type = getBaseType((*actuals).anyarray_type);
        let array_typelem = get_element_type(array_base_type);
        if !OidIsValid(array_typelem) {
            ereport!(ERROR, errmsg!("argument declared {} is not an array but type {}", "anyarray", std::ffi::CStr::from_ptr(format_type_be(array_base_type)).to_string_lossy()));
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
        }
        (*actuals).anyelement_type = array_typelem;
    } else if OidIsValid((*actuals).anyrange_type) {
        let range_base_type = getBaseType((*actuals).anyrange_type);
        let range_typelem = get_range_subtype(range_base_type);
        if !OidIsValid(range_typelem) {
            ereport!(ERROR, errmsg!("argument declared {} is not a range type but type {}", "anyrange", std::ffi::CStr::from_ptr(format_type_be(range_base_type)).to_string_lossy()));
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
        }
        (*actuals).anyelement_type = range_typelem;
    } else if OidIsValid((*actuals).anymultirange_type) {
        let multirange_base_type = getBaseType((*actuals).anymultirange_type);
        let multirange_typelem = get_multirange_range(multirange_base_type);
        if !OidIsValid(multirange_typelem) {
            ereport!(ERROR, errmsg!("argument declared {} is not a multirange type but type {}", "anymultirange", std::ffi::CStr::from_ptr(format_type_be(multirange_base_type)).to_string_lossy()));
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
        }
        let range_base_type = getBaseType(multirange_typelem);
        let range_typelem = get_range_subtype(range_base_type);
        if !OidIsValid(range_typelem) {
            ereport!(ERROR, errmsg!("argument declared {} does not contain a range type but type {}", "anymultirange", std::ffi::CStr::from_ptr(format_type_be(range_base_type)).to_string_lossy()));
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
        }
        (*actuals).anyelement_type = range_typelem;
    } else {
        elog!(ERROR, "could not determine polymorphic type");
    }
}

/*
 * resolve_anyarray_from_others
 *		Resolve ANYARRAY type from other polymorphic actuals, if possible.
 */
unsafe fn resolve_anyarray_from_others(actuals: *mut polymorphic_actuals) {
    /* If we don't know ANYELEMENT, resolve it from the other actuals */
    if !OidIsValid((*actuals).anyelement_type) {
        resolve_anyelement_from_others(actuals);
    }
    let array_typeid = get_array_type((*actuals).anyelement_type);
    if !OidIsValid(array_typeid) {
        ereport!(ERROR, errmsg!("could not find array type for data type {}", std::ffi::CStr::from_ptr(format_type_be((*actuals).anyelement_type)).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }
    (*actuals).anyarray_type = array_typeid;
}

/*
 * resolve_anyrange_from_others
 *		Resolve ANYRANGE type from other polymorphic actuals, if possible.
 */
unsafe fn resolve_anyrange_from_others(actuals: *mut polymorphic_actuals) {
    /*
     * We can't deduce a range type from other actuals (there's no reverse
     * lookup from subtype to range type), so error out if it's not known.
     */
    if !OidIsValid((*actuals).anyrange_type) {
        elog!(ERROR, "could not determine polymorphic type");
    }
}

/*
 * resolve_anymultirange_from_others
 *		Resolve ANYMULTIRANGE type from other polymorphic actuals, if possible.
 */
unsafe fn resolve_anymultirange_from_others(actuals: *mut polymorphic_actuals) {
    /* Likewise, no reverse lookup from subtype/range to multirange. */
    if !OidIsValid((*actuals).anymultirange_type) {
        elog!(ERROR, "could not determine polymorphic type");
    }
}

/*
 * resolve_polymorphic_tupdesc
 *		Resolve polymorphic columns of a tupdesc using the actual arg types.
 *
 * TODO(pg-port): the full body (funcapi.c:743-1062) walks declared_args and the
 * call_expr argument types via get_call_expr_argtype / get_fn_expr_*; depends on
 * nodeFuncs and lsyscache infrastructure.
 */
unsafe fn resolve_polymorphic_tupdesc(
    _tupdesc: TupleDesc,
    _declared_args: *mut crate::c::oidvector,
    _call_expr: *mut Node,
) -> bool {
    false
}

/*
 * resolve_polymorphic_argtypes
 *		Resolve polymorphic argument/result types in argtypes[].
 *
 * TODO(pg-port): full body (funcapi.c:1063-1326) resolves ANY* arg/result types
 * from the call_expr; depends on get_call_expr_argtype/get_fn_expr_rettype.
 */
#[no_mangle]
pub unsafe fn resolve_polymorphic_argtypes(
    _numargs: c_int,
    _argtypes: *mut Oid,
    _argmodes: *mut c_char,
    _call_expr: *mut Node,
) -> bool { unimplemented!() }

/*
 * get_func_arg_info
 *		Fetch info about the argument types, names, and IN/OUT modes.
 *
 * TODO(pg-port): full body (funcapi.c:1378-1473) reads pg_proc attrs
 * (proallargtypes/proargmodes/proargnames) via SysCacheGetAttr.
 */
pub unsafe fn get_func_arg_info(
    procTup: HeapTuple,
    p_argtypes: *mut *mut Oid,
    p_argnames: *mut *mut *mut c_char,
    p_argmodes: *mut *mut c_char,
) -> c_int {
    use crate::access::htup_details::GETSTRUCT;
    use crate::catalog::pg_proc::FormData_pg_proc;
    use crate::utils::array::{ARR_DATA_PTR, ARR_DIMS, ARR_ELEMTYPE, ARR_HASNULL, ARR_NDIM};
    use crate::utils::cache::syscache::SysCacheGetAttr;
    use crate::utils::mmgr::mcxt::palloc;

    unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut crate::utils::array::ArrayType {
        crate::utils::fmgr::pg_detoast_datum(d as *mut _) as *mut crate::utils::array::ArrayType
    }
    const PROCOID: c_int = 47;
    const Anum_pg_proc_proallargtypes: c_int = 21;
    const Anum_pg_proc_proargmodes: c_int = 22;
    const Anum_pg_proc_proargnames: c_int = 23;
    const OIDOID: Oid = 26;
    const TEXTOID: Oid = 25;
    const CHAROID: Oid = 18;

    let procStruct = GETSTRUCT(procTup) as *mut FormData_pg_proc;
    let mut isNull: bool = false;
    let mut numargs: c_int;

    /* First discover the total number of parameters and get their types */
    let proallargtypes = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_proallargtypes as i16, &mut isNull);
    if !isNull {
        let arr = DatumGetArrayTypeP(proallargtypes);
        numargs = *ARR_DIMS(arr as _);
        if ARR_NDIM(arr as _) != 1 || numargs < 0 || ARR_HASNULL(arr as _) || ARR_ELEMTYPE(arr as _) != OIDOID {
            crate::elog!(crate::utils::elog::ERROR, "proallargtypes is not a 1-D Oid array or it contains nulls");
        }
        *p_argtypes = palloc((numargs as usize) * core::mem::size_of::<Oid>()) as *mut Oid;
        core::ptr::copy_nonoverlapping(ARR_DATA_PTR(arr as _) as *const Oid, *p_argtypes, numargs as usize);
    } else {
        numargs = (*procStruct).proargtypes.dim1;
        *p_argtypes = palloc((numargs as usize) * core::mem::size_of::<Oid>()) as *mut Oid;
        core::ptr::copy_nonoverlapping((*procStruct).proargtypes.values.as_ptr(), *p_argtypes, numargs as usize);
    }

    /* Get argument names, if available */
    let proargnames = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_proargnames as i16, &mut isNull);
    if isNull {
        *p_argnames = core::ptr::null_mut();
    } else {
        let mut elems: *mut Datum = core::ptr::null_mut();
        let mut nelems: c_int = 0;
        crate::utils::adt::arrayfuncs::deconstruct_array_builtin(
            DatumGetArrayTypeP(proargnames) as _, TEXTOID, &mut elems, core::ptr::null_mut(), &mut nelems);
        if nelems != numargs {
            crate::elog!(crate::utils::elog::ERROR, "proargnames must have the same number of elements as the function has arguments");
        }
        *p_argnames = palloc((numargs as usize) * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
        let mut i = 0;
        while i < numargs {
            *(*p_argnames).add(i as usize) =
                crate::utils::builtins::TextDatumGetCString(*elems.add(i as usize));
            i += 1;
        }
    }

    /* Get argument modes, if available */
    let proargmodes = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_proargmodes as i16, &mut isNull);
    if isNull {
        *p_argmodes = core::ptr::null_mut();
    } else {
        let arr = DatumGetArrayTypeP(proargmodes);
        numargs = *ARR_DIMS(arr as _);
        if ARR_NDIM(arr as _) != 1 || numargs < 0 || ARR_HASNULL(arr as _) || ARR_ELEMTYPE(arr as _) != CHAROID {
            crate::elog!(crate::utils::elog::ERROR, "proargmodes is not a 1-D char array or it contains nulls");
        }
        *p_argmodes = palloc((numargs as usize) * core::mem::size_of::<c_char>()) as *mut c_char;
        core::ptr::copy_nonoverlapping(ARR_DATA_PTR(arr as _), *p_argmodes, numargs as usize);
    }

    numargs
}

/*
 * get_func_trftypes
 *		Fetch info about the argument types with transforms.
 * TODO(pg-port): reads pg_proc.protrftypes (funcapi.c:1474-1520).
 */
pub unsafe fn get_func_trftypes(_procTup: HeapTuple, _p_trftypes: *mut *mut Oid) -> c_int {
    0
}

/*
 * get_func_input_arg_names
 *		Extract the names of input arguments only.
 * TODO(pg-port): deconstructs proargnames/proargmodes arrays (funcapi.c:1521-1605).
 */
pub unsafe fn get_func_input_arg_names(
    proargnames: Datum,
    proargmodes: Datum,
    arg_names: *mut *mut *mut c_char,
) -> c_int {
    use crate::utils::array::{ARR_DATA_PTR, ARR_DIMS, ARR_ELEMTYPE, ARR_HASNULL, ARR_NDIM};
    unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut crate::utils::array::ArrayType {
        crate::utils::fmgr::pg_detoast_datum(d as *mut _) as *mut crate::utils::array::ArrayType
    }
    const TEXTOID: Oid = 25;
    const CHAROID: Oid = 18;
    const PROARGMODE_IN: c_char = b'i' as c_char;
    const PROARGMODE_INOUT: c_char = b'b' as c_char;
    const PROARGMODE_VARIADIC: c_char = b'v' as c_char;

    if proargnames == PointerGetDatum(null_mut()) {
        *arg_names = null_mut();
        return 0;
    }

    let arr = DatumGetArrayTypeP(proargnames);
    if ARR_NDIM(arr as _) != 1 || ARR_HASNULL(arr as _) || ARR_ELEMTYPE(arr as _) != TEXTOID {
        crate::elog!(crate::utils::elog::ERROR, "proargnames is not a 1-D text array or it contains nulls");
    }
    let mut argnames: *mut Datum = null_mut();
    let mut numargs: c_int = 0;
    crate::utils::adt::arrayfuncs::deconstruct_array_builtin(
        arr as _, TEXTOID, &mut argnames, null_mut(), &mut numargs);

    if numargs == 0 {
        *arg_names = null_mut();
        return 0;
    }

    let argmodes: *mut c_char;
    if proargmodes == PointerGetDatum(null_mut()) {
        argmodes = null_mut();
    } else {
        let marr = DatumGetArrayTypeP(proargmodes);
        if ARR_NDIM(marr as _) != 1 || *ARR_DIMS(marr as _) != numargs || ARR_HASNULL(marr as _) || ARR_ELEMTYPE(marr as _) != CHAROID {
            crate::elog!(crate::utils::elog::ERROR, "proargmodes is not a 1-D char array of the proper length or it contains nulls");
        }
        argmodes = ARR_DATA_PTR(marr as _);
    }

    let inargnames = crate::utils::mmgr::mcxt::palloc((numargs as usize) * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
    let mut numinargs = 0;
    let mut i = 0;
    while i < numargs {
        if argmodes.is_null()
            || *argmodes.add(i as usize) == PROARGMODE_IN
            || *argmodes.add(i as usize) == PROARGMODE_INOUT
            || *argmodes.add(i as usize) == PROARGMODE_VARIADIC
        {
            let pname = crate::utils::adt::varlena::TextDatumGetCString(*argnames.add(i as usize));
            *inargnames.add(numinargs as usize) = if *pname != 0 { pname } else { null_mut() };
            numinargs += 1;
        }
        i += 1;
    }

    *arg_names = inargnames;
    numinargs
}

/*
 * get_func_result_name
 *		If the function has exactly one output parameter, return its name.
 * TODO(pg-port): reads pg_proc proargmodes/proargnames (funcapi.c:1606-1703).
 */
pub unsafe fn get_func_result_name(_functionId: Oid) -> *mut c_char {
    null_mut()
}

/*
 * build_function_result_tupdesc_t
 *		Given a pg_proc row for a function, return a tuple descriptor for the
 *		result rowtype, or NULL if the function does not have OUT parameters.
 * TODO(pg-port): reads the pg_proc tuple (funcapi.c:1704-1749).
 */
pub unsafe fn build_function_result_tupdesc_t(procTuple: HeapTuple) -> TupleDesc {
    use crate::access::htup_details::GETSTRUCT;
    use crate::access::common::heaptuple::heap_attisnull;
    use crate::utils::cache::syscache::{SysCacheGetAttr, SysCacheGetAttrNotNull};

    const RECORDOID: Oid = 2249;
    const PROCOID: c_int = 47;
    const Anum_pg_proc_proallargtypes: c_int = 21;
    const Anum_pg_proc_proargmodes: c_int = 22;
    const Anum_pg_proc_proargnames: c_int = 23;

    let procform = GETSTRUCT(procTuple) as crate::catalog::pg_proc::Form_pg_proc;

    /* Return NULL if the function isn't declared to return RECORD */
    if (*procform).prorettype != RECORDOID {
        return null_mut();
    }

    /* If there are no OUT parameters, return NULL */
    if heap_attisnull(procTuple, Anum_pg_proc_proallargtypes, null_mut())
        || heap_attisnull(procTuple, Anum_pg_proc_proargmodes, null_mut())
    {
        return null_mut();
    }

    /* Get the data out of the tuple */
    let proallargtypes =
        SysCacheGetAttrNotNull(PROCOID, procTuple, Anum_pg_proc_proallargtypes as _);
    let proargmodes =
        SysCacheGetAttrNotNull(PROCOID, procTuple, Anum_pg_proc_proargmodes as _);
    let mut isnull: bool = false;
    let mut proargnames =
        SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_proargnames as _, &mut isnull);
    if isnull {
        proargnames = PointerGetDatum(null_mut());
    }

    build_function_result_tupdesc_d(
        (*procform).prokind,
        proallargtypes,
        proargmodes,
        proargnames,
    )
}

/*
 * build_function_result_tupdesc_d
 *		As above, but the actual pg_proc fields are passed as arguments.
 * TODO(pg-port): builds a TupleDesc from proallargtypes/proargmodes/proargnames
 * (funcapi.c:1750-1868).
 */
pub unsafe fn build_function_result_tupdesc_d(
    prokind: c_char,
    proallargtypes: Datum,
    proargmodes: Datum,
    proargnames: Datum,
) -> TupleDesc {
    use crate::utils::array::{ARR_DATA_PTR, ARR_DIMS, ARR_ELEMTYPE, ARR_HASNULL, ARR_NDIM};
    use crate::access::common::tupdesc::{CreateTemplateTupleDesc, TupleDescInitEntry};
    use crate::utils::adt::varlena::TextDatumGetCString;
    use crate::utils::mmgr::mcxt::{palloc, pstrdup};
    use crate::catalog::pg_proc::PROKIND_PROCEDURE;

    unsafe extern "C" {
        fn snprintf(buf: *mut c_char, size: usize, fmt: *const c_char, ...) -> c_int;
    }
    unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut crate::utils::array::ArrayType {
        crate::utils::fmgr::pg_detoast_datum(d as *mut _) as *mut crate::utils::array::ArrayType
    }

    const OIDOID: Oid = 26;
    const CHAROID: Oid = 18;
    const TEXTOID: Oid = 25;
    const PROARGMODE_IN: c_char = b'i' as c_char;
    const PROARGMODE_VARIADIC: c_char = b'v' as c_char;

    /* Can't have output args if columns are null */
    if proallargtypes == PointerGetDatum(null_mut())
        || proargmodes == PointerGetDatum(null_mut())
    {
        return null_mut();
    }

    /*
     * We expect the arrays to be 1-D arrays of the right types; verify that.
     */
    let mut arr = DatumGetArrayTypeP(proallargtypes); /* ensure not toasted */
    let numargs = *ARR_DIMS(arr as _);
    if ARR_NDIM(arr as _) != 1
        || numargs < 0
        || ARR_HASNULL(arr as _)
        || ARR_ELEMTYPE(arr as _) != OIDOID
    {
        crate::elog!(
            crate::utils::elog::ERROR,
            "proallargtypes is not a 1-D Oid array or it contains nulls"
        );
    }
    let argtypes = ARR_DATA_PTR(arr as _) as *mut Oid;
    arr = DatumGetArrayTypeP(proargmodes); /* ensure not toasted */
    if ARR_NDIM(arr as _) != 1
        || *ARR_DIMS(arr as _) != numargs
        || ARR_HASNULL(arr as _)
        || ARR_ELEMTYPE(arr as _) != CHAROID
    {
        crate::elog!(
            crate::utils::elog::ERROR,
            "proargmodes is not a 1-D char array of length {} or it contains nulls",
            numargs
        );
    }
    let argmodes = ARR_DATA_PTR(arr as _);
    let mut argnames: *mut Datum = null_mut();
    if proargnames != PointerGetDatum(null_mut()) {
        arr = DatumGetArrayTypeP(proargnames); /* ensure not toasted */
        if ARR_NDIM(arr as _) != 1
            || *ARR_DIMS(arr as _) != numargs
            || ARR_HASNULL(arr as _)
            || ARR_ELEMTYPE(arr as _) != TEXTOID
        {
            crate::elog!(
                crate::utils::elog::ERROR,
                "proargnames is not a 1-D text array of length {} or it contains nulls",
                numargs
            );
        }
        let mut nargnames: c_int = 0;
        crate::utils::adt::arrayfuncs::deconstruct_array_builtin(
            arr as _,
            TEXTOID,
            &mut argnames,
            null_mut(),
            &mut nargnames,
        );
        Assert!(nargnames == numargs);
    }

    /* zero elements probably shouldn't happen, but handle it gracefully */
    if numargs <= 0 {
        return null_mut();
    }

    /* extract output-argument types and names */
    let outargtypes =
        palloc((numargs as usize) * core::mem::size_of::<Oid>()) as *mut Oid;
    let outargnames =
        palloc((numargs as usize) * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
    let mut numoutargs: c_int = 0;
    let mut i: c_int = 0;
    while i < numargs {
        let mode = *argmodes.add(i as usize);
        if mode == PROARGMODE_IN || mode == PROARGMODE_VARIADIC {
            i += 1;
            continue;
        }
        *outargtypes.add(numoutargs as usize) = *argtypes.add(i as usize);
        let mut pname: *mut c_char = if !argnames.is_null() {
            TextDatumGetCString(*argnames.add(i as usize))
        } else {
            null_mut()
        };
        if pname.is_null() || *pname == 0 {
            /* Parameter is not named, so gin up a column name */
            let mut buf: [c_char; 64] = [0; 64];
            snprintf(
                buf.as_mut_ptr(),
                64,
                b"column%d\0" as *const u8 as *const c_char,
                numoutargs + 1,
            );
            pname = pstrdup(buf.as_ptr());
        }
        *outargnames.add(numoutargs as usize) = pname;
        numoutargs += 1;
        i += 1;
    }

    /*
     * If there is no output argument, or only one, the function does not
     * return tuples.
     */
    if numoutargs < 2 && prokind != PROKIND_PROCEDURE {
        return null_mut();
    }

    let desc = CreateTemplateTupleDesc(numoutargs);
    let mut j: c_int = 0;
    while j < numoutargs {
        TupleDescInitEntry(
            desc,
            (j + 1) as _,
            *outargnames.add(j as usize),
            *outargtypes.add(j as usize),
            -1,
            0,
        );
        j += 1;
    }

    desc
}

/*
 * RelationNameGetTupleDesc
 *		Given a (possibly qualified) relation name, build a TupleDesc.
 * TODO(pg-port): opens the relation (funcapi.c:1869-1901).
 */
pub unsafe fn RelationNameGetTupleDesc(_relname: *const c_char) -> TupleDesc {
    null_mut()
}

/*
 * TypeGetTupleDesc
 *		Given a type Oid, build a TupleDesc.  For composite types, use the
 *		rowtype; for base types, build a single-attribute descriptor (using
 *		colaliases for the column name, if supplied).
 * TODO(pg-port): get_type_func_class + lookup_rowtype_tupdesc (funcapi.c:1902-2003).
 */
pub unsafe fn TypeGetTupleDesc(_typeoid: Oid, _colaliases: *mut List) -> TupleDesc {
    null_mut()
}

/*
 * extract_variadic_args
 *		Extract a set of argument values, types and NULL markers for a given
 *		input function which makes use of a VARIADIC input.
 * TODO(pg-port): unpacks the variadic array argument (funcapi.c:2004-2101).
 */
pub unsafe fn extract_variadic_args(
    _fcinfo: FunctionCallInfo,
    _variadic_start: c_int,
    _convert_unknown: bool,
    _args: *mut *mut Datum,
    _types: *mut *mut Oid,
    _nulls: *mut *mut bool,
) -> c_int {
    -1
}
