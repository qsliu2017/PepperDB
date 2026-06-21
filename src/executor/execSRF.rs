//! src/backend/executor/execSRF.c
//!
//! Routines implementing the API for set-returning functions
//!
//! This file serves nodeFunctionscan.c and nodeProjectSet.c, providing
//! common code for calling set-returning functions according to the
//! ReturnSetInfo API.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_short, c_void};
use std::ptr::{null, null_mut};

use crate::{castNode, foreach, current_cell, makeNode, IsA};

use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use crate::nodes::execnodes::{
    ExprContext, ExprState, PlanState, ReturnSetInfo, SetExprState, Tuplestorestate,
};
use crate::nodes::execnodes::ExprDoneCond::*;
use crate::nodes::execnodes::SetFunctionReturnMode::*;
use crate::nodes::execnodes::ExprDoneCond;
use crate::nodes::nodes::{nodeTag, Node, NodeTag};
use crate::nodes::primnodes::{Expr, FuncExpr, OpExpr};
use crate::nodes::pg_list::{lfirst, list_length, List, ListCell};

use crate::access::attnum::AttrNumber;
use crate::access::common::tupdesc::TupleDesc;

use crate::utils::fmgr::{
    fmgr_info_cxt, FmgrInfo, FunctionCallInfo, SizeForFunctionCallInfo,
};
use crate::{InitFunctionCallInfoData, FunctionCallInvoke, fmgr_info_set_expr};

use crate::executor::executor::{
    executor_errposition, ExecEvalExpr, ExecInitExpr, ExecInitExprList, RegisterExprContextCallback,
    ResetExprContext,
};
use crate::nodes::execnodes::EState;
use crate::executor::tuptable::{slot_getattr, ExecClearTuple, TupleTableSlot, TupleTableSlotOps};
use crate::executor::execTuples::TTSOpsMinimalTuple;

use crate::utils::activity::pgstat_function::{
    pgstat_end_function_usage, pgstat_init_function_usage, PgStat_FunctionCallUsage,
};

/* ------- local type aliases / constants for unported deps ------- */

/* utils/funcapi.h: TypeFuncClass */
type TypeFuncClass = c_int;
const TYPEFUNC_SCALAR: TypeFuncClass = 0;
const TYPEFUNC_COMPOSITE: TypeFuncClass = 1;
const TYPEFUNC_COMPOSITE_DOMAIN: TypeFuncClass = 2;
#[allow(dead_code)]
const TYPEFUNC_RECORD: TypeFuncClass = 3;

/* access/htup.h: HeapTupleData / HeapTupleHeader */
type HeapTupleHeader = *mut c_void;

#[repr(C)]
struct HeapTupleData {
    t_len: u32,
    t_self: [u8; 6], /* ItemPointerData */
    t_tableOid: Oid,
    t_data: HeapTupleHeader,
}

/* access/tupdesc.h: dropped/format accessors return TupleConstr etc.;
 * here we treat TupleDesc as an opaque pointer for cross-module use. */
type Form_pg_attribute = *mut c_void;

/* utils/acl.h: AclResult */
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;

/* nodes/parsenodes.h: ObjectType */
type ObjectType = c_int;
const OBJECT_FUNCTION: ObjectType = 0;

/* ------------------------------------------------------------------
 * Stubs for as-yet-unported helper functions.
 * ------------------------------------------------------------------ */

unsafe fn exprType(_expr: *mut Node) -> Oid {
    crate::nodes::nodeFuncs::exprType(_expr as _) as _
}
unsafe fn exprLocation(_expr: *mut Node) -> c_int {
    crate::nodes::nodeFuncs::exprLocation(_expr as _) as _
}
unsafe fn type_is_rowtype(_typid: Oid) -> bool {
    crate::utils::cache::lsyscache::type_is_rowtype(_typid as _) as _
}
unsafe fn tuplestore_begin_heap(
    _randomAccess: bool,
    _interXact: bool,
    _maxKBytes: c_int,
) -> *mut Tuplestorestate {
    crate::utils::sort::tuplestore::tuplestore_begin_heap(_randomAccess as _, _interXact as _, _maxKBytes as _) as _
}
unsafe fn tuplestore_puttuple(_state: *mut Tuplestorestate, _tuple: *mut HeapTupleData) {
    crate::utils::sort::tuplestore::tuplestore_puttuple(_state as _, _tuple as _)
}
unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    crate::utils::sort::tuplestore::tuplestore_putvalues(_state as _, _tdesc as _, _values as _, _isnull as _)
}
unsafe fn tuplestore_gettupleslot(
    _state: *mut Tuplestorestate,
    _forward: bool,
    _copy: bool,
    _slot: *mut TupleTableSlot,
) -> bool {
    crate::utils::sort::tuplestore::tuplestore_gettupleslot(_state as _, _forward as _, _copy as _, _slot as _) as _
}
unsafe fn tuplestore_end(_state: *mut Tuplestorestate) {
    crate::utils::sort::tuplestore::tuplestore_end(_state as _)
}
unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc {
    crate::access::common::tupdesc::CreateTemplateTupleDesc(_natts as _) as _
}
unsafe fn CreateTupleDescCopy(_tupdesc: TupleDesc) -> TupleDesc {
    crate::access::common::tupdesc::CreateTupleDescCopy(_tupdesc as _) as _
}
unsafe fn FreeTupleDesc(_tupdesc: TupleDesc) {
    crate::access::common::tupdesc::FreeTupleDesc(_tupdesc as _)
}
unsafe fn TupleDescInitEntry(
    _desc: TupleDesc,
    _attributeNumber: AttrNumber,
    _attributeName: *const c_char,
    _oidtypeid: Oid,
    _typmod: i32,
    _attdim: c_int,
) {
    crate::access::common::tupdesc::TupleDescInitEntry(_desc as _, _attributeNumber as _, _attributeName as _, _oidtypeid as _, _typmod as _, _attdim as _)
}
unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: c_int) -> Form_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(_tupdesc as _, _i as _) as _
}
unsafe fn lookup_rowtype_tupdesc_copy(_type_id: Oid, _typmod: i32) -> TupleDesc {
    crate::utils::cache::typcache::lookup_rowtype_tupdesc_copy(_type_id as _, _typmod as _) as _
}
unsafe fn DatumGetHeapTupleHeader(_d: Datum) -> HeapTupleHeader {
    _d as HeapTupleHeader
}
unsafe fn HeapTupleHeaderGetTypeId(_td: HeapTupleHeader) -> Oid {
    crate::access::htup_details::HeapTupleHeaderGetTypeId(_td as _) as _
}
unsafe fn HeapTupleHeaderGetTypMod(_td: HeapTupleHeader) -> i32 {
    crate::access::htup_details::HeapTupleHeaderGetTypMod(_td as _) as _
}
unsafe fn HeapTupleHeaderGetDatumLength(_td: HeapTupleHeader) -> u32 {
    crate::access::htup_details::HeapTupleHeaderGetDatumLength(_td as _) as _
}
unsafe fn object_aclcheck(
    _classid: Oid,
    _objectid: Oid,
    _roleid: Oid,
    _mode: c_int,
) -> AclResult {
    core::mem::transmute::<crate::utils::adt::acl::AclResult, AclResult>(
        crate::catalog::aclchk::object_aclcheck(_classid, _objectid, _roleid, _mode as _),
    )
}
unsafe fn aclcheck_error(_aclerr: AclResult, _objtype: ObjectType, _objectname: *const c_char) {
    crate::catalog::aclchk::aclcheck_error(
        core::mem::transmute::<i32, crate::utils::adt::acl::AclResult>(_aclerr),
        core::mem::transmute::<i32, crate::nodes::parsenodes::ObjectType>(_objtype),
        _objectname as _,
    )
}
unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
unsafe fn get_func_name(_funcid: Oid) -> *mut c_char {
    crate::utils::cache::lsyscache::get_func_name(_funcid as _) as _
}
unsafe fn InvokeFunctionExecuteHook(_objectId: Oid) {
    // TODO: catalog/objectaccess.h -- no-op when no hook registered
}
unsafe fn get_expr_result_type(
    _expr: *mut Node,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> TypeFuncClass {
    crate::utils::fmgr::funcapi::get_expr_result_type(_expr as _, _resultTypeId as _, _resultTupleDesc as _) as _
}
unsafe fn MakeSingleTupleTableSlot(
    _tupdesc: TupleDesc,
    _tts_ops: *const TupleTableSlotOps,
) -> *mut TupleTableSlot {
    crate::executor::execTuples::MakeSingleTupleTableSlot(_tupdesc as _, _tts_ops as _) as _
}
pub unsafe fn ExecFetchSlotHeapTupleDatum(_slot: *mut TupleTableSlot) -> Datum {
    crate::executor::execTuples::ExecFetchSlotHeapTupleDatum(_slot as _) as _
}
unsafe fn check_stack_depth() {
    crate::miscadmin::check_stack_depth()
}
unsafe fn IsBinaryCoercible(_srctype: Oid, _targettype: Oid) -> bool {
    crate::parser::parse_coerce::IsBinaryCoercible(_srctype as _, _targettype as _) as _
}
unsafe fn format_type_be(_type_oid: Oid) -> *mut c_char {
    crate::utils::adt::format_type::format_type_be(_type_oid as _) as _
}

/* utils/guc.c: work_mem */
extern "C" {
    static mut work_mem: c_int;
}

/* attribute accessors -- placeholders matching C field access on
 * Form_pg_attribute (treated opaquely here). */
unsafe fn attr_atttypid(_attr: Form_pg_attribute) -> Oid {
    (*(_attr as *const crate::catalog::pg_attribute::FormData_pg_attribute)).atttypid as _
}
unsafe fn attr_attisdropped(_attr: Form_pg_attribute) -> bool {
    (*(_attr as *const crate::catalog::pg_attribute::FormData_pg_attribute)).attisdropped
}
unsafe fn attr_attlen(_attr: Form_pg_attribute) -> i16 {
    (*(_attr as *const crate::catalog::pg_attribute::FormData_pg_attribute)).attlen as _
}
unsafe fn attr_attalign(_attr: Form_pg_attribute) -> c_char {
    (*(_attr as *const crate::catalog::pg_attribute::FormData_pg_attribute)).attalign as _
}

/* TupleDesc field accessors (natts, tdtypeid, tdtypmod, tdrefcount). */
unsafe fn tupdesc_natts(_tupdesc: TupleDesc) -> c_int {
    (*_tupdesc).natts as _
}
unsafe fn tupdesc_tdtypeid(_tupdesc: TupleDesc) -> Oid {
    (*_tupdesc).tdtypeid as _
}
unsafe fn tupdesc_tdtypmod(_tupdesc: TupleDesc) -> i32 {
    (*_tupdesc).tdtypmod as _
}
unsafe fn tupdesc_tdrefcount(_tupdesc: TupleDesc) -> c_int {
    (*_tupdesc).tdrefcount as _
}

/* catalog/pg_proc.h */
const ProcedureRelationId: Oid = 1255;
/* utils/acl.h */
const ACL_EXECUTE: c_int = 1 << 5;
/* miscadmin.h: FUNC_MAX_ARGS */
const FUNC_MAX_ARGS: c_int = 100;

/* ReturnSetInfo cannot be mem::zeroed (SetFunctionReturnMode has no 0 variant);
 * mirror the C "= {0}" by constructing it with placeholder fields that callers
 * overwrite before use. */
unsafe fn new_rsinfo() -> ReturnSetInfo {
    ReturnSetInfo {
        r#type: NodeTag::T_Invalid,
        econtext: null_mut(),
        expectedDesc: null_mut(),
        allowedModes: 0,
        returnMode: SFRM_ValuePerCall,
        isDone: ExprSingleResult,
        setResult: null_mut(),
        setDesc: null_mut(),
    }
}

/* static function decls (translated below) */

/*
 * Prepare function call in FROM (ROWS FROM) for execution.
 *
 * This is used by nodeFunctionscan.c.
 */
pub unsafe fn ExecInitTableFunctionResult(
    expr: *mut Expr,
    econtext: *mut ExprContext,
    parent: *mut PlanState,
) -> *mut SetExprState {
    let state: *mut SetExprState = makeNode!(SetExprState, T_SetExprState);

    (*state).funcReturnsSet = false;
    (*state).expr = expr;
    (*state).func.fn_oid = InvalidOid;

    /*
     * Normally the passed expression tree will be a FuncExpr, since the
     * grammar only allows a function call at the top level of a table
     * function reference.  However, if the function doesn't return set then
     * the planner might have replaced the function call via constant-folding
     * or inlining.  So if we see any other kind of expression node, execute
     * it via the general ExecEvalExpr() code.  That code path will not
     * support set-returning functions buried in the expression, though.
     */
    if IsA!(expr, T_FuncExpr) {
        let func = expr as *mut FuncExpr;

        (*state).funcReturnsSet = (*func).funcretset;
        (*state).args = ExecInitExprList((*func).args, parent);

        init_sexpr(
            (*func).funcid,
            (*func).inputcollid,
            expr,
            state,
            parent,
            (*econtext).ecxt_per_query_memory,
            (*func).funcretset,
            false,
        );
    } else {
        (*state).elidedFuncState = ExecInitExpr(expr, parent);
    }

    state
}

/*
 *		ExecMakeTableFunctionResult
 *
 * Evaluate a table function, producing a materialized result in a Tuplestore
 * object.
 *
 * This is used by nodeFunctionscan.c.
 */
pub unsafe fn ExecMakeTableFunctionResult(
    setexpr: *mut SetExprState,
    econtext: *mut ExprContext,
    argContext: MemoryContext,
    expectedDesc: TupleDesc,
    randomAccess: bool,
) -> *mut Tuplestorestate {
    let mut tupstore: *mut Tuplestorestate = null_mut();
    let mut tupdesc: TupleDesc = null_mut();
    let funcrettype: Oid;
    let returnsTuple: bool;
    let mut returnsSet: bool = false;
    let fcinfo: FunctionCallInfo;
    let mut fcusage: PgStat_FunctionCallUsage = std::mem::zeroed();
    let mut rsinfo: ReturnSetInfo = new_rsinfo();
    let mut tmptup: HeapTupleData = std::mem::zeroed();
    let callerContext: MemoryContext;
    let mut first_time: bool = true;

    /*
     * Execute per-tablefunc actions in appropriate context.
     *
     * The FunctionCallInfo needs to live across all the calls to a
     * ValuePerCall function, so it can't be allocated in the per-tuple
     * context. Similarly, the function arguments need to be evaluated in a
     * context that is longer lived than the per-tuple context: The argument
     * values would otherwise disappear when we reset that context in the
     * inner loop.  As the caller's CurrentMemoryContext is typically a
     * query-lifespan context, we don't want to leak memory there.  We require
     * the caller to pass a separate memory context that can be used for this,
     * and can be reset each time through to avoid bloat.
     */
    MemoryContextReset(argContext);
    callerContext = MemoryContextSwitchTo(argContext);

    funcrettype = exprType((*setexpr).expr as *mut Node);

    returnsTuple = type_is_rowtype(funcrettype);

    /*
     * Prepare a resultinfo node for communication.  We always do this even if
     * not expecting a set result, so that we can pass expectedDesc.  In the
     * generic-expression case, the expression doesn't actually get to see the
     * resultinfo, but set it up anyway because we use some of the fields as
     * our own state variables.
     */
    rsinfo.r#type = NodeTag::T_ReturnSetInfo;
    rsinfo.econtext = econtext;
    rsinfo.expectedDesc = expectedDesc;
    rsinfo.allowedModes =
        (SFRM_ValuePerCall as c_int) | (SFRM_Materialize as c_int) | (SFRM_Materialize_Preferred as c_int);
    if randomAccess {
        rsinfo.allowedModes |= SFRM_Materialize_Random as c_int;
    }
    rsinfo.returnMode = SFRM_ValuePerCall;
    /* isDone is filled below */
    rsinfo.setResult = null_mut();
    rsinfo.setDesc = null_mut();

    fcinfo = palloc(SizeForFunctionCallInfo(list_length((*setexpr).args) as usize)) as FunctionCallInfo;

    /*
     * Normally the passed expression tree will be a SetExprState, since the
     * grammar only allows a function call at the top level of a table
     * function reference.  However, if the function doesn't return set then
     * the planner might have replaced the function call via constant-folding
     * or inlining.  So if we see any other kind of expression node, execute
     * it via the general ExecEvalExpr() code; the only difference is that we
     * don't get a chance to pass a special ReturnSetInfo to any functions
     * buried in the expression.
     */
    if (*setexpr).elidedFuncState.is_null() {
        /*
         * This path is similar to ExecMakeFunctionResultSet.
         */
        returnsSet = (*setexpr).funcReturnsSet;
        InitFunctionCallInfoData!(
            fcinfo,
            &raw mut (*setexpr).func,
            list_length((*setexpr).args) as c_short,
            (*(*setexpr).fcinfo).fncollation,
            null_mut(),
            &mut rsinfo as *mut ReturnSetInfo as *mut Node
        );
        /* evaluate the function's argument list */
        Assert!(CurrentMemoryContext == argContext);
        ExecEvalFuncArgs(fcinfo, (*setexpr).args, econtext);

        /*
         * If function is strict, and there are any NULL arguments, skip
         * calling the function and act like it returned NULL (or an empty
         * set, in the returns-set case).
         */
        if (*setexpr).func.fn_strict {
            let mut skip = false;
            let nargs = (*fcinfo).nargs as usize;
            for i in 0..nargs {
                if (*(*fcinfo).args.as_ptr().add(i)).isnull {
                    skip = true;
                    break;
                }
            }
            if skip {
                /* goto no_function_result */
                return no_function_result(
                    &mut rsinfo,
                    econtext,
                    randomAccess,
                    returnsSet,
                    expectedDesc,
                    callerContext,
                );
            }
        }
    } else {
        /* Treat setexpr as a generic expression */
        InitFunctionCallInfoData!(fcinfo, null_mut::<FmgrInfo>(), 0, InvalidOid, null_mut(), null_mut());
    }

    /*
     * Switch to short-lived context for calling the function or expression.
     */
    MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    /*
     * Loop to handle the ValuePerCall protocol (which is also the same
     * behavior needed in the generic ExecEvalExpr path).
     */
    loop {
        let result: Datum;

        CHECK_FOR_INTERRUPTS();

        /*
         * Reset per-tuple memory context before each call of the function or
         * expression. This cleans up any local memory the function may leak
         * when called.
         */
        ResetExprContext(econtext);

        /* Call the function or expression one time */
        if (*setexpr).elidedFuncState.is_null() {
            pgstat_init_function_usage(fcinfo, &mut fcusage);

            (*fcinfo).isnull = false;
            rsinfo.isDone = ExprSingleResult;
            result = FunctionCallInvoke!(fcinfo);

            pgstat_end_function_usage(&mut fcusage, rsinfo.isDone != ExprMultipleResult);
        } else {
            result = ExecEvalExpr((*setexpr).elidedFuncState, econtext, &mut (*fcinfo).isnull);
            rsinfo.isDone = ExprSingleResult;
        }

        /* Which protocol does function want to use? */
        if rsinfo.returnMode == SFRM_ValuePerCall {
            /*
             * Check for end of result set.
             */
            if rsinfo.isDone == ExprEndResult {
                break;
            }

            /*
             * If first time through, build tuplestore for result.  For a
             * scalar function result type, also make a suitable tupdesc.
             */
            if first_time {
                let oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_query_memory);

                tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);
                rsinfo.setResult = tupstore;
                if !returnsTuple {
                    tupdesc = CreateTemplateTupleDesc(1);
                    TupleDescInitEntry(
                        tupdesc,
                        1 as AttrNumber,
                        c"column".as_ptr(),
                        funcrettype,
                        -1,
                        0,
                    );
                    rsinfo.setDesc = tupdesc;
                }
                MemoryContextSwitchTo(oldcontext);
            }

            /*
             * Store current resultset item.
             */
            if returnsTuple {
                if !(*fcinfo).isnull {
                    let td: HeapTupleHeader = DatumGetHeapTupleHeader(result);

                    if tupdesc.is_null() {
                        let oldcontext =
                            MemoryContextSwitchTo((*econtext).ecxt_per_query_memory);

                        /*
                         * This is the first non-NULL result from the
                         * function.  Use the type info embedded in the
                         * rowtype Datum to look up the needed tupdesc.  Make
                         * a copy for the query.
                         */
                        tupdesc = lookup_rowtype_tupdesc_copy(
                            HeapTupleHeaderGetTypeId(td),
                            HeapTupleHeaderGetTypMod(td),
                        );
                        rsinfo.setDesc = tupdesc;
                        MemoryContextSwitchTo(oldcontext);
                    } else {
                        /*
                         * Verify all later returned rows have same subtype;
                         * necessary in case the type is RECORD.
                         */
                        if HeapTupleHeaderGetTypeId(td) != tupdesc_tdtypeid(tupdesc)
                            || HeapTupleHeaderGetTypMod(td) != tupdesc_tdtypmod(tupdesc)
                        {
                            ereport!(
                                ERROR,
                                "rows returned by function are not all of the same row type"
                            );
                        }
                    }

                    /*
                     * tuplestore_puttuple needs a HeapTuple not a bare
                     * HeapTupleHeader, but it doesn't need all the fields.
                     */
                    tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
                    tmptup.t_data = td;

                    tuplestore_puttuple(tupstore, &mut tmptup);
                } else {
                    /*
                     * NULL result from a tuple-returning function; expand it
                     * to a row of all nulls.  We rely on the expectedDesc to
                     * form such rows.  (Note: this would be problematic if
                     * tuplestore_putvalues saved the tdtypeid/tdtypmod from
                     * the provided descriptor, since that might not match
                     * what we get from the function itself.  But it doesn't.)
                     */
                    let natts = tupdesc_natts(expectedDesc);
                    let nullflags: *mut bool =
                        palloc(natts as usize * std::mem::size_of::<bool>()) as *mut bool;
                    std::ptr::write_bytes(nullflags, 1u8, natts as usize);
                    tuplestore_putvalues(tupstore, expectedDesc, null_mut(), nullflags);
                }
            } else {
                /* Scalar-type case: just store the function result */
                let mut res = result;
                tuplestore_putvalues(tupstore, tupdesc, &mut res, &mut (*fcinfo).isnull);
            }

            /*
             * Are we done?
             */
            if rsinfo.isDone != ExprMultipleResult {
                break;
            }

            /*
             * Check that set-returning functions were properly declared.
             * (Note: for historical reasons, we don't complain if a non-SRF
             * returns ExprEndResult; that's treated as returning NULL.)
             */
            if !returnsSet {
                ereport!(
                    ERROR,
                    "table-function protocol for value-per-call mode was not followed"
                );
            }
        } else if rsinfo.returnMode == SFRM_Materialize {
            /* check we're on the same page as the function author */
            if !first_time || rsinfo.isDone != ExprSingleResult || !returnsSet {
                ereport!(
                    ERROR,
                    "table-function protocol for materialize mode was not followed"
                );
            }
            /* Done evaluating the set result */
            break;
        } else {
            elog!(
                ERROR,
                "unrecognized table-function returnMode: {}",
                rsinfo.returnMode as c_int
            );
        }

        first_time = false;
    }

    /* no_function_result: */
    no_function_result(
        &mut rsinfo,
        econtext,
        randomAccess,
        returnsSet,
        expectedDesc,
        callerContext,
    )
}

/*
 * Tail logic shared by the strict-skip "goto no_function_result" and the
 * normal loop fall-through in ExecMakeTableFunctionResult.
 */
unsafe fn no_function_result(
    rsinfo: *mut ReturnSetInfo,
    econtext: *mut ExprContext,
    randomAccess: bool,
    returnsSet: bool,
    expectedDesc: TupleDesc,
    callerContext: MemoryContext,
) -> *mut Tuplestorestate {
    /*
     * If we got nothing from the function (ie, an empty-set or NULL result),
     * we have to create the tuplestore to return, and if it's a
     * non-set-returning function then insert a single all-nulls row.  As
     * above, we depend on the expectedDesc to manufacture the dummy row.
     */
    if (*rsinfo).setResult.is_null() {
        let oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_query_memory);

        let tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);
        (*rsinfo).setResult = tupstore;
        MemoryContextSwitchTo(oldcontext);

        if !returnsSet {
            let natts = tupdesc_natts(expectedDesc);
            let nullflags: *mut bool =
                palloc(natts as usize * std::mem::size_of::<bool>()) as *mut bool;
            std::ptr::write_bytes(nullflags, 1u8, natts as usize);
            tuplestore_putvalues(tupstore, expectedDesc, null_mut(), nullflags);
        }
    }

    /*
     * If function provided a tupdesc, cross-check it.  We only really need to
     * do this for functions returning RECORD, but might as well do it always.
     */
    if !(*rsinfo).setDesc.is_null() {
        tupledesc_match(expectedDesc, (*rsinfo).setDesc);

        /*
         * If it is a dynamically-allocated TupleDesc, free it: it is
         * typically allocated in a per-query context, so we must avoid
         * leaking it across multiple usages.
         */
        if tupdesc_tdrefcount((*rsinfo).setDesc) == -1 {
            FreeTupleDesc((*rsinfo).setDesc);
        }
    }

    MemoryContextSwitchTo(callerContext);

    /* All done, pass back the tuplestore */
    (*rsinfo).setResult
}

/*
 * Prepare targetlist SRF function call for execution.
 *
 * This is used by nodeProjectSet.c.
 */
pub unsafe fn ExecInitFunctionResultSet(
    expr: *mut Expr,
    econtext: *mut ExprContext,
    parent: *mut PlanState,
) -> *mut SetExprState {
    let state: *mut SetExprState = makeNode!(SetExprState, T_SetExprState);

    (*state).funcReturnsSet = true;
    (*state).expr = expr;
    (*state).func.fn_oid = InvalidOid;

    /*
     * Initialize metadata.  The expression node could be either a FuncExpr or
     * an OpExpr.
     */
    if IsA!(expr, T_FuncExpr) {
        let func = expr as *mut FuncExpr;

        (*state).args = ExecInitExprList((*func).args, parent);
        init_sexpr(
            (*func).funcid,
            (*func).inputcollid,
            expr,
            state,
            parent,
            (*econtext).ecxt_per_query_memory,
            true,
            true,
        );
    } else if IsA!(expr, T_OpExpr) {
        let op = expr as *mut OpExpr;

        (*state).args = ExecInitExprList((*op).args, parent);
        init_sexpr(
            (*op).opfuncid,
            (*op).inputcollid,
            expr,
            state,
            parent,
            (*econtext).ecxt_per_query_memory,
            true,
            true,
        );
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(expr) as c_int);
    }

    /* shouldn't get here unless the selected function returns set */
    Assert!((*state).func.fn_retset);

    state
}

/*
 *		ExecMakeFunctionResultSet
 *
 * Evaluate the arguments to a set-returning function and then call the
 * function itself.  The argument expressions may not contain set-returning
 * functions (the planner is supposed to have separated evaluation for those).
 *
 * This should be called in a short-lived (per-tuple) context, argContext
 * needs to live until all rows have been returned (i.e. *isDone set to
 * ExprEndResult or ExprSingleResult).
 *
 * This is used by nodeProjectSet.c.
 */
pub unsafe fn ExecMakeFunctionResultSet(
    fcache: *mut SetExprState,
    econtext: *mut ExprContext,
    argContext: MemoryContext,
    isNull: *mut bool,
    isDone: *mut ExprDoneCond,
) -> Datum {
    let mut arguments: *mut List;
    let mut result: Datum;
    let mut fcinfo: FunctionCallInfo;
    let mut fcusage: PgStat_FunctionCallUsage = std::mem::zeroed();
    let mut rsinfo: ReturnSetInfo = new_rsinfo();
    let mut callit: bool;
    let mut i: c_int;

    /* restart: */
    loop {
        /* Guard against stack overflow due to overly complex expressions */
        check_stack_depth();

        /*
         * If a previous call of the function returned a set result in the form of
         * a tuplestore, continue reading rows from the tuplestore until it's
         * empty.
         */
        if !(*fcache).funcResultStore.is_null() {
            let slot: *mut TupleTableSlot = (*fcache).funcResultSlot;

            /*
             * Have to make sure tuple in slot lives long enough, otherwise
             * clearing the slot could end up trying to free something already
             * freed.
             */
            let oldContext = MemoryContextSwitchTo((*slot).tts_mcxt);
            let foundTup = tuplestore_gettupleslot(
                (*fcache).funcResultStore,
                true,
                false,
                (*fcache).funcResultSlot,
            );
            MemoryContextSwitchTo(oldContext);

            if foundTup {
                *isDone = ExprMultipleResult;
                if (*fcache).funcReturnsTuple {
                    /* We must return the whole tuple as a Datum. */
                    *isNull = false;
                    return ExecFetchSlotHeapTupleDatum((*fcache).funcResultSlot);
                } else {
                    /* Extract the first column and return it as a scalar. */
                    return slot_getattr((*fcache).funcResultSlot, 1, isNull);
                }
            }
            /* Exhausted the tuplestore, so clean up */
            tuplestore_end((*fcache).funcResultStore);
            (*fcache).funcResultStore = null_mut();
            *isDone = ExprEndResult;
            *isNull = true;
            return 0 as Datum;
        }

        /*
         * arguments is a list of expressions to evaluate before passing to the
         * function manager.  We skip the evaluation if it was already done in the
         * previous call (ie, we are continuing the evaluation of a set-valued
         * function).  Otherwise, collect the current argument values into fcinfo.
         *
         * The arguments have to live in a context that lives at least until all
         * rows from this SRF have been returned, otherwise ValuePerCall SRFs
         * would reference freed memory after the first returned row.
         */
        fcinfo = (*fcache).fcinfo;
        arguments = (*fcache).args;
        if !(*fcache).setArgsValid {
            let oldContext = MemoryContextSwitchTo(argContext);

            ExecEvalFuncArgs(fcinfo, arguments, econtext);
            MemoryContextSwitchTo(oldContext);
        } else {
            /* Reset flag (we may set it again below) */
            (*fcache).setArgsValid = false;
        }

        /*
         * Now call the function, passing the evaluated parameter values.
         */

        /* Prepare a resultinfo node for communication. */
        (*fcinfo).resultinfo = &mut rsinfo as *mut ReturnSetInfo as *mut Node;
        rsinfo.r#type = NodeTag::T_ReturnSetInfo;
        rsinfo.econtext = econtext;
        rsinfo.expectedDesc = (*fcache).funcResultDesc;
        rsinfo.allowedModes = (SFRM_ValuePerCall as c_int) | (SFRM_Materialize as c_int);
        /* note we do not set SFRM_Materialize_Random or _Preferred */
        rsinfo.returnMode = SFRM_ValuePerCall;
        /* isDone is filled below */
        rsinfo.setResult = null_mut();
        rsinfo.setDesc = null_mut();

        /*
         * If function is strict, and there are any NULL arguments, skip calling
         * the function.
         */
        callit = true;
        if (*fcache).func.fn_strict {
            i = 0;
            while i < (*fcinfo).nargs as c_int {
                if (*(*fcinfo).args.as_ptr().add(i as usize)).isnull {
                    callit = false;
                    break;
                }
                i += 1;
            }
        }

        if callit {
            pgstat_init_function_usage(fcinfo, &mut fcusage);

            (*fcinfo).isnull = false;
            rsinfo.isDone = ExprSingleResult;
            result = FunctionCallInvoke!(fcinfo);
            *isNull = (*fcinfo).isnull;
            *isDone = rsinfo.isDone;

            pgstat_end_function_usage(&mut fcusage, rsinfo.isDone != ExprMultipleResult);
        } else {
            /* for a strict SRF, result for NULL is an empty set */
            result = 0 as Datum;
            *isNull = true;
            *isDone = ExprEndResult;
        }

        /* Which protocol does function want to use? */
        if rsinfo.returnMode == SFRM_ValuePerCall {
            if *isDone != ExprEndResult {
                /*
                 * Save the current argument values to re-use on the next call.
                 */
                if *isDone == ExprMultipleResult {
                    (*fcache).setArgsValid = true;
                    /* Register cleanup callback if we didn't already */
                    if !(*fcache).shutdown_reg {
                        RegisterExprContextCallback(
                            econtext,
                            Some(ShutdownSetExpr),
                            PointerGetDatum(fcache as *mut c_void),
                        );
                        (*fcache).shutdown_reg = true;
                    }
                }
            }
        } else if rsinfo.returnMode == SFRM_Materialize {
            /* check we're on the same page as the function author */
            if rsinfo.isDone != ExprSingleResult {
                ereport!(
                    ERROR,
                    "table-function protocol for materialize mode was not followed"
                );
            }
            if !rsinfo.setResult.is_null() {
                /* prepare to return values from the tuplestore */
                ExecPrepareTuplestoreResult(fcache, econtext, rsinfo.setResult, rsinfo.setDesc);
                /* loop back to top to start returning from tuplestore */
                continue;
            }
            /* if setResult was left null, treat it as empty set */
            *isDone = ExprEndResult;
            *isNull = true;
            result = 0 as Datum;
        } else {
            elog!(
                ERROR,
                "unrecognized table-function returnMode: {}",
                rsinfo.returnMode as c_int
            );
        }

        return result;
    }
}

/*
 * init_sexpr - initialize a SetExprState node during first use
 */
unsafe fn init_sexpr(
    foid: Oid,
    input_collation: Oid,
    node: *mut Expr,
    sexpr: *mut SetExprState,
    parent: *mut PlanState,
    sexprCxt: MemoryContext,
    allowSRF: bool,
    needDescForSRF: bool,
) {
    let aclresult: AclResult;
    let numargs: usize = list_length((*sexpr).args) as usize;

    /* Check permission to call function */
    aclresult = object_aclcheck(ProcedureRelationId, foid, GetUserId(), ACL_EXECUTE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(foid));
    }
    InvokeFunctionExecuteHook(foid);

    /*
     * Safety check on nargs.  Under normal circumstances this should never
     * fail, as parser should check sooner.  But possibly it might fail if
     * server has been compiled with FUNC_MAX_ARGS smaller than some functions
     * declared in pg_proc?
     */
    if list_length((*sexpr).args) > FUNC_MAX_ARGS {
        ereport!(ERROR, "cannot pass more than 100 arguments to a function");
    }

    /* Set up the primary fmgr lookup information */
    fmgr_info_cxt(foid, &raw mut (*sexpr).func, sexprCxt);
    fmgr_info_set_expr!((*sexpr).expr as *mut Node, &raw mut (*sexpr).func);

    /* Initialize the function call parameter struct as well */
    (*sexpr).fcinfo = palloc(SizeForFunctionCallInfo(numargs)) as FunctionCallInfo;
    InitFunctionCallInfoData!(
        (*sexpr).fcinfo,
        &raw mut (*sexpr).func,
        numargs as c_short,
        input_collation,
        null_mut(),
        null_mut()
    );

    /* If function returns set, check if that's allowed by caller */
    if (*sexpr).func.fn_retset && !allowSRF {
        if !parent.is_null() {
            let _errpos =
                executor_errposition((*parent).state as *mut EState, exprLocation(node as *mut Node));
        }
        ereport!(
            ERROR,
            "set-valued function called in context that cannot accept a set"
        );
    }

    /* Otherwise, caller should have marked the sexpr correctly */
    Assert!((*sexpr).func.fn_retset == (*sexpr).funcReturnsSet);

    /* If function returns set, prepare expected tuple descriptor */
    if (*sexpr).func.fn_retset && needDescForSRF {
        let functypclass: TypeFuncClass;
        let mut funcrettype: Oid = InvalidOid;
        let mut tupdesc: TupleDesc = null_mut();
        let oldcontext: MemoryContext;

        functypclass = get_expr_result_type(
            (*sexpr).func.fn_expr as *mut Node,
            &mut funcrettype,
            &mut tupdesc,
        );

        /* Must save tupdesc in sexpr's context */
        oldcontext = MemoryContextSwitchTo(sexprCxt);

        if functypclass == TYPEFUNC_COMPOSITE || functypclass == TYPEFUNC_COMPOSITE_DOMAIN {
            /* Composite data type, e.g. a table's row type */
            Assert!(!tupdesc.is_null());
            /* Must copy it out of typcache for safety */
            (*sexpr).funcResultDesc = CreateTupleDescCopy(tupdesc);
            (*sexpr).funcReturnsTuple = true;
        } else if functypclass == TYPEFUNC_SCALAR {
            /* Base data type, i.e. scalar */
            tupdesc = CreateTemplateTupleDesc(1);
            TupleDescInitEntry(tupdesc, 1 as AttrNumber, null(), funcrettype, -1, 0);
            (*sexpr).funcResultDesc = tupdesc;
            (*sexpr).funcReturnsTuple = false;
        } else if functypclass == TYPEFUNC_RECORD {
            /* This will work if function doesn't need an expectedDesc */
            (*sexpr).funcResultDesc = null_mut();
            (*sexpr).funcReturnsTuple = true;
        } else {
            /* Else, we will fail if function needs an expectedDesc */
            (*sexpr).funcResultDesc = null_mut();
        }

        MemoryContextSwitchTo(oldcontext);
    } else {
        (*sexpr).funcResultDesc = null_mut();
    }

    /* Initialize additional state */
    (*sexpr).funcResultStore = null_mut();
    (*sexpr).funcResultSlot = null_mut();
    (*sexpr).shutdown_reg = false;
}

/*
 * callback function in case a SetExprState needs to be shut down before it
 * has been run to completion
 */
unsafe fn ShutdownSetExpr(arg: Datum) {
    let sexpr: *mut SetExprState =
        castNode!(SetExprState, T_SetExprState, DatumGetPointer(arg) as *mut Node);

    /* If we have a slot, make sure it's let go of any tuplestore pointer */
    if !(*sexpr).funcResultSlot.is_null() {
        ExecClearTuple((*sexpr).funcResultSlot);
    }

    /* Release any open tuplestore */
    if !(*sexpr).funcResultStore.is_null() {
        tuplestore_end((*sexpr).funcResultStore);
    }
    (*sexpr).funcResultStore = null_mut();

    /* Clear any active set-argument state */
    (*sexpr).setArgsValid = false;

    /* execUtils will deregister the callback... */
    (*sexpr).shutdown_reg = false;
}

/*
 * Evaluate arguments for a function.
 */
unsafe fn ExecEvalFuncArgs(
    fcinfo: FunctionCallInfo,
    argList: *mut List,
    econtext: *mut ExprContext,
) {
    let mut i: c_int;

    i = 0;
    foreach!(arg, argList, {
        let argstate = lfirst(current_cell!(arg)) as *mut ExprState;

        (*(*fcinfo).args.as_mut_ptr().add(i as usize)).value = ExecEvalExpr(
            argstate,
            econtext,
            &mut (*(*fcinfo).args.as_mut_ptr().add(i as usize)).isnull,
        );
        i += 1;
    });

    Assert!(i == (*fcinfo).nargs as c_int);
}

/*
 *		ExecPrepareTuplestoreResult
 *
 * Subroutine for ExecMakeFunctionResultSet: prepare to extract rows from a
 * tuplestore function result.  We must set up a funcResultSlot (unless
 * already done in a previous call cycle) and verify that the function
 * returned the expected tuple descriptor.
 */
unsafe fn ExecPrepareTuplestoreResult(
    sexpr: *mut SetExprState,
    econtext: *mut ExprContext,
    resultStore: *mut Tuplestorestate,
    resultDesc: TupleDesc,
) {
    (*sexpr).funcResultStore = resultStore;

    if (*sexpr).funcResultSlot.is_null() {
        /* Create a slot so we can read data out of the tuplestore */
        let slotDesc: TupleDesc;

        let oldcontext = MemoryContextSwitchTo((*sexpr).func.fn_mcxt);

        /*
         * If we were not able to determine the result rowtype from context,
         * and the function didn't return a tupdesc, we have to fail.
         */
        if !(*sexpr).funcResultDesc.is_null() {
            slotDesc = (*sexpr).funcResultDesc;
        } else if !resultDesc.is_null() {
            /* don't assume resultDesc is long-lived */
            slotDesc = CreateTupleDescCopy(resultDesc);
        } else {
            ereport!(
                ERROR,
                "function returning setof record called in context that cannot accept type record"
            );
            #[allow(unreachable_code)]
            {
                slotDesc = null_mut(); /* keep compiler quiet */
            }
        }

        (*sexpr).funcResultSlot =
            MakeSingleTupleTableSlot(slotDesc, &TTSOpsMinimalTuple as *const TupleTableSlotOps);
        MemoryContextSwitchTo(oldcontext);
    }

    /*
     * If function provided a tupdesc, cross-check it.  We only really need to
     * do this for functions returning RECORD, but might as well do it always.
     */
    if !resultDesc.is_null() {
        if !(*sexpr).funcResultDesc.is_null() {
            tupledesc_match((*sexpr).funcResultDesc, resultDesc);
        }

        /*
         * If it is a dynamically-allocated TupleDesc, free it: it is
         * typically allocated in a per-query context, so we must avoid
         * leaking it across multiple usages.
         */
        if tupdesc_tdrefcount(resultDesc) == -1 {
            FreeTupleDesc(resultDesc);
        }
    }

    /* Register cleanup callback if we didn't already */
    if !(*sexpr).shutdown_reg {
        RegisterExprContextCallback(
            econtext,
            Some(ShutdownSetExpr),
            PointerGetDatum(sexpr as *mut c_void),
        );
        (*sexpr).shutdown_reg = true;
    }
}

/*
 * Check that function result tuple type (src_tupdesc) matches or can
 * be considered to match what the query expects (dst_tupdesc). If
 * they don't match, ereport.
 *
 * We really only care about number of attributes and data type.
 * Also, we can ignore type mismatch on columns that are dropped in the
 * destination type, so long as the physical storage matches.  This is
 * helpful in some cases involving out-of-date cached plans.
 */
unsafe fn tupledesc_match(dst_tupdesc: TupleDesc, src_tupdesc: TupleDesc) {
    let mut i: c_int;

    if tupdesc_natts(dst_tupdesc) != tupdesc_natts(src_tupdesc) {
        ereport!(
            ERROR,
            "function return row and query-specified return row do not match"
        );
    }

    i = 0;
    while i < tupdesc_natts(dst_tupdesc) {
        let dattr: Form_pg_attribute = TupleDescAttr(dst_tupdesc, i);
        let sattr: Form_pg_attribute = TupleDescAttr(src_tupdesc, i);

        if IsBinaryCoercible(attr_atttypid(sattr), attr_atttypid(dattr)) {
            i += 1;
            continue; /* no worries */
        }
        if !attr_attisdropped(dattr) {
            let _s_be = format_type_be(attr_atttypid(sattr));
            let _d_be = format_type_be(attr_atttypid(dattr));
            ereport!(
                ERROR,
                "function return row and query-specified return row do not match"
            );
        }

        if attr_attlen(dattr) != attr_attlen(sattr)
            || attr_attalign(dattr) != attr_attalign(sattr)
        {
            ereport!(
                ERROR,
                "function return row and query-specified return row do not match"
            );
        }

        i += 1;
    }
}
