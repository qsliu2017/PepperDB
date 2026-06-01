/*-------------------------------------------------------------------------
 *
 * nodeWindowAgg.c
 *	  routines to handle WindowAgg nodes.
 *
 * A WindowAgg node evaluates "window functions" across suitable partitions
 * of the input tuple set.  Any one WindowAgg works for just a single window
 * specification, though it can evaluate multiple window functions sharing
 * identical window specifications.  The input tuples are required to be
 * delivered in sorted order, with the PARTITION BY columns (if any) as
 * major sort keys and the ORDER BY columns (if any) as minor sort keys.
 * (The planner generates a stack of WindowAggs with intervening Sort nodes
 * as needed, if a query involves more than one window specification.)
 *
 * Since window functions can require access to any or all of the rows in
 * the current partition, we accumulate rows of the partition into a
 * tuplestore.  The window functions are called using the WindowObject API
 * so that they can access those rows as needed.
 *
 * We also support using plain aggregate functions as window functions.
 * For these, the regular Agg-node environment is emulated for each partition.
 * As required by the SQL spec, the output represents the value of the
 * aggregate function over all rows in the current row's window frame.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeWindowAgg.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]
#![allow(unreachable_code)]
#![allow(unused_unsafe)]

use crate::prelude::*;

use std::ffi::{c_int, c_void};
use std::mem::size_of;

use crate::nodes::pg_list::{List, NIL};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::primnodes::{Expr, WindowFunc};
use crate::nodes::plannodes::{Plan, WindowAgg};
use crate::nodes::execnodes::{
    EState, ExprContext, ExprState, ProjectionInfo, PlanState, ScanState,
    TupleTableSlot, Tuplestorestate,
    WindowAggState, WindowAggStatus,
    WindowAggStatus::*,
    WindowObjectData, WindowStatePerFuncData, WindowStatePerAggData,
    WindowStatePerFunc, WindowStatePerAgg,
};
use crate::nodes::execnodes::{
    WindowFuncExprState,
};
use crate::executor::executor::EXEC_FLAG_BACKWARD;
use crate::access::htup_details::HeapTuple;
use crate::nodes::pg_list::{list_head, lnext};
use crate::pg_config_manual::FUNC_MAX_ARGS;
use crate::castNode;

/* CHECK_FOR_INTERRUPTS is a macro in C (miscadmin.h); local no-op shim per port convention. */
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{}};
}
use crate::executor::executor::{
    ExecInitNode, ExecEndNode, ExecProcNode, ExecReScan,
    ExecInitQual, ExecInitExpr,
    ExecEvalExpr, ExecEvalExprSwitchContext,
    ExecQual, ExecQualAndReset, ExecProject,
    ExecInitResultTupleSlotTL, ExecInitExtraTupleSlot,
    ExecAssignExprContext, ExecAssignProjectionInfo,
    ExecCreateScanSlotFromOuterPlan,
};
use crate::executor::execGrouping::execTuplesMatchPrepare;
use crate::executor::execTuples::{TTSOpsMinimalTuple, TTSOpsVirtual};
use crate::executor::tuptable::{ExecClearTuple, ExecCopySlot, TupIsNull};
use crate::utils::fmgr::{
    FmgrInfo, FunctionCallInfo, FunctionCallInfoBaseData,
    FunctionCall5Coll,
    fmgr_info, fmgr_info_cxt,
    SizeForFunctionCallInfo,
};
use crate::utils::cache::lsyscache::{
    get_typlenbyval, get_func_name, ObjectIdGetDatum, OidInputFunctionCall,
    getTypeInputInfo,
};
use crate::utils::cache::syscache::{
    SearchSysCache1, ReleaseSysCache, SysCacheGetAttr,
};
use crate::utils::adt::expandeddatum::{
    DatumGetEOHP, DeleteExpandedObject,
};
use crate::miscadmin::GetUserId;
use crate::postgres::Datum;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::c::{int16, int32, int64, uint32, Size};
use crate::catalog::aclchk::object_aclcheck;
use crate::catalog::objectaddress_impl::format_procedure;
use crate::optimizer::optimizer::contain_volatile_functions;
use crate::optimizer::util::clauses::contain_subplans;
use crate::optimizer::prep::prepagg::resolve_aggregate_transtype;
use crate::parser::parse_agg::{build_aggregate_transfn_expr, build_aggregate_finalfn_expr};
use crate::nodes::nodeFuncs::exprType;
use crate::windowapi::{
    WindowObject, WindowObjectIsValid,
    WINDOW_SEEK_CURRENT, WINDOW_SEEK_HEAD, WINDOW_SEEK_TAIL,
};
use crate::{InitFunctionCallInfoData, FunctionCallInvoke, fmgr_info_set_expr, LOCAL_FCINFO};

/*
 * In C, `WindowObjectData` is a full struct, so the API helpers dereference a
 * `WindowObject winobj` argument directly. Here the public `WindowObjectData`
 * (nodes/execnodes.rs) is an opaque stub and the real fields live in the private
 * `WindowObjectDataFull` below. This macro spells the field-access bridge: it
 * casts the opaque `*mut WindowObjectData` to `*mut WindowObjectDataFull` via the
 * `winobj()` helper so `(*winobj!(winobj)).field` resolves. Local (not exported).
 */
macro_rules! winobj {
    ($winobj:expr) => {
        winobj($winobj)
    };
}

/* FRAMEOPTION_* flags -- from parsenodes.h */
pub const FRAMEOPTION_NONDEFAULT: c_int = 0x00001;
pub const FRAMEOPTION_RANGE: c_int = 0x00002;
pub const FRAMEOPTION_ROWS: c_int = 0x00004;
pub const FRAMEOPTION_GROUPS: c_int = 0x00008;
pub const FRAMEOPTION_BETWEEN: c_int = 0x00010;
pub const FRAMEOPTION_START_UNBOUNDED_PRECEDING: c_int = 0x00020;
pub const FRAMEOPTION_END_UNBOUNDED_PRECEDING: c_int = 0x00040;
pub const FRAMEOPTION_START_UNBOUNDED_FOLLOWING: c_int = 0x00080;
pub const FRAMEOPTION_END_UNBOUNDED_FOLLOWING: c_int = 0x00100;
pub const FRAMEOPTION_START_CURRENT_ROW: c_int = 0x00200;
pub const FRAMEOPTION_END_CURRENT_ROW: c_int = 0x00400;
pub const FRAMEOPTION_START_OFFSET_PRECEDING: c_int = 0x00800;
pub const FRAMEOPTION_END_OFFSET_PRECEDING: c_int = 0x01000;
pub const FRAMEOPTION_START_OFFSET_FOLLOWING: c_int = 0x02000;
pub const FRAMEOPTION_END_OFFSET_FOLLOWING: c_int = 0x04000;
pub const FRAMEOPTION_START_OFFSET: c_int =
    FRAMEOPTION_START_OFFSET_PRECEDING | FRAMEOPTION_START_OFFSET_FOLLOWING;
pub const FRAMEOPTION_END_OFFSET: c_int =
    FRAMEOPTION_END_OFFSET_PRECEDING | FRAMEOPTION_END_OFFSET_FOLLOWING;
pub const FRAMEOPTION_EXCLUDE_CURRENT_ROW: c_int = 0x08000;
pub const FRAMEOPTION_EXCLUDE_GROUP: c_int = 0x10000;
pub const FRAMEOPTION_EXCLUDE_TIES: c_int = 0x20000;
pub const FRAMEOPTION_EXCLUSION: c_int =
    FRAMEOPTION_EXCLUDE_CURRENT_ROW | FRAMEOPTION_EXCLUDE_GROUP | FRAMEOPTION_EXCLUDE_TIES;

/* ------------------------------------------------------------------
 * Stubs for subsystems not yet ported
 * ------------------------------------------------------------------ */

/// TODO(pg-port): tuplestore.h
unsafe fn tuplestore_begin_heap(
    _randomaccess: bool,
    _interXact: bool,
    _maxKBytes: c_int,
) -> *mut Tuplestorestate {
    std::ptr::null_mut() // TODO(pg-port): tuplestore
}
unsafe fn tuplestore_set_eflags(_state: *mut Tuplestorestate, _eflags: c_int) {
    // TODO(pg-port): tuplestore
}
unsafe fn tuplestore_alloc_read_pointer(
    _state: *mut Tuplestorestate,
    _eflags: c_int,
) -> c_int {
    0 // TODO(pg-port): tuplestore
}
unsafe fn tuplestore_select_read_pointer(_state: *mut Tuplestorestate, _ptr: c_int) {
    // TODO(pg-port): tuplestore
}
unsafe fn tuplestore_puttupleslot(_state: *mut Tuplestorestate, _slot: *mut TupleTableSlot) {
    // TODO(pg-port): tuplestore
}
unsafe fn tuplestore_gettupleslot(
    _state: *mut Tuplestorestate,
    _forward: bool,
    _copy: bool,
    _slot: *mut TupleTableSlot,
) -> bool {
    false // TODO(pg-port): tuplestore
}
unsafe fn tuplestore_skiptuples(
    _state: *mut Tuplestorestate,
    _ntuples: int64,
    _forward: bool,
) -> bool {
    false // TODO(pg-port): tuplestore
}
unsafe fn tuplestore_advance(_state: *mut Tuplestorestate, _forward: bool) -> bool {
    false // TODO(pg-port): tuplestore
}
unsafe fn tuplestore_clear(_state: *mut Tuplestorestate) {
    // TODO(pg-port): tuplestore
}
unsafe fn tuplestore_end(_state: *mut Tuplestorestate) {
    // TODO(pg-port): tuplestore
}
unsafe fn tuplestore_in_memory(_state: *mut Tuplestorestate) -> bool {
    true // TODO(pg-port): tuplestore
}
unsafe fn tuplestore_trim(_state: *mut Tuplestorestate) {
    // TODO(pg-port): tuplestore
}

/// TODO(pg-port): utils/guc.h work_mem
static mut work_mem: c_int = 4096;

/// TODO(pg-port): utils/memutils.h AllocSetContextCreate
unsafe fn AllocSetContextCreate(
    _parent: MemoryContext,
    _name: *const std::os::raw::c_char,
    _minContextSize: Size,
    _initBlockSize: Size,
    _maxBlockSize: Size,
) -> MemoryContext {
    std::ptr::null_mut() // TODO(pg-port): utils/memutils
}
/// TODO(pg-port): utils/memutils.h ALLOCSET_DEFAULT_SIZES expansion
const ALLOCSET_DEFAULT_MINSIZE: Size = 0;
const ALLOCSET_DEFAULT_INITSIZE: Size = 8 * 1024;
const ALLOCSET_DEFAULT_MAXSIZE: Size = 8 * 1024 * 1024;

/// TODO(pg-port): utils/memutils.h MemoryContextAllocZero
unsafe fn MemoryContextAllocZero(_ctx: MemoryContext, _sz: Size) -> *mut c_void {
    std::ptr::null_mut() // TODO(pg-port): utils/memutils
}
/// TODO(pg-port): utils/memutils.h MemoryContextGetParent
unsafe fn MemoryContextGetParent(_ctx: MemoryContext) -> MemoryContext {
    std::ptr::null_mut() // TODO(pg-port): utils/memutils
}

/// TODO(pg-port): utils/expandeddatum.h DatumIsReadWriteExpandedObject
unsafe fn DatumIsReadWriteExpandedObject(_d: Datum, _isnull: bool, _typlen: int16) -> bool {
    false // TODO(pg-port): utils/expandeddatum
}

/// TODO(pg-port): access/htup_details.h HeapTupleIsValid
unsafe fn HeapTupleIsValid(tup: HeapTuple) -> bool {
    !tup.is_null() // TODO(pg-port): access/htup_details
}

/* utils/expandeddatum.h: MakeExpandedObjectReadOnly(d, isnull, typlen) */
unsafe fn MakeExpandedObjectReadOnly(d: Datum, _isnull: bool, _typlen: int16) -> Datum {
    d // TODO(pg-port): utils/adt/expandeddatum - faithful R/W -> R/O conversion
}

/// TODO(pg-port): utils/datum.h datumCopy
unsafe fn datumCopy(value: Datum, typByVal: bool, typLen: int16) -> Datum {
    value // TODO(pg-port): utils/datum
}

/// TODO(pg-port): nodes/nodeFuncs.h equal
unsafe fn equal(a: *const c_void, b: *const c_void) -> bool {
    false // TODO(pg-port): nodes/equalfuncs
}

/// TODO(pg-port): utils/cache/lsyscache.h slot_getattr
unsafe fn slot_getattr(
    _slot: *mut TupleTableSlot,
    _attnum: c_int,
    _isnull: *mut bool,
) -> Datum {
    0 // TODO(pg-port): access/common/slot
}

/// TODO(pg-port): nodes/pg_list.h list_nth
unsafe fn list_nth(list: *mut List, n: c_int) -> *mut c_void {
    std::ptr::null_mut() // TODO(pg-port): nodes/pg_list
}
/// TODO(pg-port): nodes/pg_list.h list_length
unsafe fn list_length(list: *const List) -> c_int {
    0 // TODO(pg-port): nodes/pg_list
}
/// TODO(pg-port): nodes/pg_list.h lfirst
unsafe fn lfirst(cell: *mut c_void) -> *mut c_void {
    std::ptr::null_mut() // TODO(pg-port): nodes/pg_list
}

/// TODO(pg-port): nodes/nodes.h makeNode(WindowAggState)
unsafe fn makeNode_WindowAggState() -> *mut WindowAggState {
    std::ptr::null_mut() // TODO(pg-port): nodes/nodes
}
/// TODO(pg-port): nodes/nodes.h makeNode(WindowObjectData)
unsafe fn makeNode_WindowObjectData() -> *mut WindowObjectData {
    std::ptr::null_mut() // TODO(pg-port): nodes/nodes
}

/// TODO(pg-port): executor/executor.h InstrCountFiltered1
unsafe fn InstrCountFiltered1(_node: *mut WindowAggState, _n: c_int) {
    // TODO(pg-port): instrument
}
/// TODO(pg-port): executor/executor.h outerPlanState / outerPlan
unsafe fn outerPlanState(node: *mut WindowAggState) -> *mut PlanState {
    std::ptr::null_mut() // TODO(pg-port): executor/executor
}
unsafe fn outerPlan(node: *mut WindowAgg) -> *mut Plan {
    std::ptr::null_mut() // TODO(pg-port): executor/executor
}
/// TODO(pg-port): executor/executor.h ResetExprContext
unsafe fn ResetExprContext(_econtext: *mut ExprContext) {
    // TODO(pg-port): executor/executor
}

/// TODO(pg-port): catalog/pg_aggregate.h Form_pg_aggregate, FormData_pg_aggregate
#[repr(C)]
struct FormData_pg_aggregate {
    aggfnoid: Oid,
    aggkind: i8,
    aggnumdirectargs: int16,
    aggtransfn: Oid,
    aggfinalfn: Oid,
    aggcombinefn: Oid,
    aggserialfn: Oid,
    aggdeserialfn: Oid,
    aggmtransfn: Oid,
    aggminvtransfn: Oid,
    aggmfinalfn: Oid,
    aggfinalextra: bool,
    aggmfinalextra: bool,
    aggfinalmodify: i8,
    aggmfinalmodify: i8,
    aggsortop: Oid,
    aggtranstype: Oid,
    aggmtranstype: Oid,
}
type Form_pg_aggregate = *mut FormData_pg_aggregate;
/// TODO(pg-port): catalog/pg_proc.h Form_pg_proc
#[repr(C)]
struct FormData_pg_proc {
    proname: [u8; 64],
    proowner: Oid,
}
type Form_pg_proc = *mut FormData_pg_proc;
/// TODO(pg-port): access/htup_details.h GETSTRUCT
unsafe fn GETSTRUCT<T>(tup: HeapTuple) -> *mut T {
    std::ptr::null_mut() // TODO(pg-port): access/htup_details
}
/// TODO(pg-port): catalog/objectaccess.h InvokeFunctionExecuteHook
unsafe fn InvokeFunctionExecuteHook(_objectId: Oid) {
    // TODO(pg-port): catalog/objectaccess
}
/// TODO(pg-port): utils/acl.h AclResult / ACLCHECK_OK
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;
type AclMode = u64;
const ACL_EXECUTE: AclMode = 1 << 3;
unsafe fn aclcheck_error(
    _aclerr: AclResult,
    _objtype: c_int,
    _objectname: *const std::os::raw::c_char,
) {
    // TODO(pg-port): utils/acl
}
const OBJECT_FUNCTION: c_int = 38;
/// TODO(pg-port): catalog/pg_class_d.h ProcedureRelationId
const ProcedureRelationId: Oid = 1255;
/// TODO(pg-port): catalog/pg_aggregate.h Anum_pg_aggregate_agginitval / aggminitval
const Anum_pg_aggregate_agginitval: c_int = 15;
const Anum_pg_aggregate_aggminitval: c_int = 20;
/// TODO(pg-port): catalog/pg_aggregate.h AGGMODIFY_READ_ONLY
const AGGMODIFY_READ_ONLY: i8 = b'r' as i8;
/// TODO(pg-port): syscache AGGFNOID / PROCOID
const AGGFNOID: c_int = 1;
const PROCOID: c_int = 28;

/*
 * All the window function APIs are called with this object, which is passed
 * to window functions as fcinfo->context.
 */
/* WindowObjectData is defined privately here; the public opaque stub is in
 * src/nodes/execnodes.rs.  We reuse that opaque type and add real fields
 * via a local alias so the rest of this file can access them. */
#[repr(C)]
struct WindowObjectDataFull {
    type_: NodeTag,
    winstate: *mut WindowAggState, /* parent WindowAggState */
    argstates: *mut List,          /* ExprState trees for fn's arguments */
    localmem: *mut c_void,         /* WinGetPartitionLocalMemory's chunk */
    markptr: c_int,                /* tuplestore mark pointer for this fn */
    readptr: c_int,                /* tuplestore read pointer for this fn */
    markpos: int64,                /* row that markptr is positioned on */
    seekpos: int64,                /* row that readptr is positioned on */
}
/* convenience cast */
#[inline(always)]
unsafe fn winobj(p: *mut WindowObjectData) -> *mut WindowObjectDataFull {
    p as *mut WindowObjectDataFull
}

/*
 * We have one WindowStatePerFunc struct for each window function and
 * window aggregate handled by this node.
 */
#[repr(C)]
struct WindowStatePerFuncDataFull {
    /* Links to WindowFunc expr and state nodes this working state is for */
    wfuncstate: *mut WindowFuncExprState,
    wfunc: *mut WindowFunc,

    numArguments: c_int, /* number of arguments */

    flinfo: FmgrInfo, /* fmgr lookup data for window function */

    winCollation: Oid, /* collation derived for window function */

    /*
     * We need the len and byval info for the result of each function in order
     * to know how to copy/delete values.
     */
    resulttypeLen: int16,
    resulttypeByVal: bool,

    plain_agg: bool, /* is it just a plain aggregate function? */
    aggno: c_int,    /* if so, index of its WindowStatePerAggData */

    winobj: *mut WindowObjectData, /* object used in window function API */
}
/* convenience cast */
#[inline(always)]
unsafe fn perfunc(p: WindowStatePerFunc) -> *mut WindowStatePerFuncDataFull {
    p as *mut WindowStatePerFuncDataFull
}

/*
 * For plain aggregate window functions, we also have one of these.
 */
#[repr(C)]
struct WindowStatePerAggDataFull {
    /* Oids of transition functions */
    transfn_oid: Oid,
    invtransfn_oid: Oid, /* may be InvalidOid */
    finalfn_oid: Oid,    /* may be InvalidOid */

    /*
     * fmgr lookup data for transition functions --- only valid when
     * corresponding oid is not InvalidOid.  Note in particular that fn_strict
     * flags are kept here.
     */
    transfn: FmgrInfo,
    invtransfn: FmgrInfo,
    finalfn: FmgrInfo,

    numFinalArgs: c_int, /* number of arguments to pass to finalfn */

    /*
     * initial value from pg_aggregate entry
     */
    initValue: Datum,
    initValueIsNull: bool,

    /*
     * cached value for current frame boundaries
     */
    resultValue: Datum,
    resultValueIsNull: bool,

    /*
     * We need the len and byval info for the agg's input, result, and
     * transition data types in order to know how to copy/delete values.
     */
    inputtypeLen: int16,
    resulttypeLen: int16,
    transtypeLen: int16,
    inputtypeByVal: bool,
    resulttypeByVal: bool,
    transtypeByVal: bool,

    wfuncno: c_int, /* index of associated WindowStatePerFuncData */

    /* Context holding transition value and possibly other subsidiary data */
    aggcontext: MemoryContext, /* may be private, or winstate->aggcontext */

    /* Current transition value */
    transValue: Datum, /* current transition value */
    transValueIsNull: bool,

    transValueCount: int64, /* number of currently-aggregated rows */

    /* Data local to eval_windowaggregates() */
    restart: bool, /* need to restart this agg in this cycle? */
}
/* convenience cast */
#[inline(always)]
unsafe fn peragg_full(p: WindowStatePerAgg) -> *mut WindowStatePerAggDataFull {
    p as *mut WindowStatePerAggDataFull
}

/* helper: index into perfunc array */
#[inline(always)]
unsafe fn perfunc_at(winstate: *mut WindowAggState, i: c_int) -> *mut WindowStatePerFuncDataFull {
    ((*winstate).perfunc as *mut WindowStatePerFuncDataFull).add(i as usize)
}
/* helper: index into peragg array */
#[inline(always)]
unsafe fn peragg_at(winstate: *mut WindowAggState, i: c_int) -> *mut WindowStatePerAggDataFull {
    ((*winstate).peragg as *mut WindowStatePerAggDataFull).add(i as usize)
}

/*
 * initialize_windowaggregate
 * parallel to initialize_aggregates in nodeAgg.c
 */
unsafe fn initialize_windowaggregate(
    winstate: *mut WindowAggState,
    perfuncstate: *mut WindowStatePerFuncDataFull,
    peraggstate: *mut WindowStatePerAggDataFull,
) {
    let oldContext: MemoryContext;

    /*
     * If we're using a private aggcontext, we may reset it here.  But if the
     * context is shared, we don't know which other aggregates may still need
     * it, so we must leave it to the caller to reset at an appropriate time.
     */
    if (*peraggstate).aggcontext != (*winstate).aggcontext {
        MemoryContextReset((*peraggstate).aggcontext);
    }

    if (*peraggstate).initValueIsNull {
        (*peraggstate).transValue = (*peraggstate).initValue;
    } else {
        let oldContext = MemoryContextSwitchTo((*peraggstate).aggcontext);
        (*peraggstate).transValue = datumCopy(
            (*peraggstate).initValue,
            (*peraggstate).transtypeByVal,
            (*peraggstate).transtypeLen,
        );
        MemoryContextSwitchTo(oldContext);
    }
    (*peraggstate).transValueIsNull = (*peraggstate).initValueIsNull;
    (*peraggstate).transValueCount = 0;
    (*peraggstate).resultValue = 0 as Datum;
    (*peraggstate).resultValueIsNull = true;
}

/*
 * advance_windowaggregate
 * parallel to advance_aggregates in nodeAgg.c
 */
unsafe fn advance_windowaggregate(
    winstate: *mut WindowAggState,
    perfuncstate: *mut WindowStatePerFuncDataFull,
    peraggstate: *mut WindowStatePerAggDataFull,
) {
    LOCAL_FCINFO!(fcinfo, FUNC_MAX_ARGS);
    let wfuncstate: *mut WindowFuncExprState = (*perfuncstate).wfuncstate;
    let numArguments: c_int = (*perfuncstate).numArguments;
    let mut newVal: Datum;
    let econtext: *mut ExprContext = (*winstate).tmpcontext;
    let filter: *mut ExprState = (*wfuncstate).aggfilter;

    let oldContext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    /* Skip anything FILTERed out */
    if !filter.is_null() {
        let mut isnull: bool = false;
        let res: Datum = ExecEvalExpr(filter, econtext, &mut isnull);
        if isnull || !DatumGetBool(res) {
            MemoryContextSwitchTo(oldContext);
            return;
        }
    }

    /* We start from 1, since the 0th arg will be the transition value */
    let mut i: c_int = 1;
    /* foreach(arg, wfuncstate->args) */
    {
        let args: *mut List = (*wfuncstate).args;
        let mut lc = list_head(args);
        while !lc.is_null() {
            let argstate: *mut ExprState = lfirst(lc as *mut c_void) as *mut ExprState;
            (*(*fcinfo).args.as_mut_ptr().add(i as usize)).value =
                ExecEvalExpr(argstate, econtext, &mut (*(*fcinfo).args.as_mut_ptr().add(i as usize)).isnull);
            i += 1;
            lc = lnext(args, lc);
        }
    }

    if (*peraggstate).transfn.fn_strict {
        /*
         * For a strict transfn, nothing happens when there's a NULL input; we
         * just keep the prior transValue.  Note transValueCount doesn't
         * change either.
         */
        let mut j: c_int = 1;
        while j <= numArguments {
            if (*(*fcinfo).args.as_mut_ptr().add(j as usize)).isnull {
                MemoryContextSwitchTo(oldContext);
                return;
            }
            j += 1;
        }

        /*
         * For strict transition functions with initial value NULL we use the
         * first non-NULL input as the initial state.  (We already checked
         * that the agg's input type is binary-compatible with its transtype,
         * so straight copy here is OK.)
         *
         * We must copy the datum into aggcontext if it is pass-by-ref.  We do
         * not need to pfree the old transValue, since it's NULL.
         */
        if (*peraggstate).transValueCount == 0 && (*peraggstate).transValueIsNull {
            MemoryContextSwitchTo((*peraggstate).aggcontext);
            (*peraggstate).transValue = datumCopy(
                (*(*fcinfo).args.as_mut_ptr().add(1)).value,
                (*peraggstate).transtypeByVal,
                (*peraggstate).transtypeLen,
            );
            (*peraggstate).transValueIsNull = false;
            (*peraggstate).transValueCount = 1;
            MemoryContextSwitchTo(oldContext);
            return;
        }

        if (*peraggstate).transValueIsNull {
            /*
             * Don't call a strict function with NULL inputs.  Note it is
             * possible to get here despite the above tests, if the transfn is
             * strict *and* returned a NULL on a prior cycle.  If that happens
             * we will propagate the NULL all the way to the end.  That can
             * only happen if there's no inverse transition function, though,
             * since we disallow transitions back to NULL when there is one.
             */
            MemoryContextSwitchTo(oldContext);
            Assert!(!OidIsValid((*peraggstate).invtransfn_oid));
            return;
        }
    }

    /*
     * OK to call the transition function.  Set winstate->curaggcontext while
     * calling it, for possible use by AggCheckCallContext.
     */
    InitFunctionCallInfoData!(
        fcinfo,
        &mut (*peraggstate).transfn,
        (numArguments + 1) as i16,
        (*perfuncstate).winCollation,
        winstate as *mut Node,
        std::ptr::null_mut()
    );
    (*(*fcinfo).args.as_mut_ptr().add(0)).value = (*peraggstate).transValue;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = (*peraggstate).transValueIsNull;
    (*winstate).curaggcontext = (*peraggstate).aggcontext;
    newVal = FunctionCallInvoke!(fcinfo);
    (*winstate).curaggcontext = std::ptr::null_mut();

    /*
     * Moving-aggregate transition functions must not return null, see
     * advance_windowaggregate_base().
     */
    if (*fcinfo).isnull && OidIsValid((*peraggstate).invtransfn_oid) {
        ereport!(ERROR, errmsg!("moving-aggregate transition function must not return null") /* C also: errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED) */);
    }

    /*
     * We must track the number of rows included in transValue, since to
     * remove the last input, advance_windowaggregate_base() mustn't call the
     * inverse transition function, but simply reset transValue back to its
     * initial value.
     */
    (*peraggstate).transValueCount += 1;

    /*
     * If pass-by-ref datatype, must copy the new value into aggcontext and
     * free the prior transValue.  But if transfn returned a pointer to its
     * first input, we don't need to do anything.  Also, if transfn returned a
     * pointer to a R/W expanded object that is already a child of the
     * aggcontext, assume we can adopt that value without copying it.  (See
     * comments for ExecAggCopyTransValue, which this code duplicates.)
     */
    if !(*peraggstate).transtypeByVal
        && DatumGetPointer(newVal) != DatumGetPointer((*peraggstate).transValue)
    {
        if !(*fcinfo).isnull {
            MemoryContextSwitchTo((*peraggstate).aggcontext);
            if DatumIsReadWriteExpandedObject(newVal, false, (*peraggstate).transtypeLen)
                && MemoryContextGetParent((*DatumGetEOHP(newVal)).eoh_context)
                    == CurrentMemoryContext
            {
                /* do nothing */
            } else {
                newVal = datumCopy(
                    newVal,
                    (*peraggstate).transtypeByVal,
                    (*peraggstate).transtypeLen,
                );
            }
        }
        if !(*peraggstate).transValueIsNull {
            if DatumIsReadWriteExpandedObject(
                (*peraggstate).transValue,
                false,
                (*peraggstate).transtypeLen,
            ) {
                DeleteExpandedObject((*peraggstate).transValue);
            } else {
                pfree(DatumGetPointer((*peraggstate).transValue) as *mut c_void);
            }
        }
    }

    MemoryContextSwitchTo(oldContext);
    (*peraggstate).transValue = newVal;
    (*peraggstate).transValueIsNull = (*fcinfo).isnull;
}

/*
 * advance_windowaggregate_base
 * Remove the oldest tuple from an aggregation.
 *
 * This is very much like advance_windowaggregate, except that we will call
 * the inverse transition function (which caller must have checked is
 * available).
 *
 * Returns true if we successfully removed the current row from this
 * aggregate, false if not (in the latter case, caller is responsible
 * for cleaning up by restarting the aggregation).
 */
unsafe fn advance_windowaggregate_base(
    winstate: *mut WindowAggState,
    perfuncstate: *mut WindowStatePerFuncDataFull,
    peraggstate: *mut WindowStatePerAggDataFull,
) -> bool {
    LOCAL_FCINFO!(fcinfo, FUNC_MAX_ARGS);
    let wfuncstate: *mut WindowFuncExprState = (*perfuncstate).wfuncstate;
    let numArguments: c_int = (*perfuncstate).numArguments;
    let mut newVal: Datum;
    let econtext: *mut ExprContext = (*winstate).tmpcontext;
    let filter: *mut ExprState = (*wfuncstate).aggfilter;

    let oldContext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    /* Skip anything FILTERed out */
    if !filter.is_null() {
        let mut isnull: bool = false;
        let res: Datum = ExecEvalExpr(filter, econtext, &mut isnull);
        if isnull || !DatumGetBool(res) {
            MemoryContextSwitchTo(oldContext);
            return true;
        }
    }

    /* We start from 1, since the 0th arg will be the transition value */
    let mut i: c_int = 1;
    {
        let args: *mut List = (*wfuncstate).args;
        let mut lc = list_head(args);
        while !lc.is_null() {
            let argstate: *mut ExprState = lfirst(lc as *mut c_void) as *mut ExprState;
            (*(*fcinfo).args.as_mut_ptr().add(i as usize)).value =
                ExecEvalExpr(argstate, econtext, &mut (*(*fcinfo).args.as_mut_ptr().add(i as usize)).isnull);
            i += 1;
            lc = lnext(args, lc);
        }
    }

    if (*peraggstate).invtransfn.fn_strict {
        /*
         * For a strict (inv)transfn, nothing happens when there's a NULL
         * input; we just keep the prior transValue.  Note transValueCount
         * doesn't change either.
         */
        let mut j: c_int = 1;
        while j <= numArguments {
            if (*(*fcinfo).args.as_mut_ptr().add(j as usize)).isnull {
                MemoryContextSwitchTo(oldContext);
                return true;
            }
            j += 1;
        }
    }

    /* There should still be an added but not yet removed value */
    Assert!((*peraggstate).transValueCount > 0);

    /*
     * In moving-aggregate mode, the state must never be NULL, except possibly
     * before any rows have been aggregated (which is surely not the case at
     * this point).  This restriction allows us to interpret a NULL result
     * from the inverse function as meaning "sorry, can't do an inverse
     * transition in this case".  We already checked this in
     * advance_windowaggregate, but just for safety, check again.
     */
    if (*peraggstate).transValueIsNull {
        elog!(
            ERROR,
            "aggregate transition value is NULL before inverse transition"
        );
    }

    /*
     * We mustn't use the inverse transition function to remove the last
     * input.  Doing so would yield a non-NULL state, whereas we should be in
     * the initial state afterwards which may very well be NULL.  So instead,
     * we simply re-initialize the aggregate in this case.
     */
    if (*peraggstate).transValueCount == 1 {
        MemoryContextSwitchTo(oldContext);
        initialize_windowaggregate(
            winstate,
            &mut *perfunc_at(winstate, (*peraggstate).wfuncno),
            peraggstate,
        );
        return true;
    }

    /*
     * OK to call the inverse transition function.  Set
     * winstate->curaggcontext while calling it, for possible use by
     * AggCheckCallContext.
     */
    InitFunctionCallInfoData!(
        fcinfo,
        &mut (*peraggstate).invtransfn,
        (numArguments + 1) as i16,
        (*perfuncstate).winCollation,
        winstate as *mut Node,
        std::ptr::null_mut()
    );
    (*(*fcinfo).args.as_mut_ptr().add(0)).value = (*peraggstate).transValue;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = (*peraggstate).transValueIsNull;
    (*winstate).curaggcontext = (*peraggstate).aggcontext;
    newVal = FunctionCallInvoke!(fcinfo);
    (*winstate).curaggcontext = std::ptr::null_mut();

    /*
     * If the function returns NULL, report failure, forcing a restart.
     */
    if (*fcinfo).isnull {
        MemoryContextSwitchTo(oldContext);
        return false;
    }

    /* Update number of rows included in transValue */
    (*peraggstate).transValueCount -= 1;

    /*
     * If pass-by-ref datatype, must copy the new value into aggcontext and
     * free the prior transValue.  But if invtransfn returned a pointer to its
     * first input, we don't need to do anything.  Also, if invtransfn
     * returned a pointer to a R/W expanded object that is already a child of
     * the aggcontext, assume we can adopt that value without copying it. (See
     * comments for ExecAggCopyTransValue, which this code duplicates.)
     *
     * Note: the checks for null values here will never fire, but it seems
     * best to have this stanza look just like advance_windowaggregate.
     */
    if !(*peraggstate).transtypeByVal
        && DatumGetPointer(newVal) != DatumGetPointer((*peraggstate).transValue)
    {
        if !(*fcinfo).isnull {
            MemoryContextSwitchTo((*peraggstate).aggcontext);
            if DatumIsReadWriteExpandedObject(newVal, false, (*peraggstate).transtypeLen)
                && MemoryContextGetParent((*DatumGetEOHP(newVal)).eoh_context)
                    == CurrentMemoryContext
            {
                /* do nothing */
            } else {
                newVal = datumCopy(
                    newVal,
                    (*peraggstate).transtypeByVal,
                    (*peraggstate).transtypeLen,
                );
            }
        }
        if !(*peraggstate).transValueIsNull {
            if DatumIsReadWriteExpandedObject(
                (*peraggstate).transValue,
                false,
                (*peraggstate).transtypeLen,
            ) {
                DeleteExpandedObject((*peraggstate).transValue);
            } else {
                pfree(DatumGetPointer((*peraggstate).transValue) as *mut c_void);
            }
        }
    }

    MemoryContextSwitchTo(oldContext);
    (*peraggstate).transValue = newVal;
    (*peraggstate).transValueIsNull = (*fcinfo).isnull;

    return true;
}

/*
 * finalize_windowaggregate
 * parallel to finalize_aggregate in nodeAgg.c
 */
unsafe fn finalize_windowaggregate(
    winstate: *mut WindowAggState,
    perfuncstate: *mut WindowStatePerFuncDataFull,
    peraggstate: *mut WindowStatePerAggDataFull,
    result: *mut Datum,
    isnull: *mut bool,
) {
    let oldContext = MemoryContextSwitchTo(
        (*(*winstate).ss.ps.ps_ExprContext).ecxt_per_tuple_memory,
    );

    /*
     * Apply the agg's finalfn if one is provided, else return transValue.
     */
    if OidIsValid((*peraggstate).finalfn_oid) {
        LOCAL_FCINFO!(fcinfo, FUNC_MAX_ARGS);
        /* LOCAL_FCINFO uses name "fcinfodata" in C; the macro exposes it as fcinfo */
        let numFinalArgs: c_int = (*peraggstate).numFinalArgs;
        let mut anynull: bool;
        let mut i: c_int;

        InitFunctionCallInfoData!(
            fcinfo,
            &mut (*peraggstate).finalfn,
            numFinalArgs as i16,
            (*perfuncstate).winCollation,
            winstate as *mut Node,
            std::ptr::null_mut()
        );
        (*(*fcinfo).args.as_mut_ptr().add(0)).value = MakeExpandedObjectReadOnly(
            (*peraggstate).transValue,
            (*peraggstate).transValueIsNull,
            (*peraggstate).transtypeLen,
        );
        (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = (*peraggstate).transValueIsNull;
        anynull = (*peraggstate).transValueIsNull;

        /* Fill any remaining argument positions with nulls */
        i = 1;
        while i < numFinalArgs {
            (*(*fcinfo).args.as_mut_ptr().add(i as usize)).value = 0 as Datum;
            (*(*fcinfo).args.as_mut_ptr().add(i as usize)).isnull = true;
            anynull = true;
            i += 1;
        }

        if (*(*fcinfo).flinfo).fn_strict && anynull {
            /* don't call a strict function with NULL inputs */
            *result = 0 as Datum;
            *isnull = true;
        } else {
            let res: Datum;
            (*winstate).curaggcontext = (*peraggstate).aggcontext;
            res = FunctionCallInvoke!(fcinfo);
            (*winstate).curaggcontext = std::ptr::null_mut();
            *isnull = (*fcinfo).isnull;
            *result = MakeExpandedObjectReadOnly(res, (*fcinfo).isnull, (*peraggstate).resulttypeLen);
        }
    } else {
        *result = MakeExpandedObjectReadOnly(
            (*peraggstate).transValue,
            (*peraggstate).transValueIsNull,
            (*peraggstate).transtypeLen,
        );
        *isnull = (*peraggstate).transValueIsNull;
    }

    MemoryContextSwitchTo(oldContext);
}

/*
 * eval_windowaggregates
 * evaluate plain aggregates being used as window functions
 *
 * This differs from nodeAgg.c in two ways.  First, if the window's frame
 * start position moves, we use the inverse transition function (if it exists)
 * to remove rows from the transition value.  And second, we expect to be
 * able to call aggregate final functions repeatedly after aggregating more
 * data onto the same transition value.  This is not a behavior required by
 * nodeAgg.c.
 */
unsafe fn eval_windowaggregates(winstate: *mut WindowAggState) {
    let mut peraggstate: *mut WindowStatePerAggDataFull;
    let mut wfuncno: c_int;
    let numaggs: c_int;
    let mut numaggs_restart: c_int;
    let mut i: c_int;
    let mut aggregatedupto_nonrestarted: int64;
    let mut oldContext: MemoryContext;
    let econtext: *mut ExprContext;
    let agg_winobj: *mut WindowObjectData;
    let agg_row_slot: *mut TupleTableSlot;
    let temp_slot: *mut TupleTableSlot;

    numaggs = (*winstate).numaggs;
    if numaggs == 0 {
        return; /* nothing to do */
    }

    /* final output execution is in ps_ExprContext */
    econtext = (*winstate).ss.ps.ps_ExprContext;
    agg_winobj = (*winstate).agg_winobj;
    agg_row_slot = (*winstate).agg_row_slot;
    temp_slot = (*winstate).temp_slot_1;

    /*
     * If the window's frame start clause is UNBOUNDED_PRECEDING and no
     * exclusion clause is specified, then the window frame consists of a
     * contiguous group of rows extending forward from the start of the
     * partition, and rows only enter the frame, never exit it, as the current
     * row advances forward.  This makes it possible to use an incremental
     * strategy for evaluating aggregates: we run the transition function for
     * each row added to the frame, and run the final function whenever we
     * need the current aggregate value.  This is considerably more efficient
     * than the naive approach of re-running the entire aggregate calculation
     * for each current row.  It does assume that the final function doesn't
     * damage the running transition value, but we have the same assumption in
     * nodeAgg.c too (when it rescans an existing hash table).
     *
     * If the frame start does sometimes move, we can still optimize as above
     * whenever successive rows share the same frame head, but if the frame
     * head moves beyond the previous head we try to remove those rows using
     * the aggregate's inverse transition function.  This function restores
     * the aggregate's current state to what it would be if the removed row
     * had never been aggregated in the first place.  Inverse transition
     * functions may optionally return NULL, indicating that the function was
     * unable to remove the tuple from aggregation.  If this happens, or if
     * the aggregate doesn't have an inverse transition function at all, we
     * must perform the aggregation all over again for all tuples within the
     * new frame boundaries.
     *
     * If there's any exclusion clause, then we may have to aggregate over a
     * non-contiguous set of rows, so we punt and recalculate for every row.
     * (For some frame end choices, it might be that the frame is always
     * contiguous anyway, but that's an optimization to investigate later.)
     *
     * In many common cases, multiple rows share the same frame and hence the
     * same aggregate value. (In particular, if there's no ORDER BY in a RANGE
     * window, then all rows are peers and so they all have window frame equal
     * to the whole partition.)  We optimize such cases by calculating the
     * aggregate value once when we reach the first row of a peer group, and
     * then returning the saved value for all subsequent rows.
     *
     * 'aggregatedupto' keeps track of the first row that has not yet been
     * accumulated into the aggregate transition values.  Whenever we start a
     * new peer group, we accumulate forward to the end of the peer group.
     */

    /*
     * First, update the frame head position.
     *
     * The frame head should never move backwards, and the code below wouldn't
     * cope if it did, so for safety we complain if it does.
     */
    update_frameheadpos(winstate);
    if (*winstate).frameheadpos < (*winstate).aggregatedbase {
        elog!(ERROR, "window frame head moved backward");
    }

    /*
     * If the frame didn't change compared to the previous row, we can re-use
     * the result values that were previously saved at the bottom of this
     * function.  Since we don't know the current frame's end yet, this is not
     * possible to check for fully.  But if the frame end mode is UNBOUNDED
     * FOLLOWING or CURRENT ROW, no exclusion clause is specified, and the
     * current row lies within the previous row's frame, then the two frames'
     * ends must coincide.  Note that on the first row aggregatedbase ==
     * aggregatedupto, meaning this test must fail, so we don't need to check
     * the "there was no previous row" case explicitly here.
     */
    if (*winstate).aggregatedbase == (*winstate).frameheadpos
        && ((*winstate).frameOptions
            & (FRAMEOPTION_END_UNBOUNDED_FOLLOWING | FRAMEOPTION_END_CURRENT_ROW))
            != 0
        && ((*winstate).frameOptions & FRAMEOPTION_EXCLUSION) == 0
        && (*winstate).aggregatedbase <= (*winstate).currentpos
        && (*winstate).aggregatedupto > (*winstate).currentpos
    {
        i = 0;
        while i < numaggs {
            peraggstate = peragg_at(winstate, i);
            wfuncno = (*peraggstate).wfuncno;
            *(*econtext).ecxt_aggvalues.add(wfuncno as usize) = (*peraggstate).resultValue;
            *(*econtext).ecxt_aggnulls.add(wfuncno as usize) = (*peraggstate).resultValueIsNull;
            i += 1;
        }
        return;
    }

    /*----------
     * Initialize restart flags.
     *
     * We restart the aggregation:
     *	 - if we're processing the first row in the partition, or
     *	 - if the frame's head moved and we cannot use an inverse
     *	   transition function, or
     *	 - we have an EXCLUSION clause, or
     *	 - if the new frame doesn't overlap the old one
     *
     * Note that we don't strictly need to restart in the last case, but if
     * we're going to remove all rows from the aggregation anyway, a restart
     * surely is faster.
     *----------
     */
    numaggs_restart = 0;
    i = 0;
    while i < numaggs {
        peraggstate = peragg_at(winstate, i);
        if (*winstate).currentpos == 0
            || ((*winstate).aggregatedbase != (*winstate).frameheadpos
                && !OidIsValid((*peraggstate).invtransfn_oid))
            || ((*winstate).frameOptions & FRAMEOPTION_EXCLUSION) != 0
            || (*winstate).aggregatedupto <= (*winstate).frameheadpos
        {
            (*peraggstate).restart = true;
            numaggs_restart += 1;
        } else {
            (*peraggstate).restart = false;
        }
        i += 1;
    }

    /*
     * If we have any possibly-moving aggregates, attempt to advance
     * aggregatedbase to match the frame's head by removing input rows that
     * fell off the top of the frame from the aggregations.  This can fail,
     * i.e. advance_windowaggregate_base() can return false, in which case
     * we'll restart that aggregate below.
     */
    while numaggs_restart < numaggs
        && (*winstate).aggregatedbase < (*winstate).frameheadpos
    {
        /*
         * Fetch the next tuple of those being removed. This should never fail
         * as we should have been here before.
         */
        if !window_gettupleslot(agg_winobj, (*winstate).aggregatedbase, temp_slot) {
            elog!(ERROR, "could not re-fetch previously fetched frame row");
        }

        /* Set tuple context for evaluation of aggregate arguments */
        (*(*winstate).tmpcontext).ecxt_outertuple = temp_slot;

        /*
         * Perform the inverse transition for each aggregate function in the
         * window, unless it has already been marked as needing a restart.
         */
        i = 0;
        while i < numaggs {
            let ok: bool;
            peraggstate = peragg_at(winstate, i);
            if (*peraggstate).restart {
                i += 1;
                continue;
            }
            wfuncno = (*peraggstate).wfuncno;
            ok = advance_windowaggregate_base(
                winstate,
                &mut *perfunc_at(winstate, wfuncno),
                peraggstate,
            );
            if !ok {
                /* Inverse transition function has failed, must restart */
                (*peraggstate).restart = true;
                numaggs_restart += 1;
            }
            i += 1;
        }

        /* Reset per-input-tuple context after each tuple */
        ResetExprContext((*winstate).tmpcontext);

        /* And advance the aggregated-row state */
        (*winstate).aggregatedbase += 1;
        ExecClearTuple(temp_slot);
    }

    /*
     * If we successfully advanced the base rows of all the aggregates,
     * aggregatedbase now equals frameheadpos; but if we failed for any, we
     * must forcibly update aggregatedbase.
     */
    (*winstate).aggregatedbase = (*winstate).frameheadpos;

    /*
     * If we created a mark pointer for aggregates, keep it pushed up to frame
     * head, so that tuplestore can discard unnecessary rows.
     */
    if (*winobj(agg_winobj)).markptr >= 0 {
        WinSetMarkPosition(agg_winobj, (*winstate).frameheadpos);
    }

    /*
     * Now restart the aggregates that require it.
     *
     * We assume that aggregates using the shared context always restart if
     * *any* aggregate restarts, and we may thus clean up the shared
     * aggcontext if that is the case.  Private aggcontexts are reset by
     * initialize_windowaggregate() if their owning aggregate restarts. If we
     * aren't restarting an aggregate, we need to free any previously saved
     * result for it, else we'll leak memory.
     */
    if numaggs_restart > 0 {
        MemoryContextReset((*winstate).aggcontext);
    }
    i = 0;
    while i < numaggs {
        peraggstate = peragg_at(winstate, i);

        /* Aggregates using the shared ctx must restart if *any* agg does */
        Assert!(
            (*peraggstate).aggcontext != (*winstate).aggcontext
                || numaggs_restart == 0
                || (*peraggstate).restart
        );

        if (*peraggstate).restart {
            wfuncno = (*peraggstate).wfuncno;
            initialize_windowaggregate(
                winstate,
                &mut *perfunc_at(winstate, wfuncno),
                peraggstate,
            );
        } else if !(*peraggstate).resultValueIsNull {
            if !(*peraggstate).resulttypeByVal {
                pfree(DatumGetPointer((*peraggstate).resultValue) as *mut c_void);
            }
            (*peraggstate).resultValue = 0 as Datum;
            (*peraggstate).resultValueIsNull = true;
        }
        i += 1;
    }

    /*
     * Non-restarted aggregates now contain the rows between aggregatedbase
     * (i.e., frameheadpos) and aggregatedupto, while restarted aggregates
     * contain no rows.  If there are any restarted aggregates, we must thus
     * begin aggregating anew at frameheadpos, otherwise we may simply
     * continue at aggregatedupto.  We must remember the old value of
     * aggregatedupto to know how long to skip advancing non-restarted
     * aggregates.  If we modify aggregatedupto, we must also clear
     * agg_row_slot, per the loop invariant below.
     */
    aggregatedupto_nonrestarted = (*winstate).aggregatedupto;
    if numaggs_restart > 0
        && (*winstate).aggregatedupto != (*winstate).frameheadpos
    {
        (*winstate).aggregatedupto = (*winstate).frameheadpos;
        ExecClearTuple(agg_row_slot);
    }

    /*
     * Advance until we reach a row not in frame (or end of partition).
     *
     * Note the loop invariant: agg_row_slot is either empty or holds the row
     * at position aggregatedupto.  We advance aggregatedupto after processing
     * a row.
     */
    'agg_loop: loop {
        let ret: c_int;

        /* Fetch next row if we didn't already */
        if TupIsNull(agg_row_slot) {
            if !window_gettupleslot(agg_winobj, (*winstate).aggregatedupto, agg_row_slot) {
                break 'agg_loop; /* must be end of partition */
            }
        }

        /*
         * Exit loop if no more rows can be in frame.  Skip aggregation if
         * current row is not in frame but there might be more in the frame.
         */
        ret = row_is_in_frame(winstate, (*winstate).aggregatedupto, agg_row_slot);
        if ret < 0 {
            break 'agg_loop;
        }
        if ret == 0 {
            /* next_tuple: */
            /* Reset per-input-tuple context after each tuple */
            ResetExprContext((*winstate).tmpcontext);
            /* And advance the aggregated-row state */
            (*winstate).aggregatedupto += 1;
            ExecClearTuple(agg_row_slot);
            continue 'agg_loop;
        }

        /* Set tuple context for evaluation of aggregate arguments */
        (*(*winstate).tmpcontext).ecxt_outertuple = agg_row_slot;

        /* Accumulate row into the aggregates */
        i = 0;
        while i < numaggs {
            peraggstate = peragg_at(winstate, i);

            /* Non-restarted aggs skip until aggregatedupto_nonrestarted */
            if !(*peraggstate).restart
                && (*winstate).aggregatedupto < aggregatedupto_nonrestarted
            {
                i += 1;
                continue;
            }

            wfuncno = (*peraggstate).wfuncno;
            advance_windowaggregate(
                winstate,
                &mut *perfunc_at(winstate, wfuncno),
                peraggstate,
            );
            i += 1;
        }

        /* next_tuple: */
        /* Reset per-input-tuple context after each tuple */
        ResetExprContext((*winstate).tmpcontext);

        /* And advance the aggregated-row state */
        (*winstate).aggregatedupto += 1;
        ExecClearTuple(agg_row_slot);
    }

    /* The frame's end is not supposed to move backwards, ever */
    Assert!(aggregatedupto_nonrestarted <= (*winstate).aggregatedupto);

    /*
     * finalize aggregates and fill result/isnull fields.
     */
    i = 0;
    while i < numaggs {
        let result: *mut Datum;
        let isnull: *mut bool;

        peraggstate = peragg_at(winstate, i);
        wfuncno = (*peraggstate).wfuncno;
        result = (*econtext).ecxt_aggvalues.add(wfuncno as usize);
        isnull = (*econtext).ecxt_aggnulls.add(wfuncno as usize);
        finalize_windowaggregate(
            winstate,
            &mut *perfunc_at(winstate, wfuncno),
            peraggstate,
            result,
            isnull,
        );

        /*
         * save the result in case next row shares the same frame.
         *
         * XXX in some framing modes, eg ROWS/END_CURRENT_ROW, we can know in
         * advance that the next row can't possibly share the same frame. Is
         * it worth detecting that and skipping this code?
         */
        if !(*peraggstate).resulttypeByVal && !*isnull {
            oldContext = MemoryContextSwitchTo((*peraggstate).aggcontext);
            (*peraggstate).resultValue = datumCopy(
                *result,
                (*peraggstate).resulttypeByVal,
                (*peraggstate).resulttypeLen,
            );
            MemoryContextSwitchTo(oldContext);
        } else {
            (*peraggstate).resultValue = *result;
        }
        (*peraggstate).resultValueIsNull = *isnull;
        i += 1;
    }
}

/*
 * eval_windowfunction
 *
 * Arguments of window functions are not evaluated here, because a window
 * function can need random access to arbitrary rows in the partition.
 * The window function uses the special WinGetFuncArgInPartition and
 * WinGetFuncArgInFrame functions to evaluate the arguments for the rows
 * it wants.
 */
unsafe fn eval_windowfunction(
    winstate: *mut WindowAggState,
    perfuncstate: *mut WindowStatePerFuncDataFull,
    result: *mut Datum,
    isnull: *mut bool,
) {
    LOCAL_FCINFO!(fcinfo, FUNC_MAX_ARGS);
    let oldContext = MemoryContextSwitchTo(
        (*(*winstate).ss.ps.ps_ExprContext).ecxt_per_tuple_memory,
    );

    /*
     * We don't pass any normal arguments to a window function, but we do pass
     * it the number of arguments, in order to permit window function
     * implementations to support varying numbers of arguments.  The real info
     * goes through the WindowObject, which is passed via fcinfo->context.
     */
    InitFunctionCallInfoData!(
        fcinfo,
        &mut (*perfuncstate).flinfo,
        (*perfuncstate).numArguments as i16,
        (*perfuncstate).winCollation,
        (*perfuncstate).winobj as *mut Node,
        std::ptr::null_mut()
    );
    /* Just in case, make all the regular argument slots be null */
    let mut argno: c_int = 0;
    while argno < (*perfuncstate).numArguments {
        (*(*fcinfo).args.as_mut_ptr().add(argno as usize)).isnull = true;
        argno += 1;
    }
    /* Window functions don't have a current aggregate context, either */
    (*winstate).curaggcontext = std::ptr::null_mut();

    *result = FunctionCallInvoke!(fcinfo);
    *isnull = (*fcinfo).isnull;

    /*
     * The window function might have returned a pass-by-ref result that's
     * just a pointer into one of the WindowObject's temporary slots.  That's
     * not a problem if it's the only window function using the WindowObject;
     * but if there's more than one function, we'd better copy the result to
     * ensure it's not clobbered by later window functions.
     */
    if !(*perfuncstate).resulttypeByVal
        && !(*fcinfo).isnull
        && (*winstate).numfuncs > 1
    {
        *result = datumCopy(
            *result,
            (*perfuncstate).resulttypeByVal,
            (*perfuncstate).resulttypeLen,
        );
    }

    MemoryContextSwitchTo(oldContext);
}

/*
 * prepare_tuplestore
 *		Prepare the tuplestore and all of the required read pointers for the
 *		WindowAggState's frameOptions.
 *
 * Note: We use pg_noinline to avoid bloating the calling function with code
 * which is only called once.
 */
#[inline(never)]
unsafe fn prepare_tuplestore(winstate: *mut WindowAggState) {
    let node: *mut WindowAgg = (*winstate).ss.ps.plan as *mut WindowAgg;
    let frameOptions: c_int = (*winstate).frameOptions;
    let numfuncs: c_int = (*winstate).numfuncs;

    /* we shouldn't be called if this was done already */
    Assert!((*winstate).buffer.is_null());

    /* Create new tuplestore */
    (*winstate).buffer = tuplestore_begin_heap(false, false, work_mem);

    /*
     * Set up read pointers for the tuplestore.  The current pointer doesn't
     * need BACKWARD capability, but the per-window-function read pointers do,
     * and the aggregate pointer does if we might need to restart aggregation.
     */
    (*winstate).current_ptr = 0; /* read pointer 0 is pre-allocated */

    /* reset default REWIND capability bit for current ptr */
    tuplestore_set_eflags((*winstate).buffer, 0);

    /* create read pointers for aggregates, if needed */
    if (*winstate).numaggs > 0 {
        let agg_winobj: *mut WindowObjectData = (*winstate).agg_winobj;
        let mut readptr_flags: c_int = 0;

        /*
         * If the frame head is potentially movable, or we have an EXCLUSION
         * clause, we might need to restart aggregation ...
         */
        if (frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) == 0
            || (frameOptions & FRAMEOPTION_EXCLUSION) != 0
        {
            /* ... so create a mark pointer to track the frame head */
            (*winobj(agg_winobj)).markptr =
                tuplestore_alloc_read_pointer((*winstate).buffer, 0);
            /* and the read pointer will need BACKWARD capability */
            readptr_flags |= EXEC_FLAG_BACKWARD;
        }

        (*winobj(agg_winobj)).readptr = tuplestore_alloc_read_pointer(
            (*winstate).buffer,
            readptr_flags,
        );
    }

    /* create mark and read pointers for each real window function */
    let mut i: c_int = 0;
    while i < numfuncs {
        let perfuncstate: *mut WindowStatePerFuncDataFull = perfunc_at(winstate, i);
        if !(*perfuncstate).plain_agg {
            let wo: *mut WindowObjectData = (*perfuncstate).winobj;
            (*winobj(wo)).markptr =
                tuplestore_alloc_read_pointer((*winstate).buffer, 0);
            (*winobj(wo)).readptr =
                tuplestore_alloc_read_pointer((*winstate).buffer, EXEC_FLAG_BACKWARD);
        }
        i += 1;
    }

    /*
     * If we are in RANGE or GROUPS mode, then determining frame boundaries
     * requires physical access to the frame endpoint rows, except in certain
     * degenerate cases.  We create read pointers to point to those rows, to
     * simplify access and ensure that the tuplestore doesn't discard the
     * endpoint rows prematurely.  (Must create pointers in exactly the same
     * cases that update_frameheadpos and update_frametailpos need them.)
     */
    (*winstate).framehead_ptr = -1; /* if not used */
    (*winstate).frametail_ptr = -1;

    if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
        if (((frameOptions & FRAMEOPTION_START_CURRENT_ROW) != 0
            && (*node).ordNumCols != 0)
            || (frameOptions & FRAMEOPTION_START_OFFSET) != 0)
        {
            (*winstate).framehead_ptr =
                tuplestore_alloc_read_pointer((*winstate).buffer, 0);
        }
        if (((frameOptions & FRAMEOPTION_END_CURRENT_ROW) != 0
            && (*node).ordNumCols != 0)
            || (frameOptions & FRAMEOPTION_END_OFFSET) != 0)
        {
            (*winstate).frametail_ptr =
                tuplestore_alloc_read_pointer((*winstate).buffer, 0);
        }
    }

    /*
     * If we have an exclusion clause that requires knowing the boundaries of
     * the current row's peer group, we create a read pointer to track the
     * tail position of the peer group (i.e., first row of the next peer
     * group).  The head position does not require its own pointer because we
     * maintain that as a side effect of advancing the current row.
     */
    (*winstate).grouptail_ptr = -1;

    if ((frameOptions & (FRAMEOPTION_EXCLUDE_GROUP | FRAMEOPTION_EXCLUDE_TIES)) != 0)
        && (*node).ordNumCols != 0
    {
        (*winstate).grouptail_ptr =
            tuplestore_alloc_read_pointer((*winstate).buffer, 0);
    }
}

/*
 * begin_partition
 * Start buffering rows of the next partition.
 */
unsafe fn begin_partition(winstate: *mut WindowAggState) {
    let outerPlan: *mut PlanState = outerPlanState(winstate);
    let numfuncs: c_int = (*winstate).numfuncs;

    (*winstate).partition_spooled = false;
    (*winstate).framehead_valid = false;
    (*winstate).frametail_valid = false;
    (*winstate).grouptail_valid = false;
    (*winstate).spooled_rows = 0;
    (*winstate).currentpos = 0;
    (*winstate).frameheadpos = 0;
    (*winstate).frametailpos = 0;
    (*winstate).currentgroup = 0;
    (*winstate).frameheadgroup = 0;
    (*winstate).frametailgroup = 0;
    (*winstate).groupheadpos = 0;
    (*winstate).grouptailpos = -1; /* see update_grouptailpos */
    ExecClearTuple((*winstate).agg_row_slot);
    if !(*winstate).framehead_slot.is_null() {
        ExecClearTuple((*winstate).framehead_slot);
    }
    if !(*winstate).frametail_slot.is_null() {
        ExecClearTuple((*winstate).frametail_slot);
    }

    /*
     * If this is the very first partition, we need to fetch the first input
     * row to store in first_part_slot.
     */
    if TupIsNull((*winstate).first_part_slot) {
        let outerslot: *mut TupleTableSlot = ExecProcNode(outerPlan);
        if !TupIsNull(outerslot) {
            ExecCopySlot((*winstate).first_part_slot, outerslot);
        } else {
            /* outer plan is empty, so we have nothing to do */
            (*winstate).partition_spooled = true;
            (*winstate).more_partitions = false;
            return;
        }
    }

    /* Create new tuplestore if not done already. */
    if (*winstate).buffer.is_null() {
        prepare_tuplestore(winstate);
    }

    (*winstate).next_partition = false;

    if (*winstate).numaggs > 0 {
        let agg_winobj: *mut WindowObjectData = (*winstate).agg_winobj;

        /* reset mark and see positions for aggregate functions */
        (*winobj(agg_winobj)).markpos = -1;
        (*winobj(agg_winobj)).seekpos = -1;

        /* Also reset the row counters for aggregates */
        (*winstate).aggregatedbase = 0;
        (*winstate).aggregatedupto = 0;
    }

    /* reset mark and seek positions for each real window function */
    let mut i: c_int = 0;
    while i < numfuncs {
        let perfuncstate: *mut WindowStatePerFuncDataFull = perfunc_at(winstate, i);
        if !(*perfuncstate).plain_agg {
            let wo: *mut WindowObjectData = (*perfuncstate).winobj;
            (*winobj(wo)).markpos = -1;
            (*winobj(wo)).seekpos = -1;
        }
        i += 1;
    }

    /*
     * Store the first tuple into the tuplestore (it's always available now;
     * we either read it above, or saved it at the end of previous partition)
     */
    tuplestore_puttupleslot((*winstate).buffer, (*winstate).first_part_slot);
    (*winstate).spooled_rows += 1;
}

/*
 * Read tuples from the outer node, up to and including position 'pos', and
 * store them into the tuplestore. If pos is -1, reads the whole partition.
 */
unsafe fn spool_tuples(winstate: *mut WindowAggState, mut pos: int64) {
    let node: *mut WindowAgg = (*winstate).ss.ps.plan as *mut WindowAgg;
    let outerPlan: *mut PlanState;
    let mut outerslot: *mut TupleTableSlot;
    let oldcontext: MemoryContext;

    if (*winstate).buffer.is_null() {
        return; /* just a safety check */
    }
    if (*winstate).partition_spooled {
        return; /* whole partition done already */
    }

    /*
     * When in pass-through mode we can just exhaust all tuples in the current
     * partition.  We don't need these tuples for any further window function
     * evaluation, however, we do need to keep them around if we're not the
     * top-level window as another WindowAgg node above must see these.
     */
    if (*winstate).status != WINDOWAGG_RUN {
        Assert!(
            (*winstate).status == WINDOWAGG_PASSTHROUGH
                || (*winstate).status == WINDOWAGG_PASSTHROUGH_STRICT
        );
        pos = -1;
    }
    /*
     * If the tuplestore has spilled to disk, alternate reading and writing
     * becomes quite expensive due to frequent buffer flushes.  It's cheaper
     * to force the entire partition to get spooled in one go.
     *
     * XXX this is a horrid kluge --- it'd be better to fix the performance
     * problem inside tuplestore.  FIXME
     */
    else if !tuplestore_in_memory((*winstate).buffer) {
        pos = -1;
    }

    outerPlan = outerPlanState(winstate);

    /* Must be in query context to call outerplan */
    let oldcontext = MemoryContextSwitchTo(
        (*(*winstate).ss.ps.ps_ExprContext).ecxt_per_query_memory,
    );

    while (*winstate).spooled_rows <= pos || pos == -1 {
        outerslot = ExecProcNode(outerPlan);
        if TupIsNull(outerslot) {
            /* reached the end of the last partition */
            (*winstate).partition_spooled = true;
            (*winstate).more_partitions = false;
            break;
        }

        if (*node).partNumCols > 0 {
            let econtext: *mut ExprContext = (*winstate).tmpcontext;
            (*econtext).ecxt_innertuple = (*winstate).first_part_slot;
            (*econtext).ecxt_outertuple = outerslot;

            /* Check if this tuple still belongs to the current partition */
            if !ExecQualAndReset((*winstate).partEqfunction, econtext) {
                /*
                 * end of partition; copy the tuple for the next cycle.
                 */
                ExecCopySlot((*winstate).first_part_slot, outerslot);
                (*winstate).partition_spooled = true;
                (*winstate).more_partitions = true;
                break;
            }
        }

        /*
         * Remember the tuple unless we're the top-level window and we're in
         * pass-through mode.
         */
        if (*winstate).status != WINDOWAGG_PASSTHROUGH_STRICT {
            /* Still in partition, so save it into the tuplestore */
            tuplestore_puttupleslot((*winstate).buffer, outerslot);
            (*winstate).spooled_rows += 1;
        }
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * release_partition
 * clear information kept within a partition, including
 * tuplestore and aggregate results.
 */
unsafe fn release_partition(winstate: *mut WindowAggState) {
    let mut i: c_int;

    i = 0;
    while i < (*winstate).numfuncs {
        let perfuncstate: *mut WindowStatePerFuncDataFull = perfunc_at(winstate, i);
        /* Release any partition-local state of this window function */
        if !(*perfuncstate).winobj.is_null() {
            (*winobj((*perfuncstate).winobj)).localmem = std::ptr::null_mut();
        }
        i += 1;
    }

    /*
     * Release all partition-local memory (in particular, any partition-local
     * state that we might have trashed our pointers to in the above loop, and
     * any aggregate temp data).  We don't rely on retail pfree because some
     * aggregates might have allocated data we don't have direct pointers to.
     */
    MemoryContextReset((*winstate).partcontext);
    MemoryContextReset((*winstate).aggcontext);
    i = 0;
    while i < (*winstate).numaggs {
        let pa: *mut WindowStatePerAggDataFull = peragg_at(winstate, i);
        if (*pa).aggcontext != (*winstate).aggcontext {
            MemoryContextReset((*pa).aggcontext);
        }
        i += 1;
    }

    if !(*winstate).buffer.is_null() {
        tuplestore_clear((*winstate).buffer);
    }
    (*winstate).partition_spooled = false;
    (*winstate).next_partition = true;
}

/*
 * row_is_in_frame
 * Determine whether a row is in the current row's window frame according
 * to our window framing rule
 *
 * The caller must have already determined that the row is in the partition
 * and fetched it into a slot.  This function just encapsulates the framing
 * rules.
 *
 * Returns:
 * -1, if the row is out of frame and no succeeding rows can be in frame
 * 0, if the row is out of frame but succeeding rows might be in frame
 * 1, if the row is in frame
 *
 * May clobber winstate->temp_slot_2.
 */
unsafe fn row_is_in_frame(
    winstate: *mut WindowAggState,
    pos: int64,
    slot: *mut TupleTableSlot,
) -> c_int {
    let frameOptions: c_int = (*winstate).frameOptions;

    Assert!(pos >= 0); /* else caller error */

    /*
     * First, check frame starting conditions.  We might as well delegate this
     * to update_frameheadpos always; it doesn't add any notable cost.
     */
    update_frameheadpos(winstate);
    if pos < (*winstate).frameheadpos {
        return 0;
    }

    /*
     * Okay so far, now check frame ending conditions.  Here, we avoid calling
     * update_frametailpos in simple cases, so as not to spool tuples further
     * ahead than necessary.
     */
    if (frameOptions & FRAMEOPTION_END_CURRENT_ROW) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            /* rows after current row are out of frame */
            if pos > (*winstate).currentpos {
                return -1;
            }
        } else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
            /* following row that is not peer is out of frame */
            if pos > (*winstate).currentpos
                && !are_peers(winstate, slot, (*winstate).ss.ss_ScanTupleSlot)
            {
                return -1;
            }
        } else {
            Assert!(false);
        }
    } else if (frameOptions & FRAMEOPTION_END_OFFSET) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            let mut offset: int64 = DatumGetInt64((*winstate).endOffsetValue);
            /* rows after current row + offset are out of frame */
            if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) != 0 {
                offset = -offset;
            }
            if pos > (*winstate).currentpos + offset {
                return -1;
            }
        } else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
            /* hard cases, so delegate to update_frametailpos */
            update_frametailpos(winstate);
            if pos >= (*winstate).frametailpos {
                return -1;
            }
        } else {
            Assert!(false);
        }
    }

    /* Check exclusion clause */
    if (frameOptions & FRAMEOPTION_EXCLUDE_CURRENT_ROW) != 0 {
        if pos == (*winstate).currentpos {
            return 0;
        }
    } else if ((frameOptions & FRAMEOPTION_EXCLUDE_GROUP) != 0)
        || (((frameOptions & FRAMEOPTION_EXCLUDE_TIES) != 0)
            && pos != (*winstate).currentpos)
    {
        let node: *mut WindowAgg = (*winstate).ss.ps.plan as *mut WindowAgg;
        /* If no ORDER BY, all rows are peers with each other */
        if (*node).ordNumCols == 0 {
            return 0;
        }
        /* Otherwise, check the group boundaries */
        if pos >= (*winstate).groupheadpos {
            update_grouptailpos(winstate);
            if pos < (*winstate).grouptailpos {
                return 0;
            }
        }
    }

    /* If we get here, it's in frame */
    return 1;
}

/*
 * update_frameheadpos
 * make frameheadpos valid for the current row
 *
 * Note that frameheadpos is computed without regard for any window exclusion
 * clause; the current row and/or its peers are considered part of the frame
 * for this purpose even if they must be excluded later.
 *
 * May clobber winstate->temp_slot_2.
 */
unsafe fn update_frameheadpos(winstate: *mut WindowAggState) {
    let node: *mut WindowAgg = (*winstate).ss.ps.plan as *mut WindowAgg;
    let frameOptions: c_int = (*winstate).frameOptions;

    if (*winstate).framehead_valid {
        return; /* already known for current row */
    }

    /* We may be called in a short-lived context */
    let oldcontext = MemoryContextSwitchTo(
        (*(*winstate).ss.ps.ps_ExprContext).ecxt_per_query_memory,
    );

    if (frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) != 0 {
        /* In UNBOUNDED PRECEDING mode, frame head is always row 0 */
        (*winstate).frameheadpos = 0;
        (*winstate).framehead_valid = true;
    } else if (frameOptions & FRAMEOPTION_START_CURRENT_ROW) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            /* In ROWS mode, frame head is the same as current */
            (*winstate).frameheadpos = (*winstate).currentpos;
            (*winstate).framehead_valid = true;
        } else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
            /* If no ORDER BY, all rows are peers with each other */
            if (*node).ordNumCols == 0 {
                (*winstate).frameheadpos = 0;
                (*winstate).framehead_valid = true;
                MemoryContextSwitchTo(oldcontext);
                return;
            }

            /*
             * In RANGE or GROUPS START_CURRENT_ROW mode, frame head is the
             * first row that is a peer of current row.  We keep a copy of the
             * last-known frame head row in framehead_slot, and advance as
             * necessary.  Note that if we reach end of partition, we will
             * leave frameheadpos = end+1 and framehead_slot empty.
             */
            tuplestore_select_read_pointer((*winstate).buffer, (*winstate).framehead_ptr);
            if (*winstate).frameheadpos == 0
                && TupIsNull((*winstate).framehead_slot)
            {
                /* fetch first row into framehead_slot, if we didn't already */
                if !tuplestore_gettupleslot(
                    (*winstate).buffer,
                    true,
                    true,
                    (*winstate).framehead_slot,
                ) {
                    elog!(ERROR, "unexpected end of tuplestore");
                }
            }

            while !TupIsNull((*winstate).framehead_slot) {
                if are_peers(
                    winstate,
                    (*winstate).framehead_slot,
                    (*winstate).ss.ss_ScanTupleSlot,
                ) {
                    break; /* this row is the correct frame head */
                }
                /* Note we advance frameheadpos even if the fetch fails */
                (*winstate).frameheadpos += 1;
                spool_tuples(winstate, (*winstate).frameheadpos);
                if !tuplestore_gettupleslot(
                    (*winstate).buffer,
                    true,
                    true,
                    (*winstate).framehead_slot,
                ) {
                    break; /* end of partition */
                }
            }
            (*winstate).framehead_valid = true;
        } else {
            Assert!(false);
        }
    } else if (frameOptions & FRAMEOPTION_START_OFFSET) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            /* In ROWS mode, bound is physically n before/after current */
            let mut offset: int64 = DatumGetInt64((*winstate).startOffsetValue);
            if (frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING) != 0 {
                offset = -offset;
            }
            (*winstate).frameheadpos = (*winstate).currentpos + offset;
            /* frame head can't go before first row */
            if (*winstate).frameheadpos < 0 {
                (*winstate).frameheadpos = 0;
            } else if (*winstate).frameheadpos > (*winstate).currentpos + 1 {
                /* make sure frameheadpos is not past end of partition */
                spool_tuples(winstate, (*winstate).frameheadpos - 1);
                if (*winstate).frameheadpos > (*winstate).spooled_rows {
                    (*winstate).frameheadpos = (*winstate).spooled_rows;
                }
            }
            (*winstate).framehead_valid = true;
        } else if (frameOptions & FRAMEOPTION_RANGE) != 0 {
            /*
             * In RANGE START_OFFSET mode, frame head is the first row that
             * satisfies the in_range constraint relative to the current row.
             * We keep a copy of the last-known frame head row in
             * framehead_slot, and advance as necessary.  Note that if we
             * reach end of partition, we will leave frameheadpos = end+1 and
             * framehead_slot empty.
             */
            let sortCol: c_int = (*node).ordColIdx.add(0).read() as c_int;
            let mut sub: bool;
            let less: bool;

            /* We must have an ordering column */
            Assert!((*node).ordNumCols == 1);

            /* Precompute flags for in_range checks */
            if (frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING) != 0 {
                sub = true; /* subtract startOffset from current row */
            } else {
                sub = false; /* add it */
            }
            less = false; /* normally, we want frame head >= sum */
            /* If sort order is descending, flip both flags */
            if !(*winstate).inRangeAsc {
                sub = !sub;
                // less = true; -- see C: less remains false after flip per C code
                // Actually C says less = true here:
                let _ = less; // shadow to reuse name
                let less = true;
                let _ = less;
            }

            tuplestore_select_read_pointer((*winstate).buffer, (*winstate).framehead_ptr);
            if (*winstate).frameheadpos == 0
                && TupIsNull((*winstate).framehead_slot)
            {
                /* fetch first row into framehead_slot, if we didn't already */
                if !tuplestore_gettupleslot(
                    (*winstate).buffer,
                    true,
                    true,
                    (*winstate).framehead_slot,
                ) {
                    elog!(ERROR, "unexpected end of tuplestore");
                }
            }

            /* recompute less properly after possible flip */
            let less_final: bool = if !(*winstate).inRangeAsc { true } else { false };

            while !TupIsNull((*winstate).framehead_slot) {
                let mut headval: Datum = 0;
                let mut currval: Datum = 0;
                let mut headisnull: bool = false;
                let mut currisnull: bool = false;

                headval = slot_getattr((*winstate).framehead_slot, sortCol, &mut headisnull);
                currval = slot_getattr(
                    (*winstate).ss.ss_ScanTupleSlot,
                    sortCol,
                    &mut currisnull,
                );
                if headisnull || currisnull {
                    /* order of the rows depends only on nulls_first */
                    if (*winstate).inRangeNullsFirst {
                        /* advance head if head is null and curr is not */
                        if !headisnull || currisnull {
                            break;
                        }
                    } else {
                        /* advance head if head is not null and curr is null */
                        if headisnull || !currisnull {
                            break;
                        }
                    }
                } else {
                    let sub_final: bool = if (frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING) != 0 {
                        if (*winstate).inRangeAsc { true } else { false }
                    } else {
                        if (*winstate).inRangeAsc { false } else { true }
                    };
                    if DatumGetBool(FunctionCall5Coll(
                        &mut (*winstate).startInRangeFunc,
                        (*winstate).inRangeColl,
                        headval,
                        currval,
                        (*winstate).startOffsetValue,
                        BoolGetDatum(sub_final),
                        BoolGetDatum(less_final),
                    )) {
                        break; /* this row is the correct frame head */
                    }
                }
                /* Note we advance frameheadpos even if the fetch fails */
                (*winstate).frameheadpos += 1;
                spool_tuples(winstate, (*winstate).frameheadpos);
                if !tuplestore_gettupleslot(
                    (*winstate).buffer,
                    true,
                    true,
                    (*winstate).framehead_slot,
                ) {
                    break; /* end of partition */
                }
            }
            (*winstate).framehead_valid = true;
        } else if (frameOptions & FRAMEOPTION_GROUPS) != 0 {
            /*
             * In GROUPS START_OFFSET mode, frame head is the first row of the
             * first peer group whose number satisfies the offset constraint.
             * We keep a copy of the last-known frame head row in
             * framehead_slot, and advance as necessary.  Note that if we
             * reach end of partition, we will leave frameheadpos = end+1 and
             * framehead_slot empty.
             */
            let offset: int64 = DatumGetInt64((*winstate).startOffsetValue);
            let minheadgroup: int64 = if (frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING) != 0 {
                (*winstate).currentgroup - offset
            } else {
                (*winstate).currentgroup + offset
            };

            tuplestore_select_read_pointer((*winstate).buffer, (*winstate).framehead_ptr);
            if (*winstate).frameheadpos == 0
                && TupIsNull((*winstate).framehead_slot)
            {
                /* fetch first row into framehead_slot, if we didn't already */
                if !tuplestore_gettupleslot(
                    (*winstate).buffer,
                    true,
                    true,
                    (*winstate).framehead_slot,
                ) {
                    elog!(ERROR, "unexpected end of tuplestore");
                }
            }

            while !TupIsNull((*winstate).framehead_slot) {
                if (*winstate).frameheadgroup >= minheadgroup {
                    break; /* this row is the correct frame head */
                }
                ExecCopySlot((*winstate).temp_slot_2, (*winstate).framehead_slot);
                /* Note we advance frameheadpos even if the fetch fails */
                (*winstate).frameheadpos += 1;
                spool_tuples(winstate, (*winstate).frameheadpos);
                if !tuplestore_gettupleslot(
                    (*winstate).buffer,
                    true,
                    true,
                    (*winstate).framehead_slot,
                ) {
                    break; /* end of partition */
                }
                if !are_peers(winstate, (*winstate).temp_slot_2, (*winstate).framehead_slot) {
                    (*winstate).frameheadgroup += 1;
                }
            }
            ExecClearTuple((*winstate).temp_slot_2);
            (*winstate).framehead_valid = true;
        } else {
            Assert!(false);
        }
    } else {
        Assert!(false);
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * update_frametailpos
 * make frametailpos valid for the current row
 *
 * Note that frametailpos is computed without regard for any window exclusion
 * clause; the current row and/or its peers are considered part of the frame
 * for this purpose even if they must be excluded later.
 *
 * May clobber winstate->temp_slot_2.
 */
unsafe fn update_frametailpos(winstate: *mut WindowAggState) {
    let node: *mut WindowAgg = (*winstate).ss.ps.plan as *mut WindowAgg;
    let frameOptions: c_int = (*winstate).frameOptions;

    if (*winstate).frametail_valid {
        return; /* already known for current row */
    }

    /* We may be called in a short-lived context */
    let oldcontext = MemoryContextSwitchTo(
        (*(*winstate).ss.ps.ps_ExprContext).ecxt_per_query_memory,
    );

    if (frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) != 0 {
        /* In UNBOUNDED FOLLOWING mode, all partition rows are in frame */
        spool_tuples(winstate, -1);
        (*winstate).frametailpos = (*winstate).spooled_rows;
        (*winstate).frametail_valid = true;
    } else if (frameOptions & FRAMEOPTION_END_CURRENT_ROW) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            /* In ROWS mode, exactly the rows up to current are in frame */
            (*winstate).frametailpos = (*winstate).currentpos + 1;
            (*winstate).frametail_valid = true;
        } else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
            /* If no ORDER BY, all rows are peers with each other */
            if (*node).ordNumCols == 0 {
                spool_tuples(winstate, -1);
                (*winstate).frametailpos = (*winstate).spooled_rows;
                (*winstate).frametail_valid = true;
                MemoryContextSwitchTo(oldcontext);
                return;
            }

            /*
             * In RANGE or GROUPS END_CURRENT_ROW mode, frame end is the last
             * row that is a peer of current row, frame tail is the row after
             * that (if any).  We keep a copy of the last-known frame tail row
             * in frametail_slot, and advance as necessary.  Note that if we
             * reach end of partition, we will leave frametailpos = end+1 and
             * frametail_slot empty.
             */
            tuplestore_select_read_pointer((*winstate).buffer, (*winstate).frametail_ptr);
            if (*winstate).frametailpos == 0
                && TupIsNull((*winstate).frametail_slot)
            {
                /* fetch first row into frametail_slot, if we didn't already */
                if !tuplestore_gettupleslot(
                    (*winstate).buffer,
                    true,
                    true,
                    (*winstate).frametail_slot,
                ) {
                    elog!(ERROR, "unexpected end of tuplestore");
                }
            }

            while !TupIsNull((*winstate).frametail_slot) {
                if (*winstate).frametailpos > (*winstate).currentpos
                    && !are_peers(
                        winstate,
                        (*winstate).frametail_slot,
                        (*winstate).ss.ss_ScanTupleSlot,
                    )
                {
                    break; /* this row is the frame tail */
                }
                /* Note we advance frametailpos even if the fetch fails */
                (*winstate).frametailpos += 1;
                spool_tuples(winstate, (*winstate).frametailpos);
                if !tuplestore_gettupleslot(
                    (*winstate).buffer,
                    true,
                    true,
                    (*winstate).frametail_slot,
                ) {
                    break; /* end of partition */
                }
            }
            (*winstate).frametail_valid = true;
        } else {
            Assert!(false);
        }
    } else if (frameOptions & FRAMEOPTION_END_OFFSET) != 0 {
        if (frameOptions & FRAMEOPTION_ROWS) != 0 {
            /* In ROWS mode, bound is physically n before/after current */
            let mut offset: int64 = DatumGetInt64((*winstate).endOffsetValue);
            if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) != 0 {
                offset = -offset;
            }
            (*winstate).frametailpos = (*winstate).currentpos + offset + 1;
            /* smallest allowable value of frametailpos is 0 */
            if (*winstate).frametailpos < 0 {
                (*winstate).frametailpos = 0;
            } else if (*winstate).frametailpos > (*winstate).currentpos + 1 {
                /* make sure frametailpos is not past end of partition */
                spool_tuples(winstate, (*winstate).frametailpos - 1);
                if (*winstate).frametailpos > (*winstate).spooled_rows {
                    (*winstate).frametailpos = (*winstate).spooled_rows;
                }
            }
            (*winstate).frametail_valid = true;
        } else if (frameOptions & FRAMEOPTION_RANGE) != 0 {
            /*
             * In RANGE END_OFFSET mode, frame end is the last row that
             * satisfies the in_range constraint relative to the current row,
             * frame tail is the row after that (if any).  We keep a copy of
             * the last-known frame tail row in frametail_slot, and advance as
             * necessary.  Note that if we reach end of partition, we will
             * leave frametailpos = end+1 and frametail_slot empty.
             */
            let sortCol: c_int = (*node).ordColIdx.add(0).read() as c_int;
            let mut sub: bool;
            let less: bool;

            /* We must have an ordering column */
            Assert!((*node).ordNumCols == 1);

            /* Precompute flags for in_range checks */
            if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) != 0 {
                sub = true; /* subtract endOffset from current row */
            } else {
                sub = false; /* add it */
            }
            less = true; /* normally, we want frame tail <= sum */
            /* If sort order is descending, flip both flags */
            let sub_final: bool = if !(*winstate).inRangeAsc {
                if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) != 0 { false } else { true }
            } else {
                if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) != 0 { true } else { false }
            };
            let less_final: bool = if !(*winstate).inRangeAsc { false } else { true };

            tuplestore_select_read_pointer((*winstate).buffer, (*winstate).frametail_ptr);
            if (*winstate).frametailpos == 0
                && TupIsNull((*winstate).frametail_slot)
            {
                /* fetch first row into frametail_slot, if we didn't already */
                if !tuplestore_gettupleslot(
                    (*winstate).buffer,
                    true,
                    true,
                    (*winstate).frametail_slot,
                ) {
                    elog!(ERROR, "unexpected end of tuplestore");
                }
            }

            while !TupIsNull((*winstate).frametail_slot) {
                let mut tailval: Datum = 0;
                let mut currval: Datum = 0;
                let mut tailisnull: bool = false;
                let mut currisnull: bool = false;

                tailval = slot_getattr((*winstate).frametail_slot, sortCol, &mut tailisnull);
                currval = slot_getattr(
                    (*winstate).ss.ss_ScanTupleSlot,
                    sortCol,
                    &mut currisnull,
                );
                if tailisnull || currisnull {
                    /* order of the rows depends only on nulls_first */
                    if (*winstate).inRangeNullsFirst {
                        /* advance tail if tail is null or curr is not */
                        if !tailisnull {
                            break;
                        }
                    } else {
                        /* advance tail if tail is not null or curr is null */
                        if !currisnull {
                            break;
                        }
                    }
                } else {
                    if !DatumGetBool(FunctionCall5Coll(
                        &mut (*winstate).endInRangeFunc,
                        (*winstate).inRangeColl,
                        tailval,
                        currval,
                        (*winstate).endOffsetValue,
                        BoolGetDatum(sub_final),
                        BoolGetDatum(less_final),
                    )) {
                        break; /* this row is the correct frame tail */
                    }
                }
                /* Note we advance frametailpos even if the fetch fails */
                (*winstate).frametailpos += 1;
                spool_tuples(winstate, (*winstate).frametailpos);
                if !tuplestore_gettupleslot(
                    (*winstate).buffer,
                    true,
                    true,
                    (*winstate).frametail_slot,
                ) {
                    break; /* end of partition */
                }
            }
            (*winstate).frametail_valid = true;
        } else if (frameOptions & FRAMEOPTION_GROUPS) != 0 {
            /*
             * In GROUPS END_OFFSET mode, frame end is the last row of the
             * last peer group whose number satisfies the offset constraint,
             * and frame tail is the row after that (if any).  We keep a copy
             * of the last-known frame tail row in frametail_slot, and advance
             * as necessary.  Note that if we reach end of partition, we will
             * leave frametailpos = end+1 and frametail_slot empty.
             */
            let offset: int64 = DatumGetInt64((*winstate).endOffsetValue);
            let maxtailgroup: int64 = if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) != 0 {
                (*winstate).currentgroup - offset
            } else {
                (*winstate).currentgroup + offset
            };

            tuplestore_select_read_pointer((*winstate).buffer, (*winstate).frametail_ptr);
            if (*winstate).frametailpos == 0
                && TupIsNull((*winstate).frametail_slot)
            {
                /* fetch first row into frametail_slot, if we didn't already */
                if !tuplestore_gettupleslot(
                    (*winstate).buffer,
                    true,
                    true,
                    (*winstate).frametail_slot,
                ) {
                    elog!(ERROR, "unexpected end of tuplestore");
                }
            }

            while !TupIsNull((*winstate).frametail_slot) {
                if (*winstate).frametailgroup > maxtailgroup {
                    break; /* this row is the correct frame tail */
                }
                ExecCopySlot((*winstate).temp_slot_2, (*winstate).frametail_slot);
                /* Note we advance frametailpos even if the fetch fails */
                (*winstate).frametailpos += 1;
                spool_tuples(winstate, (*winstate).frametailpos);
                if !tuplestore_gettupleslot(
                    (*winstate).buffer,
                    true,
                    true,
                    (*winstate).frametail_slot,
                ) {
                    break; /* end of partition */
                }
                if !are_peers(winstate, (*winstate).temp_slot_2, (*winstate).frametail_slot) {
                    (*winstate).frametailgroup += 1;
                }
            }
            ExecClearTuple((*winstate).temp_slot_2);
            (*winstate).frametail_valid = true;
        } else {
            Assert!(false);
        }
    } else {
        Assert!(false);
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * update_grouptailpos
 * make grouptailpos valid for the current row
 *
 * May clobber winstate->temp_slot_2.
 */
unsafe fn update_grouptailpos(winstate: *mut WindowAggState) {
    let node: *mut WindowAgg = (*winstate).ss.ps.plan as *mut WindowAgg;

    if (*winstate).grouptail_valid {
        return; /* already known for current row */
    }

    /* We may be called in a short-lived context */
    let oldcontext = MemoryContextSwitchTo(
        (*(*winstate).ss.ps.ps_ExprContext).ecxt_per_query_memory,
    );

    /* If no ORDER BY, all rows are peers with each other */
    if (*node).ordNumCols == 0 {
        spool_tuples(winstate, -1);
        (*winstate).grouptailpos = (*winstate).spooled_rows;
        (*winstate).grouptail_valid = true;
        MemoryContextSwitchTo(oldcontext);
        return;
    }

    /*
     * Because grouptail_valid is reset only when current row advances into a
     * new peer group, we always reach here knowing that grouptailpos needs to
     * be advanced by at least one row.  Hence, unlike the otherwise similar
     * case for frame tail tracking, we do not need persistent storage of the
     * group tail row.
     */
    Assert!((*winstate).grouptailpos <= (*winstate).currentpos);
    tuplestore_select_read_pointer((*winstate).buffer, (*winstate).grouptail_ptr);
    loop {
        /* Note we advance grouptailpos even if the fetch fails */
        (*winstate).grouptailpos += 1;
        spool_tuples(winstate, (*winstate).grouptailpos);
        if !tuplestore_gettupleslot(
            (*winstate).buffer,
            true,
            true,
            (*winstate).temp_slot_2,
        ) {
            break; /* end of partition */
        }
        if (*winstate).grouptailpos > (*winstate).currentpos
            && !are_peers(winstate, (*winstate).temp_slot_2, (*winstate).ss.ss_ScanTupleSlot)
        {
            break; /* this row is the group tail */
        }
    }
    ExecClearTuple((*winstate).temp_slot_2);
    (*winstate).grouptail_valid = true;

    MemoryContextSwitchTo(oldcontext);
}

/*
 * calculate_frame_offsets
 *		Determine the startOffsetValue and endOffsetValue values for the
 *		WindowAgg's frame options.
 */
#[inline(never)]
unsafe fn calculate_frame_offsets(pstate: *mut PlanState) {
    let winstate: *mut WindowAggState = pstate as *mut WindowAggState;
    let econtext: *mut ExprContext;
    let frameOptions: c_int = (*winstate).frameOptions;
    let mut value: Datum;
    let mut isnull: bool = false;
    let mut len: int16 = 0;
    let mut byval: bool = false;

    /* Ensure we've not been called before for this scan */
    Assert!((*winstate).all_first);

    econtext = (*winstate).ss.ps.ps_ExprContext;

    if (frameOptions & FRAMEOPTION_START_OFFSET) != 0 {
        Assert!(!(*winstate).startOffset.is_null());
        value = ExecEvalExprSwitchContext((*winstate).startOffset, econtext, &mut isnull);
        if isnull {
            ereport!(ERROR, errmsg!("frame starting offset must not be null") /* C also: errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED) */);
        }
        /* copy value into query-lifespan context */
        get_typlenbyval(
            exprType((*(*winstate).startOffset).expr as *mut Node),
            &mut len,
            &mut byval,
        );
        (*winstate).startOffsetValue = datumCopy(value, byval, len);
        if (frameOptions & (FRAMEOPTION_ROWS | FRAMEOPTION_GROUPS)) != 0 {
            /* value is known to be int8 */
            let offset: int64 = DatumGetInt64(value);
            if offset < 0 {
                ereport!(ERROR, errmsg!("frame starting offset must not be negative") /* C also: errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE) */);
            }
        }
    }

    if (frameOptions & FRAMEOPTION_END_OFFSET) != 0 {
        Assert!(!(*winstate).endOffset.is_null());
        value = ExecEvalExprSwitchContext((*winstate).endOffset, econtext, &mut isnull);
        if isnull {
            ereport!(ERROR, errmsg!("frame ending offset must not be null") /* C also: errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED) */);
        }
        /* copy value into query-lifespan context */
        get_typlenbyval(
            exprType((*(*winstate).endOffset).expr as *mut Node),
            &mut len,
            &mut byval,
        );
        (*winstate).endOffsetValue = datumCopy(value, byval, len);
        if (frameOptions & (FRAMEOPTION_ROWS | FRAMEOPTION_GROUPS)) != 0 {
            /* value is known to be int8 */
            let offset: int64 = DatumGetInt64(value);
            if offset < 0 {
                ereport!(ERROR, errmsg!("frame ending offset must not be negative") /* C also: errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE) */);
            }
        }
    }
    (*winstate).all_first = false;
}

/* -----------------
 * ExecWindowAgg
 *
 *	ExecWindowAgg receives tuples from its outer subplan and
 *	stores them into a tuplestore, then processes window functions.
 *	This node doesn't reduce nor qualify any row so the number of
 *	returned rows is exactly the same as its outer subplan's result.
 * -----------------
 */
unsafe fn ExecWindowAgg(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let winstate: *mut WindowAggState = castNode!(WindowAggState, T_WindowAggState, pstate);
    let mut slot: *mut TupleTableSlot;
    let mut econtext: *mut ExprContext;
    let mut i: c_int;
    let mut numfuncs: c_int;

    CHECK_FOR_INTERRUPTS!();

    if (*winstate).status == WINDOWAGG_DONE {
        return std::ptr::null_mut();
    }

    /*
     * Compute frame offset values, if any, during first call (or after a
     * rescan).  These are assumed to hold constant throughout the scan; if
     * user gives us a volatile expression, we'll only use its initial value.
     */
    if unlikely((*winstate).all_first) {
        calculate_frame_offsets(pstate);
    }

    /* We need to loop as the runCondition or qual may filter out tuples */
    loop {
        if (*winstate).next_partition {
            /* Initialize for first partition and set current row = 0 */
            begin_partition(winstate);
            /* If there are no input rows, we'll detect that and exit below */
        } else {
            /* Advance current row within partition */
            (*winstate).currentpos += 1;
            /* This might mean that the frame moves, too */
            (*winstate).framehead_valid = false;
            (*winstate).frametail_valid = false;
            /* we don't need to invalidate grouptail here; see below */
        }

        /*
         * Spool all tuples up to and including the current row, if we haven't
         * already
         */
        spool_tuples(winstate, (*winstate).currentpos);

        /* Move to the next partition if we reached the end of this partition */
        if (*winstate).partition_spooled
            && (*winstate).currentpos >= (*winstate).spooled_rows
        {
            release_partition(winstate);

            if (*winstate).more_partitions {
                begin_partition(winstate);
                Assert!((*winstate).spooled_rows > 0);

                /* Come out of pass-through mode when changing partition */
                (*winstate).status = WINDOWAGG_RUN;
            } else {
                /* No further partitions?  We're done */
                (*winstate).status = WINDOWAGG_DONE;
                return std::ptr::null_mut();
            }
        }

        /* final output execution is in ps_ExprContext */
        econtext = (*winstate).ss.ps.ps_ExprContext;

        /* Clear the per-output-tuple context for current row */
        ResetExprContext(econtext);

        /*
         * Read the current row from the tuplestore, and save in
         * ScanTupleSlot. (We can't rely on the outerplan's output slot
         * because we may have to read beyond the current row.  Also, we have
         * to actually copy the row out of the tuplestore, since window
         * function evaluation might cause the tuplestore to dump its state to
         * disk.)
         *
         * In GROUPS mode, or when tracking a group-oriented exclusion clause,
         * we must also detect entering a new peer group and update associated
         * state when that happens.  We use temp_slot_2 to temporarily hold
         * the previous row for this purpose.
         *
         * Current row must be in the tuplestore, since we spooled it above.
         */
        tuplestore_select_read_pointer((*winstate).buffer, (*winstate).current_ptr);
        if ((*winstate).frameOptions
            & (FRAMEOPTION_GROUPS | FRAMEOPTION_EXCLUDE_GROUP | FRAMEOPTION_EXCLUDE_TIES))
            != 0
            && (*winstate).currentpos > 0
        {
            ExecCopySlot((*winstate).temp_slot_2, (*winstate).ss.ss_ScanTupleSlot);
            if !tuplestore_gettupleslot(
                (*winstate).buffer,
                true,
                true,
                (*winstate).ss.ss_ScanTupleSlot,
            ) {
                elog!(ERROR, "unexpected end of tuplestore");
            }
            if !are_peers(
                winstate,
                (*winstate).temp_slot_2,
                (*winstate).ss.ss_ScanTupleSlot,
            ) {
                (*winstate).currentgroup += 1;
                (*winstate).groupheadpos = (*winstate).currentpos;
                (*winstate).grouptail_valid = false;
            }
            ExecClearTuple((*winstate).temp_slot_2);
        } else {
            if !tuplestore_gettupleslot(
                (*winstate).buffer,
                true,
                true,
                (*winstate).ss.ss_ScanTupleSlot,
            ) {
                elog!(ERROR, "unexpected end of tuplestore");
            }
        }

        /* don't evaluate the window functions when we're in pass-through mode */
        if (*winstate).status == WINDOWAGG_RUN {
            /*
             * Evaluate true window functions
             */
            numfuncs = (*winstate).numfuncs;
            i = 0;
            while i < numfuncs {
                let perfuncstate: *mut WindowStatePerFuncDataFull =
                    perfunc_at(winstate, i);

                if !(*perfuncstate).plain_agg {
                    eval_windowfunction(
                        winstate,
                        perfuncstate,
                        (*econtext).ecxt_aggvalues
                            .add((*(*perfuncstate).wfuncstate).wfuncno as usize)
                            as *mut Datum,
                        (*econtext).ecxt_aggnulls
                            .add((*(*perfuncstate).wfuncstate).wfuncno as usize)
                            as *mut bool,
                    );
                }
                i += 1;
            }

            /*
             * Evaluate aggregates
             */
            if (*winstate).numaggs > 0 {
                eval_windowaggregates(winstate);
            }
        }

        /*
         * If we have created auxiliary read pointers for the frame or group
         * boundaries, force them to be kept up-to-date, because we don't know
         * whether the window function(s) will do anything that requires that.
         * Failing to advance the pointers would result in being unable to
         * trim data from the tuplestore, which is bad.  (If we could know in
         * advance whether the window functions will use frame boundary info,
         * we could skip creating these pointers in the first place ... but
         * unfortunately the window function API doesn't require that.)
         */
        if (*winstate).framehead_ptr >= 0 {
            update_frameheadpos(winstate);
        }
        if (*winstate).frametail_ptr >= 0 {
            update_frametailpos(winstate);
        }
        if (*winstate).grouptail_ptr >= 0 {
            update_grouptailpos(winstate);
        }

        /*
         * Truncate any no-longer-needed rows from the tuplestore.
         */
        tuplestore_trim((*winstate).buffer);

        /*
         * Form and return a projection tuple using the windowfunc results and
         * the current row.  Setting ecxt_outertuple arranges that any Vars
         * will be evaluated with respect to that row.
         */
        (*econtext).ecxt_outertuple = (*winstate).ss.ss_ScanTupleSlot;

        slot = ExecProject((*winstate).ss.ps.ps_ProjInfo);

        if (*winstate).status == WINDOWAGG_RUN {
            (*econtext).ecxt_scantuple = slot;

            /*
             * Now evaluate the run condition to see if we need to go into
             * pass-through mode, or maybe stop completely.
             */
            if !ExecQual((*winstate).runcondition, econtext) {
                /*
                 * Determine which mode to move into.  If there is no
                 * PARTITION BY clause and we're the top-level WindowAgg then
                 * we're done.  This tuple and any future tuples cannot
                 * possibly match the runcondition.  However, when there is a
                 * PARTITION BY clause or we're not the top-level window we
                 * can't just stop as we need to either process other
                 * partitions or ensure WindowAgg nodes above us receive all
                 * of the tuples they need to process their WindowFuncs.
                 */
                if (*winstate).use_pass_through {
                    /*
                     * When switching into a pass-through mode, we'd better
                     * NULLify the aggregate results as these are no longer
                     * updated and NULLifying them avoids the old stale
                     * results lingering.  Some of these might be byref types
                     * so we can't have them pointing to free'd memory.  The
                     * planner insisted that quals used in the runcondition
                     * are strict, so the top-level WindowAgg will always
                     * filter these NULLs out in the filter clause.
                     */
                    numfuncs = (*winstate).numfuncs;
                    i = 0;
                    while i < numfuncs {
                        *(*econtext).ecxt_aggvalues.add(i as usize) = 0 as Datum;
                        *(*econtext).ecxt_aggnulls.add(i as usize) = true;
                        i += 1;
                    }

                    /*
                     * STRICT pass-through mode is required for the top window
                     * when there is a PARTITION BY clause.  Otherwise we must
                     * ensure we store tuples that don't match the
                     * runcondition so they're available to WindowAggs above.
                     */
                    if (*winstate).top_window {
                        (*winstate).status = WINDOWAGG_PASSTHROUGH_STRICT;
                        continue;
                    } else {
                        (*winstate).status = WINDOWAGG_PASSTHROUGH;
                    }
                } else {
                    /*
                     * Pass-through not required.  We can just return NULL.
                     * Nothing else will match the runcondition.
                     */
                    (*winstate).status = WINDOWAGG_DONE;
                    return std::ptr::null_mut();
                }
            }

            /*
             * Filter out any tuples we don't need in the top-level WindowAgg.
             */
            if !ExecQual((*winstate).ss.ps.qual, econtext) {
                InstrCountFiltered1(winstate, 1);
                continue;
            }

            break;
        }
        /*
         * When not in WINDOWAGG_RUN mode, we must still return this tuple if
         * we're anything apart from the top window.
         */
        else if !(*winstate).top_window {
            break;
        }
    }

    slot
}

/* local type aliases for this translation unit */
/// TODO(pg-port): access/common/tupdesc.h TupleDesc
type TupleDesc = *mut crate::access::common::tupdesc::TupleDescData;
/// TODO(pg-port): access/attnum.h AttrNumber
type AttrNumber = int16;
/// TODO(pg-port): utils/builtins.h TextDatumGetCString
unsafe fn TextDatumGetCString(d: Datum) -> *mut std::os::raw::c_char {
    std::ptr::null_mut() // TODO(pg-port): utils/builtins
}
/// TODO(pg-port): parser/parse_coerce.h IsBinaryCoercible
unsafe fn IsBinaryCoercible(_srctype: Oid, _targettype: Oid) -> bool {
    false // TODO(pg-port): parser/parse_coerce
}
/// TODO(pg-port): executor/executor.h outerPlan(node) for generic Plan
unsafe fn outerPlan_node(node: *mut Plan) -> *mut Plan {
    std::ptr::null_mut() // TODO(pg-port): executor/executor
}
/// TODO(pg-port): executor/executor.h outerPlanState assignment target
unsafe fn outerPlanState_ptr(node: *mut WindowAggState) -> *mut *mut PlanState {
    std::ptr::null_mut() // TODO(pg-port): executor/executor
}
/* -----------------
 * ExecInitWindowAgg
 *
 *	Creates the run-time information for the WindowAgg node produced by the
 *	planner and initializes its outer subtree
 * -----------------
 */
pub unsafe fn ExecInitWindowAgg(
    node: *mut WindowAgg,
    estate: *mut EState,
    eflags: c_int,
) -> *mut WindowAggState {
    let winstate: *mut WindowAggState;
    let outerPlan: *mut Plan;
    let econtext: *mut ExprContext;
    let tmpcontext: *mut ExprContext;
    let mut perfunc: WindowStatePerFunc;
    let mut peragg: WindowStatePerAgg;
    let frameOptions: c_int = (*node).frameOptions;
    let mut numfuncs: c_int;
    let mut wfuncno: c_int;
    let mut numaggs: c_int;
    let mut aggno: c_int;
    let scanDesc: TupleDesc;
    let mut lc: *mut crate::nodes::pg_list::ListCell = std::ptr::null_mut();

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_BACKWARD | 0x0008)) == 0); /* EXEC_FLAG_MARK = 0x0008 */

    /*
     * create state structure
     */
    winstate = makeNode_WindowAggState();
    (*winstate).ss.ps.plan = node as *mut Plan;
    (*winstate).ss.ps.state = estate;
    (*winstate).ss.ps.ExecProcNode = Some(ExecWindowAgg);

    /* copy frame options to state node for easy access */
    (*winstate).frameOptions = frameOptions;

    /*
     * Create expression contexts.  We need two, one for per-input-tuple
     * processing and one for per-output-tuple processing.  We cheat a little
     * by using ExecAssignExprContext() to build both.
     */
    ExecAssignExprContext(estate, &mut (*winstate).ss.ps);
    tmpcontext = (*winstate).ss.ps.ps_ExprContext;
    (*winstate).tmpcontext = tmpcontext;
    ExecAssignExprContext(estate, &mut (*winstate).ss.ps);

    /* Create long-lived context for storage of partition-local memory etc */
    (*winstate).partcontext = AllocSetContextCreate!(
        CurrentMemoryContext,
        b"WindowAgg Partition\0".as_ptr() as *const std::os::raw::c_char,
        ALLOCSET_DEFAULT_SIZES
    );

    /*
     * Create mid-lived context for aggregate trans values etc.
     *
     * Note that moving aggregates each use their own private context, not
     * this one.
     */
    (*winstate).aggcontext = AllocSetContextCreate!(
        CurrentMemoryContext,
        b"WindowAgg Aggregates\0".as_ptr() as *const std::os::raw::c_char,
        ALLOCSET_DEFAULT_SIZES
    );

    /* Only the top-level WindowAgg may have a qual */
    Assert!((*node).plan.qual.is_null() || (*node).topWindow);

    /* Initialize the qual */
    (*winstate).ss.ps.qual = ExecInitQual(
        (*node).plan.qual,
        &mut (*winstate).ss.ps as *mut PlanState,
    );

    /*
     * Setup the run condition, if we received one from the query planner.
     * When set, this may allow us to move into pass-through mode so that we
     * don't have to perform any further evaluation of WindowFuncs in the
     * current partition or possibly stop returning tuples altogether when all
     * tuples are in the same partition.
     */
    (*winstate).runcondition = ExecInitQual(
        (*node).runCondition,
        &mut (*winstate).ss.ps as *mut PlanState,
    );

    /*
     * When we're not the top-level WindowAgg node or we are but have a
     * PARTITION BY clause we must move into one of the WINDOWAGG_PASSTHROUGH*
     * modes when the runCondition becomes false.
     */
    (*winstate).use_pass_through = !(*node).topWindow || (*node).partNumCols > 0;

    /* remember if we're the top-window or we are below the top-window */
    (*winstate).top_window = (*node).topWindow;

    /*
     * initialize child nodes
     */
    outerPlan = outerPlan_node(node as *mut Plan);
    *outerPlanState_ptr(winstate) = ExecInitNode(outerPlan, estate, eflags);

    /*
     * initialize source tuple type (which is also the tuple type that we'll
     * store in the tuplestore and use in all our working slots).
     */
    ExecCreateScanSlotFromOuterPlan(estate, &mut (*winstate).ss, &TTSOpsMinimalTuple);
    scanDesc = (*(*winstate).ss.ss_ScanTupleSlot).tts_tupleDescriptor as TupleDesc;

    /* the outer tuple isn't the child's tuple, but always a minimal tuple */
    (*winstate).ss.ps.outeropsset = true;
    (*winstate).ss.ps.outerops = &TTSOpsMinimalTuple;
    (*winstate).ss.ps.outeropsfixed = true;

    /*
     * tuple table initialization
     */
    (*winstate).first_part_slot = ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);
    (*winstate).agg_row_slot = ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);
    (*winstate).temp_slot_1 = ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);
    (*winstate).temp_slot_2 = ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);

    /*
     * create frame head and tail slots only if needed (must create slots in
     * exactly the same cases that update_frameheadpos and update_frametailpos
     * need them)
     */
    (*winstate).framehead_slot = std::ptr::null_mut();
    (*winstate).frametail_slot = std::ptr::null_mut();

    if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) != 0 {
        if ((frameOptions & FRAMEOPTION_START_CURRENT_ROW) != 0
            && (*node).ordNumCols != 0)
            || (frameOptions & FRAMEOPTION_START_OFFSET) != 0
        {
            (*winstate).framehead_slot =
                ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);
        }
        if ((frameOptions & FRAMEOPTION_END_CURRENT_ROW) != 0
            && (*node).ordNumCols != 0)
            || (frameOptions & FRAMEOPTION_END_OFFSET) != 0
        {
            (*winstate).frametail_slot =
                ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);
        }
    }

    /*
     * Initialize result slot, type and projection.
     */
    ExecInitResultTupleSlotTL(&mut (*winstate).ss.ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&mut (*winstate).ss.ps, std::ptr::null_mut());

    /* Set up data for comparing tuples */
    if (*node).partNumCols > 0 {
        (*winstate).partEqfunction = execTuplesMatchPrepare(
            scanDesc as *mut c_void,
            (*node).partNumCols,
            (*node).partColIdx as *const AttrNumber,
            (*node).partOperators as *const Oid,
            (*node).partCollations as *const Oid,
            &mut (*winstate).ss.ps as *mut PlanState as *mut c_void,
        ) as *mut ExprState;
    }

    if (*node).ordNumCols > 0 {
        (*winstate).ordEqfunction = execTuplesMatchPrepare(
            scanDesc as *mut c_void,
            (*node).ordNumCols,
            (*node).ordColIdx as *const AttrNumber,
            (*node).ordOperators as *const Oid,
            (*node).ordCollations as *const Oid,
            &mut (*winstate).ss.ps as *mut PlanState as *mut c_void,
        ) as *mut ExprState;
    }

    /*
     * WindowAgg nodes use aggvalues and aggnulls as well as Agg nodes.
     */
    numfuncs = (*winstate).numfuncs;
    numaggs = (*winstate).numaggs;
    econtext = (*winstate).ss.ps.ps_ExprContext;
    (*econtext).ecxt_aggvalues =
        palloc0(std::mem::size_of::<Datum>() * numfuncs as usize) as *mut Datum;
    (*econtext).ecxt_aggnulls =
        palloc0(std::mem::size_of::<bool>() * numfuncs as usize) as *mut bool;

    /*
     * allocate per-wfunc/per-agg state information.
     */
    perfunc = palloc0(
        std::mem::size_of::<WindowStatePerFuncDataFull>() * numfuncs as usize,
    ) as WindowStatePerFunc;
    peragg = palloc0(
        std::mem::size_of::<WindowStatePerAggDataFull>() * numaggs as usize,
    ) as WindowStatePerAgg;
    (*winstate).perfunc = perfunc;
    (*winstate).peragg = peragg;

    wfuncno = -1;
    aggno = -1;
    /* foreach(l, winstate->funcs) */
    lc = list_head((*winstate).funcs);
    while !lc.is_null() {
        let wfuncstate: *mut WindowFuncExprState =
            lfirst(lc as *mut c_void) as *mut WindowFuncExprState;
        let wfunc: *mut WindowFunc = (*wfuncstate).wfunc;
        let perfuncstate: *mut WindowStatePerFuncDataFull;
        let aclresult: AclResult;
        let mut i: c_int;

        if (*wfunc).winref != (*node).winref {
            /* planner screwed up? */
            elog!(ERROR, "WindowFunc with winref {} assigned to WindowAgg with winref {}",
                 (*wfunc).winref, (*node).winref);
        }

        /* Look for a previous duplicate window function */
        i = 0;
        while i <= wfuncno {
            let prev: *mut WindowStatePerFuncDataFull = perfunc_at(winstate, i);
            if equal(wfunc as *mut c_void, (*prev).wfunc as *mut c_void)
                && !contain_volatile_functions(wfunc as *mut Node)
            {
                break;
            }
            i += 1;
        }
        if i <= wfuncno {
            /* Found a match to an existing entry, so just mark it */
            (*wfuncstate).wfuncno = i;
            lc = lnext((*winstate).funcs, lc);
            continue;
        }

        /* Nope, so assign a new PerAgg record */
        wfuncno += 1;
        perfuncstate = perfunc_at(winstate, wfuncno);

        /* Mark WindowFunc state node with assigned index in the result array */
        (*wfuncstate).wfuncno = wfuncno;

        /* Check permission to call window function */
        aclresult = object_aclcheck(
            ProcedureRelationId,
            (*wfunc).winfnoid,
            GetUserId(),
            ACL_EXECUTE,
        ) as i32;
        if aclresult != ACLCHECK_OK {
            aclcheck_error(
                aclresult,
                OBJECT_FUNCTION,
                get_func_name((*wfunc).winfnoid),
            );
        }
        InvokeFunctionExecuteHook((*wfunc).winfnoid);

        /* Fill in the perfuncstate data */
        (*perfuncstate).wfuncstate = wfuncstate;
        (*perfuncstate).wfunc = wfunc;
        (*perfuncstate).numArguments = list_length((*wfuncstate).args);
        (*perfuncstate).winCollation = (*wfunc).inputcollid;

        get_typlenbyval(
            (*wfunc).wintype,
            &mut (*perfuncstate).resulttypeLen,
            &mut (*perfuncstate).resulttypeByVal,
        );

        /*
         * If it's really just a plain aggregate function, we'll emulate the
         * Agg environment for it.
         */
        (*perfuncstate).plain_agg = (*wfunc).winagg;
        if (*wfunc).winagg {
            let peraggstate: *mut WindowStatePerAggDataFull;

            aggno += 1;
            (*perfuncstate).aggno = aggno;
            peraggstate = peragg_at(winstate, aggno);
            initialize_peragg(
                winstate,
                wfunc,
                peraggstate as WindowStatePerAgg,
            );
            (*peraggstate).wfuncno = wfuncno;
        } else {
            let winobj: *mut WindowObjectDataFull = makeNode_WindowObjectData() as *mut WindowObjectDataFull;

            (*winobj).winstate = winstate;
            (*winobj).argstates = (*wfuncstate).args;
            (*winobj).localmem = std::ptr::null_mut();
            (*perfuncstate).winobj = winobj as *mut WindowObjectData;

            /* It's a real window function, so set up to call it. */
            fmgr_info_cxt(
                (*wfunc).winfnoid,
                &mut (*perfuncstate).flinfo,
                (*econtext).ecxt_per_query_memory,
            );
            fmgr_info_set_expr!(
                wfunc as *mut Node,
                &mut (*perfuncstate).flinfo
            );
        }

        lc = lnext((*winstate).funcs, lc);
    }

    /* Update numfuncs, numaggs to match number of unique functions found */
    (*winstate).numfuncs = wfuncno + 1;
    (*winstate).numaggs = aggno + 1;

    /* Set up WindowObject for aggregates, if needed */
    if (*winstate).numaggs > 0 {
        let agg_winobj: *mut WindowObjectDataFull = makeNode_WindowObjectData() as *mut WindowObjectDataFull;

        (*agg_winobj).winstate = winstate;
        (*agg_winobj).argstates = NIL;
        (*agg_winobj).localmem = std::ptr::null_mut();
        /* make sure markptr = -1 to invalidate. It may not get used */
        (*agg_winobj).markptr = -1;
        (*agg_winobj).readptr = -1;
        (*winstate).agg_winobj = agg_winobj as *mut WindowObjectData;
    }

    /* Set the status to running */
    (*winstate).status = WINDOWAGG_RUN;

    /* initialize frame bound offset expressions */
    (*winstate).startOffset = ExecInitExpr(
        (*node).startOffset as *mut Expr,
        &mut (*winstate).ss.ps,
    );
    (*winstate).endOffset = ExecInitExpr(
        (*node).endOffset as *mut Expr,
        &mut (*winstate).ss.ps,
    );

    /* Lookup in_range support functions if needed */
    if OidIsValid((*node).startInRangeFunc) {
        fmgr_info((*node).startInRangeFunc, &mut (*winstate).startInRangeFunc);
    }
    if OidIsValid((*node).endInRangeFunc) {
        fmgr_info((*node).endInRangeFunc, &mut (*winstate).endInRangeFunc);
    }
    (*winstate).inRangeColl = (*node).inRangeColl;
    (*winstate).inRangeAsc = (*node).inRangeAsc;
    (*winstate).inRangeNullsFirst = (*node).inRangeNullsFirst;

    (*winstate).all_first = true;
    (*winstate).partition_spooled = false;
    (*winstate).more_partitions = false;
    (*winstate).next_partition = true;

    winstate
}

/* -----------------
 * ExecEndWindowAgg
 * -----------------
 */
pub unsafe fn ExecEndWindowAgg(node: *mut WindowAggState) {
    let outerPlan: *mut PlanState;
    let mut i: c_int;

    if !(*node).buffer.is_null() {
        tuplestore_end((*node).buffer);

        /* nullify so that release_partition skips the tuplestore_clear() */
        (*node).buffer = std::ptr::null_mut();
    }

    release_partition(node);

    i = 0;
    while i < (*node).numaggs {
        let peraggstate: *mut WindowStatePerAggDataFull = peragg_at(node, i);
        if (*peraggstate).aggcontext != (*node).aggcontext {
            MemoryContextDelete((*peraggstate).aggcontext);
        }
        i += 1;
    }
    MemoryContextDelete((*node).partcontext);
    MemoryContextDelete((*node).aggcontext);

    pfree((*node).perfunc as *mut c_void);
    pfree((*node).peragg as *mut c_void);

    outerPlan = outerPlanState(node);
    ExecEndNode(outerPlan);
}

/* -----------------
 * ExecReScanWindowAgg
 * -----------------
 */
pub unsafe fn ExecReScanWindowAgg(node: *mut WindowAggState) {
    let outerPlan: *mut PlanState = outerPlanState(node);
    let econtext: *mut ExprContext = (*node).ss.ps.ps_ExprContext;

    (*node).status = WINDOWAGG_RUN;
    (*node).all_first = true;

    /* release tuplestore et al */
    release_partition(node);

    /* release all temp tuples, but especially first_part_slot */
    ExecClearTuple((*node).ss.ss_ScanTupleSlot);
    ExecClearTuple((*node).first_part_slot);
    ExecClearTuple((*node).agg_row_slot);
    ExecClearTuple((*node).temp_slot_1);
    ExecClearTuple((*node).temp_slot_2);
    if !(*node).framehead_slot.is_null() {
        ExecClearTuple((*node).framehead_slot);
    }
    if !(*node).frametail_slot.is_null() {
        ExecClearTuple((*node).frametail_slot);
    }

    /* Forget current wfunc values */
    MemSet(
        (*econtext).ecxt_aggvalues as *mut c_void,
        0,
        std::mem::size_of::<Datum>() * (*node).numfuncs as usize,
    );
    MemSet(
        (*econtext).ecxt_aggnulls as *mut c_void,
        0,
        std::mem::size_of::<bool>() * (*node).numfuncs as usize,
    );

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
}

/*
 * initialize_peragg
 *
 * Almost same as in nodeAgg.c, except we don't support DISTINCT currently.
 */
unsafe fn initialize_peragg(
    winstate: *mut WindowAggState,
    wfunc: *mut WindowFunc,
    peraggstate: WindowStatePerAgg,
) -> *mut WindowStatePerAggDataFull {
    let mut inputTypes: [Oid; 100 /* FUNC_MAX_ARGS */] = [0; 100];
    let mut numArguments: c_int;
    let aggTuple: HeapTuple;
    let aggform: Form_pg_aggregate;
    let mut aggtranstype: Oid;
    let initvalAttNo: c_int;
    let mut aclresult: AclResult;
    let use_ma_code: bool;
    let transfn_oid: Oid;
    let invtransfn_oid: Oid;
    let finalfn_oid: Oid;
    let finalextra: bool;
    let finalmodify: i8;
    let transfnexpr: *mut Expr;
    let invtransfnexpr: *mut Expr;
    let finalfnexpr: *mut Expr;
    let textInitVal: Datum;
    let mut i: c_int;
    let mut lc: *mut crate::nodes::pg_list::ListCell;
    let peraggstate: *mut WindowStatePerAggDataFull =
        peraggstate as *mut WindowStatePerAggDataFull;

    numArguments = list_length((*wfunc).args);

    i = 0;
    lc = list_head((*wfunc).args);
    while !lc.is_null() {
        inputTypes[i as usize] = exprType(lfirst(lc as *mut c_void) as *mut Node);
        i += 1;
        lc = lnext((*wfunc).args, lc);
    }

    aggTuple = SearchSysCache1(AGGFNOID as c_int, ObjectIdGetDatum((*wfunc).winfnoid));
    if !HeapTupleIsValid(aggTuple) {
        elog!(ERROR, "cache lookup failed for aggregate {}", (*wfunc).winfnoid);
    }
    aggform = GETSTRUCT::<FormData_pg_aggregate>(aggTuple);

    /*
     * Figure out whether we want to use the moving-aggregate implementation,
     * and collect the right set of fields from the pg_aggregate entry.
     *
     * It's possible that an aggregate would supply a safe moving-aggregate
     * implementation and an unsafe normal one, in which case our hand is
     * forced.  Otherwise, if the frame head can't move, we don't need
     * moving-aggregate code.  Even if we'd like to use it, don't do so if the
     * aggregate's arguments (and FILTER clause if any) contain any calls to
     * volatile functions.  Otherwise, the difference between restarting and
     * not restarting the aggregation would be user-visible.
     *
     * We also don't risk using moving aggregates when there are subplans in
     * the arguments or FILTER clause.  This is partly because
     * contain_volatile_functions() doesn't look inside subplans; but there
     * are other reasons why a subplan's output might be volatile.  For
     * example, syncscan mode can render the results nonrepeatable.
     */
    if !OidIsValid((*aggform).aggminvtransfn) {
        use_ma_code = false; /* sine qua non */
    } else if (*aggform).aggmfinalmodify == AGGMODIFY_READ_ONLY
        && (*aggform).aggfinalmodify != AGGMODIFY_READ_ONLY
    {
        use_ma_code = true; /* decision forced by safety */
    } else if ((*winstate).frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) != 0 {
        use_ma_code = false; /* non-moving frame head */
    } else if contain_volatile_functions(wfunc as *mut Node) {
        use_ma_code = false; /* avoid possible behavioral change */
    } else if contain_subplans(wfunc as *mut Node) {
        use_ma_code = false; /* subplans might contain volatile functions */
    } else {
        use_ma_code = true; /* yes, let's use it */
    }
    if use_ma_code {
        (*peraggstate).transfn_oid = (*aggform).aggmtransfn;
        transfn_oid = (*aggform).aggmtransfn;
        (*peraggstate).invtransfn_oid = (*aggform).aggminvtransfn;
        invtransfn_oid = (*aggform).aggminvtransfn;
        (*peraggstate).finalfn_oid = (*aggform).aggmfinalfn;
        finalfn_oid = (*aggform).aggmfinalfn;
        finalextra = (*aggform).aggmfinalextra;
        finalmodify = (*aggform).aggmfinalmodify;
        aggtranstype = (*aggform).aggmtranstype;
        initvalAttNo = Anum_pg_aggregate_aggminitval;
    } else {
        (*peraggstate).transfn_oid = (*aggform).aggtransfn;
        transfn_oid = (*aggform).aggtransfn;
        (*peraggstate).invtransfn_oid = InvalidOid;
        invtransfn_oid = InvalidOid;
        (*peraggstate).finalfn_oid = (*aggform).aggfinalfn;
        finalfn_oid = (*aggform).aggfinalfn;
        finalextra = (*aggform).aggfinalextra;
        finalmodify = (*aggform).aggfinalmodify;
        aggtranstype = (*aggform).aggtranstype;
        initvalAttNo = Anum_pg_aggregate_agginitval;
    }

    /*
     * ExecInitWindowAgg already checked permission to call aggregate function
     * ... but we still need to check the component functions
     */

    /* Check that aggregate owner has permission to call component fns */
    {
        let procTuple: HeapTuple;
        let aggOwner: Oid;

        procTuple = SearchSysCache1(PROCOID as c_int, ObjectIdGetDatum((*wfunc).winfnoid));
        if !HeapTupleIsValid(procTuple) {
            elog!(ERROR, "cache lookup failed for function {}", (*wfunc).winfnoid);
        }
        aggOwner = (*GETSTRUCT::<FormData_pg_proc>(procTuple)).proowner;
        ReleaseSysCache(procTuple);

        aclresult = object_aclcheck(ProcedureRelationId, transfn_oid, aggOwner, ACL_EXECUTE) as i32;
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(transfn_oid));
        }
        InvokeFunctionExecuteHook(transfn_oid);

        if OidIsValid(invtransfn_oid) {
            aclresult =
                object_aclcheck(ProcedureRelationId, invtransfn_oid, aggOwner, ACL_EXECUTE) as i32;
            if aclresult != ACLCHECK_OK {
                aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(invtransfn_oid));
            }
            InvokeFunctionExecuteHook(invtransfn_oid);
        }

        if OidIsValid(finalfn_oid) {
            aclresult =
                object_aclcheck(ProcedureRelationId, finalfn_oid, aggOwner, ACL_EXECUTE) as i32;
            if aclresult != ACLCHECK_OK {
                aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(finalfn_oid));
            }
            InvokeFunctionExecuteHook(finalfn_oid);
        }
    }

    /*
     * If the selected finalfn isn't read-only, we can't run this aggregate as
     * a window function.  This is a user-facing error, so we take a bit more
     * care with the error message than elsewhere in this function.
     */
    if finalmodify != AGGMODIFY_READ_ONLY {
        ereport!(ERROR, errmsg!(
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), format_procedure(wfunc->winfnoid) */
                "aggregate function {} does not support use as a window function",
                (*wfunc).winfnoid
            ));
    }

    /* Detect how many arguments to pass to the finalfn */
    if finalextra {
        (*peraggstate).numFinalArgs = numArguments + 1;
    } else {
        (*peraggstate).numFinalArgs = 1;
    }

    /* resolve actual type of transition state, if polymorphic */
    aggtranstype = resolve_aggregate_transtype(
        (*wfunc).winfnoid,
        aggtranstype,
        inputTypes.as_mut_ptr(),
        numArguments,
    );

    /* build expression trees using actual argument & result types */
    let mut transfnexpr: *mut Expr = std::ptr::null_mut();
    let mut invtransfnexpr: *mut Expr = std::ptr::null_mut();
    build_aggregate_transfn_expr(
        inputTypes.as_mut_ptr(),
        numArguments,
        0, /* no ordered-set window functions yet */
        false, /* no variadic window functions yet */
        aggtranstype,
        (*wfunc).inputcollid,
        transfn_oid,
        invtransfn_oid,
        &mut transfnexpr,
        &mut invtransfnexpr,
    );

    /* set up infrastructure for calling the transfn(s) and finalfn */
    fmgr_info(transfn_oid, &mut (*peraggstate).transfn);
    fmgr_info_set_expr!(transfnexpr as *mut Node, &mut (*peraggstate).transfn);

    if OidIsValid(invtransfn_oid) {
        fmgr_info(invtransfn_oid, &mut (*peraggstate).invtransfn);
        fmgr_info_set_expr!(invtransfnexpr as *mut Node, &mut (*peraggstate).invtransfn);
    }

    if OidIsValid(finalfn_oid) {
        let mut finalfnexpr: *mut Expr = std::ptr::null_mut();
        build_aggregate_finalfn_expr(
            inputTypes.as_mut_ptr(),
            (*peraggstate).numFinalArgs,
            aggtranstype,
            (*wfunc).wintype,
            (*wfunc).inputcollid,
            finalfn_oid,
            &mut finalfnexpr,
        );
        fmgr_info(finalfn_oid, &mut (*peraggstate).finalfn);
        fmgr_info_set_expr!(finalfnexpr as *mut Node, &mut (*peraggstate).finalfn);
    }

    /* get info about relevant datatypes */
    get_typlenbyval(
        (*wfunc).wintype,
        &mut (*peraggstate).resulttypeLen,
        &mut (*peraggstate).resulttypeByVal,
    );
    get_typlenbyval(
        aggtranstype,
        &mut (*peraggstate).transtypeLen,
        &mut (*peraggstate).transtypeByVal,
    );

    /*
     * initval is potentially null, so don't try to access it as a struct
     * field. Must do it the hard way with SysCacheGetAttr.
     */
    textInitVal = SysCacheGetAttr(
        AGGFNOID as c_int,
        aggTuple,
        initvalAttNo as i16,
        &mut (*peraggstate).initValueIsNull,
    );

    if (*peraggstate).initValueIsNull {
        (*peraggstate).initValue = 0 as Datum;
    } else {
        (*peraggstate).initValue = GetAggInitVal(textInitVal, aggtranstype);
    }

    /*
     * If the transfn is strict and the initval is NULL, make sure input type
     * and transtype are the same (or at least binary-compatible), so that
     * it's OK to use the first input value as the initial transValue.  This
     * should have been checked at agg definition time, but we must check
     * again in case the transfn's strictness property has been changed.
     */
    if (*peraggstate).transfn.fn_strict && (*peraggstate).initValueIsNull {
        if numArguments < 1 || !IsBinaryCoercible(inputTypes[0], aggtranstype) {
            ereport!(ERROR, errmsg!(
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
                    "aggregate {} needs to have compatible input type and transition type",
                    (*wfunc).winfnoid
                ));
        }
    }

    /*
     * Insist that forward and inverse transition functions have the same
     * strictness setting.  Allowing them to differ would require handling
     * more special cases in advance_windowaggregate and
     * advance_windowaggregate_base, for no discernible benefit.  This should
     * have been checked at agg definition time, but we must check again in
     * case either function's strictness property has been changed.
     */
    if OidIsValid(invtransfn_oid)
        && (*peraggstate).transfn.fn_strict != (*peraggstate).invtransfn.fn_strict
    {
        ereport!(ERROR, errmsg!(
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
                "strictness of aggregate's forward and inverse transition functions must match"
            ));
    }

    /*
     * Moving aggregates use their own aggcontext.
     *
     * This is necessary because they might restart at different times, so we
     * might never be able to reset the shared context otherwise.  We can't
     * make it the aggregates' responsibility to clean up after themselves,
     * because strict aggregates must be restarted whenever we remove their
     * last non-NULL input, which the aggregate won't be aware is happening.
     * Also, just pfree()ing the transValue upon restarting wouldn't help,
     * since we'd miss any indirectly referenced data.  We could, in theory,
     * make the memory allocation rules for moving aggregates different than
     * they have historically been for plain aggregates, but that seems grotty
     * and likely to lead to memory leaks.
     */
    if OidIsValid(invtransfn_oid) {
        (*peraggstate).aggcontext = AllocSetContextCreate!(
            CurrentMemoryContext,
            b"WindowAgg Per Aggregate\0".as_ptr() as *const std::os::raw::c_char,
            ALLOCSET_DEFAULT_SIZES
        );
    } else {
        (*peraggstate).aggcontext = (*winstate).aggcontext;
    }

    ReleaseSysCache(aggTuple);

    peraggstate
}

unsafe fn GetAggInitVal(textInitVal: Datum, transtype: Oid) -> Datum {
    let mut typinput: Oid = InvalidOid;
    let mut typioparam: Oid = InvalidOid;
    let strInitVal: *mut std::os::raw::c_char;
    let initVal: Datum;

    getTypeInputInfo(transtype, &mut typinput, &mut typioparam);
    strInitVal = TextDatumGetCString(textInitVal);
    initVal = OidInputFunctionCall(typinput, strInitVal, typioparam, -1);
    pfree(strInitVal as *mut c_void);
    initVal
}

/*
 * are_peers
 * compare two rows to see if they are equal according to the ORDER BY clause
 *
 * NB: this does not consider the window frame mode.
 */
unsafe fn are_peers(
    winstate: *mut WindowAggState,
    slot1: *mut TupleTableSlot,
    slot2: *mut TupleTableSlot,
) -> bool {
    let node: *mut WindowAgg = (*winstate).ss.ps.plan as *mut WindowAgg;
    let econtext: *mut ExprContext = (*winstate).tmpcontext;

    /* If no ORDER BY, all rows are peers with each other */
    if (*node).ordNumCols == 0 {
        return true;
    }

    (*econtext).ecxt_outertuple = slot1;
    (*econtext).ecxt_innertuple = slot2;
    ExecQualAndReset((*winstate).ordEqfunction, econtext)
}

/*
 * window_gettupleslot
 *	Fetch the pos'th tuple of the current partition into the slot,
 *	using the winobj's read pointer
 *
 * Returns true if successful, false if no such row
 */
unsafe fn window_gettupleslot(
    winobj: *mut WindowObjectData,
    pos: int64,
    slot: *mut TupleTableSlot,
) -> bool {
    let winstate: *mut WindowAggState = (*winobj!(winobj)).winstate;
    let oldcontext: MemoryContext;

    /* often called repeatedly in a row */
    CHECK_FOR_INTERRUPTS!();

    /* Don't allow passing -1 to spool_tuples here */
    if pos < 0 {
        return false;
    }

    /* If necessary, fetch the tuple into the spool */
    spool_tuples(winstate, pos);

    if pos >= (*winstate).spooled_rows {
        return false;
    }

    if pos < (*winobj!(winobj)).markpos {
        elog!(ERROR, "cannot fetch row before WindowObject's mark position");
    }

    oldcontext = MemoryContextSwitchTo(
        (*(*winstate).ss.ps.ps_ExprContext).ecxt_per_query_memory,
    );

    tuplestore_select_read_pointer((*winstate).buffer, (*winobj!(winobj)).readptr);

    /*
     * Advance or rewind until we are within one tuple of the one we want.
     */
    if (*winobj!(winobj)).seekpos < pos - 1 {
        if !tuplestore_skiptuples(
            (*winstate).buffer,
            pos - 1 - (*winobj!(winobj)).seekpos,
            true,
        ) {
            elog!(ERROR, "unexpected end of tuplestore");
        }
        (*winobj!(winobj)).seekpos = pos - 1;
    } else if (*winobj!(winobj)).seekpos > pos + 1 {
        if !tuplestore_skiptuples(
            (*winstate).buffer,
            (*winobj!(winobj)).seekpos - (pos + 1),
            false,
        ) {
            elog!(ERROR, "unexpected end of tuplestore");
        }
        (*winobj!(winobj)).seekpos = pos + 1;
    } else if (*winobj!(winobj)).seekpos == pos {
        /*
         * There's no API to refetch the tuple at the current position.  We
         * have to move one tuple forward, and then one backward.  (We don't
         * do it the other way because we might try to fetch the row before
         * our mark, which isn't allowed.)  XXX this case could stand to be
         * optimized.
         */
        tuplestore_advance((*winstate).buffer, true);
        (*winobj!(winobj)).seekpos += 1;
    }

    /*
     * Now we should be on the tuple immediately before or after the one we
     * want, so just fetch forwards or backwards as appropriate.
     *
     * Notice that we tell tuplestore_gettupleslot to make a physical copy of
     * the fetched tuple.  This ensures that the slot's contents remain valid
     * through manipulations of the tuplestore, which some callers depend on.
     */
    if (*winobj!(winobj)).seekpos > pos {
        if !tuplestore_gettupleslot((*winstate).buffer, false, true, slot) {
            elog!(ERROR, "unexpected end of tuplestore");
        }
        (*winobj!(winobj)).seekpos -= 1;
    } else {
        if !tuplestore_gettupleslot((*winstate).buffer, true, true, slot) {
            elog!(ERROR, "unexpected end of tuplestore");
        }
        (*winobj!(winobj)).seekpos += 1;
    }

    Assert!((*winobj!(winobj)).seekpos == pos);

    MemoryContextSwitchTo(oldcontext);

    true
}


/***********************************************************************
 * API exposed to window functions
 ***********************************************************************/


/*
 * WinGetPartitionLocalMemory
 *		Get working memory that lives till end of partition processing
 *
 * On first call within a given partition, this allocates and zeroes the
 * requested amount of space.  Subsequent calls just return the same chunk.
 *
 * Memory obtained this way is normally used to hold state that should be
 * automatically reset for each new partition.  If a window function wants
 * to hold state across the whole query, fcinfo->fn_extra can be used in the
 * usual way for that.
 */
pub unsafe fn WinGetPartitionLocalMemory(winobj: *mut WindowObjectData, sz: Size) -> *mut c_void {
    Assert!(WindowObjectIsValid(winobj));
    if (*winobj!(winobj)).localmem.is_null() {
        (*winobj!(winobj)).localmem =
            MemoryContextAllocZero((*(*winobj!(winobj)).winstate).partcontext, sz);
    }
    (*winobj!(winobj)).localmem
}

/*
 * WinGetCurrentPosition
 *		Return the current row's position (counting from 0) within the current
 *		partition.
 */
pub unsafe fn WinGetCurrentPosition(winobj: *mut WindowObjectData) -> int64 {
    Assert!(WindowObjectIsValid(winobj));
    (*(*winobj!(winobj)).winstate).currentpos
}

/*
 * WinGetPartitionRowCount
 *		Return total number of rows contained in the current partition.
 *
 * Note: this is a relatively expensive operation because it forces the
 * whole partition to be "spooled" into the tuplestore at once.  Once
 * executed, however, additional calls within the same partition are cheap.
 */
pub unsafe fn WinGetPartitionRowCount(winobj: *mut WindowObjectData) -> int64 {
    Assert!(WindowObjectIsValid(winobj));
    spool_tuples((*winobj!(winobj)).winstate, -1);
    (*(*winobj!(winobj)).winstate).spooled_rows
}

/*
 * WinSetMarkPosition
 *		Set the "mark" position for the window object, which is the oldest row
 *		number (counting from 0) it is allowed to fetch during all subsequent
 *		operations within the current partition.
 *
 * Window functions do not have to call this, but are encouraged to move the
 * mark forward when possible to keep the tuplestore size down and prevent
 * having to spill rows to disk.
 */
pub unsafe fn WinSetMarkPosition(winobj: *mut WindowObjectData, markpos: int64) {
    let winstate: *mut WindowAggState;

    Assert!(WindowObjectIsValid(winobj));
    winstate = (*winobj!(winobj)).winstate;

    if markpos < (*winobj!(winobj)).markpos {
        elog!(ERROR, "cannot move WindowObject's mark position backward");
    }
    tuplestore_select_read_pointer((*winstate).buffer, (*winobj!(winobj)).markptr);
    if markpos > (*winobj!(winobj)).markpos {
        tuplestore_skiptuples(
            (*winstate).buffer,
            markpos - (*winobj!(winobj)).markpos,
            true,
        );
        (*winobj!(winobj)).markpos = markpos;
    }
    tuplestore_select_read_pointer((*winstate).buffer, (*winobj!(winobj)).readptr);
    if markpos > (*winobj!(winobj)).seekpos {
        tuplestore_skiptuples(
            (*winstate).buffer,
            markpos - (*winobj!(winobj)).seekpos,
            true,
        );
        (*winobj!(winobj)).seekpos = markpos;
    }
}

/*
 * WinRowsArePeers
 *		Compare two rows (specified by absolute position in partition) to see
 *		if they are equal according to the ORDER BY clause.
 *
 * NB: this does not consider the window frame mode.
 */
pub unsafe fn WinRowsArePeers(
    winobj: *mut WindowObjectData,
    pos1: int64,
    pos2: int64,
) -> bool {
    let winstate: *mut WindowAggState;
    let node: *mut WindowAgg;
    let slot1: *mut TupleTableSlot;
    let slot2: *mut TupleTableSlot;
    let res: bool;

    Assert!(WindowObjectIsValid(winobj));
    winstate = (*winobj!(winobj)).winstate;
    node = (*winstate).ss.ps.plan as *mut WindowAgg;

    /* If no ORDER BY, all rows are peers; don't bother to fetch them */
    if (*node).ordNumCols == 0 {
        return true;
    }

    /*
     * Note: OK to use temp_slot_2 here because we aren't calling any
     * frame-related functions (those tend to clobber temp_slot_2).
     */
    slot1 = (*winstate).temp_slot_1;
    slot2 = (*winstate).temp_slot_2;

    if !window_gettupleslot(winobj, pos1, slot1) {
        elog!(ERROR, "specified position is out of window: {}", pos1);
    }
    if !window_gettupleslot(winobj, pos2, slot2) {
        elog!(ERROR, "specified position is out of window: {}", pos2);
    }

    res = are_peers(winstate, slot1, slot2);

    ExecClearTuple(slot1);
    ExecClearTuple(slot2);

    res
}

/*
 * WinGetFuncArgInPartition
 *		Evaluate a window function's argument expression on a specified
 *		row of the partition.  The row is identified in lseek(2) style,
 *		i.e. relative to the current, first, or last row.
 *
 * argno: argument number to evaluate (counted from 0)
 * relpos: signed rowcount offset from the seek position
 * seektype: WINDOW_SEEK_CURRENT, WINDOW_SEEK_HEAD, or WINDOW_SEEK_TAIL
 * set_mark: If the row is found and set_mark is true, the mark is moved to
 *		the row as a side-effect.
 * isnull: output argument, receives isnull status of result
 * isout: output argument, set to indicate whether target row position
 *		is out of partition (can pass NULL if caller doesn't care about this)
 *
 * Specifying a nonexistent row is not an error, it just causes a null result
 * (plus setting *isout true, if isout isn't NULL).
 */
pub unsafe fn WinGetFuncArgInPartition(
    winobj: *mut WindowObjectData,
    argno: c_int,
    relpos: c_int,
    seektype: c_int,
    set_mark: bool,
    isnull: *mut bool,
    isout: *mut bool,
) -> Datum {
    let winstate: *mut WindowAggState;
    let econtext: *mut ExprContext;
    let slot: *mut TupleTableSlot;
    let gottuple: bool;
    let abs_pos: int64;

    Assert!(WindowObjectIsValid(winobj));
    winstate = (*winobj!(winobj)).winstate;
    econtext = (*winstate).ss.ps.ps_ExprContext;
    slot = (*winstate).temp_slot_1;

    match seektype {
        x if x == WINDOW_SEEK_CURRENT => {
            abs_pos = (*winstate).currentpos + relpos as int64;
        }
        x if x == WINDOW_SEEK_HEAD => {
            abs_pos = relpos as int64;
        }
        x if x == WINDOW_SEEK_TAIL => {
            spool_tuples(winstate, -1);
            abs_pos = (*winstate).spooled_rows - 1 + relpos as int64;
        }
        _ => {
            elog!(ERROR, "unrecognized window seek type: {}", seektype);
            abs_pos = 0; /* keep compiler quiet */
        }
    }

    gottuple = window_gettupleslot(winobj, abs_pos, slot);

    if !gottuple {
        if !isout.is_null() {
            *isout = true;
        }
        *isnull = true;
        return 0 as Datum;
    } else {
        if !isout.is_null() {
            *isout = false;
        }
        if set_mark {
            WinSetMarkPosition(winobj, abs_pos);
        }
        (*econtext).ecxt_outertuple = slot;
        return ExecEvalExpr(
            list_nth((*winobj!(winobj)).argstates, argno) as *mut ExprState,
            econtext,
            isnull,
        );
    }
}

/*
 * WinGetFuncArgInFrame
 *		Evaluate a window function's argument expression on a specified
 *		row of the window frame.  The row is identified in lseek(2) style,
 *		i.e. relative to the first or last row of the frame.  (We do not
 *		support WINDOW_SEEK_CURRENT here, because it's not very clear what
 *		that should mean if the current row isn't part of the frame.)
 *
 * argno: argument number to evaluate (counted from 0)
 * relpos: signed rowcount offset from the seek position
 * seektype: WINDOW_SEEK_HEAD or WINDOW_SEEK_TAIL
 * set_mark: If the row is found/in frame and set_mark is true, the mark is
 *		moved to the row as a side-effect.
 * isnull: output argument, receives isnull status of result
 * isout: output argument, set to indicate whether target row position
 *		is out of frame (can pass NULL if caller doesn't care about this)
 *
 * Specifying a nonexistent or not-in-frame row is not an error, it just
 * causes a null result (plus setting *isout true, if isout isn't NULL).
 *
 * Note that some exclusion-clause options lead to situations where the
 * rows that are in-frame are not consecutive in the partition.  But we
 * count only in-frame rows when measuring relpos.
 *
 * The set_mark flag is interpreted as meaning that the caller will specify
 * a constant (or, perhaps, monotonically increasing) relpos in successive
 * calls, so that *if there is no exclusion clause* there will be no need
 * to fetch a row before the previously fetched row.  But we do not expect
 * the caller to know how to account for exclusion clauses.  Therefore,
 * if there is an exclusion clause we take responsibility for adjusting the
 * mark request to something that will be safe given the above assumption
 * about relpos.
 */
pub unsafe fn WinGetFuncArgInFrame(
    winobj: *mut WindowObjectData,
    argno: c_int,
    relpos: c_int,
    seektype: c_int,
    set_mark: bool,
    isnull: *mut bool,
    isout: *mut bool,
) -> Datum {
    let winstate: *mut WindowAggState;
    let econtext: *mut ExprContext;
    let slot: *mut TupleTableSlot;
    let mut abs_pos: int64;
    let mut mark_pos: int64;

    Assert!(WindowObjectIsValid(winobj));
    winstate = (*winobj!(winobj)).winstate;
    econtext = (*winstate).ss.ps.ps_ExprContext;
    slot = (*winstate).temp_slot_1;

    /* C goto -> labeled block; 'out_of_frame jumps past the match */
    let result: Option<Datum> = 'done: {
        match seektype {
            x if x == WINDOW_SEEK_CURRENT => {
                elog!(ERROR, "WINDOW_SEEK_CURRENT is not supported for WinGetFuncArgInFrame");
                abs_pos = 0;
                mark_pos = 0; /* keep compiler quiet */
            }
            x if x == WINDOW_SEEK_HEAD => {
                /* rejecting relpos < 0 is easy and simplifies code below */
                if relpos < 0 {
                    break 'done None; /* out_of_frame */
                }
                update_frameheadpos(winstate);
                abs_pos = (*winstate).frameheadpos + relpos as int64;
                mark_pos = abs_pos;

                /*
                 * Account for exclusion option if one is active, but advance only
                 * abs_pos not mark_pos.  This prevents changes of the current
                 * row's peer group from resulting in trying to fetch a row before
                 * some previous mark position.
                 */
                match (*winstate).frameOptions & FRAMEOPTION_EXCLUSION {
                    0 => {
                        /* no adjustment needed */
                    }
                    x if x == FRAMEOPTION_EXCLUDE_CURRENT_ROW => {
                        if abs_pos >= (*winstate).currentpos
                            && (*winstate).currentpos >= (*winstate).frameheadpos
                        {
                            abs_pos += 1;
                        }
                    }
                    x if x == FRAMEOPTION_EXCLUDE_GROUP => {
                        update_grouptailpos(winstate);
                        if abs_pos >= (*winstate).groupheadpos
                            && (*winstate).grouptailpos > (*winstate).frameheadpos
                        {
                            let overlapstart = if (*winstate).groupheadpos > (*winstate).frameheadpos {
                                (*winstate).groupheadpos
                            } else {
                                (*winstate).frameheadpos
                            };
                            abs_pos += (*winstate).grouptailpos - overlapstart;
                        }
                    }
                    x if x == FRAMEOPTION_EXCLUDE_TIES => {
                        update_grouptailpos(winstate);
                        if abs_pos >= (*winstate).groupheadpos
                            && (*winstate).grouptailpos > (*winstate).frameheadpos
                        {
                            let overlapstart = if (*winstate).groupheadpos > (*winstate).frameheadpos {
                                (*winstate).groupheadpos
                            } else {
                                (*winstate).frameheadpos
                            };
                            if abs_pos == overlapstart {
                                abs_pos = (*winstate).currentpos;
                            } else {
                                abs_pos += (*winstate).grouptailpos - overlapstart - 1;
                            }
                        }
                    }
                    _ => {
                        elog!(ERROR, "unrecognized frame option state: 0x{:x}", (*winstate).frameOptions);
                    }
                }
            }
            x if x == WINDOW_SEEK_TAIL => {
                /* rejecting relpos > 0 is easy and simplifies code below */
                if relpos > 0 {
                    break 'done None; /* out_of_frame */
                }
                update_frametailpos(winstate);
                abs_pos = (*winstate).frametailpos - 1 + relpos as int64;

                /*
                 * Account for exclusion option if one is active.  If there is no
                 * exclusion, we can safely set the mark at the accessed row.  But
                 * if there is, we can only mark the frame start, because we can't
                 * be sure how far back in the frame the exclusion might cause us
                 * to fetch in future.  Furthermore, we have to actually check
                 * against frameheadpos here, since it's unsafe to try to fetch a
                 * row before frame start if the mark might be there already.
                 */
                match (*winstate).frameOptions & FRAMEOPTION_EXCLUSION {
                    0 => {
                        /* no adjustment needed */
                        mark_pos = abs_pos;
                    }
                    x if x == FRAMEOPTION_EXCLUDE_CURRENT_ROW => {
                        if abs_pos <= (*winstate).currentpos
                            && (*winstate).currentpos < (*winstate).frametailpos
                        {
                            abs_pos -= 1;
                        }
                        update_frameheadpos(winstate);
                        if abs_pos < (*winstate).frameheadpos {
                            break 'done None; /* out_of_frame */
                        }
                        mark_pos = (*winstate).frameheadpos;
                    }
                    x if x == FRAMEOPTION_EXCLUDE_GROUP => {
                        update_grouptailpos(winstate);
                        if abs_pos < (*winstate).grouptailpos
                            && (*winstate).groupheadpos < (*winstate).frametailpos
                        {
                            let overlapend = if (*winstate).grouptailpos < (*winstate).frametailpos {
                                (*winstate).grouptailpos
                            } else {
                                (*winstate).frametailpos
                            };
                            abs_pos -= overlapend - (*winstate).groupheadpos;
                        }
                        update_frameheadpos(winstate);
                        if abs_pos < (*winstate).frameheadpos {
                            break 'done None; /* out_of_frame */
                        }
                        mark_pos = (*winstate).frameheadpos;
                    }
                    x if x == FRAMEOPTION_EXCLUDE_TIES => {
                        update_grouptailpos(winstate);
                        if abs_pos < (*winstate).grouptailpos
                            && (*winstate).groupheadpos < (*winstate).frametailpos
                        {
                            let overlapend = if (*winstate).grouptailpos < (*winstate).frametailpos {
                                (*winstate).grouptailpos
                            } else {
                                (*winstate).frametailpos
                            };
                            if abs_pos == overlapend - 1 {
                                abs_pos = (*winstate).currentpos;
                            } else {
                                abs_pos -= overlapend - 1 - (*winstate).groupheadpos;
                            }
                        }
                        update_frameheadpos(winstate);
                        if abs_pos < (*winstate).frameheadpos {
                            break 'done None; /* out_of_frame */
                        }
                        mark_pos = (*winstate).frameheadpos;
                    }
                    _ => {
                        elog!(ERROR, "unrecognized frame option state: 0x{:x}", (*winstate).frameOptions);
                        mark_pos = 0; /* keep compiler quiet */
                    }
                }
            }
            _ => {
                elog!(ERROR, "unrecognized window seek type: {}", seektype);
                abs_pos = 0;
                mark_pos = 0; /* keep compiler quiet */
            }
        }

        if !window_gettupleslot(winobj, abs_pos, slot) {
            break 'done None; /* out_of_frame */
        }

        /* The code above does not detect all out-of-frame cases, so check */
        if row_is_in_frame(winstate, abs_pos, slot) <= 0 {
            break 'done None; /* out_of_frame */
        }

        if !isout.is_null() {
            *isout = false;
        }
        if set_mark {
            WinSetMarkPosition(winobj, mark_pos);
        }
        (*econtext).ecxt_outertuple = slot;
        Some(ExecEvalExpr(
            list_nth((*winobj!(winobj)).argstates, argno) as *mut ExprState,
            econtext,
            isnull,
        ))
    };

    match result {
        Some(v) => v,
        None => {
            /* out_of_frame: */
            if !isout.is_null() {
                *isout = true;
            }
            *isnull = true;
            0 as Datum
        }
    }
}

/*
 * WinGetFuncArgCurrent
 *		Evaluate a window function's argument expression on the current row.
 *
 * argno: argument number to evaluate (counted from 0)
 * isnull: output argument, receives isnull status of result
 *
 * Note: this isn't quite equivalent to WinGetFuncArgInPartition or
 * WinGetFuncArgInFrame targeting the current row, because it will succeed
 * even if the WindowObject's mark has been set beyond the current row.
 * This should generally be used for "ordinary" arguments of a window
 * function, such as the offset argument of lead() or lag().
 */
pub unsafe fn WinGetFuncArgCurrent(
    winobj: *mut WindowObjectData,
    argno: c_int,
    isnull: *mut bool,
) -> Datum {
    let winstate: *mut WindowAggState;
    let econtext: *mut ExprContext;

    Assert!(WindowObjectIsValid(winobj));
    winstate = (*winobj!(winobj)).winstate;

    econtext = (*winstate).ss.ps.ps_ExprContext;

    (*econtext).ecxt_outertuple = (*winstate).ss.ss_ScanTupleSlot;
    ExecEvalExpr(
        list_nth((*winobj!(winobj)).argstates, argno) as *mut ExprState,
        econtext,
        isnull,
    )
}
