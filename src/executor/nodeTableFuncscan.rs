//! nodeTableFuncscan.c - Support routines for scanning RangeTableFunc
//!   (XMLTABLE like functions).
//!
//! postgres source: src/backend/executor/nodeTableFuncscan.c
//! companion header: src/include/executor/nodeTableFuncscan.h
//!
//! INTERFACE ROUTINES
//!     ExecTableFuncScan       scans a function.
//!     ExecFunctionNext        retrieve next tuple in sequential order.
//!     ExecInitTableFuncScan   creates and initializes a TableFuncscan node.
//!     ExecEndTableFuncScan        releases any storage allocated.
//!     ExecReScanTableFuncScan rescans the function

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::access::common::tupdesc::{BuildDescFromLists, TupleDesc, TupleDescAttr};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::miscadmin::{work_mem, CHECK_FOR_INTERRUPTS};
use crate::nodes::bitmapset::{bms_is_member, Bitmapset};
use crate::nodes::execnodes::{
    EState, ExprContext, ExprState, FmgrInfo, PlanState, ScanState, TableFuncRoutine,
    TableFuncScanState, Tuplestorestate,
};
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{list_head, lnext, lfirst, List, ListCell};
use crate::nodes::plannodes::{innerPlan, outerPlan, Plan, TableFuncScan};
use crate::nodes::primnodes::{Expr, TableFunc, TFT_XMLTABLE};
use crate::nodes::value::String as PgString;
use crate::utils::memutils::{MemoryContextReset, ALLOCSET_DEFAULT_SIZES};

use crate::executor::executor::{
    ExecScanAccessMtd, ExecScanRecheckMtd, EXEC_FLAG_MARK,
};
use crate::executor::executor::{
    ExecInitExpr, ExecInitExprList, ExecInitQual, ExecInitResultTypeTL,
    ExecInitScanTupleSlot,
};
use crate::executor::execScan::{
    ExecAssignScanProjectionInfo, ExecScan, ExecScanReScan,
};
use crate::executor::execTuples::TTSOpsMinimalTuple;
use crate::executor::execUtils::ExecAssignExprContext;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::utils::builtins::TextDatumGetCString;
use crate::utils::fmgr::fmgr_info;
use crate::utils::palloc::CurrentMemoryContext;

use crate::{castNode, foreach, current_cell, forboth, lfirst_node, makeNode, strVal, AllocSetContextCreate, Assert};

/* ----------------------------------------------------------------
 *                      Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *      TableFuncNext
 *
 *      This is a workhorse for ExecTableFuncScan
 * ----------------------------------------------------------------
 */
unsafe fn TableFuncNext(node: *mut TableFuncScanState) -> *mut TupleTableSlot {
    let scanslot: *mut TupleTableSlot;

    scanslot = (*node).ss.ss_ScanTupleSlot;

    /*
     * If first time through, read all tuples from function and put them in a
     * tuplestore. Subsequent calls just fetch tuples from tuplestore.
     */
    if (*node).tupstore.is_null() {
        tfuncFetchRows(node, (*node).ss.ps.ps_ExprContext);
    }

    /*
     * Get the next tuple from tuplestore.
     */
    tuplestore_gettupleslot((*node).tupstore, true, false, scanslot);
    scanslot
}

/*
 * TableFuncRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn TableFuncRecheck(
    _node: *mut TableFuncScanState,
    _slot: *mut TupleTableSlot,
) -> bool {
    /* nothing to check */
    true
}

/* ----------------------------------------------------------------
 *      ExecTableFuncScan(node)
 *
 *      Scans the function sequentially and returns the next qualifying
 *      tuple.
 *      We call the ExecScan() routine and pass it the appropriate
 *      access method functions.
 * ----------------------------------------------------------------
 */
unsafe fn ExecTableFuncScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut TableFuncScanState =
        castNode!(TableFuncScanState, T_TableFuncScanState, pstate);

    ExecScan(
        &mut (*node).ss,
        Some(TableFuncNext_access),
        Some(TableFuncRecheck_recheck),
    )
}

/*
 * The C code casts TableFuncNext/TableFuncRecheck (which take
 * TableFuncScanState*) to ExecScanAccessMtd/ExecScanRecheckMtd (which take
 * ScanState*).  Since ScanState is the first member of TableFuncScanState, the
 * pointer values are identical; provide thin shim functions with the
 * ScanState* signature.
 */
unsafe fn TableFuncNext_access(node: *mut ScanState) -> *mut TupleTableSlot {
    let node = node as *mut TableFuncScanState;
    TableFuncNext(node)
}

unsafe fn TableFuncRecheck_recheck(node: *mut ScanState, slot: *mut TupleTableSlot) -> bool {
    let node = node as *mut TableFuncScanState;
    TableFuncRecheck(node, slot)
}

/* Silence unused-type warnings for the canonical callback aliases. */
const _: ExecScanAccessMtd = Some(TableFuncNext_access);
const _: ExecScanRecheckMtd = Some(TableFuncRecheck_recheck);

/* ----------------------------------------------------------------
 *      ExecInitTableFuncScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitTableFuncScan(
    node: *mut TableFuncScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut TableFuncScanState {
    let scanstate: *mut TableFuncScanState;
    let tf: *mut TableFunc = (*node).tablefunc;
    let tupdesc: TupleDesc;
    let mut i: c_int;

    /* check for unsupported flags */
    Assert!(eflags & EXEC_FLAG_MARK == 0);

    /*
     * TableFuncscan should not have any children.
     */
    Assert!(outerPlan(node as *mut Plan).is_null());
    Assert!(innerPlan(node as *mut Plan).is_null());

    /*
     * create new ScanState for node
     */
    scanstate = makeNode!(TableFuncScanState, T_TableFuncScanState);
    (*scanstate).ss.ps.plan = node as *mut Plan;
    (*scanstate).ss.ps.state = estate;
    (*scanstate).ss.ps.ExecProcNode = Some(ExecTableFuncScan);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*scanstate).ss.ps);

    /*
     * initialize source tuple type
     */
    tupdesc = BuildDescFromLists(
        (*tf).colnames,
        (*tf).coltypes,
        (*tf).coltypmods,
        (*tf).colcollations,
    );
    /* and the corresponding scan slot */
    ExecInitScanTupleSlot(estate, &mut (*scanstate).ss, tupdesc, &TTSOpsMinimalTuple);

    /*
     * Initialize result type and projection.
     */
    ExecInitResultTypeTL(&mut (*scanstate).ss.ps);
    ExecAssignScanProjectionInfo(&mut (*scanstate).ss);

    /*
     * initialize child expressions
     */
    (*scanstate).ss.ps.qual =
        ExecInitQual((*node).scan.plan.qual, &mut (*scanstate).ss.ps);

    /* Only XMLTABLE and JSON_TABLE are supported currently */
    (*scanstate).routine = if (*tf).functype == TFT_XMLTABLE {
        &raw const XmlTableRoutine as *const TableFuncRoutine
    } else {
        &raw const JsonbTableRoutine as *const TableFuncRoutine
    };

    (*scanstate).perTableCxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"TableFunc per value context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    ) as *mut _;
    (*scanstate).opaque = std::ptr::null_mut(); /* initialized at runtime */

    (*scanstate).ns_names = (*tf).ns_names;

    (*scanstate).ns_uris =
        ExecInitExprList((*tf).ns_uris, scanstate as *mut PlanState);
    (*scanstate).docexpr =
        ExecInitExpr((*tf).docexpr as *mut Expr, scanstate as *mut PlanState);
    (*scanstate).rowexpr =
        ExecInitExpr((*tf).rowexpr as *mut Expr, scanstate as *mut PlanState);
    (*scanstate).colexprs =
        ExecInitExprList((*tf).colexprs, scanstate as *mut PlanState);
    (*scanstate).coldefexprs =
        ExecInitExprList((*tf).coldefexprs, scanstate as *mut PlanState);
    (*scanstate).colvalexprs =
        ExecInitExprList((*tf).colvalexprs, scanstate as *mut PlanState);
    (*scanstate).passingvalexprs =
        ExecInitExprList((*tf).passingvalexprs, scanstate as *mut PlanState);

    (*scanstate).notnulls = (*tf).notnulls;

    /* these are allocated now and initialized later */
    (*scanstate).in_functions =
        palloc(std::mem::size_of::<FmgrInfo>() * (*tupdesc).natts as usize) as *mut FmgrInfo;
    (*scanstate).typioparams =
        palloc(std::mem::size_of::<Oid>() * (*tupdesc).natts as usize) as *mut Oid;

    /*
     * Fill in the necessary fmgr infos.
     */
    i = 0;
    while i < (*tupdesc).natts {
        let mut in_funcid: Oid = Oid::default();

        getTypeInputInfo(
            (*TupleDescAttr(tupdesc, i)).atttypid,
            &mut in_funcid,
            (*scanstate).typioparams.offset(i as isize),
        );
        fmgr_info(in_funcid, (*scanstate).in_functions.offset(i as isize) as *mut _);
        i += 1;
    }

    scanstate
}

/* ----------------------------------------------------------------
 *      ExecEndTableFuncScan
 *
 *      frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndTableFuncScan(node: *mut TableFuncScanState) {
    /*
     * Release tuplestore resources
     */
    if !(*node).tupstore.is_null() {
        tuplestore_end((*node).tupstore);
    }
    (*node).tupstore = std::ptr::null_mut();
}

/* ----------------------------------------------------------------
 *      ExecReScanTableFuncScan
 *
 *      Rescans the relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanTableFuncScan(node: *mut TableFuncScanState) {
    let chgparam: *mut Bitmapset = (*node).ss.ps.chgParam;

    if !(*node).ss.ps.ps_ResultTupleSlot.is_null() {
        ExecClearTuple((*node).ss.ps.ps_ResultTupleSlot);
    }
    ExecScanReScan(&mut (*node).ss);

    /*
     * Recompute when parameters are changed.
     */
    if !chgparam.is_null() {
        if !(*node).tupstore.is_null() {
            tuplestore_end((*node).tupstore);
            (*node).tupstore = std::ptr::null_mut();
        }
    }

    if !(*node).tupstore.is_null() {
        tuplestore_rescan((*node).tupstore);
    }
}

/* ----------------------------------------------------------------
 *      tfuncFetchRows
 *
 *      Read rows from a TableFunc producer
 * ----------------------------------------------------------------
 */
unsafe fn tfuncFetchRows(tstate: *mut TableFuncScanState, econtext: *mut ExprContext) {
    let routine: *const TableFuncRoutineImpl =
        (*tstate).routine as *const TableFuncRoutineImpl;
    let oldcxt: MemoryContext;
    let value: Datum;
    let mut isnull: bool = false;

    Assert!((*tstate).opaque.is_null());

    /* build tuplestore for the result */
    oldcxt = MemoryContextSwitchTo((*econtext).ecxt_per_query_memory);
    (*tstate).tupstore = tuplestore_begin_heap(false, false, work_mem);

    /*
     * Each call to fetch a new set of rows - of which there may be very many
     * if XMLTABLE or JSON_TABLE is being used in a lateral join - will
     * allocate a possibly substantial amount of memory, so we cannot use the
     * per-query context here. perTableCxt now serves the same function as
     * "argcontext" does in FunctionScan - a place to store per-one-call (i.e.
     * one result table) lifetime data (as opposed to per-query or
     * per-result-tuple).
     */
    MemoryContextSwitchTo((*tstate).perTableCxt);

    /*
     * PG_TRY / PG_CATCH: translated as a straight-line body; the catch arm
     * destroys the opaque builder state on error and re-throws.
     * TODO(pg-port): wire up real PG_TRY error recovery.
     */
    {
        ((*routine).InitOpaque)(
            tstate,
            (*(*(*tstate).ss.ss_ScanTupleSlot).tts_tupleDescriptor).natts,
        );

        /*
         * If evaluating the document expression returns NULL, the table
         * expression is empty and we return immediately.
         */
        value = ExecEvalExpr((*tstate).docexpr, econtext, &mut isnull);

        if !isnull {
            /* otherwise, pass the document value to the table builder */
            tfuncInitialize(tstate, econtext, value);

            /* initialize ordinality counter */
            (*tstate).ordinal = 1;

            /* Load all rows into the tuplestore, and we're done */
            tfuncLoadRows(tstate, econtext);
        }
    }

    /* clean up and return to original memory context */

    if !(*tstate).opaque.is_null() {
        ((*routine).DestroyOpaque)(tstate);
        (*tstate).opaque = std::ptr::null_mut();
    }

    MemoryContextSwitchTo(oldcxt);
    MemoryContextReset((*tstate).perTableCxt);
}

/*
 * Fill in namespace declarations, the row filter, and column filters in a
 * table expression builder context.
 */
unsafe fn tfuncInitialize(
    tstate: *mut TableFuncScanState,
    econtext: *mut ExprContext,
    doc: Datum,
) {
    let routine: *const TableFuncRoutineImpl =
        (*tstate).routine as *const TableFuncRoutineImpl;
    let tupdesc: TupleDesc;
    let mut isnull: bool = false;
    let mut colno: c_int;
    let mut value: Datum;
    let ordinalitycol: c_int =
        (*(*((*tstate).ss.ps.plan as *mut TableFuncScan)).tablefunc).ordinalitycol;

    /*
     * Install the document as a possibly-toasted Datum into the tablefunc
     * context.
     */
    ((*routine).SetDocument)(tstate, doc);

    /* Evaluate namespace specifications */
    forboth!(lc1, (*tstate).ns_uris, lc2, (*tstate).ns_names, {
        let expr: *mut ExprState = lfirst(lc1) as *mut ExprState;
        let ns_node: *mut PgString = lfirst_node!(PgString, T_String, lc2);
        let ns_uri: *mut c_char;
        let ns_name: *mut c_char;

        value = ExecEvalExpr(expr, econtext, &mut isnull);
        if isnull {
            ereport!(ERROR, "namespace URI must not be null");
        }
        ns_uri = TextDatumGetCString(value);

        /* DEFAULT is passed down to SetNamespace as NULL */
        ns_name = if !ns_node.is_null() {
            strVal!(ns_node)
        } else {
            std::ptr::null_mut()
        };

        ((*routine).SetNamespace)(tstate, ns_name, ns_uri);
    });

    /*
     * Install the row filter expression, if any, into the table builder
     * context.
     */
    if (*routine).SetRowFilter.is_some() {
        value = ExecEvalExpr((*tstate).rowexpr, econtext, &mut isnull);
        if isnull {
            ereport!(ERROR, "row filter expression must not be null");
        }

        ((*routine).SetRowFilter.unwrap())(tstate, TextDatumGetCString(value));
    }

    /*
     * Install the column filter expressions into the table builder context.
     * If an expression is given, use that; otherwise the column name itself
     * is the column filter.
     */
    colno = 0;
    tupdesc = (*(*tstate).ss.ss_ScanTupleSlot).tts_tupleDescriptor;
    foreach!(lc1, (*tstate).colexprs, {
        let colfilter: *mut c_char;
        let att: Form_pg_attribute = TupleDescAttr(tupdesc, colno);

        if colno != ordinalitycol {
            let colexpr: *mut ExprState = lfirst(current_cell!(lc1)) as *mut ExprState;

            if !colexpr.is_null() {
                value = ExecEvalExpr(colexpr, econtext, &mut isnull);
                if isnull {
                    ereport!(
                        ERROR,
                        "column filter expression must not be null"
                    );
                }
                colfilter = TextDatumGetCString(value);
            } else {
                colfilter = NameStr(&(*att).attname) as *mut c_char;
            }

            ((*routine).SetColumnFilter)(tstate, colfilter, colno);
        }

        colno += 1;
    });
}

/*
 * Load all the rows from the TableFunc table builder into a tuplestore.
 */
unsafe fn tfuncLoadRows(tstate: *mut TableFuncScanState, econtext: *mut ExprContext) {
    let routine: *const TableFuncRoutineImpl =
        (*tstate).routine as *const TableFuncRoutineImpl;
    let slot: *mut TupleTableSlot = (*tstate).ss.ss_ScanTupleSlot;
    let tupdesc: TupleDesc = (*slot).tts_tupleDescriptor;
    let values: *mut Datum = (*slot).tts_values;
    let nulls: *mut bool = (*slot).tts_isnull;
    let natts: c_int = (*tupdesc).natts;
    let oldcxt: MemoryContext;
    let ordinalitycol: c_int;

    ordinalitycol =
        (*(*((*tstate).ss.ps.plan as *mut TableFuncScan)).tablefunc).ordinalitycol;

    /*
     * We need a short-lived memory context that we can clean up each time
     * around the loop, to avoid wasting space. Our default per-tuple context
     * is fine for the job, since we won't have used it for anything yet in
     * this tuple cycle.
     */
    oldcxt = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    /*
     * Keep requesting rows from the table builder until there aren't any.
     */
    while ((*routine).FetchRow)(tstate) {
        let mut cell: *mut ListCell = list_head((*tstate).coldefexprs);
        let mut colno: c_int;

        CHECK_FOR_INTERRUPTS();

        ExecClearTuple((*tstate).ss.ss_ScanTupleSlot);

        /*
         * Obtain the value of each column for this row, installing them into
         * the slot; then add the tuple to the tuplestore.
         */
        colno = 0;
        while colno < natts {
            let att: Form_pg_attribute = TupleDescAttr(tupdesc, colno);

            if colno == ordinalitycol {
                /* Fast path for ordinality column */
                *values.offset(colno as isize) = Int32GetDatum((*tstate).ordinal as int32);
                (*tstate).ordinal += 1;
                *nulls.offset(colno as isize) = false;
            } else {
                let mut isnull: bool = false;

                *values.offset(colno as isize) = ((*routine).GetValue)(
                    tstate,
                    colno,
                    (*att).atttypid,
                    (*att).atttypmod,
                    &mut isnull,
                );

                /* No value?  Evaluate and apply the default, if any */
                if isnull && !cell.is_null() {
                    let coldefexpr: *mut ExprState = lfirst(cell) as *mut ExprState;

                    if !coldefexpr.is_null() {
                        *values.offset(colno as isize) =
                            ExecEvalExpr(coldefexpr, econtext, &mut isnull);
                    }
                }

                /* Verify a possible NOT NULL constraint */
                if isnull && bms_is_member(colno, (*tstate).notnulls) {
                    elog!(
                        ERROR,
                        "null is not allowed in column \"{}\"",
                        cstr_to_str(NameStr(&(*att).attname))
                    );
                }

                *nulls.offset(colno as isize) = isnull;
            }

            /* advance list of default expressions */
            if !cell.is_null() {
                cell = lnext((*tstate).coldefexprs, cell);
            }

            colno += 1;
        }

        tuplestore_putvalues((*tstate).tupstore, tupdesc, values, nulls);

        MemoryContextReset((*econtext).ecxt_per_tuple_memory);
    }

    MemoryContextSwitchTo(oldcxt);
}

/* ----------------------------------------------------------------
 *      Local concrete TableFuncRoutine method table
 *
 *  execnodes.rs exposes TableFuncRoutine only as an opaque type (referenced
 *  via const pointer).  To call the C method table we mirror its real layout
 *  here (src/include/executor/tablefunc.h) and cast through it.
 * ----------------------------------------------------------------
 */
#[repr(C)]
struct TableFuncRoutineImpl {
    InitOpaque: unsafe fn(state: *mut TableFuncScanState, natts: c_int),
    SetDocument: unsafe fn(state: *mut TableFuncScanState, value: Datum),
    SetNamespace:
        unsafe fn(state: *mut TableFuncScanState, name: *const c_char, uri: *const c_char),
    SetRowFilter: Option<unsafe fn(state: *mut TableFuncScanState, path: *const c_char)>,
    SetColumnFilter:
        unsafe fn(state: *mut TableFuncScanState, path: *const c_char, colnum: c_int),
    FetchRow: unsafe fn(state: *mut TableFuncScanState) -> bool,
    GetValue: unsafe fn(
        state: *mut TableFuncScanState,
        colnum: c_int,
        typid: Oid,
        typmod: int32,
        isnull: *mut bool,
    ) -> Datum,
    DestroyOpaque: unsafe fn(state: *mut TableFuncScanState),
}

/* ----------------------------------------------------------------
 *      Local stubs for not-yet-ported dependencies
 * ----------------------------------------------------------------
 */

/* utils/xml.c: const TableFuncRoutine XmlTableRoutine */
/* TODO: utils/adt/xml.c - real XMLTABLE method table; stub methods panic. */
static XmlTableRoutine: TableFuncRoutineImpl = TableFuncRoutineImpl {
    InitOpaque: tf_stub_InitOpaque,
    SetDocument: tf_stub_SetDocument,
    SetNamespace: tf_stub_SetNamespace,
    SetRowFilter: Some(tf_stub_SetRowFilter),
    SetColumnFilter: tf_stub_SetColumnFilter,
    FetchRow: tf_stub_FetchRow,
    GetValue: tf_stub_GetValue,
    DestroyOpaque: tf_stub_DestroyOpaque,
};

/* utils/jsonpath_exec.c: const TableFuncRoutine JsonbTableRoutine */
/* TODO: utils/adt/jsonpath_exec.c - real JSON_TABLE method table; stub. */
static JsonbTableRoutine: TableFuncRoutineImpl = TableFuncRoutineImpl {
    InitOpaque: tf_stub_InitOpaque,
    SetDocument: tf_stub_SetDocument,
    SetNamespace: tf_stub_SetNamespace,
    SetRowFilter: Some(tf_stub_SetRowFilter),
    SetColumnFilter: tf_stub_SetColumnFilter,
    FetchRow: tf_stub_FetchRow,
    GetValue: tf_stub_GetValue,
    DestroyOpaque: tf_stub_DestroyOpaque,
};

unsafe fn tf_stub_InitOpaque(_state: *mut TableFuncScanState, _natts: c_int) {
    unimplemented!() // TODO: utils/adt/xml.c / jsonpath_exec.c
}
unsafe fn tf_stub_SetDocument(_state: *mut TableFuncScanState, _value: Datum) {
    unimplemented!() // TODO: utils/adt/xml.c / jsonpath_exec.c
}
unsafe fn tf_stub_SetNamespace(
    _state: *mut TableFuncScanState,
    _name: *const c_char,
    _uri: *const c_char,
) {
    unimplemented!() // TODO: utils/adt/xml.c / jsonpath_exec.c
}
unsafe fn tf_stub_SetRowFilter(_state: *mut TableFuncScanState, _path: *const c_char) {
    unimplemented!() // TODO: utils/adt/xml.c / jsonpath_exec.c
}
unsafe fn tf_stub_SetColumnFilter(
    _state: *mut TableFuncScanState,
    _path: *const c_char,
    _colnum: c_int,
) {
    unimplemented!() // TODO: utils/adt/xml.c / jsonpath_exec.c
}
unsafe fn tf_stub_FetchRow(_state: *mut TableFuncScanState) -> bool {
    unimplemented!() // TODO: utils/adt/xml.c / jsonpath_exec.c
}
unsafe fn tf_stub_GetValue(
    _state: *mut TableFuncScanState,
    _colnum: c_int,
    _typid: Oid,
    _typmod: int32,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: utils/adt/xml.c / jsonpath_exec.c
}
unsafe fn tf_stub_DestroyOpaque(_state: *mut TableFuncScanState) {
    unimplemented!() // TODO: utils/adt/xml.c / jsonpath_exec.c
}

/* executor/execExpr.c: ExecEvalExpr() */
unsafe fn ExecEvalExpr(
    _state: *mut ExprState,
    _econtext: *mut ExprContext,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: executor/execExprInterp.c
}

/* parse_type.c / lsyscache.c: getTypeInputInfo() */
unsafe fn getTypeInputInfo(_typid: Oid, _typInput: *mut Oid, _typIOParam: *mut Oid) {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}

/* utils/sort/tuplestore.c: tuplestore_begin_heap() */
unsafe fn tuplestore_begin_heap(
    _randomAccess: bool,
    _interXact: bool,
    _maxKBytes: c_int,
) -> *mut Tuplestorestate {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

/* utils/sort/tuplestore.c: tuplestore_gettupleslot() */
unsafe fn tuplestore_gettupleslot(
    _state: *mut Tuplestorestate,
    _forward: bool,
    _copy: bool,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

/* utils/sort/tuplestore.c: tuplestore_putvalues() */
unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

/* utils/sort/tuplestore.c: tuplestore_end() */
unsafe fn tuplestore_end(_state: *mut Tuplestorestate) {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

/* utils/sort/tuplestore.c: tuplestore_rescan() */
unsafe fn tuplestore_rescan(_state: *mut Tuplestorestate) {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

/* helper: render a NUL-terminated C string for ereport formatting */
unsafe fn cstr_to_str<'a>(s: *const c_char) -> &'a str {
    if s.is_null() {
        return "";
    }
    std::ffi::CStr::from_ptr(s).to_str().unwrap_or("")
}
