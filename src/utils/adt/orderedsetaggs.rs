//! Translation of postgres/src/backend/utils/adt/orderedsetaggs.c
//!
//! Ordered-set aggregate functions.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   catalog/pg_aggregate.h    -> crate::catalog::pg_aggregate (AGGKIND_*)
//!   catalog/pg_operator.h     -> crate::catalog::pg_known_oids (Int4LessOperator/Int4EqualOperator)
//!   catalog/pg_type.h         -> crate::catalog::pg_type_d (INT4OID/FLOAT8OID/INTERVALOID)
//!   executor/executor.h       -> crate::executor::executor / execTuples / execGrouping / execUtils / tuptable
//!   miscadmin.h               -> crate::miscadmin (work_mem)
//!   nodes/nodeFuncs.h         -> crate::nodes::nodeFuncs (exprType/exprCollation)
//!   optimizer/optimizer.h     -> crate::optimizer::optimizer (get_sortgroupclause_tle)
//!   utils/array.h             -> crate::utils::array / crate::utils::adt::arrayfuncs (ArrayType, ARR_* etc)
//!   utils/fmgrprotos.h        -> crate::utils::fmgr (PG_* macros, FunctionCall2Coll etc)
//!   utils/lsyscache.h         -> crate::utils::cache::lsyscache (get_opcode/get_typlenbyvalalign)
//!   utils/tuplesort.h         -> crate::utils::sort::tuplesort / tuplesortvariants

use crate::prelude::*;

use crate::access::attnum::AttrNumber;
use crate::catalog::pg_aggregate::{AGGKIND_HYPOTHETICAL, AGGKIND_IS_ORDERED_SET};
use crate::catalog::pg_known_oids::{Int4EqualOperator, Int4LessOperator};
use crate::catalog::pg_type::TYPALIGN_DOUBLE;
use crate::catalog::pg_type_d::{FLOAT8OID, INT4OID, INTERVALOID};

use crate::access::common::tupdesc::{
    CreateTemplateTupleDesc, FreeTupleDesc, TupleDesc, TupleDescAttr, TupleDescCopyEntry,
    TupleDescInitEntry,
};

use crate::executor::execGrouping::execTuplesMatchPrepare;
use crate::executor::execTuples::{
    ExecDropSingleTupleTableSlot, ExecStoreVirtualTuple, MakeSingleTupleTableSlot,
    TTSOpsMinimalTuple,
};
use crate::executor::execUtils::CreateStandaloneExprContext;
use crate::executor::executor::{ExecQualAndReset, ExecTypeFromTL};
use crate::executor::nodeAgg::{
    AggCheckCallContext, AggGetAggref, AggRegisterCallback, AggStateIsShared, AGG_CONTEXT_AGGREGATE,
};
use crate::executor::tuptable::{slot_getattr, ExecClearTuple, TupIsNull, TupleTableSlot};

use crate::miscadmin::{work_mem, CHECK_FOR_INTERRUPTS};

use crate::nodes::execnodes::{ExprContext, ExprState};
use crate::nodes::nodeFuncs::{exprCollation, exprType};
use crate::nodes::parsenodes::SortGroupClause;
use crate::nodes::pg_list::{lfirst, linitial, list_length, List};
use crate::nodes::primnodes::{Aggref, Node, TargetEntry};

use crate::optimizer::optimizer::get_sortgroupclause_tle;

use crate::utils::adt::arrayfuncs::{
    construct_empty_array, construct_md_array, deconstruct_array_builtin,
};
use crate::utils::array::{ARR_DIMS, ARR_LBOUND, ARR_NDIM, ArrayType};
use crate::utils::cache::lsyscache::{get_opcode, get_typlenbyvalalign};
use crate::utils::fmgr::{
    fmgr_info_cxt, get_fn_expr_argtype, DirectFunctionCall2Coll, FmgrInfo, FunctionCall2Coll,
    FunctionCallInfo,
};
use crate::utils::sort::tuplesort::{
    tuplesort_end, tuplesort_performsort, tuplesort_rescan, tuplesort_skiptuples, Tuplesortstate,
    TUPLESORT_NONE, TUPLESORT_RANDOMACCESS,
};
use crate::utils::sort::tuplesortvariants::{
    tuplesort_begin_datum, tuplesort_begin_heap, tuplesort_getdatum, tuplesort_gettupleslot,
    tuplesort_putdatum, tuplesort_puttupleslot,
};

use crate::postgres::{
    DatumGetBool, DatumGetFloat8, DatumGetInt32, DatumGetPointer, Float8GetDatum, Int32GetDatum,
    PointerGetDatum,
};

use core::ffi::c_char;

// Wrappers used by interval_lerp (DirectFunctionCall2 with InvalidOid collation).
// These dependency fns live in crate::utils::adt::timestamp (interval_mi/mul/pl).
// TODO(pg-port): import paths via fmgr-callable PGFunction pointers; here we call
// them directly through the C-style DirectFunctionCall2 helper.
use crate::utils::adt::timestamp::{interval_mi, interval_mul, interval_pl};

/*
 * Generic support for ordered-set aggregates
 *
 * The state for an ordered-set aggregate is divided into a per-group struct
 * (which is the internal-type transition state datum returned to nodeAgg.c)
 * and a per-query struct, which contains data and sub-objects that we can
 * create just once per query because they will not change across groups.
 * The per-query struct and subsidiary data live in the executor's per-query
 * memory context, and go away implicitly at ExecutorEnd().
 *
 * These structs are set up during the first call of the transition function.
 * Because we allow nodeAgg.c to merge ordered-set aggregates (but not
 * hypothetical aggregates) with identical inputs and transition functions,
 * this info must not depend on the particular aggregate (ie, particular
 * final-function), nor on the direct argument(s) of the aggregate.
 */

#[repr(C)]
pub struct OSAPerQueryState {
    /* Representative Aggref for this aggregate: */
    pub aggref: *mut Aggref,
    /* Memory context containing this struct and other per-query data: */
    pub qcontext: MemoryContext,
    /* Context for expression evaluation */
    pub econtext: *mut ExprContext,
    /* Do we expect multiple final-function calls within one group? */
    pub rescan_needed: bool,

    /* These fields are used only when accumulating tuples: */

    /* Tuple descriptor for tuples inserted into sortstate: */
    pub tupdesc: TupleDesc,
    /* Tuple slot we can use for inserting/extracting tuples: */
    pub tupslot: *mut TupleTableSlot,
    /* Per-sort-column sorting information */
    pub numSortCols: c_int,
    pub sortColIdx: *mut AttrNumber,
    pub sortOperators: *mut Oid,
    pub eqOperators: *mut Oid,
    pub sortCollations: *mut Oid,
    pub sortNullsFirsts: *mut bool,
    /* Equality operator call info, created only if needed: */
    pub compareTuple: *mut ExprState,

    /* These fields are used only when accumulating datums: */

    /* Info about datatype of datums being sorted: */
    pub sortColType: Oid,
    pub typLen: i16,
    pub typByVal: bool,
    pub typAlign: c_char,
    /* Info about sort ordering: */
    pub sortOperator: Oid,
    pub eqOperator: Oid,
    pub sortCollation: Oid,
    pub sortNullsFirst: bool,
    /* Equality operator call info, created only if needed: */
    pub equalfn: FmgrInfo,
}

#[repr(C)]
pub struct OSAPerGroupState {
    /* Link to the per-query state for this aggregate: */
    pub qstate: *mut OSAPerQueryState,
    /* Memory context containing per-group data: */
    pub gcontext: MemoryContext,
    /* Sort object we're accumulating data in: */
    pub sortstate: *mut Tuplesortstate,
    /* Number of normal rows inserted into sortstate: */
    pub number_of_rows: int64,
    /* Have we already done tuplesort_performsort? */
    pub sort_done: bool,
}

/*
 * Set up working state for an ordered-set aggregate
 */
unsafe fn ordered_set_startup(
    fcinfo: FunctionCallInfo,
    use_tuples: bool,
) -> *mut OSAPerGroupState {
    let osastate: *mut OSAPerGroupState;
    let mut qstate: *mut OSAPerQueryState;
    let mut gcontext: MemoryContext = null_mut();
    let mut oldcontext: MemoryContext;
    let mut tuplesortopt: c_int;

    /*
     * Check we're called as aggregate (and not a window function), and get
     * the Agg node's group-lifespan context (which might change from group to
     * group, so we shouldn't cache it in the per-query state).
     */
    if AggCheckCallContext(fcinfo, &mut gcontext) != AGG_CONTEXT_AGGREGATE {
        elog!(ERROR, "ordered-set aggregate called in non-aggregate context");
    }

    /*
     * We keep a link to the per-query state in fn_extra; if it's not there,
     * create it, and do the per-query setup we need.
     */
    qstate = (*(*fcinfo).flinfo).fn_extra as *mut OSAPerQueryState;
    if qstate.is_null() {
        let aggref: *mut Aggref;
        let qcontext: MemoryContext;
        let sortlist: *mut List;
        let mut numSortCols: c_int;

        /* Get the Aggref so we can examine aggregate's arguments */
        aggref = AggGetAggref(fcinfo);
        if aggref.is_null() {
            elog!(ERROR, "ordered-set aggregate called in non-aggregate context");
        }
        if !AGGKIND_IS_ORDERED_SET((*aggref).aggkind) {
            elog!(
                ERROR,
                "ordered-set aggregate support function called for non-ordered-set aggregate"
            );
        }

        /*
         * Prepare per-query structures in the fn_mcxt, which we assume is the
         * executor's per-query context; in any case it's the right place to
         * keep anything found via fn_extra.
         */
        qcontext = (*(*fcinfo).flinfo).fn_mcxt;
        oldcontext = MemoryContextSwitchTo(qcontext);

        qstate = palloc0(core::mem::size_of::<OSAPerQueryState>()) as *mut OSAPerQueryState;
        (*qstate).aggref = aggref;
        (*qstate).qcontext = qcontext;

        /* We need to support rescans if the trans state is shared */
        (*qstate).rescan_needed = AggStateIsShared(fcinfo);

        /* Extract the sort information */
        sortlist = (*aggref).aggorder;
        numSortCols = list_length(sortlist);

        if use_tuples {
            let ishypothetical: bool = (*aggref).aggkind == AGGKIND_HYPOTHETICAL;
            let mut i: c_int;

            if ishypothetical {
                numSortCols += 1; /* make space for flag column */
            }
            (*qstate).numSortCols = numSortCols;
            (*qstate).sortColIdx =
                palloc(numSortCols as usize * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;
            (*qstate).sortOperators =
                palloc(numSortCols as usize * core::mem::size_of::<Oid>()) as *mut Oid;
            (*qstate).eqOperators =
                palloc(numSortCols as usize * core::mem::size_of::<Oid>()) as *mut Oid;
            (*qstate).sortCollations =
                palloc(numSortCols as usize * core::mem::size_of::<Oid>()) as *mut Oid;
            (*qstate).sortNullsFirsts =
                palloc(numSortCols as usize * core::mem::size_of::<bool>()) as *mut bool;

            i = 0;
            foreach!(lc, sortlist, {
                let sortcl = lfirst(crate::current_cell!(lc)) as *mut SortGroupClause;
                let tle: *mut TargetEntry = get_sortgroupclause_tle(sortcl, (*aggref).args);

                /* the parser should have made sure of this */
                Assert!(OidIsValid((*sortcl).sortop));

                *(*qstate).sortColIdx.offset(i as isize) = (*tle).resno;
                *(*qstate).sortOperators.offset(i as isize) = (*sortcl).sortop;
                *(*qstate).eqOperators.offset(i as isize) = (*sortcl).eqop;
                *(*qstate).sortCollations.offset(i as isize) =
                    exprCollation((*tle).expr as *const Node);
                *(*qstate).sortNullsFirsts.offset(i as isize) = (*sortcl).nulls_first;
                i += 1;
            });

            if ishypothetical {
                /* Add an integer flag column as the last sort column */
                *(*qstate).sortColIdx.offset(i as isize) =
                    (list_length((*aggref).args) + 1) as AttrNumber;
                *(*qstate).sortOperators.offset(i as isize) = Int4LessOperator;
                *(*qstate).eqOperators.offset(i as isize) = Int4EqualOperator;
                *(*qstate).sortCollations.offset(i as isize) = InvalidOid;
                *(*qstate).sortNullsFirsts.offset(i as isize) = false;
                i += 1;
            }

            Assert!(i == numSortCols);

            /*
             * Get a tupledesc corresponding to the aggregated inputs
             * (including sort expressions) of the agg.
             */
            (*qstate).tupdesc = ExecTypeFromTL((*aggref).args);

            /* If we need a flag column, hack the tupledesc to include that */
            if ishypothetical {
                let newdesc: TupleDesc;
                let mut natts: c_int = (*(*qstate).tupdesc).natts;

                newdesc = CreateTemplateTupleDesc(natts + 1);
                let mut j: c_int = 1;
                while j <= natts {
                    TupleDescCopyEntry(newdesc, j, (*qstate).tupdesc, j);
                    j += 1;
                }

                natts += 1;
                TupleDescInitEntry(
                    newdesc,
                    natts as AttrNumber,
                    c"flag".as_ptr(),
                    INT4OID,
                    -1,
                    0,
                );

                FreeTupleDesc((*qstate).tupdesc);
                (*qstate).tupdesc = newdesc;
            }

            /* Create slot we'll use to store/retrieve rows */
            (*qstate).tupslot = MakeSingleTupleTableSlot((*qstate).tupdesc, &TTSOpsMinimalTuple);
        } else {
            /* Sort single datums */
            let sortcl: *mut SortGroupClause;
            let tle: *mut TargetEntry;

            if numSortCols != 1 || (*aggref).aggkind == AGGKIND_HYPOTHETICAL {
                elog!(
                    ERROR,
                    "ordered-set aggregate support function does not support multiple aggregated columns"
                );
            }

            sortcl = linitial(sortlist) as *mut SortGroupClause;
            tle = get_sortgroupclause_tle(sortcl, (*aggref).args);

            /* the parser should have made sure of this */
            Assert!(OidIsValid((*sortcl).sortop));

            /* Save sort ordering info */
            (*qstate).sortColType = exprType((*tle).expr as *const Node);
            (*qstate).sortOperator = (*sortcl).sortop;
            (*qstate).eqOperator = (*sortcl).eqop;
            (*qstate).sortCollation = exprCollation((*tle).expr as *const Node);
            (*qstate).sortNullsFirst = (*sortcl).nulls_first;

            /* Save datatype info */
            get_typlenbyvalalign(
                (*qstate).sortColType,
                &mut (*qstate).typLen,
                &mut (*qstate).typByVal,
                &mut (*qstate).typAlign,
            );
        }

        (*(*fcinfo).flinfo).fn_extra = qstate as *mut c_void;

        MemoryContextSwitchTo(oldcontext);
    }

    /* Now build the stuff we need in group-lifespan context */
    oldcontext = MemoryContextSwitchTo(gcontext);

    osastate = palloc(core::mem::size_of::<OSAPerGroupState>()) as *mut OSAPerGroupState;
    (*osastate).qstate = qstate;
    (*osastate).gcontext = gcontext;

    tuplesortopt = TUPLESORT_NONE;

    if (*qstate).rescan_needed {
        tuplesortopt |= TUPLESORT_RANDOMACCESS;
    }

    /*
     * Initialize tuplesort object.
     */
    if use_tuples {
        (*osastate).sortstate = tuplesort_begin_heap(
            (*qstate).tupdesc,
            (*qstate).numSortCols,
            (*qstate).sortColIdx,
            (*qstate).sortOperators,
            (*qstate).sortCollations,
            (*qstate).sortNullsFirsts,
            work_mem,
            null_mut(),
            tuplesortopt,
        );
    } else {
        (*osastate).sortstate = tuplesort_begin_datum(
            (*qstate).sortColType,
            (*qstate).sortOperator,
            (*qstate).sortCollation,
            (*qstate).sortNullsFirst,
            work_mem,
            null_mut(),
            tuplesortopt,
        );
    }

    (*osastate).number_of_rows = 0;
    (*osastate).sort_done = false;

    /* Now register a shutdown callback to clean things up at end of group */
    AggRegisterCallback(
        fcinfo,
        Some(ordered_set_shutdown),
        PointerGetDatum(osastate as *const c_void),
    );

    MemoryContextSwitchTo(oldcontext);

    osastate
}

/*
 * Clean up when evaluation of an ordered-set aggregate is complete.
 *
 * We don't need to bother freeing objects in the per-group memory context,
 * since that will get reset anyway by nodeAgg.c; nor should we free anything
 * in the per-query context, which will get cleared (if this was the last
 * group) by ExecutorEnd.  But we must take care to release any potential
 * non-memory resources.
 *
 * In the case where we're not expecting multiple finalfn calls, we could
 * arguably rely on the finalfn to clean up; but it's easier and more testable
 * if we just do it the same way in either case.
 */
unsafe fn ordered_set_shutdown(arg: Datum) {
    let osastate: *mut OSAPerGroupState = DatumGetPointer(arg) as *mut OSAPerGroupState;

    /* Tuplesort object might have temp files. */
    if !(*osastate).sortstate.is_null() {
        tuplesort_end((*osastate).sortstate);
    }
    (*osastate).sortstate = null_mut();
    /* The tupleslot probably can't be holding a pin, but let's be safe. */
    if !(*(*osastate).qstate).tupslot.is_null() {
        ExecClearTuple((*(*osastate).qstate).tupslot);
    }
}

// Bring the crate-root PG_* fmgr macros into scope for this module.
use crate::{
    PG_ARGISNULL, PG_GETARG_DATUM, PG_GETARG_FLOAT8, PG_GETARG_POINTER, PG_GET_COLLATION, PG_NARGS,
    PG_RETURN_DATUM, PG_RETURN_FLOAT8, PG_RETURN_INT64, PG_RETURN_NULL, PG_RETURN_POINTER,
};

// PG_GETARG_ARRAYTYPE_P(n): DatumGetPointer(PG_GETARG_DATUM(n)) as *mut ArrayType
// (utils/array.h). Defined locally to mirror sibling adt files.
macro_rules! PG_GETARG_ARRAYTYPE_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut ArrayType
    };
}

/*
 * Generic transition function for ordered-set aggregates
 * with a single input column in which we want to suppress nulls
 */
#[unsafe(no_mangle)]
pub unsafe fn ordered_set_transition(fcinfo: FunctionCallInfo) -> Datum {
    let osastate: *mut OSAPerGroupState;

    /* If first call, create the transition state workspace */
    if PG_ARGISNULL!(fcinfo, 0) {
        osastate = ordered_set_startup(fcinfo, false);
    } else {
        osastate = PG_GETARG_POINTER!(fcinfo, 0) as *mut OSAPerGroupState;
    }

    /* Load the datum into the tuplesort object, but only if it's not null */
    if !PG_ARGISNULL!(fcinfo, 1) {
        tuplesort_putdatum((*osastate).sortstate, PG_GETARG_DATUM!(fcinfo, 1), false);
        (*osastate).number_of_rows += 1;
    }

    PG_RETURN_POINTER!(osastate as *const c_void)
}

/*
 * Generic transition function for ordered-set aggregates
 * with (potentially) multiple aggregated input columns
 */
#[unsafe(no_mangle)]
pub unsafe fn ordered_set_transition_multi(fcinfo: FunctionCallInfo) -> Datum {
    let osastate: *mut OSAPerGroupState;
    let slot: *mut TupleTableSlot;
    let nargs: c_int;
    let mut i: c_int;

    /* If first call, create the transition state workspace */
    if PG_ARGISNULL!(fcinfo, 0) {
        osastate = ordered_set_startup(fcinfo, true);
    } else {
        osastate = PG_GETARG_POINTER!(fcinfo, 0) as *mut OSAPerGroupState;
    }

    /* Form a tuple from all the other inputs besides the transition value */
    slot = (*(*osastate).qstate).tupslot;
    ExecClearTuple(slot);
    nargs = PG_NARGS!(fcinfo) - 1;
    i = 0;
    while i < nargs {
        *(*slot).tts_values.offset(i as isize) = PG_GETARG_DATUM!(fcinfo, i + 1);
        *(*slot).tts_isnull.offset(i as isize) = PG_ARGISNULL!(fcinfo, i + 1);
        i += 1;
    }
    if (*(*(*osastate).qstate).aggref).aggkind == AGGKIND_HYPOTHETICAL {
        /* Add a zero flag value to mark this row as a normal input row */
        *(*slot).tts_values.offset(i as isize) = Int32GetDatum(0);
        *(*slot).tts_isnull.offset(i as isize) = false;
        i += 1;
    }
    Assert!(i == (*(*slot).tts_tupleDescriptor).natts);
    ExecStoreVirtualTuple(slot);

    /* Load the row into the tuplesort object */
    tuplesort_puttupleslot((*osastate).sortstate, slot);
    (*osastate).number_of_rows += 1;

    PG_RETURN_POINTER!(osastate as *const c_void)
}

/*
 * percentile_disc(float8) within group(anyelement) - discrete percentile
 */
#[unsafe(no_mangle)]
pub unsafe fn percentile_disc_final(fcinfo: FunctionCallInfo) -> Datum {
    let osastate: *mut OSAPerGroupState;
    let percentile: f64;
    let mut val: Datum = 0;
    let mut isnull: bool = false;
    let rownum: int64;

    Assert!(AggCheckCallContext(fcinfo, null_mut()) == AGG_CONTEXT_AGGREGATE);

    /* Get and check the percentile argument */
    if PG_ARGISNULL!(fcinfo, 1) {
        return PG_RETURN_NULL!();
    }

    percentile = PG_GETARG_FLOAT8!(fcinfo, 1);

    if percentile < 0.0 || percentile > 1.0 || percentile.is_nan() {
        // C also: errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        ereport!(
            ERROR,
            errmsg!("percentile value {} is not between 0 and 1", percentile)
        );
    }

    /* If there were no regular rows, the result is NULL */
    if PG_ARGISNULL!(fcinfo, 0) {
        return PG_RETURN_NULL!();
    }

    osastate = PG_GETARG_POINTER!(fcinfo, 0) as *mut OSAPerGroupState;

    /* number_of_rows could be zero if we only saw NULL input values */
    if (*osastate).number_of_rows == 0 {
        return PG_RETURN_NULL!();
    }

    /* Finish the sort, or rescan if we already did */
    if !(*osastate).sort_done {
        tuplesort_performsort((*osastate).sortstate);
        (*osastate).sort_done = true;
    } else {
        tuplesort_rescan((*osastate).sortstate);
    }

    /*----------
     * We need the smallest K such that (K/N) >= percentile.
     * N>0, therefore K >= N*percentile, therefore K = ceil(N*percentile).
     * So we skip K-1 rows (if K>0) and return the next row fetched.
     *----------
     */
    rownum = (percentile * (*osastate).number_of_rows as f64).ceil() as int64;
    Assert!(rownum <= (*osastate).number_of_rows);

    if rownum > 1 {
        if !tuplesort_skiptuples((*osastate).sortstate, rownum - 1, true) {
            elog!(ERROR, "missing row in percentile_disc");
        }
    }

    if !tuplesort_getdatum(
        (*osastate).sortstate,
        true,
        true,
        &mut val,
        &mut isnull,
        null_mut(),
    ) {
        elog!(ERROR, "missing row in percentile_disc");
    }

    /* We shouldn't have stored any nulls, but do the right thing anyway */
    if isnull {
        PG_RETURN_NULL!()
    } else {
        PG_RETURN_DATUM!(val)
    }
}

/*
 * For percentile_cont, we need a way to interpolate between consecutive
 * values. Use a helper function for that, so that we can share the rest
 * of the code between types.
 */
type LerpFunc = unsafe fn(lo: Datum, hi: Datum, pct: f64) -> Datum;

unsafe fn float8_lerp(lo: Datum, hi: Datum, pct: f64) -> Datum {
    let loval: f64 = DatumGetFloat8(lo);
    let hival: f64 = DatumGetFloat8(hi);

    Float8GetDatum(loval + (pct * (hival - loval)))
}

unsafe fn interval_lerp(lo: Datum, hi: Datum, pct: f64) -> Datum {
    let diff_result: Datum = DirectFunctionCall2Coll(interval_mi, InvalidOid, hi, lo);
    let mul_result: Datum = DirectFunctionCall2Coll(
        interval_mul,
        InvalidOid,
        diff_result,
        Float8GetDatum(pct),
    );

    DirectFunctionCall2Coll(interval_pl, InvalidOid, mul_result, lo)
}

/*
 * Continuous percentile
 */
unsafe fn percentile_cont_final_common(
    fcinfo: FunctionCallInfo,
    expect_type: Oid,
    lerpfunc: LerpFunc,
) -> Datum {
    let osastate: *mut OSAPerGroupState;
    let percentile: f64;
    let first_row: int64;
    let second_row: int64;
    let val: Datum;
    let mut first_val: Datum = 0;
    let mut second_val: Datum = 0;
    let proportion: f64;
    let mut isnull: bool = false;

    Assert!(AggCheckCallContext(fcinfo, null_mut()) == AGG_CONTEXT_AGGREGATE);

    /* Get and check the percentile argument */
    if PG_ARGISNULL!(fcinfo, 1) {
        return PG_RETURN_NULL!();
    }

    percentile = PG_GETARG_FLOAT8!(fcinfo, 1);

    if percentile < 0.0 || percentile > 1.0 || percentile.is_nan() {
        // C also: errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        ereport!(
            ERROR,
            errmsg!("percentile value {} is not between 0 and 1", percentile)
        );
    }

    /* If there were no regular rows, the result is NULL */
    if PG_ARGISNULL!(fcinfo, 0) {
        return PG_RETURN_NULL!();
    }

    osastate = PG_GETARG_POINTER!(fcinfo, 0) as *mut OSAPerGroupState;

    /* number_of_rows could be zero if we only saw NULL input values */
    if (*osastate).number_of_rows == 0 {
        return PG_RETURN_NULL!();
    }

    Assert!(expect_type == (*(*osastate).qstate).sortColType);

    /* Finish the sort, or rescan if we already did */
    if !(*osastate).sort_done {
        tuplesort_performsort((*osastate).sortstate);
        (*osastate).sort_done = true;
    } else {
        tuplesort_rescan((*osastate).sortstate);
    }

    first_row = (percentile * ((*osastate).number_of_rows - 1) as f64).floor() as int64;
    second_row = (percentile * ((*osastate).number_of_rows - 1) as f64).ceil() as int64;

    Assert!(first_row < (*osastate).number_of_rows);

    if !tuplesort_skiptuples((*osastate).sortstate, first_row, true) {
        elog!(ERROR, "missing row in percentile_cont");
    }

    if !tuplesort_getdatum(
        (*osastate).sortstate,
        true,
        true,
        &mut first_val,
        &mut isnull,
        null_mut(),
    ) {
        elog!(ERROR, "missing row in percentile_cont");
    }
    if isnull {
        return PG_RETURN_NULL!();
    }

    if first_row == second_row {
        val = first_val;
    } else {
        if !tuplesort_getdatum(
            (*osastate).sortstate,
            true,
            true,
            &mut second_val,
            &mut isnull,
            null_mut(),
        ) {
            elog!(ERROR, "missing row in percentile_cont");
        }

        if isnull {
            return PG_RETURN_NULL!();
        }

        proportion = (percentile * ((*osastate).number_of_rows - 1) as f64) - first_row as f64;
        val = lerpfunc(first_val, second_val, proportion);
    }

    PG_RETURN_DATUM!(val)
}

/*
 * percentile_cont(float8) within group (float8)	- continuous percentile
 */
#[unsafe(no_mangle)]
pub unsafe fn percentile_cont_float8_final(fcinfo: FunctionCallInfo) -> Datum {
    percentile_cont_final_common(fcinfo, FLOAT8OID, float8_lerp)
}

/*
 * percentile_cont(float8) within group (interval)	- continuous percentile
 */
#[unsafe(no_mangle)]
pub unsafe fn percentile_cont_interval_final(fcinfo: FunctionCallInfo) -> Datum {
    percentile_cont_final_common(fcinfo, INTERVALOID, interval_lerp)
}

/*
 * Support code for handling arrays of percentiles
 *
 * Note: in each pct_info entry, second_row should be equal to or
 * exactly one more than first_row.
 */
#[repr(C)]
struct pct_info {
    first_row: int64,  /* first row to sample */
    second_row: int64, /* possible second row to sample */
    proportion: f64,   /* interpolation fraction */
    idx: c_int,        /* index of this item in original array */
}

/*
 * Sort comparator to sort pct_infos by first_row then second_row
 */
unsafe fn pct_info_cmp(a: &pct_info, b: &pct_info) -> core::cmp::Ordering {
    if a.first_row != b.first_row {
        return if a.first_row < b.first_row {
            core::cmp::Ordering::Less
        } else {
            core::cmp::Ordering::Greater
        };
    }
    if a.second_row != b.second_row {
        return if a.second_row < b.second_row {
            core::cmp::Ordering::Less
        } else {
            core::cmp::Ordering::Greater
        };
    }
    core::cmp::Ordering::Equal
}

/*
 * Construct array showing which rows to sample for percentiles.
 */
unsafe fn setup_pct_info(
    num_percentiles: c_int,
    percentiles_datum: *mut Datum,
    percentiles_null: *mut bool,
    rowcount: int64,
    continuous: bool,
) -> *mut pct_info {
    let pct_info_arr: *mut pct_info;
    let mut i: c_int;

    pct_info_arr =
        palloc(num_percentiles as usize * core::mem::size_of::<pct_info>()) as *mut pct_info;

    i = 0;
    while i < num_percentiles {
        let entry = &mut *pct_info_arr.offset(i as isize);
        entry.idx = i;

        if *percentiles_null.offset(i as isize) {
            /* dummy entry for any NULL in array */
            entry.first_row = 0;
            entry.second_row = 0;
            entry.proportion = 0.0;
        } else {
            let p: f64 = DatumGetFloat8(*percentiles_datum.offset(i as isize));

            if p < 0.0 || p > 1.0 || p.is_nan() {
                // C also: errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                ereport!(
                    ERROR,
                    errmsg!("percentile value {} is not between 0 and 1", p)
                );
            }

            if continuous {
                entry.first_row = 1 + (p * (rowcount - 1) as f64).floor() as int64;
                entry.second_row = 1 + (p * (rowcount - 1) as f64).ceil() as int64;
                entry.proportion =
                    (p * (rowcount - 1) as f64) - (p * (rowcount - 1) as f64).floor();
            } else {
                /*----------
                 * We need the smallest K such that (K/N) >= percentile.
                 * N>0, therefore K >= N*percentile, therefore
                 * K = ceil(N*percentile); but not less than 1.
                 *----------
                 */
                let mut row: int64 = (p * rowcount as f64).ceil() as int64;

                row = core::cmp::max(1, row);
                entry.first_row = row;
                entry.second_row = row;
                entry.proportion = 0.0;
            }
        }
        i += 1;
    }

    /*
     * The parameter array wasn't necessarily in sorted order, but we need to
     * visit the rows in order, so sort by first_row/second_row.
     */
    let slice = core::slice::from_raw_parts_mut(pct_info_arr, num_percentiles as usize);
    slice.sort_by(|a, b| pct_info_cmp(a, b));

    pct_info_arr
}

/*
 * percentile_disc(float8[]) within group (anyelement)	- discrete percentiles
 */
#[unsafe(no_mangle)]
pub unsafe fn percentile_disc_multi_final(fcinfo: FunctionCallInfo) -> Datum {
    let osastate: *mut OSAPerGroupState;
    let param: *mut ArrayType;
    let mut percentiles_datum: *mut Datum = null_mut();
    let mut percentiles_null: *mut bool = null_mut();
    let mut num_percentiles: c_int = 0;
    let pct_info_arr: *mut pct_info;
    let result_datum: *mut Datum;
    let result_isnull: *mut bool;
    let mut rownum: int64 = 0;
    let mut val: Datum = 0;
    let mut isnull: bool = true;
    let mut i: c_int;

    Assert!(AggCheckCallContext(fcinfo, null_mut()) == AGG_CONTEXT_AGGREGATE);

    /* If there were no regular rows, the result is NULL */
    if PG_ARGISNULL!(fcinfo, 0) {
        return PG_RETURN_NULL!();
    }

    osastate = PG_GETARG_POINTER!(fcinfo, 0) as *mut OSAPerGroupState;

    /* number_of_rows could be zero if we only saw NULL input values */
    if (*osastate).number_of_rows == 0 {
        return PG_RETURN_NULL!();
    }

    /* Deconstruct the percentile-array input */
    if PG_ARGISNULL!(fcinfo, 1) {
        return PG_RETURN_NULL!();
    }
    param = PG_GETARG_ARRAYTYPE_P!(fcinfo, 1);

    deconstruct_array_builtin(
        param,
        FLOAT8OID,
        &mut percentiles_datum,
        &mut percentiles_null,
        &mut num_percentiles,
    );

    if num_percentiles == 0 {
        return PG_RETURN_POINTER!(
            construct_empty_array((*(*osastate).qstate).sortColType) as *const c_void
        );
    }

    pct_info_arr = setup_pct_info(
        num_percentiles,
        percentiles_datum,
        percentiles_null,
        (*osastate).number_of_rows,
        false,
    );

    result_datum = palloc(num_percentiles as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    result_isnull = palloc(num_percentiles as usize * core::mem::size_of::<bool>()) as *mut bool;

    /*
     * Start by dealing with any nulls in the param array - those are sorted
     * to the front on row=0, so set the corresponding result indexes to null
     */
    i = 0;
    while i < num_percentiles {
        let idx: c_int = (*pct_info_arr.offset(i as isize)).idx;

        if (*pct_info_arr.offset(i as isize)).first_row > 0 {
            break;
        }

        *result_datum.offset(idx as isize) = 0;
        *result_isnull.offset(idx as isize) = true;
        i += 1;
    }

    /*
     * If there's anything left after doing the nulls, then grind the input
     * and extract the needed values
     */
    if i < num_percentiles {
        /* Finish the sort, or rescan if we already did */
        if !(*osastate).sort_done {
            tuplesort_performsort((*osastate).sortstate);
            (*osastate).sort_done = true;
        } else {
            tuplesort_rescan((*osastate).sortstate);
        }

        while i < num_percentiles {
            let target_row: int64 = (*pct_info_arr.offset(i as isize)).first_row;
            let idx: c_int = (*pct_info_arr.offset(i as isize)).idx;

            /* Advance to target row, if not already there */
            if target_row > rownum {
                if !tuplesort_skiptuples((*osastate).sortstate, target_row - rownum - 1, true) {
                    elog!(ERROR, "missing row in percentile_disc");
                }

                if !tuplesort_getdatum(
                    (*osastate).sortstate,
                    true,
                    true,
                    &mut val,
                    &mut isnull,
                    null_mut(),
                ) {
                    elog!(ERROR, "missing row in percentile_disc");
                }

                rownum = target_row;
            }

            *result_datum.offset(idx as isize) = val;
            *result_isnull.offset(idx as isize) = isnull;
            i += 1;
        }
    }

    /* We make the output array the same shape as the input */
    PG_RETURN_POINTER!(construct_md_array(
        result_datum,
        result_isnull,
        ARR_NDIM(param),
        ARR_DIMS(param),
        ARR_LBOUND(param),
        (*(*osastate).qstate).sortColType,
        (*(*osastate).qstate).typLen as c_int,
        (*(*osastate).qstate).typByVal,
        (*(*osastate).qstate).typAlign,
    ) as *const c_void)
}

/*
 * percentile_cont(float8[]) within group ()	- continuous percentiles
 */
unsafe fn percentile_cont_multi_final_common(
    fcinfo: FunctionCallInfo,
    expect_type: Oid,
    typLen: i16,
    typByVal: bool,
    typAlign: c_char,
    lerpfunc: LerpFunc,
) -> Datum {
    let osastate: *mut OSAPerGroupState;
    let param: *mut ArrayType;
    let mut percentiles_datum: *mut Datum = null_mut();
    let mut percentiles_null: *mut bool = null_mut();
    let mut num_percentiles: c_int = 0;
    let pct_info_arr: *mut pct_info;
    let result_datum: *mut Datum;
    let result_isnull: *mut bool;
    let mut rownum: int64 = 0;
    let mut first_val: Datum = 0;
    let mut second_val: Datum = 0;
    let mut isnull: bool = false;
    let mut i: c_int;

    Assert!(AggCheckCallContext(fcinfo, null_mut()) == AGG_CONTEXT_AGGREGATE);

    /* If there were no regular rows, the result is NULL */
    if PG_ARGISNULL!(fcinfo, 0) {
        return PG_RETURN_NULL!();
    }

    osastate = PG_GETARG_POINTER!(fcinfo, 0) as *mut OSAPerGroupState;

    /* number_of_rows could be zero if we only saw NULL input values */
    if (*osastate).number_of_rows == 0 {
        return PG_RETURN_NULL!();
    }

    Assert!(expect_type == (*(*osastate).qstate).sortColType);

    /* Deconstruct the percentile-array input */
    if PG_ARGISNULL!(fcinfo, 1) {
        return PG_RETURN_NULL!();
    }
    param = PG_GETARG_ARRAYTYPE_P!(fcinfo, 1);

    deconstruct_array_builtin(
        param,
        FLOAT8OID,
        &mut percentiles_datum,
        &mut percentiles_null,
        &mut num_percentiles,
    );

    if num_percentiles == 0 {
        return PG_RETURN_POINTER!(
            construct_empty_array((*(*osastate).qstate).sortColType) as *const c_void
        );
    }

    pct_info_arr = setup_pct_info(
        num_percentiles,
        percentiles_datum,
        percentiles_null,
        (*osastate).number_of_rows,
        true,
    );

    result_datum = palloc(num_percentiles as usize * core::mem::size_of::<Datum>()) as *mut Datum;
    result_isnull = palloc(num_percentiles as usize * core::mem::size_of::<bool>()) as *mut bool;

    /*
     * Start by dealing with any nulls in the param array - those are sorted
     * to the front on row=0, so set the corresponding result indexes to null
     */
    i = 0;
    while i < num_percentiles {
        let idx: c_int = (*pct_info_arr.offset(i as isize)).idx;

        if (*pct_info_arr.offset(i as isize)).first_row > 0 {
            break;
        }

        *result_datum.offset(idx as isize) = 0;
        *result_isnull.offset(idx as isize) = true;
        i += 1;
    }

    /*
     * If there's anything left after doing the nulls, then grind the input
     * and extract the needed values
     */
    if i < num_percentiles {
        /* Finish the sort, or rescan if we already did */
        if !(*osastate).sort_done {
            tuplesort_performsort((*osastate).sortstate);
            (*osastate).sort_done = true;
        } else {
            tuplesort_rescan((*osastate).sortstate);
        }

        while i < num_percentiles {
            let first_row: int64 = (*pct_info_arr.offset(i as isize)).first_row;
            let second_row: int64 = (*pct_info_arr.offset(i as isize)).second_row;
            let idx: c_int = (*pct_info_arr.offset(i as isize)).idx;

            /*
             * Advance to first_row, if not already there.  Note that we might
             * already have rownum beyond first_row, in which case first_val
             * is already correct.  (This occurs when interpolating between
             * the same two input rows as for the previous percentile.)
             */
            if first_row > rownum {
                if !tuplesort_skiptuples((*osastate).sortstate, first_row - rownum - 1, true) {
                    elog!(ERROR, "missing row in percentile_cont");
                }

                if !tuplesort_getdatum(
                    (*osastate).sortstate,
                    true,
                    true,
                    &mut first_val,
                    &mut isnull,
                    null_mut(),
                ) || isnull
                {
                    elog!(ERROR, "missing row in percentile_cont");
                }

                rownum = first_row;
                /* Always advance second_val to be latest input value */
                second_val = first_val;
            } else if first_row == rownum {
                /*
                 * We are already at the desired row, so we must previously
                 * have read its value into second_val (and perhaps first_val
                 * as well, but this assignment is harmless in that case).
                 */
                first_val = second_val;
            }

            /* Fetch second_row if needed */
            if second_row > rownum {
                if !tuplesort_getdatum(
                    (*osastate).sortstate,
                    true,
                    true,
                    &mut second_val,
                    &mut isnull,
                    null_mut(),
                ) || isnull
                {
                    elog!(ERROR, "missing row in percentile_cont");
                }
                rownum += 1;
            }
            /* We should now certainly be on second_row exactly */
            Assert!(second_row == rownum);

            /* Compute appropriate result */
            if second_row > first_row {
                *result_datum.offset(idx as isize) =
                    lerpfunc(first_val, second_val, (*pct_info_arr.offset(i as isize)).proportion);
            } else {
                *result_datum.offset(idx as isize) = first_val;
            }

            *result_isnull.offset(idx as isize) = false;
            i += 1;
        }
    }

    /* We make the output array the same shape as the input */
    PG_RETURN_POINTER!(construct_md_array(
        result_datum,
        result_isnull,
        ARR_NDIM(param),
        ARR_DIMS(param),
        ARR_LBOUND(param),
        expect_type,
        typLen as c_int,
        typByVal,
        typAlign,
    ) as *const c_void)
}

/*
 * percentile_cont(float8[]) within group (float8)	- continuous percentiles
 */
#[unsafe(no_mangle)]
pub unsafe fn percentile_cont_float8_multi_final(fcinfo: FunctionCallInfo) -> Datum {
    percentile_cont_multi_final_common(
        fcinfo,
        FLOAT8OID,
        /* hard-wired info on type float8 */
        core::mem::size_of::<f64>() as i16,
        FLOAT8PASSBYVAL,
        TYPALIGN_DOUBLE,
        float8_lerp,
    )
}

/*
 * percentile_cont(float8[]) within group (interval)  - continuous percentiles
 */
#[unsafe(no_mangle)]
pub unsafe fn percentile_cont_interval_multi_final(fcinfo: FunctionCallInfo) -> Datum {
    percentile_cont_multi_final_common(
        fcinfo,
        INTERVALOID,
        /* hard-wired info on type interval */
        16,
        false,
        TYPALIGN_DOUBLE,
        interval_lerp,
    )
}

/*
 * mode() within group (anyelement) - most common value
 */
#[unsafe(no_mangle)]
pub unsafe fn mode_final(fcinfo: FunctionCallInfo) -> Datum {
    let osastate: *mut OSAPerGroupState;
    let mut val: Datum = 0;
    let mut isnull: bool = false;
    let mut mode_val: Datum = 0;
    let mut mode_freq: int64 = 0;
    let mut last_val: Datum = 0;
    let mut last_val_freq: int64 = 0;
    let mut last_val_is_mode: bool = false;
    let equalfn: *mut FmgrInfo;
    let mut abbrev_val: Datum = 0;
    let mut last_abbrev_val: Datum = 0;
    let shouldfree: bool;

    Assert!(AggCheckCallContext(fcinfo, null_mut()) == AGG_CONTEXT_AGGREGATE);

    /* If there were no regular rows, the result is NULL */
    if PG_ARGISNULL!(fcinfo, 0) {
        return PG_RETURN_NULL!();
    }

    osastate = PG_GETARG_POINTER!(fcinfo, 0) as *mut OSAPerGroupState;

    /* number_of_rows could be zero if we only saw NULL input values */
    if (*osastate).number_of_rows == 0 {
        return PG_RETURN_NULL!();
    }

    /* Look up the equality function for the datatype, if we didn't already */
    equalfn = &mut (*(*osastate).qstate).equalfn;
    if !OidIsValid((*equalfn).fn_oid) {
        fmgr_info_cxt(
            get_opcode((*(*osastate).qstate).eqOperator),
            equalfn,
            (*(*osastate).qstate).qcontext,
        );
    }

    shouldfree = !((*(*osastate).qstate).typByVal);

    /* Finish the sort, or rescan if we already did */
    if !(*osastate).sort_done {
        tuplesort_performsort((*osastate).sortstate);
        (*osastate).sort_done = true;
    } else {
        tuplesort_rescan((*osastate).sortstate);
    }

    /* Scan tuples and count frequencies */
    while tuplesort_getdatum(
        (*osastate).sortstate,
        true,
        true,
        &mut val,
        &mut isnull,
        &mut abbrev_val,
    ) {
        /* we don't expect any nulls, but ignore them if found */
        if isnull {
            continue;
        }

        if last_val_freq == 0 {
            /* first nonnull value - it's the mode for now */
            mode_val = val;
            last_val = val;
            mode_freq = 1;
            last_val_freq = 1;
            last_val_is_mode = true;
            last_abbrev_val = abbrev_val;
        } else if abbrev_val == last_abbrev_val
            && DatumGetBool(FunctionCall2Coll(
                equalfn,
                PG_GET_COLLATION!(fcinfo),
                val,
                last_val,
            ))
        {
            /* value equal to previous value, count it */
            if last_val_is_mode {
                mode_freq += 1; /* needn't maintain last_val_freq */
            } else {
                last_val_freq += 1;
                if last_val_freq > mode_freq {
                    /* last_val becomes new mode */
                    if shouldfree {
                        pfree(DatumGetPointer(mode_val) as *mut c_void);
                    }
                    mode_val = last_val;
                    mode_freq = last_val_freq;
                    last_val_is_mode = true;
                }
            }
            if shouldfree {
                pfree(DatumGetPointer(val) as *mut c_void);
            }
        } else {
            /* val should replace last_val */
            if shouldfree && !last_val_is_mode {
                pfree(DatumGetPointer(last_val) as *mut c_void);
            }
            last_val = val;
            /* avoid equality function calls by reusing abbreviated keys */
            last_abbrev_val = abbrev_val;
            last_val_freq = 1;
            last_val_is_mode = false;
        }

        CHECK_FOR_INTERRUPTS();
    }

    if shouldfree && !last_val_is_mode {
        pfree(DatumGetPointer(last_val) as *mut c_void);
    }

    if mode_freq != 0 {
        PG_RETURN_DATUM!(mode_val)
    } else {
        PG_RETURN_NULL!()
    }
}

/*
 * Common code to sanity-check args for hypothetical-set functions. No need
 * for friendly errors, these can only happen if someone's messing up the
 * aggregate definitions. The checks are needed for security, however.
 */
unsafe fn hypothetical_check_argtypes(
    fcinfo: FunctionCallInfo,
    nargs: c_int,
    tupdesc: TupleDesc,
) {
    let mut i: c_int;

    /* check that we have an int4 flag column */
    if tupdesc.is_null()
        || (nargs + 1) != (*tupdesc).natts
        || (*TupleDescAttr(tupdesc, nargs)).atttypid != INT4OID
    {
        elog!(ERROR, "type mismatch in hypothetical-set function");
    }

    /* check that direct args match in type with aggregated args */
    i = 0;
    while i < nargs {
        let attr = TupleDescAttr(tupdesc, i);

        if get_fn_expr_argtype((*fcinfo).flinfo, i + 1) != (*attr).atttypid {
            elog!(ERROR, "type mismatch in hypothetical-set function");
        }
        i += 1;
    }
}

/*
 * compute rank of hypothetical row
 *
 * flag should be -1 to sort hypothetical row ahead of its peers, or +1
 * to sort behind.
 * total number of regular rows is returned into *number_of_rows.
 */
unsafe fn hypothetical_rank_common(
    fcinfo: FunctionCallInfo,
    flag: c_int,
    number_of_rows: *mut int64,
) -> int64 {
    let mut nargs: c_int = PG_NARGS!(fcinfo) - 1;
    let mut rank: int64 = 1;
    let osastate: *mut OSAPerGroupState;
    let slot: *mut TupleTableSlot;
    let mut i: c_int;

    Assert!(AggCheckCallContext(fcinfo, null_mut()) == AGG_CONTEXT_AGGREGATE);

    /* If there were no regular rows, the rank is always 1 */
    if PG_ARGISNULL!(fcinfo, 0) {
        *number_of_rows = 0;
        return 1;
    }

    osastate = PG_GETARG_POINTER!(fcinfo, 0) as *mut OSAPerGroupState;
    *number_of_rows = (*osastate).number_of_rows;

    /* Adjust nargs to be the number of direct (or aggregated) args */
    if nargs % 2 != 0 {
        elog!(ERROR, "wrong number of arguments in hypothetical-set function");
    }
    nargs /= 2;

    hypothetical_check_argtypes(fcinfo, nargs, (*(*osastate).qstate).tupdesc);

    /* because we need a hypothetical row, we can't share transition state */
    Assert!(!(*osastate).sort_done);

    /* insert the hypothetical row into the sort */
    slot = (*(*osastate).qstate).tupslot;
    ExecClearTuple(slot);
    i = 0;
    while i < nargs {
        *(*slot).tts_values.offset(i as isize) = PG_GETARG_DATUM!(fcinfo, i + 1);
        *(*slot).tts_isnull.offset(i as isize) = PG_ARGISNULL!(fcinfo, i + 1);
        i += 1;
    }
    *(*slot).tts_values.offset(i as isize) = Int32GetDatum(flag);
    *(*slot).tts_isnull.offset(i as isize) = false;
    ExecStoreVirtualTuple(slot);

    tuplesort_puttupleslot((*osastate).sortstate, slot);

    /* finish the sort */
    tuplesort_performsort((*osastate).sortstate);
    (*osastate).sort_done = true;

    /* iterate till we find the hypothetical row */
    while tuplesort_gettupleslot((*osastate).sortstate, true, true, slot, null_mut()) {
        let mut isnull: bool = false;
        let d: Datum = slot_getattr(slot, nargs + 1, &mut isnull);

        if !isnull && DatumGetInt32(d) != 0 {
            break;
        }

        rank += 1;

        CHECK_FOR_INTERRUPTS();
    }

    ExecClearTuple(slot);

    rank
}

/*
 * rank()  - rank of hypothetical row
 */
#[unsafe(no_mangle)]
pub unsafe fn hypothetical_rank_final(fcinfo: FunctionCallInfo) -> Datum {
    let rank: int64;
    let mut rowcount: int64 = 0;

    rank = hypothetical_rank_common(fcinfo, -1, &mut rowcount);

    PG_RETURN_INT64!(rank)
}

/*
 * percent_rank()	- percentile rank of hypothetical row
 */
#[unsafe(no_mangle)]
pub unsafe fn hypothetical_percent_rank_final(fcinfo: FunctionCallInfo) -> Datum {
    let rank: int64;
    let mut rowcount: int64 = 0;
    let result_val: f64;

    rank = hypothetical_rank_common(fcinfo, -1, &mut rowcount);

    if rowcount == 0 {
        return PG_RETURN_FLOAT8!(0.0);
    }

    result_val = (rank - 1) as f64 / (rowcount) as f64;

    PG_RETURN_FLOAT8!(result_val)
}

/*
 * cume_dist()	- cumulative distribution of hypothetical row
 */
#[unsafe(no_mangle)]
pub unsafe fn hypothetical_cume_dist_final(fcinfo: FunctionCallInfo) -> Datum {
    let rank: int64;
    let mut rowcount: int64 = 0;
    let result_val: f64;

    rank = hypothetical_rank_common(fcinfo, 1, &mut rowcount);

    result_val = (rank) as f64 / (rowcount + 1) as f64;

    PG_RETURN_FLOAT8!(result_val)
}

/*
 * dense_rank() - rank of hypothetical row without gaps in ranking
 */
#[unsafe(no_mangle)]
pub unsafe fn hypothetical_dense_rank_final(fcinfo: FunctionCallInfo) -> Datum {
    let mut econtext: *mut ExprContext;
    let mut compareTuple: *mut ExprState;
    let mut nargs: c_int = PG_NARGS!(fcinfo) - 1;
    let mut rank: int64 = 1;
    let mut duplicate_count: int64 = 0;
    let osastate: *mut OSAPerGroupState;
    let numDistinctCols: c_int;
    let mut abbrevVal: Datum = 0;
    let mut abbrevOld: Datum = 0;
    let mut slot: *mut TupleTableSlot;
    let extraslot: *mut TupleTableSlot;
    let mut slot2: *mut TupleTableSlot;
    let mut i: c_int;

    Assert!(AggCheckCallContext(fcinfo, null_mut()) == AGG_CONTEXT_AGGREGATE);

    /* If there were no regular rows, the rank is always 1 */
    if PG_ARGISNULL!(fcinfo, 0) {
        return PG_RETURN_INT64!(rank);
    }

    osastate = PG_GETARG_POINTER!(fcinfo, 0) as *mut OSAPerGroupState;
    econtext = (*(*osastate).qstate).econtext;
    if econtext.is_null() {
        let oldcontext: MemoryContext;

        /* Make sure to we create econtext under correct parent context. */
        oldcontext = MemoryContextSwitchTo((*(*osastate).qstate).qcontext);
        (*(*osastate).qstate).econtext = CreateStandaloneExprContext();
        econtext = (*(*osastate).qstate).econtext;
        MemoryContextSwitchTo(oldcontext);
    }

    /* Adjust nargs to be the number of direct (or aggregated) args */
    if nargs % 2 != 0 {
        elog!(ERROR, "wrong number of arguments in hypothetical-set function");
    }
    nargs /= 2;

    hypothetical_check_argtypes(fcinfo, nargs, (*(*osastate).qstate).tupdesc);

    /*
     * When comparing tuples, we can omit the flag column since we will only
     * compare rows with flag == 0.
     */
    numDistinctCols = (*(*osastate).qstate).numSortCols - 1;

    /* Build tuple comparator, if we didn't already */
    compareTuple = (*(*osastate).qstate).compareTuple;
    if compareTuple.is_null() {
        let sortColIdx: *mut AttrNumber = (*(*osastate).qstate).sortColIdx;
        let oldContext: MemoryContext;

        oldContext = MemoryContextSwitchTo((*(*osastate).qstate).qcontext);
        compareTuple = execTuplesMatchPrepare(
            (*(*osastate).qstate).tupdesc,
            numDistinctCols,
            sortColIdx,
            (*(*osastate).qstate).eqOperators,
            (*(*osastate).qstate).sortCollations,
            null_mut(),
        );
        MemoryContextSwitchTo(oldContext);
        (*(*osastate).qstate).compareTuple = compareTuple;
    }

    /* because we need a hypothetical row, we can't share transition state */
    Assert!(!(*osastate).sort_done);

    /* insert the hypothetical row into the sort */
    slot = (*(*osastate).qstate).tupslot;
    ExecClearTuple(slot);
    i = 0;
    while i < nargs {
        *(*slot).tts_values.offset(i as isize) = PG_GETARG_DATUM!(fcinfo, i + 1);
        *(*slot).tts_isnull.offset(i as isize) = PG_ARGISNULL!(fcinfo, i + 1);
        i += 1;
    }
    *(*slot).tts_values.offset(i as isize) = Int32GetDatum(-1);
    *(*slot).tts_isnull.offset(i as isize) = false;
    ExecStoreVirtualTuple(slot);

    tuplesort_puttupleslot((*osastate).sortstate, slot);

    /* finish the sort */
    tuplesort_performsort((*osastate).sortstate);
    (*osastate).sort_done = true;

    /*
     * We alternate fetching into tupslot and extraslot so that we have the
     * previous row available for comparisons.  This is accomplished by
     * swapping the slot pointer variables after each row.
     */
    extraslot = MakeSingleTupleTableSlot((*(*osastate).qstate).tupdesc, &TTSOpsMinimalTuple);
    slot2 = extraslot;

    /* iterate till we find the hypothetical row */
    while tuplesort_gettupleslot((*osastate).sortstate, true, true, slot, &mut abbrevVal) {
        let mut isnull: bool = false;
        let d: Datum = slot_getattr(slot, nargs + 1, &mut isnull);
        let tmpslot: *mut TupleTableSlot;

        if !isnull && DatumGetInt32(d) != 0 {
            break;
        }

        /* count non-distinct tuples */
        (*econtext).ecxt_outertuple = slot;
        (*econtext).ecxt_innertuple = slot2;

        if !TupIsNull(slot2)
            && abbrevVal == abbrevOld
            && ExecQualAndReset(compareTuple, econtext)
        {
            duplicate_count += 1;
        }

        tmpslot = slot2;
        slot2 = slot;
        slot = tmpslot;
        /* avoid ExecQual() calls by reusing abbreviated keys */
        abbrevOld = abbrevVal;

        rank += 1;

        CHECK_FOR_INTERRUPTS();
    }

    ExecClearTuple(slot);
    ExecClearTuple(slot2);

    ExecDropSingleTupleTableSlot(extraslot);

    rank = rank - duplicate_count;

    PG_RETURN_INT64!(rank)
}
