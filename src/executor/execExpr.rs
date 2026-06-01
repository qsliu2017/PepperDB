/*-------------------------------------------------------------------------
 *
 * execExpr.rs
 *   Expression evaluation infrastructure.
 *
 * During executor startup, we compile each expression tree (which has
 * previously been processed by the parser and planner) into an ExprState,
 * using ExecInitExpr() et al.  This converts the tree into a flat array
 * of ExprEvalSteps, which may be thought of as instructions in a program.
 * At runtime, we'll execute steps, starting with the first, until we reach
 * an EEOP_DONE_{RETURN|NO_RETURN} opcode.
 *
 * This file contains the "compilation" logic.  It is independent of the
 * specific execution technology we use (switch statement, computed goto,
 * JIT compilation, etc).
 *
 * See src/backend/executor/README for some background, specifically the
 * "Expression Trees and ExprState nodes", "Expression Initialization",
 * and "Expression Evaluation" sections.
 *
 * Also merged from execExpr.h: ExprEvalOp enum, ExprEvalStep struct (with
 * its union), ExprEvalRowtypeCache, SubscriptingRefState, SubscriptExecSteps,
 * JsonConstructorExprState, and the ExecEvalSubroutine/ExecEvalBoolSubroutine
 * function-pointer typedefs.  ExprState itself lives in nodes::execnodes
 * (the real home); we import it from there.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *   src/backend/executor/execExpr.c -> src/executor/execExpr.rs
 *
 *-------------------------------------------------------------------------
 */

#![allow(unreachable_patterns)] // exhaustive C switches over partial Rust enums
use crate::prelude::*;
use crate::{IsA, makeNode};

/* ---- imports from real homes ---- */
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::access::cmptype::CompareType;
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr, TupleDescCompactAttr};
use crate::access::common::tupconvert::TupleConversionMap;
use crate::nodes::execnodes::JunkFilter;
use crate::executor::execTuples::TTSOpsVirtual;
use crate::executor::executor::ExecEvalExprSwitchContext;
use crate::executor::tuptable::{TupleTableSlot, TupleTableSlotOps};
use crate::nodes::execnodes::{
    AggState, AggStatePerPhase, AggStatePerTrans, CteScanState, DomainConstraintState,
    DomainConstraintType::{DOM_CONSTRAINT_CHECK, DOM_CONSTRAINT_NOTNULL}, EEO_FLAG_HAS_NEW,
    EEO_FLAG_HAS_OLD, EEO_FLAG_IS_QUAL, ExprContext, ExprState, JsonExprState, PlanState,
    ProjectionInfo, SubPlanState, SubqueryScanState, WindowAggState, WindowFuncExprState,
};
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::nodes::nodes::{AggStrategy::AGG_HASHED, Node, NodeTag};
use crate::nodes::nodes::{DO_AGGSPLIT_COMBINE, DO_AGGSPLIT_DESERIALIZE, DO_AGGSPLIT_SERIALIZE};
use crate::nodes::pg_list::{lappend, lappend_int, linitial, List, NIL};
use crate::nodes::plannodes::Agg;
use crate::nodes::primnodes::{
    Aggref, BoolExpr, CaseExpr, CaseWhen, CoalesceExpr, CoerceToDomain, CoerceViaIO,
    FieldSelect, FieldStore, FuncExpr, GroupingFunc, JsonBehaviorType::JSON_BEHAVIOR_ERROR,
    JsonConstructorExpr, JsonExpr, JsonExprOp::JSON_EXISTS_OP, JsonIsPredicate, JsonReturning,
    JsonValueExpr, MinMaxExpr, MinMaxOp, NextValueExpr, NullIfExpr, NullTest, OpExpr, Param,
    RowCompareExpr, RowExpr, SQLValueFunction, ScalarArrayOpExpr, SubscriptingRef, SubPlan,
    SubLinkType::MULTIEXPR_SUBLINK, TargetEntry, VarReturningType, Var, WindowFunc, XmlExpr,
};
use crate::postgres::NullableDatum;
use crate::utils::adt::arrayfuncs::ArrayMapState;
use crate::utils::cache::typcache::TYPECACHE_CMP_PROC;
use crate::utils::cache::lsyscache::RECORDOID;
use crate::utils::fmgr::{FmgrInfo, FunctionCallInfo, PGFunction};

/* ---- execExpr.h public flag bits (private to expression evaluation) ---- */
/* expression's interpreter has been initialized */
pub const EEO_FLAG_INTERPRETER_INITIALIZED: u8 = 1 << 5;
/* jump-threading is in use */
pub const EEO_FLAG_DIRECT_THREADED: u8 = 1 << 6;

/* Typical API for out-of-line evaluation subroutines */
pub type ExecEvalSubroutine = Option<
    unsafe fn(state: *mut ExprState, op: *mut ExprEvalStep, econtext: *mut ExprContext),
>;

/* API for out-of-line evaluation subroutines returning bool */
pub type ExecEvalBoolSubroutine = Option<
    unsafe fn(state: *mut ExprState, op: *mut ExprEvalStep, econtext: *mut ExprContext) -> bool,
>;

/*
 * ExprEvalSteps that cache a composite type's tupdesc need one of these.
 * (It fits in-line in some step types, otherwise allocate out-of-line.)
 */
#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalRowtypeCache {
    /*
     * cacheptr points to composite type's TypeCacheEntry if tupdesc_id is not
     * 0; or for an anonymous RECORD type, it points directly at the cached
     * tupdesc for the type, and tupdesc_id is 0.  Initial state cacheptr == NULL.
     */
    pub cacheptr: *mut c_void,
    pub tupdesc_id: uint64, /* last-seen tupdesc identifier, or 0 */
}

/*
 * Discriminator for ExprEvalSteps.
 *
 * Identifies the operation to be executed and which member in the
 * ExprEvalStep->d union is valid.
 *
 * The order of entries needs to be kept in sync with the dispatch_table[]
 * array in execExprInterp.c:ExecInterpExpr().
 */
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum ExprEvalOp {
    /* entire expression has been evaluated, return value */
    EEOP_DONE_RETURN,
    /* entire expression has been evaluated, no return value */
    EEOP_DONE_NO_RETURN,
    /* apply slot_getsomeattrs on corresponding tuple slot */
    EEOP_INNER_FETCHSOME,
    EEOP_OUTER_FETCHSOME,
    EEOP_SCAN_FETCHSOME,
    EEOP_OLD_FETCHSOME,
    EEOP_NEW_FETCHSOME,
    /* compute non-system Var value */
    EEOP_INNER_VAR,
    EEOP_OUTER_VAR,
    EEOP_SCAN_VAR,
    EEOP_OLD_VAR,
    EEOP_NEW_VAR,
    /* compute system Var value */
    EEOP_INNER_SYSVAR,
    EEOP_OUTER_SYSVAR,
    EEOP_SCAN_SYSVAR,
    EEOP_OLD_SYSVAR,
    EEOP_NEW_SYSVAR,
    /* compute wholerow Var */
    EEOP_WHOLEROW,
    /*
     * Compute non-system Var value, assign into ExprState's resultslot.
     * Not used if a CheckVarSlotCompatibility() check would be needed.
     */
    EEOP_ASSIGN_INNER_VAR,
    EEOP_ASSIGN_OUTER_VAR,
    EEOP_ASSIGN_SCAN_VAR,
    EEOP_ASSIGN_OLD_VAR,
    EEOP_ASSIGN_NEW_VAR,
    /* assign ExprState's resvalue/resnull to a column of its resultslot */
    EEOP_ASSIGN_TMP,
    /* ditto, applying MakeExpandedObjectReadOnly() */
    EEOP_ASSIGN_TMP_MAKE_RO,
    /* evaluate Const value */
    EEOP_CONST,
    /*
     * Evaluate function call (including OpExprs etc).  For speed, we
     * distinguish in the opcode whether the function is strict with 1, 2, or
     * more arguments and/or requires usage stats tracking.
     */
    EEOP_FUNCEXPR,
    EEOP_FUNCEXPR_STRICT,
    EEOP_FUNCEXPR_STRICT_1,
    EEOP_FUNCEXPR_STRICT_2,
    EEOP_FUNCEXPR_FUSAGE,
    EEOP_FUNCEXPR_STRICT_FUSAGE,
    /*
     * Evaluate boolean AND expression, one step per subexpression. FIRST/LAST
     * subexpressions are special-cased for performance.
     */
    EEOP_BOOL_AND_STEP_FIRST,
    EEOP_BOOL_AND_STEP,
    EEOP_BOOL_AND_STEP_LAST,
    /* similarly for boolean OR expression */
    EEOP_BOOL_OR_STEP_FIRST,
    EEOP_BOOL_OR_STEP,
    EEOP_BOOL_OR_STEP_LAST,
    /* evaluate boolean NOT expression */
    EEOP_BOOL_NOT_STEP,
    /* simplified version of BOOL_AND_STEP for use by ExecQual() */
    EEOP_QUAL,
    /* unconditional jump to another step */
    EEOP_JUMP,
    /* conditional jumps based on current result value */
    EEOP_JUMP_IF_NULL,
    EEOP_JUMP_IF_NOT_NULL,
    EEOP_JUMP_IF_NOT_TRUE,
    /* perform NULL tests for scalar values */
    EEOP_NULLTEST_ISNULL,
    EEOP_NULLTEST_ISNOTNULL,
    /* perform NULL tests for row values */
    EEOP_NULLTEST_ROWISNULL,
    EEOP_NULLTEST_ROWISNOTNULL,
    /* evaluate a BooleanTest expression */
    EEOP_BOOLTEST_IS_TRUE,
    EEOP_BOOLTEST_IS_NOT_TRUE,
    EEOP_BOOLTEST_IS_FALSE,
    EEOP_BOOLTEST_IS_NOT_FALSE,
    /* evaluate PARAM_EXEC/EXTERN parameters */
    EEOP_PARAM_EXEC,
    EEOP_PARAM_EXTERN,
    EEOP_PARAM_CALLBACK,
    /* set PARAM_EXEC value */
    EEOP_PARAM_SET,
    /* return CaseTestExpr value */
    EEOP_CASE_TESTVAL,
    EEOP_CASE_TESTVAL_EXT,
    /* apply MakeExpandedObjectReadOnly() to target value */
    EEOP_MAKE_READONLY,
    /* evaluate assorted special-purpose expression types */
    EEOP_IOCOERCE,
    EEOP_IOCOERCE_SAFE,
    EEOP_DISTINCT,
    EEOP_NOT_DISTINCT,
    EEOP_NULLIF,
    EEOP_SQLVALUEFUNCTION,
    EEOP_CURRENTOFEXPR,
    EEOP_NEXTVALUEEXPR,
    EEOP_RETURNINGEXPR,
    EEOP_ARRAYEXPR,
    EEOP_ARRAYCOERCE,
    EEOP_ROW,
    /*
     * Compare two individual elements of each of two compared ROW()
     * expressions.  Skip to ROWCOMPARE_FINAL if elements are not equal.
     */
    EEOP_ROWCOMPARE_STEP,
    /* evaluate boolean value based on previous ROWCOMPARE_STEP operations */
    EEOP_ROWCOMPARE_FINAL,
    /* evaluate GREATEST() or LEAST() */
    EEOP_MINMAX,
    /* evaluate FieldSelect expression */
    EEOP_FIELDSELECT,
    /*
     * Deform tuple before evaluating new values for individual fields in a
     * FieldStore expression.
     */
    EEOP_FIELDSTORE_DEFORM,
    /*
     * Form the new tuple for a FieldStore expression.  Individual fields will
     * have been evaluated into columns of the tuple deformed by the preceding
     * DEFORM step.
     */
    EEOP_FIELDSTORE_FORM,
    /* Process container subscripts; possibly short-circuit result to NULL */
    EEOP_SBSREF_SUBSCRIPTS,
    /*
     * Compute old container element/slice when a SubscriptingRef assignment
     * expression contains SubscriptingRef/FieldStore subexpressions.
     */
    EEOP_SBSREF_OLD,
    /* compute new value for SubscriptingRef assignment expression */
    EEOP_SBSREF_ASSIGN,
    /* compute element/slice for SubscriptingRef fetch expression */
    EEOP_SBSREF_FETCH,
    /* evaluate value for CoerceToDomainValue */
    EEOP_DOMAIN_TESTVAL,
    EEOP_DOMAIN_TESTVAL_EXT,
    /* evaluate a domain's NOT NULL constraint */
    EEOP_DOMAIN_NOTNULL,
    /* evaluate a single domain CHECK constraint */
    EEOP_DOMAIN_CHECK,
    /* evaluation steps for hashing */
    EEOP_HASHDATUM_SET_INITVAL,
    EEOP_HASHDATUM_FIRST,
    EEOP_HASHDATUM_FIRST_STRICT,
    EEOP_HASHDATUM_NEXT32,
    EEOP_HASHDATUM_NEXT32_STRICT,
    /* evaluate assorted special-purpose expression types */
    EEOP_CONVERT_ROWTYPE,
    EEOP_SCALARARRAYOP,
    EEOP_HASHED_SCALARARRAYOP,
    EEOP_XMLEXPR,
    EEOP_JSON_CONSTRUCTOR,
    EEOP_IS_JSON,
    EEOP_JSONEXPR_PATH,
    EEOP_JSONEXPR_COERCION,
    EEOP_JSONEXPR_COERCION_FINISH,
    EEOP_AGGREF,
    EEOP_GROUPING_FUNC,
    EEOP_WINDOW_FUNC,
    EEOP_MERGE_SUPPORT_FUNC,
    EEOP_SUBPLAN,
    /* aggregation related nodes */
    EEOP_AGG_STRICT_DESERIALIZE,
    EEOP_AGG_DESERIALIZE,
    EEOP_AGG_STRICT_INPUT_CHECK_ARGS,
    EEOP_AGG_STRICT_INPUT_CHECK_ARGS_1,
    EEOP_AGG_STRICT_INPUT_CHECK_NULLS,
    EEOP_AGG_PLAIN_PERGROUP_NULLCHECK,
    EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL,
    EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL,
    EEOP_AGG_PLAIN_TRANS_BYVAL,
    EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF,
    EEOP_AGG_PLAIN_TRANS_STRICT_BYREF,
    EEOP_AGG_PLAIN_TRANS_BYREF,
    EEOP_AGG_PRESORTED_DISTINCT_SINGLE,
    EEOP_AGG_PRESORTED_DISTINCT_MULTI,
    EEOP_AGG_ORDERED_TRANS_DATUM,
    EEOP_AGG_ORDERED_TRANS_TUPLE,
    /* non-existent operation, used e.g. to check array lengths */
    EEOP_LAST,
}

use ExprEvalOp::*;

/*
 * ExprEvalStep -- a single step in an expression evaluation sequence.
 *
 * opcode is normally an ExprEvalOp (cast to isize), but the interpreter may
 * overwrite it with a computed-goto address.
 *
 * The union 'd' holds inline data for the operation.  On 64-bit systems the
 * union must remain <= 40 bytes so the whole struct fits in 64 bytes (one
 * cache line).
 */
#[repr(C)]
pub struct ExprEvalStep {
    pub opcode: isize,
    /* where to store the result of this step */
    pub resvalue: *mut Datum,
    pub resnull: *mut bool,
    pub d: ExprEvalStep_d,
}

/* Union variants follow.  Each is a separate #[repr(C)] struct so that
 * field access is straightforward.  The union itself mirrors the C union. */

#[repr(C)]
pub union ExprEvalStep_d {
    /* for EEOP_INNER/OUTER/SCAN/OLD/NEW_FETCHSOME */
    pub fetch: ExprEvalStep_fetch,
    /* for EEOP_INNER/OUTER/SCAN/OLD/NEW_[SYS]VAR */
    pub var: ExprEvalStep_var,
    /* for EEOP_WHOLEROW */
    pub wholerow: ExprEvalStep_wholerow,
    /* for EEOP_ASSIGN_*_VAR */
    pub assign_var: ExprEvalStep_assign_var,
    /* for EEOP_ASSIGN_TMP[_MAKE_RO] */
    pub assign_tmp: ExprEvalStep_assign_tmp,
    /* for EEOP_RETURNINGEXPR */
    pub returningexpr: ExprEvalStep_returningexpr,
    /* for EEOP_CONST */
    pub constval: ExprEvalStep_constval,
    /* for EEOP_FUNCEXPR_* / NULLIF / DISTINCT */
    pub func: ExprEvalStep_func,
    /* for EEOP_BOOL_*_STEP */
    pub boolexpr: ExprEvalStep_boolexpr,
    /* for EEOP_QUAL */
    pub qualexpr: ExprEvalStep_qualexpr,
    /* for EEOP_JUMP[_CONDITION] */
    pub jump: ExprEvalStep_jump,
    /* for EEOP_NULLTEST_ROWIS[NOT]NULL */
    pub nulltest_row: ExprEvalStep_nulltest_row,
    /* for EEOP_PARAM_EXEC/EXTERN and EEOP_PARAM_SET */
    pub param: ExprEvalStep_param,
    /* for EEOP_PARAM_CALLBACK */
    pub cparam: ExprEvalStep_cparam,
    /* for EEOP_CASE_TESTVAL/DOMAIN_TESTVAL */
    pub casetest: ExprEvalStep_casetest,
    /* for EEOP_MAKE_READONLY */
    pub make_readonly: ExprEvalStep_make_readonly,
    /* for EEOP_IOCOERCE */
    pub iocoerce: ExprEvalStep_iocoerce,
    /* for EEOP_SQLVALUEFUNCTION */
    pub sqlvaluefunction: ExprEvalStep_sqlvaluefunction,
    /* for EEOP_NEXTVALUEEXPR */
    pub nextvalueexpr: ExprEvalStep_nextvalueexpr,
    /* for EEOP_ARRAYEXPR */
    pub arrayexpr: ExprEvalStep_arrayexpr,
    /* for EEOP_ARRAYCOERCE */
    pub arraycoerce: ExprEvalStep_arraycoerce,
    /* for EEOP_ROW */
    pub row: ExprEvalStep_row,
    /* for EEOP_ROWCOMPARE_STEP */
    pub rowcompare_step: ExprEvalStep_rowcompare_step,
    /* for EEOP_ROWCOMPARE_FINAL */
    pub rowcompare_final: ExprEvalStep_rowcompare_final,
    /* for EEOP_MINMAX */
    pub minmax: ExprEvalStep_minmax,
    /* for EEOP_FIELDSELECT */
    pub fieldselect: ExprEvalStep_fieldselect,
    /* for EEOP_FIELDSTORE_DEFORM / FIELDSTORE_FORM */
    pub fieldstore: ExprEvalStep_fieldstore,
    /* for EEOP_SBSREF_SUBSCRIPTS */
    pub sbsref_subscript: ExprEvalStep_sbsref_subscript,
    /* for EEOP_SBSREF_OLD / ASSIGN / FETCH */
    pub sbsref: ExprEvalStep_sbsref,
    /* for EEOP_DOMAIN_NOTNULL / DOMAIN_CHECK */
    pub domaincheck: ExprEvalStep_domaincheck,
    /* for EEOP_HASH_SET_INITVAL */
    pub hashdatum_initvalue: ExprEvalStep_hashdatum_initvalue,
    /* for EEOP_HASHDATUM_(FIRST|NEXT32)[_STRICT] */
    pub hashdatum: ExprEvalStep_hashdatum,
    /* for EEOP_CONVERT_ROWTYPE */
    pub convert_rowtype: ExprEvalStep_convert_rowtype,
    /* for EEOP_SCALARARRAYOP */
    pub scalararrayop: ExprEvalStep_scalararrayop,
    /* for EEOP_HASHED_SCALARARRAYOP */
    pub hashedscalararrayop: ExprEvalStep_hashedscalararrayop,
    /* for EEOP_XMLEXPR */
    pub xmlexpr: ExprEvalStep_xmlexpr,
    /* for EEOP_JSON_CONSTRUCTOR */
    pub json_constructor: ExprEvalStep_json_constructor,
    /* for EEOP_AGGREF */
    pub aggref: ExprEvalStep_aggref,
    /* for EEOP_GROUPING_FUNC */
    pub grouping_func: ExprEvalStep_grouping_func,
    /* for EEOP_WINDOW_FUNC */
    pub window_func: ExprEvalStep_window_func,
    /* for EEOP_SUBPLAN */
    pub subplan: ExprEvalStep_subplan,
    /* for EEOP_AGG_*DESERIALIZE */
    pub agg_deserialize: ExprEvalStep_agg_deserialize,
    /* for EEOP_AGG_STRICT_INPUT_CHECK_NULLS / STRICT_INPUT_CHECK_ARGS */
    pub agg_strict_input_check: ExprEvalStep_agg_strict_input_check,
    /* for EEOP_AGG_PLAIN_PERGROUP_NULLCHECK */
    pub agg_plain_pergroup_nullcheck: ExprEvalStep_agg_plain_pergroup_nullcheck,
    /* for EEOP_AGG_PRESORTED_DISTINCT_{SINGLE,MULTI} */
    pub agg_presorted_distinctcheck: ExprEvalStep_agg_presorted_distinctcheck,
    /* for EEOP_AGG_PLAIN_TRANS_[INIT_][STRICT_]{BYVAL,BYREF} and ORDERED_TRANS */
    pub agg_trans: ExprEvalStep_agg_trans,
    /* for EEOP_IS_JSON */
    pub is_json: ExprEvalStep_is_json,
    /* for EEOP_JSONEXPR_PATH */
    pub jsonexpr: ExprEvalStep_jsonexpr,
    /* for EEOP_JSONEXPR_COERCION */
    pub jsonexpr_coercion: ExprEvalStep_jsonexpr_coercion,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_fetch {
    /* attribute number up to which to fetch (inclusive) */
    pub last_var: c_int,
    /* will the type of slot be the same for every invocation */
    pub fixed: bool,
    /* tuple descriptor, if known */
    pub known_desc: TupleDesc,
    /* type of slot, can only be relied upon if fixed is set */
    pub kind: *const TupleTableSlotOps,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_var {
    /* attnum is attr number - 1 for regular VAR ...
     * but it's just the normal (negative) attr number for SYSVAR */
    pub attnum: c_int,
    pub vartype: Oid,
    pub varreturningtype: VarReturningType,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_wholerow {
    pub var: *mut Var,
    pub first: bool,
    pub slow: bool,
    pub tupdesc: TupleDesc,
    pub junkFilter: *mut JunkFilter,
    /// TODO(pg-port): Rust-only: cached slot for the wholerow var (set at first eval)
    pub slot: *mut crate::executor::tuptable::TupleTableSlot,
    /// TODO(pg-port): Rust-only: whether the caller wants an expanded record datum
    pub give_expanded: bool,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_assign_var {
    pub resultnum: c_int,
    pub attnum: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_assign_tmp {
    pub resultnum: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_returningexpr {
    pub nullflag: uint8,
    pub jumpdone: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_constval {
    pub value: Datum,
    pub isnull: bool,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_func {
    pub finfo: *mut FmgrInfo,
    pub fcinfo_data: FunctionCallInfo,
    pub fn_addr: PGFunction,
    pub nargs: c_int,
    pub make_ro: bool,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_boolexpr {
    pub anynull: *mut bool,
    pub jumpdone: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_qualexpr {
    pub jumpdone: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_jump {
    pub jumpdone: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_nulltest_row {
    pub rowcache: ExprEvalRowtypeCache,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_param {
    pub paramid: c_int,
    pub paramtype: Oid,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_cparam {
    pub paramfunc: ExecEvalSubroutine,
    pub paramarg: *mut c_void,
    pub paramarg2: *mut c_void,
    pub paramid: c_int,
    pub paramtype: Oid,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_casetest {
    pub value: *mut Datum,
    pub isnull: *mut bool,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_make_readonly {
    pub value: *mut Datum,
    pub isnull: *mut bool,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_iocoerce {
    pub finfo_out: *mut FmgrInfo,
    pub fcinfo_data_out: FunctionCallInfo,
    pub finfo_in: *mut FmgrInfo,
    pub fcinfo_data_in: FunctionCallInfo,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_sqlvaluefunction {
    pub svf: *mut SQLValueFunction,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_nextvalueexpr {
    pub seqid: Oid,
    pub seqtypid: Oid,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_arrayexpr {
    pub elemvalues: *mut Datum,
    pub elemnulls: *mut bool,
    pub nelems: c_int,
    pub elemtype: Oid,
    pub elemlength: int16,
    pub elembyval: bool,
    pub elemalign: c_char,
    pub multidims: bool,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_arraycoerce {
    pub elemexprstate: *mut ExprState,
    pub resultelemtype: Oid,
    pub amstate: *mut ArrayMapState,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_row {
    pub tupdesc: TupleDesc,
    pub elemvalues: *mut Datum,
    pub elemnulls: *mut bool,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_rowcompare_step {
    pub finfo: *mut FmgrInfo,
    pub fcinfo_data: FunctionCallInfo,
    pub fn_addr: PGFunction,
    pub jumpnull: c_int,
    pub jumpdone: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_rowcompare_final {
    pub cmptype: CompareType,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_minmax {
    pub values: *mut Datum,
    pub nulls: *mut bool,
    pub nelems: c_int,
    pub op: MinMaxOp,
    pub finfo: *mut FmgrInfo,
    pub fcinfo_data: FunctionCallInfo,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_fieldselect {
    pub fieldnum: AttrNumber,
    pub resulttype: Oid,
    pub rowcache: ExprEvalRowtypeCache,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_fieldstore {
    pub fstore: *mut FieldStore,
    pub rowcache: *mut ExprEvalRowtypeCache,
    pub values: *mut Datum,
    pub nulls: *mut bool,
    pub ncolumns: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_sbsref_subscript {
    pub subscriptfunc: ExecEvalBoolSubroutine,
    pub state: *mut SubscriptingRefState,
    pub jumpdone: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_sbsref {
    pub subscriptfunc: ExecEvalSubroutine,
    pub state: *mut SubscriptingRefState,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_domaincheck {
    pub constraintname: *mut c_char,
    pub checkvalue: *mut Datum,
    pub checknull: *mut bool,
    pub resulttype: Oid,
    pub escontext: *mut ErrorSaveContext,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_hashdatum_initvalue {
    pub init_value: Datum,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_hashdatum {
    pub finfo: *mut FmgrInfo,
    pub fcinfo_data: FunctionCallInfo,
    pub fn_addr: PGFunction,
    pub jumpdone: c_int,
    pub iresult: *mut NullableDatum,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_convert_rowtype {
    pub inputtype: Oid,
    pub outputtype: Oid,
    pub incache: *mut ExprEvalRowtypeCache,
    pub outcache: *mut ExprEvalRowtypeCache,
    pub map: *mut TupleConversionMap,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_scalararrayop {
    pub element_type: Oid,
    pub useOr: bool,
    pub typlen: int16,
    pub typbyval: bool,
    pub typalign: c_char,
    pub finfo: *mut FmgrInfo,
    pub fcinfo_data: FunctionCallInfo,
    pub fn_addr: PGFunction,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_hashedscalararrayop {
    pub has_nulls: bool,
    pub inclause: bool,
    pub elements_tab: *mut ScalarArrayOpExprHashTable,
    pub finfo: *mut FmgrInfo,
    pub fcinfo_data: FunctionCallInfo,
    pub saop: *mut ScalarArrayOpExpr,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_xmlexpr {
    pub xexpr: *mut XmlExpr,
    pub named_argvalue: *mut Datum,
    pub named_argnull: *mut bool,
    pub argvalue: *mut Datum,
    pub argnull: *mut bool,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_json_constructor {
    pub jcstate: *mut JsonConstructorExprState,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_aggref {
    pub aggno: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_grouping_func {
    pub clauses: *mut List,
    /// TODO(pg-port): Rust-only: argument column list (from GROUPING(...) args)
    pub args: *mut List,
    /// TODO(pg-port): Rust-only: per-set list of grouped column lists
    pub grouped_cols: *mut List,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_window_func {
    pub wfstate: *mut WindowFuncExprState,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_subplan {
    pub sstate: *mut SubPlanState,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_agg_deserialize {
    pub fcinfo_data: FunctionCallInfo,
    pub jumpnull: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_agg_strict_input_check {
    /*
     * For EEOP_AGG_STRICT_INPUT_CHECK_ARGS args contains pointers to
     * the NullableDatums that need to be checked for NULLs.
     *
     * For EEOP_AGG_STRICT_INPUT_CHECK_NULLS nulls contains pointers
     * to booleans that need to be checked for NULLs.
     */
    pub args: *mut NullableDatum,
    pub nulls: *mut bool,
    pub nargs: c_int,
    pub jumpnull: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_agg_plain_pergroup_nullcheck {
    pub setoff: c_int,
    pub jumpnull: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_agg_presorted_distinctcheck {
    pub pertrans: AggStatePerTrans,
    pub aggcontext: *mut ExprContext,
    pub jumpdistinct: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_agg_trans {
    pub pertrans: AggStatePerTrans,
    pub aggcontext: *mut ExprContext,
    pub setno: c_int,
    pub transno: c_int,
    pub setoff: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_is_json {
    pub pred: *mut JsonIsPredicate,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_jsonexpr {
    pub jsestate: *mut JsonExprState,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ExprEvalStep_jsonexpr_coercion {
    pub targettype: Oid,
    pub targettypmod: int32,
    pub omit_quotes: bool,
    pub exists_coerce: bool,
    pub exists_cast_to_int: bool,
    pub exists_check_domain: bool,
    pub json_coercion_cache: *mut c_void,
    pub escontext: *mut ErrorSaveContext,
}

/* ---- Non-inline data for container operations (from execExpr.h) ---- */

#[repr(C)]
pub struct SubscriptingRefState {
    pub isassignment: bool,
    pub workspace: *mut c_void,
    /* numupper/upperprovided[] filled at expression compile time */
    pub numupper: c_int,
    pub upperprovided: *mut bool,
    pub upperindex: *mut Datum,
    pub upperindexnull: *mut bool,
    pub numlower: c_int,
    pub lowerprovided: *mut bool,
    pub lowerindex: *mut Datum,
    pub lowerindexnull: *mut bool,
    /* for assignment: new value */
    pub replacevalue: Datum,
    pub replacenull: bool,
    /* for nested assignment: sbs_fetch_old puts old value here */
    pub prevvalue: Datum,
    pub prevnull: bool,
}

/* Execution step methods used for SubscriptingRef */
#[repr(C)]
pub struct SubscriptExecSteps {
    pub sbs_check_subscripts: ExecEvalBoolSubroutine,
    pub sbs_fetch: ExecEvalSubroutine,
    pub sbs_assign: ExecEvalSubroutine,
    pub sbs_fetch_old: ExecEvalSubroutine,
    /* sbs_fetch_strict: fetch is strict (NULL input => NULL out);
     * nodes/subscripting.h SubscriptRoutines.fetch_strict */
    pub sbs_fetch_strict: bool,
}

/* EEOP_JSON_CONSTRUCTOR state, too big to inline */
#[repr(C)]
pub struct JsonConstructorExprState {
    pub constructor: *mut JsonConstructorExpr,
    pub arg_values: *mut Datum,
    pub arg_nulls: *mut bool,
    pub arg_types: *mut Oid,
    pub arg_type_cache: *mut JsonConstructorExprState_arg_type_cache,
    pub nargs: c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct JsonConstructorExprState_arg_type_cache {
    pub category: c_int,
    pub outfuncid: Oid,
}

/* ---- Opaque/forward types needed by ExprEvalStep fields ---- */

/* TODO(pg-port): nodes/subscripting.h SubscriptRoutines */
#[repr(C)]
pub struct SubscriptRoutines {
    pub exec_setup: Option<
        unsafe fn(
            sbsref: *const SubscriptingRef,
            sbsrefstate: *mut SubscriptingRefState,
            methods: *mut SubscriptExecSteps,
        ),
    >,
    pub fetch_strict: bool,
}

/* TypeCacheEntry / DomainConstraintRef: real utils/typcache.h layout */
pub use crate::utils::cache::typcache::{DomainConstraintRef, TypeCacheEntry};

/* TODO(pg-port): nodes/execnodes.h ScalarArrayOpExprHashTable (opaque, lives in execExprInterp) */
#[repr(C)]
pub struct ScalarArrayOpExprHashTable {
    /// underlying hash table (saophash_hash*) -- opaque pointer
    pub hashtab: *mut core::ffi::c_void,
    /// the ExprEvalStep owning this table
    pub op: *mut ExprEvalStep,
    /// fmgr info for the hash function
    pub hash_finfo: crate::utils::fmgr::FmgrInfo,
    /// pre-initialized fcinfo for hash function
    pub hash_fcinfo_data: crate::utils::fmgr::FunctionCallInfoBaseData,
}

/* TODO(pg-port): utils/jsonpath.h JsonPathVariable */
#[repr(C)]
pub struct JsonPathVariable {
    pub name: *const c_char,
    pub namelen: usize,
    pub typid: Oid,
    pub typmod: int32,
    pub value: Datum,
    pub isnull: bool,
}

/* TODO(pg-port): catalog/objectaccess.h AclResult / AclMode / ObjectType */
type AclResult = c_int;
type AclMode = uint32;
const ACLCHECK_OK: AclResult = 0;
type ObjectType = c_int;
const OBJECT_FUNCTION: ObjectType = 10;

/* TODO(pg-port): catalog/pg_proc.h BTORDER_PROC */
const BTORDER_PROC: c_int = 1;

/* TODO(pg-port): catalog/pg_type.h TYPTYPE_DOMAIN */
const TYPTYPE_DOMAIN: c_char = b'd' as c_char;

/* TODO(pg-port): utils/pgstat.h pgstat_track_functions */
static pgstat_track_functions: c_int = 0;

/* TODO(pg-port): pg_config_manual.h FUNC_MAX_ARGS */
const FUNC_MAX_ARGS: c_int = 100;

/* TODO(pg-port): utils/elog.h INT4OID */
const INT4OID: Oid = 23;

/* ===================================================================
 * Stubs for unported dependency functions
 * ===================================================================
 */

/* TODO(pg-port): execExprInterp.c - sibling file, next wave */
unsafe fn ExecReadyInterpretedExpr(_state: *mut ExprState) {
    unimplemented!("TODO(pg-port): execExprInterp::ExecReadyInterpretedExpr")
}

/* TODO(pg-port): jit/jit.h - JIT compilation path */
unsafe fn jit_compile_expr(_state: *mut ExprState) -> bool {
    false /* always fall back to interpreter */
}

/* TODO(pg-port): optimizer/optimizer.h */
unsafe fn expression_planner(
    expr: *mut crate::nodes::primnodes::Expr,
) -> *mut crate::nodes::primnodes::Expr {
    unimplemented!("TODO(pg-port): optimizer::expression_planner")
}

/* TODO(pg-port): nodes/nodeFuncs.h */
unsafe fn expression_tree_walker(
    _node: *mut Node,
    _walker: unsafe fn(*mut Node, *mut c_void) -> bool,
    _context: *mut c_void,
) -> bool {
    unimplemented!("TODO(pg-port): nodes::nodeFuncs::expression_tree_walker")
}

/* TODO(pg-port): nodes/makefuncs.h */
unsafe fn make_ands_explicit(
    _qual: *mut List,
) -> *mut crate::nodes::primnodes::Expr {
    unimplemented!("TODO(pg-port): nodes::makefuncs::make_ands_explicit")
}

/* TODO(pg-port): executor/nodeSubplan.c */
unsafe fn ExecInitSubPlan(
    _subplan: *mut SubPlan,
    _parent: *mut PlanState,
) -> *mut SubPlanState {
    unimplemented!("TODO(pg-port): executor::nodeSubplan::ExecInitSubPlan")
}

/* TODO(pg-port): executor/execJunk.c */
pub unsafe fn ExecInitJunkFilter(
    _targetlist: *mut List,
    _slot: *mut TupleTableSlot,
) -> *mut JunkFilter {
    unimplemented!("TODO(pg-port): executor::execJunk::ExecInitJunkFilter")
}

/* TODO(pg-port): executor/execUtils.c */
unsafe fn ExecInitExtraTupleSlot(
    _estate: *mut crate::nodes::execnodes::EState,
    _tupdesc: TupleDesc,
    _tts_ops: *const TupleTableSlotOps,
) -> *mut TupleTableSlot {
    unimplemented!("TODO(pg-port): executor::execUtils::ExecInitExtraTupleSlot")
}

/* TODO(pg-port): executor/execUtils.c */
unsafe fn executor_errposition(
    _estate: *mut crate::nodes::execnodes::EState,
    _location: c_int,
) -> c_int {
    0
}

/* TODO(pg-port): executor/execUtils.c */
unsafe fn ExecGetResultType(_ps: *mut PlanState) -> TupleDesc {
    unimplemented!("TODO(pg-port): executor::execUtils::ExecGetResultType")
}

/* TODO(pg-port): executor/execUtils.c */
unsafe fn ExecGetResultSlotOps(
    _ps: *mut PlanState,
    _isfixed: *mut bool,
) -> *const TupleTableSlotOps {
    unimplemented!("TODO(pg-port): executor::execUtils::ExecGetResultSlotOps")
}

/* TODO(pg-port): executor/execTuples.c */
unsafe fn ExecTypeFromExprList(_exprlist: *mut List) -> TupleDesc {
    unimplemented!("TODO(pg-port): executor::execTuples::ExecTypeFromExprList")
}

/* TODO(pg-port): executor/execTuples.c */
unsafe fn ExecTypeSetColNames(_tupdesc: TupleDesc, _colnames: *mut List) {
    unimplemented!("TODO(pg-port): executor::execTuples::ExecTypeSetColNames")
}

/* TODO(pg-port): access/common/tupdesc.c */
unsafe fn BlessTupleDesc(_tupdesc: TupleDesc) {
    unimplemented!("TODO(pg-port): access::common::tupdesc::BlessTupleDesc")
}

/* TODO(pg-port): utils/typcache.c */
unsafe fn lookup_rowtype_tupdesc(_typid: Oid, _typmod: int32) -> TupleDesc {
    unimplemented!("TODO(pg-port): utils::typcache::lookup_rowtype_tupdesc")
}

/* TODO(pg-port): utils/typcache.c */
unsafe fn lookup_rowtype_tupdesc_copy(_typid: Oid, _typmod: int32) -> TupleDesc {
    unimplemented!("TODO(pg-port): utils::typcache::lookup_rowtype_tupdesc_copy")
}

/* TODO(pg-port): access/common/tupdesc.c */
unsafe fn ReleaseTupleDesc(_tupdesc: TupleDesc) {}

/* TODO(pg-port): utils/typcache.c */
unsafe fn lookup_type_cache(_typid: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!("TODO(pg-port): utils::typcache::lookup_type_cache")
}

/* TODO(pg-port): utils/typcache.c */
unsafe fn InitDomainConstraintRef(
    _typid: Oid,
    _ref_: *mut DomainConstraintRef,
    _mcxt: MemoryContext,
    _need_exprstate: bool,
) {
    unimplemented!("TODO(pg-port): utils::typcache::InitDomainConstraintRef")
}

/* TODO(pg-port): nodes/subscripting.h */
unsafe fn getSubscriptingRoutines(
    _typid: Oid,
    _fnoid: *mut Oid,
) -> *const SubscriptRoutines {
    unimplemented!("TODO(pg-port): nodes::subscripting::getSubscriptingRoutines")
}

/* TODO(pg-port): utils/lsyscache.c */
unsafe fn get_typlen(_typid: Oid) -> int16 {
    unimplemented!("TODO(pg-port): utils::lsyscache::get_typlen")
}

/* TODO(pg-port): utils/lsyscache.c */
unsafe fn get_element_type(_typid: Oid) -> Oid {
    unimplemented!("TODO(pg-port): utils::lsyscache::get_element_type")
}

/* TODO(pg-port): utils/lsyscache.c */
unsafe fn get_typlenbyvalalign(
    _typid: Oid,
    _typlen: *mut int16,
    _typbyval: *mut bool,
    _typalign: *mut c_char,
) {
    unimplemented!("TODO(pg-port): utils::lsyscache::get_typlenbyvalalign")
}

/* TODO(pg-port): utils/lsyscache.c */
unsafe fn get_func_name(_funcid: Oid) -> *mut c_char {
    unimplemented!("TODO(pg-port): utils::lsyscache::get_func_name")
}

/* TODO(pg-port): utils/lsyscache.c */
unsafe fn format_type_be(_typid: Oid) -> *mut c_char {
    unimplemented!("TODO(pg-port): utils::lsyscache::format_type_be")
}

/* TODO(pg-port): utils/lsyscache.c */
unsafe fn get_op_opfamily_properties(
    _opno: Oid,
    _opfamily: Oid,
    _ordering: bool,
    _strategy: *mut c_int,
    _lefttype: *mut Oid,
    _righttype: *mut Oid,
) {
    unimplemented!("TODO(pg-port): utils::lsyscache::get_op_opfamily_properties")
}

/* TODO(pg-port): utils/lsyscache.c */
unsafe fn get_opfamily_proc(
    _opfamily: Oid,
    _lefttype: Oid,
    _righttype: Oid,
    _procnum: c_int,
) -> Oid {
    unimplemented!("TODO(pg-port): utils::lsyscache::get_opfamily_proc")
}

/* TODO(pg-port): utils/lsyscache.c */
unsafe fn getTypeOutputInfo(_typid: Oid, _funcid: *mut Oid, _varlena: *mut bool) {
    unimplemented!("TODO(pg-port): utils::lsyscache::getTypeOutputInfo")
}

/* TODO(pg-port): utils/lsyscache.c */
unsafe fn getTypeInputInfo(_typid: Oid, _funcid: *mut Oid, _typioparam: *mut Oid) {
    unimplemented!("TODO(pg-port): utils::lsyscache::getTypeInputInfo")
}

/* TODO(pg-port): utils/lsyscache.c */
unsafe fn get_typtype(_typid: Oid) -> c_char {
    unimplemented!("TODO(pg-port): utils::lsyscache::get_typtype")
}

/* TODO(pg-port): utils/lsyscache.c */
unsafe fn getBaseType(_typid: Oid) -> Oid {
    unimplemented!("TODO(pg-port): utils::lsyscache::getBaseType")
}

/* TODO(pg-port): utils/typcache.c */
unsafe fn DomainHasConstraints(_typid: Oid) -> bool {
    unimplemented!("TODO(pg-port): utils::typcache::DomainHasConstraints")
}

/* TODO(pg-port): nodes/nodeFuncs.h */
unsafe fn exprType(_node: *const Node) -> Oid {
    unimplemented!("TODO(pg-port): nodes::nodeFuncs::exprType")
}

/* TODO(pg-port): nodes/nodeFuncs.h */
unsafe fn exprTypmod(_node: *const Node) -> int32 {
    unimplemented!("TODO(pg-port): nodes::nodeFuncs::exprTypmod")
}

/* TODO(pg-port): nodes/nodeFuncs.h */
unsafe fn exprLocation(_node: *const Node) -> c_int {
    unimplemented!("TODO(pg-port): nodes::nodeFuncs::exprLocation")
}

/* TODO(pg-port): utils/fmgr.c */
unsafe fn fmgr_info(_funcid: Oid, _finfo: *mut FmgrInfo) {
    unimplemented!("TODO(pg-port): utils::fmgr::fmgr_info")
}

/* TODO(pg-port): utils/fmgr.c */
unsafe fn fmgr_info_set_expr(_expr: *mut Node, _finfo: *mut FmgrInfo) {
    unimplemented!("TODO(pg-port): utils::fmgr::fmgr_info_set_expr")
}

/* TODO(pg-port): utils/fmgr.h */
unsafe fn InitFunctionCallInfoData(
    _fcinfo: *mut crate::utils::fmgr::FunctionCallInfoBaseData,
    _finfo: *mut FmgrInfo,
    _nargs: c_int,
    _collation: Oid,
    _context: *mut Node,
    _resultinfo: *mut Node,
) {
    unimplemented!("TODO(pg-port): utils::fmgr::InitFunctionCallInfoData")
}

/* TODO(pg-port): utils/fmgr.h SizeForFunctionCallInfo macro -> size */
unsafe fn SizeForFunctionCallInfo(_nargs: c_int) -> usize {
    unimplemented!("TODO(pg-port): utils::fmgr::SizeForFunctionCallInfo")
}

/* TODO(pg-port): catalog/objectaccess.h */
unsafe fn object_aclcheck(
    _classid: Oid, _objectid: Oid, _roleid: Oid, _mode: AclMode,
) -> AclResult {
    unimplemented!("TODO(pg-port): catalog::objectaccess::object_aclcheck")
}

/* TODO(pg-port): catalog/objectaccess.h */
unsafe fn aclcheck_error(_result: AclResult, _objtype: ObjectType, _name: *const c_char) {
    unimplemented!("TODO(pg-port): catalog::objectaccess::aclcheck_error")
}

/* TODO(pg-port): catalog/objectaccess.h */
unsafe fn InvokeFunctionExecuteHook(_funcid: Oid) {}

/* TODO(pg-port): miscadmin.h */
unsafe fn GetUserId() -> Oid {
    unimplemented!("TODO(pg-port): miscadmin::GetUserId")
}

/* TODO(pg-port): utils/jsonfuncs.h */
type JsonTypeCategory = c_int;
unsafe fn json_categorize_type(
    _typid: Oid, _is_jsonb: bool,
    _category: *mut JsonTypeCategory, _outfuncid: *mut Oid,
) {
    unimplemented!("TODO(pg-port): utils::jsonfuncs::json_categorize_type")
}

/* TODO(pg-port): nodes/makefuncs.h */
unsafe fn makeNullConst(_typid: Oid, _typmod: int32, _collid: Oid) -> *mut Node {
    unimplemented!("TODO(pg-port): nodes::makefuncs::makeNullConst")
}

/* TODO(pg-port): utils/mmgr.h CurrentMemoryContext */
unsafe fn CurrentMemoryContext() -> MemoryContext {
    unimplemented!("TODO(pg-port): utils::mmgr::CurrentMemoryContext")
}

/* DO_AGGSPLIT_COMBINE: imported from crate::nodes::nodes */

/* innerPlanState / outerPlanState macros */
#[inline]
unsafe fn innerPlanState(node: *mut PlanState) -> *mut PlanState {
    (*(*node).plan).righttree as *mut _
}

#[inline]
unsafe fn outerPlanState(node: *mut PlanState) -> *mut PlanState {
    (*(*node).plan).lefttree as *mut _
}

/* check_stack_depth - miscadmin stub */
#[inline]
unsafe fn check_stack_depth() {}

/* ProcedureRelationId (catalog/pg_proc.h) */
const ProcedureRelationId: Oid = 1255;

/*
 * ExprSetupInfo: scratch structure for ExecCreateExprSetupSteps / ExecPushExprSetupSteps.
 * Tracks the highest attribute number referenced from each tuple slot, and
 * collects any MULTIEXPR SubPlan nodes that need advance execution.
 */
struct ExprSetupInfo {
    last_inner: AttrNumber,
    last_outer: AttrNumber,
    last_scan: AttrNumber,
    last_old: AttrNumber,
    last_new: AttrNumber,
    multiexpr_subplans: *mut List,
}

/* ===================================================================
 * ExecInitExpr: prepare an expression tree for execution
 *
 * This function builds and returns an ExprState implementing the given
 * Expr node tree.  The return ExprState can then be handed to ExecEvalExpr
 * for execution.  Because the Expr tree itself is read-only as far as
 * ExecInitExpr and ExecEvalExpr are concerned, several different executions
 * of the same plan tree can occur concurrently.  (But note that an ExprState
 * does mutate at runtime, so it can't be re-used concurrently.)
 *
 * This must be called in a memory context that will last as long as repeated
 * executions of the expression are needed.  Typically the context will be
 * the same as the per-query context of the associated ExprContext.
 *
 * Any Aggref, WindowFunc, or SubPlan nodes found in the tree are added to
 * the lists of such nodes held by the parent PlanState.
 *
 * Note: there is no ExecEndExpr function; we assume that any resource
 * cleanup needed will be handled by just releasing the memory context
 * in which the state tree is built.  Functions that require additional
 * cleanup work can register a shutdown callback in the ExprContext.
 *
 *   'node' is the root of the expression tree to compile.
 *   'parent' is the PlanState node that owns the expression.
 *
 * 'parent' may be NULL if we are preparing an expression that is not
 * associated with a plan tree.  (If so, it can't have aggs or subplans.)
 * Such cases should usually come through ExecPrepareExpr, not directly here.
 *
 * Also, if 'node' is NULL, we just return NULL.  This is convenient for some
 * callers that may or may not have an expression that needs to be compiled.
 * Note that a NULL ExprState pointer *cannot* be handed to ExecEvalExpr,
 * although ExecQual and ExecCheck will accept one (and treat it as "true").
 * ===================================================================
 */
pub unsafe fn ExecInitExpr(
    node: *mut crate::nodes::primnodes::Expr,
    parent: *mut PlanState,
) -> *mut ExprState {
    let state: *mut ExprState;
    let mut scratch: ExprEvalStep = core::mem::zeroed();

    /* Special case: NULL expression produces a NULL ExprState pointer */
    if node.is_null() {
        return core::ptr::null_mut();
    }

    /* Initialize ExprState with empty step list */
    state = makeNode!(ExprState, T_ExprState);
    (*state).expr = node;
    (*state).parent = parent;
    (*state).ext_params = core::ptr::null_mut();

    /* Insert setup steps as needed */
    ExecCreateExprSetupSteps(state, node as *mut Node);

    /* Compile the expression proper */
    ExecInitExprRec(node, state, &mut (*state).resvalue, &mut (*state).resnull);

    /* Finally, append a DONE step */
    scratch.opcode = EEOP_DONE_RETURN as isize;
    ExprEvalPushStep(state, &scratch);

    ExecReadyExpr(state);

    state
}

/*
 * ExecInitExprWithParams: prepare a standalone expression tree for execution
 *
 * This is the same as ExecInitExpr, except that there is no parent PlanState,
 * and instead we may have a ParamListInfo describing PARAM_EXTERN Params.
 */
pub unsafe fn ExecInitExprWithParams(
    node: *mut crate::nodes::primnodes::Expr,
    ext_params: crate::nodes::params::ParamListInfo,
) -> *mut ExprState {
    let state: *mut ExprState;
    let mut scratch: ExprEvalStep = core::mem::zeroed();

    /* Special case: NULL expression produces a NULL ExprState pointer */
    if node.is_null() {
        return core::ptr::null_mut();
    }

    /* Initialize ExprState with empty step list */
    state = makeNode!(ExprState, T_ExprState);
    (*state).expr = node;
    (*state).parent = core::ptr::null_mut();
    (*state).ext_params = ext_params;

    /* Insert setup steps as needed */
    ExecCreateExprSetupSteps(state, node as *mut Node);

    /* Compile the expression proper */
    ExecInitExprRec(node, state, &mut (*state).resvalue, &mut (*state).resnull);

    /* Finally, append a DONE step */
    scratch.opcode = EEOP_DONE_RETURN as isize;
    ExprEvalPushStep(state, &scratch);

    ExecReadyExpr(state);

    state
}

/*
 * ExecInitQual: prepare a qual for execution by ExecQual
 *
 * Prepares for the evaluation of a conjunctive boolean expression (qual list
 * with implicit AND semantics) that returns true if none of the
 * subexpressions are false.
 *
 * We must return true if the list is empty.  Since that's a very common case,
 * we optimize it a bit further by translating to a NULL ExprState pointer
 * rather than setting up an ExprState that computes constant TRUE.
 *
 * If any of the subexpressions yield NULL, then the result of the conjunction
 * is false.  This makes ExecQual primarily useful for evaluating WHERE clauses.
 */
pub unsafe fn ExecInitQual(
    qual: *mut List,
    parent: *mut PlanState,
) -> *mut ExprState {
    let state: *mut ExprState;
    let mut scratch: ExprEvalStep = core::mem::zeroed();
    let mut adjust_jumps: *mut List = NIL;

    /* short-circuit (here and in ExecQual) for empty restriction list */
    if qual.is_null() {
        return core::ptr::null_mut();
    }

    debug_assert!(IsA!(qual, T_List));

    state = makeNode!(ExprState, T_ExprState);
    (*state).expr = qual as *mut crate::nodes::primnodes::Expr;
    (*state).parent = parent;
    (*state).ext_params = core::ptr::null_mut();

    /* mark expression as to be used with ExecQual() */
    (*state).flags = EEO_FLAG_IS_QUAL;

    /* Insert setup steps as needed */
    ExecCreateExprSetupSteps(state, qual as *mut Node);

    /*
     * ExecQual() needs to return false for an expression returning NULL. That
     * allows us to short-circuit the evaluation the first time a NULL is
     * encountered.  As qual evaluation is a hot-path this warrants using a
     * special opcode for qual evaluation that's simpler than BOOL_AND.
     */
    scratch.opcode = EEOP_QUAL as isize;

    /* We can use ExprState's resvalue/resnull as target for each qual expr. */
    scratch.resvalue = &mut (*state).resvalue;
    scratch.resnull = &mut (*state).resnull;

    let mut lc = crate::nodes::pg_list::list_head(qual);
    while !lc.is_null() {
        let node = *(lc as *mut *mut crate::nodes::primnodes::Expr);

        /* first evaluate expression */
        ExecInitExprRec(node, state, &mut (*state).resvalue, &mut (*state).resnull);

        /* then emit EEOP_QUAL to detect if it's false (or null) */
        scratch.d.qualexpr.jumpdone = -1;
        ExprEvalPushStep(state, &scratch);
        adjust_jumps = lappend_int(adjust_jumps, (*state).steps_len - 1);

        lc = crate::nodes::pg_list::lnext(qual, lc);
    }

    /* adjust jump targets */
    let mut lc2 = crate::nodes::pg_list::list_head(adjust_jumps);
    while !lc2.is_null() {
        let jump = crate::nodes::pg_list::lfirst_int(lc2);
        let as_ = &mut *(*state).steps.add(jump as usize);

        debug_assert!(as_.opcode == EEOP_QUAL as isize);
        debug_assert!(as_.d.qualexpr.jumpdone == -1);
        as_.d.qualexpr.jumpdone = (*state).steps_len;

        lc2 = crate::nodes::pg_list::lnext(adjust_jumps, lc2);
    }

    /*
     * At the end, we don't need to do anything more.  The last qual expr must
     * have yielded TRUE, and since its result is stored in the desired output
     * location, we're done.
     */
    scratch.opcode = EEOP_DONE_RETURN as isize;
    ExprEvalPushStep(state, &scratch);

    ExecReadyExpr(state);

    state
}

/*
 * ExecInitCheck: prepare a check constraint for execution by ExecCheck
 *
 * This is much like ExecInitQual/ExecQual, except that a null result from
 * the conjunction is treated as TRUE.  This behavior is appropriate for
 * evaluating CHECK constraints, since SQL specifies that NULL constraint
 * conditions are not failures.
 *
 * Note that like ExecInitQual, this expects input in implicit-AND format.
 */
pub unsafe fn ExecInitCheck(
    qual: *mut List,
    parent: *mut PlanState,
) -> *mut ExprState {
    /* short-circuit (here and in ExecCheck) for empty restriction list */
    if qual.is_null() {
        return core::ptr::null_mut();
    }

    debug_assert!(IsA!(qual, T_List));

    /*
     * Just convert the implicit-AND list to an explicit AND (if there's more
     * than one entry), and compile normally.  Unlike ExecQual, we can't
     * short-circuit on NULL results, so the regular AND behavior is needed.
     */
    ExecInitExpr(make_ands_explicit(qual), parent)
}

/*
 * Call ExecInitExpr() on a list of expressions, return a list of ExprStates.
 */
pub unsafe fn ExecInitExprList(
    nodes: *mut List,
    parent: *mut PlanState,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut lc = crate::nodes::pg_list::list_head(nodes);
    while !lc.is_null() {
        let e = *(lc as *mut *mut crate::nodes::primnodes::Expr);
        result = lappend(result, ExecInitExpr(e, parent) as *mut c_void);
        lc = crate::nodes::pg_list::lnext(nodes, lc);
    }
    result
}

/*
 * ExecBuildProjectionInfo
 *
 * Build a ProjectionInfo node for evaluating the given tlist in the given
 * econtext, and storing the result into the tuple slot.  (Caller must have
 * ensured that tuple slot has a descriptor matching the tlist!)
 *
 * inputDesc can be NULL, but if it is not, we check to see whether simple
 * Vars in the tlist match the descriptor.  It is important to provide
 * inputDesc for relation-scan plan nodes.
 *
 * This is implemented by internally building an ExprState that performs the
 * whole projection in one go.
 *
 * Caution: before PG v10, the targetList was a list of ExprStates; now it
 * should be the planner-created targetlist, since we do the compilation here.
 */
pub unsafe fn ExecBuildProjectionInfo(
    targetList: *mut List,
    econtext: *mut ExprContext,
    slot: *mut TupleTableSlot,
    parent: *mut PlanState,
    inputDesc: TupleDesc,
) -> *mut ProjectionInfo {
    let projInfo: *mut ProjectionInfo =
        makeNode!(ProjectionInfo, T_ProjectionInfo);
    let state: *mut ExprState;
    let mut scratch: ExprEvalStep = core::mem::zeroed();

    (*projInfo).pi_exprContext = econtext;
    /* We embed ExprState into ProjectionInfo instead of doing extra palloc */
    (*projInfo).pi_state.r#type = NodeTag::T_ExprState;
    state = &mut (*projInfo).pi_state;
    (*state).expr = targetList as *mut crate::nodes::primnodes::Expr;
    (*state).parent = parent;
    (*state).ext_params = core::ptr::null_mut();

    (*state).resultslot = slot;

    /* Insert setup steps as needed */
    ExecCreateExprSetupSteps(state, targetList as *mut Node);

    /* Now compile each tlist column */
    let mut lc = crate::nodes::pg_list::list_head(targetList);
    while !lc.is_null() {
        let tle = *(lc as *mut *mut TargetEntry);
        let mut variable: *mut Var = core::ptr::null_mut();
        let mut attnum: AttrNumber = 0;
        let mut isSafeVar = false;

        /*
         * If tlist expression is a safe non-system Var, use the fast-path
         * ASSIGN_*_VAR opcodes.  "Safe" means that we don't need to apply
         * CheckVarSlotCompatibility() during plan startup.
         */
        if !(*tle).expr.is_null()
            && IsA!((*tle).expr, T_Var)
            && (*((*tle).expr as *mut Var)).varattno > 0
        {
            /* Non-system Var, but how safe is it? */
            variable = (*tle).expr as *mut Var;
            attnum = (*variable).varattno;

            if inputDesc.is_null() {
                isSafeVar = true; /* can't check, just assume OK */
            } else if (attnum as c_int) <= (*inputDesc).natts {
                let attr = TupleDescAttr(inputDesc, (attnum - 1) as c_int);
                /*
                 * If user attribute is dropped or has a type mismatch, don't
                 * use ASSIGN_*_VAR.
                 */
                if !(*attr).attisdropped && (*variable).vartype == (*attr).atttypid {
                    isSafeVar = true;
                }
            }
        }

        if isSafeVar {
            /* Fast-path: just generate an EEOP_ASSIGN_*_VAR step */
            match (*variable).varno {
                crate::nodes::primnodes::INNER_VAR => {
                    scratch.opcode = EEOP_ASSIGN_INNER_VAR as isize;
                }
                crate::nodes::primnodes::OUTER_VAR => {
                    scratch.opcode = EEOP_ASSIGN_OUTER_VAR as isize;
                }
                _ => {
                    /* INDEX_VAR is handled here too */
                    match (*variable).varreturningtype {
                        VarReturningType::VAR_RETURNING_DEFAULT => {
                            scratch.opcode = EEOP_ASSIGN_SCAN_VAR as isize;
                        }
                        VarReturningType::VAR_RETURNING_OLD => {
                            scratch.opcode = EEOP_ASSIGN_OLD_VAR as isize;
                            (*state).flags |= EEO_FLAG_HAS_OLD;
                        }
                        VarReturningType::VAR_RETURNING_NEW => {
                            scratch.opcode = EEOP_ASSIGN_NEW_VAR as isize;
                            (*state).flags |= EEO_FLAG_HAS_NEW;
                        }
                    }
                }
            }
            scratch.d.assign_var.attnum = (attnum - 1) as c_int;
            scratch.d.assign_var.resultnum = ((*tle).resno - 1) as c_int;
            ExprEvalPushStep(state, &scratch);
        } else {
            /*
             * Otherwise, compile the column expression normally.
             *
             * We can't tell the expression to evaluate directly into the
             * result slot, as the result slot (and the exprstate for that
             * matter) can change between executions.  We instead evaluate
             * into the ExprState's resvalue/resnull and then move.
             */
            ExecInitExprRec(
                (*tle).expr,
                state,
                &mut (*state).resvalue,
                &mut (*state).resnull,
            );

            /*
             * Column might be referenced multiple times in upper nodes, so
             * force value to R/O - but only if it could be an expanded datum.
             */
            if get_typlen(exprType((*tle).expr as *const Node)) == -1 {
                scratch.opcode = EEOP_ASSIGN_TMP_MAKE_RO as isize;
            } else {
                scratch.opcode = EEOP_ASSIGN_TMP as isize;
            }
            scratch.d.assign_tmp.resultnum = ((*tle).resno - 1) as c_int;
            ExprEvalPushStep(state, &scratch);
        }

        lc = crate::nodes::pg_list::lnext(targetList, lc);
    }

    scratch.opcode = EEOP_DONE_NO_RETURN as isize;
    ExprEvalPushStep(state, &scratch);

    ExecReadyExpr(state);

    projInfo
}

/*
 * ExecBuildUpdateProjection
 *
 * Build a ProjectionInfo node for constructing a new tuple during UPDATE.
 * The projection will be executed in the given econtext and the result will
 * be stored into the given tuple slot.
 *
 * When evalTargetList is false, targetList contains the UPDATE ... SET
 * expressions that have already been computed by a subplan node.
 * When evalTargetList is true, targetList contains the UPDATE ... SET
 * expressions that must be computed.
 *
 * targetColnos contains a list of the target column numbers corresponding
 * to the non-resjunk entries of targetList.
 *
 * relDesc must describe the relation we intend to update.
 */
pub unsafe fn ExecBuildUpdateProjection(
    targetList: *mut List,
    evalTargetList: bool,
    targetColnos: *mut List,
    relDesc: TupleDesc,
    econtext: *mut ExprContext,
    slot: *mut TupleTableSlot,
    parent: *mut PlanState,
) -> *mut ProjectionInfo {
    let projInfo: *mut ProjectionInfo =
        makeNode!(ProjectionInfo, T_ProjectionInfo);
    let state: *mut ExprState;
    let mut nAssignableCols: c_int = 0;
    let mut sawJunk = false;
    let mut assignedCols: *mut crate::nodes::bitmapset::Bitmapset = core::ptr::null_mut();
    let mut deform = ExprSetupInfo {
        last_inner: 0,
        last_outer: 0,
        last_scan: 0,
        last_old: 0,
        last_new: 0,
        multiexpr_subplans: NIL,
    };
    let mut scratch: ExprEvalStep = core::mem::zeroed();
    let mut outerattnum: c_int = 0;

    (*projInfo).pi_exprContext = econtext;
    /* We embed ExprState into ProjectionInfo instead of doing extra palloc */
    (*projInfo).pi_state.r#type = NodeTag::T_ExprState;
    state = &mut (*projInfo).pi_state;
    if evalTargetList {
        (*state).expr = targetList as *mut crate::nodes::primnodes::Expr;
    } else {
        (*state).expr = core::ptr::null_mut(); /* not used */
    }
    (*state).parent = parent;
    (*state).ext_params = core::ptr::null_mut();

    (*state).resultslot = slot;

    /*
     * Examine the targetList to see how many non-junk columns there are, and
     * to verify that the non-junk columns come before the junk ones.
     */
    let mut lc = crate::nodes::pg_list::list_head(targetList);
    while !lc.is_null() {
        let tle = *(lc as *mut *mut TargetEntry);
        if (*tle).resjunk {
            sawJunk = true;
        } else {
            if sawJunk {
                elog!(ERROR, "subplan target list is out of order");
            }
            nAssignableCols += 1;
        }
        lc = crate::nodes::pg_list::lnext(targetList, lc);
    }

    /* We should have one targetColnos entry per non-junk column */
    if nAssignableCols != crate::nodes::pg_list::list_length(targetColnos) {
        elog!(ERROR, "targetColnos does not match subplan target list");
    }

    /*
     * Build a bitmapset of the columns in targetColnos.
     */
    let mut lc = crate::nodes::pg_list::list_head(targetColnos);
    while !lc.is_null() {
        let targetattnum = crate::nodes::pg_list::lfirst_int(lc);
        assignedCols =
            crate::nodes::bitmapset::bms_add_member(assignedCols, targetattnum);
        lc = crate::nodes::pg_list::lnext(targetColnos, lc);
    }

    /*
     * We need to insert EEOP_*_FETCHSOME steps to ensure the input tuples are
     * sufficiently deconstructed.
     */
    let mut attnum_i = (*relDesc).natts;
    while attnum_i > 0 {
        let attr = TupleDescCompactAttr(relDesc, attnum_i - 1);
        if (*attr).attisdropped {
            attnum_i -= 1;
            continue;
        }
        if crate::nodes::bitmapset::bms_is_member(attnum_i, assignedCols as *const _) {
            attnum_i -= 1;
            continue;
        }
        deform.last_scan = attnum_i as AttrNumber;
        break;
    }

    /*
     * If we're actually evaluating the tlist, incorporate its input
     * requirements too; otherwise, we'll just need to fetch the appropriate
     * number of columns of the "outer" tuple.
     */
    if evalTargetList {
        expr_setup_walker(targetList as *mut Node, &mut deform);
    } else {
        deform.last_outer = nAssignableCols as AttrNumber;
    }

    ExecPushExprSetupSteps(state, &mut deform);

    /*
     * Now generate code to evaluate the tlist's assignable expressions or
     * fetch them from the outer tuple.
     */
    let mut lc = crate::nodes::pg_list::list_head(targetList);
    let mut lc2 = crate::nodes::pg_list::list_head(targetColnos);
    loop {
        if lc.is_null() {
            break;
        }
        let tle = *(lc as *mut *mut TargetEntry);
        if (*tle).resjunk {
            lc = crate::nodes::pg_list::lnext(targetList, lc);
            continue;
        }
        let targetattnum = crate::nodes::pg_list::lfirst_int(lc2);

        /* Apply sanity checks comparable to ExecCheckPlanOutput(). */
        if targetattnum <= 0 || targetattnum > (*relDesc).natts {
            ereport!(
                ERROR,
                errmsg!(
                    "table row type and query-specified row type do not match"
                )
            );
        }
        let attr = TupleDescAttr(relDesc, targetattnum - 1);
        if (*attr).attisdropped {
            ereport!(
                ERROR,
                errmsg!(
                    "table row type and query-specified row type do not match"
                )
            );
        }
        if exprType((*tle).expr as *const Node) != (*attr).atttypid {
            ereport!(
                ERROR,
                errmsg!(
                    "table row type and query-specified row type do not match"
                )
            );
        }

        /* OK, generate code to perform the assignment. */
        if evalTargetList {
            /*
             * We must evaluate the TLE's expression and assign it.
             */
            ExecInitExprRec(
                (*tle).expr,
                state,
                &mut (*state).resvalue,
                &mut (*state).resnull,
            );
            /* Needn't worry about read-only-ness here, either. */
            scratch.opcode = EEOP_ASSIGN_TMP as isize;
            scratch.d.assign_tmp.resultnum = targetattnum - 1;
            ExprEvalPushStep(state, &scratch);
        } else {
            /* Just assign from the outer tuple. */
            scratch.opcode = EEOP_ASSIGN_OUTER_VAR as isize;
            scratch.d.assign_var.attnum = outerattnum;
            scratch.d.assign_var.resultnum = targetattnum - 1;
            ExprEvalPushStep(state, &scratch);
        }
        outerattnum += 1;

        lc = crate::nodes::pg_list::lnext(targetList, lc);
        lc2 = crate::nodes::pg_list::lnext(targetColnos, lc2);
    }

    /*
     * Now generate code to copy over any old columns that were not assigned
     * to, and to ensure that dropped columns are set to NULL.
     */
    let mut attnum_i = 1;
    while attnum_i <= (*relDesc).natts {
        let attr = TupleDescCompactAttr(relDesc, attnum_i - 1);
        if (*attr).attisdropped {
            /* Put a null into the ExprState's resvalue/resnull ... */
            scratch.opcode = EEOP_CONST as isize;
            scratch.resvalue = &mut (*state).resvalue;
            scratch.resnull = &mut (*state).resnull;
            scratch.d.constval.value = Datum::from(0usize);
            scratch.d.constval.isnull = true;
            ExprEvalPushStep(state, &scratch);
            /* ... then assign it to the result slot */
            scratch.opcode = EEOP_ASSIGN_TMP as isize;
            scratch.d.assign_tmp.resultnum = attnum_i - 1;
            ExprEvalPushStep(state, &scratch);
        } else if !crate::nodes::bitmapset::bms_is_member(attnum_i, assignedCols as *const _) {
            /* Certainly the right type, so needn't check */
            scratch.opcode = EEOP_ASSIGN_SCAN_VAR as isize;
            scratch.d.assign_var.attnum = attnum_i - 1;
            scratch.d.assign_var.resultnum = attnum_i - 1;
            ExprEvalPushStep(state, &scratch);
        }
        attnum_i += 1;
    }

    scratch.opcode = EEOP_DONE_NO_RETURN as isize;
    ExprEvalPushStep(state, &scratch);

    ExecReadyExpr(state);

    projInfo
}

/*
 * ExecPrepareExpr --- initialize for expression execution outside a normal
 * Plan tree context.
 *
 * This differs from ExecInitExpr in that we don't assume the caller is
 * already running in the EState's per-query context.  Also, we run the
 * passed expression tree through expression_planner().
 */
pub unsafe fn ExecPrepareExpr(
    node: *mut crate::nodes::primnodes::Expr,
    estate: *mut crate::nodes::execnodes::EState,
) -> *mut ExprState {
    let result: *mut ExprState;
    let oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);

    let node = expression_planner(node);

    result = ExecInitExpr(node, core::ptr::null_mut());

    MemoryContextSwitchTo(oldcontext);

    result
}

/*
 * ExecPrepareQual --- initialize for qual execution outside a normal
 * Plan tree context.
 */
pub unsafe fn ExecPrepareQual(
    qual: *mut List,
    estate: *mut crate::nodes::execnodes::EState,
) -> *mut ExprState {
    let result: *mut ExprState;
    let oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);

    let qual = expression_planner(qual as *mut crate::nodes::primnodes::Expr)
        as *mut List;

    result = ExecInitQual(qual, core::ptr::null_mut());

    MemoryContextSwitchTo(oldcontext);

    result
}

/*
 * ExecPrepareCheck -- initialize check constraint for execution outside a
 * normal Plan tree context.
 */
pub unsafe fn ExecPrepareCheck(
    qual: *mut List,
    estate: *mut crate::nodes::execnodes::EState,
) -> *mut ExprState {
    let result: *mut ExprState;
    let oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);

    let qual = expression_planner(qual as *mut crate::nodes::primnodes::Expr)
        as *mut List;

    result = ExecInitCheck(qual, core::ptr::null_mut());

    MemoryContextSwitchTo(oldcontext);

    result
}

/*
 * Call ExecPrepareExpr() on each member of a list of Exprs, and return
 * a list of ExprStates.
 */
pub unsafe fn ExecPrepareExprList(
    nodes: *mut List,
    estate: *mut crate::nodes::execnodes::EState,
) -> *mut List {
    let mut result: *mut List = NIL;
    let oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);

    let mut lc = crate::nodes::pg_list::list_head(nodes);
    while !lc.is_null() {
        let e = *(lc as *mut *mut crate::nodes::primnodes::Expr);
        result = lappend(
            result,
            ExecPrepareExpr(e, estate) as *mut c_void,
        );
        lc = crate::nodes::pg_list::lnext(nodes, lc);
    }

    MemoryContextSwitchTo(oldcontext);

    result
}

/*
 * ExecCheck - evaluate a check constraint
 *
 * For check constraints, a null result is taken as TRUE.
 */
pub unsafe fn ExecCheck(
    state: *mut ExprState,
    econtext: *mut ExprContext,
) -> bool {
    let mut isnull = false;

    /* short-circuit (here and in ExecInitCheck) for empty restriction list */
    if state.is_null() {
        return true;
    }

    /* verify that expression was not compiled using ExecInitQual */
    debug_assert!((*state).flags & EEO_FLAG_IS_QUAL == 0);

    let ret = ExecEvalExprSwitchContext(state, econtext, &mut isnull);

    if isnull {
        return true;
    }

    DatumGetBool(ret)
}

/*
 * Prepare a compiled expression for execution.  This has to be called for
 * every ExprState before it can be executed.
 *
 * NB: While this currently only calls ExecReadyInterpretedExpr(),
 * this will likely get extended to further expression evaluation methods.
 * Therefore this should be used instead of directly calling
 * ExecReadyInterpretedExpr().
 */
unsafe fn ExecReadyExpr(state: *mut ExprState) {
    if jit_compile_expr(state) {
        return;
    }

    ExecReadyInterpretedExpr(state);
}


/*
 * Append the steps necessary for the evaluation of node to ExprState->steps,
 * possibly recursing into sub-expressions of node.
 *
 * node - expression to evaluate
 * state - ExprState to whose ->steps to append the necessary operations
 * resv / resnull - where to store the result of the node into
 */
unsafe fn ExecInitExprRec(
    node: *mut crate::nodes::primnodes::Expr,
    state: *mut ExprState,
    resv: *mut Datum,
    resnull: *mut bool,
) {
    let mut scratch: ExprEvalStep = core::mem::zeroed();

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    /* Step's output location is always what the caller gave us */
    debug_assert!(!resv.is_null() && !resnull.is_null());
    scratch.resvalue = resv;
    scratch.resnull = resnull;

    /* cases should be ordered as they are in enum NodeTag */
    match NodeTag::from((*node).r#type) {
        NodeTag::T_Var => {
            let variable = node as *mut Var;

            if (*variable).varattno == InvalidAttrNumber {
                /* whole-row Var */
                ExecInitWholeRowVar(&mut scratch, variable, state);
            } else if (*variable).varattno <= 0 {
                /* system column */
                scratch.d.var.attnum = (*variable).varattno as c_int;
                scratch.d.var.vartype = (*variable).vartype;
                scratch.d.var.varreturningtype = (*variable).varreturningtype;
                match (*variable).varno {
                    crate::nodes::primnodes::INNER_VAR => {
                        scratch.opcode = EEOP_INNER_SYSVAR as isize;
                    }
                    crate::nodes::primnodes::OUTER_VAR => {
                        scratch.opcode = EEOP_OUTER_SYSVAR as isize;
                    }
                    _ => {
                        match (*variable).varreturningtype {
                            VarReturningType::VAR_RETURNING_DEFAULT => {
                                scratch.opcode = EEOP_SCAN_SYSVAR as isize;
                            }
                            VarReturningType::VAR_RETURNING_OLD => {
                                scratch.opcode = EEOP_OLD_SYSVAR as isize;
                                (*state).flags |= EEO_FLAG_HAS_OLD;
                            }
                            VarReturningType::VAR_RETURNING_NEW => {
                                scratch.opcode = EEOP_NEW_SYSVAR as isize;
                                (*state).flags |= EEO_FLAG_HAS_NEW;
                            }
                        }
                    }
                }
            } else {
                /* regular user column */
                scratch.d.var.attnum = ((*variable).varattno - 1) as c_int;
                scratch.d.var.vartype = (*variable).vartype;
                scratch.d.var.varreturningtype = (*variable).varreturningtype;
                match (*variable).varno {
                    crate::nodes::primnodes::INNER_VAR => {
                        scratch.opcode = EEOP_INNER_VAR as isize;
                    }
                    crate::nodes::primnodes::OUTER_VAR => {
                        scratch.opcode = EEOP_OUTER_VAR as isize;
                    }
                    _ => {
                        match (*variable).varreturningtype {
                            VarReturningType::VAR_RETURNING_DEFAULT => {
                                scratch.opcode = EEOP_SCAN_VAR as isize;
                            }
                            VarReturningType::VAR_RETURNING_OLD => {
                                scratch.opcode = EEOP_OLD_VAR as isize;
                                (*state).flags |= EEO_FLAG_HAS_OLD;
                            }
                            VarReturningType::VAR_RETURNING_NEW => {
                                scratch.opcode = EEOP_NEW_VAR as isize;
                                (*state).flags |= EEO_FLAG_HAS_NEW;
                            }
                        }
                    }
                }
            }

            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_Const => {
            let con = node as *mut crate::nodes::primnodes::Const;

            scratch.opcode = EEOP_CONST as isize;
            scratch.d.constval.value = (*con).constvalue;
            scratch.d.constval.isnull = (*con).constisnull;

            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_Param => {
            let param = node as *mut Param;
            let params: crate::nodes::params::ParamListInfo;

            match (*param).paramkind {
                crate::nodes::primnodes::PARAM_EXEC => {
                    scratch.opcode = EEOP_PARAM_EXEC as isize;
                    scratch.d.param.paramid = (*param).paramid;
                    scratch.d.param.paramtype = (*param).paramtype;
                    ExprEvalPushStep(state, &scratch);
                }
                crate::nodes::primnodes::PARAM_EXTERN => {
                    /*
                     * If we have a relevant ParamCompileHook, use it;
                     * otherwise compile a standard EEOP_PARAM_EXTERN step.
                     * ext_params, if supplied, takes precedence over info
                     * from the parent node's EState (if any).
                     */
                    if !(*state).ext_params.is_null() {
                        params = (*state).ext_params;
                    } else if !(*state).parent.is_null()
                        && !(*(*state).parent).state.is_null()
                    {
                        params = (*(*(*state).parent).state).es_param_list_info;
                    } else {
                        params = core::ptr::null_mut();
                    }
                    if !params.is_null() && (*params).paramCompile.is_some() {
                        ((*params).paramCompile.unwrap())(params, param, state as *mut _, resv, resnull);
                    } else {
                        scratch.opcode = EEOP_PARAM_EXTERN as isize;
                        scratch.d.param.paramid = (*param).paramid;
                        scratch.d.param.paramtype = (*param).paramtype;
                        ExprEvalPushStep(state, &scratch);
                    }
                }
                _ => {
                    elog!(
                        ERROR,
                        "unrecognized paramkind: {}",
                        (*param).paramkind as c_int
                    );
                }
            }
        }

        NodeTag::T_Aggref => {
            let aggref = node as *mut Aggref;

            scratch.opcode = EEOP_AGGREF as isize;
            scratch.d.aggref.aggno = (*aggref).aggno;

            if !(*state).parent.is_null()
                && IsA!((*state).parent, T_AggState)
            {
                let aggstate = (*state).parent as *mut AggState;
                (*aggstate).aggs = lappend(
                    (*aggstate).aggs,
                    aggref as *mut c_void,
                );
            } else {
                /* planner messed up */
                elog!(ERROR, "Aggref found in non-Agg plan node");
            }

            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_GroupingFunc => {
            let grp_node = node as *mut GroupingFunc;
            let agg: *mut Agg;

            if (*state).parent.is_null()
                || !IsA!((*state).parent, T_AggState)
                || !IsA!((*(*state).parent).plan, T_Agg)
            {
                elog!(ERROR, "GroupingFunc found in non-Agg plan node");
            }

            scratch.opcode = EEOP_GROUPING_FUNC as isize;

            agg = (*(*state).parent).plan as *mut Agg;

            if !(*agg).groupingSets.is_null() {
                scratch.d.grouping_func.clauses = (*grp_node).cols;
            } else {
                scratch.d.grouping_func.clauses = NIL;
            }

            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_WindowFunc => {
            let wfunc = node as *mut WindowFunc;
            let wfstate: *mut WindowFuncExprState =
                makeNode!(WindowFuncExprState, T_WindowFuncExprState);

            (*wfstate).wfunc = wfunc;

            if !(*state).parent.is_null()
                && IsA!((*state).parent, T_WindowAggState)
            {
                let winstate = (*state).parent as *mut WindowAggState;
                let nfuncs: c_int;

                (*winstate).funcs = lappend(
                    (*winstate).funcs,
                    wfstate as *mut c_void,
                );
                (*winstate).numfuncs += 1;
                nfuncs = (*winstate).numfuncs;
                if (*wfunc).winagg {
                    (*winstate).numaggs += 1;
                }

                /* for now initialize agg using old style expressions */
                (*wfstate).args =
                    ExecInitExprList((*wfunc).args, (*state).parent);
                (*wfstate).aggfilter =
                    ExecInitExpr((*wfunc).aggfilter, (*state).parent);

                /*
                 * Complain if the windowfunc's arguments contain any
                 * windowfuncs; nested window functions are semantically
                 * nonsensical.
                 */
                if nfuncs != (*winstate).numfuncs {
                    ereport!(
                        ERROR,
                        errmsg!("window function calls cannot be nested")
                    );
                }
            } else {
                /* planner messed up */
                elog!(ERROR, "WindowFunc found in non-WindowAgg plan node");
            }

            scratch.opcode = EEOP_WINDOW_FUNC as isize;
            scratch.d.window_func.wfstate = wfstate;
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_MergeSupportFunc => {
            /* must be in a MERGE, else something messed up */
            use crate::nodes::nodes::CmdType;
            if (*state).parent.is_null()
                || !IsA!((*state).parent, T_ModifyTableState)
                || (*((*state).parent as *mut crate::nodes::execnodes::ModifyTableState))
                    .operation != CmdType::CMD_MERGE
            {
                elog!(ERROR, "MergeSupportFunc found in non-merge plan node");
            }

            scratch.opcode = EEOP_MERGE_SUPPORT_FUNC as isize;
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_SubscriptingRef => {
            let sbsref = node as *mut SubscriptingRef;
            ExecInitSubscriptingRef(&mut scratch, sbsref, state, resv, resnull);
        }

        NodeTag::T_FuncExpr => {
            let func = node as *mut FuncExpr;
            ExecInitFunc(
                &mut scratch,
                node,
                (*func).args,
                (*func).funcid,
                (*func).inputcollid,
                state,
            );
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_OpExpr => {
            let op = node as *mut OpExpr;
            ExecInitFunc(
                &mut scratch,
                node,
                (*op).args,
                (*op).opfuncid,
                (*op).inputcollid,
                state,
            );
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_DistinctExpr => {
            let op = node as *mut crate::nodes::primnodes::DistinctExpr;
            ExecInitFunc(
                &mut scratch,
                node,
                (*op).args,
                (*op).opfuncid,
                (*op).inputcollid,
                state,
            );
            /*
             * Change opcode of call instruction to EEOP_DISTINCT.
             *
             * XXX: historically we've not called the function usage
             * pgstat infrastructure - that seems inconsistent.
             */
            scratch.opcode = EEOP_DISTINCT as isize;
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_NullIfExpr => {
            let op = node as *mut NullIfExpr;
            ExecInitFunc(
                &mut scratch,
                node,
                (*op).args,
                (*op).opfuncid,
                (*op).inputcollid,
                state,
            );

            /*
             * If first argument is of varlena type, we'll need to ensure
             * that the value passed to the comparison function is a
             * read-only pointer.
             */
            scratch.d.func.make_ro = get_typlen(exprType(
                linitial((*op).args) as *const Node,
            )) == -1;

            /*
             * Change opcode of call instruction to EEOP_NULLIF.
             */
            scratch.opcode = EEOP_NULLIF as isize;
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_ScalarArrayOpExpr => {
            let opexpr = node as *mut ScalarArrayOpExpr;
            let scalararg: *mut crate::nodes::primnodes::Expr;
            let arrayarg: *mut crate::nodes::primnodes::Expr;
            let finfo: *mut FmgrInfo;
            let fcinfo: FunctionCallInfo;
            let mut aclresult: AclResult;
            let cmpfuncid: Oid;

            /*
             * Select the correct comparison function.  When we do hashed
             * NOT IN clauses, the opfuncid will be the inequality comparison
             * function and negfuncid will be set to equality.
             */
            if OidIsValid((*opexpr).negfuncid) {
                debug_assert!(OidIsValid((*opexpr).hashfuncid));
                cmpfuncid = (*opexpr).negfuncid;
            } else {
                cmpfuncid = (*opexpr).opfuncid;
            }

            debug_assert!(crate::nodes::pg_list::list_length((*opexpr).args) == 2);
            scalararg = linitial((*opexpr).args) as *mut _;
            arrayarg = crate::nodes::pg_list::lsecond((*opexpr).args) as *mut _;

            /* Check permission to call function */
            aclresult = object_aclcheck(
                ProcedureRelationId,
                cmpfuncid,
                GetUserId(),
                0, /* ACL_EXECUTE */
            );
            if aclresult != ACLCHECK_OK {
                aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(cmpfuncid));
            }
            InvokeFunctionExecuteHook(cmpfuncid);

            if OidIsValid((*opexpr).hashfuncid) {
                aclresult = object_aclcheck(
                    ProcedureRelationId,
                    (*opexpr).hashfuncid,
                    GetUserId(),
                    0,
                );
                if aclresult != ACLCHECK_OK {
                    aclcheck_error(
                        aclresult,
                        OBJECT_FUNCTION,
                        get_func_name((*opexpr).hashfuncid),
                    );
                }
                InvokeFunctionExecuteHook((*opexpr).hashfuncid);
            }

            /* Set up the primary fmgr lookup information */
            finfo = palloc0(core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
            fcinfo = palloc0(SizeForFunctionCallInfo(2)) as FunctionCallInfo;
            fmgr_info(cmpfuncid, finfo);
            fmgr_info_set_expr(node as *mut Node, finfo);
            InitFunctionCallInfoData(
                fcinfo,
                finfo,
                2,
                (*opexpr).inputcollid,
                core::ptr::null_mut(),
                core::ptr::null_mut(),
            );

            /*
             * If hashfuncid is set, we create a EEOP_HASHED_SCALARARRAYOP
             * step instead of a EEOP_SCALARARRAYOP.
             */
            if OidIsValid((*opexpr).hashfuncid) {
                /* Evaluate scalar directly into left function argument */
                ExecInitExprRec(
                    scalararg,
                    state,
                    &mut (*(*fcinfo).args.as_mut_ptr().add(0)).value,
                    &mut (*(*fcinfo).args.as_mut_ptr().add(0)).isnull,
                );
                /*
                 * Evaluate array argument into our return value.
                 */
                ExecInitExprRec(arrayarg, state, resv, resnull);

                /* And perform the operation */
                scratch.opcode = EEOP_HASHED_SCALARARRAYOP as isize;
                scratch.d.hashedscalararrayop.inclause = (*opexpr).useOr;
                scratch.d.hashedscalararrayop.finfo = finfo;
                scratch.d.hashedscalararrayop.fcinfo_data = fcinfo;
                scratch.d.hashedscalararrayop.saop = opexpr;
                ExprEvalPushStep(state, &scratch);
            } else {
                /* Evaluate scalar directly into left function argument */
                ExecInitExprRec(
                    scalararg,
                    state,
                    &mut (*(*fcinfo).args.as_mut_ptr().add(0)).value,
                    &mut (*(*fcinfo).args.as_mut_ptr().add(0)).isnull,
                );
                /*
                 * Evaluate array argument into our return value.
                 */
                ExecInitExprRec(arrayarg, state, resv, resnull);

                /* And perform the operation */
                scratch.opcode = EEOP_SCALARARRAYOP as isize;
                scratch.d.scalararrayop.element_type = InvalidOid;
                scratch.d.scalararrayop.useOr = (*opexpr).useOr;
                scratch.d.scalararrayop.finfo = finfo;
                scratch.d.scalararrayop.fcinfo_data = fcinfo;
                scratch.d.scalararrayop.fn_addr = (*finfo).fn_addr.unwrap();
                ExprEvalPushStep(state, &scratch);
            }
        }

        NodeTag::T_BoolExpr => {
            let boolexpr = node as *mut BoolExpr;
            let nargs = crate::nodes::pg_list::list_length((*boolexpr).args);
            let mut adjust_jumps: *mut List = NIL;
            let mut off: c_int = 0;

            /* allocate scratch memory used by all steps of AND/OR */
            if (*boolexpr).boolop != crate::nodes::primnodes::NOT_EXPR {
                scratch.d.boolexpr.anynull =
                    palloc(core::mem::size_of::<bool>()) as *mut bool;
            }

            /*
             * For each argument evaluate the argument itself, then
             * perform the bool operation's appropriate handling.
             */
            let mut lc = crate::nodes::pg_list::list_head((*boolexpr).args);
            while !lc.is_null() {
                let arg = *(lc as *mut *mut crate::nodes::primnodes::Expr);

                /* Evaluate argument into our output variable */
                ExecInitExprRec(arg, state, resv, resnull);

                /* Perform the appropriate step type */
                match (*boolexpr).boolop {
                    crate::nodes::primnodes::AND_EXPR => {
                        debug_assert!(nargs >= 2);
                        if off == 0 {
                            scratch.opcode = EEOP_BOOL_AND_STEP_FIRST as isize;
                        } else if off + 1 == nargs {
                            scratch.opcode = EEOP_BOOL_AND_STEP_LAST as isize;
                        } else {
                            scratch.opcode = EEOP_BOOL_AND_STEP as isize;
                        }
                    }
                    crate::nodes::primnodes::OR_EXPR => {
                        debug_assert!(nargs >= 2);
                        if off == 0 {
                            scratch.opcode = EEOP_BOOL_OR_STEP_FIRST as isize;
                        } else if off + 1 == nargs {
                            scratch.opcode = EEOP_BOOL_OR_STEP_LAST as isize;
                        } else {
                            scratch.opcode = EEOP_BOOL_OR_STEP as isize;
                        }
                    }
                    crate::nodes::primnodes::NOT_EXPR => {
                        debug_assert!(nargs == 1);
                        scratch.opcode = EEOP_BOOL_NOT_STEP as isize;
                    }
                    _ => {
                        elog!(
                            ERROR,
                            "unrecognized boolop: {}",
                            (*boolexpr).boolop as c_int
                        );
                    }
                }

                scratch.d.boolexpr.jumpdone = -1;
                ExprEvalPushStep(state, &scratch);
                adjust_jumps =
                    lappend_int(adjust_jumps, (*state).steps_len - 1);
                off += 1;

                lc = crate::nodes::pg_list::lnext((*boolexpr).args, lc);
            }

            /* adjust jump targets */
            let mut lc2 = crate::nodes::pg_list::list_head(adjust_jumps);
            while !lc2.is_null() {
                let j = crate::nodes::pg_list::lfirst_int(lc2);
                let as_ = &mut *(*state).steps.add(j as usize);
                debug_assert!(as_.d.boolexpr.jumpdone == -1);
                as_.d.boolexpr.jumpdone = (*state).steps_len;
                lc2 = crate::nodes::pg_list::lnext(adjust_jumps, lc2);
            }
        }

        NodeTag::T_SubPlan => {
            let subplan = node as *mut SubPlan;

            /*
             * Real execution of a MULTIEXPR SubPlan has already been
             * done. What we have to do here is return a dummy NULL record
             * value in case this targetlist element is assigned someplace.
             */
            if (*subplan).subLinkType == MULTIEXPR_SUBLINK {
                scratch.opcode = EEOP_CONST as isize;
                scratch.d.constval.value = Datum::from(0usize);
                scratch.d.constval.isnull = true;
                ExprEvalPushStep(state, &scratch);
            } else {
                ExecInitSubPlanExpr(subplan, state, resv, resnull);
            }
        }

        NodeTag::T_FieldSelect => {
            let fselect = node as *mut FieldSelect;

            /* evaluate row/record argument into result area */
            ExecInitExprRec((*fselect).arg, state, resv, resnull);

            /* and extract field */
            scratch.opcode = EEOP_FIELDSELECT as isize;
            scratch.d.fieldselect.fieldnum = (*fselect).fieldnum;
            scratch.d.fieldselect.resulttype = (*fselect).resulttype;
            scratch.d.fieldselect.rowcache.cacheptr = core::ptr::null_mut();

            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_FieldStore => {
            let fstore = node as *mut FieldStore;
            let tupDesc: TupleDesc;
            let rowcachep: *mut ExprEvalRowtypeCache;
            let values: *mut Datum;
            let nulls: *mut bool;
            let ncolumns: c_int;

            /* find out the number of columns in the composite type */
            tupDesc = lookup_rowtype_tupdesc((*fstore).resulttype, -1);
            ncolumns = (*tupDesc).natts;
            ReleaseTupleDesc(tupDesc);

            /* create workspace for column values */
            values = palloc(core::mem::size_of::<Datum>() * ncolumns as usize) as *mut Datum;
            nulls = palloc(core::mem::size_of::<bool>() * ncolumns as usize) as *mut bool;

            /* create shared composite-type-lookup cache struct */
            rowcachep = palloc(core::mem::size_of::<ExprEvalRowtypeCache>())
                as *mut ExprEvalRowtypeCache;
            (*rowcachep).cacheptr = core::ptr::null_mut();

            /* emit code to evaluate the composite input value */
            ExecInitExprRec((*fstore).arg, state, resv, resnull);

            /* next, deform the input tuple into our workspace */
            scratch.opcode = EEOP_FIELDSTORE_DEFORM as isize;
            scratch.d.fieldstore.fstore = fstore;
            scratch.d.fieldstore.rowcache = rowcachep;
            scratch.d.fieldstore.values = values;
            scratch.d.fieldstore.nulls = nulls;
            scratch.d.fieldstore.ncolumns = ncolumns;
            ExprEvalPushStep(state, &scratch);

            /* evaluate new field values, store in workspace columns */
            let mut l1 = crate::nodes::pg_list::list_head((*fstore).newvals);
            let mut l2 = crate::nodes::pg_list::list_head((*fstore).fieldnums);
            while !l1.is_null() {
                let e = *(l1 as *mut *mut crate::nodes::primnodes::Expr);
                let fieldnum = crate::nodes::pg_list::lfirst_int(l2);
                let save_innermost_caseval: *mut Datum;
                let save_innermost_casenull: *mut bool;

                if fieldnum <= 0 || fieldnum > ncolumns {
                    elog!(
                        ERROR,
                        "field number {} is out of range in FieldStore",
                        fieldnum
                    );
                }

                /*
                 * Use the CaseTestExpr mechanism to pass down the old
                 * value of the field being replaced.
                 */
                save_innermost_caseval = (*state).innermost_caseval;
                save_innermost_casenull = (*state).innermost_casenull;
                (*state).innermost_caseval = values.add((fieldnum - 1) as usize);
                (*state).innermost_casenull = nulls.add((fieldnum - 1) as usize);

                ExecInitExprRec(
                    e,
                    state,
                    values.add((fieldnum - 1) as usize),
                    nulls.add((fieldnum - 1) as usize),
                );

                (*state).innermost_caseval = save_innermost_caseval;
                (*state).innermost_casenull = save_innermost_casenull;

                l1 = crate::nodes::pg_list::lnext((*fstore).newvals, l1);
                l2 = crate::nodes::pg_list::lnext((*fstore).fieldnums, l2);
            }

            /* finally, form result tuple */
            scratch.opcode = EEOP_FIELDSTORE_FORM as isize;
            scratch.d.fieldstore.fstore = fstore;
            scratch.d.fieldstore.rowcache = rowcachep;
            scratch.d.fieldstore.values = values;
            scratch.d.fieldstore.nulls = nulls;
            scratch.d.fieldstore.ncolumns = ncolumns;
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_RelabelType => {
            /* relabel doesn't need to do anything at runtime */
            let relabel = node as *mut crate::nodes::primnodes::RelabelType;
            ExecInitExprRec((*relabel).arg, state, resv, resnull);
        }

        NodeTag::T_CoerceViaIO => {
            let iocoerce = node as *mut CoerceViaIO;
            let mut iofunc: Oid = InvalidOid;
            let mut typisvarlena = false;
            let mut typioparam: Oid = InvalidOid;
            let fcinfo_in: FunctionCallInfo;

            /* evaluate argument into step's result area */
            ExecInitExprRec((*iocoerce).arg, state, resv, resnull);

            /*
             * Prepare both output and input function calls, to be
             * evaluated inside a single evaluation step for speed.
             */
            if (*state).escontext.is_null() {
                scratch.opcode = EEOP_IOCOERCE as isize;
            } else {
                scratch.opcode = EEOP_IOCOERCE_SAFE as isize;
            }

            /* lookup the source type's output function */
            scratch.d.iocoerce.finfo_out =
                palloc0(core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
            scratch.d.iocoerce.fcinfo_data_out =
                palloc0(SizeForFunctionCallInfo(1)) as FunctionCallInfo;

            getTypeOutputInfo(
                exprType((*iocoerce).arg as *const Node),
                &mut iofunc,
                &mut typisvarlena,
            );
            fmgr_info(iofunc, scratch.d.iocoerce.finfo_out);
            fmgr_info_set_expr(node as *mut Node, scratch.d.iocoerce.finfo_out);
            InitFunctionCallInfoData(
                scratch.d.iocoerce.fcinfo_data_out,
                scratch.d.iocoerce.finfo_out,
                1,
                InvalidOid,
                core::ptr::null_mut(),
                core::ptr::null_mut(),
            );

            /* lookup the result type's input function */
            scratch.d.iocoerce.finfo_in =
                palloc0(core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
            scratch.d.iocoerce.fcinfo_data_in =
                palloc0(SizeForFunctionCallInfo(3)) as FunctionCallInfo;

            getTypeInputInfo((*iocoerce).resulttype, &mut iofunc, &mut typioparam);
            fmgr_info(iofunc, scratch.d.iocoerce.finfo_in);
            fmgr_info_set_expr(node as *mut Node, scratch.d.iocoerce.finfo_in);
            InitFunctionCallInfoData(
                scratch.d.iocoerce.fcinfo_data_in,
                scratch.d.iocoerce.finfo_in,
                3,
                InvalidOid,
                core::ptr::null_mut(),
                core::ptr::null_mut(),
            );

            /*
             * We can preload the second and third arguments for the input
             * function, since they're constants.
             */
            fcinfo_in = scratch.d.iocoerce.fcinfo_data_in;
            (*(*fcinfo_in).args.as_mut_ptr().add(1)).value = ObjectIdGetDatum(typioparam);
            (*(*fcinfo_in).args.as_mut_ptr().add(1)).isnull = false;
            (*(*fcinfo_in).args.as_mut_ptr().add(2)).value = Int32GetDatum(-1);
            (*(*fcinfo_in).args.as_mut_ptr().add(2)).isnull = false;

            (*fcinfo_in).context = (*state).escontext as *mut Node;

            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_ArrayCoerceExpr => {
            let acoerce = node as *mut crate::nodes::primnodes::ArrayCoerceExpr;
            let resultelemtype: Oid;
            let elemstate: *mut ExprState;

            /* evaluate argument into step's result area */
            ExecInitExprRec((*acoerce).arg, state, resv, resnull);

            resultelemtype = get_element_type((*acoerce).resulttype);
            if !OidIsValid(resultelemtype) {
                ereport!(
                    ERROR,
                    errmsg!("target type is not an array")
                );
            }

            /*
             * Construct a sub-expression for the per-element expression.
             */
            let elemstate_node: *mut ExprState =
                makeNode!(ExprState, T_ExprState);
            (*elemstate_node).expr = (*acoerce).elemexpr;
            (*elemstate_node).parent = (*state).parent;
            (*elemstate_node).ext_params = (*state).ext_params;

            (*elemstate_node).innermost_caseval =
                palloc(core::mem::size_of::<Datum>()) as *mut Datum;
            (*elemstate_node).innermost_casenull =
                palloc(core::mem::size_of::<bool>()) as *mut bool;

            ExecInitExprRec(
                (*acoerce).elemexpr,
                elemstate_node,
                &mut (*elemstate_node).resvalue,
                &mut (*elemstate_node).resnull,
            );

            let elemstate = if (*elemstate_node).steps_len == 1
                && (*(*elemstate_node).steps).opcode == EEOP_CASE_TESTVAL as isize
            {
                /* Trivial, so we need no per-element work at runtime */
                core::ptr::null_mut::<ExprState>()
            } else {
                /* Not trivial, so append a DONE step */
                let mut done_scratch: ExprEvalStep = core::mem::zeroed();
                done_scratch.opcode = EEOP_DONE_RETURN as isize;
                ExprEvalPushStep(elemstate_node, &done_scratch);
                /* and ready the subexpression */
                ExecReadyExpr(elemstate_node);
                elemstate_node
            };

            scratch.opcode = EEOP_ARRAYCOERCE as isize;
            scratch.d.arraycoerce.elemexprstate = elemstate;
            scratch.d.arraycoerce.resultelemtype = resultelemtype;

            if !elemstate.is_null() {
                /* Set up workspace for array_map */
                scratch.d.arraycoerce.amstate =
                    palloc0(core::mem::size_of::<ArrayMapState>()) as *mut ArrayMapState;
            } else {
                /* Don't need workspace if there's no subexpression */
                scratch.d.arraycoerce.amstate = core::ptr::null_mut();
            }

            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_ConvertRowtypeExpr => {
            let convert =
                node as *mut crate::nodes::primnodes::ConvertRowtypeExpr;
            let rowcachep: *mut ExprEvalRowtypeCache;

            /* cache structs must be out-of-line for space reasons */
            rowcachep = palloc(2 * core::mem::size_of::<ExprEvalRowtypeCache>())
                as *mut ExprEvalRowtypeCache;
            (*rowcachep).cacheptr = core::ptr::null_mut();
            (*rowcachep.add(1)).cacheptr = core::ptr::null_mut();

            /* evaluate argument into step's result area */
            ExecInitExprRec((*convert).arg, state, resv, resnull);

            /* and push conversion step */
            scratch.opcode = EEOP_CONVERT_ROWTYPE as isize;
            scratch.d.convert_rowtype.inputtype =
                exprType((*convert).arg as *const Node);
            scratch.d.convert_rowtype.outputtype = (*convert).resulttype;
            scratch.d.convert_rowtype.incache = rowcachep;
            scratch.d.convert_rowtype.outcache = rowcachep.add(1);
            scratch.d.convert_rowtype.map = core::ptr::null_mut();

            ExprEvalPushStep(state, &scratch);
        }

        /* note that CaseWhen expressions are handled within this block */
        NodeTag::T_CaseExpr => {
            let caseExpr = node as *mut CaseExpr;
            let mut adjust_jumps: *mut List = NIL;
            let mut caseval: *mut Datum = core::ptr::null_mut();
            let mut casenull: *mut bool = core::ptr::null_mut();

            /*
             * If there's a test expression, we have to evaluate it and
             * save the value where the CaseTestExpr placeholders can find it.
             */
            if !(*caseExpr).arg.is_null() {
                /* Evaluate testexpr into caseval/casenull workspace */
                caseval = palloc(core::mem::size_of::<Datum>()) as *mut Datum;
                casenull = palloc(core::mem::size_of::<bool>()) as *mut bool;

                ExecInitExprRec((*caseExpr).arg, state, caseval, casenull);

                /*
                 * Since value might be read multiple times, force to R/O
                 * - but only if it could be an expanded datum.
                 */
                if get_typlen(exprType((*caseExpr).arg as *const Node)) == -1 {
                    /* change caseval in-place */
                    scratch.opcode = EEOP_MAKE_READONLY as isize;
                    scratch.resvalue = caseval;
                    scratch.resnull = casenull;
                    scratch.d.make_readonly.value = caseval;
                    scratch.d.make_readonly.isnull = casenull;
                    ExprEvalPushStep(state, &scratch);
                    /* restore normal settings of scratch fields */
                    scratch.resvalue = resv;
                    scratch.resnull = resnull;
                }
            }

            /*
             * Prepare to evaluate each of the WHEN clauses in turn.
             */
            let mut lc = crate::nodes::pg_list::list_head((*caseExpr).args);
            while !lc.is_null() {
                let when = *(lc as *mut *mut CaseWhen);
                let save_innermost_caseval: *mut Datum;
                let save_innermost_casenull: *mut bool;
                let whenstep: c_int;

                /*
                 * Make testexpr result available to CaseTestExpr nodes
                 * within the condition.
                 */
                save_innermost_caseval = (*state).innermost_caseval;
                save_innermost_casenull = (*state).innermost_casenull;
                (*state).innermost_caseval = caseval;
                (*state).innermost_casenull = casenull;

                /* evaluate condition into CASE's result variables */
                ExecInitExprRec((*when).expr, state, resv, resnull);

                (*state).innermost_caseval = save_innermost_caseval;
                (*state).innermost_casenull = save_innermost_casenull;

                /* If WHEN result isn't true, jump to next CASE arm */
                scratch.opcode = EEOP_JUMP_IF_NOT_TRUE as isize;
                scratch.d.jump.jumpdone = -1; /* computed later */
                ExprEvalPushStep(state, &scratch);
                whenstep = (*state).steps_len - 1;

                /*
                 * If WHEN result is true, evaluate THEN result.
                 */
                ExecInitExprRec((*when).result, state, resv, resnull);

                /* Emit JUMP step to jump to end of CASE's code */
                scratch.opcode = EEOP_JUMP as isize;
                scratch.d.jump.jumpdone = -1; /* computed later */
                ExprEvalPushStep(state, &scratch);

                adjust_jumps =
                    lappend_int(adjust_jumps, (*state).steps_len - 1);

                /*
                 * Set WHEN test's jump target now.
                 */
                (*(*state).steps.add(whenstep as usize)).d.jump.jumpdone =
                    (*state).steps_len;

                lc = crate::nodes::pg_list::lnext((*caseExpr).args, lc);
            }

            /* transformCaseExpr always adds a default */
            debug_assert!(!(*caseExpr).defresult.is_null());

            /* evaluate ELSE expr into CASE's result variables */
            ExecInitExprRec((*caseExpr).defresult, state, resv, resnull);

            /* adjust jump targets */
            let mut lc2 = crate::nodes::pg_list::list_head(adjust_jumps);
            while !lc2.is_null() {
                let j = crate::nodes::pg_list::lfirst_int(lc2);
                let as_ = &mut *(*state).steps.add(j as usize);
                debug_assert!(as_.opcode == EEOP_JUMP as isize);
                debug_assert!(as_.d.jump.jumpdone == -1);
                as_.d.jump.jumpdone = (*state).steps_len;
                lc2 = crate::nodes::pg_list::lnext(adjust_jumps, lc2);
            }
        }

        NodeTag::T_CaseTestExpr => {
            /*
             * Read from location identified by innermost_caseval.
             */
            if (*state).innermost_caseval.is_null() {
                scratch.opcode = EEOP_CASE_TESTVAL_EXT as isize;
            } else {
                scratch.opcode = EEOP_CASE_TESTVAL as isize;
                scratch.d.casetest.value = (*state).innermost_caseval;
                scratch.d.casetest.isnull = (*state).innermost_casenull;
            }
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_ArrayExpr => {
            let arrayexpr = node as *mut crate::nodes::primnodes::ArrayExpr;
            let nelems = crate::nodes::pg_list::list_length((*arrayexpr).elements);
            let mut elemoff: c_int = 0;

            /*
             * Evaluate by computing each element, and then forming the array.
             */
            scratch.opcode = EEOP_ARRAYEXPR as isize;
            scratch.d.arrayexpr.elemvalues = palloc(
                core::mem::size_of::<Datum>() * nelems as usize,
            ) as *mut Datum;
            scratch.d.arrayexpr.elemnulls = palloc(
                core::mem::size_of::<bool>() * nelems as usize,
            ) as *mut bool;
            scratch.d.arrayexpr.nelems = nelems;

            /* fill remaining fields of step */
            scratch.d.arrayexpr.multidims = (*arrayexpr).multidims;
            scratch.d.arrayexpr.elemtype = (*arrayexpr).element_typeid;

            /* do one-time catalog lookup for type info */
            get_typlenbyvalalign(
                (*arrayexpr).element_typeid,
                &mut scratch.d.arrayexpr.elemlength,
                &mut scratch.d.arrayexpr.elembyval,
                &mut scratch.d.arrayexpr.elemalign,
            );

            /* prepare to evaluate all arguments */
            let mut lc = crate::nodes::pg_list::list_head((*arrayexpr).elements);
            while !lc.is_null() {
                let e = *(lc as *mut *mut crate::nodes::primnodes::Expr);

                ExecInitExprRec(
                    e,
                    state,
                    scratch.d.arrayexpr.elemvalues.add(elemoff as usize),
                    scratch.d.arrayexpr.elemnulls.add(elemoff as usize),
                );
                elemoff += 1;

                lc = crate::nodes::pg_list::lnext((*arrayexpr).elements, lc);
            }

            /* and then collect all into an array */
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_RowExpr => {
            let rowexpr = node as *mut RowExpr;
            let mut nelems = crate::nodes::pg_list::list_length((*rowexpr).args);
            let tupdesc: TupleDesc;
            let mut i: c_int = 0;

            /* Build tupdesc to describe result tuples */
            if (*rowexpr).row_typeid == RECORDOID {
                /* generic record, use types of given expressions */
                tupdesc = ExecTypeFromExprList((*rowexpr).args);
                /* ... but adopt RowExpr's column aliases */
                ExecTypeSetColNames(tupdesc, (*rowexpr).colnames);
                /* Bless the tupdesc so it can be looked up later */
                BlessTupleDesc(tupdesc);
            } else {
                /* it's been cast to a named type, use that */
                tupdesc = lookup_rowtype_tupdesc_copy((*rowexpr).row_typeid, -1);
            }

            /*
             * In the named-type case, the tupdesc could have more columns
             * than are in the args list.
             */
            debug_assert!(nelems <= (*tupdesc).natts);
            nelems = if nelems > (*tupdesc).natts {
                nelems
            } else {
                (*tupdesc).natts
            };

            /*
             * Evaluate by first building datums for each field, and then
             * a final step forming the composite datum.
             */
            scratch.opcode = EEOP_ROW as isize;
            scratch.d.row.tupdesc = tupdesc;

            /* space for the individual field datums */
            scratch.d.row.elemvalues = palloc(
                core::mem::size_of::<Datum>() * nelems as usize,
            ) as *mut Datum;
            scratch.d.row.elemnulls = palloc(
                core::mem::size_of::<bool>() * nelems as usize,
            ) as *mut bool;
            /* make sure any extra columns are null */
            core::ptr::write_bytes(
                scratch.d.row.elemnulls,
                1u8, /* true */
                nelems as usize,
            );

            /* Set up evaluation, skipping any deleted columns */
            let mut l = crate::nodes::pg_list::list_head((*rowexpr).args);
            while !l.is_null() {
                let att = TupleDescAttr(tupdesc, i);
                let mut e = *(l as *mut *mut crate::nodes::primnodes::Expr);

                if !(*att).attisdropped {
                    /*
                     * Guard against ALTER COLUMN TYPE on rowtype.
                     */
                    if exprType(e as *const Node) != (*att).atttypid {
                        ereport!(
                            ERROR,
                            errmsg!("ROW() column has type mismatch")
                        );
                    }
                } else {
                    /*
                     * Ignore original expression and insert a NULL.
                     */
                    e = makeNullConst(INT4OID, -1, InvalidOid) as *mut _;
                }

                /* Evaluate column expr into appropriate workspace slot */
                ExecInitExprRec(
                    e,
                    state,
                    scratch.d.row.elemvalues.add(i as usize),
                    scratch.d.row.elemnulls.add(i as usize),
                );
                i += 1;

                l = crate::nodes::pg_list::lnext((*rowexpr).args, l);
            }

            /* And finally build the row value */
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_RowCompareExpr => {
            let rcexpr = node as *mut RowCompareExpr;
            let nopers = crate::nodes::pg_list::list_length((*rcexpr).opnos);
            let mut adjust_jumps: *mut List = NIL;

            debug_assert!(crate::nodes::pg_list::list_length((*rcexpr).largs) == nopers);
            debug_assert!(crate::nodes::pg_list::list_length((*rcexpr).rargs) == nopers);
            debug_assert!(crate::nodes::pg_list::list_length((*rcexpr).opfamilies) == nopers);
            debug_assert!(crate::nodes::pg_list::list_length((*rcexpr).inputcollids) == nopers);

            let mut l_left_expr = crate::nodes::pg_list::list_head((*rcexpr).largs);
            let mut l_right_expr = crate::nodes::pg_list::list_head((*rcexpr).rargs);
            let mut l_opno = crate::nodes::pg_list::list_head((*rcexpr).opnos);
            let mut l_opfamily = crate::nodes::pg_list::list_head((*rcexpr).opfamilies);
            let mut l_inputcollid = crate::nodes::pg_list::list_head((*rcexpr).inputcollids);

            while !l_left_expr.is_null() {
                let left_expr = *(l_left_expr as *mut *mut crate::nodes::primnodes::Expr);
                let right_expr = *(l_right_expr as *mut *mut crate::nodes::primnodes::Expr);
                let opno = crate::nodes::pg_list::lfirst_oid(l_opno);
                let opfamily = crate::nodes::pg_list::lfirst_oid(l_opfamily);
                let inputcollid = crate::nodes::pg_list::lfirst_oid(l_inputcollid);
                let mut strategy: c_int = 0;
                let mut lefttype: Oid = InvalidOid;
                let mut righttype: Oid = InvalidOid;
                let proc: Oid;
                let finfo: *mut FmgrInfo;
                let fcinfo: FunctionCallInfo;

                get_op_opfamily_properties(
                    opno, opfamily, false, &mut strategy, &mut lefttype, &mut righttype,
                );
                proc = get_opfamily_proc(opfamily, lefttype, righttype, BTORDER_PROC);
                if !OidIsValid(proc) {
                    elog!(
                        ERROR,
                        "missing support function {}({},{}) in opfamily {}",
                        BTORDER_PROC, lefttype, righttype, opfamily
                    );
                }

                /* Set up the primary fmgr lookup information */
                finfo = palloc0(core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
                fcinfo = palloc0(SizeForFunctionCallInfo(2)) as FunctionCallInfo;
                fmgr_info(proc, finfo);
                fmgr_info_set_expr(node as *mut Node, finfo);
                InitFunctionCallInfoData(
                    fcinfo, finfo, 2, inputcollid,
                    core::ptr::null_mut(), core::ptr::null_mut(),
                );

                /* evaluate left and right args directly into fcinfo */
                ExecInitExprRec(
                    left_expr, state,
                    &mut (*(*fcinfo).args.as_mut_ptr().add(0)).value, &mut (*(*fcinfo).args.as_mut_ptr().add(0)).isnull,
                );
                ExecInitExprRec(
                    right_expr, state,
                    &mut (*(*fcinfo).args.as_mut_ptr().add(1)).value, &mut (*(*fcinfo).args.as_mut_ptr().add(1)).isnull,
                );

                scratch.opcode = EEOP_ROWCOMPARE_STEP as isize;
                scratch.d.rowcompare_step.finfo = finfo;
                scratch.d.rowcompare_step.fcinfo_data = fcinfo;
                scratch.d.rowcompare_step.fn_addr = (*finfo).fn_addr.unwrap();
                scratch.d.rowcompare_step.jumpnull = -1;
                scratch.d.rowcompare_step.jumpdone = -1;

                ExprEvalPushStep(state, &scratch);
                adjust_jumps = lappend_int(adjust_jumps, (*state).steps_len - 1);

                l_left_expr = crate::nodes::pg_list::lnext((*rcexpr).largs, l_left_expr);
                l_right_expr = crate::nodes::pg_list::lnext((*rcexpr).rargs, l_right_expr);
                l_opno = crate::nodes::pg_list::lnext((*rcexpr).opnos, l_opno);
                l_opfamily = crate::nodes::pg_list::lnext((*rcexpr).opfamilies, l_opfamily);
                l_inputcollid = crate::nodes::pg_list::lnext((*rcexpr).inputcollids, l_inputcollid);
            }

            /* We could have a zero-column rowtype */
            if nopers == 0 {
                scratch.opcode = EEOP_CONST as isize;
                scratch.d.constval.value = Int32GetDatum(0);
                scratch.d.constval.isnull = false;
                ExprEvalPushStep(state, &scratch);
            }

            /* Finally, examine the last comparison result */
            scratch.opcode = EEOP_ROWCOMPARE_FINAL as isize;
            scratch.d.rowcompare_final.cmptype = (*rcexpr).cmptype;
            ExprEvalPushStep(state, &scratch);

            /* adjust jump targets */
            let mut lc = crate::nodes::pg_list::list_head(adjust_jumps);
            while !lc.is_null() {
                let j = crate::nodes::pg_list::lfirst_int(lc);
                let as_ = &mut *(*state).steps.add(j as usize);
                debug_assert!(as_.opcode == EEOP_ROWCOMPARE_STEP as isize);
                debug_assert!(as_.d.rowcompare_step.jumpdone == -1);
                debug_assert!(as_.d.rowcompare_step.jumpnull == -1);
                /* jump to comparison evaluation */
                as_.d.rowcompare_step.jumpdone = (*state).steps_len - 1;
                /* jump to the following expression */
                as_.d.rowcompare_step.jumpnull = (*state).steps_len;
                lc = crate::nodes::pg_list::lnext(adjust_jumps, lc);
            }
        }

        NodeTag::T_CoalesceExpr => {
            let coalesce = node as *mut CoalesceExpr;
            let mut adjust_jumps: *mut List = NIL;

            /* We assume there's at least one arg */
            debug_assert!(!(*coalesce).args.is_null());

            /*
             * Prepare evaluation of all coalesced arguments, after each
             * one push a step that short-circuits if not null.
             */
            let mut lc = crate::nodes::pg_list::list_head((*coalesce).args);
            while !lc.is_null() {
                let e = *(lc as *mut *mut crate::nodes::primnodes::Expr);

                /* evaluate argument, directly into result datum */
                ExecInitExprRec(e, state, resv, resnull);

                /* if it's not null, skip to end of COALESCE expr */
                scratch.opcode = EEOP_JUMP_IF_NOT_NULL as isize;
                scratch.d.jump.jumpdone = -1; /* adjust later */
                ExprEvalPushStep(state, &scratch);

                adjust_jumps = lappend_int(adjust_jumps, (*state).steps_len - 1);

                lc = crate::nodes::pg_list::lnext((*coalesce).args, lc);
            }

            /* adjust jump targets */
            let mut lc2 = crate::nodes::pg_list::list_head(adjust_jumps);
            while !lc2.is_null() {
                let j = crate::nodes::pg_list::lfirst_int(lc2);
                let as_ = &mut *(*state).steps.add(j as usize);
                debug_assert!(as_.opcode == EEOP_JUMP_IF_NOT_NULL as isize);
                debug_assert!(as_.d.jump.jumpdone == -1);
                as_.d.jump.jumpdone = (*state).steps_len;
                lc2 = crate::nodes::pg_list::lnext(adjust_jumps, lc2);
            }
        }

        NodeTag::T_MinMaxExpr => {
            let minmaxexpr = node as *mut MinMaxExpr;
            let nelems = crate::nodes::pg_list::list_length((*minmaxexpr).args);
            let typentry: *mut TypeCacheEntry;
            let finfo: *mut FmgrInfo;
            let fcinfo: FunctionCallInfo;
            let mut off: c_int = 0;

            /* Look up the btree comparison function for the datatype */
            typentry = lookup_type_cache(
                (*minmaxexpr).minmaxtype,
                TYPECACHE_CMP_PROC,
            );
            if !OidIsValid((*typentry).cmp_proc) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not identify a comparison function for type"
                    )
                );
            }

            /* Perform function lookup */
            finfo = palloc0(core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
            fcinfo = palloc0(SizeForFunctionCallInfo(2)) as FunctionCallInfo;
            fmgr_info((*typentry).cmp_proc, finfo);
            fmgr_info_set_expr(node as *mut Node, finfo);
            InitFunctionCallInfoData(
                fcinfo, finfo, 2, (*minmaxexpr).inputcollid,
                core::ptr::null_mut(), core::ptr::null_mut(),
            );

            scratch.opcode = EEOP_MINMAX as isize;
            /* allocate space to store arguments */
            scratch.d.minmax.values = palloc(
                core::mem::size_of::<Datum>() * nelems as usize,
            ) as *mut Datum;
            scratch.d.minmax.nulls = palloc(
                core::mem::size_of::<bool>() * nelems as usize,
            ) as *mut bool;
            scratch.d.minmax.nelems = nelems;
            scratch.d.minmax.op = (*minmaxexpr).op;
            scratch.d.minmax.finfo = finfo;
            scratch.d.minmax.fcinfo_data = fcinfo;

            /* evaluate expressions into minmax->values/nulls */
            let mut lc = crate::nodes::pg_list::list_head((*minmaxexpr).args);
            while !lc.is_null() {
                let e = *(lc as *mut *mut crate::nodes::primnodes::Expr);
                ExecInitExprRec(
                    e, state,
                    scratch.d.minmax.values.add(off as usize),
                    scratch.d.minmax.nulls.add(off as usize),
                );
                off += 1;
                lc = crate::nodes::pg_list::lnext((*minmaxexpr).args, lc);
            }

            /* and push the final comparison */
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_SQLValueFunction => {
            let svf = node as *mut SQLValueFunction;
            scratch.opcode = EEOP_SQLVALUEFUNCTION as isize;
            scratch.d.sqlvaluefunction.svf = svf;
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_XmlExpr => {
            let xexpr = node as *mut XmlExpr;
            let nnamed = crate::nodes::pg_list::list_length((*xexpr).named_args);
            let nargs = crate::nodes::pg_list::list_length((*xexpr).args);
            let mut off: c_int = 0;

            scratch.opcode = EEOP_XMLEXPR as isize;
            scratch.d.xmlexpr.xexpr = xexpr;

            /* allocate space for storing all the arguments */
            if nnamed > 0 {
                scratch.d.xmlexpr.named_argvalue = palloc(
                    core::mem::size_of::<Datum>() * nnamed as usize,
                ) as *mut Datum;
                scratch.d.xmlexpr.named_argnull = palloc(
                    core::mem::size_of::<bool>() * nnamed as usize,
                ) as *mut bool;
            } else {
                scratch.d.xmlexpr.named_argvalue = core::ptr::null_mut();
                scratch.d.xmlexpr.named_argnull = core::ptr::null_mut();
            }

            if nargs > 0 {
                scratch.d.xmlexpr.argvalue = palloc(
                    core::mem::size_of::<Datum>() * nargs as usize,
                ) as *mut Datum;
                scratch.d.xmlexpr.argnull = palloc(
                    core::mem::size_of::<bool>() * nargs as usize,
                ) as *mut bool;
            } else {
                scratch.d.xmlexpr.argvalue = core::ptr::null_mut();
                scratch.d.xmlexpr.argnull = core::ptr::null_mut();
            }

            /* prepare argument execution */
            off = 0;
            let mut arg = crate::nodes::pg_list::list_head((*xexpr).named_args);
            while !arg.is_null() {
                let e = *(arg as *mut *mut crate::nodes::primnodes::Expr);
                ExecInitExprRec(
                    e, state,
                    scratch.d.xmlexpr.named_argvalue.add(off as usize),
                    scratch.d.xmlexpr.named_argnull.add(off as usize),
                );
                off += 1;
                arg = crate::nodes::pg_list::lnext((*xexpr).named_args, arg);
            }

            off = 0;
            let mut arg2 = crate::nodes::pg_list::list_head((*xexpr).args);
            while !arg2.is_null() {
                let e = *(arg2 as *mut *mut crate::nodes::primnodes::Expr);
                ExecInitExprRec(
                    e, state,
                    scratch.d.xmlexpr.argvalue.add(off as usize),
                    scratch.d.xmlexpr.argnull.add(off as usize),
                );
                off += 1;
                arg2 = crate::nodes::pg_list::lnext((*xexpr).args, arg2);
            }

            /* and evaluate the actual XML expression */
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_JsonValueExpr => {
            let jve = node as *mut crate::nodes::primnodes::JsonValueExpr;

            debug_assert!(!(*jve).raw_expr.is_null());
            ExecInitExprRec((*jve).raw_expr, state, resv, resnull);
            debug_assert!(!(*jve).formatted_expr.is_null());
            ExecInitExprRec((*jve).formatted_expr, state, resv, resnull);
        }

        NodeTag::T_JsonConstructorExpr => {
            let ctor = node as *mut JsonConstructorExpr;
            let args = (*ctor).args;
            let nargs = crate::nodes::pg_list::list_length(args);
            let mut argno: c_int = 0;

            if !(*ctor).func.is_null() {
                ExecInitExprRec((*ctor).func, state, resv, resnull);
            } else if ((*ctor).r#type == crate::nodes::primnodes::JSCTOR_JSON_PARSE
                && !(*ctor).unique)
                || (*ctor).r#type == crate::nodes::primnodes::JSCTOR_JSON_SERIALIZE
            {
                /* Use the value of the first argument as result */
                ExecInitExprRec(linitial(args) as *mut _, state, resv, resnull);
            } else {
                let jcstate: *mut JsonConstructorExprState = palloc0(
                    core::mem::size_of::<JsonConstructorExprState>(),
                ) as *mut JsonConstructorExprState;

                scratch.opcode = EEOP_JSON_CONSTRUCTOR as isize;
                scratch.d.json_constructor.jcstate = jcstate;

                (*jcstate).constructor = ctor;
                (*jcstate).arg_values =
                    palloc(core::mem::size_of::<Datum>() * nargs as usize) as *mut Datum;
                (*jcstate).arg_nulls =
                    palloc(core::mem::size_of::<bool>() * nargs as usize) as *mut bool;
                (*jcstate).arg_types =
                    palloc(core::mem::size_of::<Oid>() * nargs as usize) as *mut Oid;
                (*jcstate).nargs = nargs;

                let mut lc = crate::nodes::pg_list::list_head(args);
                while !lc.is_null() {
                    let arg = *(lc as *mut *mut crate::nodes::primnodes::Expr);

                    *(*jcstate).arg_types.add(argno as usize) =
                        exprType(arg as *const Node);

                    if IsA!(arg, T_Const) {
                        /* Don't evaluate const arguments every round */
                        let con = arg as *mut crate::nodes::primnodes::Const;
                        *(*jcstate).arg_values.add(argno as usize) = (*con).constvalue;
                        *(*jcstate).arg_nulls.add(argno as usize) = (*con).constisnull;
                    } else {
                        ExecInitExprRec(
                            arg, state,
                            (*jcstate).arg_values.add(argno as usize),
                            (*jcstate).arg_nulls.add(argno as usize),
                        );
                    }
                    argno += 1;

                    lc = crate::nodes::pg_list::lnext(args, lc);
                }

                /* prepare type cache for datum_to_json[b]() */
                if (*ctor).r#type == crate::nodes::primnodes::JSCTOR_JSON_SCALAR {
                    let is_jsonb = !(*(*ctor).returning).format.is_null()
                        && (*(*(*ctor).returning).format).format_type
                            == crate::nodes::primnodes::JS_FORMAT_JSONB;

                    (*jcstate).arg_type_cache = palloc(
                        core::mem::size_of::<JsonConstructorExprState_arg_type_cache>()
                            * nargs as usize,
                    ) as *mut JsonConstructorExprState_arg_type_cache;

                    for i in 0..nargs {
                        let mut category: JsonTypeCategory = 0;
                        let mut outfuncid: Oid = InvalidOid;
                        let typid = *(*jcstate).arg_types.add(i as usize);

                        json_categorize_type(typid, is_jsonb, &mut category, &mut outfuncid);

                        (*(*jcstate).arg_type_cache.add(i as usize)).outfuncid = outfuncid;
                        (*(*jcstate).arg_type_cache.add(i as usize)).category =
                            category as c_int;
                    }
                }

                ExprEvalPushStep(state, &scratch);
            }

            if !(*ctor).coercion.is_null() {
                let innermost_caseval = (*state).innermost_caseval;
                let innermost_isnull = (*state).innermost_casenull;

                (*state).innermost_caseval = resv;
                (*state).innermost_casenull = resnull;

                ExecInitExprRec((*ctor).coercion, state, resv, resnull);

                (*state).innermost_caseval = innermost_caseval;
                (*state).innermost_casenull = innermost_isnull;
            }
        }

        NodeTag::T_JsonIsPredicate => {
            let pred = node as *mut JsonIsPredicate;

            ExecInitExprRec((*pred).expr as *mut _, state, resv, resnull);

            scratch.opcode = EEOP_IS_JSON as isize;
            scratch.d.is_json.pred = pred;

            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_JsonExpr => {
            let jsexpr = node as *mut JsonExpr;

            /*
             * No need to initialize a full JsonExprState For
             * JSON_TABLE(), because the upstream caller tfuncFetchRows()
             * is only interested in the value of formatted_expr.
             */
            if (*jsexpr).op == crate::nodes::primnodes::JSON_TABLE_OP {
                ExecInitExprRec(
                    (*jsexpr).formatted_expr as *mut _,
                    state, resv, resnull,
                );
            } else {
                ExecInitJsonExpr(jsexpr, state, resv, resnull, &mut scratch);
            }
        }

        NodeTag::T_NullTest => {
            let ntest = node as *mut NullTest;

            if (*ntest).nulltesttype == crate::nodes::primnodes::IS_NULL {
                if (*ntest).argisrow {
                    scratch.opcode = EEOP_NULLTEST_ROWISNULL as isize;
                } else {
                    scratch.opcode = EEOP_NULLTEST_ISNULL as isize;
                }
            } else if (*ntest).nulltesttype == crate::nodes::primnodes::IS_NOT_NULL {
                if (*ntest).argisrow {
                    scratch.opcode = EEOP_NULLTEST_ROWISNOTNULL as isize;
                } else {
                    scratch.opcode = EEOP_NULLTEST_ISNOTNULL as isize;
                }
            } else {
                elog!(
                    ERROR,
                    "unrecognized nulltesttype: {}",
                    (*ntest).nulltesttype as c_int
                );
            }
            /* initialize cache in case it's a row test */
            scratch.d.nulltest_row.rowcache.cacheptr = core::ptr::null_mut();

            /* first evaluate argument into result variable */
            ExecInitExprRec((*ntest).arg, state, resv, resnull);

            /* then push the test of that argument */
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_BooleanTest => {
            let btest = node as *mut crate::nodes::primnodes::BooleanTest;

            /*
             * Evaluate argument, directly into result datum.
             */
            ExecInitExprRec((*btest).arg, state, resv, resnull);

            match (*btest).booltesttype {
                crate::nodes::primnodes::IS_TRUE => {
                    scratch.opcode = EEOP_BOOLTEST_IS_TRUE as isize;
                }
                crate::nodes::primnodes::IS_NOT_TRUE => {
                    scratch.opcode = EEOP_BOOLTEST_IS_NOT_TRUE as isize;
                }
                crate::nodes::primnodes::IS_FALSE => {
                    scratch.opcode = EEOP_BOOLTEST_IS_FALSE as isize;
                }
                crate::nodes::primnodes::IS_NOT_FALSE => {
                    scratch.opcode = EEOP_BOOLTEST_IS_NOT_FALSE as isize;
                }
                crate::nodes::primnodes::IS_UNKNOWN => {
                    /* Same as scalar IS NULL test */
                    scratch.opcode = EEOP_NULLTEST_ISNULL as isize;
                }
                crate::nodes::primnodes::IS_NOT_UNKNOWN => {
                    /* Same as scalar IS NOT NULL test */
                    scratch.opcode = EEOP_NULLTEST_ISNOTNULL as isize;
                }
                _ => {
                    elog!(
                        ERROR,
                        "unrecognized booltesttype: {}",
                        (*btest).booltesttype as c_int
                    );
                }
            }

            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_CoerceToDomain => {
            let ctest = node as *mut CoerceToDomain;
            ExecInitCoerceToDomain(&mut scratch, ctest, state, resv, resnull);
        }

        NodeTag::T_CoerceToDomainValue => {
            /*
             * Read from location identified by innermost_domainval.
             */
            if (*state).innermost_domainval.is_null() {
                scratch.opcode = EEOP_DOMAIN_TESTVAL_EXT as isize;
            } else {
                scratch.opcode = EEOP_DOMAIN_TESTVAL as isize;
                /* we share instruction union variant with case testval */
                scratch.d.casetest.value = (*state).innermost_domainval;
                scratch.d.casetest.isnull = (*state).innermost_domainnull;
            }
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_CurrentOfExpr => {
            scratch.opcode = EEOP_CURRENTOFEXPR as isize;
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_NextValueExpr => {
            let nve = node as *mut NextValueExpr;
            scratch.opcode = EEOP_NEXTVALUEEXPR as isize;
            scratch.d.nextvalueexpr.seqid = (*nve).seqid;
            scratch.d.nextvalueexpr.seqtypid = (*nve).typeId;
            ExprEvalPushStep(state, &scratch);
        }

        NodeTag::T_ReturningExpr => {
            let rexpr = node as *mut crate::nodes::primnodes::ReturningExpr;
            let retstep: c_int;

            /* Skip expression evaluation if OLD/NEW row doesn't exist */
            scratch.opcode = EEOP_RETURNINGEXPR as isize;
            scratch.d.returningexpr.nullflag = if (*rexpr).retold {
                crate::nodes::execnodes::EEO_FLAG_OLD_IS_NULL
            } else {
                crate::nodes::execnodes::EEO_FLAG_NEW_IS_NULL
            };
            scratch.d.returningexpr.jumpdone = -1; /* set below */
            ExprEvalPushStep(state, &scratch);
            retstep = (*state).steps_len - 1;

            /* Steps to evaluate expression to return */
            ExecInitExprRec((*rexpr).retexpr, state, resv, resnull);

            /* Jump target used if OLD/NEW row doesn't exist */
            (*(*state).steps.add(retstep as usize))
                .d
                .returningexpr
                .jumpdone = (*state).steps_len;

            /* Update ExprState flags */
            if (*rexpr).retold {
                (*state).flags |= EEO_FLAG_HAS_OLD;
            } else {
                (*state).flags |= EEO_FLAG_HAS_NEW;
            }
        }

        _ => {
            elog!(
                ERROR,
                "unrecognized node type: {}",
                (*node).r#type as c_int
            );
        }
    }
}

/*
 * Add another expression evaluation step to ExprState->steps.
 *
 * Note that this potentially re-allocates es->steps, therefore no pointer
 * into that array may be used while the expression is still being built.
 */
pub unsafe fn ExprEvalPushStep(es: *mut ExprState, s: *const ExprEvalStep) {
    if (*es).steps_alloc == 0 {
        (*es).steps_alloc = 16;
        (*es).steps = palloc(
            core::mem::size_of::<ExprEvalStep>() * (*es).steps_alloc as usize,
        ) as *mut ExprEvalStep;
    } else if (*es).steps_alloc == (*es).steps_len {
        (*es).steps_alloc *= 2;
        (*es).steps = repalloc(
            (*es).steps as *mut c_void,
            core::mem::size_of::<ExprEvalStep>() * (*es).steps_alloc as usize,
        ) as *mut ExprEvalStep;
    }

    core::ptr::copy_nonoverlapping(s, (*es).steps.add((*es).steps_len as usize), 1);
    (*es).steps_len += 1;
}

/*
 * Perform setup necessary for the evaluation of a function-like expression,
 * appending argument evaluation steps to the steps list in *state, and
 * setting up *scratch so it is ready to be pushed.
 *
 * *scratch is not pushed here, so that callers may override the opcode,
 * which is useful for function-like cases like DISTINCT.
 */
unsafe fn ExecInitFunc(
    scratch: *mut ExprEvalStep,
    node: *mut crate::nodes::primnodes::Expr,
    args: *mut List,
    funcid: Oid,
    inputcollid: Oid,
    state: *mut ExprState,
) {
    let nargs = crate::nodes::pg_list::list_length(args);
    let aclresult: AclResult;
    let flinfo: *mut FmgrInfo;
    let fcinfo: FunctionCallInfo;

    /* Check permission to call function */
    aclresult = object_aclcheck(ProcedureRelationId, funcid, GetUserId(), 0 /* ACL_EXECUTE */);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(funcid));
    }
    InvokeFunctionExecuteHook(funcid);

    /*
     * Safety check on nargs.  Under normal circumstances this should never
     * fail, as parser should check sooner.  But possibly it might fail if
     * server has been compiled with FUNC_MAX_ARGS smaller than some functions
     * declared in pg_proc?
     */
    if nargs > FUNC_MAX_ARGS {
        ereport!(
            ERROR,
            errmsg!(
                "cannot pass more than {} arguments to a function",
                FUNC_MAX_ARGS
            )
        );
    }

    /* Allocate function lookup data and parameter workspace for this call */
    (*scratch).d.func.finfo = palloc0(core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
    (*scratch).d.func.fcinfo_data = palloc0(SizeForFunctionCallInfo(nargs))
        as FunctionCallInfo;
    flinfo = (*scratch).d.func.finfo;
    fcinfo = (*scratch).d.func.fcinfo_data;

    /* Set up the primary fmgr lookup information */
    fmgr_info(funcid, flinfo);
    fmgr_info_set_expr(node as *mut Node, flinfo);

    /* Initialize function call parameter structure too */
    InitFunctionCallInfoData(fcinfo, flinfo, nargs, inputcollid, core::ptr::null_mut(), core::ptr::null_mut());

    /* Keep extra copies of this info to save an indirection at runtime */
    (*scratch).d.func.fn_addr = (*flinfo).fn_addr.unwrap();
    (*scratch).d.func.nargs = nargs;

    /* We only support non-set functions here */
    if (*flinfo).fn_retset {
        ereport!(
            ERROR,
            errmsg!("set-valued function called in context that cannot accept a set")
        );
    }

    /* Build code to evaluate arguments directly into the fcinfo struct */
    let mut argno: c_int = 0;
    let mut lc = crate::nodes::pg_list::list_head(args);
    while !lc.is_null() {
        let arg = *(lc as *mut *mut crate::nodes::primnodes::Expr);

        if IsA!(arg, T_Const) {
            /*
             * Don't evaluate const arguments every round; especially
             * interesting for constants in comparisons.
             */
            let con = arg as *mut crate::nodes::primnodes::Const;
            (*(*fcinfo).args.as_mut_ptr().add(argno as usize)).value = (*con).constvalue;
            (*(*fcinfo).args.as_mut_ptr().add(argno as usize)).isnull = (*con).constisnull;
        } else {
            ExecInitExprRec(
                arg,
                state,
                &mut (*(*fcinfo).args.as_mut_ptr().add(argno as usize)).value,
                &mut (*(*fcinfo).args.as_mut_ptr().add(argno as usize)).isnull,
            );
        }
        argno += 1;
        lc = crate::nodes::pg_list::lnext(args, lc);
    }

    /* Insert appropriate opcode depending on strictness and stats level */
    if pgstat_track_functions <= (*flinfo).fn_stats as c_int {
        if (*flinfo).fn_strict && nargs > 0 {
            /* Choose nargs optimized implementation if available. */
            if nargs == 1 {
                (*scratch).opcode = EEOP_FUNCEXPR_STRICT_1 as isize;
            } else if nargs == 2 {
                (*scratch).opcode = EEOP_FUNCEXPR_STRICT_2 as isize;
            } else {
                (*scratch).opcode = EEOP_FUNCEXPR_STRICT as isize;
            }
        } else {
            (*scratch).opcode = EEOP_FUNCEXPR as isize;
        }
    } else {
        if (*flinfo).fn_strict && nargs > 0 {
            (*scratch).opcode = EEOP_FUNCEXPR_STRICT_FUSAGE as isize;
        } else {
            (*scratch).opcode = EEOP_FUNCEXPR_FUSAGE as isize;
        }
    }
}

/*
 * Append the steps necessary for the evaluation of a SubPlan node to
 * ExprState->steps.
 *
 * subplan - SubPlan expression to evaluate
 * state - ExprState to whose ->steps to append the necessary operations
 * resv / resnull - where to store the result of the node into
 */
unsafe fn ExecInitSubPlanExpr(
    subplan: *mut SubPlan,
    state: *mut ExprState,
    resv: *mut Datum,
    resnull: *mut bool,
) {
    let mut scratch: ExprEvalStep = core::mem::zeroed();

    if (*state).parent.is_null() {
        elog!(ERROR, "SubPlan found with no parent plan");
    }

    /*
     * Generate steps to evaluate input arguments for the subplan.
     */
    debug_assert!(
        crate::nodes::pg_list::list_length((*subplan).parParam)
            == crate::nodes::pg_list::list_length((*subplan).args)
    );

    let mut l = crate::nodes::pg_list::list_head((*subplan).parParam);
    let mut pvar = crate::nodes::pg_list::list_head((*subplan).args);
    while !l.is_null() {
        let paramid = crate::nodes::pg_list::lfirst_int(l);
        let arg = *(pvar as *mut *mut crate::nodes::primnodes::Expr);

        ExecInitExprRec(arg, state, resv, resnull);

        scratch.opcode = EEOP_PARAM_SET as isize;
        scratch.resvalue = resv;
        scratch.resnull = resnull;
        scratch.d.param.paramid = paramid;
        /* paramtype's not actually used, but we might as well fill it */
        scratch.d.param.paramtype = exprType(arg as *const Node);
        ExprEvalPushStep(state, &scratch);

        l = crate::nodes::pg_list::lnext((*subplan).parParam, l);
        pvar = crate::nodes::pg_list::lnext((*subplan).args, pvar);
    }

    let sstate: *mut SubPlanState = ExecInitSubPlan(subplan, (*state).parent);

    /* add SubPlanState nodes to state->parent->subPlan */
    (*(*state).parent).subPlan = lappend(
        (*(*state).parent).subPlan,
        sstate as *mut c_void,
    );

    scratch.opcode = EEOP_SUBPLAN as isize;
    scratch.resvalue = resv;
    scratch.resnull = resnull;
    scratch.d.subplan.sstate = sstate;

    ExprEvalPushStep(state, &scratch);
}

/*
 * Add expression steps performing setup that's needed before any of the
 * main execution of the expression.
 */
unsafe fn ExecCreateExprSetupSteps(state: *mut ExprState, node: *mut Node) {
    let mut info: ExprSetupInfo = ExprSetupInfo {
        last_inner: 0,
        last_outer: 0,
        last_scan: 0,
        last_old: 0,
        last_new: 0,
        multiexpr_subplans: NIL,
    };

    /* Prescan to find out what we need. */
    expr_setup_walker(node, &mut info);

    /* And generate those steps. */
    ExecPushExprSetupSteps(state, &mut info);
}

/*
 * Add steps performing expression setup as indicated by "info".
 * This is useful when building an ExprState covering more than one expression.
 */
unsafe fn ExecPushExprSetupSteps(state: *mut ExprState, info: *mut ExprSetupInfo) {
    let mut scratch: ExprEvalStep = core::mem::zeroed();
    scratch.resvalue = core::ptr::null_mut();
    scratch.resnull = core::ptr::null_mut();

    /*
     * Add steps deforming the ExprState's inner/outer/scan/old/new slots as
     * much as required by any Vars appearing in the expression.
     */
    if (*info).last_inner > 0 {
        scratch.opcode = EEOP_INNER_FETCHSOME as isize;
        scratch.d.fetch.last_var = (*info).last_inner as c_int;
        scratch.d.fetch.fixed = false;
        scratch.d.fetch.kind = core::ptr::null();
        scratch.d.fetch.known_desc = core::ptr::null_mut();
        if ExecComputeSlotInfo(state, &mut scratch) {
            ExprEvalPushStep(state, &scratch);
        }
    }
    if (*info).last_outer > 0 {
        scratch.opcode = EEOP_OUTER_FETCHSOME as isize;
        scratch.d.fetch.last_var = (*info).last_outer as c_int;
        scratch.d.fetch.fixed = false;
        scratch.d.fetch.kind = core::ptr::null();
        scratch.d.fetch.known_desc = core::ptr::null_mut();
        if ExecComputeSlotInfo(state, &mut scratch) {
            ExprEvalPushStep(state, &scratch);
        }
    }
    if (*info).last_scan > 0 {
        scratch.opcode = EEOP_SCAN_FETCHSOME as isize;
        scratch.d.fetch.last_var = (*info).last_scan as c_int;
        scratch.d.fetch.fixed = false;
        scratch.d.fetch.kind = core::ptr::null();
        scratch.d.fetch.known_desc = core::ptr::null_mut();
        if ExecComputeSlotInfo(state, &mut scratch) {
            ExprEvalPushStep(state, &scratch);
        }
    }
    if (*info).last_old > 0 {
        scratch.opcode = EEOP_OLD_FETCHSOME as isize;
        scratch.d.fetch.last_var = (*info).last_old as c_int;
        scratch.d.fetch.fixed = false;
        scratch.d.fetch.kind = core::ptr::null();
        scratch.d.fetch.known_desc = core::ptr::null_mut();
        if ExecComputeSlotInfo(state, &mut scratch) {
            ExprEvalPushStep(state, &scratch);
        }
    }
    if (*info).last_new > 0 {
        scratch.opcode = EEOP_NEW_FETCHSOME as isize;
        scratch.d.fetch.last_var = (*info).last_new as c_int;
        scratch.d.fetch.fixed = false;
        scratch.d.fetch.kind = core::ptr::null();
        scratch.d.fetch.known_desc = core::ptr::null_mut();
        if ExecComputeSlotInfo(state, &mut scratch) {
            ExprEvalPushStep(state, &scratch);
        }
    }

    /*
     * Add steps to execute any MULTIEXPR SubPlans appearing in the
     * expression.  We need to evaluate these before any of the Params
     * referencing their outputs are used, but after we've prepared for any
     * Var references they may contain.
     */
    let mut lc = crate::nodes::pg_list::list_head((*info).multiexpr_subplans);
    while !lc.is_null() {
        let subplan = *(lc as *mut *mut SubPlan);
        debug_assert!((*subplan).subLinkType == MULTIEXPR_SUBLINK);

        /* The result can be ignored, but we better put it somewhere */
        ExecInitSubPlanExpr(subplan, state, &mut (*state).resvalue, &mut (*state).resnull);
        lc = crate::nodes::pg_list::lnext((*info).multiexpr_subplans, lc);
    }
}

/*
 * expr_setup_walker: expression walker for ExecCreateExprSetupSteps
 */
unsafe fn expr_setup_walker(node: *mut Node, info: *mut ExprSetupInfo) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        let variable = node as *mut Var;
        let attnum = (*variable).varattno;

        match (*variable).varno {
            crate::nodes::primnodes::INNER_VAR => {
                if attnum > (*info).last_inner {
                    (*info).last_inner = attnum;
                }
            }
            crate::nodes::primnodes::OUTER_VAR => {
                if attnum > (*info).last_outer {
                    (*info).last_outer = attnum;
                }
            }
            /* INDEX_VAR is handled by default case */
            _ => {
                match (*variable).varreturningtype {
                    VarReturningType::VAR_RETURNING_DEFAULT => {
                        if attnum > (*info).last_scan {
                            (*info).last_scan = attnum;
                        }
                    }
                    VarReturningType::VAR_RETURNING_OLD => {
                        if attnum > (*info).last_old {
                            (*info).last_old = attnum;
                        }
                    }
                    VarReturningType::VAR_RETURNING_NEW => {
                        if attnum > (*info).last_new {
                            (*info).last_new = attnum;
                        }
                    }
                }
            }
        }
        return false;
    }

    /* Collect all MULTIEXPR SubPlans, too */
    if IsA!(node, T_SubPlan) {
        let subplan = node as *mut SubPlan;
        if (*subplan).subLinkType == MULTIEXPR_SUBLINK {
            (*info).multiexpr_subplans = lappend(
                (*info).multiexpr_subplans,
                subplan as *mut c_void,
            );
        }
    }

    /*
     * Don't examine the arguments or filters of Aggrefs or WindowFuncs,
     * because those do not represent expressions to be evaluated within the
     * calling expression's econtext.  GroupingFunc arguments are never
     * evaluated at all.
     */
    if IsA!(node, T_Aggref) {
        return false;
    }
    if IsA!(node, T_WindowFunc) {
        return false;
    }
    if IsA!(node, T_GroupingFunc) {
        return false;
    }
    expression_tree_walker(node, core::mem::transmute::<
        unsafe fn(*mut Node, *mut ExprSetupInfo) -> bool,
        unsafe fn(*mut Node, *mut c_void) -> bool
    >(expr_setup_walker), info as *mut c_void)
}

/*
 * Compute additional information for EEOP_*_FETCHSOME ops.
 *
 * The goal is to determine whether a slot is 'fixed', that is, every
 * evaluation of the expression will have the same type of slot, with an
 * equivalent descriptor.
 *
 * Returns true if the deforming step is required, false otherwise.
 */
unsafe fn ExecComputeSlotInfo(state: *mut ExprState, op: *mut ExprEvalStep) -> bool {
    let parent: *mut PlanState = (*state).parent;
    let mut desc: TupleDesc = core::ptr::null_mut();
    let mut tts_ops: *const TupleTableSlotOps = core::ptr::null();
    let mut isfixed = false;
    let opcode = (*op).opcode;

    debug_assert!(
        opcode == EEOP_INNER_FETCHSOME as isize
            || opcode == EEOP_OUTER_FETCHSOME as isize
            || opcode == EEOP_SCAN_FETCHSOME as isize
            || opcode == EEOP_OLD_FETCHSOME as isize
            || opcode == EEOP_NEW_FETCHSOME as isize
    );

    if !(*op).d.fetch.known_desc.is_null() {
        desc = (*op).d.fetch.known_desc;
        tts_ops = (*op).d.fetch.kind;
        isfixed = !(*op).d.fetch.kind.is_null();
    } else if parent.is_null() {
        isfixed = false;
    } else if opcode == EEOP_INNER_FETCHSOME as isize {
        let is = innerPlanState(parent);

        if (*parent).inneropsset && !(*parent).inneropsfixed {
            isfixed = false;
        } else if (*parent).inneropsset && !(*parent).innerops.is_null() {
            isfixed = true;
            tts_ops = (*parent).innerops;
            desc = ExecGetResultType(is);
        } else if !is.is_null() {
            tts_ops = ExecGetResultSlotOps(is, &mut isfixed);
            desc = ExecGetResultType(is);
        }
    } else if opcode == EEOP_OUTER_FETCHSOME as isize {
        let os = outerPlanState(parent);

        if (*parent).outeropsset && !(*parent).outeropsfixed {
            isfixed = false;
        } else if (*parent).outeropsset && !(*parent).outerops.is_null() {
            isfixed = true;
            tts_ops = (*parent).outerops;
            desc = ExecGetResultType(os);
        } else if !os.is_null() {
            tts_ops = ExecGetResultSlotOps(os, &mut isfixed);
            desc = ExecGetResultType(os);
        }
    } else if opcode == EEOP_SCAN_FETCHSOME as isize
        || opcode == EEOP_OLD_FETCHSOME as isize
        || opcode == EEOP_NEW_FETCHSOME as isize
    {
        desc = (*parent).scandesc;

        if !(*parent).scanops.is_null() {
            tts_ops = (*parent).scanops;
        }

        if (*parent).scanopsset {
            isfixed = (*parent).scanopsfixed;
        }
    }

    if isfixed && !desc.is_null() && !tts_ops.is_null() {
        (*op).d.fetch.fixed = true;
        (*op).d.fetch.kind = tts_ops;
        (*op).d.fetch.known_desc = desc;
    } else {
        (*op).d.fetch.fixed = false;
        (*op).d.fetch.kind = core::ptr::null();
        (*op).d.fetch.known_desc = core::ptr::null_mut();
    }

    /* if the slot is known to always virtual we never need to deform */
    if (*op).d.fetch.fixed && (*op).d.fetch.kind == &TTSOpsVirtual as *const _ {
        return false;
    }

    true
}

/*
 * Prepare step for the evaluation of a whole-row variable.
 * The caller still has to push the step.
 */
unsafe fn ExecInitWholeRowVar(
    scratch: *mut ExprEvalStep,
    variable: *mut Var,
    state: *mut ExprState,
) {
    let parent: *mut PlanState = (*state).parent;

    /* fill in all but the target */
    (*scratch).opcode = EEOP_WHOLEROW as isize;
    (*scratch).d.wholerow.var = variable;
    (*scratch).d.wholerow.first = true;
    (*scratch).d.wholerow.slow = false;
    (*scratch).d.wholerow.tupdesc = core::ptr::null_mut(); /* filled at runtime */
    (*scratch).d.wholerow.junkFilter = core::ptr::null_mut();

    /* update ExprState flags if Var refers to OLD/NEW */
    if (*variable).varreturningtype == VarReturningType::VAR_RETURNING_OLD {
        (*state).flags |= EEO_FLAG_HAS_OLD;
    } else if (*variable).varreturningtype == VarReturningType::VAR_RETURNING_NEW {
        (*state).flags |= EEO_FLAG_HAS_NEW;
    }

    /*
     * If the input tuple came from a subquery, it might contain "resjunk"
     * columns (such as GROUP BY or ORDER BY columns), which we don't want to
     * keep in the whole-row result.  We can get rid of such columns by
     * passing the tuple through a JunkFilter.
     */
    if !parent.is_null() {
        let mut subplan: *mut PlanState = core::ptr::null_mut();

        match NodeTag::from((*parent).r#type) {
            NodeTag::T_SubqueryScanState => {
                subplan = (*(parent as *mut SubqueryScanState)).subplan;
            }
            NodeTag::T_CteScanState => {
                subplan = (*(parent as *mut CteScanState)).cteplanstate;
            }
            _ => {}
        }

        if !subplan.is_null() {
            let mut junk_filter_needed = false;

            let mut tlist = crate::nodes::pg_list::list_head((*(*subplan).plan).targetlist);
            while !tlist.is_null() {
                let tle = *(tlist as *mut *mut crate::nodes::primnodes::TargetEntry);
                if (*tle).resjunk {
                    junk_filter_needed = true;
                    break;
                }
                tlist = crate::nodes::pg_list::lnext((*(*subplan).plan).targetlist, tlist);
            }

            /* If so, build the junkfilter now */
            if junk_filter_needed {
                (*scratch).d.wholerow.junkFilter = ExecInitJunkFilter(
                    (*(*subplan).plan).targetlist,
                    ExecInitExtraTupleSlot(
                        (*parent).state,
                        core::ptr::null_mut(),
                        &TTSOpsVirtual,
                    ),
                );
            }
        }
    }
}

/*
 * Prepare evaluation of a SubscriptingRef expression.
 */
unsafe fn ExecInitSubscriptingRef(
    scratch: *mut ExprEvalStep,
    sbsref: *mut SubscriptingRef,
    state: *mut ExprState,
    resv: *mut Datum,
    resnull: *mut bool,
) {
    let is_assignment = !(*sbsref).refassgnexpr.is_null();
    let nupper = crate::nodes::pg_list::list_length((*sbsref).refupperindexpr);
    let nlower = crate::nodes::pg_list::list_length((*sbsref).reflowerindexpr);
    let sbsroutines: *const SubscriptRoutines;
    let sbsrefstate: *mut SubscriptingRefState;
    let mut methods: SubscriptExecSteps = core::mem::zeroed();
    let mut adjust_jumps: *mut List = NIL;

    /* Look up the subscripting support methods */
    sbsroutines = getSubscriptingRoutines((*sbsref).refcontainertype, core::ptr::null_mut());
    if sbsroutines.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "cannot subscript type because it does not support subscripting"
            )
        );
    }

    /* Allocate sbsrefstate, with enough space for per-subscript arrays too */
    let sbsref_size = MAXALIGN(core::mem::size_of::<SubscriptingRefState>())
        + (nupper + nlower) as usize
            * (core::mem::size_of::<Datum>() + 2 * core::mem::size_of::<bool>());
    sbsrefstate = palloc0(sbsref_size) as *mut SubscriptingRefState;

    /* Fill constant fields of SubscriptingRefState */
    (*sbsrefstate).isassignment = is_assignment;
    (*sbsrefstate).numupper = nupper;
    (*sbsrefstate).numlower = nlower;

    /* Set up per-subscript arrays */
    let mut ptr = (sbsrefstate as *mut u8)
        .add(MAXALIGN(core::mem::size_of::<SubscriptingRefState>()));
    (*sbsrefstate).upperindex = ptr as *mut Datum;
    ptr = ptr.add(nupper as usize * core::mem::size_of::<Datum>());
    (*sbsrefstate).lowerindex = ptr as *mut Datum;
    ptr = ptr.add(nlower as usize * core::mem::size_of::<Datum>());
    (*sbsrefstate).upperprovided = ptr as *mut bool;
    ptr = ptr.add(nupper as usize * core::mem::size_of::<bool>());
    (*sbsrefstate).lowerprovided = ptr as *mut bool;
    ptr = ptr.add(nlower as usize * core::mem::size_of::<bool>());
    (*sbsrefstate).upperindexnull = ptr as *mut bool;
    ptr = ptr.add(nupper as usize * core::mem::size_of::<bool>());
    (*sbsrefstate).lowerindexnull = ptr as *mut bool;

    /*
     * Let the container-type-specific code have a chance.
     */
    ((*sbsroutines).exec_setup.unwrap())(sbsref, sbsrefstate, &mut methods);

    /*
     * Evaluate array input into resv/resnull.
     */
    ExecInitExprRec((*sbsref).refexpr, state, resv, resnull);

    /*
     * If refexpr yields NULL, and the operation should be strict, then
     * result is NULL.
     */
    if !is_assignment && methods.sbs_fetch_strict {
        (*scratch).opcode = EEOP_JUMP_IF_NULL as isize;
        (*scratch).d.jump.jumpdone = -1;
        ExprEvalPushStep(state, scratch);
        adjust_jumps = lappend_int(adjust_jumps, (*state).steps_len - 1);
    }

    /* Evaluate upper subscripts */
    let mut i: c_int = 0;
    let mut lc = crate::nodes::pg_list::list_head((*sbsref).refupperindexpr);
    while !lc.is_null() {
        let e = *(lc as *mut *mut crate::nodes::primnodes::Expr);

        /* When slicing, individual subscript bounds can be omitted */
        if e.is_null() {
            *(*sbsrefstate).upperprovided.add(i as usize) = false;
            *(*sbsrefstate).upperindexnull.add(i as usize) = true;
        } else {
            *(*sbsrefstate).upperprovided.add(i as usize) = true;
            ExecInitExprRec(
                e,
                state,
                (*sbsrefstate).upperindex.add(i as usize),
                (*sbsrefstate).upperindexnull.add(i as usize),
            );
        }
        i += 1;
        lc = crate::nodes::pg_list::lnext((*sbsref).refupperindexpr, lc);
    }

    /* Evaluate lower subscripts similarly */
    i = 0;
    let mut lc2 = crate::nodes::pg_list::list_head((*sbsref).reflowerindexpr);
    while !lc2.is_null() {
        let e = *(lc2 as *mut *mut crate::nodes::primnodes::Expr);

        /* When slicing, individual subscript bounds can be omitted */
        if e.is_null() {
            *(*sbsrefstate).lowerprovided.add(i as usize) = false;
            *(*sbsrefstate).lowerindexnull.add(i as usize) = true;
        } else {
            *(*sbsrefstate).lowerprovided.add(i as usize) = true;
            ExecInitExprRec(
                e,
                state,
                (*sbsrefstate).lowerindex.add(i as usize),
                (*sbsrefstate).lowerindexnull.add(i as usize),
            );
        }
        i += 1;
        lc2 = crate::nodes::pg_list::lnext((*sbsref).reflowerindexpr, lc2);
    }

    /* SBSREF_SUBSCRIPTS checks and converts all the subscripts at once */
    if methods.sbs_check_subscripts.is_some() {
        (*scratch).opcode = EEOP_SBSREF_SUBSCRIPTS as isize;
        (*scratch).d.sbsref_subscript.subscriptfunc = methods.sbs_check_subscripts;
        (*scratch).d.sbsref_subscript.state = sbsrefstate;
        (*scratch).d.sbsref_subscript.jumpdone = -1;
        ExprEvalPushStep(state, scratch);
        adjust_jumps = lappend_int(adjust_jumps, (*state).steps_len - 1);
    }

    if is_assignment {
        let save_innermost_caseval: *mut Datum;
        let save_innermost_casenull: *mut bool;

        /* Check for unimplemented methods */
        if methods.sbs_assign.is_none() {
            ereport!(
                ERROR,
                errmsg!("type does not support subscripted assignment")
            );
        }

        /*
         * We might have a nested-assignment situation, in which the
         * refassgnexpr is itself a FieldStore or SubscriptingRef that needs
         * to obtain and modify the previous value.
         */
        if isAssignmentIndirectionExpr((*sbsref).refassgnexpr) {
            if methods.sbs_fetch_old.is_none() {
                ereport!(
                    ERROR,
                    errmsg!("type does not support subscripted assignment")
                );
            }
            (*scratch).opcode = EEOP_SBSREF_OLD as isize;
            (*scratch).d.sbsref.subscriptfunc = methods.sbs_fetch_old;
            (*scratch).d.sbsref.state = sbsrefstate;
            ExprEvalPushStep(state, scratch);
        }

        /* SBSREF_OLD puts extracted value into prevvalue/prevnull */
        save_innermost_caseval = (*state).innermost_caseval;
        save_innermost_casenull = (*state).innermost_casenull;
        (*state).innermost_caseval = &mut (*sbsrefstate).prevvalue;
        (*state).innermost_casenull = &mut (*sbsrefstate).prevnull;

        /* evaluate replacement value into replacevalue/replacenull */
        ExecInitExprRec(
            (*sbsref).refassgnexpr,
            state,
            &mut (*sbsrefstate).replacevalue,
            &mut (*sbsrefstate).replacenull,
        );

        (*state).innermost_caseval = save_innermost_caseval;
        (*state).innermost_casenull = save_innermost_casenull;

        /* and perform the assignment */
        (*scratch).opcode = EEOP_SBSREF_ASSIGN as isize;
        (*scratch).d.sbsref.subscriptfunc = methods.sbs_assign;
        (*scratch).d.sbsref.state = sbsrefstate;
        ExprEvalPushStep(state, scratch);
    } else {
        /* array fetch is much simpler */
        (*scratch).opcode = EEOP_SBSREF_FETCH as isize;
        (*scratch).d.sbsref.subscriptfunc = methods.sbs_fetch;
        (*scratch).d.sbsref.state = sbsrefstate;
        ExprEvalPushStep(state, scratch);
    }

    /* adjust jump targets */
    let mut lc3 = crate::nodes::pg_list::list_head(adjust_jumps);
    while !lc3.is_null() {
        let j = crate::nodes::pg_list::lfirst_int(lc3);
        let as_ = &mut *(*state).steps.add(j as usize);

        if as_.opcode == EEOP_SBSREF_SUBSCRIPTS as isize {
            debug_assert!(as_.d.sbsref_subscript.jumpdone == -1);
            as_.d.sbsref_subscript.jumpdone = (*state).steps_len;
        } else {
            debug_assert!(as_.opcode == EEOP_JUMP_IF_NULL as isize);
            debug_assert!(as_.d.jump.jumpdone == -1);
            as_.d.jump.jumpdone = (*state).steps_len;
        }
        lc3 = crate::nodes::pg_list::lnext(adjust_jumps, lc3);
    }
}

/*
 * Helper for preparing SubscriptingRef expressions for evaluation: is expr
 * a nested FieldStore or SubscriptingRef that needs the old element value
 * passed down?
 */
unsafe fn isAssignmentIndirectionExpr(
    expr: *mut crate::nodes::primnodes::Expr,
) -> bool {
    if expr.is_null() {
        return false; /* just paranoia */
    }
    if IsA!(expr, T_FieldStore) {
        let fstore = expr as *mut FieldStore;
        if !(*fstore).arg.is_null() && IsA!((*fstore).arg, T_CaseTestExpr) {
            return true;
        }
    } else if IsA!(expr, T_SubscriptingRef) {
        let sbsref = expr as *mut SubscriptingRef;
        if !(*sbsref).refexpr.is_null() && IsA!((*sbsref).refexpr, T_CaseTestExpr) {
            return true;
        }
    } else if IsA!(expr, T_CoerceToDomain) {
        let cd = expr as *mut CoerceToDomain;
        return isAssignmentIndirectionExpr((*cd).arg);
    } else if IsA!(expr, T_RelabelType) {
        let r = expr as *mut crate::nodes::primnodes::RelabelType;
        return isAssignmentIndirectionExpr((*r).arg);
    }
    false
}

/*
 * Prepare evaluation of a CoerceToDomain expression.
 */
unsafe fn ExecInitCoerceToDomain(
    scratch: *mut ExprEvalStep,
    ctest: *mut CoerceToDomain,
    state: *mut ExprState,
    resv: *mut Datum,
    resnull: *mut bool,
) {
    let constraint_ref: *mut DomainConstraintRef;
    let mut domainval: *mut Datum = core::ptr::null_mut();
    let mut domainnull: *mut bool = core::ptr::null_mut();

    (*scratch).d.domaincheck.resulttype = (*ctest).resulttype;
    /* we'll allocate workspace only if needed */
    (*scratch).d.domaincheck.checkvalue = core::ptr::null_mut();
    (*scratch).d.domaincheck.checknull = core::ptr::null_mut();
    (*scratch).d.domaincheck.escontext = (*state).escontext;

    /*
     * Evaluate argument - it's fine to directly store it into resv/resnull,
     * if there's constraint failures there'll be errors, otherwise it's what
     * needs to be returned.
     */
    ExecInitExprRec((*ctest).arg, state, resv, resnull);

    /*
     * Collect the constraints associated with the domain.
     */
    constraint_ref = palloc(core::mem::size_of::<DomainConstraintRef>())
        as *mut DomainConstraintRef;
    InitDomainConstraintRef(
        (*ctest).resulttype,
        constraint_ref,
        CurrentMemoryContext(),
        false,
    );

    /*
     * Compile code to check each domain constraint.
     */
    let mut l = crate::nodes::pg_list::list_head((*constraint_ref).constraints);
    while !l.is_null() {
        let con = *(l as *mut *mut DomainConstraintState);
        let save_innermost_domainval: *mut Datum;
        let save_innermost_domainnull: *mut bool;

        (*scratch).d.domaincheck.constraintname = (*con).name;

        match (*con).constrainttype {
            DOM_CONSTRAINT_NOTNULL => {
                (*scratch).opcode = EEOP_DOMAIN_NOTNULL as isize;
                ExprEvalPushStep(state, scratch);
            }
            DOM_CONSTRAINT_CHECK => {
                /* Allocate workspace for CHECK output if we didn't yet */
                if (*scratch).d.domaincheck.checkvalue.is_null() {
                    (*scratch).d.domaincheck.checkvalue =
                        palloc(core::mem::size_of::<Datum>()) as *mut Datum;
                    (*scratch).d.domaincheck.checknull =
                        palloc(core::mem::size_of::<bool>()) as *mut bool;
                }

                /*
                 * If first time through, determine where CoerceToDomainValue
                 * nodes should read from.
                 */
                if domainval.is_null() {
                    /*
                     * Since value might be read multiple times, force to R/O
                     * - but only if it could be an expanded datum.
                     */
                    if get_typlen((*ctest).resulttype) == -1 {
                        let mut scratch2: ExprEvalStep = core::mem::zeroed();

                        /* Yes, so make output workspace for MAKE_READONLY */
                        domainval = palloc(core::mem::size_of::<Datum>()) as *mut Datum;
                        domainnull = palloc(core::mem::size_of::<bool>()) as *mut bool;

                        /* Emit MAKE_READONLY */
                        scratch2.opcode = EEOP_MAKE_READONLY as isize;
                        scratch2.resvalue = domainval;
                        scratch2.resnull = domainnull;
                        scratch2.d.make_readonly.value = resv;
                        scratch2.d.make_readonly.isnull = resnull;
                        ExprEvalPushStep(state, &scratch2);
                    } else {
                        /* No, so it's fine to read from resv/resnull */
                        domainval = resv;
                        domainnull = resnull;
                    }
                }

                /*
                 * Set up value to be returned by CoerceToDomainValue nodes.
                 */
                save_innermost_domainval = (*state).innermost_domainval;
                save_innermost_domainnull = (*state).innermost_domainnull;
                (*state).innermost_domainval = domainval;
                (*state).innermost_domainnull = domainnull;

                /* evaluate check expression value */
                ExecInitExprRec(
                    (*con).check_expr,
                    state,
                    (*scratch).d.domaincheck.checkvalue,
                    (*scratch).d.domaincheck.checknull,
                );

                (*state).innermost_domainval = save_innermost_domainval;
                (*state).innermost_domainnull = save_innermost_domainnull;

                /* now test result */
                (*scratch).opcode = EEOP_DOMAIN_CHECK as isize;
                ExprEvalPushStep(state, scratch);
            }
            _ => {
                elog!(
                    ERROR,
                    "unrecognized constraint type: {}",
                    (*con).constrainttype as c_int
                );
            }
        }

        l = crate::nodes::pg_list::lnext((*constraint_ref).constraints, l);
    }
}

/*
 * Build transition/combine function invocations for all aggregate transition
 * / combination function invocations in a grouping sets phase.
 */
pub unsafe fn ExecBuildAggTrans(
    aggstate: *mut AggState,
    phase: *mut AggStatePerPhase,
    do_sort: bool,
    do_hash: bool,
    nullcheck: bool,
) -> *mut ExprState {
    let state: *mut ExprState = makeNode!(ExprState, T_ExprState);
    let parent: *mut PlanState = &mut (*aggstate).ss.ps;
    let mut scratch: ExprEvalStep = core::mem::zeroed();
    let is_combine = DO_AGGSPLIT_COMBINE((*aggstate).aggsplit);
    let mut deform: ExprSetupInfo = ExprSetupInfo {
        last_inner: 0,
        last_outer: 0,
        last_scan: 0,
        last_old: 0,
        last_new: 0,
        multiexpr_subplans: NIL,
    };

    (*state).expr = aggstate as *mut crate::nodes::primnodes::Expr;
    (*state).parent = parent;

    scratch.resvalue = &mut (*state).resvalue;
    scratch.resnull = &mut (*state).resnull;

    /*
     * First figure out which slots, and how many columns from each, we're
     * going to need.
     */
    for transno in 0..(*aggstate).numtrans {
        let pertrans = (*aggstate).pertrans.add(transno as usize);

        expr_setup_walker((*(*pertrans).aggref).aggdirectargs as *mut Node, &mut deform);
        expr_setup_walker((*(*pertrans).aggref).args as *mut Node, &mut deform);
        expr_setup_walker((*(*pertrans).aggref).aggorder as *mut Node, &mut deform);
        expr_setup_walker((*(*pertrans).aggref).aggdistinct as *mut Node, &mut deform);
        expr_setup_walker((*(*pertrans).aggref).aggfilter as *mut Node, &mut deform);
    }
    ExecPushExprSetupSteps(state, &mut deform);

    /*
     * Emit instructions for each transition value / grouping set combination.
     */
    for transno in 0..(*aggstate).numtrans {
        let pertrans = (*aggstate).pertrans.add(transno as usize);
        let trans_fcinfo: FunctionCallInfo = (*pertrans).transfn_fcinfo;
        let mut adjust_bailout: *mut List = NIL;
        let mut strictargs: *mut NullableDatum = core::ptr::null_mut();
        let mut strictnulls: *mut bool = core::ptr::null_mut();
        let mut argno: c_int = 0;

        /*
         * If filter present, emit. Do so before evaluating the input.
         */
        if !(*(*pertrans).aggref).aggfilter.is_null() && !is_combine {
            /* evaluate filter expression */
            ExecInitExprRec(
                (*(*pertrans).aggref).aggfilter,
                state,
                &mut (*state).resvalue,
                &mut (*state).resnull,
            );
            /* and jump out if false */
            scratch.opcode = EEOP_JUMP_IF_NOT_TRUE as isize;
            scratch.d.jump.jumpdone = -1; /* adjust later */
            ExprEvalPushStep(state, &scratch);
            adjust_bailout = lappend_int(adjust_bailout, (*state).steps_len - 1);
        }

        /*
         * Evaluate arguments to aggregate/combine function.
         */
        argno = 0;
        if is_combine {
            /*
             * Combining two aggregate transition values.
             */
            let source_tle: *mut crate::nodes::primnodes::TargetEntry;

            debug_assert!((*pertrans).numSortCols == 0);
            debug_assert!(
                crate::nodes::pg_list::list_length((*(*pertrans).aggref).args) == 1
            );

            strictargs = (*trans_fcinfo).args.as_mut_ptr().add(1) as *mut NullableDatum;
            source_tle =
                linitial((*(*pertrans).aggref).args) as *mut crate::nodes::primnodes::TargetEntry;

            if !OidIsValid((*pertrans).deserialfn_oid) {
                ExecInitExprRec(
                    (*source_tle).expr,
                    state,
                    &mut (*(*trans_fcinfo).args.as_mut_ptr().add(argno as usize + 1)).value,
                    &mut (*(*trans_fcinfo).args.as_mut_ptr().add(argno as usize + 1)).isnull,
                );
            } else {
                let ds_fcinfo: FunctionCallInfo = (*pertrans).deserialfn_fcinfo;

                /* evaluate argument */
                ExecInitExprRec(
                    (*source_tle).expr,
                    state,
                    &mut (*(*ds_fcinfo).args.as_mut_ptr().add(0)).value,
                    &mut (*(*ds_fcinfo).args.as_mut_ptr().add(0)).isnull,
                );

                /* Dummy second argument for type-safety reasons */
                (*(*ds_fcinfo).args.as_mut_ptr().add(1)).value = Datum::from(0usize);
                (*(*ds_fcinfo).args.as_mut_ptr().add(1)).isnull = false;

                /* Don't call a strict deserialization function with NULL input */
                if (*pertrans).deserialfn.fn_strict {
                    scratch.opcode = EEOP_AGG_STRICT_DESERIALIZE as isize;
                } else {
                    scratch.opcode = EEOP_AGG_DESERIALIZE as isize;
                }

                scratch.d.agg_deserialize.fcinfo_data = ds_fcinfo;
                scratch.d.agg_deserialize.jumpnull = -1; /* adjust later */
                scratch.resvalue = &mut (*(*trans_fcinfo).args.as_mut_ptr().add(argno as usize + 1)).value;
                scratch.resnull = &mut (*(*trans_fcinfo).args.as_mut_ptr().add(argno as usize + 1)).isnull;

                ExprEvalPushStep(state, &scratch);
                /* don't add an adjustment unless the function is strict */
                if (*pertrans).deserialfn.fn_strict {
                    adjust_bailout = lappend_int(adjust_bailout, (*state).steps_len - 1);
                }

                /* restore normal settings of scratch fields */
                scratch.resvalue = &mut (*state).resvalue;
                scratch.resnull = &mut (*state).resnull;
            }
            argno += 1;

            debug_assert!((*pertrans).numInputs == argno);
        } else if !(*pertrans).aggsortrequired {
            /*
             * Normal transition function without ORDER BY / DISTINCT or with
             * ORDER BY / DISTINCT but the planner has given us pre-sorted input.
             */
            strictargs = (*trans_fcinfo).args.as_mut_ptr().add(1) as *mut NullableDatum;

            let mut arg = crate::nodes::pg_list::list_head((*(*pertrans).aggref).args);
            while !arg.is_null() {
                let source_tle = *(arg as *mut *mut crate::nodes::primnodes::TargetEntry);

                /* Don't initialize args for any ORDER BY clause in presorted aggregate */
                if argno == (*pertrans).numTransInputs {
                    break;
                }

                ExecInitExprRec(
                    (*source_tle).expr,
                    state,
                    &mut (*(*trans_fcinfo).args.as_mut_ptr().add(argno as usize + 1)).value,
                    &mut (*(*trans_fcinfo).args.as_mut_ptr().add(argno as usize + 1)).isnull,
                );
                argno += 1;
                arg = crate::nodes::pg_list::lnext((*(*pertrans).aggref).args, arg);
            }
            debug_assert!((*pertrans).numTransInputs == argno);
        } else if (*pertrans).numInputs == 1 {
            /*
             * Non-presorted DISTINCT and/or ORDER BY case, with a single
             * column sorted on.
             */
            let source_tle =
                linitial((*(*pertrans).aggref).args) as *mut crate::nodes::primnodes::TargetEntry;

            debug_assert!(
                crate::nodes::pg_list::list_length((*(*pertrans).aggref).args) == 1
            );

            ExecInitExprRec(
                (*source_tle).expr,
                state,
                &mut (*state).resvalue,
                &mut (*state).resnull,
            );
            strictnulls = &mut (*state).resnull;
            argno += 1;

            debug_assert!((*pertrans).numInputs == argno);
        } else {
            /*
             * Non-presorted DISTINCT and/or ORDER BY case, with multiple
             * columns sorted on.
             */
            let values: *mut Datum = (*(*pertrans).sortslot).tts_values;
            let nulls: *mut bool = (*(*pertrans).sortslot).tts_isnull;

            strictnulls = nulls;

            let mut arg = crate::nodes::pg_list::list_head((*(*pertrans).aggref).args);
            while !arg.is_null() {
                let source_tle = *(arg as *mut *mut crate::nodes::primnodes::TargetEntry);
                ExecInitExprRec(
                    (*source_tle).expr,
                    state,
                    values.add(argno as usize),
                    nulls.add(argno as usize),
                );
                argno += 1;
                arg = crate::nodes::pg_list::lnext((*(*pertrans).aggref).args, arg);
            }
            debug_assert!((*pertrans).numInputs == argno);
        }

        /*
         * For a strict transfn, nothing happens when there's a NULL input.
         */
        if (*trans_fcinfo).flinfo.as_ref().map(|f| f.fn_strict).unwrap_or(false)
            && (*pertrans).numTransInputs > 0
        {
            if !strictnulls.is_null() {
                scratch.opcode = EEOP_AGG_STRICT_INPUT_CHECK_NULLS as isize;
            } else if !strictargs.is_null() && (*pertrans).numTransInputs == 1 {
                scratch.opcode = EEOP_AGG_STRICT_INPUT_CHECK_ARGS_1 as isize;
            } else {
                scratch.opcode = EEOP_AGG_STRICT_INPUT_CHECK_ARGS as isize;
            }
            scratch.d.agg_strict_input_check.nulls = strictnulls;
            scratch.d.agg_strict_input_check.args = strictargs;
            scratch.d.agg_strict_input_check.jumpnull = -1; /* adjust later */
            scratch.d.agg_strict_input_check.nargs = (*pertrans).numTransInputs;
            ExprEvalPushStep(state, &scratch);
            adjust_bailout = lappend_int(adjust_bailout, (*state).steps_len - 1);
        }

        /* Handle DISTINCT aggregates which have pre-sorted input */
        if (*pertrans).numDistinctCols > 0 && !(*pertrans).aggsortrequired {
            if (*pertrans).numDistinctCols > 1 {
                scratch.opcode = EEOP_AGG_PRESORTED_DISTINCT_MULTI as isize;
            } else {
                scratch.opcode = EEOP_AGG_PRESORTED_DISTINCT_SINGLE as isize;
            }
            scratch.d.agg_presorted_distinctcheck.pertrans = pertrans;
            scratch.d.agg_presorted_distinctcheck.jumpdistinct = -1; /* adjust later */
            ExprEvalPushStep(state, &scratch);
            adjust_bailout = lappend_int(adjust_bailout, (*state).steps_len - 1);
        }

        /*
         * Call transition function (once for each concurrently evaluated
         * grouping set).
         */
        if do_sort {
            let process_grouping_sets = if (**phase).numsets > 1 { (**phase).numsets } else { 1 };
            let mut setoff: c_int = 0;

            for setno in 0..process_grouping_sets {
                ExecBuildAggTransCall(
                    state, aggstate, &mut scratch, trans_fcinfo,
                    pertrans, transno, setno, setoff, false, nullcheck,
                );
                setoff += 1;
            }
        }

        if do_hash {
            let num_hashes = (*aggstate).num_hashes;
            let setoff: c_int = if (*aggstate).aggstrategy != AGG_HASHED {
                (*aggstate).maxsets
            } else {
                0
            };
            let mut setoff_mut = setoff;

            for setno in 0..num_hashes {
                ExecBuildAggTransCall(
                    state, aggstate, &mut scratch, trans_fcinfo,
                    pertrans, transno, setno, setoff_mut, true, nullcheck,
                );
                setoff_mut += 1;
            }
        }

        /* adjust early bail out jump target(s) */
        let mut bail = crate::nodes::pg_list::list_head(adjust_bailout);
        while !bail.is_null() {
            let j = crate::nodes::pg_list::lfirst_int(bail);
            let as_ = &mut *(*state).steps.add(j as usize);

            if as_.opcode == EEOP_JUMP_IF_NOT_TRUE as isize {
                debug_assert!(as_.d.jump.jumpdone == -1);
                as_.d.jump.jumpdone = (*state).steps_len;
            } else if as_.opcode == EEOP_AGG_STRICT_INPUT_CHECK_ARGS as isize
                || as_.opcode == EEOP_AGG_STRICT_INPUT_CHECK_ARGS_1 as isize
                || as_.opcode == EEOP_AGG_STRICT_INPUT_CHECK_NULLS as isize
            {
                debug_assert!(as_.d.agg_strict_input_check.jumpnull == -1);
                as_.d.agg_strict_input_check.jumpnull = (*state).steps_len;
            } else if as_.opcode == EEOP_AGG_STRICT_DESERIALIZE as isize {
                debug_assert!(as_.d.agg_deserialize.jumpnull == -1);
                as_.d.agg_deserialize.jumpnull = (*state).steps_len;
            } else if as_.opcode == EEOP_AGG_PRESORTED_DISTINCT_SINGLE as isize
                || as_.opcode == EEOP_AGG_PRESORTED_DISTINCT_MULTI as isize
            {
                debug_assert!(as_.d.agg_presorted_distinctcheck.jumpdistinct == -1);
                as_.d.agg_presorted_distinctcheck.jumpdistinct = (*state).steps_len;
            } else {
                debug_assert!(false);
            }

            bail = crate::nodes::pg_list::lnext(adjust_bailout, bail);
        }
    }

    scratch.resvalue = core::ptr::null_mut();
    scratch.resnull = core::ptr::null_mut();
    scratch.opcode = EEOP_DONE_NO_RETURN as isize;
    ExprEvalPushStep(state, &scratch);

    ExecReadyExpr(state);

    state
}

/*
 * Build transition/combine function invocation for a single transition value.
 */
unsafe fn ExecBuildAggTransCall(
    state: *mut ExprState,
    aggstate: *mut AggState,
    scratch: *mut ExprEvalStep,
    fcinfo: FunctionCallInfo,
    pertrans: AggStatePerTrans,
    transno: c_int,
    setno: c_int,
    setoff: c_int,
    ishash: bool,
    nullcheck: bool,
) {
    let aggcontext: *mut ExprContext;
    let mut adjust_jumpnull: c_int = -1;

    if ishash {
        aggcontext = (*aggstate).hashcontext;
    } else {
        aggcontext = *(*aggstate).aggcontexts.add(setno as usize);
    }

    /* add check for NULL pointer? */
    if nullcheck {
        (*scratch).opcode = EEOP_AGG_PLAIN_PERGROUP_NULLCHECK as isize;
        (*scratch).d.agg_plain_pergroup_nullcheck.setoff = setoff;
        (*scratch).d.agg_plain_pergroup_nullcheck.jumpnull = -1; /* adjust later */
        ExprEvalPushStep(state, scratch);
        adjust_jumpnull = (*state).steps_len - 1;
    }

    /*
     * Determine appropriate transition implementation.
     */
    if !(*pertrans).aggsortrequired {
        if (*pertrans).transtypeByVal {
            if (*fcinfo).flinfo.as_ref().map(|f| f.fn_strict).unwrap_or(false)
                && (*pertrans).initValueIsNull
            {
                (*scratch).opcode = EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL as isize;
            } else if (*fcinfo).flinfo.as_ref().map(|f| f.fn_strict).unwrap_or(false) {
                (*scratch).opcode = EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL as isize;
            } else {
                (*scratch).opcode = EEOP_AGG_PLAIN_TRANS_BYVAL as isize;
            }
        } else {
            if (*fcinfo).flinfo.as_ref().map(|f| f.fn_strict).unwrap_or(false)
                && (*pertrans).initValueIsNull
            {
                (*scratch).opcode = EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF as isize;
            } else if (*fcinfo).flinfo.as_ref().map(|f| f.fn_strict).unwrap_or(false) {
                (*scratch).opcode = EEOP_AGG_PLAIN_TRANS_STRICT_BYREF as isize;
            } else {
                (*scratch).opcode = EEOP_AGG_PLAIN_TRANS_BYREF as isize;
            }
        }
    } else if (*pertrans).numInputs == 1 {
        (*scratch).opcode = EEOP_AGG_ORDERED_TRANS_DATUM as isize;
    } else {
        (*scratch).opcode = EEOP_AGG_ORDERED_TRANS_TUPLE as isize;
    }

    (*scratch).d.agg_trans.pertrans = pertrans;
    (*scratch).d.agg_trans.setno = setno;
    (*scratch).d.agg_trans.setoff = setoff;
    (*scratch).d.agg_trans.transno = transno;
    (*scratch).d.agg_trans.aggcontext = aggcontext;
    ExprEvalPushStep(state, scratch);

    /* fix up jumpnull */
    if adjust_jumpnull != -1 {
        let as_ = &mut *(*state).steps.add(adjust_jumpnull as usize);
        debug_assert!(as_.opcode == EEOP_AGG_PLAIN_PERGROUP_NULLCHECK as isize);
        debug_assert!(as_.d.agg_plain_pergroup_nullcheck.jumpnull == -1);
        as_.d.agg_plain_pergroup_nullcheck.jumpnull = (*state).steps_len;
    }
}

/*
 * Build an ExprState that calls the given hash function(s) on the attnums
 * given by 'keyColIdx'.
 */
pub unsafe fn ExecBuildHash32FromAttrs(
    desc: TupleDesc,
    ops: *const TupleTableSlotOps,
    hashfunctions: *mut FmgrInfo,
    collations: *mut Oid,
    num_cols: c_int,
    key_col_idx: *mut AttrNumber,
    parent: *mut PlanState,
    init_value: u32,
) -> *mut ExprState {
    let state: *mut ExprState = makeNode!(ExprState, T_ExprState);
    let mut scratch: ExprEvalStep = core::mem::zeroed();
    let mut iresult: *mut NullableDatum = core::ptr::null_mut();
    let mut opcode: isize;
    let mut last_attnum: AttrNumber = 0;

    debug_assert!(num_cols >= 0);

    (*state).parent = parent;

    /*
     * Make a place to store intermediate hash values between subsequent
     * hashing of individual columns.
     */
    if (num_cols as i64) + (init_value != 0) as i64 > 1 {
        iresult = palloc(core::mem::size_of::<NullableDatum>()) as *mut NullableDatum;
    }

    /* find the highest attnum so we deform the tuple to that point */
    for i in 0..num_cols {
        let k = *key_col_idx.add(i as usize);
        if k > last_attnum {
            last_attnum = k;
        }
    }

    scratch.opcode = EEOP_INNER_FETCHSOME as isize;
    scratch.d.fetch.last_var = last_attnum as c_int;
    scratch.d.fetch.fixed = false;
    scratch.d.fetch.kind = ops;
    scratch.d.fetch.known_desc = desc;
    if ExecComputeSlotInfo(state, &mut scratch) {
        ExprEvalPushStep(state, &scratch);
    }

    if init_value == 0 {
        opcode = EEOP_HASHDATUM_FIRST as isize;
    } else {
        scratch.opcode = EEOP_HASHDATUM_SET_INITVAL as isize;
        scratch.d.hashdatum_initvalue.init_value = UInt32GetDatum(init_value);
        scratch.resvalue = if num_cols > 0 {
            &mut (*iresult).value
        } else {
            &mut (*state).resvalue
        };
        scratch.resnull = if num_cols > 0 {
            &mut (*iresult).isnull
        } else {
            &mut (*state).resnull
        };

        ExprEvalPushStep(state, &scratch);
        opcode = EEOP_HASHDATUM_NEXT32 as isize;
    }

    for i in 0..num_cols {
        let finfo: *mut FmgrInfo = hashfunctions.add(i as usize);
        let fcinfo: FunctionCallInfo =
            palloc0(SizeForFunctionCallInfo(1)) as FunctionCallInfo;
        let inputcollid: Oid = *collations.add(i as usize);
        let attnum: AttrNumber = *key_col_idx.add(i as usize) - 1;

        /* Initialize function call parameter structure too */
        InitFunctionCallInfoData(fcinfo, finfo, 1, inputcollid, core::ptr::null_mut(), core::ptr::null_mut());

        /*
         * Fetch inner Var for this attnum and store it in the 1st arg of the hash func.
         */
        scratch.opcode = EEOP_INNER_VAR as isize;
        scratch.resvalue = &mut (*(*fcinfo).args.as_mut_ptr().add(0)).value;
        scratch.resnull = &mut (*(*fcinfo).args.as_mut_ptr().add(0)).isnull;
        scratch.d.var.attnum = attnum as c_int;
        scratch.d.var.vartype = (*TupleDescAttr(desc, attnum as c_int)).atttypid;
        scratch.d.var.varreturningtype = VarReturningType::VAR_RETURNING_DEFAULT;

        ExprEvalPushStep(state, &scratch);

        /* Call the hash function */
        scratch.opcode = opcode;

        if i == num_cols - 1 {
            scratch.resvalue = &mut (*state).resvalue;
            scratch.resnull = &mut (*state).resnull;
        } else {
            debug_assert!(!iresult.is_null());
            scratch.resvalue = &mut (*iresult).value;
            scratch.resnull = &mut (*iresult).isnull;
        }

        scratch.d.hashdatum.iresult = iresult;
        scratch.d.hashdatum.finfo = finfo;
        scratch.d.hashdatum.fcinfo_data = fcinfo;
        scratch.d.hashdatum.fn_addr = (*finfo).fn_addr.unwrap();
        scratch.d.hashdatum.jumpdone = -1;

        ExprEvalPushStep(state, &scratch);

        /* subsequent attnums must be combined with the previous */
        opcode = EEOP_HASHDATUM_NEXT32 as isize;
    }

    scratch.resvalue = core::ptr::null_mut();
    scratch.resnull = core::ptr::null_mut();
    scratch.opcode = EEOP_DONE_RETURN as isize;
    ExprEvalPushStep(state, &scratch);

    ExecReadyExpr(state);

    state
}

/*
 * Build an ExprState that calls the given hash function(s) on the given
 * 'hash_exprs'.
 */
pub unsafe fn ExecBuildHash32Expr(
    desc: TupleDesc,
    ops: *const TupleTableSlotOps,
    hashfunc_oids: *const Oid,
    collations: *const List,
    hash_exprs: *const List,
    opstrict: *const bool,
    parent: *mut PlanState,
    init_value: u32,
    keep_nulls: bool,
) -> *mut ExprState {
    let state: *mut ExprState = makeNode!(ExprState, T_ExprState);
    let mut scratch: ExprEvalStep = core::mem::zeroed();
    let mut iresult: *mut NullableDatum = core::ptr::null_mut();
    let mut adjust_jumps: *mut List = NIL;
    let mut strict_opcode: isize;
    let mut opcode: isize;
    let num_exprs = crate::nodes::pg_list::list_length(hash_exprs as *mut List);

    debug_assert!(num_exprs == crate::nodes::pg_list::list_length(collations as *mut List));

    (*state).parent = parent;

    /* Insert setup steps as needed. */
    ExecCreateExprSetupSteps(state, hash_exprs as *mut Node);

    /*
     * Make a place to store intermediate hash values.
     */
    if (num_exprs as i64) + (init_value != 0) as i64 > 1 {
        iresult = palloc(core::mem::size_of::<NullableDatum>()) as *mut NullableDatum;
    }

    if init_value == 0 {
        strict_opcode = EEOP_HASHDATUM_FIRST_STRICT as isize;
        opcode = EEOP_HASHDATUM_FIRST as isize;
    } else {
        scratch.opcode = EEOP_HASHDATUM_SET_INITVAL as isize;
        scratch.d.hashdatum_initvalue.init_value = UInt32GetDatum(init_value);
        scratch.resvalue = if num_exprs > 0 {
            &mut (*iresult).value
        } else {
            &mut (*state).resvalue
        };
        scratch.resnull = if num_exprs > 0 {
            &mut (*iresult).isnull
        } else {
            &mut (*state).resnull
        };

        ExprEvalPushStep(state, &scratch);

        strict_opcode = EEOP_HASHDATUM_NEXT32_STRICT as isize;
        opcode = EEOP_HASHDATUM_NEXT32 as isize;
    }

    let mut lc = crate::nodes::pg_list::list_head(hash_exprs as *mut List);
    let mut lc2 = crate::nodes::pg_list::list_head(collations as *mut List);
    let mut i: c_int = 0;
    while !lc.is_null() {
        let expr = *(lc as *mut *mut crate::nodes::primnodes::Expr);
        let finfo: *mut FmgrInfo = palloc0(core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
        let fcinfo: FunctionCallInfo =
            palloc0(SizeForFunctionCallInfo(1)) as FunctionCallInfo;
        let funcid: Oid = *hashfunc_oids.add(i as usize);
        let inputcollid: Oid = crate::nodes::pg_list::lfirst_oid(lc2);

        fmgr_info(funcid, finfo);

        /*
         * Build steps to evaluate the hash function's argument.
         */
        ExecInitExprRec(
            expr,
            state,
            &mut (*(*fcinfo).args.as_mut_ptr().add(0)).value,
            &mut (*(*fcinfo).args.as_mut_ptr().add(0)).isnull,
        );

        if i == num_exprs - 1 {
            scratch.resvalue = &mut (*state).resvalue;
            scratch.resnull = &mut (*state).resnull;
        } else {
            debug_assert!(!iresult.is_null());
            scratch.resvalue = &mut (*iresult).value;
            scratch.resnull = &mut (*iresult).isnull;
        }

        scratch.d.hashdatum.iresult = iresult;

        /* Initialize function call parameter structure too */
        InitFunctionCallInfoData(fcinfo, finfo, 1, inputcollid, core::ptr::null_mut(), core::ptr::null_mut());

        scratch.d.hashdatum.finfo = finfo;
        scratch.d.hashdatum.fcinfo_data = fcinfo;
        scratch.d.hashdatum.fn_addr = (*finfo).fn_addr.unwrap();

        scratch.opcode = if *opstrict.add(i as usize) && !keep_nulls {
            strict_opcode
        } else {
            opcode
        };
        scratch.d.hashdatum.jumpdone = -1;

        ExprEvalPushStep(state, &scratch);
        adjust_jumps = lappend_int(adjust_jumps, (*state).steps_len - 1);

        /* For subsequent keys combine the hash value with the previous */
        strict_opcode = EEOP_HASHDATUM_NEXT32_STRICT as isize;
        opcode = EEOP_HASHDATUM_NEXT32 as isize;

        i += 1;
        lc = crate::nodes::pg_list::lnext(hash_exprs as *mut List, lc);
        lc2 = crate::nodes::pg_list::lnext(collations as *mut List, lc2);
    }

    /* adjust jump targets */
    let mut lc3 = crate::nodes::pg_list::list_head(adjust_jumps);
    while !lc3.is_null() {
        let j = crate::nodes::pg_list::lfirst_int(lc3);
        let as_ = &mut *(*state).steps.add(j as usize);

        debug_assert!(
            as_.opcode == EEOP_HASHDATUM_FIRST as isize
                || as_.opcode == EEOP_HASHDATUM_FIRST_STRICT as isize
                || as_.opcode == EEOP_HASHDATUM_NEXT32 as isize
                || as_.opcode == EEOP_HASHDATUM_NEXT32_STRICT as isize
        );
        debug_assert!(as_.d.hashdatum.jumpdone == -1);
        as_.d.hashdatum.jumpdone = (*state).steps_len;
        lc3 = crate::nodes::pg_list::lnext(adjust_jumps, lc3);
    }

    scratch.resvalue = core::ptr::null_mut();
    scratch.resnull = core::ptr::null_mut();
    scratch.opcode = EEOP_DONE_RETURN as isize;
    ExprEvalPushStep(state, &scratch);

    ExecReadyExpr(state);

    state
}

/*
 * Build equality expression that can be evaluated using ExecQual(), returning
 * true if the expression context's inner/outer tuple are NOT DISTINCT.
 */
pub unsafe fn ExecBuildGroupingEqual(
    ldesc: TupleDesc,
    rdesc: TupleDesc,
    lops: *const TupleTableSlotOps,
    rops: *const TupleTableSlotOps,
    num_cols: c_int,
    key_col_idx: *const AttrNumber,
    eq_functions: *const Oid,
    collations: *const Oid,
    parent: *mut PlanState,
) -> *mut ExprState {
    let state: *mut ExprState = makeNode!(ExprState, T_ExprState);
    let mut scratch: ExprEvalStep = core::mem::zeroed();
    let mut maxatt: c_int = -1;
    let mut adjust_jumps: *mut List = NIL;

    /*
     * When no columns are actually compared, the result's always true.
     */
    if num_cols == 0 {
        return core::ptr::null_mut();
    }

    (*state).expr = core::ptr::null_mut();
    (*state).flags = EEO_FLAG_IS_QUAL;
    (*state).parent = parent;

    scratch.resvalue = &mut (*state).resvalue;
    scratch.resnull = &mut (*state).resnull;

    /* compute max needed attribute */
    for natt in 0..num_cols {
        let attno = *key_col_idx.add(natt as usize) as c_int;
        if attno > maxatt {
            maxatt = attno;
        }
    }
    debug_assert!(maxatt >= 0);

    /* push deform steps */
    scratch.opcode = EEOP_INNER_FETCHSOME as isize;
    scratch.d.fetch.last_var = maxatt as c_int;
    scratch.d.fetch.fixed = false;
    scratch.d.fetch.known_desc = ldesc;
    scratch.d.fetch.kind = lops;
    if ExecComputeSlotInfo(state, &mut scratch) {
        ExprEvalPushStep(state, &scratch);
    }

    scratch.opcode = EEOP_OUTER_FETCHSOME as isize;
    scratch.d.fetch.last_var = maxatt as c_int;
    scratch.d.fetch.fixed = false;
    scratch.d.fetch.known_desc = rdesc;
    scratch.d.fetch.kind = rops;
    if ExecComputeSlotInfo(state, &mut scratch) {
        ExprEvalPushStep(state, &scratch);
    }

    /*
     * Start comparing at the last field (least significant sort key).
     */
    for natt in (0..num_cols).rev() {
        let attno = *key_col_idx.add(natt as usize);
        let latt = TupleDescAttr(ldesc, (attno - 1) as c_int);
        let ratt = TupleDescAttr(rdesc, (attno - 1) as c_int);
        let foid = *eq_functions.add(natt as usize);
        let collid = *collations.add(natt as usize);
        let finfo: *mut FmgrInfo = palloc0(core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
        let fcinfo: FunctionCallInfo =
            palloc0(SizeForFunctionCallInfo(2)) as FunctionCallInfo;
        let aclresult: AclResult;

        /* Check permission to call function */
        aclresult = object_aclcheck(ProcedureRelationId, foid, GetUserId(), 0);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(foid));
        }
        InvokeFunctionExecuteHook(foid);

        fmgr_info(foid, finfo);
        fmgr_info_set_expr(core::ptr::null_mut(), finfo);
        InitFunctionCallInfoData(fcinfo, finfo, 2, collid, core::ptr::null_mut(), core::ptr::null_mut());

        /* left arg */
        scratch.opcode = EEOP_INNER_VAR as isize;
        scratch.d.var.attnum = (attno - 1) as c_int;
        scratch.d.var.vartype = (*latt).atttypid;
        scratch.d.var.varreturningtype = VarReturningType::VAR_RETURNING_DEFAULT;
        scratch.resvalue = &mut (*(*fcinfo).args.as_mut_ptr().add(0)).value;
        scratch.resnull = &mut (*(*fcinfo).args.as_mut_ptr().add(0)).isnull;
        ExprEvalPushStep(state, &scratch);

        /* right arg */
        scratch.opcode = EEOP_OUTER_VAR as isize;
        scratch.d.var.attnum = (attno - 1) as c_int;
        scratch.d.var.vartype = (*ratt).atttypid;
        scratch.d.var.varreturningtype = VarReturningType::VAR_RETURNING_DEFAULT;
        scratch.resvalue = &mut (*(*fcinfo).args.as_mut_ptr().add(1)).value;
        scratch.resnull = &mut (*(*fcinfo).args.as_mut_ptr().add(1)).isnull;
        ExprEvalPushStep(state, &scratch);

        /* evaluate distinctness */
        scratch.opcode = EEOP_NOT_DISTINCT as isize;
        scratch.d.func.finfo = finfo;
        scratch.d.func.fcinfo_data = fcinfo;
        scratch.d.func.fn_addr = (*finfo).fn_addr.unwrap();
        scratch.d.func.nargs = 2;
        scratch.resvalue = &mut (*state).resvalue;
        scratch.resnull = &mut (*state).resnull;
        ExprEvalPushStep(state, &scratch);

        /* then emit EEOP_QUAL to detect if result is false (or null) */
        scratch.opcode = EEOP_QUAL as isize;
        scratch.d.qualexpr.jumpdone = -1;
        scratch.resvalue = &mut (*state).resvalue;
        scratch.resnull = &mut (*state).resnull;
        ExprEvalPushStep(state, &scratch);
        adjust_jumps = lappend_int(adjust_jumps, (*state).steps_len - 1);
    }

    /* adjust jump targets */
    let mut lc = crate::nodes::pg_list::list_head(adjust_jumps);
    while !lc.is_null() {
        let j = crate::nodes::pg_list::lfirst_int(lc);
        let as_ = &mut *(*state).steps.add(j as usize);
        debug_assert!(as_.opcode == EEOP_QUAL as isize);
        debug_assert!(as_.d.qualexpr.jumpdone == -1);
        as_.d.qualexpr.jumpdone = (*state).steps_len;
        lc = crate::nodes::pg_list::lnext(adjust_jumps, lc);
    }

    scratch.resvalue = core::ptr::null_mut();
    scratch.resnull = core::ptr::null_mut();
    scratch.opcode = EEOP_DONE_RETURN as isize;
    ExprEvalPushStep(state, &scratch);

    ExecReadyExpr(state);

    state
}

/*
 * Build equality expression that can be evaluated using ExecQual(), returning
 * true if the expression context's inner/outer tuples are equal.
 */
pub unsafe fn ExecBuildParamSetEqual(
    desc: TupleDesc,
    lops: *const TupleTableSlotOps,
    rops: *const TupleTableSlotOps,
    eq_functions: *const Oid,
    collations: *const Oid,
    param_exprs: *const List,
    parent: *mut PlanState,
) -> *mut ExprState {
    let state: *mut ExprState = makeNode!(ExprState, T_ExprState);
    let mut scratch: ExprEvalStep = core::mem::zeroed();
    let maxatt = crate::nodes::pg_list::list_length(param_exprs as *mut List);
    let mut adjust_jumps: *mut List = NIL;

    (*state).expr = core::ptr::null_mut();
    (*state).flags = EEO_FLAG_IS_QUAL;
    (*state).parent = parent;

    scratch.resvalue = &mut (*state).resvalue;
    scratch.resnull = &mut (*state).resnull;

    /* push deform steps */
    scratch.opcode = EEOP_INNER_FETCHSOME as isize;
    scratch.d.fetch.last_var = maxatt as c_int;
    scratch.d.fetch.fixed = false;
    scratch.d.fetch.known_desc = desc;
    scratch.d.fetch.kind = lops;
    if ExecComputeSlotInfo(state, &mut scratch) {
        ExprEvalPushStep(state, &scratch);
    }

    scratch.opcode = EEOP_OUTER_FETCHSOME as isize;
    scratch.d.fetch.last_var = maxatt as c_int;
    scratch.d.fetch.fixed = false;
    scratch.d.fetch.known_desc = desc;
    scratch.d.fetch.kind = rops;
    if ExecComputeSlotInfo(state, &mut scratch) {
        ExprEvalPushStep(state, &scratch);
    }

    for attno in 0..maxatt {
        let att = TupleDescAttr(desc, attno as c_int);
        let foid = *eq_functions.add(attno as usize);
        let collid = *collations.add(attno as usize);
        let finfo: *mut FmgrInfo = palloc0(core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
        let fcinfo: FunctionCallInfo =
            palloc0(SizeForFunctionCallInfo(2)) as FunctionCallInfo;
        let aclresult: AclResult;

        /* Check permission to call function */
        aclresult = object_aclcheck(ProcedureRelationId, foid, GetUserId(), 0);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(foid));
        }
        InvokeFunctionExecuteHook(foid);

        fmgr_info(foid, finfo);
        fmgr_info_set_expr(core::ptr::null_mut(), finfo);
        InitFunctionCallInfoData(fcinfo, finfo, 2, collid, core::ptr::null_mut(), core::ptr::null_mut());

        /* left arg */
        scratch.opcode = EEOP_INNER_VAR as isize;
        scratch.d.var.attnum = attno as c_int;
        scratch.d.var.vartype = (*att).atttypid;
        scratch.d.var.varreturningtype = VarReturningType::VAR_RETURNING_DEFAULT;
        scratch.resvalue = &mut (*(*fcinfo).args.as_mut_ptr().add(0)).value;
        scratch.resnull = &mut (*(*fcinfo).args.as_mut_ptr().add(0)).isnull;
        ExprEvalPushStep(state, &scratch);

        /* right arg */
        scratch.opcode = EEOP_OUTER_VAR as isize;
        scratch.d.var.attnum = attno as c_int;
        scratch.d.var.vartype = (*att).atttypid;
        scratch.d.var.varreturningtype = VarReturningType::VAR_RETURNING_DEFAULT;
        scratch.resvalue = &mut (*(*fcinfo).args.as_mut_ptr().add(1)).value;
        scratch.resnull = &mut (*(*fcinfo).args.as_mut_ptr().add(1)).isnull;
        ExprEvalPushStep(state, &scratch);

        /* evaluate distinctness */
        scratch.opcode = EEOP_NOT_DISTINCT as isize;
        scratch.d.func.finfo = finfo;
        scratch.d.func.fcinfo_data = fcinfo;
        scratch.d.func.fn_addr = (*finfo).fn_addr.unwrap();
        scratch.d.func.nargs = 2;
        scratch.resvalue = &mut (*state).resvalue;
        scratch.resnull = &mut (*state).resnull;
        ExprEvalPushStep(state, &scratch);

        /* then emit EEOP_QUAL to detect if result is false (or null) */
        scratch.opcode = EEOP_QUAL as isize;
        scratch.d.qualexpr.jumpdone = -1;
        scratch.resvalue = &mut (*state).resvalue;
        scratch.resnull = &mut (*state).resnull;
        ExprEvalPushStep(state, &scratch);
        adjust_jumps = lappend_int(adjust_jumps, (*state).steps_len - 1);
    }

    /* adjust jump targets */
    let mut lc = crate::nodes::pg_list::list_head(adjust_jumps);
    while !lc.is_null() {
        let j = crate::nodes::pg_list::lfirst_int(lc);
        let as_ = &mut *(*state).steps.add(j as usize);
        debug_assert!(as_.opcode == EEOP_QUAL as isize);
        debug_assert!(as_.d.qualexpr.jumpdone == -1);
        as_.d.qualexpr.jumpdone = (*state).steps_len;
        lc = crate::nodes::pg_list::lnext(adjust_jumps, lc);
    }

    scratch.resvalue = core::ptr::null_mut();
    scratch.resnull = core::ptr::null_mut();
    scratch.opcode = EEOP_DONE_RETURN as isize;
    ExprEvalPushStep(state, &scratch);

    ExecReadyExpr(state);

    state
}

/*
 * Push steps to evaluate a JsonExpr and its various subsidiary expressions.
 */
unsafe fn ExecInitJsonExpr(
    jsexpr: *mut JsonExpr,
    state: *mut ExprState,
    resv: *mut Datum,
    resnull: *mut bool,
    scratch: *mut ExprEvalStep,
) {
    let jsestate: *mut JsonExprState =
        palloc0(core::mem::size_of::<JsonExprState>()) as *mut JsonExprState;
    let mut jumps_return_null: *mut List = NIL;
    let mut jumps_to_end: *mut List = NIL;
    let returning_domain =
        get_typtype((*(*jsexpr).returning).typid) == TYPTYPE_DOMAIN;

    debug_assert!(!(*jsexpr).on_error.is_null());

    (*jsestate).jsexpr = jsexpr;

    /*
     * Evaluate formatted_expr storing the result into
     * jsestate->formatted_expr.
     */
    ExecInitExprRec(
        (*jsexpr).formatted_expr as *mut _,
        state,
        &mut (*jsestate).formatted_expr.value,
        &mut (*jsestate).formatted_expr.isnull,
    );

    /* JUMP to return NULL if formatted_expr evaluates to NULL */
    jumps_return_null = lappend_int(jumps_return_null, (*state).steps_len);
    (*scratch).opcode = EEOP_JUMP_IF_NULL as isize;
    (*scratch).resnull = &mut (*jsestate).formatted_expr.isnull;
    (*scratch).d.jump.jumpdone = -1;
    ExprEvalPushStep(state, scratch);

    /*
     * Evaluate pathspec expression.
     */
    ExecInitExprRec(
        (*jsexpr).path_spec as *mut _,
        state,
        &mut (*jsestate).pathspec.value,
        &mut (*jsestate).pathspec.isnull,
    );

    /* JUMP to return NULL if path_spec evaluates to NULL */
    jumps_return_null = lappend_int(jumps_return_null, (*state).steps_len);
    (*scratch).opcode = EEOP_JUMP_IF_NULL as isize;
    (*scratch).resnull = &mut (*jsestate).pathspec.isnull;
    (*scratch).d.jump.jumpdone = -1;
    ExprEvalPushStep(state, scratch);

    /* Steps to compute PASSING args. */
    (*jsestate).args = NIL;
    let mut argexprlc = crate::nodes::pg_list::list_head((*jsexpr).passing_values);
    let mut argnamelc = crate::nodes::pg_list::list_head((*jsexpr).passing_names);
    while !argexprlc.is_null() {
        let argexpr = *(argexprlc as *mut *mut crate::nodes::primnodes::Expr);
        let argname = *(argnamelc as *mut *mut crate::nodes::value::String);
        let var: *mut JsonPathVariable =
            palloc(core::mem::size_of::<JsonPathVariable>()) as *mut JsonPathVariable;

        (*var).name = (*argname).sval;
        (*var).namelen = core::ffi::CStr::from_ptr((*var).name).to_bytes().len();
        (*var).typid = exprType(argexpr as *const Node);
        (*var).typmod = exprTypmod(argexpr as *const Node);

        ExecInitExprRec(argexpr, state, &mut (*var).value, &mut (*var).isnull);

        (*jsestate).args = lappend((*jsestate).args, var as *mut c_void);

        argexprlc = crate::nodes::pg_list::lnext((*jsexpr).passing_values, argexprlc);
        argnamelc = crate::nodes::pg_list::lnext((*jsexpr).passing_names, argnamelc);
    }

    /* Step for jsonpath evaluation */
    (*scratch).opcode = EEOP_JSONEXPR_PATH as isize;
    (*scratch).resvalue = resv;
    (*scratch).resnull = resnull;
    (*scratch).d.jsonexpr.jsestate = jsestate;
    ExprEvalPushStep(state, scratch);

    /*
     * Step to return NULL after jumping to skip the EEOP_JSONEXPR_PATH step
     * when either formatted_expr or pathspec is NULL.
     */
    let mut lc = crate::nodes::pg_list::list_head(jumps_return_null);
    while !lc.is_null() {
        let j = crate::nodes::pg_list::lfirst_int(lc);
        let as_ = &mut *(*state).steps.add(j as usize);
        as_.d.jump.jumpdone = (*state).steps_len;
        lc = crate::nodes::pg_list::lnext(jumps_return_null, lc);
    }
    (*scratch).opcode = EEOP_CONST as isize;
    (*scratch).resvalue = resv;
    (*scratch).resnull = resnull;
    (*scratch).d.constval.value = Datum::from(0usize);
    (*scratch).d.constval.isnull = true;
    ExprEvalPushStep(state, scratch);

    let escontext: *mut ErrorSaveContext = if (*(*jsexpr).on_error).btype != JSON_BEHAVIOR_ERROR {
        &mut (*jsestate).escontext
    } else {
        core::ptr::null_mut()
    };

    /*
     * To handle coercion errors softly, use the following ErrorSaveContext.
     */
    (*jsestate).escontext.r#type = NodeTag::T_ErrorSaveContext;

    /*
     * Steps to coerce the result value.
     */
    (*jsestate).jump_eval_coercion = -1;
    if (*jsexpr).use_json_coercion {
        (*jsestate).jump_eval_coercion = (*state).steps_len;

        ExecInitJsonCoercion(
            state,
            (*jsexpr).returning,
            escontext,
            (*jsexpr).omit_quotes,
            (*jsexpr).op == JSON_EXISTS_OP,
            resv,
            resnull,
        );
    } else if (*jsexpr).use_io_coercion {
        let mut typinput: Oid = InvalidOid;
        let mut typioparam: Oid = InvalidOid;
        let finfo: *mut FmgrInfo = palloc0(core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
        let fcinfo: FunctionCallInfo =
            palloc0(SizeForFunctionCallInfo(3)) as FunctionCallInfo;

        getTypeInputInfo((*(*jsexpr).returning).typid, &mut typinput, &mut typioparam);
        fmgr_info(typinput, finfo);
        fmgr_info_set_expr((*jsexpr).returning as *mut Node, finfo);
        InitFunctionCallInfoData(
            fcinfo, finfo, 3, InvalidOid, core::ptr::null_mut(), core::ptr::null_mut(),
        );

        /*
         * We can preload the second and third arguments for the input
         * function, since they're constants.
         */
        (*(*fcinfo).args.as_mut_ptr().add(1)).value = ObjectIdGetDatum(typioparam);
        (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
        (*(*fcinfo).args.as_mut_ptr().add(2)).value = Int32GetDatum((*(*jsexpr).returning).typmod);
        (*(*fcinfo).args.as_mut_ptr().add(2)).isnull = false;
        (*fcinfo).context = escontext as *mut Node;

        (*jsestate).input_fcinfo = fcinfo;
    }

    /*
     * Add a special step, if needed, to check if the coercion evaluation ran
     * into an error.
     */
    if (*jsestate).jump_eval_coercion >= 0 && !escontext.is_null() {
        (*scratch).opcode = EEOP_JSONEXPR_COERCION_FINISH as isize;
        (*scratch).d.jsonexpr.jsestate = jsestate;
        ExprEvalPushStep(state, scratch);
    }

    (*jsestate).jump_empty = -1;
    (*jsestate).jump_error = -1;

    /*
     * Step to check jsestate->error and return the ON ERROR expression.
     */
    if (*(*jsexpr).on_error).btype != JSON_BEHAVIOR_ERROR
        && (!IsA!((*(*jsexpr).on_error).expr, T_Const)
            || !(*((*(*jsexpr).on_error).expr as *mut crate::nodes::primnodes::Const)).constisnull
            || returning_domain)
    {
        let saved_escontext: *mut ErrorSaveContext;

        (*jsestate).jump_error = (*state).steps_len;

        /* JUMP to end if false, skip the ON ERROR expression. */
        jumps_to_end = lappend_int(jumps_to_end, (*state).steps_len);
        (*scratch).opcode = EEOP_JUMP_IF_NOT_TRUE as isize;
        (*scratch).resvalue = &mut (*jsestate).error.value;
        (*scratch).resnull = &mut (*jsestate).error.isnull;
        (*scratch).d.jump.jumpdone = -1;
        ExprEvalPushStep(state, scratch);

        /* Steps to evaluate the ON ERROR expression */
        saved_escontext = (*state).escontext;
        (*state).escontext = escontext;
        ExecInitExprRec((*(*jsexpr).on_error).expr as *mut _, state, resv, resnull);
        (*state).escontext = saved_escontext;

        /* Step to coerce the ON ERROR expression if needed */
        if (*(*jsexpr).on_error).coerce {
            ExecInitJsonCoercion(
                state,
                (*jsexpr).returning,
                escontext,
                (*jsexpr).omit_quotes,
                false,
                resv,
                resnull,
            );
        }

        /*
         * Add a COERCION_FINISH step to check for errors that may occur when
         * coercing and rethrow them.
         */
        if (*(*jsexpr).on_error).coerce
            || IsA!((*(*jsexpr).on_error).expr, T_CoerceViaIO)
            || IsA!((*(*jsexpr).on_error).expr, T_CoerceToDomain)
        {
            (*scratch).opcode = EEOP_JSONEXPR_COERCION_FINISH as isize;
            (*scratch).resvalue = resv;
            (*scratch).resnull = resnull;
            (*scratch).d.jsonexpr.jsestate = jsestate;
            ExprEvalPushStep(state, scratch);
        }

        /* JUMP to end to skip the ON EMPTY steps added below. */
        jumps_to_end = lappend_int(jumps_to_end, (*state).steps_len);
        (*scratch).opcode = EEOP_JUMP as isize;
        (*scratch).d.jump.jumpdone = -1;
        ExprEvalPushStep(state, scratch);
    }

    /*
     * Step to check jsestate->empty and return the ON EMPTY expression.
     */
    if !(*jsexpr).on_empty.is_null()
        && (*(*jsexpr).on_empty).btype != JSON_BEHAVIOR_ERROR
        && (!IsA!((*(*jsexpr).on_empty).expr, T_Const)
            || !(*((*(*jsexpr).on_empty).expr as *mut crate::nodes::primnodes::Const)).constisnull
            || returning_domain)
    {
        let saved_escontext: *mut ErrorSaveContext;

        (*jsestate).jump_empty = (*state).steps_len;

        /* JUMP to end if false, skip the ON EMPTY expression. */
        jumps_to_end = lappend_int(jumps_to_end, (*state).steps_len);
        (*scratch).opcode = EEOP_JUMP_IF_NOT_TRUE as isize;
        (*scratch).resvalue = &mut (*jsestate).empty.value;
        (*scratch).resnull = &mut (*jsestate).empty.isnull;
        (*scratch).d.jump.jumpdone = -1;
        ExprEvalPushStep(state, scratch);

        /* Steps to evaluate the ON EMPTY expression */
        saved_escontext = (*state).escontext;
        (*state).escontext = escontext;
        ExecInitExprRec((*(*jsexpr).on_empty).expr as *mut _, state, resv, resnull);
        (*state).escontext = saved_escontext;

        /* Step to coerce the ON EMPTY expression if needed */
        if (*(*jsexpr).on_empty).coerce {
            ExecInitJsonCoercion(
                state,
                (*jsexpr).returning,
                escontext,
                (*jsexpr).omit_quotes,
                false,
                resv,
                resnull,
            );
        }

        if (*(*jsexpr).on_empty).coerce
            || IsA!((*(*jsexpr).on_empty).expr, T_CoerceViaIO)
            || IsA!((*(*jsexpr).on_empty).expr, T_CoerceToDomain)
        {
            (*scratch).opcode = EEOP_JSONEXPR_COERCION_FINISH as isize;
            (*scratch).resvalue = resv;
            (*scratch).resnull = resnull;
            (*scratch).d.jsonexpr.jsestate = jsestate;
            ExprEvalPushStep(state, scratch);
        }
    }

    let mut lc2 = crate::nodes::pg_list::list_head(jumps_to_end);
    while !lc2.is_null() {
        let j = crate::nodes::pg_list::lfirst_int(lc2);
        let as_ = &mut *(*state).steps.add(j as usize);
        as_.d.jump.jumpdone = (*state).steps_len;
        lc2 = crate::nodes::pg_list::lnext(jumps_to_end, lc2);
    }

    (*jsestate).jump_end = (*state).steps_len;
}

/*
 * Initialize a EEOP_JSONEXPR_COERCION step to coerce the value given in resv
 * to the given RETURNING type.
 */
unsafe fn ExecInitJsonCoercion(
    state: *mut ExprState,
    returning: *mut crate::nodes::primnodes::JsonReturning,
    escontext: *mut ErrorSaveContext,
    omit_quotes: bool,
    exists_coerce: bool,
    resv: *mut Datum,
    resnull: *mut bool,
) {
    let mut scratch: ExprEvalStep = core::mem::zeroed();

    /* For json_populate_type() */
    scratch.opcode = EEOP_JSONEXPR_COERCION as isize;
    scratch.resvalue = resv;
    scratch.resnull = resnull;
    scratch.d.jsonexpr_coercion.targettype = (*returning).typid;
    scratch.d.jsonexpr_coercion.targettypmod = (*returning).typmod;
    scratch.d.jsonexpr_coercion.json_coercion_cache = core::ptr::null_mut();
    scratch.d.jsonexpr_coercion.escontext = escontext;
    scratch.d.jsonexpr_coercion.omit_quotes = omit_quotes;
    scratch.d.jsonexpr_coercion.exists_coerce = exists_coerce;
    scratch.d.jsonexpr_coercion.exists_cast_to_int =
        exists_coerce && getBaseType((*returning).typid) == INT4OID;
    scratch.d.jsonexpr_coercion.exists_check_domain =
        exists_coerce && DomainHasConstraints((*returning).typid);
    ExprEvalPushStep(state, &scratch);
}
