//! Translated from PostgreSQL src/include/executor/execExpr.h
//! Low level infrastructure related to expression evaluation.
//!
//! In-memory only (interpreter steps): idiomatic Rust, no layout contract.

#![allow(non_snake_case, non_camel_case_types, deprecated)]

use crate::postgres::{Datum, NullableDatum};
use crate::postgres_ext::Oid;

use crate::access::cmptype::CompareType;
use crate::fmgr::{FmgrInfo, PGFunction};
use crate::nodes::execnodes::{
    EeoFlag, ExprContext, ExprState, JsonExprState, SubPlanState, WindowFuncExprState,
};
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::nodes::primnodes::{
    FieldStore, JsonConstructorExpr, JsonIsPredicate, MinMaxOp, ScalarArrayOpExpr,
    SQLValueFunction, Var, VarReturningType, XmlExpr,
};
use crate::access::tupdesc::TupleDesc;
use crate::access::tupconvert::TupleConversionMap;
use crate::executor::tuptable::{TupleTableSlot, TupleTableSlotOps};

// ---------------------------------------------------------------------------
// Private ExprState flag bits
// ---------------------------------------------------------------------------
// The public EEO_FLAG_* bits live in nodes/execnodes.rs (`EeoFlag`). These two
// are private to the interpreter; bitflags! cannot be reopened, so expose them
// as associated consts on the same type.
impl EeoFlag {
    /// expression's interpreter has been initialized.
    pub const INTERPRETER_INITIALIZED: Self = Self::from_bits_retain(1 << 5);
    /// jump-threading is in use.
    pub const DIRECT_THREADED: Self = Self::from_bits_retain(1 << 6);
}

// ---------------------------------------------------------------------------
// Out-of-line evaluation subroutine signatures
// ---------------------------------------------------------------------------

/// Typical API for out-of-line evaluation subroutines.
pub type ExecEvalSubroutine =
    fn(state: &mut ExprState, op: &mut ExprEvalStep, econtext: &mut ExprContext);

/// API for out-of-line evaluation subroutines returning bool.
pub type ExecEvalBoolSubroutine =
    fn(state: &mut ExprState, op: &mut ExprEvalStep, econtext: &mut ExprContext) -> bool;

/// ExprEvalSteps that cache a composite type's tupdesc need one of these.
#[derive(Default)]
pub struct ExprEvalRowtypeCache {
    /// TypeCacheEntry ptr (when tupdesc_id != 0), or a cached tupdesc directly
    /// for an anonymous RECORD type. C `void *`; None initially.
    pub cacheptr: crate::nodes::execnodes::OpaqueState,
    /// last-seen tupdesc identifier, or 0.
    pub tupdesc_id: u64,
}

// ---------------------------------------------------------------------------
// ExprEvalOp - discriminator for ExprEvalSteps
// ---------------------------------------------------------------------------
// Sequential ordinals kept in sync with execExprInterp.c's dispatch_table[].
// (Bitflags-port appendix D: sequential opcodes -> enum.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ExprEvalOp {
    /// entire expression has been evaluated, return value
    DONE_RETURN,
    /// entire expression has been evaluated, no return value
    DONE_NO_RETURN,

    /// apply slot_getsomeattrs on corresponding tuple slot
    INNER_FETCHSOME,
    OUTER_FETCHSOME,
    SCAN_FETCHSOME,
    OLD_FETCHSOME,
    NEW_FETCHSOME,

    /// compute non-system Var value
    INNER_VAR,
    OUTER_VAR,
    SCAN_VAR,
    OLD_VAR,
    NEW_VAR,

    /// compute system Var value
    INNER_SYSVAR,
    OUTER_SYSVAR,
    SCAN_SYSVAR,
    OLD_SYSVAR,
    NEW_SYSVAR,

    /// compute wholerow Var
    WHOLEROW,

    /// compute non-system Var value, assign into resultslot
    ASSIGN_INNER_VAR,
    ASSIGN_OUTER_VAR,
    ASSIGN_SCAN_VAR,
    ASSIGN_OLD_VAR,
    ASSIGN_NEW_VAR,

    /// assign resvalue/resnull to a column of resultslot
    ASSIGN_TMP,
    /// ditto, applying MakeExpandedObjectReadOnly()
    ASSIGN_TMP_MAKE_RO,

    /// evaluate Const value
    CONST,

    /// evaluate function call (including OpExprs etc)
    FUNCEXPR,
    FUNCEXPR_STRICT,
    FUNCEXPR_STRICT_1,
    FUNCEXPR_STRICT_2,
    FUNCEXPR_FUSAGE,
    FUNCEXPR_STRICT_FUSAGE,

    /// boolean AND, one step per subexpression
    BOOL_AND_STEP_FIRST,
    BOOL_AND_STEP,
    BOOL_AND_STEP_LAST,

    /// boolean OR
    BOOL_OR_STEP_FIRST,
    BOOL_OR_STEP,
    BOOL_OR_STEP_LAST,

    /// boolean NOT
    BOOL_NOT_STEP,

    /// simplified BOOL_AND_STEP for ExecQual()
    QUAL,

    /// unconditional jump
    JUMP,

    /// conditional jumps based on current result value
    JUMP_IF_NULL,
    JUMP_IF_NOT_NULL,
    JUMP_IF_NOT_TRUE,

    /// NULL tests for scalar values
    NULLTEST_ISNULL,
    NULLTEST_ISNOTNULL,

    /// NULL tests for row values
    NULLTEST_ROWISNULL,
    NULLTEST_ROWISNOTNULL,

    /// BooleanTest expression
    BOOLTEST_IS_TRUE,
    BOOLTEST_IS_NOT_TRUE,
    BOOLTEST_IS_FALSE,
    BOOLTEST_IS_NOT_FALSE,

    /// EXEC/EXTERN parameters
    PARAM_EXEC,
    PARAM_EXTERN,
    PARAM_CALLBACK,
    /// set EXEC value
    PARAM_SET,

    /// return CaseTestExpr value
    CASE_TESTVAL,
    CASE_TESTVAL_EXT,

    /// apply MakeExpandedObjectReadOnly() to target value
    MAKE_READONLY,

    /// assorted special-purpose expression types
    IOCOERCE,
    IOCOERCE_SAFE,
    DISTINCT,
    NOT_DISTINCT,
    NULLIF,
    SQLVALUEFUNCTION,
    CURRENTOFEXPR,
    NEXTVALUEEXPR,
    RETURNINGEXPR,
    ARRAYEXPR,
    ARRAYCOERCE,
    ROW,

    /// compare two elements of two compared ROW() expressions
    ROWCOMPARE_STEP,
    /// boolean value from previous ROWCOMPARE_STEP operations
    ROWCOMPARE_FINAL,

    /// GREATEST() or LEAST()
    MINMAX,

    /// FieldSelect expression
    FIELDSELECT,
    /// deform tuple before evaluating FieldStore fields
    FIELDSTORE_DEFORM,
    /// form the new tuple for a FieldStore expression
    FIELDSTORE_FORM,

    /// process container subscripts
    SBSREF_SUBSCRIPTS,
    /// compute old container element/slice (SubscriptingRef assignment)
    SBSREF_OLD,
    /// compute new value for SubscriptingRef assignment
    SBSREF_ASSIGN,
    /// compute element/slice for SubscriptingRef fetch
    SBSREF_FETCH,

    /// CoerceToDomainValue
    DOMAIN_TESTVAL,
    DOMAIN_TESTVAL_EXT,
    /// domain NOT NULL constraint
    DOMAIN_NOTNULL,
    /// single domain CHECK constraint
    DOMAIN_CHECK,

    /// hashing
    HASHDATUM_SET_INITVAL,
    HASHDATUM_FIRST,
    HASHDATUM_FIRST_STRICT,
    HASHDATUM_NEXT32,
    HASHDATUM_NEXT32_STRICT,

    /// assorted special-purpose expression types
    CONVERT_ROWTYPE,
    SCALARARRAYOP,
    HASHED_SCALARARRAYOP,
    XMLEXPR,
    JSON_CONSTRUCTOR,
    IS_JSON,
    JSONEXPR_PATH,
    JSONEXPR_COERCION,
    JSONEXPR_COERCION_FINISH,
    AGGREF,
    GROUPING_FUNC,
    WINDOW_FUNC,
    MERGE_SUPPORT_FUNC,
    SUBPLAN,

    /// aggregation related nodes
    AGG_STRICT_DESERIALIZE,
    AGG_DESERIALIZE,
    AGG_STRICT_INPUT_CHECK_ARGS,
    AGG_STRICT_INPUT_CHECK_ARGS_1,
    AGG_STRICT_INPUT_CHECK_NULLS,
    AGG_PLAIN_PERGROUP_NULLCHECK,
    AGG_PLAIN_TRANS_INIT_STRICT_BYVAL,
    AGG_PLAIN_TRANS_STRICT_BYVAL,
    AGG_PLAIN_TRANS_BYVAL,
    AGG_PLAIN_TRANS_INIT_STRICT_BYREF,
    AGG_PLAIN_TRANS_STRICT_BYREF,
    AGG_PLAIN_TRANS_BYREF,
    AGG_PRESORTED_DISTINCT_SINGLE,
    AGG_PRESORTED_DISTINCT_MULTI,
    AGG_ORDERED_TRANS_DATUM,
    AGG_ORDERED_TRANS_TUPLE,

    /// non-existent operation, used e.g. to check array lengths
    LAST,
}

// ---------------------------------------------------------------------------
// ExprEvalStep and its per-op payloads
// ---------------------------------------------------------------------------
// The C `union d` becomes the tagged `ExprEvalStepData` enum below; each former
// union member is a payload struct. `opcode` is `intptr_t` in C (an ExprEvalOp
// that may be replaced by a computed-goto pointer at ready time); modeled here
// as the enum (jump-threading is a later optimization, see DIRECT_THREADED).

pub struct ExprEvalStep {
    /// instruction to be executed.
    pub opcode: ExprEvalOp,
    /// where to store the result of this step (C `Datum *resvalue`).
    pub resvalue: Option<Box<Datum>>,
    /// where to store the result's is-null flag (C `bool *resnull`).
    pub resnull: Option<Box<bool>>,
    /// inline data for the operation (was `union d`).
    pub d: ExprEvalStepData,
}

/// Inline per-operation data; one variant per former `union d` member.
pub enum ExprEvalStepData {
    /// for EEOP_INNER/OUTER/SCAN/OLD/NEW_FETCHSOME
    Fetch(FetchData),
    /// for EEOP_INNER/OUTER/SCAN/OLD/NEW_[SYS]VAR
    Var(VarData),
    /// for WHOLEROW
    Wholerow(WholerowData),
    /// for EEOP_ASSIGN_*_VAR
    AssignVar(AssignVarData),
    /// for ASSIGN_TMP[_MAKE_RO]
    AssignTmp(AssignTmpData),
    /// for RETURNINGEXPR
    ReturningExpr(ReturningExprData),
    /// for CONST
    Constval(ConstvalData),
    /// for EEOP_FUNCEXPR_* / NULLIF / DISTINCT
    Func(FuncData),
    /// for EEOP_BOOL_*_STEP
    Boolexpr(BoolexprData),
    /// for QUAL
    Qualexpr(QualexprData),
    /// for JUMP[_CONDITION]
    Jump(JumpData),
    /// for EEOP_NULLTEST_ROWIS[NOT]NULL
    NulltestRow(NulltestRowData),
    /// for PARAM_EXEC/EXTERN and PARAM_SET
    Param(ParamData),
    /// for PARAM_CALLBACK
    Cparam(CparamData),
    /// for CASE_TESTVAL/DOMAIN_TESTVAL
    Casetest(CasetestData),
    /// for MAKE_READONLY
    MakeReadonly(MakeReadonlyData),
    /// for IOCOERCE
    Iocoerce(IocoerceData),
    /// for SQLVALUEFUNCTION
    Sqlvaluefunction(SqlvaluefunctionData),
    /// for NEXTVALUEEXPR
    Nextvalueexpr(NextvalueexprData),
    /// for ARRAYEXPR
    Arrayexpr(ArrayexprData),
    /// for ARRAYCOERCE
    Arraycoerce(ArraycoerceData),
    /// for ROW
    Row(RowData),
    /// for ROWCOMPARE_STEP
    RowcompareStep(RowcompareStepData),
    /// for ROWCOMPARE_FINAL
    RowcompareFinal(RowcompareFinalData),
    /// for MINMAX
    Minmax(MinmaxData),
    /// for FIELDSELECT
    Fieldselect(FieldselectData),
    /// for FIELDSTORE_DEFORM / FIELDSTORE_FORM
    Fieldstore(FieldstoreData),
    /// for SBSREF_SUBSCRIPTS
    SbsrefSubscript(SbsrefSubscriptData),
    /// for SBSREF_OLD / ASSIGN / FETCH
    Sbsref(SbsrefData),
    /// for DOMAIN_NOTNULL / DOMAIN_CHECK
    Domaincheck(DomaincheckData),
    /// for HASHDATUM_SET_INITVAL
    HashdatumInitvalue(HashdatumInitvalueData),
    /// for EEOP_HASHDATUM_(FIRST|NEXT32)[_STRICT]
    Hashdatum(HashdatumData),
    /// for CONVERT_ROWTYPE
    ConvertRowtype(ConvertRowtypeData),
    /// for SCALARARRAYOP
    Scalararrayop(ScalararrayopData),
    /// for HASHED_SCALARARRAYOP
    Hashedscalararrayop(HashedscalararrayopData),
    /// for XMLEXPR
    Xmlexpr(XmlexprData),
    /// for JSON_CONSTRUCTOR
    JsonConstructor(JsonConstructorData),
    /// for AGGREF
    Aggref(AggrefData),
    /// for GROUPING_FUNC
    GroupingFunc(GroupingFuncData),
    /// for WINDOW_FUNC
    WindowFunc(WindowFuncData),
    /// for SUBPLAN
    Subplan(SubplanData),
    /// for EEOP_AGG_*DESERIALIZE
    AggDeserialize(AggDeserializeData),
    /// for EEOP_AGG_STRICT_INPUT_CHECK_{NULLS,ARGS}
    AggStrictInputCheck(AggStrictInputCheckData),
    /// for AGG_PLAIN_PERGROUP_NULLCHECK
    AggPlainPergroupNullcheck(AggPlainPergroupNullcheckData),
    /// for EEOP_AGG_PRESORTED_DISTINCT_{SINGLE,MULTI}
    AggPresortedDistinctcheck(AggPresortedDistinctcheckData),
    /// for EEOP_AGG_PLAIN_TRANS_* and EEOP_AGG_ORDERED_TRANS_*
    AggTrans(AggTransData),
    /// for IS_JSON
    IsJson(IsJsonData),
    /// for JSONEXPR_PATH
    Jsonexpr(JsonexprData),
    /// for JSONEXPR_COERCION
    JsonexprCoercion(JsonexprCoercionData),
}

pub struct FetchData {
    /// attribute number up to which to fetch (inclusive).
    pub last_var: i32,
    /// will the type of slot be the same for every invocation.
    pub fixed: bool,
    /// tuple descriptor, if known.
    pub known_desc: TupleDesc,
    /// type of slot; relied upon only if fixed is set.
    pub kind: Option<&'static dyn TupleTableSlotOps>,
}

pub struct VarData {
    /// attr number - 1 for regular VAR, else the (negative) attr number.
    pub attnum: i32,
    pub vartype: Oid,
    pub varreturningtype: VarReturningType,
}

pub struct WholerowData {
    pub var: Option<Box<Var>>,
    pub first: bool,
    pub slow: bool,
    pub tupdesc: TupleDesc,
    pub junk_filter: Option<Box<crate::nodes::execnodes::JunkFilter>>,
}

pub struct AssignVarData {
    pub resultnum: i32,
    pub attnum: i32,
}

pub struct AssignTmpData {
    pub resultnum: i32,
}

pub struct ReturningExprData {
    pub nullflag: u8,
    pub jumpdone: i32,
}

pub struct ConstvalData {
    pub value: Datum,
    pub isnull: bool,
}

pub struct FuncData {
    pub finfo: Option<Box<FmgrInfo>>,
    pub fcinfo_data: Option<Box<crate::fmgr::FunctionCallInfoBaseData>>,
    pub fn_addr: Option<PGFunction>,
    pub nargs: i32,
    /// make arg0 R/O (used only for NULLIF).
    pub make_ro: bool,
}

pub struct BoolexprData {
    pub anynull: Option<Box<bool>>,
    pub jumpdone: i32,
}

pub struct QualexprData {
    pub jumpdone: i32,
}

pub struct JumpData {
    pub jumpdone: i32,
}

pub struct NulltestRowData {
    pub rowcache: ExprEvalRowtypeCache,
}

pub struct ParamData {
    pub paramid: i32,
    pub paramtype: Oid,
}

pub struct CparamData {
    pub paramfunc: Option<ExecEvalSubroutine>,
    pub paramarg: crate::nodes::execnodes::OpaqueState,
    pub paramarg2: crate::nodes::execnodes::OpaqueState,
    pub paramid: i32,
    pub paramtype: Oid,
}

pub struct CasetestData {
    pub value: Option<Box<Datum>>,
    pub isnull: Option<Box<bool>>,
}

pub struct MakeReadonlyData {
    pub value: Option<Box<Datum>>,
    pub isnull: Option<Box<bool>>,
}

pub struct IocoerceData {
    pub finfo_out: Option<Box<FmgrInfo>>,
    pub fcinfo_data_out: Option<Box<crate::fmgr::FunctionCallInfoBaseData>>,
    pub finfo_in: Option<Box<FmgrInfo>>,
    pub fcinfo_data_in: Option<Box<crate::fmgr::FunctionCallInfoBaseData>>,
}

pub struct SqlvaluefunctionData {
    pub svf: Option<Box<SQLValueFunction>>,
}

pub struct NextvalueexprData {
    pub seqid: Oid,
    pub seqtypid: Oid,
}

pub struct ArrayexprData {
    pub elemvalues: Vec<Datum>,
    pub elemnulls: Vec<bool>,
    pub nelems: i32,
    pub elemtype: Oid,
    pub elemlength: i16,
    pub elembyval: bool,
    pub elemalign: u8,
    pub multidims: bool,
}

pub struct ArraycoerceData {
    pub elemexprstate: Option<Box<ExprState>>,
    pub resultelemtype: Oid,
    pub amstate: crate::nodes::execnodes::OpaqueState,
}

pub struct RowData {
    pub tupdesc: TupleDesc,
    pub elemvalues: Vec<Datum>,
    pub elemnulls: Vec<bool>,
}

pub struct RowcompareStepData {
    pub finfo: Option<Box<FmgrInfo>>,
    pub fcinfo_data: Option<Box<crate::fmgr::FunctionCallInfoBaseData>>,
    pub fn_addr: Option<PGFunction>,
    pub jumpnull: i32,
    pub jumpdone: i32,
}

pub struct RowcompareFinalData {
    pub cmptype: CompareType,
}

pub struct MinmaxData {
    pub values: Vec<Datum>,
    pub nulls: Vec<bool>,
    pub nelems: i32,
    pub op: MinMaxOp,
    pub finfo: Option<Box<FmgrInfo>>,
    pub fcinfo_data: Option<Box<crate::fmgr::FunctionCallInfoBaseData>>,
}

pub struct FieldselectData {
    pub fieldnum: i16,
    pub resulttype: Oid,
    pub rowcache: ExprEvalRowtypeCache,
}

pub struct FieldstoreData {
    pub fstore: Option<Box<FieldStore>>,
    /// DEFORM and FORM share the same cache.
    pub rowcache: Option<Box<ExprEvalRowtypeCache>>,
    pub values: Vec<Datum>,
    pub nulls: Vec<bool>,
    pub ncolumns: i32,
}

pub struct SbsrefSubscriptData {
    pub subscriptfunc: Option<ExecEvalBoolSubroutine>,
    pub state: Option<Box<SubscriptingRefState>>,
    pub jumpdone: i32,
}

pub struct SbsrefData {
    pub subscriptfunc: Option<ExecEvalSubroutine>,
    pub state: Option<Box<SubscriptingRefState>>,
}

pub struct DomaincheckData {
    pub constraintname: Option<String>,
    pub checkvalue: Option<Box<Datum>>,
    pub checknull: Option<Box<bool>>,
    pub resulttype: Oid,
    pub escontext: Option<Box<ErrorSaveContext>>,
}

pub struct HashdatumInitvalueData {
    pub init_value: Datum,
}

pub struct HashdatumData {
    pub finfo: Option<Box<FmgrInfo>>,
    pub fcinfo_data: Option<Box<crate::fmgr::FunctionCallInfoBaseData>>,
    pub fn_addr: Option<PGFunction>,
    pub jumpdone: i32,
    pub iresult: Option<Box<NullableDatum>>,
}

pub struct ConvertRowtypeData {
    pub inputtype: Oid,
    pub outputtype: Oid,
    pub incache: Option<Box<ExprEvalRowtypeCache>>,
    pub outcache: Option<Box<ExprEvalRowtypeCache>>,
    pub map: Option<Box<TupleConversionMap>>,
}

pub struct ScalararrayopData {
    pub element_type: Oid,
    pub useOr: bool,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: u8,
    pub finfo: Option<Box<FmgrInfo>>,
    pub fcinfo_data: Option<Box<crate::fmgr::FunctionCallInfoBaseData>>,
    pub fn_addr: Option<PGFunction>,
}

pub struct HashedscalararrayopData {
    pub has_nulls: bool,
    pub inclause: bool,
    pub null_lhs_result: bool,
    pub null_lhs_isnull: bool,
    pub elements_tab: Option<Box<ScalarArrayOpExprHashTable>>,
    pub finfo: Option<Box<FmgrInfo>>,
    pub fcinfo_data: Option<Box<crate::fmgr::FunctionCallInfoBaseData>>,
    pub saop: Option<Box<ScalarArrayOpExpr>>,
}

pub struct XmlexprData {
    pub xexpr: Option<Box<XmlExpr>>,
    pub named_argvalue: Vec<Datum>,
    pub named_argnull: Vec<bool>,
    pub argvalue: Vec<Datum>,
    pub argnull: Vec<bool>,
}

pub struct JsonConstructorData {
    pub jcstate: Option<Box<JsonConstructorExprState>>,
}

pub struct AggrefData {
    pub aggno: i32,
}

pub struct GroupingFuncData {
    /// integer list of column numbers.
    pub clauses: Vec<i32>,
}

pub struct WindowFuncData {
    /// out-of-line state, modified by nodeWindowAgg.c.
    pub wfstate: Option<Box<WindowFuncExprState>>,
}

pub struct SubplanData {
    /// out-of-line state, created by nodeSubplan.c.
    pub sstate: Option<Box<SubPlanState>>,
}

pub struct AggDeserializeData {
    pub fcinfo_data: Option<Box<crate::fmgr::FunctionCallInfoBaseData>>,
    pub jumpnull: i32,
}

pub struct AggStrictInputCheckData {
    /// pointers to NullableDatums to check (STRICT_INPUT_CHECK_ARGS).
    pub args: Vec<NullableDatum>,
    /// pointers to booleans to check (STRICT_INPUT_CHECK_NULLS).
    pub nulls: Vec<bool>,
    pub nargs: i32,
    pub jumpnull: i32,
}

pub struct AggPlainPergroupNullcheckData {
    pub setoff: i32,
    pub jumpnull: i32,
}

pub struct AggPresortedDistinctcheckData {
    pub pertrans: Option<Box<crate::executor::nodeAgg::AggStatePerTransData>>,
    pub aggcontext: Option<Box<ExprContext>>,
    pub jumpdistinct: i32,
}

pub struct AggTransData {
    pub pertrans: Option<Box<crate::executor::nodeAgg::AggStatePerTransData>>,
    pub aggcontext: Option<Box<ExprContext>>,
    pub setno: i32,
    pub transno: i32,
    pub setoff: i32,
}

pub struct IsJsonData {
    pub pred: Option<Box<JsonIsPredicate>>,
}

pub struct JsonexprData {
    pub jsestate: Option<Box<JsonExprState>>,
}

pub struct JsonexprCoercionData {
    pub targettype: Oid,
    pub targettypmod: i32,
    pub omit_quotes: bool,
    /// exists_* only relevant for EXISTS_OP.
    pub exists_coerce: bool,
    pub exists_cast_to_int: bool,
    pub exists_check_domain: bool,
    pub json_coercion_cache: crate::nodes::execnodes::OpaqueState,
    pub escontext: Option<Box<ErrorSaveContext>>,
}

// ---------------------------------------------------------------------------
// Out-of-line state structs
// ---------------------------------------------------------------------------

/// Non-inline data for container (SubscriptingRef) operations.
#[derive(Default)]
pub struct SubscriptingRefState {
    pub isassignment: bool,
    /// workspace for type-specific subscripting code (C `void *`).
    pub workspace: crate::nodes::execnodes::OpaqueState,
    pub numupper: i32,
    pub upperprovided: Vec<bool>,
    pub upperindex: Vec<Datum>,
    pub upperindexnull: Vec<bool>,
    pub numlower: i32,
    pub lowerprovided: Vec<bool>,
    pub lowerindex: Vec<Datum>,
    pub lowerindexnull: Vec<bool>,
    pub replacevalue: Datum,
    pub replacenull: bool,
    pub prevvalue: Datum,
    pub prevnull: bool,
}

/// Execution step methods used for SubscriptingRef.
pub struct SubscriptExecSteps {
    pub check_subscripts: Option<ExecEvalBoolSubroutine>,
    pub fetch: Option<ExecEvalSubroutine>,
    pub assign: Option<ExecEvalSubroutine>,
    pub fetch_old: Option<ExecEvalSubroutine>,
}

/// JSON_CONSTRUCTOR state, too big to inline.
pub struct JsonConstructorExprState {
    pub constructor: Option<Box<JsonConstructorExpr>>,
    pub arg_values: Vec<Datum>,
    pub arg_nulls: Vec<bool>,
    pub arg_types: Vec<Oid>,
    /// cache for datum_to_json[b]().
    pub arg_type_cache: Vec<JsonConstructorArgTypeCache>,
    pub nargs: i32,
}

/// Inner anonymous struct of `JsonConstructorExprState.arg_type_cache`.
pub struct JsonConstructorArgTypeCache {
    pub category: i32,
    pub outfuncid: Oid,
}

// Forward-declared in the header; concrete defs live in the .c files.

/// Opaque; private type defined in execExprInterp.c, not ported.
#[derive(Debug, Default)]
pub struct ScalarArrayOpExprHashTable;

// ---------------------------------------------------------------------------
// functions in execExpr.c
// ---------------------------------------------------------------------------

pub fn ExprEvalPushStep(_es: &mut ExprState, _s: &ExprEvalStep) {
    unimplemented!()
}

// functions in execExprInterp.c
pub fn ExecReadyInterpretedExpr(_state: &mut ExprState) {
    unimplemented!()
}

pub fn ExecEvalStepOp(_state: &ExprState, _op: &ExprEvalStep) -> ExprEvalOp {
    unimplemented!()
}

/// Returns (result, isNull) - the C `bool *isNull` out-param folded in.
pub fn ExecInterpExprStillValid(
    _state: &mut ExprState,
    _econtext: &mut ExprContext,
) -> (Datum, bool) {
    unimplemented!()
}

pub fn CheckExprStillValid(_state: &mut ExprState, _econtext: &mut ExprContext) {
    unimplemented!()
}

// Non fast-path execution functions (externs so other eval methods can reuse).
pub fn ExecEvalFuncExprFusage(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalFuncExprStrictFusage(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalParamExec(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalParamSet(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalParamExtern(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalCoerceViaIOSafe(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalSQLValueFunction(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalCurrentOfExpr(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalNextValueExpr(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalRowNull(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalRowNotNull(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalArrayExpr(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalArrayCoerce(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalRow(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalMinMax(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalFieldSelect(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalFieldStoreDeForm(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalFieldStoreForm(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalConvertRowtype(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalScalarArrayOp(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalHashedScalarArrayOp(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalConstraintNotNull(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalConstraintCheck(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalXmlExpr(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalJsonConstructor(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalJsonIsPredicate(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalJsonExprPath(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) -> i32 {
    unimplemented!()
}
pub fn ExecEvalJsonCoercion(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalJsonCoercionFinish(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalGroupingFunc(_state: &mut ExprState, _op: &mut ExprEvalStep) {
    unimplemented!()
}
pub fn ExecEvalMergeSupportFunc(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalSubPlan(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalWholeRowVar(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalSysVar(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext, _slot: &mut TupleTableSlot) {
    unimplemented!()
}

pub fn ExecAggInitGroup(
    _aggstate: &mut crate::nodes::execnodes::AggState,
    _pertrans: Option<Box<crate::executor::nodeAgg::AggStatePerTransData>>,
    _pergroup: Option<Box<crate::executor::nodeAgg::AggStatePerGroupData>>,
    _aggcontext: &mut ExprContext,
) {
    unimplemented!()
}

pub fn ExecAggCopyTransValue(
    _aggstate: &mut crate::nodes::execnodes::AggState,
    _pertrans: Option<Box<crate::executor::nodeAgg::AggStatePerTransData>>,
    _new_value: Datum,
    _new_value_is_null: bool,
    _old_value: Datum,
    _old_value_is_null: bool,
) -> Datum {
    unimplemented!()
}

pub fn ExecEvalPreOrderedDistinctSingle(
    _aggstate: &mut crate::nodes::execnodes::AggState,
    _pertrans: Option<Box<crate::executor::nodeAgg::AggStatePerTransData>>,
) -> bool {
    unimplemented!()
}

pub fn ExecEvalPreOrderedDistinctMulti(
    _aggstate: &mut crate::nodes::execnodes::AggState,
    _pertrans: Option<Box<crate::executor::nodeAgg::AggStatePerTransData>>,
) -> bool {
    unimplemented!()
}

pub fn ExecEvalAggOrderedTransDatum(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecEvalAggOrderedTransTuple(_state: &mut ExprState, _op: &mut ExprEvalStep, _econtext: &mut ExprContext) {
    unimplemented!()
}
