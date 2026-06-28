//! Translated from PostgreSQL src/include/nodes/primnodes.h

use crate::access::attnum::AttrNumber;
use crate::access::cmptype::CompareType;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::{AggSplit, CmdType, Node, ParseLoc};
use crate::nodes::value::String_;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

pub type Index = usize;

/// C `Expr` is the abstract supertype of all expression nodes; in this port the
/// universal `Node` enum subsumes it. Alias kept so `Expr`-typed signatures
/// translate directly (an `Expr *` field is `Option<Node>`).
pub type Expr = Node;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverridingKind {
    NOT_SET = 0,
    USER_VALUE,
    SYSTEM_VALUE,
}

/// Alias for a range variable; may rename columns. `colnames` is a list of
/// String nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alias {
    pub aliasname: Option<String>,
    pub colnames: Vec<String_>,
}

/// What to do at commit time for temporary relations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCommitAction {
    NOOP,
    PRESERVE_ROWS,
    DELETE_ROWS,
    DROP,
}

/// Range variable, used in FROM clauses and as table names in utility stmts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeVar {
    pub catalogname: Option<String>,
    pub schemaname: Option<String>,
    pub relname: Option<String>,
    pub inh: bool,
    /// see RELPERSISTENCE_* in pg_class.h
    pub relpersistence: i8,
    pub alias: Option<Box<Alias>>,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFuncType {
    XMLTABLE,
    JSON_TABLE,
}

/// Node for a table function, such as XMLTABLE and JSON_TABLE.
#[derive(Debug, Clone, PartialEq)]
pub struct TableFunc {
    pub functype: TableFuncType,
    pub ns_uris: Vec<Node>,
    pub ns_names: Vec<Node>,
    pub docexpr: Option<Node>,
    pub rowexpr: Option<Node>,
    pub colnames: Vec<Node>,
    pub coltypes: Vec<Oid>,
    pub coltypmods: Vec<i32>,
    pub colcollations: Vec<Oid>,
    pub colexprs: Vec<Node>,
    pub coldefexprs: Vec<Node>,
    pub colvalexprs: Vec<Node>,
    pub passingvalexprs: Vec<Node>,
    pub notnulls: Option<Bitmapset>,
    pub plan: Option<Node>,
    pub ordinalitycol: i32,
    pub location: ParseLoc,
}

/// Target info for SELECT INTO, CREATE TABLE AS, CREATE MATERIALIZED VIEW.
#[derive(Debug, Clone, PartialEq)]
pub struct IntoClause {
    pub rel: Option<Box<RangeVar>>,
    pub colNames: Vec<Node>,
    pub accessMethod: Option<String>,
    pub options: Vec<Node>,
    pub onCommit: OnCommitAction,
    pub tableSpaceName: Option<String>,
    /// materialized view's SELECT query (struct Query*)
    pub viewQuery: Option<Node>,
    pub skipData: bool,
}

// ---- Var sentinel varnos and helpers ----
pub const INNER_VAR: i32 = -1;
pub const OUTER_VAR: i32 = -2;
pub const INDEX_VAR: i32 = -3;
pub const ROWID_VAR: i32 = -4;

pub const fn IS_SPECIAL_VARNO(varno: i32) -> bool {
    varno < 0
}

pub const PRS2_OLD_VARNO: i32 = 1;
pub const PRS2_NEW_VARNO: i32 = 2;

/// Returning behavior for Vars in RETURNING list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarReturningType {
    DEFAULT,
    OLD,
    NEW,
}

/// Expression node representing a variable (ie, a table column).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Var {
    pub varno: i32,
    pub varattno: AttrNumber,
    pub vartype: Oid,
    pub vartypmod: i32,
    pub varcollid: Oid,
    pub varnullingrels: Option<Bitmapset>,
    pub varlevelsup: Index,
    pub varreturningtype: VarReturningType,
    pub varnosyn: Index,
    pub varattnosyn: AttrNumber,
    pub location: ParseLoc,
}

/// A constant value. For varlena types the value is in non-extended form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Const {
    pub consttype: Oid,
    pub consttypmod: i32,
    pub constcollid: Oid,
    pub constlen: i32,
    pub constvalue: Datum,
    pub constisnull: bool,
    pub constbyval: bool,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamKind {
    EXTERN,
    EXEC,
    SUBLINK,
    MULTIEXPR,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Param {
    pub paramkind: ParamKind,
    pub paramid: i32,
    pub paramtype: Oid,
    pub paramtypmod: i32,
    pub paramcollid: Oid,
    pub location: ParseLoc,
}

/// An aggregate-function call.
#[derive(Debug, Clone, PartialEq)]
pub struct Aggref {
    pub aggfnoid: Oid,
    pub aggtype: Oid,
    pub aggcollid: Oid,
    pub inputcollid: Oid,
    pub aggtranstype: Oid,
    pub aggargtypes: Vec<Oid>,
    pub aggdirectargs: Vec<Node>,
    pub args: Vec<Node>,
    pub aggorder: Vec<Node>,
    pub aggdistinct: Vec<Node>,
    pub aggfilter: Option<Node>,
    pub aggstar: bool,
    pub aggvariadic: bool,
    pub aggkind: i8,
    pub aggpresorted: bool,
    pub agglevelsup: Index,
    pub aggsplit: AggSplit,
    pub aggno: i32,
    pub aggtransno: i32,
    pub location: ParseLoc,
}

/// A GROUPING(...) expression.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupingFunc {
    pub args: Vec<Node>,
    pub refs: Vec<i32>,
    pub cols: Vec<i32>,
    pub agglevelsup: Index,
    pub location: ParseLoc,
}

/// A window-function call.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFunc {
    pub winfnoid: Oid,
    pub wintype: Oid,
    pub wincollid: Oid,
    pub inputcollid: Oid,
    pub args: Vec<Node>,
    pub aggfilter: Option<Node>,
    pub runCondition: Vec<Node>,
    pub winref: Index,
    pub winstar: bool,
    pub winagg: bool,
    pub location: ParseLoc,
}

/// Intermediate OpExpr used by WindowAgg to short-circuit execution.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFuncRunCondition {
    pub opno: Oid,
    pub inputcollid: Oid,
    pub wfunc_left: bool,
    pub arg: Option<Node>,
}

/// A merge support function expression (MERGE_ACTION()).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeSupportFunc {
    pub msftype: Oid,
    pub msfcollid: Oid,
    pub location: ParseLoc,
}

/// A subscripting operation over a container (array, etc).
#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptingRef {
    pub refcontainertype: Oid,
    pub refelemtype: Oid,
    pub refrestype: Oid,
    pub reftypmod: i32,
    pub refcollid: Oid,
    pub refupperindexpr: Vec<Node>,
    pub reflowerindexpr: Vec<Node>,
    pub refexpr: Option<Node>,
    pub refassgnexpr: Option<Node>,
}

/// Distinguishes the allowed set of type casts. Ordering is significant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoercionContext {
    IMPLICIT,
    ASSIGNMENT,
    PLPGSQL,
    EXPLICIT,
}

/// How to display a FuncExpr or related node. equal() ignores this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoercionForm {
    EXPLICIT_CALL,
    EXPLICIT_CAST,
    IMPLICIT_CAST,
    SQL_SYNTAX,
}

/// A function call.
#[derive(Debug, Clone, PartialEq)]
pub struct FuncExpr {
    pub funcid: Oid,
    pub funcresulttype: Oid,
    pub funcretset: bool,
    pub funcvariadic: bool,
    pub funcformat: CoercionForm,
    pub funccollid: Oid,
    pub inputcollid: Oid,
    pub args: Vec<Node>,
    pub location: ParseLoc,
}

/// A named argument of a function.
#[derive(Debug, Clone, PartialEq)]
pub struct NamedArgExpr {
    pub arg: Option<Node>,
    pub name: Option<String>,
    pub argnumber: i32,
    pub location: ParseLoc,
}

/// An operator invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct OpExpr {
    pub opno: Oid,
    pub opfuncid: Oid,
    pub opresulttype: Oid,
    pub opretset: bool,
    pub opcollid: Oid,
    pub inputcollid: Oid,
    pub args: Vec<Node>,
    pub location: ParseLoc,
}

/// "x IS DISTINCT FROM y"; represented identically to an OpExpr.
pub type DistinctExpr = OpExpr;

/// A NULLIF expression; represented identically to an OpExpr.
pub type NullIfExpr = OpExpr;

/// "scalar op ANY/ALL (array)".
#[derive(Debug, Clone, PartialEq)]
pub struct ScalarArrayOpExpr {
    pub opno: Oid,
    pub opfuncid: Oid,
    pub hashfuncid: Oid,
    pub negfuncid: Oid,
    pub useOr: bool,
    pub inputcollid: Oid,
    pub args: Vec<Node>,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoolExprType {
    AND_EXPR,
    OR_EXPR,
    NOT_EXPR,
}

/// The basic Boolean operators AND, OR, NOT.
#[derive(Debug, Clone, PartialEq)]
pub struct BoolExpr {
    pub boolop: BoolExprType,
    pub args: Vec<Node>,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubLinkType {
    EXISTS_SUBLINK,
    ALL_SUBLINK,
    ANY_SUBLINK,
    ROWCOMPARE_SUBLINK,
    EXPR_SUBLINK,
    MULTIEXPR_SUBLINK,
    ARRAY_SUBLINK,
    CTE_SUBLINK,
}

/// A subselect appearing in an expression.
#[derive(Debug, Clone, PartialEq)]
pub struct SubLink {
    pub subLinkType: SubLinkType,
    pub subLinkId: i32,
    pub testexpr: Option<Node>,
    pub operName: Vec<Node>,
    pub subselect: Option<Node>,
    pub location: ParseLoc,
}

/// Executable expression node for a subplan (sub-SELECT).
#[derive(Debug, Clone, PartialEq)]
pub struct SubPlan {
    pub subLinkType: SubLinkType,
    pub testexpr: Option<Node>,
    pub paramIds: Vec<i32>,
    pub plan_id: i32,
    pub plan_name: Option<String>,
    pub firstColType: Oid,
    pub firstColTypmod: i32,
    pub firstColCollation: Oid,
    pub useHashTable: bool,
    pub unknownEqFalse: bool,
    pub parallel_safe: bool,
    pub setParam: Vec<i32>,
    pub parParam: Vec<i32>,
    pub args: Vec<Node>,
    pub startup_cost: crate::nodes::nodes::Cost,
    pub per_call_cost: crate::nodes::nodes::Cost,
}

/// A choice among SubPlans; used only transiently during planning.
#[derive(Debug, Clone, PartialEq)]
pub struct AlternativeSubPlan {
    pub subplans: Vec<Node>,
}

/// Extracting one field from a tuple value.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldSelect {
    pub arg: Option<Node>,
    pub fieldnum: AttrNumber,
    pub resulttype: Oid,
    pub resulttypmod: i32,
    pub resultcollid: Oid,
}

/// Modifying one field in a tuple value, yielding a new tuple value.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldStore {
    pub arg: Option<Node>,
    pub newvals: Vec<Node>,
    pub fieldnums: Vec<i32>,
    pub resulttype: Oid,
}

/// A no-op type coercion between binary-compatible datatypes.
#[derive(Debug, Clone, PartialEq)]
pub struct RelabelType {
    pub arg: Option<Node>,
    pub resulttype: Oid,
    pub resulttypmod: i32,
    pub resultcollid: Oid,
    pub relabelformat: CoercionForm,
    pub location: ParseLoc,
}

/// Coercion implemented via the source typoutput then dest typinput.
#[derive(Debug, Clone, PartialEq)]
pub struct CoerceViaIO {
    pub arg: Option<Node>,
    pub resulttype: Oid,
    pub resultcollid: Oid,
    pub coerceformat: CoercionForm,
    pub location: ParseLoc,
}

/// Coercion from one array type to another, per-element.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayCoerceExpr {
    pub arg: Option<Node>,
    pub elemexpr: Option<Node>,
    pub resulttype: Oid,
    pub resulttypmod: i32,
    pub resultcollid: Oid,
    pub coerceformat: CoercionForm,
    pub location: ParseLoc,
}

/// Coercion from one composite type to another (matched by name).
#[derive(Debug, Clone, PartialEq)]
pub struct ConvertRowtypeExpr {
    pub arg: Option<Node>,
    pub resulttype: Oid,
    pub convertformat: CoercionForm,
    pub location: ParseLoc,
}

/// COLLATE expression.
#[derive(Debug, Clone, PartialEq)]
pub struct CollateExpr {
    pub arg: Option<Node>,
    pub collOid: Oid,
    pub location: ParseLoc,
}

/// A CASE expression.
#[derive(Debug, Clone, PartialEq)]
pub struct CaseExpr {
    pub casetype: Oid,
    pub casecollid: Oid,
    pub arg: Option<Node>,
    pub args: Vec<Node>,
    pub defresult: Option<Node>,
    pub location: ParseLoc,
}

/// One arm of a CASE expression.
#[derive(Debug, Clone, PartialEq)]
pub struct CaseWhen {
    pub expr: Option<Node>,
    pub result: Option<Node>,
    pub location: ParseLoc,
}

/// Placeholder for the test value processed by a CASE expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseTestExpr {
    pub typeId: Oid,
    pub typeMod: i32,
    pub collation: Oid,
}

/// An ARRAY[] expression.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayExpr {
    pub array_typeid: Oid,
    pub array_collid: Oid,
    pub element_typeid: Oid,
    pub elements: Vec<Node>,
    pub multidims: bool,
    pub list_start: ParseLoc,
    pub list_end: ParseLoc,
    pub location: ParseLoc,
}

/// A ROW() expression.
#[derive(Debug, Clone, PartialEq)]
pub struct RowExpr {
    pub args: Vec<Node>,
    pub row_typeid: Oid,
    pub row_format: CoercionForm,
    pub colnames: Vec<String_>,
    pub location: ParseLoc,
}

/// Row-wise comparison, such as (a, b) <= (1, 2).
#[derive(Debug, Clone, PartialEq)]
pub struct RowCompareExpr {
    pub cmptype: CompareType,
    pub opnos: Vec<Oid>,
    pub opfamilies: Vec<Oid>,
    pub inputcollids: Vec<Oid>,
    pub largs: Vec<Node>,
    pub rargs: Vec<Node>,
}

/// A COALESCE expression.
#[derive(Debug, Clone, PartialEq)]
pub struct CoalesceExpr {
    pub coalescetype: Oid,
    pub coalescecollid: Oid,
    pub args: Vec<Node>,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinMaxOp {
    GREATEST,
    LEAST,
}

/// A GREATEST or LEAST function.
#[derive(Debug, Clone, PartialEq)]
pub struct MinMaxExpr {
    pub minmaxtype: Oid,
    pub minmaxcollid: Oid,
    pub inputcollid: Oid,
    pub op: MinMaxOp,
    pub args: Vec<Node>,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SQLValueFunctionOp {
    CURRENT_DATE,
    CURRENT_TIME,
    CURRENT_TIME_N,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP_N,
    LOCALTIME,
    LOCALTIME_N,
    LOCALTIMESTAMP,
    LOCALTIMESTAMP_N,
    CURRENT_ROLE,
    CURRENT_USER,
    USER,
    SESSION_USER,
    CURRENT_CATALOG,
    CURRENT_SCHEMA,
}

/// Parameterless functions with special grammar productions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SQLValueFunction {
    pub op: SQLValueFunctionOp,
    pub r#type: Oid,
    pub typmod: i32,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XmlExprOp {
    XMLCONCAT,
    XMLELEMENT,
    XMLFOREST,
    XMLPARSE,
    XMLPI,
    XMLROOT,
    XMLSERIALIZE,
    DOCUMENT,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XmlOptionType {
    DOCUMENT,
    CONTENT,
}

/// Various SQL/XML functions requiring special grammar productions.
#[derive(Debug, Clone, PartialEq)]
pub struct XmlExpr {
    pub op: XmlExprOp,
    pub name: Option<String>,
    pub named_args: Vec<Node>,
    pub arg_names: Vec<Node>,
    pub args: Vec<Node>,
    pub xmloption: XmlOptionType,
    pub indent: bool,
    pub r#type: Oid,
    pub typmod: i32,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonEncoding {
    DEFAULT,
    UTF8,
    UTF16,
    UTF32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonFormatType {
    DEFAULT,
    JSON,
    JSONB,
}

/// Representation of JSON FORMAT clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonFormat {
    pub format_type: JsonFormatType,
    pub encoding: JsonEncoding,
    pub location: ParseLoc,
}

/// Transformed representation of JSON RETURNING clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonReturning {
    pub format: Option<Box<JsonFormat>>,
    pub typid: Oid,
    pub typmod: i32,
}

/// Representation of JSON value expression (expr [FORMAT JsonFormat]).
#[derive(Debug, Clone, PartialEq)]
pub struct JsonValueExpr {
    pub raw_expr: Option<Node>,
    pub formatted_expr: Option<Node>,
    pub format: Option<Box<JsonFormat>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonConstructorType {
    OBJECT = 1,
    ARRAY = 2,
    OBJECTAGG = 3,
    ARRAYAGG = 4,
    PARSE = 5,
    SCALAR = 6,
    SERIALIZE = 7,
}

/// Wrapper over FuncExpr/Aggref/WindowFunc for SQL/JSON constructors.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonConstructorExpr {
    pub r#type: JsonConstructorType,
    pub args: Vec<Node>,
    pub func: Option<Node>,
    pub coercion: Option<Node>,
    pub returning: Option<Box<JsonReturning>>,
    pub absent_on_null: bool,
    pub unique: bool,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonValueType {
    ANY,
    OBJECT,
    ARRAY,
    SCALAR,
}

/// Representation of IS JSON predicate.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonIsPredicate {
    pub expr: Option<Node>,
    pub format: Option<Box<JsonFormat>>,
    pub item_type: JsonValueType,
    pub unique_keys: bool,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonWrapper {
    UNSPEC,
    NONE,
    CONDITIONAL,
    UNCONDITIONAL,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonBehaviorType {
    NULL = 0,
    ERROR,
    EMPTY,
    TRUE,
    FALSE,
    UNKNOWN,
    EMPTY_ARRAY,
    EMPTY_OBJECT,
    DEFAULT,
}

/// ON ERROR / ON EMPTY behavior for SQL/JSON query functions.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonBehavior {
    pub btype: JsonBehaviorType,
    pub expr: Option<Node>,
    pub coerce: bool,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonExprOp {
    EXISTS_OP,
    QUERY_OP,
    VALUE_OP,
    TABLE_OP,
}

/// Transformed JSON_VALUE(), JSON_QUERY(), JSON_EXISTS().
#[derive(Debug, Clone, PartialEq)]
pub struct JsonExpr {
    pub op: JsonExprOp,
    pub column_name: Option<String>,
    pub formatted_expr: Option<Node>,
    pub format: Option<Box<JsonFormat>>,
    pub path_spec: Option<Node>,
    pub returning: Option<Box<JsonReturning>>,
    pub passing_names: Vec<Node>,
    pub passing_values: Vec<Node>,
    pub on_empty: Option<Box<JsonBehavior>>,
    pub on_error: Option<Box<JsonBehavior>>,
    pub use_io_coercion: bool,
    pub use_json_coercion: bool,
    pub wrapper: JsonWrapper,
    pub omit_quotes: bool,
    pub collation: Oid,
    pub location: ParseLoc,
}

/// A JSON path expression computed for a JSON_TABLE plan node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonTablePath {
    pub value: Option<Box<Const>>,
    pub name: Option<String>,
}

// JsonTablePlan is an abstract base (only carried NodeTag); not a concrete node.
// Its `plan` first field in the scan/join structs below is dropped.

/// JSON_TABLE plan to evaluate a JSON path expression and NESTED paths.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonTablePathScan {
    pub path: Option<Box<JsonTablePath>>,
    pub errorOnError: bool,
    pub child: Option<Node>,
    pub colMin: i32,
    pub colMax: i32,
}

/// Plan to join rows of sibling NESTED COLUMNS clauses.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonTableSiblingJoin {
    pub lplan: Option<Node>,
    pub rplan: Option<Node>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullTestType {
    NULL,
    NOT_NULL,
}

/// Testing a value for NULLness.
#[derive(Debug, Clone, PartialEq)]
pub struct NullTest {
    pub arg: Option<Node>,
    pub nulltesttype: NullTestType,
    pub argisrow: bool,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoolTestType {
    TRUE,
    NOT_TRUE,
    FALSE,
    NOT_FALSE,
    UNKNOWN,
    NOT_UNKNOWN,
}

/// Determining whether a boolean is TRUE, FALSE, or UNKNOWN.
#[derive(Debug, Clone, PartialEq)]
pub struct BooleanTest {
    pub arg: Option<Node>,
    pub booltesttype: BoolTestType,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMatchKind {
    MATCHED,
    NOT_MATCHED_BY_SOURCE,
    NOT_MATCHED_BY_TARGET,
}

pub const NUM_MERGE_MATCH_KINDS: i32 =
    MergeMatchKind::NOT_MATCHED_BY_TARGET as i32 + 1;

/// Transformed representation of a WHEN clause in a MERGE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeAction {
    pub matchKind: MergeMatchKind,
    pub commandType: CmdType,
    pub r#override: OverridingKind,
    pub qual: Option<Node>,
    pub targetList: Vec<Node>,
    pub updateColnos: Vec<i32>,
}

/// Coercing a value to a domain type.
#[derive(Debug, Clone, PartialEq)]
pub struct CoerceToDomain {
    pub arg: Option<Node>,
    pub resulttype: Oid,
    pub resulttypmod: i32,
    pub resultcollid: Oid,
    pub coercionformat: CoercionForm,
    pub location: ParseLoc,
}

/// Placeholder for the value processed by a domain's check constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoerceToDomainValue {
    pub typeId: Oid,
    pub typeMod: i32,
    pub collation: Oid,
    pub location: ParseLoc,
}

/// Placeholder for a DEFAULT marker in an INSERT or UPDATE command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetToDefault {
    pub typeId: Oid,
    pub typeMod: i32,
    pub collation: Oid,
    pub location: ParseLoc,
}

/// [WHERE] CURRENT OF cursor_name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentOfExpr {
    pub cvarno: Index,
    pub cursor_name: Option<String>,
    pub cursor_param: i32,
}

/// Get next value from sequence (no permission check).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NextValueExpr {
    pub seqid: Oid,
    pub typeId: Oid,
}

/// An element of a unique index inference specification.
#[derive(Debug, Clone, PartialEq)]
pub struct InferenceElem {
    pub expr: Option<Node>,
    pub infercollid: Oid,
    pub inferopclass: Oid,
}

/// Return OLD/NEW.(expression) in RETURNING list.
#[derive(Debug, Clone, PartialEq)]
pub struct ReturningExpr {
    pub retlevelsup: i32,
    pub retold: bool,
    pub retexpr: Option<Node>,
}

/// A target entry (used in query target lists).
#[derive(Debug, Clone, PartialEq)]
pub struct TargetEntry {
    pub expr: Option<Node>,
    pub resno: AttrNumber,
    pub resname: Option<String>,
    pub ressortgroupref: Index,
    pub resorigtbl: Oid,
    pub resorigcol: AttrNumber,
    pub resjunk: bool,
}

/// Reference to an entry in the query's rangetable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTblRef {
    pub rtindex: i32,
}

/// SQL JOIN expression.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinExpr {
    pub jointype: crate::nodes::nodes::JoinType,
    pub isNatural: bool,
    pub larg: Option<Node>,
    pub rarg: Option<Node>,
    pub usingClause: Vec<Node>,
    pub join_using_alias: Option<Box<Alias>>,
    pub quals: Option<Node>,
    pub alias: Option<Box<Alias>>,
    pub rtindex: i32,
}

/// A FROM ... WHERE ... construct.
#[derive(Debug, Clone, PartialEq)]
pub struct FromExpr {
    pub fromlist: Vec<Node>,
    pub quals: Option<Node>,
}

/// An ON CONFLICT DO ... expression.
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflictExpr {
    pub action: crate::nodes::nodes::OnConflictAction,
    pub arbiterElems: Vec<Node>,
    pub arbiterWhere: Option<Node>,
    pub constraint: Oid,
    pub onConflictSet: Vec<Node>,
    pub onConflictWhere: Option<Node>,
    pub exclRelIndex: i32,
    pub exclRelTlist: Vec<Node>,
}
