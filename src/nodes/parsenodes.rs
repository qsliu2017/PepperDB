//! Translated from PostgreSQL src/include/nodes/parsenodes.h

use bitflags::bitflags;

use crate::c::Index;
use crate::common::relpath::RelFileNumber;
use crate::c::SubTransactionId;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::lockoptions::{LockClauseStrength, LockWaitPolicy};
use crate::nodes::nodes::{Cardinality, CmdType, JoinType, Node, ParseLoc};
use crate::nodes::primnodes::{
    Alias, CoercionContext, CoercionForm, FuncExpr, IntoClause,
    JsonBehavior, JsonExprOp, JsonFormat, JsonReturning, JsonValueExpr, JsonWrapper, OnCommitAction,
    OnConflictExpr, OverridingKind, RangeVar, TableFunc, XmlOptionType,
};
use crate::nodes::value::String_;
use crate::postgres_ext::Oid;

// NOTE: `MergeMatchKind`, `JsonExprOp`, `JsonWrapper`, etc. live in primnodes;
// `String` here means a PG String value node (`crate::nodes::value::String_`),
// while C `char *` becomes Rust `Option<String>`.

/// Possible sources of a Query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuerySource {
    /// original parsetree (explicit query)
    QSRC_ORIGINAL,
    /// added by parse analysis (now unused)
    QSRC_PARSER,
    /// added by unconditional INSTEAD rule
    QSRC_INSTEAD_RULE,
    /// added by conditional INSTEAD rule
    QSRC_QUAL_INSTEAD_RULE,
    /// added by non-INSTEAD rule
    QSRC_NON_INSTEAD_RULE,
}

/// Sort ordering options for ORDER BY and CREATE INDEX.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortByDir {
    SORTBY_DEFAULT,
    SORTBY_ASC,
    SORTBY_DESC,
    /// not allowed in CREATE INDEX ...
    SORTBY_USING,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortByNulls {
    SORTBY_NULLS_DEFAULT,
    SORTBY_NULLS_FIRST,
    SORTBY_NULLS_LAST,
}

/// Options for [ ALL | DISTINCT ].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetQuantifier {
    SET_QUANTIFIER_DEFAULT,
    SET_QUANTIFIER_ALL,
    SET_QUANTIFIER_DISTINCT,
}

bitflags! {
    /// Grantable rights, OR-able into a bitmask (C: `AclMode`, `ACL_*`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AclMode: u64 {
        /// for relations
        const INSERT = 1 << 0;
        const SELECT = 1 << 1;
        const UPDATE = 1 << 2;
        const DELETE = 1 << 3;
        const TRUNCATE = 1 << 4;
        const REFERENCES = 1 << 5;
        const TRIGGER = 1 << 6;
        /// for functions
        const EXECUTE = 1 << 7;
        /// for various object types
        const USAGE = 1 << 8;
        /// for namespaces and databases
        const CREATE = 1 << 9;
        /// for databases
        const CREATE_TEMP = 1 << 10;
        /// for databases
        const CONNECT = 1 << 11;
        /// for configuration parameters
        const SET = 1 << 12;
        /// for configuration parameters
        const ALTER_SYSTEM = 1 << 13;
        /// for relations
        const MAINTAIN = 1 << 14;
    }
}

impl AclMode {
    /// C: `ACL_NO_RIGHTS` (0).
    pub const NO_RIGHTS: Self = Self::empty();
    /// C: `ACL_SELECT_FOR_UPDATE` alias of `ACL_UPDATE`.
    pub const SELECT_FOR_UPDATE: Self = Self::UPDATE;
}

/// C: `N_ACL_RIGHTS` (1 plus the last 1<<x).
pub const N_ACL_RIGHTS: i32 = 15;

/// Parse analysis turns all statements into a Query tree.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub commandType: CmdType,
    pub querySource: QuerySource,
    pub queryId: i64,
    pub canSetTag: bool,
    /// non-null if commandType == CMD_UTILITY
    pub utilityStmt: Option<Box<Node>>,
    pub resultRelation: i32,
    pub hasAggs: bool,
    pub hasWindowFuncs: bool,
    pub hasTargetSRFs: bool,
    pub hasSubLinks: bool,
    pub hasDistinctOn: bool,
    pub hasRecursive: bool,
    pub hasModifyingCTE: bool,
    pub hasForUpdate: bool,
    pub hasRowSecurity: bool,
    pub hasGroupRTE: bool,
    pub isReturn: bool,
    /// WITH list (of CommonTableExpr's)
    pub cteList: Vec<Box<Node>>,
    /// list of range table entries
    pub rtable: Vec<Box<Node>>,
    /// list of RTEPermissionInfo nodes
    pub rteperminfos: Vec<Box<Node>>,
    /// table join tree (FROM and WHERE clauses)
    pub jointree: Option<Box<Node>>,
    /// list of actions for MERGE (only)
    pub mergeActionList: Vec<Box<Node>>,
    pub mergeTargetRelation: i32,
    pub mergeJoinCondition: Option<Box<Node>>,
    /// target list (of TargetEntry)
    pub targetList: Vec<Box<Node>>,
    pub r#override: OverridingKind,
    pub onConflict: Option<Box<OnConflictExpr>>,
    pub returningOldAlias: Option<String>,
    pub returningNewAlias: Option<String>,
    pub returningList: Vec<Box<Node>>,
    pub groupClause: Vec<Box<Node>>,
    pub groupDistinct: bool,
    pub groupingSets: Vec<Box<Node>>,
    pub havingQual: Option<Box<Node>>,
    pub windowClause: Vec<Box<Node>>,
    pub distinctClause: Vec<Box<Node>>,
    pub sortClause: Vec<Box<Node>>,
    pub limitOffset: Option<Box<Node>>,
    pub limitCount: Option<Box<Node>>,
    pub limitOption: crate::nodes::nodes::LimitOption,
    pub rowMarks: Vec<Box<Node>>,
    pub setOperations: Option<Box<Node>>,
    pub constraintDeps: Vec<Box<Node>>,
    pub withCheckOptions: Vec<Box<Node>>,
    pub stmt_location: ParseLoc,
    pub stmt_len: ParseLoc,
}

/// TypeName - specifies a type in definitions.
#[derive(Debug, Clone, PartialEq)]
pub struct TypeName {
    pub names: Vec<Box<Node>>,
    pub typeOid: Oid,
    pub setof: bool,
    pub pct_type: bool,
    pub typmods: Vec<Box<Node>>,
    pub typemod: i32,
    pub arrayBounds: Vec<Box<Node>>,
    pub location: ParseLoc,
}

/// ColumnRef - reference to a column, or possibly a whole tuple.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub fields: Vec<Box<Node>>,
    pub location: ParseLoc,
}

/// ParamRef - specifies a $n parameter reference.
#[derive(Debug, Clone, PartialEq)]
pub struct ParamRef {
    pub number: i32,
    pub location: ParseLoc,
}

/// A_Expr kinds: infix, prefix, and postfix expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A_Expr_Kind {
    /// normal operator
    AEXPR_OP,
    /// scalar op ANY (array)
    AEXPR_OP_ANY,
    /// scalar op ALL (array)
    AEXPR_OP_ALL,
    /// IS DISTINCT FROM
    AEXPR_DISTINCT,
    /// IS NOT DISTINCT FROM
    AEXPR_NOT_DISTINCT,
    /// NULLIF
    AEXPR_NULLIF,
    /// [NOT] IN
    AEXPR_IN,
    /// [NOT] LIKE
    AEXPR_LIKE,
    /// [NOT] ILIKE
    AEXPR_ILIKE,
    /// [NOT] SIMILAR
    AEXPR_SIMILAR,
    /// BETWEEN
    AEXPR_BETWEEN,
    /// NOT BETWEEN
    AEXPR_NOT_BETWEEN,
    /// BETWEEN SYMMETRIC
    AEXPR_BETWEEN_SYM,
    /// NOT BETWEEN SYMMETRIC
    AEXPR_NOT_BETWEEN_SYM,
}

#[derive(Debug, Clone, PartialEq)]
pub struct A_Expr {
    pub kind: A_Expr_Kind,
    /// possibly-qualified name of operator
    pub name: Vec<Box<Node>>,
    /// left argument, or None if none
    pub lexpr: Option<Box<Node>>,
    /// right argument, or None if none
    pub rexpr: Option<Box<Node>>,
    pub rexpr_list_start: ParseLoc,
    pub rexpr_list_end: ParseLoc,
    pub location: ParseLoc,
}

/// C: `union ValUnion` - inline value node for A_Const.
#[derive(Debug, Clone, PartialEq)]
pub enum ValUnion {
    Node(Box<Node>),
    Integer(crate::nodes::value::Integer),
    Float(crate::nodes::value::Float),
    Boolean(crate::nodes::value::Boolean),
    String(String_),
    BitString(crate::nodes::value::BitString),
}

/// A_Const - a literal constant.
#[derive(Debug, Clone, PartialEq)]
pub struct A_Const {
    pub val: ValUnion,
    /// SQL NULL constant
    pub isnull: bool,
    pub location: ParseLoc,
}

/// TypeCast - a CAST expression.
#[derive(Debug, Clone, PartialEq)]
pub struct TypeCast {
    pub arg: Option<Box<Node>>,
    pub typeName: Option<Box<TypeName>>,
    pub location: ParseLoc,
}

/// CollateClause - a COLLATE expression.
#[derive(Debug, Clone, PartialEq)]
pub struct CollateClause {
    pub arg: Option<Box<Node>>,
    pub collname: Vec<Box<Node>>,
    pub location: ParseLoc,
}

/// RoleSpec - a role name or one of a few special values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleSpecType {
    /// role name is stored as a C string
    ROLESPEC_CSTRING,
    ROLESPEC_CURRENT_ROLE,
    ROLESPEC_CURRENT_USER,
    ROLESPEC_SESSION_USER,
    /// role name is "public"
    ROLESPEC_PUBLIC,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RoleSpec {
    pub roletype: RoleSpecType,
    /// filled only for ROLESPEC_CSTRING
    pub rolename: Option<String>,
    pub location: ParseLoc,
}

/// FuncCall - a function or aggregate invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct FuncCall {
    pub funcname: Vec<Box<Node>>,
    pub args: Vec<Box<Node>>,
    pub agg_order: Vec<Box<Node>>,
    pub agg_filter: Option<Box<Node>>,
    pub over: Option<Box<WindowDef>>,
    pub agg_within_group: bool,
    pub agg_star: bool,
    pub agg_distinct: bool,
    pub func_variadic: bool,
    pub funcformat: CoercionForm,
    pub location: ParseLoc,
}

/// A_Star - '*' representing all columns of a table or compound field.
#[derive(Debug, Clone, PartialEq)]
pub struct A_Star {}

/// A_Indices - array subscript or slice bounds.
#[derive(Debug, Clone, PartialEq)]
pub struct A_Indices {
    pub is_slice: bool,
    pub lidx: Option<Box<Node>>,
    pub uidx: Option<Box<Node>>,
}

/// A_Indirection - select a field and/or array element from an expression.
#[derive(Debug, Clone, PartialEq)]
pub struct A_Indirection {
    pub arg: Option<Box<Node>>,
    pub indirection: Vec<Box<Node>>,
}

/// A_ArrayExpr - an ARRAY[] construct.
#[derive(Debug, Clone, PartialEq)]
pub struct A_ArrayExpr {
    pub elements: Vec<Box<Node>>,
    pub list_start: ParseLoc,
    pub list_end: ParseLoc,
    pub location: ParseLoc,
}

/// ResTarget - result target.
#[derive(Debug, Clone, PartialEq)]
pub struct ResTarget {
    pub name: Option<String>,
    pub indirection: Vec<Box<Node>>,
    pub val: Option<Box<Node>>,
    pub location: ParseLoc,
}

/// MultiAssignRef - element of a row source expression for UPDATE.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiAssignRef {
    pub source: Option<Box<Node>>,
    pub colno: i32,
    pub ncolumns: i32,
}

/// SortBy - for ORDER BY clause.
#[derive(Debug, Clone, PartialEq)]
pub struct SortBy {
    pub node: Option<Box<Node>>,
    pub sortby_dir: SortByDir,
    pub sortby_nulls: SortByNulls,
    pub useOp: Vec<Box<Node>>,
    pub location: ParseLoc,
}

/// WindowDef - raw representation of WINDOW and OVER clauses.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowDef {
    pub name: Option<String>,
    pub refname: Option<String>,
    pub partitionClause: Vec<Box<Node>>,
    pub orderClause: Vec<Box<Node>>,
    /// frame_clause options, see FRAMEOPTION_* (FrameOptions)
    pub frameOptions: i32,
    pub startOffset: Option<Box<Node>>,
    pub endOffset: Option<Box<Node>>,
    pub location: ParseLoc,
}

bitflags! {
    /// `WindowDef.frameOptions` bits (C: `FRAMEOPTION_*`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FrameOptions: i32 {
        /// any specified?
        const NONDEFAULT = 0x00001;
        /// RANGE behavior
        const RANGE = 0x00002;
        /// ROWS behavior
        const ROWS = 0x00004;
        /// GROUPS behavior
        const GROUPS = 0x00008;
        /// BETWEEN given?
        const BETWEEN = 0x00010;
        const START_UNBOUNDED_PRECEDING = 0x00020;
        const END_UNBOUNDED_PRECEDING = 0x00040;
        const START_UNBOUNDED_FOLLOWING = 0x00080;
        const END_UNBOUNDED_FOLLOWING = 0x00100;
        const START_CURRENT_ROW = 0x00200;
        const END_CURRENT_ROW = 0x00400;
        const START_OFFSET_PRECEDING = 0x00800;
        const END_OFFSET_PRECEDING = 0x01000;
        const START_OFFSET_FOLLOWING = 0x02000;
        const END_OFFSET_FOLLOWING = 0x04000;
        const EXCLUDE_CURRENT_ROW = 0x08000;
        const EXCLUDE_GROUP = 0x10000;
        const EXCLUDE_TIES = 0x20000;
        const START_OFFSET =
            Self::START_OFFSET_PRECEDING.bits() | Self::START_OFFSET_FOLLOWING.bits();
        const END_OFFSET = Self::END_OFFSET_PRECEDING.bits() | Self::END_OFFSET_FOLLOWING.bits();
        const EXCLUSION = Self::EXCLUDE_CURRENT_ROW.bits()
            | Self::EXCLUDE_GROUP.bits()
            | Self::EXCLUDE_TIES.bits();
        const DEFAULTS = Self::RANGE.bits()
            | Self::START_UNBOUNDED_PRECEDING.bits()
            | Self::END_CURRENT_ROW.bits();
    }
}

/// RangeSubselect - subquery appearing in a FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeSubselect {
    pub lateral: bool,
    pub subquery: Option<Box<Node>>,
    pub alias: Option<Box<Alias>>,
}

/// RangeFunction - function call appearing in a FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeFunction {
    pub lateral: bool,
    pub ordinality: bool,
    pub is_rowsfrom: bool,
    pub functions: Vec<Box<Node>>,
    pub alias: Option<Box<Alias>>,
    pub coldeflist: Vec<Box<Node>>,
}

/// RangeTableFunc - raw form of "table functions" such as XMLTABLE.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeTableFunc {
    pub lateral: bool,
    pub docexpr: Option<Box<Node>>,
    pub rowexpr: Option<Box<Node>>,
    pub namespaces: Vec<Box<Node>>,
    pub columns: Vec<Box<Node>>,
    pub alias: Option<Box<Alias>>,
    pub location: ParseLoc,
}

/// RangeTableFuncCol - one column in a RangeTableFunc->columns.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeTableFuncCol {
    pub colname: Option<String>,
    pub typeName: Option<Box<TypeName>>,
    pub for_ordinality: bool,
    pub is_not_null: bool,
    pub colexpr: Option<Box<Node>>,
    pub coldefexpr: Option<Box<Node>>,
    pub location: ParseLoc,
}

/// RangeTableSample - TABLESAMPLE appearing in a raw FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeTableSample {
    pub relation: Option<Box<Node>>,
    pub method: Vec<Box<Node>>,
    pub args: Vec<Box<Node>>,
    pub repeatable: Option<Box<Node>>,
    pub location: ParseLoc,
}

/// ColumnDef - column definition (used in various creates).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub colname: Option<String>,
    pub typeName: Option<Box<TypeName>>,
    pub compression: Option<String>,
    pub inhcount: i16,
    pub is_local: bool,
    pub is_not_null: bool,
    pub is_from_type: bool,
    /// attstorage setting, or 0 for default
    pub storage: i8,
    pub storage_name: Option<String>,
    pub raw_default: Option<Box<Node>>,
    pub cooked_default: Option<Box<Node>>,
    /// attidentity setting
    pub identity: i8,
    pub identitySequence: Option<Box<RangeVar>>,
    /// attgenerated setting
    pub generated: i8,
    pub collClause: Option<Box<CollateClause>>,
    pub collOid: Oid,
    pub constraints: Vec<Box<Node>>,
    pub fdwoptions: Vec<Box<Node>>,
    pub location: ParseLoc,
}

/// TableLikeClause - CREATE TABLE ( ... LIKE ... ) clause.
#[derive(Debug, Clone, PartialEq)]
pub struct TableLikeClause {
    pub relation: Option<Box<RangeVar>>,
    /// OR of TableLikeOption flags
    pub options: u32,
    pub relationOid: Oid,
}

bitflags! {
    /// C: `TableLikeOption` (used as OR-able bits in `TableLikeClause.options`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TableLikeOption: u32 {
        const COMMENTS = 1 << 0;
        const COMPRESSION = 1 << 1;
        const CONSTRAINTS = 1 << 2;
        const DEFAULTS = 1 << 3;
        const GENERATED = 1 << 4;
        const IDENTITY = 1 << 5;
        const INDEXES = 1 << 6;
        const STATISTICS = 1 << 7;
        const STORAGE = 1 << 8;
        /// C: `CREATE_TABLE_LIKE_ALL = PG_INT32_MAX`
        const ALL = i32::MAX as u32;
    }
}

/// IndexElem - index parameters (used in CREATE INDEX, and in ON CONFLICT).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexElem {
    pub name: Option<String>,
    pub expr: Option<Box<Node>>,
    pub indexcolname: Option<String>,
    pub collation: Vec<Box<Node>>,
    pub opclass: Vec<Box<Node>>,
    pub opclassopts: Vec<Box<Node>>,
    pub ordering: SortByDir,
    pub nulls_ordering: SortByNulls,
}

/// DefElem action: SET/ADD/DROP attached to an option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefElemAction {
    /// no action given
    DEFELEM_UNSPEC,
    DEFELEM_SET,
    DEFELEM_ADD,
    DEFELEM_DROP,
}

/// DefElem - a generic "name = value" option definition.
#[derive(Debug, Clone, PartialEq)]
pub struct DefElem {
    pub defnamespace: Option<String>,
    pub defname: Option<String>,
    pub arg: Option<Box<Node>>,
    pub defaction: DefElemAction,
    pub location: ParseLoc,
}

/// LockingClause - raw FOR [NO KEY] UPDATE/[KEY] SHARE options.
#[derive(Debug, Clone, PartialEq)]
pub struct LockingClause {
    pub lockedRels: Vec<Box<Node>>,
    pub strength: LockClauseStrength,
    pub waitPolicy: LockWaitPolicy,
}

/// XMLSERIALIZE (in raw parse tree only).
#[derive(Debug, Clone, PartialEq)]
pub struct XmlSerialize {
    pub xmloption: XmlOptionType,
    pub expr: Option<Box<Node>>,
    pub typeName: Option<Box<TypeName>>,
    pub indent: bool,
    pub location: ParseLoc,
}

/// PartitionElem - parse-time representation of a single partition key.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionElem {
    pub name: Option<String>,
    pub expr: Option<Box<Node>>,
    pub collation: Vec<Box<Node>>,
    pub opclass: Vec<Box<Node>>,
    pub location: ParseLoc,
}

/// C: `PartitionStrategy` (codes used in `PartitionBoundSpec.strategy` etc).
#[repr(i8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStrategy {
    PARTITION_STRATEGY_LIST = b'l' as i8,
    PARTITION_STRATEGY_RANGE = b'r' as i8,
    PARTITION_STRATEGY_HASH = b'h' as i8,
}

/// PartitionSpec - parse-time representation of a partition key specification.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionSpec {
    pub strategy: PartitionStrategy,
    pub partParams: Vec<Box<Node>>,
    pub location: ParseLoc,
}

/// PartitionBoundSpec - a partition bound specification.
///
/// Stored on disk in pg_class.relpartbound (as a node tree, not raw bytes), so
/// in-memory representation is fine. This is the canonical definition; the
/// `crate::partitioning::partdefs` placeholder forwards to it.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionBoundSpec {
    /// see PARTITION_STRATEGY codes
    pub strategy: i8,
    pub is_default: bool,
    pub modulus: i32,
    pub remainder: i32,
    pub listdatums: Vec<Box<Node>>,
    pub lowerdatums: Vec<Box<Node>>,
    pub upperdatums: Vec<Box<Node>>,
    pub location: ParseLoc,
}

/// PartitionRangeDatumKind - one of the values in a range partition bound.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionRangeDatumKind {
    /// less than any other value
    PARTITION_RANGE_DATUM_MINVALUE = -1,
    /// a specific (bounded) value
    PARTITION_RANGE_DATUM_VALUE = 0,
    /// greater than any other value
    PARTITION_RANGE_DATUM_MAXVALUE = 1,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionRangeDatum {
    pub kind: PartitionRangeDatumKind,
    pub value: Option<Box<Node>>,
    pub location: ParseLoc,
}

/// PartitionCmd - info for ALTER TABLE/INDEX ATTACH/DETACH PARTITION.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionCmd {
    pub name: Option<Box<RangeVar>>,
    pub bound: Option<Box<PartitionBoundSpec>>,
    pub concurrent: bool,
}

/// RTEKind - kind of range table entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RTEKind {
    /// ordinary relation reference
    RTE_RELATION,
    /// subquery in FROM
    RTE_SUBQUERY,
    /// join
    RTE_JOIN,
    /// function in FROM
    RTE_FUNCTION,
    /// TableFunc(.., column list)
    RTE_TABLEFUNC,
    /// VALUES (<exprlist>), ...
    RTE_VALUES,
    /// common table expr (WITH list element)
    RTE_CTE,
    /// tuplestore, e.g. for AFTER triggers
    RTE_NAMEDTUPLESTORE,
    /// empty FROM clause; added by the planner
    RTE_RESULT,
    /// the grouping step
    RTE_GROUP,
}

/// RangeTblEntry - a range table is a List of these.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeTblEntry {
    pub alias: Option<Box<Alias>>,
    pub eref: Option<Box<Alias>>,
    pub rtekind: RTEKind,
    pub relid: Oid,
    pub inh: bool,
    pub relkind: i8,
    pub rellockmode: i32,
    pub perminfoindex: Index,
    pub tablesample: Option<Box<TableSampleClause>>,
    pub subquery: Option<Box<Query>>,
    pub security_barrier: bool,
    pub jointype: JoinType,
    pub joinmergedcols: i32,
    pub joinaliasvars: Vec<Box<Node>>,
    pub joinleftcols: Vec<i32>,
    pub joinrightcols: Vec<i32>,
    pub join_using_alias: Option<Box<Alias>>,
    pub functions: Vec<Box<Node>>,
    pub funcordinality: bool,
    pub tablefunc: Option<Box<TableFunc>>,
    pub values_lists: Vec<Box<Node>>,
    pub ctename: Option<String>,
    pub ctelevelsup: Index,
    pub self_reference: bool,
    pub coltypes: Vec<Oid>,
    pub coltypmods: Vec<i32>,
    pub colcollations: Vec<Oid>,
    pub enrname: Option<String>,
    pub enrtuples: Cardinality,
    pub groupexprs: Vec<Box<Node>>,
    pub lateral: bool,
    pub inFromCl: bool,
    pub securityQuals: Vec<Box<Node>>,
}

/// RTEPermissionInfo - per-relation information for permission checking.
#[derive(Debug, Clone, PartialEq)]
pub struct RTEPermissionInfo {
    pub relid: Oid,
    pub inh: bool,
    pub requiredPerms: AclMode,
    pub checkAsUser: Oid,
    pub selectedCols: Option<Bitmapset>,
    pub insertedCols: Option<Bitmapset>,
    pub updatedCols: Option<Bitmapset>,
}

/// RangeTblFunction - RTE subsidiary data for one function in a FUNCTION RTE.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeTblFunction {
    pub funcexpr: Option<Box<Node>>,
    pub funccolcount: i32,
    pub funccolnames: Vec<Box<Node>>,
    pub funccoltypes: Vec<Oid>,
    pub funccoltypmods: Vec<i32>,
    pub funccolcollations: Vec<Oid>,
    pub funcparams: Option<Bitmapset>,
}

/// TableSampleClause - TABLESAMPLE in a transformed FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSampleClause {
    pub tsmhandler: Oid,
    pub args: Vec<Box<Node>>,
    pub repeatable: Option<Box<Node>>,
}

/// WCOKind - kind of WITH CHECK OPTION.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WCOKind {
    /// WCO on an auto-updatable view
    WCO_VIEW_CHECK,
    WCO_RLS_INSERT_CHECK,
    WCO_RLS_UPDATE_CHECK,
    WCO_RLS_CONFLICT_CHECK,
    WCO_RLS_MERGE_UPDATE_CHECK,
    WCO_RLS_MERGE_DELETE_CHECK,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithCheckOption {
    pub kind: WCOKind,
    pub relname: Option<String>,
    pub polname: Option<String>,
    pub qual: Option<Box<Node>>,
    pub cascaded: bool,
}

/// SortGroupClause - ORDER BY/GROUP BY/PARTITION BY/DISTINCT items.
#[derive(Debug, Clone, PartialEq)]
pub struct SortGroupClause {
    pub tleSortGroupRef: Index,
    pub eqop: Oid,
    pub sortop: Oid,
    pub reverse_sort: bool,
    pub nulls_first: bool,
    pub hashable: bool,
}

/// GroupingSetKind - CUBE, ROLLUP and GROUPING SETS clauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupingSetKind {
    GROUPING_SET_EMPTY,
    GROUPING_SET_SIMPLE,
    GROUPING_SET_ROLLUP,
    GROUPING_SET_CUBE,
    GROUPING_SET_SETS,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupingSet {
    pub kind: GroupingSetKind,
    pub content: Vec<Box<Node>>,
    pub location: ParseLoc,
}

/// WindowClause - transformed representation of WINDOW and OVER clauses.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowClause {
    pub name: Option<String>,
    pub refname: Option<String>,
    pub partitionClause: Vec<Box<Node>>,
    pub orderClause: Vec<Box<Node>>,
    pub frameOptions: i32,
    pub startOffset: Option<Box<Node>>,
    pub endOffset: Option<Box<Node>>,
    pub startInRangeFunc: Oid,
    pub endInRangeFunc: Oid,
    pub inRangeColl: Oid,
    pub inRangeAsc: bool,
    pub inRangeNullsFirst: bool,
    pub winref: Index,
    pub copiedOrder: bool,
}

/// RowMarkClause - parser output for FOR [KEY] UPDATE/SHARE clauses.
#[derive(Debug, Clone, PartialEq)]
pub struct RowMarkClause {
    pub rti: Index,
    pub strength: LockClauseStrength,
    pub waitPolicy: LockWaitPolicy,
    pub pushedDown: bool,
}

/// WithClause - representation of WITH clause.
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub ctes: Vec<Box<Node>>,
    pub recursive: bool,
    pub location: ParseLoc,
}

/// InferClause - ON CONFLICT unique index inference clause.
#[derive(Debug, Clone, PartialEq)]
pub struct InferClause {
    pub indexElems: Vec<Box<Node>>,
    pub whereClause: Option<Box<Node>>,
    pub conname: Option<String>,
    pub location: ParseLoc,
}

/// OnConflictClause - representation of ON CONFLICT clause.
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflictClause {
    pub action: crate::nodes::nodes::OnConflictAction,
    pub infer: Option<Box<InferClause>>,
    pub targetList: Vec<Box<Node>>,
    pub whereClause: Option<Box<Node>>,
    pub location: ParseLoc,
}

/// CTEMaterialize - WITH list element materialization option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CTEMaterialize {
    /// no option specified
    CTEMaterializeDefault,
    /// MATERIALIZED
    CTEMaterializeAlways,
    /// NOT MATERIALIZED
    CTEMaterializeNever,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CTESearchClause {
    pub search_col_list: Vec<Box<Node>>,
    pub search_breadth_first: bool,
    pub search_seq_column: Option<String>,
    pub location: ParseLoc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CTECycleClause {
    pub cycle_col_list: Vec<Box<Node>>,
    pub cycle_mark_column: Option<String>,
    pub cycle_mark_value: Option<Box<Node>>,
    pub cycle_mark_default: Option<Box<Node>>,
    pub cycle_path_column: Option<String>,
    pub location: ParseLoc,
    pub cycle_mark_type: Oid,
    pub cycle_mark_typmod: i32,
    pub cycle_mark_collation: Oid,
    pub cycle_mark_neop: Oid,
}

/// CommonTableExpr - representation of WITH list element.
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr {
    pub ctename: Option<String>,
    pub aliascolnames: Vec<Box<Node>>,
    pub ctematerialized: CTEMaterialize,
    pub ctequery: Option<Box<Node>>,
    pub search_clause: Option<Box<CTESearchClause>>,
    pub cycle_clause: Option<Box<CTECycleClause>>,
    pub location: ParseLoc,
    pub cterecursive: bool,
    pub cterefcount: i32,
    pub ctecolnames: Vec<Box<Node>>,
    pub ctecoltypes: Vec<Oid>,
    pub ctecoltypmods: Vec<i32>,
    pub ctecolcollations: Vec<Oid>,
}

/// MergeWhenClause - raw parser representation of a WHEN clause in MERGE.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeWhenClause {
    pub matchKind: crate::nodes::primnodes::MergeMatchKind,
    pub commandType: CmdType,
    pub r#override: OverridingKind,
    pub condition: Option<Box<Node>>,
    pub targetList: Vec<Box<Node>>,
    pub values: Vec<Box<Node>>,
}

/// ReturningOptionKind - kind of option in RETURNING WITH(...) list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturningOptionKind {
    /// specify alias for OLD in RETURNING
    RETURNING_OPTION_OLD,
    /// specify alias for NEW in RETURNING
    RETURNING_OPTION_NEW,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturningOption {
    pub option: ReturningOptionKind,
    pub value: Option<String>,
    pub location: ParseLoc,
}

/// ReturningClause - RETURNING expressions plus WITH(...) options.
#[derive(Debug, Clone, PartialEq)]
pub struct ReturningClause {
    pub options: Vec<Box<Node>>,
    pub exprs: Vec<Box<Node>>,
}

/// TriggerTransition - transition row or table naming clause.
#[derive(Debug, Clone, PartialEq)]
pub struct TriggerTransition {
    pub name: Option<String>,
    pub isNew: bool,
    pub isTable: bool,
}

/// JsonOutput - JSON output clause (RETURNING type [FORMAT format]).
#[derive(Debug, Clone, PartialEq)]
pub struct JsonOutput {
    pub typeName: Option<Box<TypeName>>,
    pub returning: Option<Box<JsonReturning>>,
}

/// JsonArgument - argument from JSON PASSING clause.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonArgument {
    pub val: Option<Box<JsonValueExpr>>,
    pub name: Option<String>,
}

/// JsonQuotes - [KEEP|OMIT] QUOTES clause for JSON_QUERY().
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonQuotes {
    JS_QUOTES_UNSPEC,
    JS_QUOTES_KEEP,
    JS_QUOTES_OMIT,
}

/// JsonFuncExpr - untransformed SQL/JSON query function expressions.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonFuncExpr {
    pub op: JsonExprOp,
    pub column_name: Option<String>,
    pub context_item: Option<Box<JsonValueExpr>>,
    pub pathspec: Option<Box<Node>>,
    pub passing: Vec<Box<Node>>,
    pub output: Option<Box<JsonOutput>>,
    pub on_empty: Option<Box<JsonBehavior>>,
    pub on_error: Option<Box<JsonBehavior>>,
    pub wrapper: JsonWrapper,
    pub quotes: JsonQuotes,
    pub location: ParseLoc,
}

/// JsonTablePathSpec - untransformed JSON path expression with optional name.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonTablePathSpec {
    pub string: Option<Box<Node>>,
    pub name: Option<String>,
    pub name_location: ParseLoc,
    pub location: ParseLoc,
}

/// JsonTable - untransformed representation of JSON_TABLE.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonTable {
    pub context_item: Option<Box<JsonValueExpr>>,
    pub pathspec: Option<Box<JsonTablePathSpec>>,
    pub passing: Vec<Box<Node>>,
    pub columns: Vec<Box<Node>>,
    pub on_error: Option<Box<JsonBehavior>>,
    pub alias: Option<Box<Alias>>,
    pub lateral: bool,
    pub location: ParseLoc,
}

/// JsonTableColumnType - enumeration of JSON_TABLE column types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonTableColumnType {
    JTC_FOR_ORDINALITY,
    JTC_REGULAR,
    JTC_EXISTS,
    JTC_FORMATTED,
    JTC_NESTED,
}

/// JsonTableColumn - untransformed representation of JSON_TABLE column.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonTableColumn {
    pub coltype: JsonTableColumnType,
    pub name: Option<String>,
    pub typeName: Option<Box<TypeName>>,
    pub pathspec: Option<Box<JsonTablePathSpec>>,
    pub format: Option<Box<JsonFormat>>,
    pub wrapper: JsonWrapper,
    pub quotes: JsonQuotes,
    pub columns: Vec<Box<Node>>,
    pub on_empty: Option<Box<JsonBehavior>>,
    pub on_error: Option<Box<JsonBehavior>>,
    pub location: ParseLoc,
}

/// JsonKeyValue - untransformed JSON object key-value pair.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonKeyValue {
    pub key: Option<Box<Node>>,
    pub value: Option<Box<JsonValueExpr>>,
}

/// JsonParseExpr - untransformed representation of JSON().
#[derive(Debug, Clone, PartialEq)]
pub struct JsonParseExpr {
    pub expr: Option<Box<JsonValueExpr>>,
    pub output: Option<Box<JsonOutput>>,
    pub unique_keys: bool,
    pub location: ParseLoc,
}

/// JsonScalarExpr - untransformed representation of JSON_SCALAR().
#[derive(Debug, Clone, PartialEq)]
pub struct JsonScalarExpr {
    pub expr: Option<Box<Node>>,
    pub output: Option<Box<JsonOutput>>,
    pub location: ParseLoc,
}

/// JsonSerializeExpr - untransformed representation of JSON_SERIALIZE().
#[derive(Debug, Clone, PartialEq)]
pub struct JsonSerializeExpr {
    pub expr: Option<Box<JsonValueExpr>>,
    pub output: Option<Box<JsonOutput>>,
    pub location: ParseLoc,
}

/// JsonObjectConstructor - untransformed JSON_OBJECT() constructor.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonObjectConstructor {
    pub exprs: Vec<Box<Node>>,
    pub output: Option<Box<JsonOutput>>,
    pub absent_on_null: bool,
    pub unique: bool,
    pub location: ParseLoc,
}

/// JsonArrayConstructor - untransformed JSON_ARRAY(element,...) constructor.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonArrayConstructor {
    pub exprs: Vec<Box<Node>>,
    pub output: Option<Box<JsonOutput>>,
    pub absent_on_null: bool,
    pub location: ParseLoc,
}

/// JsonArrayQueryConstructor - untransformed JSON_ARRAY(subquery) constructor.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonArrayQueryConstructor {
    pub query: Option<Box<Node>>,
    pub output: Option<Box<JsonOutput>>,
    pub format: Option<Box<JsonFormat>>,
    pub absent_on_null: bool,
    pub location: ParseLoc,
}

/// JsonAggConstructor - common fields of JSON_ARRAYAGG()/JSON_OBJECTAGG().
#[derive(Debug, Clone, PartialEq)]
pub struct JsonAggConstructor {
    pub output: Option<Box<JsonOutput>>,
    pub agg_filter: Option<Box<Node>>,
    pub agg_order: Vec<Box<Node>>,
    pub over: Option<Box<WindowDef>>,
    pub location: ParseLoc,
}

/// JsonObjectAgg - untransformed representation of JSON_OBJECTAGG().
#[derive(Debug, Clone, PartialEq)]
pub struct JsonObjectAgg {
    pub constructor: Option<Box<JsonAggConstructor>>,
    pub arg: Option<Box<JsonKeyValue>>,
    pub absent_on_null: bool,
    pub unique: bool,
}

/// JsonArrayAgg - untransformed representation of JSON_ARRAYAGG().
#[derive(Debug, Clone, PartialEq)]
pub struct JsonArrayAgg {
    pub constructor: Option<Box<JsonAggConstructor>>,
    pub arg: Option<Box<JsonValueExpr>>,
    pub absent_on_null: bool,
}

/// RawStmt - container for any one statement's raw parse tree.
#[derive(Debug, Clone, PartialEq)]
pub struct RawStmt {
    pub stmt: Option<Box<Node>>,
    pub stmt_location: ParseLoc,
    pub stmt_len: ParseLoc,
}

/// InsertStmt - Insert Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub relation: Option<Box<RangeVar>>,
    pub cols: Vec<Box<Node>>,
    pub selectStmt: Option<Box<Node>>,
    pub onConflictClause: Option<Box<OnConflictClause>>,
    pub returningClause: Option<Box<ReturningClause>>,
    pub withClause: Option<Box<WithClause>>,
    pub r#override: OverridingKind,
}

/// DeleteStmt - Delete Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub relation: Option<Box<RangeVar>>,
    pub usingClause: Vec<Box<Node>>,
    pub whereClause: Option<Box<Node>>,
    pub returningClause: Option<Box<ReturningClause>>,
    pub withClause: Option<Box<WithClause>>,
}

/// UpdateStmt - Update Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    pub relation: Option<Box<RangeVar>>,
    pub targetList: Vec<Box<Node>>,
    pub whereClause: Option<Box<Node>>,
    pub fromClause: Vec<Box<Node>>,
    pub returningClause: Option<Box<ReturningClause>>,
    pub withClause: Option<Box<WithClause>>,
}

/// MergeStmt - Merge Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeStmt {
    pub relation: Option<Box<RangeVar>>,
    pub sourceRelation: Option<Box<Node>>,
    pub joinCondition: Option<Box<Node>>,
    pub mergeWhenClauses: Vec<Box<Node>>,
    pub returningClause: Option<Box<ReturningClause>>,
    pub withClause: Option<Box<WithClause>>,
}

/// SetOperation - type of set op for SelectStmt / SetOperationStmt.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperation {
    SETOP_NONE = 0,
    SETOP_UNION,
    SETOP_INTERSECT,
    SETOP_EXCEPT,
}

/// SelectStmt - Select Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub distinctClause: Vec<Box<Node>>,
    pub intoClause: Option<Box<IntoClause>>,
    pub targetList: Vec<Box<Node>>,
    pub fromClause: Vec<Box<Node>>,
    pub whereClause: Option<Box<Node>>,
    pub groupClause: Vec<Box<Node>>,
    pub groupDistinct: bool,
    pub havingClause: Option<Box<Node>>,
    pub windowClause: Vec<Box<Node>>,
    pub valuesLists: Vec<Box<Node>>,
    pub sortClause: Vec<Box<Node>>,
    pub limitOffset: Option<Box<Node>>,
    pub limitCount: Option<Box<Node>>,
    pub limitOption: crate::nodes::nodes::LimitOption,
    pub lockingClause: Vec<Box<Node>>,
    pub withClause: Option<Box<WithClause>>,
    pub op: SetOperation,
    pub all: bool,
    pub larg: Option<Box<SelectStmt>>,
    pub rarg: Option<Box<SelectStmt>>,
}

/// SetOperationStmt - set operation node for post-analysis query trees.
#[derive(Debug, Clone, PartialEq)]
pub struct SetOperationStmt {
    pub op: SetOperation,
    pub all: bool,
    pub larg: Option<Box<Node>>,
    pub rarg: Option<Box<Node>>,
    pub colTypes: Vec<Oid>,
    pub colTypmods: Vec<i32>,
    pub colCollations: Vec<Oid>,
    pub groupClauses: Vec<Box<Node>>,
}

/// ReturnStmt - RETURN statement (inside SQL function body).
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnStmt {
    pub returnval: Option<Box<Node>>,
}

/// PLAssignStmt - PL/pgSQL Assignment Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct PLAssignStmt {
    pub name: Option<String>,
    pub indirection: Vec<Box<Node>>,
    pub nnames: i32,
    pub val: Option<Box<SelectStmt>>,
    pub location: ParseLoc,
}

/// ObjectType - the kind of object a command acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectType {
    OBJECT_ACCESS_METHOD,
    OBJECT_AGGREGATE,
    OBJECT_AMOP,
    OBJECT_AMPROC,
    /// type's attribute, when distinct from column
    OBJECT_ATTRIBUTE,
    OBJECT_CAST,
    OBJECT_COLUMN,
    OBJECT_COLLATION,
    OBJECT_CONVERSION,
    OBJECT_DATABASE,
    OBJECT_DEFAULT,
    OBJECT_DEFACL,
    OBJECT_DOMAIN,
    OBJECT_DOMCONSTRAINT,
    OBJECT_EVENT_TRIGGER,
    OBJECT_EXTENSION,
    OBJECT_FDW,
    OBJECT_FOREIGN_SERVER,
    OBJECT_FOREIGN_TABLE,
    OBJECT_FUNCTION,
    OBJECT_INDEX,
    OBJECT_LANGUAGE,
    OBJECT_LARGEOBJECT,
    OBJECT_MATVIEW,
    OBJECT_OPCLASS,
    OBJECT_OPERATOR,
    OBJECT_OPFAMILY,
    OBJECT_PARAMETER_ACL,
    OBJECT_POLICY,
    OBJECT_PROCEDURE,
    OBJECT_PUBLICATION,
    OBJECT_PUBLICATION_NAMESPACE,
    OBJECT_PUBLICATION_REL,
    OBJECT_ROLE,
    OBJECT_ROUTINE,
    OBJECT_RULE,
    OBJECT_SCHEMA,
    OBJECT_SEQUENCE,
    OBJECT_SUBSCRIPTION,
    OBJECT_STATISTIC_EXT,
    OBJECT_TABCONSTRAINT,
    OBJECT_TABLE,
    OBJECT_TABLESPACE,
    OBJECT_TRANSFORM,
    OBJECT_TRIGGER,
    OBJECT_TSCONFIGURATION,
    OBJECT_TSDICTIONARY,
    OBJECT_TSPARSER,
    OBJECT_TSTEMPLATE,
    OBJECT_TYPE,
    OBJECT_USER_MAPPING,
    OBJECT_VIEW,
}

/// CreateSchemaStmt - Create Schema Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSchemaStmt {
    pub schemaname: Option<String>,
    pub authrole: Option<Box<RoleSpec>>,
    pub schemaElts: Vec<Box<Node>>,
    pub if_not_exists: bool,
}

/// DropBehavior - RESTRICT or CASCADE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropBehavior {
    /// drop fails if any dependent objects
    DROP_RESTRICT,
    /// remove dependent objects too
    DROP_CASCADE,
}

/// AlterTableStmt - Alter Table.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStmt {
    pub relation: Option<Box<RangeVar>>,
    pub cmds: Vec<Box<Node>>,
    pub objtype: ObjectType,
    pub missing_ok: bool,
}

/// AlterTableType - subcommand kind for ALTER TABLE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterTableType {
    AT_AddColumn,
    AT_AddColumnToView,
    AT_ColumnDefault,
    AT_CookedColumnDefault,
    AT_DropNotNull,
    AT_SetNotNull,
    AT_SetExpression,
    AT_DropExpression,
    AT_SetStatistics,
    AT_SetOptions,
    AT_ResetOptions,
    AT_SetStorage,
    AT_SetCompression,
    AT_DropColumn,
    AT_AddIndex,
    AT_ReAddIndex,
    AT_AddConstraint,
    AT_ReAddConstraint,
    AT_ReAddDomainConstraint,
    AT_AlterConstraint,
    AT_ValidateConstraint,
    AT_AddIndexConstraint,
    AT_DropConstraint,
    AT_ReAddComment,
    AT_AlterColumnType,
    AT_AlterColumnGenericOptions,
    AT_ChangeOwner,
    AT_ClusterOn,
    AT_DropCluster,
    AT_SetLogged,
    AT_SetUnLogged,
    AT_DropOids,
    AT_SetAccessMethod,
    AT_SetTableSpace,
    AT_SetRelOptions,
    AT_ResetRelOptions,
    AT_ReplaceRelOptions,
    AT_EnableTrig,
    AT_EnableAlwaysTrig,
    AT_EnableReplicaTrig,
    AT_DisableTrig,
    AT_EnableTrigAll,
    AT_DisableTrigAll,
    AT_EnableTrigUser,
    AT_DisableTrigUser,
    AT_EnableRule,
    AT_EnableAlwaysRule,
    AT_EnableReplicaRule,
    AT_DisableRule,
    AT_AddInherit,
    AT_DropInherit,
    AT_AddOf,
    AT_DropOf,
    AT_ReplicaIdentity,
    AT_EnableRowSecurity,
    AT_DisableRowSecurity,
    AT_ForceRowSecurity,
    AT_NoForceRowSecurity,
    AT_GenericOptions,
    AT_AttachPartition,
    AT_DetachPartition,
    AT_DetachPartitionFinalize,
    AT_AddIdentity,
    AT_SetIdentity,
    AT_DropIdentity,
    AT_ReAddStatistics,
}

/// AlterTableCmd - one subcommand of an ALTER TABLE.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableCmd {
    pub subtype: AlterTableType,
    pub name: Option<String>,
    pub num: i16,
    pub newowner: Option<Box<RoleSpec>>,
    pub def: Option<Box<Node>>,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
    pub recurse: bool,
}

/// ATAlterConstraint - ad-hoc node for AT_AlterConstraint.
#[derive(Debug, Clone, PartialEq)]
pub struct ATAlterConstraint {
    pub conname: Option<String>,
    pub alterEnforceability: bool,
    pub is_enforced: bool,
    pub alterDeferrability: bool,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub alterInheritability: bool,
    pub noinherit: bool,
}

/// ReplicaIdentityStmt - ad-hoc node for AT_ReplicaIdentity.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaIdentityStmt {
    pub identity_type: i8,
    pub name: Option<String>,
}

/// AlterCollationStmt - Alter Collation.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterCollationStmt {
    pub collname: Vec<Box<Node>>,
}

/// AlterDomainStmt - Alter Domain.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterDomainStmt {
    /// T/N/O/C/X subtype code
    pub subtype: i8,
    pub typeName: Vec<Box<Node>>,
    pub name: Option<String>,
    pub def: Option<Box<Node>>,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
}

/// GrantTargetType - type of the grant target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantTargetType {
    /// grant on specific named object(s)
    ACL_TARGET_OBJECT,
    /// grant on all objects in given schema(s)
    ACL_TARGET_ALL_IN_SCHEMA,
    /// ALTER DEFAULT PRIVILEGES
    ACL_TARGET_DEFAULTS,
}

/// GrantStmt - Grant|Revoke Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct GrantStmt {
    pub is_grant: bool,
    pub targtype: GrantTargetType,
    pub objtype: ObjectType,
    pub objects: Vec<Box<Node>>,
    pub privileges: Vec<Box<Node>>,
    pub grantees: Vec<Box<Node>>,
    pub grant_option: bool,
    pub grantor: Option<Box<RoleSpec>>,
    pub behavior: DropBehavior,
}

/// ObjectWithArgs - function/procedure/operator name plus parameter ids.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectWithArgs {
    pub objname: Vec<Box<Node>>,
    pub objargs: Vec<Box<Node>>,
    pub objfuncargs: Vec<Box<Node>>,
    pub args_unspecified: bool,
}

/// AccessPriv - an access privilege with optional list of column names.
#[derive(Debug, Clone, PartialEq)]
pub struct AccessPriv {
    pub priv_name: Option<String>,
    pub cols: Vec<Box<Node>>,
}

/// GrantRoleStmt - Grant/Revoke Role Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct GrantRoleStmt {
    pub granted_roles: Vec<Box<Node>>,
    pub grantee_roles: Vec<Box<Node>>,
    pub is_grant: bool,
    pub opt: Vec<Box<Node>>,
    pub grantor: Option<Box<RoleSpec>>,
    pub behavior: DropBehavior,
}

/// AlterDefaultPrivilegesStmt - Alter Default Privileges Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterDefaultPrivilegesStmt {
    pub options: Vec<Box<Node>>,
    pub action: Option<Box<GrantStmt>>,
}

/// CopyStmt - Copy Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CopyStmt {
    pub relation: Option<Box<RangeVar>>,
    pub query: Option<Box<Node>>,
    pub attlist: Vec<Box<Node>>,
    pub is_from: bool,
    pub is_program: bool,
    pub filename: Option<String>,
    pub options: Vec<Box<Node>>,
    pub whereClause: Option<Box<Node>>,
}

/// VariableSetKind - SET/RESET variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariableSetKind {
    /// SET var = value
    VAR_SET_VALUE,
    /// SET var TO DEFAULT
    VAR_SET_DEFAULT,
    /// SET var FROM CURRENT
    VAR_SET_CURRENT,
    /// special case for SET TRANSACTION ...
    VAR_SET_MULTI,
    /// RESET var
    VAR_RESET,
    /// RESET ALL
    VAR_RESET_ALL,
}

/// VariableSetStmt - SET Statement (includes RESET).
#[derive(Debug, Clone, PartialEq)]
pub struct VariableSetStmt {
    pub kind: VariableSetKind,
    pub name: Option<String>,
    pub args: Vec<Box<Node>>,
    pub jumble_args: bool,
    pub is_local: bool,
    pub location: ParseLoc,
}

/// VariableShowStmt - Show Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct VariableShowStmt {
    pub name: Option<String>,
}

/// CreateStmt - Create Table Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateStmt {
    pub relation: Option<Box<RangeVar>>,
    pub tableElts: Vec<Box<Node>>,
    pub inhRelations: Vec<Box<Node>>,
    pub partbound: Option<Box<PartitionBoundSpec>>,
    pub partspec: Option<Box<PartitionSpec>>,
    pub ofTypename: Option<Box<TypeName>>,
    pub constraints: Vec<Box<Node>>,
    pub nnconstraints: Vec<Box<Node>>,
    pub options: Vec<Box<Node>>,
    pub oncommit: OnCommitAction,
    pub tablespacename: Option<String>,
    pub accessMethod: Option<String>,
    pub if_not_exists: bool,
}

/// ConstrType - types of constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstrType {
    /// not standard SQL, but expected
    CONSTR_NULL,
    CONSTR_NOTNULL,
    CONSTR_DEFAULT,
    CONSTR_IDENTITY,
    CONSTR_GENERATED,
    CONSTR_CHECK,
    CONSTR_PRIMARY,
    CONSTR_UNIQUE,
    CONSTR_EXCLUSION,
    CONSTR_FOREIGN,
    /// attributes for previous constraint node
    CONSTR_ATTR_DEFERRABLE,
    CONSTR_ATTR_NOT_DEFERRABLE,
    CONSTR_ATTR_DEFERRED,
    CONSTR_ATTR_IMMEDIATE,
    CONSTR_ATTR_ENFORCED,
    CONSTR_ATTR_NOT_ENFORCED,
}

/// Foreign key action codes (stored in pg_constraint).
pub const FKCONSTR_ACTION_NOACTION: i8 = b'a' as i8;
pub const FKCONSTR_ACTION_RESTRICT: i8 = b'r' as i8;
pub const FKCONSTR_ACTION_CASCADE: i8 = b'c' as i8;
pub const FKCONSTR_ACTION_SETNULL: i8 = b'n' as i8;
pub const FKCONSTR_ACTION_SETDEFAULT: i8 = b'd' as i8;

/// Foreign key matchtype codes.
pub const FKCONSTR_MATCH_FULL: i8 = b'f' as i8;
pub const FKCONSTR_MATCH_PARTIAL: i8 = b'p' as i8;
pub const FKCONSTR_MATCH_SIMPLE: i8 = b's' as i8;

/// Constraint - a table/column constraint.
#[derive(Debug, Clone, PartialEq)]
pub struct Constraint {
    pub contype: ConstrType,
    pub conname: Option<String>,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub is_enforced: bool,
    pub skip_validation: bool,
    pub initially_valid: bool,
    pub is_no_inherit: bool,
    pub raw_expr: Option<Box<Node>>,
    pub cooked_expr: Option<String>,
    pub generated_when: i8,
    pub generated_kind: i8,
    pub nulls_not_distinct: bool,
    pub keys: Vec<Box<Node>>,
    pub without_overlaps: bool,
    pub including: Vec<Box<Node>>,
    pub exclusions: Vec<Box<Node>>,
    pub options: Vec<Box<Node>>,
    pub indexname: Option<String>,
    pub indexspace: Option<String>,
    pub reset_default_tblspc: bool,
    pub access_method: Option<String>,
    pub where_clause: Option<Box<Node>>,
    pub pktable: Option<Box<RangeVar>>,
    pub fk_attrs: Vec<Box<Node>>,
    pub pk_attrs: Vec<Box<Node>>,
    pub fk_with_period: bool,
    pub pk_with_period: bool,
    pub fk_matchtype: i8,
    pub fk_upd_action: i8,
    pub fk_del_action: i8,
    pub fk_del_set_cols: Vec<Box<Node>>,
    pub old_conpfeqop: Vec<Box<Node>>,
    pub old_pktable_oid: Oid,
    pub location: ParseLoc,
}

/// CreateTableSpaceStmt - Create Table Space Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableSpaceStmt {
    pub tablespacename: Option<String>,
    pub owner: Option<Box<RoleSpec>>,
    pub location: Option<String>,
    pub options: Vec<Box<Node>>,
}

/// DropTableSpaceStmt - Drop Table Space Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableSpaceStmt {
    pub tablespacename: Option<String>,
    pub missing_ok: bool,
}

/// AlterTableSpaceOptionsStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableSpaceOptionsStmt {
    pub tablespacename: Option<String>,
    pub options: Vec<Box<Node>>,
    pub isReset: bool,
}

/// AlterTableMoveAllStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableMoveAllStmt {
    pub orig_tablespacename: Option<String>,
    pub objtype: ObjectType,
    pub roles: Vec<Box<Node>>,
    pub new_tablespacename: Option<String>,
    pub nowait: bool,
}

/// CreateExtensionStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateExtensionStmt {
    pub extname: Option<String>,
    pub if_not_exists: bool,
    pub options: Vec<Box<Node>>,
}

/// AlterExtensionStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterExtensionStmt {
    pub extname: Option<String>,
    pub options: Vec<Box<Node>>,
}

/// AlterExtensionContentsStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterExtensionContentsStmt {
    pub extname: Option<String>,
    /// +1 = add object, -1 = drop object
    pub action: i32,
    pub objtype: ObjectType,
    pub object: Option<Box<Node>>,
}

/// CreateFdwStmt - Create FOREIGN DATA WRAPPER.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateFdwStmt {
    pub fdwname: Option<String>,
    pub func_options: Vec<Box<Node>>,
    pub options: Vec<Box<Node>>,
}

/// AlterFdwStmt - Alter FOREIGN DATA WRAPPER.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterFdwStmt {
    pub fdwname: Option<String>,
    pub func_options: Vec<Box<Node>>,
    pub options: Vec<Box<Node>>,
}

/// CreateForeignServerStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateForeignServerStmt {
    pub servername: Option<String>,
    pub servertype: Option<String>,
    pub version: Option<String>,
    pub fdwname: Option<String>,
    pub if_not_exists: bool,
    pub options: Vec<Box<Node>>,
}

/// AlterForeignServerStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterForeignServerStmt {
    pub servername: Option<String>,
    pub version: Option<String>,
    pub options: Vec<Box<Node>>,
    pub has_version: bool,
}

/// CreateForeignTableStmt - Create FOREIGN TABLE Statement.
/// C embeds `CreateStmt base` as the first member (struct inheritance).
#[derive(Debug, Clone, PartialEq)]
pub struct CreateForeignTableStmt {
    pub base: CreateStmt,
    pub servername: Option<String>,
    pub options: Vec<Box<Node>>,
}

/// CreateUserMappingStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateUserMappingStmt {
    pub user: Option<Box<RoleSpec>>,
    pub servername: Option<String>,
    pub if_not_exists: bool,
    pub options: Vec<Box<Node>>,
}

/// AlterUserMappingStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterUserMappingStmt {
    pub user: Option<Box<RoleSpec>>,
    pub servername: Option<String>,
    pub options: Vec<Box<Node>>,
}

/// DropUserMappingStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct DropUserMappingStmt {
    pub user: Option<Box<RoleSpec>>,
    pub servername: Option<String>,
    pub missing_ok: bool,
}

/// ImportForeignSchemaType - type of table list in IMPORT FOREIGN SCHEMA.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportForeignSchemaType {
    /// all relations wanted
    FDW_IMPORT_SCHEMA_ALL,
    /// include only listed tables
    FDW_IMPORT_SCHEMA_LIMIT_TO,
    /// exclude listed tables
    FDW_IMPORT_SCHEMA_EXCEPT,
}

/// ImportForeignSchemaStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct ImportForeignSchemaStmt {
    pub server_name: Option<String>,
    pub remote_schema: Option<String>,
    pub local_schema: Option<String>,
    pub list_type: ImportForeignSchemaType,
    pub table_list: Vec<Box<Node>>,
    pub options: Vec<Box<Node>>,
}

/// CreatePolicyStmt - Create POLICY Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreatePolicyStmt {
    pub policy_name: Option<String>,
    pub table: Option<Box<RangeVar>>,
    pub cmd_name: Option<String>,
    pub permissive: bool,
    pub roles: Vec<Box<Node>>,
    pub qual: Option<Box<Node>>,
    pub with_check: Option<Box<Node>>,
}

/// AlterPolicyStmt - Alter POLICY Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterPolicyStmt {
    pub policy_name: Option<String>,
    pub table: Option<Box<RangeVar>>,
    pub roles: Vec<Box<Node>>,
    pub qual: Option<Box<Node>>,
    pub with_check: Option<Box<Node>>,
}

/// CreateAmStmt - Create ACCESS METHOD Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateAmStmt {
    pub amname: Option<String>,
    pub handler_name: Vec<Box<Node>>,
    pub amtype: i8,
}

/// CreateTrigStmt - Create TRIGGER Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTrigStmt {
    pub replace: bool,
    pub isconstraint: bool,
    pub trigname: Option<String>,
    pub relation: Option<Box<RangeVar>>,
    pub funcname: Vec<Box<Node>>,
    pub args: Vec<Box<Node>>,
    pub row: bool,
    /// BEFORE, AFTER, or INSTEAD (TRIGGER_TYPE bits)
    pub timing: i16,
    /// OR of INSERT/UPDATE/DELETE/TRUNCATE (TRIGGER_TYPE bits)
    pub events: i16,
    pub columns: Vec<Box<Node>>,
    pub whenClause: Option<Box<Node>>,
    pub transitionRels: Vec<Box<Node>>,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub constrrel: Option<Box<RangeVar>>,
}

/// CreateEventTrigStmt - Create EVENT TRIGGER Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateEventTrigStmt {
    pub trigname: Option<String>,
    pub eventname: Option<String>,
    pub whenclause: Vec<Box<Node>>,
    pub funcname: Vec<Box<Node>>,
}

/// AlterEventTrigStmt - Alter EVENT TRIGGER Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterEventTrigStmt {
    pub trigname: Option<String>,
    pub tgenabled: i8,
}

/// CreatePLangStmt - Create LANGUAGE Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreatePLangStmt {
    pub replace: bool,
    pub plname: Option<String>,
    pub plhandler: Vec<Box<Node>>,
    pub plinline: Vec<Box<Node>>,
    pub plvalidator: Vec<Box<Node>>,
    pub pltrusted: bool,
}

/// RoleStmtType - ROLE/USER/GROUP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleStmtType {
    ROLESTMT_ROLE,
    ROLESTMT_USER,
    ROLESTMT_GROUP,
}

/// CreateRoleStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateRoleStmt {
    pub stmt_type: RoleStmtType,
    pub role: Option<String>,
    pub options: Vec<Box<Node>>,
}

/// AlterRoleStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterRoleStmt {
    pub role: Option<Box<RoleSpec>>,
    pub options: Vec<Box<Node>>,
    /// +1 = add members, -1 = drop members
    pub action: i32,
}

/// AlterRoleSetStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterRoleSetStmt {
    pub role: Option<Box<RoleSpec>>,
    pub database: Option<String>,
    pub setstmt: Option<Box<VariableSetStmt>>,
}

/// DropRoleStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct DropRoleStmt {
    pub roles: Vec<Box<Node>>,
    pub missing_ok: bool,
}

/// CreateSeqStmt - Create SEQUENCE Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSeqStmt {
    pub sequence: Option<Box<RangeVar>>,
    pub options: Vec<Box<Node>>,
    pub ownerId: Oid,
    pub for_identity: bool,
    pub if_not_exists: bool,
}

/// AlterSeqStmt - Alter SEQUENCE Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterSeqStmt {
    pub sequence: Option<Box<RangeVar>>,
    pub options: Vec<Box<Node>>,
    pub for_identity: bool,
    pub missing_ok: bool,
}

/// DefineStmt - Create {Aggregate|Operator|Type} Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DefineStmt {
    pub kind: ObjectType,
    pub oldstyle: bool,
    pub defnames: Vec<Box<Node>>,
    pub args: Vec<Box<Node>>,
    pub definition: Vec<Box<Node>>,
    pub if_not_exists: bool,
    pub replace: bool,
}

/// CreateDomainStmt - Create Domain Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateDomainStmt {
    pub domainname: Vec<Box<Node>>,
    pub typeName: Option<Box<TypeName>>,
    pub collClause: Option<Box<CollateClause>>,
    pub constraints: Vec<Box<Node>>,
}

/// CreateOpClassStmt - Create Operator Class Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateOpClassStmt {
    pub opclassname: Vec<Box<Node>>,
    pub opfamilyname: Vec<Box<Node>>,
    pub amname: Option<String>,
    pub datatype: Option<Box<TypeName>>,
    pub items: Vec<Box<Node>>,
    pub isDefault: bool,
}

/// CreateOpClassItem item type codes.
pub const OPCLASS_ITEM_OPERATOR: i32 = 1;
pub const OPCLASS_ITEM_FUNCTION: i32 = 2;
pub const OPCLASS_ITEM_STORAGETYPE: i32 = 3;

/// CreateOpClassItem.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateOpClassItem {
    pub itemtype: i32,
    pub name: Option<Box<ObjectWithArgs>>,
    pub number: i32,
    pub order_family: Vec<Box<Node>>,
    pub class_args: Vec<Box<Node>>,
    pub storedtype: Option<Box<TypeName>>,
}

/// CreateOpFamilyStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateOpFamilyStmt {
    pub opfamilyname: Vec<Box<Node>>,
    pub amname: Option<String>,
}

/// AlterOpFamilyStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterOpFamilyStmt {
    pub opfamilyname: Vec<Box<Node>>,
    pub amname: Option<String>,
    pub isDrop: bool,
    pub items: Vec<Box<Node>>,
}

/// DropStmt - Drop Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropStmt {
    pub objects: Vec<Box<Node>>,
    pub removeType: ObjectType,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
    pub concurrent: bool,
}

/// TruncateStmt - Truncate Table Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct TruncateStmt {
    pub relations: Vec<Box<Node>>,
    pub restart_seqs: bool,
    pub behavior: DropBehavior,
}

/// CommentStmt - Comment On Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CommentStmt {
    pub objtype: ObjectType,
    pub object: Option<Box<Node>>,
    pub comment: Option<String>,
}

/// SecLabelStmt - SECURITY LABEL Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SecLabelStmt {
    pub objtype: ObjectType,
    pub object: Option<Box<Node>>,
    pub provider: Option<String>,
    pub label: Option<String>,
}

bitflags! {
    /// Cursor options (C: `CURSOR_OPT_*`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CursorOptions: i32 {
        /// BINARY
        const BINARY = 0x0001;
        /// SCROLL explicitly given
        const SCROLL = 0x0002;
        /// NO SCROLL explicitly given
        const NO_SCROLL = 0x0004;
        /// INSENSITIVE
        const INSENSITIVE = 0x0008;
        /// ASENSITIVE
        const ASENSITIVE = 0x0010;
        /// WITH HOLD
        const HOLD = 0x0020;
        /// prefer fast-start plan
        const FAST_PLAN = 0x0100;
        /// force use of generic plan
        const GENERIC_PLAN = 0x0200;
        /// force use of custom plan
        const CUSTOM_PLAN = 0x0400;
        /// parallel mode OK
        const PARALLEL_OK = 0x0800;
    }
}

/// DeclareCursorStmt - Declare Cursor Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DeclareCursorStmt {
    pub portalname: Option<String>,
    /// bitmask of CURSOR_OPT_* (CursorOptions)
    pub options: i32,
    pub query: Option<Box<Node>>,
}

/// ClosePortalStmt - Close Portal Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ClosePortalStmt {
    /// None means CLOSE ALL
    pub portalname: Option<String>,
}

/// FetchDirection - Fetch Statement direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchDirection {
    FETCH_FORWARD,
    FETCH_BACKWARD,
    FETCH_ABSOLUTE,
    FETCH_RELATIVE,
}

/// C: `FETCH_ALL` = `LONG_MAX`.
pub const FETCH_ALL: i64 = i64::MAX;

/// FetchStmt - Fetch Statement (also Move).
#[derive(Debug, Clone, PartialEq)]
pub struct FetchStmt {
    pub direction: FetchDirection,
    /// number of rows, or position argument
    pub howMany: i64,
    pub portalname: Option<String>,
    pub ismove: bool,
}

/// IndexStmt - Create Index Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexStmt {
    pub idxname: Option<String>,
    pub relation: Option<Box<RangeVar>>,
    pub accessMethod: Option<String>,
    pub tableSpace: Option<String>,
    pub indexParams: Vec<Box<Node>>,
    pub indexIncludingParams: Vec<Box<Node>>,
    pub options: Vec<Box<Node>>,
    pub whereClause: Option<Box<Node>>,
    pub excludeOpNames: Vec<Box<Node>>,
    pub idxcomment: Option<String>,
    pub indexOid: Oid,
    pub oldNumber: RelFileNumber,
    pub oldCreateSubid: SubTransactionId,
    pub oldFirstRelfilelocatorSubid: SubTransactionId,
    pub unique: bool,
    pub nulls_not_distinct: bool,
    pub primary: bool,
    pub isconstraint: bool,
    pub iswithoutoverlaps: bool,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub transformed: bool,
    pub concurrent: bool,
    pub if_not_exists: bool,
    pub reset_default_tblspc: bool,
}

/// CreateStatsStmt - Create Statistics Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateStatsStmt {
    pub defnames: Vec<Box<Node>>,
    pub stat_types: Vec<Box<Node>>,
    pub exprs: Vec<Box<Node>>,
    pub relations: Vec<Box<Node>>,
    pub stxcomment: Option<String>,
    pub transformed: bool,
    pub if_not_exists: bool,
}

/// StatsElem - statistics parameters (used in CREATE STATISTICS).
#[derive(Debug, Clone, PartialEq)]
pub struct StatsElem {
    pub name: Option<String>,
    pub expr: Option<Box<Node>>,
}

/// AlterStatsStmt - Alter Statistics Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterStatsStmt {
    pub defnames: Vec<Box<Node>>,
    pub stxstattarget: Option<Box<Node>>,
    pub missing_ok: bool,
}

/// CreateFunctionStmt - Create Function Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateFunctionStmt {
    pub is_procedure: bool,
    pub replace: bool,
    pub funcname: Vec<Box<Node>>,
    pub parameters: Vec<Box<Node>>,
    pub returnType: Option<Box<TypeName>>,
    pub options: Vec<Box<Node>>,
    pub sql_body: Option<Box<Node>>,
}

/// FunctionParameterMode - IN/OUT/etc (values appear in pg_proc).
#[repr(i8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionParameterMode {
    FUNC_PARAM_IN = b'i' as i8,
    FUNC_PARAM_OUT = b'o' as i8,
    FUNC_PARAM_INOUT = b'b' as i8,
    FUNC_PARAM_VARIADIC = b'v' as i8,
    FUNC_PARAM_TABLE = b't' as i8,
    FUNC_PARAM_DEFAULT = b'd' as i8,
}

/// FunctionParameter.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionParameter {
    pub name: Option<String>,
    pub argType: Option<Box<TypeName>>,
    pub mode: FunctionParameterMode,
    pub defexpr: Option<Box<Node>>,
    pub location: ParseLoc,
}

/// AlterFunctionStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterFunctionStmt {
    pub objtype: ObjectType,
    pub func: Option<Box<ObjectWithArgs>>,
    pub actions: Vec<Box<Node>>,
}

/// DoStmt - DO Statement (raw parser output).
#[derive(Debug, Clone, PartialEq)]
pub struct DoStmt {
    pub args: Vec<Box<Node>>,
}

/// InlineCodeBlock - execution-time API for DO (not a parse-tree member).
#[derive(Debug, Clone, PartialEq)]
pub struct InlineCodeBlock {
    pub source_text: Option<String>,
    pub langOid: Oid,
    pub langIsTrusted: bool,
    pub atomic: bool,
}

/// CallStmt - CALL statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CallStmt {
    pub funccall: Option<Box<FuncCall>>,
    pub funcexpr: Option<Box<FuncExpr>>,
    pub outargs: Vec<Box<Node>>,
}

/// CallContext - not a member of parse trees.
#[derive(Debug, Clone, PartialEq)]
pub struct CallContext {
    pub atomic: bool,
}

/// RenameStmt - Alter Object Rename Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct RenameStmt {
    pub renameType: ObjectType,
    pub relationType: ObjectType,
    pub relation: Option<Box<RangeVar>>,
    pub object: Option<Box<Node>>,
    pub subname: Option<String>,
    pub newname: Option<String>,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
}

/// AlterObjectDependsStmt - ALTER object DEPENDS ON EXTENSION.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterObjectDependsStmt {
    pub objectType: ObjectType,
    pub relation: Option<Box<RangeVar>>,
    pub object: Option<Box<Node>>,
    pub extname: Option<Box<String_>>,
    pub remove: bool,
}

/// AlterObjectSchemaStmt - ALTER object SET SCHEMA Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterObjectSchemaStmt {
    pub objectType: ObjectType,
    pub relation: Option<Box<RangeVar>>,
    pub object: Option<Box<Node>>,
    pub newschema: Option<String>,
    pub missing_ok: bool,
}

/// AlterOwnerStmt - Alter Object Owner Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterOwnerStmt {
    pub objectType: ObjectType,
    pub relation: Option<Box<RangeVar>>,
    pub object: Option<Box<Node>>,
    pub newowner: Option<Box<RoleSpec>>,
}

/// AlterOperatorStmt - Alter Operator Set ( this-n-that ).
#[derive(Debug, Clone, PartialEq)]
pub struct AlterOperatorStmt {
    pub opername: Option<Box<ObjectWithArgs>>,
    pub options: Vec<Box<Node>>,
}

/// AlterTypeStmt - Alter Type Set ( this-n-that ).
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTypeStmt {
    pub typeName: Vec<Box<Node>>,
    pub options: Vec<Box<Node>>,
}

/// RuleStmt - Create Rule Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct RuleStmt {
    pub relation: Option<Box<RangeVar>>,
    pub rulename: Option<String>,
    pub whereClause: Option<Box<Node>>,
    pub event: CmdType,
    pub instead: bool,
    pub actions: Vec<Box<Node>>,
    pub replace: bool,
}

/// NotifyStmt - Notify Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct NotifyStmt {
    pub conditionname: Option<String>,
    pub payload: Option<String>,
}

/// ListenStmt - Listen Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ListenStmt {
    pub conditionname: Option<String>,
}

/// UnlistenStmt - Unlisten Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct UnlistenStmt {
    pub conditionname: Option<String>,
}

/// TransactionStmtKind - {Begin|Commit|Rollback} Transaction Statement kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStmtKind {
    TRANS_STMT_BEGIN,
    /// semantically identical to BEGIN
    TRANS_STMT_START,
    TRANS_STMT_COMMIT,
    TRANS_STMT_ROLLBACK,
    TRANS_STMT_SAVEPOINT,
    TRANS_STMT_RELEASE,
    TRANS_STMT_ROLLBACK_TO,
    TRANS_STMT_PREPARE,
    TRANS_STMT_COMMIT_PREPARED,
    TRANS_STMT_ROLLBACK_PREPARED,
}

/// TransactionStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct TransactionStmt {
    pub kind: TransactionStmtKind,
    pub options: Vec<Box<Node>>,
    pub savepoint_name: Option<String>,
    pub gid: Option<String>,
    pub chain: bool,
    pub location: ParseLoc,
}

/// CompositeTypeStmt - Create Type Statement, composite types.
#[derive(Debug, Clone, PartialEq)]
pub struct CompositeTypeStmt {
    pub typevar: Option<Box<RangeVar>>,
    pub coldeflist: Vec<Box<Node>>,
}

/// CreateEnumStmt - Create Type Statement, enum types.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateEnumStmt {
    pub typeName: Vec<Box<Node>>,
    pub vals: Vec<Box<Node>>,
}

/// CreateRangeStmt - Create Type Statement, range types.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateRangeStmt {
    pub typeName: Vec<Box<Node>>,
    pub params: Vec<Box<Node>>,
}

/// AlterEnumStmt - Alter Type Statement, enum types.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterEnumStmt {
    pub typeName: Vec<Box<Node>>,
    pub oldVal: Option<String>,
    pub newVal: Option<String>,
    pub newValNeighbor: Option<String>,
    pub newValIsAfter: bool,
    pub skipIfNewValExists: bool,
}

/// ViewCheckOption - WITH CHECK OPTION.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewCheckOption {
    NO_CHECK_OPTION,
    LOCAL_CHECK_OPTION,
    CASCADED_CHECK_OPTION,
}

/// ViewStmt - Create View Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewStmt {
    pub view: Option<Box<RangeVar>>,
    pub aliases: Vec<Box<Node>>,
    pub query: Option<Box<Node>>,
    pub replace: bool,
    pub options: Vec<Box<Node>>,
    pub withCheckOption: ViewCheckOption,
}

/// LoadStmt - Load Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadStmt {
    pub filename: Option<String>,
}

/// CreatedbStmt - Createdb Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreatedbStmt {
    pub dbname: Option<String>,
    pub options: Vec<Box<Node>>,
}

/// AlterDatabaseStmt - Alter Database.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterDatabaseStmt {
    pub dbname: Option<String>,
    pub options: Vec<Box<Node>>,
}

/// AlterDatabaseRefreshCollStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterDatabaseRefreshCollStmt {
    pub dbname: Option<String>,
}

/// AlterDatabaseSetStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterDatabaseSetStmt {
    pub dbname: Option<String>,
    pub setstmt: Option<Box<VariableSetStmt>>,
}

/// DropdbStmt - Dropdb Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropdbStmt {
    pub dbname: Option<String>,
    pub missing_ok: bool,
    pub options: Vec<Box<Node>>,
}

/// AlterSystemStmt - Alter System Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterSystemStmt {
    pub setstmt: Option<Box<VariableSetStmt>>,
}

/// ClusterStmt - Cluster Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterStmt {
    pub relation: Option<Box<RangeVar>>,
    pub indexname: Option<String>,
    pub params: Vec<Box<Node>>,
}

/// VacuumStmt - Vacuum and Analyze Statements.
#[derive(Debug, Clone, PartialEq)]
pub struct VacuumStmt {
    pub options: Vec<Box<Node>>,
    pub rels: Vec<Box<Node>>,
    pub is_vacuumcmd: bool,
}

/// VacuumRelation - a single target table of VACUUM/ANALYZE.
#[derive(Debug, Clone, PartialEq)]
pub struct VacuumRelation {
    pub relation: Option<Box<RangeVar>>,
    pub oid: Oid,
    pub va_cols: Vec<Box<Node>>,
}

/// ExplainStmt - Explain Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStmt {
    pub query: Option<Box<Node>>,
    pub options: Vec<Box<Node>>,
}

/// CreateTableAsStmt - CREATE TABLE AS Statement (a/k/a SELECT INTO).
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableAsStmt {
    pub query: Option<Box<Node>>,
    pub into: Option<Box<IntoClause>>,
    pub objtype: ObjectType,
    pub is_select_into: bool,
    pub if_not_exists: bool,
}

/// RefreshMatViewStmt - REFRESH MATERIALIZED VIEW Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct RefreshMatViewStmt {
    pub concurrent: bool,
    pub skipData: bool,
    pub relation: Option<Box<RangeVar>>,
}

/// CheckPointStmt - Checkpoint Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckPointStmt {}

/// DiscardMode - Discard Statement target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardMode {
    DISCARD_ALL,
    DISCARD_PLANS,
    DISCARD_SEQUENCES,
    DISCARD_TEMP,
}

/// DiscardStmt - Discard Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DiscardStmt {
    pub target: DiscardMode,
}

/// LockStmt - LOCK Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct LockStmt {
    pub relations: Vec<Box<Node>>,
    pub mode: i32,
    pub nowait: bool,
}

/// ConstraintsSetStmt - SET CONSTRAINTS Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ConstraintsSetStmt {
    pub constraints: Vec<Box<Node>>,
    pub deferred: bool,
}

/// ReindexObjectType - REINDEX target kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReindexObjectType {
    REINDEX_OBJECT_INDEX,
    REINDEX_OBJECT_TABLE,
    REINDEX_OBJECT_SCHEMA,
    REINDEX_OBJECT_SYSTEM,
    REINDEX_OBJECT_DATABASE,
}

/// ReindexStmt - REINDEX Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ReindexStmt {
    pub kind: ReindexObjectType,
    pub relation: Option<Box<RangeVar>>,
    pub name: Option<String>,
    pub params: Vec<Box<Node>>,
}

/// CreateConversionStmt - CREATE CONVERSION Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateConversionStmt {
    pub conversion_name: Vec<Box<Node>>,
    pub for_encoding_name: Option<String>,
    pub to_encoding_name: Option<String>,
    pub func_name: Vec<Box<Node>>,
    pub def: bool,
}

/// CreateCastStmt - CREATE CAST Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateCastStmt {
    pub sourcetype: Option<Box<TypeName>>,
    pub targettype: Option<Box<TypeName>>,
    pub func: Option<Box<ObjectWithArgs>>,
    pub context: CoercionContext,
    pub inout: bool,
}

/// CreateTransformStmt - CREATE TRANSFORM Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTransformStmt {
    pub replace: bool,
    pub type_name: Option<Box<TypeName>>,
    pub lang: Option<String>,
    pub fromsql: Option<Box<ObjectWithArgs>>,
    pub tosql: Option<Box<ObjectWithArgs>>,
}

/// PrepareStmt - PREPARE Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct PrepareStmt {
    pub name: Option<String>,
    pub argtypes: Vec<Box<Node>>,
    pub query: Option<Box<Node>>,
}

/// ExecuteStmt - EXECUTE Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecuteStmt {
    pub name: Option<String>,
    pub params: Vec<Box<Node>>,
}

/// DeallocateStmt - DEALLOCATE Statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DeallocateStmt {
    /// None if DEALLOCATE ALL
    pub name: Option<String>,
    pub isall: bool,
    pub location: ParseLoc,
}

/// DropOwnedStmt - DROP OWNED statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropOwnedStmt {
    pub roles: Vec<Box<Node>>,
    pub behavior: DropBehavior,
}

/// ReassignOwnedStmt - REASSIGN OWNED statement.
#[derive(Debug, Clone, PartialEq)]
pub struct ReassignOwnedStmt {
    pub roles: Vec<Box<Node>>,
    pub newrole: Option<Box<RoleSpec>>,
}

/// AlterTSDictionaryStmt - TS Dictionary stmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTSDictionaryStmt {
    pub dictname: Vec<Box<Node>>,
    pub options: Vec<Box<Node>>,
}

/// AlterTSConfigType - TS Configuration stmt kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterTSConfigType {
    ALTER_TSCONFIG_ADD_MAPPING,
    ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN,
    ALTER_TSCONFIG_REPLACE_DICT,
    ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN,
    ALTER_TSCONFIG_DROP_MAPPING,
}

/// AlterTSConfigurationStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTSConfigurationStmt {
    pub kind: AlterTSConfigType,
    pub cfgname: Vec<Box<Node>>,
    pub tokentype: Vec<Box<Node>>,
    pub dicts: Vec<Box<Node>>,
    pub r#override: bool,
    pub replace: bool,
    pub missing_ok: bool,
}

/// PublicationTable.
#[derive(Debug, Clone, PartialEq)]
pub struct PublicationTable {
    pub relation: Option<Box<RangeVar>>,
    pub whereClause: Option<Box<Node>>,
    pub columns: Vec<Box<Node>>,
}

/// PublicationObjSpecType - Publication object type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationObjSpecType {
    /// A table
    PUBLICATIONOBJ_TABLE,
    /// All tables in schema
    PUBLICATIONOBJ_TABLES_IN_SCHEMA,
    /// All tables in first search_path element
    PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA,
    /// Continuation of previous type
    PUBLICATIONOBJ_CONTINUATION,
}

/// PublicationObjSpec.
#[derive(Debug, Clone, PartialEq)]
pub struct PublicationObjSpec {
    pub pubobjtype: PublicationObjSpecType,
    pub name: Option<String>,
    pub pubtable: Option<Box<PublicationTable>>,
    pub location: ParseLoc,
}

/// CreatePublicationStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct CreatePublicationStmt {
    pub pubname: Option<String>,
    pub options: Vec<Box<Node>>,
    pub pubobjects: Vec<Box<Node>>,
    pub for_all_tables: bool,
}

/// AlterPublicationAction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterPublicationAction {
    /// add objects to publication
    AP_AddObjects,
    /// remove objects from publication
    AP_DropObjects,
    /// set list of objects
    AP_SetObjects,
}

/// AlterPublicationStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterPublicationStmt {
    pub pubname: Option<String>,
    pub options: Vec<Box<Node>>,
    pub pubobjects: Vec<Box<Node>>,
    pub for_all_tables: bool,
    pub action: AlterPublicationAction,
}

/// CreateSubscriptionStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSubscriptionStmt {
    pub subname: Option<String>,
    pub conninfo: Option<String>,
    pub publication: Vec<Box<Node>>,
    pub options: Vec<Box<Node>>,
}

/// AlterSubscriptionType.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterSubscriptionType {
    ALTER_SUBSCRIPTION_OPTIONS,
    ALTER_SUBSCRIPTION_CONNECTION,
    ALTER_SUBSCRIPTION_SET_PUBLICATION,
    ALTER_SUBSCRIPTION_ADD_PUBLICATION,
    ALTER_SUBSCRIPTION_DROP_PUBLICATION,
    ALTER_SUBSCRIPTION_REFRESH,
    ALTER_SUBSCRIPTION_ENABLED,
    ALTER_SUBSCRIPTION_SKIP,
}

/// AlterSubscriptionStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterSubscriptionStmt {
    pub kind: AlterSubscriptionType,
    pub subname: Option<String>,
    pub conninfo: Option<String>,
    pub publication: Vec<Box<Node>>,
    pub options: Vec<Box<Node>>,
}

/// DropSubscriptionStmt.
#[derive(Debug, Clone, PartialEq)]
pub struct DropSubscriptionStmt {
    pub subname: Option<String>,
    pub missing_ok: bool,
    pub behavior: DropBehavior,
}
