//! Translated from PostgreSQL src/include/parser/parse_node.h

use crate::access::attnum::AttrNumber;
use crate::c::Index;
use crate::nodes::nodes::{Node, ParseLoc};
use crate::nodes::parsenodes::{
    A_Const, ColumnRef, CommonTableExpr, ParamRef, RTEPermissionInfo, RangeTblEntry,
};
use crate::nodes::primnodes::{Alias, Const, Param, SubscriptingRef, VarReturningType};
use crate::postgres_ext::Oid;
use crate::utils::elog::ErrorContextCallback;
use crate::utils::queryenvironment::QueryEnvironment;
use std::sync::Arc;
use crate::utils::rel::RelationData;

/// Expression kinds distinguished by transformExpr().  Used so that
/// context-specific error messages can be printed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseExprKind {
    /// "not in an expression"
    None = 0,
    /// reserved for extensions
    Other,
    /// JOIN ON
    JoinOn,
    /// JOIN USING
    JoinUsing,
    /// sub-SELECT in FROM clause
    FromSubselect,
    /// function in FROM clause
    FromFunction,
    /// WHERE
    Where,
    /// HAVING
    Having,
    /// FILTER
    Filter,
    /// window definition PARTITION BY
    WindowPartition,
    /// window definition ORDER BY
    WindowOrder,
    /// window frame clause with RANGE
    WindowFrameRange,
    /// window frame clause with ROWS
    WindowFrameRows,
    /// window frame clause with GROUPS
    WindowFrameGroups,
    /// SELECT target list item
    SelectTarget,
    /// INSERT target list item
    InsertTarget,
    /// UPDATE assignment source item
    UpdateSource,
    /// UPDATE assignment target item
    UpdateTarget,
    /// MERGE WHEN [NOT] MATCHED condition
    MergeWhen,
    /// GROUP BY
    GroupBy,
    /// ORDER BY
    OrderBy,
    /// DISTINCT ON
    DistinctOn,
    /// LIMIT
    Limit,
    /// OFFSET
    Offset,
    /// RETURNING in INSERT/UPDATE/DELETE
    Returning,
    /// RETURNING in MERGE
    MergeReturning,
    /// VALUES
    Values,
    /// single-row VALUES (in INSERT only)
    ValuesSingle,
    /// CHECK constraint for a table
    CheckConstraint,
    /// CHECK constraint for a domain
    DomainCheck,
    /// default value for a table column
    ColumnDefault,
    /// default parameter value for function
    FunctionDefault,
    /// index expression
    IndexExpression,
    /// index predicate
    IndexPredicate,
    /// extended statistics expression
    StatsExpression,
    /// transform expr in ALTER COLUMN TYPE
    AlterColTransform,
    /// parameter value in EXECUTE
    ExecuteParameter,
    /// WHEN condition in CREATE TRIGGER
    TriggerWhen,
    /// USING or WITH CHECK expr in policy
    Policy,
    /// partition bound expression
    PartitionBound,
    /// PARTITION BY expression
    PartitionExpression,
    /// procedure argument in CALL
    CallArgument,
    /// WHERE condition in COPY FROM
    CopyWhere,
    /// generation expression for a column
    GeneratedColumn,
    /// cycle mark value
    CycleMark,
}

// Function signatures for parser hooks. Per function-mapping.md these are
// runtime-pluggable callbacks; modeled as fn pointers (the `void *arg`
// passthrough is `p_ref_hook_state` below).

/// C: `Node *(*PreParseColumnRefHook)(ParseState *pstate, ColumnRef *cref);`
pub type PreParseColumnRefHook =
    fn(pstate: &mut ParseState, cref: &mut ColumnRef) -> Option<Node>;
/// C: `Node *(*PostParseColumnRefHook)(ParseState *pstate, ColumnRef *cref, Node *var);`
pub type PostParseColumnRefHook =
    fn(pstate: &mut ParseState, cref: &mut ColumnRef, var: Option<Node>) -> Option<Node>;
/// C: `Node *(*ParseParamRefHook)(ParseState *pstate, ParamRef *pref);`
pub type ParseParamRefHook =
    fn(pstate: &mut ParseState, pref: &mut ParamRef) -> Option<Node>;
/// C: `Node *(*CoerceParamHook)(ParseState *, Param *, Oid, int32, int);`
pub type CoerceParamHook = fn(
    pstate: &mut ParseState,
    param: &mut Param,
    target_type_id: Oid,
    target_type_mod: i32,
    location: i32,
) -> Option<Node>;

/// State information used during parse analysis.
///
/// Resolves the `crate::nodes::params::ParseState` forward declaration.
pub struct ParseState {
    /// stack link (NULL in a top-level ParseState)
    pub parent_parse_state: Option<Box<Self>>,
    /// source text, or None if not available
    pub p_sourcetext: Option<String>,
    /// range table so far
    pub p_rtable: Vec<RangeTblEntry>,
    /// RTEPermissionInfo nodes for each RELATION entry in rtable
    pub p_rteperminfos: Vec<RTEPermissionInfo>,
    /// JoinExprs for JOIN p_rtable entries (NULLs for non-join RTEs)
    pub p_joinexprs: Vec<Option<Node>>,
    /// Bitmapsets showing nulling outer joins
    pub p_nullingrels: Vec<Option<crate::nodes::bitmapset::Bitmapset>>,
    /// join items so far (will become FromExpr node's fromlist)
    pub p_joinlist: Vec<Node>,
    /// currently-referenceable RTEs (list of ParseNamespaceItem)
    pub p_namespace: Vec<ParseNamespaceItem>,
    /// lateral_only items visible?
    pub p_lateral_active: bool,
    /// current namespace for common table exprs
    pub p_ctenamespace: Vec<CommonTableExpr>,
    /// common table exprs not yet in namespace
    pub p_future_ctes: Vec<CommonTableExpr>,
    /// this query's containing CTE
    pub p_parent_cte: Option<Box<CommonTableExpr>>,
    /// INSERT/UPDATE/DELETE/MERGE target rel (None = no target)
    pub p_target_relation: Option<Arc<RelationData>>,
    /// target rel's NSItem, or None
    pub p_target_nsitem: Option<Box<ParseNamespaceItem>>,
    /// NSItem for grouping, or None
    pub p_grouping_nsitem: Option<Box<ParseNamespaceItem>>,
    /// process assignment like INSERT not UPDATE
    pub p_is_insert: bool,
    /// raw representations of window clauses
    pub p_windowdefs: Vec<Node>,
    /// what kind of expression we're parsing
    pub p_expr_kind: ParseExprKind,
    /// next targetlist resno to assign
    pub p_next_resno: i32,
    /// junk tlist entries for multiassign
    pub p_multiassign_exprs: Vec<Node>,
    /// raw FOR UPDATE/FOR SHARE info
    pub p_locking_clause: Vec<Node>,
    /// parent has marked this subquery with FOR UPDATE/FOR SHARE
    pub p_locked_from_parent: bool,
    /// resolve unknown-type SELECT outputs as type text
    pub p_resolve_unknowns: bool,
    /// curr env, incl refs to enclosing env
    pub p_query_env: Option<Box<QueryEnvironment>>,

    // Flags telling about things found in the query:
    pub p_has_aggs: bool,
    pub p_has_window_funcs: bool,
    pub p_has_target_srfs: bool,
    pub p_has_sub_links: bool,
    pub p_has_modifying_cte: bool,

    /// most recent set-returning func/op found
    pub p_last_srf: Option<Node>,

    // Optional hook functions for parser callbacks. None unless set up by the
    // caller of make_parsestate.
    pub p_pre_columnref_hook: Option<PreParseColumnRefHook>,
    pub p_post_columnref_hook: Option<PostParseColumnRefHook>,
    pub p_paramref_hook: Option<ParseParamRefHook>,
    pub p_coerce_param_hook: Option<CoerceParamHook>,
    /// common passthrough link for the hooks above (C `void *p_ref_hook_state`)
    pub p_ref_hook_state: Option<Node>, // TODO(ptr): opaque hook state
}

/// An element of a namespace list.
pub struct ParseNamespaceItem {
    /// Table and column names
    pub names: Box<Alias>,
    /// The relation's rangetable entry
    pub rte: Box<RangeTblEntry>,
    /// The relation's index in the rangetable
    pub rtindex: i32,
    /// The relation's rteperminfos entry
    pub perminfo: Option<Box<RTEPermissionInfo>>,
    /// per-column data (array of same length as names->colnames)
    pub nscolumns: Vec<ParseNamespaceColumn>,
    /// Arc<RelationData> name is visible?
    pub rel_visible: bool,
    /// Column names visible as unqualified refs?
    pub cols_visible: bool,
    /// Is only visible to LATERAL expressions?
    pub lateral_only: bool,
    /// If so, does join type allow use?
    pub lateral_ok: bool,
    /// Is OLD/NEW for use in RETURNING?
    pub returning_type: VarReturningType,
}

/// Data about one column of a ParseNamespaceItem.
pub struct ParseNamespaceColumn {
    /// rangetable index
    pub varno: Index,
    /// attribute number of the column
    pub varattno: AttrNumber,
    /// pg_type OID
    pub vartype: Oid,
    /// type modifier value
    pub vartypmod: i32,
    /// OID of collation, or InvalidOid
    pub varcollid: Oid,
    /// for RETURNING OLD/NEW
    pub varreturningtype: VarReturningType,
    /// rangetable index of syntactic referent
    pub varnosyn: Index,
    /// attribute number of syntactic referent
    pub varattnosyn: AttrNumber,
    /// not included in star expansion
    pub dontexpand: bool,
}

/// Support for parser_errposition_callback function.
pub struct ParseCallbackState {
    pub pstate: Option<Box<ParseState>>, // TODO(ptr): borrow of pstate
    pub location: i32,
    pub errcallback: ErrorContextCallback,
}

/// PG `make_parsestate`. See `crate::backend::parser::parse_node`.
pub use crate::backend::parser::parse_node::make_parsestate;

/// PG `free_parsestate`. See `crate::backend::parser::parse_node`.
pub use crate::backend::parser::parse_node::free_parsestate;

pub fn parser_errposition(_pstate: &mut ParseState, _location: i32) -> i32 {
    unimplemented!()
}

pub fn setup_parser_errposition_callback(
    _pcbstate: &mut ParseCallbackState,
    _pstate: &mut ParseState,
    _location: i32,
) {
    unimplemented!()
}

pub fn cancel_parser_errposition_callback(_pcbstate: &mut ParseCallbackState) {
    unimplemented!()
}

/// C: out-params `Oid *containerType, int32 *containerTypmod` -> (Oid, i32).
pub fn transform_container_type(_container_type: Oid, _container_typmod: i32) -> (Oid, i32) {
    unimplemented!()
}

pub fn transform_container_subscripts(
    _pstate: &mut ParseState,
    _container_base: Option<Node>,
    _container_type: Oid,
    _container_typ_mod: i32,
    _indirection: Vec<Node>,
    _is_assignment: bool,
) -> Box<SubscriptingRef> {
    unimplemented!()
}

/// PG `make_const`. See `crate::backend::parser::parse_node`.
pub use crate::backend::parser::parse_node::make_const;
