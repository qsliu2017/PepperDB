//! Translated from PostgreSQL src/include/nodes/pathnodes.h
//!
//! Planner internal data structures. All in-memory (no on-disk layout), so
//! everything is idiomatic Rust. Many of these carry a NodeTag in C and so get
//! a `Node` variant; the planner-internal cross-links stay concrete boxed types
//! rather than `Node`.

use bitflags::bitflags;

use crate::access::attnum::AttrNumber;
use crate::access::cmptype::CompareType;
use crate::access::sdir::ScanDirection;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::{
    AggSplit, AggStrategy, Cardinality, CmdType, Cost, JoinType, LimitOption, Node, ParseLoc,
    Selectivity, SetOpCmd, SetOpStrategy,
};
use crate::nodes::parsenodes::{Query, RTEKind, WindowClause};
use crate::nodes::primnodes::{OnConflictExpr, Param, Var};
use crate::pg_config_manual::INDEX_MAX_KEYS;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;

/// Set of relation identifiers (indexes into the rangetable).
pub type Relids = Bitmapset;

/// Cheapest-startup vs cheapest-total selector.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostSelector {
    STARTUP_COST,
    TOTAL_COST,
}

/// One-time (startup) cost plus a per-tuple cost.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QualCost {
    pub startup: Cost,
    pub per_tuple: Cost,
}

/// Statistics about the aggregates executed by a given Agg node.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AggClauseCosts {
    /// Total per-input-row execution costs.
    pub trans_cost: QualCost,
    /// Total per-aggregated-row costs.
    pub final_cost: QualCost,
    /// Space for pass-by-ref transition data.
    pub transition_space: usize,
}

/// Types of "upper" (post-scan/join) relations dealt with during planning.
/// `FINAL` must be last; it sizes arrays.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpperRelationKind {
    SETOP,
    PARTIAL_GROUP_AGG,
    GROUP_AGG,
    WINDOW,
    PARTIAL_DISTINCT,
    DISTINCT,
    ORDERED,
    FINAL,
}

/// Number of upper-rel slots (FINAL + 1).
pub const UPPERREL_COUNT: usize = UpperRelationKind::FINAL as usize + 1;

/// Global information for an entire planner invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannerGlobal {
    // ParamListInfo boundParams -> opaque planner input (read_write_ignore in C,
    // and not Clone/Eq); dropped from the skeleton.
    /// Plans for SubPlan nodes.
    pub subplans: Vec<Node>,
    /// The SubPlan expression nodes themselves (testexpr/setParam/parParam/args),
    /// parallel to `subplans` by `plan_id`. PG keeps these in the expression tree /
    /// init_plans list; the port collects them here so the executor can build the
    /// SubPlan run-states for every subplan referenced by the plan (M12, step 44).
    pub subplan_nodes: Vec<crate::nodes::primnodes::SubPlan>,
    /// Paths from which the SubPlan Plans were made.
    pub subpaths: Vec<Box<Path>>,
    /// PlannerInfos for SubPlan nodes.
    pub subroots: Vec<Box<PlannerInfo>>,
    /// Indices of subplans that require REWIND.
    pub rewind_plan_ids: Option<Bitmapset>,
    /// "flat" rangetable for executor.
    pub finalrtable: Vec<Node>,
    /// RT indexes of all relation RTEs in finalrtable.
    pub all_relids: Option<Bitmapset>,
    /// RT indexes of leaf partitions subject to initial pruning.
    pub prunable_relids: Option<Bitmapset>,
    /// "flat" list of RTEPermissionInfos.
    pub finalrteperminfos: Vec<Node>,
    /// "flat" list of PlanRowMarks.
    pub finalrowmarks: Vec<Node>,
    /// "flat" list of integer RT indexes.
    pub result_relations: Vec<i32>,
    /// "flat" list of AppendRelInfos.
    pub append_relations: Vec<Box<AppendRelInfo>>,
    /// "flat" list of PartitionPruneInfos.
    pub part_prune_infos: Vec<Node>,
    /// OIDs of relations the plan depends on.
    pub relation_oids: Vec<Oid>,
    /// Other dependencies, as PlanInvalItems.
    pub inval_items: Vec<Node>,
    /// Type OIDs for EXEC Params.
    pub param_exec_types: Vec<Oid>,
    /// Highest PlaceHolderVar ID assigned.
    pub last_phid: usize,
    /// Highest PlanRowMark ID assigned.
    pub last_row_mark_id: usize,
    /// Highest plan node ID assigned.
    pub last_plan_node_id: i32,
    /// Redo plan when TransactionXmin changes?
    pub transient_plan: bool,
    /// Is plan specific to current role?
    pub depends_on_role: bool,
    /// Parallel mode potentially OK?
    pub parallel_mode_ok: bool,
    /// Parallel mode actually required?
    pub parallel_mode_needed: bool,
    /// Worst PROPARALLEL hazard level.
    pub max_parallel_hazard: u8,
    // PartitionDirectory partition_directory -> opaque planner state, dropped.
}

/// Per-query planner state, conventionally called "root".
#[derive(Debug, Clone, PartialEq)]
pub struct PlannerInfo {
    /// The Query being planned.
    pub parse: Box<Query>,
    /// Global info for current planner run.
    pub glob: Box<PlannerGlobal>,
    /// 1 at the outermost Query.
    pub query_level: usize,
    /// NULL at outermost Query.
    pub parent_root: Option<Box<Self>>,
    /// List of PlannerParamItems.
    pub plan_params: Vec<Box<PlannerParamItem>>,
    pub outer_params: Option<Bitmapset>,
    /// Pointers to base/other rels, indexed by RT index (entry 0 wasted).
    pub simple_rel_array: Vec<Option<Box<RelOptInfo>>>,
    /// Associated rangetable entries (same length as simple_rel_array).
    pub simple_rte_array: Vec<Option<Box<crate::nodes::parsenodes::RangeTblEntry>>>,
    /// AppendRelInfo per child_relid, or NULL.
    pub append_rel_array: Vec<Option<Box<AppendRelInfo>>>,
    /// All base relids (not joins/"other") in the query.
    pub all_baserels: Option<Relids>,
    /// All outer-join relids in the query.
    pub outer_join_rels: Option<Relids>,
    /// All base+OJ relids (not "other"); identifier of the final join.
    pub all_query_rels: Option<Relids>,
    /// All join-relation RelOptInfos considered this run.
    pub join_rel_list: Vec<Box<RelOptInfo>>,
    // HTAB *join_rel_hash -> derived HashMap, dropped from skeleton.
    /// Per-level lists of join-relation RelOptInfos (DP join search).
    pub join_rel_level: Vec<Vec<Box<RelOptInfo>>>,
    /// Index of list being extended.
    pub join_cur_level: i32,
    /// Init SubPlans for query.
    pub init_plans: Vec<Node>,
    /// Per-CTE-item subplan IDs (-1 if none).
    pub cte_plan_ids: Vec<i32>,
    /// Lists of Lists of Params for MULTIEXPR subquery outputs.
    pub multiexpr_params: Vec<Node>,
    /// JoinDomains used in the query (higher ones first).
    pub join_domains: Vec<Box<JoinDomain>>,
    /// Active EquivalenceClasses.
    pub eq_classes: Vec<Box<EquivalenceClass>>,
    /// Set true once ECs are canonical.
    pub ec_merging_done: bool,
    /// "canonical" PathKeys.
    pub canon_pathkeys: Vec<Box<PathKey>>,
    /// OuterJoinClauseInfos for mergejoinable left-side clauses.
    pub left_join_clauses: Vec<Box<OuterJoinClauseInfo>>,
    /// OuterJoinClauseInfos for mergejoinable right-side clauses.
    pub right_join_clauses: Vec<Box<OuterJoinClauseInfo>>,
    /// OuterJoinClauseInfos for mergejoinable full join clauses.
    pub full_join_clauses: Vec<Box<OuterJoinClauseInfo>>,
    /// SpecialJoinInfos.
    pub join_info_list: Vec<Box<SpecialJoinInfo>>,
    /// Counter for assigning RestrictInfo serial numbers.
    pub last_rinfo_serial: i32,
    /// Set of all result relids.
    pub all_result_relids: Option<Relids>,
    /// Set of all leaf relids.
    pub leaf_result_relids: Option<Relids>,
    /// AppendRelInfos.
    pub append_rel_list: Vec<Box<AppendRelInfo>>,
    /// RowIdentityVarInfos.
    pub row_identity_vars: Vec<Box<RowIdentityVarInfo>>,
    /// PlanRowMarks.
    pub row_marks: Vec<Node>,
    /// PlaceHolderInfos.
    pub placeholder_list: Vec<Box<PlaceHolderInfo>>,
    /// PlaceHolderInfos indexed by phid.
    pub placeholder_array: Vec<Option<Box<PlaceHolderInfo>>>,
    /// ForeignKeyOptInfos.
    pub fkey_list: Vec<Box<ForeignKeyOptInfo>>,
    /// Desired pathkeys for query_planner().
    pub query_pathkeys: Vec<Box<PathKey>>,
    /// groupClause pathkeys, if any.
    pub group_pathkeys: Vec<Box<PathKey>>,
    /// Number of group_pathkeys belonging to the GROUP BY clause.
    pub num_groupby_pathkeys: i32,
    /// Pathkeys of bottom window, if any.
    pub window_pathkeys: Vec<Box<PathKey>>,
    /// distinctClause pathkeys, if any.
    pub distinct_pathkeys: Vec<Box<PathKey>>,
    /// sortClause pathkeys, if any.
    pub sort_pathkeys: Vec<Box<PathKey>>,
    /// Set operator pathkeys, if any.
    pub setop_pathkeys: Vec<Box<PathKey>>,
    /// Canonicalised partition schemes used in the query.
    pub part_schemes: Vec<PartitionScheme>,
    /// RelOptInfos we are now trying to join.
    pub initial_rels: Vec<Box<RelOptInfo>>,
    /// Upper-rel RelOptInfos, indexed by UpperRelationKind.
    pub upper_rels: [Vec<Box<RelOptInfo>>; UPPERREL_COUNT],
    /// Result tlists for upper-stage processing, indexed by UpperRelationKind.
    pub upper_targets: [Option<Box<PathTarget>>; UPPERREL_COUNT],
    /// Fully-processed groupClause.
    pub processed_group_clause: Vec<Node>,
    /// Fully-processed distinctClause.
    pub processed_distinct_clause: Vec<Node>,
    /// Fully-processed targetlist.
    pub processed_tlist: Vec<Node>,
    /// The scan/join (group-input) targetlist the base scan should compute: the
    /// flattened Vars feeding the grouping/aggregation. Empty when the scan target
    /// equals `processed_tlist` (no grouping). (Port helper; PG carries this as the
    /// scanjoin/grouping PathTargets in `upper_targets`.)
    pub scan_input_tlist: Vec<Node>,
    /// UPDATE target attribute numbers for first N processed_tlist entries.
    pub update_colnos: Vec<i32>,
    /// For GroupingFunc fixup.
    pub grouping_map: Vec<AttrNumber>,
    /// MinMaxAggInfos.
    pub minmax_aggs: Vec<Box<MinMaxAggInfo>>,
    // MemoryContext planner_cxt -> RAII arena, dropped from skeleton.
    /// # of pages in all non-dummy tables of query.
    pub total_table_pages: Cardinality,
    /// tuple_fraction passed to query_planner.
    pub tuple_fraction: Selectivity,
    /// limit_tuples passed to query_planner.
    pub limit_tuples: Cardinality,
    /// Minimum security_level for quals (0 if no securityQuals).
    pub qual_security_level: usize,
    pub has_join_rtes: bool,
    pub has_lateral_rtes: bool,
    pub has_having_qual: bool,
    pub has_pseudo_constant_quals: bool,
    pub has_alternative_subplans: bool,
    pub placeholders_frozen: bool,
    pub has_recursion: bool,
    /// RT index for the GROUP RTE, or 0 if none.
    pub group_rtindex: i32,
    /// AggInfo structs.
    pub agginfos: Vec<Box<AggInfo>>,
    /// AggTransInfo structs.
    pub aggtransinfos: Vec<Box<AggTransInfo>>,
    /// Number of aggs with DISTINCT/ORDER BY/WITHIN GROUP.
    pub num_ordered_aggs: i32,
    pub has_non_partial_aggs: bool,
    pub has_non_serial_aggs: bool,
    /// EXEC ID for the work table (only if has_recursion).
    pub wt_param_id: i32,
    /// A path for non-recursive term.
    pub non_recursive_path: Option<Box<Path>>,
    /// Outer rels above current node (workspace for createplan.c).
    pub cur_outer_rels: Option<Relids>,
    /// Not-yet-assigned NestLoopParams.
    pub cur_outer_params: Vec<Node>,
    /// Workspace for setrefs.c: per-subplan alt-subplan flags.
    pub is_alt_subplan: Vec<bool>,
    pub is_used_subplan: Vec<bool>,
    // void *join_search_private -> opaque GEQO state, dropped.
    /// Does this query modify any partition key columns?
    pub part_cols_updated: bool,
    /// PartitionPruneInfos added in this query's plan.
    pub part_prune_infos: Vec<Node>,
}

/// Properties shared by relations partitioned the same way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSchemeData {
    /// Partition strategy.
    pub strategy: u8,
    /// Number of partition attributes.
    pub partnatts: i16,
    /// OIDs of operator families.
    pub partopfamily: Vec<Oid>,
    /// OIDs of opclass declared input data types.
    pub partopcintype: Vec<Oid>,
    /// OIDs of partitioning collations.
    pub partcollation: Vec<Oid>,
    /// Cached partition key type lengths.
    pub parttyplen: Vec<i16>,
    /// Cached partition key by-value flags.
    pub parttypbyval: Vec<bool>,
    // FmgrInfo *partsupfunc -> resolved comparison fns; dropped from skeleton.
}

pub type PartitionScheme = Box<PartitionSchemeData>;

bitflags! {
    /// Bitmask of flags supported by table AMs.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AmFlags: u32 {
        const HAS_TID_RANGE = 1 << 0;
    }
}

/// Kind of a RelOptInfo.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelOptKind {
    BASEREL,
    JOINREL,
    OTHER_MEMBER_REL,
    OTHER_JOINREL,
    UPPER_REL,
    OTHER_UPPER_REL,
}

/// Per-relation planner information.
#[derive(Debug, Clone, PartialEq)]
pub struct RelOptInfo {
    pub reloptkind: RelOptKind,
    /// All relids included (base + OJ relids).
    pub relids: Option<Relids>,
    /// Estimated number of result tuples.
    pub rows: Cardinality,
    pub consider_startup: bool,
    pub consider_param_startup: bool,
    pub consider_parallel: bool,
    /// Default result targetlist for Paths scanning this relation.
    pub reltarget: Option<Box<PathTarget>>,
    /// Path structures.
    pub pathlist: Vec<Box<Path>>,
    /// ParamPathInfos used in pathlist.
    pub ppilist: Vec<Box<ParamPathInfo>>,
    /// Partial Paths.
    pub partial_pathlist: Vec<Box<Path>>,
    pub cheapest_startup_path: Option<Box<Path>>,
    pub cheapest_total_path: Option<Box<Path>>,
    pub cheapest_unique_path: Option<Box<Path>>,
    pub cheapest_parameterized_paths: Vec<Box<Path>>,
    /// Rels directly laterally referenced.
    pub direct_lateral_relids: Option<Relids>,
    /// Minimum parameterization of rel.
    pub lateral_relids: Option<Relids>,
    /// RTE index (base rel).
    pub relid: usize,
    /// Containing tablespace.
    pub reltablespace: Oid,
    /// RELATION, SUBQUERY, FUNCTION, etc.
    pub rtekind: RTEKind,
    /// Smallest attrno of rel (often <0).
    pub min_attr: AttrNumber,
    /// Largest attrno of rel.
    pub max_attr: AttrNumber,
    /// Per-attribute highest joinrel needed (indexed [min_attr..max_attr]).
    pub attr_needed: Vec<Option<Relids>>,
    /// Per-attribute width estimates.
    pub attr_widths: Vec<i32>,
    /// Zero-based set of attnums of NOT NULL columns.
    pub notnullattnums: Option<Bitmapset>,
    /// Relids of outer joins that can null this baserel.
    pub nulling_relids: Option<Relids>,
    /// LATERAL Vars and PHVs referenced by rel.
    pub lateral_vars: Vec<Node>,
    /// Rels that reference this baserel laterally.
    pub lateral_referencers: Option<Relids>,
    /// IndexOptInfo list.
    pub indexlist: Vec<Box<IndexOptInfo>>,
    /// StatisticExtInfo list.
    pub statlist: Vec<Box<StatisticExtInfo>>,
    /// Disk pages (from pg_class).
    pub pages: BlockNumber,
    pub tuples: Cardinality,
    pub allvisfrac: f64,
    /// Indexes into eq_classes of ECs that mention this rel.
    pub eclass_indexes: Option<Bitmapset>,
    /// PlannerInfo for subquery, if any.
    pub subroot: Option<Box<PlannerInfo>>,
    /// PlannerParamItems passed to subquery.
    pub subplan_params: Vec<Box<PlannerParamItem>>,
    /// Wanted number of parallel workers.
    pub rel_parallel_workers: i32,
    /// Optional table AM features.
    pub amflags: AmFlags,
    /// Server for the table or join.
    pub serverid: Oid,
    /// User to check access as (0 = current user).
    pub userid: Oid,
    pub useridiscurrent: bool,
    // FdwRoutine *fdwroutine / void *fdw_private -> FDW state, dropped.
    /// Known unique for these other relid set(s) given in UniqueRelInfo(s).
    pub unique_for_rels: Vec<Box<UniqueRelInfo>>,
    /// Known not unique for these set(s).
    pub non_unique_for_rels: Vec<Node>,
    /// RestrictInfo structures (if base rel).
    pub baserestrictinfo: Vec<Box<RestrictInfo>>,
    /// Cost of evaluating the above.
    pub baserestrictcost: QualCost,
    /// Min security_level found in baserestrictinfo.
    pub baserestrict_min_security: usize,
    /// RestrictInfo structures for join clauses involving this rel.
    pub joininfo: Vec<Box<RestrictInfo>>,
    pub has_eclass_joins: bool,
    pub consider_partitionwise_join: bool,
    /// Immediate parent relation (otherrel).
    pub parent: Option<Box<Self>>,
    /// Topmost parent relation.
    pub top_parent: Option<Box<Self>>,
    /// Relids of topmost parent.
    pub top_parent_relids: Option<Relids>,
    /// Partitioning scheme.
    pub part_scheme: Option<PartitionScheme>,
    /// Number of partitions; -1 if not yet set.
    pub nparts: i32,
    // PartitionBoundInfoData *boundinfo -> opaque bound info, dropped.
    pub partbounds_merged: bool,
    /// Partition constraint, if not the root.
    pub partition_qual: Vec<Node>,
    /// RelOptInfos for each partition.
    pub part_rels: Vec<Option<Box<Self>>>,
    /// Live partitions after pruning (indexes into part_rels).
    pub live_parts: Option<Bitmapset>,
    /// All partition relids.
    pub all_partrels: Option<Relids>,
    /// Non-nullable partition key expressions (length partnatts).
    pub partexprs: Vec<Vec<Node>>,
    /// Nullable partition key expressions (length partnatts).
    pub nullable_partexprs: Vec<Vec<Node>>,
}

impl RelOptInfo {
    /// A lightweight clone for use as a `Path.parent` back-pointer. In PG `parent`
    /// is a shared pointer; here each field is owned, so a naive `clone()` would
    /// deep-copy the rel's `pathlist` -- and every path in it re-embeds its own
    /// parent rel, forming a value cycle that blows up super-linearly across DP
    /// levels (a 3-rel join exhausts memory). The `Path.parent` consumers only read
    /// scalar metadata (relids/relid/rows/rtekind/reltarget/...), never the parent's
    /// paths, so this snapshot drops the path-bearing fields to break the cycle.
    #[must_use]
    pub fn parent_snapshot(&self) -> Self {
        let mut snap = self.clone();
        snap.pathlist = Vec::new();
        snap.partial_pathlist = Vec::new();
        snap.cheapest_startup_path = None;
        snap.cheapest_total_path = None;
        snap.cheapest_unique_path = None;
        snap.cheapest_parameterized_paths = Vec::new();
        snap
    }
}

/// Per-index planner information.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexOptInfo {
    /// OID of the index relation.
    pub indexoid: Oid,
    /// Tablespace of index.
    pub reltablespace: Oid,
    /// Back-link to index's table.
    pub rel: Option<Box<RelOptInfo>>,
    /// Disk pages in index.
    pub pages: BlockNumber,
    /// Index tuples in index.
    pub tuples: Cardinality,
    /// Index tree height, or -1 if unknown.
    pub tree_height: i32,
    /// Number of columns in index.
    pub ncolumns: i32,
    /// Number of key columns in index.
    pub nkeycolumns: i32,
    /// Table column numbers (0 for expression columns); length ncolumns.
    pub indexkeys: Vec<i32>,
    /// Collations of index columns; length nkeycolumns.
    pub indexcollations: Vec<Oid>,
    /// Operator families for columns; length nkeycolumns.
    pub opfamily: Vec<Oid>,
    /// Opclass declared input data types; length nkeycolumns.
    pub opcintype: Vec<Oid>,
    /// Btree opfamilies if orderable; empty if partitioned.
    pub sortopfamily: Vec<Oid>,
    /// Is sort order descending? empty if partitioned.
    pub reverse_sort: Vec<bool>,
    /// Do NULLs come first? empty if partitioned.
    pub nulls_first: Vec<bool>,
    /// Opclass-specific options per column.
    pub opclassoptions: Vec<Option<Vec<u8>>>,
    /// Which index cols can be returned in an index-only scan; length ncolumns.
    pub canreturn: Vec<bool>,
    /// Access method OID (pg_am).
    pub relam: Oid,
    /// Expressions for non-simple index columns.
    pub indexprs: Vec<Node>,
    /// Predicate if a partial index, else empty.
    pub indpred: Vec<Node>,
    /// Targetlist representing index columns.
    pub indextlist: Vec<Node>,
    /// Parent's baserestrictinfo less conditions implied by the index predicate.
    pub indrestrictinfo: Vec<Box<RestrictInfo>>,
    pub pred_ok: bool,
    pub unique: bool,
    pub nullsnotdistinct: bool,
    pub immediate: bool,
    pub hypothetical: bool,
    pub amcanorderbyop: bool,
    pub amoptionalkey: bool,
    pub amsearcharray: bool,
    pub amsearchnulls: bool,
    pub amhasgettuple: bool,
    pub amhasgetbitmap: bool,
    pub amcanparallel: bool,
    pub amcanmarkpos: bool,
    // amcostestimate fn pointer -> AM vtable hook; dropped from skeleton.
}

/// Per-foreign-key planner information.
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKeyOptInfo {
    /// RT index of the referencing table.
    pub con_relid: usize,
    /// RT index of the referenced table.
    pub ref_relid: usize,
    /// Number of columns in the foreign key.
    pub nkeys: i32,
    /// Cols in referencing table (valid: nkeys).
    pub conkey: [AttrNumber; INDEX_MAX_KEYS],
    /// Cols in referenced table (valid: nkeys).
    pub confkey: [AttrNumber; INDEX_MAX_KEYS],
    /// PK = FK operator OIDs (valid: nkeys).
    pub conpfeqop: [Oid; INDEX_MAX_KEYS],
    /// # of FK cols matched by ECs.
    pub nmatched_ec: i32,
    /// # of these ECs that are has_const.
    pub nconst_ec: i32,
    /// # of FK cols matched by non-EC rinfos.
    pub nmatched_rcols: i32,
    /// Total # of non-EC rinfos matched to FK.
    pub nmatched_ri: i32,
    /// EClass matching each column's condition, if any.
    pub eclass: [Option<Box<EquivalenceClass>>; INDEX_MAX_KEYS],
    /// EClass member for the referencing Var, if any.
    pub fk_eclass_member: [Option<Box<EquivalenceMember>>; INDEX_MAX_KEYS],
    /// Non-EC RestrictInfos matching each column's condition.
    pub rinfos: [Vec<Box<RestrictInfo>>; INDEX_MAX_KEYS],
}

/// Extended-statistics planner information.
#[derive(Debug, Clone, PartialEq)]
pub struct StatisticExtInfo {
    /// OID of the statistics row.
    pub stat_oid: Oid,
    /// Includes child relations.
    pub inherit: bool,
    /// Back-link to statistic's table.
    pub rel: Option<Box<RelOptInfo>>,
    /// Statistics kind of this entry.
    pub kind: u8,
    /// Attnums of the columns covered.
    pub keys: Option<Bitmapset>,
    /// Expressions.
    pub exprs: Vec<Node>,
}

/// Scope of EquivalenceClass deductions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinDomain {
    /// All relids contained within the domain.
    pub jd_relids: Option<Relids>,
}

/// A set of mutually-equal expressions.
#[derive(Debug, Clone, PartialEq)]
pub struct EquivalenceClass {
    /// Btree operator family OIDs.
    pub opfamilies: Vec<Oid>,
    /// Collation, if datatypes are collatable.
    pub collation: Oid,
    /// # elements in childmembers.
    pub childmembers_size: i32,
    /// EquivalenceMembers.
    pub members: Vec<Box<EquivalenceMember>>,
    /// Per-relid arrays of child members.
    pub childmembers: Vec<Vec<Box<EquivalenceMember>>>,
    /// Generating RestrictInfos.
    pub sources: Vec<Box<RestrictInfo>>,
    /// Derived RestrictInfos.
    pub derives_list: Vec<Box<RestrictInfo>>,
    // derives_hash *ec_derives_hash -> optional HashMap, dropped from skeleton.
    /// All relids in members (except child members).
    pub relids: Option<Relids>,
    pub has_const: bool,
    pub has_volatile: bool,
    pub broken: bool,
    /// Originating sortclause label, or 0.
    pub sortref: usize,
    pub min_security: usize,
    pub max_security: usize,
    /// Set if merged into another EC.
    pub merged: Option<Box<Self>>,
}

impl EquivalenceClass {
    /// A lightweight clone for use as a `RestrictInfo` back-pointer (`parent_ec` /
    /// `left_ec` / `right_ec`). In PG these are shared pointers; here each field is
    /// owned, so a full `clone()` deep-copies `sources` / `derives_list` -- and each
    /// `RestrictInfo` in them owns a clone of THIS ec, a value cycle that grows the
    /// derived-clause cache super-linearly (a multi-rel join exhausts memory). The
    /// back-pointer's only role is identity/marking (which EC a clause came from);
    /// dropping the RestrictInfo-bearing lists breaks the cycle while keeping the
    /// identity-bearing fields (members / relids / opfamilies / has_const / ...).
    /// Compare live ECs against this via `identity_snapshot` on both sides.
    #[must_use]
    pub fn identity_snapshot(&self) -> Self {
        let mut snap = self.clone();
        snap.sources = Vec::new();
        snap.derives_list = Vec::new();
        snap
    }
}

/// One member expression of an EquivalenceClass.
#[derive(Debug, Clone, PartialEq)]
pub struct EquivalenceMember {
    /// The expression represented.
    pub expr: Node,
    /// All relids appearing in expr.
    pub relids: Option<Relids>,
    pub is_const: bool,
    pub is_child: bool,
    /// The "nominal type" used by the opfamily.
    pub datatype: Oid,
    /// Join domain containing the source clause.
    pub jdomain: Box<JoinDomain>,
    /// If is_child, link to the top-parent EM.
    pub parent: Option<Box<Self>>,
}

/// Iterator over an EquivalenceClass's parent and selected child members.
#[derive(Debug, Clone, PartialEq)]
pub struct EquivalenceMemberIterator {
    /// The EquivalenceClass to iterate over.
    pub ec: Box<EquivalenceClass>,
    /// Current relid position; -1 while looping members, -2 at end.
    pub current_relid: i32,
    /// Relids of child relations of interest.
    pub child_relids: Option<Relids>,
    /// Index of the next member within current_list.
    pub current_cell: usize,
    /// Current list of members being returned.
    pub current_list: Vec<Box<EquivalenceMember>>,
}

/// One sort key in a path's ordering.
#[derive(Debug, Clone, PartialEq)]
pub struct PathKey {
    /// The value that is ordered.
    pub eclass: Box<EquivalenceClass>,
    /// Index opfamily defining the ordering.
    pub opfamily: Oid,
    /// Sort direction (COMPARE_LT for ASC, COMPARE_GT for DESC).
    pub cmptype: CompareType,
    /// Do NULLs come before normal values?
    pub nulls_first: bool,
}

/// An order of group-by clauses with the corresponding pathkeys.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupByOrdering {
    pub pathkeys: Vec<Box<PathKey>>,
    pub clauses: Vec<Node>,
}

/// Cached contain_volatile_functions status.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolatileFunctionStatus {
    UNKNOWN = 0,
    VOLATILE,
    NOVOLATILE,
}

/// The targetlist (output columns) a Path will compute.
#[derive(Debug, Clone, PartialEq)]
pub struct PathTarget {
    /// Expressions to be computed.
    pub exprs: Vec<Node>,
    /// Corresponding sort/group refnos, or 0; length matches exprs (or empty).
    pub sortgrouprefs: Vec<usize>,
    /// Cost of evaluating the expressions.
    pub cost: QualCost,
    /// Estimated avg width of result tuples.
    pub width: i32,
    /// Whether exprs contain any volatile functions.
    pub has_volatile_expr: VolatileFunctionStatus,
}

/// Shared info for parameterized paths of a relation.
#[derive(Debug, Clone, PartialEq)]
pub struct ParamPathInfo {
    /// Rels supplying parameters used by path.
    pub req_outer: Option<Relids>,
    /// Estimated number of result tuples.
    pub rows: Cardinality,
    /// Join clauses available from outer rels.
    pub clauses: Vec<Box<RestrictInfo>>,
    /// Set of rinfo_serial for enforced quals.
    pub serials: Option<Bitmapset>,
}

/// Base Path: sequential-scan and simple plan types use it as-is; other path
/// types embed it as their first field.
#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    /// NodeTag of the Plan node this Path could build (kept as enum tag).
    pub pathtype: PathType,
    /// The relation this path can build.
    pub parent: Option<Box<RelOptInfo>>,
    /// Output columns the Path computes.
    pub pathtarget: Option<Box<PathTarget>>,
    /// Parameterization info, or None.
    pub param_info: Option<Box<ParamPathInfo>>,
    pub parallel_aware: bool,
    pub parallel_safe: bool,
    /// Desired # of workers; 0 = not parallel.
    pub parallel_workers: i32,
    /// Estimated number of result tuples.
    pub rows: Cardinality,
    /// Count of disabled nodes.
    pub disabled_nodes: i32,
    /// Cost before fetching any tuples.
    pub startup_cost: Cost,
    /// Total cost (assuming all tuples fetched).
    pub total_cost: Cost,
    /// Sort ordering of path's output.
    pub pathkeys: Vec<Box<PathKey>>,
    /// Index/bitmap path detail carried alongside the base `Path` in the rel's
    /// pathlist. In PG `create_plan_recurse` downcasts `Path*` to the concrete path
    /// node (IndexPath/BitmapHeapPath) by `pathtype`; the port's pathlist is a flat
    /// `Vec<Box<Path>>`, so the extra fields ride here. `None` for plain paths
    /// (SeqScan/Result); `Some` only for IndexScan/IndexOnlyScan/BitmapHeapScan.
    pub index_detail: Option<Box<IndexPathDetail>>,
    /// Join-path detail carried alongside the base `Path` for a join path (NestLoop/
    /// MergeJoin/HashJoin). As with `index_detail`, PG downcasts `Path*` to the
    /// concrete NestPath/MergePath/HashPath by `pathtype`; the flat pathlist rides
    /// the extra fields here. `None` for non-join paths.
    pub join_detail: Option<Box<JoinPathDetail>>,
}

/// The join-path detail `create_plan_recurse` (step 32) needs to build a NestLoop/
/// MergeJoin/HashJoin plan: the JoinPath fields plus the merge/hash specifics. The
/// outer/inner subpaths are full `Path`s (which themselves may carry index/join
/// detail), so a join tree round-trips through the flat pathlist.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinPathDetail {
    pub jointype: JoinType,
    pub inner_unique: bool,
    pub outerjoinpath: Box<Path>,
    pub innerjoinpath: Box<Path>,
    pub joinrestrictinfo: Vec<Box<RestrictInfo>>,
    /// MergeJoin: the merge clauses + explicit sort keys + materialize flag.
    pub merge: Option<MergePathDetail>,
    /// HashJoin: the hash clauses + batch count.
    pub hash: Option<HashPathDetail>,
}

/// MergeJoin-specific path detail (the MergePath fields beyond JoinPath).
#[derive(Debug, Clone, PartialEq)]
pub struct MergePathDetail {
    pub path_mergeclauses: Vec<Box<RestrictInfo>>,
    pub outersortkeys: Vec<Box<PathKey>>,
    pub innersortkeys: Vec<Box<PathKey>>,
    pub outer_presorted_keys: i32,
    pub skip_mark_restore: bool,
    pub materialize_inner: bool,
}

/// HashJoin-specific path detail (the HashPath fields beyond JoinPath).
#[derive(Debug, Clone, PartialEq)]
pub struct HashPathDetail {
    pub path_hashclauses: Vec<Box<RestrictInfo>>,
    pub num_batches: i32,
    pub inner_rows_total: Cardinality,
}

/// The index/bitmap-path detail `create_plan_recurse` needs to build an IndexScan /
/// BitmapHeapScan plan (the fields of IndexPath/BitmapHeapPath beyond the base Path).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexPathDetail {
    /// The chosen index.
    pub indexinfo: Box<IndexOptInfo>,
    /// The matched index clauses (the index quals).
    pub indexclauses: Vec<Box<IndexClause>>,
    /// Scan direction.
    pub indexscandir: ScanDirection,
    /// The index-access-only cost (excludes the heap fetch); used as the bitmap
    /// producer cost when this index path feeds a BitmapHeapScan.
    pub indextotalcost: Cost,
    /// The index selectivity (the fraction of heap rows the index quals select).
    pub indexselectivity: Selectivity,
    /// For a BitmapHeapScan path: the bitmap-producer subpath (an IndexScan path).
    pub bitmapqual: Option<Box<Path>>,
}

/// The Plan-node NodeTag a Path can produce. In C this is a raw NodeTag stored
/// in `Path.pathtype`; here it is the discriminant of the buildable plan.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathType {
    SeqScan,
    SampleScan,
    IndexScan,
    IndexOnlyScan,
    BitmapHeapScan,
    TidScan,
    TidRangeScan,
    SubqueryScan,
    FunctionScan,
    TableFuncScan,
    ValuesScan,
    CteScan,
    NamedTuplestoreScan,
    WorkTableScan,
    ForeignScan,
    CustomScan,
    NestLoop,
    MergeJoin,
    HashJoin,
    Append,
    MergeAppend,
    Result,
    ProjectSet,
    Material,
    Memoize,
    Unique,
    Gather,
    GatherMerge,
    Sort,
    IncrementalSort,
    Group,
    Agg,
    WindowAgg,
    SetOp,
    RecursiveUnion,
    LockRows,
    ModifyTable,
    Limit,
}

/// Index scan over a single index (regular or index-only).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexPath {
    pub path: Path,
    pub indexinfo: Box<IndexOptInfo>,
    pub indexclauses: Vec<Box<IndexClause>>,
    pub indexorderbys: Vec<Node>,
    pub indexorderbycols: Vec<i32>,
    pub indexscandir: ScanDirection,
    pub indextotalcost: Cost,
    pub indexselectivity: Selectivity,
}

/// One index-checkable restriction within an IndexPath.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexClause {
    /// Original restriction or join clause.
    pub rinfo: Box<RestrictInfo>,
    /// Indexqual(s) derived from it.
    pub indexquals: Vec<Box<RestrictInfo>>,
    /// Are indexquals a lossy version of clause?
    pub lossy: bool,
    /// Index column the clause uses (zero-based).
    pub indexcol: AttrNumber,
    /// Multiple index columns, if RowCompare.
    pub indexcols: Vec<i32>,
}

/// Indexscans producing a TID bitmap, then a bitmap heap scan.
#[derive(Debug, Clone, PartialEq)]
pub struct BitmapHeapPath {
    pub path: Path,
    /// IndexPath, BitmapAndPath, or BitmapOrPath.
    pub bitmapqual: Box<Path>,
}

/// A BitmapAnd plan node (substructure of a BitmapHeapPath).
#[derive(Debug, Clone, PartialEq)]
pub struct BitmapAndPath {
    pub path: Path,
    /// IndexPaths and BitmapOrPaths.
    pub bitmapquals: Vec<Box<Path>>,
    pub bitmapselectivity: Selectivity,
}

/// A BitmapOr plan node (substructure of a BitmapHeapPath).
#[derive(Debug, Clone, PartialEq)]
pub struct BitmapOrPath {
    pub path: Path,
    /// IndexPaths and BitmapAndPaths.
    pub bitmapquals: Vec<Box<Path>>,
    pub bitmapselectivity: Selectivity,
}

/// A scan by TID.
#[derive(Debug, Clone, PartialEq)]
pub struct TidPath {
    pub path: Path,
    /// Qual(s) involving CTID = something.
    pub tidquals: Vec<Node>,
}

/// A scan by a contiguous range of TIDs.
#[derive(Debug, Clone, PartialEq)]
pub struct TidRangePath {
    pub path: Path,
    pub tidrangequals: Vec<Node>,
}

/// A scan of an unflattened subquery-in-FROM.
#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryScanPath {
    pub path: Path,
    /// Path representing subquery execution.
    pub subpath: Box<Path>,
}

/// A scan of a foreign table/join/upper-relation.
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignPath {
    pub path: Path,
    pub fdw_outerpath: Option<Box<Path>>,
    pub fdw_restrictinfo: Vec<Box<RestrictInfo>>,
    pub fdw_private: Vec<Node>,
}

/// A table scan/join done by an out-of-core extension.
#[derive(Debug, Clone, PartialEq)]
pub struct CustomPath {
    pub path: Path,
    /// Mask of CUSTOMPATH_* flags (see nodes/extensible.h).
    pub flags: u32,
    /// Child Path nodes, if any.
    pub custom_paths: Vec<Box<Path>>,
    pub custom_restrictinfo: Vec<Box<RestrictInfo>>,
    pub custom_private: Vec<Node>,
    // const CustomPathMethods *methods -> extension vtable; dropped from skeleton.
}

/// An Append plan (successive execution of several member plans).
#[derive(Debug, Clone, PartialEq)]
pub struct AppendPath {
    pub path: Path,
    /// Component Paths.
    pub subpaths: Vec<Box<Path>>,
    /// Index of first partial path in subpaths.
    pub first_partial_path: i32,
    /// Hard limit on output tuples, or -1.
    pub limit_tuples: Cardinality,
}

/// A MergeAppend plan (merging sorted results).
#[derive(Debug, Clone, PartialEq)]
pub struct MergeAppendPath {
    pub path: Path,
    pub subpaths: Vec<Box<Path>>,
    pub limit_tuples: Cardinality,
}

/// A Result plan computing a degenerate GROUP BY case.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupResultPath {
    pub path: Path,
    pub quals: Vec<Node>,
}

/// A Material plan (caching the output of its subpath).
#[derive(Debug, Clone, PartialEq)]
pub struct MaterialPath {
    pub path: Path,
    pub subpath: Box<Path>,
}

/// A Memoize plan (cache for parameterized paths).
#[derive(Debug, Clone, PartialEq)]
pub struct MemoizePath {
    pub path: Path,
    /// Outerpath to cache tuples from.
    pub subpath: Box<Path>,
    /// OIDs of hash equality ops for cache keys.
    pub hash_operators: Vec<Oid>,
    /// Expressions that are cache keys.
    pub param_exprs: Vec<Node>,
    pub singlerow: bool,
    pub binary_mode: bool,
    /// Expected number of rescans.
    pub calls: Cardinality,
    /// Max entries the planner expects to fit, or 0 if unknown.
    pub est_entries: u32,
}

/// Strategy for a UniquePath.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniquePathMethod {
    NOOP,
    HASH,
    SORT,
}

/// Elimination of distinct rows from a subpath's output.
#[derive(Debug, Clone, PartialEq)]
pub struct UniquePath {
    pub path: Path,
    pub subpath: Box<Path>,
    pub umethod: UniquePathMethod,
    /// Equality operators of the IN clause.
    pub in_operators: Vec<Oid>,
    /// Expressions to be made unique.
    pub uniq_exprs: Vec<Node>,
}

/// Runs several copies of a plan in parallel and collects results.
#[derive(Debug, Clone, PartialEq)]
pub struct GatherPath {
    pub path: Path,
    /// Path for each worker.
    pub subpath: Box<Path>,
    /// Don't execute path more than once.
    pub single_copy: bool,
    /// Number of workers sought to help.
    pub num_workers: i32,
}

/// Like GatherPath, but preserving common sort order.
#[derive(Debug, Clone, PartialEq)]
pub struct GatherMergePath {
    pub path: Path,
    pub subpath: Box<Path>,
    pub num_workers: i32,
}

/// Fields shared by all join-type paths.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinPath {
    pub path: Path,
    pub jointype: JoinType,
    /// Each outer tuple provably matches no more than one inner tuple.
    pub inner_unique: bool,
    /// Path for the outer side of the join.
    pub outerjoinpath: Box<Path>,
    /// Path for the inner side of the join.
    pub innerjoinpath: Box<Path>,
    /// RestrictInfos to apply to join.
    pub joinrestrictinfo: Vec<Box<RestrictInfo>>,
}

/// A nested-loop join path.
#[derive(Debug, Clone, PartialEq)]
pub struct NestPath {
    pub jpath: JoinPath,
}

/// A mergejoin path.
#[derive(Debug, Clone, PartialEq)]
pub struct MergePath {
    pub jpath: JoinPath,
    /// Join clauses to be used for merge.
    pub path_mergeclauses: Vec<Box<RestrictInfo>>,
    /// Keys for explicit sort of outer input, if any.
    pub outersortkeys: Vec<Box<PathKey>>,
    /// Keys for explicit sort of inner input, if any.
    pub innersortkeys: Vec<Box<PathKey>>,
    /// Number of presorted keys of the outer path.
    pub outer_presorted_keys: i32,
    /// Can executor skip mark/restore?
    pub skip_mark_restore: bool,
    /// Add Materialize to inner?
    pub materialize_inner: bool,
}

/// A hashjoin path.
#[derive(Debug, Clone, PartialEq)]
pub struct HashPath {
    pub jpath: JoinPath,
    /// Join clauses used for hashing.
    pub path_hashclauses: Vec<Box<RestrictInfo>>,
    /// Number of batches expected.
    pub num_batches: i32,
    /// Total inner rows expected.
    pub inner_rows_total: Cardinality,
}

/// A projection (targetlist computation) step.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionPath {
    pub path: Path,
    /// Path representing input source.
    pub subpath: Box<Path>,
    /// True if no separate Result is needed.
    pub dummypp: bool,
}

/// Evaluation of a targetlist with set-returning functions.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectSetPath {
    pub path: Path,
    pub subpath: Box<Path>,
}

/// An explicit sort step.
#[derive(Debug, Clone, PartialEq)]
pub struct SortPath {
    pub path: Path,
    pub subpath: Box<Path>,
}

/// An incremental sort step.
#[derive(Debug, Clone, PartialEq)]
pub struct IncrementalSortPath {
    pub spath: SortPath,
    /// Number of presorted columns.
    pub n_presorted_cols: i32,
}

/// Grouping of presorted input.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupPath {
    pub path: Path,
    pub subpath: Box<Path>,
    /// SortGroupClause's.
    pub group_clause: Vec<Node>,
    /// HAVING quals, if any.
    pub qual: Vec<Node>,
}

/// Adjacent-duplicate removal in presorted input.
#[derive(Debug, Clone, PartialEq)]
pub struct UpperUniquePath {
    pub path: Path,
    pub subpath: Box<Path>,
    /// Number of pathkey columns to compare.
    pub numkeys: i32,
}

/// Generic aggregate-function computation.
#[derive(Debug, Clone, PartialEq)]
pub struct AggPath {
    pub path: Path,
    pub subpath: Box<Path>,
    pub aggstrategy: AggStrategy,
    pub aggsplit: AggSplit,
    /// Estimated number of groups in input.
    pub num_groups: Cardinality,
    /// For pass-by-ref transition data.
    pub transition_space: u64,
    pub group_clause: Vec<Node>,
    pub qual: Vec<Node>,
}

/// Annotations for one grouping set.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupingSetData {
    /// Grouping set as list of sortgrouprefs.
    pub set: Vec<i32>,
    /// Est. number of result groups.
    pub num_groups: Cardinality,
}

/// A rollup within a GROUPING SETS aggregation.
#[derive(Debug, Clone, PartialEq)]
pub struct RollupData {
    /// Applicable subset of parse->groupClause.
    pub group_clause: Vec<Node>,
    /// Lists of integer indexes into group_clause.
    pub gsets: Vec<Node>,
    /// GroupingSetData list.
    pub gsets_data: Vec<Box<GroupingSetData>>,
    pub num_groups: Cardinality,
    pub hashable: bool,
    pub is_hashed: bool,
}

/// A GROUPING SETS aggregation.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupingSetsPath {
    pub path: Path,
    pub subpath: Box<Path>,
    pub aggstrategy: AggStrategy,
    /// RollupData list.
    pub rollups: Vec<Box<RollupData>>,
    pub qual: Vec<Node>,
    pub transition_space: u64,
}

/// Computation of MIN/MAX aggregates from indexes.
#[derive(Debug, Clone, PartialEq)]
pub struct MinMaxAggPath {
    pub path: Path,
    /// MinMaxAggInfo list.
    pub mmaggregates: Vec<Box<MinMaxAggInfo>>,
    /// HAVING quals, if any.
    pub quals: Vec<Node>,
}

/// Generic window-function computation.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowAggPath {
    pub path: Path,
    pub subpath: Box<Path>,
    /// WindowClause we'll be using.
    pub winclause: Box<WindowClause>,
    /// Lower-level WindowAgg runconditions.
    pub qual: Vec<Node>,
    /// OpExpr list to short-circuit execution.
    pub run_condition: Vec<Node>,
    /// False for all apart from the WindowAgg closest to the root.
    pub topwindow: bool,
}

/// A set-operation (INTERSECT or EXCEPT).
#[derive(Debug, Clone, PartialEq)]
pub struct SetOpPath {
    pub path: Path,
    /// Paths representing input sources.
    pub leftpath: Box<Path>,
    pub rightpath: Box<Path>,
    pub cmd: SetOpCmd,
    pub strategy: SetOpStrategy,
    /// SortGroupClauses identifying target cols.
    pub group_list: Vec<Node>,
    /// Estimated number of groups in left input.
    pub num_groups: Cardinality,
}

/// A recursive UNION node.
#[derive(Debug, Clone, PartialEq)]
pub struct RecursiveUnionPath {
    pub path: Path,
    pub leftpath: Box<Path>,
    pub rightpath: Box<Path>,
    /// SortGroupClauses identifying target cols.
    pub distinct_list: Vec<Node>,
    /// ID of Param representing work table.
    pub wt_param: i32,
    pub num_groups: Cardinality,
}

/// Acquiring row locks for SELECT FOR UPDATE/SHARE.
#[derive(Debug, Clone, PartialEq)]
pub struct LockRowsPath {
    pub path: Path,
    pub subpath: Box<Path>,
    /// PlanRowMark list.
    pub row_marks: Vec<Node>,
    /// ID of Param for EvalPlanQual re-eval.
    pub epq_param: i32,
}

/// Performing INSERT/UPDATE/DELETE/MERGE.
#[derive(Debug, Clone, PartialEq)]
pub struct ModifyTablePath {
    pub path: Path,
    /// Path producing source data.
    pub subpath: Box<Path>,
    pub operation: CmdType,
    /// Do we set the command tag/processed?
    pub can_set_tag: bool,
    /// Parent RT index for EXPLAIN.
    pub nominal_relation: usize,
    /// Root RT index, if partitioned/inherited.
    pub root_relation: usize,
    /// Some part key in hierarchy updated?
    pub part_cols_updated: bool,
    /// Integer list of RT indexes.
    pub result_relations: Vec<i32>,
    /// Per-target-table update_colnos lists.
    pub update_colnos_lists: Vec<Node>,
    /// Per-target-table WCO lists.
    pub with_check_option_lists: Vec<Node>,
    /// Per-target-table RETURNING tlists.
    pub returning_lists: Vec<Node>,
    /// PlanRowMarks (non-locking only).
    pub row_marks: Vec<Node>,
    /// ON CONFLICT clause, or None.
    pub onconflict: Option<Box<OnConflictExpr>>,
    /// ID of Param for EvalPlanQual re-eval.
    pub epq_param: i32,
    /// Per-target-table MERGE action lists.
    pub merge_action_lists: Vec<Node>,
    /// Per-target-table MERGE join conditions.
    pub merge_join_conditions: Vec<Node>,
}

/// Applying LIMIT/OFFSET restrictions.
#[derive(Debug, Clone, PartialEq)]
pub struct LimitPath {
    pub path: Path,
    pub subpath: Box<Path>,
    /// OFFSET parameter, or None.
    pub limit_offset: Option<Node>,
    /// COUNT parameter, or None.
    pub limit_count: Option<Node>,
    pub limit_option: LimitOption,
}

/// Restriction clause info (one per AND sub-clause of WHERE/JOIN-ON).
#[derive(Debug, Clone, PartialEq)]
pub struct RestrictInfo {
    /// The represented clause of WHERE or JOIN.
    pub clause: Node,
    /// True if clause was pushed down in level.
    pub is_pushed_down: bool,
    pub can_join: bool,
    pub pseudoconstant: bool,
    pub has_clone: bool,
    pub is_clone: bool,
    /// Known to contain no leaked Vars.
    pub leakproof: bool,
    pub has_volatile: VolatileFunctionStatus,
    pub security_level: usize,
    /// Number of base rels in clause_relids.
    pub num_base_rels: i32,
    /// Relids actually referenced in the clause.
    pub clause_relids: Option<Relids>,
    /// Relids required to evaluate the clause.
    pub required_relids: Option<Relids>,
    /// Relids above which we cannot evaluate the clause.
    pub incompatible_relids: Option<Relids>,
    /// Outer-side relations, if an outer-join clause; else None.
    pub outer_relids: Option<Relids>,
    pub left_relids: Option<Relids>,
    pub right_relids: Option<Relids>,
    /// Modified clause with RestrictInfos; None unless clause is an OR clause.
    pub orclause: Option<Node>,
    /// Serial number, unique within the PlannerInfo context.
    pub rinfo_serial: i32,
    /// Generating EquivalenceClass; None unless potentially redundant.
    pub parent_ec: Option<Box<EquivalenceClass>>,
    /// Eval cost of clause; -1 if not yet set.
    pub eval_cost: QualCost,
    /// Selectivity for INNER semantics; -1 if not yet set.
    pub norm_selec: Selectivity,
    /// Selectivity for outer join semantics; -1 if not yet set.
    pub outer_selec: Selectivity,
    /// Opfamilies containing clause operator if mergejoinable, else empty.
    pub mergeopfamilies: Vec<Oid>,
    pub left_ec: Option<Box<EquivalenceClass>>,
    pub right_ec: Option<Box<EquivalenceClass>>,
    pub left_em: Option<Box<EquivalenceMember>>,
    pub right_em: Option<Box<EquivalenceMember>>,
    /// MergeScanSelCache list.
    pub scansel_cache: Vec<Box<MergeScanSelCache>>,
    /// Transient: outer var on left (T) or right (F).
    pub outer_is_left: bool,
    /// Clause operator if hashjoinable, else InvalidOid.
    pub hashjoinoperator: Oid,
    pub left_bucketsize: Selectivity,
    pub right_bucketsize: Selectivity,
    pub left_mcvfreq: Selectivity,
    pub right_mcvfreq: Selectivity,
    pub left_hasheqoperator: Oid,
    pub right_hasheqoperator: Oid,
}

/// Cached mergejoinscansel() results for one ordering. Not a Node.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeScanSelCache {
    /// Index opfamily defining the ordering.
    pub opfamily: Oid,
    /// Collation for the ordering.
    pub collation: Oid,
    /// Sort direction (ASC or DESC).
    pub cmptype: CompareType,
    pub nulls_first: bool,
    pub leftstartsel: Selectivity,
    pub leftendsel: Selectivity,
    pub rightstartsel: Selectivity,
    pub rightendsel: Selectivity,
}

/// Placeholder for an expression evaluated below the top of a plan tree.
/// Treated as an expression node, but declared here (not primnodes).
#[derive(Debug, Clone, PartialEq)]
pub struct PlaceHolderVar {
    /// The represented expression.
    pub phexpr: Node,
    /// Base+OJ relids syntactically within expr src.
    pub phrels: Option<Relids>,
    /// RT indexes of outer joins that can null PHV's value.
    pub phnullingrels: Option<Relids>,
    /// ID for PHV (unique within planner run).
    pub phid: usize,
    /// > 0 if PHV belongs to outer query.
    pub phlevelsup: usize,
}

/// Info about a flattened outer/semi/anti join.
#[derive(Debug, Clone, PartialEq)]
pub struct SpecialJoinInfo {
    /// Base+OJ relids in minimum LHS for join.
    pub min_lefthand: Option<Relids>,
    /// Base+OJ relids in minimum RHS for join.
    pub min_righthand: Option<Relids>,
    /// Base+OJ relids syntactically within LHS.
    pub syn_lefthand: Option<Relids>,
    /// Base+OJ relids syntactically within RHS.
    pub syn_righthand: Option<Relids>,
    /// Always INNER, LEFT, FULL, SEMI, or ANTI.
    pub jointype: JoinType,
    /// Outer join's RT index; 0 if none.
    pub ojrelid: usize,
    pub commute_above_l: Option<Relids>,
    pub commute_above_r: Option<Relids>,
    pub commute_below_l: Option<Relids>,
    pub commute_below_r: Option<Relids>,
    /// Joinclause is strict for some LHS rel.
    pub lhs_strict: bool,
    /// True if semi_operators are all btree (SEMI only).
    pub semi_can_btree: bool,
    /// True if semi_operators are all hash (SEMI only).
    pub semi_can_hash: bool,
    /// OIDs of equality join operators.
    pub semi_operators: Vec<Oid>,
    /// Righthand-side expressions of these ops.
    pub semi_rhs_exprs: Vec<Node>,
}

/// Transient mergejoinable outer-join clause info.
#[derive(Debug, Clone, PartialEq)]
pub struct OuterJoinClauseInfo {
    /// A mergejoinable outer-join clause.
    pub rinfo: Box<RestrictInfo>,
    /// The outer join's SpecialJoinInfo.
    pub sjinfo: Box<SpecialJoinInfo>,
}

/// Per-child info for an expanded append relation.
#[derive(Debug, Clone, PartialEq)]
pub struct AppendRelInfo {
    /// RT index of append parent rel.
    pub parent_relid: usize,
    /// RT index of append child rel.
    pub child_relid: usize,
    /// OID of parent's composite type.
    pub parent_reltype: Oid,
    /// OID of child's composite type.
    pub child_reltype: Oid,
    /// Per-parent-column child expressions (NULL element = dropped column).
    pub translated_vars: Vec<Option<Node>>,
    /// Length of parent_colnos array.
    pub num_child_cols: i32,
    /// 1-based parent column number for each child column, or 0.
    pub parent_colnos: Vec<AttrNumber>,
    /// OID of parent relation (InvalidOid for UNION ALL).
    pub parent_reloid: Oid,
}

/// Info about a row-identity "resjunk" column in UPDATE/DELETE/MERGE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowIdentityVarInfo {
    /// Var to be evaluated (varno=ROWID_VAR).
    pub rowidvar: Box<Var>,
    /// Estimated average width.
    pub rowidwidth: i32,
    /// Name of the resjunk column.
    pub rowidname: Option<String>,
    /// RTE indexes of target rels using this.
    pub rowidrels: Option<Relids>,
}

/// Central info for a distinct placeholder expression.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaceHolderInfo {
    /// ID for PH (unique within planner run).
    pub phid: usize,
    /// Copy of PlaceHolderVar tree.
    pub ph_var: Box<PlaceHolderVar>,
    /// Lowest level we can evaluate value at.
    pub ph_eval_at: Option<Relids>,
    /// Relids of contained lateral refs, if any.
    pub ph_lateral: Option<Relids>,
    /// Highest level the value is needed at.
    pub ph_needed: Option<Relids>,
    /// Estimated attribute width.
    pub ph_width: i32,
}

/// One potentially index-optimizable MIN/MAX aggregate.
#[derive(Debug, Clone, PartialEq)]
pub struct MinMaxAggInfo {
    /// pg_proc Oid of the aggregate.
    pub aggfnoid: Oid,
    /// Oid of its sort operator.
    pub aggsortop: Oid,
    /// Expression we are aggregating on.
    pub target: Node,
    /// Modified "root" for planning the subquery.
    pub subroot: Option<Box<PlannerInfo>>,
    /// Access path for subquery.
    pub path: Option<Box<Path>>,
    /// Estimated cost to fetch first row.
    pub pathcost: Cost,
    /// Param for subplan's output.
    pub param: Box<Param>,
}

/// An outer-reference or nestloop parameter item (Var/PlaceHolderVar/Aggref).
#[derive(Debug, Clone, PartialEq)]
pub struct PlannerParamItem {
    /// The Var, PlaceHolderVar, or Aggref.
    pub item: Node,
    /// Its assigned EXEC slot number.
    pub param_id: i32,
}

/// Correction factors for SEMI/ANTI/inner_unique join cost estimation. Not a Node.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SemiAntiJoinFactors {
    /// Fraction of outer tuples expected to have at least one match.
    pub outer_match_frac: Selectivity,
    /// Average matches expected for outer tuples that have at least one match.
    pub match_count: Selectivity,
}

/// Extra info passed to subroutines of add_paths_to_joinrel. Not a Node.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinPathExtraData {
    pub restrictlist: Vec<Box<RestrictInfo>>,
    pub mergeclause_list: Vec<Box<RestrictInfo>>,
    pub inner_unique: bool,
    pub sjinfo: Box<SpecialJoinInfo>,
    pub semifactors: SemiAntiJoinFactors,
    pub param_source_rels: Option<Relids>,
}

bitflags! {
    /// Flags indicating what kinds of grouping are possible.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct GroupingFlags: i32 {
        const CAN_USE_SORT = 0x0001;
        const CAN_USE_HASH = 0x0002;
        const CAN_PARTIAL_AGG = 0x0004;
    }
}

/// Kind of partitionwise aggregation in use.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionwiseAggregateType {
    NONE,
    FULL,
    PARTIAL,
}

/// Extra info passed to subroutines of create_grouping_paths. Not a Node.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupPathExtraData {
    pub flags: i32,
    pub partial_costs_set: bool,
    pub agg_partial_costs: AggClauseCosts,
    pub agg_final_costs: AggClauseCosts,
    pub target_parallel_safe: bool,
    pub having_qual: Option<Node>,
    pub target_list: Vec<Node>,
    pub patype: PartitionwiseAggregateType,
}

/// Extra info passed to subroutines of grouping_planner. Not a Node.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FinalPathExtraData {
    pub limit_needed: bool,
    pub limit_tuples: Cardinality,
    pub count_est: i64,
    pub offset_est: i64,
}

/// Preliminary join-cost estimates carried between cost phases. Not a Node.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JoinCostWorkspace {
    pub disabled_nodes: i32,
    pub startup_cost: Cost,
    pub total_cost: Cost,
    pub run_cost: Cost,
    pub inner_run_cost: Cost,
    pub inner_rescan_run_cost: Cost,
    pub outer_rows: Cardinality,
    pub inner_rows: Cardinality,
    pub outer_skip_rows: Cardinality,
    pub inner_skip_rows: Cardinality,
    pub numbuckets: i32,
    pub numbatches: i32,
    pub inner_rows_total: Cardinality,
}

/// Info about an aggregate that needs to be computed.
#[derive(Debug, Clone, PartialEq)]
pub struct AggInfo {
    /// Aggref exprs that this state value is for.
    pub aggrefs: Vec<Node>,
    /// Transition state number for this aggregate.
    pub transno: i32,
    /// False if this agg cannot share state values (read-write final fn).
    pub shareable: bool,
    /// Oid of the final function, or InvalidOid if none.
    pub finalfn_oid: Oid,
}

/// Info about a transition state shared by aggregates.
#[derive(Debug, Clone, PartialEq)]
pub struct AggTransInfo {
    /// Inputs for this transition state.
    pub args: Vec<Node>,
    pub aggfilter: Option<Node>,
    pub transfn_oid: Oid,
    pub serialfn_oid: Oid,
    pub deserialfn_oid: Oid,
    pub combinefn_oid: Oid,
    /// Oid of state value's datatype.
    pub aggtranstype: Oid,
    pub aggtranstypmod: i32,
    pub transtype_len: i32,
    pub transtype_by_val: bool,
    pub aggtransspace: i32,
    /// Initial value from pg_aggregate entry.
    pub init_value: Datum,
    pub init_value_is_null: bool,
}

/// Caches that a relation is unique when joined to other relation(s).
#[derive(Debug, Clone, PartialEq)]
pub struct UniqueRelInfo {
    /// Set of other relids for which the relation is unique.
    pub outerrelids: Option<Relids>,
    /// Unique considering only self-join-suitable clauses.
    pub self_join: bool,
    /// Additional baserestrictinfo clauses used to prove uniqueness.
    pub extra_clauses: Vec<Box<RestrictInfo>>,
}
