//! Translated from PostgreSQL 18.3 `src/include/nodes/pathnodes.h`.
//!
//! Definitions for planner's internal data structures, especially Paths.
//!
//! We don't support copying RelOptInfo, IndexOptInfo, or Path nodes.
//! There are some subsidiary structs that are useful to copy, though.
//!
//! The copy/equal/out/read support functions are generated elsewhere; this
//! file holds only the node/enum definitions, verbatim from the C header.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

#![allow(non_camel_case_types)]

use crate::prelude::*; // Oid, Datum, int*, uint*, Index, Size, bytea, c_char/c_int/c_void, etc.
use crate::nodes::nodes::{
    Node, NodeTag, ParseLoc, Cardinality, Cost, Selectivity, CmdType, JoinType, AggSplit, SetOpCmd,
    OnConflictAction, LimitOption,
};
use crate::nodes::pg_list::{List, ListCell};
use crate::nodes::bitmapset::Bitmapset; // Relids = *mut Bitmapset
use crate::nodes::primnodes::*; // Expr, Var, Param, OnConflictExpr, AttrNumber, CompareType, ...
use crate::nodes::parsenodes::*; // RangeTblEntry, RTEKind, WindowClause, SortGroupClause, ...

// Both primnodes (a stub) and parsenodes (the real def) glob-export `Query`.
// The C header includes parsenodes.h, so PlannerInfo.parse is the real Query;
// this explicit import shadows the ambiguous glob and pins it to parsenodes.
use crate::nodes::parsenodes::Query;

// Additional enums/strategies that live in nodes.rs but are used by-value here.
use crate::nodes::nodes::{AggStrategy, SetOpStrategy};

// ----------------------------------------------------------------
//  Forward stubs for cross-header types not yet translated.
//  All POINTER-only fields below are kept verbatim with their original
//  names; the pointed-to structs are opaque placeholders (or `*mut Node`
//  / scalar typedefs) until the corresponding headers are ported.
// ----------------------------------------------------------------

/// TODO(pg-port): real def `typedef uint32 BlockNumber` in storage/block.h.
pub type BlockNumber = uint32;

/// TODO(pg-port): real def `typedef enum ScanDirection` in access/sdir.h.
/// Used by value in IndexPath.indexscandir; stubbed as the underlying int.
pub type ScanDirection = c_int;

/// TODO(pg-port): real def `typedef struct ParamListInfoData *ParamListInfo`
/// in nodes/params.h.  Referenced only by pointer.
pub type ParamListInfo = *mut c_void;

/// TODO(pg-port): real def `typedef struct PartitionDirectoryData *PartitionDirectory`
/// in partitioning/partdefs.h.  Referenced only by pointer.
pub type PartitionDirectory = *mut c_void;

/// TODO(pg-port): real def `typedef struct MemoryContextData *MemoryContext`
/// in nodes/memnodes.h.  Referenced only by pointer.
pub type MemoryContext = *mut c_void;

/// TODO(pg-port): real def `struct HTAB` in utils/hsearch.h.  Pointer only.
#[repr(C)]
pub struct HTAB {
    _opaque: [u8; 0],
}

/// Real `struct FmgrInfo` lives in utils/fmgr.rs (fmgr.h); re-export it so
/// PartitionSchemeData.partsupfunc and friends use the canonical layout.
pub use crate::utils::fmgr::FmgrInfo;

/// TODO(pg-port): real def `struct FdwRoutine` in foreign/fdwapi.h.  Pointer only.
#[repr(C)]
pub struct FdwRoutine {
    _opaque: [u8; 0],
}

/// PartitionBoundInfoData (partitioning/partbounds.h) -- concrete layout so
/// partprune/partbounds/partdesc can access fields directly (canonical home).
#[repr(C)]
pub struct PartitionBoundInfoData {
    pub strategy: c_char,
    pub ndatums: c_int,
    pub datums: *mut *mut Datum,
    pub kind: *mut *mut crate::nodes::parsenodes::PartitionRangeDatumKind,
    pub nindexes: c_int,
    pub indexes: *mut c_int,
    pub null_index: c_int,
    pub default_index: c_int,
    pub interleaved_parts: *mut Bitmapset,
}

/// TODO(pg-port): real def `struct CustomPathMethods` in nodes/extensible.h.
/// Referenced by `const` pointer from CustomPath.methods.
#[repr(C)]
pub struct CustomPathMethods {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real def `struct derives_hash` (private to equivclass.c).
/// Pointer only, optional hash table for EquivalenceClass derived clauses.
#[repr(C)]
pub struct derives_hash {
    _opaque: [u8; 0],
}

/// INDEX_MAX_KEYS from pg_config_manual.h; controls fixed-size FK arrays.
/// TODO(pg-port): real def in pg_config_manual.h (default 32).
pub const INDEX_MAX_KEYS: usize = 32;

// ----------------------------------------------------------------

/*
 * Relids
 *		Set of relation identifiers (indexes into the rangetable).
 */
pub type Relids = *mut Bitmapset;

/*
 * When looking for a "cheapest path", this enum specifies whether we want
 * cheapest startup cost or cheapest total cost.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CostSelector {
    STARTUP_COST,
    TOTAL_COST,
}
pub use CostSelector::*;

/*
 * The cost estimate produced by cost_qual_eval() includes both a one-time
 * (startup) cost, and a per-tuple cost.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QualCost {
    pub startup: Cost,    /* one-time cost */
    pub per_tuple: Cost,  /* per-evaluation cost */
}

/*
 * Costing aggregate function execution requires these statistics about
 * the aggregates to be executed by a given Agg node.  Note that the costs
 * include the execution costs of the aggregates' argument expressions as
 * well as the aggregate functions themselves.  Also, the fields must be
 * defined so that initializing the struct to zeroes with memset is correct.
 */
#[repr(C)]
pub struct AggClauseCosts {
    pub transCost: QualCost,     /* total per-input-row execution costs */
    pub finalCost: QualCost,     /* total per-aggregated-row costs */
    pub transitionSpace: Size,   /* space for pass-by-ref transition data */
}

/*
 * This enum identifies the different types of "upper" (post-scan/join)
 * relations that we might deal with during planning.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum UpperRelationKind {
    UPPERREL_SETOP, /* result of UNION/INTERSECT/EXCEPT, if any */
    UPPERREL_PARTIAL_GROUP_AGG, /* result of partial grouping/aggregation, if
                                 * any */
    UPPERREL_GROUP_AGG,        /* result of grouping/aggregation, if any */
    UPPERREL_WINDOW,           /* result of window functions, if any */
    UPPERREL_PARTIAL_DISTINCT, /* result of partial "SELECT DISTINCT", if any */
    UPPERREL_DISTINCT,         /* result of "SELECT DISTINCT", if any */
    UPPERREL_ORDERED,          /* result of ORDER BY, if any */
    UPPERREL_FINAL,            /* result of any remaining top-level actions */
    /* NB: UPPERREL_FINAL must be last enum entry; it's used to size arrays */
}
pub use UpperRelationKind::*;

/* number of slots in arrays sized by UPPERREL_FINAL + 1 */
const UPPERREL_FINAL_PLUS_1: usize = UpperRelationKind::UPPERREL_FINAL as usize + 1;

/*----------
 * PlannerGlobal
 *		Global information for planning/optimization
 *
 * PlannerGlobal holds state for an entire planner invocation; this state
 * is shared across all levels of sub-Queries that exist in the command being
 * planned.
 *
 * Not all fields are printed.  (In some cases, there is no print support for
 * the field type; in others, doing so would lead to infinite recursion.)
 *----------
 */
#[repr(C)]
pub struct PlannerGlobal {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    /* Param values provided to planner() */
    pub boundParams: ParamListInfo, // pg_node_attr(read_write_ignore)

    /* Plans for SubPlan nodes */
    pub subplans: *mut List,

    /* Paths from which the SubPlan Plans were made */
    pub subpaths: *mut List,

    /* PlannerInfos for SubPlan nodes */
    pub subroots: *mut List, // pg_node_attr(read_write_ignore)

    /* indices of subplans that require REWIND */
    pub rewindPlanIDs: *mut Bitmapset,

    /* "flat" rangetable for executor */
    pub finalrtable: *mut List,

    /*
     * RT indexes of all relation RTEs in finalrtable (RTE_RELATION and
     * RTE_SUBQUERY RTEs of views)
     */
    pub allRelids: *mut Bitmapset,

    /*
     * RT indexes of all leaf partitions in nodes that support pruning and are
     * subject to runtime pruning at plan initialization time ("initial"
     * pruning).
     */
    pub prunableRelids: *mut Bitmapset,

    /* "flat" list of RTEPermissionInfos */
    pub finalrteperminfos: *mut List,

    /* "flat" list of PlanRowMarks */
    pub finalrowmarks: *mut List,

    /* "flat" list of integer RT indexes */
    pub resultRelations: *mut List,

    /* "flat" list of AppendRelInfos */
    pub appendRelations: *mut List,

    /* "flat" list of PartitionPruneInfos */
    pub partPruneInfos: *mut List,

    /* OIDs of relations the plan depends on */
    pub relationOids: *mut List,

    /* other dependencies, as PlanInvalItems */
    pub invalItems: *mut List,

    /* type OIDs for PARAM_EXEC Params */
    pub paramExecTypes: *mut List,

    /* highest PlaceHolderVar ID assigned */
    pub lastPHId: Index,

    /* highest PlanRowMark ID assigned */
    pub lastRowMarkId: Index,

    /* highest plan node ID assigned */
    pub lastPlanNodeId: c_int,

    /* redo plan when TransactionXmin changes? */
    pub transientPlan: bool,

    /* is plan specific to current role? */
    pub dependsOnRole: bool,

    /* parallel mode potentially OK? */
    pub parallelModeOK: bool,

    /* parallel mode actually required? */
    pub parallelModeNeeded: bool,

    /* worst PROPARALLEL hazard level */
    pub maxParallelHazard: c_char,

    /* partition descriptors */
    pub partition_directory: PartitionDirectory, // pg_node_attr(read_write_ignore)
}

/* macro for fetching the Plan associated with a SubPlan node */
/* #define planner_subplan_get_plan(root, subplan) \
 *     ((Plan *) list_nth((root)->glob->subplans, (subplan)->plan_id - 1))
 * TODO(pg-port): requires Plan / SubPlan / list_nth, not yet ported. */

/*----------
 * PlannerInfo
 *		Per-query information for planning/optimization
 *
 * This struct is conventionally called "root" in all the planner routines.
 * It holds links to all of the planner's working state, in addition to the
 * original Query.  Note that at present the planner extensively modifies
 * the passed-in Query data structure; someday that should stop.
 *
 * For reasons explained in optimizer/optimizer.h, we define the typedef
 * either here or in that header, whichever is read first.
 *
 * Not all fields are printed.  (In some cases, there is no print support for
 * the field type; in others, doing so would lead to infinite recursion or
 * bloat dump output more than seems useful.)
 *
 * NOTE: When adding new entries containing relids and relid bitmapsets,
 * remember to check that they will be correctly processed by
 * the remove_self_join_rel function - relid of removing relation will be
 * correctly replaced with the keeping one.
 *----------
 */
#[repr(C)]
pub struct PlannerInfo {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    /* the Query being planned */
    pub parse: *mut Query,

    /* global info for current planner run */
    pub glob: *mut PlannerGlobal,

    /* 1 at the outermost Query */
    pub query_level: Index,

    /* NULL at outermost Query */
    pub parent_root: *mut PlannerInfo, // pg_node_attr(read_write_ignore)

    /*
     * plan_params contains the expressions that this query level needs to
     * make available to a lower query level that is currently being planned.
     * outer_params contains the paramIds of PARAM_EXEC Params that outer
     * query levels will make available to this query level.
     */
    /* list of PlannerParamItems, see below */
    pub plan_params: *mut List,
    pub outer_params: *mut Bitmapset,

    /*
     * simple_rel_array holds pointers to "base rels" and "other rels" (see
     * comments for RelOptInfo for more info).  It is indexed by rangetable
     * index (so entry 0 is always wasted).  Entries can be NULL when an RTE
     * does not correspond to a base relation, such as a join RTE or an
     * unreferenced view RTE; or if the RelOptInfo hasn't been made yet.
     */
    pub simple_rel_array: *mut *mut RelOptInfo, // pg_node_attr(array_size(simple_rel_array_size))
    /* allocated size of array */
    pub simple_rel_array_size: c_int,

    /*
     * simple_rte_array is the same length as simple_rel_array and holds
     * pointers to the associated rangetable entries.  Using this is a shade
     * faster than using rt_fetch(), mostly due to fewer indirections.  (Not
     * printed because it'd be redundant with parse->rtable.)
     */
    pub simple_rte_array: *mut *mut RangeTblEntry, // pg_node_attr(read_write_ignore)

    /*
     * append_rel_array is the same length as the above arrays, and holds
     * pointers to the corresponding AppendRelInfo entry indexed by
     * child_relid, or NULL if the rel is not an appendrel child.  The array
     * itself is not allocated if append_rel_list is empty.  (Not printed
     * because it'd be redundant with append_rel_list.)
     */
    pub append_rel_array: *mut *mut AppendRelInfo, // pg_node_attr(read_write_ignore)

    /*
     * all_baserels is a Relids set of all base relids (but not joins or
     * "other" rels) in the query.  This is computed in deconstruct_jointree.
     */
    pub all_baserels: Relids,

    /*
     * outer_join_rels is a Relids set of all outer-join relids in the query.
     * This is computed in deconstruct_jointree.
     */
    pub outer_join_rels: Relids,

    /*
     * all_query_rels is a Relids set of all base relids and outer join relids
     * (but not "other" relids) in the query.  This is the Relids identifier
     * of the final join we need to form.  This is computed in
     * deconstruct_jointree.
     */
    pub all_query_rels: Relids,

    /*
     * join_rel_list is a list of all join-relation RelOptInfos we have
     * considered in this planning run.  For small problems we just scan the
     * list to do lookups, but when there are many join relations we build a
     * hash table for faster lookups.  The hash table is present and valid
     * when join_rel_hash is not NULL.  Note that we still maintain the list
     * even when using the hash table for lookups; this simplifies life for
     * GEQO.
     */
    pub join_rel_list: *mut List,
    pub join_rel_hash: *mut HTAB, // pg_node_attr(read_write_ignore)

    /*
     * When doing a dynamic-programming-style join search, join_rel_level[k]
     * is a list of all join-relation RelOptInfos of level k, and
     * join_cur_level is the current level.  New join-relation RelOptInfos are
     * automatically added to the join_rel_level[join_cur_level] list.
     * join_rel_level is NULL if not in use.
     *
     * Note: we've already printed all baserel and joinrel RelOptInfos above,
     * so we don't dump join_rel_level or other lists of RelOptInfos.
     */
    /* lists of join-relation RelOptInfos */
    pub join_rel_level: *mut *mut List, // pg_node_attr(read_write_ignore)
    /* index of list being extended */
    pub join_cur_level: c_int,

    /* init SubPlans for query */
    pub init_plans: *mut List,

    /*
     * per-CTE-item list of subplan IDs (or -1 if no subplan was made for that
     * CTE)
     */
    pub cte_plan_ids: *mut List,

    /* List of Lists of Params for MULTIEXPR subquery outputs */
    pub multiexpr_params: *mut List,

    /* list of JoinDomains used in the query (higher ones first) */
    pub join_domains: *mut List,

    /* list of active EquivalenceClasses */
    pub eq_classes: *mut List,

    /* set true once ECs are canonical */
    pub ec_merging_done: bool,

    /* list of "canonical" PathKeys */
    pub canon_pathkeys: *mut List,

    /*
     * list of OuterJoinClauseInfos for mergejoinable outer join clauses
     * w/nonnullable var on left
     */
    pub left_join_clauses: *mut List,

    /*
     * list of OuterJoinClauseInfos for mergejoinable outer join clauses
     * w/nonnullable var on right
     */
    pub right_join_clauses: *mut List,

    /*
     * list of OuterJoinClauseInfos for mergejoinable full join clauses
     */
    pub full_join_clauses: *mut List,

    /* list of SpecialJoinInfos */
    pub join_info_list: *mut List,

    /* counter for assigning RestrictInfo serial numbers */
    pub last_rinfo_serial: c_int,

    /*
     * all_result_relids is empty for SELECT, otherwise it contains at least
     * parse->resultRelation.  For UPDATE/DELETE/MERGE across an inheritance
     * or partitioning tree, the result rel's child relids are added.  When
     * using multi-level partitioning, intermediate partitioned rels are
     * included. leaf_result_relids is similar except that only actual result
     * tables, not partitioned tables, are included in it.
     */
    /* set of all result relids */
    pub all_result_relids: Relids,
    /* set of all leaf relids */
    pub leaf_result_relids: Relids,

    /*
     * list of AppendRelInfos
     *
     * Note: for AppendRelInfos describing partitions of a partitioned table,
     * we guarantee that partitions that come earlier in the partitioned
     * table's PartitionDesc will appear earlier in append_rel_list.
     */
    pub append_rel_list: *mut List,

    /* list of RowIdentityVarInfos */
    pub row_identity_vars: *mut List,

    /* list of PlanRowMarks */
    pub rowMarks: *mut List,

    /* list of PlaceHolderInfos */
    pub placeholder_list: *mut List,

    /* array of PlaceHolderInfos indexed by phid */
    pub placeholder_array: *mut *mut PlaceHolderInfo, // pg_node_attr(read_write_ignore, array_size(placeholder_array_size))
    /* allocated size of array */
    pub placeholder_array_size: c_int, // pg_node_attr(read_write_ignore)

    /* list of ForeignKeyOptInfos */
    pub fkey_list: *mut List,

    /* desired pathkeys for query_planner() */
    pub query_pathkeys: *mut List,

    /* groupClause pathkeys, if any */
    pub group_pathkeys: *mut List,

    /*
     * The number of elements in the group_pathkeys list which belong to the
     * GROUP BY clause.  Additional ones belong to ORDER BY / DISTINCT
     * aggregates.
     */
    pub num_groupby_pathkeys: c_int,

    /* pathkeys of bottom window, if any */
    pub window_pathkeys: *mut List,
    /* distinctClause pathkeys, if any */
    pub distinct_pathkeys: *mut List,
    /* sortClause pathkeys, if any */
    pub sort_pathkeys: *mut List,
    /* set operator pathkeys, if any */
    pub setop_pathkeys: *mut List,

    /* Canonicalised partition schemes used in the query. */
    pub part_schemes: *mut List, // pg_node_attr(read_write_ignore)

    /* RelOptInfos we are now trying to join */
    pub initial_rels: *mut List, // pg_node_attr(read_write_ignore)

    /*
     * Upper-rel RelOptInfos. Use fetch_upper_rel() to get any particular
     * upper rel.
     */
    pub upper_rels: [*mut List; UPPERREL_FINAL_PLUS_1], // pg_node_attr(read_write_ignore)

    /* Result tlists chosen by grouping_planner for upper-stage processing */
    pub upper_targets: [*mut PathTarget; UPPERREL_FINAL_PLUS_1], // pg_node_attr(read_write_ignore)

    /*
     * The fully-processed groupClause is kept here.  It differs from
     * parse->groupClause in that we remove any items that we can prove
     * redundant, so that only the columns named here actually need to be
     * compared to determine grouping.  Note that it's possible for *all* the
     * items to be proven redundant, implying that there is only one group
     * containing all the query's rows.  Hence, if you want to check whether
     * GROUP BY was specified, test for nonempty parse->groupClause, not for
     * nonempty processed_groupClause.  Optimizer chooses specific order of
     * group-by clauses during the upper paths generation process, attempting
     * to use different strategies to minimize number of sorts or engage
     * incremental sort.  See preprocess_groupclause() and
     * get_useful_group_keys_orderings() for details.
     *
     * Currently, when grouping sets are specified we do not attempt to
     * optimize the groupClause, so that processed_groupClause will be
     * identical to parse->groupClause.
     */
    pub processed_groupClause: *mut List,

    /*
     * The fully-processed distinctClause is kept here.  It differs from
     * parse->distinctClause in that we remove any items that we can prove
     * redundant, so that only the columns named here actually need to be
     * compared to determine uniqueness.  Note that it's possible for *all*
     * the items to be proven redundant, implying that there should be only
     * one output row.  Hence, if you want to check whether DISTINCT was
     * specified, test for nonempty parse->distinctClause, not for nonempty
     * processed_distinctClause.
     */
    pub processed_distinctClause: *mut List,

    /*
     * The fully-processed targetlist is kept here.  It differs from
     * parse->targetList in that (for INSERT) it's been reordered to match the
     * target table, and defaults have been filled in.  Also, additional
     * resjunk targets may be present.  preprocess_targetlist() does most of
     * that work, but note that more resjunk targets can get added during
     * appendrel expansion.  (Hence, upper_targets mustn't get set up till
     * after that.)
     */
    pub processed_tlist: *mut List,

    /*
     * For UPDATE, this list contains the target table's attribute numbers to
     * which the first N entries of processed_tlist are to be assigned.  (Any
     * additional entries in processed_tlist must be resjunk.)  DO NOT use the
     * resnos in processed_tlist to identify the UPDATE target columns.
     */
    pub update_colnos: *mut List,

    /*
     * Fields filled during create_plan() for use in setrefs.c
     */
    /* for GroupingFunc fixup (can't print: array length not known here) */
    pub grouping_map: *mut AttrNumber, // pg_node_attr(read_write_ignore)
    /* List of MinMaxAggInfos */
    pub minmax_aggs: *mut List,

    /* context holding PlannerInfo */
    pub planner_cxt: MemoryContext, // pg_node_attr(read_write_ignore)

    /* # of pages in all non-dummy tables of query */
    pub total_table_pages: Cardinality,

    /* tuple_fraction passed to query_planner */
    pub tuple_fraction: Selectivity,
    /* limit_tuples passed to query_planner */
    pub limit_tuples: Cardinality,

    /*
     * Minimum security_level for quals. Note: qual_security_level is zero if
     * there are no securityQuals.
     */
    pub qual_security_level: Index,

    /* true if any RTEs are RTE_JOIN kind */
    pub hasJoinRTEs: bool,
    /* true if any RTEs are marked LATERAL */
    pub hasLateralRTEs: bool,
    /* true if havingQual was non-null */
    pub hasHavingQual: bool,
    /* true if any RestrictInfo has pseudoconstant = true */
    pub hasPseudoConstantQuals: bool,
    /* true if we've made any of those */
    pub hasAlternativeSubPlans: bool,
    /* true once we're no longer allowed to add PlaceHolderInfos */
    pub placeholdersFrozen: bool,
    /* true if planning a recursive WITH item */
    pub hasRecursion: bool,

    /*
     * The rangetable index for the RTE_GROUP RTE, or 0 if there is no
     * RTE_GROUP RTE.
     */
    pub group_rtindex: c_int,

    /*
     * Information about aggregates. Filled by preprocess_aggrefs().
     */
    /* AggInfo structs */
    pub agginfos: *mut List,
    /* AggTransInfo structs */
    pub aggtransinfos: *mut List,
    /* number of aggs with DISTINCT/ORDER BY/WITHIN GROUP */
    pub numOrderedAggs: c_int,
    /* does any agg not support partial mode? */
    pub hasNonPartialAggs: bool,
    /* is any partial agg non-serializable? */
    pub hasNonSerialAggs: bool,

    /*
     * These fields are used only when hasRecursion is true:
     */
    /* PARAM_EXEC ID for the work table */
    pub wt_param_id: c_int,
    /* a path for non-recursive term */
    pub non_recursive_path: *mut Path,

    /*
     * These fields are workspace for createplan.c
     */
    /* outer rels above current node */
    pub curOuterRels: Relids,
    /* not-yet-assigned NestLoopParams */
    pub curOuterParams: *mut List,

    /*
     * These fields are workspace for setrefs.c.  Each is an array
     * corresponding to glob->subplans.  (We could probably teach
     * gen_node_support.pl how to determine the array length, but it doesn't
     * seem worth the trouble, so just mark them read_write_ignore.)
     */
    pub isAltSubplan: *mut bool, // pg_node_attr(read_write_ignore)
    pub isUsedSubplan: *mut bool, // pg_node_attr(read_write_ignore)

    /* optional private data for join_search_hook, e.g., GEQO */
    pub join_search_private: *mut c_void, // pg_node_attr(read_write_ignore)

    /* Does this query modify any partition key columns? */
    pub partColsUpdated: bool,

    /* PartitionPruneInfos added in this query's plan. */
    pub partPruneInfos: *mut List,
}

/*
 * In places where it's known that simple_rte_array[] must have been prepared
 * already, we just index into it to fetch RTEs.  In code that might be
 * executed before or after entering query_planner(), use this macro.
 *
 * #define planner_rt_fetch(rti, root) \
 *     ((root)->simple_rte_array ? (root)->simple_rte_array[rti] : \
 *      rt_fetch(rti, (root)->parse->rtable))
 * TODO(pg-port): requires rt_fetch, not yet ported.
 */

/*
 * If multiple relations are partitioned the same way, all such partitions
 * will have a pointer to the same PartitionScheme.  A list of PartitionScheme
 * objects is attached to the PlannerInfo.  By design, the partition scheme
 * incorporates only the general properties of the partition method (LIST vs.
 * RANGE, number of partitioning columns and the type information for each)
 * and not the specific bounds.
 *
 * We store the opclass-declared input data types instead of the partition key
 * datatypes since the former rather than the latter are used to compare
 * partition bounds. Since partition key data types and the opclass declared
 * input data types are expected to be binary compatible (per ResolveOpClass),
 * both of those should have same byval and length properties.
 */
#[repr(C)]
pub struct PartitionSchemeData {
    pub strategy: c_char,        /* partition strategy */
    pub partnatts: int16,        /* number of partition attributes */
    pub partopfamily: *mut Oid,  /* OIDs of operator families */
    pub partopcintype: *mut Oid, /* OIDs of opclass declared input data types */
    pub partcollation: *mut Oid, /* OIDs of partitioning collations */

    /* Cached information about partition key data types. */
    pub parttyplen: *mut int16,
    pub parttypbyval: *mut bool,

    /* Cached information about partition comparison functions. */
    pub partsupfunc: *mut FmgrInfo,
}

pub type PartitionScheme = *mut PartitionSchemeData;

/*----------
 * RelOptInfo
 *		Per-relation information for planning/optimization
 *
 * (See pathnodes.h for the very long block comment describing RelOptInfo's
 *  semantics; it is omitted here for brevity but applies verbatim.)
 *----------
 */

/* Bitmask of flags supported by table AMs */
pub const AMFLAG_HAS_TID_RANGE: u32 = 1 << 0;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RelOptKind {
    RELOPT_BASEREL,
    RELOPT_JOINREL,
    RELOPT_OTHER_MEMBER_REL,
    RELOPT_OTHER_JOINREL,
    RELOPT_UPPER_REL,
    RELOPT_OTHER_UPPER_REL,
}
pub use RelOptKind::*;

/*
 * Is the given relation a simple relation i.e a base or "other" member
 * relation?
 */
#[inline]
pub unsafe fn IS_SIMPLE_REL(rel: *const RelOptInfo) -> bool {
    (*rel).reloptkind == RELOPT_BASEREL || (*rel).reloptkind == RELOPT_OTHER_MEMBER_REL
}

/* Is the given relation a join relation? */
#[inline]
pub unsafe fn IS_JOIN_REL(rel: *const RelOptInfo) -> bool {
    (*rel).reloptkind == RELOPT_JOINREL || (*rel).reloptkind == RELOPT_OTHER_JOINREL
}

/* Is the given relation an upper relation? */
#[inline]
pub unsafe fn IS_UPPER_REL(rel: *const RelOptInfo) -> bool {
    (*rel).reloptkind == RELOPT_UPPER_REL || (*rel).reloptkind == RELOPT_OTHER_UPPER_REL
}

/* Is the given relation an "other" relation? */
#[inline]
pub unsafe fn IS_OTHER_REL(rel: *const RelOptInfo) -> bool {
    (*rel).reloptkind == RELOPT_OTHER_MEMBER_REL
        || (*rel).reloptkind == RELOPT_OTHER_JOINREL
        || (*rel).reloptkind == RELOPT_OTHER_UPPER_REL
}

#[repr(C)]
pub struct RelOptInfo {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    pub reloptkind: RelOptKind,

    /*
     * all relations included in this RelOptInfo; set of base + OJ relids
     * (rangetable indexes)
     */
    pub relids: Relids,

    /*
     * size estimates generated by planner
     */
    /* estimated number of result tuples */
    pub rows: Cardinality,

    /*
     * per-relation planner control flags
     */
    /* keep cheap-startup-cost paths? */
    pub consider_startup: bool,
    /* ditto, for parameterized paths? */
    pub consider_param_startup: bool,
    /* consider parallel paths? */
    pub consider_parallel: bool,

    /*
     * default result targetlist for Paths scanning this relation; list of
     * Vars/Exprs, cost, width
     */
    pub reltarget: *mut PathTarget,

    /*
     * materialization information
     */
    pub pathlist: *mut List,         /* Path structures */
    pub ppilist: *mut List,          /* ParamPathInfos used in pathlist */
    pub partial_pathlist: *mut List, /* partial Paths */
    pub cheapest_startup_path: *mut Path,
    pub cheapest_total_path: *mut Path,
    pub cheapest_unique_path: *mut Path,
    pub cheapest_parameterized_paths: *mut List,

    /*
     * parameterization information needed for both base rels and join rels
     * (see also lateral_vars and lateral_referencers)
     */
    /* rels directly laterally referenced */
    pub direct_lateral_relids: Relids,
    /* minimum parameterization of rel */
    pub lateral_relids: Relids,

    /*
     * information about a base rel (not set for join rels!)
     */
    pub relid: Index,
    /* containing tablespace */
    pub reltablespace: Oid,
    /* RELATION, SUBQUERY, FUNCTION, etc */
    pub rtekind: RTEKind,
    /* smallest attrno of rel (often <0) */
    pub min_attr: AttrNumber,
    /* largest attrno of rel */
    pub max_attr: AttrNumber,
    /* array indexed [min_attr .. max_attr] */
    pub attr_needed: *mut Relids, // pg_node_attr(read_write_ignore)
    /* array indexed [min_attr .. max_attr] */
    pub attr_widths: *mut int32, // pg_node_attr(read_write_ignore)

    /*
     * Zero-based set containing attnums of NOT NULL columns.  Not populated
     * for rels corresponding to non-partitioned inh==true RTEs.
     */
    pub notnullattnums: *mut Bitmapset,
    /* relids of outer joins that can null this baserel */
    pub nulling_relids: Relids,
    /* LATERAL Vars and PHVs referenced by rel */
    pub lateral_vars: *mut List,
    /* rels that reference this baserel laterally */
    pub lateral_referencers: Relids,
    /* list of IndexOptInfo */
    pub indexlist: *mut List,
    /* list of StatisticExtInfo */
    pub statlist: *mut List,
    /* size estimates derived from pg_class */
    pub pages: BlockNumber,
    pub tuples: Cardinality,
    pub allvisfrac: f64,
    /* indexes in PlannerInfo's eq_classes list of ECs that mention this rel */
    pub eclass_indexes: *mut Bitmapset,
    pub subroot: *mut PlannerInfo, /* if subquery */
    pub subplan_params: *mut List, /* if subquery */
    /* wanted number of parallel workers */
    pub rel_parallel_workers: c_int,
    /* Bitmask of optional features supported by the table AM */
    pub amflags: uint32,

    /*
     * Information about foreign tables and foreign joins
     */
    /* identifies server for the table or join */
    pub serverid: Oid,
    /* identifies user to check access as; 0 means to check as current user */
    pub userid: Oid,
    /* join is only valid for current user */
    pub useridiscurrent: bool,
    /* use "struct FdwRoutine" to avoid including fdwapi.h here */
    pub fdwroutine: *mut FdwRoutine, // pg_node_attr(read_write_ignore)
    pub fdw_private: *mut c_void,    // pg_node_attr(read_write_ignore)

    /*
     * cache space for remembering if we have proven this relation unique
     */
    /* known unique for these other relid set(s) given in UniqueRelInfo(s) */
    pub unique_for_rels: *mut List,
    /* known not unique for these set(s) */
    pub non_unique_for_rels: *mut List,

    /*
     * used by various scans and joins:
     */
    /* RestrictInfo structures (if base rel) */
    pub baserestrictinfo: *mut List,
    /* cost of evaluating the above */
    pub baserestrictcost: QualCost,
    /* min security_level found in baserestrictinfo */
    pub baserestrict_min_security: Index,
    /* RestrictInfo structures for join clauses involving this rel */
    pub joininfo: *mut List,
    /* T means joininfo is incomplete */
    pub has_eclass_joins: bool,

    /*
     * used by partitionwise joins:
     */
    /* consider partitionwise join paths? (if partitioned rel) */
    pub consider_partitionwise_join: bool,

    /*
     * inheritance links, if this is an otherrel (otherwise NULL):
     */
    /* Immediate parent relation (dumping it would be too verbose) */
    pub parent: *mut RelOptInfo, // pg_node_attr(read_write_ignore)
    /* Topmost parent relation (dumping it would be too verbose) */
    pub top_parent: *mut RelOptInfo, // pg_node_attr(read_write_ignore)
    /* Relids of topmost parent (redundant, but handy) */
    pub top_parent_relids: Relids,

    /*
     * used for partitioned relations:
     */
    /* Partitioning scheme */
    pub part_scheme: PartitionScheme, // pg_node_attr(read_write_ignore)

    /*
     * Number of partitions; -1 if not yet set; in case of a join relation 0
     * means it's considered unpartitioned
     */
    pub nparts: c_int,
    /* Partition bounds */
    pub boundinfo: *mut PartitionBoundInfoData, // pg_node_attr(read_write_ignore)
    /* True if partition bounds were created by partition_bounds_merge() */
    pub partbounds_merged: bool,
    /* Partition constraint, if not the root */
    pub partition_qual: *mut List,

    /*
     * Array of RelOptInfos of partitions, stored in the same order as bounds
     * (don't print, too bulky and duplicative)
     */
    pub part_rels: *mut *mut RelOptInfo, // pg_node_attr(read_write_ignore)

    /*
     * Bitmap with members acting as indexes into the part_rels[] array to
     * indicate which partitions survived partition pruning.
     */
    pub live_parts: *mut Bitmapset,
    /* Relids set of all partition relids */
    pub all_partrels: Relids,

    /*
     * These arrays are of length partkey->partnatts, which we don't have at
     * hand, so don't try to print
     */

    /* Non-nullable partition key expressions */
    pub partexprs: *mut *mut List, // pg_node_attr(read_write_ignore)
    /* Nullable partition key expressions */
    pub nullable_partexprs: *mut *mut List, // pg_node_attr(read_write_ignore)
}

/*
 * Is given relation partitioned?
 *
 * It's not enough to test whether rel->part_scheme is set, because it might
 * be that the basic partitioning properties of the input relations matched
 * but the partition bounds did not.  Also, if we are able to prove a rel
 * dummy (empty), we should henceforth treat it as unpartitioned.
 *
 * TODO(pg-port): is_dummy_rel() is declared extern (defined in joinrels.rs).
 */
#[inline]
pub unsafe fn IS_PARTITIONED_REL(rel: *mut RelOptInfo) -> bool {
    !(*rel).part_scheme.is_null()
        && !(*rel).boundinfo.is_null()
        && (*rel).nparts > 0
        && !(*rel).part_rels.is_null()
        && !is_dummy_rel(rel)
}

/*
 * Convenience macro to make sure that a partitioned relation has all the
 * required members set.
 *
 * #define REL_HAS_ALL_PART_PROPS(rel)	\
 *     ((rel)->part_scheme && (rel)->boundinfo && (rel)->nparts > 0 && \
 *      (rel)->part_rels && (rel)->partexprs && (rel)->nullable_partexprs)
 */
#[inline]
pub unsafe fn REL_HAS_ALL_PART_PROPS(rel: *const RelOptInfo) -> bool {
    !(*rel).part_scheme.is_null()
        && !(*rel).boundinfo.is_null()
        && (*rel).nparts > 0
        && !(*rel).part_rels.is_null()
        && !(*rel).partexprs.is_null()
        && !(*rel).nullable_partexprs.is_null()
}

/*
 * IndexOptInfo
 *		Per-index information for planning/optimization
 *
 * (See pathnodes.h for the full descriptive comment; arrays sized by ncolumns
 *  hold key+included columns, those sized by nkeycolumns hold only key cols.)
 */
#[repr(C)]
pub struct IndexOptInfo {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    /* OID of the index relation */
    pub indexoid: Oid,
    /* tablespace of index (not table) */
    pub reltablespace: Oid,
    /* back-link to index's table; don't print, else infinite recursion */
    pub rel: *mut RelOptInfo, // pg_node_attr(read_write_ignore)

    /*
     * index-size statistics (from pg_class and elsewhere)
     */
    /* number of disk pages in index */
    pub pages: BlockNumber,
    /* number of index tuples in index */
    pub tuples: Cardinality,
    /* index tree height, or -1 if unknown */
    pub tree_height: c_int,

    /*
     * index descriptor information
     */
    /* number of columns in index */
    pub ncolumns: c_int,
    /* number of key columns in index */
    pub nkeycolumns: c_int,

    /*
     * table column numbers of index's columns (both key and included
     * columns), or 0 for expression columns
     */
    pub indexkeys: *mut c_int, // pg_node_attr(array_size(ncolumns))
    /* OIDs of collations of index columns */
    pub indexcollations: *mut Oid, // pg_node_attr(array_size(nkeycolumns))
    /* OIDs of operator families for columns */
    pub opfamily: *mut Oid, // pg_node_attr(array_size(nkeycolumns))
    /* OIDs of opclass declared input data types */
    pub opcintype: *mut Oid, // pg_node_attr(array_size(nkeycolumns))
    /* OIDs of btree opfamilies, if orderable.  NULL if partitioned index */
    pub sortopfamily: *mut Oid, // pg_node_attr(array_size(nkeycolumns))
    /* is sort order descending? or NULL if partitioned index */
    pub reverse_sort: *mut bool, // pg_node_attr(array_size(nkeycolumns))
    /* do NULLs come first in the sort order? or NULL if partitioned index */
    pub nulls_first: *mut bool, // pg_node_attr(array_size(nkeycolumns))
    /* opclass-specific options for columns */
    pub opclassoptions: *mut *mut bytea, // pg_node_attr(read_write_ignore)
    /* which index cols can be returned in an index-only scan? */
    pub canreturn: *mut bool, // pg_node_attr(array_size(ncolumns))
    /* OID of the access method (in pg_am) */
    pub relam: Oid,

    /*
     * expressions for non-simple index columns; redundant to print since we
     * print indextlist
     */
    pub indexprs: *mut List, // pg_node_attr(read_write_ignore)
    /* predicate if a partial index, else NIL */
    pub indpred: *mut List,

    /* targetlist representing index columns */
    pub indextlist: *mut List,

    /*
     * parent relation's baserestrictinfo list, less any conditions implied by
     * the index's predicate (unless it's a target rel, see comments in
     * check_index_predicates())
     */
    pub indrestrictinfo: *mut List,

    /* true if index predicate matches query */
    pub predOK: bool,
    /* true if a unique index */
    pub unique: bool,
    /* true if the index was defined with NULLS NOT DISTINCT */
    pub nullsnotdistinct: bool,
    /* is uniqueness enforced immediately? */
    pub immediate: bool,
    /* true if index doesn't really exist */
    pub hypothetical: bool,

    /*
     * Remaining fields are copied from the index AM's API struct
     * (IndexAmRoutine).  These fields are not set for partitioned indexes.
     */
    pub amcanorderbyop: bool,
    pub amoptionalkey: bool,
    pub amsearcharray: bool,
    pub amsearchnulls: bool,
    /* does AM have amgettuple interface? */
    pub amhasgettuple: bool,
    /* does AM have amgetbitmap interface? */
    pub amhasgetbitmap: bool,
    pub amcanparallel: bool,
    /* does AM have ammarkpos interface? */
    pub amcanmarkpos: bool,
    /* AM's cost estimator */
    /* Rather than include amapi.h here, we declare amcostestimate like this */
    pub amcostestimate: Option<
        unsafe extern "C" fn(
            *mut PlannerInfo,
            *mut IndexPath,
            f64,
            *mut Cost,
            *mut Cost,
            *mut Selectivity,
            *mut f64,
            *mut f64,
        ),
    >, // pg_node_attr(read_write_ignore)
}

/*
 * ForeignKeyOptInfo
 *		Per-foreign-key information for planning/optimization
 *
 * The per-FK-column arrays can be fixed-size because we allow at most
 * INDEX_MAX_KEYS columns in a foreign key constraint.  Each array has
 * nkeys valid entries.
 */
#[repr(C)]
pub struct ForeignKeyOptInfo {
    // pg_node_attr(custom_read_write, no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    /*
     * Basic data about the foreign key (fetched from catalogs):
     */

    /* RT index of the referencing table */
    pub con_relid: Index,
    /* RT index of the referenced table */
    pub ref_relid: Index,
    /* number of columns in the foreign key */
    pub nkeys: c_int,
    /* cols in referencing table */
    pub conkey: [AttrNumber; INDEX_MAX_KEYS], // pg_node_attr(array_size(nkeys))
    /* cols in referenced table */
    pub confkey: [AttrNumber; INDEX_MAX_KEYS], // pg_node_attr(array_size(nkeys))
    /* PK = FK operator OIDs */
    pub conpfeqop: [Oid; INDEX_MAX_KEYS], // pg_node_attr(array_size(nkeys))

    /*
     * Derived info about whether FK's equality conditions match the query:
     */

    /* # of FK cols matched by ECs */
    pub nmatched_ec: c_int,
    /* # of these ECs that are ec_has_const */
    pub nconst_ec: c_int,
    /* # of FK cols matched by non-EC rinfos */
    pub nmatched_rcols: c_int,
    /* total # of non-EC rinfos matched to FK */
    pub nmatched_ri: c_int,
    /* Pointer to eclass matching each column's condition, if there is one */
    pub eclass: [*mut EquivalenceClass; INDEX_MAX_KEYS],
    /* Pointer to eclass member for the referencing Var, if there is one */
    pub fk_eclass_member: [*mut EquivalenceMember; INDEX_MAX_KEYS],
    /* List of non-EC RestrictInfos matching each column's condition */
    pub rinfos: [*mut List; INDEX_MAX_KEYS],
}

/*
 * StatisticExtInfo
 *		Information about extended statistics for planning/optimization
 *
 * Each pg_statistic_ext row is represented by one or more nodes of this
 * type, or even zero if ANALYZE has not computed them.
 */
#[repr(C)]
pub struct StatisticExtInfo {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    /* OID of the statistics row */
    pub statOid: Oid,

    /* includes child relations */
    pub inherit: bool,

    /* back-link to statistic's table; don't print, else infinite recursion */
    pub rel: *mut RelOptInfo, // pg_node_attr(read_write_ignore)

    /* statistics kind of this entry */
    pub kind: c_char,

    /* attnums of the columns covered */
    pub keys: *mut Bitmapset,

    /* expressions */
    pub exprs: *mut List,
}

/*
 * JoinDomains
 *
 * A "join domain" defines the scope of applicability of deductions made via
 * the EquivalenceClass mechanism.  (See pathnodes.h for the full comment.)
 *
 * The JoinDomains for a query are computed in deconstruct_jointree.
 * We do not copy JoinDomain structs once made, so they can be compared
 * for equality by simple pointer equality.
 */
#[repr(C)]
pub struct JoinDomain {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    pub jd_relids: Relids, /* all relids contained within the domain */
}

/*
 * EquivalenceClasses
 *
 * (See pathnodes.h for the very long descriptive comment.)
 *
 * NB: if ec_merged isn't NULL, this class has been merged into another, and
 * should be ignored in favor of using the pointed-to class.
 *
 * NB: EquivalenceClasses are never copied after creation.  Therefore,
 * copyObject() copies pointers to them as pointers, and equal() compares
 * pointers to EquivalenceClasses via pointer equality.  This is implemented
 * by putting copy_as_scalar and equal_as_scalar attributes on fields that
 * are pointers to EquivalenceClasses.  The same goes for EquivalenceMembers.
 */
#[repr(C)]
pub struct EquivalenceClass {
    // pg_node_attr(custom_read_write, no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    pub ec_opfamilies: *mut List, /* btree operator family OIDs */
    pub ec_collation: Oid,        /* collation, if datatypes are collatable */
    pub ec_childmembers_size: c_int, /* # elements in ec_childmembers */
    pub ec_members: *mut List,    /* list of EquivalenceMembers */
    pub ec_childmembers: *mut *mut List, /* array of Lists of child members */
    pub ec_sources: *mut List,    /* list of generating RestrictInfos */
    pub ec_derives_list: *mut List, /* list of derived RestrictInfos */
    pub ec_derives_hash: *mut derives_hash, /* optional hash table for fast
                                            * lookup; contains same
                                            * RestrictInfos as list */
    pub ec_relids: Relids, /* all relids appearing in ec_members, except
                            * for child members (see below) */
    pub ec_has_const: bool,    /* any pseudoconstants in ec_members? */
    pub ec_has_volatile: bool, /* the (sole) member is a volatile expr */
    pub ec_broken: bool,       /* failed to generate needed clauses? */
    pub ec_sortref: Index,     /* originating sortclause label, or 0 */
    pub ec_min_security: Index, /* minimum security_level in ec_sources */
    pub ec_max_security: Index, /* maximum security_level in ec_sources */
    pub ec_merged: *mut EquivalenceClass, /* set if merged into another EC */
}

/*
 * If an EC contains a constant, any PathKey depending on it must be
 * redundant, since there's only one possible value of the key.
 *
 * #define EC_MUST_BE_REDUNDANT(eclass)  ((eclass)->ec_has_const)
 */
#[inline]
pub unsafe fn EC_MUST_BE_REDUNDANT(eclass: *const EquivalenceClass) -> bool {
    (*eclass).ec_has_const
}

/*
 * EquivalenceMember - one member expression of an EquivalenceClass
 *
 * (See pathnodes.h for the full descriptive comment on em_is_child and
 *  em_datatype semantics.)
 */
#[repr(C)]
pub struct EquivalenceMember {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    pub em_expr: *mut Expr,       /* the expression represented */
    pub em_relids: Relids,        /* all relids appearing in em_expr */
    pub em_is_const: bool,        /* expression is pseudoconstant? */
    pub em_is_child: bool,        /* derived version for a child relation? */
    pub em_datatype: Oid,         /* the "nominal type" used by the opfamily */
    pub em_jdomain: *mut JoinDomain, /* join domain containing the source clause */
    /* if em_is_child is true, this links to corresponding EM for top parent */
    pub em_parent: *mut EquivalenceMember, // pg_node_attr(read_write_ignore)
}

/*
 * EquivalenceMemberIterator
 *
 * EquivalenceMemberIterator allows efficient access to sets of
 * EquivalenceMembers for callers which require access to child members.
 * (See pathnodes.h for the full descriptive comment.)
 *
 * Note: this is an anonymous struct typedef in C; we name it after its typedef.
 */
#[repr(C)]
pub struct EquivalenceMemberIterator {
    pub ec: *mut EquivalenceClass, /* The EquivalenceClass to iterate over */
    pub current_relid: c_int,      /* Current relid position within 'relids'. -1
                                    * when still looping over ec_members and -2
                                    * at the end of iteration */
    pub child_relids: Relids,      /* Relids of child relations of interest.
                                    * Non-child rels are ignored */
    pub current_cell: *mut ListCell, /* Next cell to return within current_list */
    pub current_list: *mut List,   /* Current list of members being returned */
}

/*
 * PathKeys
 *
 * The sort ordering of a path is represented by a list of PathKey nodes.
 * (See pathnodes.h for the full descriptive comment.)
 *
 * Note: pk_cmptype is either COMPARE_LT (for ASC) or COMPARE_GT (for DESC).
 */
#[repr(C)]
pub struct PathKey {
    // pg_node_attr(no_read, no_query_jumble)
    pub r#type: NodeTag,

    /* the value that is ordered */
    pub pk_eclass: *mut EquivalenceClass, // pg_node_attr(copy_as_scalar, equal_as_scalar)
    pub pk_opfamily: Oid,    /* index opfamily defining the ordering */
    pub pk_cmptype: CompareType, /* sort direction (ASC or DESC) */
    pub pk_nulls_first: bool, /* do NULLs come before normal values? */
}

/*
 * Contains an order of group-by clauses and the corresponding list of
 * pathkeys.
 *
 * The elements of 'clauses' list should have the same order as the head of
 * 'pathkeys' list.  The tleSortGroupRef of the clause should be equal to
 * ec_sortref of the pathkey equivalence class.  If there are redundant
 * clauses with the same tleSortGroupRef, they must be grouped together.
 */
#[repr(C)]
pub struct GroupByOrdering {
    pub r#type: NodeTag,

    pub pathkeys: *mut List,
    pub clauses: *mut List,
}

/*
 * VolatileFunctionStatus -- allows nodes to cache their
 * contain_volatile_functions properties. VOLATILITY_UNKNOWN means not yet
 * determined.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum VolatileFunctionStatus {
    VOLATILITY_UNKNOWN = 0,
    VOLATILITY_VOLATILE,
    VOLATILITY_NOVOLATILE,
}
pub use VolatileFunctionStatus::*;

/*
 * PathTarget
 *
 * This struct contains what we need to know during planning about the
 * targetlist (output columns) that a Path will compute.  (See pathnodes.h
 * for the full descriptive comment.)
 */
#[repr(C)]
pub struct PathTarget {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    /* list of expressions to be computed */
    pub exprs: *mut List,

    /* corresponding sort/group refnos, or 0 */
    pub sortgrouprefs: *mut Index, // pg_node_attr(array_size(exprs))

    /* cost of evaluating the expressions */
    pub cost: QualCost,

    /* estimated avg width of result tuples */
    pub width: c_int,

    /* indicates if exprs contain any volatile functions */
    pub has_volatile_expr: VolatileFunctionStatus,
}

/*
 * Convenience macro to get a sort/group refno from a PathTarget
 *
 * #define get_pathtarget_sortgroupref(target, colno) \
 *     ((target)->sortgrouprefs ? (target)->sortgrouprefs[colno] : (Index) 0)
 */
#[inline]
pub unsafe fn get_pathtarget_sortgroupref(target: *const PathTarget, colno: c_int) -> Index {
    if !(*target).sortgrouprefs.is_null() {
        *(*target).sortgrouprefs.add(colno as usize)
    } else {
        0 as Index
    }
}

/*
 * ParamPathInfo
 *
 * All parameterized paths for a given relation with given required outer rels
 * link to a single ParamPathInfo, which stores common information such as
 * the estimated rowcount for this parameterization.  (See pathnodes.h.)
 */
#[repr(C)]
pub struct ParamPathInfo {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    pub ppi_req_outer: Relids,  /* rels supplying parameters used by path */
    pub ppi_rows: Cardinality,  /* estimated number of result tuples */
    pub ppi_clauses: *mut List, /* join clauses available from outer rels */
    pub ppi_serials: *mut Bitmapset, /* set of rinfo_serial for enforced quals */
}

/*
 * Type "Path" is used as-is for sequential-scan paths, as well as some other
 * simple plan types that we don't need any extra information in the path for.
 * For other path types it is the first component of a larger struct.
 *
 * (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
pub struct Path {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    /* tag identifying scan/join method */
    pub pathtype: NodeTag,

    /*
     * the relation this path can build
     *
     * We do NOT print the parent, else we'd be in infinite recursion.  We can
     * print the parent's relids for identification purposes, though.
     */
    pub parent: *mut RelOptInfo, // pg_node_attr(write_only_relids)

    /*
     * list of Vars/Exprs, cost, width
     *
     * We print the pathtarget only if it's not the default one for the rel.
     */
    pub pathtarget: *mut PathTarget, // pg_node_attr(write_only_nondefault_pathtarget)

    /*
     * parameterization info, or NULL if none
     *
     * We do not print the whole of param_info, since it's printed via
     * RelOptInfo; it's sufficient and less cluttering to print just the
     * required outer relids.
     */
    pub param_info: *mut ParamPathInfo, // pg_node_attr(write_only_req_outer)

    /* engage parallel-aware logic? */
    pub parallel_aware: bool,
    /* OK to use as part of parallel plan? */
    pub parallel_safe: bool,
    /* desired # of workers; 0 = not parallel */
    pub parallel_workers: c_int,

    /* estimated size/costs for path (see costsize.c for more info) */
    pub rows: Cardinality,      /* estimated number of result tuples */
    pub disabled_nodes: c_int,  /* count of disabled nodes */
    pub startup_cost: Cost,     /* cost expended before fetching any tuples */
    pub total_cost: Cost,       /* total cost (assuming all tuples fetched) */

    /* sort ordering of path's output; a List of PathKey nodes; see above */
    pub pathkeys: *mut List,
}

/* Macro for extracting a path's parameterization relids; beware double eval
 *
 * #define PATH_REQ_OUTER(path)  \
 *     ((path)->param_info ? (path)->param_info->ppi_req_outer : (Relids) NULL)
 */
#[inline]
pub unsafe fn PATH_REQ_OUTER(path: *const Path) -> Relids {
    if !(*path).param_info.is_null() {
        (*(*path).param_info).ppi_req_outer
    } else {
        null_mut()
    }
}

/*----------
 * IndexPath represents an index scan over a single index.
 *
 * (See pathnodes.h for the full descriptive comment.)
 *----------
 */
#[repr(C)]
pub struct IndexPath {
    pub path: Path,
    pub indexinfo: *mut IndexOptInfo,
    pub indexclauses: *mut List,
    pub indexorderbys: *mut List,
    pub indexorderbycols: *mut List,
    pub indexscandir: ScanDirection,
    pub indextotalcost: Cost,
    pub indexselectivity: Selectivity,
}

/*
 * Each IndexClause references a RestrictInfo node from the query's WHERE
 * or JOIN conditions, and shows how that restriction can be applied to
 * the particular index.  (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
pub struct IndexClause {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,
    pub rinfo: *mut RestrictInfo, /* original restriction or join clause */
    pub indexquals: *mut List,    /* indexqual(s) derived from it */
    pub lossy: bool,              /* are indexquals a lossy version of clause? */
    pub indexcol: AttrNumber,     /* index column the clause uses (zero-based) */
    pub indexcols: *mut List,     /* multiple index columns, if RowCompare */
}

/*
 * BitmapHeapPath represents one or more indexscans that generate TID bitmaps
 * instead of directly accessing the heap, followed by AND/OR combinations
 * to produce a single bitmap, followed by a heap scan that uses the bitmap.
 * (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
pub struct BitmapHeapPath {
    pub path: Path,
    pub bitmapqual: *mut Path, /* IndexPath, BitmapAndPath, BitmapOrPath */
}

/*
 * BitmapAndPath represents a BitmapAnd plan node; it can only appear as
 * part of the substructure of a BitmapHeapPath.  The Path structure is
 * a bit more heavyweight than we really need for this, but for simplicity
 * we make it a derivative of Path anyway.
 */
#[repr(C)]
pub struct BitmapAndPath {
    pub path: Path,
    pub bitmapquals: *mut List, /* IndexPaths and BitmapOrPaths */
    pub bitmapselectivity: Selectivity,
}

/*
 * BitmapOrPath represents a BitmapOr plan node; it can only appear as
 * part of the substructure of a BitmapHeapPath.  The Path structure is
 * a bit more heavyweight than we really need for this, but for simplicity
 * we make it a derivative of Path anyway.
 */
#[repr(C)]
pub struct BitmapOrPath {
    pub path: Path,
    pub bitmapquals: *mut List, /* IndexPaths and BitmapAndPaths */
    pub bitmapselectivity: Selectivity,
}

/*
 * TidPath represents a scan by TID
 *
 * tidquals is an implicitly OR'ed list of qual expressions of the form
 * "CTID = pseudoconstant", or "CTID = ANY(pseudoconstant_array)",
 * or a CurrentOfExpr for the relation.
 */
#[repr(C)]
pub struct TidPath {
    pub path: Path,
    pub tidquals: *mut List, /* qual(s) involving CTID = something */
}

/*
 * TidRangePath represents a scan by a contiguous range of TIDs
 *
 * tidrangequals is an implicitly AND'ed list of qual expressions of the form
 * "CTID relop pseudoconstant", where relop is one of >,>=,<,<=.
 */
#[repr(C)]
pub struct TidRangePath {
    pub path: Path,
    pub tidrangequals: *mut List,
}

/*
 * SubqueryScanPath represents a scan of an unflattened subquery-in-FROM
 *
 * (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
pub struct SubqueryScanPath {
    pub path: Path,
    pub subpath: *mut Path, /* path representing subquery execution */
}

/*
 * ForeignPath represents a potential scan of a foreign table, foreign join
 * or foreign upper-relation.
 *
 * (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
pub struct ForeignPath {
    pub path: Path,
    pub fdw_outerpath: *mut Path,
    pub fdw_restrictinfo: *mut List,
    pub fdw_private: *mut List,
}

/*
 * CustomPath represents a table scan or a table join done by some out-of-core
 * extension.
 *
 * (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
pub struct CustomPath {
    pub path: Path,
    pub flags: uint32,             /* mask of CUSTOMPATH_* flags, see
                                    * nodes/extensible.h */
    pub custom_paths: *mut List,   /* list of child Path nodes, if any */
    pub custom_restrictinfo: *mut List,
    pub custom_private: *mut List,
    pub methods: *const CustomPathMethods,
}

/*
 * AppendPath represents an Append plan, ie, successive execution of
 * several member plans.
 *
 * (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
pub struct AppendPath {
    pub path: Path,
    pub subpaths: *mut List, /* list of component Paths */
    /* Index of first partial path in subpaths; list_length(subpaths) if none */
    pub first_partial_path: c_int,
    pub limit_tuples: Cardinality, /* hard limit on output tuples, or -1 */
}

/*
 * #define IS_DUMMY_APPEND(p) \
 *     (IsA((p), AppendPath) && ((AppendPath *) (p))->subpaths == NIL)
 * TODO(pg-port): depends on IsA(); keep as note.
 */

/*
 * A relation that's been proven empty will have one path that is dummy
 * (but might have projection paths on top).  For historical reasons,
 * this is provided as a macro that wraps is_dummy_rel().
 *
 * #define IS_DUMMY_REL(r) is_dummy_rel(r)
 * extern bool is_dummy_rel(RelOptInfo *rel);
 * TODO(pg-port): is_dummy_rel() lives in optimizer/util/pathnode.c.
 */
extern "C" {
    pub fn is_dummy_rel(rel: *mut RelOptInfo) -> bool;
}

/*
 * MergeAppendPath represents a MergeAppend plan, ie, the merging of sorted
 * results from several member plans to produce similarly-sorted output.
 */
#[repr(C)]
pub struct MergeAppendPath {
    pub path: Path,
    pub subpaths: *mut List, /* list of component Paths */
    pub limit_tuples: Cardinality, /* hard limit on output tuples, or -1 */
}

/*
 * GroupResultPath represents use of a Result plan node to compute the
 * output of a degenerate GROUP BY case, wherein we know we should produce
 * exactly one row, which might then be filtered by a HAVING qual.
 *
 * Note that quals is a list of bare clauses, not RestrictInfos.
 */
#[repr(C)]
pub struct GroupResultPath {
    pub path: Path,
    pub quals: *mut List,
}

/*
 * MaterialPath represents use of a Material plan node, i.e., caching of
 * the output of its subpath.  This is used when the subpath is expensive
 * and needs to be scanned repeatedly, or when we need mark/restore ability
 * and the subpath doesn't have it.
 */
#[repr(C)]
pub struct MaterialPath {
    pub path: Path,
    pub subpath: *mut Path,
}

/*
 * MemoizePath represents a Memoize plan node, i.e., a cache that caches
 * tuples from parameterized paths to save the underlying node from having to
 * be rescanned for parameter values which are already cached.
 */
#[repr(C)]
pub struct MemoizePath {
    pub path: Path,
    pub subpath: *mut Path,      /* outerpath to cache tuples from */
    pub hash_operators: *mut List, /* OIDs of hash equality ops for cache keys */
    pub param_exprs: *mut List,  /* expressions that are cache keys */
    pub singlerow: bool,         /* true if the cache entry is to be marked as
                                  * complete after caching the first record. */
    pub binary_mode: bool,       /* true when cache key should be compared bit
                                  * by bit, false when using hash equality ops */
    pub calls: Cardinality,      /* expected number of rescans */
    pub est_entries: uint32,     /* The maximum number of entries that the
                                  * planner expects will fit in the cache, or 0
                                  * if unknown */
}

/*
 * UniquePath represents elimination of distinct rows from the output of
 * its subpath.
 *
 * (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum UniquePathMethod {
    UNIQUE_PATH_NOOP, /* input is known unique already */
    UNIQUE_PATH_HASH, /* use hashing */
    UNIQUE_PATH_SORT, /* use sorting */
}
pub use UniquePathMethod::*;

#[repr(C)]
pub struct UniquePath {
    pub path: Path,
    pub subpath: *mut Path,
    pub umethod: UniquePathMethod,
    pub in_operators: *mut List, /* equality operators of the IN clause */
    pub uniq_exprs: *mut List,   /* expressions to be made unique */
}

/*
 * GatherPath runs several copies of a plan in parallel and collects the
 * results.  The parallel leader may also execute the plan, unless the
 * single_copy flag is set.
 */
#[repr(C)]
pub struct GatherPath {
    pub path: Path,
    pub subpath: *mut Path,  /* path for each worker */
    pub single_copy: bool,   /* don't execute path more than once */
    pub num_workers: c_int,  /* number of workers sought to help */
}

/*
 * GatherMergePath runs several copies of a plan in parallel and collects
 * the results, preserving their common sort order.
 */
#[repr(C)]
pub struct GatherMergePath {
    pub path: Path,
    pub subpath: *mut Path, /* path for each worker */
    pub num_workers: c_int, /* number of workers sought to help */
}

/*
 * All join-type paths share these fields.
 */
#[repr(C)]
pub struct JoinPath {
    // pg_node_attr(abstract)
    pub path: Path,

    pub jointype: JoinType,

    pub inner_unique: bool, /* each outer tuple provably matches no more
                             * than one inner tuple */

    pub outerjoinpath: *mut Path, /* path for the outer side of the join */
    pub innerjoinpath: *mut Path, /* path for the inner side of the join */

    pub joinrestrictinfo: *mut List, /* RestrictInfos to apply to join */

    /*
     * See the notes for RelOptInfo and ParamPathInfo to understand why
     * joinrestrictinfo is needed in JoinPath, and can't be merged into the
     * parent RelOptInfo.
     */
}

/*
 * A nested-loop path needs no special fields.
 */
#[repr(C)]
pub struct NestPath {
    pub jpath: JoinPath,
}

/*
 * A mergejoin path has these fields.
 *
 * (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
pub struct MergePath {
    pub jpath: JoinPath,
    pub path_mergeclauses: *mut List, /* join clauses to be used for merge */
    pub outersortkeys: *mut List,     /* keys for explicit sort, if any */
    pub innersortkeys: *mut List,     /* keys for explicit sort, if any */
    pub outer_presorted_keys: c_int,  /* number of presorted keys of the
                                       * outer path */
    pub skip_mark_restore: bool,      /* can executor skip mark/restore? */
    pub materialize_inner: bool,      /* add Materialize to inner? */
}

/*
 * A hashjoin path has these fields.
 *
 * (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
pub struct HashPath {
    pub jpath: JoinPath,
    pub path_hashclauses: *mut List, /* join clauses used for hashing */
    pub num_batches: c_int,          /* number of batches expected */
    pub inner_rows_total: Cardinality, /* total inner rows expected */
}

/*
 * ProjectionPath represents a projection (that is, targetlist computation)
 *
 * (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
pub struct ProjectionPath {
    pub path: Path,
    pub subpath: *mut Path, /* path representing input source */
    pub dummypp: bool,      /* true if no separate Result is needed */
}

/*
 * ProjectSetPath represents evaluation of a targetlist that includes
 * set-returning function(s), which will need to be implemented by a
 * ProjectSet plan node.
 */
#[repr(C)]
pub struct ProjectSetPath {
    pub path: Path,
    pub subpath: *mut Path, /* path representing input source */
}

/*
 * SortPath represents an explicit sort step
 *
 * The sort keys are, by definition, the same as path.pathkeys.
 *
 * Note: the Sort plan node cannot project, so path.pathtarget must be the
 * same as the input's pathtarget.
 */
#[repr(C)]
pub struct SortPath {
    pub path: Path,
    pub subpath: *mut Path, /* path representing input source */
}

/*
 * IncrementalSortPath represents an incremental sort step
 *
 * This is like a regular sort, except some leading key columns are assumed
 * to be ordered already.
 */
#[repr(C)]
pub struct IncrementalSortPath {
    pub spath: SortPath,
    pub nPresortedCols: c_int, /* number of presorted columns */
}

/*
 * GroupPath represents grouping (of presorted input)
 *
 * groupClause represents the columns to be grouped on; the input path
 * must be at least that well sorted.
 *
 * We can also apply a qual to the grouped rows (equivalent of HAVING)
 */
#[repr(C)]
pub struct GroupPath {
    pub path: Path,
    pub subpath: *mut Path,    /* path representing input source */
    pub groupClause: *mut List, /* a list of SortGroupClause's */
    pub qual: *mut List,       /* quals (HAVING quals), if any */
}

/*
 * UpperUniquePath represents adjacent-duplicate removal (in presorted input)
 *
 * The columns to be compared are the first numkeys columns of the path's
 * pathkeys.  The input is presumed already sorted that way.
 */
#[repr(C)]
pub struct UpperUniquePath {
    pub path: Path,
    pub subpath: *mut Path, /* path representing input source */
    pub numkeys: c_int,     /* number of pathkey columns to compare */
}

/*
 * AggPath represents generic computation of aggregate functions
 *
 * This may involve plain grouping (but not grouping sets), using either
 * sorted or hashed grouping; for the AGG_SORTED case, the input must be
 * appropriately presorted.
 */
#[repr(C)]
pub struct AggPath {
    pub path: Path,
    pub subpath: *mut Path,      /* path representing input source */
    pub aggstrategy: AggStrategy, /* basic strategy, see nodes.h */
    pub aggsplit: AggSplit,      /* agg-splitting mode, see nodes.h */
    pub numGroups: Cardinality,  /* estimated number of groups in input */
    pub transitionSpace: uint64, /* for pass-by-ref transition data */
    pub groupClause: *mut List,  /* a list of SortGroupClause's */
    pub qual: *mut List,         /* quals (HAVING quals), if any */
}

/*
 * Various annotations used for grouping sets in the planner.
 */
#[repr(C)]
pub struct GroupingSetData {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,
    pub set: *mut List,         /* grouping set as list of sortgrouprefs */
    pub numGroups: Cardinality, /* est. number of result groups */
}

#[repr(C)]
pub struct RollupData {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,
    pub groupClause: *mut List, /* applicable subset of parse->groupClause */
    pub gsets: *mut List,       /* lists of integer indexes into groupClause */
    pub gsets_data: *mut List,  /* list of GroupingSetData */
    pub numGroups: Cardinality, /* est. number of result groups */
    pub hashable: bool,         /* can be hashed */
    pub is_hashed: bool,        /* to be implemented as a hashagg */
}

/*
 * GroupingSetsPath represents a GROUPING SETS aggregation
 */
#[repr(C)]
pub struct GroupingSetsPath {
    pub path: Path,
    pub subpath: *mut Path,       /* path representing input source */
    pub aggstrategy: AggStrategy, /* basic strategy */
    pub rollups: *mut List,       /* list of RollupData */
    pub qual: *mut List,          /* quals (HAVING quals), if any */
    pub transitionSpace: uint64,  /* for pass-by-ref transition data */
}

/*
 * MinMaxAggPath represents computation of MIN/MAX aggregates from indexes
 */
#[repr(C)]
pub struct MinMaxAggPath {
    pub path: Path,
    pub mmaggregates: *mut List, /* list of MinMaxAggInfo */
    pub quals: *mut List,        /* HAVING quals, if any */
}

/*
 * WindowAggPath represents generic computation of window functions
 */
#[repr(C)]
pub struct WindowAggPath {
    pub path: Path,
    pub subpath: *mut Path,       /* path representing input source */
    pub winclause: *mut WindowClause, /* WindowClause we'll be using */
    pub qual: *mut List,          /* lower-level WindowAgg runconditions */
    pub runCondition: *mut List,  /* OpExpr List to short-circuit execution */
    pub topwindow: bool,          /* false for all apart from the WindowAgg
                                   * that's closest to the root of the plan */
}

/*
 * SetOpPath represents a set-operation, that is INTERSECT or EXCEPT
 */
#[repr(C)]
pub struct SetOpPath {
    pub path: Path,
    pub leftpath: *mut Path, /* paths representing input sources */
    pub rightpath: *mut Path,
    pub cmd: SetOpCmd,           /* what to do, see nodes.h */
    pub strategy: SetOpStrategy, /* how to do it, see nodes.h */
    pub groupList: *mut List,    /* SortGroupClauses identifying target cols */
    pub numGroups: Cardinality,  /* estimated number of groups in left input */
}

/*
 * RecursiveUnionPath represents a recursive UNION node
 */
#[repr(C)]
pub struct RecursiveUnionPath {
    pub path: Path,
    pub leftpath: *mut Path, /* paths representing input sources */
    pub rightpath: *mut Path,
    pub distinctList: *mut List, /* SortGroupClauses identifying target cols */
    pub wtParam: c_int,          /* ID of Param representing work table */
    pub numGroups: Cardinality,  /* estimated number of groups in input */
}

/*
 * LockRowsPath represents acquiring row locks for SELECT FOR UPDATE/SHARE
 */
#[repr(C)]
pub struct LockRowsPath {
    pub path: Path,
    pub subpath: *mut Path, /* path representing input source */
    pub rowMarks: *mut List, /* a list of PlanRowMark's */
    pub epqParam: c_int,    /* ID of Param for EvalPlanQual re-eval */
}

/*
 * ModifyTablePath represents performing INSERT/UPDATE/DELETE/MERGE
 *
 * We represent most things that will be in the ModifyTable plan node
 * literally, except we have a child Path not Plan.  But analysis of the
 * OnConflictExpr is deferred to createplan.c, as is collection of FDW data.
 */
#[repr(C)]
pub struct ModifyTablePath {
    pub path: Path,
    pub subpath: *mut Path,       /* Path producing source data */
    pub operation: CmdType,       /* INSERT, UPDATE, DELETE, or MERGE */
    pub canSetTag: bool,          /* do we set the command tag/es_processed? */
    pub nominalRelation: Index,   /* Parent RT index for use of EXPLAIN */
    pub rootRelation: Index,      /* Root RT index, if partitioned/inherited */
    pub partColsUpdated: bool,    /* some part key in hierarchy updated? */
    pub resultRelations: *mut List, /* integer list of RT indexes */
    pub updateColnosLists: *mut List, /* per-target-table update_colnos lists */
    pub withCheckOptionLists: *mut List, /* per-target-table WCO lists */
    pub returningLists: *mut List, /* per-target-table RETURNING tlists */
    pub rowMarks: *mut List,      /* PlanRowMarks (non-locking only) */
    pub onconflict: *mut OnConflictExpr, /* ON CONFLICT clause, or NULL */
    pub epqParam: c_int,          /* ID of Param for EvalPlanQual re-eval */
    pub mergeActionLists: *mut List, /* per-target-table lists of actions for
                                      * MERGE */
    pub mergeJoinConditions: *mut List, /* per-target-table join conditions
                                         * for MERGE */
}

/*
 * LimitPath represents applying LIMIT/OFFSET restrictions
 */
#[repr(C)]
pub struct LimitPath {
    pub path: Path,
    pub subpath: *mut Path,    /* path representing input source */
    pub limitOffset: *mut Node, /* OFFSET parameter, or NULL if none */
    pub limitCount: *mut Node, /* COUNT parameter, or NULL if none */
    pub limitOption: LimitOption, /* FETCH FIRST with ties or exact number */
}

/*
 * Restriction clause info.
 *
 * (See pathnodes.h for the very long descriptive comment on RestrictInfo
 *  semantics, is_pushed_down, clone clauses, security_level, etc.)
 */
#[repr(C)]
pub struct RestrictInfo {
    // pg_node_attr(no_read, no_query_jumble)
    pub r#type: NodeTag,

    /* the represented clause of WHERE or JOIN */
    pub clause: *mut Expr,

    /* true if clause was pushed down in level */
    pub is_pushed_down: bool,

    /* see comment above */
    pub can_join: bool, // pg_node_attr(equal_ignore)

    /* see comment above */
    pub pseudoconstant: bool, // pg_node_attr(equal_ignore)

    /* see comment above */
    pub has_clone: bool,
    pub is_clone: bool,

    /* true if known to contain no leaked Vars */
    pub leakproof: bool, // pg_node_attr(equal_ignore)

    /* indicates if clause contains any volatile functions */
    pub has_volatile: VolatileFunctionStatus, // pg_node_attr(equal_ignore)

    /* see comment above */
    pub security_level: Index,

    /* number of base rels in clause_relids */
    pub num_base_rels: c_int, // pg_node_attr(equal_ignore)

    /* The relids (varnos+varnullingrels) actually referenced in the clause: */
    pub clause_relids: Relids, // pg_node_attr(equal_ignore)

    /* The set of relids required to evaluate the clause: */
    pub required_relids: Relids,

    /* Relids above which we cannot evaluate the clause (see comment above) */
    pub incompatible_relids: Relids,

    /* If an outer-join clause, the outer-side relations, else NULL: */
    pub outer_relids: Relids,

    /*
     * Relids in the left/right side of the clause.  These fields are set for
     * any binary opclause.
     */
    pub left_relids: Relids,  // pg_node_attr(equal_ignore)
    pub right_relids: Relids, // pg_node_attr(equal_ignore)

    /*
     * Modified clause with RestrictInfos.  This field is NULL unless clause
     * is an OR clause.
     */
    pub orclause: *mut Expr, // pg_node_attr(equal_ignore)

    /*----------
     * Serial number of this RestrictInfo.  This is unique within the current
     * PlannerInfo context, with a few critical exceptions (see pathnodes.h).
     *----------
     */
    pub rinfo_serial: c_int,

    /*
     * Generating EquivalenceClass.  This field is NULL unless clause is
     * potentially redundant.
     */
    pub parent_ec: *mut EquivalenceClass, // pg_node_attr(copy_as_scalar, equal_ignore, read_write_ignore)

    /*
     * cache space for cost and selectivity
     */

    /* eval cost of clause; -1 if not yet set */
    pub eval_cost: QualCost, // pg_node_attr(equal_ignore)

    /* selectivity for "normal" (JOIN_INNER) semantics; -1 if not yet set */
    pub norm_selec: Selectivity, // pg_node_attr(equal_ignore)
    /* selectivity for outer join semantics; -1 if not yet set */
    pub outer_selec: Selectivity, // pg_node_attr(equal_ignore)

    /*
     * opfamilies containing clause operator; valid if clause is
     * mergejoinable, else NIL
     */
    pub mergeopfamilies: *mut List, // pg_node_attr(equal_ignore)

    /*
     * cache space for mergeclause processing; NULL if not yet set
     */

    /* EquivalenceClass containing lefthand */
    pub left_ec: *mut EquivalenceClass, // pg_node_attr(copy_as_scalar, equal_ignore, read_write_ignore)
    /* EquivalenceClass containing righthand */
    pub right_ec: *mut EquivalenceClass, // pg_node_attr(copy_as_scalar, equal_ignore, read_write_ignore)
    /* EquivalenceMember for lefthand */
    pub left_em: *mut EquivalenceMember, // pg_node_attr(copy_as_scalar, equal_ignore)
    /* EquivalenceMember for righthand */
    pub right_em: *mut EquivalenceMember, // pg_node_attr(copy_as_scalar, equal_ignore)

    /*
     * List of MergeScanSelCache structs.  Those aren't Nodes, so hard to
     * copy; instead replace with NIL.  That has the effect that copying will
     * just reset the cache.  Likewise, can't compare or print them.
     */
    pub scansel_cache: *mut List, // pg_node_attr(copy_as(NIL), equal_ignore, read_write_ignore)

    /*
     * transient workspace for use while considering a specific join path; T =
     * outer var on left, F = on right
     */
    pub outer_is_left: bool, // pg_node_attr(equal_ignore)

    /*
     * copy of clause operator; valid if clause is hashjoinable, else
     * InvalidOid
     */
    pub hashjoinoperator: Oid, // pg_node_attr(equal_ignore)

    /*
     * cache space for hashclause processing; -1 if not yet set
     */
    /* avg bucketsize of left side */
    pub left_bucketsize: Selectivity, // pg_node_attr(equal_ignore)
    /* avg bucketsize of right side */
    pub right_bucketsize: Selectivity, // pg_node_attr(equal_ignore)
    /* left side's most common val's freq */
    pub left_mcvfreq: Selectivity, // pg_node_attr(equal_ignore)
    /* right side's most common val's freq */
    pub right_mcvfreq: Selectivity, // pg_node_attr(equal_ignore)

    /* hash equality operators used for memoize nodes, else InvalidOid */
    pub left_hasheqoperator: Oid, // pg_node_attr(equal_ignore)
    pub right_hasheqoperator: Oid, // pg_node_attr(equal_ignore)
}

/*
 * This macro embodies the correct way to test whether a RestrictInfo is
 * "pushed down" to a given outer join, that is, should be treated as a filter
 * clause rather than a join clause at that outer join.  (See pathnodes.h.)
 *
 * #define RINFO_IS_PUSHED_DOWN(rinfo, joinrelids) \
 *     ((rinfo)->is_pushed_down || \
 *      !bms_is_subset((rinfo)->required_relids, joinrelids))
 * TODO(pg-port): bms_is_subset() from bitmapset.c.
 */

/*
 * Since mergejoinscansel() is a relatively expensive function, and would
 * otherwise be invoked many times while planning a large join tree,
 * we go out of our way to cache its results.  Each mergejoinable
 * RestrictInfo carries a list of the specific sort orderings that have
 * been considered for use with it, and the resulting selectivities.
 */
#[repr(C)]
pub struct MergeScanSelCache {
    /* Ordering details (cache lookup key) */
    pub opfamily: Oid,    /* index opfamily defining the ordering */
    pub collation: Oid,   /* collation for the ordering */
    pub cmptype: CompareType, /* sort direction (ASC or DESC) */
    pub nulls_first: bool, /* do NULLs come before normal values? */
    /* Results */
    pub leftstartsel: Selectivity,  /* first-join fraction for clause left side */
    pub leftendsel: Selectivity,    /* last-join fraction for clause left side */
    pub rightstartsel: Selectivity, /* first-join fraction for clause right side */
    pub rightendsel: Selectivity,   /* last-join fraction for clause right side */
}

/*
 * Placeholder node for an expression to be evaluated below the top level
 * of a plan tree.  (See pathnodes.h for the full descriptive comment.)
 *
 * Although the planner treats this as an expression node type, it is not
 * recognized by the parser or executor, so we declare it here rather than
 * in primnodes.h.
 */
#[repr(C)]
pub struct PlaceHolderVar {
    // pg_node_attr(no_query_jumble)
    pub xpr: Expr,

    /* the represented expression */
    pub phexpr: *mut Expr, // pg_node_attr(equal_ignore)

    /* base+OJ relids syntactically within expr src */
    pub phrels: Relids, // pg_node_attr(equal_ignore)

    /* RT indexes of outer joins that can null PHV's value */
    pub phnullingrels: Relids,

    /* ID for PHV (unique within planner run) */
    pub phid: Index,

    /* > 0 if PHV belongs to outer query */
    pub phlevelsup: Index,
}

/*
 * "Special join" info.
 *
 * (See pathnodes.h for the very long descriptive comment on SpecialJoinInfo
 *  semantics, commute_above/below, lhs_strict, semi_xxx fields, etc.)
 */
#[repr(C)]
pub struct SpecialJoinInfo {
    // pg_node_attr(no_read, no_query_jumble)
    pub r#type: NodeTag,
    pub min_lefthand: Relids,  /* base+OJ relids in minimum LHS for join */
    pub min_righthand: Relids, /* base+OJ relids in minimum RHS for join */
    pub syn_lefthand: Relids,  /* base+OJ relids syntactically within LHS */
    pub syn_righthand: Relids, /* base+OJ relids syntactically within RHS */
    pub jointype: JoinType,    /* always INNER, LEFT, FULL, SEMI, or ANTI */
    pub ojrelid: Index,        /* outer join's RT index; 0 if none */
    pub commute_above_l: Relids, /* commuting OJs above this one, if LHS */
    pub commute_above_r: Relids, /* commuting OJs above this one, if RHS */
    pub commute_below_l: Relids, /* commuting OJs in this one's LHS */
    pub commute_below_r: Relids, /* commuting OJs in this one's RHS */
    pub lhs_strict: bool,      /* joinclause is strict for some LHS rel */
    /* Remaining fields are set only for JOIN_SEMI jointype: */
    pub semi_can_btree: bool,  /* true if semi_operators are all btree */
    pub semi_can_hash: bool,   /* true if semi_operators are all hash */
    pub semi_operators: *mut List, /* OIDs of equality join operators */
    pub semi_rhs_exprs: *mut List, /* righthand-side expressions of these ops */
}

/*
 * Transient outer-join clause info.
 *
 * We set aside every outer join ON clause that looks mergejoinable,
 * and process it specially at the end of qual distribution.
 */
#[repr(C)]
pub struct OuterJoinClauseInfo {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,
    pub rinfo: *mut RestrictInfo,  /* a mergejoinable outer-join clause */
    pub sjinfo: *mut SpecialJoinInfo, /* the outer join's SpecialJoinInfo */
}

/*
 * Append-relation info.
 *
 * (See pathnodes.h for the full descriptive comment on AppendRelInfo.)
 */
#[repr(C)]
pub struct AppendRelInfo {
    // pg_node_attr(no_query_jumble)
    pub r#type: NodeTag,

    /*
     * These fields uniquely identify this append relationship.  There can be
     * (in fact, always should be) multiple AppendRelInfos for the same
     * parent_relid, but never more than one per child_relid, since a given
     * RTE cannot be a child of more than one append parent.
     */
    pub parent_relid: Index, /* RT index of append parent rel */
    pub child_relid: Index,  /* RT index of append child rel */

    /*
     * For an inheritance appendrel, the parent and child are both regular
     * relations, and we store their rowtype OIDs here for use in translating
     * whole-row Vars.  For a UNION-ALL appendrel, the parent and child are
     * both subqueries with no named rowtype, and we store InvalidOid here.
     */
    pub parent_reltype: Oid, /* OID of parent's composite type */
    pub child_reltype: Oid,  /* OID of child's composite type */

    /*
     * The N'th element of this list is a Var or expression representing the
     * child column corresponding to the N'th column of the parent. This is
     * used to translate Vars referencing the parent rel into references to
     * the child.  A list element is NULL if it corresponds to a dropped
     * column of the parent (this is only possible for inheritance cases, not
     * UNION ALL).  The list elements are always simple Vars for inheritance
     * cases, but can be arbitrary expressions in UNION ALL cases.
     *
     * Notice we only store entries for user columns (attno > 0).  Whole-row
     * Vars are special-cased, and system columns (attno < 0) need no special
     * translation since their attnos are the same for all tables.
     *
     * Caution: the Vars have varlevelsup = 0.  Be careful to adjust as needed
     * when copying into a subquery.
     */
    pub translated_vars: *mut List, /* Expressions in the child's Vars */

    /*
     * This array simplifies translations in the reverse direction, from
     * child's column numbers to parent's.  The entry at [ccolno - 1] is the
     * 1-based parent column number for child column ccolno, or zero if that
     * child column is dropped or doesn't exist in the parent.
     */
    pub num_child_cols: c_int, /* length of array */
    pub parent_colnos: *mut AttrNumber, // pg_node_attr(array_size(num_child_cols))

    /*
     * We store the parent table's OID here for inheritance, or InvalidOid for
     * UNION ALL.  This is only needed to help in generating error messages if
     * an attempt is made to reference a dropped parent column.
     */
    pub parent_reloid: Oid, /* OID of parent relation */
}

/*
 * Information about a row-identity "resjunk" column in UPDATE/DELETE/MERGE.
 *
 * (See pathnodes.h for the full descriptive comment on RowIdentityVarInfo.)
 */
#[repr(C)]
pub struct RowIdentityVarInfo {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    pub rowidvar: *mut Var,   /* Var to be evaluated (but varno=ROWID_VAR) */
    pub rowidwidth: int32,    /* estimated average width */
    pub rowidname: *mut c_char, /* name of the resjunk column */
    pub rowidrels: Relids,    /* RTE indexes of target rels using this */
}

/*
 * For each distinct placeholder expression generated during planning, we
 * store a PlaceHolderInfo node in the PlannerInfo node's placeholder_list.
 * (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
pub struct PlaceHolderInfo {
    // pg_node_attr(no_read, no_query_jumble)
    pub r#type: NodeTag,

    /* ID for PH (unique within planner run) */
    pub phid: Index,

    /*
     * copy of PlaceHolderVar tree (should be redundant for comparison, could
     * be ignored)
     */
    pub ph_var: *mut PlaceHolderVar,

    /* lowest level we can evaluate value at */
    pub ph_eval_at: Relids,

    /* relids of contained lateral refs, if any */
    pub ph_lateral: Relids,

    /* highest level the value is needed at */
    pub ph_needed: Relids,

    /* estimated attribute width */
    pub ph_width: int32,
}

/*
 * This struct describes one potentially index-optimizable MIN/MAX aggregate
 * function.  MinMaxAggPath contains a list of these, and if we accept that
 * path, the list is stored into root->minmax_aggs for use during setrefs.c.
 */
#[repr(C)]
pub struct MinMaxAggInfo {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    /* pg_proc Oid of the aggregate */
    pub aggfnoid: Oid,

    /* Oid of its sort operator */
    pub aggsortop: Oid,

    /* expression we are aggregating on */
    pub target: *mut Expr,

    /*
     * modified "root" for planning the subquery; not printed, too large, not
     * interesting enough
     */
    pub subroot: *mut PlannerInfo, // pg_node_attr(read_write_ignore)

    /* access path for subquery */
    pub path: *mut Path,

    /* estimated cost to fetch first row */
    pub pathcost: Cost,

    /* param for subplan's output */
    pub param: *mut Param,
}

/*
 * At runtime, PARAM_EXEC slots are used to pass values around from one plan
 * node to another.  (See pathnodes.h for the full descriptive comment on
 * PlannerParamItem and outer/nestloop parameter handling.)
 */
#[repr(C)]
pub struct PlannerParamItem {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    pub item: *mut Node, /* the Var, PlaceHolderVar, or Aggref */
    pub paramId: c_int,  /* its assigned PARAM_EXEC slot number */
}

/*
 * When making cost estimates for a SEMI/ANTI/inner_unique join, there are
 * some correction factors that are needed in both nestloop and hash joins
 * to account for the fact that the executor can stop scanning inner rows
 * as soon as it finds a match to the current outer row.  (See pathnodes.h.)
 */
#[repr(C)]
pub struct SemiAntiJoinFactors {
    pub outer_match_frac: Selectivity,
    pub match_count: Selectivity,
}

/*
 * Struct for extra information passed to subroutines of add_paths_to_joinrel
 *
 * (See pathnodes.h for the descriptive comment on each field.)
 */
#[repr(C)]
pub struct JoinPathExtraData {
    pub restrictlist: *mut List,
    pub mergeclause_list: *mut List,
    pub inner_unique: bool,
    pub sjinfo: *mut SpecialJoinInfo,
    pub semifactors: SemiAntiJoinFactors,
    pub param_source_rels: Relids,
}

/*
 * Various flags indicating what kinds of grouping are possible.
 *
 * (See pathnodes.h for the descriptive comment.)
 */
pub const GROUPING_CAN_USE_SORT: c_int = 0x0001;
pub const GROUPING_CAN_USE_HASH: c_int = 0x0002;
pub const GROUPING_CAN_PARTIAL_AGG: c_int = 0x0004;

/*
 * What kind of partitionwise aggregation is in use?
 *
 * PARTITIONWISE_AGGREGATE_NONE: Not used.
 *
 * PARTITIONWISE_AGGREGATE_FULL: Aggregate each partition separately, and
 * append the results.
 *
 * PARTITIONWISE_AGGREGATE_PARTIAL: Partially aggregate each partition
 * separately, append the results, and then finalize aggregation.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PartitionwiseAggregateType {
    PARTITIONWISE_AGGREGATE_NONE,
    PARTITIONWISE_AGGREGATE_FULL,
    PARTITIONWISE_AGGREGATE_PARTIAL,
}
pub use PartitionwiseAggregateType::*;

/*
 * Struct for extra information passed to subroutines of create_grouping_paths
 *
 * (See pathnodes.h for the descriptive comment on each field.)
 *
 * Note: this is an anonymous struct typedef in C, named GroupPathExtraData
 * after its typedef.
 */
#[repr(C)]
pub struct GroupPathExtraData {
    /* Data which remains constant once set. */
    pub flags: c_int,
    pub partial_costs_set: bool,
    pub agg_partial_costs: AggClauseCosts,
    pub agg_final_costs: AggClauseCosts,

    /* Data which may differ across partitions. */
    pub target_parallel_safe: bool,
    pub havingQual: *mut Node,
    pub targetList: *mut List,
    pub patype: PartitionwiseAggregateType,
}

/*
 * Struct for extra information passed to subroutines of grouping_planner
 *
 * (See pathnodes.h for the descriptive comment on each field.)
 *
 * Note: this is an anonymous struct typedef in C, named FinalPathExtraData
 * after its typedef.
 */
#[repr(C)]
pub struct FinalPathExtraData {
    pub limit_needed: bool,
    pub limit_tuples: Cardinality,
    pub count_est: int64,
    pub offset_est: int64,
}

/*
 * For speed reasons, cost estimation for join paths is performed in two
 * phases: the first phase tries to quickly derive a lower bound for the
 * join cost, and then we check if that's sufficient to reject the path.
 * (See pathnodes.h for the full descriptive comment.)
 */
#[repr(C)]
pub struct JoinCostWorkspace {
    /* Preliminary cost estimates --- must not be larger than final ones! */
    pub disabled_nodes: c_int,
    pub startup_cost: Cost, /* cost expended before fetching any tuples */
    pub total_cost: Cost,   /* total cost (assuming all tuples fetched) */

    /* Fields below here should be treated as private to costsize.c */
    pub run_cost: Cost, /* non-startup cost components */

    /* private for cost_nestloop code */
    pub inner_run_cost: Cost, /* also used by cost_mergejoin code */
    pub inner_rescan_run_cost: Cost,

    /* private for cost_mergejoin code */
    pub outer_rows: Cardinality,
    pub inner_rows: Cardinality,
    pub outer_skip_rows: Cardinality,
    pub inner_skip_rows: Cardinality,

    /* private for cost_hashjoin code */
    pub numbuckets: c_int,
    pub numbatches: c_int,
    pub inner_rows_total: Cardinality,
}

/*
 * AggInfo holds information about an aggregate that needs to be computed.
 * Multiple Aggrefs in a query can refer to the same AggInfo by having the
 * same 'aggno' value, so that the aggregate is computed only once.
 */
#[repr(C)]
pub struct AggInfo {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    /*
     * List of Aggref exprs that this state value is for.
     *
     * There will always be at least one, but there can be multiple identical
     * Aggref's sharing the same per-agg.
     */
    pub aggrefs: *mut List,

    /* Transition state number for this aggregate */
    pub transno: c_int,

    /*
     * "shareable" is false if this agg cannot share state values with other
     * aggregates because the final function is read-write.
     */
    pub shareable: bool,

    /* Oid of the final function, or InvalidOid if none */
    pub finalfn_oid: Oid,
}

/*
 * AggTransInfo holds information about transition state that is used by one
 * or more aggregates in the query.  Multiple aggregates can share the same
 * transition state, if they have the same inputs and the same transition
 * function.  Aggrefs that share the same transition info have the same
 * 'aggtransno' value.
 */
#[repr(C)]
pub struct AggTransInfo {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    /* Inputs for this transition state */
    pub args: *mut List,
    pub aggfilter: *mut Expr,

    /* Oid of the state transition function */
    pub transfn_oid: Oid,

    /* Oid of the serialization function, or InvalidOid if none */
    pub serialfn_oid: Oid,

    /* Oid of the deserialization function, or InvalidOid if none */
    pub deserialfn_oid: Oid,

    /* Oid of the combine function, or InvalidOid if none */
    pub combinefn_oid: Oid,

    /* Oid of state value's datatype */
    pub aggtranstype: Oid,

    /* Additional data about transtype */
    pub aggtranstypmod: int32,
    pub transtypeLen: c_int,
    pub transtypeByVal: bool,

    /* Space-consumption estimate */
    pub aggtransspace: int32,

    /* Initial value from pg_aggregate entry */
    pub initValue: Datum, // pg_node_attr(read_write_ignore)
    pub initValueIsNull: bool,
}

/*
 * UniqueRelInfo caches a fact that a relation is unique when being joined
 * to other relation(s).
 */
#[repr(C)]
pub struct UniqueRelInfo {
    // pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    pub r#type: NodeTag,

    /*
     * The relation in consideration is unique when being joined with this set
     * of other relation(s).
     */
    pub outerrelids: Relids,

    /*
     * The relation in consideration is unique when considering only clauses
     * suitable for self-join (passed split_selfjoin_quals()).
     */
    pub self_join: bool,

    /*
     * Additional clauses from a baserestrictinfo list that were used to prove
     * the uniqueness.   We cache it for the self-join checking procedure: a
     * self-join can be removed if the outer relation contains strictly the
     * same set of clauses.
     */
    pub extra_clauses: *mut List,
}
