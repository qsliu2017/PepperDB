//! selfuncs.rs
//!   Selectivity functions and index cost estimation functions for standard
//!   operators and index access methods.
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/selfuncs.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/selfuncs.c

// #include "postgres.h"
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_macros)]
#![allow(dead_code)]

use crate::prelude::*;

// #include <ctype.h>, <math.h> -> libm via f64 methods

// Core node/fmgr types used throughout.
use crate::nodes::nodes::{Cost, JoinType, Node, NodeTag, Selectivity};
use crate::nodes::pg_list::List;
use crate::nodes::primnodes::{
    ArrayCoerceExpr, ArrayExpr, CaseTestExpr, Const, Expr, NullTest, OpExpr, RelabelType,
    RowCompareExpr, ScalarArrayOpExpr, Var,
};
use crate::utils::fmgr::{FmgrInfo, FunctionCallInfo};

// catalog/pg_statistic.h
use crate::catalog::pg_statistic::Form_pg_statistic;

// access/htup_details.h
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};

use std::ffi::{c_char, c_int, c_void};

// =====================================================================
// Type aliases / scalar helpers (from c.h / postgres.h)
// =====================================================================
// Pointer/Size/Index/RegProcedure/InvalidOid/OidIsValid come from the prelude.
type AttrNumber = int16;
type Relids = *mut crate::nodes::bitmapset::Bitmapset;
type Bitmapset = crate::nodes::bitmapset::Bitmapset;
type StrategyNumber = uint16;
type BlockNumber = uint32;
type OffsetNumber = uint16;
type Buffer = c_int;

const InvalidBuffer: Buffer = 0;
const InvalidStrategy: StrategyNumber = 0;
const InvalidBlockNumber: BlockNumber = 0xFFFFFFFF;
const InvalidAttrNumber: AttrNumber = 0;
const INDEX_MAX_KEYS: usize = 32;

// =====================================================================
// selfuncs.h: default selectivity constants
// =====================================================================

/* default selectivity estimate for equalities such as "A = b" */
const DEFAULT_EQ_SEL: f64 = 0.005;

/* default selectivity estimate for inequalities such as "A < b" */
const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;

/* default selectivity estimate for range inequalities "A > b AND A < c" */
const DEFAULT_RANGE_INEQ_SEL: f64 = 0.005;

/* default selectivity estimate for multirange inequalities "A > b AND A < c" */
const DEFAULT_MULTIRANGE_INEQ_SEL: f64 = 0.005;

/* default selectivity estimate for pattern-match operators such as LIKE */
const DEFAULT_MATCH_SEL: f64 = 0.005;

/* default selectivity estimate for other matching operators */
const DEFAULT_MATCHING_SEL: f64 = 0.010;

/* default number of distinct values in a table */
const DEFAULT_NUM_DISTINCT: f64 = 200.0;

/* default selectivity estimate for boolean and null test nodes */
const DEFAULT_UNK_SEL: f64 = 0.005;
const DEFAULT_NOT_UNK_SEL: f64 = 1.0 - DEFAULT_UNK_SEL;

const DEFAULT_PAGE_CPU_MULTIPLIER: f64 = 50.0;

/*
 * Clamp a computed probability estimate (which may suffer from roundoff or
 * estimation errors) to valid range.  Argument must be a float variable.
 */
macro_rules! CLAMP_PROBABILITY {
    ($p:expr) => {{
        if $p < 0.0 {
            $p = 0.0;
        } else if $p > 1.0 {
            $p = 1.0;
        }
    }};
}

/*
 * A set of flags which some selectivity estimation functions can pass back to
 * callers to provide further details about some assumptions which were made
 * during the estimation.
 */
const SELFLAG_USED_DEFAULT: uint32 = 1 << 0;

#[repr(C)]
pub struct EstimationInfo {
    pub flags: uint32, /* Flags marking special properties of the estimation. */
}

/* Return data from examine_variable and friends */
#[repr(C)]
pub struct VariableStatData {
    pub var: *mut Node,       /* the Var or expression tree */
    pub rel: *mut RelOptInfo, /* Relation, or NULL if not identifiable */
    pub statsTuple: HeapTuple, /* pg_statistic tuple, or NULL if none */
    /* NB: if statsTuple!=NULL, it must be freed when caller is done */
    pub freefunc: Option<unsafe fn(tuple: HeapTuple)>, /* how to free statsTuple */
    pub vartype: Oid,         /* exposed type of expression */
    pub atttype: Oid,         /* actual type (after stripping relabel) */
    pub atttypmod: int32,     /* actual typmod (after stripping relabel) */
    pub isunique: bool,       /* matches unique index, DISTINCT or GROUP-BY clause */
    pub acl_ok: bool,         /* true if user has SELECT privilege on all rows */
}

/*
 * ReleaseVariableStats: free vardata.statsTuple if valid.
 */
macro_rules! ReleaseVariableStats {
    ($vardata:expr) => {{
        if HeapTupleIsValid($vardata.statsTuple) {
            if let Some(f) = $vardata.freefunc {
                f($vardata.statsTuple);
            }
        }
    }};
}

/*
 * GenericCosts: see selfuncs.h
 */
#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct GenericCosts {
    /* These are the values the cost estimator must return to the planner */
    pub indexStartupCost: Cost,    /* index-related startup cost */
    pub indexTotalCost: Cost,      /* total index-related scan cost */
    pub indexSelectivity: Selectivity, /* selectivity of index */
    pub indexCorrelation: f64,     /* order correlation of index */

    /* Intermediate values we obtain along the way */
    pub numIndexPages: f64,        /* number of leaf pages visited */
    pub numIndexTuples: f64,       /* number of leaf tuples visited */
    pub spc_random_page_cost: f64, /* relevant random_page_cost value */
    pub num_sa_scans: f64,         /* # indexscans from ScalarArrayOpExprs */
}

/* Hooks for plugins to get control when we ask for stats */
pub type get_relation_stats_hook_type = unsafe fn(
    root: *mut PlannerInfo,
    rte: *mut RangeTblEntry,
    attnum: AttrNumber,
    vardata: *mut VariableStatData,
) -> bool;
pub type get_index_stats_hook_type = unsafe fn(
    root: *mut PlannerInfo,
    indexOid: Oid,
    indexattnum: AttrNumber,
    vardata: *mut VariableStatData,
) -> bool;

pub static mut get_relation_stats_hook: Option<get_relation_stats_hook_type> = None;
pub static mut get_index_stats_hook: Option<get_index_stats_hook_type> = None;

// =====================================================================
// catalog/pg_type.h type OIDs (utils/builtins, pg_type_d.h)
// =====================================================================
use crate::catalog::pg_type_d::{
    BOOLOID, BYTEAOID, FLOAT8OID, INT2OID, INT4OID, INT8OID, INETOID, NUMERICOID, TEXTOID,
    TIMESTAMPOID,
};

// Additional type OIDs not present in pg_type_d.rs yet.
// TODO(pg-port): real values live in catalog/pg_type_d.rs
const FLOAT4OID: Oid = 700;
const OIDOID: Oid = 26;
const REGPROCOID: Oid = 24;
const REGPROCEDUREOID: Oid = 2202;
const REGOPEROID: Oid = 2203;
const REGOPERATOROID: Oid = 2204;
const REGCLASSOID: Oid = 2205;
const REGTYPEOID: Oid = 2206;
const REGCOLLATIONOID: Oid = 4191;
const REGCONFIGOID: Oid = 3734;
const REGDICTIONARYOID: Oid = 3769;
const REGROLEOID: Oid = 4096;
const REGNAMESPACEOID: Oid = 4089;
const CHAROID: Oid = 18;
const BPCHAROID: Oid = 1042;
const VARCHAROID: Oid = 1043;
const NAMEOID: Oid = 19;
const TIMESTAMPTZOID: Oid = 1184;
const DATEOID: Oid = 1082;
const INTERVALOID: Oid = 1186;
const TIMEOID: Oid = 1083;
const TIMETZOID: Oid = 1266;
const CIDROID: Oid = 650;
const MACADDROID: Oid = 829;
const MACADDR8OID: Oid = 774;

// catalog/pg_collation_d.h
const DEFAULT_COLLATION_OID: Oid = 100;

// access/stratnum.h: btree strategy numbers
const BTLessStrategyNumber: StrategyNumber = 1;
const BTEqualStrategyNumber: StrategyNumber = 3;

// catalog/pg_operator: BooleanEqualOperator
const BooleanEqualOperator: Oid = 91;

// utils/fmgroids.h: selectivity function fmgr OIDs
const F_EQSEL: RegProcedure = 101;
const F_EQJOINSEL: RegProcedure = 105;
const F_NEQSEL: RegProcedure = 102;
const F_NEQJOINSEL: RegProcedure = 106;

// access/sysattr.h
const SelfItemPointerAttributeNumber: AttrNumber = -1;
const TableOidAttributeNumber: AttrNumber = -7;
const FirstLowInvalidHeapAttributeNumber: AttrNumber = -8;

// catalog/pg_statistic.h: STATISTIC_KIND_*
const STATISTIC_KIND_MCV: int16 = 1;
const STATISTIC_KIND_HISTOGRAM: int16 = 2;
const STATISTIC_KIND_CORRELATION: int16 = 3;
const STATISTIC_KIND_MCELEM: int16 = 4;
const STATISTIC_KIND_DECHIST: int16 = 5;

// utils/lsyscache.h: get_attstatsslot() flags
const ATTSTATSSLOT_VALUES: c_int = 0x01;
const ATTSTATSSLOT_NUMBERS: c_int = 0x02;

// utils/typcache.h: lookup_type_cache() flags
const TYPECACHE_EQ_OPR: c_int = 0x0001;

// nodes/parsenodes.h: rtekind values
const RTE_RELATION: c_int = 0;
const RTE_SUBQUERY: c_int = 1;
const RTE_JOIN: c_int = 2;
const RTE_FUNCTION: c_int = 3;
const RTE_VALUES: c_int = 5;
const RTE_CTE: c_int = 6;

// catalog/pg_class.h: relkind values
const RELKIND_RELATION: c_char = b'r' as c_char;
const RELKIND_MATVIEW: c_char = b'm' as c_char;
const RELKIND_FOREIGN_TABLE: c_char = b'f' as c_char;
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;

// statistics/statistics.h: STATS_EXT_* kinds
const STATS_EXT_NDISTINCT: c_char = b'd' as c_char;
const STATS_EXT_EXPRESSIONS: c_char = b'e' as c_char;

// nodes/primnodes.h: BoolTestType / NullTestType (real enums)
use crate::nodes::primnodes::BoolTestType::{
    self, IS_FALSE, IS_NOT_FALSE, IS_NOT_TRUE, IS_NOT_UNKNOWN, IS_TRUE, IS_UNKNOWN,
};
use crate::nodes::primnodes::NullTestType::{self, IS_NOT_NULL, IS_NULL};

// access/cmptype.h: CompareType
const COMPARE_LT: c_int = 1;
const COMPARE_LE: c_int = 2;
const COMPARE_EQ: c_int = 3;
const COMPARE_GE: c_int = 4;
const COMPARE_GT: c_int = 5;

// access/sdir.h: ScanDirection
type ScanDirection = c_int;
const BackwardScanDirection: ScanDirection = -1;
const ForwardScanDirection: ScanDirection = 1;

// access/skey.h: scankey flags
const SK_ISNULL: c_int = 0x0002;
const SK_SEARCHNOTNULL: c_int = 0x0040;

// access/gin.h: GIN search modes / proc numbers
const GIN_SEARCH_MODE_DEFAULT: int32 = 0;
const GIN_SEARCH_MODE_INCLUDE_EMPTY: int32 = 1;
const GIN_SEARCH_MODE_ALL: int32 = 2;
const GIN_EXTRACTQUERY_PROC: uint16 = 3;

// access/brin_page.h / brin.h
const BRIN_DEFAULT_PAGES_PER_RANGE: c_int = 128;
const REVMAP_PAGE_MAXITEMS: f64 = 1360.0;

// storage/block.h
const BLCKSZ: f64 = 8192.0;

// utils/acl.h
const ACL_SELECT: u64 = 1 << 1;
const ACLMASK_ALL: c_int = 0;
const ACLCHECK_OK: c_int = 0;

// access/itup.h
const NoLock: c_int = 0;

// =====================================================================
// JoinType variants (nodes/nodes.h)
// =====================================================================
use crate::nodes::nodes::JoinType::{JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_SEMI};

// =====================================================================
// Opaque node / catalog struct stubs (real homes noted)
// =====================================================================
// TODO(pg-port): real PlannerInfo lives in nodes/pathnodes.rs
#[repr(C)]
pub struct PlannerInfo {
    pub parse: *mut Query,
    pub glob: *mut PlannerGlobal,
    pub parent_root: *mut PlannerInfo,
    pub simple_rel_array: *mut *mut RelOptInfo,
    pub simple_rte_array: *mut *mut RangeTblEntry,
    pub append_rel_array: *mut *mut AppendRelInfo,
    pub outer_join_rels: Relids,
    pub cte_plan_ids: *mut List,
    pub aggtransinfos: *mut List,
    _private: [u8; 0],
}

// TODO(pg-port): real RelOptInfo lives in nodes/pathnodes.rs
#[repr(C)]
pub struct RelOptInfo {
    pub relid: Index,
    pub relids: Relids,
    pub rows: f64,
    pub tuples: f64,
    pub pages: f64,
    pub rtekind: c_int,
    pub indexlist: *mut List,
    pub statlist: *mut List,
    pub subroot: *mut PlannerInfo,
    pub userid: Oid,
    _private: [u8; 0],
}

// TODO(pg-port): real RestrictInfo lives in nodes/pathnodes.rs
#[repr(C)]
pub struct RestrictInfo {
    pub clause: *mut Expr,
    pub outer_is_left: bool,
    pub left_relids: Relids,
    pub right_relids: Relids,
    _private: [u8; 0],
}

// TODO(pg-port): real SpecialJoinInfo lives in nodes/pathnodes.rs
#[repr(C)]
pub struct SpecialJoinInfo {
    pub min_righthand: Relids,
    pub syn_lefthand: Relids,
    pub syn_righthand: Relids,
    pub jointype: JoinType,
    _private: [u8; 0],
}

// TODO(pg-port): real IndexOptInfo lives in nodes/pathnodes.rs
#[repr(C)]
pub struct IndexOptInfo {
    pub indexoid: Oid,
    pub rel: *mut RelOptInfo,
    pub pages: f64,
    pub tuples: f64,
    pub tree_height: c_int,
    pub ncolumns: c_int,
    pub nkeycolumns: c_int,
    pub indexkeys: *mut c_int,
    pub indexcollations: *mut Oid,
    pub opfamily: *mut Oid,
    pub opcintype: *mut Oid,
    pub sortopfamily: *mut Oid,
    pub reverse_sort: *mut bool,
    pub canreturn: *mut bool,
    pub relam: Oid,
    pub indexprs: *mut List,
    pub indpred: *mut List,
    pub predOK: bool,
    pub unique: bool,
    pub hypothetical: bool,
    pub reltablespace: Oid,
    pub opclassoptions: *mut *mut c_void,
    _private: [u8; 0],
}

// TODO(pg-port): real IndexPath lives in nodes/pathnodes.rs
#[repr(C)]
pub struct IndexPath {
    pub indexinfo: *mut IndexOptInfo,
    pub indexclauses: *mut List,
    pub indexorderbys: *mut List,
    _private: [u8; 0],
}

// TODO(pg-port): real Path lives in nodes/pathnodes.rs
#[repr(C)]
pub struct Path {
    pub pathtarget: *mut PathTarget,
    _private: [u8; 0],
}

// TODO(pg-port): real PathTarget lives in nodes/pathnodes.rs
#[repr(C)]
pub struct PathTarget {
    pub width: c_int,
    _private: [u8; 0],
}

// TODO(pg-port): real PlannerGlobal lives in nodes/pathnodes.rs
#[repr(C)]
pub struct PlannerGlobal {
    pub lastPHId: c_int,
    pub subroots: *mut List,
    _private: [u8; 0],
}

// TODO(pg-port): real QualCost lives in nodes/pathnodes.rs
#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct QualCost {
    pub startup: Cost,
    pub per_tuple: Cost,
}

// TODO(pg-port): real AggClauseCosts lives in nodes/pathnodes.rs
#[repr(C)]
pub struct AggClauseCosts {
    pub transitionSpace: Size,
    _private: [u8; 0],
}

// TODO(pg-port): real Query/RangeTblEntry/etc live in nodes/parsenodes.rs
#[repr(C)]
pub struct Query {
    pub rteperminfos: *mut List,
    pub cteList: *mut List,
    pub setOperations: *mut Node,
    pub groupingSets: *mut List,
    pub returningList: *mut List,
    pub targetList: *mut List,
    pub distinctClause: *mut List,
    pub groupClause: *mut List,
    _private: [u8; 0],
}
#[repr(C)]
pub struct RangeTblEntry {
    pub rtekind: c_int,
    pub relid: Oid,
    pub relkind: c_char,
    pub inh: bool,
    pub security_barrier: bool,
    pub securityQuals: *mut List,
    pub ctelevelsup: Index,
    pub ctename: *mut c_char,
    pub self_reference: bool,
    pub eref: *mut Alias,
    _private: [u8; 0],
}
#[repr(C)]
pub struct Alias {
    pub aliasname: *mut c_char,
    _private: [u8; 0],
}
#[repr(C)]
pub struct RTEPermissionInfo {
    pub checkAsUser: Oid,
    _private: [u8; 0],
}
#[repr(C)]
pub struct AppendRelInfo {
    pub parent_relid: Index,
    pub num_child_cols: c_int,
    pub parent_colnos: *mut AttrNumber,
    _private: [u8; 0],
}
#[repr(C)]
pub struct TargetEntry {
    pub expr: *mut Expr,
    pub resjunk: bool,
    _private: [u8; 0],
}
#[repr(C)]
pub struct CommonTableExpr {
    pub ctename: *mut c_char,
    _private: [u8; 0],
}

// nodes/primnodes.h node types come from nodes::primnodes (imported above).
// PlaceHolderVar lives in nodes::pathnodes.
use crate::nodes::pathnodes::PlaceHolderVar;

// nodes/pathnodes.h: IndexClause (real home nodes/pathnodes.rs)
// TODO(pg-port): real IndexClause lives in nodes/pathnodes.rs
#[repr(C)]
pub struct IndexClause {
    pub xpr: Node,
    pub rinfo: *mut RestrictInfo,
    pub indexquals: *mut List,
    pub lossy: bool,
    pub indexcol: c_int,
    pub indexcols: *mut List,
}

// statistics/statistics.h
#[repr(C)]
pub struct StatisticExtInfo {
    pub statOid: Oid,
    pub kind: c_char,
    pub inherit: bool,
    pub keys: *mut Bitmapset,
    pub exprs: *mut List,
    _private: [u8; 0],
}
#[repr(C)]
pub struct MVNDistinct {
    pub nitems: c_int,
    pub items: *mut MVNDistinctItem,
    _private: [u8; 0],
}
#[repr(C)]
pub struct MVNDistinctItem {
    pub ndistinct: f64,
    pub nattributes: c_int,
    pub attributes: *mut AttrNumber,
    _private: [u8; 0],
}

// access/gin.h, access/brin.h stats data
#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct GinStatsData {
    pub nPendingPages: BlockNumber,
    pub nTotalPages: BlockNumber,
    pub nEntryPages: BlockNumber,
    pub nDataPages: BlockNumber,
    pub nEntries: i64,
}
#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct BrinStatsData {
    pub pagesPerRange: BlockNumber,
    pub revmapNumPages: BlockNumber,
}

/*
 * utils/lsyscache.h: AttStatsSlot.  The real home is utils/cache/lsyscache.rs.
 */
// TODO(pg-port): real AttStatsSlot lives in utils/cache/lsyscache.rs
#[repr(C)]
pub struct AttStatsSlot {
    pub staop: Oid,
    pub stacoll: Oid,
    pub valuetype: Oid,
    pub values: *mut Datum,
    pub nvalues: c_int,
    pub numbers: *mut float4,
    pub nnumbers: c_int,
}

/*
 * utils/typcache.h: TypeCacheEntry.  Real home: utils/cache/typcache.rs.
 */
// TODO(pg-port): real TypeCacheEntry lives in utils/cache/typcache.rs
#[repr(C)]
pub struct TypeCacheEntry {
    pub type_id: Oid,
    pub eq_opr: Oid,
    _private: [u8; 0],
}

// utils/array.h
#[repr(C)]
pub struct ArrayType {
    _private: [u8; 0],
}

// utils/sortsupport / storage stubs
#[repr(C)]
pub struct Relation {
    _private: [u8; 0],
}
type RelationPtr = *mut Relation;
#[repr(C)]
pub struct TupleTableSlot {
    _private: [u8; 0],
}
// MemoryContext comes from the prelude (utils/palloc).
#[repr(C)]
pub struct IndexScanDescData {
    pub xs_want_itup: bool,
    pub xs_itup: *mut c_void,
    pub xs_itupdesc: *mut c_void,
    pub xs_recheck: bool,
    _private: [u8; 0],
}
type IndexScanDesc = *mut IndexScanDescData;
#[repr(C)]
pub struct ScanKeyData {
    _private: [u8; 64],
}
type ScanKey = *mut ScanKeyData;
type ItemPointer = *mut ItemPointerData;
#[repr(C)]
pub struct ItemPointerData {
    _private: [u8; 6],
}
#[repr(C)]
pub struct SnapshotData {
    _private: [u8; 0],
}
type pg_locale_t = *mut PgLocaleStruct;
#[repr(C)]
pub struct PgLocaleStruct {
    pub collate_is_c: bool,
    _private: [u8; 0],
}
#[repr(C)]
pub struct NameData {
    pub data: [c_char; 64],
}
type bytea = c_void;
#[repr(C)]
pub struct Interval {
    pub time: i64,
    pub day: int32,
    pub month: int32,
}
#[repr(C)]
pub struct TimeTzADT {
    pub time: i64,
    pub zone: int32,
}

// =====================================================================
// Macro / node-tag helpers
// =====================================================================

// fcinfo args[] are a flexible array; access them through these helpers.
macro_rules! FC_SET_ISNULL {
    ($fc:expr, $n:expr, $v:expr) => {
        (*(*$fc).args.as_mut_ptr().add($n)).isnull = $v
    };
}
macro_rules! FC_SET_VALUE {
    ($fc:expr, $n:expr, $v:expr) => {
        (*(*$fc).args.as_mut_ptr().add($n)).value = $v
    };
}

// IsA_!(node, T): C IsA(node, T).  Delegates to the crate-root IsA! macro,
// which compares nodeTag() against NodeTag::T_<T>.  We accept the bare struct
// name (as in C) and map it to the matching T_ tag.
use crate::nodes::nodes::nodeTag as nodeTag_;
macro_rules! IsA_ {
    ($node:expr, Const) => { !$node.is_null() && crate::IsA!($node, T_Const) };
    ($node:expr, Var) => { !$node.is_null() && crate::IsA!($node, T_Var) };
    ($node:expr, OpExpr) => { !$node.is_null() && crate::IsA!($node, T_OpExpr) };
    ($node:expr, ArrayCoerceExpr) => { !$node.is_null() && crate::IsA!($node, T_ArrayCoerceExpr) };
    ($node:expr, RelabelType) => { !$node.is_null() && crate::IsA!($node, T_RelabelType) };
    ($node:expr, CaseTestExpr) => { !$node.is_null() && crate::IsA!($node, T_CaseTestExpr) };
    ($node:expr, ArrayExpr) => { !$node.is_null() && crate::IsA!($node, T_ArrayExpr) };
    ($node:expr, ScalarArrayOpExpr) => { !$node.is_null() && crate::IsA!($node, T_ScalarArrayOpExpr) };
    ($node:expr, RowCompareExpr) => { !$node.is_null() && crate::IsA!($node, T_RowCompareExpr) };
    ($node:expr, NullTest) => { !$node.is_null() && crate::IsA!($node, T_NullTest) };
    ($node:expr, PlaceHolderVar) => { !$node.is_null() && crate::IsA!($node, T_PlaceHolderVar) };
    ($node:expr, RestrictInfo) => { !$node.is_null() && crate::IsA!($node, T_RestrictInfo) };
    ($node:expr, RangeTblEntry) => { !$node.is_null() && crate::IsA!($node, T_RangeTblEntry) };
    ($node:expr, PlannerInfo) => { !$node.is_null() && crate::IsA!($node, T_PlannerInfo) };
    ($node:expr, Query) => { !$node.is_null() && crate::IsA!($node, T_Query) };
    ($node:expr, IndexClause) => { !$node.is_null() && crate::IsA!($node, T_IndexClause) };
}

// makeNode_!(T): C makeNode(T).  Delegates to crate-root makeNode!.
macro_rules! makeNode_ {
    (CaseTestExpr) => { crate::makeNode!(CaseTestExpr, T_CaseTestExpr) };
}

// Math helpers (libm)
#[inline]
fn isnan(x: f64) -> bool {
    x.is_nan()
}
#[inline]
fn pow(x: f64, y: f64) -> f64 {
    x.powf(y)
}
#[inline]
fn ceil(x: f64) -> f64 {
    x.ceil()
}
#[inline]
fn floor(x: f64) -> f64 {
    x.floor()
}
#[inline]
fn rint(x: f64) -> f64 {
    x.round_ties_even()
}
#[inline]
fn log(x: f64) -> f64 {
    x.ln()
}
#[inline]
fn fabs(x: f64) -> f64 {
    x.abs()
}
#[inline]
fn Min<T: PartialOrd>(a: T, b: T) -> T {
    if a < b { a } else { b }
}
#[inline]
fn Max<T: PartialOrd>(a: T, b: T) -> T {
    if a > b { a } else { b }
}

// time-related conversion constants (datatype/timestamp.h)
const USECS_PER_DAY: f64 = 86400000000.0;
const DAYS_PER_YEAR: f64 = 365.25;
const MONTHS_PER_YEAR: f64 = 12.0;

// =====================================================================
// External-function stubs (real homes noted with TODO(pg-port)).
// These are heavily-dependent helpers from lsyscache, plancat, cost,
// typcache, syscache, fmgr, pg_list, bitmapset, etc.  Stubbed minimally
// so that selfuncs.rs has no undefined symbols until those modules land.
// =====================================================================

// --- utils/cache/lsyscache.c ---
unsafe fn get_negator(opno: Oid) -> Oid { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_negator") }
unsafe fn get_commutator(opno: Oid) -> Oid { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_commutator") }
unsafe fn get_opcode(opno: Oid) -> Oid { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_opcode") }
unsafe fn get_oprrest(opno: Oid) -> RegProcedure { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_oprrest") }
unsafe fn get_oprjoin(opno: Oid) -> RegProcedure { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_oprjoin") }
unsafe fn get_base_element_type(typid: Oid) -> Oid { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_base_element_type") }
unsafe fn get_typlenbyval(typid: Oid, typlen: *mut int16, typbyval: *mut bool) { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_typlenbyval") }
unsafe fn get_typlenbyvalalign(typid: Oid, typlen: *mut int16, typbyval: *mut bool, typalign: *mut c_char) { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_typlenbyvalalign") }
unsafe fn get_opfamily_member(opfamily: Oid, lefttype: Oid, righttype: Oid, strategy: StrategyNumber) -> Oid { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_opfamily_member") }
unsafe fn get_opfamily_method(opfamily: Oid) -> Oid { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_opfamily_method") }
unsafe fn get_opfamily_proc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: uint16) -> Oid { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_opfamily_proc") }
unsafe fn get_op_opfamily_strategy(opno: Oid, opfamily: Oid) -> c_int { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_op_opfamily_strategy") }
unsafe fn get_op_opfamily_properties(opno: Oid, opfamily: Oid, ordering_op: bool, strategy: *mut c_int, lefttype: *mut Oid, righttype: *mut Oid) { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_op_opfamily_properties") }
unsafe fn comparison_ops_are_compatible(opno1: Oid, opno2: Oid) -> bool { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs comparison_ops_are_compatible") }
unsafe fn get_func_leakproof(func_oid: Oid) -> bool { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_func_leakproof") }
unsafe fn get_func_name(func_oid: Oid) -> *mut c_char { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_func_name") }
unsafe fn get_rel_name(relid: Oid) -> *mut c_char { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_rel_name") }
unsafe fn get_attstatsslot(sslot: *mut AttStatsSlot, statstuple: HeapTuple, reqkind: int16, reqop: Oid, flags: c_int) -> bool { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs get_attstatsslot") }
unsafe fn free_attstatsslot(sslot: *mut AttStatsSlot) { unimplemented!("TODO(pg-port): utils/cache/lsyscache.rs free_attstatsslot") }

// --- utils/cache/typcache.c ---
unsafe fn lookup_type_cache(type_id: Oid, flags: c_int) -> *mut TypeCacheEntry { unimplemented!("TODO(pg-port): utils/cache/typcache.rs lookup_type_cache") }

// --- utils/cache/syscache.c ---
unsafe fn SearchSysCache3(cacheId: c_int, key1: Datum, key2: Datum, key3: Datum) -> HeapTuple { unimplemented!("TODO(pg-port): utils/cache/syscache.rs SearchSysCache3") }
unsafe fn ReleaseSysCache(tuple: HeapTuple) { unimplemented!("TODO(pg-port): utils/cache/syscache.rs ReleaseSysCache") }
const STATRELATTINH: c_int = 0; // TODO(pg-port): syscacheid in utils/cache/syscache.rs

// --- access/index AM translation (access/amapi.c) ---
unsafe fn IndexAmTranslateStrategy(strategy: c_int, amoid: Oid, opfamily: Oid, guaranteed: bool) -> c_int { unimplemented!("TODO(pg-port): access/amapi.rs IndexAmTranslateStrategy") }
unsafe fn IndexAmTranslateCompareType(cmptype: c_int, amoid: Oid, opfamily: Oid, guaranteed: bool) -> c_int { unimplemented!("TODO(pg-port): access/amapi.rs IndexAmTranslateCompareType") }

// --- optimizer (clausesel/plancat/cost/paths/util) ---
unsafe fn clauselist_selectivity(root: *mut PlannerInfo, clauses: *mut List, varRelid: c_int, jointype: JoinType, sjinfo: *mut SpecialJoinInfo) -> Selectivity { unimplemented!("TODO(pg-port): optimizer/path/clausesel.rs clauselist_selectivity") }
unsafe fn clause_selectivity(root: *mut PlannerInfo, clause: *mut Node, varRelid: c_int, jointype: JoinType, sjinfo: *mut SpecialJoinInfo) -> Selectivity { unimplemented!("TODO(pg-port): optimizer/path/clausesel.rs clause_selectivity") }
unsafe fn restriction_selectivity(root: *mut PlannerInfo, operatorid: Oid, args: *mut List, inputcollid: Oid, varRelid: c_int) -> Selectivity { unimplemented!("TODO(pg-port): optimizer/util/plancat.rs restriction_selectivity") }
unsafe fn join_selectivity(root: *mut PlannerInfo, operatorid: Oid, args: *mut List, inputcollid: Oid, jointype: JoinType, sjinfo: *mut SpecialJoinInfo) -> Selectivity { unimplemented!("TODO(pg-port): optimizer/util/plancat.rs join_selectivity") }
unsafe fn estimate_expression_value(root: *mut PlannerInfo, node: *mut Node) -> *mut Node { unimplemented!("TODO(pg-port): optimizer/util/clauses.rs estimate_expression_value") }
unsafe fn expression_returns_set_rows(root: *mut PlannerInfo, clause: *mut Node) -> f64 { unimplemented!("TODO(pg-port): optimizer/util/clauses.rs expression_returns_set_rows") }
unsafe fn contain_volatile_functions(clause: *mut Node) -> bool { unimplemented!("TODO(pg-port): optimizer/util/clauses.rs contain_volatile_functions") }
unsafe fn find_base_rel(root: *mut PlannerInfo, relid: c_int) -> *mut RelOptInfo { unimplemented!("TODO(pg-port): optimizer/util/relnode.rs find_base_rel") }
unsafe fn find_base_rel_noerr(root: *mut PlannerInfo, relid: Index) -> *mut RelOptInfo { unimplemented!("TODO(pg-port): optimizer/util/relnode.rs find_base_rel_noerr") }
unsafe fn find_join_rel(root: *mut PlannerInfo, relids: Relids) -> *mut RelOptInfo { unimplemented!("TODO(pg-port): optimizer/util/relnode.rs find_join_rel") }
unsafe fn has_unique_index(rel: *mut RelOptInfo, attno: AttrNumber) -> bool { unimplemented!("TODO(pg-port): optimizer/util/plancat.rs has_unique_index") }
unsafe fn match_index_to_operand(operand: *mut Node, indexcol: c_int, index: *mut IndexOptInfo) -> bool { unimplemented!("TODO(pg-port): optimizer/path/indxpath.rs match_index_to_operand") }
unsafe fn exprs_known_equal(root: *mut PlannerInfo, item1: *mut Node, item2: *mut Node, opfamily: Oid) -> bool { unimplemented!("TODO(pg-port): optimizer/path/equivclass.rs exprs_known_equal") }
unsafe fn cost_qual_eval_node(cost: *mut QualCost, qual: *mut Node, root: *mut PlannerInfo) { unimplemented!("TODO(pg-port): optimizer/path/costsize.rs cost_qual_eval_node") }
unsafe fn index_pages_fetched(tuples_fetched: f64, pages: BlockNumber, index_pages: f64, root: *mut PlannerInfo) -> f64 { unimplemented!("TODO(pg-port): optimizer/path/costsize.rs index_pages_fetched") }
unsafe fn get_tablespace_page_costs(spcid: Oid, spc_random_page_cost: *mut f64, spc_seq_page_cost: *mut f64) { unimplemented!("TODO(pg-port): utils/cache/spccache.rs get_tablespace_page_costs") }
unsafe fn clamp_row_est(nrows: f64) -> f64 { unimplemented!("TODO(pg-port): optimizer/path/costsize.rs clamp_row_est") }
unsafe fn predicate_implied_by(predicate_list: *mut List, clause_list: *mut List, weak: bool) -> bool { unimplemented!("TODO(pg-port): optimizer/util/predtest.rs predicate_implied_by") }
unsafe fn hash_agg_entry_size(numTrans: c_int, tupleWidth: Size, transitionSpace: Size) -> Size { unimplemented!("TODO(pg-port): executor/nodeAgg.rs hash_agg_entry_size") }
unsafe fn NumRelids(root: *mut PlannerInfo, clause: *mut Node) -> c_int { unimplemented!("TODO(pg-port): optimizer/util/clauses.rs NumRelids") }
unsafe fn statext_ndistinct_load(mvoid: Oid, inh: bool) -> *mut MVNDistinct { unimplemented!("TODO(pg-port): statistics/extended_stats.rs statext_ndistinct_load") }
unsafe fn statext_expressions_load(stxoid: Oid, inh: bool, idx: c_int) -> HeapTuple { unimplemented!("TODO(pg-port): statistics/extended_stats.rs statext_expressions_load") }

// --- nodes/nodeFuncs.c, rewrite/rewriteManip.c, optimizer ---
unsafe fn exprType(expr: *mut Node) -> Oid { unimplemented!("TODO(pg-port): nodes/nodeFuncs.rs exprType") }
unsafe fn exprTypmod(expr: *mut Node) -> int32 { unimplemented!("TODO(pg-port): nodes/nodeFuncs.rs exprTypmod") }
unsafe fn exprCollation(expr: *mut Node) -> Oid { unimplemented!("TODO(pg-port): nodes/nodeFuncs.rs exprCollation") }
unsafe fn equal(a: *const c_void, b: *const c_void) -> bool { unimplemented!("TODO(pg-port): nodes/equalfuncs.rs equal") }
unsafe fn is_opclause(clause: *mut Node) -> bool { unimplemented!("TODO(pg-port): nodes/nodeFuncs.rs is_opclause") }
unsafe fn get_leftop(clause: *mut Expr) -> *mut Node { unimplemented!("TODO(pg-port): nodes/nodeFuncs.rs get_leftop") }
unsafe fn get_rightop(clause: *mut Expr) -> *mut Node { unimplemented!("TODO(pg-port): nodes/nodeFuncs.rs get_rightop") }
unsafe fn pull_varnos(root: *mut PlannerInfo, node: *mut Node) -> Relids { unimplemented!("TODO(pg-port): optimizer/util/var.rs pull_varnos") }
unsafe fn pull_var_clause(node: *mut Node, flags: c_int) -> *mut List { unimplemented!("TODO(pg-port): optimizer/util/var.rs pull_var_clause") }
unsafe fn remove_nulling_relids(node: *mut Node, removable_relids: Relids, except_relids: Relids) -> *mut Node { unimplemented!("TODO(pg-port): rewrite/rewriteManip.rs remove_nulling_relids") }
unsafe fn expression_tree_walker(node: *mut Node, walker: unsafe fn(*mut Node, *mut c_void) -> bool, context: *mut c_void) -> bool { unimplemented!("TODO(pg-port): nodes/nodeFuncs.rs expression_tree_walker") }
unsafe fn expression_tree_mutator(node: *mut Node, mutator: unsafe fn(*mut Node, *mut c_void) -> *mut Node, context: *mut c_void) -> *mut Node { unimplemented!("TODO(pg-port): nodes/nodeFuncs.rs expression_tree_mutator") }
const PVC_RECURSE_AGGREGATES: c_int = 0x0002;
const PVC_RECURSE_WINDOWFUNCS: c_int = 0x0008;
const PVC_RECURSE_PLACEHOLDERS: c_int = 0x0020;

// --- parser/parsetree.c, parse_relation, parse_clause ---
unsafe fn getRTEPermissionInfo(rteperminfos: *mut List, rte: *mut RangeTblEntry) -> *mut RTEPermissionInfo { unimplemented!("TODO(pg-port): parser/parse_relation.rs getRTEPermissionInfo") }
unsafe fn get_tle_by_resno(tlist: *mut List, resno: AttrNumber) -> *mut TargetEntry { unimplemented!("TODO(pg-port): parser/parsetree.rs get_tle_by_resno") }
unsafe fn targetIsInSortList(tle: *mut TargetEntry, sortop: Oid, sortlist: *mut List) -> bool { unimplemented!("TODO(pg-port): parser/parse_clause.rs targetIsInSortList") }
unsafe fn makeConst(consttype: Oid, consttypmod: int32, constcollid: Oid, constlen: c_int, constvalue: Datum, constisnull: bool, constbyval: bool) -> *mut Const { unimplemented!("TODO(pg-port): nodes/makefuncs.rs makeConst") }

// --- utils/adt: array, datum, network, numeric, timestamp ---
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType { unimplemented!("TODO(pg-port): utils/adt/arrayfuncs.rs DatumGetArrayTypeP") }
unsafe fn deconstruct_array(array: *mut ArrayType, elmtype: Oid, elmlen: int16, elmbyval: bool, elmalign: c_char, elemsp: *mut *mut Datum, nullsp: *mut *mut bool, nelemsp: *mut c_int) { unimplemented!("TODO(pg-port): utils/adt/arrayfuncs.rs deconstruct_array") }
unsafe fn ARR_ELEMTYPE(a: *mut ArrayType) -> Oid { unimplemented!("TODO(pg-port): utils/adt/array.rs ARR_ELEMTYPE") }
unsafe fn ARR_NDIM(a: *mut ArrayType) -> c_int { unimplemented!("TODO(pg-port): utils/adt/array.rs ARR_NDIM") }
unsafe fn ARR_DIMS(a: *mut ArrayType) -> *mut c_int { unimplemented!("TODO(pg-port): utils/adt/array.rs ARR_DIMS") }
unsafe fn ArrayGetNItems(ndim: c_int, dims: *mut c_int) -> c_int { unimplemented!("TODO(pg-port): utils/adt/arrayutils.rs ArrayGetNItems") }
unsafe fn datumCopy(value: Datum, typByVal: bool, typLen: int16) -> Datum { unimplemented!("TODO(pg-port): utils/adt/datum.rs datumCopy") }
unsafe fn convert_network_to_scalar(value: Datum, typid: Oid, failure: *mut bool) -> f64 { unimplemented!("TODO(pg-port): utils/adt/network.rs convert_network_to_scalar") }
unsafe fn date2timestamp_no_overflow(dateVal: int32) -> f64 { unimplemented!("TODO(pg-port): utils/adt/date.rs date2timestamp_no_overflow") }

// --- access/table, index, visibilitymap, bufmgr, tableam, snapmgr ---
unsafe fn table_open(relationId: Oid, lockmode: c_int) -> *mut Relation { unimplemented!("TODO(pg-port): access/table/table.rs table_open") }
unsafe fn table_close(relation: *mut Relation, lockmode: c_int) { unimplemented!("TODO(pg-port): access/table/table.rs table_close") }
unsafe fn index_open(relationId: Oid, lockmode: c_int) -> *mut Relation { unimplemented!("TODO(pg-port): access/index/indexam.rs index_open") }
unsafe fn index_close(relation: *mut Relation, lockmode: c_int) { unimplemented!("TODO(pg-port): access/index/indexam.rs index_close") }
unsafe fn table_slot_create(relation: *mut Relation, reglist: *mut *mut List) -> *mut TupleTableSlot { unimplemented!("TODO(pg-port): access/table/tableam.rs table_slot_create") }
unsafe fn ExecDropSingleTupleTableSlot(slot: *mut TupleTableSlot) { unimplemented!("TODO(pg-port): executor/execTuples.rs ExecDropSingleTupleTableSlot") }
unsafe fn ExecClearTuple(slot: *mut TupleTableSlot) { unimplemented!("TODO(pg-port): executor/execTuples.rs ExecClearTuple") }
unsafe fn index_beginscan(heapRelation: *mut Relation, indexRelation: *mut Relation, snapshot: *mut SnapshotData, instrument: *mut c_void, nkeys: c_int, norderbys: c_int) -> IndexScanDesc { unimplemented!("TODO(pg-port): access/index/indexam.rs index_beginscan") }
unsafe fn index_rescan(scan: IndexScanDesc, keys: ScanKey, nkeys: c_int, orderbys: ScanKey, norderbys: c_int) { unimplemented!("TODO(pg-port): access/index/indexam.rs index_rescan") }
unsafe fn index_endscan(scan: IndexScanDesc) { unimplemented!("TODO(pg-port): access/index/indexam.rs index_endscan") }
unsafe fn index_getnext_tid(scan: IndexScanDesc, direction: ScanDirection) -> ItemPointer { unimplemented!("TODO(pg-port): access/index/indexam.rs index_getnext_tid") }
unsafe fn index_fetch_heap(scan: IndexScanDesc, slot: *mut TupleTableSlot) -> bool { unimplemented!("TODO(pg-port): access/index/indexam.rs index_fetch_heap") }
unsafe fn index_deform_tuple(tup: *mut c_void, tupleDescriptor: *mut c_void, values: *mut Datum, isnull: *mut bool) { unimplemented!("TODO(pg-port): access/common/indextuple.rs index_deform_tuple") }
unsafe fn ScanKeyEntryInitialize(entry: ScanKey, flags: c_int, attributeNumber: AttrNumber, strategy: StrategyNumber, subtype: Oid, collation: Oid, procedure: RegProcedure, argument: Datum) { unimplemented!("TODO(pg-port): access/common/scankey.rs ScanKeyEntryInitialize") }
unsafe fn VM_ALL_VISIBLE(rel: *mut Relation, block: BlockNumber, vmbuf: *mut Buffer) -> bool { unimplemented!("TODO(pg-port): access/heap/visibilitymap.rs VM_ALL_VISIBLE") }
unsafe fn ReleaseBuffer(buffer: Buffer) { unimplemented!("TODO(pg-port): storage/buffer/bufmgr.rs ReleaseBuffer") }
unsafe fn GlobalVisTestFor(rel: *mut Relation) -> *mut c_void { unimplemented!("TODO(pg-port): storage/ipc/procarray.rs GlobalVisTestFor") }
unsafe fn ItemPointerGetBlockNumber(pointer: ItemPointer) -> BlockNumber { unimplemented!("TODO(pg-port): storage/itemptr.rs ItemPointerGetBlockNumber") }
unsafe fn ItemPointerGetBlockNumberNoCheck(pointer: ItemPointer) -> BlockNumber { unimplemented!("TODO(pg-port): storage/itemptr.rs ItemPointerGetBlockNumberNoCheck") }
unsafe fn ItemPointerGetOffsetNumberNoCheck(pointer: ItemPointer) -> OffsetNumber { unimplemented!("TODO(pg-port): storage/itemptr.rs ItemPointerGetOffsetNumberNoCheck") }
unsafe fn RelationGetRelationName(relation: *mut Relation) -> *mut c_char { unimplemented!("TODO(pg-port): utils/rel.rs RelationGetRelationName") }

// --- access/gin.c, access/brin.c stats ---
unsafe fn ginGetStats(index: *mut Relation, stats: *mut GinStatsData) { unimplemented!("TODO(pg-port): access/gin/ginutil.rs ginGetStats") }
unsafe fn brinGetStats(index: *mut Relation, stats: *mut BrinStatsData) { unimplemented!("TODO(pg-port): access/brin/brin.rs brinGetStats") }

// --- utils/mmgr ---
// palloc/palloc0/pfree/pstrdup/MemoryContext*/AllocSetContextCreate/
// ALLOCSET_DEFAULT_SIZES/CurrentMemoryContext come from the prelude.

// --- utils/acl ---
unsafe fn pg_class_aclcheck(table_oid: Oid, roleid: Oid, mode: u64) -> c_int { unimplemented!("TODO(pg-port): utils/adt/acl.rs pg_class_aclcheck") }
unsafe fn pg_attribute_aclcheck(table_oid: Oid, attnum: AttrNumber, roleid: Oid, mode: u64) -> c_int { unimplemented!("TODO(pg-port): utils/adt/acl.rs pg_attribute_aclcheck") }
unsafe fn pg_attribute_aclcheck_all(table_oid: Oid, roleid: Oid, mode: u64, how: c_int) -> c_int { unimplemented!("TODO(pg-port): utils/adt/acl.rs pg_attribute_aclcheck_all") }
unsafe fn GetUserId() -> Oid { unimplemented!("TODO(pg-port): utils/init/miscinit.rs GetUserId") }

// --- pg_locale ---
unsafe fn pg_newlocale_from_collation(collid: Oid) -> pg_locale_t { unimplemented!("TODO(pg-port): utils/adt/pg_locale.rs pg_newlocale_from_collation") }
unsafe fn pg_strxfrm(dest: *mut c_char, src: *const c_char, destsize: usize, locale: pg_locale_t) -> usize { unimplemented!("TODO(pg-port): utils/adt/pg_locale.rs pg_strxfrm") }

// --- bitmapset ---
unsafe fn bms_is_empty(a: Relids) -> bool { unimplemented!("TODO(pg-port): nodes/bitmapset.rs bms_is_empty") }
unsafe fn bms_is_member(x: c_int, a: Relids) -> bool { unimplemented!("TODO(pg-port): nodes/bitmapset.rs bms_is_member") }
unsafe fn bms_is_subset(a: Relids, b: Relids) -> bool { unimplemented!("TODO(pg-port): nodes/bitmapset.rs bms_is_subset") }
unsafe fn bms_overlap(a: Relids, b: Relids) -> bool { unimplemented!("TODO(pg-port): nodes/bitmapset.rs bms_overlap") }
unsafe fn bms_difference(a: Relids, b: Relids) -> Relids { unimplemented!("TODO(pg-port): nodes/bitmapset.rs bms_difference") }
unsafe fn bms_free(a: Relids) { unimplemented!("TODO(pg-port): nodes/bitmapset.rs bms_free") }
unsafe fn bms_get_singleton_member(a: Relids, member: *mut c_int) -> bool { unimplemented!("TODO(pg-port): nodes/bitmapset.rs bms_get_singleton_member") }
unsafe fn bms_make_singleton(x: c_int) -> Relids { unimplemented!("TODO(pg-port): nodes/bitmapset.rs bms_make_singleton") }
unsafe fn bms_add_member(a: Relids, x: c_int) -> Relids { unimplemented!("TODO(pg-port): nodes/bitmapset.rs bms_add_member") }
unsafe fn bms_num_members(a: Relids) -> c_int { unimplemented!("TODO(pg-port): nodes/bitmapset.rs bms_num_members") }
unsafe fn bms_next_member(a: Relids, prevbit: c_int) -> c_int { unimplemented!("TODO(pg-port): nodes/bitmapset.rs bms_next_member") }

// --- array_selfuncs.c (sibling) ---
unsafe fn scalararraysel_containment(root: *mut PlannerInfo, leftop: *mut Node, rightop: *mut Node, elemtype: Oid, isEquality: bool, useOr: bool, varRelid: c_int) -> Selectivity { unimplemented!("TODO(pg-port): utils/adt/array_selfuncs.rs scalararraysel_containment") }

// --- fmgr ---
unsafe fn fmgr_info(functionId: Oid, finfo: *mut FmgrInfo) { unimplemented!("TODO(pg-port): utils/fmgr.rs fmgr_info") }
unsafe fn set_fn_opclass_options(flinfo: *mut FmgrInfo, options: *mut c_void) { unimplemented!("TODO(pg-port): utils/fmgr.rs set_fn_opclass_options") }
unsafe fn FunctionCall2Coll(flinfo: *mut FmgrInfo, collation: Oid, arg1: Datum, arg2: Datum) -> Datum { unimplemented!("TODO(pg-port): utils/fmgr.rs FunctionCall2Coll") }

// DirectFunctionCall1 for numeric_float8_no_overflow / similar single-arg.
// fmgroids: numeric_float8_no_overflow lives in utils/adt/numeric.rs
unsafe fn numeric_float8_no_overflow(fcinfo: FunctionCallInfo) -> Datum { unimplemented!("TODO(pg-port): utils/adt/numeric.rs numeric_float8_no_overflow") }

// FunctionCall4Coll / FunctionCall5Coll / FunctionCall7Coll (utils/fmgr.rs)
unsafe fn FunctionCall4Coll(flinfo: *mut FmgrInfo, collation: Oid, arg1: Datum, arg2: Datum, arg3: Datum, arg4: Datum) -> Datum { unimplemented!("TODO(pg-port): utils/fmgr.rs FunctionCall4Coll") }
unsafe fn FunctionCall5Coll(flinfo: *mut FmgrInfo, collation: Oid, arg1: Datum, arg2: Datum, arg3: Datum, arg4: Datum, arg5: Datum) -> Datum { unimplemented!("TODO(pg-port): utils/fmgr.rs FunctionCall5Coll") }
unsafe fn FunctionCall7Coll(flinfo: *mut FmgrInfo, collation: Oid, arg1: Datum, arg2: Datum, arg3: Datum, arg4: Datum, arg5: Datum, arg6: Datum, arg7: Datum) -> Datum { unimplemented!("TODO(pg-port): utils/fmgr.rs FunctionCall7Coll") }
// DirectFunctionCall5Coll for eqjoinsel re-entry
unsafe fn DirectFunctionCall5Coll(func: unsafe fn(FunctionCallInfo) -> Datum, collation: Oid, arg1: Datum, arg2: Datum, arg3: Datum, arg4: Datum, arg5: Datum) -> Datum { unimplemented!("TODO(pg-port): utils/fmgr.rs DirectFunctionCall5Coll local helper") }

// DatumGet helpers not present in postgres.rs prelude
unsafe fn DatumGetInt64(d: Datum) -> i64 { d as i64 }
unsafe fn DatumGetFloat8(d: Datum) -> f64 { f64::from_bits(d as u64) }
unsafe fn DatumGetTimestamp(d: Datum) -> i64 { d as i64 }
unsafe fn DatumGetTimestampTz(d: Datum) -> i64 { d as i64 }
unsafe fn DatumGetDateADT(d: Datum) -> int32 { d as int32 }
unsafe fn DatumGetTimeADT(d: Datum) -> i64 { d as i64 }
unsafe fn DatumGetIntervalP(d: Datum) -> *mut Interval { DatumGetPointer(d) as *mut Interval }
unsafe fn DatumGetTimeTzADTP(d: Datum) -> *mut TimeTzADT { DatumGetPointer(d) as *mut TimeTzADT }
unsafe fn DatumGetByteaPP(d: Datum) -> *mut bytea { DatumGetPointer(d) as *mut bytea }
unsafe fn TextDatumGetCString(d: Datum) -> *mut c_char { unimplemented!("TODO(pg-port): utils/adt/varlena.rs TextDatumGetCString") }
unsafe fn VARSIZE_ANY_EXHDR(p: *mut bytea) -> c_int { unimplemented!("TODO(pg-port): c.h VARSIZE_ANY_EXHDR") }
unsafe fn VARDATA_ANY(p: *mut bytea) -> *mut c_char { unimplemented!("TODO(pg-port): c.h VARDATA_ANY") }
unsafe fn NameStr(n: &NameData) -> *const c_char { n.data.as_ptr() }

// IS_SIMPLE_REL / planner_rt_fetch helpers (pathnodes.h, parsetree.h)
unsafe fn IS_SIMPLE_REL(rel: *mut RelOptInfo) -> bool { true }
unsafe fn planner_rt_fetch(rti: Index, root: *mut PlannerInfo) -> *mut RangeTblEntry {
    *(*root).simple_rte_array.add(rti as usize)
}
#[inline]
fn AttrNumberIsForUserDefinedAttr(attno: AttrNumber) -> bool { attno > 0 }

// list helpers (pg_list.h): use the real implementations.
use crate::nodes::pg_list::{
    lappend, lfirst, linitial, linitial_oid, list_concat, list_copy, list_free, list_free_deep,
    list_head, list_length, list_member_int, list_member_ptr, list_nth, list_nth_int, lnext,
    lsecond, ListCell,
};
use crate::{current_cell, foreach, list_make2};
// fmgr / PG_* macros (crate-root #[macro_export]).
use crate::{
    DirectFunctionCall1, FunctionCallInvoke, InitFunctionCallInfoData, LOCAL_FCINFO,
    PG_GETARG_INT16, PG_GETARG_INT32, PG_GETARG_OID, PG_GETARG_POINTER, PG_GET_COLLATION,
    PG_RETURN_FLOAT8,
};
const NIL: *mut List = std::ptr::null_mut();

// =====================================================================
//  Selectivity estimation functions
// =====================================================================

/*
 *		eqsel			- Selectivity of "=" for any data types.
 */
pub unsafe fn eqsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(eqsel_internal(fcinfo, false) as float8)
}

/*
 * Common code for eqsel() and neqsel()
 */
unsafe fn eqsel_internal(fcinfo: FunctionCallInfo, negate: bool) -> f64 {
    let root = PG_GETARG_POINTER!(fcinfo, 0) as *mut PlannerInfo;
    let mut operator = PG_GETARG_OID!(fcinfo, 1);
    let args = PG_GETARG_POINTER!(fcinfo, 2) as *mut List;
    let varRelid = PG_GETARG_INT32!(fcinfo, 3);
    let collation = PG_GET_COLLATION!(fcinfo);
    let mut vardata: VariableStatData = std::mem::zeroed();
    let mut other: *mut Node = std::ptr::null_mut();
    let mut varonleft: bool = false;
    let selec: f64;

    /*
     * When asked about <>, we do the estimation using the corresponding =
     * operator, then convert to <> via "1.0 - eq_selectivity - nullfrac".
     */
    if negate {
        operator = get_negator(operator);
        if !OidIsValid(operator) {
            /* Use default selectivity (should we raise an error instead?) */
            return 1.0 - DEFAULT_EQ_SEL;
        }
    }

    /*
     * If expression is not variable = something or something = variable, then
     * punt and return a default estimate.
     */
    if !get_restriction_variable(root, args, varRelid, &mut vardata, &mut other, &mut varonleft) {
        return if negate { 1.0 - DEFAULT_EQ_SEL } else { DEFAULT_EQ_SEL };
    }

    /*
     * We can do a lot better if the something is a constant.
     */
    if IsA_!(other, Const) {
        selec = var_eq_const(
            &mut vardata,
            operator,
            collation,
            (*(other as *mut Const)).constvalue,
            (*(other as *mut Const)).constisnull,
            varonleft,
            negate,
        );
    } else {
        selec = var_eq_non_const(&mut vardata, operator, collation, other, varonleft, negate);
    }

    ReleaseVariableStats!(vardata);

    selec
}

/*
 * var_eq_const --- eqsel for var = const case
 */
pub unsafe fn var_eq_const(
    vardata: *mut VariableStatData,
    oproid: Oid,
    collation: Oid,
    constval: Datum,
    constisnull: bool,
    varonleft: bool,
    negate: bool,
) -> f64 {
    let mut selec: f64;
    let mut nullfrac: f64 = 0.0;
    let mut isdefault: bool = false;
    let opfuncoid: Oid;

    /*
     * If the constant is NULL, assume operator is strict and return zero.
     */
    if constisnull {
        return 0.0;
    }

    /*
     * Grab the nullfrac for use below.
     */
    if HeapTupleIsValid((*vardata).statsTuple) {
        let stats = GETSTRUCT((*vardata).statsTuple) as Form_pg_statistic;
        nullfrac = (*stats).stanullfrac as f64;
    }

    /*
     * If we matched the var to a unique index, DISTINCT or GROUP-BY clause,
     * assume there is exactly one match regardless of anything else.
     */
    if (*vardata).isunique && !(*vardata).rel.is_null() && (*(*vardata).rel).tuples >= 1.0 {
        selec = 1.0 / (*(*vardata).rel).tuples;
    } else if HeapTupleIsValid((*vardata).statsTuple)
        && {
            opfuncoid = get_opcode(oproid);
            statistic_proc_security_check(vardata, opfuncoid)
        }
    {
        let mut sslot: AttStatsSlot = std::mem::zeroed();
        let mut r#match = false;
        let mut i: c_int = 0;

        /*
         * Is the constant "=" to any of the column's most common values?
         */
        if get_attstatsslot(
            &mut sslot,
            (*vardata).statsTuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        ) {
            LOCAL_FCINFO!(fcinfo_l, 2);
            let mut eqproc: FmgrInfo = std::mem::zeroed();

            fmgr_info(opfuncoid, &mut eqproc);

            InitFunctionCallInfoData!(fcinfo_l, &mut eqproc, 2, collation, std::ptr::null_mut(), std::ptr::null_mut());
            FC_SET_ISNULL!(fcinfo_l, 0, false);
            FC_SET_ISNULL!(fcinfo_l, 1, false);
            /* be careful to apply operator right way 'round */
            if varonleft {
                FC_SET_VALUE!(fcinfo_l, 1, constval);
            } else {
                FC_SET_VALUE!(fcinfo_l, 0, constval);
            }

            i = 0;
            while i < sslot.nvalues {
                if varonleft {
                    FC_SET_VALUE!(fcinfo_l, 0, *sslot.values.add(i as usize));
                } else {
                    FC_SET_VALUE!(fcinfo_l, 1, *sslot.values.add(i as usize));
                }
                (*fcinfo_l).isnull = false;
                let fresult = FunctionCallInvoke!(fcinfo_l);
                if !(*fcinfo_l).isnull && DatumGetBool(fresult) {
                    r#match = true;
                    break;
                }
                i += 1;
            }
        } else {
            /* no most-common-value info available */
            i = 0; /* keep compiler quiet */
        }

        if r#match {
            /*
             * Constant is "=" to this common value.
             */
            selec = *sslot.numbers.add(i as usize) as f64;
        } else {
            /*
             * Comparison is against a constant that is neither NULL nor any
             * of the common values.
             */
            let mut sumcommon = 0.0;
            let otherdistinct;

            let mut j = 0;
            while j < sslot.nnumbers {
                sumcommon += *sslot.numbers.add(j as usize) as f64;
                j += 1;
            }
            selec = 1.0 - sumcommon - nullfrac;
            CLAMP_PROBABILITY!(selec);

            /*
             * and in fact it's probably a good deal less.
             */
            otherdistinct =
                get_variable_numdistinct(vardata, &mut isdefault) - sslot.nnumbers as f64;
            if otherdistinct > 1.0 {
                selec /= otherdistinct;
            }

            /*
             * Another cross-check: selectivity shouldn't be estimated as more
             * than the least common "most common value".
             */
            if sslot.nnumbers > 0
                && selec > *sslot.numbers.add((sslot.nnumbers - 1) as usize) as f64
            {
                selec = *sslot.numbers.add((sslot.nnumbers - 1) as usize) as f64;
            }
        }

        free_attstatsslot(&mut sslot);
    } else {
        /*
         * No ANALYZE stats available, so make a guess.
         */
        selec = 1.0 / get_variable_numdistinct(vardata, &mut isdefault);
    }

    /* now adjust if we wanted <> rather than = */
    if negate {
        selec = 1.0 - selec - nullfrac;
    }

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY!(selec);

    selec
}

/*
 * var_eq_non_const --- eqsel for var = something-other-than-const case
 */
pub unsafe fn var_eq_non_const(
    vardata: *mut VariableStatData,
    oproid: Oid,
    collation: Oid,
    other: *mut Node,
    varonleft: bool,
    negate: bool,
) -> f64 {
    let mut selec: f64;
    let mut nullfrac: f64 = 0.0;
    let mut isdefault: bool = false;

    /*
     * Grab the nullfrac for use below.
     */
    if HeapTupleIsValid((*vardata).statsTuple) {
        let stats = GETSTRUCT((*vardata).statsTuple) as Form_pg_statistic;
        nullfrac = (*stats).stanullfrac as f64;
    }

    /*
     * If we matched the var to a unique index, DISTINCT or GROUP-BY clause,
     * assume there is exactly one match regardless of anything else.
     */
    if (*vardata).isunique && !(*vardata).rel.is_null() && (*(*vardata).rel).tuples >= 1.0 {
        selec = 1.0 / (*(*vardata).rel).tuples;
    } else if HeapTupleIsValid((*vardata).statsTuple) {
        let ndistinct;
        let mut sslot: AttStatsSlot = std::mem::zeroed();

        /*
         * Search is for a value that we do not know a priori, but we will
         * assume it is not NULL.
         */
        selec = 1.0 - nullfrac;
        ndistinct = get_variable_numdistinct(vardata, &mut isdefault);
        if ndistinct > 1.0 {
            selec /= ndistinct;
        }

        /*
         * Cross-check: selectivity should never be estimated as more than the
         * most common value's.
         */
        if get_attstatsslot(
            &mut sslot,
            (*vardata).statsTuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_NUMBERS,
        ) {
            if sslot.nnumbers > 0 && selec > *sslot.numbers.add(0) as f64 {
                selec = *sslot.numbers.add(0) as f64;
            }
            free_attstatsslot(&mut sslot);
        }
    } else {
        /*
         * No ANALYZE stats available, so make a guess.
         */
        selec = 1.0 / get_variable_numdistinct(vardata, &mut isdefault);
    }

    /* now adjust if we wanted <> rather than = */
    if negate {
        selec = 1.0 - selec - nullfrac;
    }

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY!(selec);

    selec
}

/*
 *		neqsel			- Selectivity of "!=" for any data types.
 */
pub unsafe fn neqsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(eqsel_internal(fcinfo, true) as float8)
}

/*
 *	scalarineqsel		- Selectivity of "<", "<=", ">", ">=" for scalars.
 */
unsafe fn scalarineqsel(
    root: *mut PlannerInfo,
    operator: Oid,
    isgt: bool,
    iseq: bool,
    collation: Oid,
    vardata: *mut VariableStatData,
    constval: Datum,
    consttype: Oid,
) -> f64 {
    let stats: Form_pg_statistic;
    let mut opproc: FmgrInfo = std::mem::zeroed();
    let mcv_selec;
    let hist_selec;
    let mut sumcommon = 0.0;
    let mut selec;

    if !HeapTupleIsValid((*vardata).statsTuple) {
        /*
         * No stats are available.  Typically this means we have to fall back
         * on the default estimate; but if the variable is CTID then we can
         * make an estimate based on comparing the constant to the table size.
         */
        if !(*vardata).var.is_null()
            && IsA_!((*vardata).var, Var)
            && (*((*vardata).var as *mut Var)).varattno == SelfItemPointerAttributeNumber
        {
            let itemptr: ItemPointer;
            let mut block: f64;
            let density: f64;

            if (*(*vardata).rel).pages == 0.0 {
                return 1.0;
            }

            itemptr = DatumGetPointer(constval) as ItemPointer;
            block = ItemPointerGetBlockNumberNoCheck(itemptr) as f64;

            let mut density = (*(*vardata).rel).tuples / ((*(*vardata).rel).pages - 0.5);

            /* If target is the last page, use half the density. */
            if block >= (*(*vardata).rel).pages - 1.0 {
                density *= 0.5;
            }

            if density > 0.0 {
                let offset = ItemPointerGetOffsetNumberNoCheck(itemptr);
                block += Min(offset as f64 / density, 1.0);
            }

            let mut selec = block / ((*(*vardata).rel).pages - 0.5);

            if iseq == isgt && (*(*vardata).rel).tuples >= 1.0 {
                selec -= 1.0 / (*(*vardata).rel).tuples;
            }

            if isgt {
                selec = 1.0 - selec;
            }

            CLAMP_PROBABILITY!(selec);
            return selec;
        }

        /* no stats available, so default result */
        return DEFAULT_INEQ_SEL;
    }
    stats = GETSTRUCT((*vardata).statsTuple) as Form_pg_statistic;

    fmgr_info(get_opcode(operator), &mut opproc);

    /*
     * If we have most-common-values info, add up the fractions of the MCV
     * entries that satisfy MCV OP CONST.
     */
    mcv_selec = mcv_selectivity(vardata, &mut opproc, collation, constval, true, &mut sumcommon);

    /*
     * If there is a histogram, determine which bin the constant falls in.
     */
    hist_selec = ineq_histogram_selectivity(
        root,
        vardata,
        operator,
        &mut opproc,
        isgt,
        iseq,
        collation,
        constval,
        consttype,
    );

    /*
     * Now merge the results from the MCV and histogram calculations.
     */
    selec = 1.0 - (*stats).stanullfrac as f64 - sumcommon;

    if hist_selec >= 0.0 {
        selec *= hist_selec;
    } else {
        /* arbitrarily assume half of them will match */
        selec *= 0.5;
    }

    selec += mcv_selec;

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY!(selec);

    selec
}

/*
 *	mcv_selectivity			- Examine the MCV list for selectivity estimates
 */
pub unsafe fn mcv_selectivity(
    vardata: *mut VariableStatData,
    opproc: *mut FmgrInfo,
    collation: Oid,
    constval: Datum,
    varonleft: bool,
    sumcommonp: *mut f64,
) -> f64 {
    let mut mcv_selec;
    let mut sumcommon;
    let mut sslot: AttStatsSlot = std::mem::zeroed();
    let mut i: c_int;

    mcv_selec = 0.0;
    sumcommon = 0.0;

    if HeapTupleIsValid((*vardata).statsTuple)
        && statistic_proc_security_check(vardata, (*opproc).fn_oid)
        && get_attstatsslot(
            &mut sslot,
            (*vardata).statsTuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        )
    {
        LOCAL_FCINFO!(fcinfo_l, 2);

        InitFunctionCallInfoData!(fcinfo_l, opproc, 2, collation, std::ptr::null_mut(), std::ptr::null_mut());
        FC_SET_ISNULL!(fcinfo_l, 0, false);
        FC_SET_ISNULL!(fcinfo_l, 1, false);
        /* be careful to apply operator right way 'round */
        if varonleft {
            FC_SET_VALUE!(fcinfo_l, 1, constval);
        } else {
            FC_SET_VALUE!(fcinfo_l, 0, constval);
        }

        i = 0;
        while i < sslot.nvalues {
            if varonleft {
                FC_SET_VALUE!(fcinfo_l, 0, *sslot.values.add(i as usize));
            } else {
                FC_SET_VALUE!(fcinfo_l, 1, *sslot.values.add(i as usize));
            }
            (*fcinfo_l).isnull = false;
            let fresult = FunctionCallInvoke!(fcinfo_l);
            if !(*fcinfo_l).isnull && DatumGetBool(fresult) {
                mcv_selec += *sslot.numbers.add(i as usize) as f64;
            }
            sumcommon += *sslot.numbers.add(i as usize) as f64;
            i += 1;
        }
        free_attstatsslot(&mut sslot);
    }

    *sumcommonp = sumcommon;
    mcv_selec
}

/*
 *	histogram_selectivity	- Examine the histogram for selectivity estimates
 */
pub unsafe fn histogram_selectivity(
    vardata: *mut VariableStatData,
    opproc: *mut FmgrInfo,
    collation: Oid,
    constval: Datum,
    varonleft: bool,
    min_hist_size: c_int,
    n_skip: c_int,
    hist_size: *mut c_int,
) -> f64 {
    let result: f64;
    let mut sslot: AttStatsSlot = std::mem::zeroed();

    /* check sanity of parameters */
    Assert!(n_skip >= 0);
    Assert!(min_hist_size > 2 * n_skip);

    if HeapTupleIsValid((*vardata).statsTuple)
        && statistic_proc_security_check(vardata, (*opproc).fn_oid)
        && get_attstatsslot(
            &mut sslot,
            (*vardata).statsTuple,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        )
    {
        *hist_size = sslot.nvalues;
        if sslot.nvalues >= min_hist_size {
            LOCAL_FCINFO!(fcinfo_l, 2);
            let mut nmatch = 0;
            let mut i: c_int;

            InitFunctionCallInfoData!(fcinfo_l, opproc, 2, collation, std::ptr::null_mut(), std::ptr::null_mut());
            FC_SET_ISNULL!(fcinfo_l, 0, false);
            FC_SET_ISNULL!(fcinfo_l, 1, false);
            /* be careful to apply operator right way 'round */
            if varonleft {
                FC_SET_VALUE!(fcinfo_l, 1, constval);
            } else {
                FC_SET_VALUE!(fcinfo_l, 0, constval);
            }

            i = n_skip;
            while i < sslot.nvalues - n_skip {
                if varonleft {
                    FC_SET_VALUE!(fcinfo_l, 0, *sslot.values.add(i as usize));
                } else {
                    FC_SET_VALUE!(fcinfo_l, 1, *sslot.values.add(i as usize));
                }
                (*fcinfo_l).isnull = false;
                let fresult = FunctionCallInvoke!(fcinfo_l);
                if !(*fcinfo_l).isnull && DatumGetBool(fresult) {
                    nmatch += 1;
                }
                i += 1;
            }
            result = (nmatch as f64) / ((sslot.nvalues - 2 * n_skip) as f64);
        } else {
            result = -1.0;
        }
        free_attstatsslot(&mut sslot);
    } else {
        *hist_size = 0;
        result = -1.0;
    }

    result
}

/*
 *	generic_restriction_selectivity		- Selectivity for almost anything
 */
pub unsafe fn generic_restriction_selectivity(
    root: *mut PlannerInfo,
    oproid: Oid,
    collation: Oid,
    args: *mut List,
    varRelid: c_int,
    default_selectivity: f64,
) -> f64 {
    let mut selec: f64;
    let mut vardata: VariableStatData = std::mem::zeroed();
    let mut other: *mut Node = std::ptr::null_mut();
    let mut varonleft: bool = false;

    /*
     * If expression is not variable OP something or something OP variable,
     * then punt and return the default estimate.
     */
    if !get_restriction_variable(root, args, varRelid, &mut vardata, &mut other, &mut varonleft) {
        return default_selectivity;
    }

    /*
     * If the something is a NULL constant, assume operator is strict and
     * return zero.
     */
    if IsA_!(other, Const) && (*(other as *mut Const)).constisnull {
        ReleaseVariableStats!(vardata);
        return 0.0;
    }

    if IsA_!(other, Const) {
        /* Variable is being compared to a known non-null constant */
        let constval = (*(other as *mut Const)).constvalue;
        let mut opproc: FmgrInfo = std::mem::zeroed();
        let mut mcvsum = 0.0;
        let mcvsel;
        let nullfrac;
        let mut hist_size: c_int = 0;

        fmgr_info(get_opcode(oproid), &mut opproc);

        /*
         * Calculate the selectivity for the column's most common values.
         */
        mcvsel = mcv_selectivity(&mut vardata, &mut opproc, collation, constval, varonleft, &mut mcvsum);

        /*
         * If the histogram is large enough, see what fraction of it matches.
         */
        selec = histogram_selectivity(&mut vardata, &mut opproc, collation, constval, varonleft, 10, 1, &mut hist_size);
        if selec < 0.0 {
            /* Nope, fall back on default */
            selec = default_selectivity;
        } else if hist_size < 100 {
            /*
             * For histogram sizes from 10 to 100, we combine the histogram
             * and default selectivities.
             */
            let hist_weight = hist_size as f64 / 100.0;

            selec = selec * hist_weight + default_selectivity * (1.0 - hist_weight);
        }

        /* In any case, don't believe extremely small or large estimates. */
        if selec < 0.0001 {
            selec = 0.0001;
        } else if selec > 0.9999 {
            selec = 0.9999;
        }

        /* Don't forget to account for nulls. */
        if HeapTupleIsValid(vardata.statsTuple) {
            nullfrac = (*(GETSTRUCT(vardata.statsTuple) as Form_pg_statistic)).stanullfrac as f64;
        } else {
            nullfrac = 0.0;
        }

        /*
         * Now merge the results from the MCV and histogram calculations.
         */
        selec *= 1.0 - nullfrac - mcvsum;
        selec += mcvsel;
    } else {
        /* Comparison value is not constant, so we can't do anything */
        selec = default_selectivity;
    }

    ReleaseVariableStats!(vardata);

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY!(selec);

    selec
}

/*
 *	ineq_histogram_selectivity	- Examine the histogram for scalarineqsel
 */
pub unsafe fn ineq_histogram_selectivity(
    root: *mut PlannerInfo,
    vardata: *mut VariableStatData,
    opoid: Oid,
    opproc: *mut FmgrInfo,
    isgt: bool,
    iseq: bool,
    collation: Oid,
    constval: Datum,
    consttype: Oid,
) -> f64 {
    let mut hist_selec: f64;
    let mut sslot: AttStatsSlot = std::mem::zeroed();

    hist_selec = -1.0;

    if HeapTupleIsValid((*vardata).statsTuple)
        && statistic_proc_security_check(vardata, (*opproc).fn_oid)
        && get_attstatsslot(
            &mut sslot,
            (*vardata).statsTuple,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        )
    {
        if sslot.nvalues > 1
            && sslot.stacoll == collation
            && comparison_ops_are_compatible(sslot.staop, opoid)
        {
            let histfrac: f64;
            let mut lobound: c_int = 0; /* first possible slot to search */
            let mut hibound: c_int = sslot.nvalues; /* last+1 slot to search */
            let mut have_end = false;

            /*
             * If there are only two histogram entries, we'll want up-to-date
             * values for both.
             */
            if sslot.nvalues == 2 {
                have_end = get_actual_variable_range(
                    root,
                    vardata,
                    sslot.staop,
                    collation,
                    sslot.values.add(0),
                    sslot.values.add(1),
                );
            }

            while lobound < hibound {
                let probe = (lobound + hibound) / 2;
                let mut ltcmp;

                if probe == 0 && sslot.nvalues > 2 {
                    have_end = get_actual_variable_range(
                        root,
                        vardata,
                        sslot.staop,
                        collation,
                        sslot.values.add(0),
                        std::ptr::null_mut(),
                    );
                } else if probe == sslot.nvalues - 1 && sslot.nvalues > 2 {
                    have_end = get_actual_variable_range(
                        root,
                        vardata,
                        sslot.staop,
                        collation,
                        std::ptr::null_mut(),
                        sslot.values.add(probe as usize),
                    );
                }

                ltcmp = DatumGetBool(FunctionCall2Coll(
                    opproc,
                    collation,
                    *sslot.values.add(probe as usize),
                    constval,
                ));
                if isgt {
                    ltcmp = !ltcmp;
                }
                if ltcmp {
                    lobound = probe + 1;
                } else {
                    hibound = probe;
                }
            }

            if lobound <= 0 {
                /*
                 * Constant is below lower histogram boundary.
                 */
                histfrac = 0.0;
            } else if lobound >= sslot.nvalues {
                /*
                 * Inverse case: constant is above upper histogram boundary.
                 */
                histfrac = 1.0;
            } else {
                /* We have values[i-1] <= constant <= values[i]. */
                let i = lobound;
                let mut eq_selec = 0.0;
                let mut val = 0.0;
                let mut high = 0.0;
                let mut low = 0.0;
                let binfrac: f64;

                /*
                 * In the cases where we'll need it below, obtain an estimate
                 * of the selectivity of "x = constval".
                 */
                if i == 1 || isgt == iseq {
                    let mut otherdistinct;
                    let mut isdefault: bool = false;
                    let mut mcvslot: AttStatsSlot = std::mem::zeroed();

                    /* Get estimated number of distinct values */
                    otherdistinct = get_variable_numdistinct(vardata, &mut isdefault);

                    /* Subtract off the number of known MCVs */
                    if get_attstatsslot(
                        &mut mcvslot,
                        (*vardata).statsTuple,
                        STATISTIC_KIND_MCV,
                        InvalidOid,
                        ATTSTATSSLOT_NUMBERS,
                    ) {
                        otherdistinct -= mcvslot.nnumbers as f64;
                        free_attstatsslot(&mut mcvslot);
                    }

                    /* If result doesn't seem sane, leave eq_selec at 0 */
                    if otherdistinct > 1.0 {
                        eq_selec = 1.0 / otherdistinct;
                    }
                }

                /*
                 * Convert the constant and the two nearest bin boundary
                 * values to a uniform comparison scale.
                 */
                if convert_to_scalar(
                    constval,
                    consttype,
                    collation,
                    &mut val,
                    *sslot.values.add((i - 1) as usize),
                    *sslot.values.add(i as usize),
                    (*vardata).vartype,
                    &mut low,
                    &mut high,
                ) {
                    if high <= low {
                        /* cope if bin boundaries appear identical */
                        binfrac = 0.5;
                    } else if val <= low {
                        binfrac = 0.0;
                    } else if val >= high {
                        binfrac = 1.0;
                    } else {
                        let mut bf = (val - low) / (high - low);

                        /*
                         * Watch out for the possibility that we got a NaN or
                         * Infinity from the division.
                         */
                        if isnan(bf) || bf < 0.0 || bf > 1.0 {
                            bf = 0.5;
                        }
                        binfrac = bf;
                    }
                } else {
                    /*
                     * Ideally we'd produce an error here, but currently give a
                     * default estimate.
                     */
                    binfrac = 0.5;
                }

                /*
                 * Now, compute the overall selectivity across the values
                 * represented by the histogram.
                 */
                let mut histfrac_l = (i - 1) as f64 + binfrac;
                histfrac_l /= (sslot.nvalues - 1) as f64;

                /*
                 * Rescale for the first bin (i==1).
                 */
                if i == 1 {
                    histfrac_l += eq_selec * (1.0 - binfrac);
                }

                /*
                 * Decrease estimate by eq_selec for "<" or ">=".
                 */
                if isgt == iseq {
                    histfrac_l -= eq_selec;
                }

                histfrac = histfrac_l;
            }

            /*
             * Now the estimate is finished for "<" and "<=" cases.  Flip for
             * ">" or ">=".
             */
            hist_selec = if isgt { 1.0 - histfrac } else { histfrac };

            /*
             * Don't believe extremely small or large selectivity estimates
             * unless we got actual current endpoint values.
             */
            if have_end {
                CLAMP_PROBABILITY!(hist_selec);
            } else {
                let cutoff = 0.01 / ((sslot.nvalues - 1) as f64);

                if hist_selec < cutoff {
                    hist_selec = cutoff;
                } else if hist_selec > 1.0 - cutoff {
                    hist_selec = 1.0 - cutoff;
                }
            }
        } else if sslot.nvalues > 1 {
            /*
             * If we get here, we have a histogram but it's not sorted the way
             * we want.  Do a brute-force search.
             */
            LOCAL_FCINFO!(fcinfo_l, 2);
            let mut nmatch = 0;

            InitFunctionCallInfoData!(fcinfo_l, opproc, 2, collation, std::ptr::null_mut(), std::ptr::null_mut());
            FC_SET_ISNULL!(fcinfo_l, 0, false);
            FC_SET_ISNULL!(fcinfo_l, 1, false);
            FC_SET_VALUE!(fcinfo_l, 1, constval);
            let mut i: c_int = 0;
            while i < sslot.nvalues {
                FC_SET_VALUE!(fcinfo_l, 0, *sslot.values.add(i as usize));
                (*fcinfo_l).isnull = false;
                let fresult = FunctionCallInvoke!(fcinfo_l);
                if !(*fcinfo_l).isnull && DatumGetBool(fresult) {
                    nmatch += 1;
                }
                i += 1;
            }
            hist_selec = (nmatch as f64) / (sslot.nvalues as f64);

            /*
             * As above, clamp to a hundredth of the histogram resolution.
             */
            {
                let cutoff = 0.01 / ((sslot.nvalues - 1) as f64);

                if hist_selec < cutoff {
                    hist_selec = cutoff;
                } else if hist_selec > 1.0 - cutoff {
                    hist_selec = 1.0 - cutoff;
                }
            }
        }

        free_attstatsslot(&mut sslot);
    }

    hist_selec
}

/*
 * Common wrapper function for the selectivity estimators that simply
 * invoke scalarineqsel().
 */
unsafe fn scalarineqsel_wrapper(fcinfo: FunctionCallInfo, isgt_in: bool, iseq: bool) -> Datum {
    let root = PG_GETARG_POINTER!(fcinfo, 0) as *mut PlannerInfo;
    let mut operator = PG_GETARG_OID!(fcinfo, 1);
    let args = PG_GETARG_POINTER!(fcinfo, 2) as *mut List;
    let varRelid = PG_GETARG_INT32!(fcinfo, 3);
    let collation = PG_GET_COLLATION!(fcinfo);
    let mut vardata: VariableStatData = std::mem::zeroed();
    let mut other: *mut Node = std::ptr::null_mut();
    let mut varonleft: bool = false;
    let constval: Datum;
    let consttype: Oid;
    let selec: f64;
    let mut isgt = isgt_in;

    /*
     * If expression is not variable op something or something op variable,
     * then punt and return a default estimate.
     */
    if !get_restriction_variable(root, args, varRelid, &mut vardata, &mut other, &mut varonleft) {
        PG_RETURN_FLOAT8!(DEFAULT_INEQ_SEL);
    }

    /*
     * Can't do anything useful if the something is not a constant, either.
     */
    if !IsA_!(other, Const) {
        ReleaseVariableStats!(vardata);
        PG_RETURN_FLOAT8!(DEFAULT_INEQ_SEL);
    }

    /*
     * If the constant is NULL, assume operator is strict and return zero.
     */
    if (*(other as *mut Const)).constisnull {
        ReleaseVariableStats!(vardata);
        PG_RETURN_FLOAT8!(0.0);
    }
    constval = (*(other as *mut Const)).constvalue;
    consttype = (*(other as *mut Const)).consttype;

    /*
     * Force the var to be on the left to simplify logic in scalarineqsel.
     */
    if !varonleft {
        operator = get_commutator(operator);
        if operator == InvalidOid {
            ReleaseVariableStats!(vardata);
            PG_RETURN_FLOAT8!(DEFAULT_INEQ_SEL);
        }
        isgt = !isgt;
    }

    /* The rest of the work is done by scalarineqsel(). */
    selec = scalarineqsel(root, operator, isgt, iseq, collation, &mut vardata, constval, consttype);

    ReleaseVariableStats!(vardata);

    PG_RETURN_FLOAT8!(selec as float8)
}

/*
 *		scalarltsel		- Selectivity of "<" for scalars.
 */
pub unsafe fn scalarltsel(fcinfo: FunctionCallInfo) -> Datum {
    scalarineqsel_wrapper(fcinfo, false, false)
}

/*
 *		scalarlesel		- Selectivity of "<=" for scalars.
 */
pub unsafe fn scalarlesel(fcinfo: FunctionCallInfo) -> Datum {
    scalarineqsel_wrapper(fcinfo, false, true)
}

/*
 *		scalargtsel		- Selectivity of ">" for scalars.
 */
pub unsafe fn scalargtsel(fcinfo: FunctionCallInfo) -> Datum {
    scalarineqsel_wrapper(fcinfo, true, false)
}

/*
 *		scalargesel		- Selectivity of ">=" for scalars.
 */
pub unsafe fn scalargesel(fcinfo: FunctionCallInfo) -> Datum {
    scalarineqsel_wrapper(fcinfo, true, true)
}

/*
 *		boolvarsel		- Selectivity of Boolean variable.
 */
pub unsafe fn boolvarsel(root: *mut PlannerInfo, arg: *mut Node, varRelid: c_int) -> Selectivity {
    let mut vardata: VariableStatData = std::mem::zeroed();
    let selec: f64;

    examine_variable(root, arg, varRelid, &mut vardata);
    if HeapTupleIsValid(vardata.statsTuple) {
        /*
         * A boolean variable V is equivalent to the clause V = 't'.
         */
        selec = var_eq_const(
            &mut vardata,
            BooleanEqualOperator,
            InvalidOid,
            BoolGetDatum(true),
            false,
            true,
            false,
        );
    } else {
        /* Otherwise, the default estimate is 0.5 */
        selec = 0.5;
    }
    ReleaseVariableStats!(vardata);
    selec
}

/*
 *		booltestsel		- Selectivity of BooleanTest Node.
 */
pub unsafe fn booltestsel(
    root: *mut PlannerInfo,
    booltesttype: BoolTestType,
    arg: *mut Node,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    let mut vardata: VariableStatData = std::mem::zeroed();
    let mut selec: f64;

    examine_variable(root, arg, varRelid, &mut vardata);

    if HeapTupleIsValid(vardata.statsTuple) {
        let stats: Form_pg_statistic;
        let freq_null: f64;
        let mut sslot: AttStatsSlot = std::mem::zeroed();

        stats = GETSTRUCT(vardata.statsTuple) as Form_pg_statistic;
        freq_null = (*stats).stanullfrac as f64;

        if get_attstatsslot(
            &mut sslot,
            vardata.statsTuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        ) && sslot.nnumbers > 0
        {
            let freq_true: f64;
            let freq_false: f64;

            /*
             * Get first MCV frequency and derive frequency for true.
             */
            if DatumGetBool(*sslot.values.add(0)) {
                freq_true = *sslot.numbers.add(0) as f64;
            } else {
                freq_true = 1.0 - *sslot.numbers.add(0) as f64 - freq_null;
            }

            /*
             * Next derive frequency for false.
             */
            freq_false = 1.0 - freq_true - freq_null;

            match booltesttype {
                IS_UNKNOWN => {
                    /* select only NULL values */
                    selec = freq_null;
                }
                IS_NOT_UNKNOWN => {
                    /* select non-NULL values */
                    selec = 1.0 - freq_null;
                }
                IS_TRUE => {
                    /* select only TRUE values */
                    selec = freq_true;
                }
                IS_NOT_TRUE => {
                    /* select non-TRUE values */
                    selec = 1.0 - freq_true;
                }
                IS_FALSE => {
                    /* select only FALSE values */
                    selec = freq_false;
                }
                IS_NOT_FALSE => {
                    /* select non-FALSE values */
                    selec = 1.0 - freq_false;
                }
                #[allow(unreachable_patterns)]
                _ => {
                    elog!(ERROR, "unrecognized booltesttype: {}", booltesttype as c_int);
                    selec = 0.0; /* Keep compiler quiet */
                }
            }

            free_attstatsslot(&mut sslot);
        } else {
            /*
             * No most-common-value info available.
             */
            match booltesttype {
                IS_UNKNOWN => {
                    selec = freq_null;
                }
                IS_NOT_UNKNOWN => {
                    selec = 1.0 - freq_null;
                }
                IS_TRUE | IS_FALSE => {
                    /* Assume we select half of the non-NULL values */
                    selec = (1.0 - freq_null) / 2.0;
                }
                IS_NOT_TRUE | IS_NOT_FALSE => {
                    /* Assume we select NULLs plus half of the non-NULLs */
                    selec = (freq_null + 1.0) / 2.0;
                }
                #[allow(unreachable_patterns)]
                _ => {
                    elog!(ERROR, "unrecognized booltesttype: {}", booltesttype as c_int);
                    selec = 0.0; /* Keep compiler quiet */
                }
            }
        }
    } else {
        /*
         * If we can't get variable statistics, perhaps clause_selectivity can
         * do something with it.
         */
        match booltesttype {
            IS_UNKNOWN => {
                selec = DEFAULT_UNK_SEL;
            }
            IS_NOT_UNKNOWN => {
                selec = DEFAULT_NOT_UNK_SEL;
            }
            IS_TRUE | IS_NOT_FALSE => {
                selec = clause_selectivity(root, arg, varRelid, jointype, sjinfo) as f64;
            }
            IS_FALSE | IS_NOT_TRUE => {
                selec = 1.0 - clause_selectivity(root, arg, varRelid, jointype, sjinfo) as f64;
            }
            #[allow(unreachable_patterns)]
            _ => {
                elog!(ERROR, "unrecognized booltesttype: {}", booltesttype as c_int);
                selec = 0.0; /* Keep compiler quiet */
            }
        }
    }

    ReleaseVariableStats!(vardata);

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY!(selec);

    selec as Selectivity
}

/*
 *		nulltestsel		- Selectivity of NullTest Node.
 */
pub unsafe fn nulltestsel(
    root: *mut PlannerInfo,
    nulltesttype: NullTestType,
    arg: *mut Node,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    let mut vardata: VariableStatData = std::mem::zeroed();
    let mut selec: f64;

    examine_variable(root, arg, varRelid, &mut vardata);

    if HeapTupleIsValid(vardata.statsTuple) {
        let stats: Form_pg_statistic;
        let freq_null: f64;

        stats = GETSTRUCT(vardata.statsTuple) as Form_pg_statistic;
        freq_null = (*stats).stanullfrac as f64;

        match nulltesttype {
            IS_NULL => {
                /* Use freq_null directly. */
                selec = freq_null;
            }
            IS_NOT_NULL => {
                /* Select not unknown (not null) values. */
                selec = 1.0 - freq_null;
            }
            #[allow(unreachable_patterns)]
            _ => {
                elog!(ERROR, "unrecognized nulltesttype: {}", nulltesttype as c_int);
                ReleaseVariableStats!(vardata);
                return 0 as Selectivity; /* keep compiler quiet */
            }
        }
    } else if !vardata.var.is_null()
        && IsA_!(vardata.var, Var)
        && (*(vardata.var as *mut Var)).varattno < 0
    {
        /*
         * There are no stats for system columns, but we know they are never
         * NULL.
         */
        selec = if matches!(nulltesttype, IS_NULL) { 0.0 } else { 1.0 };
    } else {
        /*
         * No ANALYZE stats available, so make a guess
         */
        match nulltesttype {
            IS_NULL => {
                selec = DEFAULT_UNK_SEL;
            }
            IS_NOT_NULL => {
                selec = DEFAULT_NOT_UNK_SEL;
            }
            #[allow(unreachable_patterns)]
            _ => {
                elog!(ERROR, "unrecognized nulltesttype: {}", nulltesttype as c_int);
                ReleaseVariableStats!(vardata);
                return 0 as Selectivity; /* keep compiler quiet */
            }
        }
    }

    ReleaseVariableStats!(vardata);

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY!(selec);

    selec as Selectivity
}

/*
 * strip_array_coercion - strip binary-compatible relabeling from an array expr
 */
unsafe fn strip_array_coercion(mut node: *mut Node) -> *mut Node {
    loop {
        if !node.is_null() && IsA_!(node, ArrayCoerceExpr) {
            let acoerce = node as *mut ArrayCoerceExpr;

            /*
             * If the per-element expression is just a RelabelType on top of
             * CaseTestExpr, then we know it's a binary-compatible relabeling.
             */
            if IsA_!((*acoerce).elemexpr, RelabelType)
                && IsA_!((*((*acoerce).elemexpr as *mut RelabelType)).arg, CaseTestExpr)
            {
                node = (*acoerce).arg as *mut Node;
            } else {
                break;
            }
        } else if !node.is_null() && IsA_!(node, RelabelType) {
            /* We don't really expect this case, but may as well cope */
            node = (*(node as *mut RelabelType)).arg as *mut Node;
        } else {
            break;
        }
    }
    node
}

/*
 *		scalararraysel		- Selectivity of ScalarArrayOpExpr Node.
 */
pub unsafe fn scalararraysel(
    root: *mut PlannerInfo,
    clause: *mut ScalarArrayOpExpr,
    is_join_clause: bool,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    let operator = (*clause).opno;
    let useOr = (*clause).useOr;
    let mut isEquality = false;
    let mut isInequality = false;
    let mut leftop: *mut Node;
    let mut rightop: *mut Node;
    let nominal_element_type: Oid;
    let nominal_element_collation: Oid;
    let typentry: *mut TypeCacheEntry;
    let oprsel: RegProcedure;
    let mut oprselproc: FmgrInfo = std::mem::zeroed();
    let mut s1: Selectivity;
    let mut s1disjoint: Selectivity;

    /* First, deconstruct the expression */
    Assert!(list_length((*clause).args) == 2);
    leftop = linitial((*clause).args) as *mut Node;
    rightop = lsecond((*clause).args) as *mut Node;

    /* aggressively reduce both sides to constants */
    leftop = estimate_expression_value(root, leftop);
    rightop = estimate_expression_value(root, rightop);

    /* get nominal (after relabeling) element type of rightop */
    nominal_element_type = get_base_element_type(exprType(rightop));
    if !OidIsValid(nominal_element_type) {
        return 0.5 as Selectivity; /* probably shouldn't happen */
    }
    /* get nominal collation, too, for generating constants */
    nominal_element_collation = exprCollation(rightop);

    /* look through any binary-compatible relabeling of rightop */
    rightop = strip_array_coercion(rightop);

    /*
     * Detect whether the operator is the default equality or inequality
     * operator of the array element type.
     */
    typentry = lookup_type_cache(nominal_element_type, TYPECACHE_EQ_OPR);
    if OidIsValid((*typentry).eq_opr) {
        if operator == (*typentry).eq_opr {
            isEquality = true;
        } else if get_negator(operator) == (*typentry).eq_opr {
            isInequality = true;
        }
    }

    /*
     * If it is equality or inequality, we might be able to estimate this as a
     * form of array containment.
     */
    if (isEquality || isInequality) && !is_join_clause {
        s1 = scalararraysel_containment(
            root,
            leftop,
            rightop,
            nominal_element_type,
            isEquality,
            useOr,
            varRelid,
        );
        if s1 >= 0.0 {
            return s1;
        }
    }

    /*
     * Look up the underlying operator's selectivity estimator.
     */
    if is_join_clause {
        oprsel = get_oprjoin(operator);
    } else {
        oprsel = get_oprrest(operator);
    }
    if oprsel == 0 {
        return 0.5 as Selectivity;
    }
    fmgr_info(oprsel, &mut oprselproc);

    /*
     * Also believe that any operators using eqsel()/neqsel() act like
     * equality or inequality.
     */
    if oprsel == F_EQSEL || oprsel == F_EQJOINSEL {
        isEquality = true;
    } else if oprsel == F_NEQSEL || oprsel == F_NEQJOINSEL {
        isInequality = true;
    }

    /*
     * We consider three cases.
     */
    if !rightop.is_null() && IsA_!(rightop, Const) {
        let arraydatum = (*(rightop as *mut Const)).constvalue;
        let arrayisnull = (*(rightop as *mut Const)).constisnull;
        let arrayval: *mut ArrayType;
        let mut elmlen: int16 = 0;
        let mut elmbyval: bool = false;
        let mut elmalign: c_char = 0;
        let mut num_elems: c_int = 0;
        let mut elem_values: *mut Datum = std::ptr::null_mut();
        let mut elem_nulls: *mut bool = std::ptr::null_mut();
        let mut i: c_int;

        if arrayisnull {
            /* qual can't succeed if null array */
            return 0.0 as Selectivity;
        }
        arrayval = DatumGetArrayTypeP(arraydatum);
        get_typlenbyvalalign(
            ARR_ELEMTYPE(arrayval),
            &mut elmlen,
            &mut elmbyval,
            &mut elmalign,
        );
        deconstruct_array(
            arrayval,
            ARR_ELEMTYPE(arrayval),
            elmlen,
            elmbyval,
            elmalign,
            &mut elem_values,
            &mut elem_nulls,
            &mut num_elems,
        );

        s1 = if useOr { 0.0 } else { 1.0 };
        s1disjoint = s1;

        i = 0;
        while i < num_elems {
            let args: *mut List;
            let s2: Selectivity;

            args = list_make2!(
                leftop as *mut c_void,
                makeConst(
                    nominal_element_type,
                    -1,
                    nominal_element_collation,
                    elmlen as c_int,
                    *elem_values.add(i as usize),
                    *elem_nulls.add(i as usize),
                    elmbyval,
                ) as *mut c_void
            );
            if is_join_clause {
                s2 = DatumGetFloat8(FunctionCall5Coll(
                    &mut oprselproc,
                    (*clause).inputcollid,
                    PointerGetDatum(root as *const c_void),
                    ObjectIdGetDatum(operator),
                    PointerGetDatum(args as *const c_void),
                    Int16GetDatum(jointype as int16),
                    PointerGetDatum(sjinfo as *const c_void),
                ));
            } else {
                s2 = DatumGetFloat8(FunctionCall4Coll(
                    &mut oprselproc,
                    (*clause).inputcollid,
                    PointerGetDatum(root as *const c_void),
                    ObjectIdGetDatum(operator),
                    PointerGetDatum(args as *const c_void),
                    Int32GetDatum(varRelid),
                ));
            }

            if useOr {
                s1 = s1 + s2 - s1 * s2;
                if isEquality {
                    s1disjoint += s2;
                }
            } else {
                s1 = s1 * s2;
                if isInequality {
                    s1disjoint += s2 - 1.0;
                }
            }
            i += 1;
        }

        /* accept disjoint-probability estimate if in range */
        if (if useOr { isEquality } else { isInequality })
            && s1disjoint >= 0.0
            && s1disjoint <= 1.0
        {
            s1 = s1disjoint;
        }
    } else if !rightop.is_null()
        && IsA_!(rightop, ArrayExpr)
        && !(*(rightop as *mut ArrayExpr)).multidims
    {
        let arrayexpr = rightop as *mut ArrayExpr;
        let mut elmlen: int16 = 0;
        let mut elmbyval: bool = false;

        get_typlenbyval((*arrayexpr).element_typeid, &mut elmlen, &mut elmbyval);

        s1 = if useOr { 0.0 } else { 1.0 };
        s1disjoint = s1;

        foreach!(l, (*arrayexpr).elements, {
            let elem = lfirst(current_cell!(l)) as *mut Node;
            let args: *mut List;
            let s2: Selectivity;

            args = list_make2!(leftop as *mut c_void, elem as *mut c_void);
            if is_join_clause {
                s2 = DatumGetFloat8(FunctionCall5Coll(
                    &mut oprselproc,
                    (*clause).inputcollid,
                    PointerGetDatum(root as *const c_void),
                    ObjectIdGetDatum(operator),
                    PointerGetDatum(args as *const c_void),
                    Int16GetDatum(jointype as int16),
                    PointerGetDatum(sjinfo as *const c_void),
                ));
            } else {
                s2 = DatumGetFloat8(FunctionCall4Coll(
                    &mut oprselproc,
                    (*clause).inputcollid,
                    PointerGetDatum(root as *const c_void),
                    ObjectIdGetDatum(operator),
                    PointerGetDatum(args as *const c_void),
                    Int32GetDatum(varRelid),
                ));
            }

            if useOr {
                s1 = s1 + s2 - s1 * s2;
                if isEquality {
                    s1disjoint += s2;
                }
            } else {
                s1 = s1 * s2;
                if isInequality {
                    s1disjoint += s2 - 1.0;
                }
            }
        });

        /* accept disjoint-probability estimate if in range */
        if (if useOr { isEquality } else { isInequality })
            && s1disjoint >= 0.0
            && s1disjoint <= 1.0
        {
            s1 = s1disjoint;
        }
    } else {
        let dummyexpr: *mut CaseTestExpr;
        let args: *mut List;
        let s2: Selectivity;
        let mut i: c_int;

        /*
         * We need a dummy rightop to pass to the operator selectivity routine.
         */
        dummyexpr = makeNode_!(CaseTestExpr);
        (*dummyexpr).typeId = nominal_element_type;
        (*dummyexpr).typeMod = -1;
        (*dummyexpr).collation = (*clause).inputcollid;
        args = list_make2!(leftop as *mut c_void, dummyexpr as *mut c_void);
        if is_join_clause {
            s2 = DatumGetFloat8(FunctionCall5Coll(
                &mut oprselproc,
                (*clause).inputcollid,
                PointerGetDatum(root as *const c_void),
                ObjectIdGetDatum(operator),
                PointerGetDatum(args as *const c_void),
                Int16GetDatum(jointype as int16),
                PointerGetDatum(sjinfo as *const c_void),
            ));
        } else {
            s2 = DatumGetFloat8(FunctionCall4Coll(
                &mut oprselproc,
                (*clause).inputcollid,
                PointerGetDatum(root as *const c_void),
                ObjectIdGetDatum(operator),
                PointerGetDatum(args as *const c_void),
                Int32GetDatum(varRelid),
            ));
        }
        s1 = if useOr { 0.0 } else { 1.0 };

        /*
         * Arbitrarily assume 10 elements in the eventual array value.
         */
        i = 0;
        while i < 10 {
            if useOr {
                s1 = s1 + s2 - s1 * s2;
            } else {
                s1 = s1 * s2;
            }
            i += 1;
        }
    }

    /* result should be in range, but make sure... */
    CLAMP_PROBABILITY!(s1);

    s1
}

/*
 * Estimate number of elements in the array yielded by an expression.
 */
pub unsafe fn estimate_array_length(root: *mut PlannerInfo, mut arrayexpr: *mut Node) -> f64 {
    /* look through any binary-compatible relabeling of arrayexpr */
    arrayexpr = strip_array_coercion(arrayexpr);

    if !arrayexpr.is_null() && IsA_!(arrayexpr, Const) {
        let arraydatum = (*(arrayexpr as *mut Const)).constvalue;
        let arrayisnull = (*(arrayexpr as *mut Const)).constisnull;
        let arrayval: *mut ArrayType;

        if arrayisnull {
            return 0.0;
        }
        arrayval = DatumGetArrayTypeP(arraydatum);
        return ArrayGetNItems(ARR_NDIM(arrayval), ARR_DIMS(arrayval)) as f64;
    } else if !arrayexpr.is_null()
        && IsA_!(arrayexpr, ArrayExpr)
        && !(*(arrayexpr as *mut ArrayExpr)).multidims
    {
        return list_length((*(arrayexpr as *mut ArrayExpr)).elements) as f64;
    } else if !arrayexpr.is_null() && !root.is_null() {
        /* See if we can find any statistics about it */
        let mut vardata: VariableStatData = std::mem::zeroed();
        let mut sslot: AttStatsSlot = std::mem::zeroed();
        let mut nelem = 0.0;

        examine_variable(root, arrayexpr, 0, &mut vardata);
        if HeapTupleIsValid(vardata.statsTuple) {
            /*
             * Found stats, so use the average element count.
             */
            if get_attstatsslot(
                &mut sslot,
                vardata.statsTuple,
                STATISTIC_KIND_DECHIST,
                InvalidOid,
                ATTSTATSSLOT_NUMBERS,
            ) {
                if sslot.nnumbers > 0 {
                    nelem = clamp_row_est(*sslot.numbers.add((sslot.nnumbers - 1) as usize) as f64);
                }
                free_attstatsslot(&mut sslot);
            }
        }
        ReleaseVariableStats!(vardata);

        if nelem > 0.0 {
            return nelem;
        }
    }

    /* Else use a default guess --- this should match scalararraysel */
    10.0
}

/*
 *		rowcomparesel		- Selectivity of RowCompareExpr Node.
 */
pub unsafe fn rowcomparesel(
    root: *mut PlannerInfo,
    clause: *mut RowCompareExpr,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
) -> Selectivity {
    let s1: Selectivity;
    let opno = linitial_oid((*clause).opnos);
    let inputcollid = linitial_oid((*clause).inputcollids);
    let opargs: *mut List;
    let is_join_clause: bool;

    /* Build equivalent arg list for single operator */
    opargs = list_make2!(
        linitial((*clause).largs),
        linitial((*clause).rargs)
    );

    /*
     * Decide if it's a join clause.
     */
    if varRelid != 0 {
        is_join_clause = false;
    } else if sjinfo.is_null() {
        is_join_clause = false;
    } else {
        is_join_clause = NumRelids(root, opargs as *mut Node) > 1;
    }

    if is_join_clause {
        /* Estimate selectivity for a join clause. */
        s1 = join_selectivity(root, opno, opargs, inputcollid, jointype, sjinfo);
    } else {
        /* Estimate selectivity for a restriction clause. */
        s1 = restriction_selectivity(root, opno, opargs, inputcollid, varRelid);
    }

    s1
}

/*
 *		eqjoinsel		- Join selectivity of "="
 */
pub unsafe fn eqjoinsel(fcinfo: FunctionCallInfo) -> Datum {
    let root = PG_GETARG_POINTER!(fcinfo, 0) as *mut PlannerInfo;
    let operator = PG_GETARG_OID!(fcinfo, 1);
    let args = PG_GETARG_POINTER!(fcinfo, 2) as *mut List;
    // #ifdef NOT_USED: jointype = PG_GETARG_INT16(3)
    let sjinfo = PG_GETARG_POINTER!(fcinfo, 4) as *mut SpecialJoinInfo;
    let collation = PG_GET_COLLATION!(fcinfo);
    let mut selec: f64;
    let selec_inner: f64;
    let mut vardata1: VariableStatData = std::mem::zeroed();
    let mut vardata2: VariableStatData = std::mem::zeroed();
    let nd1: f64;
    let nd2: f64;
    let mut isdefault1: bool = false;
    let mut isdefault2: bool = false;
    let opfuncoid: Oid;
    let mut sslot1: AttStatsSlot = std::mem::zeroed();
    let mut sslot2: AttStatsSlot = std::mem::zeroed();
    let mut stats1: Form_pg_statistic = std::ptr::null_mut();
    let mut stats2: Form_pg_statistic = std::ptr::null_mut();
    let mut have_mcvs1 = false;
    let mut have_mcvs2 = false;
    let get_mcv_stats: bool;
    let mut join_is_reversed: bool = false;
    let inner_rel: *mut RelOptInfo;

    get_join_variables(
        root,
        args,
        sjinfo,
        &mut vardata1,
        &mut vardata2,
        &mut join_is_reversed,
    );

    nd1 = get_variable_numdistinct(&mut vardata1, &mut isdefault1);
    nd2 = get_variable_numdistinct(&mut vardata2, &mut isdefault2);

    opfuncoid = get_opcode(operator);

    std::ptr::write_bytes(&mut sslot1 as *mut AttStatsSlot, 0, 1);
    std::ptr::write_bytes(&mut sslot2 as *mut AttStatsSlot, 0, 1);

    /*
     * There is no use in fetching one side's MCVs if we lack MCVs for the
     * other side, so do a quick check to verify that both stats exist.
     */
    get_mcv_stats = HeapTupleIsValid(vardata1.statsTuple)
        && HeapTupleIsValid(vardata2.statsTuple)
        && get_attstatsslot(
            &mut sslot1,
            vardata1.statsTuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            0,
        )
        && get_attstatsslot(
            &mut sslot2,
            vardata2.statsTuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            0,
        );

    if HeapTupleIsValid(vardata1.statsTuple) {
        /* note we allow use of nullfrac regardless of security check */
        stats1 = GETSTRUCT(vardata1.statsTuple) as Form_pg_statistic;
        if get_mcv_stats && statistic_proc_security_check(&mut vardata1, opfuncoid) {
            have_mcvs1 = get_attstatsslot(
                &mut sslot1,
                vardata1.statsTuple,
                STATISTIC_KIND_MCV,
                InvalidOid,
                ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
            );
        }
    }

    if HeapTupleIsValid(vardata2.statsTuple) {
        /* note we allow use of nullfrac regardless of security check */
        stats2 = GETSTRUCT(vardata2.statsTuple) as Form_pg_statistic;
        if get_mcv_stats && statistic_proc_security_check(&mut vardata2, opfuncoid) {
            have_mcvs2 = get_attstatsslot(
                &mut sslot2,
                vardata2.statsTuple,
                STATISTIC_KIND_MCV,
                InvalidOid,
                ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
            );
        }
    }

    /* We need to compute the inner-join selectivity in all cases */
    selec_inner = eqjoinsel_inner(
        opfuncoid,
        collation,
        &mut vardata1,
        &mut vardata2,
        nd1,
        nd2,
        isdefault1,
        isdefault2,
        &mut sslot1,
        &mut sslot2,
        stats1,
        stats2,
        have_mcvs1,
        have_mcvs2,
    );

    match (*sjinfo).jointype {
        JOIN_INNER | JOIN_LEFT | JOIN_FULL => {
            selec = selec_inner;
        }
        JOIN_SEMI | JOIN_ANTI => {
            /*
             * Look up the join's inner relation.
             */
            inner_rel = find_join_input_rel(root, (*sjinfo).min_righthand);

            if !join_is_reversed {
                selec = eqjoinsel_semi(
                    opfuncoid,
                    collation,
                    &mut vardata1,
                    &mut vardata2,
                    nd1,
                    nd2,
                    isdefault1,
                    isdefault2,
                    &mut sslot1,
                    &mut sslot2,
                    stats1,
                    stats2,
                    have_mcvs1,
                    have_mcvs2,
                    inner_rel,
                );
            } else {
                let commop = get_commutator(operator);
                let commopfuncoid = if OidIsValid(commop) {
                    get_opcode(commop)
                } else {
                    InvalidOid
                };

                selec = eqjoinsel_semi(
                    commopfuncoid,
                    collation,
                    &mut vardata2,
                    &mut vardata1,
                    nd2,
                    nd1,
                    isdefault2,
                    isdefault1,
                    &mut sslot2,
                    &mut sslot1,
                    stats2,
                    stats1,
                    have_mcvs2,
                    have_mcvs1,
                    inner_rel,
                );
            }

            /*
             * Clamp Ssemi <= N2 * Sinner.
             */
            selec = Min(selec, (*inner_rel).rows * selec_inner);
        }
        #[allow(unreachable_patterns)]
        _ => {
            /* other values not expected here */
            elog!(ERROR, "unrecognized join type: {}", (*sjinfo).jointype as c_int);
            selec = 0.0; /* keep compiler quiet */
        }
    }

    free_attstatsslot(&mut sslot1);
    free_attstatsslot(&mut sslot2);

    ReleaseVariableStats!(vardata1);
    ReleaseVariableStats!(vardata2);

    CLAMP_PROBABILITY!(selec);

    PG_RETURN_FLOAT8!(selec as float8)
}

/*
 * eqjoinsel_inner --- eqjoinsel for normal inner join
 */
unsafe fn eqjoinsel_inner(
    opfuncoid: Oid,
    collation: Oid,
    vardata1: *mut VariableStatData,
    vardata2: *mut VariableStatData,
    nd1: f64,
    nd2: f64,
    isdefault1: bool,
    isdefault2: bool,
    sslot1: *mut AttStatsSlot,
    sslot2: *mut AttStatsSlot,
    stats1: Form_pg_statistic,
    stats2: Form_pg_statistic,
    have_mcvs1: bool,
    have_mcvs2: bool,
) -> f64 {
    let selec: f64;

    if have_mcvs1 && have_mcvs2 {
        /*
         * We have most-common-value lists for both relations.
         */
        LOCAL_FCINFO!(fcinfo_l, 2);
        let mut eqproc: FmgrInfo = std::mem::zeroed();
        let hasmatch1: *mut bool;
        let hasmatch2: *mut bool;
        let nullfrac1 = (*stats1).stanullfrac as f64;
        let nullfrac2 = (*stats2).stanullfrac as f64;
        let mut matchprodfreq;
        let mut matchfreq1;
        let mut matchfreq2;
        let mut unmatchfreq1;
        let mut unmatchfreq2;
        let mut otherfreq1;
        let mut otherfreq2;
        let mut totalsel1;
        let mut totalsel2;
        let mut i: c_int;
        let mut nmatches: c_int;

        fmgr_info(opfuncoid, &mut eqproc);

        InitFunctionCallInfoData!(fcinfo_l, &mut eqproc, 2, collation, std::ptr::null_mut(), std::ptr::null_mut());
        FC_SET_ISNULL!(fcinfo_l, 0, false);
        FC_SET_ISNULL!(fcinfo_l, 1, false);

        hasmatch1 = palloc0(((*sslot1).nvalues as usize) * std::mem::size_of::<bool>()) as *mut bool;
        hasmatch2 = palloc0(((*sslot2).nvalues as usize) * std::mem::size_of::<bool>()) as *mut bool;

        matchprodfreq = 0.0;
        nmatches = 0;
        i = 0;
        while i < (*sslot1).nvalues {
            let mut j: c_int;

            FC_SET_VALUE!(fcinfo_l, 0, *(*sslot1).values.add(i as usize));

            j = 0;
            while j < (*sslot2).nvalues {
                if *hasmatch2.add(j as usize) {
                    j += 1;
                    continue;
                }
                FC_SET_VALUE!(fcinfo_l, 1, *(*sslot2).values.add(j as usize));
                (*fcinfo_l).isnull = false;
                let fresult = FunctionCallInvoke!(fcinfo_l);
                if !(*fcinfo_l).isnull && DatumGetBool(fresult) {
                    *hasmatch1.add(i as usize) = true;
                    *hasmatch2.add(j as usize) = true;
                    matchprodfreq += *(*sslot1).numbers.add(i as usize) as f64
                        * *(*sslot2).numbers.add(j as usize) as f64;
                    nmatches += 1;
                    break;
                }
                j += 1;
            }
            i += 1;
        }
        CLAMP_PROBABILITY!(matchprodfreq);
        /* Sum up frequencies of matched and unmatched MCVs */
        matchfreq1 = 0.0;
        unmatchfreq1 = 0.0;
        i = 0;
        while i < (*sslot1).nvalues {
            if *hasmatch1.add(i as usize) {
                matchfreq1 += *(*sslot1).numbers.add(i as usize) as f64;
            } else {
                unmatchfreq1 += *(*sslot1).numbers.add(i as usize) as f64;
            }
            i += 1;
        }
        CLAMP_PROBABILITY!(matchfreq1);
        CLAMP_PROBABILITY!(unmatchfreq1);
        matchfreq2 = 0.0;
        unmatchfreq2 = 0.0;
        i = 0;
        while i < (*sslot2).nvalues {
            if *hasmatch2.add(i as usize) {
                matchfreq2 += *(*sslot2).numbers.add(i as usize) as f64;
            } else {
                unmatchfreq2 += *(*sslot2).numbers.add(i as usize) as f64;
            }
            i += 1;
        }
        CLAMP_PROBABILITY!(matchfreq2);
        CLAMP_PROBABILITY!(unmatchfreq2);
        pfree(hasmatch1 as *mut c_void);
        pfree(hasmatch2 as *mut c_void);

        /*
         * Compute total frequency of non-null values that are not in the MCV
         * lists.
         */
        otherfreq1 = 1.0 - nullfrac1 - matchfreq1 - unmatchfreq1;
        otherfreq2 = 1.0 - nullfrac2 - matchfreq2 - unmatchfreq2;
        CLAMP_PROBABILITY!(otherfreq1);
        CLAMP_PROBABILITY!(otherfreq2);

        /*
         * We can estimate the total selectivity from the point of view of
         * relation 1.
         */
        totalsel1 = matchprodfreq;
        if nd2 > (*sslot2).nvalues as f64 {
            totalsel1 += unmatchfreq1 * otherfreq2 / (nd2 - (*sslot2).nvalues as f64);
        }
        if nd2 > nmatches as f64 {
            totalsel1 += otherfreq1 * (otherfreq2 + unmatchfreq2) / (nd2 - nmatches as f64);
        }
        /* Same estimate from the point of view of relation 2. */
        totalsel2 = matchprodfreq;
        if nd1 > (*sslot1).nvalues as f64 {
            totalsel2 += unmatchfreq2 * otherfreq1 / (nd1 - (*sslot1).nvalues as f64);
        }
        if nd1 > nmatches as f64 {
            totalsel2 += otherfreq2 * (otherfreq1 + unmatchfreq1) / (nd1 - nmatches as f64);
        }

        /*
         * Use the smaller of the two estimates.
         */
        selec = if totalsel1 < totalsel2 {
            totalsel1
        } else {
            totalsel2
        };
    } else {
        /*
         * We do not have MCV lists for both sides.
         */
        let nullfrac1 = if !stats1.is_null() {
            (*stats1).stanullfrac as f64
        } else {
            0.0
        };
        let nullfrac2 = if !stats2.is_null() {
            (*stats2).stanullfrac as f64
        } else {
            0.0
        };

        let mut s = (1.0 - nullfrac1) * (1.0 - nullfrac2);
        if nd1 > nd2 {
            s /= nd1;
        } else {
            s /= nd2;
        }
        selec = s;
    }

    selec
}

/*
 * eqjoinsel_semi --- eqjoinsel for semi join
 */
unsafe fn eqjoinsel_semi(
    opfuncoid: Oid,
    collation: Oid,
    vardata1: *mut VariableStatData,
    vardata2: *mut VariableStatData,
    nd1_in: f64,
    nd2_in: f64,
    isdefault1: bool,
    isdefault2_in: bool,
    sslot1: *mut AttStatsSlot,
    sslot2: *mut AttStatsSlot,
    stats1: Form_pg_statistic,
    stats2: Form_pg_statistic,
    have_mcvs1: bool,
    have_mcvs2: bool,
    inner_rel: *mut RelOptInfo,
) -> f64 {
    let selec: f64;
    let mut nd1 = nd1_in;
    let mut nd2 = nd2_in;
    let mut isdefault2 = isdefault2_in;

    /*
     * We clamp nd2 to be not more than what we estimate the inner relation's
     * size to be.
     */
    if !(*vardata2).rel.is_null() {
        if nd2 >= (*(*vardata2).rel).rows {
            nd2 = (*(*vardata2).rel).rows;
            isdefault2 = false;
        }
    }
    if nd2 >= (*inner_rel).rows {
        nd2 = (*inner_rel).rows;
        isdefault2 = false;
    }

    if have_mcvs1 && have_mcvs2 && OidIsValid(opfuncoid) {
        /*
         * We have most-common-value lists for both relations.
         */
        LOCAL_FCINFO!(fcinfo_l, 2);
        let mut eqproc: FmgrInfo = std::mem::zeroed();
        let hasmatch1: *mut bool;
        let hasmatch2: *mut bool;
        let nullfrac1 = (*stats1).stanullfrac as f64;
        let mut matchfreq1;
        let uncertainfrac;
        let mut uncertain;
        let mut i: c_int;
        let mut nmatches: c_int;
        let clamped_nvalues2: c_int;

        /*
         * The clamping above could have resulted in nd2 being less than
         * sslot2->nvalues.
         */
        clamped_nvalues2 = Min((*sslot2).nvalues, nd2 as c_int);

        fmgr_info(opfuncoid, &mut eqproc);

        InitFunctionCallInfoData!(fcinfo_l, &mut eqproc, 2, collation, std::ptr::null_mut(), std::ptr::null_mut());
        FC_SET_ISNULL!(fcinfo_l, 0, false);
        FC_SET_ISNULL!(fcinfo_l, 1, false);

        hasmatch1 = palloc0(((*sslot1).nvalues as usize) * std::mem::size_of::<bool>()) as *mut bool;
        hasmatch2 = palloc0((clamped_nvalues2 as usize) * std::mem::size_of::<bool>()) as *mut bool;

        nmatches = 0;
        i = 0;
        while i < (*sslot1).nvalues {
            let mut j: c_int;

            FC_SET_VALUE!(fcinfo_l, 0, *(*sslot1).values.add(i as usize));

            j = 0;
            while j < clamped_nvalues2 {
                if *hasmatch2.add(j as usize) {
                    j += 1;
                    continue;
                }
                FC_SET_VALUE!(fcinfo_l, 1, *(*sslot2).values.add(j as usize));
                (*fcinfo_l).isnull = false;
                let fresult = FunctionCallInvoke!(fcinfo_l);
                if !(*fcinfo_l).isnull && DatumGetBool(fresult) {
                    *hasmatch1.add(i as usize) = true;
                    *hasmatch2.add(j as usize) = true;
                    nmatches += 1;
                    break;
                }
                j += 1;
            }
            i += 1;
        }
        /* Sum up frequencies of matched MCVs */
        matchfreq1 = 0.0;
        i = 0;
        while i < (*sslot1).nvalues {
            if *hasmatch1.add(i as usize) {
                matchfreq1 += *(*sslot1).numbers.add(i as usize) as f64;
            }
            i += 1;
        }
        CLAMP_PROBABILITY!(matchfreq1);
        pfree(hasmatch1 as *mut c_void);
        pfree(hasmatch2 as *mut c_void);

        /*
         * Now estimate the fraction of relation 1 that has at least one join
         * partner.
         */
        if !isdefault1 && !isdefault2 {
            nd1 -= nmatches as f64;
            nd2 -= nmatches as f64;
            if nd1 <= nd2 || nd2 < 0.0 {
                uncertainfrac = 1.0;
            } else {
                uncertainfrac = nd2 / nd1;
            }
        } else {
            uncertainfrac = 0.5;
        }
        uncertain = 1.0 - matchfreq1 - nullfrac1;
        CLAMP_PROBABILITY!(uncertain);
        selec = matchfreq1 + uncertainfrac * uncertain;
    } else {
        /*
         * Without MCV lists for both sides, we can only use the heuristic
         * about nd1 vs nd2.
         */
        let nullfrac1 = if !stats1.is_null() {
            (*stats1).stanullfrac as f64
        } else {
            0.0
        };

        if !isdefault1 && !isdefault2 {
            if nd1 <= nd2 || nd2 < 0.0 {
                selec = 1.0 - nullfrac1;
            } else {
                selec = (nd2 / nd1) * (1.0 - nullfrac1);
            }
        } else {
            selec = 0.5 * (1.0 - nullfrac1);
        }
    }

    selec
}

/*
 *		neqjoinsel		- Join selectivity of "!="
 */
pub unsafe fn neqjoinsel(fcinfo: FunctionCallInfo) -> Datum {
    let root = PG_GETARG_POINTER!(fcinfo, 0) as *mut PlannerInfo;
    let operator = PG_GETARG_OID!(fcinfo, 1);
    let args = PG_GETARG_POINTER!(fcinfo, 2) as *mut List;
    let jointype: JoinType = std::mem::transmute(PG_GETARG_INT16!(fcinfo, 3) as c_int);
    let sjinfo = PG_GETARG_POINTER!(fcinfo, 4) as *mut SpecialJoinInfo;
    let collation = PG_GET_COLLATION!(fcinfo);
    let mut result: float8;

    if matches!(jointype, JOIN_SEMI) || matches!(jointype, JOIN_ANTI) {
        /*
         * For semi-joins / anti-joins, the selectivity estimate should be
         * 1 - nullfrac.
         */
        let mut leftvar: VariableStatData = std::mem::zeroed();
        let mut rightvar: VariableStatData = std::mem::zeroed();
        let mut reversed: bool = false;
        let statsTuple: HeapTuple;
        let nullfrac: f64;

        get_join_variables(root, args, sjinfo, &mut leftvar, &mut rightvar, &mut reversed);
        statsTuple = if reversed {
            rightvar.statsTuple
        } else {
            leftvar.statsTuple
        };
        if HeapTupleIsValid(statsTuple) {
            nullfrac = (*(GETSTRUCT(statsTuple) as Form_pg_statistic)).stanullfrac as f64;
        } else {
            nullfrac = 0.0;
        }
        ReleaseVariableStats!(leftvar);
        ReleaseVariableStats!(rightvar);

        result = 1.0 - nullfrac;
    } else {
        /*
         * We want 1 - eqjoinsel() where the equality operator is the negator.
         */
        let eqop = get_negator(operator);

        if OidIsValid(eqop) {
            result = DatumGetFloat8(DirectFunctionCall5Coll(
                eqjoinsel,
                collation,
                PointerGetDatum(root as *const c_void),
                ObjectIdGetDatum(eqop),
                PointerGetDatum(args as *const c_void),
                Int16GetDatum(jointype as int16),
                PointerGetDatum(sjinfo as *const c_void),
            ));
        } else {
            /* Use default selectivity */
            result = DEFAULT_EQ_SEL;
        }
        result = 1.0 - result;
    }

    PG_RETURN_FLOAT8!(result)
}

/*
 *		scalarltjoinsel - Join selectivity of "<" for scalars
 */
pub unsafe fn scalarltjoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(DEFAULT_INEQ_SEL)
}

/*
 *		scalarlejoinsel - Join selectivity of "<=" for scalars
 */
pub unsafe fn scalarlejoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(DEFAULT_INEQ_SEL)
}

/*
 *		scalargtjoinsel - Join selectivity of ">" for scalars
 */
pub unsafe fn scalargtjoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(DEFAULT_INEQ_SEL)
}

/*
 *		scalargejoinsel - Join selectivity of ">=" for scalars
 */
pub unsafe fn scalargejoinsel(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_FLOAT8!(DEFAULT_INEQ_SEL)
}

/*
 *	matchingsel -- generic matching-operator selectivity support
 */
pub unsafe fn matchingsel(fcinfo: FunctionCallInfo) -> Datum {
    let root = PG_GETARG_POINTER!(fcinfo, 0) as *mut PlannerInfo;
    let operator = PG_GETARG_OID!(fcinfo, 1);
    let args = PG_GETARG_POINTER!(fcinfo, 2) as *mut List;
    let varRelid = PG_GETARG_INT32!(fcinfo, 3);
    let collation = PG_GET_COLLATION!(fcinfo);
    let selec: f64;

    /* Use generic restriction selectivity logic. */
    selec = generic_restriction_selectivity(
        root,
        operator,
        collation,
        args,
        varRelid,
        DEFAULT_MATCHING_SEL,
    );

    PG_RETURN_FLOAT8!(selec as float8)
}

pub unsafe fn matchingjoinsel(fcinfo: FunctionCallInfo) -> Datum {
    /* Just punt, for the moment. */
    PG_RETURN_FLOAT8!(DEFAULT_MATCHING_SEL)
}

// =====================================================================
//  Support routines
// =====================================================================

/*
 * GroupVarInfo: helper for estimate_num_groups.
 */
#[repr(C)]
pub struct GroupVarInfo {
    pub var: *mut Node,       /* might be an expression, not just a Var */
    pub rel: *mut RelOptInfo, /* relation it belongs to */
    pub ndistinct: f64,       /* # distinct values */
    pub isdefault: bool,      /* true if DEFAULT_NUM_DISTINCT was used */
}

/*
 * convert_to_scalar
 */
unsafe fn convert_to_scalar(
    value: Datum,
    valuetypid: Oid,
    collid: Oid,
    scaledvalue: *mut f64,
    lobound: Datum,
    hibound: Datum,
    boundstypid: Oid,
    scaledlobound: *mut f64,
    scaledhibound: *mut f64,
) -> bool {
    let mut failure = false;

    match valuetypid {
        /* Built-in numeric types */
        BOOLOID | INT2OID | INT4OID | INT8OID | FLOAT4OID | FLOAT8OID | NUMERICOID | OIDOID
        | REGPROCOID | REGPROCEDUREOID | REGOPEROID | REGOPERATOROID | REGCLASSOID | REGTYPEOID
        | REGCOLLATIONOID | REGCONFIGOID | REGDICTIONARYOID | REGROLEOID | REGNAMESPACEOID => {
            *scaledvalue = convert_numeric_to_scalar(value, valuetypid, &mut failure);
            *scaledlobound = convert_numeric_to_scalar(lobound, boundstypid, &mut failure);
            *scaledhibound = convert_numeric_to_scalar(hibound, boundstypid, &mut failure);
            return !failure;
        }

        /* Built-in string types */
        CHAROID | BPCHAROID | VARCHAROID | TEXTOID | NAMEOID => {
            let valstr = convert_string_datum(value, valuetypid, collid, &mut failure);
            let lostr = convert_string_datum(lobound, boundstypid, collid, &mut failure);
            let histr = convert_string_datum(hibound, boundstypid, collid, &mut failure);

            /*
             * Bail out if any of the values is not of string type.
             */
            if failure {
                return false;
            }

            convert_string_to_scalar(
                valstr,
                scaledvalue,
                lostr,
                scaledlobound,
                histr,
                scaledhibound,
            );
            pfree(valstr as *mut c_void);
            pfree(lostr as *mut c_void);
            pfree(histr as *mut c_void);
            return true;
        }

        /* Built-in bytea type */
        BYTEAOID => {
            /* We only support bytea vs bytea comparison */
            if boundstypid != BYTEAOID {
                return false;
            }
            convert_bytea_to_scalar(
                value,
                scaledvalue,
                lobound,
                scaledlobound,
                hibound,
                scaledhibound,
            );
            return true;
        }

        /* Built-in time types */
        TIMESTAMPOID | TIMESTAMPTZOID | DATEOID | INTERVALOID | TIMEOID | TIMETZOID => {
            *scaledvalue = convert_timevalue_to_scalar(value, valuetypid, &mut failure);
            *scaledlobound = convert_timevalue_to_scalar(lobound, boundstypid, &mut failure);
            *scaledhibound = convert_timevalue_to_scalar(hibound, boundstypid, &mut failure);
            return !failure;
        }

        /* Built-in network types */
        INETOID | CIDROID | MACADDROID | MACADDR8OID => {
            *scaledvalue = convert_network_to_scalar(value, valuetypid, &mut failure);
            *scaledlobound = convert_network_to_scalar(lobound, boundstypid, &mut failure);
            *scaledhibound = convert_network_to_scalar(hibound, boundstypid, &mut failure);
            return !failure;
        }
        _ => {}
    }
    /* Don't know how to convert */
    *scaledvalue = 0.0;
    *scaledlobound = 0.0;
    *scaledhibound = 0.0;
    false
}

/*
 * Do convert_to_scalar()'s work for any numeric data type.
 */
unsafe fn convert_numeric_to_scalar(value: Datum, typid: Oid, failure: *mut bool) -> f64 {
    match typid {
        BOOLOID => return DatumGetBool(value) as i32 as f64,
        INT2OID => return DatumGetInt16(value) as f64,
        INT4OID => return DatumGetInt32(value) as f64,
        INT8OID => return DatumGetInt64(value) as f64,
        FLOAT4OID => return DatumGetFloat4(value) as f64,
        FLOAT8OID => return DatumGetFloat8(value),
        NUMERICOID => {
            /* Note: out-of-range values will be clamped to +-HUGE_VAL */
            return DatumGetFloat8(DirectFunctionCall1!(numeric_float8_no_overflow, value));
        }
        OIDOID | REGPROCOID | REGPROCEDUREOID | REGOPEROID | REGOPERATOROID | REGCLASSOID
        | REGTYPEOID | REGCOLLATIONOID | REGCONFIGOID | REGDICTIONARYOID | REGROLEOID
        | REGNAMESPACEOID => {
            /* we can treat OIDs as integers... */
            return DatumGetObjectId(value) as f64;
        }
        _ => {}
    }

    *failure = true;
    0.0
}

/*
 * Do convert_to_scalar()'s work for any character-string data type.
 */
unsafe fn convert_string_to_scalar(
    value_in: *mut c_char,
    scaledvalue: *mut f64,
    lobound_in: *mut c_char,
    scaledlobound: *mut f64,
    hibound_in: *mut c_char,
    scaledhibound: *mut f64,
) {
    let mut rangelo: c_int;
    let mut rangehi: c_int;
    let mut sptr: *mut c_char;
    let mut value = value_in;
    let mut lobound = lobound_in;
    let mut hibound = hibound_in;

    rangelo = *hibound as u8 as c_int;
    rangehi = rangelo;
    sptr = lobound;
    while *sptr != 0 {
        let c = *sptr as u8 as c_int;
        if rangelo > c {
            rangelo = c;
        }
        if rangehi < c {
            rangehi = c;
        }
        sptr = sptr.add(1);
    }
    sptr = hibound;
    while *sptr != 0 {
        let c = *sptr as u8 as c_int;
        if rangelo > c {
            rangelo = c;
        }
        if rangehi < c {
            rangehi = c;
        }
        sptr = sptr.add(1);
    }
    /* If range includes any upper-case ASCII chars, make it include all */
    if rangelo <= b'Z' as c_int && rangehi >= b'A' as c_int {
        if rangelo > b'A' as c_int {
            rangelo = b'A' as c_int;
        }
        if rangehi < b'Z' as c_int {
            rangehi = b'Z' as c_int;
        }
    }
    /* Ditto lower-case */
    if rangelo <= b'z' as c_int && rangehi >= b'a' as c_int {
        if rangelo > b'a' as c_int {
            rangelo = b'a' as c_int;
        }
        if rangehi < b'z' as c_int {
            rangehi = b'z' as c_int;
        }
    }
    /* Ditto digits */
    if rangelo <= b'9' as c_int && rangehi >= b'0' as c_int {
        if rangelo > b'0' as c_int {
            rangelo = b'0' as c_int;
        }
        if rangehi < b'9' as c_int {
            rangehi = b'9' as c_int;
        }
    }

    /*
     * If range includes less than 10 chars, assume we have not got enough
     * data, and make it include regular ASCII set.
     */
    if rangehi - rangelo < 9 {
        rangelo = b' ' as c_int;
        rangehi = 127;
    }

    /*
     * Now strip any common prefix of the three strings.
     */
    while *lobound != 0 {
        if *lobound != *hibound || *lobound != *value {
            break;
        }
        lobound = lobound.add(1);
        hibound = hibound.add(1);
        value = value.add(1);
    }

    /*
     * Now we can do the conversions.
     */
    *scaledvalue = convert_one_string_to_scalar(value, rangelo, rangehi);
    *scaledlobound = convert_one_string_to_scalar(lobound, rangelo, rangehi);
    *scaledhibound = convert_one_string_to_scalar(hibound, rangelo, rangehi);
}

unsafe fn convert_one_string_to_scalar(value_in: *mut c_char, rangelo: c_int, rangehi: c_int) -> f64 {
    let mut slen = strlen(value_in) as c_int;
    let mut num: f64;
    let mut denom: f64;
    let base: f64;
    let mut value = value_in;

    if slen <= 0 {
        return 0.0; /* empty string has scalar value 0 */
    }

    /*
     * There seems little point in considering more than a dozen bytes.
     */
    if slen > 12 {
        slen = 12;
    }

    /* Convert initial characters to fraction */
    base = (rangehi - rangelo + 1) as f64;
    num = 0.0;
    denom = base;
    while slen > 0 {
        slen -= 1;
        let mut ch = *value as u8 as c_int;
        value = value.add(1);

        if ch < rangelo {
            ch = rangelo - 1;
        } else if ch > rangehi {
            ch = rangehi + 1;
        }
        num += ((ch - rangelo) as f64) / denom;
        denom *= base;
    }

    num
}

/*
 * Convert a string-type Datum into a palloc'd, null-terminated string.
 */
unsafe fn convert_string_datum(value: Datum, typid: Oid, collid: Oid, failure: *mut bool) -> *mut c_char {
    let mut val: *mut c_char;
    let mylocale: pg_locale_t;

    match typid {
        CHAROID => {
            val = palloc(2) as *mut c_char;
            *val.add(0) = DatumGetChar(value);
            *val.add(1) = 0;
        }
        BPCHAROID | VARCHAROID | TEXTOID => {
            val = TextDatumGetCString(value);
        }
        NAMEOID => {
            let nm = DatumGetPointer(value) as *mut NameData;
            val = pstrdup(NameStr(&*nm));
        }
        _ => {
            *failure = true;
            return std::ptr::null_mut();
        }
    }

    mylocale = pg_newlocale_from_collation(collid);

    if !(*mylocale).collate_is_c {
        let xfrmstr: *mut c_char;
        let xfrmlen: usize;
        let _xfrmlen2: usize;

        xfrmlen = pg_strxfrm(std::ptr::null_mut(), val, 0, mylocale);
        // #ifdef WIN32: skip INT_MAX handling
        xfrmstr = palloc(xfrmlen + 1) as *mut c_char;
        _xfrmlen2 = pg_strxfrm(xfrmstr, val, xfrmlen + 1, mylocale);

        /*
         * Some systems can return a smaller value from the second call.
         */
        Assert!(_xfrmlen2 <= xfrmlen);
        pfree(val as *mut c_void);
        val = xfrmstr;
    }

    val
}

/*
 * Do convert_to_scalar()'s work for any bytea data type.
 */
unsafe fn convert_bytea_to_scalar(
    value: Datum,
    scaledvalue: *mut f64,
    lobound: Datum,
    scaledlobound: *mut f64,
    hibound: Datum,
    scaledhibound: *mut f64,
) {
    let valuep = DatumGetByteaPP(value);
    let loboundp = DatumGetByteaPP(lobound);
    let hiboundp = DatumGetByteaPP(hibound);
    let rangelo: c_int;
    let rangehi: c_int;
    let mut valuelen = VARSIZE_ANY_EXHDR(valuep);
    let mut loboundlen = VARSIZE_ANY_EXHDR(loboundp);
    let mut hiboundlen = VARSIZE_ANY_EXHDR(hiboundp);
    let mut i: c_int;
    let minlen: c_int;
    let mut valstr = VARDATA_ANY(valuep) as *mut u8;
    let mut lostr = VARDATA_ANY(loboundp) as *mut u8;
    let mut histr = VARDATA_ANY(hiboundp) as *mut u8;

    /*
     * Assume bytea data is uniformly distributed across all byte values.
     */
    rangelo = 0;
    rangehi = 255;

    /*
     * Now strip any common prefix of the three strings.
     */
    minlen = Min(Min(valuelen, loboundlen), hiboundlen);
    i = 0;
    while i < minlen {
        if *lostr != *histr || *lostr != *valstr {
            break;
        }
        lostr = lostr.add(1);
        histr = histr.add(1);
        valstr = valstr.add(1);
        loboundlen -= 1;
        hiboundlen -= 1;
        valuelen -= 1;
        i += 1;
    }

    /*
     * Now we can do the conversions.
     */
    *scaledvalue = convert_one_bytea_to_scalar(valstr, valuelen, rangelo, rangehi);
    *scaledlobound = convert_one_bytea_to_scalar(lostr, loboundlen, rangelo, rangehi);
    *scaledhibound = convert_one_bytea_to_scalar(histr, hiboundlen, rangelo, rangehi);
}

unsafe fn convert_one_bytea_to_scalar(
    value_in: *mut u8,
    valuelen_in: c_int,
    rangelo: c_int,
    rangehi: c_int,
) -> f64 {
    let mut num: f64;
    let mut denom: f64;
    let base: f64;
    let mut value = value_in;
    let mut valuelen = valuelen_in;

    if valuelen <= 0 {
        return 0.0; /* empty string has scalar value 0 */
    }

    /*
     * Since base is 256, need not consider more than about 10 chars.
     */
    if valuelen > 10 {
        valuelen = 10;
    }

    /* Convert initial characters to fraction */
    base = (rangehi - rangelo + 1) as f64;
    num = 0.0;
    denom = base;
    while valuelen > 0 {
        valuelen -= 1;
        let mut ch = *value as c_int;
        value = value.add(1);

        if ch < rangelo {
            ch = rangelo - 1;
        } else if ch > rangehi {
            ch = rangehi + 1;
        }
        num += ((ch - rangelo) as f64) / denom;
        denom *= base;
    }

    num
}

/*
 * Do convert_to_scalar()'s work for any timevalue data type.
 */
unsafe fn convert_timevalue_to_scalar(value: Datum, typid: Oid, failure: *mut bool) -> f64 {
    match typid {
        TIMESTAMPOID => return DatumGetTimestamp(value) as f64,
        TIMESTAMPTZOID => return DatumGetTimestampTz(value) as f64,
        DATEOID => return date2timestamp_no_overflow(DatumGetDateADT(value)),
        INTERVALOID => {
            let interval = DatumGetIntervalP(value);

            /*
             * Convert the month part of Interval to days.
             */
            return (*interval).time as f64
                + (*interval).day as f64 * USECS_PER_DAY
                + (*interval).month as f64
                    * ((DAYS_PER_YEAR / MONTHS_PER_YEAR) * USECS_PER_DAY);
        }
        TIMEOID => return DatumGetTimeADT(value) as f64,
        TIMETZOID => {
            let timetz = DatumGetTimeTzADTP(value);
            /* use GMT-equivalent time */
            return (*timetz).time as f64 + ((*timetz).zone as f64 * 1000000.0);
        }
        _ => {}
    }

    *failure = true;
    0.0
}

/*
 * get_restriction_variable
 */
pub unsafe fn get_restriction_variable(
    root: *mut PlannerInfo,
    args: *mut List,
    varRelid: c_int,
    vardata: *mut VariableStatData,
    other: *mut *mut Node,
    varonleft: *mut bool,
) -> bool {
    let left: *mut Node;
    let right: *mut Node;
    let mut rdata: VariableStatData = std::mem::zeroed();

    /* Fail if not a binary opclause (probably shouldn't happen) */
    if list_length(args) != 2 {
        return false;
    }

    left = linitial(args) as *mut Node;
    right = lsecond(args) as *mut Node;

    /*
     * Examine both sides.
     */
    examine_variable(root, left, varRelid, vardata);
    examine_variable(root, right, varRelid, &mut rdata);

    /*
     * If one side is a variable and the other not, we win.
     */
    if !(*vardata).rel.is_null() && rdata.rel.is_null() {
        *varonleft = true;
        *other = estimate_expression_value(root, rdata.var);
        /* Assume we need no ReleaseVariableStats(rdata) here */
        return true;
    }

    if (*vardata).rel.is_null() && !rdata.rel.is_null() {
        *varonleft = false;
        *other = estimate_expression_value(root, (*vardata).var);
        /* Assume we need no ReleaseVariableStats(*vardata) here */
        *vardata = rdata;
        return true;
    }

    /* Oops, clause has wrong structure (probably var op var) */
    ReleaseVariableStats!((*vardata));
    ReleaseVariableStats!(rdata);

    false
}

/*
 * get_join_variables
 */
pub unsafe fn get_join_variables(
    root: *mut PlannerInfo,
    args: *mut List,
    sjinfo: *mut SpecialJoinInfo,
    vardata1: *mut VariableStatData,
    vardata2: *mut VariableStatData,
    join_is_reversed: *mut bool,
) {
    let left: *mut Node;
    let right: *mut Node;

    if list_length(args) != 2 {
        elog!(ERROR, "join operator should take two arguments");
    }

    left = linitial(args) as *mut Node;
    right = lsecond(args) as *mut Node;

    examine_variable(root, left, 0, vardata1);
    examine_variable(root, right, 0, vardata2);

    if !(*vardata1).rel.is_null()
        && bms_is_subset((*(*vardata1).rel).relids, (*sjinfo).syn_righthand)
    {
        *join_is_reversed = true; /* var1 is on RHS */
    } else if !(*vardata2).rel.is_null()
        && bms_is_subset((*(*vardata2).rel).relids, (*sjinfo).syn_lefthand)
    {
        *join_is_reversed = true; /* var2 is on LHS */
    } else {
        *join_is_reversed = false;
    }
}

/* statext_expressions_load copies the tuple, so just pfree it. */
unsafe fn ReleaseDummy(tuple: HeapTuple) {
    pfree(tuple as *mut c_void);
}

/*
 * statistic_proc_security_check
 */
pub unsafe fn statistic_proc_security_check(vardata: *mut VariableStatData, func_oid: Oid) -> bool {
    if (*vardata).acl_ok {
        return true; /* have SELECT privs and no securityQuals */
    }

    if !OidIsValid(func_oid) {
        return false;
    }

    if get_func_leakproof(func_oid) {
        return true;
    }

    ereport!(
        DEBUG2,
        errmsg!(
            "not using statistics because function \"{}\" is not leakproof",
            std::ffi::CStr::from_ptr(get_func_name(func_oid)).to_string_lossy()
        )
    );
    false
}

/*
 * find_join_input_rel
 */
unsafe fn find_join_input_rel(root: *mut PlannerInfo, relids: Relids) -> *mut RelOptInfo {
    let mut rel: *mut RelOptInfo = std::ptr::null_mut();

    if !bms_is_empty(relids) {
        let mut relid: c_int = 0;

        if bms_get_singleton_member(relids, &mut relid) {
            rel = find_base_rel(root, relid);
        } else {
            rel = find_join_rel(root, relids);
        }
    }

    if rel.is_null() {
        elog!(ERROR, "could not find RelOptInfo for given relids");
    }

    rel
}

// libc strlen wrapper for C-string handling in convert_string_* routines.
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * get_variable_numdistinct
 */
pub unsafe fn get_variable_numdistinct(vardata: *mut VariableStatData, isdefault: *mut bool) -> f64 {
    let mut stadistinct: f64;
    let mut stanullfrac: f64 = 0.0;
    let ntuples: f64;

    *isdefault = false;

    /*
     * Determine the stadistinct value to use.
     */
    if HeapTupleIsValid((*vardata).statsTuple) {
        /* Use the pg_statistic entry */
        let stats = GETSTRUCT((*vardata).statsTuple) as Form_pg_statistic;
        stadistinct = (*stats).stadistinct as f64;
        stanullfrac = (*stats).stanullfrac as f64;
    } else if (*vardata).vartype == BOOLOID {
        /*
         * Special-case boolean columns: presumably, two distinct values.
         */
        stadistinct = 2.0;
    } else if !(*vardata).rel.is_null() && (*(*vardata).rel).rtekind == RTE_VALUES {
        /*
         * If the Var represents a column of a VALUES RTE, assume it's unique.
         */
        stadistinct = -1.0; /* unique (and all non null) */
    } else {
        /*
         * We don't keep statistics for system columns, but in some cases we
         * can infer distinctness anyway.
         */
        if !(*vardata).var.is_null() && IsA_!((*vardata).var, Var) {
            match (*((*vardata).var as *mut Var)).varattno {
                SelfItemPointerAttributeNumber => {
                    stadistinct = -1.0; /* unique (and all non null) */
                }
                TableOidAttributeNumber => {
                    stadistinct = 1.0; /* only 1 value */
                }
                _ => {
                    stadistinct = 0.0; /* means "unknown" */
                }
            }
        } else {
            stadistinct = 0.0; /* means "unknown" */
        }
    }

    /*
     * If there is a unique index, DISTINCT or GROUP-BY clause for the
     * variable, assume it is unique.
     */
    if (*vardata).isunique {
        stadistinct = -1.0 * (1.0 - stanullfrac);
    }

    /*
     * If we had an absolute estimate, use that.
     */
    if stadistinct > 0.0 {
        return clamp_row_est(stadistinct);
    }

    /*
     * Otherwise we need to get the relation size; punt if not available.
     */
    if (*vardata).rel.is_null() {
        *isdefault = true;
        return DEFAULT_NUM_DISTINCT;
    }
    ntuples = (*(*vardata).rel).tuples;
    if ntuples <= 0.0 {
        *isdefault = true;
        return DEFAULT_NUM_DISTINCT;
    }

    /*
     * If we had a relative estimate, use that.
     */
    if stadistinct < 0.0 {
        return clamp_row_est(-stadistinct * ntuples);
    }

    /*
     * With no data, estimate ndistinct = ntuples if the table is small.
     */
    if ntuples < DEFAULT_NUM_DISTINCT {
        return clamp_row_est(ntuples);
    }

    *isdefault = true;
    DEFAULT_NUM_DISTINCT
}

/*
 * get_variable_range
 */
unsafe fn get_variable_range(
    root: *mut PlannerInfo,
    vardata: *mut VariableStatData,
    sortop: Oid,
    collation: Oid,
    min: *mut Datum,
    max: *mut Datum,
) -> bool {
    let mut tmin: Datum = 0;
    let mut tmax: Datum = 0;
    let mut have_data = false;
    let mut typLen: int16 = 0;
    let mut typByVal: bool = false;
    let opfuncoid: Oid;
    let mut opproc: FmgrInfo = std::mem::zeroed();
    let mut sslot: AttStatsSlot = std::mem::zeroed();

    // #ifdef NOT_USED block omitted (calls get_actual_variable_range)

    if !HeapTupleIsValid((*vardata).statsTuple) {
        /* no stats available, so default result */
        return false;
    }

    /*
     * If we can't apply the sortop to the stats data, just fail.
     */
    opfuncoid = get_opcode(sortop);
    if !statistic_proc_security_check(vardata, opfuncoid) {
        return false;
    }

    opproc.fn_oid = InvalidOid; /* mark this as not looked up yet */

    get_typlenbyval((*vardata).atttype, &mut typLen, &mut typByVal);

    /*
     * If there is a histogram with the ordering we want, grab the first and
     * last values.
     */
    if get_attstatsslot(
        &mut sslot,
        (*vardata).statsTuple,
        STATISTIC_KIND_HISTOGRAM,
        sortop,
        ATTSTATSSLOT_VALUES,
    ) {
        if sslot.stacoll == collation && sslot.nvalues > 0 {
            tmin = datumCopy(*sslot.values.add(0), typByVal, typLen);
            tmax = datumCopy(*sslot.values.add((sslot.nvalues - 1) as usize), typByVal, typLen);
            have_data = true;
        }
        free_attstatsslot(&mut sslot);
    }

    /*
     * Otherwise, if there is a histogram with some other ordering, scan it.
     */
    if !have_data
        && get_attstatsslot(
            &mut sslot,
            (*vardata).statsTuple,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        )
    {
        get_stats_slot_range(
            &mut sslot,
            opfuncoid,
            &mut opproc,
            collation,
            typLen,
            typByVal,
            &mut tmin,
            &mut tmax,
            &mut have_data,
        );
        free_attstatsslot(&mut sslot);
    }

    /*
     * If we have most-common-values info, look for extreme MCVs.
     */
    if get_attstatsslot(
        &mut sslot,
        (*vardata).statsTuple,
        STATISTIC_KIND_MCV,
        InvalidOid,
        if have_data {
            ATTSTATSSLOT_VALUES
        } else {
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS
        },
    ) {
        let mut use_mcvs = have_data;

        if !have_data {
            let mut sumcommon = 0.0;
            let nullfrac;
            let mut i: c_int = 0;

            while i < sslot.nnumbers {
                sumcommon += *sslot.numbers.add(i as usize) as f64;
                i += 1;
            }
            nullfrac = (*(GETSTRUCT((*vardata).statsTuple) as Form_pg_statistic)).stanullfrac as f64;
            if sumcommon + nullfrac > 0.99999 {
                use_mcvs = true;
            }
        }

        if use_mcvs {
            get_stats_slot_range(
                &mut sslot,
                opfuncoid,
                &mut opproc,
                collation,
                typLen,
                typByVal,
                &mut tmin,
                &mut tmax,
                &mut have_data,
            );
        }
        free_attstatsslot(&mut sslot);
    }

    *min = tmin;
    *max = tmax;
    have_data
}

/*
 * get_stats_slot_range: scan sslot for min/max values
 */
unsafe fn get_stats_slot_range(
    sslot: *mut AttStatsSlot,
    opfuncoid: Oid,
    opproc: *mut FmgrInfo,
    collation: Oid,
    typLen: int16,
    typByVal: bool,
    min: *mut Datum,
    max: *mut Datum,
    p_have_data: *mut bool,
) {
    let mut tmin = *min;
    let mut tmax = *max;
    let mut have_data = *p_have_data;
    let mut found_tmin = false;
    let mut found_tmax = false;

    /* Look up the comparison function, if we didn't already do so */
    if (*opproc).fn_oid != opfuncoid {
        fmgr_info(opfuncoid, opproc);
    }

    /* Scan all the slot's values */
    let mut i: c_int = 0;
    while i < (*sslot).nvalues {
        if !have_data {
            tmin = *(*sslot).values.add(i as usize);
            tmax = tmin;
            found_tmin = true;
            found_tmax = true;
            have_data = true;
            *p_have_data = true;
            i += 1;
            continue;
        }
        if DatumGetBool(FunctionCall2Coll(
            opproc,
            collation,
            *(*sslot).values.add(i as usize),
            tmin,
        )) {
            tmin = *(*sslot).values.add(i as usize);
            found_tmin = true;
        }
        if DatumGetBool(FunctionCall2Coll(
            opproc,
            collation,
            tmax,
            *(*sslot).values.add(i as usize),
        )) {
            tmax = *(*sslot).values.add(i as usize);
            found_tmax = true;
        }
        i += 1;
    }

    /*
     * Copy the slot's values, if we found new extreme values.
     */
    if found_tmin {
        *min = datumCopy(tmin, typByVal, typLen);
    }
    if found_tmax {
        *max = datumCopy(tmax, typByVal, typLen);
    }
}

/*
 * examine_variable
 *		Try to look up statistical data about an expression.
 */
pub unsafe fn examine_variable(
    root: *mut PlannerInfo,
    node_in: *mut Node,
    varRelid: c_int,
    vardata: *mut VariableStatData,
) {
    let mut basenode: *mut Node;
    let varnos: Relids;
    let basevarnos: Relids;
    let mut node = node_in;

    /* Make sure we don't return dangling pointers in vardata */
    std::ptr::write_bytes(vardata as *mut u8, 0, std::mem::size_of::<VariableStatData>());

    /* Save the exposed type of the expression */
    (*vardata).vartype = exprType(node);

    /*
     * PlaceHolderVars are transparent; strip them out first.
     */
    basenode = strip_all_phvs_deep(root, node);

    /*
     * Look inside any binary-compatible relabeling.
     */
    while IsA_!(basenode, RelabelType) {
        basenode = (*(basenode as *mut RelabelType)).arg as *mut Node;
    }

    /* Fast path for a simple Var */
    if IsA_!(basenode, Var)
        && (varRelid == 0 || varRelid == (*(basenode as *mut Var)).varno)
    {
        let var = basenode as *mut Var;

        /* Set up result fields other than the stats tuple */
        (*vardata).var = basenode; /* return Var without phvs or relabeling */
        (*vardata).rel = find_base_rel(root, (*var).varno);
        (*vardata).atttype = (*var).vartype;
        (*vardata).atttypmod = (*var).vartypmod;
        (*vardata).isunique = has_unique_index((*vardata).rel, (*var).varattno);

        /* Try to locate some stats */
        examine_simple_variable(root, var, vardata);

        return;
    }

    /*
     * Okay, it's a more complicated expression.
     */
    varnos = pull_varnos(root, basenode);
    basevarnos = bms_difference(varnos, (*root).outer_join_rels);

    if bms_is_empty(basevarnos) {
        /* No Vars at all ... must be pseudo-constant clause */
    } else {
        let mut relid: c_int = 0;

        /* Check if the expression is in vars of a single base relation */
        if bms_get_singleton_member(basevarnos, &mut relid) {
            if varRelid == 0 || varRelid == relid {
                (*vardata).rel = find_base_rel(root, relid);
                node = basenode; /* strip any phvs or relabeling */
            }
            /* else treat it as a constant */
        } else {
            /* varnos has multiple relids */
            if varRelid == 0 {
                /* treat it as a variable of a join relation */
                (*vardata).rel = find_join_rel(root, varnos);
                node = basenode;
            } else if bms_is_member(varRelid, varnos) {
                /* ignore the vars belonging to other relations */
                (*vardata).rel = find_base_rel(root, varRelid);
                node = basenode;
            }
            /* else treat it as a constant */
        }
    }
    let onerel = (*vardata).rel;

    bms_free(basevarnos);

    (*vardata).var = node;
    (*vardata).atttype = exprType(node);
    (*vardata).atttypmod = exprTypmod(node);

    if !onerel.is_null() {
        /*
         * We have an expression in vars of a single relation.  Try to match
         * it to expressional index columns.
         */

        /*
         * Strip out nullingrels bits.
         */
        if bms_overlap(varnos, (*root).outer_join_rels) {
            node = remove_nulling_relids(node, (*root).outer_join_rels, std::ptr::null_mut());
        }

        foreach!(ilist, (*onerel).indexlist, {
            let index = lfirst(current_cell!(ilist)) as *mut IndexOptInfo;
            let mut indexpr_item: *mut ListCell;
            let mut pos: c_int;

            indexpr_item = list_head((*index).indexprs);
            if indexpr_item.is_null() {
                continue; /* no expressions here... */
            }

            pos = 0;
            while pos < (*index).ncolumns {
                if *(*index).indexkeys.add(pos as usize) == 0 {
                    let mut indexkey: *mut Node;

                    if indexpr_item.is_null() {
                        elog!(ERROR, "too few entries in indexprs list");
                    }
                    indexkey = lfirst(indexpr_item) as *mut Node;
                    if !indexkey.is_null() && IsA_!(indexkey, RelabelType) {
                        indexkey = (*(indexkey as *mut RelabelType)).arg as *mut Node;
                    }
                    if equal(node as *const c_void, indexkey as *const c_void) {
                        /*
                         * Found a match ... is it a unique index?
                         */
                        if (*index).unique
                            && (*index).nkeycolumns == 1
                            && pos == 0
                            && ((*index).indpred.is_null() || (*index).predOK)
                        {
                            (*vardata).isunique = true;
                        }

                        /*
                         * Has it got stats?
                         */
                        if get_index_stats_hook.is_some()
                            && (get_index_stats_hook.unwrap())(
                                root,
                                (*index).indexoid,
                                (pos + 1) as AttrNumber,
                                vardata,
                            )
                        {
                            if HeapTupleIsValid((*vardata).statsTuple)
                                && (*vardata).freefunc.is_none()
                            {
                                elog!(ERROR, "no function provided to release variable stats with");
                            }
                        } else if (*index).indpred.is_null() {
                            (*vardata).statsTuple = SearchSysCache3(
                                STATRELATTINH,
                                ObjectIdGetDatum((*index).indexoid),
                                Int16GetDatum((pos + 1) as int16),
                                BoolGetDatum(false),
                            );
                            (*vardata).freefunc = Some(ReleaseSysCache);

                            if HeapTupleIsValid((*vardata).statsTuple) {
                                /*
                                 * Test if user has permission to access all
                                 * rows from the index's table.
                                 */
                                (*vardata).acl_ok = all_rows_selectable(
                                    root,
                                    (*(*index).rel).relid,
                                    std::ptr::null_mut(),
                                );
                            } else {
                                /* suppress leakproofness checks later */
                                (*vardata).acl_ok = true;
                            }
                        }
                        if !(*vardata).statsTuple.is_null() {
                            break;
                        }
                    }
                    indexpr_item = lnext((*index).indexprs, indexpr_item);
                }
                pos += 1;
            }
            if !(*vardata).statsTuple.is_null() {
                break;
            }
        });

        /*
         * Search extended statistics for one with a matching expression.
         */
        foreach!(slist, (*onerel).statlist, {
            let info = lfirst(current_cell!(slist)) as *mut StatisticExtInfo;
            let rte = planner_rt_fetch((*onerel).relid, root);
            let mut pos: c_int;

            /*
             * Stop once we've found statistics for the expression.
             */
            if !(*vardata).statsTuple.is_null() {
                break;
            }

            /* skip stats without per-expression stats */
            if (*info).kind != STATS_EXT_EXPRESSIONS {
                continue;
            }

            /* skip stats with mismatching stxdinherit value */
            if (*info).inherit != (*rte).inh {
                continue;
            }

            pos = 0;
            foreach!(expr_item, (*info).exprs, {
                let mut expr = lfirst(current_cell!(expr_item)) as *mut Node;

                Assert!(!expr.is_null());

                /* strip RelabelType before comparing it */
                if !expr.is_null() && IsA_!(expr, RelabelType) {
                    expr = (*(expr as *mut RelabelType)).arg as *mut Node;
                }

                /* found a match, see if we can extract pg_statistic row */
                if equal(node as *const c_void, expr as *const c_void) {
                    (*vardata).statsTuple =
                        statext_expressions_load((*info).statOid, (*rte).inh, pos);

                    /* Nothing to release if no data found */
                    if !(*vardata).statsTuple.is_null() {
                        (*vardata).freefunc = Some(ReleaseDummy);
                    }

                    /*
                     * Test if user has permission to access all rows.
                     */
                    (*vardata).acl_ok =
                        all_rows_selectable(root, (*onerel).relid, std::ptr::null_mut());

                    break;
                }

                pos += 1;
            });
        });
    }

    bms_free(varnos);
}

/*
 * strip_all_phvs_deep
 */
unsafe fn strip_all_phvs_deep(root: *mut PlannerInfo, node: *mut Node) -> *mut Node {
    /* If there are no PHVs anywhere, we needn't work hard */
    if (*(*root).glob).lastPHId == 0 {
        return node;
    }

    if !contain_placeholder_walker(node, std::ptr::null_mut()) {
        return node;
    }
    strip_all_phvs_mutator(node, std::ptr::null_mut())
}

/*
 * contain_placeholder_walker
 */
unsafe fn contain_placeholder_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA_!(node, PlaceHolderVar) {
        return true;
    }

    expression_tree_walker(node, contain_placeholder_walker, context)
}

/*
 * strip_all_phvs_mutator
 */
unsafe fn strip_all_phvs_mutator(node: *mut Node, context: *mut c_void) -> *mut Node {
    if node.is_null() {
        return std::ptr::null_mut();
    }
    if IsA_!(node, PlaceHolderVar) {
        /* Strip it and recurse into its contained expression */
        let phv = node as *mut PlaceHolderVar;

        return strip_all_phvs_mutator((*phv).phexpr as *mut Node, context);
    }

    expression_tree_mutator(node, strip_all_phvs_mutator, context)
}

/*
 * examine_simple_variable
 *		Handle a simple Var for examine_variable
 */
unsafe fn examine_simple_variable(
    root: *mut PlannerInfo,
    mut var: *mut Var,
    vardata: *mut VariableStatData,
) {
    let rte = *(*root).simple_rte_array.add((*var).varno as usize);

    Assert!(IsA_!(rte, RangeTblEntry));

    if get_relation_stats_hook.is_some()
        && (get_relation_stats_hook.unwrap())(root, rte, (*var).varattno, vardata)
    {
        /*
         * The hook took control of acquiring a stats tuple.
         */
        if HeapTupleIsValid((*vardata).statsTuple) && (*vardata).freefunc.is_none() {
            elog!(ERROR, "no function provided to release variable stats with");
        }
    } else if (*rte).rtekind == RTE_RELATION {
        /*
         * Plain table or parent of an inheritance appendrel.
         */
        (*vardata).statsTuple = SearchSysCache3(
            STATRELATTINH,
            ObjectIdGetDatum((*rte).relid),
            Int16GetDatum((*var).varattno),
            BoolGetDatum((*rte).inh),
        );
        (*vardata).freefunc = Some(ReleaseSysCache);

        if HeapTupleIsValid((*vardata).statsTuple) {
            /*
             * Test if user has permission to read all rows from this column.
             */
            (*vardata).acl_ok = all_rows_selectable(
                root,
                (*var).varno as Index,
                bms_make_singleton(
                    ((*var).varattno - FirstLowInvalidHeapAttributeNumber) as c_int,
                ),
            );
        } else {
            /* suppress any possible leakproofness checks later */
            (*vardata).acl_ok = true;
        }
    } else if ((*rte).rtekind == RTE_SUBQUERY && !(*rte).inh)
        || ((*rte).rtekind == RTE_CTE && !(*rte).self_reference)
    {
        /*
         * Plain subquery or non-recursive CTE.
         */
        let subroot: *mut PlannerInfo;
        let subquery: *mut Query;
        let subtlist: *mut List;
        let ste: *mut TargetEntry;

        /*
         * Punt if it's a whole-row var.
         */
        if (*var).varattno == InvalidAttrNumber {
            return;
        }

        /*
         * Otherwise, find the subquery's planner subroot.
         */
        if (*rte).rtekind == RTE_SUBQUERY {
            let rel = find_base_rel(root, (*var).varno);
            subroot = (*rel).subroot;
        } else {
            /* CTE case is more difficult */
            let cteroot: *mut PlannerInfo;
            let mut levelsup: Index;
            let mut ndx: c_int;
            let plan_id: c_int;
            let mut found_cte = false;

            /*
             * Find the referenced CTE, and locate the subroot.
             */
            levelsup = (*rte).ctelevelsup;
            let mut cteroot_m = root;
            while levelsup > 0 {
                levelsup -= 1;
                cteroot_m = (*cteroot_m).parent_root;
                if cteroot_m.is_null() {
                    elog!(
                        ERROR,
                        "bad levelsup for CTE \"{}\"",
                        std::ffi::CStr::from_ptr((*rte).ctename).to_string_lossy()
                    );
                }
            }
            cteroot = cteroot_m;

            ndx = 0;
            foreach!(lc, (*(*cteroot).parse).cteList, {
                let cte = lfirst(current_cell!(lc)) as *mut CommonTableExpr;
                if strcmp((*cte).ctename, (*rte).ctename) == 0 {
                    found_cte = true;
                    break;
                }
                ndx += 1;
            });
            if !found_cte {
                elog!(
                    ERROR,
                    "could not find CTE \"{}\"",
                    std::ffi::CStr::from_ptr((*rte).ctename).to_string_lossy()
                );
            }
            if ndx >= list_length((*cteroot).cte_plan_ids) {
                elog!(
                    ERROR,
                    "could not find plan for CTE \"{}\"",
                    std::ffi::CStr::from_ptr((*rte).ctename).to_string_lossy()
                );
            }
            plan_id = list_nth_int((*cteroot).cte_plan_ids, ndx);
            if plan_id <= 0 {
                elog!(
                    ERROR,
                    "no plan was made for CTE \"{}\"",
                    std::ffi::CStr::from_ptr((*rte).ctename).to_string_lossy()
                );
            }
            subroot = list_nth((*(*root).glob).subroots, plan_id - 1) as *mut PlannerInfo;
        }

        /* If the subquery hasn't been planned yet, we have to punt */
        if subroot.is_null() {
            return;
        }
        Assert!(IsA_!(subroot, PlannerInfo));

        /*
         * We must use the subquery parsetree as mangled by the planner.
         */
        subquery = (*subroot).parse;
        Assert!(IsA_!(subquery, Query));

        /*
         * Punt if subquery uses set operations or grouping sets.
         */
        if !(*subquery).setOperations.is_null() || !(*subquery).groupingSets.is_null() {
            return;
        }

        /* Get the subquery output expression referenced by the upper Var */
        if !(*subquery).returningList.is_null() {
            subtlist = (*subquery).returningList;
        } else {
            subtlist = (*subquery).targetList;
        }
        ste = get_tle_by_resno(subtlist, (*var).varattno);
        if ste.is_null() || (*ste).resjunk {
            elog!(
                ERROR,
                "subquery {} does not have attribute {}",
                std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy(),
                (*var).varattno
            );
        }
        var = (*ste).expr as *mut Var;

        /*
         * If subquery uses DISTINCT, we can't make use of any stats.
         */
        if !(*subquery).distinctClause.is_null() {
            if list_length((*subquery).distinctClause) == 1
                && targetIsInSortList(ste, InvalidOid, (*subquery).distinctClause)
            {
                (*vardata).isunique = true;
            }
            /* cannot go further */
            return;
        }

        /* The same idea works for a GROUP-BY too */
        if !(*subquery).groupClause.is_null() {
            if list_length((*subquery).groupClause) == 1
                && targetIsInSortList(ste, InvalidOid, (*subquery).groupClause)
            {
                (*vardata).isunique = true;
            }
            /* cannot go further */
            return;
        }

        /*
         * If the sub-query originated from a security_barrier view, stop here.
         */
        if (*rte).security_barrier {
            return;
        }

        /* Can only handle a simple Var of subquery's query level */
        if !var.is_null() && IsA_!(var, Var) && (*var).varlevelsup == 0 {
            /*
             * OK, recurse into the subquery.
             */
            examine_simple_variable(subroot, var, vardata);
        }
    } else {
        /*
         * Otherwise, the Var comes from a FUNCTION or VALUES RTE.
         */
    }
}

/*
 * all_rows_selectable
 */
pub unsafe fn all_rows_selectable(
    root: *mut PlannerInfo,
    mut varno: Index,
    mut varattnos: *mut Bitmapset,
) -> bool {
    let rel = find_base_rel_noerr(root, varno);
    let mut rte = planner_rt_fetch(varno, root);
    let mut userid: Oid;
    let mut varattno: c_int;

    Assert!((*rte).rtekind == RTE_RELATION);

    /*
     * Determine the user ID to use for privilege checks.
     */
    if !rel.is_null() {
        userid = (*rel).userid;
    } else {
        let perminfo = getRTEPermissionInfo((*(*root).parse).rteperminfos, rte);
        userid = (*perminfo).checkAsUser;
    }
    if !OidIsValid(userid) {
        userid = GetUserId();
    }

    /*
     * Permissions and securityQuals must be checked on the table actually
     * mentioned in the query; navigate up to the inheritance root parent.
     */
    if !(*root).append_rel_array.is_null() {
        let mut appinfo = *(*root).append_rel_array.add(varno as usize);

        while !appinfo.is_null()
            && (*planner_rt_fetch((*appinfo).parent_relid, root)).rtekind == RTE_RELATION
        {
            let mut parent_varattnos: *mut Bitmapset = std::ptr::null_mut();

            /*
             * For each child attribute, find the corresponding parent attr.
             */
            varattno = -1;
            loop {
                varattno = bms_next_member(varattnos, varattno);
                if varattno < 0 {
                    break;
                }
                let mut attno: AttrNumber;
                let parent_attno: AttrNumber;

                attno = (varattno as AttrNumber) + FirstLowInvalidHeapAttributeNumber;

                if attno == InvalidAttrNumber {
                    /*
                     * Whole-row reference; map each column of the child.
                     */
                    attno = 1;
                    while attno <= (*appinfo).num_child_cols as AttrNumber {
                        let pa = *(*appinfo).parent_colnos.add((attno - 1) as usize);
                        if pa == 0 {
                            return false; /* attr is local to child */
                        }
                        parent_varattnos = bms_add_member(
                            parent_varattnos,
                            (pa - FirstLowInvalidHeapAttributeNumber) as c_int,
                        );
                        attno += 1;
                    }
                } else {
                    if attno < 0 {
                        /* System attnos are the same in all tables */
                        parent_attno = attno;
                    } else {
                        if attno > (*appinfo).num_child_cols as AttrNumber {
                            return false; /* safety check */
                        }
                        let pa = *(*appinfo).parent_colnos.add((attno - 1) as usize);
                        if pa == 0 {
                            return false; /* attr is local to child */
                        }
                        parent_attno = pa;
                    }
                    parent_varattnos = bms_add_member(
                        parent_varattnos,
                        (parent_attno - FirstLowInvalidHeapAttributeNumber) as c_int,
                    );
                }
            }

            /* If the parent is itself a child, continue up */
            varno = (*appinfo).parent_relid;
            varattnos = parent_varattnos;
            appinfo = *(*root).append_rel_array.add(varno as usize);
        }

        /* Perform the access check on this parent rel */
        rte = planner_rt_fetch(varno, root);
        Assert!((*rte).rtekind == RTE_RELATION);
    }

    /*
     * For all rows to be accessible, there must be no securityQuals.
     */
    if !(*rte).securityQuals.is_null() {
        return false;
    }

    /*
     * Test for table-level SELECT privilege.
     */
    if pg_class_aclcheck((*rte).relid, userid, ACL_SELECT) == ACLCHECK_OK {
        return true;
    }

    if varattnos.is_null() {
        return false; /* whole-table access requested */
    }

    /*
     * Don't have table-level SELECT privilege; check per-column privileges.
     */
    varattno = -1;
    loop {
        varattno = bms_next_member(varattnos, varattno);
        if varattno < 0 {
            break;
        }
        let attno = (varattno as AttrNumber) + FirstLowInvalidHeapAttributeNumber;

        if attno == InvalidAttrNumber {
            /* Whole-row reference, so must have access to all columns */
            if pg_attribute_aclcheck_all((*rte).relid, userid, ACL_SELECT, ACLMASK_ALL)
                != ACLCHECK_OK
            {
                return false;
            }
        } else if pg_attribute_aclcheck((*rte).relid, attno, userid, ACL_SELECT) != ACLCHECK_OK {
            return false;
        }
    }

    /* If we reach here, have all required column privileges */
    true
}

/*
 * examine_indexcol_variable
 */
unsafe fn examine_indexcol_variable(
    root: *mut PlannerInfo,
    index: *mut IndexOptInfo,
    indexcol: c_int,
    vardata: *mut VariableStatData,
) {
    let colnum: AttrNumber;
    let relid: Oid;

    if *(*index).indexkeys.add(indexcol as usize) != 0 {
        /* Simple variable --- look to stats for the underlying table */
        let rte = planner_rt_fetch((*(*index).rel).relid, root);

        Assert!((*rte).rtekind == RTE_RELATION);
        relid = (*rte).relid;
        Assert!(relid != InvalidOid);
        colnum = *(*index).indexkeys.add(indexcol as usize) as AttrNumber;
        (*vardata).rel = (*index).rel;

        if get_relation_stats_hook.is_some()
            && (get_relation_stats_hook.unwrap())(root, rte, colnum, vardata)
        {
            if HeapTupleIsValid((*vardata).statsTuple) && (*vardata).freefunc.is_none() {
                elog!(ERROR, "no function provided to release variable stats with");
            }
        } else {
            (*vardata).statsTuple = SearchSysCache3(
                STATRELATTINH,
                ObjectIdGetDatum(relid),
                Int16GetDatum(colnum),
                BoolGetDatum((*rte).inh),
            );
            (*vardata).freefunc = Some(ReleaseSysCache);
        }
    } else {
        /* Expression --- maybe there are stats for the index itself */
        relid = (*index).indexoid;
        colnum = (indexcol + 1) as AttrNumber;

        if get_index_stats_hook.is_some()
            && (get_index_stats_hook.unwrap())(root, relid, colnum, vardata)
        {
            if HeapTupleIsValid((*vardata).statsTuple) && (*vardata).freefunc.is_none() {
                elog!(ERROR, "no function provided to release variable stats with");
            }
        } else {
            (*vardata).statsTuple = SearchSysCache3(
                STATRELATTINH,
                ObjectIdGetDatum(relid),
                Int16GetDatum(colnum),
                BoolGetDatum(false),
            );
            (*vardata).freefunc = Some(ReleaseSysCache);
        }
    }
}

// libc strcmp wrapper for C-string comparison.
unsafe fn strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let mut i = 0usize;
    loop {
        let ca = *a.add(i) as u8;
        let cb = *b.add(i) as u8;
        if ca != cb {
            return ca as c_int - cb as c_int;
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}

/*
 * get_actual_variable_range
 */
#[allow(unreachable_code)]
unsafe fn get_actual_variable_range(
    root: *mut PlannerInfo,
    vardata: *mut VariableStatData,
    sortop: Oid,
    collation: Oid,
    min: *mut Datum,
    max: *mut Datum,
) -> bool {
    let mut have_data = false;
    let rel = (*vardata).rel;
    let rte: *mut RangeTblEntry;

    /* No hope if no relation or it doesn't have indexes */
    if rel.is_null() || (*rel).indexlist.is_null() {
        return false;
    }
    /* If it has indexes it must be a plain relation */
    rte = *(*root).simple_rte_array.add((*rel).relid as usize);
    Assert!((*rte).rtekind == RTE_RELATION);

    /* ignore partitioned tables */
    if (*rte).relkind == RELKIND_PARTITIONED_TABLE {
        return false;
    }

    /* Search through the indexes to see if any match our problem */
    foreach!(lc, (*rel).indexlist, {
        let index = lfirst(current_cell!(lc)) as *mut IndexOptInfo;
        let indexscandir: ScanDirection;
        let strategy: StrategyNumber;

        /* Ignore non-ordering indexes */
        if (*index).sortopfamily.is_null() {
            continue;
        }

        /* Ignore partial indexes */
        if !(*index).indpred.is_null() {
            continue;
        }

        /* don't try hypothetical indexes */
        if (*index).hypothetical {
            continue;
        }

        /* ignore indexes that can't index-only-scan on first column */
        if !*(*index).canreturn.add(0) {
            continue;
        }

        /*
         * The first index column must match the desired variable, sortop,
         * and collation.
         */
        if collation != *(*index).indexcollations.add(0) {
            continue; /* test first 'cause it's cheapest */
        }
        if !match_index_to_operand((*vardata).var, 0, index) {
            continue;
        }
        strategy = get_op_opfamily_strategy(sortop, *(*index).sortopfamily.add(0)) as StrategyNumber;
        match IndexAmTranslateStrategy(
            strategy as c_int,
            (*index).relam,
            *(*index).sortopfamily.add(0),
            true,
        ) {
            COMPARE_LT => {
                if *(*index).reverse_sort.add(0) {
                    indexscandir = BackwardScanDirection;
                } else {
                    indexscandir = ForwardScanDirection;
                }
            }
            COMPARE_GT => {
                if *(*index).reverse_sort.add(0) {
                    indexscandir = ForwardScanDirection;
                } else {
                    indexscandir = BackwardScanDirection;
                }
            }
            _ => {
                /* index doesn't match the sortop */
                continue;
            }
        }

        /*
         * Found a suitable index to extract data from.
         */
        {
            let tmpcontext: MemoryContext;
            let oldcontext: MemoryContext;
            let heapRel: *mut Relation;
            let indexRel: *mut Relation;
            let slot: *mut TupleTableSlot;
            let mut typLen: int16 = 0;
            let mut typByVal: bool = false;
            let mut scankeys: [ScanKeyData; 1] = std::mem::zeroed();

            /* Make sure any cruft gets recycled when we're done */
            tmpcontext = AllocSetContextCreate!(
                CurrentMemoryContext,
                c"get_actual_variable_range workspace".as_ptr(),
                ALLOCSET_DEFAULT_SIZES
            );
            oldcontext = MemoryContextSwitchTo(tmpcontext);

            /*
             * Open the table and index so we can read from them.
             */
            heapRel = table_open((*rte).relid, NoLock);
            indexRel = index_open((*index).indexoid, NoLock);

            /* build some stuff needed for indexscan execution */
            slot = table_slot_create(heapRel, std::ptr::null_mut());
            get_typlenbyval((*vardata).atttype, &mut typLen, &mut typByVal);

            /* set up an IS NOT NULL scan key so that we ignore nulls */
            ScanKeyEntryInitialize(
                &mut scankeys[0],
                SK_ISNULL | SK_SEARCHNOTNULL,
                1, /* index col to scan */
                InvalidStrategy,
                InvalidOid,
                InvalidOid,
                InvalidOid,
                0 as Datum,
            );

            /* If min is requested ... */
            if !min.is_null() {
                have_data = get_actual_variable_endpoint(
                    heapRel,
                    indexRel,
                    indexscandir,
                    scankeys.as_mut_ptr(),
                    typLen,
                    typByVal,
                    slot,
                    oldcontext,
                    min,
                );
            } else {
                /* If min not requested, still want to fetch max */
                have_data = true;
            }

            /* If max is requested, and we didn't already fail ... */
            if !max.is_null() && have_data {
                have_data = get_actual_variable_endpoint(
                    heapRel,
                    indexRel,
                    -indexscandir,
                    scankeys.as_mut_ptr(),
                    typLen,
                    typByVal,
                    slot,
                    oldcontext,
                    max,
                );
            }

            /* Clean everything up */
            ExecDropSingleTupleTableSlot(slot);

            index_close(indexRel, NoLock);
            table_close(heapRel, NoLock);

            MemoryContextSwitchTo(oldcontext);
            MemoryContextDelete(tmpcontext);

            /* And we're done */
            break;
        }
    });

    have_data
}

/*
 * Get one endpoint datum (min or max depending on indexscandir).
 */
unsafe fn get_actual_variable_endpoint(
    heapRel: *mut Relation,
    indexRel: *mut Relation,
    indexscandir: ScanDirection,
    scankeys: ScanKey,
    typLen: int16,
    typByVal: bool,
    tableslot: *mut TupleTableSlot,
    outercontext: MemoryContext,
    endpointDatum: *mut Datum,
) -> bool {
    let mut have_data = false;
    let mut SnapshotNonVacuumable: SnapshotData = std::mem::zeroed();
    let index_scan: IndexScanDesc;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let mut last_heap_block: BlockNumber = InvalidBlockNumber;
    let mut n_visited_heap_pages: c_int = 0;
    let mut tid: ItemPointer;
    let mut values: [Datum; INDEX_MAX_KEYS] = std::mem::zeroed();
    let mut isnull: [bool; INDEX_MAX_KEYS] = std::mem::zeroed();
    let oldcontext: MemoryContext;

    const VISITED_PAGES_LIMIT: c_int = 100;

    InitNonVacuumableSnapshot(&mut SnapshotNonVacuumable, GlobalVisTestFor(heapRel));

    index_scan = index_beginscan(
        heapRel,
        indexRel,
        &mut SnapshotNonVacuumable,
        std::ptr::null_mut(),
        1,
        0,
    );
    /* Set it up for index-only scan */
    (*index_scan).xs_want_itup = true;
    index_rescan(index_scan, scankeys, 1, std::ptr::null_mut(), 0);

    /* Fetch first/next tuple in specified direction */
    loop {
        tid = index_getnext_tid(index_scan, indexscandir);
        if tid.is_null() {
            break;
        }
        let block = ItemPointerGetBlockNumber(tid);

        if !VM_ALL_VISIBLE(heapRel, block, &mut vmbuffer) {
            /* Rats, we have to visit the heap to check visibility */
            if !index_fetch_heap(index_scan, tableslot) {
                /*
                 * No visible tuple for this index entry; count heap page
                 * fetches and give up if we've done too many.
                 */
                if block != last_heap_block {
                    last_heap_block = block;
                    n_visited_heap_pages += 1;
                    if n_visited_heap_pages > VISITED_PAGES_LIMIT {
                        break;
                    }
                }

                continue; /* no visible tuple, try next index entry */
            }

            /* We don't actually need the heap tuple for anything */
            ExecClearTuple(tableslot);
        }

        /*
         * We expect that the index will return data in IndexTuple format.
         */
        if (*index_scan).xs_itup.is_null() {
            elog!(ERROR, "no data returned for index-only scan");
        }

        /*
         * We do not yet support recheck here.
         */
        if (*index_scan).xs_recheck {
            break;
        }

        /* OK to deconstruct the index tuple */
        index_deform_tuple(
            (*index_scan).xs_itup,
            (*index_scan).xs_itupdesc,
            values.as_mut_ptr(),
            isnull.as_mut_ptr(),
        );

        /* Shouldn't have got a null, but be careful */
        if isnull[0] {
            elog!(
                ERROR,
                "found unexpected null value in index \"{}\"",
                std::ffi::CStr::from_ptr(RelationGetRelationName(indexRel)).to_string_lossy()
            );
        }

        /* Copy the index column value out to caller's context */
        oldcontext = MemoryContextSwitchTo(outercontext);
        *endpointDatum = datumCopy(values[0], typByVal, typLen);
        MemoryContextSwitchTo(oldcontext);
        have_data = true;
        break;
    }

    if vmbuffer != InvalidBuffer {
        ReleaseBuffer(vmbuffer);
    }
    index_endscan(index_scan);

    have_data
}

// utils/snapmgr.h: InitNonVacuumableSnapshot
unsafe fn InitNonVacuumableSnapshot(snapshot: *mut SnapshotData, vistest: *mut c_void) { unimplemented!("TODO(pg-port): utils/time/snapmgr.rs InitNonVacuumableSnapshot") }

// =====================================================================
//  Index cost estimation functions
// =====================================================================

// optimizer/cost.h cost parameters (real home optimizer/path/costsize.rs)
// TODO(pg-port): real GUC-backed values live in optimizer/path/costsize.rs
static mut cpu_operator_cost: f64 = 0.0025;
static mut cpu_index_tuple_cost: f64 = 0.005;

// lfirst_node!(T, cell): typed cast of lfirst.  Local shim using IsA_.
macro_rules! lfirst_node_ {
    ($t:ty, $tag:tt, $cell:expr) => {{
        let p = lfirst($cell) as *mut $t;
        Assert!(IsA_!(p, $tag));
        p
    }};
}

/*
 * Extract the actual indexquals (as RestrictInfos) from an IndexClause list
 */
pub unsafe fn get_quals_from_indexclauses(indexclauses: *mut List) -> *mut List {
    let mut result: *mut List = NIL;

    foreach!(lc, indexclauses, {
        let iclause = lfirst_node_!(IndexClause, IndexClause, current_cell!(lc));

        foreach!(lc2, (*iclause).indexquals, {
            let rinfo = lfirst_node_!(RestrictInfo, RestrictInfo, current_cell!(lc2));
            result = lappend(result, rinfo as *mut c_void);
        });
    });
    result
}

/*
 * Compute the total evaluation cost of the comparison operands in a list
 * of index qual expressions.
 */
pub unsafe fn index_other_operands_eval_cost(root: *mut PlannerInfo, indexquals: *mut List) -> Cost {
    let mut qual_arg_cost: Cost = 0.0;

    foreach!(lc, indexquals, {
        let mut clause = lfirst(current_cell!(lc)) as *mut Expr;
        let other_operand: *mut Node;
        let mut index_qual_cost: QualCost = std::mem::zeroed();

        /*
         * Index quals will have RestrictInfos, indexorderbys won't.
         */
        if IsA_!(clause, RestrictInfo) {
            clause = (*(clause as *mut RestrictInfo)).clause;
        }

        if IsA_!(clause, OpExpr) {
            let op = clause as *mut OpExpr;
            other_operand = lsecond((*op).args) as *mut Node;
        } else if IsA_!(clause, RowCompareExpr) {
            let rc = clause as *mut RowCompareExpr;
            other_operand = (*rc).rargs as *mut Node;
        } else if IsA_!(clause, ScalarArrayOpExpr) {
            let saop = clause as *mut ScalarArrayOpExpr;
            other_operand = lsecond((*saop).args) as *mut Node;
        } else if IsA_!(clause, NullTest) {
            other_operand = std::ptr::null_mut();
        } else {
            elog!(ERROR, "unsupported indexqual type: {}", nodeTag_(clause as *const c_void) as c_int);
            other_operand = std::ptr::null_mut(); /* keep compiler quiet */
        }

        cost_qual_eval_node(&mut index_qual_cost, other_operand, root);
        qual_arg_cost += index_qual_cost.startup + index_qual_cost.per_tuple;
    });
    qual_arg_cost
}

pub unsafe fn genericcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    costs: *mut GenericCosts,
) {
    let index = (*path).indexinfo;
    let indexQuals = get_quals_from_indexclauses((*path).indexclauses);
    let indexOrderBys = (*path).indexorderbys;
    let indexStartupCost: Cost;
    let mut indexTotalCost: Cost;
    let indexSelectivity: Selectivity;
    let indexCorrelation: f64;
    let numIndexPages: f64;
    let mut numIndexTuples: f64;
    let mut spc_random_page_cost: f64 = 0.0;
    let mut num_sa_scans: f64;
    let num_outer_scans: f64;
    let num_scans: f64;
    let qual_op_cost: f64;
    let qual_arg_cost: f64;
    let selectivityQuals: *mut List;

    /*
     * If the index is partial, AND the index predicate with the indexquals.
     */
    selectivityQuals = add_predicate_to_index_quals(index, indexQuals);

    /*
     * Estimate the number of index descents for ScalarArrayOpExpr index scans.
     */
    num_sa_scans = (*costs).num_sa_scans;
    if num_sa_scans < 1.0 {
        num_sa_scans = 1.0;
        foreach!(l, indexQuals, {
            let rinfo = lfirst(current_cell!(l)) as *mut RestrictInfo;

            if IsA_!((*rinfo).clause, ScalarArrayOpExpr) {
                let saop = (*rinfo).clause as *mut ScalarArrayOpExpr;
                let alength = estimate_array_length(root, lsecond((*saop).args) as *mut Node);

                if alength > 1.0 {
                    num_sa_scans *= alength;
                }
            }
        });
    }

    /* Estimate the fraction of main-table tuples that will be visited */
    indexSelectivity = clauselist_selectivity(
        root,
        selectivityQuals,
        (*(*index).rel).relid as c_int,
        JOIN_INNER,
        std::ptr::null_mut(),
    );

    /*
     * Estimate the number of index tuples that will be visited.
     */
    numIndexTuples = (*costs).numIndexTuples;
    if numIndexTuples <= 0.0 {
        numIndexTuples = indexSelectivity * (*(*index).rel).tuples;

        numIndexTuples = rint(numIndexTuples / num_sa_scans);
    }

    /*
     * We can bound the number of tuples by the index size.
     */
    if numIndexTuples > (*index).tuples {
        numIndexTuples = (*index).tuples;
    }
    if numIndexTuples < 1.0 {
        numIndexTuples = 1.0;
    }

    /*
     * Estimate the number of index pages that will be retrieved.
     */
    if (*index).pages > 1.0 && (*index).tuples > 1.0 {
        numIndexPages = ceil(numIndexTuples * (*index).pages / (*index).tuples);
    } else {
        numIndexPages = 1.0;
    }

    /* fetch estimated page cost for tablespace containing index */
    get_tablespace_page_costs(
        (*index).reltablespace,
        &mut spc_random_page_cost,
        std::ptr::null_mut(),
    );

    /*
     * Now compute the disk access costs.
     */
    num_outer_scans = loop_count;
    num_scans = num_sa_scans * num_outer_scans;

    if num_scans > 1.0 {
        let mut pages_fetched;

        /* total page fetches ignoring cache effects */
        pages_fetched = numIndexPages * num_scans;

        /* use Mackert and Lohman formula to adjust for cache effects */
        pages_fetched = index_pages_fetched(
            pages_fetched,
            (*index).pages as BlockNumber,
            (*index).pages,
            root,
        );

        indexTotalCost = (pages_fetched * spc_random_page_cost) / num_outer_scans;
    } else {
        /*
         * For a single index scan, we just charge spc_random_page_cost per
         * page touched.
         */
        indexTotalCost = numIndexPages * spc_random_page_cost;
    }

    /*
     * CPU cost.
     */
    qual_arg_cost = index_other_operands_eval_cost(root, indexQuals)
        + index_other_operands_eval_cost(root, indexOrderBys);
    qual_op_cost = cpu_operator_cost
        * (list_length(indexQuals) + list_length(indexOrderBys)) as f64;

    indexStartupCost = qual_arg_cost;
    indexTotalCost += qual_arg_cost;
    indexTotalCost += numIndexTuples * num_sa_scans * (cpu_index_tuple_cost + qual_op_cost);

    /*
     * Generic assumption about index correlation: there isn't any.
     */
    indexCorrelation = 0.0;

    /*
     * Return everything to caller.
     */
    (*costs).indexStartupCost = indexStartupCost;
    (*costs).indexTotalCost = indexTotalCost;
    (*costs).indexSelectivity = indexSelectivity;
    (*costs).indexCorrelation = indexCorrelation;
    (*costs).numIndexPages = numIndexPages;
    (*costs).numIndexTuples = numIndexTuples;
    (*costs).spc_random_page_cost = spc_random_page_cost;
    (*costs).num_sa_scans = num_sa_scans;
}

/*
 * If the index is partial, add its predicate to the given qual list.
 */
pub unsafe fn add_predicate_to_index_quals(index: *mut IndexOptInfo, indexQuals: *mut List) -> *mut List {
    let mut predExtraQuals: *mut List = NIL;

    if (*index).indpred.is_null() {
        return indexQuals;
    }

    foreach!(lc, (*index).indpred, {
        let predQual = lfirst(current_cell!(lc)) as *mut Node;
        let oneQual: *mut List = list_make1_local(predQual as *mut c_void);

        if !predicate_implied_by(oneQual, indexQuals, false) {
            predExtraQuals = list_concat(predExtraQuals, oneQual);
        }
    });
    list_concat(predExtraQuals, indexQuals)
}

/*
 * Estimate correlation of btree index's first column.
 */
unsafe fn btcost_correlation(index: *mut IndexOptInfo, vardata: *mut VariableStatData) -> f64 {
    let sortop: Oid;
    let mut sslot: AttStatsSlot = std::mem::zeroed();
    let mut indexCorrelation = 0.0;

    Assert!(HeapTupleIsValid((*vardata).statsTuple));

    sortop = get_opfamily_member(
        *(*index).opfamily.add(0),
        *(*index).opcintype.add(0),
        *(*index).opcintype.add(0),
        BTLessStrategyNumber,
    );
    if OidIsValid(sortop)
        && get_attstatsslot(
            &mut sslot,
            (*vardata).statsTuple,
            STATISTIC_KIND_CORRELATION,
            sortop,
            ATTSTATSSLOT_NUMBERS,
        )
    {
        let mut varCorrelation;

        Assert!(sslot.nnumbers == 1);
        varCorrelation = *sslot.numbers.add(0) as f64;

        if *(*index).reverse_sort.add(0) {
            varCorrelation = -varCorrelation;
        }

        if (*index).nkeycolumns > 1 {
            indexCorrelation = varCorrelation * 0.75;
        } else {
            indexCorrelation = varCorrelation;
        }

        free_attstatsslot(&mut sslot);
    }

    indexCorrelation
}

// list_make1 local helper (single-element list).
unsafe fn list_make1_local(d: *mut c_void) -> *mut List {
    lappend(NIL, d)
}

pub unsafe fn btcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    let index = (*path).indexinfo;
    let mut costs: GenericCosts = GenericCosts::default();
    let mut vardata: VariableStatData = std::mem::zeroed();
    let mut numIndexTuples: f64;
    let mut descentCost: Cost;
    let mut indexBoundQuals: *mut List;
    let mut indexSkipQuals: *mut List;
    let mut indexcol: c_int;
    let mut eqQualHere: bool;
    let mut found_row_compare: bool;
    let mut found_array: bool;
    let mut found_is_null_op: bool;
    let mut have_correlation = false;
    let mut num_sa_scans: f64;
    let mut correlation = 0.0;

    /*
     * Examine the given indexquals to find out which ones count as boundary
     * quals.  (See the long comment in selfuncs.c.)
     */
    indexBoundQuals = NIL;
    indexSkipQuals = NIL;
    indexcol = 0;
    eqQualHere = false;
    found_row_compare = false;
    found_array = false;
    found_is_null_op = false;
    num_sa_scans = 1.0;
    'outer: loop {
        foreach!(lc, (*path).indexclauses, {
            let iclause = lfirst_node_!(IndexClause, IndexClause, current_cell!(lc));

            if indexcol < (*iclause).indexcol {
                let num_sa_scans_prev_cols = num_sa_scans;

                /*
                 * Beginning of a new column's quals.  Consider how nbtree will
                 * backfill skip arrays for any index columns that lacked an
                 * '=' qual.
                 */
                if found_row_compare {
                    break 'outer;
                }
                if eqQualHere {
                    indexcol += 1;
                    indexSkipQuals = NIL;
                }
                eqQualHere = false;

                while indexcol < (*iclause).indexcol {
                    let mut ndistinct;
                    let mut isdefault: bool = true;

                    found_array = true;

                    examine_indexcol_variable(root, index, indexcol, &mut vardata);
                    ndistinct = get_variable_numdistinct(&mut vardata, &mut isdefault);

                    if indexcol == 0 {
                        if HeapTupleIsValid(vardata.statsTuple) {
                            correlation = btcost_correlation(index, &mut vardata);
                        }
                        have_correlation = true;
                    }

                    ReleaseVariableStats!(vardata);

                    if isdefault {
                        num_sa_scans = num_sa_scans_prev_cols;
                        break;
                    }

                    if !indexSkipQuals.is_null() {
                        let partialSkipQuals: *mut List;
                        let ndistinctfrac: Selectivity;

                        partialSkipQuals = add_predicate_to_index_quals(index, indexSkipQuals);

                        ndistinctfrac = clauselist_selectivity(
                            root,
                            partialSkipQuals,
                            (*(*index).rel).relid as c_int,
                            JOIN_INNER,
                            std::ptr::null_mut(),
                        );

                        if ndistinctfrac < DEFAULT_RANGE_INEQ_SEL {
                            num_sa_scans = num_sa_scans_prev_cols;
                            break;
                        }

                        ndistinct = rint(ndistinct * ndistinctfrac);
                        ndistinct = Max(ndistinct, 1.0);
                    }

                    if indexSkipQuals.is_null() {
                        ndistinct += 1.0;
                    }

                    num_sa_scans *= ndistinct;

                    if (*index).pages < num_sa_scans {
                        num_sa_scans = num_sa_scans_prev_cols;
                        break;
                    }

                    indexcol += 1;
                    indexSkipQuals = NIL;
                }

                if indexcol != (*iclause).indexcol {
                    break 'outer;
                }
            }

            Assert!(indexcol == (*iclause).indexcol);

            /* Examine each indexqual associated with this index clause */
            foreach!(lc2, (*iclause).indexquals, {
                let rinfo = lfirst_node_!(RestrictInfo, RestrictInfo, current_cell!(lc2));
                let clause = (*rinfo).clause;
                let mut clause_op: Oid = InvalidOid;
                let op_strategy: c_int;

                if IsA_!(clause, OpExpr) {
                    let op = clause as *mut OpExpr;
                    clause_op = (*op).opno;
                } else if IsA_!(clause, RowCompareExpr) {
                    let rc = clause as *mut RowCompareExpr;
                    clause_op = linitial_oid((*rc).opnos);
                    found_row_compare = true;
                } else if IsA_!(clause, ScalarArrayOpExpr) {
                    let saop = clause as *mut ScalarArrayOpExpr;
                    let other_operand = lsecond((*saop).args) as *mut Node;
                    let alength = estimate_array_length(root, other_operand);

                    clause_op = (*saop).opno;
                    found_array = true;
                    /* estimate SA descents by indexBoundQuals only */
                    if alength > 1.0 {
                        num_sa_scans *= alength;
                    }
                } else if IsA_!(clause, NullTest) {
                    let nt = clause as *mut NullTest;

                    if matches!((*nt).nulltesttype, IS_NULL) {
                        found_is_null_op = true;
                        /* IS NULL is like = for selectivity/skip scan purposes */
                        eqQualHere = true;
                    }
                } else {
                    elog!(ERROR, "unsupported indexqual type: {}", nodeTag_(clause as *const c_void) as c_int);
                }

                /* check for equality operator */
                if OidIsValid(clause_op) {
                    op_strategy = get_op_opfamily_strategy(
                        clause_op,
                        *(*index).opfamily.add(indexcol as usize),
                    );
                    Assert!(op_strategy != 0);
                    if op_strategy == BTEqualStrategyNumber as c_int {
                        eqQualHere = true;
                    }
                }

                indexBoundQuals = lappend(indexBoundQuals, rinfo as *mut c_void);

                /*
                 * Save this indexcol's RestrictInfos if needed for skip arrays.
                 */
                if !eqQualHere
                    && !found_row_compare
                    && indexcol < (*index).nkeycolumns - 1
                {
                    indexSkipQuals = lappend(indexSkipQuals, rinfo as *mut c_void);
                }
            });
        });
        break;
    }

    /*
     * If index is unique and we found an '=' clause for each column, we can
     * just assume numIndexTuples = 1.
     */
    if (*index).unique
        && indexcol == (*index).nkeycolumns - 1
        && eqQualHere
        && !found_array
        && !found_is_null_op
    {
        numIndexTuples = 1.0;
    } else {
        let selectivityQuals: *mut List;
        let btreeSelectivity: Selectivity;

        selectivityQuals = add_predicate_to_index_quals(index, indexBoundQuals);

        btreeSelectivity = clauselist_selectivity(
            root,
            selectivityQuals,
            (*(*index).rel).relid as c_int,
            JOIN_INNER,
            std::ptr::null_mut(),
        );
        numIndexTuples = btreeSelectivity * (*(*index).rel).tuples;

        /*
         * Clamp the number of descents to at most 1/3 the number of pages.
         */
        num_sa_scans = Min(num_sa_scans, ceil((*index).pages * 0.3333333));
        num_sa_scans = Max(num_sa_scans, 1.0);

        numIndexTuples = rint(numIndexTuples / num_sa_scans);
    }

    /*
     * Now do generic index cost estimation.
     */
    costs.numIndexTuples = numIndexTuples;
    costs.num_sa_scans = num_sa_scans;

    genericcostestimate(root, path, loop_count, &mut costs);

    /*
     * Add a CPU-cost component to represent initial btree descent.
     */
    if (*index).tuples > 1.0 {
        descentCost = ceil(log((*index).tuples) / log(2.0)) * cpu_operator_cost;
        costs.indexStartupCost += descentCost;
        costs.indexTotalCost += costs.num_sa_scans * descentCost;
    }

    /*
     * Charge some CPU cost per page descended through.
     */
    descentCost = ((*index).tree_height + 1) as f64 * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.indexStartupCost += descentCost;
    costs.indexTotalCost += costs.num_sa_scans * descentCost;

    if !have_correlation {
        examine_indexcol_variable(root, index, 0, &mut vardata);
        if HeapTupleIsValid(vardata.statsTuple) {
            costs.indexCorrelation = btcost_correlation(index, &mut vardata);
        }
        ReleaseVariableStats!(vardata);
    } else {
        /* btcost_correlation already called earlier on */
        costs.indexCorrelation = correlation;
    }

    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
    *indexPages = costs.numIndexPages;
}

pub unsafe fn hashcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    let mut costs: GenericCosts = GenericCosts::default();

    genericcostestimate(root, path, loop_count, &mut costs);

    /*
     * A hash index has no descent costs as such.
     */

    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
    *indexPages = costs.numIndexPages;
}

pub unsafe fn gistcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    let index = (*path).indexinfo;
    let mut costs: GenericCosts = GenericCosts::default();
    let mut descentCost: Cost;

    genericcostestimate(root, path, loop_count, &mut costs);

    /*
     * We model index descent costs similarly to btree, with assumed fanout
     * 100.
     */
    if (*index).tree_height < 0 {
        if (*index).pages > 1.0 {
            (*index).tree_height = (log((*index).pages) / log(100.0)) as c_int;
        } else {
            (*index).tree_height = 0;
        }
    }

    /*
     * Add a CPU-cost component to represent the costs of initial descent.
     */
    if (*index).tuples > 1.0 {
        descentCost = ceil(log((*index).tuples)) * cpu_operator_cost;
        costs.indexStartupCost += descentCost;
        costs.indexTotalCost += costs.num_sa_scans * descentCost;
    }

    /*
     * Likewise add a per-page charge.
     */
    descentCost = ((*index).tree_height + 1) as f64 * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.indexStartupCost += descentCost;
    costs.indexTotalCost += costs.num_sa_scans * descentCost;

    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
    *indexPages = costs.numIndexPages;
}

pub unsafe fn spgcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    let index = (*path).indexinfo;
    let mut costs: GenericCosts = GenericCosts::default();
    let mut descentCost: Cost;

    genericcostestimate(root, path, loop_count, &mut costs);

    if (*index).tree_height < 0 {
        if (*index).pages > 1.0 {
            (*index).tree_height = (log((*index).pages) / log(100.0)) as c_int;
        } else {
            (*index).tree_height = 0;
        }
    }

    if (*index).tuples > 1.0 {
        descentCost = ceil(log((*index).tuples)) * cpu_operator_cost;
        costs.indexStartupCost += descentCost;
        costs.indexTotalCost += costs.num_sa_scans * descentCost;
    }

    descentCost = ((*index).tree_height + 1) as f64 * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.indexStartupCost += descentCost;
    costs.indexTotalCost += costs.num_sa_scans * descentCost;

    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
    *indexPages = costs.numIndexPages;
}

/*
 * mergejoinscansel			- Scan selectivity of merge join.
 */
pub unsafe fn mergejoinscansel(
    root: *mut PlannerInfo,
    clause: *mut Node,
    opfamily: Oid,
    cmptype: c_int,
    nulls_first: bool,
    leftstart: *mut Selectivity,
    leftend: *mut Selectivity,
    rightstart: *mut Selectivity,
    rightend: *mut Selectivity,
) {
    let left: *mut Node;
    let right: *mut Node;
    let mut leftvar: VariableStatData = std::mem::zeroed();
    let mut rightvar: VariableStatData = std::mem::zeroed();
    let opmethod: Oid;
    let mut op_strategy: c_int = 0;
    let mut op_lefttype: Oid = 0;
    let mut op_righttype: Oid = 0;
    let opno: Oid;
    let collation: Oid;
    let mut lsortop: Oid = InvalidOid;
    let mut rsortop: Oid = InvalidOid;
    let mut lstatop: Oid = InvalidOid;
    let mut rstatop: Oid = InvalidOid;
    let mut ltop: Oid = InvalidOid;
    let mut leop: Oid = InvalidOid;
    let mut revltop: Oid = InvalidOid;
    let mut revleop: Oid = InvalidOid;
    let ltstrat: StrategyNumber;
    let lestrat: StrategyNumber;
    let gtstrat: StrategyNumber;
    let gestrat: StrategyNumber;
    let isgt: bool;
    let mut leftmin: Datum = 0;
    let mut leftmax: Datum = 0;
    let mut rightmin: Datum = 0;
    let mut rightmax: Datum = 0;
    let mut selec: f64;

    /* Set default results if we can't figure anything out. */
    *leftstart = 0.0;
    *rightstart = 0.0;
    *leftend = 1.0;
    *rightend = 1.0;

    /* Deconstruct the merge clause */
    if !is_opclause(clause) {
        return; /* shouldn't happen */
    }
    opno = (*(clause as *mut OpExpr)).opno;
    collation = (*(clause as *mut OpExpr)).inputcollid;
    left = get_leftop(clause as *mut Expr);
    right = get_rightop(clause as *mut Expr);
    if right.is_null() {
        return; /* shouldn't happen */
    }

    /* Look for stats for the inputs */
    examine_variable(root, left, 0, &mut leftvar);
    examine_variable(root, right, 0, &mut rightvar);

    opmethod = get_opfamily_method(opfamily);

    /* Extract the operator's declared left/right datatypes */
    get_op_opfamily_properties(
        opno,
        opfamily,
        false,
        &mut op_strategy,
        &mut op_lefttype,
        &mut op_righttype,
    );
    Assert!(IndexAmTranslateStrategy(op_strategy, opmethod, opfamily, true) == COMPARE_EQ);

    /*
     * Look up the various operators we need.
     */
    match cmptype {
        COMPARE_LT => {
            isgt = false;
            ltstrat = IndexAmTranslateCompareType(COMPARE_LT, opmethod, opfamily, true) as StrategyNumber;
            lestrat = IndexAmTranslateCompareType(COMPARE_LE, opmethod, opfamily, true) as StrategyNumber;
            if op_lefttype == op_righttype {
                /* easy case */
                ltop = get_opfamily_member(opfamily, op_lefttype, op_righttype, ltstrat);
                leop = get_opfamily_member(opfamily, op_lefttype, op_righttype, lestrat);
                lsortop = ltop;
                rsortop = ltop;
                lstatop = lsortop;
                rstatop = rsortop;
                revltop = ltop;
                revleop = leop;
            } else {
                ltop = get_opfamily_member(opfamily, op_lefttype, op_righttype, ltstrat);
                leop = get_opfamily_member(opfamily, op_lefttype, op_righttype, lestrat);
                lsortop = get_opfamily_member(opfamily, op_lefttype, op_lefttype, ltstrat);
                rsortop = get_opfamily_member(opfamily, op_righttype, op_righttype, ltstrat);
                lstatop = lsortop;
                rstatop = rsortop;
                revltop = get_opfamily_member(opfamily, op_righttype, op_lefttype, ltstrat);
                revleop = get_opfamily_member(opfamily, op_righttype, op_lefttype, lestrat);
            }
        }
        COMPARE_GT => {
            /* descending-order case */
            isgt = true;
            ltstrat = IndexAmTranslateCompareType(COMPARE_LT, opmethod, opfamily, true) as StrategyNumber;
            gtstrat = IndexAmTranslateCompareType(COMPARE_GT, opmethod, opfamily, true) as StrategyNumber;
            gestrat = IndexAmTranslateCompareType(COMPARE_GE, opmethod, opfamily, true) as StrategyNumber;
            if op_lefttype == op_righttype {
                /* easy case */
                ltop = get_opfamily_member(opfamily, op_lefttype, op_righttype, gtstrat);
                leop = get_opfamily_member(opfamily, op_lefttype, op_righttype, gestrat);
                lsortop = ltop;
                rsortop = ltop;
                lstatop = get_opfamily_member(opfamily, op_lefttype, op_lefttype, ltstrat);
                rstatop = lstatop;
                revltop = ltop;
                revleop = leop;
            } else {
                ltop = get_opfamily_member(opfamily, op_lefttype, op_righttype, gtstrat);
                leop = get_opfamily_member(opfamily, op_lefttype, op_righttype, gestrat);
                lsortop = get_opfamily_member(opfamily, op_lefttype, op_lefttype, gtstrat);
                rsortop = get_opfamily_member(opfamily, op_righttype, op_righttype, gtstrat);
                lstatop = get_opfamily_member(opfamily, op_lefttype, op_lefttype, ltstrat);
                rstatop = get_opfamily_member(opfamily, op_righttype, op_righttype, ltstrat);
                revltop = get_opfamily_member(opfamily, op_righttype, op_lefttype, gtstrat);
                revleop = get_opfamily_member(opfamily, op_righttype, op_lefttype, gestrat);
            }
        }
        _ => {
            // goto fail; shouldn't get here
            ReleaseVariableStats!(leftvar);
            ReleaseVariableStats!(rightvar);
            return;
        }
    }

    if !OidIsValid(lsortop)
        || !OidIsValid(rsortop)
        || !OidIsValid(lstatop)
        || !OidIsValid(rstatop)
        || !OidIsValid(ltop)
        || !OidIsValid(leop)
        || !OidIsValid(revltop)
        || !OidIsValid(revleop)
    {
        ReleaseVariableStats!(leftvar);
        ReleaseVariableStats!(rightvar);
        return; /* insufficient info in catalogs */
    }

    /* Try to get ranges of both inputs */
    if !isgt {
        if !get_variable_range(root, &mut leftvar, lstatop, collation, &mut leftmin, &mut leftmax) {
            ReleaseVariableStats!(leftvar);
            ReleaseVariableStats!(rightvar);
            return;
        }
        if !get_variable_range(root, &mut rightvar, rstatop, collation, &mut rightmin, &mut rightmax) {
            ReleaseVariableStats!(leftvar);
            ReleaseVariableStats!(rightvar);
            return;
        }
    } else {
        /* need to swap the max and min */
        if !get_variable_range(root, &mut leftvar, lstatop, collation, &mut leftmax, &mut leftmin) {
            ReleaseVariableStats!(leftvar);
            ReleaseVariableStats!(rightvar);
            return;
        }
        if !get_variable_range(root, &mut rightvar, rstatop, collation, &mut rightmax, &mut rightmin) {
            ReleaseVariableStats!(leftvar);
            ReleaseVariableStats!(rightvar);
            return;
        }
    }

    /*
     * Now, the fraction of the left variable that will be scanned.
     */
    selec = scalarineqsel(root, leop, isgt, true, collation, &mut leftvar, rightmax, op_righttype);
    if selec != DEFAULT_INEQ_SEL {
        *leftend = selec;
    }

    /* And similarly for the right variable. */
    selec = scalarineqsel(root, revleop, isgt, true, collation, &mut rightvar, leftmax, op_lefttype);
    if selec != DEFAULT_INEQ_SEL {
        *rightend = selec;
    }

    /*
     * Only one of the two "end" fractions can really be less than 1.0.
     */
    if *leftend > *rightend {
        *leftend = 1.0;
    } else if *leftend < *rightend {
        *rightend = 1.0;
    } else {
        *leftend = 1.0;
        *rightend = 1.0;
    }

    /*
     * The fraction of the left variable scanned before the first join pair.
     */
    selec = scalarineqsel(root, ltop, isgt, false, collation, &mut leftvar, rightmin, op_righttype);
    if selec != DEFAULT_INEQ_SEL {
        *leftstart = selec;
    }

    /* And similarly for the right variable. */
    selec = scalarineqsel(root, revltop, isgt, false, collation, &mut rightvar, leftmin, op_lefttype);
    if selec != DEFAULT_INEQ_SEL {
        *rightstart = selec;
    }

    /*
     * Only one of the two "start" fractions can really be more than zero.
     */
    if *leftstart < *rightstart {
        *leftstart = 0.0;
    } else if *leftstart > *rightstart {
        *rightstart = 0.0;
    } else {
        *leftstart = 0.0;
        *rightstart = 0.0;
    }

    /*
     * If the sort order is nulls-first, skip over any nulls too.
     */
    if nulls_first {
        let mut stats: Form_pg_statistic;

        if HeapTupleIsValid(leftvar.statsTuple) {
            stats = GETSTRUCT(leftvar.statsTuple) as Form_pg_statistic;
            *leftstart += (*stats).stanullfrac as f64;
            CLAMP_PROBABILITY!(*leftstart);
            *leftend += (*stats).stanullfrac as f64;
            CLAMP_PROBABILITY!(*leftend);
        }
        if HeapTupleIsValid(rightvar.statsTuple) {
            stats = GETSTRUCT(rightvar.statsTuple) as Form_pg_statistic;
            *rightstart += (*stats).stanullfrac as f64;
            CLAMP_PROBABILITY!(*rightstart);
            *rightend += (*stats).stanullfrac as f64;
            CLAMP_PROBABILITY!(*rightend);
        }
    }

    /* Disbelieve start >= end, just in case that can happen */
    if *leftstart >= *leftend {
        *leftstart = 0.0;
        *leftend = 1.0;
    }
    if *rightstart >= *rightend {
        *rightstart = 0.0;
        *rightend = 1.0;
    }

    ReleaseVariableStats!(leftvar);
    ReleaseVariableStats!(rightvar);
}

/*
 * estimate_hash_bucket_stats
 */
pub unsafe fn estimate_hash_bucket_stats(
    root: *mut PlannerInfo,
    hashkey: *mut Node,
    nbuckets: f64,
    mcv_freq: *mut Selectivity,
    bucketsize_frac: *mut Selectivity,
) {
    let mut vardata: VariableStatData = std::mem::zeroed();
    let mut estfract: f64;
    let mut ndistinct: f64;
    let stanullfrac: f64;
    let avgfreq: f64;
    let mut isdefault: bool = false;
    let mut sslot: AttStatsSlot = std::mem::zeroed();

    examine_variable(root, hashkey, 0, &mut vardata);

    /* Look up the frequency of the most common value, if available */
    *mcv_freq = 0.0;

    if HeapTupleIsValid(vardata.statsTuple) {
        if get_attstatsslot(
            &mut sslot,
            vardata.statsTuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_NUMBERS,
        ) {
            if sslot.nnumbers > 0 {
                *mcv_freq = *sslot.numbers.add(0) as f64;
            }
            free_attstatsslot(&mut sslot);
        }
    }

    /* Get number of distinct values */
    ndistinct = get_variable_numdistinct(&mut vardata, &mut isdefault);

    /*
     * If ndistinct isn't real, punt.
     */
    if isdefault {
        *bucketsize_frac = Max(0.1, *mcv_freq) as Selectivity;
        ReleaseVariableStats!(vardata);
        return;
    }

    /* Get fraction that are null */
    if HeapTupleIsValid(vardata.statsTuple) {
        let stats = GETSTRUCT(vardata.statsTuple) as Form_pg_statistic;
        stanullfrac = (*stats).stanullfrac as f64;
    } else {
        stanullfrac = 0.0;
    }

    /* Compute avg freq of all distinct data values in raw relation */
    avgfreq = (1.0 - stanullfrac) / ndistinct;

    /*
     * Adjust ndistinct to account for restriction clauses.
     */
    if !vardata.rel.is_null() && (*vardata.rel).tuples > 0.0 {
        ndistinct *= (*vardata.rel).rows / (*vardata.rel).tuples;
        ndistinct = clamp_row_est(ndistinct);
    }

    /*
     * Initial estimate of bucketsize fraction.
     */
    if ndistinct > nbuckets {
        estfract = 1.0 / nbuckets;
    } else {
        estfract = 1.0 / ndistinct;
    }

    /*
     * Adjust estimated bucketsize upward to account for skewed distribution.
     */
    if avgfreq > 0.0 && *mcv_freq > avgfreq {
        estfract *= *mcv_freq / avgfreq;
    }

    /*
     * Clamp bucketsize to sane range.
     */
    if estfract < 1.0e-6 {
        estfract = 1.0e-6;
    } else if estfract > 1.0 {
        estfract = 1.0;
    }

    *bucketsize_frac = estfract as Selectivity;

    ReleaseVariableStats!(vardata);
}

/*
 * estimate_hashagg_tablesize
 */
pub unsafe fn estimate_hashagg_tablesize(
    root: *mut PlannerInfo,
    path: *mut Path,
    agg_costs: *const AggClauseCosts,
    dNumGroups: f64,
) -> f64 {
    let hashentrysize: Size;

    hashentrysize = hash_agg_entry_size(
        list_length((*root).aggtransinfos),
        (*(*path).pathtarget).width as Size,
        (*agg_costs).transitionSpace,
    );

    hashentrysize as f64 * dNumGroups
}

// =====================================================================
//  Remaining grouped/multivariate estimators and GIN/BRIN cost
//  estimators.  These are large and depend on extended-statistics and
//  GIN/BRIN AM internals that are not ported yet; bodies are deferred.
//  Signatures are faithful to selfuncs.c so callers link correctly.
// =====================================================================

/*
 * add_unique_group_var: add an item to a list of GroupVarInfos.
 * TODO(pg-port): faithful body deferred (needs remove_nulling_relids/equal/
 * exprs_known_equal/foreach_delete_current).
 */
unsafe fn add_unique_group_var(
    root: *mut PlannerInfo,
    varinfos: *mut List,
    var: *mut Node,
    vardata: *mut VariableStatData,
) -> *mut List {
    unimplemented!("TODO(pg-port): selfuncs.rs add_unique_group_var")
}

/*
 * estimate_num_groups
 * TODO(pg-port): faithful body deferred (needs pull_var_clause/equal/
 * extended-stats helpers).
 */
pub unsafe fn estimate_num_groups(
    root: *mut PlannerInfo,
    groupExprs: *mut List,
    input_rows: f64,
    pgset: *mut *mut List,
    estinfo: *mut EstimationInfo,
) -> f64 {
    unimplemented!("TODO(pg-port): selfuncs.rs estimate_num_groups")
}

/*
 * estimate_multivariate_bucketsize
 * TODO(pg-port): faithful body deferred (needs extended statistics).
 */
pub unsafe fn estimate_multivariate_bucketsize(
    root: *mut PlannerInfo,
    inner: *mut RelOptInfo,
    hashclauses: *mut List,
    innerbucketsize: *mut Selectivity,
) -> *mut List {
    unimplemented!("TODO(pg-port): selfuncs.rs estimate_multivariate_bucketsize")
}

/*
 * estimate_multivariate_ndistinct
 * TODO(pg-port): faithful body deferred (needs statext_ndistinct_load).
 */
unsafe fn estimate_multivariate_ndistinct(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    varinfos: *mut *mut List,
    ndistinct: *mut f64,
) -> bool {
    unimplemented!("TODO(pg-port): selfuncs.rs estimate_multivariate_ndistinct")
}

/*
 * gincostestimate
 * TODO(pg-port): faithful body deferred (needs GIN AM internals:
 * ginGetStats/extractQuery support functions).
 */
pub unsafe fn gincostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    unimplemented!("TODO(pg-port): selfuncs.rs gincostestimate")
}

/*
 * brincostestimate
 * TODO(pg-port): faithful body deferred (needs BRIN AM internals:
 * brinGetStats).
 */
pub unsafe fn brincostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    unimplemented!("TODO(pg-port): selfuncs.rs brincostestimate")
}
