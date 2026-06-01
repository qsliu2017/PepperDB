//! extended_stats.c
//!   POSTGRES extended statistics
//!
//! Generic code supporting statistics objects created via CREATE STATISTICS.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/statistics/extended_stats.c

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::access::attnum::AttrNumber;
use crate::access::htup_details::HeapTuple;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::{JoinType, Node, Selectivity};
use crate::nodes::pathnodes::{
    PlannerInfo, RelOptInfo, RestrictInfo, SpecialJoinInfo, StatisticExtInfo,
};
use crate::nodes::pg_list::{lfirst, lfirst_int, List};
use crate::nodes::primnodes::{
    BoolExpr, Const, NullTest, OpExpr, RelabelType, ScalarArrayOpExpr, Var,
};
use crate::IsA;
use crate::postgres_ext::Oid;
use crate::statistics::extended_stats_internal::{SortItem, StatsBuildData};
use crate::statistics::statistics::{MCVList, MVDependencies, MVNDistinct};
use crate::utils::rel::{
    Relation, RelationGetDescr, RelationGetRelationName, RelationGetRelid,
};
use crate::utils::sort::sortsupport::{SortSupport, SortSupportData};

use crate::foreach;

/*
 * To avoid consuming too much memory during analysis and/or too much space
 * in the resulting pg_statistic rows, we ignore varlena datums that are wider
 * than WIDTH_THRESHOLD (after detoasting!).  This is legitimate for MCV
 * and distinct-value calculations since a wide value is unlikely to be
 * duplicated at all, much less be a most-common value.  For the same reason,
 * ignoring wide values will not affect our estimates of histogram bin
 * boundaries very much.
 */
const WIDTH_THRESHOLD: c_int = 1024;

/*
 * Used internally to refer to an individual statistics object, i.e.,
 * a pg_statistic_ext entry.
 */
#[repr(C)]
pub struct StatExtEntry {
    pub statOid: Oid,            /* OID of pg_statistic_ext entry */
    pub schema: *mut c_char,     /* statistics object's schema */
    pub name: *mut c_char,       /* statistics object's name */
    pub columns: *mut Bitmapset, /* attribute numbers covered by the object */
    pub types: *mut List,        /* 'char' list of enabled statistics kinds */
    pub stattarget: c_int,       /* statistics target (-1 for default) */
    pub exprs: *mut List,        /* expressions */
}

/* Information needed to analyze a single simple expression. */
#[repr(C)]
pub struct AnlExprData {
    pub expr: *mut Node,                 /* expression to analyze */
    pub vacattrstat: *mut VacAttrStats,  /* statistics attrs to analyze */
}

// ---------------------------------------------------------------------------
// Local stub types for dependencies in not-yet-ported headers.
// ---------------------------------------------------------------------------

/* commands/vacuum.h: VacAttrStats (and its function pointer aliases). */
// TODO(pg-port): dedup when commands/vacuum.h lands.
#[repr(C)]
pub struct VacAttrStats {
    pub attrtypid: Oid,
    pub attrtypmod: i32,
    pub attrcollid: Oid,
    pub attrtype: Form_pg_type,
    pub anl_context: MemoryContext,
    pub attstattarget: c_int,
    pub compute_stats: AnalyzeAttrComputeStatsFunc,
    pub minrows: c_int,
    pub extra_data: *mut c_void,
    pub stats_valid: bool,
    pub stanullfrac: f32,
    pub stawidth: i32,
    pub stadistinct: f32,
    pub stakind: [i16; STATISTIC_NUM_SLOTS as usize],
    pub staop: [Oid; STATISTIC_NUM_SLOTS as usize],
    pub stacoll: [Oid; STATISTIC_NUM_SLOTS as usize],
    pub numnumbers: [c_int; STATISTIC_NUM_SLOTS as usize],
    pub stanumbers: [*mut f32; STATISTIC_NUM_SLOTS as usize],
    pub numvalues: [c_int; STATISTIC_NUM_SLOTS as usize],
    pub stavalues: [*mut Datum; STATISTIC_NUM_SLOTS as usize],
    pub statypid: [Oid; STATISTIC_NUM_SLOTS as usize],
    pub statyplen: [i16; STATISTIC_NUM_SLOTS as usize],
    pub statypbyval: [bool; STATISTIC_NUM_SLOTS as usize],
    pub statypalign: [c_char; STATISTIC_NUM_SLOTS as usize],
    pub tupattnum: c_int,
    pub tupDesc: TupleDesc,
    pub exprvals: *mut Datum,
    pub exprnulls: *mut bool,
    pub rowstride: c_int,
}

// TODO(pg-port): dedup when commands/vacuum.h lands.
pub type VacAttrStatsP = *mut VacAttrStats;
// TODO(pg-port): dedup when commands/vacuum.h lands.
pub type AnalyzeAttrComputeStatsFunc = Option<
    unsafe fn(stats: *mut VacAttrStats, fetchfunc: AnalyzeAttrFetchFunc, samplerows: c_int, totalrows: f64),
>;
// TODO(pg-port): dedup when commands/vacuum.h lands.
pub type AnalyzeAttrFetchFunc =
    Option<unsafe fn(stats: VacAttrStatsP, rownum: c_int, isNull: *mut bool) -> Datum>;

const STATISTIC_NUM_SLOTS: c_int = 5; // TODO(pg-port): catalog/pg_statistic.h

/* commands/vacuum.h / analyze: GUC default_statistics_target. */
// TODO(pg-port): utils/misc/guc_tables.c default_statistics_target.
static mut default_statistics_target: c_int = 100;

/* commands/vacuum.h: MAX_STATISTICS_TARGET. */
// TODO(pg-port): dedup when commands/vacuum.h lands.
const MAX_STATISTICS_TARGET: c_int = 10000;

/* statistics/statistics.h: stat kind chars. */
// TODO(pg-port): dedup when statistics/statistics.h lands.
const STATS_EXT_NDISTINCT: c_char = b'd' as c_char;
const STATS_EXT_DEPENDENCIES: c_char = b'f' as c_char;
const STATS_EXT_MCV: c_char = b'm' as c_char;
const STATS_EXT_EXPRESSIONS: c_char = b'e' as c_char;
const STATS_MAX_DIMENSIONS: c_int = 8;

/* access/htup_details.h: TupleDesc, Form_pg_type. */
// TODO(pg-port): dedup when access/tupdesc.h / catalog/pg_type.h land.
pub use crate::access::common::tupdesc::TupleDesc;
// TODO(pg-port): dedup when catalog/pg_type.h lands.
pub type Form_pg_type = *mut c_void;
// TODO(pg-port): dedup when nodes/parsenodes.h lands.
pub type Index = c_uint;
// TODO(pg-port): dedup when utils/array.h lands.
pub type ArrayType = c_void;
// TODO(pg-port): dedup when utils/array.h lands.
pub type ArrayBuildState = c_void;
// TODO(pg-port): dedup when access/htup.h lands.
pub type Form_pg_statistic_ext = *mut c_void;
// TODO(pg-port): dedup when nodes/parsenodes.h lands.
pub type RangeTblEntry = c_void;
// TODO(pg-port): dedup when c.h Size lands.
pub type bytea = c_void;
// TODO(pg-port): dedup when access/genam.h lands.
pub type SysScanDesc = *mut c_void;
// TODO(pg-port): dedup when access/skey.h lands.
#[repr(C)]
pub struct ScanKeyData {
    _opaque: [u8; 0],
}
// TODO(pg-port): dedup when commands/defrem.h lands.
#[repr(C)]
pub struct AttributeOpts {
    pub n_distinct: f64,
}
// TODO(pg-port): dedup when executor/executor.h lands.
pub type TupleTableSlot = c_void;
// TODO(pg-port): dedup when nodes/execnodes.h lands.
pub type EState = c_void;
// TODO(pg-port): dedup when nodes/execnodes.h lands.
pub type ExprContext = c_void;
// TODO(pg-port): dedup when nodes/execnodes.h lands.
pub type ExprState = c_void;
// TODO(pg-port): dedup when nodes/primnodes.h lands.
pub type Expr = c_void;
// TODO(pg-port): dedup when utils/array.h lands.
pub type ExpandedArrayHeader = c_void;
// TODO(pg-port): dedup when access/htup.h lands.
pub type HeapTupleHeader = *mut c_void;
// TODO(pg-port): dedup when access/htup.h lands.
#[repr(C)]
pub struct HeapTupleData {
    _opaque: [u8; 0],
}
// TODO(pg-port): dedup when access/multixact.h / sortsupport land.
pub type MultiSortSupport = *mut MultiSortSupportData;
// TODO(pg-port): dedup when statistics/extended_stats_internal.h lands.
#[repr(C)]
pub struct MultiSortSupportData {
    _opaque: [u8; 0],
}

/* catalog OIDs / Anum constants used below. */
// TODO(pg-port): dedup when catalog headers land.
const StatisticExtRelationId: Oid = 3381;
const StatisticExtDataRelationId: Oid = 3429;
const StatisticRelationId: Oid = 2619;
const StatisticExtRelidIndexId: Oid = 3380;
const CHAROID: Oid = 18;
const FLOAT4OID: Oid = 700;
const InvalidOid: Oid = 0;
const RowExclusiveLock: c_int = 3;
const BTEqualStrategyNumber: c_int = 3;
const InvalidAttrNumber: c_int = 0;
const FirstLowInvalidHeapAttributeNumber: c_int = -8;
const MaxAttrNumber: c_int = 1600;

const STATEXTOID: c_int = 0; // TODO(pg-port): utils/syscache.h
const STATEXTDATASTXOID: c_int = 0; // TODO(pg-port): utils/syscache.h
const TYPEOID: c_int = 0; // TODO(pg-port): utils/syscache.h

const F_OIDEQ: Oid = 184; // TODO(pg-port): utils/fmgroids.h
const F_EQSEL: Oid = 101; // TODO(pg-port): utils/fmgroids.h
const F_NEQSEL: Oid = 102; // TODO(pg-port): utils/fmgroids.h
const F_SCALARLTSEL: Oid = 103; // TODO(pg-port): utils/fmgroids.h
const F_SCALARLESEL: Oid = 336; // TODO(pg-port): utils/fmgroids.h
const F_SCALARGTSEL: Oid = 104; // TODO(pg-port): utils/fmgroids.h
const F_SCALARGESEL: Oid = 337; // TODO(pg-port): utils/fmgroids.h

const Natts_pg_statistic_ext_data: usize = 7; // TODO(pg-port): catalog
const Anum_pg_statistic_ext_data_stxdndistinct: c_int = 3;
const Anum_pg_statistic_ext_data_stxddependencies: c_int = 4;
const Anum_pg_statistic_ext_data_stxdmcv: c_int = 5;
const Anum_pg_statistic_ext_data_stxdexpr: c_int = 6;
const Anum_pg_statistic_ext_data_stxoid: c_int = 1;
const Anum_pg_statistic_ext_data_stxdinherit: c_int = 2;

const Anum_pg_statistic_ext_stxrelid: c_int = 2;
const Anum_pg_statistic_ext_stxstattarget: c_int = 8;
const Anum_pg_statistic_ext_stxkind: c_int = 9;
const Anum_pg_statistic_ext_stxexprs: c_int = 11;

const Natts_pg_statistic: usize = 31; // TODO(pg-port): catalog/pg_statistic.h
const Anum_pg_statistic_starelid: c_int = 1;
const Anum_pg_statistic_staattnum: c_int = 2;
const Anum_pg_statistic_stainherit: c_int = 3;
const Anum_pg_statistic_stanullfrac: c_int = 4;
const Anum_pg_statistic_stawidth: c_int = 5;
const Anum_pg_statistic_stadistinct: c_int = 6;
const Anum_pg_statistic_stakind1: c_int = 7;
const Anum_pg_statistic_staop1: c_int = 12;
const Anum_pg_statistic_stacoll1: c_int = 17;
const Anum_pg_statistic_stanumbers1: c_int = 22;
const Anum_pg_statistic_stavalues1: c_int = 27;

/* progress reporting constants (commands/progress.h). */
// TODO(pg-port): dedup when commands/progress.h lands.
const PROGRESS_ANALYZE_PHASE: c_int = 0;
const PROGRESS_ANALYZE_EXT_STATS_TOTAL: c_int = 0;
const PROGRESS_ANALYZE_EXT_STATS_COMPUTED: c_int = 0;
const PROGRESS_ANALYZE_PHASE_COMPUTE_EXT_STATS: i64 = 0;

/* errcodes (utils/errcodes.h). */
// TODO(pg-port): dedup when utils/errcodes.h lands.
const ERRCODE_INVALID_OBJECT_DEFINITION: c_int = 0;
const ERRCODE_WRONG_OBJECT_TYPE: c_int = 0;

// ---------------------------------------------------------------------------
// Stubbed dependency functions (defined in other .c files).
// ---------------------------------------------------------------------------

// TODO(pg-port): statistics/extended_stats_internal.h.
unsafe fn statext_ndistinct_build(_totalrows: f64, _data: *mut StatsBuildData) -> *mut MVNDistinct { unimplemented!() }
unsafe fn statext_ndistinct_serialize(_ndistinct: *mut MVNDistinct) -> *mut bytea { unimplemented!() }
unsafe fn statext_dependencies_build(_data: *mut StatsBuildData) -> *mut MVDependencies { unimplemented!() }
unsafe fn statext_dependencies_serialize(_deps: *mut MVDependencies) -> *mut bytea { unimplemented!() }
unsafe fn statext_mcv_build(_data: *mut StatsBuildData, _totalrows: f64, _stattarget: c_int) -> *mut MCVList { unimplemented!() }
unsafe fn statext_mcv_serialize(_mcv: *mut MCVList, _stats: *mut *mut VacAttrStats) -> *mut bytea { unimplemented!() }
unsafe fn statext_mcv_load(_mvoid: Oid, _inh: bool) -> *mut MCVList { unimplemented!() }
unsafe fn mcv_combine_selectivities(_simple_sel: Selectivity, _mcv_sel: Selectivity, _mcv_basesel: Selectivity, _mcv_totalsel: Selectivity) -> Selectivity { unimplemented!() }
unsafe fn mcv_clauselist_selectivity(_root: *mut PlannerInfo, _stat: *mut StatisticExtInfo, _clauses: *mut List, _varRelid: c_int, _jointype: JoinType, _sjinfo: *mut SpecialJoinInfo, _rel: *mut RelOptInfo, _basesel: *mut Selectivity, _totalsel: *mut Selectivity) -> Selectivity { unimplemented!() }
unsafe fn mcv_clause_selectivity_or(_root: *mut PlannerInfo, _stat: *mut StatisticExtInfo, _mcv: *mut MCVList, _clause: *mut Node, _or_matches: *mut *mut bool, _basesel: *mut Selectivity, _overlap_mcvsel: *mut Selectivity, _overlap_basesel: *mut Selectivity, _totalsel: *mut Selectivity) -> Selectivity { unimplemented!() }

// TODO(pg-port): statistics/dependencies.c.
unsafe fn dependencies_clauselist_selectivity(_root: *mut PlannerInfo, _clauses: *mut List, _varRelid: c_int, _jointype: JoinType, _sjinfo: *mut SpecialJoinInfo, _rel: *mut RelOptInfo, _estimatedclauses: *mut *mut Bitmapset) -> Selectivity { unimplemented!() }

// TODO(pg-port): nodes/nodeFuncs.c / optimizer/util/clauses.c.
unsafe fn equal(_a: *const c_void, _b: *const c_void) -> bool { unimplemented!() }
unsafe fn is_opclause(_clause: *const c_void) -> bool { unimplemented!() }
unsafe fn is_andclause(_clause: *const c_void) -> bool { unimplemented!() }
unsafe fn is_orclause(_clause: *const c_void) -> bool { unimplemented!() }
unsafe fn is_notclause(_clause: *const c_void) -> bool { unimplemented!() }

// TODO(pg-port): postgres.h CLAMP_PROBABILITY macro.
unsafe fn CLAMP_PROBABILITY(p: &mut Selectivity) {
    if *p < 0.0 {
        *p = 0.0;
    } else if *p > 1.0 {
        *p = 1.0;
    }
}

// TODO(pg-port): access/htup_details.h DatumGetHeapTupleHeader.
unsafe fn DatumGetHeapTupleHeader(_d: Datum) -> HeapTupleHeader { unimplemented!() }
// TODO(pg-port): storage/itemptr.h ItemPointerSetInvalid.
unsafe fn ItemPointerSetInvalid(_pointer: *mut c_void) {}
// TODO(pg-port): utils/builtins.h TextDatumGetCString.
unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char { unimplemented!() }
// TODO(pg-port): nodes/value.h / catalog NameStr, utils/mmgr pstrdup.
unsafe fn pstrdup(_s: *const c_char) -> *mut c_char { unimplemented!() }
unsafe fn NameStr(_name: *const c_void) -> *mut c_char { unimplemented!() }
// TODO(pg-port): fmgr.h OidFunctionCall1.
unsafe fn OidFunctionCall1(_functionId: Oid, _arg1: Datum) -> Datum { unimplemented!() }
// TODO(pg-port): access/htup.h GETSTRUCT for Form_pg_statistic_ext (see GETSTRUCT above).

// TODO(pg-port): nodes/bitmapset.c.
unsafe fn bms_add_member(_a: *mut Bitmapset, _x: c_int) -> *mut Bitmapset { unimplemented!() }
unsafe fn bms_add_members(_a: *mut Bitmapset, _b: *const Bitmapset) -> *mut Bitmapset { unimplemented!() }
unsafe fn bms_num_members(_a: *const Bitmapset) -> c_int { unimplemented!() }
unsafe fn bms_next_member(_a: *const Bitmapset, _prevbit: c_int) -> c_int { unimplemented!() }
unsafe fn bms_is_member(_x: c_int, _a: *const Bitmapset) -> bool { unimplemented!() }
unsafe fn bms_is_subset(_a: *const Bitmapset, _b: *const Bitmapset) -> bool { unimplemented!() }
unsafe fn bms_membership(_a: *const Bitmapset) -> c_int { unimplemented!() }
unsafe fn bms_get_singleton_member(_a: *const Bitmapset, _member: *mut c_int) -> bool { unimplemented!() }
unsafe fn bms_free(_a: *mut Bitmapset) {}
const BMS_SINGLETON: c_int = 1; // TODO(pg-port): nodes/bitmapset.h

// TODO(pg-port): nodes/list.c.
unsafe fn lappend(_list: *mut List, _datum: *mut c_void) -> *mut List { unimplemented!() }
unsafe fn lappend_int(_list: *mut List, _datum: c_int) -> *mut List { unimplemented!() }
unsafe fn list_length(_l: *const List) -> c_int { unimplemented!() }
unsafe fn list_free(_list: *mut List) {}
unsafe fn linitial(_l: *const List) -> *mut c_void { unimplemented!() }
unsafe fn lsecond(_l: *const List) -> *mut c_void { unimplemented!() }
const NIL: *mut List = core::ptr::null_mut();

// TODO(pg-port): access/table.c.
unsafe fn table_open(_relationId: Oid, _lockmode: c_int) -> Relation { unimplemented!() }
unsafe fn table_close(_relation: Relation, _lockmode: c_int) {}

// TODO(pg-port): access/genam.c.
unsafe fn systable_beginscan(_heapRelation: Relation, _indexId: Oid, _indexOK: bool, _snapshot: *mut c_void, _nkeys: c_int, _key: *mut ScanKeyData) -> SysScanDesc { unimplemented!() }
unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple { unimplemented!() }
unsafe fn systable_endscan(_sysscan: SysScanDesc) {}
unsafe fn ScanKeyInit(_entry: *mut ScanKeyData, _attributeNumber: c_int, _strategy: c_int, _procedure: Oid, _argument: Datum) {}

// TODO(pg-port): access/common/heaptuple.c.
unsafe fn heap_attisnull(_tup: HeapTuple, _attnum: c_int, _tupleDesc: TupleDesc) -> bool { unimplemented!() }
unsafe fn heap_form_tuple(_tupleDescriptor: TupleDesc, _values: *mut Datum, _isnull: *mut bool) -> HeapTuple { unimplemented!() }
unsafe fn heap_freetuple(_htup: HeapTuple) {}
unsafe fn heap_copytuple(_tuple: *mut HeapTupleData) -> HeapTuple { unimplemented!() }
unsafe fn heap_getattr(_tup: HeapTuple, _attnum: c_int, _tupleDesc: TupleDesc, _isnull: *mut bool) -> Datum { unimplemented!() }
unsafe fn heap_copy_tuple_as_datum(_tuple: HeapTuple, _tupleDesc: TupleDesc) -> Datum { unimplemented!() }
unsafe fn HeapTupleIsValid(htup: HeapTuple) -> bool { !htup.is_null() }
unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut c_void { unimplemented!() }
unsafe fn HeapTupleHeaderGetDatumLength(_tup: HeapTupleHeader) -> u32 { unimplemented!() }

// TODO(pg-port): utils/cache/syscache.c.
unsafe fn SearchSysCacheCopy1(_cacheId: c_int, _key1: Datum) -> HeapTuple { unimplemented!() }
unsafe fn SearchSysCache2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> HeapTuple { unimplemented!() }
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {}
unsafe fn SysCacheGetAttr(_cacheId: c_int, _tup: HeapTuple, _attributeNumber: c_int, _isNull: *mut bool) -> Datum { unimplemented!() }
unsafe fn SysCacheGetAttrNotNull(_cacheId: c_int, _tup: HeapTuple, _attributeNumber: c_int) -> Datum { unimplemented!() }

// TODO(pg-port): utils/cache/lsyscache.c.
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char { unimplemented!() }
unsafe fn get_typlen(_typid: Oid) -> i16 { unimplemented!() }
unsafe fn get_oprrest(_opno: Oid) -> Oid { unimplemented!() }
unsafe fn get_opcode(_opno: Oid) -> Oid { unimplemented!() }
unsafe fn get_func_leakproof(_funcid: Oid) -> bool { unimplemented!() }
unsafe fn get_rel_type_id(_relid: Oid) -> Oid { unimplemented!() }

// TODO(pg-port): catalog/indexing.c.
unsafe fn CatalogTupleInsert(_heapRel: Relation, _tup: HeapTuple) {}

// TODO(pg-port): statistics/stat_utils.c / commands/statscmds.c.
unsafe fn RemoveStatisticsDataById(_statsOid: Oid, _inh: bool) {}

// TODO(pg-port): optimizer/util/clauses.c.
unsafe fn eval_const_expressions(_root: *mut PlannerInfo, _node: *mut Node) -> *mut Node { unimplemented!() }
unsafe fn fix_opfuncids(_node: *mut Node) {}

// TODO(pg-port): nodes/read.c.
unsafe fn stringToNode(_str: *mut c_char) -> *mut c_void { unimplemented!() }

// TODO(pg-port): nodes/nodeFuncs.c.
unsafe fn exprType(_expr: *const Node) -> Oid { unimplemented!() }
unsafe fn exprTypmod(_expr: *const Node) -> i32 { unimplemented!() }
unsafe fn exprCollation(_expr: *const Node) -> Oid { unimplemented!() }

// TODO(pg-port): optimizer/util/var.c.
unsafe fn pull_varattnos(_node: *mut Node, _varno: c_int, _varattnos: *mut *mut Bitmapset) {}

// TODO(pg-port): commands/analyze.c.
unsafe fn std_typanalyze(_stats: *mut VacAttrStats) -> bool { unimplemented!() }
unsafe fn all_rows_selectable(_root: *mut PlannerInfo, _relid: Index, _attnums: *mut Bitmapset) -> bool { unimplemented!() }

// TODO(pg-port): utils/cache/attoptcache.c.
unsafe fn get_attribute_options(_attrelid: Oid, _attnum: c_int) -> *mut AttributeOpts { unimplemented!() }

// TODO(pg-port): utils/adt/selfuncs.c.
unsafe fn clause_selectivity_ext(_root: *mut PlannerInfo, _clause: *mut Node, _varRelid: c_int, _jointype: JoinType, _sjinfo: *mut SpecialJoinInfo, _use_extended_stats: bool) -> Selectivity { unimplemented!() }
unsafe fn clauselist_selectivity_ext(_root: *mut PlannerInfo, _clauses: *mut List, _varRelid: c_int, _jointype: JoinType, _sjinfo: *mut SpecialJoinInfo, _use_extended_stats: bool) -> Selectivity { unimplemented!() }

// TODO(pg-port): parser/parsetree.c.
unsafe fn planner_rt_fetch(_rti: Index, _root: *mut PlannerInfo) -> *mut RangeTblEntry { unimplemented!() }

// TODO(pg-port): postmaster/autovacuum.c.
unsafe fn AmAutoVacuumWorkerProcess() -> bool { unimplemented!() }

// TODO(pg-port): pgstat.c.
unsafe fn pgstat_progress_update_multi_param(_nparam: c_int, _index: *const c_int, _val: *const i64) {}
unsafe fn pgstat_progress_update_param(_index: c_int, _val: i64) {}

// TODO(pg-port): access/detoast.c.
unsafe fn toast_raw_datum_size(_value: Datum) -> c_int { unimplemented!() }
unsafe fn pg_detoast_datum(_datum: *mut c_void) -> *mut c_void { unimplemented!() }

// TODO(pg-port): lib/qsort_interruptible.c.
type qsort_arg_comparator = unsafe fn(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int;
unsafe fn qsort_interruptible(_base: *mut c_void, _nel: usize, _elsize: usize, _cmp: qsort_arg_comparator, _arg: *mut c_void) { unimplemented!() }

// TODO(pg-port): utils/sort/sortsupport.c.
unsafe fn PrepareSortSupportFromOrderingOp(_orderingOp: Oid, _ssup: SortSupport) { unimplemented!() }
unsafe fn ApplySortComparator(_datum1: Datum, _isNull1: bool, _datum2: Datum, _isNull2: bool, _ssup: SortSupport) -> c_int { unimplemented!() }

// TODO(pg-port): utils/adt/arrayfuncs.c / array_userfuncs.c.
unsafe fn DatumGetArrayTypeP(_datum: Datum) -> *mut ArrayType { unimplemented!() }
unsafe fn construct_array(_elems: *mut Datum, _nelems: c_int, _elmtype: Oid, _elmlen: i16, _elmbyval: bool, _elmalign: c_char) -> *mut ArrayType { unimplemented!() }
unsafe fn construct_array_builtin(_elems: *mut Datum, _nelems: c_int, _elmtype: Oid) -> *mut ArrayType { unimplemented!() }
unsafe fn accumArrayResult(_astate: *mut ArrayBuildState, _dvalue: Datum, _disnull: bool, _element_type: Oid, _rcontext: MemoryContext) -> *mut ArrayBuildState { unimplemented!() }
unsafe fn makeArrayResult(_astate: *mut ArrayBuildState, _rcontext: MemoryContext) -> Datum { unimplemented!() }
unsafe fn ARR_NDIM(_a: *mut ArrayType) -> c_int { unimplemented!() }
unsafe fn ARR_HASNULL(_a: *mut ArrayType) -> bool { unimplemented!() }
unsafe fn ARR_ELEMTYPE(_a: *mut ArrayType) -> Oid { unimplemented!() }
unsafe fn ARR_DIMS(_a: *mut ArrayType) -> *mut c_int { unimplemented!() }
unsafe fn ARR_DATA_PTR(_a: *mut ArrayType) -> *mut c_char { unimplemented!() }
unsafe fn DatumGetExpandedArray(_d: Datum) -> *mut ExpandedArrayHeader { unimplemented!() }
unsafe fn deconstruct_expanded_array(_eah: *mut ExpandedArrayHeader) {}

// TODO(pg-port): utils/adt/datum.c.
unsafe fn datumCopy(_value: Datum, _typByVal: bool, _typLen: c_int) -> Datum { unimplemented!() }

// TODO(pg-port): executor/execMain.c / execExpr.c / execTuples.c.
unsafe fn CreateExecutorState() -> *mut EState { unimplemented!() }
unsafe fn FreeExecutorState(_estate: *mut EState) {}
unsafe fn GetPerTupleExprContext(_estate: *mut EState) -> *mut ExprContext { unimplemented!() }
unsafe fn ResetExprContext(_econtext: *mut ExprContext) {}
unsafe fn ExecPrepareExpr(_node: *mut Expr, _estate: *mut EState) -> *mut ExprState { unimplemented!() }
unsafe fn ExecPrepareExprList(_nodes: *mut List, _estate: *mut EState) -> *mut List { unimplemented!() }
unsafe fn ExecEvalExpr(_state: *mut ExprState, _econtext: *mut ExprContext, _isNull: *mut bool) -> Datum { unimplemented!() }
unsafe fn ExecEvalExprSwitchContext(_state: *mut ExprState, _econtext: *mut ExprContext, _isNull: *mut bool) -> Datum { unimplemented!() }
unsafe fn MakeSingleTupleTableSlot(_tupdesc: TupleDesc, _tts_ops: *const c_void) -> *mut TupleTableSlot { unimplemented!() }
unsafe fn ExecDropSingleTupleTableSlot(_slot: *mut TupleTableSlot) {}
unsafe fn ExecStoreHeapTuple(_tuple: HeapTuple, _slot: *mut TupleTableSlot, _shouldFree: bool) -> *mut TupleTableSlot { unimplemented!() }

// ecxt_scantuple field access helper (executor/execnodes.h).
// TODO(pg-port): dedup when nodes/execnodes.h lands.
unsafe fn set_ecxt_scantuple(_econtext: *mut ExprContext, _slot: *mut TupleTableSlot) {}

extern "C" {
    static TTSOpsHeapTuple: c_void;
}

// ----- Field accessor helpers for stubbed opaque types -----

unsafe fn relnamespace_of(_onerel: Relation) -> Oid { unimplemented!() } // TODO(pg-port): rd_rel->relnamespace
unsafe fn rel_oid_of(_onerel: Relation) -> Oid { unimplemented!() }      // TODO(pg-port): rd_id
unsafe fn errtable(_rel: Relation) -> c_int { 0 }                       // TODO(pg-port): utils/elog errtable

// Form_pg_type field accessors (catalog/pg_type.h).
// TODO(pg-port): dedup when catalog/pg_type.h lands.
unsafe fn form_pg_type_typlen(_t: Form_pg_type) -> i16 { unimplemented!() }
unsafe fn form_pg_type_typbyval(_t: Form_pg_type) -> bool { unimplemented!() }
unsafe fn form_pg_type_typalign(_t: Form_pg_type) -> c_char { unimplemented!() }
unsafe fn form_pg_type_typanalyze(_t: Form_pg_type) -> Oid { unimplemented!() }

// Form_pg_statistic_ext field accessors (catalog/pg_statistic_ext.h).
// TODO(pg-port): dedup when catalog/pg_statistic_ext.h lands.
unsafe fn staForm_oid(_f: Form_pg_statistic_ext) -> Oid { unimplemented!() }
unsafe fn staForm_stxnamespace(_f: Form_pg_statistic_ext) -> Oid { unimplemented!() }
unsafe fn staForm_stxname(_f: Form_pg_statistic_ext) -> *mut c_void { unimplemented!() }
unsafe fn staForm_stxkeys_dim1(_f: Form_pg_statistic_ext) -> c_int { unimplemented!() }
unsafe fn staForm_stxkeys_values(_f: Form_pg_statistic_ext, _i: c_int) -> i16 { unimplemented!() }

// ExpandedArrayHeader field accessors (utils/array.h).
// TODO(pg-port): dedup when utils/array.h lands.
unsafe fn eah_dnulls(_eah: *mut ExpandedArrayHeader) -> *mut bool { unimplemented!() }
unsafe fn eah_dvalues(_eah: *mut ExpandedArrayHeader) -> *mut Datum { unimplemented!() }

// HeapTupleData field accessors (access/htup.h).
// TODO(pg-port): dedup when access/htup.h lands.
unsafe fn set_heaptuple_t_len(_tup: *mut HeapTupleData, _len: u32) {}
unsafe fn set_heaptuple_t_tableOid(_tup: *mut HeapTupleData, _oid: Oid) {}
unsafe fn set_heaptuple_t_data(_tup: *mut HeapTupleData, _data: HeapTupleHeader) {}
unsafe fn heaptuple_t_self_ptr(_tup: *mut HeapTupleData) -> *mut c_void { unimplemented!() }

// econtext->ecxt_scantuple setter is set_ecxt_scantuple (above).
// stats->compute_stats invocation helper (commands/vacuum.h).
unsafe fn invoke_compute_stats(stats: *mut VacAttrStats, fetchfunc: AnalyzeAttrFetchFunc, samplerows: c_int, totalrows: f64) {
    if let Some(f) = (*stats).compute_stats {
        f(stats, fetchfunc, samplerows, totalrows);
    }
}

// MultiSortSupportData field accessors (statistics/extended_stats_internal.h).
// TODO(pg-port): dedup when statistics/extended_stats_internal.h MultiSortSupportData lands.
unsafe fn memoffset_MultiSortSupportData_ssup() -> usize { unimplemented!() }
unsafe fn set_mss_ndims(_mss: MultiSortSupport, _ndims: c_int) {}
unsafe fn mss_ndims(_mss: MultiSortSupport) -> c_int { unimplemented!() }
unsafe fn mss_ssup(_mss: MultiSortSupport, _i: c_int) -> SortSupport { unimplemented!() }

// SortSupportData field setters (utils/sort/sortsupport.h).
// TODO(pg-port): dedup when utils/sort/sortsupport.h SortSupportData lands.
unsafe fn set_ssup_cxt(_ssup: SortSupport, _cxt: MemoryContext) {}
unsafe fn set_ssup_collation(_ssup: SortSupport, _collation: Oid) {}
unsafe fn set_ssup_nulls_first(_ssup: SortSupport, _v: bool) {}

// attnum.h validators.
// TODO(pg-port): dedup when access/attnum.h validators land.
unsafe fn AttributeNumberIsValid(attributeNumber: c_int) -> bool { attributeNumber != InvalidAttrNumber }
unsafe fn AttrNumberIsForUserDefinedAttr(attributeNumber: AttrNumber) -> bool { attributeNumber as c_int > InvalidAttrNumber }

// ---------------------------------------------------------------------------
// Functions translated from extended_stats.c
// ---------------------------------------------------------------------------

/*
 * Compute requested extended stats, using the rows sampled for the plain
 * (single-column) stats.
 *
 * This fetches a list of stats types from pg_statistic_ext, computes the
 * requested stats, and serializes them back into the catalog.
 */
pub unsafe fn BuildRelationExtStatistics(
    onerel: Relation,
    inh: bool,
    totalrows: f64,
    numrows: c_int,
    rows: *mut HeapTuple,
    natts: c_int,
    vacattrstats: *mut *mut VacAttrStats,
) {
    let pg_stext: Relation;
    let statslist: *mut List;
    let cxt: MemoryContext;
    let oldcxt: MemoryContext;
    let mut ext_cnt: i64;

    /* Do nothing if there are no columns to analyze. */
    if natts == 0 {
        return;
    }

    /* the list of stats has to be allocated outside the memory context */
    pg_stext = table_open(StatisticExtRelationId, RowExclusiveLock);
    statslist = fetch_statentries_for_relation(pg_stext, RelationGetRelid(onerel));

    /* memory context for building each statistics object */
    cxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"BuildRelationExtStatistics".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );
    oldcxt = MemoryContextSwitchTo(cxt);

    /* report this phase */
    if statslist != NIL {
        let index: [c_int; 2] = [PROGRESS_ANALYZE_PHASE, PROGRESS_ANALYZE_EXT_STATS_TOTAL];
        let val: [i64; 2] = [
            PROGRESS_ANALYZE_PHASE_COMPUTE_EXT_STATS,
            list_length(statslist) as i64,
        ];

        pgstat_progress_update_multi_param(2, index.as_ptr(), val.as_ptr());
    }

    ext_cnt = 0;
    foreach!(lc, statslist, {
        let stat = lfirst(crate::current_cell!(lc)) as *mut StatExtEntry;
        let mut ndistinct: *mut MVNDistinct = null_mut();
        let mut dependencies: *mut MVDependencies = null_mut();
        let mut mcv: *mut MCVList = null_mut();
        let mut exprstats: Datum = 0 as Datum;
        let stats: *mut *mut VacAttrStats;
        let stattarget: c_int;
        let data: *mut StatsBuildData;

        /*
         * Check if we can build these stats based on the column analyzed. If
         * not, report this fact (except in autovacuum) and move on.
         */
        stats = lookup_var_attr_stats((*stat).columns, (*stat).exprs, natts, vacattrstats);
        if stats.is_null() {
            if !AmAutoVacuumWorkerProcess() {
                ereport!(
                    WARNING,
                    /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */                    errmsg!(
                        "statistics object \"{}.{}\" could not be computed for relation \"{}.{}\"",
                        std::ffi::CStr::from_ptr((*stat).schema).to_string_lossy(),
                        std::ffi::CStr::from_ptr((*stat).name).to_string_lossy(),
                        std::ffi::CStr::from_ptr(get_namespace_name(relnamespace_of(onerel))).to_string_lossy(),
                        std::ffi::CStr::from_ptr(RelationGetRelationName(onerel)).to_string_lossy()
                    )
                );
                /* C also: errtable(onerel) */
                let _ = errtable(onerel);
            }
            continue;
        }

        /* compute statistics target for this statistics object */
        stattarget =
            statext_compute_stattarget((*stat).stattarget, bms_num_members((*stat).columns), stats);

        /*
         * Don't rebuild statistics objects with statistics target set to 0
         * (we just leave the existing values around, just like we do for
         * regular per-column statistics).
         */
        if stattarget == 0 {
            continue;
        }

        /* evaluate expressions (if the statistics object has any) */
        data = make_build_data(onerel, stat, numrows, rows, stats, stattarget);

        /* compute statistic of each requested type */
        foreach!(lc2, (*stat).types, {
            let t: c_char = lfirst_int(crate::current_cell!(lc2)) as c_char;

            if t == STATS_EXT_NDISTINCT {
                ndistinct = statext_ndistinct_build(totalrows, data);
            } else if t == STATS_EXT_DEPENDENCIES {
                dependencies = statext_dependencies_build(data);
            } else if t == STATS_EXT_MCV {
                mcv = statext_mcv_build(data, totalrows, stattarget);
            } else if t == STATS_EXT_EXPRESSIONS {
                let exprdata: *mut AnlExprData;
                let nexprs: c_int;

                /* should not happen, thanks to checks when defining stats */
                if (*stat).exprs.is_null() {
                    elog!(ERROR, "requested expression stats, but there are no expressions");
                }

                exprdata = build_expr_data((*stat).exprs, stattarget);
                nexprs = list_length((*stat).exprs);

                compute_expr_stats(onerel, exprdata, nexprs, rows, numrows);

                exprstats = serialize_expr_stats(exprdata, nexprs);
            }
        });

        /* store the statistics in the catalog */
        statext_store((*stat).statOid, inh, ndistinct, dependencies, mcv, exprstats, stats);

        /* for reporting progress */
        ext_cnt += 1;
        pgstat_progress_update_param(PROGRESS_ANALYZE_EXT_STATS_COMPUTED, ext_cnt);

        /* free the data used for building this statistics object */
        MemoryContextReset(cxt);
    });

    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(cxt);

    list_free(statslist);

    table_close(pg_stext, RowExclusiveLock);
}

/*
 * ComputeExtStatisticsRows
 *		Compute number of rows required by extended statistics on a table.
 *
 * Computes number of rows we need to sample to build extended statistics on a
 * table. This only looks at statistics we can actually build - for example
 * when analyzing only some of the columns, this will skip statistics objects
 * that would require additional columns.
 *
 * See statext_compute_stattarget for details about how we compute the
 * statistics target for a statistics object (from the object target,
 * attribute targets and default statistics target).
 */
pub unsafe fn ComputeExtStatisticsRows(
    onerel: Relation,
    natts: c_int,
    vacattrstats: *mut *mut VacAttrStats,
) -> c_int {
    let pg_stext: Relation;
    let lstats: *mut List;
    let cxt: MemoryContext;
    let oldcxt: MemoryContext;
    let mut result: c_int = 0;

    /* If there are no columns to analyze, just return 0. */
    if natts == 0 {
        return 0;
    }

    cxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"ComputeExtStatisticsRows".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );
    oldcxt = MemoryContextSwitchTo(cxt);

    pg_stext = table_open(StatisticExtRelationId, RowExclusiveLock);
    lstats = fetch_statentries_for_relation(pg_stext, RelationGetRelid(onerel));

    foreach!(lc, lstats, {
        let stat = lfirst(crate::current_cell!(lc)) as *mut StatExtEntry;
        let stattarget: c_int;
        let stats: *mut *mut VacAttrStats;
        let nattrs: c_int = bms_num_members((*stat).columns);

        /*
         * Check if we can build this statistics object based on the columns
         * analyzed. If not, ignore it (don't report anything, we'll do that
         * during the actual build BuildRelationExtStatistics).
         */
        stats = lookup_var_attr_stats((*stat).columns, (*stat).exprs, natts, vacattrstats);

        if stats.is_null() {
            continue;
        }

        /*
         * Compute statistics target, based on what's set for the statistic
         * object itself, and for its attributes.
         */
        stattarget = statext_compute_stattarget((*stat).stattarget, nattrs, stats);

        /* Use the largest value for all statistics objects. */
        if stattarget > result {
            result = stattarget;
        }
    });

    table_close(pg_stext, RowExclusiveLock);

    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(cxt);

    /* compute sample size based on the statistics target */
    300 * result
}

/*
 * statext_compute_stattarget
 *		compute statistics target for an extended statistic
 *
 * When computing target for extended statistics objects, we consider three
 * places where the target may be set - the statistics object itself,
 * attributes the statistics object is defined on, and then the default
 * statistics target.
 *
 * First we look at what's set for the statistics object itself, using the
 * ALTER STATISTICS ... SET STATISTICS command. If we find a valid value
 * there (i.e. not -1) we're done. Otherwise we look at targets set for any
 * of the attributes the statistic is defined on, and if there are columns
 * with defined target, we use the maximum value. We do this mostly for
 * backwards compatibility, because this is what we did before having
 * statistics target for extended statistics.
 *
 * And finally, if we still don't have a statistics target, we use the value
 * set in default_statistics_target.
 */
unsafe fn statext_compute_stattarget(
    stattarget: c_int,
    nattrs: c_int,
    stats: *mut *mut VacAttrStats,
) -> c_int {
    let mut stattarget = stattarget;

    /*
     * If there's statistics target set for the statistics object, use it. It
     * may be set to 0 which disables building of that statistic.
     */
    if stattarget >= 0 {
        return stattarget;
    }

    /*
     * The target for the statistics object is set to -1, in which case we
     * look at the maximum target set for any of the attributes the object is
     * defined on.
     */
    for i in 0..nattrs {
        /* keep the maximum statistics target */
        if (**stats.add(i as usize)).attstattarget > stattarget {
            stattarget = (**stats.add(i as usize)).attstattarget;
        }
    }

    /*
     * If the value is still negative (so neither the statistics object nor
     * any of the columns have custom statistics target set), use the global
     * default target.
     */
    if stattarget < 0 {
        stattarget = default_statistics_target;
    }

    /* As this point we should have a valid statistics target. */
    Assert!((stattarget >= 0) && (stattarget <= MAX_STATISTICS_TARGET));

    stattarget
}

/*
 * statext_is_kind_built
 *		Is this stat kind built in the given pg_statistic_ext_data tuple?
 */
pub unsafe fn statext_is_kind_built(htup: HeapTuple, r#type: c_char) -> bool {
    let attnum: AttrNumber;

    match r#type {
        x if x == STATS_EXT_NDISTINCT => {
            attnum = Anum_pg_statistic_ext_data_stxdndistinct as AttrNumber;
        }
        x if x == STATS_EXT_DEPENDENCIES => {
            attnum = Anum_pg_statistic_ext_data_stxddependencies as AttrNumber;
        }
        x if x == STATS_EXT_MCV => {
            attnum = Anum_pg_statistic_ext_data_stxdmcv as AttrNumber;
        }
        x if x == STATS_EXT_EXPRESSIONS => {
            attnum = Anum_pg_statistic_ext_data_stxdexpr as AttrNumber;
        }
        _ => {
            elog!(ERROR, "unexpected statistics type requested: {}", r#type as c_int);
            unreachable!()
        }
    }

    !heap_attisnull(htup, attnum as c_int, null_mut())
}

/*
 * Return a list (of StatExtEntry) of statistics objects for the given relation.
 */
unsafe fn fetch_statentries_for_relation(pg_statext: Relation, relid: Oid) -> *mut List {
    let scan: SysScanDesc;
    let mut skey: ScanKeyData = core::mem::zeroed();
    let mut htup: HeapTuple;
    let mut result: *mut List = NIL;

    /*
     * Prepare to scan pg_statistic_ext for entries having stxrelid = this
     * rel.
     */
    ScanKeyInit(
        &mut skey,
        Anum_pg_statistic_ext_stxrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );

    scan = systable_beginscan(pg_statext, StatisticExtRelidIndexId, true, null_mut(), 1, &mut skey);

    loop {
        htup = systable_getnext(scan);
        if !HeapTupleIsValid(htup) {
            break;
        }

        let entry: *mut StatExtEntry;
        let mut datum: Datum;
        let mut isnull: bool = false;
        let mut i: c_int;
        let arr: *mut ArrayType;
        let enabled: *mut c_char;
        let staForm: Form_pg_statistic_ext;
        let mut exprs: *mut List = NIL;

        entry = palloc0(core::mem::size_of::<StatExtEntry>()) as *mut StatExtEntry;
        staForm = GETSTRUCT(htup) as Form_pg_statistic_ext;
        (*entry).statOid = staForm_oid(staForm);
        (*entry).schema = get_namespace_name(staForm_stxnamespace(staForm));
        (*entry).name = pstrdup(NameStr(staForm_stxname(staForm)));
        i = 0;
        while i < staForm_stxkeys_dim1(staForm) {
            (*entry).columns = bms_add_member((*entry).columns, staForm_stxkeys_values(staForm, i) as c_int);
            i += 1;
        }

        datum = SysCacheGetAttr(STATEXTOID, htup, Anum_pg_statistic_ext_stxstattarget, &mut isnull);
        (*entry).stattarget = if isnull { -1 } else { DatumGetInt16(datum) as c_int };

        /* decode the stxkind char array into a list of chars */
        datum = SysCacheGetAttrNotNull(STATEXTOID, htup, Anum_pg_statistic_ext_stxkind);
        arr = DatumGetArrayTypeP(datum);
        if ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != CHAROID {
            elog!(ERROR, "stxkind is not a 1-D char array");
        }
        enabled = ARR_DATA_PTR(arr);
        i = 0;
        while i < *ARR_DIMS(arr).add(0) {
            Assert!(
                (*enabled.add(i as usize) == STATS_EXT_NDISTINCT)
                    || (*enabled.add(i as usize) == STATS_EXT_DEPENDENCIES)
                    || (*enabled.add(i as usize) == STATS_EXT_MCV)
                    || (*enabled.add(i as usize) == STATS_EXT_EXPRESSIONS)
            );
            (*entry).types = lappend_int((*entry).types, *enabled.add(i as usize) as c_int);
            i += 1;
        }

        /* decode expression (if any) */
        datum = SysCacheGetAttr(STATEXTOID, htup, Anum_pg_statistic_ext_stxexprs, &mut isnull);

        if !isnull {
            let exprsString: *mut c_char;

            exprsString = TextDatumGetCString(datum);
            exprs = stringToNode(exprsString) as *mut List;

            pfree(exprsString as *mut c_void);

            /*
             * Run the expressions through eval_const_expressions. This is not
             * just an optimization, but is necessary, because the planner
             * will be comparing them to similarly-processed qual clauses, and
             * may fail to detect valid matches without this.  We must not use
             * canonicalize_qual, however, since these aren't qual
             * expressions.
             */
            exprs = eval_const_expressions(null_mut(), exprs as *mut Node) as *mut List;

            /* May as well fix opfuncids too */
            fix_opfuncids(exprs as *mut Node);
        }

        (*entry).exprs = exprs;

        result = lappend(result, entry as *mut c_void);
    }

    systable_endscan(scan);

    result
}

/*
 * examine_attribute -- pre-analysis of a single column
 *
 * Determine whether the column is analyzable; if so, create and initialize
 * a VacAttrStats struct for it.  If not, return NULL.
 */
unsafe fn examine_attribute(expr: *mut Node) -> *mut VacAttrStats {
    let typtuple: HeapTuple;
    let stats: *mut VacAttrStats;
    let mut i: c_int;
    let ok: bool;

    /*
     * Create the VacAttrStats struct.
     */
    stats = palloc0(core::mem::size_of::<VacAttrStats>()) as *mut VacAttrStats;
    (*stats).attstattarget = -1;

    /*
     * When analyzing an expression, believe the expression tree's type not
     * the column datatype --- the latter might be the opckeytype storage type
     * of the opclass, which is not interesting for our purposes.  (Note: if
     * we did anything with non-expression statistics columns, we'd need to
     * figure out where to get the correct type info from, but for now that's
     * not a problem.)	It's not clear whether anyone will care about the
     * typmod, but we store that too just in case.
     */
    (*stats).attrtypid = exprType(expr);
    (*stats).attrtypmod = exprTypmod(expr);
    (*stats).attrcollid = exprCollation(expr);

    typtuple = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum((*stats).attrtypid));
    if !HeapTupleIsValid(typtuple) {
        elog!(ERROR, "cache lookup failed for type {}", (*stats).attrtypid as c_uint);
    }
    (*stats).attrtype = GETSTRUCT(typtuple) as Form_pg_type;

    /*
     * We don't actually analyze individual attributes, so no need to set the
     * memory context.
     */
    (*stats).anl_context = null_mut();
    (*stats).tupattnum = InvalidAttrNumber;

    /*
     * The fields describing the stats->stavalues[n] element types default to
     * the type of the data being analyzed, but the type-specific typanalyze
     * function can change them if it wants to store something else.
     */
    i = 0;
    while i < STATISTIC_NUM_SLOTS {
        (*stats).statypid[i as usize] = (*stats).attrtypid;
        (*stats).statyplen[i as usize] = form_pg_type_typlen((*stats).attrtype);
        (*stats).statypbyval[i as usize] = form_pg_type_typbyval((*stats).attrtype);
        (*stats).statypalign[i as usize] = form_pg_type_typalign((*stats).attrtype);
        i += 1;
    }

    /*
     * Call the type-specific typanalyze function.  If none is specified, use
     * std_typanalyze().
     */
    if OidIsValid(form_pg_type_typanalyze((*stats).attrtype)) {
        ok = DatumGetBool(OidFunctionCall1(form_pg_type_typanalyze((*stats).attrtype), PointerGetDatum(stats as *const c_void)));
    } else {
        ok = std_typanalyze(stats);
    }

    if !ok || (*stats).compute_stats.is_none() || (*stats).minrows <= 0 {
        heap_freetuple(typtuple);
        pfree(stats as *mut c_void);
        return null_mut();
    }

    stats
}

/*
 * examine_expression -- pre-analysis of a single expression
 *
 * Determine whether the expression is analyzable; if so, create and initialize
 * a VacAttrStats struct for it.  If not, return NULL.
 */
unsafe fn examine_expression(expr: *mut Node, stattarget: c_int) -> *mut VacAttrStats {
    let typtuple: HeapTuple;
    let stats: *mut VacAttrStats;
    let mut i: c_int;
    let ok: bool;

    Assert!(!expr.is_null());

    /*
     * Create the VacAttrStats struct.
     */
    stats = palloc0(core::mem::size_of::<VacAttrStats>()) as *mut VacAttrStats;

    /*
     * We can't have statistics target specified for the expression, so we
     * could use either the default_statistics_target, or the target computed
     * for the extended statistics. The second option seems more reasonable.
     */
    (*stats).attstattarget = stattarget;

    /*
     * When analyzing an expression, believe the expression tree's type.
     */
    (*stats).attrtypid = exprType(expr);
    (*stats).attrtypmod = exprTypmod(expr);

    /*
     * We don't allow collation to be specified in CREATE STATISTICS, so we
     * have to use the collation specified for the expression. It's possible
     * to specify the collation in the expression "(col COLLATE "en_US")" in
     * which case exprCollation() does the right thing.
     */
    (*stats).attrcollid = exprCollation(expr);

    typtuple = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum((*stats).attrtypid));
    if !HeapTupleIsValid(typtuple) {
        elog!(ERROR, "cache lookup failed for type {}", (*stats).attrtypid as c_uint);
    }

    (*stats).attrtype = GETSTRUCT(typtuple) as Form_pg_type;
    (*stats).anl_context = CurrentMemoryContext; /* XXX should be using
                                                  * something else? */
    (*stats).tupattnum = InvalidAttrNumber;

    /*
     * The fields describing the stats->stavalues[n] element types default to
     * the type of the data being analyzed, but the type-specific typanalyze
     * function can change them if it wants to store something else.
     */
    i = 0;
    while i < STATISTIC_NUM_SLOTS {
        (*stats).statypid[i as usize] = (*stats).attrtypid;
        (*stats).statyplen[i as usize] = form_pg_type_typlen((*stats).attrtype);
        (*stats).statypbyval[i as usize] = form_pg_type_typbyval((*stats).attrtype);
        (*stats).statypalign[i as usize] = form_pg_type_typalign((*stats).attrtype);
        i += 1;
    }

    /*
     * Call the type-specific typanalyze function.  If none is specified, use
     * std_typanalyze().
     */
    if OidIsValid(form_pg_type_typanalyze((*stats).attrtype)) {
        ok = DatumGetBool(OidFunctionCall1(form_pg_type_typanalyze((*stats).attrtype), PointerGetDatum(stats as *const c_void)));
    } else {
        ok = std_typanalyze(stats);
    }

    if !ok || (*stats).compute_stats.is_none() || (*stats).minrows <= 0 {
        heap_freetuple(typtuple);
        pfree(stats as *mut c_void);
        return null_mut();
    }

    stats
}

/*
 * Using 'vacatts' of size 'nvacatts' as input data, return a newly-built
 * VacAttrStats array which includes only the items corresponding to
 * attributes indicated by 'attrs'.  If we don't have all of the per-column
 * stats available to compute the extended stats, then we return NULL to
 * indicate to the caller that the stats should not be built.
 */
unsafe fn lookup_var_attr_stats(
    attrs: *mut Bitmapset,
    exprs: *mut List,
    nvacatts: c_int,
    vacatts: *mut *mut VacAttrStats,
) -> *mut *mut VacAttrStats {
    let mut i: c_int = 0;
    let mut x: c_int = -1;
    let natts: c_int;
    let stats: *mut *mut VacAttrStats;

    natts = bms_num_members(attrs) + list_length(exprs);

    stats = palloc(natts as usize * core::mem::size_of::<*mut VacAttrStats>()) as *mut *mut VacAttrStats;

    /* lookup VacAttrStats info for the requested columns (same attnum) */
    loop {
        x = bms_next_member(attrs, x);
        if x < 0 {
            break;
        }

        let mut j: c_int;

        *stats.add(i as usize) = null_mut();
        j = 0;
        while j < nvacatts {
            if x == (**vacatts.add(j as usize)).tupattnum {
                *stats.add(i as usize) = *vacatts.add(j as usize);
                break;
            }
            j += 1;
        }

        if (*stats.add(i as usize)).is_null() {
            /*
             * Looks like stats were not gathered for one of the columns
             * required. We'll be unable to build the extended stats without
             * this column.
             */
            pfree(stats as *mut c_void);
            return null_mut();
        }

        i += 1;
    }

    /* also add info for expressions */
    foreach!(lc, exprs, {
        let expr = lfirst(crate::current_cell!(lc)) as *mut Node;

        *stats.add(i as usize) = examine_attribute(expr);

        /*
         * If the expression has been found as non-analyzable, give up.  We
         * will not be able to build extended stats with it.
         */
        if (*stats.add(i as usize)).is_null() {
            pfree(stats as *mut c_void);
            return null_mut();
        }

        /*
         * XXX We need tuple descriptor later, and we just grab it from
         * stats[0]->tupDesc (see e.g. statext_mcv_build). But as coded
         * examine_attribute does not set that, so just grab it from the first
         * vacatts element.
         */
        (**stats.add(i as usize)).tupDesc = (**vacatts.add(0)).tupDesc;

        i += 1;
    });

    stats
}

/*
 * statext_store
 *	Serializes the statistics and stores them into the pg_statistic_ext_data
 *	tuple.
 */
unsafe fn statext_store(
    statOid: Oid,
    inh: bool,
    ndistinct: *mut MVNDistinct,
    dependencies: *mut MVDependencies,
    mcv: *mut MCVList,
    exprs: Datum,
    stats: *mut *mut VacAttrStats,
) {
    let pg_stextdata: Relation;
    let stup: HeapTuple;
    let mut values: [Datum; Natts_pg_statistic_ext_data] = [0; Natts_pg_statistic_ext_data];
    let mut nulls: [bool; Natts_pg_statistic_ext_data] = [true; Natts_pg_statistic_ext_data];

    pg_stextdata = table_open(StatisticExtDataRelationId, RowExclusiveLock);

    /* basic info */
    values[(Anum_pg_statistic_ext_data_stxoid - 1) as usize] = ObjectIdGetDatum(statOid);
    nulls[(Anum_pg_statistic_ext_data_stxoid - 1) as usize] = false;

    values[(Anum_pg_statistic_ext_data_stxdinherit - 1) as usize] = BoolGetDatum(inh);
    nulls[(Anum_pg_statistic_ext_data_stxdinherit - 1) as usize] = false;

    /*
     * Construct a new pg_statistic_ext_data tuple, replacing the calculated
     * stats.
     */
    if !ndistinct.is_null() {
        let data: *mut bytea = statext_ndistinct_serialize(ndistinct);

        nulls[(Anum_pg_statistic_ext_data_stxdndistinct - 1) as usize] = data.is_null();
        values[(Anum_pg_statistic_ext_data_stxdndistinct - 1) as usize] = PointerGetDatum(data);
    }

    if !dependencies.is_null() {
        let data: *mut bytea = statext_dependencies_serialize(dependencies);

        nulls[(Anum_pg_statistic_ext_data_stxddependencies - 1) as usize] = data.is_null();
        values[(Anum_pg_statistic_ext_data_stxddependencies - 1) as usize] = PointerGetDatum(data);
    }
    if !mcv.is_null() {
        let data: *mut bytea = statext_mcv_serialize(mcv, stats);

        nulls[(Anum_pg_statistic_ext_data_stxdmcv - 1) as usize] = data.is_null();
        values[(Anum_pg_statistic_ext_data_stxdmcv - 1) as usize] = PointerGetDatum(data);
    }
    if exprs != 0 as Datum {
        nulls[(Anum_pg_statistic_ext_data_stxdexpr - 1) as usize] = false;
        values[(Anum_pg_statistic_ext_data_stxdexpr - 1) as usize] = exprs;
    }

    /*
     * Delete the old tuple if it exists, and insert a new one. It's easier
     * than trying to update or insert, based on various conditions.
     */
    RemoveStatisticsDataById(statOid, inh);

    /* form and insert a new tuple */
    stup = heap_form_tuple(RelationGetDescr(pg_stextdata), values.as_mut_ptr(), nulls.as_mut_ptr());
    CatalogTupleInsert(pg_stextdata, stup);

    heap_freetuple(stup);

    table_close(pg_stextdata, RowExclusiveLock);
}

/* initialize multi-dimensional sort */
pub unsafe fn multi_sort_init(ndims: c_int) -> MultiSortSupport {
    let mss: MultiSortSupport;

    Assert!(ndims >= 2);

    mss = palloc0(
        memoffset_MultiSortSupportData_ssup() + core::mem::size_of::<SortSupportData>() * ndims as usize,
    ) as MultiSortSupport;

    set_mss_ndims(mss, ndims);

    mss
}

/*
 * Prepare sort support info using the given sort operator and collation
 * at the position 'sortdim'
 */
pub unsafe fn multi_sort_add_dimension(mss: MultiSortSupport, sortdim: c_int, oper: Oid, collation: Oid) {
    let ssup: SortSupport = mss_ssup(mss, sortdim);

    set_ssup_cxt(ssup, CurrentMemoryContext);
    set_ssup_collation(ssup, collation);
    set_ssup_nulls_first(ssup, false);

    PrepareSortSupportFromOrderingOp(oper, ssup);
}

/* compare all the dimensions in the selected order */
pub unsafe fn multi_sort_compare(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    let mss: MultiSortSupport = arg as MultiSortSupport;
    let ia: *mut SortItem = a as *mut SortItem;
    let ib: *mut SortItem = b as *mut SortItem;
    let mut i: c_int;

    i = 0;
    while i < mss_ndims(mss) {
        let compare: c_int;

        compare = ApplySortComparator(
            *(*ia).values.add(i as usize),
            *(*ia).isnull.add(i as usize),
            *(*ib).values.add(i as usize),
            *(*ib).isnull.add(i as usize),
            mss_ssup(mss, i),
        );

        if compare != 0 {
            return compare;
        }
        i += 1;
    }

    /* equal by default */
    0
}

/* compare selected dimension */
pub unsafe fn multi_sort_compare_dim(dim: c_int, a: *const SortItem, b: *const SortItem, mss: MultiSortSupport) -> c_int {
    ApplySortComparator(
        *(*a).values.add(dim as usize),
        *(*a).isnull.add(dim as usize),
        *(*b).values.add(dim as usize),
        *(*b).isnull.add(dim as usize),
        mss_ssup(mss, dim),
    )
}

pub unsafe fn multi_sort_compare_dims(
    start: c_int,
    end: c_int,
    a: *const SortItem,
    b: *const SortItem,
    mss: MultiSortSupport,
) -> c_int {
    let mut dim: c_int;

    dim = start;
    while dim <= end {
        let r: c_int = ApplySortComparator(
            *(*a).values.add(dim as usize),
            *(*a).isnull.add(dim as usize),
            *(*b).values.add(dim as usize),
            *(*b).isnull.add(dim as usize),
            mss_ssup(mss, dim),
        );

        if r != 0 {
            return r;
        }
        dim += 1;
    }

    0
}

pub unsafe fn compare_scalars_simple(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    compare_datums_simple(*(a as *const Datum), *(b as *const Datum), arg as SortSupport)
}

pub unsafe fn compare_datums_simple(a: Datum, b: Datum, ssup: SortSupport) -> c_int {
    ApplySortComparator(a, false, b, false, ssup)
}

/*
 * build_attnums_array
 *		Transforms a bitmap into an array of AttrNumber values.
 *
 * This is used for extended statistics only, so all the attributes must be
 * user-defined. That means offsetting by FirstLowInvalidHeapAttributeNumber
 * is not necessary here (and when querying the bitmap).
 */
pub unsafe fn build_attnums_array(attrs: *mut Bitmapset, nexprs: c_int, numattrs: *mut c_int) -> *mut AttrNumber {
    let mut i: c_int;
    let mut j: c_int;
    let attnums: *mut AttrNumber;
    let num: c_int = bms_num_members(attrs);

    if !numattrs.is_null() {
        *numattrs = num;
    }

    /* build attnums from the bitmapset */
    attnums = palloc(core::mem::size_of::<AttrNumber>() * num as usize) as *mut AttrNumber;
    i = 0;
    j = -1;
    loop {
        j = bms_next_member(attrs, j);
        if j < 0 {
            break;
        }

        let attnum: c_int = j - nexprs;

        /*
         * Make sure the bitmap contains only user-defined attributes. As
         * bitmaps can't contain negative values, this can be violated in two
         * ways. Firstly, the bitmap might contain 0 as a member, and secondly
         * the integer value might be larger than MaxAttrNumber.
         */
        Assert!(AttributeNumberIsValid(attnum));
        Assert!(attnum <= MaxAttrNumber);
        Assert!(attnum >= (-nexprs));

        *attnums.add(i as usize) = attnum as AttrNumber;
        i += 1;

        /* protect against overflows */
        Assert!(i <= num);
    }

    attnums
}

/*
 * build_sorted_items
 *		build a sorted array of SortItem with values from rows
 *
 * Note: All the memory is allocated in a single chunk, so that the caller
 * can simply pfree the return value to release all of it.
 */
pub unsafe fn build_sorted_items(
    data: *mut StatsBuildData,
    nitems: *mut c_int,
    mss: MultiSortSupport,
    numattrs: c_int,
    attnums: *mut AttrNumber,
) -> *mut SortItem {
    let mut i: c_int;
    let mut j: c_int;
    let len: c_int;
    let mut nrows: c_int;
    let nvalues: c_int = (*data).numrows * numattrs;

    let items: *mut SortItem;
    let values: *mut Datum;
    let isnull: *mut bool;
    let mut ptr: *mut c_char;
    let typlen: *mut c_int;

    /* Compute the total amount of memory we need (both items and values). */
    len = (*data).numrows * core::mem::size_of::<SortItem>() as c_int
        + nvalues * (core::mem::size_of::<Datum>() as c_int + core::mem::size_of::<bool>() as c_int);

    /* Allocate the memory and split it into the pieces. */
    ptr = palloc0(len as usize) as *mut c_char;

    /* items to sort */
    items = ptr as *mut SortItem;
    ptr = ptr.add((*data).numrows as usize * core::mem::size_of::<SortItem>());

    /* values and null flags */
    values = ptr as *mut Datum;
    ptr = ptr.add(nvalues as usize * core::mem::size_of::<Datum>());

    isnull = ptr as *mut bool;
    ptr = ptr.add(nvalues as usize * core::mem::size_of::<bool>());

    /* make sure we consumed the whole buffer exactly */
    Assert!((ptr as isize - items as *mut c_char as isize) == len as isize);

    /* fix the pointers to Datum and bool arrays */
    nrows = 0;
    i = 0;
    while i < (*data).numrows {
        (*items.add(nrows as usize)).values = values.add((nrows * numattrs) as usize);
        (*items.add(nrows as usize)).isnull = isnull.add((nrows * numattrs) as usize);

        nrows += 1;
        i += 1;
    }

    /* build a local cache of typlen for all attributes */
    typlen = palloc(core::mem::size_of::<c_int>() * (*data).nattnums as usize) as *mut c_int;
    i = 0;
    while i < (*data).nattnums {
        *typlen.add(i as usize) = get_typlen((*(*((*data).stats as *mut *mut VacAttrStats).add(i as usize))).attrtypid) as c_int;
        i += 1;
    }

    nrows = 0;
    i = 0;
    while i < (*data).numrows {
        let mut toowide: bool = false;

        /* load the values/null flags from sample rows */
        j = 0;
        while j < numattrs {
            let mut value: Datum;
            let isnull_v: bool;
            let attlen: c_int;
            let attnum: AttrNumber = *attnums.add(j as usize);

            let mut idx: c_int;

            /* match attnum to the pre-calculated data */
            idx = 0;
            while idx < (*data).nattnums {
                if attnum == *(*data).attnums.add(idx as usize) {
                    break;
                }
                idx += 1;
            }

            Assert!(idx < (*data).nattnums);

            value = *(*(*data).values.add(idx as usize)).add(i as usize);
            isnull_v = *(*(*data).nulls.add(idx as usize)).add(i as usize);
            attlen = *typlen.add(idx as usize);

            /*
             * If this is a varlena value, check if it's too wide and if yes
             * then skip the whole item. Otherwise detoast the value.
             *
             * XXX It may happen that we've already detoasted some preceding
             * values for the current item. We don't bother to cleanup those
             * on the assumption that those are small (below WIDTH_THRESHOLD)
             * and will be discarded at the end of analyze.
             */
            if (!isnull_v) && (attlen == -1) {
                if toast_raw_datum_size(value) > WIDTH_THRESHOLD {
                    toowide = true;
                    break;
                }

                value = PointerGetDatum(pg_detoast_datum(value as *mut c_void));
            }

            *(*items.add(nrows as usize)).values.add(j as usize) = value;
            *(*items.add(nrows as usize)).isnull.add(j as usize) = isnull_v;
            j += 1;
        }

        if toowide {
            i += 1;
            continue;
        }

        nrows += 1;
        i += 1;
    }

    /* store the actual number of items (ignoring the too-wide ones) */
    *nitems = nrows;

    /* all items were too wide */
    if nrows == 0 {
        /* everything is allocated as a single chunk */
        pfree(items as *mut c_void);
        return null_mut();
    }

    /* do the sort, using the multi-sort */
    qsort_interruptible(
        items as *mut c_void,
        nrows as usize,
        core::mem::size_of::<SortItem>(),
        multi_sort_compare,
        mss as *mut c_void,
    );

    items
}

/*
 * has_stats_of_kind
 *		Check whether the list contains statistic of a given kind
 */
pub unsafe fn has_stats_of_kind(stats: *mut List, requiredkind: c_char) -> bool {
    foreach!(l, stats, {
        let stat = lfirst(crate::current_cell!(l)) as *mut StatisticExtInfo;

        if (*stat).kind == requiredkind {
            return true;
        }
    });

    false
}

/*
 * stat_find_expression
 *		Search for an expression in statistics object's list of expressions.
 *
 * Returns the index of the expression in the statistics object's list of
 * expressions, or -1 if not found.
 */
unsafe fn stat_find_expression(stat: *mut StatisticExtInfo, expr: *mut Node) -> c_int {
    let mut idx: c_int;

    idx = 0;
    foreach!(lc, (*stat).exprs, {
        let stat_expr = lfirst(crate::current_cell!(lc)) as *mut Node;

        if equal(stat_expr as *const c_void, expr as *const c_void) {
            return idx;
        }
        idx += 1;
    });

    /* Expression not found */
    -1
}

/*
 * stat_covers_expressions
 * 		Test whether a statistics object covers all expressions in a list.
 *
 * Returns true if all expressions are covered.  If expr_idxs is non-NULL, it
 * is populated with the indexes of the expressions found.
 */
unsafe fn stat_covers_expressions(stat: *mut StatisticExtInfo, exprs: *mut List, expr_idxs: *mut *mut Bitmapset) -> bool {
    foreach!(lc, exprs, {
        let expr = lfirst(crate::current_cell!(lc)) as *mut Node;
        let expr_idx: c_int;

        expr_idx = stat_find_expression(stat, expr);
        if expr_idx == -1 {
            return false;
        }

        if !expr_idxs.is_null() {
            *expr_idxs = bms_add_member(*expr_idxs, expr_idx);
        }
    });

    /* If we reach here, all expressions are covered */
    true
}

/*
 * choose_best_statistics
 *		Look for and return statistics with the specified 'requiredkind' which
 *		have keys that match at least two of the given attnums.  Return NULL if
 *		there's no match.
 *
 * The current selection criteria is very simple - we choose the statistics
 * object referencing the most attributes in covered (and still unestimated
 * clauses), breaking ties in favor of objects with fewer keys overall.
 *
 * The clause_attnums is an array of bitmaps, storing attnums for individual
 * clauses. A NULL element means the clause is either incompatible or already
 * estimated.
 *
 * XXX If multiple statistics objects tie on both criteria, then which object
 * is chosen depends on the order that they appear in the stats list. Perhaps
 * further tiebreakers are needed.
 */
pub unsafe fn choose_best_statistics(
    stats: *mut List,
    requiredkind: c_char,
    inh: bool,
    clause_attnums: *mut *mut Bitmapset,
    clause_exprs: *mut *mut List,
    nclauses: c_int,
) -> *mut StatisticExtInfo {
    let mut best_match: *mut StatisticExtInfo = null_mut();
    let mut best_num_matched: c_int = 2; /* goal #1: maximize */
    let mut best_match_keys: c_int = STATS_MAX_DIMENSIONS + 1; /* goal #2: minimize */

    foreach!(lc, stats, {
        let mut i: c_int;
        let info = lfirst(crate::current_cell!(lc)) as *mut StatisticExtInfo;
        let mut matched_attnums: *mut Bitmapset = null_mut();
        let mut matched_exprs: *mut Bitmapset = null_mut();
        let num_matched: c_int;
        let numkeys: c_int;

        /* skip statistics that are not of the correct type */
        if (*info).kind != requiredkind {
            continue;
        }

        /* skip statistics with mismatching inheritance flag */
        if (*info).inherit != inh {
            continue;
        }

        /*
         * Collect attributes and expressions in remaining (unestimated)
         * clauses fully covered by this statistic object.
         *
         * We know already estimated clauses have both clause_attnums and
         * clause_exprs set to NULL. We leave the pointers NULL if already
         * estimated, or we reset them to NULL after estimating the clause.
         */
        i = 0;
        while i < nclauses {
            let mut expr_idxs: *mut Bitmapset = null_mut();

            /* ignore incompatible/estimated clauses */
            if (*clause_attnums.add(i as usize)).is_null() && (*clause_exprs.add(i as usize)).is_null() {
                i += 1;
                continue;
            }

            /* ignore clauses that are not covered by this object */
            if !bms_is_subset(*clause_attnums.add(i as usize), (*info).keys)
                || !stat_covers_expressions(info, *clause_exprs.add(i as usize), &mut expr_idxs)
            {
                i += 1;
                continue;
            }

            /* record attnums and indexes of expressions covered */
            matched_attnums = bms_add_members(matched_attnums, *clause_attnums.add(i as usize));
            matched_exprs = bms_add_members(matched_exprs, expr_idxs);
            i += 1;
        }

        num_matched = bms_num_members(matched_attnums) + bms_num_members(matched_exprs);

        bms_free(matched_attnums);
        bms_free(matched_exprs);

        /*
         * save the actual number of keys in the stats so that we can choose
         * the narrowest stats with the most matching keys.
         */
        numkeys = bms_num_members((*info).keys) + list_length((*info).exprs);

        /*
         * Use this object when it increases the number of matched attributes
         * and expressions or when it matches the same number of attributes
         * and expressions but these stats have fewer keys than any previous
         * match.
         */
        if num_matched > best_num_matched || (num_matched == best_num_matched && numkeys < best_match_keys) {
            best_match = info;
            best_num_matched = num_matched;
            best_match_keys = numkeys;
        }
    });

    best_match
}

/*
 * statext_is_compatible_clause_internal
 *		Determines if the clause is compatible with MCV lists.
 *
 * To be compatible, the given clause must be a combination of supported
 * clauses built from Vars or sub-expressions (where a sub-expression is
 * something that exactly matches an expression found in statistics objects).
 * This function recursively examines the clause and extracts any
 * sub-expressions that will need to be matched against statistics.
 *
 * Currently, we only support the following types of clauses:
 *
 * (a) OpExprs of the form (Var/Expr op Const), or (Const op Var/Expr), where
 * the op is one of ("=", "<", ">", ">=", "<=")
 *
 * (b) (Var/Expr IS [NOT] NULL)
 *
 * (c) combinations using AND/OR/NOT
 *
 * (d) ScalarArrayOpExprs of the form (Var/Expr op ANY (Const)) or
 * (Var/Expr op ALL (Const))
 *
 * In the future, the range of supported clauses may be expanded to more
 * complex cases, for example (Var op Var).
 *
 * Arguments:
 * clause: (sub)clause to be inspected (bare clause, not a RestrictInfo)
 * relid: rel that all Vars in clause must belong to
 * *attnums: input/output parameter collecting attribute numbers of all
 *		mentioned Vars.  Note that we do not offset the attribute numbers,
 *		so we can't cope with system columns.
 * *exprs: input/output parameter collecting primitive subclauses within
 *		the clause tree
 * *leakproof: input/output parameter recording the leakproofness of the
 *		clause tree.  This should be true initially, and will be set to false
 *		if any operator function used in an OpExpr is not leakproof.
 *
 * Returns false if there is something we definitively can't handle.
 * On true return, we can proceed to match the *exprs against statistics.
 */
unsafe fn statext_is_compatible_clause_internal(
    root: *mut PlannerInfo,
    clause: *mut Node,
    relid: Index,
    attnums: *mut *mut Bitmapset,
    exprs: *mut *mut List,
    leakproof: *mut bool,
) -> bool {
    let mut clause = clause;

    /* Look inside any binary-compatible relabeling (as in examine_variable) */
    if IsA!(clause, T_RelabelType) {
        clause = (*(clause as *mut RelabelType)).arg as *mut Node;
    }

    /* plain Var references (boolean Vars or recursive checks) */
    if IsA!(clause, T_Var) {
        let var = clause as *mut Var;

        /* Ensure var is from the correct relation */
        if (*var).varno != relid as c_int {
            return false;
        }

        /* we also better ensure the Var is from the current level */
        if (*var).varlevelsup > 0 {
            return false;
        }

        /*
         * Also reject system attributes and whole-row Vars (we don't allow
         * stats on those).
         */
        if !AttrNumberIsForUserDefinedAttr((*var).varattno) {
            return false;
        }

        /* OK, record the attnum for later permissions checks. */
        *attnums = bms_add_member(*attnums, (*var).varattno as c_int);

        return true;
    }

    /* (Var/Expr op Const) or (Const op Var/Expr) */
    if is_opclause(clause as *const c_void) {
        let expr = clause as *mut OpExpr;
        let mut clause_expr: *mut Node = null_mut();

        /* Only expressions with two arguments are considered compatible. */
        if list_length((*expr).args) != 2 {
            return false;
        }

        /* Check if the expression has the right shape */
        if !examine_opclause_args((*expr).args, &mut clause_expr, null_mut(), null_mut()) {
            return false;
        }

        /*
         * If it's not one of the supported operators ("=", "<", ">", etc.),
         * just ignore the clause, as it's not compatible with MCV lists.
         *
         * This uses the function for estimating selectivity, not the operator
         * directly (a bit awkward, but well ...).
         */
        match get_oprrest((*expr).opno) {
            x if x == F_EQSEL
                || x == F_NEQSEL
                || x == F_SCALARLTSEL
                || x == F_SCALARLESEL
                || x == F_SCALARGTSEL
                || x == F_SCALARGESEL =>
            {
                /* supported, will continue with inspection of the Var/Expr */
            }
            _ => {
                /* other estimators are considered unknown/unsupported */
                return false;
            }
        }

        /* Check if the operator is leakproof */
        if *leakproof {
            *leakproof = get_func_leakproof(get_opcode((*expr).opno));
        }

        /* Check (Var op Const) or (Const op Var) clauses by recursing. */
        if IsA!(clause_expr, T_Var) {
            return statext_is_compatible_clause_internal(root, clause_expr, relid, attnums, exprs, leakproof);
        }

        /* Otherwise we have (Expr op Const) or (Const op Expr). */
        *exprs = lappend(*exprs, clause_expr as *mut c_void);
        return true;
    }

    /* Var/Expr IN Array */
    if IsA!(clause, T_ScalarArrayOpExpr) {
        let expr = clause as *mut ScalarArrayOpExpr;
        let mut clause_expr: *mut Node = null_mut();
        let mut expronleft: bool = false;

        /* Only expressions with two arguments are considered compatible. */
        if list_length((*expr).args) != 2 {
            return false;
        }

        /* Check if the expression has the right shape (one Var, one Const) */
        if !examine_opclause_args((*expr).args, &mut clause_expr, null_mut(), &mut expronleft) {
            return false;
        }

        /* We only support Var on left, Const on right */
        if !expronleft {
            return false;
        }

        /*
         * If it's not one of the supported operators ("=", "<", ">", etc.),
         * just ignore the clause, as it's not compatible with MCV lists.
         *
         * This uses the function for estimating selectivity, not the operator
         * directly (a bit awkward, but well ...).
         */
        match get_oprrest((*expr).opno) {
            x if x == F_EQSEL
                || x == F_NEQSEL
                || x == F_SCALARLTSEL
                || x == F_SCALARLESEL
                || x == F_SCALARGTSEL
                || x == F_SCALARGESEL =>
            {
                /* supported, will continue with inspection of the Var/Expr */
            }
            _ => {
                /* other estimators are considered unknown/unsupported */
                return false;
            }
        }

        /* Check if the operator is leakproof */
        if *leakproof {
            *leakproof = get_func_leakproof(get_opcode((*expr).opno));
        }

        /* Check Var IN Array clauses by recursing. */
        if IsA!(clause_expr, T_Var) {
            return statext_is_compatible_clause_internal(root, clause_expr, relid, attnums, exprs, leakproof);
        }

        /* Otherwise we have Expr IN Array. */
        *exprs = lappend(*exprs, clause_expr as *mut c_void);
        return true;
    }

    /* AND/OR/NOT clause */
    if is_andclause(clause as *const c_void) || is_orclause(clause as *const c_void) || is_notclause(clause as *const c_void) {
        /*
         * AND/OR/NOT-clauses are supported if all sub-clauses are supported
         *
         * Perhaps we could improve this by handling mixed cases, when some of
         * the clauses are supported and some are not. Selectivity for the
         * supported subclauses would be computed using extended statistics,
         * and the remaining clauses would be estimated using the traditional
         * algorithm (product of selectivities).
         *
         * It however seems overly complex, and in a way we already do that
         * because if we reject the whole clause as unsupported here, it will
         * be eventually passed to clauselist_selectivity() which does exactly
         * this (split into supported/unsupported clauses etc).
         */
        let expr = clause as *mut BoolExpr;

        foreach!(lc, (*expr).args, {
            /*
             * If we find an incompatible clause in the arguments, treat the
             * whole clause as incompatible.
             */
            if !statext_is_compatible_clause_internal(
                root,
                lfirst(crate::current_cell!(lc)) as *mut Node,
                relid,
                attnums,
                exprs,
                leakproof,
            ) {
                return false;
            }
        });

        return true;
    }

    /* Var/Expr IS NULL */
    if IsA!(clause, T_NullTest) {
        let nt = clause as *mut NullTest;

        /* Check Var IS NULL clauses by recursing. */
        if IsA!((*nt).arg, T_Var) {
            return statext_is_compatible_clause_internal(root, (*nt).arg as *mut Node, relid, attnums, exprs, leakproof);
        }

        /* Otherwise we have Expr IS NULL. */
        *exprs = lappend(*exprs, (*nt).arg as *mut c_void);
        return true;
    }

    /*
     * Treat any other expressions as bare expressions to be matched against
     * expressions in statistics objects.
     */
    *exprs = lappend(*exprs, clause as *mut c_void);
    true
}

/*
 * statext_is_compatible_clause
 *		Determines if the clause is compatible with MCV lists.
 *
 * See statext_is_compatible_clause_internal, above, for the basic rules.
 * This layer deals with RestrictInfo superstructure and applies permissions
 * checks to verify that it's okay to examine all mentioned Vars.
 *
 * Arguments:
 * clause: clause to be inspected (in RestrictInfo form)
 * relid: rel that all Vars in clause must belong to
 * *attnums: input/output parameter collecting attribute numbers of all
 *		mentioned Vars.  Note that we do not offset the attribute numbers,
 *		so we can't cope with system columns.
 * *exprs: input/output parameter collecting primitive subclauses within
 *		the clause tree
 *
 * Returns false if there is something we definitively can't handle.
 * On true return, we can proceed to match the *exprs against statistics.
 */
unsafe fn statext_is_compatible_clause(
    root: *mut PlannerInfo,
    clause: *mut Node,
    relid: Index,
    attnums: *mut *mut Bitmapset,
    exprs: *mut *mut List,
) -> bool {
    let rinfo: *mut RestrictInfo;
    let mut clause_relid: c_int = 0;
    let mut leakproof: bool;

    /*
     * Special-case handling for bare BoolExpr AND clauses, because the
     * restrictinfo machinery doesn't build RestrictInfos on top of AND
     * clauses.
     */
    if is_andclause(clause as *const c_void) {
        let expr = clause as *mut BoolExpr;

        /*
         * Check that each sub-clause is compatible.  We expect these to be
         * RestrictInfos.
         */
        foreach!(lc, (*expr).args, {
            if !statext_is_compatible_clause(root, lfirst(crate::current_cell!(lc)) as *mut Node, relid, attnums, exprs) {
                return false;
            }
        });

        return true;
    }

    /* Otherwise it must be a RestrictInfo. */
    if !IsA!(clause, T_RestrictInfo) {
        return false;
    }
    rinfo = clause as *mut RestrictInfo;

    /* Pseudoconstants are not really interesting here. */
    if (*rinfo).pseudoconstant {
        return false;
    }

    /* Clauses referencing other varnos are incompatible. */
    if !bms_get_singleton_member((*rinfo).clause_relids, &mut clause_relid) || clause_relid != relid as c_int {
        return false;
    }

    /*
     * Check the clause, determine what attributes it references, and whether
     * it includes any non-leakproof operators.
     */
    leakproof = true;
    if !statext_is_compatible_clause_internal(root, (*rinfo).clause as *mut Node, relid, attnums, exprs, &mut leakproof) {
        return false;
    }

    /*
     * If the clause includes any non-leakproof operators, check that the user
     * has permission to read all required attributes, otherwise the operators
     * might reveal values from the MCV list that the user doesn't have
     * permission to see.  We require all rows to be selectable --- there must
     * be no securityQuals from security barrier views or RLS policies.  See
     * similar code in examine_variable(), examine_simple_variable(), and
     * statistic_proc_security_check().
     *
     * Note that for an inheritance child, the permission checks are performed
     * on the inheritance root parent, and whole-table select privilege on the
     * parent doesn't guarantee that the user could read all columns of the
     * child. Therefore we must check all referenced columns.
     */
    if !leakproof {
        let mut clause_attnums: *mut Bitmapset = null_mut();
        let mut attnum: c_int = -1;

        /*
         * We have to check per-column privileges.  *attnums has the attnums
         * for individual Vars we saw, but there may also be Vars within
         * subexpressions in *exprs.  We can use pull_varattnos() to extract
         * those, but there's an impedance mismatch: attnums returned by
         * pull_varattnos() are offset by FirstLowInvalidHeapAttributeNumber,
         * while attnums within *attnums aren't.  Convert *attnums to the
         * offset style so we can combine the results.
         */
        loop {
            attnum = bms_next_member(*attnums, attnum);
            if attnum < 0 {
                break;
            }
            clause_attnums = bms_add_member(clause_attnums, attnum - FirstLowInvalidHeapAttributeNumber);
        }

        /* Now merge attnums from *exprs into clause_attnums */
        if *exprs != NIL {
            pull_varattnos(*exprs as *mut Node, relid as c_int, &mut clause_attnums);
        }

        /* Must have permission to read all rows from these columns */
        if !all_rows_selectable(root, relid, clause_attnums) {
            return false;
        }
    }

    /* If we reach here, the clause is OK */
    true
}

/*
 * statext_mcv_clauselist_selectivity
 *		Estimate clauses using the best multi-column statistics.
 *
 * Applies available extended (multi-column) statistics on a table. There may
 * be multiple applicable statistics (with respect to the clauses), in which
 * case we use greedy approach. In each round we select the best statistic on
 * a table (measured by the number of attributes extracted from the clauses
 * and covered by it), and compute the selectivity for the supplied clauses.
 * We repeat this process with the remaining clauses (if any), until none of
 * the available statistics can be used.
 *
 * One of the main challenges with using MCV lists is how to extrapolate the
 * estimate to the data not covered by the MCV list. To do that, we compute
 * not only the "MCV selectivity" (selectivities for MCV items matching the
 * supplied clauses), but also the following related selectivities:
 *
 * - simple selectivity:  Computed without extended statistics, i.e. as if the
 * columns/clauses were independent.
 *
 * - base selectivity:  Similar to simple selectivity, but is computed using
 * the extended statistic by adding up the base frequencies (that we compute
 * and store for each MCV item) of matching MCV items.
 *
 * - total selectivity: Selectivity covered by the whole MCV list.
 *
 * These are passed to mcv_combine_selectivities() which combines them to
 * produce a selectivity estimate that makes use of both per-column statistics
 * and the multi-column MCV statistics.
 *
 * 'estimatedclauses' is an input/output parameter.  We set bits for the
 * 0-based 'clauses' indexes we estimate for and also skip clause items that
 * already have a bit set.
 */
unsafe fn statext_mcv_clauselist_selectivity(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    rel: *mut RelOptInfo,
    estimatedclauses: *mut *mut Bitmapset,
    is_or: bool,
) -> Selectivity {
    let list_attnums: *mut *mut Bitmapset; /* attnums extracted from the clause */
    let list_exprs: *mut *mut List; /* expressions matched to any statistic */
    let mut listidx: c_int;
    let mut sel: Selectivity = if is_or { 0.0 } else { 1.0 };
    let rte: *mut RangeTblEntry = planner_rt_fetch((*rel).relid, root);

    /* check if there's any stats that might be useful for us. */
    if !has_stats_of_kind((*rel).statlist, STATS_EXT_MCV) {
        return sel;
    }

    list_attnums =
        palloc(core::mem::size_of::<*mut Bitmapset>() * list_length(clauses) as usize) as *mut *mut Bitmapset;

    /* expressions extracted from complex expressions */
    list_exprs = palloc(core::mem::size_of::<*mut Node>() * list_length(clauses) as usize) as *mut *mut List;

    /*
     * Pre-process the clauses list to extract the attnums and expressions
     * seen in each item.  We need to determine if there are any clauses which
     * will be useful for selectivity estimations with extended stats.  Along
     * the way we'll record all of the attnums and expressions for each clause
     * in lists which we'll reference later so we don't need to repeat the
     * same work again.
     *
     * We also skip clauses that we already estimated using different types of
     * statistics (we treat them as incompatible).
     */
    listidx = 0;
    foreach!(l, clauses, {
        let clause = lfirst(crate::current_cell!(l)) as *mut Node;
        let mut attnums: *mut Bitmapset = null_mut();
        let mut exprs: *mut List = NIL;

        if !bms_is_member(listidx, *estimatedclauses)
            && statext_is_compatible_clause(root, clause, (*rel).relid, &mut attnums, &mut exprs)
        {
            *list_attnums.add(listidx as usize) = attnums;
            *list_exprs.add(listidx as usize) = exprs;
        } else {
            *list_attnums.add(listidx as usize) = null_mut();
            *list_exprs.add(listidx as usize) = NIL;
        }

        listidx += 1;
    });

    /* apply as many extended statistics as possible */
    loop {
        let stat: *mut StatisticExtInfo;
        let mut stat_clauses: *mut List;
        let mut simple_clauses: *mut Bitmapset;

        /* find the best suited statistics object for these attnums */
        stat = choose_best_statistics(
            (*rel).statlist,
            STATS_EXT_MCV,
            rte_inh(rte),
            list_attnums,
            list_exprs,
            list_length(clauses),
        );

        /*
         * if no (additional) matching stats could be found then we've nothing
         * to do
         */
        if stat.is_null() {
            break;
        }

        /* Ensure choose_best_statistics produced an expected stats type. */
        Assert!((*stat).kind == STATS_EXT_MCV);

        /* now filter the clauses to be estimated using the selected MCV */
        stat_clauses = NIL;

        /* record which clauses are simple (single column or expression) */
        simple_clauses = null_mut();

        listidx = -1;
        foreach!(l, clauses, {
            /* Increment the index before we decide if to skip the clause. */
            listidx += 1;

            /*
             * Ignore clauses from which we did not extract any attnums or
             * expressions (this needs to be consistent with what we do in
             * choose_best_statistics).
             *
             * This also eliminates already estimated clauses - both those
             * estimated before and during applying extended statistics.
             *
             * XXX This check is needed because both bms_is_subset and
             * stat_covers_expressions return true for empty attnums and
             * expressions.
             */
            if (*list_attnums.add(listidx as usize)).is_null() && (*list_exprs.add(listidx as usize)).is_null() {
                continue;
            }

            /*
             * The clause was not estimated yet, and we've extracted either
             * attnums or expressions from it. Ignore it if it's not fully
             * covered by the chosen statistics object.
             *
             * We need to check both attributes and expressions, and reject if
             * either is not covered.
             */
            if !bms_is_subset(*list_attnums.add(listidx as usize), (*stat).keys)
                || !stat_covers_expressions(stat, *list_exprs.add(listidx as usize), null_mut())
            {
                continue;
            }

            /*
             * Now we know the clause is compatible (we have either attnums or
             * expressions extracted from it), and was not estimated yet.
             */

            /* record simple clauses (single column or expression) */
            if ((*list_attnums.add(listidx as usize)).is_null() && list_length(*list_exprs.add(listidx as usize)) == 1)
                || (*list_exprs.add(listidx as usize) == NIL
                    && bms_membership(*list_attnums.add(listidx as usize)) == BMS_SINGLETON)
            {
                simple_clauses = bms_add_member(simple_clauses, list_length(stat_clauses));
            }

            /* add clause to list and mark it as estimated */
            stat_clauses = lappend(stat_clauses, lfirst(crate::current_cell!(l)));
            *estimatedclauses = bms_add_member(*estimatedclauses, listidx);

            /*
             * Reset the pointers, so that choose_best_statistics knows this
             * clause was estimated and does not consider it again.
             */
            bms_free(*list_attnums.add(listidx as usize));
            *list_attnums.add(listidx as usize) = null_mut();

            list_free(*list_exprs.add(listidx as usize));
            *list_exprs.add(listidx as usize) = null_mut();
        });

        if is_or {
            let mut or_matches: *mut bool = null_mut();
            let mut simple_or_sel: Selectivity = 0.0;
            let mut stat_sel: Selectivity = 0.0;
            let mcv_list: *mut MCVList;

            /* Load the MCV list stored in the statistics object */
            mcv_list = statext_mcv_load((*stat).statOid, rte_inh(rte));

            /*
             * Compute the selectivity of the ORed list of clauses covered by
             * this statistics object by estimating each in turn and combining
             * them using the formula P(A OR B) = P(A) + P(B) - P(A AND B).
             * This allows us to use the multivariate MCV stats to better
             * estimate the individual terms and their overlap.
             *
             * Each time we iterate this formula, the clause "A" above is
             * equal to all the clauses processed so far, combined with "OR".
             */
            listidx = 0;
            foreach!(l, stat_clauses, {
                let clause = lfirst(crate::current_cell!(l)) as *mut Node;
                let simple_sel: Selectivity;
                let overlap_simple_sel: Selectivity;
                let mcv_sel: Selectivity;
                let mut mcv_basesel: Selectivity = 0.0;
                let mut overlap_mcvsel: Selectivity = 0.0;
                let mut overlap_basesel: Selectivity = 0.0;
                let mut mcv_totalsel: Selectivity = 0.0;
                let clause_sel: Selectivity;
                let overlap_sel: Selectivity;

                /*
                 * "Simple" selectivity of the next clause and its overlap
                 * with any of the previous clauses.  These are our initial
                 * estimates of P(B) and P(A AND B), assuming independence of
                 * columns/clauses.
                 */
                simple_sel = clause_selectivity_ext(root, clause, varRelid, jointype, sjinfo, false);

                overlap_simple_sel = simple_or_sel * simple_sel;

                /*
                 * New "simple" selectivity of all clauses seen so far,
                 * assuming independence.
                 */
                simple_or_sel += simple_sel - overlap_simple_sel;
                CLAMP_PROBABILITY(&mut simple_or_sel);

                /*
                 * Multi-column estimate of this clause using MCV statistics,
                 * along with base and total selectivities, and corresponding
                 * selectivities for the overlap term P(A AND B).
                 */
                mcv_sel = mcv_clause_selectivity_or(
                    root,
                    stat,
                    mcv_list,
                    clause,
                    &mut or_matches,
                    &mut mcv_basesel,
                    &mut overlap_mcvsel,
                    &mut overlap_basesel,
                    &mut mcv_totalsel,
                );

                /*
                 * Combine the simple and multi-column estimates.
                 *
                 * If this clause is a simple single-column clause, then we
                 * just use the simple selectivity estimate for it, since the
                 * multi-column statistics are unlikely to improve on that
                 * (and in fact could make it worse).  For the overlap, we
                 * always make use of the multi-column statistics.
                 */
                if bms_is_member(listidx, simple_clauses) {
                    clause_sel = simple_sel;
                } else {
                    clause_sel = mcv_combine_selectivities(simple_sel, mcv_sel, mcv_basesel, mcv_totalsel);
                }

                overlap_sel = mcv_combine_selectivities(overlap_simple_sel, overlap_mcvsel, overlap_basesel, mcv_totalsel);

                /* Factor these into the result for this statistics object */
                stat_sel += clause_sel - overlap_sel;
                CLAMP_PROBABILITY(&mut stat_sel);

                listidx += 1;
            });

            /*
             * Factor the result for this statistics object into the overall
             * result.  We treat the results from each separate statistics
             * object as independent of one another.
             */
            sel = sel + stat_sel - sel * stat_sel;
        } else
        /* Implicitly-ANDed list of clauses */
        {
            let simple_sel: Selectivity;
            let mcv_sel: Selectivity;
            let mut mcv_basesel: Selectivity = 0.0;
            let mut mcv_totalsel: Selectivity = 0.0;
            let stat_sel: Selectivity;

            /*
             * "Simple" selectivity, i.e. without any extended statistics,
             * essentially assuming independence of the columns/clauses.
             */
            simple_sel = clauselist_selectivity_ext(root, stat_clauses, varRelid, jointype, sjinfo, false);

            /*
             * Multi-column estimate using MCV statistics, along with base and
             * total selectivities.
             */
            mcv_sel = mcv_clauselist_selectivity(
                root,
                stat,
                stat_clauses,
                varRelid,
                jointype,
                sjinfo,
                rel,
                &mut mcv_basesel,
                &mut mcv_totalsel,
            );

            /* Combine the simple and multi-column estimates. */
            stat_sel = mcv_combine_selectivities(simple_sel, mcv_sel, mcv_basesel, mcv_totalsel);

            /* Factor this into the overall result */
            sel *= stat_sel;
        }
    }

    sel
}

/*
 * statext_clauselist_selectivity
 *		Estimate clauses using the best multi-column statistics.
 */
pub unsafe fn statext_clauselist_selectivity(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    rel: *mut RelOptInfo,
    estimatedclauses: *mut *mut Bitmapset,
    is_or: bool,
) -> Selectivity {
    let mut sel: Selectivity;

    /* First, try estimating clauses using a multivariate MCV list. */
    sel = statext_mcv_clauselist_selectivity(root, clauses, varRelid, jointype, sjinfo, rel, estimatedclauses, is_or);

    /*
     * Functional dependencies only work for clauses connected by AND, so for
     * OR clauses we're done.
     */
    if is_or {
        return sel;
    }

    /*
     * Then, apply functional dependencies on the remaining clauses by calling
     * dependencies_clauselist_selectivity.  Pass 'estimatedclauses' so the
     * function can properly skip clauses already estimated above.
     *
     * The reasoning for applying dependencies last is that the more complex
     * stats can track more complex correlations between the attributes, and
     * so may be considered more reliable.
     *
     * For example, MCV list can give us an exact selectivity for values in
     * two columns, while functional dependencies can only provide information
     * about the overall strength of the dependency.
     */
    sel *= dependencies_clauselist_selectivity(root, clauses, varRelid, jointype, sjinfo, rel, estimatedclauses);

    sel
}

/*
 * examine_opclause_args
 *		Split an operator expression's arguments into Expr and Const parts.
 *
 * Attempts to match the arguments to either (Expr op Const) or (Const op
 * Expr), possibly with a RelabelType on top. When the expression matches this
 * form, returns true, otherwise returns false.
 *
 * Optionally returns pointers to the extracted Expr/Const nodes, when passed
 * non-null pointers (exprp, cstp and expronleftp). The expronleftp flag
 * specifies on which side of the operator we found the expression node.
 */
pub unsafe fn examine_opclause_args(
    args: *mut List,
    exprp: *mut *mut Node,
    cstp: *mut *mut Const,
    expronleftp: *mut bool,
) -> bool {
    let expr: *mut Node;
    let cst: *mut Const;
    let expronleft: bool;
    let mut leftop: *mut Node;
    let mut rightop: *mut Node;

    /* enforced by statext_is_compatible_clause_internal */
    Assert!(list_length(args) == 2);

    leftop = linitial(args) as *mut Node;
    rightop = lsecond(args) as *mut Node;

    /* strip RelabelType from either side of the expression */
    if IsA!(leftop, T_RelabelType) {
        leftop = (*(leftop as *mut RelabelType)).arg as *mut Node;
    }

    if IsA!(rightop, T_RelabelType) {
        rightop = (*(rightop as *mut RelabelType)).arg as *mut Node;
    }

    if IsA!(rightop, T_Const) {
        expr = leftop;
        cst = rightop as *mut Const;
        expronleft = true;
    } else if IsA!(leftop, T_Const) {
        expr = rightop;
        cst = leftop as *mut Const;
        expronleft = false;
    } else {
        return false;
    }

    /* return pointers to the extracted parts if requested */
    if !exprp.is_null() {
        *exprp = expr;
    }

    if !cstp.is_null() {
        *cstp = cst;
    }

    if !expronleftp.is_null() {
        *expronleftp = expronleft;
    }

    true
}

// RangeTblEntry field accessor (nodes/parsenodes.h).
// TODO(pg-port): dedup when nodes/parsenodes.h RangeTblEntry lands.
unsafe fn rte_inh(_rte: *mut RangeTblEntry) -> bool { unimplemented!() }

/*
 * Compute statistics about expressions of a relation.
 */
unsafe fn compute_expr_stats(onerel: Relation, exprdata: *mut AnlExprData, nexprs: c_int, rows: *mut HeapTuple, numrows: c_int) {
    let expr_context: MemoryContext;
    let old_context: MemoryContext;
    let mut ind: c_int;
    let mut i: c_int;

    expr_context = AllocSetContextCreate!(CurrentMemoryContext, c"Analyze Expression".as_ptr(), ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(expr_context);

    ind = 0;
    while ind < nexprs {
        let thisdata: *mut AnlExprData = exprdata.add(ind as usize);
        let stats: *mut VacAttrStats = (*thisdata).vacattrstat;
        let expr: *mut Node = (*thisdata).expr;
        let slot: *mut TupleTableSlot;
        let estate: *mut EState;
        let econtext: *mut ExprContext;
        let exprvals: *mut Datum;
        let exprnulls: *mut bool;
        let exprstate: *mut ExprState;
        let mut tcnt: c_int;

        /* Are we still in the main context? */
        Assert!(CurrentMemoryContext == expr_context);

        /*
         * Need an EState for evaluation of expressions.  Create it in the
         * per-expression context to be sure it gets cleaned up at the bottom
         * of the loop.
         */
        estate = CreateExecutorState();
        econtext = GetPerTupleExprContext(estate);

        /* Set up expression evaluation state */
        exprstate = ExecPrepareExpr(expr as *mut Expr, estate);

        /* Need a slot to hold the current heap tuple, too */
        slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel), &TTSOpsHeapTuple as *const c_void);

        /* Arrange for econtext's scan tuple to be the tuple under test */
        set_ecxt_scantuple(econtext, slot);

        /* Compute and save expression values */
        exprvals = palloc(numrows as usize * core::mem::size_of::<Datum>()) as *mut Datum;
        exprnulls = palloc(numrows as usize * core::mem::size_of::<bool>()) as *mut bool;

        tcnt = 0;
        i = 0;
        while i < numrows {
            let datum: Datum;
            let mut isnull: bool = false;

            /*
             * Reset the per-tuple context each time, to reclaim any cruft
             * left behind by evaluating the statistics expressions.
             */
            ResetExprContext(econtext);

            /* Set up for expression evaluation */
            ExecStoreHeapTuple(*rows.add(i as usize), slot, false);

            /*
             * Evaluate the expression. We do this in the per-tuple context so
             * as not to leak memory, and then copy the result into the
             * context created at the beginning of this function.
             */
            datum = ExecEvalExprSwitchContext(exprstate, GetPerTupleExprContext(estate), &mut isnull);
            if isnull {
                *exprvals.add(tcnt as usize) = 0 as Datum;
                *exprnulls.add(tcnt as usize) = true;
            } else {
                /* Make sure we copy the data into the context. */
                Assert!(CurrentMemoryContext == expr_context);

                *exprvals.add(tcnt as usize) =
                    datumCopy(datum, form_pg_type_typbyval((*stats).attrtype), form_pg_type_typlen((*stats).attrtype) as c_int);
                *exprnulls.add(tcnt as usize) = false;
            }

            tcnt += 1;
            i += 1;
        }

        /*
         * Now we can compute the statistics for the expression columns.
         *
         * XXX Unlike compute_index_stats we don't need to switch and reset
         * memory contexts here, because we're only computing stats for a
         * single expression (and not iterating over many indexes), so we just
         * do it in expr_context. Note that compute_stats copies the result
         * into stats->anl_context, so it does not disappear.
         */
        if tcnt > 0 {
            let aopt: *mut AttributeOpts = get_attribute_options(rel_oid_of(onerel), (*stats).tupattnum);

            (*stats).exprvals = exprvals;
            (*stats).exprnulls = exprnulls;
            (*stats).rowstride = 1;
            invoke_compute_stats(stats, Some(expr_fetch_func), tcnt, tcnt as f64);

            /*
             * If the n_distinct option is specified, it overrides the above
             * computation.
             */
            if !aopt.is_null() && (*aopt).n_distinct != 0.0 {
                (*stats).stadistinct = (*aopt).n_distinct as f32;
            }
        }

        /* And clean up */
        MemoryContextSwitchTo(expr_context);

        ExecDropSingleTupleTableSlot(slot);
        FreeExecutorState(estate);
        MemoryContextReset(expr_context);

        ind += 1;
    }

    MemoryContextSwitchTo(old_context);
    MemoryContextDelete(expr_context);
}

/*
 * Fetch function for analyzing statistics object expressions.
 *
 * We have not bothered to construct tuples from the data, instead the data
 * is just in Datum arrays.
 */
unsafe fn expr_fetch_func(stats: VacAttrStatsP, rownum: c_int, isNull: *mut bool) -> Datum {
    let i: c_int;

    /* exprvals and exprnulls are already offset for proper column */
    i = rownum * (*stats).rowstride;
    *isNull = *(*stats).exprnulls.add(i as usize);
    *(*stats).exprvals.add(i as usize)
}

/*
 * Build analyze data for a list of expressions. As this is not tied
 * directly to a relation (table or index), we have to fake some of
 * the fields in examine_expression().
 */
unsafe fn build_expr_data(exprs: *mut List, stattarget: c_int) -> *mut AnlExprData {
    let mut idx: c_int;
    let nexprs: c_int = list_length(exprs);
    let exprdata: *mut AnlExprData;

    exprdata = palloc0(nexprs as usize * core::mem::size_of::<AnlExprData>()) as *mut AnlExprData;

    idx = 0;
    foreach!(lc, exprs, {
        let expr = lfirst(crate::current_cell!(lc)) as *mut Node;
        let thisdata: *mut AnlExprData = exprdata.add(idx as usize);

        (*thisdata).expr = expr;
        (*thisdata).vacattrstat = examine_expression(expr, stattarget);
        idx += 1;
    });

    exprdata
}

/* form an array of pg_statistic rows (per update_attstats) */
unsafe fn serialize_expr_stats(exprdata: *mut AnlExprData, nexprs: c_int) -> Datum {
    let mut exprno: c_int;
    let typOid: Oid;
    let sd: Relation;

    let mut astate: *mut ArrayBuildState = null_mut();

    sd = table_open(StatisticRelationId, RowExclusiveLock);

    /* lookup OID of composite type for pg_statistic */
    typOid = get_rel_type_id(StatisticRelationId);
    if !OidIsValid(typOid) {
        ereport!(
            ERROR,
            errmsg!("relation \"{}\" does not have a composite type", "pg_statistic")
        );
        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        let _ = ERRCODE_WRONG_OBJECT_TYPE;
    }

    exprno = 0;
    while exprno < nexprs {
        let mut i: c_int;
        let mut k: c_int;
        let stats: *mut VacAttrStats = (*exprdata.add(exprno as usize)).vacattrstat;

        let mut values: [Datum; Natts_pg_statistic] = [0; Natts_pg_statistic];
        let mut nulls: [bool; Natts_pg_statistic] = [false; Natts_pg_statistic];
        let stup: HeapTuple;

        if !(*stats).stats_valid {
            astate = accumArrayResult(astate, 0 as Datum, true, typOid, CurrentMemoryContext);
            exprno += 1;
            continue;
        }

        /*
         * Construct a new pg_statistic tuple
         */
        i = 0;
        while i < Natts_pg_statistic as c_int {
            nulls[i as usize] = false;
            i += 1;
        }

        values[(Anum_pg_statistic_starelid - 1) as usize] = ObjectIdGetDatum(InvalidOid);
        values[(Anum_pg_statistic_staattnum - 1) as usize] = Int16GetDatum(InvalidAttrNumber as i16);
        values[(Anum_pg_statistic_stainherit - 1) as usize] = BoolGetDatum(false);
        values[(Anum_pg_statistic_stanullfrac - 1) as usize] = Float4GetDatum((*stats).stanullfrac);
        values[(Anum_pg_statistic_stawidth - 1) as usize] = Int32GetDatum((*stats).stawidth);
        values[(Anum_pg_statistic_stadistinct - 1) as usize] = Float4GetDatum((*stats).stadistinct);
        i = Anum_pg_statistic_stakind1 - 1;
        k = 0;
        while k < STATISTIC_NUM_SLOTS {
            values[i as usize] = Int16GetDatum((*stats).stakind[k as usize]); /* stakindN */
            i += 1;
            k += 1;
        }
        i = Anum_pg_statistic_staop1 - 1;
        k = 0;
        while k < STATISTIC_NUM_SLOTS {
            values[i as usize] = ObjectIdGetDatum((*stats).staop[k as usize]); /* staopN */
            i += 1;
            k += 1;
        }
        i = Anum_pg_statistic_stacoll1 - 1;
        k = 0;
        while k < STATISTIC_NUM_SLOTS {
            values[i as usize] = ObjectIdGetDatum((*stats).stacoll[k as usize]); /* stacollN */
            i += 1;
            k += 1;
        }
        i = Anum_pg_statistic_stanumbers1 - 1;
        k = 0;
        while k < STATISTIC_NUM_SLOTS {
            let nnum: c_int = (*stats).numnumbers[k as usize];

            if nnum > 0 {
                let mut n: c_int;
                let numdatums: *mut Datum = palloc(nnum as usize * core::mem::size_of::<Datum>()) as *mut Datum;
                let arry: *mut ArrayType;

                n = 0;
                while n < nnum {
                    *numdatums.add(n as usize) = Float4GetDatum(*(*stats).stanumbers[k as usize].add(n as usize));
                    n += 1;
                }
                arry = construct_array_builtin(numdatums, nnum, FLOAT4OID);
                values[i as usize] = PointerGetDatum(arry as *const c_void); /* stanumbersN */
                i += 1;
            } else {
                nulls[i as usize] = true;
                values[i as usize] = 0 as Datum;
                i += 1;
            }
            k += 1;
        }
        i = Anum_pg_statistic_stavalues1 - 1;
        k = 0;
        while k < STATISTIC_NUM_SLOTS {
            if (*stats).numvalues[k as usize] > 0 {
                let arry: *mut ArrayType;

                arry = construct_array(
                    (*stats).stavalues[k as usize],
                    (*stats).numvalues[k as usize],
                    (*stats).statypid[k as usize],
                    (*stats).statyplen[k as usize],
                    (*stats).statypbyval[k as usize],
                    (*stats).statypalign[k as usize],
                );
                values[i as usize] = PointerGetDatum(arry as *const c_void); /* stavaluesN */
                i += 1;
            } else {
                nulls[i as usize] = true;
                values[i as usize] = 0 as Datum;
                i += 1;
            }
            k += 1;
        }

        stup = heap_form_tuple(RelationGetDescr(sd), values.as_mut_ptr(), nulls.as_mut_ptr());

        astate = accumArrayResult(
            astate,
            heap_copy_tuple_as_datum(stup, RelationGetDescr(sd)),
            false,
            typOid,
            CurrentMemoryContext,
        );

        exprno += 1;
    }

    table_close(sd, RowExclusiveLock);

    makeArrayResult(astate, CurrentMemoryContext)
}

/*
 * Loads pg_statistic record from expression statistics for expression
 * identified by the supplied index.
 *
 * Returns the pg_statistic record found, or NULL if there is no statistics
 * data to use.
 */
pub unsafe fn statext_expressions_load(stxoid: Oid, inh: bool, idx: c_int) -> HeapTuple {
    let mut isnull: bool = false;
    let value: Datum;
    let htup: HeapTuple;
    let eah: *mut ExpandedArrayHeader;
    let td: HeapTupleHeader;
    let mut tmptup: HeapTupleData = core::mem::zeroed();
    let tup: HeapTuple;

    htup = SearchSysCache2(STATEXTDATASTXOID, ObjectIdGetDatum(stxoid), BoolGetDatum(inh));
    if !HeapTupleIsValid(htup) {
        elog!(ERROR, "cache lookup failed for statistics object {}", stxoid as c_uint);
    }

    value = SysCacheGetAttr(STATEXTDATASTXOID, htup, Anum_pg_statistic_ext_data_stxdexpr, &mut isnull);
    if isnull {
        elog!(
            ERROR,
            "requested statistics kind \"{}\" is not yet built for statistics object {}",
            STATS_EXT_EXPRESSIONS as u8 as char,
            stxoid as c_uint
        );
    }

    eah = DatumGetExpandedArray(value);

    deconstruct_expanded_array(eah);

    if !eah_dnulls(eah).is_null() && *eah_dnulls(eah).add(idx as usize) {
        /* No data found for this expression, give up. */
        ReleaseSysCache(htup);
        return null_mut();
    }

    td = DatumGetHeapTupleHeader(*eah_dvalues(eah).add(idx as usize));

    /* Build a temporary HeapTuple control structure */
    set_heaptuple_t_len(&mut tmptup, HeapTupleHeaderGetDatumLength(td));
    ItemPointerSetInvalid(heaptuple_t_self_ptr(&mut tmptup));
    set_heaptuple_t_tableOid(&mut tmptup, InvalidOid);
    set_heaptuple_t_data(&mut tmptup, td);

    tup = heap_copytuple(&mut tmptup);

    ReleaseSysCache(htup);

    tup
}

/*
 * Evaluate the expressions, so that we can use the results to build
 * all the requested statistics types. This matters especially for
 * expensive expressions, of course.
 */
unsafe fn make_build_data(
    rel: Relation,
    stat: *mut StatExtEntry,
    numrows: c_int,
    rows: *mut HeapTuple,
    stats: *mut *mut VacAttrStats,
    stattarget: c_int,
) -> *mut StatsBuildData {
    /* evaluated expressions */
    let result: *mut StatsBuildData;
    let mut ptr: *mut c_char;
    let mut len: Size;

    let mut i: c_int;
    let mut k: c_int;
    let mut idx: c_int;
    let slot: *mut TupleTableSlot;
    let estate: *mut EState;
    let econtext: *mut ExprContext;
    let exprstates: *mut List;
    let nkeys: c_int = bms_num_members((*stat).columns) + list_length((*stat).exprs);

    /* allocate everything as a single chunk, so we can free it easily */
    len = MAXALIGN(core::mem::size_of::<StatsBuildData>());
    len += MAXALIGN(core::mem::size_of::<AttrNumber>() * nkeys as usize); /* attnums */
    len += MAXALIGN(core::mem::size_of::<*mut VacAttrStats>() * nkeys as usize); /* stats */

    /* values */
    len += MAXALIGN(core::mem::size_of::<*mut Datum>() * nkeys as usize);
    len += nkeys as usize * MAXALIGN(core::mem::size_of::<Datum>() * numrows as usize);

    /* nulls */
    len += MAXALIGN(core::mem::size_of::<*mut bool>() * nkeys as usize);
    len += nkeys as usize * MAXALIGN(core::mem::size_of::<bool>() * numrows as usize);

    ptr = palloc(len) as *mut c_char;

    /* set the pointers */
    result = ptr as *mut StatsBuildData;
    ptr = ptr.add(MAXALIGN(core::mem::size_of::<StatsBuildData>()));

    /* attnums */
    (*result).attnums = ptr as *mut AttrNumber;
    ptr = ptr.add(MAXALIGN(core::mem::size_of::<AttrNumber>() * nkeys as usize));

    /* stats */
    (*result).stats = ptr as *mut *mut c_void;
    ptr = ptr.add(MAXALIGN(core::mem::size_of::<*mut VacAttrStats>() * nkeys as usize));

    /* values */
    (*result).values = ptr as *mut *mut Datum;
    ptr = ptr.add(MAXALIGN(core::mem::size_of::<*mut Datum>() * nkeys as usize));

    /* nulls */
    (*result).nulls = ptr as *mut *mut bool;
    ptr = ptr.add(MAXALIGN(core::mem::size_of::<*mut bool>() * nkeys as usize));

    i = 0;
    while i < nkeys {
        *(*result).values.add(i as usize) = ptr as *mut Datum;
        ptr = ptr.add(MAXALIGN(core::mem::size_of::<Datum>() * numrows as usize));

        *(*result).nulls.add(i as usize) = ptr as *mut bool;
        ptr = ptr.add(MAXALIGN(core::mem::size_of::<bool>() * numrows as usize));
        i += 1;
    }

    Assert!((ptr as isize - result as *mut c_char as isize) == len as isize);

    /* we have it allocated, so let's fill the values */
    (*result).nattnums = nkeys;
    (*result).numrows = numrows;

    /* fill the attribute info - first attributes, then expressions */
    idx = 0;
    k = -1;
    loop {
        k = bms_next_member((*stat).columns, k);
        if k < 0 {
            break;
        }
        *(*result).attnums.add(idx as usize) = k as AttrNumber;
        *((*result).stats as *mut *mut VacAttrStats).add(idx as usize) = *stats.add(idx as usize);

        idx += 1;
    }

    k = -1;
    foreach!(lc, (*stat).exprs, {
        let expr = lfirst(crate::current_cell!(lc)) as *mut Node;

        *(*result).attnums.add(idx as usize) = k as AttrNumber;
        *((*result).stats as *mut *mut VacAttrStats).add(idx as usize) = examine_expression(expr, stattarget);

        idx += 1;
        k -= 1;
    });

    /* first extract values for all the regular attributes */
    i = 0;
    while i < numrows {
        idx = 0;
        k = -1;
        loop {
            k = bms_next_member((*stat).columns, k);
            if k < 0 {
                break;
            }
            *(*(*result).values.add(idx as usize)).add(i as usize) = heap_getattr(
                *rows.add(i as usize),
                k,
                (*(*((*result).stats as *mut *mut VacAttrStats).add(idx as usize))).tupDesc,
                (*(*result).nulls.add(idx as usize)).add(i as usize),
            );

            idx += 1;
        }
        i += 1;
    }

    /* Need an EState for evaluation expressions. */
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);

    /* Need a slot to hold the current heap tuple, too */
    slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), &TTSOpsHeapTuple as *const c_void);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    set_ecxt_scantuple(econtext, slot);

    /* Set up expression evaluation state */
    exprstates = ExecPrepareExprList((*stat).exprs, estate);

    i = 0;
    while i < numrows {
        /*
         * Reset the per-tuple context each time, to reclaim any cruft left
         * behind by evaluating the statistics object expressions.
         */
        ResetExprContext(econtext);

        /* Set up for expression evaluation */
        ExecStoreHeapTuple(*rows.add(i as usize), slot, false);

        idx = bms_num_members((*stat).columns);
        foreach!(lc, exprstates, {
            let datum: Datum;
            let mut isnull: bool = false;
            let exprstate = lfirst(crate::current_cell!(lc)) as *mut ExprState;

            /*
             * XXX This probably leaks memory. Maybe we should use
             * ExecEvalExprSwitchContext but then we need to copy the result
             * somewhere else.
             */
            datum = ExecEvalExpr(exprstate, GetPerTupleExprContext(estate), &mut isnull);
            if isnull {
                *(*(*result).values.add(idx as usize)).add(i as usize) = 0 as Datum;
                *(*(*result).nulls.add(idx as usize)).add(i as usize) = true;
            } else {
                *(*(*result).values.add(idx as usize)).add(i as usize) = datum;
                *(*(*result).nulls.add(idx as usize)).add(i as usize) = false;
            }

            idx += 1;
        });
        i += 1;
    }

    ExecDropSingleTupleTableSlot(slot);
    FreeExecutorState(estate);

    result
}
