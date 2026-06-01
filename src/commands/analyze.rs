/*-------------------------------------------------------------------------
 *
 * analyze.rs
 *   the Postgres statistics generator
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *   src/backend/commands/analyze.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_return)]

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};
use std::mem::size_of;

use crate::access::htup_details::HeapTupleData;
type HeapTuple = *mut HeapTupleData;

use crate::access::table::table::{table_open, table_close};
use crate::access::table::tableam::table_slot_create;
use crate::access::transam::InvalidTransactionId;
use crate::access::common::tupdesc::TupleDesc;
use crate::storage::block::BlockNumber;
use core::ffi::CStr;

use crate::nodes::nodes::Node;
use crate::nodes::execnodes::{IndexInfo, INDEX_MAX_KEYS, EState, ExprContext, TupleTableSlot};
use crate::nodes::pg_list::{List, ListCell, list_length, list_head, list_free, lnext, lfirst,
    lfirst_oid, NIL};
use crate::foreach;

/* --------------------------------------------------------------------------
 * Local type stubs for unported dependencies
 * -------------------------------------------------------------------------- */

// Relation
type RelationData = crate::utils::rel::RelationData;
type Relation = *mut RelationData;

// VacAttrStats / VacAttrStatsP  TODO(pg-port)
#[repr(C)] pub struct VacAttrStats { _opaque: [u8; 0] }
type VacAttrStatsP = *mut VacAttrStats;

// AcquireSampleRowsFunc  TODO(pg-port)
type AcquireSampleRowsFunc = unsafe extern "C" fn(
    onerel: Relation,
    elevel: c_int,
    rows: *mut HeapTuple,
    targrows: c_int,
    totalrows: *mut f64,
    totaldeadrows: *mut f64,
) -> c_int;

// VacuumParams  TODO(pg-port)
#[repr(C)] pub struct VacuumParams { _opaque: [u8; 0] }

// RangeVar
use crate::nodes::primnodes::RangeVar;

// BufferAccessStrategy  TODO(pg-port)
#[repr(C)] pub struct BufferAccessStrategyData { _opaque: [u8; 0] }
type BufferAccessStrategy = *mut BufferAccessStrategyData;

// MemoryContext  TODO(pg-port)
type MemoryContext = *mut c_void;

// IndexBulkDeleteResult  TODO(pg-port)
#[repr(C)] pub struct IndexBulkDeleteResult { _opaque: [u8; 0] }

// IndexVacuumInfo  TODO(pg-port)
#[repr(C)] pub struct IndexVacuumInfo { _opaque: [u8; 0] }

// TupleConversionMap  TODO(pg-port)
#[repr(C)] pub struct TupleConversionMap { _opaque: [u8; 0] }

// TableScanDesc  TODO(pg-port)
#[repr(C)] pub struct TableScanDescData { _opaque: [u8; 0] }
type TableScanDesc = *mut TableScanDescData;

// CHECK_FOR_INTERRUPTS  TODO(pg-port): real one lives in miscadmin.rs
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {};
}

// table_beginscan_analyze / table_scan_analyze_next_block /
// table_scan_analyze_next_tuple / table_endscan  TODO(pg-port):
// these are static inline wrappers in access/tableam.h, not yet ported.
unsafe fn table_beginscan_analyze(_relation: Relation) -> TableScanDesc {
    core::ptr::null_mut()
}
unsafe fn table_scan_analyze_next_block(_scan: TableScanDesc,
                                        _stream: *mut ReadStream) -> bool {
    false
}
unsafe fn table_scan_analyze_next_tuple(_scan: TableScanDesc,
                                        _oldest_xmin: TransactionId,
                                        _liverows: *mut f64,
                                        _deadrows: *mut f64,
                                        _slot: *mut TupleTableSlot) -> bool {
    false
}
unsafe fn table_endscan(_scan: TableScanDesc) {}

// ReadStream  TODO(pg-port)
#[repr(C)] pub struct ReadStream { _opaque: [u8; 0] }

// BlockSamplerData / ReservoirStateData  TODO(pg-port)
#[repr(C)] pub struct BlockSamplerData { _opaque: [u8; 0] }
#[repr(C)] pub struct ReservoirStateData { _opaque: [u8; 0] }

// PGRUsage  TODO(pg-port)
#[repr(C)] pub struct PGRUsage { _opaque: [u8; 0] }

// WalUsage / BufferUsage / PgStat_Counter  TODO(pg-port)
#[repr(C)] pub struct WalUsage { _opaque: [u8; 0] }
#[repr(C)] pub struct BufferUsage { _opaque: [u8; 0] }
type PgStat_Counter = i64;

// TimestampTz  TODO(pg-port)
type TimestampTz = i64;

// ExprState  TODO(pg-port)
#[repr(C)] pub struct ExprState { _opaque: [u8; 0] }

// CatalogIndexState  TODO(pg-port)
#[repr(C)] pub struct CatalogIndexStateData { _opaque: [u8; 0] }
type CatalogIndexState = *mut CatalogIndexStateData;

// ArrayType  TODO(pg-port)
#[repr(C)] pub struct ArrayType { _opaque: [u8; 0] }

// SortSupportData  TODO(pg-port)
#[repr(C)] pub struct SortSupportData { _opaque: [u8; 0] }

// ScalarItem  TODO(pg-port)
#[repr(C)] pub struct ScalarItem {
    value: Datum,
    tupno: c_int,
}

// StdAnalyzeData  TODO(pg-port)
#[repr(C)] pub struct StdAnalyzeData { _opaque: [u8; 0] }

// FmgrInfo  TODO(pg-port)
#[repr(C)] pub struct FmgrInfo { _opaque: [u8; 0] }

// Form_pg_attribute / Form_pg_type  TODO(pg-port)
#[repr(C)] pub struct FormData_pg_attribute { _opaque: [u8; 0] }
type Form_pg_attribute = *mut FormData_pg_attribute;
#[repr(C)] pub struct FormData_pg_type { _opaque: [u8; 0] }
type Form_pg_type = *mut FormData_pg_type;

// StringInfoData  TODO(pg-port)
use crate::lib::stringinfo::StringInfoData;

/* Per-index data for ANALYZE */
#[repr(C)]
pub struct AnlIndexData {
    indexInfo: *mut IndexInfo,  /* BuildIndexInfo result */
    tupleFract: f64,            /* fraction of rows for partial index */
    vacattrstats: *mut VacAttrStatsP, /* index attrs to analyze */
    attr_cnt: c_int,
}


/* Default statistics target (GUC parameter) */
pub static mut default_statistics_target: c_int = 100;

/* A few variables that don't seem worth passing around as parameters */
static mut anl_context: MemoryContext = core::ptr::null_mut();
static mut vac_strategy: BufferAccessStrategy = core::ptr::null_mut();


/* --------------------------------------------------------------------------
 * Stub externs for unported functions
 * -------------------------------------------------------------------------- */

extern "C" {
    // vacuum.h
    fn vacuum_open_relation(relid: Oid, relation: *mut RangeVar, options: c_int,
                            verbose: bool, lmode: c_int) -> Relation;
    fn vacuum_is_permitted_for_relation(relid: Oid, classForm: *mut c_void,
                                        options: c_int) -> bool;
    fn vac_open_indexes(relation: Relation, lockmode: c_int,
                        nindexes: *mut c_int, Irel: *mut *mut Relation);
    fn vac_close_indexes(nindexes: c_int, Irel: *mut Relation, lockmode: c_int);
    fn vac_update_relstats(relation: Relation, num_pages: BlockNumber,
                           num_tuples: f64, num_allvisible: BlockNumber,
                           num_allfrozen: BlockNumber, hasindex: bool,
                           frozenxid: TransactionId, minmulti: c_int,
                           a: *mut c_void, b: *mut c_void,
                           in_outer_xact: bool);
    fn vacuum_delay_point(is_analyze: bool);

    // access/relation.h
    fn relation_close(relation: Relation, lockmode: c_int);

    // access/visibilitymap.h
    fn visibilitymap_count(rel: Relation, all_visible: *mut BlockNumber,
                           all_frozen: *mut BlockNumber);

    // access/xact.h
    fn CommandCounterIncrement();

    // access/transam.h
    fn GetOldestNonRemovableTransactionId(rel: Relation) -> TransactionId;

    // access/tupconvert.h
    fn convert_tuples_by_name(indesc: TupleDesc, outdesc: TupleDesc)
        -> *mut TupleConversionMap;
    fn execute_attr_map_tuple(tuple: HeapTuple, map: *mut TupleConversionMap) -> HeapTuple;
    fn free_conversion_map(map: *mut TupleConversionMap);
    fn equalRowTypes(tupdesc1: TupleDesc, tupdesc2: TupleDesc) -> bool;

    // catalog/index.h
    fn BuildIndexInfo(index: Relation) -> *mut IndexInfo;
    fn SetRelationHasSubclass(relid: Oid, relhassubclass: bool);

    // catalog/pg_inherits.h
    fn find_all_inheritors(parentrelId: Oid, lockmode: c_int,
                           numparents: *mut c_int) -> *mut List;

    // commands/vacuum.h
    fn index_vacuum_cleanup(ivinfo: *mut IndexVacuumInfo,
                            stats: *mut IndexBulkDeleteResult) -> *mut IndexBulkDeleteResult;

    // executor/executor.h
    fn CreateExecutorState() -> *mut EState;
    fn FreeExecutorState(estate: *mut EState);
    fn ExecPrepareQual(qual: *mut List, estate: *mut EState) -> *mut ExprState;
    fn ExecQual(qual: *mut ExprState, econtext: *mut ExprContext) -> bool;
    fn GetPerTupleExprContext(estate: *mut EState) -> *mut ExprContext;
    fn ResetExprContext(econtext: *mut ExprContext);

    // executor/tuptable.h
    fn MakeSingleTupleTableSlot(tupdesc: TupleDesc, tts_ops: *const c_void)
        -> *mut TupleTableSlot;
    fn ExecDropSingleTupleTableSlot(slot: *mut TupleTableSlot);
    fn ExecStoreHeapTuple(tuple: HeapTuple, slot: *mut TupleTableSlot,
                          shouldFree: bool) -> *mut TupleTableSlot;
    fn ExecCopySlotHeapTuple(slot: *mut TupleTableSlot) -> HeapTuple;

    // foreign/fdwapi.h
    fn GetFdwRoutineForRelation(relation: Relation, makecopy: bool) -> *mut FdwRoutine;

    // miscadmin.h
    fn GetUserIdAndSecContext(userid: *mut Oid, sec_context: *mut c_int);
    fn SetUserIdAndSecContext(userid: Oid, sec_context: c_int);
    fn NewGUCNestLevel() -> c_int;
    fn RestrictSearchPath();
    fn AtEOXact_GUC(isCommit: bool, nestLevel: c_int);
    fn AmAutoVacuumWorkerProcess() -> bool;

    // pgstat.h
    fn pgstat_progress_start_command(cmdtype: c_int, relid: Oid);
    fn pgstat_progress_end_command();
    fn pgstat_progress_update_param(index: c_int, val: i64);
    fn pgstat_progress_update_multi_param(nparam: c_int, index: *const c_int,
                                          val: *const i64);
    fn pgstat_report_analyze(rel: Relation, livetuples: f64, deadtuples: f64,
                             resetcounter: bool, starttime: TimestampTz);

    // statistics/statistics.h
    fn ComputeExtStatisticsRows(onerel: Relation, natts: c_int,
                                vacattrstats: *mut VacAttrStatsP) -> c_int;
    fn BuildRelationExtStatistics(onerel: Relation, inh: bool, totalrows: f64,
                                  numrows: c_int, rows: *mut HeapTuple,
                                  natts: c_int, vacattrstats: *mut VacAttrStatsP);

    // utils/attoptcache.h
    fn get_attribute_options(relid: Oid, attnum: c_int) -> *mut AttributeOpts;

    // utils/datum.h
    fn datumCopy(value: Datum, typByVal: bool, typLen: i16) -> Datum;

    // utils/lsyscache.h
    fn get_namespace_name(nspid: Oid) -> *mut c_char;
    fn get_database_name(dbid: Oid) -> *mut c_char;
    fn attnameAttNum(rel: Relation, attname: *const c_char, sysColOK: bool) -> c_int;
    fn get_sort_group_operators(argtype: Oid, needLT: bool, needEQ: bool, needGT: bool,
                                ltOpr: *mut Oid, eqOpr: *mut Oid, gtOpr: *mut Oid,
                                isHashable: *mut bool);
    fn get_opcode(opno: Oid) -> Oid;

    // utils/memutils.h
    fn AllocSetContextCreate(parent: MemoryContext, name: *const c_char,
                             minContextSize: usize, initBlockSize: usize,
                             maxBlockSize: usize) -> MemoryContext;
    fn MemoryContextSwitchTo(context: MemoryContext) -> MemoryContext;
    fn MemoryContextDelete(context: MemoryContext);
    fn MemoryContextReset(context: MemoryContext);

    // utils/pg_rusage.h
    fn pg_rusage_init(ru: *mut PGRUsage);
    fn pg_rusage_show(ru: *const PGRUsage) -> *const c_char;

    // utils/sampling.h
    fn BlockSampler_Init(bs: *mut BlockSamplerData, nblocks: BlockNumber,
                         samplesize: c_int, randseed: u32) -> BlockNumber;
    fn BlockSampler_HasMore(bs: *mut BlockSamplerData) -> bool;
    fn BlockSampler_Next(bs: *mut BlockSamplerData) -> BlockNumber;
    fn reservoir_init_selection_state(rstate: *mut ReservoirStateData, n: c_int);
    fn reservoir_get_next_S(rstate: *mut ReservoirStateData, t: f64, n: c_int) -> f64;
    fn sampler_random_fract(randstate: *mut c_void) -> f64;

    // utils/sortsupport.h
    fn PrepareSortSupportFromOrderingOp(opno: Oid, ssup: *mut SortSupportData);
    fn ApplySortComparator(datum1: Datum, isnull1: bool, datum2: Datum,
                           isnull2: bool, ssup: *mut SortSupportData) -> c_int;

    // utils/syscache.h
    fn SearchSysCache2(cacheId: c_int, key1: Datum, key2: Datum) -> HeapTuple;
    fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple;
    fn SearchSysCache3(cacheId: c_int, key1: Datum, key2: Datum, key3: Datum) -> HeapTuple;
    fn SysCacheGetAttr(cacheId: c_int, tup: HeapTuple, attributeNumber: c_int,
                       isnull: *mut bool) -> Datum;
    fn ReleaseSysCache(tuple: HeapTuple);

    // utils/timestamp.h
    fn GetCurrentTimestamp() -> TimestampTz;
    fn TimestampDifferenceExceeds(start_time: TimestampTz, stop_time: TimestampTz,
                                  msec: c_int) -> bool;
    fn TimestampDifferenceMilliseconds(start_time: TimestampTz,
                                       stop_time: TimestampTz) -> i64;

    // access/detoast.h
    fn toast_raw_datum_size(value: Datum) -> usize;

    // heap/heaptuple.h
    fn heap_freetuple(htup: HeapTuple);
    fn heap_getattr(tup: HeapTuple, attnum: c_int, tupleDesc: TupleDesc,
                    isnull: *mut bool) -> Datum;
    fn heap_form_tuple(tupleDescriptor: TupleDesc, values: *mut Datum,
                       isnull: *mut bool) -> HeapTuple;
    fn heap_modify_tuple(tuple: HeapTuple, tupleDesc: TupleDesc,
                         replValues: *mut Datum, replIsnull: *mut bool,
                         doReplace: *mut bool) -> HeapTuple;

    // catalog/indexing.h
    fn CatalogOpenIndexes(heapRel: Relation) -> CatalogIndexState;
    fn CatalogCloseIndexes(indstate: CatalogIndexState);
    fn CatalogTupleInsertWithInfo(heapRel: Relation, tup: HeapTuple,
                                  indstate: CatalogIndexState);
    fn CatalogTupleUpdateWithInfo(heapRel: Relation, otid: *mut c_void,
                                  tup: HeapTuple, indstate: CatalogIndexState);

    // utils/array.h
    fn construct_array(elems: *mut Datum, nelems: c_int, elmtype: Oid,
                       elmlen: i16, elmbyval: bool, elmalign: c_char) -> *mut ArrayType;
    fn construct_array_builtin(elems: *mut Datum, nelems: c_int, elmtype: Oid)
        -> *mut ArrayType;

    // fmgr.h
    fn OidFunctionCall1(functionId: Oid, arg1: Datum) -> Datum;
    fn fmgr_info(functionId: Oid, finfo: *mut FmgrInfo);
    fn FunctionCall2Coll(flinfo: *mut FmgrInfo, collation: Oid,
                         arg1: Datum, arg2: Datum) -> Datum;

    // nodes/nodeFuncs.h
    fn exprType(expr: *const Node) -> Oid;
    fn exprTypmod(expr: *const Node) -> i32;
    fn exprCollation(expr: *const Node) -> Oid;

    // parser/parse_oper.h (via statistics)
    // std_typanalyze is defined below in this file

    // executor/nodeIndexscan.h (via access/tableam.h)
    fn FormIndexDatum(indexInfo: *mut IndexInfo, slot: *mut TupleTableSlot,
                      estate: *mut EState, values: *mut Datum, isnull: *mut bool);

    // read stream
    fn read_stream_begin_relation(flags: c_int, strategy: BufferAccessStrategy,
                                  relation: Relation, forknum: c_int,
                                  next_block_cb: unsafe extern "C" fn(
                                      *mut ReadStream, *mut c_void, *mut c_void) -> BlockNumber,
                                  callback_private_data: *mut c_void,
                                  per_buffer_data_size: usize) -> *mut ReadStream;
    fn read_stream_end(stream: *mut ReadStream);

    // instrumentation
    fn BufferUsageAccumDiff(dst: *mut BufferUsage, add: *const BufferUsage,
                            sub: *const BufferUsage);
    fn WalUsageAccumDiff(dst: *mut WalUsage, add: *const WalUsage,
                         sub: *const WalUsage);

    // lib/stringinfo.h
    fn initStringInfo(str_: *mut StringInfoData);
    fn appendStringInfo(str_: *mut StringInfoData, fmt: *const c_char, ...);
    fn pfree(pointer: *mut c_void);
    fn palloc(size: usize) -> *mut c_void;
    fn palloc0(size: usize) -> *mut c_void;

    // bitmapset
    fn bms_is_member(x: c_int, a: *mut c_void) -> bool;
    fn bms_add_member(a: *mut c_void, x: c_int) -> *mut c_void;

    // miscellaneous
    fn RelationGetNumberOfBlocks(relation: Relation) -> BlockNumber;
    fn RelationGetDescr(rel: Relation) -> TupleDesc;
    fn RelationGetRelid(rel: Relation) -> Oid;
    fn RelationGetRelationName(rel: Relation) -> *const c_char;
    fn RelationGetNamespace(rel: Relation) -> Oid;
    fn RelationGetIndexList(relation: Relation) -> *mut List;
    fn qsort_interruptible(base: *mut c_void, nmemb: usize, size: usize,
                           cmp: unsafe extern "C" fn(*const c_void, *const c_void,
                                                     *mut c_void) -> c_int,
                           arg: *mut c_void);
    fn pg_prng_uint32(state: *mut c_void) -> u32;

    // GUC / globals
    static pg_global_prng_state: c_void;
    static pgWalUsage: WalUsage;
    static pgBufferUsage: BufferUsage;
    static pgStatBlockReadTime: PgStat_Counter;
    static pgStatBlockWriteTime: PgStat_Counter;
    static MyDatabaseId: Oid;
    static track_io_timing: bool;
    static track_cost_delay_timing: bool;
    static mut MyBEEntry: *mut c_void;

    // TTSOpsHeapTuple
    static TTSOpsHeapTuple: c_void;

    // Relation macros (as fns in C)
    fn RELATION_IS_OTHER_TEMP(relation: Relation) -> bool;
    fn RELKIND_HAS_STORAGE(relkind: c_char) -> bool;

    // strVal / lfirst helpers exposed as fns  TODO(pg-port)
    fn strVal(v: *mut c_void) -> *const c_char;
    fn TupleDescAttr(tupdesc: TupleDesc, attno: c_int) -> Form_pg_attribute;
    fn GETSTRUCT(tuple: HeapTuple) -> *mut c_void;
    fn HeapTupleIsValid(htup: HeapTuple) -> bool;
}

// FdwRoutine stub  TODO(pg-port)
#[repr(C)]
pub struct FdwRoutine {
    pub AnalyzeForeignTable: Option<unsafe extern "C" fn(
        relation: Relation,
        acquirefunc: *mut AcquireSampleRowsFunc,
        totalpages: *mut BlockNumber,
    ) -> bool>,
    _other: [u8; 0],
}

// AttributeOpts stub  TODO(pg-port)
#[repr(C)]
pub struct AttributeOpts {
    pub n_distinct: float8,
    pub n_distinct_inherited: float8,
}

// Constants  TODO(pg-port)
const VACOPT_VERBOSE: c_int = 1 << 0;
const VACOPT_VACUUM: c_int = 1 << 1;
const ALLOCSET_DEFAULT_SIZES: usize = 0; /* placeholder - real C macro passes 3 args */
const SECURITY_RESTRICTED_OPERATION: c_int = 0x0002;
const ShareUpdateExclusiveLock: c_int = 4;
const AccessShareLock: c_int = 1;
const RowExclusiveLock: c_int = 3;
const NoLock: c_int = 0;
const InvalidBlockNumber: BlockNumber = BlockNumber::MAX;
const InvalidAttrNumber: c_int = 0;
const PROGRESS_COMMAND_ANALYZE: c_int = 2;
const PROGRESS_ANALYZE_PHASE: c_int = 0;
const PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS: i64 = 1;
const PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS_INH: i64 = 2;
const PROGRESS_ANALYZE_PHASE_COMPUTE_STATS: i64 = 3;
const PROGRESS_ANALYZE_PHASE_FINALIZE_ANALYZE: i64 = 4;
const PROGRESS_ANALYZE_BLOCKS_TOTAL: c_int = 1;
const PROGRESS_ANALYZE_BLOCKS_DONE: c_int = 2;
const PROGRESS_ANALYZE_CHILD_TABLES_TOTAL: c_int = 3;
const PROGRESS_ANALYZE_CHILD_TABLES_DONE: c_int = 4;
const PROGRESS_ANALYZE_CURRENT_CHILD_TABLE_RELID: c_int = 5;
const PROGRESS_ANALYZE_DELAY_TIME: usize = 6;
const STATISTIC_NUM_SLOTS: usize = 5;
const STATISTIC_KIND_MCV: i16 = 1;
const STATISTIC_KIND_HISTOGRAM: i16 = 2;
const STATISTIC_KIND_CORRELATION: i16 = 3;
const ATTNUM: c_int = 14;
const TYPEOID: c_int = 26;
const STATRELATTINH: c_int = 27;
const FLOAT4OID: Oid = 700;
const INFO: c_int = 17;
const DEBUG2: c_int = 12;
const LOG: c_int = 15;
const ERROR: c_int = 20;
const WARNING: c_int = 19;
const READ_STREAM_MAINTENANCE: c_int = 1 << 0;
const READ_STREAM_USE_BATCHING: c_int = 1 << 1;
const MAIN_FORKNUM: c_int = 0;
const WIDTH_THRESHOLD: usize = 1024;
const RELKIND_RELATION: c_char = b'r' as c_char;
const RELKIND_MATVIEW: c_char = b'm' as c_char;
const RELKIND_FOREIGN_TABLE: c_char = b'f' as c_char;
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;
// pg_statistic catalog numbers  TODO(pg-port)
const StatisticRelationId: Oid = 2619;
const Natts_pg_statistic: usize = 24;
const Anum_pg_statistic_starelid: usize = 1;
const Anum_pg_statistic_staattnum: usize = 2;
const Anum_pg_statistic_stainherit: usize = 3;
const Anum_pg_statistic_stanullfrac: usize = 4;
const Anum_pg_statistic_stawidth: usize = 5;
const Anum_pg_statistic_stadistinct: usize = 6;
const Anum_pg_statistic_stakind1: usize = 7;
const Anum_pg_statistic_staop1: usize = 12;
const Anum_pg_statistic_stacoll1: usize = 17;
const Anum_pg_statistic_stanumbers1: usize = 22; // placeholder; real layout differs
const Anum_pg_statistic_stavalues1: usize = 23; // placeholder
const Anum_pg_attribute_attstattarget: c_int = 33;
const ATTRIBUTE_GENERATED_VIRTUAL: c_char = b'v' as c_char;
const InvalidOid: Oid = 0;

// Datum helpers  TODO(pg-port)
#[inline] unsafe fn ObjectIdGetDatum(x: Oid) -> Datum { x as Datum }
#[inline] unsafe fn Int16GetDatum(x: i16) -> Datum { x as Datum }
#[inline] unsafe fn Int32GetDatum(x: i32) -> Datum { x as Datum }
#[inline] unsafe fn BoolGetDatum(x: bool) -> Datum { x as Datum }
#[inline] unsafe fn Float4GetDatum(x: f32) -> Datum { x.to_bits() as Datum }
#[inline] unsafe fn PointerGetDatum(x: *mut c_void) -> Datum { x as Datum }
#[inline] unsafe fn DatumGetPointer(x: Datum) -> *mut c_void { x as *mut c_void }
#[inline] unsafe fn DatumGetBool(x: Datum) -> bool { x != 0 }
#[inline] unsafe fn DatumGetInt16(x: Datum) -> i16 { x as i16 }
#[inline] unsafe fn DatumGetCString(x: Datum) -> *mut c_char { x as *mut c_char }
#[inline] unsafe fn OidIsValid(oid: Oid) -> bool { oid != InvalidOid }

// VARSIZE_ANY  TODO(pg-port)
macro_rules! VARSIZE_ANY {
    ($p:expr) => { *($p as *const u32) as usize }
}

// PG_DETOAST_DATUM  TODO(pg-port)
macro_rules! PG_DETOAST_DATUM {
    ($d:expr) => { $d as *mut c_void }
}

macro_rules! swapInt {
    ($a:expr, $b:expr) => {{
        let _tmp = $a; $a = $b; $b = _tmp;
    }}
}
macro_rules! swapDatum {
    ($a:expr, $b:expr) => {{
        let _tmp = $a; $a = $b; $b = _tmp;
    }}
}

/*
 *	analyze_rel() -- analyze one relation
 *
 * relid identifies the relation to analyze.  If relation is supplied, use
 * the name therein for reporting any failure to open/lock the rel; do not
 * use it once we've successfully opened the rel, since it might be stale.
 */
pub unsafe fn analyze_rel(relid: Oid, relation: *mut RangeVar,
                           params: *mut VacuumParams, va_cols: *mut List,
                           in_outer_xact: bool,
                           bstrategy: BufferAccessStrategy)
{
    let mut onerel: Relation;
    let elevel: c_int;
    let mut acquirefunc: Option<AcquireSampleRowsFunc> = None;
    let mut relpages: BlockNumber = 0;

    /* Select logging level */
    if vacparams_options(params) & VACOPT_VERBOSE != 0 {
        elevel = INFO;
    } else {
        elevel = DEBUG2;
    }

    /* Set up static variables */
    vac_strategy = bstrategy;

    /*
     * Check for user-requested abort.
     */
    CHECK_FOR_INTERRUPTS!();

    /*
     * Open the relation, getting ShareUpdateExclusiveLock to ensure that two
     * ANALYZEs don't run on it concurrently.  (This also locks out a
     * concurrent VACUUM, which doesn't matter much at the moment but might
     * matter if we ever try to accumulate stats on dead tuples.) If the rel
     * has been dropped since we last saw it, we don't need to process it.
     *
     * Make sure to generate only logs for ANALYZE in this case.
     */
    onerel = vacuum_open_relation(relid, relation,
                                  vacparams_options(params) & !(VACOPT_VACUUM),
                                  vacparams_log_min_duration(params) >= 0,
                                  ShareUpdateExclusiveLock);

    /* leave if relation could not be opened or locked */
    if onerel.is_null() {
        return;
    }

    /*
     * Check if relation needs to be skipped based on privileges.  This check
     * happens also when building the relation list to analyze for a manual
     * operation, and needs to be done additionally here as ANALYZE could
     * happen across multiple transactions where privileges could have changed
     * in-between.  Make sure to generate only logs for ANALYZE in this case.
     */
    if !vacuum_is_permitted_for_relation(RelationGetRelid(onerel),
                                         rel_rd_rel(onerel) as *mut c_void,
                                         vacparams_options(params) & !VACOPT_VACUUM)
    {
        relation_close(onerel, ShareUpdateExclusiveLock);
        return;
    }

    /*
     * Silently ignore tables that are temp tables of other backends ---
     * trying to analyze these is rather pointless, since their contents are
     * probably not up-to-date on disk.  (We don't throw a warning here; it
     * would just lead to chatter during a database-wide ANALYZE.)
     */
    if RELATION_IS_OTHER_TEMP(onerel) {
        relation_close(onerel, ShareUpdateExclusiveLock);
        return;
    }

    /*
     * We can ANALYZE any table except pg_statistic. See update_attstats
     */
    if RelationGetRelid(onerel) == StatisticRelationId {
        relation_close(onerel, ShareUpdateExclusiveLock);
        return;
    }

    /*
     * Check that it's of an analyzable relkind, and set up appropriately.
     */
    let relkind = rel_relkind(onerel);
    if relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW {
        /* Regular table, so we'll use the regular row acquisition function */
        acquirefunc = Some(acquire_sample_rows);
        /* Also get regular table's size */
        relpages = RelationGetNumberOfBlocks(onerel);
    } else if relkind == RELKIND_FOREIGN_TABLE {
        /*
         * For a foreign table, call the FDW's hook function to see whether it
         * supports analysis.
         */
        let fdwroutine: *mut FdwRoutine;
        let mut ok = false;

        fdwroutine = GetFdwRoutineForRelation(onerel, false);

        if let Some(f) = (*fdwroutine).AnalyzeForeignTable {
            let mut af: AcquireSampleRowsFunc = acquire_sample_rows; /* placeholder */
            ok = f(onerel, &mut af as *mut AcquireSampleRowsFunc, &mut relpages);
            if ok { acquirefunc = Some(af); }
        }

        if !ok {
            ereport!(WARNING,
                     errmsg!("skipping \"{}\" --- cannot analyze this foreign table",
                              CStr::from_ptr(RelationGetRelationName(onerel)).to_string_lossy()));
            relation_close(onerel, ShareUpdateExclusiveLock);
            return;
        }
    } else if relkind == RELKIND_PARTITIONED_TABLE {
        /*
         * For partitioned tables, we want to do the recursive ANALYZE below.
         */
    } else {
        /* No need for a WARNING if we already complained during VACUUM */
        if vacparams_options(params) & VACOPT_VACUUM == 0 {
            ereport!(WARNING,
                     errmsg!("skipping \"{}\" --- cannot analyze non-tables or special system tables",
                              CStr::from_ptr(RelationGetRelationName(onerel)).to_string_lossy()));
        }
        relation_close(onerel, ShareUpdateExclusiveLock);
        return;
    }

    /*
     * OK, let's do it.  First, initialize progress reporting.
     */
    pgstat_progress_start_command(PROGRESS_COMMAND_ANALYZE,
                                  RelationGetRelid(onerel));

    /*
     * Do the normal non-recursive ANALYZE.  We can skip this for partitioned
     * tables, which don't contain any rows.
     */
    if rel_relkind(onerel) != RELKIND_PARTITIONED_TABLE {
        do_analyze_rel(onerel, params, va_cols, acquirefunc,
                       relpages, false, in_outer_xact, elevel);
    }

    /*
     * If there are child tables, do recursive ANALYZE.
     */
    if rel_relhassubclass(onerel) {
        do_analyze_rel(onerel, params, va_cols, acquirefunc, relpages,
                       true, in_outer_xact, elevel);
    }

    /*
     * Close source relation now, but keep lock so that no one deletes it
     * before we commit.  (If someone did, they'd fail to clean up the entries
     * we made in pg_statistic.  Also, releasing the lock before commit would
     * expose us to concurrent-update failures in update_attstats.)
     */
    relation_close(onerel, NoLock);

    pgstat_progress_end_command();
}

/* --------------------------------------------------------------------------
 * Small accessor helpers to avoid reaching into opaque C structs directly.
 * These are all TODO(pg-port) shims.
 * -------------------------------------------------------------------------- */

#[inline]
unsafe fn vacparams_options(params: *mut VacuumParams) -> c_int {
    // TODO(pg-port): read params->options
    0
}

#[inline]
unsafe fn vacparams_log_min_duration(params: *mut VacuumParams) -> i64 {
    // TODO(pg-port): read params->log_min_duration
    -1
}

#[inline]
unsafe fn rel_relkind(rel: Relation) -> c_char {
    // TODO(pg-port): read rel->rd_rel->relkind
    0
}

#[inline]
unsafe fn rel_rd_rel(rel: Relation) -> *mut c_void {
    // TODO(pg-port): return rel->rd_rel
    core::ptr::null_mut()
}

#[inline]
unsafe fn rel_relhassubclass(rel: Relation) -> bool {
    // TODO(pg-port): read rel->rd_rel->relhassubclass
    false
}

#[inline]
unsafe fn rel_relowner(rel: Relation) -> Oid {
    // TODO(pg-port): read rel->rd_rel->relowner
    0
}

#[inline]
unsafe fn rel_reltuples(rel: Relation) -> f32 {
    // TODO(pg-port): read rel->rd_rel->reltuples
    0.0
}

#[inline]
unsafe fn rel_rd_att(rel: Relation) -> TupleDesc {
    // TODO(pg-port): read rel->rd_att
    core::ptr::null_mut()
}

#[inline]
unsafe fn rel_rd_indcollation(rel: Relation, i: usize) -> Oid {
    // TODO(pg-port): read rel->rd_indcollation[i]
    0
}

#[inline]
unsafe fn indexinfo_ii_Expressions(ii: *mut IndexInfo) -> *mut List {
    // TODO(pg-port): read indexInfo->ii_Expressions
    core::ptr::null_mut()
}

#[inline]
unsafe fn indexinfo_ii_NumIndexAttrs(ii: *mut IndexInfo) -> c_int {
    // TODO(pg-port): read indexInfo->ii_NumIndexAttrs
    0
}

#[inline]
unsafe fn indexinfo_ii_IndexAttrNumbers(ii: *mut IndexInfo, i: usize) -> c_int {
    // TODO(pg-port): read indexInfo->ii_IndexAttrNumbers[i]
    0
}

#[inline]
unsafe fn indexinfo_ii_Predicate(ii: *mut IndexInfo) -> *mut List {
    // TODO(pg-port): read indexInfo->ii_Predicate
    core::ptr::null_mut()
}

#[inline]
unsafe fn tupdesc_natts(td: TupleDesc) -> c_int {
    // TODO(pg-port): read tupdesc->natts
    0
}

#[inline]
unsafe fn scan_rs_rd(scan: TableScanDesc) -> Relation {
    // TODO(pg-port): read scan->rs_rd
    core::ptr::null_mut()
}

#[inline]
unsafe fn econtext_set_scantuple(ec: *mut ExprContext, slot: *mut TupleTableSlot) {
    // TODO(pg-port): ec->ecxt_scantuple = slot
}

#[inline]
unsafe fn vacattrstats_minrows(stats: *mut VacAttrStats) -> c_int {
    // TODO(pg-port): read stats->minrows
    0
}

#[inline]
unsafe fn vacattrstats_rows(stats: *mut VacAttrStats) -> *mut HeapTuple {
    // TODO(pg-port): read stats->rows
    core::ptr::null_mut()
}

#[inline]
unsafe fn vacattrstats_set_rows(stats: *mut VacAttrStats, rows: *mut HeapTuple) {
    // TODO(pg-port): stats->rows = rows
}

#[inline]
unsafe fn vacattrstats_set_tupDesc(stats: *mut VacAttrStats, tupDesc: TupleDesc) {
    // TODO(pg-port): stats->tupDesc = tupDesc
}

#[inline]
unsafe fn vacattrstats_compute_stats(stats: *mut VacAttrStats,
                                     fetchfunc: AnalyzeAttrFetchFunc,
                                     samplerows: c_int, totalrows: f64) {
    // TODO(pg-port): stats->compute_stats(stats, fetchfunc, samplerows, totalrows)
}

#[inline]
unsafe fn vacattrstats_tupattnum(stats: *mut VacAttrStats) -> c_int {
    // TODO(pg-port): read stats->tupattnum
    0
}

#[inline]
unsafe fn vacattrstats_exprvals(stats: *mut VacAttrStats) -> *mut Datum {
    // TODO(pg-port): read stats->exprvals
    core::ptr::null_mut()
}

#[inline]
unsafe fn vacattrstats_set_exprvals(stats: *mut VacAttrStats, v: *mut Datum) {
    // TODO(pg-port): stats->exprvals = v
}

#[inline]
unsafe fn vacattrstats_exprnulls(stats: *mut VacAttrStats) -> *mut bool {
    // TODO(pg-port): read stats->exprnulls
    core::ptr::null_mut()
}

#[inline]
unsafe fn vacattrstats_set_exprnulls(stats: *mut VacAttrStats, v: *mut bool) {
    // TODO(pg-port): stats->exprnulls = v
}

#[inline]
unsafe fn vacattrstats_set_rowstride(stats: *mut VacAttrStats, v: c_int) {
    // TODO(pg-port): stats->rowstride = v
}

#[inline]
unsafe fn vacattrstats_stats_valid(stats: *mut VacAttrStats) -> bool {
    // TODO(pg-port): read stats->stats_valid
    false
}

// AnalyzeAttrFetchFunc type alias
type AnalyzeAttrFetchFunc = unsafe extern "C" fn(stats: VacAttrStatsP,
                                                  rownum: c_int,
                                                  isNull: *mut bool) -> Datum;

// std_fetch_func and ind_fetch_func are C-ABI callbacks
unsafe extern "C" fn std_fetch_func(stats: VacAttrStatsP, rownum: c_int,
                                     isNull: *mut bool) -> Datum
{
    let attnum = vacattrstats_tupattnum(stats);
    let tuple = *(stats as *mut *mut HeapTupleData).add(rownum as usize); // placeholder
    let tupDesc = core::ptr::null_mut(); // placeholder
    heap_getattr(tuple, attnum, tupDesc, isNull)
}

unsafe extern "C" fn ind_fetch_func(stats: VacAttrStatsP, rownum: c_int,
                                     isNull: *mut bool) -> Datum
{
    /* exprvals and exprnulls are already offset for proper column */
    let i = rownum * vacattrstats_rowstride(stats);
    *isNull = *vacattrstats_exprnulls(stats).add(i as usize);
    *vacattrstats_exprvals(stats).add(i as usize)
}

#[inline]
unsafe fn vacattrstats_rowstride(stats: *mut VacAttrStats) -> c_int {
    // TODO(pg-port): read stats->rowstride
    0
}

// MIN helper
#[inline]
fn Min(a: c_int, b: c_int) -> c_int { if a < b { a } else { b } }

/*
 *	do_analyze_rel() -- analyze one relation, recursively or not
 *
 * Note that "acquirefunc" is only relevant for the non-inherited case.
 * For the inherited case, acquire_inherited_sample_rows() determines the
 * appropriate acquirefunc for each child table.
 */
unsafe fn do_analyze_rel(onerel: Relation, params: *mut VacuumParams,
                          va_cols: *mut List,
                          acquirefunc: Option<AcquireSampleRowsFunc>,
                          relpages: BlockNumber, inh: bool,
                          in_outer_xact: bool, elevel: c_int)
{
    let mut attr_cnt: c_int;
    let mut tcnt: c_int;
    let mut i: c_int;
    let mut ind: c_int;
    let mut Irel: *mut Relation = core::ptr::null_mut();
    let mut nindexes: c_int = 0;
    let verbose: bool;
    let instrument: bool;
    let hasindex: bool;
    let mut vacattrstats: *mut VacAttrStatsP;
    let mut indexdata: *mut AnlIndexData = core::ptr::null_mut();
    let mut targrows: c_int;
    let mut numrows: c_int;
    let mut minrows: c_int;
    let mut totalrows: f64 = 0.0;
    let mut totaldeadrows: f64 = 0.0;
    let mut rows: *mut HeapTuple;
    let mut ru0: PGRUsage = core::mem::zeroed();
    let mut starttime: TimestampTz = 0;
    let caller_context: MemoryContext;
    let mut save_userid: Oid = 0;
    let mut save_sec_context: c_int = 0;
    let save_nestlevel: c_int;
    let startwalusage: WalUsage = core::mem::zeroed(); // pgWalUsage snapshot
    let startbufferusage: BufferUsage = core::mem::zeroed(); // pgBufferUsage snapshot
    let mut bufferusage: BufferUsage = core::mem::zeroed();
    let mut startreadtime: PgStat_Counter = 0;
    let mut startwritetime: PgStat_Counter = 0;

    verbose = vacparams_options(params) & VACOPT_VERBOSE != 0;
    instrument = verbose || (AmAutoVacuumWorkerProcess() &&
                              vacparams_log_min_duration(params) >= 0);
    if inh {
        ereport!(elevel,
                 errmsg!("analyzing \"{}.{}\" inheritance tree",
                          std::ffi::CStr::from_ptr(get_namespace_name(RelationGetNamespace(onerel))).to_string_lossy(),
                          std::ffi::CStr::from_ptr(RelationGetRelationName(onerel)).to_string_lossy()));
    } else {
        ereport!(elevel,
                 errmsg!("analyzing \"{}.{}\"",
                          std::ffi::CStr::from_ptr(get_namespace_name(RelationGetNamespace(onerel))).to_string_lossy(),
                          std::ffi::CStr::from_ptr(RelationGetRelationName(onerel)).to_string_lossy()));
    }

    /*
     * Set up a working context so that we can easily free whatever junk gets
     * created.
     */
    anl_context = AllocSetContextCreate(CurrentMemoryContext(),
                                        b"Analyze\0".as_ptr() as *const c_char,
                                        0, 0, 0);
    caller_context = MemoryContextSwitchTo(anl_context);

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.  Also lock down security-restricted operations and
     * arrange to make GUC variable changes local to this command.
     */
    GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
    SetUserIdAndSecContext(rel_relowner(onerel),
                           save_sec_context | SECURITY_RESTRICTED_OPERATION);
    save_nestlevel = NewGUCNestLevel();
    RestrictSearchPath();

    /*
     * When verbose or autovacuum logging is used, initialize a resource usage
     * snapshot and optionally track I/O timing.
     */
    if instrument {
        if track_io_timing {
            startreadtime = pgStatBlockReadTime;
            startwritetime = pgStatBlockWriteTime;
        }

        pg_rusage_init(&mut ru0);
    }

    /* Used for instrumentation and stats report */
    starttime = GetCurrentTimestamp();

    /*
     * Determine which columns to analyze
     *
     * Note that system attributes are never analyzed, so we just reject them
     * at the lookup stage.  We also reject duplicate column mentions.  (We
     * could alternatively ignore duplicates, but analyzing a column twice
     * won't work; we'd end up making a conflicting update in pg_statistic.)
     */
    if !va_cols.is_null() && list_length(va_cols) > 0 {
        let mut unique_cols: *mut c_void = core::ptr::null_mut();
        let mut le: *mut ListCell;

        vacattrstats = palloc(list_length(va_cols) as usize *
                              size_of::<VacAttrStatsP>()) as *mut VacAttrStatsP;
        tcnt = 0;
        le = list_head(va_cols) as *mut ListCell;
        while !le.is_null() {
            let col = strVal(lfirst(le) as *mut c_void);

            i = attnameAttNum(onerel, col, false);
            if i == InvalidAttrNumber {
                ereport!(ERROR,
                         errmsg!("column of relation \"{}\" does not exist",
                                  std::ffi::CStr::from_ptr(RelationGetRelationName(onerel)).to_string_lossy()));
            }
            if bms_is_member(i, unique_cols) {
                ereport!(ERROR,
                         errmsg!("column of relation \"{}\" appears more than once",
                                  std::ffi::CStr::from_ptr(RelationGetRelationName(onerel)).to_string_lossy()));
            }
            unique_cols = bms_add_member(unique_cols, i);

            *vacattrstats.add(tcnt as usize) = examine_attribute(onerel, i, core::ptr::null_mut());
            if !(*vacattrstats.add(tcnt as usize)).is_null() {
                tcnt += 1;
            }
            le = lnext(va_cols, le as *mut ListCell) as *mut ListCell;
        }
        attr_cnt = tcnt;
    } else {
        attr_cnt = tupdesc_natts(rel_rd_att(onerel));
        vacattrstats = palloc(attr_cnt as usize * size_of::<VacAttrStatsP>()) as *mut VacAttrStatsP;
        tcnt = 0;
        i = 1;
        while i <= attr_cnt {
            *vacattrstats.add(tcnt as usize) = examine_attribute(onerel, i, core::ptr::null_mut());
            if !(*vacattrstats.add(tcnt as usize)).is_null() {
                tcnt += 1;
            }
            i += 1;
        }
        attr_cnt = tcnt;
    }

    /*
     * Open all indexes of the relation, and see if there are any analyzable
     * columns in the indexes.  We do not analyze index columns if there was
     * an explicit column list in the ANALYZE command, however.
     *
     * If we are doing a recursive scan, we don't want to touch the parent's
     * indexes at all.  If we're processing a partitioned table, we need to
     * know if there are any indexes, but we don't want to process them.
     */
    if rel_relkind(onerel) == RELKIND_PARTITIONED_TABLE {
        let idxs = RelationGetIndexList(onerel);

        Irel = core::ptr::null_mut();
        nindexes = 0;
        hasindex = !idxs.is_null() && list_length(idxs) > 0;
        list_free(idxs);
    } else if !inh {
        vac_open_indexes(onerel, AccessShareLock, &mut nindexes, &mut Irel);
        hasindex = nindexes > 0;
    } else {
        Irel = core::ptr::null_mut();
        nindexes = 0;
        hasindex = false;
    }
    indexdata = core::ptr::null_mut();
    if nindexes > 0 {
        indexdata = palloc0(nindexes as usize * size_of::<AnlIndexData>()) as *mut AnlIndexData;
        ind = 0;
        while ind < nindexes {
            let thisdata = &mut *indexdata.add(ind as usize);
            let indexInfo: *mut IndexInfo;
            let irel: Relation = *Irel.add(ind as usize);

            thisdata.indexInfo = BuildIndexInfo(irel);
            indexInfo = thisdata.indexInfo;
            thisdata.tupleFract = 1.0; /* fix later if partial */
            if !indexinfo_ii_Expressions(indexInfo).is_null()
               && list_length(indexinfo_ii_Expressions(indexInfo)) > 0
               && (va_cols.is_null() || list_length(va_cols) == 0)
            {
                let mut indexpr_item = list_head(indexinfo_ii_Expressions(indexInfo));

                thisdata.vacattrstats = palloc(
                    indexinfo_ii_NumIndexAttrs(indexInfo) as usize *
                    size_of::<VacAttrStatsP>()) as *mut VacAttrStatsP;
                tcnt = 0;
                i = 0;
                while i < indexinfo_ii_NumIndexAttrs(indexInfo) {
                    let keycol = indexinfo_ii_IndexAttrNumbers(indexInfo, i as usize);

                    if keycol == 0 {
                        /* Found an index expression */
                        let indexkey: *mut Node;

                        if indexpr_item.is_null() {
                            /* shouldn't happen */
                            elog!(ERROR, "too few entries in indexprs list");
                        }
                        indexkey = lfirst(indexpr_item as *mut ListCell) as *mut Node;
                        indexpr_item = lnext(indexinfo_ii_Expressions(indexInfo),
                                             indexpr_item as *mut ListCell) as *mut ListCell;
                        *thisdata.vacattrstats.add(tcnt as usize) =
                            examine_attribute(irel, i + 1, indexkey);
                        if !(*thisdata.vacattrstats.add(tcnt as usize)).is_null() {
                            tcnt += 1;
                        }
                    }
                    i += 1;
                }
                thisdata.attr_cnt = tcnt;
            }
            ind += 1;
        }
    }

    /*
     * Determine how many rows we need to sample, using the worst case from
     * all analyzable columns.  We use a lower bound of 100 rows to avoid
     * possible overflow in Vitter's algorithm.  (Note: that will also be the
     * target in the corner case where there are no analyzable columns.)
     */
    targrows = 100;
    i = 0;
    while i < attr_cnt {
        if targrows < vacattrstats_minrows(*vacattrstats.add(i as usize)) {
            targrows = vacattrstats_minrows(*vacattrstats.add(i as usize));
        }
        i += 1;
    }
    ind = 0;
    while ind < nindexes {
        let thisdata = &*indexdata.add(ind as usize);

        i = 0;
        while i < thisdata.attr_cnt {
            if targrows < vacattrstats_minrows(*thisdata.vacattrstats.add(i as usize)) {
                targrows = vacattrstats_minrows(*thisdata.vacattrstats.add(i as usize));
            }
            i += 1;
        }
        ind += 1;
    }

    /*
     * Look at extended statistics objects too, as those may define custom
     * statistics target. So we may need to sample more rows and then build
     * the statistics with enough detail.
     */
    minrows = ComputeExtStatisticsRows(onerel, attr_cnt, vacattrstats);

    if targrows < minrows {
        targrows = minrows;
    }

    /*
     * Acquire the sample rows
     */
    rows = palloc(targrows as usize * size_of::<HeapTuple>()) as *mut HeapTuple;
    pgstat_progress_update_param(PROGRESS_ANALYZE_PHASE,
                                 if inh { PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS_INH }
                                 else { PROGRESS_ANALYZE_PHASE_ACQUIRE_SAMPLE_ROWS });
    if inh {
        numrows = acquire_inherited_sample_rows(onerel, elevel,
                                                rows, targrows,
                                                &mut totalrows, &mut totaldeadrows);
    } else {
        numrows = (acquirefunc.unwrap())(onerel, elevel,
                                         rows, targrows,
                                         &mut totalrows, &mut totaldeadrows);
    }

    /*
     * Compute the statistics.  Temporary results during the calculations for
     * each column are stored in a child context.  The calc routines are
     * responsible to make sure that whatever they store into the VacAttrStats
     * structure is allocated in anl_context.
     */
    if numrows > 0 {
        let col_context: MemoryContext;
        let old_context: MemoryContext;

        pgstat_progress_update_param(PROGRESS_ANALYZE_PHASE,
                                     PROGRESS_ANALYZE_PHASE_COMPUTE_STATS);

        col_context = AllocSetContextCreate(anl_context,
                                            b"Analyze Column\0".as_ptr() as *const c_char,
                                            0, 0, 0);
        old_context = MemoryContextSwitchTo(col_context);

        i = 0;
        while i < attr_cnt {
            let stats = *vacattrstats.add(i as usize);
            let aopt: *mut AttributeOpts;

            vacattrstats_set_rows(stats, rows);
            vacattrstats_set_tupDesc(stats, rel_rd_att(onerel));
            vacattrstats_compute_stats(stats, std_fetch_func, numrows, totalrows);

            /*
             * If the appropriate flavor of the n_distinct option is
             * specified, override with the corresponding value.
             */
            aopt = get_attribute_options(RelationGetRelid(onerel),
                                          vacattrstats_tupattnum(stats));
            if !aopt.is_null() {
                let n_distinct: f64;

                n_distinct = if inh { (*aopt).n_distinct_inherited }
                             else { (*aopt).n_distinct };
                if n_distinct != 0.0 {
                    vacattrstats_set_stadistinct(stats, n_distinct as f32);
                }
            }

            MemoryContextReset(col_context);
            i += 1;
        }

        if nindexes > 0 {
            compute_index_stats(onerel, totalrows,
                                indexdata, nindexes,
                                rows, numrows,
                                col_context);
        }

        MemoryContextSwitchTo(old_context);
        MemoryContextDelete(col_context);

        /*
         * Emit the completed stats rows into pg_statistic, replacing any
         * previous statistics for the target columns.  (If there are stats in
         * pg_statistic for columns we didn't process, we leave them alone.)
         */
        update_attstats(RelationGetRelid(onerel), inh,
                        attr_cnt, vacattrstats);

        ind = 0;
        while ind < nindexes {
            let thisdata = &*indexdata.add(ind as usize);

            update_attstats(RelationGetRelid(*Irel.add(ind as usize)), false,
                            thisdata.attr_cnt, thisdata.vacattrstats);
            ind += 1;
        }

        /* Build extended statistics (if there are any). */
        BuildRelationExtStatistics(onerel, inh, totalrows, numrows, rows,
                                   attr_cnt, vacattrstats);
    }

    pgstat_progress_update_param(PROGRESS_ANALYZE_PHASE,
                                 PROGRESS_ANALYZE_PHASE_FINALIZE_ANALYZE);

    /*
     * Update pages/tuples stats in pg_class ... but not if we're doing
     * inherited stats.
     *
     * We assume that VACUUM hasn't set pg_class.reltuples already, even
     * during a VACUUM ANALYZE.  Although VACUUM often updates pg_class,
     * exceptions exist.  A "VACUUM (ANALYZE, INDEX_CLEANUP OFF)" command will
     * never update pg_class entries for index relations.  It's also possible
     * that an individual index's pg_class entry won't be updated during
     * VACUUM if the index AM returns NULL from its amvacuumcleanup() routine.
     */
    if !inh {
        let mut relallvisible: BlockNumber = 0;
        let mut relallfrozen: BlockNumber = 0;

        if RELKIND_HAS_STORAGE(rel_relkind(onerel)) {
            visibilitymap_count(onerel, &mut relallvisible, &mut relallfrozen);
        }

        /*
         * Update pg_class for table relation.  CCI first, in case acquirefunc
         * updated pg_class.
         */
        CommandCounterIncrement();
        vac_update_relstats(onerel,
                            relpages,
                            totalrows,
                            relallvisible,
                            relallfrozen,
                            hasindex,
                            InvalidTransactionId,
                            0, // InvalidMultiXactId placeholder
                            core::ptr::null_mut(), core::ptr::null_mut(),
                            in_outer_xact);

        /* Same for indexes */
        ind = 0;
        while ind < nindexes {
            let thisdata = &*indexdata.add(ind as usize);
            let totalindexrows: f64;
            let irel = *Irel.add(ind as usize);

            totalindexrows = f64::ceil(thisdata.tupleFract * totalrows);
            vac_update_relstats(irel,
                                RelationGetNumberOfBlocks(irel),
                                totalindexrows,
                                0, 0,
                                false,
                                InvalidTransactionId,
                                0, // InvalidMultiXactId placeholder
                                core::ptr::null_mut(), core::ptr::null_mut(),
                                in_outer_xact);
            ind += 1;
        }
    } else if rel_relkind(onerel) == RELKIND_PARTITIONED_TABLE {
        /*
         * Partitioned tables don't have storage, so we don't set any fields
         * in their pg_class entries except for reltuples and relhasindex.
         */
        CommandCounterIncrement();
        vac_update_relstats(onerel, BlockNumber::MAX /* -1 */, totalrows,
                            0, 0, hasindex, InvalidTransactionId,
                            0, // InvalidMultiXactId
                            core::ptr::null_mut(), core::ptr::null_mut(),
                            in_outer_xact);
    }

    /*
     * Now report ANALYZE to the cumulative stats system.  For regular tables,
     * we do it only if not doing inherited stats.  For partitioned tables, we
     * only do it for inherited stats. (We're never called for not-inherited
     * stats on partitioned tables anyway.)
     *
     * Reset the changes_since_analyze counter only if we analyzed all
     * columns; otherwise, there is still work for auto-analyze to do.
     */
    if !inh {
        pgstat_report_analyze(onerel, totalrows, totaldeadrows,
                              va_cols.is_null() || list_length(va_cols) == 0,
                              starttime);
    } else if rel_relkind(onerel) == RELKIND_PARTITIONED_TABLE {
        pgstat_report_analyze(onerel, 0.0, 0.0,
                              va_cols.is_null() || list_length(va_cols) == 0,
                              starttime);
    }

    /*
     * If this isn't part of VACUUM ANALYZE, let index AMs do cleanup.
     *
     * Note that most index AMs perform a no-op as a matter of policy for
     * amvacuumcleanup() when called in ANALYZE-only mode.  The only exception
     * among core index AMs is GIN/ginvacuumcleanup().
     */
    if vacparams_options(params) & VACOPT_VACUUM == 0 {
        ind = 0;
        while ind < nindexes {
            let mut ivinfo: IndexVacuumInfo = core::mem::zeroed();
            let irel = *Irel.add(ind as usize);

            ivinfo_set_index(&mut ivinfo, irel);
            ivinfo_set_heaprel(&mut ivinfo, onerel);
            ivinfo_set_analyze_only(&mut ivinfo, true);
            ivinfo_set_estimated_count(&mut ivinfo, true);
            ivinfo_set_message_level(&mut ivinfo, elevel);
            ivinfo_set_num_heap_tuples(&mut ivinfo, rel_reltuples(onerel) as f64);
            ivinfo_set_strategy(&mut ivinfo, vac_strategy);

            let stats = index_vacuum_cleanup(&mut ivinfo, core::ptr::null_mut());

            if !stats.is_null() {
                pfree(stats as *mut c_void);
            }
            ind += 1;
        }
    }

    /* Done with indexes */
    vac_close_indexes(nindexes, Irel, NoLock);

    /* Log the action if appropriate */
    if instrument {
        let endtime = GetCurrentTimestamp();

        if verbose || vacparams_log_min_duration(params) == 0 ||
           TimestampDifferenceExceeds(starttime, endtime,
                                       vacparams_log_min_duration(params) as c_int)
        {
            let delay_in_ms: i64;
            let mut walusage: WalUsage = core::mem::zeroed();
            let mut read_rate: f64 = 0.0;
            let mut write_rate: f64 = 0.0;
            let msgfmt: &str;
            let mut buf: StringInfoData = core::mem::zeroed();
            let total_blks_hit: i64;
            let total_blks_read: i64;
            let total_blks_dirtied: i64;

            // memset(&bufferusage, 0, size_of::<BufferUsage>());
            bufferusage = core::mem::zeroed();
            BufferUsageAccumDiff(&mut bufferusage, &pgBufferUsage, &startbufferusage);
            // memset(&walusage, 0, size_of::<WalUsage>());
            walusage = core::mem::zeroed();
            WalUsageAccumDiff(&mut walusage, &pgWalUsage, &startwalusage);

            total_blks_hit = bufferusage_shared_blks_hit(&bufferusage) +
                bufferusage_local_blks_hit(&bufferusage);
            total_blks_read = bufferusage_shared_blks_read(&bufferusage) +
                bufferusage_local_blks_read(&bufferusage);
            total_blks_dirtied = bufferusage_shared_blks_dirtied(&bufferusage) +
                bufferusage_local_blks_dirtied(&bufferusage);

            /*
             * We do not expect an analyze to take > 25 days and it simplifies
             * things a bit to use TimestampDifferenceMilliseconds.
             */
            delay_in_ms = TimestampDifferenceMilliseconds(starttime, endtime);

            /*
             * Note that we are reporting these read/write rates in the same
             * manner as VACUUM does, which means that while the 'average read
             * rate' here actually corresponds to page misses and resulting
             * reads which are also picked up by track_io_timing, if enabled,
             * the 'average write rate' is actually talking about the rate of
             * pages being dirtied, not being written out, so it's typical to
             * have a non-zero 'avg write rate' while I/O timings only reports
             * reads.
             *
             * It's not clear that an ANALYZE will ever result in
             * FlushBuffer() being called, but we track and support reporting
             * on I/O write time in case that changes as it's practically free
             * to do so anyway.
             */

            if delay_in_ms > 0 {
                let blcksz = 8192_f64; // BLCKSZ placeholder
                read_rate = blcksz * total_blks_read as f64 /
                    (1024.0 * 1024.0) / (delay_in_ms as f64 / 1000.0);
                write_rate = blcksz * total_blks_dirtied as f64 /
                    (1024.0 * 1024.0) / (delay_in_ms as f64 / 1000.0);
            }

            /*
             * We split this up so we don't emit empty I/O timing values when
             * track_io_timing isn't enabled.
             */

            initStringInfo(&mut buf);

            if AmAutoVacuumWorkerProcess() {
                msgfmt = "automatic analyze of table \"{}.{}.{}\"\n";
            } else {
                msgfmt = "finished analyzing table \"{}.{}.{}\"\n";
            }

            let db_name = std::ffi::CStr::from_ptr(get_database_name(MyDatabaseId)).to_string_lossy();
            let ns_name = std::ffi::CStr::from_ptr(get_namespace_name(RelationGetNamespace(onerel))).to_string_lossy();
            let rel_name = std::ffi::CStr::from_ptr(RelationGetRelationName(onerel)).to_string_lossy();
            let msg_line = format!("{}{}.{}.{}\n", if AmAutoVacuumWorkerProcess() {
                "automatic analyze of table \""
            } else {
                "finished analyzing table \""
            }, db_name, ns_name, rel_name);
            // Build buf via Rust formatting (appendStringInfo is a C varargs fn; use Rust string)
            let mut log_msg = msg_line;

            if track_cost_delay_timing {
                /*
                 * We bypass the changecount mechanism because this value is
                 * only updated by the calling process.
                 */
                let delay_val = progress_param_at(MyBEEntry, PROGRESS_ANALYZE_DELAY_TIME) as f64 / 1_000_000.0;
                log_msg += &format!("delay time: {:.3} ms\n", delay_val);
            }
            if track_io_timing {
                let read_ms = (pgStatBlockReadTime - startreadtime) as f64 / 1000.0;
                let write_ms = (pgStatBlockWriteTime - startwritetime) as f64 / 1000.0;

                log_msg += &format!("I/O timings: read: {:.3} ms, write: {:.3} ms\n",
                                    read_ms, write_ms);
            }
            log_msg += &format!("avg read rate: {:.3} MB/s, avg write rate: {:.3} MB/s\n",
                                 read_rate, write_rate);
            log_msg += &format!("buffer usage: {} hits, {} reads, {} dirtied\n",
                                 total_blks_hit, total_blks_read, total_blks_dirtied);
            log_msg += &format!("WAL usage: {} records, {} full page images, {} bytes, {} buffers full\n",
                                 walusage_wal_records(&walusage),
                                 walusage_wal_fpi(&walusage),
                                 walusage_wal_bytes(&walusage),
                                 walusage_wal_buffers_full(&walusage));
            log_msg += &format!("system usage: {}",
                                 std::ffi::CStr::from_ptr(pg_rusage_show(&ru0)).to_string_lossy());

            ereport!(if verbose { INFO } else { LOG },
                     errmsg!("{}", log_msg));
        }
    }

    /* Roll back any GUC changes executed by index functions */
    AtEOXact_GUC(false, save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    /* Restore current context and release memory */
    MemoryContextSwitchTo(caller_context);
    MemoryContextDelete(anl_context);
    anl_context = core::ptr::null_mut();
}

// Additional accessor stubs used in do_analyze_rel  TODO(pg-port)
#[inline] unsafe fn CurrentMemoryContext() -> MemoryContext { core::ptr::null_mut() }
#[inline] unsafe fn vacattrstats_set_stadistinct(_s: *mut VacAttrStats, _v: f32) {}
#[inline] unsafe fn ivinfo_set_index(_i: *mut IndexVacuumInfo, _r: Relation) {}
#[inline] unsafe fn ivinfo_set_heaprel(_i: *mut IndexVacuumInfo, _r: Relation) {}
#[inline] unsafe fn ivinfo_set_analyze_only(_i: *mut IndexVacuumInfo, _v: bool) {}
#[inline] unsafe fn ivinfo_set_estimated_count(_i: *mut IndexVacuumInfo, _v: bool) {}
#[inline] unsafe fn ivinfo_set_message_level(_i: *mut IndexVacuumInfo, _v: c_int) {}
#[inline] unsafe fn ivinfo_set_num_heap_tuples(_i: *mut IndexVacuumInfo, _v: f64) {}
#[inline] unsafe fn ivinfo_set_strategy(_i: *mut IndexVacuumInfo, _s: BufferAccessStrategy) {}
#[inline] unsafe fn bufferusage_shared_blks_hit(_b: *const BufferUsage) -> i64 { 0 }
#[inline] unsafe fn bufferusage_local_blks_hit(_b: *const BufferUsage) -> i64 { 0 }
#[inline] unsafe fn bufferusage_shared_blks_read(_b: *const BufferUsage) -> i64 { 0 }
#[inline] unsafe fn bufferusage_local_blks_read(_b: *const BufferUsage) -> i64 { 0 }
#[inline] unsafe fn bufferusage_shared_blks_dirtied(_b: *const BufferUsage) -> i64 { 0 }
#[inline] unsafe fn bufferusage_local_blks_dirtied(_b: *const BufferUsage) -> i64 { 0 }
#[inline] unsafe fn walusage_wal_records(_w: *const WalUsage) -> i64 { 0 }
#[inline] unsafe fn walusage_wal_fpi(_w: *const WalUsage) -> i64 { 0 }
#[inline] unsafe fn walusage_wal_bytes(_w: *const WalUsage) -> u64 { 0 }
#[inline] unsafe fn walusage_wal_buffers_full(_w: *const WalUsage) -> i64 { 0 }
#[inline] unsafe fn progress_param_at(_entry: *mut c_void, _idx: usize) -> i64 { 0 }


/*
 * Compute statistics about indexes of a relation
 */
unsafe fn compute_index_stats(onerel: Relation, totalrows: f64,
                               indexdata: *mut AnlIndexData, nindexes: c_int,
                               rows: *mut HeapTuple, numrows: c_int,
                               col_context: MemoryContext)
{
    let ind_context: MemoryContext;
    let old_context: MemoryContext;
    let mut values: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let mut ind: c_int;
    let mut i: c_int;

    ind_context = AllocSetContextCreate(anl_context,
                                        b"Analyze Index\0".as_ptr() as *const c_char,
                                        0, 0, 0);
    old_context = MemoryContextSwitchTo(ind_context);

    ind = 0;
    while ind < nindexes {
        let thisdata = &mut *indexdata.add(ind as usize);
        let indexInfo = thisdata.indexInfo;
        let attr_cnt = thisdata.attr_cnt;
        let slot: *mut TupleTableSlot;
        let estate: *mut EState;
        let econtext: *mut ExprContext;
        let predicate: *mut ExprState;
        let exprvals: *mut Datum;
        let exprnulls: *mut bool;
        let mut numindexrows: c_int;
        let mut tcnt: c_int;
        let mut rowno: c_int;
        let totalindexrows: f64;

        /* Ignore index if no columns to analyze and not partial */
        if attr_cnt == 0 && (indexinfo_ii_Predicate(indexInfo).is_null() ||
                              list_length(indexinfo_ii_Predicate(indexInfo)) == 0)
        {
            ind += 1;
            continue;
        }

        /*
         * Need an EState for evaluation of index expressions and
         * partial-index predicates.  Create it in the per-index context to be
         * sure it gets cleaned up at the bottom of the loop.
         */
        estate = CreateExecutorState();
        econtext = GetPerTupleExprContext(estate);
        /* Need a slot to hold the current heap tuple, too */
        slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel),
                                        &TTSOpsHeapTuple as *const c_void);

        /* Arrange for econtext's scan tuple to be the tuple under test */
        econtext_set_scantuple(econtext, slot);

        /* Set up execution state for predicate. */
        predicate = ExecPrepareQual(indexinfo_ii_Predicate(indexInfo), estate);

        /* Compute and save index expression values */
        exprvals = palloc(numrows as usize * attr_cnt as usize * size_of::<Datum>()) as *mut Datum;
        exprnulls = palloc(numrows as usize * attr_cnt as usize * size_of::<bool>()) as *mut bool;
        numindexrows = 0;
        tcnt = 0;
        rowno = 0;
        while rowno < numrows {
            let heapTuple = *rows.add(rowno as usize);

            vacuum_delay_point(true);

            /*
             * Reset the per-tuple context each time, to reclaim any cruft
             * left behind by evaluating the predicate or index expressions.
             */
            ResetExprContext(econtext);

            /* Set up for predicate or expression evaluation */
            ExecStoreHeapTuple(heapTuple, slot, false);

            /* If index is partial, check predicate */
            if !predicate.is_null() {
                if !ExecQual(predicate, econtext) {
                    rowno += 1;
                    continue;
                }
            }
            numindexrows += 1;

            if attr_cnt > 0 {
                /*
                 * Evaluate the index row to compute expression values. We
                 * could do this by hand, but FormIndexDatum is convenient.
                 */
                FormIndexDatum(indexInfo,
                               slot,
                               estate,
                               values.as_mut_ptr(),
                               isnull.as_mut_ptr());

                /*
                 * Save just the columns we care about.  We copy the values
                 * into ind_context from the estate's per-tuple context.
                 */
                i = 0;
                while i < attr_cnt {
                    let stats = *thisdata.vacattrstats.add(i as usize);
                    let attnum = vacattrstats_tupattnum(stats);

                    if isnull[(attnum - 1) as usize] {
                        *exprvals.add(tcnt as usize) = 0;
                        *exprnulls.add(tcnt as usize) = true;
                    } else {
                        let attrtype = vacattrstats_attrtype(stats);
                        *exprvals.add(tcnt as usize) = datumCopy(
                            values[(attnum - 1) as usize],
                            attrtype_typbyval(attrtype),
                            attrtype_typlen(attrtype));
                        *exprnulls.add(tcnt as usize) = false;
                    }
                    tcnt += 1;
                    i += 1;
                }
            }
            rowno += 1;
        }

        /*
         * Having counted the number of rows that pass the predicate in the
         * sample, we can estimate the total number of rows in the index.
         */
        thisdata.tupleFract = numindexrows as f64 / numrows as f64;
        totalindexrows = f64::ceil(thisdata.tupleFract * totalrows);

        /*
         * Now we can compute the statistics for the expression columns.
         */
        if numindexrows > 0 {
            MemoryContextSwitchTo(col_context);
            i = 0;
            while i < attr_cnt {
                let stats = *thisdata.vacattrstats.add(i as usize);

                vacattrstats_set_exprvals(stats, exprvals.add(i as usize));
                vacattrstats_set_exprnulls(stats, exprnulls.add(i as usize));
                vacattrstats_set_rowstride(stats, attr_cnt);
                vacattrstats_compute_stats(stats,
                                           ind_fetch_func,
                                           numindexrows,
                                           totalindexrows);

                MemoryContextReset(col_context);
                i += 1;
            }
        }

        /* And clean up */
        MemoryContextSwitchTo(ind_context);

        ExecDropSingleTupleTableSlot(slot);
        FreeExecutorState(estate);
        MemoryContextReset(ind_context);

        ind += 1;
    }

    MemoryContextSwitchTo(old_context);
    MemoryContextDelete(ind_context);
}

// Additional accessor stubs used in compute_index_stats  TODO(pg-port)
#[inline] unsafe fn vacattrstats_attrtype(_s: *mut VacAttrStats) -> *mut FormData_pg_type { core::ptr::null_mut() }
#[inline] unsafe fn attrtype_typbyval(_t: *mut FormData_pg_type) -> bool { false }
#[inline] unsafe fn attrtype_typlen(_t: *mut FormData_pg_type) -> i16 { 0 }

/*
 * examine_attribute -- pre-analysis of a single column
 *
 * Determine whether the column is analyzable; if so, create and initialize
 * a VacAttrStats struct for it.  If not, return NULL.
 *
 * If index_expr isn't NULL, then we're trying to analyze an expression index,
 * and index_expr is the expression tree representing the column's data.
 */
unsafe fn examine_attribute(onerel: Relation, attnum: c_int,
                             index_expr: *mut Node) -> *mut VacAttrStats
{
    let attr: Form_pg_attribute = TupleDescAttr(rel_rd_att(onerel), attnum - 1);
    let mut attstattarget: c_int;
    let atttuple: HeapTuple;
    let mut dat: Datum = 0;
    let mut isnull: bool = false;
    let typtuple: HeapTuple;
    let stats: *mut VacAttrStats;
    let mut i: c_int;
    let ok: bool;

    /* Never analyze dropped columns */
    if attr_attisdropped(attr) {
        return core::ptr::null_mut();
    }

    /* Don't analyze virtual generated columns */
    if attr_attgenerated(attr) == ATTRIBUTE_GENERATED_VIRTUAL {
        return core::ptr::null_mut();
    }

    /*
     * Get attstattarget value.  Set to -1 if null.  (Analyze functions expect
     * -1 to mean use default_statistics_target; see for example
     * std_typanalyze.)
     */
    atttuple = SearchSysCache2(ATTNUM,
                               ObjectIdGetDatum(RelationGetRelid(onerel)),
                               Int16GetDatum(attnum as i16));
    if !HeapTupleIsValid(atttuple) {
        elog!(ERROR, "cache lookup failed for attribute {} of relation {}",
              attnum, RelationGetRelid(onerel));
    }
    dat = SysCacheGetAttr(ATTNUM, atttuple,
                          Anum_pg_attribute_attstattarget, &mut isnull);
    attstattarget = if isnull { -1 } else { DatumGetInt16(dat) as c_int };
    ReleaseSysCache(atttuple);

    /* Don't analyze column if user has specified not to */
    if attstattarget == 0 {
        return core::ptr::null_mut();
    }

    /*
     * Create the VacAttrStats struct.
     */
    stats = palloc0(size_of::<VacAttrStats>()) as *mut VacAttrStats;
    vacattrstats_set_attstattarget(stats, attstattarget);

    /*
     * When analyzing an expression index, believe the expression tree's type
     * not the column datatype --- the latter might be the opckeytype storage
     * type of the opclass, which is not interesting for our purposes.  (Note:
     * if we did anything with non-expression index columns, we'd need to
     * figure out where to get the correct type info from, but for now that's
     * not a problem.)	It's not clear whether anyone will care about the
     * typmod, but we store that too just in case.
     */
    if !index_expr.is_null() {
        vacattrstats_set_attrtypid(stats, exprType(index_expr as *const Node));
        vacattrstats_set_attrtypmod(stats, exprTypmod(index_expr as *const Node));

        /*
         * If a collation has been specified for the index column, use that in
         * preference to anything else; but if not, fall back to whatever we
         * can get from the expression.
         */
        if OidIsValid(rel_rd_indcollation(onerel, (attnum - 1) as usize)) {
            vacattrstats_set_attrcollid(stats,
                rel_rd_indcollation(onerel, (attnum - 1) as usize));
        } else {
            vacattrstats_set_attrcollid(stats, exprCollation(index_expr as *const Node));
        }
    } else {
        vacattrstats_set_attrtypid(stats, attr_atttypid(attr));
        vacattrstats_set_attrtypmod(stats, attr_atttypmod(attr));
        vacattrstats_set_attrcollid(stats, attr_attcollation(attr));
    }

    typtuple = SearchSysCacheCopy1(TYPEOID,
                                   ObjectIdGetDatum(vacattrstats_attrtypid(stats)));
    if !HeapTupleIsValid(typtuple) {
        elog!(ERROR, "cache lookup failed for type {}", vacattrstats_attrtypid(stats));
    }
    vacattrstats_set_attrtype(stats, GETSTRUCT(typtuple) as Form_pg_type);
    vacattrstats_set_anl_context(stats, anl_context);
    vacattrstats_set_tupattnum(stats, attnum);

    /*
     * The fields describing the stats->stavalues[n] element types default to
     * the type of the data being analyzed, but the type-specific typanalyze
     * function can change them if it wants to store something else.
     */
    i = 0;
    while i < STATISTIC_NUM_SLOTS as c_int {
        let attrtypid = vacattrstats_attrtypid(stats);
        let attrtype = vacattrstats_attrtype(stats);
        vacattrstats_set_statypid(stats, i as usize, attrtypid);
        vacattrstats_set_statyplen(stats, i as usize, attrtype_typlen(attrtype));
        vacattrstats_set_statypbyval(stats, i as usize, attrtype_typbyval(attrtype));
        vacattrstats_set_statypalign(stats, i as usize, attrtype_typalign(attrtype));
        i += 1;
    }

    /*
     * Call the type-specific typanalyze function.  If none is specified, use
     * std_typanalyze().
     */
    if OidIsValid(attrtype_typanalyze(vacattrstats_attrtype(stats))) {
        ok = DatumGetBool(OidFunctionCall1(
            attrtype_typanalyze(vacattrstats_attrtype(stats)),
            PointerGetDatum(stats as *mut c_void)));
    } else {
        ok = std_typanalyze(stats);
    }

    if !ok || !vacattrstats_has_compute_stats(stats) || vacattrstats_minrows(stats) <= 0 {
        heap_freetuple(typtuple);
        pfree(stats as *mut c_void);
        return core::ptr::null_mut();
    }

    stats
}

// Additional stubs for examine_attribute  TODO(pg-port)
#[inline] unsafe fn attr_attisdropped(_a: Form_pg_attribute) -> bool { false }
#[inline] unsafe fn attr_attgenerated(_a: Form_pg_attribute) -> c_char { 0 }
#[inline] unsafe fn attr_atttypid(_a: Form_pg_attribute) -> Oid { 0 }
#[inline] unsafe fn attr_atttypmod(_a: Form_pg_attribute) -> i32 { 0 }
#[inline] unsafe fn attr_attcollation(_a: Form_pg_attribute) -> Oid { 0 }
#[inline] unsafe fn attrtype_typanalyze(_t: Form_pg_type) -> Oid { 0 }
#[inline] unsafe fn attrtype_typalign(_t: Form_pg_type) -> c_char { 0 }
#[inline] unsafe fn vacattrstats_set_attstattarget(_s: *mut VacAttrStats, _v: c_int) {}
#[inline] unsafe fn vacattrstats_set_attrtypid(_s: *mut VacAttrStats, _v: Oid) {}
#[inline] unsafe fn vacattrstats_set_attrtypmod(_s: *mut VacAttrStats, _v: i32) {}
#[inline] unsafe fn vacattrstats_set_attrcollid(_s: *mut VacAttrStats, _v: Oid) {}
#[inline] unsafe fn vacattrstats_set_attrtype(_s: *mut VacAttrStats, _v: Form_pg_type) {}
#[inline] unsafe fn vacattrstats_set_anl_context(_s: *mut VacAttrStats, _v: MemoryContext) {}
#[inline] unsafe fn vacattrstats_set_tupattnum(_s: *mut VacAttrStats, _v: c_int) {}
#[inline] unsafe fn vacattrstats_attrtypid(_s: *mut VacAttrStats) -> Oid { 0 }
#[inline] unsafe fn vacattrstats_set_statypid(_s: *mut VacAttrStats, _i: usize, _v: Oid) {}
#[inline] unsafe fn vacattrstats_set_statyplen(_s: *mut VacAttrStats, _i: usize, _v: i16) {}
#[inline] unsafe fn vacattrstats_set_statypbyval(_s: *mut VacAttrStats, _i: usize, _v: bool) {}
#[inline] unsafe fn vacattrstats_set_statypalign(_s: *mut VacAttrStats, _i: usize, _v: c_char) {}
#[inline] unsafe fn vacattrstats_has_compute_stats(_s: *mut VacAttrStats) -> bool { false }


/*
 * block_sampling_read_stream_next -- get next block for sampling
 */
unsafe extern "C" fn block_sampling_read_stream_next(stream: *mut ReadStream,
                                                     callback_private_data: *mut c_void,
                                                     per_buffer_data: *mut c_void)
    -> BlockNumber
{
    let bs = callback_private_data as *mut BlockSamplerData;

    if BlockSampler_HasMore(bs) { BlockSampler_Next(bs) } else { InvalidBlockNumber }
}

/*
 * acquire_sample_rows -- acquire a random sample of rows from the table
 *
 * Selected rows are returned in the caller-allocated array rows[], which
 * must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also estimate the total numbers of live and dead rows in the table,
 * and return them into *totalrows and *totaldeadrows, respectively.
 *
 * The returned list of tuples is in order by physical position in the table.
 * (We will rely on this later to derive correlation estimates.)
 *
 * As of May 2004 we use a new two-stage method:  Stage one selects up
 * to targrows random blocks (or all blocks, if there aren't so many).
 * Stage two scans these blocks and uses the Vitter algorithm to create
 * a random sample of targrows rows (or less, if there are less in the
 * sample of blocks).  The two stages are executed simultaneously: each
 * block is processed as soon as stage one returns its number and while
 * the rows are read stage two controls which ones are to be inserted
 * into the sample.
 *
 * Although every row has an equal chance of ending up in the final
 * sample, this sampling method is not perfect: not every possible
 * sample has an equal chance of being selected.  For large relations
 * the number of different blocks represented by the sample tends to be
 * too small.  We can live with that for now.  Improvements are welcome.
 *
 * An important property of this sampling method is that because we do
 * look at a statistically unbiased set of blocks, we should get
 * unbiased estimates of the average numbers of live and dead rows per
 * block.  The previous sampling method put too much credence in the row
 * density near the start of the table.
 */
unsafe extern "C" fn acquire_sample_rows(onerel: Relation, elevel: c_int,
                                         rows: *mut HeapTuple, targrows: c_int,
                                         totalrows: *mut f64, totaldeadrows: *mut f64)
    -> c_int
{
    let mut numrows: c_int = 0;     /* # rows now in reservoir */
    let mut samplerows: f64 = 0.0;  /* total # rows collected */
    let mut liverows: f64 = 0.0;    /* # live rows seen */
    let mut deadrows: f64 = 0.0;    /* # dead rows seen */
    let mut rowstoskip: f64 = -1.0; /* -1 means not set yet */
    let randseed: u32;              /* Seed for block sampler(s) */
    let totalblocks: BlockNumber;
    let OldestXmin: TransactionId;
    let mut bs: BlockSamplerData = core::mem::zeroed();
    let mut rstate: ReservoirStateData = core::mem::zeroed();
    let slot: *mut TupleTableSlot;
    let scan: TableScanDesc;
    let nblocks: BlockNumber;
    let mut blksdone: BlockNumber = 0;
    let stream: *mut ReadStream;

    Assert!(targrows > 0);

    totalblocks = RelationGetNumberOfBlocks(onerel);

    /* Need a cutoff xmin for HeapTupleSatisfiesVacuum */
    OldestXmin = GetOldestNonRemovableTransactionId(onerel);

    /* Prepare for sampling block numbers */
    randseed = pg_prng_uint32(&pg_global_prng_state as *const c_void as *mut c_void);
    nblocks = BlockSampler_Init(&mut bs, totalblocks, targrows, randseed);

    /* Report sampling block numbers */
    pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_TOTAL,
                                 nblocks as i64);

    /* Prepare for sampling rows */
    reservoir_init_selection_state(&mut rstate, targrows);

    scan = table_beginscan_analyze(onerel);
    slot = table_slot_create(onerel, core::ptr::null_mut());

    /*
     * It is safe to use batching, as block_sampling_read_stream_next never
     * blocks.
     */
    stream = read_stream_begin_relation(READ_STREAM_MAINTENANCE |
                                        READ_STREAM_USE_BATCHING,
                                        vac_strategy,
                                        scan_rs_rd(scan),
                                        MAIN_FORKNUM,
                                        block_sampling_read_stream_next,
                                        &mut bs as *mut BlockSamplerData as *mut c_void,
                                        0);

    /* Outer loop over blocks to sample */
    while table_scan_analyze_next_block(scan, stream) {
        vacuum_delay_point(true);

        while table_scan_analyze_next_tuple(scan, OldestXmin, &mut liverows,
                                            &mut deadrows, slot) {
            /*
             * The first targrows sample rows are simply copied into the
             * reservoir. Then we start replacing tuples in the sample until
             * we reach the end of the relation.  This algorithm is from Jeff
             * Vitter's paper (see full citation in utils/misc/sampling.c). It
             * works by repeatedly computing the number of tuples to skip
             * before selecting a tuple, which replaces a randomly chosen
             * element of the reservoir (current set of tuples).  At all times
             * the reservoir is a true random sample of the tuples we've
             * passed over so far, so when we fall off the end of the relation
             * we're done.
             */
            if numrows < targrows {
                *rows.add(numrows as usize) = ExecCopySlotHeapTuple(slot);
                numrows += 1;
            } else {
                /*
                 * t in Vitter's paper is the number of records already
                 * processed.  If we need to compute a new S value, we must
                 * use the not-yet-incremented value of samplerows as t.
                 */
                if rowstoskip < 0.0 {
                    rowstoskip = reservoir_get_next_S(&mut rstate, samplerows, targrows);
                }

                if rowstoskip <= 0.0 {
                    /*
                     * Found a suitable tuple, so save it, replacing one old
                     * tuple at random
                     */
                    let k: c_int = (targrows as f64
                        * sampler_random_fract(reservoirstate_randstate(&mut rstate))) as c_int;

                    Assert!(k >= 0 && k < targrows);
                    heap_freetuple(*rows.add(k as usize));
                    *rows.add(k as usize) = ExecCopySlotHeapTuple(slot);
                }

                rowstoskip -= 1.0;
            }

            samplerows += 1.0;
        }

        blksdone += 1;
        pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_DONE,
                                     blksdone as i64);
    }

    read_stream_end(stream);

    ExecDropSingleTupleTableSlot(slot);
    table_endscan(scan);

    /*
     * If we didn't find as many tuples as we wanted then we're done. No sort
     * is needed, since they're already in order.
     *
     * Otherwise we need to sort the collected tuples by position
     * (itempointer). It's not worth worrying about corner cases where the
     * tuples are already sorted.
     */
    if numrows == targrows {
        qsort_interruptible(rows as *mut c_void, numrows as usize,
                            size_of::<HeapTuple>(),
                            compare_rows, core::ptr::null_mut());
    }

    /*
     * Estimate total numbers of live and dead rows in relation, extrapolating
     * on the assumption that the average tuple density in pages we didn't
     * scan is the same as in the pages we did scan.  Since what we scanned is
     * a random sample of the pages in the relation, this should be a good
     * assumption.
     */
    if blocksampler_m(&mut bs) > 0 {
        *totalrows = f64::floor((liverows / blocksampler_m(&mut bs) as f64)
            * totalblocks as f64 + 0.5);
        *totaldeadrows = f64::floor((deadrows / blocksampler_m(&mut bs) as f64)
            * totalblocks as f64 + 0.5);
    } else {
        *totalrows = 0.0;
        *totaldeadrows = 0.0;
    }

    /*
     * Emit some interesting relation info
     */
    ereport!(elevel,
             errmsg!("\"{}\": scanned {} of {} pages, containing {:.0} live rows and {:.0} dead rows; {} rows in sample, {:.0} estimated total rows",
                     std::ffi::CStr::from_ptr(RelationGetRelationName(onerel)).to_string_lossy(),
                     blocksampler_m(&mut bs), totalblocks,
                     liverows, deadrows,
                     numrows, *totalrows));

    return numrows;
}

/*
 * Comparator for sorting rows[] array
 */
unsafe extern "C" fn compare_rows(a: *const c_void, b: *const c_void, arg: *mut c_void)
    -> c_int
{
    let ha: HeapTuple = *(a as *const HeapTuple);
    let hb: HeapTuple = *(b as *const HeapTuple);
    let ba: BlockNumber = ItemPointerGetBlockNumber(htup_t_self(ha));
    let oa: OffsetNumber = ItemPointerGetOffsetNumber(htup_t_self(ha));
    let bb: BlockNumber = ItemPointerGetBlockNumber(htup_t_self(hb));
    let ob: OffsetNumber = ItemPointerGetOffsetNumber(htup_t_self(hb));

    if ba < bb {
        return -1;
    }
    if ba > bb {
        return 1;
    }
    if oa < ob {
        return -1;
    }
    if oa > ob {
        return 1;
    }
    return 0;
}

// Accessor / helper stubs for acquire_sample_rows + compare_rows  TODO(pg-port)
type OffsetNumber = u16;
#[inline] unsafe fn reservoirstate_randstate(_rs: *mut ReservoirStateData) -> *mut c_void {
    // TODO(pg-port): &rstate->randstate
    core::ptr::null_mut()
}
#[inline] unsafe fn blocksampler_m(_bs: *mut BlockSamplerData) -> BlockNumber {
    // TODO(pg-port): read bs->m
    0
}
#[inline] unsafe fn htup_t_self(_t: HeapTuple) -> *mut c_void {
    // TODO(pg-port): &tuple->t_self
    core::ptr::null_mut()
}
#[inline] unsafe fn ItemPointerGetBlockNumber(_p: *mut c_void) -> BlockNumber {
    // TODO(pg-port): ItemPointerGetBlockNumber
    0
}
#[inline] unsafe fn ItemPointerGetOffsetNumber(_p: *mut c_void) -> OffsetNumber {
    // TODO(pg-port): ItemPointerGetOffsetNumber
    0
}

/*
 * acquire_inherited_sample_rows -- acquire sample rows from inheritance tree
 *
 * This has the same API as acquire_sample_rows, except that rows are
 * collected from all inheritance children as well as the specified table.
 * We fail and return zero if there are no inheritance children, or if all
 * children are foreign tables that don't support ANALYZE.
 */
unsafe extern "C" fn acquire_inherited_sample_rows(onerel: Relation, elevel: c_int,
                                                   rows: *mut HeapTuple, targrows: c_int,
                                                   totalrows: *mut f64,
                                                   totaldeadrows: *mut f64)
    -> c_int
{
    let tableOIDs: *mut List;
    let rels: *mut Relation;
    let acquirefuncs: *mut Option<AcquireSampleRowsFunc>;
    let relblocks: *mut f64;
    let mut totalblocks: f64;
    let mut numrows: c_int;
    let mut nrels: c_int;
    let mut i: c_int;
    let mut has_child: bool;

    /* Initialize output parameters to zero now, in case we exit early */
    *totalrows = 0.0;
    *totaldeadrows = 0.0;

    /*
     * Find all members of inheritance set.  We only need AccessShareLock on
     * the children.
     */
    tableOIDs =
        find_all_inheritors(RelationGetRelid(onerel), AccessShareLock, core::ptr::null_mut());

    /*
     * Check that there's at least one descendant, else fail.  This could
     * happen despite analyze_rel's relhassubclass check, if table once had a
     * child but no longer does.  In that case, we can clear the
     * relhassubclass field so as not to make the same mistake again later.
     * (This is safe because we hold ShareUpdateExclusiveLock.)
     */
    if list_length(tableOIDs) < 2 {
        /* CCI because we already updated the pg_class row in this command */
        CommandCounterIncrement();
        SetRelationHasSubclass(RelationGetRelid(onerel), false);
        ereport!(elevel,
                 errmsg!("skipping analyze of \"{}.{}\" inheritance tree --- this inheritance tree contains no child tables",
                         std::ffi::CStr::from_ptr(get_namespace_name(RelationGetNamespace(onerel))).to_string_lossy(),
                         std::ffi::CStr::from_ptr(RelationGetRelationName(onerel)).to_string_lossy()));
        return 0;
    }

    /*
     * Identify acquirefuncs to use, and count blocks in all the relations.
     * The result could overflow BlockNumber, so we use double arithmetic.
     */
    rels = palloc(list_length(tableOIDs) as usize * size_of::<Relation>()) as *mut Relation;
    acquirefuncs =
        palloc(list_length(tableOIDs) as usize * size_of::<AcquireSampleRowsFunc>())
        as *mut Option<AcquireSampleRowsFunc>;
    relblocks = palloc(list_length(tableOIDs) as usize * size_of::<f64>()) as *mut f64;
    totalblocks = 0.0;
    nrels = 0;
    has_child = false;
    foreach!(lc, tableOIDs, {
        let childOID: Oid = lfirst_oid(crate::current_cell!(lc));
        let childrel: Relation;
        let mut acquirefunc: Option<AcquireSampleRowsFunc> = None;
        let mut relpages: BlockNumber = 0;

        /* We already got the needed lock */
        childrel = table_open(childOID, NoLock);

        /* Ignore if temp table of another backend */
        if RELATION_IS_OTHER_TEMP(childrel) {
            /* ... but release the lock on it */
            Assert!(childrel != onerel);
            table_close(childrel, AccessShareLock);
            continue;
        }

        /* Check table type (MATVIEW can't happen, but might as well allow) */
        if rel_relkind(childrel) == RELKIND_RELATION ||
            rel_relkind(childrel) == RELKIND_MATVIEW {
            /* Regular table, so use the regular row acquisition function */
            acquirefunc = Some(acquire_sample_rows as AcquireSampleRowsFunc);
            relpages = RelationGetNumberOfBlocks(childrel);
        } else if rel_relkind(childrel) == RELKIND_FOREIGN_TABLE {
            /*
             * For a foreign table, call the FDW's hook function to see
             * whether it supports analysis.
             */
            let fdwroutine: *mut FdwRoutine;
            let mut ok: bool = false;

            fdwroutine = GetFdwRoutineForRelation(childrel, false);

            if (*fdwroutine).AnalyzeForeignTable.is_some() {
                ok = ((*fdwroutine).AnalyzeForeignTable.unwrap())(childrel,
                                                                  &mut acquirefunc as *mut Option<AcquireSampleRowsFunc> as *mut AcquireSampleRowsFunc,
                                                                  &mut relpages);
            }

            if !ok {
                /* ignore, but release the lock on it */
                Assert!(childrel != onerel);
                table_close(childrel, AccessShareLock);
                continue;
            }
        } else {
            /*
             * ignore, but release the lock on it.  don't try to unlock the
             * passed-in relation
             */
            Assert!(rel_relkind(childrel) == RELKIND_PARTITIONED_TABLE);
            if childrel != onerel {
                table_close(childrel, AccessShareLock);
            } else {
                table_close(childrel, NoLock);
            }
            continue;
        }

        /* OK, we'll process this child */
        has_child = true;
        *rels.add(nrels as usize) = childrel;
        *acquirefuncs.add(nrels as usize) = acquirefunc;
        *relblocks.add(nrels as usize) = relpages as f64;
        totalblocks += relpages as f64;
        nrels += 1;
    });

    /*
     * If we don't have at least one child table to consider, fail.  If the
     * relation is a partitioned table, it's not counted as a child table.
     */
    if !has_child {
        ereport!(elevel,
                 errmsg!("skipping analyze of \"{}.{}\" inheritance tree --- this inheritance tree contains no analyzable child tables",
                         std::ffi::CStr::from_ptr(get_namespace_name(RelationGetNamespace(onerel))).to_string_lossy(),
                         std::ffi::CStr::from_ptr(RelationGetRelationName(onerel)).to_string_lossy()));
        return 0;
    }

    /*
     * Now sample rows from each relation, proportionally to its fraction of
     * the total block count.  (This might be less than desirable if the child
     * rels have radically different free-space percentages, but it's not
     * clear that it's worth working harder.)
     */
    pgstat_progress_update_param(PROGRESS_ANALYZE_CHILD_TABLES_TOTAL,
                                 nrels as i64);
    numrows = 0;
    i = 0;
    while i < nrels {
        let childrel: Relation = *rels.add(i as usize);
        let acquirefunc: Option<AcquireSampleRowsFunc> = *acquirefuncs.add(i as usize);
        let childblocks: f64 = *relblocks.add(i as usize);

        /*
         * Report progress.  The sampling function will normally report blocks
         * done/total, but we need to reset them to 0 here, so that they don't
         * show an old value until that.
         */
        {
            let progress_index: [c_int; 3] = [
                PROGRESS_ANALYZE_CURRENT_CHILD_TABLE_RELID,
                PROGRESS_ANALYZE_BLOCKS_DONE,
                PROGRESS_ANALYZE_BLOCKS_TOTAL,
            ];
            let progress_vals: [i64; 3] = [
                RelationGetRelid(childrel) as i64,
                0,
                0,
            ];

            pgstat_progress_update_multi_param(3, progress_index.as_ptr(),
                                               progress_vals.as_ptr());
        }

        if childblocks > 0.0 {
            let mut childtargrows: c_int;

            childtargrows = f64::round(targrows as f64 * childblocks / totalblocks) as c_int;
            /* Make sure we don't overrun due to roundoff error */
            childtargrows = Min(childtargrows, targrows - numrows);
            if childtargrows > 0 {
                let childrows: c_int;
                let mut trows: f64 = 0.0;
                let mut tdrows: f64 = 0.0;

                /* Fetch a random sample of the child's rows */
                childrows = (acquirefunc.unwrap())(childrel, elevel,
                                                   rows.add(numrows as usize), childtargrows,
                                                   &mut trows, &mut tdrows);

                /* We may need to convert from child's rowtype to parent's */
                if childrows > 0 &&
                    !equalRowTypes(RelationGetDescr(childrel),
                                   RelationGetDescr(onerel)) {
                    let map: *mut TupleConversionMap;

                    map = convert_tuples_by_name(RelationGetDescr(childrel),
                                                 RelationGetDescr(onerel));
                    if !map.is_null() {
                        let mut j: c_int;

                        j = 0;
                        while j < childrows {
                            let newtup: HeapTuple;

                            newtup = execute_attr_map_tuple(*rows.add((numrows + j) as usize), map);
                            heap_freetuple(*rows.add((numrows + j) as usize));
                            *rows.add((numrows + j) as usize) = newtup;
                            j += 1;
                        }
                        free_conversion_map(map);
                    }
                }

                /* And add to counts */
                numrows += childrows;
                *totalrows += trows;
                *totaldeadrows += tdrows;
            }
        }

        /*
         * Note: we cannot release the child-table locks, since we may have
         * pointers to their TOAST tables in the sampled rows.
         */
        table_close(childrel, NoLock);
        pgstat_progress_update_param(PROGRESS_ANALYZE_CHILD_TABLES_DONE,
                                     (i + 1) as i64);
        i += 1;
    }

    return numrows;
}

/*
 *	update_attstats() -- update attribute statistics for one relation
 *
 *		Statistics are stored in several places: the pg_class row for the
 *		relation has stats about the whole relation, and there is a
 *		pg_statistic row for each (non-system) attribute that has ever
 *		been analyzed.  The pg_class values are updated by VACUUM, not here.
 *
 *		pg_statistic rows are just added or updated normally.  This means
 *		that pg_statistic will probably contain some deleted rows at the
 *		completion of a vacuum cycle, unless it happens to get vacuumed last.
 *
 *		To keep things simple, we punt for pg_statistic, and don't try
 *		to compute or store rows for pg_statistic itself in pg_statistic.
 *		This could possibly be made to work, but it's not worth the trouble.
 *		Note analyze_rel() has seen to it that we won't come here when
 *		vacuuming pg_statistic itself.
 *
 *		Note: there would be a race condition here if two backends could
 *		ANALYZE the same table concurrently.  Presently, we lock that out
 *		by taking a self-exclusive lock on the relation in analyze_rel().
 */
unsafe fn update_attstats(relid: Oid, inh: bool, natts: c_int,
                          vacattrstats: *mut VacAttrStatsP)
{
    let sd: Relation;
    let mut attno: c_int;
    let mut indstate: CatalogIndexState = core::ptr::null_mut();

    if natts <= 0 {
        return;					/* nothing to do */
    }

    sd = table_open(StatisticRelationId, RowExclusiveLock);

    attno = 0;
    while attno < natts {
        let stats: *mut VacAttrStats = *vacattrstats.add(attno as usize);
        let stup: HeapTuple;
        let oldtup: HeapTuple;
        let mut i: c_int;
        let mut k: c_int;
        let mut n: c_int;
        let mut values: [Datum; Natts_pg_statistic] = [0; Natts_pg_statistic];
        let mut nulls: [bool; Natts_pg_statistic] = [false; Natts_pg_statistic];
        let mut replaces: [bool; Natts_pg_statistic] = [false; Natts_pg_statistic];

        /* Ignore attr if we weren't able to collect stats */
        if !vacattrstats_stats_valid(stats) {
            attno += 1;
            continue;
        }

        /*
         * Construct a new pg_statistic tuple
         */
        i = 0;
        while (i as usize) < Natts_pg_statistic {
            nulls[i as usize] = false;
            replaces[i as usize] = true;
            i += 1;
        }

        values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
        values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(vacattrstats_tupattnum(stats) as i16);
        values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(inh);
        values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(vacattrstats_stanullfrac(stats));
        values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(vacattrstats_stawidth(stats));
        values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(vacattrstats_stadistinct(stats));
        i = Anum_pg_statistic_stakind1 as c_int - 1;
        k = 0;
        while (k as usize) < STATISTIC_NUM_SLOTS {
            values[i as usize] = Int16GetDatum(vacattrstats_stakind(stats, k as usize)); /* stakindN */
            i += 1;
            k += 1;
        }
        i = Anum_pg_statistic_staop1 as c_int - 1;
        k = 0;
        while (k as usize) < STATISTIC_NUM_SLOTS {
            values[i as usize] = ObjectIdGetDatum(vacattrstats_staop(stats, k as usize));	/* staopN */
            i += 1;
            k += 1;
        }
        i = Anum_pg_statistic_stacoll1 as c_int - 1;
        k = 0;
        while (k as usize) < STATISTIC_NUM_SLOTS {
            values[i as usize] = ObjectIdGetDatum(vacattrstats_stacoll(stats, k as usize));	/* stacollN */
            i += 1;
            k += 1;
        }
        i = Anum_pg_statistic_stanumbers1 as c_int - 1;
        k = 0;
        while (k as usize) < STATISTIC_NUM_SLOTS {
            let nnum: c_int = vacattrstats_numnumbers(stats, k as usize);

            if nnum > 0 {
                let numdatums: *mut Datum = palloc(nnum as usize * size_of::<Datum>()) as *mut Datum;
                let arry: *mut ArrayType;

                n = 0;
                while n < nnum {
                    *numdatums.add(n as usize) =
                        Float4GetDatum(vacattrstats_stanumbers(stats, k as usize, n as usize));
                    n += 1;
                }
                arry = construct_array_builtin(numdatums, nnum, FLOAT4OID);
                values[i as usize] = PointerGetDatum(arry as *mut c_void);	/* stanumbersN */
                i += 1;
            } else {
                nulls[i as usize] = true;
                values[i as usize] = 0 as Datum;
                i += 1;
            }
            k += 1;
        }
        i = Anum_pg_statistic_stavalues1 as c_int - 1;
        k = 0;
        while (k as usize) < STATISTIC_NUM_SLOTS {
            if vacattrstats_numvalues(stats, k as usize) > 0 {
                let arry: *mut ArrayType;

                arry = construct_array(vacattrstats_stavalues(stats, k as usize),
                                       vacattrstats_numvalues(stats, k as usize),
                                       vacattrstats_statypid(stats, k as usize),
                                       vacattrstats_statyplen(stats, k as usize),
                                       vacattrstats_statypbyval(stats, k as usize),
                                       vacattrstats_statypalign(stats, k as usize));
                values[i as usize] = PointerGetDatum(arry as *mut c_void);	/* stavaluesN */
                i += 1;
            } else {
                nulls[i as usize] = true;
                values[i as usize] = 0 as Datum;
                i += 1;
            }
            k += 1;
        }

        /* Is there already a pg_statistic tuple for this attribute? */
        oldtup = SearchSysCache3(STATRELATTINH,
                                 ObjectIdGetDatum(relid),
                                 Int16GetDatum(vacattrstats_tupattnum(stats) as i16),
                                 BoolGetDatum(inh));

        /* Open index information when we know we need it */
        if indstate.is_null() {
            indstate = CatalogOpenIndexes(sd);
        }

        if HeapTupleIsValid(oldtup) {
            /* Yes, replace it */
            stup = heap_modify_tuple(oldtup,
                                     RelationGetDescr(sd),
                                     values.as_mut_ptr(),
                                     nulls.as_mut_ptr(),
                                     replaces.as_mut_ptr());
            ReleaseSysCache(oldtup);
            CatalogTupleUpdateWithInfo(sd, htup_t_self(stup), stup, indstate);
        } else {
            /* No, insert new tuple */
            stup = heap_form_tuple(RelationGetDescr(sd), values.as_mut_ptr(), nulls.as_mut_ptr());
            CatalogTupleInsertWithInfo(sd, stup, indstate);
        }

        heap_freetuple(stup);
        attno += 1;
    }

    if !indstate.is_null() {
        CatalogCloseIndexes(indstate);
    }
    table_close(sd, RowExclusiveLock);
}

// Accessor stubs for update_attstats  TODO(pg-port)
#[inline] unsafe fn vacattrstats_stanullfrac(_s: *mut VacAttrStats) -> f32 { 0.0 }
#[inline] unsafe fn vacattrstats_stawidth(_s: *mut VacAttrStats) -> i32 { 0 }
#[inline] unsafe fn vacattrstats_stadistinct(_s: *mut VacAttrStats) -> f32 { 0.0 }
#[inline] unsafe fn vacattrstats_stakind(_s: *mut VacAttrStats, _k: usize) -> i16 { 0 }
#[inline] unsafe fn vacattrstats_staop(_s: *mut VacAttrStats, _k: usize) -> Oid { 0 }
#[inline] unsafe fn vacattrstats_stacoll(_s: *mut VacAttrStats, _k: usize) -> Oid { 0 }
#[inline] unsafe fn vacattrstats_numnumbers(_s: *mut VacAttrStats, _k: usize) -> c_int { 0 }
#[inline] unsafe fn vacattrstats_stanumbers(_s: *mut VacAttrStats, _k: usize, _n: usize) -> f32 { 0.0 }
#[inline] unsafe fn vacattrstats_numvalues(_s: *mut VacAttrStats, _k: usize) -> c_int { 0 }
#[inline] unsafe fn vacattrstats_stavalues(_s: *mut VacAttrStats, _k: usize) -> *mut Datum {
    core::ptr::null_mut()
}
#[inline] unsafe fn vacattrstats_statypid(_s: *mut VacAttrStats, _k: usize) -> Oid { 0 }
#[inline] unsafe fn vacattrstats_statyplen(_s: *mut VacAttrStats, _k: usize) -> i16 { 0 }
#[inline] unsafe fn vacattrstats_statypbyval(_s: *mut VacAttrStats, _k: usize) -> bool { false }
#[inline] unsafe fn vacattrstats_statypalign(_s: *mut VacAttrStats, _k: usize) -> c_char { 0 }

/*
 * std_typanalyze -- the default type-specific typanalyze function
 */
pub unsafe fn std_typanalyze(stats: *mut VacAttrStats) -> bool {
    let mut ltopr: Oid = 0;
    let mut eqopr: Oid = 0;
    let mystats: *mut StdAnalyzeData;

    /* If the attstattarget column is negative, use the default value */
    if vacattrstats_attstattarget(stats) < 0 {
        vacattrstats_set_attstattarget(stats, default_statistics_target);
    }

    /* Look for default "<" and "=" operators for column's type */
    get_sort_group_operators(vacattrstats_attrtypid(stats),
                             false, false, false,
                             &mut ltopr, &mut eqopr, core::ptr::null_mut(),
                             core::ptr::null_mut());

    /* Save the operator info for compute_stats routines */
    mystats = palloc(size_of::<StdAnalyzeData>()) as *mut StdAnalyzeData;
    stdanalyzedata_set_eqopr(mystats, eqopr);
    stdanalyzedata_set_eqfunc(mystats, if OidIsValid(eqopr) { get_opcode(eqopr) } else { InvalidOid });
    stdanalyzedata_set_ltopr(mystats, ltopr);
    vacattrstats_set_extra_data(stats, mystats as *mut c_void);

    /*
     * Determine which standard statistics algorithm to use
     */
    if OidIsValid(eqopr) && OidIsValid(ltopr) {
        /* Seems to be a scalar datatype */
        vacattrstats_set_compute_stats(stats, compute_scalar_stats);
        /*--------------------
         * The following choice of minrows is based on the paper
         * "Random sampling for histogram construction: how much is enough?"
         * by Surajit Chaudhuri, Rajeev Motwani and Vivek Narasayya, in
         * Proceedings of ACM SIGMOD International Conference on Management
         * of Data, 1998, Pages 436-447.  Their Corollary 1 to Theorem 5
         * says that for table size n, histogram size k, maximum relative
         * error in bin size f, and error probability gamma, the minimum
         * random sample size is
         *		r = 4 * k * ln(2*n/gamma) / f^2
         * Taking f = 0.5, gamma = 0.01, n = 10^6 rows, we obtain
         *		r = 305.82 * k
         * Note that because of the log function, the dependence on n is
         * quite weak; even at n = 10^12, a 300*k sample gives <= 0.66
         * bin size error with probability 0.99.  So there's no real need to
         * scale for n, which is a good thing because we don't necessarily
         * know it at this point.
         *--------------------
         */
        vacattrstats_set_minrows(stats, 300 * vacattrstats_attstattarget(stats));
    } else if OidIsValid(eqopr) {
        /* We can still recognize distinct values */
        vacattrstats_set_compute_stats(stats, compute_distinct_stats);
        /* Might as well use the same minrows as above */
        vacattrstats_set_minrows(stats, 300 * vacattrstats_attstattarget(stats));
    } else {
        /* Can't do much but the trivial stuff */
        vacattrstats_set_compute_stats(stats, compute_trivial_stats);
        /* Might as well use the same minrows as above */
        vacattrstats_set_minrows(stats, 300 * vacattrstats_attstattarget(stats));
    }

    return true;
}

// Accessor stubs for std_typanalyze  TODO(pg-port)
#[inline] unsafe fn vacattrstats_attstattarget(_s: *mut VacAttrStats) -> c_int { 0 }
#[inline] unsafe fn vacattrstats_set_extra_data(_s: *mut VacAttrStats, _v: *mut c_void) {}
#[inline] unsafe fn vacattrstats_set_compute_stats(_s: *mut VacAttrStats,
    _f: unsafe fn(VacAttrStatsP, AnalyzeAttrFetchFunc, c_int, f64)) {}
#[inline] unsafe fn vacattrstats_set_minrows(_s: *mut VacAttrStats, _v: c_int) {}
#[inline] unsafe fn stdanalyzedata_set_eqopr(_m: *mut StdAnalyzeData, _v: Oid) {}
#[inline] unsafe fn stdanalyzedata_set_eqfunc(_m: *mut StdAnalyzeData, _v: Oid) {}
#[inline] unsafe fn stdanalyzedata_set_ltopr(_m: *mut StdAnalyzeData, _v: Oid) {}

/*
 *	compute_trivial_stats() -- compute very basic column statistics
 *
 *	We use this when we cannot find a hash "=" operator for the datatype.
 *
 *	We determine the fraction of non-null rows and the average datum width.
 */
unsafe fn compute_trivial_stats(stats: VacAttrStatsP,
                                fetchfunc: AnalyzeAttrFetchFunc,
                                samplerows: c_int,
                                totalrows: f64)
{
    let mut i: c_int;
    let mut null_cnt: c_int = 0;
    let mut nonnull_cnt: c_int = 0;
    let mut total_width: f64 = 0.0;
    let is_varlena: bool = !attrtype_typbyval(vacattrstats_attrtype(stats)) &&
                           attrtype_typlen(vacattrstats_attrtype(stats)) == -1;
    let is_varwidth: bool = !attrtype_typbyval(vacattrstats_attrtype(stats)) &&
                            attrtype_typlen(vacattrstats_attrtype(stats)) < 0;

    i = 0;
    while i < samplerows {
        let value: Datum;
        let mut isnull: bool = false;

        vacuum_delay_point(true);

        value = fetchfunc(stats, i, &mut isnull);

        /* Check for null/nonnull */
        if isnull {
            null_cnt += 1;
            i += 1;
            continue;
        }
        nonnull_cnt += 1;

        /*
         * If it's a variable-width field, add up widths for average width
         * calculation.  Note that if the value is toasted, we use the toasted
         * width.  We don't bother with this calculation if it's a fixed-width
         * type.
         */
        if is_varlena {
            total_width += VARSIZE_ANY!(DatumGetPointer(value)) as f64;
        } else if is_varwidth {
            /* must be cstring */
            total_width += (libc_strlen(DatumGetCString(value)) + 1) as f64;
        }

        i += 1;
    }

    /* We can only compute average width if we found some non-null values. */
    if nonnull_cnt > 0 {
        vacattrstats_set_stats_valid(stats, true);
        /* Do the simple null-frac and width stats */
        vacattrstats_set_stanullfrac(stats, (null_cnt as f64 / samplerows as f64) as f32);
        if is_varwidth {
            vacattrstats_set_stawidth(stats, (total_width / nonnull_cnt as f64) as i32);
        } else {
            vacattrstats_set_stawidth(stats, attrtype_typlen(vacattrstats_attrtype(stats)) as i32);
        }
        vacattrstats_set_stadistinct(stats, 0.0);	/* "unknown" */
    } else if null_cnt > 0 {
        /* We found only nulls; assume the column is entirely null */
        vacattrstats_set_stats_valid(stats, true);
        vacattrstats_set_stanullfrac(stats, 1.0);
        if is_varwidth {
            vacattrstats_set_stawidth(stats, 0);	/* "unknown" */
        } else {
            vacattrstats_set_stawidth(stats, attrtype_typlen(vacattrstats_attrtype(stats)) as i32);
        }
        vacattrstats_set_stadistinct(stats, 0.0);	/* "unknown" */
    }
}

// Accessor / helper stubs for compute_trivial_stats  TODO(pg-port)
// attrtype_typbyval, attrtype_typlen, vacattrstats_attrtype,
// vacattrstats_set_stadistinct already defined above.
#[inline] unsafe fn vacattrstats_set_stats_valid(_s: *mut VacAttrStats, _v: bool) {}
#[inline] unsafe fn vacattrstats_set_stanullfrac(_s: *mut VacAttrStats, _v: f32) {}
#[inline] unsafe fn vacattrstats_set_stawidth(_s: *mut VacAttrStats, _v: i32) {}
#[inline] unsafe fn libc_strlen(s: *const c_char) -> usize {
    // TODO(pg-port): strlen
    let mut n: usize = 0;
    while *s.add(n) != 0 { n += 1; }
    n
}

/*
 *	compute_distinct_stats() -- compute column statistics including ndistinct
 *
 *	We use this when we can find only an "=" operator for the datatype.
 *
 *	We determine the fraction of non-null rows, the average width, the
 *	most common values, and the (estimated) number of distinct values.
 *
 *	The most common values are determined by brute force: we keep a list
 *	of previously seen values, ordered by number of times seen, as we scan
 *	the samples.  A newly seen value is inserted just after the last
 *	multiply-seen value, causing the bottommost (oldest) singly-seen value
 *	to drop off the list.  The accuracy of this method, and also its cost,
 *	depend mainly on the length of the list we are willing to keep.
 */
unsafe fn compute_distinct_stats(stats: VacAttrStatsP,
                                 fetchfunc: AnalyzeAttrFetchFunc,
                                 samplerows: c_int,
                                 totalrows: f64)
{
    let mut i: c_int;
    let mut null_cnt: c_int = 0;
    let mut nonnull_cnt: c_int = 0;
    let mut toowide_cnt: c_int = 0;
    let mut total_width: f64 = 0.0;
    let is_varlena: bool = !attrtype_typbyval(vacattrstats_attrtype(stats)) &&
                           attrtype_typlen(vacattrstats_attrtype(stats)) == -1;
    let is_varwidth: bool = !attrtype_typbyval(vacattrstats_attrtype(stats)) &&
                            attrtype_typlen(vacattrstats_attrtype(stats)) < 0;
    let mut f_cmpeq: FmgrInfo = core::mem::zeroed();
    let track: *mut TrackItem;
    let mut track_cnt: c_int;
    let mut track_max: c_int;
    let mut num_mcv: c_int = vacattrstats_attstattarget(stats);
    let mystats: *mut StdAnalyzeData = vacattrstats_extra_data(stats) as *mut StdAnalyzeData;

    /*
     * We track up to 2*n values for an n-element MCV list; but at least 10
     */
    track_max = 2 * num_mcv;
    if track_max < 10 {
        track_max = 10;
    }
    track = palloc(track_max as usize * size_of::<TrackItem>()) as *mut TrackItem;
    track_cnt = 0;

    fmgr_info(stdanalyzedata_eqfunc(mystats), &mut f_cmpeq);

    i = 0;
    while i < samplerows {
        let mut value: Datum;
        let mut isnull: bool = false;
        let mut r#match: bool;
        let mut firstcount1: c_int;
        let mut j: c_int;

        vacuum_delay_point(true);

        value = fetchfunc(stats, i, &mut isnull);

        /* Check for null/nonnull */
        if isnull {
            null_cnt += 1;
            i += 1;
            continue;
        }
        nonnull_cnt += 1;

        /*
         * If it's a variable-width field, add up widths for average width
         * calculation.  Note that if the value is toasted, we use the toasted
         * width.  We don't bother with this calculation if it's a fixed-width
         * type.
         */
        if is_varlena {
            total_width += VARSIZE_ANY!(DatumGetPointer(value)) as f64;

            /*
             * If the value is toasted, we want to detoast it just once to
             * avoid repeated detoastings and resultant excess memory usage
             * during the comparisons.  Also, check to see if the value is
             * excessively wide, and if so don't detoast at all --- just
             * ignore the value.
             */
            if toast_raw_datum_size(value) > WIDTH_THRESHOLD {
                toowide_cnt += 1;
                i += 1;
                continue;
            }
            value = PointerGetDatum(PG_DETOAST_DATUM!(value));
        } else if is_varwidth {
            /* must be cstring */
            total_width += (libc_strlen(DatumGetCString(value)) + 1) as f64;
        }

        /*
         * See if the value matches anything we're already tracking.
         */
        r#match = false;
        firstcount1 = track_cnt;
        j = 0;
        while j < track_cnt {
            if DatumGetBool(FunctionCall2Coll(&mut f_cmpeq,
                                              vacattrstats_attrcollid(stats),
                                              value, trackitem_value(track, j))) {
                r#match = true;
                break;
            }
            if j < firstcount1 && trackitem_count(track, j) == 1 {
                firstcount1 = j;
            }
            j += 1;
        }

        if r#match {
            /* Found a match */
            trackitem_set_count(track, j, trackitem_count(track, j) + 1);
            /* This value may now need to "bubble up" in the track list */
            while j > 0 && trackitem_count(track, j) > trackitem_count(track, j - 1) {
                let tv = trackitem_value(track, j);
                trackitem_set_value(track, j, trackitem_value(track, j - 1));
                trackitem_set_value(track, j - 1, tv);
                let tc = trackitem_count(track, j);
                trackitem_set_count(track, j, trackitem_count(track, j - 1));
                trackitem_set_count(track, j - 1, tc);
                j -= 1;
            }
        } else {
            /* No match.  Insert at head of count-1 list */
            if track_cnt < track_max {
                track_cnt += 1;
            }
            j = track_cnt - 1;
            while j > firstcount1 {
                trackitem_set_value(track, j, trackitem_value(track, j - 1));
                trackitem_set_count(track, j, trackitem_count(track, j - 1));
                j -= 1;
            }
            if firstcount1 < track_cnt {
                trackitem_set_value(track, firstcount1, value);
                trackitem_set_count(track, firstcount1, 1);
            }
        }

        i += 1;
    }

    /* We can only compute real stats if we found some non-null values. */
    if nonnull_cnt > 0 {
        let mut nmultiple: c_int;
        let mut summultiple: c_int;

        vacattrstats_set_stats_valid(stats, true);
        /* Do the simple null-frac and width stats */
        vacattrstats_set_stanullfrac(stats, (null_cnt as f64 / samplerows as f64) as f32);
        if is_varwidth {
            vacattrstats_set_stawidth(stats, (total_width / nonnull_cnt as f64) as i32);
        } else {
            vacattrstats_set_stawidth(stats, attrtype_typlen(vacattrstats_attrtype(stats)) as i32);
        }

        /* Count the number of values we found multiple times */
        summultiple = 0;
        nmultiple = 0;
        while nmultiple < track_cnt {
            if trackitem_count(track, nmultiple) == 1 {
                break;
            }
            summultiple += trackitem_count(track, nmultiple);
            nmultiple += 1;
        }

        if nmultiple == 0 {
            /*
             * If we found no repeated non-null values, assume it's a unique
             * column; but be sure to discount for any nulls we found.
             */
            vacattrstats_set_stadistinct(stats,
                (-1.0 * (1.0 - vacattrstats_stanullfrac(stats) as f64)) as f32);
        } else if track_cnt < track_max && toowide_cnt == 0 &&
                  nmultiple == track_cnt {
            /*
             * Our track list includes every value in the sample, and every
             * value appeared more than once.  Assume the column has just
             * these values.  (This case is meant to address columns with
             * small, fixed sets of possible values, such as boolean or enum
             * columns.  If there are any values that appear just once in the
             * sample, including too-wide values, we should assume that that's
             * not what we're dealing with.)
             */
            vacattrstats_set_stadistinct(stats, track_cnt as f32);
        } else {
            /*----------
             * Estimate the number of distinct values using the estimator
             * proposed by Haas and Stokes in IBM Research Report RJ 10025:
             *		n*d / (n - f1 + f1*n/N)
             * where f1 is the number of distinct values that occurred
             * exactly once in our sample of n rows (from a total of N),
             * and d is the total number of distinct values in the sample.
             * This is their Duj1 estimator; the other estimators they
             * recommend are considerably more complex, and are numerically
             * very unstable when n is much smaller than N.
             *
             * In this calculation, we consider only non-nulls.  We used to
             * include rows with null values in the n and N counts, but that
             * leads to inaccurate answers in columns with many nulls, and
             * it's intuitively bogus anyway considering the desired result is
             * the number of distinct non-null values.
             *
             * We assume (not very reliably!) that all the multiply-occurring
             * values are reflected in the final track[] list, and the other
             * nonnull values all appeared but once.  (XXX this usually
             * results in a drastic overestimate of ndistinct.  Can we do
             * any better?)
             *----------
             */
            let f1: c_int = nonnull_cnt - summultiple;
            let d: c_int = f1 + nmultiple;
            let n: f64 = (samplerows - null_cnt) as f64;
            let N: f64 = totalrows * (1.0 - vacattrstats_stanullfrac(stats) as f64);
            let mut stadistinct: f64;

            /* N == 0 shouldn't happen, but just in case ... */
            if N > 0.0 {
                stadistinct = (n * d as f64) / ((n - f1 as f64) + f1 as f64 * n / N);
            } else {
                stadistinct = 0.0;
            }

            /* Clamp to sane range in case of roundoff error */
            if stadistinct < d as f64 {
                stadistinct = d as f64;
            }
            if stadistinct > N {
                stadistinct = N;
            }
            /* And round to integer */
            vacattrstats_set_stadistinct(stats, f64::floor(stadistinct + 0.5) as f32);
        }

        /*
         * If we estimated the number of distinct values at more than 10% of
         * the total row count (a very arbitrary limit), then assume that
         * stadistinct should scale with the row count rather than be a fixed
         * value.
         */
        if vacattrstats_stadistinct(stats) as f64 > 0.1 * totalrows {
            vacattrstats_set_stadistinct(stats,
                -(vacattrstats_stadistinct(stats) as f64 / totalrows) as f32);
        }

        /*
         * Decide how many values are worth storing as most-common values. If
         * we are able to generate a complete MCV list (all the values in the
         * sample will fit, and we think these are all the ones in the table),
         * then do so.  Otherwise, store only those values that are
         * significantly more common than the values not in the list.
         *
         * Note: the first of these cases is meant to address columns with
         * small, fixed sets of possible values, such as boolean or enum
         * columns.  If we can *completely* represent the column population by
         * an MCV list that will fit into the stats target, then we should do
         * so and thus provide the planner with complete information.  But if
         * the MCV list is not complete, it's generally worth being more
         * selective, and not just filling it all the way up to the stats
         * target.
         */
        if track_cnt < track_max && toowide_cnt == 0 &&
            vacattrstats_stadistinct(stats) > 0.0 &&
            track_cnt <= num_mcv {
            /* Track list includes all values seen, and all will fit */
            num_mcv = track_cnt;
        } else {
            let mcv_counts: *mut c_int;

            /* Incomplete list; decide how many values are worth keeping */
            if num_mcv > track_cnt {
                num_mcv = track_cnt;
            }

            if num_mcv > 0 {
                mcv_counts = palloc(num_mcv as usize * size_of::<c_int>()) as *mut c_int;
                i = 0;
                while i < num_mcv {
                    *mcv_counts.add(i as usize) = trackitem_count(track, i);
                    i += 1;
                }

                num_mcv = analyze_mcv_list(mcv_counts, num_mcv,
                                           vacattrstats_stadistinct(stats) as f64,
                                           vacattrstats_stanullfrac(stats) as f64,
                                           samplerows, totalrows);
            }
        }

        /* Generate MCV slot entry */
        if num_mcv > 0 {
            let old_context: MemoryContext;
            let mcv_values: *mut Datum;
            let mcv_freqs: *mut float4;

            /* Must copy the target values into anl_context */
            old_context = MemoryContextSwitchTo(vacattrstats_anl_context(stats));
            mcv_values = palloc(num_mcv as usize * size_of::<Datum>()) as *mut Datum;
            mcv_freqs = palloc(num_mcv as usize * size_of::<float4>()) as *mut float4;
            i = 0;
            while i < num_mcv {
                *mcv_values.add(i as usize) = datumCopy(trackitem_value(track, i),
                                                        attrtype_typbyval(vacattrstats_attrtype(stats)),
                                                        attrtype_typlen(vacattrstats_attrtype(stats)));
                *mcv_freqs.add(i as usize) =
                    (trackitem_count(track, i) as f64 / samplerows as f64) as float4;
                i += 1;
            }
            MemoryContextSwitchTo(old_context);

            vacattrstats_set_stakind(stats, 0, STATISTIC_KIND_MCV);
            vacattrstats_set_staop(stats, 0, stdanalyzedata_eqopr(mystats));
            vacattrstats_set_stacoll(stats, 0, vacattrstats_attrcollid(stats));
            vacattrstats_set_stanumbers_ptr(stats, 0, mcv_freqs);
            vacattrstats_set_numnumbers(stats, 0, num_mcv);
            vacattrstats_set_stavalues_ptr(stats, 0, mcv_values);
            vacattrstats_set_numvalues(stats, 0, num_mcv);

            /*
             * Accept the defaults for stats->statypid and others. They have
             * been set before we were called (see vacuum.h)
             */
        }
    } else if null_cnt > 0 {
        /* We found only nulls; assume the column is entirely null */
        vacattrstats_set_stats_valid(stats, true);
        vacattrstats_set_stanullfrac(stats, 1.0);
        if is_varwidth {
            vacattrstats_set_stawidth(stats, 0);	/* "unknown" */
        } else {
            vacattrstats_set_stawidth(stats, attrtype_typlen(vacattrstats_attrtype(stats)) as i32);
        }
        vacattrstats_set_stadistinct(stats, 0.0);	/* "unknown" */
    }

    /* We don't need to bother cleaning up any of our temporary palloc's */
}

// TrackItem + accessor stubs for compute_distinct_stats  TODO(pg-port)
#[repr(C)]
struct TrackItem {
    value: Datum,
    count: c_int,
}
#[inline] unsafe fn trackitem_value(t: *mut TrackItem, i: c_int) -> Datum { (*t.add(i as usize)).value }
#[inline] unsafe fn trackitem_set_value(t: *mut TrackItem, i: c_int, v: Datum) { (*t.add(i as usize)).value = v; }
#[inline] unsafe fn trackitem_count(t: *mut TrackItem, i: c_int) -> c_int { (*t.add(i as usize)).count }
#[inline] unsafe fn trackitem_set_count(t: *mut TrackItem, i: c_int, v: c_int) { (*t.add(i as usize)).count = v; }
#[inline] unsafe fn vacattrstats_extra_data(_s: *mut VacAttrStats) -> *mut c_void { core::ptr::null_mut() }
#[inline] unsafe fn vacattrstats_attrcollid(_s: *mut VacAttrStats) -> Oid { 0 }
#[inline] unsafe fn vacattrstats_anl_context(_s: *mut VacAttrStats) -> MemoryContext { core::ptr::null_mut() }
#[inline] unsafe fn stdanalyzedata_eqfunc(_m: *mut StdAnalyzeData) -> Oid { 0 }
#[inline] unsafe fn stdanalyzedata_eqopr(_m: *mut StdAnalyzeData) -> Oid { 0 }
#[inline] unsafe fn stdanalyzedata_ltopr(_m: *mut StdAnalyzeData) -> Oid { 0 }
#[inline] unsafe fn vacattrstats_set_stakind(_s: *mut VacAttrStats, _k: usize, _v: i16) {}
#[inline] unsafe fn vacattrstats_set_staop(_s: *mut VacAttrStats, _k: usize, _v: Oid) {}
#[inline] unsafe fn vacattrstats_set_stacoll(_s: *mut VacAttrStats, _k: usize, _v: Oid) {}
#[inline] unsafe fn vacattrstats_set_stanumbers_ptr(_s: *mut VacAttrStats, _k: usize, _v: *mut float4) {}
#[inline] unsafe fn vacattrstats_set_numnumbers(_s: *mut VacAttrStats, _k: usize, _v: c_int) {}
#[inline] unsafe fn vacattrstats_set_stavalues_ptr(_s: *mut VacAttrStats, _k: usize, _v: *mut Datum) {}
#[inline] unsafe fn vacattrstats_set_numvalues(_s: *mut VacAttrStats, _k: usize, _v: c_int) {}

/*
 *	compute_scalar_stats() -- compute column statistics
 *
 *	We use this when we can find "=" and "<" operators for the datatype.
 *
 *	We determine the fraction of non-null rows, the average width, the
 *	most common values, the (estimated) number of distinct values, the
 *	distribution histogram, and the correlation of physical to logical order.
 *
 *	The desired stats can be determined fairly easily after sorting the
 *	data values into order.
 */
unsafe fn compute_scalar_stats(stats: VacAttrStatsP,
                               fetchfunc: AnalyzeAttrFetchFunc,
                               samplerows: c_int,
                               totalrows: f64)
{
    let mut i: c_int;
    let mut null_cnt: c_int = 0;
    let mut nonnull_cnt: c_int = 0;
    let mut toowide_cnt: c_int = 0;
    let mut total_width: f64 = 0.0;
    let is_varlena: bool = !attrtype_typbyval(vacattrstats_attrtype(stats)) &&
                           attrtype_typlen(vacattrstats_attrtype(stats)) == -1;
    let is_varwidth: bool = !attrtype_typbyval(vacattrstats_attrtype(stats)) &&
                            attrtype_typlen(vacattrstats_attrtype(stats)) < 0;
    let mut corr_xysum: f64;
    let mut ssup: SortSupportData = core::mem::zeroed();
    let values: *mut ScalarItem;
    let mut values_cnt: c_int = 0;
    let tupnoLink: *mut c_int;
    let track: *mut ScalarMCVItem;
    let mut track_cnt: c_int = 0;
    let mut num_mcv: c_int = vacattrstats_attstattarget(stats);
    let num_bins: c_int = vacattrstats_attstattarget(stats);
    let mystats: *mut StdAnalyzeData = vacattrstats_extra_data(stats) as *mut StdAnalyzeData;

    values = palloc(samplerows as usize * size_of::<ScalarItem>()) as *mut ScalarItem;
    tupnoLink = palloc(samplerows as usize * size_of::<c_int>()) as *mut c_int;
    track = palloc(num_mcv as usize * size_of::<ScalarMCVItem>()) as *mut ScalarMCVItem;

    /* memset(&ssup, 0, sizeof(ssup)); -- done by zeroed() above */
    sortsupport_set_ssup_cxt(&mut ssup, CurrentMemoryContext());
    sortsupport_set_ssup_collation(&mut ssup, vacattrstats_attrcollid(stats));
    sortsupport_set_ssup_nulls_first(&mut ssup, false);

    /*
     * For now, don't perform abbreviated key conversion, because full values
     * are required for MCV slot generation.  Supporting that optimization
     * would necessitate teaching compare_scalars() to call a tie-breaker.
     */
    sortsupport_set_abbreviate(&mut ssup, false);

    PrepareSortSupportFromOrderingOp(stdanalyzedata_ltopr(mystats), &mut ssup);

    /* Initial scan to find sortable values */
    i = 0;
    while i < samplerows {
        let mut value: Datum;
        let mut isnull: bool = false;

        vacuum_delay_point(true);

        value = fetchfunc(stats, i, &mut isnull);

        /* Check for null/nonnull */
        if isnull {
            null_cnt += 1;
            i += 1;
            continue;
        }
        nonnull_cnt += 1;

        /*
         * If it's a variable-width field, add up widths for average width
         * calculation.  Note that if the value is toasted, we use the toasted
         * width.  We don't bother with this calculation if it's a fixed-width
         * type.
         */
        if is_varlena {
            total_width += VARSIZE_ANY!(DatumGetPointer(value)) as f64;

            /*
             * If the value is toasted, we want to detoast it just once to
             * avoid repeated detoastings and resultant excess memory usage
             * during the comparisons.  Also, check to see if the value is
             * excessively wide, and if so don't detoast at all --- just
             * ignore the value.
             */
            if toast_raw_datum_size(value) > WIDTH_THRESHOLD {
                toowide_cnt += 1;
                i += 1;
                continue;
            }
            value = PointerGetDatum(PG_DETOAST_DATUM!(value));
        } else if is_varwidth {
            /* must be cstring */
            total_width += (libc_strlen(DatumGetCString(value)) + 1) as f64;
        }

        /* Add it to the list to be sorted */
        (*values.add(values_cnt as usize)).value = value;
        (*values.add(values_cnt as usize)).tupno = values_cnt;
        *tupnoLink.add(values_cnt as usize) = values_cnt;
        values_cnt += 1;

        i += 1;
    }

    /* We can only compute real stats if we found some sortable values. */
    if values_cnt > 0 {
        let mut ndistinct: c_int;	/* # distinct values in sample */
        let mut nmultiple: c_int;	/* # that appear multiple times */
        let mut num_hist: c_int;
        let mut dups_cnt: c_int;
        let mut slot_idx: c_int = 0;
        let mut cxt: CompareScalarsContext = core::mem::zeroed();

        /* Sort the collected values */
        compscalarsctx_set_ssup(&mut cxt, &mut ssup);
        compscalarsctx_set_tupnoLink(&mut cxt, tupnoLink);
        qsort_interruptible(values as *mut c_void, values_cnt as usize,
                            size_of::<ScalarItem>(),
                            compare_scalars, &mut cxt as *mut CompareScalarsContext as *mut c_void);

        /*
         * Now scan the values in order, find the most common ones, and also
         * accumulate ordering-correlation statistics.
         *
         * To determine which are most common, we first have to count the
         * number of duplicates of each value.  The duplicates are adjacent in
         * the sorted list, so a brute-force approach is to compare successive
         * datum values until we find two that are not equal. However, that
         * requires N-1 invocations of the datum comparison routine, which are
         * completely redundant with work that was done during the sort.  (The
         * sort algorithm must at some point have compared each pair of items
         * that are adjacent in the sorted order; otherwise it could not know
         * that it's ordered the pair correctly.) We exploit this by having
         * compare_scalars remember the highest tupno index that each
         * ScalarItem has been found equal to.  At the end of the sort, a
         * ScalarItem's tupnoLink will still point to itself if and only if it
         * is the last item of its group of duplicates (since the group will
         * be ordered by tupno).
         */
        corr_xysum = 0.0;
        ndistinct = 0;
        nmultiple = 0;
        dups_cnt = 0;
        i = 0;
        while i < values_cnt {
            let tupno: c_int = (*values.add(i as usize)).tupno;

            corr_xysum += (i as f64) * (tupno as f64);
            dups_cnt += 1;
            if *tupnoLink.add(tupno as usize) == tupno {
                /* Reached end of duplicates of this value */
                ndistinct += 1;
                if dups_cnt > 1 {
                    nmultiple += 1;
                    if track_cnt < num_mcv ||
                        dups_cnt > (*track.add((track_cnt - 1) as usize)).count {
                        /*
                         * Found a new item for the mcv list; find its
                         * position, bubbling down old items if needed. Loop
                         * invariant is that j points at an empty/ replaceable
                         * slot.
                         */
                        let mut j: c_int;

                        if track_cnt < num_mcv {
                            track_cnt += 1;
                        }
                        j = track_cnt - 1;
                        while j > 0 {
                            if dups_cnt <= (*track.add((j - 1) as usize)).count {
                                break;
                            }
                            (*track.add(j as usize)).count = (*track.add((j - 1) as usize)).count;
                            (*track.add(j as usize)).first = (*track.add((j - 1) as usize)).first;
                            j -= 1;
                        }
                        (*track.add(j as usize)).count = dups_cnt;
                        (*track.add(j as usize)).first = i + 1 - dups_cnt;
                    }
                }
                dups_cnt = 0;
            }
            i += 1;
        }

        vacattrstats_set_stats_valid(stats, true);
        /* Do the simple null-frac and width stats */
        vacattrstats_set_stanullfrac(stats, (null_cnt as f64 / samplerows as f64) as f32);
        if is_varwidth {
            vacattrstats_set_stawidth(stats, (total_width / nonnull_cnt as f64) as i32);
        } else {
            vacattrstats_set_stawidth(stats, attrtype_typlen(vacattrstats_attrtype(stats)) as i32);
        }

        if nmultiple == 0 {
            /*
             * If we found no repeated non-null values, assume it's a unique
             * column; but be sure to discount for any nulls we found.
             */
            vacattrstats_set_stadistinct(stats,
                (-1.0 * (1.0 - vacattrstats_stanullfrac(stats) as f64)) as f32);
        } else if toowide_cnt == 0 && nmultiple == ndistinct {
            /*
             * Every value in the sample appeared more than once.  Assume the
             * column has just these values.  (This case is meant to address
             * columns with small, fixed sets of possible values, such as
             * boolean or enum columns.  If there are any values that appear
             * just once in the sample, including too-wide values, we should
             * assume that that's not what we're dealing with.)
             */
            vacattrstats_set_stadistinct(stats, ndistinct as f32);
        } else {
            /*----------
             * Estimate the number of distinct values using the estimator
             * proposed by Haas and Stokes in IBM Research Report RJ 10025:
             *		n*d / (n - f1 + f1*n/N)
             * where f1 is the number of distinct values that occurred
             * exactly once in our sample of n rows (from a total of N),
             * and d is the total number of distinct values in the sample.
             * This is their Duj1 estimator; the other estimators they
             * recommend are considerably more complex, and are numerically
             * very unstable when n is much smaller than N.
             *
             * In this calculation, we consider only non-nulls.  We used to
             * include rows with null values in the n and N counts, but that
             * leads to inaccurate answers in columns with many nulls, and
             * it's intuitively bogus anyway considering the desired result is
             * the number of distinct non-null values.
             *
             * Overwidth values are assumed to have been distinct.
             *----------
             */
            let f1: c_int = ndistinct - nmultiple + toowide_cnt;
            let d: c_int = f1 + nmultiple;
            let n: f64 = (samplerows - null_cnt) as f64;
            let N: f64 = totalrows * (1.0 - vacattrstats_stanullfrac(stats) as f64);
            let mut stadistinct: f64;

            /* N == 0 shouldn't happen, but just in case ... */
            if N > 0.0 {
                stadistinct = (n * d as f64) / ((n - f1 as f64) + f1 as f64 * n / N);
            } else {
                stadistinct = 0.0;
            }

            /* Clamp to sane range in case of roundoff error */
            if stadistinct < d as f64 {
                stadistinct = d as f64;
            }
            if stadistinct > N {
                stadistinct = N;
            }
            /* And round to integer */
            vacattrstats_set_stadistinct(stats, f64::floor(stadistinct + 0.5) as f32);
        }

        /*
         * If we estimated the number of distinct values at more than 10% of
         * the total row count (a very arbitrary limit), then assume that
         * stadistinct should scale with the row count rather than be a fixed
         * value.
         */
        if vacattrstats_stadistinct(stats) as f64 > 0.1 * totalrows {
            vacattrstats_set_stadistinct(stats,
                -(vacattrstats_stadistinct(stats) as f64 / totalrows) as f32);
        }

        /*
         * Decide how many values are worth storing as most-common values. If
         * we are able to generate a complete MCV list (all the values in the
         * sample will fit, and we think these are all the ones in the table),
         * then do so.  Otherwise, store only those values that are
         * significantly more common than the values not in the list.
         *
         * Note: the first of these cases is meant to address columns with
         * small, fixed sets of possible values, such as boolean or enum
         * columns.  If we can *completely* represent the column population by
         * an MCV list that will fit into the stats target, then we should do
         * so and thus provide the planner with complete information.  But if
         * the MCV list is not complete, it's generally worth being more
         * selective, and not just filling it all the way up to the stats
         * target.
         */
        if track_cnt == ndistinct && toowide_cnt == 0 &&
            vacattrstats_stadistinct(stats) > 0.0 &&
            track_cnt <= num_mcv {
            /* Track list includes all values seen, and all will fit */
            num_mcv = track_cnt;
        } else {
            let mcv_counts: *mut c_int;

            /* Incomplete list; decide how many values are worth keeping */
            if num_mcv > track_cnt {
                num_mcv = track_cnt;
            }

            if num_mcv > 0 {
                mcv_counts = palloc(num_mcv as usize * size_of::<c_int>()) as *mut c_int;
                i = 0;
                while i < num_mcv {
                    *mcv_counts.add(i as usize) = (*track.add(i as usize)).count;
                    i += 1;
                }

                num_mcv = analyze_mcv_list(mcv_counts, num_mcv,
                                           vacattrstats_stadistinct(stats) as f64,
                                           vacattrstats_stanullfrac(stats) as f64,
                                           samplerows, totalrows);
            }
        }

        /* Generate MCV slot entry */
        if num_mcv > 0 {
            let old_context: MemoryContext;
            let mcv_values: *mut Datum;
            let mcv_freqs: *mut float4;

            /* Must copy the target values into anl_context */
            old_context = MemoryContextSwitchTo(vacattrstats_anl_context(stats));
            mcv_values = palloc(num_mcv as usize * size_of::<Datum>()) as *mut Datum;
            mcv_freqs = palloc(num_mcv as usize * size_of::<float4>()) as *mut float4;
            i = 0;
            while i < num_mcv {
                *mcv_values.add(i as usize) =
                    datumCopy((*values.add((*track.add(i as usize)).first as usize)).value,
                              attrtype_typbyval(vacattrstats_attrtype(stats)),
                              attrtype_typlen(vacattrstats_attrtype(stats)));
                *mcv_freqs.add(i as usize) =
                    ((*track.add(i as usize)).count as f64 / samplerows as f64) as float4;
                i += 1;
            }
            MemoryContextSwitchTo(old_context);

            vacattrstats_set_stakind(stats, slot_idx as usize, STATISTIC_KIND_MCV);
            vacattrstats_set_staop(stats, slot_idx as usize, stdanalyzedata_eqopr(mystats));
            vacattrstats_set_stacoll(stats, slot_idx as usize, vacattrstats_attrcollid(stats));
            vacattrstats_set_stanumbers_ptr(stats, slot_idx as usize, mcv_freqs);
            vacattrstats_set_numnumbers(stats, slot_idx as usize, num_mcv);
            vacattrstats_set_stavalues_ptr(stats, slot_idx as usize, mcv_values);
            vacattrstats_set_numvalues(stats, slot_idx as usize, num_mcv);

            /*
             * Accept the defaults for stats->statypid and others. They have
             * been set before we were called (see vacuum.h)
             */
            slot_idx += 1;
        }

        /*
         * Generate a histogram slot entry if there are at least two distinct
         * values not accounted for in the MCV list.  (This ensures the
         * histogram won't collapse to empty or a singleton.)
         */
        num_hist = ndistinct - num_mcv;
        if num_hist > num_bins {
            num_hist = num_bins + 1;
        }
        if num_hist >= 2 {
            let old_context: MemoryContext;
            let hist_values: *mut Datum;
            let nvals: c_int;
            let mut pos: c_int;
            let mut posfrac: c_int;
            let delta: c_int;
            let deltafrac: c_int;

            /* Sort the MCV items into position order to speed next loop */
            qsort_interruptible(track as *mut c_void, num_mcv as usize,
                                size_of::<ScalarMCVItem>(),
                                compare_mcvs, core::ptr::null_mut());

            /*
             * Collapse out the MCV items from the values[] array.
             *
             * Note we destroy the values[] array here... but we don't need it
             * for anything more.  We do, however, still need values_cnt.
             * nvals will be the number of remaining entries in values[].
             */
            if num_mcv > 0 {
                let mut src: c_int;
                let mut dest: c_int;
                let mut j: c_int;

                src = 0;
                dest = 0;
                j = 0;			/* index of next interesting MCV item */
                while src < values_cnt {
                    let ncopy: c_int;

                    if j < num_mcv {
                        let first: c_int = (*track.add(j as usize)).first;

                        if src >= first {
                            /* advance past this MCV item */
                            src = first + (*track.add(j as usize)).count;
                            j += 1;
                            continue;
                        }
                        ncopy = first - src;
                    } else {
                        ncopy = values_cnt - src;
                    }
                    core::ptr::copy(values.add(src as usize), values.add(dest as usize),
                                    ncopy as usize);
                    src += ncopy;
                    dest += ncopy;
                }
                nvals = dest;
            } else {
                nvals = values_cnt;
            }
            Assert!(nvals >= num_hist);

            /* Must copy the target values into anl_context */
            old_context = MemoryContextSwitchTo(vacattrstats_anl_context(stats));
            hist_values = palloc(num_hist as usize * size_of::<Datum>()) as *mut Datum;

            /*
             * The object of this loop is to copy the first and last values[]
             * entries along with evenly-spaced values in between.  So the
             * i'th value is values[(i * (nvals - 1)) / (num_hist - 1)].  But
             * computing that subscript directly risks integer overflow when
             * the stats target is more than a couple thousand.  Instead we
             * add (nvals - 1) / (num_hist - 1) to pos at each step, tracking
             * the integral and fractional parts of the sum separately.
             */
            delta = (nvals - 1) / (num_hist - 1);
            deltafrac = (nvals - 1) % (num_hist - 1);
            pos = 0;
            posfrac = 0;

            i = 0;
            while i < num_hist {
                *hist_values.add(i as usize) =
                    datumCopy((*values.add(pos as usize)).value,
                              attrtype_typbyval(vacattrstats_attrtype(stats)),
                              attrtype_typlen(vacattrstats_attrtype(stats)));
                pos += delta;
                posfrac += deltafrac;
                if posfrac >= (num_hist - 1) {
                    /* fractional part exceeds 1, carry to integer part */
                    pos += 1;
                    posfrac -= num_hist - 1;
                }
                i += 1;
            }

            MemoryContextSwitchTo(old_context);

            vacattrstats_set_stakind(stats, slot_idx as usize, STATISTIC_KIND_HISTOGRAM);
            vacattrstats_set_staop(stats, slot_idx as usize, stdanalyzedata_ltopr(mystats));
            vacattrstats_set_stacoll(stats, slot_idx as usize, vacattrstats_attrcollid(stats));
            vacattrstats_set_stavalues_ptr(stats, slot_idx as usize, hist_values);
            vacattrstats_set_numvalues(stats, slot_idx as usize, num_hist);

            /*
             * Accept the defaults for stats->statypid and others. They have
             * been set before we were called (see vacuum.h)
             */
            slot_idx += 1;
        }

        /* Generate a correlation entry if there are multiple values */
        if values_cnt > 1 {
            let old_context: MemoryContext;
            let corrs: *mut float4;
            let corr_xsum: f64;
            let corr_x2sum: f64;

            /* Must copy the target values into anl_context */
            old_context = MemoryContextSwitchTo(vacattrstats_anl_context(stats));
            corrs = palloc(size_of::<float4>()) as *mut float4;
            MemoryContextSwitchTo(old_context);

            /*----------
             * Since we know the x and y value sets are both
             *		0, 1, ..., values_cnt-1
             * we have sum(x) = sum(y) =
             *		(values_cnt-1)*values_cnt / 2
             * and sum(x^2) = sum(y^2) =
             *		(values_cnt-1)*values_cnt*(2*values_cnt-1) / 6.
             *----------
             */
            corr_xsum = (values_cnt - 1) as f64 *
                values_cnt as f64 / 2.0;
            corr_x2sum = (values_cnt - 1) as f64 *
                values_cnt as f64 * (2 * values_cnt - 1) as f64 / 6.0;

            /* And the correlation coefficient reduces to */
            *corrs.add(0) = ((values_cnt as f64 * corr_xysum - corr_xsum * corr_xsum) /
                (values_cnt as f64 * corr_x2sum - corr_xsum * corr_xsum)) as float4;

            vacattrstats_set_stakind(stats, slot_idx as usize, STATISTIC_KIND_CORRELATION);
            vacattrstats_set_staop(stats, slot_idx as usize, stdanalyzedata_ltopr(mystats));
            vacattrstats_set_stacoll(stats, slot_idx as usize, vacattrstats_attrcollid(stats));
            vacattrstats_set_stanumbers_ptr(stats, slot_idx as usize, corrs);
            vacattrstats_set_numnumbers(stats, slot_idx as usize, 1);
            slot_idx += 1;
        }
    } else if nonnull_cnt > 0 {
        /* We found some non-null values, but they were all too wide */
        Assert!(nonnull_cnt == toowide_cnt);
        vacattrstats_set_stats_valid(stats, true);
        /* Do the simple null-frac and width stats */
        vacattrstats_set_stanullfrac(stats, (null_cnt as f64 / samplerows as f64) as f32);
        if is_varwidth {
            vacattrstats_set_stawidth(stats, (total_width / nonnull_cnt as f64) as i32);
        } else {
            vacattrstats_set_stawidth(stats, attrtype_typlen(vacattrstats_attrtype(stats)) as i32);
        }
        /* Assume all too-wide values are distinct, so it's a unique column */
        vacattrstats_set_stadistinct(stats,
            (-1.0 * (1.0 - vacattrstats_stanullfrac(stats) as f64)) as f32);
    } else if null_cnt > 0 {
        /* We found only nulls; assume the column is entirely null */
        vacattrstats_set_stats_valid(stats, true);
        vacattrstats_set_stanullfrac(stats, 1.0);
        if is_varwidth {
            vacattrstats_set_stawidth(stats, 0);	/* "unknown" */
        } else {
            vacattrstats_set_stawidth(stats, attrtype_typlen(vacattrstats_attrtype(stats)) as i32);
        }
        vacattrstats_set_stadistinct(stats, 0.0);	/* "unknown" */
    }

    /* We don't need to bother cleaning up any of our temporary palloc's */
}

// ScalarMCVItem / CompareScalarsContext + accessor stubs  TODO(pg-port)
#[repr(C)]
struct ScalarMCVItem {
    first: c_int,
    count: c_int,
}
#[repr(C)]
struct CompareScalarsContext {
    ssup: *mut SortSupportData,
    tupnoLink: *mut c_int,
}
// CurrentMemoryContext already defined above.
#[inline] unsafe fn sortsupport_set_ssup_cxt(_s: *mut SortSupportData, _v: MemoryContext) {}
#[inline] unsafe fn sortsupport_set_ssup_collation(_s: *mut SortSupportData, _v: Oid) {}
#[inline] unsafe fn sortsupport_set_ssup_nulls_first(_s: *mut SortSupportData, _v: bool) {}
#[inline] unsafe fn sortsupport_set_abbreviate(_s: *mut SortSupportData, _v: bool) {}
#[inline] unsafe fn compscalarsctx_set_ssup(c: *mut CompareScalarsContext, v: *mut SortSupportData) { (*c).ssup = v; }
#[inline] unsafe fn compscalarsctx_set_tupnoLink(c: *mut CompareScalarsContext, v: *mut c_int) { (*c).tupnoLink = v; }

/*
 * Comparator for sorting ScalarItems
 *
 * Aside from sorting the items, we update the tupnoLink[] array
 * whenever two ScalarItems are found to contain equal datums.  The array
 * is indexed by tupno; for each ScalarItem, it contains the highest
 * tupno that that item's datum has been found to be equal to.  This allows
 * us to avoid additional comparisons in compute_scalar_stats().
 */
unsafe extern "C" fn compare_scalars(a: *const c_void, b: *const c_void, arg: *mut c_void)
    -> c_int
{
    let da: Datum = (*(a as *const ScalarItem)).value;
    let ta: c_int = (*(a as *const ScalarItem)).tupno;
    let db: Datum = (*(b as *const ScalarItem)).value;
    let tb: c_int = (*(b as *const ScalarItem)).tupno;
    let cxt: *mut CompareScalarsContext = arg as *mut CompareScalarsContext;
    let compare: c_int;

    compare = ApplySortComparator(da, false, db, false, (*cxt).ssup);
    if compare != 0 {
        return compare;
    }

    /*
     * The two datums are equal, so update cxt->tupnoLink[].
     */
    if *(*cxt).tupnoLink.add(ta as usize) < tb {
        *(*cxt).tupnoLink.add(ta as usize) = tb;
    }
    if *(*cxt).tupnoLink.add(tb as usize) < ta {
        *(*cxt).tupnoLink.add(tb as usize) = ta;
    }

    /*
     * For equal datums, sort by tupno
     */
    return ta - tb;
}

/*
 * Comparator for sorting ScalarMCVItems by position
 */
unsafe extern "C" fn compare_mcvs(a: *const c_void, b: *const c_void, arg: *mut c_void)
    -> c_int
{
    let da: c_int = (*(a as *const ScalarMCVItem)).first;
    let db: c_int = (*(b as *const ScalarMCVItem)).first;

    return da - db;
}

/*
 * Analyze the list of common values in the sample and decide how many are
 * worth storing in the table's MCV list.
 *
 * mcv_counts is assumed to be a list of the counts of the most common values
 * seen in the sample, starting with the most common.  The return value is the
 * number that are significantly more common than the values not in the list,
 * and which are therefore deemed worth storing in the table's MCV list.
 */
unsafe fn analyze_mcv_list(mcv_counts: *mut c_int,
                           mut num_mcv: c_int,
                           stadistinct: f64,
                           stanullfrac: f64,
                           samplerows: c_int,
                           totalrows: f64)
    -> c_int
{
    let mut ndistinct_table: f64;
    let mut sumcount: f64;
    let mut i: c_int;

    /*
     * If the entire table was sampled, keep the whole list.  This also
     * protects us against division by zero in the code below.
     */
    if samplerows as f64 == totalrows || totalrows <= 1.0 {
        return num_mcv;
    }

    /* Re-extract the estimated number of distinct nonnull values in table */
    ndistinct_table = stadistinct;
    if ndistinct_table < 0.0 {
        ndistinct_table = -ndistinct_table * totalrows;
    }

    /*
     * Exclude the least common values from the MCV list, if they are not
     * significantly more common than the estimated selectivity they would
     * have if they weren't in the list.  All non-MCV values are assumed to be
     * equally common, after taking into account the frequencies of all the
     * values in the MCV list and the number of nulls (c.f. eqsel()).
     *
     * Here sumcount tracks the total count of all but the last (least common)
     * value in the MCV list, allowing us to determine the effect of excluding
     * that value from the list.
     *
     * Note that we deliberately do this by removing values from the full
     * list, rather than starting with an empty list and adding values,
     * because the latter approach can fail to add any values if all the most
     * common values have around the same frequency and make up the majority
     * of the table, so that the overall average frequency of all values is
     * roughly the same as that of the common values.  This would lead to any
     * uncommon values being significantly overestimated.
     */
    sumcount = 0.0;
    i = 0;
    while i < num_mcv - 1 {
        sumcount += *mcv_counts.add(i as usize) as f64;
        i += 1;
    }

    while num_mcv > 0 {
        let mut selec: f64;
        let otherdistinct: f64;
        let N: f64;
        let n: f64;
        let K: f64;
        let variance: f64;
        let stddev: f64;

        /*
         * Estimated selectivity the least common value would have if it
         * wasn't in the MCV list (c.f. eqsel()).
         */
        selec = 1.0 - sumcount / samplerows as f64 - stanullfrac;
        if selec < 0.0 {
            selec = 0.0;
        }
        if selec > 1.0 {
            selec = 1.0;
        }
        otherdistinct = ndistinct_table - (num_mcv - 1) as f64;
        if otherdistinct > 1.0 {
            selec /= otherdistinct;
        }

        /*
         * If the value is kept in the MCV list, its population frequency is
         * assumed to equal its sample frequency.  We use the lower end of a
         * textbook continuity-corrected Wald-type confidence interval to
         * determine if that is significantly more common than the non-MCV
         * frequency --- specifically we assume the population frequency is
         * highly likely to be within around 2 standard errors of the sample
         * frequency, which equates to an interval of 2 standard deviations
         * either side of the sample count, plus an additional 0.5 for the
         * continuity correction.  Since we are sampling without replacement,
         * this is a hypergeometric distribution.
         *
         * XXX: Empirically, this approach seems to work quite well, but it
         * may be worth considering more advanced techniques for estimating
         * the confidence interval of the hypergeometric distribution.
         */
        N = totalrows;
        n = samplerows as f64;
        K = N * *mcv_counts.add((num_mcv - 1) as usize) as f64 / n;
        variance = n * K * (N - K) * (N - n) / (N * N * (N - 1.0));
        stddev = f64::sqrt(variance);

        if *mcv_counts.add((num_mcv - 1) as usize) as f64
            > selec * samplerows as f64 + 2.0 * stddev + 0.5 {
            /*
             * The value is significantly more common than the non-MCV
             * selectivity would suggest.  Keep it, and all the other more
             * common values in the list.
             */
            break;
        } else {
            /* Discard this value and consider the next least common value */
            num_mcv -= 1;
            if num_mcv == 0 {
                break;
            }
            sumcount -= *mcv_counts.add((num_mcv - 1) as usize) as f64;
        }
    }
    return num_mcv;
}
