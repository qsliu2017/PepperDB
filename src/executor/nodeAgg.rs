//! Translation of `postgres/src/backend/executor/nodeAgg.c`
//!
//! ExecAgg normally evaluates each aggregate in the following steps:
//!
//!    transvalue = initcond
//!    foreach input_tuple do
//!       transvalue = transfunc(transvalue, input_value(s))
//!    result = finalfunc(transvalue, direct_argument(s))
//!
//! If a finalfunc is not supplied then the result is just the ending
//! value of transvalue.
//!
//! Other behaviors can be selected by the "aggsplit" mode, which exists
//! to support partial aggregation.  It is possible to:
//! * Skip running the finalfunc, so that the output is always the
//! final transvalue state.
//! * Substitute the combinefunc for the transfunc, so that transvalue
//! states (propagated up from a child partial-aggregation step) are merged
//! rather than processing raw input rows.
//! * Apply the serializefunc to the output values.
//! * Apply the deserializefunc to the input values.
//!
//! Grouping sets: a list of grouping sets which is structurally equivalent
//! to a ROLLUP clause can be processed in a single pass over ordered data,
//! by keeping a separate set of transition values for each grouping set.
//!
//! Hashing can be mixed with sorted grouping.  AGG_MIXED strategy populates
//! the hashtables during the first sorted phase, and switches to reading them
//! out after completing all sort phases.
//!
//! Spilling To Disk: when the hash table memory exceeds the limit, we enter
//! "spill mode".  Tuples that would create new hash table entries are instead
//! spilled to logical tapes, partitioned by hash value bits.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]

use crate::prelude::*;

use crate::nodes::pg_list::{List, NIL};
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::primnodes::{Aggref, Var, Expr, OUTER_VAR};
use crate::nodes::plannodes::{Plan, Agg, Sort};
use crate::nodes::execnodes::PlanState;
use crate::nodes::nodes::{
    Node, NodeTag, AggStrategy, AggSplit,
    AGG_HASHED, AGG_MIXED, AGG_PLAIN, AGG_SORTED,
    DO_AGGSPLIT_COMBINE, DO_AGGSPLIT_SKIPFINAL,
    DO_AGGSPLIT_SERIALIZE, DO_AGGSPLIT_DESERIALIZE,
};
use crate::nodes::execnodes::{
    AggState, AggStatePerAgg, AggStatePerAggData,
    AggStatePerTrans, AggStatePerTransData,
    AggStatePerGroup, AggStatePerGroupData,
    AggStatePerPhase, AggStatePerPhaseData,
    AggStatePerHash, AggStatePerHashData,
    AggregateInstrumentation, SharedAggInfo,
    EState, ExprContext, ExprState, ProjectionInfo,
    TupleTableSlot,
    TupleHashTable, TupleHashEntry,
    TupleHashIterator,
    Tuplesortstate,
    LogicalTapeSet,
    HashAggSpill,
};
use crate::executor::executor::{
    EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND, EXEC_FLAG_EXPLAIN_ONLY,
    ExecInitNode, ExecEndNode, ExecProcNode, ExecReScan,
    ExecInitQual, ExecInitExprList,
    ExecEvalExpr, ExecEvalExprNoReturnSwitchContext,
    ExecQual, ExecQualAndReset, ExecProject,
    ExecInitResultTupleSlotTL, ExecInitExtraTupleSlot, ExecTypeFromTL,
    ExecAssignExprContext, ExecGetResultSlotOps, ExecGetResultType,
    ExecAssignProjectionInfo, ExecCreateScanSlotFromOuterPlan,
    ExecBuildAggTrans,
    TupleHashEntrySize, TupleHashEntryGetTuple, TupleHashEntryGetAdditional,
};
use crate::executor::execGrouping::{
    execTuplesMatchPrepare, execTuplesHashPrepare,
    BuildTupleHashTable, LookupTupleHashEntry, LookupTupleHashEntryHash,
    ResetTupleHashTable,
};
use crate::executor::execTuples::{
    TTSOpsMinimalTuple, TTSOpsVirtual,
    ExecStoreVirtualTuple, ExecStoreAllNullTuple,
    ExecStoreMinimalTuple, ExecFetchSlotMinimalTuple,
    ExecForceStoreHeapTuple,
};
use crate::executor::tuptable::{
    TupleTableSlotOps,
    ExecClearTuple, ExecCopySlotHeapTuple,
    TTS_EMPTY, TupIsNull,
    slot_getsomeattrs, slot_getallattrs,
};
use crate::access::common::heaptuple::heap_freetuple;
use crate::access::htup_details::{HeapTuple, MinimalTuple, MinimalTupleData};
use crate::utils::fmgr::{
    FmgrInfo, FunctionCallInfo, FunctionCallInfoBaseData,
    FunctionCall2Coll,
    fmgr_info,
    SizeForFunctionCallInfo,
};
use crate::postgres::NullableDatum;
// macros from crate root (macro_export):
use crate::{InitFunctionCallInfoData, FunctionCallInvoke, fmgr_info_set_expr, LOCAL_FCINFO};
use crate::utils::cache::lsyscache::{
    get_typlenbyval, get_func_name, get_opcode,
    ObjectIdGetDatum,
};
use crate::utils::cache::syscache::{
    SearchSysCache1, ReleaseSysCache, SysCacheGetAttr,
};
use crate::utils::cache::syscache_ids_gen::{AGGFNOID, PROCOID};
use crate::utils::builtins::{format_type_be, TextDatumGetCString};
use crate::utils::hash::dynahash::my_log2;
use crate::miscadmin::{CHECK_FOR_INTERRUPTS, GetUserId};
// TODO(pg-port): access/transam/parallel.h -- IsParallelWorker / ParallelWorkerNumber
#[inline] unsafe fn IsParallelWorker() -> bool { false }
static ParallelWorkerNumber: c_int = -1;
use crate::nodes::nodeFuncs::expression_tree_walker;
use crate::optimizer::util::tlist::get_sortgroupclause_tle;
use crate::pg_config_manual::FUNC_MAX_ARGS;

/*
 * Control how many partitions are created when spilling HashAgg to disk.
 *
 * HASHAGG_PARTITION_FACTOR is multiplied by the estimated number of
 * partitions needed such that each partition will fit in memory.
 */
const HASHAGG_PARTITION_FACTOR: f64 = 1.50;
const HASHAGG_MIN_PARTITIONS: i32 = 4;
const HASHAGG_MAX_PARTITIONS: i32 = 1024;

/*
 * For reading from tapes, the buffer size must be a multiple of BLCKSZ.
 */
const HASHAGG_READ_BUFFER_SIZE: usize = BLCKSZ;
const HASHAGG_WRITE_BUFFER_SIZE: usize = BLCKSZ;

/*
 * HyperLogLog bit width: 5 bits ~= 32 bytes, worst-case error ~18%.
 */
const HASHAGG_HLL_BIT_WIDTH: u8 = 5;

/*
 * Assume the palloc overhead always uses sizeof(MemoryChunk) bytes.
 */
// CHUNKHDRSZ = sizeof(MemoryChunk) -- stubbed as pointer size
const CHUNKHDRSZ: Size = core::mem::size_of::<*mut c_void>();

// BLCKSZ from pg_config_manual (8192 bytes)
const BLCKSZ: usize = 8192;

// --------------------------------------------------------------------------
// Stubs for unported subsystems
// --------------------------------------------------------------------------

/// TODO(pg-port): lib/hyperloglog.h -- hyperLogLog cardinality estimator.
#[repr(C)]
struct hyperLogLogState {
    _opaque: [u8; 32],
}

unsafe fn initHyperLogLog(_cE: *mut hyperLogLogState, _bwidth: u8) {
    crate::lib::hyperloglog::initHyperLogLog(_cE as _, _bwidth as _)
}
unsafe fn addHyperLogLog(_cE: *mut hyperLogLogState, _hash: uint32) {
    crate::lib::hyperloglog::addHyperLogLog(_cE as _, _hash as _)
}
unsafe fn estimateHyperLogLog(_cE: *mut hyperLogLogState) -> f64 {
    crate::lib::hyperloglog::estimateHyperLogLog(_cE as _) as _
}
unsafe fn freeHyperLogLog(_cE: *mut hyperLogLogState) {
    crate::lib::hyperloglog::freeHyperLogLog(_cE as _)
}

/// TODO(pg-port): common/hashfn.h hash_bytes_uint32
unsafe fn hash_bytes_uint32(k: uint32) -> uint32 {
    k // TODO(pg-port): common/hashfn
}

/// TODO(pg-port): utils/logtape.h LogicalTape (real def in utils/sort/logtape.rs)
/// Re-used from crate::utils::sort::logtape::LogicalTape
use crate::utils::sort::logtape::{
    LogicalTape,
    LogicalTapeSetCreate, LogicalTapeCreate, LogicalTapeClose,
    LogicalTapeSetClose, LogicalTapeSetBlocks,
    LogicalTapeRead, LogicalTapeWrite, LogicalTapeRewindForRead,
};

/// TODO(pg-port): tuplesort.c -- stub wrappers.
unsafe fn tuplesort_begin_heap(
    _tupDesc: *mut c_void,
    _nkeys: c_int,
    _attNums: *const i16,
    _sortOperators: *const Oid,
    _collations: *const Oid,
    _nullsFirstFlags: *const bool,
    _workMem: c_int,
    _coordinate: *mut c_void,
    _flags: c_int,
) -> *mut Tuplesortstate {
    crate::utils::sort::tuplesortvariants::tuplesort_begin_heap(_tupDesc as _, _nkeys as _, _attNums as _, _sortOperators as _, _collations as _, _nullsFirstFlags as _, _workMem as _, _coordinate as _, _flags as _) as _
}
unsafe fn tuplesort_begin_datum(
    _datumType: Oid,
    _sortOperator: Oid,
    _sortCollation: Oid,
    _nullsFirstFlag: bool,
    _workMem: c_int,
    _coordinate: *mut c_void,
    _flags: c_int,
) -> *mut Tuplesortstate {
    crate::utils::sort::tuplesortvariants::tuplesort_begin_datum(_datumType as _, _sortOperator as _, _sortCollation as _, _nullsFirstFlag as _, _workMem as _, _coordinate as _, _flags as _) as _
}
unsafe fn tuplesort_performsort(_state: *mut Tuplesortstate) {
    crate::utils::sort::tuplesort::tuplesort_performsort(_state as _)
}
unsafe fn tuplesort_end(_state: *mut Tuplesortstate) {
    crate::utils::sort::tuplesort::tuplesort_end(_state as _)
}
unsafe fn tuplesort_gettupleslot(
    _state: *mut Tuplesortstate,
    _forward: bool,
    _copy: bool,
    _slot: *mut TupleTableSlot,
    _abbrevp: *mut Datum,
) -> bool {
    crate::utils::sort::tuplesortvariants::tuplesort_gettupleslot(_state as _, _forward as _, _copy as _, _slot as _, _abbrevp as _) as _
}
unsafe fn tuplesort_getdatum(
    _state: *mut Tuplesortstate,
    _forward: bool,
    _copy: bool,
    _val: *mut Datum,
    _isNull: *mut bool,
    _abbrevp: *mut Datum,
) -> bool {
    crate::utils::sort::tuplesortvariants::tuplesort_getdatum(_state as _, _forward as _, _copy as _, _val as _, _isNull as _, _abbrevp as _) as _
}
unsafe fn tuplesort_puttupleslot(_state: *mut Tuplesortstate, _slot: *mut TupleTableSlot) {
    crate::utils::sort::tuplesortvariants::tuplesort_puttupleslot(_state as _, _slot as _)
}

/// TODO(pg-port): utils/tuplesort.h TUPLESORT_NONE
const TUPLESORT_NONE: c_int = 0;

/// TODO(pg-port): access/parallel.h -- parallel context stubs
#[repr(C)]
pub struct ParallelContext {
    pub nworkers: c_int,
    pub toc: *mut shm_toc,
    pub estimator: shm_toc_estimator,
}
use crate::storage::ipc::shm_toc::{shm_toc, shm_toc_estimator, shm_toc_allocate, shm_toc_insert};
/// TODO(pg-port): shm_toc_estimate_chunk / shm_toc_estimate_keys
unsafe fn shm_toc_estimate_chunk(_e: *mut shm_toc_estimator, _sz: Size) {
    unimplemented!()
}
unsafe fn shm_toc_estimate_keys(_e: *mut shm_toc_estimator, _cnt: Size) {
    unimplemented!()
}

#[repr(C)]
pub struct ParallelWorkerContext {
    pub toc: *mut shm_toc,
}
use crate::storage::ipc::shm_toc::shm_toc_lookup;

/// TODO(pg-port): utils/memutils.h
unsafe fn CreateWorkExprContext(_estate: *mut EState) -> *mut ExprContext {
    crate::executor::execUtils::CreateWorkExprContext(_estate as _) as _
}
unsafe fn AllocSetContextCreate(
    parent: MemoryContext,
    name: *const c_char,
    minctx: Size,
    initctx: Size,
    maxctx: Size,
) -> MemoryContext {
    crate::utils::mmgr::aset::AllocSetContextCreateInternal(parent as _, name, minctx, initctx, maxctx) as _
}
unsafe fn BumpContextCreate(
    _parent: MemoryContext,
    _name: *const c_char,
    _minctx: Size,
    _initctx: Size,
    _maxctx: Size,
) -> MemoryContext {
    crate::utils::mmgr::bump::BumpContextCreate(_parent as _, _name as _, _minctx as _, _initctx as _, _maxctx as _) as _
}
const ALLOCSET_DEFAULT_MINSIZE: Size = 0;
const ALLOCSET_DEFAULT_INITSIZE: Size = 8192;
const ALLOCSET_DEFAULT_MAXSIZE: Size = 8388608;
const ALLOCSET_DEFAULT_SIZES: (Size, Size, Size) = (
    ALLOCSET_DEFAULT_MINSIZE,
    ALLOCSET_DEFAULT_INITSIZE,
    ALLOCSET_DEFAULT_MAXSIZE,
);
unsafe fn MemoryContextDelete(_context: MemoryContext) {
    // TODO(pg-port): utils/mmgr/mcxt
}
unsafe fn MemoryContextReset(_context: MemoryContext) {
    // TODO(pg-port): utils/mmgr/mcxt
}
unsafe fn MemoryContextMemAllocated(_context: MemoryContext, _recurse: bool) -> Size {
    crate::utils::mmgr::mcxt::MemoryContextMemAllocated(_context as _, _recurse as _) as _
}
unsafe fn MemoryContextSwitchTo(context: MemoryContext) -> MemoryContext {
    context // TODO(pg-port): utils/mmgr/mcxt
}
unsafe fn ResetExprContext(_econtext: *mut ExprContext) {
    // TODO(pg-port): executor/executor
}
unsafe fn ReScanExprContext(_econtext: *mut ExprContext) {
    crate::executor::execUtils::ReScanExprContext(_econtext as _)
}
unsafe fn RegisterExprContextCallback(
    _econtext: *mut ExprContext,
    _function: ExprContextCallbackFunction,
    _arg: Datum,
) {
    // TODO(pg-port): executor/execUtils
}
type ExprContextCallbackFunction = unsafe fn(Datum);

/// TODO(pg-port): optimizer/optimizer.h
unsafe fn get_aggregate_argtypes(_aggref: *mut Aggref, _inputTypes: *mut Oid) -> c_int {
    crate::parser::parse_agg::get_aggregate_argtypes(_aggref as _, _inputTypes as _) as _
}
unsafe fn build_aggregate_transfn_expr(
    _inputTypes: *const Oid,
    _numArguments: c_int,
    _numDirectArgs: c_int,
    _aggVariadic: bool,
    _aggtranstype: Oid,
    _inputcollid: Oid,
    _transfn_oid: Oid,
    _invtransfn_oid: Oid,
    _transfnexpr: *mut *mut Expr,
    _invtransfnexpr: *mut *mut Expr,
) {
    crate::parser::parse_agg::build_aggregate_transfn_expr(_inputTypes as _, _numArguments as _, _numDirectArgs as _, _aggVariadic as _, _aggtranstype as _, _inputcollid as _, _transfn_oid as _, _invtransfn_oid as _, _transfnexpr as _, _invtransfnexpr as _)
}
unsafe fn build_aggregate_finalfn_expr(
    _inputTypes: *const Oid,
    _numFinalArgs: c_int,
    _aggtranstype: Oid,
    _aggresulttype: Oid,
    _inputcollid: Oid,
    _finalfn_oid: Oid,
    _finalfnexpr: *mut *mut Expr,
) {
    crate::parser::parse_agg::build_aggregate_finalfn_expr(_inputTypes as _, _numFinalArgs as _, _aggtranstype as _, _aggresulttype as _, _inputcollid as _, _finalfn_oid as _, _finalfnexpr as _)
}
unsafe fn build_aggregate_serialfn_expr(_serialfn_oid: Oid, _serialfnexpr: *mut *mut Expr) {
    crate::parser::parse_agg::build_aggregate_serialfn_expr(_serialfn_oid as _, _serialfnexpr as _)
}
unsafe fn build_aggregate_deserialfn_expr(
    _deserialfn_oid: Oid,
    _deserialfnexpr: *mut *mut Expr,
) {
    crate::parser::parse_agg::build_aggregate_deserialfn_expr(_deserialfn_oid as _, _deserialfnexpr as _)
}
unsafe fn AGGKIND_IS_ORDERED_SET(aggkind: c_char) -> bool {
    aggkind != 'n' as c_char // TODO(pg-port): nodes/primnodes.h AGGKIND_NORMAL='n'
}
unsafe fn IsBinaryCoercible(_srctype: Oid, _targettype: Oid) -> bool {
    crate::parser::parse_coerce::IsBinaryCoercible(_srctype as _, _targettype as _) as _
}

/// TODO(pg-port): utils/acl.h
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;
/// TODO(pg-port): catalog/objectaccess.h
unsafe fn object_aclcheck(
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mode: AclMode,
) -> AclResult {
    crate::catalog::aclchk::object_aclcheck(classid as _, objectid as _, roleid as _, mode as _) as _
}
type AclMode = u64;
const ACL_EXECUTE: AclMode = 1 << 3;
unsafe fn aclcheck_error(
    _aclerr: AclResult,
    _objtype: c_int,
    _objectname: *const c_char,
) {
    // TODO(pg-port): utils/acl
}
const OBJECT_AGGREGATE: c_int = 0;
const OBJECT_FUNCTION: c_int = 1;

/// TODO(pg-port): catalog/objectaccess.h InvokeFunctionExecuteHook
unsafe fn InvokeFunctionExecuteHook(_objectId: Oid) {
    // TODO(pg-port): catalog/objectaccess
}

use crate::catalog::pg_aggregate::{FormData_pg_aggregate, Form_pg_aggregate};
use crate::catalog::pg_proc::{FormData_pg_proc, Form_pg_proc};

/// TODO(pg-port): nodes/pg_list.h GETSTRUCT
unsafe fn GETSTRUCT<T>(tup: HeapTuple) -> *mut T {
    crate::access::htup_details::GETSTRUCT(tup as _) as _
}

/// TODO(pg-port): utils/syscache.h cache IDs
const AGGFNOID_CACHE: c_int = 1; // placeholder; real value in syscache.h
/// catalog/pg_aggregate_d.h Anum_pg_aggregate_agginitval
const Anum_pg_aggregate_agginitval: c_int = 21;

/// TODO(pg-port): catalog/pg_type.h INTERNALOID
use crate::catalog::pg_type_d::INTERNALOID;
/// TODO(pg-port): catalog/pg_class_d.h ProcedureRelationId
const ProcedureRelationId: Oid = 1255;

/// TODO(pg-port): utils/oid.h OidIsValid
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// TODO(pg-port): utils/builtins.h OidInputFunctionCall
unsafe fn OidInputFunctionCall(
    typinput: Oid,
    string: *mut c_char,
    typioparam: Oid,
    atttypmod: i32,
) -> Datum {
    crate::utils::fmgr::OidInputFunctionCall(typinput, string, typioparam, atttypmod)
}
/// utils/lsyscache.h getTypeInputInfo
unsafe fn getTypeInputInfo(type_oid: Oid, typinput: *mut Oid, typioparam: *mut Oid) {
    crate::utils::cache::lsyscache::getTypeInputInfo(type_oid, typinput, typioparam)
}

/// TODO(pg-port): utils/expandeddatum.h MakeExpandedObjectReadOnly
unsafe fn MakeExpandedObjectReadOnly(d: Datum, _isnull: bool, _typlen: i16) -> Datum {
    d // TODO(pg-port): utils/adt/expandeddatum
}

/// TODO(pg-port): utils/memutils.h pg_nextpower2_size_t / pg_prevpower2_size_t
fn pg_nextpower2_size_t(n: Size) -> Size {
    if n == 0 { return 1; }
    let mut v = n - 1;
    v |= v >> 1; v |= v >> 2; v |= v >> 4; v |= v >> 8;
    v |= v >> 16;
    if core::mem::size_of::<Size>() > 4 { v |= v >> 32; }
    v + 1
}
fn pg_prevpower2_size_t(n: Size) -> Size {
    if n == 0 { return 0; }
    let p = pg_nextpower2_size_t(n);
    if p == n { n } else { p >> 1 }
}

/// TODO(pg-port): utils/guc.h work_mem (GUC variable)
static mut work_mem: c_int = 4096;

/// TODO(pg-port): utils/memutils.h get_hash_memory_limit
unsafe fn get_hash_memory_limit() -> Size {
    (work_mem as Size) * 1024 // TODO(pg-port): real GUC-based limit
}

/// TODO(pg-port): nodes/nodeFuncs.h IsA macro
macro_rules! IsA {
    ($ptr:expr, AggState) => {
        (!($ptr).is_null() && unsafe { (*(($ptr) as *const crate::nodes::nodes::Node)).r#type == NodeTag::T_AggState })
    };
    ($ptr:expr, WindowAggState) => {
        (!($ptr).is_null() && unsafe { (*(($ptr) as *const crate::nodes::nodes::Node)).r#type == NodeTag::T_WindowAggState })
    };
    ($ptr:expr, Var) => {
        (!($ptr).is_null() && unsafe { (*(($ptr) as *const crate::nodes::nodes::Node)).r#type == NodeTag::T_Var })
    };
    ($ptr:expr, Aggref) => {
        (!($ptr).is_null() && unsafe { (*(($ptr) as *const crate::nodes::nodes::Node)).r#type == NodeTag::T_Aggref })
    };
}

/// TODO(pg-port): nodes/nodes.h castNode
macro_rules! castNode {
    ($T:ty, $ptr:expr) => { ($ptr) as *mut $T }
}

/// TODO(pg-port): nodes/nodes.h makeNode(AggState)
unsafe fn makeNode_AggState() -> *mut AggState {
    let p = palloc0(core::mem::size_of::<AggState>()) as *mut AggState;
    (*p).ss.ps.r#type = NodeTag::T_AggState;
    p
}

/// ScanTupleHashTable -- TODO(pg-port): execGrouping.c simplehash iterator.
unsafe fn ScanTupleHashTable(
    hashtable: TupleHashTable,
    iter: *mut TupleHashIterator,
) -> TupleHashEntry {
    crate::executor::execGrouping::ScanTupleHashTable(hashtable as _, iter as _) as _
}

/// ResetTupleHashIterator -- TODO(pg-port): execGrouping.c simplehash iterator.
unsafe fn ResetTupleHashIterator(
    hashtable: TupleHashTable,
    iter: *mut TupleHashIterator,
) {
    crate::executor::execGrouping::ResetTupleHashIterator(hashtable as _, iter as _)
}

unsafe fn datumCopy(value: Datum, typByVal: bool, typLen: i16) -> Datum {
    crate::utils::adt::datum::datumCopy(value, typByVal, typLen as c_int)
}

/// TODO(pg-port): nodes/pg_list.h list macros
unsafe fn lappend(list: *mut List, datum: *mut c_void) -> *mut List {
    crate::nodes::pg_list::lappend(list as _, datum as _) as _
}
unsafe fn lcons_int(datum: c_int, list: *mut List) -> *mut List {
    crate::nodes::list::lcons_int(datum as _, list as _) as _
}
unsafe fn llast(list: *mut List) -> *mut c_void {
    crate::nodes::pg_list::llast(list as _) as _
}
unsafe fn list_delete_last(list: *mut List) -> *mut List {
    crate::nodes::pg_list::list_delete_last(list as _) as _
}
unsafe fn list_free_deep(list: *mut List) {
    crate::nodes::pg_list::list_free_deep(list as _)
}
unsafe fn list_free(list: *mut List) {
    crate::nodes::pg_list::list_free(list as _)
}
unsafe fn list_nth(_list: *mut List, _n: c_int) -> *mut c_void {
    crate::nodes::pg_list::list_nth(_list as _, _n as _) as _
}
unsafe fn list_length(list: *const List) -> c_int {
    crate::nodes::pg_list::list_length(list as _) as _
}
unsafe fn list_nth_node(list: *mut List, n: c_int) -> *mut c_void {
    crate::nodes::pg_list::list_nth(list as _, n as _) as _
}
unsafe fn linitial_int(_list: *const List) -> c_int {
    crate::nodes::pg_list::linitial_int(_list as _) as _
}
unsafe fn lfirst(cell: *mut c_void) -> *mut c_void {
    crate::nodes::pg_list::lfirst(cell as _) as _
}
unsafe fn lfirst_int(cell: *mut c_void) -> c_int {
    crate::nodes::pg_list::lfirst_int(cell as _) as _
}
type ListCell = c_void;

/// TODO(pg-port): nodes/bitmapset.h
unsafe fn bms_add_member(a: *mut Bitmapset, x: c_int) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_add_member(a as _, x as _) as _
}
unsafe fn bms_add_members(a: *mut Bitmapset, b: *const Bitmapset) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_add_members(a as _, b as _) as _
}
unsafe fn bms_del_member(a: *mut Bitmapset, x: c_int) -> *mut Bitmapset {
    a // TODO(pg-port): nodes/bitmapset
}
unsafe fn bms_copy(a: *const Bitmapset) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_copy(a as _) as _
}
unsafe fn bms_union(a: *mut Bitmapset, b: *mut Bitmapset) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_union(a as _, b as _) as _
}
unsafe fn bms_free(a: *mut Bitmapset) {
    crate::nodes::bitmapset::bms_free(a as _)
}
unsafe fn bms_is_member(x: c_int, a: *const Bitmapset) -> bool {
    crate::nodes::bitmapset::bms_is_member(x as _, a as _) as _
}
unsafe fn bms_next_member(a: *const Bitmapset, prev: c_int) -> c_int {
    crate::nodes::bitmapset::bms_next_member(a as _, prev as _) as _
}
unsafe fn bms_num_members(a: *const Bitmapset) -> c_int {
    crate::nodes::bitmapset::bms_num_members(a as _) as _
}
unsafe fn bms_overlap(a: *const Bitmapset, b: *const Bitmapset) -> bool {
    crate::nodes::bitmapset::bms_overlap(a as _, b as _) as _
}

/// InstrCountFiltered1 -- TODO(pg-port): executor/instrument.h
unsafe fn InstrCountFiltered1(_node: *mut AggState, _count: f64) {
    // TODO(pg-port): executor/instrument
}

/// TODO(pg-port): utils/injection_point.h
macro_rules! INJECTION_POINT {
    ($name:expr, $p:expr) => {};
}
macro_rules! IS_INJECTION_POINT_ATTACHED {
    ($name:expr) => { false };
}
macro_rules! INJECTION_POINT_CACHED {
    ($name:expr, $p:expr) => {};
}

/// TODO(pg-port): math helper used in hash_choose_num_partitions
/// Max(a,b) -- integer max
macro_rules! Max {
    ($a:expr, $b:expr) => { if ($a) > ($b) { $a } else { $b } };
}
macro_rules! Min {
    ($a:expr, $b:expr) => { if ($a) < ($b) { $a } else { $b } };
}

/// TODO(pg-port): c stdlib MemSet
macro_rules! MemSet {
    ($ptr:expr, $val:expr, $len:expr) => {
        core::ptr::write_bytes($ptr as *mut u8, $val as u8, $len)
    };
}

/// TODO(pg-port): nodes/primnodes.h SortGroupClause
#[repr(C)]
struct SortGroupClause {
    pub sortop: Oid,
    pub eqop: Oid,
    pub nulls_first: bool,
}
/// TODO(pg-port): nodes/primnodes.h TargetEntry
#[repr(C)]
struct TargetEntry {
    pub resno: i16,
    pub expr: *mut Expr,
}

// DatumGetPointer / DatumGetBool / DatumGetUInt32 -- from prelude
#[inline] unsafe fn DatumGetPointer(d: Datum) -> *mut c_void { d as *mut c_void }
#[inline] unsafe fn DatumGetBool(d: Datum) -> bool { d != 0 }

/// Assert macro
macro_rules! Assert {
    ($e:expr) => { debug_assert!($e) };
}

// --------------------------------------------------------------------------
// Private structs (nodeAgg.c local types)
// --------------------------------------------------------------------------

// HashAggSpill is defined in crate::nodes::execnodes; imported via the execnodes use above.
// Local aliases for the field types used in spill init/finish code.

/*
 * Represents work to be done for one pass of hash aggregation (with only one
 * grouping set).
 *
 * Also tracks the bits of the hash already used for partition selection by
 * earlier iterations.
 */
#[repr(C)]
struct HashAggBatch {
    pub setno: c_int,               /* grouping set */
    pub used_bits: c_int,           /* number of bits of hash already used */
    pub input_tape: *mut LogicalTape, /* input partition tape */
    pub input_tuples: int64,        /* number of tuples in this batch */
    pub input_card: f64,            /* estimated group cardinality */
}

/* used to find referenced colnos */
#[repr(C)]
struct FindColsContext {
    pub is_aggref: bool,           /* is under an aggref */
    pub aggregated: *mut Bitmapset, /* column references under an aggref */
    pub unaggregated: *mut Bitmapset, /* other column references */
}

// --------------------------------------------------------------------------
// Fully-defined per-agg / per-trans structs (replacing opaque stubs above).
// These mirror the C definitions from nodeAgg.c private scope.
// Fields tagged `// C home: nodeAgg.c` were previously stubs in execnodes.rs.
// --------------------------------------------------------------------------

/// Full definition of AggStatePerAggData (private to nodeAgg.c).
// C home: nodeAgg.c
#[repr(C)]
pub struct AggStatePerAggDataFull {
    /// Aggref node for this aggregate
    pub aggref: *mut Aggref,
    /// index into pertrans[] array for this agg's transition state
    pub transno: c_int,
    /// is transition state shared with another aggregate?
    pub aggshared: bool,
    /// number of arguments to pass to finalfn
    pub numFinalArgs: c_int,
    /// ExprState for the final function, if any
    pub finalfn: FmgrInfo,
    /// Oid of the final function, or InvalidOid
    pub finalfn_oid: Oid,
    /// list of direct argument ExprStates (ordered-set aggs)
    pub aggdirectargs: *mut List,
    /// length of result type in bytes (-1 = varlen)
    pub resulttypeLen: i16,
    /// is result type pass-by-value?
    pub resulttypeByVal: bool,
}

/// AggStatePerTransDataFull is unified with the canonical AggStatePerTransData
/// in execnodes.rs (single shared layout for nodeAgg and the executor).
pub use crate::nodes::execnodes::AggStatePerTransData as AggStatePerTransDataFull;

/// Full definition of AggStatePerGroupData (private to nodeAgg.c).
// C home: nodeAgg.c
#[repr(C)]
pub struct AggStatePerGroupDataFull {
    /// transition value
    pub transValue: Datum,
    /// true if transValue is NULL
    pub transValueIsNull: bool,
    /// true if no non-NULL input yet
    pub noTransValue: bool,
}

/// Full definition of AggStatePerPhaseData (extends the partial stub in execnodes.rs).
// C home: nodeAgg.c
#[repr(C)]
pub struct AggStatePerPhaseDataFull {
    // -- field already in execnodes.rs stub --
    /// number of grouping sets in this phase
    pub numsets: c_int,

    // -- additional fields --
    /// strategy for this phase
    pub aggstrategy: AggStrategy,   // C home: nodeAgg.c
    /// associated Agg plan node
    pub aggnode: *mut Agg,          // C home: nodeAgg.c
    /// associated Sort plan node (NULL for phase 1)
    pub sortnode: *mut Sort,        // C home: nodeAgg.c
    /// per-grouping-set col counts
    pub gset_lengths: *mut c_int,   // C home: nodeAgg.c
    /// per-grouping-set Bitmapsets of grouped cols
    pub grouped_cols: *mut *mut Bitmapset, // C home: nodeAgg.c
    /// equality ExprStates, indexed by grouping col count
    pub eqfunctions: *mut *mut ExprState, // C home: nodeAgg.c
    /// current aggregate eval ExprState
    pub evaltrans: *mut ExprState,  // C home: nodeAgg.c
    /// cached evaltrans variants [minslot][nullcheck]
    pub evaltrans_cache: [[*mut ExprState; 2]; 2], // C home: nodeAgg.c
}

/// Full definition of AggStatePerHashData (private to nodeAgg.c).
// C home: nodeAgg.c
#[repr(C)]
pub struct AggStatePerHashDataFull {
    /// hash table for this grouping set
    pub hashtable: TupleHashTable,      // C home: nodeAgg.c
    /// iterator for scanning the hash table
    pub hashiter: TupleHashIterator,    // C home: nodeAgg.c
    /// slot for hashing grouping cols
    pub hashslot: *mut TupleTableSlot, // C home: nodeAgg.c
    /// input col indices to store in hash table
    pub hashGrpColIdxInput: *mut i16,  // C home: nodeAgg.c
    /// col indices for hashing
    pub hashGrpColIdxHash: *mut i16,   // C home: nodeAgg.c
    /// number of cols in hashGrpColIdxInput
    pub numhashGrpCols: c_int,         // C home: nodeAgg.c
    /// number of cols being hashed (GROUP BY cols)
    pub numCols: c_int,                // C home: nodeAgg.c
    /// equality function OIDs for hashing
    pub eqfuncoids: *mut Oid,          // C home: nodeAgg.c
    /// hash functions for hash cols
    pub hashfunctions: *mut FmgrInfo,  // C home: nodeAgg.c
    /// the Agg plan node for this hash grouping set
    pub aggnode: *mut Agg,             // C home: nodeAgg.c
    /// largest colno in hashGrpColIdxInput
    pub largestGrpColIdx: c_int,       // C home: nodeAgg.c
}

// AGG_CONTEXT_AGGREGATE and AGG_CONTEXT_WINDOW constants (from nodeAgg.h)
pub const AGG_CONTEXT_AGGREGATE: c_int = 1; /* regular aggregate */
pub const AGG_CONTEXT_WINDOW: c_int = 2;    /* window function */

// --------------------------------------------------------------------------
// Static helper: forward declaration trampolines
// --------------------------------------------------------------------------

/// Convenience cast: AggStatePerTransData to the full layout.
/// Until execnodes.rs is updated, callers in this file use the full struct directly.
/// When we have AggStatePerTransDataFull arrays, we address them as that type.
type PertransFull = AggStatePerTransDataFull;
type PergroupFull = AggStatePerGroupDataFull;
type PeraggFull   = AggStatePerAggDataFull;
type PerphaseFull = AggStatePerPhaseDataFull;
type PerhashFull  = AggStatePerHashDataFull;

// --------------------------------------------------------------------------
// select_current_set
// --------------------------------------------------------------------------

/*
 * Select the current grouping set; affects current_set and
 * curaggcontext.
 */
unsafe fn select_current_set(aggstate: *mut AggState, setno: c_int, is_hash: bool) {
    /*
     * When changing this, also adapt ExecAggPlainTransByVal() and
     * ExecAggPlainTransByRef().
     */
    if is_hash {
        (*aggstate).curaggcontext = (*aggstate).hashcontext;
    } else {
        (*aggstate).curaggcontext = *(*aggstate).aggcontexts.add(setno as usize);
    }
    (*aggstate).current_set = setno;
}

// --------------------------------------------------------------------------
// initialize_phase
// --------------------------------------------------------------------------

/*
 * Switch to phase "newphase", which must either be 0 or 1 (to reset) or
 * current_phase + 1. Juggle the tuplesorts accordingly.
 *
 * Phase 0 is for hashing, which we currently handle last in the AGG_MIXED
 * case, so when entering phase 0, all we need to do is drop open sorts.
 */
unsafe fn initialize_phase(aggstate: *mut AggState, newphase: c_int) {
    Assert!(newphase <= 1 || newphase == (*aggstate).current_phase + 1);

    /*
     * Whatever the previous state, we're now done with whatever input
     * tuplesort was in use.
     */
    if !(*aggstate).sort_in.is_null() {
        tuplesort_end((*aggstate).sort_in);
        (*aggstate).sort_in = std::ptr::null_mut();
    }

    if newphase <= 1 {
        /*
         * Discard any existing output tuplesort.
         */
        if !(*aggstate).sort_out.is_null() {
            tuplesort_end((*aggstate).sort_out);
            (*aggstate).sort_out = std::ptr::null_mut();
        }
    } else {
        /*
         * The old output tuplesort becomes the new input one, and this is the
         * right time to actually sort it.
         */
        (*aggstate).sort_in = (*aggstate).sort_out;
        (*aggstate).sort_out = std::ptr::null_mut();
        Assert!(!(*aggstate).sort_in.is_null());
        tuplesort_performsort((*aggstate).sort_in);
    }

    /*
     * If this isn't the last phase, we need to sort appropriately for the
     * next phase in sequence.
     */
    if newphase > 0 && newphase < (*aggstate).numphases - 1 {
        let phase_arr = (*aggstate).phases as *mut PerphaseFull;
        let sortnode = (*phase_arr.add((newphase + 1) as usize)).sortnode;
        let outerNode = outerPlanState(&raw mut (*aggstate).ss.ps);
        let tupDesc = ExecGetResultType(outerNode);

        (*aggstate).sort_out = tuplesort_begin_heap(
            tupDesc as *mut c_void,
            (*sortnode).numCols,
            (*sortnode).sortColIdx,
            (*sortnode).sortOperators,
            (*sortnode).collations,
            (*sortnode).nullsFirst,
            work_mem,
            std::ptr::null_mut(),
            TUPLESORT_NONE,
        );
    }

    (*aggstate).current_phase = newphase;
    let phase_arr = (*aggstate).phases as *mut PerphaseFull;
    (*aggstate).phase = phase_arr.add(newphase as usize) as AggStatePerPhase;
}

/*
 * Helper: outerPlanState
 */
unsafe fn outerPlanState(node: *mut PlanState) -> *mut PlanState {
    crate::nodes::execnodes::outerPlanState(node)
}

// --------------------------------------------------------------------------
// fetch_input_tuple
// --------------------------------------------------------------------------

/*
 * Fetch a tuple from either the outer plan (for phase 1) or from the sorter
 * populated by the previous phase.  Copy it to the sorter for the next phase
 * if any.
 */
unsafe fn fetch_input_tuple(aggstate: *mut AggState) -> *mut TupleTableSlot {
    let slot: *mut TupleTableSlot;

    if !(*aggstate).sort_in.is_null() {
        /* make sure we check for interrupts in either path through here */
        CHECK_FOR_INTERRUPTS();
        if !tuplesort_gettupleslot(
            (*aggstate).sort_in,
            true,
            false,
            (*aggstate).sort_slot,
            std::ptr::null_mut(),
        ) {
            return std::ptr::null_mut();
        }
        slot = (*aggstate).sort_slot;
    } else {
        slot = ExecProcNode(outerPlanState(&raw mut (*aggstate).ss.ps));
    }

    if !TupIsNull(slot) && !(*aggstate).sort_out.is_null() {
        tuplesort_puttupleslot((*aggstate).sort_out, slot);
    }

    slot
}

// --------------------------------------------------------------------------
// initialize_aggregate / initialize_aggregates
// --------------------------------------------------------------------------

/*
 * (Re)Initialize an individual aggregate.
 *
 * This function handles only one grouping set, already set in
 * aggstate->current_set.
 */
unsafe fn initialize_aggregate(
    aggstate: *mut AggState,
    pertrans: *mut PertransFull,
    pergroupstate: *mut PergroupFull,
) {
    /*
     * Start a fresh sort operation for each DISTINCT/ORDER BY aggregate.
     */
    if (*pertrans).aggsortrequired {
        /*
         * In case of rescan, maybe there could be an uncompleted sort
         * operation?  Clean it up if so.
         */
        let setno = (*aggstate).current_set as usize;
        if !(*(*pertrans).sortstates.add(setno)).is_null() {
            tuplesort_end(*(*pertrans).sortstates.add(setno));
        }

        /*
         * We use a plain Datum sorter when there's a single input column;
         * otherwise sort the full tuple.
         */
        if (*pertrans).numInputs == 1 {
            // TupleDescAttr is a function that returns Form_pg_attribute
            // attr->atttypid is the type OID
            let atttypid: Oid = 0; // TODO(pg-port): TupleDescAttr(pertrans->sortdesc, 0)->atttypid
            *(*pertrans).sortstates.add(setno) = tuplesort_begin_datum(
                atttypid,
                *(*pertrans).sortOperators.add(0),
                *(*pertrans).sortCollations.add(0),
                *(*pertrans).sortNullsFirst.add(0),
                work_mem,
                std::ptr::null_mut(),
                TUPLESORT_NONE,
            );
        } else {
            *(*pertrans).sortstates.add(setno) = tuplesort_begin_heap(
                (*pertrans).sortdesc,
                (*pertrans).numSortCols,
                (*pertrans).sortColIdx,
                (*pertrans).sortOperators,
                (*pertrans).sortCollations,
                (*pertrans).sortNullsFirst,
                work_mem,
                std::ptr::null_mut(),
                TUPLESORT_NONE,
            );
        }
    }

    /*
     * (Re)set transValue to the initial value.
     *
     * Note that when the initial value is pass-by-ref, we must copy it (into
     * the aggcontext) since we will pfree the transValue later.
     */
    if (*pertrans).initValueIsNull {
        (*pergroupstate).transValue = (*pertrans).initValue;
    } else {
        let oldContext = MemoryContextSwitchTo(
            (*(*aggstate).curaggcontext).ecxt_per_tuple_memory,
        );
        (*pergroupstate).transValue = datumCopy(
            (*pertrans).initValue,
            (*pertrans).transtypeByVal,
            (*pertrans).transtypeLen,
        );
        MemoryContextSwitchTo(oldContext);
    }
    (*pergroupstate).transValueIsNull = (*pertrans).initValueIsNull;

    /*
     * If the initial value for the transition state doesn't exist in the
     * pg_aggregate table then we will let the first non-NULL value returned
     * from the outer procNode become the initial value.
     */
    (*pergroupstate).noTransValue = (*pertrans).initValueIsNull;
}

/*
 * Initialize all aggregate transition states for a new group of input values.
 */
unsafe fn initialize_aggregates(
    aggstate: *mut AggState,
    pergroups: *mut AggStatePerGroup,
    numReset: c_int,
) {
    let numGroupingSets: c_int = Max!(
        (*((*aggstate).phase as *mut PerphaseFull)).numsets,
        1
    );
    let mut numReset = numReset;
    let numTrans: c_int = (*aggstate).numtrans;
    let transstates = (*aggstate).pertrans as *mut PertransFull;

    if numReset == 0 {
        numReset = numGroupingSets;
    }

    for setno in 0..numReset {
        let pergroup = *pergroups.add(setno as usize) as *mut PergroupFull;
        select_current_set(aggstate, setno, false);
        for transno in 0..numTrans {
            let pertrans = transstates.add(transno as usize);
            let pergroupstate = pergroup.add(transno as usize);
            initialize_aggregate(aggstate, pertrans, pergroupstate);
        }
    }
}

// --------------------------------------------------------------------------
// advance_transition_function
// --------------------------------------------------------------------------

/*
 * Given new input value(s), advance the transition function of one aggregate
 * state within one grouping set only (already set in aggstate->current_set).
 */
unsafe fn advance_transition_function(
    aggstate: *mut AggState,
    pertrans: *mut PertransFull,
    pergroupstate: *mut PergroupFull,
) {
    let fcinfo: FunctionCallInfo = (*pertrans).transfn_fcinfo;
    let newVal: Datum;

    if (*pertrans).transfn.fn_strict {
        /*
         * For a strict transfn, nothing happens when there's a NULL input; we
         * just keep the prior transValue.
         */
        let numTransInputs = (*pertrans).numTransInputs;
        for i in 1..=numTransInputs {
            if (*(*fcinfo).args.as_ptr().add(i as usize)).isnull {
                return;
            }
        }
        if (*pergroupstate).noTransValue {
            /*
             * transValue has not been initialized. This is the first non-NULL
             * input value. We use it as the initial value for transValue.
             */
            let oldContext = MemoryContextSwitchTo(
                (*(*aggstate).curaggcontext).ecxt_per_tuple_memory,
            );
            (*pergroupstate).transValue = datumCopy(
                (*(*fcinfo).args.as_ptr().add(1)).value,
                (*pertrans).transtypeByVal,
                (*pertrans).transtypeLen,
            );
            (*pergroupstate).transValueIsNull = false;
            (*pergroupstate).noTransValue = false;
            MemoryContextSwitchTo(oldContext);
            return;
        }
        if (*pergroupstate).transValueIsNull {
            /*
             * Don't call a strict function with NULL inputs.
             */
            return;
        }
    }

    /* We run the transition functions in per-input-tuple memory context */
    let oldContext = MemoryContextSwitchTo(
        (*(*aggstate).tmpcontext).ecxt_per_tuple_memory,
    );

    /* set up aggstate->curpertrans for AggGetAggref() */
    (*aggstate).curpertrans = pertrans as AggStatePerTrans;

    /*
     * OK to call the transition function
     */
    (*(*fcinfo).args.as_mut_ptr().add(0)).value = (*pergroupstate).transValue;
    (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = (*pergroupstate).transValueIsNull;
    (*fcinfo).isnull = false; /* just in case transfn doesn't set it */

    newVal = FunctionCallInvoke!(fcinfo);

    (*aggstate).curpertrans = std::ptr::null_mut();

    /*
     * If pass-by-ref datatype, must copy the new value into aggcontext and
     * free the prior transValue.  But if transfn returned a pointer to its
     * first input, we don't need to do anything.
     */
    let new_val = if !(*pertrans).transtypeByVal
        && DatumGetPointer(newVal) != DatumGetPointer((*pergroupstate).transValue)
    {
        ExecAggCopyTransValue(
            aggstate,
            pertrans,
            newVal,
            (*fcinfo).isnull,
            (*pergroupstate).transValue,
            (*pergroupstate).transValueIsNull,
        )
    } else {
        newVal
    };

    (*pergroupstate).transValue = new_val;
    (*pergroupstate).transValueIsNull = (*fcinfo).isnull;

    MemoryContextSwitchTo(oldContext);
}

/*
 * ExecAggCopyTransValue -- copy a new transition value into aggcontext and
 * free the old one if appropriate.
 * TODO(pg-port): real impl in execExpr.c / generated expression steps.
 */
unsafe fn ExecAggCopyTransValue(
    aggstate: *mut AggState,
    pertrans: *mut PertransFull,
    newValue: Datum,
    newValueIsNull: bool,
    oldValue: Datum,
    oldValueIsNull: bool,
) -> Datum {
    if !newValueIsNull {
        let oldContext = MemoryContextSwitchTo(
            (*(*aggstate).curaggcontext).ecxt_per_tuple_memory,
        );
        let result = datumCopy(newValue, (*pertrans).transtypeByVal, (*pertrans).transtypeLen);
        MemoryContextSwitchTo(oldContext);
        if !oldValueIsNull {
            pfree(DatumGetPointer(oldValue));
        }
        result
    } else {
        if !oldValueIsNull {
            pfree(DatumGetPointer(oldValue));
        }
        newValue
    }
}

// --------------------------------------------------------------------------
// advance_aggregates
// --------------------------------------------------------------------------

/*
 * Advance each aggregate transition state for one input tuple.
 */
unsafe fn advance_aggregates(aggstate: *mut AggState) {
    let phase = (*aggstate).phase as *mut PerphaseFull;
    ExecEvalExprNoReturnSwitchContext((*phase).evaltrans, (*aggstate).tmpcontext);
}

// --------------------------------------------------------------------------
// process_ordered_aggregate_single
// --------------------------------------------------------------------------

/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with only one input.
 */
unsafe fn process_ordered_aggregate_single(
    aggstate: *mut AggState,
    pertrans: *mut PertransFull,
    pergroupstate: *mut PergroupFull,
) {
    let mut oldVal: Datum = 0;
    let mut oldIsNull: bool = true;
    let mut haveOldVal: bool = false;
    let workcontext = (*(*aggstate).tmpcontext).ecxt_per_tuple_memory;
    let isDistinct: bool = (*pertrans).numDistinctCols > 0;
    let mut newAbbrevVal: Datum = 0;
    let mut oldAbbrevVal: Datum = 0;
    let fcinfo: FunctionCallInfo = (*pertrans).transfn_fcinfo;

    Assert!((*pertrans).numDistinctCols < 2);

    let setno = (*aggstate).current_set as usize;
    tuplesort_performsort(*(*pertrans).sortstates.add(setno));

    /* Load the column into argument 1 (arg 0 will be transition value) */
    let args = (*fcinfo).args.as_mut_ptr();
    let newVal = &mut (*args.add(1)).value;
    let isNull = &mut (*args.add(1)).isnull;

    /*
     * Note: if input type is pass-by-ref, the datums returned by the sort are
     * freshly palloc'd in the per-query context, so we must be careful to
     * pfree them when they are no longer needed.
     */
    while tuplesort_getdatum(
        *(*pertrans).sortstates.add(setno),
        true,
        false,
        newVal,
        isNull,
        &mut newAbbrevVal,
    ) {
        /*
         * Clear and select the working context for evaluation of the equality
         * function and transition function.
         */
        MemoryContextReset(workcontext);
        let oldContext = MemoryContextSwitchTo(workcontext);

        /*
         * If DISTINCT mode, and not distinct from prior, skip it.
         */
        if isDistinct
            && haveOldVal
            && ((oldIsNull && *isNull)
                || (!oldIsNull
                    && !*isNull
                    && oldAbbrevVal == newAbbrevVal
                    && DatumGetBool(FunctionCall2Coll(
                        &mut (*pertrans).equalfnOneFull,
                        (*pertrans).aggCollation,
                        oldVal,
                        *newVal,
                    ))))
        {
            MemoryContextSwitchTo(oldContext);
            continue;
        } else {
            advance_transition_function(aggstate, pertrans, pergroupstate);
            MemoryContextSwitchTo(oldContext);

            /*
             * Forget the old value, if any, and remember the new one for
             * subsequent equality checks.
             */
            if !(*pertrans).inputtypeByVal {
                if !oldIsNull {
                    pfree(DatumGetPointer(oldVal));
                }
                if !*isNull {
                    oldVal = datumCopy(
                        *newVal,
                        (*pertrans).inputtypeByVal,
                        (*pertrans).inputtypeLen,
                    );
                }
            } else {
                oldVal = *newVal;
            }
            oldAbbrevVal = newAbbrevVal;
            oldIsNull = *isNull;
            haveOldVal = true;
        }
    }

    if !oldIsNull && !(*pertrans).inputtypeByVal {
        pfree(DatumGetPointer(oldVal));
    }

    tuplesort_end(*(*pertrans).sortstates.add(setno));
    *(*pertrans).sortstates.add(setno) = std::ptr::null_mut();
}

// --------------------------------------------------------------------------
// process_ordered_aggregate_multi
// --------------------------------------------------------------------------

/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with more than one input.
 */
unsafe fn process_ordered_aggregate_multi(
    aggstate: *mut AggState,
    pertrans: *mut PertransFull,
    pergroupstate: *mut PergroupFull,
) {
    let tmpcontext = (*aggstate).tmpcontext;
    let fcinfo: FunctionCallInfo = (*pertrans).transfn_fcinfo;
    let mut slot1: *mut TupleTableSlot = (*pertrans).sortslot;
    let mut slot2: *mut TupleTableSlot = (*pertrans).uniqslot;
    let numTransInputs = (*pertrans).numTransInputs;
    let numDistinctCols = (*pertrans).numDistinctCols;
    let mut newAbbrevVal: Datum = 0;
    let mut oldAbbrevVal: Datum = 0;
    let mut haveOldValue: bool = false;
    let save: *mut TupleTableSlot = (*tmpcontext).ecxt_outertuple;

    let setno = (*aggstate).current_set as usize;
    tuplesort_performsort(*(*pertrans).sortstates.add(setno));

    ExecClearTuple(slot1);
    if !slot2.is_null() {
        ExecClearTuple(slot2);
    }

    while tuplesort_gettupleslot(
        *(*pertrans).sortstates.add(setno),
        true,
        true,
        slot1,
        &mut newAbbrevVal,
    ) {
        CHECK_FOR_INTERRUPTS();

        (*tmpcontext).ecxt_outertuple = slot1;
        (*tmpcontext).ecxt_innertuple = slot2;

        if numDistinctCols == 0
            || !haveOldValue
            || newAbbrevVal != oldAbbrevVal
            || !ExecQual((*pertrans).equalfnMultiFull, tmpcontext)
        {
            /*
             * Extract the first numTransInputs columns as datums to pass to
             * the transfn.
             */
            slot_getsomeattrs(slot1, numTransInputs);

            /* Load values into fcinfo */
            /* Start from 1, since the 0th arg will be the transition value */
            let args = (*fcinfo).args.as_mut_ptr();
            for i in 0..numTransInputs {
                (*args.add((i + 1) as usize)).value = (*slot1).tts_values.add(i as usize).read();
                (*args.add((i + 1) as usize)).isnull = (*slot1).tts_isnull.add(i as usize).read();
            }

            advance_transition_function(aggstate, pertrans, pergroupstate);

            if numDistinctCols > 0 {
                /* swap the slot pointers to retain the current tuple */
                let tmpslot = slot2;
                slot2 = slot1;
                slot1 = tmpslot;
                /* avoid ExecQual() calls by reusing abbreviated keys */
                oldAbbrevVal = newAbbrevVal;
                haveOldValue = true;
            }
        }

        /* Reset context each time */
        ResetExprContext(tmpcontext);

        ExecClearTuple(slot1);
    }

    if !slot2.is_null() {
        ExecClearTuple(slot2);
    }

    tuplesort_end(*(*pertrans).sortstates.add(setno));
    *(*pertrans).sortstates.add(setno) = std::ptr::null_mut();

    /* restore previous slot, potentially in use for grouping sets */
    (*tmpcontext).ecxt_outertuple = save;
}

// --------------------------------------------------------------------------
// finalize_aggregate / finalize_partialaggregate
// --------------------------------------------------------------------------

/*
 * Compute the final value of one aggregate.
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * The finalfn will be run, and the result delivered, in the
 * output-tuple context; caller's CurrentMemoryContext does not matter.
 */
unsafe fn finalize_aggregate(
    aggstate: *mut AggState,
    peragg: *mut PeraggFull,
    pergroupstate: *mut PergroupFull,
    resultVal: *mut Datum,
    resultIsNull: *mut bool,
) {
    LOCAL_FCINFO!(fcinfo, FUNC_MAX_ARGS);
    let mut anynull: bool = false;
    let pertrans = ((*aggstate).pertrans as *mut PertransFull)
        .add((*peragg).transno as usize);

    let oldContext = MemoryContextSwitchTo(
        (*(*aggstate).ss.ps.ps_ExprContext).ecxt_per_tuple_memory,
    );

    /*
     * Evaluate any direct arguments.  We do this even if there's no finalfn
     * (which is unlikely anyway), so that side-effects happen as expected.
     */
    let mut i: c_int = 1;
    // foreach(lc, peragg->aggdirectargs)
    {
        // TODO(pg-port): iterate over aggdirectargs list
        // for each ExprState expr in the list:
        //   fcinfo->args[i].value = ExecEvalExpr(expr, ps_ExprContext, &fcinfo->args[i].isnull)
        //   anynull |= fcinfo->args[i].isnull
        //   i++
    }

    /*
     * Apply the agg's finalfn if one is provided, else return transValue.
     */
    if OidIsValid((*peragg).finalfn_oid) {
        let numFinalArgs = (*peragg).numFinalArgs;

        /* set up aggstate->curperagg for AggGetAggref() */
        (*aggstate).curperagg = peragg as AggStatePerAgg;

        InitFunctionCallInfoData!(
            fcinfo,
            &mut (*peragg).finalfn,
            numFinalArgs as i16,
            (*pertrans).aggCollation,
            &raw mut (*aggstate).ss.ps as *mut Node,
            std::ptr::null_mut()
        );

        /* Fill in the transition state value */
        (*(*fcinfo).args.as_mut_ptr().add(0)).value = MakeExpandedObjectReadOnly(
            (*pergroupstate).transValue,
            (*pergroupstate).transValueIsNull,
            (*pertrans).transtypeLen,
        );
        (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = (*pergroupstate).transValueIsNull;
        anynull |= (*pergroupstate).transValueIsNull;

        /* Fill any remaining argument positions with nulls */
        while i < numFinalArgs {
            (*(*fcinfo).args.as_mut_ptr().add(i as usize)).value = 0;
            (*(*fcinfo).args.as_mut_ptr().add(i as usize)).isnull = true;
            anynull = true;
            i += 1;
        }

        if (*(*fcinfo).flinfo).fn_strict && anynull {
            /* don't call a strict function with NULL inputs */
            *resultVal = 0;
            *resultIsNull = true;
        } else {
            let result = FunctionCallInvoke!(fcinfo);
            *resultIsNull = (*fcinfo).isnull;
            *resultVal = MakeExpandedObjectReadOnly(
                result,
                (*fcinfo).isnull,
                (*peragg).resulttypeLen,
            );
        }
        (*aggstate).curperagg = std::ptr::null_mut();
    } else {
        *resultVal = MakeExpandedObjectReadOnly(
            (*pergroupstate).transValue,
            (*pergroupstate).transValueIsNull,
            (*pertrans).transtypeLen,
        );
        *resultIsNull = (*pergroupstate).transValueIsNull;
    }

    MemoryContextSwitchTo(oldContext);
}

/*
 * Compute the output value of one partial aggregate.
 *
 * The serialization function will be run, and the result delivered, in the
 * output-tuple context.
 */
unsafe fn finalize_partialaggregate(
    aggstate: *mut AggState,
    peragg: *mut PeraggFull,
    pergroupstate: *mut PergroupFull,
    resultVal: *mut Datum,
    resultIsNull: *mut bool,
) {
    let pertrans = ((*aggstate).pertrans as *mut PertransFull)
        .add((*peragg).transno as usize);

    let oldContext = MemoryContextSwitchTo(
        (*(*aggstate).ss.ps.ps_ExprContext).ecxt_per_tuple_memory,
    );

    /*
     * serialfn_oid will be set if we must serialize the transvalue before
     * returning it.
     */
    if OidIsValid((*pertrans).serialfn_oid) {
        /* Don't call a strict serialization function with NULL input. */
        if (*pertrans).serialfn.fn_strict && (*pergroupstate).transValueIsNull {
            *resultVal = 0;
            *resultIsNull = true;
        } else {
            let fcinfo: FunctionCallInfo = (*pertrans).serialfn_fcinfo;
            (*(*fcinfo).args.as_mut_ptr().add(0)).value = MakeExpandedObjectReadOnly(
                (*pergroupstate).transValue,
                (*pergroupstate).transValueIsNull,
                (*pertrans).transtypeLen,
            );
            (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = (*pergroupstate).transValueIsNull;
            (*fcinfo).isnull = false;

            let result = FunctionCallInvoke!(fcinfo);
            *resultIsNull = (*fcinfo).isnull;
            *resultVal = MakeExpandedObjectReadOnly(
                result,
                (*fcinfo).isnull,
                (*peragg).resulttypeLen,
            );
        }
    } else {
        *resultVal = MakeExpandedObjectReadOnly(
            (*pergroupstate).transValue,
            (*pergroupstate).transValueIsNull,
            (*pertrans).transtypeLen,
        );
        *resultIsNull = (*pergroupstate).transValueIsNull;
    }

    MemoryContextSwitchTo(oldContext);
}

// --------------------------------------------------------------------------
// prepare_hash_slot
// --------------------------------------------------------------------------

/*
 * Extract the attributes that make up the grouping key into the
 * hashslot.
 */
#[inline]
unsafe fn prepare_hash_slot(
    perhash: *mut PerhashFull,
    inputslot: *mut TupleTableSlot,
    hashslot: *mut TupleTableSlot,
) {
    /* transfer just the needed columns into hashslot */
    slot_getsomeattrs(inputslot, (*perhash).largestGrpColIdx);
    ExecClearTuple(hashslot);

    for i in 0..(*perhash).numhashGrpCols {
        let varNumber = (*(*perhash).hashGrpColIdxInput.add(i as usize) as c_int - 1) as usize;
        (*hashslot).tts_values.add(i as usize).write(
            (*inputslot).tts_values.add(varNumber).read(),
        );
        (*hashslot).tts_isnull.add(i as usize).write(
            (*inputslot).tts_isnull.add(varNumber).read(),
        );
    }
    ExecStoreVirtualTuple(hashslot);
}

// --------------------------------------------------------------------------
// prepare_projection_slot
// --------------------------------------------------------------------------

/*
 * Prepare to finalize and project based on the specified representative tuple
 * slot and grouping set.
 *
 * In the specified tuple slot, force to null all attributes that should be
 * read as null in the context of the current grouping set.
 */
unsafe fn prepare_projection_slot(
    aggstate: *mut AggState,
    slot: *mut TupleTableSlot,
    currentSet: c_int,
) {
    let phase = (*aggstate).phase as *mut PerphaseFull;
    if !(*phase).grouped_cols.is_null() {
        let grouped_cols = *(*phase).grouped_cols.add(currentSet as usize);
        (*aggstate).grouped_cols = grouped_cols;

        if TTS_EMPTY(slot) {
            /*
             * Force all values to be NULL if working on an empty input tuple
             */
            ExecStoreAllNullTuple(slot);
        } else if !(*aggstate).all_grouped_cols.is_null() {
            /* all_grouped_cols is arranged in desc order */
            slot_getsomeattrs(slot, linitial_int((*aggstate).all_grouped_cols));

            // foreach(lc, aggstate->all_grouped_cols)
            // TODO(pg-port): iterate list
            // for each attnum in all_grouped_cols:
            //   if !bms_is_member(attnum, grouped_cols)
            //     slot->tts_isnull[attnum - 1] = true
        }
    }
}

// --------------------------------------------------------------------------
// finalize_aggregates
// --------------------------------------------------------------------------

/*
 * Compute the final value of all aggregates for one group.
 */
unsafe fn finalize_aggregates(
    aggstate: *mut AggState,
    peraggs: *mut PeraggFull,
    pergroup: *mut PergroupFull,
) {
    let econtext = (*aggstate).ss.ps.ps_ExprContext;
    let aggvalues: *mut Datum = (*econtext).ecxt_aggvalues;
    let aggnulls: *mut bool = (*econtext).ecxt_aggnulls;
    let numtrans = (*aggstate).numtrans;
    let numaggs = (*aggstate).numaggs;
    let transstates = (*aggstate).pertrans as *mut PertransFull;

    /*
     * If there were any DISTINCT and/or ORDER BY aggregates, sort their
     * inputs and run the transition functions.
     */
    for transno in 0..numtrans {
        let pertrans = transstates.add(transno as usize);
        let pergroupstate = pergroup.add(transno as usize);

        if (*pertrans).aggsortrequired {
            Assert!(
                (*aggstate).aggstrategy != AGG_HASHED
                    && (*aggstate).aggstrategy != AGG_MIXED
            );

            if (*pertrans).numInputs == 1 {
                process_ordered_aggregate_single(aggstate, pertrans, pergroupstate);
            } else {
                process_ordered_aggregate_multi(aggstate, pertrans, pergroupstate);
            }
        } else if (*pertrans).numDistinctCols > 0 && (*pertrans).haslast {
            (*pertrans).haslast = false;

            if (*pertrans).numDistinctCols == 1 {
                if !(*pertrans).inputtypeByVal && !(*pertrans).lastisnull {
                    pfree(DatumGetPointer((*pertrans).lastdatum));
                }
                (*pertrans).lastisnull = false;
                (*pertrans).lastdatum = 0;
            } else {
                ExecClearTuple((*pertrans).uniqslot);
            }
        }
    }

    /*
     * Run the final functions.
     */
    for aggno in 0..numaggs {
        let peragg = peraggs.add(aggno as usize);
        let transno = (*peragg).transno;
        let pergroupstate = pergroup.add(transno as usize);

        if DO_AGGSPLIT_SKIPFINAL((*aggstate).aggsplit) {
            finalize_partialaggregate(
                aggstate,
                peragg,
                pergroupstate,
                aggvalues.add(aggno as usize),
                aggnulls.add(aggno as usize),
            );
        } else {
            finalize_aggregate(
                aggstate,
                peragg,
                pergroupstate,
                aggvalues.add(aggno as usize),
                aggnulls.add(aggno as usize),
            );
        }
    }
}

// --------------------------------------------------------------------------
// project_aggregates
// --------------------------------------------------------------------------

/*
 * Project the result of a group (whose aggs have already been calculated by
 * finalize_aggregates).
 */
unsafe fn project_aggregates(aggstate: *mut AggState) -> *mut TupleTableSlot {
    let econtext = (*aggstate).ss.ps.ps_ExprContext;

    /*
     * Check the qual (HAVING clause); if the group does not match, ignore it.
     */
    if ExecQual((*aggstate).ss.ps.qual, econtext) {
        /*
         * Form and return projection tuple using the aggregate results and
         * the representative input tuple.
         */
        ExecProject((*aggstate).ss.ps.ps_ProjInfo)
    } else {
        InstrCountFiltered1(aggstate, 1.0);
        std::ptr::null_mut()
    }
}

// --------------------------------------------------------------------------
// find_cols / find_cols_walker
// --------------------------------------------------------------------------

/*
 * Find input-tuple columns that are needed, dividing them into
 * aggregated and unaggregated sets.
 */
unsafe fn find_cols(
    aggstate: *mut AggState,
    aggregated: *mut *mut Bitmapset,
    unaggregated: *mut *mut Bitmapset,
) {
    let agg = (*aggstate).ss.ps.plan as *mut Agg;
    let mut context = FindColsContext {
        is_aggref: false,
        aggregated: std::ptr::null_mut(),
        unaggregated: std::ptr::null_mut(),
    };

    /* Examine tlist and quals */
    find_cols_walker((*agg).plan.targetlist as *mut Node, &mut context);
    find_cols_walker((*agg).plan.qual as *mut Node, &mut context);

    /* In some cases, grouping columns will not appear in the tlist */
    for i in 0..(*agg).numCols {
        context.unaggregated = bms_add_member(
            context.unaggregated,
            *(*agg).grpColIdx.add(i as usize) as c_int,
        );
    }

    *aggregated = context.aggregated;
    *unaggregated = context.unaggregated;
}

unsafe fn find_cols_walker(node: *mut Node, context: *mut FindColsContext) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, Var) {
        let var = node as *mut Var;
        /* setrefs.c should have set the varno to OUTER_VAR */
        Assert!((*var).varno == OUTER_VAR);
        Assert!((*var).varlevelsup == 0);
        if (*context).is_aggref {
            (*context).aggregated =
                bms_add_member((*context).aggregated, (*var).varattno as c_int);
        } else {
            (*context).unaggregated =
                bms_add_member((*context).unaggregated, (*var).varattno as c_int);
        }
        return false;
    }
    if IsA!(node, Aggref) {
        Assert!(!(*context).is_aggref);
        (*context).is_aggref = true;
        expression_tree_walker(
            node,
            Some(find_cols_walker_trampoline),
            context as *mut c_void,
        );
        (*context).is_aggref = false;
        return false;
    }
    expression_tree_walker(
        node,
        Some(find_cols_walker_trampoline),
        context as *mut c_void,
    )
}

unsafe fn find_cols_walker_trampoline(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    find_cols_walker(node, context as *mut FindColsContext)
}

// --------------------------------------------------------------------------
// build_hash_tables / build_hash_table / find_hash_columns
// --------------------------------------------------------------------------

/*
 * (Re-)initialize the hash table(s) to empty.
 */
unsafe fn build_hash_tables(aggstate: *mut AggState) {
    let perhash_arr = (*aggstate).perhash as *mut PerhashFull;
    for setno in 0..(*aggstate).num_hashes {
        let perhash = perhash_arr.add(setno as usize);
        if !(*perhash).hashtable.is_null() {
            ResetTupleHashTable((*perhash).hashtable);
            continue;
        }

        Assert!((*(*perhash).aggnode).numGroups > 0);

        let memory: Size = (*aggstate).hash_mem_limit / (*aggstate).num_hashes as Size;

        /* choose reasonable number of buckets per hashtable */
        let nbuckets = hash_choose_num_buckets(
            (*aggstate).hashentrysize,
            (*(*perhash).aggnode).numGroups as i64,
            memory,
        );

        // USE_INJECTION_POINTS
        // if IS_INJECTION_POINT_ATTACHED("hash-aggregate-oversize-table") { nbuckets = ... }

        build_hash_table(aggstate, setno, nbuckets);
    }

    (*aggstate).hash_ngroups_current = 0;
}

/*
 * Build a single hashtable for this grouping set.
 */
unsafe fn build_hash_table(aggstate: *mut AggState, setno: c_int, nbuckets: i64) {
    let perhash = ((*aggstate).perhash as *mut PerhashFull).add(setno as usize);
    let metacxt = (*aggstate).hash_metacxt;
    let tablecxt = (*aggstate).hash_tablecxt;
    let tmpcxt = (*(*aggstate).tmpcontext).ecxt_per_tuple_memory;

    Assert!(
        (*aggstate).aggstrategy == AGG_HASHED || (*aggstate).aggstrategy == AGG_MIXED
    );

    /*
     * Used to make sure initial hash table allocation does not exceed
     * hash_mem.
     */
    let additionalsize: Size =
        (*aggstate).numtrans as Size * core::mem::size_of::<AggStatePerGroupDataFull>();

    (*perhash).hashtable = BuildTupleHashTable(
        &raw mut (*aggstate).ss.ps as *mut c_void,
        (*(*perhash).hashslot).tts_tupleDescriptor as *mut c_void,
        (*(*perhash).hashslot).tts_ops,
        (*perhash).numCols,
        (*perhash).hashGrpColIdxHash,
        (*perhash).eqfuncoids,
        (*perhash).hashfunctions as *mut crate::executor::execGrouping::FmgrInfo,
        (*(*perhash).aggnode).grpCollations,
        nbuckets,
        additionalsize,
        metacxt,
        tablecxt,
        tmpcxt,
        DO_AGGSPLIT_SKIPFINAL((*aggstate).aggsplit),
    );
}

/*
 * Compute columns that actually need to be stored in hashtable entries.
 */
unsafe fn find_hash_columns(aggstate: *mut AggState) {
    let mut base_colnos: *mut Bitmapset = std::ptr::null_mut();
    let mut aggregated_colnos: *mut Bitmapset = std::ptr::null_mut();
    let scanDesc = (*(*aggstate).ss.ss_ScanTupleSlot).tts_tupleDescriptor;
    let outerTlist = (*(*outerPlanState(&raw mut (*aggstate).ss.ps)).plan).targetlist;
    let numHashes = (*aggstate).num_hashes;
    let estate = (*aggstate).ss.ps.state;
    let perhash_arr = (*aggstate).perhash as *mut PerhashFull;

    /* Find Vars that will be needed in tlist and qual */
    find_cols(aggstate, &mut aggregated_colnos, &mut base_colnos);
    (*aggstate).colnos_needed = bms_union(base_colnos, aggregated_colnos);
    (*aggstate).max_colno_needed = 0;
    (*aggstate).all_cols_needed = true;

    for i in 0..(*scanDesc).natts {
        let colno = i + 1;
        if bms_is_member(colno, (*aggstate).colnos_needed) {
            (*aggstate).max_colno_needed = colno;
        } else {
            (*aggstate).all_cols_needed = false;
        }
    }

    for j in 0..numHashes {
        let perhash = perhash_arr.add(j as usize);
        let mut colnos: *mut Bitmapset = bms_copy(base_colnos);
        let grpColIdx: *const i16 = (*(*perhash).aggnode).grpColIdx;
        let mut hashTlist: *mut List = NIL as *mut List;
        let maxCols: c_int;
        (*perhash).largestGrpColIdx = 0;

        /*
         * If we're doing grouping sets, then some Vars might be referenced in
         * tlist/qual for the benefit of other grouping sets, but not needed
         * when hashing.
         */
        let phase0 = ((*aggstate).phases as *mut PerphaseFull).add(0);
        if !(*phase0).grouped_cols.is_null() {
            let grouped_cols = *(*phase0).grouped_cols.add(j as usize);
            // foreach(lc, aggstate->all_grouped_cols) -- TODO(pg-port): list iteration
            // for each attnum in all_grouped_cols:
            //   if !bms_is_member(attnum, grouped_cols): colnos = bms_del_member(colnos, attnum)
        }

        /*
         * Compute maximum number of input columns.
         */
        maxCols = bms_num_members(colnos) + (*perhash).numCols;

        (*perhash).hashGrpColIdxInput =
            palloc(maxCols as Size * core::mem::size_of::<i16>()) as *mut i16;
        (*perhash).hashGrpColIdxHash =
            palloc((*perhash).numCols as Size * core::mem::size_of::<i16>()) as *mut i16;

        /* Add all the grouping columns to colnos */
        for i in 0..(*perhash).numCols {
            colnos = bms_add_member(colnos, *grpColIdx.add(i as usize) as c_int);
        }

        /*
         * First build mapping for columns directly hashed.
         */
        for i in 0..(*perhash).numCols {
            *(*perhash).hashGrpColIdxInput.add((*perhash).numhashGrpCols as usize) =
                *grpColIdx.add(i as usize);
            *(*perhash).hashGrpColIdxHash.add(i as usize) = (i + 1) as i16;
            (*perhash).numhashGrpCols += 1;
            /* delete already mapped columns */
            colnos = bms_del_member(colnos, *grpColIdx.add(i as usize) as c_int);
        }

        /* and add the remaining columns */
        let mut idx: c_int = -1;
        loop {
            idx = bms_next_member(colnos, idx);
            if idx < 0 { break; }
            *(*perhash).hashGrpColIdxInput.add((*perhash).numhashGrpCols as usize) =
                idx as i16;
            (*perhash).numhashGrpCols += 1;
        }

        /* and build a tuple descriptor for the hashtable */
        for i in 0..(*perhash).numhashGrpCols {
            let varNumber =
                (*(*perhash).hashGrpColIdxInput.add(i as usize) as c_int - 1) as usize;
            hashTlist = lappend(
                hashTlist,
                list_nth(outerTlist, varNumber as c_int),
            );
            (*perhash).largestGrpColIdx = Max!(
                (varNumber + 1) as c_int,
                (*perhash).largestGrpColIdx
            );
        }

        let hashDesc = ExecTypeFromTL(hashTlist);

        execTuplesHashPrepare(
            (*perhash).numCols,
            (*(*perhash).aggnode).grpOperators,
            &mut (*perhash).eqfuncoids,
            &mut (*perhash).hashfunctions as *mut *mut FmgrInfo as _,
        );
        (*perhash).hashslot = ExecAllocTableSlot(
            &mut (*estate).es_tupleTable as *mut *mut List as *mut *mut c_void,
            hashDesc as *mut c_void,
            &TTSOpsMinimalTuple,
        );

        list_free(hashTlist);
        bms_free(colnos);
    }

    bms_free(base_colnos);
}

/// TODO(pg-port): executor/execTuples.h ExecAllocTableSlot
unsafe fn ExecAllocTableSlot(
    _tupleTable: *mut *mut c_void,
    _desc: *mut c_void,
    _tts_ops: *const TupleTableSlotOps,
) -> *mut TupleTableSlot {
    crate::executor::execTuples::ExecAllocTableSlot(_tupleTable as _, _desc as _, _tts_ops as _) as _
}

/// TODO(pg-port): pg_compat.h MAXALIGN
macro_rules! MAXALIGN {
    ($n:expr) => { (($n) + 7) & !7usize };
}

// --------------------------------------------------------------------------
// hash_agg_entry_size (public)
// --------------------------------------------------------------------------

/*
 * Estimate per-hash-table-entry overhead.
 */
pub unsafe fn hash_agg_entry_size(
    numTrans: c_int,
    tupleWidth: Size,
    transitionSpace: Size,
) -> f64 {
    let tupleSize: Size = MAXALIGN!(
        core::mem::size_of::<MinimalTupleData>() + tupleWidth
    );
    let pergroupSize: Size =
        numTrans as Size * core::mem::size_of::<AggStatePerGroupDataFull>();

    let tupleChunkSize: Size = MAXALIGN!(tupleSize);
    let pergroupChunkSize: Size = pergroupSize;

    let transitionChunkSize: Size = if transitionSpace > 0 {
        CHUNKHDRSZ + pg_nextpower2_size_t(transitionSpace)
    } else {
        0
    };

    (TupleHashEntrySize()
        + tupleChunkSize
        + pergroupChunkSize
        + transitionChunkSize) as f64
}

// --------------------------------------------------------------------------
// hashagg_recompile_expressions
// --------------------------------------------------------------------------

/*
 * hashagg_recompile_expressions()
 *
 * Identifies the right phase, compiles the right expression given the
 * arguments, and then sets phase->evalfunc to that expression.
 */
unsafe fn hashagg_recompile_expressions(
    aggstate: *mut AggState,
    minslot: bool,
    nullcheck: bool,
) {
    let phase_arr = (*aggstate).phases as *mut PerphaseFull;
    let phase: *mut PerphaseFull;
    let i: usize = if minslot { 1 } else { 0 };
    let j: usize = if nullcheck { 1 } else { 0 };

    Assert!(
        (*aggstate).aggstrategy == AGG_HASHED
            || (*aggstate).aggstrategy == AGG_MIXED
    );

    if (*aggstate).aggstrategy == AGG_HASHED {
        phase = phase_arr.add(0);
    } else {
        /* AGG_MIXED */
        phase = phase_arr.add(1);
    }

    if (*phase).evaltrans_cache[i][j].is_null() {
        let outerops = (*aggstate).ss.ps.outerops;
        let outerfixed = (*aggstate).ss.ps.outeropsfixed;
        let dohash: bool = true;
        let dosort: bool;

        /*
         * If minslot is true, that means we are processing a spilled batch,
         * and we must not advance the sorted grouping sets.
         */
        if (*aggstate).aggstrategy == AGG_MIXED && !minslot {
            dosort = true;
        } else {
            dosort = false;
        }

        /* temporarily change the outerops while compiling the expression */
        if minslot {
            (*aggstate).ss.ps.outerops = &TTSOpsMinimalTuple;
            (*aggstate).ss.ps.outeropsfixed = true;
        }

        (*phase).evaltrans_cache[i][j] = ExecBuildAggTrans(
            aggstate,
            phase as *mut AggStatePerPhaseData,
            dosort,
            dohash,
            nullcheck,
        );

        /* change back */
        (*aggstate).ss.ps.outerops = outerops;
        (*aggstate).ss.ps.outeropsfixed = outerfixed;
    }

    (*phase).evaltrans = (*phase).evaltrans_cache[i][j];
}

// --------------------------------------------------------------------------
// hash_agg_set_limits (public)
// --------------------------------------------------------------------------

/*
 * Set limits that trigger spilling to avoid exceeding hash_mem. Consider the
 * number of partitions we expect to create (if we do spill).
 */
pub unsafe fn hash_agg_set_limits(
    hashentrysize: f64,
    input_groups: f64,
    used_bits: c_int,
    mem_limit: *mut Size,
    ngroups_limit: *mut uint64,
    num_partitions: *mut c_int,
) {
    let hash_mem_limit: Size = get_hash_memory_limit();

    /* if not expected to spill, use all of hash_mem */
    if input_groups * hashentrysize <= hash_mem_limit as f64 {
        if !num_partitions.is_null() {
            *num_partitions = 0;
        }
        *mem_limit = hash_mem_limit;
        *ngroups_limit = (hash_mem_limit as f64 / hashentrysize) as uint64;
        return;
    }

    /*
     * Calculate expected memory requirements for spilling.
     */
    let npartitions = hash_choose_num_partitions(
        input_groups,
        hashentrysize,
        used_bits,
        std::ptr::null_mut(),
    );
    if !num_partitions.is_null() {
        *num_partitions = npartitions;
    }

    let partition_mem: Size =
        HASHAGG_READ_BUFFER_SIZE + HASHAGG_WRITE_BUFFER_SIZE * npartitions as Size;

    /*
     * Don't set the limit below 3/4 of hash_mem.
     */
    if hash_mem_limit > 4 * partition_mem {
        *mem_limit = hash_mem_limit - partition_mem;
    } else {
        *mem_limit = (hash_mem_limit as f64 * 0.75) as Size;
    }

    if *mem_limit > hashentrysize as Size {
        *ngroups_limit = (*mem_limit as f64 / hashentrysize) as uint64;
    } else {
        *ngroups_limit = 1;
    }
}

// --------------------------------------------------------------------------
// hash_agg_check_limits / hash_agg_enter_spill_mode / hash_agg_update_metrics
// --------------------------------------------------------------------------

/*
 * After adding a new group to the hash table, check whether we need to enter
 * spill mode.
 */
unsafe fn hash_agg_check_limits(aggstate: *mut AggState) {
    let ngroups: uint64 = (*aggstate).hash_ngroups_current;
    let meta_mem: Size =
        MemoryContextMemAllocated((*aggstate).hash_metacxt, true);
    let entry_mem: Size =
        MemoryContextMemAllocated((*aggstate).hash_tablecxt, true);
    let tval_mem: Size = MemoryContextMemAllocated(
        (*(*aggstate).hashcontext).ecxt_per_tuple_memory,
        true,
    );
    let total_mem: Size = meta_mem + entry_mem + tval_mem;
    let mut do_spill: bool = false;

    // USE_INJECTION_POINTS
    // if ngroups >= 1000 && IS_INJECTION_POINT_ATTACHED("hash-aggregate-spill-1000") { do_spill = true; }

    /*
     * Don't spill unless there's at least one group in the hash table so we
     * can be sure to make progress even in edge cases.
     */
    if (*aggstate).hash_ngroups_current > 0
        && (total_mem > (*aggstate).hash_mem_limit
            || ngroups > (*aggstate).hash_ngroups_limit)
    {
        do_spill = true;
    }

    if do_spill {
        hash_agg_enter_spill_mode(aggstate);
    }
}

/*
 * Enter "spill mode", meaning that no new groups are added to any of the hash
 * tables.
 */
unsafe fn hash_agg_enter_spill_mode(aggstate: *mut AggState) {
    INJECTION_POINT!("hash-aggregate-enter-spill-mode", std::ptr::null_mut::<c_void>());
    (*aggstate).hash_spill_mode = true;
    hashagg_recompile_expressions(aggstate, (*aggstate).table_filled, true);

    if !(*aggstate).hash_ever_spilled {
        Assert!((*aggstate).hash_tapeset.is_null());
        Assert!((*aggstate).hash_spills.is_null());

        (*aggstate).hash_ever_spilled = true;

        (*aggstate).hash_tapeset = LogicalTapeSetCreate(true, std::ptr::null_mut(), -1) as *mut LogicalTapeSet;

        (*aggstate).hash_spills = palloc(
            core::mem::size_of::<HashAggSpill>() * (*aggstate).num_hashes as usize,
        ) as *mut HashAggSpill;

        let perhash_arr = (*aggstate).perhash as *mut PerhashFull;
        for setno in 0..(*aggstate).num_hashes {
            let perhash = perhash_arr.add(setno as usize);
            let spill = (*aggstate).hash_spills.add(setno as usize);
            hashagg_spill_init(
                spill,
                (*aggstate).hash_tapeset,
                0,
                (*(*perhash).aggnode).numGroups as f64,
                (*aggstate).hashentrysize,
            );
        }
    }
}

/*
 * Update metrics after filling the hash table.
 */
unsafe fn hash_agg_update_metrics(
    aggstate: *mut AggState,
    from_tape: bool,
    npartitions: c_int,
) {
    if (*aggstate).aggstrategy != AGG_MIXED && (*aggstate).aggstrategy != AGG_HASHED {
        return;
    }

    /* memory for the hash table itself */
    let meta_mem: Size = MemoryContextMemAllocated((*aggstate).hash_metacxt, true);
    /* memory for hash entries */
    let entry_mem: Size = MemoryContextMemAllocated((*aggstate).hash_tablecxt, true);
    /* memory for byref transition states */
    let hashkey_mem: Size = MemoryContextMemAllocated(
        (*(*aggstate).hashcontext).ecxt_per_tuple_memory,
        true,
    );
    /* memory for read/write tape buffers, if spilled */
    let mut buffer_mem: Size = npartitions as Size * HASHAGG_WRITE_BUFFER_SIZE;
    if from_tape {
        buffer_mem += HASHAGG_READ_BUFFER_SIZE;
    }

    /* update peak mem */
    let total_mem: Size = meta_mem + entry_mem + hashkey_mem + buffer_mem;
    if total_mem > (*aggstate).hash_mem_peak {
        (*aggstate).hash_mem_peak = total_mem;
    }

    /* update disk usage */
    if !(*aggstate).hash_tapeset.is_null() {
        let disk_used: uint64 =
            LogicalTapeSetBlocks((*aggstate).hash_tapeset as *mut crate::utils::sort::logtape::LogicalTapeSet) as uint64 * (BLCKSZ as uint64 / 1024);
        if (*aggstate).hash_disk_used < disk_used {
            (*aggstate).hash_disk_used = disk_used;
        }
    }

    /* update hashentrysize estimate based on contents */
    if (*aggstate).hash_ngroups_current > 0 {
        (*aggstate).hashentrysize = TupleHashEntrySize() as f64
            + (hashkey_mem as f64 / (*aggstate).hash_ngroups_current as f64);
    }
}

// --------------------------------------------------------------------------
// hash_create_memory
// --------------------------------------------------------------------------

/*
 * Create memory contexts used for hash aggregation.
 */
unsafe fn hash_create_memory(aggstate: *mut AggState) {
    let mut maxBlockSize: Size = ALLOCSET_DEFAULT_MAXSIZE;

    /*
     * The hashcontext's per-tuple memory will be used for byref transition
     * values and returned by AggCheckCallContext().
     */
    (*aggstate).hashcontext = CreateWorkExprContext((*aggstate).ss.ps.state);

    /*
     * The meta context will be used for the bucket array.
     */
    (*aggstate).hash_metacxt = AllocSetContextCreate(
        (*(*aggstate).ss.ps.state).es_query_cxt,
        b"HashAgg meta context\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
    );

    /*
     * The hash entries themselves are stored in the table context.
     * Like CreateWorkExprContext(), use smaller sizings for smaller work_mem.
     */
    maxBlockSize = pg_prevpower2_size_t(work_mem as Size * 1024 / 16);
    /* But no bigger than ALLOCSET_DEFAULT_MAXSIZE */
    maxBlockSize = Min!(maxBlockSize, ALLOCSET_DEFAULT_MAXSIZE);
    /* and no smaller than ALLOCSET_DEFAULT_INITSIZE */
    maxBlockSize = Max!(maxBlockSize, ALLOCSET_DEFAULT_INITSIZE);

    (*aggstate).hash_tablecxt = BumpContextCreate(
        (*(*aggstate).ss.ps.state).es_query_cxt,
        b"HashAgg table context\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        maxBlockSize,
    );
}

// --------------------------------------------------------------------------
// hash_choose_num_buckets / hash_choose_num_partitions
// --------------------------------------------------------------------------

/*
 * Choose a reasonable number of buckets for the initial hash table size.
 */
unsafe fn hash_choose_num_buckets(
    hashentrysize: f64,
    ngroups: i64,
    memory: Size,
) -> i64 {
    let mut max_nbuckets: i64 = (memory as f64 / hashentrysize) as i64;
    let mut nbuckets: i64 = ngroups;

    /*
     * Underestimating is better than overestimating.
     */
    max_nbuckets >>= 1;

    if nbuckets > max_nbuckets {
        nbuckets = max_nbuckets;
    }

    Max!(nbuckets, 1)
}

/*
 * Determine the number of partitions to create when spilling.
 */
unsafe fn hash_choose_num_partitions(
    input_groups: f64,
    hashentrysize: f64,
    used_bits: c_int,
    log2_npartitions: *mut c_int,
) -> c_int {
    let hash_mem_limit: Size = get_hash_memory_limit();
    let partition_limit: f64 =
        (hash_mem_limit as f64 * 0.25 - HASHAGG_READ_BUFFER_SIZE as f64)
            / HASHAGG_WRITE_BUFFER_SIZE as f64;

    let mem_wanted: f64 = HASHAGG_PARTITION_FACTOR * input_groups * hashentrysize;

    /* make enough partitions so that each one is likely to fit in memory */
    let mut dpartitions: f64 = 1.0 + (mem_wanted / hash_mem_limit as f64);

    if dpartitions > partition_limit {
        dpartitions = partition_limit;
    }
    if dpartitions < HASHAGG_MIN_PARTITIONS as f64 {
        dpartitions = HASHAGG_MIN_PARTITIONS as f64;
    }
    if dpartitions > HASHAGG_MAX_PARTITIONS as f64 {
        dpartitions = HASHAGG_MAX_PARTITIONS as f64;
    }

    /* HASHAGG_MAX_PARTITIONS limit makes this safe */
    let npartitions: c_int = dpartitions as c_int;

    /* ceil(log2(npartitions)) */
    let mut partition_bits: c_int = my_log2(npartitions as c_long);

    /* make sure that we don't exhaust the hash bits */
    if partition_bits + used_bits >= 32 {
        partition_bits = 32 - used_bits;
    }

    if !log2_npartitions.is_null() {
        *log2_npartitions = partition_bits;
    }

    /* number of partitions will be a power of two */
    1 << partition_bits
}

// --------------------------------------------------------------------------
// initialize_hash_entry / lookup_hash_entries
// --------------------------------------------------------------------------

/*
 * Initialize a freshly-created TupleHashEntry.
 */
unsafe fn initialize_hash_entry(
    aggstate: *mut AggState,
    hashtable: TupleHashTable,
    entry: TupleHashEntry,
) {
    (*aggstate).hash_ngroups_current += 1;
    hash_agg_check_limits(aggstate);

    /* no need to allocate or initialize per-group state */
    if (*aggstate).numtrans == 0 {
        return;
    }

    let pergroup =
        TupleHashEntryGetAdditional(hashtable, entry) as *mut PergroupFull;

    /*
     * Initialize aggregates for new tuple group.
     */
    let transstates = (*aggstate).pertrans as *mut PertransFull;
    for transno in 0..(*aggstate).numtrans {
        let pertrans = transstates.add(transno as usize);
        let pergroupstate = pergroup.add(transno as usize);
        initialize_aggregate(aggstate, pertrans, pergroupstate);
    }
}

/*
 * Look up hash entries for the current tuple in all hashed grouping sets.
 */
unsafe fn lookup_hash_entries(aggstate: *mut AggState) {
    let pergroup = (*aggstate).hash_pergroup;
    let outerslot = (*(*aggstate).tmpcontext).ecxt_outertuple;
    let perhash_arr = (*aggstate).perhash as *mut PerhashFull;

    for setno in 0..(*aggstate).num_hashes {
        let perhash = perhash_arr.add(setno as usize);
        let hashtable: TupleHashTable = (*perhash).hashtable;
        let hashslot: *mut TupleTableSlot = (*perhash).hashslot;
        let mut isnew: bool = false;
        let p_isnew: *mut bool = if (*aggstate).hash_spill_mode {
            std::ptr::null_mut()
        } else {
            &mut isnew
        };

        select_current_set(aggstate, setno, true);
        prepare_hash_slot(perhash, outerslot, hashslot);

        let mut hash: uint32 = 0;
        let entry = LookupTupleHashEntry(hashtable, hashslot, p_isnew, &mut hash);

        if !entry.is_null() {
            if isnew {
                initialize_hash_entry(aggstate, hashtable, entry);
            }
            *pergroup.add(setno as usize) =
                TupleHashEntryGetAdditional(hashtable, entry) as AggStatePerGroup;
        } else {
            let spill = (*aggstate).hash_spills.add(setno as usize);
            let slot = (*(*aggstate).tmpcontext).ecxt_outertuple;

            if (*spill).partitions.is_null() {
                hashagg_spill_init(
                    spill,
                    (*aggstate).hash_tapeset,
                    0,
                    (*(*perhash).aggnode).numGroups as f64,
                    (*aggstate).hashentrysize,
                );
            }

            hashagg_spill_tuple(aggstate, spill, slot, hash);
            *pergroup.add(setno as usize) = std::ptr::null_mut();
        }
    }
}

// --------------------------------------------------------------------------
// ExecAgg / agg_retrieve_direct
// --------------------------------------------------------------------------

/*
 * ExecAgg -
 *
 *    ExecAgg receives tuples from its outer subplan and aggregates over
 *    the appropriate attribute for each aggregate function use (Aggref
 *    node) appearing in the targetlist or qual of the node.
 */
pub unsafe fn ExecAgg(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node = pstate as *mut AggState;
    let mut result: *mut TupleTableSlot = std::ptr::null_mut();

    CHECK_FOR_INTERRUPTS();

    if !(*node).agg_done {
        let phase = (*node).phase as *mut PerphaseFull;
        /* Dispatch based on strategy */
        match (*phase).aggstrategy {
            AGG_HASHED => {
                if !(*node).table_filled {
                    agg_fill_hash_table(node);
                }
                result = agg_retrieve_hash_table(node);
            }
            AGG_MIXED => {
                result = agg_retrieve_hash_table(node);
            }
            AGG_PLAIN | AGG_SORTED => {
                result = agg_retrieve_direct(node);
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }

        if !TupIsNull(result) {
            return result;
        }
    }

    std::ptr::null_mut()
}

/*
 * ExecAgg for non-hashed case
 */
unsafe fn agg_retrieve_direct(aggstate: *mut AggState) -> *mut TupleTableSlot {
    let phase = (*aggstate).phase as *mut PerphaseFull;
    let mut node = (*phase).aggnode;
    let econtext: *mut ExprContext;
    let tmpcontext: *mut ExprContext;
    let peragg: *mut PeraggFull;
    let pergroups: *mut AggStatePerGroup;
    let mut outerslot: *mut TupleTableSlot;
    let firstSlot: *mut TupleTableSlot;
    let mut result: *mut TupleTableSlot;
    let hasGroupingSets: bool = (*phase).numsets > 0;
    let mut numGroupingSets: c_int = Max!((*phase).numsets, 1);
    let mut currentSet: c_int;
    let mut nextSetSize: c_int;
    let mut numReset: c_int;

    /*
     * get state info from node
     *
     * econtext is the per-output-tuple expression context
     * tmpcontext is the per-input-tuple expression context
     */
    econtext = (*aggstate).ss.ps.ps_ExprContext;
    tmpcontext = (*aggstate).tmpcontext;

    peragg = (*aggstate).peragg as *mut PeraggFull;
    pergroups = (*aggstate).pergroups;
    firstSlot = (*aggstate).ss.ss_ScanTupleSlot;

    /*
     * We loop retrieving groups until we find one matching
     * aggstate->ss.ps.qual
     */
    'outer: while !(*aggstate).agg_done {
        /*
         * Clear the per-output-tuple context for each group, as well as
         * aggcontext.
         */
        ReScanExprContext(econtext);

        /*
         * Determine how many grouping sets need to be reset at this boundary.
         */
        if (*aggstate).projected_set >= 0
            && (*aggstate).projected_set < numGroupingSets
        {
            numReset = (*aggstate).projected_set + 1;
        } else {
            numReset = numGroupingSets;
        }

        for i in 0..numReset {
            ReScanExprContext(*(*aggstate).aggcontexts.add(i as usize));
        }

        /*
         * Check if input is complete and there are no more groups to project
         * in this phase; move to next phase or mark as done.
         */
        if (*aggstate).input_done
            && (*aggstate).projected_set >= (numGroupingSets - 1)
        {
            if (*aggstate).current_phase < (*aggstate).numphases - 1 {
                initialize_phase(aggstate, (*aggstate).current_phase + 1);
                (*aggstate).input_done = false;
                (*aggstate).projected_set = -1;
                numGroupingSets = Max!((*((*aggstate).phase as *mut PerphaseFull)).numsets, 1);
                node = (*((*aggstate).phase as *mut PerphaseFull)).aggnode;
                numReset = numGroupingSets;
            } else if (*aggstate).aggstrategy == AGG_MIXED {
                /*
                 * Mixed mode; we've output all the grouped stuff and have
                 * full hashtables, so switch to outputting those.
                 */
                initialize_phase(aggstate, 0);
                (*aggstate).table_filled = true;
                let ph0 = ((*aggstate).perhash as *mut PerhashFull).add(0);
                ResetTupleHashIterator((*ph0).hashtable, &mut (*ph0).hashiter);
                select_current_set(aggstate, 0, true);
                return agg_retrieve_hash_table(aggstate);
            } else {
                (*aggstate).agg_done = true;
                break 'outer;
            }
        }

        /*
         * Get the number of columns in the next grouping set after the last
         * projected one (if any).
         */
        if (*aggstate).projected_set >= 0
            && (*aggstate).projected_set < (numGroupingSets - 1)
        {
            let phase = (*aggstate).phase as *mut PerphaseFull;
            nextSetSize = *(*phase).gset_lengths.add(
                ((*aggstate).projected_set + 1) as usize,
            );
        } else {
            nextSetSize = 0;
        }

        /*----------
         * If a subgroup for the current grouping set is present, project it.
         *----------
         */
        (*tmpcontext).ecxt_innertuple = (*econtext).ecxt_outertuple;
        let phase = (*aggstate).phase as *mut PerphaseFull;
        if (*aggstate).input_done
            || ((*node).aggstrategy != AGG_PLAIN
                && (*aggstate).projected_set != -1
                && (*aggstate).projected_set < (numGroupingSets - 1)
                && nextSetSize > 0
                && !ExecQualAndReset(
                    *(*phase).eqfunctions.add((nextSetSize - 1) as usize),
                    tmpcontext,
                ))
        {
            (*aggstate).projected_set += 1;

            Assert!((*aggstate).projected_set < numGroupingSets);
            Assert!(nextSetSize > 0 || (*aggstate).input_done);
        } else {
            /*
             * We no longer care what group we just projected, the next
             * projection will always be the first grouping set.
             */
            (*aggstate).projected_set = 0;

            /*
             * If we don't already have the first tuple of the new group,
             * fetch it from the outer plan.
             */
            if (*aggstate).grp_firstTuple.is_null() {
                outerslot = fetch_input_tuple(aggstate);
                if !TupIsNull(outerslot) {
                    /*
                     * Make a copy of the first input tuple.
                     */
                    (*aggstate).grp_firstTuple = ExecCopySlotHeapTuple(outerslot);
                } else {
                    /* outer plan produced no tuples at all */
                    if hasGroupingSets {
                        (*aggstate).input_done = true;

                        let phase = (*aggstate).phase as *mut PerphaseFull;
                        while *(*phase).gset_lengths.add((*aggstate).projected_set as usize) > 0
                        {
                            (*aggstate).projected_set += 1;
                            if (*aggstate).projected_set >= numGroupingSets {
                                break;
                            }
                        }

                        if (*aggstate).projected_set >= numGroupingSets {
                            continue;
                        }
                    } else {
                        (*aggstate).agg_done = true;
                        /* If we are grouping, we should produce no tuples too */
                        if (*node).aggstrategy != AGG_PLAIN {
                            return std::ptr::null_mut();
                        }
                    }
                }
            }

            /*
             * Initialize working state for a new input tuple group.
             */
            initialize_aggregates(aggstate, pergroups, numReset);

            if !(*aggstate).grp_firstTuple.is_null() {
                /*
                 * Store the copied first input tuple in the tuple table slot.
                 */
                ExecForceStoreHeapTuple((*aggstate).grp_firstTuple, firstSlot, true);
                (*aggstate).grp_firstTuple = std::ptr::null_mut(); /* don't keep two pointers */

                /* set up for first advance_aggregates call */
                (*tmpcontext).ecxt_outertuple = firstSlot;

                /*
                 * Process each outer-plan tuple, and then fetch the next one,
                 * until we exhaust the outer plan or cross a group boundary.
                 */
                'inner: loop {
                    /*
                     * During phase 1 only of a mixed agg, we need to update
                     * hashtables as well in advance_aggregates.
                     */
                    if (*aggstate).aggstrategy == AGG_MIXED
                        && (*aggstate).current_phase == 1
                    {
                        lookup_hash_entries(aggstate);
                    }

                    /* Advance the aggregates (or combine functions) */
                    advance_aggregates(aggstate);

                    /* Reset per-input-tuple context after each tuple */
                    ResetExprContext(tmpcontext);

                    outerslot = fetch_input_tuple(aggstate);
                    if TupIsNull(outerslot) {
                        /* no more outer-plan tuples available */

                        /* if we built hash tables, finalize any spills */
                        if (*aggstate).aggstrategy == AGG_MIXED
                            && (*aggstate).current_phase == 1
                        {
                            hashagg_finish_initial_spills(aggstate);
                        }

                        if hasGroupingSets {
                            (*aggstate).input_done = true;
                            break 'inner;
                        } else {
                            (*aggstate).agg_done = true;
                            break 'inner;
                        }
                    }
                    /* set up for next advance_aggregates call */
                    (*tmpcontext).ecxt_outertuple = outerslot;

                    /*
                     * If we are grouping, check whether we've crossed a group
                     * boundary.
                     */
                    if (*node).aggstrategy != AGG_PLAIN && (*node).numCols > 0 {
                        let phase = (*aggstate).phase as *mut PerphaseFull;
                        (*tmpcontext).ecxt_innertuple = firstSlot;
                        if !ExecQual(
                            *(*phase).eqfunctions.add(((*node).numCols - 1) as usize),
                            tmpcontext,
                        ) {
                            (*aggstate).grp_firstTuple =
                                ExecCopySlotHeapTuple(outerslot);
                            break 'inner;
                        }
                    }
                }
            }

            /*
             * Use the representative input tuple for any references to
             * non-aggregated input columns.
             */
            (*econtext).ecxt_outertuple = firstSlot;
        }

        Assert!((*aggstate).projected_set >= 0);

        currentSet = (*aggstate).projected_set;

        prepare_projection_slot(aggstate, (*econtext).ecxt_outertuple, currentSet);

        select_current_set(aggstate, currentSet, false);

        finalize_aggregates(
            aggstate,
            peragg,
            (*pergroups.add(currentSet as usize)) as *mut PergroupFull,
        );

        /*
         * If there's no row to project right now, we must continue rather
         * than returning a null since there might be more groups.
         */
        result = project_aggregates(aggstate);
        if !result.is_null() {
            return result;
        }
    }

    /* No more groups */
    std::ptr::null_mut()
}

// --------------------------------------------------------------------------
// agg_fill_hash_table / agg_refill_hash_table
// --------------------------------------------------------------------------

/*
 * ExecAgg for hashed case: read input and build hash table
 */
unsafe fn agg_fill_hash_table(aggstate: *mut AggState) {
    let tmpcontext = (*aggstate).tmpcontext;

    /*
     * Process each outer-plan tuple, and then fetch the next one, until we
     * exhaust the outer plan.
     */
    loop {
        let outerslot = fetch_input_tuple(aggstate);
        if TupIsNull(outerslot) {
            break;
        }

        /* set up for lookup_hash_entries and advance_aggregates */
        (*tmpcontext).ecxt_outertuple = outerslot;

        /* Find or build hashtable entries */
        lookup_hash_entries(aggstate);

        /* Advance the aggregates (or combine functions) */
        advance_aggregates(aggstate);

        /*
         * Reset per-input-tuple context after each tuple, but note that the
         * hash lookups do this too.
         */
        ResetExprContext((*aggstate).tmpcontext);
    }

    /* finalize spills, if any */
    hashagg_finish_initial_spills(aggstate);

    (*aggstate).table_filled = true;
    /* Initialize to walk the first hash table */
    select_current_set(aggstate, 0, true);
    let ph0 = ((*aggstate).perhash as *mut PerhashFull).add(0);
    ResetTupleHashIterator((*ph0).hashtable, &mut (*ph0).hashiter);
}

/*
 * If any data was spilled during hash aggregation, reset the hash table and
 * reprocess one batch of spilled data.
 */
unsafe fn agg_refill_hash_table(aggstate: *mut AggState) -> bool {
    if (*aggstate).hash_batches.is_null() || (*aggstate).hash_batches as usize == NIL as usize {
        return false;
    }

    /* hash_batches is a stack, with the top item at the end of the list */
    let batch = llast((*aggstate).hash_batches) as *mut HashAggBatch;
    (*aggstate).hash_batches = list_delete_last((*aggstate).hash_batches);

    hash_agg_set_limits(
        (*aggstate).hashentrysize,
        (*batch).input_card,
        (*batch).used_bits,
        &mut (*aggstate).hash_mem_limit,
        &mut (*aggstate).hash_ngroups_limit,
        std::ptr::null_mut(),
    );

    /*
     * Each batch only processes one grouping set; set the rest to NULL so
     * that advance_aggregates() knows to ignore them.
     */
    MemSet!(
        (*aggstate).hash_pergroup,
        0,
        core::mem::size_of::<AggStatePerGroup>() * (*aggstate).num_hashes as usize
    );

    /* free memory and reset hash tables */
    ReScanExprContext((*aggstate).hashcontext);
    MemoryContextReset((*aggstate).hash_tablecxt);
    let perhash_arr = (*aggstate).perhash as *mut PerhashFull;
    for setno in 0..(*aggstate).num_hashes {
        ResetTupleHashTable((*perhash_arr.add(setno as usize)).hashtable);
    }

    (*aggstate).hash_ngroups_current = 0;

    /*
     * In AGG_MIXED mode, hash aggregation happens in phase 1 and the output
     * happens in phase 0.
     */
    Assert!((*aggstate).current_phase == 0);
    let phase = (*aggstate).phase as *mut PerphaseFull;
    if (*phase).aggstrategy == AGG_MIXED {
        (*aggstate).current_phase = 1;
        let phase_arr = (*aggstate).phases as *mut PerphaseFull;
        (*aggstate).phase = phase_arr.add((*aggstate).current_phase as usize) as AggStatePerPhase;
    }

    select_current_set(aggstate, (*batch).setno, true);

    let perhash = perhash_arr.add((*aggstate).current_set as usize);

    /*
     * Spilled tuples are always read back as MinimalTuples, so recompile
     * the aggregate expressions.
     */
    hashagg_recompile_expressions(aggstate, true, true);

    let tapeset = (*aggstate).hash_tapeset;
    let mut spill: HashAggSpill = core::mem::zeroed();
    let mut spill_initialized: bool = false;

    INJECTION_POINT!("hash-aggregate-process-batch", std::ptr::null_mut::<c_void>());
    loop {
        let spillslot = (*aggstate).hash_spill_rslot;
        let hashslot: *mut TupleTableSlot = (*perhash).hashslot;
        let hashtable: TupleHashTable = (*perhash).hashtable;
        let mut isnew: bool = false;
        let p_isnew: *mut bool = if (*aggstate).hash_spill_mode {
            std::ptr::null_mut()
        } else {
            &mut isnew
        };

        CHECK_FOR_INTERRUPTS();

        let tuple = hashagg_batch_read(batch, std::ptr::null_mut());
        if tuple.is_null() {
            break;
        }

        let mut hash: uint32 = 0;
        let tuple2 = hashagg_batch_read(batch, &mut hash);
        // NOTE: C reads hash first then tuple; we do a simplified version here
        // TODO(pg-port): fix hashagg_batch_read to match C (reads hash then tuple data)
        ExecStoreMinimalTuple(tuple, spillslot, true);
        (*(*aggstate).tmpcontext).ecxt_outertuple = spillslot;

        prepare_hash_slot(perhash, (*(*aggstate).tmpcontext).ecxt_outertuple, hashslot);
        let entry = LookupTupleHashEntryHash(hashtable, hashslot, p_isnew, hash);

        if !entry.is_null() {
            if isnew {
                initialize_hash_entry(aggstate, hashtable, entry);
            }
            *(*aggstate).hash_pergroup.add((*batch).setno as usize) =
                TupleHashEntryGetAdditional(hashtable, entry) as AggStatePerGroup;
            advance_aggregates(aggstate);
        } else {
            if !spill_initialized {
                /*
                 * Avoid initializing the spill until we actually need it so
                 * that we don't assign tapes that will never be used.
                 */
                spill_initialized = true;
                hashagg_spill_init(
                    &mut spill,
                    tapeset,
                    (*batch).used_bits,
                    (*batch).input_card,
                    (*aggstate).hashentrysize,
                );
            }
            /* no memory for a new group, spill */
            hashagg_spill_tuple(aggstate, &mut spill, spillslot, hash);
            *(*aggstate).hash_pergroup.add((*batch).setno as usize) = std::ptr::null_mut();
        }

        /*
         * Reset per-input-tuple context after each tuple.
         */
        ResetExprContext((*aggstate).tmpcontext);
    }

    LogicalTapeClose((*batch).input_tape);

    /* change back to phase 0 */
    (*aggstate).current_phase = 0;
    let phase_arr = (*aggstate).phases as *mut PerphaseFull;
    (*aggstate).phase = phase_arr.add(0) as AggStatePerPhase;

    if spill_initialized {
        hashagg_spill_finish(aggstate, &mut spill, (*batch).setno);
        hash_agg_update_metrics(aggstate, true, spill.npartitions);
    } else {
        hash_agg_update_metrics(aggstate, true, 0);
    }

    (*aggstate).hash_spill_mode = false;

    /* prepare to walk the first hash table */
    select_current_set(aggstate, (*batch).setno, true);
    let ph_batch = perhash_arr.add((*batch).setno as usize);
    ResetTupleHashIterator((*ph_batch).hashtable, &mut (*ph_batch).hashiter);

    pfree(batch as *mut c_void);

    true
}

// --------------------------------------------------------------------------
// agg_retrieve_hash_table / agg_retrieve_hash_table_in_memory
// --------------------------------------------------------------------------

/*
 * ExecAgg for hashed case: retrieving groups from hash table
 */
unsafe fn agg_retrieve_hash_table(aggstate: *mut AggState) -> *mut TupleTableSlot {
    let mut result: *mut TupleTableSlot = std::ptr::null_mut();

    while result.is_null() {
        result = agg_retrieve_hash_table_in_memory(aggstate);
        if result.is_null() {
            if !agg_refill_hash_table(aggstate) {
                (*aggstate).agg_done = true;
                break;
            }
        }
    }

    result
}

/*
 * Retrieve the groups from the in-memory hash tables without considering any
 * spilled tuples.
 */
unsafe fn agg_retrieve_hash_table_in_memory(
    aggstate: *mut AggState,
) -> *mut TupleTableSlot {
    let econtext = (*aggstate).ss.ps.ps_ExprContext;
    let peragg = (*aggstate).peragg as *mut PeraggFull;
    let firstSlot = (*aggstate).ss.ss_ScanTupleSlot;
    let perhash_arr = (*aggstate).perhash as *mut PerhashFull;

    /*
     * Note that perhash (and therefore anything accessed through it) can
     * change inside the loop, as we change between grouping sets.
     */
    let mut perhash = perhash_arr.add((*aggstate).current_set as usize);

    /*
     * We loop retrieving groups until we find one satisfying
     * aggstate->ss.ps.qual
     */
    loop {
        let hashslot: *mut TupleTableSlot = (*perhash).hashslot;
        let hashtable: TupleHashTable = (*perhash).hashtable;

        CHECK_FOR_INTERRUPTS();

        /*
         * Find the next entry in the hash table
         */
        let entry = ScanTupleHashTable(hashtable, &mut (*perhash).hashiter);
        if entry.is_null() {
            let nextset = (*aggstate).current_set + 1;

            if nextset < (*aggstate).num_hashes {
                /*
                 * Switch to next grouping set, reinitialize, and restart the
                 * loop.
                 */
                select_current_set(aggstate, nextset, true);
                perhash = perhash_arr.add((*aggstate).current_set as usize);
                ResetTupleHashIterator((*perhash).hashtable, &mut (*perhash).hashiter);
                continue;
            } else {
                return std::ptr::null_mut();
            }
        }

        /*
         * Clear the per-output-tuple context for each group
         */
        ResetExprContext(econtext);

        /*
         * Transform representative tuple back into one with the right
         * columns.
         */
        ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry), hashslot, false);
        slot_getallattrs(hashslot);

        ExecClearTuple(firstSlot);
        core::ptr::write_bytes(
            (*firstSlot).tts_isnull,
            true as u8,
            (*(*firstSlot).tts_tupleDescriptor).natts as usize,
        );

        for i in 0..(*perhash).numhashGrpCols {
            let varNumber =
                (*(*perhash).hashGrpColIdxInput.add(i as usize) as c_int - 1) as usize;
            (*firstSlot).tts_values.add(varNumber).write(
                (*hashslot).tts_values.add(i as usize).read(),
            );
            (*firstSlot).tts_isnull.add(varNumber).write(
                (*hashslot).tts_isnull.add(i as usize).read(),
            );
        }
        ExecStoreVirtualTuple(firstSlot);

        let pergroup =
            TupleHashEntryGetAdditional(hashtable, entry) as *mut PergroupFull;

        /*
         * Use the representative input tuple for any references to
         * non-aggregated input columns.
         */
        (*econtext).ecxt_outertuple = firstSlot;

        prepare_projection_slot(
            aggstate,
            (*econtext).ecxt_outertuple,
            (*aggstate).current_set,
        );

        finalize_aggregates(aggstate, peragg, pergroup);

        let result = project_aggregates(aggstate);
        if !result.is_null() {
            return result;
        }
    }
}

// --------------------------------------------------------------------------
// hashagg_spill_init / hashagg_spill_tuple / hashagg_batch_new /
// hashagg_batch_read / hashagg_finish_initial_spills / hashagg_spill_finish /
// hashagg_reset_spill_state
// --------------------------------------------------------------------------

/*
 * hashagg_spill_init
 *
 * Called after we determined that spilling is necessary. Chooses the number
 * of partitions to create, and initializes them.
 */
unsafe fn hashagg_spill_init(
    spill: *mut HashAggSpill,
    tapeset: *mut LogicalTapeSet, /* execnodes opaque; cast to logtape internally */
    used_bits: c_int,
    input_groups: f64,
    hashentrysize: f64,
) {
    let mut partition_bits: c_int = 0;
    let mut npartitions = hash_choose_num_partitions(
        input_groups,
        hashentrysize,
        used_bits,
        &mut partition_bits,
    );

    // USE_INJECTION_POINTS
    // if IS_INJECTION_POINT_ATTACHED("hash-aggregate-single-partition") { npartitions = 1; partition_bits = 0; }

    (*spill).partitions = palloc0(
        core::mem::size_of::<*mut LogicalTape>() * npartitions as usize,
    );
    (*spill).ntuples =
        palloc0(core::mem::size_of::<int64>() * npartitions as usize) as *mut int64;
    (*spill).hll_card =
        palloc0(core::mem::size_of::<hyperLogLogState>() * npartitions as usize);

    let parts = (*spill).partitions as *mut *mut LogicalTape;
    for i in 0..npartitions {
        *parts.add(i as usize) = LogicalTapeCreate(tapeset as *mut crate::utils::sort::logtape::LogicalTapeSet);
    }

    (*spill).shift = 32 - used_bits - partition_bits;
    if (*spill).shift < 32 {
        (*spill).mask = ((npartitions - 1) as uint32) << (*spill).shift;
    } else {
        (*spill).mask = 0;
    }
    (*spill).npartitions = npartitions;

    let hlls = (*spill).hll_card as *mut hyperLogLogState;
    for i in 0..npartitions {
        initHyperLogLog(hlls.add(i as usize), HASHAGG_HLL_BIT_WIDTH);
    }
}

/*
 * hashagg_spill_tuple
 *
 * No room for new groups in the hash table. Save for later in the appropriate
 * partition.
 */
unsafe fn hashagg_spill_tuple(
    aggstate: *mut AggState,
    spill: *mut HashAggSpill,
    inputslot: *mut TupleTableSlot,
    hash: uint32,
) -> Size {
    let mut spillslot: *mut TupleTableSlot;
    let partition: usize;
    let mut shouldFree: bool = false;
    let mut total_written: c_int = 0;

    Assert!(!(*spill).partitions.is_null());

    /* spill only attributes that we actually need */
    if !(*aggstate).all_cols_needed {
        spillslot = (*aggstate).hash_spill_wslot;
        slot_getsomeattrs(inputslot, (*aggstate).max_colno_needed);
        ExecClearTuple(spillslot);
        for i in 0..(*(*spillslot).tts_tupleDescriptor).natts as usize {
            if bms_is_member((i + 1) as c_int, (*aggstate).colnos_needed) {
                (*spillslot).tts_values.add(i).write((*inputslot).tts_values.add(i).read());
                (*spillslot).tts_isnull.add(i).write((*inputslot).tts_isnull.add(i).read());
            } else {
                (*spillslot).tts_isnull.add(i).write(true);
            }
        }
        ExecStoreVirtualTuple(spillslot);
    } else {
        spillslot = inputslot;
    }

    let tuple = ExecFetchSlotMinimalTuple(spillslot, &mut shouldFree);

    if (*spill).shift < 32 {
        partition = ((hash & (*spill).mask) >> (*spill).shift) as usize;
    } else {
        partition = 0;
    }

    *(*spill).ntuples.add(partition) += 1;

    /*
     * All hash values destined for a given partition have some bits in
     * common, which causes bad HLL cardinality estimates. Hash the hash to
     * get a more uniform distribution.
     */
    addHyperLogLog(
        ((*spill).hll_card as *mut hyperLogLogState).add(partition),
        hash_bytes_uint32(hash),
    );

    let tape = *((*spill).partitions as *mut *mut LogicalTape).add(partition);

    LogicalTapeWrite(tape, &hash as *const uint32 as *const c_void, core::mem::size_of::<uint32>());
    total_written += core::mem::size_of::<uint32>() as c_int;

    LogicalTapeWrite(tape, tuple as *const c_void, (*tuple).t_len as usize);
    total_written += (*tuple).t_len as c_int;

    if shouldFree {
        pfree(tuple as *mut c_void);
    }

    total_written as Size
}

/*
 * hashagg_batch_new
 *
 * Construct a HashAggBatch item, which represents one iteration of HashAgg to
 * be done.
 */
unsafe fn hashagg_batch_new(
    input_tape: *mut LogicalTape,
    setno: c_int,
    input_tuples: int64,
    input_card: f64,
    used_bits: c_int,
) -> *mut HashAggBatch {
    let batch = palloc0(core::mem::size_of::<HashAggBatch>()) as *mut HashAggBatch;
    (*batch).setno = setno;
    (*batch).used_bits = used_bits;
    (*batch).input_tape = input_tape;
    (*batch).input_tuples = input_tuples;
    (*batch).input_card = input_card;
    batch
}

/*
 * hashagg_batch_read
 *      read the next tuple from a batch's tape.  Return NULL if no more.
 */
unsafe fn hashagg_batch_read(batch: *mut HashAggBatch, hashp: *mut uint32) -> MinimalTuple {
    let tape = (*batch).input_tape;
    let mut t_len: uint32 = 0;
    let mut hash: uint32 = 0;

    let nread = LogicalTapeRead(
        tape,
        &mut hash as *mut uint32 as *mut c_void,
        core::mem::size_of::<uint32>(),
    ) as usize;
    if nread == 0 {
        return std::ptr::null_mut();
    }
    if nread != core::mem::size_of::<uint32>() {
        ereport!(
            ERROR,
            errmsg!("unexpected EOF for tape reading hash value")
        );
    }
    if !hashp.is_null() {
        *hashp = hash;
    }

    let nread = LogicalTapeRead(
        tape,
        &mut t_len as *mut uint32 as *mut c_void,
        core::mem::size_of::<uint32>(),
    ) as usize;
    if nread != core::mem::size_of::<uint32>() {
        ereport!(
            ERROR,
            errmsg!("unexpected EOF for tape reading tuple length")
        );
    }

    let tuple = palloc(t_len as Size) as MinimalTuple;
    (*tuple).t_len = t_len;

    let nread = LogicalTapeRead(
        tape,
        (tuple as *mut u8).add(core::mem::size_of::<uint32>()) as *mut c_void,
        (t_len - core::mem::size_of::<uint32>() as uint32) as usize,
    ) as usize;
    if nread != (t_len - core::mem::size_of::<uint32>() as uint32) as usize {
        ereport!(
            ERROR,
            errmsg!("unexpected EOF for tape reading tuple data")
        );
    }

    tuple
}

/*
 * hashagg_finish_initial_spills
 *
 * After a HashAggBatch has been processed, it may have spilled tuples to
 * disk. If so, turn the spilled partitions into new batches.
 */
unsafe fn hashagg_finish_initial_spills(aggstate: *mut AggState) {
    let mut total_npartitions: c_int = 0;

    if !(*aggstate).hash_spills.is_null() {
        for setno in 0..(*aggstate).num_hashes {
            let spill = (*aggstate).hash_spills.add(setno as usize);
            total_npartitions += (*spill).npartitions;
            hashagg_spill_finish(aggstate, spill, setno);
        }

        /*
         * We're not processing tuples from outer plan any more; only
         * processing batches of spilled tuples.
         */
        pfree((*aggstate).hash_spills as *mut c_void);
        (*aggstate).hash_spills = std::ptr::null_mut();
    }

    hash_agg_update_metrics(aggstate, false, total_npartitions);
    (*aggstate).hash_spill_mode = false;
}

/*
 * hashagg_spill_finish
 *
 * Transform spill partitions into new batches.
 */
unsafe fn hashagg_spill_finish(
    aggstate: *mut AggState,
    spill: *mut HashAggSpill,
    setno: c_int,
) {
    let used_bits: c_int = 32 - (*spill).shift;

    if (*spill).npartitions == 0 {
        return; /* didn't spill */
    }

    let parts = (*spill).partitions as *mut *mut LogicalTape;
    let hlls = (*spill).hll_card as *mut hyperLogLogState;
    for i in 0..(*spill).npartitions {
        let tape: *mut LogicalTape = *parts.add(i as usize);

        /* if the partition is empty, don't create a new batch of work */
        if *(*spill).ntuples.add(i as usize) == 0 {
            continue;
        }

        let cardinality = estimateHyperLogLog(hlls.add(i as usize));
        freeHyperLogLog(hlls.add(i as usize));

        /* rewinding frees the buffer while not in use */
        LogicalTapeRewindForRead(tape, HASHAGG_READ_BUFFER_SIZE);

        let new_batch = hashagg_batch_new(
            tape,
            setno,
            *(*spill).ntuples.add(i as usize),
            cardinality,
            used_bits,
        );
        (*aggstate).hash_batches = lappend(
            (*aggstate).hash_batches,
            new_batch as *mut c_void,
        );
        (*aggstate).hash_batches_used += 1;
    }

    pfree((*spill).ntuples as *mut c_void);
    pfree((*spill).hll_card as *mut c_void);
    pfree((*spill).partitions as *mut c_void);
}

/*
 * Free resources related to a spilled HashAgg.
 */
unsafe fn hashagg_reset_spill_state(aggstate: *mut AggState) {
    /* free spills from initial pass */
    if !(*aggstate).hash_spills.is_null() {
        for setno in 0..(*aggstate).num_hashes {
            let spill = (*aggstate).hash_spills.add(setno as usize);
            pfree((*spill).ntuples as *mut c_void);
            pfree((*spill).partitions as *mut c_void);
        }
        pfree((*aggstate).hash_spills as *mut c_void);
        (*aggstate).hash_spills = std::ptr::null_mut();
    }

    /* free batches */
    list_free_deep((*aggstate).hash_batches);
    (*aggstate).hash_batches = NIL as *mut List;

    /* close tape set */
    if !(*aggstate).hash_tapeset.is_null() {
        LogicalTapeSetClose((*aggstate).hash_tapeset as *mut crate::utils::sort::logtape::LogicalTapeSet);
        (*aggstate).hash_tapeset = std::ptr::null_mut();
    }
}

// --------------------------------------------------------------------------
// GetAggInitVal
// --------------------------------------------------------------------------

/*
 * GetAggInitVal -- convert pg_aggregate.agginitval to the proper datum.
 */
unsafe fn GetAggInitVal(textInitVal: Datum, transtype: Oid) -> Datum {
    let mut typinput: Oid = InvalidOid;
    let mut typioparam: Oid = InvalidOid;
    getTypeInputInfo(transtype, &mut typinput, &mut typioparam);
    let strInitVal = TextDatumGetCString(textInitVal);
    let initVal = OidInputFunctionCall(typinput, strInitVal, typioparam, -1);
    pfree(strInitVal as *mut c_void);
    initVal
}

// --------------------------------------------------------------------------
// ExecInitAgg
// --------------------------------------------------------------------------

/*
 * ExecInitAgg
 *
 * Creates the run-time information for the agg node produced by the
 * planner and initializes its outer subtree.
 */
pub unsafe fn ExecInitAgg(node: *mut Agg, estate: *mut EState, eflags: c_int) -> *mut AggState {
    let aggstate: *mut AggState;
    let mut peraggs: *mut PeraggFull;
    let mut pertransstates: *mut PertransFull;
    let mut pergroups: *mut AggStatePerGroup;
    let outerPlan: *mut Plan;
    let econtext: *mut ExprContext;
    let scanDesc: crate::access::common::tupdesc::TupleDesc; // TupleDesc
    let mut max_aggno: c_int = -1;
    let mut max_transno: c_int = -1;
    let mut numaggrefs: c_int;
    let mut numaggs: c_int;
    let mut numtrans: c_int;
    let mut numGroupingSets: c_int = 1;
    let mut numPhases: c_int;
    let mut numHashes: c_int;
    let mut all_grouped_cols: *mut Bitmapset = std::ptr::null_mut();
    let use_hashing: bool =
        (*node).aggstrategy == AGG_HASHED || (*node).aggstrategy == AGG_MIXED;

    /* check for unsupported flags */
    Assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    /*
     * create state structure
     */
    aggstate = makeNode_AggState();
    (*aggstate).ss.ps.plan = &raw mut (*node).plan;
    (*aggstate).ss.ps.state = estate;
    (*aggstate).ss.ps.ExecProcNode = Some(ExecAgg);

    (*aggstate).aggs = NIL as *mut List;
    (*aggstate).numaggs = 0;
    (*aggstate).numtrans = 0;
    (*aggstate).aggstrategy = (*node).aggstrategy;
    (*aggstate).aggsplit = (*node).aggsplit;
    (*aggstate).maxsets = 0;
    (*aggstate).projected_set = -1;
    (*aggstate).current_set = 0;
    (*aggstate).peragg = std::ptr::null_mut();
    (*aggstate).pertrans = std::ptr::null_mut();
    (*aggstate).curperagg = std::ptr::null_mut();
    (*aggstate).curpertrans = std::ptr::null_mut();
    (*aggstate).input_done = false;
    (*aggstate).agg_done = false;
    (*aggstate).pergroups = std::ptr::null_mut();
    (*aggstate).grp_firstTuple = std::ptr::null_mut();
    (*aggstate).sort_in = std::ptr::null_mut();
    (*aggstate).sort_out = std::ptr::null_mut();

    /*
     * phases[0] always exists, but is dummy in sorted/plain mode
     */
    numPhases = if use_hashing { 1 } else { 2 };
    numHashes = if use_hashing { 1 } else { 0 };

    /*
     * Calculate the maximum number of grouping sets in any phase; this
     * determines the size of some allocations.
     */
    if !(*node).groupingSets.is_null() {
        numGroupingSets = list_length((*node).groupingSets);

        // foreach(l, node->chain)
        // TODO(pg-port): iterate list
        // for each Agg *agg in node->chain:
        //   numGroupingSets = Max(numGroupingSets, list_length(agg->groupingSets))
        //   if agg->aggstrategy != AGG_HASHED: ++numPhases; else: ++numHashes;
    }

    (*aggstate).maxsets = numGroupingSets;
    (*aggstate).numphases = numPhases;

    (*aggstate).aggcontexts = palloc0(
        core::mem::size_of::<*mut ExprContext>() * numGroupingSets as usize,
    ) as *mut *mut ExprContext;

    /*
     * Create expression contexts.
     */
    ExecAssignExprContext(estate, &raw mut (*aggstate).ss.ps);
    (*aggstate).tmpcontext = (*aggstate).ss.ps.ps_ExprContext;

    for i in 0..numGroupingSets {
        ExecAssignExprContext(estate, &raw mut (*aggstate).ss.ps);
        *(*aggstate).aggcontexts.add(i as usize) = (*aggstate).ss.ps.ps_ExprContext;
    }

    if use_hashing {
        hash_create_memory(aggstate);
    }

    ExecAssignExprContext(estate, &raw mut (*aggstate).ss.ps);

    /*
     * Initialize child nodes.
     */
    let mut eflags = eflags;
    if (*node).aggstrategy == AGG_HASHED {
        eflags &= !EXEC_FLAG_REWIND;
    }
    outerPlan = (*node).plan.lefttree; // outerPlan(node)
    let outerPlanState_node =
        ExecInitNode(outerPlan, estate, eflags);
    (*aggstate).ss.ps.lefttree = outerPlanState_node; // outerPlanState(aggstate) = ...

    /*
     * initialize source tuple type.
     */
    (*aggstate).ss.ps.outerops = ExecGetResultSlotOps(
        outerPlanState_node,
        &mut (*aggstate).ss.ps.outeropsfixed,
    );
    (*aggstate).ss.ps.outeropsset = true;

    ExecCreateScanSlotFromOuterPlan(estate, &raw mut (*aggstate).ss as *mut crate::nodes::execnodes::ScanState, (*aggstate).ss.ps.outerops);
    scanDesc = (*(*aggstate).ss.ss_ScanTupleSlot).tts_tupleDescriptor;

    /*
     * If there are more than two phases, input will be resorted using
     * tuplesort. Need a slot for that.
     */
    if numPhases > 2 {
        (*aggstate).sort_slot =
            ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);

        if (*aggstate).ss.ps.outeropsfixed
            && (*aggstate).ss.ps.outerops != &TTSOpsMinimalTuple
        {
            (*aggstate).ss.ps.outeropsfixed = false;
        }
    }

    /*
     * Initialize result type, slot and projection.
     */
    ExecInitResultTupleSlotTL(&raw mut (*aggstate).ss.ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&raw mut (*aggstate).ss.ps, std::ptr::null_mut());

    /*
     * initialize child expressions
     */
    (*aggstate).ss.ps.qual = ExecInitQual(
        (*node).plan.qual,
        &raw mut (*aggstate).ss.ps,
    );

    /*
     * We should now have found all Aggrefs in the targetlist and quals.
     */
    numaggrefs = list_length((*aggstate).aggs);
    max_aggno = -1;
    max_transno = -1;
    {
        let n = list_length((*aggstate).aggs);
        for idx in 0..n {
            let aggref = list_nth((*aggstate).aggs, idx) as *mut Aggref;
            max_aggno = Max!(max_aggno, (*aggref).aggno);
            max_transno = Max!(max_transno, (*aggref).aggtransno);
        }
    }
    (*aggstate).numaggs = max_aggno + 1;
    numaggs = (*aggstate).numaggs;
    (*aggstate).numtrans = max_transno + 1;
    numtrans = (*aggstate).numtrans;

    /*
     * For each phase, prepare grouping set data and fmgr lookup data.
     */
    (*aggstate).phases = palloc0(
        numPhases as usize * core::mem::size_of::<PerphaseFull>(),
    ) as AggStatePerPhase;

    (*aggstate).num_hashes = numHashes;
    if numHashes > 0 {
        (*aggstate).perhash = palloc0(
            core::mem::size_of::<PerhashFull>() * numHashes as usize,
        ) as AggStatePerHash;
        let phase0 = ((*aggstate).phases as *mut PerphaseFull).add(0);
        (*phase0).numsets = 0;
        (*phase0).gset_lengths = palloc(numHashes as Size * core::mem::size_of::<c_int>()) as *mut c_int;
        (*phase0).grouped_cols = palloc(numHashes as Size * core::mem::size_of::<*mut Bitmapset>()) as *mut *mut Bitmapset;
    }

    let phases = (*aggstate).phases as *mut PerphaseFull;
    let mut phase: c_int = 0;
    for phaseidx in 0..=list_length((*node).chain) {
        let aggnode: *mut Agg;
        let sortnode: *mut Sort;

        if phaseidx > 0 {
            aggnode = list_nth_node((*node).chain, phaseidx - 1) as *mut Agg;
            sortnode = castNode!(Sort, (*aggnode).plan.lefttree);
        } else {
            aggnode = node;
            sortnode = std::ptr::null_mut();
        }

        Assert!(phase <= 1 || !sortnode.is_null());

        if (*aggnode).aggstrategy == AGG_HASHED || (*aggnode).aggstrategy == AGG_MIXED {
            let phasedata = phases.add(0);
            let mut cols: *mut Bitmapset = std::ptr::null_mut();

            Assert!(phase == 0);
            let i = (*phasedata).numsets;
            (*phasedata).numsets += 1;
            let perhash = ((*aggstate).perhash as *mut PerhashFull).add(i as usize);

            /* phase 0 always points to the "real" Agg in the hash case */
            (*phasedata).aggnode = node;
            (*phasedata).aggstrategy = (*node).aggstrategy;

            /* but the actual Agg node representing this hash is saved here */
            (*perhash).aggnode = aggnode;

            (*perhash).numCols = (*aggnode).numCols;
            *(*phasedata).gset_lengths.add(i as usize) = (*aggnode).numCols;

            for j in 0..(*aggnode).numCols {
                cols = bms_add_member(cols, *(*aggnode).grpColIdx.add(j as usize) as c_int);
            }

            *(*phasedata).grouped_cols.add(i as usize) = cols;

            all_grouped_cols = bms_add_members(all_grouped_cols, cols);
            continue;
        } else {
            phase += 1;
            let phasedata = phases.add(phase as usize);
            let num_sets = list_length((*aggnode).groupingSets);

            (*phasedata).numsets = num_sets;

            if num_sets != 0 {
                (*phasedata).gset_lengths =
                    palloc(num_sets as Size * core::mem::size_of::<c_int>()) as *mut c_int;
                (*phasedata).grouped_cols =
                    palloc(num_sets as Size * core::mem::size_of::<*mut Bitmapset>())
                        as *mut *mut Bitmapset;

                let nsets = list_length((*aggnode).groupingSets);
                for i in 0..nsets {
                    let gset = list_nth((*aggnode).groupingSets, i) as *mut List;
                    let current_length = list_length(gset);
                    let mut cols: *mut Bitmapset = std::ptr::null_mut();

                    /* planner forces this to be correct */
                    for j in 0..current_length {
                        cols = bms_add_member(cols, *(*aggnode).grpColIdx.add(j as usize) as c_int);
                    }

                    *(*phasedata).grouped_cols.add(i as usize) = cols;
                    *(*phasedata).gset_lengths.add(i as usize) = current_length;
                }

                all_grouped_cols =
                    bms_add_members(all_grouped_cols, *(*phasedata).grouped_cols.add(0));
            } else {
                Assert!(phaseidx == 0);

                (*phasedata).gset_lengths = std::ptr::null_mut();
                (*phasedata).grouped_cols = std::ptr::null_mut();
            }

            /*
             * If we are grouping, precompute fmgr lookup data for inner loop.
             */
            if (*aggnode).aggstrategy == AGG_SORTED {
                (*phasedata).eqfunctions =
                    palloc0((*aggnode).numCols as Size * core::mem::size_of::<*mut ExprState>())
                        as *mut *mut ExprState;

                /* for each grouping set */
                for k in 0..(*phasedata).numsets {
                    let length = *(*phasedata).gset_lengths.add(k as usize);

                    if length == 0 {
                        continue;
                    }

                    if !(*(*phasedata).eqfunctions.add((length - 1) as usize)).is_null() {
                        continue;
                    }

                    *(*phasedata).eqfunctions.add((length - 1) as usize) = execTuplesMatchPrepare(
                        scanDesc as *mut c_void,
                        length,
                        (*aggnode).grpColIdx,
                        (*aggnode).grpOperators,
                        (*aggnode).grpCollations,
                        &raw mut (*aggstate).ss.ps as *mut c_void,
                    ) as *mut ExprState;
                }

                /* and for all grouped columns, unless already computed */
                if (*aggnode).numCols > 0
                    && (*(*phasedata).eqfunctions.add(((*aggnode).numCols - 1) as usize)).is_null()
                {
                    *(*phasedata).eqfunctions.add(((*aggnode).numCols - 1) as usize) =
                        execTuplesMatchPrepare(
                            scanDesc as *mut c_void,
                            (*aggnode).numCols,
                            (*aggnode).grpColIdx,
                            (*aggnode).grpOperators,
                            (*aggnode).grpCollations,
                            &raw mut (*aggstate).ss.ps as *mut c_void,
                        ) as *mut ExprState;
                }
            }

            (*phasedata).aggnode = aggnode;
            (*phasedata).aggstrategy = (*aggnode).aggstrategy;
            (*phasedata).sortnode = sortnode;
        }
    }

    /*
     * Convert all_grouped_cols to a descending-order list.
     */
    {
        let mut i: c_int = -1;
        loop {
            i = bms_next_member(all_grouped_cols, i);
            if i < 0 {
                break;
            }
            (*aggstate).all_grouped_cols = lcons_int(i, (*aggstate).all_grouped_cols);
        }
    }

    /*
     * Set up aggregate-result storage in the output expr context.
     */
    econtext = (*aggstate).ss.ps.ps_ExprContext;
    (*econtext).ecxt_aggvalues =
        palloc0(core::mem::size_of::<Datum>() * numaggs as usize) as *mut Datum;
    (*econtext).ecxt_aggnulls =
        palloc0(core::mem::size_of::<bool>() * numaggs as usize) as *mut bool;

    peraggs = palloc0(
        core::mem::size_of::<PeraggFull>() * numaggs as usize,
    ) as *mut PeraggFull;
    pertransstates = palloc0(
        core::mem::size_of::<PertransFull>() * numtrans as usize,
    ) as *mut PertransFull;

    (*aggstate).peragg = peraggs as AggStatePerAgg;
    (*aggstate).pertrans = pertransstates as AggStatePerTrans;

    (*aggstate).all_pergroups = palloc0(
        core::mem::size_of::<AggStatePerGroup>()
            * (numGroupingSets + numHashes) as usize,
    ) as *mut AggStatePerGroup;
    pergroups = (*aggstate).all_pergroups;

    if (*node).aggstrategy != AGG_HASHED {
        for i in 0..numGroupingSets {
            *pergroups.add(i as usize) = palloc0(
                core::mem::size_of::<PergroupFull>() * numaggs as usize,
            ) as AggStatePerGroup;
        }
        (*aggstate).pergroups = pergroups;
        pergroups = pergroups.add(numGroupingSets as usize);
    }

    /*
     * Hashing can only appear in the initial phase.
     */
    if use_hashing {
        let outerplan = outerPlan;
        let mut totalGroups: uint64 = 0;

        (*aggstate).hash_spill_rslot =
            ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);
        (*aggstate).hash_spill_wslot =
            ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsVirtual);

        /* this is an array of pointers, not structures */
        (*aggstate).hash_pergroup = pergroups;

        (*aggstate).hashentrysize = hash_agg_entry_size(
            (*aggstate).numtrans,
            (*outerplan).plan_width as Size,
            (*node).transitionSpace as Size,
        );

        // for k in 0..aggstate->num_hashes: totalGroups += perhash[k].aggnode->numGroups
        // TODO(pg-port): perhash loop

        hash_agg_set_limits(
            (*aggstate).hashentrysize,
            totalGroups as f64,
            0,
            &mut (*aggstate).hash_mem_limit,
            &mut (*aggstate).hash_ngroups_limit,
            &mut (*aggstate).hash_planned_partitions,
        );
        find_hash_columns(aggstate);

        /* Skip massive memory allocation if we are just doing EXPLAIN */
        if eflags & EXEC_FLAG_EXPLAIN_ONLY == 0 {
            build_hash_tables(aggstate);
        }

        (*aggstate).table_filled = false;

        /* Initialize this to 1, meaning nothing spilled, yet */
        (*aggstate).hash_batches_used = 1;
    }

    /*
     * Initialize current phase-dependent values to initial phase.
     */
    if (*node).aggstrategy == AGG_HASHED {
        (*aggstate).current_phase = 0;
        initialize_phase(aggstate, 0);
        select_current_set(aggstate, 0, true);
    } else {
        (*aggstate).current_phase = 1;
        initialize_phase(aggstate, 1);
        select_current_set(aggstate, 0, false);
    }

    /*
     * Perform lookups of aggregate function info, and initialize the
     * unchanging fields of the per-agg and per-trans data.
     */
    {
        let naggs = list_length((*aggstate).aggs);
        for aidx in 0..naggs {
            let aggref = list_nth((*aggstate).aggs, aidx) as *mut Aggref;
            let mut aggTransFnInputTypes: [Oid; FUNC_MAX_ARGS as usize] =
                [0; FUNC_MAX_ARGS as usize];
            let numAggTransFnArgs: c_int;
            let numDirectArgs: c_int;
            let aggform: Form_pg_aggregate;
            let mut aclresult: AclResult;
            let finalfn_oid: Oid;
            let mut serialfn_oid: Oid;
            let mut deserialfn_oid: Oid;
            let aggOwner: Oid;
            let mut finalfnexpr: *mut Expr = std::ptr::null_mut();
            let aggtranstype: Oid;

            /* Planner should have assigned aggregate to correct level */
            Assert!((*aggref).agglevelsup == 0);
            /* ... and the split mode should match */
            Assert!((*aggref).aggsplit == (*aggstate).aggsplit);

            let peragg = peraggs.add((*aggref).aggno as usize);

            /* Check if we initialized the state for this aggregate already. */
            if !(*peragg).aggref.is_null() {
                continue;
            }

            (*peragg).aggref = aggref;
            (*peragg).transno = (*aggref).aggtransno;

            /* Fetch the pg_aggregate row */
            let aggTuple = SearchSysCache1(
                AGGFNOID,
                ObjectIdGetDatum((*aggref).aggfnoid),
            );
            if aggTuple.is_null() {
                elog!(ERROR, "cache lookup failed for aggregate {}", (*aggref).aggfnoid);
            }
            aggform = GETSTRUCT::<FormData_pg_aggregate>(aggTuple);

            /* Check permission to call aggregate function */
            aclresult = object_aclcheck(
                ProcedureRelationId,
                (*aggref).aggfnoid,
                GetUserId(),
                ACL_EXECUTE,
            );
            if aclresult != ACLCHECK_OK {
                aclcheck_error(aclresult, OBJECT_AGGREGATE, get_func_name((*aggref).aggfnoid));
            }
            InvokeFunctionExecuteHook((*aggref).aggfnoid);

            /* planner recorded transition state type in the Aggref itself */
            aggtranstype = (*aggref).aggtranstype;
            Assert!(OidIsValid(aggtranstype));

            /* Final function only required if we're finalizing the aggregates */
            if DO_AGGSPLIT_SKIPFINAL((*aggstate).aggsplit) {
                finalfn_oid = InvalidOid;
            } else {
                finalfn_oid = (*aggform).aggfinalfn;
            }
            (*peragg).finalfn_oid = finalfn_oid;

            serialfn_oid = InvalidOid;
            deserialfn_oid = InvalidOid;

            /*
             * Check if serialization/deserialization is required.  We only do
             * it for aggregates that have transtype INTERNAL.
             */
            if aggtranstype == INTERNALOID {
                if DO_AGGSPLIT_SERIALIZE((*aggstate).aggsplit) {
                    Assert!(DO_AGGSPLIT_SKIPFINAL((*aggstate).aggsplit));
                    if !OidIsValid((*aggform).aggserialfn) {
                        elog!(ERROR, "serialfunc not provided for serialization aggregation");
                    }
                    serialfn_oid = (*aggform).aggserialfn;
                }
                if DO_AGGSPLIT_DESERIALIZE((*aggstate).aggsplit) {
                    Assert!(DO_AGGSPLIT_COMBINE((*aggstate).aggsplit));
                    if !OidIsValid((*aggform).aggdeserialfn) {
                        elog!(ERROR, "deserialfunc not provided for deserialization aggregation");
                    }
                    deserialfn_oid = (*aggform).aggdeserialfn;
                }
            }

            /* Check that aggregate owner has permission to call component fns */
            {
                let procTuple = SearchSysCache1(
                    PROCOID,
                    ObjectIdGetDatum((*aggref).aggfnoid),
                );
                if procTuple.is_null() {
                    elog!(ERROR, "cache lookup failed for function {}", (*aggref).aggfnoid);
                }
                aggOwner = (*GETSTRUCT::<FormData_pg_proc>(procTuple)).proowner;
                ReleaseSysCache(procTuple);

                if OidIsValid(finalfn_oid) {
                    aclresult = object_aclcheck(ProcedureRelationId, finalfn_oid, aggOwner, ACL_EXECUTE);
                    if aclresult != ACLCHECK_OK {
                        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(finalfn_oid));
                    }
                    InvokeFunctionExecuteHook(finalfn_oid);
                }
                if OidIsValid(serialfn_oid) {
                    aclresult = object_aclcheck(ProcedureRelationId, serialfn_oid, aggOwner, ACL_EXECUTE);
                    if aclresult != ACLCHECK_OK {
                        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(serialfn_oid));
                    }
                    InvokeFunctionExecuteHook(serialfn_oid);
                }
                if OidIsValid(deserialfn_oid) {
                    aclresult = object_aclcheck(ProcedureRelationId, deserialfn_oid, aggOwner, ACL_EXECUTE);
                    if aclresult != ACLCHECK_OK {
                        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(deserialfn_oid));
                    }
                    InvokeFunctionExecuteHook(deserialfn_oid);
                }
            }

            /*
             * Get actual datatypes of the (nominal) aggregate inputs.
             */
            numAggTransFnArgs =
                get_aggregate_argtypes(aggref, aggTransFnInputTypes.as_mut_ptr());

            /* Count the "direct" arguments, if any */
            numDirectArgs = list_length((*aggref).aggdirectargs);

            /* Detect how many arguments to pass to the finalfn */
            if (*aggform).aggfinalextra {
                (*peragg).numFinalArgs = numAggTransFnArgs + 1;
            } else {
                (*peragg).numFinalArgs = numDirectArgs + 1;
            }

            /* Initialize any direct-argument expressions */
            (*peragg).aggdirectargs =
                ExecInitExprList((*aggref).aggdirectargs, &raw mut (*aggstate).ss.ps);

            /*
             * build expression trees using actual argument & result types for
             * the finalfn, if it exists and is required.
             */
            if OidIsValid(finalfn_oid) {
                build_aggregate_finalfn_expr(
                    aggTransFnInputTypes.as_mut_ptr(),
                    (*peragg).numFinalArgs,
                    aggtranstype,
                    (*aggref).aggtype,
                    (*aggref).inputcollid,
                    finalfn_oid,
                    &mut finalfnexpr,
                );
                fmgr_info(finalfn_oid, &mut (*peragg).finalfn);
                fmgr_info_set_expr!(finalfnexpr as *mut Node, &mut (*peragg).finalfn);
            }

            /* get info about the output value's datatype */
            get_typlenbyval(
                (*aggref).aggtype,
                &mut (*peragg).resulttypeLen,
                &mut (*peragg).resulttypeByVal,
            );

            /*
             * Build working state for invoking the transition function, if we
             * haven't done it already.
             */
            let pertrans = pertransstates.add((*aggref).aggtransno as usize);
            if (*pertrans).aggref.is_null() {
                let initValue: Datum;
                let mut initValueIsNull: bool = false;
                let transfn_oid: Oid;

                if DO_AGGSPLIT_COMBINE((*aggstate).aggsplit) {
                    transfn_oid = (*aggform).aggcombinefn;
                    if !OidIsValid(transfn_oid) {
                        elog!(ERROR, "combinefn not set for aggregate function");
                    }
                } else {
                    transfn_oid = (*aggform).aggtransfn;
                }

                aclresult = object_aclcheck(ProcedureRelationId, transfn_oid, aggOwner, ACL_EXECUTE);
                if aclresult != ACLCHECK_OK {
                    aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(transfn_oid));
                }
                InvokeFunctionExecuteHook(transfn_oid);

                let textInitVal = SysCacheGetAttr(
                    AGGFNOID,
                    aggTuple,
                    Anum_pg_aggregate_agginitval as crate::access::attnum::AttrNumber,
                    &mut initValueIsNull,
                );
                if initValueIsNull {
                    initValue = 0 as Datum;
                } else {
                    initValue = GetAggInitVal(textInitVal, aggtranstype);
                }

                if DO_AGGSPLIT_COMBINE((*aggstate).aggsplit) {
                    let combineFnInputTypes: [Oid; 2] = [aggtranstype, aggtranstype];

                    (*pertrans).numTransInputs = 1;

                    build_pertrans_for_aggref(
                        pertrans, aggstate, estate, aggref, transfn_oid, aggtranstype,
                        serialfn_oid, deserialfn_oid, initValue, initValueIsNull,
                        combineFnInputTypes.as_ptr(), 2,
                    );

                    if (*pertrans).transfn.fn_strict && aggtranstype == INTERNALOID {
                        ereport!(ERROR, errmsg!(
                            "combine function with transition type {} must not be declared STRICT",
                            std::ffi::CStr::from_ptr(format_type_be(aggtranstype)).to_string_lossy()
                        ));
                    }
                } else {
                    if AGGKIND_IS_ORDERED_SET((*aggref).aggkind) {
                        (*pertrans).numTransInputs = list_length((*aggref).args);
                    } else {
                        (*pertrans).numTransInputs = numAggTransFnArgs;
                    }

                    build_pertrans_for_aggref(
                        pertrans, aggstate, estate, aggref, transfn_oid, aggtranstype,
                        serialfn_oid, deserialfn_oid, initValue, initValueIsNull,
                        aggTransFnInputTypes.as_ptr(), numAggTransFnArgs,
                    );

                    if (*pertrans).transfn.fn_strict && (*pertrans).initValueIsNull
                        && (numAggTransFnArgs <= numDirectArgs
                            || !IsBinaryCoercible(
                                aggTransFnInputTypes[numDirectArgs as usize],
                                aggtranstype,
                            ))
                    {
                        ereport!(ERROR, errmsg!(
                            "aggregate {} needs to have compatible input type and transition type",
                            (*aggref).aggfnoid
                        ));
                    }
                }
            } else {
                (*pertrans).aggshared = true;
            }
            ReleaseSysCache(aggTuple);
        }
    }

    /*
     * Last, check whether any more aggregates got added onto the node.
     */
    if numaggrefs != list_length((*aggstate).aggs) {
        ereport!(ERROR, errmsg!("aggregate function calls cannot be nested"));
    }

    /*
     * Build expressions doing all the transition work at once.
     */
    for phaseidx in 0..(*aggstate).numphases {
        let phase_p = ((*aggstate).phases as *mut PerphaseFull).add(phaseidx as usize);
        let mut dohash = false;
        let mut dosort = false;

        /* phase 0 doesn't necessarily exist */
        if (*phase_p).aggnode.is_null() {
            continue;
        }

        if (*aggstate).aggstrategy == AGG_MIXED && phaseidx == 1 {
            dohash = true;
            dosort = true;
        } else if (*aggstate).aggstrategy == AGG_MIXED && phaseidx == 0 {
            continue;
        } else if (*phase_p).aggstrategy == AGG_PLAIN || (*phase_p).aggstrategy == AGG_SORTED {
            dohash = false;
            dosort = true;
        } else if (*phase_p).aggstrategy == AGG_HASHED {
            dohash = true;
            dosort = false;
        } else {
            Assert!(false);
        }

        (*phase_p).evaltrans =
            ExecBuildAggTrans(aggstate, phase_p as AggStatePerPhase, dosort, dohash, false);

        /* cache compiled expression for outer slot without NULL check */
        (*phase_p).evaltrans_cache[0][0] = (*phase_p).evaltrans;
    }

    aggstate
}

// --------------------------------------------------------------------------
// build_pertrans_for_aggref
// --------------------------------------------------------------------------

/*
 * Build the state needed to calculate a state value for an aggregate.
 */
unsafe fn build_pertrans_for_aggref(
    pertrans: *mut PertransFull,
    aggstate: *mut AggState,
    estate: *mut EState,
    aggref: *mut Aggref,
    transfn_oid: Oid,
    aggtranstype: Oid,
    aggserialfn: Oid,
    aggdeserialfn: Oid,
    initValue: Datum,
    initValueIsNull: bool,
    inputTypes: *const Oid,
    numArguments: c_int,
) {
    let numGroupingSets: c_int = Max!((*aggstate).maxsets, 1);
    let mut transfnexpr: *mut Expr = std::ptr::null_mut();
    let numTransArgs: c_int;
    let mut serialfnexpr: *mut Expr = std::ptr::null_mut();
    let mut deserialfnexpr: *mut Expr = std::ptr::null_mut();
    let numInputs: c_int;
    let numDirectArgs: c_int;
    let mut numSortCols: c_int;
    let mut numDistinctCols: c_int;

    /* Begin filling in the pertrans data */
    (*pertrans).aggref = aggref;
    (*pertrans).aggshared = false;
    (*pertrans).aggCollation = (*aggref).inputcollid;
    (*pertrans).transfn_oid = transfn_oid;
    (*pertrans).serialfn_oid = aggserialfn;
    (*pertrans).deserialfn_oid = aggdeserialfn;
    (*pertrans).initValue = initValue;
    (*pertrans).initValueIsNull = initValueIsNull;

    /* Count the "direct" arguments, if any */
    numDirectArgs = list_length((*aggref).aggdirectargs);

    /* Count the number of aggregated input columns */
    (*pertrans).numInputs = list_length((*aggref).args);
    numInputs = (*pertrans).numInputs;

    (*pertrans).aggtranstype = aggtranstype;

    /* account for the current transition state */
    numTransArgs = (*pertrans).numTransInputs + 1;

    /*
     * Set up infrastructure for calling the transfn.
     */
    build_aggregate_transfn_expr(
        inputTypes,
        numArguments,
        numDirectArgs,
        (*aggref).aggvariadic,
        aggtranstype,
        (*aggref).inputcollid,
        transfn_oid,
        InvalidOid,
        &mut transfnexpr,
        std::ptr::null_mut(),
    );

    fmgr_info(transfn_oid, &mut (*pertrans).transfn);
    fmgr_info_set_expr!(transfnexpr as *mut Node, &mut (*pertrans).transfn);

    (*pertrans).transfn_fcinfo =
        palloc(SizeForFunctionCallInfo(numTransArgs as usize)) as FunctionCallInfo;
    InitFunctionCallInfoData!(
        (*pertrans).transfn_fcinfo,
        &mut (*pertrans).transfn,
        numTransArgs as i16,
        (*pertrans).aggCollation,
        aggstate as *mut Node,
        std::ptr::null_mut()
    );

    /* get info about the state value's datatype */
    get_typlenbyval(
        aggtranstype,
        &mut (*pertrans).transtypeLen,
        &mut (*pertrans).transtypeByVal,
    );

    if OidIsValid(aggserialfn) {
        build_aggregate_serialfn_expr(aggserialfn, &mut serialfnexpr);
        fmgr_info(aggserialfn, &mut (*pertrans).serialfn);
        fmgr_info_set_expr!(serialfnexpr as *mut Node, &mut (*pertrans).serialfn);

        (*pertrans).serialfn_fcinfo =
            palloc(SizeForFunctionCallInfo(1)) as FunctionCallInfo;
        InitFunctionCallInfoData!(
            (*pertrans).serialfn_fcinfo,
            &mut (*pertrans).serialfn,
            1_i16,
            InvalidOid,
            aggstate as *mut Node,
            std::ptr::null_mut()
        );
    }

    if OidIsValid(aggdeserialfn) {
        build_aggregate_deserialfn_expr(aggdeserialfn, &mut deserialfnexpr);
        fmgr_info(aggdeserialfn, &mut (*pertrans).deserialfn);
        fmgr_info_set_expr!(deserialfnexpr as *mut Node, &mut (*pertrans).deserialfn);

        (*pertrans).deserialfn_fcinfo =
            palloc(SizeForFunctionCallInfo(2)) as FunctionCallInfo;
        InitFunctionCallInfoData!(
            (*pertrans).deserialfn_fcinfo,
            &mut (*pertrans).deserialfn,
            2_i16,
            InvalidOid,
            aggstate as *mut Node,
            std::ptr::null_mut()
        );
    }

    /*
     * If we're doing either DISTINCT or ORDER BY for a plain agg, then we
     * have a list of SortGroupClause nodes; fish out the data in them and
     * stick them into arrays.
     */
    let sortlist: *mut List;
    if AGGKIND_IS_ORDERED_SET((*aggref).aggkind) {
        sortlist = NIL as *mut List;
        numSortCols = 0;
        numDistinctCols = 0;
        (*pertrans).aggsortrequired = false;
    } else if (*aggref).aggpresorted && (*aggref).aggdistinct.is_null() {
        sortlist = NIL as *mut List;
        numSortCols = 0;
        numDistinctCols = 0;
        (*pertrans).aggsortrequired = false;
    } else if !(*aggref).aggdistinct.is_null() {
        sortlist = (*aggref).aggdistinct;
        numSortCols = list_length(sortlist);
        numDistinctCols = numSortCols;
        (*pertrans).aggsortrequired = !(*aggref).aggpresorted;
    } else {
        sortlist = (*aggref).aggorder;
        numSortCols = list_length(sortlist);
        numDistinctCols = 0;
        (*pertrans).aggsortrequired = numSortCols > 0;
    }

    (*pertrans).numSortCols = numSortCols;
    (*pertrans).numDistinctCols = numDistinctCols;

    /*
     * If we have either sorting or filtering to do, create a tupledesc and
     * slot corresponding to the aggregated inputs.
     */
    if numSortCols > 0 || !(*aggref).aggfilter.is_null() {
        (*pertrans).sortdesc = ExecTypeFromTL((*aggref).args) as *mut c_void;
        (*pertrans).sortslot = ExecInitExtraTupleSlot(
            estate,
            (*pertrans).sortdesc as crate::access::common::tupdesc::TupleDesc,
            &TTSOpsMinimalTuple,
        );
    }

    if numSortCols > 0 {
        /* ORDER BY aggregates are not supported with partial aggregation */
        Assert!(!DO_AGGSPLIT_COMBINE((*aggstate).aggsplit));

        /* If we have only one input, we need its len/byval info. */
        if numInputs == 1 {
            get_typlenbyval(
                *inputTypes.add(numDirectArgs as usize),
                &mut (*pertrans).inputtypeLen,
                &mut (*pertrans).inputtypeByVal,
            );
        } else if numDistinctCols > 0 {
            /* we will need an extra slot to store prior values */
            (*pertrans).uniqslot = ExecInitExtraTupleSlot(
                estate,
                (*pertrans).sortdesc as crate::access::common::tupdesc::TupleDesc,
                &TTSOpsMinimalTuple,
            );
        }

        /* Extract the sort information for use later */
        (*pertrans).sortColIdx =
            palloc(numSortCols as Size * core::mem::size_of::<i16>()) as *mut i16;
        (*pertrans).sortOperators =
            palloc(numSortCols as Size * core::mem::size_of::<Oid>()) as *mut Oid;
        (*pertrans).sortCollations =
            palloc(numSortCols as Size * core::mem::size_of::<Oid>()) as *mut Oid;
        (*pertrans).sortNullsFirst =
            palloc(numSortCols as Size * core::mem::size_of::<bool>()) as *mut bool;

        // TODO(pg-port): foreach(lc, sortlist) -- fill sortColIdx/sortOperators etc.
    }

    if !(*aggref).aggdistinct.is_null() {
        let ops =
            palloc(numDistinctCols as Size * core::mem::size_of::<Oid>()) as *mut Oid;

        // TODO(pg-port): foreach(lc, aggref->aggdistinct) ops[i++] = sortcl->eqop

        /* lookup / build the necessary comparators */
        if numDistinctCols == 1 {
            fmgr_info(get_opcode(*ops.add(0)), &mut (*pertrans).equalfnOneFull);
        } else {
            let eq_result = execTuplesMatchPrepare(
                (*pertrans).sortdesc as *mut c_void,
                numDistinctCols,
                (*pertrans).sortColIdx,
                ops,
                (*pertrans).sortCollations,
                &raw mut (*aggstate).ss.ps as *mut c_void,
            );
            (*pertrans).equalfnMultiFull = eq_result as *mut ExprState;
        }
        pfree(ops as *mut c_void);
    }

    (*pertrans).sortstates = palloc0(
        core::mem::size_of::<*mut Tuplesortstate>() * numGroupingSets as usize,
    ) as *mut *mut Tuplesortstate;
}

// --------------------------------------------------------------------------
// ExecEndAgg
// --------------------------------------------------------------------------

pub unsafe fn ExecEndAgg(node: *mut AggState) {
    let numGroupingSets: c_int = Max!((*node).maxsets, 1);

    /*
     * When ending a parallel worker, copy the statistics gathered by the
     * worker back into shared memory.
     */
    if !(*node).shared_info.is_null() && IsParallelWorker() {
        let si = &mut (*(*node).shared_info).sinstrument[ParallelWorkerNumber as usize];
        si.hash_batches_used = (*node).hash_batches_used;
        si.hash_disk_used = (*node).hash_disk_used;
        si.hash_mem_peak = (*node).hash_mem_peak;
    }

    /* Make sure we have closed any open tuplesorts */
    if !(*node).sort_in.is_null() {
        tuplesort_end((*node).sort_in);
    }
    if !(*node).sort_out.is_null() {
        tuplesort_end((*node).sort_out);
    }

    hashagg_reset_spill_state(node);

    if !(*node).hash_metacxt.is_null() {
        MemoryContextDelete((*node).hash_metacxt);
        (*node).hash_metacxt = std::ptr::null_mut();
    }
    if !(*node).hash_tablecxt.is_null() {
        MemoryContextDelete((*node).hash_tablecxt);
        (*node).hash_tablecxt = std::ptr::null_mut();
    }

    let transstates = (*node).pertrans as *mut PertransFull;
    for transno in 0..(*node).numtrans {
        let pertrans = transstates.add(transno as usize);
        for setno in 0..numGroupingSets {
            if !(*(*pertrans).sortstates.add(setno as usize)).is_null() {
                tuplesort_end(*(*pertrans).sortstates.add(setno as usize));
            }
        }
    }

    /* And ensure any agg shutdown callbacks have been called */
    for setno in 0..numGroupingSets {
        ReScanExprContext(*(*node).aggcontexts.add(setno as usize));
    }
    if !(*node).hashcontext.is_null() {
        ReScanExprContext((*node).hashcontext);
    }

    let outerPlan = outerPlanState(&raw mut (*node).ss.ps);
    ExecEndNode(outerPlan);
}

// --------------------------------------------------------------------------
// ExecReScanAgg
// --------------------------------------------------------------------------

pub unsafe fn ExecReScanAgg(node: *mut AggState) {
    let econtext = (*node).ss.ps.ps_ExprContext;
    let outerPlan = outerPlanState(&raw mut (*node).ss.ps);
    let aggnode = (*node).ss.ps.plan as *mut Agg; // node->ss.ps.plan cast
    let numGroupingSets: c_int = Max!((*node).maxsets, 1);

    (*node).agg_done = false;

    if (*node).aggstrategy == AGG_HASHED {
        /*
         * In the hashed case, if we haven't yet built the hash table then we
         * can just return.
         */
        if !(*node).table_filled {
            return;
        }

        /*
         * If we do have the hash table, and it never spilled, and the subplan
         * does not have any parameter changes, and none of our own parameter
         * changes affect input expressions, then we can rescan the hash table.
         */
        if (*outerPlan).chgParam.is_null()
            && !(*node).hash_ever_spilled
            && !bms_overlap(
                (*node).ss.ps.chgParam,
                (*aggnode).aggParams,
            )
        {
            let ph0 = ((*node).perhash as *mut PerhashFull).add(0);
            ResetTupleHashIterator((*ph0).hashtable, &mut (*ph0).hashiter);
            select_current_set(node, 0, true);
            return;
        }
    }

    /* Make sure we have closed any open tuplesorts */
    for transno in 0..(*node).numtrans {
        let pertrans = ((*node).pertrans as *mut PertransFull).add(transno as usize);
        for setno in 0..numGroupingSets {
            if !(*(*pertrans).sortstates.add(setno as usize)).is_null() {
                tuplesort_end(*(*pertrans).sortstates.add(setno as usize));
                *(*pertrans).sortstates.add(setno as usize) = std::ptr::null_mut();
            }
        }
    }

    /*
     * We don't need to ReScanExprContext the output tuple context here;
     * ExecReScan already did it. But we do need to reset our per-grouping-set
     * contexts.
     */
    for setno in 0..numGroupingSets {
        ReScanExprContext(*(*node).aggcontexts.add(setno as usize));
    }

    /* Release first tuple of group, if we have made a copy */
    if !(*node).grp_firstTuple.is_null() {
        heap_freetuple((*node).grp_firstTuple);
        (*node).grp_firstTuple = std::ptr::null_mut();
    }
    ExecClearTuple((*node).ss.ss_ScanTupleSlot);

    /* Forget current agg values */
    core::ptr::write_bytes(
        (*econtext).ecxt_aggvalues as *mut u8,
        0,
        core::mem::size_of::<Datum>() * (*node).numaggs as usize,
    );
    core::ptr::write_bytes(
        (*econtext).ecxt_aggnulls as *mut u8,
        0,
        core::mem::size_of::<bool>() * (*node).numaggs as usize,
    );

    if (*node).aggstrategy == AGG_HASHED || (*node).aggstrategy == AGG_MIXED {
        hashagg_reset_spill_state(node);

        (*node).hash_ever_spilled = false;
        (*node).hash_spill_mode = false;
        (*node).hash_ngroups_current = 0;

        ReScanExprContext((*node).hashcontext);
        MemoryContextReset((*node).hash_tablecxt);
        /* Rebuild an empty hash table */
        build_hash_tables(node);
        (*node).table_filled = false;
        /* iterator will be reset when the table is filled */

        hashagg_recompile_expressions(node, false, false);
    }

    if (*node).aggstrategy != AGG_HASHED {
        /*
         * Reset the per-group state (in particular, mark transvalues null)
         */
        for setno in 0..numGroupingSets {
            core::ptr::write_bytes(
                *(*node).pergroups.add(setno as usize) as *mut u8,
                0,
                core::mem::size_of::<PergroupFull>() * (*node).numaggs as usize,
            );
        }

        /* reset to phase 1 */
        initialize_phase(node, 1);

        (*node).input_done = false;
        (*node).projected_set = -1;
    }

    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
}

// --------------------------------------------------------------------------
// API exposed to aggregate functions
// --------------------------------------------------------------------------

/*
 * AggCheckCallContext - test if a SQL function is being called as an aggregate
 *
 * The transition and/or final functions of an aggregate may want to verify
 * that they are being called as aggregates, rather than as plain SQL
 * functions.  They should use this function to do so.
 */
pub unsafe fn AggCheckCallContext(
    fcinfo: FunctionCallInfo,
    aggcontext: *mut MemoryContext,
) -> c_int {
    use crate::nodes::execnodes::WindowAggState;

    if !(*fcinfo).context.is_null() && IsA!((*fcinfo).context, AggState) {
        if !aggcontext.is_null() {
            let aggstate = (*fcinfo).context as *mut AggState;
            let cxt: *mut ExprContext = (*aggstate).curaggcontext;
            *aggcontext = (*cxt).ecxt_per_tuple_memory;
        }
        return AGG_CONTEXT_AGGREGATE;
    }
    if !(*fcinfo).context.is_null() && IsA!((*fcinfo).context, WindowAggState) {
        if !aggcontext.is_null() {
            let winstate = (*fcinfo).context as *mut WindowAggState;
            *aggcontext = (*winstate).curaggcontext;
        }
        return AGG_CONTEXT_WINDOW;
    }

    /* this is just to prevent "uninitialized variable" warnings */
    if !aggcontext.is_null() {
        *aggcontext = std::ptr::null_mut();
    }
    0
}

/*
 * AggGetAggref - allow an aggregate support function to get its Aggref
 *
 * If the function is being called as an aggregate support function,
 * return the Aggref node for the aggregate call.  Otherwise, return NULL.
 */
pub unsafe fn AggGetAggref(fcinfo: FunctionCallInfo) -> *mut Aggref {
    if !(*fcinfo).context.is_null() && IsA!((*fcinfo).context, AggState) {
        let aggstate = (*fcinfo).context as *mut AggState;

        /* check curperagg (valid when in a final function) */
        let curperagg = (*aggstate).curperagg as *mut PeraggFull;
        if !curperagg.is_null() {
            return (*curperagg).aggref;
        }

        /* check curpertrans (valid when in a transition function) */
        let curpertrans = (*aggstate).curpertrans as *mut PertransFull;
        if !curpertrans.is_null() {
            return (*curpertrans).aggref;
        }
    }
    std::ptr::null_mut()
}

/*
 * AggGetTempMemoryContext - fetch short-term memory context for aggregates
 *
 * This is useful in agg final functions.
 */
pub unsafe fn AggGetTempMemoryContext(fcinfo: FunctionCallInfo) -> MemoryContext {
    if !(*fcinfo).context.is_null() && IsA!((*fcinfo).context, AggState) {
        let aggstate = (*fcinfo).context as *mut AggState;
        return (*(*aggstate).tmpcontext).ecxt_per_tuple_memory;
    }
    std::ptr::null_mut()
}

/*
 * AggStateIsShared - find out whether transition state is shared
 *
 * If the function is being called as an aggregate support function,
 * return true if the aggregate's transition state is shared across
 * multiple aggregates, false if it is not.
 */
pub unsafe fn AggStateIsShared(fcinfo: FunctionCallInfo) -> bool {
    if !(*fcinfo).context.is_null() && IsA!((*fcinfo).context, AggState) {
        let aggstate = (*fcinfo).context as *mut AggState;

        /* check curperagg (valid when in a final function) */
        let curperagg = (*aggstate).curperagg as *mut PeraggFull;
        if !curperagg.is_null() {
            let pertrans = ((*aggstate).pertrans as *mut PertransFull)
                .add((*curperagg).transno as usize);
            return (*pertrans).aggshared;
        }

        /* check curpertrans (valid when in a transition function) */
        let curpertrans = (*aggstate).curpertrans as *mut PertransFull;
        if !curpertrans.is_null() {
            return (*curpertrans).aggshared;
        }
    }
    true
}

/*
 * AggRegisterCallback - register a cleanup callback for an aggregate
 *
 * This is useful for aggs to register shutdown callbacks.
 */
pub unsafe fn AggRegisterCallback(
    fcinfo: FunctionCallInfo,
    func: ExprContextCallbackFunction,
    arg: Datum,
) {
    if !(*fcinfo).context.is_null() && IsA!((*fcinfo).context, AggState) {
        let aggstate = (*fcinfo).context as *mut AggState;
        let cxt: *mut ExprContext = (*aggstate).curaggcontext;
        RegisterExprContextCallback(cxt, func, arg);
        return;
    }
    elog!(ERROR, "aggregate function cannot register a callback in this context");
}

// --------------------------------------------------------------------------
// Parallel Query Support
// --------------------------------------------------------------------------

/*
 * ExecAggEstimate
 *
 * Estimate space required to propagate aggregate statistics.
 */
pub unsafe fn ExecAggEstimate(node: *mut AggState, pcxt: *mut ParallelContext) {
    /* don't need this if not instrumenting or no workers */
    if (*node).ss.ps.instrument.is_null() || (*pcxt).nworkers == 0 {
        return;
    }

    let size = mul_size(
        (*pcxt).nworkers as Size,
        core::mem::size_of::<AggregateInstrumentation>(),
    );
    let size = add_size(
        size,
        core::mem::offset_of!(SharedAggInfo, sinstrument),
    );
    shm_toc_estimate_chunk(&mut (*pcxt).estimator, size);
    shm_toc_estimate_keys(&mut (*pcxt).estimator, 1);
}

/*
 * ExecAggInitializeDSM
 *
 * Initialize DSM space for aggregate statistics.
 */
pub unsafe fn ExecAggInitializeDSM(node: *mut AggState, pcxt: *mut ParallelContext) {
    /* don't need this if not instrumenting or no workers */
    if (*node).ss.ps.instrument.is_null() || (*pcxt).nworkers == 0 {
        return;
    }

    let size = core::mem::offset_of!(SharedAggInfo, sinstrument)
        + (*pcxt).nworkers as usize * core::mem::size_of::<AggregateInstrumentation>();
    (*node).shared_info =
        shm_toc_allocate((*pcxt).toc, size) as *mut SharedAggInfo;
    /* ensure any unfilled slots will contain zeroes */
    core::ptr::write_bytes((*node).shared_info as *mut u8, 0, size);
    (*(*node).shared_info).num_workers = (*pcxt).nworkers;
    shm_toc_insert(
        (*pcxt).toc,
        (*(*node).ss.ps.plan).plan_node_id as uint64,
        (*node).shared_info as *mut c_void,
    );
}

/*
 * ExecAggInitializeWorker
 *
 * Attach worker to DSM space for aggregate statistics.
 */
pub unsafe fn ExecAggInitializeWorker(
    node: *mut AggState,
    pwcxt: *mut ParallelWorkerContext,
) {
    (*node).shared_info = shm_toc_lookup(
        (*pwcxt).toc,
        (*(*node).ss.ps.plan).plan_node_id as uint64,
        true,
    ) as *mut SharedAggInfo;
}

/*
 * ExecAggRetrieveInstrumentation
 *
 * Transfer aggregate statistics from DSM to private memory.
 */
pub unsafe fn ExecAggRetrieveInstrumentation(node: *mut AggState) {
    if (*node).shared_info.is_null() {
        return;
    }

    let size = core::mem::offset_of!(SharedAggInfo, sinstrument)
        + (*(*node).shared_info).num_workers as usize
            * core::mem::size_of::<AggregateInstrumentation>();
    let si = palloc(size) as *mut SharedAggInfo;
    core::ptr::copy_nonoverlapping(
        (*node).shared_info as *const u8,
        si as *mut u8,
        size,
    );
    (*node).shared_info = si;
}

// --------------------------------------------------------------------------
// Helpers used above but defined elsewhere in the C file (wrappers / stubs)
// --------------------------------------------------------------------------

/// TODO(pg-port): utils/memutils.h mul_size / add_size
fn mul_size(s1: Size, s2: Size) -> Size {
    s1.saturating_mul(s2)
}
fn add_size(s1: Size, s2: Size) -> Size {
    s1.saturating_add(s2)
}

// TODO(pg-port): utils/fmgr.h InitFunctionCallInfoData wrapper
// -- already imported from crate::utils::fmgr above; this stub used inline above.

// TODO(pg-port): executor/execTuples.h ExecStoreMinimalTuple (already imported above)
// ExecFetchSlotMinimalTuple already imported above.
