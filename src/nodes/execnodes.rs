//! Translated from PostgreSQL 18.3 `src/include/nodes/execnodes.h`.
//!
//! Definitions for executor state nodes.
//!
//! Most plan node types declared in plannodes.h have a corresponding
//! execution-state node type declared here.  An exception is that
//! expression nodes (subtypes of Expr) are usually represented by steps
//! of an ExprState, and fully handled within execExpr* - but sometimes
//! their state needs to be shared with other parts of the executor, as
//! for example with SubPlanState, which nodeSubplan.c has to modify.
//!
//! Node types declared in this file do not have any copy/equal/out/read
//! support.  (That is currently hard-wired in gen_node_support.pl, rather
//! than being explicitly represented by pg_node_attr decorations here.)
//! There is no need for copy, equal, or read support for executor trees.
//! Output support could be useful for debugging; but there are a lot of
//! specialized fields that would require custom code, so for now it's
//! not provided.
//!
//! This header is the executor *runtime state*; it references a great deal of
//! not-yet-translated infrastructure (TupleTableSlot, ExprContext, TupleDesc,
//! Relation, HeapTuple, MemoryContext, Instrumentation, dsa_area, Snapshot,
//! TriggerDesc, etc.).  Those types are aggressively stubbed here (opaque
//! zero-sized structs or `*mut Node`) and get real definitions when their
//! subsystems are translated.  The goal is a compiling, faithful-shaped set of
//! executor-state structs with correct field names and NodeTags.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]

use crate::prelude::*; // Oid, Datum, int*, uint*, Index, Size, bits32, CommandId, NullableDatum, MemoryContext, c_char/c_int/c_void, etc.
use crate::nodes::nodes::{Node, NodeTag, ParseLoc, CmdType, JoinType, AggSplit, Cardinality};
use crate::nodes::nodes::{AggStrategy, LimitOption}; // by-value enums in AggState/LimitState
use crate::nodes::pg_list::List;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::primnodes::*; // Expr, WindowFunc, SubPlan, JsonExpr, MergeAction, Aggref, AttrNumber, NUM_MERGE_MATCH_KINDS, etc.
use crate::nodes::parsenodes::*; // RangeTblEntry, Query, RTEPermissionInfo, TableSampleClause, etc.
use crate::nodes::plannodes::*; // Plan and the plan-node types (real); also ScanDirection, RowMarkType
use crate::nodes::lockoptions::{LockClauseStrength, LockWaitPolicy}; // ExecRowMark strength/waitPolicy

// ParseLoc/Cardinality/JoinType/CmdType/AggSplit are kept in the import list per
// the port spec.  Cardinality and ParseLoc are not referenced directly by any
// field in this header; they are pinned here to match the import contract.
const _: Option<Cardinality> = None;
const _: ParseLoc = 0;

// ----------------------------------------------------------------
//  Scalar typedefs not yet provided by a translated header.
// ----------------------------------------------------------------

/// TODO(pg-port): real def `typedef uint16 OffsetNumber` in storage/off.h.
/// Used by value in PresortedKeyData.attno.
pub type OffsetNumber = uint16;

/// TODO(pg-port): real def `typedef int Buffer` in storage/buf.h.
/// Used by value in IndexOnlyScanState.ioss_VMBuffer.
pub type Buffer = c_int;

/// TODO(pg-port): real def `typedef int slock_t` (spinlock) in storage/s_lock.h
/// (machine dependent).  Used by value in ParallelBitmapHeapState.mutex.
pub type slock_t = c_int;

/// INDEX_MAX_KEYS from pg_config_manual.h; controls fixed-size key arrays.
/// (Mirrors the constant in pathnodes.rs to avoid an extra cross-module import.)
pub const INDEX_MAX_KEYS: usize = 32;

// ----------------------------------------------------------------
//  Opaque stubs for cross-header types not yet translated.
//  Each is defined once and reused for all fields of that type.
// ----------------------------------------------------------------

// The slot abstraction over the various tuple representations the executor
// passes around.  Now backed by the real definitions in executor/tuptable.rs;
// re-exported here so existing `crate::nodes::execnodes::TupleTableSlot` paths
// keep resolving.
pub use crate::executor::tuptable::{TupleTableSlot, TupleTableSlotOps};

// The real TupleDesc now lives in access/common/tupdesc.rs; re-export it here
// (like TupleTableSlot above) so execnodes consumers share the one true type.
pub use crate::access::common::tupdesc::{TupleDesc, TupleDescData};

// RelationData / Relation / RelationPtr now carry their real utils/rel.h
// layout in crate::utils::rel; re-exported here for the many consumers that
// import them via nodes::execnodes.
pub use crate::utils::rel::{Relation, RelationData, RelationPtr};

/// TODO(pg-port): real def `typedef HeapTupleData *HeapTuple` in
/// access/htup.h.
// HeapTupleData/HeapTuple and MinimalTupleData/MinimalTuple now carry their
// real access/htup.h layout in access::htup_details; ItemPointerData its real
// storage/itemptr.h layout. Re-exported here so consumers that referenced them
// via nodes::execnodes keep working, and (importantly) the by-value uses below
// -- curCtid / trss_mintid / trss_maxtid -- get the correct 6-byte size.
pub use crate::access::htup_details::{HeapTuple, HeapTupleData, MinimalTuple, MinimalTupleData};
pub use crate::storage::itemptr::ItemPointerData;

/// TODO(pg-port): real def `typedef struct SnapshotData *Snapshot` in
/// utils/snapshot.h.
#[repr(C)]
pub struct SnapshotData {
    _opaque: [u8; 0],
}
pub type Snapshot = *mut SnapshotData;

/// Real definition lives in utils/fmgr.rs; re-export it so executor-state
/// `.func` fields carry the full FmgrInfo layout (fn_oid/fn_retset/etc).
pub use crate::utils::fmgr::FmgrInfo;

// FunctionCallInfoBaseData/FunctionCallInfo: real fmgr.h layout in utils::fmgr.
pub use crate::utils::fmgr::{FunctionCallInfo, FunctionCallInfoBaseData};

// ParamExecData / ParamListInfoData+ParamListInfo: real nodes/params.h layout.
pub use crate::nodes::params::{ParamExecData, ParamListInfo, ParamListInfoData};

// ErrorSaveContext: real nodes/miscnodes.h layout lives in nodes::miscnodes.
pub use crate::nodes::miscnodes::ErrorSaveContext;

// Instrumentation: real executor/instrument.h layout in executor::instrument.
pub use crate::executor::instrument::Instrumentation;

/// `WorkerInstrumentation` -- per-worker instrumentation array (executor/instrument.h).
#[repr(C)]
pub struct WorkerInstrumentation {
    pub num_workers: c_int,
    pub instrument: [Instrumentation; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/// TODO(pg-port): real def `typedef struct Tuplestorestate` in
/// utils/tuplestore.h.
#[repr(C)]
pub struct Tuplestorestate {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real def `typedef struct Tuplesortstate` in
/// utils/tuplesort.h.
#[repr(C)]
pub struct Tuplesortstate {
    _opaque: [u8; 0],
}

// SortSupportData/SortSupport: real utils/sortsupport.h layout in utils::sort::sortsupport.
pub use crate::utils::sort::sortsupport::{SortSupport, SortSupportData};

/// TODO(pg-port): real def `typedef struct TuplesortInstrumentation` in
/// utils/tuplesort.h.  Embedded by value in SharedSortInfo flexible array.
#[repr(C)]
pub struct TuplesortInstrumentation {
    /// sortMethod -- sort algorithm used (TuplesortMethod, utils/tuplesort.h)
    pub sortMethod: c_int,
    /// spaceType -- type of space spaceUsed represents (TuplesortSpaceType)
    pub spaceType: c_int,
    /// spaceUsed -- space consumption, in kB
    pub spaceUsed: int64,
}

/// TODO(pg-port): real def `typedef struct TriggerDesc` in utils/reltrigger.h.
#[repr(C)]
pub struct TriggerDesc {
    _opaque: [u8; 0],
    /// trig_insert_before_row -- utils/reltrigger.h
    pub trig_insert_before_row: bool,
    /// trig_insert_instead_row -- utils/reltrigger.h
    pub trig_insert_instead_row: bool,
    /// trig_insert_new_table -- utils/reltrigger.h
    pub trig_insert_new_table: bool,
    /// trig_insert_after_row -- utils/reltrigger.h
    pub trig_insert_after_row: bool,
    /// trig_update_before_row -- utils/reltrigger.h
    pub trig_update_before_row: bool,
    /// trig_update_after_row -- utils/reltrigger.h
    pub trig_update_after_row: bool,
    /// trig_delete_before_row -- utils/reltrigger.h
    pub trig_delete_before_row: bool,
    /// trig_delete_after_row -- utils/reltrigger.h
    pub trig_delete_after_row: bool,
    /// trig_update_instead_row -- utils/reltrigger.h // C home: utils/reltrigger.h
    pub trig_update_instead_row: bool,
    /// trig_delete_instead_row -- utils/reltrigger.h // C home: utils/reltrigger.h
    pub trig_delete_instead_row: bool,
}

/// Real definition lives in access::common::tupconvert; re-export it.
/// C home: access/tupconvert.h
pub use crate::access::common::tupconvert::TupleConversionMap;

// QueryEnvironment: real utils/queryenvironment.h layout in utils::misc::queryenvironment.
pub use crate::utils::misc::queryenvironment::QueryEnvironment;

/// TODO(pg-port): real def `typedef struct HTAB` in utils/hsearch.h.
#[repr(C)]
pub struct HTAB {
    _opaque: [u8; 0],
}

// PartitionDirectoryData: real def in partitioning/partdesc.rs. Re-export to unify.
pub use crate::partitioning::partdesc::PartitionDirectoryData;
pub type PartitionDirectory = *mut PartitionDirectoryData;

/// TODO(pg-port): real def `typedef struct ConditionVariable` in
/// storage/condition_variable.h.  Embedded by value in
/// ParallelBitmapHeapState.cv.
#[repr(C)]
pub struct ConditionVariable {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real def `typedef Size dsa_pointer` in utils/dsa.h.
/// Used by value in ParallelBitmapHeapState.tbmiterator.
pub type dsa_pointer = Size;

/// Real definition lives in nodes/tidbitmap.rs; re-export it.
pub use crate::nodes::tidbitmap::TIDBitmap;

/// Real definition lives in lib/pairingheap.rs; re-export it.
pub use crate::lib::pairingheap::pairingheap;

/// TODO(pg-port): real def `typedef struct dlist_head` in lib/ilist.h.
/// Embedded by value in MemoizeState.lru_list.
#[repr(C)]
pub struct dlist_head {
    _opaque: [u8; 0],
}

/// access/skey.h ScanKeyData. Re-export the canonical struct so sizeof/field
/// layout match everywhere (an opaque zero-sized stub made scan-key arrays
/// allocate 0 bytes -> heap corruption + garbage sk_attno in index scans).
pub use crate::access::common::scankey::ScanKeyData;

/// TODO(pg-port): real def `typedef struct TableScanDescData` in
/// access/relscan.h.
#[repr(C)]
pub struct TableScanDescData {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real def `typedef struct IndexScanDescData` in
/// access/relscan.h.
#[repr(C)]
pub struct IndexScanDescData {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real def `typedef struct IndexScanInstrumentation` in
/// access/genam.h.  Embedded by value in the *ScanState structs.
#[repr(C)]
pub struct IndexScanInstrumentation {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real def `typedef struct SharedIndexScanInstrumentation` in
/// access/genam.h.
#[repr(C)]
pub struct SharedIndexScanInstrumentation {
    _opaque: [u8; 0],
}

/// Real definition lives in lib/binaryheap.rs; re-export it so the heap
/// fields (MergeAppendState.ms_heap, GatherMergeState.gm_heap) carry the
/// full struct and binaryheap_* calls typecheck without casts.
pub use crate::lib::binaryheap::binaryheap;

/// TODO(pg-port): real def `typedef struct FdwRoutine` in foreign/fdwapi.h.
/// Fields added from foreign/fdwapi.h for use by commands/copyfrom.c.
#[repr(C)]
pub struct FdwRoutine {
    _opaque: [u8; 0],
    /// ExecForeignInsert(estate, rri, slot, planSlot) -> *TupleTableSlot -- foreign/fdwapi.h
    pub ExecForeignInsert: Option<
        unsafe extern "C" fn(
            *mut EState,
            *mut ResultRelInfo,
            *mut TupleTableSlot,
            *mut TupleTableSlot,
        ) -> *mut TupleTableSlot,
    >,
    /// ExecForeignBatchInsert(estate, rri, slots, planSlots, numSlots) -> **TupleTableSlot -- foreign/fdwapi.h
    pub ExecForeignBatchInsert: Option<
        unsafe extern "C" fn(
            *mut EState,
            *mut ResultRelInfo,
            *mut *mut TupleTableSlot,
            *mut *mut TupleTableSlot,
            *mut c_int,
        ) -> *mut *mut TupleTableSlot,
    >,
    /// BeginForeignInsert(mtstate, rri) -- foreign/fdwapi.h
    pub BeginForeignInsert: Option<
        unsafe extern "C" fn(*mut ModifyTableState, *mut ResultRelInfo),
    >,
    /// EndForeignInsert(estate, rri) -- foreign/fdwapi.h
    pub EndForeignInsert: Option<
        unsafe extern "C" fn(*mut EState, *mut ResultRelInfo),
    >,
    /// GetForeignModifyBatchSize(rri) -> c_int -- foreign/fdwapi.h
    pub GetForeignModifyBatchSize: Option<
        unsafe extern "C" fn(*mut ResultRelInfo) -> c_int,
    >,
    /// ExecForeignUpdate(estate, rri, slot, planSlot) -> *TupleTableSlot -- foreign/fdwapi.h // C home: foreign/fdwapi.h
    pub ExecForeignUpdate: Option<
        unsafe extern "C" fn(
            *mut EState,
            *mut ResultRelInfo,
            *mut TupleTableSlot,
            *mut TupleTableSlot,
        ) -> *mut TupleTableSlot,
    >,
    /// ExecForeignDelete(estate, rri, slot, planSlot) -> *TupleTableSlot -- foreign/fdwapi.h // C home: foreign/fdwapi.h
    pub ExecForeignDelete: Option<
        unsafe extern "C" fn(
            *mut EState,
            *mut ResultRelInfo,
            *mut TupleTableSlot,
            *mut TupleTableSlot,
        ) -> *mut TupleTableSlot,
    >,
    /// EndForeignModify(estate, rri) -- foreign/fdwapi.h // C home: foreign/fdwapi.h
    pub EndForeignModify: Option<
        unsafe extern "C" fn(*mut EState, *mut ResultRelInfo),
    >,
    /// ExplainForeignScan(node, es) -- foreign/fdwapi.h
    pub ExplainForeignScan: Option<
        unsafe extern "C" fn(*mut ForeignScanState, *mut c_void),
    >,
    /// ExplainForeignModify(mtstate, rinfo, fdw_private, subplan_index, es) -- foreign/fdwapi.h
    pub ExplainForeignModify: Option<
        unsafe extern "C" fn(*mut ModifyTableState, *mut ResultRelInfo, *mut crate::nodes::pg_list::List, c_int, *mut c_void),
    >,
    /// ExplainDirectModify(node, es) -- foreign/fdwapi.h
    pub ExplainDirectModify: Option<
        unsafe extern "C" fn(*mut ForeignScanState, *mut c_void),
    >,
}

// TsmRoutine now carries its real access/tsmapi.h vtable layout in
// crate::access::tsmapi; re-exported here for SampleScanState.tsmroutine.
pub use crate::access::tsmapi::TsmRoutine;

/// `TableFuncRoutine` from executor/tablefunc.h.
/// Function-pointer table for table-producing nodes (XMLTABLE, JSON_TABLE).
#[repr(C)]
pub struct TableFuncRoutine {
    pub InitOpaque:     Option<unsafe extern "C" fn(state: *mut TableFuncScanState, natts: c_int)>,
    pub SetDocument:    Option<unsafe extern "C" fn(state: *mut TableFuncScanState, value: crate::postgres::Datum)>,
    pub SetNamespace:   Option<unsafe extern "C" fn(state: *mut TableFuncScanState, name: *const c_char, uri: *const c_char)>,
    pub SetRowFilter:   Option<unsafe extern "C" fn(state: *mut TableFuncScanState, path: *const c_char)>,
    pub SetColumnFilter:Option<unsafe extern "C" fn(state: *mut TableFuncScanState, path: *const c_char, colnum: c_int)>,
    pub FetchRow:       Option<unsafe extern "C" fn(state: *mut TableFuncScanState) -> bool>,
    pub GetValue:       Option<unsafe extern "C" fn(state: *mut TableFuncScanState, colnum: c_int, typid: crate::postgres_ext::Oid, typmod: i32, isnull: *mut bool) -> crate::postgres::Datum>,
    pub DestroyOpaque:  Option<unsafe extern "C" fn(state: *mut TableFuncScanState)>,
}

/// Real definition lives in nodes/extensible.rs; re-export it so
/// CustomScanState.methods carries the full method table.
pub use crate::nodes::extensible::CustomExecMethods;

/// TODO(pg-port): real def `typedef struct CopyMultiInsertBuffer` in
/// commands/copyfrom_internal.h.
#[repr(C)]
pub struct CopyMultiInsertBuffer {
    _opaque: [u8; 0],
}

// ExprEvalStep: real execExpr.h layout lives in executor::execExpr.
pub use crate::executor::execExpr::ExprEvalStep;

/// TODO(pg-port): real def `typedef struct LogicalTapeSet` in
/// utils/logtape.h.
#[repr(C)]
pub struct LogicalTapeSet {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real def `typedef struct dsa_area` in utils/dsa.h.
#[repr(C)]
pub struct dsa_area {
    _opaque: [u8; 0],
}

/// `JitContext` -- JIT compilation context (jit/jit.h).
#[repr(C)]
pub struct JitContext {
    pub flags: c_int,
    pub instr: JitInstrumentation,
}

/// `JitInstrumentation` -- JIT instrumentation (jit/jit.h).
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct JitInstrumentation {
    pub created_functions: usize,
    pub generation_counter: crate::portability::instr_time::instr_time,
    pub deform_counter:     crate::portability::instr_time::instr_time,
    pub inlining_counter:   crate::portability::instr_time::instr_time,
    pub optimization_counter: crate::portability::instr_time::instr_time,
    pub emission_counter:   crate::portability::instr_time::instr_time,
}

/// `SharedJitInstrumentation` -- DSM aggregate JIT stats (jit/jit.h).
#[repr(C)]
pub struct SharedJitInstrumentation {
    pub num_workers: c_int,
    pub jit_instr: [JitInstrumentation; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/// TODO(pg-port): real def `typedef struct WaitEventSet` in storage/waiteventset.h.
#[repr(C)]
pub struct WaitEventSet {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real def `typedef struct ParallelExecutorInfo` in
/// executor/execParallel.h.
#[repr(C)]
pub struct ParallelExecutorInfo {
    _opaque: [u8; 0],
}

/// Real definition lives in executor/tqueue.rs; re-export it.
pub use crate::executor::tqueue::TupleQueueReader;

/// TODO(pg-port): real def `typedef struct PartitionPruneState` in
/// executor/execPartition.h (private).
#[repr(C)]
pub struct PartitionPruneState {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real def `typedef struct PartitionTupleRouting` in
/// executor/execPartition.h.
#[repr(C)]
pub struct PartitionTupleRouting {
    _opaque: [u8; 0],
}

/// `typedef struct TransitionCaptureState` -- commands/trigger.h.
/// The `tcs_*_private` pointers are `struct AfterTriggersTableData *` in C
/// (private to commands/trigger.c); kept as raw `c_void` here to avoid a
/// cross-module type dependency, and cast at the use site.
#[repr(C)]
pub struct TransitionCaptureState {
    pub tcs_delete_old_table: bool,
    pub tcs_update_old_table: bool,
    pub tcs_update_new_table: bool,
    pub tcs_insert_new_table: bool,
    /// tcs_original_insert_tuple -- commands/trigger.h
    pub tcs_original_insert_tuple: *mut TupleTableSlot,
    pub tcs_insert_private: *mut core::ffi::c_void,
    pub tcs_update_private: *mut core::ffi::c_void,
    pub tcs_delete_private: *mut core::ffi::c_void,
}

// ParallelHashJoinState / HashJoinTupleData / HashJoinTableData: real defs live
// in executor/hashjoin.rs (the hashjoin.h port). Re-export to unify the types
// across the executor (avoids duplicate-type-identity conflicts).
pub use crate::executor::hashjoin::{
    HashJoinTableData, HashJoinTupleData, ParallelHashJoinState,
};
pub type HashJoinTuple = *mut HashJoinTupleData;
pub type HashJoinTable = *mut HashJoinTableData;

/// TODO(pg-port): real def `typedef struct MergeJoinClauseData
/// *MergeJoinClause` (private in nodeMergejoin.c).
#[repr(C)]
pub struct MergeJoinClauseData {
    _opaque: [u8; 0],
}
pub type MergeJoinClause = *mut MergeJoinClauseData;

/// TODO(pg-port): real def `typedef struct FunctionScanPerFuncState` (private
/// in nodeFunctionscan.c).
#[repr(C)]
pub struct FunctionScanPerFuncState {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real def `typedef struct WindowObjectData` in
/// windowapi.h (private).
#[repr(C)]
pub struct WindowObjectData {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real def `typedef struct GMReaderTupleBuffer` (private in
/// nodeGatherMerge.c).
#[repr(C)]
pub struct GMReaderTupleBuffer {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real def `typedef struct HashAggSpill` (private in nodeAgg.c).
/// Fields match the private C struct from nodeAgg.c; pointer fields use c_void
/// to avoid pulling in logtape/hyperLogLog here.
#[repr(C)]
pub struct HashAggSpill {
    pub npartitions: core::ffi::c_int,           /* number of partitions */
    pub partitions: *mut core::ffi::c_void,      /* *mut *mut LogicalTape */
    pub ntuples: *mut i64,                       /* int64[] per partition */
    pub mask: u32,
    pub shift: core::ffi::c_int,
    pub hll_card: *mut core::ffi::c_void,        /* *mut hyperLogLogState */
}

/// TODO(pg-port): private hash-table types generated by simplehash.h.
/// `tuplehash_hash` is the table; `tuplehash_iterator` the iterator.
#[repr(C)]
pub struct tuplehash_hash {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct tuplehash_iterator {
    pub inner: crate::lib::simplehash::SimpleHashIterator,
}

/// TODO(pg-port): private hash-table type `memoize_hash` generated by
/// simplehash.h for nodeMemoize.c.
#[repr(C)]
pub struct memoize_hash {
    _opaque: [u8; 0],
}

/// TODO(pg-port): real defs (private in nodeMemoize.c).
#[repr(C)]
pub struct MemoizeEntry {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct MemoizeTuple {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct MemoizeKey {
    _opaque: [u8; 0],
}

// ----------------------------------------------------------------
//		ExprState node
//
// ExprState represents the evaluation state for a whole expression tree.
// It contains instructions (in ->steps) to evaluate the expression.
// ----------------------------------------------------------------

pub type ExprStateEvalFunc =
    Option<unsafe fn(expression: *mut ExprState, econtext: *mut ExprContext, isNull: *mut bool) -> Datum>;

/* Bits in ExprState->flags (see also execExpr.h for private flag bits): */
/* expression is for use with ExecQual() */
pub const EEO_FLAG_IS_QUAL: uint8 = 1 << 0;
/* expression refers to OLD table columns */
pub const EEO_FLAG_HAS_OLD: uint8 = 1 << 1;
/* expression refers to NEW table columns */
pub const EEO_FLAG_HAS_NEW: uint8 = 1 << 2;
/* OLD table row is NULL in RETURNING list */
pub const EEO_FLAG_OLD_IS_NULL: uint8 = 1 << 3;
/* NEW table row is NULL in RETURNING list */
pub const EEO_FLAG_NEW_IS_NULL: uint8 = 1 << 4;

pub const FIELDNO_EXPRSTATE_FLAGS: usize = 1;
pub const FIELDNO_EXPRSTATE_RESNULL: usize = 2;
pub const FIELDNO_EXPRSTATE_RESVALUE: usize = 3;
pub const FIELDNO_EXPRSTATE_RESULTSLOT: usize = 4;
pub const FIELDNO_EXPRSTATE_PARENT: usize = 11;

#[repr(C)]
pub struct ExprState {
    pub r#type: NodeTag,

    // FIELDNO_EXPRSTATE_FLAGS 1
    pub flags: uint8, /* bitmask of EEO_FLAG_* bits, see above */

    /*
     * Storage for result value of a scalar expression, or for individual
     * column results within expressions built by ExecBuildProjectionInfo().
     */
    // FIELDNO_EXPRSTATE_RESNULL 2
    pub resnull: bool,
    // FIELDNO_EXPRSTATE_RESVALUE 3
    pub resvalue: Datum,

    /*
     * If projecting a tuple result, this slot holds the result; else NULL.
     */
    // FIELDNO_EXPRSTATE_RESULTSLOT 4
    pub resultslot: *mut TupleTableSlot,

    /*
     * Instructions to compute expression's return value.
     */
    pub steps: *mut ExprEvalStep,

    /*
     * Function that actually evaluates the expression.  This can be set to
     * different values depending on the complexity of the expression.
     */
    pub evalfunc: ExprStateEvalFunc,

    /* original expression tree, for debugging only */
    pub expr: *mut Expr,

    /* private state for an evalfunc */
    pub evalfunc_private: *mut c_void,

    /*
     * XXX: following fields only needed during "compilation" (ExecInitExpr);
     * could be thrown away afterwards.
     */
    pub steps_len: c_int,   /* number of steps currently */
    pub steps_alloc: c_int, /* allocated length of steps array */

    // FIELDNO_EXPRSTATE_PARENT 11
    pub parent: *mut PlanState, /* parent PlanState node, if any */
    pub ext_params: ParamListInfo, /* for compiling PARAM_EXTERN nodes */

    pub innermost_caseval: *mut Datum,
    pub innermost_casenull: *mut bool,

    pub innermost_domainval: *mut Datum,
    pub innermost_domainnull: *mut bool,

    /*
     * For expression nodes that support soft errors. Should be set to NULL if
     * the caller wants errors to be thrown. Callers that do not want errors
     * thrown should set it to a valid ErrorSaveContext before calling
     * ExecInitExprRec().
     */
    pub escontext: *mut ErrorSaveContext,
}

/* ----------------
 *	  IndexInfo information
 *
 *		this struct holds the information needed to construct new index
 *		entries for a particular index.  Used for both index_build and
 *		retail creation of index entries.
 *
 *		NumIndexAttrs		total number of columns in this index
 *		NumIndexKeyAttrs	number of key columns in index
 *		IndexAttrNumbers	underlying-rel attribute numbers used as keys
 *							(zeroes indicate expressions). It also contains
 * 							info about included columns.
 *		Expressions			expr trees for expression entries, or NIL if none
 *		ExpressionsState	exec state for expressions, or NIL if none
 *		Predicate			partial-index predicate, or NIL if none
 *		PredicateState		exec state for predicate, or NIL if none
 *		ExclusionOps		Per-column exclusion operators, or NULL if none
 *		ExclusionProcs		Underlying function OIDs for ExclusionOps
 *		ExclusionStrats		Opclass strategy numbers for ExclusionOps
 *		UniqueOps			These are like Exclusion*, but for unique indexes
 *		UniqueProcs
 *		UniqueStrats
 *		Unique				is it a unique index?
 *		NullsNotDistinct	is NULLS NOT DISTINCT?
 *		ReadyForInserts		is it valid for inserts?
 *		CheckedUnchanged	IndexUnchanged status determined yet?
 *		IndexUnchanged		aminsert hint, cached for retail inserts
 *		Concurrent			are we doing a concurrent index build?
 *		BrokenHotChain		did we detect any broken HOT chains?
 *		WithoutOverlaps		is it a WITHOUT OVERLAPS index?
 *		Summarizing			is it a summarizing index?
 *		ParallelWorkers		# of workers requested (excludes leader)
 *		Am					Oid of index AM
 *		AmCache				private cache area for index AM
 *		Context				memory context holding this IndexInfo
 *
 * ii_Concurrent, ii_BrokenHotChain, and ii_ParallelWorkers are used only
 * during index build; they're conventionally zeroed otherwise.
 * ----------------
 */
#[repr(C)]
pub struct IndexInfo {
    pub r#type: NodeTag,
    pub ii_NumIndexAttrs: c_int,    /* total number of columns in index */
    pub ii_NumIndexKeyAttrs: c_int, /* number of key columns in index */
    pub ii_IndexAttrNumbers: [AttrNumber; INDEX_MAX_KEYS],
    pub ii_Expressions: *mut List, /* list of Expr */
    pub ii_ExpressionsState: *mut List, /* list of ExprState */
    pub ii_Predicate: *mut List,   /* list of Expr */
    pub ii_PredicateState: *mut ExprState,
    pub ii_ExclusionOps: *mut Oid,    /* array with one entry per column */
    pub ii_ExclusionProcs: *mut Oid,  /* array with one entry per column */
    pub ii_ExclusionStrats: *mut uint16, /* array with one entry per column */
    pub ii_UniqueOps: *mut Oid,    /* array with one entry per column */
    pub ii_UniqueProcs: *mut Oid,  /* array with one entry per column */
    pub ii_UniqueStrats: *mut uint16, /* array with one entry per column */
    pub ii_Unique: bool,
    pub ii_NullsNotDistinct: bool,
    pub ii_ReadyForInserts: bool,
    pub ii_CheckedUnchanged: bool,
    pub ii_IndexUnchanged: bool,
    pub ii_Concurrent: bool,
    pub ii_BrokenHotChain: bool,
    pub ii_Summarizing: bool,
    pub ii_WithoutOverlaps: bool,
    pub ii_ParallelWorkers: c_int,
    pub ii_Am: Oid,
    pub ii_AmCache: *mut c_void,
    pub ii_Context: MemoryContext,
}

/* ----------------
 *	  ExprContext_CB
 *
 *		List of callbacks to be called at ExprContext shutdown.
 * ----------------
 */
pub type ExprContextCallbackFunction = Option<unsafe fn(arg: Datum)>;

#[repr(C)]
pub struct ExprContext_CB {
    pub next: *mut ExprContext_CB,
    pub function: ExprContextCallbackFunction,
    pub arg: Datum,
}

/* ----------------
 *	  ExprContext
 *
 *		This class holds the "current context" information
 *		needed to evaluate expressions for doing tuple qualifications
 *		and tuple projections.  For example, if an expression refers
 *		to an attribute in the current inner tuple then we need to know
 *		what the current inner tuple is and so we look at the expression
 *		context.
 *
 *	There are two memory contexts associated with an ExprContext:
 *	* ecxt_per_query_memory is a query-lifespan context, typically the same
 *	  context the ExprContext node itself is allocated in.  This context
 *	  can be used for purposes such as storing function call cache info.
 *	* ecxt_per_tuple_memory is a short-term context for expression results.
 *	  As the name suggests, it will typically be reset once per tuple,
 *	  before we begin to evaluate expressions for that tuple.  Each
 *	  ExprContext normally has its very own per-tuple memory context.
 *
 *	CurrentMemoryContext should be set to ecxt_per_tuple_memory before
 *	calling ExecEvalExpr() --- see ExecEvalExprSwitchContext().
 * ----------------
 */
pub const FIELDNO_EXPRCONTEXT_SCANTUPLE: usize = 1;
pub const FIELDNO_EXPRCONTEXT_INNERTUPLE: usize = 2;
pub const FIELDNO_EXPRCONTEXT_OUTERTUPLE: usize = 3;
pub const FIELDNO_EXPRCONTEXT_AGGVALUES: usize = 8;
pub const FIELDNO_EXPRCONTEXT_AGGNULLS: usize = 9;
pub const FIELDNO_EXPRCONTEXT_CASEDATUM: usize = 10;
pub const FIELDNO_EXPRCONTEXT_CASENULL: usize = 11;
pub const FIELDNO_EXPRCONTEXT_DOMAINDATUM: usize = 12;
pub const FIELDNO_EXPRCONTEXT_DOMAINNULL: usize = 13;
pub const FIELDNO_EXPRCONTEXT_OLDTUPLE: usize = 14;
pub const FIELDNO_EXPRCONTEXT_NEWTUPLE: usize = 15;

#[repr(C)]
pub struct ExprContext {
    pub r#type: NodeTag,

    /* Tuples that Var nodes in expression may refer to */
    // FIELDNO_EXPRCONTEXT_SCANTUPLE 1
    pub ecxt_scantuple: *mut TupleTableSlot,
    // FIELDNO_EXPRCONTEXT_INNERTUPLE 2
    pub ecxt_innertuple: *mut TupleTableSlot,
    // FIELDNO_EXPRCONTEXT_OUTERTUPLE 3
    pub ecxt_outertuple: *mut TupleTableSlot,

    /* Memory contexts for expression evaluation --- see notes above */
    pub ecxt_per_query_memory: MemoryContext,
    pub ecxt_per_tuple_memory: MemoryContext,

    /* Values to substitute for Param nodes in expression */
    pub ecxt_param_exec_vals: *mut ParamExecData, /* for PARAM_EXEC params */
    pub ecxt_param_list_info: ParamListInfo,      /* for other param types */

    /*
     * Values to substitute for Aggref nodes in the expressions of an Agg
     * node, or for WindowFunc nodes within a WindowAgg node.
     */
    // FIELDNO_EXPRCONTEXT_AGGVALUES 8
    pub ecxt_aggvalues: *mut Datum, /* precomputed values for aggs/windowfuncs */
    // FIELDNO_EXPRCONTEXT_AGGNULLS 9
    pub ecxt_aggnulls: *mut bool, /* null flags for aggs/windowfuncs */

    /* Value to substitute for CaseTestExpr nodes in expression */
    // FIELDNO_EXPRCONTEXT_CASEDATUM 10
    pub caseValue_datum: Datum,
    // FIELDNO_EXPRCONTEXT_CASENULL 11
    pub caseValue_isNull: bool,

    /* Value to substitute for CoerceToDomainValue nodes in expression */
    // FIELDNO_EXPRCONTEXT_DOMAINDATUM 12
    pub domainValue_datum: Datum,
    // FIELDNO_EXPRCONTEXT_DOMAINNULL 13
    pub domainValue_isNull: bool,

    /* Tuples that OLD/NEW Var nodes in RETURNING may refer to */
    // FIELDNO_EXPRCONTEXT_OLDTUPLE 14
    pub ecxt_oldtuple: *mut TupleTableSlot,
    // FIELDNO_EXPRCONTEXT_NEWTUPLE 15
    pub ecxt_newtuple: *mut TupleTableSlot,

    /* Link to containing EState (NULL if a standalone ExprContext) */
    pub ecxt_estate: *mut EState,

    /* Functions to call back when ExprContext is shut down or rescanned */
    pub ecxt_callbacks: *mut ExprContext_CB,
}

/*
 * Set-result status used when evaluating functions potentially returning a
 * set.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ExprDoneCond {
    ExprSingleResult,   /* expression does not return a set */
    ExprMultipleResult, /* this result is an element of a set */
    ExprEndResult,      /* there are no more elements in the set */
}
pub use ExprDoneCond::*;

/*
 * Return modes for functions returning sets.  Note values must be chosen
 * as separate bits so that a bitmask can be formed to indicate supported
 * modes.  SFRM_Materialize_Random and SFRM_Materialize_Preferred are
 * auxiliary flags about SFRM_Materialize mode, rather than separate modes.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SetFunctionReturnMode {
    SFRM_ValuePerCall = 0x01,        /* one value returned per call */
    SFRM_Materialize = 0x02,         /* result set instantiated in Tuplestore */
    SFRM_Materialize_Random = 0x04,  /* Tuplestore needs randomAccess */
    SFRM_Materialize_Preferred = 0x08, /* caller prefers Tuplestore */
}
pub use SetFunctionReturnMode::*;

/*
 * When calling a function that might return a set (multiple rows),
 * a node of this type is passed as fcinfo->resultinfo to allow
 * return status to be passed back.  A function returning set should
 * raise an error if no such resultinfo is provided.
 */
#[repr(C)]
pub struct ReturnSetInfo {
    pub r#type: NodeTag,
    /* values set by caller: */
    pub econtext: *mut ExprContext, /* context function is being called in */
    pub expectedDesc: TupleDesc,    /* tuple descriptor expected by caller */
    pub allowedModes: c_int,        /* bitmask: return modes caller can handle */
    /* result status from function (but pre-initialized by caller): */
    pub returnMode: SetFunctionReturnMode, /* actual return mode */
    pub isDone: ExprDoneCond,       /* status for ValuePerCall mode */
    /* fields filled by function in Materialize return mode: */
    pub setResult: *mut Tuplestorestate, /* holds the complete returned tuple set */
    pub setDesc: TupleDesc,         /* actual descriptor for returned tuples */
}

/* ----------------
 *		ProjectionInfo node information
 *
 *		This is all the information needed to perform projections ---
 *		that is, form new tuples by evaluation of targetlist expressions.
 *		Nodes which need to do projections create one of these.
 *
 *		The target tuple slot is kept in ProjectionInfo->pi_state.resultslot.
 *		ExecProject() evaluates the tlist, forms a tuple, and stores it
 *		in the given slot.  Note that the result will be a "virtual" tuple
 *		unless ExecMaterializeSlot() is then called to force it to be
 *		converted to a physical tuple.  The slot must have a tupledesc
 *		that matches the output of the tlist!
 * ----------------
 */
#[repr(C)]
pub struct ProjectionInfo {
    pub r#type: NodeTag,
    /* instructions to evaluate projection */
    pub pi_state: ExprState,
    /* expression context in which to evaluate expression */
    pub pi_exprContext: *mut ExprContext,
}

/* ----------------
 *	  JunkFilter
 *
 *	  This class is used to store information regarding junk attributes.
 *	  A junk attribute is an attribute in a tuple that is needed only for
 *	  storing intermediate information in the executor, and does not belong
 *	  in emitted tuples.  For example, when we do an UPDATE query,
 *	  the planner adds a "junk" entry to the targetlist so that the tuples
 *	  returned to ExecutePlan() contain an extra attribute: the ctid of
 *	  the tuple to be updated.  This is needed to do the update, but we
 *	  don't want the ctid to be part of the stored new tuple!  So, we
 *	  apply a "junk filter" to remove the junk attributes and form the
 *	  real output tuple.  The junkfilter code also provides routines to
 *	  extract the values of the junk attribute(s) from the input tuple.
 *
 *	  targetList:		the original target list (including junk attributes).
 *	  cleanTupType:		the tuple descriptor for the "clean" tuple (with
 *						junk attributes removed).
 *	  cleanMap:			A map with the correspondence between the non-junk
 *						attribute numbers of the "original" tuple and the
 *						attribute numbers of the "clean" tuple.
 *	  resultSlot:		tuple slot used to hold cleaned tuple.
 * ----------------
 */
#[repr(C)]
pub struct JunkFilter {
    pub r#type: NodeTag,
    pub jf_targetList: *mut List,
    pub jf_cleanTupType: TupleDesc,
    pub jf_cleanMap: *mut AttrNumber,
    pub jf_resultSlot: *mut TupleTableSlot,
}

/*
 * OnConflictSetState
 *
 * Executor state of an ON CONFLICT DO UPDATE operation.
 */
#[repr(C)]
pub struct OnConflictSetState {
    pub r#type: NodeTag,

    pub oc_Existing: *mut TupleTableSlot, /* slot to store existing target tuple in */
    pub oc_ProjSlot: *mut TupleTableSlot, /* CONFLICT ... SET ... projection target */
    pub oc_ProjInfo: *mut ProjectionInfo, /* for ON CONFLICT DO UPDATE SET */
    pub oc_WhereClause: *mut ExprState,   /* state for the WHERE clause */
}

/* ----------------
 *	 MergeActionState information
 *
 *	Executor state for a MERGE action.
 * ----------------
 */
#[repr(C)]
pub struct MergeActionState {
    pub r#type: NodeTag,

    pub mas_action: *mut MergeAction,  /* associated MergeAction node */
    pub mas_proj: *mut ProjectionInfo, /* projection of the action's targetlist for
                                        * this rel */
    pub mas_whenqual: *mut ExprState, /* WHEN [NOT] MATCHED AND conditions */
}

/*
 * ResultRelInfo
 *
 * Whenever we update an existing relation, we have to update indexes on the
 * relation, and perhaps also fire triggers.  ResultRelInfo holds all the
 * information needed about a result relation, including indexes.
 *
 * Normally, a ResultRelInfo refers to a table that is in the query's range
 * table; then ri_RangeTableIndex is the RT index and ri_RelationDesc is
 * just a copy of the relevant es_relations[] entry.  However, in some
 * situations we create ResultRelInfos for relations that are not in the
 * range table, namely for targets of tuple routing in a partitioned table,
 * and when firing triggers in tables other than the target tables (See
 * ExecGetTriggerResultRel).  In these situations, ri_RangeTableIndex is 0
 * and ri_RelationDesc is a separately-opened relcache pointer that needs to
 * be separately closed.
 */
#[repr(C)]
pub struct ResultRelInfo {
    pub r#type: NodeTag,

    /* result relation's range table index, or 0 if not in range table */
    pub ri_RangeTableIndex: Index,

    /* relation descriptor for result relation */
    pub ri_RelationDesc: Relation,

    /* # of indices existing on result relation */
    pub ri_NumIndices: c_int,

    /* array of relation descriptors for indices */
    pub ri_IndexRelationDescs: RelationPtr,

    /* array of key/attr info for indices */
    pub ri_IndexRelationInfo: *mut *mut IndexInfo,

    /*
     * For UPDATE/DELETE/MERGE result relations, the attribute number of the
     * row identity junk attribute in the source plan's output tuples
     */
    pub ri_RowIdAttNo: AttrNumber,

    /* For UPDATE, attnums of generated columns to be computed */
    pub ri_extraUpdatedCols: *mut Bitmapset,
    /* true if the above has been computed */
    pub ri_extraUpdatedCols_valid: bool,

    /* Projection to generate new tuple in an INSERT/UPDATE */
    pub ri_projectNew: *mut ProjectionInfo,
    /* Slot to hold that tuple */
    pub ri_newTupleSlot: *mut TupleTableSlot,
    /* Slot to hold the old tuple being updated */
    pub ri_oldTupleSlot: *mut TupleTableSlot,
    /* Have the projection and the slots above been initialized? */
    pub ri_projectNewInfoValid: bool,

    /* updates do LockTuple() before oldtup read; see README.tuplock */
    pub ri_needLockTagTuple: bool,

    /* triggers to be fired, if any */
    pub ri_TrigDesc: *mut TriggerDesc,

    /* cached lookup info for trigger functions */
    pub ri_TrigFunctions: *mut FmgrInfo,

    /* array of trigger WHEN expr states */
    pub ri_TrigWhenExprs: *mut *mut ExprState,

    /* optional runtime measurements for triggers */
    pub ri_TrigInstrument: *mut Instrumentation,

    /* On-demand created slots for triggers / returning processing */
    pub ri_ReturningSlot: *mut TupleTableSlot, /* for trigger output tuples */
    pub ri_TrigOldSlot: *mut TupleTableSlot,   /* for a trigger's old tuple */
    pub ri_TrigNewSlot: *mut TupleTableSlot,   /* for a trigger's new tuple */
    pub ri_AllNullSlot: *mut TupleTableSlot,   /* for RETURNING OLD/NEW */

    /* FDW callback functions, if foreign table */
    pub ri_FdwRoutine: *mut FdwRoutine,

    /* available to save private state of FDW */
    pub ri_FdwState: *mut c_void,

    /* true when modifying foreign table directly */
    pub ri_usesFdwDirectModify: bool,

    /* batch insert stuff */
    pub ri_NumSlots: c_int,            /* number of slots in the array */
    pub ri_NumSlotsInitialized: c_int, /* number of initialized slots */
    pub ri_BatchSize: c_int,           /* max slots inserted in a single batch */
    pub ri_Slots: *mut *mut TupleTableSlot, /* input tuples for batch insert */
    pub ri_PlanSlots: *mut *mut TupleTableSlot,

    /* list of WithCheckOption's to be checked */
    pub ri_WithCheckOptions: *mut List,

    /* list of WithCheckOption expr states */
    pub ri_WithCheckOptionExprs: *mut List,

    /* array of expr states for checking check constraints */
    pub ri_CheckConstraintExprs: *mut *mut ExprState,

    /*
     * array of expr states for checking not-null constraints on virtual
     * generated columns
     */
    pub ri_GenVirtualNotNullConstraintExprs: *mut *mut ExprState,

    /*
     * Arrays of stored generated columns ExprStates for INSERT/UPDATE/MERGE.
     */
    pub ri_GeneratedExprsI: *mut *mut ExprState,
    pub ri_GeneratedExprsU: *mut *mut ExprState,

    /* number of stored generated columns we need to compute */
    pub ri_NumGeneratedNeededI: c_int,
    pub ri_NumGeneratedNeededU: c_int,

    /* list of RETURNING expressions */
    pub ri_returningList: *mut List,

    /* for computing a RETURNING list */
    pub ri_projectReturning: *mut ProjectionInfo,

    /* list of arbiter indexes to use to check conflicts */
    pub ri_onConflictArbiterIndexes: *mut List,

    /* ON CONFLICT evaluation state */
    pub ri_onConflict: *mut OnConflictSetState,

    /* for MERGE, lists of MergeActionState (one per MergeMatchKind) */
    pub ri_MergeActions: [*mut List; NUM_MERGE_MATCH_KINDS as usize],

    /* for MERGE, expr state for checking the join condition */
    pub ri_MergeJoinCondition: *mut ExprState,

    /* partition check expression state (NULL if not set up yet) */
    pub ri_PartitionCheckExpr: *mut ExprState,

    /*
     * Map to convert child result relation tuples to the format of the table
     * actually mentioned in the query (called "root").  Computed only if
     * needed.  A NULL map value indicates that no conversion is needed, so we
     * must have a separate flag to show if the map has been computed.
     */
    pub ri_ChildToRootMap: *mut TupleConversionMap,
    pub ri_ChildToRootMapValid: bool,

    /*
     * As above, but in the other direction.
     */
    pub ri_RootToChildMap: *mut TupleConversionMap,
    pub ri_RootToChildMapValid: bool,

    /*
     * Other information needed by child result relations
     *
     * ri_RootResultRelInfo gives the target relation mentioned in the query.
     * Used as the root for tuple routing and/or transition capture.
     *
     * ri_PartitionTupleSlot is non-NULL if the relation is a partition to
     * route tuples into and ri_RootToChildMap conversion is needed.
     */
    pub ri_RootResultRelInfo: *mut ResultRelInfo,
    pub ri_PartitionTupleSlot: *mut TupleTableSlot,

    /* for use by copyfrom.c when performing multi-inserts */
    pub ri_CopyMultiInsertBuffer: *mut CopyMultiInsertBuffer,

    /*
     * Used when a leaf partition is involved in a cross-partition update of
     * one of its ancestors; see ExecCrossPartitionUpdateForeignKey().
     */
    pub ri_ancestorResultRels: *mut List,
}

/* ----------------
 *	  AsyncRequest
 *
 * State for an asynchronous tuple request.
 * ----------------
 */
#[repr(C)]
pub struct AsyncRequest {
    pub requestor: *mut PlanState, /* Node that wants a tuple */
    pub requestee: *mut PlanState, /* Node from which a tuple is wanted */
    pub request_index: c_int,      /* Scratch space for requestor */
    pub callback_pending: bool,    /* Callback is needed */
    pub request_complete: bool,    /* Request complete, result valid */
    pub result: *mut TupleTableSlot, /* Result (NULL or an empty slot if no more
                                      * tuples) */
}

/* ----------------
 *	  EState information
 *
 * Working state for an Executor invocation
 * ----------------
 */
#[repr(C)]
pub struct EState {
    pub r#type: NodeTag,

    /* Basic state for all query types: */
    pub es_direction: ScanDirection, /* current scan direction */
    pub es_snapshot: Snapshot,       /* time qual to use */
    pub es_crosscheck_snapshot: Snapshot, /* crosscheck time qual for RI */
    pub es_range_table: *mut List,   /* List of RangeTblEntry */
    pub es_range_table_size: Index,  /* size of the range table arrays */
    pub es_relations: *mut Relation, /* Array of per-range-table-entry Relation
                                      * pointers, or NULL if not yet opened */
    pub es_rowmarks: *mut *mut ExecRowMark, /* Array of per-range-table-entry
                                             * ExecRowMarks, or NULL if none */
    pub es_rteperminfos: *mut List, /* List of RTEPermissionInfo */
    pub es_plannedstmt: *mut PlannedStmt, /* link to top of plan tree */
    pub es_part_prune_infos: *mut List, /* List of PartitionPruneInfo */
    pub es_part_prune_states: *mut List, /* List of PartitionPruneState */
    pub es_part_prune_results: *mut List, /* List of Bitmapset */
    pub es_unpruned_relids: *mut Bitmapset, /* PlannedStmt.unprunableRelids + RT
                                             * indexes of leaf partitions that survive
                                             * initial pruning; see
                                             * ExecDoInitialPruning() */
    pub es_sourceText: *const c_char, /* Source text from QueryDesc */

    pub es_junkFilter: *mut JunkFilter, /* top-level junk filter, if any */

    /* If query can insert/delete tuples, the command ID to mark them with */
    pub es_output_cid: CommandId,

    /* Info about target table(s) for insert/update/delete queries: */
    pub es_result_relations: *mut *mut ResultRelInfo, /* Array of per-range-table-entry
                                                       * ResultRelInfo pointers, or NULL
                                                       * if not a target table */
    pub es_opened_result_relations: *mut List, /* List of non-NULL entries in
                                                * es_result_relations in no
                                                * specific order */

    pub es_partition_directory: PartitionDirectory, /* for PartitionDesc lookup */

    /*
     * The following list contains ResultRelInfos created by the tuple routing
     * code for partitions that aren't found in the es_result_relations array.
     */
    pub es_tuple_routing_result_relations: *mut List,

    /* Stuff used for firing triggers: */
    pub es_trig_target_relations: *mut List, /* trigger-only ResultRelInfos */

    /* Parameter info: */
    pub es_param_list_info: ParamListInfo, /* values of external params */
    pub es_param_exec_vals: *mut ParamExecData, /* values of internal params */

    pub es_queryEnv: *mut QueryEnvironment, /* query environment */

    /* Other working state: */
    pub es_query_cxt: MemoryContext, /* per-query context in which EState lives */

    pub es_tupleTable: *mut List, /* List of TupleTableSlots */

    pub es_processed: uint64, /* # of tuples processed during one
                               * ExecutorRun() call. */
    pub es_total_processed: uint64, /* total # of tuples aggregated across all
                                     * ExecutorRun() calls. */

    pub es_top_eflags: c_int, /* eflags passed to ExecutorStart */
    pub es_instrument: c_int, /* OR of InstrumentOption flags */
    pub es_finished: bool,    /* true when ExecutorFinish is done */

    pub es_exprcontexts: *mut List, /* List of ExprContexts within EState */

    pub es_subplanstates: *mut List, /* List of PlanState for SubPlans */

    pub es_auxmodifytables: *mut List, /* List of secondary ModifyTableStates */

    /*
     * this ExprContext is for per-output-tuple operations, such as constraint
     * checks and index-value computations.  It will be reset for each output
     * tuple.  Note that it will be created only if needed.
     */
    pub es_per_tuple_exprcontext: *mut ExprContext,

    /*
     * If not NULL, this is an EPQState's EState. This is a field in EState
     * both to allow EvalPlanQual aware executor nodes to detect that they
     * need to perform EPQ related work, and to provide necessary information
     * to do so.
     */
    pub es_epq_active: *mut EPQState,

    pub es_use_parallel_mode: bool, /* can we use parallel workers? */

    pub es_parallel_workers_to_launch: c_int, /* number of workers to
                                               * launch. */
    pub es_parallel_workers_launched: c_int, /* number of workers actually
                                              * launched. */

    /* The per-query shared memory area to use for parallel execution. */
    pub es_query_dsa: *mut dsa_area,

    /*
     * JIT information. es_jit_flags indicates whether JIT should be performed
     * and with which options.  es_jit is created on-demand when JITing is
     * performed.
     *
     * es_jit_worker_instr is the combined, on demand allocated,
     * instrumentation from all workers. The leader's instrumentation is kept
     * separate, and is combined on demand by ExplainPrintJITSummary().
     */
    pub es_jit_flags: c_int,
    pub es_jit: *mut JitContext,
    pub es_jit_worker_instr: *mut JitInstrumentation,

    /*
     * Lists of ResultRelInfos for foreign tables on which batch-inserts are
     * to be executed and owning ModifyTableStates, stored in the same order.
     */
    pub es_insert_pending_result_relations: *mut List,
    pub es_insert_pending_modifytables: *mut List,
}

/*
 * ExecRowMark -
 *	   runtime representation of FOR [KEY] UPDATE/SHARE clauses
 *
 * When doing UPDATE/DELETE/MERGE/SELECT FOR [KEY] UPDATE/SHARE, we will have
 * an ExecRowMark for each non-target relation in the query (except inheritance
 * parent RTEs, which can be ignored at runtime).  Virtual relations such as
 * subqueries-in-FROM will have an ExecRowMark with relation == NULL.  See
 * PlanRowMark for details about most of the fields.  In addition to fields
 * directly derived from PlanRowMark, we store an activity flag (to denote
 * inactive children of inheritance trees), curCtid, which is used by the
 * WHERE CURRENT OF code, and ermExtra, which is available for use by the plan
 * node that sources the relation (e.g., for a foreign table the FDW can use
 * ermExtra to hold information).
 *
 * EState->es_rowmarks is an array of these structs, indexed by RT index,
 * with NULLs for irrelevant RT indexes.  es_rowmarks itself is NULL if
 * there are no rowmarks.
 */
#[repr(C)]
pub struct ExecRowMark {
    pub relation: Relation,    /* opened and suitably locked relation */
    pub relid: Oid,            /* its OID (or InvalidOid, if subquery) */
    pub rti: Index,            /* its range table index */
    pub prti: Index,           /* parent range table index, if child */
    pub rowmarkId: Index,      /* unique identifier for resjunk columns */
    pub markType: RowMarkType, /* see enum in nodes/plannodes.h */
    pub strength: LockClauseStrength, /* LockingClause's strength, or LCS_NONE */
    pub waitPolicy: LockWaitPolicy, /* NOWAIT and SKIP LOCKED */
    pub ermActive: bool,       /* is this mark relevant for current tuple? */
    pub curCtid: ItemPointerData, /* ctid of currently locked tuple, if any */
    pub ermExtra: *mut c_void, /* available for use by relation source node */
}

/*
 * ExecAuxRowMark -
 *	   additional runtime representation of FOR [KEY] UPDATE/SHARE clauses
 *
 * Each LockRows and ModifyTable node keeps a list of the rowmarks it needs to
 * deal with.  In addition to a pointer to the related entry in es_rowmarks,
 * this struct carries the column number(s) of the resjunk columns associated
 * with the rowmark (see comments for PlanRowMark for more detail).
 */
#[repr(C)]
pub struct ExecAuxRowMark {
    pub rowmark: *mut ExecRowMark, /* related entry in es_rowmarks */
    pub ctidAttNo: AttrNumber,     /* resno of ctid junk attribute, if any */
    pub toidAttNo: AttrNumber,     /* resno of tableoid junk attribute, if any */
    pub wholeAttNo: AttrNumber,    /* resno of whole-row junk attribute, if any */
}

/* ----------------------------------------------------------------
 *				 Tuple Hash Tables
 *
 * All-in-memory tuple hash tables are used for a number of purposes.
 *
 * Note: tab_hash_expr is for hashing the key datatype(s) stored in the table,
 * and tab_eq_func is a non-cross-type ExprState for equality checks on those
 * types.  Normally these are the only ExprStates used, but
 * FindTupleHashEntry() supports searching a hashtable using cross-data-type
 * hashing.  For that, the caller must supply an ExprState to hash the LHS
 * datatype as well as the cross-type equality ExprState to use.  in_hash_expr
 * and cur_eq_func are set to point to the caller's hash and equality
 * ExprStates while doing such a search.  During LookupTupleHashEntry(), they
 * point to tab_hash_expr and tab_eq_func respectively.
 * ----------------------------------------------------------------
 */
// Unified to execGrouping (canonical home); were field-identical duplicate stubs.
pub use crate::executor::execGrouping::{
    TupleHashEntry, TupleHashEntryData, TupleHashTable, TupleHashTableData,
};

pub type TupleHashIterator = tuplehash_iterator;

/* ----------------------------------------------------------------
 *				 Expression State Nodes
 *
 * Formerly, there was a separate executor expression state node corresponding
 * to each node in a planned expression tree.  That's no longer the case; for
 * common expression node types, all the execution info is embedded into
 * step(s) in a single ExprState node.  But we still have a few executor state
 * node types for selected expression node types, mostly those in which info
 * has to be shared with other parts of the execution state tree.
 * ----------------------------------------------------------------
 */

/* ----------------
 *		WindowFuncExprState node
 * ----------------
 */
#[repr(C)]
pub struct WindowFuncExprState {
    pub r#type: NodeTag,
    pub wfunc: *mut WindowFunc,  /* expression plan node */
    pub args: *mut List,         /* ExprStates for argument expressions */
    pub aggfilter: *mut ExprState, /* FILTER expression */
    pub wfuncno: c_int,          /* ID number for wfunc within its plan node */
}

/* ----------------
 *		SetExprState node
 *
 * State for evaluating a potentially set-returning expression (like FuncExpr
 * or OpExpr).  In some cases, like some of the expressions in ROWS FROM(...)
 * the expression might not be a SRF, but nonetheless it uses the same
 * machinery as SRFs; it will be treated as a SRF returning a single row.
 * ----------------
 */
#[repr(C)]
pub struct SetExprState {
    pub r#type: NodeTag,
    pub expr: *mut Expr, /* expression plan node */
    pub args: *mut List, /* ExprStates for argument expressions */

    /*
     * In ROWS FROM, functions can be inlined, removing the FuncExpr normally
     * inside.  In such a case this is the compiled expression (which cannot
     * return a set), which'll be evaluated using regular ExecEvalExpr().
     */
    pub elidedFuncState: *mut ExprState,

    /*
     * Function manager's lookup info for the target function.  If func.fn_oid
     * is InvalidOid, we haven't initialized it yet (nor any of the following
     * fields, except funcReturnsSet).
     */
    pub func: FmgrInfo,

    /*
     * For a set-returning function (SRF) that returns a tuplestore, we keep
     * the tuplestore here and dole out the result rows one at a time. The
     * slot holds the row currently being returned.
     */
    pub funcResultStore: *mut Tuplestorestate,
    pub funcResultSlot: *mut TupleTableSlot,

    /*
     * In some cases we need to compute a tuple descriptor for the function's
     * output.  If so, it's stored here.
     */
    pub funcResultDesc: TupleDesc,
    pub funcReturnsTuple: bool, /* valid when funcResultDesc isn't NULL */

    /*
     * Remember whether the function is declared to return a set.  This is set
     * by ExecInitExpr, and is valid even before the FmgrInfo is set up.
     */
    pub funcReturnsSet: bool,

    /*
     * setArgsValid is true when we are evaluating a set-returning function
     * that uses value-per-call mode and we are in the middle of a call
     * series; we want to pass the same argument values to the function again
     * (and again, until it returns ExprEndResult).  This indicates that
     * fcinfo_data already contains valid argument data.
     */
    pub setArgsValid: bool,

    /*
     * Flag to remember whether we have registered a shutdown callback for
     * this SetExprState.  We do so only if funcResultStore or setArgsValid
     * has been set at least once (since all the callback is for is to release
     * the tuplestore or clear setArgsValid).
     */
    pub shutdown_reg: bool, /* a shutdown callback is registered */

    /*
     * Call parameter structure for the function.  This has been initialized
     * (by InitFunctionCallInfoData) if func.fn_oid is valid.  It also saves
     * argument values between calls, when setArgsValid is true.
     */
    pub fcinfo: FunctionCallInfo,
}

/* ----------------
 *		SubPlanState node
 * ----------------
 */
#[repr(C)]
pub struct SubPlanState {
    pub r#type: NodeTag,
    pub subplan: *mut SubPlan,    /* expression plan node */
    pub planstate: *mut PlanState, /* subselect plan's state tree */
    pub parent: *mut PlanState,   /* parent plan node's state tree */
    pub testexpr: *mut ExprState, /* state of combining expression */
    pub curTuple: HeapTuple,      /* copy of most recent tuple from subplan */
    pub curArray: Datum,          /* most recent array from ARRAY() subplan */
    /* these are used when hashing the subselect's output: */
    pub descRight: TupleDesc,     /* subselect desc after projection */
    pub projLeft: *mut ProjectionInfo, /* for projecting lefthand exprs */
    pub projRight: *mut ProjectionInfo, /* for projecting subselect output */
    pub hashtable: TupleHashTable, /* hash table for no-nulls subselect rows */
    pub hashnulls: TupleHashTable, /* hash table for rows with null(s) */
    pub havehashrows: bool,       /* true if hashtable is not empty */
    pub havenullrows: bool,       /* true if hashnulls is not empty */
    pub hashtablecxt: MemoryContext, /* memory context containing hash tables */
    pub hashtempcxt: MemoryContext, /* temp memory context for hash tables */
    pub innerecontext: *mut ExprContext, /* econtext for computing inner tuples */
    pub numCols: c_int,           /* number of columns being hashed */
    /* each of the remaining fields is an array of length numCols: */
    pub keyColIdx: *mut AttrNumber, /* control data for hash tables */
    pub tab_eq_funcoids: *mut Oid, /* equality func oids for table
                                    * datatype(s) */
    pub tab_collations: *mut Oid, /* collations for hash and comparison */
    pub tab_hash_funcs: *mut FmgrInfo, /* hash functions for table datatype(s) */
    pub lhs_hash_expr: *mut ExprState, /* hash expr for lefthand datatype(s) */
    pub cur_eq_funcs: *mut FmgrInfo, /* equality functions for LHS vs. table */
    pub cur_eq_comp: *mut ExprState, /* equality comparator for LHS vs. table */
}

/*
 * DomainConstraintState - one item to check during CoerceToDomain
 *
 * Note: we consider this to be part of an ExprState tree, so we give it
 * a name following the xxxState convention.  But there's no directly
 * associated plan-tree node.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DomainConstraintType {
    DOM_CONSTRAINT_NOTNULL,
    DOM_CONSTRAINT_CHECK,
}
pub use DomainConstraintType::*;

#[repr(C)]
pub struct DomainConstraintState {
    pub r#type: NodeTag,
    pub constrainttype: DomainConstraintType, /* constraint type */
    pub name: *mut c_char,        /* name of constraint (for error msgs) */
    pub check_expr: *mut Expr,    /* for CHECK, a boolean expression */
    pub check_exprstate: *mut ExprState, /* check_expr's eval state, or NULL */
}

/*
 * State for JsonExpr evaluation, too big to inline.
 *
 * This contains the information going into and coming out of the
 * EEOP_JSONEXPR_PATH eval step.
 */
#[repr(C)]
pub struct JsonExprState {
    /* original expression node */
    pub jsexpr: *mut JsonExpr,

    /* value/isnull for formatted_expr */
    pub formatted_expr: NullableDatum,

    /* value/isnull for pathspec */
    pub pathspec: NullableDatum,

    /* JsonPathVariable entries for passing_values */
    pub args: *mut List,

    /*
     * Output variables that drive the EEOP_JUMP_IF_NOT_TRUE steps that are
     * added for ON ERROR and ON EMPTY expressions, if any.
     *
     * Reset for each evaluation of EEOP_JSONEXPR_PATH.
     */

    /* Set to true if jsonpath evaluation cause an error.  */
    pub error: NullableDatum,

    /* Set to true if the jsonpath evaluation returned 0 items. */
    pub empty: NullableDatum,

    /*
     * Addresses of steps that implement the non-ERROR variant of ON EMPTY and
     * ON ERROR behaviors, respectively.
     */
    pub jump_empty: c_int,
    pub jump_error: c_int,

    /*
     * Address of the step to coerce the result value of jsonpath evaluation
     * to the RETURNING type.  -1 if no coercion if JsonExpr.use_io_coercion
     * is true.
     */
    pub jump_eval_coercion: c_int,

    /*
     * Address to jump to when skipping all the steps after performing
     * ExecEvalJsonExprPath() so as to return whatever the JsonPath* function
     * returned as is, that is, in the cases where there's no error and no
     * coercion is necessary.
     */
    pub jump_end: c_int,

    /*
     * RETURNING type input function invocation info when
     * JsonExpr.use_io_coercion is true.
     */
    pub input_fcinfo: FunctionCallInfo,

    /*
     * For error-safe evaluation of coercions.  When the ON ERROR behavior is
     * not ERROR, a pointer to this is passed to ExecInitExprRec() when
     * initializing the coercion expressions or to ExecInitJsonCoercion().
     *
     * Reset for each evaluation of EEOP_JSONEXPR_PATH.
     */
    pub escontext: ErrorSaveContext,
}

/* ----------------------------------------------------------------
 *				 Executor State Trees
 *
 * An executing query has a PlanState tree paralleling the Plan tree
 * that describes the plan.
 * ----------------------------------------------------------------
 */

/* ----------------
 *	 ExecProcNodeMtd
 *
 * This is the method called by ExecProcNode to return the next tuple
 * from an executor node.  It returns NULL, or an empty TupleTableSlot,
 * if no more tuples are available.
 * ----------------
 */
pub type ExecProcNodeMtd = Option<unsafe fn(pstate: *mut PlanState) -> *mut TupleTableSlot>;

/* ----------------
 *		PlanState node
 *
 * We never actually instantiate any PlanState nodes; this is just the common
 * abstract superclass for all PlanState-type nodes.
 * ----------------
 */
#[repr(C)]
pub struct PlanState {
    // pg_node_attr(abstract)
    pub r#type: NodeTag,

    pub plan: *mut Plan, /* associated Plan node */

    pub state: *mut EState, /* at execution time, states of individual
                             * nodes point to one EState for the whole
                             * top-level plan */

    pub ExecProcNode: ExecProcNodeMtd, /* function to return next tuple */
    pub ExecProcNodeReal: ExecProcNodeMtd, /* actual function, if above is a
                                            * wrapper */

    pub instrument: *mut Instrumentation, /* Optional runtime stats for this node */
    pub worker_instrument: *mut WorkerInstrumentation, /* per-worker instrumentation */

    /* Per-worker JIT instrumentation */
    pub worker_jit_instrument: *mut SharedJitInstrumentation,

    /*
     * Common structural data for all Plan types.  These links to subsidiary
     * state trees parallel links in the associated plan tree (except for the
     * subPlan list, which does not exist in the plan tree).
     */
    pub qual: *mut ExprState,      /* boolean qual condition */
    pub lefttree: *mut PlanState,  /* input plan tree(s) */
    pub righttree: *mut PlanState,

    pub initPlan: *mut List, /* Init SubPlanState nodes (un-correlated expr
                              * subselects) */
    pub subPlan: *mut List,  /* SubPlanState nodes in my expressions */

    /*
     * State for management of parameter-change-driven rescanning
     */
    pub chgParam: *mut Bitmapset, /* set of IDs of changed Params */

    /*
     * Other run-time state needed by most if not all node types.
     */
    pub ps_ResultTupleDesc: TupleDesc, /* node's return type */
    pub ps_ResultTupleSlot: *mut TupleTableSlot, /* slot for my result tuples */
    pub ps_ExprContext: *mut ExprContext, /* node's expression-evaluation context */
    pub ps_ProjInfo: *mut ProjectionInfo, /* info for doing tuple projection */

    pub async_capable: bool, /* true if node is async-capable */

    /*
     * Scanslot's descriptor if known. This is a bit of a hack, but otherwise
     * it's hard for expression compilation to optimize based on the
     * descriptor, without encoding knowledge about all executor nodes.
     */
    pub scandesc: TupleDesc,

    /*
     * Define the slot types for inner, outer and scanslots for expression
     * contexts with this state as a parent.  If *opsset is set, then
     * *opsfixed indicates whether *ops is guaranteed to be the type of slot
     * used. That means that every slot in the corresponding
     * ExprContext.ecxt_*tuple will point to a slot of that type, while
     * evaluating the expression.  If *opsfixed is false, but *ops is set,
     * that indicates the most likely type of slot.
     *
     * The scan* fields are set by ExecInitScanTupleSlot(). If that's not
     * called, nodes can initialize the fields themselves.
     *
     * If outer/inneropsset is false, the information is inferred on-demand
     * using ExecGetResultSlotOps() on ->righttree/lefttree, using the
     * corresponding node's resultops* fields.
     *
     * The result* fields are automatically set when ExecInitResultSlot is
     * used (be it directly or when the slot is created by
     * ExecAssignScanProjectionInfo() /
     * ExecConditionalAssignProjectionInfo()).  If no projection is necessary
     * ExecConditionalAssignProjectionInfo() defaults those fields to the scan
     * operations.
     */
    pub scanops: *const TupleTableSlotOps,
    pub outerops: *const TupleTableSlotOps,
    pub innerops: *const TupleTableSlotOps,
    pub resultops: *const TupleTableSlotOps,
    pub scanopsfixed: bool,
    pub outeropsfixed: bool,
    pub inneropsfixed: bool,
    pub resultopsfixed: bool,
    pub scanopsset: bool,
    pub outeropsset: bool,
    pub inneropsset: bool,
    pub resultopsset: bool,
}

/* ----------------
 *	these are defined to avoid confusion problems with "left"
 *	and "right" and "inner" and "outer".  The convention is that
 *	the "left" plan is the "outer" plan and the "right" plan is
 *	the inner plan, but these make the code more readable.
 * ----------------
 */
/// `innerPlanState(node)`: the inner (right) input PlanState.
///
/// # Safety
/// `node` must point to a value whose layout starts with a `PlanState`.
#[inline]
pub unsafe fn innerPlanState(node: *mut PlanState) -> *mut PlanState {
    (*node).righttree
}
/// `outerPlanState(node)`: the outer (left) input PlanState.
///
/// # Safety
/// `node` must point to a value whose layout starts with a `PlanState`.
#[inline]
pub unsafe fn outerPlanState(node: *mut PlanState) -> *mut PlanState {
    (*node).lefttree
}

/* Macros for inline access to certain instrumentation counters */
/* InstrCountTuples2 / InstrCountFiltered1 / InstrCountFiltered2 increment
 * Instrumentation counters; they require the (not-yet-translated)
 * Instrumentation layout and are deferred until executor/instrument lands.
 * TODO(pg-port): executor/instrument.h -- implement once Instrumentation has
 * real fields (ntuples2, nfiltered1, nfiltered2). */

/*
 * EPQState is state for executing an EvalPlanQual recheck on a candidate
 * tuples e.g. in ModifyTable or LockRows.
 *
 * To execute EPQ a separate EState is created (stored in ->recheckestate),
 * which shares some resources, like the rangetable, with the main query's
 * EState (stored in ->parentestate). The (sub-)tree of the plan that needs to
 * be rechecked (in ->plan), is separately initialized (into
 * ->recheckplanstate), but shares plan nodes with the corresponding nodes in
 * the main query. The scan nodes in that separate executor tree are changed
 * to return only the current tuple of interest for the respective
 * table. Those tuples are either provided by the caller (using
 * EvalPlanQualSlot), and/or found using the rowmark mechanism (non-locking
 * rowmarks by the EPQ machinery itself, locking ones by the caller).
 *
 * While the plan to be checked may be changed using EvalPlanQualSetPlan(),
 * all such plans need to share the same EState.
 */
#[repr(C)]
pub struct EPQState {
    /* These are initialized by EvalPlanQualInit() and do not change later: */
    pub parentestate: *mut EState, /* main query's EState */
    pub epqParam: c_int,           /* ID of Param to force scan node re-eval */
    pub resultRelations: *mut List, /* integer list of RT indexes, or NIL */

    /*
     * relsubs_slot[scanrelid - 1] holds the EPQ test tuple to be returned by
     * the scan node for the scanrelid'th RT index, in place of performing an
     * actual table scan.  Callers should use EvalPlanQualSlot() to fetch
     * these slots.
     */
    pub tuple_table: *mut List, /* tuple table for relsubs_slot */
    pub relsubs_slot: *mut *mut TupleTableSlot,

    /*
     * Initialized by EvalPlanQualInit(), may be changed later with
     * EvalPlanQualSetPlan():
     */
    pub plan: *mut Plan,     /* plan tree to be executed */
    pub arowMarks: *mut List, /* ExecAuxRowMarks (non-locking only) */

    /*
     * The original output tuple to be rechecked.  Set by
     * EvalPlanQualSetSlot(), before EvalPlanQualNext() or EvalPlanQual() may
     * be called.
     */
    pub origslot: *mut TupleTableSlot,

    /* Initialized or reset by EvalPlanQualBegin(): */
    pub recheckestate: *mut EState, /* EState for EPQ execution, see above */

    /*
     * Rowmarks that can be fetched on-demand using
     * EvalPlanQualFetchRowMark(), indexed by scanrelid - 1. Only non-locking
     * rowmarks.
     */
    pub relsubs_rowmark: *mut *mut ExecAuxRowMark,

    /*
     * relsubs_done[scanrelid - 1] is true if there is no EPQ tuple for this
     * target relation or it has already been fetched in the current scan of
     * this target relation within the current EvalPlanQual test.
     */
    pub relsubs_done: *mut bool,

    /*
     * relsubs_blocked[scanrelid - 1] is true if there is no EPQ tuple for
     * this target relation during the current EvalPlanQual test.  We keep
     * these flags set for all relids listed in resultRelations, but
     * transiently clear the one for the relation whose tuple is actually
     * passed to EvalPlanQual().
     */
    pub relsubs_blocked: *mut bool,

    pub recheckplanstate: *mut PlanState, /* EPQ specific exec nodes, for ->plan */
}

/* ----------------
 *	 ResultState information
 * ----------------
 */
#[repr(C)]
pub struct ResultState {
    pub ps: PlanState, /* its first field is NodeTag */
    pub resconstantqual: *mut ExprState,
    pub rs_done: bool,      /* are we done? */
    pub rs_checkqual: bool, /* do we need to check the qual? */
}

/* ----------------
 *	 ProjectSetState information
 *
 * Note: at least one of the "elems" will be a SetExprState; the rest are
 * regular ExprStates.
 * ----------------
 */
#[repr(C)]
pub struct ProjectSetState {
    pub ps: PlanState,           /* its first field is NodeTag */
    pub elems: *mut *mut Node,   /* array of expression states */
    pub elemdone: *mut ExprDoneCond, /* array of per-SRF is-done states */
    pub nelems: c_int,           /* length of elemdone[] array */
    pub pending_srf_tuples: bool, /* still evaluating srfs in tlist? */
    pub argcontext: MemoryContext, /* context for SRF arguments */
}

/* flags for mt_merge_subcommands */
pub const MERGE_INSERT: c_int = 0x01;
pub const MERGE_UPDATE: c_int = 0x02;
pub const MERGE_DELETE: c_int = 0x04;

/* ----------------
 *	 ModifyTableState information
 * ----------------
 */
#[repr(C)]
pub struct ModifyTableState {
    pub ps: PlanState,       /* its first field is NodeTag */
    pub operation: CmdType,  /* INSERT, UPDATE, DELETE, or MERGE */
    pub canSetTag: bool,     /* do we set the command tag/es_processed? */
    pub mt_done: bool,       /* are we done? */
    pub mt_nrels: c_int,     /* number of entries in resultRelInfo[] */
    pub resultRelInfo: *mut ResultRelInfo, /* info about target relation(s) */

    /*
     * Target relation mentioned in the original statement, used to fire
     * statement-level triggers and as the root for tuple routing.  (This
     * might point to one of the resultRelInfo[] entries, but it can also be a
     * distinct struct.)
     */
    pub rootResultRelInfo: *mut ResultRelInfo,

    pub mt_epqstate: EPQState, /* for evaluating EvalPlanQual rechecks */
    pub fireBSTriggers: bool,  /* do we need to fire stmt triggers? */

    /*
     * These fields are used for inherited UPDATE and DELETE, to track which
     * target relation a given tuple is from.  If there are a lot of target
     * relations, we use a hash table to translate table OIDs to
     * resultRelInfo[] indexes; otherwise mt_resultOidHash is NULL.
     */
    pub mt_resultOidAttno: c_int, /* resno of "tableoid" junk attr */
    pub mt_lastResultOid: Oid,    /* last-seen value of tableoid */
    pub mt_lastResultIndex: c_int, /* corresponding index in resultRelInfo[] */
    pub mt_resultOidHash: *mut HTAB, /* optional hash table to speed lookups */

    /*
     * Slot for storing tuples in the root partitioned table's rowtype during
     * an UPDATE of a partitioned table.
     */
    pub mt_root_tuple_slot: *mut TupleTableSlot,

    /* Tuple-routing support info */
    pub mt_partition_tuple_routing: *mut PartitionTupleRouting,

    /* controls transition table population for specified operation */
    pub mt_transition_capture: *mut TransitionCaptureState,

    /* controls transition table population for INSERT...ON CONFLICT UPDATE */
    pub mt_oc_transition_capture: *mut TransitionCaptureState,

    /* Flags showing which subcommands are present INS/UPD/DEL/DO NOTHING */
    pub mt_merge_subcommands: c_int,

    /* For MERGE, the action currently being executed */
    pub mt_merge_action: *mut MergeActionState,

    /*
     * For MERGE, if there is a pending NOT MATCHED [BY TARGET] action to be
     * performed, this will be the last tuple read from the subplan; otherwise
     * it will be NULL --- see the comments in ExecMerge().
     */
    pub mt_merge_pending_not_matched: *mut TupleTableSlot,

    /* tuple counters for MERGE */
    pub mt_merge_inserted: f64,
    pub mt_merge_updated: f64,
    pub mt_merge_deleted: f64,

    /*
     * Lists of valid updateColnosLists, mergeActionLists, and
     * mergeJoinConditions.  These contain only entries for unpruned
     * relations, filtered from the corresponding lists in ModifyTable.
     */
    pub mt_updateColnosLists: *mut List,
    pub mt_mergeActionLists: *mut List,
    pub mt_mergeJoinConditions: *mut List,
}

/* ----------------
 *	 AppendState information
 *
 *		nplans				how many plans are in the array
 *		whichplan			which synchronous plan is being executed (0 .. n-1)
 *							or a special negative value. See nodeAppend.c.
 *		prune_state			details required to allow partitions to be
 *							eliminated from the scan, or NULL if not possible.
 *		valid_subplans		for runtime pruning, valid synchronous appendplans
 *							indexes to scan.
 * ----------------
 */
pub type ParallelAppendState = ParallelAppendStateData;
/// TODO(pg-port): real def `struct ParallelAppendState` (private in
/// nodeAppend.c).
#[repr(C)]
pub struct ParallelAppendStateData {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct AppendState {
    pub ps: PlanState,            /* its first field is NodeTag */
    pub appendplans: *mut *mut PlanState, /* array of PlanStates for my inputs */
    pub as_nplans: c_int,
    pub as_whichplan: c_int,
    pub as_begun: bool,           /* false means need to initialize */
    pub as_asyncplans: *mut Bitmapset, /* asynchronous plans indexes */
    pub as_nasyncplans: c_int,    /* # of asynchronous plans */
    pub as_asyncrequests: *mut *mut AsyncRequest, /* array of AsyncRequests */
    pub as_asyncresults: *mut *mut TupleTableSlot, /* unreturned results of async plans */
    pub as_nasyncresults: c_int,  /* # of valid entries in as_asyncresults */
    pub as_syncdone: bool,        /* true if all synchronous plans done in
                                   * asynchronous mode, else false */
    pub as_nasyncremain: c_int,   /* # of remaining asynchronous plans */
    pub as_needrequest: *mut Bitmapset, /* asynchronous plans needing a new request */
    pub as_eventset: *mut WaitEventSet, /* WaitEventSet used to configure file
                                         * descriptor wait events */
    pub as_first_partial_plan: c_int, /* Index of 'appendplans' containing
                                       * the first partial plan */
    pub as_pstate: *mut ParallelAppendState, /* parallel coordination info */
    pub pstate_len: Size,         /* size of parallel coordination info */
    pub as_prune_state: *mut PartitionPruneState,
    pub as_valid_subplans_identified: bool, /* is as_valid_subplans valid? */
    pub as_valid_subplans: *mut Bitmapset,
    pub as_valid_asyncplans: *mut Bitmapset, /* valid asynchronous plans indexes */
    pub choose_next_subplan: Option<unsafe fn(*mut AppendState) -> bool>,
}

/* ----------------
 *	 MergeAppendState information
 *
 *		nplans			how many plans are in the array
 *		nkeys			number of sort key columns
 *		sortkeys		sort keys in SortSupport representation
 *		slots			current output tuple of each subplan
 *		heap			heap of active tuples
 *		initialized		true if we have fetched first tuple from each subplan
 *		prune_state		details required to allow partitions to be
 *						eliminated from the scan, or NULL if not possible.
 *		valid_subplans	for runtime pruning, valid mergeplans indexes to
 *						scan.
 * ----------------
 */
#[repr(C)]
pub struct MergeAppendState {
    pub ps: PlanState,           /* its first field is NodeTag */
    pub mergeplans: *mut *mut PlanState, /* array of PlanStates for my inputs */
    pub ms_nplans: c_int,
    pub ms_nkeys: c_int,
    pub ms_sortkeys: SortSupport, /* array of length ms_nkeys */
    pub ms_slots: *mut *mut TupleTableSlot, /* array of length ms_nplans */
    pub ms_heap: *mut binaryheap, /* binary heap of slot indices */
    pub ms_initialized: bool,    /* are subplans started? */
    pub ms_prune_state: *mut PartitionPruneState,
    pub ms_valid_subplans: *mut Bitmapset,
}

/* ----------------
 *	 RecursiveUnionState information
 *
 *		RecursiveUnionState is used for performing a recursive union.
 *
 *		recursing			T when we're done scanning the non-recursive term
 *		intermediate_empty	T if intermediate_table is currently empty
 *		working_table		working table (to be scanned by recursive term)
 *		intermediate_table	current recursive output (next generation of WT)
 * ----------------
 */
#[repr(C)]
pub struct RecursiveUnionState {
    pub ps: PlanState, /* its first field is NodeTag */
    pub recursing: bool,
    pub intermediate_empty: bool,
    pub working_table: *mut Tuplestorestate,
    pub intermediate_table: *mut Tuplestorestate,
    /* Remaining fields are unused in UNION ALL case */
    pub eqfuncoids: *mut Oid,    /* per-grouping-field equality fns */
    pub hashfunctions: *mut FmgrInfo, /* per-grouping-field hash fns */
    pub tempContext: MemoryContext, /* short-term context for comparisons */
    pub hashtable: TupleHashTable, /* hash table for tuples already seen */
    pub tableContext: MemoryContext, /* memory context containing hash table */
}

/* ----------------
 *	 BitmapAndState information
 * ----------------
 */
#[repr(C)]
pub struct BitmapAndState {
    pub ps: PlanState,           /* its first field is NodeTag */
    pub bitmapplans: *mut *mut PlanState, /* array of PlanStates for my inputs */
    pub nplans: c_int,           /* number of input plans */
}

/* ----------------
 *	 BitmapOrState information
 * ----------------
 */
#[repr(C)]
pub struct BitmapOrState {
    pub ps: PlanState,           /* its first field is NodeTag */
    pub bitmapplans: *mut *mut PlanState, /* array of PlanStates for my inputs */
    pub nplans: c_int,           /* number of input plans */
}

/* ----------------------------------------------------------------
 *				 Scan State Information
 * ----------------------------------------------------------------
 */

/* ----------------
 *	 ScanState information
 *
 *		ScanState extends PlanState for node types that represent
 *		scans of an underlying relation.  It can also be used for nodes
 *		that scan the output of an underlying plan node --- in that case,
 *		only ScanTupleSlot is actually useful, and it refers to the tuple
 *		retrieved from the subplan.
 *
 *		currentRelation    relation being scanned (NULL if none)
 *		currentScanDesc    current scan descriptor for scan (NULL if none)
 *		ScanTupleSlot	   pointer to slot in tuple table holding scan tuple
 * ----------------
 */
#[repr(C)]
pub struct ScanState {
    pub ps: PlanState, /* its first field is NodeTag */
    pub ss_currentRelation: Relation,
    pub ss_currentScanDesc: *mut TableScanDescData,
    pub ss_ScanTupleSlot: *mut TupleTableSlot,
}

/* ----------------
 *	 SeqScanState information
 * ----------------
 */
#[repr(C)]
pub struct SeqScanState {
    pub ss: ScanState,   /* its first field is NodeTag */
    pub pscan_len: Size, /* size of parallel heap scan descriptor */
}

/* ----------------
 *	 SampleScanState information
 * ----------------
 */
#[repr(C)]
pub struct SampleScanState {
    pub ss: ScanState,
    pub args: *mut List,        /* expr states for TABLESAMPLE params */
    pub repeatable: *mut ExprState, /* expr state for REPEATABLE expr */
    /* use struct pointer to avoid including tsmapi.h here */
    pub tsmroutine: *mut TsmRoutine, /* descriptor for tablesample method */
    pub tsm_state: *mut c_void, /* tablesample method can keep state here */
    pub use_bulkread: bool,     /* use bulkread buffer access strategy? */
    pub use_pagemode: bool,     /* use page-at-a-time visibility checking? */
    pub begun: bool,            /* false means need to call BeginSampleScan */
    pub seed: uint32,           /* random seed */
    pub donetuples: int64,      /* number of tuples already returned */
    pub haveblock: bool,        /* has a block for sampling been determined */
    pub done: bool,             /* exhausted all tuples? */
}

/*
 * These structs store information about index quals that don't have simple
 * constant right-hand sides.  See comments for ExecIndexBuildScanKeys()
 * for discussion.
 */
#[repr(C)]
pub struct IndexRuntimeKeyInfo {
    pub scan_key: *mut ScanKeyData, /* scankey to put value into */
    pub key_expr: *mut ExprState,   /* expr to evaluate to get value */
    pub key_toastable: bool,        /* is expr's result a toastable datatype? */
}

#[repr(C)]
pub struct IndexArrayKeyInfo {
    pub scan_key: *mut ScanKeyData, /* scankey to put value into */
    pub array_expr: *mut ExprState, /* expr to evaluate to get array value */
    pub next_elem: c_int,           /* next array element to use */
    pub num_elems: c_int,           /* number of elems in current array value */
    pub elem_values: *mut Datum,    /* array of num_elems Datums */
    pub elem_nulls: *mut bool,      /* array of num_elems is-null flags */
}

/* ----------------
 *	 IndexScanState information
 *
 *		indexqualorig	   execution state for indexqualorig expressions
 *		indexorderbyorig   execution state for indexorderbyorig expressions
 *		ScanKeys		   Skey structures for index quals
 *		NumScanKeys		   number of ScanKeys
 *		OrderByKeys		   Skey structures for index ordering operators
 *		NumOrderByKeys	   number of OrderByKeys
 *		RuntimeKeys		   info about Skeys that must be evaluated at runtime
 *		NumRuntimeKeys	   number of RuntimeKeys
 *		RuntimeKeysReady   true if runtime Skeys have been computed
 *		RuntimeContext	   expr context for evaling runtime Skeys
 *		RelationDesc	   index relation descriptor
 *		ScanDesc		   index scan descriptor
 *		Instrument		   local index scan instrumentation
 *		SharedInfo		   parallel worker instrumentation (no leader entry)
 *
 *		ReorderQueue	   tuples that need reordering due to re-check
 *		ReachedEnd		   have we fetched all tuples from index already?
 *		OrderByValues	   values of ORDER BY exprs of last fetched tuple
 *		OrderByNulls	   null flags for OrderByValues
 *		SortSupport		   for reordering ORDER BY exprs
 *		OrderByTypByVals   is the datatype of order by expression pass-by-value?
 *		OrderByTypLens	   typlens of the datatypes of order by expressions
 *		PscanLen		   size of parallel index scan descriptor
 * ----------------
 */
#[repr(C)]
pub struct IndexScanState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub indexqualorig: *mut ExprState,
    pub indexorderbyorig: *mut List,
    pub iss_ScanKeys: *mut ScanKeyData,
    pub iss_NumScanKeys: c_int,
    pub iss_OrderByKeys: *mut ScanKeyData,
    pub iss_NumOrderByKeys: c_int,
    pub iss_RuntimeKeys: *mut IndexRuntimeKeyInfo,
    pub iss_NumRuntimeKeys: c_int,
    pub iss_RuntimeKeysReady: bool,
    pub iss_RuntimeContext: *mut ExprContext,
    pub iss_RelationDesc: Relation,
    pub iss_ScanDesc: *mut IndexScanDescData,
    pub iss_Instrument: IndexScanInstrumentation,
    pub iss_SharedInfo: *mut SharedIndexScanInstrumentation,

    /* These are needed for re-checking ORDER BY expr ordering */
    pub iss_ReorderQueue: *mut pairingheap,
    pub iss_ReachedEnd: bool,
    pub iss_OrderByValues: *mut Datum,
    pub iss_OrderByNulls: *mut bool,
    pub iss_SortSupport: SortSupport,
    pub iss_OrderByTypByVals: *mut bool,
    pub iss_OrderByTypLens: *mut int16,
    pub iss_PscanLen: Size,
}

/* ----------------
 *	 IndexOnlyScanState information
 *
 *		recheckqual		   execution state for recheckqual expressions
 *		ScanKeys		   Skey structures for index quals
 *		NumScanKeys		   number of ScanKeys
 *		OrderByKeys		   Skey structures for index ordering operators
 *		NumOrderByKeys	   number of OrderByKeys
 *		RuntimeKeys		   info about Skeys that must be evaluated at runtime
 *		NumRuntimeKeys	   number of RuntimeKeys
 *		RuntimeKeysReady   true if runtime Skeys have been computed
 *		RuntimeContext	   expr context for evaling runtime Skeys
 *		RelationDesc	   index relation descriptor
 *		ScanDesc		   index scan descriptor
 *		Instrument		   local index scan instrumentation
 *		SharedInfo		   parallel worker instrumentation (no leader entry)
 *		TableSlot		   slot for holding tuples fetched from the table
 *		VMBuffer		   buffer in use for visibility map testing, if any
 *		PscanLen		   size of parallel index-only scan descriptor
 *		NameCStringAttNums attnums of name typed columns to pad to NAMEDATALEN
 *		NameCStringCount   number of elements in the NameCStringAttNums array
 * ----------------
 */
#[repr(C)]
pub struct IndexOnlyScanState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub recheckqual: *mut ExprState,
    pub ioss_ScanKeys: *mut ScanKeyData,
    pub ioss_NumScanKeys: c_int,
    pub ioss_OrderByKeys: *mut ScanKeyData,
    pub ioss_NumOrderByKeys: c_int,
    pub ioss_RuntimeKeys: *mut IndexRuntimeKeyInfo,
    pub ioss_NumRuntimeKeys: c_int,
    pub ioss_RuntimeKeysReady: bool,
    pub ioss_RuntimeContext: *mut ExprContext,
    pub ioss_RelationDesc: Relation,
    pub ioss_ScanDesc: *mut IndexScanDescData,
    pub ioss_Instrument: IndexScanInstrumentation,
    pub ioss_SharedInfo: *mut SharedIndexScanInstrumentation,
    pub ioss_TableSlot: *mut TupleTableSlot,
    pub ioss_VMBuffer: Buffer,
    pub ioss_PscanLen: Size,
    pub ioss_NameCStringAttNums: *mut AttrNumber,
    pub ioss_NameCStringCount: c_int,
}

/* ----------------
 *	 BitmapIndexScanState information
 *
 *		result			   bitmap to return output into, or NULL
 *		ScanKeys		   Skey structures for index quals
 *		NumScanKeys		   number of ScanKeys
 *		RuntimeKeys		   info about Skeys that must be evaluated at runtime
 *		NumRuntimeKeys	   number of RuntimeKeys
 *		ArrayKeys		   info about Skeys that come from ScalarArrayOpExprs
 *		NumArrayKeys	   number of ArrayKeys
 *		RuntimeKeysReady   true if runtime Skeys have been computed
 *		RuntimeContext	   expr context for evaling runtime Skeys
 *		RelationDesc	   index relation descriptor
 *		ScanDesc		   index scan descriptor
 *		Instrument		   local index scan instrumentation
 *		SharedInfo		   parallel worker instrumentation (no leader entry)
 * ----------------
 */
#[repr(C)]
pub struct BitmapIndexScanState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub biss_result: *mut TIDBitmap,
    pub biss_ScanKeys: *mut ScanKeyData,
    pub biss_NumScanKeys: c_int,
    pub biss_RuntimeKeys: *mut IndexRuntimeKeyInfo,
    pub biss_NumRuntimeKeys: c_int,
    pub biss_ArrayKeys: *mut IndexArrayKeyInfo,
    pub biss_NumArrayKeys: c_int,
    pub biss_RuntimeKeysReady: bool,
    pub biss_RuntimeContext: *mut ExprContext,
    pub biss_RelationDesc: Relation,
    pub biss_ScanDesc: *mut IndexScanDescData,
    pub biss_Instrument: IndexScanInstrumentation,
    pub biss_SharedInfo: *mut SharedIndexScanInstrumentation,
}

/* ----------------
 *	 BitmapHeapScanInstrumentation information
 *
 *		exact_pages		   total number of exact pages retrieved
 *		lossy_pages		   total number of lossy pages retrieved
 * ----------------
 */
#[repr(C)]
pub struct BitmapHeapScanInstrumentation {
    pub exact_pages: uint64, /* total number of exact pages retrieved */
    pub lossy_pages: uint64, /* total number of lossy pages retrieved */
}

/* ----------------
 *	 SharedBitmapState information
 *
 *		BM_INITIAL		TIDBitmap creation is not yet started, so first worker
 *						to see this state will set the state to BM_INPROGRESS
 *						and that process will be responsible for creating
 *						TIDBitmap.
 *		BM_INPROGRESS	TIDBitmap creation is in progress; workers need to
 *						sleep until it's finished.
 *		BM_FINISHED		TIDBitmap creation is done, so now all workers can
 *						proceed to iterate over TIDBitmap.
 * ----------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SharedBitmapState {
    BM_INITIAL,
    BM_INPROGRESS,
    BM_FINISHED,
}
pub use SharedBitmapState::*;

/* ----------------
 *	 ParallelBitmapHeapState information
 *		tbmiterator				iterator for scanning current pages
 *		mutex					mutual exclusion for state
 *		state					current state of the TIDBitmap
 *		cv						conditional wait variable
 * ----------------
 */
#[repr(C)]
pub struct ParallelBitmapHeapState {
    pub tbmiterator: dsa_pointer,
    pub mutex: slock_t,
    pub state: SharedBitmapState,
    pub cv: ConditionVariable,
}

/* ----------------
 *	 Instrumentation data for a parallel bitmap heap scan.
 *
 * A shared memory struct that each parallel worker copies its
 * BitmapHeapScanInstrumentation information into at executor shutdown to
 * allow the leader to display the information in EXPLAIN ANALYZE.
 * ----------------
 */
#[repr(C)]
pub struct SharedBitmapHeapInstrumentation {
    pub num_workers: c_int,
    pub sinstrument: [BitmapHeapScanInstrumentation; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/* ----------------
 *	 BitmapHeapScanState information
 *
 *		bitmapqualorig	   execution state for bitmapqualorig expressions
 *		tbm				   bitmap obtained from child index scan(s)
 *		stats			   execution statistics
 *		initialized		   is node is ready to iterate
 *		pstate			   shared state for parallel bitmap scan
 *		sinstrument		   statistics for parallel workers
 *		recheck			   do current page's tuples need recheck
 * ----------------
 */
#[repr(C)]
pub struct BitmapHeapScanState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub bitmapqualorig: *mut ExprState,
    pub tbm: *mut TIDBitmap,
    pub stats: BitmapHeapScanInstrumentation,
    pub initialized: bool,
    pub pstate: *mut ParallelBitmapHeapState,
    pub sinstrument: *mut SharedBitmapHeapInstrumentation,
    pub recheck: bool,
}

/* ----------------
 *	 TidScanState information
 *
 *		tidexprs	   list of TidExpr structs (see nodeTidscan.c)
 *		isCurrentOf    scan has a CurrentOfExpr qual
 *		NumTids		   number of tids in this scan
 *		TidPtr		   index of currently fetched tid
 *		TidList		   evaluated item pointers (array of size NumTids)
 * ----------------
 */
#[repr(C)]
pub struct TidScanState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub tss_tidexprs: *mut List,
    pub tss_isCurrentOf: bool,
    pub tss_NumTids: c_int,
    pub tss_TidPtr: c_int,
    pub tss_TidList: *mut ItemPointerData,
}

/* ----------------
 *	 TidRangeScanState information
 *
 *		trss_tidexprs		list of TidOpExpr structs (see nodeTidrangescan.c)
 *		trss_mintid			the lowest TID in the scan range
 *		trss_maxtid			the highest TID in the scan range
 *		trss_inScan			is a scan currently in progress?
 * ----------------
 */
#[repr(C)]
pub struct TidRangeScanState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub trss_tidexprs: *mut List,
    pub trss_mintid: ItemPointerData,
    pub trss_maxtid: ItemPointerData,
    pub trss_inScan: bool,
}

/* ----------------
 *	 SubqueryScanState information
 *
 *		SubqueryScanState is used for scanning a sub-query in the range table.
 *		ScanTupleSlot references the current output tuple of the sub-query.
 * ----------------
 */
#[repr(C)]
pub struct SubqueryScanState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub subplan: *mut PlanState,
}

/* ----------------
 *	 FunctionScanState information
 *
 *		Function nodes are used to scan the results of a
 *		function appearing in FROM (typically a function returning set).
 *
 *		eflags				node's capability flags
 *		ordinality			is this scan WITH ORDINALITY?
 *		simple				true if we have 1 function and no ordinality
 *		ordinal				current ordinal column value
 *		nfuncs				number of functions being executed
 *		funcstates			per-function execution states (private in
 *							nodeFunctionscan.c)
 *		argcontext			memory context to evaluate function arguments in
 * ----------------
 */
#[repr(C)]
pub struct FunctionScanState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub eflags: c_int,
    pub ordinality: bool,
    pub simple: bool,
    pub ordinal: int64,
    pub nfuncs: c_int,
    pub funcstates: *mut FunctionScanPerFuncState, /* array of length nfuncs */
    pub argcontext: MemoryContext,
}

/* ----------------
 *	 ValuesScanState information
 *
 *		ValuesScan nodes are used to scan the results of a VALUES list
 *
 *		rowcontext			per-expression-list context
 *		exprlists			array of expression lists being evaluated
 *		exprstatelists		array of expression state lists, for SubPlans only
 *		array_len			size of above arrays
 *		curr_idx			current array index (0-based)
 *
 *	Note: ss.ps.ps_ExprContext is used to evaluate any qual or projection
 *	expressions attached to the node.  We create a second ExprContext,
 *	rowcontext, in which to build the executor expression state for each
 *	Values sublist.  Resetting this context lets us get rid of expression
 *	state for each row, avoiding major memory leakage over a long values list.
 *	However, that doesn't work for sublists containing SubPlans, because a
 *	SubPlan has to be connected up to the outer plan tree to work properly.
 *	Therefore, for only those sublists containing SubPlans, we do expression
 *	state construction at executor start, and store those pointers in
 *	exprstatelists[].  NULL entries in that array correspond to simple
 *	subexpressions that are handled as described above.
 * ----------------
 */
#[repr(C)]
pub struct ValuesScanState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub rowcontext: *mut ExprContext,
    pub exprlists: *mut *mut List,
    pub exprstatelists: *mut *mut List,
    pub array_len: c_int,
    pub curr_idx: c_int,
}

/* ----------------
 *		TableFuncScanState node
 *
 * Used in table-expression functions like XMLTABLE.
 * ----------------
 */
#[repr(C)]
pub struct TableFuncScanState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub docexpr: *mut ExprState, /* state for document expression */
    pub rowexpr: *mut ExprState, /* state for row-generating expression */
    pub colexprs: *mut List,     /* state for column-generating expression */
    pub coldefexprs: *mut List,  /* state for column default expressions */
    pub colvalexprs: *mut List,  /* state for column value expressions */
    pub passingvalexprs: *mut List, /* state for PASSING argument expressions */
    pub ns_names: *mut List,     /* same as TableFunc.ns_names */
    pub ns_uris: *mut List,      /* list of states of namespace URI exprs */
    pub notnulls: *mut Bitmapset, /* nullability flag for each output column */
    pub opaque: *mut c_void,     /* table builder private space */
    pub routine: *const TableFuncRoutine, /* table builder methods */
    pub in_functions: *mut FmgrInfo, /* input function for each column */
    pub typioparams: *mut Oid,   /* typioparam for each column */
    pub ordinal: int64,          /* row number to be output next */
    pub perTableCxt: MemoryContext, /* per-table context */
    pub tupstore: *mut Tuplestorestate, /* output tuple store */
}

/* ----------------
 *	 CteScanState information
 *
 *		CteScan nodes are used to scan a CommonTableExpr query.
 *
 * Multiple CteScan nodes can read out from the same CTE query.  We use
 * a tuplestore to hold rows that have been read from the CTE query but
 * not yet consumed by all readers.
 * ----------------
 */
#[repr(C)]
pub struct CteScanState {
    pub ss: ScanState,  /* its first field is NodeTag */
    pub eflags: c_int,  /* capability flags to pass to tuplestore */
    pub readptr: c_int, /* index of my tuplestore read pointer */
    pub cteplanstate: *mut PlanState, /* PlanState for the CTE query itself */
    /* Link to the "leader" CteScanState (possibly this same node) */
    pub leader: *mut CteScanState,
    /* The remaining fields are only valid in the "leader" CteScanState */
    pub cte_table: *mut Tuplestorestate, /* rows already read from the CTE query */
    pub eof_cte: bool,  /* reached end of CTE query? */
}

/* ----------------
 *	 NamedTuplestoreScanState information
 *
 *		NamedTuplestoreScan nodes are used to scan a Tuplestore created and
 *		named prior to execution of the query.  An example is a transition
 *		table for an AFTER trigger.
 *
 * Multiple NamedTuplestoreScan nodes can read out from the same Tuplestore.
 * ----------------
 */
#[repr(C)]
pub struct NamedTuplestoreScanState {
    pub ss: ScanState,  /* its first field is NodeTag */
    pub readptr: c_int, /* index of my tuplestore read pointer */
    pub tupdesc: TupleDesc, /* format of the tuples in the tuplestore */
    pub relation: *mut Tuplestorestate, /* the rows */
}

/* ----------------
 *	 WorkTableScanState information
 *
 *		WorkTableScan nodes are used to scan the work table created by
 *		a RecursiveUnion node.  We locate the RecursiveUnion node
 *		during executor startup.
 * ----------------
 */
#[repr(C)]
pub struct WorkTableScanState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub rustate: *mut RecursiveUnionState,
}

/* ----------------
 *	 ForeignScanState information
 *
 *		ForeignScan nodes are used to scan foreign-data tables.
 * ----------------
 */
#[repr(C)]
pub struct ForeignScanState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub fdw_recheck_quals: *mut ExprState, /* original quals not in ss.ps.qual */
    pub pscan_len: Size, /* size of parallel coordination information */
    pub resultRelInfo: *mut ResultRelInfo, /* result rel info, if UPDATE or DELETE */
    /* use struct pointer to avoid including fdwapi.h here */
    pub fdwroutine: *mut FdwRoutine,
    pub fdw_state: *mut c_void, /* foreign-data wrapper can keep state here */
}

/* ----------------
 *	 CustomScanState information
 *
 *		CustomScan nodes are used to execute custom code within executor.
 *
 * Core code must avoid assuming that the CustomScanState is only as large as
 * the structure declared here; providers are allowed to make it the first
 * element in a larger structure, and typically would need to do so.  The
 * struct is actually allocated by the CreateCustomScanState method associated
 * with the plan node.  Any additional fields can be initialized there, or in
 * the BeginCustomScan method.
 * ----------------
 */
#[repr(C)]
pub struct CustomScanState {
    pub ss: ScanState,
    pub flags: uint32, /* mask of CUSTOMPATH_* flags, see
                        * nodes/extensible.h */
    pub custom_ps: *mut List, /* list of child PlanState nodes, if any */
    pub pscan_len: Size, /* size of parallel coordination information */
    pub methods: *const CustomExecMethods,
    pub slotOps: *const TupleTableSlotOps,
}

/* ----------------------------------------------------------------
 *				 Join State Information
 * ----------------------------------------------------------------
 */

/* ----------------
 *	 JoinState information
 *
 *		Superclass for state nodes of join plans.
 * ----------------
 */
#[repr(C)]
pub struct JoinState {
    pub ps: PlanState,
    pub jointype: JoinType,
    pub single_match: bool, /* True if we should skip to next outer tuple
                             * after finding one inner match */
    pub joinqual: *mut ExprState, /* JOIN quals (in addition to ps.qual) */
}

/* ----------------
 *	 NestLoopState information
 *
 *		NeedNewOuter	   true if need new outer tuple on next call
 *		MatchedOuter	   true if found a join match for current outer tuple
 *		NullInnerTupleSlot prepared null tuple for left outer joins
 * ----------------
 */
#[repr(C)]
pub struct NestLoopState {
    pub js: JoinState, /* its first field is NodeTag */
    pub nl_NeedNewOuter: bool,
    pub nl_MatchedOuter: bool,
    pub nl_NullInnerTupleSlot: *mut TupleTableSlot,
}

/* ----------------
 *	 MergeJoinState information
 *
 *		NumClauses		   number of mergejoinable join clauses
 *		Clauses			   info for each mergejoinable clause
 *		JoinState		   current state of ExecMergeJoin state machine
 *		SkipMarkRestore    true if we may skip Mark and Restore operations
 *		ExtraMarks		   true to issue extra Mark operations on inner scan
 *		ConstFalseJoin	   true if we have a constant-false joinqual
 *		FillOuter		   true if should emit unjoined outer tuples anyway
 *		FillInner		   true if should emit unjoined inner tuples anyway
 *		MatchedOuter	   true if found a join match for current outer tuple
 *		MatchedInner	   true if found a join match for current inner tuple
 *		OuterTupleSlot	   slot in tuple table for cur outer tuple
 *		InnerTupleSlot	   slot in tuple table for cur inner tuple
 *		MarkedTupleSlot    slot in tuple table for marked tuple
 *		NullOuterTupleSlot prepared null tuple for right outer joins
 *		NullInnerTupleSlot prepared null tuple for left outer joins
 *		OuterEContext	   workspace for computing outer tuple's join values
 *		InnerEContext	   workspace for computing inner tuple's join values
 * ----------------
 */
/* private in nodeMergejoin.c: */

#[repr(C)]
pub struct MergeJoinState {
    pub js: JoinState, /* its first field is NodeTag */
    pub mj_NumClauses: c_int,
    pub mj_Clauses: MergeJoinClause, /* array of length mj_NumClauses */
    pub mj_JoinState: c_int,
    pub mj_SkipMarkRestore: bool,
    pub mj_ExtraMarks: bool,
    pub mj_ConstFalseJoin: bool,
    pub mj_FillOuter: bool,
    pub mj_FillInner: bool,
    pub mj_MatchedOuter: bool,
    pub mj_MatchedInner: bool,
    pub mj_OuterTupleSlot: *mut TupleTableSlot,
    pub mj_InnerTupleSlot: *mut TupleTableSlot,
    pub mj_MarkedTupleSlot: *mut TupleTableSlot,
    pub mj_NullOuterTupleSlot: *mut TupleTableSlot,
    pub mj_NullInnerTupleSlot: *mut TupleTableSlot,
    pub mj_OuterEContext: *mut ExprContext,
    pub mj_InnerEContext: *mut ExprContext,
}

/* ----------------
 *	 HashJoinState information
 *
 *		hashclauses				original form of the hashjoin condition
 *		hj_OuterHash			ExprState for hashing outer keys
 *		hj_HashTable			hash table for the hashjoin
 *								(NULL if table not built yet)
 *		hj_CurHashValue			hash value for current outer tuple
 *		hj_CurBucketNo			regular bucket# for current outer tuple
 *		hj_CurSkewBucketNo		skew bucket# for current outer tuple
 *		hj_CurTuple				last inner tuple matched to current outer
 *								tuple, or NULL if starting search
 *								(hj_CurXXX variables are undefined if
 *								OuterTupleSlot is empty!)
 *		hj_OuterTupleSlot		tuple slot for outer tuples
 *		hj_HashTupleSlot		tuple slot for inner (hashed) tuples
 *		hj_NullOuterTupleSlot	prepared null tuple for right/right-anti/full
 *								outer joins
 *		hj_NullInnerTupleSlot	prepared null tuple for left/full outer joins
 *		hj_FirstOuterTupleSlot	first tuple retrieved from outer plan
 *		hj_JoinState			current state of ExecHashJoin state machine
 *		hj_MatchedOuter			true if found a join match for current outer
 *		hj_OuterNotEmpty		true if outer relation known not empty
 * ----------------
 */

/* these structs are defined in executor/hashjoin.h: */

#[repr(C)]
pub struct HashJoinState {
    pub js: JoinState, /* its first field is NodeTag */
    pub hashclauses: *mut ExprState,
    pub hj_OuterHash: *mut ExprState,
    pub hj_HashTable: HashJoinTable,
    pub hj_CurHashValue: uint32,
    pub hj_CurBucketNo: c_int,
    pub hj_CurSkewBucketNo: c_int,
    pub hj_CurTuple: HashJoinTuple,
    pub hj_OuterTupleSlot: *mut TupleTableSlot,
    pub hj_HashTupleSlot: *mut TupleTableSlot,
    pub hj_NullOuterTupleSlot: *mut TupleTableSlot,
    pub hj_NullInnerTupleSlot: *mut TupleTableSlot,
    pub hj_FirstOuterTupleSlot: *mut TupleTableSlot,
    pub hj_JoinState: c_int,
    pub hj_MatchedOuter: bool,
    pub hj_OuterNotEmpty: bool,
}

/* ----------------------------------------------------------------
 *				 Materialization State Information
 * ----------------------------------------------------------------
 */

/* ----------------
 *	 MaterialState information
 *
 *		materialize nodes are used to materialize the results
 *		of a subplan into a temporary file.
 *
 *		ss.ss_ScanTupleSlot refers to output of underlying plan.
 * ----------------
 */
#[repr(C)]
pub struct MaterialState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub eflags: c_int, /* capability flags to pass to tuplestore */
    pub eof_underlying: bool, /* reached end of underlying plan? */
    pub tuplestorestate: *mut Tuplestorestate,
}

#[repr(C)]
pub struct MemoizeInstrumentation {
    pub cache_hits: uint64, /* number of rescans where we've found the
                             * scan parameter values to be cached */
    pub cache_misses: uint64, /* number of rescans where we've not found the
                               * scan parameter values to be cached. */
    pub cache_evictions: uint64, /* number of cache entries removed due to
                                  * the need to free memory */
    pub cache_overflows: uint64, /* number of times we've had to bypass the
                                  * cache when filling it due to not being
                                  * able to free enough space to store the
                                  * current scan's tuples. */
    pub mem_peak: uint64, /* peak memory usage in bytes */
}

/* ----------------
 *	 Shared memory container for per-worker memoize information
 * ----------------
 */
#[repr(C)]
pub struct SharedMemoizeInfo {
    pub num_workers: c_int,
    pub sinstrument: [MemoizeInstrumentation; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/* ----------------
 *	 MemoizeState information
 *
 *		memoize nodes are used to cache recent and commonly seen results from
 *		a parameterized scan.
 * ----------------
 */
#[repr(C)]
pub struct MemoizeState {
    pub ss: ScanState,  /* its first field is NodeTag */
    pub mstatus: c_int, /* value of ExecMemoize state machine */
    pub nkeys: c_int,   /* number of cache keys */
    pub hashtable: *mut memoize_hash, /* hash table for cache entries */
    pub hashkeydesc: TupleDesc, /* tuple descriptor for cache keys */
    pub tableslot: *mut TupleTableSlot, /* min tuple slot for existing cache entries */
    pub probeslot: *mut TupleTableSlot, /* virtual slot used for hash lookups */
    pub cache_eq_expr: *mut ExprState, /* Compare exec params to hash key */
    pub param_exprs: *mut *mut ExprState, /* exprs containing the parameters to this
                                           * node */
    pub hashfunctions: *mut FmgrInfo, /* lookup data for hash funcs nkeys in size */
    pub collations: *mut Oid, /* collation for comparisons nkeys in size */
    pub mem_used: uint64, /* bytes of memory used by cache */
    pub mem_limit: uint64, /* memory limit in bytes for the cache */
    pub tableContext: MemoryContext, /* memory context to store cache data */
    pub lru_list: dlist_head, /* least recently used entry list */
    pub last_tuple: *mut MemoizeTuple, /* Used to point to the last tuple
                                        * returned during a cache hit and the
                                        * tuple we last stored when
                                        * populating the cache. */
    pub entry: *mut MemoizeEntry, /* the entry that 'last_tuple' belongs to or
                                   * NULL if 'last_tuple' is NULL. */
    pub singlerow: bool, /* true if the cache entry is to be marked as
                          * complete after caching the first tuple. */
    pub binary_mode: bool, /* true when cache key should be compared bit
                            * by bit, false when using hash equality ops */
    pub stats: MemoizeInstrumentation, /* execution statistics */
    pub shared_info: *mut SharedMemoizeInfo, /* statistics for parallel workers */
    pub keyparamids: *mut Bitmapset, /* Param->paramids of expressions belonging to
                                      * param_exprs */
}

/* ----------------
 *	 When performing sorting by multiple keys, it's possible that the input
 *	 dataset is already sorted on a prefix of those keys. We call these
 *	 "presorted keys".
 *	 PresortedKeyData represents information about one such key.
 * ----------------
 */
#[repr(C)]
pub struct PresortedKeyData {
    pub flinfo: FmgrInfo,        /* comparison function info */
    pub fcinfo: FunctionCallInfo, /* comparison function call info */
    pub attno: OffsetNumber,     /* attribute number in tuple */
}

/* ----------------
 *	 Shared memory container for per-worker sort information
 * ----------------
 */
#[repr(C)]
pub struct SharedSortInfo {
    pub num_workers: c_int,
    pub sinstrument: [TuplesortInstrumentation; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/* ----------------
 *	 SortState information
 * ----------------
 */
#[repr(C)]
pub struct SortState {
    pub ss: ScanState,     /* its first field is NodeTag */
    pub randomAccess: bool, /* need random access to sort output? */
    pub bounded: bool,     /* is the result set bounded? */
    pub bound: int64,      /* if bounded, how many tuples are needed */
    pub sort_Done: bool,   /* sort completed yet? */
    pub bounded_Done: bool, /* value of bounded we did the sort with */
    pub bound_Done: int64, /* value of bound we did the sort with */
    pub tuplesortstate: *mut c_void, /* private state of tuplesort.c */
    pub am_worker: bool,   /* are we a worker? */
    pub datumSort: bool,   /* Datum sort instead of tuple sort? */
    pub shared_info: *mut SharedSortInfo, /* one entry per worker */
}

/* ----------------
 *	 Instrumentation information for IncrementalSort
 * ----------------
 */
#[repr(C)]
pub struct IncrementalSortGroupInfo {
    pub groupCount: int64,
    pub maxDiskSpaceUsed: int64,
    pub totalDiskSpaceUsed: int64,
    pub maxMemorySpaceUsed: int64,
    pub totalMemorySpaceUsed: int64,
    pub sortMethods: bits32, /* bitmask of TuplesortMethod */
}

#[repr(C)]
pub struct IncrementalSortInfo {
    pub fullsortGroupInfo: IncrementalSortGroupInfo,
    pub prefixsortGroupInfo: IncrementalSortGroupInfo,
}

/* ----------------
 *	 Shared memory container for per-worker incremental sort information
 * ----------------
 */
#[repr(C)]
pub struct SharedIncrementalSortInfo {
    pub num_workers: c_int,
    pub sinfo: [IncrementalSortInfo; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/* ----------------
 *	 IncrementalSortState information
 * ----------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IncrementalSortExecutionStatus {
    INCSORT_LOADFULLSORT,
    INCSORT_LOADPREFIXSORT,
    INCSORT_READFULLSORT,
    INCSORT_READPREFIXSORT,
}
pub use IncrementalSortExecutionStatus::*;

#[repr(C)]
pub struct IncrementalSortState {
    pub ss: ScanState,    /* its first field is NodeTag */
    pub bounded: bool,    /* is the result set bounded? */
    pub bound: int64,     /* if bounded, how many tuples are needed */
    pub outerNodeDone: bool, /* finished fetching tuples from outer node */
    pub bound_Done: int64, /* value of bound we did the sort with */
    pub execution_status: IncrementalSortExecutionStatus,
    pub n_fullsort_remaining: int64,
    pub fullsort_state: *mut Tuplesortstate, /* private state of tuplesort.c */
    pub prefixsort_state: *mut Tuplesortstate, /* private state of tuplesort.c */
    /* the keys by which the input path is already sorted */
    pub presorted_keys: *mut PresortedKeyData,

    pub incsort_info: IncrementalSortInfo,

    /* slot for pivot tuple defining values of presorted keys within group */
    pub group_pivot: *mut TupleTableSlot,
    pub transfer_tuple: *mut TupleTableSlot,
    pub am_worker: bool, /* are we a worker? */
    pub shared_info: *mut SharedIncrementalSortInfo, /* one entry per worker */
}

/* ---------------------
 *	GroupState information
 * ---------------------
 */
#[repr(C)]
pub struct GroupState {
    pub ss: ScanState, /* its first field is NodeTag */
    pub eqfunction: *mut ExprState, /* equality function */
    pub grp_done: bool, /* indicates completion of Group scan */
}

/* ---------------------
 *	per-worker aggregate information
 * ---------------------
 */
#[repr(C)]
pub struct AggregateInstrumentation {
    pub hash_mem_peak: Size,       /* peak hash table memory usage */
    pub hash_disk_used: uint64,    /* kB of disk space used */
    pub hash_batches_used: c_int,  /* batches used during entire execution */
}

/* ----------------
 *	 Shared memory container for per-worker aggregate information
 * ----------------
 */
#[repr(C)]
pub struct SharedAggInfo {
    pub num_workers: c_int,
    pub sinstrument: [AggregateInstrumentation; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/* ---------------------
 *	AggState information
 *
 *	ss.ss_ScanTupleSlot refers to output of underlying plan.
 *
 *	Note: ss.ps.ps_ExprContext contains ecxt_aggvalues and
 *	ecxt_aggnulls arrays, which hold the computed agg values for the current
 *	input group during evaluation of an Agg node's output tuple(s).  We
 *	create a second ExprContext, tmpcontext, in which to evaluate input
 *	expressions and run the aggregate transition functions.
 * ---------------------
 */
/* these structs are private in nodeAgg.c: */
/// TODO(pg-port): real def `typedef struct AggStatePerAggData *AggStatePerAgg`
/// (private in nodeAgg.c).
#[repr(C)]
pub struct AggStatePerAggData {
    _opaque: [u8; 0],
}
pub type AggStatePerAgg = *mut AggStatePerAggData;
/// TODO(pg-port): real def `typedef struct AggStatePerTransData
/// *AggStatePerTrans` (private in nodeAgg.c).
/// Fields added at end for execExpr.c use; C home: nodeAgg.h / nodeAgg.c.
#[repr(C)]
pub struct AggStatePerTransData {
    _opaque: [u8; 0],
    /// aggref -- the Aggref node for this transition state (nodeAgg.h)
    pub aggref: *mut crate::nodes::primnodes::Aggref,
    /// numInputs -- total # of inputs including ORDER BY cols (nodeAgg.h)
    pub numInputs: c_int,
    /// numTransInputs -- # of non-ORDER-BY inputs (nodeAgg.h)
    pub numTransInputs: c_int,
    /// numSortCols -- number of sort columns (nodeAgg.h)
    pub numSortCols: c_int,
    /// numDistinctCols -- number of distinct cols (nodeAgg.h)
    pub numDistinctCols: c_int,
    /// aggsortrequired -- sorting/distinct required? (nodeAgg.h)
    pub aggsortrequired: bool,
    /// sortslot -- virtual slot for input to sorter (nodeAgg.h)
    pub sortslot: *mut TupleTableSlot,
    /// transfn_fcinfo -- call info for transition function (nodeAgg.h)
    pub transfn_fcinfo: FunctionCallInfo,
    /// deserialfn_oid -- Oid of deserialization function (nodeAgg.h)
    pub deserialfn_oid: Oid,
    /// deserialfn -- deserialization function info (nodeAgg.h)
    pub deserialfn: FmgrInfo,
    /// deserialfn_fcinfo -- call info for deserialization function (nodeAgg.h)
    pub deserialfn_fcinfo: FunctionCallInfo,
    /// transtypeByVal -- transition value is by-value? (nodeAgg.h)
    pub transtypeByVal: bool,
    /// initValueIsNull -- transition initial value is NULL? (nodeAgg.h)
    pub initValueIsNull: bool,
    // Fields added at END for execExprInterp.rs use (append-only per port convention):
    /// transtypeLen -- byte length of the transition value type (nodeAgg.h)
    pub transtypeLen: i16,
    /// uniqslot -- slot for deduplication in multi-column DISTINCT (nodeAgg.h)
    pub uniqslot: *mut TupleTableSlot,
    /// sortstates -- per-set sort objects for DISTINCT/ORDER BY (nodeAgg.h)
    pub sortstates: *mut *mut Tuplesortstate,
    /// equalfnOne -- pre-initialized fcinfo for single-column equality (nodeAgg.h)
    pub equalfnOne: FunctionCallInfo,
    /// equalfnOneAddr -- fn pointer extracted from equalfnOne (Rust-only)
    pub equalfnOneAddr: Option<crate::utils::fmgr::PGFunction>,
    /// equalfnMulti -- array of fcinfos for multi-column equality (nodeAgg.h)
    pub equalfnMulti: *mut crate::utils::fmgr::FunctionCallInfoBaseData,
    /// equalfnMultiAddr -- array of fn ptrs for multi-column equality (Rust-only)
    pub equalfnMultiAddr: *mut Option<crate::utils::fmgr::PGFunction>,
    /// aggref_set -- which grouping-set index this pertrans was built for (Rust-only)
    pub aggref_set: c_int,
    // -- additional C fields used by nodeAgg.c, appended to keep offsets stable --
    /// aggshared -- transition state shared with another aggregate? (nodeAgg.h)
    pub aggshared: bool,
    /// aggCollation -- collation for transition function (nodeAgg.h)
    pub aggCollation: Oid,
    /// transfn_oid -- Oid of transition (or combine) function (nodeAgg.h)
    pub transfn_oid: Oid,
    /// serialfn_oid -- Oid of serialization function (nodeAgg.h)
    pub serialfn_oid: Oid,
    /// transfn -- transition function FmgrInfo (nodeAgg.h)
    pub transfn: FmgrInfo,
    /// serialfn -- serialization function FmgrInfo (nodeAgg.h)
    pub serialfn: FmgrInfo,
    /// serialfn_fcinfo -- call info for serialization function (nodeAgg.h)
    pub serialfn_fcinfo: FunctionCallInfo,
    /// aggtranstype -- OID of aggregate's declared transition type (nodeAgg.h)
    pub aggtranstype: Oid,
    /// initValue -- initial transition value (nodeAgg.h)
    pub initValue: crate::postgres::Datum,
    /// inputtypeLen -- length of input type (-1 = varlen) (nodeAgg.h)
    pub inputtypeLen: i16,
    /// inputtypeByVal -- is input type pass-by-value? (nodeAgg.h)
    pub inputtypeByVal: bool,
    /// lastdatum -- last value seen for single-col DISTINCT (nodeAgg.h)
    pub lastdatum: crate::postgres::Datum,
    /// lastisnull -- is lastdatum NULL? (nodeAgg.h)
    pub lastisnull: bool,
    /// haslast -- have we set lastdatum yet? (nodeAgg.h)
    pub haslast: bool,
    /// sortdesc -- tuple descriptor for sort operations (nodeAgg.h)
    pub sortdesc: *mut core::ffi::c_void,
    /// sortColIdx -- sort column indices (nodeAgg.h)
    pub sortColIdx: *mut i16,
    /// sortOperators -- sort operator OIDs (nodeAgg.h)
    pub sortOperators: *mut Oid,
    /// sortCollations -- sort collation OIDs (nodeAgg.h)
    pub sortCollations: *mut Oid,
    /// sortNullsFirst -- NULLS FIRST flags (nodeAgg.h)
    pub sortNullsFirst: *mut bool,
    /// equalfnOneFull -- single-input equality FmgrInfo (nodeAgg.c use)
    pub equalfnOneFull: FmgrInfo,
    /// equalfnMultiFull -- multi-col equality ExprState (nodeAgg.c use)
    pub equalfnMultiFull: *mut ExprState,
}
pub type AggStatePerTrans = *mut AggStatePerTransData;
/// TODO(pg-port): real def `typedef struct AggStatePerGroupData
/// *AggStatePerGroup` (private in nodeAgg.c).
#[repr(C)]
pub struct AggStatePerGroupData {
    // Fields from nodeAgg.h AggStatePerGroupData:
    /// transValue -- current transition value
    pub transValue: crate::postgres::Datum,
    /// transValueIsNull -- is transValue null?
    pub transValueIsNull: bool,
    /// noTransValue -- true if transValue has not been set yet
    pub noTransValue: bool,
}
pub type AggStatePerGroup = *mut AggStatePerGroupData;
/// TODO(pg-port): real def `typedef struct AggStatePerPhaseData
/// *AggStatePerPhase` (private in nodeAgg.c).
/// Fields added at end for execExpr.c use; C home: nodeAgg.h / nodeAgg.c.
#[repr(C)]
pub struct AggStatePerPhaseData {
    _opaque: [u8; 0],
    /// numsets -- number of grouping sets in this phase (nodeAgg.h)
    pub numsets: c_int,
}
pub type AggStatePerPhase = *mut AggStatePerPhaseData;
/// TODO(pg-port): real def `typedef struct AggStatePerHashData
/// *AggStatePerHash` (private in nodeAgg.c).
#[repr(C)]
pub struct AggStatePerHashData {
    _opaque: [u8; 0],
}
pub type AggStatePerHash = *mut AggStatePerHashData;

pub const FIELDNO_AGGSTATE_CURAGGCONTEXT: usize = 14;
pub const FIELDNO_AGGSTATE_CURPERTRANS: usize = 16;
pub const FIELDNO_AGGSTATE_CURRENT_SET: usize = 20;
pub const FIELDNO_AGGSTATE_ALL_PERGROUPS: usize = 54;

#[repr(C)]
pub struct AggState {
    pub ss: ScanState,       /* its first field is NodeTag */
    pub aggs: *mut List,     /* all Aggref nodes in targetlist & quals */
    pub numaggs: c_int,      /* length of list (could be zero!) */
    pub numtrans: c_int,     /* number of pertrans items */
    pub aggstrategy: AggStrategy, /* strategy mode */
    pub aggsplit: AggSplit,  /* agg-splitting mode, see nodes.h */
    pub phase: AggStatePerPhase, /* pointer to current phase data */
    pub numphases: c_int,    /* number of phases (including phase 0) */
    pub current_phase: c_int, /* current phase number */
    pub peragg: AggStatePerAgg, /* per-Aggref information */
    pub pertrans: AggStatePerTrans, /* per-Trans state information */
    pub hashcontext: *mut ExprContext, /* econtexts for long-lived data (hashtable) */
    pub aggcontexts: *mut *mut ExprContext, /* econtexts for long-lived data (per GS) */
    pub tmpcontext: *mut ExprContext, /* econtext for input expressions */
    // FIELDNO_AGGSTATE_CURAGGCONTEXT 14
    pub curaggcontext: *mut ExprContext, /* currently active aggcontext */
    pub curperagg: AggStatePerAgg, /* currently active aggregate, if any */
    // FIELDNO_AGGSTATE_CURPERTRANS 16
    pub curpertrans: AggStatePerTrans, /* currently active trans state, if any */
    pub input_done: bool,    /* indicates end of input */
    pub agg_done: bool,      /* indicates completion of Agg scan */
    pub projected_set: c_int, /* The last projected grouping set */
    // FIELDNO_AGGSTATE_CURRENT_SET 20
    pub current_set: c_int,  /* The current grouping set being evaluated */
    pub grouped_cols: *mut Bitmapset, /* grouped cols in current projection */
    pub all_grouped_cols: *mut List, /* list of all grouped cols in DESC order */
    pub colnos_needed: *mut Bitmapset, /* all columns needed from the outer plan */
    pub max_colno_needed: c_int, /* highest colno needed from outer plan */
    pub all_cols_needed: bool, /* are all cols from outer plan needed? */
    /* These fields are for grouping set phase data */
    pub maxsets: c_int,      /* The max number of sets in any phase */
    pub phases: AggStatePerPhase, /* array of all phases */
    pub sort_in: *mut Tuplesortstate, /* sorted input to phases > 1 */
    pub sort_out: *mut Tuplesortstate, /* input is copied here for next phase */
    pub sort_slot: *mut TupleTableSlot, /* slot for sort results */
    /* these fields are used in AGG_PLAIN and AGG_SORTED modes: */
    pub pergroups: *mut AggStatePerGroup, /* grouping set indexed array of per-group
                                           * pointers */
    pub grp_firstTuple: HeapTuple, /* copy of first tuple of current group */
    /* these fields are used in AGG_HASHED and AGG_MIXED modes: */
    pub table_filled: bool,  /* hash table filled yet? */
    pub num_hashes: c_int,
    pub hash_metacxt: MemoryContext, /* memory for hash table bucket array */
    pub hash_tablecxt: MemoryContext, /* memory for hash table entries */
    pub hash_tapeset: *mut LogicalTapeSet, /* tape set for hash spill tapes */
    pub hash_spills: *mut HashAggSpill, /* HashAggSpill for each grouping set,
                                         * exists only during first pass */
    pub hash_spill_rslot: *mut TupleTableSlot, /* for reading spill files */
    pub hash_spill_wslot: *mut TupleTableSlot, /* for writing spill files */
    pub hash_batches: *mut List, /* hash batches remaining to be processed */
    pub hash_ever_spilled: bool, /* ever spilled during this execution? */
    pub hash_spill_mode: bool, /* we hit a limit during the current batch
                                * and we must not create new groups */
    pub hash_mem_limit: Size, /* limit before spilling hash table */
    pub hash_ngroups_limit: uint64, /* limit before spilling hash table */
    pub hash_planned_partitions: c_int, /* number of partitions planned
                                         * for first pass */
    pub hashentrysize: f64,  /* estimate revised during execution */
    pub hash_mem_peak: Size, /* peak hash table memory usage */
    pub hash_ngroups_current: uint64, /* number of groups currently in
                                       * memory in all hash tables */
    pub hash_disk_used: uint64, /* kB of disk space used */
    pub hash_batches_used: c_int, /* batches used during entire execution */

    pub perhash: AggStatePerHash, /* array of per-hashtable data */
    pub hash_pergroup: *mut AggStatePerGroup, /* grouping set indexed array of
                                               * per-group pointers */

    /* support for evaluation of agg input expressions: */
    // FIELDNO_AGGSTATE_ALL_PERGROUPS 54
    pub all_pergroups: *mut AggStatePerGroup, /* array of first ->pergroups, than
                                               * ->hash_pergroup */
    pub shared_info: *mut SharedAggInfo, /* one entry per worker */
}

/* ----------------
 *	WindowAggState information
 * ----------------
 */
/* these structs are private in nodeWindowAgg.c: */
/// TODO(pg-port): real def `typedef struct WindowStatePerFuncData
/// *WindowStatePerFunc` (private in nodeWindowAgg.c).
#[repr(C)]
pub struct WindowStatePerFuncData {
    _opaque: [u8; 0],
}
pub type WindowStatePerFunc = *mut WindowStatePerFuncData;
/// TODO(pg-port): real def `typedef struct WindowStatePerAggData
/// *WindowStatePerAgg` (private in nodeWindowAgg.c).
#[repr(C)]
pub struct WindowStatePerAggData {
    _opaque: [u8; 0],
}
pub type WindowStatePerAgg = *mut WindowStatePerAggData;

/*
 * WindowAggStatus -- Used to track the status of WindowAggState
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WindowAggStatus {
    WINDOWAGG_DONE,        /* No more processing to do */
    WINDOWAGG_RUN,         /* Normal processing of window funcs */
    WINDOWAGG_PASSTHROUGH, /* Don't eval window funcs */
    WINDOWAGG_PASSTHROUGH_STRICT, /* Pass-through plus don't store new
                                   * tuples during spool */
}
pub use WindowAggStatus::*;

#[repr(C)]
pub struct WindowAggState {
    pub ss: ScanState, /* its first field is NodeTag */

    /* these fields are filled in by ExecInitExpr: */
    pub funcs: *mut List, /* all WindowFunc nodes in targetlist */
    pub numfuncs: c_int,  /* total number of window functions */
    pub numaggs: c_int,   /* number that are plain aggregates */

    pub perfunc: WindowStatePerFunc, /* per-window-function information */
    pub peragg: WindowStatePerAgg, /* per-plain-aggregate information */
    pub partEqfunction: *mut ExprState, /* equality funcs for partition columns */
    pub ordEqfunction: *mut ExprState, /* equality funcs for ordering columns */
    pub buffer: *mut Tuplestorestate, /* stores rows of current partition */
    pub current_ptr: c_int, /* read pointer # for current row */
    pub framehead_ptr: c_int, /* read pointer # for frame head, if used */
    pub frametail_ptr: c_int, /* read pointer # for frame tail, if used */
    pub grouptail_ptr: c_int, /* read pointer # for group tail, if used */
    pub spooled_rows: int64, /* total # of rows in buffer */
    pub currentpos: int64, /* position of current row in partition */
    pub frameheadpos: int64, /* current frame head position */
    pub frametailpos: int64, /* current frame tail position (frame end+1) */
    /* use struct pointer to avoid including windowapi.h here */
    pub agg_winobj: *mut WindowObjectData, /* winobj for aggregate fetches */
    pub aggregatedbase: int64, /* start row for current aggregates */
    pub aggregatedupto: int64, /* rows before this one are aggregated */
    pub status: WindowAggStatus, /* run status of WindowAggState */

    pub frameOptions: c_int, /* frame_clause options, see WindowDef */
    pub startOffset: *mut ExprState, /* expression for starting bound offset */
    pub endOffset: *mut ExprState, /* expression for ending bound offset */
    pub startOffsetValue: Datum, /* result of startOffset evaluation */
    pub endOffsetValue: Datum, /* result of endOffset evaluation */

    /* these fields are used with RANGE offset PRECEDING/FOLLOWING: */
    pub startInRangeFunc: FmgrInfo, /* in_range function for startOffset */
    pub endInRangeFunc: FmgrInfo, /* in_range function for endOffset */
    pub inRangeColl: Oid,  /* collation for in_range tests */
    pub inRangeAsc: bool,  /* use ASC sort order for in_range tests? */
    pub inRangeNullsFirst: bool, /* nulls sort first for in_range tests? */

    /* fields relating to runconditions */
    pub use_pass_through: bool, /* When false, stop execution when
                                 * runcondition is no longer true.  Else
                                 * just stop evaluating window funcs. */
    pub top_window: bool, /* true if this is the top-most WindowAgg or
                           * the only WindowAgg in this query level */
    pub runcondition: *mut ExprState, /* Condition which must remain true otherwise
                                       * execution of the WindowAgg will finish or
                                       * go into pass-through mode.  NULL when there
                                       * is no such condition. */

    /* these fields are used in GROUPS mode: */
    pub currentgroup: int64, /* peer group # of current row in partition */
    pub frameheadgroup: int64, /* peer group # of frame head row */
    pub frametailgroup: int64, /* peer group # of frame tail row */
    pub groupheadpos: int64, /* current row's peer group head position */
    pub grouptailpos: int64, /* " " " " tail position (group end+1) */

    pub partcontext: MemoryContext, /* context for partition-lifespan data */
    pub aggcontext: MemoryContext, /* shared context for aggregate working data */
    pub curaggcontext: MemoryContext, /* current aggregate's working data */
    pub tmpcontext: *mut ExprContext, /* short-term evaluation context */

    pub all_first: bool, /* true if the scan is starting */
    pub partition_spooled: bool, /* true if all tuples in current partition
                                  * have been spooled into tuplestore */
    pub next_partition: bool, /* true if begin_partition needs to be called */
    pub more_partitions: bool, /* true if there's more partitions after
                                * this one */
    pub framehead_valid: bool, /* true if frameheadpos is known up to
                                * date for current row */
    pub frametail_valid: bool, /* true if frametailpos is known up to
                                * date for current row */
    pub grouptail_valid: bool, /* true if grouptailpos is known up to
                                * date for current row */

    pub first_part_slot: *mut TupleTableSlot, /* first tuple of current or next
                                               * partition */
    pub framehead_slot: *mut TupleTableSlot, /* first tuple of current frame */
    pub frametail_slot: *mut TupleTableSlot, /* first tuple after current frame */

    /* temporary slots for tuples fetched back from tuplestore */
    pub agg_row_slot: *mut TupleTableSlot,
    pub temp_slot_1: *mut TupleTableSlot,
    pub temp_slot_2: *mut TupleTableSlot,
}

/* ----------------
 *	 UniqueState information
 *
 *		Unique nodes are used "on top of" sort nodes to discard
 *		duplicate tuples returned from the sort phase.  Basically
 *		all it does is compare the current tuple from the subplan
 *		with the previously fetched tuple (stored in its result slot).
 *		If the two are identical in all interesting fields, then
 *		we just fetch another tuple from the sort and try again.
 * ----------------
 */
#[repr(C)]
pub struct UniqueState {
    pub ps: PlanState, /* its first field is NodeTag */
    pub eqfunction: *mut ExprState, /* tuple equality qual */
}

/* ----------------
 * GatherState information
 *
 *		Gather nodes launch 1 or more parallel workers, run a subplan
 *		in those workers, and collect the results.
 * ----------------
 */
#[repr(C)]
pub struct GatherState {
    pub ps: PlanState, /* its first field is NodeTag */
    pub initialized: bool, /* workers launched? */
    pub need_to_scan_locally: bool, /* need to read from local plan? */
    pub tuples_needed: int64, /* tuple bound, see ExecSetTupleBound */
    /* these fields are set up once: */
    pub funnel_slot: *mut TupleTableSlot,
    pub pei: *mut ParallelExecutorInfo,
    /* all remaining fields are reinitialized during a rescan: */
    pub nworkers_launched: c_int, /* original number of workers */
    pub nreaders: c_int,   /* number of still-active workers */
    pub nextreader: c_int, /* next one to try to read from */
    pub reader: *mut *mut TupleQueueReader, /* array with nreaders active entries */
}

/* ----------------
 * GatherMergeState information
 *
 *		Gather merge nodes launch 1 or more parallel workers, run a
 *		subplan which produces sorted output in each worker, and then
 *		merge the results into a single sorted stream.
 * ----------------
 */

#[repr(C)]
pub struct GatherMergeState {
    pub ps: PlanState, /* its first field is NodeTag */
    pub initialized: bool, /* workers launched? */
    pub gm_initialized: bool, /* gather_merge_init() done? */
    pub need_to_scan_locally: bool, /* need to read from local plan? */
    pub tuples_needed: int64, /* tuple bound, see ExecSetTupleBound */
    /* these fields are set up once: */
    pub tupDesc: TupleDesc, /* descriptor for subplan result tuples */
    pub gm_nkeys: c_int,   /* number of sort columns */
    pub gm_sortkeys: SortSupport, /* array of length gm_nkeys */
    pub pei: *mut ParallelExecutorInfo,
    /* all remaining fields are reinitialized during a rescan */
    /* (but the arrays are not reallocated, just cleared) */
    pub nworkers_launched: c_int, /* original number of workers */
    pub nreaders: c_int,   /* number of active workers */
    pub gm_slots: *mut *mut TupleTableSlot, /* array with nreaders+1 entries */
    pub reader: *mut *mut TupleQueueReader, /* array with nreaders active entries */
    pub gm_tuple_buffers: *mut GMReaderTupleBuffer, /* nreaders tuple buffers */
    pub gm_heap: *mut binaryheap, /* binary heap of slot indices */
}

/* ----------------
 *	 Values displayed by EXPLAIN ANALYZE
 * ----------------
 */
#[repr(C)]
pub struct HashInstrumentation {
    pub nbuckets: c_int,          /* number of buckets at end of execution */
    pub nbuckets_original: c_int, /* planned number of buckets */
    pub nbatch: c_int,            /* number of batches at end of execution */
    pub nbatch_original: c_int,   /* planned number of batches */
    pub space_peak: Size,         /* peak memory usage in bytes */
}

/* ----------------
 *	 Shared memory container for per-worker hash information
 * ----------------
 */
#[repr(C)]
pub struct SharedHashInfo {
    pub num_workers: c_int,
    pub hinstrument: [HashInstrumentation; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/* ----------------
 *	 HashState information
 * ----------------
 */
#[repr(C)]
pub struct HashState {
    pub ps: PlanState, /* its first field is NodeTag */
    pub hashtable: HashJoinTable, /* hash table for the hashjoin */
    pub hash_expr: *mut ExprState, /* ExprState to get hash value */

    pub skew_hashfunction: *mut FmgrInfo, /* lookup data for skew hash function */
    pub skew_collation: Oid, /* collation to call skew_hashfunction with */

    /*
     * In a parallelized hash join, the leader retains a pointer to the
     * shared-memory stats area in its shared_info field, and then copies the
     * shared-memory info back to local storage before DSM shutdown.  The
     * shared_info field remains NULL in workers, or in non-parallel joins.
     */
    pub shared_info: *mut SharedHashInfo,

    /*
     * If we are collecting hash stats, this points to an initially-zeroed
     * collection area, which could be either local storage or in shared
     * memory; either way it's for just one process.
     */
    pub hinstrument: *mut HashInstrumentation,

    /* Parallel hash state. */
    pub parallel_state: *mut ParallelHashJoinState,
}

/* ----------------
 *	 SetOpState information
 *
 *		SetOp nodes support either sorted or hashed de-duplication.
 *		The sorted mode is a bit like MergeJoin, the hashed mode like Agg.
 * ----------------
 */
#[repr(C)]
pub struct SetOpStatePerInput {
    pub firstTupleSlot: *mut TupleTableSlot, /* first tuple of current group */
    pub numTuples: int64,  /* number of tuples in current group */
    pub nextTupleSlot: *mut TupleTableSlot, /* next input tuple, if already read */
    pub needGroup: bool,   /* do we need to load a new group? */
}

#[repr(C)]
pub struct SetOpState {
    pub ps: PlanState,    /* its first field is NodeTag */
    pub setop_done: bool, /* indicates completion of output scan */
    pub numOutput: int64, /* number of dups left to output */
    pub numCols: c_int,   /* number of grouping columns */

    /* these fields are used in SETOP_SORTED mode: */
    pub sortKeys: SortSupport, /* per-grouping-field sort data */
    pub leftInput: SetOpStatePerInput, /* current outer-relation input state */
    pub rightInput: SetOpStatePerInput, /* current inner-relation input state */
    pub need_init: bool, /* have we read the first tuples yet? */

    /* these fields are used in SETOP_HASHED mode: */
    pub eqfuncoids: *mut Oid, /* per-grouping-field equality fns */
    pub hashfunctions: *mut FmgrInfo, /* per-grouping-field hash fns */
    pub hashtable: TupleHashTable, /* hash table with one entry per group */
    pub tableContext: MemoryContext, /* memory context containing hash table */
    pub table_filled: bool, /* hash table filled yet? */
    pub hashiter: TupleHashIterator, /* for iterating through hash table */
}

/* ----------------
 *	 LockRowsState information
 *
 *		LockRows nodes are used to enforce FOR [KEY] UPDATE/SHARE locking.
 * ----------------
 */
#[repr(C)]
pub struct LockRowsState {
    pub ps: PlanState,        /* its first field is NodeTag */
    pub lr_arowMarks: *mut List, /* List of ExecAuxRowMarks */
    pub lr_epqstate: EPQState, /* for evaluating EvalPlanQual rechecks */
}

/* ----------------
 *	 LimitState information
 *
 *		Limit nodes are used to enforce LIMIT/OFFSET clauses.
 *		They just select the desired subrange of their subplan's output.
 *
 * offset is the number of initial tuples to skip (0 does nothing).
 * count is the number of tuples to return after skipping the offset tuples.
 * If no limit count was specified, count is undefined and noCount is true.
 * When lstate == LIMIT_INITIAL, offset/count/noCount haven't been set yet.
 * ----------------
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LimitStateCond {
    LIMIT_INITIAL,        /* initial state for LIMIT node */
    LIMIT_RESCAN,         /* rescan after recomputing parameters */
    LIMIT_EMPTY,          /* there are no returnable rows */
    LIMIT_INWINDOW,       /* have returned a row in the window */
    LIMIT_WINDOWEND_TIES, /* have returned a tied row */
    LIMIT_SUBPLANEOF,     /* at EOF of subplan (within window) */
    LIMIT_WINDOWEND,      /* stepped off end of window */
    LIMIT_WINDOWSTART,    /* stepped off beginning of window */
}
pub use LimitStateCond::*;

#[repr(C)]
pub struct LimitState {
    pub ps: PlanState, /* its first field is NodeTag */
    pub limitOffset: *mut ExprState, /* OFFSET parameter, or NULL if none */
    pub limitCount: *mut ExprState, /* COUNT parameter, or NULL if none */
    pub limitOption: LimitOption, /* limit specification type */
    pub offset: int64, /* current OFFSET value */
    pub count: int64,  /* current COUNT, if any */
    pub noCount: bool, /* if true, ignore count */
    pub lstate: LimitStateCond, /* state machine status, as above */
    pub position: int64, /* 1-based index of last tuple returned */
    pub subSlot: *mut TupleTableSlot, /* tuple last obtained from subplan */
    pub eqfunction: *mut ExprState, /* tuple equality qual in case of WITH TIES
                                     * option */
    pub last_slot: *mut TupleTableSlot, /* slot for evaluation of ties */
}
