//! foreign/fdwapi.h - API for foreign-data wrappers.

use std::ffi::c_void;

use crate::c::{Index, Size};
use crate::nodes::pg_list::List;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// ---------------------------------------------------------------------------
// Referenced-but-not-yet-ported types. All used only behind pointers in these
// signatures, so opaque c_void aliases are sufficient.
// TODO: dedup when the owning headers land.
// ---------------------------------------------------------------------------

// TODO: dedup when nodes/nodes.h lands (NodeTag).
pub type NodeTag = std::ffi::c_int;

// TODO: dedup when nodes/pathnodes.h lands.
pub type PlannerInfo = c_void;
pub type RelOptInfo = c_void;
pub type ForeignPath = c_void;
pub type JoinPathExtraData = c_void;
pub type Path = c_void;

// TODO: dedup when nodes/plannodes.h lands.
pub type ForeignScan = c_void;
pub type Plan = c_void;
pub type ModifyTable = c_void;

// TODO: dedup when nodes/execnodes.h lands.
pub type ForeignScanState = c_void;
pub type ModifyTableState = c_void;
pub type ResultRelInfo = c_void;
pub type EState = c_void;
pub type ExecRowMark = c_void;
pub type AsyncRequest = c_void;

// TODO: dedup when executor/tuptable.h lands.
pub type TupleTableSlot = c_void;

// TODO: dedup when nodes/parsenodes.h lands.
pub type RangeTblEntry = c_void;
pub type ImportForeignSchemaStmt = c_void;

// TODO: dedup when utils/rel.h lands (Relation).
pub type Relation = *mut c_void;

// TODO: dedup when access/htup.h lands (HeapTuple).
pub type HeapTuple = *mut c_void;

// TODO: dedup when storage/block.h lands (BlockNumber).
pub type BlockNumber = u32;

// TODO: dedup when access/parallel.h / storage/shm_toc.h land.
pub type ParallelContext = c_void;
pub type shm_toc = c_void;

// TODO: dedup when nodes/nodes.h lands (JoinType).
pub type JoinType = std::ffi::c_int;

// TODO: dedup when nodes/pathnodes.h lands (UpperRelationKind).
pub type UpperRelationKind = std::ffi::c_int;

// TODO: dedup when nodes/lockoptions.h lands (RowMarkType, LockClauseStrength).
pub type RowMarkType = std::ffi::c_int;
pub type LockClauseStrength = std::ffi::c_int;

// TODO: dedup when nodes/parsenodes.h lands (DropBehavior).
pub type DropBehavior = std::ffi::c_int;

// To avoid including explain.h here, reference ExplainState thus.
// TODO: dedup when commands/explain.h lands (ExplainState).
pub type ExplainState = c_void;

// ---------------------------------------------------------------------------
// Callback function signatures --- see fdwhandler.sgml for more info.
// ---------------------------------------------------------------------------

pub type GetForeignRelSize_function =
    Option<unsafe extern "C" fn(root: *mut PlannerInfo, baserel: *mut RelOptInfo, foreigntableid: Oid)>;

pub type GetForeignPaths_function =
    Option<unsafe extern "C" fn(root: *mut PlannerInfo, baserel: *mut RelOptInfo, foreigntableid: Oid)>;

pub type GetForeignPlan_function = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        baserel: *mut RelOptInfo,
        foreigntableid: Oid,
        best_path: *mut ForeignPath,
        tlist: *mut List,
        scan_clauses: *mut List,
        outer_plan: *mut Plan,
    ) -> *mut ForeignScan,
>;

pub type BeginForeignScan_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState, eflags: std::ffi::c_int)>;

pub type IterateForeignScan_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState) -> *mut TupleTableSlot>;

pub type RecheckForeignScan_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState, slot: *mut TupleTableSlot) -> bool>;

pub type ReScanForeignScan_function = Option<unsafe extern "C" fn(node: *mut ForeignScanState)>;

pub type EndForeignScan_function = Option<unsafe extern "C" fn(node: *mut ForeignScanState)>;

pub type GetForeignJoinPaths_function = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        joinrel: *mut RelOptInfo,
        outerrel: *mut RelOptInfo,
        innerrel: *mut RelOptInfo,
        jointype: JoinType,
        extra: *mut JoinPathExtraData,
    ),
>;

pub type GetForeignUpperPaths_function = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        stage: UpperRelationKind,
        input_rel: *mut RelOptInfo,
        output_rel: *mut RelOptInfo,
        extra: *mut c_void,
    ),
>;

pub type AddForeignUpdateTargets_function = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        rtindex: Index,
        target_rte: *mut RangeTblEntry,
        target_relation: Relation,
    ),
>;

pub type PlanForeignModify_function = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        plan: *mut ModifyTable,
        resultRelation: Index,
        subplan_index: std::ffi::c_int,
    ) -> *mut List,
>;

pub type BeginForeignModify_function = Option<
    unsafe extern "C" fn(
        mtstate: *mut ModifyTableState,
        rinfo: *mut ResultRelInfo,
        fdw_private: *mut List,
        subplan_index: std::ffi::c_int,
        eflags: std::ffi::c_int,
    ),
>;

pub type ExecForeignInsert_function = Option<
    unsafe extern "C" fn(
        estate: *mut EState,
        rinfo: *mut ResultRelInfo,
        slot: *mut TupleTableSlot,
        planSlot: *mut TupleTableSlot,
    ) -> *mut TupleTableSlot,
>;

pub type ExecForeignBatchInsert_function = Option<
    unsafe extern "C" fn(
        estate: *mut EState,
        rinfo: *mut ResultRelInfo,
        slots: *mut *mut TupleTableSlot,
        planSlots: *mut *mut TupleTableSlot,
        numSlots: *mut std::ffi::c_int,
    ) -> *mut *mut TupleTableSlot,
>;

pub type GetForeignModifyBatchSize_function =
    Option<unsafe extern "C" fn(rinfo: *mut ResultRelInfo) -> std::ffi::c_int>;

pub type ExecForeignUpdate_function = Option<
    unsafe extern "C" fn(
        estate: *mut EState,
        rinfo: *mut ResultRelInfo,
        slot: *mut TupleTableSlot,
        planSlot: *mut TupleTableSlot,
    ) -> *mut TupleTableSlot,
>;

pub type ExecForeignDelete_function = Option<
    unsafe extern "C" fn(
        estate: *mut EState,
        rinfo: *mut ResultRelInfo,
        slot: *mut TupleTableSlot,
        planSlot: *mut TupleTableSlot,
    ) -> *mut TupleTableSlot,
>;

pub type EndForeignModify_function =
    Option<unsafe extern "C" fn(estate: *mut EState, rinfo: *mut ResultRelInfo)>;

pub type BeginForeignInsert_function =
    Option<unsafe extern "C" fn(mtstate: *mut ModifyTableState, rinfo: *mut ResultRelInfo)>;

pub type EndForeignInsert_function =
    Option<unsafe extern "C" fn(estate: *mut EState, rinfo: *mut ResultRelInfo)>;

pub type IsForeignRelUpdatable_function =
    Option<unsafe extern "C" fn(rel: Relation) -> std::ffi::c_int>;

pub type PlanDirectModify_function = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        plan: *mut ModifyTable,
        resultRelation: Index,
        subplan_index: std::ffi::c_int,
    ) -> bool,
>;

pub type BeginDirectModify_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState, eflags: std::ffi::c_int)>;

pub type IterateDirectModify_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState) -> *mut TupleTableSlot>;

pub type EndDirectModify_function = Option<unsafe extern "C" fn(node: *mut ForeignScanState)>;

pub type GetForeignRowMarkType_function = Option<
    unsafe extern "C" fn(rte: *mut RangeTblEntry, strength: LockClauseStrength) -> RowMarkType,
>;

pub type RefetchForeignRow_function = Option<
    unsafe extern "C" fn(
        estate: *mut EState,
        erm: *mut ExecRowMark,
        rowid: Datum,
        slot: *mut TupleTableSlot,
        updated: *mut bool,
    ),
>;

pub type ExplainForeignScan_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState, es: *mut ExplainState)>;

pub type ExplainForeignModify_function = Option<
    unsafe extern "C" fn(
        mtstate: *mut ModifyTableState,
        rinfo: *mut ResultRelInfo,
        fdw_private: *mut List,
        subplan_index: std::ffi::c_int,
        es: *mut ExplainState,
    ),
>;

pub type ExplainDirectModify_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState, es: *mut ExplainState)>;

pub type AcquireSampleRowsFunc = Option<
    unsafe extern "C" fn(
        relation: Relation,
        elevel: std::ffi::c_int,
        rows: *mut HeapTuple,
        targrows: std::ffi::c_int,
        totalrows: *mut f64,
        totaldeadrows: *mut f64,
    ) -> std::ffi::c_int,
>;

pub type AnalyzeForeignTable_function = Option<
    unsafe extern "C" fn(
        relation: Relation,
        func: *mut AcquireSampleRowsFunc,
        totalpages: *mut BlockNumber,
    ) -> bool,
>;

pub type ImportForeignSchema_function = Option<
    unsafe extern "C" fn(stmt: *mut ImportForeignSchemaStmt, serverOid: Oid) -> *mut List,
>;

pub type ExecForeignTruncate_function =
    Option<unsafe extern "C" fn(rels: *mut List, behavior: DropBehavior, restart_seqs: bool)>;

pub type EstimateDSMForeignScan_function =
    Option<unsafe extern "C" fn(node: *mut ForeignScanState, pcxt: *mut ParallelContext) -> Size>;

pub type InitializeDSMForeignScan_function = Option<
    unsafe extern "C" fn(node: *mut ForeignScanState, pcxt: *mut ParallelContext, coordinate: *mut c_void),
>;

pub type ReInitializeDSMForeignScan_function = Option<
    unsafe extern "C" fn(node: *mut ForeignScanState, pcxt: *mut ParallelContext, coordinate: *mut c_void),
>;

pub type InitializeWorkerForeignScan_function = Option<
    unsafe extern "C" fn(node: *mut ForeignScanState, toc: *mut shm_toc, coordinate: *mut c_void),
>;

pub type ShutdownForeignScan_function = Option<unsafe extern "C" fn(node: *mut ForeignScanState)>;

pub type IsForeignScanParallelSafe_function = Option<
    unsafe extern "C" fn(root: *mut PlannerInfo, rel: *mut RelOptInfo, rte: *mut RangeTblEntry) -> bool,
>;

pub type ReparameterizeForeignPathByChild_function = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        fdw_private: *mut List,
        child_rel: *mut RelOptInfo,
    ) -> *mut List,
>;

pub type IsForeignPathAsyncCapable_function =
    Option<unsafe extern "C" fn(path: *mut ForeignPath) -> bool>;

pub type ForeignAsyncRequest_function = Option<unsafe extern "C" fn(areq: *mut AsyncRequest)>;

pub type ForeignAsyncConfigureWait_function = Option<unsafe extern "C" fn(areq: *mut AsyncRequest)>;

pub type ForeignAsyncNotify_function = Option<unsafe extern "C" fn(areq: *mut AsyncRequest)>;

/// FdwRoutine is the struct returned by a foreign-data wrapper's handler
/// function.  It provides pointers to the callback functions needed by the
/// planner and executor.
///
/// More function pointers are likely to be added in the future.  Therefore
/// it's recommended that the handler initialize the struct with
/// makeNode(FdwRoutine) so that all fields are set to NULL.  This will
/// ensure that no fields are accidentally left undefined.
#[repr(C)]
pub struct FdwRoutine {
    pub type_: NodeTag,

    // Functions for scanning foreign tables
    pub GetForeignRelSize: GetForeignRelSize_function,
    pub GetForeignPaths: GetForeignPaths_function,
    pub GetForeignPlan: GetForeignPlan_function,
    pub BeginForeignScan: BeginForeignScan_function,
    pub IterateForeignScan: IterateForeignScan_function,
    pub ReScanForeignScan: ReScanForeignScan_function,
    pub EndForeignScan: EndForeignScan_function,

    // Remaining functions are optional. Set the pointer to NULL for any that
    // are not provided.

    // Functions for remote-join planning
    pub GetForeignJoinPaths: GetForeignJoinPaths_function,

    // Functions for remote upper-relation (post scan/join) planning
    pub GetForeignUpperPaths: GetForeignUpperPaths_function,

    // Functions for updating foreign tables
    pub AddForeignUpdateTargets: AddForeignUpdateTargets_function,
    pub PlanForeignModify: PlanForeignModify_function,
    pub BeginForeignModify: BeginForeignModify_function,
    pub ExecForeignInsert: ExecForeignInsert_function,
    pub ExecForeignBatchInsert: ExecForeignBatchInsert_function,
    pub GetForeignModifyBatchSize: GetForeignModifyBatchSize_function,
    pub ExecForeignUpdate: ExecForeignUpdate_function,
    pub ExecForeignDelete: ExecForeignDelete_function,
    pub EndForeignModify: EndForeignModify_function,
    pub BeginForeignInsert: BeginForeignInsert_function,
    pub EndForeignInsert: EndForeignInsert_function,
    pub IsForeignRelUpdatable: IsForeignRelUpdatable_function,
    pub PlanDirectModify: PlanDirectModify_function,
    pub BeginDirectModify: BeginDirectModify_function,
    pub IterateDirectModify: IterateDirectModify_function,
    pub EndDirectModify: EndDirectModify_function,

    // Functions for SELECT FOR UPDATE/SHARE row locking
    pub GetForeignRowMarkType: GetForeignRowMarkType_function,
    pub RefetchForeignRow: RefetchForeignRow_function,
    pub RecheckForeignScan: RecheckForeignScan_function,

    // Support functions for EXPLAIN
    pub ExplainForeignScan: ExplainForeignScan_function,
    pub ExplainForeignModify: ExplainForeignModify_function,
    pub ExplainDirectModify: ExplainDirectModify_function,

    // Support functions for ANALYZE
    pub AnalyzeForeignTable: AnalyzeForeignTable_function,

    // Support functions for IMPORT FOREIGN SCHEMA
    pub ImportForeignSchema: ImportForeignSchema_function,

    // Support functions for TRUNCATE
    pub ExecForeignTruncate: ExecForeignTruncate_function,

    // Support functions for parallelism under Gather node
    pub IsForeignScanParallelSafe: IsForeignScanParallelSafe_function,
    pub EstimateDSMForeignScan: EstimateDSMForeignScan_function,
    pub InitializeDSMForeignScan: InitializeDSMForeignScan_function,
    pub ReInitializeDSMForeignScan: ReInitializeDSMForeignScan_function,
    pub InitializeWorkerForeignScan: InitializeWorkerForeignScan_function,
    pub ShutdownForeignScan: ShutdownForeignScan_function,

    // Support functions for path reparameterization.
    pub ReparameterizeForeignPathByChild: ReparameterizeForeignPathByChild_function,

    // Support functions for asynchronous execution
    pub IsForeignPathAsyncCapable: IsForeignPathAsyncCapable_function,
    pub ForeignAsyncRequest: ForeignAsyncRequest_function,
    pub ForeignAsyncConfigureWait: ForeignAsyncConfigureWait_function,
    pub ForeignAsyncNotify: ForeignAsyncNotify_function,
}

// ---------------------------------------------------------------------------
// Functions in foreign/foreign.c
// ---------------------------------------------------------------------------

pub unsafe fn GetFdwRoutine(fdwhandler: Oid) -> *mut FdwRoutine {
    unimplemented!()
}

pub unsafe fn GetForeignServerIdByRelId(relid: Oid) -> Oid {
    unimplemented!()
}

pub unsafe fn GetFdwRoutineByServerId(serverid: Oid) -> *mut FdwRoutine {
    unimplemented!()
}

pub unsafe fn GetFdwRoutineByRelId(relid: Oid) -> *mut FdwRoutine {
    unimplemented!()
}

pub unsafe fn GetFdwRoutineForRelation(relation: Relation, makecopy: bool) -> *mut FdwRoutine {
    unimplemented!()
}

pub unsafe fn IsImportableForeignTable(
    tablename: *const std::ffi::c_char,
    stmt: *mut ImportForeignSchemaStmt,
) -> bool {
    unimplemented!()
}

pub unsafe fn GetExistingLocalJoinPath(joinrel: *mut RelOptInfo) -> *mut Path {
    unimplemented!()
}
