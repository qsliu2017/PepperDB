//! Translated from PostgreSQL src/include/foreign/fdwapi.h

use crate::access::htup::HeapTuple;
use crate::access::parallel::{shm_toc, ParallelContext};
use crate::commands::explain_state::ExplainState;
use crate::nodes::execnodes::{
    AsyncRequest, EState, ExecRowMark, ForeignScanState, ModifyTableState, ResultRelInfo,
};
use crate::nodes::lockoptions::LockClauseStrength;
use crate::nodes::nodes::JoinType;
use crate::nodes::parsenodes::{DropBehavior, ImportForeignSchemaStmt, RangeTblEntry};
use crate::nodes::pathnodes::{
    ForeignPath, JoinPathExtraData, Path, PlannerInfo, RelOptInfo, UpperRelationKind,
};
use crate::nodes::plannodes::{ForeignScan, ModifyTable, Plan, RowMarkType};
use crate::executor::tuptable::TupleTableSlot;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::utils::rel::Relation;
use crate::c::Index;

/// AcquireSampleRowsFunc: fills `rows`; returns count + totalrows/totaldeadrows
/// out-params (folded into the return tuple).
pub type AcquireSampleRowsFunc =
    fn(relation: Relation, elevel: i32, rows: &mut [HeapTuple], targrows: i32) -> (i32, f64, f64);

/// FdwRoutine: the struct of callbacks a foreign-data wrapper's handler returns.
/// Per routine-struct.md appendix B: a base trait, required scan callbacks as
/// non-defaulted methods, and the block-comment "Remaining functions are
/// optional ... NULL" group as default methods returning None / no-op.
/// The C `NodeTag type` field is dropped (the trait is its own discriminant).
#[allow(clippy::too_many_arguments)]
pub trait FdwRoutine {
    // --- Functions for scanning foreign tables (required) ---

    fn get_foreign_rel_size(
        &self,
        root: &mut PlannerInfo,
        baserel: &mut RelOptInfo,
        foreigntableid: Oid,
    );

    fn get_foreign_paths(
        &self,
        root: &mut PlannerInfo,
        baserel: &mut RelOptInfo,
        foreigntableid: Oid,
    );

    fn get_foreign_plan(
        &self,
        root: &mut PlannerInfo,
        baserel: &mut RelOptInfo,
        foreigntableid: Oid,
        best_path: &mut ForeignPath,
        tlist: Vec<crate::nodes::nodes::Node>,
        scan_clauses: Vec<crate::nodes::nodes::Node>,
        outer_plan: *mut Plan,
    ) -> *mut ForeignScan;

    fn begin_foreign_scan(&self, node: &mut ForeignScanState, eflags: i32);

    // IterateForeignScan returns NULL at end-of-scan -> Option.
    fn iterate_foreign_scan(&self, node: &mut ForeignScanState) -> Option<*mut TupleTableSlot>;

    fn re_scan_foreign_scan(&self, node: &mut ForeignScanState);

    fn end_foreign_scan(&self, node: &mut ForeignScanState);

    // --- Remaining functions are optional (NULL in C) -> default methods ---

    /// Functions for remote-join planning.
    fn get_foreign_join_paths(
        &self,
        _root: &mut PlannerInfo,
        _joinrel: &mut RelOptInfo,
        _outerrel: &mut RelOptInfo,
        _innerrel: &mut RelOptInfo,
        _jointype: JoinType,
        _extra: &mut JoinPathExtraData,
    ) {
    }

    /// Functions for remote upper-relation (post scan/join) planning.
    /// C `void *extra` -> opaque; left as raw ptr pending the .c body.
    fn get_foreign_upper_paths(
        &self,
        _root: &mut PlannerInfo,
        _stage: UpperRelationKind,
        _input_rel: &mut RelOptInfo,
        _output_rel: &mut RelOptInfo,
        _extra: *mut core::ffi::c_void, // TODO(ptr): stage-specific extra
    ) {
    }

    // --- Functions for updating foreign tables (optional) ---

    fn add_foreign_update_targets(
        &self,
        _root: &mut PlannerInfo,
        _rtindex: Index,
        _target_rte: &mut RangeTblEntry,
        _target_relation: Relation,
    ) {
    }

    fn plan_foreign_modify(
        &self,
        _root: &mut PlannerInfo,
        _plan: &mut ModifyTable,
        _result_relation: Index,
        _subplan_index: i32,
    ) -> Vec<crate::nodes::nodes::Node> {
        unimplemented!()
    }

    fn begin_foreign_modify(
        &self,
        _mtstate: &mut ModifyTableState,
        _rinfo: &mut ResultRelInfo,
        _fdw_private: Vec<crate::nodes::nodes::Node>,
        _subplan_index: i32,
        _eflags: i32,
    ) {
    }

    fn exec_foreign_insert(
        &self,
        _estate: &mut EState,
        _rinfo: &mut ResultRelInfo,
        _slot: &mut TupleTableSlot,
        _plan_slot: &mut TupleTableSlot,
    ) -> Option<*mut TupleTableSlot> {
        None
    }

    /// ExecForeignBatchInsert: `numSlots` in/out -> returns the written slots.
    fn exec_foreign_batch_insert(
        &self,
        _estate: &mut EState,
        _rinfo: &mut ResultRelInfo,
        _slots: &mut [*mut TupleTableSlot],
        _plan_slots: &mut [*mut TupleTableSlot],
        _num_slots: &mut i32,
    ) -> *mut *mut TupleTableSlot {
        unimplemented!()
    }

    fn get_foreign_modify_batch_size(&self, _rinfo: &mut ResultRelInfo) -> i32 {
        unimplemented!()
    }

    fn exec_foreign_update(
        &self,
        _estate: &mut EState,
        _rinfo: &mut ResultRelInfo,
        _slot: &mut TupleTableSlot,
        _plan_slot: &mut TupleTableSlot,
    ) -> Option<*mut TupleTableSlot> {
        None
    }

    fn exec_foreign_delete(
        &self,
        _estate: &mut EState,
        _rinfo: &mut ResultRelInfo,
        _slot: &mut TupleTableSlot,
        _plan_slot: &mut TupleTableSlot,
    ) -> Option<*mut TupleTableSlot> {
        None
    }

    fn end_foreign_modify(&self, _estate: &mut EState, _rinfo: &mut ResultRelInfo) {}

    fn begin_foreign_insert(&self, _mtstate: &mut ModifyTableState, _rinfo: &mut ResultRelInfo) {}

    fn end_foreign_insert(&self, _estate: &mut EState, _rinfo: &mut ResultRelInfo) {}

    fn is_foreign_rel_updatable(&self, _rel: Relation) -> i32 {
        0
    }

    fn plan_direct_modify(
        &self,
        _root: &mut PlannerInfo,
        _plan: &mut ModifyTable,
        _result_relation: Index,
        _subplan_index: i32,
    ) -> bool {
        false
    }

    fn begin_direct_modify(&self, _node: &mut ForeignScanState, _eflags: i32) {}

    fn iterate_direct_modify(&self, _node: &mut ForeignScanState) -> Option<*mut TupleTableSlot> {
        None
    }

    fn end_direct_modify(&self, _node: &mut ForeignScanState) {}

    // --- SELECT FOR UPDATE/SHARE row locking (optional) ---

    fn get_foreign_row_mark_type(
        &self,
        _rte: &mut RangeTblEntry,
        _strength: LockClauseStrength,
    ) -> RowMarkType {
        unimplemented!()
    }

    /// RefetchForeignRow: `updated` out-param -> folded into the return.
    fn refetch_foreign_row(
        &self,
        _estate: &mut EState,
        _erm: &mut ExecRowMark,
        _rowid: Datum,
        _slot: &mut TupleTableSlot,
    ) -> bool {
        unimplemented!()
    }

    fn recheck_foreign_scan(&self, _node: &mut ForeignScanState, _slot: &mut TupleTableSlot) -> bool {
        unimplemented!()
    }

    // --- Support functions for EXPLAIN (optional) ---

    fn explain_foreign_scan(&self, _node: &mut ForeignScanState, _es: &mut ExplainState) {}

    fn explain_foreign_modify(
        &self,
        _mtstate: &mut ModifyTableState,
        _rinfo: &mut ResultRelInfo,
        _fdw_private: Vec<crate::nodes::nodes::Node>,
        _subplan_index: i32,
        _es: &mut ExplainState,
    ) {
    }

    fn explain_direct_modify(&self, _node: &mut ForeignScanState, _es: &mut ExplainState) {}

    // --- Support functions for ANALYZE (optional) ---

    /// AnalyzeForeignTable: func/totalpages out-params; returns Some((func,pages))
    /// when the table can be analyzed, None otherwise.
    fn analyze_foreign_table(
        &self,
        _relation: Relation,
    ) -> Option<(AcquireSampleRowsFunc, BlockNumber)> {
        None
    }

    // --- Support functions for IMPORT FOREIGN SCHEMA (optional) ---

    fn import_foreign_schema(
        &self,
        _stmt: &mut ImportForeignSchemaStmt,
        _server_oid: Oid,
    ) -> Vec<crate::nodes::nodes::Node> {
        unimplemented!()
    }

    // --- Support functions for TRUNCATE (optional) ---

    fn exec_foreign_truncate(
        &self,
        _rels: Vec<crate::nodes::nodes::Node>,
        _behavior: DropBehavior,
        _restart_seqs: bool,
    ) {
    }

    // --- Support functions for parallelism under Gather node (optional) ---

    fn is_foreign_scan_parallel_safe(
        &self,
        _root: &mut PlannerInfo,
        _rel: &mut RelOptInfo,
        _rte: &mut RangeTblEntry,
    ) -> bool {
        false
    }

    fn estimate_dsm_foreign_scan(
        &self,
        _node: &mut ForeignScanState,
        _pcxt: &mut ParallelContext,
    ) -> usize {
        0
    }

    fn initialize_dsm_foreign_scan(
        &self,
        _node: &mut ForeignScanState,
        _pcxt: &mut ParallelContext,
        _coordinate: *mut core::ffi::c_void, // TODO(ptr): DSM coordinate
    ) {
    }

    fn re_initialize_dsm_foreign_scan(
        &self,
        _node: &mut ForeignScanState,
        _pcxt: &mut ParallelContext,
        _coordinate: *mut core::ffi::c_void, // TODO(ptr)
    ) {
    }

    fn initialize_worker_foreign_scan(
        &self,
        _node: &mut ForeignScanState,
        _toc: *mut shm_toc,
        _coordinate: *mut core::ffi::c_void, // TODO(ptr)
    ) {
    }

    fn shutdown_foreign_scan(&self, _node: &mut ForeignScanState) {}

    // --- Support functions for path reparameterization (optional) ---

    fn reparameterize_foreign_path_by_child(
        &self,
        _root: &mut PlannerInfo,
        _fdw_private: Vec<crate::nodes::nodes::Node>,
        _child_rel: &mut RelOptInfo,
    ) -> Vec<crate::nodes::nodes::Node> {
        unimplemented!()
    }

    // --- Support functions for asynchronous execution (optional) ---

    fn is_foreign_path_async_capable(&self, _path: &mut ForeignPath) -> bool {
        false
    }

    fn foreign_async_request(&self, _areq: &mut AsyncRequest) {}

    fn foreign_async_configure_wait(&self, _areq: &mut AsyncRequest) {}

    fn foreign_async_notify(&self, _areq: &mut AsyncRequest) {}
}

// Functions in foreign/foreign.c. The FdwRoutine vtable is open (extension FDWs),
// so these return a boxed trait object (fn-pointer fallback per routine-struct.md).
pub fn GetFdwRoutine(_fdwhandler: Oid) -> Box<dyn FdwRoutine> {
    unimplemented!()
}

pub fn GetForeignServerIdByRelId(_relid: Oid) -> Oid {
    unimplemented!()
}

pub fn GetFdwRoutineByServerId(_serverid: Oid) -> Box<dyn FdwRoutine> {
    unimplemented!()
}

pub fn GetFdwRoutineByRelId(_relid: Oid) -> Box<dyn FdwRoutine> {
    unimplemented!()
}

pub fn GetFdwRoutineForRelation(_relation: Relation, _makecopy: bool) -> Box<dyn FdwRoutine> {
    unimplemented!()
}

pub fn IsImportableForeignTable(
    _tablename: &str,
    _stmt: &mut ImportForeignSchemaStmt,
) -> bool {
    unimplemented!()
}

// GetExistingLocalJoinPath returns NULL if none found -> Option.
pub fn GetExistingLocalJoinPath(_joinrel: &mut RelOptInfo) -> Option<*mut Path> {
    unimplemented!()
}
