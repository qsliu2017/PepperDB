//! LockRows node executor. Translated from
//! backend/executor/nodeLockRows.c (disposition: full leaf for M8, step 34).
//!
//! `ExecLockRows` sits over the scan/join subplan of a `SELECT ... FOR UPDATE/SHARE`.
//! For each row from the subplan it locks the source tuple(s) per the plan's row
//! marks (heap_lock_tuple via the table AM), then returns the row unchanged. On a
//! concurrent-update conflict PG runs EvalPlanQual to re-fetch the latest version;
//! that path is staged (the EPQ scaffolding exists in execMain). The common,
//! non-concurrent case -- lock the row, return it -- is complete.
//!
//! Row identity: PG reads the ctid via a junk Var; the port reads it off the subplan
//! slot's `tts_tid` (stamped by the scan). The lock buffer heap_lock_tuple pins is
//! released immediately (the lock persists on the tuple header / multixact).
//!
//! Async: heap_lock_tuple reaches the buffer pool, so `ExecLockRows` is `async`.

use std::sync::Arc;

use crate::access::tableam::TM_Result;
use crate::backend::access::heap::heapam_handler::heapam_tuple_lock;
use crate::backend::executor::execProcnode::{
    exec_end_node, exec_init_node, exec_proc_node, PlanStateNode,
};
use crate::nodes::execnodes::{EState, LockRowsState, PlanState, TupleTableSlot};
use crate::nodes::lockoptions::{LockClauseStrength, LockTupleMode, LockWaitPolicy};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::{LockRows, PlanRowMark, RowMarkType};
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;

/// A row mark resolved for execution: the plan mark + the borrowed relation it locks.
pub struct ExecRowMarkRun<'rel> {
    pub mark: PlanRowMark,
    pub relation: &'rel RelationData,
}

/// Run-state pairing the PG `LockRowsState` with its child plan-state and the
/// resolved row marks.
pub struct LockRowsRun<'rel> {
    pub state: Box<LockRowsState>,
    pub subplan: Box<PlanStateNode<'rel>>,
    /// the resolved (plan-mark + borrowed relation) locking marks.
    pub row_marks: Vec<ExecRowMarkRun<'rel>>,
    /// the node's own result slot (LockRows projects its child unchanged; the locked
    /// row is copied here so the return borrow is the node's, not the subplan's --
    /// avoids a cross-iteration subplan-slot borrow for SKIP LOCKED).
    pub result_slot: Box<TupleTableSlot>,
}

/// PG `ExecInitLockRows`: init the child subplan and resolve the row marks (each to
/// its borrowed relation from the EState range table). REFERENCE marks (non-locking)
/// are dropped; only locking marks remain.
pub fn exec_init_lock_rows<'rel>(
    node: &LockRows,
    estate: &mut EState<'rel>,
    eflags: i32,
) -> Box<LockRowsRun<'rel>> {
    let subplan_node = node
        .plan
        .lefttree
        .as_ref()
        .unwrap_or_else(|| unimplemented!("ExecInitLockRows: LockRows without a child subplan"));
    let subplan = exec_init_node(Some(subplan_node), estate, eflags)
        .unwrap_or_else(|| unimplemented!("ExecInitLockRows: null child subplan"));

    let row_marks: Vec<ExecRowMarkRun<'rel>> = node
        .row_marks
        .iter()
        .filter_map(|m| {
            let Node::PlanRowMark(rm) = m else {
                unimplemented!("ExecInitLockRows: row mark is not a PlanRowMark");
            };
            // Only locking marks (not ROW_MARK_REFERENCE / COPY) need a tuple lock.
            if matches!(rm.mark_type, RowMarkType::REFERENCE | RowMarkType::COPY) {
                return None;
            }
            let relation = estate
                .es_range_table_rels
                .get(rm.rti - 1)
                .copied()
                .flatten()
                .unwrap_or_else(|| unimplemented!("ExecInitLockRows: locked relation not registered for RTI"));
            Some(ExecRowMarkRun { mark: (**rm).clone(), relation })
        })
        .collect();

    let ps = PlanState { plan: Some(Node::LockRows(Box::new(node.clone()))), ..PlanState::default() };
    let state = Box::new(LockRowsState { ps, ..LockRowsState::default() });

    // The result slot is sized from the child's rowtype (LockRows projects it).
    let result_desc = crate::backend::executor::execProcnode::result_type_of(&subplan);
    let result_slot = crate::backend::executor::execTuples::make_tuple_table_slot(
        result_desc,
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );

    Box::new(LockRowsRun { state, subplan: Box::new(subplan), row_marks, result_slot })
}

/// PG `ExecLockRows`: pull the next row from the subplan, lock the source tuple per
/// each row mark, and return the row. `None` at end of subplan.
pub async fn exec_lock_rows<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut LockRowsRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();
    let shared = shared
        .unwrap_or_else(|| unimplemented!("ExecLockRows: a LockRows node requires a SharedState"));
    let cid = crate::backend::access::transam::xact::GetCurrentCommandId(false);

    // Split the disjoint field borrows: the subplan (yields the row), the row marks
    // (locked per row), and the node's own result slot (the locked row is copied here
    // so the return borrow is the node's -- the subplan slot is free to advance on a
    // SKIP LOCKED retry without a cross-iteration borrow).
    let LockRowsRun { subplan, row_marks, result_slot, .. } = run;

    // Loop so a SKIP LOCKED row that can't be locked advances to the next row.
    'rows: loop {
        // Snapshot the row (values + TID), releasing the subplan-slot borrow.
        let (tid, values, isnull) = {
            let slot = Box::pin(exec_proc_node(Some(shared), subplan)).await?;
            let n = slot.nvalid.max(0) as usize;
            (slot.tid, slot.values[..n].to_vec(), slot.isnull[..n].to_vec())
        };
        crate::assert!(tid.is_valid(), "ExecLockRows: subplan row has no TID");

        // Lock the source tuple per each (locking) row mark.
        for erm in row_marks.iter() {
            let lockmode = lock_tuple_mode_for(erm.mark.strength);
            let wait_policy = erm.mark.wait_policy;
            let mut tuple = crate::access::htup::HeapTupleData::null(tid, erm.relation.rd_id);
            tuple.t_self = tid;
            let (res, _tmfd, buffer) = heapam_tuple_lock(
                shared,
                erm.relation,
                &mut tuple,
                None,
                cid,
                lockmode,
                wait_policy,
                false,
            )
            .await;
            // Release the pinned buffer (the lock persists on the tuple).
            if buffer != crate::storage::buf::BufId::Invalid {
                shared.buffers().release_buffer(buffer);
            }
            match res {
                TM_Result::Ok | TM_Result::SelfModified => {}
                TM_Result::WouldBlock => {
                    if wait_policy == LockWaitPolicy::LockWaitSkip {
                        continue 'rows; // SKIP LOCKED: skip this row, fetch the next.
                    }
                    unimplemented!("ExecLockRows: NOWAIT lock conflict not yet reachable");
                }
                TM_Result::Updated | TM_Result::Deleted => {
                    // Concurrent update/delete -> EvalPlanQual re-fetch. The EPQ
                    // scaffolding exists (execMain); resolving the conflict is staged.
                    unimplemented!("ExecLockRows: concurrent update (TM_Updated/TM_Deleted) -> EPQ recheck not yet reachable");
                }
                other => unimplemented!("ExecLockRows: unexpected table_tuple_lock result {other:?}"),
            }
        }

        // All marks locked: copy the locked row into the node's result slot and yield
        // it (LockRows projects its child unchanged).
        crate::executor::tuptable::ExecClearTuple(result_slot);
        let n = values.len();
        if n > 0 {
            result_slot.values[..n].copy_from_slice(&values);
            result_slot.isnull[..n].copy_from_slice(&isnull);
        }
        result_slot.tid = tid;
        crate::backend::executor::execTuples::exec_store_virtual_tuple(result_slot);
        return Some(result_slot);
    }
}

/// PG `ExecEndLockRows`: tear down the child subplan.
pub fn exec_end_lock_rows(shared: Option<&Arc<SharedState>>, run: &mut LockRowsRun<'_>) {
    exec_end_node(shared, &mut run.subplan);
}

/// Map a `LockClauseStrength` to the heap `LockTupleMode` (PG's
/// `ExecUpdateLockMode`-style strength mapping in nodeLockRows / heapam).
fn lock_tuple_mode_for(strength: LockClauseStrength) -> LockTupleMode {
    match strength {
        LockClauseStrength::FORUPDATE => LockTupleMode::LockTupleExclusive,
        LockClauseStrength::FORNOKEYUPDATE => LockTupleMode::LockTupleNoKeyExclusive,
        LockClauseStrength::FORSHARE => LockTupleMode::LockTupleShare,
        LockClauseStrength::FORKEYSHARE => LockTupleMode::LockTupleKeyShare,
        LockClauseStrength::NONE => {
            unimplemented!("ExecLockRows: ROW_MARK_REFERENCE strength has no tuple lock mode")
        }
    }
}
