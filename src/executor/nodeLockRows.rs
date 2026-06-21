//! Routines to handle FOR UPDATE/FOR SHARE row locking
//!
//! src/backend/executor/nodeLockRows.c
//! src/include/executor/nodeLockRows.h
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! INTERFACE ROUTINES
//!     ExecLockRows        - fetch locked rows
//!     ExecInitLockRows    - initialize node and subnodes..
//!     ExecEndLockRows     - shutdown node and subnodes

use crate::prelude::*;

use std::ffi::c_int;

use crate::nodes::execnodes::{
    EState, ExecAuxRowMark, ExecRowMark, LockRowsState, PlanState, TupleTableSlot,
};
use crate::nodes::plannodes::LockRows;
use crate::nodes::plannodes::RowMarkType::{self, ROW_MARK_EXCLUSIVE, ROW_MARK_NOKEYEXCLUSIVE, ROW_MARK_SHARE, ROW_MARK_KEYSHARE};
use crate::storage::itemptr::{ItemPointer, ItemPointerData};
use crate::nodes::pg_list::{List, ListCell, NIL};
use crate::{castNode, foreach, current_cell, makeNode, IsA};

// ----------------------------------------------------------------
//      Local stubs for not-yet-ported dependencies
// ----------------------------------------------------------------

// access/tableam.h
const TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS: c_int = 1 << 0;
const TUPLE_LOCK_FLAG_FIND_LAST_VERSION: c_int = 1 << 1;

// LockTupleMode (utils/lockwaitpolicy.h / access/heapam.h)
type LockTupleMode = c_int;
const LockTupleKeyShare: LockTupleMode = 0;
const LockTupleShare: LockTupleMode = 1;
const LockTupleNoKeyExclusive: LockTupleMode = 2;
const LockTupleExclusive: LockTupleMode = 3;

// TM_Result (access/tableam.h)
type TM_Result = c_int;
const TM_Ok: TM_Result = 0;
const TM_Invisible: TM_Result = 1;
const TM_SelfModified: TM_Result = 2;
const TM_Updated: TM_Result = 3;
const TM_Deleted: TM_Result = 4;
const TM_WouldBlock: TM_Result = 6;

// RowMarkType (nodes/plannodes.h)

const RELKIND_FOREIGN_TABLE: c_char = b'f' as c_char;

const RTE_RELATION: c_int = 0;

// TM_FailureData (access/tableam.h)
#[repr(C)]
struct TM_FailureData {
    ctid: ItemPointerData,
    xmax: TransactionId,
    cmax: u32,
    traversed: bool,
}


unsafe fn CHECK_FOR_INTERRUPTS() {
    // TODO: miscadmin.h
}

unsafe fn outerPlanState(node: *mut PlanState) -> *mut PlanState {
    crate::nodes::execnodes::outerPlanState(node as _) as _
}

unsafe fn ExecProcNode(node: *mut PlanState) -> *mut TupleTableSlot {
    crate::executor::executor::ExecProcNode(node as _) as _
}

unsafe fn TupIsNull(slot: *mut TupleTableSlot) -> bool {
    crate::executor::tuptable::TupIsNull(slot as _) as _
}

unsafe fn EvalPlanQualEnd(epqstate: *mut core::ffi::c_void) {
    crate::executor::execMain::EvalPlanQualEnd(epqstate as _)
}

unsafe fn EvalPlanQualBegin(epqstate: *mut core::ffi::c_void) {
    crate::executor::execMain::EvalPlanQualBegin(epqstate as _)
}

unsafe fn EvalPlanQualSetSlot(epqstate: *mut core::ffi::c_void, slot: *mut TupleTableSlot) {
    crate::executor::executor::EvalPlanQualSetSlot(epqstate as _, slot as _)
}

unsafe fn EvalPlanQualNext(epqstate: *mut core::ffi::c_void) -> *mut TupleTableSlot {
    crate::executor::execMain::EvalPlanQualNext(epqstate as _) as _
}

unsafe fn EvalPlanQualSlot(
    epqstate: *mut core::ffi::c_void,
    relation: *mut core::ffi::c_void,
    rti: Index,
) -> *mut TupleTableSlot {
    crate::executor::execMain::EvalPlanQualSlot(epqstate as _, relation as _, rti as _) as _
}

unsafe fn EvalPlanQualInit(
    epqstate: *mut core::ffi::c_void,
    estate: *mut EState,
    plan: *mut core::ffi::c_void,
    arowmarks: *mut List,
    epqParam: c_int,
    resultRelations: *mut List,
) {
    crate::executor::execMain::EvalPlanQualInit(epqstate as _, estate as _, plan as _, arowmarks as _, epqParam as _, resultRelations as _)
}

unsafe fn ExecClearTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    crate::executor::tuptable::ExecClearTuple(slot as _) as _
}

unsafe fn ExecGetJunkAttribute(
    slot: *mut TupleTableSlot,
    attno: i16,
    isNull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: executor/executor.h
}

unsafe fn GetFdwRoutineForRelation(
    relation: *mut core::ffi::c_void,
    makecopy: bool,
) -> *mut FdwRoutine {
    crate::foreign::foreign::GetFdwRoutineForRelation(relation as _, makecopy as _) as _
}

unsafe fn IsolationUsesXactSnapshot() -> bool {
    unimplemented!() // TODO: access/xact.h
}

unsafe fn table_tuple_lock(
    rel: *mut core::ffi::c_void,
    tid: ItemPointer,
    snapshot: *mut core::ffi::c_void,
    slot: *mut TupleTableSlot,
    cid: u32,
    mode: LockTupleMode,
    wait_policy: c_int,
    flags: c_int,
    tmfd: *mut TM_FailureData,
) -> TM_Result {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn ExecInitResultTypeTL(planstate: *mut PlanState) {
    crate::executor::execTuples::ExecInitResultTypeTL(planstate as _)
}

unsafe fn ExecInitNode(
    node: *mut core::ffi::c_void,
    estate: *mut EState,
    eflags: c_int,
) -> *mut PlanState {
    crate::executor::execProcnode::ExecInitNode(node as _, estate as _, eflags as _) as _
}

unsafe fn ExecGetResultSlotOps(
    planstate: *mut PlanState,
    isfixed: *mut bool,
) -> *const core::ffi::c_void {
    unimplemented!() // TODO: executor/execUtils.c
}

unsafe fn ExecEndNode(node: *mut PlanState) {
    crate::executor::execProcnode::ExecEndNode(node as _)
}

unsafe fn ExecReScan(node: *mut PlanState) {
    crate::executor::execAmi::ExecReScan(node as _)
}

unsafe fn exec_rt_fetch(rti: Index, estate: *mut EState) -> *mut RangeTblEntry {
    crate::executor::executor::exec_rt_fetch(rti as _, estate as _) as _
}

unsafe fn ExecFindRowMark(estate: *mut EState, rti: Index, missing_ok: bool) -> *mut ExecRowMark {
    crate::executor::execMain::ExecFindRowMark(estate as _, rti as _, missing_ok as _) as _
}

unsafe fn ExecBuildAuxRowMark(
    erm: *mut ExecRowMark,
    targetlist: *mut List,
) -> *mut ExecAuxRowMark {
    crate::executor::execMain::ExecBuildAuxRowMark(erm as _, targetlist as _) as _
}

unsafe fn RowMarkRequiresRowShareLock(marktype: RowMarkType) -> bool {
    unimplemented!() // TODO: nodes/plannodes.h
}

unsafe fn lappend(list: *mut List, datum: *mut core::ffi::c_void) -> *mut List {
    unimplemented!() // TODO: nodes/pg_list.h
}

unsafe fn bms_is_member(x: c_int, a: *mut core::ffi::c_void) -> bool {
    crate::nodes::bitmapset::bms_is_member(x as _, a as _) as _
}

unsafe fn lfirst(lc: *mut ListCell) -> *mut core::ffi::c_void {
    crate::nodes::pg_list::lfirst(lc as _) as _
}

unsafe fn lfirst_node_PlanRowMark(lc: *mut ListCell) -> *mut PlanRowMark {
    unimplemented!() // TODO: nodes/pg_list.h
}

unsafe fn DatumGetObjectId(d: Datum) -> Oid {
    unimplemented!() // TODO: postgres.h
}

unsafe fn DatumGetPointer(d: Datum) -> *mut core::ffi::c_void {
    unimplemented!() // TODO: postgres.h
}

unsafe fn ItemPointerSetInvalid(p: *mut ItemPointerData) {
    crate::storage::itemptr::ItemPointerSetInvalid(p as _)
}

unsafe fn OidIsValid(oid: Oid) -> bool {
    oid != 0
}

unsafe fn RelationGetRelationName(rel: *mut core::ffi::c_void) -> *const c_char {
    unimplemented!() // TODO: utils/rel.h
}

// EXEC_FLAG_MARK (executor/executor.h)
const EXEC_FLAG_MARK: c_int = 0x0008;

// Opaque foreign / catalog / plannode structs used only via pointers here.
#[repr(C)]
struct FdwRoutine {
    // ... only RefetchForeignRow is touched here
    RefetchForeignRow: Option<
        unsafe extern "C" fn(
            estate: *mut EState,
            erm: *mut ExecRowMark,
            rowid: Datum,
            slot: *mut TupleTableSlot,
            updated: *mut bool,
        ),
    >,
}

#[repr(C)]
struct RangeTblEntry {
    rtekind: c_int,
    // ...
}

#[repr(C)]
struct PlanRowMark {
    rti: Index,
    prti: Index,
    isParent: bool,
    // ...
}

// ----------------------------------------------------------------
//      ExecLockRows
// ----------------------------------------------------------------
//	return: a tuple or NULL
unsafe fn ExecLockRows(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut LockRowsState = castNode!(LockRowsState, T_LockRowsState, pstate);
    let mut slot: *mut TupleTableSlot;
    let estate: *mut EState;
    let outerPlan: *mut PlanState;
    let mut epq_needed: bool;

    CHECK_FOR_INTERRUPTS();

    /*
     * get information from the node
     */
    estate = (*node).ps.state;
    outerPlan = outerPlanState(&mut (*node).ps);

    /*
     * Get next tuple from subplan, if any.
     */
    'lnext: loop {
        slot = ExecProcNode(outerPlan);

        if TupIsNull(slot) {
            /* Release any resources held by EPQ mechanism before exiting */
            EvalPlanQualEnd(&mut (*node).lr_epqstate as *mut _ as *mut core::ffi::c_void);
            return std::ptr::null_mut();
        }

        /* We don't need EvalPlanQual unless we get updated tuple version(s) */
        epq_needed = false;

        /*
         * Attempt to lock the source tuple(s).  (Note we only have locking
         * rowmarks in lr_arowMarks.)
         */
        let mut skip_to_lnext = false;
        foreach!(lc, (*node).lr_arowMarks, {
            let aerm: *mut ExecAuxRowMark = lfirst(current_cell!(lc)) as *mut ExecAuxRowMark;
            let erm: *mut ExecRowMark = (*aerm).rowmark;
            let mut datum: Datum;
            let mut isNull: bool = false;
            let tid: ItemPointerData;
            let mut tmfd: TM_FailureData = core::mem::zeroed();
            let lockmode: LockTupleMode;
            let mut lockflags: c_int = 0;
            let test: TM_Result;
            let markSlot: *mut TupleTableSlot;

            /* clear any leftover test tuple for this rel */
            markSlot = EvalPlanQualSlot(
                &mut (*node).lr_epqstate as *mut _ as *mut core::ffi::c_void,
                (*erm).relation as *mut core::ffi::c_void,
                (*erm).rti,
            );
            ExecClearTuple(markSlot);

            /* if child rel, must check whether it produced this row */
            if (*erm).rti != (*erm).prti {
                let tableoid: Oid;

                datum = ExecGetJunkAttribute(slot, (*aerm).toidAttNo, &mut isNull);
                /* shouldn't ever get a null result... */
                if isNull {
                    elog!(ERROR, "tableoid is NULL");
                }
                tableoid = DatumGetObjectId(datum);

                assert!(OidIsValid((*erm).relid));
                if tableoid != (*erm).relid {
                    /* this child is inactive right now */
                    (*erm).ermActive = false;
                    ItemPointerSetInvalid(&mut (*erm).curCtid as *mut _ as *mut ItemPointerData);
                    continue;
                }
            }
            (*erm).ermActive = true;

            /* fetch the tuple's ctid */
            datum = ExecGetJunkAttribute(slot, (*aerm).ctidAttNo, &mut isNull);
            /* shouldn't ever get a null result... */
            if isNull {
                elog!(ERROR, "ctid is NULL");
            }

            /* requests for foreign tables must be passed to their FDW */
            if (*(*(*erm).relation).rd_rel).relkind == RELKIND_FOREIGN_TABLE {
                let fdwroutine: *mut FdwRoutine;
                let mut updated: bool = false;

                fdwroutine =
                    GetFdwRoutineForRelation((*erm).relation as *mut core::ffi::c_void, false);
                /* this should have been checked already, but let's be safe */
                if (*fdwroutine).RefetchForeignRow.is_none() {
                    ereport!(
                        ERROR,
                        "cannot lock rows in foreign table"
                    );
                }

                ((*fdwroutine).RefetchForeignRow.unwrap())(
                    estate,
                    erm,
                    datum,
                    markSlot,
                    &mut updated,
                );
                if TupIsNull(markSlot) {
                    /* couldn't get the lock, so skip this row */
                    skip_to_lnext = true;
                    break;
                }

                /*
                 * if FDW says tuple was updated before getting locked, we need to
                 * perform EPQ testing to see if quals are still satisfied
                 */
                if updated {
                    epq_needed = true;
                }

                continue;
            }

            /* okay, try to lock (and fetch) the tuple */
            tid = *(DatumGetPointer(datum) as ItemPointer);
            match (*erm).markType {
                ROW_MARK_EXCLUSIVE => {
                    lockmode = LockTupleExclusive;
                }
                ROW_MARK_NOKEYEXCLUSIVE => {
                    lockmode = LockTupleNoKeyExclusive;
                }
                ROW_MARK_SHARE => {
                    lockmode = LockTupleShare;
                }
                ROW_MARK_KEYSHARE => {
                    lockmode = LockTupleKeyShare;
                }
                _ => {
                    elog!(ERROR, "unsupported rowmark type");
                    #[allow(unreachable_code)]
                    {
                        lockmode = LockTupleNoKeyExclusive; /* keep compiler quiet */
                    }
                }
            }

            lockflags = TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS;
            if !IsolationUsesXactSnapshot() {
                lockflags |= TUPLE_LOCK_FLAG_FIND_LAST_VERSION;
            }

            let mut tid_mut = tid;
            test = table_tuple_lock(
                (*erm).relation as *mut core::ffi::c_void,
                &mut tid_mut as *mut ItemPointerData,
                (*estate).es_snapshot as *mut core::ffi::c_void,
                markSlot,
                (*estate).es_output_cid,
                lockmode,
                (*erm).waitPolicy as c_int,
                lockflags,
                &mut tmfd,
            );

            match test {
                TM_WouldBlock => {
                    /* couldn't lock tuple in SKIP LOCKED mode */
                    skip_to_lnext = true;
                    break;
                }

                TM_SelfModified => {
                    /*
                     * The target tuple was already updated or deleted by the
                     * current command, or by a later command in the current
                     * transaction.  We *must* ignore the tuple in the former
                     * case, so as to avoid the "Halloween problem" of repeated
                     * update attempts.  In the latter case it might be sensible
                     * to fetch the updated tuple instead, but doing so would
                     * require changing heap_update and heap_delete to not
                     * complain about updating "invisible" tuples, which seems
                     * pretty scary (table_tuple_lock will not complain, but few
                     * callers expect TM_Invisible, and we're not one of them). So
                     * for now, treat the tuple as deleted and do not process.
                     */
                    skip_to_lnext = true;
                    break;
                }

                TM_Ok => {
                    /*
                     * Got the lock successfully, the locked tuple saved in
                     * markSlot for, if needed, EvalPlanQual testing below.
                     */
                    if tmfd.traversed {
                        epq_needed = true;
                    }
                }

                TM_Updated => {
                    if IsolationUsesXactSnapshot() {
                        ereport!(
                            ERROR,
                            "could not serialize access due to concurrent update"
                        );
                    }
                    elog!(ERROR, "unexpected table_tuple_lock status: {}", test);
                }

                TM_Deleted => {
                    if IsolationUsesXactSnapshot() {
                        ereport!(
                            ERROR,
                            "could not serialize access due to concurrent update"
                        );
                    }
                    /* tuple was deleted so don't return it */
                    skip_to_lnext = true;
                    break;
                }

                TM_Invisible => {
                    elog!(ERROR, "attempted to lock invisible tuple");
                }

                _ => {
                    elog!(ERROR, "unrecognized table_tuple_lock status: {}", test);
                }
            }

            /* Remember locked tuple's TID for EPQ testing and WHERE CURRENT OF */
            (*erm).curCtid = tid;
        });

        if skip_to_lnext {
            continue 'lnext;
        }

        /*
         * If we need to do EvalPlanQual testing, do so.
         */
        if epq_needed {
            /* Initialize EPQ machinery */
            EvalPlanQualBegin(&mut (*node).lr_epqstate as *mut _ as *mut core::ffi::c_void);

            /*
             * To fetch non-locked source rows the EPQ logic needs to access junk
             * columns from the tuple being tested.
             */
            EvalPlanQualSetSlot(
                &mut (*node).lr_epqstate as *mut _ as *mut core::ffi::c_void,
                slot,
            );

            /*
             * And finally we can re-evaluate the tuple.
             */
            slot = EvalPlanQualNext(&mut (*node).lr_epqstate as *mut _ as *mut core::ffi::c_void);
            if TupIsNull(slot) {
                /* Updated tuple fails qual, so ignore it and go on */
                continue 'lnext;
            }
        }

        /* Got all locks, so return the current tuple */
        return slot;
    }
}

// ----------------------------------------------------------------
//      ExecInitLockRows
//
//      This initializes the LockRows node state structures and
//      the node's subplan.
// ----------------------------------------------------------------
pub unsafe fn ExecInitLockRows(
    node: *mut LockRows,
    estate: *mut EState,
    eflags: c_int,
) -> *mut LockRowsState {
    let lrstate: *mut LockRowsState;
    let outerPlan: *mut core::ffi::c_void = outerPlan_of_plan(node);
    let mut epq_arowmarks: *mut List;

    /* check for unsupported flags */
    assert!((eflags & EXEC_FLAG_MARK) == 0);

    /*
     * create state structure
     */
    lrstate = makeNode!(LockRowsState, T_LockRowsState);
    (*lrstate).ps.plan = node as *mut _;
    (*lrstate).ps.state = estate;
    (*lrstate).ps.ExecProcNode = Some(ExecLockRows);

    /*
     * Miscellaneous initialization
     *
     * LockRows nodes never call ExecQual or ExecProject, therefore no
     * ExprContext is needed.
     */

    /*
     * Initialize result type.
     */
    ExecInitResultTypeTL(&mut (*lrstate).ps);

    /*
     * then initialize outer plan
     */
    let outer_ps = ExecInitNode(outerPlan, estate, eflags);
    set_outerPlanState(&mut (*lrstate).ps, outer_ps);

    /* node returns unmodified slots from the outer plan */
    (*lrstate).ps.resultopsset = true;
    (*lrstate).ps.resultops = ExecGetResultSlotOps(
        outerPlanState(&mut (*lrstate).ps),
        &mut (*lrstate).ps.resultopsfixed,
    ) as *const _;

    /*
     * LockRows nodes do no projections, so initialize projection info for
     * this node appropriately
     */
    (*lrstate).ps.ps_ProjInfo = std::ptr::null_mut();

    /*
     * Locate the ExecRowMark(s) that this node is responsible for, and
     * construct ExecAuxRowMarks for them.  (InitPlan should already have
     * built the global list of ExecRowMarks.)
     */
    (*lrstate).lr_arowMarks = NIL;
    epq_arowmarks = NIL;
    foreach!(lc, (*node).rowMarks, {
        let rc: *mut PlanRowMark = lfirst_node_PlanRowMark(current_cell!(lc));
        let rte: *mut RangeTblEntry = exec_rt_fetch((*rc).rti, estate);
        let erm: *mut ExecRowMark;
        let aerm: *mut ExecAuxRowMark;

        /* ignore "parent" rowmarks; they are irrelevant at runtime */
        if (*rc).isParent {
            continue;
        }

        /*
         * Also ignore rowmarks belonging to child tables that have been
         * pruned in ExecDoInitialPruning().
         */
        if (*rte).rtekind == RTE_RELATION
            && !bms_is_member(
                (*rc).rti as c_int,
                (*estate).es_unpruned_relids as *mut core::ffi::c_void,
            )
        {
            continue;
        }

        /* find ExecRowMark and build ExecAuxRowMark */
        erm = ExecFindRowMark(estate, (*rc).rti, false);
        aerm = ExecBuildAuxRowMark(erm, plan_targetlist(outerPlan));

        /*
         * Only locking rowmarks go into our own list.  Non-locking marks are
         * passed off to the EvalPlanQual machinery.  This is because we don't
         * want to bother fetching non-locked rows unless we actually have to
         * do an EPQ recheck.
         */
        if RowMarkRequiresRowShareLock((*erm).markType) {
            (*lrstate).lr_arowMarks =
                lappend((*lrstate).lr_arowMarks, aerm as *mut core::ffi::c_void);
        } else {
            epq_arowmarks = lappend(epq_arowmarks, aerm as *mut core::ffi::c_void);
        }
    });

    /* Now we have the info needed to set up EPQ state */
    EvalPlanQualInit(
        &mut (*lrstate).lr_epqstate as *mut _ as *mut core::ffi::c_void,
        estate,
        outerPlan,
        epq_arowmarks,
        (*node).epqParam,
        NIL,
    );

    lrstate
}

// ----------------------------------------------------------------
//      ExecEndLockRows
//
//      This shuts down the subplan and frees resources allocated
//      to this node.
// ----------------------------------------------------------------
pub unsafe fn ExecEndLockRows(node: *mut LockRowsState) {
    /* We may have shut down EPQ already, but no harm in another call */
    EvalPlanQualEnd(&mut (*node).lr_epqstate as *mut _ as *mut core::ffi::c_void);
    ExecEndNode(outerPlanState(&mut (*node).ps));
}

pub unsafe fn ExecReScanLockRows(node: *mut LockRowsState) {
    let outerPlan: *mut PlanState = outerPlanState(&mut (*node).ps);

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
}

// ----------------------------------------------------------------
//      additional local stubs needing field access on not-yet-stable types
// ----------------------------------------------------------------

unsafe fn outerPlan_of_plan(node: *mut LockRows) -> *mut core::ffi::c_void {
    unimplemented!() // TODO: nodes/plannodes.h (outerPlan macro)
}

unsafe fn plan_targetlist(plan: *mut core::ffi::c_void) -> *mut List {
    unimplemented!() // TODO: nodes/plannodes.h (Plan.targetlist)
}

unsafe fn set_outerPlanState(parent: *mut PlanState, child: *mut PlanState) {
    unimplemented!() // TODO: nodes/execnodes.h (outerPlanState assignment)
}
