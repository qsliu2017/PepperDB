//! src/backend/executor/execProcnode.c
//!
//! contains dispatch functions which call the appropriate "initialize",
//! "get a tuple", and "cleanup" routines for the given node type.
//! If the node has children, then it will presumably call ExecInitNode,
//! ExecProcNode, or ExecEndNode on its subnodes and do the appropriate
//! processing.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/executor/execProcnode.c

/*
 *	 NOTES
 *		This used to be three files.  It is now all combined into
 *		one file so that it is easier to keep the dispatch routines
 *		in sync when new nodes are added.
 *
 *	 EXAMPLE
 *		Suppose we want the age of the manager of the shoe department and
 *		the number of employees in that department.  So we have the query:
 *
 *				select DEPT.no_emps, EMP.age
 *				from DEPT, EMP
 *				where EMP.name = DEPT.mgr and
 *					  DEPT.name = "shoe"
 *
 *		Suppose the planner gives us the following plan:
 *
 *						Nest Loop (DEPT.mgr = EMP.name)
 *						/		\
 *					   /		 \
 *				   Seq Scan		Seq Scan
 *					DEPT		  EMP
 *				(name = "shoe")
 *
 *		ExecutorStart() is called first.
 *		It calls InitPlan() which calls ExecInitNode() on
 *		the root of the plan -- the nest loop node.
 *
 *	  * ExecInitNode() notices that it is looking at a nest loop and
 *		as the code below demonstrates, it calls ExecInitNestLoop().
 *		Eventually this calls ExecInitNode() on the right and left subplans
 *		and so forth until the entire plan is initialized.  The result
 *		of ExecInitNode() is a plan state tree built with the same structure
 *		as the underlying plan tree.
 *
 *	  * Then when ExecutorRun() is called, it calls ExecutePlan() which calls
 *		ExecProcNode() repeatedly on the top node of the plan state tree.
 *		Each time this happens, ExecProcNode() will end up calling
 *		ExecNestLoop(), which calls ExecProcNode() on its subplans.
 *		Each of these subplans is a sequential scan so ExecSeqScan() is
 *		called.  The slots returned by ExecSeqScan() may contain
 *		tuples which contain the attributes ExecNestLoop() uses to
 *		form the tuples it returns.
 *
 *	  * Eventually ExecSeqScan() stops returning tuples and the nest
 *		loop join ends.  Lastly, ExecutorEnd() calls ExecEndNode() which
 *		calls ExecEndNestLoop() which in turn calls ExecEndNode() on
 *		its subplans which result in ExecEndSeqScan().
 *
 *		This should show how the executor works by having
 *		ExecInitNode(), ExecProcNode() and ExecEndNode() dispatch
 *		their work to the appropriate node support routines which may
 *		in turn call these routines themselves on their subplans.
 */

use crate::prelude::*;

use std::ffi::c_int;

use crate::nodes::execnodes::*;
use crate::nodes::nodes::*;
use crate::nodes::pg_list::*;
use crate::nodes::plannodes::*;
use crate::nodes::primnodes::*;
use crate::nodes::bitmapset::Bitmapset;
use crate::{castNode, foreach, current_cell, IsA};

/* ------------------------------------------------------------------------
 *		ExecInitNode
 *
 *		Recursively initializes all the nodes in the plan tree rooted
 *		at 'node'.
 *
 *		Inputs:
 *		  'node' is the current node of the plan produced by the query planner
 *		  'estate' is the shared execution state for the plan tree
 *		  'eflags' is a bitwise OR of flag bits described in executor.h
 *
 *		Returns a PlanState node corresponding to the given Plan node.
 * ------------------------------------------------------------------------
 */
pub unsafe fn ExecInitNode(
    node: *mut Plan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut PlanState {
    let result: *mut PlanState;
    let mut subps: *mut List;

    /*
     * do nothing when we get to the end of a leaf on tree.
     */
    if node.is_null() {
        return std::ptr::null_mut();
    }

    /*
     * Make sure there's enough stack available. Need to check here, in
     * addition to ExecProcNode() (via ExecProcNodeFirst()), to ensure the
     * stack isn't overrun while initializing the node tree.
     */
    check_stack_depth();

    match nodeTag(node as *const Node) {
        /*
         * control nodes
         */
        NodeTag::T_Result => {
            result = ExecInitResult(node as *mut Result, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_ProjectSet => {
            result =
                ExecInitProjectSet(node as *mut ProjectSet, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_ModifyTable => {
            result =
                ExecInitModifyTable(node as *mut ModifyTable, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_Append => {
            result = ExecInitAppend(node as *mut Append, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_MergeAppend => {
            result =
                ExecInitMergeAppend(node as *mut MergeAppend, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_RecursiveUnion => {
            result = ExecInitRecursiveUnion(node as *mut RecursiveUnion, estate, eflags)
                as *mut PlanState;
        }

        NodeTag::T_BitmapAnd => {
            result =
                ExecInitBitmapAnd(node as *mut BitmapAnd, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_BitmapOr => {
            result = ExecInitBitmapOr(node as *mut BitmapOr, estate, eflags) as *mut PlanState;
        }

        /*
         * scan nodes
         */
        NodeTag::T_SeqScan => {
            result = ExecInitSeqScan(node as *mut SeqScan, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_SampleScan => {
            result =
                ExecInitSampleScan(node as *mut SampleScan, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_IndexScan => {
            result =
                ExecInitIndexScan(node as *mut IndexScan, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_IndexOnlyScan => {
            result = ExecInitIndexOnlyScan(node as *mut IndexOnlyScan, estate, eflags)
                as *mut PlanState;
        }

        NodeTag::T_BitmapIndexScan => {
            result = ExecInitBitmapIndexScan(node as *mut BitmapIndexScan, estate, eflags)
                as *mut PlanState;
        }

        NodeTag::T_BitmapHeapScan => {
            result = ExecInitBitmapHeapScan(node as *mut BitmapHeapScan, estate, eflags)
                as *mut PlanState;
        }

        NodeTag::T_TidScan => {
            result = ExecInitTidScan(node as *mut TidScan, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_TidRangeScan => {
            result = ExecInitTidRangeScan(node as *mut TidRangeScan, estate, eflags)
                as *mut PlanState;
        }

        NodeTag::T_SubqueryScan => {
            result = ExecInitSubqueryScan(node as *mut SubqueryScan, estate, eflags)
                as *mut PlanState;
        }

        NodeTag::T_FunctionScan => {
            result = ExecInitFunctionScan(node as *mut FunctionScan, estate, eflags)
                as *mut PlanState;
        }

        NodeTag::T_TableFuncScan => {
            result = ExecInitTableFuncScan(node as *mut TableFuncScan, estate, eflags)
                as *mut PlanState;
        }

        NodeTag::T_ValuesScan => {
            result =
                ExecInitValuesScan(node as *mut ValuesScan, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_CteScan => {
            result = ExecInitCteScan(node as *mut CteScan, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_NamedTuplestoreScan => {
            result =
                ExecInitNamedTuplestoreScan(node as *mut NamedTuplestoreScan, estate, eflags)
                    as *mut PlanState;
        }

        NodeTag::T_WorkTableScan => {
            result = ExecInitWorkTableScan(node as *mut WorkTableScan, estate, eflags)
                as *mut PlanState;
        }

        NodeTag::T_ForeignScan => {
            result =
                ExecInitForeignScan(node as *mut ForeignScan, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_CustomScan => {
            result =
                ExecInitCustomScan(node as *mut CustomScan, estate, eflags) as *mut PlanState;
        }

        /*
         * join nodes
         */
        NodeTag::T_NestLoop => {
            result = ExecInitNestLoop(node as *mut NestLoop, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_MergeJoin => {
            result =
                ExecInitMergeJoin(node as *mut MergeJoin, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_HashJoin => {
            result = ExecInitHashJoin(node as *mut HashJoin, estate, eflags) as *mut PlanState;
        }

        /*
         * materialization nodes
         */
        NodeTag::T_Material => {
            result = ExecInitMaterial(node as *mut Material, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_Sort => {
            result = ExecInitSort(node as *mut Sort, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_IncrementalSort => {
            result = ExecInitIncrementalSort(node as *mut IncrementalSort, estate, eflags)
                as *mut PlanState;
        }

        NodeTag::T_Memoize => {
            result = ExecInitMemoize(node as *mut Memoize, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_Group => {
            result = ExecInitGroup(node as *mut Group, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_Agg => {
            result = ExecInitAgg(node as *mut Agg, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_WindowAgg => {
            result =
                ExecInitWindowAgg(node as *mut WindowAgg, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_Unique => {
            result = ExecInitUnique(node as *mut Unique, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_Gather => {
            result = ExecInitGather(node as *mut Gather, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_GatherMerge => {
            result =
                ExecInitGatherMerge(node as *mut GatherMerge, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_Hash => {
            result = ExecInitHash(node as *mut Hash, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_SetOp => {
            result = ExecInitSetOp(node as *mut SetOp, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_LockRows => {
            result =
                ExecInitLockRows(node as *mut LockRows, estate, eflags) as *mut PlanState;
        }

        NodeTag::T_Limit => {
            result = ExecInitLimit(node as *mut Limit, estate, eflags) as *mut PlanState;
        }

        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(node as *const Node) as c_int);
            #[allow(unreachable_code)]
            {
                result = std::ptr::null_mut(); /* keep compiler quiet */
            }
        }
    }

    ExecSetExecProcNode(result, (*result).ExecProcNode);

    /*
     * Initialize any initPlans present in this node.  The planner put them in
     * a separate list for us.
     *
     * The defining characteristic of initplans is that they don't have
     * arguments, so we don't need to evaluate them (in contrast to
     * ExecInitSubPlanExpr()).
     */
    subps = NIL;
    foreach!(l, (*node).initPlan, {
        let subplan: *mut SubPlan = lfirst(current_cell!(l)) as *mut SubPlan;
        let sstate: *mut SubPlanState;

        Assert!(IsA!(subplan, T_SubPlan));
        Assert!((*subplan).args == NIL);
        sstate = ExecInitSubPlan(subplan, result);
        subps = lappend(subps, sstate as *mut std::ffi::c_void);
    });
    (*result).initPlan = subps;

    /* Set up instrumentation for this node if requested */
    if (*estate).es_instrument != 0 {
        (*result).instrument = InstrAlloc(1, (*estate).es_instrument, (*result).async_capable);
    }

    result
}

/*
 * If a node wants to change its ExecProcNode function after ExecInitNode()
 * has finished, it should do so with this function.  That way any wrapper
 * functions can be reinstalled, without the node having to know how that
 * works.
 */
pub unsafe fn ExecSetExecProcNode(node: *mut PlanState, function: ExecProcNodeMtd) {
    /*
     * Add a wrapper around the ExecProcNode callback that checks stack depth
     * during the first execution and maybe adds an instrumentation wrapper.
     * When the callback is changed after execution has already begun that
     * means we'll superfluously execute ExecProcNodeFirst, but that seems ok.
     */
    (*node).ExecProcNodeReal = function;
    (*node).ExecProcNode = Some(ExecProcNodeFirst);
}

/*
 * ExecProcNode wrapper that performs some one-time checks, before calling
 * the relevant node method (possibly via an instrumentation wrapper).
 */
unsafe fn ExecProcNodeFirst(node: *mut PlanState) -> *mut TupleTableSlot {
    /*
     * Perform stack depth check during the first execution of the node.  We
     * only do so the first time round because it turns out to not be cheap on
     * some common architectures (eg. x86).  This relies on the assumption
     * that ExecProcNode calls for a given plan node will always be made at
     * roughly the same stack depth.
     */
    check_stack_depth();

    /*
     * If instrumentation is required, change the wrapper to one that just
     * does instrumentation.  Otherwise we can dispense with all wrappers and
     * have ExecProcNode() directly call the relevant function from now on.
     */
    if !(*node).instrument.is_null() {
        (*node).ExecProcNode = Some(ExecProcNodeInstr);
    } else {
        (*node).ExecProcNode = (*node).ExecProcNodeReal;
    }

    ((*node).ExecProcNode.unwrap())(node)
}

/*
 * ExecProcNode wrapper that performs instrumentation calls.  By keeping
 * this a separate function, we avoid overhead in the normal case where
 * no instrumentation is wanted.
 */
unsafe fn ExecProcNodeInstr(node: *mut PlanState) -> *mut TupleTableSlot {
    let result: *mut TupleTableSlot;

    InstrStartNode((*node).instrument);

    result = ((*node).ExecProcNodeReal.unwrap())(node);

    InstrStopNode(
        (*node).instrument,
        if TupIsNull(result) { 0.0 } else { 1.0 },
    );

    result
}

/* ----------------------------------------------------------------
 *		MultiExecProcNode
 *
 *		Execute a node that doesn't return individual tuples
 *		(it might return a hashtable, bitmap, etc).  Caller should
 *		check it got back the expected kind of Node.
 *
 * This has essentially the same responsibilities as ExecProcNode,
 * but it does not do InstrStartNode/InstrStopNode (mainly because
 * it can't tell how many returned tuples to count).  Each per-node
 * function must provide its own instrumentation support.
 * ----------------------------------------------------------------
 */
pub unsafe fn MultiExecProcNode(node: *mut PlanState) -> *mut Node {
    let result: *mut Node;

    check_stack_depth();

    crate::miscadmin::CHECK_FOR_INTERRUPTS();

    if !(*node).chgParam.is_null() {
        /* something changed */
        ExecReScan(node); /* let ReScan handle this */
    }

    match nodeTag(node as *const Node) {
        /*
         * Only node types that actually support multiexec will be listed
         */
        NodeTag::T_HashState => {
            result = MultiExecHash(node as *mut HashState);
        }

        NodeTag::T_BitmapIndexScanState => {
            result = MultiExecBitmapIndexScan(node as *mut BitmapIndexScanState);
        }

        NodeTag::T_BitmapAndState => {
            result = MultiExecBitmapAnd(node as *mut BitmapAndState);
        }

        NodeTag::T_BitmapOrState => {
            result = MultiExecBitmapOr(node as *mut BitmapOrState);
        }

        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(node as *const Node) as c_int);
            #[allow(unreachable_code)]
            {
                result = std::ptr::null_mut();
            }
        }
    }

    result
}

/* ----------------------------------------------------------------
 *		ExecEndNode
 *
 *		Recursively cleans up all the nodes in the plan rooted
 *		at 'node'.
 *
 *		After this operation, the query plan will not be able to be
 *		processed any further.  This should be called only after
 *		the query plan has been fully executed.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndNode(node: *mut PlanState) {
    /*
     * do nothing when we get to the end of a leaf on tree.
     */
    if node.is_null() {
        return;
    }

    /*
     * Make sure there's enough stack available. Need to check here, in
     * addition to ExecProcNode() (via ExecProcNodeFirst()), because it's not
     * guaranteed that ExecProcNode() is reached for all nodes.
     */
    check_stack_depth();

    if !(*node).chgParam.is_null() {
        bms_free((*node).chgParam);
        (*node).chgParam = std::ptr::null_mut();
    }

    match nodeTag(node as *const Node) {
        /*
         * control nodes
         */
        NodeTag::T_ResultState => {
            ExecEndResult(node as *mut ResultState);
        }

        NodeTag::T_ProjectSetState => {
            ExecEndProjectSet(node as *mut ProjectSetState);
        }

        NodeTag::T_ModifyTableState => {
            ExecEndModifyTable(node as *mut ModifyTableState);
        }

        NodeTag::T_AppendState => {
            ExecEndAppend(node as *mut AppendState);
        }

        NodeTag::T_MergeAppendState => {
            ExecEndMergeAppend(node as *mut MergeAppendState);
        }

        NodeTag::T_RecursiveUnionState => {
            ExecEndRecursiveUnion(node as *mut RecursiveUnionState);
        }

        NodeTag::T_BitmapAndState => {
            ExecEndBitmapAnd(node as *mut BitmapAndState);
        }

        NodeTag::T_BitmapOrState => {
            ExecEndBitmapOr(node as *mut BitmapOrState);
        }

        /*
         * scan nodes
         */
        NodeTag::T_SeqScanState => {
            ExecEndSeqScan(node as *mut SeqScanState);
        }

        NodeTag::T_SampleScanState => {
            ExecEndSampleScan(node as *mut SampleScanState);
        }

        NodeTag::T_GatherState => {
            ExecEndGather(node as *mut GatherState);
        }

        NodeTag::T_GatherMergeState => {
            ExecEndGatherMerge(node as *mut GatherMergeState);
        }

        NodeTag::T_IndexScanState => {
            ExecEndIndexScan(node as *mut IndexScanState);
        }

        NodeTag::T_IndexOnlyScanState => {
            ExecEndIndexOnlyScan(node as *mut IndexOnlyScanState);
        }

        NodeTag::T_BitmapIndexScanState => {
            ExecEndBitmapIndexScan(node as *mut BitmapIndexScanState);
        }

        NodeTag::T_BitmapHeapScanState => {
            ExecEndBitmapHeapScan(node as *mut BitmapHeapScanState);
        }

        NodeTag::T_TidScanState => {
            ExecEndTidScan(node as *mut TidScanState);
        }

        NodeTag::T_TidRangeScanState => {
            ExecEndTidRangeScan(node as *mut TidRangeScanState);
        }

        NodeTag::T_SubqueryScanState => {
            ExecEndSubqueryScan(node as *mut SubqueryScanState);
        }

        NodeTag::T_FunctionScanState => {
            ExecEndFunctionScan(node as *mut FunctionScanState);
        }

        NodeTag::T_TableFuncScanState => {
            ExecEndTableFuncScan(node as *mut TableFuncScanState);
        }

        NodeTag::T_CteScanState => {
            ExecEndCteScan(node as *mut CteScanState);
        }

        NodeTag::T_ForeignScanState => {
            ExecEndForeignScan(node as *mut ForeignScanState);
        }

        NodeTag::T_CustomScanState => {
            ExecEndCustomScan(node as *mut CustomScanState);
        }

        /*
         * join nodes
         */
        NodeTag::T_NestLoopState => {
            ExecEndNestLoop(node as *mut NestLoopState);
        }

        NodeTag::T_MergeJoinState => {
            ExecEndMergeJoin(node as *mut MergeJoinState);
        }

        NodeTag::T_HashJoinState => {
            ExecEndHashJoin(node as *mut HashJoinState);
        }

        /*
         * materialization nodes
         */
        NodeTag::T_MaterialState => {
            ExecEndMaterial(node as *mut MaterialState);
        }

        NodeTag::T_SortState => {
            ExecEndSort(node as *mut SortState);
        }

        NodeTag::T_IncrementalSortState => {
            ExecEndIncrementalSort(node as *mut IncrementalSortState);
        }

        NodeTag::T_MemoizeState => {
            ExecEndMemoize(node as *mut MemoizeState);
        }

        NodeTag::T_GroupState => {
            ExecEndGroup(node as *mut GroupState);
        }

        NodeTag::T_AggState => {
            ExecEndAgg(node as *mut AggState);
        }

        NodeTag::T_WindowAggState => {
            ExecEndWindowAgg(node as *mut WindowAggState);
        }

        NodeTag::T_UniqueState => {
            ExecEndUnique(node as *mut UniqueState);
        }

        NodeTag::T_HashState => {
            ExecEndHash(node as *mut HashState);
        }

        NodeTag::T_SetOpState => {
            ExecEndSetOp(node as *mut SetOpState);
        }

        NodeTag::T_LockRowsState => {
            ExecEndLockRows(node as *mut LockRowsState);
        }

        NodeTag::T_LimitState => {
            ExecEndLimit(node as *mut LimitState);
        }

        /* No clean up actions for these nodes. */
        NodeTag::T_ValuesScanState
        | NodeTag::T_NamedTuplestoreScanState
        | NodeTag::T_WorkTableScanState => {}

        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(node as *const Node) as c_int);
        }
    }
}

/*
 * ExecShutdownNode
 *
 * Give execution nodes a chance to stop asynchronous resource consumption
 * and release any resources still held.
 */
pub unsafe fn ExecShutdownNode(node: *mut PlanState) {
    ExecShutdownNode_walker(node, std::ptr::null_mut());
}

unsafe extern "C" fn ExecShutdownNode_walker(
    node: *mut PlanState,
    context: *mut std::ffi::c_void,
) -> bool {
    if node.is_null() {
        return false;
    }

    check_stack_depth();

    /*
     * Treat the node as running while we shut it down, but only if it's run
     * at least once already.  We don't expect much CPU consumption during
     * node shutdown, but in the case of Gather or Gather Merge, we may shut
     * down workers at this stage.  If so, their buffer usage will get
     * propagated into pgBufferUsage at this point, and we want to make sure
     * that it gets associated with the Gather node.  We skip this if the node
     * has never been executed, so as to avoid incorrectly making it appear
     * that it has.
     */
    if !(*node).instrument.is_null() && (*(*node).instrument).running {
        InstrStartNode((*node).instrument);
    }

    planstate_tree_walker(node, Some(ExecShutdownNode_walker), context);

    match nodeTag(node as *const Node) {
        NodeTag::T_GatherState => {
            ExecShutdownGather(node as *mut GatherState);
        }
        NodeTag::T_ForeignScanState => {
            ExecShutdownForeignScan(node as *mut ForeignScanState);
        }
        NodeTag::T_CustomScanState => {
            ExecShutdownCustomScan(node as *mut CustomScanState);
        }
        NodeTag::T_GatherMergeState => {
            ExecShutdownGatherMerge(node as *mut GatherMergeState);
        }
        NodeTag::T_HashState => {
            ExecShutdownHash(node as *mut HashState);
        }
        NodeTag::T_HashJoinState => {
            ExecShutdownHashJoin(node as *mut HashJoinState);
        }
        _ => {}
    }

    /* Stop the node if we started it above, reporting 0 tuples. */
    if !(*node).instrument.is_null() && (*(*node).instrument).running {
        InstrStopNode((*node).instrument, 0.0);
    }

    false
}

/*
 * ExecSetTupleBound
 *
 * Set a tuple bound for a planstate node.  This lets child plan nodes
 * optimize based on the knowledge that the maximum number of tuples that
 * their parent will demand is limited.  The tuple bound for a node may
 * only be changed between scans (i.e., after node initialization or just
 * before an ExecReScan call).
 *
 * Any negative tuples_needed value means "no limit", which should be the
 * default assumption when this is not called at all for a particular node.
 *
 * Note: if this is called repeatedly on a plan tree, the exact same set
 * of nodes must be updated with the new limit each time; be careful that
 * only unchanging conditions are tested here.
 */
pub unsafe fn ExecSetTupleBound(tuples_needed: int64, child_node: *mut PlanState) {
    /*
     * Since this function recurses, in principle we should check stack depth
     * here.  In practice, it's probably pointless since the earlier node
     * initialization tree traversal would surely have consumed more stack.
     */

    if IsA!(child_node, T_SortState) {
        /*
         * If it is a Sort node, notify it that it can use bounded sort.
         *
         * Note: it is the responsibility of nodeSort.c to react properly to
         * changes of these parameters.  If we ever redesign this, it'd be a
         * good idea to integrate this signaling with the parameter-change
         * mechanism.
         */
        let sortState: *mut SortState = child_node as *mut SortState;

        if tuples_needed < 0 {
            /* make sure flag gets reset if needed upon rescan */
            (*sortState).bounded = false;
        } else {
            (*sortState).bounded = true;
            (*sortState).bound = tuples_needed;
        }
    } else if IsA!(child_node, T_IncrementalSortState) {
        /*
         * If it is an IncrementalSort node, notify it that it can use bounded
         * sort.
         *
         * Note: it is the responsibility of nodeIncrementalSort.c to react
         * properly to changes of these parameters.  If we ever redesign this,
         * it'd be a good idea to integrate this signaling with the
         * parameter-change mechanism.
         */
        let sortState: *mut IncrementalSortState = child_node as *mut IncrementalSortState;

        if tuples_needed < 0 {
            /* make sure flag gets reset if needed upon rescan */
            (*sortState).bounded = false;
        } else {
            (*sortState).bounded = true;
            (*sortState).bound = tuples_needed;
        }
    } else if IsA!(child_node, T_AppendState) {
        /*
         * If it is an Append, we can apply the bound to any nodes that are
         * children of the Append, since the Append surely need read no more
         * than that many tuples from any one input.
         */
        let aState: *mut AppendState = child_node as *mut AppendState;

        let mut i: c_int = 0;
        while i < (*aState).as_nplans {
            ExecSetTupleBound(tuples_needed, *(*aState).appendplans.offset(i as isize));
            i += 1;
        }
    } else if IsA!(child_node, T_MergeAppendState) {
        /*
         * If it is a MergeAppend, we can apply the bound to any nodes that
         * are children of the MergeAppend, since the MergeAppend surely need
         * read no more than that many tuples from any one input.
         */
        let maState: *mut MergeAppendState = child_node as *mut MergeAppendState;

        let mut i: c_int = 0;
        while i < (*maState).ms_nplans {
            ExecSetTupleBound(tuples_needed, *(*maState).mergeplans.offset(i as isize));
            i += 1;
        }
    } else if IsA!(child_node, T_ResultState) {
        /*
         * Similarly, for a projecting Result, we can apply the bound to its
         * child node.
         *
         * If Result supported qual checking, we'd have to punt on seeing a
         * qual.  Note that having a resconstantqual is not a showstopper: if
         * that condition succeeds it affects nothing, while if it fails, no
         * rows will be demanded from the Result child anyway.
         */
        if !outerPlanState(child_node).is_null() {
            ExecSetTupleBound(tuples_needed, outerPlanState(child_node));
        }
    } else if IsA!(child_node, T_SubqueryScanState) {
        /*
         * We can also descend through SubqueryScan, but only if it has no
         * qual (otherwise it might discard rows).
         */
        let subqueryState: *mut SubqueryScanState = child_node as *mut SubqueryScanState;

        if (*subqueryState).ss.ps.qual.is_null() {
            ExecSetTupleBound(tuples_needed, (*subqueryState).subplan);
        }
    } else if IsA!(child_node, T_GatherState) {
        /*
         * A Gather node can propagate the bound to its workers.  As with
         * MergeAppend, no one worker could possibly need to return more
         * tuples than the Gather itself needs to.
         *
         * Note: As with Sort, the Gather node is responsible for reacting
         * properly to changes to this parameter.
         */
        let gstate: *mut GatherState = child_node as *mut GatherState;

        (*gstate).tuples_needed = tuples_needed;

        /* Also pass down the bound to our own copy of the child plan */
        ExecSetTupleBound(tuples_needed, outerPlanState(child_node));
    } else if IsA!(child_node, T_GatherMergeState) {
        /* Same comments as for Gather */
        let gstate: *mut GatherMergeState = child_node as *mut GatherMergeState;

        (*gstate).tuples_needed = tuples_needed;

        ExecSetTupleBound(tuples_needed, outerPlanState(child_node));
    }

    /*
     * In principle we could descend through any plan node type that is
     * certain not to discard or combine input rows; but on seeing a node that
     * can do that, we can't propagate the bound any further.  For the moment
     * it's unclear that any other cases are worth checking here.
     */
}

/* ----------------------------------------------------------------
 *		local stubs for unported dependencies
 * ----------------------------------------------------------------
 */

unsafe fn check_stack_depth() {
    // TODO: src/backend/tcop/postgres.c
}

unsafe fn ExecReScan(_node: *mut PlanState) {
    unimplemented!() // TODO: src/backend/executor/execAmi.c
}

unsafe fn outerPlanState(node: *mut PlanState) -> *mut PlanState {
    // outerPlanState(node) == ((PlanState *) (node))->lefttree
    (*node).lefttree
}

unsafe fn InstrAlloc(_n: c_int, _instrument_options: c_int, _async_mode: bool) -> *mut Instrumentation {
    unimplemented!() // TODO: src/backend/executor/instrument.c
}

unsafe fn InstrStartNode(_instr: *mut Instrumentation) {
    unimplemented!() // TODO: src/backend/executor/instrument.c
}

unsafe fn InstrStopNode(_instr: *mut Instrumentation, _nTuples: f64) {
    unimplemented!() // TODO: src/backend/executor/instrument.c
}

unsafe fn TupIsNull(_slot: *mut TupleTableSlot) -> bool {
    unimplemented!() // TODO: src/include/executor/tuptable.h
}

unsafe fn bms_free(_a: *mut Bitmapset) {
    unimplemented!() // TODO: src/backend/nodes/bitmapset.c
}

unsafe fn planstate_tree_walker(
    _planstate: *mut PlanState,
    _walker: Option<unsafe extern "C" fn(*mut PlanState, *mut std::ffi::c_void) -> bool>,
    _context: *mut std::ffi::c_void,
) -> bool {
    unimplemented!() // TODO: src/backend/nodes/nodeFuncs.c
}

unsafe fn ExecInitSubPlan(_subplan: *mut SubPlan, _parent: *mut PlanState) -> *mut SubPlanState {
    unimplemented!() // TODO: src/backend/executor/nodeSubplan.c
}

/* per-node init/end/multiexec/shutdown dispatch targets */

unsafe fn ExecInitResult(_node: *mut Result, _estate: *mut EState, _eflags: c_int) -> *mut ResultState {
    unimplemented!() // TODO: src/backend/executor/nodeResult.c
}
unsafe fn ExecEndResult(_node: *mut ResultState) {
    unimplemented!() // TODO: src/backend/executor/nodeResult.c
}

unsafe fn ExecInitProjectSet(_node: *mut ProjectSet, _estate: *mut EState, _eflags: c_int) -> *mut ProjectSetState {
    unimplemented!() // TODO: src/backend/executor/nodeProjectSet.c
}
unsafe fn ExecEndProjectSet(_node: *mut ProjectSetState) {
    unimplemented!() // TODO: src/backend/executor/nodeProjectSet.c
}

unsafe fn ExecInitModifyTable(_node: *mut ModifyTable, _estate: *mut EState, _eflags: c_int) -> *mut ModifyTableState {
    unimplemented!() // TODO: src/backend/executor/nodeModifyTable.c
}
unsafe fn ExecEndModifyTable(_node: *mut ModifyTableState) {
    unimplemented!() // TODO: src/backend/executor/nodeModifyTable.c
}

unsafe fn ExecInitAppend(_node: *mut Append, _estate: *mut EState, _eflags: c_int) -> *mut AppendState {
    unimplemented!() // TODO: src/backend/executor/nodeAppend.c
}
unsafe fn ExecEndAppend(_node: *mut AppendState) {
    unimplemented!() // TODO: src/backend/executor/nodeAppend.c
}

unsafe fn ExecInitMergeAppend(_node: *mut MergeAppend, _estate: *mut EState, _eflags: c_int) -> *mut MergeAppendState {
    unimplemented!() // TODO: src/backend/executor/nodeMergeAppend.c
}
unsafe fn ExecEndMergeAppend(_node: *mut MergeAppendState) {
    unimplemented!() // TODO: src/backend/executor/nodeMergeAppend.c
}

unsafe fn ExecInitRecursiveUnion(_node: *mut RecursiveUnion, _estate: *mut EState, _eflags: c_int) -> *mut RecursiveUnionState {
    unimplemented!() // TODO: src/backend/executor/nodeRecursiveunion.c
}
unsafe fn ExecEndRecursiveUnion(_node: *mut RecursiveUnionState) {
    unimplemented!() // TODO: src/backend/executor/nodeRecursiveunion.c
}

unsafe fn ExecInitBitmapAnd(_node: *mut BitmapAnd, _estate: *mut EState, _eflags: c_int) -> *mut BitmapAndState {
    unimplemented!() // TODO: src/backend/executor/nodeBitmapAnd.c
}
unsafe fn ExecEndBitmapAnd(_node: *mut BitmapAndState) {
    unimplemented!() // TODO: src/backend/executor/nodeBitmapAnd.c
}
unsafe fn MultiExecBitmapAnd(_node: *mut BitmapAndState) -> *mut Node {
    unimplemented!() // TODO: src/backend/executor/nodeBitmapAnd.c
}

unsafe fn ExecInitBitmapOr(_node: *mut BitmapOr, _estate: *mut EState, _eflags: c_int) -> *mut BitmapOrState {
    unimplemented!() // TODO: src/backend/executor/nodeBitmapOr.c
}
unsafe fn ExecEndBitmapOr(_node: *mut BitmapOrState) {
    unimplemented!() // TODO: src/backend/executor/nodeBitmapOr.c
}
unsafe fn MultiExecBitmapOr(_node: *mut BitmapOrState) -> *mut Node {
    unimplemented!() // TODO: src/backend/executor/nodeBitmapOr.c
}

unsafe fn ExecInitSeqScan(_node: *mut SeqScan, _estate: *mut EState, _eflags: c_int) -> *mut SeqScanState {
    unimplemented!() // TODO: src/backend/executor/nodeSeqscan.c
}
unsafe fn ExecEndSeqScan(_node: *mut SeqScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeSeqscan.c
}

unsafe fn ExecInitSampleScan(_node: *mut SampleScan, _estate: *mut EState, _eflags: c_int) -> *mut SampleScanState {
    unimplemented!() // TODO: src/backend/executor/nodeSamplescan.c
}
unsafe fn ExecEndSampleScan(_node: *mut SampleScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeSamplescan.c
}

unsafe fn ExecInitIndexScan(_node: *mut IndexScan, _estate: *mut EState, _eflags: c_int) -> *mut IndexScanState {
    unimplemented!() // TODO: src/backend/executor/nodeIndexscan.c
}
unsafe fn ExecEndIndexScan(_node: *mut IndexScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeIndexscan.c
}

unsafe fn ExecInitIndexOnlyScan(_node: *mut IndexOnlyScan, _estate: *mut EState, _eflags: c_int) -> *mut IndexOnlyScanState {
    unimplemented!() // TODO: src/backend/executor/nodeIndexonlyscan.c
}
unsafe fn ExecEndIndexOnlyScan(_node: *mut IndexOnlyScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeIndexonlyscan.c
}

unsafe fn ExecInitBitmapIndexScan(_node: *mut BitmapIndexScan, _estate: *mut EState, _eflags: c_int) -> *mut BitmapIndexScanState {
    unimplemented!() // TODO: src/backend/executor/nodeBitmapIndexscan.c
}
unsafe fn ExecEndBitmapIndexScan(_node: *mut BitmapIndexScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeBitmapIndexscan.c
}
unsafe fn MultiExecBitmapIndexScan(_node: *mut BitmapIndexScanState) -> *mut Node {
    unimplemented!() // TODO: src/backend/executor/nodeBitmapIndexscan.c
}

unsafe fn ExecInitBitmapHeapScan(_node: *mut BitmapHeapScan, _estate: *mut EState, _eflags: c_int) -> *mut BitmapHeapScanState {
    unimplemented!() // TODO: src/backend/executor/nodeBitmapHeapscan.c
}
unsafe fn ExecEndBitmapHeapScan(_node: *mut BitmapHeapScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeBitmapHeapscan.c
}

unsafe fn ExecInitTidScan(_node: *mut TidScan, _estate: *mut EState, _eflags: c_int) -> *mut TidScanState {
    unimplemented!() // TODO: src/backend/executor/nodeTidscan.c
}
unsafe fn ExecEndTidScan(_node: *mut TidScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeTidscan.c
}

unsafe fn ExecInitTidRangeScan(_node: *mut TidRangeScan, _estate: *mut EState, _eflags: c_int) -> *mut TidRangeScanState {
    unimplemented!() // TODO: src/backend/executor/nodeTidrangescan.c
}
unsafe fn ExecEndTidRangeScan(_node: *mut TidRangeScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeTidrangescan.c
}

unsafe fn ExecInitSubqueryScan(_node: *mut SubqueryScan, _estate: *mut EState, _eflags: c_int) -> *mut SubqueryScanState {
    unimplemented!() // TODO: src/backend/executor/nodeSubqueryscan.c
}
unsafe fn ExecEndSubqueryScan(_node: *mut SubqueryScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeSubqueryscan.c
}

unsafe fn ExecInitFunctionScan(_node: *mut FunctionScan, _estate: *mut EState, _eflags: c_int) -> *mut FunctionScanState {
    unimplemented!() // TODO: src/backend/executor/nodeFunctionscan.c
}
unsafe fn ExecEndFunctionScan(_node: *mut FunctionScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeFunctionscan.c
}

unsafe fn ExecInitTableFuncScan(_node: *mut TableFuncScan, _estate: *mut EState, _eflags: c_int) -> *mut TableFuncScanState {
    unimplemented!() // TODO: src/backend/executor/nodeTableFuncscan.c
}
unsafe fn ExecEndTableFuncScan(_node: *mut TableFuncScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeTableFuncscan.c
}

unsafe fn ExecInitValuesScan(_node: *mut ValuesScan, _estate: *mut EState, _eflags: c_int) -> *mut ValuesScanState {
    unimplemented!() // TODO: src/backend/executor/nodeValuesscan.c
}

unsafe fn ExecInitCteScan(_node: *mut CteScan, _estate: *mut EState, _eflags: c_int) -> *mut CteScanState {
    unimplemented!() // TODO: src/backend/executor/nodeCtescan.c
}
unsafe fn ExecEndCteScan(_node: *mut CteScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeCtescan.c
}

unsafe fn ExecInitNamedTuplestoreScan(_node: *mut NamedTuplestoreScan, _estate: *mut EState, _eflags: c_int) -> *mut NamedTuplestoreScanState {
    unimplemented!() // TODO: src/backend/executor/nodeNamedtuplestorescan.c
}

unsafe fn ExecInitWorkTableScan(_node: *mut WorkTableScan, _estate: *mut EState, _eflags: c_int) -> *mut WorkTableScanState {
    unimplemented!() // TODO: src/backend/executor/nodeWorktablescan.c
}

unsafe fn ExecInitForeignScan(_node: *mut ForeignScan, _estate: *mut EState, _eflags: c_int) -> *mut ForeignScanState {
    unimplemented!() // TODO: src/backend/executor/nodeForeignscan.c
}
unsafe fn ExecEndForeignScan(_node: *mut ForeignScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeForeignscan.c
}
unsafe fn ExecShutdownForeignScan(_node: *mut ForeignScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeForeignscan.c
}

unsafe fn ExecInitCustomScan(_node: *mut CustomScan, _estate: *mut EState, _eflags: c_int) -> *mut CustomScanState {
    unimplemented!() // TODO: src/backend/executor/nodeCustom.c
}
unsafe fn ExecEndCustomScan(_node: *mut CustomScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeCustom.c
}
unsafe fn ExecShutdownCustomScan(_node: *mut CustomScanState) {
    unimplemented!() // TODO: src/backend/executor/nodeCustom.c
}

unsafe fn ExecInitNestLoop(_node: *mut NestLoop, _estate: *mut EState, _eflags: c_int) -> *mut NestLoopState {
    unimplemented!() // TODO: src/backend/executor/nodeNestloop.c
}
unsafe fn ExecEndNestLoop(_node: *mut NestLoopState) {
    unimplemented!() // TODO: src/backend/executor/nodeNestloop.c
}

unsafe fn ExecInitMergeJoin(_node: *mut MergeJoin, _estate: *mut EState, _eflags: c_int) -> *mut MergeJoinState {
    unimplemented!() // TODO: src/backend/executor/nodeMergejoin.c
}
unsafe fn ExecEndMergeJoin(_node: *mut MergeJoinState) {
    unimplemented!() // TODO: src/backend/executor/nodeMergejoin.c
}

unsafe fn ExecInitHashJoin(_node: *mut HashJoin, _estate: *mut EState, _eflags: c_int) -> *mut HashJoinState {
    unimplemented!() // TODO: src/backend/executor/nodeHashjoin.c
}
unsafe fn ExecEndHashJoin(_node: *mut HashJoinState) {
    unimplemented!() // TODO: src/backend/executor/nodeHashjoin.c
}
unsafe fn ExecShutdownHashJoin(_node: *mut HashJoinState) {
    unimplemented!() // TODO: src/backend/executor/nodeHashjoin.c
}

unsafe fn ExecInitMaterial(_node: *mut Material, _estate: *mut EState, _eflags: c_int) -> *mut MaterialState {
    unimplemented!() // TODO: src/backend/executor/nodeMaterial.c
}
unsafe fn ExecEndMaterial(_node: *mut MaterialState) {
    unimplemented!() // TODO: src/backend/executor/nodeMaterial.c
}

unsafe fn ExecInitSort(_node: *mut Sort, _estate: *mut EState, _eflags: c_int) -> *mut SortState {
    unimplemented!() // TODO: src/backend/executor/nodeSort.c
}
unsafe fn ExecEndSort(_node: *mut SortState) {
    unimplemented!() // TODO: src/backend/executor/nodeSort.c
}

unsafe fn ExecInitIncrementalSort(_node: *mut IncrementalSort, _estate: *mut EState, _eflags: c_int) -> *mut IncrementalSortState {
    unimplemented!() // TODO: src/backend/executor/nodeIncrementalSort.c
}
unsafe fn ExecEndIncrementalSort(_node: *mut IncrementalSortState) {
    unimplemented!() // TODO: src/backend/executor/nodeIncrementalSort.c
}

unsafe fn ExecInitMemoize(_node: *mut Memoize, _estate: *mut EState, _eflags: c_int) -> *mut MemoizeState {
    unimplemented!() // TODO: src/backend/executor/nodeMemoize.c
}
unsafe fn ExecEndMemoize(_node: *mut MemoizeState) {
    unimplemented!() // TODO: src/backend/executor/nodeMemoize.c
}

unsafe fn ExecInitGroup(_node: *mut Group, _estate: *mut EState, _eflags: c_int) -> *mut GroupState {
    unimplemented!() // TODO: src/backend/executor/nodeGroup.c
}
unsafe fn ExecEndGroup(_node: *mut GroupState) {
    unimplemented!() // TODO: src/backend/executor/nodeGroup.c
}

unsafe fn ExecInitAgg(_node: *mut Agg, _estate: *mut EState, _eflags: c_int) -> *mut AggState {
    unimplemented!() // TODO: src/backend/executor/nodeAgg.c
}
unsafe fn ExecEndAgg(_node: *mut AggState) {
    unimplemented!() // TODO: src/backend/executor/nodeAgg.c
}

unsafe fn ExecInitWindowAgg(_node: *mut WindowAgg, _estate: *mut EState, _eflags: c_int) -> *mut WindowAggState {
    unimplemented!() // TODO: src/backend/executor/nodeWindowAgg.c
}
unsafe fn ExecEndWindowAgg(_node: *mut WindowAggState) {
    unimplemented!() // TODO: src/backend/executor/nodeWindowAgg.c
}

unsafe fn ExecInitUnique(_node: *mut Unique, _estate: *mut EState, _eflags: c_int) -> *mut UniqueState {
    unimplemented!() // TODO: src/backend/executor/nodeUnique.c
}
unsafe fn ExecEndUnique(_node: *mut UniqueState) {
    unimplemented!() // TODO: src/backend/executor/nodeUnique.c
}

unsafe fn ExecInitGather(_node: *mut Gather, _estate: *mut EState, _eflags: c_int) -> *mut GatherState {
    unimplemented!() // TODO: src/backend/executor/nodeGather.c
}
unsafe fn ExecEndGather(_node: *mut GatherState) {
    unimplemented!() // TODO: src/backend/executor/nodeGather.c
}
unsafe fn ExecShutdownGather(_node: *mut GatherState) {
    unimplemented!() // TODO: src/backend/executor/nodeGather.c
}

unsafe fn ExecInitGatherMerge(_node: *mut GatherMerge, _estate: *mut EState, _eflags: c_int) -> *mut GatherMergeState {
    unimplemented!() // TODO: src/backend/executor/nodeGatherMerge.c
}
unsafe fn ExecEndGatherMerge(_node: *mut GatherMergeState) {
    unimplemented!() // TODO: src/backend/executor/nodeGatherMerge.c
}
unsafe fn ExecShutdownGatherMerge(_node: *mut GatherMergeState) {
    unimplemented!() // TODO: src/backend/executor/nodeGatherMerge.c
}

unsafe fn ExecInitHash(_node: *mut Hash, _estate: *mut EState, _eflags: c_int) -> *mut HashState {
    unimplemented!() // TODO: src/backend/executor/nodeHash.c
}
unsafe fn ExecEndHash(_node: *mut HashState) {
    unimplemented!() // TODO: src/backend/executor/nodeHash.c
}
unsafe fn MultiExecHash(_node: *mut HashState) -> *mut Node {
    unimplemented!() // TODO: src/backend/executor/nodeHash.c
}
unsafe fn ExecShutdownHash(_node: *mut HashState) {
    unimplemented!() // TODO: src/backend/executor/nodeHash.c
}

unsafe fn ExecInitSetOp(_node: *mut SetOp, _estate: *mut EState, _eflags: c_int) -> *mut SetOpState {
    unimplemented!() // TODO: src/backend/executor/nodeSetOp.c
}
unsafe fn ExecEndSetOp(_node: *mut SetOpState) {
    unimplemented!() // TODO: src/backend/executor/nodeSetOp.c
}

unsafe fn ExecInitLockRows(_node: *mut LockRows, _estate: *mut EState, _eflags: c_int) -> *mut LockRowsState {
    unimplemented!() // TODO: src/backend/executor/nodeLockRows.c
}
unsafe fn ExecEndLockRows(_node: *mut LockRowsState) {
    unimplemented!() // TODO: src/backend/executor/nodeLockRows.c
}

unsafe fn ExecInitLimit(_node: *mut Limit, _estate: *mut EState, _eflags: c_int) -> *mut LimitState {
    unimplemented!() // TODO: src/backend/executor/nodeLimit.c
}
unsafe fn ExecEndLimit(_node: *mut LimitState) {
    unimplemented!() // TODO: src/backend/executor/nodeLimit.c
}
