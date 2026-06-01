//! src/backend/executor/execAmi.c
//!
//! miscellaneous executor access method routines
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::nodes::nodes::{nodeTag, Node, NodeTag};
use crate::nodes::pg_list::{lfirst, linitial, list_length, List};

// Function-like macros live at the crate root (#[macro_export]).
use crate::{castNode, current_cell, foreach, IsA};

// ---------------------------------------------------------------------------
// Local stubs for unported dependencies.
// ---------------------------------------------------------------------------

unsafe fn InstrEndLoop(_instr: *mut c_void) {
    unimplemented!() // TODO: src/backend/executor/instrument.c
}

unsafe fn UpdateChangedParamSet(_node: *mut PlanState, _newchg: *mut c_void) {
    unimplemented!() // TODO: src/backend/executor/execUtils.c
}

unsafe fn ExecReScanSetParamPlan(_node: *mut SubPlanState, _parent: *mut PlanState) {
    unimplemented!() // TODO: src/backend/executor/nodeSubplan.c
}

unsafe fn ReScanExprContext(_econtext: *mut c_void) {
    unimplemented!() // TODO: src/backend/executor/execUtils.c
}

unsafe fn bms_free(_a: *mut c_void) {
    unimplemented!() // TODO: src/backend/nodes/bitmapset.c
}

unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> *mut c_void {
    unimplemented!() // TODO: src/backend/utils/cache/syscache.c
}

unsafe fn ReleaseSysCache(_tuple: *mut c_void) {
    unimplemented!() // TODO: src/backend/utils/cache/syscache.c
}

// Local stubs matching the *mut c_void syscache flow above. The real
// HeapTupleIsValid(HeapTuple)/GETSTRUCT(*const HeapTupleData) live in
// crate::access::htup_details but are typed against ported HeapTuple structs;
// once SearchSysCache1 returns a real HeapTuple these can be switched over.
unsafe fn HeapTupleIsValid(tuple: *mut c_void) -> bool {
    !tuple.is_null()
}

unsafe fn GETSTRUCT(_tuple: *mut c_void) -> *mut c_void {
    unimplemented!() // TODO: src/include/access/htup_details.h
}

unsafe fn GetIndexAmRoutineByAmId(_amoid: Oid, _noerror: bool) -> *mut IndexAmRoutine {
    unimplemented!() // TODO: src/backend/access/index/amapi.c
}

// Per-node ReScan routines
unsafe fn ExecReScanResult(_node: *mut PlanState) { unimplemented!() } // TODO: nodeResult.c
unsafe fn ExecReScanProjectSet(_node: *mut PlanState) { unimplemented!() } // TODO: nodeProjectSet.c
unsafe fn ExecReScanModifyTable(_node: *mut PlanState) { unimplemented!() } // TODO: nodeModifyTable.c
unsafe fn ExecReScanAppend(_node: *mut PlanState) { unimplemented!() } // TODO: nodeAppend.c
unsafe fn ExecReScanMergeAppend(_node: *mut PlanState) { unimplemented!() } // TODO: nodeMergeAppend.c
unsafe fn ExecReScanRecursiveUnion(_node: *mut PlanState) { unimplemented!() } // TODO: nodeRecursiveunion.c
unsafe fn ExecReScanBitmapAnd(_node: *mut PlanState) { unimplemented!() } // TODO: nodeBitmapAnd.c
unsafe fn ExecReScanBitmapOr(_node: *mut PlanState) { unimplemented!() } // TODO: nodeBitmapOr.c
unsafe fn ExecReScanSeqScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeSeqscan.c
unsafe fn ExecReScanSampleScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeSamplescan.c
unsafe fn ExecReScanGather(_node: *mut PlanState) { unimplemented!() } // TODO: nodeGather.c
unsafe fn ExecReScanGatherMerge(_node: *mut PlanState) { unimplemented!() } // TODO: nodeGatherMerge.c
unsafe fn ExecReScanIndexScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeIndexscan.c
unsafe fn ExecReScanIndexOnlyScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeIndexonlyscan.c
unsafe fn ExecReScanBitmapIndexScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeBitmapIndexscan.c
unsafe fn ExecReScanBitmapHeapScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeBitmapHeapscan.c
unsafe fn ExecReScanTidScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeTidscan.c
unsafe fn ExecReScanTidRangeScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeTidrangescan.c
unsafe fn ExecReScanSubqueryScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeSubqueryscan.c
unsafe fn ExecReScanFunctionScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeFunctionscan.c
unsafe fn ExecReScanTableFuncScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeTableFuncscan.c
unsafe fn ExecReScanValuesScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeValuesscan.c
unsafe fn ExecReScanCteScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeCtescan.c
unsafe fn ExecReScanNamedTuplestoreScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeNamedtuplestorescan.c
unsafe fn ExecReScanWorkTableScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeWorktablescan.c
unsafe fn ExecReScanForeignScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeForeignscan.c
unsafe fn ExecReScanCustomScan(_node: *mut PlanState) { unimplemented!() } // TODO: nodeCustom.c
unsafe fn ExecReScanNestLoop(_node: *mut PlanState) { unimplemented!() } // TODO: nodeNestloop.c
unsafe fn ExecReScanMergeJoin(_node: *mut PlanState) { unimplemented!() } // TODO: nodeMergejoin.c
unsafe fn ExecReScanHashJoin(_node: *mut PlanState) { unimplemented!() } // TODO: nodeHashjoin.c
unsafe fn ExecReScanMaterial(_node: *mut PlanState) { unimplemented!() } // TODO: nodeMaterial.c
unsafe fn ExecReScanMemoize(_node: *mut PlanState) { unimplemented!() } // TODO: nodeMemoize.c
unsafe fn ExecReScanSort(_node: *mut PlanState) { unimplemented!() } // TODO: nodeSort.c
unsafe fn ExecReScanIncrementalSort(_node: *mut PlanState) { unimplemented!() } // TODO: nodeIncrementalSort.c
unsafe fn ExecReScanGroup(_node: *mut PlanState) { unimplemented!() } // TODO: nodeGroup.c
unsafe fn ExecReScanAgg(_node: *mut PlanState) { unimplemented!() } // TODO: nodeAgg.c
unsafe fn ExecReScanWindowAgg(_node: *mut PlanState) { unimplemented!() } // TODO: nodeWindowAgg.c
unsafe fn ExecReScanUnique(_node: *mut PlanState) { unimplemented!() } // TODO: nodeUnique.c
unsafe fn ExecReScanHash(_node: *mut PlanState) { unimplemented!() } // TODO: nodeHash.c
unsafe fn ExecReScanSetOp(_node: *mut PlanState) { unimplemented!() } // TODO: nodeSetOp.c
unsafe fn ExecReScanLockRows(_node: *mut PlanState) { unimplemented!() } // TODO: nodeLockRows.c
unsafe fn ExecReScanLimit(_node: *mut PlanState) { unimplemented!() } // TODO: nodeLimit.c

// Per-node MarkPos routines
unsafe fn ExecIndexMarkPos(_node: *mut PlanState) { unimplemented!() } // TODO: nodeIndexscan.c
unsafe fn ExecIndexOnlyMarkPos(_node: *mut PlanState) { unimplemented!() } // TODO: nodeIndexonlyscan.c
unsafe fn ExecCustomMarkPos(_node: *mut PlanState) { unimplemented!() } // TODO: nodeCustom.c
unsafe fn ExecMaterialMarkPos(_node: *mut PlanState) { unimplemented!() } // TODO: nodeMaterial.c
unsafe fn ExecSortMarkPos(_node: *mut PlanState) { unimplemented!() } // TODO: nodeSort.c
unsafe fn ExecResultMarkPos(_node: *mut PlanState) { unimplemented!() } // TODO: nodeResult.c

// Per-node RestrPos routines
unsafe fn ExecIndexRestrPos(_node: *mut PlanState) { unimplemented!() } // TODO: nodeIndexscan.c
unsafe fn ExecIndexOnlyRestrPos(_node: *mut PlanState) { unimplemented!() } // TODO: nodeIndexonlyscan.c
unsafe fn ExecCustomRestrPos(_node: *mut PlanState) { unimplemented!() } // TODO: nodeCustom.c
unsafe fn ExecMaterialRestrPos(_node: *mut PlanState) { unimplemented!() } // TODO: nodeMaterial.c
unsafe fn ExecSortRestrPos(_node: *mut PlanState) { unimplemented!() } // TODO: nodeSort.c
unsafe fn ExecResultRestrPos(_node: *mut PlanState) { unimplemented!() } // TODO: nodeResult.c

// ---------------------------------------------------------------------------
// Stub types (defined fully in their own modules once ported).
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct PlanState {
    pub instrument: *mut c_void,
    pub chgParam: *mut c_void,
    pub initPlan: *mut List,
    pub subPlan: *mut List,
    pub ps_ExprContext: *mut c_void,
    pub plan: *mut Plan,
    pub lefttree: *mut PlanState,
    pub righttree: *mut PlanState,
}

#[repr(C)]
pub struct SubPlanState {
    pub planstate: *mut PlanState,
}

#[repr(C)]
pub struct Plan {
    pub parallel_aware: bool,
    pub extParam: *mut c_void,
    pub lefttree: *mut Plan,
    pub righttree: *mut Plan,
}

#[repr(C)]
pub struct Path {
    pub pathtype: NodeTag,
}

#[repr(C)]
pub struct IndexAmRoutine {
    pub amcanbackward: bool,
}

#[repr(C)]
pub struct Form_pg_class_data {
    pub relam: Oid,
}
pub type Form_pg_class = *mut Form_pg_class_data;

// SearchSysCache cache id for RELOID (catalog/syscache.h)
const RELOID: c_int = 0; // TODO: utils/syscache.h

// CustomPath / CustomScan flag bits (nodes/extensible.h)
const CUSTOMPATH_SUPPORT_MARK_RESTORE: u32 = 0x0002;
const CUSTOMPATH_SUPPORT_BACKWARD_SCAN: u32 = 0x0004;

// ---------------------------------------------------------------------------
// Accessor macros from execnodes.h / plannodes.h
// ---------------------------------------------------------------------------

#[inline]
unsafe fn outerPlanState(node: *mut PlanState) -> *mut PlanState {
    (*node).lefttree
}

#[inline]
unsafe fn innerPlanState(node: *mut PlanState) -> *mut PlanState {
    (*node).righttree
}

#[inline]
unsafe fn outerPlan(node: *mut Plan) -> *mut Plan {
    (*node).lefttree
}

// ---------------------------------------------------------------------------

/*
 * ExecReScan
 *		Reset a plan node so that its output can be re-scanned.
 *
 * Note that if the plan node has parameters that have changed value,
 * the output might be different from last time.
 */
pub unsafe fn ExecReScan(node: *mut PlanState) {
    /* If collecting timing stats, update them */
    if !(*node).instrument.is_null() {
        InstrEndLoop((*node).instrument);
    }

    /*
     * If we have changed parameters, propagate that info.
     *
     * Note: ExecReScanSetParamPlan() can add bits to node->chgParam,
     * corresponding to the output param(s) that the InitPlan will update.
     * Since we make only one pass over the list, that means that an InitPlan
     * can depend on the output param(s) of a sibling InitPlan only if that
     * sibling appears earlier in the list.  This is workable for now given
     * the limited ways in which one InitPlan could depend on another, but
     * eventually we might need to work harder (or else make the planner
     * enlarge the extParam/allParam sets to include the params of depended-on
     * InitPlans).
     */
    if !(*node).chgParam.is_null() {
        foreach!(l, (*node).initPlan, {
            let sstate = lfirst(current_cell!(l)) as *mut SubPlanState;
            let splan = (*sstate).planstate;

            if !(*(*splan).plan).extParam.is_null() {
                /* don't care about child local Params */
                UpdateChangedParamSet(splan, (*node).chgParam);
            }
            if !(*splan).chgParam.is_null() {
                ExecReScanSetParamPlan(sstate, node);
            }
        });
        foreach!(l, (*node).subPlan, {
            let sstate = lfirst(current_cell!(l)) as *mut SubPlanState;
            let splan = (*sstate).planstate;

            if !(*(*splan).plan).extParam.is_null() {
                UpdateChangedParamSet(splan, (*node).chgParam);
            }
        });
        /* Well. Now set chgParam for child trees. */
        if !outerPlanState(node).is_null() {
            UpdateChangedParamSet(outerPlanState(node), (*node).chgParam);
        }
        if !innerPlanState(node).is_null() {
            UpdateChangedParamSet(innerPlanState(node), (*node).chgParam);
        }
    }

    /* Call expression callbacks */
    if !(*node).ps_ExprContext.is_null() {
        ReScanExprContext((*node).ps_ExprContext);
    }

    /* And do node-type-specific processing */
    match nodeTag(node as *mut Node) {
        NodeTag::T_ResultState => ExecReScanResult(node),
        NodeTag::T_ProjectSetState => ExecReScanProjectSet(node),
        NodeTag::T_ModifyTableState => ExecReScanModifyTable(node),
        NodeTag::T_AppendState => ExecReScanAppend(node),
        NodeTag::T_MergeAppendState => ExecReScanMergeAppend(node),
        NodeTag::T_RecursiveUnionState => ExecReScanRecursiveUnion(node),
        NodeTag::T_BitmapAndState => ExecReScanBitmapAnd(node),
        NodeTag::T_BitmapOrState => ExecReScanBitmapOr(node),
        NodeTag::T_SeqScanState => ExecReScanSeqScan(node),
        NodeTag::T_SampleScanState => ExecReScanSampleScan(node),
        NodeTag::T_GatherState => ExecReScanGather(node),
        NodeTag::T_GatherMergeState => ExecReScanGatherMerge(node),
        NodeTag::T_IndexScanState => ExecReScanIndexScan(node),
        NodeTag::T_IndexOnlyScanState => ExecReScanIndexOnlyScan(node),
        NodeTag::T_BitmapIndexScanState => ExecReScanBitmapIndexScan(node),
        NodeTag::T_BitmapHeapScanState => ExecReScanBitmapHeapScan(node),
        NodeTag::T_TidScanState => ExecReScanTidScan(node),
        NodeTag::T_TidRangeScanState => ExecReScanTidRangeScan(node),
        NodeTag::T_SubqueryScanState => ExecReScanSubqueryScan(node),
        NodeTag::T_FunctionScanState => ExecReScanFunctionScan(node),
        NodeTag::T_TableFuncScanState => ExecReScanTableFuncScan(node),
        NodeTag::T_ValuesScanState => ExecReScanValuesScan(node),
        NodeTag::T_CteScanState => ExecReScanCteScan(node),
        NodeTag::T_NamedTuplestoreScanState => ExecReScanNamedTuplestoreScan(node),
        NodeTag::T_WorkTableScanState => ExecReScanWorkTableScan(node),
        NodeTag::T_ForeignScanState => ExecReScanForeignScan(node),
        NodeTag::T_CustomScanState => ExecReScanCustomScan(node),
        NodeTag::T_NestLoopState => ExecReScanNestLoop(node),
        NodeTag::T_MergeJoinState => ExecReScanMergeJoin(node),
        NodeTag::T_HashJoinState => ExecReScanHashJoin(node),
        NodeTag::T_MaterialState => ExecReScanMaterial(node),
        NodeTag::T_MemoizeState => ExecReScanMemoize(node),
        NodeTag::T_SortState => ExecReScanSort(node),
        NodeTag::T_IncrementalSortState => ExecReScanIncrementalSort(node),
        NodeTag::T_GroupState => ExecReScanGroup(node),
        NodeTag::T_AggState => ExecReScanAgg(node),
        NodeTag::T_WindowAggState => ExecReScanWindowAgg(node),
        NodeTag::T_UniqueState => ExecReScanUnique(node),
        NodeTag::T_HashState => ExecReScanHash(node),
        NodeTag::T_SetOpState => ExecReScanSetOp(node),
        NodeTag::T_LockRowsState => ExecReScanLockRows(node),
        NodeTag::T_LimitState => ExecReScanLimit(node),
        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(node as *mut Node) as c_int);
        }
    }

    if !(*node).chgParam.is_null() {
        bms_free((*node).chgParam);
        (*node).chgParam = std::ptr::null_mut();
    }
}

/*
 * ExecMarkPos
 *
 * Marks the current scan position.
 *
 * NOTE: mark/restore capability is currently needed only for plan nodes
 * that are the immediate inner child of a MergeJoin node.  Since MergeJoin
 * requires sorted input, there is never any need to support mark/restore in
 * node types that cannot produce sorted output.  There are some cases in
 * which a node can pass through sorted data from its child; if we don't
 * implement mark/restore for such a node type, the planner compensates by
 * inserting a Material node above that node.
 */
pub unsafe fn ExecMarkPos(node: *mut PlanState) {
    match nodeTag(node as *mut Node) {
        NodeTag::T_IndexScanState => ExecIndexMarkPos(node),
        NodeTag::T_IndexOnlyScanState => ExecIndexOnlyMarkPos(node),
        NodeTag::T_CustomScanState => ExecCustomMarkPos(node),
        NodeTag::T_MaterialState => ExecMaterialMarkPos(node),
        NodeTag::T_SortState => ExecSortMarkPos(node),
        NodeTag::T_ResultState => ExecResultMarkPos(node),
        _ => {
            /* don't make hard error unless caller asks to restore... */
            elog!(DEBUG2, "unrecognized node type: {}", nodeTag(node as *mut Node) as c_int);
        }
    }
}

/*
 * ExecRestrPos
 *
 * restores the scan position previously saved with ExecMarkPos()
 *
 * NOTE: the semantics of this are that the first ExecProcNode following
 * the restore operation will yield the same tuple as the first one following
 * the mark operation.  It is unspecified what happens to the plan node's
 * result TupleTableSlot.  (In most cases the result slot is unchanged by
 * a restore, but the node may choose to clear it or to load it with the
 * restored-to tuple.)	Hence the caller should discard any previously
 * returned TupleTableSlot after doing a restore.
 */
pub unsafe fn ExecRestrPos(node: *mut PlanState) {
    match nodeTag(node as *mut Node) {
        NodeTag::T_IndexScanState => ExecIndexRestrPos(node),
        NodeTag::T_IndexOnlyScanState => ExecIndexOnlyRestrPos(node),
        NodeTag::T_CustomScanState => ExecCustomRestrPos(node),
        NodeTag::T_MaterialState => ExecMaterialRestrPos(node),
        NodeTag::T_SortState => ExecSortRestrPos(node),
        NodeTag::T_ResultState => ExecResultRestrPos(node),
        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(node as *mut Node) as c_int);
        }
    }
}

/*
 * ExecSupportsMarkRestore - does a Path support mark/restore?
 *
 * This is used during planning and so must accept a Path, not a Plan.
 * We keep it here to be adjacent to the routines above, which also must
 * know which plan types support mark/restore.
 */
pub unsafe fn ExecSupportsMarkRestore(pathnode: *mut Path) -> bool {
    /*
     * For consistency with the routines above, we do not examine the nodeTag
     * but rather the pathtype, which is the Plan node type the Path would
     * produce.
     */
    match (*pathnode).pathtype {
        NodeTag::T_IndexScan | NodeTag::T_IndexOnlyScan => {
            /*
             * Not all index types support mark/restore.
             */
            (*(*castNode!(IndexPath, T_IndexPath, pathnode)).indexinfo).amcanmarkpos
        }

        NodeTag::T_Material | NodeTag::T_Sort => true,

        NodeTag::T_CustomScan => {
            if (*castNode!(CustomPath, T_CustomPath, pathnode)).flags
                & CUSTOMPATH_SUPPORT_MARK_RESTORE
                != 0
            {
                return true;
            }
            false
        }

        NodeTag::T_Result => {
            /*
             * Result supports mark/restore iff it has a child plan that does.
             *
             * We have to be careful here because there is more than one Path
             * type that can produce a Result plan node.
             */
            if IsA!(pathnode, T_ProjectionPath) {
                ExecSupportsMarkRestore((*(pathnode as *mut ProjectionPath)).subpath)
            } else if IsA!(pathnode, T_MinMaxAggPath) {
                false /* childless Result */
            } else if IsA!(pathnode, T_GroupResultPath) {
                false /* childless Result */
            } else {
                /* Simple RTE_RESULT base relation */
                Assert!(IsA!(pathnode, T_Path));
                false /* childless Result */
            }
        }

        NodeTag::T_Append => {
            let appendPath = castNode!(AppendPath, T_AppendPath, pathnode);

            /*
             * If there's exactly one child, then there will be no Append
             * in the final plan, so we can handle mark/restore if the
             * child plan node can.
             */
            if list_length((*appendPath).subpaths) == 1 {
                return ExecSupportsMarkRestore(
                    linitial((*appendPath).subpaths) as *mut Path,
                );
            }
            /* Otherwise, Append can't handle it */
            false
        }

        NodeTag::T_MergeAppend => {
            let mapath = castNode!(MergeAppendPath, T_MergeAppendPath, pathnode);

            /*
             * Like the Append case above, single-subpath MergeAppends
             * won't be in the final plan, so just return the child's
             * mark/restore ability.
             */
            if list_length((*mapath).subpaths) == 1 {
                return ExecSupportsMarkRestore(
                    linitial((*mapath).subpaths) as *mut Path,
                );
            }
            /* Otherwise, MergeAppend can't handle it */
            false
        }

        _ => false,
    }
}

/*
 * ExecSupportsBackwardScan - does a plan type support backwards scanning?
 *
 * Ideally, all plan types would support backwards scan, but that seems
 * unlikely to happen soon.  In some cases, a plan node passes the backwards
 * scan down to its children, and so supports backwards scan only if its
 * children do.  Therefore, this routine must be passed a complete plan tree.
 */
pub unsafe fn ExecSupportsBackwardScan(node: *mut Plan) -> bool {
    if node.is_null() {
        return false;
    }

    /*
     * Parallel-aware nodes return a subset of the tuples in each worker, and
     * in general we can't expect to have enough bookkeeping state to know
     * which ones we returned in this worker as opposed to some other worker.
     */
    if (*node).parallel_aware {
        return false;
    }

    match nodeTag(node as *mut Node) {
        NodeTag::T_Result => {
            if !outerPlan(node).is_null() {
                ExecSupportsBackwardScan(outerPlan(node))
            } else {
                false
            }
        }

        NodeTag::T_Append => {
            /* With async, tuples may be interleaved, so can't back up. */
            if (*(node as *mut Append)).nasyncplans > 0 {
                return false;
            }

            foreach!(l, (*(node as *mut Append)).appendplans, {
                if !ExecSupportsBackwardScan(lfirst(current_cell!(l)) as *mut Plan) {
                    return false;
                }
            });
            /* need not check tlist because Append doesn't evaluate it */
            true
        }

        NodeTag::T_SampleScan => {
            /* Simplify life for tablesample methods by disallowing this */
            false
        }

        NodeTag::T_Gather => false,

        NodeTag::T_IndexScan => {
            IndexSupportsBackwardScan((*(node as *mut IndexScan)).indexid)
        }

        NodeTag::T_IndexOnlyScan => {
            IndexSupportsBackwardScan((*(node as *mut IndexOnlyScan)).indexid)
        }

        NodeTag::T_SubqueryScan => {
            ExecSupportsBackwardScan((*(node as *mut SubqueryScan)).subplan)
        }

        NodeTag::T_CustomScan => {
            if (*(node as *mut CustomScan)).flags & CUSTOMPATH_SUPPORT_BACKWARD_SCAN != 0 {
                return true;
            }
            false
        }

        NodeTag::T_SeqScan
        | NodeTag::T_TidScan
        | NodeTag::T_TidRangeScan
        | NodeTag::T_FunctionScan
        | NodeTag::T_ValuesScan
        | NodeTag::T_CteScan
        | NodeTag::T_Material
        | NodeTag::T_Sort => {
            /* these don't evaluate tlist */
            true
        }

        NodeTag::T_IncrementalSort => {
            /*
             * Unlike full sort, incremental sort keeps only a single group of
             * tuples in memory, so it can't scan backwards.
             */
            false
        }

        NodeTag::T_LockRows | NodeTag::T_Limit => ExecSupportsBackwardScan(outerPlan(node)),

        _ => false,
    }
}

/*
 * An IndexScan or IndexOnlyScan node supports backward scan only if the
 * index's AM does.
 */
unsafe fn IndexSupportsBackwardScan(indexid: Oid) -> bool {
    let result: bool;

    /* Fetch the pg_class tuple of the index relation */
    let ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(indexid));
    if !HeapTupleIsValid(ht_idxrel) {
        elog!(ERROR, "cache lookup failed for relation {}", indexid);
    }
    let idxrelrec = GETSTRUCT(ht_idxrel) as Form_pg_class;

    /* Fetch the index AM's API struct */
    let amroutine = GetIndexAmRoutineByAmId((*idxrelrec).relam, false);

    result = (*amroutine).amcanbackward;

    pfree(amroutine as *mut c_void);
    ReleaseSysCache(ht_idxrel);

    result
}

/*
 * ExecMaterializesOutput - does a plan type materialize its output?
 *
 * Returns true if the plan node type is one that automatically materializes
 * its output (typically by keeping it in a tuplestore).  For such plans,
 * a rescan without any parameter change will have zero startup cost and
 * very low per-tuple cost.
 */
pub unsafe fn ExecMaterializesOutput(plantype: NodeTag) -> bool {
    match plantype {
        NodeTag::T_Material
        | NodeTag::T_FunctionScan
        | NodeTag::T_TableFuncScan
        | NodeTag::T_CteScan
        | NodeTag::T_NamedTuplestoreScan
        | NodeTag::T_WorkTableScan
        | NodeTag::T_Sort => true,

        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Additional stub types referenced above (defined fully once their modules
// are ported).
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct IndexPath {
    pub indexinfo: *mut IndexOptInfo,
}

#[repr(C)]
pub struct IndexOptInfo {
    pub amcanmarkpos: bool,
}

#[repr(C)]
pub struct CustomPath {
    pub flags: u32,
}

#[repr(C)]
pub struct ProjectionPath {
    pub subpath: *mut Path,
}

#[repr(C)]
pub struct AppendPath {
    pub subpaths: *mut List,
}

#[repr(C)]
pub struct MergeAppendPath {
    pub subpaths: *mut List,
}

#[repr(C)]
pub struct Append {
    pub nasyncplans: c_int,
    pub appendplans: *mut List,
}

#[repr(C)]
pub struct IndexScan {
    pub indexid: Oid,
}

#[repr(C)]
pub struct IndexOnlyScan {
    pub indexid: Oid,
}

#[repr(C)]
pub struct SubqueryScan {
    pub subplan: *mut Plan,
}

#[repr(C)]
pub struct CustomScan {
    pub flags: u32,
}
