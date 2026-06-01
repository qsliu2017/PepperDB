//! executor support for WHERE CURRENT OF cursor
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! src/backend/executor/execCurrent.c

use crate::prelude::*;
use crate::{IsA, makeNode, castNode, foreach, current_cell};

use std::ffi::{c_char, c_int};

// access/genam.h
use crate::access::relscan::IndexScanDesc;
// access/sysattr.h
use crate::access::sysattr::{SelfItemPointerAttributeNumber, TableOidAttributeNumber};
use crate::access::attnum::AttrNumber;
// catalog/pg_type.h
use crate::catalog::pg_type_d::REFCURSOROID;
// nodes
use crate::nodes::execnodes::{
    AppendState, ExecRowMark, ExprContext, IndexOnlyScanState, ParamListInfo,
    PlanState, ScanState, SubqueryScanState,
};
use crate::nodes::params::ParamExternData;
use crate::nodes::nodes::{nodeTag, NodeTag};
use crate::nodes::primnodes::CurrentOfExpr;
use crate::nodes::plannodes::RowMarkType;
use crate::executor::execdesc::QueryDesc;
use crate::postgres_ext::Oid;
use crate::c::Index;
// storage
use crate::storage::itemptr::ItemPointer;
// utils/portal.h
use crate::utils::portal::{Portal, PortalStrategy, PORTAL_ONE_SELECT};

/*
 * execCurrentOf
 *
 * Given a CURRENT OF expression and the OID of a table, determine which row
 * of the table is currently being scanned by the cursor named by CURRENT OF,
 * and return the row's TID into *current_tid.
 *
 * Returns true if a row was identified.  Returns false if the cursor is valid
 * for the table but is not currently scanning a row of the table (this is a
 * legal situation in inheritance cases).  Raises error if cursor is not a
 * valid updatable scan of the specified table.
 */
pub unsafe fn execCurrentOf(
    cexpr: *mut CurrentOfExpr,
    econtext: *mut ExprContext,
    table_oid: Oid,
    current_tid: ItemPointer,
) -> bool {
    let cursor_name: *mut c_char;
    let table_name: *mut c_char;
    let portal: Portal;
    let queryDesc: *mut QueryDesc;

    /* Get the cursor name --- may have to look up a parameter reference */
    if !(*cexpr).cursor_name.is_null() {
        cursor_name = (*cexpr).cursor_name;
    } else {
        cursor_name = fetch_cursor_param_value(econtext, (*cexpr).cursor_param);
    }

    /* Fetch table name for possible use in error messages */
    table_name = get_rel_name(table_oid);
    if table_name.is_null() {
        elog!(ERROR, "cache lookup failed for relation {}", table_oid);
    }

    /* Find the cursor's portal */
    portal = GetPortalByName(cursor_name);
    if !PortalIsValid(portal) {
        ereport!(ERROR, "cursor does not exist");
        unreachable!()
    }

    /*
     * We have to watch out for non-SELECT queries as well as held cursors,
     * both of which may have null queryDesc.
     */
    if (*portal).strategy != PORTAL_ONE_SELECT {
        ereport!(ERROR, "cursor is not a SELECT query");
        unreachable!()
    }
    queryDesc = (*portal).queryDesc;
    if queryDesc.is_null() || (*queryDesc).estate.is_null() {
        ereport!(ERROR, "cursor is held from a previous transaction");
        unreachable!()
    }

    /*
     * We have two different strategies depending on whether the cursor uses
     * FOR UPDATE/SHARE or not.  The reason for supporting both is that the
     * FOR UPDATE code is able to identify a target table in many cases where
     * the other code can't, while the non-FOR-UPDATE case allows use of WHERE
     * CURRENT OF with an insensitive cursor.
     */
    let estate = (*queryDesc).estate;
    if !(*estate).es_rowmarks.is_null() {
        let mut erm: *mut ExecRowMark;
        let mut i: Index;

        /*
         * Here, the query must have exactly one FOR UPDATE/SHARE reference to
         * the target table, and we dig the ctid info out of that.
         */
        erm = std::ptr::null_mut();
        i = 0;
        while i < (*estate).es_range_table_size {
            let thiserm: *mut ExecRowMark = *(*estate).es_rowmarks.add(i as usize);

            if thiserm.is_null() || !RowMarkRequiresRowShareLock((*thiserm).markType) {
                i += 1;
                continue; /* ignore non-FOR UPDATE/SHARE items */
            }

            if (*thiserm).relid == table_oid {
                if !erm.is_null() {
                    ereport!(
                        ERROR,
                        "cursor has multiple FOR UPDATE/SHARE references to table"
                    );
                    unreachable!()
                }
                erm = thiserm;
            }
            i += 1;
        }

        if erm.is_null() {
            ereport!(
                ERROR,
                "cursor does not have a FOR UPDATE/SHARE reference to table"
            );
            unreachable!()
        }

        /*
         * The cursor must have a current result row: per the SQL spec, it's
         * an error if not.
         */
        if (*portal).atStart || (*portal).atEnd {
            ereport!(ERROR, "cursor is not positioned on a row");
            unreachable!()
        }

        /* Return the currently scanned TID, if there is one */
        if ItemPointerIsValid(&mut (*erm).curCtid) {
            *current_tid = (*erm).curCtid;
            return true;
        }

        /*
         * This table didn't produce the cursor's current row; some other
         * inheritance child of the same parent must have.  Signal caller to
         * do nothing on this table.
         */
        return false;
    } else {
        /*
         * Without FOR UPDATE, we dig through the cursor's plan to find the
         * scan node.  Fail if it's not there or buried underneath
         * aggregation.
         */
        let scanstate: *mut ScanState;
        let mut pending_rescan: bool = false;

        scanstate = search_plan_tree((*queryDesc).planstate, table_oid, &mut pending_rescan);
        if scanstate.is_null() {
            ereport!(ERROR, "cursor is not a simply updatable scan of table");
            unreachable!()
        }

        /*
         * The cursor must have a current result row: per the SQL spec, it's
         * an error if not.  We test this at the top level, rather than at the
         * scan node level, because in inheritance cases any one table scan
         * could easily not be on a row. We want to return false, not raise
         * error, if the passed-in table OID is for one of the inactive scans.
         */
        if (*portal).atStart || (*portal).atEnd {
            ereport!(ERROR, "cursor is not positioned on a row");
            unreachable!()
        }

        /*
         * Now OK to return false if we found an inactive scan.  It is
         * inactive either if it's not positioned on a row, or there's a
         * rescan pending for it.
         */
        if TupIsNull((*scanstate).ss_ScanTupleSlot) || pending_rescan {
            return false;
        }

        /*
         * Extract TID of the scan's current row.  The mechanism for this is
         * in principle scan-type-dependent, but for most scan types, we can
         * just dig the TID out of the physical scan tuple.
         */
        if IsA!(scanstate, T_IndexOnlyScanState) {
            /*
             * For IndexOnlyScan, the tuple stored in ss_ScanTupleSlot may be
             * a virtual tuple that does not have the ctid column, so we have
             * to get the TID from xs_heaptid.
             */
            let scan: IndexScanDesc = (*(scanstate as *mut IndexOnlyScanState)).ioss_ScanDesc as IndexScanDesc;

            *current_tid = (*scan).xs_heaptid;
        } else {
            /*
             * Default case: try to fetch TID from the scan node's current
             * tuple.  As an extra cross-check, verify tableoid in the current
             * tuple.  If the scan hasn't provided a physical tuple, we have
             * to fail.
             */
            let mut ldatum: Datum;
            let mut lisnull: bool = false;
            let tuple_tid: ItemPointer;

            #[cfg(debug_assertions)]
            {
                ldatum = slot_getsysattr(
                    (*scanstate).ss_ScanTupleSlot,
                    TableOidAttributeNumber,
                    &mut lisnull,
                );
                if lisnull {
                    ereport!(ERROR, "cursor is not a simply updatable scan of table");
                    unreachable!()
                }
                assert!(DatumGetObjectId(ldatum) == table_oid);
            }

            ldatum = slot_getsysattr(
                (*scanstate).ss_ScanTupleSlot,
                SelfItemPointerAttributeNumber,
                &mut lisnull,
            );
            if lisnull {
                ereport!(ERROR, "cursor is not a simply updatable scan of table");
                unreachable!()
            }
            tuple_tid = DatumGetPointer(ldatum) as ItemPointer;

            *current_tid = *tuple_tid;
        }

        assert!(ItemPointerIsValid(current_tid));

        return true;
    }
}

/*
 * fetch_cursor_param_value
 *
 * Fetch the string value of a param, verifying it is of type REFCURSOR.
 */
unsafe fn fetch_cursor_param_value(econtext: *mut ExprContext, paramId: c_int) -> *mut c_char {
    let paramInfo: ParamListInfo = (*econtext).ecxt_param_list_info;

    if !paramInfo.is_null() && paramId > 0 && paramId <= (*paramInfo).numParams {
        let prm: *mut ParamExternData;
        let mut prmdata: ParamExternData = std::mem::zeroed();

        /* give hook a chance in case parameter is dynamic */
        if (*paramInfo).paramFetch.is_some() {
            prm = ((*paramInfo).paramFetch.unwrap())(paramInfo, paramId, false, &mut prmdata);
        } else {
            prm = (*paramInfo).params.as_mut_ptr().add((paramId - 1) as usize);
        }

        if OidIsValid((*prm).ptype) && !(*prm).isnull {
            /* safety check in case hook did something unexpected */
            if (*prm).ptype != REFCURSOROID {
                ereport!(
                    ERROR,
                    "type of parameter does not match that when preparing the plan"
                );
                unreachable!()
            }

            /* We know that refcursor uses text's I/O routines */
            return TextDatumGetCString((*prm).value);
        }
    }

    ereport!(ERROR, "no value found for parameter");
    #[allow(unreachable_code)]
    {
        std::ptr::null_mut()
    }
}

/*
 * search_plan_tree
 *
 * Search through a PlanState tree for a scan node on the specified table.
 * Return NULL if not found or multiple candidates.
 *
 * CAUTION: this function is not charged simply with finding some candidate
 * scan, but with ensuring that that scan returned the plan tree's current
 * output row.  That's why we must reject multiple-match cases.
 *
 * If a candidate is found, set *pending_rescan to true if that candidate
 * or any node above it has a pending rescan action, i.e. chgParam != NULL.
 * That indicates that we shouldn't consider the node to be positioned on a
 * valid tuple, even if its own state would indicate that it is.  (Caller
 * must initialize *pending_rescan to false, and should not trust its state
 * if multiple candidates are found.)
 */
unsafe fn search_plan_tree(
    node: *mut PlanState,
    table_oid: Oid,
    pending_rescan: *mut bool,
) -> *mut ScanState {
    let mut result: *mut ScanState = std::ptr::null_mut();

    if node.is_null() {
        return std::ptr::null_mut();
    }
    match nodeTag(node as *const _) {
        /*
         * Relation scan nodes can all be treated alike: check to see if
         * they are scanning the specified table.
         *
         * ForeignScan and CustomScan might not have a currentRelation, in
         * which case we just ignore them.  (We dare not descend to any
         * child plan nodes they might have, since we do not know the
         * relationship of such a node's current output tuple to the
         * children's current outputs.)
         */
        NodeTag::T_SeqScanState
        | NodeTag::T_SampleScanState
        | NodeTag::T_IndexScanState
        | NodeTag::T_IndexOnlyScanState
        | NodeTag::T_BitmapHeapScanState
        | NodeTag::T_TidScanState
        | NodeTag::T_TidRangeScanState
        | NodeTag::T_ForeignScanState
        | NodeTag::T_CustomScanState => {
            let sstate: *mut ScanState = node as *mut ScanState;

            if !(*sstate).ss_currentRelation.is_null()
                && RelationGetRelid((*sstate).ss_currentRelation) == table_oid
            {
                result = sstate;
            }
        }

        /*
         * For Append, we can check each input node.  It is safe to
         * descend to the inputs because only the input that resulted in
         * the Append's current output node could be positioned on a tuple
         * at all; the other inputs are either at EOF or not yet started.
         * Hence, if the desired table is scanned by some
         * currently-inactive input node, we will find that node but then
         * our caller will realize that it didn't emit the tuple of
         * interest.
         *
         * We do need to watch out for multiple matches (possible if
         * Append was from UNION ALL rather than an inheritance tree).
         *
         * Note: we can NOT descend through MergeAppend similarly, since
         * its inputs are likely all active, and we don't know which one
         * returned the current output tuple.  (Perhaps that could be
         * fixed if we were to let this code know more about MergeAppend's
         * internal state, but it does not seem worth the trouble.  Users
         * should not expect plans for ORDER BY queries to be considered
         * simply-updatable, since they won't be if the sorting is
         * implemented by a Sort node.)
         */
        NodeTag::T_AppendState => {
            let astate: *mut AppendState = node as *mut AppendState;
            let mut i: c_int;

            i = 0;
            while i < (*astate).as_nplans {
                let elem: *mut ScanState = search_plan_tree(
                    *(*astate).appendplans.add(i as usize),
                    table_oid,
                    pending_rescan,
                );

                if elem.is_null() {
                    i += 1;
                    continue;
                }
                if !result.is_null() {
                    return std::ptr::null_mut(); /* multiple matches */
                }
                result = elem;
                i += 1;
            }
        }

        /*
         * Result and Limit can be descended through (these are safe
         * because they always return their input's current row)
         */
        NodeTag::T_ResultState | NodeTag::T_LimitState => {
            result = search_plan_tree(outerPlanState(node), table_oid, pending_rescan);
        }

        /*
         * SubqueryScan too, but it keeps the child in a different place
         */
        NodeTag::T_SubqueryScanState => {
            result = search_plan_tree(
                (*(node as *mut SubqueryScanState)).subplan,
                table_oid,
                pending_rescan,
            );
        }

        _ => {
            /* Otherwise, assume we can't descend through it */
        }
    }

    /*
     * If we found a candidate at or below this node, then this node's
     * chgParam indicates a pending rescan that will affect the candidate.
     */
    if !result.is_null() && !(*node).chgParam.is_null() {
        *pending_rescan = true;
    }

    result
}

// ---------------------------------------------------------------------------
// Local stubs for unported helpers
// ---------------------------------------------------------------------------

unsafe fn get_rel_name(_relid: Oid) -> *mut c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}

unsafe fn GetPortalByName(_name: *mut c_char) -> Portal {
    unimplemented!() // TODO: utils/mmgr/portalmem.c
}

unsafe fn PortalIsValid(p: Portal) -> bool {
    !p.is_null()
}

unsafe fn RowMarkRequiresRowShareLock(_marktype: RowMarkType) -> bool {
    unimplemented!() // TODO: executor/execMain.c
}

unsafe fn ItemPointerIsValid(_pointer: ItemPointer) -> bool {
    unimplemented!() // TODO: storage/itemptr.h
}

unsafe fn TupIsNull(_slot: *mut crate::nodes::execnodes::TupleTableSlot) -> bool {
    unimplemented!() // TODO: executor/tuptable.h
}

unsafe fn slot_getsysattr(
    _slot: *mut crate::nodes::execnodes::TupleTableSlot,
    _attnum: AttrNumber,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: executor/execTuples.c
}

unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/varlena.c
}

unsafe fn RelationGetRelid(_relation: *mut crate::utils::rel::RelationData) -> Oid {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn outerPlanState(_node: *mut PlanState) -> *mut PlanState {
    unimplemented!() // TODO: nodes/execnodes.h
}
