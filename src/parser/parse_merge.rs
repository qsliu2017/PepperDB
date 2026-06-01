//! parser/parse_merge.c - handle merge-statement in parser
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::{current_cell, foreach, forthree, lfirst_node, list_make1, makeNode};

use crate::access::attnum::AttrNumber;
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;
use crate::catalog::pg_class::{RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW};
use crate::nodes::bitmapset::bms_add_member;
use crate::nodes::makefuncs::{makeFromExpr, makeTargetEntry};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::nodes::CmdType::*;
use crate::nodes::parsenodes::{
    AclMode, MergeStmt, MergeWhenClause, Query, RTEPermissionInfo, RangeTblEntry, ResTarget,
    ACL_DELETE, ACL_INSERT, ACL_NO_RIGHTS, ACL_SELECT, ACL_UPDATE,
};
use crate::nodes::pg_list::{lappend, lfirst, lfirst_int, list_length, List, NIL};
use crate::nodes::primnodes::{
    Expr, MergeAction, TargetEntry, NUM_MERGE_MATCH_KINDS,
};
use crate::nodes::primnodes::MergeMatchKind::*;
use crate::utils::rel::{RelationData, RelationGetRelationName};

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

// ---------------------------------------------------------------------------
// Stubs for unported parser/relcache dependencies.
//
// The full ParseState (parser/parse_node.h) is not yet ported - only opaque
// stubs exist elsewhere - so we define a local struct carrying the fields this
// file references. ParseNamespaceItem / its subsidiary types and the parser
// transform routines are likewise unported and stubbed here.
// ---------------------------------------------------------------------------

pub type Relation = *mut RelationData;
pub type Index = c_uint;

/* parser/parse_node.h: ParseExprKind */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ParseExprKind {
    EXPR_KIND_NONE = 0,
    EXPR_KIND_JOIN_ON,
    EXPR_KIND_MERGE_WHEN,
    EXPR_KIND_MERGE_RETURNING,
    EXPR_KIND_VALUES_SINGLE,
}
pub use ParseExprKind::*;

/* primnodes.h: Alias (only aliasname needed here) */
#[repr(C)]
pub struct ParseNamespaceNames {
    pub r#type: NodeTag,
    pub aliasname: *mut c_char,
}

/* parser/parse_node.h: ParseNamespaceItem (partial) */
#[repr(C)]
pub struct ParseNamespaceItem {
    pub p_names: *mut ParseNamespaceNames,
    pub p_rte: *mut RangeTblEntry,
    pub p_perminfo: *mut RTEPermissionInfo,
    pub p_rel_visible: bool,
    pub p_cols_visible: bool,
}

/* parser/parse_node.h: ParseState (partial) */
#[repr(C)]
pub struct ParseState {
    pub p_rtable: *mut List,
    pub p_rteperminfos: *mut List,
    pub p_joinlist: *mut List,
    pub p_namespace: *mut List,
    pub p_ctenamespace: *mut List,
    pub p_target_relation: Relation,
    pub p_target_nsitem: *mut ParseNamespaceItem,
    pub p_is_insert: bool,
    pub p_hasSubLinks: bool,
    pub p_hasModifyingCTE: bool,
}

/* parser/parsetree.h: rt_fetch(rangetable_index, rangetable) */
#[inline]
unsafe fn rt_fetch(rangetable_index: Index, rangetable: *mut List) -> *mut RangeTblEntry {
    list_nth(rangetable, (rangetable_index - 1) as c_int) as *mut RangeTblEntry
}

unsafe fn list_nth(list: *mut List, n: c_int) -> *mut c_void {
    lfirst((*list).elements.add(n as usize))
}

// Unported parser transform routines (parser/analyze.h, parse_clause.h,
// parse_collate.h, parse_cte.h, parse_expr.h, parse_relation.h,
// parse_target.h). Stubbed locally until ported.

unsafe fn transformWithClause(_pstate: *mut ParseState, _withClause: *mut c_void) -> *mut List {
    unimplemented!()
}
unsafe fn setTargetTable(
    _pstate: *mut ParseState,
    _relation: *mut c_void,
    _inh: bool,
    _alsoSource: bool,
    _requiredPerms: AclMode,
) -> c_int {
    unimplemented!()
}
unsafe fn transformFromClause(_pstate: *mut ParseState, _frmList: *mut List) {
    unimplemented!()
}
unsafe fn GetNSItemByRangeTablePosn(
    _pstate: *mut ParseState,
    _varno: c_int,
    _sublevels_up: c_int,
) -> *mut ParseNamespaceItem {
    unimplemented!()
}
unsafe fn addNSItemToQuery(
    _pstate: *mut ParseState,
    _nsitem: *mut ParseNamespaceItem,
    _addToJoinList: bool,
    _addToRelNameSpace: bool,
    _addToVarNameSpace: bool,
) {
    unimplemented!()
}
unsafe fn transformExpr(
    _pstate: *mut ParseState,
    _expr: *mut Node,
    _exprKind: ParseExprKind,
) -> *mut Node {
    unimplemented!()
}
unsafe fn transformReturningClause(
    _pstate: *mut ParseState,
    _qry: *mut Query,
    _returningClause: *mut c_void,
    _exprKind: ParseExprKind,
) {
    unimplemented!()
}
unsafe fn transformWhereClause(
    _pstate: *mut ParseState,
    _clause: *mut Node,
    _exprKind: ParseExprKind,
    _constructName: *const c_char,
) -> *mut Node {
    unimplemented!()
}
unsafe fn checkInsertTargets(
    _pstate: *mut ParseState,
    _cols: *mut List,
    _attrnos: *mut *mut List,
) -> *mut List {
    unimplemented!()
}
unsafe fn transformExpressionList(
    _pstate: *mut ParseState,
    _exprlist: *mut List,
    _exprKind: ParseExprKind,
    _allowDefault: bool,
) -> *mut List {
    unimplemented!()
}
unsafe fn transformInsertRow(
    _pstate: *mut ParseState,
    _exprlist: *mut List,
    _stmtcols: *mut List,
    _icolumns: *mut List,
    _attrnos: *mut List,
    _strip_indirection: bool,
) -> *mut List {
    unimplemented!()
}
unsafe fn transformUpdateTargetList(_pstate: *mut ParseState, _targetList: *mut List) -> *mut List {
    unimplemented!()
}
unsafe fn assign_query_collations(_pstate: *mut ParseState, _query: *mut Query) {
    unimplemented!()
}

/* access/table.h: errdetail_relkind_not_supported (not ported yet) */
unsafe fn errdetail_relkind_not_supported(_relkind: c_char) -> c_int {
    unimplemented!()
}

// ---------------------------------------------------------------------------

/*
 * Make appropriate changes to the namespace visibility while transforming
 * individual action's quals and targetlist expressions. In particular, for
 * INSERT actions we must only see the source relation (since INSERT action is
 * invoked for NOT MATCHED [BY TARGET] tuples and hence there is no target
 * tuple to deal with). On the other hand, UPDATE and DELETE actions can see
 * both source and target relations, unless invoked for NOT MATCHED BY SOURCE.
 *
 * Also, since the internal join node can hide the source and target
 * relations, we must explicitly make the respective relation as visible so
 * that columns can be referenced unqualified from these relations.
 */
unsafe fn setNamespaceForMergeWhen(
    pstate: *mut ParseState,
    mergeWhenClause: *mut MergeWhenClause,
    targetRTI: Index,
    sourceRTI: Index,
) {
    let targetRelRTE: *mut RangeTblEntry;
    let sourceRelRTE: *mut RangeTblEntry;

    targetRelRTE = rt_fetch(targetRTI, (*pstate).p_rtable);
    sourceRelRTE = rt_fetch(sourceRTI, (*pstate).p_rtable);

    if (*mergeWhenClause).matchKind == MERGE_WHEN_MATCHED {
        Assert!(
            (*mergeWhenClause).commandType == CMD_UPDATE
                || (*mergeWhenClause).commandType == CMD_DELETE
                || (*mergeWhenClause).commandType == CMD_NOTHING
        );

        /* MATCHED actions can see both target and source relations. */
        setNamespaceVisibilityForRTE((*pstate).p_namespace, targetRelRTE, true, true);
        setNamespaceVisibilityForRTE((*pstate).p_namespace, sourceRelRTE, true, true);
    } else if (*mergeWhenClause).matchKind == MERGE_WHEN_NOT_MATCHED_BY_SOURCE {
        /*
         * NOT MATCHED BY SOURCE actions can see the target relation, but they
         * can't see the source relation.
         */
        Assert!(
            (*mergeWhenClause).commandType == CMD_UPDATE
                || (*mergeWhenClause).commandType == CMD_DELETE
                || (*mergeWhenClause).commandType == CMD_NOTHING
        );
        setNamespaceVisibilityForRTE((*pstate).p_namespace, targetRelRTE, true, true);
        setNamespaceVisibilityForRTE((*pstate).p_namespace, sourceRelRTE, false, false);
    } else
    /* MERGE_WHEN_NOT_MATCHED_BY_TARGET */
    {
        /*
         * NOT MATCHED [BY TARGET] actions can't see target relation, but they
         * can see source relation.
         */
        Assert!(
            (*mergeWhenClause).commandType == CMD_INSERT
                || (*mergeWhenClause).commandType == CMD_NOTHING
        );
        setNamespaceVisibilityForRTE((*pstate).p_namespace, targetRelRTE, false, false);
        setNamespaceVisibilityForRTE((*pstate).p_namespace, sourceRelRTE, true, true);
    }
}

/*
 * transformMergeStmt -
 *	  transforms a MERGE statement
 */
pub unsafe fn transformMergeStmt(pstate: *mut ParseState, stmt: *mut MergeStmt) -> *mut Query {
    let qry: *mut Query = makeNode!(Query, T_Query);
    let mut l: *mut crate::nodes::pg_list::ListCell;
    let mut targetPerms: AclMode = ACL_NO_RIGHTS;
    let mut is_terminal: [bool; NUM_MERGE_MATCH_KINDS as usize] =
        [false; NUM_MERGE_MATCH_KINDS as usize];
    let sourceRTI: Index;
    let mut mergeActionList: *mut List;
    let nsitem: *mut ParseNamespaceItem;

    /* There can't be any outer WITH to worry about */
    Assert!((*pstate).p_ctenamespace == NIL);

    (*qry).commandType = CMD_MERGE;
    (*qry).hasRecursive = false;

    /* process the WITH clause independently of all else */
    if !(*stmt).withClause.is_null() {
        if (*(*stmt).withClause).recursive {
            ereport!(
                ERROR,
                "WITH RECURSIVE is not supported for MERGE statement"
            );
        }

        (*qry).cteList = transformWithClause(pstate, (*stmt).withClause as *mut c_void);
        (*qry).hasModifyingCTE = (*pstate).p_hasModifyingCTE;
    }

    /*
     * Check WHEN clauses for permissions and sanity
     */
    is_terminal[MERGE_WHEN_MATCHED as usize] = false;
    is_terminal[MERGE_WHEN_NOT_MATCHED_BY_SOURCE as usize] = false;
    is_terminal[MERGE_WHEN_NOT_MATCHED_BY_TARGET as usize] = false;
    foreach!(l, (*stmt).mergeWhenClauses, {
        let mergeWhenClause: *mut MergeWhenClause =
            lfirst(current_cell!(l)) as *mut MergeWhenClause;

        /*
         * Collect permissions to check, according to action types. We require
         * SELECT privileges for DO NOTHING because it'd be irregular to have
         * a target relation with zero privileges checked, in case DO NOTHING
         * is the only action.  There's no damage from that: any meaningful
         * MERGE command requires at least some access to the table anyway.
         */
        match (*mergeWhenClause).commandType {
            CMD_INSERT => {
                targetPerms |= ACL_INSERT;
            }
            CMD_UPDATE => {
                targetPerms |= ACL_UPDATE;
            }
            CMD_DELETE => {
                targetPerms |= ACL_DELETE;
            }
            CMD_NOTHING => {
                targetPerms |= ACL_SELECT;
            }
            _ => {
                elog!(ERROR, "unknown action in MERGE WHEN clause");
            }
        }

        /*
         * Check for unreachable WHEN clauses
         */
        if is_terminal[(*mergeWhenClause).matchKind as usize] {
            ereport!(
                ERROR,
                "unreachable WHEN clause specified after unconditional WHEN clause"
            );
        }
        if (*mergeWhenClause).condition.is_null() {
            is_terminal[(*mergeWhenClause).matchKind as usize] = true;
        }
    });

    /*
     * Set up the MERGE target table.  The target table is added to the
     * namespace below and to joinlist in transform_MERGE_to_join, so don't do
     * it here.
     *
     * Initially mergeTargetRelation is the same as resultRelation, so data is
     * read from the table being updated.  However, that might be changed by
     * the rewriter, if the target is a trigger-updatable view, to allow
     * target data to be read from the expanded view query while updating the
     * original view relation.
     */
    (*qry).resultRelation = setTargetTable(
        pstate,
        (*stmt).relation as *mut c_void,
        (*(*stmt).relation).inh,
        false,
        targetPerms,
    );
    (*qry).mergeTargetRelation = (*qry).resultRelation;

    /* The target relation must be a table or a view */
    if (*(*(*pstate).p_target_relation).rd_rel).relkind != RELKIND_RELATION
        && (*(*(*pstate).p_target_relation).rd_rel).relkind != RELKIND_PARTITIONED_TABLE
        && (*(*(*pstate).p_target_relation).rd_rel).relkind != RELKIND_VIEW
    {
        let _ = errdetail_relkind_not_supported(
            (*(*(*pstate).p_target_relation).rd_rel).relkind,
        );
        let _ = RelationGetRelationName((*pstate).p_target_relation);
        ereport!(ERROR, "cannot execute MERGE on relation");
    }

    /* Now transform the source relation to produce the source RTE. */
    transformFromClause(pstate, list_make1!((*stmt).sourceRelation as *mut c_void));
    sourceRTI = list_length((*pstate).p_rtable) as Index;
    nsitem = GetNSItemByRangeTablePosn(pstate, sourceRTI as c_int, 0);

    /*
     * Check that the target table doesn't conflict with the source table.
     * This would typically be a checkNameSpaceConflicts call, but we want a
     * more specific error message.
     */
    if strcmp(
        (*(*(*pstate).p_target_nsitem).p_names).aliasname,
        (*(*nsitem).p_names).aliasname,
    ) == 0
    {
        ereport!(ERROR, "name specified more than once");
    }

    /*
     * There's no need for a targetlist here; it'll be set up by
     * preprocess_targetlist later.
     */
    (*qry).targetList = NIL;
    (*qry).rtable = (*pstate).p_rtable;
    (*qry).rteperminfos = (*pstate).p_rteperminfos;

    /*
     * Transform the join condition.  This includes references to the target
     * side, so add that to the namespace.
     */
    addNSItemToQuery(pstate, (*pstate).p_target_nsitem, false, true, true);
    (*qry).mergeJoinCondition =
        transformExpr(pstate, (*stmt).joinCondition, EXPR_KIND_JOIN_ON);

    /*
     * Create the temporary query's jointree using the joinlist we built using
     * just the source relation; the target relation is not included. The join
     * will be constructed fully by transform_MERGE_to_join.
     */
    (*qry).jointree = makeFromExpr((*pstate).p_joinlist, null_mut());

    /* Transform the RETURNING list, if any */
    transformReturningClause(
        pstate,
        qry,
        (*stmt).returningClause as *mut c_void,
        EXPR_KIND_MERGE_RETURNING,
    );

    /*
     * We now have a good query shape, so now look at the WHEN conditions and
     * action targetlists.
     *
     * Overall, the MERGE Query's targetlist is NIL.
     *
     * Each individual action has its own targetlist that needs separate
     * transformation. These transforms don't do anything to the overall
     * targetlist, since that is only used for resjunk columns.
     *
     * We can reference any column in Target or Source, which is OK because
     * both of those already have RTEs. There is nothing like the EXCLUDED
     * pseudo-relation for INSERT ON CONFLICT.
     */
    mergeActionList = NIL;
    foreach!(l, (*stmt).mergeWhenClauses, {
        let mergeWhenClause: *mut MergeWhenClause =
            lfirst_node!(MergeWhenClause, T_MergeWhenClause, current_cell!(l));
        let action: *mut MergeAction;

        action = makeNode!(MergeAction, T_MergeAction);
        (*action).commandType = (*mergeWhenClause).commandType;
        (*action).matchKind = (*mergeWhenClause).matchKind;

        /*
         * Set namespace for the specific action. This must be done before
         * analyzing the WHEN quals and the action targetlist.
         */
        setNamespaceForMergeWhen(
            pstate,
            mergeWhenClause,
            (*qry).resultRelation as Index,
            sourceRTI,
        );

        /*
         * Transform the WHEN condition.
         *
         * Note that these quals are NOT added to the join quals; instead they
         * are evaluated separately during execution to decide which of the
         * WHEN MATCHED or WHEN NOT MATCHED actions to execute.
         */
        (*action).qual = transformWhereClause(
            pstate,
            (*mergeWhenClause).condition,
            EXPR_KIND_MERGE_WHEN,
            c"WHEN".as_ptr(),
        );

        /*
         * Transform target lists for each INSERT and UPDATE action stmt
         */
        match (*action).commandType {
            CMD_INSERT => {
                let mut exprList: *mut List;
                let mut lc: *mut crate::nodes::pg_list::ListCell;
                let perminfo: *mut RTEPermissionInfo;
                let mut icols: *mut crate::nodes::pg_list::ListCell;
                let mut attnos: *mut crate::nodes::pg_list::ListCell;
                let icolumns: *mut List;
                let mut attrnos: *mut List = null_mut();

                (*pstate).p_is_insert = true;

                icolumns = checkInsertTargets(
                    pstate,
                    (*mergeWhenClause).targetList,
                    &mut attrnos,
                );
                Assert!(list_length(icolumns) == list_length(attrnos));

                (*action).r#override = (*mergeWhenClause).r#override;

                /*
                 * Handle INSERT much like in transformInsertStmt
                 */
                if (*mergeWhenClause).values == NIL {
                    /*
                     * We have INSERT ... DEFAULT VALUES.  We can handle
                     * this case by emitting an empty targetlist --- all
                     * columns will be defaulted when the planner expands
                     * the targetlist.
                     */
                    exprList = NIL;
                } else {
                    /*
                     * Process INSERT ... VALUES with a single VALUES
                     * sublist.  We treat this case separately for
                     * efficiency.  The sublist is just computed directly
                     * as the Query's targetlist, with no VALUES RTE.  So
                     * it works just like a SELECT without any FROM.
                     */

                    /*
                     * Do basic expression transformation (same as a ROW()
                     * expr, but allow SetToDefault at top level)
                     */
                    exprList = transformExpressionList(
                        pstate,
                        (*mergeWhenClause).values,
                        EXPR_KIND_VALUES_SINGLE,
                        true,
                    );

                    /* Prepare row for assignment to target table */
                    exprList = transformInsertRow(
                        pstate,
                        exprList,
                        (*mergeWhenClause).targetList,
                        icolumns,
                        attrnos,
                        false,
                    );
                }

                /*
                 * Generate action's target list using the computed list
                 * of expressions. Also, mark all the target columns as
                 * needing insert permissions.
                 */
                perminfo = (*(*pstate).p_target_nsitem).p_perminfo;
                forthree!(lc, exprList, icols, icolumns, attnos, attrnos, {
                    let expr: *mut Expr = lfirst(lc) as *mut Expr;
                    let col: *mut ResTarget = lfirst_node!(ResTarget, T_ResTarget, icols);
                    let attr_num: AttrNumber = lfirst_int(attnos) as AttrNumber;
                    let tle: *mut TargetEntry;

                    tle = makeTargetEntry(expr, attr_num, (*col).name, false);
                    (*action).targetList = lappend((*action).targetList, tle as *mut c_void);

                    (*perminfo).insertedCols = bms_add_member(
                        (*perminfo).insertedCols,
                        attr_num as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
                    );
                });
            }
            CMD_UPDATE => {
                (*pstate).p_is_insert = false;
                (*action).targetList =
                    transformUpdateTargetList(pstate, (*mergeWhenClause).targetList);
            }
            CMD_DELETE => {}

            CMD_NOTHING => {
                (*action).targetList = NIL;
            }
            _ => {
                elog!(ERROR, "unknown action in MERGE WHEN clause");
            }
        }

        mergeActionList = lappend(mergeActionList, action as *mut c_void);
    });

    (*qry).mergeActionList = mergeActionList;

    (*qry).hasTargetSRFs = false;
    (*qry).hasSubLinks = (*pstate).p_hasSubLinks;

    assign_query_collations(pstate, qry);

    qry
}

unsafe fn setNamespaceVisibilityForRTE(
    namespace: *mut List,
    rte: *mut RangeTblEntry,
    rel_visible: bool,
    cols_visible: bool,
) {
    let mut lc: *mut crate::nodes::pg_list::ListCell;

    foreach!(lc, namespace, {
        let nsitem: *mut ParseNamespaceItem =
            lfirst(current_cell!(lc)) as *mut ParseNamespaceItem;

        if (*nsitem).p_rte == rte {
            (*nsitem).p_rel_visible = rel_visible;
            (*nsitem).p_cols_visible = cols_visible;
            break;
        }
    });
}
