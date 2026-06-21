/*-------------------------------------------------------------------------
 *
 * preptlist.c
 *	  Routines to preprocess the parse tree target list
 *
 * For an INSERT, the targetlist must contain an entry for each attribute of
 * the target relation in the correct order.
 *
 * For an UPDATE, the targetlist just contains the expressions for the new
 * column values.
 *
 * For UPDATE and DELETE queries, the targetlist must also contain "junk"
 * tlist entries needed to allow the executor to identify the rows to be
 * updated or deleted; for example, the ctid of a heap row.  (The planner
 * adds these; they're not in what we receive from the parser/rewriter.)
 *
 * For all query types, there can be additional junk tlist entries, such as
 * sort keys, Vars needed for a RETURNING list, and row ID information needed
 * for SELECT FOR UPDATE locking and/or EvalPlanQual checking.
 *
 * The query rewrite phase also does preprocessing of the targetlist (see
 * rewriteTargetListIU).  The division of labor between here and there is
 * partially historical, but it's not entirely arbitrary.  The stuff done
 * here is closely connected to physical access to tables, whereas the
 * rewriter's work is more concerned with SQL semantics.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/prep/preptlist.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use crate::{foreach, current_cell, IsA};
use crate::access::attnum::AttrNumber;
use crate::nodes::primnodes::MergeAction;
use crate::access::common::tupdesc::TupleDesc;
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::{Query, RangeTblEntry, RTEKind};
use crate::nodes::pathnodes::PlannerInfo;
use crate::nodes::pg_list::{lfirst, List, ListCell};
use crate::nodes::plannodes::PlanRowMark;
use crate::nodes::primnodes::{Const, Expr, TargetEntry, Var};
use crate::postgres_ext::Oid;
use crate::utils::rel::Relation;

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

/*
 * preprocess_targetlist
 *	  Driver for preprocessing the parse tree targetlist.
 *
 * The preprocessed targetlist is returned in root->processed_tlist.
 * Also, if this is an UPDATE, we return a list of target column numbers
 * in root->update_colnos.  (Resnos in processed_tlist will be consecutive,
 * so do not look at that to find out which columns are targets!)
 */
pub unsafe fn preprocess_targetlist(root: *mut PlannerInfo) {
    let parse: *mut Query = (*root).parse;
    let result_relation: c_int = (*parse).resultRelation;
    let range_table: *mut List = (*parse).rtable;
    let command_type: CmdType = (*parse).commandType;
    let mut target_rte: *mut RangeTblEntry = std::ptr::null_mut();
    let mut target_relation: Relation = std::ptr::null_mut();
    let mut tlist: *mut List;
    let lc: *mut ListCell;

    /*
     * If there is a result relation, open it so we can look for missing
     * columns and so on.  We assume that previous code already acquired at
     * least AccessShareLock on the relation, so we need no lock here.
     */
    if result_relation != 0 {
        target_rte = rt_fetch(result_relation, range_table);

        /*
         * Sanity check: it'd better be a real relation not, say, a subquery.
         * Else parser or rewriter messed up.
         */
        if (*target_rte).rtekind != RTEKind::RTE_RELATION {
            elog!(ERROR, "result relation must be a regular relation");
        }

        target_relation = table_open((*target_rte).relid, NoLock as LOCKMODE);
    } else {
        Assert!(command_type == CmdType::CMD_SELECT);
    }

    /*
     * In an INSERT, the executor expects the targetlist to match the exact
     * order of the target table's attributes, including entries for
     * attributes not mentioned in the source query.
     *
     * In an UPDATE, we don't rearrange the tlist order, but we need to make a
     * separate list of the target attribute numbers, in tlist order, and then
     * renumber the processed_tlist entries to be consecutive.
     */
    tlist = (*parse).targetList;
    if command_type == CmdType::CMD_INSERT {
        tlist = expand_insert_targetlist(root, tlist, target_relation);
    } else if command_type == CmdType::CMD_UPDATE {
        (*root).update_colnos = extract_update_targetlist_colnos(tlist);
    }

    /*
     * For non-inherited UPDATE/DELETE/MERGE, register any junk column(s)
     * needed to allow the executor to identify the rows to be updated or
     * deleted.  In the inheritance case, we do nothing now, leaving this to
     * be dealt with when expand_inherited_rtentry() makes the leaf target
     * relations.  (But there might not be any leaf target relations, in which
     * case we must do this in distribute_row_identity_vars().)
     */
    if (command_type == CmdType::CMD_UPDATE
        || command_type == CmdType::CMD_DELETE
        || command_type == CmdType::CMD_MERGE)
        && !(*target_rte).inh
    {
        /* row-identity logic expects to add stuff to processed_tlist */
        (*root).processed_tlist = tlist;
        add_row_identity_columns(root, result_relation, target_rte, target_relation);
        tlist = (*root).processed_tlist;
    }

    /*
     * For MERGE we also need to handle the target list for each INSERT and
     * UPDATE action separately.  In addition, we examine the qual of each
     * action and add any Vars there (other than those of the target rel) to
     * the subplan targetlist.
     */
    if command_type == CmdType::CMD_MERGE {
        let l: *mut ListCell;
        let mut vars: *mut List;

        /*
         * For MERGE, handle targetlist of each MergeAction separately. Give
         * the same treatment to MergeAction->targetList as we would have
         * given to a regular INSERT.  For UPDATE, collect the column numbers
         * being modified.
         */
        foreach!(l, (*parse).mergeActionList, {
            let action: *mut MergeAction = lfirst(current_cell!(l)) as *mut MergeAction;
            let l2: *mut ListCell;

            if (*action).commandType == CmdType::CMD_INSERT {
                (*action).targetList =
                    expand_insert_targetlist(root, (*action).targetList, target_relation);
            } else if (*action).commandType == CmdType::CMD_UPDATE {
                (*action).updateColnos =
                    extract_update_targetlist_colnos((*action).targetList);
            }

            /*
             * Add resjunk entries for any Vars and PlaceHolderVars used in
             * each action's targetlist and WHEN condition that belong to
             * relations other than the target.  We don't expect to see any
             * aggregates or window functions here.
             */
            vars = pull_var_clause(
                list_concat_copy((*action).qual as *mut List, (*action).targetList) as *mut Node,
                PVC_INCLUDE_PLACEHOLDERS as c_int,
            );
            foreach!(l2, vars, {
                let var: *mut Var = lfirst(current_cell!(l2)) as *mut Var;
                let tle: *mut TargetEntry;

                if IsA!(var, T_Var) && (*var).varno == result_relation {
                    continue; /* don't need it */
                }

                if tlist_member(var as *mut Expr, tlist) != std::ptr::null_mut() {
                    continue; /* already got it */
                }

                tle = makeTargetEntry(
                    var as *mut Expr,
                    (list_length(tlist) + 1) as AttrNumber,
                    std::ptr::null_mut(),
                    true,
                );
                tlist = lappend(tlist, tle as *mut c_void);
            });
            list_free(vars);
        });

        /*
         * Add resjunk entries for any Vars and PlaceHolderVars used in the
         * join condition that belong to relations other than the target.  We
         * don't expect to see any aggregates or window functions here.
         */
        vars = pull_var_clause((*parse).mergeJoinCondition, PVC_INCLUDE_PLACEHOLDERS as c_int);
        foreach!(l, vars, {
            let var: *mut Var = lfirst(current_cell!(l)) as *mut Var;
            let tle: *mut TargetEntry;

            if IsA!(var, T_Var) && (*var).varno == result_relation {
                continue; /* don't need it */
            }

            if tlist_member(var as *mut Expr, tlist) != std::ptr::null_mut() {
                continue; /* already got it */
            }

            tle = makeTargetEntry(
                var as *mut Expr,
                (list_length(tlist) + 1) as AttrNumber,
                std::ptr::null_mut(),
                true,
            );
            tlist = lappend(tlist, tle as *mut c_void);
        });
    }

    /*
     * Add necessary junk columns for rowmarked rels.  These values are needed
     * for locking of rels selected FOR UPDATE/SHARE, and to do EvalPlanQual
     * rechecking.  See comments for PlanRowMark in plannodes.h.  If you
     * change this stanza, see also expand_inherited_rtentry(), which has to
     * be able to add on junk columns equivalent to these.
     *
     * (Someday it might be useful to fold these resjunk columns into the
     * row-identity-column management used for UPDATE/DELETE.  Today is not
     * that day, however.  One notable issue is that it seems important that
     * the whole-row Vars made here use the real table rowtype, not RECORD, so
     * that conversion to/from child relations' rowtypes will happen.  Also,
     * since these entries don't potentially bloat with more and more child
     * relations, there's not really much need for column sharing.)
     */
    foreach!(lc, (*root).rowMarks, {
        let rc: *mut PlanRowMark = lfirst(current_cell!(lc)) as *mut PlanRowMark;
        let mut var: *mut Var;
        let mut resname: [c_char; 32] = [0; 32];
        let mut tle: *mut TargetEntry;

        /* child rels use the same junk attrs as their parents */
        if (*rc).rti != (*rc).prti {
            continue;
        }

        if (*rc).allMarkTypes & !(1 << ROW_MARK_COPY) != 0 {
            /* Need to fetch TID */
            var = makeVar(
                (*rc).rti,
                SelfItemPointerAttributeNumber as AttrNumber,
                TIDOID,
                -1,
                InvalidOid,
                0,
            );
            snprintf(
                resname.as_mut_ptr(),
                std::mem::size_of::<[c_char; 32]>(),
                c"ctid%u".as_ptr(),
                (*rc).rowmarkId,
            );
            tle = makeTargetEntry(
                var as *mut Expr,
                (list_length(tlist) + 1) as AttrNumber,
                pstrdup(resname.as_ptr()),
                true,
            );
            tlist = lappend(tlist, tle as *mut c_void);
        }
        if (*rc).allMarkTypes & (1 << ROW_MARK_COPY) != 0 {
            /* Need the whole row as a junk var */
            var = makeWholeRowVar(rt_fetch((*rc).rti as c_int, range_table), (*rc).rti, 0, false);
            snprintf(
                resname.as_mut_ptr(),
                std::mem::size_of::<[c_char; 32]>(),
                c"wholerow%u".as_ptr(),
                (*rc).rowmarkId,
            );
            tle = makeTargetEntry(
                var as *mut Expr,
                (list_length(tlist) + 1) as AttrNumber,
                pstrdup(resname.as_ptr()),
                true,
            );
            tlist = lappend(tlist, tle as *mut c_void);
        }

        /* If parent of inheritance tree, always fetch the tableoid too. */
        if (*rc).isParent {
            var = makeVar(
                (*rc).rti,
                TableOidAttributeNumber as AttrNumber,
                OIDOID,
                -1,
                InvalidOid,
                0,
            );
            snprintf(
                resname.as_mut_ptr(),
                std::mem::size_of::<[c_char; 32]>(),
                c"tableoid%u".as_ptr(),
                (*rc).rowmarkId,
            );
            tle = makeTargetEntry(
                var as *mut Expr,
                (list_length(tlist) + 1) as AttrNumber,
                pstrdup(resname.as_ptr()),
                true,
            );
            tlist = lappend(tlist, tle as *mut c_void);
        }
    });

    /*
     * If the query has a RETURNING list, add resjunk entries for any Vars
     * used in RETURNING that belong to other relations.  We need to do this
     * to make these Vars available for the RETURNING calculation.  Vars that
     * belong to the result rel don't need to be added, because they will be
     * made to refer to the actual heap tuple.
     */
    if !(*parse).returningList.is_null() && list_length((*parse).rtable) > 1 {
        let mut vars: *mut List;
        let l: *mut ListCell;

        vars = pull_var_clause(
            (*parse).returningList as *mut Node,
            (PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS) as c_int,
        );
        foreach!(l, vars, {
            let var: *mut Var = lfirst(current_cell!(l)) as *mut Var;
            let tle: *mut TargetEntry;

            if IsA!(var, T_Var) && (*var).varno == result_relation {
                continue; /* don't need it */
            }

            if tlist_member(var as *mut Expr, tlist) != std::ptr::null_mut() {
                continue; /* already got it */
            }

            tle = makeTargetEntry(
                var as *mut Expr,
                (list_length(tlist) + 1) as AttrNumber,
                std::ptr::null_mut(),
                true,
            );

            tlist = lappend(tlist, tle as *mut c_void);
        });
        list_free(vars);
    }

    (*root).processed_tlist = tlist;

    if !target_relation.is_null() {
        table_close(target_relation, NoLock as LOCKMODE);
    }
}

/*
 * extract_update_targetlist_colnos
 * 		Extract a list of the target-table column numbers that
 * 		an UPDATE's targetlist wants to assign to, then renumber.
 *
 * The convention in the parser and rewriter is that the resnos in an
 * UPDATE's non-resjunk TLE entries are the target column numbers
 * to assign to.  Here, we extract that info into a separate list, and
 * then convert the tlist to the sequential-numbering convention that's
 * used by all other query types.
 *
 * This is also applied to the tlist associated with INSERT ... ON CONFLICT
 * ... UPDATE, although not till much later in planning.
 */
pub unsafe fn extract_update_targetlist_colnos(tlist: *mut List) -> *mut List {
    let mut update_colnos: *mut List = std::ptr::null_mut(); /* NIL */
    let mut nextresno: AttrNumber = 1;
    let lc: *mut ListCell;

    foreach!(lc, tlist, {
        let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;

        if !(*tle).resjunk {
            update_colnos = lappend_int(update_colnos, (*tle).resno as c_int);
        }
        (*tle).resno = nextresno;
        nextresno += 1;
    });
    update_colnos
}

/*****************************************************************************
 *
 *		TARGETLIST EXPANSION
 *
 *****************************************************************************/

/*
 * expand_insert_targetlist
 *	  Given a target list as generated by the parser and a result relation,
 *	  add targetlist entries for any missing attributes, and ensure the
 *	  non-junk attributes appear in proper field order.
 *
 * Once upon a time we also did more or less this with UPDATE targetlists,
 * but now this code is only applied to INSERT targetlists.
 */
unsafe fn expand_insert_targetlist(
    root: *mut PlannerInfo,
    tlist: *mut List,
    rel: Relation,
) -> *mut List {
    let mut new_tlist: *mut List = std::ptr::null_mut(); /* NIL */
    let mut tlist_item: *mut ListCell;
    let mut attrno: c_int;
    let numattrs: c_int;

    tlist_item = list_head(tlist);

    /*
     * The rewriter should have already ensured that the TLEs are in correct
     * order; but we have to insert TLEs for any missing attributes.
     *
     * Scan the tuple description in the relation's relcache entry to make
     * sure we have all the user attributes in the right order.
     */
    numattrs = RelationGetNumberOfAttributes(rel);

    attrno = 1;
    while attrno <= numattrs {
        let att_tup: Form_pg_attribute = TupleDescAttr((*rel).rd_att, (attrno - 1) as c_int);
        let mut new_tle: *mut TargetEntry = std::ptr::null_mut();

        if !tlist_item.is_null() {
            let old_tle: *mut TargetEntry = lfirst(tlist_item) as *mut TargetEntry;

            if !(*old_tle).resjunk && (*old_tle).resno as c_int == attrno {
                new_tle = old_tle;
                tlist_item = lnext(tlist, tlist_item);
            }
        }

        if new_tle.is_null() {
            /*
             * Didn't find a matching tlist entry, so make one.
             *
             * INSERTs should insert NULL in this case.  (We assume the
             * rewriter would have inserted any available non-NULL default
             * value.)  Also, normally we must apply any domain constraints
             * that might exist --- this is to catch domain NOT NULL.
             *
             * When generating a NULL constant for a dropped column, we label
             * it INT4 (any other guaranteed-to-exist datatype would do as
             * well). We can't label it with the dropped column's datatype
             * since that might not exist anymore.  It does not really matter
             * what we claim the type is, since NULL is NULL --- its
             * representation is datatype-independent.  This could perhaps
             * confuse code comparing the finished plan to the target
             * relation, however.
             *
             * Another exception is that if the column is generated, the value
             * we produce here will be ignored, and we don't want to risk
             * throwing an error.  So in that case we *don't* want to apply
             * domain constraints, so we must produce a NULL of the base type.
             * Again, code comparing the finished plan to the target relation
             * must account for this.
             */
            let new_expr: *mut Node;

            if (*att_tup).attisdropped {
                /* Insert NULL for dropped column */
                new_expr = makeConst(
                    INT4OID,
                    -1,
                    InvalidOid,
                    std::mem::size_of::<int32>() as c_int,
                    0 as Datum,
                    true,  /* isnull */
                    true,  /* byval */
                ) as *mut Node;
            } else if (*att_tup).attgenerated != 0 {
                /* Generated column, insert a NULL of the base type */
                let mut baseTypeId: Oid = (*att_tup).atttypid;
                let mut baseTypeMod: int32 = (*att_tup).atttypmod;

                baseTypeId = getBaseTypeAndTypmod(baseTypeId, &mut baseTypeMod);
                new_expr = makeConst(
                    baseTypeId,
                    baseTypeMod,
                    (*att_tup).attcollation,
                    (*att_tup).attlen as c_int,
                    0 as Datum,
                    true, /* isnull */
                    (*att_tup).attbyval,
                ) as *mut Node;
            } else {
                /* Normal column, insert a NULL of the column datatype */
                let mut e: *mut Node = coerce_null_to_domain(
                    (*att_tup).atttypid,
                    (*att_tup).atttypmod,
                    (*att_tup).attcollation,
                    (*att_tup).attlen as c_int,
                    (*att_tup).attbyval,
                );
                /* Must run expression preprocessing on any non-const nodes */
                if !IsA!(e, T_Const) {
                    e = eval_const_expressions(root, e);
                }
                new_expr = e;
            }

            new_tle = makeTargetEntry(
                new_expr as *mut Expr,
                attrno as AttrNumber,
                pstrdup(NameStr!((*att_tup).attname)),
                false,
            );
        }

        new_tlist = lappend(new_tlist, new_tle as *mut c_void);

        attrno += 1;
    }

    /*
     * The remaining tlist entries should be resjunk; append them all to the
     * end of the new tlist, making sure they have resnos higher than the last
     * real attribute.  (Note: although the rewriter already did such
     * renumbering, we have to do it again here in case we added NULL entries
     * above.)
     */
    while !tlist_item.is_null() {
        let mut old_tle: *mut TargetEntry = lfirst(tlist_item) as *mut TargetEntry;

        if !(*old_tle).resjunk {
            elog!(ERROR, "targetlist is not sorted correctly");
        }
        /* Get the resno right, but don't copy unnecessarily */
        if (*old_tle).resno as c_int != attrno {
            old_tle = flatCopyTargetEntry(old_tle);
            (*old_tle).resno = attrno as AttrNumber;
        }
        new_tlist = lappend(new_tlist, old_tle as *mut c_void);
        attrno += 1;
        tlist_item = lnext(tlist, tlist_item);
    }

    new_tlist
}

/*
 * Locate PlanRowMark for given RT index, or return NULL if none
 *
 * This probably ought to be elsewhere, but there's no very good place
 */
pub unsafe fn get_plan_rowmark(rowmarks: *mut List, rtindex: Index) -> *mut PlanRowMark {
    let l: *mut ListCell;

    foreach!(l, rowmarks, {
        let rc: *mut PlanRowMark = lfirst(current_cell!(l)) as *mut PlanRowMark;

        if (*rc).rti == rtindex {
            return rc;
        }
    });
    std::ptr::null_mut()
}

/* ---- local stubs for unported dependencies ---- */

use crate::c::{int32, Index};

type LOCKMODE = c_int;

const NoLock: c_int = 0;
const PVC_INCLUDE_PLACEHOLDERS: u32 = 0x0004;
const PVC_RECURSE_AGGREGATES: u32 = 0x0001;
const PVC_RECURSE_WINDOWFUNCS: u32 = 0x0002;
const ROW_MARK_COPY: c_int = 6;

unsafe fn rt_fetch(rangetable_index: c_int, rangetable: *mut List) -> *mut RangeTblEntry {
    crate::parser::parsetree::rt_fetch(rangetable_index as _, rangetable as _) as _
}

unsafe fn table_open(relationId: Oid, lockmode: LOCKMODE) -> Relation {
    crate::access::table::table::table_open(relationId as _, lockmode as _) as _
}

unsafe fn table_close(relation: Relation, lockmode: LOCKMODE) {
    crate::access::table::table::table_close(relation as _, lockmode as _)
}

unsafe fn add_row_identity_columns(
    root: *mut PlannerInfo,
    rtindex: c_int,
    target_rte: *mut RangeTblEntry,
    target_relation: Relation,
) {
    crate::optimizer::util::appendinfo::add_row_identity_columns(
        root as _,
        rtindex as _,
        target_rte as _,
        target_relation as _,
    )
}

unsafe fn pull_var_clause(node: *mut Node, flags: c_int) -> *mut List {
    crate::optimizer::util::var::pull_var_clause(node as _, flags) as _
}

unsafe fn list_concat_copy(list1: *mut List, list2: *mut List) -> *mut List {
    crate::nodes::list::list_concat_copy(list1 as _, list2 as _) as _
}

unsafe fn tlist_member(node: *mut Expr, targetlist: *mut List) -> *mut TargetEntry {
    crate::optimizer::util::tlist::tlist_member(node as _, targetlist as _) as _
}

unsafe fn makeTargetEntry(
    expr: *mut Expr,
    resno: AttrNumber,
    resname: *mut c_char,
    resjunk: bool,
) -> *mut TargetEntry {
    crate::nodes::makefuncs::makeTargetEntry(expr as _, resno as _, resname as _, resjunk) as _
}

unsafe fn flatCopyTargetEntry(src_tle: *mut TargetEntry) -> *mut TargetEntry {
    crate::nodes::makefuncs::flatCopyTargetEntry(src_tle as _) as _
}

unsafe fn makeVar(
    varno: Index,
    varattno: AttrNumber,
    vartype: Oid,
    vartypmod: int32,
    varcollid: Oid,
    varlevelsup: Index,
) -> *mut Var {
    crate::nodes::makefuncs::makeVar(
        varno as _,
        varattno as _,
        vartype as _,
        vartypmod as _,
        varcollid as _,
        varlevelsup as _,
    ) as _
}

unsafe fn makeWholeRowVar(
    rte: *mut RangeTblEntry,
    varno: Index,
    varlevelsup: Index,
    allowScalar: bool,
) -> *mut Var {
    crate::nodes::makefuncs::makeWholeRowVar(rte as _, varno as _, varlevelsup as _, allowScalar) as _
}

unsafe fn makeConst(
    consttype: Oid,
    consttypmod: int32,
    constcollid: Oid,
    constlen: c_int,
    constvalue: Datum,
    constisnull: bool,
    constbyval: bool,
) -> *mut Const {
    crate::nodes::makefuncs::makeConst(
        consttype as _,
        consttypmod as _,
        constcollid as _,
        constlen as _,
        constvalue as _,
        constisnull,
        constbyval,
    ) as _
}

unsafe fn coerce_null_to_domain(
    typid: Oid,
    typmod: int32,
    collation: Oid,
    typlen: c_int,
    typbyval: bool,
) -> *mut Node {
    crate::parser::parse_coerce::coerce_null_to_domain(
        typid as _,
        typmod as _,
        collation as _,
        typlen as _,
        typbyval,
    ) as _
}

unsafe fn eval_const_expressions(root: *mut PlannerInfo, node: *mut Node) -> *mut Node {
    crate::optimizer::util::clauses::eval_const_expressions(root as _, node as _) as _
}

unsafe fn getBaseTypeAndTypmod(typid: Oid, typmod: *mut int32) -> Oid {
    crate::utils::cache::lsyscache::getBaseTypeAndTypmod(typid as _, typmod as _) as _
}

unsafe fn list_head(l: *mut List) -> *mut ListCell {
    crate::nodes::pg_list::list_head(l as _) as _
}

unsafe fn lnext(l: *mut List, cell: *mut ListCell) -> *mut ListCell {
    crate::nodes::pg_list::lnext(l as _, cell as _) as _
}

unsafe fn lappend(list: *mut List, datum: *mut c_void) -> *mut List {
    crate::nodes::list::lappend(list as _, datum as _) as _
}

unsafe fn lappend_int(list: *mut List, datum: c_int) -> *mut List {
    crate::nodes::list::lappend_int(list as _, datum) as _
}

unsafe fn list_free(list: *mut List) {
    crate::nodes::list::list_free(list as _)
}

unsafe fn list_length(l: *mut List) -> c_int {
    crate::nodes::pg_list::list_length(l as _)
}

unsafe fn RelationGetNumberOfAttributes(relation: Relation) -> c_int {
    crate::utils::rel::RelationGetNumberOfAttributes(relation as _)
}

unsafe fn TupleDescAttr(tupdesc: TupleDesc, i: c_int) -> Form_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(tupdesc as _, i) as _
}

unsafe fn pstrdup(s: *const c_char) -> *mut c_char {
    crate::utils::mmgr::mcxt::pstrdup(s as _) as _
}

/* Attribute number constants (src/include/access/sysattr.h) */
const SelfItemPointerAttributeNumber: c_int = -1;
const TableOidAttributeNumber: c_int = -7;

/* Type OID constants (src/include/catalog/pg_type_d.h) */
const INT4OID: Oid = 23;
const TIDOID: Oid = 27;
const OIDOID: Oid = 26;
const InvalidOid: Oid = 0;

/* NameStr macro (src/include/c.h) */
macro_rules! NameStr {
    ($name:expr) => {
        ($name).data.as_ptr() as *const c_char
    };
}
use NameStr;
