/*-------------------------------------------------------------------------
 *
 * rewriteSearchCycle.c
 *		Support for rewriting SEARCH and CYCLE clauses.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/rewrite/rewriteSearchCycle.c
 *
 *-------------------------------------------------------------------------
 */
//! Translated from postgres/src/backend/rewrite/rewriteSearchCycle.c
//! Companion header: postgres/src/include/rewrite/rewriteSearchCycle.h

use crate::prelude::*;

use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::nodes::pg_list::{lappend, lcons, lfirst, lfirst_mut, linitial, list_head, list_length, list_nth, List};
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::{RTEKind, SetOperation};
use crate::nodes::primnodes::CoercionForm;
use crate::catalog::pg_type_d::{BOOLOID, INT8OID, RECORDOID};
use crate::catalog::pg_known_oids::RECORD_EQ_OP;
use crate::postgres_ext::Oid;

// #[macro_export] macros live at the crate root.
use crate::{castNode, current_cell, foreach, list_make1, list_make2, list_nth_node, makeNode, strVal};

use std::ffi::{c_char, c_int};

// ----- Stub type aliases for unported node types -----
// These appear as opaque pointers; faithful structure first.
type CommonTableExpr = crate::nodes::parsenodes::CommonTableExpr;
type Query = crate::nodes::parsenodes::Query;
type SetOperationStmt = crate::nodes::parsenodes::SetOperationStmt;
type RangeTblEntry = crate::nodes::parsenodes::RangeTblEntry;
type RangeTblRef = crate::nodes::primnodes::RangeTblRef;
type TargetEntry = crate::nodes::primnodes::TargetEntry;
type RowExpr = crate::nodes::primnodes::RowExpr;
type ArrayExpr = crate::nodes::primnodes::ArrayExpr;
type FuncExpr = crate::nodes::primnodes::FuncExpr;
type Var = crate::nodes::primnodes::Var;
type Expr = crate::nodes::primnodes::Expr;
type FieldSelect = crate::nodes::primnodes::FieldSelect;
type ScalarArrayOpExpr = crate::nodes::primnodes::ScalarArrayOpExpr;
type CaseExpr = crate::nodes::primnodes::CaseExpr;
type CaseWhen = crate::nodes::primnodes::CaseWhen;

/*----------
 * Rewrite a CTE with SEARCH or CYCLE clause
 *
 * Consider a CTE like
 *
 * WITH RECURSIVE ctename (col1, col2, col3) AS (
 *     query1
 *   UNION [ALL]
 *     SELECT trosl FROM ctename
 * )
 *
 * With a search clause
 *
 * SEARCH BREADTH FIRST BY col1, col2 SET sqc
 *
 * the CTE is rewritten to
 *
 * WITH RECURSIVE ctename (col1, col2, col3, sqc) AS (
 *     SELECT col1, col2, col3,               -- original WITH column list
 *            ROW(0, col1, col2)              -- initial row of search columns
 *       FROM (query1) "*TLOCRN*" (col1, col2, col3)
 *   UNION [ALL]
 *     SELECT col1, col2, col3,               -- same as above
 *            ROW(sqc.depth + 1, col1, col2)  -- count depth
 *       FROM (SELECT trosl, ctename.sqc FROM ctename) "*TROCRN*" (col1, col2, col3, sqc)
 * )
 *
 * (This isn't quite legal SQL: sqc.depth is meant to refer to the first
 * column of sqc, which has a row type, but the field names are not defined
 * here.  Representing this properly in SQL would be more complicated (and the
 * SQL standard actually does it in that more complicated way), but the
 * internal representation allows us to construct it this way.)
 *
 * With a search clause
 *
 * SEARCH DEPTH FIRST BY col1, col2 SET sqc
 *
 * the CTE is rewritten to
 *
 * WITH RECURSIVE ctename (col1, col2, col3, sqc) AS (
 *     SELECT col1, col2, col3,               -- original WITH column list
 *            ARRAY[ROW(col1, col2)]          -- initial row of search columns
 *       FROM (query1) "*TLOCRN*" (col1, col2, col3)
 *   UNION [ALL]
 *     SELECT col1, col2, col3,               -- same as above
 *            sqc || ARRAY[ROW(col1, col2)]   -- record rows seen
 *       FROM (SELECT trosl, ctename.sqc FROM ctename) "*TROCRN*" (col1, col2, col3, sqc)
 * )
 *
 * With a cycle clause
 *
 * CYCLE col1, col2 SET cmc TO 'Y' DEFAULT 'N' USING cpa
 *
 * (cmc = cycle mark column, cpa = cycle path) the CTE is rewritten to
 *
 * WITH RECURSIVE ctename (col1, col2, col3, cmc, cpa) AS (
 *     SELECT col1, col2, col3,               -- original WITH column list
 *            'N',                            -- cycle mark default
 *            ARRAY[ROW(col1, col2)]          -- initial row of cycle columns
 *       FROM (query1) "*TLOCRN*" (col1, col2, col3)
 *   UNION [ALL]
 *     SELECT col1, col2, col3,               -- same as above
 *            CASE WHEN ROW(col1, col2) = ANY (ARRAY[cpa]) THEN 'Y' ELSE 'N' END,  -- compute cycle mark column
 *            cpa || ARRAY[ROW(col1, col2)]   -- record rows seen
 *       FROM (SELECT trosl, ctename.cmc, ctename.cpa FROM ctename) "*TROCRN*" (col1, col2, col3, cmc, cpa)
 *       WHERE cmc <> 'Y'
 * )
 *
 * The expression to compute the cycle mark column in the right-hand query is
 * written as
 *
 * CASE WHEN ROW(col1, col2) IN (SELECT p.* FROM TABLE(cpa) p) THEN cmv ELSE cmd END
 *
 * in the SQL standard, but in PostgreSQL we can use the scalar-array operator
 * expression shown above.
 *
 * Also, in some of the cases where operators are shown above we actually
 * directly produce the underlying function call.
 *
 * If both a search clause and a cycle clause is specified, then the search
 * clause column is added before the cycle clause columns.
 */

/*
 * Make a RowExpr from the specified column names, which have to be among the
 * output columns of the CTE.
 */
unsafe fn make_path_rowexpr(cte: *const CommonTableExpr, col_list: *const List) -> *mut RowExpr {
    let rowexpr: *mut RowExpr;
    let lc: *mut ListCell;

    rowexpr = makeNode!(RowExpr, T_RowExpr);
    (*rowexpr).row_typeid = RECORDOID;
    (*rowexpr).row_format = CoercionForm::COERCE_IMPLICIT_CAST;
    (*rowexpr).location = -1;

    foreach!(lc, col_list, {
        let colname: *mut c_char = strVal!(lfirst(current_cell!(lc)));

        for i in 0..list_length((*cte).ctecolnames) {
            let colname2: *mut c_char = strVal!(list_nth((*cte).ctecolnames, i));

            if strcmp(colname, colname2) == 0 {
                let var: *mut Var;

                var = makeVar(
                    1,
                    (i + 1) as AttrNumber,
                    list_nth_oid((*cte).ctecoltypes, i),
                    list_nth_int((*cte).ctecoltypmods, i),
                    list_nth_oid((*cte).ctecolcollations, i),
                    0,
                );
                (*rowexpr).args = lappend((*rowexpr).args, var as *mut _);
                (*rowexpr).colnames = lappend((*rowexpr).colnames, makeString(colname) as *mut _);
                break;
            }
        }
    });

    rowexpr
}

/*
 * Wrap a RowExpr in an ArrayExpr, for the initial search depth first or cycle
 * row.
 */
unsafe fn make_path_initial_array(rowexpr: *mut RowExpr) -> *mut Expr {
    let arr: *mut ArrayExpr;

    arr = makeNode!(ArrayExpr, T_ArrayExpr);
    (*arr).array_typeid = RECORDARRAYOID;
    (*arr).element_typeid = RECORDOID;
    (*arr).location = -1;
    (*arr).elements = list_make1!(rowexpr as *mut _);

    arr as *mut Expr
}

/*
 * Make an array catenation expression like
 *
 * cpa || ARRAY[ROW(cols)]
 *
 * where the varattno of cpa is provided as path_varattno.
 */
unsafe fn make_path_cat_expr(rowexpr: *mut RowExpr, path_varattno: AttrNumber) -> *mut Expr {
    let arr: *mut ArrayExpr;
    let fexpr: *mut FuncExpr;

    arr = makeNode!(ArrayExpr, T_ArrayExpr);
    (*arr).array_typeid = RECORDARRAYOID;
    (*arr).element_typeid = RECORDOID;
    (*arr).location = -1;
    (*arr).elements = list_make1!(rowexpr as *mut _);

    fexpr = makeFuncExpr(
        F_ARRAY_CAT,
        RECORDARRAYOID,
        list_make2!(
            makeVar(1, path_varattno, RECORDARRAYOID, -1, 0, 0) as *mut _,
            arr as *mut _
        ),
        InvalidOid,
        InvalidOid,
        CoercionForm::COERCE_EXPLICIT_CALL,
    );

    fexpr as *mut Expr
}

/*
 * The real work happens here.
 */
pub unsafe fn rewriteSearchAndCycle(mut cte: *mut CommonTableExpr) -> *mut CommonTableExpr {
    let ctequery: *mut Query;
    let sos: *mut SetOperationStmt;
    let rti1: c_int;
    let rti2: c_int;
    let rte1: *mut RangeTblEntry;
    let rte2: *mut RangeTblEntry;
    let mut newrte: *mut RangeTblEntry;
    let newq1: *mut Query;
    let newq2: *mut Query;
    let mut newsubquery: *mut Query;
    let mut rtr: *mut RangeTblRef;
    let mut search_seq_type: Oid = InvalidOid;
    let mut sqc_attno: AttrNumber = InvalidAttrNumber;
    let mut cmc_attno: AttrNumber = InvalidAttrNumber;
    let mut cpa_attno: AttrNumber = InvalidAttrNumber;
    let mut tle: *mut TargetEntry;
    let mut cycle_col_rowexpr: *mut RowExpr = std::ptr::null_mut();
    let mut search_col_rowexpr: *mut RowExpr = std::ptr::null_mut();
    let ewcl: *mut List;
    let mut cte_rtindex: c_int = -1;

    Assert!(!(*cte).search_clause.is_null() || !(*cte).cycle_clause.is_null());

    cte = copyObject(cte as *mut _) as *mut CommonTableExpr;

    ctequery = castNode!(Query, T_Query, (*cte).ctequery);

    /*
     * The top level of the CTE's query should be a UNION.  Find the two
     * subqueries.
     */
    Assert!(!(*ctequery).setOperations.is_null());
    sos = castNode!(SetOperationStmt, T_SetOperationStmt, (*ctequery).setOperations);
    Assert!((*sos).op == SetOperation::SETOP_UNION);

    rti1 = (*castNode!(RangeTblRef, T_RangeTblRef, (*sos).larg)).rtindex;
    rti2 = (*castNode!(RangeTblRef, T_RangeTblRef, (*sos).rarg)).rtindex;

    rte1 = rt_fetch(rti1, (*ctequery).rtable);
    rte2 = rt_fetch(rti2, (*ctequery).rtable);

    Assert!((*rte1).rtekind == RTEKind::RTE_SUBQUERY);
    Assert!((*rte2).rtekind == RTEKind::RTE_SUBQUERY);

    /*
     * We'll need this a few times later.
     */
    if !(*cte).search_clause.is_null() {
        if (*(*cte).search_clause).search_breadth_first {
            search_seq_type = RECORDOID;
        } else {
            search_seq_type = RECORDARRAYOID;
        }
    }

    /*
     * Attribute numbers of the added columns in the CTE's column list
     */
    if !(*cte).search_clause.is_null() {
        sqc_attno = (list_length((*cte).ctecolnames) + 1) as AttrNumber;
    }
    if !(*cte).cycle_clause.is_null() {
        cmc_attno = (list_length((*cte).ctecolnames) + 1) as AttrNumber;
        cpa_attno = (list_length((*cte).ctecolnames) + 2) as AttrNumber;
        if !(*cte).search_clause.is_null() {
            cmc_attno += 1;
            cpa_attno += 1;
        }
    }

    /*
     * Make new left subquery
     */
    newq1 = makeNode!(Query, T_Query);
    (*newq1).commandType = CmdType::CMD_SELECT;
    (*newq1).canSetTag = true;

    newrte = makeNode!(RangeTblEntry, T_RangeTblEntry);
    (*newrte).rtekind = RTEKind::RTE_SUBQUERY;
    (*newrte).alias = makeAlias(c"*TLOCRN*".as_ptr(), (*cte).ctecolnames);
    (*newrte).eref = (*newrte).alias;
    newsubquery = copyObject((*rte1).subquery as *mut _) as *mut Query;
    IncrementVarSublevelsUp(newsubquery as *mut Node, 1, 1);
    (*newrte).subquery = newsubquery;
    (*newrte).inFromCl = true;
    (*newq1).rtable = list_make1!(newrte as *mut _);

    rtr = makeNode!(RangeTblRef, T_RangeTblRef);
    (*rtr).rtindex = 1;
    (*newq1).jointree = makeFromExpr(list_make1!(rtr as *mut _), std::ptr::null_mut());

    /*
     * Make target list
     */
    for i in 0..list_length((*cte).ctecolnames) {
        let var: *mut Var;

        var = makeVar(
            1,
            (i + 1) as AttrNumber,
            list_nth_oid((*cte).ctecoltypes, i),
            list_nth_int((*cte).ctecoltypmods, i),
            list_nth_oid((*cte).ctecolcollations, i),
            0,
        );
        tle = makeTargetEntry(
            var as *mut Expr,
            (i + 1) as AttrNumber,
            strVal!(list_nth((*cte).ctecolnames, i)),
            false,
        );
        (*tle).resorigtbl =
            (*list_nth_node!(TargetEntry, T_TargetEntry, (*rte1).subquery.cast(), i)).resorigtbl;
        (*tle).resorigcol =
            (*list_nth_node!(TargetEntry, T_TargetEntry, (*rte1).subquery.cast(), i)).resorigcol;
        (*newq1).targetList = lappend((*newq1).targetList, tle as *mut _);
    }

    if !(*cte).search_clause.is_null() {
        let texpr: *mut Expr;

        search_col_rowexpr = make_path_rowexpr(cte, (*(*cte).search_clause).search_col_list);
        if (*(*cte).search_clause).search_breadth_first {
            (*search_col_rowexpr).args = lcons(
                makeConst(
                    INT8OID,
                    -1,
                    InvalidOid,
                    std::mem::size_of::<int64>() as c_int,
                    Int64GetDatum(0),
                    false,
                    FLOAT8PASSBYVAL,
                ) as *mut _,
                (*search_col_rowexpr).args,
            );
            (*search_col_rowexpr).colnames =
                lcons(makeString(c"*DEPTH*".as_ptr() as *mut c_char) as *mut _, (*search_col_rowexpr).colnames);
            texpr = search_col_rowexpr as *mut Expr;
        } else {
            texpr = make_path_initial_array(search_col_rowexpr);
        }
        tle = makeTargetEntry(
            texpr,
            (list_length((*newq1).targetList) + 1) as AttrNumber,
            (*(*cte).search_clause).search_seq_column,
            false,
        );
        (*newq1).targetList = lappend((*newq1).targetList, tle as *mut _);
    }
    if !(*cte).cycle_clause.is_null() {
        tle = makeTargetEntry(
            (*(*cte).cycle_clause).cycle_mark_default as *mut Expr,
            (list_length((*newq1).targetList) + 1) as AttrNumber,
            (*(*cte).cycle_clause).cycle_mark_column,
            false,
        );
        (*newq1).targetList = lappend((*newq1).targetList, tle as *mut _);
        cycle_col_rowexpr = make_path_rowexpr(cte, (*(*cte).cycle_clause).cycle_col_list);
        tle = makeTargetEntry(
            make_path_initial_array(cycle_col_rowexpr),
            (list_length((*newq1).targetList) + 1) as AttrNumber,
            (*(*cte).cycle_clause).cycle_path_column,
            false,
        );
        (*newq1).targetList = lappend((*newq1).targetList, tle as *mut _);
    }

    (*rte1).subquery = newq1;

    if !(*cte).search_clause.is_null() {
        (*(*rte1).eref).colnames = lappend(
            (*(*rte1).eref).colnames,
            makeString((*(*cte).search_clause).search_seq_column) as *mut _,
        );
    }
    if !(*cte).cycle_clause.is_null() {
        (*(*rte1).eref).colnames = lappend(
            (*(*rte1).eref).colnames,
            makeString((*(*cte).cycle_clause).cycle_mark_column) as *mut _,
        );
        (*(*rte1).eref).colnames = lappend(
            (*(*rte1).eref).colnames,
            makeString((*(*cte).cycle_clause).cycle_path_column) as *mut _,
        );
    }

    /*
     * Make new right subquery
     */
    newq2 = makeNode!(Query, T_Query);
    (*newq2).commandType = CmdType::CMD_SELECT;
    (*newq2).canSetTag = true;

    newrte = makeNode!(RangeTblEntry, T_RangeTblEntry);
    (*newrte).rtekind = RTEKind::RTE_SUBQUERY;
    ewcl = copyObject((*cte).ctecolnames as *mut _) as *mut List;
    if !(*cte).search_clause.is_null() {
        let _ = lappend(ewcl, makeString((*(*cte).search_clause).search_seq_column) as *mut _);
    }
    if !(*cte).cycle_clause.is_null() {
        let _ = lappend(ewcl, makeString((*(*cte).cycle_clause).cycle_mark_column) as *mut _);
        let _ = lappend(ewcl, makeString((*(*cte).cycle_clause).cycle_path_column) as *mut _);
    }
    (*newrte).alias = makeAlias(c"*TROCRN*".as_ptr(), ewcl);
    (*newrte).eref = (*newrte).alias;

    /*
     * Find the reference to the recursive CTE in the right UNION subquery's
     * range table.  We expect it to be two levels up from the UNION subquery
     * (and must check that to avoid being fooled by sub-WITHs with the same
     * CTE name).  There will not be more than one such reference, because the
     * parser would have rejected that (see checkWellFormedRecursion() in
     * parse_cte.c).  However, the parser doesn't insist that the reference
     * appear in the UNION subquery's topmost range table, so we might fail to
     * find it at all.  That's an unimplemented case for the moment.
     */
    {
        let mut rti = 1;
        while rti <= list_length((*(*rte2).subquery).rtable) {
            let e: *mut RangeTblEntry = rt_fetch(rti, (*(*rte2).subquery).rtable);

            if (*e).rtekind == RTEKind::RTE_CTE
                && strcmp((*cte).ctename, (*e).ctename) == 0
                && (*e).ctelevelsup == 2
            {
                cte_rtindex = rti;
                break;
            }
            rti += 1;
        }
    }
    if cte_rtindex <= 0 {
        ereport!(
            ERROR,
            "with a SEARCH or CYCLE clause, the recursive reference to WITH query must be at the top level of its right-hand SELECT"
        );
        unreachable!();
    }

    newsubquery = copyObject((*rte2).subquery as *mut _) as *mut Query;
    IncrementVarSublevelsUp(newsubquery as *mut Node, 1, 1);

    /*
     * Add extra columns to target list of subquery of right subquery
     */
    if !(*cte).search_clause.is_null() {
        let var: *mut Var;

        /* ctename.sqc */
        var = makeVar(cte_rtindex, sqc_attno, search_seq_type, -1, InvalidOid, 0);
        tle = makeTargetEntry(
            var as *mut Expr,
            (list_length((*newsubquery).targetList) + 1) as AttrNumber,
            (*(*cte).search_clause).search_seq_column,
            false,
        );
        (*newsubquery).targetList = lappend((*newsubquery).targetList, tle as *mut _);
    }
    if !(*cte).cycle_clause.is_null() {
        let mut var: *mut Var;

        /* ctename.cmc */
        var = makeVar(
            cte_rtindex,
            cmc_attno,
            (*(*cte).cycle_clause).cycle_mark_type,
            (*(*cte).cycle_clause).cycle_mark_typmod,
            (*(*cte).cycle_clause).cycle_mark_collation,
            0,
        );
        tle = makeTargetEntry(
            var as *mut Expr,
            (list_length((*newsubquery).targetList) + 1) as AttrNumber,
            (*(*cte).cycle_clause).cycle_mark_column,
            false,
        );
        (*newsubquery).targetList = lappend((*newsubquery).targetList, tle as *mut _);

        /* ctename.cpa */
        var = makeVar(cte_rtindex, cpa_attno, RECORDARRAYOID, -1, InvalidOid, 0);
        tle = makeTargetEntry(
            var as *mut Expr,
            (list_length((*newsubquery).targetList) + 1) as AttrNumber,
            (*(*cte).cycle_clause).cycle_path_column,
            false,
        );
        (*newsubquery).targetList = lappend((*newsubquery).targetList, tle as *mut _);
    }

    (*newrte).subquery = newsubquery;
    (*newrte).inFromCl = true;
    (*newq2).rtable = list_make1!(newrte as *mut _);

    rtr = makeNode!(RangeTblRef, T_RangeTblRef);
    (*rtr).rtindex = 1;

    if !(*cte).cycle_clause.is_null() {
        let expr: *mut Expr;

        /*
         * Add cmc <> cmv condition
         */
        expr = make_opclause(
            (*(*cte).cycle_clause).cycle_mark_neop,
            BOOLOID,
            false,
            makeVar(
                1,
                cmc_attno,
                (*(*cte).cycle_clause).cycle_mark_type,
                (*(*cte).cycle_clause).cycle_mark_typmod,
                (*(*cte).cycle_clause).cycle_mark_collation,
                0,
            ) as *mut Expr,
            (*(*cte).cycle_clause).cycle_mark_value as *mut Expr,
            InvalidOid,
            (*(*cte).cycle_clause).cycle_mark_collation,
        );

        (*newq2).jointree = makeFromExpr(list_make1!(rtr as *mut _), expr as *mut Node);
    } else {
        (*newq2).jointree = makeFromExpr(list_make1!(rtr as *mut _), std::ptr::null_mut());
    }

    /*
     * Make target list
     */
    for i in 0..list_length((*cte).ctecolnames) {
        let var: *mut Var;

        var = makeVar(
            1,
            (i + 1) as AttrNumber,
            list_nth_oid((*cte).ctecoltypes, i),
            list_nth_int((*cte).ctecoltypmods, i),
            list_nth_oid((*cte).ctecolcollations, i),
            0,
        );
        tle = makeTargetEntry(
            var as *mut Expr,
            (i + 1) as AttrNumber,
            strVal!(list_nth((*cte).ctecolnames, i)),
            false,
        );
        (*tle).resorigtbl =
            (*list_nth_node!(TargetEntry, T_TargetEntry, (*rte2).subquery.cast(), i)).resorigtbl;
        (*tle).resorigcol =
            (*list_nth_node!(TargetEntry, T_TargetEntry, (*rte2).subquery.cast(), i)).resorigcol;
        (*newq2).targetList = lappend((*newq2).targetList, tle as *mut _);
    }

    if !(*cte).search_clause.is_null() {
        let texpr: *mut Expr;

        if (*(*cte).search_clause).search_breadth_first {
            let fs: *mut FieldSelect;
            let fexpr: *mut FuncExpr;

            /*
             * ROW(sqc.depth + 1, cols)
             */

            search_col_rowexpr = copyObject(search_col_rowexpr as *mut _) as *mut RowExpr;

            fs = makeNode!(FieldSelect, T_FieldSelect);
            (*fs).arg = makeVar(1, sqc_attno, RECORDOID, -1, 0, 0) as *mut Expr;
            (*fs).fieldnum = 1;
            (*fs).resulttype = INT8OID;
            (*fs).resulttypmod = -1;

            fexpr = makeFuncExpr(
                F_INT8INC,
                INT8OID,
                list_make1!(fs as *mut _),
                InvalidOid,
                InvalidOid,
                CoercionForm::COERCE_EXPLICIT_CALL,
            );

            *lfirst_mut(list_head((*search_col_rowexpr).args)) = fexpr as *mut _;

            texpr = search_col_rowexpr as *mut Expr;
        } else {
            /*
             * sqc || ARRAY[ROW(cols)]
             */
            texpr = make_path_cat_expr(search_col_rowexpr, sqc_attno);
        }
        tle = makeTargetEntry(
            texpr,
            (list_length((*newq2).targetList) + 1) as AttrNumber,
            (*(*cte).search_clause).search_seq_column,
            false,
        );
        (*newq2).targetList = lappend((*newq2).targetList, tle as *mut _);
    }

    if !(*cte).cycle_clause.is_null() {
        let saoe: *mut ScalarArrayOpExpr;
        let caseexpr: *mut CaseExpr;
        let casewhen: *mut CaseWhen;

        /*
         * CASE WHEN ROW(cols) = ANY (ARRAY[cpa]) THEN cmv ELSE cmd END
         */

        saoe = makeNode!(ScalarArrayOpExpr, T_ScalarArrayOpExpr);
        (*saoe).location = -1;
        (*saoe).opno = RECORD_EQ_OP;
        (*saoe).useOr = true;
        (*saoe).args = list_make2!(
            cycle_col_rowexpr as *mut _,
            makeVar(1, cpa_attno, RECORDARRAYOID, -1, 0, 0) as *mut _
        );

        caseexpr = makeNode!(CaseExpr, T_CaseExpr);
        (*caseexpr).location = -1;
        (*caseexpr).casetype = (*(*cte).cycle_clause).cycle_mark_type;
        (*caseexpr).casecollid = (*(*cte).cycle_clause).cycle_mark_collation;
        casewhen = makeNode!(CaseWhen, T_CaseWhen);
        (*casewhen).location = -1;
        (*casewhen).expr = saoe as *mut Expr;
        (*casewhen).result = (*(*cte).cycle_clause).cycle_mark_value as *mut Expr;
        (*caseexpr).args = list_make1!(casewhen as *mut _);
        (*caseexpr).defresult = (*(*cte).cycle_clause).cycle_mark_default as *mut Expr;

        tle = makeTargetEntry(
            caseexpr as *mut Expr,
            (list_length((*newq2).targetList) + 1) as AttrNumber,
            (*(*cte).cycle_clause).cycle_mark_column,
            false,
        );
        (*newq2).targetList = lappend((*newq2).targetList, tle as *mut _);

        /*
         * cpa || ARRAY[ROW(cols)]
         */
        tle = makeTargetEntry(
            make_path_cat_expr(cycle_col_rowexpr, cpa_attno),
            (list_length((*newq2).targetList) + 1) as AttrNumber,
            (*(*cte).cycle_clause).cycle_path_column,
            false,
        );
        (*newq2).targetList = lappend((*newq2).targetList, tle as *mut _);
    }

    (*rte2).subquery = newq2;

    if !(*cte).search_clause.is_null() {
        (*(*rte2).eref).colnames = lappend(
            (*(*rte2).eref).colnames,
            makeString((*(*cte).search_clause).search_seq_column) as *mut _,
        );
    }
    if !(*cte).cycle_clause.is_null() {
        (*(*rte2).eref).colnames = lappend(
            (*(*rte2).eref).colnames,
            makeString((*(*cte).cycle_clause).cycle_mark_column) as *mut _,
        );
        (*(*rte2).eref).colnames = lappend(
            (*(*rte2).eref).colnames,
            makeString((*(*cte).cycle_clause).cycle_path_column) as *mut _,
        );
    }

    /*
     * Add the additional columns to the SetOperationStmt
     */
    if !(*cte).search_clause.is_null() {
        (*sos).colTypes = lappend_oid((*sos).colTypes, search_seq_type);
        (*sos).colTypmods = lappend_int((*sos).colTypmods, -1);
        (*sos).colCollations = lappend_oid((*sos).colCollations, InvalidOid);
        if !(*sos).all {
            (*sos).groupClauses = lappend(
                (*sos).groupClauses,
                makeSortGroupClauseForSetOp(search_seq_type, true) as *mut _,
            );
        }
    }
    if !(*cte).cycle_clause.is_null() {
        (*sos).colTypes = lappend_oid((*sos).colTypes, (*(*cte).cycle_clause).cycle_mark_type);
        (*sos).colTypmods = lappend_int((*sos).colTypmods, (*(*cte).cycle_clause).cycle_mark_typmod);
        (*sos).colCollations =
            lappend_oid((*sos).colCollations, (*(*cte).cycle_clause).cycle_mark_collation);
        if !(*sos).all {
            (*sos).groupClauses = lappend(
                (*sos).groupClauses,
                makeSortGroupClauseForSetOp((*(*cte).cycle_clause).cycle_mark_type, true) as *mut _,
            );
        }

        (*sos).colTypes = lappend_oid((*sos).colTypes, RECORDARRAYOID);
        (*sos).colTypmods = lappend_int((*sos).colTypmods, -1);
        (*sos).colCollations = lappend_oid((*sos).colCollations, InvalidOid);
        if !(*sos).all {
            (*sos).groupClauses = lappend(
                (*sos).groupClauses,
                makeSortGroupClauseForSetOp(RECORDARRAYOID, true) as *mut _,
            );
        }
    }

    /*
     * Add the additional columns to the CTE query's target list
     */
    if !(*cte).search_clause.is_null() {
        (*ctequery).targetList = lappend(
            (*ctequery).targetList,
            makeTargetEntry(
                makeVar(1, sqc_attno, search_seq_type, -1, InvalidOid, 0) as *mut Expr,
                (list_length((*ctequery).targetList) + 1) as AttrNumber,
                (*(*cte).search_clause).search_seq_column,
                false,
            ) as *mut _,
        );
    }
    if !(*cte).cycle_clause.is_null() {
        (*ctequery).targetList = lappend(
            (*ctequery).targetList,
            makeTargetEntry(
                makeVar(
                    1,
                    cmc_attno,
                    (*(*cte).cycle_clause).cycle_mark_type,
                    (*(*cte).cycle_clause).cycle_mark_typmod,
                    (*(*cte).cycle_clause).cycle_mark_collation,
                    0,
                ) as *mut Expr,
                (list_length((*ctequery).targetList) + 1) as AttrNumber,
                (*(*cte).cycle_clause).cycle_mark_column,
                false,
            ) as *mut _,
        );
        (*ctequery).targetList = lappend(
            (*ctequery).targetList,
            makeTargetEntry(
                makeVar(1, cpa_attno, RECORDARRAYOID, -1, InvalidOid, 0) as *mut Expr,
                (list_length((*ctequery).targetList) + 1) as AttrNumber,
                (*(*cte).cycle_clause).cycle_path_column,
                false,
            ) as *mut _,
        );
    }

    /*
     * Add the additional columns to the CTE's output columns
     */
    (*cte).ctecolnames = ewcl;
    if !(*cte).search_clause.is_null() {
        (*cte).ctecoltypes = lappend_oid((*cte).ctecoltypes, search_seq_type);
        (*cte).ctecoltypmods = lappend_int((*cte).ctecoltypmods, -1);
        (*cte).ctecolcollations = lappend_oid((*cte).ctecolcollations, InvalidOid);
    }
    if !(*cte).cycle_clause.is_null() {
        (*cte).ctecoltypes = lappend_oid((*cte).ctecoltypes, (*(*cte).cycle_clause).cycle_mark_type);
        (*cte).ctecoltypmods =
            lappend_int((*cte).ctecoltypmods, (*(*cte).cycle_clause).cycle_mark_typmod);
        (*cte).ctecolcollations =
            lappend_oid((*cte).ctecolcollations, (*(*cte).cycle_clause).cycle_mark_collation);

        (*cte).ctecoltypes = lappend_oid((*cte).ctecoltypes, RECORDARRAYOID);
        (*cte).ctecoltypmods = lappend_int((*cte).ctecoltypmods, -1);
        (*cte).ctecolcollations = lappend_oid((*cte).ctecolcollations, InvalidOid);
    }

    cte
}

// ===== Local stubs for unported helpers / constants =====

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
}

const RECORDARRAYOID: Oid = 2287;
const F_ARRAY_CAT: Oid = 378; // fmgroids.h
const F_INT8INC: Oid = 1219; // fmgroids.h

#[allow(non_camel_case_types)]
type ListCell = crate::nodes::pg_list::ListCell;

unsafe fn rt_fetch(rangetable_index: c_int, rangetable: *mut List) -> *mut RangeTblEntry {
    list_nth(rangetable, rangetable_index - 1) as *mut RangeTblEntry
}

unsafe fn list_nth_oid(list: *const List, n: c_int) -> Oid {
    unimplemented!() // TODO: nodes/pg_list.c
}

unsafe fn list_nth_int(list: *const List, n: c_int) -> c_int {
    unimplemented!() // TODO: nodes/pg_list.c
}

unsafe fn lappend_oid(list: *mut List, datum: Oid) -> *mut List {
    unimplemented!() // TODO: nodes/list.c
}

unsafe fn lappend_int(list: *mut List, datum: c_int) -> *mut List {
    unimplemented!() // TODO: nodes/list.c
}

unsafe fn makeVar(
    varno: c_int,
    varattno: AttrNumber,
    vartype: Oid,
    vartypmod: i32,
    varcollid: Oid,
    varlevelsup: u32,
) -> *mut Var {
    unimplemented!() // TODO: nodes/makefuncs.c
}

unsafe fn makeConst(
    consttype: Oid,
    consttypmod: i32,
    constcollid: Oid,
    constlen: c_int,
    constvalue: Datum,
    constisnull: bool,
    constbyval: bool,
) -> *mut crate::nodes::primnodes::Const {
    unimplemented!() // TODO: nodes/makefuncs.c
}

unsafe fn makeTargetEntry(
    expr: *mut Expr,
    resno: AttrNumber,
    resname: *mut c_char,
    resjunk: bool,
) -> *mut TargetEntry {
    unimplemented!() // TODO: nodes/makefuncs.c
}

unsafe fn makeFuncExpr(
    funcid: Oid,
    rettype: Oid,
    args: *mut List,
    funccollid: Oid,
    inputcollid: Oid,
    fformat: crate::nodes::primnodes::CoercionForm,
) -> *mut FuncExpr {
    unimplemented!() // TODO: nodes/makefuncs.c
}

unsafe fn makeAlias(aliasname: *const c_char, colnames: *mut List) -> *mut crate::nodes::primnodes::Alias {
    unimplemented!() // TODO: nodes/makefuncs.c
}

unsafe fn makeString(s: *mut c_char) -> *mut crate::nodes::value::String {
    unimplemented!() // TODO: nodes/value.c
}

unsafe fn makeFromExpr(fromlist: *mut List, quals: *mut Node) -> *mut crate::nodes::primnodes::FromExpr {
    unimplemented!() // TODO: nodes/makefuncs.c
}

unsafe fn make_opclause(
    opno: Oid,
    opresulttype: Oid,
    opretset: bool,
    leftop: *mut Expr,
    rightop: *mut Expr,
    opcollid: Oid,
    inputcollid: Oid,
) -> *mut Expr {
    unimplemented!() // TODO: nodes/makefuncs.c
}

unsafe fn makeSortGroupClauseForSetOp(
    rescoltype: Oid,
    require_hash: bool,
) -> *mut crate::nodes::parsenodes::SortGroupClause {
    unimplemented!() // TODO: parser/analyze.c
}

unsafe fn IncrementVarSublevelsUp(node: *mut Node, delta_sublevels_up: c_int, min_sublevels_up: c_int) {
    unimplemented!() // TODO: rewrite/rewriteManip.c
}

unsafe fn copyObject<T>(node: *const T) -> *mut T {
    unimplemented!() // TODO: nodes/copyfuncs.c
}
